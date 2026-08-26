#!/usr/bin/env python3
"""Repair recent canonical QMT history gaps on the Windows QMT edge.

This is deliberately separate from the local-history backfill.  It inspects
only the production canonical tables, proves every partition against immutable
QMT calendar/catalog evidence, and invokes the existing exact publishers one
session at a time.  A completed partition is never fetched again, so a failed
run can be retried safely.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from server.common.qmt_trade_calendar import (
    load_trade_calendar_receipt,
    validate_trade_calendar_runtime_schema,
)
from tools.env_config import load_project_env


RESULT_SCHEMA = "probiga.qmt-canonical-history-gap-repair-result.v1"
TASK_TYPE = "qmt_canonical_history_gap_repair"
EDGE_ROLE = "qmt_windows_edge"
PROVIDER = "gj_big_qmt_inner"
DATASET_ORDER = (
    "stock_daily",
    "stock_minute",
    "stock_minute_flow",
    "index_kline",
    "index_minute",
    "etf_daily",
)
INNER_TASK_TYPES = {
    "stock_daily": "qmt_stock_daily_canonical",
    "stock_minute": "qmt_stock_minute_canonical",
    "stock_minute_flow": "qmt_stock_minute_flow_canonical",
    "index_kline": "qmt_index_kline",
    "index_minute": "qmt_index_minute",
}
_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CLOSED_READY_TIME = time(15, 10)
_REPAIR_WINDOW_START = time(20, 30)
_REPAIR_WINDOW_END = time(8, 0)


class CanonicalGapRepairBlocked(RuntimeError):
    """The repair cannot produce exact canonical evidence yet."""


@dataclass(frozen=True, order=True)
class PartitionRef:
    dataset: str
    trade_date: str

    @property
    def partition_id(self) -> str:
        return f"{self.dataset}:{self.trade_date}"


@dataclass(frozen=True)
class CalendarWindow:
    sessions: tuple[str, ...]
    batch_id: str
    manifest_hash: str
    source_session_set_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "manifest_hash": self.manifest_hash,
            "source_session_set_hash": self.source_session_set_hash,
            "selected_session_set_hash": _digest(list(self.sessions)),
        }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _signed(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["result_sha256"] = _digest(result)
    return result


def _shanghai_now() -> datetime:
    return datetime.now(_SHANGHAI).replace(microsecond=0)


def _as_shanghai(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=_SHANGHAI, microsecond=0)
    return value.astimezone(_SHANGHAI).replace(microsecond=0)


def _timestamp(value: datetime) -> str:
    return _as_shanghai(value).isoformat(timespec="seconds")


def _parse_timestamp(value: Any, *, field: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} is not an ISO timestamp") from exc
    return _as_shanghai(parsed)


def _normalized_datasets(values: Iterable[str]) -> tuple[str, ...]:
    requested = {str(item or "").strip() for item in values}
    unknown = sorted(requested - set(DATASET_ORDER))
    if unknown:
        raise ValueError(f"unsupported canonical repair datasets: {unknown}")
    return tuple(item for item in DATASET_ORDER if item in requested)


def _build_sha(explicit: str = "") -> str:
    value = str(
        explicit
        or os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if _SHA40.fullmatch(value) is None or value == "0" * 40:
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: exact scheduler build SHA is unavailable"
        )
    return value


def validate_executor(
    *,
    platform_name: str | None = None,
    executor_role: str | None = None,
    task_type: str | None = None,
) -> None:
    platform = os.name if platform_name is None else str(platform_name)
    role = (
        str(os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") or "").strip()
        if executor_role is None
        else str(executor_role).strip()
    )
    observed_type = (
        str(os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") or "").strip()
        if task_type is None
        else str(task_type).strip()
    )
    if platform != "nt" or role != EDGE_ROLE:
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical history repair is Windows QMT edge-only"
        )
    if observed_type and observed_type != TASK_TYPE:
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: scheduler task type differs from canonical gap repair"
        )


def validate_repair_clock(now: datetime) -> None:
    """Keep the heavy repair away from the signed-in QMT trading window."""

    current = _as_shanghai(now).time().replace(tzinfo=None)
    if _REPAIR_WINDOW_END <= current < _REPAIR_WINDOW_START:
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical history repair waits for the 20:30-08:00 window"
        )


def load_recent_closed_window(
    engine: Any,
    *,
    now: datetime,
    lookback_sessions: int,
    batch_id: str | None = None,
) -> CalendarWindow:
    count = int(lookback_sessions)
    if count <= 0 or count > 120:
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: lookback session count is outside 1..120"
        )
    current = _as_shanghai(now)
    cutoff = (
        current.date()
        if current.time().replace(tzinfo=None) >= _CLOSED_READY_TIME
        else current.date() - timedelta(days=1)
    )
    search_days = max(60, count * 4 + 30)
    start = cutoff - timedelta(days=search_days)
    validate_trade_calendar_runtime_schema(engine)
    try:
        with engine.connect() as connection:
            receipt = load_trade_calendar_receipt(
                connection,
                start_date=start.isoformat(),
                end_date=cutoff.isoformat(),
                decision_known_at=current.replace(tzinfo=None),
                batch_id=batch_id,
            )
    except Exception as exc:
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: immutable QMT calendar receipt does not cover repair window"
        ) from exc
    sessions = [
        item
        for item in receipt.sessions_between(start.isoformat(), cutoff.isoformat())
        if item <= cutoff.isoformat()
    ][-count:]
    if len(sessions) != count:
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: immutable QMT calendar lacks requested closed sessions"
        )
    return CalendarWindow(
        sessions=tuple(sessions),
        batch_id=str(receipt.batch_id),
        manifest_hash=str(receipt.manifest_hash),
        source_session_set_hash=str(receipt.session_set_hash),
    )


def _mapping_rows(result: Any) -> list[dict[str, Any]]:
    mappings = getattr(result, "mappings", None)
    if callable(mappings):
        return [dict(row) for row in mappings().all()]
    return [dict(row) for row in result]


class CanonicalPartitionInspector:
    """Pure-read exactness checks for the six canonical history datasets."""

    def __init__(
        self,
        primary_engine: Any,
        history_engine: Any,
        minute_engine: Any,
        *,
        window: CalendarWindow,
        decision_time: datetime,
    ) -> None:
        self.primary_engine = primary_engine
        self.history_engine = history_engine
        self.minute_engine = minute_engine
        self.window = window
        self.decision_time = _as_shanghai(decision_time)
        self._index_catalog: Sequence[Any] | None = None

    def __call__(self, partition: PartitionRef) -> dict[str, Any]:
        if partition.trade_date not in self.window.sessions:
            raise CanonicalGapRepairBlocked(
                "partition lies outside immutable repair window"
            )
        if partition.dataset == "stock_daily":
            return self._stock_daily(partition.trade_date)
        if partition.dataset == "stock_minute":
            return self._stock_minute(partition.trade_date)
        if partition.dataset == "stock_minute_flow":
            return self._stock_minute_flow(partition.trade_date)
        if partition.dataset == "index_kline":
            return self._index(partition.trade_date, dataset="kline")
        if partition.dataset == "index_minute":
            return self._index(partition.trade_date, dataset="minute")
        if partition.dataset == "etf_daily":
            return self._etf(partition.trade_date)
        raise CanonicalGapRepairBlocked("unsupported canonical partition")

    def _stock_daily(self, trade_date: str) -> dict[str, Any]:
        from server.common.qmt_attestation_contract import (
            expected_stock_set_contract,
        )
        from server.common.qmt_daily_market_truth import (
            load_qmt_daily_market_truth,
        )
        from server.common.qmt_stock_catalog import load_stock_catalog
        from tools import sync_qmt_stock_edge as publisher

        decision = self.decision_time.replace(tzinfo=None)
        with self.history_engine.connect() as connection:
            truth = load_qmt_daily_market_truth(
                connection,
                start_date=trade_date,
                end_date=trade_date,
                decision_known_at=decision,
            )
            catalog = load_stock_catalog(
                connection,
                batch_id=truth.catalog_batch_id,
                decision_known_at=decision,
            )
        codes = sorted(catalog.eligible_codes(trade_date))
        expected = expected_stock_set_contract(trade_date, codes)
        if int(truth.attested_row_count) != int(expected["stock_count"]):
            raise CanonicalGapRepairBlocked(
                "canonical stock daily attested count differs"
            )
        proof = publisher._read_daily_partition(
            self.history_engine,
            trade_date=trade_date,
            expected_count=int(expected["stock_count"]),
            expected_set_hash=str(expected["stock_set_hash"]),
        )
        return {
            "dataset": "stock_daily",
            "trade_date": trade_date,
            "row_count": int(proof["row_count"]),
            "row_hash": str(proof["row_hash"]),
            "code_count": int(proof["code_count"]),
            "code_set_hash": str(proof["code_set_hash"]),
        }

    def _stock_minute(self, trade_date: str) -> dict[str, Any]:
        from server.common.qmt_history_coverage import (
            validate_coverage_authority,
        )
        from tools import sync_qmt_stock_edge as publisher

        receipt = publisher._minute_receipt(self.primary_engine, trade_date)
        bundle = {
            "manifest": dict(receipt["manifest"]),
            "entities": list(receipt["entities"]),
        }
        with self.primary_engine.connect() as connection:
            manifest = validate_coverage_authority(connection, bundle)
        proof = publisher._validate_minute_partition(
            self.history_engine,
            trade_date=trade_date,
            receipt=receipt,
        )
        return {
            "dataset": "stock_minute",
            "trade_date": trade_date,
            "row_count": int(proof["row_count"]),
            "row_hash": str(proof["row_hash"]),
            "expected_entity_count": int(
                manifest["expected_entity_count"]
            ),
            "expected_entity_set_hash": str(
                manifest["expected_entity_set_hash"]
            ),
            "traded_code_count": int(proof["traded_code_count"]),
            "traded_code_set_hash": str(proof["traded_code_set_hash"]),
            "minute_grid_count": int(proof["minute_grid_count"]),
            "minute_grid_hash": str(proof["minute_grid_hash"]),
        }

    def _stock_minute_flow(self, trade_date: str) -> dict[str, Any]:
        from tools import sync_qmt_minute_flow_exact as publisher

        publisher.validate_runtime_schema(
            self.primary_engine,
            self.minute_engine,
        )
        universe = publisher.load_flow_universe(
            self.primary_engine,
            trade_date=trade_date,
            now=self.decision_time,
        )
        with self.minute_engine.connect() as connection:
            proof = publisher._stream_table_proof(
                connection,
                table=publisher.TABLE,
                trade_date=trade_date,
            )
        try:
            nonzero_ratio = Decimal(str(proof["nonzero_code_ratio"]))
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise CanonicalGapRepairBlocked(
                "canonical stock minute-flow nonzero proof differs"
            ) from exc
        if (
            int(proof.get("code_count") or 0) != universe.traded_stock_count
            or proof.get("code_set_hash") != universe.traded_stock_set_hash
            or int(proof.get("row_count") or 0)
            != universe.traded_stock_count * len(publisher.GRID)
            or proof.get("minute_grid_profile")
            != publisher.QMT_MINUTE_GRID_PROFILE
            or int(proof.get("minute_grid_count") or 0) != len(publisher.GRID)
            or proof.get("minute_grid_hash") != publisher.GRID_HASH
            or nonzero_ratio < publisher.MIN_NONZERO_CODE_RATIO
        ):
            raise CanonicalGapRepairBlocked(
                "canonical stock minute-flow partition differs"
            )
        return {
            "dataset": "stock_minute_flow",
            "trade_date": trade_date,
            "row_count": int(proof["row_count"]),
            "row_hash": str(proof["row_hash"]),
            "code_count": int(proof["code_count"]),
            "code_set_hash": str(proof["code_set_hash"]),
            "minute_grid_count": int(proof["minute_grid_count"]),
            "minute_grid_hash": str(proof["minute_grid_hash"]),
            "nonzero_code_ratio": format(nonzero_ratio, "f"),
            "catalog_manifest_hash": str(universe.catalog["manifest_hash"]),
            "daily_truth_hash": str(universe.daily_truth["truth_hash"]),
        }

    def _load_index_catalog(self) -> Sequence[Any]:
        if self._index_catalog is None:
            from tools import sync_qmt_index_edge as publisher

            self._index_catalog = publisher._load_index_catalog(
                self.primary_engine,
                expected_batch_id=self.window.batch_id,
            )
        return self._index_catalog

    def _index(self, trade_date: str, *, dataset: str) -> dict[str, Any]:
        from tools import sync_qmt_index_edge as publisher

        catalog = self._load_index_catalog()
        expected = publisher.expected_codes_by_session(catalog, [trade_date])
        expected_codes = sorted(expected[trade_date])
        # Read the complete catalog-owned partition, including members that
        # are not eligible for this historical session.  The validator can
        # then reject stale rows for pre-listing/post-expiry members instead
        # of silently hiding them behind an eligible-only SQL predicate.
        owned_codes = sorted(member.index_code for member in catalog)
        frame = publisher._read_published(
            dataset=dataset,
            primary_engine=self.primary_engine,
            history_engine=self.history_engine,
            catalog=catalog,
            codes=owned_codes,
            sessions=[trade_date],
        )
        captured_at = self.decision_time.replace(tzinfo=None)
        if dataset == "kline":
            verified = publisher.validate_kline_frame(
                frame,
                catalog=catalog,
                expected_by_session=expected,
                captured_at=captured_at,
            )
            stable_columns = (
                "index_code", "trade_time", "trade_date", "k_type", "open",
                "close", "high", "low", "volume", "amount", "change",
                "change_pct",
            )
            result_dataset = "index_kline"
        else:
            verified = publisher.validate_minute_frame(
                frame,
                catalog=catalog,
                expected_by_session=expected,
                captured_at=captured_at,
            )
            stable_columns = (
                "index_code", "trade_time", "trade_date", "price",
                "avg_price", "change", "change_pct", "volume", "amount",
            )
            result_dataset = "index_minute"
        stable_rows = verified[list(stable_columns)].to_dict("records")
        result: dict[str, Any] = {
            "dataset": result_dataset,
            "trade_date": trade_date,
            "row_count": len(stable_rows),
            "row_hash": _digest(stable_rows),
            "code_count": len(expected_codes),
            "code_set_hash": _digest(expected_codes),
        }
        if dataset == "minute":
            from server.common.qmt_history_coverage import minute_time_grid

            grid = list(minute_time_grid())
            result.update(
                minute_grid_count=len(grid),
                minute_grid_hash=_digest(grid),
            )
        return result

    def _etf(self, trade_date: str) -> dict[str, Any]:
        from tools.sync_etf_bigqmt_daily import validate_partition_rows

        with self.primary_engine.connect() as connection:
            rows = _mapping_rows(
                connection.execute(
                    text(
                        """
                        SELECT etf_code,trade_time,trade_date,k_type,adjust_type,
                               `open`,`close`,high,low,volume,amount,pre_close,
                               `change`,change_pct,data_source,validation_source,
                               validation_status,batch_id,data_version,
                               quality_status,permission_status
                          FROM sm_etf_kline
                         WHERE trade_date=:trade_date
                           AND k_type=1 AND adjust_type IN (0,1)
                         ORDER BY adjust_type,etf_code
                        """
                    ),
                    {"trade_date": trade_date},
                )
            )
        proof = validate_partition_rows(rows, trade_date=trade_date)
        return {
            "dataset": "etf_daily",
            "trade_date": trade_date,
            "row_count": int(proof["row_count"]),
            "row_hash": str(proof["row_hash"]),
            "group_hashes": dict(proof["group_hashes"]),
        }


@contextmanager
def _inner_task_identity(task_type: str):
    key = "PROBIGA_SCHEDULER_TASK_TYPE"
    previous = os.environ.get(key)
    os.environ[key] = task_type
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


class ExactPartitionPublisher:
    """Adapter around existing exact, atomic, read-back-verified publishers."""

    def __init__(
        self,
        primary_engine: Any,
        *,
        expected_build_sha: str,
        now: datetime,
        minute_engine: Any | None = None,
    ) -> None:
        self.primary_engine = primary_engine
        self.expected_build_sha = expected_build_sha
        self.now = _as_shanghai(now)
        self.minute_engine = minute_engine

    def __call__(self, partition: PartitionRef) -> dict[str, Any]:
        if partition.dataset == "stock_minute_flow":
            return self._stock_minute_flow(partition)
        if partition.dataset.startswith("stock_"):
            return self._stock(partition)
        if partition.dataset.startswith("index_"):
            return self._index(partition)
        if partition.dataset == "etf_daily":
            return self._etf(partition)
        raise CanonicalGapRepairBlocked("unsupported exact publisher dataset")

    def _stock_minute_flow(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import sync_qmt_minute_flow_exact as publisher

        owned_engine = None
        minute_engine = self.minute_engine
        if minute_engine is None:
            owned_engine = get_minute_engine()
            minute_engine = owned_engine
        try:
            with _inner_task_identity(INNER_TASK_TYPES[partition.dataset]):
                result = publisher.run_sync(
                    self.primary_engine,
                    minute_engine,
                    trade_date=partition.trade_date,
                    apply=True,
                    expected_build_sha=self.expected_build_sha,
                    now=self.now,
                )
        finally:
            dispose = getattr(owned_engine, "dispose", None)
            if callable(dispose):
                dispose()
        if (
            publisher.validate_task_result(result, 0) != "complete"
            or result.get("trade_date") != partition.trade_date
            or result.get("build_sha") != self.expected_build_sha
            or _SHA256.fullmatch(str(result.get("receipt_id") or "")) is None
        ):
            raise CanonicalGapRepairBlocked(
                "stock minute-flow exact publisher result is incomplete"
            )
        return {
            "source_schema": result["schema"],
            "source_status": result["status"],
            "source_receipt_sha256": str(result["receipt_id"]),
            "forward_observation_created": False,
        }

    def _stock(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import sync_qmt_stock_edge as publisher

        dataset = "daily" if partition.dataset == "stock_daily" else "minute"
        with _inner_task_identity(INNER_TASK_TYPES[partition.dataset]):
            result = publisher.run(
                dataset=dataset,
                latest_session=False,
                start_date=partition.trade_date,
                end_date=partition.trade_date,
                expected_build_sha=self.expected_build_sha,
                apply=True,
                now=self.now.replace(tzinfo=None),
            )
        if (
            publisher.validate_task_result(result, 0) != "complete"
            or list(result.get("sessions") or []) != [partition.trade_date]
            or result.get("build_sha") != self.expected_build_sha
        ):
            raise CanonicalGapRepairBlocked(
                "stock exact publisher result is incomplete"
            )
        return {
            "source_schema": result["schema"],
            "source_status": result["status"],
            "source_receipt_sha256": str(result["receipt_id"]),
            "forward_observation_created": False,
        }

    def _index(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import sync_qmt_index_edge as publisher

        dataset = "kline" if partition.dataset == "index_kline" else "minute"
        with _inner_task_identity(INNER_TASK_TYPES[partition.dataset]):
            result = publisher.run(
                dataset=dataset,
                latest_session=False,
                start_date=partition.trade_date,
                end_date=partition.trade_date,
                expected_build_sha=self.expected_build_sha,
                apply=True,
                now=self.now.replace(tzinfo=None),
            )
        manifest = result.get("manifest")
        if (
            publisher.validate_task_result(result, 0) != "complete"
            or not isinstance(manifest, Mapping)
            or list(manifest.get("sessions") or []) != [partition.trade_date]
            or manifest.get("build_sha") != self.expected_build_sha
        ):
            raise CanonicalGapRepairBlocked(
                "index exact publisher result is incomplete"
            )
        return {
            "source_schema": result["schema"],
            "source_status": result["status"],
            "source_receipt_sha256": str(result["manifest_hash"]),
            "forward_observation_created": False,
        }

    def _etf(self, partition: PartitionRef) -> dict[str, Any]:
        from tools import run_etf_forward_daily as forward_publisher
        from tools import sync_etf_bigqmt_daily as market_publisher

        target = date.fromisoformat(partition.trade_date)
        if target < self.now.date():
            result = forward_publisher.run_daily(
                self.primary_engine,
                trade_date=partition.trade_date,
                expected_build_sha=self.expected_build_sha,
                now=self.now,
            )
            unsigned = dict(result)
            supplied = unsigned.pop("receipt_id", None)
            forward = result.get("forward_ledger")
            market = result.get("market_data")
            if (
                supplied != _digest(unsigned)
                or result.get("schema") != forward_publisher.RECEIPT_SCHEMA
                or result.get("status") != "PASS"
                or result.get("trade_date") != partition.trade_date
                or result.get("provider") != PROVIDER
                or result.get("executor_owner") != EDGE_ROLE
                or result.get("automatic_order_submission") is not False
                or not isinstance(forward, Mapping)
                or forward.get("status")
                != "NOT_RUN_HISTORICAL_BACKFILL_PROHIBITED"
                or forward.get("data_date") != partition.trade_date
                or not isinstance(market, Mapping)
                or market.get("status") != "PASS"
                or market.get("trade_date") != partition.trade_date
                or _SHA256.fullmatch(
                    str(market.get("receipt_id") or "")
                )
                is None
            ):
                raise CanonicalGapRepairBlocked(
                    "historical ETF publisher attempted an invalid forward phase"
                )
            source_hash = str(supplied)
            source_schema = str(result["schema"])
        else:
            # The gap repair owns market data only.  On a same-day closed
            # session it calls the exact market phase directly so it can never
            # create a strategy observation that belongs to the live task.
            market = market_publisher.run_sync(
                self.primary_engine,
                trade_date=partition.trade_date,
                expected_build_sha=self.expected_build_sha,
                now=self.now,
            )
            unsigned = dict(market)
            supplied = unsigned.pop("receipt_id", None)
            if (
                supplied != _digest(unsigned)
                or market.get("schema") != market_publisher.RECEIPT_SCHEMA
                or market.get("status") != "PASS"
                or market.get("trade_date") != partition.trade_date
                or market.get("provider") != PROVIDER
                or market.get("executor_owner") != EDGE_ROLE
                or market.get("automatic_order_submission") is not False
            ):
                raise CanonicalGapRepairBlocked(
                    "ETF market-only exact publisher result is incomplete"
                )
            source_hash = str(supplied)
            source_schema = str(market["schema"])
        if _SHA256.fullmatch(source_hash) is None:
            raise CanonicalGapRepairBlocked("ETF source receipt hash is invalid")
        return {
            "source_schema": source_schema,
            "source_status": "PASS",
            "source_receipt_sha256": source_hash,
            "forward_observation_created": False,
        }


def _inspection_failure(exc: BaseException) -> dict[str, str]:
    return {
        "error_type": type(exc).__name__,
        "error_sha256": _digest(str(exc)),
    }


def _inspect_all(
    plan: Sequence[PartitionRef],
    inspect_partition: Callable[[PartitionRef], Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, str]]]:
    exact: dict[str, dict[str, Any]] = {}
    missing: dict[str, dict[str, str]] = {}
    for partition in plan:
        try:
            proof = dict(inspect_partition(partition))
            if (
                proof.get("dataset") != partition.dataset
                or proof.get("trade_date") != partition.trade_date
                or int(proof.get("row_count") or 0) <= 0
                or _SHA256.fullmatch(str(proof.get("row_hash") or "")) is None
            ):
                raise CanonicalGapRepairBlocked(
                    "canonical partition proof identity differs"
                )
            exact[partition.partition_id] = proof
        except Exception as exc:  # noqa: BLE001 - a failed proof is a gap
            missing[partition.partition_id] = _inspection_failure(exc)
    return exact, missing


def _plan(window: CalendarWindow, datasets: Sequence[str]) -> list[PartitionRef]:
    return [
        PartitionRef(dataset=dataset, trade_date=trade_date)
        for trade_date in window.sessions
        for dataset in datasets
    ]


def _partition_root(exact: Mapping[str, Mapping[str, Any]]) -> str:
    entries = [
        {
            "partition_id": partition_id,
            "proof_sha256": _digest(exact[partition_id]),
        }
        for partition_id in sorted(exact)
    ]
    return _digest(entries)


def repair_recent_partitions(
    *,
    expected_build_sha: str,
    datasets: Sequence[str],
    lookback_sessions: int,
    max_repairs_per_run: int,
    apply: bool,
    now: datetime,
    window: CalendarWindow,
    inspect_partition: Callable[[PartitionRef], Mapping[str, Any]],
    publish_partition: Callable[[PartitionRef], Mapping[str, Any]],
) -> dict[str, Any]:
    started_at = _as_shanghai(now)
    selected = _normalized_datasets(datasets)
    if not selected:
        raise ValueError("at least one canonical repair dataset is required")
    if int(lookback_sessions) != len(window.sessions):
        raise ValueError("calendar window size differs from requested lookback")
    budget = int(max_repairs_per_run)
    if budget <= 0:
        raise ValueError("max repairs per run must be positive")
    if _SHA40.fullmatch(str(expected_build_sha)) is None:
        raise ValueError("expected build SHA must be lower SHA-1")

    plan = _plan(window, selected)
    plan_ids = [item.partition_id for item in plan]
    initial_exact, initial_missing = _inspect_all(plan, inspect_partition)
    attempts: list[dict[str, Any]] = []
    publisher_failed = False

    if apply:
        for partition in plan:
            if partition.partition_id not in initial_missing:
                continue
            if len(attempts) >= budget:
                break
            attempt: dict[str, Any] = {
                "partition_id": partition.partition_id,
                "before": initial_missing[partition.partition_id],
            }
            try:
                source = dict(publish_partition(partition))
                source_hash = str(source.get("source_receipt_sha256") or "")
                if (
                    _SHA256.fullmatch(source_hash) is None
                    or source.get("forward_observation_created") is not False
                ):
                    raise CanonicalGapRepairBlocked(
                        "exact publisher summary is not hash-bound/read-only-forward"
                    )
                proof = dict(inspect_partition(partition))
                if (
                    proof.get("dataset") != partition.dataset
                    or proof.get("trade_date") != partition.trade_date
                    or int(proof.get("row_count") or 0) <= 0
                    or _SHA256.fullmatch(str(proof.get("row_hash") or ""))
                    is None
                ):
                    raise CanonicalGapRepairBlocked(
                        "canonical post-publish readback remains incomplete"
                    )
                attempt.update(
                    status="REPAIRED",
                    source_receipt_sha256=source_hash,
                    canonical_proof_sha256=_digest(proof),
                )
            except Exception as exc:  # noqa: BLE001 - fail closed, resume later
                attempt.update(status="DATA_BLOCKED", **_inspection_failure(exc))
                publisher_failed = True
            attempts.append(attempt)
            # Partitions are independently atomic.  Isolate a provider/data
            # failure to this partition and spend the remaining bounded
            # budget on other sessions/datasets; the next run will re-scan
            # and retry only what is still incomplete.

    final_exact, final_missing = _inspect_all(plan, inspect_partition)
    remaining = sorted(final_missing)
    repaired_count = sum(item.get("status") == "REPAIRED" for item in attempts)
    complete = not remaining
    if complete:
        blocked_reason = None
    elif not apply:
        blocked_reason = "dry_run_missing_partitions"
    elif publisher_failed:
        blocked_reason = "exact_publisher_data_blocked"
    elif len(attempts) >= budget:
        blocked_reason = "repair_budget_exhausted"
    else:
        blocked_reason = "canonical_partitions_remain_incomplete"

    finished_at = max(_shanghai_now(), started_at)
    return _signed(
        {
            "schema": RESULT_SCHEMA,
            "status": "COMPLETE" if complete else "DATA_BLOCKED",
            "task_type": TASK_TYPE,
            "executor_owner": EDGE_ROLE,
            "provider": PROVIDER,
            "build_sha": expected_build_sha,
            "started_at": _timestamp(started_at),
            "finished_at": _timestamp(finished_at),
            "apply": bool(apply),
            "retryable": not complete,
            "lookback_sessions": int(lookback_sessions),
            "sessions": list(window.sessions),
            "session_set_sha256": _digest(list(window.sessions)),
            "calendar": window.as_dict(),
            "datasets": list(selected),
            "plan_partition_count": len(plan),
            "plan_partition_root_sha256": _digest(plan_ids),
            "exact_before_count": len(initial_exact),
            "candidate_before_count": len(initial_missing),
            "candidate_before_ids": sorted(initial_missing),
            "attempted_count": len(attempts),
            "repaired_count": repaired_count,
            "max_repairs_per_run": budget,
            "attempts": attempts,
            "exact_after_count": len(final_exact),
            "exact_partition_root_sha256": _partition_root(final_exact),
            "remaining_count": len(remaining),
            "remaining_partition_ids": remaining,
            "blocked_reason": blocked_reason,
            "automatic_order_submission": False,
        }
    )


def _failure_result(
    *,
    datasets: Sequence[str],
    lookback_sessions: int,
    max_repairs_per_run: int,
    apply: bool,
    build_sha: str,
    started_at: datetime,
    error: BaseException,
) -> dict[str, Any]:
    return _signed(
        {
            "schema": RESULT_SCHEMA,
            "status": "DATA_BLOCKED",
            "task_type": TASK_TYPE,
            "executor_owner": EDGE_ROLE,
            "provider": PROVIDER,
            "build_sha": build_sha or None,
            "started_at": _timestamp(started_at),
            "finished_at": _timestamp(_shanghai_now()),
            "apply": bool(apply),
            "retryable": True,
            "lookback_sessions": int(lookback_sessions),
            "sessions": [],
            "session_set_sha256": _digest([]),
            "calendar": None,
            "datasets": list(_normalized_datasets(datasets)),
            "plan_partition_count": 0,
            "plan_partition_root_sha256": _digest([]),
            "exact_before_count": 0,
            "candidate_before_count": 0,
            "candidate_before_ids": [],
            "attempted_count": 0,
            "repaired_count": 0,
            "max_repairs_per_run": int(max_repairs_per_run),
            "attempts": [],
            "exact_after_count": 0,
            "exact_partition_root_sha256": _digest([]),
            "remaining_count": 0,
            "remaining_partition_ids": [],
            "blocked_reason": "repair_precondition_data_blocked",
            "error_type": type(error).__name__,
            "error_sha256": _digest(str(error)),
            "automatic_order_submission": False,
        }
    )


def validate_task_result(payload: Mapping[str, Any], return_code: int) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        raise ValueError("canonical gap repair result schema differs")
    unsigned = dict(payload)
    supplied_hash = unsigned.pop("result_sha256", None)
    if supplied_hash != _digest(unsigned):
        raise ValueError("canonical gap repair result hash differs")
    status = str(payload.get("status") or "")
    try:
        datasets = _normalized_datasets(payload["datasets"])
        sessions = [date.fromisoformat(str(item)).isoformat() for item in payload["sessions"]]
        lookback = int(payload["lookback_sessions"])
        plan_count = int(payload["plan_partition_count"])
        exact_before = int(payload["exact_before_count"])
        candidates = int(payload["candidate_before_count"])
        attempted = int(payload["attempted_count"])
        repaired = int(payload["repaired_count"])
        exact_after = int(payload["exact_after_count"])
        remaining = int(payload["remaining_count"])
        budget = int(payload["max_repairs_per_run"])
        attempts = list(payload["attempts"])
        remaining_ids = list(payload["remaining_partition_ids"])
        candidate_ids = list(payload["candidate_before_ids"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("canonical gap repair result counters are invalid") from exc
    plan_ids = [
        PartitionRef(dataset=dataset, trade_date=trade_date).partition_id
        for trade_date in sessions
        for dataset in datasets
    ]
    valid_common = bool(
        payload.get("task_type") == TASK_TYPE
        and payload.get("executor_owner") == EDGE_ROLE
        and payload.get("provider") == PROVIDER
        and payload.get("automatic_order_submission") is False
        and isinstance(payload.get("apply"), bool)
        and budget > 0
        and sessions == sorted(set(sessions))
        and payload.get("session_set_sha256") == _digest(sessions)
        and plan_count == len(plan_ids)
        and payload.get("plan_partition_root_sha256") == _digest(plan_ids)
        and 0 <= exact_before <= plan_count
        and 0 <= candidates <= plan_count
        and exact_before + candidates == plan_count
        and candidate_ids == sorted(set(candidate_ids))
        and candidates == len(candidate_ids)
        and set(candidate_ids).issubset(plan_ids)
        and attempted == len(attempts) <= budget
        and 0 <= repaired <= attempted
        and 0 <= exact_after <= plan_count
        and 0 <= remaining <= plan_count
        and exact_after + remaining == plan_count
        and remaining_ids == sorted(set(remaining_ids))
        and remaining == len(remaining_ids)
        and set(remaining_ids).issubset(plan_ids)
        and _SHA256.fullmatch(
            str(payload.get("exact_partition_root_sha256") or "")
        )
        is not None
        and _parse_timestamp(payload.get("started_at"), field="started_at")
        <= _parse_timestamp(payload.get("finished_at"), field="finished_at")
    )
    if not valid_common:
        raise ValueError("canonical gap repair result contract differs")
    if sessions:
        calendar = payload.get("calendar")
        build_sha = str(payload.get("build_sha") or "")
        if (
            lookback != len(sessions)
            or _SHA40.fullmatch(build_sha) is None
            or build_sha == "0" * 40
            or not isinstance(calendar, Mapping)
            or not str(calendar.get("batch_id") or "")
            or any(
                _SHA256.fullmatch(str(calendar.get(field) or "")) is None
                for field in (
                    "manifest_hash",
                    "source_session_set_hash",
                    "selected_session_set_hash",
                )
            )
            or calendar.get("selected_session_set_hash") != _digest(sessions)
        ):
            raise ValueError("canonical gap repair authority contract differs")
    elif payload.get("calendar") is not None or plan_count != 0:
        raise ValueError("canonical gap repair empty authority contract differs")
    attempt_ids: list[str] = []
    repaired_attempts = 0
    for attempt in attempts:
        if (
            not isinstance(attempt, Mapping)
            or attempt.get("partition_id") not in candidate_ids
            or attempt.get("status") not in {"REPAIRED", "DATA_BLOCKED"}
        ):
            raise ValueError("canonical gap repair attempt identity differs")
        attempt_ids.append(str(attempt["partition_id"]))
        if attempt.get("status") == "REPAIRED" and (
            _SHA256.fullmatch(str(attempt.get("source_receipt_sha256") or ""))
            is None
            or _SHA256.fullmatch(str(attempt.get("canonical_proof_sha256") or ""))
            is None
        ):
            raise ValueError("canonical gap repair attempt proof differs")
        if attempt.get("status") == "REPAIRED":
            repaired_attempts += 1
    if len(attempt_ids) != len(set(attempt_ids)) or repaired != repaired_attempts:
        raise ValueError("canonical gap repair attempt counters differ")
    if status == "COMPLETE":
        if (
            int(return_code) != 0
            or payload.get("retryable") is not False
            or remaining != 0
            or exact_after != plan_count
            or payload.get("blocked_reason") is not None
        ):
            raise ValueError("canonical gap repair COMPLETE result differs")
        return "complete"
    if status == "DATA_BLOCKED":
        if (
            int(return_code) != 2
            or payload.get("retryable") is not True
            or not str(payload.get("blocked_reason") or "")
        ):
            raise ValueError("canonical gap repair DATA_BLOCKED result differs")
        return "blocked"
    raise ValueError("canonical gap repair status is invalid")


def parse_result(output: str | None) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for line in str(output or "").splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("schema") == RESULT_SCHEMA:
            matches.append(value)
    if len(matches) != 1:
        raise ValueError(
            "canonical gap repair output must contain exactly one machine result"
        )
    return matches[0]


def scheduler_output_status(output: str | None, *, return_code: int) -> str:
    try:
        payload = parse_result(output)
        disposition = validate_task_result(payload, return_code)
    except (TypeError, ValueError):
        return "failed"
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if expected_build and payload.get("build_sha") != expected_build:
        return "failed"
    return "success" if disposition == "complete" else "blocked"


def validate_persisted_result(
    primary_engine: Any,
    payload: Mapping[str, Any],
    *,
    history_engine: Any | None = None,
    minute_engine: Any | None = None,
    now: datetime | None = None,
    window_loader: Callable[..., CalendarWindow] = load_recent_closed_window,
    inspect_partition: Callable[[PartitionRef], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Pure-read replay of a COMPLETE repair receipt and all canonical rows."""

    if validate_task_result(payload, 0) != "complete":
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical repair receipt is not COMPLETE"
        )
    checked_at = _as_shanghai(now or _shanghai_now())
    finished_at = _parse_timestamp(payload["finished_at"], field="finished_at")
    if finished_at > checked_at + timedelta(minutes=5):
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical repair receipt is from the future"
        )
    environment_sha = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if environment_sha and environment_sha != payload.get("build_sha"):
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical repair build receipt is stale"
        )
    calendar = payload["calendar"]
    window = window_loader(
        primary_engine,
        now=finished_at,
        lookback_sessions=int(payload["lookback_sessions"]),
        batch_id=str(calendar["batch_id"]),
    )
    if (
        list(window.sessions) != list(payload["sessions"])
        or window.as_dict() != dict(calendar)
    ):
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical repair calendar replay differs"
        )
    owned_history = None
    owned_minute = None
    if inspect_partition is None:
        owned_history = history_engine or get_kline_engine()
        owned_minute = minute_engine or get_minute_engine()
        inspect_partition = CanonicalPartitionInspector(
            primary_engine,
            owned_history,
            owned_minute,
            window=window,
            decision_time=finished_at,
        )
    try:
        plan = _plan(window, _normalized_datasets(payload["datasets"]))
        exact, missing = _inspect_all(plan, inspect_partition)
    finally:
        owned_engines = []
        if history_engine is None and owned_history is not None:
            owned_engines.append(owned_history)
        if minute_engine is None and owned_minute is not None:
            owned_engines.append(owned_minute)
        disposed: set[int] = set()
        for owned_engine in owned_engines:
            if id(owned_engine) in disposed:
                continue
            disposed.add(id(owned_engine))
            dispose = getattr(owned_engine, "dispose", None)
            if callable(dispose):
                dispose()
    if missing or len(exact) != int(payload["plan_partition_count"]):
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical partition became incomplete after repair"
        )
    root_hash = _partition_root(exact)
    if root_hash != payload.get("exact_partition_root_sha256"):
        raise CanonicalGapRepairBlocked(
            "DATA_BLOCKED: canonical persisted proof root differs"
        )
    return {
        "status": "COMPLETE",
        "task_type": TASK_TYPE,
        "build_sha": payload["build_sha"],
        "sessions": list(window.sessions),
        "partition_count": len(exact),
        "exact_partition_root_sha256": root_hash,
        "checked_at": _timestamp(checked_at),
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=DATASET_ORDER,
        default=[],
        help="Repeat to limit repair scope; default repairs all history datasets.",
    )
    parser.add_argument("--lookback-sessions", type=int, default=5)
    parser.add_argument("--max-repairs-per-run", type=int, default=30)
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.lookback_sessions <= 0 or args.lookback_sessions > 120:
        parser.error("--lookback-sessions must be between 1 and 120")
    if args.max_repairs_per_run <= 0:
        parser.error("--max-repairs-per-run must be positive")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    datasets = _normalized_datasets(args.dataset or DATASET_ORDER)
    started_at = _shanghai_now()
    build_sha = ""
    primary_engine = None
    history_engine = None
    minute_engine = None
    try:
        validate_executor()
        validate_repair_clock(started_at)
        build_sha = _build_sha(args.expected_build_sha)
        load_project_env()
        primary_engine = create_batch_engine(future=True)
        history_engine = get_kline_engine()
        minute_engine = get_minute_engine()
        window = load_recent_closed_window(
            primary_engine,
            now=started_at,
            lookback_sessions=args.lookback_sessions,
        )
        inspector = CanonicalPartitionInspector(
            primary_engine,
            history_engine,
            minute_engine,
            window=window,
            decision_time=started_at,
        )
        publisher = ExactPartitionPublisher(
            primary_engine,
            expected_build_sha=build_sha,
            now=started_at,
            minute_engine=minute_engine,
        )
        result = repair_recent_partitions(
            expected_build_sha=build_sha,
            datasets=datasets,
            lookback_sessions=args.lookback_sessions,
            max_repairs_per_run=args.max_repairs_per_run,
            apply=bool(args.apply),
            now=started_at,
            window=window,
            inspect_partition=inspector,
            publish_partition=publisher,
        )
    except Exception as exc:  # noqa: BLE001 - emit one fail-closed receipt
        result = _failure_result(
            datasets=datasets,
            lookback_sessions=args.lookback_sessions,
            max_repairs_per_run=args.max_repairs_per_run,
            apply=bool(args.apply),
            build_sha=build_sha,
            started_at=started_at,
            error=exc,
        )
    finally:
        disposed: set[int] = set()
        for engine in (minute_engine, history_engine, primary_engine):
            if engine is None or id(engine) in disposed:
                continue
            disposed.add(id(engine))
            dispose = getattr(engine, "dispose", None)
            if callable(dispose):
                dispose()
    print(_canonical_json(result), flush=True)
    return 0 if result.get("status") == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
