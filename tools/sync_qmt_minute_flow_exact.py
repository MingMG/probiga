#!/usr/bin/env python3
"""Publish one catalog-complete native-QMT minute capital-flow history date.

Only QMT's historical ``transactioncount1m`` cumulative fields are accepted.
The publisher binds the request to an immutable target-date QMT stock catalog
and a completed daily-bar attestation, requires every traded stock to carry the
native 241-minute grid, proves nonzero VIP feature output, stages all rows in a
temporary table, and atomically replaces the date after a full readback.  It
never substitutes Eastmoney's current snapshot for missing history.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable, Iterable, Mapping, Sequence
import uuid
from zoneinfo import ZoneInfo

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt import bridge as qmt_bridge  # noqa: E402
from server.common.batch_db import quote_identifier  # noqa: E402
from server.common.minute_data import get_minute_engine  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from server.common.qmt_daily_market_truth import (  # noqa: E402
    QMT_DAILY_PROVIDER,
    load_qmt_daily_market_truth,
)
from server.common.qmt_history_coverage import (  # noqa: E402
    QMT_MINUTE_GRID_PROFILE,
    canonical_digest,
    minute_time_grid,
)
from server.common.qmt_stock_catalog import (  # noqa: E402
    load_target_stock_catalog,
    validate_stock_catalog_runtime_schema,
)
from tools.env_config import create_tool_engine, load_project_env  # noqa: E402


RESULT_SCHEMA = "probiga.qmt-minute-flow-result.v1"
TASK_TYPE = "qmt_stock_minute_flow_canonical"
EXECUTOR_OWNER = "qmt_windows_edge"
PROVIDER_ID = "gj_qmt_transactioncount1m"
QMT_PROVIDER_ID = "gj_qmt"
PERIOD = "transactioncount1m"
NATIVE_FIELDS = (
    "netInflowMostAmount",
    "netInflowBigAmount",
    "netInflowMediumAmount",
    "netInflowSmallAmount",
)
LOCK_NAME = "probiga:capital_flow_minute"
TABLE = "sm_stock_capital_flow_min"
SHANGHAI = ZoneInfo("Asia/Shanghai")
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA64 = re.compile(r"^[0-9a-f]{64}$")
QMT_CODE_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
MIN_NONZERO_CODE_RATIO = Decimal("0.20")
CODE_BATCH_SIZE = 100
WORKER_TIMEOUT_SECONDS = 900
WORKER = ROOT / "tools" / "qmt_minute_flow_exact_worker.py"
GRID = minute_time_grid(QMT_MINUTE_GRID_PROFILE)
GRID_HASH = canonical_digest(list(GRID))

FLOW_VALUE_COLUMNS = (
    "main_net_inflow",
    "max_net_inflow",
    "lg_net_inflow",
    "mid_net_inflow",
    "sm_net_inflow",
)
FLOW_INSERT_COLUMNS = (
    "stock_code",
    "trade_time",
    *FLOW_VALUE_COLUMNS,
    "snapshot_at",
    "etl_sync_at",
    "qmt_code",
    "data_source",
    "source_time",
    "received_at",
    "batch_id",
    "data_version",
    "quality_status",
    "permission_status",
)
FLOW_HASH_COLUMNS = FLOW_INSERT_COLUMNS


class MinuteFlowDataBlocked(RuntimeError):
    """The native historical flow date cannot be proven complete."""


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
    result["receipt_id"] = _digest(result)
    return result


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    return completed.stdout.strip().lower()


def resolve_build_sha(explicit: str = "") -> str:
    value = str(
        explicit
        or os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if SHA40.fullmatch(value) is None or value == "0" * 40:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: exact QMT minute-flow scheduler build SHA unavailable"
        )
    if _git_head() != value:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow checkout differs from scheduler build"
        )
    return value


def _validate_executor() -> None:
    role = str(os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") or "").strip()
    if role != EXECUTOR_OWNER:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: native minute-flow history is not running on QMT Windows edge"
        )
    task_type = str(os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") or "").strip()
    if task_type and task_type != TASK_TYPE:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow scheduler task type differs"
        )


def _iso_date(value: Any, *, field: str = "trade_date") -> str:
    raw = str(value or "").strip()[:10]
    try:
        normalized = date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise MinuteFlowDataBlocked(f"DATA_BLOCKED: {field} is invalid") from exc
    if raw != normalized:
        raise MinuteFlowDataBlocked(f"DATA_BLOCKED: {field} is invalid")
    return normalized


def _stock_code(value: Any) -> str:
    code = str(value or "").strip().split(".", 1)[0].zfill(6)
    if len(code) != 6 or not code.isdigit() or code == "000000":
        raise MinuteFlowDataBlocked(
            f"DATA_BLOCKED: invalid minute-flow stock code {value!r}"
        )
    return code


def _qmt_code(value: Any) -> str:
    code = str(value or "").strip().upper()
    if QMT_CODE_RE.fullmatch(code) is None:
        raise MinuteFlowDataBlocked(
            f"DATA_BLOCKED: invalid minute-flow QMT code {value!r}"
        )
    return code


def _decimal(value: Any, *, field: str, nonnegative: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MinuteFlowDataBlocked(
            f"DATA_BLOCKED: minute-flow {field} is not numeric"
        ) from exc
    if not number.is_finite() or (nonnegative and number < 0):
        raise MinuteFlowDataBlocked(
            f"DATA_BLOCKED: minute-flow {field} is invalid"
        )
    return number.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace("T", " ")[:19]
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise MinuteFlowDataBlocked(
                f"DATA_BLOCKED: minute-flow {field} is invalid"
            ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _canonical_value(value: Any, *, column: str) -> Any:
    if value is None:
        return None
    if column in FLOW_VALUE_COLUMNS:
        return format(_decimal(value, field=column), ".6f")
    if column in {
        "trade_time",
        "snapshot_at",
        "etl_sync_at",
        "source_time",
        "received_at",
    }:
        return _datetime(value, field=column).isoformat(sep=" ")
    return str(value)


def _canonical_flow_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        column: _canonical_value(row.get(column), column=column)
        for column in FLOW_HASH_COLUMNS
    }


def _code_set_hash(codes: Iterable[Any]) -> str:
    normalized = sorted({_stock_code(code) for code in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _qmt_code_set_hash(codes: Iterable[Any]) -> str:
    normalized = sorted({_qmt_code(code) for code in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _runtime_identity_is_valid(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    try:
        connection_port = int(value.get("connection_port"))
    except (TypeError, ValueError):
        return False
    return (
        0 < connection_port <= 65535
        and value.get("download_method")
        in {"download_history_data2", "download_history_data"}
        and value.get("count") == -1
        and value.get("fill_data") is True
        and value.get("fields") == list(NATIVE_FIELDS)
        and bool(str(value.get("sdk_module") or "").strip())
        and bool(str(value.get("sdk_version") or "").strip())
    )


@dataclass(frozen=True)
class FlowUniverse:
    trade_date: str
    qmt_by_stock: Mapping[str, str]
    catalog: Mapping[str, Any]
    daily_truth: Mapping[str, Any]
    all_stock_count: int
    traded_stock_count: int
    traded_stock_set_hash: str

    @property
    def qmt_codes(self) -> tuple[str, ...]:
        return tuple(self.qmt_by_stock[code] for code in sorted(self.qmt_by_stock))

    def receipt(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "catalog": dict(self.catalog),
            "daily_truth": dict(self.daily_truth),
            "all_stock_count": self.all_stock_count,
            "traded_stock_count": self.traded_stock_count,
            "traded_stock_set_hash": self.traded_stock_set_hash,
            "qmt_code_set_hash": _qmt_code_set_hash(self.qmt_codes),
            "suspension_rule": "daily volume=0 AND amount=0; flow omitted only then",
        }


def _rows(result: Any) -> list[dict[str, Any]]:
    mappings = getattr(result, "mappings", None)
    if callable(mappings):
        return [dict(row) for row in mappings().all()]
    return [dict(row) for row in result]


def load_flow_universe(engine: Any, *, trade_date: str, now: datetime) -> FlowUniverse:
    target = _iso_date(trade_date)
    validate_stock_catalog_runtime_schema(engine)
    catalog, expected_codes = load_target_stock_catalog(
        engine,
        target_date=target,
        decision_known_at=now.replace(tzinfo=None),
    )
    with engine.connect() as connection:
        truth = load_qmt_daily_market_truth(
            connection,
            start_date=target,
            end_date=target,
            decision_known_at=now.replace(tzinfo=None),
        )
        daily_rows = _rows(
            connection.execute(
                text(
                    """
                    SELECT stock_code,volume,amount,data_source,
                           quality_status,permission_status
                      FROM sm_stock_kline
                     WHERE trade_date=:trade_date
                       AND k_type=1 AND adjust_type=0
                     ORDER BY stock_code
                    """
                ),
                {"trade_date": target},
            )
        )
    expected = sorted({_stock_code(code) for code in expected_codes})
    if len(expected) != len(expected_codes):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: immutable minute-flow catalog contains duplicate codes"
        )
    actual_codes = [_stock_code(row.get("stock_code")) for row in daily_rows]
    if len(actual_codes) != len(set(actual_codes)) or set(actual_codes) != set(expected):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: canonical daily partition differs from minute-flow catalog"
        )
    if (
        truth.catalog_batch_id != catalog.batch_id
        or truth.catalog_manifest_hash != catalog.manifest_hash
        or truth.catalog_member_set_hash != catalog.member_set_hash
        or truth.attested_row_count != len(expected)
        or tuple(truth.requested_sessions) != (target,)
    ):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: minute-flow daily attestation/catalog roots differ"
        )
    traded: set[str] = set()
    for row, code in zip(daily_rows, actual_codes):
        if (
            str(row.get("data_source") or "") != QMT_DAILY_PROVIDER
            or str(row.get("quality_status") or "") != "QMT_ATTESTED"
            or str(row.get("permission_status") or "") != "SUPPORTED"
        ):
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: minute-flow prerequisite daily row is not canonical QMT"
            )
        volume = _decimal(row.get("volume"), field="daily volume", nonnegative=True)
        amount = _decimal(row.get("amount"), field="daily amount", nonnegative=True)
        if volume != 0 or amount != 0:
            traded.add(code)
    if not traded:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: target-date minute-flow traded universe is empty"
        )
    qmt_by_stock = {
        _stock_code(member["stock_code"]): _qmt_code(member["qmt_code"])
        for member in catalog.members
        if _stock_code(member["stock_code"]) in traded
        and str(member["list_date"]) <= target
        and (
            member.get("expire_date") in (None, "")
            or target < str(member["expire_date"])
        )
    }
    if set(qmt_by_stock) != traded or len(set(qmt_by_stock.values())) != len(traded):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: target-date traded stocks lack exact QMT instrument identities"
        )
    return FlowUniverse(
        trade_date=target,
        qmt_by_stock=dict(sorted(qmt_by_stock.items())),
        catalog={
            "batch_id": catalog.batch_id,
            "manifest_hash": catalog.manifest_hash,
            "member_set_hash": catalog.member_set_hash,
            "captured_at": catalog.captured_at,
            "history_complete_from": catalog.history_complete_from,
        },
        daily_truth={
            "run_id": truth.run_id,
            "run_finished_at": truth.run_finished_at,
            "calendar_batch_id": truth.calendar_batch_id,
            "calendar_manifest_hash": truth.calendar_manifest_hash,
            "truth_hash": truth.truth_hash,
        },
        all_stock_count=len(expected),
        traded_stock_count=len(traded),
        traded_stock_set_hash=_code_set_hash(traded),
    )


class ExactQmtFlowWorker:
    """Execute the reviewed one-shot worker in the configured QMT runtime."""

    def __init__(
        self,
        *,
        expected_build_sha: str,
        python_path: Path | None = None,
        worker_path: Path = WORKER,
        timeout_seconds: int = WORKER_TIMEOUT_SECONDS,
        runner: Callable[..., Any] = subprocess.run,
    ) -> None:
        self.expected_build_sha = expected_build_sha
        self.python_path = Path(python_path or qmt_bridge.python_path()).resolve()
        self.worker_path = Path(worker_path).resolve()
        self.timeout_seconds = max(30, int(timeout_seconds))
        self.runner = runner
        if not self.python_path.is_file():
            raise MinuteFlowDataBlocked(
                f"DATA_BLOCKED: QMT Python runtime unavailable: {self.python_path}"
            )
        if not self.worker_path.is_file() or not self.worker_path.is_relative_to(ROOT):
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: exact minute-flow worker is unavailable"
            )
        self.worker_sha256 = hashlib.sha256(self.worker_path.read_bytes()).hexdigest()

    def identity(self) -> dict[str, Any]:
        current_hash = hashlib.sha256(self.worker_path.read_bytes()).hexdigest()
        if current_hash != self.worker_sha256:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: minute-flow worker changed during collection"
            )
        return {
            "build_sha": self.expected_build_sha,
            "worker_path": str(self.worker_path.relative_to(ROOT)).replace("\\", "/"),
            "worker_sha256": current_hash,
            "period": PERIOD,
            "field_semantics": "native cumulative non-Dx netInflow*Amount",
            "count": -1,
            "fill_data": True,
        }

    def fetch(self, qmt_codes: Sequence[str], *, trade_date: str) -> Mapping[str, Any]:
        requested = sorted(_qmt_code(code) for code in qmt_codes)
        payload = {
            "action": "flow_min_exact",
            "trade_date": _iso_date(trade_date),
            "qmt_codes": requested,
            "history_wait_seconds": 1.0,
        }
        environment = os.environ.copy()
        environment["PYTHONIOENCODING"] = "utf-8"
        environment["PROBIGA_BUILD_COMMIT_SHA"] = self.expected_build_sha
        try:
            completed = self.runner(
                [str(self.python_path), str(self.worker_path)],
                input=_canonical_json(payload),
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                cwd=str(ROOT),
                env=environment,
                timeout=self.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise MinuteFlowDataBlocked(
                f"DATA_BLOCKED: exact QMT minute-flow batch timed out after "
                f"{self.timeout_seconds}s"
            ) from exc
        json_lines = [
            line.strip()
            for line in str(completed.stdout or "").splitlines()
            if line.strip().startswith("{") and line.strip().endswith("}")
        ]
        if not json_lines:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: QMT minute-flow worker returned no machine result"
            )
        try:
            result = json.loads(json_lines[-1])
        except json.JSONDecodeError as exc:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: QMT minute-flow worker result is malformed"
            ) from exc
        if completed.returncode != 0 or not isinstance(result, Mapping) or result.get("ok") is not True:
            error = str(result.get("error") if isinstance(result, Mapping) else "")[:500]
            raise MinuteFlowDataBlocked(
                f"DATA_BLOCKED: QMT minute-flow source unavailable: {error or 'worker failed'}"
            )
        return result


def _flow_time(value: Any, *, trade_date: str) -> datetime:
    parsed = _datetime(value, field="trade_time")
    if parsed.date().isoformat() != trade_date or parsed.second != 0:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow row is outside exact date/minute boundary"
        )
    return parsed


def normalize_flow_batch(
    response: Mapping[str, Any],
    *,
    expected_qmt_codes: Sequence[str],
    qmt_to_stock: Mapping[str, str],
    trade_date: str,
    observed_at: datetime,
    batch_id: str,
    build_sha: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    requested = sorted(_qmt_code(code) for code in expected_qmt_codes)
    runtime_identity = response.get("source_identity")
    if (
        response.get("provider") != QMT_PROVIDER_ID
        or response.get("period") != PERIOD
        or response.get("trade_date") != trade_date
        or int(response.get("requested_qmt_code_count") or 0) != len(requested)
        or response.get("requested_qmt_code_set_hash") != _qmt_code_set_hash(requested)
        or not isinstance(runtime_identity, Mapping)
    ):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow batch request/response identity differs"
        )
    if not _runtime_identity_is_valid(runtime_identity):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow runtime/source contract differs"
        )
    raw_rows = response.get("rows")
    if not isinstance(raw_rows, list) or int(response.get("row_count") or -1) != len(raw_rows):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow batch row counter differs"
        )
    expected_set = set(requested)
    grouped: dict[str, list[dict[str, Any]]] = {code: [] for code in requested}
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            raise MinuteFlowDataBlocked("DATA_BLOCKED: malformed QMT minute-flow row")
        qmt_code = _qmt_code(raw.get("qmt_code"))
        if qmt_code not in expected_set:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: QMT minute-flow batch returned an unexpected instrument"
            )
        stock_code = _stock_code(raw.get("stock_code"))
        if stock_code != qmt_to_stock.get(qmt_code) or qmt_code[:6] != stock_code:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: QMT minute-flow instrument identity differs"
            )
        trade_time = _flow_time(raw.get("trade_time"), trade_date=trade_date)
        maximum = _decimal(raw.get("netInflowMostAmount"), field="netInflowMostAmount")
        large = _decimal(raw.get("netInflowBigAmount"), field="netInflowBigAmount")
        middle = _decimal(raw.get("netInflowMediumAmount"), field="netInflowMediumAmount")
        small = _decimal(raw.get("netInflowSmallAmount"), field="netInflowSmallAmount")
        grouped[qmt_code].append(
            {
                "stock_code": stock_code,
                "trade_time": trade_time,
                "main_net_inflow": maximum + large,
                "max_net_inflow": maximum,
                "lg_net_inflow": large,
                "mid_net_inflow": middle,
                "sm_net_inflow": small,
                "snapshot_at": observed_at,
                "etl_sync_at": observed_at,
                "qmt_code": qmt_code,
                "data_source": PROVIDER_ID,
                "source_time": trade_time,
                "received_at": observed_at,
                "batch_id": batch_id,
                "data_version": build_sha,
                "quality_status": "QMT_NATIVE_EXACT",
                "permission_status": "SUPPORTED",
            }
        )
    normalized: list[dict[str, Any]] = []
    for qmt_code in requested:
        rows = sorted(grouped[qmt_code], key=lambda row: row["trade_time"])
        times = [row["trade_time"].strftime("%H:%M:%S") for row in rows]
        if times != list(GRID):
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: QMT transactioncount1m grid differs: "
                f"qmt_code={qmt_code},rows={len(times)},"
                f"first={times[:3]},last={times[-3:]}"
            )
        normalized.extend(rows)
    normalized.sort(key=lambda row: (row["stock_code"], row["trade_time"]))
    return normalized, dict(runtime_identity)


class FlowProofAccumulator:
    """Streaming canonical hash plus exact per-code minute-grid proof."""

    def __init__(self) -> None:
        self._hash = hashlib.sha256()
        self._row_count = 0
        self._codes: set[str] = set()
        self._nonzero_codes: set[str] = set()
        self._previous_identity: tuple[str, datetime] | None = None
        self._current_code = ""
        self._current_times: list[str] = []

    def _finish_code(self) -> None:
        if not self._current_code:
            return
        if self._current_times != list(GRID):
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: persisted minute-flow grid differs for "
                f"{self._current_code}: rows={len(self._current_times)}"
            )

    def add(self, row: Mapping[str, Any]) -> None:
        code = _stock_code(row.get("stock_code"))
        trade_time = _datetime(row.get("trade_time"), field="trade_time")
        identity = (code, trade_time)
        if self._previous_identity is not None and identity <= self._previous_identity:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: minute-flow rows are duplicated or not canonical-order"
            )
        if self._current_code and code != self._current_code:
            self._finish_code()
            self._current_times = []
        self._current_code = code
        self._current_times.append(trade_time.strftime("%H:%M:%S"))
        self._codes.add(code)
        values = [_decimal(row.get(column), field=column) for column in FLOW_VALUE_COLUMNS]
        if values[0] != values[1] + values[2]:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: minute-flow main amount differs from max+large"
            )
        if any(value != 0 for value in values):
            self._nonzero_codes.add(code)
        canonical = _canonical_flow_row(row)
        self._hash.update(_canonical_json(canonical).encode("utf-8"))
        self._hash.update(b"\n")
        self._row_count += 1
        self._previous_identity = identity

    def finish(self) -> dict[str, Any]:
        self._finish_code()
        code_count = len(self._codes)
        ratio = Decimal(len(self._nonzero_codes)) / Decimal(max(code_count, 1))
        return {
            "row_count": self._row_count,
            "row_hash": self._hash.hexdigest(),
            "code_count": code_count,
            "code_set_hash": _code_set_hash(self._codes),
            "minute_grid_profile": QMT_MINUTE_GRID_PROFILE,
            "minute_grid_count": len(GRID),
            "minute_grid_hash": GRID_HASH,
            "nonzero_code_count": len(self._nonzero_codes),
            "nonzero_code_ratio": float(ratio),
        }


def proof_from_rows(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    accumulator = FlowProofAccumulator()
    for row in rows:
        accumulator.add(row)
    return accumulator.finish()


def _stream_table_proof(connection: Any, *, table: str, trade_date: str) -> dict[str, Any]:
    target = quote_identifier(table)
    result = connection.execution_options(stream_results=True).execute(
        text(
            f"SELECT {','.join(quote_identifier(column) for column in FLOW_HASH_COLUMNS)} "
            f"FROM {target} "
            "WHERE trade_time>=:trade_date "
            "AND trade_time<DATE_ADD(:trade_date,INTERVAL 1 DAY) "
            "ORDER BY stock_code,trade_time"
        ),
        {"trade_date": trade_date},
    )
    accumulator = FlowProofAccumulator()
    try:
        for row in result.mappings():
            accumulator.add(dict(row))
    finally:
        result.close()
    return accumulator.finish()


def _same_proof(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    keys = (
        "row_count",
        "row_hash",
        "code_count",
        "code_set_hash",
        "minute_grid_profile",
        "minute_grid_count",
        "minute_grid_hash",
        "nonzero_code_count",
        "nonzero_code_ratio",
    )
    return all(actual.get(key) == expected.get(key) for key in keys)


def _create_stage(connection: Any) -> str:
    stage = f"{TABLE}_exact_{uuid.uuid4().hex[:12]}"
    connection.execute(
        text(
            f"CREATE TEMPORARY TABLE {quote_identifier(stage)} "
            f"LIKE {quote_identifier(TABLE)}"
        )
    )
    connection.commit()
    return stage


def _insert_statement(table: str) -> Any:
    columns = ",".join(quote_identifier(column) for column in FLOW_INSERT_COLUMNS)
    values = ",".join(f":{column}" for column in FLOW_INSERT_COLUMNS)
    return text(
        f"INSERT INTO {quote_identifier(table)} ({columns}) VALUES ({values})"
    )


def _append_stage(connection: Any, *, stage: str, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise MinuteFlowDataBlocked("DATA_BLOCKED: refusing an empty flow stage batch")
    statement = _insert_statement(stage)
    prepared = [
        {column: row.get(column) for column in FLOW_INSERT_COLUMNS}
        for row in rows
    ]
    with connection.begin():
        for offset in range(0, len(prepared), 1000):
            connection.execute(statement, prepared[offset : offset + 1000])


def _publish_stage(
    minute_engine: Any,
    connection: Any,
    *,
    stage: str,
    trade_date: str,
    expected: Mapping[str, Any],
) -> dict[str, Any]:
    stage_proof = _stream_table_proof(
        connection,
        table=stage,
        trade_date=trade_date,
    )
    if not _same_proof(stage_proof, expected):
        raise MinuteFlowDataBlocked("DATA_BLOCKED: minute-flow stage readback differs")
    if connection.in_transaction():
        connection.commit()
    with mysql_named_lock(
        minute_engine,
        LOCK_NAME,
        timeout_seconds=30,
        connection=connection,
    ):
        if connection.in_transaction():
            connection.commit()
        columns = ",".join(quote_identifier(column) for column in FLOW_INSERT_COLUMNS)
        with connection.begin():
            connection.execute(
                text(
                    f"DELETE FROM {quote_identifier(TABLE)} "
                    "WHERE trade_time>=:trade_date "
                    "AND trade_time<DATE_ADD(:trade_date,INTERVAL 1 DAY)"
                ),
                {"trade_date": trade_date},
            )
            inserted = connection.execute(
                text(
                    f"INSERT INTO {quote_identifier(TABLE)} ({columns}) "
                    f"SELECT {columns} FROM {quote_identifier(stage)}"
                )
            )
            if (
                inserted.rowcount is not None
                and inserted.rowcount >= 0
                and int(inserted.rowcount) != int(expected["row_count"])
            ):
                raise MinuteFlowDataBlocked(
                    "DATA_BLOCKED: minute-flow inserted row count differs"
                )
            transaction_proof = _stream_table_proof(
                connection,
                table=TABLE,
                trade_date=trade_date,
            )
            if not _same_proof(transaction_proof, expected):
                raise MinuteFlowDataBlocked(
                    "DATA_BLOCKED: minute-flow transaction readback differs"
                )
    with minute_engine.connect() as committed_connection:
        committed = _stream_table_proof(
            committed_connection,
            table=TABLE,
            trade_date=trade_date,
        )
    if not _same_proof(committed, expected):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: committed minute-flow partition differs"
        )
    return committed


def validate_runtime_schema(primary_engine: Any, minute_engine: Any) -> dict[str, Any]:
    validate_stock_catalog_runtime_schema(primary_engine)
    required = {"id", *FLOW_INSERT_COLUMNS}
    with minute_engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT COLUMN_NAME
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA=DATABASE()
                   AND TABLE_NAME='sm_stock_capital_flow_min'
                 ORDER BY ORDINAL_POSITION
                """
            )
        ).fetchall()
    observed = [str(row[0]) for row in rows]
    missing = sorted(required - set(observed))
    if missing:
        raise MinuteFlowDataBlocked(
            f"DATA_BLOCKED: minute-flow runtime schema differs: {missing}"
        )
    return {"schema_hash": _digest(observed), "columns": observed}


def _require_closed_session(*, trade_date: str, now: datetime) -> None:
    target = _iso_date(trade_date)
    if target > now.date().isoformat():
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: minute-flow target date is in the future"
        )
    if target == now.date().isoformat() and now.time() < wall_time(15, 10):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: current minute-flow session is not final"
        )


def resolve_requested_trade_date(
    engine: Any,
    *,
    trade_date: str,
    latest_session: bool,
    now: datetime,
) -> str:
    """Resolve scheduler convenience to one explicit, closed calendar date."""

    if bool(str(trade_date or "").strip()) == bool(latest_session):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: choose exactly one of trade_date/latest_session"
        )
    if trade_date:
        return _iso_date(trade_date)
    current = now
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.astimezone(SHANGHAI)
    latest_allowed = current.date()
    if current.time() < wall_time(15, 10):
        latest_allowed -= timedelta(days=1)
    with engine.connect() as connection:
        observed = connection.execute(
            text(
                "SELECT MAX(trade_date) FROM si_trade_calendar "
                "WHERE trade_status=1 AND trade_date<=:latest_allowed"
            ),
            {"latest_allowed": latest_allowed.isoformat()},
        ).scalar()
    if observed is None:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: calendar has no closed minute-flow session"
        )
    return _iso_date(observed)


def _chunks(values: Sequence[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), size):
        yield list(values[offset : offset + size])


def run_sync(
    primary_engine: Any,
    minute_engine: Any,
    *,
    trade_date: str,
    apply: bool,
    expected_build_sha: str,
    provider: ExactQmtFlowWorker | None = None,
    now: datetime | None = None,
    batch_size: int = CODE_BATCH_SIZE,
) -> dict[str, Any]:
    if batch_size != CODE_BATCH_SIZE:
        raise MinuteFlowDataBlocked(
            f"DATA_BLOCKED: production minute-flow batch size is fixed at {CODE_BATCH_SIZE}"
        )
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.astimezone(SHANGHAI).replace(microsecond=0)
    target = _iso_date(trade_date)
    build_sha = resolve_build_sha(expected_build_sha)
    _validate_executor()
    _require_closed_session(trade_date=target, now=current)
    schema = validate_runtime_schema(primary_engine, minute_engine)
    universe = load_flow_universe(
        primary_engine,
        trade_date=target,
        now=current,
    )
    worker = provider or ExactQmtFlowWorker(expected_build_sha=build_sha)
    worker_identity_before = worker.identity()
    batch_id = _digest(
        {
            "schema": RESULT_SCHEMA,
            "trade_date": target,
            "build_sha": build_sha,
            "catalog_manifest_hash": universe.catalog["manifest_hash"],
            "daily_truth_hash": universe.daily_truth["truth_hash"],
            "started_at": current.isoformat(),
        }
    )
    qmt_to_stock = {
        qmt_code: stock_code
        for stock_code, qmt_code in universe.qmt_by_stock.items()
    }
    source_accumulator = FlowProofAccumulator()
    source_identity: dict[str, Any] | None = None
    connection = minute_engine.connect() if apply else None
    stage = ""
    try:
        if connection is not None:
            stage = _create_stage(connection)
        for batch in _chunks(list(universe.qmt_codes), CODE_BATCH_SIZE):
            response = worker.fetch(batch, trade_date=target)
            normalized, observed_identity = normalize_flow_batch(
                response,
                expected_qmt_codes=batch,
                qmt_to_stock=qmt_to_stock,
                trade_date=target,
                observed_at=current.replace(tzinfo=None),
                batch_id=batch_id,
                build_sha=build_sha,
            )
            if source_identity is None:
                source_identity = observed_identity
            elif observed_identity != source_identity:
                raise MinuteFlowDataBlocked(
                    "DATA_BLOCKED: QMT SDK/source identity changed between batches"
                )
            for row in normalized:
                source_accumulator.add(row)
            if connection is not None:
                _append_stage(connection, stage=stage, rows=normalized)
        source_proof = source_accumulator.finish()
        if (
            source_proof["code_count"] != universe.traded_stock_count
            or source_proof["code_set_hash"] != universe.traded_stock_set_hash
            or source_proof["row_count"]
            != universe.traded_stock_count * len(GRID)
        ):
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: full-market minute-flow source coverage differs"
            )
        if Decimal(str(source_proof["nonzero_code_ratio"])) < MIN_NONZERO_CODE_RATIO:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: QMT transactioncount1m lacks nonzero VIP field evidence: "
                f"ratio={source_proof['nonzero_code_ratio']:.6f} "
                f"required={float(MIN_NONZERO_CODE_RATIO):.6f}"
            )
        worker_identity_after = worker.identity()
        if worker_identity_after != worker_identity_before:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: minute-flow worker identity changed during collection"
            )
        if source_identity is None:
            raise MinuteFlowDataBlocked(
                "DATA_BLOCKED: minute-flow source identity is empty"
            )
        if apply:
            assert connection is not None and stage
            database = _publish_stage(
                minute_engine,
                connection,
                stage=stage,
                trade_date=target,
                expected=source_proof,
            )
            status = "PASS"
        else:
            database = {**source_proof, "not_written": True}
            status = "DRY_RUN"
    finally:
        if connection is not None:
            connection.close()
    finished = datetime.now(SHANGHAI).replace(microsecond=0)
    return _signed(
        {
            "schema": RESULT_SCHEMA,
            "status": status,
            "task_type": TASK_TYPE,
            "dataset": "stock_minute_capital_flow",
            "executor_owner": EXECUTOR_OWNER,
            "provider": PROVIDER_ID,
            "trade_date": target,
            "build_sha": build_sha,
            "started_at": current.isoformat(),
            "finished_at": finished.isoformat(),
            "batch_id": batch_id,
            "runtime_schema_hash": schema["schema_hash"],
            "universe": universe.receipt(),
            "source_identity": {
                **worker_identity_after,
                "qmt_runtime": source_identity,
            },
            "collection": source_proof,
            "database": database,
        }
    )


def _failure(*, trade_date: str, error: BaseException) -> dict[str, Any]:
    worker_hash = (
        hashlib.sha256(WORKER.read_bytes()).hexdigest()
        if WORKER.is_file()
        else ""
    )
    message = str(error)
    terminal_markers = (
        "scheduler build SHA unavailable",
        "checkout differs",
        "not running on QMT Windows edge",
        "scheduler task type differs",
        "QMT Python runtime unavailable",
        "exact minute-flow worker is unavailable",
        "worker changed",
        "runtime schema differs",
        "runtime/source contract differs",
        "lacks nonzero VIP field evidence",
        "production minute-flow batch size is fixed",
        "choose exactly one of trade_date/latest_session",
    )
    retryable = not any(marker in message for marker in terminal_markers)
    return _signed(
        {
            "schema": RESULT_SCHEMA,
            "status": "DATA_BLOCKED",
            "task_type": TASK_TYPE,
            "dataset": "stock_minute_capital_flow",
            "executor_owner": EXECUTOR_OWNER,
            "provider": PROVIDER_ID,
            "trade_date": str(trade_date or "")[:10],
            "period": PERIOD,
            "worker_sha256": worker_hash,
            "retryable": retryable,
            "error_type": type(error).__name__,
            "error": message[:1000],
        }
    )


def validate_task_result(payload: Mapping[str, Any], return_code: int) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        return "failed"
    unsigned = dict(payload)
    supplied = unsigned.pop("receipt_id", None)
    if supplied != _digest(unsigned):
        return "failed"
    if payload.get("status") == "DATA_BLOCKED" and int(return_code) == 2:
        if payload.get("retryable") is True:
            return "failed"
        if payload.get("retryable") is False:
            return "blocked"
        return "failed"
    if payload.get("status") != "PASS" or int(return_code) != 0:
        return "failed"
    try:
        target = _iso_date(payload["trade_date"])
        build_sha = str(payload["build_sha"])
        universe = payload["universe"]
        source = payload["collection"]
        database = payload["database"]
        identity = payload["source_identity"]
    except (KeyError, TypeError, MinuteFlowDataBlocked):
        return "failed"
    valid = (
        payload.get("task_type") == TASK_TYPE
        and payload.get("dataset") == "stock_minute_capital_flow"
        and payload.get("executor_owner") == EXECUTOR_OWNER
        and payload.get("provider") == PROVIDER_ID
        and target == payload.get("trade_date")
        and SHA40.fullmatch(build_sha) is not None
        and isinstance(universe, Mapping)
        and isinstance(source, Mapping)
        and isinstance(database, Mapping)
        and isinstance(identity, Mapping)
        and identity.get("build_sha") == build_sha
        and SHA64.fullmatch(str(identity.get("worker_sha256") or "")) is not None
        and identity.get("period") == PERIOD
        and identity.get("count") == -1
        and identity.get("fill_data") is True
        and _runtime_identity_is_valid(identity.get("qmt_runtime"))
    )
    if not valid or not _same_proof(source, database):
        return "failed"
    try:
        row_count = int(source["row_count"])
        code_count = int(source["code_count"])
        nonzero_ratio = Decimal(str(source["nonzero_code_ratio"]))
        expected_codes = int(universe["traded_stock_count"])
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return "failed"
    if (
        code_count != expected_codes
        or row_count != code_count * len(GRID)
        or source.get("code_set_hash") != universe.get("traded_stock_set_hash")
        or source.get("minute_grid_profile") != QMT_MINUTE_GRID_PROFILE
        or int(source.get("minute_grid_count") or 0) != len(GRID)
        or source.get("minute_grid_hash") != GRID_HASH
        or SHA64.fullmatch(str(source.get("row_hash") or "")) is None
        or nonzero_ratio < MIN_NONZERO_CODE_RATIO
    ):
        return "failed"
    return "complete"


def validate_persisted_result(
    primary_engine: Any,
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
    minute_engine: Any | None = None,
    expected_session: str = "",
) -> dict[str, Any]:
    """Rebuild the prerequisite universe and stream the committed date again."""

    if validate_task_result(payload, 0) != "complete":
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow task receipt is invalid"
        )
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.astimezone(SHANGHAI).replace(microsecond=0)
    finished = _datetime(payload.get("finished_at"), field="finished_at")
    if finished > current.replace(tzinfo=None) + timedelta(minutes=5):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow receipt is from the future"
        )
    if _git_head() != str(payload.get("build_sha") or ""):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: persisted QMT minute-flow build differs from checkout"
        )
    target = _iso_date(payload["trade_date"])
    expected = str(expected_session or "").strip()
    expected_target = resolve_requested_trade_date(
        primary_engine,
        trade_date=expected,
        latest_session=not bool(expected),
        now=current,
    )
    if target != expected_target:
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: stale QMT minute-flow session receipt replay"
        )
    truth_time = _datetime(
        payload["universe"]["daily_truth"]["run_finished_at"],
        field="daily_truth.run_finished_at",
    )
    universe = load_flow_universe(
        primary_engine,
        trade_date=target,
        now=truth_time,
    )
    if universe.receipt() != dict(payload["universe"]):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: persisted minute-flow prerequisite universe differs"
        )
    target_minute_engine = minute_engine or get_minute_engine()
    owns_minute_engine = minute_engine is None
    try:
        validate_runtime_schema(primary_engine, target_minute_engine)
        with target_minute_engine.connect() as connection:
            observed = _stream_table_proof(
                connection,
                table=TABLE,
                trade_date=target,
            )
    finally:
        if owns_minute_engine:
            target_minute_engine.dispose()
    if not _same_proof(observed, payload["database"]):
        raise MinuteFlowDataBlocked(
            "DATA_BLOCKED: persisted minute-flow database receipt differs"
        )
    return {
        "trade_date": target,
        "row_count": observed["row_count"],
        "code_count": observed["code_count"],
        "row_hash": observed["row_hash"],
        "minute_grid_hash": observed["minute_grid_hash"],
        "catalog_manifest_hash": universe.catalog["manifest_hash"],
        "daily_truth_hash": universe.daily_truth["truth_hash"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--trade-date", default="")
    date_group.add_argument("--latest-session", action="store_true")
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--code-batch-size", type=int, default=CODE_BATCH_SIZE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    target = args.trade_date
    try:
        load_project_env()
        primary = create_tool_engine()
        minute = get_minute_engine()
        try:
            current = datetime.now(SHANGHAI).replace(microsecond=0)
            target = resolve_requested_trade_date(
                primary,
                trade_date=args.trade_date,
                latest_session=args.latest_session,
                now=current,
            )
            result = run_sync(
                primary,
                minute,
                trade_date=target,
                apply=args.apply,
                expected_build_sha=args.expected_build_sha,
                now=current,
                batch_size=args.code_batch_size,
            )
        finally:
            primary.dispose()
            if minute is not primary:
                minute.dispose()
        code = 0
    except Exception as exc:
        result = _failure(trade_date=target, error=exc)
        code = 2
    print(_canonical_json(result), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
