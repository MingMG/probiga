#!/usr/bin/env python3
"""Publish canonical stock daily/minute partitions from exact-main BigQMT."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt import bridge
from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine
from server.common.qmt_history_coverage import (
    COVERAGE_ENTITY_TABLE,
    QMT_MINUTE_GRID_PROFILE,
    canonical_digest,
    minute_time_grid,
    require_exact_coverage,
)
from server.common.qmt_attestation_contract import expected_stock_set_contract
from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from tools.run_qmt_windows_edge_release_bootstrap import (
    validate_bigqmt_strategy_release,
)
from tools.sync_qmt_primary import run_dataset


RESULT_SCHEMA = "probiga.qmt-stock-edge-result.v1"
PROVIDER = "gj_big_qmt_inner"
EDGE_ROLE = "qmt_windows_edge"
TASK_TYPES = {
    "daily": "qmt_stock_daily_canonical",
    "minute": "qmt_stock_minute_canonical",
}
SHANGHAI = ZoneInfo("Asia/Shanghai")
SHA40 = re.compile(r"[0-9a-f]{40}")
STOCK_HISTORY_READY_TIME = time(15, 5)


class StockDataBlocked(RuntimeError):
    pass


def _now() -> datetime:
    return datetime.now(SHANGHAI).replace(tzinfo=None, microsecond=0)


def _digest(value: Any) -> str:
    return canonical_digest(value)


def _signed(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["receipt_id"] = _digest(result)
    return result


def _build_sha(explicit: str = "") -> str:
    value = str(
        explicit
        or os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if SHA40.fullmatch(value) is None or value == "0" * 40:
        raise StockDataBlocked("DATA_BLOCKED: exact scheduler build SHA unavailable")
    return value


def _validate_executor(dataset: str) -> None:
    if str(os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") or "").strip() != EDGE_ROLE:
        raise StockDataBlocked("DATA_BLOCKED: canonical stock publisher is not on QMT edge")
    observed = str(os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") or "").strip()
    if observed and observed != TASK_TYPES[dataset]:
        raise StockDataBlocked("DATA_BLOCKED: scheduler task type differs")


def _release(build_sha: str) -> dict[str, Any]:
    try:
        return validate_bigqmt_strategy_release(
            bridge.capabilities(timeout=180),
            expected_build_sha=build_sha,
        )
    except Exception as exc:
        raise StockDataBlocked(
            "DATA_BLOCKED: exact-main frozen BigQMT release unavailable"
        ) from exc


def _release_identity(proof: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: proof.get(key)
        for key in (
            "strategy_release_protocol",
            "strategy_identity_protocol",
            "strategy_identity_frozen",
            "strategy_build_sha",
            "strategy_git_blob",
            "strategy_source_sha256",
            "strategy_artifact_sha256",
            "strategy_loaded_identity_sha256",
        )
    }


def _sessions(
    engine: Any,
    *,
    dataset: str,
    latest_session: bool,
    start_date: str,
    end_date: str,
    now: datetime,
) -> tuple[Any, list[str]]:
    if dataset not in TASK_TYPES:
        raise StockDataBlocked("DATA_BLOCKED: unsupported stock dataset")
    current = now
    if current.tzinfo is not None:
        current = current.astimezone(SHANGHAI).replace(tzinfo=None)
    today = current.date().isoformat()
    if latest_session:
        latest_allowed = current.date()
        if current.time() < STOCK_HISTORY_READY_TIME:
            latest_allowed -= timedelta(days=1)
        start = (latest_allowed - timedelta(days=14)).isoformat()
        end = latest_allowed.isoformat()
    else:
        try:
            start = date.fromisoformat(start_date).isoformat()
            end = date.fromisoformat(end_date).isoformat()
        except ValueError as exc:
            raise StockDataBlocked("DATA_BLOCKED: stock target range invalid") from exc
        if start > end or end > today:
            raise StockDataBlocked("DATA_BLOCKED: stock target range invalid")
    try:
        with engine.connect() as connection:
            receipt = load_trade_calendar_receipt(
                connection,
                start_date=start,
                end_date=end,
                decision_known_at=now,
            )
        observed = receipt.sessions_between(start, end)
    except Exception as exc:
        raise StockDataBlocked(
            "DATA_BLOCKED: immutable QMT calendar receipt unavailable"
        ) from exc
    if not observed:
        raise StockDataBlocked("DATA_BLOCKED: target range has no trading session")
    return receipt, [observed[-1]] if latest_session else list(observed)


def _rows(result: Any) -> list[dict[str, Any]]:
    mappings = getattr(result, "mappings", None)
    if callable(mappings):
        return [dict(row) for row in mappings().all()]
    return [dict(row) for row in result]


def _canonical_daily_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: (str(row.get(key)) if row.get(key) is not None else None)
        for key in (
            "stock_code", "trade_date", "open", "close", "high", "low",
            "volume", "amount", "pre_close", "data_source", "batch_id",
            "data_version", "quality_status", "permission_status",
        )
    }


def _read_daily_partition(
    engine: Any,
    *,
    trade_date: str,
    expected_count: int,
    expected_set_hash: str,
) -> dict[str, Any]:
    """Read back one exact canonical daily slice without trusting run stdout."""

    with engine.connect() as connection:
        rows = _rows(connection.execute(text("""
            SELECT stock_code,trade_date,`open`,`close`,high,low,volume,amount,
                   pre_close,data_source,batch_id,data_version,quality_status,
                   permission_status
              FROM sm_stock_kline
             WHERE trade_date=:trade_date AND k_type=1 AND adjust_type=0
             ORDER BY stock_code
        """), {"trade_date": trade_date}))
    codes = [str(row.get("stock_code") or "").zfill(6) for row in rows]
    actual_set = expected_stock_set_contract(trade_date, codes)
    if (
        len(rows) != expected_count
        or len(set(codes)) != len(codes)
        or actual_set["stock_count"] != expected_count
        or actual_set["stock_set_hash"] != expected_set_hash
        or any(
            row.get("data_source") != PROVIDER
            or row.get("quality_status") != "QMT_ATTESTED"
            or row.get("permission_status") != "SUPPORTED"
            for row in rows
        )
    ):
        raise StockDataBlocked("DATA_BLOCKED: daily database partition differs")
    canonical = [_canonical_daily_row(row) for row in rows]
    return {
        "row_count": len(canonical),
        "row_hash": _digest(canonical),
        "code_count": len(codes),
        "code_set_hash": expected_set_hash,
    }


def _validate_daily_partition(
    engine: Any,
    *,
    trade_date: str,
    attestation: Mapping[str, Any],
) -> dict[str, Any]:
    numeric = {
        key: int(attestation.get(key) or 0)
        for key in (
            "target_rows", "qmt_rows", "matched_rows", "missing_qmt_rows",
            "source_only_rows", "catalog_missing_target_rows",
            "target_not_catalog_rows", "catalog_missing_source_rows",
            "source_not_catalog_rows", "mismatched_rows",
        )
    }
    if (
        attestation.get("status") != "COMPLETED"
        or attestation.get("apply") is not True
        or attestation.get("provider") != PROVIDER
        or attestation.get("start_date") != trade_date
        or attestation.get("end_date") != trade_date
        or numeric["target_rows"] <= 0
        or numeric["qmt_rows"] != numeric["target_rows"]
        or numeric["matched_rows"] != numeric["target_rows"]
        or any(
            numeric[key] != 0
            for key in numeric
            if key not in {"target_rows", "qmt_rows", "matched_rows"}
        )
    ):
        raise StockDataBlocked("DATA_BLOCKED: daily attestation is not exact")
    contract = (attestation.get("daily_universe") or {}).get(trade_date)
    if not isinstance(contract, Mapping):
        raise StockDataBlocked("DATA_BLOCKED: daily universe manifest missing")
    expected_count = int(contract.get("stock_count") or 0)
    expected_hash = str(contract.get("stock_set_hash") or "")
    if expected_count != numeric["target_rows"]:
        raise StockDataBlocked("DATA_BLOCKED: daily universe count differs")
    proof = _read_daily_partition(
        engine,
        trade_date=trade_date,
        expected_count=expected_count,
        expected_set_hash=expected_hash,
    )
    native_no_trade_rows = int(
        attestation.get("native_no_trade_rows") or 0
    )
    native_by_date = attestation.get("native_no_trade_by_date") or {}
    native_codes = (
        list(native_by_date.get(trade_date) or [])
        if isinstance(native_by_date, Mapping)
        else []
    )
    if (
        not isinstance(native_by_date, Mapping)
        or native_no_trade_rows < 0
        or len(native_codes) != native_no_trade_rows
        or len(set(native_codes)) != len(native_codes)
        or any(re.fullmatch(r"(?:0|3|4|6|8|9)\d{5}", str(code)) is None
               for code in native_codes)
        or set(native_by_date) - {trade_date}
    ):
        raise StockDataBlocked("DATA_BLOCKED: native NO_TRADE proof differs")
    return {
        **proof,
        "native_no_trade_rows": native_no_trade_rows,
        "native_no_trade_codes": sorted(str(code) for code in native_codes),
        "attestation_run_id": str(attestation.get("run_id") or ""),
        "catalog_manifest_hash": attestation.get("catalog_manifest_hash"),
        "calendar_manifest_hash": attestation.get("calendar_manifest_hash"),
    }


def _minute_receipt(engine: Any, trade_date: str) -> dict[str, Any]:
    with engine.connect() as connection:
        receipts = _rows(connection.execute(text("""
            SELECT receipt_id,trade_date,first_trade_time,last_trade_time,
                   expected_count,observed_count,coverage,row_count,
                   source_provider,capture_mode,forward_eligible,
                   quality_status,evidence_json,created_at
              FROM st_qmt_minute_sync_receipt_v2
             WHERE trade_date=:trade_date AND quality_status='PASS'
             ORDER BY created_at DESC,receipt_id
        """), {"trade_date": trade_date}))
    if len(receipts) != 1:
        raise StockDataBlocked("DATA_BLOCKED: exact active minute receipt unavailable")
    receipt = receipts[0]
    try:
        evidence = json.loads(str(receipt.get("evidence_json") or ""))
        raw_manifest = evidence["minute_coverage_manifest"]
        if not isinstance(raw_manifest, Mapping):
            raise TypeError("minute coverage manifest is not an object")
        manifest_hash = str(raw_manifest.get("manifest_hash") or "")
        with engine.connect() as connection:
            entity_rows = _rows(connection.execute(text(f"""
                SELECT manifest_hash,stock_code,expected_state,classification,
                       bar_count,time_set_hash,first_time,last_time,
                       source_row_hash,row_hash
                  FROM {COVERAGE_ENTITY_TABLE}
                 WHERE manifest_hash=:manifest_hash
                 ORDER BY stock_code
            """), {"manifest_hash": manifest_hash}))
        bundle = {
            "manifest": dict(raw_manifest),
            "entities": entity_rows,
        }
        manifest = require_exact_coverage(bundle)
    except Exception as exc:
        raise StockDataBlocked("DATA_BLOCKED: minute coverage manifest invalid") from exc
    response_receipts = evidence.get("source_response_receipts")
    if (
        str(receipt.get("trade_date") or "")[:10] != trade_date
        or receipt.get("source_provider") != PROVIDER
        or receipt.get("quality_status") != "PASS"
        or receipt.get("capture_mode") != "AFTER_CLOSE_BACKFILL"
        or bool(receipt.get("forward_eligible"))
        or str(receipt.get("first_trade_time") or "")[:19]
        != f"{trade_date} 09:30:00"
        or str(receipt.get("last_trade_time") or "")[:19]
        != f"{trade_date} 15:00:00"
        or int(receipt.get("expected_count") or 0) <= 0
        or int(receipt.get("observed_count") or 0)
        != int(receipt.get("expected_count") or 0)
        or float(receipt.get("coverage") or 0) != 1.0
        or int(receipt.get("row_count") or 0) != int(manifest["bar_count"])
        or evidence.get("minute_grid_profile") != QMT_MINUTE_GRID_PROFILE
        or int(evidence.get("minute_grid_bar_count") or 0) != 241
        or not isinstance(response_receipts, list)
        or int(evidence.get("source_response_receipt_count") or 0)
        != len(response_receipts)
        or evidence.get("source_response_receipt_hash")
        != _digest(response_receipts)
    ):
        raise StockDataBlocked("DATA_BLOCKED: minute receipt is not exact")
    return {
        **receipt,
        "evidence": evidence,
        "manifest": manifest,
        "entities": entity_rows,
    }


def _canonical_minute_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: (str(row.get(key)) if row.get(key) is not None else None)
        for key in (
            "stock_code", "trade_time", "price", "avg_price", "change",
            "change_pct", "volume", "amount",
        )
    }


def _validate_minute_partition(
    engine: Any,
    *,
    trade_date: str,
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    with engine.connect() as connection:
        rows = _rows(connection.execute(text("""
            SELECT stock_code,trade_time,price,avg_price,`change`,change_pct,
                   volume,amount
              FROM sm_stock_minute
             WHERE trade_date=:trade_date
             ORDER BY stock_code,trade_time
        """), {"trade_date": trade_date}))
    manifest = receipt["manifest"]
    traded = sorted(
        str(item["stock_code"])
        for item in receipt["entities"]
        if item.get("expected_state") == "TRADED"
    )
    grid = list(minute_time_grid(QMT_MINUTE_GRID_PROFILE))
    actual: dict[str, list[str]] = {}
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        timestamp = str(row.get("trade_time") or "")[:19]
        if not timestamp.startswith(trade_date + " "):
            raise StockDataBlocked("DATA_BLOCKED: minute row is outside target date")
        actual.setdefault(code, []).append(timestamp[11:19])
    if (
        len(rows) != int(receipt.get("row_count") or 0)
        or sorted(actual) != traded
        or any(times != grid for times in actual.values())
    ):
        raise StockDataBlocked("DATA_BLOCKED: minute database grid differs")
    canonical = [_canonical_minute_row(row) for row in rows]
    return {
        "row_count": len(canonical),
        "row_hash": _digest(canonical),
        "traded_code_count": len(traded),
        "traded_code_set_hash": _digest(traded),
        "minute_grid_hash": _digest(grid),
        "minute_grid_count": len(grid),
        "source_receipt_id": receipt.get("receipt_id"),
        "catalog_manifest_hash": receipt["evidence"]["reference_roots"].get(
            "catalog_manifest_hash"
        ),
        "calendar_manifest_hash": receipt["evidence"]["reference_roots"].get(
            "calendar_manifest_hash"
        ),
    }


def run(
    *,
    dataset: str,
    latest_session: bool,
    start_date: str,
    end_date: str,
    expected_build_sha: str,
    apply: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or _now()
    _validate_executor(dataset)
    build_sha = _build_sha(expected_build_sha)
    if not apply:
        return _signed({
            "schema": RESULT_SCHEMA,
            "status": "DRY_RUN",
            "dataset": dataset,
            "task_type": TASK_TYPES[dataset],
            "executor_owner": EDGE_ROLE,
            "provider": PROVIDER,
            "build_sha": build_sha,
        })
    primary = create_batch_engine(future=True)
    history = get_kline_engine()
    calendar, sessions = _sessions(
        primary,
        dataset=dataset,
        latest_session=latest_session,
        start_date=start_date,
        end_date=end_date,
        now=current,
    )
    if sessions[-1] == current.date().isoformat() and current.hour * 100 + current.minute < 1505:
        raise StockDataBlocked("DATA_BLOCKED: current session has not closed")
    before = _release(build_sha)
    partitions: list[dict[str, Any]] = []
    for session in sessions:
        outcome = run_dataset(
            "daily_kline" if dataset == "daily" else "minute_price",
            date_str=session,
            require_bigqmt=True,
        )
        if outcome.get("status") != "success" or outcome.get("source_policy") != "bigqmt_primary":
            raise StockDataBlocked(
                f"DATA_BLOCKED: BigQMT canonical {dataset} run failed for {session}"
            )
        if dataset == "daily":
            attestation = outcome.get("attestation")
            if not isinstance(attestation, Mapping):
                raise StockDataBlocked("DATA_BLOCKED: daily attestation missing")
            proof = _validate_daily_partition(
                history,
                trade_date=session,
                attestation=attestation,
            )
        else:
            minute_receipt = _minute_receipt(primary, session)
            proof = _validate_minute_partition(
                history,
                trade_date=session,
                receipt=minute_receipt,
            )
        partitions.append({"trade_date": session, **proof})
    after = _release(build_sha)
    if _release_identity(before) != _release_identity(after):
        raise StockDataBlocked("DATA_BLOCKED: BigQMT release changed during publish")
    return _signed({
        "schema": RESULT_SCHEMA,
        "status": "PASS",
        "dataset": dataset,
        "task_type": TASK_TYPES[dataset],
        "executor_owner": EDGE_ROLE,
        "provider": PROVIDER,
        "build_sha": build_sha,
        "sessions": sessions,
        "session_count": len(sessions),
        "session_set_hash": _digest(sessions),
        "calendar": {
            "batch_id": calendar.batch_id,
            "manifest_hash": calendar.manifest_hash,
            "session_set_hash": calendar.session_set_hash,
        },
        "source_identity": _release_identity(after),
        "partitions": partitions,
        "partition_manifest_hash": _digest(partitions),
    })


def _failure(dataset: str, exc: BaseException) -> dict[str, Any]:
    return _signed({
        "schema": RESULT_SCHEMA,
        "status": "DATA_BLOCKED",
        "dataset": dataset,
        "task_type": TASK_TYPES.get(dataset, ""),
        "executor_owner": EDGE_ROLE,
        "provider": PROVIDER,
        "error_type": type(exc).__name__,
        "error": str(exc)[:1000],
    })


def validate_task_result(payload: Mapping[str, Any], return_code: int) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        return "failed"
    unsigned = dict(payload)
    supplied = unsigned.pop("receipt_id", None)
    if supplied != _digest(unsigned):
        return "failed"
    if payload.get("status") == "DATA_BLOCKED" and int(return_code) == 2:
        return "blocked"
    try:
        dataset = str(payload["dataset"])
        sessions = list(payload["sessions"])
        partitions = list(payload["partitions"])
    except (KeyError, TypeError):
        return "failed"
    try:
        calendar = payload["calendar"]
        identity = payload["source_identity"]
        build_sha = str(payload["build_sha"])
        normalized_sessions = [date.fromisoformat(str(item)).isoformat() for item in sessions]
    except (KeyError, TypeError, ValueError):
        return "failed"
    valid = (
        int(return_code) == 0
        and payload.get("status") == "PASS"
        and dataset in TASK_TYPES
        and payload.get("task_type") == TASK_TYPES[dataset]
        and payload.get("provider") == PROVIDER
        and payload.get("executor_owner") == EDGE_ROLE
        and SHA40.fullmatch(build_sha) is not None
        and build_sha != "0" * 40
        and normalized_sessions == sessions == sorted(set(sessions))
        and int(payload.get("session_count") or 0) == len(sessions) > 0
        and payload.get("session_set_hash") == _digest(sessions)
        and len(partitions) == len(sessions)
        and [item.get("trade_date") for item in partitions] == sessions
        and payload.get("partition_manifest_hash") == _digest(partitions)
        and isinstance(calendar, Mapping)
        and str(calendar.get("batch_id") or "")
        and re.fullmatch(r"[0-9a-f]{64}", str(calendar.get("manifest_hash") or ""))
        is not None
        and re.fullmatch(r"[0-9a-f]{64}", str(calendar.get("session_set_hash") or ""))
        is not None
        and isinstance(identity, Mapping)
        and identity.get("strategy_identity_frozen") is True
        and identity.get("strategy_build_sha") == build_sha
        and str(identity.get("strategy_release_protocol") or "")
        and str(identity.get("strategy_identity_protocol") or "")
        and re.fullmatch(
            r"[0-9a-f]{40}|[0-9a-f]{64}",
            str(identity.get("strategy_git_blob") or ""),
        ) is not None
        and all(
            re.fullmatch(r"[0-9a-f]{64}", str(identity.get(key) or ""))
            is not None
            for key in (
                "strategy_source_sha256",
                "strategy_artifact_sha256",
                "strategy_loaded_identity_sha256",
            )
        )
    )
    if not valid:
        return "failed"
    for partition in partitions:
        if not isinstance(partition, Mapping):
            return "failed"
        try:
            row_count = int(partition["row_count"])
            row_hash = str(partition["row_hash"])
        except (KeyError, TypeError, ValueError):
            return "failed"
        if row_count <= 0 or re.fullmatch(r"[0-9a-f]{64}", row_hash) is None:
            return "failed"
        if dataset == "daily":
            if (
                int(partition.get("code_count") or 0) != row_count
                or re.fullmatch(
                    r"[0-9a-f]{64}", str(partition.get("code_set_hash") or "")
                ) is None
                or not str(partition.get("attestation_run_id") or "")
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(partition.get("catalog_manifest_hash") or ""),
                ) is None
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(partition.get("calendar_manifest_hash") or ""),
                ) is None
            ):
                return "failed"
        elif (
            int(partition.get("minute_grid_count") or 0) != 241
            or partition.get("minute_grid_hash")
            != _digest(list(minute_time_grid(QMT_MINUTE_GRID_PROFILE)))
            or int(partition.get("traded_code_count") or 0) <= 0
            or row_count
            != int(partition.get("traded_code_count") or 0) * 241
            or not str(partition.get("source_receipt_id") or "")
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(partition.get("catalog_manifest_hash") or ""),
            ) is None
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(partition.get("calendar_manifest_hash") or ""),
            ) is None
        ):
            return "failed"
    return "complete"


def validate_persisted_result(
    primary_engine: Any,
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
    expected_session: str = "",
) -> dict[str, Any]:
    """Re-read each receipt-authoritative partition before scheduler success."""

    if validate_task_result(payload, 0) != "complete":
        raise StockDataBlocked("DATA_BLOCKED: stock task result is invalid")
    current = now or _now()
    if current.tzinfo is not None:
        current = current.astimezone(SHANGHAI).replace(tzinfo=None)
    current = current.replace(microsecond=0)
    expected = str(expected_session or "").strip()
    if expected:
        try:
            expected = date.fromisoformat(expected).isoformat()
        except ValueError as exc:
            raise StockDataBlocked(
                "DATA_BLOCKED: expected stock session is invalid"
            ) from exc
    _calendar, expected_sessions = _sessions(
        primary_engine,
        dataset=str(payload["dataset"]),
        latest_session=not bool(expected),
        start_date=expected,
        end_date=expected,
        now=current,
    )
    sessions = list(payload["sessions"])
    if sessions != expected_sessions:
        raise StockDataBlocked("DATA_BLOCKED: stale stock receipt replay")
    history_engine = get_kline_engine()
    verified: list[dict[str, Any]] = []
    for partition in payload["partitions"]:
        trade_date = str(partition["trade_date"])
        if payload["dataset"] == "daily":
            with history_engine.connect() as connection:
                truth = load_qmt_daily_market_truth(
                    connection,
                    start_date=trade_date,
                    end_date=trade_date,
                    decision_known_at=current,
                )
            if (
                truth.run_id != partition.get("attestation_run_id")
                or truth.catalog_manifest_hash
                != partition.get("catalog_manifest_hash")
                or truth.calendar_manifest_hash
                != partition.get("calendar_manifest_hash")
                or truth.attested_row_count != int(partition["row_count"])
            ):
                raise StockDataBlocked(
                    "DATA_BLOCKED: daily attestation receipt differs"
                )
            actual = _read_daily_partition(
                history_engine,
                trade_date=trade_date,
                expected_count=int(partition["code_count"]),
                expected_set_hash=str(partition["code_set_hash"]),
            )
            expected = {
                key: partition[key]
                for key in ("row_count", "row_hash", "code_count", "code_set_hash")
            }
        else:
            receipt = _minute_receipt(primary_engine, trade_date)
            actual = _validate_minute_partition(
                history_engine,
                trade_date=trade_date,
                receipt=receipt,
            )
            expected = {
                key: partition[key]
                for key in (
                    "row_count", "row_hash", "traded_code_count",
                    "traded_code_set_hash", "minute_grid_hash",
                    "minute_grid_count", "source_receipt_id",
                    "catalog_manifest_hash", "calendar_manifest_hash",
                )
            }
        if actual != expected:
            raise StockDataBlocked(
                f"DATA_BLOCKED: persisted {payload['dataset']} partition differs"
            )
        verified.append({"trade_date": trade_date, **actual})
    return {
        "dataset": payload["dataset"],
        "sessions": sessions,
        "partition_manifest_hash": _digest(verified),
        "row_count": sum(int(item["row_count"]) for item in verified),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(TASK_TYPES), required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--latest-session", action="store_true")
    group.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.start_date and not args.end_date:
        parser.error("--start-date requires --end-date")
    try:
        result = run(
            dataset=args.dataset,
            latest_session=args.latest_session,
            start_date=args.start_date,
            end_date=args.end_date,
            expected_build_sha=args.expected_build_sha,
            apply=args.apply,
        )
        code = 0
    except Exception as exc:
        result = _failure(args.dataset, exc)
        code = 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
