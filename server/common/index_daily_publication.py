"""Availability proof for existing QMT DAT and verified public index publications."""
from __future__ import annotations

from datetime import datetime, time
import json
from time import monotonic
from typing import Any, Mapping, Sequence

from sqlalchemy import bindparam, text

from server.common.pit_facts import normalize_decision_at
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_result import SHA64, row_hash


FREE_RECEIPT_SCHEMA = "probiga.free-index-daily-receipt.v1"
FREE_BAR_RECEIPT_SCHEMA = "probiga.free-index-daily-bar-receipt.v1"
FREE_BAR_PARTITION_PREFIX = "index_bar:"
FREE_INDEX_RESULT_SCHEMA = "probiga.free-index-daily-result.v2"
PUBLIC_INDEX_DAILY_CODES = (
    "000001", "000016", "000300", "000680", "000688", "000852", "000905",
    "399001", "399006", "399303", "899050",
)
_BATCH_DAYS = 16
_MAX_ROWS_PER_BATCH = 16_000
_BAR_FIELDS = (
    "index_code", "trade_time", "trade_date", "k_type", "open", "close",
    "high", "low", "volume", "amount", "change", "change_pct",
)


def canonical_free_index_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Use the public index publisher's persisted-row fingerprint contract."""
    return sorted([
        {"index_code": str(row["index_code"]).zfill(6), "trade_date": str(row["trade_date"])[:10],
         **{key: str(row[key]) for key in ("open", "close", "high", "low", "volume", "amount")}}
        for row in rows
    ], key=lambda item: item["index_code"])


def free_index_bar_hash(row: Mapping[str, Any]) -> str:
    return row_hash([{key: row[key] for key in _BAR_FIELDS}])


def index_scope_fingerprint(bars: Sequence[Mapping[str, Any]]) -> str:
    return canonical_digest([
        {"index_code": bar["index_code"], "row_hash": free_index_bar_hash(bar),
         "available_at": normalize_decision_at(bar["available_at"]).isoformat(sep=" ")}
        for bar in sorted(bars, key=lambda row: row["index_code"])
    ])


def _detail(raw: Any) -> dict:
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _matches_counts(receipt: Mapping[str, Any], *, count: int) -> bool:
    return receipt.get("row_count") == count and receipt.get("code_count") == count


def _datadir_matches(receipt: Mapping[str, Any], rows: list[dict]) -> bool:
    detail = _detail(receipt["detail_json"])
    count = len(rows)
    return (_matches_counts(receipt, count=count)
            and detail.get("readback_rows") == detail.get("readback_codes") == count
            and SHA64.fullmatch(str(receipt.get("content_sha256") or "")) is not None
            and detail.get("row_hash") == row_hash([{key: row[key] for key in _BAR_FIELDS} for row in rows]))


def _free_matches(receipt: Mapping[str, Any], rows: list[dict], day: str) -> bool:
    detail = _detail(receipt["detail_json"])
    receipt_id = detail.pop("receipt_id", None)
    count = len(rows)
    if (detail.get("schema") != FREE_RECEIPT_SCHEMA or detail.get("dataset") != "index_daily"
            or detail.get("provider") != "tencent_east_verified" or detail.get("target_date") != day
            or detail.get("status") != "complete" or detail.get("missing_codes") != []
            or detail.get("qmt_attestation_claimed") is not False
            or detail.get("captured_at") != normalize_decision_at(receipt["available_at"]).isoformat(sep=" ")
            or receipt_id != receipt["request_id"] or receipt_id != canonical_digest(detail)
            or not _matches_counts(detail, count=count) or receipt["written_rows"] != count
            or detail.get("expected_count") != count):
        return False
    canonical = canonical_free_index_rows(rows)
    code_hash = canonical_digest([row["index_code"] for row in canonical])
    return (detail.get("code_set_hash") == detail.get("expected_code_set_hash") == code_hash
            and detail.get("row_hash") == canonical_digest(canonical))


def _free_bar_matches(receipt: Mapping[str, Any], row: Mapping[str, Any], day: str) -> bool:
    detail = _detail(receipt["detail_json"])
    receipt_id = detail.pop("receipt_id", None)
    code = row["index_code"]
    digest = free_index_bar_hash(row)
    available_at = normalize_decision_at(receipt["available_at"])
    closed_at = datetime.combine(datetime.fromisoformat(day).date(), time(15))
    return (
        detail.get("schema") == FREE_BAR_RECEIPT_SCHEMA
        and detail.get("provider") == "tencent_east_verified"
        and detail.get("source_variant") in {"tencent_only", "east_only", "cross_checked", "cnindex_official"}
        and detail.get("target_date") == day
        and detail.get("index_code") == code
        and detail.get("available_at") == available_at.isoformat(sep=" ")
        and detail.get("row_hash") == digest
        and receipt["partition_key"] == f"{FREE_BAR_PARTITION_PREFIX}{code}:{digest[:40]}"
        and receipt["written_rows"] == 1
        and receipt_id == receipt["request_id"] == canonical_digest(detail)
        and row["trade_time"] is not None
        and normalize_decision_at(row["trade_time"]) == closed_at
        and row["etl_sync_at"] is not None
        and closed_at <= normalize_decision_at(row["etl_sync_at"]) <= available_at
    )


def _partition_at_receipt(partition: list[dict], receipt: Mapping[str, Any]) -> list[dict]:
    published_at = normalize_decision_at(receipt["available_at"])
    return [row for row in partition if row["etl_sync_at"] is None
            or normalize_decision_at(row["etl_sync_at"]) <= published_at]


def load_verified_index_daily_bars(
    primary: Any, history: Any, *, sessions: Sequence[str], code: str | None = None,
    codes: Sequence[str] | None = None,
    as_of: datetime, deadline: float,
) -> list[dict]:
    """Read only content still matching a verified post-write proof at the cutoff.

    A historical partition receipt can still verify its original rows after
    an additive repair. An added bar requires its own actual availability proof.
    """
    if not sessions:
        return []
    if (code is None) == (codes is None):
        raise ValueError("provide one exact index code scope")
    selected = {code} if code is not None else set(codes or ())
    if not selected:
        return []
    if monotonic() >= deadline:
        raise TimeoutError("指数发布回执查询超时")
    cutoff = normalize_decision_at(as_of)
    bounds = {"start": sessions[0], "end": sessions[-1], "cutoff": cutoff}
    with primary.connect() as connection:
        datadir = {str(row["trade_date"])[:10]: row for row in connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(2500) */ trade_date, row_count, code_count,
                   content_sha256, finished_at AS available_at, detail_json
            FROM qmt_datadir_import_partition
            WHERE dataset='index_daily' AND status='COMPLETE'
              AND trade_date BETWEEN :start AND :end AND finished_at<=:cutoff
        """), bounds).mappings()}
        if monotonic() >= deadline:
            raise TimeoutError("公开指数回执查询超时")
        free = {str(row["trade_date"])[:10]: row for row in connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(2500) */ target_date AS trade_date, request_id,
                   written_rows, last_success_at AS available_at, detail_json
            FROM acquisition_partition_state
            WHERE dataset='index_daily' AND source='tencent_east_verified'
              AND partition_key='__index_daily_partition__' AND status='complete'
              AND target_date BETWEEN :start AND :end AND last_success_at<=:cutoff
        """), bounds).mappings()}
        if monotonic() >= deadline:
            raise TimeoutError("指数单条补采回执查询超时")
        repairs: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        for row in connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(2500) */ target_date AS trade_date, partition_key,
                   request_id, written_rows, last_success_at AS available_at, detail_json
            FROM acquisition_partition_state
            WHERE dataset='index_daily' AND source='tencent_east_verified' AND status='complete'
              AND target_date BETWEEN :start AND :end AND last_success_at<=:cutoff
              AND partition_key LIKE 'index!_bar:%' ESCAPE '!'
              AND SUBSTR(partition_key,11,6) IN :codes
        """).bindparams(bindparam("codes", expanding=True)), {**bounds, "codes": sorted(selected)}).mappings():
            key = (str(row["trade_date"])[:10], row["partition_key"][10:16])
            repairs.setdefault(key, []).append(row)

    repair_dates = {day for day, _ in repairs}
    eligible = [day for day in sessions if day in datadir or day in free or day in repair_dates]
    if not eligible:
        return []
    select = text("""
        SELECT /*+ MAX_EXECUTION_TIME(2500) */ index_code, trade_time, trade_date, k_type,
               open, close, high, low, volume, amount, `change`, change_pct, etl_sync_at
        FROM sm_index_kline WHERE k_type=1 AND trade_date IN :dates
        ORDER BY trade_date, index_code, trade_time LIMIT :limit
    """).bindparams(bindparam("dates", expanding=True))
    result = []
    with history.connect() as connection:
        for offset in range(0, len(eligible), _BATCH_DAYS):
            if monotonic() >= deadline:
                raise TimeoutError("指数发布内容核验超时")
            dates = eligible[offset:offset + _BATCH_DAYS]
            rows = connection.execute(select, {"dates": dates, "limit": _MAX_ROWS_PER_BATCH + 1}).mappings().all()
            if len(rows) > _MAX_ROWS_PER_BATCH:
                raise ValueError("指数发布内容超过核验上限")
            grouped: dict[str, list[dict]] = {}
            for row in rows:
                grouped.setdefault(str(row["trade_date"])[:10], []).append(dict(row))
            for day, partition in grouped.items():
                if len({row["index_code"] for row in partition}) != len(partition):
                    continue
                published: dict[str, list[datetime]] = {}
                if day in datadir:
                    receipt = datadir[day]
                    original = _partition_at_receipt(partition, receipt)
                    if _datadir_matches(receipt, original):
                        stamp = normalize_decision_at(receipt["available_at"])
                        for row in original:
                            published.setdefault(row["index_code"], []).append(stamp)
                if day in free:
                    receipt = free[day]
                    original = _partition_at_receipt(partition, receipt)
                    if _free_matches(receipt, original, day):
                        stamp = normalize_decision_at(receipt["available_at"])
                        for row in original:
                            published.setdefault(row["index_code"], []).append(stamp)
                closed_at = datetime.combine(datetime.fromisoformat(day).date(), time(15))
                for bar in partition:
                    symbol = bar["index_code"]
                    if symbol not in selected:
                        continue
                    evidence = published.get(symbol, [])
                    evidence.extend(normalize_decision_at(receipt["available_at"])
                                    for receipt in repairs.get((day, symbol), ())
                                    if _free_bar_matches(receipt, bar, day))
                    available = [stamp for stamp in evidence if closed_at <= stamp <= cutoff]
                    if available:
                        result.append({**bar, "available_at": min(available)})
    return result
