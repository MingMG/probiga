"""Reuse complete closed-session minute data without changing its provenance.

This is collection evidence only. It never manufactures a QMT attestation or
turns a public-source partition into QMT-native market data.
"""
from __future__ import annotations

import hashlib
import os
import re
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Mapping

from sqlalchemy import bindparam, text

from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE
from server.common.qmt_attestation_contract import canonical_digest

SCHEMA = "probiga.minute-acquisition-result.v1"
TASK_DATASETS = {
    "qmt_stock_minute_canonical": "stock",
    "qmt_stock_minute_flow_canonical": "flow",
    "intraday_minute_kline": "stock",
    "intraday_minute_flow": "flow",
    "stock_minute": "stock",
    "stock_minute_flow": "flow",
}
TABLES = {"stock": "sm_stock_minute", "flow": "sm_stock_capital_flow_min"}
FIELDS = {
    "stock": ("price", "avg_price", "change", "change_pct", "volume", "amount"),
    "flow": ("main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"),
}
SOURCES = {
    "stock": {"east_push2delay": 240, "gj_big_qmt_inner": 241},
    "flow": {"east_push2delay": 240, "gj_qmt_transactioncount1m": 241},
}


def _grid_hash(count: int) -> str:
    start = datetime(2000, 1, 1, 9, 30 if count == 241 else 31)
    end = datetime(2000, 1, 1, 15, 0)
    values = []
    while start <= end:
        if start.hour < 12 or start.hour >= 13 and (start.hour != 13 or start.minute > 0):
            if not (start.hour == 11 and start.minute > 30):
                values.append(start.strftime("%H:%M:%S"))
        start += timedelta(minutes=1)
    assert len(values) == count
    return hashlib.sha256("".join(values).encode()).hexdigest()


GRID_HASHES = {count: _grid_hash(count) for count in (240, 241)}


def _query(kind: str) -> Any:
    fields = FIELDS[kind]
    values = ",".join("`" + field + "`" for field in fields)
    invalid = " OR ".join(f"`{field}` IS NULL OR ABS(`{field}`)>=1e20"
                          for field in fields if field != "avg_price")
    if kind == "stock":
        invalid += " OR price<=0 OR (avg_price IS NULL AND data_source<>'east_push2delay') OR (avg_price IS NOT NULL AND (avg_price<=0 OR ABS(avg_price)>=1e20)) OR volume<0 OR amount<0 OR trade_date<>:day"
    invalid += " OR source_time IS NULL OR source_time<>trade_time OR MICROSECOND(trade_time)<>0"
    row_hash = f"SHA2(CAST(JSON_ARRAY(stock_code,trade_time,{values},data_source,source_time) AS CHAR),256)"
    # Fixed-width hashes keep concatenation bounded at 241*64 bytes per stock.
    nonzero = " OR ".join(f"`{field}`<>0" for field in fields)
    return text(f"""
        SELECT /*+ MAX_EXECUTION_TIME(30000) */ stock_code,COUNT(*) row_count,COUNT(DISTINCT trade_time) time_count,
               COUNT(DISTINCT data_source) source_count,MIN(data_source) data_source,
               SUM(CASE WHEN {invalid} THEN 1 ELSE 0 END) invalid_count,
               MAX(CASE WHEN {nonzero} THEN 1 ELSE 0 END) has_nonzero,
               SHA2(GROUP_CONCAT(DATE_FORMAT(trade_time,'%H:%i:%s') ORDER BY trade_time SEPARATOR ''),256) grid_hash,
               SHA2(GROUP_CONCAT({row_hash} ORDER BY trade_time SEPARATOR ''),256) row_hash,
               LENGTH(GROUP_CONCAT({row_hash} ORDER BY trade_time SEPARATOR '')) hashed_bytes
          FROM `{TABLES[kind]}`
         WHERE stock_code IN :codes AND trade_time>=:day AND trade_time<:next_day
         GROUP BY stock_code ORDER BY stock_code
    """).bindparams(bindparam("codes", expanding=True))


def validate_inventory(rows, *, kind: str, expected_codes, no_trade_codes) -> dict | None:
    """Exact entity/time/value checks; counts alone never establish completeness."""
    expected = set(expected_codes)
    no_trade = set(no_trade_codes)
    if not expected or not no_trade <= expected:
        return None
    traded = expected - no_trade
    seen, proofs, sources = set(), [], {}
    nonzero_codes = 0
    for raw in rows:
        row = dict(raw)
        code, source = str(row.get("stock_code") or ""), row.get("data_source")
        size = SOURCES[kind].get(source)
        if (code not in traded or code in seen or size is None
                or row.get("source_count") != 1 or row.get("invalid_count") != 0
                or row.get("row_count") != size or row.get("time_count") != size
                or row.get("hashed_bytes") != size * 64
                or row.get("grid_hash") != GRID_HASHES[size]
                or not re.fullmatch(r"[0-9a-f]{64}", str(row.get("row_hash") or ""))):
            return None
        seen.add(code)
        proofs.append([code, source, size, row["grid_hash"], row["row_hash"]])
        sources[source] = sources.get(source, 0) + size
        nonzero_codes += int(row.get("has_nonzero") == 1)
    if seen != traded or not seen:
        return None
    # A permission-denied QMT flow response can contain a full grid of zeros.
    # Preserve the collector's market-wide quality threshold when reusing it.
    if kind == "flow" and nonzero_codes * 5 < len(seen):
        return None
    return {
        "row_count": sum(sources.values()), "stock_count": len(seen),
        "stock_set_hash": canonical_digest(sorted(seen)),
        "no_trade_count": len(no_trade), "no_trade_set_hash": canonical_digest(sorted(no_trade)),
        "source_rows": dict(sorted(sources.items())), "partition_hash": canonical_digest(sorted(proofs)),
    }


def inspect_complete_partition(primary_engine, data_engine, *, kind: str,
                               trade_date: str, now: datetime, catalog_batch_id=None) -> dict | None:
    from server.common.qmt_stock_catalog import load_target_stock_catalog
    from tools.crawl_minute_kline import verified_no_trade_codes, _is_trade_day

    target = date.fromisoformat(trade_date)
    current = (now.astimezone(PRODUCTION_TIMEZONE) if now.tzinfo else now).replace(tzinfo=None, microsecond=0)
    if kind not in TABLES or target > current.date() or (target == current.date() and current.hour < 16):
        return None
    if not _is_trade_day(primary_engine, target):
        return None
    catalog, expected = load_target_stock_catalog(
        primary_engine, target_date=trade_date, decision_known_at=current, batch_id=catalog_batch_id,
    )
    no_trade, native_ref = verified_no_trade_codes(
        primary_engine, catalog, trade_date=trade_date, decision_known_at=current,
    )
    with data_engine.connect() as connection:
        original_limit = int(connection.exec_driver_sql("SELECT @@SESSION.group_concat_max_len").scalar_one())
        connection.exec_driver_sql("SET SESSION group_concat_max_len=32768")
        rows = []
        codes = sorted(expected)
        # Both split databases have a (stock_code, trade_time) index. Bound
        # every lookup to it instead of scanning decades of minute history.
        try:
            for offset in range(0, len(codes), 100):
                rows.extend(connection.execute(_query(kind), {
                    "day": trade_date, "next_day": (target + timedelta(days=1)).isoformat(),
                    "codes": codes[offset:offset + 100],
                }).mappings().all())
        finally:
            connection.exec_driver_sql(f"SET SESSION group_concat_max_len={original_limit}")
    proof = validate_inventory(rows, kind=kind, expected_codes=expected, no_trade_codes=no_trade)
    if proof is None:
        return None
    return {
        "dataset": kind, "trade_date": trade_date, "table": TABLES[kind],
        "decision_known_at": current.isoformat(sep=" "),
        "catalog_batch_id": catalog.batch_id, "catalog_manifest_hash": catalog.manifest_hash,
        "expected_stock_count": len(expected), "expected_stock_set_hash": canonical_digest(sorted(expected)),
        "native_no_trade_evidence": native_ref, **proof,
    }


def result_for(partitions, *, task_type: str, build_sha: str, started_at: datetime,
               captured_sessions=()) -> dict:
    captured = sorted(captured_sessions)
    started = (started_at.astimezone(PRODUCTION_TIMEZONE) if started_at.tzinfo else started_at).replace(tzinfo=None, microsecond=0)
    result = {
        "schema": SCHEMA, "status": "COMPLETE" if captured else "REUSED", "task_type": task_type,
        "dataset": TASK_DATASETS[task_type], "build_sha": build_sha,
        "run_uid": os.environ.get("PROBIGA_SCHEDULER_HISTORY_RUN_UID") or uuid.uuid4().hex,
        "started_at": started.isoformat(sep=" "),
        "finished_at": datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None).isoformat(sep=" "),
        "network_accessed": bool(captured), "database_writes": bool(captured),
        "captured_sessions": captured,
        "publication_authority": False, "partitions": partitions,
    }
    result["receipt_sha256"] = canonical_digest(result)
    validate_result(result, task_type=task_type)
    return result


def validate_result(result: Mapping, *, task_type: str) -> None:
    unsigned = dict(result)
    signature = unsigned.pop("receipt_sha256", None)
    partitions = result.get("partitions")
    captured = result.get("captured_sessions")
    if (result.get("schema") != SCHEMA
            or not isinstance(captured, list) or captured != sorted(set(captured))
            or result.get("status") != ("COMPLETE" if captured else "REUSED")
            or result.get("task_type") != task_type or task_type not in TASK_DATASETS
            or result.get("dataset") != TASK_DATASETS[task_type]
            or any(result.get(key) is not bool(captured) for key in ("network_accessed", "database_writes"))
            or result.get("publication_authority") is not False
            or signature != canonical_digest(unsigned)
            or not re.fullmatch(r"[0-9a-f]{40}", str(result.get("build_sha") or ""))
            or result.get("build_sha") == "0" * 40
            or not re.fullmatch(r"[0-9a-f]{32}", str(result.get("run_uid") or ""))
            or not isinstance(partitions, list) or not 1 <= len(partitions) <= 120
            or any(not isinstance(part, dict) for part in partitions)):
        raise ValueError("invalid existing-minute acquisition receipt")
    dates = [part["trade_date"] for part in partitions]
    if dates != sorted(set(dates)) or not set(captured) <= set(dates):
        raise ValueError("reused minute dates differ")
    start = datetime.fromisoformat(result["started_at"])
    finish = datetime.fromisoformat(result["finished_at"])
    if start.tzinfo or finish.tzinfo or not start <= finish or (finish-start).total_seconds() > 2700:
        raise ValueError("reused minute execution time differs")
    for part in partitions:
        decision = datetime.fromisoformat(part["decision_known_at"])
        if (part.get("dataset") != result["dataset"] or part.get("table") != TABLES[result["dataset"]]
                or decision.tzinfo or not start <= decision <= finish
                or not isinstance(part.get("source_rows"), dict) or not part["source_rows"]
                or any(type(part.get(key)) is not int or part[key] < minimum for key, minimum in
                       (("row_count", 1), ("stock_count", 1), ("no_trade_count", 0), ("expected_stock_count", 1)))
                or part["stock_count"] + part["no_trade_count"] != part["expected_stock_count"]
                or not isinstance(part.get("catalog_batch_id"), str) or not 1 <= len(part["catalog_batch_id"]) <= 64
                or any(not re.fullmatch(r"[0-9a-f]{64}", str(part.get(key) or "")) for key in
                       ("catalog_manifest_hash", "expected_stock_set_hash", "stock_set_hash", "no_trade_set_hash", "partition_hash"))
                or not set(part["source_rows"]) <= set(SOURCES[result["dataset"]])
                or any(type(count) is not int or count <= 0 or count % SOURCES[result["dataset"]][source]
                       for source, count in part["source_rows"].items())
                or sum(part["source_rows"].values()) != part["row_count"]
                or sum(count // SOURCES[result["dataset"]][source] for source, count in part["source_rows"].items()) != part["stock_count"]
                or date.fromisoformat(part["trade_date"]) > start.date()):
            raise ValueError("reused minute partition differs")


def replay_result(result, primary_engine, *, now: datetime) -> None:
    from server.common.kline_data import get_kline_engine
    from server.common.minute_data import get_minute_engine

    validate_result(result, task_type=result["task_type"])
    current = (now.astimezone(PRODUCTION_TIMEZONE) if now.tzinfo else now).replace(tzinfo=None)
    if datetime.fromisoformat(result["finished_at"]) > current + timedelta(minutes=5):
        raise ValueError("minute acquisition receipt is from the future")
    kind = result["dataset"]
    data_engine = get_kline_engine() if kind == "stock" else get_minute_engine()
    for part in result["partitions"]:
        actual = inspect_complete_partition(primary_engine, data_engine, kind=kind,
                                            trade_date=part["trade_date"], now=datetime.fromisoformat(part["decision_known_at"]),
                                            catalog_batch_id=part["catalog_batch_id"])
        if actual != part:
            raise ValueError("reused minute partition changed or is incomplete")
