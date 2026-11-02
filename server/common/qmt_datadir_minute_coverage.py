"""Exact native minute matrices bound to the immutable daily trade scope."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import json
import time
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_daily_coverage import load_daily_manifest
from server.common.qmt_index_minute_grid import CORE_GRID

GRID_HASH = canonical_digest(list(CORE_GRID))
SCHEMA = "probiga.qmt-datadir-minute-coverage.v1"


def trade_scope(engine, day: str, *, manifest_hash: str = "", decision_known_at=None) -> dict:
    cutoff = decision_known_at or datetime.now(ZoneInfo("Asia/Shanghai"))
    proof = load_daily_manifest(engine, day, decision_known_at=cutoff,
        manifest_hash=manifest_hash)
    if proof is None:
        raise ValueError("DATA_BLOCKED: exact daily manifest is required before minute publication")
    codes = tuple(item["stock_code"] for item in proof["entities"]
        if item["classification"] == "TRADED"
        and (Decimal(item["evidence"]["volume"]) > 0 or Decimal(item["evidence"]["amount"]) > 0))
    if not codes:
        raise ValueError("DATA_BLOCKED: exact daily trade scope is empty")
    return dict(full_market_manifest_hash=proof["manifest_hash"], codes=codes,
        daily_close_by_code={item["stock_code"]: item["evidence"]["close"]
            for item in proof["entities"] if item["stock_code"] in codes},
        expected_traded_code_set_sha256=canonical_digest(list(codes)),
        eligible_code_count=len(codes))


def validate_code_scope(entry: dict, scope: dict) -> None:
    expected, actual = set(scope["codes"]), set(entry["codes"])
    if actual != expected or len(entry["codes"]) != len(expected):
        raise ValueError("exact minute trade scope differs: missing="
            + ",".join(sorted(expected - actual)[:20]) + "; unexpected="
            + ",".join(sorted(actual - expected)[:20]))
    if (set(entry["code_rows"]) != expected
            or any(int(count) != len(CORE_GRID) for count in entry["code_rows"].values())
            or int(entry["rows"]) != len(expected) * len(CORE_GRID)):
        raise ValueError("exact native minute grid is incomplete")


def validate_matrix(rows, *, day: str, scope: dict, deadline_seconds: float = 180) -> dict:
    """Consume ordered rows in bounded memory; hash matches canonical_rows JSON."""
    from server.common.qmt_datadir_result import _normalize

    expected, grid = set(scope["codes"]), set(CORE_GRID)
    seen_codes, current_times = set(), set()
    current_code, count = None, 0
    digest = sha256(b"[")
    deadline = time.monotonic() + deadline_seconds
    for source in rows:
        if time.monotonic() > deadline:
            raise TimeoutError("minute_matrix_readback: total deadline exceeded")
        row = dict(source)
        code = str(row["stock_code"])
        stamp = datetime.fromisoformat(str(row["trade_time"]))
        symbol = code + (".SH" if code.startswith("6") else ".BJ"
                         if code.startswith(("4", "8", "9")) else ".SZ")
        if (code not in expected or str(row["trade_date"])[:10] != day
                or stamp.date().isoformat() != day or stamp.strftime("%H:%M:%S") not in grid
                or stamp.microsecond or row["data_source"] != "qmt_datadir"
                or row.get("qmt_code") != symbol
                or datetime.fromisoformat(str(row.get("source_time"))) != stamp
                or row.get("quality_status") != "VERIFIED"
                or row.get("permission_status") != "SUPPORTED"):
            raise ValueError("minute matrix security/date/source differs")
        if code != current_code:
            if current_code is not None and current_times != grid:
                raise ValueError("minute matrix native grid is incomplete")
            if code in seen_codes:
                raise ValueError("minute matrix security ordering differs")
            seen_codes.add(code)
            current_code, current_times = code, set()
        moment = stamp.strftime("%H:%M:%S")
        if moment in current_times:
            raise ValueError("minute matrix has a duplicate native timestamp")
        current_times.add(moment)
        for field in ("price", "volume", "amount"):
            number = Decimal(str(row[field]))
            if not number.is_finite() or number < 0 or (field == "price" and number == 0):
                raise ValueError("minute matrix native numeric field differs")
        for field in ("change", "change_pct"):
            if not Decimal(str(row[field])).is_finite():
                raise ValueError("minute matrix native change field differs")
        if (moment == "15:00:00" and Decimal(str(row["price"]))
                != Decimal(str(scope["daily_close_by_code"][code]))):
            raise ValueError("minute matrix closing price differs from exact daily anchor")
        if count:
            digest.update(b",")
        digest.update(json.dumps({str(key): _normalize(value) for key, value in sorted(row.items())},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8"))
        count += 1
    if seen_codes != expected or current_times != grid or count != len(expected) * len(grid):
        raise ValueError("minute matrix exact security/time coverage is incomplete")
    digest.update(b"]")
    return dict(row_count=count, code_count=len(seen_codes), row_hash=digest.hexdigest(),
        full_market_manifest_hash=scope["full_market_manifest_hash"],
        expected_traded_code_set_sha256=scope["expected_traded_code_set_sha256"],
        eligible_code_count=len(expected), coverage=1.0,
        minute_grid_count=len(CORE_GRID), minute_grid_sha256=GRID_HASH, bad_grid_codes=0)


def persisted_matrix(engine, day: str, *, scope: dict) -> dict:
    connection_scope = engine.connect() if callable(getattr(engine, "connect", None)) else nullcontext(engine)
    with connection_scope as connection:
        foreign = connection.execute(text(
            "SELECT /*+ MAX_EXECUTION_TIME(20000) */ data_source FROM sm_stock_minute "
            "WHERE trade_date=:day AND (data_source IS NULL OR data_source<>'qmt_datadir') LIMIT 1"
        ), {"day": day}).first()
        if foreign is not None:
            raise ValueError("minute matrix contains an unexpected provider")
        rows = connection.execution_options(stream_results=True, max_row_buffer=2000).execute(text(
            "SELECT /*+ MAX_EXECUTION_TIME(120000) */ stock_code,trade_time,trade_date,price,"
            "`change`,change_pct,volume,amount,qmt_code,data_source,source_time,batch_id,"
            "data_version,quality_status,permission_status FROM sm_stock_minute "
            "WHERE trade_date=:day AND data_source='qmt_datadir' ORDER BY stock_code,trade_time"
        ), {"day": day}).mappings()
        try:
            return validate_matrix(rows, day=day, scope=scope)
        finally:
            rows.close()


def load_verified_minute_publication(engine, day: str, *, decision_known_at=None) -> dict | None:
    from server.common.qmt_datadir_result import load_receipt

    receipt = load_receipt(engine, dataset="stock_minute", trade_date=day)
    if receipt is None:
        return None
    cutoff = decision_known_at or datetime.now(ZoneInfo("Asia/Shanghai"))
    cutoff = datetime.fromisoformat(str(cutoff)) if not isinstance(cutoff, datetime) else cutoff
    cutoff = cutoff.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None) if cutoff.tzinfo else cutoff
    finished = receipt.get("finished_at")
    finished = datetime.fromisoformat(str(finished)) if finished is not None else None
    if receipt["status"] != "COMPLETE" or finished is None or finished > cutoff:
        raise ValueError("exact minute completion is unavailable at the decision cutoff")
    detail = receipt.get("detail") or {}
    daily_hash = str(detail.get("full_market_manifest_hash") or "")
    if len(daily_hash) != 64 or detail.get("coverage") != 1.0:
        raise ValueError("exact minute receipt lacks its daily scope proof")
    scope = trade_scope(engine, day, manifest_hash=daily_hash, decision_known_at=cutoff)
    actual = persisted_matrix(engine, day, scope=scope)
    if (int(receipt["row_count"]) != actual["row_count"]
            or int(receipt["code_count"]) != actual["code_count"]
            or detail.get("readback_rows") != actual["row_count"]
            or detail.get("readback_codes") != actual["code_count"]
            or any(detail.get(key) != actual[key] for key in (
                "row_hash", "coverage", "eligible_code_count", "full_market_manifest_hash",
                "expected_traded_code_set_sha256", "minute_grid_count", "minute_grid_sha256", "bad_grid_codes"))):
        raise ValueError("exact minute receipt and persisted matrix differ")
    core = dict(schema=SCHEMA, provider="qmt_datadir", dataset="stock_minute", trade_date=day,
        status="EXACT", strategy_eligible=True, run_id=receipt["run_id"],
        captured_at=finished.isoformat(sep=" "), **actual)
    return {**core, "manifest_hash": canonical_digest(core)}
