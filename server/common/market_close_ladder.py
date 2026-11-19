"""Immutable exact-session Eastmoney close-pool evidence for the market board."""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text

from server.common.eastmoney_upper_limit_history import EASTMONEY_CLOSE_POOL_CONFIG, _cache_root, _fetch_pool
from server.common.mysql_lock import mysql_named_lock
from server.common.qmt_attestation_contract import canonical_digest


SCHEMA = "probiga.market-close-limit-pool.v1"
CAPTURE_SCHEMA = "probiga.market-close-ladder-capture.v1"
SOURCE = "东财收盘涨停池 · QMT 收盘价复核"
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_RAW = 512 * 1024
_MAX_RECORD = 2 * _MAX_RAW + 16 * 1024
_MAX_CODES = 7_000
_CLOSE_TOLERANCE = Decimal("0.001")
_POOL_ENDPOINTS = {kind: config[0] for kind, config in EASTMONEY_CLOSE_POOL_CONFIG.items()}


class MarketCloseLadderBlocked(RuntimeError):
    pass


def _now() -> datetime:
    return datetime.now(_SHANGHAI).replace(tzinfo=None)


def _stamp(value) -> datetime:
    try:
        stamp = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise MarketCloseLadderBlocked("close-pool timestamp is invalid") from exc
    return stamp.astimezone(_SHANGHAI).replace(tzinfo=None) if stamp.tzinfo else stamp


def _clock_text(stamp: datetime) -> str:
    return stamp.replace(tzinfo=_SHANGHAI).isoformat()


def _path(target: date, pool_kind: str = "limit_up") -> Path:
    prefix = "close-ladder" if pool_kind == "limit_up" else f"close-{pool_kind}"
    return _cache_root() / f"{prefix}-{target.isoformat()}.json"


def _pool(raw: str, target: date, *, observed_at: datetime | None = None, pool_kind: str = "limit_up") -> list[dict]:
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > _MAX_RAW:
        raise MarketCloseLadderBlocked("close-pool raw response exceeds bound")
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise MarketCloseLadderBlocked("close-pool response is invalid JSON") from exc
    data = payload.get("data") if isinstance(payload, dict) and type(payload.get("rc")) is int and payload["rc"] == 0 else None
    if not isinstance(data, dict):
        raise MarketCloseLadderBlocked("close-pool response does not prove the requested session")
    try:
        quote_date = datetime.strptime(str(data.get("qdate")), "%Y%m%d").date()
    except (TypeError, ValueError) as exc:
        raise MarketCloseLadderBlocked("close-pool quote date is invalid") from exc
    # Historical date requests retain the provider's latest quote date. Bind
    # their nonempty rows to the requested session through every QMT close.
    if not target <= quote_date <= (observed_at or _now()).date():
        raise MarketCloseLadderBlocked("close-pool quote date is outside the capture window")
    rows, count = data.get("pool"), data.get("tc")
    if type(count) is not int or not isinstance(rows, list) or not 0 <= count == len(rows) <= _MAX_CODES:
        raise MarketCloseLadderBlocked("close-pool count differs from provider coverage")
    if not rows and quote_date != target:
        raise MarketCloseLadderBlocked("empty close-pool does not prove the requested session")
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            raise MarketCloseLadderBlocked("close-pool row is invalid")
        code, name, price, streak = (row.get(key) for key in ("c", "n", "p", "lbc"))
        if (not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code) or code in result
                or not isinstance(name, str) or not 0 < len(name.strip()) <= 128
                or type(price) is not int or price <= 0
                or (pool_kind == "limit_up" and (type(streak) is not int or streak <= 0))):
            raise MarketCloseLadderBlocked("close-pool identity, price or streak differs")
        result[code] = {"stock_code": code, "short_name": name.strip(), "provider_close": Decimal(price) / 1000,
                        "streak": streak}
    return [result[code] for code in sorted(result)]


def _qmt_close_hash(target: date, pool: list[dict], quotes: list[dict]) -> str:
    codes = {row["stock_code"] for row in pool}
    matched = {}
    for quote in quotes:
        code = quote.get("stock_code")
        if code not in codes:
            continue
        if (code in matched or str(quote.get("trade_date"))[:10] != target.isoformat()
                or quote.get("data_source") not in {"qmt_datadir", "gj_big_qmt_inner"}
                or quote.get("quality_status") not in {"VERIFIED", "QMT_ATTESTED"}
                or quote.get("permission_status") != "SUPPORTED"):
            raise MarketCloseLadderBlocked(f"QMT close source or row identity differs for {code}")
        try:
            price = Decimal(str(quote.get("price")))
        except InvalidOperation as exc:
            raise MarketCloseLadderBlocked(f"QMT close is invalid for {code}") from exc
        if not price.is_finite() or price <= 0:
            raise MarketCloseLadderBlocked(f"QMT close is invalid for {code}")
        matched[code] = price
    if set(matched) != codes:
        raise MarketCloseLadderBlocked("QMT closes do not cover the exact provider sample")
    if any(abs(matched[row["stock_code"]] - row["provider_close"]) > _CLOSE_TOLERANCE for row in pool):
        raise MarketCloseLadderBlocked("Eastmoney close price differs from QMT close")
    return canonical_digest([{"stock_code": code, "close": format(matched[code].normalize(), "f")}
                             for code in sorted(matched)])


def _load_qmt_closes(engine, target: date, pool: list[dict], cutoff: datetime) -> list[dict]:
    if not pool:
        return []
    with engine.connect() as connection:
        statement = text("""
            SELECT /*+ MAX_EXECUTION_TIME(8000) */ stock_code, trade_date, close AS price,
                   data_source, quality_status, permission_status
            FROM sm_stock_kline WHERE trade_date=:day AND k_type=1 AND adjust_type=0
              AND stock_code IN :codes AND etl_sync_at BETWEEN :closed_at AND :cutoff
            LIMIT :limit
        """).bindparams(bindparam("codes", expanding=True))
        return [dict(row) for row in connection.execute(statement, {
            "day": target.isoformat(), "codes": [row["stock_code"] for row in pool],
            "closed_at": datetime.combine(target, time(15)), "cutoff": cutoff, "limit": len(pool) + 1,
        }).mappings()]


def _load_record(target: date, cutoff: datetime, pool_kind: str = "limit_up") -> tuple[dict, list[dict]] | None:
    path = _path(target, pool_kind)
    if path.is_symlink():
        raise MarketCloseLadderBlocked("close-pool receipt cannot be a symlink")
    try:
        with path.open("rb") as source:
            blob = source.read(_MAX_RECORD + 1)
    except FileNotFoundError:
        return None
    if len(blob) > _MAX_RECORD:
        raise MarketCloseLadderBlocked("close-pool receipt exceeds bound")
    try:
        record = json.loads(blob)
    except (UnicodeError, ValueError) as exc:
        raise MarketCloseLadderBlocked("close-pool receipt is unreadable") from exc
    if not isinstance(record, dict):
        raise MarketCloseLadderBlocked("close-pool receipt schema differs")
    digest = record.pop("receipt_sha256", None)
    observed, published = _stamp(record.get("observed_at")), _stamp(record.get("published_at"))
    if (record.get("schema") != SCHEMA or record.get("trade_date") != target.isoformat()
            or record.get("endpoint") != _POOL_ENDPOINTS[pool_kind]
            or not datetime.combine(target, time(15)) <= observed <= published <= cutoff
            or not re.fullmatch(r"[0-9a-f]{64}", str(record.get("qmt_close_sha256") or ""))
            or digest != canonical_digest(record)):
        raise MarketCloseLadderBlocked("close-pool receipt identity or availability differs")
    raw = record.get("raw_payload")
    pool = _pool(raw, target, observed_at=observed, pool_kind=pool_kind)
    if len(pool) != record.get("sample_count") or hashlib.sha256(raw.encode("utf-8")).hexdigest() != record.get("raw_sha256"):
        raise MarketCloseLadderBlocked("close-pool response evidence differs")
    return {**record, "receipt_sha256": digest}, pool


def _receipt_summary(record: dict) -> dict:
    return {key: record[key] for key in ("trade_date", "sample_count", "observed_at", "published_at", "receipt_sha256")}


def verify_market_close_ladder_capture(history, target: date, *, now: datetime) -> dict:
    """Read back the exact immutable capture and its persisted QMT close evidence."""
    cutoff = _stamp(now)
    receipts = {}
    for kind in _POOL_ENDPOINTS:
        evidence = _load_record(target, cutoff, kind)
        if evidence is None:
            raise MarketCloseLadderBlocked(f"{kind} durable receipt is missing")
        record, pool = evidence
        if _qmt_close_hash(target, pool, _load_qmt_closes(history, target, pool, cutoff)) != record["qmt_close_sha256"]:
            raise MarketCloseLadderBlocked(f"{kind} persisted QMT price identity differs")
        receipts[kind] = _receipt_summary(record)
    return {**receipts["limit_up"], "pool_receipts": receipts}


def _capture_pool(primary, history, target: date, pool_kind: str) -> dict:
    """Capture one closed session and atomically retain the source and verification receipt."""
    now = _now()
    if now < datetime.combine(target, time(15)):
        raise MarketCloseLadderBlocked("close-pool target has not closed")
    with mysql_named_lock(primary, f"probiga:close-ladder:{target.isoformat()}") as connection:
        sessions = list(connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(1500) */ trade_status
            FROM si_trade_calendar WHERE trade_date=:day LIMIT 2
        """), {"day": target.isoformat()}).scalars())
        if sessions != [1]:
            raise MarketCloseLadderBlocked("close-pool target is not a verified trading session")
        existing = _load_record(target, now, pool_kind)
        if existing:
            record, pool = existing
            if _qmt_close_hash(target, pool, _load_qmt_closes(history, target, pool, now)) != record["qmt_close_sha256"]:
                raise MarketCloseLadderBlocked("retained close-pool QMT price identity differs")
            return {"status": "COMPLETE", "reused": True, **_receipt_summary(record)}
        _, raw = _fetch_pool(target, timeout_seconds=8, max_payload_bytes=_MAX_RAW, pool_kind=pool_kind)
        observed = _now()
        pool = _pool(raw, target, observed_at=observed, pool_kind=pool_kind)
        quotes = _load_qmt_closes(history, target, pool, observed)
        price_hash = _qmt_close_hash(target, pool, quotes)
        published = _now()
        record = {"schema": SCHEMA, "trade_date": target.isoformat(), "endpoint": _POOL_ENDPOINTS[pool_kind],
                  "observed_at": _clock_text(observed), "published_at": _clock_text(published),
                  "sample_count": len(pool), "raw_payload": raw, "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                  "qmt_close_sha256": price_hash}
        record["receipt_sha256"] = canonical_digest(record)
        path = _path(target, pool_kind)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as staged:
            temporary = Path(staged.name)
            try:
                staged.write(json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
                staged.flush()
                os.fsync(staged.fileno())
            except BaseException:
                staged.close()
                temporary.unlink(missing_ok=True)
                raise
        try:
            # The date-scoped database lock serializes producers. Readers see
            # either no receipt or the complete, synced record.
            os.replace(temporary, path)
            if os.name == "posix":
                directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)
        stored = _load_record(target, published, pool_kind)
        if not stored or stored[0]["receipt_sha256"] != record["receipt_sha256"]:
            raise MarketCloseLadderBlocked("close-pool durable receipt readback differs")
        return {"status": "COMPLETE", "reused": False, **_receipt_summary(record)}


def capture_market_close_ladder(primary, history, target: date) -> dict:
    """Resume the three independent close pools without recapturing valid records."""
    captures = {kind: _capture_pool(primary, history, target, kind) for kind in _POOL_ENDPOINTS}
    return {**captures["limit_up"], "reused": all(item["reused"] for item in captures.values()),
            "pool_receipts": {kind: _receipt_summary(item) for kind, item in captures.items()}}


def load_market_close_ladder(target: date, *, quotes: list[dict], now: datetime) -> dict | None:
    """Read only an immutable exact-date pool whose QMT prices still match the board."""
    evidence = _load_record(target, now)
    if evidence is None:
        return None
    record, pool = evidence
    if _qmt_close_hash(target, pool, quotes) != record["qmt_close_sha256"]:
        raise MarketCloseLadderBlocked("close-pool current QMT price identity differs")
    by_streak = defaultdict(list)
    for row in pool:
        by_streak[row["streak"]].append(row["short_name"])
    additional, receipts = {}, {}
    for kind in ("limit_down", "opened_limit_up"):
        retained = _load_record(target, now, kind)
        if retained is None:
            continue
        extra_record, extra_pool = retained
        if _qmt_close_hash(target, extra_pool, quotes) != extra_record["qmt_close_sha256"]:
            raise MarketCloseLadderBlocked(f"{kind} current QMT price identity differs")
        if {row["stock_code"] for row in pool} & {row["stock_code"] for row in extra_pool}:
            raise MarketCloseLadderBlocked(f"{kind} overlaps the closed limit-up pool")
        additional[kind + "_count"] = len(extra_pool)
        receipts[kind] = _receipt_summary(extra_record)
    opened = additional.get("opened_limit_up_count")
    seal_rate = (100.0 * len(pool) / (len(pool) + opened)
                 if opened is not None and len(pool) + opened else None)
    available_at = max([record["published_at"], *(item["published_at"] for item in receipts.values())])
    return {"trade_date": target.isoformat(), "available_at": available_at,
            "source": SOURCE, "scope": "东财收录样本", "receipt_sha256": record["receipt_sha256"],
            "count": len(pool), "limit_up_count": len(pool), "max_streak": max(by_streak, default=None),
            **additional, "seal_rate_pct": seal_rate, "pool_receipts": receipts,
            "rows": [{"streak": streak, "count": len(names), "stocks": names}
                     for streak, names in sorted(by_streak.items(), reverse=True)]}
