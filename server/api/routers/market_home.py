"""Exact-session market breadth and ranking observations for the home page."""
from __future__ import annotations

import json
import logging
import math
import re
from collections import OrderedDict
from datetime import date, datetime, time, timedelta
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError

from biz.intraday_alert.core import QualityGateError, validate_qmt_snapshot
from server.api.routers._engine import get_engine
from server.common.current_data import get_current_engine
from server.common.kline_data import get_kline_engine
from server.common.sql_reader import read_sql_rows
from server.common.verified_daily_market_truth import VERIFIED_FREE_DAILY_ROW_SOURCES


router = APIRouter(tags=["market-home"])
logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
INDEX_NAMES = {
    "000001": "上证指数",
    "399001": "深证成指",
    "399006": "创业板指",
    "000680": "科创综指",
    "000688": "科创50",
    "000300": "沪深300",
    "000852": "中证1000",
}
BUCKETS = (
    ("below_minus_7", "≤−7%", None, -7),
    ("minus_7_to_5", "−7% 至 −5%", -7, -5),
    ("minus_5_to_3", "−5% 至 −3%", -5, -3),
    ("minus_3_to_1", "−3% 至 −1%", -3, -1),
    ("minus_1_to_0", "−1% 至 0%", -1, 0),
    ("flat", "平盘", 0, 0),
    ("plus_0_to_1", "0% 至 +1%", 0, 1),
    ("plus_1_to_3", "+1% 至 +3%", 1, 3),
    ("plus_3_to_5", "+3% 至 +5%", 3, 5),
    ("plus_5_to_7", "+5% 至 +7%", 5, 7),
    ("above_plus_7", "≥+7%", 7, None),
)
LIVE_SOURCE = "gj_big_qmt_inner"
VERIFIED_DAILY = {"qmt_datadir", LIVE_SOURCE}
_STOCK_LIMIT = 7_000
_INDEX_LIMIT = 2 * len(INDEX_NAMES)
_EVENT_LIMIT = 20
_RANK_LIMIT = 10
_CACHE_TTL = 10
_CACHE_SIZE = 32
_cache: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_cache_lock = Lock()


def _number(value):
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if not isinstance(value, bool) and math.isfinite(result) else None


def _nonnegative(value):
    result = _number(value)
    return result if result is not None and result >= 0 else None


def _clock(value):
    try:
        result = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return result.astimezone(SHANGHAI).replace(tzinfo=None) if result.tzinfo else result


def _timestamp(value: datetime) -> str:
    return value.replace(tzinfo=SHANGHAI).isoformat(timespec="seconds")


def _read(engine, sql: str, params: dict, *, deadline: float) -> list[dict]:
    if monotonic() >= deadline:
        raise TimeoutError("market board read budget exhausted")
    rows = read_sql_rows(engine, sql, params, context="market_home")
    if monotonic() >= deadline:
        raise TimeoutError("market board read budget exhausted")
    return rows


def _live_session(now: datetime) -> bool:
    return time(9, 30) <= now.time() <= time(11, 30) or time(13) <= now.time() < time(15)


def _trading_day(day: str, deadline: float) -> bool:
    rows = _read(get_engine(), """
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ trade_status
        FROM si_trade_calendar WHERE trade_date=:day LIMIT 2
    """, {"day": day}, deadline=deadline)
    return len(rows) == 1 and rows[0]["trade_status"] == 1


def _daily_source_valid(row: dict) -> bool:
    provider = row.get("data_source")
    quality = row.get("quality_status")
    return row.get("permission_status") == "SUPPORTED" and (
        (provider in VERIFIED_DAILY and quality in {"VERIFIED", "QMT_ATTESTED"})
        or (provider in VERIFIED_FREE_DAILY_ROW_SOURCES and quality == "SOURCE_VERIFIED")
    )


def _day_quotes(day: str, now: datetime, deadline: float) -> list[dict]:
    rows = _read(get_kline_engine(), """
        SELECT /*+ MAX_EXECUTION_TIME(2500) */ stock_code, short_name, trade_date,
               close AS price, pre_close, amount, turnover_ratio AS turnover_rate,
               data_source, quality_status, permission_status
        FROM sm_stock_kline
        WHERE trade_date=:day AND k_type=1 AND adjust_type=0 AND etl_sync_at<=:now
          AND permission_status='SUPPORTED' AND quality_status IN ('VERIFIED','QMT_ATTESTED','SOURCE_VERIFIED')
        LIMIT :limit
    """, {"day": day, "now": now, "limit": _STOCK_LIMIT + 1}, deadline=deadline)
    if len(rows) > _STOCK_LIMIT:
        raise ValueError("market board daily rows exceed bound")
    return [row for row in rows if _daily_source_valid(row)]


def _live_quotes(day: str, now: datetime, deadline: float) -> tuple[list[dict], datetime | None]:
    engine = get_current_engine()
    rows = _read(engine, """
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ receipt_id, source_provider,
               source_snapshot_token, source_generated_at, expected_count,
               observed_count, coverage, published_at, capture_mode, quality_status, evidence_json
        FROM st_qmt_realtime_sync_receipt_v2
        WHERE source_provider=:source AND published_at<=:now
        ORDER BY published_at DESC, created_at DESC LIMIT 1
    """, {"source": LIVE_SOURCE, "now": now}, deadline=deadline)
    if not rows:
        return [], None
    receipt = rows[0]
    evidence = receipt.get("evidence_json")
    evidence = json.loads(evidence) if isinstance(evidence, (str, bytes)) else evidence
    batch_id = evidence.get("full_batch_id") if isinstance(evidence, dict) else None
    generated = _clock(receipt.get("source_generated_at"))
    published = _clock(receipt.get("published_at"))
    expected, observed = _number(receipt.get("expected_count")), _number(receipt.get("observed_count"))
    earliest = now - timedelta(seconds=90)
    if (not isinstance(batch_id, str) or not batch_id or not generated or not published
            or not earliest <= generated <= published <= now
            or generated.date().isoformat() != day or not _live_session(generated)
            or not expected or not observed or not 0.95 <= observed / expected <= 1):
        return [], None
    stocks = _read(engine, """
        SELECT /*+ MAX_EXECUTION_TIME(2500) */ stock_code, short_name, price, change_pct, amount,
               snapshot_at, batch_id, data_source, quality_status, permission_status
        FROM sm_stock_current
        WHERE batch_id=:batch_id AND data_source=:source
          AND snapshot_at BETWEEN :earliest AND :now AND permission_status='SUPPORTED'
          AND quality_status IN ('VALIDATED','VERIFIED','VERIFIED_LIVE','QMT_ATTESTED')
        LIMIT :limit
    """, {"batch_id": batch_id, "source": LIVE_SOURCE, "earliest": earliest,
          "now": now, "limit": _STOCK_LIMIT + 1}, deadline=deadline)
    if len(stocks) > _STOCK_LIMIT:
        raise ValueError("market board live rows exceed bound")
    snapshot = validate_qmt_snapshot(receipt, stocks, now=now)
    if snapshot.warnings or len(stocks) != len({row.get("stock_code") for row in stocks}):
        return [], None
    return [dict(row, trade_date=day) for row in snapshot.rows], generated


def _index_quotes(day: str, now: datetime, deadline: float) -> list[dict]:
    params = {f"code_{index}": code for index, code in enumerate(INDEX_NAMES)}
    codes = ", ".join(f":{key}" for key in params)
    params.update(day=day, now=now, limit=_INDEX_LIMIT + 1)
    return _read(get_kline_engine(), f"""
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ index_code, trade_date, trade_time,
               close AS price, change_pct, amount
        FROM sm_index_kline WHERE index_code IN ({codes}) AND trade_date=:day
          AND k_type=1 AND etl_sync_at<=:now
        LIMIT :limit
    """, params, deadline=deadline)


def _unique_rows(rows: list[dict], key: str) -> list[dict]:
    unique = {}
    duplicate = set()
    for row in rows:
        code = str(row.get(key) or "")
        if not re.fullmatch(r"[0-9]{6}", code):
            continue
        if code in unique:
            duplicate.add(code)
        unique[code] = row
    return [row for code, row in unique.items() if code not in duplicate]


def _stock_rows(rows: list[dict], day: str, *, live: bool) -> list[dict]:
    result = []
    for row in _unique_rows(rows, "stock_code"):
        if str(row.get("trade_date"))[:10] != day:
            continue
        price = _number(row.get("price"))
        previous = _number(row.get("pre_close"))
        if price is None or price <= 0:
            continue
        if live:
            change = _number(row.get("change_pct"))
        else:
            change = round((price - previous) / previous * 100, 6) if previous and previous > 0 else None
        if change is None or not math.isfinite(change) or change <= -100:
            continue
        result.append({
            "stock_code": row["stock_code"], "short_name": row.get("short_name") or row["stock_code"],
            "trade_date": day, "price": price, "change_pct": change,
            "amount": _nonnegative(row.get("amount")),
            # DAT/native writers do not update turnover_ratio, which may still
            # contain the previous publisher's value on an upserted row.
            "turnover_rate": (_nonnegative(row.get("turnover_rate"))
                              if row.get("data_source") in VERIFIED_FREE_DAILY_ROW_SOURCES else None),
            "source": row.get("data_source"),
        })
    return result


def _index_rows(rows: list[dict], day: str, as_of: datetime) -> list[dict]:
    if len(rows) > _INDEX_LIMIT:
        return []
    result = {}
    for row in _unique_rows(rows, "index_code"):
        code = row["index_code"]
        price, change = _number(row.get("price")), _number(row.get("change_pct"))
        if code not in INDEX_NAMES or str(row.get("trade_date"))[:10] != day or price is None or price <= 0:
            continue
        trade_time = _clock(row.get("trade_time"))
        if row.get("trade_time") is not None and (not trade_time or trade_time.date().isoformat() != day):
            continue
        result[code] = {
            "index_code": code, "index_name": INDEX_NAMES[code], "trade_date": day, "price": price,
            "change_pct": change if change is not None and change > -100 else None,
            "amount": _nonnegative(row.get("amount")), "source": "sm_index_kline",
            "as_of": _timestamp(as_of),
        }
    return [result[code] for code in INDEX_NAMES if code in result]


def _market_events(day: date, now: datetime, deadline: float) -> list[dict]:
    start = datetime.combine(day, time.min)
    rows = _read(get_engine(), """
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ event_type, direction, stock_code, sector_name,
               snapshot_at, data_source, detail_json
        FROM sm_market_radar_event
        WHERE snapshot_at>=:start AND snapshot_at<:end AND snapshot_at<=:now
          AND created_at>=snapshot_at AND created_at<=:now AND data_source='qmt_full_tick_5level'
          AND event_type IN ('stock_anomaly','sector_anomaly')
        ORDER BY snapshot_at DESC, event_id DESC LIMIT :limit
    """, {"start": start, "end": start + timedelta(days=1), "now": now,
          "limit": _EVENT_LIMIT}, deadline=deadline)
    result = []
    for row in rows:
        stamp = _clock(row.get("snapshot_at"))
        code = str(row.get("stock_code") or "")
        sector = str(row.get("sector_name") or "")
        if not stamp or stamp.date() != day or not _live_session(stamp):
            continue
        if row["event_type"] == "stock_anomaly" and not re.fullmatch(r"[0-9]{6}", code):
            continue
        if row["event_type"] == "sector_anomaly" and not sector:
            continue
        detail = {}
        try:
            raw = json.loads(row.get("detail_json") or "{}")
            if code and isinstance(raw, dict) and isinstance(raw.get("stock"), dict):
                detail["short_name"] = str(raw["stock"].get("short_name") or "")[:128]
        except (TypeError, ValueError):
            pass
        result.append({"event_type": row["event_type"], "direction": row["direction"],
                       "stock_code": code or None, "sector_name": sector or None, "snapshot_at": _timestamp(stamp),
                       "data_source": row["data_source"], "detail_json": detail})
    return result


def _distribution(rows: list[dict]) -> list[dict]:
    values = [row["change_pct"] for row in rows]
    result = []
    for key, label, lower, upper in BUCKETS:
        if key == "flat":
            count = values.count(0)
        elif upper is not None and upper <= 0:
            count = sum(value < 0 and (lower is None or lower < value) and value <= upper for value in values)
        else:
            count = sum(value > 0 and lower <= value and (upper is None or value < upper) for value in values)
        result.append({"key": key, "label": label, "lower": lower, "upper": upper, "count": count})
    return result


def _rank(rows: list[dict], key: str, *, descending: bool) -> list[dict]:
    order = -1 if descending else 1
    return sorted(rows, key=lambda row: (order * row[key], row["stock_code"]))[:_RANK_LIMIT]


def build_market_board(day: date, *, now: datetime) -> dict:
    """Keep exact dated reads and unavailable data boundaries independent."""
    deadline = monotonic() + 8
    day_text = day.isoformat()
    live = day == now.date() and _live_session(now)
    as_of = None
    source = None
    stocks, indices, events = [], [], []
    stage = "not_closed_or_not_trading"
    if day <= now.date() and (day < now.date() or live or now.time() >= time(15)):
        try:
            trading = _trading_day(day_text, deadline)
        except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
            logger.warning("market board calendar unavailable: %s", type(exc).__name__)
            stage, trading = "calendar_unavailable", False
        if trading:
            stage = "market_data_unavailable"
            close = datetime.combine(day, time(15))
            try:
                rows, observed_at = _live_quotes(day_text, now, deadline) if live else (
                    _day_quotes(day_text, now, deadline), close,
                )
                stocks = _stock_rows(rows, day_text, live=live)
                if stocks:
                    as_of = observed_at
                    source = "sm_stock_current" if live else "sm_stock_kline"
            except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError, QualityGateError) as exc:
                logger.warning("market board stock data unavailable: %s", type(exc).__name__)
            # The current index table is shared by publishers and has no
            # per-row provenance. A fresh timestamp alone cannot attest it.
            if not live:
                try:
                    indices = _index_rows(_index_quotes(day_text, now, deadline), day_text, close)
                except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
                    logger.warning("market board index data unavailable: %s", type(exc).__name__)
            try:
                events = _market_events(day, now, deadline)
            except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
                logger.warning("market board events unavailable: %s", type(exc).__name__)
    unavailable = ["limit_ladder"]
    if not events:
        unavailable.append("events")
    amount_complete = bool(stocks) and all(row["amount"] is not None for row in stocks)
    turnover_complete = bool(stocks) and all(row["turnover_rate"] is not None for row in stocks)
    if not stocks:
        unavailable.extend(("distribution", "strong", "rankings.gainers", "rankings.losers"))
    if not amount_complete:
        unavailable.append("rankings.amount")
    if not turnover_complete:
        unavailable.append("rankings.turnover")
    available_indices = {row["index_code"] for row in indices}
    missing_indices = [code for code in INDEX_NAMES if code not in available_indices]
    if missing_indices or any(row["change_pct"] is None for row in indices):
        unavailable.append("indices")
    if not stocks and indices:
        as_of, source = datetime.combine(day, time(15)), "sm_index_kline"
    if not as_of and events:
        as_of, source = _clock(events[0]["snapshot_at"]), "sm_market_radar_event"
    up = [row for row in stocks if row["change_pct"] > 0]
    down = [row for row in stocks if row["change_pct"] < 0]
    return {
        "status": "partial" if as_of else "unavailable",
        "reason": None if as_of else stage,
        "requested_date": day_text, "trade_date": day_text if as_of else None,
        "as_of": _timestamp(as_of) if as_of else None, "source": source,
        "as_of_basis": {"sm_stock_current": "qmt_snapshot", "sm_stock_kline": "exchange_close",
                        "sm_index_kline": "exchange_close", "sm_market_radar_event": "radar_observation"}.get(source),
        "sample_size": len(stocks), "sample_scope": "verified_quotes_with_price_and_change_pct",
        "sample_sources": sorted({row["source"] for row in stocks}),
        "distribution": _distribution(stocks) if stocks else [],
        "strong": {"up_3pct": sum(row["change_pct"] >= 3 for row in up) if stocks else None,
                   "down_3pct": sum(row["change_pct"] <= -3 for row in down) if stocks else None},
        "rankings": {
            "gainers": _rank(up, "change_pct", descending=True),
            "losers": _rank(down, "change_pct", descending=False),
            "amount": _rank(stocks, "amount", descending=True) if amount_complete else [],
            "turnover": _rank(stocks, "turnover_rate", descending=True) if turnover_complete else [],
        },
        "indices": indices, "missing_index_codes": missing_indices,
        "events": events, "limit_ladder": None, "unavailable_fields": unavailable,
    }


@router.get("/monitor/board")
def market_board(requested: str = Query(alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$")) -> dict:
    try:
        selected_day = date.fromisoformat(requested)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="date must be a valid exchange date") from exc
    key = requested
    with _cache_lock:
        cached = _cache.get(key)
        if cached and monotonic() - cached[0] < _CACHE_TTL:
            return cached[1]
    result = build_market_board(selected_day, now=datetime.now(SHANGHAI).replace(tzinfo=None))
    with _cache_lock:
        _cache[key] = (monotonic(), result)
        _cache.move_to_end(key)
        while len(_cache) > _CACHE_SIZE:
            _cache.popitem(last=False)
    return result
