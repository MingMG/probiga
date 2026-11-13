"""Exact-session market breadth and ranking observations for the home page."""
from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
from collections import OrderedDict, defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError

from biz.intraday_alert.core import QualityGateError, validate_qmt_snapshot
from server.api.routers._engine import get_engine
from server.common.current_data import get_current_engine
from server.common.daily_delivery_control import load_completed_market_capture_receipt
from server.common.kline_data import get_kline_engine
from server.common.limit_up_events import LIMIT_STATUS_BASIS
from server.common.limit_up_event_schema import LIMIT_UP_EVENT_TABLE
from server.common.market_close_ladder import load_market_close_ladder
from server.common.qmt_attestation_contract import canonical_digest
from server.common.sector_heatmap_store import read_concept_names, read_industry_flow
from server.common.sql_reader import read_sql_rows
from server.common.turnover_snapshot import (
    TURNOVER_CAPTURE_KIND,
    TURNOVER_PROVIDER,
    TURNOVER_SOURCE_FIELD,
    TURNOVER_UNIT,
    _reconstruct_persisted_capture,
)
from server.common.turnover_snapshot_schema import TURNOVER_SNAPSHOT_ROW_TABLE, TURNOVER_SNAPSHOT_RUN_TABLE
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
_TURNOVER_CORRECTION_LIMIT = 16
_CACHE_TTL = 10
_CLOSED_CACHE_TTL = 30
_CACHE_SIZE = 32
_LIVE_MAX_AGE = timedelta(seconds=90)
_HEATMAP_SESSIONS = 5
_HEATMAP_TAIL = 6
_HEATMAP_BOARD_LIMIT = 1_500
_HEATMAP_MINUTES = 240
_HEATMAP_CONCEPT_SOURCE = "东财概念行情"
_HEATMAP_INDUSTRY_SOURCE = "东财一级行业行情"
_HEATMAP_CONCEPT_FLOW_SOURCE = "东财概念主力净流入"
_HEATMAP_INDUSTRY_FLOW_SOURCE = "东财一级行业主力净流入"
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


def _fresh_live_stamp(stamp: datetime | None, now: datetime) -> bool:
    return bool(stamp and stamp.date() == now.date() and _live_session(stamp) and _live_session(now)
                and timedelta(0) <= now - stamp <= _LIVE_MAX_AGE)


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
        SELECT /*+ MAX_EXECUTION_TIME(2500) */ id, stock_code, short_name, trade_date,
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


def _verified_turnover(day: str, quotes: list[dict], stocks: list[dict], now: datetime, deadline: float) -> dict | None:
    """Reuse the sealed capture stage and match its rates to the board's current quote set."""
    engine = get_engine()
    params = {"day": day, "now": now, "count": len(stocks), "kind": TURNOVER_CAPTURE_KIND,
              "provider": TURNOVER_PROVIDER, "field": TURNOVER_SOURCE_FIELD, "unit": TURNOVER_UNIT}
    runs = _read(engine, f"""
        SELECT /*+ MAX_EXECUTION_TIME(1200) */ run_id, collector_build_sha, decision_at, published_at, expected_count,
               semantic_sha256, field_value_root_sha256
        FROM {TURNOVER_SNAPSHOT_RUN_TABLE}
        WHERE target_date=:day AND published_at<=:now AND decision_at<=:now
          AND capture_kind=:kind AND provider=:provider AND source_field=:field AND unit=:unit
          AND status='COMPLETED' AND expected_count BETWEEN 1 AND :count AND expected_count=fetched_count
          AND expected_count=valid_count AND expected_count=matched_count AND expected_count=promoted_count
        ORDER BY expected_count DESC, decision_at DESC, published_at DESC, run_id DESC LIMIT 1
    """, params, deadline=deadline)
    if not runs:
        return None
    run = runs[0]
    expected_count = run["expected_count"]
    completion = load_completed_market_capture_receipt(
        engine, raw_run_id=run["run_id"], stage_name="target_turnover_snapshot", target_date=day,
        build_sha=run["collector_build_sha"], decision_at=now,
    )
    receipt = completion["receipt"]
    known_at, published_at = _clock(completion["known_at"]), _clock(run["published_at"])
    decision_at = _clock(run["decision_at"])
    if (not decision_at or not known_at or not published_at or not decision_at <= published_at <= known_at <= now
            or _clock(receipt.get("decision_at")) != decision_at or _clock(receipt.get("published_at")) != published_at
            or any(receipt.get(key) != run[key] for key in ("semantic_sha256", "field_value_root_sha256"))
            or receipt.get("expected_count") != expected_count or receipt.get("promoted_count") != expected_count):
        return None
    rows = _read(engine, f"""
        SELECT /*+ MAX_EXECUTION_TIME(2000) */ r.stock_code, CAST(r.field_value_decimal AS CHAR) AS rate,
               q.id, q.close AS price, q.pre_close, q.amount, q.data_source,
               CASE WHEN q.volume<>r.qmt_volume_shares OR q.amount<>r.qmt_amount THEN 1 ELSE 0 END AS corrected
        FROM {TURNOVER_SNAPSHOT_ROW_TABLE} r LEFT JOIN sm_stock_kline q
          ON q.id=r.target_row_id AND q.stock_code=r.stock_code AND q.trade_date=r.trade_date AND q.k_type=r.k_type
          AND q.adjust_type=r.adjust_type AND q.open=r.qmt_open AND q.high=r.qmt_high
          AND q.low=r.qmt_low AND q.close=r.qmt_close
          AND q.data_source=r.qmt_data_source AND q.quality_status=r.qmt_quality_status
          AND q.permission_status=r.qmt_permission_status
          AND ((q.volume=r.qmt_volume_shares AND q.amount=r.qmt_amount
                AND (q.turnover_ratio IS NULL OR q.turnover_ratio=r.field_value_decimal))
            OR (q.data_source='qmt_datadir' AND q.open=r.source_open AND q.high=r.source_high
                AND q.low=r.source_low AND q.close=r.source_close AND q.volume=r.source_volume_shares
                AND ABS(q.amount-r.source_amount)<=0.50 AND q.turnover_ratio=r.field_value_decimal))
        WHERE r.run_id=:run_id AND r.trade_date=:day AND r.k_type=1 AND r.adjust_type=0
          AND r.validation_status='MATCHED' AND r.promoted_at<=:now
        ORDER BY r.stock_code LIMIT :limit
    """, {"day": day, "now": now, "run_id": run["run_id"], "limit": expected_count + 1}, deadline=deadline)
    if len(rows) != expected_count:
        return None
    quote_by_code = {row["stock_code"]: row for row in _unique_rows(quotes, "stock_code")}
    stock_by_code = {row["stock_code"]: row for row in stocks}
    fields = ("id", "price", "pre_close", "amount", "data_source")
    seen, values, corrected, root_items = set(), {}, {}, []
    for row in rows:
        code = row["stock_code"]
        quote = quote_by_code.get(code)
        rate = _nonnegative(row["rate"])
        if code not in stock_by_code or code in seen or quote is None or rate is None:
            return None
        seen.add(code)
        root_items.append({"stock_code": code, "trade_date": day,
                           "turnover_percent": format(Decimal(str(row["rate"])).normalize(), "f") if rate else "0"})
        if row["id"] is not None and all(row[field] == quote[field] for field in fields):
            (corrected if row["corrected"] else values)[code] = rate
    if canonical_digest(root_items) != run["field_value_root_sha256"]:
        return None
    if 0 < len(corrected) <= _TURNOVER_CORRECTION_LIMIT:
        keys = {f"code_{index}": code for index, code in enumerate(corrected)}
        corrected_rows = _read(engine, f"""
            SELECT /*+ MAX_EXECUTION_TIME(1200) */ * FROM {TURNOVER_SNAPSHOT_ROW_TABLE}
            WHERE run_id=:run_id AND trade_date=:day AND k_type=1 AND adjust_type=0
              AND stock_code IN ({', '.join(f':{key}' for key in keys)}) AND length(raw_payload)<=16384
            LIMIT :limit
        """, {**keys, "day": day, "run_id": run["run_id"], "limit": len(corrected) + 1}, deadline=deadline)
        # QMT DAT records traded amount to yuan. The second binding permits that
        # rounding only after the original source row has been reconstructed.
        for row in _unique_rows(corrected_rows, "stock_code"):
            try:
                captured = _reconstruct_persisted_capture(row, decision_at=decision_at, published_at=published_at)
            except (RuntimeError, ValueError):
                continue
            code = captured.target.stock_code
            if code in corrected and captured.turnover_percent == Decimal(str(corrected[code])):
                values[code] = corrected[code]
    if not values:
        return None
    for stock in stocks:
        rate = values.get(stock["stock_code"])
        stock["turnover_rate"] = rate
        if rate is not None:
            stock["turnover_rate_source"] = TURNOVER_PROVIDER
    return {"run_id": run["run_id"], "source": TURNOVER_PROVIDER,
            "decision_at": decision_at.replace(tzinfo=SHANGHAI).isoformat(),
            "published_at": published_at.replace(tzinfo=SHANGHAI).isoformat(),
            "known_at": _timestamp(known_at), "field_value_root_sha256": run["field_value_root_sha256"],
            "sample_size": len(stocks), "verified_count": len(values),
            "corrected_binding_count": len(values.keys() & corrected.keys())}


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
    earliest = now - _LIVE_MAX_AGE
    if (not isinstance(batch_id, str) or not batch_id or not generated or not published
            or not generated <= published <= now
            or generated.date().isoformat() != day or not _fresh_live_stamp(generated, now)
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


def _index_quotes(day: str, close_at: datetime, now: datetime, deadline: float) -> list[dict]:
    params = {f"code_{index}": code for index, code in enumerate(INDEX_NAMES)}
    codes = ", ".join(f":{key}" for key in params)
    params.update(day=day, close_at=close_at, now=now, limit=_INDEX_LIMIT + 1)
    return _read(get_kline_engine(), f"""
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ index_code, trade_date, trade_time, etl_sync_at,
               close AS price, change_pct, amount
        FROM sm_index_kline WHERE index_code IN ({codes}) AND trade_date=:day
          AND k_type=1 AND trade_time=:close_at AND etl_sync_at BETWEEN :close_at AND :now
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
        if change is not None and (not math.isfinite(change) or change <= -100):
            change = None
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


def _index_rows(rows: list[dict], day: str, close_at: datetime, now: datetime) -> list[dict]:
    if len(rows) > _INDEX_LIMIT:
        return []
    result = {}
    for row in _unique_rows(rows, "index_code"):
        code = row["index_code"]
        price, change = _number(row.get("price")), _number(row.get("change_pct"))
        if code not in INDEX_NAMES or str(row.get("trade_date"))[:10] != day or price is None or price <= 0:
            continue
        ingested_at = _clock(row.get("etl_sync_at"))
        if _clock(row.get("trade_time")) != close_at or not ingested_at or not close_at <= ingested_at <= now:
            continue
        # Daily publishers use 15:00 as the exchange close marker. A matching
        # post-close ingestion proves a current store observation, not a PIT receipt.
        result[code] = {
            "index_code": code, "index_name": INDEX_NAMES[code], "trade_date": day, "price": price,
            "change_pct": change if change is not None and change > -100 else None,
            "amount": _nonnegative(row.get("amount")), "source": "sm_index_kline",
            "as_of": _timestamp(close_at), "ingested_at": _timestamp(ingested_at),
            "evidence_mode": "CURRENT_OBSERVATION",
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


def _limit_up_events(day: date, now: datetime, deadline: float) -> list[dict]:
    rows = _read(get_current_engine(), f"""
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ event_type, stock_code, short_name,
               event_at, source_snapshot_at, created_at, source_provider, evidence_json
        FROM {LIMIT_UP_EVENT_TABLE}
        WHERE trade_date=:day AND event_at BETWEEN :opening AND :close_window
          AND event_at<=source_snapshot_at AND source_snapshot_at<=created_at AND created_at<=:now
          AND source_provider=:provider AND limit_status_basis=:basis
          AND event_type IN ('TOUCH','SEAL','RESEAL','BREAK')
        ORDER BY event_at DESC, event_id DESC LIMIT :limit
    """, {"day": day.isoformat(), "opening": datetime.combine(day, time(9, 30)),
          "close_window": datetime.combine(day, time(15, 1)), "now": now,
          "provider": LIVE_SOURCE, "basis": LIMIT_STATUS_BASIS, "limit": _EVENT_LIMIT}, deadline=deadline)
    result = []
    for row in rows:
        event_at, source_at, known_at = (_clock(row.get(key)) for key in ("event_at", "source_snapshot_at", "created_at"))
        code, kind = row.get("stock_code"), row.get("event_type")
        if (not event_at or not source_at or not known_at or not event_at <= source_at <= known_at <= now
                or event_at.date() != day or source_at.date() != day or known_at.date() != day
                or not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code)):
            continue
        try:
            evidence = json.loads(row.get("evidence_json") or "{}")
        except (TypeError, ValueError):
            continue
        if (not isinstance(evidence, dict) or evidence.get("limit_status_basis") != LIMIT_STATUS_BASIS
                or evidence.get("at_limit") is not (kind != "BREAK")
                or (kind in {"SEAL", "RESEAL", "BREAK"} and evidence.get("sealed") is not (kind != "BREAK"))):
            continue
        result.append({"event_type": kind, "event_family": "LIMIT_UP_LIFECYCLE", "stock_code": code,
                       "snapshot_at": event_at.replace(tzinfo=SHANGHAI).isoformat(),
                       "source_snapshot_at": source_at.replace(tzinfo=SHANGHAI).isoformat(),
                       "available_at": known_at.replace(tzinfo=SHANGHAI).isoformat(),
                       "data_source": row["source_provider"], "detail_json": {"short_name": str(row.get("short_name") or "")[:128]}})
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
    started_at = monotonic()
    day_text = day.isoformat()
    same_day = day == now.date()
    live = same_day and _live_session(now)
    lunch = same_day and time(11, 30) < now.time() < time(13)
    close_ready = day < now.date() or (same_day and now.time() >= time(15))
    deadline = started_at + (12 if close_ready else 8)
    as_of = None
    source = None
    turnover_evidence = None
    limit_ladder = None
    stocks, indices, events = [], [], []
    stage = "not_closed_or_not_trading"
    if close_ready or live or lunch:
        try:
            trading = _trading_day(day_text, deadline)
        except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
            logger.warning("market board calendar unavailable: %s", type(exc).__name__)
            stage, trading = "calendar_unavailable", False
        if trading:
            stage = "lunch_pause" if lunch else "market_data_unavailable"
            close = datetime.combine(day, time(15))
            if live or close_ready:
                try:
                    quotes, observed_at = _live_quotes(day_text, now, deadline) if live else (
                        _day_quotes(day_text, now, deadline), close,
                    )
                    stocks = _stock_rows(quotes, day_text, live=live)
                    if stocks:
                        as_of = observed_at
                        source = "sm_stock_current" if live else "sm_stock_kline"
                except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError, QualityGateError) as exc:
                    logger.warning("market board stock data unavailable: %s", type(exc).__name__)
            # The current index table is shared by publishers and has no
            # per-row provenance. A fresh timestamp alone cannot attest it.
            if close_ready:
                try:
                    indices = _index_rows(_index_quotes(day_text, close, now, deadline), day_text, close, now)
                except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
                    logger.warning("market board index data unavailable: %s", type(exc).__name__)
            if close_ready and stocks:
                try:
                    limit_ladder = load_market_close_ladder(day, quotes=quotes, now=now)
                except (OSError, RuntimeError, ValueError) as exc:
                    logger.warning("market board close limit ladder unavailable: %s", type(exc).__name__)
            if close_ready and stocks and any(row["turnover_rate"] is None for row in stocks):
                try:
                    turnover_evidence = _verified_turnover(day_text, quotes, stocks, now, deadline)
                except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
                    logger.warning("market board turnover evidence unavailable: %s", type(exc).__name__)
            try:
                events = _limit_up_events(day, now, deadline)
            except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
                logger.warning("market board QMT limit events unavailable: %s", type(exc).__name__)
            try:
                radar = _market_events(day, now, deadline)
                events = sorted([*events, *radar], key=lambda row: _clock(row["snapshot_at"]), reverse=True)[:_EVENT_LIMIT]
            except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
                logger.warning("market board radar events unavailable: %s", type(exc).__name__)
    if live and stocks and not _fresh_live_stamp(as_of, now + timedelta(seconds=monotonic() - started_at)):
        stocks = []
        as_of, source, stage = None, None, "live_quote_expired"
    unavailable = [] if limit_ladder is not None else ["limit_ladder"]
    if not events:
        unavailable.append("events")
    amount_complete = bool(stocks) and all(row["amount"] is not None for row in stocks)
    turnover_rows = [row for row in stocks if row["turnover_rate"] is not None]
    if turnover_evidence is None and len(turnover_rows) != len(stocks):
        turnover_rows = []
    return_stocks = [row for row in stocks if row["change_pct"] is not None]
    if not return_stocks:
        unavailable.extend(("distribution", "strong", "rankings.gainers", "rankings.losers"))
    if not amount_complete:
        unavailable.append("rankings.amount")
    if not turnover_rows:
        unavailable.append("rankings.turnover")
    available_indices = {row["index_code"] for row in indices}
    missing_indices = [code for code in INDEX_NAMES if code not in available_indices]
    if missing_indices or any(row["change_pct"] is None for row in indices):
        unavailable.append("indices")
    if not stocks and indices:
        as_of, source = datetime.combine(day, time(15)), "sm_index_kline"
    if not as_of and events:
        as_of = _clock(events[0]["snapshot_at"])
        source = LIMIT_UP_EVENT_TABLE if events[0].get("event_family") == "LIMIT_UP_LIFECYCLE" else "sm_market_radar_event"
    up = [row for row in return_stocks if row["change_pct"] > 0]
    down = [row for row in return_stocks if row["change_pct"] < 0]
    status = "unavailable"
    if as_of:
        status = "partial" if unavailable or len(turnover_rows) != len(stocks) else "available"
    return {
        "status": status,
        "reason": None if as_of else stage,
        "requested_date": day_text, "trade_date": day_text if as_of else None,
        "as_of": _timestamp(as_of) if as_of else None, "source": source,
        "as_of_basis": {"sm_stock_current": "qmt_snapshot", "sm_stock_kline": "exchange_close",
                        "sm_index_kline": "exchange_close", "sm_market_radar_event": "radar_observation",
                        LIMIT_UP_EVENT_TABLE: "qmt_limit_event"}.get(source),
        "sample_size": len(return_stocks), "sample_scope": "verified_quotes_with_price_and_change_pct",
        "price_sample_size": len(stocks),
        "sample_sources": sorted({row["source"] for row in stocks}),
        "distribution": _distribution(return_stocks) if return_stocks else [],
        "strong": {"up_3pct": sum(row["change_pct"] >= 3 for row in up) if return_stocks else None,
                   "down_3pct": sum(row["change_pct"] <= -3 for row in down) if return_stocks else None},
        "rankings": {
            "gainers": _rank(up, "change_pct", descending=True),
            "losers": _rank(down, "change_pct", descending=False),
            "amount": _rank(stocks, "amount", descending=True) if amount_complete else [],
            "turnover": _rank(turnover_rows, "turnover_rate", descending=True),
        },
        "turnover_evidence": turnover_evidence,
        "turnover_coverage": {
            "status": ("unavailable" if not turnover_rows else
                       "complete" if len(turnover_rows) == len(stocks) else "partial"),
            "verified_count": len(turnover_rows), "sample_size": len(stocks),
            "excluded_count": len(stocks) - len(turnover_rows),
            "excluded_codes": (sorted(row["stock_code"] for row in stocks if row["turnover_rate"] is None)[:20]
                               if turnover_evidence else []),
        },
        "indices": indices, "missing_index_codes": missing_indices,
        "events": events, "limit_ladder": limit_ladder, "unavailable_fields": unavailable,
    }


@router.get("/monitor/board")
def market_board(requested: str = Query(alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$")) -> dict:
    try:
        selected_day = date.fromisoformat(requested)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="date must be a valid exchange date") from exc
    key = requested
    now = datetime.now(SHANGHAI).replace(tzinfo=None)
    with _cache_lock:
        cached = _cache.get(key)
        if cached:
            captured, board = cached
            is_live = board.get("source") == "sm_stock_current"
            is_complete_close = board.get("as_of_basis") == "exchange_close" and board.get("status") == "available"
            ttl = _CLOSED_CACHE_TTL if is_complete_close else _CACHE_TTL
            if monotonic() - captured < ttl and (not is_live or _fresh_live_stamp(_clock(board.get("as_of")), now)):
                return board
    result = build_market_board(selected_day, now=now)
    with _cache_lock:
        _cache[key] = (monotonic(), result)
        _cache.move_to_end(key)
        while len(_cache) > _CACHE_SIZE:
            _cache.popitem(last=False)
    return result


def _sector_rank(rows: list[dict], source: str) -> dict:
    ranked = sorted(rows, key=lambda row: (-row["value"], row["name"], row["code"]))
    positive = [row for row in ranked if row["value"] > 0]
    negative = sorted((row for row in ranked if row["value"] < 0),
                      key=lambda row: (row["value"], row["name"], row["code"]))
    flat = [row for row in ranked if row["value"] == 0]
    displayed = positive[:_HEATMAP_TAIL] + negative[:_HEATMAP_TAIL]
    if len(displayed) < _HEATMAP_TAIL:
        displayed += flat[:_HEATMAP_TAIL - len(displayed)]
    note = f"{source} · {len(rows)} 个有数据板块" if rows else f"{source} · 当日数据待核验"
    return {"status": "available" if rows else "unavailable", "rows": displayed, "note": note}


def _native_close_series(window: list[dict]) -> list[float]:
    closes = [_number(row["close"]) for row in window]
    changes = [_number(row["change"]) for row in window]
    if any(close is None or close <= 0 for close in closes) or any(change is None for change in changes):
        return []
    previous = [close - change for close, change in zip(closes, changes)]
    if previous[0] <= 0 or any(abs(prev - close) > 0.02 for prev, close in zip(previous[1:], closes)):
        return []
    return [0.0, *[round((close / previous[0] - 1) * 100, 8) for close in closes]]


def _sector_changes(rows: list[dict], names: dict[str, str], sessions: list[str], source: str, *, native=False) -> tuple[dict, dict]:
    history: dict[str, dict[str, dict | None]] = defaultdict(dict)
    selected = sessions[-1]
    for row in rows:
        code, session = str(row["code"] or ""), str(row["session"])[:10]
        if not code or session not in sessions or (native and not re.fullmatch(r"BK[0-9]{4,8}", code)):
            continue
        value = _number(row["change_pct"])
        # A duplicated identity is ambiguous even when one copy looks valid.
        valid = value is not None and value > -100 and session not in history[code]
        history[code][session] = {**row, "change_pct": value} if valid else None

    daily, five_day = [], []
    for code, changes in history.items():
        current = changes.get(selected)
        if current is None:
            continue
        identity = {"code": code, "name": names.get(code, code), "source": source}
        today = {**identity, "value": current["change_pct"]}
        daily.append(today)
        window = [changes.get(session) for session in sessions]
        if len(window) != _HEATMAP_SESSIONS or any(value is None for value in window):
            continue
        if native:
            series = _native_close_series(window)
        else:
            gross_return = 1.0
            series = [0.0]
            for row in window:
                gross_return *= 1 + row["change_pct"] / 100
                series.append(round((gross_return - 1) * 100, 8))
        if series:
            curve = {"series": series, "series_label": "近 5 个交易日"}
            five_day.append({**identity, "value": series[-1], **curve})
            if not native:
                today.update(curve)
    basis = "连续 5 个交易日收盘价" if native else "连续 5 个交易日复合涨幅"
    return _sector_rank(daily, source), _sector_rank(five_day, f"{source} · {basis}")


def _sector_read(engine_factory, sql, params, *, deadline, limit, stage):
    try:
        rows = _read(engine_factory(), sql, {**params, "limit": limit + 1}, deadline=deadline)
        if len(rows) > limit:
            raise ValueError("sector heatmap row limit exceeded")
        return rows
    except (SQLAlchemyError, RuntimeError, TimeoutError, ValueError) as exc:
        logger.warning("sector heatmap %s unavailable: %s", stage, type(exc).__name__)
        return []


def _concept_minute_curves(metric: dict, closes: dict, selected: str, now: datetime, deadline: float) -> None:
    codes = [row["code"] for row in metric["rows"]]
    if not codes:
        return
    bindings = {f"code_{index}": code for index, code in enumerate(codes)}
    placeholders = ", ".join(f":{key}" for key in bindings)
    rows = _sector_read(get_kline_engine, f"""
        SELECT /*+ MAX_EXECUTION_TIME(1400) */ index_code, trade_time, price
        FROM sm_concept_east_minute
        WHERE trade_date = :selected AND trade_time >= :selected AND trade_time < :tomorrow AND etl_sync_at <= :now
          AND index_code IN ({placeholders})
        ORDER BY index_code, trade_time LIMIT :limit
    """, {**bindings, "selected": selected, "tomorrow": date.fromisoformat(selected) + timedelta(days=1),
          "now": now}, deadline=deadline, limit=len(codes) * _HEATMAP_MINUTES, stage="concept minute curves")
    opens = (datetime.fromisoformat(selected + "T09:30:00"), datetime.fromisoformat(selected + "T13:00:00"))
    expected = [opened + timedelta(minutes=minute) for opened in opens for minute in range(1, 121)]
    by_code = defaultdict(list)
    for row in rows:
        by_code[row["index_code"]].append(row)
    for row in metric["rows"]:
        minutes = by_code[row["code"]]
        if [_clock(point["trade_time"]) for point in minutes] != expected:
            continue
        # Keep a bounded, time-ordered series of actual observations, including the close.
        prices = [_number(point["price"]) for point in minutes]
        close = closes.get(row["code"])
        if close is not None and all(price is not None and price > 0 for price in prices) and abs(prices[-1] - close) <= 0.02:
            row["series"] = [prices[0], *prices[4::5]]
            row["series_label"] = "当日分时"


def _read_sector_store(reader, selected: str, now: datetime) -> list[dict]:
    try:
        return reader(selected, now=now)
    except (OSError, sqlite3.Error, ValueError) as exc:
        logger.warning("sector heatmap %s unavailable: %s", reader.__name__, type(exc).__name__)
        return []


def _concept_close_coverage(selected: str, now: datetime, codes: set[str], deadline: float) -> dict | None:
    if not codes:
        return None
    rows = _sector_read(get_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(1000) */ output
        FROM st_scheduled_task_history
        WHERE task_type='eastmoney_concept_kline' AND status IN ('success','degraded')
          AND exit_code=0 AND run_at>=:selected AND finished_at<=:now
        ORDER BY id DESC LIMIT :receipt_limit
    """, {"selected": selected, "now": now, "receipt_limit": 6}, deadline=deadline, limit=6,
        stage="concept close coverage")
    from server.common.scheduler_validation import (
        _eastmoney_concept_market_output_status, _eastmoney_concept_market_payload,
        _notice_code_set_hash,
    )
    for row in rows:
        output = row.get("output")
        receipt = _eastmoney_concept_market_payload(output)
        if not receipt or receipt.get("target_trade_date") != selected:
            continue
        if _eastmoney_concept_market_output_status("eastmoney_concept_kline", output, return_code=0) != "degraded":
            return None
        directory = receipt["directory"]
        coverage = directory["session_coverage"]
        excluded = {item["code"] for item in coverage["exclusions"]}
        if (not codes & excluded and _notice_code_set_hash(codes) == coverage["eligible_code_set_sha256"]
                and _notice_code_set_hash(codes | excluded) == directory["code_set_sha256"]):
            return {"observed_count": directory["observed_count"], "eligible_count": len(codes),
                    "excluded_count": len(excluded), "reason": "NATIVE_DAILY_BAR_UNAVAILABLE"}
        return None
    return None


def _concept_flow_history_coverage(selected: str, now: datetime, codes: set[str], deadline: float) -> dict | None:
    if not codes:
        return None
    rows = _sector_read(get_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(1000) */ output
        FROM st_scheduled_task_history
        WHERE task_type='eastmoney_concept_flow_snapshot' AND status IN ('success','degraded')
          AND exit_code=0 AND run_at>=:selected AND finished_at<=:now
          AND (output LIKE :compact_date OR output LIKE :spaced_date)
        ORDER BY id DESC LIMIT :receipt_limit
    """, {"selected": selected, "now": now, "receipt_limit": 12,
          "compact_date": f'%"source_date":"{selected}"%',
          "spaced_date": f'%"source_date": "{selected}"%'}, deadline=deadline, limit=12,
        stage="historical concept flow coverage")
    from server.common.scheduler_validation import _eastmoney_concept_flow_payload
    from tools.fetch_concept_flow_datacenter import validate_task_result

    for row in rows:
        receipt = _eastmoney_concept_flow_payload(row.get("output"))
        if not receipt or receipt.get("source_date") != selected:
            continue
        try:
            if validate_task_result(dict(receipt), 0, expected_session=selected) != "degraded":
                return None
            manifest = receipt["manifest"]
            if manifest["code_set_hash"] != canonical_digest(sorted(codes)):
                return None
            native = manifest["native_history"]
            return {"observed_count": native["roster_count"], "eligible_count": native["available_count"],
                    "excluded_count": native["missing_count"], "source": "eastmoney.push2his"}
        except (KeyError, TypeError, ValueError):
            return None
    return None


def build_sector_heatmap(day: date, *, now: datetime) -> dict:
    selected = day.isoformat()
    concept = {"change": _sector_rank([], _HEATMAP_CONCEPT_SOURCE),
               "five_day_return": _sector_rank([], _HEATMAP_CONCEPT_SOURCE),
               "net_inflow": _sector_rank([], _HEATMAP_CONCEPT_FLOW_SOURCE)}
    industry = {"change": _sector_rank([], _HEATMAP_INDUSTRY_SOURCE),
                "five_day_return": _sector_rank([], _HEATMAP_INDUSTRY_SOURCE),
                "net_inflow": _sector_rank([], _HEATMAP_INDUSTRY_FLOW_SOURCE)}
    result = {"requested_date": selected, "trade_date": None, "concept": concept, "industry": industry}
    if day > now.date() or (day == now.date() and now.time() < time(15)):
        return result
    deadline = monotonic() + 8
    sessions = _sector_read(get_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(1200) */ trade_date FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date <= :selected
        ORDER BY trade_date DESC LIMIT :session_limit
    """, {"selected": selected, "session_limit": _HEATMAP_SESSIONS}, deadline=deadline,
        limit=_HEATMAP_SESSIONS, stage="trading calendar")
    sessions = [str(row["trade_date"])[:10] for row in reversed(sessions)]
    if not sessions or sessions[-1] != selected or len(set(sessions)) != len(sessions):
        return result
    result["trade_date"] = selected
    bounds = {"selected": selected, "start": sessions[0], "now": now}
    changes = _sector_read(get_kline_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(1800) */ index_code AS code, trade_date AS session, trade_time,
               change_pct, close, `change`
        FROM sm_concept_east_kline WHERE k_type=1 AND trade_date BETWEEN :start AND :selected
          AND etl_sync_at <= :now LIMIT :limit
    """, bounds, deadline=deadline, limit=len(sessions) * _HEATMAP_BOARD_LIMIT, stage="concept closes")
    changes = [row for row in changes if _clock(row["trade_time"]) == _clock(row["session"])]
    flows = _sector_read(get_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(1400) */ index_code AS code, MAX(index_name) AS name,
               MAX(main_net_inflow) AS main_net_inflow, MAX(max_net_inflow) AS max_net_inflow, MAX(lg_net_inflow) AS lg_net_inflow
        FROM sm_concept_capital_flow_east WHERE days_type=1 AND snapshot_at=:snapshot AND etl_sync_at <= :now
          AND index_code LIKE 'BK%' GROUP BY index_code HAVING COUNT(*)=1 LIMIT :limit
    """, {**bounds, "snapshot": selected + " 00:00:00"}, deadline=deadline,
        limit=_HEATMAP_BOARD_LIMIT, stage="concept capital flow")
    names = {row["code"]: row["name"] for row in flows if row["name"]}
    names.update({row["index_code"]: row["index_name"] for row in _read_sector_store(read_concept_names, selected, now)})
    concept["change"], concept["five_day_return"] = _sector_changes(changes, names, sessions, _HEATMAP_CONCEPT_SOURCE, native=True)
    flow_rows = []
    for row in flows:
        code = row["code"]
        main, big, largest = map(_number, (row["main_net_inflow"], row["lg_net_inflow"], row["max_net_inflow"]))
        if re.fullmatch(r"BK[0-9]{4,8}", code) and None not in (main, big, largest) and abs(main - big - largest) <= 0.03:
            flow_rows.append({"code": code, "name": names.get(code, code), "value": main, "source": _HEATMAP_CONCEPT_FLOW_SOURCE})
    concept["net_inflow"] = _sector_rank(flow_rows, _HEATMAP_CONCEPT_FLOW_SOURCE)

    industry_changes = _sector_read(get_engine, """
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ concept_name AS code, concept_code AS native_code,
               snapshot_date AS session, change_pct, hot_tag
        FROM st_hot_concept_ths_daily WHERE plate_type=3 AND snapshot_date BETWEEN :start AND :selected
          AND hot_tag LIKE '东财一级行业%' AND etl_sync_at <= :now LIMIT :limit
    """, bounds, deadline=deadline, limit=len(sessions) * 100, stage="industry closes")
    # The collector's fixed industry names identify the board. Its representative
    # vendor code may change when a primary industry aggregates several children.
    industry_names = {row["code"]: row["code"] for row in industry_changes if row["code"]}
    industry["change"], industry["five_day_return"] = _sector_changes(
        industry_changes, industry_names, sessions, _HEATMAP_INDUSTRY_SOURCE,
    )
    native_industries = defaultdict(set)
    for row in industry_changes:
        if str(row["session"])[:10] == selected and row["hot_tag"] == "东财一级行业成交额":
            native_industries[row["native_code"]].add(row["code"])
    industry_flows = []
    for row in _read_sector_store(read_industry_flow, selected, now):
        name_set = native_industries[row["index_code"]]
        if len(name_set) == 1:
            industry_flows.append({"code": row["index_code"], "name": next(iter(name_set)),
                                   "value": float(row["main_net_inflow"]), "source": _HEATMAP_INDUSTRY_FLOW_SOURCE})
    industry["net_inflow"] = _sector_rank(industry_flows, _HEATMAP_INDUSTRY_FLOW_SOURCE)
    closes = {row["code"]: _number(row["close"]) for row in changes if str(row["session"])[:10] == selected}
    coverage = _concept_close_coverage(selected, now, set(closes), deadline)
    if coverage:
        note = (f"收盘行情已核验 {coverage['eligible_count']}/{coverage['observed_count']} 个板块，"
                f"{coverage['excluded_count']} 个未返回收盘行情")
        for metric in ("change", "five_day_return"):
            concept[metric]["coverage"] = coverage
            concept[metric]["note"] += f" · {note}"
    flow_coverage = _concept_flow_history_coverage(selected, now, {row["code"] for row in flow_rows}, deadline)
    if flow_coverage:
        metric = concept["net_inflow"]
        metric["coverage"] = flow_coverage
        metric["note"] = (f"东财历史日资金流 · 已核验 {flow_coverage['eligible_count']}/"
                          f"{flow_coverage['observed_count']} 个当日板块")
        if flow_coverage["excluded_count"]:
            metric["note"] += f"，{flow_coverage['excluded_count']} 个未返回可核验数据"
        for row in metric["rows"]:
            row["source"] = "东财历史日资金流"
    _concept_minute_curves(concept["change"], closes, selected, now, deadline)
    return result


@router.get("/monitor/sector-heatmap")
def sector_heatmap(requested: str = Query(alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$")) -> dict:
    try:
        selected_day = date.fromisoformat(requested)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="date must be a valid exchange date") from exc
    return build_sector_heatmap(selected_day, now=datetime.now(SHANGHAI).replace(tzinfo=None))
