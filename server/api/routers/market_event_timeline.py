"""Browse persisted QMT market events without claiming session coverage."""
from __future__ import annotations

import base64
import binascii
import json
import logging
import re
from datetime import date, datetime, time
from functools import lru_cache
from time import monotonic
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from biz.market_radar.core import EVENT_TABLE as RADAR_EVENT_TABLE
from server.common.config import get_current_mysql_url, get_mysql_url
from server.common.engine_factory import create_pooled_engine
from server.common.limit_up_events import LIMIT_STATUS_BASIS
from server.common.limit_up_event_schema import LIMIT_UP_EVENT_TABLE


router = APIRouter(tags=["market-event-timeline"])
logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
Stream = Literal["limit_up", "radar"]
_STOCK = re.compile(r"[0-9]{6}")
_MAX_ID = 2**63 - 1


@lru_cache(maxsize=2)
def _engine(url: str):
    return create_pooled_engine(
        url, pool_pre_ping=False, pool_size=1, max_overflow=0, pool_timeout=1,
        connect_args={"connect_timeout": 2, "read_timeout": 2, "write_timeout": 2},
    )


def _clock(value) -> datetime:
    return datetime.fromisoformat(str(value))


def _timestamp(value: datetime) -> str:
    return value.replace(tzinfo=SHANGHAI).isoformat()


def _cursor(day, stream, cutoff, through_id, event_at, event_id) -> str:
    payload = [day.isoformat(), stream, cutoff.isoformat(), through_id, event_at.isoformat(), event_id]
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")


def _cursor_bounds(cursor: str, day: date, stream: Stream, now: datetime) -> tuple:
    if not cursor:
        return now, None, datetime.combine(day, time(9, 30)), 0
    try:
        packed = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        token_day, token_stream, cutoff_text, through_id, after_text, after_id = json.loads(packed)
        cutoff, after = _clock(cutoff_text), _clock(after_text)
        if (token_day != day.isoformat() or token_stream != stream or cutoff.tzinfo or after.tzinfo
                or not datetime.combine(day, time(9, 30)) <= after <= cutoff <= now
                or after.date() != day or not _in_session(after)
                or type(through_id) is not int or type(after_id) is not int
                or not 0 <= after_id <= through_id <= _MAX_ID
                or (after_id == 0 and after != datetime.combine(day, time(9, 30)))):
            raise ValueError("invalid cursor boundary")
        return cutoff, through_id, after, after_id
    except (TypeError, ValueError, binascii.Error) as exc:
        raise HTTPException(status_code=422, detail="cursor does not match this date or source") from exc


def _in_session(value: datetime) -> bool:
    stamp = value.time()
    return time(9, 30) <= stamp < time(11, 31) or time(13) <= stamp <= time(15, 1)


def _object(raw):
    try:
        value = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _project(row: dict, stream: Stream, day: date) -> dict | None:
    code = row["stock_code"] or ""
    kind = row["event_type"]
    event_at, available_at = _clock(row["event_at"]), _clock(row["available_at"])
    if (not _in_session(event_at) or event_at.date() != day
            or available_at < event_at):
        return None
    details = _object(row["detail"])
    if stream == "limit_up":
        source_at = _clock(row["source_at"])
        if (not _STOCK.fullmatch(code) or not re.fullmatch(r"[a-f0-9]{64}", row["event_key"] or "")
                or not event_at <= source_at <= available_at or source_at.date() != day
                or details.get("limit_status_basis") != LIMIT_STATUS_BASIS
                or details.get("at_limit") is not (kind != "BREAK")
                or (kind != "TOUCH" and details.get("sealed") is not (kind != "BREAK"))):
            return None
        subject = row["short_name"] or code
    else:
        source_at = event_at
        if kind == "stock_anomaly" and not _STOCK.fullmatch(code):
            return None
        if kind == "sector_anomaly" and not row["sector_name"]:
            return None
        stock = details.get("stock")
        stock_name = stock.get("short_name") if isinstance(stock, dict) else None
        subject = row["sector_name"] or stock_name or code
    return {
        "event_id": str(row["event_id"]),
        "event_type": kind,
        "event_at": _timestamp(event_at),
        "source_snapshot_at": _timestamp(source_at),
        "available_at": _timestamp(available_at),
        "data_source": row["provider"],
        "stock_code": code or None,
        "subject": str(subject)[:192],
        "direction": row["direction"],
    }


def read_timeline(engine, day: date, stream: Stream, *, limit: int, cursor: str, now: datetime) -> dict:
    cutoff, through_id, after_at, after_id = _cursor_bounds(cursor, day, stream, now)
    deadline = monotonic() + 5
    start, morning_end = datetime.combine(day, time(9, 30)), datetime.combine(day, time(11, 31))
    afternoon, end = datetime.combine(day, time(13)), datetime.combine(day, time(15, 1))
    is_limit = stream == "limit_up"
    table = LIMIT_UP_EVENT_TABLE if is_limit else RADAR_EVENT_TABLE
    clock_column = "event_at" if is_limit else "snapshot_at"
    params = {"day": day.isoformat(), "start": start, "morning_end": morning_end, "afternoon": afternoon,
              "end": end, "cutoff": cutoff, "after_at": after_at, "after_id": after_id, "limit": limit + 1,
              "basis": LIMIT_STATUS_BASIS}
    response = {
        "trade_date": day.isoformat(),
        "stream": stream,
        "source": table,
        "coverage": {"status": "UNVERIFIED", "reason_code": "NO_SESSION_COVERAGE_RECEIPT"},
        "known_through": _timestamp(cutoff),
        "rows": [],
        "discarded_count": 0,
        "page_cursor": None,
        "next_cursor": None,
    }
    if day > now.date():
        return {**response, "status": "DATA_BLOCKED", "reason_code": "SESSION_IN_FUTURE"}
    if is_limit:
        fields = """
            event_key, stock_code, short_name, event_type, event_at, source_snapshot_at AS source_at,
            created_at AS available_at, source_provider AS provider, NULL AS direction,
            SUBSTR(evidence_json, 1, 8193) AS detail
        """
        source_filter = """
            AND trade_date=:day AND source_provider='gj_big_qmt_inner' AND limit_status_basis=:basis
            AND event_at<=source_snapshot_at AND source_snapshot_at<=created_at
            AND event_type IN ('TOUCH','SEAL','RESEAL','BREAK','QUEUE_SNAPSHOT')
        """
    else:
        fields = """
            stock_code, sector_name, event_type, snapshot_at AS event_at, created_at AS available_at,
            data_source AS provider, direction, SUBSTR(detail_json, 1, 8193) AS detail
        """
        source_filter = """
            AND data_source='qmt_full_tick_5level' AND snapshot_at<=created_at
            AND event_type IN ('stock_anomaly','sector_anomaly')
        """
    try:
        with engine.connect() as connection:
            if through_id is None:
                through_id = connection.execute(text(f"""
                    SELECT /*+ MAX_EXECUTION_TIME(1200) */ COALESCE(MAX(event_id),0) FROM {table}
                """)).scalar_one()
            if monotonic() >= deadline:
                raise TimeoutError("timeline cursor boundary read timed out")
            records = connection.execute(text(f"""
                SELECT /*+ MAX_EXECUTION_TIME(1500) */ event_id, {fields}
                FROM {table}
                WHERE {clock_column}>=:start AND {clock_column}<=:end
                  AND ({clock_column}<:morning_end OR {clock_column}>=:afternoon)
                  AND created_at<=:cutoff AND event_id<=:through_id
                  AND ({clock_column}>:after_at OR ({clock_column}=:after_at AND event_id>:after_id))
                  {source_filter}
                ORDER BY {clock_column}, event_id LIMIT :limit
            """), {**params, "through_id": through_id}).mappings().all()
        if monotonic() >= deadline:
            raise TimeoutError("timeline page read timed out")
    except (SQLAlchemyError, TimeoutError) as exc:
        logger.warning("market event timeline %s read failed: %s", stream, type(exc).__name__)
        return {**response, "status": "DATA_BLOCKED", "reason_code": "EVENT_STORE_UNAVAILABLE"}

    if cutoff >= start:
        response["page_cursor"] = _cursor(day, stream, cutoff, through_id, after_at, after_id)
    page = records[:limit]
    for row in page:
        item = _project(row, stream, day)
        if item is None:
            response["discarded_count"] += 1
        else:
            response["rows"].append(item)
    if len(records) > limit:
        last = page[-1]
        response["next_cursor"] = _cursor(day, stream, cutoff, through_id, _clock(last["event_at"]), last["event_id"])
    return {**response, "status": "AVAILABLE" if response["rows"] else "NO_VERIFIED_RECORDS"}


@router.get("/monitor/event-timeline")
def market_event_timeline(
    trade_date: str = Query(alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    stream: Stream = "limit_up",
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str = Query(default="", max_length=320),
) -> dict:
    try:
        day = date.fromisoformat(trade_date)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="date must be a valid calendar date") from exc
    now = datetime.now(SHANGHAI).replace(tzinfo=None)
    try:
        url = get_current_mysql_url() if stream == "limit_up" else get_mysql_url()
        engine = _engine(url)
    except (SQLAlchemyError, RuntimeError, ValueError) as exc:
        logger.warning("market event timeline %s configuration unavailable: %s", stream, type(exc).__name__)
        return {"trade_date": trade_date, "stream": stream, "status": "DATA_BLOCKED", "rows": [],
                "reason_code": "EVENT_STORE_UNAVAILABLE", "next_cursor": None,
                "coverage": {"status": "UNVERIFIED", "reason_code": "NO_SESSION_COVERAGE_RECEIPT"}}
    return read_timeline(engine, day, stream, limit=limit, cursor=cursor, now=now)
