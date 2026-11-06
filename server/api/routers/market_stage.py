"""Read-only market-stage research over the canonical index history."""
from __future__ import annotations

from datetime import date, datetime, time
from functools import lru_cache
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.config import get_kline_mysql_url, get_mysql_url
from server.common.engine_factory import create_pooled_engine
from server.common.index_daily_publication import load_verified_index_daily_bars
from server.common.qmt_trade_calendar import load_trade_calendar_window_receipt
from server.trading_v3.market_stage import MODEL_VERSION, evaluate_market_stage, valid_market_bar
from server.trading_v3.market_stage_replay import replay_market_stage


router = APIRouter(prefix="/market-stage", tags=["market-stage"])
SHANGHAI = ZoneInfo("Asia/Shanghai")
INDEX_CODE = "000001"
SOURCE = "sm_index_kline · 上证指数 000001"
DECISION_TIME = time(20)
LOOKBACK_SESSIONS = 260
MIN_DAILY_SESSIONS = 20
REPLAY_SESSIONS = 20
FORWARD_SESSIONS = 20
REQUEST_SECONDS = 20


@lru_cache(maxsize=2)
def _read_engine(url: str):
    return create_pooled_engine(url, pool_pre_ping=False, pool_size=2, max_overflow=0,
                                pool_timeout=2, connect_args={"connect_timeout": 3,
                                "read_timeout": 3, "write_timeout": 3})


def _index_bars(history: Any, *, start: str, end: str, observed_at: datetime) -> list[dict]:
    with history.connect() as connection:
        rows = connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(2500) */ trade_date, open, high, low, close, amount
            FROM sm_index_kline
            WHERE index_code=:code AND k_type=1
              AND trade_date BETWEEN :start AND :end
            ORDER BY trade_date, trade_time
            LIMIT :limit
        """), {"code": INDEX_CODE, "start": start, "end": end,
                 "limit": LOOKBACK_SESSIONS + FORWARD_SESSIONS + 1}).mappings().all()
    if len(rows) > LOOKBACK_SESSIONS + FORWARD_SESSIONS:
        raise ValueError("基准指数行数超过日历范围")
    return [{**row, "available_at": observed_at} for row in rows if valid_market_bar(row)]


def build_market_stage(primary: Any, history: Any, *, trade_date: date | None,
                       now: datetime, replay: bool = False) -> dict:
    """Build a reproducible view without treating late imports as earlier facts."""
    deadline = monotonic() + REQUEST_SECONDS
    now = now.astimezone(SHANGHAI) if now.tzinfo else now.replace(tzinfo=SHANGHAI)
    latest = authoritative_closed_trade_date(primary, now=now)
    target = trade_date.isoformat() if trade_date else latest
    point_in_time = replay or trade_date is not None
    envelope = {"trade_date": target, "source": SOURCE, "model_version": MODEL_VERSION,
                "evidence_mode": "VERIFIED_PUBLICATION" if point_in_time else "CURRENT_OBSERVATION"}
    if not target or target > latest:
        return {**envelope, "status": "DATA_BLOCKED", "reason": "请求日期尚无可用的收盘交易日"}

    target_day = date.fromisoformat(target)
    cutoff = min(now, datetime.combine(target_day, DECISION_TIME, SHANGHAI)) if trade_date else now
    with primary.connect() as connection:
        calendar = load_trade_calendar_window_receipt(
            connection, end_date=latest if replay else target,
            required_sessions=(REPLAY_SESSIONS + (FORWARD_SESSIONS if trade_date is None else 0)
                               if replay else MIN_DAILY_SESSIONS),
            decision_known_at=now if replay else cutoff,
            deadline=deadline,
        )
        if replay:
            known_sessions = list(calendar.sessions_between(calendar.start_date, latest))
            if target not in known_sessions:
                raise ValueError("回放日期不是交易日")
            end_index = known_sessions.index(target) if trade_date else len(known_sessions) - 1 - FORWARD_SESSIONS
            if end_index < REPLAY_SESSIONS - 1:
                raise ValueError("回放区间的日历样本不足")
            end = known_sessions[end_index]
            first_decision = known_sessions[end_index - REPLAY_SESSIONS + 1]
            outcome_end = known_sessions[min(len(known_sessions) - 1, end_index + FORWARD_SESSIONS)]
            historical_calendar = load_trade_calendar_window_receipt(
                connection, end_date=first_decision,
                required_sessions=MIN_DAILY_SESSIONS,
                decision_known_at=datetime.combine(date.fromisoformat(first_decision), DECISION_TIME, SHANGHAI),
                deadline=deadline,
            )
            warmup = list(historical_calendar.sessions_between(historical_calendar.start_date, first_decision))
            start = warmup[-(LOOKBACK_SESSIONS - REPLAY_SESSIONS + 1):][0]
            if (historical_calendar.end_date < outcome_end
                    or historical_calendar.sessions_between(first_decision, outcome_end)
                    != calendar.sessions_between(first_decision, outcome_end)):
                raise ValueError("历史决策日历与结果区间不一致")
            calendar = historical_calendar
        else:
            closed = list(calendar.sessions_between(calendar.start_date, target))
            start = closed[-LOOKBACK_SESSIONS:][0]

    sessions = list(calendar.sessions_between(start, calendar.end_date))
    envelope["calendar_window"] = {
        "start_date": start,
        "end_date": end if replay else target,
        "sessions": len(calendar.sessions_between(start, end if replay else target)),
    }
    if monotonic() >= deadline:
        raise TimeoutError("市场阶段行情读取超时")
    if point_in_time:
        bars = load_verified_index_daily_bars(
            primary, history, sessions=calendar.sessions_between(start, outcome_end if replay else target),
            code=INDEX_CODE, as_of=now if replay else cutoff, deadline=deadline,
        )
    else:
        bars = _index_bars(history, start=start, end=target, observed_at=now)
    if monotonic() >= deadline:
        raise TimeoutError("市场阶段计算预算已用尽")
    if replay:
        report = replay_market_stage(
            bars, trading_sessions=sessions, start_date=first_decision,
            end_date=end, outcome_as_of=now,
        )
        return {**envelope, "status": report["status"], "replay": report,
                **({"reason": "回放区间缺少决策时点已发布的指数版本或连续行情。"}
                   if report["status"] == "DATA_BLOCKED" else {}),
                "calendar_batch_id": calendar.batch_id}

    snapshot = evaluate_market_stage(
        (bar for bar in bars if valid_market_bar(bar)), as_of=cutoff,
        trading_sessions=sessions, target_session=target,
    )
    if snapshot["periods"]["daily"]["end_date"] != target:
        reason = ("目标日缺少在决策时间前已发布且可核验的指数日线" if point_in_time
                  else "最近收盘交易日缺少有效指数日线")
        return {**envelope, "status": "DATA_BLOCKED", "reason": reason}
    return {**envelope, "status": snapshot["status"], "snapshot": snapshot,
            "calendar_batch_id": calendar.batch_id}


def _response(trade_date: date | None, *, replay: bool = False) -> dict:
    try:
        return build_market_stage(
            _read_engine(get_mysql_url(required=True)), _read_engine(get_kline_mysql_url()), trade_date=trade_date,
            now=datetime.now(SHANGHAI), replay=replay,
        )
    except (SQLAlchemyError, TimeoutError, ValueError, RuntimeError) as exc:
        return {"status": "DATA_BLOCKED", "trade_date": trade_date.isoformat() if trade_date else None,
                "source": SOURCE, "model_version": MODEL_VERSION,
                "evidence_mode": "VERIFIED_PUBLICATION" if replay or trade_date is not None else "CURRENT_OBSERVATION",
                "reason": str(exc) if isinstance(exc, TimeoutError) else "指数行情或交易日历的覆盖及可用时间无法核验"}


@router.get("/current")
def current_market_stage(trade_date: date | None = Query(default=None)) -> dict:
    return _response(trade_date)


@router.get("/replay")
def market_stage_replay(trade_date: date | None = Query(default=None)) -> dict:
    return _response(trade_date, replay=True)
