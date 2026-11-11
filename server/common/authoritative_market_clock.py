"""Authoritative post-close trading-day clock shared by every write path."""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Literal
from zoneinfo import ZoneInfo

from sqlalchemy import text

from tools.qmt_announcement_task_contract import (
    ANALYSIS_DAILY_PIPELINE_DECISION_TIME, PREMARKET_ANNOUNCEMENT_DECISION_TIME,
)


PRODUCTION_TIMEZONE = ZoneInfo("Asia/Shanghai")
DAILY_CLOSE_READY_HOUR = 18
DAILY_CLOSE_READY_TIME = time(DAILY_CLOSE_READY_HOUR, 0)
STOCK_MINUTE_CAPTURE_READY_TIME = time(15, 35)
STRATEGY_POOL_INITIAL_TIME = time.fromisoformat(ANALYSIS_DAILY_PIPELINE_DECISION_TIME)
STRATEGY_POOL_PREMARKET_REVISION_TIME = time.fromisoformat(PREMARKET_ANNOUNCEMENT_DECISION_TIME)
STRATEGY_POOL_PREMARKET_PUBLICATION_DEADLINE = time(9, 25)
StrategyPoolDecisionPhase = Literal["INITIAL", "PREMARKET_REVISION"]


@dataclass(frozen=True)
class StrategyPoolDecisionSlot:
    phase: StrategyPoolDecisionPhase
    data_trade_date: str
    execution_session_date: str
    scheduled_at: datetime
    evaluated_at: datetime


def resolve_strategy_pool_decision_slot(
    engine,
    *,
    data_trade_date: str,
    phase: StrategyPoolDecisionPhase,
    evaluated_at: datetime,
) -> StrategyPoolDecisionSlot:
    """Resolve the fixed slot and record when its evaluation actually occurs.

    Naive scheduler timestamps use Shanghai time. A delayed evaluation keeps
    its actual timestamp and the original scheduled timestamp.
    """
    if phase not in ("INITIAL", "PREMARKET_REVISION"):
        raise ValueError("unknown strategy pool decision phase")
    if not isinstance(data_trade_date, str):
        raise ValueError("data_trade_date must use YYYY-MM-DD")
    data_session = date.fromisoformat(data_trade_date)
    if data_session.isoformat() != data_trade_date:
        raise ValueError("data_trade_date must use YYYY-MM-DD")
    if not isinstance(evaluated_at, datetime):
        raise ValueError("evaluated_at must be a datetime")
    actual = (
        evaluated_at.replace(tzinfo=PRODUCTION_TIMEZONE)
        if evaluated_at.utcoffset() is None
        else evaluated_at.astimezone(PRODUCTION_TIMEZONE)
    )

    with (engine.connect() if hasattr(engine, "connect") else nullcontext(engine)) as connection:
        open_sessions = connection.execute(
            text(
                "SELECT trade_date FROM si_trade_calendar WHERE trade_status=1 "
                "AND trade_date >= :data_trade_date ORDER BY trade_date LIMIT 2"
            ),
            {"data_trade_date": data_trade_date},
        ).scalars().all()
    if not open_sessions or str(open_sessions[0]) != data_trade_date:
        raise ValueError("data_trade_date is not an open trading session")
    if len(open_sessions) != 2:
        raise ValueError("next open trading session is unavailable")
    execution_session_date = str(open_sessions[1])
    execution_session = date.fromisoformat(execution_session_date)
    if execution_session <= data_session or execution_session.isoformat() != execution_session_date:
        raise ValueError("next open trading session is invalid")

    if phase == "INITIAL":
        scheduled_at = datetime.combine(data_session, STRATEGY_POOL_INITIAL_TIME, PRODUCTION_TIMEZONE)
    else:
        scheduled_at = datetime.combine(
            execution_session, STRATEGY_POOL_PREMARKET_REVISION_TIME, PRODUCTION_TIMEZONE
        )
    if actual < scheduled_at:
        raise ValueError("evaluation precedes the scheduled strategy pool decision slot")
    return StrategyPoolDecisionSlot(phase, data_trade_date, execution_session_date, scheduled_at, actual)


def authoritative_elapsed_trade_date(engine, now: datetime | None = None) -> str:
    """Latest exchange date strictly before today's Shanghai civil date.

    Use this for data products whose completion contract requires the source
    session's natural day to have ended. Stock minutes have their own native
    daily-close proof and use ``authoritative_stock_minute_trade_date``.
    """
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    with (engine.connect() if hasattr(engine, "connect") else nullcontext(engine)) as connection:
        value = connection.execute(text(
            "SELECT MAX(trade_date) FROM si_trade_calendar "
            "WHERE trade_status=1 AND trade_date < :today"
        ), {"today": current.date().isoformat()}).scalar()
    return str(value or "")[:10]


def authoritative_closed_trade_date(
    engine,
    now: datetime | None = None,
    *,
    close_ready_time: time = DAILY_CLOSE_READY_TIME,
) -> str:
    """Return the latest exchange session whose daily inputs may be closed.

    On a trading day the current session is intentionally unavailable until
    ``close_ready_time`` (18:00 by default). Weekends and exchange holidays
    naturally resolve to the most recent open session in
    ``si_trade_calendar``.
    """

    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    comparator = (
        "<="
        if current.time().replace(tzinfo=None) >= close_ready_time
        else "<"
    )
    connection_scope = (
        engine.connect() if hasattr(engine, "connect") else nullcontext(engine)
    )
    with connection_scope as connection:
        value = connection.execute(
            text(
                "SELECT MAX(trade_date) FROM si_trade_calendar "
                "WHERE trade_status=1 "
                f"AND trade_date {comparator} :today"
            ),
            {"today": current.date().isoformat()},
        ).scalar()
    return str(value or "")[:10]


def authoritative_stock_minute_trade_date(
    engine,
    now: datetime | None = None,
) -> str:
    """Select the stock-minute collection target after the final daily close.

    This clock only schedules a candidate partition. Its publisher still
    requires native daily finality evidence, a complete minute grid and an
    exact daily/minute close match before certifying the partition.
    """

    return authoritative_closed_trade_date(
        engine,
        now=now,
        close_ready_time=STOCK_MINUTE_CAPTURE_READY_TIME,
    )
