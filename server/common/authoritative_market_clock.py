"""Authoritative post-close trading-day clock shared by every write path."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, time
from zoneinfo import ZoneInfo

from sqlalchemy import text


PRODUCTION_TIMEZONE = ZoneInfo("Asia/Shanghai")
DAILY_CLOSE_READY_HOUR = 18
DAILY_CLOSE_READY_TIME = time(DAILY_CLOSE_READY_HOUR, 0)


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
