"""Authoritative post-close trading-day clock shared by every write path."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text


PRODUCTION_TIMEZONE = ZoneInfo("Asia/Shanghai")
DAILY_CLOSE_READY_HOUR = 18


def authoritative_closed_trade_date(engine, now: datetime | None = None) -> str:
    """Return the latest exchange session whose daily inputs may be closed.

    On a trading day the current session is intentionally unavailable until
    18:00 Asia/Shanghai.  Weekends and exchange holidays naturally resolve to
    the most recent open session in ``si_trade_calendar``.
    """

    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    comparator = (
        "<="
        if (current.hour, current.minute, current.second)
        >= (DAILY_CLOSE_READY_HOUR, 0, 0)
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
