"""Strict trade-calendar helpers; no weekday fallback for V2 actions."""
from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine


def is_trade_day(engine: Engine, day: date) -> bool:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT COUNT(*) FROM si_trade_calendar
                WHERE trade_date = :trade_date AND trade_status = 1
                """
            ),
            {"trade_date": day},
        ).scalar()
    return bool(int(value or 0))


def latest_trade_day(engine: Engine, on_or_before: date) -> date:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT MAX(trade_date) FROM si_trade_calendar
                WHERE trade_date <= :trade_date AND trade_status = 1
                """
            ),
            {"trade_date": on_or_before},
        ).scalar()
    if value is None:
        raise RuntimeError("trade calendar has no eligible date")
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
