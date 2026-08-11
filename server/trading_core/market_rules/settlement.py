"""Trading-calendar-aware T+N sellability functions."""

from __future__ import annotations

from datetime import date
from typing import Iterable, Sequence

from ..contracts import PositionLot


def _settlement_days(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("settlement_days must be an integer")
    if value < 0:
        raise ValueError("settlement_days cannot be negative")
    return value


def _calendar(trading_days: Sequence[date]) -> tuple[date, ...]:
    days = tuple(trading_days)
    if not days:
        raise ValueError("trading_days cannot be empty")
    if any(type(item) is not date for item in days):
        raise TypeError("trading_days must contain exact date values")
    if days != tuple(sorted(set(days))):
        raise ValueError("trading_days must be strictly increasing and unique")
    return days


def earliest_sell_date(
    *,
    acquired_on: date,
    trading_days: Sequence[date],
    settlement_days: int = 1,
) -> date | None:
    """Return the first sellable session, or None if calendar ends first."""

    settlement_days = _settlement_days(settlement_days)
    if type(acquired_on) is not date:
        raise TypeError("acquired_on must be exactly date")
    days = _calendar(trading_days)
    try:
        acquired_index = days.index(acquired_on)
    except ValueError as exc:
        raise ValueError("acquired_on must be present in trading_days") from exc
    target_index = acquired_index + settlement_days
    return days[target_index] if target_index < len(days) else None


def is_lot_sellable(
    lot: PositionLot,
    *,
    on_date: date,
    trading_days: Sequence[date],
    settlement_days: int = 1,
) -> bool:
    settlement_days = _settlement_days(settlement_days)
    if type(on_date) is not date:
        raise TypeError("on_date must be exactly date")
    days = _calendar(trading_days)
    if on_date not in days:
        return False
    first_sell = earliest_sell_date(
        acquired_on=lot.acquired_on,
        trading_days=days,
        settlement_days=settlement_days,
    )
    return first_sell is not None and on_date >= first_sell


def sellable_quantity(
    lots: Iterable[PositionLot],
    *,
    on_date: date,
    trading_days: Sequence[date],
    settlement_days: int = 1,
) -> int:
    settlement_days = _settlement_days(settlement_days)
    return sum(
        lot.quantity
        for lot in lots
        if is_lot_sellable(
            lot,
            on_date=on_date,
            trading_days=trading_days,
            settlement_days=settlement_days,
        )
    )


def locked_quantity(
    lots: Iterable[PositionLot],
    *,
    on_date: date,
    trading_days: Sequence[date],
    settlement_days: int = 1,
) -> int:
    settlement_days = _settlement_days(settlement_days)
    lot_list = tuple(lots)
    total = sum(lot.quantity for lot in lot_list)
    return total - sellable_quantity(
        lot_list,
        on_date=on_date,
        trading_days=trading_days,
        settlement_days=settlement_days,
    )
