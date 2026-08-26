"""Disclosure-period applicability rules for full-market finance coverage."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class FinanceDisclosureGate:
    minimum_report_date: date
    disclosure_deadline: date


def finance_disclosure_gate(as_of: date) -> FinanceDisclosureGate:
    """Return the latest reporting period whose statutory window has closed."""

    if as_of.month <= 4:
        return FinanceDisclosureGate(
            minimum_report_date=date(as_of.year - 1, 9, 30),
            disclosure_deadline=date(as_of.year - 1, 10, 31),
        )
    if as_of.month <= 8:
        return FinanceDisclosureGate(
            minimum_report_date=date(as_of.year, 3, 31),
            disclosure_deadline=date(as_of.year, 4, 30),
        )
    if as_of.month <= 10:
        return FinanceDisclosureGate(
            minimum_report_date=date(as_of.year, 6, 30),
            disclosure_deadline=date(as_of.year, 8, 31),
        )
    return FinanceDisclosureGate(
        minimum_report_date=date(as_of.year, 9, 30),
        disclosure_deadline=date(as_of.year, 10, 31),
    )


def coerce_optional_date(value: Any) -> date | None:
    if value in (None, "", "NaT"):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid listing date: {value!r}") from exc


def report_period_gate_applies(
    listing_date: date | None,
    gate: FinanceDisclosureGate,
) -> bool:
    """A post-deadline listing is not bound to the earlier periodic filing."""

    return listing_date is None or listing_date <= gate.disclosure_deadline


__all__ = [
    "FinanceDisclosureGate",
    "coerce_optional_date",
    "finance_disclosure_gate",
    "report_period_gate_applies",
]
