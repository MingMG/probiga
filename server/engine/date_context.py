# -*- coding: utf-8 -*-
"""Helpers for anchoring historical analysis to the intended trade date."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any


def coerce_date(value: Any, default: date | None = None) -> date | None:
    """Convert common date-like values into ``date`` objects."""
    if value in (None, ""):
        return default
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return default

    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return default


def extract_analysis_date(data: dict, default: date | None = None) -> date | None:
    """Read the analysis anchor date from loader output."""
    return coerce_date(data.get("trade_date"), default=default)
