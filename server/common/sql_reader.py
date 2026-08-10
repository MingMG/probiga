# -*- coding: utf-8 -*-
"""Small SQL read helpers for API hot paths."""
from __future__ import annotations

import logging
import math
import re
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.config import get_api_observability_config

logger = logging.getLogger(__name__)


def normalize_sql_value(value: Any, *, stringify_datetime: bool = False) -> Any:
    """Normalize DB scalar values into JSON-friendly Python values."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if stringify_datetime and isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if stringify_datetime and isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def sql_preview(sql: str, *, limit: int = 240) -> str:
    """Return a compact SQL preview without params or credentials."""
    preview = re.sub(r"\s+", " ", str(sql)).strip()
    if len(preview) <= limit:
        return preview
    return preview[: limit - 3] + "..."


def _slow_sql_ms() -> int:
    try:
        return int(get_api_observability_config().get("slow_sql_ms", 0))
    except Exception:
        return 0


def read_sql_rows(
    engine: Engine,
    sql: str,
    params: dict | None = None,
    *,
    context: str = "sql",
    stringify_datetime: bool = False,
) -> list[dict]:
    """Execute a SELECT-like query and return normalized mapping rows."""
    start = time.perf_counter()
    rows: list[dict] = []
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            rows = [
                {
                    key: normalize_sql_value(value, stringify_datetime=stringify_datetime)
                    for key, value in row.items()
                }
                for row in result.mappings().all()
            ]
            return rows
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        threshold_ms = _slow_sql_ms()
        if threshold_ms and elapsed_ms >= threshold_ms:
            logger.warning(
                "Slow SQL [%s] completed in %.2fms rows=%s sql=%s",
                context,
                elapsed_ms,
                len(rows),
                sql_preview(sql),
            )
