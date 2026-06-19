# -*- coding: utf-8 -*-
"""Configurable stock-minute data source.

The production database can stay small while historical 1-minute bars live in a
separate MySQL instance, for example a workstation reached through VPN or an SSH
tunnel.  Business code should call this module instead of hard-coding
``sm_stock_minute``.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from server.common.config import get_minute_mysql_pool_config, get_minute_mysql_url, get_settings

_MINUTE_ENGINE: Engine | None = None
_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


def get_minute_engine() -> Engine:
    """Return the engine used for stock-minute reads."""
    global _MINUTE_ENGINE
    if _MINUTE_ENGINE is None:
        pool = get_minute_mysql_pool_config()
        _MINUTE_ENGINE = create_engine(
            get_minute_mysql_url(),
            pool_pre_ping=True,
            pool_size=pool["pool_size"],
            max_overflow=pool["max_overflow"],
            pool_recycle=pool["pool_recycle"],
        )
    return _MINUTE_ENGINE


def _configured_source() -> str:
    return (get_settings().minute_data_source or "legacy").strip().lower()


def get_minute_stock_table() -> str:
    """Return the configured stock-minute table name."""
    settings = get_settings()
    table = (settings.minute_stock_table or "").strip()
    if not table:
        source = _configured_source()
        if source in {"gm", "myquant", "goldminer", "ohlc"}:
            table = "sm_stock_minute_gm"
        elif source in {"gml", "jq", "joinquant"}:
            table = "sm_stock_minute_gml"
        else:
            table = "sm_stock_minute"
    if not _TABLE_RE.fullmatch(table):
        raise ValueError(f"Invalid minute table name: {table}")
    return table


def _table_kind(table: str) -> str:
    source = _configured_source()
    if source in {"gm", "myquant", "goldminer", "gml", "jq", "joinquant", "ohlc"}:
        return "ohlc"
    if table in {"sm_stock_minute_gm", "sm_stock_minute_gml"}:
        return "ohlc"
    return "legacy"


def minute_source_info() -> dict[str, Any]:
    table = get_minute_stock_table()
    return {
        "source": _configured_source(),
        "table": table,
        "kind": _table_kind(table),
        "external": bool((get_settings().minute_mysql_url or "").strip()),
    }


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _read_rows(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    with get_minute_engine().connect() as conn:
        result = conn.execute(text(sql), params)
        rows = []
        for row in result.mappings().all():
            rows.append({k: _normalize_value(v) for k, v in dict(row).items()})
        return rows


def _minute_select_sql(table: str, *, single_day: bool, limit: int | None = None) -> str:
    quoted = f"`{table}`"
    date_filter = "trade_date = :d" if single_day else "trade_date >= :s AND trade_date <= :e"
    limit_sql = f"\n        LIMIT {int(limit)}" if limit else ""
    if _table_kind(table) == "ohlc":
        return f"""
        SELECT trade_date,
               trade_time,
               close AS price,
               NULL AS avg_price,
               CASE
                 WHEN pre_close IS NOT NULL AND pre_close > 0 THEN close - pre_close
                 ELSE NULL
               END AS `change`,
               CASE
                 WHEN pre_close IS NOT NULL AND pre_close > 0 THEN (close - pre_close) / pre_close * 100
                 ELSE NULL
               END AS change_pct,
               volume,
               amount,
               open,
               high,
               low,
               close,
               pre_close
        FROM {quoted}
        WHERE stock_code = :c
          AND {date_filter}
          AND close IS NOT NULL
          AND close > 0
          AND TIME(trade_time) >= '09:30:00'
        ORDER BY trade_time ASC{limit_sql}
        """
    return f"""
        SELECT trade_date,
               trade_time,
               price,
               avg_price,
               `change`,
               change_pct,
               volume,
               amount,
               NULL AS open,
               NULL AS high,
               NULL AS low,
               price AS close,
               NULL AS pre_close
        FROM {quoted}
        WHERE stock_code = :c
          AND {date_filter}
          AND price IS NOT NULL
          AND price > 0
          AND TIME(trade_time) >= '09:30:00'
        ORDER BY trade_time ASC{limit_sql}
        """


def get_stock_minute_prices(stock_code: str, start_date: str, end_date: str = "") -> list[dict[str, Any]]:
    """Read ordered minute prices for a stock over a date range."""
    code = str(stock_code).strip().zfill(6)
    end_date = end_date or start_date
    table = get_minute_stock_table()
    return _read_rows(
        _minute_select_sql(table, single_day=False),
        {"c": code, "s": start_date[:10], "e": end_date[:10]},
    )


def get_first_stock_minute_price(stock_code: str, trade_date: str) -> dict[str, Any] | None:
    """Read the first available minute bar for a stock on a trade date."""
    code = str(stock_code).strip().zfill(6)
    table = get_minute_stock_table()
    rows = _read_rows(
        _minute_select_sql(table, single_day=True, limit=1),
        {"c": code, "d": trade_date[:10]},
    )
    if not rows:
        return None
    row = rows[0]
    row["source"] = f"minute:{table}"
    return row


def get_latest_stock_minute_price(stock_code: str, start_date: str) -> dict[str, Any] | None:
    """Read the latest minute bar for a stock from a start date onward."""
    code = str(stock_code).strip().zfill(6)
    table = get_minute_stock_table()
    price_col = "close" if _table_kind(table) == "ohlc" else "price"
    rows = _read_rows(
        f"""
        SELECT trade_date, trade_time, {price_col} AS price
        FROM `{table}`
        WHERE stock_code = :c
          AND trade_date >= :d
          AND {price_col} IS NOT NULL
          AND {price_col} > 0
        ORDER BY trade_time DESC
        LIMIT 1
        """,
        {"c": code, "d": start_date[:10]},
    )
    if not rows:
        return None
    rows[0]["source"] = f"minute:{table}"
    return rows[0]


def get_max_stock_minute_price(stock_code: str, start_date: str, before_date: str) -> float:
    """Read the highest minute price before a trade date for trailing stops."""
    code = str(stock_code).strip().zfill(6)
    table = get_minute_stock_table()
    price_col = "close" if _table_kind(table) == "ohlc" else "price"
    rows = _read_rows(
        f"""
        SELECT MAX({price_col}) AS max_price
        FROM `{table}`
        WHERE stock_code = :c
          AND trade_date >= :s
          AND trade_date < :e
          AND {price_col} IS NOT NULL
          AND {price_col} > 0
        """,
        {"c": code, "s": start_date[:10], "e": before_date[:10]},
    )
    return float((rows[0] if rows else {}).get("max_price") or 0)
