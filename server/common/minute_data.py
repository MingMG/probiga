# -*- coding: utf-8 -*-
"""Configurable stock-minute and persisted capital-flow data source.

The production database can stay small while historical 1-minute bars live in a
separate MySQL instance, for example a workstation reached through VPN or an SSH
tunnel.  Business code should call this module instead of hard-coding
``sm_stock_minute``.
"""
from __future__ import annotations

import re
import threading
from typing import Any

from sqlalchemy.engine import Engine

from server.common.batch_db import quote_identifier
from server.common.config import get_minute_mysql_pool_config, get_minute_mysql_url, get_settings
from server.common.engine_factory import create_pooled_engine
from server.common.sql_reader import read_sql_rows

_MINUTE_ENGINE: Engine | None = None
_MINUTE_ENGINE_LOCK = threading.Lock()

CAPITAL_FLOW_TABLES = {
    "sm_stock_capital_flow_daily",
    "sm_stock_capital_flow_min",
}
_TABLE_REF_RE = re.compile(r"\b(?:FROM|JOIN)\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE)


def get_minute_engine() -> Engine:
    """Return the engine used for minute/flow market-data reads and writes."""
    global _MINUTE_ENGINE
    if _MINUTE_ENGINE is None:
        with _MINUTE_ENGINE_LOCK:
            if _MINUTE_ENGINE is None:
                pool = get_minute_mysql_pool_config()
                _MINUTE_ENGINE = create_pooled_engine(
                    get_minute_mysql_url(),
                    pool_config=pool,
                )
    return _MINUTE_ENGINE


def dispose_minute_engine() -> None:
    """Dispose the shared minute-data engine if it has been initialized."""
    global _MINUTE_ENGINE
    with _MINUTE_ENGINE_LOCK:
        engine = _MINUTE_ENGINE
        _MINUTE_ENGINE = None
        if engine is not None:
            engine.dispose()


def should_use_capital_flow_engine(sql: str) -> bool:
    """Route pure persisted capital-flow queries to the market-data DB.

    Queries that join business tables deliberately stay on the primary engine;
    cross-database joins are not safe when the configured URLs point at
    different MySQL instances.
    """
    tables = {match.group(1).lower() for match in _TABLE_REF_RE.finditer(sql or "")}
    return bool(tables) and tables.issubset(CAPITAL_FLOW_TABLES)


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
    try:
        quote_identifier(table)
    except ValueError as exc:
        raise ValueError(f"Invalid minute table name: {table}") from exc
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


def _read_rows(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return read_sql_rows(
        get_minute_engine(),
        sql,
        params,
        context="minute_data",
        stringify_datetime=True,
    )


def _minute_select_sql(table: str, *, single_day: bool, limit: int | None = None) -> str:
    quoted = quote_identifier(table)
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
