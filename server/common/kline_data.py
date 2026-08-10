# -*- coding: utf-8 -*-
"""Dedicated market K-line database routing."""
from __future__ import annotations

import re
import threading

from sqlalchemy.engine import Engine

from server.common.config import get_kline_mysql_url, get_minute_mysql_pool_config
from server.common.engine_factory import create_pooled_engine

_KLINE_ENGINE: Engine | None = None
_KLINE_ENGINE_LOCK = threading.Lock()

KLINE_TABLES = {
    "sm_stock_kline",
    "sm_index_kline",
    "sm_concept_ths_kline",
    "sm_concept_east_kline",
}

MINUTE_KLINE_TABLES = {
    "sm_stock_minute",
    "sm_stock_minute_gm",
    "sm_stock_minute_gml",
    "sm_index_minute",
    "sm_concept_ths_minute",
    "sm_concept_east_minute",
}

_TABLE_REF_RE = re.compile(r"\b(?:FROM|JOIN)\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE)


def get_kline_engine() -> Engine:
    """Return the engine used for pure K-line reads."""
    global _KLINE_ENGINE
    if _KLINE_ENGINE is None:
        with _KLINE_ENGINE_LOCK:
            if _KLINE_ENGINE is None:
                pool = get_minute_mysql_pool_config()
                _KLINE_ENGINE = create_pooled_engine(
                    get_kline_mysql_url(),
                    pool_config=pool,
                )
    return _KLINE_ENGINE


def dispose_kline_engine() -> None:
    """Dispose the shared K-line engine if it has been initialized."""
    global _KLINE_ENGINE
    with _KLINE_ENGINE_LOCK:
        engine = _KLINE_ENGINE
        _KLINE_ENGINE = None
        if engine is not None:
            engine.dispose()


def referenced_tables(sql: str) -> set[str]:
    """Best-effort table extraction for simple SELECT routing."""
    return {m.group(1).lower() for m in _TABLE_REF_RE.finditer(sql or "")}


def should_use_kline_engine(sql: str) -> bool:
    """Route only pure K-line/minute-K-line SELECTs to the K-line database."""
    tables = referenced_tables(sql)
    market_tables = KLINE_TABLES | MINUTE_KLINE_TABLES
    return bool(tables) and tables.issubset(market_tables)
