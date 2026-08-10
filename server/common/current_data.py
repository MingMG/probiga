"""Dedicated database routing for persisted current-market quotes."""
from __future__ import annotations

import re
import threading

from sqlalchemy.engine import Engine

from server.common.config import get_current_mysql_url, get_minute_mysql_pool_config
from server.common.engine_factory import create_pooled_engine

_CURRENT_ENGINE: Engine | None = None
_CURRENT_ENGINE_LOCK = threading.Lock()

CURRENT_TABLES = {"sm_stock_current", "sm_rt_quote_snapshot"}
_TABLE_REF_RE = re.compile(r"\b(?:FROM|JOIN)\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE)


def get_current_engine() -> Engine:
    """Return the engine used to read/write current quote snapshots."""
    global _CURRENT_ENGINE
    if _CURRENT_ENGINE is None:
        with _CURRENT_ENGINE_LOCK:
            if _CURRENT_ENGINE is None:
                _CURRENT_ENGINE = create_pooled_engine(
                    get_current_mysql_url(),
                    pool_config=get_minute_mysql_pool_config(),
                )
    return _CURRENT_ENGINE


def dispose_current_engine() -> None:
    global _CURRENT_ENGINE
    with _CURRENT_ENGINE_LOCK:
        engine = _CURRENT_ENGINE
        _CURRENT_ENGINE = None
        if engine is not None:
            engine.dispose()


def referenced_tables(sql: str) -> set[str]:
    return {match.group(1).lower() for match in _TABLE_REF_RE.finditer(sql or "")}


def should_use_current_engine(sql: str) -> bool:
    """Route pure current-quote reads to the QMT collector database."""
    tables = referenced_tables(sql)
    return bool(tables) and tables.issubset(CURRENT_TABLES)
