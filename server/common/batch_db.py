# -*- coding: utf-8 -*-
"""Shared database helpers for batch jobs and report generators."""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Iterator

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.engine import Connection, Engine

from server.common.config import get_mysql_url
from server.common.engine_factory import create_pooled_engine
from server.common.kline_data import get_kline_engine, should_use_kline_engine

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
logger = logging.getLogger(__name__)
_TRANSIENT_DB_ERRNOS = {1205, 1213, 2003, 2013}


def create_batch_engine(url: str | None = None, **kwargs: Any) -> Engine:
    """Create a MySQL engine with production-safe defaults for batch jobs."""
    return create_pooled_engine(url or get_mysql_url(required=True), **kwargs)


def routed_read_engine(sql: object, engine: Engine) -> Engine:
    """Route pure market-history reads to their configured databases."""
    sql_text = str(sql)
    if should_use_kline_engine(sql_text):
        return get_kline_engine()

    # Import lazily because minute_data uses quote_identifier from this module.
    from server.common.minute_data import (  # pylint: disable=import-outside-toplevel
        get_minute_engine,
        should_use_capital_flow_engine,
    )

    if should_use_capital_flow_engine(sql_text):
        return get_minute_engine()
    return engine


def _transient_db_errno(exc: BaseException) -> int | None:
    orig = getattr(exc, "orig", None)
    args = getattr(orig, "args", ()) or getattr(exc, "args", ())
    if not args:
        return None
    try:
        return int(args[0])
    except (TypeError, ValueError):
        return None


def _read_retry_attempts() -> int:
    raw = os.environ.get("PROBIGA_BATCH_DB_READ_RETRIES", "3")
    try:
        return max(1, int(float(raw)))
    except (TypeError, ValueError):
        return 3


def quote_identifier(value: str) -> str:
    """Quote a simple SQL identifier after validating it."""
    name = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return f"`{name}`"


def qualified_table_name(*parts: str) -> str:
    """Return a safely quoted dotted table name such as `db`.`table`."""
    if not parts:
        raise ValueError("qualified table name requires at least one part")
    return ".".join(quote_identifier(part) for part in parts)


def read_frame(sql: object, engine: Engine, params: dict | None = None) -> pd.DataFrame:
    """Read a SQL query into a DataFrame, using K-line routing when possible."""
    read_engine = routed_read_engine(sql, engine)
    return read_frame_direct(sql, read_engine, params=params)


def read_frame_direct(sql: object, engine: Engine, params: dict | None = None) -> pd.DataFrame:
    """Read through the explicitly supplied engine without applying table routing."""
    attempts = _read_retry_attempts()
    for attempt in range(1, attempts + 1):
        try:
            return pd.read_sql(sql, engine, params=params)
        except DBAPIError as exc:
            errno = _transient_db_errno(exc)
            if errno not in _TRANSIENT_DB_ERRNOS or attempt >= attempts:
                raise
            delay = min(5.0, 0.5 * (2 ** (attempt - 1)))
            logger.warning(
                "Transient batch SQL read failed errno=%s attempt=%s/%s; retrying in %.1fs",
                errno,
                attempt,
                attempts,
                delay,
            )
            try:
                engine.dispose()
            except Exception:
                logger.debug("Failed to dispose engine after transient read error", exc_info=True)
            time.sleep(delay)
    raise RuntimeError("unreachable batch SQL retry state")


def read_frame_chunks(
    sql: object,
    engine: Engine | Connection,
    *,
    params: dict | None = None,
    chunksize: int = 100_000,
    stream_results: bool = True,
) -> Iterator[pd.DataFrame]:
    """Stream large query results through the centralized pandas boundary."""

    if chunksize < 1:
        raise ValueError("chunksize must be positive")
    owns_connection = isinstance(engine, Engine)
    connection = engine.connect() if owns_connection else engine
    if stream_results:
        connection = connection.execution_options(stream_results=True)
    try:
        yield from pd.read_sql_query(
            sql,
            connection,
            params=params,
            chunksize=chunksize,
        )
    finally:
        if owns_connection:
            connection.close()


def write_frame(
    frame: pd.DataFrame,
    table_name: str,
    engine: Engine,
    *,
    if_exists: str = "append",
    index: bool = False,
    chunksize: int | None = None,
    method: str | None = None,
    schema: str | None = None,
    **kwargs: Any,
) -> int:
    """Write a DataFrame to a validated table name and return rows attempted."""
    if frame is None or frame.empty:
        return 0
    quote_identifier(table_name)
    if schema:
        quote_identifier(schema)
    frame.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=index,
        chunksize=chunksize,
        method=method,
        schema=schema,
        **kwargs,
    )
    return int(len(frame))


def replace_table_rows(
    frame: pd.DataFrame,
    table_name: str,
    engine: Engine,
    *,
    where_sql: str = "",
    params: dict[str, Any] | None = None,
    chunksize: int | None = 1000,
    method: str | None = "multi",
) -> int:
    """Replace a validated slice in one transaction without destructive TRUNCATE.

    The caller must provide a complete, already validated replacement frame.
    DELETE and INSERT share one transaction, so a failed write rolls back to the
    previous good rows instead of leaving a partially refreshed table.
    """
    if frame is None or frame.empty:
        raise ValueError(f"replacement frame for {table_name!r} must not be empty")
    quote_identifier(table_name)
    predicate = f" WHERE {where_sql.strip()}" if where_sql.strip() else ""
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {quote_identifier(table_name)}{predicate}"),
            params or {},
        )
        return write_frame(
            frame,
            table_name,
            conn,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            method=method,
        )


def records_from_frame(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame into JSON-friendly records."""
    if df is None or df.empty:
        return []
    safe = df.astype(object).where(pd.notna(df), None)
    return safe.to_dict(orient="records")


def read_records(
    sql: object,
    engine: Engine,
    params: dict | None = None,
    *,
    ignore_errors: bool = False,
) -> list[dict]:
    """Read a SQL query into a list of dictionaries."""
    try:
        return records_from_frame(read_frame(sql, engine, params=params))
    except Exception as exc:
        if ignore_errors:
            logger.debug("Ignoring SQL read error: %s", exc)
            return []
        raise
