# -*- coding: utf-8 -*-
"""Repair helpers for canonical daily business keys.

These operations deliberately keep the newest row for a duplicate business
key.  The repair is only a migration step; ongoing writers are protected by
the unique indexes declared in ``server.db.migrations``.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.batch_db import quote_identifier


def _duplicate_row_count(
    conn: Any,
    table: str,
    key_columns: tuple[str, ...],
    where_sql: str,
    params: dict[str, Any],
) -> int:
    keys = ", ".join(quote_identifier(column) for column in key_columns)
    row = conn.execute(
        text(
            f"SELECT COALESCE(SUM(c - 1), 0) FROM ("
            f"SELECT {keys}, COUNT(*) AS c FROM {quote_identifier(table)} "
            f"WHERE {where_sql} GROUP BY {keys} HAVING c > 1"
            f") duplicates"
        ),
        params,
    ).scalar()
    return int(row or 0)


def deduplicate_business_rows(
    engine: Engine,
    *,
    table: str,
    key_columns: tuple[str, ...],
    where_sql: str = "1=1",
    delete_where_sql: str | None = None,
    dry_run: bool = False,
    params: dict[str, Any] | None = None,
) -> dict[str, int | str]:
    """Delete older duplicate rows and return before/after counts."""
    with engine.connect() as conn:
        columns = {
            str(row[0])
            for row in conn.execute(
                text(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
                ),
                {"table_name": table},
            ).fetchall()
        }
    if "id" not in columns:
        raise ValueError(f"{table} must have an id column for safe de-duplication")

    safe_params = dict(params or {})
    with engine.begin() as conn:
        before = _duplicate_row_count(conn, table, key_columns, where_sql, safe_params)
        if before and not dry_run:
            key_match = " AND ".join(
                f"older.{quote_identifier(column)} = newer.{quote_identifier(column)}"
                for column in key_columns
            )
            conn.execute(
                text(
                    f"DELETE older FROM {quote_identifier(table)} older "
                    f"JOIN {quote_identifier(table)} newer "
                    f"ON {key_match} AND newer.`id` > older.`id` "
                    f"WHERE {delete_where_sql or where_sql}"
                ),
                safe_params,
            )
        after = _duplicate_row_count(conn, table, key_columns, where_sql, safe_params)
    return {"table": table, "duplicate_rows_before": before, "duplicate_rows_after": after}


def deduplicate_daily_kline(
    engine: Engine,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool = False,
) -> dict[str, int | str]:
    clauses = ["k_type = 1"]
    delete_clauses = ["older.`k_type` = 1"]
    params: dict[str, Any] = {}
    if start_date:
        clauses.append("trade_date >= :start_date")
        delete_clauses.append("older.`trade_date` >= :start_date")
        params["start_date"] = start_date
    if end_date:
        clauses.append("trade_date <= :end_date")
        delete_clauses.append("older.`trade_date` <= :end_date")
        params["end_date"] = end_date
    return deduplicate_business_rows(
        engine,
        table="sm_stock_kline",
        key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
        where_sql=" AND ".join(clauses),
        delete_where_sql=" AND ".join(delete_clauses),
        dry_run=dry_run,
        params=params,
    )


def deduplicate_daily_flow(engine: Engine, *, dry_run: bool = False) -> dict[str, int | str]:
    return deduplicate_business_rows(
        engine,
        table="sm_stock_capital_flow_daily",
        key_columns=("stock_code", "trade_date"),
        dry_run=dry_run,
    )
