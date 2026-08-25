# -*- coding: utf-8 -*-
"""Read-only contracts for pre-existing shared business tables.

Some early collectors referenced local SQL files that are no longer part of
the release artifact and attempted to replay those files on every invocation.
Modern runtime code must instead fail closed when the privileged deployment has
not prepared its storage.  These helpers intentionally validate only the
surface a caller actually uses; old shared tables may contain additional valid
columns and indexes.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from sqlalchemy import inspect, text


_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


def _names(values: Iterable[str], *, kind: str) -> tuple[str, ...]:
    result = tuple(sorted({str(value).strip() for value in values}))
    if not result:
        raise ValueError(f"{kind} contract cannot be empty")
    for value in result:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"unsafe {kind} identifier: {value}")
    return result


def validate_required_table_surface(
    engine,
    tables: Iterable[str],
    *,
    context: str,
    required_columns: Mapping[str, Iterable[str]] | None = None,
) -> dict[str, Any]:
    """Validate required tables/columns using metadata reads only."""

    expected_tables = _names(tables, kind="table")
    column_contract = {
        str(table): _names(columns, kind="column")
        for table, columns in (required_columns or {}).items()
    }
    unknown = sorted(set(column_contract) - set(expected_tables))
    if unknown:
        raise ValueError(f"column contract references unknown tables: {unknown}")

    dialect = str(getattr(getattr(engine, "dialect", None), "name", ""))
    if dialect == "mysql":
        binds = {f"table_{index}": name for index, name in enumerate(expected_tables)}
        placeholders = ", ".join(f":table_{index}" for index in range(len(expected_tables)))
        with engine.connect() as connection:
            table_rows = connection.execute(
                text(
                    "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
                    f"({placeholders})"
                ),
                binds,
            ).fetchall()
            actual_tables = {str(row[0]): str(row[1] or "") for row in table_rows}
            missing = sorted(set(expected_tables) - set(actual_tables))
            if missing:
                raise RuntimeError(
                    f"{context} runtime schema is not prepared: missing_tables={missing}"
                )
            non_transactional = sorted(
                table for table, storage in actual_tables.items()
                if storage.upper() != "INNODB"
            )
            if non_transactional:
                raise RuntimeError(
                    f"{context} requires InnoDB tables: {non_transactional}"
                )
            if column_contract:
                column_rows = connection.execute(
                    text(
                        "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
                        f"({placeholders})"
                    ),
                    binds,
                ).fetchall()
                actual_columns: dict[str, set[str]] = {
                    table: set() for table in column_contract
                }
                for table, column in column_rows:
                    if str(table) in actual_columns:
                        actual_columns[str(table)].add(str(column))
    else:
        # SQLite support keeps unit tests and local dry-runs read-only too.
        inspector = inspect(engine)
        actual_table_names = set(inspector.get_table_names())
        missing = sorted(set(expected_tables) - actual_table_names)
        if missing:
            raise RuntimeError(
                f"{context} runtime schema is not prepared: missing_tables={missing}"
            )
        actual_columns = {
            table: {str(item["name"]) for item in inspector.get_columns(table)}
            for table in column_contract
        }

    missing_columns = {
        table: sorted(set(columns) - actual_columns.get(table, set()))
        for table, columns in column_contract.items()
        if set(columns) - actual_columns.get(table, set())
    }
    if missing_columns:
        raise RuntimeError(
            f"{context} runtime schema is not prepared: "
            f"missing_columns={missing_columns}"
        )
    return {
        "context": context,
        "tables": expected_tables,
        "required_columns": {
            table: tuple(columns) for table, columns in sorted(column_contract.items())
        },
        "required_surface_verified": True,
        "read_only": True,
    }


__all__ = ["validate_required_table_surface"]
