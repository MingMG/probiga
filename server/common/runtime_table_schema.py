# -*- coding: utf-8 -*-
"""Read-only MySQL runtime schema contracts and privileged storage migration."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text


MYSQL_RUNTIME_ENGINE = "InnoDB"
MYSQL_RUNTIME_CHARSET = "utf8mb4"
MYSQL_RUNTIME_COLLATION = "utf8mb4_unicode_ci"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_STRING_TYPES = frozenset({"char", "varchar", "tinytext", "text", "mediumtext", "longtext"})


@dataclass(frozen=True)
class RuntimeColumn:
    data_type: str
    nullable: bool
    character_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    datetime_precision: int | None = None
    auto_increment: bool = False
    unsigned: bool = False


@dataclass(frozen=True)
class RuntimeIndex:
    columns: tuple[str, ...]
    unique: bool = False
    index_type: str = "BTREE"


@dataclass(frozen=True)
class RuntimeTable:
    columns: Mapping[str, RuntimeColumn]
    indexes: tuple[RuntimeIndex, ...]
    engine: str = MYSQL_RUNTIME_ENGINE
    collation: str = MYSQL_RUNTIME_COLLATION


def _validated_contracts(
    contracts: Mapping[str, RuntimeTable],
) -> dict[str, RuntimeTable]:
    result = dict(contracts)
    if not result:
        raise ValueError("runtime schema contracts cannot be empty")
    for table, contract in result.items():
        if not _IDENTIFIER.fullmatch(str(table)):
            raise ValueError(f"unsafe runtime table identifier: {table}")
        if not contract.columns:
            raise ValueError(f"runtime table has no columns: {table}")
        for column in contract.columns:
            if not _IDENTIFIER.fullmatch(str(column)):
                raise ValueError(f"unsafe runtime column identifier: {table}.{column}")
        for index in contract.indexes:
            if not index.columns or not set(index.columns).issubset(contract.columns):
                raise ValueError(f"invalid runtime index contract: {table}.{index.columns}")
    return result


def _table_params(contracts: Mapping[str, RuntimeTable]) -> tuple[str, dict[str, str]]:
    tables = tuple(contracts)
    return (
        ", ".join(f":table_{index}" for index in range(len(tables))),
        {f"table_{index}": table for index, table in enumerate(tables)},
    )


def _rows(connection, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(text(sql), params).mappings().all()
    ]


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _validate_runtime_tables_on_connection(
    connection,
    contracts: Mapping[str, RuntimeTable],
    *,
    context: str,
) -> None:
    contracts = _validated_contracts(contracts)
    placeholders, params = _table_params(contracts)
    table_rows = _rows(
        connection,
        "SELECT table_name AS table_name, engine AS engine, "
        "table_collation AS table_collation FROM information_schema.tables "
        "WHERE table_schema=DATABASE() AND table_name IN "
        f"({placeholders})",
        params,
    )
    by_table = {str(row.get("table_name") or ""): row for row in table_rows}
    if len(by_table) != len(table_rows) or set(by_table) != set(contracts):
        missing = sorted(set(contracts) - set(by_table))
        unexpected = sorted(set(by_table) - set(contracts))
        raise RuntimeError(
            f"{context} runtime schema is not prepared: "
            f"missing_tables={missing} unexpected_tables={unexpected}"
        )
    for table, contract in contracts.items():
        row = by_table[table]
        if (
            str(row.get("engine") or "").lower() != contract.engine.lower()
            or str(row.get("table_collation") or "") != contract.collation
        ):
            raise RuntimeError(
                f"{context} runtime table storage drift: {table} "
                f"engine={row.get('engine')} collation={row.get('table_collation')}"
            )

    column_rows = _rows(
        connection,
        "SELECT table_name AS table_name, column_name AS column_name, "
        "data_type AS data_type, column_type AS column_type, "
        "is_nullable AS is_nullable, "
        "character_maximum_length AS character_maximum_length, "
        "numeric_precision AS numeric_precision, numeric_scale AS numeric_scale, "
        "datetime_precision AS datetime_precision, extra AS extra, "
        "character_set_name AS character_set_name, collation_name AS collation_name "
        "FROM information_schema.columns WHERE table_schema=DATABASE() "
        f"AND table_name IN ({placeholders}) ORDER BY table_name, ordinal_position",
        params,
    )
    columns_by_table: dict[str, dict[str, dict[str, Any]]] = {
        table: {} for table in contracts
    }
    for row in column_rows:
        table = str(row.get("table_name") or "")
        column = str(row.get("column_name") or "")
        if table in columns_by_table:
            if column in columns_by_table[table]:
                raise RuntimeError(
                    f"{context} runtime schema duplicate column: {table}.{column}"
                )
            columns_by_table[table][column] = row
    for table, contract in contracts.items():
        actual_columns = set(columns_by_table[table])
        expected_columns = set(contract.columns)
        if actual_columns != expected_columns:
            raise RuntimeError(
                f"{context} runtime column set drift: {table} "
                f"missing={sorted(expected_columns - actual_columns)} "
                f"unexpected={sorted(actual_columns - expected_columns)}"
            )
        for column, expected in contract.columns.items():
            row = columns_by_table[table][column]
            data_type = str(row.get("data_type") or "").lower()
            actual = RuntimeColumn(
                data_type=data_type,
                nullable=str(row.get("is_nullable") or "").upper() == "YES",
                character_length=(
                    _optional_int(row.get("character_maximum_length"))
                    if expected.character_length is not None else None
                ),
                numeric_precision=(
                    _optional_int(row.get("numeric_precision"))
                    if expected.numeric_precision is not None else None
                ),
                numeric_scale=(
                    _optional_int(row.get("numeric_scale"))
                    if expected.numeric_scale is not None else None
                ),
                datetime_precision=(
                    _optional_int(row.get("datetime_precision"))
                    if expected.datetime_precision is not None else None
                ),
                auto_increment=(
                    "auto_increment" in str(row.get("extra") or "").lower()
                ),
                unsigned="unsigned" in str(row.get("column_type") or "").lower(),
            )
            if actual != expected:
                raise RuntimeError(
                    f"{context} runtime column type drift: {table}.{column} "
                    f"expected={expected} actual={actual}"
                )
            if data_type in _STRING_TYPES and (
                str(row.get("character_set_name") or "") != MYSQL_RUNTIME_CHARSET
                or str(row.get("collation_name") or "") != contract.collation
            ):
                raise RuntimeError(
                    f"{context} runtime column collation drift: {table}.{column}"
                )

    index_rows = _rows(
        connection,
        "SELECT table_name AS table_name, index_name AS index_name, "
        "non_unique AS non_unique, seq_in_index AS seq_in_index, "
        "column_name AS column_name, sub_part AS sub_part, "
        "index_type AS index_type FROM information_schema.statistics "
        "WHERE table_schema=DATABASE() AND table_name IN "
        f"({placeholders}) ORDER BY table_name, index_name, seq_in_index",
        params,
    )
    parts: dict[str, dict[str, list[tuple[int, str, int | None]]]] = {
        table: {} for table in contracts
    }
    unique: dict[str, dict[str, bool]] = {table: {} for table in contracts}
    index_types: dict[str, dict[str, str]] = {table: {} for table in contracts}
    for row in index_rows:
        table = str(row.get("table_name") or "")
        if table not in parts:
            continue
        name = str(row.get("index_name") or "")
        parts[table].setdefault(name, []).append((
            int(row.get("seq_in_index") or 0),
            str(row.get("column_name") or ""),
            _optional_int(row.get("sub_part")),
        ))
        unique[table][name] = int(row.get("non_unique") or 0) == 0
        index_types[table][name] = str(row.get("index_type") or "").upper()
    for table, contract in contracts.items():
        actual_indexes = {
            RuntimeIndex(
                columns=tuple(column for _, column, _ in sorted(index_parts)),
                unique=bool(unique[table].get(name)),
                index_type=index_types[table].get(name, ""),
            )
            for name, index_parts in parts[table].items()
            if all(sub_part is None for _, _, sub_part in index_parts)
        }
        missing = [index for index in contract.indexes if index not in actual_indexes]
        if missing:
            raise RuntimeError(
                f"{context} runtime index drift: {table} missing={missing}"
            )


def validate_runtime_tables(
    engine,
    contracts: Mapping[str, RuntimeTable],
    *,
    context: str,
    connection=None,
) -> None:
    """Validate a frozen table contract using SELECT statements only."""

    if connection is not None:
        _validate_runtime_tables_on_connection(
            connection, contracts, context=context,
        )
        return
    with engine.connect() as bound_connection:
        _validate_runtime_tables_on_connection(
            bound_connection, contracts, context=context,
        )


def privileged_normalize_mysql_storage(
    connection,
    contracts: Mapping[str, RuntimeTable],
) -> None:
    """Normalize engine/collation only inside an explicit release migration."""

    contracts = _validated_contracts(contracts)
    placeholders, params = _table_params(contracts)
    rows = _rows(
        connection,
        "SELECT table_name AS table_name, engine AS engine, "
        "table_collation AS table_collation FROM information_schema.tables "
        "WHERE table_schema=DATABASE() AND table_name IN "
        f"({placeholders})",
        params,
    )
    by_table = {str(row.get("table_name") or ""): row for row in rows}
    missing = sorted(set(contracts) - set(by_table))
    if missing:
        raise RuntimeError(f"privileged migration did not create tables: {missing}")
    for table, contract in contracts.items():
        row = by_table[table]
        if (
            str(row.get("engine") or "").lower() == contract.engine.lower()
            and str(row.get("table_collation") or "") == contract.collation
        ):
            continue
        connection.execute(text(
            f"ALTER TABLE `{table}` ENGINE={contract.engine}, "
            f"CONVERT TO CHARACTER SET {MYSQL_RUNTIME_CHARSET} "
            f"COLLATE {contract.collation}"
        ))


__all__ = [
    "MYSQL_RUNTIME_CHARSET",
    "MYSQL_RUNTIME_COLLATION",
    "MYSQL_RUNTIME_ENGINE",
    "RuntimeColumn",
    "RuntimeIndex",
    "RuntimeTable",
    "privileged_normalize_mysql_storage",
    "validate_runtime_tables",
]
