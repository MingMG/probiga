# -*- coding: utf-8 -*-
"""Privileged portfolio migration and read-only runtime surface validation.

The watchlist table predates the current application and can contain legacy
columns that are not owned by this module.  Runtime validation therefore
freezes the complete surface used by the portfolio API without pretending
that those legacy extras belong to this migration.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text


PORTFOLIO_TABLE = "st_user_portfolio"
PORTFOLIO_TRANSACTION_TABLE = "st_portfolio_trans_log"
_TABLES = (PORTFOLIO_TABLE, PORTFOLIO_TRANSACTION_TABLE)


@dataclass(frozen=True)
class _RequiredColumn:
    data_types: frozenset[str]
    nullable: bool | None = None
    character_minimum: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    auto_increment: bool = False


_INTEGER_TYPES = frozenset({"tinyint", "smallint", "mediumint", "int", "bigint"})
_TEXT_TYPES = frozenset({"char", "varchar", "tinytext", "text", "mediumtext", "longtext"})
_DATETIME_TYPES = frozenset({"datetime", "timestamp"})


PORTFOLIO_REQUIRED_SURFACE: dict[str, dict[str, _RequiredColumn]] = {
    PORTFOLIO_TABLE: {
        "id": _RequiredColumn(_INTEGER_TYPES, False, auto_increment=True),
        "stock_code": _RequiredColumn(frozenset({"varchar"}), False, character_minimum=16),
        "short_name": _RequiredColumn(_TEXT_TYPES, character_minimum=1),
        "cost_price": _RequiredColumn(
            frozenset({"decimal"}),
            False,
            numeric_precision=12,
            numeric_scale=4,
        ),
        "shares": _RequiredColumn(_INTEGER_TYPES, False),
        "position_source": _RequiredColumn(
            frozenset({"varchar"}), character_minimum=16,
        ),
        "position_date": _RequiredColumn(frozenset({"date"}), True),
        "add_date": _RequiredColumn(frozenset({"date"})),
        "sort_order": _RequiredColumn(_INTEGER_TYPES),
        "notes": _RequiredColumn(_TEXT_TYPES, character_minimum=1),
        "is_holding": _RequiredColumn(_INTEGER_TYPES),
        "etl_sync_at": _RequiredColumn(_DATETIME_TYPES),
    },
    PORTFOLIO_TRANSACTION_TABLE: {
        "id": _RequiredColumn(_INTEGER_TYPES, False, auto_increment=True),
        "stock_code": _RequiredColumn(frozenset({"varchar"}), False, character_minimum=16),
        "trans_type": _RequiredColumn(frozenset({"varchar"}), False, character_minimum=8),
        "price": _RequiredColumn(
            frozenset({"decimal"}),
            False,
            numeric_precision=12,
            numeric_scale=4,
        ),
        "shares": _RequiredColumn(_INTEGER_TYPES, False),
        "source": _RequiredColumn(frozenset({"varchar"}), character_minimum=16),
        "trans_date": _RequiredColumn(frozenset({"date"}), False),
        "created_at": _RequiredColumn(_DATETIME_TYPES, False),
    },
}


_TRANSACTION_DDL = f"""
CREATE TABLE IF NOT EXISTS `{PORTFOLIO_TRANSACTION_TABLE}` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `stock_code` VARCHAR(16) NOT NULL,
  `trans_type` VARCHAR(8) NOT NULL,
  `price` DECIMAL(12,4) NOT NULL DEFAULT 0,
  `shares` INT NOT NULL DEFAULT 0,
  `source` VARCHAR(16) DEFAULT 'trade',
  `trans_date` DATE NOT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_code_date` (`stock_code`, `trans_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _rows(result) -> list[dict[str, Any]]:
    return [dict(row) for row in result.mappings().all()]


def _table_metadata(connection) -> dict[str, dict[str, Any]]:
    params = {f"table_{index}": table for index, table in enumerate(_TABLES)}
    placeholders = ", ".join(f":{key}" for key in params)
    rows = _rows(
        connection.execute(
            text(
                "SELECT TABLE_NAME AS table_name, ENGINE AS engine, "
                "TABLE_COLLATION AS table_collation "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() "
                f"AND TABLE_NAME IN ({placeholders})"
            ),
            params,
        )
    )
    return {str(row.get("table_name") or ""): row for row in rows}


def _column_metadata(connection) -> dict[str, dict[str, dict[str, Any]]]:
    required = {
        (table, column)
        for table, columns in PORTFOLIO_REQUIRED_SURFACE.items()
        for column in columns
    }
    table_params = {f"table_{index}": table for index, table in enumerate(_TABLES)}
    column_names = sorted({column for _, column in required})
    column_params = {
        f"column_{index}": column for index, column in enumerate(column_names)
    }
    table_placeholders = ", ".join(f":{key}" for key in table_params)
    column_placeholders = ", ".join(f":{key}" for key in column_params)
    rows = _rows(
        connection.execute(
            text(
                "SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, "
                "DATA_TYPE AS data_type, IS_NULLABLE AS is_nullable, "
                "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
                "NUMERIC_PRECISION AS numeric_precision, "
                "NUMERIC_SCALE AS numeric_scale, EXTRA AS extra, "
                "CHARACTER_SET_NAME AS character_set_name, "
                "COLLATION_NAME AS collation_name "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() "
                f"AND TABLE_NAME IN ({table_placeholders}) "
                f"AND COLUMN_NAME IN ({column_placeholders})"
            ),
            {**table_params, **column_params},
        )
    )
    by_table: dict[str, dict[str, dict[str, Any]]] = {table: {} for table in _TABLES}
    for row in rows:
        table = str(row.get("table_name") or "")
        column = str(row.get("column_name") or "")
        if (table, column) not in required or column in by_table.get(table, {}):
            raise RuntimeError("portfolio runtime column metadata is ambiguous")
        by_table[table][column] = row
    return by_table


def _index_metadata(connection) -> dict[str, list[dict[str, Any]]]:
    params = {f"table_{index}": table for index, table in enumerate(_TABLES)}
    placeholders = ", ".join(f":{key}" for key in params)
    rows = _rows(
        connection.execute(
            text(
                "SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, "
                "NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
                "COLUMN_NAME AS column_name, SUB_PART AS sub_part, "
                "INDEX_TYPE AS index_type FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA=DATABASE() "
                f"AND TABLE_NAME IN ({placeholders}) "
                "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
            ),
            params,
        )
    )
    by_table: dict[str, list[dict[str, Any]]] = {table: [] for table in _TABLES}
    for row in rows:
        table = str(row.get("table_name") or "")
        if table in by_table:
            by_table[table].append(row)
    return by_table


def _index_shapes(rows: list[dict[str, Any]]) -> set[tuple[bool, tuple[str, ...]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("index_name") or ""), []).append(row)
    shapes: set[tuple[bool, tuple[str, ...]]] = set()
    for parts in grouped.values():
        ordered = sorted(parts, key=lambda row: int(row.get("seq_in_index") or 0))
        if any(part.get("sub_part") is not None for part in ordered):
            continue
        if any(str(part.get("index_type") or "").upper() != "BTREE" for part in ordered):
            continue
        shapes.add(
            (
                all(int(part.get("non_unique") or 0) == 0 for part in ordered),
                tuple(str(part.get("column_name") or "") for part in ordered),
            )
        )
    return shapes


def _validate_on_connection(connection) -> None:
    tables = _table_metadata(connection)
    if set(tables) != set(_TABLES):
        raise RuntimeError(
            "portfolio runtime tables are not prepared: "
            f"missing={sorted(set(_TABLES) - set(tables))}"
        )
    for table, row in tables.items():
        if str(row.get("engine") or "").lower() != "innodb":
            raise RuntimeError(f"portfolio runtime storage drift: {table}.engine")
        if str(row.get("table_collation") or "") != "utf8mb4_unicode_ci":
            raise RuntimeError(f"portfolio runtime storage drift: {table}.collation")

    actual = _column_metadata(connection)
    for table, expected_columns in PORTFOLIO_REQUIRED_SURFACE.items():
        missing = sorted(set(expected_columns) - set(actual[table]))
        if missing:
            raise RuntimeError(
                f"portfolio runtime columns are not prepared: {table} missing={missing}"
            )
        for column, expected in expected_columns.items():
            row = actual[table][column]
            data_type = str(row.get("data_type") or "").lower()
            if data_type not in expected.data_types:
                raise RuntimeError(f"portfolio runtime type drift: {table}.{column}")
            nullable = str(row.get("is_nullable") or "").upper() == "YES"
            if expected.nullable is not None and nullable != expected.nullable:
                raise RuntimeError(f"portfolio runtime nullability drift: {table}.{column}")
            if expected.character_minimum is not None:
                capacity = row.get("character_maximum_length")
                if capacity is not None and int(capacity) < expected.character_minimum:
                    raise RuntimeError(f"portfolio runtime capacity drift: {table}.{column}")
                if (
                    str(row.get("character_set_name") or "") != "utf8mb4"
                    or str(row.get("collation_name") or "") != "utf8mb4_unicode_ci"
                ):
                    raise RuntimeError(f"portfolio runtime collation drift: {table}.{column}")
            if (
                expected.numeric_precision is not None
                and int(row.get("numeric_precision") or 0) != expected.numeric_precision
            ):
                raise RuntimeError(f"portfolio runtime precision drift: {table}.{column}")
            if (
                expected.numeric_scale is not None
                and int(row.get("numeric_scale") or 0) != expected.numeric_scale
            ):
                raise RuntimeError(f"portfolio runtime scale drift: {table}.{column}")
            auto_increment = "auto_increment" in str(row.get("extra") or "").lower()
            if expected.auto_increment != auto_increment:
                raise RuntimeError(f"portfolio runtime identity drift: {table}.{column}")

    indexes = _index_metadata(connection)
    required_indexes = {
        PORTFOLIO_TABLE: {(True, ("id",)), (True, ("stock_code",))},
        PORTFOLIO_TRANSACTION_TABLE: {
            (True, ("id",)),
            (False, ("stock_code", "trans_date")),
        },
    }
    for table, required in required_indexes.items():
        missing = required - _index_shapes(indexes[table])
        if missing:
            raise RuntimeError(f"portfolio runtime index drift: {table} missing={sorted(missing)}")


def validate_portfolio_runtime_schema(engine, *, connection=None) -> None:
    """Validate the portfolio API surface with information-schema reads only."""

    if connection is not None:
        _validate_on_connection(connection)
        return
    with engine.connect() as bound_connection:
        _validate_on_connection(bound_connection)


def _single_column(connection, table: str, column: str) -> dict[str, Any] | None:
    rows = _rows(
        connection.execute(
            text(
                "SELECT DATA_TYPE AS data_type, IS_NULLABLE AS is_nullable, "
                "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
                "NUMERIC_PRECISION AS numeric_precision, NUMERIC_SCALE AS numeric_scale "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
                "AND COLUMN_NAME=:column_name"
            ),
            {"table_name": table, "column_name": column},
        )
    )
    if len(rows) > 1:
        raise RuntimeError(f"portfolio migration metadata is ambiguous: {table}.{column}")
    return rows[0] if rows else None


def _normalize_column(
    connection,
    *,
    table: str,
    column: str,
    definition: str,
    after: str,
    is_prepared,
) -> None:
    row = _single_column(connection, table, column)
    if row is None:
        connection.execute(
            text(f"ALTER TABLE `{table}` ADD COLUMN `{column}` {definition} AFTER `{after}`")
        )
        return
    if not is_prepared(row):
        connection.execute(text(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {definition}"))


def privileged_migrate_portfolio_schema(engine) -> None:
    """Run portfolio DDL only from the fenced release migration phase."""

    with engine.begin() as connection:
        base_table = _table_metadata(connection).get(PORTFOLIO_TABLE)
        if base_table is None:
            raise RuntimeError("st_user_portfolio must exist before portfolio migration")
        base_columns = _column_metadata(connection)[PORTFOLIO_TABLE]
        legacy_required = {
            "id",
            "stock_code",
            "short_name",
            "cost_price",
            "shares",
            "add_date",
            "sort_order",
            "notes",
            "is_holding",
            "etl_sync_at",
        }
        missing = sorted(legacy_required - set(base_columns))
        if missing:
            raise RuntimeError(f"st_user_portfolio legacy surface is incomplete: {missing}")

        connection.execute(text(_TRANSACTION_DDL))
        _normalize_column(
            connection,
            table=PORTFOLIO_TRANSACTION_TABLE,
            column="source",
            definition="VARCHAR(16) DEFAULT 'trade' COMMENT '来源：trade/position_add'",
            after="shares",
            is_prepared=lambda row: (
                str(row.get("data_type") or "").lower() == "varchar"
                and int(row.get("character_maximum_length") or 0) >= 16
            ),
        )
        _normalize_column(
            connection,
            table=PORTFOLIO_TABLE,
            column="position_source",
            definition="VARCHAR(16) DEFAULT 'manual' COMMENT '持仓来源：manual/today_buy'",
            after="shares",
            is_prepared=lambda row: (
                str(row.get("data_type") or "").lower() == "varchar"
                and int(row.get("character_maximum_length") or 0) >= 16
            ),
        )
        _normalize_column(
            connection,
            table=PORTFOLIO_TABLE,
            column="position_date",
            definition="DATE DEFAULT NULL COMMENT '持仓来源日期'",
            after="position_source",
            is_prepared=lambda row: (
                str(row.get("data_type") or "").lower() == "date"
                and str(row.get("is_nullable") or "").upper() == "YES"
            ),
        )
        _normalize_column(
            connection,
            table=PORTFOLIO_TABLE,
            column="cost_price",
            definition="DECIMAL(12,4) NOT NULL DEFAULT 0 COMMENT '成本价'",
            after="short_name",
            is_prepared=lambda row: (
                str(row.get("data_type") or "").lower() == "decimal"
                and int(row.get("numeric_precision") or 0) == 12
                and int(row.get("numeric_scale") or 0) == 4
                and str(row.get("is_nullable") or "").upper() == "NO"
            ),
        )
        for table in _TABLES:
            connection.execute(
                text(
                    f"ALTER TABLE `{table}` ENGINE=InnoDB, "
                    "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            )
        validate_portfolio_runtime_schema(engine, connection=connection)


def ensure_portfolio_runtime_schema(engine) -> None:
    """Compatibility boundary: API callers validate and never mutate schema."""

    validate_portfolio_runtime_schema(engine)


__all__ = [
    "PORTFOLIO_REQUIRED_SURFACE",
    "PORTFOLIO_TABLE",
    "PORTFOLIO_TRANSACTION_TABLE",
    "ensure_portfolio_runtime_schema",
    "privileged_migrate_portfolio_schema",
    "validate_portfolio_runtime_schema",
]
