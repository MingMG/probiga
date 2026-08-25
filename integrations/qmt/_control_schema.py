"""Strict MySQL contracts for QMT control tables.

This module deliberately separates two authorities:

* release/setup code may call :func:`privileged_migrate_frozen_tables`;
* scheduled/runtime code may only call :func:`validate_frozen_tables`.

The validator uses ``SELECT`` against ``information_schema`` only.  It checks
the complete ordered column contract, defaults, charset/collation, engine and
the exact named index inventory so a partially prepared control schema cannot
be mistaken for a healthy runtime.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Mapping

from sqlalchemy import text


EXPECTED_ENGINE = "InnoDB"
EXPECTED_CHARSET = "utf8mb4"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class FrozenColumn:
    column_type: str
    nullable: bool
    default: str | None = None
    character: bool = False
    extra: str = ""


@dataclass(frozen=True)
class FrozenIndex:
    columns: tuple[str, ...]
    unique: bool
    index_type: str = "BTREE"


@dataclass(frozen=True)
class FrozenTable:
    ddl: str
    columns: tuple[tuple[str, FrozenColumn], ...]
    indexes: Mapping[str, FrozenIndex]
    engine: str = EXPECTED_ENGINE
    collation: str = EXPECTED_COLLATION


def character_column(
    column_type: str,
    *,
    nullable: bool,
    default: str | None = None,
) -> FrozenColumn:
    return FrozenColumn(
        column_type=column_type,
        nullable=nullable,
        default=default,
        character=True,
    )


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] == "'":
        normalized = normalized[1:-1].replace("''", "'")
    if normalized.casefold() in {"current_timestamp", "current_timestamp()"}:
        return "current_timestamp"
    try:
        number = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return normalized
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    return canonical or "0"


def _normalize_extra(value: Any) -> str:
    # MySQL versions disagree on whether DEFAULT_GENERATED is reported for a
    # literal/current-timestamp default.  It does not change storage semantics.
    tokens = str(value or "").casefold().replace("default_generated", " ")
    return " ".join(tokens.split())


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    for candidate in (name, name.lower(), name.upper()):
        try:
            if candidate in row:
                return row[candidate]
        except TypeError:
            break
    return None


def _validate_contracts(contracts: Mapping[str, FrozenTable]) -> dict[str, FrozenTable]:
    result = dict(contracts)
    if not result:
        raise ValueError("QMT control table contracts cannot be empty")
    for table_name, contract in result.items():
        if not _IDENTIFIER.fullmatch(table_name):
            raise ValueError(f"unsafe QMT control table identifier: {table_name}")
        column_names = tuple(name for name, _ in contract.columns)
        if not column_names or len(set(column_names)) != len(column_names):
            raise ValueError(f"invalid QMT control column contract: {table_name}")
        if any(not _IDENTIFIER.fullmatch(name) for name in column_names):
            raise ValueError(f"unsafe QMT control column identifier: {table_name}")
        for index_name, index in contract.indexes.items():
            if not _IDENTIFIER.fullmatch(index_name):
                raise ValueError(f"unsafe QMT control index identifier: {index_name}")
            if not index.columns or not set(index.columns).issubset(column_names):
                raise ValueError(f"invalid QMT control index: {table_name}.{index_name}")
    return result


def _table_params(contracts: Mapping[str, FrozenTable]) -> tuple[str, dict[str, str]]:
    names = tuple(contracts)
    return (
        ", ".join(f":table_{index}" for index in range(len(names))),
        {f"table_{index}": name for index, name in enumerate(names)},
    )


def _mapping_rows(result) -> list[dict[str, Any]]:
    return [dict(row) for row in result.mappings().all()]


def _contract_payload(contracts: Mapping[str, FrozenTable]) -> dict[str, Any]:
    return {
        table_name: {
            "engine": contract.engine,
            "charset": EXPECTED_CHARSET,
            "collation": contract.collation,
            "columns": [
                {
                    "name": name,
                    "column_type": column.column_type,
                    "nullable": column.nullable,
                    "default": column.default,
                    "character": column.character,
                    "extra": column.extra,
                }
                for name, column in contract.columns
            ],
            "indexes": {
                name: {
                    "columns": list(index.columns),
                    "unique": index.unique,
                    "index_type": index.index_type,
                }
                for name, index in sorted(contract.indexes.items())
            },
        }
        for table_name, contract in sorted(contracts.items())
    }


def frozen_contract_hash(contracts: Mapping[str, FrozenTable]) -> str:
    payload = json.dumps(
        _contract_payload(_validate_contracts(contracts)),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_on_connection(
    connection,
    contracts: Mapping[str, FrozenTable],
    *,
    context: str,
) -> dict[str, Any]:
    contracts = _validate_contracts(contracts)
    placeholders, params = _table_params(contracts)
    table_rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION "
        "FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
        f"({placeholders})"
    ), params))
    actual_tables = {
        str(_row_value(row, "TABLE_NAME") or ""): row for row in table_rows
    }
    if len(actual_tables) != len(table_rows) or set(actual_tables) != set(contracts):
        raise RuntimeError(
            f"{context} physical table inventory differs: "
            f"missing={sorted(set(contracts) - set(actual_tables))} "
            f"unexpected={sorted(set(actual_tables) - set(contracts))}"
        )
    for table_name, contract in contracts.items():
        row = actual_tables[table_name]
        engine = str(_row_value(row, "ENGINE") or "")
        collation = str(_row_value(row, "TABLE_COLLATION") or "")
        if engine.casefold() != contract.engine.casefold() or collation != contract.collation:
            raise RuntimeError(
                f"{context} physical storage differs: {table_name} "
                f"engine={engine!r} collation={collation!r}"
            )

    column_rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, "
        "IS_NULLABLE, COLUMN_DEFAULT, EXTRA, CHARACTER_SET_NAME, COLLATION_NAME "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
        f"({placeholders}) ORDER BY TABLE_NAME, ORDINAL_POSITION"
    ), params))
    by_table: dict[str, list[dict[str, Any]]] = {name: [] for name in contracts}
    for row in column_rows:
        table_name = str(_row_value(row, "TABLE_NAME") or "")
        if table_name in by_table:
            by_table[table_name].append(row)
    for table_name, contract in contracts.items():
        actual = sorted(
            by_table[table_name],
            key=lambda row: int(_row_value(row, "ORDINAL_POSITION") or 0),
        )
        actual_names = tuple(str(_row_value(row, "COLUMN_NAME") or "") for row in actual)
        expected_names = tuple(name for name, _ in contract.columns)
        if actual_names != expected_names:
            raise RuntimeError(
                f"{context} physical column inventory differs: {table_name} "
                f"expected={expected_names} actual={actual_names}"
            )
        for row, (column_name, expected) in zip(actual, contract.columns):
            actual_type = " ".join(
                str(_row_value(row, "COLUMN_TYPE") or "").casefold().split()
            )
            actual_nullable = str(_row_value(row, "IS_NULLABLE") or "").upper() == "YES"
            actual_default = _normalize_default(_row_value(row, "COLUMN_DEFAULT"))
            actual_extra = _normalize_extra(_row_value(row, "EXTRA"))
            expected_extra = _normalize_extra(expected.extra)
            if (
                actual_type != expected.column_type.casefold()
                or actual_nullable != expected.nullable
                or actual_default != _normalize_default(expected.default)
                or actual_extra != expected_extra
            ):
                raise RuntimeError(
                    f"{context} physical column differs: {table_name}.{column_name} "
                    f"type={actual_type!r} nullable={actual_nullable} "
                    f"default={actual_default!r} extra={actual_extra!r}"
                )
            charset = _row_value(row, "CHARACTER_SET_NAME")
            collation = _row_value(row, "COLLATION_NAME")
            if expected.character:
                if str(charset or "") != EXPECTED_CHARSET or str(collation or "") != contract.collation:
                    raise RuntimeError(
                        f"{context} physical column collation differs: "
                        f"{table_name}.{column_name}"
                    )
            elif charset is not None or collation is not None:
                raise RuntimeError(
                    f"{context} non-character column has collation: "
                    f"{table_name}.{column_name}"
                )

    index_rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, "
        "SUB_PART, INDEX_TYPE FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
        f"({placeholders}) ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
    ), params))
    index_parts: dict[str, dict[str, list[dict[str, Any]]]] = {
        name: {} for name in contracts
    }
    for row in index_rows:
        table_name = str(_row_value(row, "TABLE_NAME") or "")
        index_name = str(_row_value(row, "INDEX_NAME") or "")
        if table_name in index_parts:
            index_parts[table_name].setdefault(index_name, []).append(row)
    for table_name, contract in contracts.items():
        if set(index_parts[table_name]) != set(contract.indexes):
            raise RuntimeError(
                f"{context} physical index inventory differs: {table_name} "
                f"expected={sorted(contract.indexes)} "
                f"actual={sorted(index_parts[table_name])}"
            )
        for index_name, expected in contract.indexes.items():
            rows = sorted(
                index_parts[table_name][index_name],
                key=lambda row: int(_row_value(row, "SEQ_IN_INDEX") or 0),
            )
            columns = tuple(str(_row_value(row, "COLUMN_NAME") or "") for row in rows)
            unique_values = {int(_row_value(row, "NON_UNIQUE") or 0) == 0 for row in rows}
            index_types = {str(_row_value(row, "INDEX_TYPE") or "").upper() for row in rows}
            prefix_parts = {_row_value(row, "SUB_PART") for row in rows}
            if (
                columns != expected.columns
                or unique_values != {expected.unique}
                or index_types != {expected.index_type.upper()}
                or prefix_parts != {None}
            ):
                raise RuntimeError(
                    f"{context} physical index differs: {table_name}.{index_name}"
                )

    return {
        "table_names": list(contracts),
        "table_count": len(contracts),
        "contract_hash": frozen_contract_hash(contracts),
        "physical_contract_verified": True,
        "read_only": True,
        "runtime_ddl_required": False,
    }


def validate_frozen_tables(
    engine,
    contracts: Mapping[str, FrozenTable],
    *,
    context: str,
    connection=None,
) -> dict[str, Any]:
    """Validate the complete physical contract using SELECT statements only."""

    if connection is not None:
        return _validate_on_connection(connection, contracts, context=context)
    with engine.connect() as bound_connection:
        return _validate_on_connection(bound_connection, contracts, context=context)


def privileged_migrate_frozen_tables(
    engine,
    contracts: Mapping[str, FrozenTable],
    *,
    context: str,
) -> dict[str, Any]:
    """Create frozen QMT tables inside an explicit privileged release window.

    ``CREATE IF NOT EXISTS`` is intentionally followed by the strict validator.
    Existing column/index drift is never silently rewritten or dropped.
    """

    contracts = _validate_contracts(contracts)
    with engine.begin() as connection:
        for contract in contracts.values():
            connection.execute(text(contract.ddl))
        result = _validate_on_connection(connection, contracts, context=context)
    return {
        **result,
        "migrated_table_count": len(contracts),
        "privileged_migration": True,
        "read_only": False,
    }


__all__ = [
    "EXPECTED_CHARSET",
    "EXPECTED_COLLATION",
    "EXPECTED_ENGINE",
    "FrozenColumn",
    "FrozenIndex",
    "FrozenTable",
    "character_column",
    "frozen_contract_hash",
    "privileged_migrate_frozen_tables",
    "validate_frozen_tables",
]
