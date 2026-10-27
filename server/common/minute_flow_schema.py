# -*- coding: utf-8 -*-
"""Release migration and read-only contract for minute-flow trade dates."""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from sqlalchemy import text

from server.common.batch_db import quote_identifier


MINUTE_FLOW_TABLE = "sm_stock_capital_flow_min"
MINUTE_FLOW_DATE_COLUMN = "trade_date"
MINUTE_FLOW_DATE_INDEX = "idx_scfm_trade_date"
MINUTE_FLOW_DATE_INDEX_COLUMNS = (
    MINUTE_FLOW_DATE_COLUMN,
)
MINUTE_FLOW_DATE_EXPRESSIONS = (
    "cast(`trade_time` as date)",
    "date(`trade_time`)",
)


def _normalized_generation_expression(value: Any) -> str:
    return "".join(str(value or "").lower().replace("`", "").split())


def validate_minute_flow_trade_date_metadata(
    column: Mapping[str, Any] | None,
    indexes: Iterable[Sequence[str]],
) -> None:
    """Fail closed unless the date column and its daily access path are exact."""

    if column is None:
        raise RuntimeError(
            f"{MINUTE_FLOW_TABLE} runtime schema is missing "
            f"{MINUTE_FLOW_DATE_COLUMN}"
        )
    extra = str(column.get("EXTRA") or column.get("extra") or "").upper()
    expression = _normalized_generation_expression(
        column.get("GENERATION_EXPRESSION")
        or column.get("generation_expression")
    )
    if (
        str(column.get("DATA_TYPE") or column.get("data_type") or "").lower()
        != "date"
        or "VIRTUAL GENERATED" not in extra
        or expression
        not in {
            _normalized_generation_expression(value)
            for value in MINUTE_FLOW_DATE_EXPRESSIONS
        }
        or str(column.get("IS_NULLABLE") or column.get("is_nullable") or "").upper()
        != "YES"
    ):
        raise RuntimeError(
            f"{MINUTE_FLOW_TABLE} runtime schema has an invalid generated "
            f"{MINUTE_FLOW_DATE_COLUMN} contract"
        )
    expected = tuple(MINUTE_FLOW_DATE_INDEX_COLUMNS)
    if not any(tuple(index) == expected for index in indexes):
        raise RuntimeError(
            f"{MINUTE_FLOW_TABLE} runtime schema is missing index "
            f"{MINUTE_FLOW_DATE_INDEX}{expected}"
        )


def _column_row(connection) -> Mapping[str, Any] | None:
    return connection.execute(
        text(
            """
            SELECT DATA_TYPE, IS_NULLABLE, EXTRA, GENERATION_EXPRESSION
              FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA=DATABASE()
               AND TABLE_NAME=:table_name
               AND COLUMN_NAME=:column_name
            """
        ),
        {
            "table_name": MINUTE_FLOW_TABLE,
            "column_name": MINUTE_FLOW_DATE_COLUMN,
        },
    ).mappings().first()


def _indexes(connection) -> list[tuple[str, ...]]:
    rows = connection.execute(
        text(
            """
            SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME
              FROM information_schema.STATISTICS
             WHERE TABLE_SCHEMA=DATABASE()
               AND TABLE_NAME=:table_name
               AND NON_UNIQUE=1
               AND SUB_PART IS NULL
             ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """
        ),
        {"table_name": MINUTE_FLOW_TABLE},
    ).mappings().all()
    grouped: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        grouped.setdefault(str(row["INDEX_NAME"]), []).append(
            (int(row["SEQ_IN_INDEX"]), str(row["COLUMN_NAME"]))
        )
    return [
        tuple(column for _, column in sorted(parts))
        for parts in grouped.values()
    ]


def validate_minute_flow_trade_date_runtime(engine, *, connection=None) -> None:
    """Validate the generated date column and daily index using SELECTs only."""

    if connection is None:
        with engine.connect() as bound_connection:
            validate_minute_flow_trade_date_runtime(
                engine,
                connection=bound_connection,
            )
        return
    validate_minute_flow_trade_date_metadata(
        _column_row(connection),
        _indexes(connection),
    )


def privileged_migrate_minute_flow_trade_date(engine) -> dict[str, Any]:
    """Add and index the generated minute-flow date inside the release fence."""

    if engine.dialect.name != "mysql":
        raise RuntimeError(
            f"{MINUTE_FLOW_TABLE} date migration requires MySQL"
        )
    column_added = False
    index_added = False
    table = quote_identifier(MINUTE_FLOW_TABLE)
    index_name = quote_identifier(MINUTE_FLOW_DATE_INDEX)
    column_name = quote_identifier(MINUTE_FLOW_DATE_COLUMN)
    with engine.begin() as connection:
        exists = connection.execute(
            text(
                """
                SELECT COUNT(*)
                  FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA=DATABASE()
                   AND TABLE_NAME=:table_name
                """
            ),
            {"table_name": MINUTE_FLOW_TABLE},
        ).scalar_one()
        if int(exists) != 1:
            raise RuntimeError(
                f"{MINUTE_FLOW_TABLE} must exist before date migration"
            )
        if _column_row(connection) is None:
            connection.execute(
                text(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN {column_name} DATE GENERATED ALWAYS AS "
                    "(DATE(`trade_time`)) VIRTUAL "
                    "AFTER `trade_time`"
                )
            )
            column_added = True
        expected_columns = tuple(MINUTE_FLOW_DATE_INDEX_COLUMNS)
        if not any(
            tuple(index) == expected_columns for index in _indexes(connection)
        ):
            indexed_columns = ",".join(
                quote_identifier(column)
                for column in MINUTE_FLOW_DATE_INDEX_COLUMNS
            )
            connection.execute(
                text(
                    f"ALTER TABLE {table} ADD INDEX {index_name} "
                    f"({indexed_columns}), ALGORITHM=INPLACE, LOCK=NONE"
                )
            )
            index_added = True
    validate_minute_flow_trade_date_runtime(engine)
    return {
        "table": MINUTE_FLOW_TABLE,
        "column": MINUTE_FLOW_DATE_COLUMN,
        "index": MINUTE_FLOW_DATE_INDEX,
        "column_added": column_added,
        "index_added": index_added,
        "validated": True,
    }


__all__ = [
    "MINUTE_FLOW_DATE_COLUMN",
    "MINUTE_FLOW_DATE_EXPRESSIONS",
    "MINUTE_FLOW_DATE_INDEX",
    "MINUTE_FLOW_DATE_INDEX_COLUMNS",
    "MINUTE_FLOW_TABLE",
    "privileged_migrate_minute_flow_trade_date",
    "validate_minute_flow_trade_date_metadata",
    "validate_minute_flow_trade_date_runtime",
]
