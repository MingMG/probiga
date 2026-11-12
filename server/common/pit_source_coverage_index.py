"""Nonunique physical access indexes, separate from logical PIT contracts."""
from __future__ import annotations

from sqlalchemy import text


PIT_SOURCE_COVERAGE_TABLE = "st_pit_source_coverage"
PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME = "idx_pit_coverage_batch_stock"
PIT_SOURCE_COVERAGE_BATCH_INDEX_COLUMNS = ("fact_kind", "source", "batch_id", "stock_code")
PIT_SOURCE_COVERAGE_BATCH_INDEX_DDL = (
    "ALTER TABLE `st_pit_source_coverage` "
    "ADD INDEX `idx_pit_coverage_batch_stock` (`fact_kind`,`source`,`batch_id`,`stock_code`), "
    "ALGORITHM=INPLACE, LOCK=NONE"
)

PIT_SOURCE_COVERAGE_DATE_INDEX_NAME = "idx_pit_coverage_source_known_batch"
PIT_SOURCE_COVERAGE_DATE_INDEX_COLUMNS = ("fact_kind", "source", "watermark_kind", "known_at", "batch_id")
PIT_SOURCE_COVERAGE_DATE_INDEX_DDL = (
    "ALTER TABLE `st_pit_source_coverage` "
    "ADD INDEX `idx_pit_coverage_source_known_batch` "
    "(`fact_kind`,`source`,`watermark_kind`,`known_at`,`batch_id`), "
    "ALGORITHM=INPLACE, LOCK=NONE"
)


def _index_rows(connection, index_name: str):
    return connection.execute(text(
        "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, "
        "SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, SUB_PART AS sub_part, "
        "INDEX_TYPE AS index_type, IS_VISIBLE AS is_visible, EXPRESSION AS expression, "
        "COLLATION AS collation FROM information_schema.statistics "
        "WHERE table_schema=DATABASE() AND table_name=:table_name AND index_name=:index_name "
        "ORDER BY SEQ_IN_INDEX"
    ), {
        "table_name": PIT_SOURCE_COVERAGE_TABLE,
        "index_name": index_name,
    }).mappings().all()


def _validate_index(rows, index_name: str, columns: tuple[str, ...], label: str) -> None:
    try:
        actual = [
            (
                row["index_name"], int(row["non_unique"]), int(row["seq_in_index"]),
                row["column_name"], row["sub_part"], row["index_type"],
                row["is_visible"], row["expression"], row["collation"],
            )
            for row in rows
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"PIT source coverage {label} metadata differs") from exc
    expected = [
        (index_name, 1, ordinal, column, None, "BTREE", "YES", None, "A")
        for ordinal, column in enumerate(columns, 1)
    ]
    if actual != expected:
        raise RuntimeError(f"PIT source coverage {label} shape differs")


def _ensure_index(engine, index_name: str, columns: tuple[str, ...], ddl: str, label: str) -> dict:
    """Run only inside the existing privileged, fenced schema prepare stage.

    The migrator already bounds metadata locks to 120 seconds and complete
    database replies to 1800 seconds. There are no retries or destructive DDL.
    A failed/ambiguous ADD is resolved by checking the index on the next normal
    preparation, preserving the original decision index and every fact row.
    """
    detail = {
        "table": PIT_SOURCE_COVERAGE_TABLE,
        "index": index_name,
        "columns": list(columns),
        "unique": False,
        "created": False,
        "verified": False,
    }
    if engine.dialect.name != "mysql":
        return {**detail, "not_applicable": True}
    with engine.begin() as connection:
        rows = _index_rows(connection, index_name)
        if not rows:
            connection.execute(text(ddl))
            detail["created"] = True
            rows = _index_rows(connection, index_name)
        _validate_index(rows, index_name, columns, label)
    return {**detail, "verified": True}


def ensure_pit_source_coverage_batch_index(engine) -> dict:
    return _ensure_index(
        engine, PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME, PIT_SOURCE_COVERAGE_BATCH_INDEX_COLUMNS,
        PIT_SOURCE_COVERAGE_BATCH_INDEX_DDL, "batch index",
    )


def ensure_pit_source_coverage_date_index(engine) -> dict:
    return _ensure_index(
        engine, PIT_SOURCE_COVERAGE_DATE_INDEX_NAME, PIT_SOURCE_COVERAGE_DATE_INDEX_COLUMNS,
        PIT_SOURCE_COVERAGE_DATE_INDEX_DDL, "date index",
    )
