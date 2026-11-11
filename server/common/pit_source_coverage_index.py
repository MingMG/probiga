"""Nonunique physical access index, separate from logical PIT contracts."""
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


def _index_rows(connection):
    return connection.execute(text(
        "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, "
        "SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, SUB_PART AS sub_part, "
        "INDEX_TYPE AS index_type, IS_VISIBLE AS is_visible, EXPRESSION AS expression, "
        "COLLATION AS collation FROM information_schema.statistics "
        "WHERE table_schema=DATABASE() AND table_name=:table_name AND index_name=:index_name "
        "ORDER BY SEQ_IN_INDEX"
    ), {
        "table_name": PIT_SOURCE_COVERAGE_TABLE,
        "index_name": PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME,
    }).mappings().all()


def _validate_index(rows) -> None:
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
        raise RuntimeError("PIT source coverage batch index metadata differs") from exc
    expected = [
        (PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME, 1, ordinal, column, None, "BTREE", "YES", None, "A")
        for ordinal, column in enumerate(PIT_SOURCE_COVERAGE_BATCH_INDEX_COLUMNS, 1)
    ]
    if actual != expected:
        raise RuntimeError("PIT source coverage batch index shape differs")


def ensure_pit_source_coverage_batch_index(engine) -> dict:
    """Run only inside the existing privileged, fenced schema prepare stage.

    The migrator already bounds metadata locks to 120 seconds and complete
    database replies to 1800 seconds. There are no retries or destructive DDL.
    A failed/ambiguous ADD is resolved by checking the index on the next normal
    preparation, preserving the original decision index and every fact row.
    """
    detail = {
        "table": PIT_SOURCE_COVERAGE_TABLE,
        "index": PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME,
        "columns": list(PIT_SOURCE_COVERAGE_BATCH_INDEX_COLUMNS),
        "unique": False,
        "created": False,
        "verified": False,
    }
    if engine.dialect.name != "mysql":
        return {**detail, "not_applicable": True}
    with engine.begin() as connection:
        rows = _index_rows(connection)
        if not rows:
            connection.execute(text(PIT_SOURCE_COVERAGE_BATCH_INDEX_DDL))
            detail["created"] = True
            rows = _index_rows(connection)
        _validate_index(rows)
    return {**detail, "verified": True}
