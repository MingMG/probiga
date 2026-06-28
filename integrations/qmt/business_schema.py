from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.qmt.catalog import api_definitions


IDENTIFIER = re.compile(r"^[0-9A-Za-z_]+$")
EXCLUDED_TARGET_TABLES = {"", "qmt_raw_manifest"}


@dataclass(frozen=True)
class QmtBusinessColumn:
    name: str
    definition: str
    description: str


@dataclass(frozen=True)
class TableMigrationResult:
    table_name: str
    status: str
    added_columns: list[str]
    skipped_columns: list[str]
    error: str | None = None


QMT_BUSINESS_COLUMNS: tuple[QmtBusinessColumn, ...] = (
    QmtBusinessColumn("qmt_code", "VARCHAR(32) NULL COMMENT '国金QMT原始证券代码'", "QMT原始证券代码"),
    QmtBusinessColumn("data_source", "VARCHAR(32) NULL COMMENT '数据来源渠道'", "数据来源，QMT写入时使用gj_qmt"),
    QmtBusinessColumn("source_time", "DATETIME NULL COMMENT '数据源原始时间'", "QMT返回数据对应的原始时间"),
    QmtBusinessColumn("received_at", "DATETIME NULL COMMENT '本系统接收时间'", "系统接收并处理该条数据的时间"),
    QmtBusinessColumn("batch_id", "VARCHAR(64) NULL COMMENT '数据同步批次ID'", "同步批次ID"),
    QmtBusinessColumn("data_version", "VARCHAR(64) NULL COMMENT '数据版本或哈希'", "业务数据版本或来源哈希"),
    QmtBusinessColumn("quality_status", "VARCHAR(32) NULL COMMENT '数据质量状态'", "质量校验状态"),
    QmtBusinessColumn("permission_status", "VARCHAR(32) NULL COMMENT 'QMT权限状态'", "QMT接口权限状态"),
)


def qmt_business_tables() -> list[str]:
    tables = {
        definition.target_table
        for definition in api_definitions()
        if definition.target_table not in EXCLUDED_TARGET_TABLES
    }
    return sorted(tables)


def _quote_identifier(identifier: str) -> str:
    if not IDENTIFIER.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _table_exists(engine: Engine, table_name: str) -> bool:
    with engine.begin() as conn:
        value = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
                """
            ),
            {"table_name": table_name},
        ).scalar()
    return bool(value)


def _existing_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {str(row[0]) for row in rows}


def missing_qmt_columns(existing_columns: Iterable[str]) -> list[QmtBusinessColumn]:
    existing = {str(column) for column in existing_columns}
    return [column for column in QMT_BUSINESS_COLUMNS if column.name not in existing]


def migrate_table(engine: Engine, table_name: str, *, dry_run: bool = False) -> TableMigrationResult:
    _quote_identifier(table_name)
    if not _table_exists(engine, table_name):
        return TableMigrationResult(
            table_name=table_name,
            status="SKIPPED_TABLE_MISSING",
            added_columns=[],
            skipped_columns=[],
        )

    existing = _existing_columns(engine, table_name)
    missing = missing_qmt_columns(existing)
    if not missing:
        return TableMigrationResult(
            table_name=table_name,
            status="UNCHANGED",
            added_columns=[],
            skipped_columns=[column.name for column in QMT_BUSINESS_COLUMNS],
        )

    if not dry_run:
        with engine.begin() as conn:
            for column in missing:
                conn.execute(
                    text(
                        f"ALTER TABLE {_quote_identifier(table_name)} "
                        f"ADD COLUMN {_quote_identifier(column.name)} {column.definition}"
                    )
                )

    return TableMigrationResult(
        table_name=table_name,
        status="DRY_RUN" if dry_run else "MIGRATED",
        added_columns=[column.name for column in missing],
        skipped_columns=[column.name for column in QMT_BUSINESS_COLUMNS if column.name in existing],
    )


def migrate_qmt_business_tables(
    engine: Engine,
    *,
    tables: Iterable[str] | None = None,
    dry_run: bool = False,
) -> list[TableMigrationResult]:
    results: list[TableMigrationResult] = []
    for table_name in sorted(set(tables or qmt_business_tables())):
        try:
            results.append(migrate_table(engine, table_name, dry_run=dry_run))
        except Exception as exc:
            results.append(
                TableMigrationResult(
                    table_name=table_name,
                    status="ERROR",
                    added_columns=[],
                    skipped_columns=[],
                    error=str(exc),
                )
            )
    return results


def result_dicts(results: Iterable[TableMigrationResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]
