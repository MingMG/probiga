# -*- coding: utf-8 -*-
"""Small idempotent schema migrations for operational tables."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.batch_db import quote_identifier


@dataclass(frozen=True)
class MigrationResult:
    table: str
    column: str
    status: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


REQUIRED_COLUMNS: tuple[dict[str, str], ...] = (
    {
        "table": "st_sim_event",
        "column": "short_name",
        "ddl": "VARCHAR(20) DEFAULT '' AFTER `stock_code`",
    },
    {
        "table": "st_daily_review",
        "column": "sentiment_cycle",
        "ddl": "VARCHAR(16) DEFAULT NULL COMMENT '情绪周期'",
    },
    {
        "table": "st_daily_review",
        "column": "sentiment_cycle_desc",
        "ddl": "VARCHAR(256) DEFAULT NULL COMMENT '情绪周期说明'",
    },
    {
        "table": "st_daily_review",
        "column": "limit_up_count",
        "ddl": "INT DEFAULT NULL COMMENT '涨停家数'",
    },
    {
        "table": "st_daily_review",
        "column": "limit_down_count",
        "ddl": "INT DEFAULT NULL COMMENT '跌停家数'",
    },
    {
        "table": "st_daily_review",
        "column": "touch_limit_up",
        "ddl": "INT DEFAULT NULL COMMENT '触及涨停家数'",
    },
    {
        "table": "st_daily_review",
        "column": "broken_board_count",
        "ddl": "INT DEFAULT NULL COMMENT '炸板家数'",
    },
    {
        "table": "st_daily_review",
        "column": "seal_rate",
        "ddl": "DECIMAL(5,2) DEFAULT NULL COMMENT '封板率'",
    },
    {
        "table": "st_daily_review",
        "column": "broken_rate",
        "ddl": "DECIMAL(5,2) DEFAULT NULL COMMENT '炸板率'",
    },
    {
        "table": "st_daily_review",
        "column": "max_boards",
        "ddl": "INT DEFAULT NULL COMMENT '最高连板'",
    },
    {
        "table": "st_daily_review",
        "column": "highest_board_stocks",
        "ddl": "TEXT DEFAULT NULL COMMENT '空间板个股'",
    },
    {
        "table": "st_daily_review",
        "column": "market_emotion_json",
        "ddl": "TEXT DEFAULT NULL COMMENT '市场情绪结构'",
    },
)

REQUIRED_UNIQUE_INDEXES: tuple[dict[str, str], ...] = (
    {
        "table": "sm_stock_kline",
        "index": "uk_sm_stock_kline_business",
        "columns": "stock_code,trade_date,k_type,adjust_type",
    },
    {
        "table": "sm_stock_capital_flow_daily",
        "index": "uk_sm_stock_flow_business",
        "columns": "stock_code,trade_date",
    },
)


# Keep this contract deliberately narrow.  Changing a character column's
# collation is not expand-only, so the runner validates the complete column
# shape before issuing DDL and refuses to guess when the live schema drifts.
REQUIRED_COLUMN_COLLATIONS: tuple[dict[str, str], ...] = (
    {
        "table": "st_user_portfolio",
        "column": "stock_code",
        "column_type": "varchar(16)",
        "nullable": "NO",
        "character_set": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
    },
)


def _table_exists(conn, table_name: str) -> bool:
    return bool(conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
        ),
        {"table_name": table_name},
    ).scalar())


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    return bool(conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :table_name AND COLUMN_NAME = :column_name"
        ),
        {"table_name": table_name, "column_name": column_name},
    ).scalar())


def _index_exists(conn, table_name: str, index_name: str) -> bool:
    return bool(conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :table_name AND INDEX_NAME = :index_name"
        ),
        {"table_name": table_name, "index_name": index_name},
    ).scalar())


def _column_metadata(conn, table_name: str, column_name: str) -> dict[str, Any]:
    row = conn.execute(
        text(
            "SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "
            "CHARACTER_SET_NAME, COLLATION_NAME, EXTRA, COLUMN_COMMENT "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :table_name AND COLUMN_NAME = :column_name"
        ),
        {"table_name": table_name, "column_name": column_name},
    ).mappings().first()
    return dict(row) if row else {}


def _run_column_collation_migrations(conn, *, dry_run: bool) -> list[MigrationResult]:
    results: list[MigrationResult] = []
    for item in REQUIRED_COLUMN_COLLATIONS:
        table_name = item["table"]
        column_name = item["column"]
        if not _table_exists(conn, table_name):
            results.append(MigrationResult(table_name, column_name, "missing_table", "table does not exist"))
            continue
        metadata = _column_metadata(conn, table_name, column_name)
        if not metadata:
            results.append(MigrationResult(table_name, column_name, "missing_column", "column does not exist"))
            continue

        expected_charset = item["character_set"]
        expected_collation = item["collation"]
        if (
            str(metadata.get("CHARACTER_SET_NAME") or "").lower() == expected_charset.lower()
            and str(metadata.get("COLLATION_NAME") or "").lower() == expected_collation.lower()
        ):
            results.append(MigrationResult(table_name, column_name, "exists", "column collation already matches"))
            continue

        actual_shape = {
            "column_type": str(metadata.get("COLUMN_TYPE") or "").lower(),
            "nullable": str(metadata.get("IS_NULLABLE") or "").upper(),
            "default": metadata.get("COLUMN_DEFAULT"),
            "extra": str(metadata.get("EXTRA") or ""),
        }
        expected_shape = {
            "column_type": item["column_type"].lower(),
            "nullable": item["nullable"].upper(),
            "default": None,
            "extra": "",
        }
        if actual_shape != expected_shape:
            results.append(MigrationResult(
                table_name,
                column_name,
                "contract_mismatch",
                f"refusing collation change: expected {expected_shape}, found {actual_shape}",
            ))
            continue

        ddl = (
            f"{item['column_type'].upper()} CHARACTER SET {expected_charset} "
            f"COLLATE {expected_collation} NOT NULL"
        )
        if dry_run:
            results.append(MigrationResult(table_name, column_name, "would_modify", ddl))
            continue

        # MODIFY resets omitted column attributes.  Preserve the live comment;
        # the validated contract above guarantees type/null/default/index
        # semantics remain unchanged.  Existing indexes survive MODIFY.
        conn.execute(
            text(
                f"ALTER TABLE {quote_identifier(table_name)} "
                f"MODIFY COLUMN {quote_identifier(column_name)} {ddl} COMMENT :column_comment"
            ),
            {"column_comment": str(metadata.get("COLUMN_COMMENT") or "")},
        )
        verified = _column_metadata(conn, table_name, column_name)
        if (
            str(verified.get("CHARACTER_SET_NAME") or "").lower() != expected_charset.lower()
            or str(verified.get("COLLATION_NAME") or "").lower() != expected_collation.lower()
        ):
            raise RuntimeError(
                f"collation migration verification failed for {table_name}.{column_name}"
            )
        results.append(MigrationResult(table_name, column_name, "modified", ddl))
    return results


def run_portfolio_collation_migration(
    engine: Engine,
    *,
    dry_run: bool = False,
) -> list[MigrationResult]:
    """Apply only the guarded portfolio/current join collation migration."""
    with engine.begin() as conn:
        return _run_column_collation_migrations(conn, dry_run=dry_run)


def run_migrations(engine: Engine, *, dry_run: bool = False) -> list[MigrationResult]:
    """Apply idempotent schema updates for known operational drift."""
    results: list[MigrationResult] = []
    with engine.begin() as conn:
        for item in REQUIRED_COLUMNS:
            table_name = item["table"]
            column_name = item["column"]
            if not _table_exists(conn, table_name):
                results.append(MigrationResult(table_name, column_name, "missing_table", "table does not exist"))
                continue
            if _column_exists(conn, table_name, column_name):
                results.append(MigrationResult(table_name, column_name, "exists", "column already exists"))
                continue
            ddl = item["ddl"]
            if dry_run:
                results.append(MigrationResult(table_name, column_name, "would_add", ddl))
                continue
            conn.execute(text(
                f"ALTER TABLE {quote_identifier(table_name)} "
                f"ADD COLUMN {quote_identifier(column_name)} {ddl}"
            ))
            results.append(MigrationResult(table_name, column_name, "added", ddl))
        for item in REQUIRED_UNIQUE_INDEXES:
            table_name = item["table"]
            index_name = item["index"]
            if not _table_exists(conn, table_name):
                results.append(MigrationResult(table_name, index_name, "missing_table", "table does not exist"))
                continue
            if _index_exists(conn, table_name, index_name):
                results.append(MigrationResult(table_name, index_name, "exists", "index already exists"))
                continue
            ddl = (
                f"UNIQUE KEY {quote_identifier(index_name)} "
                f"({', '.join(quote_identifier(c) for c in item['columns'].split(','))})"
            )
            if dry_run:
                results.append(MigrationResult(table_name, index_name, "would_add", ddl))
                continue
            conn.execute(text(
                f"ALTER TABLE {quote_identifier(table_name)} ADD {ddl}"
            ))
            results.append(MigrationResult(table_name, index_name, "added", ddl))
        results.extend(_run_column_collation_migrations(conn, dry_run=dry_run))
    return results


def summarize_results(results: list[MigrationResult]) -> dict[str, Any]:
    summary: dict[str, Any] = {"total": len(results), "by_status": {}}
    for result in results:
        summary["by_status"][result.status] = summary["by_status"].get(result.status, 0) + 1
    return summary
