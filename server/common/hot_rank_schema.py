# -*- coding: utf-8 -*-
"""Release migration and read-only runtime contract for hot-rank collectors."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import text

from server.common.legacy_table_surface import validate_required_table_surface


HOT_RANK_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "st_hot_rank_ths": frozenset({
        "snapshot_date", "rank", "stock_code", "short_name", "change_pct",
        "hot_value", "pop_tag", "concept_tag", "etl_sync_at",
    }),
    "st_hot_pop_rank_east": frozenset({
        "snapshot_date", "rank", "stock_code", "short_name", "rank_change",
        "his_rank", "price", "price_change", "change_pct", "hot_value",
        "pop_tag", "concept_tag", "etl_sync_at",
    }),
    "st_hot_rank_xq": frozenset({
        "snapshot_date", "rank", "stock_code", "short_name", "current",
        "percent", "chg", "amount", "market_capital", "followers", "sector",
        "exchange", "increment", "diff", "etl_sync_at",
    }),
    "st_hot_rank_sina": frozenset({
        "snapshot_date", "rank", "stock_code", "short_name", "price",
        "price_change", "change_pct", "amount", "volume", "market_capital",
        "turnover_ratio", "etl_sync_at",
    }),
}

_CREATE_XQ = """
CREATE TABLE IF NOT EXISTS `st_hot_rank_xq` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `snapshot_date` DATE NOT NULL,
  `rank` INT NULL,
  `stock_code` VARCHAR(10) NULL,
  `short_name` VARCHAR(50) NULL,
  `current` DECIMAL(12,4) NULL,
  `percent` DECIMAL(8,4) NULL,
  `chg` DECIMAL(12,4) NULL,
  `amount` DECIMAL(20,2) NULL,
  `market_capital` DECIMAL(20,2) NULL,
  `followers` INT NULL,
  `sector` VARCHAR(50) NULL,
  `exchange` VARCHAR(10) NULL,
  `increment` INT NULL,
  `diff` INT NULL,
  `etl_sync_at` DATETIME NULL,
  PRIMARY KEY (`id`),
  KEY `idx_hot_rank_xq_date` (`snapshot_date`),
  KEY `idx_hot_rank_xq_stock` (`stock_code`),
  KEY `idx_hot_rank_xq_date_stock` (`snapshot_date`, `stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

_CREATE_SINA = """
CREATE TABLE IF NOT EXISTS `st_hot_rank_sina` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `snapshot_date` DATE NOT NULL,
  `rank` INT NULL,
  `stock_code` VARCHAR(10) NULL,
  `short_name` VARCHAR(50) NULL,
  `price` DECIMAL(12,4) NULL,
  `price_change` DECIMAL(12,4) NULL,
  `change_pct` DECIMAL(10,4) NULL,
  `amount` DECIMAL(20,2) NULL,
  `volume` DECIMAL(20,2) NULL,
  `market_capital` DECIMAL(20,2) NULL,
  `turnover_ratio` DECIMAL(10,4) NULL,
  `etl_sync_at` DATETIME NULL,
  PRIMARY KEY (`id`),
  KEY `idx_hot_rank_sina_date` (`snapshot_date`),
  KEY `idx_hot_rank_sina_stock` (`stock_code`),
  KEY `idx_hot_rank_sina_date_stock` (`snapshot_date`, `stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _selected_tables(tables: Iterable[str] | None) -> tuple[str, ...]:
    selected = tuple(sorted(set(tables or HOT_RANK_REQUIRED_COLUMNS)))
    unknown = sorted(set(selected) - set(HOT_RANK_REQUIRED_COLUMNS))
    if not selected or unknown:
        raise ValueError(f"invalid hot-rank table selection: {unknown}")
    return selected


def validate_hot_rank_runtime_schema(
    engine,
    *,
    tables: Iterable[str] | None = None,
) -> dict[str, Any]:
    selected = _selected_tables(tables)
    return validate_required_table_surface(
        engine,
        selected,
        context="hot-rank collectors",
        required_columns={
            table: HOT_RANK_REQUIRED_COLUMNS[table] for table in selected
        },
    )


def privileged_migrate_hot_rank_schema(engine) -> dict[str, Any]:
    """Create new hot-rank tables and add legacy snapshot columns at release."""

    added: list[str] = []
    with engine.begin() as connection:
        connection.execute(text(_CREATE_XQ))
        connection.execute(text(_CREATE_SINA))
        table_rows = connection.execute(
            text(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
                "('st_hot_rank_ths','st_hot_pop_rank_east','st_hot_rank_xq','st_hot_rank_sina')"
            )
        ).fetchall()
        present = {str(row[0]) for row in table_rows}
        missing_legacy = sorted(
            {"st_hot_rank_ths", "st_hot_pop_rank_east"} - present
        )
        if missing_legacy:
            raise RuntimeError(
                "legacy hot-rank base tables are missing: "
                f"{missing_legacy}"
            )
        column_rows = connection.execute(
            text(
                "SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
                "('st_hot_rank_ths','st_hot_pop_rank_east','st_hot_rank_xq','st_hot_rank_sina')"
            )
        ).fetchall()
        columns: dict[str, set[str]] = {
            table: set() for table in HOT_RANK_REQUIRED_COLUMNS
        }
        for table, column in column_rows:
            if str(table) in columns:
                columns[str(table)].add(str(column))
        for table in ("st_hot_rank_ths", "st_hot_pop_rank_east"):
            if "snapshot_date" not in columns[table]:
                connection.execute(
                    text(
                        f"ALTER TABLE `{table}` ADD COLUMN `snapshot_date` "
                        "DATE NULL AFTER `id`"
                    )
                )
                added.append(f"{table}.snapshot_date")
    validation = validate_hot_rank_runtime_schema(engine)
    return {
        **validation,
        "added_columns": tuple(sorted(added)),
        "privileged_migration": True,
    }


__all__ = [
    "HOT_RANK_REQUIRED_COLUMNS",
    "privileged_migrate_hot_rank_schema",
    "validate_hot_rank_runtime_schema",
]
