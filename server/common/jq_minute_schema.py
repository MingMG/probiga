# -*- coding: utf-8 -*-
"""Privileged migration and exact runtime contract for JQ minute bars."""
from __future__ import annotations

from sqlalchemy import text

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


JQ_MINUTE_TABLE = "sm_stock_minute_gml"

JQ_MINUTE_SCHEMA = {
    JQ_MINUTE_TABLE: RuntimeTable(
        columns={
            "stock_code": RuntimeColumn("varchar", False, character_length=16),
            "jq_code": RuntimeColumn("varchar", False, character_length=24),
            "trade_time": RuntimeColumn("datetime", False, datetime_precision=0),
            "trade_date": RuntimeColumn("date", False),
            "open": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "high": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "low": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "close": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "volume": RuntimeColumn("bigint", True),
            "amount": RuntimeColumn("decimal", True, numeric_precision=20, numeric_scale=4),
            "pre_close": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "is_current_bar": RuntimeColumn("tinyint", False),
            "etl_sync_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("stock_code", "trade_time", "trade_date"), unique=True),
            RuntimeIndex(("trade_date", "trade_time")),
            RuntimeIndex(("etl_sync_at",)),
        ),
    )
}


JQ_MINUTE_DDL = f"""
CREATE TABLE IF NOT EXISTS `{JQ_MINUTE_TABLE}` (
  `stock_code` VARCHAR(16) NOT NULL COMMENT '6-digit stock code',
  `jq_code` VARCHAR(24) NOT NULL DEFAULT '' COMMENT 'JoinQuant security code',
  `trade_time` DATETIME NOT NULL COMMENT 'minute bar time',
  `trade_date` DATE NOT NULL COMMENT 'trade date',
  `open` DECIMAL(12,4) DEFAULT NULL COMMENT 'open price',
  `high` DECIMAL(12,4) DEFAULT NULL COMMENT 'high price',
  `low` DECIMAL(12,4) DEFAULT NULL COMMENT 'low price',
  `close` DECIMAL(12,4) DEFAULT NULL COMMENT 'close price',
  `volume` BIGINT DEFAULT NULL COMMENT 'volume',
  `amount` DECIMAL(20,4) DEFAULT NULL COMMENT 'turnover amount',
  `pre_close` DECIMAL(12,4) DEFAULT NULL COMMENT 'previous close, if available',
  `is_current_bar` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'latest include_now bar in this sync batch',
  `etl_sync_at` DATETIME NOT NULL COMMENT 'sync time',
  PRIMARY KEY (`stock_code`, `trade_time`, `trade_date`),
  KEY `idx_gml_date_time` (`trade_date`, `trade_time`),
  KEY `idx_gml_sync` (`etl_sync_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def privileged_migrate_jq_minute_tables(engine) -> None:
    """Create/normalize the JQ table only in a fenced release window."""

    with engine.begin() as connection:
        connection.execute(text(JQ_MINUTE_DDL))
        privileged_normalize_mysql_storage(connection, JQ_MINUTE_SCHEMA)
        validate_jq_minute_runtime(engine, connection=connection)


def validate_jq_minute_runtime(engine, *, connection=None) -> None:
    """Validate the full JQ physical contract using SELECTs only."""

    validate_runtime_tables(
        engine,
        JQ_MINUTE_SCHEMA,
        context="jq_minute",
        connection=connection,
    )


def ensure_jq_minute_table(engine) -> None:
    """Compatibility name: runtime callers validate and never execute DDL."""

    validate_jq_minute_runtime(engine)


__all__ = [
    "JQ_MINUTE_DDL",
    "JQ_MINUTE_SCHEMA",
    "JQ_MINUTE_TABLE",
    "ensure_jq_minute_table",
    "privileged_migrate_jq_minute_tables",
    "validate_jq_minute_runtime",
]
