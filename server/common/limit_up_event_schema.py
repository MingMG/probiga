# -*- coding: utf-8 -*-
"""Append-only limit-up lifecycle events captured from full-market QMT ticks."""
from __future__ import annotations

from sqlalchemy import text

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


LIMIT_UP_EVENT_TABLE = "sm_limit_up_event"
LIMIT_UP_EVENT_SCHEMA_VERSION = "probiga.limit-up-event.v1"


LIMIT_UP_EVENT_SCHEMA = {
    LIMIT_UP_EVENT_TABLE: RuntimeTable(
        columns={
            "event_id": RuntimeColumn("bigint", False, auto_increment=True),
            "event_key": RuntimeColumn("char", False, character_length=64),
            "trade_date": RuntimeColumn("date", False),
            "stock_code": RuntimeColumn("varchar", False, character_length=16),
            "short_name": RuntimeColumn("varchar", False, character_length=128),
            "event_type": RuntimeColumn("varchar", False, character_length=24),
            "event_at": RuntimeColumn(
                "datetime", False, datetime_precision=6,
            ),
            "price": RuntimeColumn(
                "decimal", True, numeric_precision=20, numeric_scale=6,
            ),
            "pre_close": RuntimeColumn(
                "decimal", True, numeric_precision=20, numeric_scale=6,
            ),
            "upper_limit": RuntimeColumn(
                "decimal", True, numeric_precision=20, numeric_scale=6,
            ),
            "limit_status_basis": RuntimeColumn(
                "varchar", False, character_length=40,
            ),
            "bid1_price": RuntimeColumn(
                "decimal", True, numeric_precision=20, numeric_scale=6,
            ),
            "bid1_volume_raw": RuntimeColumn(
                "decimal", True, numeric_precision=28, numeric_scale=4,
            ),
            "previous_bid1_volume_raw": RuntimeColumn(
                "decimal", True, numeric_precision=28, numeric_scale=4,
            ),
            "ask1_price": RuntimeColumn(
                "decimal", True, numeric_precision=20, numeric_scale=6,
            ),
            "ask1_volume_raw": RuntimeColumn(
                "decimal", True, numeric_precision=28, numeric_scale=4,
            ),
            "source_provider": RuntimeColumn(
                "varchar", False, character_length=64,
            ),
            "source_snapshot_at": RuntimeColumn(
                "datetime", False, datetime_precision=6,
            ),
            "source_timetag": RuntimeColumn(
                "varchar", True, character_length=40,
            ),
            "evidence_json": RuntimeColumn("longtext", False),
            "created_at": RuntimeColumn(
                "datetime", False, datetime_precision=6,
            ),
        },
        indexes=(
            RuntimeIndex(("event_id",), unique=True),
            RuntimeIndex(("event_key",), unique=True),
            RuntimeIndex(("trade_date", "event_at")),
            RuntimeIndex(("stock_code", "trade_date", "event_at")),
            RuntimeIndex(("event_type", "trade_date", "event_at")),
        ),
    ),
}


_DDL = f"""
CREATE TABLE IF NOT EXISTS {LIMIT_UP_EVENT_TABLE} (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    event_key CHAR(64) NOT NULL,
    trade_date DATE NOT NULL,
    stock_code VARCHAR(16) NOT NULL,
    short_name VARCHAR(128) NOT NULL,
    event_type VARCHAR(24) NOT NULL,
    event_at DATETIME(6) NOT NULL,
    price DECIMAL(20,6) NULL,
    pre_close DECIMAL(20,6) NULL,
    upper_limit DECIMAL(20,6) NULL,
    limit_status_basis VARCHAR(40) NOT NULL,
    bid1_price DECIMAL(20,6) NULL,
    bid1_volume_raw DECIMAL(28,4) NULL,
    previous_bid1_volume_raw DECIMAL(28,4) NULL,
    ask1_price DECIMAL(20,6) NULL,
    ask1_volume_raw DECIMAL(28,4) NULL,
    source_provider VARCHAR(64) NOT NULL,
    source_snapshot_at DATETIME(6) NOT NULL,
    source_timetag VARCHAR(40) NULL,
    evidence_json LONGTEXT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    UNIQUE KEY uk_limit_up_event_key (event_key),
    KEY idx_limit_up_event_time (trade_date, event_at),
    KEY idx_limit_up_event_stock (stock_code, trade_date, event_at),
    KEY idx_limit_up_event_type (event_type, trade_date, event_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def privileged_migrate_limit_up_event_schema(engine) -> dict[str, object]:
    """Create or normalize the append-only event table during release."""

    with engine.begin() as connection:
        connection.execute(text(_DDL))
        privileged_normalize_mysql_storage(
            connection,
            LIMIT_UP_EVENT_SCHEMA,
        )
    validate_limit_up_event_runtime(engine)
    return {
        "status": "PASS",
        "schema": LIMIT_UP_EVENT_SCHEMA_VERSION,
        "table": LIMIT_UP_EVENT_TABLE,
        "append_only": True,
    }


def validate_limit_up_event_runtime(engine, *, connection=None) -> None:
    """Fail closed unless the event table exactly matches its runtime contract."""

    validate_runtime_tables(
        engine,
        LIMIT_UP_EVENT_SCHEMA,
        context="limit_up_event",
        connection=connection,
    )


__all__ = [
    "LIMIT_UP_EVENT_SCHEMA",
    "LIMIT_UP_EVENT_SCHEMA_VERSION",
    "LIMIT_UP_EVENT_TABLE",
    "privileged_migrate_limit_up_event_schema",
    "validate_limit_up_event_runtime",
]
