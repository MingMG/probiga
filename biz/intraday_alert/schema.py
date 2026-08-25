# -*- coding: utf-8 -*-
"""Durable observation, state and outbox storage for intraday alerts."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


OBSERVATION_TABLE = "sm_intraday_alert_observation"
STATE_TABLE = "st_intraday_alert_state"
OUTBOX_TABLE = "st_intraday_alert_outbox"
BENCHMARK_TABLE = "sm_intraday_benchmark_snapshot"


_DDL = (
    f"""
    CREATE TABLE IF NOT EXISTS {OBSERVATION_TABLE} (
        observation_id CHAR(64) NOT NULL PRIMARY KEY,
        trade_date DATE NOT NULL,
        source_snapshot_at DATETIME NOT NULL,
        observed_at DATETIME NOT NULL,
        session_minute INT NOT NULL,
        source_provider VARCHAR(80) NOT NULL,
        source_receipt_id VARCHAR(64) NOT NULL,
        expected_count INT NOT NULL,
        observed_count INT NOT NULL,
        coverage DECIMAL(18,8) NOT NULL,
        median_return_pct DECIMAL(18,8) NOT NULL,
        equal_weight_return_pct DECIMAL(18,8) NOT NULL,
        positive_breadth_pct DECIMAL(18,8) NOT NULL,
        total_amount DECIMAL(24,4) NOT NULL,
        amount_delta DECIMAL(24,4) NOT NULL,
        market_json LONGTEXT NOT NULL,
        sector_json LONGTEXT NOT NULL,
        key_stock_json LONGTEXT NOT NULL,
        style_json LONGTEXT NOT NULL,
        benchmark_json LONGTEXT NOT NULL,
        quality_status VARCHAR(16) NOT NULL,
        config_version VARCHAR(80) NOT NULL,
        config_hash CHAR(64) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uk_intraday_alert_source
            (trade_date, source_snapshot_at, source_receipt_id),
        KEY idx_intraday_alert_observation_latest
            (trade_date, observed_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
        trade_date DATE NOT NULL,
        event_key VARCHAR(220) NOT NULL,
        cycle INT NOT NULL DEFAULT 1,
        event_type VARCHAR(48) NOT NULL,
        subject_code VARCHAR(160) NOT NULL,
        subject_name VARCHAR(192) NOT NULL,
        direction VARCHAR(16) NOT NULL,
        state VARCHAR(24) NOT NULL,
        severity INT NOT NULL DEFAULT 0,
        hit_count INT NOT NULL DEFAULT 0,
        miss_count INT NOT NULL DEFAULT 0,
        first_seen_at DATETIME NOT NULL,
        last_seen_at DATETIME NOT NULL,
        last_sent_state VARCHAR(24) NULL,
        last_sent_at DATETIME NULL,
        cooldown_until DATETIME NULL,
        evidence_hash CHAR(64) NOT NULL,
        evidence_json LONGTEXT NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (trade_date, event_key),
        KEY idx_intraday_alert_state_latest (trade_date, last_seen_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {OUTBOX_TABLE} (
        outbox_id CHAR(64) NOT NULL PRIMARY KEY,
        trade_date DATE NOT NULL,
        event_key VARCHAR(220) NOT NULL,
        cycle INT NOT NULL,
        transition_name VARCHAR(32) NOT NULL,
        state VARCHAR(24) NOT NULL,
        mode VARCHAR(16) NOT NULL,
        status VARCHAR(16) NOT NULL,
        evidence_hash CHAR(64) NOT NULL,
        evidence_json LONGTEXT NOT NULL,
        content_sha256 CHAR(64) NOT NULL,
        content_markdown TEXT NOT NULL,
        attempts INT NOT NULL DEFAULT 0,
        next_retry_at DATETIME NULL,
        claimed_at DATETIME NULL,
        delivery_id VARCHAR(36) NULL,
        error_message VARCHAR(512) NULL,
        created_at DATETIME NOT NULL,
        sent_at DATETIME NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE KEY uk_intraday_alert_transition
            (trade_date, event_key, cycle, transition_name, evidence_hash),
        KEY idx_intraday_alert_outbox_due
            (mode, status, next_retry_at, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {BENCHMARK_TABLE} (
        trade_date DATE NOT NULL,
        snapshot_at DATETIME NOT NULL,
        session_minute INT NOT NULL,
        instrument_code VARCHAR(16) NOT NULL,
        instrument_name VARCHAR(80) NOT NULL,
        instrument_type VARCHAR(16) NOT NULL,
        price DECIMAL(20,6) NOT NULL,
        change_pct DECIMAL(18,8) NOT NULL,
        amount DECIMAL(24,4) NOT NULL,
        amount_delta DECIMAL(24,4) NOT NULL,
        source_provider VARCHAR(80) NOT NULL,
        source_time DATETIME NOT NULL,
        quality_status VARCHAR(16) NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (trade_date, snapshot_at, instrument_code),
        KEY idx_intraday_benchmark_code_time
            (instrument_code, trade_date, session_minute, snapshot_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)

_INTRADAY_ALERT_SCHEMA = {
    OBSERVATION_TABLE: RuntimeTable(
        columns={
            "observation_id": RuntimeColumn("char", False, character_length=64),
            "trade_date": RuntimeColumn("date", False),
            "source_snapshot_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "observed_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "session_minute": RuntimeColumn("int", False),
            "source_provider": RuntimeColumn("varchar", False, character_length=80),
            "source_receipt_id": RuntimeColumn("varchar", False, character_length=64),
            "expected_count": RuntimeColumn("int", False),
            "observed_count": RuntimeColumn("int", False),
            "coverage": RuntimeColumn("decimal", False, numeric_precision=18, numeric_scale=8),
            "median_return_pct": RuntimeColumn("decimal", False, numeric_precision=18, numeric_scale=8),
            "equal_weight_return_pct": RuntimeColumn("decimal", False, numeric_precision=18, numeric_scale=8),
            "positive_breadth_pct": RuntimeColumn("decimal", False, numeric_precision=18, numeric_scale=8),
            "total_amount": RuntimeColumn("decimal", False, numeric_precision=24, numeric_scale=4),
            "amount_delta": RuntimeColumn("decimal", False, numeric_precision=24, numeric_scale=4),
            "market_json": RuntimeColumn("longtext", False),
            "sector_json": RuntimeColumn("longtext", False),
            "key_stock_json": RuntimeColumn("longtext", False),
            "style_json": RuntimeColumn("longtext", False),
            "benchmark_json": RuntimeColumn("longtext", False),
            "quality_status": RuntimeColumn("varchar", False, character_length=16),
            "config_version": RuntimeColumn("varchar", False, character_length=80),
            "config_hash": RuntimeColumn("char", False, character_length=64),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("observation_id",), unique=True),
            RuntimeIndex(("trade_date", "source_snapshot_at", "source_receipt_id"), unique=True),
            RuntimeIndex(("trade_date", "observed_at")),
        ),
    ),
    STATE_TABLE: RuntimeTable(
        columns={
            "trade_date": RuntimeColumn("date", False),
            "event_key": RuntimeColumn("varchar", False, character_length=220),
            "cycle": RuntimeColumn("int", False),
            "event_type": RuntimeColumn("varchar", False, character_length=48),
            "subject_code": RuntimeColumn("varchar", False, character_length=160),
            "subject_name": RuntimeColumn("varchar", False, character_length=192),
            "direction": RuntimeColumn("varchar", False, character_length=16),
            "state": RuntimeColumn("varchar", False, character_length=24),
            "severity": RuntimeColumn("int", False),
            "hit_count": RuntimeColumn("int", False),
            "miss_count": RuntimeColumn("int", False),
            "first_seen_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "last_seen_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "last_sent_state": RuntimeColumn("varchar", True, character_length=24),
            "last_sent_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "cooldown_until": RuntimeColumn("datetime", True, datetime_precision=0),
            "evidence_hash": RuntimeColumn("char", False, character_length=64),
            "evidence_json": RuntimeColumn("longtext", False),
            "updated_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("trade_date", "event_key"), unique=True),
            RuntimeIndex(("trade_date", "last_seen_at")),
        ),
    ),
    OUTBOX_TABLE: RuntimeTable(
        columns={
            "outbox_id": RuntimeColumn("char", False, character_length=64),
            "trade_date": RuntimeColumn("date", False),
            "event_key": RuntimeColumn("varchar", False, character_length=220),
            "cycle": RuntimeColumn("int", False),
            "transition_name": RuntimeColumn("varchar", False, character_length=32),
            "state": RuntimeColumn("varchar", False, character_length=24),
            "mode": RuntimeColumn("varchar", False, character_length=16),
            "status": RuntimeColumn("varchar", False, character_length=16),
            "evidence_hash": RuntimeColumn("char", False, character_length=64),
            "evidence_json": RuntimeColumn("longtext", False),
            "content_sha256": RuntimeColumn("char", False, character_length=64),
            "content_markdown": RuntimeColumn("text", False),
            "attempts": RuntimeColumn("int", False),
            "next_retry_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "claimed_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "delivery_id": RuntimeColumn("varchar", True, character_length=36),
            "error_message": RuntimeColumn("varchar", True, character_length=512),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "sent_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "updated_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("outbox_id",), unique=True),
            RuntimeIndex(("trade_date", "event_key", "cycle", "transition_name", "evidence_hash"), unique=True),
            RuntimeIndex(("mode", "status", "next_retry_at", "created_at")),
        ),
    ),
    BENCHMARK_TABLE: RuntimeTable(
        columns={
            "trade_date": RuntimeColumn("date", False),
            "snapshot_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "session_minute": RuntimeColumn("int", False),
            "instrument_code": RuntimeColumn("varchar", False, character_length=16),
            "instrument_name": RuntimeColumn("varchar", False, character_length=80),
            "instrument_type": RuntimeColumn("varchar", False, character_length=16),
            "price": RuntimeColumn("decimal", False, numeric_precision=20, numeric_scale=6),
            "change_pct": RuntimeColumn("decimal", False, numeric_precision=18, numeric_scale=8),
            "amount": RuntimeColumn("decimal", False, numeric_precision=24, numeric_scale=4),
            "amount_delta": RuntimeColumn("decimal", False, numeric_precision=24, numeric_scale=4),
            "source_provider": RuntimeColumn("varchar", False, character_length=80),
            "source_time": RuntimeColumn("datetime", False, datetime_precision=0),
            "quality_status": RuntimeColumn("varchar", False, character_length=16),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("trade_date", "snapshot_at", "instrument_code"), unique=True),
            RuntimeIndex(("instrument_code", "trade_date", "session_minute", "snapshot_at")),
        ),
    ),
}


def privileged_migrate_intraday_alert_tables(engine: Engine) -> None:
    """Create/normalize alert tables only during a privileged release window."""

    with engine.begin() as connection:
        for statement in _DDL:
            connection.execute(text(statement))
        privileged_normalize_mysql_storage(connection, _INTRADAY_ALERT_SCHEMA)
        validate_intraday_alert_runtime(engine, connection=connection)


def validate_intraday_alert_runtime(engine: Engine, *, connection=None) -> None:
    """Read-only fail-closed intraday alert table contract."""

    validate_runtime_tables(
        engine,
        _INTRADAY_ALERT_SCHEMA,
        context="intraday_alert",
        connection=connection,
    )


def ensure_intraday_alert_tables(engine: Engine) -> None:
    """Compatibility guard: validate only; never mutate runtime schema."""

    validate_intraday_alert_runtime(engine)


__all__ = [
    "BENCHMARK_TABLE",
    "OBSERVATION_TABLE",
    "OUTBOX_TABLE",
    "STATE_TABLE",
    "ensure_intraday_alert_tables",
    "privileged_migrate_intraday_alert_tables",
    "validate_intraday_alert_runtime",
]
