# -*- coding: utf-8 -*-
"""Durable observation, state and outbox storage for intraday alerts."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)


def ensure_intraday_alert_tables(engine: Engine) -> None:
    """Create append/state tables idempotently without storing any secret."""

    with engine.begin() as connection:
        for statement in _DDL:
            connection.execute(text(statement))


__all__ = [
    "BENCHMARK_TABLE",
    "OBSERVATION_TABLE",
    "OUTBOX_TABLE",
    "STATE_TABLE",
    "ensure_intraday_alert_tables",
]
