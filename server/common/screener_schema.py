# -*- coding: utf-8 -*-
"""Exact physical contracts for the four persisted screener tables."""
from __future__ import annotations

from sqlalchemy import text

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


SCREENER_SCHEMA = {
    "st_screener_saved": RuntimeTable(
        columns={
            "id": RuntimeColumn("bigint", False, auto_increment=True),
            "name": RuntimeColumn("varchar", False, character_length=120),
            "definition_json": RuntimeColumn("longtext", False),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "updated_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("id",), unique=True),
            RuntimeIndex(("name",), unique=True),
        ),
    ),
    "st_screener_candidate_pool": RuntimeTable(
        columns={
            "id": RuntimeColumn("bigint", False, auto_increment=True),
            "stock_code": RuntimeColumn("varchar", False, character_length=12),
            "stock_name": RuntimeColumn("varchar", False, character_length=80),
            "source": RuntimeColumn("varchar", False, character_length=40),
            "screen_name": RuntimeColumn("varchar", False, character_length=120),
            "score": RuntimeColumn("decimal", True, numeric_precision=10, numeric_scale=2),
            "as_of_date": RuntimeColumn("date", True),
            "status": RuntimeColumn("varchar", False, character_length=20),
            "reason": RuntimeColumn("text", True),
            "payload_json": RuntimeColumn("longtext", True),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "updated_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("id",), unique=True),
            RuntimeIndex(("stock_code", "source", "as_of_date"), unique=True),
        ),
    ),
    "st_screener_run_history": RuntimeTable(
        columns={
            "id": RuntimeColumn("bigint", False, auto_increment=True),
            "run_uid": RuntimeColumn("char", False, character_length=32),
            "run_key": RuntimeColumn("char", False, character_length=64),
            "preset": RuntimeColumn("varchar", False, character_length=64),
            "requested_date": RuntimeColumn("date", True),
            "session_date": RuntimeColumn("date", True),
            "data_date": RuntimeColumn("date", True),
            "evidence_date": RuntimeColumn("date", True),
            "observed_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "generated_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "freshness": RuntimeColumn("varchar", False, character_length=32),
            "status": RuntimeColumn("varchar", False, character_length=32),
            "source": RuntimeColumn("varchar", False, character_length=255),
            "universe": RuntimeColumn("varchar", False, character_length=32),
            "concept_code": RuntimeColumn("varchar", False, character_length=32),
            "result_count": RuntimeColumn("int", False),
            "request_json": RuntimeColumn("longtext", False),
            "summary_json": RuntimeColumn("longtext", True),
            "selector_json": RuntimeColumn("longtext", True),
            "push_status": RuntimeColumn("varchar", False, character_length=32),
            "push_error": RuntimeColumn("varchar", True, character_length=500),
            "pushed_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "updated_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("id",), unique=True),
            RuntimeIndex(("run_uid",), unique=True),
            RuntimeIndex(("run_key",), unique=True),
            RuntimeIndex(("session_date", "preset", "generated_at")),
            RuntimeIndex(("data_date", "preset")),
        ),
    ),
    "st_screener_run_result": RuntimeTable(
        columns={
            "id": RuntimeColumn("bigint", False, auto_increment=True),
            "run_uid": RuntimeColumn("char", False, character_length=32),
            "rank_no": RuntimeColumn("int", False),
            "selector_rank": RuntimeColumn("int", True),
            "stock_code": RuntimeColumn("varchar", False, character_length=12),
            "stock_name": RuntimeColumn("varchar", False, character_length=120),
            "score": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "ensemble_score": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "candidate_grade": RuntimeColumn("varchar", False, character_length=20),
            "action_status": RuntimeColumn("varchar", False, character_length=40),
            "primary_concept": RuntimeColumn("varchar", False, character_length=120),
            "change_pct": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "price": RuntimeColumn("decimal", True, numeric_precision=18, numeric_scale=4),
            "payload_json": RuntimeColumn("longtext", False),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("id",), unique=True),
            RuntimeIndex(("run_uid", "rank_no"), unique=True),
            RuntimeIndex(("run_uid", "stock_code"), unique=True),
            RuntimeIndex(("stock_code", "run_uid")),
        ),
    ),
}


_SCREENER_DDL = (
    """
    CREATE TABLE IF NOT EXISTS st_screener_saved (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(120) NOT NULL,
        definition_json LONGTEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_st_screener_saved_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS st_screener_candidate_pool (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(12) NOT NULL,
        stock_name VARCHAR(80) NOT NULL DEFAULT '',
        source VARCHAR(40) NOT NULL DEFAULT 'screener',
        screen_name VARCHAR(120) NOT NULL DEFAULT '',
        score DECIMAL(10,2) NULL,
        as_of_date DATE NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
        reason TEXT NULL,
        payload_json LONGTEXT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_st_screener_candidate (stock_code, source, as_of_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS st_screener_run_history (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_uid CHAR(32) NOT NULL,
        run_key CHAR(64) NOT NULL,
        preset VARCHAR(64) NOT NULL,
        requested_date DATE NULL,
        session_date DATE NULL,
        data_date DATE NULL,
        evidence_date DATE NULL,
        observed_at DATETIME NULL,
        generated_at DATETIME NOT NULL,
        freshness VARCHAR(32) NOT NULL DEFAULT '',
        status VARCHAR(32) NOT NULL DEFAULT '',
        source VARCHAR(255) NOT NULL DEFAULT '',
        universe VARCHAR(32) NOT NULL DEFAULT 'market',
        concept_code VARCHAR(32) NOT NULL DEFAULT '',
        result_count INT NOT NULL DEFAULT 0,
        request_json LONGTEXT NOT NULL,
        summary_json LONGTEXT NULL,
        selector_json LONGTEXT NULL,
        push_status VARCHAR(32) NOT NULL DEFAULT 'NOT_REQUESTED',
        push_error VARCHAR(500) NULL,
        pushed_at DATETIME NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_st_screener_run_uid (run_uid),
        UNIQUE KEY uq_st_screener_run_key (run_key),
        KEY idx_st_screener_run_date (session_date, preset, generated_at),
        KEY idx_st_screener_data_date (data_date, preset)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS st_screener_run_result (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_uid CHAR(32) NOT NULL,
        rank_no INT NOT NULL,
        selector_rank INT NULL,
        stock_code VARCHAR(12) NOT NULL,
        stock_name VARCHAR(120) NOT NULL DEFAULT '',
        score DECIMAL(12,4) NULL,
        ensemble_score DECIMAL(12,4) NULL,
        candidate_grade VARCHAR(20) NOT NULL DEFAULT '',
        action_status VARCHAR(40) NOT NULL DEFAULT '',
        primary_concept VARCHAR(120) NOT NULL DEFAULT '',
        change_pct DECIMAL(12,4) NULL,
        price DECIMAL(18,4) NULL,
        payload_json LONGTEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_st_screener_result_rank (run_uid, rank_no),
        UNIQUE KEY uq_st_screener_result_stock (run_uid, stock_code),
        KEY idx_st_screener_result_lookup (stock_code, run_uid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)


def privileged_migrate_screener_tables(engine) -> None:
    """Create/normalize screener persistence only in a release window."""

    with engine.begin() as connection:
        for statement in _SCREENER_DDL:
            connection.execute(text(statement))
        privileged_normalize_mysql_storage(connection, SCREENER_SCHEMA)
        validate_screener_runtime(engine, connection=connection)


def validate_screener_runtime(engine, *, connection=None) -> None:
    """Validate all four physical contracts using SELECTs only."""

    validate_runtime_tables(
        engine,
        SCREENER_SCHEMA,
        context="screener",
        connection=connection,
    )


def ensure_screener_tables(engine) -> None:
    """Compatibility name: runtime callers validate and never execute DDL."""

    validate_screener_runtime(engine)


__all__ = [
    "SCREENER_SCHEMA",
    "ensure_screener_tables",
    "privileged_migrate_screener_tables",
    "validate_screener_runtime",
]
