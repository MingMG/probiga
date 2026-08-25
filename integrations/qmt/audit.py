from __future__ import annotations

from sqlalchemy.engine import Engine

from integrations.qmt._control_schema import (
    FrozenColumn,
    FrozenIndex,
    FrozenTable,
    character_column,
    privileged_migrate_frozen_tables,
    validate_frozen_tables,
)


AUDIT_TABLE_DDLS = (
    """
    CREATE TABLE IF NOT EXISTS qmt_raw_manifest (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        manifest_key VARCHAR(191) NOT NULL,
        batch_id VARCHAR(64) NOT NULL,
        provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
        dataset VARCHAR(96) NOT NULL,
        api_name VARCHAR(96) NOT NULL,
        period VARCHAR(64) NOT NULL DEFAULT '',
        file_path VARCHAR(512) NOT NULL,
        symbol_count INT NOT NULL DEFAULT 0,
        row_count BIGINT NOT NULL DEFAULT 0,
        min_source_time DATETIME NULL,
        max_source_time DATETIME NULL,
        payload_hash VARCHAR(128) NULL,
        status VARCHAR(32) NOT NULL,
        error_message TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_qmt_raw_manifest (manifest_key),
        KEY idx_qmt_raw_batch (batch_id),
        KEY idx_qmt_raw_dataset_time (dataset, max_source_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS sys_data_sync_run (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
        task_type VARCHAR(64) NOT NULL,
        target_trade_date DATE NULL,
        status VARCHAR(32) NOT NULL,
        expected_count BIGINT NOT NULL DEFAULT 0,
        actual_count BIGINT NOT NULL DEFAULT 0,
        missing_count BIGINT NOT NULL DEFAULT 0,
        started_at DATETIME NOT NULL,
        finished_at DATETIME NULL,
        error_message TEXT NULL,
        extra_json TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_data_sync_run (run_id),
        KEY idx_data_sync_task_date (task_type, target_trade_date),
        KEY idx_data_sync_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS sys_data_coverage (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
        dataset VARCHAR(96) NOT NULL,
        trade_date DATE NOT NULL,
        expected_count BIGINT NOT NULL DEFAULT 0,
        actual_count BIGINT NOT NULL DEFAULT 0,
        missing_count BIGINT NOT NULL DEFAULT 0,
        coverage_ratio DECIMAL(12,8) NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL,
        batch_id VARCHAR(64) NULL,
        details_json TEXT NULL,
        checked_at DATETIME NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_data_coverage (provider, dataset, trade_date),
        KEY idx_data_coverage_status (status, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS sys_data_gap (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
        dataset VARCHAR(96) NOT NULL,
        symbol VARCHAR(64) NOT NULL DEFAULT '',
        period VARCHAR(64) NOT NULL DEFAULT '',
        gap_start DATETIME NULL,
        gap_end DATETIME NULL,
        reason VARCHAR(256) NULL,
        status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
        retry_count INT NOT NULL DEFAULT 0,
        last_run_id VARCHAR(64) NULL,
        last_error TEXT NULL,
        next_retry_at DATETIME NULL,
        resolved_at DATETIME NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NULL,
        KEY idx_data_gap_status_retry (status, next_retry_at),
        KEY idx_data_gap_dataset_symbol (dataset, symbol)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS sys_data_quality_result (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        batch_id VARCHAR(64) NULL,
        provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
        dataset VARCHAR(96) NOT NULL,
        rule_name VARCHAR(128) NOT NULL,
        status VARCHAR(32) NOT NULL,
        checked_rows BIGINT NOT NULL DEFAULT 0,
        failed_rows BIGINT NOT NULL DEFAULT 0,
        metric_value DECIMAL(24,8) NULL,
        threshold_value DECIMAL(24,8) NULL,
        details_json TEXT NULL,
        checked_at DATETIME NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_quality_run (run_id),
        KEY idx_quality_dataset_status (dataset, status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)


AUDIT_TABLE_CONTRACTS: dict[str, FrozenTable] = {
    "qmt_raw_manifest": FrozenTable(
        ddl=AUDIT_TABLE_DDLS[0],
        columns=(
            ("id", FrozenColumn("bigint", False, extra="auto_increment")),
            ("manifest_key", character_column("varchar(191)", nullable=False)),
            ("batch_id", character_column("varchar(64)", nullable=False)),
            ("provider", character_column("varchar(32)", nullable=False, default="gj_qmt")),
            ("dataset", character_column("varchar(96)", nullable=False)),
            ("api_name", character_column("varchar(96)", nullable=False)),
            ("period", character_column("varchar(64)", nullable=False, default="")),
            ("file_path", character_column("varchar(512)", nullable=False)),
            ("symbol_count", FrozenColumn("int", False, default="0")),
            ("row_count", FrozenColumn("bigint", False, default="0")),
            ("min_source_time", FrozenColumn("datetime", True)),
            ("max_source_time", FrozenColumn("datetime", True)),
            ("payload_hash", character_column("varchar(128)", nullable=True)),
            ("status", character_column("varchar(32)", nullable=False)),
            ("error_message", character_column("text", nullable=True)),
            ("created_at", FrozenColumn("timestamp", False, default="current_timestamp")),
        ),
        indexes={
            "PRIMARY": FrozenIndex(("id",), True),
            "uk_qmt_raw_manifest": FrozenIndex(("manifest_key",), True),
            "idx_qmt_raw_batch": FrozenIndex(("batch_id",), False),
            "idx_qmt_raw_dataset_time": FrozenIndex(("dataset", "max_source_time"), False),
        },
    ),
    "sys_data_sync_run": FrozenTable(
        ddl=AUDIT_TABLE_DDLS[1],
        columns=(
            ("id", FrozenColumn("bigint", False, extra="auto_increment")),
            ("run_id", character_column("varchar(64)", nullable=False)),
            ("provider", character_column("varchar(32)", nullable=False, default="gj_qmt")),
            ("task_type", character_column("varchar(64)", nullable=False)),
            ("target_trade_date", FrozenColumn("date", True)),
            ("status", character_column("varchar(32)", nullable=False)),
            ("expected_count", FrozenColumn("bigint", False, default="0")),
            ("actual_count", FrozenColumn("bigint", False, default="0")),
            ("missing_count", FrozenColumn("bigint", False, default="0")),
            ("started_at", FrozenColumn("datetime", False)),
            ("finished_at", FrozenColumn("datetime", True)),
            ("error_message", character_column("text", nullable=True)),
            ("extra_json", character_column("text", nullable=True)),
            ("created_at", FrozenColumn("timestamp", False, default="current_timestamp")),
        ),
        indexes={
            "PRIMARY": FrozenIndex(("id",), True),
            "uk_data_sync_run": FrozenIndex(("run_id",), True),
            "idx_data_sync_task_date": FrozenIndex(("task_type", "target_trade_date"), False),
            "idx_data_sync_status": FrozenIndex(("status",), False),
        },
    ),
    "sys_data_coverage": FrozenTable(
        ddl=AUDIT_TABLE_DDLS[2],
        columns=(
            ("id", FrozenColumn("bigint", False, extra="auto_increment")),
            ("provider", character_column("varchar(32)", nullable=False, default="gj_qmt")),
            ("dataset", character_column("varchar(96)", nullable=False)),
            ("trade_date", FrozenColumn("date", False)),
            ("expected_count", FrozenColumn("bigint", False, default="0")),
            ("actual_count", FrozenColumn("bigint", False, default="0")),
            ("missing_count", FrozenColumn("bigint", False, default="0")),
            ("coverage_ratio", FrozenColumn("decimal(12,8)", False, default="0")),
            ("status", character_column("varchar(32)", nullable=False)),
            ("batch_id", character_column("varchar(64)", nullable=True)),
            ("details_json", character_column("text", nullable=True)),
            ("checked_at", FrozenColumn("datetime", False)),
            ("created_at", FrozenColumn("timestamp", False, default="current_timestamp")),
        ),
        indexes={
            "PRIMARY": FrozenIndex(("id",), True),
            "uk_data_coverage": FrozenIndex(("provider", "dataset", "trade_date"), True),
            "idx_data_coverage_status": FrozenIndex(("status", "trade_date"), False),
        },
    ),
    "sys_data_gap": FrozenTable(
        ddl=AUDIT_TABLE_DDLS[3],
        columns=(
            ("id", FrozenColumn("bigint", False, extra="auto_increment")),
            ("provider", character_column("varchar(32)", nullable=False, default="gj_qmt")),
            ("dataset", character_column("varchar(96)", nullable=False)),
            ("symbol", character_column("varchar(64)", nullable=False, default="")),
            ("period", character_column("varchar(64)", nullable=False, default="")),
            ("gap_start", FrozenColumn("datetime", True)),
            ("gap_end", FrozenColumn("datetime", True)),
            ("reason", character_column("varchar(256)", nullable=True)),
            ("status", character_column("varchar(32)", nullable=False, default="PENDING")),
            ("retry_count", FrozenColumn("int", False, default="0")),
            ("last_run_id", character_column("varchar(64)", nullable=True)),
            ("last_error", character_column("text", nullable=True)),
            ("next_retry_at", FrozenColumn("datetime", True)),
            ("resolved_at", FrozenColumn("datetime", True)),
            ("created_at", FrozenColumn("timestamp", False, default="current_timestamp")),
            ("updated_at", FrozenColumn("datetime", True)),
        ),
        indexes={
            "PRIMARY": FrozenIndex(("id",), True),
            "idx_data_gap_status_retry": FrozenIndex(("status", "next_retry_at"), False),
            "idx_data_gap_dataset_symbol": FrozenIndex(("dataset", "symbol"), False),
        },
    ),
    "sys_data_quality_result": FrozenTable(
        ddl=AUDIT_TABLE_DDLS[4],
        columns=(
            ("id", FrozenColumn("bigint", False, extra="auto_increment")),
            ("run_id", character_column("varchar(64)", nullable=False)),
            ("batch_id", character_column("varchar(64)", nullable=True)),
            ("provider", character_column("varchar(32)", nullable=False, default="gj_qmt")),
            ("dataset", character_column("varchar(96)", nullable=False)),
            ("rule_name", character_column("varchar(128)", nullable=False)),
            ("status", character_column("varchar(32)", nullable=False)),
            ("checked_rows", FrozenColumn("bigint", False, default="0")),
            ("failed_rows", FrozenColumn("bigint", False, default="0")),
            ("metric_value", FrozenColumn("decimal(24,8)", True)),
            ("threshold_value", FrozenColumn("decimal(24,8)", True)),
            ("details_json", character_column("text", nullable=True)),
            ("checked_at", FrozenColumn("datetime", False)),
            ("created_at", FrozenColumn("timestamp", False, default="current_timestamp")),
        ),
        indexes={
            "PRIMARY": FrozenIndex(("id",), True),
            "idx_quality_run": FrozenIndex(("run_id",), False),
            "idx_quality_dataset_status": FrozenIndex(("dataset", "status"), False),
        },
    ),
}


def validate_audit_schema(engine: Engine, *, connection=None) -> dict[str, object]:
    """Validate all QMT audit tables using SELECT statements only."""

    return validate_frozen_tables(
        engine,
        AUDIT_TABLE_CONTRACTS,
        context="QMT audit ledger",
        connection=connection,
    )


def privileged_migrate_audit_schema(engine: Engine) -> dict[str, object]:
    """Create/validate QMT audit tables during privileged release setup."""

    return privileged_migrate_frozen_tables(
        engine,
        AUDIT_TABLE_CONTRACTS,
        context="QMT audit ledger",
    )
