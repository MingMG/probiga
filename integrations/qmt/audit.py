from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)


def ensure_audit_tables(engine: Engine) -> int:
    with engine.begin() as conn:
        for ddl in AUDIT_TABLE_DDLS:
            conn.execute(text(ddl))
    return len(AUDIT_TABLE_DDLS)
