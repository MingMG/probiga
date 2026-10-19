"""Canonical schema for the deterministic daily research pool.

The pool is a product deliverable independent of the AI/QMT evidence chain.
It is derived only from the completed daily market snapshot and an optional
capital-flow confirmation, so a data or release failure cannot silently leave
the operator without a dated research pool.
"""
from __future__ import annotations


DAILY_POOL_TABLE = "st_daily_pool"
DAILY_POOL_MIGRATION_VERSION = "20261019_001_daily_pool"

DAILY_POOL_DDL = (
    """
    CREATE TABLE IF NOT EXISTS st_daily_pool (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        run_uid CHAR(32) NOT NULL,
        pool_date DATE NOT NULL,
        data_date DATE NOT NULL,
        rank_no INT NOT NULL,
        stock_code VARCHAR(16) NOT NULL,
        stock_name VARCHAR(64) NOT NULL,
        score DECIMAL(12,6) NOT NULL,
        close_price DECIMAL(18,6) NOT NULL,
        change_pct DECIMAL(10,4) NOT NULL,
        amount DECIMAL(20,2) NOT NULL,
        main_net_inflow DECIMAL(20,2) NULL,
        reason VARCHAR(255) NOT NULL,
        payload_json LONGTEXT NOT NULL,
        status VARCHAR(24) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_daily_pool_run_code (run_uid, stock_code),
        KEY idx_daily_pool_date_rank (pool_date, rank_no),
        KEY idx_daily_pool_data_date (data_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)


__all__ = [
    "DAILY_POOL_DDL",
    "DAILY_POOL_MIGRATION_VERSION",
    "DAILY_POOL_TABLE",
]
