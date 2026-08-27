# -*- coding: utf-8 -*-
"""Privileged migrations and read-only contracts for auxiliary collectors.

The tables in this module are shared by small scheduled collectors and the
Windows BigQMT bridge.  Those processes run with the application database
principal, so they must never create or alter persistent objects at runtime.
Deployment calls the ``privileged_migrate_*`` functions; collectors call only
the matching ``validate_*`` functions before publishing data.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from sqlalchemy import text

from server.common.legacy_table_surface import validate_required_table_surface


MARKET_OVERVIEW_REQUIRED_COLUMNS = frozenset(
    {
        "trade_date",
        "up_cnt",
        "down_cnt",
        "sideline_cnt",
        "total",
        "total_amount",
        "small_up_cnt",
        "small_total",
        "small_avg_chg",
        "source_table",
        "quality_status",
        "updated_at",
    }
)

HOT_STATS_REQUIRED_COLUMNS = frozenset(
    {
        "id",
        "stat_date",
        "stat_type",
        "stat_name",
        "stat_value",
        "stat_desc",
        "etl_sync_at",
    }
)

QMT_REALTIME_SYNC_RECEIPT_REQUIRED_COLUMNS = frozenset(
    {
        "receipt_id",
        "source_provider",
        "source_snapshot_token",
        "source_full_file_token",
        "source_generated_at",
        "heartbeat_at",
        "expected_count",
        "observed_count",
        "coverage",
        "published_at",
        "capture_mode",
        "quality_status",
        "evidence_json",
        "created_at",
    }
)

HOT_RANK_FUSION_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "st_hot_rank_fused": frozenset(
        {
            "stock_code",
            "short_name",
            "change_pct",
            "east_rank",
            "ths_rank",
            "xq_rank",
            "sina_rank",
            "east_score",
            "ths_score",
            "xq_score",
            "sina_score",
            "total_score",
            "source_flag",
            "fused_rank",
            "snapshot_date",
            "industry_name",
            "etl_sync_at",
        }
    ),
    "st_hot_rank_multi_day": frozenset(
        {
            "stock_code",
            "short_name",
            "appear_days",
            "continuity_rate",
            "avg_east_rank",
            "avg_ths_rank",
            "avg_xq_rank",
            "avg_sina_rank",
            "last_east_rank",
            "last_ths_rank",
            "last_xq_rank",
            "last_sina_rank",
            "avg_total_score",
            "avg_change_pct",
            "source_flag",
            "fused_rank",
            "industry_name",
            "stat_date",
            "stat_days",
            "etl_sync_at",
        }
    ),
}

SI_ALL_INDEX_CODE_REQUIRED_COLUMNS = frozenset(
    {"index_code", "concept_code", "name", "source", "etl_sync_at"}
)

QMT_MEMBERSHIP_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "qmt_membership_snapshot_run": frozenset(
        {
            "id",
            "snapshot_date",
            "source",
            "quality_status",
            "capture_mode",
            "concept_count",
            "concept_relation_count",
            "industry_count",
            "industry_relation_count",
            "concept_hash",
            "industry_hash",
            "captured_at",
        }
    ),
    "qmt_concept_member_snapshot": frozenset(
        {
            "id",
            "snapshot_date",
            "source",
            "concept_code",
            "concept_name",
            "stock_code",
            "short_name",
            "quality_status",
            "captured_at",
        }
    ),
    "qmt_industry_member_snapshot": frozenset(
        {
            "id",
            "snapshot_date",
            "source",
            "industry_code",
            "industry_name",
            "industry_type",
            "stock_code",
            "short_name",
            "quality_status",
            "captured_at",
        }
    ),
}

QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES = (
    "trg_qmt_membership_run_no_update",
    "trg_qmt_membership_run_no_delete",
    "trg_qmt_membership_concept_no_update",
    "trg_qmt_membership_concept_no_delete",
    "trg_qmt_membership_industry_no_update",
    "trg_qmt_membership_industry_no_delete",
)


_MARKET_OVERVIEW_DDL = """
CREATE TABLE IF NOT EXISTS sm_market_overview_daily (
    trade_date DATE NOT NULL PRIMARY KEY,
    up_cnt INT NOT NULL DEFAULT 0,
    down_cnt INT NOT NULL DEFAULT 0,
    sideline_cnt INT NOT NULL DEFAULT 0,
    total INT NOT NULL DEFAULT 0,
    total_amount DECIMAL(50,6) NULL,
    small_up_cnt INT NOT NULL DEFAULT 0,
    small_total INT NOT NULL DEFAULT 0,
    small_avg_chg DECIMAL(20,6) NULL,
    source_table VARCHAR(64) NOT NULL DEFAULT 'sm_stock_kline',
    quality_status VARCHAR(32) NULL,
    updated_at DATETIME NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_HOT_STATS_DDL = """
CREATE TABLE IF NOT EXISTS `st_hot_stats` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `stat_date` DATE NOT NULL COMMENT '统计日期',
  `stat_type` VARCHAR(64) NOT NULL COMMENT '统计类型',
  `stat_name` VARCHAR(256) NOT NULL COMMENT '统计项名称',
  `stat_value` DECIMAL(50,6) DEFAULT NULL COMMENT '统计值',
  `stat_desc` VARCHAR(1024) DEFAULT NULL COMMENT '统计说明',
  `etl_sync_at` DATETIME NOT NULL COMMENT '同步写入时间',
  PRIMARY KEY (`id`),
  KEY `idx_stats_date` (`stat_date`),
  KEY `idx_stats_type` (`stat_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='热门数据统计汇总'
"""

_QMT_REALTIME_SYNC_RECEIPT_DDL = """
CREATE TABLE IF NOT EXISTS st_qmt_realtime_sync_receipt_v2 (
    receipt_id VARCHAR(64) PRIMARY KEY,
    source_provider VARCHAR(80) NOT NULL,
    source_snapshot_token VARCHAR(128) NOT NULL,
    source_full_file_token VARCHAR(160) NOT NULL,
    source_generated_at DATETIME NOT NULL,
    heartbeat_at DATETIME NOT NULL,
    expected_count INT NOT NULL,
    observed_count INT NOT NULL,
    coverage DECIMAL(18,8) NOT NULL,
    published_at DATETIME NOT NULL,
    capture_mode VARCHAR(32) NOT NULL,
    quality_status VARCHAR(16) NOT NULL,
    evidence_json LONGTEXT NOT NULL,
    created_at DATETIME NOT NULL,
    UNIQUE KEY uk_qmt_realtime_source_snapshot
        (source_provider, source_snapshot_token),
    KEY idx_qmt_realtime_receipt_latest
        (capture_mode, quality_status, published_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_HOT_RANK_FUSION_DDL = (
    """
    CREATE TABLE IF NOT EXISTS `st_hot_rank_fused` (
      `id` BIGINT NOT NULL AUTO_INCREMENT,
      `snapshot_date` DATE NOT NULL,
      `fused_rank` INT NOT NULL,
      `stock_code` VARCHAR(10) NOT NULL,
      `short_name` VARCHAR(80) NOT NULL DEFAULT '',
      `industry_name` VARCHAR(160) DEFAULT NULL,
      `change_pct` DECIMAL(12,4) DEFAULT NULL,
      `east_rank` INT DEFAULT NULL,
      `ths_rank` INT DEFAULT NULL,
      `xq_rank` INT DEFAULT NULL,
      `sina_rank` INT DEFAULT NULL,
      `east_score` DECIMAL(12,4) NOT NULL DEFAULT 0,
      `ths_score` DECIMAL(12,4) NOT NULL DEFAULT 0,
      `xq_score` DECIMAL(12,4) NOT NULL DEFAULT 0,
      `sina_score` DECIMAL(12,4) NOT NULL DEFAULT 0,
      `total_score` DECIMAL(14,4) NOT NULL DEFAULT 0,
      `source_flag` VARCHAR(32) NOT NULL,
      `etl_sync_at` DATETIME NOT NULL,
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_hot_rank_fused_date_stock` (`snapshot_date`, `stock_code`),
      KEY `idx_hot_rank_fused_date_rank` (`snapshot_date`, `fused_rank`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS `st_hot_rank_multi_day` (
      `id` BIGINT NOT NULL AUTO_INCREMENT,
      `stat_date` DATE NOT NULL,
      `stat_days` INT NOT NULL,
      `fused_rank` INT NOT NULL,
      `stock_code` VARCHAR(10) NOT NULL,
      `short_name` VARCHAR(80) NOT NULL DEFAULT '',
      `industry_name` VARCHAR(160) DEFAULT NULL,
      `appear_days` INT NOT NULL,
      `continuity_rate` DECIMAL(10,2) NOT NULL DEFAULT 0,
      `avg_east_rank` DECIMAL(10,2) DEFAULT NULL,
      `avg_ths_rank` DECIMAL(10,2) DEFAULT NULL,
      `avg_xq_rank` DECIMAL(10,2) DEFAULT NULL,
      `avg_sina_rank` DECIMAL(10,2) DEFAULT NULL,
      `last_east_rank` INT DEFAULT NULL,
      `last_ths_rank` INT DEFAULT NULL,
      `last_xq_rank` INT DEFAULT NULL,
      `last_sina_rank` INT DEFAULT NULL,
      `avg_total_score` DECIMAL(14,4) NOT NULL DEFAULT 0,
      `avg_change_pct` DECIMAL(12,4) DEFAULT NULL,
      `source_flag` VARCHAR(32) NOT NULL,
      `etl_sync_at` DATETIME NOT NULL,
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_hot_rank_multi_date_days_stock`
        (`stat_date`, `stat_days`, `stock_code`),
      KEY `idx_hot_rank_multi_date_days_rank`
        (`stat_date`, `stat_days`, `fused_rank`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)

_SI_ALL_INDEX_CODE_DDL = """
CREATE TABLE IF NOT EXISTS `si_all_index_code` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `index_code` VARCHAR(32) NOT NULL,
  `concept_code` VARCHAR(64) NOT NULL DEFAULT '',
  `name` VARCHAR(256) NOT NULL DEFAULT '',
  `source` VARCHAR(64) NOT NULL DEFAULT '',
  `etl_sync_at` DATETIME NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_si_all_index_code` (`index_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

_QMT_MEMBERSHIP_DDL = (
    """
    CREATE TABLE IF NOT EXISTS qmt_membership_snapshot_run (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        source VARCHAR(40) NOT NULL,
        quality_status VARCHAR(32) NOT NULL,
        capture_mode VARCHAR(40) NOT NULL,
        concept_count INT NOT NULL,
        concept_relation_count INT NOT NULL,
        industry_count INT NOT NULL,
        industry_relation_count INT NOT NULL,
        concept_hash CHAR(64) NOT NULL,
        industry_hash CHAR(64) NOT NULL,
        captured_at DATETIME NOT NULL,
        UNIQUE KEY uk_qmt_membership_snapshot_run (snapshot_date, source),
        KEY idx_qmt_membership_snapshot_captured (captured_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS qmt_concept_member_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        source VARCHAR(40) NOT NULL,
        concept_code VARCHAR(80) NOT NULL,
        concept_name VARCHAR(160) NOT NULL DEFAULT '',
        stock_code VARCHAR(10) NOT NULL,
        short_name VARCHAR(80) NOT NULL DEFAULT '',
        quality_status VARCHAR(32) NOT NULL,
        captured_at DATETIME NOT NULL,
        UNIQUE KEY uk_qmt_concept_member_snapshot
            (snapshot_date, source, concept_code, stock_code),
        KEY idx_qmt_concept_member_stock
            (stock_code, snapshot_date),
        KEY idx_qmt_concept_member_concept
            (concept_code, snapshot_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS qmt_industry_member_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        source VARCHAR(40) NOT NULL,
        industry_code VARCHAR(80) NOT NULL,
        industry_name VARCHAR(160) NOT NULL DEFAULT '',
        industry_type VARCHAR(40) NOT NULL DEFAULT '',
        stock_code VARCHAR(10) NOT NULL,
        short_name VARCHAR(80) NOT NULL DEFAULT '',
        quality_status VARCHAR(32) NOT NULL,
        captured_at DATETIME NOT NULL,
        UNIQUE KEY uk_qmt_industry_member_snapshot
            (snapshot_date, source, industry_code, stock_code),
        KEY idx_qmt_industry_member_stock
            (stock_code, snapshot_date),
        KEY idx_qmt_industry_member_industry
            (industry_code, snapshot_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)

_QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_DDL = (
    """
    CREATE TRIGGER `trg_qmt_membership_run_no_update`
    BEFORE UPDATE ON `qmt_membership_snapshot_run`
    FOR EACH ROW
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_membership_snapshot_run is append-only'
    """,
    """
    CREATE TRIGGER `trg_qmt_membership_run_no_delete`
    BEFORE DELETE ON `qmt_membership_snapshot_run`
    FOR EACH ROW
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_membership_snapshot_run is append-only'
    """,
    """
    CREATE TRIGGER `trg_qmt_membership_concept_no_update`
    BEFORE UPDATE ON `qmt_concept_member_snapshot`
    FOR EACH ROW
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_concept_member_snapshot is append-only'
    """,
    """
    CREATE TRIGGER `trg_qmt_membership_concept_no_delete`
    BEFORE DELETE ON `qmt_concept_member_snapshot`
    FOR EACH ROW
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_concept_member_snapshot is append-only'
    """,
    """
    CREATE TRIGGER `trg_qmt_membership_industry_no_update`
    BEFORE UPDATE ON `qmt_industry_member_snapshot`
    FOR EACH ROW
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_industry_member_snapshot is append-only'
    """,
    """
    CREATE TRIGGER `trg_qmt_membership_industry_no_delete`
    BEFORE DELETE ON `qmt_industry_member_snapshot`
    FOR EACH ROW
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_industry_member_snapshot is append-only'
    """,
)


def qmt_membership_trigger_ddl_statements() -> tuple[str, ...]:
    """Return the exact six CREATE statements consumed by the release broker."""

    return tuple(_QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_DDL)


def _normalized_qmt_membership_trigger_body(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("`", "").lower()


def _expected_qmt_membership_trigger_contracts() -> dict[str, tuple[str, str, str]]:
    """Derive the complete runtime contract from the privileged DDL."""

    contracts: dict[str, tuple[str, str, str]] = {}
    pattern = re.compile(
        r"^CREATE TRIGGER\s+`?([^`\s]+)`?\s+"
        r"BEFORE\s+(UPDATE|DELETE)\s+ON\s+`?([^`\s]+)`?\s+"
        r"FOR EACH ROW\s+(.*)$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for statement in _QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_DDL:
        matched = pattern.match(statement.strip())
        if matched is None:
            raise RuntimeError("QMT membership trigger DDL is unparsable")
        name, event, table_name, action_statement = matched.groups()
        if name in contracts:
            raise RuntimeError("QMT membership trigger DDL inventory differs")
        contracts[name] = (
            table_name.lower(),
            event.upper(),
            _normalized_qmt_membership_trigger_body(action_statement),
        )
    if set(contracts) != set(QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES):
        raise RuntimeError("QMT membership trigger DDL inventory differs")
    return contracts


def validate_qmt_membership_snapshot_immutability(connection) -> None:
    """Attest the exact six-trigger append-only contract on MySQL."""

    expected = _expected_qmt_membership_trigger_contracts()
    tables = tuple(sorted(QMT_MEMBERSHIP_REQUIRED_COLUMNS))
    placeholders = ", ".join(
        f":membership_table_{index}" for index in range(len(tables))
    )
    rows = connection.execute(
        text(
            "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, "
            "ACTION_TIMING, ACTION_ORIENTATION, ACTION_STATEMENT "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA=DATABASE() AND EVENT_OBJECT_TABLE IN "
            f"({placeholders})"
        ),
        {
            f"membership_table_{index}": table_name
            for index, table_name in enumerate(tables)
        },
    ).mappings().all()
    observed = {
        str(row.get("TRIGGER_NAME") or row.get("trigger_name") or ""): row
        for row in rows
    }
    if len(rows) != len(expected) or set(observed) != set(expected):
        raise RuntimeError("QMT membership append-only trigger inventory differs")
    for name, (table_name, event, action_statement) in expected.items():
        row = observed[name]
        if (
            str(
                row.get("EVENT_OBJECT_TABLE")
                or row.get("event_object_table")
                or ""
            ).lower()
            != table_name
            or str(
                row.get("EVENT_MANIPULATION")
                or row.get("event_manipulation")
                or ""
            ).upper()
            != event
            or str(
                row.get("ACTION_TIMING") or row.get("action_timing") or ""
            ).upper()
            != "BEFORE"
            or str(
                row.get("ACTION_ORIENTATION")
                or row.get("action_orientation")
                or ""
            ).upper()
            != "ROW"
            or _normalized_qmt_membership_trigger_body(
                row.get("ACTION_STATEMENT")
                or row.get("action_statement")
                or ""
            )
            != action_statement
        ):
            raise RuntimeError(
                f"QMT membership append-only trigger contract differs: {name}"
            )


def validate_market_overview_daily_runtime_schema(engine) -> dict[str, Any]:
    return validate_required_table_surface(
        engine,
        ("sm_market_overview_daily",),
        context="market overview collector",
        required_columns={
            "sm_market_overview_daily": MARKET_OVERVIEW_REQUIRED_COLUMNS,
        },
    )


def validate_hot_stats_runtime_schema(engine) -> dict[str, Any]:
    return validate_required_table_surface(
        engine,
        ("st_hot_stats",),
        context="hot-data statistics collector",
        required_columns={"st_hot_stats": HOT_STATS_REQUIRED_COLUMNS},
    )


def validate_qmt_realtime_sync_receipt_runtime_schema(engine) -> dict[str, Any]:
    return validate_required_table_surface(
        engine,
        ("st_qmt_realtime_sync_receipt_v2",),
        context="BigQMT realtime sync receipt",
        required_columns={
            "st_qmt_realtime_sync_receipt_v2": (
                QMT_REALTIME_SYNC_RECEIPT_REQUIRED_COLUMNS
            ),
        },
    )


def _selected_hot_rank_fusion_tables(
    tables: Iterable[str] | None,
) -> tuple[str, ...]:
    selected = tuple(sorted(set(tables or HOT_RANK_FUSION_REQUIRED_COLUMNS)))
    unknown = sorted(set(selected) - set(HOT_RANK_FUSION_REQUIRED_COLUMNS))
    if not selected or unknown:
        raise ValueError(f"invalid hot-rank fusion table selection: {unknown}")
    return selected


def validate_hot_rank_fusion_runtime_schema(
    engine,
    *,
    tables: Iterable[str] | None = None,
) -> dict[str, Any]:
    selected = _selected_hot_rank_fusion_tables(tables)
    return validate_required_table_surface(
        engine,
        selected,
        context="hot-rank fusion collector",
        required_columns={
            table: HOT_RANK_FUSION_REQUIRED_COLUMNS[table]
            for table in selected
        },
    )


def validate_si_all_index_code_runtime_schema(engine) -> dict[str, Any]:
    return validate_required_table_surface(
        engine,
        ("si_all_index_code",),
        context="Sina index-code collector",
        required_columns={
            "si_all_index_code": SI_ALL_INDEX_CODE_REQUIRED_COLUMNS,
        },
    )


def validate_qmt_membership_snapshot_runtime_schema(
    engine,
    *,
    require_triggers: bool = True,
) -> dict[str, Any]:
    if type(require_triggers) is not bool:
        raise TypeError("require_triggers must be bool")
    surface = validate_required_table_surface(
        engine,
        QMT_MEMBERSHIP_REQUIRED_COLUMNS,
        context="BigQMT membership snapshot",
        required_columns=QMT_MEMBERSHIP_REQUIRED_COLUMNS,
    )
    dialect = str(
        getattr(getattr(engine, "dialect", None), "name", "") or ""
    ).lower()
    if dialect != "mysql" or not require_triggers:
        return {
            **surface,
            "trigger_names": QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES,
            "append_only_verified": False,
            "immutability_validation_skipped": (
                "release_broker_pending"
                if dialect == "mysql"
                else dialect or "non_mysql"
            ),
        }
    with engine.connect() as connection:
        validate_qmt_membership_snapshot_immutability(connection)
    return {
        **surface,
        "trigger_names": QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES,
        "append_only_verified": True,
    }


def _privileged_create(engine, statements: tuple[str, ...]) -> None:
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def privileged_migrate_market_overview_daily_schema(engine) -> dict[str, Any]:
    _privileged_create(engine, (_MARKET_OVERVIEW_DDL,))
    return {
        **validate_market_overview_daily_runtime_schema(engine),
        "privileged_migration": True,
    }


def privileged_migrate_hot_stats_schema(engine) -> dict[str, Any]:
    _privileged_create(engine, (_HOT_STATS_DDL,))
    return {
        **validate_hot_stats_runtime_schema(engine),
        "privileged_migration": True,
    }


def privileged_migrate_qmt_realtime_sync_receipt_schema(engine) -> dict[str, Any]:
    _privileged_create(engine, (_QMT_REALTIME_SYNC_RECEIPT_DDL,))
    return {
        **validate_qmt_realtime_sync_receipt_runtime_schema(engine),
        "privileged_migration": True,
    }


def privileged_migrate_hot_rank_fusion_schema(engine) -> dict[str, Any]:
    _privileged_create(engine, _HOT_RANK_FUSION_DDL)
    return {
        **validate_hot_rank_fusion_runtime_schema(engine),
        "privileged_migration": True,
    }


def privileged_migrate_si_all_index_code_schema(engine) -> dict[str, Any]:
    _privileged_create(engine, (_SI_ALL_INDEX_CODE_DDL,))
    return {
        **validate_si_all_index_code_runtime_schema(engine),
        "privileged_migration": True,
    }


def privileged_migrate_qmt_membership_snapshot_schema(
    engine,
    *,
    install_triggers: bool = False,
) -> dict[str, Any]:
    if type(install_triggers) is not bool:
        raise TypeError("install_triggers must be bool")
    if install_triggers:
        raise RuntimeError(
            "QMT membership triggers require the frozen release broker"
        )
    _privileged_create(engine, _QMT_MEMBERSHIP_DDL)
    return {
        **validate_qmt_membership_snapshot_runtime_schema(
            engine,
            require_triggers=False,
        ),
        "triggers_installed": False,
        "trigger_installation": "FROZEN_RELEASE_BROKER_REQUIRED",
        "privileged_migration": True,
    }


def privileged_migrate_auxiliary_runtime_schema(
    engine,
    *,
    install_triggers: bool = False,
) -> dict[str, Any]:
    """Prepare every auxiliary table inside the fenced release window."""

    if type(install_triggers) is not bool:
        raise TypeError("install_triggers must be bool")
    if install_triggers:
        raise RuntimeError(
            "auxiliary runtime triggers require the frozen release broker"
        )

    return {
        "market_overview": privileged_migrate_market_overview_daily_schema(engine),
        "hot_stats": privileged_migrate_hot_stats_schema(engine),
        "qmt_realtime_sync_receipt": (
            privileged_migrate_qmt_realtime_sync_receipt_schema(engine)
        ),
        "hot_rank_fusion": privileged_migrate_hot_rank_fusion_schema(engine),
        "si_all_index_code": privileged_migrate_si_all_index_code_schema(engine),
        "qmt_membership_snapshot": (
            privileged_migrate_qmt_membership_snapshot_schema(
                engine,
                install_triggers=False,
            )
        ),
        "privileged_migration": True,
    }


def validate_auxiliary_runtime_schema(
    engine,
    *,
    require_triggers: bool = True,
) -> dict[str, Any]:
    """Validate every auxiliary runtime surface without persistent writes."""

    if type(require_triggers) is not bool:
        raise TypeError("require_triggers must be bool")

    return {
        "market_overview": validate_market_overview_daily_runtime_schema(engine),
        "hot_stats": validate_hot_stats_runtime_schema(engine),
        "qmt_realtime_sync_receipt": (
            validate_qmt_realtime_sync_receipt_runtime_schema(engine)
        ),
        "hot_rank_fusion": validate_hot_rank_fusion_runtime_schema(engine),
        "si_all_index_code": validate_si_all_index_code_runtime_schema(engine),
        "qmt_membership_snapshot": (
            validate_qmt_membership_snapshot_runtime_schema(
                engine,
                require_triggers=require_triggers,
            )
        ),
        "read_only": True,
    }


__all__ = [
    "HOT_RANK_FUSION_REQUIRED_COLUMNS",
    "HOT_STATS_REQUIRED_COLUMNS",
    "MARKET_OVERVIEW_REQUIRED_COLUMNS",
    "QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES",
    "QMT_MEMBERSHIP_REQUIRED_COLUMNS",
    "QMT_REALTIME_SYNC_RECEIPT_REQUIRED_COLUMNS",
    "SI_ALL_INDEX_CODE_REQUIRED_COLUMNS",
    "qmt_membership_trigger_ddl_statements",
    "privileged_migrate_auxiliary_runtime_schema",
    "privileged_migrate_hot_rank_fusion_schema",
    "privileged_migrate_hot_stats_schema",
    "privileged_migrate_market_overview_daily_schema",
    "privileged_migrate_qmt_membership_snapshot_schema",
    "privileged_migrate_qmt_realtime_sync_receipt_schema",
    "privileged_migrate_si_all_index_code_schema",
    "validate_auxiliary_runtime_schema",
    "validate_hot_stats_runtime_schema",
    "validate_hot_rank_fusion_runtime_schema",
    "validate_market_overview_daily_runtime_schema",
    "validate_qmt_membership_snapshot_immutability",
    "validate_qmt_membership_snapshot_runtime_schema",
    "validate_qmt_realtime_sync_receipt_runtime_schema",
    "validate_si_all_index_code_runtime_schema",
]
