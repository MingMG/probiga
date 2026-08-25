# -*- coding: utf-8 -*-
"""Privileged migration and read-only runtime contract for commentary profiles."""
from __future__ import annotations

from sqlalchemy import text

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


COMMENTARY_PROFILE_TABLE = "st_commentary_profiles"

COMMENTARY_PROFILE_SCHEMA = {
    COMMENTARY_PROFILE_TABLE: RuntimeTable(
        columns={
            "id": RuntimeColumn("bigint", False, auto_increment=True),
            "profile_name": RuntimeColumn("varchar", False, character_length=120),
            "commentary_text": RuntimeColumn("mediumtext", False),
            "reference_date": RuntimeColumn("date", True),
            "phase": RuntimeColumn("varchar", False, character_length=16),
            "cron_time": RuntimeColumn("varchar", False, character_length=5),
            "enabled": RuntimeColumn("tinyint", False),
            "push_enabled": RuntimeColumn("tinyint", False),
            "webhook_kind": RuntimeColumn("varchar", False, character_length=16),
            "last_run_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "last_run_status": RuntimeColumn("varchar", True, character_length=32),
            "last_push_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "last_push_status": RuntimeColumn("varchar", True, character_length=32),
            "created_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "updated_at": RuntimeColumn("datetime", True, datetime_precision=0),
        },
        indexes=(RuntimeIndex(("id",), unique=True),),
    )
}


_COMMENTARY_PROFILE_DDL = f"""
CREATE TABLE IF NOT EXISTS `{COMMENTARY_PROFILE_TABLE}` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `profile_name` VARCHAR(120) NOT NULL,
  `commentary_text` MEDIUMTEXT NOT NULL,
  `reference_date` DATE DEFAULT NULL,
  `phase` VARCHAR(16) NOT NULL DEFAULT 'premarket',
  `cron_time` VARCHAR(5) NOT NULL DEFAULT '08:55',
  `enabled` TINYINT NOT NULL DEFAULT 1,
  `push_enabled` TINYINT NOT NULL DEFAULT 1,
  `webhook_kind` VARCHAR(16) NOT NULL DEFAULT 'briefing',
  `last_run_at` DATETIME DEFAULT NULL,
  `last_run_status` VARCHAR(32) DEFAULT '',
  `last_push_at` DATETIME DEFAULT NULL,
  `last_push_status` VARCHAR(32) DEFAULT '',
  `created_at` DATETIME DEFAULT NULL,
  `updated_at` DATETIME DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def privileged_migrate_commentary_profile_table(engine) -> None:
    """Create/normalize the profile table during a fenced release window."""

    with engine.begin() as connection:
        connection.execute(text(_COMMENTARY_PROFILE_DDL))
        privileged_normalize_mysql_storage(connection, COMMENTARY_PROFILE_SCHEMA)
        validate_commentary_profile_runtime(engine, connection=connection)


def validate_commentary_profile_runtime(engine, *, connection=None) -> None:
    """Fail closed using information_schema SELECTs only."""

    validate_runtime_tables(
        engine,
        COMMENTARY_PROFILE_SCHEMA,
        context="commentary_profile",
        connection=connection,
    )


def ensure_commentary_profile_table(engine) -> None:
    """Compatibility name: runtime callers validate and never execute DDL."""

    validate_commentary_profile_runtime(engine)


__all__ = [
    "COMMENTARY_PROFILE_SCHEMA",
    "COMMENTARY_PROFILE_TABLE",
    "ensure_commentary_profile_table",
    "privileged_migrate_commentary_profile_table",
    "validate_commentary_profile_runtime",
]
