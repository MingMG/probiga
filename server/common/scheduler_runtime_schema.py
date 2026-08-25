"""Privileged additive schema contract for scheduler heartbeat identity."""
from __future__ import annotations

from typing import Any

from sqlalchemy import text


EXPECTED_COLUMNS = {
    "build_sha": {
        "data_type": "char",
        "character_maximum_length": 40,
        "is_nullable": "YES",
    },
    "executor_role": {
        "data_type": "varchar",
        "character_maximum_length": 40,
        "is_nullable": "YES",
    },
}


def _runtime_columns(connection) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        text(
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
            "IS_NULLABLE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME='st_scheduler_runtime' "
            "AND COLUMN_NAME IN ('build_sha', 'executor_role')"
        )
    ).mappings()
    return {
        str(row["COLUMN_NAME"]): {
            "data_type": str(row["DATA_TYPE"]).lower(),
            "character_maximum_length": (
                int(row["CHARACTER_MAXIMUM_LENGTH"])
                if row["CHARACTER_MAXIMUM_LENGTH"] is not None
                else None
            ),
            "is_nullable": str(row["IS_NULLABLE"]).upper(),
        }
        for row in rows
    }


def preflight_scheduler_runtime_heartbeat_schema(engine) -> dict[str, Any]:
    """Read only: allow missing legacy columns, reject incompatible ones."""

    with engine.connect() as connection:
        actual = _runtime_columns(connection)
    drift = {
        name: spec
        for name, spec in actual.items()
        if name not in EXPECTED_COLUMNS or EXPECTED_COLUMNS[name] != spec
    }
    if drift:
        raise RuntimeError(
            "st_scheduler_runtime heartbeat identity columns differ from contract"
        )
    missing = sorted(set(EXPECTED_COLUMNS) - set(actual))
    return {
        "status": "ok",
        "table": "st_scheduler_runtime",
        "existing_columns": actual,
        "missing_columns": missing,
        "migration_required": bool(missing),
        "read_only": True,
    }


def validate_scheduler_runtime_heartbeat_schema(engine) -> dict[str, Any]:
    """Read only: require the exact post-cutover physical contract."""

    with engine.connect() as connection:
        actual = _runtime_columns(connection)
    if actual != EXPECTED_COLUMNS:
        raise RuntimeError(
            "st_scheduler_runtime heartbeat identity columns differ from contract"
        )
    return {
        "table": "st_scheduler_runtime",
        "columns": actual,
        "physical_contract_verified": True,
        "read_only": True,
    }


def migrate_scheduler_runtime_heartbeat(engine) -> dict[str, Any]:
    """DDL entrypoint; callers must supply the fenced privileged migrator."""

    added: list[str] = []
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE IF NOT EXISTS st_scheduler_runtime ("
                "instance_id VARCHAR(128) PRIMARY KEY, "
                "mode VARCHAR(32) NOT NULL, host_name VARCHAR(128) NULL, "
                "pid INT NULL, build_sha CHAR(40) NULL, "
                "executor_role VARCHAR(40) NULL, started_at DATETIME NULL, "
                "heartbeat_at DATETIME NOT NULL, poll_seconds INT NULL, "
                "max_concurrent_tasks INT NULL, "
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP "
                "ON UPDATE CURRENT_TIMESTAMP)"
            )
        )
        existing = _runtime_columns(connection)
        drift = {
            name: spec
            for name, spec in existing.items()
            if name not in EXPECTED_COLUMNS or EXPECTED_COLUMNS[name] != spec
        }
        if drift:
            raise RuntimeError(
                "st_scheduler_runtime heartbeat identity columns differ from contract"
            )
        if "build_sha" not in existing:
            connection.execute(
                text(
                    "ALTER TABLE st_scheduler_runtime "
                    "ADD COLUMN build_sha CHAR(40) NULL AFTER pid"
                )
            )
            added.append("build_sha")
        if "executor_role" not in existing:
            connection.execute(
                text(
                    "ALTER TABLE st_scheduler_runtime "
                    "ADD COLUMN executor_role VARCHAR(40) NULL AFTER build_sha"
                )
            )
            added.append("executor_role")
        actual = _runtime_columns(connection)
        if actual != EXPECTED_COLUMNS:
            raise RuntimeError(
                "st_scheduler_runtime heartbeat identity columns differ from contract"
            )
    return {
        "status": "ok",
        "table": "st_scheduler_runtime",
        "added_columns": sorted(added),
        "columns": actual,
        "physical_contract_verified": True,
    }
