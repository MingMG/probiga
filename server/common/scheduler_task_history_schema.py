"""Privileged schema migration for the scheduler's mandatory audit ledger.

Runtime scheduler accounts are DML-only.  They may validate and append audit
rows, but table creation, legacy-column upgrades and index repair belong to
the writer-fenced privileged release migration.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text


TABLE_NAME = "st_scheduled_task_history"
PROTECTED_RELEASE_TASK_TYPES = (
    "qmt_edge_release_request",
    "qmt_edge_release_bootstrap",
)
RELEASE_RECEIPT_TRIGGER_NAMES = (
    "trg_scheduler_history_qmt_release_no_update",
    "trg_scheduler_history_qmt_release_no_delete",
)
REQUIRED_COLUMNS = (
    "id",
    "run_uid",
    "task_id",
    "task_name",
    "task_type",
    "run_at",
    "finished_at",
    "status",
    "duration",
    "exit_code",
    "output",
    "host_name",
    "scheduler_instance_id",
    "build_sha",
    "trigger_source",
)
_ADDITIVE_COLUMNS = {
    "run_uid": "VARCHAR(64) NULL",
    "task_name": "VARCHAR(255) NULL",
    "task_type": "VARCHAR(64) NULL",
    "finished_at": "DATETIME NULL",
    "exit_code": "INT NULL",
    "output": "TEXT NULL",
    "host_name": "VARCHAR(128) NULL",
    "scheduler_instance_id": "VARCHAR(128) NULL",
    "build_sha": "CHAR(40) NULL AFTER scheduler_instance_id",
    "trigger_source": "VARCHAR(32) NULL DEFAULT 'scheduled'",
}
_REQUIRED_INDEX_SHAPES = {
    (True, ("id",)),
    (True, ("run_uid",)),
    (False, ("task_id", "run_at")),
}


def scheduler_task_history_trigger_ddl_statements() -> tuple[str, ...]:
    protected = ",".join(
        f"'{value}'" for value in PROTECTED_RELEASE_TASK_TYPES
    )
    return (
        f"""CREATE TRIGGER IF NOT EXISTS {RELEASE_RECEIPT_TRIGGER_NAMES[0]}
        BEFORE UPDATE ON {TABLE_NAME} FOR EACH ROW
        BEGIN
          IF OLD.task_type IN ({protected}) THEN
            SIGNAL SQLSTATE '45000'
              SET MESSAGE_TEXT='QMT edge release audit rows are append-only';
          END IF;
        END""",
        f"""CREATE TRIGGER IF NOT EXISTS {RELEASE_RECEIPT_TRIGGER_NAMES[1]}
        BEFORE DELETE ON {TABLE_NAME} FOR EACH ROW
        BEGIN
          IF OLD.task_type IN ({protected}) THEN
            SIGNAL SQLSTATE '45000'
              SET MESSAGE_TEXT='QMT edge release audit rows are append-only';
          END IF;
        END""",
    )


def _columns(connection) -> set[str]:
    rows = connection.execute(
        text(f"SHOW COLUMNS FROM {TABLE_NAME}")
    ).fetchall()
    return {str(row[0]) for row in rows}


def _audit_column_contract(connection) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        text(
            "SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            f"AND TABLE_NAME='{TABLE_NAME}' "
            "AND COLUMN_NAME IN ('run_uid','trigger_source')"
        )
    ).mappings().all()
    return {
        str(row.get("COLUMN_NAME") or row.get("column_name") or ""): {
            "is_nullable": str(
                row.get("IS_NULLABLE") or row.get("is_nullable") or ""
            ).upper(),
            "default": (
                row.get("COLUMN_DEFAULT")
                if "COLUMN_DEFAULT" in row
                else row.get("column_default")
            ),
        }
        for row in rows
    }


def _index_inventory(connection) -> tuple[set[tuple[bool, tuple[str, ...]]], set[str]]:
    rows = connection.execute(
        text(f"SHOW INDEX FROM {TABLE_NAME}")
    ).mappings().all()
    indexes: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("Key_name") or row.get("key_name") or "")
        if not name:
            continue
        non_unique = (
            row.get("Non_unique")
            if row.get("Non_unique") is not None
            else row.get("non_unique")
        )
        item = indexes.setdefault(
            name,
            {"unique": int(non_unique or 0) == 0, "columns": []},
        )
        item["columns"].append(
            (
                int(
                    row.get("Seq_in_index")
                    or row.get("seq_in_index")
                    or 0
                ),
                str(
                    row.get("Column_name")
                    or row.get("column_name")
                    or ""
                ),
            )
        )
    return (
        {
            (
                bool(item["unique"]),
                tuple(
                    column
                    for _sequence, column in sorted(item["columns"])
                ),
            )
            for item in indexes.values()
        },
        set(indexes),
    )


def _available_index_name(used_names: set[str], preferred: str) -> str:
    if preferred not in used_names:
        used_names.add(preferred)
        return preferred
    suffix = 2
    while f"{preferred}_{suffix}" in used_names:
        suffix += 1
    result = f"{preferred}_{suffix}"
    used_names.add(result)
    return result


def validate_scheduler_task_history_schema(engine) -> dict[str, Any]:
    """Read-only proof that every execution can be uniquely audited."""

    with engine.connect() as connection:
        columns = _columns(connection)
        index_shapes, _names = _index_inventory(connection)
    missing_columns = sorted(set(REQUIRED_COLUMNS) - columns)
    missing_indexes = sorted(
        shape
        for shape in _REQUIRED_INDEX_SHAPES
        if shape not in index_shapes
        and not (
            shape == (False, ("task_id", "run_at"))
            and (True, ("task_id", "run_at")) in index_shapes
        )
    )
    if missing_columns or missing_indexes:
        raise RuntimeError(
            "scheduler task history physical contract differs: "
            f"missing_columns={missing_columns}, "
            f"missing_indexes={missing_indexes}"
        )
    return {
        "table": TABLE_NAME,
        "column_names": list(REQUIRED_COLUMNS),
        "required_index_count": len(_REQUIRED_INDEX_SHAPES),
        "physical_contract_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def migrate_scheduler_task_history(engine) -> dict[str, Any]:
    """Create/upgrade the audit ledger using a privileged fenced engine."""

    added_columns: list[str] = []
    added_indexes: list[str] = []
    with engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ("
                "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                "run_uid VARCHAR(64) NOT NULL, task_id INT NOT NULL, "
                "task_name VARCHAR(255) NULL, task_type VARCHAR(64) NULL, "
                "run_at DATETIME NOT NULL, finished_at DATETIME NULL, "
                "status VARCHAR(32) NOT NULL, duration INT NOT NULL DEFAULT 0, "
                "exit_code INT NULL, output TEXT NULL, host_name VARCHAR(128) NULL, "
                "scheduler_instance_id VARCHAR(128) NULL, "
                "build_sha CHAR(40) NULL, "
                "trigger_source VARCHAR(32) NOT NULL DEFAULT 'scheduled', "
                "UNIQUE KEY uk_scheduled_task_history_run_uid (run_uid), "
                "KEY idx_scheduled_task_history_task_run (task_id, run_at)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
                "COLLATE=utf8mb4_unicode_ci"
            )
        )
        columns = _columns(connection)
        immutable_core = {"id", "task_id", "run_at", "status", "duration"}
        missing_core = sorted(immutable_core - columns)
        if missing_core:
            raise RuntimeError(
                "legacy scheduler history core columns differ: "
                + ",".join(missing_core)
            )
        for column, ddl in _ADDITIVE_COLUMNS.items():
            if column in columns:
                continue
            connection.execute(
                text(
                    f"ALTER TABLE {TABLE_NAME} "
                    f"ADD COLUMN `{column}` {ddl}"
                )
            )
            added_columns.append(column)

        column_contract = _audit_column_contract(connection)
        run_uid_contract = column_contract.get("run_uid")
        trigger_contract = column_contract.get("trigger_source")
        if run_uid_contract is None or trigger_contract is None:
            raise RuntimeError("scheduler history audit columns are unavailable")
        run_uid_needs_normalization = (
            run_uid_contract["is_nullable"] != "NO"
        )
        trigger_needs_normalization = (
            trigger_contract["is_nullable"] != "NO"
            or str(trigger_contract["default"] or "") != "scheduled"
        )
        # Legacy rows predate run_uid/trigger_source.  Backfill deterministic
        # local identifiers before installing the uniqueness contract.
        if run_uid_needs_normalization:
            connection.execute(
                text(
                    f"UPDATE {TABLE_NAME} SET run_uid=CONCAT("
                    "'legacy-', LPAD(LOWER(HEX(id)), 16, '0')) "
                    "WHERE run_uid IS NULL OR run_uid=''"
                )
            )
        if trigger_needs_normalization:
            connection.execute(
                text(
                    f"UPDATE {TABLE_NAME} SET trigger_source='scheduled' "
                    "WHERE trigger_source IS NULL OR trigger_source=''"
                )
            )
        duplicate = connection.execute(
            text(
                f"SELECT run_uid FROM {TABLE_NAME} GROUP BY run_uid "
                "HAVING COUNT(*)<>1 LIMIT 1"
            )
        ).first()
        if duplicate is not None:
            raise RuntimeError("scheduler history run_uid is not unique")
        normalization_clauses: list[str] = []
        if run_uid_needs_normalization:
            normalization_clauses.append(
                "MODIFY COLUMN run_uid VARCHAR(64) NOT NULL"
            )
        if trigger_needs_normalization:
            normalization_clauses.append(
                "MODIFY COLUMN trigger_source VARCHAR(32) NOT NULL "
                "DEFAULT 'scheduled'"
            )
        if normalization_clauses:
            connection.execute(
                text(
                    f"ALTER TABLE {TABLE_NAME} "
                    + ", ".join(normalization_clauses)
                )
            )

        index_shapes, used_names = _index_inventory(connection)
        if (True, ("id",)) not in index_shapes:
            raise RuntimeError("scheduler history primary id index differs")
        if (True, ("run_uid",)) not in index_shapes:
            name = _available_index_name(
                used_names,
                "uk_scheduled_task_history_run_uid",
            )
            connection.execute(
                text(
                    f"ALTER TABLE {TABLE_NAME} ADD UNIQUE INDEX "
                    f"`{name}` (`run_uid`)"
                )
            )
            added_indexes.append(name)
        if not any(
            shape in index_shapes
            for shape in (
                (False, ("task_id", "run_at")),
                (True, ("task_id", "run_at")),
            )
        ):
            name = _available_index_name(
                used_names,
                "idx_scheduled_task_history_task_run",
            )
            connection.execute(
                text(
                    f"ALTER TABLE {TABLE_NAME} ADD INDEX "
                    f"`{name}` (`task_id`, `run_at`)"
                )
            )
            added_indexes.append(name)

    validated = validate_scheduler_task_history_schema(engine)
    return {
        **validated,
        "status": "ok",
        "added_columns": sorted(added_columns),
        "added_indexes": sorted(added_indexes),
    }


__all__ = [
    "PROTECTED_RELEASE_TASK_TYPES",
    "REQUIRED_COLUMNS",
    "RELEASE_RECEIPT_TRIGGER_NAMES",
    "TABLE_NAME",
    "migrate_scheduler_task_history",
    "scheduler_task_history_trigger_ddl_statements",
    "validate_scheduler_task_history_schema",
]
