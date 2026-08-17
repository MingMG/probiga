from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.batch_db import quote_identifier

SCHEDULED_TASK_TABLE = "st_scheduled_tasks"

DEFAULT_SCHEDULER_COLUMNS: dict[str, str] = {
    "task_type": "VARCHAR(50) DEFAULT 'python'",
    "group_name": "VARCHAR(32) DEFAULT 'system'",
    "script_args": "VARCHAR(500) DEFAULT ''",
    "date_param": "VARCHAR(100) DEFAULT ''",
    "date_param_desc": "VARCHAR(200) DEFAULT ''",
    "interval_minutes": "INT DEFAULT 0",
    "sort_order": "INT DEFAULT 0",
    "last_triggered_at": "DATETIME DEFAULT NULL",
    "last_run_output": "TEXT DEFAULT NULL",
    "last_run_duration": "INT DEFAULT 0",
    "etl_sync_at": "DATETIME DEFAULT NULL",
    "updated_at": "DATETIME DEFAULT NULL",
    "description": "VARCHAR(500) DEFAULT ''",
}

NOW_COLUMNS = {"created_at", "updated_at", "etl_sync_at"}

TASK_PAYLOAD_COLUMNS = {
    "task_name",
    "task_type",
    "group_name",
    "script_path",
    "script_args",
    "cron_time",
    "interval_minutes",
    "enabled",
    "description",
    "sort_order",
    "date_param",
    "date_param_desc",
}


def table_columns(engine: Engine, table_name: str = SCHEDULED_TASK_TABLE) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {str(row[0]) for row in rows}


def ensure_scheduler_columns(
    engine: Engine,
    *,
    table_name: str = SCHEDULED_TASK_TABLE,
    column_definitions: Mapping[str, str] | None = None,
) -> set[str]:
    columns = table_columns(engine, table_name)
    if not columns:
        raise RuntimeError(f"{table_name} does not exist")

    definitions = dict(DEFAULT_SCHEDULER_COLUMNS)
    if column_definitions:
        definitions.update(column_definitions)

    with engine.begin() as conn:
        for column, ddl in definitions.items():
            if column not in columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {quote_identifier(table_name)} "
                        f"ADD COLUMN {quote_identifier(column)} {ddl}"
                    )
                )
                columns.add(column)
    return columns


def task_payload(
    task: Mapping[str, Any],
    columns: set[str],
    *,
    allowed_columns: set[str] | None = None,
) -> dict[str, Any]:
    allowed = allowed_columns or TASK_PAYLOAD_COLUMNS
    return {key: value for key, value in task.items() if key in allowed and key in columns}


def upsert_scheduler_task(
    engine: Engine,
    payload: Mapping[str, Any],
    *,
    lookup_where: str,
    lookup_params: Mapping[str, Any],
    update_exclude: set[str] | None = None,
    forced_values: Mapping[str, Any] | None = None,
    table_name: str = SCHEDULED_TASK_TABLE,
    column_definitions: Mapping[str, str] | None = None,
    allowed_columns: set[str] | None = None,
) -> dict[str, Any]:
    columns = ensure_scheduler_columns(
        engine,
        table_name=table_name,
        column_definitions=column_definitions,
    )
    compatible = task_payload(payload, columns, allowed_columns=allowed_columns)
    for key, value in (forced_values or {}).items():
        if key in columns:
            compatible[key] = value
    if not compatible:
        raise RuntimeError("no compatible scheduler columns found")

    quoted_table = quote_identifier(table_name)
    with engine.begin() as conn:
        task_id = conn.execute(
            text(f"SELECT id FROM {quoted_table} WHERE {lookup_where} LIMIT 1"),
            dict(lookup_params),
        ).scalar()

        if task_id:
            excluded = update_exclude or set()
            update_payload = {key: value for key, value in compatible.items() if key not in excluded}
            assignments = ", ".join(f"{quote_identifier(key)} = :{key}" for key in update_payload)
            if "updated_at" in columns:
                if assignments:
                    assignments += ", "
                assignments += f"{quote_identifier('updated_at')} = NOW()"
            if assignments:
                conn.execute(
                    text(f"UPDATE {quoted_table} SET {assignments} WHERE id = :id"),
                    {**update_payload, "id": task_id},
                )
            return {"id": int(task_id), "action": "updated"}

        insert_payload = dict(compatible)
        for column in NOW_COLUMNS:
            if column in columns:
                insert_payload[column] = None
        names = ", ".join(quote_identifier(key) for key in insert_payload)
        values = ", ".join("NOW()" if key in NOW_COLUMNS else f":{key}" for key in insert_payload)
        bind_payload = {key: value for key, value in insert_payload.items() if key not in NOW_COLUMNS}
        conn.execute(text(f"INSERT INTO {quoted_table} ({names}) VALUES ({values})"), bind_payload)
        inserted_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        return {"id": int(inserted_id or 0), "action": "inserted"}


def update_scheduler_tasks(
    engine: Engine,
    values: Mapping[str, Any],
    *,
    lookup_where: str,
    lookup_params: Mapping[str, Any],
    now_columns: set[str] | None = None,
    touch_updated_at: bool = True,
    table_name: str = SCHEDULED_TASK_TABLE,
) -> int:
    columns = table_columns(engine, table_name)
    if not columns:
        raise RuntimeError(f"{table_name} does not exist")

    requested_now_columns = set(now_columns or set())
    now_update_columns = {column for column in requested_now_columns if column in columns}
    update_payload = {
        key: value
        for key, value in values.items()
        if key in columns and key not in now_update_columns
    }
    if touch_updated_at and "updated_at" in columns:
        update_payload.pop("updated_at", None)
        now_update_columns.add("updated_at")
    if not update_payload and not now_update_columns:
        return 0

    assignments = [
        f"{quote_identifier(key)} = :{key}"
        for key in update_payload
    ]
    assignments.extend(
        f"{quote_identifier(key)} = NOW()"
        for key in sorted(now_update_columns)
    )
    quoted_table = quote_identifier(table_name)
    with engine.begin() as conn:
        result = conn.execute(
            text(f"UPDATE {quoted_table} SET {', '.join(assignments)} WHERE {lookup_where}"),
            {**update_payload, **dict(lookup_params)},
        )
    return int(getattr(result, "rowcount", 0) or 0)


def set_scheduler_tasks_enabled_atomically(
    engine: Engine,
    task_types: Sequence[str],
    *,
    enabled: bool,
    expected_row_count: int | None = None,
    table_name: str = SCHEDULED_TASK_TABLE,
) -> int:
    """Set an exact task set's enabled bit in one fail-closed transaction.

    When ``expected_row_count`` is supplied, the cardinality check happens
    before the transaction context exits.  A missing or duplicate task row
    therefore raises and rolls the whole update back.
    """

    normalized = tuple(str(item).strip() for item in task_types)
    if not normalized or any(not item for item in normalized):
        raise ValueError("task_types must contain non-empty values")
    if len(set(normalized)) != len(normalized):
        raise ValueError("task_types must be unique")
    if type(enabled) is not bool:
        raise TypeError("enabled must be a strict boolean")
    if expected_row_count is not None and (
        type(expected_row_count) is not int or expected_row_count < 0
    ):
        raise ValueError("expected_row_count must be a non-negative integer")

    quoted_table = quote_identifier(table_name)
    quoted_enabled = quote_identifier("enabled")
    quoted_task_type = quote_identifier("task_type")
    bind_names = tuple(f"task_type_{index}" for index in range(len(normalized)))
    placeholders = ", ".join(f":{name}" for name in bind_names)
    params = {
        "enabled": 1 if enabled else 0,
        **dict(zip(bind_names, normalized, strict=True)),
    }
    statement = text(
        f"UPDATE {quoted_table} SET {quoted_enabled} = :enabled "
        f"WHERE {quoted_task_type} IN ({placeholders})"
    )
    with engine.begin() as connection:
        result = connection.execute(statement, params)
        changed = int(getattr(result, "rowcount", 0) or 0)
        if (
            expected_row_count is not None
            and changed != expected_row_count
        ):
            raise RuntimeError(
                "scheduler task enablement cardinality mismatch: "
                f"expected={expected_row_count}, changed={changed}"
            )
    return changed


def update_scheduler_task(
    engine: Engine,
    task_id: int,
    values: Mapping[str, Any],
    *,
    now_columns: set[str] | None = None,
    touch_updated_at: bool = True,
    table_name: str = SCHEDULED_TASK_TABLE,
) -> int:
    return update_scheduler_tasks(
        engine,
        values,
        lookup_where="id = :id",
        lookup_params={"id": task_id},
        now_columns=now_columns,
        touch_updated_at=touch_updated_at,
        table_name=table_name,
    )


def claim_scheduler_task_run(
    engine: Engine,
    task_id: int,
    *,
    table_name: str = SCHEDULED_TASK_TABLE,
) -> bool:
    quoted_table = quote_identifier(table_name)
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"UPDATE {quoted_table} "
                "SET last_run_status='running', last_run_at=NOW(), last_triggered_at=NOW(), "
                "updated_at=NOW() "
                "WHERE id=:id AND enabled=1 "
                "AND (last_run_status IS NULL OR last_run_status <> 'running')"
            ),
            {"id": int(task_id)},
        )
    return int(getattr(result, "rowcount", 0) or 0) > 0


def reset_stale_running_scheduler_tasks(
    engine: Engine,
    *,
    stale_minutes: int = 30,
    note: str = " [启动重置]",
    table_name: str = SCHEDULED_TASK_TABLE,
) -> int:
    minutes = max(1, int(stale_minutes))
    quoted_table = quote_identifier(table_name)
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"UPDATE {quoted_table} "
                "SET last_run_status='', "
                "last_run_output=CONCAT(IFNULL(last_run_output,''), :note), "
                "updated_at=NOW() "
                "WHERE last_run_status='running' "
                f"AND last_triggered_at < NOW() - INTERVAL {minutes} MINUTE"
            ),
            {"note": note},
        )
    return int(getattr(result, "rowcount", 0) or 0)
