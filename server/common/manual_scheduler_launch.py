# -*- coding: utf-8 -*-
"""Fail-closed manual launch of one already-registered scheduler task."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from sqlalchemy import text


logger = logging.getLogger(__name__)

SCHEDULER_TABLE = "st_scheduled_tasks"
_REQUIRED_COLUMNS = frozenset(
    {
        "id",
        "task_name",
        "task_type",
        "script_path",
        "script_args",
        "date_param",
        "enabled",
    }
)
_STRING_COLUMNS = frozenset(
    {"task_name", "task_type", "script_path", "script_args", "date_param"}
)
_INTEGER_TYPES = frozenset({"tinyint", "smallint", "mediumint", "int", "bigint"})
_STRING_TYPES = frozenset({"char", "varchar", "tinytext", "text", "mediumtext", "longtext"})
_MIN_STRING_CAPACITY = {
    "task_name": 1,
    "task_type": 32,
    "script_path": 64,
    "script_args": 500,
    "date_param": 10,
}


def _mapped_rows(result) -> list[dict[str, Any]]:
    return [dict(row) for row in result.mappings().all()]


def _validate_scheduler_launch_surface_on_connection(connection) -> None:
    table_rows = _mapped_rows(
        connection.execute(
            text(
                "SELECT ENGINE AS engine, TABLE_COLLATION AS table_collation "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
            ),
            {"table_name": SCHEDULER_TABLE},
        )
    )
    if len(table_rows) != 1:
        raise RuntimeError("scheduler launch table is not prepared")
    table_row = table_rows[0]
    if str(table_row.get("engine") or "").lower() != "innodb":
        raise RuntimeError("scheduler launch table must use InnoDB")
    if not str(table_row.get("table_collation") or "").lower().startswith("utf8mb4_"):
        raise RuntimeError("scheduler launch table must use utf8mb4")

    params = {f"column_{index}": name for index, name in enumerate(sorted(_REQUIRED_COLUMNS))}
    placeholders = ", ".join(f":{name}" for name in params)
    column_rows = _mapped_rows(
        connection.execute(
            text(
                "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, "
                "COLUMN_TYPE AS column_type, IS_NULLABLE AS is_nullable, "
                "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
                "CHARACTER_SET_NAME AS character_set_name, EXTRA AS extra "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
                f"AND COLUMN_NAME IN ({placeholders})"
            ),
            {"table_name": SCHEDULER_TABLE, **params},
        )
    )
    by_name = {str(row.get("column_name") or ""): row for row in column_rows}
    if len(by_name) != len(column_rows) or set(by_name) != _REQUIRED_COLUMNS:
        missing = sorted(_REQUIRED_COLUMNS - set(by_name))
        raise RuntimeError(f"scheduler launch columns are not prepared: missing={missing}")

    for name in ("id", "enabled"):
        row = by_name[name]
        if str(row.get("data_type") or "").lower() not in _INTEGER_TYPES:
            raise RuntimeError(f"scheduler launch integer type drift: {name}")
        if str(row.get("is_nullable") or "").upper() != "NO":
            raise RuntimeError(f"scheduler launch nullability drift: {name}")
    if "auto_increment" not in str(by_name["id"].get("extra") or "").lower():
        raise RuntimeError("scheduler launch id must be auto_increment")

    for name in _STRING_COLUMNS:
        row = by_name[name]
        data_type = str(row.get("data_type") or "").lower()
        if data_type not in _STRING_TYPES:
            raise RuntimeError(f"scheduler launch string type drift: {name}")
        if str(row.get("character_set_name") or "").lower() != "utf8mb4":
            raise RuntimeError(f"scheduler launch string charset drift: {name}")
        capacity = row.get("character_maximum_length")
        minimum = _MIN_STRING_CAPACITY[name]
        if capacity is not None and int(capacity) < minimum:
            raise RuntimeError(f"scheduler launch string capacity drift: {name}")

    index_rows = _mapped_rows(
        connection.execute(
            text(
                "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, "
                "SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, "
                "SUB_PART AS sub_part, INDEX_TYPE AS index_type "
                "FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
                "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
            ),
            {"table_name": SCHEDULER_TABLE},
        )
    )
    indexes: dict[str, list[dict[str, Any]]] = {}
    for row in index_rows:
        indexes.setdefault(str(row.get("index_name") or ""), []).append(row)
    has_unique_id = any(
        all(int(part.get("non_unique") or 0) == 0 for part in parts)
        and tuple(
            str(part.get("column_name") or "")
            for part in sorted(parts, key=lambda item: int(item.get("seq_in_index") or 0))
        )
        == ("id",)
        and all(part.get("sub_part") is None for part in parts)
        and all(str(part.get("index_type") or "").upper() == "BTREE" for part in parts)
        for parts in indexes.values()
    )
    if not has_unique_id:
        raise RuntimeError("scheduler launch id unique index is missing")


def validate_scheduler_launch_surface(engine, *, connection=None) -> None:
    """Read-only validation of only the columns required by the launcher."""

    if connection is not None:
        _validate_scheduler_launch_surface_on_connection(connection)
        return
    with engine.connect() as bound_connection:
        _validate_scheduler_launch_surface_on_connection(bound_connection)


def _failure(task_type: str, status: str, message: str) -> dict[str, Any]:
    return {
        "accepted": False,
        "status": status,
        "task_type": task_type,
        "job_id": "",
        "error": message,
    }


def launch_registered_scheduler_task(
    engine,
    *,
    task_type: str,
    expected_script_path: str,
    script_args: str,
    root: Path,
) -> dict[str, Any]:
    """Launch one exact registration without mutating its persisted row."""

    normalized_type = str(task_type or "").strip()
    normalized_path = str(expected_script_path or "").replace("\\", "/").strip()
    if not normalized_type or not normalized_path or normalized_path.startswith(("/", ".")):
        raise ValueError("manual scheduler task contract is invalid")
    if not isinstance(script_args, str) or len(script_args) > 500:
        raise ValueError("manual scheduler arguments exceed the registered surface")

    try:
        validate_scheduler_launch_surface(engine)
        with engine.connect() as connection:
            rows = _mapped_rows(
                connection.execute(
                    text(
                        "SELECT * FROM st_scheduled_tasks "
                        "WHERE task_type=:task_type ORDER BY id LIMIT 2"
                    ),
                    {"task_type": normalized_type},
                )
            )
    except Exception as exc:  # fail closed without returning database details
        logger.exception("scheduler registry read failed for %s: %s", normalized_type, type(exc).__name__)
        return _failure(normalized_type, "scheduler_registry_unavailable", "生产任务注册表不可用，已拒绝执行")

    if not rows:
        return _failure(normalized_type, "task_registration_missing", "生产任务尚未注册，已拒绝执行")
    if len(rows) != 1:
        return _failure(normalized_type, "task_registration_ambiguous", "生产任务注册不唯一，已拒绝执行")

    row = dict(rows[0])
    registered_path = str(row.get("script_path") or "").replace("\\", "/").strip()
    if registered_path != normalized_path:
        return _failure(normalized_type, "task_contract_mismatch", "生产任务脚本合同不匹配，已拒绝执行")
    row["script_path"] = normalized_path
    row["script_args"] = script_args
    row["date_param"] = ""

    try:
        from server.api.scheduler_runtime import launch_scheduler_task

        result = dict(launch_scheduler_task(row, root=root, engine=engine))
    except Exception as exc:  # launcher owns claim/audit cleanup; do not leak raw details
        logger.exception("scheduler launch failed for %s: %s", normalized_type, type(exc).__name__)
        return _failure(normalized_type, "scheduler_launch_failed", "生产任务提交失败，请查看调度审计")
    result.setdefault("accepted", False)
    result.setdefault("job_id", "")
    result.setdefault("task_type", normalized_type)
    return result


__all__ = [
    "SCHEDULER_TABLE",
    "launch_registered_scheduler_task",
    "validate_scheduler_launch_surface",
]
