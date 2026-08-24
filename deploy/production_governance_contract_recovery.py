"""Restore or verify only the stable governance scheduler-task contract.

This helper is extracted from the authenticated requested release by the
production deploy engine, then executed with the guarded release's sealed
Python environment.  It intentionally never overwrites scheduler runtime or
audit columns such as ``last_run_*``, ``etl_sync_at`` or ``updated_at``.
"""
from __future__ import annotations

import argparse
from datetime import time, timedelta
import json
import re
import sys
from typing import Any, TextIO

from sqlalchemy import text

from server.common.batch_db import quote_identifier
from server.common.scheduler_tasks import TASK_PAYLOAD_COLUMNS
from tools.env_config import create_tool_engine, load_project_env
from tools.strategy_governance_task_contract import TASK


_IDENTITY_COLUMNS = {"task_type", "script_path"}
_CLOCK_RE = re.compile(
    r"^(?P<hour>[01][0-9]|2[0-3]):(?P<minute>[0-5][0-9])"
    r"(?::(?P<second>[0-5][0-9]))?$"
)


def _read_snapshot(stream: TextIO) -> dict[str, Any]:
    raw = json.load(stream)
    if (
        not isinstance(raw, dict)
        or raw.get("format_version") != 1
        or raw.get("task_type") != TASK["task_type"]
        or raw.get("script_path") != TASK["script_path"]
        or not isinstance(raw.get("rows"), list)
        or len(raw["rows"]) != 1
        or not isinstance(raw["rows"][0], dict)
    ):
        raise RuntimeError("invalid sealed governance contract snapshot")
    return dict(raw["rows"][0])


def _json_normalized(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            ensure_ascii=False,
            default=str,
            sort_keys=True,
        )
    )


def _canonical_clock(value: Any) -> str:
    if isinstance(value, time):
        if value.second or value.microsecond:
            raise RuntimeError("governance cron_time must have minute precision")
        return value.strftime("%H:%M")
    if isinstance(value, timedelta):
        total_seconds = int(value.total_seconds())
        if value.microseconds or not 0 <= total_seconds < 24 * 60 * 60:
            raise RuntimeError("governance cron_time is outside one day")
        if total_seconds % 60:
            raise RuntimeError("governance cron_time must have minute precision")
        hour, remainder = divmod(total_seconds, 60 * 60)
        minute = remainder // 60
        return f"{hour:02d}:{minute:02d}"
    if not isinstance(value, str):
        raise RuntimeError("governance cron_time must be a clock string")
    match = _CLOCK_RE.fullmatch(value)
    if match is None or int(match.group("second") or 0) != 0:
        raise RuntimeError("governance cron_time must have minute precision")
    return f"{match.group('hour')}:{match.group('minute')}"


def _canonical_value(key: str, value: Any) -> Any:
    if key == "cron_time":
        return _canonical_clock(value)
    contract_value = TASK[key]
    if type(value) is not type(contract_value):
        raise RuntimeError(
            f"governance {key} must be {type(contract_value).__name__}"
        )
    return value


def _canonical_projection(
    row: dict[str, Any], payload_columns: set[str]
) -> dict[str, Any]:
    return {
        key: _canonical_value(key, row[key]) for key in sorted(payload_columns)
    }


def _require_innodb(connection) -> None:
    engine_name = connection.execute(
        text(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME='st_scheduled_tasks'"
        )
    ).scalar_one_or_none()
    if str(engine_name or "").upper() != "INNODB":
        raise RuntimeError("st_scheduled_tasks must use InnoDB")


def _require_schema_columns(
    connection, required_columns: set[str]
) -> set[str]:
    rows = connection.execute(
        text(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME='st_scheduled_tasks'"
        )
    ).fetchall()
    columns = {str(row[0]) for row in rows}
    missing_schema = sorted(required_columns - columns)
    if missing_schema:
        raise RuntimeError(
            "governance scheduler schema misses contract columns: "
            + ", ".join(missing_schema)
        )
    return columns


def _matching_rows(
    connection, expected: dict[str, Any], *, lock: bool
) -> list[dict[str, Any]]:
    suffix = " FOR UPDATE" if lock else ""
    return [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT * FROM st_scheduled_tasks "
                "WHERE id=:sealed_id OR task_type=:task_type "
                "OR script_path=:script_path "
                f"ORDER BY id{suffix}"
            ),
            {
                "sealed_id": expected["id"],
                "task_type": TASK["task_type"],
                "script_path": TASK["script_path"],
            },
        )
        .mappings()
        .all()
    ]


def _require_live_identity(
    rows: list[dict[str, Any]], expected: dict[str, Any]
) -> dict[str, Any]:
    if len(rows) != 1:
        raise RuntimeError(
            "live governance scheduler identity is not unique: "
            f"{len(rows)} matching rows"
        )
    current = rows[0]
    if type(current.get("id")) is not int or current["id"] != expected["id"]:
        raise RuntimeError("live governance scheduler task id differs")
    for key in sorted(_IDENTITY_COLUMNS):
        if current.get(key) != expected[key]:
            raise RuntimeError(f"live governance scheduler {key} differs")
    return current


def _verify_projection(
    current: dict[str, Any],
    expected_projection: dict[str, Any],
    payload_columns: set[str],
) -> None:
    if _canonical_projection(current, payload_columns) != expected_projection:
        raise RuntimeError(
            "live governance scheduler contract differs from sealed NEW"
        )


def reconcile_contract(
    engine, expected: dict[str, Any], *, action: str
) -> dict[str, Any]:
    if action not in {"restore", "verify"}:
        raise ValueError("unsupported governance contract action")
    if getattr(getattr(engine, "dialect", None), "name", None) != "mysql":
        raise RuntimeError("governance contract recovery requires MySQL")

    payload_columns = set(TASK_PAYLOAD_COLUMNS)
    # The sealed NEW row belongs to the interrupted guarded release, not to the
    # incoming recovery engine.  Importing this contract from the guarded
    # PYTHONPATH is therefore deliberate and also makes future recovery engines
    # generic across older transaction targets.
    if payload_columns != set(TASK):
        raise RuntimeError(
            "guarded governance payload columns differ from the task contract"
        )
    required_columns = payload_columns | {"id"}
    missing_snapshot = sorted(required_columns - set(expected))
    if missing_snapshot:
        raise RuntimeError(
            "sealed governance snapshot misses contract columns: "
            + ", ".join(missing_snapshot)
        )
    if type(expected.get("id")) is not int or expected["id"] < 1:
        raise RuntimeError("sealed governance snapshot has an invalid task id")
    if (
        expected.get("task_type") != TASK["task_type"]
        or expected.get("script_path") != TASK["script_path"]
    ):
        raise RuntimeError("sealed governance task identity differs")

    expected_projection = _canonical_projection(expected, payload_columns)
    if expected_projection != _canonical_projection(TASK, payload_columns):
        raise RuntimeError("sealed NEW governance task contract is unexpected")

    if action == "verify":
        with engine.connect() as connection:
            _require_innodb(connection)
            _require_schema_columns(connection, required_columns)
            current = _require_live_identity(
                _matching_rows(connection, expected, lock=False), expected
            )
            _verify_projection(current, expected_projection, payload_columns)
        return {
            "action": action,
            "changed": False,
            "id": expected["id"],
            "verified": True,
        }

    changed = False
    with engine.begin() as connection:
        _require_innodb(connection)
        columns = _require_schema_columns(connection, required_columns)
        current = _require_live_identity(
            _matching_rows(connection, expected, lock=True), expected
        )
        current_projection = _canonical_projection(current, payload_columns)

        if current_projection != expected_projection:
            changed_columns = sorted(
                key
                for key in payload_columns - _IDENTITY_COLUMNS
                if current_projection[key] != expected_projection[key]
            )
            if not changed_columns:
                raise RuntimeError(
                    "governance identity differs and cannot be rewritten"
                )
            volatile_before = _json_normalized(
                {
                    key: value
                    for key, value in current.items()
                    if key not in payload_columns
                }
            )
            assignments_list = [
                f"{quote_identifier(key)}=:{key}" for key in changed_columns
            ]
            # MySQL can declare updated_at with ON UPDATE.  Explicitly assigning
            # the live column to itself prevents a configuration repair from
            # advancing or rewinding that audit value.  No value from the sealed
            # historical snapshot is ever replayed into an audit column.
            if "updated_at" in columns:
                quoted_updated_at = quote_identifier("updated_at")
                assignments_list.append(
                    f"{quoted_updated_at}={quoted_updated_at}"
                )
            assignments = ", ".join(assignments_list)
            result = connection.execute(
                text(
                    f"UPDATE st_scheduled_tasks SET {assignments} "
                    "WHERE id=:restore_id AND task_type=:identity_task_type "
                    "AND script_path=:identity_script_path"
                ),
                {
                    **{key: expected[key] for key in changed_columns},
                    "restore_id": expected["id"],
                    "identity_task_type": expected["task_type"],
                    "identity_script_path": expected["script_path"],
                },
            )
            if result.rowcount not in {0, 1}:
                raise RuntimeError("governance contract restore changed many rows")
            changed = True
            current = _require_live_identity(
                _matching_rows(connection, expected, lock=True), expected
            )
            _verify_projection(current, expected_projection, payload_columns)
            volatile_after = _json_normalized(
                {
                    key: value
                    for key, value in current.items()
                    if key not in payload_columns
                }
            )
            if volatile_after != volatile_before:
                raise RuntimeError(
                    "governance runtime or audit fields changed during restore"
                )
        else:
            _verify_projection(current, expected_projection, payload_columns)

    return {
        "action": action,
        "changed": changed,
        "id": expected["id"],
        "verified": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="恢复或验证封存的策略治理调度配置投影"
    )
    parser.add_argument("action", choices=("restore", "verify"))
    args = parser.parse_args(argv)
    expected = _read_snapshot(sys.stdin)
    load_project_env()
    engine = create_tool_engine()
    try:
        result = reconcile_contract(engine, expected, action=args.action)
    finally:
        engine.dispose()
    print(
        json.dumps(
            {"status": "ok", "governance_contract": result},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
