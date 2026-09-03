"""Restore or verify sealed scheduler-task contracts during recovery.

This helper is extracted from the authenticated requested release by the
production deploy engine, then executed with the guarded release's sealed
Python environment.  Forward recovery intentionally never overwrites scheduler
runtime/audit columns.  Rollback recovery instead restores exactly the columns
present in the pre-migration snapshot, while ignoring additive live columns.
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
_ROW_IDENTITY_COLUMNS = frozenset({"id", *_IDENTITY_COLUMNS})
_MAX_SNAPSHOT_BYTES = 1024 * 1024
_SAFE_COLUMN_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")

# Keep rollback identities and the scheduler column surface inside this
# authenticated helper.  The helper is materialized from the incoming release,
# so rollback never depends on changing (or executing snapshot logic from) the
# immutable guarded release.
_GOVERNANCE_IDENTITY = (
    "strategy_governance_daily",
    "tools/run_strategy_governance_daily.py",
)
_QMT_ANNOUNCEMENT_IDENTITY = (
    "qmt_announcement_pit",
    "tools/sync_qmt_announcement_pit.py",
)
_QMT_OPERATION_IDENTITIES = (
    (
        "qmt_local_gap_repair_execute",
        "tools/backfill_guojin_qmt_local_history.py",
    ),
    (
        "qmt_nightly_reconciliation",
        "tools/nightly_guojin_qmt_reconciliation.py",
    ),
    (
        "qmt_local_history_2024",
        "tools/run_guojin_qmt_full_market_history.py",
    ),
    (
        "qmt_reference_incremental",
        "tools/sync_guojin_qmt_reference_data.py",
    ),
    (
        "qmt_gap_repair_plan",
        "tools/repair_guojin_qmt_gaps.py",
    ),
)
_QMT_FULL_HISTORY_IDENTITY = (
    "qmt_local_history_2024",
    "tools/run_guojin_qmt_full_market_history.py",
)
_QMT_FULL_HISTORY_LEGACY_IDENTITY = (
    "qmt_local_history_2026",
    "tools/run_guojin_qmt_full_market_history.py",
)
_ROLLBACK_IDENTITY_ALIASES = {
    _QMT_FULL_HISTORY_IDENTITY: (
        _QMT_FULL_HISTORY_IDENTITY,
        _QMT_FULL_HISTORY_LEGACY_IDENTITY,
    ),
}
_SCHEDULER_SNAPSHOT_COLUMNS = frozenset(
    {
        "id",
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
        "last_triggered_at",
        "last_run_output",
        "last_run_duration",
        "last_run_status",
        "last_run_at",
        "etl_sync_at",
        "updated_at",
        "created_at",
    }
)
_ADDITIVE_ROLLBACK_COLUMN = "created_at"
_LEGACY_SCHEDULER_SNAPSHOT_COLUMNS = (
    _SCHEDULER_SNAPSHOT_COLUMNS - {_ADDITIVE_ROLLBACK_COLUMN}
)
# Once the sealed OLD runtime has been restored and writers are live again,
# scheduler execution/audit fields legitimately advance.  Cleanup retries still
# bind every task identity and configuration field plus the immutable row-birth
# timestamp; they never relax schema, primary-key or snapshot-seal validation.
_ROLLBACK_STABLE_COLUMNS = frozenset(
    {"id", *TASK_PAYLOAD_COLUMNS, _ADDITIVE_ROLLBACK_COLUMN}
)
_CLOCK_RE = re.compile(
    r"^(?P<hour>[01][0-9]|2[0-3]):(?P<minute>[0-5][0-9])"
    r"(?::(?P<second>[0-5][0-9]))?$"
)


def _static_failure_code(error: Exception) -> str:
    """Return a bounded diagnostic code without exposing SQL or row values."""
    message = str(error)
    if message.startswith("invalid sealed governance contract snapshot"):
        return "snapshot-envelope"
    if message.startswith("invalid sealed rollback scheduler snapshot"):
        return "snapshot-envelope"
    if message.startswith("sealed governance task identity differs"):
        return "sealed-identity"
    if message.startswith("sealed rollback scheduler identity differs"):
        return "sealed-identity"
    if message.startswith("sealed NEW governance task contract is unexpected"):
        return "projection"
    if message.startswith("sealed governance snapshot") or message.startswith(
        "guarded governance payload columns"
    ) or message.startswith("sealed rollback scheduler snapshot columns"):
        return "contract-shape"
    if message.startswith("governance cron_time") or (
        message.startswith("governance ") and " must be " in message
    ):
        return "contract-shape"
    if message.startswith("governance contract recovery requires MySQL") or (
        message.startswith("st_scheduled_tasks must use InnoDB")
    ) or message.startswith("governance scheduler schema misses") or (
        message.startswith("rollback scheduler schema")
    ):
        return "engine-schema"
    if message.startswith("live governance scheduler identity is not unique"):
        return "live-count"
    if message.startswith("live rollback scheduler identity is not unique"):
        return "live-count"
    if message.startswith("live governance scheduler task id differs"):
        return "live-id"
    if message.startswith("live rollback scheduler task id differs"):
        return "live-id"
    if message.startswith("live governance scheduler task_type differs") or (
        message.startswith("live governance scheduler script_path differs")
    ) or message.startswith("governance identity differs"):
        return "live-identity"
    if message.startswith("live rollback scheduler task identity differs"):
        return "live-identity"
    if message.startswith("live governance scheduler contract differs"):
        return "projection"
    if message.startswith("live rollback scheduler projection differs"):
        return "projection"
    if message.startswith("governance contract restore changed many rows"):
        return "update-rowcount"
    if message.startswith("rollback scheduler restore changed unexpected rows"):
        return "update-rowcount"
    if message.startswith("governance runtime or audit fields changed"):
        return "volatile-drift"
    if message.startswith("unsupported governance contract action"):
        return "contract-shape"
    return "database-runtime"


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeError("invalid sealed rollback scheduler snapshot")
        result[key] = value
    return result


def _read_json_object(stream: TextIO) -> dict[str, Any]:
    raw_text = stream.read(_MAX_SNAPSHOT_BYTES + 1)
    if not isinstance(raw_text, str) or len(raw_text.encode("utf-8")) > (
        _MAX_SNAPSHOT_BYTES
    ):
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    try:
        raw = json.loads(
            raw_text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON value")
            ),
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("invalid sealed rollback scheduler snapshot") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    return raw


def _read_snapshot(stream: TextIO) -> dict[str, Any]:
    raw = _read_json_object(stream)
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


def _identity_payload(identity: tuple[str, str]) -> dict[str, str]:
    return {"task_type": identity[0], "script_path": identity[1]}


def _validate_rollback_rows(
    rows: Any,
    identities: tuple[tuple[str, str], ...],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not isinstance(rows, list) or len(rows) > len(identities):
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    allowed = set(identities)
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    column_set: frozenset[str] | None = None
    seen_ids: set[int] = set()
    for raw_row in rows:
        if not isinstance(raw_row, dict) or not raw_row:
            raise RuntimeError("invalid sealed rollback scheduler snapshot")
        keys = frozenset(str(key) for key in raw_row)
        if keys != frozenset(raw_row) or not _ROW_IDENTITY_COLUMNS <= keys:
            raise RuntimeError("invalid sealed rollback scheduler snapshot")
        if any(not _SAFE_COLUMN_RE.fullmatch(key) for key in keys) or not (
            keys <= _SCHEDULER_SNAPSHOT_COLUMNS
        ):
            raise RuntimeError(
                "sealed rollback scheduler snapshot columns are unsupported"
            )
        if keys not in {
            _SCHEDULER_SNAPSHOT_COLUMNS,
            _LEGACY_SCHEDULER_SNAPSHOT_COLUMNS,
        }:
            raise RuntimeError(
                "sealed rollback scheduler snapshot columns are unsupported"
            )
        if column_set is None:
            column_set = keys
        elif keys != column_set:
            raise RuntimeError(
                "sealed rollback scheduler snapshot columns are inconsistent"
            )
        identity = (
            raw_row.get("task_type"),
            raw_row.get("script_path"),
        )
        if identity not in allowed:
            raise RuntimeError("sealed rollback scheduler identity differs")
        row_id = raw_row.get("id")
        if type(row_id) is not int or row_id < 1 or row_id in seen_ids:
            raise RuntimeError("invalid sealed rollback scheduler snapshot")
        if identity in by_identity:
            raise RuntimeError("invalid sealed rollback scheduler snapshot")
        for value in raw_row.values():
            if isinstance(value, (dict, list)):
                raise RuntimeError("invalid sealed rollback scheduler snapshot")
        seen_ids.add(row_id)
        by_identity[identity] = dict(raw_row)
    return by_identity


def _read_rollback_snapshot(
    stream: TextIO, *, snapshot_kind: str
) -> dict[str, Any]:
    raw = _read_json_object(stream)
    if snapshot_kind == "rollback-governance":
        if (
            set(raw) != {"format_version", "task_type", "script_path", "rows"}
            or raw.get("format_version") != 1
            or (raw.get("task_type"), raw.get("script_path"))
            != _GOVERNANCE_IDENTITY
        ):
            raise RuntimeError("invalid sealed rollback scheduler snapshot")
        identities = (_GOVERNANCE_IDENTITY,)
        rows = _validate_rollback_rows(raw.get("rows"), identities)
        return {
            "snapshot_kind": snapshot_kind,
            "identities": identities,
            "rows": rows,
        }
    if snapshot_kind != "rollback-qmt":
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    operation_types = sorted(item[0] for item in _QMT_OPERATION_IDENTITIES)
    operation_paths = sorted(item[1] for item in _QMT_OPERATION_IDENTITIES)
    operations = raw.get("operations")
    if (
        set(raw)
        != {"schema", "task_type", "script_path", "rows", "operations"}
        or raw.get("schema")
        != "probiga.qmt-announcement-task-snapshot.v1"
        or (raw.get("task_type"), raw.get("script_path"))
        != _QMT_ANNOUNCEMENT_IDENTITY
        or not isinstance(operations, dict)
        or set(operations) != {"task_types", "script_paths", "rows"}
        or operations.get("task_types") != operation_types
        or operations.get("script_paths") != operation_paths
    ):
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    announcement_rows = _validate_rollback_rows(
        raw.get("rows"), (_QMT_ANNOUNCEMENT_IDENTITY,)
    )
    raw_operation_rows = _validate_rollback_rows(
        operations.get("rows"),
        (*_QMT_OPERATION_IDENTITIES, _QMT_FULL_HISTORY_LEGACY_IDENTITY),
    )
    if len(raw_operation_rows) > len(_QMT_OPERATION_IDENTITIES):
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    operation_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row_identity, row in raw_operation_rows.items():
        logical_identity = (
            _QMT_FULL_HISTORY_IDENTITY
            if row_identity == _QMT_FULL_HISTORY_LEGACY_IDENTITY
            else row_identity
        )
        if logical_identity in operation_rows:
            # The historical and current names are one logical task.  A sealed
            # snapshot containing both is ambiguous and must fail closed.
            raise RuntimeError("invalid sealed rollback scheduler snapshot")
        operation_rows[logical_identity] = row
    combined = {**announcement_rows, **operation_rows}
    if len(combined) != len(announcement_rows) + len(operation_rows):
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    combined_ids = [row["id"] for row in combined.values()]
    if len(combined_ids) != len(set(combined_ids)):
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    nonempty_column_sets = {
        frozenset(row) for row in combined.values()
    }
    if len(nonempty_column_sets) > 1:
        raise RuntimeError(
            "sealed rollback scheduler snapshot columns are inconsistent"
        )
    return {
        "snapshot_kind": snapshot_kind,
        "identities": (_QMT_ANNOUNCEMENT_IDENTITY, *_QMT_OPERATION_IDENTITIES),
        "rows": combined,
    }


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


def _require_scheduler_primary_key(connection) -> None:
    rows = connection.execute(
        text(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME='st_scheduled_tasks' AND INDEX_NAME='PRIMARY' "
            "ORDER BY SEQ_IN_INDEX"
        )
    ).fetchall()
    if [str(row[0]) for row in rows] != ["id"]:
        raise RuntimeError("rollback scheduler schema primary key differs")


def _rollback_snapshot_columns(payload: dict[str, Any]) -> frozenset[str]:
    return frozenset(
        key for row in payload["rows"].values() for key in row
    )


def _require_rollback_schema(
    connection,
    *,
    snapshot_columns: frozenset[str],
) -> set[str]:
    rows = connection.execute(
        text(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME='st_scheduled_tasks' ORDER BY ORDINAL_POSITION"
        )
    ).fetchall()
    metadata = {
        str(row[0]): (
            str(row[1] or "").lower(),
            str(row[2] or "").upper(),
            row[3],
            str(row[4] or "").lower(),
        )
        for row in rows
    }
    live_columns = set(metadata)
    if live_columns != set(_SCHEDULER_SNAPSHOT_COLUMNS):
        raise RuntimeError("rollback scheduler schema column surface differs")
    if snapshot_columns and snapshot_columns not in {
        _SCHEDULER_SNAPSHOT_COLUMNS,
        _LEGACY_SCHEDULER_SNAPSHOT_COLUMNS,
    }:
        raise RuntimeError(
            "sealed rollback scheduler snapshot columns are unsupported"
        )
    if metadata.get(_ADDITIVE_ROLLBACK_COLUMN) != (
        "datetime",
        "YES",
        None,
        "",
    ):
        raise RuntimeError(
            "rollback scheduler schema additive column contract differs"
        )
    _require_scheduler_primary_key(connection)
    return live_columns


def _rollback_projection_columns(payload: dict[str, Any]) -> tuple[str, ...]:
    snapshot_columns = set(_rollback_snapshot_columns(payload))
    snapshot_columns.update(_ROW_IDENTITY_COLUMNS)
    if _ADDITIVE_ROLLBACK_COLUMN not in snapshot_columns:
        snapshot_columns.add(_ADDITIVE_ROLLBACK_COLUMN)
    return tuple(sorted(snapshot_columns))


def _rollback_matching_rows(
    connection,
    *,
    identity: tuple[str, str],
    expected: dict[str, Any] | None,
    projection_columns: tuple[str, ...],
    lock: bool,
) -> list[dict[str, Any]]:
    selected = ", ".join(quote_identifier(key) for key in projection_columns)
    identity_aliases = _ROLLBACK_IDENTITY_ALIASES.get(identity, (identity,))
    predicate = "task_type=:task_type OR script_path=:script_path"
    params: dict[str, Any] = _identity_payload(identity)
    if len(identity_aliases) == 2:
        predicate = (
            "task_type=:task_type OR task_type=:task_type_alias "
            "OR script_path=:script_path"
        )
        params["task_type_alias"] = identity_aliases[1][0]
    if expected is not None:
        predicate = "id=:sealed_id OR " + predicate
        params["sealed_id"] = expected["id"]
    lock_suffix = " FOR UPDATE" if lock else ""
    return [
        dict(row)
        for row in connection.execute(
            text(
                f"SELECT {selected} FROM st_scheduled_tasks "
                f"WHERE {predicate} ORDER BY id{lock_suffix}"
            ),
            params,
        )
        .mappings()
        .all()
    ]


def _require_rollback_live_identity(
    rows: list[dict[str, Any]],
    *,
    identity: tuple[str, str],
    expected: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if len(rows) > 1:
        raise RuntimeError(
            "live rollback scheduler identity is not unique: "
            f"{len(rows)} matching rows"
        )
    if not rows:
        return None
    current = rows[0]
    current_id = current.get("id")
    if type(current_id) is not int or current_id < 1:
        raise RuntimeError("live rollback scheduler task id differs")
    if expected is not None and current_id != expected["id"]:
        raise RuntimeError("live rollback scheduler task id differs")
    live_identity = (current.get("task_type"), current.get("script_path"))
    if live_identity not in _ROLLBACK_IDENTITY_ALIASES.get(
        identity, (identity,)
    ):
        raise RuntimeError("live rollback scheduler task identity differs")
    return current


def _rollback_projection(
    row: dict[str, Any],
    expected: dict[str, Any],
    *,
    stable_only: bool = False,
) -> dict[str, Any]:
    keys = set(expected)
    if stable_only:
        keys &= _ROLLBACK_STABLE_COLUMNS
    return _json_normalized({key: row.get(key) for key in sorted(keys)})


def _require_additive_rollback_value(
    current: dict[str, Any] | None,
    *,
    snapshot_columns: frozenset[str],
) -> None:
    if (
        current is not None
        and _ADDITIVE_ROLLBACK_COLUMN not in snapshot_columns
        and current.get(_ADDITIVE_ROLLBACK_COLUMN) is not None
    ):
        raise RuntimeError(
            "rollback scheduler schema additive column value differs"
        )


def _verify_rollback_state(
    connection,
    payload: dict[str, Any],
    *,
    projection_columns: tuple[str, ...],
    snapshot_columns: frozenset[str],
    lock: bool,
    stable_only: bool = False,
) -> None:
    for identity in payload["identities"]:
        expected = payload["rows"].get(identity)
        current = _require_rollback_live_identity(
            _rollback_matching_rows(
                connection,
                identity=identity,
                expected=expected,
                projection_columns=projection_columns,
                lock=lock,
            ),
            identity=identity,
            expected=expected,
        )
        if expected is None:
            if current is not None:
                raise RuntimeError(
                    "live rollback scheduler projection differs from sealed OLD"
                )
            continue
        _require_additive_rollback_value(
            current, snapshot_columns=snapshot_columns
        )
        expected_projection = _rollback_projection(
            expected, expected, stable_only=stable_only
        )
        if current is None or _rollback_projection(
            current, expected, stable_only=stable_only
        ) != expected_projection:
            raise RuntimeError(
                "live rollback scheduler projection differs from sealed OLD"
            )


def reconcile_rollback_snapshot(
    engine,
    payload: dict[str, Any],
    *,
    action: str,
) -> dict[str, Any]:
    """Restore OLD exactly or verify its exact/stable sealed projection."""

    if action not in {"restore", "verify", "verify-stable"}:
        raise ValueError("unsupported governance contract action")
    if getattr(getattr(engine, "dialect", None), "name", None) != "mysql":
        raise RuntimeError("governance contract recovery requires MySQL")
    if payload.get("snapshot_kind") not in {
        "rollback-governance",
        "rollback-qmt",
    }:
        raise RuntimeError("invalid sealed rollback scheduler snapshot")
    projection_columns = _rollback_projection_columns(payload)
    snapshot_columns = _rollback_snapshot_columns(payload)

    if action in {"verify", "verify-stable"}:
        with engine.connect() as connection:
            _require_innodb(connection)
            _require_rollback_schema(
                connection, snapshot_columns=snapshot_columns
            )
            _verify_rollback_state(
                connection,
                payload,
                projection_columns=projection_columns,
                snapshot_columns=snapshot_columns,
                lock=False,
                stable_only=action == "verify-stable",
            )
        return {
            "action": action,
            "changed": False,
            "snapshot_kind": payload["snapshot_kind"],
            "row_count": len(payload["rows"]),
            "verified": True,
        }

    changed_count = 0
    with engine.begin() as connection:
        _require_innodb(connection)
        live_columns = _require_rollback_schema(
            connection, snapshot_columns=snapshot_columns
        )
        current_by_identity: dict[
            tuple[str, str], dict[str, Any] | None
        ] = {}
        for identity in payload["identities"]:
            expected = payload["rows"].get(identity)
            current_by_identity[identity] = _require_rollback_live_identity(
                _rollback_matching_rows(
                    connection,
                    identity=identity,
                    expected=expected,
                    projection_columns=projection_columns,
                    lock=True,
                ),
                identity=identity,
                expected=expected,
            )
            if expected is not None:
                _require_additive_rollback_value(
                    current_by_identity[identity],
                    snapshot_columns=snapshot_columns,
                )

        for identity in payload["identities"]:
            expected = payload["rows"].get(identity)
            current = current_by_identity[identity]
            if expected is None:
                if current is None:
                    continue
                identity_params = _identity_payload(
                    (current["task_type"], current["script_path"])
                )
                result = connection.execute(
                    text(
                        "DELETE FROM st_scheduled_tasks "
                        "WHERE id=:restore_id AND task_type=:task_type "
                        "AND script_path=:script_path"
                    ),
                    {**identity_params, "restore_id": current["id"]},
                )
                if result.rowcount != 1:
                    raise RuntimeError(
                        "rollback scheduler restore changed unexpected rows"
                    )
                changed_count += 1
                continue

            if current is None:
                names = ", ".join(
                    quote_identifier(key) for key in sorted(expected)
                )
                values = ", ".join(f":{key}" for key in sorted(expected))
                result = connection.execute(
                    text(
                        "INSERT INTO st_scheduled_tasks "
                        f"({names}) VALUES ({values})"
                    ),
                    expected,
                )
                if result.rowcount != 1:
                    raise RuntimeError(
                        "rollback scheduler restore changed unexpected rows"
                    )
                changed_count += 1
                continue

            current_projection = _rollback_projection(current, expected)
            expected_projection = _json_normalized(expected)
            changed_columns = [
                key
                for key in sorted(expected)
                if key not in {"id", "script_path"}
                and current_projection[key] != expected_projection[key]
            ]
            if not changed_columns:
                if current_projection != expected_projection:
                    raise RuntimeError(
                        "live rollback scheduler task identity differs"
                    )
                continue
            assignments = [
                f"{quote_identifier(key)}=:restore_value_{key}"
                for key in changed_columns
            ]
            preserve_params: dict[str, Any] = {}
            if "updated_at" in live_columns and "updated_at" not in expected:
                quoted_updated_at = quote_identifier("updated_at")
                assignments.append(f"{quoted_updated_at}={quoted_updated_at}")
            elif "updated_at" in expected and "updated_at" not in changed_columns:
                assignments.append("`updated_at`=:preserve_updated_at")
                preserve_params["preserve_updated_at"] = expected["updated_at"]
            result = connection.execute(
                text(
                    "UPDATE st_scheduled_tasks SET "
                    + ", ".join(assignments)
                    + " WHERE id=:restore_id AND task_type=:task_type "
                    "AND script_path=:script_path"
                ),
                {
                    **_identity_payload(
                        (current["task_type"], current["script_path"])
                    ),
                    **{
                        f"restore_value_{key}": expected[key]
                        for key in changed_columns
                    },
                    **preserve_params,
                    "restore_id": expected["id"],
                },
            )
            if result.rowcount not in {0, 1}:
                raise RuntimeError(
                    "rollback scheduler restore changed unexpected rows"
                )
            changed_count += 1

        _verify_rollback_state(
            connection,
            payload,
            projection_columns=projection_columns,
            snapshot_columns=snapshot_columns,
            lock=True,
        )

    return {
        "action": action,
        "changed": bool(changed_count),
        "changed_row_count": changed_count,
        "snapshot_kind": payload["snapshot_kind"],
        "row_count": len(payload["rows"]),
        "verified": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="恢复或验证封存的策略治理调度配置投影"
    )
    parser.add_argument(
        "action", choices=("restore", "verify", "verify-stable")
    )
    parser.add_argument(
        "snapshot_kind",
        nargs="?",
        choices=(
            "forward-governance",
            "rollback-governance",
            "rollback-qmt",
        ),
        default="forward-governance",
    )
    args = parser.parse_args(argv)
    try:
        if args.snapshot_kind == "forward-governance":
            expected = _read_snapshot(sys.stdin)
        else:
            expected = _read_rollback_snapshot(
                sys.stdin, snapshot_kind=args.snapshot_kind
            )
        load_project_env()
        engine = create_tool_engine()
        try:
            if args.snapshot_kind == "forward-governance":
                result = reconcile_contract(engine, expected, action=args.action)
            else:
                result = reconcile_rollback_snapshot(
                    engine, expected, action=args.action
                )
        finally:
            engine.dispose()
    except Exception as error:  # noqa: BLE001 - emit only a bounded static code
        print(
            "probiga_governance_contract_failure="
            f"{_static_failure_code(error)}",
            file=sys.stderr,
        )
        return 2
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
