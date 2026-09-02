#!/usr/bin/env python3
"""Fail-closed primitives used by the Layer-4 production maintenance shell.

This command deliberately does not stop services, create backups, or decide
whether a production change is approved.  It supplies small JSON contracts for
the remote shell: exact migration planning/verification, durable task-fence
inspection, scheduler-writer drainage, and the cross-process maintenance lock.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.mysql_lock import mysql_named_lock
from server.common.mysql_version_policy import (
    MYSQL_84_ISOLATED_ACCEPTANCE,
    is_oracle_mysql_distribution,
    isolated_acceptance_version,
)
from server.common.scheduler_authority import LAYER4_WRITER_TASK_TYPES
from server.common.scheduler_tasks import read_fresh_scheduler_writers
from server.common.trading_v3_maintenance import (
    TRADING_V3_MAINTENANCE_LOCK_NAME,
)
from server.db.migrations_v3 import (
    HORIZON_CANDIDATE_LEDGER_RDS_DDL,
    HORIZON_PROTOCOL_V2_RDS_DDL,
    MIGRATIONS,
    SHADOW_INTELLIGENCE_RDS_DDL,
    run_v3_migrations,
)
from server.trading_v3.horizon_candidate_ledger_schema import (
    HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
)
from server.trading_v3.horizon_protocol_v2_schema import (
    HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
)
from server.trading_v3.shadow_intelligence_schema import (
    SHADOW_INTELLIGENCE_MIGRATION_VERSION,
)
from tools.env_config import create_tool_engine, load_project_env
from tools.trading_v3_fourth_layer_readiness import (
    collect_migration_readiness,
)


class MaintenanceBlocked(RuntimeError):
    """A verified production precondition did not hold."""


def _checksum(statements: Sequence[str]) -> str:
    return hashlib.sha256(
        "\n".join(item.strip() for item in statements).encode("utf-8")
    ).hexdigest()


TARGET_MIGRATIONS: tuple[dict[str, Any], ...] = (
    {
        "version": SHADOW_INTELLIGENCE_MIGRATION_VERSION,
        "checksum": _checksum(SHADOW_INTELLIGENCE_RDS_DDL),
        "statement_count": len(SHADOW_INTELLIGENCE_RDS_DDL),
    },
    {
        "version": HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
        "checksum": _checksum(HORIZON_PROTOCOL_V2_RDS_DDL),
        "statement_count": len(HORIZON_PROTOCOL_V2_RDS_DDL),
    },
    {
        "version": HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
        "checksum": _checksum(HORIZON_CANDIDATE_LEDGER_RDS_DDL),
        "statement_count": len(HORIZON_CANDIDATE_LEDGER_RDS_DDL),
    },
)
TARGET_BY_VERSION = {
    str(item["version"]): item for item in TARGET_MIGRATIONS
}
DECLARED_BY_VERSION = {
    str(item["version"]): {
        "checksum": _checksum(tuple(item["statements"])),
        "statement_count": len(tuple(item["statements"])),
    }
    for item in MIGRATIONS
}


def _identity(engine: Engine) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(text(
            "SELECT VERSION() AS version, @@version_comment AS version_comment, "
            "DATABASE() AS database_name, CURRENT_USER() AS effective_user, "
            "@@server_uuid AS server_uuid"
        )).mappings().one()
    result = dict(row)
    version = str(result.get("version") or "")
    version_comment = str(result.get("version_comment") or "")
    expected_uuid = os.environ.get(
        "PROBIGA_EXPECTED_MYSQL_SERVER_UUID", ""
    ).strip().lower()
    if str(getattr(engine.dialect, "name", "")).lower() != "mysql":
        raise MaintenanceBlocked("LAYER4_DATABASE_NOT_ORACLE_MYSQL")
    if (
        isolated_acceptance_version(version) != MYSQL_84_ISOLATED_ACCEPTANCE
        or not is_oracle_mysql_distribution(version, version_comment)
    ):
        raise MaintenanceBlocked("LAYER4_DATABASE_VERSION_NOT_EXACT_MYSQL_8_4_11")
    if str(result.get("database_name") or "") != "probiga":
        raise MaintenanceBlocked("LAYER4_DATABASE_SCHEMA_NOT_PROBIGA")
    observed_uuid = str(result.get("server_uuid") or "").strip().lower()
    if expected_uuid and observed_uuid != expected_uuid:
        raise MaintenanceBlocked("LAYER4_DATABASE_SERVER_UUID_MISMATCH")
    return {
        "version": version,
        "version_comment": version_comment,
        "database_name": "probiga",
        "current_user": str(result.get("effective_user") or ""),
        "server_uuid": observed_uuid,
        "server_uuid_pinned": bool(expected_uuid),
    }


def _table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as connection:
        return bool(connection.execute(text(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
        ), {"table_name": table_name}).scalar())


def collect_migration_plan(
    engine: Engine,
    *,
    allow_resume: bool,
) -> dict[str, Any]:
    identity = _identity(engine)
    dry_run = run_v3_migrations(engine, dry_run=True)
    statuses = {item.version: item.status for item in dry_run}
    if set(statuses) != set(DECLARED_BY_VERSION):
        raise MaintenanceBlocked("LAYER4_DECLARED_MIGRATION_INVENTORY_DRIFT")

    with engine.connect() as connection:
        ledger = {
            str(row["version"]): dict(row)
            for row in connection.execute(text(
                "SELECT version, checksum, statement_count, applied_at "
                "FROM schema_migration_v3 ORDER BY version"
            )).mappings()
        }
        progress: dict[str, dict[str, Any]] = {}
        if _table_exists(engine, "schema_migration_v3_progress"):
            progress = {
                str(row["version"]): dict(row)
                for row in connection.execute(text(
                    "SELECT version, checksum, statement_count, "
                    "completed_statement_count, updated_at "
                    "FROM schema_migration_v3_progress "
                    "WHERE version IN (:shadow, :protocol, :candidate) "
                    "ORDER BY version"
                ), {
                    "shadow": SHADOW_INTELLIGENCE_MIGRATION_VERSION,
                    "protocol": HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
                    "candidate": HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
                }).mappings()
            }

    unknown = sorted(set(ledger) - set(DECLARED_BY_VERSION))
    if unknown:
        raise MaintenanceBlocked(
            "LAYER4_UNKNOWN_MIGRATION_LEDGER_ROWS:" + ",".join(unknown)
        )
    for version, row in ledger.items():
        expected = DECLARED_BY_VERSION[version]
        if (
            str(row.get("checksum") or "") != expected["checksum"]
            or int(row.get("statement_count") or 0)
            != expected["statement_count"]
        ):
            raise MaintenanceBlocked(
                f"LAYER4_MIGRATION_LEDGER_CONTRACT_DRIFT:{version}"
            )

    non_target_pending = sorted(
        version
        for version, status in statuses.items()
        if version not in TARGET_BY_VERSION and status != "exists"
    )
    if non_target_pending:
        raise MaintenanceBlocked(
            "LAYER4_UNEXPECTED_PENDING_MIGRATIONS:"
            + ",".join(non_target_pending)
        )

    target_statuses = {
        version: statuses[version] for version in TARGET_BY_VERSION
    }
    if any(
        status not in {"exists", "would_apply"}
        for status in target_statuses.values()
    ):
        raise MaintenanceBlocked("LAYER4_TARGET_MIGRATION_STATUS_INVALID")
    mixed = len(set(target_statuses.values())) > 1
    if mixed and not allow_resume:
        raise MaintenanceBlocked("LAYER4_FORWARD_RECOVERY_ACK_REQUIRED")

    progress_summary: dict[str, dict[str, Any]] = {}
    for version, expected in TARGET_BY_VERSION.items():
        row = dict(progress.get(version) or {})
        status = target_statuses[version]
        if row:
            completed = int(row.get("completed_statement_count") or 0)
            valid = bool(
                str(row.get("checksum") or "") == expected["checksum"]
                and int(row.get("statement_count") or 0)
                == expected["statement_count"]
                and 0 <= completed <= expected["statement_count"]
            )
            if not valid:
                raise MaintenanceBlocked(
                    f"LAYER4_MIGRATION_PROGRESS_CONTRACT_DRIFT:{version}"
                )
            if status == "exists" and completed != expected["statement_count"]:
                raise MaintenanceBlocked(
                    f"LAYER4_APPLIED_MIGRATION_PROGRESS_INCOMPLETE:{version}"
                )
            if status == "would_apply" and not allow_resume:
                raise MaintenanceBlocked("LAYER4_FORWARD_RECOVERY_ACK_REQUIRED")
        elif status == "exists":
            raise MaintenanceBlocked(
                f"LAYER4_APPLIED_MIGRATION_PROGRESS_MISSING:{version}"
            )
        progress_summary[version] = {
            "present": bool(row),
            "completed_statement_count": (
                int(row.get("completed_statement_count") or 0) if row else 0
            ),
            "statement_count": expected["statement_count"],
        }

    return {
        "status": "ok",
        "identity": identity,
        "allow_resume": bool(allow_resume),
        "apply_required": any(
            status == "would_apply" for status in target_statuses.values()
        ),
        "target_migrations": [
            {
                **TARGET_BY_VERSION[version],
                "status": target_statuses[version],
                "progress": progress_summary[version],
            }
            for version in TARGET_BY_VERSION
        ],
        "declared_migration_count": len(statuses),
        "ledger_row_count": len(ledger),
    }


def collect_verified_migrations(engine: Engine) -> dict[str, Any]:
    identity = _identity(engine)
    readiness = collect_migration_readiness(engine)
    if readiness.get("ready") is not True:
        raise MaintenanceBlocked("LAYER4_MIGRATION_SCHEMA_READINESS_BLOCKED")
    return {
        "status": "ok",
        "identity": identity,
        "migration_readiness": readiness,
    }


def collect_task_state(
    engine: Engine,
    *,
    expected_enabled: bool,
) -> dict[str, Any]:
    _identity(engine)
    with engine.connect() as connection:
        rows = [dict(row) for row in connection.execute(text(
            "SELECT id, task_type, enabled, script_path, script_args, "
            "date_param, cron_time, interval_minutes FROM st_scheduled_tasks "
            "WHERE task_type IN (:counterfactual, :continuous) ORDER BY id"
        ), {
            "counterfactual": LAYER4_WRITER_TASK_TYPES[0],
            "continuous": LAYER4_WRITER_TASK_TYPES[1],
        }).mappings()]
    counts = {
        task_type: sum(
            str(row.get("task_type") or "") == task_type for row in rows
        )
        for task_type in LAYER4_WRITER_TASK_TYPES
    }
    expected_bit = 1 if expected_enabled else 0
    ready = bool(
        len(rows) == len(LAYER4_WRITER_TASK_TYPES)
        and all(count == 1 for count in counts.values())
        and all(int(row.get("enabled") or 0) == expected_bit for row in rows)
    )
    if not ready:
        raise MaintenanceBlocked("LAYER4_TASK_STATE_OR_CARDINALITY_INVALID")
    return {
        "status": "ok",
        "expected_enabled": bool(expected_enabled),
        "task_count": len(rows),
        "task_ids": [int(row["id"]) for row in rows],
        "task_types": [str(row["task_type"]) for row in rows],
    }


def wait_for_writer_quiescence(
    engine: Engine,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    _identity(engine)
    if not 0 <= timeout_seconds <= 600:
        raise ValueError("timeout_seconds must be between 0 and 600")
    if not 0.1 <= poll_seconds <= 60:
        raise ValueError("poll_seconds must be between 0.1 and 60")
    deadline = time.monotonic() + timeout_seconds
    while True:
        live = tuple(read_fresh_scheduler_writers(engine))
        if not live:
            return {
                "status": "ok",
                "ready": True,
                "live_writer_count": 0,
                "live_writers": [],
            }
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise MaintenanceBlocked(
                "LAYER4_FRESH_SCHEDULER_WRITERS_REMAIN:"
                + ",".join(str(row.get("instance_id") or "") for row in live)
            )
        time.sleep(min(poll_seconds, remaining))


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    parent = path.parent.resolve(strict=True)
    if path.parent.resolve() != parent or path.exists():
        raise RuntimeError("maintenance lock ready file must be new")
    temporary = parent / f".{path.name}.{os.getpid()}.tmp"
    if temporary.exists():
        raise RuntimeError("maintenance lock temporary file already exists")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
        os.chmod(temporary, 0o600)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _process_is_alive(process_id: int) -> bool:
    """Probe a process without ever delivering a signal on Windows.

    POSIX defines ``kill(pid, 0)`` as a permission/existence check.  Python's
    Windows implementation does not provide that contract for arbitrary
    numeric signals and may terminate the target process.  The maintenance
    holder is also exercised by Windows CI, so use a read-only process handle
    there instead of risking a signal to the parent pytest/shell process.
    """

    if process_id <= 1:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = (
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        )
        open_process.restype = wintypes.HANDLE
        get_exit_code = kernel32.GetExitCodeProcess
        get_exit_code.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.DWORD),
        )
        get_exit_code.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = (wintypes.HANDLE,)
        close_handle.restype = wintypes.BOOL

        handle = open_process(
            process_query_limited_information,
            False,
            process_id,
        )
        if not handle:
            return False
        try:
            exit_code = wintypes.DWORD()
            return bool(get_exit_code(handle, ctypes.byref(exit_code))) and (
                exit_code.value == still_active
            )
        finally:
            close_handle(handle)

    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def hold_maintenance_lock(
    engine: Engine,
    *,
    ready_file: Path,
    release_file: Path,
    timeout_seconds: int,
    max_hold_seconds: int,
    parent_pid: int,
) -> dict[str, Any]:
    identity = _identity(engine)
    if release_file.exists():
        raise MaintenanceBlocked("LAYER4_MAINTENANCE_RELEASE_FILE_PREEXISTS")
    if not 0 <= timeout_seconds <= 300:
        raise ValueError("timeout_seconds must be between 0 and 300")
    if not 30 <= max_hold_seconds <= 7200:
        raise ValueError("max_hold_seconds must be between 30 and 7200")
    if parent_pid <= 1:
        raise ValueError("parent_pid must identify the maintenance shell")

    started = time.monotonic()
    with mysql_named_lock(
        engine,
        TRADING_V3_MAINTENANCE_LOCK_NAME,
        timeout_seconds=timeout_seconds,
    ) as connection:
        connection_id = int(
            connection.execute(text("SELECT CONNECTION_ID() ")).scalar_one()
        )
        _atomic_json(ready_file, {
            "status": "held",
            "lock_name": TRADING_V3_MAINTENANCE_LOCK_NAME,
            "connection_id": connection_id,
            "holder_pid": os.getpid(),
            "database_server_uuid": identity["server_uuid"],
        })
        while not release_file.exists():
            if time.monotonic() - started >= max_hold_seconds:
                raise MaintenanceBlocked("LAYER4_MAINTENANCE_LOCK_MAX_HOLD_EXPIRED")
            if not _process_is_alive(parent_pid):
                raise MaintenanceBlocked("LAYER4_MAINTENANCE_PARENT_EXITED")
            time.sleep(0.25)
    return {
        "status": "released",
        "lock_name": TRADING_V3_MAINTENANCE_LOCK_NAME,
        "connection_id": connection_id,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("migration-plan")
    plan.add_argument("--allow-resume", action="store_true")
    subparsers.add_parser("verify-migrations")

    tasks = subparsers.add_parser("task-state")
    tasks.add_argument(
        "--expected",
        choices=("fenced", "enabled"),
        required=True,
    )

    writers = subparsers.add_parser("wait-writers")
    writers.add_argument("--timeout-seconds", type=float, default=150.0)
    writers.add_argument("--poll-seconds", type=float, default=5.0)

    hold = subparsers.add_parser("hold-lock")
    hold.add_argument("--ready-file", type=Path, required=True)
    hold.add_argument("--release-file", type=Path, required=True)
    hold.add_argument("--timeout-seconds", type=int, default=30)
    hold.add_argument("--max-hold-seconds", type=int, default=3600)
    hold.add_argument("--parent-pid", type=int, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    load_project_env()
    engine = create_tool_engine()
    try:
        if args.command == "migration-plan":
            payload = collect_migration_plan(
                engine,
                allow_resume=bool(args.allow_resume),
            )
        elif args.command == "verify-migrations":
            payload = collect_verified_migrations(engine)
        elif args.command == "task-state":
            payload = collect_task_state(
                engine,
                expected_enabled=args.expected == "enabled",
            )
        elif args.command == "wait-writers":
            payload = wait_for_writer_quiescence(
                engine,
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
            )
        elif args.command == "hold-lock":
            payload = hold_maintenance_lock(
                engine,
                ready_file=args.ready_file,
                release_file=args.release_file,
                timeout_seconds=args.timeout_seconds,
                max_hold_seconds=args.max_hold_seconds,
                parent_pid=args.parent_pid,
            )
        else:  # pragma: no cover - argparse owns the command inventory
            raise AssertionError(args.command)
    except MaintenanceBlocked as exc:
        print(json.dumps({
            "status": "blocked",
            "reason_code": str(exc),
            "order_authority": False,
        }, ensure_ascii=False, sort_keys=True))
        return 2
    except TimeoutError as exc:
        print(json.dumps({
            "status": "blocked",
            "reason_code": "LAYER4_MAINTENANCE_LOCK_BUSY",
            "detail": str(exc)[:300],
            "order_authority": False,
        }, ensure_ascii=False, sort_keys=True))
        return 3
    finally:
        engine.dispose()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
