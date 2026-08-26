"""Fail-closed validation for the production standalone scheduler heartbeat."""
from __future__ import annotations

from datetime import datetime
import re
from socket import gethostname
from typing import Any

from sqlalchemy import text

from tools.qmt_host_ownership_contract import (
    WINDOWS_QMT_EDGE_TASKS,
    WINDOWS_QMT_EDGE_TASKS_BY_TYPE,
    WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES,
)


BUILD_SHA_RE = re.compile(r"[0-9a-f]{40}")
LINUX_STANDALONE_ROLE = "linux_standalone"
QMT_WINDOWS_EDGE_ROLE = "qmt_windows_edge"
QMT_WINDOWS_EDGE_TASK_TYPES = tuple(
    str(task["task_type"]) for task in WINDOWS_QMT_EDGE_TASKS
)
QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES = tuple(
    WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES
)
# These jobs are scheduled daily, but the scheduler deliberately skips them on
# non-trading days.  Ninety-six hours proves recent execution across a normal
# weekend without allowing an old task row to masquerade as a live executor.
QMT_WINDOWS_EDGE_SUCCESS_MAX_AGE_SECONDS = 96 * 60 * 60
DEFAULT_SCHEDULER_POLL_SECONDS = 60


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _classify_current_heartbeats(
    rows: list[dict[str, Any]],
    *,
    expected_poll_seconds: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Classify current rows against a fixed deployment freshness window."""

    fresh_rows: list[dict[str, Any]] = []
    future_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for row in rows:
        age = _integer(row.get("heartbeat_age_seconds"))
        poll_seconds = _integer(row.get("poll_seconds"))
        if age is None:
            errors.append("heartbeat_age_invalid")
            continue
        if age < 0:
            future_rows.append(row)
            continue
        if poll_seconds is None or poll_seconds < 15:
            if age <= 2 * expected_poll_seconds:
                errors.append("fresh_heartbeat_poll_invalid")
            continue
        # Match the dispatch authority's per-row lease semantics first.  A
        # row with a drifted, very large poll interval must remain visible as
        # a conflicting live executor instead of extending its lease while
        # disappearing from health.
        if age <= 2 * poll_seconds:
            fresh_rows.append(row)
            if poll_seconds != expected_poll_seconds:
                errors.append("poll_seconds_mismatch")
    if future_rows:
        errors.append("future_heartbeat_present")
    return fresh_rows, future_rows, errors


def check_linux_standalone_scheduler_heartbeat(
    connection,
    *,
    expected_build_sha: str,
    expected_pid: int,
    expected_host: str = "",
    expected_poll_seconds: int = DEFAULT_SCHEDULER_POLL_SECONDS,
) -> tuple[bool, dict[str, Any]]:
    """Require one fresh heartbeat bound to this host, PID and release.

    Historical rows are retained for diagnosis and do not make a healthy
    current executor ambiguous.  A future-dated row is always an error, while
    exactly one row whose age is in ``0..2 * poll_seconds`` must match the
    current systemd process identity.
    """

    build_sha = str(expected_build_sha or "").strip().lower()
    host_name = str(expected_host or gethostname()).strip()
    pid = _integer(expected_pid)
    poll = _integer(expected_poll_seconds)
    input_errors: list[str] = []
    if not BUILD_SHA_RE.fullmatch(build_sha) or build_sha == "0" * 40:
        input_errors.append("expected_build_sha_invalid")
    if not host_name or len(host_name) > 128:
        input_errors.append("expected_host_invalid")
    if pid is None or pid <= 0:
        input_errors.append("expected_pid_invalid")
    if poll is None or poll < 15:
        input_errors.append("expected_poll_seconds_invalid")
    if input_errors:
        return False, {
            "executor_role": LINUX_STANDALONE_ROLE,
            "role_row_count": 0,
            "fresh_row_count": 0,
            "future_row_count": 0,
            "expected_host": host_name or None,
            "expected_pid": pid,
            "expected_build_sha": build_sha or None,
            "expected_poll_seconds": poll,
            "current": None,
            "errors": input_errors,
        }

    try:
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT instance_id, mode, host_name, pid, build_sha, "
                    "executor_role, started_at, heartbeat_at, "
                    "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) "
                    "AS heartbeat_age_seconds, poll_seconds, "
                    "max_concurrent_tasks "
                    "FROM st_scheduler_runtime "
                    "WHERE executor_role=:executor_role "
                    "ORDER BY heartbeat_at DESC, instance_id ASC"
                ),
                {"executor_role": LINUX_STANDALONE_ROLE},
            ).mappings()
        ]
    except Exception:
        return False, {
            "executor_role": LINUX_STANDALONE_ROLE,
            "role_row_count": 0,
            "fresh_row_count": 0,
            "future_row_count": 0,
            "expected_host": host_name,
            "expected_pid": pid,
            "expected_build_sha": build_sha,
            "current": None,
            "errors": ["scheduler_runtime_query_failed"],
        }

    assert poll is not None
    fresh_rows, future_rows, errors = _classify_current_heartbeats(
        rows,
        expected_poll_seconds=poll,
    )
    if len(fresh_rows) != 1:
        errors.append("fresh_heartbeat_not_unique")

    current = fresh_rows[0] if len(fresh_rows) == 1 else None
    current_detail: dict[str, Any] | None = None
    if current is not None:
        current_pid = _integer(current.get("pid"))
        current_poll = _integer(current.get("poll_seconds"))
        current_concurrency = _integer(current.get("max_concurrent_tasks"))
        current_age = _integer(current.get("heartbeat_age_seconds"))
        current_detail = {
            "instance_id": str(current.get("instance_id") or ""),
            "mode": str(current.get("mode") or ""),
            "host_name": str(current.get("host_name") or ""),
            "pid": current_pid,
            "build_sha": str(current.get("build_sha") or ""),
            "executor_role": str(current.get("executor_role") or ""),
            "heartbeat_age_seconds": current_age,
            "poll_seconds": current_poll,
            "max_concurrent_tasks": current_concurrency,
        }
        expected_instance_id = f"{host_name}-{pid}"
        if current_detail["instance_id"] != expected_instance_id:
            errors.append("instance_id_mismatch")
        if current_detail["mode"] != "standalone":
            errors.append("mode_mismatch")
        if current_detail["host_name"] != host_name:
            errors.append("host_mismatch")
        if current_pid != pid:
            errors.append("pid_mismatch")
        if current_detail["build_sha"] != build_sha:
            errors.append("build_sha_mismatch")
        if current_detail["executor_role"] != LINUX_STANDALONE_ROLE:
            errors.append("executor_role_mismatch")
        if current_poll != poll:
            errors.append("poll_seconds_mismatch")
        if current_concurrency is None or current_concurrency < 1:
            errors.append("max_concurrent_tasks_invalid")
        if current.get("started_at") is None:
            errors.append("started_at_missing")
        if current.get("heartbeat_at") is None:
            errors.append("heartbeat_at_missing")

    return not errors, {
        "executor_role": LINUX_STANDALONE_ROLE,
        "role_row_count": len(rows),
        "fresh_row_count": len(fresh_rows),
        "future_row_count": len(future_rows),
        "expected_host": host_name,
        "expected_pid": pid,
        "expected_build_sha": build_sha,
        "expected_poll_seconds": poll,
        "current": current_detail,
        "errors": errors,
    }


def check_linux_standalone_active_release(
    connection,
    *,
    expected_build_sha: str,
    expected_poll_seconds: int = DEFAULT_SCHEDULER_POLL_SECONDS,
) -> tuple[bool, dict[str, Any]]:
    """Prove the one fresh Linux executor for a build across hosts.

    Unlike the local systemd check above, a Windows edge does not know the
    Linux PID in advance.  It therefore derives the identity only after the
    shared heartbeat lease proves there is exactly one fresh Linux executor,
    then binds activation evidence to that exact instance and start time.
    """

    build_sha = str(expected_build_sha or "").strip().lower()
    poll = _integer(expected_poll_seconds)
    errors: list[str] = []
    base = {
        "executor_role": LINUX_STANDALONE_ROLE,
        "expected_build_sha": build_sha or None,
        "expected_poll_seconds": poll,
        "role_row_count": 0,
        "fresh_row_count": 0,
        "future_row_count": 0,
        "current": None,
        "errors": errors,
    }
    if not BUILD_SHA_RE.fullmatch(build_sha) or build_sha == "0" * 40:
        errors.append("expected_build_sha_invalid")
    if poll is None or poll < 15:
        errors.append("expected_poll_seconds_invalid")
    if errors:
        return False, base
    try:
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT instance_id, mode, host_name, pid, build_sha, "
                    "executor_role, started_at, heartbeat_at, "
                    "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) "
                    "AS heartbeat_age_seconds, poll_seconds, "
                    "max_concurrent_tasks FROM st_scheduler_runtime "
                    "WHERE executor_role=:executor_role "
                    "ORDER BY heartbeat_at DESC, instance_id ASC"
                ),
                {"executor_role": LINUX_STANDALONE_ROLE},
            ).mappings()
        ]
    except Exception:
        return False, {**base, "errors": ["scheduler_runtime_query_failed"]}
    assert poll is not None
    fresh_rows, future_rows, classification_errors = (
        _classify_current_heartbeats(
            rows,
            expected_poll_seconds=poll,
        )
    )
    errors.extend(classification_errors)
    if len(fresh_rows) != 1:
        errors.append("fresh_heartbeat_not_unique")
    current = fresh_rows[0] if len(fresh_rows) == 1 else None
    current_detail: dict[str, Any] | None = None
    if current is not None:
        host_name = str(current.get("host_name") or "").strip()
        pid = _integer(current.get("pid"))
        instance_id = str(current.get("instance_id") or "")
        current_poll = _integer(current.get("poll_seconds"))
        concurrency = _integer(current.get("max_concurrent_tasks"))
        started_at = str(current.get("started_at") or "")[:19].replace(" ", "T")
        current_detail = {
            "instance_id": instance_id,
            "mode": str(current.get("mode") or ""),
            "host_name": host_name,
            "pid": pid,
            "build_sha": str(current.get("build_sha") or "").lower(),
            "executor_role": str(current.get("executor_role") or ""),
            "started_at": started_at,
            "heartbeat_age_seconds": _integer(
                current.get("heartbeat_age_seconds")
            ),
            "poll_seconds": current_poll,
            "max_concurrent_tasks": concurrency,
        }
        if not host_name or len(host_name) > 128:
            errors.append("host_invalid")
        if pid is None or pid <= 0:
            errors.append("pid_invalid")
        if instance_id != f"{host_name}-{pid}":
            errors.append("instance_id_mismatch")
        if current_detail["mode"] != "standalone":
            errors.append("mode_mismatch")
        if current_detail["build_sha"] != build_sha:
            errors.append("build_sha_mismatch")
        if current_detail["executor_role"] != LINUX_STANDALONE_ROLE:
            errors.append("executor_role_mismatch")
        if current_poll != poll:
            errors.append("poll_seconds_mismatch")
        if concurrency is None or concurrency < 1:
            errors.append("max_concurrent_tasks_invalid")
        try:
            if (
                not started_at
                or datetime.fromisoformat(started_at).replace(microsecond=0)
                .isoformat(timespec="seconds")
                != started_at
            ):
                raise ValueError
        except ValueError:
            errors.append("started_at_invalid")
        if current.get("heartbeat_at") is None:
            errors.append("heartbeat_at_missing")
    return not errors, {
        **base,
        "role_row_count": len(rows),
        "fresh_row_count": len(fresh_rows),
        "future_row_count": len(future_rows),
        "current": current_detail,
        "errors": errors,
    }


def check_qmt_windows_edge_identity(
    connection,
    *,
    expected_build_sha: str,
    expected_poll_seconds: int = DEFAULT_SCHEDULER_POLL_SECONDS,
) -> tuple[bool, dict[str, Any]]:
    """Prove one current Windows edge identity without claiming job success."""

    build_sha = str(expected_build_sha or "").strip().lower()
    poll = _integer(expected_poll_seconds)
    errors: list[str] = []
    if not BUILD_SHA_RE.fullmatch(build_sha) or build_sha == "0" * 40:
        errors.append("expected_build_sha_invalid")
    if poll is None or poll < 15:
        errors.append("expected_poll_seconds_invalid")
    base = {
        "executor_role": QMT_WINDOWS_EDGE_ROLE,
        "expected_build_sha": build_sha or None,
        "expected_poll_seconds": poll,
        "role_row_count": 0,
        "fresh_row_count": 0,
        "future_row_count": 0,
        "current": None,
        "errors": errors,
    }
    if errors:
        return False, base
    try:
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT instance_id, mode, host_name, pid, build_sha, "
                    "executor_role, started_at, heartbeat_at, "
                    "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) "
                    "AS heartbeat_age_seconds, poll_seconds, "
                    "max_concurrent_tasks FROM st_scheduler_runtime "
                    "WHERE executor_role=:executor_role "
                    "ORDER BY heartbeat_at DESC, instance_id ASC"
                ),
                {"executor_role": QMT_WINDOWS_EDGE_ROLE},
            ).mappings()
        ]
    except Exception:
        return False, {**base, "errors": ["scheduler_runtime_query_failed"]}
    assert poll is not None
    fresh_rows, future_rows, classification_errors = (
        _classify_current_heartbeats(
            rows,
            expected_poll_seconds=poll,
        )
    )
    errors.extend(classification_errors)
    if len(fresh_rows) != 1:
        errors.append("fresh_heartbeat_not_unique")
    current = fresh_rows[0] if len(fresh_rows) == 1 else None
    current_detail: dict[str, Any] | None = None
    if current is not None:
        host_name = str(current.get("host_name") or "").strip()
        pid = _integer(current.get("pid"))
        instance_id = str(current.get("instance_id") or "")
        current_poll = _integer(current.get("poll_seconds"))
        concurrency = _integer(current.get("max_concurrent_tasks"))
        current_detail = {
            "instance_id": instance_id,
            "mode": str(current.get("mode") or ""),
            "host_name": host_name,
            "pid": pid,
            "build_sha": str(current.get("build_sha") or "").lower(),
            "executor_role": str(current.get("executor_role") or ""),
            "heartbeat_age_seconds": _integer(
                current.get("heartbeat_age_seconds")
            ),
            "poll_seconds": current_poll,
            "max_concurrent_tasks": concurrency,
        }
        if not host_name or len(host_name) > 128:
            errors.append("host_invalid")
        if pid is None or pid <= 0:
            errors.append("pid_invalid")
        if instance_id != f"{host_name}-{pid}":
            errors.append("instance_id_mismatch")
        if current_detail["mode"] != "standalone":
            errors.append("mode_mismatch")
        if current_detail["build_sha"] != build_sha:
            errors.append("build_sha_mismatch")
        if current_detail["executor_role"] != QMT_WINDOWS_EDGE_ROLE:
            errors.append("executor_role_mismatch")
        if current_poll != poll:
            errors.append("poll_seconds_mismatch")
        if concurrency is None or concurrency < 1:
            errors.append("max_concurrent_tasks_invalid")
        if current.get("started_at") is None:
            errors.append("started_at_missing")
        if current.get("heartbeat_at") is None:
            errors.append("heartbeat_at_missing")
    return not errors, {
        **base,
        "role_row_count": len(rows),
        "fresh_row_count": len(fresh_rows),
        "future_row_count": len(future_rows),
        "current": current_detail,
        "errors": errors,
    }


def check_qmt_windows_edge_release_receipt(
    connection,
    *,
    expected_build_sha: str,
    expected_poll_seconds: int = DEFAULT_SCHEDULER_POLL_SECONDS,
) -> tuple[bool, dict[str, Any]]:
    """Bind one live edge instance to its immutable reference capture."""

    identity_ok, identity = check_qmt_windows_edge_identity(
        connection,
        expected_build_sha=expected_build_sha,
        expected_poll_seconds=expected_poll_seconds,
    )
    detail: dict[str, Any] = {
        "status": "UNAVAILABLE",
        "strategy_eligible": False,
        "expected_build_sha": str(expected_build_sha or "").strip().lower(),
        "expected_poll_seconds": _integer(expected_poll_seconds),
        "identity": identity,
        "receipt_count": 0,
        "receipt": None,
        "immutable_reference_verified": False,
        "errors": list(identity.get("errors") or ()),
    }
    current = identity.get("current") if identity_ok else None
    if not isinstance(current, dict):
        return False, detail
    try:
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT run_uid, task_id, task_type, status, run_at, "
                    "finished_at, exit_code, output, host_name, "
                    "scheduler_instance_id, build_sha, trigger_source "
                    "FROM st_scheduled_task_history WHERE "
                    "task_type='qmt_edge_release_bootstrap' "
                    "AND trigger_source='release_bootstrap' "
                    "AND status='success' AND exit_code=0 "
                    "AND build_sha=:build_sha "
                    "AND scheduler_instance_id=:instance_id "
                    "AND host_name=:host_name ORDER BY finished_at DESC, id DESC"
                ),
                {
                    "build_sha": str(expected_build_sha).lower(),
                    "instance_id": current["instance_id"],
                    "host_name": current["host_name"],
                },
            ).mappings()
        ]
    except Exception:
        detail["errors"].append("release_receipt_query_failed")
        return False, detail
    detail["receipt_count"] = len(rows)
    if len(rows) != 1:
        detail["errors"].append("release_receipt_not_unique")
        return False, detail
    row = rows[0]
    try:
        from server.common.qmt_edge_release_receipt import (
            validate_qmt_edge_release_receipt,
        )

        receipt = validate_qmt_edge_release_receipt(
            connection,
            str(row.get("output") or ""),
            expected_build_sha=str(expected_build_sha),
            expected_host_name=str(current["host_name"]),
            expected_scheduler_instance_id=str(current["instance_id"]),
        )
    except Exception:
        detail["errors"].append("release_receipt_invalid")
        return False, detail
    detail.update({
        "status": "AVAILABLE",
        "strategy_eligible": True,
        "receipt": receipt,
        "immutable_reference_verified": True,
        "errors": [],
    })
    return True, detail


def _qmt_task_contract_errors(
    rows: list[dict[str, Any]],
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    expected = {
        task_type: WINDOWS_QMT_EDGE_TASKS_BY_TYPE[task_type]
        for task_type in QMT_WINDOWS_EDGE_TASK_TYPES
    }
    observed: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for row in rows:
        task_type = str(row.get("task_type") or "")
        if task_type not in expected:
            errors.append("unexpected_task_identity")
            continue
        if task_type in observed:
            errors.append(f"task_identity_not_unique:{task_type}")
            continue
        observed[task_type] = row
    for task_type, contract in expected.items():
        row = observed.get(task_type)
        if row is None:
            errors.append(f"task_missing:{task_type}")
            continue
        exact_fields = (
            "task_name",
            "task_type",
            "group_name",
            "script_path",
            "script_args",
            "cron_time",
            "interval_minutes",
            "enabled",
            "sort_order",
            "date_param",
            "description",
        )
        if any(row.get(field) != contract[field] for field in exact_fields):
            errors.append(f"task_contract_drift:{task_type}")
        task_id = _integer(row.get("id"))
        if task_id is None or task_id <= 0:
            errors.append(f"task_id_invalid:{task_type}")
        last_status = str(row.get("last_run_status") or "").lower()
        if (
            task_type in QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES
            and last_status not in {"success", "running"}
        ):
            errors.append(f"task_last_status_unhealthy:{task_type}")
    return errors, observed


def check_qmt_windows_edge_executor(
    connection,
    *,
    expected_build_sha: str,
    expected_poll_seconds: int = DEFAULT_SCHEDULER_POLL_SECONDS,
    success_max_age_seconds: int = QMT_WINDOWS_EDGE_SUCCESS_MAX_AGE_SECONDS,
) -> tuple[bool, dict[str, Any]]:
    """Prove that one capable Windows edge is live and has run its jobs.

    Enabled scheduler rows only describe intent.  This check additionally
    binds the edge role to one fresh heartbeat, release SHA, host/PID identity,
    and a recent successful history row for every QMT foundation job that
    cannot execute on Linux.
    """

    build_sha = str(expected_build_sha or "").strip().lower()
    max_age = _integer(success_max_age_seconds)
    poll = _integer(expected_poll_seconds)
    errors: list[str] = []
    if not BUILD_SHA_RE.fullmatch(build_sha) or build_sha == "0" * 40:
        errors.append("expected_build_sha_invalid")
    if max_age is None or max_age < 60 * 60:
        errors.append("success_max_age_invalid")
    if poll is None or poll < 15:
        errors.append("expected_poll_seconds_invalid")
    empty_detail = {
        "status": "UNAVAILABLE",
        "strategy_eligible": False,
        "executor_role": QMT_WINDOWS_EDGE_ROLE,
        "expected_build_sha": build_sha or None,
        "expected_poll_seconds": poll,
        "role_row_count": 0,
        "fresh_row_count": 0,
        "future_row_count": 0,
        "current": None,
        "required_task_types": list(
            QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES
        ),
        "owned_task_types": list(QMT_WINDOWS_EDGE_TASK_TYPES),
        "ownership_contract_verified": False,
        "task_count": 0,
        "owned_task_count": 0,
        "last_success_count": 0,
        "success_max_age_seconds": max_age,
        "tasks": {},
        "owned_tasks": {},
        "errors": errors,
    }
    if errors:
        return False, empty_detail

    try:
        runtime_rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT instance_id, mode, host_name, pid, build_sha, "
                    "executor_role, started_at, heartbeat_at, "
                    "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) "
                    "AS heartbeat_age_seconds, poll_seconds, "
                    "max_concurrent_tasks FROM st_scheduler_runtime "
                    "WHERE executor_role=:executor_role "
                    "ORDER BY heartbeat_at DESC, instance_id ASC"
                ),
                {"executor_role": QMT_WINDOWS_EDGE_ROLE},
            ).mappings()
        ]
    except Exception:
        empty_detail["errors"] = ["scheduler_runtime_query_failed"]
        return False, empty_detail

    assert poll is not None
    fresh_rows, future_rows, heartbeat_errors = (
        _classify_current_heartbeats(
            runtime_rows,
            expected_poll_seconds=poll,
        )
    )
    errors.extend(heartbeat_errors)
    if len(fresh_rows) != 1:
        errors.append("fresh_heartbeat_not_unique")

    current = fresh_rows[0] if len(fresh_rows) == 1 else None
    current_detail: dict[str, Any] | None = None
    if current is not None:
        host_name = str(current.get("host_name") or "").strip()
        pid = _integer(current.get("pid"))
        instance_id = str(current.get("instance_id") or "")
        current_build_sha = str(current.get("build_sha") or "").lower()
        poll_seconds = _integer(current.get("poll_seconds"))
        concurrency = _integer(current.get("max_concurrent_tasks"))
        current_detail = {
            "instance_id": instance_id,
            "mode": str(current.get("mode") or ""),
            "host_name": host_name,
            "pid": pid,
            "build_sha": current_build_sha,
            "executor_role": str(current.get("executor_role") or ""),
            "heartbeat_age_seconds": _integer(
                current.get("heartbeat_age_seconds")
            ),
            "poll_seconds": poll_seconds,
            "max_concurrent_tasks": concurrency,
        }
        if not host_name or len(host_name) > 128:
            errors.append("host_invalid")
        if pid is None or pid <= 0:
            errors.append("pid_invalid")
        if instance_id != f"{host_name}-{pid}":
            errors.append("instance_id_mismatch")
        if current_detail["mode"] != "standalone":
            errors.append("mode_mismatch")
        if current_build_sha != build_sha:
            errors.append("build_sha_mismatch")
        if current_detail["executor_role"] != QMT_WINDOWS_EDGE_ROLE:
            errors.append("executor_role_mismatch")
        if poll_seconds != poll:
            errors.append("poll_seconds_mismatch")
        if concurrency is None or concurrency < 1:
            errors.append("max_concurrent_tasks_invalid")
        if current.get("started_at") is None:
            errors.append("started_at_missing")
        if current.get("heartbeat_at") is None:
            errors.append("heartbeat_at_missing")

    task_errors: list[str] = []
    try:
        task_placeholders = ",".join(
            f":task_type_{index}"
            for index, _ in enumerate(QMT_WINDOWS_EDGE_TASK_TYPES)
        )
        task_rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT id, task_name, task_type, group_name, script_path, "
                    "script_args, cron_time, interval_minutes, enabled, "
                    "sort_order, date_param, description, last_run_status "
                    "FROM st_scheduled_tasks WHERE task_type IN "
                    f"({task_placeholders}) "
                    "ORDER BY task_type, id"
                ),
                {
                    f"task_type_{index}": task_type
                    for index, task_type in enumerate(
                        QMT_WINDOWS_EDGE_TASK_TYPES
                    )
                },
            ).mappings()
        ]
        task_errors, tasks_by_type = _qmt_task_contract_errors(task_rows)
        errors.extend(task_errors)
    except Exception:
        task_rows = []
        tasks_by_type = {}
        task_errors = ["scheduler_task_query_failed"]
        errors.append("scheduler_task_query_failed")

    try:
        proof_placeholders = ",".join(
            f":task_type_{index}"
            for index, _ in enumerate(
                QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES
            )
        )
        history_rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT id, task_id, task_type, status, run_at, "
                    "finished_at, exit_code, host_name, scheduler_instance_id, "
                    "TIMESTAMPDIFF(SECOND, finished_at, NOW()) "
                    "AS success_age_seconds FROM st_scheduled_task_history "
                    "WHERE task_type IN "
                    f"({proof_placeholders}) "
                    "AND status='success' AND finished_at IS NOT NULL "
                    "ORDER BY task_type, finished_at DESC, id DESC"
                ),
                {
                    f"task_type_{index}": task_type
                    for index, task_type in enumerate(
                        QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES
                    )
                },
            ).mappings()
        ]
    except Exception:
        history_rows = []
        errors.append("scheduler_history_query_failed")

    latest_success: dict[str, dict[str, Any]] = {}
    for row in history_rows:
        task_type = str(row.get("task_type") or "")
        if task_type in QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES:
            latest_success.setdefault(task_type, row)

    owned_task_details: dict[str, dict[str, Any]] = {}
    for task_type in QMT_WINDOWS_EDGE_TASK_TYPES:
        task_row = tasks_by_type.get(task_type)
        success = latest_success.get(task_type)
        task_id = _integer(task_row.get("id")) if task_row else None
        success_age = (
            _integer(success.get("success_age_seconds")) if success else None
        )
        success_task_id = (
            _integer(success.get("task_id")) if success else None
        )
        success_host = str(success.get("host_name") or "") if success else ""
        success_instance = (
            str(success.get("scheduler_instance_id") or "")
            if success
            else ""
        )
        owned_task_details[task_type] = {
            "task_id": task_id,
            "execution_proof_required": (
                task_type in QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES
            ),
            "last_run_status": (
                str(task_row.get("last_run_status") or "")
                if task_row
                else None
            ),
            "last_success_age_seconds": success_age,
            "last_success_host": success_host or None,
            "last_success_instance_id": success_instance or None,
        }
        if task_type not in QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES:
            continue
        if success is None:
            errors.append(f"last_success_missing:{task_type}")
            continue
        if success_task_id != task_id:
            errors.append(f"last_success_task_id_mismatch:{task_type}")
        if success_age is None or success_age < 0:
            errors.append(f"last_success_time_invalid:{task_type}")
        elif success_age > int(max_age):
            errors.append(f"last_success_stale:{task_type}")
        if _integer(success.get("exit_code")) != 0:
            errors.append(f"last_success_exit_code_invalid:{task_type}")
        if current_detail is not None:
            if success_host != current_detail["host_name"]:
                errors.append(f"last_success_host_mismatch:{task_type}")
            expected_prefix = f"{current_detail['host_name']}-"
            historical_pid = _integer(
                success_instance[len(expected_prefix) :]
                if success_instance.startswith(expected_prefix)
                else None
            )
            if historical_pid is None or historical_pid <= 0:
                errors.append(
                    f"last_success_instance_invalid:{task_type}"
                )

    passed = not errors
    proof_task_details = {
        task_type: owned_task_details[task_type]
        for task_type in QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES
    }
    return passed, {
        "status": "AVAILABLE" if passed else "UNAVAILABLE",
        "strategy_eligible": passed,
        "executor_role": QMT_WINDOWS_EDGE_ROLE,
        "expected_build_sha": build_sha,
        "expected_poll_seconds": poll,
        "role_row_count": len(runtime_rows),
        "fresh_row_count": len(fresh_rows),
        "future_row_count": len(future_rows),
        "current": current_detail,
        "required_task_types": list(
            QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES
        ),
        "owned_task_types": list(QMT_WINDOWS_EDGE_TASK_TYPES),
        "ownership_contract_verified": not task_errors,
        "task_count": len(proof_task_details),
        "owned_task_count": len(task_rows),
        "last_success_count": len(latest_success),
        "success_max_age_seconds": max_age,
        "tasks": proof_task_details,
        "owned_tasks": owned_task_details,
        "errors": errors,
    }


__all__ = [
    "LINUX_STANDALONE_ROLE",
    "DEFAULT_SCHEDULER_POLL_SECONDS",
    "QMT_WINDOWS_EDGE_ROLE",
    "QMT_WINDOWS_EDGE_SUCCESS_MAX_AGE_SECONDS",
    "QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES",
    "QMT_WINDOWS_EDGE_TASK_TYPES",
    "check_qmt_windows_edge_identity",
    "check_linux_standalone_active_release",
    "check_linux_standalone_scheduler_heartbeat",
    "check_qmt_windows_edge_executor",
    "check_qmt_windows_edge_release_receipt",
]
