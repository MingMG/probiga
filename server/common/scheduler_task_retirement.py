"""Retire superseded ambient-provider tasks without changing execution audit."""
from __future__ import annotations

from typing import Mapping
from datetime import date, datetime
import hashlib
import re

from sqlalchemy import text

from tools.qmt_host_ownership_contract import (
    UNFROZEN_PROVIDER_SCRIPT_PATHS,
    UNFROZEN_PROVIDER_TASK_TYPES,
)


def is_retired_provider_task(row: Mapping[str, object]) -> bool:
    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").strip().replace("\\", "/")
    return task_type in UNFROZEN_PROVIDER_TASK_TYPES or (
        script_path in UNFROZEN_PROVIDER_SCRIPT_PATHS
        and task_type not in {"intraday_minute_kline", "intraday_minute_flow"}
    )


def retire_superseded_provider_tasks(engine) -> dict:
    """Release migration: disable only; keep ids, receipts and running owners."""
    retired = []
    with engine.begin() as connection:
        suffix = "" if connection.dialect.name == "sqlite" else " FOR UPDATE"
        rows = connection.execute(text(
            "SELECT id, task_type, script_path, enabled FROM st_scheduled_tasks "
            "ORDER BY id" + suffix
        )).mappings().all()
        for row in rows:
            if not is_retired_provider_task(row) or int(row["enabled"] or 0) != 1:
                continue
            result = connection.execute(text(
                "UPDATE st_scheduled_tasks SET enabled=0 "
                "WHERE id=:id AND enabled=1 AND task_type=:task_type "
                "AND script_path=:script_path"
            ), dict(row))
            if result.rowcount != 1:
                raise RuntimeError("provider task retirement identity changed")
            retired.append({"id": int(row["id"]), "task_type": row["task_type"],
                            "script_path": row["script_path"], "previous_enabled": 1})
    return {"status": "PASS", "reason": "superseded_ambient_provider_identity",
            "retired_tasks": retired, "history_preserved": True}


_CALENDAR_SKIP = re.compile(r"Skipped automatically: (?P<trade_date>\d{4}-\d{2}-\d{2}) is not a trading day\.")
_RUN_STATUS = frozenset({"running", "success", "degraded", "failed", "blocked", "timeout", "stopped"})


def _projection_audit(values: Mapping[str, object]) -> dict:
    return {**{key: str(values.get(key) if values.get(key) is not None else "") for key in (
        "last_run_status", "last_run_at", "last_run_duration",
    )}, "last_run_output_sha256": hashlib.sha256(
        str(values.get("last_run_output") or "").encode("utf-8")
    ).hexdigest()}


def restore_calendar_skip_projections(engine) -> dict:
    """Repair the compact display from immutable runs, without closing a run.

    Only the exact obsolete calendar message is eligible. The scheduler cursor
    stays intact; a running history stays running and still requires the normal
    exact-owner recovery. Reports identify receipts without disclosing log text.
    """
    restored, unresolved = [], []
    with engine.begin() as connection:
        suffix = "" if connection.dialect.name == "sqlite" else " FOR UPDATE"
        rows = connection.execute(text(
            "SELECT id,task_type,last_run_status,last_run_at,last_run_output,last_run_duration "
            "FROM st_scheduled_tasks ORDER BY id" + suffix
        )).mappings().all()
        for row in rows:
            skipped = _CALENDAR_SKIP.fullmatch(str(row.get("last_run_output") or ""))
            if skipped is None:
                continue
            try:
                date.fromisoformat(skipped.group("trade_date"))
            except ValueError:
                continue
            history = connection.execute(text(
                "SELECT task_id,task_type,run_uid,build_sha,run_at,finished_at,status,duration,output "
                "FROM st_scheduled_task_history WHERE task_id=:id ORDER BY id DESC LIMIT 1" + suffix
            ), {"id": row["id"]}).mappings().first()
            try:
                if history is None or history["task_type"] != row["task_type"]:
                    raise ValueError("history_identity_unavailable")
                if not re.fullmatch(r"[0-9a-f]{32,64}", str(history["run_uid"] or "")):
                    raise ValueError("history_run_identity_invalid")
                if not re.fullmatch(r"[0-9a-f]{40}", str(history["build_sha"] or "")):
                    raise ValueError("history_build_identity_invalid")
                run_at = datetime.fromisoformat(str(history["run_at"]))
                skip_at = datetime.fromisoformat(str(row["last_run_at"]))
                status = str(history["status"] or "")
                if status not in _RUN_STATUS or run_at > skip_at:
                    raise ValueError("history_execution_identity_invalid")
                finished = history["finished_at"]
                if status == "running":
                    if finished is not None:
                        raise ValueError("running_history_is_finished")
                elif finished is None or not run_at <= datetime.fromisoformat(str(finished)) <= skip_at:
                    raise ValueError("terminal_history_time_invalid")
            except (TypeError, ValueError) as exc:
                unresolved.append({"id": int(row["id"]), "task_type": row["task_type"],
                                   "reason": str(exc) if str(exc).startswith(("history_", "running_", "terminal_")) else "history_shape_invalid"})
                continue
            after = {"last_run_status": status, "last_run_at": history["run_at"],
                     "last_run_output": history["output"], "last_run_duration": history["duration"]}
            changed = connection.execute(text(
                "UPDATE st_scheduled_tasks SET last_run_status=:last_run_status, "
                "last_run_at=:last_run_at,last_run_output=:last_run_output,last_run_duration=:last_run_duration "
                "WHERE id=:id AND task_type=:task_type AND last_run_output=:skip_output"
            ), {**after, "id": row["id"], "task_type": row["task_type"], "skip_output": row["last_run_output"]})
            if changed.rowcount != 1:
                raise RuntimeError("calendar skip projection identity changed")
            restored.append({"id": int(row["id"]), "task_type": row["task_type"],
                             "source_run_uid": history["run_uid"], "source_build_sha": history["build_sha"],
                             "before": _projection_audit(row), "after": _projection_audit(after)})
    return {"status": "BLOCKED" if unresolved else "PASS", "restored": restored,
            "unresolved": unresolved, "history_preserved": True}
