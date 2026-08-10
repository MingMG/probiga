# -*- coding: utf-8 -*-
"""Synchronous runner for manually triggered scheduler tasks."""
from __future__ import annotations

import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy.engine import Engine

from server.common.process_env import build_child_env
from server.common.scheduler_script_policy import (
    SchedulerScriptPolicyError,
    resolve_scheduler_script,
)
from server.common.scheduler_args import build_scheduler_task_args
from server.common.scheduler_tasks import update_scheduler_task
from server.common.scheduler_validation import (
    is_market_closed_skip_output,
    scheduler_output_status,
    validate_scheduler_task_result,
)


def _manual_task_timeout_seconds(timeout_seconds: int | None) -> int:
    if timeout_seconds is not None:
        return max(60, int(timeout_seconds))
    raw = os.environ.get("SCHEDULER_MANUAL_TASK_TIMEOUT_SECONDS", "").strip()
    if raw:
        try:
            return max(60, int(raw))
        except ValueError:
            pass
    raw_minutes = os.environ.get("SCHEDULER_MANUAL_TASK_TIMEOUT_MINUTES", "").strip()
    if raw_minutes:
        try:
            return max(60, int(raw_minutes) * 60)
        except ValueError:
            pass
    return 3 * 60 * 60


def run_scheduler_task_sync(
    task: Mapping[str, Any],
    *,
    root: Path,
    engine: Engine,
    output_tail_chars: int = 2000,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Run a scheduler task synchronously and persist its status."""
    task_id = int(task["id"])
    script_path = str(task.get("script_path") or "")
    try:
        script = resolve_scheduler_script(root, script_path)
    except SchedulerScriptPolicyError as exc:
        output = f"SCHEDULER_SCRIPT_BLOCKED: {exc}"
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": output,
                "last_run_duration": 0,
            },
        )
        return {"id": task_id, "status": "failed", "duration": 0, "output": output}
    if not script.is_file():
        output = f"SCHEDULER_SCRIPT_MISSING: {script}"
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": output,
                "last_run_duration": 0,
            },
        )
        return {"id": task_id, "status": "failed", "duration": 0, "output": output}
    update_scheduler_task(
        engine,
        task_id,
        {"last_run_status": "running"},
        now_columns={"last_run_at", "last_triggered_at"},
    )
    today = datetime.now().strftime("%Y-%m-%d")
    args = build_scheduler_task_args(task, script_path, today)
    cmd = [sys.executable, str(script)] + args

    timeout_value = _manual_task_timeout_seconds(timeout_seconds)
    start = datetime.now()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_value,
            cwd=str(root),
            env=build_child_env(root, engine=engine),
        )
        duration = int((datetime.now() - start).total_seconds())
        status = "success" if result.returncode == 0 else "failed"
        machine_output = (result.stdout or "") + (result.stderr or "")
        output = (result.stdout or "")[-output_tail_chars:] + (result.stderr or "")[-output_tail_chars:]
        # A capability validator may deliberately use a non-zero exit code to
        # signal BLOCK.  That means the check completed and must not enter the
        # generic failed-task retry loop.
        status = scheduler_output_status(task, machine_output) or status
        if status == "success" and not is_market_closed_skip_output(output):
            validation = validate_scheduler_task_result(task, engine=engine, started_at=start)
            if validation.checked:
                marker = "DATA_VALIDATION_OK" if validation.ok else "DATA_VALIDATION_FAILED"
                output = (output + f"\n{marker}: {validation.message}")[-(output_tail_chars * 2):]
                if not validation.ok:
                    status = "failed"
    except subprocess.TimeoutExpired as exc:
        status = "timeout"
        duration = int((datetime.now() - start).total_seconds())
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        output = (
            f"Task exceeded manual timeout of {timeout_value}s and was stopped.\n"
            + str(stdout)[-output_tail_chars:]
            + str(stderr)[-output_tail_chars:]
        )
    except Exception as exc:
        status = "failed"
        duration = int((datetime.now() - start).total_seconds())
        output = str(exc)

    update_scheduler_task(
        engine,
        task_id,
        {
            "last_run_status": status,
            "last_run_output": output,
            "last_run_duration": duration,
        },
    )
    return {"id": task_id, "status": status, "duration": duration, "output": output}
