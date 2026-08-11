# -*- coding: utf-8 -*-
"""Lightweight deployment console API."""

from __future__ import annotations

import json
import os
import subprocess
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from server.common.process_env import build_child_env

router = APIRouter(tags=["deploy"])

ROOT = Path(__file__).resolve().parents[3]
RUN_DIR = ROOT / "runtime" / "deploy"
HISTORY_FILE = RUN_DIR / "history.json"
MAX_LOG_CHARS = 60000
MAX_HISTORY = 20
DEFAULT_GIT_TIMEOUT_SECONDS = 60
DEFAULT_DEPLOY_COMMAND_TIMEOUT_SECONDS = 30 * 60

_runs: dict[str, dict] = {}
_lock = threading.Lock()


class DeployRunRequest(BaseModel):
    action: Literal["push", "local", "commit_push"] = "push"
    commit_message: str = Field(default="", max_length=160)
    add_all: bool = True


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _in_app_deploy_enabled() -> bool:
    """Production defaults to no deployment mutation from the web process."""

    return os.environ.get("PROBIGA_IN_APP_DEPLOY_ENABLED", "").strip() == "1"


def _require_in_app_deploy_enabled() -> None:
    if not _in_app_deploy_enabled():
        raise HTTPException(
            status_code=503,
            detail=(
                "In-app deployment is disabled. Use the reviewed CI release "
                "workflow for a pinned commit."
            ),
        )


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, str(default)) or default))
    except ValueError:
        return default


def _run_git(args: list[str], *, check: bool = False) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["git", *args],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=_int_env("PROBIGA_DEPLOY_GIT_TIMEOUT_SECONDS", DEFAULT_GIT_TIMEOUT_SECONDS),
    )
    if check and result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "git command failed").strip())
    return result


def _append_log(run: dict, text: str) -> None:
    if not text:
        return
    with _lock:
        run["log"] = (run.get("log", "") + text)[-MAX_LOG_CHARS:]


def _save_history(run: dict) -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    item = {
        "id": run["id"],
        "action": run["action"],
        "status": run["status"],
        "started_at": run["started_at"],
        "finished_at": run.get("finished_at"),
        "branch": run.get("branch"),
        "commit": run.get("commit"),
    }
    try:
        history = json.loads(HISTORY_FILE.read_text(encoding="utf-8")) if HISTORY_FILE.exists() else []
    except Exception:
        history = []
    history.insert(0, item)
    HISTORY_FILE.write_text(
        json.dumps(history[:MAX_HISTORY], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_history() -> list[dict]:
    try:
        return json.loads(HISTORY_FILE.read_text(encoding="utf-8")) if HISTORY_FILE.exists() else []
    except Exception:
        return []


def _git_status_payload() -> dict:
    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip()
    commit = _run_git(["rev-parse", "--short", "HEAD"]).stdout.strip()
    subject = _run_git(["log", "-1", "--pretty=%s"]).stdout.strip()
    status_lines = [line for line in _run_git(["status", "--short"]).stdout.splitlines() if line.strip()]
    remote = _run_git(["remote", "get-url", "origin"]).stdout.strip()
    upstream_result = _run_git(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    upstream = upstream_result.stdout.strip() if upstream_result.returncode == 0 else ""

    return {
        "branch": branch,
        "commit": commit,
        "subject": subject,
        "remote": remote,
        "upstream": upstream,
        "dirty": bool(status_lines),
        "changed_count": len(status_lines),
        "changed_files": status_lines[:80],
    }


def _run_process(run: dict, cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    _append_log(run, f"$ {' '.join(cmd)}\n")
    child_env = build_child_env(ROOT)
    if env:
        child_env.update(env)

    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=child_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    timeout = _int_env("PROBIGA_DEPLOY_COMMAND_TIMEOUT_SECONDS", DEFAULT_DEPLOY_COMMAND_TIMEOUT_SECONDS)
    try:
        stdout, _stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        stdout, _stderr = proc.communicate()
        partial = exc.output or ""
        _append_log(run, str(partial) + (stdout or ""))
        raise RuntimeError(f"Command timed out after {timeout}s: {' '.join(cmd)}") from exc
    _append_log(run, stdout or "")
    code = proc.returncode
    if code != 0:
        raise RuntimeError(f"Command exited with code {code}: {' '.join(cmd)}")


def _deploy_worker(run_id: str, request: DeployRunRequest) -> None:
    with _lock:
        run = _runs[run_id]
        run["status"] = "running"

    try:
        if request.action != "push":
            raise RuntimeError(
                "In-app commit and direct-host deployment actions are forbidden."
            )
        status = _git_status_payload()
        with _lock:
            run["branch"] = status["branch"]
            run["commit"] = status["commit"]

        _append_log(run, f"[{_now()}] ProBigA deploy task started.\n")
        _append_log(run, f"Branch: {status['branch']}  Commit: {status['commit']} {status['subject']}\n")

        if status["branch"] != "main":
            raise RuntimeError("Only the main branch can be deployed from this console.")
        if status["dirty"]:
            raise RuntimeError(
                "Refusing to push from a dirty working tree; CI must receive a reviewed commit."
            )
        _run_process(run, ["git", "push", "origin", "HEAD:main"])
        _append_log(
            run,
            "\nGitHub Actions will validate and deploy the pushed commit.\n",
        )

        with _lock:
            run["status"] = "success"
            run["finished_at"] = _now()
        _append_log(run, f"\n[{_now()}] Deploy task finished successfully.\n")
    except Exception as exc:
        with _lock:
            run["status"] = "failed"
            run["finished_at"] = _now()
            run["error"] = str(exc)
        _append_log(run, f"\n[{_now()}] Deploy task failed: {exc}\n")
    finally:
        with _lock:
            snapshot = dict(_runs[run_id])
        _save_history(snapshot)


@router.get("/deploy/status")
def deploy_status():
    _require_in_app_deploy_enabled()
    return {
        "repo": _git_status_payload(),
        "runs": list(_runs.values())[-5:][::-1],
        "history": _read_history(),
        "actions": {
            "push": "Push the current clean main commit and trigger the CI deployment.",
        },
    }


@router.post("/deploy/run")
def deploy_run(request: DeployRunRequest):
    _require_in_app_deploy_enabled()
    if request.action != "push":
        raise HTTPException(
            status_code=403,
            detail="In-app commit and direct-host deployment actions are forbidden.",
        )
    running = [r for r in _runs.values() if r.get("status") == "running"]
    if running:
        raise HTTPException(status_code=409, detail=f"Deploy task already running: {running[-1]['id']}")

    run_id = uuid.uuid4().hex[:12]
    run = {
        "id": run_id,
        "action": request.action,
        "status": "queued",
        "started_at": _now(),
        "finished_at": None,
        "log": "",
        "error": "",
    }
    with _lock:
        _runs[run_id] = run

    thread = threading.Thread(target=_deploy_worker, args=(run_id, request), daemon=True, name=f"deploy-{run_id}")
    thread.start()
    return {"id": run_id, "status": "queued"}


@router.get("/deploy/runs/{run_id}")
def deploy_run_detail(run_id: str):
    _require_in_app_deploy_enabled()
    run = _runs.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Deploy run not found")
    return run
