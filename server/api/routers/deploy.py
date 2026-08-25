# -*- coding: utf-8 -*-
"""Lightweight deployment console API."""

from __future__ import annotations

import json
import os
import subprocess
import stat
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(tags=["deploy"])

ROOT = Path(__file__).resolve().parents[3]
MAX_LOG_CHARS = 60000
MAX_HISTORY = 20

_runs: dict[str, dict] = {}
_lock = threading.Lock()


def _in_app_deploy_enabled() -> bool:
    return os.environ.get("PROBIGA_IN_APP_DEPLOY_ENABLED", "").strip() == "1"


def _require_in_app_deploy_enabled() -> None:
    if not _in_app_deploy_enabled():
        raise HTTPException(status_code=404, detail="In-app deployment is disabled")


def _deploy_runtime_paths() -> tuple[Path, Path]:
    configured = os.environ.get("PROBIGA_DEPLOY_RUNTIME_ROOT", "").strip()
    if configured:
        run_dir = Path(configured)
    elif os.name == "nt":
        program_data = os.environ.get("PROGRAMDATA", "").strip()
        if not program_data:
            raise RuntimeError(
                "PROBIGA_DEPLOY_RUNTIME_ROOT or PROGRAMDATA is required"
            )
        run_dir = Path(program_data) / "ProBigA" / "deploy-console"
    else:
        run_dir = Path("/var/lib/probiga/deploy-console")
    if not run_dir.is_absolute():
        raise RuntimeError("deploy runtime root must be absolute")
    code_root = ROOT.resolve(strict=True)
    prospective = run_dir.resolve(strict=False)
    try:
        prospective.relative_to(code_root)
    except ValueError:
        pass
    else:
        raise RuntimeError("deploy runtime state must not be inside the code tree")
    candidate = run_dir
    while True:
        if os.path.lexists(candidate) and candidate.is_symlink():
            raise RuntimeError(f"deploy runtime path contains symlink: {candidate}")
        if candidate.parent == candidate:
            break
        candidate = candidate.parent
    run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    resolved = run_dir.resolve(strict=True)
    if os.name != "nt":
        root_stat = resolved.stat()
        if root_stat.st_uid != os.geteuid():
            raise RuntimeError("deploy runtime root is not owned by the service user")
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise RuntimeError("deploy runtime root mode must be 0700")
    return resolved, resolved / "history.json"


class DeployRunRequest(BaseModel):
    action: Literal["push", "local", "commit_push"] = "push"
    commit_message: str = Field(default="", max_length=160)
    add_all: bool = True


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _run_git(args: list[str], *, check: bool = False) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["git", *args],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
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
    run_dir, history_file = _deploy_runtime_paths()
    item = {
        "id": run["id"],
        "action": run["action"],
        "status": run["status"],
        "started_at": run["started_at"],
        "finished_at": run.get("finished_at"),
        "branch": run.get("branch"),
        "commit": run.get("commit"),
    }
    if os.path.lexists(history_file) and history_file.is_symlink():
        raise RuntimeError("deploy history file must not be a symlink")
    history = (
        json.loads(history_file.read_text(encoding="utf-8"))
        if history_file.exists() else []
    )
    if not isinstance(history, list):
        raise RuntimeError("deploy history must be a JSON array")
    history.insert(0, item)
    temp_file = run_dir / f".history-{uuid.uuid4().hex}.tmp"
    try:
        with open(temp_file, "x", encoding="utf-8") as handle:
            handle.write(json.dumps(
                history[:MAX_HISTORY], ensure_ascii=False, indent=2,
            ))
            handle.flush()
            os.fsync(handle.fileno())
        if os.name != "nt":
            os.chmod(temp_file, 0o600)
        if os.path.lexists(history_file) and history_file.is_symlink():
            raise RuntimeError("deploy history file must not be a symlink")
        os.replace(temp_file, history_file)
    finally:
        if temp_file.exists():
            temp_file.unlink()


def _read_history() -> list[dict]:
    _run_dir, history_file = _deploy_runtime_paths()
    if os.path.lexists(history_file) and history_file.is_symlink():
        raise RuntimeError("deploy history file must not be a symlink")
    history = (
        json.loads(history_file.read_text(encoding="utf-8"))
        if history_file.exists() else []
    )
    if not isinstance(history, list):
        raise RuntimeError("deploy history must be a JSON array")
    return history


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
    child_env = os.environ.copy()
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
    assert proc.stdout is not None
    for line in proc.stdout:
        _append_log(run, line)
    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"Command exited with code {code}: {' '.join(cmd)}")


def _deploy_worker(run_id: str, request: DeployRunRequest) -> None:
    with _lock:
        run = _runs[run_id]
        run["status"] = "running"

    try:
        if not _in_app_deploy_enabled():
            raise RuntimeError("In-app deployment was disabled before execution")
        status = _git_status_payload()
        with _lock:
            run["branch"] = status["branch"]
            run["commit"] = status["commit"]

        _append_log(run, f"[{_now()}] ProBigA deploy task started.\n")
        _append_log(run, f"Branch: {status['branch']}  Commit: {status['commit']} {status['subject']}\n")

        if status["branch"] != "main":
            raise RuntimeError("Only the main branch can be deployed from this console.")

        if request.action == "commit_push":
            message = request.commit_message.strip()
            if not message:
                raise RuntimeError("Commit message is required for commit and deploy.")
            if request.add_all:
                _run_process(run, ["git", "add", "-A"])
            staged = _run_git(["diff", "--cached", "--name-only"], check=True).stdout.strip()
            if not staged:
                raise RuntimeError("No staged changes to commit.")
            _run_process(run, ["git", "commit", "-m", message])
            status = _git_status_payload()
            with _lock:
                run["commit"] = status["commit"]

        if request.action in {"push", "commit_push"}:
            _run_process(run, ["git", "push", "origin", "HEAD:main"])
            _append_log(run, "\nGitHub Actions will run .github/workflows/deploy.yml after this push.\n")

        if request.action == "local":
            if status["dirty"]:
                raise RuntimeError("Working tree has uncommitted changes. Commit first or use commit and deploy.")
            deploy_script = ROOT / "deploy" / "deploy.ps1"
            if not deploy_script.exists():
                raise RuntimeError(f"Deploy script not found: {deploy_script}")
            _run_process(
                run,
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(deploy_script)],
                env={"PROBIGA_NONINTERACTIVE": "1"},
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
            "push": "Push current main branch and trigger GitHub Actions deployment.",
            "commit_push": "Commit selected changes, push main, then trigger GitHub Actions deployment.",
            "local": "Run deploy/deploy.ps1 locally.",
        },
    }


@router.post("/deploy/run")
def deploy_run(request: DeployRunRequest):
    _require_in_app_deploy_enabled()
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
