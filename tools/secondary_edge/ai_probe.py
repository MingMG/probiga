"""Verify candidate-host AI providers without consuming production jobs.

Codex sessions and Chrome state belong to this diagnostic, not the production
worker. Passing this probe does not import production history or authorize a
host cutover. Login is always performed manually on the candidate computer.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any, Iterator
from urllib.parse import urlsplit
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PRODUCTION_THREADS = frozenset({
    "019fbe02-0390-7663-a7ba-bd150e063fe7",
    "019fbe02-0a70-7c62-9bf2-9ab439bea770",
})
THREAD_ID = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")


@contextmanager
def isolated_environment(values: dict[str, str]) -> Iterator[None]:
    """Process-local scope only; never persist user or system variables."""
    previous = {key: os.environ.get(key) for key in values}
    try:
        os.environ.update(values)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def working_directory(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(previous)


def load_providers():
    # The formal module computes its default URL at import time. Supply a
    # local, non-contacted value instead of depending on ambient SSH settings.
    with isolated_environment({"PROBIGA_REMOTE_SSH_HOST": "127.0.0.1"}):
        return importlib.import_module("tools.run_codex_web_bridge")


def _failure(reason: str, exc: Exception | None = None, **extra: Any) -> dict:
    # Raw provider output/exceptions can contain credentials or private text.
    result = {"status": "fail", "reason": reason, **extra}
    if exc is not None:
        result["error_type"] = type(exc).__name__
    return result


def _absolute_path(value: Any) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("AI_PATH_REQUIRED")
    path = Path(value)
    if not path.is_absolute():
        raise ValueError("AI_ABSOLUTE_PATH_REQUIRED")
    return path.resolve()


def isolation_paths(ai: dict) -> tuple[Path, Path]:
    profile = _absolute_path(ai.get("profile_dir"))
    codex_home = (profile.parent / "codex-home").resolve()
    user_codex_home = (Path.home() / ".codex").resolve()
    ambient = os.environ.get("CODEX_HOME", "")
    forbidden = {user_codex_home}
    if ambient:
        forbidden.add(Path(ambient).resolve())
    if any(path == blocked or blocked in path.parents
           for path in (codex_home, profile) for blocked in forbidden):
        raise ValueError("PRODUCTION_CODEX_HOME_FORBIDDEN")
    if (profile == (ROOT / "data/ai_bridge/deepseek_chrome_profile").resolve()
            or tuple(part.lower() for part in profile.parts[-3:])
            == ("data", "ai_bridge", "deepseek_chrome_profile")):
        raise ValueError("PRODUCTION_CHROME_PROFILE_FORBIDDEN")
    if profile == codex_home or codex_home in profile.parents:
        raise ValueError("AI_PROFILE_MUST_BE_SEPARATE")
    return profile, codex_home


def _git_workspace(home: Path) -> Path:
    git = shutil.which("git.exe") or shutil.which("git")
    if not git:
        raise FileNotFoundError("OFFLINE_GIT_REQUIRED")
    workspace = home / "probe-workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    if workspace.is_symlink() or (workspace / ".git").is_symlink():
        raise ValueError("AI_WORKSPACE_SYMLINK_FORBIDDEN")
    if not (workspace / ".git").is_dir():
        initialized = subprocess.run(
            [git, "init", "--quiet", str(workspace)],
            stdin=subprocess.DEVNULL, capture_output=True, timeout=30,
            env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull},
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if initialized.returncode != 0 or not (workspace / ".git").is_dir():
            raise RuntimeError("AI_ISOLATED_GIT_INIT_FAILED")
    return workspace


def _new_codex_session(executable: Path, workspace: Path, timeout: int) -> str:
    result = subprocess.run(
        [str(executable), "exec", "--ignore-user-config", "--json",
         "--sandbox", "read-only", "--skip-git-repo-check", "--cd", str(workspace),
         "This is an isolated connectivity check. Do not use tools, read files, "
         "or modify anything. Reply exactly PROBIGA_ISOLATED_SESSION_READY."],
        cwd=workspace, env=os.environ.copy(), stdin=subprocess.DEVNULL,
        capture_output=True, timeout=timeout,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        raise RuntimeError("CODEX_LOGIN_OR_GENERATION_REQUIRED")
    output = result.stdout.decode("utf-8", errors="replace")
    ids = set()
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue
        if isinstance(event, dict) and event.get("type") == "thread.started":
            thread_id = event.get("thread_id")
            if isinstance(thread_id, str) and THREAD_ID.fullmatch(thread_id):
                ids.add(thread_id)
    if len(ids) != 1 or ids & PRODUCTION_THREADS:
        raise RuntimeError("NEW_ISOLATED_CODEX_SESSION_NOT_PROVEN")
    return ids.pop()


def probe_codex(ai: dict, home: Path, providers, timeout: int) -> dict:
    channels: dict[str, dict] = {}
    try:
        executable = _absolute_path(ai.get("codex_exe"))
        if not executable.is_file():
            raise FileNotFoundError("CODEX_EXECUTABLE_REQUIRED")
        home.mkdir(parents=True, exist_ok=True)
        with isolated_environment({
            "CODEX_HOME": str(home), "PROBIGA_CODEX_EXE": str(executable),
            "PROBIGA_CODEX_STOCK_THREAD_ID": "", "PROBIGA_CODEX_GENERAL_THREAD_ID": "",
        }):
            workspace = _git_workspace(home)
            with working_directory(workspace):
                provider = providers.CodexTaskProvider(timeout)
                # Never permit the formal provider's default production IDs.
                provider.thread_ids = {}
                seen: set[str] = set()
                for channel in ("stock", "general"):
                    try:
                        thread_id = _new_codex_session(executable, workspace, timeout)
                        if thread_id in seen:
                            raise RuntimeError("ISOLATED_CODEX_SESSION_REUSED")
                        seen.add(thread_id)
                        provider.thread_ids[channel] = thread_id
                        marker = f"PROBIGA_AI_PROBE_{channel.upper()}_{uuid4().hex}"
                        answer = provider.ask(channel, "Do not use tools, read files, or modify "
                                              f"anything. Reply exactly {marker}.")
                        if not isinstance(answer, str) or answer.strip() != marker:
                            raise RuntimeError("CODEX_PROBE_ANSWER_NOT_VERIFIED")
                        channels[channel] = {"status": "pass", "thread_id": thread_id,
                                             "answer_sha256": hashlib.sha256(answer.encode()).hexdigest()}
                    except Exception as exc:
                        channels[channel] = _failure("codex_login_or_generation_not_verified", exc)
        passed = all(channels.get(name, {}).get("status") == "pass" for name in ("stock", "general"))
        return {"status": "pass" if passed else "fail", "channels": channels,
                "codex_home": str(home), "login_action": "Manually run codex login with this CODEX_HOME; rerun the probe.",
                "production_history_used": False}
    except Exception as exc:
        return _failure("isolated_codex_prerequisites_not_ready", exc, codex_home=str(home),
                        channels=channels, production_history_used=False,
                        login_action="Manually run codex login with this CODEX_HOME; rerun the probe.")


def probe_deepseek(ai: dict, profile: Path, providers, timeout: int) -> dict:
    common = {"profile_dir": str(profile),
              "login_action": "Log in manually in the dedicated Chrome window; rerun after any CAPTCHA."}
    try:
        chrome = _absolute_path(ai.get("chrome_exe"))
        if not chrome.is_file():
            raise FileNotFoundError("CHROME_EXECUTABLE_REQUIRED")
        with isolated_environment({"PROBIGA_DEEPSEEK_CHROME_EXE": str(chrome)}):
            provider = providers.DeepSeekWebProvider(timeout_seconds=timeout, login_wait_seconds=90,
                                                    profile_dir=profile)
            if not provider.prepare():
                return _failure("deepseek_manual_login_required", **common)
            marker = f"PROBIGA_DEEPSEEK_PROBE_{uuid4().hex}"
            answer = provider.ask(f"This is a connectivity check. Reply exactly {marker}.")
            if not isinstance(answer, str) or answer.strip() != marker:
                return _failure("deepseek_generation_not_verified", **common)
        return {"status": "pass", **common,
                "answer_sha256": hashlib.sha256(answer.encode()).hexdigest()}
    except Exception as exc:
        return _failure("deepseek_login_or_generation_not_verified", exc, **common)


def probe_api(ai: dict) -> dict:
    common = {"scope": "GET /api/health only; queue authorization and full cutover are not tested",
              "http_status": None, "reachable": False}
    try:
        url = ai.get("server_url")
        if not isinstance(url, str):
            raise ValueError("API_URL_REQUIRED")
        parsed = urlsplit(url)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("API_URL_INVALID_OR_CONTAINS_CREDENTIALS")
        import httpx
        with httpx.Client(timeout=30, trust_env=False, follow_redirects=False) as client:
            response = client.get(url.rstrip("/") + "/api/health")
        common.update(http_status=response.status_code, reachable=True)
        if response.status_code != 200:
            return _failure("api_health_http_status_not_200", **common)
        payload = response.json()
        revision = payload.get("release_revision") if isinstance(payload, dict) else None
        if (not isinstance(revision, dict) or revision.get("deployment_mode") != "production"
                or payload.get("status") != "ok"):
            return _failure("production_api_health_contract_not_verified", **common)
        return {"status": "pass", "health_status": "ok", **common}
    except Exception as exc:
        return _failure("api_health_not_verified", exc, **common)


def run_probe(config: dict, *, timeout: int = 240, providers=None) -> dict:
    report = {"schema": "probiga.secondary-edge-ai-status.v1",
              "captured_at": datetime.now(timezone.utc).isoformat(),
              "production_queue_accessed": False, "production_takeover_authorized": False,
              "history_migration": {"status": "blocked", "reason": "original_production_history_import_not_verified",
                                    "production_threads_modified": False}}
    try:
        ai = config.get("ai") if isinstance(config, dict) else None
        if not isinstance(ai, dict) or not 30 <= timeout <= 600:
            raise ValueError("AI_CONFIG_OR_TIMEOUT_INVALID")
        profile, home = isolation_paths(ai)
        if providers is None:
            providers = load_providers()
        checks = {"codex": probe_codex(ai, home, providers, timeout),
                  "deepseek": probe_deepseek(ai, profile, providers, timeout),
                  "api": probe_api(ai)}
    except Exception as exc:
        checks = {name: _failure("ai_probe_configuration_or_dependencies_not_ready", exc)
                  for name in ("codex", "deepseek", "api")}
    report["checks"] = checks
    report["status"] = "pass" if all(item["status"] == "pass" for item in checks.values()) else "fail"
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeout", type=int, default=240)
    args = parser.parse_args(argv)
    output = args.output or args.config.parent / "ai-status.json"
    try:
        config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    except Exception:
        config = None
    report = run_probe(config, timeout=args.timeout)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(output)
    print(json.dumps({"status": report["status"], "report": str(output)}, ensure_ascii=False))
    return 0 if report["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
