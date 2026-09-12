"""Validate staged Windows code before cutover; evidence never grants activation.

The updater prepares trusted main in a detached worktree and invokes this tool
with the registered production interpreter. Linux only reads the resulting
append-only evidence. Neither mode stops services, changes the live checkout,
installs dependencies, submits QMT requests, or touches the terminal UI.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from server.common.release_candidate import (
    CandidateValidationError, SCHEMA, append_candidate, build_candidate, read_candidate,
)


def now() -> datetime:
    return datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)


def git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "--no-replace-objects", "-C", str(root), *args],
        capture_output=True, timeout=30, check=False,
    )
    if result.returncode:
        raise CandidateValidationError("CANDIDATE_GIT_FAILED")
    return result.stdout.decode("utf-8").strip()


def ordinary(path: Path) -> Path:
    if not path.is_absolute():
        raise CandidateValidationError("CANDIDATE_PATH_NOT_ABSOLUTE")
    for part in (path, *path.parents):
        if (not part.exists() or part.is_symlink()
                or getattr(part, "is_junction", lambda: False)()):
            raise CandidateValidationError("CANDIDATE_PATH_NOT_ORDINARY")
    return path.resolve()


def checkout(root: Path, sha: str, *, production: bool = False) -> str:
    ordinary(root)
    if (not re.fullmatch(r"[0-9a-f]{40}", sha)
            or Path(git(root, "rev-parse", "--show-toplevel")).resolve() != root.resolve()
            or git(root, "rev-parse", "HEAD") != sha
            or git(root, "status", "--porcelain", "--untracked-files=normal")
            or git(root, "remote", "get-url", "origin") != "https://github.com/MingMG/probiga.git"):
        raise CandidateValidationError("CANDIDATE_CHECKOUT_DIFFERS")
    if production and git(root, "symbolic-ref", "--short", "HEAD") != "main":
        raise CandidateValidationError("CANDIDATE_PRODUCTION_BRANCH_DIFFERS")
    return git(root, "rev-parse", "HEAD^{tree}")


def runtime_fingerprint(runtime: Path) -> str:
    files = [runtime / ".env", runtime / ".venv/Scripts/python.exe",
             runtime / "runtime/qmt-py313/Scripts/python.exe"]
    contents = [hashlib.sha256(ordinary(path).read_bytes()).hexdigest() for path in files]
    packages = sorted((d.metadata["Name"], d.version) for d in importlib.metadata.distributions())
    # Include inherited runtime configuration without exposing its values.
    config = sorted((k, v) for k, v in os.environ.items() if k.startswith((
        "PROBIGA_", "QMT_", "BIG_QMT_", "GJ_QMT_", "MYSQL_", "GM_", "MYQUANT_")))
    return hashlib.sha256(json.dumps([contents, packages, config], sort_keys=True,
                                    ensure_ascii=True).encode()).hexdigest()


def probe(command: list[str], *, env: dict[str, str], stage: str) -> dict:
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True,
                            timeout=120, check=False)
    if result.returncode:
        # Native diagnostics may contain configuration. Keep only fixed stages.
        raise CandidateValidationError(f"CANDIDATE_{stage}_FAILED")
    try:
        payload = json.loads(result.stdout.decode("utf-8-sig"))
    except (UnicodeError, ValueError) as exc:
        raise CandidateValidationError(f"CANDIDATE_{stage}_JSON_INVALID") from exc
    if not isinstance(payload, dict):
        raise CandidateValidationError(f"CANDIDATE_{stage}_ENVELOPE_INVALID")
    return payload


def validate_native(runtime: Path, build_sha: str, prior_build_sha: str) -> dict:
    if os.name != "nt":
        raise CandidateValidationError("CANDIDATE_REQUIRES_NATIVE_WINDOWS")
    runtime = ordinary(runtime)
    if Path(sys.executable).resolve() != ordinary(runtime / ".venv/Scripts/python.exe"):
        raise CandidateValidationError("CANDIDATE_PRODUCTION_PYTHON_DIFFERS")
    tree = checkout(ROOT, build_sha)
    checkout(runtime, prior_build_sha, production=True)
    # A clean conversation commit can be tested before merging. The updater's
    # preparation entrypoint and Linux broker separately restrict activation
    # to trusted merged main; a candidate receipt never authorizes deployment.
    git(ROOT, "merge-base", "--is-ancestor", prior_build_sha, build_sha)
    before = runtime_fingerprint(runtime)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONSAFEPATH="1",
               PYTHONPATH=str(ROOT), PROBIGA_CODE_ROOT=str(ROOT),
               PROBIGA_BUILD_COMMIT_SHA=build_sha, PROBIGA_EXPECTED_GIT_SHA=build_sha)
    ps = ordinary(Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe")
    native = [str(ps), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File"]
    imports = probe([sys.executable, "-P", "-c",
        "import tools.run_qmt_windows_edge_release_bootstrap; "
        "import tools.run_scheduler_daemon; import integrations.bigqmt.health; "
        "import json; print(json.dumps({'imports': 'PASS'}), flush=True)"], env=env, stage="IMPORTS")
    if imports != {"imports": "PASS"}:
        raise CandidateValidationError("CANDIDATE_IMPORTS_ENVELOPE_INVALID")
    sdk = probe([str(runtime / "runtime/qmt-py313/Scripts/python.exe"), "-P",
        str(ROOT / "tools/ensure_qmt_myquant_runtime.py"), "--expected-build-sha", build_sha,
        "--runtime-root", str(runtime)], env=env, stage="MYQUANT")
    if (sdk.get("status") != "READY" or sdk.get("mode") != "check"
            or sdk.get("build_sha") != build_sha or sdk.get("installed") is not False
            or sdk.get("database_writes") is not False or sdk.get("qmt_calls") is not False):
        raise CandidateValidationError("CANDIDATE_MYQUANT_ENVELOPE_INVALID")
    ui = probe(native + [str(ROOT / "tools/reload_big_qmt_strategy.ps1"),
        "-RegisteredRoot", str(ROOT), "-RuntimeRoot", str(runtime),
        "-ExpectedBuildSha", build_sha, "-PreflightOnly"], env=env, stage="POWERSHELL")
    if (ui.get("schema") != "probiga.bigqmt-ui-release-reload.v1"
            or ui.get("mode") != "PREFLIGHT_ONLY" or ui.get("status") != "READY"
            or ui.get("expected_build_sha") != build_sha
            or any(ui.get(k) is not False for k in ("qmt_calls", "database_writes",
                "ui_actions_attempted", "authentication_attempted", "automatic_order_submission",
                "direct_python_strategy_execution"))):
        raise CandidateValidationError("CANDIDATE_POWERSHELL_ENVELOPE_INVALID")
    health = probe(native + [str(ROOT / "tools/ensure_big_qmt_strategy_running.ps1"),
        "-CheckOnly", "-RuntimeRoot", str(runtime)], env=env, stage="RECOVERY_HEALTH")
    if (health.get("schema") != "probiga.qmt-recovery-health.v1"
            or health.get("healthy") is not True or health.get("failed_checks") != []
            or health.get("qmt_client_pid") != ui.get("qmt_client_pid")
            or health.get("database_writes") is not False
            or health.get("ui_actions_attempted") is not False):
        raise CandidateValidationError("CANDIDATE_RECOVERY_HEALTH_ENVELOPE_INVALID")
    if (checkout(ROOT, build_sha) != tree
            or runtime_fingerprint(runtime) != before):
        raise CandidateValidationError("CANDIDATE_CHANGED_DURING_VALIDATION")
    checkout(runtime, prior_build_sha, production=True)
    return build_candidate(build_sha=build_sha, tree_sha=tree,
        prior_build_sha=prior_build_sha, host_name=socket.gethostname(), validated_at=now(),
        runtime_fingerprint=before, qmt_client_pid=health["qmt_client_pid"],
        model_instance_id=health["model_instance_id"])


def wait_candidate(engine, *, build_sha: str, tree_sha: str, prior_build_sha: str,
                   wait_seconds: int) -> dict:
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            # Each poll gets a new transaction; never retain an old snapshot.
            with engine.connect() as connection:
                return read_candidate(connection, build_sha=build_sha, tree_sha=tree_sha,
                                      prior_build_sha=prior_build_sha, now=now())
        except CandidateValidationError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(5, max(0, deadline - time.monotonic())))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--validate", action="store_true")
    modes.add_argument("--check", action="store_true")
    parser.add_argument("--runtime-root", type=Path)
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--prior-build-sha", required=True)
    parser.add_argument("--tree-sha")
    parser.add_argument("--wait-seconds", type=int, default=0, choices=range(0, 901), metavar="0..900")
    args = parser.parse_args()
    from tools.env_config import create_tool_engine, load_project_env
    engine = None
    try:
        if args.validate:
            if args.runtime_root is None or args.tree_sha or args.wait_seconds:
                raise CandidateValidationError("CANDIDATE_VALIDATE_ARGUMENTS_INVALID")
            load_project_env(ordinary(args.runtime_root / ".env"))
            payload = validate_native(args.runtime_root, args.expected_build_sha, args.prior_build_sha)
            engine = create_tool_engine()
            with engine.begin() as connection:
                append_candidate(connection, payload, now=now())
        else:
            if not args.tree_sha or args.runtime_root:
                raise CandidateValidationError("CANDIDATE_CHECK_ARGUMENTS_INVALID")
            load_project_env()
            engine = create_tool_engine()
            payload = wait_candidate(engine, build_sha=args.expected_build_sha,
                tree_sha=args.tree_sha, prior_build_sha=args.prior_build_sha,
                wait_seconds=args.wait_seconds)
        print(json.dumps(dict(schema=SCHEMA, status="READY", candidate=payload,
                              activation_granted=False, database_writes=bool(args.validate))), flush=True)
        return 0
    except Exception as exc:
        reason = str(exc) if isinstance(exc, CandidateValidationError) else "CANDIDATE_PROBE_UNAVAILABLE"
        print(json.dumps(dict(schema=SCHEMA, status="BLOCKED", reason=reason,
                              activation_granted=False, database_writes=False)), flush=True)
        return 4
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
