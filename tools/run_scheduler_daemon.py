# -*- coding: utf-8 -*-
"""Run the ProBigA scheduler as a standalone process."""

import json
import os
import re
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


_WINDOWS_MUTEX_NAME = "Global\\ProBigA.RunSchedulerDaemon"
_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_WINDOWS_SCHEDULER_POLL_SECONDS = 60
_WINDOWS_QMT_PYTHON = Path(
    "runtime/qmt-py313/Scripts/python.exe"
)
_WINDOWS_RUNTIME_STATE_NAME = "scheduler-runtime.json"
_WINDOWS_SHUTDOWN_REQUEST_NAME = "scheduler-shutdown-request.json"
_WINDOWS_SHUTDOWN_RECEIPT_NAME = "scheduler-shutdown-receipt.json"
_WINDOWS_CONTROL_HEARTBEAT_SECONDS = 2.0
_WINDOWS_SHUTDOWN_REQUEST_MAX_AGE_SECONDS = 180.0


def _bind_windows_state_roots() -> dict[str, str]:
    raw_program_data = str(os.environ.get("ProgramData") or "").strip()
    if not raw_program_data:
        raise RuntimeError("Windows ProgramData is unavailable")
    program_data = Path(raw_program_data).resolve(strict=True)
    roots = {
        "PROBIGA_JOB_LOG_ROOT": program_data / "ProBigA" / "jobs",
        "PROBIGA_SCHEDULER_STATE_ROOT": (
            program_data / "ProBigA" / "scheduler"
        ),
    }
    bound: dict[str, str] = {}
    for name, candidate in roots.items():
        resolved = candidate.resolve(strict=True)
        if program_data not in resolved.parents:
            raise RuntimeError("Windows scheduler state root escapes ProgramData")
        is_junction = getattr(resolved, "is_junction", lambda: False)
        if resolved.is_symlink() or is_junction():
            raise RuntimeError("Windows scheduler state root is a reparse point")
        bound[name] = str(resolved)
        os.environ[name] = str(resolved)
    os.environ["PROBIGA_API_SCHEDULER_POLL_SECONDS"] = str(
        _WINDOWS_SCHEDULER_POLL_SECONDS
    )
    return bound


def _bind_windows_build_sha() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    actual = str(completed.stdout or "").strip().lower()
    if completed.returncode != 0 or _COMMIT_SHA_RE.fullmatch(actual) is None:
        raise RuntimeError("Windows QMT edge build identity is unavailable")
    configured = str(
        os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or os.environ.get("PROBIGA_EXPECTED_GIT_SHA")
        or ""
    ).strip().lower()
    if configured and (
        _COMMIT_SHA_RE.fullmatch(configured) is None or configured != actual
    ):
        raise RuntimeError("Windows QMT edge build identity differs from checkout")
    os.environ["PROBIGA_BUILD_COMMIT_SHA"] = actual
    os.environ["PROBIGA_EXPECTED_GIT_SHA"] = actual
    return actual


def _bind_windows_qmt_python() -> str:
    """Freeze xtquant subprocesses to the registered checkout's runtime."""

    root = ROOT.resolve(strict=True)
    candidate = ROOT / _WINDOWS_QMT_PYTHON
    chain = (
        ROOT,
        ROOT / "runtime",
        ROOT / "runtime" / "qmt-py313",
        ROOT / "runtime" / "qmt-py313" / "Scripts",
        candidate,
    )
    for item in chain:
        if not item.exists():
            raise RuntimeError("Windows QMT Python runtime is unavailable")
        is_junction = getattr(item, "is_junction", lambda: False)
        if item.is_symlink() or is_junction():
            raise RuntimeError("Windows QMT Python runtime is a reparse point")
    resolved = candidate.resolve(strict=True)
    if not resolved.is_file() or root not in resolved.parents:
        raise RuntimeError("Windows QMT Python runtime escapes checkout")
    value = str(resolved)
    os.environ["QMT_PYTHON"] = value
    return value


def _load_windows_runtime_env() -> int:
    """Load the Windows owner's local environment before runtime imports."""

    if os.name != "nt":
        return 0
    from dotenv import dotenv_values

    values = {
        str(key): str(value)
        for key, value in dotenv_values(ROOT / ".env").items()
        if key and value is not None
    }
    os.environ.update(values)
    # Launcher-provided scheduler controls are not authority.  Clear them
    # before binding the protected ProgramData roots below so the roots do not
    # accidentally remove themselves from the child environment.
    for name in tuple(os.environ):
        if name.startswith("PROBIGA_SCHEDULER_"):
            os.environ.pop(name, None)
    _bind_windows_qmt_python()
    _bind_windows_state_roots()
    # Force the one capability identity accepted by the shared
    # scheduler/health contract.
    os.environ["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] = "qmt_windows_edge"
    _bind_windows_build_sha()
    if not (os.environ.get("MYSQL_URL") or os.environ.get("DATABASE_URL")):
        raise RuntimeError("scheduler database URL is unavailable")
    return len(values)


def _acquire_windows_singleton() -> tuple[object, int] | None:
    """Acquire one machine-wide scheduler mutex on Windows, fail closed."""

    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateMutexW.argtypes = (
        wintypes.LPVOID,
        wintypes.BOOL,
        wintypes.LPCWSTR,
    )
    kernel32.CreateMutexW.restype = wintypes.HANDLE
    kernel32.ReleaseMutex.argtypes = (wintypes.HANDLE,)
    kernel32.ReleaseMutex.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.CreateMutexW(None, True, _WINDOWS_MUTEX_NAME)
    error_code = ctypes.get_last_error()
    if not handle:
        raise OSError(error_code, "cannot create global scheduler mutex")
    if error_code == 183:  # ERROR_ALREADY_EXISTS
        kernel32.CloseHandle(handle)
        raise RuntimeError("another Windows scheduler instance is already active")
    return kernel32, int(handle)


def _release_windows_singleton(singleton: tuple[object, int] | None) -> None:
    if singleton is None:
        return
    kernel32, handle = singleton
    try:
        kernel32.ReleaseMutex(handle)
    finally:
        kernel32.CloseHandle(handle)


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _write_control_json(path: Path, payload: dict[str, object]) -> None:
    """Atomically publish one local scheduler-control record."""

    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise RuntimeError(f"scheduler control path is not an ordinary file: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _parse_utc_timestamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _shutdown_request_matches(
    payload: object,
    *,
    identity: dict[str, object],
) -> bool:
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        return False
    for key in ("instance_id", "pid", "build_sha"):
        if payload.get(key) != identity.get(key):
            return False
    nonce = str(payload.get("request_uid") or "").strip().lower()
    if re.fullmatch(r"[0-9a-f-]{36}", nonce) is None:
        return False
    requested_at = _parse_utc_timestamp(payload.get("requested_at_utc"))
    if requested_at is None:
        return False
    age_seconds = (datetime.now(timezone.utc) - requested_at).total_seconds()
    return -10.0 <= age_seconds <= _WINDOWS_SHUTDOWN_REQUEST_MAX_AGE_SECONDS


def _start_windows_shutdown_monitor(
    *,
    build_sha: str,
) -> tuple[threading.Event, threading.Event, threading.Thread, dict[str, object]] | None:
    """Publish a fresh identity and accept only an exact targeted stop."""

    if os.name != "nt":
        return None
    state_root_text = str(os.environ.get("PROBIGA_SCHEDULER_STATE_ROOT") or "").strip()
    if not state_root_text:
        raise RuntimeError("Windows scheduler state root is unavailable")
    state_root = Path(state_root_text).resolve(strict=True)
    if state_root.is_symlink() or not state_root.is_dir():
        raise RuntimeError("Windows scheduler state root is not an ordinary directory")
    identity: dict[str, object] = {
        "schema_version": 1,
        "instance_id": str(uuid.uuid4()),
        "pid": os.getpid(),
        "build_sha": build_sha,
        "started_at_utc": _utc_now_text(),
    }
    runtime_path = state_root / _WINDOWS_RUNTIME_STATE_NAME
    request_path = state_root / _WINDOWS_SHUTDOWN_REQUEST_NAME
    stop_event = threading.Event()
    monitor_done = threading.Event()

    def monitor() -> None:
        try:
            while not monitor_done.is_set():
                heartbeat = dict(identity)
                heartbeat["heartbeat_at_utc"] = _utc_now_text()
                _write_control_json(runtime_path, heartbeat)
                if request_path.is_file() and not request_path.is_symlink():
                    try:
                        request = json.loads(request_path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                        request = None
                    if _shutdown_request_matches(request, identity=identity):
                        identity["request_uid"] = request["request_uid"]
                        stop_event.set()
                        return
                monitor_done.wait(_WINDOWS_CONTROL_HEARTBEAT_SECONDS)
        except Exception:
            # Losing the protected local heartbeat means the process can no
            # longer prove its identity to the updater.  Stop dispatching and
            # let the Job Object close the process tree.
            stop_event.set()

    # The first heartbeat must exist before main reports the daemon as started.
    first_heartbeat = dict(identity)
    first_heartbeat["heartbeat_at_utc"] = _utc_now_text()
    _write_control_json(runtime_path, first_heartbeat)
    thread = threading.Thread(
        target=monitor,
        daemon=True,
        name="scheduler-shutdown-monitor",
    )
    thread.start()
    return stop_event, monitor_done, thread, identity


def _finish_windows_shutdown_monitor(
    control: tuple[
        threading.Event,
        threading.Event,
        threading.Thread,
        dict[str, object],
    ] | None,
) -> None:
    if control is None:
        return
    _stop_event, monitor_done, thread, identity = control
    monitor_done.set()
    thread.join(timeout=5.0)
    state_root = Path(str(os.environ["PROBIGA_SCHEDULER_STATE_ROOT"]))
    receipt = dict(identity)
    receipt.update({"status": "stopped", "stopped_at_utc": _utc_now_text()})
    _write_control_json(state_root / _WINDOWS_SHUTDOWN_RECEIPT_NAME, receipt)
    runtime_path = state_root / _WINDOWS_RUNTIME_STATE_NAME
    try:
        current = json.loads(runtime_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        current = None
    if isinstance(current, dict) and current.get("instance_id") == identity["instance_id"]:
        runtime_path.unlink(missing_ok=True)


def main() -> int:
    _load_windows_runtime_env()
    singleton = _acquire_windows_singleton()
    control = None
    try:
        # Import only after loading .env so module-level scheduler limits and
        # source ownership switches use the same configuration as child jobs.
        from server.api.scheduler_runtime import (
            run_scheduler_forever,
            scheduler_runtime_info,
        )
        if (
            os.name != "nt"
            and os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower()
            == "production"
        ):
            from server.api.routers._engine import get_engine
            from server.common.release_manifest import (
                register_runtime_release_manifest,
                verify_runtime_release_manifest,
            )

            release_identity = verify_runtime_release_manifest(ROOT)
            if release_identity.get("verified") is not True:
                raise RuntimeError("production release manifest identity differs")
            register_runtime_release_manifest(
                get_engine(),
                release_identity["manifest"],
            )

        info = scheduler_runtime_info()
        build_sha = str(os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or "")
        control = _start_windows_shutdown_monitor(build_sha=build_sha)
        print(
            "ProBigA scheduler daemon starting "
            f"(max_concurrent_tasks={info['scheduler_max_concurrent_tasks']}, "
            f"poll_seconds={info['scheduler_poll_seconds']})"
        )
        run_scheduler_forever(
            stop_event=control[0] if control is not None else None,
        )
        return 0
    finally:
        try:
            _finish_windows_shutdown_monitor(control)
        finally:
            _release_windows_singleton(singleton)


if __name__ == "__main__":
    raise SystemExit(main())
