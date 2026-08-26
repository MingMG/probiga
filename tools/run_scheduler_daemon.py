# -*- coding: utf-8 -*-
"""Run the ProBigA scheduler as a standalone process."""

import os
import re
import subprocess
import sys
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
    _bind_windows_qmt_python()
    _bind_windows_state_roots()
    # Clear stale launcher controls, then force the one capability identity
    # accepted by the shared scheduler/health contract.
    for name in tuple(os.environ):
        if name.startswith("PROBIGA_SCHEDULER_"):
            os.environ.pop(name, None)
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


def main() -> int:
    _load_windows_runtime_env()
    singleton = _acquire_windows_singleton()
    try:
        # Import only after loading .env so module-level scheduler limits and
        # source ownership switches use the same configuration as child jobs.
        from server.api.scheduler_runtime import (
            run_scheduler_forever,
            scheduler_runtime_info,
        )

        info = scheduler_runtime_info()
        print(
            "ProBigA scheduler daemon starting "
            f"(max_concurrent_tasks={info['scheduler_max_concurrent_tasks']}, "
            f"poll_seconds={info['scheduler_poll_seconds']})"
        )
        run_scheduler_forever()
        return 0
    finally:
        _release_windows_singleton(singleton)


if __name__ == "__main__":
    raise SystemExit(main())
