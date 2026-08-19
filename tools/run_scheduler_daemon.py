# -*- coding: utf-8 -*-
"""Run the ProBigA scheduler as a standalone process."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


_WINDOWS_MUTEX_NAME = "Global\\ProBigA.RunSchedulerDaemon"


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
    # Launcher-control variables are never business configuration and must not
    # leak into scheduler task subprocesses.
    for name in tuple(os.environ):
        if name.startswith("PROBIGA_SCHEDULER_"):
            os.environ.pop(name, None)
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
