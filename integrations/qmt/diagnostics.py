from __future__ import annotations

import csv
import io
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from integrations.qmt import bridge


PROVIDER_ID = "gj_qmt"
CLIENT_PROCESS_NAME = "XtMiniQmt.exe"

_cache_lock = threading.Lock()
_diagnostics_cache: tuple[float, dict[str, Any]] | None = None
_capabilities_cache: tuple[float, dict[str, Any]] | None = None
_core_probe_cache: tuple[float, dict[str, Any]] | None = None


def iter_client_candidates() -> list[Path]:
    candidates: list[Path] = []

    explicit_exe = (os.environ.get("GJ_QMT_EXE") or os.environ.get("QMT_CLIENT_EXE") or "").strip()
    if explicit_exe:
        candidates.append(Path(explicit_exe))

    explicit_home = (os.environ.get("GJ_QMT_HOME") or os.environ.get("QMT_HOME") or "").strip()
    if explicit_home:
        home = Path(explicit_home)
        candidates.extend([home / "bin.x64" / CLIENT_PROCESS_NAME, home / CLIENT_PROCESS_NAME])

    candidates.extend(
        [
            Path(r"D:\国金证券QMT交易端\bin.x64\XtMiniQmt.exe"),
            Path(r"C:\国金证券QMT交易端\bin.x64\XtMiniQmt.exe"),
            Path(r"D:\QMT\bin.x64\XtMiniQmt.exe"),
            Path(r"C:\QMT\bin.x64\XtMiniQmt.exe"),
        ]
    )

    result: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result


def resolve_client_path() -> Path | None:
    for candidate in iter_client_candidates():
        if candidate.is_file():
            return candidate.resolve()
    return None


def _client_file_version(path: Path | None) -> str | None:
    if path is None or os.name != "nt":
        return None
    try:
        env = os.environ.copy()
        env["GJ_QMT_VERSION_PATH"] = str(path)
        proc = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-Command",
                "(Get-Item -LiteralPath $env:GJ_QMT_VERSION_PATH).VersionInfo.FileVersion",
            ],
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=5,
            check=False,
            env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    value = (proc.stdout or "").strip().replace(",", ".")
    return value or None


def _windows_process_rows() -> list[dict[str, str]]:
    if os.name != "nt":
        return []
    try:
        proc = subprocess.run(
            [
                "tasklist.exe",
                "/FI",
                f"IMAGENAME eq {CLIENT_PROCESS_NAME}",
                "/FO",
                "CSV",
                "/NH",
            ],
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    rows: list[dict[str, str]] = []
    for row in csv.reader(io.StringIO(proc.stdout or "")):
        if not row or row[0].casefold() != CLIENT_PROCESS_NAME.casefold():
            continue
        rows.append(
            {
                "image_name": row[0],
                "pid": row[1] if len(row) > 1 else "",
                "memory": row[4] if len(row) > 4 else "",
            }
        )
    return rows


def client_status() -> dict[str, Any]:
    client_path = resolve_client_path()
    processes = _windows_process_rows()
    return {
        "provider": PROVIDER_ID,
        "installed": client_path is not None,
        "client_path": str(client_path) if client_path else None,
        "client_version": _client_file_version(client_path),
        "running": bool(processes),
        "processes": processes,
    }


def _classify_bridge_error(exc: Exception) -> str:
    text = str(exc).casefold()
    if "timed out" in text or "timeout" in text:
        return "CONNECT_TIMEOUT"
    if "runtime not found" in text:
        return "SDK_RUNTIME_MISSING"
    if "no module named" in text or "not importable" in text:
        return "SDK_IMPORT_FAILED"
    if "connect" in text or "miniqmt" in text:
        return "CLIENT_NOT_CONNECTED"
    return "CONNECT_FAILED"


def bridge_status(*, timeout: int = 8) -> dict[str, Any]:
    runtime_path = bridge.python_path()
    base = {
        "configured": bridge.is_configured(),
        "python_path": str(runtime_path),
        "python_exists": runtime_path.is_file(),
        "connected": False,
        "error_code": None,
        "error": None,
    }
    if not base["configured"]:
        base["error_code"] = "SDK_RUNTIME_MISSING"
        base["error"] = "QMT Python runtime or worker is missing"
        return base
    try:
        result = bridge.ping(timeout=max(1, int(timeout)))
    except Exception as exc:  # bridge returns normalized operational failures
        base["error_code"] = _classify_bridge_error(exc)
        base["error"] = str(exc)[:500]
        return base
    rows = result.get("rows") or []
    ping_row = rows[0] if rows and isinstance(rows[0], dict) else {}
    base.update(
        {
            "connected": True,
            "connection_port": ping_row.get("connection_port"),
            "sdk_module": ping_row.get("sdk_module"),
            "sdk_version": ping_row.get("sdk_version"),
            "transport": result.get("transport") or ping_row.get("transport"),
        }
    )
    return base


def diagnostics(*, timeout: int = 8, cache_seconds: int = 15, force: bool = False) -> dict[str, Any]:
    global _diagnostics_cache
    now = time.monotonic()
    with _cache_lock:
        if not force and _diagnostics_cache and now - _diagnostics_cache[0] <= cache_seconds:
            return dict(_diagnostics_cache[1])

    client = client_status()
    sdk = bridge_status(timeout=timeout)
    if not client["installed"]:
        status = "error"
    elif not client["running"] or not sdk["connected"]:
        status = "warn"
    else:
        status = "ok"
    result = {
        "status": status,
        "provider": PROVIDER_ID,
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "client": client,
        "sdk": sdk,
    }
    with _cache_lock:
        _diagnostics_cache = (time.monotonic(), dict(result))
    return result


def capabilities(*, timeout: int = 12, cache_seconds: int = 60, force: bool = False) -> dict[str, Any]:
    global _capabilities_cache
    now = time.monotonic()
    with _cache_lock:
        if not force and _capabilities_cache and now - _capabilities_cache[0] <= cache_seconds:
            return dict(_capabilities_cache[1])
    try:
        result = bridge.capabilities(timeout=timeout)
    except Exception as exc:
        result = {
            "ok": False,
            "provider": PROVIDER_ID,
            "status": "error",
            "error_code": _classify_bridge_error(exc),
            "error": str(exc)[:500],
            "rows": [],
        }
    else:
        result = {**result, "provider": PROVIDER_ID, "status": "ok"}
    with _cache_lock:
        _capabilities_cache = (time.monotonic(), dict(result))
    return result


def core_probe(*, timeout: int = 30, cache_seconds: int = 300, force: bool = False) -> dict[str, Any]:
    global _core_probe_cache
    now = time.monotonic()
    with _cache_lock:
        if not force and _core_probe_cache and now - _core_probe_cache[0] <= cache_seconds:
            return dict(_core_probe_cache[1])
    try:
        result = bridge.probe_core(timeout=timeout)
    except Exception as exc:
        result = {
            "ok": False,
            "provider": PROVIDER_ID,
            "status": "error",
            "error_code": _classify_bridge_error(exc),
            "error": str(exc)[:500],
            "rows": [],
        }
    else:
        failed = [row for row in result.get("rows", []) if row.get("status") == "FAILED"]
        unavailable = [row for row in result.get("rows", []) if row.get("status") != "SUPPORTED"]
        result = {
            **result,
            "provider": PROVIDER_ID,
            "status": "error" if failed else ("warn" if unavailable else "ok"),
        }
    with _cache_lock:
        _core_probe_cache = (time.monotonic(), dict(result))
    return result
