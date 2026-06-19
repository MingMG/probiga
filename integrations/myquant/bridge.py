# -*- coding: utf-8 -*-
"""Bridge the main app to the MyQuant SDK running on its compatible Python.

The official gm SDK currently ships Windows cp36 binaries in this project
setup. The application itself can keep using its normal Python; this module
delegates SDK calls to a small Python 3.6 worker and exchanges JSON.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PYTHON = ROOT / "runtime" / "emquant-py36" / "python.exe"
WORKER = Path(__file__).with_name("worker.py")


class MyQuantBridgeError(RuntimeError):
    """Raised when the MyQuant bridge cannot return usable data."""


def _read_windows_user_env(name: str) -> str:
    if sys.platform != "win32":
        return ""
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, _ = winreg.QueryValueEx(key, name)
        return str(value or "").strip()
    except OSError:
        return ""


def _get_token() -> str:
    for name in ("GM_TOKEN", "MYQUANT_TOKEN", "EMQUANT_TOKEN", "EASTMONEY_QUANT_TOKEN"):
        value = (os.environ.get(name) or _read_windows_user_env(name)).strip()
        if value:
            return value
    return ""


def _python_path() -> Path:
    raw = (os.environ.get("MYQUANT_PYTHON") or os.environ.get("EMQUANT_PYTHON") or "").strip()
    return Path(raw) if raw else DEFAULT_PYTHON


def is_configured() -> bool:
    return bool(_get_token()) and _python_path().exists() and WORKER.exists()


def to_gm_symbol(code: str) -> str | None:
    """Map a project stock code to a Goldminer symbol.

    The installed SDK version returns data for SHSE/SZSE A-share symbols. It
    returned no data for BJSE/BSE during local verification, so Beijing codes
    intentionally return None and should continue through existing fallbacks.
    """
    text = str(code or "").strip().upper()
    if not text:
        return None
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return None
    if digits.startswith("6"):
        return f"SHSE.{digits}"
    if digits.startswith(("0", "3")):
        return f"SZSE.{digits}"
    return None


def to_stock_code(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    return text.split(".", 1)[1] if "." in text else text


def _as_list(items: Iterable[str] | str) -> list[str]:
    if isinstance(items, str):
        return [items]
    return [str(item) for item in items]


def _run(payload: dict[str, Any], *, timeout: int | None = None) -> dict[str, Any]:
    token = _get_token()
    if not token:
        raise MyQuantBridgeError("GM_TOKEN is not configured")

    python = _python_path()
    if not python.exists():
        raise MyQuantBridgeError(f"MyQuant Python runtime not found: {python}")
    if not WORKER.exists():
        raise MyQuantBridgeError(f"MyQuant worker not found: {WORKER}")

    env = os.environ.copy()
    env["GM_TOKEN"] = token

    proc = subprocess.run(
        [str(python), str(WORKER)],
        input=json.dumps(payload, ensure_ascii=True),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        cwd=str(ROOT),
        env=env,
        timeout=timeout or int(os.environ.get("MYQUANT_TIMEOUT", "120")),
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise MyQuantBridgeError(f"MyQuant worker failed with code {proc.returncode}: {detail}")

    output = (proc.stdout or "").strip()
    if not output:
        raise MyQuantBridgeError("MyQuant worker returned empty output")
    try:
        result = json.loads(output)
    except json.JSONDecodeError as exc:
        raise MyQuantBridgeError(f"MyQuant worker returned invalid JSON: {output[:500]}") from exc
    if not result.get("ok"):
        raise MyQuantBridgeError(str(result.get("error") or "MyQuant worker failed"))
    return result


def history(
    symbols: Iterable[str] | str,
    *,
    frequency: str,
    start_time: str,
    end_time: str,
    fields: str = "symbol,eob,open,high,low,close,volume,amount",
    adjust: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    gm_symbols = [s for s in (to_gm_symbol(item) for item in _as_list(symbols)) if s]
    if not gm_symbols:
        return pd.DataFrame()

    result = _run(
        {
            "action": "history",
            "symbols": gm_symbols,
            "frequency": frequency,
            "start_time": start_time,
            "end_time": end_time,
            "fields": fields,
            "adjust": adjust,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def current(
    symbols: Iterable[str] | str,
    *,
    fields: str = "symbol,price,open,high,low,cum_volume,cum_amount,created_at",
    timeout: int | None = None,
) -> pd.DataFrame:
    gm_symbols = [s for s in (to_gm_symbol(item) for item in _as_list(symbols)) if s]
    if not gm_symbols:
        return pd.DataFrame()

    result = _run(
        {
            "action": "current",
            "symbols": gm_symbols,
            "fields": fields,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])
