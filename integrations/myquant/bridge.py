# -*- coding: utf-8 -*-
"""Bridge the main app to the MyQuant SDK running on its compatible Python.

The official gm SDK currently ships Windows cp36 binaries in this project
setup. The application itself can keep using its normal Python; this module
delegates SDK calls to a small Python 3.6 worker and exchanges JSON.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PYTHON = ROOT / "runtime" / "emquant-py36" / "python.exe"
WORKER = Path(__file__).with_name("worker.py")
UPPER_LIMIT_HISTORY_ACTION = "history_instruments_upper_limit"
UPPER_LIMIT_HISTORY_FIELDS = (
    "symbol,trade_date,pre_close,upper_limit,lower_limit,is_suspended"
)
UPPER_LIMIT_HISTORY_COLUMNS = tuple(UPPER_LIMIT_HISTORY_FIELDS.split(","))
SHANGHAI_TIMEZONE_NAME = "Asia/Shanghai"
_STRICT_GM_SYMBOL = re.compile(r"^(?:SHSE\.6[0-9]{5}|SZSE\.[03][0-9]{5})$")


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


def _as_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def _redact_worker_text(value: Any, *, token: str) -> str:
    text = _as_bytes(value).decode("utf-8", errors="replace")
    return text.replace(token, "<redacted>") if token else text


def _run_capture(
    payload: dict[str, Any],
    *,
    timeout: int | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    token = _get_token()
    if not token:
        raise MyQuantBridgeError("GM_TOKEN is not configured")

    python = _python_path()
    if not python.exists():
        raise MyQuantBridgeError(f"MyQuant Python runtime not found: {python}")
    if not WORKER.exists():
        raise MyQuantBridgeError(f"MyQuant worker not found: {WORKER}")

    canonical_request_json = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    request_bytes = canonical_request_json.encode("utf-8")
    request_sha256 = hashlib.sha256(request_bytes).hexdigest()
    worker_sha256 = hashlib.sha256(WORKER.read_bytes()).hexdigest()

    env = os.environ.copy()
    env["GM_TOKEN"] = token

    try:
        proc = subprocess.run(
            [str(python), str(WORKER)],
            input=request_bytes,
            capture_output=True,
            cwd=str(ROOT),
            env=env,
            timeout=timeout or int(os.environ.get("MYQUANT_TIMEOUT", "120")),
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise MyQuantBridgeError("MyQuant worker timed out") from None
    except OSError:
        raise MyQuantBridgeError("MyQuant worker could not be started") from None
    if hashlib.sha256(WORKER.read_bytes()).hexdigest() != worker_sha256:
        raise MyQuantBridgeError("MyQuant worker changed while the request was running")

    stdout_bytes = _as_bytes(proc.stdout)
    stderr_bytes = _as_bytes(proc.stderr)
    token_bytes = token.encode("utf-8")
    if token_bytes and (token_bytes in stdout_bytes or token_bytes in stderr_bytes):
        raise MyQuantBridgeError("MyQuant worker output contained a configured credential")
    if proc.returncode != 0:
        stdout_text = _redact_worker_text(stdout_bytes, token=token).strip()
        stderr_text = _redact_worker_text(stderr_bytes, token=token).strip()
        detail = ""
        try:
            failure = json.loads(stdout_text)
            if isinstance(failure, dict):
                detail = str(failure.get("error") or "").strip()
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
        detail = (detail or stderr_text or stdout_text or "no error detail")[:1000]
        raise MyQuantBridgeError(f"MyQuant worker failed with code {proc.returncode}: {detail}")

    try:
        raw_stdout = stdout_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MyQuantBridgeError("MyQuant worker stdout is not UTF-8") from exc
    output = raw_stdout.strip()
    if not output:
        raise MyQuantBridgeError("MyQuant worker returned empty output")
    try:
        result = json.loads(output)
    except json.JSONDecodeError as exc:
        safe_output = _redact_worker_text(output, token=token)
        raise MyQuantBridgeError(
            f"MyQuant worker returned invalid JSON: {safe_output[:500]}"
        ) from exc
    if not isinstance(result, dict):
        raise MyQuantBridgeError("MyQuant worker JSON is not an object")
    if result.get("ok") is not True:
        detail = _redact_worker_text(
            str(result.get("error") or "MyQuant worker failed"), token=token
        )
        raise MyQuantBridgeError(detail)
    return result, {
        "raw_stdout": raw_stdout,
        "raw_stdout_sha256": hashlib.sha256(stdout_bytes).hexdigest(),
        "canonical_request_json": canonical_request_json,
        "canonical_request_sha256": request_sha256,
        "worker_sha256": worker_sha256,
    }


def _run(payload: dict[str, Any], *, timeout: int | None = None) -> dict[str, Any]:
    result, _capture = _run_capture(payload, timeout=timeout)
    return result


def _canonical_date(value: str, *, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise MyQuantBridgeError(f"{field} must use YYYY-MM-DD") from exc
    if text != parsed.isoformat():
        raise MyQuantBridgeError(f"{field} must be canonical YYYY-MM-DD")
    return text


def _strict_evidence_symbols(symbols: Iterable[str] | str) -> list[str]:
    raw_symbols = _as_list(symbols)
    if not raw_symbols:
        raise MyQuantBridgeError("upper-limit evidence requires at least one symbol")
    gm_symbols: list[str] = []
    rejected: list[str] = []
    for raw in raw_symbols:
        mapped = to_gm_symbol(raw)
        if mapped is None or _STRICT_GM_SYMBOL.fullmatch(mapped) is None:
            rejected.append(str(raw))
        else:
            gm_symbols.append(mapped)
    if rejected:
        raise MyQuantBridgeError(
            "upper-limit evidence contains unsupported symbols: "
            + ",".join(rejected)
        )
    if len(gm_symbols) != len(set(gm_symbols)):
        raise MyQuantBridgeError(
            "upper-limit evidence symbols must be unique after normalization"
        )
    return gm_symbols


def _shanghai_timestamp(value: Any, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except ValueError as exc:
        raise MyQuantBridgeError(f"MyQuant {field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(hours=8):
        raise MyQuantBridgeError(f"MyQuant {field} must be Asia/Shanghai aware")
    return parsed


def _positive_price(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MyQuantBridgeError(f"MyQuant {field} must be a positive number")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise MyQuantBridgeError(f"MyQuant {field} must be a positive number")
    return number


def _validate_upper_limit_result(
    result: dict[str, Any],
    *,
    requested_symbols: list[str],
    start_date: str,
    end_date: str,
) -> None:
    expected_keys = {
        "ok",
        "action",
        "fields",
        "columns",
        "requested_symbols",
        "start_date",
        "end_date",
        "request_started_at",
        "captured_at",
        "timezone",
        "sdk_version",
        "python_version",
        "entitlement_status",
        "rows",
        "errors",
    }
    if set(result) != expected_keys:
        raise MyQuantBridgeError("MyQuant upper-limit worker schema is not exact")
    if result.get("action") != UPPER_LIMIT_HISTORY_ACTION:
        raise MyQuantBridgeError("MyQuant upper-limit worker action mismatch")
    if result.get("fields") != UPPER_LIMIT_HISTORY_FIELDS:
        raise MyQuantBridgeError("MyQuant upper-limit worker fields mismatch")
    if result.get("columns") != list(UPPER_LIMIT_HISTORY_COLUMNS):
        raise MyQuantBridgeError("MyQuant upper-limit worker columns mismatch")
    if result.get("errors") != {}:
        raise MyQuantBridgeError("MyQuant upper-limit worker reported symbol errors")
    if result.get("requested_symbols") != requested_symbols:
        raise MyQuantBridgeError("MyQuant upper-limit worker symbol echo mismatch")
    if result.get("start_date") != start_date or result.get("end_date") != end_date:
        raise MyQuantBridgeError("MyQuant upper-limit worker date echo mismatch")
    if result.get("timezone") != SHANGHAI_TIMEZONE_NAME:
        raise MyQuantBridgeError("MyQuant upper-limit worker timezone mismatch")
    if result.get("entitlement_status") != "SUPPORTED":
        raise MyQuantBridgeError("MyQuant upper-limit entitlement is not supported")
    if not str(result.get("sdk_version") or "").strip():
        raise MyQuantBridgeError("MyQuant SDK version is missing")
    if not str(result.get("python_version") or "").strip():
        raise MyQuantBridgeError("MyQuant Python version is missing")

    request_started_at = _shanghai_timestamp(
        result.get("request_started_at"), field="request_started_at"
    )
    captured_at = _shanghai_timestamp(
        result.get("captured_at"), field="captured_at"
    )
    if captured_at < request_started_at:
        raise MyQuantBridgeError("MyQuant captured_at precedes request_started_at")

    rows = result.get("rows")
    if not isinstance(rows, list) or not rows:
        raise MyQuantBridgeError("MyQuant upper-limit worker returned no rows")
    expected_row_keys = set(UPPER_LIMIT_HISTORY_COLUMNS)
    requested = set(requested_symbols)
    observed_symbols: set[str] = set()
    observed_keys: set[tuple[str, str]] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != expected_row_keys:
            raise MyQuantBridgeError("MyQuant upper-limit row schema is not exact")
        symbol = str(row.get("symbol") or "")
        if symbol not in requested:
            raise MyQuantBridgeError("MyQuant upper-limit row contains an extra symbol")
        trade_at = _shanghai_timestamp(row.get("trade_date"), field="trade_date")
        trade_date = trade_at.date().isoformat()
        if not start_date <= trade_date <= end_date:
            raise MyQuantBridgeError("MyQuant upper-limit row date is out of range")
        key = (symbol, trade_date)
        if key in observed_keys:
            raise MyQuantBridgeError("MyQuant upper-limit worker returned a duplicate key")
        observed_keys.add(key)
        observed_symbols.add(symbol)
        pre_close = _positive_price(row.get("pre_close"), field="pre_close")
        upper_limit = _positive_price(row.get("upper_limit"), field="upper_limit")
        lower_limit = _positive_price(row.get("lower_limit"), field="lower_limit")
        if not upper_limit > pre_close > lower_limit:
            raise MyQuantBridgeError("MyQuant upper/lower price relationship is invalid")
        suspended = row.get("is_suspended")
        if isinstance(suspended, bool) or not isinstance(suspended, int) or suspended not in {0, 1}:
            raise MyQuantBridgeError("MyQuant is_suspended must be integer 0 or 1")
    if observed_symbols != requested:
        raise MyQuantBridgeError("MyQuant upper-limit response silently omitted a symbol")


def upper_limit_history_evidence(
    symbols: Iterable[str] | str,
    *,
    start_date: str,
    end_date: str,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Return fixed-schema historical price limits plus transport evidence."""

    gm_symbols = _strict_evidence_symbols(symbols)
    start = _canonical_date(start_date, field="start_date")
    end = _canonical_date(end_date, field="end_date")
    if start > end:
        raise MyQuantBridgeError("start_date must not be after end_date")
    result, capture = _run_capture(
        {
            "action": UPPER_LIMIT_HISTORY_ACTION,
            "symbols": gm_symbols,
            "start_date": start,
            "end_date": end,
        },
        timeout=timeout,
    )
    _validate_upper_limit_result(
        result,
        requested_symbols=gm_symbols,
        start_date=start,
        end_date=end,
    )
    return dict(result) | capture


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
