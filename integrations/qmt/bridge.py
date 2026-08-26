from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Iterable
from urllib import error as urllib_error
from urllib import request as urllib_request

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PYTHON = ROOT / "runtime" / "qmt-py313" / "Scripts" / "python.exe"
WORKER = Path(__file__).with_name("worker.py")
DEFAULT_GATEWAY_URL = "http://127.0.0.1:18765"


class QmtBridgeError(RuntimeError):
    """Raised when the QMT bridge cannot return usable data."""


def python_path() -> Path:
    raw = (os.environ.get("QMT_PYTHON") or "").strip()
    return Path(raw) if raw else DEFAULT_PYTHON


def is_configured() -> bool:
    gateway_enabled = os.environ.get("QMT_GATEWAY_ENABLED", "1").strip().lower()
    if gateway_enabled not in {"0", "false", "no", "off"}:
        return True
    return python_path().exists() and WORKER.exists()


def is_probe_runtime_configured() -> bool:
    """Return whether this host explicitly owns a probe-capable QMT runtime.

    ``is_configured`` intentionally treats the default localhost gateway as a
    usable execution fallback.  Health endpoints need a stricter distinction:
    a Linux API host with neither an explicitly configured gateway nor a local
    Windows interpreter is an external-collector reader and must not report a
    missing ``python.exe`` as a QMT outage.
    """

    gateway_enabled = os.environ.get("QMT_GATEWAY_ENABLED", "1").strip().lower()
    gateway_explicit = bool(
        str(os.environ.get("QMT_GATEWAY_URL") or "").strip()
        or str(os.environ.get("QMT_GATEWAY_REQUIRED") or "").strip() == "1"
    )
    if (
        gateway_enabled not in {"0", "false", "no", "off"}
        and gateway_explicit
    ):
        return True
    return python_path().exists() and WORKER.exists()


def _decode_output(text: str) -> dict[str, Any]:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise json.JSONDecodeError("no json object in worker output", text, 0)


def gateway_url() -> str:
    return (os.environ.get("QMT_GATEWAY_URL") or DEFAULT_GATEWAY_URL).rstrip("/")


def _run_gateway(payload: dict[str, Any], *, timeout: int) -> dict[str, Any] | None:
    if os.environ.get("QMT_GATEWAY_ENABLED", "1").strip().lower() in {"0", "false", "no", "off"}:
        return None
    body = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    token = (os.environ.get("QMT_GATEWAY_TOKEN") or "").strip()
    if token:
        headers["X-QMT-Token"] = token
    req = urllib_request.Request(f"{gateway_url()}/call", data=body, headers=headers, method="POST")
    gateway_timeout = min(float(timeout), float(os.environ.get("QMT_GATEWAY_REQUEST_TIMEOUT", "30") or 30))
    try:
        with urllib_request.urlopen(req, timeout=max(0.5, gateway_timeout)) as response:
            result = json.loads(response.read().decode("utf-8"))
    except (urllib_error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError):
        if os.environ.get("QMT_GATEWAY_REQUIRED", "0") == "1":
            raise QmtBridgeError("Guojin QMT persistent gateway is unavailable")
        return None
    if not result.get("ok"):
        raise QmtBridgeError(str(result.get("error") or "Guojin QMT gateway call failed"))
    return result


def _run(payload: dict[str, Any], *, timeout: int | None = None) -> dict[str, Any]:
    effective_timeout = timeout or int(os.environ.get("QMT_TIMEOUT", "180"))
    gateway_result = _run_gateway(payload, timeout=effective_timeout)
    if gateway_result is not None:
        return gateway_result

    # The production Linux host can use the authenticated Windows QMT
    # gateway without carrying a Windows python.exe.  Only the local fallback
    # requires the bundled QMT interpreter and worker.
    python = python_path()
    if not python.exists():
        raise QmtBridgeError(f"QMT Python runtime not found: {python}")
    if not WORKER.exists():
        raise QmtBridgeError(f"QMT worker not found: {WORKER}")

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("QMT_ENABLE_HELLO", "0")

    try:
        proc = subprocess.run(
            [str(python), str(WORKER)],
            input=json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            cwd=str(ROOT),
            env=env,
            timeout=effective_timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise QmtBridgeError(f"QMT worker timed out after {effective_timeout}s") from exc

    try:
        result = _decode_output(proc.stdout or "")
    except json.JSONDecodeError as exc:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise QmtBridgeError(f"QMT worker returned invalid output: {detail[:500]}") from exc

    if proc.returncode != 0 or not result.get("ok"):
        detail = str(result.get("error") or (proc.stderr or proc.stdout or "").strip())
        raise QmtBridgeError(detail)
    return result


def _as_codes(items: Iterable[str] | str) -> list[str]:
    if isinstance(items, str):
        return [items]
    return [str(item).strip() for item in items if str(item).strip()]


def ping(*, timeout: int | None = None) -> dict[str, Any]:
    return _run({"action": "ping"}, timeout=timeout)


def capabilities(*, timeout: int | None = None) -> dict[str, Any]:
    return _run({"action": "capabilities"}, timeout=timeout)


def probe_core(*, timeout: int | None = None) -> dict[str, Any]:
    return _run({"action": "probe_core"}, timeout=timeout)


def refresh_reference_data(
    operations: Iterable[str] | str | None = None,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    operation_list = [] if operations is None else _as_codes(operations)
    return _run({"action": "refresh_reference_data", "operations": operation_list}, timeout=timeout)


def kline(
    stock_codes: Iterable[str] | str,
    *,
    start_date: str,
    end_date: str,
    dividend_type: str = "front",
    batch_size: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "kline",
            "stock_codes": _as_codes(stock_codes),
            "start_date": start_date,
            "end_date": end_date,
            "dividend_type": dividend_type,
            "batch_size": batch_size,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def minute(
    stock_codes: Iterable[str] | str,
    *,
    trade_date: str,
    start_date: str | None = None,
    end_date: str | None = None,
    count: int = 0,
    fill_data: bool = True,
    batch_size: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "minute",
            "stock_codes": _as_codes(stock_codes),
            "trade_date": trade_date,
            "start_date": start_date or trade_date,
            "end_date": end_date or trade_date,
            "count": count,
            "fill_data": fill_data,
            "batch_size": batch_size,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def current(
    stock_codes: Iterable[str] | str,
    *,
    batch_size: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "current",
            "stock_codes": _as_codes(stock_codes),
            "batch_size": batch_size,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def tick(
    stock_codes: Iterable[str] | str,
    *,
    count: int = 100,
    batch_size: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "tick",
            "stock_codes": _as_codes(stock_codes),
            "count": count,
            "batch_size": batch_size,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def flow_daily(
    stock_codes: Iterable[str] | str,
    *,
    start_date: str,
    end_date: str,
    batch_size: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "flow_daily",
            "stock_codes": _as_codes(stock_codes),
            "start_date": start_date,
            "end_date": end_date,
            "batch_size": batch_size,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def flow_min(
    stock_codes: Iterable[str] | str,
    *,
    trade_date: str,
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "flow_min",
            "stock_codes": _as_codes(stock_codes),
            "trade_date": trade_date,
            "start_date": start_date or trade_date,
            "end_date": end_date or trade_date,
            "batch_size": batch_size,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def sector_list(*, timeout: int | None = None) -> pd.DataFrame:
    result = _run({"action": "sector_list"}, timeout=timeout)
    return pd.DataFrame(result.get("rows") or [])


def sector_members(
    sector_name: str,
    *,
    realtime_tag: int | str = -1,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "sector_members",
            "sector_name": str(sector_name),
            "realtime_tag": realtime_tag,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def sector_members_many(
    sector_names: Iterable[str] | str,
    *,
    realtime_tag: int | str = -1,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "sector_members_many",
            "sector_names": _as_codes(sector_names),
            "realtime_tag": realtime_tag,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def historical_contract_catalog(
    *, timeout: int | None = None,
) -> tuple[list[str], pd.DataFrame]:
    result = _run({"action": "historical_contract_catalog"}, timeout=timeout)
    sectors = sorted({
        str(value or "").strip()
        for value in (result.get("expired_sectors") or [])
        if str(value or "").strip()
    })
    rows = pd.DataFrame(result.get("rows") or [])
    if not sectors or rows.empty:
        raise QmtBridgeError("QMT historical contract catalog is incomplete")
    return sectors, rows


def instrument_details(
    stock_codes: Iterable[str] | str,
    *,
    iscomplete: bool = False,
    batch_size: int | None = None,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "instrument_details",
            "stock_codes": _as_codes(stock_codes),
            "iscomplete": bool(iscomplete),
            "batch_size": batch_size,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def index_weight(
    index_code: str,
    *,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "index_weight",
            "index_code": str(index_code).strip(),
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def index_weight_many(
    index_codes: Iterable[str] | str,
    *,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "index_weight_many",
            "index_codes": _as_codes(index_codes),
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])


def trading_calendar(
    market: str = "SH",
    *,
    start_date: str,
    end_date: str,
    timeout: int | None = None,
) -> pd.DataFrame:
    result = _run(
        {
            "action": "trading_calendar",
            "market": str(market or "SH").strip().upper() or "SH",
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=timeout,
    )
    return pd.DataFrame(result.get("rows") or [])
