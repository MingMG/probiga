# -*- coding: utf-8 -*-
"""Isolated worker for the official gm SDK.

Input: a single JSON object on stdin.
Output: a single JSON object on stdout.
"""
from __future__ import print_function

import datetime as _dt
import json
import math
import os
import platform
import re
import sys

import pandas as pd
from gm.api import current, get_history_instruments, history, set_token
from gm.__version__ import __version__ as GM_SDK_VERSION


UPPER_LIMIT_HISTORY_ACTION = "history_instruments_upper_limit"
UPPER_LIMIT_HISTORY_FIELDS = (
    "symbol,trade_date,pre_close,upper_limit,lower_limit,is_suspended"
)
UPPER_LIMIT_HISTORY_COLUMNS = tuple(UPPER_LIMIT_HISTORY_FIELDS.split(","))
SHANGHAI_TIMEZONE_NAME = "Asia/Shanghai"
SHANGHAI_TIMEZONE = _dt.timezone(
    _dt.timedelta(hours=8),
    SHANGHAI_TIMEZONE_NAME,
)
_STRICT_GM_SYMBOL = re.compile(r"^(?:SHSE\.6[0-9]{5}|SZSE\.[03][0-9]{5})$")


def _json_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (pd.Timestamp, _dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if hasattr(value, "item"):
        try:
            return _json_value(value.item())
        except Exception:
            pass
    return value


def _records(data):
    if data is None:
        return []
    if isinstance(data, pd.DataFrame):
        rows = data.to_dict("records")
    elif isinstance(data, list):
        rows = data
    else:
        rows = [data]
    return [{str(k): _json_value(v) for k, v in row.items()} for row in rows if isinstance(row, dict)]


def _safe_error_text(exc):
    """Format an error without ever echoing the credential from this process."""

    text = "{}: {}".format(type(exc).__name__, exc)
    token = os.environ.get("GM_TOKEN") or ""
    if token:
        text = text.replace(token, "<redacted>")
    return text


def _history(payload):
    symbols = payload.get("symbols") or []
    fields = payload.get("fields") or "symbol,eob,open,high,low,close,volume,amount"
    frequency = payload.get("frequency") or "1d"
    start_time = payload.get("start_time") or ""
    end_time = payload.get("end_time") or ""
    adjust = payload.get("adjust")
    rows = []
    errors = {}
    for symbol in symbols:
        try:
            df = history(
                symbol=symbol,
                frequency=frequency,
                start_time=start_time,
                end_time=end_time,
                fields=fields,
                adjust=adjust,
                df=True,
            )
            rows.extend(_records(df))
        except Exception as exc:
            errors[symbol] = _safe_error_text(exc)
    return {"rows": rows, "errors": errors}


def _current(payload):
    symbols = payload.get("symbols") or []
    fields = payload.get("fields") or ""
    data = current(symbols=",".join(symbols), fields=fields)
    return {"rows": _records(data), "errors": {}}


def _shanghai_now_text():
    return _dt.datetime.now(SHANGHAI_TIMEZONE).isoformat()


def _canonical_date(value, name):
    text = str(value or "").strip()
    try:
        parsed = _dt.datetime.strptime(text, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        raise ValueError("{} must use YYYY-MM-DD".format(name))
    canonical = parsed.strftime("%Y-%m-%d")
    if text != canonical:
        raise ValueError("{} must be canonical YYYY-MM-DD".format(name))
    return canonical


def _history_instruments_upper_limit(payload):
    """Read the one fixed historical price-limit dataset.

    ``fields`` is intentionally not read from ``payload``.  This action is a
    formal evidence transport, not the SDK's generic query surface, so callers
    cannot weaken or broaden its schema.
    """

    symbols = payload.get("symbols")
    if (
        not isinstance(symbols, list)
        or not symbols
        or any(not isinstance(item, str) or not item.strip() for item in symbols)
    ):
        raise ValueError("symbols must be a non-empty string list")
    symbols = [item.strip().upper() for item in symbols]
    if len(symbols) != len(set(symbols)):
        raise ValueError("symbols must not contain duplicates")
    if any(_STRICT_GM_SYMBOL.match(item) is None for item in symbols):
        raise ValueError("symbols contain an unsupported GM stock symbol")

    start_date = _canonical_date(payload.get("start_date"), "start_date")
    end_date = _canonical_date(payload.get("end_date"), "end_date")
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    request_started_at = _shanghai_now_text()
    data = get_history_instruments(
        symbols=symbols,
        fields=UPPER_LIMIT_HISTORY_FIELDS,
        start_date=start_date,
        end_date=end_date,
        df=True,
    )
    # The timestamp is taken immediately after the SDK has materialized the
    # complete response.  It is therefore a conservative FIRST_OBSERVED time.
    captured_at = _shanghai_now_text()
    rows = _records(data)
    if not isinstance(data, pd.DataFrame) or data.empty:
        # gm.api.get_history_instruments catches its own transport/permission
        # errors and returns an empty list.  Treat that value as an error rather
        # than claiming that the entitlement is supported.
        raise RuntimeError("get_history_instruments returned no evidence rows")
    actual_columns = tuple(data.columns) if isinstance(data, pd.DataFrame) else ()
    if actual_columns != UPPER_LIMIT_HISTORY_COLUMNS:
        raise ValueError(
            "get_history_instruments returned unexpected columns: {}".format(
                list(actual_columns)
            )
        )
    expected_columns = set(UPPER_LIMIT_HISTORY_COLUMNS)
    if any(set(row) != expected_columns for row in rows):
        raise ValueError("get_history_instruments returned an invalid row schema")
    observed_symbols = set()
    observed_keys = set()
    for row in rows:
        symbol = row.get("symbol")
        if symbol not in symbols:
            raise ValueError("get_history_instruments returned an extra symbol")
        trade_date = pd.Timestamp(row.get("trade_date"))
        if pd.isna(trade_date):
            raise ValueError("get_history_instruments returned an invalid trade_date")
        trade_date_text = trade_date.strftime("%Y-%m-%d")
        if trade_date_text < start_date or trade_date_text > end_date:
            raise ValueError("get_history_instruments returned an out-of-range trade_date")
        key = (symbol, trade_date_text)
        if key in observed_keys:
            raise ValueError("get_history_instruments returned a duplicate symbol/date")
        observed_keys.add(key)
        observed_symbols.add(symbol)
    if observed_symbols != set(symbols):
        raise RuntimeError("get_history_instruments silently omitted a requested symbol")

    return {
        "action": UPPER_LIMIT_HISTORY_ACTION,
        "fields": UPPER_LIMIT_HISTORY_FIELDS,
        "columns": list(UPPER_LIMIT_HISTORY_COLUMNS),
        "requested_symbols": symbols,
        "start_date": start_date,
        "end_date": end_date,
        "request_started_at": request_started_at,
        "captured_at": captured_at,
        "timezone": SHANGHAI_TIMEZONE_NAME,
        "sdk_version": str(GM_SDK_VERSION),
        "python_version": platform.python_version(),
        # SUPPORTED is emitted only after a successful SDK response and schema
        # validation.  Failure responses never claim entitlement support.
        "entitlement_status": "SUPPORTED",
        "rows": rows,
        "errors": {},
    }


def main():
    try:
        token = os.environ.get("GM_TOKEN") or ""
        if not token:
            raise RuntimeError("GM_TOKEN is not configured")
        set_token(token)
        payload = json.loads(sys.stdin.read() or "{}")
        action = payload.get("action")
        if action == "history":
            result = _history(payload)
        elif action == "current":
            result = _current(payload)
        elif action == UPPER_LIMIT_HISTORY_ACTION:
            result = _history_instruments_upper_limit(payload)
        elif action == "ping":
            result = {"rows": [{"status": "ok"}], "errors": {}}
        else:
            raise ValueError("unsupported action: {}".format(action))
        result["ok"] = True
        print(json.dumps(result, ensure_ascii=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": _safe_error_text(exc),
                },
                ensure_ascii=True,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
