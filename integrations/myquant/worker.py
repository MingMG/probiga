# -*- coding: utf-8 -*-
"""Python 3.6 worker for the official gm SDK.

Input: a single JSON object on stdin.
Output: a single JSON object on stdout.
"""
from __future__ import print_function

import datetime as _dt
import json
import math
import os
import sys
import traceback

import pandas as pd
from gm.api import current, history, set_token


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
            errors[symbol] = "{}: {}".format(type(exc).__name__, exc)
    return {"rows": rows, "errors": errors}


def _current(payload):
    symbols = payload.get("symbols") or []
    fields = payload.get("fields") or ""
    data = current(symbols=",".join(symbols), fields=fields)
    return {"rows": _records(data), "errors": {}}


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
                    "error": "{}: {}".format(type(exc).__name__, exc),
                    "traceback": traceback.format_exc(),
                },
                ensure_ascii=True,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
