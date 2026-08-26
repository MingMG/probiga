#!/usr/bin/env python3
"""One-shot native-QMT worker for one exact transactioncount1m batch.

This module deliberately has no application/database dependencies.  It is
executed by the QMT Python interpreter from the checked-out release, downloads
the requested historical feature without swallowing errors, and returns the
raw cumulative net-flow fields for one date.  The Linux/application-side
publisher performs catalog, grid, accounting and database validation.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import time
import traceback
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
import xtquant
from xtquant import xtdata


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.runtime import connect_xtdata  # noqa: E402


PERIOD = "transactioncount1m"
FIELDS = (
    "netInflowMostAmount",
    "netInflowBigAmount",
    "netInflowMediumAmount",
    "netInflowSmallAmount",
)
QMT_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _json_value(item())
        except Exception:
            pass
    return value


def _date(value: Any) -> str:
    raw = str(value or "").strip()[:10]
    normalized = datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
    if raw != normalized:
        raise ValueError("trade_date must be exact YYYY-MM-DD")
    return normalized


def _codes(values: Any) -> list[str]:
    if not isinstance(values, list):
        raise ValueError("qmt_codes must be a list")
    result = [str(value or "").strip().upper() for value in values]
    if (
        not result
        or len(result) != len(set(result))
        or any(QMT_CODE.fullmatch(value) is None for value in result)
    ):
        raise ValueError("qmt_codes are empty, duplicated or malformed")
    return sorted(result)


def _timestamp(value: Any) -> pd.Timestamp:
    if isinstance(value, (datetime, pd.Timestamp)):
        parsed = pd.Timestamp(value)
        if parsed.tzinfo is not None:
            parsed = parsed.tz_convert(SHANGHAI).tz_localize(None)
        return parsed
    raw = str(value or "").strip()
    digits = "".join(character for character in raw if character.isdigit())
    # QMT builds have emitted both compact local wall-clock labels and Unix
    # epochs.  A 14/17-digit wall-clock label must be recognized before the
    # generic epoch-size branches (20260826093000 is not epoch milliseconds).
    if len(digits) >= 14 and digits[:4].isdigit() and 1900 <= int(digits[:4]) <= 2200:
        return pd.to_datetime(
            digits[:14], format="%Y%m%d%H%M%S", errors="coerce"
        )
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            return pd.NaT
        absolute = abs(number)
        unit = (
            "ns" if absolute >= 1e17
            else "us" if absolute >= 1e14
            else "ms" if absolute >= 1e11
            else "s" if absolute >= 1e8
            else ""
        )
        if unit:
            parsed = pd.to_datetime(int(number), unit=unit, errors="coerce", utc=True)
            if pd.isna(parsed):
                return pd.NaT
            return parsed.tz_convert(SHANGHAI).tz_localize(None)
    if digits == raw and len(digits) in {10, 13, 16, 19}:
        unit = {10: "s", 13: "ms", 16: "us", 19: "ns"}[len(digits)]
        parsed = pd.to_datetime(int(digits), unit=unit, errors="coerce", utc=True)
        if pd.isna(parsed):
            return pd.NaT
        return parsed.tz_convert(SHANGHAI).tz_localize(None)
    parsed = pd.to_datetime(value, errors="coerce")
    if not pd.isna(parsed) and parsed.tzinfo is not None:
        parsed = parsed.tz_convert(SHANGHAI).tz_localize(None)
    return parsed


def _frame_rows(qmt_code: str, frame: Any, target: str) -> list[dict[str, Any]]:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return []
    missing = [field for field in FIELDS if field not in frame.columns]
    if missing:
        raise RuntimeError(f"QMT {PERIOD} omitted fields: {missing}")
    clean = frame.copy()
    raw_times = clean["time"] if "time" in clean.columns else pd.Series(clean.index, index=clean.index)
    clean["_trade_time"] = [_timestamp(value) for value in raw_times.tolist()]
    clean = clean.loc[clean["_trade_time"].notna()].copy()
    clean = clean.loc[clean["_trade_time"].dt.strftime("%Y-%m-%d") == target]
    clean = clean.sort_values("_trade_time")
    rows: list[dict[str, Any]] = []
    for _, row in clean.iterrows():
        rows.append(
            {
                "qmt_code": qmt_code,
                "stock_code": qmt_code.split(".", 1)[0],
                "trade_time": row["_trade_time"].strftime("%Y-%m-%d %H:%M:%S"),
                "netInflowMostAmount": _json_value(row["netInflowMostAmount"]),
                "netInflowBigAmount": _json_value(row["netInflowBigAmount"]),
                "netInflowMediumAmount": _json_value(row["netInflowMediumAmount"]),
                "netInflowSmallAmount": _json_value(row["netInflowSmallAmount"]),
            }
        )
    return rows


def _code_set_hash(codes: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(codes)).encode("ascii")).hexdigest()


def dispatch(payload: Mapping[str, Any]) -> dict[str, Any]:
    if payload.get("action") != "flow_min_exact":
        raise ValueError("unsupported action")
    target = _date(payload.get("trade_date"))
    codes = _codes(payload.get("qmt_codes"))
    connection_port = connect_xtdata(xtdata)
    start_time = target.replace("-", "") + "000000"
    end_time = target.replace("-", "") + "235959"
    downloader = getattr(xtdata, "download_history_data2", None)
    single = getattr(xtdata, "download_history_data", None)
    if callable(downloader):
        downloader(
            stock_list=codes,
            period=PERIOD,
            start_time=start_time,
            end_time=end_time,
        )
        download_method = "download_history_data2"
    elif callable(single):
        for code in codes:
            single(code, PERIOD, start_time, end_time)
        download_method = "download_history_data"
    else:
        raise RuntimeError("QMT history download API is unavailable")
    wait_seconds = max(0.0, float(payload.get("history_wait_seconds") or 1.0))
    if wait_seconds:
        time.sleep(wait_seconds)
    data = xtdata.get_market_data_ex(
        field_list=[],
        stock_list=codes,
        period=PERIOD,
        start_time=start_time,
        end_time=end_time,
        count=-1,
        dividend_type="none",
        fill_data=True,
    )
    if not isinstance(data, dict):
        raise RuntimeError("QMT flow response is not a code/frame mapping")
    unexpected = sorted(set(map(str, data)) - set(codes))
    if unexpected:
        raise RuntimeError(f"QMT flow response contains unexpected codes: {unexpected[:20]}")
    rows: list[dict[str, Any]] = []
    for code in codes:
        rows.extend(_frame_rows(code, data.get(code), target))
    return {
        "ok": True,
        "provider": "gj_qmt",
        "period": PERIOD,
        "trade_date": target,
        "requested_qmt_code_count": len(codes),
        "requested_qmt_code_set_hash": _code_set_hash(codes),
        "row_count": len(rows),
        "rows": rows,
        "source_identity": {
            "connection_port": connection_port,
            "sdk_module": str(getattr(xtdata, "__file__", "") or ""),
            "sdk_version": str(getattr(xtquant, "__version__", "") or "unknown"),
            "download_method": download_method,
            "count": -1,
            "fill_data": True,
            "fields": list(FIELDS),
        },
    }


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        if not isinstance(payload, Mapping):
            raise ValueError("payload must be an object")
        result = dispatch(payload)
        print(json.dumps(result, ensure_ascii=True, separators=(",", ":"), default=_json_value))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
                    "traceback": traceback.format_exc(limit=10),
                },
                ensure_ascii=True,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
