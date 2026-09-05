# coding:gbk
"""One-shot FULL QMT native strategy probe, not a production data collector.

Open as a separate built-in Python model; do not replace the live bridge.
No orders, downloads, subscriptions, project imports or database writes.
init reads only the current native cache. Empty rows do not prove entitlement
is missing. Results never authorize a trading decision or historical cutoff.
"""

import json


def _json_value(value):
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else None
    if isinstance(value, dict):
        return dict((str(key), _json_value(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _json_value(tolist())
    scalar = getattr(value, "item", None)
    if callable(scalar):
        return _json_value(scalar())
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    raise ValueError("Unsupported native scalar: " + type(value).__name__)


def init(C):
    symbols = ["000001.SZ", "600000.SH"]
    results = {}
    # The full QMT installation may lack pandas; its existing live bridge
    # already uses the native _ori reader to avoid that optional wrapper.
    reader = getattr(C, "get_market_data_ex_ori", None)
    method = "ContextInfo.get_market_data_ex_ori"
    if not callable(reader):
        reader = getattr(C, "get_market_data_ex", None)
        method = "ContextInfo.get_market_data_ex"
    for period in ("stoppricedata", "1d"):
        try:
            if not callable(reader):
                raise ValueError("Native ContextInfo reader is absent")
            data = reader([], symbols, period=period, start_time="20260903",
                          end_time="20260904", count=-1, dividend_type="none",
                          fill_data=False, subscribe=False)
            if not isinstance(data, dict) or any(code not in symbols for code in data):
                raise ValueError("Native result is not the requested stock map")
            # _ori is not a DataFrame API. Keep its native stock->object
            # structure unchanged; parse fields only after inspecting it.
            raw = _json_value(data)
            if len(json.dumps(raw, ensure_ascii=True)) > 65536:
                raise ValueError("Native probe exceeded 64 KiB")
            results[period] = {"status": "OBSERVED", "raw": raw,
                               "stock_value_types": dict((code, type(value).__name__)
                                                         for code, value in data.items())}
        except Exception as exc:
            results[period] = {"status": "ERROR", "error_type": type(exc).__name__,
                               "error": str(exc)[:500]}
    payload = {"probe_only": True, "evidence_status": "UNVERIFIED",
               "source": "full_qmt_builtin_context", "source_method": method,
               "start_time": "20260903", "end_time": "20260904",
               "requested_stock_codes": symbols, "download_history": False,
               "fill_data": False, "subscribe": False, "count": -1,
               "dividend_type": "none", "period_results": results}
    print("PROBIGA_QMT_NATIVE_UPPER_HISTORY_PROBE " + json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


def handlebar(C):
    pass
