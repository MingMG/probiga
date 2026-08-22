# coding:gbk
"""ProBigA data exporter for the standard QMT built-in Python runtime.

The model is read-only.  It exports quotes, local history and reference data
through userdata/probiga_bridge.  It contains no order or cancel API calls.
"""

import gzip
import json
import os
import threading
import time
import traceback


BRIDGE_VERSION = "bigqmt_inner_v2"
MAX_TRACKED_CODES = 280

_lock = threading.RLock()
_bridge_root = None
_config_path = None
_requests_root = None
_responses_root = None
_config_mtime = None
_config = {}
_all_codes = []
_tracked_codes = []
_subscription_id = None
_tracked_quotes = {}
_last_tracked_flush = 0.0
_last_full_refresh = 0.0
_last_request_at = ""
_last_request_action = ""
_last_error = ""
_last_callback_at = ""
_last_callback_ts = 0.0
_callback_batch_count = 0


def _replace_with_retry(temporary, path, retry_seconds=2.0, retry_interval=0.02):
    """Replace a bridge file after transient Windows sharing locks clear."""
    deadline = time.monotonic() + max(0.0, float(retry_seconds))
    while True:
        try:
            os.replace(temporary, path)
            return
        except OSError as exc:
            transient = isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in (5, 32, 33)
            if not transient or time.monotonic() >= deadline:
                raise
            time.sleep(max(0.01, float(retry_interval)))


def _find_bridge_root():
    candidates = []
    script_path = globals().get("__file__")
    if script_path:
        qmt_home = os.path.dirname(os.path.dirname(os.path.abspath(script_path)))
        candidates.append(os.path.join(qmt_home, "userdata", "probiga_bridge"))
    current = os.path.abspath(os.getcwd())
    candidates.append(os.path.join(current, "userdata", "probiga_bridge"))
    candidates.append(os.path.join(os.path.dirname(current), "userdata", "probiga_bridge"))
    for candidate in candidates:
        parent = os.path.dirname(candidate)
        if os.path.isdir(parent):
            if not os.path.isdir(candidate):
                os.makedirs(candidate)
            return candidate
    raise RuntimeError("cannot locate standard QMT userdata directory")


def _json_safe(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return dict((str(key), _json_safe(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return _json_safe(item_method())
        except Exception:
            return str(value)
    return str(value)


def _atomic_write(name, payload):
    path = os.path.join(_bridge_root, name)
    temporary = path + ".%s.tmp" % os.getpid()
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(_json_safe(payload), handle, ensure_ascii=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    _replace_with_retry(temporary, path)


def _atomic_gzip_write(path, payload):
    temporary = path + ".%s.tmp" % os.getpid()
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=5) as handle:
        json.dump(_json_safe(payload), handle, ensure_ascii=True, separators=(",", ":"))
    _replace_with_retry(temporary, path)


def _now_text():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _valid_symbol(value):
    text = str(value or "").strip().upper()
    parts = text.split(".")
    if len(parts) != 2 or len(parts[0]) != 6 or not parts[0].isdigit():
        return ""
    if parts[1] not in ("SH", "SZ", "BJ"):
        return ""
    return text


def _normalize_codes(values, limit=0):
    result = []
    seen = set()
    for value in values or []:
        symbol = _valid_symbol(value)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(symbol)
        if limit and len(result) >= limit:
            break
    return result


def _float(value, default=0.0):
    try:
        number = float(value)
        if number != number:
            return default
        return number
    except Exception:
        return default


def _date_digits(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _time_text(value, period):
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10000000000:
            timestamp = timestamp / 1000.0
        if timestamp > 1000000000:
            rendered = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(timestamp)
            )
            return (
                rendered[:10] + " 15:00:00"
                if period == "1d"
                else rendered
            )
    raw = str(value or "").strip()
    digits = _date_digits(raw)
    if len(digits) >= 14:
        clock = (
            ("15", "00", "00")
            if period == "1d"
            else (digits[8:10], digits[10:12], digits[12:14])
        )
        return "%s-%s-%s %s:%s:%s" % (
            digits[0:4], digits[4:6], digits[6:8],
            clock[0], clock[1], clock[2]
        )
    if len(digits) >= 8:
        suffix = "15:00:00" if period == "1d" else "00:00:00"
        return "%s-%s-%s %s" % (digits[0:4], digits[4:6], digits[6:8], suffix)
    if len(raw) >= 19 and raw[4:5] == "-":
        return (
            raw[:10] + " 15:00:00"
            if period == "1d"
            else raw[:19]
        )
    return ""


def _load_config(force=False):
    global _config_mtime, _config, _all_codes, _tracked_codes
    if not os.path.isfile(_config_path):
        return False
    mtime = os.path.getmtime(_config_path)
    if not force and _config_mtime == mtime:
        return False
    with open(_config_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("watchlist.json must contain an object")
    _config = payload
    _all_codes = _normalize_codes(payload.get("all_codes"))
    _tracked_codes = _normalize_codes(payload.get("tracked_codes"), MAX_TRACKED_CODES)
    _config_mtime = mtime
    return True


def _snapshot_payload(kind, quotes):
    now = time.time()
    return {
        "schema_version": 2,
        "bridge_version": BRIDGE_VERSION,
        "source": "gj_big_qmt_inner",
        "kind": kind,
        "generated_at": _now_text(),
        "generated_ts": now,
        "batch_id": "bigqmt_%s_%s" % (kind, time.strftime("%Y%m%d%H%M%S", time.localtime(now))),
        "quote_count": len(quotes),
        "last_callback_at": _last_callback_at,
        "last_callback_ts": _last_callback_ts,
        "callback_batch_count": _callback_batch_count,
        "quotes": quotes,
    }


def _write_tracked_snapshot(force=False):
    global _last_tracked_flush
    now = time.time()
    interval = max(0.2, float(_config.get("tracked_flush_seconds", 1.0)))
    if not force and now - _last_tracked_flush < interval:
        return
    selected = dict((code, _tracked_quotes[code]) for code in _tracked_codes if code in _tracked_quotes)
    _atomic_write("tracked_quotes.json", _snapshot_payload("tracked", selected))
    _last_tracked_flush = now


def whole_quote_callback(data):
    global _tracked_quotes, _last_error
    global _last_callback_at, _last_callback_ts, _callback_batch_count
    if not isinstance(data, dict):
        return
    try:
        with _lock:
            received_ts = time.time()
            received_at = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(received_ts),
            )
            accepted_count = 0
            for raw_code, raw_tick in data.items():
                code = _valid_symbol(raw_code)
                if code and code in _tracked_codes and isinstance(raw_tick, dict):
                    normalized = _json_safe(raw_tick)
                    if isinstance(normalized, dict):
                        # Retain the first-party callback receipt time per
                        # symbol.  Snapshot publication may repeat an
                        # unchanged book, but it must not manufacture a new
                        # ingress timestamp for that old quote.
                        normalized["_probiga_received_at"] = received_at
                    _tracked_quotes[code] = normalized
                    accepted_count += 1
            if accepted_count:
                _last_callback_at = received_at
                _last_callback_ts = received_ts
                _callback_batch_count += 1
            _write_tracked_snapshot(force=False)
            _last_error = ""
    except Exception:
        _last_error = traceback.format_exc()[-2000:]


def _refresh_subscription(C, force=False):
    global _subscription_id, _tracked_quotes, _last_error
    changed = _load_config(force=force)
    if not changed and not force:
        return
    if _subscription_id is not None:
        try:
            C.unsubscribe_quote(_subscription_id)
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
        _subscription_id = None
    _tracked_quotes = dict((code, tick) for code, tick in _tracked_quotes.items() if code in _tracked_codes)
    if _tracked_codes:
        _subscription_id = C.subscribe_whole_quote(_tracked_codes, callback=whole_quote_callback)
        # Do not seed this stream from ``get_full_tick``.  It is a reconnect
        # cache and an older consumer could otherwise substitute its own
        # receive time for the missing callback marker.  The full-market
        # snapshot already serves display/current data; this tracked stream
        # stays empty until the first genuine subscription callback.
    _write_tracked_snapshot(force=True)


def _refresh_full_snapshot(C):
    global _last_full_refresh
    now = time.time()
    interval = max(5, int(_config.get("full_refresh_seconds", 30)))
    if now - _last_full_refresh < interval:
        return
    batch_size = max(50, int(_config.get("full_batch_size", 800)))
    quotes = {}
    errors = []
    for offset in range(0, len(_all_codes), batch_size):
        batch = _all_codes[offset:offset + batch_size]
        try:
            data = C.get_full_tick(batch)
            if isinstance(data, dict):
                for raw_code, raw_tick in data.items():
                    code = _valid_symbol(raw_code)
                    if code and isinstance(raw_tick, dict):
                        quotes[code] = _json_safe(raw_tick)
        except Exception:
            errors.append(traceback.format_exc()[-500:])
    _atomic_write("full_quotes.json", _snapshot_payload("full", quotes))
    _last_full_refresh = now
    if errors:
        raise RuntimeError("get_full_tick failed for %s batch(es): %s" % (len(errors), errors[-1]))


def _global_function(name):
    function = globals().get(name)
    if callable(function):
        return function
    raise RuntimeError("standard QMT built-in function is unavailable: %s" % name)


def _sector_members(C, sector_name, realtime_tag=-1):
    try:
        values = C.get_stock_list_in_sector(str(sector_name), realtime_tag)
    except TypeError:
        values = C.get_stock_list_in_sector(str(sector_name))
    return _normalize_codes(values or [])


def _sector_list_rows():
    get_list = _global_function("get_sector_list")
    queue = [("", "")]
    seen_nodes = set()
    seen_sectors = set()
    rows = []
    while queue and len(seen_nodes) < 10000:
        node, parent_path = queue.pop(0)
        if node in seen_nodes:
            continue
        seen_nodes.add(node)
        result = get_list(node)
        sectors = result[0] if isinstance(result, (list, tuple)) and len(result) > 0 else []
        folders = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
        node_path = parent_path
        if node:
            node_path = (parent_path + "/" + str(node)).strip("/")
        for sector_name in sectors or []:
            text = str(sector_name or "").strip()
            if text and text not in seen_sectors:
                seen_sectors.add(text)
                rows.append({"sector_name": text, "parent_name": str(node or ""), "parent_path": node_path})
        for folder in folders or []:
            text = str(folder or "").strip()
            if text and text not in seen_nodes:
                queue.append((text, node_path))
    return rows


def _instrument_row(C, symbol, iscomplete=False):
    try:
        detail = C.get_instrument_detail(symbol, bool(iscomplete))
    except TypeError:
        detail = C.get_instrument_detail(symbol)
    if not isinstance(detail, dict) or not detail:
        return None
    return {
        "qmt_code": symbol,
        "stock_code": symbol.split(".", 1)[0],
        "short_name": detail.get("InstrumentName") or detail.get("instrument_name") or "",
        "exchange": detail.get("ExchangeID") or detail.get("ExchangeCode") or symbol.split(".", 1)[-1],
        "list_date": detail.get("OpenDate") or detail.get("CreateDate") or "",
        "product_type": detail.get("ProductType"),
        "is_trading": detail.get("IsTrading"),
    }


def _download_history(symbols, period, start_time, end_time):
    # Newer QMT builds expose the batch downloader used by xtquant.  One
    # batch call is materially faster and more reliable than hundreds of
    # sequential single-symbol downloads for a full-market minute refresh.
    download_many = globals().get("download_history_data2")
    if callable(download_many):
        try:
            download_many(
                stock_list=symbols,
                period=period,
                start_time=start_time,
                end_time=end_time,
            )
        except TypeError:
            download_many(symbols, period, start_time, end_time)
        return

    download = _global_function("download_history_data")
    for symbol in symbols:
        try:
            download(symbol, period, start_time, end_time, incrementally=True)
        except TypeError:
            download(symbol, period, start_time, end_time)


def _bar_rows(data, period):
    rows = []
    if not isinstance(data, dict):
        return rows
    for raw_symbol, frame in data.items():
        symbol = _valid_symbol(raw_symbol)
        if not symbol or frame is None:
            continue
        iterator = getattr(frame, "iterrows", None)
        if callable(iterator):
            records = iterator()
        elif isinstance(frame, (list, tuple)):
            records = enumerate(frame)
        elif isinstance(frame, dict):
            # Raw QMT payloads are either {time: record} or {field: values}.
            values = list(frame.values())
            if values and all(isinstance(value, dict) for value in values):
                records = frame.items()
            elif values and all(isinstance(value, (list, tuple)) for value in values):
                length = max(len(value) for value in values)
                records = (
                    (offset, dict((key, value[offset] if offset < len(value) else None) for key, value in frame.items()))
                    for offset in range(length)
                )
            else:
                records = [(0, frame)]
        else:
            continue
        for index_value, series in records:
            to_dict = getattr(series, "to_dict", None)
            record = to_dict() if callable(to_dict) else series if isinstance(series, dict) else {}
            trade_time = _time_text(record.get("stime") or record.get("time") or index_value, period)
            if not trade_time:
                continue
            close = _float(record.get("close"))
            native_pre_close = _float(record.get("preClose"))
            pre_close = native_pre_close if native_pre_close > 0 else None
            pre_close_origin = (
                "NATIVE_QMT"
                if native_pre_close > 0
                else "MISSING_NATIVE_QMT"
            )
            change = (
                close - pre_close
                if close > 0 and pre_close is not None
                else 0.0
            )
            change_pct = (
                change / pre_close * 100.0
                if pre_close is not None
                else 0.0
            )
            common = {
                "qmt_code": symbol,
                "stock_code": symbol.split(".", 1)[0],
                "trade_time": trade_time,
                "trade_date": trade_time[:10],
                "open": _float(record.get("open")),
                "close": close,
                "high": _float(record.get("high")),
                "low": _float(record.get("low")),
                "volume": max(0.0, _float(record.get("volume"))),
                "amount": max(0.0, _float(record.get("amount"))),
                "pre_close": pre_close,
                "pre_close_origin": pre_close_origin,
                "change": change,
                "change_pct": change_pct,
            }
            if period == "1d":
                common["k_type"] = 1
                common["turnover_ratio"] = record.get("turnoverRatio", record.get("turnover"))
            else:
                common["price"] = close
                common["avg_price"] = record.get("avgPrice")
            rows.append(common)
    return rows


def _market_rows(C, params, period):
    symbols = _normalize_codes(params.get("stock_codes"))
    start_time = _date_digits(params.get("start_date"))[:14]
    end_time = _date_digits(params.get("end_date"))[:14]
    if params.get("download_history"):
        _download_history(symbols, period, start_time, end_time)
    count = int(params.get("count", -1) or 0) if period == "1m" else -1
    # A synthetic filled bar is not an exchange observation and must never
    # enter the daily governance universe.  Minute consumers retain their
    # historical fill behavior; daily attestation requires actual raw bars.
    fill_data = period != "1d"
    raw_reader = getattr(C, "get_market_data_ex_ori", None)
    if callable(raw_reader):
        data = raw_reader(
            [], symbols, period=period, start_time=start_time, end_time=end_time,
            count=count, dividend_type=str(params.get("dividend_type") or "none"),
            fill_data=fill_data, subscribe=False
        )
    else:
        data = C.get_market_data_ex(
            [], symbols, period=period, start_time=start_time, end_time=end_time,
            count=count, dividend_type=str(params.get("dividend_type") or "none"),
            fill_data=fill_data, subscribe=False
        )
    return _bar_rows(data, period)


def _current_rows(C, params):
    symbols = _normalize_codes(params.get("stock_codes"))
    batch_size = max(20, int(params.get("batch_size") or 500))
    rows = []
    snapshot_at = _now_text()
    for offset in range(0, len(symbols), batch_size):
        data = C.get_full_tick(symbols[offset:offset + batch_size])
        if not isinstance(data, dict):
            continue
        for raw_symbol, tick in data.items():
            symbol = _valid_symbol(raw_symbol)
            if not symbol or not isinstance(tick, dict):
                continue
            price = _float(tick.get("lastPrice", tick.get("close")))
            pre_close = _float(tick.get("lastClose", tick.get("preClose")))
            if price <= 0:
                price = pre_close
            change = price - pre_close if pre_close > 0 else 0.0
            rows.append({
                "qmt_code": symbol,
                "stock_code": symbol.split(".", 1)[0],
                "snapshot_at": _time_text(tick.get("time") or tick.get("stime"), "tick") or snapshot_at,
                "open": _float(tick.get("open")),
                "price": price,
                "high": _float(tick.get("high")),
                "low": _float(tick.get("low")),
                "volume": max(0.0, _float(tick.get("volume", tick.get("pvolume")))),
                "amount": max(0.0, _float(tick.get("amount"))),
                "change": change,
                "change_pct": change / pre_close * 100.0 if pre_close > 0 else 0.0,
            })
    return rows


def _execute_request(C, action, params):
    if action in ("ping", "capabilities"):
        return {
            "bridge_version": BRIDGE_VERSION,
            "read_only": True,
            "pandas_free_history": True,
            "actions": [
                "current", "kline", "minute", "sector_list", "sector_members_many",
                "instrument_details", "index_members_many"
            ],
        }
    if action == "current":
        return {"rows": _current_rows(C, params)}
    if action == "kline":
        return {"rows": _market_rows(C, params, "1d")}
    if action == "minute":
        return {"rows": _market_rows(C, params, "1m")}
    if action == "sector_list":
        return {"rows": _sector_list_rows()}
    if action == "sector_members_many":
        rows = []
        for sector_name in params.get("sector_names") or []:
            for symbol in _sector_members(C, sector_name, params.get("realtime_tag", -1)):
                rows.append({
                    "sector_name": str(sector_name),
                    "qmt_code": symbol,
                    "stock_code": symbol.split(".", 1)[0],
                })
        return {"rows": rows}
    if action == "instrument_details":
        rows = []
        for symbol in _normalize_codes(params.get("stock_codes")):
            row = _instrument_row(C, symbol, params.get("iscomplete", False))
            if row:
                rows.append(row)
        return {"rows": rows}
    if action == "index_members_many":
        rows = []
        for index_symbol in _normalize_codes(params.get("index_codes")):
            detail = _instrument_row(C, index_symbol, False)
            sector_name = str((detail or {}).get("short_name") or "").strip()
            if not sector_name:
                continue
            for member in _sector_members(C, sector_name, -1):
                rows.append({
                    "index_code": index_symbol.split(".", 1)[0],
                    "index_qmt_code": index_symbol,
                    "sector_name": sector_name,
                    "stock_code": member.split(".", 1)[0],
                    "qmt_code": member,
                    "weight": None,
                })
        return {"rows": rows}
    raise ValueError("unsupported Big QMT bridge action: %s" % action)


def _process_one_request(C):
    global _last_request_at, _last_request_action, _last_error
    try:
        names = sorted(name for name in os.listdir(_requests_root) if name.endswith(".json"))
    except OSError:
        return False
    if not names:
        return False
    name = names[0]
    path = os.path.join(_requests_root, name)
    request_id = name[:-5]
    response_path = os.path.join(_responses_root, request_id + ".json.gz")
    try:
        with open(path, "r", encoding="utf-8") as handle:
            request_payload = json.load(handle)
        request_id = str(request_payload.get("request_id") or request_id)
        action = str(request_payload.get("action") or "").strip()
        params = request_payload.get("params") or {}
        _last_request_at = _now_text()
        _last_request_action = action
        _write_heartbeat("busy")
        result = _execute_request(C, action, params)
        response = {
            "schema_version": 2,
            "request_id": request_id,
            "action": action,
            "status": "ok",
            "source": "gj_big_qmt_inner",
            "bridge_version": BRIDGE_VERSION,
            "generated_at": _now_text(),
        }
        response.update(result)
        _atomic_gzip_write(response_path, response)
        _last_error = ""
    except Exception:
        _last_error = traceback.format_exc()[-4000:]
        _atomic_gzip_write(response_path, {
            "schema_version": 2,
            "request_id": request_id,
            "status": "error",
            "source": "gj_big_qmt_inner",
            "bridge_version": BRIDGE_VERSION,
            "generated_at": _now_text(),
            "error": _last_error,
        })
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    return True


def _write_heartbeat(status):
    try:
        pending = len([name for name in os.listdir(_requests_root) if name.endswith(".json")])
    except Exception:
        pending = 0
    payload = {
        "schema_version": 2,
        "bridge_version": BRIDGE_VERSION,
        "source": "gj_big_qmt_inner",
        "status": status,
        "updated_at": _now_text(),
        "updated_ts": time.time(),
        "pid": os.getpid(),
        "all_code_count": len(_all_codes),
        "tracked_code_count": len(_tracked_codes),
        "tracked_quote_count": len(_tracked_quotes),
        "subscription_id": _subscription_id,
        "last_callback_at": _last_callback_at,
        "last_callback_ts": _last_callback_ts,
        "callback_batch_count": _callback_batch_count,
        "last_full_refresh_ts": _last_full_refresh,
        "pending_request_count": pending,
        "last_request_at": _last_request_at,
        "last_request_action": _last_request_action,
        "last_error": _last_error,
    }
    _atomic_write("heartbeat.json", payload)


def bridge_tick(C):
    global _last_error
    with _lock:
        try:
            _refresh_subscription(C, force=False)
            _process_one_request(C)
            _refresh_full_snapshot(C)
            _write_tracked_snapshot(force=False)
            _last_error = ""
            _write_heartbeat("running")
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
            _write_heartbeat("error")


def init(C):
    global _bridge_root, _config_path, _requests_root, _responses_root, _last_error
    with _lock:
        _bridge_root = _find_bridge_root()
        _config_path = os.path.join(_bridge_root, "watchlist.json")
        _requests_root = os.path.join(_bridge_root, "requests")
        _responses_root = os.path.join(_bridge_root, "responses")
        for directory in (_requests_root, _responses_root):
            if not os.path.isdir(directory):
                os.makedirs(directory)
        try:
            _refresh_subscription(C, force=True)
            _last_error = ""
            _write_heartbeat("starting")
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
            _write_heartbeat("error")
        C.run_time("bridge_tick", "5nSecond", "2000-01-01 00:00:00")


def after_init(C):
    bridge_tick(C)


def handlebar(C):
    return


def stop(C):
    global _subscription_id, _last_error
    with _lock:
        if _subscription_id is not None:
            try:
                C.unsubscribe_quote(_subscription_id)
            except Exception:
                _last_error = traceback.format_exc()[-2000:]
            _subscription_id = None
        _write_heartbeat("stopped")
