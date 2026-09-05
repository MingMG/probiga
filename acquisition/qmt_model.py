"""Standalone, read-only full-QMT model. Install explicitly, never from Python.

Only the standard library is imported: ContextInfo is supplied by the logged-in
full QMT client. No xtdata, database, shell, trading or automatic reload entry.
"""
import datetime as dt
import json
import math
import os
import re
import shutil
import stat
import threading
import time
import uuid

SHANGHAI = dt.timezone(dt.timedelta(hours=8))
MAX_REQUEST_BYTES = 262144
MAX_RESULT_BYTES = 32 * 1024 * 1024
MAX_CODES = 40
MAX_LIVE_CODES = 10000
MAX_LIVE_BATCH = 800
REQUEST_FIELDS = frozenset((
    "request_id", "dataset", "source", "codes", "start_date", "end_date",
    "period", "adjustment", "requested_at", "deadline_at",
))
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
SYMBOL_RE = re.compile(r"[0-9]{6}\.(SH|SZ|BJ)\Z")
HISTORY = {
    "stock_daily": "1d", "index_daily": "1d", "etf_daily": "1d",
    "stock_minute": "1m", "index_minute": "1m",
}
CURRENT = frozenset(("stock_current", "index_current"))
_model = None


def now_shanghai():
    return dt.datetime.now(SHANGHAI)


def parse_instant(value):
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO string with UTC offset")
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})",
        value,
    )
    if not match:
        raise ValueError("timestamp must include an explicit UTC offset")
    base = dt.datetime.strptime(match[1] + " " + match[2], "%Y-%m-%d %H:%M:%S")
    fraction = match[3]
    if fraction:
        base = base.replace(microsecond=int(fraction[1:].ljust(6, "0")))
    zone = match[4]
    minutes = 0 if zone == "Z" else int(zone[1:3]) * 60 + int(zone[4:6])
    if zone != "Z" and (int(zone[1:3]) > 23 or int(zone[4:6]) > 59):
        raise ValueError("invalid timestamp offset")
    if zone.startswith("-"):
        minutes = -minutes
    return base.replace(tzinfo=dt.timezone(dt.timedelta(minutes=minutes))).astimezone(SHANGHAI)


def validate_id(request_id):
    if not isinstance(request_id, str) or not ID_RE.fullmatch(request_id):
        raise ValueError("invalid request_id")
    return request_id


def validate_request(request):
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    expected_fields = REQUEST_FIELDS
    if request.get("dataset") == "reference" and "asset_class" in request:
        expected_fields = REQUEST_FIELDS | {"asset_class"}
        if request["asset_class"] not in ("stock", "index", "etf"):
            raise ValueError("unsupported reference asset class")
    if set(request) != expected_fields:
        raise ValueError("request fields differ from the fixed data contract")
    validate_id(request["request_id"])
    dataset = request["dataset"]
    period = request["period"]
    if request["source"] != "guojin_qmt":
        raise ValueError("only full Guojin QMT is supported")
    codes = request["codes"]
    if not isinstance(codes, list) or not 1 <= len(codes) <= MAX_CODES:
        raise ValueError("request requires a bounded nonempty code batch")
    if any(not isinstance(code, str) for code in codes) or len(codes) != len(set(codes)):
        raise ValueError("request codes must be unique strings")
    if dataset == "reference" and period == "sector":
        if any(not code.strip() or len(code) > 128 or any(ord(c) < 32 for c in code) for code in codes):
            raise ValueError("invalid sector name")
    elif any(not SYMBOL_RE.fullmatch(code) for code in codes):
        raise ValueError("qualified QMT security codes are required")
    if dataset in HISTORY:
        if period != HISTORY[dataset]:
            raise ValueError("history period differs from dataset")
    elif dataset in CURRENT:
        if period != "tick":
            raise ValueError("current dataset requires tick")
    elif dataset == "reference":
        if period not in ("instrument", "calendar", "sector"):
            raise ValueError("unsupported reference product")
    else:
        raise ValueError("unsupported read-only dataset")
    adjustment = request["adjustment"]
    if adjustment not in ("none", "front", "back"):
        raise ValueError("unknown adjustment")
    if dataset not in ("stock_daily", "etf_daily") and adjustment != "none":
        raise ValueError("this product must remain unadjusted")
    for field in ("start_date", "end_date"):
        parsed = dt.datetime.strptime(request[field], "%Y-%m-%d").date()
        if parsed.isoformat() != request[field]:
            raise ValueError("noncanonical target date")
    if request["start_date"] != request["end_date"]:
        raise ValueError("one request covers exactly one date")
    if parse_instant(request["deadline_at"]) <= parse_instant(request["requested_at"]):
        raise ValueError("request deadline must follow request start")
    return request


def _ordinary(path):
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
        raise ValueError("acquisition paths cannot be links or reparse points")
    return info


def trusted_root(root, create=False):
    root = os.fspath(root)
    if not os.path.isabs(root) or root.startswith(("\\\\", "//")):
        raise ValueError("an absolute local acquisition directory is required")
    root = os.path.abspath(root)
    if os.path.dirname(root) == root:
        raise ValueError("a dedicated acquisition directory is required")
    current = root
    while True:
        if os.path.lexists(current):
            _ordinary(current)
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    if create:
        os.makedirs(root, exist_ok=True)
    if not stat.S_ISDIR(_ordinary(root).st_mode):
        raise ValueError("acquisition root is not a directory")
    return root


def read_json(path, limit):
    if not os.path.lexists(path):
        return None
    if not stat.S_ISREG(_ordinary(path).st_mode):
        raise ValueError("expected an ordinary acquisition file")
    with open(path, "rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError("acquisition file exceeds size limit")
    def invalid_constant(value):
        raise ValueError("nonfinite JSON token: " + value)
    value = json.loads(data.decode("utf-8"), parse_constant=invalid_constant)
    if not isinstance(value, dict):
        raise ValueError("acquisition file must be a JSON object")
    return value


def publish_json(path, payload, limit, immutable=False):
    data = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(data) > limit:
        raise ValueError("acquisition result exceeds size limit; reduce next batch")
    if os.path.lexists(path):
        _ordinary(path)
    temporary = path + "." + uuid.uuid4().hex + ".tmp"
    try:
        with open(temporary, "xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if immutable:
            os.link(temporary, path)  # Atomic create-if-absent; never overwrite ready.
        else:
            os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _raw_value(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return dict((str(key), _raw_value(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return [_raw_value(item) for item in value]
    scalar = getattr(value, "item", None)
    if callable(scalar):
        return _raw_value(scalar())
    return str(value)


def _records(frame, code):
    if frame is None:
        return []
    iterator = getattr(frame, "iterrows", None)
    if callable(iterator):
        pairs = iterator()
    elif isinstance(frame, list):
        pairs = ((None, row) for row in frame)
    elif isinstance(frame, dict):
        if not frame:
            return []
        if all(isinstance(item, dict) for item in frame.values()):
            pairs = frame.items()
        else:
            pairs = ((None, frame),)
    else:
        raise ValueError("unsupported native row container")
    rows = []
    for native_index, row in pairs:
        converter = getattr(row, "to_dict", None)
        record = converter() if callable(converter) else row
        if not isinstance(record, dict):
            raise ValueError("native row is not an object")
        raw = _raw_value(record)
        raw["qmt_code"] = code
        raw["native_index"] = _raw_value(native_index)
        rows.append(raw)
    return rows


def _error(code, reason):
    return {"status": "error", "rows": [], "error_code": code, "reason": reason}


def _data(rows):
    # An empty container alone does not prove suspension or a legal empty day.
    return {"status": "data", "rows": rows} if rows else _error(
        "EMPTY_NATIVE_RESULT", "native result is empty; no explicit no-data evidence")


def history_allowed(now):
    local = now.astimezone(SHANGHAI)
    # Conservatively stop before the morning session, including holidays.
    return local.weekday() >= 5 or local.time() < dt.time(8, 30) or local.time() >= dt.time(15, 30)


def _reader(C):
    for name in ("get_market_data_ex_ori", "get_market_data_ex"):
        method = getattr(C, name, None)
        if callable(method):
            return method, "ContextInfo." + name
    raise RuntimeError("native historical reader unavailable")


def execute_request(C, request, clock=now_shanghai, native_globals=None):
    validate_request(request)
    started = clock()
    codes = request["codes"]
    dataset = request["dataset"]
    outcomes = {}
    method_name = "not_called"
    if started >= parse_instant(request["deadline_at"]):
        outcomes = dict((code, _error("REQUEST_EXPIRED", "request expired before native execution")) for code in codes)
    elif dataset in HISTORY and not history_allowed(started):
        outcomes = dict((code, _error("HISTORY_WINDOW_CLOSED", "native history is disabled during the daytime session")) for code in codes)
    else:
        try:
            if dataset in HISTORY:
                reader, reader_name = _reader(C)
                native = globals() if native_globals is None else native_globals
                downloader = native.get("download_history_data")
                if not callable(downloader):
                    raise RuntimeError("native download_history_data unavailable")
                start = request["start_date"].replace("-", "") + "000000"
                end = request["end_date"].replace("-", "") + "235959"
                for code in codes:
                    if not history_allowed(clock()) or clock() >= parse_instant(request["deadline_at"]):
                        raise RuntimeError("history request budget/window ended between native calls")
                    method_name = "download_history_data"
                    downloader(code, request["period"], start, end)
                method_name = reader_name
                data = reader([], codes, period=request["period"], start_time=start,
                              end_time=end, count=-1, dividend_type=request["adjustment"],
                              fill_data=False, subscribe=False)
                if not isinstance(data, dict) or set(data) - set(codes):
                    raise ValueError("native historical symbol map differs")
                for code in codes:
                    try:
                        outcomes[code] = _data(_records(data.get(code), code)) if code in data else _error(
                            "MISSING_SOURCE_RESULT", "native response omitted requested security")
                    except (ValueError, TypeError):
                        outcomes[code] = _error("INVALID_NATIVE_ROWS", "native rows could not be serialized")
            elif dataset in CURRENT:
                method_name = "ContextInfo.get_full_tick"
                data = C.get_full_tick(codes)
                if not isinstance(data, dict) or set(data) - set(codes):
                    raise ValueError("native snapshot symbol map differs")
                for code in codes:
                    tick = data.get(code)
                    try:
                        outcomes[code] = _data(_records(tick, code)) if isinstance(tick, dict) else _error(
                            "MISSING_SOURCE_RESULT", "native snapshot omitted requested security")
                    except (ValueError, TypeError):
                        outcomes[code] = _error("INVALID_NATIVE_ROWS", "native rows could not be serialized")
            else:
                period = request["period"]
                method_name = {"instrument": "ContextInfo.get_instrument_detail",
                               "calendar": "ContextInfo.get_trading_dates",
                               "sector": "ContextInfo.get_stock_list_in_sector"}[period]
                for code in codes:
                    try:
                        if period == "instrument":
                            detail = C.get_instrument_detail(code, True)
                            rows = _records(detail, code) if isinstance(detail, dict) else []
                        elif period == "calendar":
                            values = C.get_trading_dates(code, request["start_date"].replace("-", ""),
                                                        request["end_date"].replace("-", ""), -1, "1d")
                            rows = [{"native_time": _raw_value(value), "qmt_code": code} for value in values]
                        else:
                            values = C.get_stock_list_in_sector(code)
                            rows = [{"qmt_code": str(value), "sector": code} for value in values]
                        outcomes[code] = _data(rows)
                    except Exception as exc:
                        outcomes[code] = _error("NATIVE_CALL_FAILED", type(exc).__name__)
        except Exception as exc:
            # Do not leak native exception text that may contain account data.
            outcomes = dict((code, _error("NATIVE_CALL_FAILED", type(exc).__name__)) for code in codes)
    return {"request": request, "received_at": clock().isoformat(),
            "source_method": method_name, "outcomes": outcomes}


def _native_tick_time(row):
    value = row.get("time") if row.get("time") is not None else row.get("stime")
    if value is None or isinstance(value, bool):
        return None
    try:
        raw = str(value)
        if re.fullmatch(r"\d{14}", raw):
            return dt.datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)
        if re.fullmatch(r"\d{10}(?:\.\d+)?|\d{13}", raw):
            stamp = float(raw)
            return dt.datetime.fromtimestamp(stamp / 1000 if len(raw) == 13 else stamp, SHANGHAI)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", raw):
            return dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=SHANGHAI)
        return parse_instant(raw)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


class Model:
    def __init__(self, root, clock=now_shanghai):
        self.root = trusted_root(root)
        self.clock = clock
        self.lock = threading.Lock()
        self.busy_request = None
        self.instance_id = uuid.uuid4().hex
        self.last_live = 0.0

    def live(self, C):
        """Separate snapshot products; never publish a synthesized source time."""
        now = self.clock()
        local = now.astimezone(SHANGHAI)
        if local.weekday() >= 5 or not (
            dt.time(9, 15) <= local.time() <= dt.time(11, 31)
            or dt.time(13, 0) <= local.time() <= dt.time(15, 1)
        ):
            return
        if now.timestamp() - self.last_live < 15:
            return
        self.last_live = now.timestamp()
        plan = read_json(os.path.join(self.root, "live_plan.json"), MAX_REQUEST_BYTES)
        if plan is None:
            return
        if set(plan) - CURRENT:
            raise ValueError("unsupported live product")
        if any(not isinstance(codes, list) for codes in plan.values()) or sum(len(codes) for codes in plan.values()) > MAX_LIVE_CODES:
            raise ValueError("live plan exceeds the total security limit")
        for dataset, codes in plan.items():
            if (not isinstance(codes, list) or not 1 <= len(codes) <= MAX_LIVE_CODES
                    or any(not isinstance(code, str) or not SYMBOL_RE.fullmatch(code) for code in codes)
                    or len(codes) != len(set(codes))):
                raise ValueError("live plan must contain a bounded unique symbol set")
            path = os.path.join(self.root, dataset + ".snapshot.json")
            previous = read_json(path, MAX_RESULT_BYTES) or {}
            retained = previous.get("outcomes") or {}
            outcomes = {}
            for offset in range(0, len(codes), MAX_LIVE_BATCH):
                batch = codes[offset:offset + MAX_LIVE_BATCH]
                try:
                    ticks = C.get_full_tick(batch)
                    if not isinstance(ticks, dict) or set(ticks) - set(batch):
                        raise ValueError("live native response differs")
                except Exception as exc:
                    for code in batch:
                        outcomes[code] = _error("NATIVE_CALL_FAILED", type(exc).__name__)
                    continue
                for code in batch:
                    tick = ticks.get(code)
                    raw_rows = _records(tick, code) if isinstance(tick, dict) else []
                    stamp = _native_tick_time(raw_rows[0]) if raw_rows else None
                    # Keep errors explicit; old persisted good quotes are not
                    # overwritten by a row with absent or impossible source time.
                    if stamp is None or stamp > self.clock() + dt.timedelta(seconds=5):
                        outcomes[code] = _error("INVALID_SOURCE_TIME", "native tick time missing, invalid or future")
                        continue
                    older = retained.get(code, {})
                    old_rows = older.get("rows") or []
                    old_time = _native_tick_time(old_rows[0]) if old_rows else None
                    outcomes[code] = older if old_time is not None and stamp <= old_time else _data(raw_rows)
            request = {"request_id": "live_" + uuid.uuid4().hex, "dataset": dataset,
                       "source": "guojin_qmt", "codes": codes,
                       "start_date": local.date().isoformat(), "end_date": local.date().isoformat(),
                       "period": "tick", "adjustment": "none", "requested_at": now.isoformat(),
                       "deadline_at": (now + dt.timedelta(seconds=15)).isoformat()}
            publish_json(path, {"request": request, "received_at": self.clock().isoformat(),
                                "source_method": "ContextInfo.get_full_tick", "outcomes": outcomes}, MAX_RESULT_BYTES)

    def heartbeat(self, status, error_code=None):
        publish_json(os.path.join(self.root, "heartbeat.json"), {
            "status": status, "pid": os.getpid(), "instance_id": self.instance_id,
            "updated_at": self.clock().isoformat(), "active_request_id": self.busy_request,
            "error_code": error_code,
        }, MAX_REQUEST_BYTES)

    def poll(self, C):
        if not self.lock.acquire(False):
            return  # A timeout in another process does not cancel native work.
        try:
            self.live(C)
            request = read_json(os.path.join(self.root, "active.json"), MAX_REQUEST_BYTES)
            if request is None:
                self.heartbeat("idle")
                return
            validate_request(request)
            request_id = request["request_id"]
            ready = os.path.join(self.root, request_id + ".ready.json")
            self.busy_request = request_id
            existing = read_json(ready, MAX_RESULT_BYTES)
            # Consumer may have retained the result and crashed before the
            # final active-file removal. That is already executed, not new work.
            archive_dir = os.path.join(self.root, "processed", request_id)
            if existing is None and os.path.exists(archive_dir):
                trusted_root(archive_dir)
                existing = read_json(os.path.join(archive_dir, request_id + ".ready.json"), MAX_RESULT_BYTES)
            if existing is not None:
                if existing.get("request") != request:
                    raise ValueError("existing result belongs to another request")
                self.heartbeat("awaiting_commit")
                return
            self.heartbeat("busy")
            if shutil.disk_usage(self.root).free < MAX_RESULT_BYTES * 2:
                result = {"request": request, "received_at": self.clock().isoformat(),
                          "source_method": "not_called", "outcomes": dict((code, _error(
                              "DISK_SPACE_LOW", "preserve pending results; no new native request")) for code in request["codes"])}
            else:
                result = execute_request(C, request, clock=self.clock)
            try:
                publish_json(ready, result, MAX_RESULT_BYTES, immutable=True)
            except ValueError:
                # Never publish a silently truncated result. Preserve one outcome
                # per code so the ordinary consumer can record the failed batch.
                result["outcomes"] = dict((code, _error("RESULT_TOO_LARGE", "reduce the next batch size")) for code in request["codes"])
                publish_json(ready, result, MAX_RESULT_BYTES, immutable=True)
            self.heartbeat("awaiting_commit")
        except Exception as exc:
            self.heartbeat("error", type(exc).__name__)
        finally:
            self.busy_request = None
            self.lock.release()


def init(C):
    global _model
    root = os.environ.get("DIRECT_ACQUISITION_QMT_ROOT", "")
    _model = Model(root)
    _model.heartbeat("starting")
    C.run_time("direct_acquisition_tick", "1nSecond", "2000-01-01 00:00:00")


def direct_acquisition_tick(C):
    if _model is not None:
        _model.poll(C)


def handlebar(C):
    return


def stop(C):
    if _model is not None:
        _model.heartbeat("stopped")
