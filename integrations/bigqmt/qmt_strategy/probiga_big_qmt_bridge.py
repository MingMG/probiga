# coding:gbk
"""ProBigA data exporter for the standard QMT built-in Python runtime.

The model is read-only.  It exports quotes, local history and reference data
through userdata/probiga_bridge.  It contains no order or cancel API calls.
"""

import datetime
import faulthandler
import gzip
import hashlib
import importlib.util
import json
import os
import stat
import threading
import time
import traceback
import uuid


BRIDGE_VERSION = "bigqmt_inner_v2"
STRATEGY_RELEASE_PROTOCOL = "probiga.bigqmt-strategy-release.v2"
STRATEGY_IDENTITY_PROTOCOL = "probiga.bigqmt-loaded-strategy-identity.v1"
STRATEGY_RELEASE_MANIFEST_SCHEMA = "probiga.bigqmt-strategy-manifest.v1"
STRATEGY_RELEASE_MANIFEST_NAME = "probiga_big_qmt_bridge.release.json"
EMBEDDED_STRATEGY_BUILD_SHA = "__PROBIGA_EMBEDDED_BUILD_SHA__"
EMBEDDED_STRATEGY_GIT_BLOB = "__PROBIGA_EMBEDDED_GIT_BLOB__"
EMBEDDED_STRATEGY_SOURCE_SHA256 = "__PROBIGA_EMBEDDED_SOURCE_SHA256__"
EMBEDDED_STRATEGY_IDENTITY_SHA256 = "__PROBIGA_EMBEDDED_IDENTITY_SHA256__"
DIRECT_ACQUISITION_MODEL_SHA256 = "075a10f0edca637196c2c18bc036219b5c13c136ce4b0bbc1f86937f1ed3ac42"
DIRECT_ACQUISITION_MODEL_PREFIX = "probiga_direct_acquisition_"
MAX_TRACKED_CODES = 280
MAX_ANNOUNCEMENT_BATCH_ROWS = 200000
MAX_ANNOUNCEMENT_BATCH_JSON_BYTES = 64 * 1024 * 1024
MINUTE_FLOW_NATIVE_FIELDS = (
    "netInflowMostAmount", "netInflowBigAmount",
    "netInflowMediumAmount", "netInflowSmallAmount",
)

_lock = threading.RLock()
# Only the timer/lifecycle owns native requests and file publication.  Never
# hold the ingress lock while calling QMT: a native call may wait for its
# quote callback, which needs that lock to deliver the update.
_execution_lock = threading.Lock()
_bridge_root = None
_config_path = None
_requests_root = None
_responses_root = None
_inflight_root = None
_checkpoints_root = None
_dead_letter_root = None
_cancelled_root = None
_config_mtime = None
_config = {}
_all_codes = []
_tracked_codes = []
_subscription_id = None
_tracked_quotes = {}
_quote_cache = {}
_subscribed_codes = frozenset()
_subscription_generation = 0
_seed_pending = []
_last_seed_attempt = 0.0
_last_market_callback_ts = 0.0
_subscription_started_ts = 0.0
_last_subscription_attempt = 0.0
_quote_phase_key = None
_last_tracked_flush = 0.0
_last_full_refresh = 0.0
_last_request_at = ""
_last_request_action = ""
_last_error = ""
_last_callback_at = ""
_last_callback_ts = 0.0
_callback_batch_count = 0
_model_instance_id = ""
_model_started_ts = 0.0
_heartbeat_seq = 0
_last_queue_cleanup = 0.0
_direct_model = None
_timer_id = None
_timer_generation = 0
_stopping = False
_stop_completed = False
_fault_log = None


def _enable_fault_log():
    global _fault_log
    # Keep this descriptor alive for the entire embedded interpreter lifetime.
    # Native access violations cannot be caught by bridge_tick's except block.
    if _fault_log is None:
        _fault_log = open(os.path.join(_bridge_root, "native_fault.log"), "ab", buffering=0)
    faulthandler.enable(file=_fault_log, all_threads=True)


def _schedule_next_tick(C):
    global _timer_id, _timer_generation
    if _stopping or _timer_id is not None:
        return
    _timer_generation += 1
    generation = _timer_generation
    _timer_id = C.schedule_run(
        lambda context: _scheduled_tick(context, generation),
        datetime.datetime.now() + datetime.timedelta(seconds=1),
        repeat_times=0, name="probiga_bridge_tick",
    )
    if not isinstance(_timer_id, int) or _timer_id < 0:
        _timer_id = None
        raise RuntimeError("QMT did not return a cancellable bridge timer")


def _scheduled_tick(C, generation):
    global _timer_id
    if _stopping or generation != _timer_generation or _timer_id is None:
        return
    # Consume the one-shot before entering native work. No periodic native
    # callback can accumulate while a history request or publication is slow.
    _timer_id = None
    try:
        bridge_tick(C)
    finally:
        _schedule_next_tick(C)


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


def _load_direct_acquisition_model():
    qmt_home = os.path.dirname(os.path.dirname(_bridge_root))
    model_path = os.path.join(
        qmt_home,
        "python",
        DIRECT_ACQUISITION_MODEL_PREFIX
        + DIRECT_ACQUISITION_MODEL_SHA256
        + ".py",
    )
    info = os.lstat(model_path)
    if stat.S_ISLNK(info.st_mode) or (
        getattr(info, "st_file_attributes", 0) & 0x400
    ):
        raise RuntimeError("direct acquisition model cannot be a link")
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError("direct acquisition model is not an ordinary file")
    if _file_sha256(model_path) != DIRECT_ACQUISITION_MODEL_SHA256:
        raise RuntimeError("direct acquisition model hash differs")
    module_name = (
        "probiga_direct_acquisition_" + DIRECT_ACQUISITION_MODEL_SHA256
    )
    spec = importlib.util.spec_from_file_location(module_name, model_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("direct acquisition model cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    direct_root = os.path.join(
        os.path.dirname(_bridge_root), "probiga_direct_acquisition", "qmt"
    )
    if not os.path.isdir(direct_root):
        os.makedirs(direct_root)
    return module.Model(
        direct_root,
        source_sha256=DIRECT_ACQUISITION_MODEL_SHA256,
        native_globals=globals(),
    )


def _json_safe(value):
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), -float("inf")) else None
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


def _file_sha256(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _valid_hex(value, lengths):
    text = str(value or "").strip().lower()
    return (
        len(text) in lengths
        and all(character in "0123456789abcdef" for character in text)
    )


def _freeze_loaded_strategy_identity():
    """Freeze source/build evidence once, while QMT loads this model.

    Capabilities and heartbeats must never re-read these files.  Therefore an
    installer overwriting the strategy or manifest cannot make already-loaded
    Python code advertise the new release; a real model reload is required.
    """
    unavailable = {
        "strategy_identity_protocol": STRATEGY_IDENTITY_PROTOCOL,
        "strategy_identity_frozen": True,
        "strategy_identity_status": "UNAVAILABLE",
        "strategy_identity_error": "load_identity_unavailable",
        "strategy_build_sha": "",
        "strategy_git_blob": "",
        "strategy_source_sha256": "",
        "strategy_artifact_sha256": "",
        "strategy_loaded_identity_sha256": "",
        "strategy_identity_loaded_at": time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime()
        ),
    }
    try:
        build_sha = str(EMBEDDED_STRATEGY_BUILD_SHA or "").lower()
        git_blob = str(EMBEDDED_STRATEGY_GIT_BLOB or "").lower()
        source_sha256 = str(EMBEDDED_STRATEGY_SOURCE_SHA256 or "").lower()
        identity_sha256 = str(
            EMBEDDED_STRATEGY_IDENTITY_SHA256 or ""
        ).lower()
        expected_identity = hashlib.sha256(
            (
                STRATEGY_IDENTITY_PROTOCOL
                + "\n" + build_sha
                + "\n" + git_blob
                + "\n" + source_sha256
            ).encode("ascii")
        ).hexdigest()
        if (
            not _valid_hex(build_sha, (40,))
            or build_sha == "0" * 40
            or not _valid_hex(git_blob, (40, 64))
            or not _valid_hex(source_sha256, (64,))
            or not _valid_hex(identity_sha256, (64,))
            or identity_sha256 != expected_identity
        ):
            return unavailable
        script_path = globals().get("__file__")
        manifest_candidates = []
        if script_path:
            path = os.path.abspath(script_path)
            manifest_candidates.append(os.path.join(
                os.path.dirname(path), STRATEGY_RELEASE_MANIFEST_NAME
            ))
        try:
            bridge_root = _find_bridge_root()
            qmt_home = os.path.dirname(os.path.dirname(bridge_root))
            manifest_candidates.append(os.path.join(
                qmt_home, "python", STRATEGY_RELEASE_MANIFEST_NAME
            ))
        except Exception:
            pass
        manifest_path = next(
            (
                candidate for candidate in manifest_candidates
                if os.path.isfile(candidate)
            ),
            "",
        )
        if not manifest_path:
            return unavailable
        with open(manifest_path, "r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        if not isinstance(manifest, dict):
            return unavailable
        artifact_sha256 = str(
            manifest.get("strategy_artifact_sha256") or ""
        ).lower()
        if (
            manifest.get("schema") != STRATEGY_RELEASE_MANIFEST_SCHEMA
            or manifest.get("strategy_release_protocol")
            != STRATEGY_RELEASE_PROTOCOL
            or manifest.get("strategy_identity_protocol")
            != STRATEGY_IDENTITY_PROTOCOL
            or str(manifest.get("strategy_build_sha") or "").lower()
            != build_sha
            or str(manifest.get("strategy_git_blob") or "").lower()
            != git_blob
            or str(manifest.get("strategy_source_sha256") or "").lower()
            != source_sha256
            or str(
                manifest.get("strategy_loaded_identity_sha256") or ""
            ).lower() != identity_sha256
            or not _valid_hex(artifact_sha256, (64,))
        ):
            return unavailable
        if script_path:
            path = os.path.abspath(script_path)
            if (
                os.path.basename(path).casefold()
                != "probiga_big_qmt_bridge.py".casefold()
                or not os.path.isfile(path)
                or _file_sha256(path) != artifact_sha256
            ):
                return unavailable
        return {
            "strategy_identity_protocol": STRATEGY_IDENTITY_PROTOCOL,
            "strategy_identity_frozen": True,
            "strategy_identity_status": "BOUND",
            "strategy_identity_error": "",
            "strategy_build_sha": build_sha,
            "strategy_git_blob": git_blob,
            "strategy_source_sha256": source_sha256,
            "strategy_artifact_sha256": artifact_sha256,
            "strategy_loaded_identity_sha256": identity_sha256,
            "strategy_identity_loaded_at": unavailable[
                "strategy_identity_loaded_at"
            ],
        }
    except Exception:
        return unavailable


_LOADED_STRATEGY_IDENTITY = _freeze_loaded_strategy_identity()


def _strategy_identity_payload():
    # Every request response is independently release-attested.  Capabilities
    # already exposes the release protocol in its action payload, but ordinary
    # actions (including ``trading_calendar``) only receive this shared
    # identity envelope.  Keep the protocol in that envelope as well so a
    # capability proof and the subsequent data response have the exact same
    # release identity.
    return {
        "strategy_release_protocol": STRATEGY_RELEASE_PROTOCOL,
        **dict(_LOADED_STRATEGY_IDENTITY),
    }


def _atomic_write(name, payload):
    path = os.path.join(_bridge_root, name)
    temporary = path + ".%s.tmp" % os.getpid()
    encoded = json.dumps(_json_safe(payload), ensure_ascii=True, separators=(",", ":"))
    with open(temporary, "w", encoding="utf-8") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    _replace_with_retry(temporary, path)


def _atomic_json_path(path, payload):
    temporary = path + ".%s.tmp" % os.getpid()
    encoded = json.dumps(_json_safe(payload), ensure_ascii=True, separators=(",", ":"))
    with open(temporary, "w", encoding="utf-8") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    _replace_with_retry(temporary, path)


def _atomic_gzip_write(path, payload):
    temporary = path + ".%s.tmp" % os.getpid()
    encoded = json.dumps(_json_safe(payload), ensure_ascii=True, separators=(",", ":"))
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=5) as handle:
        handle.write(encoded)
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
    all_codes = _normalize_codes(payload.get("all_codes"))
    tracked_codes = _normalize_codes(payload.get("tracked_codes"), MAX_TRACKED_CODES)
    with _lock:
        _config = payload
        _all_codes = all_codes
        _tracked_codes = tracked_codes
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
    # Callback updates replace whole normalized rows.  This detached mapping
    # remains stable while ingress continues during serialization/fsync.
    with _lock:
        selected = dict((code, _tracked_quotes[code]) for code in _tracked_codes if code in _tracked_quotes)
    _atomic_write("tracked_quotes.json", _snapshot_payload("tracked", selected))
    _last_tracked_flush = now


def whole_quote_callback(data):
    _receive_quotes(data, _subscription_generation)


def _receive_quotes(data, generation):
    global _tracked_quotes, _last_error
    global _last_market_callback_ts
    global _last_callback_at, _last_callback_ts, _callback_batch_count
    if not isinstance(data, dict):
        return
    tracked = frozenset(_tracked_codes)
    try:
        with _lock:
            if _stopping or generation != _subscription_generation:
                return
            received_ts = time.time()
            received_at = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(received_ts),
            )
            accepted_count = 0
            for raw_code, raw_tick in data.items():
                code = _valid_symbol(raw_code)
                if code and (code in _subscribed_codes or code in tracked) and isinstance(raw_tick, dict):
                    normalized = _json_safe(raw_tick)
                    previous = _quote_cache.get(code, {})
                    if (_float(previous.get("time")) > 0 and
                            _float(normalized.get("time")) < _float(previous.get("time"))):
                        continue
                    if isinstance(normalized, dict):
                        # Retain the first-party callback receipt time per
                        # symbol.  Snapshot publication may repeat an
                        # unchanged book, but it must not manufacture a new
                        # ingress timestamp for that old quote.
                        normalized["_probiga_received_at"] = received_at
                    _quote_cache[code] = normalized
                    _last_market_callback_ts = received_ts
                    if code in tracked:
                        _tracked_quotes[code] = normalized
                        accepted_count += 1
            if accepted_count:
                _last_callback_at = received_at
                _last_callback_ts = received_ts
                _callback_batch_count += 1
            # The native callback only receives data.  Disk publication and
            # retry sleeps belong to bridge_tick, never the quote thread.
    except Exception:
        _last_error = traceback.format_exc()[-2000:]


def _quote_phase(now):
    local = datetime.datetime.fromtimestamp(now)
    if local.weekday() < 5 and datetime.time(9, 15) <= local.time() < datetime.time(15, 10):
        return ("live", local.date().isoformat())
    closed = local.date()
    if local.time() < datetime.time(15, 10):
        closed -= datetime.timedelta(days=1)
    while closed.weekday() >= 5:
        closed -= datetime.timedelta(days=1)
    # This is an acquisition slot, not a claim that the exchange traded.
    # Native quote timestamps remain the authority on holidays.
    return ("closed", closed.isoformat())


def _refresh_subscription(C, force=False):
    global _subscription_id, _tracked_quotes, _last_error
    global _quote_cache, _subscribed_codes, _subscription_generation
    global _seed_pending, _last_seed_attempt
    global _subscription_started_ts, _last_subscription_attempt
    global _quote_phase_key
    changed = _load_config(force=force)
    wanted = frozenset(_all_codes) | frozenset(_tracked_codes)
    now = time.time()
    phase = _quote_phase(now)
    live = phase[0] == "live"
    phase_changed = phase != _quote_phase_key
    local = time.localtime(now)
    minute = local.tm_hour * 60 + local.tm_min
    market_open = local.tm_wday < 5 and (570 <= minute < 690 or 780 <= minute < 900)
    silent = bool(live and wanted and market_open and now - max(
        _last_market_callback_ts, _subscription_started_ts) > 120)
    if changed:
        with _lock:
            _tracked_quotes = dict((code, tick) for code, tick in _quote_cache.items()
                                   if code in _tracked_codes and "_probiga_received_at" in tick)
    if not force and not phase_changed and now - _last_subscription_attempt < 30:
        return
    if (not force and not phase_changed and wanted == _subscribed_codes
            and (not live or _subscription_id is not None) and not silent):
        with _lock:
            _tracked_quotes = dict((code, tick) for code, tick in _quote_cache.items()
                                   if code in _tracked_codes and "_probiga_received_at" in tick)
        return
    _last_subscription_attempt = now
    _quote_phase_key = phase
    # Fence late callbacks from the old native subscription before replacing it.
    with _lock:
        _subscription_generation += 1
        generation = _subscription_generation
        _subscribed_codes = wanted
        _quote_cache = {}
        _tracked_quotes = {}
        _seed_pending = sorted(wanted)
        _last_seed_attempt = 0.0
    if _subscription_id is not None:
        try:
            C.unsubscribe_quote(_subscription_id)
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
        _subscription_id = None
    if wanted and live:
        subscription = C.subscribe_whole_quote(
            sorted(wanted), callback=lambda data: _receive_quotes(data, generation))
        if not isinstance(subscription, int) or subscription <= 0:
            raise RuntimeError("full-market quote subscription failed")
        _subscription_id = subscription
        _subscription_started_ts = now
        # Do not seed this stream from ``get_full_tick``.  It is a reconnect
        # cache and an older consumer could otherwise substitute its own
        # receive time for the missing callback marker.  The full-market
        # snapshot already serves display/current data; this tracked stream
        # stays empty until the first genuine subscription callback.
    _write_tracked_snapshot(force=True)


def _refresh_full_snapshot(C):
    global _last_full_refresh
    global _last_seed_attempt
    now = time.time()
    # Cold-start cache fill is paced across timer invocations. Once seeded,
    # market updates come exclusively from the native whole-quote subscription.
    # Never label this initial cache read as callback/Level-1 evidence.
    if _seed_pending:
        if now - _last_seed_attempt < 1.0:
            return
        _last_seed_attempt = now
        batch = _seed_pending[:40]
        data = C.get_full_tick(batch)
        if not isinstance(data, dict) or set(data) - set(batch):
            raise ValueError("initial quote cache response differs from requested symbols")
        with _lock:
            for code, tick in data.items():
                if isinstance(tick, dict) and code not in _quote_cache:
                    _quote_cache[code] = _json_safe(tick)
            del _seed_pending[:len(batch)]
        if _seed_pending:
            return
    interval = max(5, int(_config.get("full_refresh_seconds", 30)))
    if now - _last_full_refresh < interval:
        return
    with _lock:
        quotes = dict((code, _quote_cache[code]) for code in _all_codes if code in _quote_cache)
    _atomic_write("full_quotes.json", _snapshot_payload("full", quotes))
    _last_full_refresh = now


class _QuoteCacheContext:
    """Share one subscribed quote book with all in-process acquisition readers."""

    def __init__(self, native):
        self.native = native

    def __getattr__(self, name):
        return getattr(self.native, name)

    def get_full_tick(self, codes):
        with _lock:
            # Consumers may normalize/mutate results; never expose ingress rows.
            result = dict((code, _json_safe(_quote_cache[code])) for code in codes
                          if code in _quote_cache)
            outside = [code for code in codes if code not in _subscribed_codes]
        # Ad-hoc instruments outside the managed universe still need a native
        # lookup. Do not silently turn a supported query into missing data.
        if outside:
            data = self.native.get_full_tick(outside)
            if not isinstance(data, dict) or set(data) - set(outside):
                raise ValueError("ad-hoc quote response differs from requested symbols")
            result.update(_json_safe(data))
        return result


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


def _trading_calendar_rows(C, params):
    """Read the native built-in QMT trading-date series without inference."""
    market = str(params.get("market") or "SH").strip().upper() or "SH"
    if market != "SH":
        raise ValueError("Big QMT trading calendar currently supports SH only")
    source_stock_code = str(
        params.get("source_stock_code") or "000001.SH"
    ).strip().upper()
    if _valid_symbol(source_stock_code) != source_stock_code:
        raise ValueError("Big QMT trading calendar source stock is invalid")
    start_date = _date_digits(params.get("start_date"))[:8]
    end_date = _date_digits(params.get("end_date"))[:8]
    if (
        len(start_date) != 8
        or len(end_date) != 8
        or start_date > end_date
    ):
        raise ValueError("Big QMT trading calendar range is invalid")
    get_dates = getattr(C, "get_trading_dates", None)
    if not callable(get_dates):
        raise RuntimeError(
            "standard QMT built-in ContextInfo.get_trading_dates is unavailable"
        )
    values = get_dates(
        source_stock_code, start_date, end_date, -1, "1d"
    )
    normalized = set()
    for value in values or []:
        rendered = _time_text(value, "1d")
        trade_date = rendered[:10] if rendered else ""
        compact = trade_date.replace("-", "")
        if len(compact) == 8 and start_date <= compact <= end_date:
            normalized.add(trade_date)
    if not normalized:
        raise RuntimeError("Big QMT native trading calendar returned no sessions")
    rows = []
    for trade_date in sorted(normalized):
        parsed = time.strptime(trade_date, "%Y-%m-%d")
        rows.append({
            "market": market,
            "trade_date": trade_date,
            "calendar_year": int(trade_date[:4]),
            "trade_status": 1,
            "day_week": int(parsed.tm_wday) + 1,
        })
    return {
        "rows": rows,
        "source_method": "ContextInfo.get_trading_dates",
        "source_stock_code": source_stock_code,
        "requested_start_date": (
            start_date[:4] + "-" + start_date[4:6] + "-" + start_date[6:8]
        ),
        "requested_end_date": (
            end_date[:4] + "-" + end_date[4:6] + "-" + end_date[6:8]
        ),
        "observed_start_date": rows[0]["trade_date"],
        "observed_end_date": rows[-1]["trade_date"],
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


def _download_announcement_history(symbols, start_time, end_time):
    """Download the exact announcement window without incremental widening."""

    download_many = globals().get("download_history_data2")
    if callable(download_many):
        try:
            download_many(
                stock_list=symbols,
                period="announcement",
                start_time=start_time,
                end_time=end_time,
            )
        except TypeError:
            download_many(symbols, "announcement", start_time, end_time)
        return
    download = _global_function("download_history_data")
    for symbol in symbols:
        download(symbol, "announcement", start_time, end_time)


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
    # Synthetic padding cannot prove either a traded minute or a suspension.
    # The publisher separately requires one native raw daily row to classify
    # no-trade codes, so both daily and minute reads remain unfilled facts.
    fill_data = False
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


def _minute_flow_capture(C, params):
    """The fixed native cumulative feature; never synthesize or rename OHLC."""
    if set(params) != set(("stock_codes", "trade_date")):
        raise ValueError("QMT_MINUTE_FLOW_REQUEST_SCOPE_INVALID")
    supplied = params.get("stock_codes")
    symbols = _normalize_codes(supplied)
    if not isinstance(supplied, list) or not symbols or len(symbols) > 40 or len(symbols) != len(supplied):
        raise ValueError("QMT_MINUTE_FLOW_CODE_SCOPE_INVALID")
    target = str(params.get("trade_date") or "")
    parsed = time.strptime(target, "%Y-%m-%d")
    if time.strftime("%Y-%m-%d", parsed) != target:
        raise ValueError("QMT_MINUTE_FLOW_DATE_INVALID")
    start = target.replace("-", "") + "000000"
    end = target.replace("-", "") + "235959"
    _download_history(symbols, "transactioncount1m", start, end)
    reader = getattr(C, "get_market_data_ex_ori", None)
    if not callable(reader):
        raise RuntimeError("QMT_MINUTE_FLOW_NATIVE_READER_UNAVAILABLE")
    data = reader(
        [], symbols, period="transactioncount1m", start_time=start, end_time=end,
        count=-1, dividend_type="none", fill_data=False, subscribe=False,
    )
    if not isinstance(data, dict) or set(data) - set(symbols):
        raise RuntimeError("QMT_MINUTE_FLOW_SYMBOL_MAP_INVALID")
    rows = []
    for symbol, frame in data.items():
        if frame is None:
            continue
        if callable(getattr(frame, "iterrows", None)):
            records = frame.iterrows()
        elif isinstance(frame, (list, tuple)):
            records = enumerate(frame)
        elif isinstance(frame, dict):
            values = list(frame.values())
            if not values:
                continue
            if all(isinstance(value, dict) for value in values):
                records = frame.items()
            elif all(isinstance(value, (list, tuple)) for value in values):
                lengths = set(len(value) for value in values)
                if len(lengths) != 1:
                    raise RuntimeError("QMT_MINUTE_FLOW_COLUMN_LENGTH_INVALID")
                records = (
                    (offset, dict((key, value[offset]) for key, value in frame.items()))
                    for offset in range(next(iter(lengths)))
                )
            else:
                records = [(0, frame)]
        else:
            raise RuntimeError("QMT_MINUTE_FLOW_FRAME_INVALID")
        for index, value in records:
            record = value.to_dict() if callable(getattr(value, "to_dict", None)) else value
            if not isinstance(record, dict) or any(field not in record for field in MINUTE_FLOW_NATIVE_FIELDS):
                raise RuntimeError("QMT_MINUTE_FLOW_NATIVE_FIELDS_MISSING")
            raw_stamp = record.get("stime") or record.get("time") or index
            compact = str(raw_stamp).split(".")[0]
            if len(compact) in (14, 17) and compact.isdigit() and "1900" <= compact[:4] <= "2200":
                raw_stamp = compact
            stamp = _time_text(raw_stamp, "1m")
            if not stamp or stamp[:10] != target:
                raise RuntimeError("QMT_MINUTE_FLOW_TIMESTAMP_INVALID")
            row = dict((field, record[field]) for field in MINUTE_FLOW_NATIVE_FIELDS)
            row.update({"qmt_code": symbol, "stock_code": symbol.split(".")[0], "trade_time": stamp})
            rows.append(row)
            if len(rows) > len(symbols) * 241:
                raise RuntimeError("QMT_MINUTE_FLOW_NATIVE_GRID_OVERSIZED")
    return {
        "schema": "probiga.bigqmt-minute-flow-capture.v1",
        "period": "transactioncount1m", "trade_date": target,
        "requested_qmt_code_count": len(symbols),
        "requested_qmt_code_set_hash": hashlib.sha256("\n".join(sorted(symbols)).encode("ascii")).hexdigest(),
        "native_fields": list(MINUTE_FLOW_NATIVE_FIELDS),
        "source_method": "ContextInfo.get_market_data_ex_ori",
        "download_method": "download_history_data2" if callable(globals().get("download_history_data2")) else "download_history_data",
        "count": -1, "fill_data": False, "subscribe": False,
        "row_count": len(rows), "rows": rows,
    }


def _announcement_frame_payload(frame):
    """Serialize one native announcement DataFrame without importing pandas."""

    if frame is None:
        raise RuntimeError("Big QMT announcement frame is unavailable")
    iterator = getattr(frame, "iterrows", None)
    if callable(iterator):
        records = iterator()
        index_name = getattr(getattr(frame, "index", None), "name", None)
    elif isinstance(frame, (list, tuple)):
        records = enumerate(frame)
        index_name = None
    elif isinstance(frame, dict):
        records = frame.items()
        index_name = None
    else:
        raise RuntimeError("Big QMT announcement frame shape is unsupported")
    rows = []
    estimated_bytes = 0
    for index_value, raw_row in records:
        to_dict = getattr(raw_row, "to_dict", None)
        row = to_dict() if callable(to_dict) else raw_row
        if not isinstance(row, dict):
            raise RuntimeError("Big QMT announcement row shape is unsupported")
        payload = {
            "index": _announcement_json_value(index_value),
            "row": _announcement_json_value(row),
        }
        estimated_bytes += len(json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8"))
        rows.append(payload)
    return {
        "index_name": _announcement_json_value(index_name),
        "rows": rows,
        "estimated_uncompressed_bytes": estimated_bytes,
    }


def _announcement_json_value(value):
    """Strict JSON scalar/container conversion for source announcement rows."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), -float("inf")) else None
    if isinstance(value, dict):
        return dict(
            (str(key), _announcement_json_value(item))
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return [_announcement_json_value(item) for item in value]
    item_method = getattr(value, "item", None)
    if callable(item_method):
        scalar = item_method()
        if scalar is not value:
            return _announcement_json_value(scalar)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        rendered = isoformat()
        if isinstance(rendered, str):
            return rendered
    raise RuntimeError(
        "Big QMT announcement scalar shape is unsupported: %s"
        % type(value).__name__
    )


def _announcement_code_set_sha256(symbols):
    ordered = sorted(set(str(symbol) for symbol in symbols))
    encoded = json.dumps(
        ordered, ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _announcement_frames(C, params):
    symbols = _normalize_codes(params.get("stock_codes"))
    start_time = _date_digits(params.get("start_date"))[:14]
    end_time = _date_digits(params.get("end_date"))[:14]
    if not symbols or len(start_time) != 14 or len(end_time) != 14:
        raise ValueError("Big QMT announcement request scope is invalid")
    download_history = bool(params.get("download_history", True))
    if download_history:
        _download_announcement_history(symbols, start_time, end_time)
    raw_reader = getattr(C, "get_market_data_ex", None)
    source_method = "ContextInfo.get_market_data_ex"
    if not callable(raw_reader):
        raw_reader = getattr(C, "get_market_data_ex_ori", None)
        source_method = "ContextInfo.get_market_data_ex_ori"
    if not callable(raw_reader):
        raise RuntimeError("Big QMT announcement ContextInfo reader is unavailable")
    data = raw_reader(
        [], symbols, period="announcement", start_time=start_time,
        end_time=end_time, count=-1, dividend_type="none",
        fill_data=False, subscribe=False
    )
    if not isinstance(data, dict):
        raise RuntimeError("Big QMT announcement response is not a stock map")
    frames = {}
    observed_row_count = 0
    estimated_uncompressed_bytes = 0
    for raw_symbol, frame in data.items():
        symbol = _valid_symbol(raw_symbol)
        if not symbol or symbol in frames:
            raise RuntimeError("Big QMT announcement response stock identity differs")
        payload = _announcement_frame_payload(frame)
        frames[symbol] = payload
        observed_row_count += len(payload["rows"])
        estimated_uncompressed_bytes += int(
            payload["estimated_uncompressed_bytes"]
        )
    if (
        observed_row_count > MAX_ANNOUNCEMENT_BATCH_ROWS
        or estimated_uncompressed_bytes > MAX_ANNOUNCEMENT_BATCH_JSON_BYTES
    ):
        raise RuntimeError("Big QMT announcement response exceeds batch limits")
    contract = {
        "frames": frames,
        "source_method": source_method,
        "period": "announcement",
        "count": -1,
        "dividend_type": "none",
        "fill_data": False,
        "subscribe": False,
        "download_history": download_history,
        "requested_start_time": start_time,
        "requested_end_time": end_time,
        "requested_stock_count": len(symbols),
        "requested_stock_set_sha256": _announcement_code_set_sha256(symbols),
        "observed_stock_count": len(frames),
        "observed_stock_set_sha256": _announcement_code_set_sha256(frames),
        "observed_row_count": observed_row_count,
        "estimated_uncompressed_bytes": estimated_uncompressed_bytes,
    }
    receipt_payload = dict(contract)
    contract["capture_receipt_sha256"] = hashlib.sha256(json.dumps(
        receipt_payload, ensure_ascii=True, sort_keys=True,
        separators=(",", ":"), allow_nan=False
    ).encode("utf-8")).hexdigest()
    return contract


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


def _capabilities_payload(C):
    payload = {
        "schema_version": 3,
        "status": "ok",
        "source": "gj_big_qmt_inner",
        "bridge_version": BRIDGE_VERSION,
        "strategy_release_protocol": STRATEGY_RELEASE_PROTOCOL,
        "read_only": True,
        "simulation_only": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "pandas_free_history": True,
        "model_instance_id": _model_instance_id,
        "model_started_ts": _model_started_ts,
        "generated_at": _now_text(),
        "generated_ts": time.time(),
        "actions": [
            "current", "kline", "minute", "sector_list", "sector_members_many",
            "instrument_details", "index_members_many", "trading_calendar",
            "announcement", "minute_flow_exact"
        ],
        "native_capabilities": [{
            "capability": "trading_calendar",
            "action": "trading_calendar",
            "available": callable(getattr(C, "get_trading_dates", None)),
            "source_method": "ContextInfo.get_trading_dates",
        }, {
            "capability": "announcement",
            "action": "announcement",
            "available": callable(getattr(C, "get_market_data_ex_ori", None))
            or callable(getattr(C, "get_market_data_ex", None)),
            "source_method": "ContextInfo.get_market_data_ex(_ori)",
        }, {
            "capability": "index_weight",
            "action": "index_members_many",
            "available": False,
            "source_method": "membership_only_no_native_weight",
        }],
    }
    payload.update(_strategy_identity_payload())
    return payload


def _execute_request(C, action, params):
    if action in ("ping", "capabilities"):
        return _capabilities_payload(C)
    if action == "current":
        return {"rows": _current_rows(C, params)}
    if action == "kline":
        return {"rows": _market_rows(C, params, "1d")}
    if action == "minute":
        return {"rows": _market_rows(C, params, "1m")}
    if action == "minute_flow_exact":
        return _minute_flow_capture(C, params)
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
    if action == "trading_calendar":
        return _trading_calendar_rows(C, params)
    if action == "announcement":
        return _announcement_frames(C, params)
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


def _request_deadline_expired(payload):
    try:
        return float(payload.get("deadline_ts") or 0) > 0 and time.time() > float(
            payload.get("deadline_ts")
        )
    except Exception:
        return False


def _ensure_queue_roots():
    global _inflight_root, _checkpoints_root, _dead_letter_root, _cancelled_root
    if not _bridge_root:
        return
    expected = {
        "inflight": os.path.join(_bridge_root, "inflight"),
        "checkpoints": os.path.join(_bridge_root, "checkpoints"),
        "dead_letter": os.path.join(_bridge_root, "dead_letter"),
        "cancelled": os.path.join(_bridge_root, "cancelled"),
    }
    _inflight_root = expected["inflight"]
    _checkpoints_root = expected["checkpoints"]
    _dead_letter_root = expected["dead_letter"]
    _cancelled_root = expected["cancelled"]
    for directory in (
        _inflight_root, _checkpoints_root, _dead_letter_root, _cancelled_root,
    ):
        if not os.path.isdir(directory):
            os.makedirs(directory)


def _request_cancelled(request_id):
    return os.path.isfile(os.path.join(_cancelled_root, request_id + ".json"))


def _dead_letter_claim(path, request_id, reason, payload=None):
    target = os.path.join(
        _dead_letter_root,
        "%s_%s.json" % (request_id, int(time.time() * 1000)),
    )
    try:
        os.replace(path, target)
    except OSError:
        return False
    _atomic_json_path(target + ".meta.json", {
        "schema_version": 1,
        "request_id": request_id,
        "reason": reason,
        "dead_lettered_at": _now_text(),
        "dead_lettered_ts": time.time(),
        "request": payload or {},
    })
    return True


def _write_request_checkpoint(request_payload, phase, **extra):
    request_id = str(request_payload.get("request_id") or "unknown")
    checkpoint = {
        "schema_version": 1,
        "request_id": request_id,
        "action": str(request_payload.get("action") or ""),
        "run_id": str(request_payload.get("run_id") or ""),
        "build_id": str(request_payload.get("build_id") or ""),
        "attempt": int(request_payload.get("attempt") or 1),
        "cursor": int(request_payload.get("cursor") or 0),
        "phase": phase,
        "model_instance_id": _model_instance_id,
        "updated_at": _now_text(),
        "updated_ts": time.time(),
    }
    checkpoint.update(extra)
    _atomic_json_path(
        os.path.join(_checkpoints_root, request_id + ".json"), checkpoint
    )


def _recover_inflight_requests():
    _ensure_queue_roots()
    try:
        names = [
            name for name in os.listdir(_inflight_root)
            if name.endswith(".json")
        ]
    except OSError:
        return
    for name in names:
        path = os.path.join(_inflight_root, name)
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            request_id = str(payload.get("request_id") or name[:-5])
            response_path = os.path.join(
                _responses_root, request_id + ".json.gz"
            )
            if os.path.isfile(response_path):
                os.remove(path)
            elif _request_cancelled(request_id):
                _dead_letter_claim(path, request_id, "cancelled_during_restart", payload)
            elif _request_deadline_expired(payload):
                _dead_letter_claim(path, request_id, "deadline_expired_during_restart", payload)
            else:
                os.replace(path, os.path.join(_requests_root, name))
        except Exception:
            _dead_letter_claim(path, name[:-5], "inflight_recovery_failed")


def _cleanup_queue_artifacts(force=False):
    global _last_queue_cleanup
    now_ts = time.time()
    if not force and now_ts - _last_queue_cleanup < 300:
        return
    _last_queue_cleanup = now_ts
    policies = (
        (_responses_root, 7 * 86400),
        (_checkpoints_root, 7 * 86400),
        (_cancelled_root, 7 * 86400),
        (_dead_letter_root, 30 * 86400),
    )
    removed = 0
    for directory, max_age in policies:
        try:
            names = os.listdir(directory)
        except OSError:
            continue
        for name in names:
            if removed >= 100:
                return
            path = os.path.join(directory, name)
            try:
                if (
                    os.path.isfile(path)
                    and now_ts - os.path.getmtime(path) > max_age
                ):
                    os.remove(path)
                    removed += 1
            except OSError:
                continue


def _process_one_request(C):
    global _last_request_at, _last_request_action, _last_error
    _ensure_queue_roots()
    try:
        names = sorted(name for name in os.listdir(_requests_root) if name.endswith(".json"))
    except OSError:
        return False
    if not names:
        return False
    name = names[0]
    pending_path = os.path.join(_requests_root, name)
    path = os.path.join(_inflight_root, name)
    try:
        os.replace(pending_path, path)
    except OSError:
        return False
    request_id = name[:-5]
    response_path = ""
    request_payload = {}
    dead_lettered = False
    try:
        with open(path, "r", encoding="utf-8") as handle:
            request_payload = json.load(handle)
        request_id = str(request_payload.get("request_id") or request_id)
        response_path = os.path.join(_responses_root, request_id + ".json.gz")
        action = str(request_payload.get("action") or "").strip()
        params = request_payload.get("params") or {}
        if os.path.isfile(response_path):
            return True
        if _request_cancelled(request_id):
            dead_lettered = _dead_letter_claim(
                path, request_id, "cancelled_before_execution", request_payload
            )
            return True
        if _request_deadline_expired(request_payload):
            dead_lettered = _dead_letter_claim(
                path, request_id, "deadline_expired_before_execution", request_payload
            )
            return True
        _last_request_at = _now_text()
        _last_request_action = action
        _write_request_checkpoint(request_payload, "CLAIMED")
        _write_heartbeat("busy")
        result = _execute_request(C, action, params)
        if _request_cancelled(request_id):
            dead_lettered = _dead_letter_claim(
                path, request_id, "cancelled_during_execution", request_payload
            )
            return True
        response = {
            "schema_version": 3,
            "request_id": request_id,
            "action": action,
            "status": "ok",
            "source": "gj_big_qmt_inner",
            "bridge_version": BRIDGE_VERSION,
            "generated_at": _now_text(),
            "model_instance_id": _model_instance_id,
            "run_id": str(request_payload.get("run_id") or ""),
            "build_id": str(request_payload.get("build_id") or ""),
            "attempt": int(request_payload.get("attempt") or 1),
            "cursor": int(request_payload.get("cursor") or 0),
        }
        response.update(_strategy_identity_payload())
        response.update(result)
        _atomic_gzip_write(response_path, response)
        _write_request_checkpoint(request_payload, "COMPLETED")
        _last_error = ""
    except Exception:
        _last_error = traceback.format_exc()[-4000:]
        if response_path and not _request_cancelled(request_id):
            _atomic_gzip_write(response_path, {
                "schema_version": 3,
                "request_id": request_id,
                "status": "error",
                "source": "gj_big_qmt_inner",
                "bridge_version": BRIDGE_VERSION,
                "generated_at": _now_text(),
                "model_instance_id": _model_instance_id,
                "error": _last_error,
            })
        if request_payload:
            _write_request_checkpoint(
                request_payload, "ERROR", error=_last_error
            )
    finally:
        if not dead_lettered:
            try:
                os.remove(path)
            except OSError:
                pass
    return True


def _write_heartbeat(status):
    global _heartbeat_seq
    _ensure_queue_roots()
    _heartbeat_seq += 1
    try:
        pending_names = [
            name for name in os.listdir(_requests_root)
            if name.endswith(".json")
        ]
    except Exception:
        pending_names = []
    pending = len(pending_names)
    oldest_request_age = None
    if pending_names:
        try:
            oldest_request_age = max(
                0.0,
                time.time() - min(
                    os.path.getmtime(os.path.join(_requests_root, name))
                    for name in pending_names
                ),
            )
        except Exception:
            oldest_request_age = None
    try:
        inflight = len([
            name for name in os.listdir(_inflight_root)
            if name.endswith(".json")
        ])
    except Exception:
        inflight = 0
    oldest_inflight_age = None
    if inflight:
        try:
            oldest_inflight_age = max(
                0.0,
                time.time() - min(
                    os.path.getmtime(os.path.join(_inflight_root, name))
                    for name in os.listdir(_inflight_root)
                    if name.endswith(".json")
                ),
            )
        except Exception:
            oldest_inflight_age = None
    payload = {
        "schema_version": 3,
        "bridge_version": BRIDGE_VERSION,
        "strategy_release_protocol": STRATEGY_RELEASE_PROTOCOL,
        "source": "gj_big_qmt_inner",
        "status": status,
        "updated_at": _now_text(),
        "updated_ts": time.time(),
        "pid": os.getpid(),
        "model_instance_id": _model_instance_id,
        "model_started_ts": _model_started_ts,
        "heartbeat_seq": _heartbeat_seq,
        "all_code_count": len(_all_codes),
        "tracked_code_count": len(_tracked_codes),
        "tracked_quote_count": len(_tracked_quotes),
        "subscription_id": _subscription_id,
        "last_callback_at": _last_callback_at,
        "last_callback_ts": _last_callback_ts,
        "callback_batch_count": _callback_batch_count,
        "last_full_refresh_ts": _last_full_refresh,
        "quote_acquisition_mode": "whole_quote_cache" if _quote_phase_key and _quote_phase_key[0] == "live" else "closing_quote_cache",
        "quote_acquisition_slot": _quote_phase_key,
        "quote_cache_count": len(_quote_cache),
        "quote_seed_remaining": len(_seed_pending),
        "last_market_callback_ts": _last_market_callback_ts,
        "pending_request_count": pending,
        "inflight_request_count": inflight,
        "oldest_pending_request_age_seconds": oldest_request_age,
        "oldest_inflight_request_age_seconds": oldest_inflight_age,
        "last_request_at": _last_request_at,
        "last_request_action": _last_request_action,
        "last_error": _last_error,
        "direct_acquisition_model_sha256": getattr(
            _direct_model, "source_sha256", ""
        ),
        "direct_acquisition_status": getattr(
            _direct_model, "last_status", "unavailable"
        ),
    }
    payload.update(_strategy_identity_payload())
    _atomic_write("heartbeat.json", payload)


def bridge_tick(C):
    global _last_error
    # Skip overlapping/reentrant timer invocations instead of accumulating
    # waiters in QMT's native scheduler.
    if _stopping or not _execution_lock.acquire(False):
        return
    try:
        cached_context = _QuoteCacheContext(C)
        try:
            _refresh_subscription(C, force=False)
            if _stopping:
                return
            _process_one_request(cached_context)
            if _stopping:
                return
            _refresh_full_snapshot(C)
            _write_tracked_snapshot(force=False)
            _cleanup_queue_artifacts()
            _last_error = ""
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
        # Preserve independent acquisition progress when quote publication
        # fails, while keeping every native request on the same timer.
        if _stopping:
            return
        _poll_direct_acquisition(cached_context)
        if _stopping:
            return
        _write_heartbeat(
            "error" if _last_error or _direct_model is None or
            (_subscribed_codes and _quote_phase_key and
             _quote_phase_key[0] == "live" and _subscription_id is None) else "running"
        )
    finally:
        try:
            if _stopping:
                _finish_stop(C)
        finally:
            _execution_lock.release()


def _poll_direct_acquisition(C):
    if _direct_model is None:
        return
    try:
        _direct_model.poll(C)
    except Exception:
        try:
            _direct_model.heartbeat("error", "ADAPTER_FAILURE")
        except Exception:
            pass


def init(C):
    global _bridge_root, _config_path, _requests_root, _responses_root
    global _inflight_root, _checkpoints_root, _dead_letter_root, _cancelled_root
    global _last_error, _model_instance_id, _model_started_ts, _heartbeat_seq
    global _direct_model
    if not callable(getattr(C, "schedule_run", None)) or not callable(
            getattr(C, "cancel_schedule_run", None)):
        raise RuntimeError("QMT bridge requires cancellable schedule_run timers")
    if _model_instance_id:
        raise RuntimeError("QMT bridge instance has already been initialized")
    with _execution_lock:
        _model_instance_id = uuid.uuid4().hex
        _model_started_ts = time.time()
        _heartbeat_seq = 0
        _bridge_root = _find_bridge_root()
        _config_path = os.path.join(_bridge_root, "watchlist.json")
        _requests_root = os.path.join(_bridge_root, "requests")
        _responses_root = os.path.join(_bridge_root, "responses")
        _inflight_root = os.path.join(_bridge_root, "inflight")
        _checkpoints_root = os.path.join(_bridge_root, "checkpoints")
        _dead_letter_root = os.path.join(_bridge_root, "dead_letter")
        _cancelled_root = os.path.join(_bridge_root, "cancelled")
        _direct_model = None
        for directory in (
            _requests_root, _responses_root, _inflight_root,
            _checkpoints_root, _dead_letter_root, _cancelled_root,
        ):
            if not os.path.isdir(directory):
                os.makedirs(directory)
        _enable_fault_log()
        _recover_inflight_requests()
        try:
            _direct_model = _load_direct_acquisition_model()
            _direct_model.heartbeat("idle")
            _refresh_subscription(C, force=True)
            _atomic_write("capabilities.json", _capabilities_payload(C))
            _last_error = ""
            _write_heartbeat("starting")
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
            _write_heartbeat("error")
    _schedule_next_tick(C)


def after_init(C):
    # init registered the sole execution entry. Do not run a second native
    # acquisition pass while the host is completing model initialization.
    return


def handlebar(C):
    return


def stop(C):
    global _stopping, _timer_id, _timer_generation, _last_error
    _stopping = True
    _timer_generation += 1
    timer_id, _timer_id = _timer_id, None
    if timer_id is not None:
        try:
            C.cancel_schedule_run(timer_id)
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
    # A native request may be waiting for this lifecycle callback. Never
    # block the host on our lock; the active pass finishes teardown on exit.
    if not _execution_lock.acquire(False):
        return
    try:
        _finish_stop(C)
    finally:
        _execution_lock.release()


def _finish_stop(C):
    global _subscription_id, _last_error, _stop_completed
    if _stop_completed:
        return
    _stop_completed = True
    if _subscription_id is not None:
        try:
            C.unsubscribe_quote(_subscription_id)
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
        _subscription_id = None
    if _direct_model is not None:
        try:
            _direct_model.heartbeat("stopped")
        except Exception:
            _last_error = traceback.format_exc()[-2000:]
    _write_heartbeat("stopped")
