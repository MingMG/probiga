from __future__ import annotations

import json
import gzip
import hashlib
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from integrations.qmt.backend import from_qmt_symbol, to_qmt_symbol


PROVIDER_ID = "gj_big_qmt_inner"
BRIDGE_DIR_NAME = "probiga_bridge"
STRATEGY_FILE_NAME = "probiga_big_qmt_bridge.py"
LEGACY_STRATEGY_FILE_NAMES = ("PROBIGA_BIGQMT_BRIDGE.py",)
CONTROL_ACTIONS = {"ping", "capabilities"}
REALTIME_ACTIONS = {"current"}
BULK_ACTIONS = {
    "announcement",
    "index_members_many",
    "instrument_details",
    "kline",
    "minute",
    "sector_members_many",
    "trading_calendar",
}


def _replace_with_retry(
    temporary: Path,
    path: Path,
    *,
    retry_seconds: float = 2.0,
    retry_interval: float = 0.02,
) -> None:
    """Replace a bridge file after transient Windows sharing locks clear."""
    deadline = time.monotonic() + max(0.0, float(retry_seconds))
    while True:
        try:
            os.replace(temporary, path)
            return
        except OSError as exc:
            transient = isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in {5, 32, 33}
            if not transient or time.monotonic() >= deadline:
                raise
            time.sleep(max(0.01, float(retry_interval)))


def _read_json_file(
    path: Path,
    *,
    retry_seconds: float = 2.0,
    retry_interval: float = 0.02,
) -> dict[str, Any]:
    """Copy file contents quickly, close the handle, then parse JSON."""
    deadline = time.monotonic() + max(0.0, float(retry_seconds))
    while True:
        try:
            with path.open("r", encoding="utf-8") as handle:
                raw = handle.read()
            payload = json.loads(raw)
            break
        except (PermissionError, FileNotFoundError, json.JSONDecodeError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(max(0.01, float(retry_interval)))
    if not isinstance(payload, dict):
        raise ValueError(f"Big QMT bridge file is not a JSON object: {path}")
    return payload


def _is_big_qmt_home(path: Path) -> bool:
    return (
        path.is_dir()
        and (path / "bin.x64" / "XtItClient.exe").is_file()
        and (path / "userdata").is_dir()
    )


def _candidate_homes() -> Iterable[Path]:
    seen: set[str] = set()
    for env_name in ("BIG_QMT_HOME", "GJ_QMT_HOME", "QMT_HOME"):
        raw = os.environ.get(env_name, "").strip().strip('"')
        if raw:
            candidate = Path(raw).expanduser()
            key = str(candidate).casefold()
            if key not in seen:
                seen.add(key)
                yield candidate

    known = (
        Path("D:/国金证券QMT交易端"),
        Path("C:/国金证券QMT交易端"),
        Path("D:/QMT"),
        Path("C:/QMT"),
    )
    for candidate in known:
        key = str(candidate).casefold()
        if key not in seen:
            seen.add(key)
            yield candidate

    for drive in (Path("D:/"), Path("C:/")):
        if not drive.exists():
            continue
        try:
            children = drive.iterdir()
        except OSError:
            continue
        for child in children:
            key = str(child).casefold()
            if key in seen:
                continue
            seen.add(key)
            yield child


def resolve_big_qmt_home(required: bool = True) -> Path | None:
    for candidate in _candidate_homes():
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate
        if _is_big_qmt_home(resolved):
            return resolved
    if required:
        raise RuntimeError(
            "Standard QMT was not found. Set BIG_QMT_HOME to the directory "
            "containing bin.x64/XtItClient.exe and userdata."
        )
    return None


def bridge_dir(qmt_home: Path | str | None = None) -> Path:
    home = Path(qmt_home) if qmt_home else resolve_big_qmt_home(required=True)
    assert home is not None
    return home / "userdata" / BRIDGE_DIR_NAME


def bridge_paths(qmt_home: Path | str | None = None) -> dict[str, Path]:
    root = bridge_dir(qmt_home)
    return {
        "root": root,
        "watchlist": root / "watchlist.json",
        "tracked": root / "tracked_quotes.json",
        "full": root / "full_quotes.json",
        "heartbeat": root / "heartbeat.json",
        "consumer_status": root / "consumer_status.json",
        "capabilities": root / "capabilities.json",
        "requests": root / "requests",
        "responses": root / "responses",
        "inflight": root / "inflight",
        "checkpoints": root / "checkpoints",
        "dead_letter": root / "dead_letter",
        "cancelled": root / "cancelled",
    }


def _request_priority(action: str, value: int | None) -> int:
    if value is not None:
        return max(0, min(999, int(value)))
    if action in CONTROL_ACTIONS:
        return 0
    if action in REALTIME_ACTIONS:
        return 10
    if action in BULK_ACTIONS:
        return 90
    return 50


def _atomic_json_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"), default=str)
        handle.flush()
        os.fsync(handle.fileno())
    _replace_with_retry(temporary, path)


def write_watchlist(
    *,
    all_codes: Iterable[str],
    tracked_codes: Iterable[str],
    qmt_home: Path | str | None = None,
    full_refresh_seconds: int = 30,
    tracked_flush_seconds: float = 1.0,
    full_batch_size: int = 800,
) -> Path:
    def normalize(items: Iterable[str]) -> list[str]:
        values: list[str] = []
        seen: set[str] = set()
        for item in items:
            symbol = to_qmt_symbol(str(item))
            if symbol and symbol not in seen:
                seen.add(symbol)
                values.append(symbol)
        return values

    all_symbols = normalize(all_codes)
    tracked_symbols = normalize(tracked_codes)
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "all_codes": all_symbols,
        "tracked_codes": tracked_symbols,
        "full_refresh_seconds": max(5, int(full_refresh_seconds)),
        "tracked_flush_seconds": max(0.2, float(tracked_flush_seconds)),
        "full_batch_size": max(50, int(full_batch_size)),
    }
    path = bridge_paths(qmt_home)["watchlist"]
    try:
        current = read_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        current = {}
    comparable_keys = (
        "schema_version",
        "all_codes",
        "tracked_codes",
        "full_refresh_seconds",
        "tracked_flush_seconds",
        "full_batch_size",
    )
    if current and all(current.get(key) == payload.get(key) for key in comparable_keys):
        return path
    _atomic_json_write(path, payload)
    return path


def install_qmt_strategy(
    *,
    qmt_home: Path | str | None = None,
    source_path: Path | str | None = None,
) -> Path:
    home = Path(qmt_home) if qmt_home else resolve_big_qmt_home(required=True)
    assert home is not None
    source = Path(source_path) if source_path else Path(__file__).parent / "qmt_strategy" / STRATEGY_FILE_NAME
    if not source.is_file():
        raise FileNotFoundError(source)
    target = home / "python" / STRATEGY_FILE_NAME
    target.parent.mkdir(parents=True, exist_ok=True)
    targets = [
        target,
        target.with_name(STRATEGY_FILE_NAME.upper()),
        *(target.with_name(name) for name in LEGACY_STRATEGY_FILE_NAMES),
    ]
    # Some QMT installations keep strategy files in a case-sensitive Python
    # directory even on Windows.  The editor may have registered an uppercase
    # alias while the installer writes the canonical lowercase filename.
    # Refresh every case-fold-equivalent alias so QMT cannot keep executing a
    # stale copy.
    for existing in target.parent.iterdir():
        if (
            existing.is_file()
            and existing.name.casefold() == STRATEGY_FILE_NAME.casefold()
            and str(existing) not in {str(item) for item in targets}
        ):
            targets.append(existing)
    installed_paths: set[str] = set()
    for installed_target in targets:
        if str(installed_target) in installed_paths:
            continue
        # QMT keeps the already-running model in memory while a release is
        # prepared.  Publish each on-disk alias with an atomic replace so the
        # model editor can never observe a partially copied Python file.  The
        # release installer verifies every alias before the UI control plane
        # is allowed to stop the old in-memory model.
        temporary_target = installed_target.with_name(
            f".{installed_target.name}.{os.getpid()}.tmp"
        )
        try:
            shutil.copy2(source, temporary_target)
            with temporary_target.open("r+b") as handle:
                os.fsync(handle.fileno())
            _replace_with_retry(temporary_target, installed_target)
        finally:
            temporary_target.unlink(missing_ok=True)
        installed_paths.add(str(installed_target))
    bridge_dir(home).mkdir(parents=True, exist_ok=True)
    return target


def read_json(path: Path, *, max_age_seconds: float | None = None) -> dict[str, Any]:
    if not path.is_file():
        return {}
    if max_age_seconds is not None:
        age = max(0.0, time.time() - path.stat().st_mtime)
        if age > float(max_age_seconds):
            raise RuntimeError(f"Big QMT bridge file is stale: {path} age={age:.1f}s")
    return _read_json_file(path)


def _read_gzip_json(
    path: Path,
    *,
    retry_seconds: float = 3.0,
    retry_interval: float = 0.05,
) -> dict[str, Any]:
    """Read an atomic QMT response after Windows releases the file handle.

    ``os.replace`` makes the payload complete before it becomes visible, but
    antivirus/indexing filters can still hold the newly renamed gzip file for
    a few milliseconds on Windows.  Treat those transient sharing violations
    like the producer-not-finished state instead of failing the whole batch.
    """
    deadline = time.monotonic() + max(0.0, float(retry_seconds))
    while True:
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
            break
        except (PermissionError, FileNotFoundError, EOFError, gzip.BadGzipFile, json.JSONDecodeError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(max(0.01, float(retry_interval)))
    if not isinstance(payload, dict):
        raise ValueError(f"Big QMT bridge response is not a JSON object: {path}")
    return payload


def request(
    action: str,
    *,
    qmt_home: Path | str | None = None,
    timeout: float = 180.0,
    poll_seconds: float = 0.2,
    priority: int | None = None,
    idempotency_key: str | None = None,
    run_id: str | None = None,
    build_id: str | None = None,
    attempt: int = 1,
    cursor: int = 0,
    **params: Any,
) -> dict[str, Any]:
    """Send one command to the standard-QMT built-in strategy.

    Requests and responses are separate immutable files, so concurrent
    scheduler jobs cannot overwrite each other.  The producer writes a gzip
    response atomically; large minute-bar batches therefore stay bounded on
    disk and no miniQMT socket/runtime is involved.
    """
    normalized_action = str(action or "").strip()
    if not normalized_action:
        raise ValueError("Big QMT bridge action is required")

    paths = bridge_paths(qmt_home)
    for key in (
        "requests", "responses", "inflight", "checkpoints", "dead_letter",
        "cancelled",
    ):
        paths[key].mkdir(parents=True, exist_ok=True)
    normalized_idempotency_key = str(idempotency_key or "").strip()
    if normalized_idempotency_key:
        request_id = "idem_" + hashlib.sha256(
            normalized_idempotency_key.encode("utf-8")
        ).hexdigest()[:48]
    else:
        request_id = f"{time.time_ns()}_{os.getpid()}_{uuid.uuid4().hex[:10]}"
    request_priority = _request_priority(normalized_action, priority)
    request_name = f"{request_priority:03d}_{time.time_ns()}_{request_id}.json"
    request_path = paths["requests"] / request_name
    response_path = paths["responses"] / f"{request_id}.json.gz"
    cancellation_path = paths["cancelled"] / f"{request_id}.json"
    if normalized_idempotency_key and response_path.is_file():
        response = _read_gzip_json(response_path)
        if str(response.get("request_id") or "") == request_id:
            if str(response.get("status") or "").lower() != "ok":
                detail = str(response.get("error") or "unknown standard-QMT error")
                raise RuntimeError(f"Big QMT {normalized_action} failed: {detail}")
            return response
    created_ts = time.time()
    deadline_ts = created_ts + max(1.0, float(timeout))
    payload = {
        "schema_version": 3,
        "request_id": request_id,
        "action": normalized_action,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "created_ts": created_ts,
        "deadline_ts": deadline_ts,
        "priority": request_priority,
        "idempotency_key": normalized_idempotency_key,
        "run_id": str(run_id or ""),
        "build_id": str(build_id or ""),
        "attempt": max(1, int(attempt)),
        "cursor": max(0, int(cursor)),
        "params": params,
    }
    _atomic_json_write(request_path, payload)

    deadline = time.monotonic() + max(1.0, float(timeout))
    completed = False
    try:
        while time.monotonic() < deadline:
            if response_path.is_file():
                response = _read_gzip_json(response_path)
                if str(response.get("request_id") or "") != request_id:
                    raise RuntimeError("Big QMT bridge returned a mismatched request id")
                completed = True
                if str(response.get("status") or "").lower() != "ok":
                    detail = str(response.get("error") or "unknown standard-QMT error")
                    raise RuntimeError(f"Big QMT {normalized_action} failed: {detail}")
                return response
            time.sleep(max(0.05, float(poll_seconds)))
        heartbeat = read_json(paths["heartbeat"])
        status = str(heartbeat.get("status") or "missing")
        updated = str(heartbeat.get("updated_at") or "unknown")
        raise TimeoutError(
            f"Big QMT {normalized_action} timed out after {float(timeout):.1f}s "
            f"(strategy status={status}, heartbeat={updated})"
        )
    finally:
        if not completed:
            try:
                _atomic_json_write(
                    cancellation_path,
                    {
                        "schema_version": 1,
                        "request_id": request_id,
                        "action": normalized_action,
                        "cancelled_at": datetime.now(timezone.utc).isoformat(
                            timespec="seconds"
                        ),
                        "cancelled_ts": time.time(),
                        "reason": "caller_timeout_or_exit",
                    },
                )
                if request_path.is_file():
                    os.replace(request_path, paths["cancelled"] / request_name)
            except OSError:
                pass
        if not normalized_idempotency_key:
            try:
                response_path.unlink(missing_ok=True)
            except OSError:
                pass


def read_snapshot(
    kind: str,
    *,
    qmt_home: Path | str | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any]:
    normalized = str(kind).strip().lower()
    if normalized not in {"tracked", "full"}:
        raise ValueError(f"unsupported Big QMT snapshot kind: {kind}")
    return read_json(bridge_paths(qmt_home)[normalized], max_age_seconds=max_age_seconds)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(number):
        return default
    return number


def _level_value(value: Any, index: int = 0) -> float | None:
    """Return one QMT order-book level without inventing missing values."""
    if not isinstance(value, (list, tuple)) or len(value) <= index:
        return None
    number = _float(value[index], default=float("nan"))
    if pd.isna(number) or number <= 0:
        return None
    return number


def _level_quantity(value: Any, index: int = 0) -> int | None:
    number = _level_value(value, index)
    if number is None:
        return None
    return max(0, int(number))


def _source_time(tick: Mapping[str, Any], fallback: datetime) -> datetime:
    raw_time = tick.get("time")
    try:
        timestamp = float(raw_time)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        if timestamp > 1_000_000_000:
            return datetime.fromtimestamp(timestamp).replace(microsecond=0)
    except (TypeError, ValueError, OSError, OverflowError):
        pass

    for key, formats in (
        ("stime", ("%Y%m%d%H%M%S.%f", "%Y%m%d%H%M%S")),
        ("timetag", ("%Y%m%d %H:%M:%S", "%Y-%m-%d %H:%M:%S")),
    ):
        raw = str(tick.get(key) or "").strip()
        for fmt in formats:
            try:
                return datetime.strptime(raw, fmt).replace(microsecond=0)
            except ValueError:
                continue
    return fallback


def snapshot_frame(
    payload: Mapping[str, Any],
    *,
    short_name_map: Mapping[str, str] | None = None,
    received_at: datetime | None = None,
) -> pd.DataFrame:
    quotes = payload.get("quotes", payload)
    if not isinstance(quotes, Mapping):
        return pd.DataFrame()
    names = short_name_map or {}
    now = (received_at or datetime.now()).replace(microsecond=0)
    batch_id = str(payload.get("batch_id") or f"bigqmt_{now.strftime('%Y%m%d%H%M%S')}")
    rows: list[dict[str, Any]] = []

    for raw_symbol, raw_tick in quotes.items():
        if not isinstance(raw_tick, Mapping):
            continue
        symbol = to_qmt_symbol(str(raw_symbol))
        if not symbol:
            continue
        code = from_qmt_symbol(symbol)
        pre_close = _float(raw_tick.get("lastClose", raw_tick.get("preClose")))
        last_price = _float(raw_tick.get("lastPrice", raw_tick.get("close")))
        price = last_price if last_price > 0 else pre_close
        if price <= 0:
            continue
        change = price - pre_close if pre_close > 0 else 0.0
        change_pct = change / pre_close * 100.0 if pre_close > 0 else 0.0
        source_time = _source_time(raw_tick, now)
        quote_received_at = (
            raw_tick.get("_probiga_received_at")
            or raw_tick.get("probigaReceivedAt")
            or now
        )
        bid1 = _level_value(raw_tick.get("bidPrice"))
        ask1 = _level_value(raw_tick.get("askPrice"))
        bid1_volume = _level_quantity(raw_tick.get("bidVol"))
        ask1_volume = _level_quantity(raw_tick.get("askVol"))
        rows.append(
            {
                "stock_code": code,
                "short_name": str(names.get(code) or raw_tick.get("name") or "").strip(),
                "price": price,
                "change": change,
                "change_pct": change_pct,
                "volume": max(0.0, _float(raw_tick.get("volume", raw_tick.get("pvolume")))),
                "amount": max(0.0, _float(raw_tick.get("amount"))),
                "snapshot_at": source_time,
                "etl_sync_at": now,
                "qmt_code": symbol,
                "data_source": PROVIDER_ID,
                "source_time": source_time,
                "received_at": quote_received_at,
                "bid1": bid1,
                "bid1_volume": bid1_volume,
                "ask1": ask1,
                "ask1_volume": ask1_volume,
                "pre_close": pre_close if pre_close > 0 else None,
                "stock_status": raw_tick.get("stockStatus"),
                "batch_id": batch_id,
                "data_version": "bigqmt_inner_v2",
                "quality_status": "VALIDATED",
                "permission_status": "SUPPORTED",
            }
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(subset=["stock_code"], keep="last")


def merge_snapshot_frames(full: pd.DataFrame, tracked: pd.DataFrame) -> pd.DataFrame:
    frames = [frame for frame in (full, tracked) if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["stock_code"], keep="last")
