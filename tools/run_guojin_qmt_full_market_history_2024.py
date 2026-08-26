from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import time
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.local_history import (
    LOCAL_KLINE_TABLE,
    LOCAL_MINUTE_TABLE,
    backfill_daily_kline_local,
    backfill_minute_local,
    get_local_history_engine,
    validate_local_history_tables,
)
from server.common.config import get_mysql_url
from server.common.qmt_attestation_contract import daily_market_source_batch_id
from server.common.qmt_history_coverage import (
    assess_daily_coverage,
    assess_minute_coverage,
    combine_minute_coverage_partitions,
    insert_coverage_bundle,
    require_exact_coverage,
)
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID
from tools.qmt_operations_task_contract import (
    QMT_FULL_HISTORY_LOCK_PATH,
    QMT_FULL_HISTORY_LOG_PATH,
    QMT_FULL_HISTORY_STATE_ROOT,
)

PRODUCTION_KLINE_TABLE = "sm_stock_kline"
LOCK_INITIALIZATION_GRACE_SECONDS = 5.0
WINDOWS_STATE_DIRECTORY_PARTS = (
    "ProBigA",
    "qmt-full-market-history",
)


def _source_engine():
    return create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = (
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        )
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.DWORD),
        )
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.OpenProcess(
            process_query_limited_information,
            False,
            pid,
        )
        if not handle:
            return False
        try:
            exit_code = wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == still_active
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _windows_state_mapping(program_data: str) -> tuple[str, str, str]:
    """Map the frozen Linux contract into one fixed Windows ProgramData root."""

    from pathlib import PureWindowsPath

    base = PureWindowsPath(str(program_data or "").strip())
    if not base.is_absolute() or not base.drive:
        raise RuntimeError("Windows PROGRAMDATA must be one absolute drive path")
    root = base.joinpath(*WINDOWS_STATE_DIRECTORY_PARTS)
    lock = root / Path(QMT_FULL_HISTORY_LOCK_PATH).name
    log = root / Path(QMT_FULL_HISTORY_LOG_PATH).name
    return str(root), str(lock), str(log)


def _runtime_path_arguments(
    *,
    state_root: str,
    lock_path: str,
    log_path: str,
) -> tuple[Path, Path, Path]:
    """Resolve the exact task paths, including a deterministic Windows map."""

    raw = (
        str(state_root or "").strip(),
        str(lock_path or "").strip(),
        str(log_path or "").strip(),
    )
    if not all(raw):
        raise RuntimeError(
            "--state-root, --lock-path and --log-path are all required"
        )
    if os.name == "nt" and raw == (
        QMT_FULL_HISTORY_STATE_ROOT,
        QMT_FULL_HISTORY_LOCK_PATH,
        QMT_FULL_HISTORY_LOG_PATH,
    ):
        raw = _windows_state_mapping(os.environ.get("PROGRAMDATA", ""))
    paths = tuple(Path(item) for item in raw)
    if not all(path.is_absolute() for path in paths):
        raise RuntimeError("QMT full-history runtime paths must be absolute")
    return paths  # type: ignore[return-value]


def _assert_runtime_file(path: Path, *, owner_uid: int | None) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink():
        raise RuntimeError(f"QMT full-history runtime file is a symlink: {path}")
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError(f"QMT full-history runtime path is not a file: {path}")
    if os.name != "nt":
        if owner_uid is None or info.st_uid != owner_uid:
            raise RuntimeError(
                f"QMT full-history runtime file has the wrong owner: {path}"
            )
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise RuntimeError(
                f"QMT full-history runtime file must have mode 0600: {path}"
            )


def _validated_runtime_paths(
    *,
    state_root: str,
    lock_path: str,
    log_path: str,
) -> tuple[Path, Path, Path]:
    root, lock, log = _runtime_path_arguments(
        state_root=state_root,
        lock_path=lock_path,
        log_path=log_path,
    )
    if not root.exists() or root.is_symlink() or not root.is_dir():
        raise RuntimeError(
            "QMT full-history state root must be a pre-created real directory"
        )
    resolved_root = root.resolve(strict=True)
    if resolved_root != root:
        raise RuntimeError("QMT full-history state root contains a path indirection")
    resolved_code_root = ROOT.resolve(strict=True)
    if _is_relative_to(resolved_root, resolved_code_root):
        raise RuntimeError("QMT full-history state root cannot be inside the code tree")
    if lock.parent != root or log.parent != root or lock == log:
        raise RuntimeError(
            "QMT full-history lock and log must be distinct direct state-root children"
        )
    for child in root.iterdir():
        if child.is_symlink():
            raise RuntimeError(
                f"QMT full-history state root contains a symlink: {child}"
            )
    owner_uid = None if os.name == "nt" else os.geteuid()
    if os.name != "nt":
        info = root.stat()
        if info.st_uid != owner_uid:
            raise RuntimeError("QMT full-history state root has the wrong owner")
        if stat.S_IMODE(info.st_mode) != 0o700:
            raise RuntimeError("QMT full-history state root must have mode 0700")
    if not os.access(root, os.R_OK | os.W_OK | os.X_OK):
        raise RuntimeError("QMT full-history state root is not service-writable")
    _assert_runtime_file(lock, owner_uid=owner_uid)
    _assert_runtime_file(log, owner_uid=owner_uid)
    return root, lock, log


def _acquire_lock(lock_path: Path) -> tuple[bool, str]:
    payload = f"{os.getpid()} {datetime.now().isoformat(timespec='seconds')}"
    owner = ""
    for _attempt in range(3):
        if lock_path.is_symlink():
            return False, "lock_error:symlink"
        try:
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(
                lock_path,
                flags,
                0o600,
            )
        except FileExistsError:
            before = None
            try:
                before = lock_path.stat()
                owner = lock_path.read_text(
                    encoding="utf-8", errors="ignore"
                ).strip()
                existing_pid = int(owner.split()[0])
            except (OSError, ValueError, IndexError):
                existing_pid = 0
            if _pid_alive(existing_pid):
                return False, owner
            if (
                existing_pid <= 0
                and before is not None
                and max(0.0, time.time() - before.st_mtime)
                < LOCK_INITIALIZATION_GRACE_SECONDS
            ):
                return False, owner or "lock_initializing"
            try:
                if before is None:
                    continue
                after = lock_path.stat()
                before_identity = (
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                )
                after_identity = (
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                )
                if before_identity == after_identity:
                    lock_path.unlink()
            except (OSError, UnboundLocalError):
                pass
            continue
        except OSError as exc:
            return False, f"lock_error:{type(exc).__name__}"
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            lock_path.unlink(missing_ok=True)
            raise
        return True, ""
    return False, owner or "stale_lock_could_not_be_replaced"


def _release_lock(lock_path: Path) -> None:
    try:
        if lock_path.is_symlink():
            return
        raw = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
        pid = int(raw.split()[0])
        if pid == os.getpid():
            lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _latest_trade_date(source_engine) -> str:
    today = date.today()
    start = (today - timedelta(days=60)).isoformat()
    with source_engine.begin() as connection:
        receipt = load_trade_calendar_receipt(
            connection,
            start_date=start,
            end_date=today.isoformat(),
            decision_known_at=datetime.now().replace(microsecond=0),
        )
    sessions = receipt.sessions_between(start, today.isoformat())
    if not sessions:
        raise RuntimeError("immutable QMT calendar has no recent trade session")
    return sessions[-1]


def _local_count(local_engine, *, table: str, trade_date: str) -> int:
    native_raw_filter = (
        " AND adjust_type=0 AND pre_close_origin='NATIVE_QMT'"
        f" AND provider='{BIGQMT_PROVIDER_ID}'"
        if table == LOCAL_KLINE_TABLE
        else ""
    )
    with local_engine.begin() as conn:
        return int(
            conn.execute(
                text(
                    f"SELECT COUNT(*) FROM `{table}` WHERE trade_date = :d"
                    f"{native_raw_filter}"
                ),
                {"d": trade_date},
            ).scalar()
            or 0
        )


def _daily_stock_set(
    engine,
    *,
    table: str,
    trade_date: str,
    require_native_qmt: bool,
    source_batch_id: str = "",
) -> set[str]:
    native_raw_filter = (
        " AND adjust_type=0 AND pre_close_origin='NATIVE_QMT'"
        f" AND provider='{BIGQMT_PROVIDER_ID}'"
        if require_native_qmt
        else " AND adjust_type=0"
    )
    source_batch_filter = (
        " AND batch_id=:source_batch_id" if source_batch_id else ""
    )
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"SELECT DISTINCT stock_code FROM `{table}` "
                "WHERE trade_date = :d AND k_type=1 "
                "AND stock_code REGEXP '^(0|3|4|6|8|9)'"
                f"{native_raw_filter}{source_batch_filter}"
            ),
            {"d": trade_date, "source_batch_id": source_batch_id},
        )
        return {
            str(row[0]).strip()
            for row in rows
            if row[0] is not None and str(row[0]).strip()
        }


def _daily_coverage_error(
    *,
    trade_date: str,
    expected: set[str],
    actual: set[str],
) -> RuntimeError:
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    return RuntimeError(
        "QMT daily coverage mismatch after backfill: "
        f"trade_date={trade_date}, expected={len(expected)}, actual={len(actual)}, "
        f"missing={len(missing)} sample={missing[:20]}, "
        f"unexpected={len(unexpected)} sample={unexpected[:20]}"
    )


def _local_daily_rows(
    local_engine,
    *,
    trade_date: str,
    source_batch_id: str,
) -> list[dict[str, Any]]:
    with local_engine.begin() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(
                    f"""SELECT stock_code, trade_date, period, k_type,
                        adjust_type, open, high, low, close, volume, amount,
                        pre_close, pre_close_origin, provider, batch_id
                    FROM `{LOCAL_KLINE_TABLE}`
                    WHERE trade_date=:trade_date AND period='1d' AND k_type=1
                      AND adjust_type=0 AND pre_close_origin='NATIVE_QMT'
                      AND provider=:provider AND batch_id=:batch_id
                    ORDER BY stock_code"""
                ),
                {
                    "trade_date": trade_date,
                    "provider": BIGQMT_PROVIDER_ID,
                    "batch_id": source_batch_id,
                },
            ).mappings()
        ]


def _local_minute_rows(
    local_engine,
    *,
    trade_date: str,
    stock_codes: list[str],
    batch_id: str = "",
) -> list[dict[str, Any]]:
    if not stock_codes:
        return []
    batch_filter = " AND batch_id=:batch_id" if batch_id else ""
    code_parameters = {
        f"stock_code_{index}": code
        for index, code in enumerate(stock_codes)
    }
    code_placeholders = ",".join(
        f":{name}" for name in code_parameters
    )
    with local_engine.begin() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(
                    f"""SELECT stock_code, trade_time, period, price,
                        avg_price, volume, amount, provider, batch_id
                    FROM `{LOCAL_MINUTE_TABLE}`
                    WHERE trade_date=:trade_date AND period='1m'
                      AND provider=:provider{batch_filter}
                      AND stock_code IN ({code_placeholders})
                    ORDER BY stock_code, trade_time"""
                ),
                {
                    "trade_date": trade_date,
                    "provider": BIGQMT_PROVIDER_ID,
                    "batch_id": batch_id,
                    **code_parameters,
                },
            ).mappings()
        ]


def _local_minute_identity(
    local_engine,
    *,
    trade_date: str,
    batch_id: str = "",
) -> tuple[set[str], set[str]]:
    batch_filter = " AND batch_id=:batch_id" if batch_id else ""
    with local_engine.begin() as connection:
        rows = connection.execute(
            text(
                f"""SELECT DISTINCT stock_code, batch_id
                FROM `{LOCAL_MINUTE_TABLE}`
                WHERE trade_date=:trade_date AND period='1m'
                  AND provider=:provider{batch_filter}
                ORDER BY stock_code, batch_id"""
            ),
            {
                "trade_date": trade_date,
                "provider": BIGQMT_PROVIDER_ID,
                "batch_id": batch_id,
            },
        )
        codes: set[str] = set()
        batches: set[str] = set()
        for row in rows:
            codes.add(str(row[0] or "").strip())
            batches.add(str(row[1] or "").strip())
    return ({code for code in codes if code}, {item for item in batches if item})


def _coverage_capture_time() -> str:
    return datetime.now().replace(microsecond=0).isoformat(timespec="seconds")


def _insert_coverage(source_engine, bundle: dict[str, Any]) -> dict[str, Any]:
    with source_engine.begin() as connection:
        return insert_coverage_bundle(connection, bundle)


def _persist_coverage(source_engine, bundle: dict[str, Any]) -> dict[str, Any]:
    insertion = _insert_coverage(source_engine, bundle)
    manifest = require_exact_coverage(bundle)
    return {
        **insertion,
        "status": manifest["status"],
        "manifest_hash": manifest["manifest_hash"],
        "bar_count": int(manifest["bar_count"]),
        "entity_count": int(manifest["entity_count"]),
    }


def _daily_coverage_bundle(
    *,
    expected_codes: set[str],
    rows: list[dict[str, Any]],
    trade_date: str,
    source_batch_id: str,
    catalog: Any,
    calendar_receipt: Any,
) -> dict[str, Any]:
    return assess_daily_coverage(
        expected_codes=sorted(expected_codes),
        rows=rows,
        trade_date=trade_date,
        provider=BIGQMT_PROVIDER_ID,
        run_id=source_batch_id,
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_batch_id=calendar_receipt.batch_id,
        calendar_manifest_hash=calendar_receipt.manifest_hash,
        source_batch_id=source_batch_id,
        captured_at=_coverage_capture_time(),
    )


def _minute_coverage_bundle(
    *,
    expected_codes: set[str],
    daily_rows: list[dict[str, Any]],
    minute_rows: list[dict[str, Any]],
    trade_date: str,
    minute_run_id: str,
    daily_source_batch_id: str,
    catalog: Any,
    calendar_receipt: Any,
    captured_at: str | None = None,
) -> dict[str, Any]:
    return assess_minute_coverage(
        expected_codes=sorted(expected_codes),
        daily_rows=daily_rows,
        minute_rows=minute_rows,
        trade_date=trade_date,
        provider=BIGQMT_PROVIDER_ID,
        daily_provider=BIGQMT_PROVIDER_ID,
        run_id=minute_run_id,
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_batch_id=calendar_receipt.batch_id,
        calendar_manifest_hash=calendar_receipt.manifest_hash,
        source_batch_id=minute_run_id,
        daily_source_batch_id=daily_source_batch_id,
        captured_at=captured_at or _coverage_capture_time(),
    )


def _minute_coverage_from_local(
    *,
    local_engine: Any,
    expected_codes: set[str],
    daily_rows: list[dict[str, Any]],
    trade_date: str,
    minute_run_id: str,
    daily_source_batch_id: str,
    catalog: Any,
    calendar_receipt: Any,
    batch_id: str = "",
    partition_size: int = 80,
) -> dict[str, Any]:
    observed_codes, _observed_batches = _local_minute_identity(
        local_engine,
        trade_date=trade_date,
        batch_id=batch_id,
    )
    ordered_expected = sorted(expected_codes)
    unexpected = sorted(observed_codes - expected_codes)
    daily_by_code = {
        str(row.get("stock_code") or "").strip(): row for row in daily_rows
    }
    captured_at = _coverage_capture_time()
    partitions: list[dict[str, Any]] = []
    for offset in range(0, len(ordered_expected), max(1, partition_size)):
        expected_partition = ordered_expected[offset : offset + max(1, partition_size)]
        query_codes = [
            *expected_partition,
            *(unexpected if offset == 0 else []),
        ]
        rows = _local_minute_rows(
            local_engine,
            trade_date=trade_date,
            stock_codes=query_codes,
            batch_id=batch_id,
        )
        partitions.append(
            _minute_coverage_bundle(
                expected_codes=set(expected_partition),
                daily_rows=[
                    daily_by_code[code]
                    for code in expected_partition
                    if code in daily_by_code
                ],
                minute_rows=rows,
                trade_date=trade_date,
                minute_run_id=minute_run_id,
                daily_source_batch_id=daily_source_batch_id,
                catalog=catalog,
                calendar_receipt=calendar_receipt,
                captured_at=captured_at,
            )
        )
    return combine_minute_coverage_partitions(
        expected_codes=ordered_expected,
        partitions=partitions,
    )


def _log(path: Path, payload: dict[str, Any]) -> None:
    payload = {"logged_at": datetime.now().isoformat(timespec="seconds"), **payload}
    if path.is_symlink():
        raise RuntimeError("QMT full-history log path is a symlink")
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _expected_minute_rows(stock_count: int) -> int:
    # A-share 1m data normally has 241 bars from 09:30 to 15:00 inclusive.
    return stock_count * 241


def _parse_stop_at(value: str) -> datetime_time | None:
    if not value:
        return None
    try:
        hour, minute = value.split(":", 1)
        return datetime_time(hour=int(hour), minute=int(minute))
    except Exception as exc:
        raise ValueError("--stop-at must be HH:MM, for example 07:00") from exc


def _stop_time_reached(stop_at: datetime_time | None) -> bool:
    if stop_at is None:
        return False
    return datetime.now().time() >= stop_at


def run_full_history(
    *,
    start_date: str,
    end_date: str,
    modes: set[str],
    daily_batch_size: int,
    minute_batch_size: int,
    sleep_seconds: float,
    resume: bool,
    log_path: Path,
    stop_at: datetime_time | None = None,
) -> dict[str, Any]:
    source_engine = _source_engine()
    local_engine = get_local_history_engine()
    validate_local_history_tables(local_engine)
    with source_engine.connect() as connection:
        catalog = load_stock_catalog(
            connection,
            decision_known_at=datetime.now().replace(microsecond=0),
        )
        calendar_receipt = load_trade_calendar_receipt(
            connection,
            start_date=start_date,
            end_date=end_date,
            decision_known_at=datetime.now().replace(microsecond=0),
        )
    trade_dates = calendar_receipt.sessions_between(start_date, end_date)
    expected_codes_by_date = {
        trade_date: set(catalog.eligible_codes(trade_date))
        for trade_date in trade_dates
    }
    if any(not codes for codes in expected_codes_by_date.values()):
        raise RuntimeError("independent QMT historical target universe is empty")
    codes = sorted(set().union(*expected_codes_by_date.values()))
    source_batch_id = daily_market_source_batch_id(
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_manifest_hash=calendar_receipt.manifest_hash,
    )
    stock_count = len(codes)
    summary = {
        "status": "started",
        "start_date": start_date,
        "end_date": end_date,
        "trade_days": len(trade_dates),
        "stock_count": stock_count,
        "modes": sorted(modes),
        "local_database": str(local_engine.url.database or ""),
        "stop_at": stop_at.isoformat(timespec="minutes") if stop_at else "",
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "calendar_batch_id": calendar_receipt.batch_id,
        "calendar_manifest_hash": calendar_receipt.manifest_hash,
        "source_batch_id": source_batch_id,
    }
    _log(log_path, {"event": "start", **summary})

    daily_done = 0
    minute_done = 0
    errors = 0
    stopped_by_window = False
    for trade_date in trade_dates:
        if _stop_time_reached(stop_at):
            stopped_by_window = True
            _log(log_path, {"event": "stop_window_reached", "trade_date": trade_date})
            break
        if "daily" in modes:
            expected_daily_codes = expected_codes_by_date[trade_date]
            if not expected_daily_codes:
                error = RuntimeError(
                    "production daily target set is empty: "
                    f"trade_date={trade_date}, table={PRODUCTION_KLINE_TABLE}"
                )
                _log(log_path, {"event": "daily_error", "trade_date": trade_date, "error": str(error)})
                raise error
            current_daily_rows = _local_daily_rows(
                local_engine,
                trade_date=trade_date,
                source_batch_id=source_batch_id,
            )
            current_daily_bundle = _daily_coverage_bundle(
                expected_codes=expected_daily_codes,
                rows=current_daily_rows,
                trade_date=trade_date,
                source_batch_id=source_batch_id,
                catalog=catalog,
                calendar_receipt=calendar_receipt,
            )
            _insert_coverage(source_engine, current_daily_bundle)
            if (
                resume
                and current_daily_bundle["manifest"]["status"] == "EXACT"
            ):
                certified = require_exact_coverage(current_daily_bundle)
                _log(
                    log_path,
                    {
                        "event": "skip_daily",
                        "trade_date": trade_date,
                        "rows": len(current_daily_rows),
                        "expected_rows": len(expected_daily_codes),
                        "coverage": "certified_exact",
                        "coverage_manifest_hash": certified["manifest_hash"],
                    },
                )
            else:
                try:
                    result = backfill_daily_kline_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=sorted(expected_daily_codes),
                        start_date=trade_date,
                        end_date=trade_date,
                        batch_size=daily_batch_size,
                        dry_run=False,
                        source_batch_id=source_batch_id,
                    )
                except Exception as exc:
                    errors += 1
                    _log(log_path, {"event": "daily_error", "trade_date": trade_date, "error": str(exc)})
                    continue
                else:
                    verified_daily_rows = _local_daily_rows(
                        local_engine,
                        trade_date=trade_date,
                        source_batch_id=source_batch_id,
                    )
                    verified_daily_bundle = _daily_coverage_bundle(
                        expected_codes=expected_daily_codes,
                        rows=verified_daily_rows,
                        trade_date=trade_date,
                        source_batch_id=source_batch_id,
                        catalog=catalog,
                        calendar_receipt=calendar_receipt,
                    )
                    try:
                        _insert_coverage(source_engine, verified_daily_bundle)
                        if verified_daily_bundle["manifest"]["status"] != "EXACT":
                            actual_codes = {
                                str(row.get("stock_code") or "")
                                for row in verified_daily_rows
                            }
                            raise _daily_coverage_error(
                                trade_date=trade_date,
                                expected=expected_daily_codes,
                                actual=actual_codes,
                            )
                        certified = require_exact_coverage(verified_daily_bundle)
                    except Exception as exc:
                        errors += 1
                        _log(
                            log_path,
                            {
                                "event": "daily_error",
                                "trade_date": trade_date,
                                "error": str(exc),
                            },
                        )
                        raise
                    daily_done += 1
                    _log(
                        log_path,
                        {
                            "event": "daily_done",
                            "trade_date": trade_date,
                            "fetched_rows": result.fetched_rows,
                            "written_rows": result.written_rows,
                            "batch_count": result.batch_count,
                            "expected_rows": len(expected_daily_codes),
                            "verified_rows": len(verified_daily_rows),
                            "coverage": "certified_exact",
                            "coverage_manifest_hash": certified[
                                "manifest_hash"
                            ],
                        },
                    )
        if _stop_time_reached(stop_at):
            stopped_by_window = True
            _log(log_path, {"event": "stop_window_reached", "trade_date": trade_date, "after": "daily"})
            break
        if "minute" in modes:
            expected_minute_codes = expected_codes_by_date[trade_date]
            daily_evidence_rows = _local_daily_rows(
                local_engine,
                trade_date=trade_date,
                source_batch_id=source_batch_id,
            )
            _existing_codes, existing_batch_id_set = _local_minute_identity(
                local_engine,
                trade_date=trade_date,
            )
            existing_batch_ids = sorted(existing_batch_id_set)
            existing_minute_run_id = (
                existing_batch_ids[0]
                if len(existing_batch_ids) == 1
                else "qmt_minute_probe_" + trade_date.replace("-", "")
            )
            existing_minute_bundle = _minute_coverage_from_local(
                local_engine=local_engine,
                expected_codes=expected_minute_codes,
                daily_rows=daily_evidence_rows,
                trade_date=trade_date,
                minute_run_id=existing_minute_run_id,
                daily_source_batch_id=source_batch_id,
                catalog=catalog,
                calendar_receipt=calendar_receipt,
            )
            _insert_coverage(source_engine, existing_minute_bundle)
            if (
                resume
                and existing_minute_bundle["manifest"]["status"] == "EXACT"
            ):
                certified = require_exact_coverage(existing_minute_bundle)
                _log(
                    log_path,
                    {
                        "event": "skip_minute",
                        "trade_date": trade_date,
                        "rows": int(certified["bar_count"]),
                        "expected_codes": len(expected_minute_codes),
                        "coverage": "certified_exact",
                        "coverage_manifest_hash": certified["manifest_hash"],
                    },
                )
            else:
                try:
                    result = backfill_minute_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=sorted(expected_minute_codes),
                        trade_dates=[trade_date],
                        batch_size=minute_batch_size,
                        dry_run=False,
                    )
                    verified_minute_bundle = _minute_coverage_from_local(
                        local_engine=local_engine,
                        expected_codes=expected_minute_codes,
                        daily_rows=daily_evidence_rows,
                        trade_date=trade_date,
                        minute_run_id=result.run_id,
                        daily_source_batch_id=source_batch_id,
                        catalog=catalog,
                        calendar_receipt=calendar_receipt,
                        batch_id=result.run_id,
                        partition_size=minute_batch_size,
                    )
                    certified = _persist_coverage(
                        source_engine, verified_minute_bundle
                    )
                    minute_done += 1
                    _log(
                        log_path,
                        {
                            "event": "minute_done",
                            "trade_date": trade_date,
                            "fetched_rows": result.fetched_rows,
                            "written_rows": result.written_rows,
                            "batch_count": result.batch_count,
                            "expected_codes": len(expected_minute_codes),
                            "verified_rows": certified["bar_count"],
                            "coverage": "certified_exact",
                            "coverage_manifest_hash": certified[
                                "manifest_hash"
                            ],
                        },
                    )
                except Exception as exc:
                    errors += 1
                    _log(log_path, {"event": "minute_error", "trade_date": trade_date, "error": str(exc)})
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    final = {
        "status": "stopped_window" if stopped_by_window else "finished",
        "daily_trade_days_done": daily_done,
        "minute_trade_days_done": minute_done,
        "errors": errors,
        "trade_days": len(trade_dates),
        "stock_count": stock_count,
        "stop_at": stop_at.isoformat(timespec="minutes") if stop_at else "",
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    _log(log_path, {"event": "finish", **final})
    return final


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Guojin QMT full-market local history backfill.")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--mode", choices=["all", "daily", "minute"], default="all")
    parser.add_argument("--daily-batch-size", type=int, default=120)
    parser.add_argument("--minute-batch-size", type=int, default=80)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--stop-at", default="", help="Stop naturally once local time reaches HH:MM, e.g. 07:00.")
    parser.add_argument("--state-root", default="")
    parser.add_argument("--lock-path", default="")
    parser.add_argument("--log-path", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    state_root, lock_path, log_path = _validated_runtime_paths(
        state_root=args.state_root,
        lock_path=args.lock_path,
        log_path=args.log_path,
    )
    source_engine = _source_engine()
    end_date = args.end_date or _latest_trade_date(source_engine)
    modes = {"daily", "minute"} if args.mode == "all" else {args.mode}
    stop_at = _parse_stop_at(args.stop_at)
    acquired, owner = _acquire_lock(lock_path)
    if not acquired:
        lock_failed = str(owner).startswith("lock_error:") or owner == (
            "stale_lock_could_not_be_replaced"
        )
        result = {
            "status": "lock_error" if lock_failed else "already_running",
            "lock_path": str(lock_path),
            "state_root": str(state_root),
            "log_path": str(log_path),
            "owner": owner,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
        }
    else:
        try:
            result = run_full_history(
                start_date=args.start_date,
                end_date=end_date,
                modes=modes,
                daily_batch_size=max(1, args.daily_batch_size),
                minute_batch_size=max(1, args.minute_batch_size),
                sleep_seconds=max(0.0, args.sleep_seconds),
                resume=not args.no_resume,
                log_path=log_path,
                stop_at=stop_at,
            )
        finally:
            _release_lock(lock_path)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    else:
        print(result)
    if result.get("status") == "already_running":
        return 0
    if result.get("status") == "lock_error":
        return 2
    return 0 if result.get("errors", 0) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
