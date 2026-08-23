from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.local_history import (
    backfill_daily_kline_local,
    backfill_minute_local,
    ensure_local_history_tables,
    get_local_history_engine,
    load_stock_codes,
    load_trade_dates,
    _normalize_date,
    result_dict,
)
from server.common.config import get_mysql_url
from tools.migrate_qmt_local_history_provenance import (
    WINDOWS_LOCAL_HISTORY_DATABASE,
    WINDOWS_LOCAL_OPTION_FILE,
    _connect_from_windows_option_file,
    _create_windows_local_history_engine,
    _validate_windows_local_mysql84_boundary,
)


WINDOWS_LOCAL_PRIMARY_DATABASE = "probiga"
TARGET_DAILY_TABLE = "sm_stock_kline"
TARGET_DAILY_QUARANTINE_TABLE = "qmt_target_daily_quarantine"
TARGET_WINDOW_UNIVERSE_SOURCE = "sm_stock_kline.target_window_exact_union"
CURRENT_UNIVERSE_SOURCE = "si_all_code.current_snapshot"
EXPLICIT_UNIVERSE_SOURCE = "explicit_codes"
DAILY_LOCK_PATH = ROOT / "data" / "runtime" / "qmt_local_daily_backfill.lock"
QUARANTINED_LEGACY_PROVIDER = "gj_big_qmt_legacy_unverified"
QUARANTINED_LEGACY_QUALITY = "QUARANTINED_LEGACY"
TARGET_INVALID_QUARANTINE_ACTION = "QUARANTINE_INVALID_NO_NATIVE"
TARGET_INVALID_QUARANTINE_REASON = "INVALID_PRE_CLOSE_NO_NATIVE_QMT"
TARGET_SYNTHETIC_QUARANTINE_ACTION = "QUARANTINE_SYNTHETIC_NO_NATIVE"
TARGET_SYNTHETIC_QUARANTINE_REASON = "SYNTHETIC_ZERO_VOLUME_NO_NATIVE_QMT"
TARGET_UNATTESTABLE_REASON = "INVALID_OR_SYNTHETIC_NO_NATIVE_QMT"
TARGET_QUARANTINE_ROW_LIMIT = 200
LOCK_INITIALIZATION_GRACE_SECONDS = 5.0


def _source_engine():
    return create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)


def _windows_local_engines():
    """Open the fixed local primary/history schemas without copying a secret."""
    history_engine = _create_windows_local_history_engine()
    try:
        _validate_windows_local_mysql84_boundary(history_engine)
    except Exception:
        history_engine.dispose()
        raise

    def connect_primary():
        connection = _connect_from_windows_option_file(WINDOWS_LOCAL_OPTION_FILE)
        try:
            connection.select_db(WINDOWS_LOCAL_PRIMARY_DATABASE)
        except Exception:
            connection.close()
            raise
        return connection

    primary_engine = create_engine(
        URL.create("mysql+pymysql", database=WINDOWS_LOCAL_PRIMARY_DATABASE),
        creator=connect_primary,
        pool_pre_ping=True,
        future=True,
    )
    try:
        with primary_engine.connect() as connection:
            selected_database = str(
                connection.execute(text("SELECT DATABASE()"), {}).scalar() or ""
            )
        if selected_database != WINDOWS_LOCAL_PRIMARY_DATABASE:
            raise RuntimeError("fixed Windows local primary database differs")
    except Exception:
        primary_engine.dispose()
        history_engine.dispose()
        raise
    return primary_engine, history_engine


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


def _acquire_lock(lock_path: Path) -> tuple[bool, str]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = f"{os.getpid()} {datetime.now().isoformat(timespec='seconds')}"
    owner = ""
    for _attempt in range(3):
        try:
            descriptor = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
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
                # The O_EXCL owner may not have flushed its PID payload yet.
                # A fresh empty/malformed file is initializing, not stale.
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
        raw = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
        pid = int(raw.split()[0])
        if pid == os.getpid():
            lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _codes_from_arg(source_engine, raw_codes: str, *, limit: int) -> list[str]:
    codes = [item.strip().zfill(6) for item in raw_codes.split(",") if item.strip()]
    return load_stock_codes(source_engine, codes=codes or None, limit=max(0, int(limit or 0)))


def _date_values(rows) -> list[str]:
    return sorted(
        {
            str(row[0])[:10]
            for row in rows
            if row[0] is not None and str(row[0]).strip()
        }
    )


def _decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _synthetic_target_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"({prefix}volume=0 AND {prefix}amount=0 "
        f"AND {prefix}`open`={prefix}`close` "
        f"AND {prefix}`high`={prefix}`close` "
        f"AND {prefix}`low`={prefix}`close` "
        f"AND {prefix}pre_close={prefix}`close` "
        f"AND {prefix}`close` IS NOT NULL AND {prefix}`close` > 0)"
    )


def _target_quarantine_classification(
    row: dict[str, Any] | Any,
) -> tuple[str, str] | None:
    pre_close = _decimal_value(row.get("pre_close"))
    if pre_close is None or pre_close <= 0:
        return (
            TARGET_INVALID_QUARANTINE_ACTION,
            TARGET_INVALID_QUARANTINE_REASON,
        )
    values = {
        key: _decimal_value(row.get(key))
        for key in ("open", "close", "high", "low", "volume", "amount")
    }
    close = values["close"]
    if (
        close is not None
        and close > 0
        and values["volume"] == 0
        and values["amount"] == 0
        and values["open"] == close
        and values["high"] == close
        and values["low"] == close
        and pre_close == close
    ):
        return (
            TARGET_SYNTHETIC_QUARANTINE_ACTION,
            TARGET_SYNTHETIC_QUARANTINE_REASON,
        )
    return None


def _target_window_codes(
    source_engine,
    *,
    start_date: str,
    end_date: str,
) -> tuple[list[str], list[str]]:
    """Load the exact point-in-window A-share union and prove target dates."""
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    if normalized_start > normalized_end:
        raise RuntimeError("target universe start date is after end date")
    params = {"start_date": normalized_start, "end_date": normalized_end}
    with source_engine.begin() as connection:
        calendar_dates = _date_values(
            connection.execute(
                text(
                    "SELECT trade_date FROM si_trade_calendar "
                    "WHERE trade_status=1 "
                    "AND trade_date BETWEEN :start_date AND :end_date "
                    "ORDER BY trade_date"
                ),
                params,
            ).fetchall()
        )
        target_dates = _date_values(
            connection.execute(
                text(
                    f"SELECT DISTINCT trade_date FROM {TARGET_DAILY_TABLE} "
                    "WHERE k_type=1 "
                    "AND adjust_type=0 "
                    "AND stock_code REGEXP '^(0|3|6)' "
                    "AND trade_date BETWEEN :start_date AND :end_date "
                    "ORDER BY trade_date"
                ),
                params,
            ).fetchall()
        )
        rows = connection.execute(
            text(
                f"SELECT DISTINCT stock_code FROM {TARGET_DAILY_TABLE} "
                "WHERE k_type=1 "
                "AND adjust_type=0 "
                "AND stock_code REGEXP '^(0|3|6)' "
                "AND trade_date BETWEEN :start_date AND :end_date "
                "ORDER BY stock_code"
            ),
            params,
        ).fetchall()

    missing_dates = sorted(set(calendar_dates) - set(target_dates))
    unexpected_dates = sorted(set(target_dates) - set(calendar_dates))
    if not calendar_dates or missing_dates or unexpected_dates:
        raise RuntimeError(
            "target daily window is incomplete: "
            f"calendar_dates={len(calendar_dates)}, "
            f"target_dates={len(target_dates)}, "
            f"missing_count={len(missing_dates)}, "
            f"missing_sample={missing_dates[:10]}, "
            f"unexpected_count={len(unexpected_dates)}, "
            f"unexpected_sample={unexpected_dates[:10]}"
        )
    codes = sorted(
        {
            str(row[0]).strip().zfill(6)
            for row in rows
            if row[0] is not None and str(row[0]).strip()
        }
    )
    if not codes:
        raise RuntimeError("target daily window stock-code union is empty")
    return codes, target_dates


def _target_window_unattestable_codes(
    source_engine,
    *,
    start_date: str,
    end_date: str,
) -> list[str]:
    """Return codes whose window is entirely invalid or synthetic."""
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    rows = []
    with source_engine.begin() as connection:
        rows = connection.execute(
            text(
                f"""
                SELECT stock_code
                FROM `{WINDOWS_LOCAL_PRIMARY_DATABASE}`.`{TARGET_DAILY_TABLE}`
                WHERE k_type=1
                  AND adjust_type=0
                  AND stock_code REGEXP '^(0|3|6)'
                  AND trade_date BETWEEN :start_date AND :end_date
                GROUP BY stock_code
                HAVING SUM(
                    CASE
                        WHEN pre_close IS NOT NULL AND pre_close > 0
                         AND NOT {_synthetic_target_sql()}
                        THEN 1
                        ELSE 0
                    END
                )=0
                ORDER BY stock_code
                """
            ),
            {
                "start_date": normalized_start,
                "end_date": normalized_end,
            },
        ).fetchall()
    return sorted(
        {
            str(row[0]).strip().zfill(6)
            for row in rows
            if row[0] is not None and str(row[0]).strip()
        }
    )


def _universe_proof(
    codes: list[str],
    *,
    source: str,
    start_date: str = "",
    end_date: str = "",
    target_trade_dates: list[str] | None = None,
) -> dict[str, Any]:
    canonical_codes = sorted(set(codes))
    digest = hashlib.sha256(
        "\n".join(canonical_codes).encode("utf-8")
    ).hexdigest()
    proof: dict[str, Any] = {
        "source": source,
        "stock_count": len(canonical_codes),
        "stock_codes_sha256": digest,
    }
    if start_date and end_date:
        proof.update(
            {
                "start_date": _normalize_date(start_date),
                "end_date": _normalize_date(end_date),
            }
        )
    if target_trade_dates is not None:
        proof["target_trade_date_count"] = len(target_trade_dates)
        proof["target_first_trade_date"] = target_trade_dates[0]
        proof["target_last_trade_date"] = target_trade_dates[-1]
    return proof


def _repair_target_source_only_rows(
    source_engine,
    *,
    start_date: str,
    end_date: str,
    provider: str,
) -> dict[str, Any]:
    """Insert only native QMT rows missing from the canonical daily table."""
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    params = {
        "start_date": normalized_start,
        "end_date": normalized_end,
        "provider": provider,
    }
    history_table = (
        f"`{WINDOWS_LOCAL_HISTORY_DATABASE}`.`qmt_local_stock_kline`"
    )
    target_table = f"`{WINDOWS_LOCAL_PRIMARY_DATABASE}`.`{TARGET_DAILY_TABLE}`"
    eligible = """
        s.provider=:provider
        AND s.period='1d'
        AND s.k_type=1
        AND s.adjust_type=0
        AND s.trade_date BETWEEN :start_date AND :end_date
        AND s.stock_code REGEXP '^(0|3|6)'
        AND BINARY s.pre_close_origin=BINARY 'NATIVE_QMT'
        AND s.pre_close IS NOT NULL AND s.pre_close > 0
        AND s.`open` IS NOT NULL AND s.`open` > 0
        AND s.`close` IS NOT NULL AND s.`close` > 0
        AND s.`high` IS NOT NULL AND s.`high` > 0
        AND s.`low` IS NOT NULL AND s.`low` > 0
        AND s.volume IS NOT NULL AND s.volume >= 0
        AND s.amount IS NOT NULL AND s.amount >= 0
        AND t.id IS NULL
    """
    count_sql = text(
        f"""
        SELECT COUNT(*)
        FROM {history_table} s
        LEFT JOIN {target_table} t
          ON t.stock_code=s.stock_code COLLATE utf8mb4_unicode_ci
         AND t.trade_date=s.trade_date
         AND t.k_type=1
         AND t.adjust_type=0
        WHERE {eligible}
        """
    )
    insert_sql = text(
        f"""
        INSERT INTO {target_table} (
            stock_code, short_name, trade_time, trade_date,
            k_type, adjust_type, `open`, `close`, `high`, `low`,
            volume, amount, `change`, change_pct, turnover_ratio,
            pre_close, etl_sync_at, qmt_code, data_source,
            source_time, received_at, batch_id, data_version,
            quality_status, permission_status
        )
        SELECT
            s.stock_code, COALESCE(s.short_name, ''),
            s.trade_time, s.trade_date,
            1, 0, s.`open`, s.`close`, s.`high`, s.`low`,
            s.volume, s.amount, s.`change`, s.change_pct,
            s.turnover_ratio, s.pre_close, NOW(), s.qmt_code,
            s.provider, s.source_time, s.received_at, s.batch_id,
            COALESCE(s.data_version, 'bigqmt_inner_v2'),
            'QMT_PENDING_ATTESTATION',
            COALESCE(s.permission_status, 'SUPPORTED')
        FROM {history_table} s
        LEFT JOIN {target_table} t
          ON t.stock_code=s.stock_code COLLATE utf8mb4_unicode_ci
         AND t.trade_date=s.trade_date
         AND t.k_type=1
         AND t.adjust_type=0
        WHERE {eligible}
        """
    )
    with source_engine.begin() as connection:
        source_only_before = int(
            connection.execute(count_sql, params).scalar() or 0
        )
        inserted_rows = 0
        if source_only_before:
            inserted_rows = int(
                connection.execute(insert_sql, params).rowcount or 0
            )
        source_only_after = int(
            connection.execute(count_sql, params).scalar() or 0
        )
        if (
            inserted_rows != source_only_before
            or source_only_after != 0
        ):
            raise RuntimeError(
                "QMT source-only target repair is incomplete: "
                f"source_only_before={source_only_before}, "
                f"inserted_rows={inserted_rows}, "
                f"source_only_after={source_only_after}"
            )
    return {
        "status": "APPLIED",
        "provider": provider,
        "start_date": normalized_start,
        "end_date": normalized_end,
        "source_only_before": source_only_before,
        "inserted_rows": inserted_rows,
        "source_only_after": source_only_after,
        "existing_rows_updated": 0,
    }


def _ensure_target_daily_quarantine_table(source_engine) -> None:
    table_name = (
        f"`{WINDOWS_LOCAL_PRIMARY_DATABASE}`."
        f"`{TARGET_DAILY_QUARANTINE_TABLE}`"
    )
    with source_engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    run_id VARCHAR(64) NOT NULL,
                    original_id BIGINT NOT NULL,
                    action VARCHAR(64) NOT NULL,
                    reason VARCHAR(128) NOT NULL,
                    native_provider VARCHAR(64) NOT NULL,
                    stock_code VARCHAR(16) NOT NULL,
                    trade_date DATE NOT NULL,
                    k_type INT NOT NULL,
                    adjust_type INT NOT NULL,
                    row_payload LONGTEXT NOT NULL,
                    row_sha256 CHAR(64) NOT NULL,
                    quarantined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    restored_at DATETIME NULL,
                    restore_run_id VARCHAR(64) NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY uq_qmt_target_quarantine_original_action (
                        original_id, action
                    ),
                    KEY idx_qmt_target_quarantine_window (
                        trade_date, stock_code, action
                    ),
                    KEY idx_qmt_target_quarantine_run (run_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                  COLLATE=utf8mb4_unicode_ci
                """
            )
        )


def _quarantine_invalid_target_rows_without_native(
    source_engine,
    *,
    start_date: str,
    end_date: str,
    provider: str,
    max_rows: int = TARGET_QUARANTINE_ROW_LIMIT,
) -> dict[str, Any]:
    """Audit-copy then remove unattestable target rows lacking native QMT.

    The bounded set contains either an invalid ``pre_close`` row or a
    deterministic zero-volume synthetic suspension bar.  Native-backed rows
    are always retained for the attester to repair in place.
    """
    if int(max_rows) <= 0:
        raise ValueError("target quarantine row limit must be positive")
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    _ensure_target_daily_quarantine_table(source_engine)
    history_table = (
        f"`{WINDOWS_LOCAL_HISTORY_DATABASE}`.`qmt_local_stock_kline`"
    )
    run_table = (
        f"`{WINDOWS_LOCAL_HISTORY_DATABASE}`.`qmt_local_backfill_run`"
    )
    target_table = f"`{WINDOWS_LOCAL_PRIMARY_DATABASE}`.`{TARGET_DAILY_TABLE}`"
    quarantine_table = (
        f"`{WINDOWS_LOCAL_PRIMARY_DATABASE}`."
        f"`{TARGET_DAILY_QUARANTINE_TABLE}`"
    )
    params = {
        "start_date": normalized_start,
        "end_date": normalized_end,
        "provider": provider,
    }
    quarantine_candidate = f"""
        t.k_type=1
        AND t.adjust_type=0
        AND t.trade_date BETWEEN :start_date AND :end_date
        AND t.stock_code REGEXP '^(0|3|6)'
        AND (
            t.pre_close IS NULL OR t.pre_close <= 0
            OR {_synthetic_target_sql('t')}
        )
    """
    valid_native_for_target = f"""
        AND NOT EXISTS (
            SELECT 1
            FROM {history_table} s
            WHERE s.provider=:provider
              AND s.period='1d'
              AND s.k_type=1
              AND s.adjust_type=0
              AND s.stock_code COLLATE utf8mb4_unicode_ci=t.stock_code
              AND s.trade_date=t.trade_date
              AND BINARY s.pre_close_origin=BINARY 'NATIVE_QMT'
              AND s.pre_close IS NOT NULL AND s.pre_close > 0
              AND s.`open` IS NOT NULL AND s.`open` > 0
              AND s.`close` IS NOT NULL AND s.`close` > 0
              AND s.`high` IS NOT NULL AND s.`high` > 0
              AND s.`low` IS NOT NULL AND s.`low` > 0
              AND s.volume IS NOT NULL AND s.volume >= 0
              AND s.amount IS NOT NULL AND s.amount >= 0
        )
    """
    select_sql = text(
        f"""
        SELECT
            t.id, t.stock_code, t.short_name, t.trade_time, t.trade_date,
            t.k_type, t.adjust_type, t.`open`, t.`close`, t.`high`, t.`low`,
            t.volume, t.amount, t.`change`, t.change_pct, t.turnover_ratio,
            t.pre_close, t.etl_sync_at, t.qmt_code, t.data_source,
            t.source_time, t.received_at, t.batch_id, t.data_version,
            t.quality_status, t.permission_status
        FROM {target_table} t
        WHERE {quarantine_candidate}
        ORDER BY t.id
        FOR UPDATE
        """
    )
    native_lookup_sql = text(
        f"""
        SELECT s.id
        FROM {history_table} s FORCE INDEX (uk_qmt_local_kline)
        WHERE s.provider=:provider
          AND s.stock_code=:stock_code
          AND s.period='1d'
          AND s.trade_date=:trade_date
          AND s.adjust_type=0
          AND s.k_type=1
          AND BINARY s.pre_close_origin=BINARY 'NATIVE_QMT'
          AND s.pre_close IS NOT NULL AND s.pre_close > 0
          AND s.`open` IS NOT NULL AND s.`open` > 0
          AND s.`close` IS NOT NULL AND s.`close` > 0
          AND s.`high` IS NOT NULL AND s.`high` > 0
          AND s.`low` IS NOT NULL AND s.`low` > 0
          AND s.volume IS NOT NULL AND s.volume >= 0
          AND s.amount IS NOT NULL AND s.amount >= 0
        LIMIT 1
        FOR UPDATE
        """
    )
    conflict_sql = text(
        f"""
        SELECT COUNT(*)
        FROM {quarantine_table}
        WHERE original_id=:original_id
          AND action=:action
        """
    )
    insert_sql = text(
        f"""
        INSERT INTO {quarantine_table} (
            run_id, original_id, action, reason, native_provider,
            stock_code, trade_date, k_type, adjust_type,
            row_payload, row_sha256, quarantined_at
        ) VALUES (
            :run_id, :original_id, :action, :reason, :provider,
            :stock_code, :trade_date, :k_type, :adjust_type,
            :row_payload, :row_sha256, NOW()
        )
        """
    )
    delete_sql = text(
        f"""
        DELETE t
        FROM {target_table} t
        WHERE t.id=:original_id
          AND {quarantine_candidate}
          {valid_native_for_target}
        """
    )
    remaining_sql = text(
        f"SELECT COUNT(*) FROM {target_table} WHERE id=:original_id"
    )
    run_id = (
        f"qmt_target_quarantine_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    with source_engine.begin() as connection:
        candidate_rows = list(
            connection.execute(select_sql, params).mappings().all()
        )
        classified_candidates: list[dict[str, Any]] = []
        candidate_action_counts: dict[str, int] = {}
        for raw_row in candidate_rows:
            row = dict(raw_row)
            classification = _target_quarantine_classification(row)
            if classification is None:
                raise RuntimeError(
                    "unattestable target quarantine SQL/Python classification differs"
                )
            action, reason = classification
            row["_quarantine_action"] = action
            row["_quarantine_reason"] = reason
            classified_candidates.append(row)
            candidate_action_counts[action] = (
                candidate_action_counts.get(action, 0) + 1
            )

        rows: list[dict[str, Any]] = []
        native_backed_action_counts: dict[str, int] = {}
        for row in classified_candidates:
            native_id = connection.execute(
                native_lookup_sql,
                {
                    **params,
                    "stock_code": str(row["stock_code"]),
                    "trade_date": row["trade_date"],
                },
            ).scalar()
            if int(native_id or 0) > 0:
                action = str(row["_quarantine_action"])
                native_backed_action_counts[action] = (
                    native_backed_action_counts.get(action, 0) + 1
                )
                continue
            rows.append(row)
        if len(rows) > int(max_rows):
            raise RuntimeError(
                "unattestable target quarantine exceeds the bounded row limit: "
                f"selected_rows={len(rows)}, candidate_rows={len(candidate_rows)}, "
                f"max_rows={int(max_rows)}"
            )
        selected_action_counts: dict[str, int] = {}
        for row in rows:
            action = str(row["_quarantine_action"])
            selected_action_counts[action] = (
                selected_action_counts.get(action, 0) + 1
            )
        conflict_rows = sum(
            int(
                connection.execute(
                    conflict_sql,
                    {
                        **params,
                        "original_id": int(row["id"]),
                        "action": str(row["_quarantine_action"]),
                    },
                ).scalar()
                or 0
            )
            for row in rows
        )
        if conflict_rows:
            raise RuntimeError(
                "unattestable target quarantine conflicts with an existing audit copy: "
                f"conflict_rows={conflict_rows}"
            )

        audit_rows: list[dict[str, Any]] = []
        row_hashes: list[dict[str, Any]] = []
        for row in rows:
            payload = {
                key: row[key]
                for key in row.keys()
                if not key.startswith("_quarantine_")
            }
            payload_json = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            row_sha256 = hashlib.sha256(
                payload_json.encode("utf-8")
            ).hexdigest()
            audit_rows.append(
                {
                    **params,
                    "run_id": run_id,
                    "action": str(row["_quarantine_action"]),
                    "reason": str(row["_quarantine_reason"]),
                    "original_id": int(row["id"]),
                    "stock_code": str(row["stock_code"]),
                    "trade_date": row["trade_date"],
                    "k_type": int(row["k_type"]),
                    "adjust_type": int(row["adjust_type"]),
                    "row_payload": payload_json,
                    "row_sha256": row_sha256,
                }
            )
            row_hashes.append(
                {
                    "original_id": int(row["id"]),
                    "row_sha256": row_sha256,
                }
            )
        row_set_sha256 = hashlib.sha256(
            json.dumps(
                row_hashes,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

        copied_rows = 0
        deleted_rows = 0
        if audit_rows:
            copied_rows = int(
                connection.execute(insert_sql, audit_rows).rowcount or 0
            )
            deleted_rows = int(
                connection.execute(delete_sql, audit_rows).rowcount or 0
            )
        remaining_rows = sum(
            int(
                connection.execute(remaining_sql, audit_row).scalar()
                or 0
            )
            for audit_row in audit_rows
        )
        if (
            copied_rows != len(rows)
            or deleted_rows != len(rows)
            or remaining_rows != 0
        ):
            raise RuntimeError(
                "unattestable target quarantine is incomplete: "
                f"selected_rows={len(rows)}, "
                f"copied_rows={copied_rows}, "
                f"deleted_rows={deleted_rows}, "
                f"remaining_rows={remaining_rows}"
            )
        extra = {
            "candidate_action_counts": candidate_action_counts,
            "selected_action_counts": selected_action_counts,
            "native_backed_action_counts": native_backed_action_counts,
            "native_provider": provider,
            "quarantine_table": (
                f"{WINDOWS_LOCAL_PRIMARY_DATABASE}."
                f"{TARGET_DAILY_QUARANTINE_TABLE}"
            ),
            "row_set_sha256": row_set_sha256,
            "full_row_payload_preserved": True,
            "recoverable": True,
            "candidate_target_rows_checked": len(candidate_rows),
            "invalid_target_rows_checked": candidate_action_counts.get(
                TARGET_INVALID_QUARANTINE_ACTION, 0
            ),
            "synthetic_target_rows_checked": candidate_action_counts.get(
                TARGET_SYNTHETIC_QUARANTINE_ACTION, 0
            ),
            "native_backed_invalid_rows": native_backed_action_counts.get(
                TARGET_INVALID_QUARANTINE_ACTION, 0
            ),
            "native_backed_synthetic_rows": native_backed_action_counts.get(
                TARGET_SYNTHETIC_QUARANTINE_ACTION, 0
            ),
            "max_rows": int(max_rows),
            "existing_valid_target_rows_updated": 0,
        }
        connection.execute(
            text(
                f"""
                INSERT INTO {run_table} (
                    run_id, provider, dataset, period,
                    start_date, end_date, status, requested_codes,
                    fetched_rows, written_rows, error_message,
                    started_at, finished_at, extra_json
                ) VALUES (
                    :run_id, :provider, 'sm_stock_kline_target_quarantine',
                    '1d', :start_date, :end_date, 'SUCCESS',
                    :requested_codes, :row_count, :row_count, NULL,
                    NOW(), NOW(), :extra_json
                )
                """
            ),
            {
                **params,
                "run_id": run_id,
                "requested_codes": len(
                    {str(row["stock_code"]) for row in rows}
                ),
                "row_count": len(rows),
                "extra_json": json.dumps(
                    extra,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    return {
        "status": "APPLIED",
        "run_id": run_id,
        "provider": provider,
        "start_date": normalized_start,
        "end_date": normalized_end,
        "selected_rows": len(rows),
        "candidate_target_rows_checked": len(candidate_rows),
        "invalid_target_rows_checked": candidate_action_counts.get(
            TARGET_INVALID_QUARANTINE_ACTION, 0
        ),
        "synthetic_target_rows_checked": candidate_action_counts.get(
            TARGET_SYNTHETIC_QUARANTINE_ACTION, 0
        ),
        "native_backed_invalid_rows": native_backed_action_counts.get(
            TARGET_INVALID_QUARANTINE_ACTION, 0
        ),
        "native_backed_synthetic_rows": native_backed_action_counts.get(
            TARGET_SYNTHETIC_QUARANTINE_ACTION, 0
        ),
        "candidate_action_counts": candidate_action_counts,
        "selected_action_counts": selected_action_counts,
        "native_backed_action_counts": native_backed_action_counts,
        "max_rows": int(max_rows),
        "audit_copied_rows": copied_rows,
        "deleted_rows": deleted_rows,
        "remaining_rows": remaining_rows,
        "row_set_sha256": row_set_sha256,
        "quarantine_table": (
            f"{WINDOWS_LOCAL_PRIMARY_DATABASE}."
            f"{TARGET_DAILY_QUARANTINE_TABLE}"
        ),
        "recoverable": True,
        "existing_valid_target_rows_updated": 0,
    }


def _quarantine_source_only_legacy_rows(
    source_engine,
    *,
    start_date: str,
    end_date: str,
    provider: str,
) -> dict[str, Any]:
    """Move orphaned unverified legacy rows out of the native proof provider."""
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    history_table = (
        f"`{WINDOWS_LOCAL_HISTORY_DATABASE}`.`qmt_local_stock_kline`"
    )
    run_table = (
        f"`{WINDOWS_LOCAL_HISTORY_DATABASE}`.`qmt_local_backfill_run`"
    )
    target_table = f"`{WINDOWS_LOCAL_PRIMARY_DATABASE}`.`{TARGET_DAILY_TABLE}`"
    params = {
        "start_date": normalized_start,
        "end_date": normalized_end,
        "provider": provider,
        "quarantine_provider": QUARANTINED_LEGACY_PROVIDER,
        "quarantine_quality": QUARANTINED_LEGACY_QUALITY,
    }
    orphan_join = f"""
        FROM {history_table} s
        LEFT JOIN {target_table} t
          ON t.stock_code=s.stock_code COLLATE utf8mb4_unicode_ci
         AND t.trade_date=s.trade_date
         AND t.k_type=1
         AND t.adjust_type=0
        WHERE s.provider=:provider
          AND s.period='1d'
          AND s.k_type=1
          AND s.adjust_type=0
          AND s.trade_date BETWEEN :start_date AND :end_date
          AND s.stock_code REGEXP '^(0|3|6)'
          AND BINARY s.pre_close_origin=BINARY 'UNVERIFIED_LEGACY'
          AND t.id IS NULL
    """
    select_sql = text(
        "SELECT s.id, s.stock_code, s.trade_date, s.adjust_type, "
        f"s.data_version {orphan_join} ORDER BY s.id FOR UPDATE"
    )
    remaining_sql = text(f"SELECT COUNT(*) {orphan_join}")
    conflict_sql = text(
        f"""
        SELECT COUNT(*)
        FROM {history_table} s
        JOIN {history_table} q
          ON q.provider=:quarantine_provider
         AND q.stock_code=s.stock_code
         AND q.period=s.period
         AND q.trade_date=s.trade_date
         AND q.adjust_type=s.adjust_type
        LEFT JOIN {target_table} t
          ON t.stock_code=s.stock_code COLLATE utf8mb4_unicode_ci
         AND t.trade_date=s.trade_date
         AND t.k_type=1
         AND t.adjust_type=0
        WHERE s.provider=:provider
          AND s.period='1d'
          AND s.k_type=1
          AND s.adjust_type=0
          AND s.trade_date BETWEEN :start_date AND :end_date
          AND s.stock_code REGEXP '^(0|3|6)'
          AND BINARY s.pre_close_origin=BINARY 'UNVERIFIED_LEGACY'
          AND t.id IS NULL
        """
    )
    update_sql = text(
        f"""
        UPDATE {history_table} s
        LEFT JOIN {target_table} t
          ON t.stock_code=s.stock_code COLLATE utf8mb4_unicode_ci
         AND t.trade_date=s.trade_date
         AND t.k_type=1
         AND t.adjust_type=0
        SET s.provider=:quarantine_provider,
            s.quality_status=:quarantine_quality,
            s.updated_at=NOW()
        WHERE s.provider=:provider
          AND s.period='1d'
          AND s.k_type=1
          AND s.adjust_type=0
          AND s.trade_date BETWEEN :start_date AND :end_date
          AND s.stock_code REGEXP '^(0|3|6)'
          AND BINARY s.pre_close_origin=BINARY 'UNVERIFIED_LEGACY'
          AND t.id IS NULL
        """
    )
    run_id = (
        f"qmt_quarantine_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    with source_engine.begin() as connection:
        rows = list(
            connection.execute(select_sql, params).mappings().all()
        )
        identities = [
            {
                "id": int(row["id"]),
                "stock_code": str(row["stock_code"]),
                "trade_date": str(row["trade_date"])[:10],
                "adjust_type": int(row["adjust_type"]),
                "data_version": str(row.get("data_version") or ""),
            }
            for row in rows
        ]
        identity_hash = hashlib.sha256(
            json.dumps(
                identities,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        conflict_rows = int(
            connection.execute(conflict_sql, params).scalar() or 0
        )
        if conflict_rows:
            raise RuntimeError(
                "legacy quarantine provider conflicts with existing rows: "
                f"conflict_rows={conflict_rows}"
            )
        quarantined_rows = 0
        if rows:
            quarantined_rows = int(
                connection.execute(update_sql, params).rowcount or 0
            )
        remaining_rows = int(
            connection.execute(remaining_sql, params).scalar() or 0
        )
        if quarantined_rows != len(rows) or remaining_rows != 0:
            raise RuntimeError(
                "legacy source-only quarantine is incomplete: "
                f"selected_rows={len(rows)}, "
                f"quarantined_rows={quarantined_rows}, "
                f"remaining_rows={remaining_rows}"
            )
        extra = {
            "reason": "SOURCE_ONLY_UNVERIFIED_LEGACY",
            "source_provider": provider,
            "quarantine_provider": QUARANTINED_LEGACY_PROVIDER,
            "row_identity_sha256": identity_hash,
            "existing_target_rows_updated": 0,
        }
        connection.execute(
            text(
                f"""
                INSERT INTO {run_table} (
                    run_id, provider, dataset, period,
                    start_date, end_date, status, requested_codes,
                    fetched_rows, written_rows, error_message,
                    started_at, finished_at, extra_json
                ) VALUES (
                    :run_id, :provider, 'qmt_local_stock_kline_quarantine',
                    '1d', :start_date, :end_date, 'SUCCESS',
                    :requested_codes, :row_count, :row_count, NULL,
                    NOW(), NOW(), :extra_json
                )
                """
            ),
            {
                **params,
                "run_id": run_id,
                "requested_codes": len(
                    {row["stock_code"] for row in identities}
                ),
                "row_count": len(rows),
                "extra_json": json.dumps(
                    extra,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    return {
        "status": "APPLIED",
        "run_id": run_id,
        "provider": provider,
        "quarantine_provider": QUARANTINED_LEGACY_PROVIDER,
        "start_date": normalized_start,
        "end_date": normalized_end,
        "selected_rows": len(rows),
        "quarantined_rows": quarantined_rows,
        "remaining_rows": remaining_rows,
        "row_identity_sha256": identity_hash,
        "existing_target_rows_updated": 0,
    }


@dataclass(frozen=True)
class ResolvedLimits:
    stock_limit: int
    gap_limit: int


def _resolve_limits(mode: str, *, limit: int, stock_limit: int | None, gap_limit: int | None) -> ResolvedLimits:
    """Resolve legacy --limit without accidentally shrinking full-market gap repair."""
    raw_limit = max(0, int(limit or 0))
    if mode == "from-gaps":
        return ResolvedLimits(
            stock_limit=max(0, int(stock_limit or 0)),
            gap_limit=max(1, int(gap_limit or raw_limit or 20)),
        )
    return ResolvedLimits(
        stock_limit=max(0, int(stock_limit if stock_limit is not None else raw_limit)),
        gap_limit=max(1, int(gap_limit or 20)),
    )


def _gap_rows(source_engine, *, limit: int, dataset: str = "") -> list[dict[str, Any]]:
    where_dataset = "AND dataset = :dataset" if dataset else ""
    params: dict[str, Any] = {"limit": max(1, int(limit or 20))}
    if dataset:
        params["dataset"] = dataset
    with source_engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT id, dataset, period, gap_start, gap_end
                FROM sys_data_gap
                WHERE provider = 'gj_qmt'
                  AND status IN ('PENDING', 'RETRYING')
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW() OR status = 'PENDING')
                  {where_dataset}
                ORDER BY
                  CASE status WHEN 'PENDING' THEN 0 ELSE 1 END,
                  COALESCE(next_retry_at, created_at),
                  id
                LIMIT :limit
                """
            ),
            params,
        ).mappings().fetchall()
    return [dict(row) for row in rows]


def _update_gap_status(
    source_engine,
    *,
    gap_id: int,
    run_id: str | None,
    resolved: bool,
    message: str,
    dry_run: bool,
) -> str:
    if dry_run:
        return "DRY_RUN"
    if resolved:
        with source_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE sys_data_gap
                    SET status = 'RESOLVED',
                        resolved_at = NOW(),
                        last_run_id = :run_id,
                        last_error = :message,
                        next_retry_at = NULL,
                        updated_at = NOW()
                    WHERE id = :gap_id
                    """
                ),
                {"gap_id": int(gap_id), "run_id": run_id, "message": message[:1000]},
            )
        return "RESOLVED"

    with source_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE sys_data_gap
                SET status = 'PENDING',
                    retry_count = retry_count + 1,
                    last_run_id = :run_id,
                    last_error = :message,
                    next_retry_at = DATE_ADD(NOW(), INTERVAL 6 HOUR),
                    updated_at = NOW()
                WHERE id = :gap_id
                """
            ),
            {"gap_id": int(gap_id), "run_id": run_id, "message": message[:1000]},
        )
    return "PENDING"


def _print(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        return
    print(json.dumps(payload, ensure_ascii=False, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill bulky Guojin QMT historical data into a local/off-production MySQL database."
    )
    parser.add_argument("mode", choices=["init", "daily", "minute", "from-gaps"])
    parser.add_argument("--local-url", default="", help="Override QMT_HISTORY_MYSQL_URL/MINUTE_MYSQL_URL.")
    parser.add_argument(
        "--windows-local-option-file",
        action="store_true",
        help=(
            "Use the fixed protected Windows MySQL 8.4 option file for both "
            "the local primary and QMT history schemas."
        ),
    )
    parser.add_argument("--codes", default="", help="Comma-separated stock codes. Empty means si_all_code universe.")
    parser.add_argument(
        "--target-window-universe",
        action="store_true",
        help=(
            "For daily mode, load the exact distinct 0/3/6 stock-code union "
            "from sm_stock_kline in --start-date/--end-date."
        ),
    )
    parser.add_argument(
        "--repair-target-source-only",
        action="store_true",
        help=(
            "After a successful native daily backfill, insert only valid QMT "
            "rows missing from the fixed local sm_stock_kline target."
        ),
    )
    parser.add_argument(
        "--quarantine-source-only-legacy",
        action="store_true",
        help=(
            "After native source-only repair, audit and quarantine only "
            "orphaned UNVERIFIED_LEGACY rows from the native provider."
        ),
    )
    parser.add_argument(
        "--quarantine-invalid-target-no-native",
        "--quarantine-unattestable-target-no-native",
        dest="quarantine_invalid_target_no_native",
        action="store_true",
        help=(
            "After native backfill, audit-copy and quarantine invalid or "
            "deterministic zero-volume synthetic target rows only when no "
            "valid native QMT row exists."
        ),
    )
    parser.add_argument(
        "--max-target-quarantine-rows",
        type=int,
        default=TARGET_QUARANTINE_ROW_LIMIT,
        help="Fail closed if one target window has more quarantine candidates.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Legacy limiter: stocks in daily/minute, gaps in from-gaps.")
    parser.add_argument("--stock-limit", type=int, default=None, help="Limit stock universe. In from-gaps, default is full market.")
    parser.add_argument("--gap-limit", type=int, default=None, help="Limit sys_data_gap rows. Defaults to --limit in from-gaps.")
    parser.add_argument("--start-date", default="", help="YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--end-date", default="", help="YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--trade-date", default="", help="One trading date for minute mode.")
    parser.add_argument("--batch-size", type=int, default=80)
    parser.add_argument("--dividend-type", default="none", choices=["none", "front", "back", "qfq", "hfq"])
    parser.add_argument(
        "--provider",
        default="gj_big_qmt_inner",
        choices=["gj_big_qmt_inner", "gj_qmt"],
        help="Daily source route; governance requires gj_big_qmt_inner.",
    )
    parser.add_argument("--gap-dataset", default="", choices=["", "sm_stock_kline.1d", "sm_stock_minute.1m"])
    parser.add_argument("--apply", action="store_true", help="Actually write rows and update sys_data_gap. Default is dry-run.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if args.windows_local_option_file and args.local_url:
        parser.error(
            "--windows-local-option-file and --local-url are mutually exclusive"
        )
    if args.target_window_universe:
        if args.mode != "daily":
            parser.error("--target-window-universe is only valid in daily mode")
        if not args.start_date or not args.end_date:
            parser.error(
                "--target-window-universe requires --start-date and --end-date"
            )
        if args.codes or int(args.limit or 0) or int(args.stock_limit or 0):
            parser.error(
                "--target-window-universe cannot be combined with stock-code limits"
            )
    if args.repair_target_source_only:
        if (
            args.mode != "daily"
            or not args.windows_local_option_file
            or not args.target_window_universe
            or not args.apply
            or args.provider != "gj_big_qmt_inner"
            or args.dividend_type != "none"
        ):
            parser.error(
                "--repair-target-source-only requires daily mode, "
                "--windows-local-option-file, --target-window-universe, "
                "--provider gj_big_qmt_inner, --dividend-type none and --apply"
            )
    if args.quarantine_source_only_legacy:
        if not args.repair_target_source_only:
            parser.error(
                "--quarantine-source-only-legacy requires "
                "--repair-target-source-only and its strict prerequisites"
            )
    if args.quarantine_invalid_target_no_native:
        if not args.quarantine_source_only_legacy:
            parser.error(
                "--quarantine-invalid-target-no-native requires "
                "--quarantine-source-only-legacy, "
                "--repair-target-source-only and their strict prerequisites"
            )
        if int(args.max_target_quarantine_rows) <= 0:
            parser.error("--max-target-quarantine-rows must be positive")

    if args.windows_local_option_file:
        source_engine, local_engine = _windows_local_engines()
        connection_mode = "fixed_protected_windows_option_file"
    else:
        source_engine = _source_engine()
        local_engine = get_local_history_engine(args.local_url or None)
        connection_mode = "configured_mysql_urls"
    ensure_local_history_tables(local_engine)
    limits = _resolve_limits(args.mode, limit=args.limit, stock_limit=args.stock_limit, gap_limit=args.gap_limit)

    if args.mode == "init":
        _print(
            {
                "status": "ok",
                "mode": "init",
                "local_database": str(local_engine.url.database or ""),
                "connection_mode": connection_mode,
                "tables": ["qmt_local_stock_kline", "qmt_local_stock_minute", "qmt_local_backfill_run"],
            },
            as_json=args.json,
        )
        return 0

    target_trade_dates: list[str] | None = None
    if args.target_window_universe:
        codes, target_trade_dates = _target_window_codes(
            source_engine,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        universe_source = TARGET_WINDOW_UNIVERSE_SOURCE
    else:
        codes = _codes_from_arg(
            source_engine,
            args.codes,
            limit=limits.stock_limit,
        )
        universe_source = (
            EXPLICIT_UNIVERSE_SOURCE if args.codes else CURRENT_UNIVERSE_SOURCE
        )
    if not codes:
        raise RuntimeError("No stock codes available for QMT local history backfill")
    universe = _universe_proof(
        codes,
        source=universe_source,
        start_date=args.start_date if args.mode == "daily" else "",
        end_date=args.end_date if args.mode == "daily" else "",
        target_trade_dates=target_trade_dates,
    )

    dry_run = not args.apply
    if args.mode == "daily":
        if not args.start_date or not args.end_date:
            raise RuntimeError("daily mode requires --start-date and --end-date")
        acquired, owner = _acquire_lock(DAILY_LOCK_PATH)
        if not acquired:
            _print(
                {
                    "status": "already_running",
                    "mode": "daily",
                    "dry_run": dry_run,
                    "lock_path": str(DAILY_LOCK_PATH),
                    "owner": owner,
                    "universe": universe,
                    "connection_mode": connection_mode,
                },
                as_json=args.json,
            )
            return 2
        try:
            allowed_missing_codes: list[str] = []
            if args.quarantine_invalid_target_no_native:
                allowed_missing_codes = _target_window_unattestable_codes(
                    source_engine,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
                unexpected_allowed_codes = sorted(
                    set(allowed_missing_codes) - set(codes)
                )
                if unexpected_allowed_codes:
                    raise RuntimeError(
                        "unattestable target codes fall outside the exact universe: "
                        f"count={len(unexpected_allowed_codes)}, "
                        f"sample={unexpected_allowed_codes[:10]}"
                    )
            result = backfill_daily_kline_local(
                source_engine=source_engine,
                local_engine=local_engine,
                stock_codes=codes,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=max(1, args.batch_size),
                dividend_type=args.dividend_type,
                provider=args.provider,
                dry_run=dry_run,
                allowed_missing_stock_codes=allowed_missing_codes,
            )
            if result.fetched_rows <= 0 or result.code_count != len(codes):
                raise RuntimeError(
                    "QMT daily backfill result is incomplete: "
                    f"requested_codes={len(codes)}, "
                    f"result_codes={result.code_count}, "
                    f"fetched_rows={result.fetched_rows}"
                )
            target_invalid_quarantine = None
            if args.quarantine_invalid_target_no_native:
                target_invalid_quarantine = (
                    _quarantine_invalid_target_rows_without_native(
                        source_engine,
                        start_date=args.start_date,
                        end_date=args.end_date,
                        provider=args.provider,
                        max_rows=args.max_target_quarantine_rows,
                    )
                )
            target_repair = None
            if args.repair_target_source_only:
                target_repair = _repair_target_source_only_rows(
                    source_engine,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    provider=args.provider,
                )
            legacy_quarantine = None
            if args.quarantine_source_only_legacy:
                legacy_quarantine = _quarantine_source_only_legacy_rows(
                    source_engine,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    provider=args.provider,
                )
        finally:
            _release_lock(DAILY_LOCK_PATH)
        payload = result_dict(result)
        payload["dry_run"] = dry_run
        payload["universe"] = universe
        payload["connection_mode"] = connection_mode
        if args.quarantine_invalid_target_no_native:
            payload["allowed_missing_target_codes"] = {
                "reason": TARGET_UNATTESTABLE_REASON,
                "stock_count": len(allowed_missing_codes),
                "stock_codes": allowed_missing_codes,
                "stock_codes_sha256": hashlib.sha256(
                    "\n".join(allowed_missing_codes).encode("utf-8")
                ).hexdigest(),
            }
        if target_invalid_quarantine is not None:
            payload["invalid_target_quarantine"] = target_invalid_quarantine
        if target_repair is not None:
            payload["target_source_only_repair"] = target_repair
        if legacy_quarantine is not None:
            payload["source_only_legacy_quarantine"] = legacy_quarantine
        _print(payload, as_json=args.json)
        return 0 if result.status == "SUCCESS" else 2

    if args.mode == "minute":
        trade_dates = [args.trade_date] if args.trade_date else load_trade_dates(
            source_engine,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=0,
        )
        if not trade_dates:
            raise RuntimeError("minute mode requires --trade-date, or parseable --start-date/--end-date")
        result = backfill_minute_local(
            source_engine=source_engine,
            local_engine=local_engine,
            stock_codes=codes,
            trade_dates=trade_dates,
            batch_size=max(1, args.batch_size),
            dry_run=dry_run,
        )
        payload = result_dict(result)
        payload["dry_run"] = dry_run
        _print(payload, as_json=args.json)
        return 0 if result.status == "SUCCESS" else 2

    lock_path = ROOT / "data" / "runtime" / "qmt_local_gap_repair.lock"
    acquired, owner = (True, "")
    if args.apply:
        acquired, owner = _acquire_lock(lock_path)
    if not acquired:
        _print(
            {
                "status": "already_running",
                "mode": "from-gaps",
                "dry_run": dry_run,
                "lock_path": str(lock_path),
                "owner": owner,
            },
            as_json=args.json,
        )
        return 0

    gaps = _gap_rows(source_engine, limit=limits.gap_limit, dataset=args.gap_dataset)
    results: list[dict[str, Any]] = []
    try:
        for gap in gaps:
            dataset = str(gap["dataset"])
            start_date = str(gap["gap_start"])[:10]
            end_date = str(gap["gap_end"])[:10]
            try:
                if dataset == "sm_stock_kline.1d":
                    result = backfill_daily_kline_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        start_date=start_date,
                        end_date=end_date,
                        batch_size=max(1, args.batch_size),
                        dividend_type=args.dividend_type,
                        provider=args.provider,
                        dry_run=dry_run,
                    )
                elif dataset == "sm_stock_minute.1m":
                    result = backfill_minute_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        trade_dates=[start_date],
                        batch_size=max(1, min(args.batch_size, 80)),
                        dry_run=dry_run,
                    )
                else:
                    results.append(
                        {
                            "gap_id": gap["id"],
                            "dataset": dataset,
                            "status": "skipped",
                            "reason": "unsupported_dataset",
                        }
                    )
                    continue

                item = result_dict(result)
                item["gap_id"] = gap["id"]
                resolved = result.status == "SUCCESS" and (dry_run or result.written_rows > 0)
                message = (
                    f"backfill {result.status}: fetched={result.fetched_rows}, "
                    f"written={result.written_rows}, dry_run={dry_run}"
                )
                item["gap_status_update"] = _update_gap_status(
                    source_engine,
                    gap_id=int(gap["id"]),
                    run_id=result.run_id,
                    resolved=resolved,
                    message=message,
                    dry_run=dry_run,
                )
                results.append(item)
            except Exception as exc:
                update_status = _update_gap_status(
                    source_engine,
                    gap_id=int(gap["id"]),
                    run_id=None,
                    resolved=False,
                    message=f"backfill failed: {exc}",
                    dry_run=dry_run,
                )
                results.append(
                    {
                        "gap_id": gap["id"],
                        "dataset": dataset,
                        "status": "failed",
                        "error": str(exc),
                        "gap_status_update": update_status,
                    }
                )
    finally:
        if args.apply:
            _release_lock(lock_path)

    failed_count = sum(1 for item in results if item.get("status") == "failed")
    _print(
        {
            "status": "ok" if failed_count == 0 else "partial_failed",
            "mode": "from-gaps",
            "dry_run": dry_run,
            "stock_count": len(codes),
            "stock_limit": limits.stock_limit,
            "gap_limit": limits.gap_limit,
            "gap_count": len(gaps),
            "executed": len(results),
            "resolved": sum(1 for item in results if item.get("gap_status_update") == "RESOLVED"),
            "failed": failed_count,
            "results": results,
        },
        as_json=args.json,
    )
    return 0 if failed_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
