from __future__ import annotations

import hashlib
import json
import logging
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from integrations.bigqmt import bridge as bigqmt_bridge
from integrations.bigqmt.backend import BigQmtBackend
from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID
from integrations.qmt import bridge
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.diagnostics import PROVIDER_ID as LEGACY_PROVIDER_ID
from server.common.batch_db import create_batch_engine
from server.common.config import get_mysql_url, get_qmt_history_mysql_url


logger = logging.getLogger(__name__)

CHINA_STANDARD_TIME = timezone(timedelta(hours=8), name="Asia/Shanghai")
LOCAL_KLINE_TABLE = "qmt_local_stock_kline"
LOCAL_MINUTE_TABLE = "qmt_local_stock_minute"
LOCAL_RUN_TABLE = "qmt_local_backfill_run"


@dataclass(frozen=True)
class LocalBackfillBatchResult:
    dataset: str
    period: str
    start_date: str
    end_date: str
    requested_codes: int
    fetched_rows: int
    written_rows: int
    skipped: bool
    error: str | None = None


@dataclass(frozen=True)
class LocalBackfillResult:
    run_id: str
    dataset: str
    status: str
    local_database: str
    start_date: str
    end_date: str
    code_count: int
    batch_count: int
    fetched_rows: int
    written_rows: int
    batches: list[LocalBackfillBatchResult]


def now_china() -> datetime:
    return datetime.now(CHINA_STANDARD_TIME).replace(tzinfo=None, microsecond=0)


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def _data_version(row: Mapping[str, Any]) -> str:
    keys = sorted(key for key in row if key not in {"received_at", "batch_id", "data_version"})
    payload = {key: row.get(key) for key in keys}
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()[:32]


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            return value
    return value


def _normalize_date(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return str(value or "").strip()[:10]


def _same_database(url_a: str, url_b: str) -> bool:
    if not url_a or not url_b:
        return False
    a = make_url(url_a)
    b = make_url(url_b)
    return (
        (a.drivername or "").split("+", 1)[0] == (b.drivername or "").split("+", 1)[0]
        and (a.host or "localhost").lower() in {(b.host or "localhost").lower(), "localhost", "127.0.0.1"}
        and (b.host or "localhost").lower() in {(a.host or "localhost").lower(), "localhost", "127.0.0.1"}
        and int(a.port or 3306) == int(b.port or 3306)
        and (a.database or "") == (b.database or "")
        and (a.username or "") == (b.username or "")
    )


def get_local_history_engine(local_url: str | None = None) -> Engine:
    prod = get_mysql_url(required=False)
    try:
        resolved = (local_url or get_qmt_history_mysql_url(required=True)).strip()
    except RuntimeError:
        prod_url = make_url(prod) if prod else None
        if prod_url and (prod_url.host or "localhost").lower() in {"localhost", "127.0.0.1"}:
            resolved = prod_url.set(database="probiga_qmt_history").render_as_string(hide_password=False)
        else:
            raise
    if prod and _same_database(resolved, prod):
        raise RuntimeError("QMT 历史本地库配置与生产 MYSQL_URL 相同，已拒绝执行")
    return create_batch_engine(resolved)


def ensure_local_history_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_KLINE_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
                    qmt_code VARCHAR(32) NOT NULL,
                    stock_code VARCHAR(16) NOT NULL,
                    short_name VARCHAR(128) NULL,
                    period VARCHAR(16) NOT NULL DEFAULT '1d',
                    trade_time DATETIME NOT NULL,
                    trade_date DATE NOT NULL,
                    k_type INT NOT NULL DEFAULT 1,
                    adjust_type INT NOT NULL DEFAULT 1,
                    open DECIMAL(20,6) NULL,
                    close DECIMAL(20,6) NULL,
                    high DECIMAL(20,6) NULL,
                    low DECIMAL(20,6) NULL,
                    volume DECIMAL(24,6) NULL,
                    amount DECIMAL(24,6) NULL,
                    `change` DECIMAL(20,6) NULL,
                    change_pct DECIMAL(20,6) NULL,
                    turnover_ratio DECIMAL(20,6) NULL,
                    pre_close DECIMAL(20,6) NULL,
                    source_time DATETIME NULL,
                    received_at DATETIME NOT NULL,
                    batch_id VARCHAR(64) NOT NULL,
                    data_version VARCHAR(64) NULL,
                    quality_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                    permission_status VARCHAR(32) NOT NULL DEFAULT 'SUPPORTED',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NULL,
                    UNIQUE KEY uk_qmt_local_kline (provider, stock_code, period, trade_date, adjust_type),
                    KEY idx_qmt_local_kline_date (trade_date),
                    KEY idx_qmt_local_kline_code_time (stock_code, trade_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_MINUTE_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
                    qmt_code VARCHAR(32) NOT NULL,
                    stock_code VARCHAR(16) NOT NULL,
                    short_name VARCHAR(128) NULL,
                    period VARCHAR(16) NOT NULL DEFAULT '1m',
                    trade_time DATETIME NOT NULL,
                    trade_date DATE NOT NULL,
                    price DECIMAL(20,6) NULL,
                    avg_price DECIMAL(20,6) NULL,
                    `change` DECIMAL(20,6) NULL,
                    change_pct DECIMAL(20,6) NULL,
                    volume DECIMAL(24,6) NULL,
                    amount DECIMAL(24,6) NULL,
                    pre_close DECIMAL(20,6) NULL,
                    source_time DATETIME NULL,
                    received_at DATETIME NOT NULL,
                    batch_id VARCHAR(64) NOT NULL,
                    data_version VARCHAR(64) NULL,
                    quality_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                    permission_status VARCHAR(32) NOT NULL DEFAULT 'SUPPORTED',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NULL,
                    UNIQUE KEY uk_qmt_local_minute (provider, stock_code, period, trade_time),
                    KEY idx_qmt_local_minute_date (trade_date),
                    KEY idx_qmt_local_minute_code_time (stock_code, trade_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_RUN_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(64) NOT NULL,
                    provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
                    dataset VARCHAR(64) NOT NULL,
                    period VARCHAR(16) NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    requested_codes INT NOT NULL DEFAULT 0,
                    fetched_rows BIGINT NOT NULL DEFAULT 0,
                    written_rows BIGINT NOT NULL DEFAULT 0,
                    error_message TEXT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    extra_json TEXT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_qmt_local_run (run_id),
                    KEY idx_qmt_local_run_dataset (dataset, period, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )


def load_stock_codes(source_engine: Engine, *, codes: Sequence[str] | None = None, limit: int = 0) -> list[str]:
    if codes:
        result = []
        for code in codes:
            text_value = str(code or "").strip()
            if not text_value:
                continue
            result.append(text_value.split(".", 1)[0].zfill(6))
        return sorted(set(result))
    sql = "SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|4|6|8|9)' ORDER BY stock_code"
    params: dict[str, Any] = {}
    if limit > 0:
        sql += " LIMIT :limit"
        params["limit"] = limit
    with source_engine.begin() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [str(row[0]).strip().zfill(6) for row in rows]


def load_trade_dates(source_engine: Engine, *, start_date: str, end_date: str, limit: int = 0) -> list[str]:
    params = {"start": _normalize_date(start_date), "end": _normalize_date(end_date)}
    calendar_sql = """
        SELECT trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date >= :start
          AND trade_date <= :end
        ORDER BY trade_date
    """
    if limit > 0:
        calendar_sql += " LIMIT :limit"
        params["limit"] = limit
    rows = []
    try:
        with source_engine.begin() as conn:
            rows = conn.execute(text(calendar_sql), params).fetchall()
    except Exception as exc:
        logger.debug("QMT local history calendar query failed, falling back to kline dates: %s", exc)
        rows = []
    if rows:
        return [str(row[0])[:10] for row in rows]

    fallback_sql = """
        SELECT DISTINCT trade_date
        FROM sm_stock_kline
        WHERE k_type = 1
          AND trade_date >= :start
          AND trade_date <= :end
        ORDER BY trade_date
    """
    if limit > 0:
        fallback_sql += " LIMIT :limit"
    with source_engine.begin() as conn:
        rows = conn.execute(text(fallback_sql), params).fetchall()
    return [str(row[0])[:10] for row in rows]


def _chunked(items: Sequence[str], size: int) -> Iterable[list[str]]:
    chunk_size = max(1, int(size))
    for idx in range(0, len(items), chunk_size):
        yield list(items[idx : idx + chunk_size])


def _short_name_map(source_engine: Engine, codes: Sequence[str]) -> dict[str, str]:
    if not codes:
        return {}
    placeholders = ", ".join(f":code_{idx}" for idx, _ in enumerate(codes))
    params = {f"code_{idx}": code for idx, code in enumerate(codes)}
    with source_engine.begin() as conn:
        rows = conn.execute(
            text(f"SELECT stock_code, short_name FROM si_all_code WHERE stock_code IN ({placeholders})"),
            params,
        ).fetchall()
    return {str(code).zfill(6): str(name or "") for code, name in rows}


def _prepare_kline_rows(
    frame: pd.DataFrame,
    *,
    source_engine: Engine,
    period: str,
    batch_id: str,
    provider: str = LEGACY_PROVIDER_ID,
) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows = frame.to_dict(orient="records")
    names = _short_name_map(source_engine, [str(row.get("stock_code") or "").zfill(6) for row in rows])
    received_at = now_china()
    prepared: list[dict[str, Any]] = []
    for raw in rows:
        stock_code = str(raw.get("stock_code") or "").zfill(6)
        raw_adjust_type = raw.get("adjust_type")
        try:
            adjust_type = 0 if raw_adjust_type is None or pd.isna(raw_adjust_type) else raw_adjust_type
        except (TypeError, ValueError):
            adjust_type = raw_adjust_type
        row = {
            "provider": raw.get("data_source") or provider,
            "qmt_code": raw.get("qmt_code") or to_qmt_symbol(stock_code) or "",
            "stock_code": stock_code,
            "short_name": raw.get("short_name") or names.get(stock_code, ""),
            "period": period,
            "trade_time": raw.get("trade_time"),
            "trade_date": raw.get("trade_date"),
            "k_type": raw.get("k_type") or 1,
            "adjust_type": adjust_type,
            "open": raw.get("open"),
            "close": raw.get("close"),
            "high": raw.get("high"),
            "low": raw.get("low"),
            "volume": raw.get("volume"),
            "amount": raw.get("amount"),
            "change": raw.get("change"),
            "change_pct": raw.get("change_pct"),
            "turnover_ratio": raw.get("turnover_ratio"),
            "pre_close": raw.get("pre_close"),
            "source_time": raw.get("source_time") or raw.get("trade_time"),
            "received_at": raw.get("received_at") or received_at,
            "batch_id": raw.get("batch_id") or batch_id,
            "quality_status": raw.get("quality_status") or "SOURCE_CAPTURED",
            "permission_status": raw.get("permission_status") or "SUPPORTED",
        }
        row = {key: _clean_value(value) for key, value in row.items()}
        row["data_version"] = _data_version(row)
        prepared.append(row)
    return prepared


def _prepare_minute_rows(
    frame: pd.DataFrame,
    *,
    source_engine: Engine,
    period: str,
    batch_id: str,
    provider: str = LEGACY_PROVIDER_ID,
) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows = frame.to_dict(orient="records")
    names = _short_name_map(source_engine, [str(row.get("stock_code") or "").zfill(6) for row in rows])
    received_at = now_china()
    prepared: list[dict[str, Any]] = []
    for raw in rows:
        stock_code = str(raw.get("stock_code") or "").zfill(6)
        row = {
            "provider": raw.get("data_source") or provider,
            "qmt_code": raw.get("qmt_code") or to_qmt_symbol(stock_code) or "",
            "stock_code": stock_code,
            "short_name": names.get(stock_code, ""),
            "period": period,
            "trade_time": raw.get("trade_time"),
            "trade_date": raw.get("trade_date"),
            "price": raw.get("price"),
            "avg_price": raw.get("avg_price"),
            "change": raw.get("change"),
            "change_pct": raw.get("change_pct"),
            "volume": raw.get("volume"),
            "amount": raw.get("amount"),
            "pre_close": raw.get("pre_close"),
            "source_time": raw.get("source_time") or raw.get("trade_time"),
            "received_at": raw.get("received_at") or received_at,
            "batch_id": raw.get("batch_id") or batch_id,
            "quality_status": raw.get("quality_status") or "SOURCE_CAPTURED",
            "permission_status": raw.get("permission_status") or "SUPPORTED",
        }
        row = {key: _clean_value(value) for key, value in row.items()}
        row["data_version"] = _data_version(row)
        prepared.append(row)
    return prepared


def _upsert_rows(engine: Engine, *, table_name: str, rows: Sequence[Mapping[str, Any]], key_columns: Sequence[str]) -> int:
    if not rows:
        return 0
    columns = list(rows[0].keys())
    col_sql = ", ".join(f"`{column}`" for column in columns)
    val_sql = ", ".join(f":{column}" for column in columns)
    update_columns = [column for column in columns if column not in set(key_columns) and column not in {"id", "created_at"}]
    update_sql = ", ".join(f"`{column}`=VALUES(`{column}`)" for column in update_columns)
    sql = text(
        f"INSERT INTO `{table_name}` ({col_sql}) VALUES ({val_sql}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}, updated_at=NOW()"
    )
    with engine.begin() as conn:
        conn.execute(sql, [dict(row) for row in rows])
    return len(rows)


def persist_daily_kline_capture(
    frame: pd.DataFrame,
    *,
    source_engine: Engine,
    local_engine: Engine | None = None,
    batch_id: str = "",
    provider: str = BIGQMT_PROVIDER_ID,
) -> int:
    """Persist the raw QMT evidence before canonical daily bars are published."""
    target_engine = local_engine or get_local_history_engine()
    ensure_local_history_tables(target_engine)
    rows = _prepare_kline_rows(
        frame,
        source_engine=source_engine,
        period="1d",
        batch_id=batch_id or (
            f"qmt_capture_{now_china().strftime('%Y%m%d_%H%M%S')}"
        ),
        provider=provider,
    )
    return _upsert_rows(
        target_engine,
        table_name=LOCAL_KLINE_TABLE,
        rows=rows,
        key_columns=[
            "provider",
            "stock_code",
            "period",
            "trade_date",
            "adjust_type",
        ],
    )


def _record_run_start(
    engine: Engine,
    *,
    run_id: str,
    dataset: str,
    period: str,
    start_date: str,
    end_date: str,
    requested_codes: int,
    extra: Mapping[str, Any],
    provider: str = LEGACY_PROVIDER_ID,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {LOCAL_RUN_TABLE} (
                    run_id, provider, dataset, period, start_date, end_date, status,
                    requested_codes, started_at, extra_json
                ) VALUES (
                    :run_id, :provider, :dataset, :period, :start_date, :end_date, 'RUNNING',
                    :requested_codes, :started_at, :extra_json
                )
                """
            ),
            {
                "run_id": run_id,
                "provider": provider,
                "dataset": dataset,
                "period": period,
                "start_date": _normalize_date(start_date),
                "end_date": _normalize_date(end_date),
                "requested_codes": requested_codes,
                "started_at": now_china(),
                "extra_json": _canonical(extra),
            },
        )


def _record_run_finish(
    engine: Engine,
    *,
    run_id: str,
    status: str,
    fetched_rows: int,
    written_rows: int,
    error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {LOCAL_RUN_TABLE}
                SET status=:status, fetched_rows=:fetched_rows, written_rows=:written_rows,
                    error_message=:error_message, finished_at=:finished_at
                WHERE run_id=:run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "fetched_rows": fetched_rows,
                "written_rows": written_rows,
                "error_message": error_message,
                "finished_at": now_china(),
            },
        )


def backfill_daily_kline_local(
    *,
    source_engine: Engine,
    local_engine: Engine,
    stock_codes: Sequence[str],
    start_date: str,
    end_date: str,
    batch_size: int = 80,
    dividend_type: str = "none",
    backend: str = "bigqmt",
    dry_run: bool = False,
) -> LocalBackfillResult:
    ensure_local_history_tables(local_engine)
    run_id = f"qmt_hist_kline_{now_china().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    batches: list[LocalBackfillBatchResult] = []
    fetched_total = 0
    written_total = 0
    selected_backend = str(backend or "bigqmt").strip().lower()
    if selected_backend not in {"bigqmt", "legacy", "auto"}:
        raise ValueError("backend must be bigqmt, legacy or auto")
    if selected_backend == "auto":
        selected_backend = "bigqmt" if bigqmt_bridge.is_configured() else "legacy"
    if selected_backend == "bigqmt" and not bigqmt_bridge.is_configured():
        raise RuntimeError(
            "BigQMT built-in strategy bridge is not active; refusing to fall back "
            "to the incompatible legacy runtime"
        )
    provider = BIGQMT_PROVIDER_ID if selected_backend == "bigqmt" else LEGACY_PROVIDER_ID
    _record_run_start(
        local_engine,
        run_id=run_id,
        dataset=LOCAL_KLINE_TABLE,
        period="1d",
        start_date=start_date,
        end_date=end_date,
        requested_codes=len(stock_codes),
        extra={
            "dry_run": dry_run,
            "dividend_type": dividend_type,
            "batch_size": batch_size,
            "backend": selected_backend,
        },
        provider=provider,
    )
    status = "SUCCESS"
    error_message: str | None = None
    try:
        for batch in _chunked(list(stock_codes), batch_size):
            if selected_backend == "bigqmt":
                frame = BigQmtBackend().fetch_kline(
                    list(batch),
                    start_date,
                    end_date,
                    dividend_type=dividend_type,
                    download_history=True,
                )
                requested_codes = len(batch)
            else:
                qmt_codes = [to_qmt_symbol(code) for code in batch]
                qmt_codes = [code for code in qmt_codes if code]
                frame = bridge.kline(
                    qmt_codes,
                    start_date=start_date,
                    end_date=end_date,
                    dividend_type=dividend_type,
                    batch_size=batch_size,
                    timeout=900,
                )
                requested_codes = len(qmt_codes)
            rows = _prepare_kline_rows(
                frame,
                source_engine=source_engine,
                period="1d",
                batch_id=run_id,
                provider=provider,
            )
            fetched_total += len(rows)
            written = 0 if dry_run else _upsert_rows(
                local_engine,
                table_name=LOCAL_KLINE_TABLE,
                rows=rows,
                key_columns=["provider", "stock_code", "period", "trade_date", "adjust_type"],
            )
            written_total += written
            batches.append(
                LocalBackfillBatchResult(
                    dataset=LOCAL_KLINE_TABLE,
                    period="1d",
                    start_date=_normalize_date(start_date),
                    end_date=_normalize_date(end_date),
                    requested_codes=requested_codes,
                    fetched_rows=len(rows),
                    written_rows=written,
                    skipped=dry_run,
                )
            )
    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)
        raise
    finally:
        _record_run_finish(
            local_engine,
            run_id=run_id,
            status=status,
            fetched_rows=fetched_total,
            written_rows=written_total,
            error_message=error_message,
        )
    return LocalBackfillResult(
        run_id=run_id,
        dataset=LOCAL_KLINE_TABLE,
        status=status,
        local_database=str(make_url(str(local_engine.url)).database or ""),
        start_date=_normalize_date(start_date),
        end_date=_normalize_date(end_date),
        code_count=len(stock_codes),
        batch_count=len(batches),
        fetched_rows=fetched_total,
        written_rows=written_total,
        batches=batches,
    )


def backfill_minute_local(
    *,
    source_engine: Engine,
    local_engine: Engine,
    stock_codes: Sequence[str],
    trade_dates: Sequence[str],
    batch_size: int = 50,
    backend: str = "bigqmt",
    dry_run: bool = False,
) -> LocalBackfillResult:
    ensure_local_history_tables(local_engine)
    start_date = trade_dates[0] if trade_dates else ""
    end_date = trade_dates[-1] if trade_dates else ""
    run_id = f"qmt_hist_minute_{now_china().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    batches: list[LocalBackfillBatchResult] = []
    fetched_total = 0
    written_total = 0
    selected_backend = str(backend or "bigqmt").strip().lower()
    if selected_backend not in {"bigqmt", "legacy", "auto"}:
        raise ValueError("backend must be bigqmt, legacy or auto")
    if selected_backend == "auto":
        selected_backend = "bigqmt" if bigqmt_bridge.is_configured() else "legacy"
    if selected_backend == "bigqmt" and not bigqmt_bridge.is_configured():
        raise RuntimeError(
            "BigQMT built-in strategy bridge is not active; refusing to fall back "
            "to the incompatible legacy runtime"
        )
    provider = BIGQMT_PROVIDER_ID if selected_backend == "bigqmt" else LEGACY_PROVIDER_ID
    _record_run_start(
        local_engine,
        run_id=run_id,
        dataset=LOCAL_MINUTE_TABLE,
        period="1m",
        start_date=start_date,
        end_date=end_date,
        requested_codes=len(stock_codes),
        extra={
            "dry_run": dry_run,
            "batch_size": batch_size,
            "trade_dates": list(trade_dates),
            "backend": selected_backend,
        },
        provider=provider,
    )
    status = "SUCCESS"
    error_message: str | None = None
    try:
        for trade_date in trade_dates:
            for batch in _chunked(list(stock_codes), batch_size):
                if selected_backend == "bigqmt":
                    frame = BigQmtBackend().fetch_minute(
                        list(batch),
                        trade_date,
                        start_date=trade_date,
                        end_date=trade_date,
                        download_history=True,
                    )
                    requested_codes = len(batch)
                else:
                    qmt_codes = [to_qmt_symbol(code) for code in batch]
                    qmt_codes = [code for code in qmt_codes if code]
                    frame = bridge.minute(
                        qmt_codes,
                        trade_date=trade_date,
                        start_date=trade_date,
                        end_date=trade_date,
                        batch_size=batch_size,
                        timeout=900,
                    )
                    requested_codes = len(qmt_codes)
                rows = _prepare_minute_rows(
                    frame,
                    source_engine=source_engine,
                    period="1m",
                    batch_id=run_id,
                    provider=provider,
                )
                fetched_total += len(rows)
                written = 0 if dry_run else _upsert_rows(
                    local_engine,
                    table_name=LOCAL_MINUTE_TABLE,
                    rows=rows,
                    key_columns=["provider", "stock_code", "period", "trade_time"],
                )
                written_total += written
                batches.append(
                    LocalBackfillBatchResult(
                        dataset=LOCAL_MINUTE_TABLE,
                        period="1m",
                        start_date=trade_date,
                        end_date=trade_date,
                        requested_codes=requested_codes,
                        fetched_rows=len(rows),
                        written_rows=written,
                        skipped=dry_run,
                    )
                )
    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)
        raise
    finally:
        _record_run_finish(
            local_engine,
            run_id=run_id,
            status=status,
            fetched_rows=fetched_total,
            written_rows=written_total,
            error_message=error_message,
        )
    return LocalBackfillResult(
        run_id=run_id,
        dataset=LOCAL_MINUTE_TABLE,
        status=status,
        local_database=str(make_url(str(local_engine.url)).database or ""),
        start_date=_normalize_date(start_date),
        end_date=_normalize_date(end_date),
        code_count=len(stock_codes),
        batch_count=len(batches),
        fetched_rows=fetched_total,
        written_rows=written_total,
        batches=batches,
    )


def result_dict(result: LocalBackfillResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["batches"] = [asdict(batch) for batch in result.batches]
    return payload
