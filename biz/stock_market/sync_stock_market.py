# -*- coding: utf-8 -*-
"""
将 adata.stock.market（股票-STOCK-MARKET 文档）接口同步到 MySQL。

执行示例::
  python -m biz.stock_market.sync_stock_market
  python -m biz.stock_market.sync_stock_market --only stock_current
  python -m biz.stock_market.sync_stock_market --only dividend,stock_kline
  python -m biz.stock_market.sync_stock_market --only stock_kline --kline-source akshare \\
    --kline-start 2020-01-01 --kline-end 2024-12-31 --kline-adjust qfq
  # 类 a_share_daily_import 分批 / 断点（YYYYMMDD、limit=0 表示从 offset 到表尾）::
  python -m biz.stock_market.sync_stock_market --only stock_kline --kline-source akshare \\
    --start-date 20200101 --end-date 20260417 --offset 0 --limit 200 \\
    --progress-file stock_kline_akshare_progress.txt
  python -m biz.stock_market.sync_stock_market --only stock_kline --kline-source akshare \\
    --start-date 20200101 --end-date 20260417 --offset 0 --limit 0 --skip-progress

环境变量（常用）：
  MYSQL_URL                 必填，MySQL 连接串；也可写入项目根目录 ``.env``
  运行账号只验证已迁移表面，不执行持久 DDL，也不允许全局/单表预清空。
  SM_REQUEST_SLEEP          请求后休眠秒数，默认 0.2
  SM_HTTP_RETRIES / SM_HTTP_BACKOFF  重试参数，默认 8 / 3.0
  SM_MAX_STOCKS             个股类接口股票数量上限；默认 200。设为 0 表示不限制（同步 si_all_code 全部）
  SM_MAX_INDEXES            指数类接口数量上限，默认 50
  SM_MAX_CONCEPTS           概念类接口数量上限，默认 50
  SM_MARKET_START           个股 K线起始日，默认 2020-01-01
  SM_MARKET_END             个股 K线结束日，默认当天
  SM_INDEX_START            指数/概念 K线起始日，默认 2020-01-01
  SM_INDEX_END              指数/概念 K线结束日，默认当天
  SM_STOCK_K_TYPE           个股 K线类型，默认 1
  SM_STOCK_ADJUST_TYPE      个股复权类型，默认 1（adata 用 0/1/2）
  SM_STOCK_KLINE_SOURCE     个股 K 线数据源：adata（默认）| akshare（日 K：默认新浪，见下；无需安装 akshare）
                             写入 sm_stock_kline 时会带 short_name（简称），来自表 si_all_code，请先同步 STOCK-INFO 全码表。
  SM_STOCK_KLINE_ENGINE     仅 akshare 日 K 引擎：sina（默认，需 Node + vendor 解密脚本）| east / em / eastmoney（东财 push2his）
  SM_NODE_BIN               新浪日 K 解密用：node.exe 绝对路径（未加入 PATH 时填写，如 C:\\Program Files\\nodejs\\node.exe）
  SM_STOCK_KLINE_AKSHARE_TRUNCATE  已废弃；设为 1 会失败关闭，仅允许股票+日期分区原子替换
  SM_STOCK_KLINE_AKSHARE_ADJUST     akshare 复权：空/qfq/hfq，默认空（与 --kline-adjust 二选一，CLI 优先）
  SM_STOCK_KLINE_AKSHARE_SLEEP      仅 akshare 个股 K：每只股票请求后的休眠秒数；未设置则沿用 SM_REQUEST_SLEEP。
                                    东财接口亦可能限流，求稳可设 0.5~2；不建议为提速低于 0.15 或并发多请求。
  SM_STOCK_KLINE_PROGRESS_LOG_EVERY  拉取时默认每 25 只股票打一条进度 INFO；设为 1 则每只都打（日志量大）。
  SM_KLINE_EAST_CHUNK_DAYS          东财日 K 单次请求最长日历天（分片），默认 240，过长单次易被断连。
  SM_KLINE_EAST_CHUNK_SLEEP         分片之间休眠秒数，默认 0.12。
  SM_KLINE_EAST_HOSTS               备用东财镜像根 URL，逗号分隔；不设则用内置 push2his / 33 / 63 / 81 / 90 等。
  --start-date / --end-date         与 a_share 一致可写 YYYYMMDD；若指定则覆盖 --kline-start/--kline-end。
  --offset / --limit                仅 si_all_code 切片：limit=-1（默认）仍按 SM_MAX_STOCKS；limit=0 表示从 offset 到末尾；limit>0 本批只数。
  --progress-file / --skip-progress  akshare 模式断点续跑（见 --help）。
  SM_INDEX_K_TYPE           指数/概念 K线类型，默认 1
  SM_CURRENT_BATCH          实时行情分批大小，默认 300
  SM_MAX_WORKERS            并发线程数，默认 4；个股/概念/指数接口均通过线程池并行拉取
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar

import numpy as np
import pandas as pd
import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import make_url

try:
    from urllib3.exceptions import ProtocolError as Urllib3ProtocolError

    _URLLIB3_RETRIABLE = (Urllib3ProtocolError,)
except ImportError:
    _URLLIB3_RETRIABLE = ()

T = TypeVar("T")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sync_stock_market")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from tools.env_config import load_project_env

load_project_env()

from server.common.batch_db import create_batch_engine, quote_identifier, read_frame, read_frame_direct, replace_table_rows, replace_table_rows_exact_keys, write_frame
from server.common.kline_data import get_kline_engine
from server.common.legacy_table_surface import validate_required_table_surface
from server.common.mysql_lock import (
    CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
    STOCK_KLINE_FREEZE_LOCK_NAME,
    STOCK_MINUTE_FREEZE_LOCK_NAME,
    mysql_named_lock,
    supersede_overlapping_qmt_minute_forward_receipts,
)
from server.common.qmt_history_coverage import (
    QMT_MINUTE_GRID_NATIVE_FIXTURE_HASH,
    QMT_MINUTE_GRID_PROFILE,
    assess_minute_coverage,
    canonical_digest as qmt_coverage_digest,
    combine_minute_coverage_partitions,
    insert_coverage_bundle,
    minute_grid_profile_for_capture,
    minute_time_grid,
    require_exact_coverage,
)
from server.common.process_env import temporary_env
from integrations.qmt.safe_upsert import safe_upsert_rows

RUNTIME_TABLE_ORDER = [
    "sm_index_current",
    "sm_index_minute",
    "sm_index_kline",
    "sm_concept_capital_flow_east",
    "sm_concept_east_current",
    "sm_concept_east_minute",
    "sm_concept_east_kline",
    "sm_concept_ths_current",
    "sm_concept_ths_minute",
    "sm_concept_ths_kline",
    "sm_stock_capital_flow_daily",
    "sm_stock_capital_flow_min",
    "sm_stock_bar",
    "sm_stock_five_level",
    "sm_stock_current",
    "sm_stock_minute",
    "sm_stock_kline",
    "sm_dividend",
]

STEP_NAMES = {
    "dividend",
    "stock_kline",
    "stock_minute",
    "stock_current",
    "stock_five",
    "stock_bar",
    "stock_flow_min",
    "stock_flow_daily",
    "concept_ths_kline",
    "concept_ths_minute",
    "concept_ths_current",
    "concept_east_kline",
    "concept_east_minute",
    "concept_east_current",
    "concept_flow_east",
    "index_kline",
    "index_minute",
    "index_current",
}


def _log_mysql_target(url_str: str) -> None:
    """Log the primary metadata database without implying history write routing."""
    try:
        u = make_url(url_str)
        logger.info(
            "Primary MySQL: database=%s host=%s; K-line/minute writes follow the configured history route.",
            u.database,
            u.host,
        )
    except Exception as e:  # pylint: disable=broad-except
        logger.info("MySQL URL 已使用（解析库名失败）：%s", e)


def _sleep() -> None:
    time.sleep(float(os.environ.get("SM_REQUEST_SLEEP", "0.2")))


def _source_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name, "").strip().lower()
        if value:
            if value in {"big_qmt", "qmt_big"}:
                return "bigqmt"
            return value
    return default


def _qmt_runtime_available(source: str = "qmt") -> bool:
    """Return True only when the local QMT bridge can run on this host."""
    try:
        if str(source).strip().lower() == "bigqmt":
            from integrations.bigqmt import bridge
        else:
            from integrations.qmt import bridge

        return bool(bridge.is_configured())
    except Exception as exc:  # pragma: no cover - defensive import guard
        logger.debug("QMT runtime availability check failed: %s", exc)
        return False


def _index_source(kind: str) -> str:
    upper = str(kind or "").strip().upper()
    return _source_value(
        f"DATA_SOURCE_INDEX_{upper}",
        f"SM_INDEX_{upper}_SOURCE",
        f"DATA_SOURCE_{upper}",
    )


def _concept_source(kind: str) -> str:
    upper = str(kind or "").strip().upper()
    return _source_value(
        f"DATA_SOURCE_CONCEPT_{upper}",
        f"SM_CONCEPT_{upper}_SOURCE",
        "DATA_SOURCE_CONCEPT_LIST",
        "SI_CONCEPT_SOURCE",
    )


def _stock_flow_source(kind: str) -> str:
    upper = str(kind or "").strip().upper()
    return _source_value(
        f"DATA_SOURCE_FLOW_{upper}",
        f"SM_STOCK_FLOW_{upper}_SOURCE",
        f"DATA_SOURCE_STOCK_FLOW_{upper}",
    )


def _max_workers() -> int:
    return max(1, int(os.environ.get("SM_MAX_WORKERS", "4")))


def _concurrent_run(
    codes: list[str],
    fn: Callable[[str], pd.DataFrame | None],
    *,
    label: str = "",
    log_every: int = 200,
    fail_on_error: bool = False,
) -> list[pd.DataFrame]:
    """线程池并发执行 per-code API 调用，收集非空 DataFrame。"""
    if not codes:
        return []
    workers = _max_workers()
    parts: list[pd.DataFrame] = []
    done_count = 0
    total = len(codes)
    lock = threading.Lock()
    failures: list[tuple[str, BaseException]] = []

    def _worker(code: str) -> pd.DataFrame | None:
        df = fn(code)
        _sleep()
        return df

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, c): c for c in codes}
        for future in as_completed(futures):
            code = futures[future]
            done_count += 1
            try:
                df = future.result()
                if df is not None and not df.empty:
                    with lock:
                        parts.append(df)
            except Exception as e:
                logger.warning("%s %s 失败：%s", label, code, e)
                failures.append((code, e))
            if done_count % log_every == 0 or done_count == total:
                logger.info("%s：进度 %d/%d（并发 %d）", label, done_count, total, workers)
    if fail_on_error and failures:
        samples = ", ".join(f"{code}: {error}" for code, error in failures[:3])
        raise RuntimeError(
            f"{label or 'remote collection'} failed for {len(failures)}/{total} "
            f"requested codes; preserving previous data; samples={samples}"
        )
    return parts


def _minimum_coverage(env_name: str, default: float) -> float:
    try:
        return min(1.0, max(0.0, float(os.environ.get(env_name, str(default)))))
    except (TypeError, ValueError):
        return default


def _validated_code_snapshot(
    frame: pd.DataFrame,
    requested_codes: list[str],
    *,
    code_column: str,
    label: str,
    coverage_env: str,
    default_coverage: float,
) -> tuple[pd.DataFrame, list[str], float]:
    """Validate a fetched code-scoped snapshot before any canonical DML."""

    requested = {
        str(code).strip().zfill(6)
        for code in requested_codes
        if str(code).strip()
    }
    if not requested:
        raise RuntimeError(f"{label} has no requested code universe")
    if frame is None or frame.empty or code_column not in frame.columns:
        raise RuntimeError(f"{label} returned no rows; preserving previous data")
    complete = frame.copy()
    complete[code_column] = complete[code_column].astype(str).str.strip().str.zfill(6)
    received = set(complete[code_column].tolist())
    outside = received - requested
    if outside:
        raise RuntimeError(
            f"{label} returned {len(outside)} codes outside the requested universe"
        )
    coverage = len(received) / len(requested)
    minimum = _minimum_coverage(coverage_env, default_coverage)
    if coverage < minimum:
        raise RuntimeError(
            f"{label} coverage below threshold: {len(received)}/{len(requested)} "
            f"({coverage:.1%}) < {minimum:.1%}; preserving previous data"
        )
    # Only codes proven present in the replacement may be deleted.  A source
    # may legitimately omit halted/new symbols even after passing the global
    # coverage gate; their previous rows must remain intact.
    return complete, sorted(received), coverage


def _code_scope_predicate(
    codes: list[str],
    *,
    column: str,
    prefix: str = "scope_code",
) -> tuple[str, dict[str, Any]]:
    normalized = sorted({str(code).strip().zfill(6) for code in codes if str(code).strip()})
    if not normalized:
        raise RuntimeError("replacement code scope must not be empty")
    placeholders = ", ".join(f":{prefix}_{idx}" for idx in range(len(normalized)))
    return (
        f"{quote_identifier(column)} IN ({placeholders})",
        {f"{prefix}_{idx}": code for idx, code in enumerate(normalized)},
    )


def _replace_validated_code_snapshot(
    engine: Engine,
    frame: pd.DataFrame,
    *,
    table_name: str,
    requested_codes: list[str],
    code_column: str,
    label: str,
    coverage_env: str,
    default_coverage: float,
) -> int:
    complete, scope_codes, coverage = _validated_code_snapshot(
        frame,
        requested_codes,
        code_column=code_column,
        label=label,
        coverage_env=coverage_env,
        default_coverage=default_coverage,
    )
    predicate, params = _code_scope_predicate(scope_codes, column=code_column)
    written = replace_table_rows(
        _clean_df(_with_etl(complete)),
        table_name,
        engine,
        where_sql=predicate,
        params=params,
    )
    logger.info(
        "%s: atomically replaced %d rows for %d requested codes (coverage=%.2f%%)",
        label,
        written,
        len(scope_codes),
        coverage * 100,
    )
    return written


def _replace_validated_code_date_frame(
    engine: Engine,
    frame: pd.DataFrame,
    *,
    table_name: str,
    requested_codes: list[str],
    code_column: str,
    date_column: str,
    label: str,
    coverage_env: str,
    default_coverage: float,
    day_partition: bool = False,
    identity_columns: tuple[str, ...] | None = None,
    extra_where: str = "",
    extra_params: dict[str, Any] | None = None,
    receipt_engine: Engine | None = None,
) -> int:
    complete, scope_codes, coverage = _validated_code_snapshot(
        frame,
        requested_codes,
        code_column=code_column,
        label=label,
        coverage_env=coverage_env,
        default_coverage=default_coverage,
    )
    parsed_dates = pd.to_datetime(complete[date_column], errors="coerce")
    complete = complete.loc[parsed_dates.notna()].copy()
    parsed_dates = parsed_dates.loc[parsed_dates.notna()]
    if complete.empty:
        raise RuntimeError(
            f"{label} has no valid target dates; preserving previous data"
        )
    # Re-run coverage after date validation.  A symbol represented only by
    # malformed dates must not enter the DELETE scope.
    complete, scope_codes, coverage = _validated_code_snapshot(
        complete,
        requested_codes,
        code_column=code_column,
        label=label,
        coverage_env=coverage_env,
        default_coverage=default_coverage,
    )
    parsed_dates = pd.to_datetime(complete[date_column], errors="coerce")
    start_value = parsed_dates.min()
    end_value = parsed_dates.max()
    complete[date_column] = (
        parsed_dates
        if day_partition
        else parsed_dates.dt.strftime("%Y-%m-%d")
    )
    identities = tuple(identity_columns or (code_column, date_column))
    if (
        not identities
        or code_column not in identities
        or date_column not in identities
        or len(set(identities)) != len(identities)
        or any(column not in complete.columns for column in identities)
    ):
        raise RuntimeError(f"{label} exact replacement identity is invalid")
    for column in identities:
        series = complete[column]
        if series.isna().any() or series.astype(str).str.strip().eq("").any():
            raise RuntimeError(
                f"{label} exact replacement identity {column} is incomplete"
            )
    if complete.duplicated(subset=list(identities), keep=False).any():
        raise RuntimeError(
            f"{label} returned duplicate exact identities; preserving previous data"
        )
    # Legacy callers supplied the former broad DELETE predicate for K-line
    # dimensions.  Keep validating those frozen values, but never use the
    # predicate to delete an entire date range.
    for column, parameter in re.findall(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\s*=\s*:([A-Za-z_][A-Za-z0-9_]*)",
        extra_where or "",
    ):
        if (
            column not in identities
            or column not in complete.columns
            or parameter not in (extra_params or {})
            or not complete[column].eq((extra_params or {})[parameter]).all()
        ):
            raise RuntimeError(
                f"{label} frozen identity predicate is not bound to staged rows"
            )
    stage_connection, stage_table = _create_temporary_stage(
        engine,
        target_table=table_name,
        prefix=f"tmp_exact_{table_name}",
    )
    try:
        staged = _append_temporary_stage(
            stage_connection,
            stage_table,
            complete,
        )
        if staged != len(complete):
            raise RuntimeError(
                f"{label} exact stage row mismatch; preserving previous data"
            )
        written = _publish_temporary_stage_exact_keys(
            engine,
            stage_connection,
            stage_table=stage_table,
            target_table=table_name,
            identity_columns=identities,
            lock_name=(
                STOCK_MINUTE_FREEZE_LOCK_NAME
                if table_name == "sm_stock_minute"
                else STOCK_KLINE_FREEZE_LOCK_NAME
                if table_name == "sm_stock_kline"
                else CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME
                if table_name == "sm_stock_capital_flow_daily"
                else f"probiga:exact:{table_name}"[:64]
            ),
            receipt_engine=receipt_engine,
        )
    finally:
        stage_connection.close()
    logger.info(
        "%s: atomically merged %d exact rows in %s..%s for %d codes "
        "(coverage=%.2f%%); missing dates/timestamps were preserved",
        label,
        written,
        start_value,
        end_value,
        len(scope_codes),
        coverage * 100,
    )
    return written


def _temporary_stage_name(prefix: str) -> str:
    safe_prefix = re.sub(r"[^a-z0-9_]", "_", str(prefix).strip().lower())[:30]
    return f"{safe_prefix}_{os.getpid()}_{time.time_ns()}"[:63]


def _create_temporary_stage(
    engine: Engine,
    *,
    target_table: str,
    prefix: str,
) -> tuple[Connection, str]:
    """Create a session-local LIKE stage that disappears when closed."""

    target = quote_identifier(target_table)
    stage_table = _temporary_stage_name(prefix)
    stage = quote_identifier(stage_table)
    connection = engine.connect()
    try:
        connection.execute(text(f"CREATE TEMPORARY TABLE {stage} LIKE {target}"))
        connection.commit()
        return connection, stage_table
    except BaseException:
        connection.close()
        raise


def _append_temporary_stage(
    connection: Connection,
    stage_table: str,
    frame: pd.DataFrame,
    *,
    chunksize: int = 1000,
) -> int:
    if frame is None or frame.empty:
        return 0
    with connection.begin():
        return write_frame(
            _clean_df(_with_etl(frame)),
            stage_table,
            connection,
            if_exists="append",
            index=False,
            chunksize=max(100, int(chunksize)),
            method="multi",
        )


def _publish_temporary_stage(
    engine: Engine,
    connection: Connection,
    *,
    stage_table: str,
    target_table: str,
    where_sql: str,
    params: dict[str, Any],
    lock_name: str,
    receipt_engine: Engine | None = None,
) -> int:
    """Publish a complete stage with DELETE/INSERT in one target transaction."""

    target = quote_identifier(target_table)
    stage = quote_identifier(stage_table)
    staged_rows = int(
        connection.execute(text(f"SELECT COUNT(*) FROM {stage}")).scalar() or 0
    )
    column_rows = connection.execute(
        text(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
            "AND EXTRA NOT LIKE '%GENERATED%' AND COLUMN_NAME <> 'id' "
            "ORDER BY ORDINAL_POSITION"
        ),
        {"table_name": target_table},
    ).fetchall()
    revoke_window: tuple[datetime, datetime] | None = None
    if target_table == "sm_stock_minute":
        if receipt_engine is None:
            raise RuntimeError(
                "sm_stock_minute publish requires the authority receipt engine"
            )
        window = connection.execute(
            text(f"SELECT MIN(trade_date), MAX(trade_date) FROM {stage}")
        ).one()
        if window[0] is None or window[1] is None:
            raise RuntimeError(
                "sm_stock_minute stage has no receipt revocation window"
            )
        revoke_window = (
            pd.Timestamp(window[0]).normalize().to_pydatetime(),
            (
                pd.Timestamp(window[1]).normalize()
                + pd.Timedelta(days=1)
                - pd.Timedelta(microseconds=1)
            ).to_pydatetime(),
        )
    connection.commit()
    if staged_rows <= 0:
        raise RuntimeError(
            f"temporary replacement stage for {target_table} is empty; "
            "preserving previous data"
        )
    columns = [quote_identifier(str(row[0])) for row in column_rows]
    if not columns:
        raise RuntimeError(f"{target_table} has no publishable columns")
    column_list = ", ".join(columns)
    with mysql_named_lock(
        engine,
        lock_name,
        timeout_seconds=max(0, int(os.environ.get("SM_REFRESH_LOCK_TIMEOUT", "0"))),
        connection=connection,
    ):
        connection.commit()
        if revoke_window is not None:
            supersede_overlapping_qmt_minute_forward_receipts(
                receipt_engine,
                first_trade_time=revoke_window[0],
                last_trade_time=revoke_window[1],
                reason=f"{target_table} staged partition publish",
            )
        with connection.begin():
            connection.execute(
                text(f"DELETE FROM {target} WHERE {where_sql}"),
                params,
            )
            result = connection.execute(
                text(
                    f"INSERT INTO {target} ({column_list}) "
                    f"SELECT {column_list} FROM {stage}"
                )
            )
            inserted = (
                int(result.rowcount)
                if result.rowcount is not None and result.rowcount >= 0
                else staged_rows
            )
            if inserted != staged_rows:
                raise RuntimeError(
                    f"{target_table} staged publish row mismatch: "
                    f"expected={staged_rows} actual={inserted}"
                )
    return staged_rows


def _publish_temporary_stage_exact_keys(
    engine: Engine,
    connection: Connection,
    *,
    stage_table: str,
    target_table: str,
    identity_columns: tuple[str, ...],
    lock_name: str,
    receipt_engine: Engine | None = None,
) -> int:
    """Replace only identities actually present in a complete temporary stage.

    Date-range replacement is unsafe for independently fetched symbols: a
    response can contain the global first/last timestamp while one symbol is
    missing dates or intraday bars in between.  Joining the target to the stage
    on the complete business identity preserves every absent old row, while
    DELETE and INSERT still commit as one transaction.
    """

    target = quote_identifier(target_table)
    stage = quote_identifier(stage_table)
    identities = tuple(str(column) for column in identity_columns)
    if not identities or len(set(identities)) != len(identities):
        raise ValueError("exact stage identity columns are invalid")
    identity_sql = [quote_identifier(column) for column in identities]
    staged_rows = int(
        connection.execute(text(f"SELECT COUNT(*) FROM {stage}")).scalar() or 0
    )
    if staged_rows <= 0:
        connection.rollback()
        raise RuntimeError(
            f"temporary exact stage for {target_table} is empty; "
            "preserving previous data"
        )
    null_predicate = " OR ".join(
        f"{column} IS NULL" for column in identity_sql
    )
    null_count = int(
        connection.execute(
            text(f"SELECT COUNT(*) FROM {stage} WHERE {null_predicate}")
        ).scalar()
        or 0
    )
    grouped = ", ".join(identity_sql)
    duplicate = connection.execute(
        text(
            f"SELECT 1 FROM {stage} GROUP BY {grouped} "
            "HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    column_rows = connection.execute(
        text(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
            "AND EXTRA NOT LIKE '%GENERATED%' AND COLUMN_NAME <> 'id' "
            "ORDER BY ORDINAL_POSITION"
        ),
        {"table_name": target_table},
    ).fetchall()
    revoke_window: tuple[datetime, datetime] | None = None
    if target_table == "sm_stock_minute":
        if receipt_engine is None:
            raise RuntimeError(
                "sm_stock_minute exact publish requires the authority receipt engine"
            )
        window = connection.execute(
            text(f"SELECT MIN(trade_time), MAX(trade_time) FROM {stage}")
        ).one()
        if window[0] is None or window[1] is None:
            raise RuntimeError(
                "sm_stock_minute exact stage has no receipt revocation window"
            )
        revoke_window = (
            pd.Timestamp(window[0]).to_pydatetime(),
            pd.Timestamp(window[1]).to_pydatetime(),
        )
    connection.commit()
    if null_count or duplicate is not None:
        raise RuntimeError(
            f"temporary exact stage for {target_table} has null/duplicate "
            "business identities; preserving previous data"
        )
    columns = [quote_identifier(str(row[0])) for row in column_rows]
    if not columns or any(column not in columns for column in identity_sql):
        raise RuntimeError(
            f"{target_table} does not expose the staged business identity"
        )
    column_list = ", ".join(columns)
    join_sql = " AND ".join(
        f"target.{column}=staged.{column}" for column in identity_sql
    )
    with mysql_named_lock(
        engine,
        lock_name,
        timeout_seconds=max(0, int(os.environ.get("SM_REFRESH_LOCK_TIMEOUT", "0"))),
        connection=connection,
    ):
        connection.commit()
        if revoke_window is not None:
            supersede_overlapping_qmt_minute_forward_receipts(
                receipt_engine,
                first_trade_time=revoke_window[0],
                last_trade_time=revoke_window[1],
                reason=f"{target_table} exact-key publish",
            )
        with connection.begin():
            connection.execute(
                text(
                    f"DELETE target FROM {target} AS target "
                    f"INNER JOIN {stage} AS staged ON {join_sql}"
                )
            )
            result = connection.execute(
                text(
                    f"INSERT INTO {target} ({column_list}) "
                    f"SELECT {column_list} FROM {stage}"
                )
            )
            inserted = (
                int(result.rowcount)
                if result.rowcount is not None and result.rowcount >= 0
                else staged_rows
            )
            if inserted != staged_rows:
                raise RuntimeError(
                    f"{target_table} exact publish row mismatch: "
                    f"expected={staged_rows} actual={inserted}"
                )
    return staged_rows


def _load_stock_short_name_map(engine: Engine) -> dict[str, str]:
    """stock_code(6位) -> 简称；来自 si_all_code（需先跑 sync_stock_info）。"""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT stock_code, short_name FROM si_all_code")).fetchall()
        out: dict[str, str] = {}
        for r in rows:
            code = str(r[0]).strip().zfill(6)
            nm = r[1]
            out[code] = (str(nm).strip() if nm is not None else "")[:128]
        return out
    except Exception as e:  # pylint: disable=broad-except
        logger.warning("读取 si_all_code 失败，K 线 short_name 将留空：%s", e)
        return {}


def _current_min_coverage() -> float:
    """Return the minimum safe coverage for any realtime quote source."""
    raw = os.environ.get(
        "CURRENT_MIN_COVERAGE",
        os.environ.get("QMT_CURRENT_MIN_COVERAGE", "0.98"),
    )
    try:
        return min(1.0, max(0.0, float(raw)))
    except (TypeError, ValueError):
        return 0.98


def _fetch_sina_stock_current(
    stock_codes: list[str],
    short_name_map: dict[str, str],
) -> pd.DataFrame:
    """Fetch a validated full-market fallback when QMT/adata is unavailable."""
    from biz.stock_market.realtime_quotes import fetch_list_market_current

    batch_size = max(50, int(os.environ.get("SINA_CURRENT_BATCH_SIZE", "500")))
    parts: list[pd.DataFrame] = []
    dropped = 0
    snapshot_at = _now()
    for batch in _chunked(stock_codes, batch_size):
        frame = fetch_list_market_current(batch)
        if frame is None or frame.empty:
            continue
        frame = frame.copy()
        frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
        frame["short_name"] = frame.get("short_name", "").fillna("")
        frame["short_name"] = frame["short_name"].where(
            frame["short_name"].astype(str).str.strip().ne(""),
            frame["stock_code"].map(short_name_map).fillna(""),
        )
        for column in ("price", "volume", "amount"):
            frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
        valid = (
            frame["price"].notna()
            & (frame["price"] > 0)
            & frame["volume"].fillna(0).ge(0)
            & frame["amount"].fillna(0).ge(0)
        )
        dropped += int((~valid).sum())
        frame = frame.loc[valid].copy()
        if frame.empty:
            continue
        frame["snapshot_at"] = snapshot_at
        parts.append(
            frame.reindex(
                columns=[
                    "stock_code",
                    "short_name",
                    "price",
                    "change",
                    "change_pct",
                    "volume",
                    "amount",
                    "snapshot_at",
                ]
            )
        )
    if not parts:
        raise RuntimeError("Sina realtime fallback returned no valid rows")
    result = pd.concat(parts, ignore_index=True)
    if dropped:
        logger.warning("Sina realtime fallback dropped %d invalid rows", dropped)
    return result


def _validate_stock_current_frame(
    df: pd.DataFrame,
    stock_codes: list[str],
    *,
    source: str,
) -> tuple[int, float]:
    """Validate a quote batch before it can replace the current snapshot."""
    if df is None or df.empty or "stock_code" not in df.columns:
        raise RuntimeError(f"{source} realtime source returned no valid rows")

    expected = {str(code).strip().zfill(6) for code in stock_codes if str(code).strip()}
    received_series = df["stock_code"].astype(str).str.strip().str.zfill(6)
    received = set(received_series.tolist())
    if not expected:
        raise RuntimeError("unable to determine stock universe for realtime coverage validation")
    if received_series.duplicated().any():
        raise RuntimeError(f"{source} realtime source returned duplicate stock codes")

    outside_pool = received - expected
    if outside_pool:
        raise RuntimeError(
            f"{source} realtime source returned {len(outside_pool)} codes outside the canonical stock pool"
        )

    numeric = df.copy()
    for column in ("price", "volume", "amount"):
        if column not in numeric.columns:
            numeric[column] = None
        numeric[column] = pd.to_numeric(numeric[column], errors="coerce")
    bad_price = int((numeric["price"].isna() | (numeric["price"] <= 0)).sum())
    bad_volume_amount = int(
        (numeric["volume"] < 0).fillna(False).sum()
        + (numeric["amount"] < 0).fillna(False).sum()
    )
    if bad_price or bad_volume_amount:
        raise RuntimeError(
            f"{source} realtime source returned invalid values: bad_price={bad_price}, "
            f"bad_volume_amount={bad_volume_amount}"
        )

    coverage = len(received) / max(len(expected), 1)
    minimum = _current_min_coverage()
    if coverage < minimum:
        raise RuntimeError(
            f"{source} realtime coverage below threshold: {len(received)}/{len(expected)} "
            f"({coverage:.1%}) < {minimum:.1%}"
        )
    return len(received), coverage


def _sleep_stock_kline_akshare() -> None:
    """AkShare 新浪日线：可单独限流；未配置则与全局 SM_REQUEST_SLEEP 一致。"""
    raw = os.environ.get("SM_STOCK_KLINE_AKSHARE_SLEEP", "").strip()
    if raw:
        time.sleep(float(raw))
    else:
        _sleep()


def _normalize_cli_date(raw: str) -> str:
    """YYYY-MM-DD 或 YYYYMMDD -> YYYY-MM-DD（供 K 线起止）。"""
    s = raw.strip().replace("-", "")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return raw.strip()


def _kline_load_progress(path: str) -> set[str]:
    if not path.strip() or not os.path.isfile(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def _kline_append_progress(path: str, code: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(code + "\n")


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.replace({np.nan: None, pd.NaT: None})


def _with_etl(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out["etl_sync_at"] = _now()
    return out


def _records_without_nan(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def _replace_qmt_minute_window(
    engine: Engine,
    df: pd.DataFrame,
    *,
    receipt_engine: Engine | None = None,
) -> int:
    """Atomically replace only the fetched time window for a small stock batch."""
    if df is None or df.empty:
        return 0
    stamped = _with_etl(df).copy()
    stamped["trade_time"] = pd.to_datetime(stamped["trade_time"], errors="coerce")
    stamped["trade_date"] = pd.to_datetime(stamped["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    stamped = stamped.dropna(subset=["stock_code", "trade_time", "trade_date", "price"])
    if stamped.empty:
        return 0
    if receipt_engine is None:
        raise RuntimeError(
            "sm_stock_minute window publish requires the authority receipt engine"
        )
    stamped["stock_code"] = stamped["stock_code"].astype(str).str.zfill(6)
    stamped = stamped.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")
    codes = sorted(stamped["stock_code"].unique().tolist())
    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params: dict[str, Any] = {f"code_{idx}": code for idx, code in enumerate(codes)}
    params["start_time"] = stamped["trade_time"].min().to_pydatetime()
    params["end_time_exclusive"] = (
        stamped["trade_time"].max() + pd.Timedelta(seconds=1)
    ).to_pydatetime()
    conn = engine.connect()
    try:
        with mysql_named_lock(
            engine,
            STOCK_MINUTE_FREEZE_LOCK_NAME,
            timeout_seconds=0,
            connection=conn,
        ):
            conn.commit()
            supersede_overlapping_qmt_minute_forward_receipts(
                receipt_engine,
                first_trade_time=params["start_time"],
                last_trade_time=params["end_time_exclusive"],
                reason="legacy QMT minute window publish",
            )
            with conn.begin():
                conn.execute(
                    text(
                        "DELETE FROM `sm_stock_minute` "
                        f"WHERE stock_code IN ({placeholders}) "
                        "AND trade_time >= :start_time "
                        "AND trade_time < :end_time_exclusive"
                    ),
                    params,
                )
                write_frame(
                    _clean_df(stamped),
                    "sm_stock_minute",
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=max(
                        500,
                        int(os.environ.get("QMT_MINUTE_DB_CHUNK_SIZE", "1000")),
                    ),
                    method="multi",
                )
    finally:
        conn.close()
    return int(len(stamped))


def _create_qmt_minute_stage(engine: Engine, stage_table: str) -> Connection:
    """Open a session-local stage; no persistent schema object is created."""

    connection = engine.connect()
    try:
        connection.execute(
            text(
                f"CREATE TEMPORARY TABLE {quote_identifier(stage_table)} "
                "LIKE `sm_stock_minute`"
            )
        )
        connection.commit()
        return connection
    except BaseException:
        connection.close()
        raise


def _append_qmt_minute_stage(
    connection: Connection,
    stage_table: str,
    df: pd.DataFrame,
) -> int:
    if df is None or df.empty:
        return 0
    stamped = _with_etl(df).copy()
    stage = quote_identifier(stage_table)
    target_columns = [
        str(row[0])
        for row in connection.execute(
            text(f"SHOW COLUMNS FROM {stage}")
        ).fetchall()
        if row and str(row[0]) != "id"
    ]
    connection.commit()
    required_columns = {"stock_code", "trade_time", "trade_date", "price"}
    if not required_columns.issubset(stamped.columns):
        raise RuntimeError("QMT minute stage frame lacks canonical columns")
    publish_columns = [
        column for column in target_columns if column in stamped.columns
    ]
    if not required_columns.issubset(publish_columns):
        raise RuntimeError("QMT minute target lacks canonical columns")
    # BigQMT also carries local-only evidence such as native ``pre_close``.
    # Publish every column supported by the canonical target and leave the
    # additional evidence in qmt_local_stock_minute instead of issuing an
    # unknown-column insert.
    stamped = stamped.reindex(columns=publish_columns)
    with connection.begin():
        write_frame(
            _clean_df(stamped),
            stage_table,
            connection,
            if_exists="append",
            index=False,
            chunksize=max(500, int(os.environ.get("QMT_MINUTE_DB_CHUNK_SIZE", "1000"))),
            method="multi",
        )
    return int(len(stamped))


def _commit_qmt_minute_stage(
    engine: Engine,
    stage_connection: Connection,
    stage_table: str,
    *,
    trade_date: str,
    replacement_codes: list[str],
) -> int:
    """Publish a validated minute stage in small per-code transactions.

    A single DELETE/INSERT transaction for an entire trading day can retain
    hundreds of thousands of InnoDB row locks and fail with MySQL error 1206.
    The stage has already passed coverage validation, so publish it in small
    stock-code groups while holding the existing application-level named lock.
    Each group's old and new rows remain atomic and the business table/schema
    stays unchanged.
    """
    target = quote_identifier("sm_stock_minute")
    stage = quote_identifier(stage_table)
    batch_size = max(
        1,
        min(100, int(os.environ.get("QMT_MINUTE_PUBLISH_CODE_BATCH_SIZE", "10"))),
    )
    lock_owner = stage_connection.execute(
        text(
            "SELECT IS_USED_LOCK(:lock_name), CONNECTION_ID()"
        ),
        {"lock_name": STOCK_MINUTE_FREEZE_LOCK_NAME},
    ).one()
    if int(lock_owner[0] or 0) != int(lock_owner[1] or -1):
        stage_connection.rollback()
        raise RuntimeError("QMT minute publish requires the owned generation lock")
    staged_rows = int(
        stage_connection.execute(text(f"SELECT COUNT(*) FROM {stage}")).scalar()
        or 0
    )
    staged_codes = {
        str(row[0]).zfill(6)
        for row in stage_connection.execute(
            text(f"SELECT DISTINCT stock_code FROM {stage} ORDER BY stock_code")
        ).fetchall()
        if row[0] is not None
    }
    codes = sorted({str(code).strip().zfill(6) for code in replacement_codes})
    if not codes or not staged_codes.issubset(codes):
        stage_connection.rollback()
        raise RuntimeError("QMT minute replacement universe differs from stage")
    column_rows = stage_connection.execute(
        text(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sm_stock_minute' "
            "AND COLUMN_NAME <> 'id' ORDER BY ORDINAL_POSITION"
        ),
    ).fetchall()
    stage_connection.commit()
    columns = [quote_identifier(str(row[0])) for row in column_rows]
    if not columns:
        raise RuntimeError("sm_stock_minute has no publishable columns")
    column_list = ", ".join(columns)
    day_start = datetime.fromisoformat(f"{trade_date} 00:00:00")
    day_end = day_start + timedelta(days=1)
    inserted_rows = 0
    total_batches = (len(codes) + batch_size - 1) // batch_size
    for offset in range(0, len(codes), batch_size):
        batch = codes[offset : offset + batch_size]
        placeholders = ", ".join(f":code_{idx}" for idx in range(len(batch)))
        params: dict[str, Any] = {
            f"code_{idx}": code for idx, code in enumerate(batch)
        }
        params.update(
            {
                "day_start": day_start,
                "day_end": day_end,
            }
        )
        with stage_connection.begin():
            stage_connection.execute(
                text(
                    f"DELETE FROM {target} WHERE stock_code IN ({placeholders}) "
                    "AND trade_time >= :day_start "
                    "AND trade_time < :day_end"
                ),
                params,
            )
            result = stage_connection.execute(
                text(
                    f"INSERT INTO {target} ({column_list}) "
                    f"SELECT {column_list} FROM {stage} "
                    f"WHERE stock_code IN ({placeholders})"
                ),
                params,
            )
            if result.rowcount is not None and result.rowcount >= 0:
                inserted_rows += int(result.rowcount)
        batch_number = offset // batch_size + 1
        if batch_number == total_batches or batch_number % 25 == 0:
            logger.info(
                "QMT minute publish batch %s/%s: inserted=%s",
                batch_number,
                total_batches,
                inserted_rows,
            )
    return inserted_rows or staged_rows


def _drop_qmt_minute_stage(connection: Connection, _stage_table: str) -> None:
    """Closing the owning session destroys its temporary stage."""

    connection.close()


def _record_qmt_minute_receipt(
    engine: Engine,
    *,
    trade_date: str,
    first_trade_time: datetime,
    last_trade_time: datetime,
    expected_count: int,
    observed_count: int,
    row_count: int,
    source_provider: str,
    capture_mode: str,
    forward_eligible: bool,
    evidence: dict[str, Any],
    quality_status: str = "PASS",
) -> None:
    """Attest which provider produced a published minute window.

    ``sm_stock_minute`` predates row-level provenance columns.  The receipt
    makes the source and coverage independently verifiable before an
    intraday paper order can trust that window.
    """
    status = str(quality_status or "").strip().upper()
    if status not in {"PUBLISHING", "PASS", "PARTIAL", "FAILED"}:
        raise ValueError("QMT minute receipt quality status is invalid")
    if status != "PASS" and forward_eligible:
        raise ValueError("unfinished QMT minute publication cannot be forward eligible")
    if status == "PASS" and not _qmt_minute_evidence_proves_exact_grid(
        evidence,
        expected_count=expected_count,
        row_count=row_count,
    ):
        raise ValueError(
            "PASS QMT minute receipt requires an exact per-code time-grid manifest"
        )
    if forward_eligible and (
        expected_count <= 0
        or observed_count != expected_count
        or row_count <= 0
    ):
        raise ValueError(
            "forward-eligible QMT minute receipt requires exact requested coverage "
            "and published rows"
        )
    if forward_eligible and not _qmt_minute_evidence_proves_full_coverage(
        evidence,
        expected_count=expected_count,
        observed_count=observed_count,
    ):
        raise ValueError(
            "forward-eligible QMT minute receipt requires frozen universe evidence"
        )
    payload = {
        "trade_date": trade_date,
        "first_trade_time": first_trade_time.isoformat(),
        "last_trade_time": last_trade_time.isoformat(),
        "expected_count": int(expected_count),
        "observed_count": int(observed_count),
        "row_count": int(row_count),
        "source_provider": source_provider,
        "capture_mode": capture_mode,
        "forward_eligible": bool(forward_eligible),
        "quality_status": status,
        "evidence": evidence,
    }
    receipt_id = hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()[:32]
    coverage = observed_count / max(expected_count, 1)
    validate_required_table_surface(
        engine,
        {"st_qmt_minute_sync_receipt_v2"},
        context="QMT minute receipt writer",
        required_columns={
            "st_qmt_minute_sync_receipt_v2": {
                "receipt_id",
                "trade_date",
                "first_trade_time",
                "last_trade_time",
                "expected_count",
                "observed_count",
                "coverage",
                "row_count",
                "source_provider",
                "capture_mode",
                "forward_eligible",
                "quality_status",
                "evidence_json",
                "created_at",
            },
        },
    )
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE st_qmt_minute_sync_receipt_v2
                SET forward_eligible=0,
                    quality_status='SUPERSEDED'
                WHERE trade_date=:trade_date
                  AND first_trade_time<=:last_trade_time
                  AND last_trade_time>=:first_trade_time
                  AND receipt_id<>:receipt_id
                """
            ),
            {
                "trade_date": trade_date,
                "first_trade_time": first_trade_time,
                "last_trade_time": last_trade_time,
                "receipt_id": receipt_id,
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO st_qmt_minute_sync_receipt_v2
                (receipt_id, trade_date, first_trade_time,
                 last_trade_time, expected_count, observed_count,
                 coverage, row_count, source_provider, capture_mode,
                 forward_eligible, quality_status, evidence_json, created_at)
                VALUES
                (:receipt_id, :trade_date, :first_trade_time,
                 :last_trade_time, :expected_count, :observed_count,
                 :coverage, :row_count, :source_provider, :capture_mode,
                 :forward_eligible, :quality_status, :evidence_json, :created_at)
                ON DUPLICATE KEY UPDATE
                    receipt_id=VALUES(receipt_id),
                    expected_count=VALUES(expected_count),
                    observed_count=VALUES(observed_count),
                    coverage=VALUES(coverage),
                    row_count=VALUES(row_count),
                    forward_eligible=VALUES(forward_eligible),
                    quality_status=VALUES(quality_status),
                    evidence_json=VALUES(evidence_json),
                    created_at=VALUES(created_at)
                """
            ),
            {
                "receipt_id": receipt_id,
                "trade_date": trade_date,
                "first_trade_time": first_trade_time,
                "last_trade_time": last_trade_time,
                "expected_count": expected_count,
                "observed_count": observed_count,
                "coverage": coverage,
                "row_count": row_count,
                "source_provider": source_provider,
                "capture_mode": capture_mode,
                "forward_eligible": int(bool(forward_eligible)),
                "quality_status": status,
                "evidence_json": json.dumps(
                    evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "created_at": datetime.now(),
            },
        )


def _classify_qmt_minute_capture(
    *,
    trade_date: str,
    last_trade_time: datetime,
    captured_at: datetime | None = None,
) -> tuple[str, bool, float]:
    """Separate real forward capture from after-close/history backfill."""

    captured = (captured_at or datetime.now()).replace(microsecond=0)
    last_bar = last_trade_time.replace(microsecond=0)
    lag_seconds = (captured - last_bar).total_seconds()
    hhmm = captured.hour * 100 + captured.minute
    live_forward = bool(
        trade_date == captured.date().isoformat()
        and captured.weekday() < 5
        and ((930 <= hhmm <= 1132) or (1300 <= hhmm <= 1502))
        and -5 <= lag_seconds <= 120
    )
    return (
        "LIVE_FORWARD" if live_forward else "AFTER_CLOSE_BACKFILL",
        live_forward,
        lag_seconds,
    )


def _qmt_minute_code_set_sha256(codes: list[str]) -> str:
    return hashlib.sha256(
        json.dumps(
            codes,
            ensure_ascii=True,
            sort_keys=False,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()


def _qmt_minute_universe_evidence(
    requested_codes: list[str],
    responded_codes: set[str] | list[str],
    published_codes: set[str] | list[str],
) -> dict[str, Any]:
    """Freeze requested, validated-response and physically published sets."""

    requested = sorted(
        {str(code).strip().zfill(6) for code in requested_codes if str(code).strip()}
    )
    responded = sorted(
        {str(code).strip().zfill(6) for code in responded_codes if str(code).strip()}
    )
    published = sorted(
        {str(code).strip().zfill(6) for code in published_codes if str(code).strip()}
    )

    return {
        "stock_code_set_schema": "probiga.sorted-stock-code-set.v1",
        "requested_stock_code_count": len(requested),
        "requested_stock_codes_sha256": _qmt_minute_code_set_sha256(requested),
        "responded_stock_code_count": len(responded),
        "responded_stock_codes_sha256": _qmt_minute_code_set_sha256(responded),
        "responded_stock_codes": responded,
        "published_stock_code_count": len(published),
        "published_stock_codes_sha256": _qmt_minute_code_set_sha256(published),
        "published_stock_codes": published,
        "full_requested_response_coverage": responded == requested,
    }


def _qmt_minute_evidence_proves_full_coverage(
    evidence: dict[str, Any],
    *,
    expected_count: int,
    observed_count: int,
) -> bool:
    if type(evidence) is not dict:
        return False
    responded_codes = evidence.get("responded_stock_codes")
    published_codes = evidence.get("published_stock_codes")
    if (
        evidence.get("stock_code_set_schema")
        != "probiga.sorted-stock-code-set.v1"
        or evidence.get("full_requested_response_coverage") is not True
        or type(responded_codes) is not list
        or any(
            type(code) is not str or len(code) != 6 or not code.isdigit()
            for code in responded_codes
        )
        or responded_codes != sorted(set(responded_codes))
        or type(published_codes) is not list
        or not published_codes
        or any(
            type(code) is not str or len(code) != 6 or not code.isdigit()
            for code in published_codes
        )
        or published_codes != sorted(set(published_codes))
        or not set(published_codes).issubset(responded_codes)
        or type(evidence.get("requested_stock_code_count")) is not int
        or type(evidence.get("responded_stock_code_count")) is not int
        or type(evidence.get("published_stock_code_count")) is not int
        or evidence["requested_stock_code_count"] != expected_count
        or evidence["responded_stock_code_count"] != observed_count
        or evidence["published_stock_code_count"] != len(published_codes)
        or len(responded_codes) != observed_count
    ):
        return False
    responded_hash = _qmt_minute_code_set_sha256(responded_codes)
    published_hash = _qmt_minute_code_set_sha256(published_codes)
    return bool(
        evidence.get("requested_stock_codes_sha256") == responded_hash
        and evidence.get("responded_stock_codes_sha256") == responded_hash
        and evidence.get("published_stock_codes_sha256") == published_hash
        and _qmt_minute_evidence_proves_exact_grid(
            evidence,
            expected_count=expected_count,
        )
    )


def _qmt_minute_evidence_proves_exact_grid(
    evidence: dict[str, Any],
    *,
    expected_count: int,
    row_count: int | None = None,
) -> bool:
    """Validate the hash-bound per-code grid manifest in a minute receipt."""

    manifest = evidence.get("minute_coverage_manifest")
    if not isinstance(manifest, dict):
        return False
    supplied_hash = str(manifest.get("manifest_hash") or "").lower()
    supplied_json = str(manifest.get("manifest_json") or "")
    core = {
        key: value
        for key, value in manifest.items()
        if key not in {"manifest_hash", "manifest_json"}
    }
    canonical_json = json.dumps(
        core,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    try:
        grid = minute_time_grid(str(manifest.get("grid_profile") or ""))
    except Exception:
        return False
    expected_set_hash = str(
        evidence.get("requested_stock_codes_sha256") or ""
    )
    expected_traded = int(manifest.get("expected_traded_count") or 0)
    actual_traded = int(manifest.get("actual_traded_count") or 0)
    no_trade = int(manifest.get("no_trade_count") or 0)
    bar_count = int(manifest.get("bar_count") or 0)
    return bool(
        manifest.get("schema") == "probiga.qmt-history-coverage.v1"
        and manifest.get("dataset") == "stock_minute"
        and manifest.get("period") == "1m"
        and manifest.get("status") == "EXACT"
        and manifest.get("strategy_eligible") is True
        and int(manifest.get("expected_entity_count") or 0) == expected_count
        and int(manifest.get("entity_count") or 0) == expected_count
        and expected_traded == actual_traded
        and expected_traded + no_trade == expected_count
        and bar_count == actual_traded * len(grid)
        and (row_count is None or bar_count == int(row_count))
        and manifest.get("expected_entity_set_hash") == expected_set_hash
        and manifest.get("minute_grid_hash")
        == qmt_coverage_digest(list(grid))
        and evidence.get("minute_grid_profile") == manifest.get("grid_profile")
        and evidence.get("minute_grid_hash") == manifest.get("minute_grid_hash")
        and int(evidence.get("minute_grid_bar_count") or 0) == len(grid)
        and supplied_json == canonical_json
        and len(supplied_hash) == 64
        and qmt_coverage_digest(core) == supplied_hash
    )


def _qmt_minute_receipt_disposition(
    requested_codes: list[str],
    responded_codes: set[str] | list[str],
    published_codes: set[str] | list[str],
    *,
    live_forward_capture: bool,
) -> tuple[dict[str, Any], str, bool]:
    """Return evidence/status/authority; partial coverage always fails closed."""

    evidence = _qmt_minute_universe_evidence(
        requested_codes,
        responded_codes,
        published_codes,
    )
    full_coverage = evidence["full_requested_response_coverage"] is True
    has_published_rows = bool(evidence["published_stock_code_count"])
    return (
        evidence,
        "PASS" if full_coverage else "PARTIAL",
        bool(live_forward_capture and full_coverage and has_published_rows),
    )


def _qmt_minute_validated_code_sets(
    frame: pd.DataFrame,
) -> tuple[set[str], set[str]]:
    """Separate explicit valid responses from rows eligible for publication."""

    if frame is None or frame.empty:
        return set(), set()
    responded = set(frame["stock_code"].astype(str))
    published = set(
        frame.groupby("stock_code")[["volume", "amount"]]
        .sum(min_count=1)
        .query("volume > 0 or amount > 0")
        .index.astype(str)
        .tolist()
    )
    return responded, published


def _is_complete_stock_universe(engine: Engine, stock_codes: list[str]) -> bool:
    """Return whether this job was given the complete canonical stock pool."""
    requested = {str(code).zfill(6) for code in stock_codes if str(code).strip()}
    if not requested:
        return False
    canonical = read_frame(text("SELECT DISTINCT stock_code FROM `si_all_code`"), engine)
    if canonical is None or canonical.empty or "stock_code" not in canonical.columns:
        return False
    expected = {
        str(code).zfill(6)
        for code in canonical["stock_code"].dropna().astype(str).tolist()
        if str(code).strip()
    }
    return bool(expected) and requested == expected


def _is_complete_index_universe(engine: Engine, index_codes: list[str]) -> bool:
    """Return whether this job was given the complete canonical index pool."""
    requested = {str(code).zfill(6) for code in index_codes if str(code).strip()}
    if not requested:
        return False
    canonical = read_frame(text("SELECT DISTINCT index_code FROM `si_all_index_code`"), engine)
    if canonical is None or canonical.empty or "index_code" not in canonical.columns:
        return False
    expected = {
        str(code).zfill(6)
        for code in canonical["index_code"].dropna().astype(str).tolist()
        if str(code).strip()
    }
    return bool(expected) and requested == expected


def _prune_snapshot_codes(
    engine: Engine,
    *,
    table_name: str,
    code_column: str,
    date_column: str,
    target_date: str,
    keep_codes: set[str],
) -> int:
    del engine, table_name, code_column, date_column, target_date, keep_codes
    raise RuntimeError(
        "post-publish snapshot pruning is disabled; replacement scope must be "
        "decided before the atomic publish"
    )


def _prune_snapshot_time_bounds(
    engine: Engine,
    *,
    table_name: str,
    date_column: str,
    time_column: str,
    target_date: str,
    first_time: datetime,
    last_time: datetime,
) -> int:
    del engine, table_name, date_column, time_column, target_date, first_time, last_time
    raise RuntimeError(
        "post-publish time pruning is disabled; replacement scope must be "
        "decided before the atomic publish"
    )


def _replace_qmt_index_window(
    engine: Engine,
    df: pd.DataFrame,
    *,
    table_name: str,
    time_column: str,
    replace_date: str | None = None,
) -> int:
    """Atomically replace a validated index batch without clearing other data."""
    if df is None or df.empty:
        return 0
    stamped = _with_etl(df).copy()
    stamped[time_column] = pd.to_datetime(stamped[time_column], errors="coerce")
    stamped["index_code"] = stamped["index_code"].astype(str).str.zfill(6)
    stamped = stamped.dropna(subset=["index_code", time_column])
    if stamped.empty:
        return 0
    key_columns = ["index_code", time_column]
    if table_name == "sm_index_kline":
        stamped["trade_date"] = pd.to_datetime(stamped["trade_date"], errors="coerce").dt.date
        key_columns.append("k_type")
    stamped = stamped.drop_duplicates(subset=key_columns, keep="last")
    codes = sorted(stamped["index_code"].unique().tolist())
    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params: dict[str, Any] = {f"code_{idx}": code for idx, code in enumerate(codes)}
    start_value = stamped[time_column].min()
    end_value = stamped[time_column].max()
    params["start_value"] = start_value.to_pydatetime() if hasattr(start_value, "to_pydatetime") else start_value
    params["end_value"] = end_value.to_pydatetime() if hasattr(end_value, "to_pydatetime") else end_value
    if replace_date and table_name == "sm_index_minute":
        params["replace_date"] = replace_date
        predicate = f"index_code IN ({placeholders}) AND trade_date = :replace_date"
    else:
        predicate = (
            f"index_code IN ({placeholders}) AND {quote_identifier(time_column)} "
            "BETWEEN :start_value AND :end_value"
        )
    if table_name == "sm_index_kline":
        params["k_type"] = int(stamped["k_type"].iloc[0])
        predicate += " AND k_type = :k_type"
    return replace_table_rows(
        _clean_df(stamped),
        table_name,
        engine,
        where_sql=predicate,
        params=params,
        chunksize=max(500, int(os.environ.get("QMT_INDEX_DB_CHUNK_SIZE", "1000"))),
        method="multi",
    )


def _to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def retry_remote(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    max_retries = max(1, int(os.environ.get("SM_HTTP_RETRIES", "8")))
    base = float(os.environ.get("SM_HTTP_BACKOFF", "3.0"))
    retriable = (ConnectionError, Timeout, ChunkedEncodingError) + _URLLIB3_RETRIABLE
    last: BaseException | None = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except retriable as e:
            last = e
            if attempt >= max_retries - 1:
                break
            wait = base * (2**attempt) + random.uniform(0.5, 2.0)
            logger.warning("远程请求失败 (%s/%s)，%.1f 秒后重试：%s", attempt + 1, max_retries, wait, e)
            time.sleep(wait)
    assert last is not None
    raise last


def _sm_column_exists(conn: Any, table: str, column: str) -> bool:
    r = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = :t AND column_name = :c
            """
        ),
        {"t": table, "c": column},
    ).scalar()
    return int(r or 0) > 0


def _ensure_sm_stock_kline_short_name(engine: Engine) -> None:
    """Read-only guard for the release-migrated K-line name column."""

    validate_required_table_surface(
        engine,
        {"sm_stock_kline"},
        context="stock kline short-name",
        required_columns={
            "sm_stock_kline": {"stock_code", "short_name", "trade_date"},
        },
    )


def privileged_migrate_sm_stock_kline_short_name(engine: Engine) -> dict[str, Any]:
    """Release-only additive migration for the legacy short-name column."""

    added = False
    with engine.begin() as conn:
        if not _sm_column_exists(conn, "sm_stock_kline", "short_name"):
            conn.execute(
                text(
                    "ALTER TABLE `sm_stock_kline` ADD COLUMN `short_name` "
                    "VARCHAR(128) NOT NULL DEFAULT '' AFTER `stock_code`"
                )
            )
            added = True
        conn.execute(
            text(
                "UPDATE `sm_stock_kline` k "
                "INNER JOIN `si_all_code` s ON k.stock_code=s.stock_code "
                "SET k.short_name=s.short_name "
                "WHERE (k.short_name IS NULL OR k.short_name='') "
                "AND s.short_name IS NOT NULL"
            )
        )
    _ensure_sm_stock_kline_short_name(engine)
    return {"table": "sm_stock_kline", "column_added": added, "validated": True}


def validate_stock_market_runtime_schema(engine: Engine) -> dict[str, Any]:
    return validate_required_table_surface(
        engine,
        RUNTIME_TABLE_ORDER,
        context="stock market collector",
        required_columns={
            "sm_stock_current": {"stock_code", "price", "snapshot_at", "etl_sync_at"},
            "sm_stock_kline": {"stock_code", "short_name", "trade_date", "close"},
            "sm_stock_minute": {"stock_code", "trade_time", "trade_date", "price"},
        },
    )


def run_ddl(engine: Engine) -> None:
    """Legacy entrypoint retained as a read-only prepared-schema guard."""

    validate_stock_market_runtime_schema(engine)


def truncate_all(engine: Engine) -> None:
    del engine
    raise RuntimeError(
        "stock-market global preclear is disabled; use a validated atomic "
        "table/partition replacement"
    )


def truncate_only(engine: Engine, *tables: str) -> None:
    del engine, tables
    raise RuntimeError(
        "stock-market table preclear is disabled; use a validated atomic "
        "table/partition replacement"
    )


def delete_stock_minute_dates(engine: Engine, dates: list[str]) -> None:
    del engine, dates
    raise RuntimeError(
        "stock-minute preclear is disabled; publish a validated temporary stage atomically"
    )


def df_to_table(engine: Engine, df: pd.DataFrame, table: str) -> None:
    del engine, df, table
    raise RuntimeError(
        "unscoped append is disabled for stock-market refreshes; use an atomic "
        "validated table/partition replacement"
    )


def replace_stock_current_snapshot(engine: Engine, df: pd.DataFrame) -> int:
    """Replace only the validated quote codes in one DML transaction."""
    if df is None or df.empty:
        raise ValueError("sm_stock_current snapshot must not be empty")

    cleaned = _clean_df(df)
    if "stock_code" not in cleaned.columns:
        raise ValueError("sm_stock_current snapshot has no stock_code column")
    cleaned = cleaned.copy()
    cleaned["stock_code"] = cleaned["stock_code"].astype(str).str.strip().str.zfill(6)
    cleaned = cleaned.drop_duplicates(subset=["stock_code"], keep="last")
    predicate, params = _code_scope_predicate(
        cleaned["stock_code"].tolist(),
        column="stock_code",
        prefix="current_code",
    )
    lock_timeout = max(0, int(os.environ.get("CURRENT_SNAPSHOT_LOCK_TIMEOUT", "0")))
    with mysql_named_lock(
        engine,
        "probiga:stock_current",
        timeout_seconds=lock_timeout,
    ):
        written = replace_table_rows(
            cleaned,
            "sm_stock_current",
            engine,
            where_sql=predicate,
            params=params,
            chunksize=1000,
            method="multi",
        )
        if written != len(cleaned):
            raise RuntimeError(
                "sm_stock_current replacement row mismatch: "
                f"expected={len(cleaned)} actual={written}"
            )
    logger.info("sm_stock_current: atomically replaced %s rows", len(cleaned))
    return int(len(cleaned))


def upsert_current_frame(engine: Engine, df: pd.DataFrame, table: str, key_columns: list[str]) -> int:
    if df is None or df.empty:
        logger.info("Table %s: no rows to upsert.", table)
        return 0
    cleaned = _clean_df(df)
    batch_id = f"{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    result = safe_upsert_rows(
        engine,
        table_name=table,
        rows=cleaned.to_dict(orient="records"),
        key_columns=key_columns,
        batch_id=batch_id,
    )
    logger.info("Table %s: upserted %s rows.", table, result.accepted_rows)
    return result.accepted_rows


def read_stock_codes(engine: Engine, *, slice_offset: int = 0, slice_limit: int = -1) -> list[str]:
    """
    slice_limit == -1：沿用 SM_MAX_STOCKS（默认 200，0=不限制），不使用 offset。
    slice_limit >= 0：先取 si_all_code 全表再内存切片；limit==0 表示从 offset 到末尾，limit>0 表示本批条数。
    """
    if slice_limit == -1:
        limit = int(os.environ.get("SM_MAX_STOCKS", "200"))
        sql = "SELECT stock_code FROM si_all_code ORDER BY stock_code"
        if limit > 0:
            sql += f" LIMIT {limit}"
        with engine.connect() as conn:
            return [r[0] for r in conn.execute(text(sql)).fetchall()]
    with engine.connect() as conn:
        allc = [r[0] for r in conn.execute(text("SELECT stock_code FROM si_all_code ORDER BY stock_code")).fetchall()]
    start = max(0, int(slice_offset))
    if int(slice_limit) == 0:
        return allc[start:]
    return allc[start : start + int(slice_limit)]


def read_index_codes(engine: Engine) -> list[str]:
    limit = int(os.environ.get("SM_MAX_INDEXES", "50"))
    sql = "SELECT index_code FROM si_all_index_code ORDER BY index_code"
    if limit > 0:
        sql += f" LIMIT {limit}"
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(text(sql)).fetchall()]


def read_concept_ths_codes(engine: Engine) -> list[str]:
    limit = int(os.environ.get("SM_MAX_CONCEPTS", "50"))
    sql = """
    SELECT DISTINCT COALESCE(index_code, concept_code) AS code
    FROM si_concept_code_ths
    WHERE COALESCE(index_code, concept_code) IS NOT NULL
      AND COALESCE(index_code, concept_code) <> ''
      AND COALESCE(index_code, concept_code) LIKE '8%%'
    ORDER BY code
    """
    if limit > 0:
        sql += f" LIMIT {limit}"
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(text(sql)).fetchall()]


def read_concept_east_codes(engine: Engine) -> list[str]:
    limit = int(os.environ.get("SM_MAX_CONCEPTS", "50"))
    if _concept_source("list") == "qmt":
        sql = """
        SELECT DISTINCT concept_code AS code
        FROM si_concept_code_east
        WHERE concept_code IS NOT NULL
          AND concept_code <> ''
        ORDER BY concept_code
        """
    else:
        sql = """
        SELECT DISTINCT concept_code AS code
        FROM si_concept_code_east
        WHERE concept_code IS NOT NULL
          AND concept_code <> ''
          AND concept_code LIKE 'BK%%'
        ORDER BY concept_code
        """
    if limit > 0:
        sql += f" LIMIT {limit}"
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(text(sql)).fetchall()]


def _read_qmt_concept_meta(engine: Engine, concept_codes: list[str]) -> tuple[pd.DataFrame, dict[str, str]]:
    if not concept_codes:
        return pd.DataFrame(columns=["concept_code", "stock_code"]), {}
    placeholders = ", ".join(f":c{i}" for i in range(len(concept_codes)))
    params = {f"c{i}": code for i, code in enumerate(concept_codes)}
    members = read_frame(
        text(
            f"""
            SELECT concept_code, stock_code
            FROM si_concept_constituent_east
            WHERE concept_code IN ({placeholders})
            """
        ),
        engine,
        params=params,
    )
    names = read_frame(
        text(
            f"""
            SELECT concept_code, name
            FROM si_concept_code_east
            WHERE concept_code IN ({placeholders})
            """
        ),
        engine,
        params=params,
    )
    if members.empty:
        return pd.DataFrame(columns=["concept_code", "stock_code"]), {}
    members["stock_code"] = members["stock_code"].astype(str).str.zfill(6)
    name_map = (
        names.drop_duplicates(subset=["concept_code"], keep="first")
        .set_index("concept_code")["name"]
        .fillna("")
        .astype(str)
        .to_dict()
        if not names.empty
        else {}
    )
    return members.drop_duplicates(subset=["concept_code", "stock_code"], keep="first"), name_map


def _concept_snapshot_change_pct(concept_kline: pd.DataFrame, concept_code: str, days: int) -> float | None:
    if concept_kline is None or concept_kline.empty:
        return None
    df = concept_kline[concept_kline["index_code"].astype(str) == str(concept_code)].copy()
    if df.empty:
        return None
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df[df["trade_date"].notna()].sort_values("trade_date")
    if df.empty:
        return None
    if days <= 1:
        value = pd.to_numeric(df.iloc[-1].get("change_pct"), errors="coerce")
        return None if pd.isna(value) else float(value)
    tail = df.tail(days)
    if tail.empty:
        return None
    factor = 1.0
    for value in pd.to_numeric(tail["change_pct"], errors="coerce").fillna(0):
        factor *= 1 + float(value) / 100
    return (factor - 1) * 100


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def step_dividend(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_dividend import StockDividend

    if not stock_codes:
        logger.warning("dividend: empty stock universe; preserving previous data")
        return
    div = StockDividend()

    def _fetch_dividend(code: str) -> pd.DataFrame | None:
        df = retry_remote(div.get_dividend, stock_code=code)
        if df is not None and not df.empty:
            if "report_date" in df.columns:
                df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            if "ex_dividend_date" in df.columns:
                df["ex_dividend_date"] = pd.to_datetime(df["ex_dividend_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            return df[["stock_code", "report_date", "dividend_plan", "ex_dividend_date"]]
        return None

    parts = _concurrent_run(
        stock_codes,
        _fetch_dividend,
        label="分红",
        log_every=500,
        fail_on_error=True,
    )
    if not parts:
        raise RuntimeError("dividend returned no rows; preserving previous data")
    complete = pd.concat(parts, ignore_index=True)
    complete, scope_codes, _coverage = _validated_code_snapshot(
        complete,
        stock_codes,
        code_column="stock_code",
        label="dividend",
        coverage_env="SM_DIVIDEND_MIN_COVERAGE",
        default_coverage=0.20,
    )
    predicate, params = _code_scope_predicate(scope_codes, column="stock_code")
    replace_table_rows(
        _clean_df(_with_etl(complete)),
        "sm_dividend",
        engine,
        where_sql=predicate,
        params=params,
    )


def _step_stock_kline_adata(
    engine: Engine,
    stock_codes: list[str],
    start: str,
    end: str | None,
    k_type: int,
    adjust_type: int,
    *,
    incremental: bool = False,
) -> None:
    from adata.stock.market.stock_market.stock_market import StockMarket

    mk = StockMarket()
    end_value = end or datetime.now().strftime("%Y-%m-%d")
    logger.info(
        "个股K线(adata)：%d 只，区间 %s ~ %s，k_type=%s，adjust_type=%s，"
        "mode=%s staged-atomic",
        len(stock_codes),
        start,
        end_value,
        k_type,
        adjust_type,
        "incremental" if incremental else "refresh",
    )
    name_map = _load_stock_short_name_map(engine)
    kline_cols = [
        "stock_code", "short_name", "trade_time", "trade_date", "k_type", "adjust_type",
        "open", "close", "high", "low", "volume", "amount", "change", "change_pct",
        "turnover_ratio", "pre_close",
    ]

    def _fetch_kline(code: str) -> pd.DataFrame | None:
        df = retry_remote(
            mk.get_market,
            stock_code=code,
            start_date=start,
            end_date=end,
            k_type=k_type,
            adjust_type=adjust_type,
        )
        if df is not None and not df.empty:
            df = _to_numeric(df, ["open", "close", "high", "low", "volume", "amount", "change", "change_pct", "turnover_ratio", "pre_close"])
            df["k_type"] = k_type
            df["adjust_type"] = adjust_type
            df["short_name"] = name_map.get(str(code).strip().zfill(6), "")
            return df[kline_cols]
        return None

    workers = _max_workers()
    fetch_batch_size = max(
        workers,
        int(os.environ.get("ADATA_KLINE_FETCH_CODE_BATCH_SIZE", str(workers * 8))),
    )
    batch: list[pd.DataFrame] = []
    done_count = 0
    total = len(stock_codes)
    failed_codes: list[str] = []
    received_codes: set[str] = set()
    staged_rows = 0
    stage_connection, stage_table = _create_temporary_stage(
        engine,
        target_table="sm_stock_kline",
        prefix="sm_stock_kline_adata_stage",
    )

    def _flush() -> None:
        nonlocal batch, staged_rows
        if batch:
            complete_batch = pd.concat(batch, ignore_index=True)
            complete_batch["stock_code"] = (
                complete_batch["stock_code"].astype(str).str.strip().str.zfill(6)
            )
            complete_batch = complete_batch.drop_duplicates(
                subset=["stock_code", "trade_date", "k_type", "adjust_type"],
                keep="last",
            )
            staged_rows += _append_temporary_stage(
                stage_connection,
                stage_table,
                complete_batch,
                chunksize=1000,
            )
            received_codes.update(complete_batch["stock_code"].unique().tolist())
            logger.info("K线 分批写入临时区 %d 只，累计 %d/%d", len(batch), done_count, total)
            batch = []

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for request_batch in _chunked(stock_codes, fetch_batch_size):
                futures = {
                    pool.submit(_fetch_kline, code): code for code in request_batch
                }
                for future in as_completed(futures):
                    done_count += 1
                    try:
                        df = future.result()
                        if df is not None:
                            batch.append(df)
                    except Exception as exc:  # pylint: disable=broad-except
                        failed_codes.append(futures[future])
                        logger.warning("stock_kline %s 失败：%s", futures[future], exc)
                    if done_count % 500 == 0:
                        logger.info("K线 进度 %d/%d（并发 %d）", done_count, total, workers)
                # Release this bounded future/result set before requesting the
                # next group; multi-year per-symbol frames can be large.
                _flush()
        _flush()
        coverage = len(received_codes) / max(total, 1)
        minimum = _minimum_coverage("ADATA_KLINE_MIN_COVERAGE", 0.90)
        if staged_rows <= 0 or coverage < minimum:
            raise RuntimeError(
                "adata K-line coverage below threshold: "
                f"{len(received_codes)}/{total} ({coverage:.1%}) < {minimum:.1%}; "
                f"failed={failed_codes[:10]}; preserving previous data"
            )
        code_where, params = _code_scope_predicate(
            sorted(received_codes),
            column="stock_code",
            prefix="kline_code",
        )
        params.update(
            {
                "start_date": start,
                "end_date": end_value,
                "k_type": k_type,
                "adjust_type": adjust_type,
            }
        )
        published = _publish_temporary_stage(
            engine,
            stage_connection,
            stage_table=stage_table,
            target_table="sm_stock_kline",
            where_sql=(
                f"{code_where} AND trade_date >= :start_date "
                "AND trade_date <= :end_date AND k_type=:k_type "
                "AND adjust_type=:adjust_type"
            ),
            params=params,
            lock_name="probiga:stock_kline",
        )
        logger.info(
            "adata K-line published %d staged rows, coverage=%.2f%%",
            published,
            coverage * 100,
        )
    finally:
        stage_connection.close()


def _step_stock_kline_akshare(
    engine: Engine,
    stock_codes: list[str],
    kline_start: str | None,
    kline_end: str | None,
    akshare_adjust: str,
    *,
    progress_file: str = "",
    skip_progress: bool = False,
) -> None:
    from biz.stock_market.stock_kline_akshare import (
        ADJUST_TO_INT,
        akshare_daily_to_sm_kline,
        em_code_to_sina_symbol,
        fetch_stock_daily_kline,
        _to_yyyymmdd,
        _to_yyyy_mm_dd,
    )

    k_type = int(os.environ.get("SM_STOCK_K_TYPE", "1"))
    if k_type != 1:
        logger.warning("AkShare 新浪日线仅支持日 K，已忽略 SM_STOCK_K_TYPE=%s，按 k_type=1 处理。", k_type)
    adjust = (akshare_adjust or os.environ.get("SM_STOCK_KLINE_AKSHARE_ADJUST", "") or "").strip().lower()
    if adjust not in ADJUST_TO_INT:
        raise ValueError(f"--kline-adjust / SM_STOCK_KLINE_AKSHARE_ADJUST 须为 空|qfq|hfq，收到: {akshare_adjust!r}")
    adjust_type = ADJUST_TO_INT[adjust]

    start_raw = kline_start or os.environ.get("SM_MARKET_START", "2020-01-01")
    end_raw = kline_end or os.environ.get("SM_MARKET_END") or datetime.now().strftime("%Y-%m-%d")
    start_api = _to_yyyymmdd(start_raw)
    end_api = _to_yyyymmdd(end_raw)
    start_sql = _to_yyyy_mm_dd(start_raw)
    end_sql = _to_yyyy_mm_dd(end_raw)

    do_truncate = os.environ.get("SM_STOCK_KLINE_AKSHARE_TRUNCATE") == "1"
    if do_truncate:
        raise RuntimeError(
            "SM_STOCK_KLINE_AKSHARE_TRUNCATE=1 is no longer supported; "
            "use safe code/date-partition replacement"
        )
    pf = (progress_file or "").strip()
    use_flush = bool(pf) and not skip_progress
    logger.info(
        "个股K线(akshare)：%d 只，区间 %s ~ %s（API %s~%s），复权=%s，安全分区替换=%s，"
        "断点进度文件(--progress-file 且未 --skip-progress)=%s",
        len(stock_codes),
        start_sql,
        end_sql,
        start_api,
        end_api,
        adjust or "不复权",
        True,
        use_flush,
    )
    if not use_flush:
        logger.info(
            "未启用断点逐只写入时，仍按「每只股票拉取后立即写入表 sm_stock_kline」，"
            "便于在库里随时看到进度（不再攒全市场后一次性写入）。"
        )

    written_flush = 0
    written_stocks = 0
    failed_codes: list[str] = []
    total_n = len(stock_codes)
    name_map = _load_stock_short_name_map(engine)
    log_every = max(1, int(os.environ.get("SM_STOCK_KLINE_PROGRESS_LOG_EVERY", "25")))
    for i, code in enumerate(stock_codes):
        if i == 0 or (i + 1) % log_every == 0 or (i + 1) == total_n:
            logger.info("个股K线(akshare)：拉取进度 %s/%s（当前 %s）", i + 1, total_n, code)
        try:
            sina_sym = em_code_to_sina_symbol(code)
            if not sina_sym:
                logger.warning("AkShare K线：无法映射新浪代码，跳过 %s", code)
                _sleep_stock_kline_akshare()
                continue
            def _pull() -> pd.DataFrame | None:
                return fetch_stock_daily_kline(code, start_api, end_api, adjust)

            logger.info("个股K线(akshare)：请求 %s %s …", code, sina_sym)
            raw = retry_remote(_pull)
            if raw is None or raw.empty:
                logger.warning("AkShare K线：无数据 %s %s", code, sina_sym)
                _sleep_stock_kline_akshare()
                continue
            sm_df = akshare_daily_to_sm_kline(
                raw,
                code,
                1,
                adjust_type,
                short_name=name_map.get(str(code).strip().zfill(6), ""),
            )
            if sm_df is not None and not sm_df.empty:
                sm_df = _to_numeric(
                    sm_df,
                    ["open", "close", "high", "low", "volume", "amount", "change", "change_pct", "turnover_ratio", "pre_close"],
                )
                etl = _with_etl(sm_df)
                exact_frame = _clean_df(etl).drop_duplicates(
                    subset=["stock_code", "trade_date", "k_type", "adjust_type"],
                    keep="last",
                )
                replace_table_rows_exact_keys(
                    exact_frame,
                    "sm_stock_kline",
                    engine,
                    key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
                    lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
                )
                if use_flush:
                    _kline_append_progress(pf, code)
                    written_flush += 1
                written_stocks += 1
        except Exception as e:  # pylint: disable=broad-except
            failed_codes.append(str(code).strip().zfill(6))
            logger.warning("AkShare stock_kline %s 失败：%s", code, e, exc_info=True)
        _sleep_stock_kline_akshare()

    try:
        with engine.connect() as conn:
            n_rows = conn.execute(text("SELECT COUNT(*) FROM sm_stock_kline")).scalar()
        dbn = make_url(str(engine.url)).database
        logger.info(
            "个股K线(akshare)：表 sm_stock_kline 在库「%s」中当前总行数=%s（请用同一库查询）",
            dbn,
            n_rows,
        )
    except Exception as e:  # pylint: disable=broad-except
        logger.warning("无法统计 sm_stock_kline 行数：%s", e)

    if use_flush and written_flush == 0 and stock_codes:
        logger.warning(
            "个股K线(akshare)：共 %d 只均未写入（逐只入库+进度文件模式）。示例代码：%s",
            len(stock_codes),
            stock_codes[0],
        )
    coverage = written_stocks / max(total_n, 1)
    minimum = _minimum_coverage("AKSHARE_KLINE_MIN_COVERAGE", 0.90)
    if coverage < minimum:
        raise RuntimeError(
            "AkShare K-line coverage below threshold: "
            f"{written_stocks}/{total_n} ({coverage:.1%}) < {minimum:.1%}; "
            f"failed={failed_codes[:10]}; missing partitions retained"
        )
    elif use_flush and written_flush:
        logger.info("个股K线(akshare)：逐只入库完成 %s 只，进度已写入 %s", written_flush, pf)
    elif not use_flush and written_stocks == 0 and stock_codes:
        logger.warning(
            "个股K线(akshare)：共 %d 只均未写入（表 sm_stock_kline 无新增）。示例代码：%s",
            len(stock_codes),
            stock_codes[0] if stock_codes else "",
        )
    elif not use_flush and written_stocks:
        logger.info(
            "个股K线(akshare)：本批成功写入 %s 只股票的数据（表 `sm_stock_kline`，每只股票多行按交易日）。",
            written_stocks,
        )


def _is_myquant_source(source: str) -> bool:
    return (source or "").strip().lower() in {"myquant", "gm", "emquant", "goldminer"}


def _myquant_batch_size(kind: str, default: int) -> int:
    specific = os.environ.get(f"MYQUANT_{kind.upper()}_BATCH_SIZE", "").strip()
    raw = specific or os.environ.get("MYQUANT_BATCH_SIZE", "").strip()
    if not raw:
        return default
    return max(1, int(raw))


def _myquant_timeout(default: int = 180) -> int:
    return max(30, int(os.environ.get("MYQUANT_TIMEOUT", str(default))))


def _to_local_naive_datetime(values: Any) -> pd.Series:
    series = pd.to_datetime(values, errors="coerce", utc=True)
    return series.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)


def _myquant_daily_to_sm_kline(df: pd.DataFrame, name_map: dict[str, str]) -> pd.DataFrame:
    from biz.stock_market.stock_kline_akshare import akshare_daily_to_sm_kline
    from integrations.myquant import to_stock_code

    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    work["stock_code"] = work["symbol"].map(to_stock_code).astype(str).str.zfill(6)
    work["date"] = _to_local_naive_datetime(work["eob"]).dt.strftime("%Y-%m-%d")
    if "change_percent" in work.columns and "change_pct" not in work.columns:
        work["change_pct"] = work["change_percent"]

    parts: list[pd.DataFrame] = []
    for code, one in work.groupby("stock_code", sort=False):
        cols = ["date", "open", "high", "low", "close", "volume", "amount", "pre_close"]
        cols += [c for c in ["change", "change_pct"] if c in one.columns]
        raw = one[cols].copy()
        parts.append(akshare_daily_to_sm_kline(raw, code, 1, 0, short_name=name_map.get(code, "")))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _step_stock_kline_myquant(
    engine: Engine,
    stock_codes: list[str],
    kline_start: str | None,
    kline_end: str | None,
    *,
    incremental: bool = False,
) -> None:
    from integrations.myquant import history, is_configured, to_gm_symbol

    if not is_configured():
        raise RuntimeError("MyQuant source selected but GM_TOKEN or runtime/emquant-py36/python.exe is not configured")

    start = kline_start or os.environ.get("SM_MARKET_START", "2020-01-01")
    end = kline_end or os.environ.get("SM_MARKET_END") or datetime.now().strftime("%Y-%m-%d")

    supported = [c for c in stock_codes if to_gm_symbol(c)]
    skipped = len(stock_codes) - len(supported)
    logger.info(
        "Stock K-line (MyQuant): %d supported stocks, %d skipped, range %s ~ %s, "
        "adjust_type=0, mode=%s staged-atomic",
        len(supported),
        skipped,
        start,
        end,
        "incremental" if incremental else "refresh",
    )
    if not supported:
        raise RuntimeError("MyQuant stock K-line has no supported stock symbols")

    name_map = _load_stock_short_name_map(engine)
    batch_size = _myquant_batch_size("kline", 80)
    fields = "symbol,eob,open,high,low,close,pre_close,volume,amount"
    written_rows = 0
    written_stocks: set[str] = set()
    stage_connection, stage_table = _create_temporary_stage(
        engine,
        target_table="sm_stock_kline",
        prefix="sm_stock_kline_myquant_stage",
    )
    try:
        for i, batch in enumerate(_chunked(supported, batch_size), start=1):
            raw = history(
                batch,
                frequency="1d",
                start_time=start,
                end_time=end,
                fields=fields,
                adjust=None,
                timeout=_myquant_timeout(300),
            )
            sm_df = _myquant_daily_to_sm_kline(raw, name_map)
            if sm_df is not None and not sm_df.empty:
                sm_df = _to_numeric(
                    sm_df,
                    ["open", "close", "high", "low", "volume", "amount", "change", "change_pct", "turnover_ratio", "pre_close"],
                )
                sm_df["stock_code"] = sm_df["stock_code"].astype(str).str.zfill(6)
                sm_df = sm_df.drop_duplicates(
                    subset=["stock_code", "trade_date", "k_type", "adjust_type"],
                    keep="last",
                )
                written_rows += _append_temporary_stage(
                    stage_connection,
                    stage_table,
                    sm_df,
                    chunksize=1000,
                )
                written_stocks.update(sm_df["stock_code"].unique().tolist())
            logger.info(
                "Stock K-line (MyQuant): stage batch %d/%d, rows=%d, stocks=%d",
                i,
                (len(supported) + batch_size - 1) // batch_size,
                written_rows,
                len(written_stocks),
            )
            _sleep()
        coverage = len(written_stocks) / len(supported)
        minimum = _minimum_coverage("MYQUANT_KLINE_MIN_COVERAGE", 0.90)
        if written_rows <= 0 or coverage < minimum:
            raise RuntimeError(
                "MyQuant stock K-line coverage below threshold: "
                f"{len(written_stocks)}/{len(supported)} ({coverage:.1%}) "
                f"< {minimum:.1%}; preserving previous data"
            )
        code_where, params = _code_scope_predicate(
            sorted(written_stocks),
            column="stock_code",
            prefix="kline_code",
        )
        params.update({"start_date": start, "end_date": end})
        _publish_temporary_stage(
            engine,
            stage_connection,
            stage_table=stage_table,
            target_table="sm_stock_kline",
            where_sql=(
                f"{code_where} AND trade_date >= :start_date "
                "AND trade_date <= :end_date AND k_type=1 AND adjust_type=0"
            ),
            params=params,
            lock_name="probiga:stock_kline",
        )
    finally:
        stage_connection.close()


_BIGQMT_IDENTITY_FIELDS = (
    "strategy_release_protocol",
    "strategy_identity_protocol",
    "strategy_identity_frozen",
    "strategy_build_sha",
    "strategy_git_blob",
    "strategy_source_sha256",
    "strategy_artifact_sha256",
    "strategy_loaded_identity_sha256",
)


def _formal_bigqmt_release_proof() -> dict[str, Any]:
    executor_role = os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "").strip()
    build_sha = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if executor_role != "qmt_windows_edge":
        raise RuntimeError("formal BigQMT publisher is not Windows-edge bound")
    if re.fullmatch(r"[0-9a-f]{40}", build_sha) is None or build_sha == "0" * 40:
        raise RuntimeError("formal BigQMT publisher build SHA is unavailable")
    from integrations.bigqmt import bridge as bigqmt_bridge
    from tools.run_qmt_windows_edge_release_bootstrap import (
        validate_bigqmt_strategy_release,
    )

    return validate_bigqmt_strategy_release(
        bigqmt_bridge.capabilities(timeout=180),
        expected_build_sha=build_sha,
    )


def _validate_bigqmt_capture_identity(
    capture: dict[str, Any],
    *,
    release_proof: dict[str, Any],
    requested_codes: list[str],
    action: str,
) -> dict[str, Any]:
    receipts = (
        capture.get("batch_receipts")
        if action == "minute"
        else [capture]
    )
    if not isinstance(receipts, list) or not receipts:
        raise RuntimeError(f"BigQMT {action} response receipts are unavailable")
    requested = sorted({str(code).split(".", 1)[0].zfill(6) for code in requested_codes})
    receipt_requested: list[str] = []
    receipt_ids: list[str] = []
    for receipt in receipts:
        if not isinstance(receipt, dict):
            raise RuntimeError(f"BigQMT {action} response receipt is malformed")
        if (
            receipt.get("status") != "ok"
            or receipt.get("source") != "gj_big_qmt_inner"
            or receipt.get("bridge_version") != "bigqmt_inner_v2"
            or receipt.get("action") != action
            or not str(receipt.get("request_id") or "")
        ):
            raise RuntimeError(f"BigQMT {action} response provenance is incomplete")
        for field in _BIGQMT_IDENTITY_FIELDS:
            if receipt.get(field) != release_proof.get(field):
                raise RuntimeError(
                    f"BigQMT {action} response release identity differs: {field}"
                )
        receipt_ids.append(str(receipt["request_id"]))
        raw_requested = receipt.get("requested_codes")
        if isinstance(raw_requested, list):
            receipt_requested.extend(
                str(code).split(".", 1)[0].zfill(6) for code in raw_requested
            )
    if sorted(receipt_requested) != requested:
        raise RuntimeError(f"BigQMT {action} response request set differs")
    return {
        "response_count": len(receipts),
        "request_id_set_hash": hashlib.sha256(
            "\n".join(sorted(receipt_ids)).encode("utf-8")
        ).hexdigest(),
        "requested_code_count": len(requested),
        "requested_code_set_hash": hashlib.sha256(
            "\n".join(requested).encode("utf-8")
        ).hexdigest(),
    }


def _step_stock_kline_qmt(
    engine: Engine,
    backend: Any,
    stock_codes: list[str],
    start: str,
    end: str,
    short_name_map: dict[str, str],
) -> None:
    """Replace an exact catalog-bound QMT daily window atomically."""
    from server.common.qmt_attestation_contract import (
        daily_market_source_batch_id,
    )
    from server.common.qmt_stock_catalog import load_stock_catalog
    from server.common.qmt_trade_calendar import load_trade_calendar_receipt

    bigqmt_release_proof = None
    if str(getattr(backend, "name", "")).lower() == "bigqmt":
        bigqmt_release_proof = _formal_bigqmt_release_proof()
    batch_size = max(20, int(os.environ.get("QMT_PRODUCTION_KLINE_BATCH_SIZE", "200")))
    history_engine = get_kline_engine()
    normalized_start = pd.to_datetime(start, errors="coerce")
    normalized_end = pd.to_datetime(end, errors="coerce")
    if (
        pd.isna(normalized_start)
        or pd.isna(normalized_end)
        or normalized_start > normalized_end
    ):
        raise RuntimeError("QMT daily K-line target range is invalid")
    normalized_start_text = normalized_start.strftime("%Y-%m-%d")
    normalized_end_text = normalized_end.strftime("%Y-%m-%d")
    with engine.connect() as connection:
        catalog = load_stock_catalog(
            connection,
            decision_known_at=datetime.now().replace(microsecond=0),
        )
        calendar_receipt = load_trade_calendar_receipt(
            connection,
            start_date=normalized_start_text,
            end_date=normalized_end_text,
            decision_known_at=datetime.now().replace(microsecond=0),
        )
    sessions = calendar_receipt.sessions_between(
        normalized_start_text, normalized_end_text
    )
    if not sessions:
        raise RuntimeError("QMT daily K-line range has no authoritative session")
    member_by_code = {
        str(member["stock_code"]).zfill(6): member for member in catalog.members
    }
    stock_codes = sorted(
        code
        for code, member in member_by_code.items()
        if str(member["list_date"]) <= normalized_end_text
        and (
            member.get("expire_date") in (None, "")
            or normalized_start_text <= str(member["expire_date"])
        )
    )
    if not stock_codes:
        raise RuntimeError("QMT catalog target-range stock universe is empty")
    # Bind the raw source batch to both independent roots.  A catalog or
    # calendar change during the fetch is therefore detected by attestation.
    capture_batch_id = daily_market_source_batch_id(
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_manifest_hash=calendar_receipt.manifest_hash,
    )
    stage_connection, stage_table = _create_temporary_stage(
        history_engine,
        target_table="sm_stock_kline",
        prefix="sm_stock_kline_qmt_stage",
    )
    staged_rows = 0
    native_no_trade_pairs: set[tuple[str, str]] = set()
    total_batches = (len(stock_codes) + batch_size - 1) // batch_size
    try:
        for batch_no, batch in enumerate(_chunked(stock_codes, batch_size), start=1):
            frame = backend.fetch_kline(
                batch,
                start,
                end,
                short_name_map=short_name_map,
                dividend_type=os.environ.get("QMT_DIVIDEND_TYPE", "none"),
            )
            if frame is None:
                raise RuntimeError(
                    f"QMT daily K-line batch {batch_no}/{total_batches} returned no frame; "
                    "preserving previous data"
                )
            if bigqmt_release_proof is not None:
                _validate_bigqmt_capture_identity(
                    dict(frame.attrs.get("bigqmt_capture") or {}),
                    release_proof=bigqmt_release_proof,
                    requested_codes=batch,
                    action="kline",
                )
            frame = frame.copy()
            required_columns = {
                "stock_code", "trade_date", "k_type", "adjust_type",
                "open", "close", "high", "low", "volume", "amount",
            }
            missing_columns = sorted(required_columns - set(frame.columns))
            if missing_columns:
                raise RuntimeError(
                    "QMT daily K-line response schema differs: "
                    f"missing={missing_columns}"
                )
            frame["trade_date"] = pd.to_datetime(
                frame["trade_date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            frame = frame[
                (frame["trade_date"] >= normalized_start_text)
                & (frame["trade_date"] <= normalized_end_text)
            ]
            for column in ("open", "close", "high", "low", "volume", "amount"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            frame = frame.dropna(
                subset=["stock_code", "trade_date", "open", "close", "high", "low"]
            )
            frame = frame[
                (frame["open"] > 0)
                & (frame["close"] > 0)
                & (frame["high"] >= frame[["open", "close"]].max(axis=1))
                & (frame["low"] <= frame[["open", "close"]].min(axis=1))
                & (frame["volume"] >= 0)
                & (frame["amount"] >= 0)
            ]
            frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
            frame = frame[
                frame["stock_code"].isin(batch)
                & (pd.to_numeric(frame["k_type"], errors="coerce") == 1)
                & (pd.to_numeric(frame["adjust_type"], errors="coerce") == 0)
            ]
            frame = frame.drop_duplicates(
                subset=["stock_code", "trade_date", "k_type", "adjust_type"],
                keep="last",
            )
            observed_by_date = {
                str(trade_date): set(group["stock_code"].astype(str))
                for trade_date, group in frame.groupby("trade_date")
            }
            for trade_date in sessions:
                expected = {
                    code
                    for code in batch
                    if str(member_by_code[code]["list_date"]) <= trade_date
                    and (
                        member_by_code[code].get("expire_date") in (None, "")
                        or trade_date <= str(member_by_code[code]["expire_date"])
                    )
                }
                observed = observed_by_date.get(trade_date, set())
                extra = sorted(observed - expected)
                if extra:
                    raise RuntimeError(
                        "QMT daily K-line contains codes outside the frozen catalog: "
                        f"batch={batch_no}, date={trade_date}, "
                        f"extra={extra[:10]}"
                    )
                missing = sorted(expected - observed)
                if missing:
                    if bigqmt_release_proof is None:
                        raise RuntimeError(
                            "QMT daily K-line cannot classify absent catalog "
                            "codes without exact BigQMT response identity"
                        )
                    native_no_trade_pairs.update(
                        (code, trade_date) for code in missing
                    )
                    logger.info(
                        "QMT daily K-line native NO_TRADE: batch=%d "
                        "date=%s count=%d sample=%s",
                        batch_no,
                        trade_date,
                        len(missing),
                        missing[:10],
                    )
            extra_sessions = set(observed_by_date) - set(sessions)
            if extra_sessions:
                raise RuntimeError(
                    "QMT daily K-line returned sessions outside the frozen calendar: "
                    f"{sorted(extra_sessions)[:10]}"
                )
            if frame.empty:
                raise RuntimeError(
                    f"QMT daily K-line batch {batch_no}/{total_batches} failed validation"
                )
            if str(getattr(backend, "name", "")).lower() == "bigqmt":
                from integrations.qmt.local_history import persist_daily_kline_capture

                captured = persist_daily_kline_capture(
                    frame,
                    source_engine=engine,
                    batch_id=capture_batch_id,
                )
                if captured != len(frame):
                    raise RuntimeError(
                        "BigQMT raw daily capture mismatch: "
                        f"{captured}/{len(frame)}"
                    )
            staged_rows += _append_temporary_stage(
                stage_connection,
                stage_table,
                frame,
                chunksize=1000,
            )
            logger.info(
                "QMT daily K-line stage batch %d/%d: rows=%d staged=%d",
                batch_no,
                total_batches,
                len(frame),
                staged_rows,
            )
        written = _publish_temporary_stage(
            history_engine,
            stage_connection,
            stage_table=stage_table,
            target_table="sm_stock_kline",
            where_sql=(
                "trade_date BETWEEN :start_date AND :end_date "
                "AND k_type=1 AND adjust_type=0"
            ),
            params={
                "start_date": normalized_start_text,
                "end_date": normalized_end_text,
            },
            lock_name="probiga:stock_kline",
        )
        logger.info(
            "QMT daily K-line complete: rows=%d native_no_trade=%d "
            "catalog_batch=%s source_batch=%s",
            written,
            len(native_no_trade_pairs),
            catalog.batch_id,
            capture_batch_id,
        )
    finally:
        stage_connection.close()


def _try_step_stock_kline_registry(
    engine: Engine,
    stock_codes: list[str],
    kline_start: str | None,
    kline_end: str | None,
    *,
    incremental: bool,
) -> bool:
    try:
        from integrations.registry import get_backend

        backend = get_backend("kline")
    except Exception as exc:
        logger.warning(
            "Data-source registry error; falling back to the explicit K-line source: %s",
            exc,
        )
        return False
    if backend is None:
        return False

    logger.info("Stock K-line: using registry backend '%s'", backend.name)
    short_name_map = _load_stock_short_name_map(engine)
    start = kline_start or os.environ.get("SM_MARKET_START", "2020-01-01")
    end = kline_end or os.environ.get("SM_MARKET_END") or datetime.now().strftime("%Y-%m-%d")
    if backend.name in {"qmt", "bigqmt"}:
        _step_stock_kline_qmt(engine, backend, stock_codes, start, end, short_name_map)
        return True
    df = backend.fetch_kline(stock_codes, start, end, short_name_map=short_name_map)
    if df is not None and not df.empty:
        kline_cols = [
            "stock_code",
            "short_name",
            "trade_time",
            "trade_date",
            "k_type",
            "adjust_type",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount",
            "change",
            "change_pct",
            "turnover_ratio",
            "pre_close",
        ]
        for col in kline_cols:
            if col not in df.columns:
                if col == "k_type":
                    df[col] = int(os.environ.get("SM_STOCK_K_TYPE", "1"))
                elif col == "adjust_type":
                    df[col] = 0
                else:
                    df[col] = None
        df = _to_numeric(
            df,
            [
                "open",
                "close",
                "high",
                "low",
                "volume",
                "amount",
                "change",
                "change_pct",
                "turnover_ratio",
                "pre_close",
            ],
        )
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].copy()
        type_pairs = df[["k_type", "adjust_type"]].dropna().drop_duplicates()
        if len(type_pairs) != 1:
            raise RuntimeError(
                f"Stock K-line ({backend.name}) returned ambiguous k_type/adjust_type pairs; "
                "preserving previous data"
            )
        k_type = int(type_pairs.iloc[0]["k_type"])
        adjust_type = int(type_pairs.iloc[0]["adjust_type"])
        written = _replace_validated_code_date_frame(
            get_kline_engine(),
            df,
            table_name="sm_stock_kline",
            requested_codes=stock_codes,
            code_column="stock_code",
            date_column="trade_date",
            label=f"Stock K-line ({backend.name})",
            coverage_env="REGISTRY_KLINE_MIN_COVERAGE",
            default_coverage=0.90,
            identity_columns=(
                "stock_code", "trade_date", "k_type", "adjust_type",
            ),
            extra_where="k_type=:scope_k_type AND adjust_type=:scope_adjust_type",
            extra_params={
                "scope_k_type": k_type,
                "scope_adjust_type": adjust_type,
            },
        )
        logger.info(
            "Stock K-line (%s): atomically wrote %d rows, mode=%s",
            backend.name,
            written,
            "incremental" if incremental else "refresh",
        )
    else:
        raise RuntimeError(
            f"Stock K-line ({backend.name}) returned no data; preserving previous data"
        )
    return True


def step_stock_kline(
    engine: Engine,
    stock_codes: list[str],
    kline_start: str | None = None,
    kline_end: str | None = None,
    *,
    kline_source: str = "adata",
    akshare_adjust: str = "",
    progress_file: str = "",
    skip_progress: bool = False,
    incremental: bool = False,
) -> None:
    if not stock_codes:
        logger.warning("个股K线：si_all_code 未读到代码，保留旧数据并跳过写入。请先执行 sync_stock_info 写入 si_all_code。")
        return

    source_override = {"SM_STOCK_KLINE_SOURCE": kline_source} if kline_source else {}
    with temporary_env(source_override):
        handled_by_registry = _try_step_stock_kline_registry(
            engine,
            stock_codes,
            kline_start,
            kline_end,
            incremental=incremental,
        )
    if handled_by_registry:
        return

    src = (kline_source or os.environ.get("SM_STOCK_KLINE_SOURCE", "adata")).strip().lower()
    if _is_myquant_source(src):
        _step_stock_kline_myquant(
            engine,
            stock_codes,
            kline_start,
            kline_end,
            incremental=incremental,
        )
        return
    if src == "akshare":
        _step_stock_kline_akshare(
            engine,
            stock_codes,
            kline_start,
            kline_end,
            akshare_adjust,
            progress_file=progress_file,
            skip_progress=skip_progress,
        )
        return

    start = kline_start or os.environ.get("SM_MARKET_START", "2020-01-01")
    end = kline_end or os.environ.get("SM_MARKET_END")
    k_type = int(os.environ.get("SM_STOCK_K_TYPE", "1"))
    adjust_type = int(os.environ.get("SM_STOCK_ADJUST_TYPE", "1"))
    _step_stock_kline_adata(engine, stock_codes, start, end, k_type, adjust_type, incremental=incremental)


def _step_stock_minute_qmt(engine: Engine, backend: Any, stock_codes: list[str]) -> None:
    """Synchronize QMT minute bars in small atomic batches.

    This avoids materializing roughly 1.3 million rows in one JSON response and
    prevents a failed QMT call from clearing the canonical trading day.
    """
    executor_role = os.environ.get(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", ""
    ).strip()
    release_build_sha = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if executor_role != "qmt_windows_edge":
        raise RuntimeError("QMT minute publisher is not Windows-edge bound")
    if (
        len(release_build_sha) != 40
        or release_build_sha == "0" * 40
        or any(
            character not in "0123456789abcdef"
            for character in release_build_sha
        )
    ):
        raise RuntimeError("QMT minute publisher build identity is unavailable")
    strategy_release_proof: dict[str, Any] | None = None
    if str(getattr(backend, "name", "")).strip().lower() == "bigqmt":
        strategy_release_proof = _formal_bigqmt_release_proof()
    caller_stock_codes = sorted(
        {str(code).strip().zfill(6) for code in stock_codes if str(code).strip()}
    )
    trade_date = _default_myquant_minute_date(engine)
    decision_known_at = datetime.now().replace(microsecond=0)
    from server.common.qmt_stock_catalog import load_target_stock_catalog
    from server.common.qmt_trade_calendar import load_trade_calendar_receipt

    catalog, stock_codes = load_target_stock_catalog(
        engine,
        target_date=trade_date,
        decision_known_at=decision_known_at,
    )
    with engine.connect() as connection:
        calendar_receipt = load_trade_calendar_receipt(
            connection,
            start_date=trade_date,
            end_date=trade_date,
            decision_known_at=decision_known_at,
        )
    sessions = calendar_receipt.sessions_between(trade_date, trade_date)
    if sessions != [trade_date]:
        raise RuntimeError(
            "QMT minute target is not one authoritative trading session"
        )
    if not stock_codes:
        raise RuntimeError("QMT minute immutable target universe is empty")
    requested_code_set = set(stock_codes)
    reference_evidence = {
        "schema": "probiga.qmt-minute-reference-roots.v1",
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "calendar_batch_id": calendar_receipt.batch_id,
        "calendar_manifest_hash": calendar_receipt.manifest_hash,
        "calendar_source_batch_id": calendar_receipt.source_batch_id,
        "trade_date": trade_date,
        "release_build_sha": release_build_sha,
        "executor_role": executor_role,
        "bigqmt_strategy_release": strategy_release_proof,
        "caller_stock_code_count": len(caller_stock_codes),
        "caller_stock_codes_sha256": _qmt_minute_code_set_sha256(
            caller_stock_codes
        ),
    }
    source_provider = (
        "gj_big_qmt_inner"
        if str(getattr(backend, "name", "")).lower() == "bigqmt"
        else "guojin_miniqmt_gateway"
    )
    coverage_captured_at = datetime.now().replace(microsecond=0)
    minute_run_id = (
        f"qmt_min_{trade_date.replace('-', '')}_"
        f"{coverage_captured_at.strftime('%H%M%S')}_{os.getpid()}"
    )
    daily_run_id = (
        f"qmt_day_{trade_date.replace('-', '')}_"
        f"{coverage_captured_at.strftime('%H%M%S')}_{os.getpid()}"
    )
    grid_profile = minute_grid_profile_for_capture(
        trade_date=trade_date,
        captured_at=coverage_captured_at,
    )
    expected_minute_grid = minute_time_grid(grid_profile)
    expected_minute_times = set(expected_minute_grid)
    full_native_minute_times = set(minute_time_grid(QMT_MINUTE_GRID_PROFILE))
    history_engine = get_kline_engine()
    batch_size = max(5, int(os.environ.get("QMT_PRODUCTION_MINUTE_BATCH_SIZE", "40")))
    count = max(0, int(os.environ.get("QMT_MINUTE_COUNT", "0") or 0))
    min_coverage = min(1.0, max(0.0, float(os.environ.get("QMT_MINUTE_MIN_COVERAGE", "0.85"))))
    total_batches = (len(stock_codes) + batch_size - 1) // batch_size
    written = 0
    responded_codes: set[str] = set()
    published_codes: set[str] = set()
    coverage_partitions: list[dict[str, Any]] = []
    source_response_receipts: list[dict[str, Any]] = []
    stage_table = f"sm_stock_minute_qmt_stage_{os.getpid()}"
    stage_connection = _create_qmt_minute_stage(history_engine, stage_table)
    try:
        for batch_no, batch in enumerate(_chunked(stock_codes, batch_size), start=1):
            frame = backend.fetch_minute(
                batch,
                trade_date,
                start_date=trade_date,
                end_date=trade_date,
                count=count,
                # A live QMT subscription does not guarantee that every
                # symbol's 1-minute cache is present.  Ask QMT to refresh the
                # requested day incrementally even for a bounded last-N run;
                # otherwise uncached symbols return padded zero-volume rows.
                download_history=True,
            )
            frame = frame.copy() if frame is not None else pd.DataFrame()
            if strategy_release_proof is not None:
                source_response_receipts.append(
                    {
                        "kind": "minute",
                        "batch_number": batch_no,
                        **_validate_bigqmt_capture_identity(
                            dict(frame.attrs.get("bigqmt_capture") or {}),
                            release_proof=strategy_release_proof,
                            requested_codes=batch,
                            action="minute",
                        ),
                    }
                )
            if "stock_code" not in frame.columns:
                frame["stock_code"] = pd.Series(dtype=str)
            frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
            returned_codes = set(frame["stock_code"].astype(str))
            outside_batch = returned_codes - set(batch)
            if outside_batch:
                raise RuntimeError(
                    "QMT minute returned codes outside the requested batch: "
                    f"{sorted(outside_batch)[:10]}"
                )
            if "trade_time" not in frame.columns:
                frame["trade_time"] = pd.Series(dtype="datetime64[ns]")
            frame["trade_time"] = pd.to_datetime(
                frame["trade_time"], errors="coerce"
            )
            if "trade_date" not in frame.columns:
                frame["trade_date"] = frame["trade_time"]
            frame["trade_date"] = pd.to_datetime(
                frame["trade_date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            # Freeze a live capture at one session prefix.  Native bars that
            # arrive after that cutoff are excluded consistently across later
            # batches; non-native timestamps remain and fail the assessment.
            observed_times = frame["trade_time"].dt.strftime("%H:%M:%S")
            later_native_rows = observed_times.isin(
                full_native_minute_times - expected_minute_times
            )
            frame = frame.loc[~later_native_rows].copy()
            for column in ("price", "avg_price", "change", "change_pct", "volume", "amount"):
                if column not in frame.columns:
                    frame[column] = None
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            frame["period"] = "1m"
            frame["data_source"] = source_provider
            frame["batch_id"] = minute_run_id

            daily_frame = backend.fetch_kline(
                batch,
                trade_date,
                trade_date,
                dividend_type="none",
                download_history=True,
            )
            daily_frame = (
                daily_frame.copy()
                if daily_frame is not None else pd.DataFrame()
            )
            if strategy_release_proof is not None:
                source_response_receipts.append(
                    {
                        "kind": "daily_no_trade_evidence",
                        "batch_number": batch_no,
                        **_validate_bigqmt_capture_identity(
                            dict(daily_frame.attrs.get("bigqmt_capture") or {}),
                            release_proof=strategy_release_proof,
                            requested_codes=batch,
                            action="kline",
                        ),
                    }
                )
            if "stock_code" not in daily_frame.columns:
                daily_frame["stock_code"] = pd.Series(dtype=str)
            daily_frame["stock_code"] = (
                daily_frame["stock_code"].astype(str).str.zfill(6)
            )
            outside_daily_batch = set(daily_frame["stock_code"]) - set(batch)
            if outside_daily_batch:
                raise RuntimeError(
                    "QMT daily no-trade evidence returned codes outside the "
                    f"requested batch: {sorted(outside_daily_batch)[:10]}"
                )
            daily_frame["data_source"] = source_provider
            daily_frame["batch_id"] = daily_run_id

            partition = assess_minute_coverage(
                expected_codes=batch,
                daily_rows=daily_frame.to_dict(orient="records"),
                minute_rows=frame.to_dict(orient="records"),
                trade_date=trade_date,
                provider=source_provider,
                daily_provider=source_provider,
                run_id=minute_run_id,
                catalog_batch_id=catalog.batch_id,
                catalog_manifest_hash=catalog.manifest_hash,
                calendar_batch_id=calendar_receipt.batch_id,
                calendar_manifest_hash=calendar_receipt.manifest_hash,
                source_batch_id=minute_run_id,
                daily_source_batch_id=daily_run_id,
                captured_at=coverage_captured_at,
                grid_profile=grid_profile,
            )
            require_exact_coverage(partition)
            coverage_partitions.append(partition)
            active_codes = {
                str(row["stock_code"])
                for row in partition["entities"]
                if row.get("expected_state") == "TRADED"
            }
            responded_codes.update(batch)
            published_codes.update(active_codes)
            frame = frame[frame["stock_code"].isin(active_codes)]
            if frame.empty:
                logger.info(
                    "QMT minute batch %d/%d: validated=%d published=0",
                    batch_no,
                    total_batches,
                    len(batch),
                )
                continue
            batch_written = _append_qmt_minute_stage(
                stage_connection,
                stage_table,
                frame,
            )
            written += batch_written
            logger.info(
                "QMT minute batch %d/%d: rows=%d responded=%d published=%d",
                batch_no,
                total_batches,
                batch_written,
                len(responded_codes),
                len(published_codes),
            )

        coverage_bundle = combine_minute_coverage_partitions(
            expected_codes=stock_codes,
            partitions=coverage_partitions,
        )
        coverage_manifest = require_exact_coverage(coverage_bundle)
        coverage = len(responded_codes) / max(len(stock_codes), 1)
        if (
            responded_codes != requested_code_set
            or coverage != 1.0
            or int(coverage_manifest["bar_count"]) != written
        ):
            raise RuntimeError("QMT minute exact coverage/physical stage differs")
        first_trade_time = pd.Timestamp(
            f"{trade_date} {expected_minute_grid[0]}"
        )
        last_trade_time = pd.Timestamp(
            f"{trade_date} {expected_minute_grid[-1]}"
        )
        captured_at = datetime.now().replace(microsecond=0)
        (
            capture_mode,
            live_forward_capture,
            capture_lag_seconds,
        ) = _classify_qmt_minute_capture(
            trade_date=trade_date,
            last_trade_time=last_trade_time.to_pydatetime(),
            captured_at=captured_at,
        )
        with engine.begin() as coverage_connection:
            coverage_insert = insert_coverage_bundle(
                coverage_connection,
                coverage_bundle,
            )
        (
            universe_evidence,
            final_quality_status,
            forward_eligible,
        ) = _qmt_minute_receipt_disposition(
            stock_codes,
            responded_codes,
            published_codes,
            live_forward_capture=live_forward_capture,
        )
        full_requested_response_coverage = bool(
            universe_evidence["full_requested_response_coverage"]
            and responded_codes == requested_code_set
        )
        if not full_requested_response_coverage:
            final_quality_status = "PARTIAL"
            forward_eligible = False
        receipt_evidence = {
            "backend": str(getattr(backend, "name", "")),
            "batch_size": batch_size,
            "minute_count": count,
            "coverage_threshold": min_coverage,
            "capture_mode": capture_mode,
            "captured_at": captured_at.isoformat(sep=" "),
            "capture_lag_seconds": capture_lag_seconds,
            "reference_roots": reference_evidence,
            "minute_coverage_manifest": coverage_manifest,
            "minute_coverage_insert": coverage_insert,
            "minute_grid_profile": grid_profile,
            "minute_grid_hash": coverage_manifest["minute_grid_hash"],
            "minute_grid_bar_count": len(expected_minute_grid),
            "minute_grid_native_fixture_hash": (
                QMT_MINUTE_GRID_NATIVE_FIXTURE_HASH
            ),
            "source_response_receipt_count": len(source_response_receipts),
            "source_response_receipt_hash": qmt_coverage_digest(
                source_response_receipts
            ),
            "source_response_receipts": source_response_receipts,
            **universe_evidence,
        }
        # Publication intentionally uses bounded per-code transactions to
        # avoid MySQL lock-table exhaustion.  The generation lock spans the
        # receipt barrier, every target batch and the final PASS receipt, so a
        # concurrent refresh cannot re-enable an older receipt mid-publish.
        with mysql_named_lock(
            history_engine,
            STOCK_MINUTE_FREEZE_LOCK_NAME,
            timeout_seconds=0,
            connection=stage_connection,
        ):
            # Revoke every overlapping prior receipt before the first target
            # batch.  A crash leaves PUBLISHING/FAILED and therefore no
            # forward consumer can accept the mixed physical window.
            _record_qmt_minute_receipt(
                engine,
                trade_date=trade_date,
                first_trade_time=first_trade_time.to_pydatetime(),
                last_trade_time=last_trade_time.to_pydatetime(),
                expected_count=len(stock_codes),
                observed_count=len(responded_codes),
                row_count=0,
                source_provider=source_provider,
                capture_mode=capture_mode,
                forward_eligible=False,
                quality_status="PUBLISHING",
                evidence={
                    **receipt_evidence,
                    "publication_state": "PUBLISHING",
                },
            )
            try:
                published_rows = _commit_qmt_minute_stage(
                    history_engine,
                    stage_connection,
                    stage_table,
                    trade_date=trade_date,
                    replacement_codes=stock_codes,
                )
                if published_rows != written:
                    raise RuntimeError(
                        "QMT minute target/receipt row mismatch: "
                        f"published={published_rows} staged={written}"
                    )
            except BaseException:
                try:
                    _record_qmt_minute_receipt(
                        engine,
                        trade_date=trade_date,
                        first_trade_time=first_trade_time.to_pydatetime(),
                        last_trade_time=last_trade_time.to_pydatetime(),
                        expected_count=len(stock_codes),
                        observed_count=len(responded_codes),
                        row_count=0,
                        source_provider=source_provider,
                        capture_mode=capture_mode,
                        forward_eligible=False,
                        quality_status="FAILED",
                        evidence={
                            **receipt_evidence,
                            "publication_state": "FAILED",
                        },
                    )
                except Exception:
                    logger.exception(
                        "Unable to mark failed QMT minute publication receipt"
                    )
                raise
            _record_qmt_minute_receipt(
                engine,
                trade_date=trade_date,
                first_trade_time=first_trade_time.to_pydatetime(),
                last_trade_time=last_trade_time.to_pydatetime(),
                expected_count=len(stock_codes),
                observed_count=len(responded_codes),
                row_count=written,
                source_provider=source_provider,
                capture_mode=capture_mode,
                forward_eligible=forward_eligible,
                quality_status=final_quality_status,
                evidence={
                    **receipt_evidence,
                    "publication_state": final_quality_status,
                },
            )
        logger.info("QMT minute complete: rows=%d coverage=%.2f%%", written, coverage * 100)
    finally:
        _drop_qmt_minute_stage(stage_connection, stage_table)


def step_stock_minute(engine: Engine, stock_codes: list[str]) -> None:
    # --- registry 统一数据源入口 ---
    try:
        from integrations.registry import get_backend
        backend = get_backend("minute")
    except Exception as exc:
        logger.warning("数据源 registry 错误，改用显式分钟数据源: %s", exc)
        backend = None
    if backend is not None:
        logger.info("分钟K线: 使用 registry backend '%s'", backend.name)
        if backend.name in {"qmt", "bigqmt"}:
            _step_stock_minute_qmt(engine, backend, stock_codes)
            return
        trade_date = _default_myquant_minute_date(engine)
        df = backend.fetch_minute(stock_codes, trade_date)
        if df is not None and not df.empty:
            minute_cols = ["stock_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount"]
            for col in minute_cols:
                if col not in df.columns:
                    df[col] = None
            df = _to_numeric(df, ["price", "avg_price", "change", "change_pct", "volume", "amount"])
            written = _replace_validated_code_date_frame(
                get_kline_engine(),
                df,
                table_name="sm_stock_minute",
                requested_codes=stock_codes,
                code_column="stock_code",
                date_column="trade_time",
                label=f"stock minute ({backend.name})",
                coverage_env="REGISTRY_MINUTE_MIN_COVERAGE",
                default_coverage=0.80,
                day_partition=True,
                receipt_engine=engine,
            )
            logger.info("分钟K线(%s): 原子写入 %d 行", backend.name, written)
        else:
            raise RuntimeError(
                f"分钟K线({backend.name}): 未获取到数据，保留旧分区"
            )
        return
    # --- end registry ---

    source = os.environ.get("SM_STOCK_MINUTE_SOURCE", os.environ.get("SM_MARKET_DATA_SOURCE", "")).strip().lower()
    if _is_myquant_source(source):
        _step_stock_minute_myquant(engine, stock_codes)
        return

    from adata.stock.market.stock_market.stock_market import StockMarket

    mk = StockMarket()
    minute_cols = ["stock_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount"]

    def _fetch_minute(code: str) -> pd.DataFrame | None:
        df = retry_remote(mk.get_market_min, stock_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "avg_price", "change", "change_pct", "volume", "amount"])
            if "trade_date" not in df.columns:
                df["trade_date"] = pd.to_datetime(df["trade_time"]).dt.strftime("%Y-%m-%d")
            return df[minute_cols]
        return None

    history_engine = get_kline_engine()
    stage_connection, stage_table = _create_temporary_stage(
        history_engine,
        target_table="sm_stock_minute",
        prefix="sm_stock_minute_adata_stage",
    )
    workers = _max_workers()
    received_codes: set[str] = set()
    failed_codes: list[str] = []
    staged_rows = 0
    first_date: str | None = None
    last_date: str | None = None
    pending_frames: list[pd.DataFrame] = []
    stage_batch_size = max(
        5, int(os.environ.get("ADATA_MINUTE_STAGE_CODE_BATCH_SIZE", "25"))
    )
    fetch_batch_size = max(
        workers,
        int(os.environ.get("ADATA_MINUTE_FETCH_CODE_BATCH_SIZE", str(workers * 16))),
    )

    def _flush_minute_stage() -> None:
        nonlocal pending_frames, staged_rows
        if not pending_frames:
            return
        staged_rows += _append_temporary_stage(
            stage_connection,
            stage_table,
            pd.concat(pending_frames, ignore_index=True),
            chunksize=1000,
        )
        pending_frames = []

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            done_count = 0
            for request_batch in _chunked(stock_codes, fetch_batch_size):
                futures = {
                    pool.submit(_fetch_minute, code): code for code in request_batch
                }
                for future in as_completed(futures):
                    done_count += 1
                    code = futures[future]
                    try:
                        frame = future.result()
                    except Exception as exc:  # pylint: disable=broad-except
                        failed_codes.append(code)
                        logger.warning("个股分钟 %s 失败：%s", code, exc)
                        continue
                    if frame is None or frame.empty:
                        continue
                    frame = frame.copy()
                    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
                    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
                    frame["trade_date"] = pd.to_datetime(
                        frame["trade_date"], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")
                    frame = frame.dropna(subset=["stock_code", "trade_time", "trade_date", "price"])
                    frame = frame[pd.to_numeric(frame["price"], errors="coerce") > 0]
                    frame = frame.drop_duplicates(
                        subset=["stock_code", "trade_time"], keep="last"
                    )
                    if frame.empty:
                        continue
                    received_codes.update(frame["stock_code"].unique().tolist())
                    dates = frame["trade_date"].astype(str)
                    batch_first = dates.min()
                    batch_last = dates.max()
                    first_date = batch_first if first_date is None else min(first_date, batch_first)
                    last_date = batch_last if last_date is None else max(last_date, batch_last)
                    pending_frames.append(frame)
                    if len(pending_frames) >= stage_batch_size:
                        _flush_minute_stage()
                    if done_count % 200 == 0:
                        logger.info(
                            "个股分钟临时区进度 %d/%d，rows=%d",
                            done_count,
                            len(stock_codes),
                            staged_rows,
                        )
                _flush_minute_stage()
        _flush_minute_stage()
        coverage = len(received_codes) / max(len(stock_codes), 1)
        minimum = _minimum_coverage("ADATA_MINUTE_MIN_COVERAGE", 0.80)
        if (
            staged_rows <= 0
            or first_date is None
            or last_date is None
            or coverage < minimum
        ):
            raise RuntimeError(
                "adata stock minute coverage below threshold: "
                f"{len(received_codes)}/{len(stock_codes)} ({coverage:.1%}) "
                f"< {minimum:.1%}; failed={failed_codes[:10]}; preserving previous data"
            )
        code_where, params = _code_scope_predicate(
            sorted(received_codes),
            column="stock_code",
            prefix="minute_code",
        )
        params.update({"first_date": first_date, "last_date": last_date})
        _publish_temporary_stage(
            history_engine,
            stage_connection,
            stage_table=stage_table,
            target_table="sm_stock_minute",
            where_sql=(
                f"{code_where} AND trade_date >= :first_date "
                "AND trade_date <= :last_date"
            ),
            params=params,
            lock_name=STOCK_MINUTE_FREEZE_LOCK_NAME,
            receipt_engine=engine,
        )
    finally:
        stage_connection.close()


def _default_myquant_minute_date(engine: Engine) -> str:
    raw = os.environ.get("MYQUANT_MINUTE_DATE", "").strip()
    if raw:
        return _normalize_cli_date(raw)
    today = datetime.now().date()
    if today.weekday() < 5:
        return today.isoformat()
    try:
        with engine.connect() as conn:
            d = conn.execute(text("SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1")).scalar()
        if d:
            return str(d)[:10]
    except Exception:  # pylint: disable=broad-except
        logger.debug("Failed to read latest kline trade date; using today.", exc_info=True)
    return today.isoformat()


def _myquant_minute_to_sm(df: pd.DataFrame) -> pd.DataFrame:
    from integrations.myquant import to_stock_code

    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["stock_code"] = df["symbol"].map(to_stock_code).astype(str).str.zfill(6)
    trade_time = _to_local_naive_datetime(df["eob"])
    out["trade_time"] = trade_time.dt.strftime("%Y-%m-%d %H:%M:%S")
    out["trade_date"] = trade_time.dt.strftime("%Y-%m-%d")
    out["price"] = pd.to_numeric(df.get("close"), errors="coerce")
    out["avg_price"] = None
    out["change"] = None
    out["change_pct"] = None
    out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
    out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
    return out.dropna(subset=["stock_code", "trade_time"])


def _step_stock_minute_myquant(engine: Engine, stock_codes: list[str]) -> None:
    from integrations.myquant import history, is_configured, to_gm_symbol

    if not is_configured():
        raise RuntimeError("MyQuant minute source selected but GM_TOKEN or runtime/emquant-py36/python.exe is not configured")

    trade_date = _default_myquant_minute_date(engine)
    start = os.environ.get("MYQUANT_MINUTE_START", f"{trade_date} 09:30:00")
    end = os.environ.get("MYQUANT_MINUTE_END", f"{trade_date} 15:00:00")
    frequency = os.environ.get("MYQUANT_MINUTE_FREQUENCY", "60s").strip() or "60s"

    supported = [c for c in stock_codes if to_gm_symbol(c)]
    if not supported:
        raise RuntimeError("MyQuant stock minute has no supported stock symbols")
    logger.info(
        "Stock minute (MyQuant): %d supported stocks, date=%s, frequency=%s",
        len(supported),
        trade_date,
        frequency,
    )
    batch_size = _myquant_batch_size("minute", 50)
    fields = "symbol,eob,open,high,low,close,volume,amount"
    written_rows = 0
    written_stocks: set[str] = set()
    history_engine = get_kline_engine()
    stage_connection, stage_table = _create_temporary_stage(
        history_engine,
        target_table="sm_stock_minute",
        prefix="sm_stock_minute_myquant_stage",
    )
    try:
        for i, batch in enumerate(_chunked(supported, batch_size), start=1):
            raw = history(
                batch,
                frequency=frequency,
                start_time=start,
                end_time=end,
                fields=fields,
                timeout=_myquant_timeout(300),
            )
            sm_df = _myquant_minute_to_sm(raw)
            if sm_df is not None and not sm_df.empty:
                sm_df = _to_numeric(sm_df, ["price", "avg_price", "change", "change_pct", "volume", "amount"])
                sm_df["stock_code"] = sm_df["stock_code"].astype(str).str.zfill(6)
                sm_df = sm_df.drop_duplicates(
                    subset=["stock_code", "trade_time"], keep="last"
                )
                written_rows += _append_temporary_stage(
                    stage_connection,
                    stage_table,
                    sm_df,
                    chunksize=1000,
                )
                written_stocks.update(sm_df["stock_code"].unique().tolist())
            logger.info(
                "Stock minute (MyQuant): stage batch %d/%d, rows=%d",
                i,
                (len(supported) + batch_size - 1) // batch_size,
                written_rows,
            )
            _sleep()
        coverage = len(written_stocks) / len(supported)
        minimum = _minimum_coverage("MYQUANT_MINUTE_MIN_COVERAGE", 0.80)
        if written_rows <= 0 or coverage < minimum:
            raise RuntimeError(
                "MyQuant stock minute coverage below threshold: "
                f"{len(written_stocks)}/{len(supported)} ({coverage:.1%}) "
                f"< {minimum:.1%}; preserving previous data"
            )
        code_where, params = _code_scope_predicate(
            sorted(written_stocks),
            column="stock_code",
            prefix="minute_code",
        )
        params.update({"trade_date": trade_date})
        _publish_temporary_stage(
            history_engine,
            stage_connection,
            stage_table=stage_table,
            target_table="sm_stock_minute",
            where_sql=f"{code_where} AND trade_date=:trade_date",
            params=params,
            lock_name=STOCK_MINUTE_FREEZE_LOCK_NAME,
            receipt_engine=engine,
        )
    finally:
        stage_connection.close()


def _legacy_step_stock_current(engine: Engine, stock_codes: list[str]) -> None:
    backend = None
    # --- registry 统一数据源入口 ---
    try:
        from integrations.registry import get_backend
        backend = get_backend("current")
    except Exception as exc:
        logger.error("数据源 registry 错误: %s", exc)
        return
    if backend is not None:
        logger.info("实时行情: 使用 registry backend '%s'", backend.name)
        short_name_map = _load_stock_short_name_map(engine)
        df = backend.fetch_current(stock_codes, short_name_map=short_name_map)
        if df is not None and not df.empty:
            current_cols = ["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount", "snapshot_at"]
            for col in current_cols:
                if col not in df.columns:
                    df[col] = None
            df = _to_numeric(df, ["price", "change", "change_pct", "volume", "amount"])
            replace_stock_current_snapshot(engine, _with_etl(df))
            logger.info("实时行情(%s): 写入 %d 行", backend.name, len(df))
        else:
            logger.warning("实时行情(%s): 未获取到数据", backend.name)
        return
    # --- end registry ---

    source = os.environ.get("SM_STOCK_CURRENT_SOURCE", os.environ.get("SM_MARKET_DATA_SOURCE", "")).strip().lower()
    if _is_myquant_source(source):
        _step_stock_current_myquant(engine, stock_codes)
        return

    from adata.stock.market.stock_market.stock_market import StockMarket

    mk = StockMarket()
    batch_size = max(10, int(os.environ.get("SM_CURRENT_BATCH", "300")))
    now = _now()
    parts: list[pd.DataFrame] = []
    for batch in _chunked(stock_codes, batch_size):
        df = retry_remote(mk.list_market_current, code_list=batch)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "change", "change_pct", "volume", "amount"])
            df["snapshot_at"] = now
            parts.append(df[["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount", "snapshot_at"]])
        _sleep()
    if parts:
        replace_stock_current_snapshot(engine, _with_etl(pd.concat(parts, ignore_index=True)))


def _load_prev_close_map(engine: Engine, snapshot_date: str) -> dict[str, float]:
    try:
        with engine.connect() as conn:
            d = conn.execute(
                text("SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1 AND trade_date < :d"),
                {"d": snapshot_date},
            ).scalar()
            if not d:
                d = conn.execute(text("SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1")).scalar()
            if not d:
                return {}
            rows = conn.execute(
                text("SELECT stock_code, close FROM sm_stock_kline WHERE k_type = 1 AND trade_date = :d"),
                {"d": str(d)[:10]},
            ).fetchall()
        return {str(r[0]).strip().zfill(6): float(r[1]) for r in rows if r[1] is not None}
    except Exception as e:  # pylint: disable=broad-except
        logger.warning("Failed to load previous close map for MyQuant current: %s", e)
        return {}


def _myquant_current_to_sm(df: pd.DataFrame, name_map: dict[str, str], prev_close_map: dict[str, float]) -> pd.DataFrame:
    from integrations.myquant import to_stock_code

    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["stock_code"] = df["symbol"].map(to_stock_code).astype(str).str.zfill(6)
    out["short_name"] = out["stock_code"].map(name_map).fillna("")
    out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    prev = out["stock_code"].map(prev_close_map)
    out["change"] = out["price"] - prev
    out["change_pct"] = (out["change"] / prev) * 100
    out.loc[prev.isna() | (prev <= 0), ["change", "change_pct"]] = None
    out["volume"] = pd.to_numeric(df.get("cum_volume"), errors="coerce")
    out["amount"] = pd.to_numeric(df.get("cum_amount"), errors="coerce")
    if "created_at" in df.columns:
        out["snapshot_at"] = _to_local_naive_datetime(df["created_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        out["snapshot_at"] = _now()
    return out[["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount", "snapshot_at"]]


def _step_stock_current_myquant(engine: Engine, stock_codes: list[str]) -> None:
    from integrations.myquant import current, is_configured, to_gm_symbol

    if not is_configured():
        raise RuntimeError("MyQuant current source selected but GM_TOKEN or runtime/emquant-py36/python.exe is not configured")

    supported = [c for c in stock_codes if to_gm_symbol(c)]
    name_map = _load_stock_short_name_map(engine)
    prev_close_cache: dict[str, dict[str, float]] = {}
    batch_size = _myquant_batch_size("current", max(10, int(os.environ.get("SM_CURRENT_BATCH", "300"))))
    fields = "symbol,price,open,high,low,cum_volume,cum_amount,created_at"
    parts: list[pd.DataFrame] = []
    for batch in _chunked(supported, batch_size):
        raw = current(batch, fields=fields, timeout=_myquant_timeout(120))
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        if raw is not None and not raw.empty and "created_at" in raw.columns:
            dates = _to_local_naive_datetime(raw["created_at"]).dt.strftime("%Y-%m-%d").dropna()
            if not dates.empty:
                snapshot_date = str(dates.iloc[0])[:10]
        if snapshot_date not in prev_close_cache:
            prev_close_cache[snapshot_date] = _load_prev_close_map(engine, snapshot_date)
        prev_close_map = prev_close_cache[snapshot_date]
        sm_df = _myquant_current_to_sm(raw, name_map, prev_close_map)
        if sm_df is not None and not sm_df.empty:
            parts.append(sm_df)
        _sleep()
    if parts:
        frame = pd.concat(parts, ignore_index=True)
        _validate_stock_current_frame(frame, stock_codes, source="myquant")
        replace_stock_current_snapshot(engine, _with_etl(frame))
    elif supported:
        raise RuntimeError("MyQuant current returned no rows")


def _step_stock_current_myquant_safe(engine: Engine, stock_codes: list[str]) -> None:
    from integrations.myquant import current, is_configured, to_gm_symbol

    if not is_configured():
        raise RuntimeError("MyQuant current source selected but GM_TOKEN or runtime/emquant-py36/python.exe is not configured")

    supported = [c for c in stock_codes if to_gm_symbol(c)]
    name_map = _load_stock_short_name_map(engine)
    prev_close_cache: dict[str, dict[str, float]] = {}
    batch_size = _myquant_batch_size("current", max(10, int(os.environ.get("SM_CURRENT_BATCH", "300"))))
    fields = "symbol,price,open,high,low,cum_volume,cum_amount,created_at"
    parts: list[pd.DataFrame] = []
    for batch in _chunked(supported, batch_size):
        raw = current(batch, fields=fields, timeout=_myquant_timeout(120))
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        if raw is not None and not raw.empty and "created_at" in raw.columns:
            dates = _to_local_naive_datetime(raw["created_at"]).dt.strftime("%Y-%m-%d").dropna()
            if not dates.empty:
                snapshot_date = str(dates.iloc[0])[:10]
        if snapshot_date not in prev_close_cache:
            prev_close_cache[snapshot_date] = _load_prev_close_map(engine, snapshot_date)
        prev_close_map = prev_close_cache[snapshot_date]
        sm_df = _myquant_current_to_sm(raw, name_map, prev_close_map)
        if sm_df is not None and not sm_df.empty:
            parts.append(sm_df)
        _sleep()
    if parts:
        frame = pd.concat(parts, ignore_index=True)
        _validate_stock_current_frame(frame, stock_codes, source="myquant")
        replace_stock_current_snapshot(engine, _with_etl(frame))
    elif supported:
        raise RuntimeError("MyQuant current returned no rows")


def step_stock_current(engine: Engine, stock_codes: list[str]) -> None:
    backend = None
    try:
        from integrations.registry import get_backend

        backend = get_backend("current")
    except Exception as exc:
        logger.warning("current registry backend unavailable, falling back to legacy source: %s", exc)

    if backend is not None:
        logger.info("stock current: using registry backend '%s'", backend.name)
        try:
            short_name_map = _load_stock_short_name_map(engine)
            df = backend.fetch_current(stock_codes, short_name_map=short_name_map)
        except Exception as exc:
            logger.warning("stock current backend '%s' failed; falling back to legacy source: %s", backend.name, exc)
        else:
            if df is not None and not df.empty:
                try:
                    received, coverage = _validate_stock_current_frame(
                        df,
                        stock_codes,
                        source=backend.name,
                    )
                    if backend.name == "qmt":
                        now = datetime.now()
                        hhmm = now.hour * 100 + now.minute
                        trading_now = now.weekday() < 5 and ((925 <= hhmm <= 1135) or (1255 <= hhmm <= 1505))
                        if trading_now:
                            latest_source = pd.to_datetime(df["snapshot_at"], errors="coerce").max()
                            max_age = max(30, int(os.environ.get("QMT_CURRENT_MAX_AGE_SECONDS", "180")))
                            if pd.isna(latest_source) or (now - latest_source.to_pydatetime()).total_seconds() > max_age:
                                raise RuntimeError(
                                    f"QMT realtime snapshot is stale: latest={latest_source}, max_age={max_age}s"
                                )
                    current_cols = ["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount", "snapshot_at"]
                    for col in current_cols:
                        if col not in df.columns:
                            df[col] = None
                    df = _to_numeric(df, ["price", "change", "change_pct", "volume", "amount"])
                    replace_stock_current_snapshot(engine, _with_etl(df))
                except Exception as exc:
                    logger.warning(
                        "stock current backend '%s' failed validation/write; falling back to legacy source: %s",
                        backend.name,
                        exc,
                    )
                else:
                    logger.info(
                        "stock current(%s): wrote %d rows, coverage=%.2f%%",
                        backend.name,
                        len(df),
                        coverage * 100,
                    )
                    return
            logger.warning("stock current backend '%s' returned no rows; falling back to legacy source", backend.name)

    source = os.environ.get("SM_STOCK_CURRENT_SOURCE", os.environ.get("SM_MARKET_DATA_SOURCE", "")).strip().lower()
    if _is_myquant_source(source):
        _step_stock_current_myquant_safe(engine, stock_codes)
        return

    from adata.stock.market.stock_market.stock_market import StockMarket

    mk = StockMarket()
    batch_size = max(10, int(os.environ.get("SM_CURRENT_BATCH", "300")))
    now = _now()
    parts: list[pd.DataFrame] = []
    for batch in _chunked(stock_codes, batch_size):
        df = retry_remote(mk.list_market_current, code_list=batch)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "change", "change_pct", "volume", "amount"])
            df["snapshot_at"] = now
            parts.append(df[["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount", "snapshot_at"]])
        _sleep()
    if not parts:
        raise RuntimeError("stock_current fetched no rows from registry or legacy source")
    fallback_df = pd.concat(parts, ignore_index=True)
    source_name = "legacy"
    try:
        received, coverage = _validate_stock_current_frame(
            fallback_df,
            stock_codes,
            source=source_name,
        )
    except RuntimeError as legacy_error:
        logger.warning(
            "legacy realtime source failed quality validation; trying Sina fallback: %s",
            legacy_error,
        )
        fallback_df = _fetch_sina_stock_current(
            stock_codes,
            _load_stock_short_name_map(engine),
        )
        source_name = "sina"
        received, coverage = _validate_stock_current_frame(
            fallback_df,
            stock_codes,
            source=source_name,
        )
    replace_stock_current_snapshot(engine, _with_etl(fallback_df))
    logger.info(
        "stock current(%s): wrote %d rows, coverage=%.2f%%",
        source_name,
        received,
        coverage * 100,
    )


def step_stock_five(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_market.stock_market import StockMarket

    mk = StockMarket()
    now = _now()
    numeric_cols = [
        "s5","sv5","s4","sv4","s3","sv3","s2","sv2","s1","sv1","b1","bv1","b2","bv2","b3","bv3","b4","bv4","b5","bv5",
    ]

    def _fetch_five(code: str) -> pd.DataFrame | None:
        df = retry_remote(mk.get_market_five, stock_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, numeric_cols)
            df["snapshot_at"] = now
            return df[["stock_code","short_name"] + numeric_cols + ["snapshot_at"]]
        return None

    parts = _concurrent_run(
        stock_codes,
        _fetch_five,
        label="五档盘口",
        log_every=500,
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_snapshot(
        engine,
        frame,
        table_name="sm_stock_five_level",
        requested_codes=stock_codes,
        code_column="stock_code",
        label="stock five-level snapshot",
        coverage_env="SM_STOCK_FIVE_MIN_COVERAGE",
        default_coverage=0.80,
    )


def step_stock_bar(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_market.stock_market import StockMarket

    mk = StockMarket()
    now = _now()

    def _fetch_bar(code: str) -> pd.DataFrame | None:
        df = retry_remote(mk.get_market_bar, stock_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "volume"])
            df["snapshot_at"] = now
            return df[["stock_code", "trade_time", "price", "volume", "bs_type", "snapshot_at"]]
        return None

    parts = _concurrent_run(
        stock_codes,
        _fetch_bar,
        label="逐笔成交",
        log_every=500,
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_snapshot(
        engine,
        frame,
        table_name="sm_stock_bar",
        requested_codes=stock_codes,
        code_column="stock_code",
        label="stock trade-tick snapshot",
        coverage_env="SM_STOCK_BAR_MIN_COVERAGE",
        default_coverage=0.50,
    )


def step_stock_flow_min(engine: Engine, stock_codes: list[str]) -> None:
    if _stock_flow_source("min") == "qmt":
        from integrations.qmt import bridge
        from integrations.qmt.info import to_qmt_stock_symbols

        trade_date = _default_myquant_minute_date(engine)
        qmt_codes = to_qmt_stock_symbols(stock_codes)
        if not qmt_codes:
            raise RuntimeError("个股分时资金流(QMT): no valid QMT stock codes")
        else:
            df = bridge.flow_min(
                qmt_codes,
                trade_date=trade_date,
                start_date=trade_date,
                end_date=trade_date,
                batch_size=int(os.environ.get("QMT_FLOW_MIN_BATCH_SIZE", "200")),
                timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
            )
            if df is not None and not df.empty:
                out = df.copy()
                out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
                out["trade_time"] = pd.to_datetime(out["trade_time"], errors="coerce")
                out["snapshot_at"] = _now()
                cols = ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]
                out = _to_numeric(out, cols)
                _replace_validated_code_date_frame(
                    engine,
                    out[["stock_code", "trade_time"] + cols + ["snapshot_at"]],
                    table_name="sm_stock_capital_flow_min",
                    requested_codes=stock_codes,
                    code_column="stock_code",
                    date_column="trade_time",
                    label="QMT stock minute capital flow",
                    coverage_env="QMT_FLOW_MIN_MIN_COVERAGE",
                    default_coverage=0.80,
                    day_partition=True,
                )
                return
            raise RuntimeError(
                "QMT stock flow minute returned no rows from transactioncount1m; "
                "refusing to fall back to a non-QMT capital-flow source."
            )

    from adata.stock.market.capital_flow.stock_capital_flow import StockCapitalFlow

    cf = StockCapitalFlow()
    now = _now()
    cols = ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]

    def _fetch_flow_min(code: str) -> pd.DataFrame | None:
        df = retry_remote(cf.get_capital_flow_min, stock_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, cols)
            df["snapshot_at"] = now
            return df[["stock_code", "trade_time"] + cols + ["snapshot_at"]]
        return None

    parts = _concurrent_run(
        stock_codes,
        _fetch_flow_min,
        label="分钟资金",
        log_every=500,
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        engine,
        frame,
        table_name="sm_stock_capital_flow_min",
        requested_codes=stock_codes,
        code_column="stock_code",
        date_column="trade_time",
        label="stock minute capital flow",
        coverage_env="SM_FLOW_MIN_MIN_COVERAGE",
        default_coverage=0.50,
        day_partition=True,
    )


def step_stock_flow_daily(engine: Engine, stock_codes: list[str], flow_date: str = "") -> None:
    if _stock_flow_source("daily") == "qmt":
        from integrations.qmt import bridge
        from integrations.qmt.info import to_qmt_stock_symbols

        target_date = (flow_date or "").strip()
        end_date = target_date or datetime.now().strftime("%Y-%m-%d")
        start_date = end_date
        qmt_codes = to_qmt_stock_symbols(stock_codes)
        if not qmt_codes:
            raise RuntimeError("个股日资金流(QMT): no valid QMT stock codes")
        else:
            df = bridge.flow_daily(
                qmt_codes,
                start_date=start_date,
                end_date=end_date,
                batch_size=int(os.environ.get("QMT_FLOW_DAILY_BATCH_SIZE", "250")),
                timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
            )
            if df is not None and not df.empty:
                out = df.copy()
                out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
                out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                if target_date:
                    out = out[out["trade_date"] == target_date[:10]].copy()
                cols = ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]
                out = _to_numeric(out, cols)
                out = out.drop_duplicates(subset=["stock_code", "trade_date"], keep="last")
                _replace_validated_code_date_frame(
                    engine,
                    out[["stock_code", "trade_date"] + cols],
                    table_name="sm_stock_capital_flow_daily",
                    requested_codes=stock_codes,
                    code_column="stock_code",
                    date_column="trade_date",
                    label="QMT stock daily capital flow",
                    coverage_env="QMT_FLOW_DAILY_MIN_COVERAGE",
                    default_coverage=0.80,
                )
                return
            raise RuntimeError(
                "QMT stock flow daily returned no rows from transactioncount1d; "
                "refusing to fall back to a non-QMT capital-flow source."
            )

    from adata.stock.market.capital_flow.stock_capital_flow import StockCapitalFlow

    if flow_date:
        logger.info("日度资金流向：指定日期 %s，将在验证后原子替换该日分区", flow_date)
    cf = StockCapitalFlow()
    cols = ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]
    _fd = flow_date

    def _fetch_flow_daily(code: str) -> pd.DataFrame | None:
        kwargs: dict[str, Any] = {"stock_code": code}
        if _fd:
            kwargs["start_date"] = _fd
            kwargs["end_date"] = _fd
        df = retry_remote(cf.get_capital_flow, **kwargs)
        if df is not None and not df.empty:
            if _fd:
                mask = df["trade_date"].astype(str).str[:10] == _fd[:10]
                df = df.loc[mask].copy()
            if not df.empty:
                df = _to_numeric(df, cols)
                return df[["stock_code", "trade_date"] + cols]
        return None

    parts = _concurrent_run(
        stock_codes,
        _fetch_flow_daily,
        label="日度资金",
        log_every=500,
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        engine,
        frame,
        table_name="sm_stock_capital_flow_daily",
        requested_codes=stock_codes,
        code_column="stock_code",
        date_column="trade_date",
        label="stock daily capital flow",
        coverage_env="SM_FLOW_DAILY_MIN_COVERAGE",
        default_coverage=0.50,
    )


def _concept_ths_instance():
    from adata.stock.market.concepth_market.concept_market_ths import ConceptMarketThs

    return ConceptMarketThs()


def _concept_east_instance():
    from adata.stock.market.concepth_market.concept_market_east import ConceptMarketEase

    return ConceptMarketEase()


def step_concept_ths_kline(
    engine: Engine, concept_codes: list[str], kline_start: str | None = None, kline_end: str | None = None
) -> None:
    ins = _concept_ths_instance()
    k_type = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    start = kline_start or os.environ.get("SM_INDEX_START", "2020-01-01")
    end = kline_end or os.environ.get("SM_INDEX_END")
    cols = ["index_code", "trade_time", "trade_date", "k_type", "open", "close", "high", "low", "volume", "amount", "change", "change_pct"]
    _start, _end, _kt = start, end, k_type

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_concept_ths, index_code=code, k_type=_kt)
        if isinstance(df, Exception):
            raise df
        if df is not None and not df.empty:
            if "trade_date" in df.columns:
                df = df[df["trade_date"] >= _start]
                if _end:
                    df = df[df["trade_date"] <= _end]
            df = _to_numeric(df, ["open", "close", "high", "low", "volume", "amount", "change", "change_pct"])
            df["k_type"] = _kt
            return df[cols]
        return None

    parts = _concurrent_run(
        concept_codes,
        _fetch,
        label="THS概念K线",
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        engine,
        frame,
        table_name="sm_concept_ths_kline",
        requested_codes=concept_codes,
        code_column="index_code",
        date_column="trade_date",
        label="THS concept K-line",
        coverage_env="THS_CONCEPT_KLINE_MIN_COVERAGE",
        default_coverage=0.80,
        identity_columns=("index_code", "trade_date", "k_type"),
        extra_where="k_type = :scope_k_type",
        extra_params={"scope_k_type": k_type},
    )


def step_concept_ths_minute(engine: Engine, concept_codes: list[str]) -> None:
    ins = _concept_ths_instance()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_concept_min_ths, index_code=code)
        if isinstance(df, Exception):
            raise df
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "avg_price", "change", "change_pct", "volume", "amount"])
            df["snapshot_at"] = now
            return df[cols]
        return None

    parts = _concurrent_run(
        concept_codes,
        _fetch,
        label="THS概念分钟",
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        engine,
        frame,
        table_name="sm_concept_ths_minute",
        requested_codes=concept_codes,
        code_column="index_code",
        date_column="trade_time",
        label="THS concept minute",
        coverage_env="THS_CONCEPT_MINUTE_MIN_COVERAGE",
        default_coverage=0.80,
        day_partition=True,
    )


def step_concept_ths_current(engine: Engine, concept_codes: list[str]) -> None:
    ins = _concept_ths_instance()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "open", "price", "high", "low", "volume", "amount", "change", "change_pct", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_concept_current_ths, index_code=code)
        if isinstance(df, Exception):
            raise df
        if df is not None and not df.empty:
            df = _to_numeric(df, ["open", "price", "high", "low", "volume", "amount", "change", "change_pct"])
            df["snapshot_at"] = now
            return df[cols]
        return None

    parts = _concurrent_run(concept_codes, _fetch, label="THS概念实时")
    if not parts:
        raise RuntimeError("no THS concept current rows fetched")
    complete = pd.concat(parts, ignore_index=True).drop_duplicates(
        subset=["index_code"], keep="last"
    )
    requested = {str(code).strip() for code in concept_codes if str(code).strip()}
    received = set(complete["index_code"].astype(str).str.strip())
    coverage = len(received & requested) / max(len(requested), 1)
    min_coverage = min(
        1.0,
        max(0.0, float(os.environ.get("THS_CONCEPT_CURRENT_MIN_COVERAGE", "0.80"))),
    )
    if coverage < min_coverage:
        raise RuntimeError(
            "THS concept current coverage below threshold: "
            f"{len(received & requested)}/{len(requested)} ({coverage:.1%}) "
            f"< {min_coverage:.1%}; preserving previous snapshot"
        )
    _replace_validated_code_snapshot(
        engine,
        complete,
        table_name="sm_concept_ths_current",
        requested_codes=concept_codes,
        code_column="index_code",
        label="THS concept current",
        coverage_env="THS_CONCEPT_CURRENT_MIN_COVERAGE",
        default_coverage=0.80,
    )


def step_concept_east_kline(
    engine: Engine, concept_codes: list[str], kline_start: str | None = None, kline_end: str | None = None
) -> None:
    if _concept_source("kline") == "qmt":
        from integrations.qmt import bridge
        from integrations.qmt.aggregate import aggregate_concept_kline
        from integrations.qmt.info import to_qmt_stock_symbols

        members, _name_map = _read_qmt_concept_meta(engine, concept_codes)
        if members.empty:
            raise RuntimeError("QMT concept K-line has no concept members")
        start = kline_start or os.environ.get("SM_INDEX_START", "2020-01-01")
        end = kline_end or os.environ.get("SM_INDEX_END") or datetime.now().strftime("%Y-%m-%d")
        qmt_codes = to_qmt_stock_symbols(members["stock_code"].astype(str).tolist())
        df = bridge.kline(
            qmt_codes,
            start_date=start,
            end_date=end,
            dividend_type=os.environ.get("QMT_DIVIDEND_TYPE", "none"),
            batch_size=int(os.environ.get("QMT_CONCEPT_KLINE_BATCH_SIZE", os.environ.get("QMT_KLINE_BATCH_SIZE", "300"))),
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if df is None or df.empty:
            raise RuntimeError("QMT concept kline source returned no stock rows")
        out = aggregate_concept_kline(members, df)
        if out.empty:
            raise RuntimeError("QMT concept kline aggregation returned no rows")
        _replace_validated_code_date_frame(
            engine,
            out,
            table_name="sm_concept_east_kline",
            requested_codes=concept_codes,
            code_column="index_code",
            date_column="trade_date",
            label="QMT Eastmoney concept K-line",
            coverage_env="QMT_CONCEPT_KLINE_MIN_COVERAGE",
            default_coverage=0.80,
            identity_columns=("index_code", "trade_date", "k_type"),
            extra_where="k_type = :scope_k_type",
            extra_params={"scope_k_type": int(os.environ.get("SM_INDEX_K_TYPE", "1"))},
        )
        return

    ins = _concept_east_instance()
    k_type = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    start = kline_start or os.environ.get("SM_INDEX_START", "2020-01-01")
    end = kline_end or os.environ.get("SM_INDEX_END")
    cols = ["index_code", "trade_time", "trade_date", "k_type", "open", "close", "high", "low", "volume", "amount", "change", "change_pct"]
    _start, _end, _kt = start, end, k_type

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_concept_east, index_code=code, k_type=_kt)
        if df is not None and not df.empty:
            if "trade_date" in df.columns:
                df = df[df["trade_date"] >= _start]
                if _end:
                    df = df[df["trade_date"] <= _end]
            df = _to_numeric(df, ["open", "close", "high", "low", "volume", "amount", "change", "change_pct"])
            df["k_type"] = _kt
            return df[cols]
        return None

    parts = _concurrent_run(
        concept_codes,
        _fetch,
        label="东财概念K线",
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        engine,
        frame,
        table_name="sm_concept_east_kline",
        requested_codes=concept_codes,
        code_column="index_code",
        date_column="trade_date",
        label="Eastmoney concept K-line",
        coverage_env="EAST_CONCEPT_KLINE_MIN_COVERAGE",
        default_coverage=0.80,
        identity_columns=("index_code", "trade_date", "k_type"),
        extra_where="k_type = :scope_k_type",
        extra_params={"scope_k_type": k_type},
    )


def step_concept_east_minute(engine: Engine, concept_codes: list[str]) -> None:
    if _concept_source("minute") == "qmt":
        from integrations.qmt import bridge
        from integrations.qmt.aggregate import aggregate_concept_minute
        from integrations.qmt.info import to_qmt_stock_symbols

        members, _name_map = _read_qmt_concept_meta(engine, concept_codes)
        if members.empty:
            raise RuntimeError("QMT concept minute has no concept members")
        trade_date = datetime.now().strftime("%Y-%m-%d")
        qmt_codes = to_qmt_stock_symbols(members["stock_code"].astype(str).tolist())
        df = bridge.minute(
            qmt_codes,
            trade_date=trade_date,
            start_date=trade_date,
            end_date=trade_date,
            count=int(os.environ.get("QMT_CONCEPT_MINUTE_COUNT", os.environ.get("QMT_MINUTE_COUNT", "0")) or 0),
            batch_size=int(os.environ.get("QMT_CONCEPT_MINUTE_BATCH_SIZE", os.environ.get("QMT_MINUTE_BATCH_SIZE", "200"))),
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if df is None or df.empty:
            raise RuntimeError("QMT concept minute source returned no stock rows")
        out = aggregate_concept_minute(members, df, snapshot_at=pd.Timestamp(_now()))
        if out.empty:
            raise RuntimeError("QMT concept minute aggregation returned no rows")
        _replace_validated_code_date_frame(
            engine,
            out,
            table_name="sm_concept_east_minute",
            requested_codes=concept_codes,
            code_column="index_code",
            date_column="trade_time",
            label="QMT Eastmoney concept minute",
            coverage_env="QMT_CONCEPT_MINUTE_MIN_COVERAGE",
            default_coverage=0.80,
            day_partition=True,
        )
        return

    ins = _concept_east_instance()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_concept_min_east, index_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "avg_price", "change", "change_pct", "volume", "amount"])
            df["snapshot_at"] = now
            return df[cols]
        return None

    parts = _concurrent_run(
        concept_codes,
        _fetch,
        label="东财概念分钟",
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        engine,
        frame,
        table_name="sm_concept_east_minute",
        requested_codes=concept_codes,
        code_column="index_code",
        date_column="trade_time",
        label="Eastmoney concept minute",
        coverage_env="EAST_CONCEPT_MINUTE_MIN_COVERAGE",
        default_coverage=0.80,
        day_partition=True,
    )


def step_concept_east_current(engine: Engine, concept_codes: list[str]) -> None:
    concept_source = _concept_source("current")
    if concept_source in {"qmt", "bigqmt"}:
        if concept_source == "bigqmt":
            from integrations.bigqmt import bridge
        else:
            from integrations.qmt import bridge
        from integrations.qmt.aggregate import aggregate_concept_current
        from integrations.qmt.info import to_qmt_stock_symbols

        members, _name_map = _read_qmt_concept_meta(engine, concept_codes)
        if members.empty:
            raise RuntimeError(
                "QMT concept current has no concept members; preserving previous snapshot"
            )
        qmt_codes = to_qmt_stock_symbols(members["stock_code"].astype(str).tolist())
        df = bridge.current(
            qmt_codes,
            batch_size=int(os.environ.get("QMT_CONCEPT_CURRENT_BATCH_SIZE", os.environ.get("QMT_CURRENT_BATCH_SIZE", "500"))),
            timeout=int(os.environ.get("QMT_TIMEOUT", "180")),
        )
        if df is None or df.empty:
            raise RuntimeError("QMT concept current source returned no stock rows")
        out = aggregate_concept_current(members, df)
        if out.empty:
            raise RuntimeError("QMT concept current aggregation returned no rows")
        requested = {str(code).strip() for code in concept_codes if str(code).strip()}
        received = set(out["index_code"].astype(str).str.strip())
        coverage = len(received & requested) / max(len(requested), 1)
        min_coverage = min(
            1.0,
            max(0.0, float(os.environ.get("QMT_CONCEPT_CURRENT_MIN_COVERAGE", "0.80"))),
        )
        if coverage < min_coverage:
            raise RuntimeError(
                "QMT concept current coverage below threshold: "
                f"{len(received & requested)}/{len(requested)} ({coverage:.1%}) "
                f"< {min_coverage:.1%}; preserving previous snapshot"
            )
        _replace_validated_code_snapshot(
            engine,
            out,
            table_name="sm_concept_east_current",
            requested_codes=concept_codes,
            code_column="index_code",
            label="QMT Eastmoney concept current",
            coverage_env="QMT_CONCEPT_CURRENT_MIN_COVERAGE",
            default_coverage=0.80,
        )
        return

    ins = _concept_east_instance()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "open", "price", "high", "low", "volume", "amount", "change", "change_pct", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_concept_current_east, index_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["open", "price", "high", "low", "volume", "amount", "change", "change_pct"])
            if "trade_date" not in df.columns:
                df["trade_date"] = pd.to_datetime(df["trade_time"]).dt.strftime("%Y-%m-%d")
            df["snapshot_at"] = now
            return df[cols]
        return None

    parts = _concurrent_run(concept_codes, _fetch, label="东财概念实时")
    if not parts:
        raise RuntimeError(
            "no Eastmoney concept current rows fetched; preserving previous snapshot"
        )
    complete = pd.concat(parts, ignore_index=True).drop_duplicates(
        subset=["index_code"], keep="last"
    )
    requested = {str(code).strip() for code in concept_codes if str(code).strip()}
    received = set(complete["index_code"].astype(str).str.strip())
    coverage = len(received & requested) / max(len(requested), 1)
    min_coverage = min(
        1.0,
        max(0.0, float(os.environ.get("EAST_CONCEPT_CURRENT_MIN_COVERAGE", "0.80"))),
    )
    if coverage < min_coverage:
        raise RuntimeError(
            "Eastmoney concept current coverage below threshold: "
            f"{len(received & requested)}/{len(requested)} ({coverage:.1%}) "
            f"< {min_coverage:.1%}; preserving previous snapshot"
        )
    _replace_validated_code_snapshot(
        engine,
        complete,
        table_name="sm_concept_east_current",
        requested_codes=concept_codes,
        code_column="index_code",
        label="Eastmoney concept current",
        coverage_env="EAST_CONCEPT_CURRENT_MIN_COVERAGE",
        default_coverage=0.80,
    )


def step_concept_flow_east(engine: Engine) -> None:
    if _concept_source("flow") == "qmt":
        from integrations.qmt import bridge
        from integrations.qmt.aggregate import aggregate_concept_kline
        from integrations.qmt.info import to_qmt_stock_symbols

        concept_codes = read_concept_east_codes(engine)
        members, name_map = _read_qmt_concept_meta(engine, concept_codes)
        if members.empty:
            raise RuntimeError("QMT concept flow has no concept members")

        unique_stock_codes = members["stock_code"].astype(str).str.zfill(6).drop_duplicates().tolist()
        qmt_codes = to_qmt_stock_symbols(unique_stock_codes)
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - pd.Timedelta(days=20)).strftime("%Y-%m-%d")

        flow_df = bridge.flow_daily(
            qmt_codes,
            start_date=start_date,
            end_date=end_date,
            batch_size=int(os.environ.get("QMT_FLOW_DAILY_BATCH_SIZE", "250")),
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if flow_df is None or flow_df.empty:
            logger.warning("QMT concept flow source returned no stock flow rows; concept flow will be skipped.")
            return

        kline_df = bridge.kline(
            qmt_codes,
            start_date=start_date,
            end_date=end_date,
            dividend_type=os.environ.get("QMT_DIVIDEND_TYPE", "none"),
            batch_size=int(os.environ.get("QMT_CONCEPT_KLINE_BATCH_SIZE", os.environ.get("QMT_KLINE_BATCH_SIZE", "300"))),
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if kline_df is None or kline_df.empty:
            raise RuntimeError("QMT concept flow source returned no stock kline rows")

        flow_df = flow_df.copy()
        flow_df["stock_code"] = flow_df["stock_code"].astype(str).str.zfill(6)
        flow_df["trade_date"] = pd.to_datetime(flow_df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        flow_df = _to_numeric(flow_df, ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"])
        flow_df = flow_df.dropna(subset=["trade_date"])
        if flow_df.empty:
            logger.warning("QMT concept flow rows became empty after normalization; concept flow will be skipped.")
            return

        kline_df = kline_df.copy()
        kline_df["stock_code"] = kline_df["stock_code"].astype(str).str.zfill(6)
        kline_df["trade_date"] = pd.to_datetime(kline_df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        kline_df = _to_numeric(kline_df, ["amount", "change_pct"])
        concept_kline = aggregate_concept_kline(members, kline_df)

        stock_name_map = _load_stock_short_name_map(engine)
        all_dates = sorted(flow_df["trade_date"].dropna().astype(str).unique().tolist())
        if not all_dates:
            raise RuntimeError("QMT concept flow has no trade dates")

        rows: list[dict[str, Any]] = []
        now = _now()
        for days_type in (1, 5, 10):
            window_dates = all_dates[-days_type:]
            window_flow = flow_df[flow_df["trade_date"].isin(window_dates)].copy()
            if window_flow.empty:
                continue
            stock_flow = (
                window_flow.groupby("stock_code", as_index=False)[
                    ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]
                ]
                .sum()
            )
            stock_amount = (
                kline_df[kline_df["trade_date"].isin(window_dates)]
                .groupby("stock_code", as_index=False)[["amount"]]
                .sum()
                .rename(columns={"amount": "window_amount"})
            )
            merged = members.merge(stock_flow, on="stock_code", how="left").merge(stock_amount, on="stock_code", how="left")
            merged = merged.fillna(
                {
                    "main_net_inflow": 0,
                    "max_net_inflow": 0,
                    "lg_net_inflow": 0,
                    "mid_net_inflow": 0,
                    "sm_net_inflow": 0,
                    "window_amount": 0,
                }
            )

            for concept_code, frame in merged.groupby("concept_code", sort=False):
                top = frame.sort_values("main_net_inflow", ascending=False).iloc[0]
                amount_total = float(pd.to_numeric(frame["window_amount"], errors="coerce").fillna(0).sum())
                main_net = float(pd.to_numeric(frame["main_net_inflow"], errors="coerce").fillna(0).sum())
                max_net = float(pd.to_numeric(frame["max_net_inflow"], errors="coerce").fillna(0).sum())
                lg_net = float(pd.to_numeric(frame["lg_net_inflow"], errors="coerce").fillna(0).sum())
                mid_net = float(pd.to_numeric(frame["mid_net_inflow"], errors="coerce").fillna(0).sum())
                sm_net = float(pd.to_numeric(frame["sm_net_inflow"], errors="coerce").fillna(0).sum())
                rows.append(
                    {
                        "days_type": days_type,
                        "index_code": concept_code,
                        "index_name": name_map.get(concept_code, concept_code),
                        "change_pct": _concept_snapshot_change_pct(concept_kline, concept_code, days_type),
                        "main_net_inflow": main_net,
                        "main_net_inflow_rate": (main_net / amount_total * 100) if amount_total > 0 else None,
                        "max_net_inflow": max_net,
                        "max_net_inflow_rate": (max_net / amount_total * 100) if amount_total > 0 else None,
                        "lg_net_inflow": lg_net,
                        "lg_net_inflow_rate": (lg_net / amount_total * 100) if amount_total > 0 else None,
                        "mid_net_inflow": mid_net,
                        "mid_net_inflow_rate": (mid_net / amount_total * 100) if amount_total > 0 else None,
                        "sm_net_inflow": sm_net,
                        "sm_net_inflow_rate": (sm_net / amount_total * 100) if amount_total > 0 else None,
                        "stock_code": str(top["stock_code"]).zfill(6),
                        "stock_name": stock_name_map.get(str(top["stock_code"]).zfill(6), ""),
                        "snapshot_at": now,
                    }
                )

        if not rows:
            raise RuntimeError("QMT concept flow aggregation returned no rows")
        _replace_validated_code_snapshot(
            engine,
            pd.DataFrame(rows),
            table_name="sm_concept_capital_flow_east",
            requested_codes=concept_codes,
            code_column="index_code",
            label="QMT Eastmoney concept capital-flow snapshot",
            coverage_env="QMT_CONCEPT_FLOW_MIN_COVERAGE",
            default_coverage=0.80,
        )
        return

    from adata.stock.market.concept_capital_flow.capital_flow_east import CapitalFlowEast

    ins = CapitalFlowEast()
    now = _now()
    parts: list[pd.DataFrame] = []
    for d in (1, 5, 10):
        df = retry_remote(ins.all_capital_flow_east, days_type=d)
        if df is not None and not df.empty:
            num_cols = [
                "change_pct","main_net_inflow","main_net_inflow_rate","max_net_inflow","max_net_inflow_rate",
                "lg_net_inflow","lg_net_inflow_rate","mid_net_inflow","mid_net_inflow_rate","sm_net_inflow","sm_net_inflow_rate",
            ]
            df = _to_numeric(df, num_cols)
            df["days_type"] = d
            df["snapshot_at"] = now
            parts.append(
                df[
                    [
                        "days_type","index_code","index_name","change_pct","main_net_inflow","main_net_inflow_rate",
                        "max_net_inflow","max_net_inflow_rate","lg_net_inflow","lg_net_inflow_rate",
                        "mid_net_inflow","mid_net_inflow_rate","sm_net_inflow","sm_net_inflow_rate",
                        "stock_code","stock_name","snapshot_at",
                    ]
                ]
            )
        _sleep()
    if len(parts) != 3:
        raise RuntimeError(
            "Eastmoney concept capital flow did not return all 1/5/10-day snapshots; "
            "preserving previous data"
        )
    concept_codes = read_concept_east_codes(engine)
    _replace_validated_code_snapshot(
        engine,
        pd.concat(parts, ignore_index=True),
        table_name="sm_concept_capital_flow_east",
        requested_codes=concept_codes,
        code_column="index_code",
        label="Eastmoney concept capital-flow snapshot",
        coverage_env="EAST_CONCEPT_FLOW_MIN_COVERAGE",
        default_coverage=0.80,
    )


def _legacy_step_index_kline(engine: Engine, index_codes: list[str], kline_start: str | None = None, kline_end: str | None = None) -> None:
    index_source = _index_source("kline")
    if index_source in {"qmt", "bigqmt"}:
        if index_source == "bigqmt":
            from integrations.bigqmt import bridge
        else:
            from integrations.qmt import bridge
        from integrations.qmt.info import to_qmt_index_symbols

        start = kline_start or os.environ.get("SM_INDEX_START", "2020-01-01")
        end = kline_end or os.environ.get("SM_INDEX_END") or datetime.now().strftime("%Y-%m-%d")
        qmt_codes = to_qmt_index_symbols(index_codes)
        if not qmt_codes:
            logger.warning("指数K线(QMT): no valid index codes")
            return
        df = bridge.kline(
            qmt_codes,
            start_date=start,
            end_date=end,
            dividend_type="none",
            batch_size=int(os.environ.get("QMT_INDEX_KLINE_BATCH_SIZE", os.environ.get("QMT_KLINE_BATCH_SIZE", "300"))),
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if df is None or df.empty:
            raise RuntimeError("QMT index kline returned no rows")
        out = pd.DataFrame()
        out["index_code"] = df["stock_code"].astype(str).str.zfill(6)
        out["trade_time"] = df["trade_time"]
        out["trade_date"] = df["trade_date"]
        out["k_type"] = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
        out["open"] = pd.to_numeric(df.get("open"), errors="coerce")
        out["close"] = pd.to_numeric(df.get("close"), errors="coerce")
        out["high"] = pd.to_numeric(df.get("high"), errors="coerce")
        out["low"] = pd.to_numeric(df.get("low"), errors="coerce")
        out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
        out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
        out["change"] = pd.to_numeric(df.get("change"), errors="coerce")
        out["change_pct"] = pd.to_numeric(df.get("change_pct"), errors="coerce")
        _replace_validated_code_date_frame(
            get_kline_engine(),
            out,
            table_name="sm_index_kline",
            requested_codes=index_codes,
            code_column="index_code",
            date_column="trade_date",
            label="legacy QMT index K-line",
            coverage_env="QMT_INDEX_KLINE_MIN_COVERAGE",
            default_coverage=0.80,
            identity_columns=("index_code", "trade_date", "k_type"),
            extra_where="k_type=:scope_k_type",
            extra_params={"scope_k_type": int(os.environ.get("SM_INDEX_K_TYPE", "1"))},
        )
        return

    from adata.stock.market.index_market.market_index import StockMarketIndex

    ins = StockMarketIndex()
    start = kline_start or os.environ.get("SM_INDEX_START", "2020-01-01")
    end = kline_end or os.environ.get("SM_INDEX_END")
    k_type = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    cols = ["index_code", "trade_time", "trade_date", "k_type", "open", "close", "high", "low", "volume", "amount", "change", "change_pct"]
    _start, _end, _kt = start, end, k_type

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_index, index_code=code, start_date=_start, k_type=_kt)
        if df is not None and not df.empty:
            if _end and "trade_date" in df.columns:
                df = df[df["trade_date"] <= _end]
            df = _to_numeric(df, ["open", "close", "high", "low", "volume", "amount", "change", "change_pct"])
            df["k_type"] = _kt
            return df[cols]
        return None

    parts = _concurrent_run(
        index_codes,
        _fetch,
        label="指数K线",
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        get_kline_engine(),
        frame,
        table_name="sm_index_kline",
        requested_codes=index_codes,
        code_column="index_code",
        date_column="trade_date",
        label="legacy index K-line",
        coverage_env="INDEX_KLINE_MIN_COVERAGE",
        default_coverage=0.80,
        identity_columns=("index_code", "trade_date", "k_type"),
        extra_where="k_type=:scope_k_type",
        extra_params={"scope_k_type": k_type},
    )


def _legacy_step_index_minute(engine: Engine, index_codes: list[str]) -> None:
    index_source = _index_source("minute")
    if index_source in {"qmt", "bigqmt"}:
        if index_source == "bigqmt":
            from integrations.bigqmt import bridge
        else:
            from integrations.qmt import bridge
        from integrations.qmt.info import to_qmt_index_symbols

        qmt_codes = to_qmt_index_symbols(index_codes)
        if not qmt_codes:
            logger.warning("指数分钟(QMT): no valid index codes")
            return
        trade_date = _default_myquant_minute_date(engine)
        df = bridge.minute(
            qmt_codes,
            trade_date=trade_date,
            start_date=trade_date,
            end_date=trade_date,
            count=int(os.environ.get("QMT_INDEX_MINUTE_COUNT", os.environ.get("QMT_MINUTE_COUNT", "0")) or 0),
            batch_size=int(os.environ.get("QMT_INDEX_MINUTE_BATCH_SIZE", os.environ.get("QMT_MINUTE_BATCH_SIZE", "200"))),
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if df is None or df.empty:
            raise RuntimeError("QMT index minute returned no rows")
        out = pd.DataFrame()
        out["index_code"] = df["stock_code"].astype(str).str.zfill(6)
        out["trade_time"] = df["trade_time"]
        out["trade_date"] = df["trade_date"]
        out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
        out["avg_price"] = None
        out["change"] = pd.to_numeric(df.get("change"), errors="coerce")
        out["change_pct"] = pd.to_numeric(df.get("change_pct"), errors="coerce")
        out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
        out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
        out["snapshot_at"] = _now()
        _replace_validated_code_date_frame(
            get_kline_engine(),
            out,
            table_name="sm_index_minute",
            requested_codes=index_codes,
            code_column="index_code",
            date_column="trade_time",
            label="legacy QMT index minute",
            coverage_env="QMT_INDEX_MINUTE_MIN_COVERAGE",
            default_coverage=0.80,
            day_partition=True,
        )
        return

    from adata.stock.market.index_market.market_index import StockMarketIndex

    ins = StockMarketIndex()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_index_min, index_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "avg_price", "change", "change_pct", "volume", "amount"])
            df["snapshot_at"] = now
            return df[cols]
        return None

    parts = _concurrent_run(
        index_codes,
        _fetch,
        label="指数分钟",
        fail_on_error=True,
    )
    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        get_kline_engine(),
        frame,
        table_name="sm_index_minute",
        requested_codes=index_codes,
        code_column="index_code",
        date_column="trade_time",
        label="legacy index minute",
        coverage_env="INDEX_MINUTE_MIN_COVERAGE",
        default_coverage=0.80,
        day_partition=True,
    )


def _normalize_index_kline_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["index_code"] = df["stock_code"].astype(str).str.zfill(6)
    out["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    out["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    out["k_type"] = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    for column in ("open", "close", "high", "low", "volume", "amount", "change", "change_pct"):
        out[column] = pd.to_numeric(df.get(column), errors="coerce")
    out = out.dropna(subset=["index_code", "trade_time", "trade_date", "open", "close", "high", "low"])
    return out[
        (out["open"] > 0)
        & (out["close"] > 0)
        & (out["high"] >= out[["open", "close", "low"]].max(axis=1))
        & (out["low"] <= out[["open", "close", "high"]].min(axis=1))
    ]


_TENCENT_INDEX_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/kline/kline"
_EAST_INDEX_KLINE_URLS = (
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://33.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://63.push2his.eastmoney.com/api/qt/stock/kline/get",
)
_CNINDEX_INDEX_KLINE_URL = (
    "https://hq.cnindex.com.cn/market/market/getIndexDailyDataWithDataFormat"
)
_CNINDEX_INDEX_CURRENT_URL = (
    "https://hq.cnindex.com.cn/market/market/getIndexLatestRealTimeData"
)


def _tencent_index_symbol(index_code: str) -> str:
    code = str(index_code or "").strip().zfill(6)
    return f"sz{code}" if code.startswith(("399", "970", "980")) else f"sh{code}"


def _fetch_tencent_index_kline(
    index_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch one index's unadjusted daily bars from Tencent.

    One prior calendar window is requested so ``change`` and ``change_pct`` can
    be derived from the provider's own previous close.  Tencent reports index
    volume in shares, matching the existing QMT rows in ``sm_index_kline``.
    """
    code = str(index_code or "").strip().zfill(6)
    symbol = _tencent_index_symbol(code)
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    fetch_start = (start_ts - timedelta(days=14)).strftime("%Y-%m-%d")
    span_days = max(1, (end_ts - pd.Timestamp(fetch_start)).days)
    count = min(2000, max(30, span_days * 2))
    request_params = {"param": f"{symbol},day,{fetch_start},{end_ts:%Y-%m-%d},{count}"}
    attempts = max(1, int(os.environ.get("TENCENT_INDEX_KLINE_ATTEMPTS", "5")))
    last_error: Exception | None = None
    payload: dict[str, Any] = {}
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                _TENCENT_INDEX_KLINE_URL,
                params=request_params,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=float(os.environ.get("TENCENT_INDEX_KLINE_TIMEOUT", "20")),
            )
            response.raise_for_status()
            payload = response.json()
            break
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(0.25 * attempt)
    if not payload and last_error:
        raise last_error
    if int(payload.get("code") or 0) != 0:
        return pd.DataFrame()
    rows = (((payload.get("data") or {}).get(symbol) or {}).get("day") or [])
    records: list[dict[str, Any]] = []
    for values in rows:
        if not isinstance(values, list) or len(values) < 6:
            continue
        try:
            records.append(
                {
                    "index_code": code,
                    "trade_date": str(values[0])[:10],
                    "open": float(values[1]),
                    "close": float(values[2]),
                    "high": float(values[3]),
                    "low": float(values[4]),
                    "volume": float(values[5]),
                }
            )
        except (TypeError, ValueError):
            continue
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame(records).drop_duplicates(subset=["trade_date"], keep="last")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["trade_date"]).sort_values("trade_date")
    frame["pre_close"] = frame["close"].shift(1)
    frame["change"] = frame["close"] - frame["pre_close"]
    frame["change_pct"] = frame["change"] / frame["pre_close"] * 100.0
    frame = frame[(frame["trade_date"] >= start_ts) & (frame["trade_date"] <= end_ts)].copy()
    if frame.empty:
        return frame
    frame["trade_time"] = frame["trade_date"] + pd.Timedelta(hours=15)
    frame["k_type"] = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    frame["amount"] = None
    return frame[
        [
            "index_code",
            "trade_time",
            "trade_date",
            "k_type",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount",
            "change",
            "change_pct",
        ]
    ]


def _fetch_east_index_kline(
    index_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch a second, amount-bearing daily-bar copy from Eastmoney."""
    code = str(index_code or "").strip().zfill(6)
    secid = f"0.{code}" if code.startswith(("399", "970", "980")) else f"1.{code}"
    params = {
        "secid": secid,
        "klt": "101",
        "fqt": "0",
        "beg": str(start_date).replace("-", ""),
        "end": str(end_date).replace("-", ""),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
    }
    last_error: Exception | None = None
    payload: dict[str, Any] = {}
    with requests.Session() as session:
        session.trust_env = os.environ.get("SM_TRUST_ENV_PROXY") == "1"
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://quote.eastmoney.com/",
            }
        )
        for url in _EAST_INDEX_KLINE_URLS:
            try:
                response = session.get(
                    url,
                    params=params,
                    timeout=float(os.environ.get("EAST_INDEX_KLINE_TIMEOUT", "15")),
                )
                response.raise_for_status()
                payload = response.json()
                if ((payload.get("data") or {}).get("klines") or []):
                    break
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                continue
    raw_rows = ((payload.get("data") or {}).get("klines") or [])
    if not raw_rows:
        if last_error:
            raise last_error
        return pd.DataFrame()
    records: list[dict[str, Any]] = []
    for raw in raw_rows:
        values = str(raw).split(",")
        if len(values) < 10:
            continue
        try:
            records.append(
                {
                    "index_code": code,
                    "trade_date": str(values[0])[:10],
                    "open": float(values[1]),
                    "close": float(values[2]),
                    "high": float(values[3]),
                    "low": float(values[4]),
                    "volume": float(values[5]),
                    "amount": float(values[6]),
                    "change_pct": float(values[8]),
                    "change": float(values[9]),
                }
            )
        except (TypeError, ValueError):
            continue
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame(records).drop_duplicates(subset=["trade_date"], keep="last")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["trade_date"]).sort_values("trade_date")
    frame["trade_time"] = frame["trade_date"] + pd.Timedelta(hours=15)
    frame["k_type"] = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    return frame[
        [
            "index_code",
            "trade_time",
            "trade_date",
            "k_type",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount",
            "change",
            "change_pct",
        ]
    ]


def _fetch_cnindex_index_kline(
    index_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch daily bars from the official Shenzhen Securities Information source.

    The CNINDEX endpoint publishes amount in CNY 100 millions and volume in
    millions of shares.  Convert both to their base units before persisting.
    """
    code = str(index_code or "").strip().zfill(6)
    params = {
        "indexCode": code,
        "startDate": str(start_date)[:10],
        "endDate": str(end_date)[:10],
        "frequency": "day",
    }
    attempts = max(1, int(os.environ.get("CNINDEX_KLINE_ATTEMPTS", "4")))
    last_error: Exception | None = None
    payload: dict[str, Any] = {}
    with requests.Session() as session:
        session.trust_env = os.environ.get("SM_TRUST_ENV_PROXY") == "1"
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.cnindex.com.cn/",
            }
        )
        for attempt in range(1, attempts + 1):
            try:
                response = session.get(
                    _CNINDEX_INDEX_KLINE_URL,
                    params=params,
                    timeout=float(os.environ.get("CNINDEX_KLINE_TIMEOUT", "20")),
                )
                response.raise_for_status()
                payload = response.json()
                break
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt < attempts:
                    time.sleep(0.25 * attempt)
    if not payload and last_error:
        raise last_error
    if int(payload.get("code") or 0) != 200:
        return pd.DataFrame()
    data = payload.get("data") or {}
    fields = data.get("item") or []
    raw_rows = data.get("data") or []
    if not isinstance(fields, list) or not isinstance(raw_rows, list):
        return pd.DataFrame()

    def _number(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    records: list[dict[str, Any]] = []
    for values in raw_rows:
        if not isinstance(values, list):
            continue
        raw = dict(zip(fields, values))
        trade_date = str(raw.get("timestamp") or "")[:10]
        open_price = _number(raw.get("open"))
        close_price = _number(raw.get("close"))
        high_price = _number(raw.get("high"))
        low_price = _number(raw.get("low"))
        if not trade_date or None in (open_price, close_price, high_price, low_price):
            continue
        change_pct_text = str(raw.get("percent") or "").strip().rstrip("%")
        change_pct = _number(change_pct_text)
        volume = _number(raw.get("volume"))
        amount = _number(raw.get("amount"))
        records.append(
            {
                "index_code": code,
                "trade_date": trade_date,
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "volume": None if volume is None else round(volume * 1_000_000.0),
                "amount": None if amount is None else round(amount * 100_000_000.0),
                "change": _number(raw.get("chg")),
                "change_pct": change_pct,
            }
        )
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame(records).drop_duplicates(subset=["trade_date"], keep="last")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["trade_date"]).sort_values("trade_date")
    frame = frame[
        (frame["open"] > 0)
        & (frame["close"] > 0)
        & (frame["high"] >= frame[["open", "close", "low"]].max(axis=1))
        & (frame["low"] <= frame[["open", "close", "high"]].min(axis=1))
    ].copy()
    if frame.empty:
        return frame
    frame["trade_time"] = frame["trade_date"] + pd.Timedelta(hours=15)
    frame["k_type"] = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    return frame[
        [
            "index_code",
            "trade_time",
            "trade_date",
            "k_type",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount",
            "change",
            "change_pct",
        ]
    ]


def _fetch_cnindex_index_current(
    index_code: str,
    snapshot_at: datetime,
) -> pd.DataFrame:
    """Fetch one real-time CNI index row from the official publisher."""
    code = str(index_code or "").strip().zfill(6)
    attempts = max(1, int(os.environ.get("CNINDEX_CURRENT_ATTEMPTS", "4")))
    payload: dict[str, Any] = {}
    last_error: Exception | None = None
    with requests.Session() as session:
        session.trust_env = os.environ.get("SM_TRUST_ENV_PROXY") == "1"
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.cnindex.com.cn/",
            }
        )
        for attempt in range(1, attempts + 1):
            try:
                response = session.get(
                    _CNINDEX_INDEX_CURRENT_URL,
                    params={"indexCode": code, "t": int(time.time() * 1000)},
                    timeout=float(os.environ.get("CNINDEX_CURRENT_TIMEOUT", "20")),
                )
                response.raise_for_status()
                payload = response.json()
                break
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt < attempts:
                    time.sleep(0.25 * attempt)
    if not payload and last_error:
        raise last_error
    if int(payload.get("code") or 0) != 200:
        return pd.DataFrame()
    data = payload.get("data") or {}
    fields = data.get("item") or []
    raw_rows = data.get("data") or []
    if not isinstance(fields, list) or not isinstance(raw_rows, list) or not raw_rows:
        return pd.DataFrame()
    raw = dict(zip(fields, raw_rows[-1]))

    def _number(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    timestamp = _number(raw.get("timestamp"))
    price = _number(raw.get("current"))
    if timestamp is None or price is None or price <= 0:
        return pd.DataFrame()
    trade_time = (
        pd.to_datetime(timestamp, unit="ms", utc=True)
        .tz_convert("Asia/Shanghai")
        .tz_localize(None)
    )
    change_pct = _number(raw.get("percent"))
    volume = _number(raw.get("volume"))
    amount = _number(raw.get("amount"))
    row = {
        "index_code": code,
        "trade_time": trade_time,
        "trade_date": trade_time.strftime("%Y-%m-%d"),
        "open": _number(raw.get("open")),
        "price": price,
        "high": _number(raw.get("high")),
        "low": _number(raw.get("low")),
        "volume": None if volume is None else round(volume),
        "amount": None if amount is None else round(amount, 2),
        "change": _number(raw.get("chg")),
        "change_pct": None if change_pct is None else change_pct * 100.0,
        "snapshot_at": snapshot_at,
    }
    return pd.DataFrame([row])


def _fetch_verified_external_index_kline(
    index_code: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, str]:
    """Cross-check Eastmoney OHLCV against Tencent and retain Eastmoney amount."""
    code = str(index_code or "").strip().zfill(6)
    if code.startswith(("970", "980")):
        try:
            official = _fetch_cnindex_index_kline(code, start_date, end_date)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Official CNINDEX K-line unavailable for %s: %s", code, exc)
        else:
            if not official.empty:
                return official, "cnindex_official"

    tencent = _fetch_tencent_index_kline(index_code, start_date, end_date)
    try:
        east = _fetch_east_index_kline(index_code, start_date, end_date)
    except Exception as exc:  # pylint: disable=broad-except
        logger.debug("Eastmoney index K-line unavailable for %s: %s", index_code, exc)
        east = pd.DataFrame()
    if tencent.empty:
        if not east.empty:
            return east, "east_only"
        try:
            official = _fetch_cnindex_index_kline(code, start_date, end_date)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Official CNINDEX K-line unavailable for %s: %s", code, exc)
            official = pd.DataFrame()
        return official, "cnindex_official" if not official.empty else "unavailable"
    if east.empty:
        return tencent, "tencent_only"
    merged = tencent.merge(
        east,
        on=["index_code", "trade_date"],
        how="inner",
        suffixes=("_tencent", "_east"),
    )
    if len(merged) != len(tencent) or len(merged) != len(east):
        raise RuntimeError(
            f"index K-line date coverage mismatch for {index_code}: "
            f"Tencent={len(tencent)} Eastmoney={len(east)} matched={len(merged)}"
        )
    for column in ("open", "close", "high", "low", "volume"):
        left = pd.to_numeric(merged[f"{column}_tencent"], errors="coerce")
        right = pd.to_numeric(merged[f"{column}_east"], errors="coerce")
        if not np.allclose(left, right, rtol=0, atol=1e-6, equal_nan=False):
            raise RuntimeError(f"index K-line {column} mismatch for {index_code}")
    return east, "cross_checked"


def _latest_index_kline_date(engine: Engine) -> str:
    try:
        frame = read_frame_direct(text("SELECT MAX(trade_date) AS trade_date FROM sm_index_kline"), engine)
    except Exception as exc:  # pragma: no cover - defensive database fallback
        logger.warning("Unable to read latest index K-line date: %s", exc)
        return ""
    if frame is None or frame.empty or pd.isna(frame.iloc[0].get("trade_date")):
        return ""
    return str(frame.iloc[0]["trade_date"])[:10]


def _latest_completed_stock_kline_date(engine: Engine) -> str:
    """Return the latest completed stock daily-bar date used as index cutoff."""
    try:
        frame = read_frame_direct(
            text(
                "SELECT trade_date FROM sm_stock_kline "
                "WHERE k_type = 1 AND adjust_type = 0 "
                "ORDER BY trade_date DESC LIMIT 1"
            ),
            engine,
        )
    except Exception as exc:  # pragma: no cover - defensive database fallback
        logger.warning("Unable to read latest completed stock K-line date: %s", exc)
        return ""
    if frame is None or frame.empty or pd.isna(frame.iloc[0].get("trade_date")):
        return ""
    return str(frame.iloc[0]["trade_date"])[:10]


def _normalize_index_minute_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["index_code"] = df["stock_code"].astype(str).str.zfill(6)
    out["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    out["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    out["avg_price"] = None
    for column in ("change", "change_pct", "volume", "amount"):
        out[column] = pd.to_numeric(df.get(column), errors="coerce")
    out["snapshot_at"] = _now()
    out = out.dropna(subset=["index_code", "trade_time", "trade_date", "price"])
    return out[(out["price"] > 0) & (out["volume"] >= 0) & (out["amount"] >= 0)]


def step_index_kline(
    engine: Engine,
    index_codes: list[str],
    kline_start: str | None = None,
    kline_end: str | None = None,
) -> None:
    """Synchronize index K-lines without deleting the last good table first."""
    history_engine = get_kline_engine()
    index_source = _index_source("kline")
    if kline_start:
        start = kline_start
    elif index_source in {"tencent", "qq"}:
        start = _latest_index_kline_date(history_engine) or os.environ.get("SM_INDEX_START", "2020-01-01")
    else:
        start = os.environ.get("SM_INDEX_START", "2020-01-01")
    if kline_end:
        end = kline_end
    elif index_source in {"tencent", "qq"}:
        # Never compare or persist an unfinished intraday daily bar.  The
        # stock daily table is the canonical completed-session watermark.
        end = (
            _latest_completed_stock_kline_date(history_engine)
            or os.environ.get("SM_INDEX_END")
            or datetime.now().strftime("%Y-%m-%d")
        )
    else:
        end = os.environ.get("SM_INDEX_END") or datetime.now().strftime("%Y-%m-%d")
    if pd.Timestamp(start) > pd.Timestamp(end):
        start = end
    if index_source in {"qmt", "bigqmt"}:
        if index_source == "bigqmt":
            from integrations.bigqmt import bridge
        else:
            from integrations.qmt import bridge
        from integrations.qmt.info import to_qmt_index_symbols

        qmt_codes = to_qmt_index_symbols(index_codes)
        if not qmt_codes:
            raise RuntimeError("QMT index K-line has no valid symbols")
        batch_size = max(5, int(os.environ.get("QMT_PRODUCTION_INDEX_KLINE_BATCH_SIZE", "40")))
        timeout = int(os.environ.get("QMT_INDEX_KLINE_TIMEOUT", os.environ.get("QMT_TIMEOUT", "120")))
        min_coverage = min(1.0, max(0.0, float(os.environ.get("QMT_INDEX_KLINE_MIN_COVERAGE", "0.50"))))
        total_batches = (len(qmt_codes) + batch_size - 1) // batch_size
        received_codes: set[str] = set()
        staged_parts: list[pd.DataFrame] = []
        for batch_no, batch in enumerate(_chunked(qmt_codes, batch_size), start=1):
            raw = bridge.kline(
                batch,
                start_date=start,
                end_date=end,
                dividend_type="none",
                batch_size=batch_size,
                timeout=timeout,
            )
            out = _normalize_index_kline_frame(raw)
            if out.empty:
                logger.warning("QMT index K batch %d/%d returned no valid rows", batch_no, total_batches)
                continue
            staged_parts.append(out)
            received_codes.update(out["index_code"].astype(str).unique().tolist())
            logger.info(
                "QMT index K staged batch %d/%d: rows=%d cumulative_codes=%d",
                batch_no,
                total_batches,
                len(out),
                len(received_codes),
            )
        coverage = len(received_codes) / max(len(qmt_codes), 1)
        if not staged_parts or coverage < min_coverage:
            raise RuntimeError(
                f"QMT index kline coverage below threshold: {len(received_codes)}/{len(qmt_codes)} "
                f"({coverage:.1%}) < {min_coverage:.1%}"
            )
        complete = pd.concat(staged_parts, ignore_index=True)
        written = _replace_validated_code_date_frame(
            history_engine,
            complete,
            table_name="sm_index_kline",
            requested_codes=index_codes,
            code_column="index_code",
            date_column="trade_date",
            label="QMT index K-line",
            coverage_env="QMT_INDEX_KLINE_MIN_COVERAGE",
            default_coverage=0.50,
            identity_columns=("index_code", "trade_date", "k_type"),
            extra_where="k_type=:scope_k_type",
            extra_params={
                "scope_k_type": int(os.environ.get("SM_INDEX_K_TYPE", "1"))
            },
        )
        logger.info("QMT index K complete: rows=%d coverage=%.2f%%", written, coverage * 100)
        return

    if index_source in {"tencent", "qq"}:
        workers = max(1, min(32, int(os.environ.get("TENCENT_INDEX_KLINE_WORKERS", "16"))))
        parts: list[pd.DataFrame] = []
        errors: list[str] = []
        failed_codes: list[str] = []
        source_counts: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_fetch_verified_external_index_kline, code, start, end): str(code).zfill(6)
                for code in index_codes
            }
            for future in as_completed(futures):
                code = futures[future]
                try:
                    frame, source_status = future.result()
                except Exception as exc:  # pylint: disable=broad-except
                    errors.append(f"{code}:{exc}")
                    failed_codes.append(code)
                    continue
                source_counts[source_status] = source_counts.get(source_status, 0) + 1
                if frame is not None and not frame.empty:
                    parts.append(frame)
        if failed_codes:
            logger.warning(
                "Retrying %d transient index K-line failures after the first pass: %s",
                len(failed_codes),
                failed_codes[:10],
            )
            retry_errors: list[str] = []
            for code in failed_codes:
                try:
                    frame, source_status = _fetch_verified_external_index_kline(code, start, end)
                except Exception as exc:  # pylint: disable=broad-except
                    retry_errors.append(f"{code}:{exc}")
                    continue
                source_counts[source_status] = source_counts.get(source_status, 0) + 1
                if frame is not None and not frame.empty:
                    parts.append(frame)
            errors = retry_errors
        if not parts:
            raise RuntimeError(
                "Tencent index K-line returned no rows"
                + (f"; errors={errors[:3]}" if errors else "")
            )
        complete = pd.concat(
            [frame.dropna(axis=1, how="all") for frame in parts],
            ignore_index=True,
        )
        received_codes = set(complete["index_code"].astype(str).str.zfill(6).unique())
        requested_codes = {str(code).zfill(6) for code in index_codes if str(code).strip()}
        coverage = len(received_codes) / max(len(requested_codes), 1)
        min_coverage = min(
            1.0,
            max(0.0, float(os.environ.get("TENCENT_INDEX_KLINE_MIN_COVERAGE", "0.978"))),
        )
        if coverage < min_coverage:
            raise RuntimeError(
                f"Tencent index K-line coverage below threshold: "
                f"{len(received_codes)}/{len(requested_codes)} ({coverage:.1%}) < {min_coverage:.1%}; "
                f"errors={errors[:3]}"
            )
        written = _replace_validated_code_date_frame(
            history_engine,
            complete,
            table_name="sm_index_kline",
            requested_codes=index_codes,
            code_column="index_code",
            date_column="trade_date",
            label="verified external index K-line",
            coverage_env="TENCENT_INDEX_KLINE_MIN_COVERAGE",
            default_coverage=0.978,
            identity_columns=("index_code", "trade_date", "k_type"),
            extra_where="k_type=:scope_k_type",
            extra_params={
                "scope_k_type": int(os.environ.get("SM_INDEX_K_TYPE", "1"))
            },
        )
        logger.info(
            "Verified external index K complete: rows=%d coverage=%d/%d (%.2f%%), "
            "range=%s..%s, sources=%s, errors=%d",
            written,
            len(received_codes),
            len(requested_codes),
            coverage * 100,
            start,
            end,
            source_counts,
            len(errors),
        )
        return

    from adata.stock.market.index_market.market_index import StockMarketIndex

    ins = StockMarketIndex()
    k_type = int(os.environ.get("SM_INDEX_K_TYPE", "1"))
    cols = ["index_code", "trade_time", "trade_date", "k_type", "open", "close", "high", "low", "volume", "amount", "change", "change_pct"]

    def _fetch(code: str) -> pd.DataFrame | None:
        frame = retry_remote(ins.get_market_index, index_code=code, start_date=start, k_type=k_type)
        if frame is None or frame.empty:
            return None
        if end and "trade_date" in frame.columns:
            frame = frame[frame["trade_date"] <= end]
        frame = _to_numeric(frame, ["open", "close", "high", "low", "volume", "amount", "change", "change_pct"])
        frame["k_type"] = k_type
        return frame[cols]

    parts = _concurrent_run(
        index_codes,
        _fetch,
        label="index K-line",
        fail_on_error=True,
    )
    complete = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        history_engine,
        complete,
        table_name="sm_index_kline",
        requested_codes=index_codes,
        code_column="index_code",
        date_column="trade_date",
        label="external index K-line",
        coverage_env="INDEX_KLINE_MIN_COVERAGE",
        default_coverage=0.80,
        identity_columns=("index_code", "trade_date", "k_type"),
        extra_where="k_type=:scope_k_type",
        extra_params={"scope_k_type": k_type},
    )


def step_index_minute(engine: Engine, index_codes: list[str]) -> None:
    """Synchronize index minutes in bounded, atomic batches."""
    history_engine = get_kline_engine()
    index_source = _index_source("minute")
    if index_source in {"qmt", "bigqmt"}:
        if index_source == "bigqmt":
            from integrations.bigqmt import bridge
        else:
            from integrations.qmt import bridge
        from integrations.qmt.info import to_qmt_index_symbols

        qmt_codes = to_qmt_index_symbols(index_codes)
        if not qmt_codes:
            raise RuntimeError("QMT index minute has no valid symbols")
        trade_date = _default_myquant_minute_date(engine)
        count = max(0, int(os.environ.get("QMT_INDEX_MINUTE_COUNT", os.environ.get("QMT_MINUTE_COUNT", "0")) or 0))
        batch_size = max(5, int(os.environ.get("QMT_PRODUCTION_INDEX_MINUTE_BATCH_SIZE", "40")))
        timeout = int(os.environ.get("QMT_INDEX_MINUTE_TIMEOUT", os.environ.get("QMT_TIMEOUT", "120")))
        min_coverage = min(1.0, max(0.0, float(os.environ.get("QMT_INDEX_MINUTE_MIN_COVERAGE", "0.50"))))
        total_batches = (len(qmt_codes) + batch_size - 1) // batch_size
        received_codes: set[str] = set()
        staged_parts: list[pd.DataFrame] = []
        for batch_no, batch in enumerate(_chunked(qmt_codes, batch_size), start=1):
            raw = bridge.minute(
                batch,
                trade_date=trade_date,
                start_date=trade_date,
                end_date=trade_date,
                count=count,
                download_history=(count == 0),
                batch_size=batch_size,
                timeout=timeout,
            )
            out = _normalize_index_minute_frame(raw)
            if out.empty:
                logger.warning("QMT index minute batch %d/%d returned no valid rows", batch_no, total_batches)
                continue
            staged_parts.append(out)
            received_codes.update(out["index_code"].astype(str).unique().tolist())
            logger.info(
                "QMT index minute staged batch %d/%d: rows=%d cumulative_codes=%d",
                batch_no,
                total_batches,
                len(out),
                len(received_codes),
            )
        coverage = len(received_codes) / max(len(qmt_codes), 1)
        if not staged_parts or coverage < min_coverage:
            raise RuntimeError(
                f"QMT index minute coverage below threshold: {len(received_codes)}/{len(qmt_codes)} "
                f"({coverage:.1%}) < {min_coverage:.1%}"
            )
        complete = pd.concat(staged_parts, ignore_index=True)
        if count == 0:
            written = _replace_validated_code_date_frame(
                history_engine,
                complete,
                table_name="sm_index_minute",
                requested_codes=index_codes,
                code_column="index_code",
                date_column="trade_time",
                label="QMT index minute",
                coverage_env="QMT_INDEX_MINUTE_MIN_COVERAGE",
                default_coverage=0.50,
                day_partition=True,
            )
        else:
            written = _replace_qmt_index_window(
                history_engine,
                complete,
                table_name="sm_index_minute",
                time_column="trade_time",
            )
        logger.info("QMT index minute complete: rows=%d coverage=%.2f%%", written, coverage * 100)
        return

    from adata.stock.market.index_market.market_index import StockMarketIndex

    ins = StockMarketIndex()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "price", "avg_price", "change", "change_pct", "volume", "amount", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        frame = retry_remote(ins.get_market_index_min, index_code=code)
        if frame is None or frame.empty:
            return None
        frame = _to_numeric(frame, ["price", "avg_price", "change", "change_pct", "volume", "amount"])
        frame["snapshot_at"] = now
        return frame[cols]

    parts = _concurrent_run(
        index_codes,
        _fetch,
        label="index minute",
        fail_on_error=True,
    )
    complete = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _replace_validated_code_date_frame(
        history_engine,
        complete,
        table_name="sm_index_minute",
        requested_codes=index_codes,
        code_column="index_code",
        date_column="trade_time",
        label="external index minute",
        coverage_env="INDEX_MINUTE_MIN_COVERAGE",
        default_coverage=0.80,
        day_partition=True,
    )


def _sina_index_symbol(index_code: str) -> str | None:
    code = str(index_code or "").strip().lower()
    if not code:
        return None
    if code.startswith(("sh", "sz")) and len(code) >= 8:
        return code
    digits = re.sub(r"\D", "", code)
    if len(digits) != 6:
        return None
    if digits.startswith(("399", "395", "970", "980")):
        return "sz" + digits
    return "sh" + digits


def _to_float_or_none(value: Any) -> float | None:
    try:
        text_value = str(value).strip()
        if not text_value or text_value in {"-", "None", "nan"}:
            return None
        return float(text_value)
    except (TypeError, ValueError):
        return None


def _fetch_sina_index_current(index_codes: list[str], snapshot_at: datetime) -> pd.DataFrame:
    symbol_to_code: dict[str, str] = {}
    for code in index_codes:
        symbol = _sina_index_symbol(code)
        if symbol:
            symbol_to_code[symbol] = str(code).strip()
    if not symbol_to_code:
        return pd.DataFrame()

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://finance.sina.com.cn/",
    }
    timeout = float(os.environ.get("SM_SINA_TIMEOUT", "15"))
    rows: list[dict[str, Any]] = []
    pattern = re.compile(r'var hq_str_([a-z0-9]+)="(.*)";')
    for symbols in _chunked(list(symbol_to_code), 80):
        url = "https://hq.sinajs.cn/list=" + ",".join(symbols)
        resp = retry_remote(requests.get, url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        text_body = resp.content.decode("gb18030", errors="replace")
        for line in text_body.splitlines():
            match = pattern.match(line.strip())
            if not match:
                continue
            symbol, payload = match.groups()
            values = payload.split(",")
            if len(values) < 32 or not values[0].strip():
                continue
            open_price = _to_float_or_none(values[1])
            prev_close = _to_float_or_none(values[2])
            price = _to_float_or_none(values[3])
            high = _to_float_or_none(values[4])
            low = _to_float_or_none(values[5])
            volume = _to_float_or_none(values[8])
            amount = _to_float_or_none(values[9])
            if price is None or price <= 0:
                continue
            change = None
            change_pct = None
            if prev_close and prev_close > 0:
                change = price - prev_close
                change_pct = change / prev_close * 100
            trade_date = values[30].strip() or None
            trade_time_text = (values[31].strip() if len(values) > 31 else "") or "00:00:00"
            trade_time = pd.to_datetime(f"{trade_date} {trade_time_text}", errors="coerce") if trade_date else pd.NaT
            rows.append(
                {
                    "index_code": symbol_to_code.get(symbol, symbol[-6:]),
                    "trade_time": None if pd.isna(trade_time) else trade_time.to_pydatetime(),
                    "trade_date": trade_date,
                    "open": open_price,
                    "price": price,
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "amount": amount,
                    "change": change,
                    "change_pct": change_pct,
                    "snapshot_at": snapshot_at,
                }
            )
        _sleep()

    cols = ["index_code", "trade_time", "trade_date", "open", "price", "high", "low", "volume", "amount", "change", "change_pct", "snapshot_at"]
    return pd.DataFrame(rows, columns=cols)


def _supplement_cnindex_index_current(
    frame: pd.DataFrame,
    index_codes: list[str],
    snapshot_at: datetime,
) -> pd.DataFrame:
    """Fill missing CNI current rows with the publisher's completed daily bar."""
    if frame is None or frame.empty:
        return frame
    expected = {
        str(code).strip().zfill(6)
        for code in index_codes
        if str(code).strip().zfill(6).startswith(("970", "980"))
    }
    present = {
        str(code).strip().zfill(6)
        for code in frame.get("index_code", pd.Series(dtype=str)).dropna().tolist()
    }
    missing = sorted(expected - present)
    trade_date_values = frame.get("trade_date")
    if trade_date_values is None:
        return frame
    trade_dates = pd.to_datetime(trade_date_values, errors="coerce").dropna()
    if not missing or trade_dates.empty:
        return frame
    target_date = trade_dates.max().strftime("%Y-%m-%d")
    rows: list[dict[str, Any]] = []
    for code in missing:
        try:
            official = _fetch_cnindex_index_current(code, snapshot_at)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Official CNINDEX current supplement unavailable for %s: %s", code, exc)
            continue
        if official.empty:
            continue
        official = official[
            pd.to_datetime(official["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            == target_date
        ]
        if not official.empty:
            rows.append(official.sort_values("trade_time").iloc[-1].to_dict())
    if not rows:
        return frame
    logger.info(
        "index current: supplemented %d/%d missing CNI rows from the official publisher",
        len(rows),
        len(missing),
    )
    return pd.concat([frame, pd.DataFrame(rows)], ignore_index=True)


def _legacy_step_index_current(engine: Engine, index_codes: list[str]) -> None:
    index_source = _index_source("current")
    if index_source in {"qmt", "bigqmt"}:
        if index_source == "bigqmt":
            from integrations.bigqmt import bridge
        else:
            from integrations.qmt import bridge
        from integrations.qmt.info import to_qmt_index_symbols

        qmt_codes = to_qmt_index_symbols(index_codes)
        if not qmt_codes:
            logger.warning("指数快照(QMT): no valid index codes")
            return
        df = bridge.current(
            qmt_codes,
            batch_size=int(os.environ.get("QMT_INDEX_CURRENT_BATCH_SIZE", os.environ.get("QMT_CURRENT_BATCH_SIZE", "500"))),
            timeout=int(os.environ.get("QMT_TIMEOUT", "120")),
        )
        if df is None or df.empty:
            raise RuntimeError("QMT index current returned no rows")
        out = pd.DataFrame()
        out["index_code"] = df["stock_code"].astype(str).str.zfill(6)
        out["trade_time"] = df["snapshot_at"]
        out["trade_date"] = pd.to_datetime(df["snapshot_at"], errors="coerce").dt.strftime("%Y-%m-%d")
        out["open"] = pd.to_numeric(df.get("open"), errors="coerce")
        out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
        out["high"] = pd.to_numeric(df.get("high"), errors="coerce")
        out["low"] = pd.to_numeric(df.get("low"), errors="coerce")
        out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
        out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
        out["change"] = pd.to_numeric(df.get("change"), errors="coerce")
        out["change_pct"] = pd.to_numeric(df.get("change_pct"), errors="coerce")
        out["snapshot_at"] = df["snapshot_at"]
        upsert_current_frame(engine, _with_etl(out), "sm_index_current", ["index_code"])
        return

    from adata.stock.market.index_market.market_index import StockMarketIndex

    ins = StockMarketIndex()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "open", "price", "high", "low", "volume", "amount", "change", "change_pct", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_index_current, index_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["open", "price", "high", "low", "volume", "amount", "change", "change_pct"])
            df["snapshot_at"] = now
            return df[cols]
        return None

    parts = _concurrent_run(index_codes, _fetch, label="指数实时")
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        logger.warning("index_current: adata returned no rows; trying Sina quote fallback.")
        df = _fetch_sina_index_current(index_codes, now)
        logger.info("index_current: Sina fallback fetched %s/%s rows.", len(df), len(index_codes))
    if df.empty:
        raise RuntimeError("index_current fetched no rows from adata or Sina fallback")
    upsert_current_frame(engine, _with_etl(df), "sm_index_current", ["index_code"])


def _replace_index_current_snapshot(
    engine: Engine,
    frame: pd.DataFrame,
    index_codes: list[str],
    *,
    source: str,
) -> int:
    """Validate and atomically replace the complete index-current snapshot."""
    if frame is None or frame.empty:
        raise RuntimeError(f"{source} index current returned no rows")
    expected_codes = {str(code).zfill(6) for code in index_codes if str(code).strip()}
    out = frame.copy()
    out["index_code"] = out["index_code"].astype(str).str.zfill(6)
    out["price"] = pd.to_numeric(out.get("price"), errors="coerce")
    out = out.dropna(subset=["index_code", "price"])
    out = out[(out["price"] > 0) & out["index_code"].isin(expected_codes)]
    out = out.drop_duplicates(subset=["index_code"], keep="last")
    received_codes = set(out["index_code"].tolist())
    min_coverage = min(
        1.0,
        max(0.0, float(os.environ.get("QMT_INDEX_CURRENT_MIN_COVERAGE", "0.90"))),
    )
    coverage = len(received_codes) / max(len(expected_codes), 1)
    if out.empty or coverage < min_coverage:
        raise RuntimeError(
            f"{source} index current coverage below threshold: "
            f"{len(received_codes)}/{len(expected_codes)} ({coverage:.1%}) < {min_coverage:.1%}"
        )
    predicate, params = _code_scope_predicate(
        sorted(received_codes),
        column="index_code",
        prefix="index_current_code",
    )
    written = replace_table_rows(
        _clean_df(_with_etl(out)),
        "sm_index_current",
        engine,
        where_sql=predicate,
        params=params,
        chunksize=max(100, int(os.environ.get("QMT_INDEX_DB_CHUNK_SIZE", "1000"))),
        method="multi",
    )
    logger.info(
        "index current (%s): atomically replaced %d rows, coverage=%.2f%%",
        source,
        written,
        coverage * 100,
    )
    return written


def step_index_current(engine: Engine, index_codes: list[str]) -> None:
    index_source = _index_source("current")
    if index_source in {"qmt", "bigqmt"}:
        if _qmt_runtime_available(index_source):
            try:
                if index_source == "bigqmt":
                    from integrations.bigqmt import bridge
                else:
                    from integrations.qmt import bridge
                from integrations.qmt.info import to_qmt_index_symbols

                qmt_codes = to_qmt_index_symbols(index_codes)
                if qmt_codes:
                    df = bridge.current(
                        qmt_codes,
                        batch_size=int(os.environ.get("QMT_INDEX_CURRENT_BATCH_SIZE", os.environ.get("QMT_CURRENT_BATCH_SIZE", "500"))),
                        timeout=int(os.environ.get("QMT_TIMEOUT", "120")),
                    )
                    if df is None or df.empty:
                        raise RuntimeError("QMT index current returned no rows")
                    out = pd.DataFrame()
                    out["index_code"] = df["stock_code"].astype(str).str.zfill(6)
                    out["trade_time"] = df["snapshot_at"]
                    out["trade_date"] = pd.to_datetime(df["snapshot_at"], errors="coerce").dt.strftime("%Y-%m-%d")
                    out["open"] = pd.to_numeric(df.get("open"), errors="coerce")
                    out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
                    out["high"] = pd.to_numeric(df.get("high"), errors="coerce")
                    out["low"] = pd.to_numeric(df.get("low"), errors="coerce")
                    out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
                    out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
                    out["change"] = pd.to_numeric(df.get("change"), errors="coerce")
                    out["change_pct"] = pd.to_numeric(df.get("change_pct"), errors="coerce")
                    out["snapshot_at"] = df["snapshot_at"]
                    out = _supplement_cnindex_index_current(out, index_codes, _now())
                    _replace_index_current_snapshot(engine, out, index_codes, source="QMT/CNINDEX")
                    return
                logger.warning("index current QMT returned no valid symbols; falling back to adata/Sina")
            except Exception as exc:
                logger.warning("index current QMT failed; falling back to adata/Sina: %s", exc)
        else:
            logger.warning("index current is configured for QMT, but QMT runtime is unavailable; falling back to adata/Sina")

    from adata.stock.market.index_market.market_index import StockMarketIndex

    ins = StockMarketIndex()
    now = _now()
    cols = ["index_code", "trade_time", "trade_date", "open", "price", "high", "low", "volume", "amount", "change", "change_pct", "snapshot_at"]

    def _fetch(code: str) -> pd.DataFrame | None:
        df = retry_remote(ins.get_market_index_current, index_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["open", "price", "high", "low", "volume", "amount", "change", "change_pct"])
            df["snapshot_at"] = now
            return df[cols]
        return None

    parts = _concurrent_run(index_codes, _fetch, label="index current")
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        logger.warning("index_current: adata returned no rows; trying Sina quote fallback.")
        df = _fetch_sina_index_current(index_codes, now)
        logger.info("index_current: Sina fallback fetched %s/%s rows.", len(df), len(index_codes))
    if df.empty:
        raise RuntimeError("index_current fetched no rows from adata or Sina fallback")
    df = _supplement_cnindex_index_current(df, index_codes, now)
    _replace_index_current_snapshot(engine, df, index_codes, source="adata/Sina/CNINDEX")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="STOCK-MARKET 同步")
    parser.add_argument("--only", type=str, default="", help=f"只执行步骤，逗号分隔。可选：{','.join(sorted(STEP_NAMES))}")
    parser.add_argument("--kline-start", type=str, default="", help="K线起始日期（YYYY-MM-DD），覆盖 SM_MARKET_START/SM_INDEX_START")
    parser.add_argument("--kline-end", type=str, default="", help="K线结束日期（YYYY-MM-DD），覆盖 SM_MARKET_END/SM_INDEX_END")
    parser.add_argument("--kline-today", action="store_true", help="K线仅同步当天（收盘后常用）")
    parser.add_argument(
        "--kline-incremental",
        action="store_true",
        help="K线增量模式：先抓取/验证，再原子替换目标代码的日期分区",
    )
    parser.add_argument(
        "--kline-source",
        type=str,
        choices=["adata", "akshare", "myquant", "qmt", "bigqmt"],
        default=os.environ.get("SM_STOCK_KLINE_SOURCE", "adata").strip().lower() or "adata",
        help="个股 K 线数据源：adata（东财/百度）、akshare（东财日 K）、myquant（掘金）、qmt（迅投）",
    )
    parser.add_argument(
        "--kline-adjust",
        type=str,
        default="",
        choices=["", "qfq", "hfq"],
        help="仅 akshare 新浪日线复权：默认不复权；qfq/hfq 前/后复权",
    )
    parser.add_argument(
        "--max-stocks",
        type=int,
        default=None,
        metavar="N",
        help="覆盖 SM_MAX_STOCKS：个股相关步骤的股票数量上限。0=不限制（全 si_all_code）；不传则仍读环境变量或默认 200",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="",
        help="K 线起始日，可 YYYYMMDD（与 a_share_daily_import 一致）；若指定则覆盖 --kline-start",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="",
        help="K 线结束日，可 YYYYMMDD；若指定则覆盖 --kline-end",
    )
    parser.add_argument("--offset", type=int, default=0, help="si_all_code 排序后偏移（需配合 --limit>=0）")
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        metavar="N",
        help="股票批大小：-1（默认）仍按 SM_MAX_STOCKS；0=从 offset 到表尾；>0=本批只数",
    )
    parser.add_argument(
        "--progress-file",
        type=str,
        default="",
        help="akshare 模式断点续跑进度文件（每行一码）；空字符串表示不写进度、不逐只入库",
    )
    parser.add_argument("--skip-progress", action="store_true", help="忽略进度文件（不过滤、不追加进度）")
    parser.add_argument("--flow-date", type=str, default="", help="资金流向指定日期（YYYY-MM-DD），仅拉取并原子替换该日分区")
    return parser.parse_args()


def _parse_only_set(raw: str) -> set[str]:
    if not raw.strip():
        return set()
    only = {x.strip() for x in raw.split(",") if x.strip()}
    bad = sorted(only - STEP_NAMES)
    if bad:
        raise ValueError(f"--only 含无效步骤：{','.join(bad)}")
    return only


def _should_run(step: str, only_set: set[str]) -> bool:
    return not only_set or step in only_set


def main() -> None:
    args = _parse_args()
    only_set = _parse_only_set(args.only)
    kline_start = args.kline_start.strip() or None
    kline_end = args.kline_end.strip() or None
    if args.kline_today:
        today = datetime.now().strftime("%Y-%m-%d")
        kline_start = today
        kline_end = today
    if args.start_date.strip():
        kline_start = _normalize_cli_date(args.start_date)
    if args.end_date.strip():
        kline_end = _normalize_cli_date(args.end_date)

    kline_source = (args.kline_source or "adata").strip().lower()
    kline_adjust = (args.kline_adjust or "").strip().lower()
    flow_date = (args.flow_date or "").strip()
    max_stocks_limit = None

    if args.max_stocks is not None:
        max_stocks_limit = args.max_stocks
        logger.info("已设置 SM_MAX_STOCKS=%s（--max-stocks）", args.max_stocks)

    slice_limit = args.limit
    slice_offset = max(0, args.offset)
    if slice_offset > 0 and slice_limit == -1:
        slice_limit = 0
        logger.info("已指定 --offset 但未指定 --limit，已按 --limit 0（从 offset 到 si_all_code 末尾）处理。")
    elif max_stocks_limit is not None and slice_limit == -1:
        slice_limit = max_stocks_limit

    engine = create_batch_engine()
    _log_mysql_target(str(engine.url))
    run_ddl(engine)
    _ensure_sm_stock_kline_short_name(engine)

    stock_steps = {
        "dividend",
        "stock_kline",
        "stock_minute",
        "stock_current",
        "stock_five",
        "stock_bar",
        "stock_flow_min",
        "stock_flow_daily",
    }
    index_steps = {"index_kline", "index_minute", "index_current"}
    concept_ths_steps = {"concept_ths_kline", "concept_ths_minute", "concept_ths_current"}
    concept_east_steps = {"concept_east_kline", "concept_east_minute", "concept_east_current"}

    stock_codes = (
        read_stock_codes(engine, slice_offset=slice_offset, slice_limit=slice_limit)
        if not only_set or bool(only_set & stock_steps)
        else []
    )
    progress_path = (args.progress_file or "").strip()
    if not args.skip_progress and progress_path:
        done = _kline_load_progress(progress_path)
        n_before = len(stock_codes)
        stock_codes = [c for c in stock_codes if c not in done]
        logger.info(
            "进度文件 %s：已跳过 %s 只，本批待处理 %s 只",
            progress_path,
            n_before - len(stock_codes),
            len(stock_codes),
        )

    index_codes = read_index_codes(engine) if not only_set or bool(only_set & index_steps) else []
    concept_ths_codes = (
        read_concept_ths_codes(engine)
        if not only_set or bool(only_set & concept_ths_steps)
        else []
    )
    concept_east_codes = (
        read_concept_east_codes(engine)
        if not only_set or bool(only_set & concept_east_steps)
        else []
    )

    steps: list[tuple[str, str, Callable[[], None]]] = [
        ("dividend", "分红", lambda: step_dividend(engine, stock_codes)),
        (
            "stock_kline",
            "个股K线",
            lambda: step_stock_kline(
                engine,
                stock_codes,
                kline_start,
                kline_end,
                kline_source=kline_source,
                akshare_adjust=kline_adjust,
                progress_file=progress_path,
                skip_progress=args.skip_progress,
                incremental=args.kline_incremental,
            ),
        ),
        ("stock_minute", "个股分时", lambda: step_stock_minute(engine, stock_codes)),
        ("stock_current", "个股实时", lambda: step_stock_current(engine, stock_codes)),
        ("stock_five", "五档盘口", lambda: step_stock_five(engine, stock_codes)),
        ("stock_bar", "分时成交", lambda: step_stock_bar(engine, stock_codes)),
        ("stock_flow_min", "分时资金流向", lambda: step_stock_flow_min(engine, stock_codes)),
        ("stock_flow_daily", "日度资金流向", lambda: step_stock_flow_daily(engine, stock_codes, flow_date=flow_date)),
        (
            "concept_ths_kline",
            "同花顺概念K线",
            lambda: step_concept_ths_kline(engine, concept_ths_codes, kline_start, kline_end),
        ),
        ("concept_ths_minute", "同花顺概念分时", lambda: step_concept_ths_minute(engine, concept_ths_codes)),
        ("concept_ths_current", "同花顺概念实时", lambda: step_concept_ths_current(engine, concept_ths_codes)),
        (
            "concept_east_kline",
            "东财概念K线",
            lambda: step_concept_east_kline(engine, concept_east_codes, kline_start, kline_end),
        ),
        ("concept_east_minute", "东财概念分时", lambda: step_concept_east_minute(engine, concept_east_codes)),
        ("concept_east_current", "东财概念实时", lambda: step_concept_east_current(engine, concept_east_codes)),
        ("concept_flow_east", "概念资金流向", lambda: step_concept_flow_east(engine)),
        ("index_kline", "指数K线", lambda: step_index_kline(engine, index_codes, kline_start, kline_end)),
        ("index_minute", "指数分时", lambda: step_index_minute(engine, index_codes)),
        ("index_current", "指数实时", lambda: step_index_current(engine, index_codes)),
    ]

    for key, name, fn in steps:
        if not _should_run(key, only_set):
            continue
        try:
            fn()
        except Exception as e:  # pylint: disable=broad-except
            logger.exception("步骤「%s」失败：%s", name, e)
            raise

    logger.info("STOCK-MARKET 同步流程结束。")


if __name__ == "__main__":
    main()
