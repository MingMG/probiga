# -*- coding: utf-8 -*-
"""
将 adata.stock.info（STOCK-INFO 文档）下可批量同步的接口写入 MySQL 表 ``probiga``。

前置：
  pip install -e ./adata
  pip install -r requirements-platform.txt

执行（在仓库根目录 ``ProBigA``）::

  python -m biz.stock_info.sync_stock_info

环境变量（可选）：
  MYSQL_URL  必填，MySQL 连接串；也可写入项目根目录 ``.env``
  SI_REQUEST_SLEEP  每次远程请求后的休眠秒数，默认 ``0.2``
  SI_YEAR_START / SI_YEAR_END  交易日历年份范围，默认 ``2010`` 至 ``当年+1``
  SI_MAX_STOCKS  仅调试：大于 0 时个股六表将失败关闭并保留旧快照，
                 避免用前 N 只的部分数据覆盖全市场表。
  SI_SKIP_DDL  兼容保留；运行账号始终只验证已预置的表结构，不执行 DDL。
  SI_INCLUDE_THS_NAME  设为 ``1`` 时对同花顺按「概念名称」拉成分（请求多、易被风控），默认 ``0``

说明：
  - ``stock.info.get_dynamic_core_index`` 在 adata 内为 TODO，无数据，不落库。
  - 全市场股本/成分/概念等为高并发 HTTP，首次全量可能极耗时；请按需调 ``SI_REQUEST_SLEEP``。
  - 若出现 ``RemoteDisconnected`` / ``Connection aborted``：多为数据源临时断连或限流，已内置重试；仍失败时可加大 ``SI_REQUEST_SLEEP``、``SI_HTTP_BACKOFF`` 后重跑。完整快照在远程拉取和校验成功前不会清除旧表。
  - 指数列表专用：``SI_COOLDOWN_BEFORE_INDEX``（拉指数前休眠秒数）、``SI_INDEX_FALLBACK=1``（adata 失败后用浏览器头多域名重试）、``SI_CONTINUE_WITHOUT_INDEX=1``（指数仍失败则跳过 ``si_*index*``，继续后面概念/个股同步）。
  - ``SI_INDEX_EAST_BASES``  备用/轮换的东财 push2 根 URL，逗号分隔；分页请求会逐个镜像重试（默认含 33/63/81/90 等多线路）。
  - ``SI_INDEX_FALLBACK_PAGE_SLEEP``  备用线路分页间隔秒数，默认 ``0.35``；仍断连时可加大到 ``0.8~1.5``。
  - ``SI_INDEX_SINA_FALLBACK``  东财全失败时是否改用新浪财经 ``Market_Center.getHQNodeData``（默认 ``1``）；设 ``0`` 关闭。
  - ``SI_INDEX_SINA_NODE``  新浪行情中心 node，默认 ``hs_s``（沪深指数）；``SI_INDEX_SINA_PAGE_SLEEP`` 分页间隔秒，默认 ``0.25``。
  - ``SI_INDEX_PRIMARY``  设为 ``sina`` 时**跳过东财**，直接只拉新浪（适合 push2 长期不可用环境）。
  - ``SI_INDEX_CONSTITUENT_PRIMARY``  指数成分数据源顺序：``auto``（默认，先百度后新浪）、``baidu``、``sina``。百度常返回空体/非 JSON 时自动改试新浪。
  - **只补其它表、保留已有 ``si_all_code``**：设 ``SI_SYNC_SKIP_ALL_CODE=1``，从库读 ``si_all_code`` 跑后续。``SI_SKIP_GLOBAL_TRUNCATE`` 仅兼容旧调度参数；新流程从不在开场全局清表。
  - 每张完整快照都是先拉取、去重和非空校验，再在同一数据库事务内 ``DELETE + INSERT``；插入失败会回滚到旧快照。
  - **分步容错**：某一步异常不会中断后续步骤（见 ``main`` 内日志）。
"""
from __future__ import annotations

import logging
import os
import random
import re
import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

import numpy as np
import pandas as pd
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from sqlalchemy import text
from sqlalchemy.engine import Engine

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
logger = logging.getLogger("sync_stock_info")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.batch_db import (
    create_batch_engine,
    quote_identifier,
    read_frame,
    replace_table_rows,
    write_frame,
)
from server.common.legacy_table_surface import validate_required_table_surface

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_si_stock_info_tables.sql"

TABLES_TRUNCATE_ORDER = [
    "si_trade_calendar",
    "si_index_constituent",
    "si_all_index_code",
    "si_concept_constituent_east",
    "si_concept_constituent_ths",
    "si_concept_code_east",
    "si_concept_code_ths",
    "si_stock_concept_ths",
    "si_stock_concept_baidu",
    "si_stock_plate_east",
    "si_stock_concept_east",
    "si_stock_shares",
    "si_industry_sw",
    "si_all_code",
]

_QMT_SECTOR_CACHE: dict[str, pd.DataFrame] | None = None
_QMT_SECTOR_CACHE_SOURCE = ""


def _stock_pool_name_map(engine: Engine) -> dict[str, str]:
    """Return the canonical A-share universe used by all membership tables."""
    frame = read_frame(
        text("SELECT stock_code, short_name FROM si_all_code"),
        engine,
    )
    if frame is None or frame.empty:
        return {}
    frame = frame.copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
    frame["short_name"] = frame["short_name"].fillna("").astype(str).str.strip()
    frame = frame[frame["stock_code"].str.fullmatch(r"[0-9]{6}", na=False)]
    return (
        frame.drop_duplicates(subset=["stock_code"], keep="last")
        .set_index("stock_code")["short_name"]
        .to_dict()
    )


def _sleep() -> None:
    time.sleep(float(os.environ.get("SI_REQUEST_SLEEP", "0.2")))


def _qmt_runtime_available() -> bool:
    """Return True only when the local QMT bridge can run on this host."""
    try:
        from integrations.qmt import bridge

        return bool(bridge.is_configured())
    except Exception as exc:  # pragma: no cover - defensive import guard
        logger.debug("QMT runtime availability check failed: %s", exc)
        return False


def _big_qmt_runtime_available() -> bool:
    """Return True when the standard-QMT built-in strategy is alive."""
    try:
        from integrations.bigqmt import bridge

        return bool(bridge.is_configured())
    except Exception as exc:  # pragma: no cover - defensive import guard
        logger.debug("standard QMT runtime availability check failed: %s", exc)
        return False


def _source_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name, "").strip().lower()
        if value:
            if value in {"big_qmt", "qmt_big"}:
                value = "bigqmt"
            if value == "qmt" and not _qmt_runtime_available():
                logger.warning("QMT source %s requested but QMT runtime is unavailable; falling back.", name)
                continue
            if value == "bigqmt" and not _big_qmt_runtime_available():
                logger.warning("standard QMT source %s requested but its built-in strategy is unavailable; falling back.", name)
                continue
            return value
    return default


def _use_qmt_sector_data() -> bool:
    return _source_value(
        "SI_CONCEPT_SOURCE",
        "SI_INDUSTRY_SOURCE",
        "DATA_SOURCE_CONCEPT_LIST",
        "DATA_SOURCE_CODE_LIST",
    ) in {"qmt", "bigqmt"}


def _qmt_sector_tables() -> dict[str, pd.DataFrame]:
    global _QMT_SECTOR_CACHE, _QMT_SECTOR_CACHE_SOURCE
    source = _source_value(
        "SI_CONCEPT_SOURCE",
        "SI_INDUSTRY_SOURCE",
        "DATA_SOURCE_CONCEPT_LIST",
        "DATA_SOURCE_CODE_LIST",
    )
    if _QMT_SECTOR_CACHE is None or _QMT_SECTOR_CACHE_SOURCE != source:
        if source == "bigqmt":
            from integrations.bigqmt.reference import fetch_sector_datasets
        else:
            from integrations.qmt.sectors import fetch_sector_datasets

        _QMT_SECTOR_CACHE = fetch_sector_datasets()
        _QMT_SECTOR_CACHE_SOURCE = source
    return _QMT_SECTOR_CACHE


def retry_remote(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """
    对数据源 HTTP 调用做重试（指数退避 + 抖动），缓解 ``RemoteDisconnected`` 等瞬时断连。
    环境变量：``SI_HTTP_RETRIES``（默认 8）、``SI_HTTP_BACKOFF``（首轮等待秒数，默认 3）。
    """
    max_retries = max(1, int(os.environ.get("SI_HTTP_RETRIES", "8")))
    base = float(os.environ.get("SI_HTTP_BACKOFF", "3.0"))
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
            logger.warning(
                "远程请求失败 (%s/%s)，%.1f 秒后重试：%s",
                attempt + 1,
                max_retries,
                wait,
                e,
            )
            time.sleep(wait)
    assert last is not None
    raise last


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _decimal_year(y: int) -> float:
    return float(y)


def _coerce_decimals(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _clean_object_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.replace({np.nan: None, pd.NaT: None})
    return df


def _is_bad_ths_result(res) -> bool:
    return res is None or isinstance(res, Exception) or not isinstance(res, pd.DataFrame)


def validate_stock_info_runtime_schema(engine: Engine) -> dict[str, Any]:
    return validate_required_table_surface(
        engine,
        TABLES_TRUNCATE_ORDER,
        context="stock information collector",
        required_columns={
            "si_all_code": {"stock_code", "short_name", "etl_sync_at"},
            "si_trade_calendar": {"trade_date", "trade_status", "etl_sync_at"},
        },
    )


def run_ddl(engine: Engine) -> None:
    """Legacy entrypoint retained as a read-only prepared-schema guard."""

    validate_stock_info_runtime_schema(engine)


def truncate_all(engine: Engine) -> None:
    """Fail closed: destructive pre-clears are not a supported refresh mode."""

    del engine
    raise RuntimeError(
        "全局清空股票资料表已禁用；采集完成后必须原子替换完整快照"
    )


def truncate_only(engine: Engine, *table_names: str) -> None:
    """Fail closed: deletion and replacement may not cross transactions."""

    del engine, table_names
    raise RuntimeError(
        "预清空股票资料表已禁用；删除与完整写入必须在同一事务提交"
    )


def _replace_full_snapshot(engine: Engine, df: pd.DataFrame, table: str) -> int:
    """Validate and atomically replace one complete table snapshot."""

    if df is None or df.empty:
        raise ValueError(f"table {table} received an empty snapshot; preserving previous rows")
    clean = _clean_object_df(df)
    written = replace_table_rows(
        clean,
        table,
        engine,
        chunksize=500,
        method="multi",
    )
    logger.info("表 %s：原子替换 %s 行。", table, written)
    return written


def _replace_full_snapshots_atomically(
    engine: Engine,
    snapshots: dict[str, pd.DataFrame],
) -> dict[str, int]:
    """Publish a related snapshot set as one all-or-nothing transaction."""

    if not snapshots:
        raise ValueError("related snapshot set must not be empty")
    prepared: dict[str, pd.DataFrame] = {}
    for table, frame in snapshots.items():
        quote_identifier(table)
        if frame is None or frame.empty:
            raise ValueError(
                f"table {table} received an empty related snapshot; "
                "preserving the complete previous set"
            )
        prepared[table] = _clean_object_df(frame)
    written: dict[str, int] = {}
    with engine.begin() as connection:
        for table in prepared:
            connection.execute(text(f"DELETE FROM {quote_identifier(table)}"))
        for table, frame in prepared.items():
            written[table] = write_frame(
                frame,
                table,
                connection,
                if_exists="append",
                index=False,
                chunksize=500,
                method="multi",
            )
    logger.info(
        "关联快照已原子替换：%s",
        ", ".join(f"{table}={count}" for table, count in written.items()),
    )
    return written


def _validated_relation_shard(
    frame: Any,
    *,
    source: str,
    shard: str,
    expected_codes: list[str],
) -> pd.DataFrame:
    """Prove that one per-stock source shard covers exactly its requested codes."""

    expected = {str(code).strip().zfill(6) for code in expected_codes if str(code).strip()}
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ValueError(f"{source} shard {shard} returned an empty response")
    if "stock_code" not in frame.columns:
        raise ValueError(f"{source} shard {shard} omitted stock_code evidence")
    clean = frame.copy()
    clean["stock_code"] = clean["stock_code"].astype(str).str.strip().str.zfill(6)
    if not clean["stock_code"].str.fullmatch(r"[0-9]{6}", na=False).all():
        raise ValueError(f"{source} shard {shard} contains invalid stock codes")
    observed = {
        code
        for code in clean["stock_code"].tolist()
        if re.fullmatch(r"[0-9]{6}", code)
    }
    if observed != expected:
        missing = sorted(expected - observed)
        unexpected = sorted(observed - expected)
        raise ValueError(
            f"{source} shard {shard} code coverage mismatch: "
            f"expected={len(expected)} observed={len(observed)} "
            f"missing={missing[:5]} unexpected={unexpected[:5]}"
        )
    return clean


def _replace_value_partitions(
    engine: Engine,
    frame: pd.DataFrame,
    table: str,
    column: str,
    values: set[str],
) -> int:
    """Atomically replace only explicitly proven source partitions."""

    quote_identifier(table)
    quote_identifier(column)
    if frame is None or frame.empty or not values or column not in frame.columns:
        raise ValueError(f"{table} has no proven {column} partitions to replace")
    observed = set(frame[column].dropna().astype(str).str.strip().unique())
    if observed != values:
        raise ValueError(
            f"{table} partition evidence mismatch: expected={sorted(values)} "
            f"observed={sorted(observed)}"
        )
    params = {f"partition_{index}": value for index, value in enumerate(sorted(values))}
    return replace_table_rows(
        _clean_object_df(frame),
        table,
        engine,
        where_sql=f"{quote_identifier(column)} IN (" + ", ".join(
            f":{key}" for key in params
        ) + ")",
        params=params,
        chunksize=2000,
        method="multi",
    )


_COMPLETENESS_ATTR = "probiga_snapshot_completeness"


class PartialSnapshotPublished(RuntimeError):
    """A safe partition refresh completed, but a full snapshot was not proven."""

    status = "partial"
    exit_code = 2

    def __init__(
        self,
        *,
        table: str,
        frame: pd.DataFrame,
        reason: str,
    ) -> None:
        self.table = table
        self.frame = frame
        self.reason = reason
        super().__init__(
            f"status=partial exit_code={self.exit_code} table={table} "
            f"published_partitions={len(frame)} missing_old_rows=preserved reason={reason}"
        )


def _with_completeness_evidence(
    frame: pd.DataFrame,
    evidence: dict[str, Any],
) -> pd.DataFrame:
    """Attach collector-owned evidence without adding persistence columns."""

    frame.attrs[_COMPLETENESS_ATTR] = dict(evidence)
    return frame


def _identity_value(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    normalized = str(value).strip()
    return "" if normalized.lower() in {"nan", "none", "nat"} else normalized


def _identity_set(frame: pd.DataFrame, key_columns: tuple[str, ...]) -> set[str]:
    """Use the first populated stable key, namespaced by its column."""

    identities: set[str] = set()
    if frame is None or frame.empty:
        return identities
    for row in frame.to_dict(orient="records"):
        for column in key_columns:
            value = _identity_value(row.get(column))
            if value:
                identities.add(f"{column}:{value}")
                break
    return identities


def _read_directory_baseline(
    engine: Engine,
    table: str,
    key_columns: tuple[str, ...],
) -> pd.DataFrame:
    quoted_columns = ", ".join(quote_identifier(column) for column in key_columns)
    quoted_table = quote_identifier(table)
    try:
        return read_frame(
            text(f"SELECT {quoted_columns}, `etl_sync_at` FROM {quoted_table}"),
            engine,
        )
    except Exception:
        # Some historical/test schemas predate etl_sync_at.  Their identity set
        # remains usable, but no elapsed-time drift allowance is granted.
        return read_frame(
            text(f"SELECT {quoted_columns} FROM {quoted_table}"),
            engine,
        )


def _ratio_env(name: str, default: str) -> float:
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number between 0 and 1") from exc
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be between 0 and 1")
    return value


def _baseline_drift_limit(baseline: pd.DataFrame) -> float:
    """Scale acceptable set churn by the age of the last complete baseline."""

    initial = _ratio_env("SI_DIRECTORY_BASELINE_INITIAL_DRIFT_RATIO", "0.002")
    daily = _ratio_env("SI_DIRECTORY_BASELINE_DAILY_DRIFT_RATIO", "0.001")
    cap = _ratio_env("SI_DIRECTORY_BASELINE_MAX_DRIFT_RATIO", "0.05")
    age_days = 0.0
    if "etl_sync_at" in baseline.columns:
        timestamps = pd.to_datetime(baseline["etl_sync_at"], errors="coerce").dropna()
        if not timestamps.empty:
            latest = timestamps.max()
            if getattr(latest, "tzinfo", None) is not None:
                latest = latest.tz_localize(None)
            age_days = max(0.0, (pd.Timestamp.now() - latest).total_seconds() / 86400.0)
    return min(cap, initial + daily * age_days)


def _authoritative_completeness_reason(
    evidence: dict[str, Any],
    *,
    observed_identities: set[str],
) -> str | None:
    """Accept only internally verifiable total, terminal-page, or request-set proof."""

    if not evidence or evidence.get("complete") is not True:
        return None
    kind = str(evidence.get("kind") or "")
    received = int(evidence.get("received_rows") or -1)
    if received != len(observed_identities):
        return None
    if kind == "authoritative_total":
        expected = int(evidence.get("expected_total") or -1)
        if expected > 0 and received == expected:
            return f"authoritative total matched ({received}/{expected})"
        return None
    if kind == "pagination_terminal":
        if evidence.get("pages_contiguous") is True and evidence.get("terminal_page") is True:
            return f"contiguous pagination reached terminal page ({received} identities)"
        return None
    if kind == "request_set":
        expected = {str(value) for value in evidence.get("expected_identities") or []}
        if expected and expected == observed_identities:
            return f"authoritative request set matched ({received} identities)"
    return None


def _snapshot_completeness_reason(
    engine: Engine,
    frame: pd.DataFrame,
    table: str,
    key_columns: tuple[str, ...],
    *,
    evidence: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    observed = _identity_set(frame, key_columns)
    if not observed or len(observed) != len(frame):
        return False, (
            "candidate rows do not map one-to-one to populated stable identities "
            f"(rows={len(frame)} identities={len(observed)})"
        )

    if evidence:
        authoritative = _authoritative_completeness_reason(
            evidence,
            observed_identities=observed,
        )
        if authoritative:
            return True, authoritative
        return False, (
            "collector completeness evidence is explicitly incomplete or inconsistent: "
            f"kind={evidence.get('kind')} failure={evidence.get('failure') or 'proof mismatch'}"
        )

    baseline = _read_directory_baseline(engine, table, key_columns)
    previous = _identity_set(baseline, key_columns)
    if not previous:
        return False, "no authoritative source proof and no historical identity baseline"

    removed = previous - observed
    added = observed - previous
    denominator = max(len(previous), 1)
    removed_ratio = len(removed) / denominator
    added_ratio = len(added) / denominator
    drift_limit = _baseline_drift_limit(baseline)
    # A non-authoritative source may prove continuity, but never deletion.
    # Even one missing old identity can be a silently interrupted page; true
    # removals require an authoritative total/terminal/request-set publication.
    if not removed and added_ratio <= drift_limit:
        return True, (
            "historical identity continuity is complete with reasonable additions: "
            f"previous={len(previous)} current={len(observed)} removed=0 "
            f"added={len(added)} added_ratio={added_ratio:.3%} "
            f"allowance={drift_limit:.3%}"
        )
    return False, (
        "historical identity continuity cannot authorize deletion/addition drift: "
        f"previous={len(previous)} current={len(observed)} removed={len(removed)} "
        f"added={len(added)} removed_ratio={removed_ratio:.3%} "
        f"added_ratio={added_ratio:.3%} allowance={drift_limit:.3%}"
    )


def _replace_directory_partitions(
    engine: Engine,
    frame: pd.DataFrame,
    table: str,
    key_columns: tuple[str, ...],
) -> int:
    """Replace only identities returned successfully, in one transaction."""

    if len(key_columns) == 1:
        column = key_columns[0]
        values = {
            _identity_value(value)
            for value in frame[column].tolist()
            if _identity_value(value)
        } if column in frame.columns else set()
        return _replace_value_partitions(engine, frame, table, column, values)

    predicates: list[str] = []
    params: dict[str, str] = {}
    seen: set[tuple[tuple[str, str], ...]] = set()
    for row_index, row in enumerate(frame.to_dict(orient="records")):
        populated = [
            (column, _identity_value(row.get(column)))
            for column in key_columns
            if _identity_value(row.get(column))
        ]
        # A display name is a fallback identity, not part of a coded identity.
        # This permits a legitimate rename while preventing one concept's name
        # from deleting a different concept carrying that same display name.
        coded = [(column, value) for column, value in populated if column != "name"]
        business_keys = tuple(coded or populated)
        if not business_keys or business_keys in seen:
            continue
        seen.add(business_keys)
        clauses: list[str] = []
        for column, value in business_keys:
            key = f"identity_{row_index}_{column}"
            params[key] = value
            clauses.append(f"{quote_identifier(column)} = :{key}")
        predicates.append(" AND ".join(clauses))
    if not predicates:
        raise ValueError(f"table {table} has no successful identity partitions")
    return replace_table_rows(
        _clean_object_df(frame),
        table,
        engine,
        where_sql=" OR ".join(f"({predicate})" for predicate in predicates),
        params=params,
        chunksize=500,
        method="multi",
    )


def _publish_directory_snapshot(
    engine: Engine,
    frame: pd.DataFrame,
    table: str,
    key_columns: tuple[str, ...],
    *,
    evidence: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Publish a proven full snapshot or safely publish successful partitions."""

    if frame is None or frame.empty:
        raise RuntimeError(f"table {table} returned no successful identity partitions")
    prepared = _clean_object_df(frame.copy())
    complete, reason = _snapshot_completeness_reason(
        engine,
        prepared,
        table,
        key_columns,
        evidence=evidence,
    )
    if complete:
        _replace_full_snapshot(engine, prepared, table)
        prepared.attrs["publication_status"] = "complete"
        prepared.attrs["completeness_reason"] = reason
        logger.info("status=complete table=%s evidence=%s", table, reason)
        return prepared

    _replace_directory_partitions(engine, prepared, table, key_columns)
    prepared.attrs["publication_status"] = "partial"
    prepared.attrs["completeness_reason"] = reason
    error = PartialSnapshotPublished(table=table, frame=prepared, reason=reason)
    logger.error("%s", error)
    raise error


def load_info():
    # 仅加载 stock.info，避免 import adata 时提前加载 fund 等（见 adata/__init__.py 按需加载）
    from adata.stock.info import info

    return info


def sync_all_code(engine: Engine, info) -> pd.DataFrame:
    source = _source_value("SI_ALL_CODE_SOURCE", "DATA_SOURCE_CODE_LIST")
    if source in {"qmt", "bigqmt"}:
        if source == "bigqmt":
            from integrations.bigqmt.reference import fetch_all_stock_codes
        else:
            from integrations.qmt.info import fetch_all_stock_codes

        ts = _now()
        df = fetch_all_stock_codes()
        if df is None or df.empty:
            raise RuntimeError("QMT stock code list returned no rows")
        evidence = dict(df.attrs.get(_COMPLETENESS_ATTR) or {})
        df = df.copy()
        if "stock_code" not in df.columns:
            raise RuntimeError("QMT stock code list omitted stock_code identities")
        df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
        if not df["stock_code"].str.fullmatch(r"[0-9]{6}", na=False).all():
            raise RuntimeError("QMT stock code list contains invalid stock_code identities")
        df = df.drop_duplicates(subset=["stock_code"], keep="last")
        df["etl_sync_at"] = ts
        published = _publish_directory_snapshot(
            engine,
            df,
            "si_all_code",
            ("stock_code",),
            evidence=evidence,
        )
        _sleep()
        return published

    ts = _now()
    df = retry_remote(info.all_code)
    if df is None or df.empty:
        raise RuntimeError("stock code source returned no rows")
    evidence = dict(df.attrs.get(_COMPLETENESS_ATTR) or {})
    df = df.copy()
    if "stock_code" not in df.columns:
        raise RuntimeError("stock code source omitted stock_code identities")
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    if not df["stock_code"].str.fullmatch(r"[0-9]{6}", na=False).all():
        raise RuntimeError("stock code source contains invalid stock_code identities")
    df = df.drop_duplicates(subset=["stock_code"], keep="last")
    df["etl_sync_at"] = ts
    published = _publish_directory_snapshot(
        engine,
        df,
        "si_all_code",
        ("stock_code",),
        evidence=evidence,
    )
    _sleep()
    return published


def fetch_all_index_code_fallback() -> pd.DataFrame:
    """
    与 adata ``stock_index.__all_index_code_east`` 同源 ``/api/qt/clist/get``，改用浏览器头；
    **每一页**在 ``SI_INDEX_EAST_BASES`` 多个 push2 镜像上轮询，单镜像断连时换线再试，提高成功率。
    """
    import requests as rq
    from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/javascript,*/*;q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://quote.eastmoney.com/",
        "Connection": "keep-alive",
    }
    bases = [
        b.strip().rstrip("/")
        for b in os.environ.get(
            "SI_INDEX_EAST_BASES",
            "https://push2.eastmoney.com,"
            "https://82.push2.eastmoney.com,"
            "https://33.push2.eastmoney.com,"
            "https://63.push2.eastmoney.com,"
            "https://81.push2.eastmoney.com,"
            "https://90.push2.eastmoney.com,"
            "https://39.push2.eastmoney.com,"
            "https://31.push2.eastmoney.com",
        ).split(",")
        if b.strip()
    ]
    page_sleep = float(os.environ.get("SI_INDEX_FALLBACK_PAGE_SLEEP", "0.35"))
    per_try = max(1, int(os.environ.get("SI_INDEX_FALLBACK_TRY_PER_BASE", "2")))
    retriable = (ConnectionError, Timeout, ChunkedEncodingError)
    try:
        from urllib3.exceptions import ProtocolError as Urllib3ProtocolError

        retriable = retriable + (Urllib3ProtocolError,)
    except ImportError:
        pass

    def fetch_page_json(fs: str, page: int) -> dict | None:
        """单页：各镜像各试若干次，成功返回 json dict。"""
        qs = (
            f"pn={page}&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281"
            f"&fltt=2&invt=2&dect=1&wbp2u=|0|0|0|web&fid=f3&fs={fs}&fields=f12,f13,f14&_=1"
        )
        last_err: BaseException | None = None
        with rq.Session() as session:
            session.headers.update(headers)
            for base in bases:
                for attempt in range(per_try):
                    url = f"{base}/api/qt/clist/get?{qs}"
                    try:
                        r = session.get(url, timeout=55)
                        r.raise_for_status()
                        return r.json()
                    except retriable as e:
                        last_err = e
                        time.sleep(0.4 + attempt * 0.35)
                    except Exception as e:
                        last_err = e
                        break
        if last_err is not None:
            logger.debug("指数列表分页 %s 页仍失败：%s", page, last_err)
        return None

    data: list[dict] = []
    segments: list[dict[str, Any]] = []
    for i in range(2):
        fs = "m:1+s:2" if i == 0 else "m:0+t:5"
        curr_page = 1
        expected_total: int | None = None
        received_rows = 0
        complete = False
        failure = "page limit reached before authoritative total"
        while curr_page < 88:
            res_json = fetch_page_json(fs, curr_page)
            if not res_json:
                failure = f"page {curr_page} request failed"
                break
            block = res_json.get("data")
            if not block:
                failure = f"page {curr_page} omitted data/total"
                break
            try:
                page_total = int(block["total"])
            except (KeyError, TypeError, ValueError):
                failure = f"page {curr_page} omitted a valid authoritative total"
                break
            if expected_total is None:
                expected_total = page_total
            elif page_total != expected_total:
                failure = (
                    f"page {curr_page} total changed from {expected_total} to {page_total}"
                )
                break
            diff = block.get("diff") or []
            if not diff:
                complete = received_rows == expected_total
                failure = "" if complete else (
                    f"terminal page arrived at {received_rows}/{expected_total} rows"
                )
                break
            for row in diff:
                data.append(
                    {
                        "index_code": row.get("f12"),
                        "concept_code": "",
                        "name": row.get("f14"),
                        "source": "东方财富",
                    }
                )
            received_rows += len(diff)
            if received_rows >= expected_total:
                complete = received_rows == expected_total
                failure = "" if complete else (
                    f"received {received_rows} rows beyond authoritative total {expected_total}"
                )
                break
            if len(diff) < 20:
                failure = (
                    f"short page {curr_page} arrived at {received_rows}/{expected_total} rows"
                )
                break
            curr_page += 1
            time.sleep(page_sleep)
        segments.append(
            {
                "filter": fs,
                "expected_total": expected_total,
                "received_rows": received_rows,
                "complete": complete,
                "failure": failure,
            }
        )
        time.sleep(page_sleep)

    if data:
        logger.info("指数列表备用线路成功，原始行数=%s", len(data))
        out = pd.DataFrame(data)
        out = out.dropna(subset=["index_code"]).drop_duplicates(subset=["index_code"], keep="first")
        expected_total = sum(
            int(segment["expected_total"] or 0) for segment in segments
        )
        return _with_completeness_evidence(
            out,
            {
                "kind": "authoritative_total",
                "complete": bool(segments) and all(segment["complete"] for segment in segments),
                "expected_total": expected_total,
                "received_rows": len(out),
                "segments": segments,
            },
        )
    raise RuntimeError("所有备用线路仍无法拉取东财指数列表。")


def fetch_all_index_code_sina() -> pd.DataFrame:
    """
    新浪财经行情中心 ``Market_Center.getHQNodeData``（默认 node=hs_s），与东财无关。
    返回列与 ``all_index_code`` 一致：index_code, concept_code, name, source。
    """
    import json

    import requests as rq
    from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

    node = (os.environ.get("SI_INDEX_SINA_NODE") or "hs_s").strip() or "hs_s"
    page_sleep = float(os.environ.get("SI_INDEX_SINA_PAGE_SLEEP", "0.25"))
    max_pages = max(1, int(os.environ.get("SI_INDEX_SINA_MAX_PAGES", "500")))
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Referer": "https://finance.sina.com.cn/",
    }
    retriable = (ConnectionError, Timeout, ChunkedEncodingError)
    try:
        from urllib3.exceptions import ProtocolError as Urllib3ProtocolError

        retriable = retriable + (Urllib3ProtocolError,)
    except ImportError:
        pass

    rows: list[dict[str, str]] = []
    raw_rows = 0
    pages_contiguous = True
    terminal_page = False
    failure = "page limit reached before a terminal page"
    with rq.Session() as s:
        s.headers.update(headers)
        for page in range(1, max_pages + 1):
            url = (
                "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                f"Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=1&node={node}&_s_r_a=init"
            )
            last_err: BaseException | None = None
            block = None
            for attempt in range(4):
                try:
                    r = s.get(url, timeout=40)
                    r.raise_for_status()
                    block = json.loads(r.content.decode("utf-8"))
                    break
                except retriable as e:
                    last_err = e
                    time.sleep(0.5 + attempt * 0.4)
                except Exception as e:
                    last_err = e
                    break
            if not isinstance(block, list):
                if last_err:
                    logger.warning("新浪指数列表 page=%s 非列表或失败：%s", page, last_err)
                pages_contiguous = False
                failure = f"page {page} request/JSON failed"
                break
            if not block:
                terminal_page = True
                failure = ""
                break
            raw_rows += len(block)
            for item in block:
                code = str(item.get("code") or "").strip()
                if not code:
                    sym = str(item.get("symbol") or "")
                    if sym.startswith(("sh", "sz", "bj")) and len(sym) > 2:
                        code = sym[2:]
                if not code:
                    continue
                code = code.zfill(6)
                nm = item.get("name")
                name = str(nm).strip() if nm is not None else ""
                rows.append(
                    {
                        "index_code": code,
                        "concept_code": "",
                        "name": name[:256] if name else "",
                        "source": "新浪财经",
                    }
                )
            if len(block) < 100:
                terminal_page = True
                failure = ""
                break
            time.sleep(page_sleep)

    if not rows:
        raise RuntimeError("新浪财经指数列表返回空（请检查网络或 SI_INDEX_SINA_NODE）。")
    out = pd.DataFrame(rows)
    out = out.dropna(subset=["index_code"]).drop_duplicates(subset=["index_code"], keep="first")
    return _with_completeness_evidence(
        out,
        {
            "kind": "pagination_terminal",
            "complete": pages_contiguous and terminal_page and raw_rows == len(out),
            "pages_contiguous": pages_contiguous,
            "terminal_page": terminal_page,
            "received_rows": len(out),
            "raw_rows": raw_rows,
            "failure": failure,
        },
    )


def sync_trade_calendar(engine: Engine, info) -> None:
    ts = _now()
    y0 = int(os.environ.get("SI_YEAR_START", "2010"))
    y1 = int(os.environ.get("SI_YEAR_END", str(datetime.now().year + 1)))
    if y1 < y0:
        raise ValueError(f"invalid trade-calendar range: {y0}..{y1}")
    parts: list[pd.DataFrame] = []
    received_years: set[int] = set()
    for y in range(y0, y1 + 1):
        df = retry_remote(info.trade_calendar, year=y)
        if df is None or df.empty:
            logger.warning("交易日历 %s 年返回空，本次不会覆盖旧快照。", y)
            _sleep()
            continue
        df = df.copy()
        if "trade_date" not in df.columns:
            logger.warning("交易日历 %s 年缺少 trade_date 证据，本次不会覆盖旧快照。", y)
            _sleep()
            continue
        parsed_dates = pd.to_datetime(df["trade_date"], errors="coerce")
        observed_years = set(parsed_dates.dropna().dt.year.unique())
        if parsed_dates.isna().any() or observed_years != {y}:
            logger.warning(
                "交易日历 %s 年日期覆盖不匹配（observed=%s），本次不会覆盖旧快照。",
                y,
                sorted(observed_years),
            )
            _sleep()
            continue
        df["trade_date"] = parsed_dates.dt.date
        df["calendar_year"] = _decimal_year(y)
        df["trade_status"] = pd.to_numeric(df["trade_status"], errors="coerce")
        df["day_week"] = pd.to_numeric(df["day_week"], errors="coerce")
        df = _coerce_decimals(df, ["trade_status", "day_week", "calendar_year"])
        df["etl_sync_at"] = ts
        parts.append(df)
        received_years.add(y)
        logger.info("交易日历 %s 年：%s 行", y, len(df))
        _sleep()
    requested_years = set(range(y0, y1 + 1))
    coverage = len(received_years) / max(len(requested_years), 1)
    if not parts or received_years != requested_years:
        missing = sorted(requested_years - received_years)
        raise RuntimeError(
            "trade calendar snapshot is incomplete: "
            f"source=trade_calendar coverage={coverage:.1%}, missing_years={missing}; "
            "preserving previous snapshot"
        )
    _replace_full_snapshot(engine, pd.concat(parts, ignore_index=True), "si_trade_calendar")


def sync_all_index_code(engine: Engine, info) -> pd.DataFrame:
    source = _source_value("SI_ALL_INDEX_CODE_SOURCE", "DATA_SOURCE_INDEX_LIST")
    if source in {"qmt", "bigqmt"}:
        if source == "bigqmt":
            from integrations.bigqmt.reference import fetch_all_index_codes
        else:
            from integrations.qmt.info import fetch_all_index_codes

        ts = _now()
        df = fetch_all_index_codes(engine=engine)
        if df is None or df.empty:
            raise RuntimeError("QMT index code list returned no rows")
        evidence = dict(df.attrs.get(_COMPLETENESS_ATTR) or {})
        df = df.copy()
        if "index_code" not in df.columns:
            raise RuntimeError("QMT index code list omitted index_code identities")
        df["index_code"] = df["index_code"].astype(str).str.strip().str.zfill(6)
        if not df["index_code"].str.fullmatch(r"[0-9]{6}", na=False).all():
            raise RuntimeError("QMT index code list contains invalid index_code identities")
        df = df.drop_duplicates(subset=["index_code"], keep="last")
        df["etl_sync_at"] = ts
        published = _publish_directory_snapshot(
            engine,
            df,
            "si_all_index_code",
            ("index_code",),
            evidence=evidence,
        )
        _sleep()
        return published

    """
    报错栈若指向本函数：说明卡在 **东财指数列表**（adata 内 ``stock_index.py`` → ``push2*.eastmoney.com``），
    不是你的 MySQL。控制台里 ``('Connection aborted.', ...)`` 多为 urllib3/requests 对断连的打印/封装。
    """
    ts = _now()
    if (os.environ.get("SI_INDEX_PRIMARY") or "").strip().lower() != "sina":
        cool = float(os.environ.get("SI_COOLDOWN_BEFORE_INDEX", "10"))
        if cool > 0:
            logger.info("指数列表：冷却 %.0f 秒后请求东财（减轻连续断连）…", cool)
            time.sleep(cool)

    df: pd.DataFrame | None = None
    if (os.environ.get("SI_INDEX_PRIMARY") or "").strip().lower() == "sina":
        try:
            df = fetch_all_index_code_sina()
            logger.info("指数列表：已按 SI_INDEX_PRIMARY=sina 仅使用新浪财经。")
        except Exception as e:
            logger.error("新浪指数列表拉取失败：%s", e)
            df = None
    else:
        try:
            df = retry_remote(info.all_index_code)
        except Exception as e:
            logger.error(
                "adata.stock.info.all_index_code() 失败。"
                "脚本：biz/stock_info/sync_stock_info.py → sync_all_index_code；"
                "底层：adata/adata/stock/info/stock_index.py（东财 push2）。错误：%s",
                e,
            )
            if os.environ.get("SI_INDEX_FALLBACK", "1") == "1":
                try:
                    df = fetch_all_index_code_fallback()
                except Exception as e2:
                    logger.error("指数列表备用拉取仍失败：%s", e2)
                    df = None

    if (df is None or df.empty) and os.environ.get("SI_INDEX_SINA_FALLBACK", "1") == "1":
        try:
            df = fetch_all_index_code_sina()
            logger.info("指数列表：东财不可用，已改用新浪财经（node=%s）。", os.environ.get("SI_INDEX_SINA_NODE", "hs_s"))
        except Exception as e3:
            logger.warning("新浪指数列表备用仍失败：%s", e3)

    if df is None or df.empty:
        raise RuntimeError(
            "status=failed exit_code=1: index directory source returned no rows; "
            "previous si_all_index_code rows were preserved"
        )

    evidence = dict(df.attrs.get(_COMPLETENESS_ATTR) or {})
    df = df.copy()
    if "index_code" not in df.columns:
        raise RuntimeError("index code source omitted index_code identities")
    df["index_code"] = df["index_code"].astype(str).str.strip().str.zfill(6)
    if not df["index_code"].str.fullmatch(r"[0-9]{6}", na=False).all():
        raise RuntimeError("index code source contains invalid index_code identities")
    df = df.drop_duplicates(subset=["index_code"], keep="last")
    if df.empty:
        raise RuntimeError("validated index code snapshot is empty; preserving previous snapshot")
    df["etl_sync_at"] = ts
    published = _publish_directory_snapshot(
        engine,
        df,
        "si_all_index_code",
        ("index_code",),
        evidence=evidence,
    )
    _sleep()
    return published


def sync_index_constituent(engine: Engine, info, df_index: pd.DataFrame) -> None:
    source = _source_value("SI_INDEX_CONSTITUENT_SOURCE", "SI_ALL_INDEX_CODE_SOURCE", "DATA_SOURCE_INDEX_LIST")
    if source in {"qmt", "bigqmt"}:
        if source == "bigqmt":
            from integrations.bigqmt.reference import fetch_index_constituents
        else:
            from integrations.qmt.info import fetch_index_constituents

        ts = _now()
        if df_index is None or df_index.empty:
            return
        codes = (
            df_index["index_code"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", np.nan)
            .dropna()
            .unique()
        )
        df = fetch_index_constituents(codes)
        if df is None or df.empty:
            raise RuntimeError("QMT index constituents returned no rows; preserving previous snapshot")
        allowed_indexes = {str(code).strip().zfill(6) for code in codes}
        stock_names = _stock_pool_name_map(engine)
        if not stock_names:
            raise RuntimeError("canonical stock pool is empty; preserving previous index constituents")
        df = df.copy()
        df["index_code"] = df["index_code"].astype(str).str.strip().str.zfill(6)
        df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
        before_filter = len(df)
        df = df[
            df["index_code"].isin(allowed_indexes)
            & df["stock_code"].isin(stock_names)
        ].drop_duplicates(subset=["index_code", "stock_code"], keep="last")
        if df.empty:
            raise RuntimeError("QMT index constituents failed stock-pool validation; preserving previous snapshot")
        covered_indexes = set(df["index_code"].unique())
        coverage = len(covered_indexes) / max(len(allowed_indexes), 1)
        df["short_name"] = df["stock_code"].map(stock_names).fillna("")
        df["etl_sync_at"] = ts
        clean = _clean_object_df(df[["index_code", "stock_code", "short_name", "etl_sync_at"]])
        if covered_indexes == allowed_indexes:
            replace_table_rows(clean, "si_index_constituent", engine, chunksize=2000, method="multi")
            publication_mode = "full"
        else:
            _replace_value_partitions(
                engine,
                clean,
                "si_index_constituent",
                "index_code",
                covered_indexes,
            )
            publication_mode = "successful-index-partitions"
        logger.info(
            "QMT index constituents atomically replaced: mode=%s rows=%d indexes=%d "
            "coverage=%.2f%% filtered=%d missing-preserved=%d",
            publication_mode,
            len(clean),
            df["index_code"].nunique(),
            coverage * 100,
            before_filter - len(df),
            len(allowed_indexes - covered_indexes),
        )
        _sleep()
        return

    ts = _now()
    if df_index is None or df_index.empty:
        return
    codes = (
        df_index["index_code"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", np.nan)
        .dropna()
        .unique()
    )
    requested_indexes = {str(code).strip().zfill(6) for code in codes}
    parts: list[pd.DataFrame] = []
    failed_shards: list[str] = []
    for i, ic in enumerate(codes):
        requested_code = str(ic).strip().zfill(6)
        try:
            df = retry_remote(info.index_constituent, index_code=str(ic))
        except Exception as e:  # noqa: BLE001 — 单指数失败不中断整表
            logger.warning("指数 %s 成分拉取异常，已跳过：%s", ic, e)
            failed_shards.append(f"{requested_code}:{type(e).__name__}")
            _sleep()
            continue
        if df is None or df.empty:
            failed_shards.append(f"{requested_code}:empty")
            _sleep()
            continue
        df = df.copy()
        if "index_code" not in df.columns:
            failed_shards.append(f"{requested_code}:missing-index-code")
            _sleep()
            continue
        df["index_code"] = df["index_code"].astype(str).str.strip().str.zfill(6)
        if set(df["index_code"].dropna().unique()) != {requested_code}:
            failed_shards.append(f"{requested_code}:wrong-index-code")
            _sleep()
            continue
        df["etl_sync_at"] = ts
        parts.append(df)
        if (i + 1) % 20 == 0:
            logger.info("指数成分进度：%s/%s", i + 1, len(codes))
        _sleep()
    if not parts:
        raise RuntimeError(
            "external index constituent shards are incomplete; "
            f"source=external requested={len(requested_indexes)} "
            f"successful={len(parts)} failures={failed_shards[:10]}; "
            "preserving previous snapshot"
        )
    combined = pd.concat(parts, ignore_index=True)
    combined["index_code"] = combined["index_code"].astype(str).str.strip().str.zfill(6)
    combined["stock_code"] = combined["stock_code"].astype(str).str.strip().str.zfill(6)
    combined = combined.dropna(subset=["index_code", "stock_code"])
    combined = combined.drop_duplicates(subset=["index_code", "stock_code"], keep="last")
    covered_indexes = int(combined["index_code"].nunique())
    coverage = covered_indexes / max(len(codes), 1)
    covered_set = set(combined["index_code"].unique())
    if "short_name" not in combined.columns:
        combined["short_name"] = ""
    combined["etl_sync_at"] = ts
    clean = _clean_object_df(
        combined[["index_code", "stock_code", "short_name", "etl_sync_at"]]
    )
    if not failed_shards and covered_set == requested_indexes:
        replace_table_rows(
            clean,
            "si_index_constituent",
            engine,
            chunksize=2000,
            method="multi",
        )
        publication_mode = "full"
    else:
        _replace_value_partitions(
            engine,
            clean,
            "si_index_constituent",
            "index_code",
            covered_set,
        )
        publication_mode = "successful-index-partitions"
    logger.info(
        "External index constituents atomically replaced: mode=%s rows=%d indexes=%d "
        "coverage=%.2f%% failed-preserved=%d",
        publication_mode,
        len(clean),
        covered_indexes,
        coverage * 100,
        len(failed_shards),
    )


def sync_concept_code_east(engine: Engine, info) -> pd.DataFrame:
    if _use_qmt_sector_data():
        ts = _now()
        df = _qmt_sector_tables().get("concept_catalog", pd.DataFrame()).copy()
        if df.empty:
            raise RuntimeError("QMT concept catalog returned no rows; preserving previous snapshot")
        df["etl_sync_at"] = ts
        replace_table_rows(_clean_object_df(df), "si_concept_code_east", engine)
        _sleep()
        return df

    ts = _now()
    df = retry_remote(info.all_concept_code_east)
    df = _clean_object_df(df)
    if df is None or df.empty or "concept_code" not in df.columns:
        raise RuntimeError("Eastmoney concept catalog returned no usable rows; preserving previous snapshot")
    if "concept_code" in df.columns:
        df = df.dropna(subset=["concept_code"]).drop_duplicates(subset=["concept_code"], keep="first")
    if df.empty:
        raise RuntimeError("Eastmoney concept catalog failed validation; preserving previous snapshot")
    df["etl_sync_at"] = ts
    _replace_full_snapshot(engine, df, "si_concept_code_east")
    _sleep()
    return df


def sync_concept_constituent_east(engine: Engine, info, df_codes: pd.DataFrame) -> None:
    if _use_qmt_sector_data():
        ts = _now()
        df = _qmt_sector_tables().get("concept_constituents", pd.DataFrame()).copy()
        allowed: set[str] = set()
        if df_codes is not None and not df_codes.empty and "concept_code" in df_codes.columns:
            allowed = set(
                df_codes["concept_code"].dropna().astype(str).str.strip().replace("", np.nan).dropna().unique().tolist()
            )
            if allowed:
                df = df[df["concept_code"].astype(str).isin(allowed)].copy()
        if df.empty:
            raise RuntimeError("QMT concept constituents returned no rows; preserving previous snapshot")
        if "concept_code" not in df.columns:
            raise RuntimeError("QMT concept constituents omitted concept_code evidence")
        df["concept_code"] = df["concept_code"].astype(str).str.strip()
        covered = set(df["concept_code"].dropna().unique())
        df["etl_sync_at"] = ts
        clean = _clean_object_df(df)
        if allowed and covered == allowed:
            replace_table_rows(clean, "si_concept_constituent_east", engine)
        else:
            _replace_value_partitions(
                engine,
                clean,
                "si_concept_constituent_east",
                "concept_code",
                covered,
            )
        _sleep()
        return

    ts = _now()
    if df_codes is None or df_codes.empty:
        raise RuntimeError("Eastmoney concept catalog is empty; preserving previous constituent snapshot")
    cset = (
        df_codes["concept_code"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", np.nan)
        .dropna()
        .unique()
    )
    parts: list[pd.DataFrame] = []
    failed_codes: list[str] = []
    for i, cc in enumerate(cset):
        try:
            df = retry_remote(info.concept_constituent_east, concept_code=str(cc))
        except Exception as exc:  # noqa: BLE001 - collect complete shard evidence
            failed_codes.append(f"{cc}:{type(exc).__name__}")
            _sleep()
            continue
        if df is None or df.empty:
            failed_codes.append(f"{cc}:empty")
            _sleep()
            continue
        df = df.copy()
        df["concept_code"] = str(cc)
        df["etl_sync_at"] = ts
        parts.append(df)
        if (i + 1) % 50 == 0:
            logger.info("东财概念成分进度：%s/%s", i + 1, len(cset))
        _sleep()
    if not parts:
        raise RuntimeError(
            "Eastmoney concept constituent snapshot is incomplete: "
            f"source=eastmoney requested={len(cset)} successful={len(parts)} "
            f"failures={failed_codes[:10]}; preserving previous snapshot"
        )
    combined = pd.concat(parts, ignore_index=True)
    covered_codes = int(combined["concept_code"].astype(str).nunique())
    coverage = covered_codes / max(len(cset), 1)
    covered = set(combined["concept_code"].astype(str).unique())
    requested = {str(value).strip() for value in cset}
    if not failed_codes and covered == requested:
        _replace_full_snapshot(engine, combined, "si_concept_constituent_east")
    else:
        _replace_value_partitions(
            engine,
            combined,
            "si_concept_constituent_east",
            "concept_code",
            covered,
        )
        logger.warning(
            "Eastmoney concept membership partially refreshed: source=eastmoney "
            "covered=%d/%d (%.1f%%), failed partitions preserved=%d",
            covered_codes,
            len(cset),
            coverage * 100,
            len(failed_codes),
        )


class ExternalConceptSourceUnavailable(RuntimeError):
    """The external concept service is reachable but cannot serve members."""


def _fetch_external_concept_reference() -> dict[str, pd.DataFrame]:
    """Fetch the Eastmoney concept snapshot fully before any table is touched."""
    info = load_info()
    catalog = retry_remote(info.all_concept_code_east)
    catalog = _clean_object_df(catalog)
    if catalog is None or catalog.empty or "concept_code" not in catalog.columns:
        raise RuntimeError("external concept catalog returned no rows; preserving previous snapshots")
    catalog = catalog.dropna(subset=["concept_code"]).drop_duplicates(
        subset=["concept_code"],
        keep="last",
    )
    parts: list[pd.DataFrame] = []
    attempted = 0
    consecutive_failures = 0
    errors: list[str] = []
    probe_limit = max(1, int(os.environ.get("EXTERNAL_CONCEPT_PROBE_LIMIT", "5")))
    failure_limit = max(
        probe_limit,
        int(os.environ.get("EXTERNAL_CONCEPT_MAX_CONSECUTIVE_FAILURES", "10")),
    )
    for concept_code in catalog["concept_code"].astype(str).str.strip().unique():
        attempted += 1
        try:
            # The provider already applies its own bounded HTTP timeout.  A
            # second generic eight-attempt retry here can turn a systemic WAF
            # outage into an hours-long scheduler run.
            frame = info.concept_constituent_east(concept_code=concept_code)
        except Exception as exc:
            consecutive_failures += 1
            errors.append(f"{concept_code}: {type(exc).__name__}: {exc}")
            logger.warning("External concept member fetch failed for %s: %s", concept_code, exc)
            if (
                (not parts and attempted >= probe_limit)
                or consecutive_failures >= failure_limit
            ):
                raise ExternalConceptSourceUnavailable(
                    "external concept member service is unavailable; "
                    f"attempted={attempted}, consecutive_failures={consecutive_failures}; "
                    "preserving previous snapshots; last_errors="
                    + " | ".join(errors[-3:])
                ) from exc
            _sleep()
            continue
        if frame is None or frame.empty:
            consecutive_failures += 1
            errors.append(f"{concept_code}: empty response")
            if (
                (not parts and attempted >= probe_limit)
                or consecutive_failures >= failure_limit
            ):
                raise ExternalConceptSourceUnavailable(
                    "external concept member service returned no usable rows; "
                    f"attempted={attempted}, consecutive_failures={consecutive_failures}; "
                    "preserving previous snapshots"
                )
            _sleep()
            continue
        consecutive_failures = 0
        frame = frame.copy()
        frame["concept_code"] = concept_code
        parts.append(frame)
        _sleep()
    constituents = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if errors:
        raise ExternalConceptSourceUnavailable(
            "external concept member snapshot is incomplete; "
            f"attempted={attempted}, successful={len(parts)}, failures={len(errors)}; "
            "preserving previous snapshots; last_errors=" + " | ".join(errors[-3:])
        )
    return {
        "concept_catalog": catalog,
        "concept_constituents": constituents,
    }


def sync_qmt_concept_reference(engine: Engine) -> dict[str, int]:
    """Fetch one complete concept snapshot, then replace both tables atomically."""
    configured_source = _source_value(
        "SI_CONCEPT_SOURCE",
        "SI_INDUSTRY_SOURCE",
        "DATA_SOURCE_CONCEPT_LIST",
        "DATA_SOURCE_CODE_LIST",
        default="qmt",
    )
    source = "qmt" if configured_source in {"qmt", "bigqmt"} else "external"
    tables = _qmt_sector_tables() if source == "qmt" else _fetch_external_concept_reference()
    catalog = tables.get("concept_catalog", pd.DataFrame()).copy()
    constituents = tables.get("concept_constituents", pd.DataFrame()).copy()
    if catalog.empty:
        raise RuntimeError(f"{source} concept catalog returned no rows; preserving previous snapshots")
    if constituents.empty:
        raise RuntimeError(f"{source} concept constituents returned no rows; preserving previous snapshots")

    catalog_codes = {
        str(code).strip()
        for code in catalog["concept_code"].dropna().astype(str).tolist()
        if str(code).strip()
    }
    constituents["concept_code"] = constituents["concept_code"].astype(str).str.strip()
    constituents["stock_code"] = constituents["stock_code"].astype(str).str.zfill(6)
    stock_names = _stock_pool_name_map(engine)
    if not stock_names:
        raise RuntimeError("canonical stock pool is empty; preserving previous concept snapshots")
    constituents = constituents[
        constituents["concept_code"].isin(catalog_codes)
        & constituents["stock_code"].str.fullmatch(r"[0-9]{6}", na=False)
        & constituents["stock_code"].isin(stock_names)
    ].drop_duplicates(subset=["concept_code", "stock_code"], keep="last")
    if constituents.empty:
        raise RuntimeError(f"{source} concept memberships failed validation; preserving previous snapshots")
    membership_coverage = constituents["concept_code"].nunique() / max(len(catalog_codes), 1)
    covered_concepts = set(constituents["concept_code"].unique())
    if covered_concepts != catalog_codes:
        raise RuntimeError(
            f"{source} concept membership snapshot is incomplete: "
            f"{constituents['concept_code'].nunique()}/{len(catalog_codes)} "
            f"({membership_coverage:.1%}), missing={sorted(catalog_codes - covered_concepts)[:10]}; "
            "preserving previous snapshots"
        )

    ts = _now()
    catalog["etl_sync_at"] = ts
    constituents["short_name"] = constituents["stock_code"].map(stock_names).fillna("")
    constituents["etl_sync_at"] = ts
    clean_catalog = _clean_object_df(catalog)
    clean_constituents = _clean_object_df(constituents)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM `si_concept_constituent_east`"))
        conn.execute(text("DELETE FROM `si_concept_code_east`"))
        write_frame(clean_catalog, "si_concept_code_east", conn, chunksize=1000, method="multi")
        write_frame(
            clean_constituents,
            "si_concept_constituent_east",
            conn,
            chunksize=2000,
            method="multi",
        )
    logger.info(
        "%s concept reference atomically replaced: concepts=%d memberships=%d",
        source,
        len(clean_catalog),
        len(clean_constituents),
    )
    return {"concepts": int(len(clean_catalog)), "memberships": int(len(clean_constituents))}


def sync_concept_code_ths(engine: Engine, info) -> pd.DataFrame:
    ts = _now()
    df = retry_remote(info.all_concept_code_ths)
    if df is None or df.empty:
        raise RuntimeError("THS concept catalog returned no rows; preserving previous snapshot")
    evidence = dict(df.attrs.get(_COMPLETENESS_ATTR) or {})
    df = df.copy()
    identity_columns = [
        column for column in ("index_code", "concept_code", "name") if column in df.columns
    ]
    if not identity_columns:
        raise RuntimeError("THS concept catalog has no identity columns; preserving previous snapshot")
    for column in identity_columns:
        df[column] = df[column].map(_identity_value)
    df = df.drop_duplicates(subset=identity_columns, keep="last")
    df["etl_sync_at"] = ts
    published = _publish_directory_snapshot(
        engine,
        df,
        "si_concept_code_ths",
        tuple(identity_columns),
        evidence=evidence,
    )
    _sleep()
    return published


def sync_concept_constituent_ths(engine: Engine, info, df_ths: pd.DataFrame) -> None:
    ts = _now()
    if df_ths is None or df_ths.empty:
        raise RuntimeError("THS concept catalog is empty; preserving previous constituent snapshot")
    parts: list[pd.DataFrame] = []
    failed_shards: list[str] = []

    def append_result(query_type: str, query_key: str, res):
        if _is_bad_ths_result(res) or res.empty:
            failed_shards.append(f"{query_type}:{query_key}:empty-or-invalid")
            return False
        d = res.copy()
        d["query_type"] = query_type
        d["query_key"] = query_key
        d["etl_sync_at"] = ts
        parts.append(d)
        return True

    # 1) 优先 index_code
    idx_series = df_ths["index_code"].dropna().astype(str).str.strip().replace("", np.nan).dropna().unique()
    for i, ic in enumerate(idx_series):
        res = retry_remote(info.concept_constituent_ths, index_code=str(ic), wait_time=300)
        append_result("index_code", str(ic), res)
        if (i + 1) % 10 == 0:
            logger.info("同花顺成分(index_code)：%s/%s", i + 1, len(idx_series))
        _sleep()

    # 2) concept_code（3 开头等）
    cc_series = df_ths["concept_code"].dropna().astype(str).str.strip().replace("", np.nan).dropna().unique()
    for i, cc in enumerate(cc_series):
        res = retry_remote(info.concept_constituent_ths, concept_code=str(cc), wait_time=300)
        append_result("concept_code", str(cc), res)
        if (i + 1) % 20 == 0:
            logger.info("同花顺成分(concept_code)：%s/%s", i + 1, len(cc_series))
        _sleep()

    # 3) 按名称（可选，请求多）
    if os.environ.get("SI_INCLUDE_THS_NAME") == "1":
        names = df_ths["name"].dropna().astype(str).str.strip().replace("", np.nan).dropna().unique()
        for i, nm in enumerate(names):
            res = retry_remote(info.concept_constituent_ths, name=str(nm), wait_time=500)
            append_result("name", str(nm), res)
            if (i + 1) % 10 == 0:
                logger.info("同花顺成分(name)：%s/%s", i + 1, len(names))
            _sleep()

    if failed_shards or not parts:
        raise RuntimeError(
            "THS concept constituent snapshot is incomplete: "
            f"source=ths successful={len(parts)} failures={failed_shards[:10]}; "
            "preserving previous snapshot"
        )
    out = pd.concat(parts, ignore_index=True)
    identity_columns = [
        column
        for column in ("query_type", "query_key", "stock_code")
        if column in out.columns
    ]
    if identity_columns:
        out = out.drop_duplicates(subset=identity_columns, keep="last")
    _replace_full_snapshot(engine, out, "si_concept_constituent_ths")


def _stock_limit(codes: list[str]) -> list[str]:
    lim = int(os.environ.get("SI_MAX_STOCKS", "0"))
    if lim > 0:
        return codes[:lim]
    return codes


def sync_per_stock_tables(engine: Engine, info, df_codes: pd.DataFrame) -> None:
    if df_codes is None or df_codes.empty:
        return
    debug_limit = int(os.environ.get("SI_MAX_STOCKS", "0"))
    if debug_limit > 0:
        raise RuntimeError(
            "SI_MAX_STOCKS is a partial-universe debug limit and must not replace full-table "
            "snapshots; preserving all six per-stock tables"
        )
    if _use_qmt_sector_data():
        ts = _now()
        codes = set(df_codes["stock_code"].astype(str).str.zfill(6).tolist())
        tables = _qmt_sector_tables()

        industry_df = tables.get("industry_sw", pd.DataFrame()).copy()
        if not industry_df.empty:
            industry_df = industry_df[industry_df["stock_code"].astype(str).str.zfill(6).isin(codes)].copy()

        concept_df = tables.get("stock_concepts", pd.DataFrame()).copy()
        if not concept_df.empty:
            concept_df = concept_df[concept_df["stock_code"].astype(str).str.zfill(6).isin(codes)].copy()

        plate_df = tables.get("stock_plates", pd.DataFrame()).copy()
        if not plate_df.empty:
            plate_df = plate_df[plate_df["stock_code"].astype(str).str.zfill(6).isin(codes)].copy()

        empty_targets = [
            table_name
            for table_name, frame in (
                ("si_industry_sw", industry_df),
                ("si_stock_concept_east", concept_df),
                ("si_stock_plate_east", plate_df),
            )
            if frame.empty
        ]
        if empty_targets:
            raise RuntimeError(
                "QMT stock relations returned empty datasets; preserving previous snapshots: "
                + ", ".join(empty_targets)
            )

        for table_name, frame in (
            ("si_industry_sw", industry_df),
            ("si_stock_concept_east", concept_df),
            ("si_stock_plate_east", plate_df),
        ):
            if "stock_code" not in frame.columns or "source" not in frame.columns:
                raise RuntimeError(
                    f"{table_name} omitted QMT source/code evidence; preserving previous snapshots"
                )
            frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
            if frame["source"].fillna("").astype(str).str.strip().eq("").any():
                raise RuntimeError(
                    f"{table_name} contains rows without source evidence; preserving previous snapshots"
                )
        industry_codes = set(industry_df["stock_code"].unique())
        plate_codes = set(plate_df["stock_code"].unique())
        catalog = tables.get("concept_catalog", pd.DataFrame())
        if catalog.empty or "concept_code" not in catalog.columns or "concept_code" not in concept_df.columns:
            raise RuntimeError(
                "QMT concept relation omitted catalog coverage evidence; preserving previous snapshots"
            )
        catalog_codes = set(catalog["concept_code"].dropna().astype(str).str.strip())
        relation_concepts = set(concept_df["concept_code"].dropna().astype(str).str.strip())
        if industry_codes != codes or plate_codes != codes or relation_concepts != catalog_codes:
            raise RuntimeError(
                "QMT stock relation snapshot is incomplete: "
                f"source={_QMT_SECTOR_CACHE_SOURCE or 'qmt'} "
                f"industry_codes={len(industry_codes)}/{len(codes)} "
                f"plate_codes={len(plate_codes)}/{len(codes)} "
                f"concepts={len(relation_concepts)}/{len(catalog_codes)}; "
                "preserving previous snapshots"
            )

        industry_df["etl_sync_at"] = ts
        concept_df["etl_sync_at"] = ts
        plate_df["etl_sync_at"] = ts
        _replace_full_snapshots_atomically(
            engine,
            {
                "si_industry_sw": industry_df,
                "si_stock_concept_east": concept_df,
                "si_stock_plate_east": plate_df,
            },
        )

        logger.info(
            "QMT 个股维表已原表替换：industry=%s concept=%s plate=%s；其它 si_* 表未改动",
            len(industry_df),
            len(concept_df),
            len(plate_df),
        )
        return

    ts = _now()
    codes = (
        df_codes["stock_code"]
        .astype(str)
        .str.zfill(6)
        .drop_duplicates()
        .tolist()
    )
    fetch_failures: list[str] = []
    logger.info("个股维度同步，股票数：%s", len(codes))

    # 股本
    share_parts: list[pd.DataFrame] = []
    for i, code in enumerate(codes):
        try:
            df = retry_remote(info.get_stock_shares, stock_code=code, is_history=True)
            df = _validated_relation_shard(
                df, source="shares", shard=code, expected_codes=[code]
            )
        except Exception as e:
            logger.warning("股本 %s 失败：%s", code, e)
            fetch_failures.append(f"shares:{code}:{type(e).__name__}")
            df = pd.DataFrame()
        if df is not None and not df.empty:
            df = df.copy()
            if "change_date" not in df.columns:
                fetch_failures.append(f"shares:{code}:missing-date-coverage")
                _sleep()
                continue
            df["change_date"] = pd.to_datetime(df["change_date"], errors="coerce").dt.date
            if df["change_date"].isna().any():
                fetch_failures.append(f"shares:{code}:invalid-date-coverage")
                _sleep()
                continue
            df = _coerce_decimals(df, ["total_shares", "limit_shares", "list_a_shares"])
            df["etl_sync_at"] = ts
            share_parts.append(df)
        if (i + 1) % 100 == 0:
            logger.info("股本进度：%s/%s", i + 1, len(codes))
        _sleep()
    # 申万行业（批量）
    batch = 40
    ind_parts: list[pd.DataFrame] = []
    for i in range(0, len(codes), batch):
        chunk = codes[i : i + batch]
        try:
            df = retry_remote(info.get_industry_sw, stock_code=chunk)
            df = _validated_relation_shard(
                df,
                source="industry_sw",
                shard=f"batch-{i // batch + 1}",
                expected_codes=chunk,
            )
        except Exception as e:
            logger.warning("申万行业 batch %s 失败：%s", chunk[:3], e)
            fetch_failures.append(
                f"industry:{','.join(chunk[:3])}:{type(e).__name__}"
            )
            df = pd.DataFrame()
        if df is not None and not df.empty:
            df = df.copy()
            df["etl_sync_at"] = ts
            ind_parts.append(df)
        _sleep()
    # 东财概念 / 板块
    sce, spe = [], []
    for i, code in enumerate(codes):
        try:
            d1 = retry_remote(info.get_concept_east, stock_code=code)
            d1 = _validated_relation_shard(
                d1, source="concept_east", shard=code, expected_codes=[code]
            )
        except Exception as e:
            logger.warning("东财概念 %s：%s", code, e)
            fetch_failures.append(f"concept_east:{code}:{type(e).__name__}")
            d1 = pd.DataFrame()
        if d1 is not None and not d1.empty:
            d1 = d1.copy()
            d1["etl_sync_at"] = ts
            sce.append(d1)
        try:
            d2 = retry_remote(info.get_plate_east, stock_code=code, plate_type=None)
            d2 = _validated_relation_shard(
                d2, source="plate_east", shard=code, expected_codes=[code]
            )
        except Exception as e:
            logger.warning("东财板块 %s：%s", code, e)
            fetch_failures.append(f"plate_east:{code}:{type(e).__name__}")
            d2 = pd.DataFrame()
        if d2 is not None and not d2.empty:
            d2 = d2.copy()
            d2["etl_sync_at"] = ts
            spe.append(d2)
        if (i + 1) % 200 == 0:
            logger.info("东财概念/板块进度：%s/%s", i + 1, len(codes))
        _sleep()
    # 百度概念（批量）
    scb_parts: list[pd.DataFrame] = []
    for i in range(0, len(codes), batch):
        chunk = codes[i : i + batch]
        try:
            df = retry_remote(info.get_concept_baidu, stock_code=chunk)
            df = _validated_relation_shard(
                df,
                source="concept_baidu",
                shard=f"batch-{i // batch + 1}",
                expected_codes=chunk,
            )
        except Exception as e:
            logger.warning("百度概念 batch：%s", e)
            fetch_failures.append(
                f"concept_baidu:{','.join(chunk[:3])}:{type(e).__name__}"
            )
            df = pd.DataFrame()
        if df is not None and not df.empty:
            df = df.copy()
            df["etl_sync_at"] = ts
            scb_parts.append(df)
        _sleep()
    # 同花顺个股概念（单票，易风控）
    sct_parts: list[pd.DataFrame] = []
    for i, code in enumerate(codes):
        try:
            df = retry_remote(info.get_concept_ths, stock_code=code)
            df = _validated_relation_shard(
                df, source="concept_ths", shard=code, expected_codes=[code]
            )
        except Exception as e:
            logger.warning("同花顺个股概念 %s：%s", code, e)
            fetch_failures.append(f"concept_ths:{code}:{type(e).__name__}")
            df = pd.DataFrame()
        if _is_bad_ths_result(df):
            _sleep()
            continue
        if not df.empty:
            df = df.copy()
            df["etl_sync_at"] = ts
            sct_parts.append(df)
        if (i + 1) % 100 == 0:
            logger.info("同花顺个股概念进度：%s/%s", i + 1, len(codes))
        _sleep()
    if fetch_failures:
        raise RuntimeError(
            "per-stock snapshot fetches were incomplete; preserving all previous tables: "
            + " | ".join(fetch_failures[:10])
            + (f" | ... total={len(fetch_failures)}" if len(fetch_failures) > 10 else "")
        )

    snapshots = {
        "si_stock_shares": pd.concat(share_parts, ignore_index=True) if share_parts else pd.DataFrame(),
        "si_industry_sw": pd.concat(ind_parts, ignore_index=True) if ind_parts else pd.DataFrame(),
        "si_stock_concept_east": pd.concat(sce, ignore_index=True) if sce else pd.DataFrame(),
        "si_stock_plate_east": pd.concat(spe, ignore_index=True) if spe else pd.DataFrame(),
        "si_stock_concept_baidu": pd.concat(scb_parts, ignore_index=True) if scb_parts else pd.DataFrame(),
        "si_stock_concept_ths": pd.concat(sct_parts, ignore_index=True) if sct_parts else pd.DataFrame(),
    }
    empty_tables = [table for table, frame in snapshots.items() if frame.empty]
    if empty_tables:
        raise RuntimeError(
            "per-stock sources returned empty full-table snapshots; preserving all previous tables: "
            + ", ".join(empty_tables)
        )
    _replace_full_snapshots_atomically(engine, snapshots)


def _step(
    label: str,
    fn: Callable[..., Any],
    *args: Any,
    _failures: list[tuple[str, int]] | None = None,
    **kwargs: Any,
) -> Any:
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logger.exception("步骤 [%s] 失败，已跳过继续后续：%s", label, e)
        if _failures is not None:
            _failures.append((label, int(getattr(e, "exit_code", 1))))
        return None


def main() -> int:
    engine = create_batch_engine(future=True)
    logger.info("连接：%s", re.sub(r":([^:@]+)@", r":***@", str(engine.url)))
    run_ddl(engine)
    logger.info(
        "生产安全刷新：不执行开场全局清表；"
        "每张快照只在拉取与校验成功后事务替换。"
    )
    info = load_info()
    failures: list[tuple[str, int]] = []

    if os.environ.get("SI_SYNC_SKIP_ALL_CODE") == "1":
        df_code = read_frame(text("SELECT stock_code FROM si_all_code ORDER BY stock_code"), engine)
        if df_code.empty:
            logger.error("SI_SYNC_SKIP_ALL_CODE=1 但 si_all_code 为空，无法继续。")
            return 1
        logger.info("SI_SYNC_SKIP_ALL_CODE=1：从库读取 si_all_code，共 %s 只股票。", len(df_code))
    else:
        df_code = _step(
            "股票代码列表",
            sync_all_code,
            engine,
            info,
            _failures=failures,
        )

    _step("交易日历", sync_trade_calendar, engine, info, _failures=failures)
    df_idx = _step(
        "指数列表",
        sync_all_index_code,
        engine,
        info,
        _failures=failures,
    )
    if not isinstance(df_idx, pd.DataFrame):
        df_idx = pd.DataFrame()
    if not df_idx.empty:
        _step(
            "指数成分",
            sync_index_constituent,
            engine,
            info,
            df_idx,
            _failures=failures,
        )
    df_east = _step(
        "东财概念列表",
        sync_concept_code_east,
        engine,
        info,
        _failures=failures,
    )
    if not isinstance(df_east, pd.DataFrame):
        df_east = pd.DataFrame()
    if not df_east.empty:
        _step(
            "东财概念成分",
            sync_concept_constituent_east,
            engine,
            info,
            df_east,
            _failures=failures,
        )
    if not _use_qmt_sector_data():
        df_ths = _step(
            "同花顺概念列表",
            sync_concept_code_ths,
            engine,
            info,
            _failures=failures,
        )
        if not isinstance(df_ths, pd.DataFrame):
            df_ths = pd.DataFrame()
        if not df_ths.empty:
            _step(
                "同花顺概念成分",
                sync_concept_constituent_ths,
                engine,
                info,
                df_ths,
                _failures=failures,
            )
    if isinstance(df_code, pd.DataFrame) and not df_code.empty:
        _step(
            "个股维度表",
            sync_per_stock_tables,
            engine,
            info,
            df_code,
            _failures=failures,
        )

    if failures:
        status = "partial" if any(code == PartialSnapshotPublished.exit_code for _, code in failures) else "failed"
        exit_code = max(code for _, code in failures)
        logger.error(
            "STOCK-INFO 同步结果：status=%s exit_code=%s failures=%s",
            status,
            exit_code,
            failures,
        )
        return exit_code
    logger.info("STOCK-INFO 同步结果：status=complete exit_code=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
