# -*- coding: utf-8 -*-
"""
将 adata.sentiment（舆情-SENTIMENT 文档）相关接口写入 MySQL 库 ``probiga`` 的 ``st_*`` 表。

前置（仓库根目录）::

  pip install -e ./adata
  pip install -r requirements-platform.txt

执行::

  python -m biz.sentiment.sync_sentiment
  python -m biz.sentiment.sync_sentiment --only hot_concept

环境变量（可选）：
  MYSQL_URL           同 STOCK-INFO，必填；也可写入项目根目录 ``.env``
  SE_SKIP_DDL         ``1`` 跳过建表 SQL
  SE_SKIP_GLOBAL_TRUNCATE  已废弃；采集器始终先完成抓取，再原子替换对应快照
  SE_MARGIN_START     融资融券起始日，默认 ``2020-01-01``
  SE_NORTH_START      北向日度起始日，默认 ``2017-01-01``
  SE_A_LIST_DATE      龙虎榜日列表单日报告日，默认当天 ``YYYY-MM-DD``（与起止区间二选一）
  SE_A_LIST_DATE_START / SE_A_LIST_DATE_END  历史区间（含首尾）；可只设 START（END 默认今天）或只设 END（START=END 单日）
  SE_A_LIST_TRADING_DAYS_ONLY  设为 ``1`` 时仅拉 ``si_trade_calendar`` 中 ``trade_status=1`` 的日期（需已同步交易日历；查不到则退回按自然日遍历）
  SE_A_LIST_PROGRESS_EVERY  历史区间时每 N 天打一条进度日志，默认 ``20``
  SE_MAX_STOCKS       扫雷按 ``si_all_code`` 遍历数量，默认 ``300``；``0`` 表示不限制（慎用）
  SE_SKIP_MINE        ``1`` 跳过扫雷
  SE_A_LIST_INFO      是否拉取单股席位明细：默认 ``1``（执行）；设为 ``0`` 才跳过
  SE_A_LIST_INFO_MAX  明细每交易日最多股票数，默认 ``80``；设为 ``0`` 表示不限制（批量时与日表当日条数对齐，请求量会很大）
  SE_A_LIST_FROM_DB   仅当 ``--only a_list_info``（未包含 ``a_list_daily``）时：设为 ``1`` 则从库读 ``st_a_list_daily`` 取代码列表，不再先请求日表接口（适合已有人多日龙虎榜列表、只补明细）
  批量明细（与日表 ``trade_date`` 一一对应）：``SE_A_LIST_FROM_DB=1`` + ``--only a_list_info`` + 同时设 ``SE_A_LIST_DATE_START`` / ``SE_A_LIST_DATE_END``（可只设 START，END 默认今天）。将按区间内日表出现的每个 ``trade_date`` 逐日拉明细并按实际成功日期原子替换 ``st_a_list_info``。**此模式下会忽略环境变量 ``SE_A_LIST_DATE``**（避免会话里残留的「今天」导致误走单日逻辑）。若只要库里的单日明细且不设区间，请只设 ``SE_A_LIST_DATE`` 并**不要**同时设 START/END。
  SE_A_LIST_INFO_PROGRESS_EVERY  批量明细时每 N 个交易日打一条进度，默认 ``10``
  SE_REQUEST_SLEEP / SI_REQUEST_SLEEP  每次远程请求后的休眠秒数，默认 ``0.2``
  SE_HTTP_RETRIES / SI_HTTP_RETRIES、SE_HTTP_BACKOFF / SI_HTTP_BACKOFF  与 ``sync_stock_info`` 共用语义（重试）

说明：
  - 各步骤失败会打日志并继续后续步骤。
  - 北向分时若同花顺限流返回异常对象，本步会跳过写入。
  - 热门板块会写入两张表：``st_hot_concept_ths_daily``（按天留存）与 ``st_hot_concept_ths_rt``（盘中刷新）。
  - 支持 ``--only`` 单步执行（逗号分隔），便于按需单独刷新某类数据。
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import re
import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta
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
logger = logging.getLogger("sync_sentiment")

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

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_sentiment_tables.sql"
HOT_RANK_TOTAL = 100
HOT_CONCEPT_ROWS_PER_TYPE = 20

TABLES_TRUNCATE_ORDER = [
    "st_mine_clearance_tdx",
    "st_a_list_info",
    "st_a_list_daily",
    "st_hot_concept_ths_rt",
    "st_hot_rank_ths",
    "st_hot_pop_rank_east",
    "st_north_flow_current",
    "st_north_flow_min",
    "st_north_flow_daily",
    "st_securities_margin",
    "st_stock_lifting_last_month",
]


def _sleep() -> None:
    time.sleep(
        float(
            os.environ.get(
                "SE_REQUEST_SLEEP",
                os.environ.get("SI_REQUEST_SLEEP", "0.2"),
            )
        )
    )


def retry_remote(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    max_retries = max(
        1,
        int(os.environ.get("SE_HTTP_RETRIES", os.environ.get("SI_HTTP_RETRIES", "8"))),
    )
    base = float(os.environ.get("SE_HTTP_BACKOFF", os.environ.get("SI_HTTP_BACKOFF", "3.0")))
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


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _clean_object_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.replace({np.nan: None, pd.NaT: None})


def validate_sentiment_runtime_schema(engine: Engine) -> dict[str, Any]:
    return validate_required_table_surface(
        engine,
        TABLES_TRUNCATE_ORDER,
        context="sentiment collector",
        required_columns={
            "st_a_list_daily": {"stock_code", "trade_date", "etl_sync_at"},
            "st_a_list_info": {"stock_code", "etl_sync_at"},
        },
    )


def run_ddl(engine: Engine) -> None:
    """Legacy entrypoint retained as a read-only prepared-schema guard."""

    validate_sentiment_runtime_schema(engine)


def truncate_all_sentiment(engine: Engine) -> None:
    del engine
    raise RuntimeError(
        "全局清空舆情表已禁用；每个采集步骤必须抓取并校验后原子替换"
    )


def truncate_only(engine: Engine, *table_names: str) -> None:
    del engine, table_names
    raise RuntimeError(
        "预清空舆情表已禁用；完整替换必须在一次数据库事务中提交"
    )


def df_to_table(engine: Engine, df: pd.DataFrame, table: str) -> None:
    """Fail closed: an arbitrary frame is never proof of a complete table."""

    del engine, df, table
    raise RuntimeError(
        "通用全表替换已禁用；调用方必须声明并验证精确业务键或完整快照覆盖证据"
    )


def _with_etl(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out["etl_sync_at"] = _now()
    return out


def _request_date() -> str:
    """Return the collector-side request/capture date."""

    return datetime.now().strftime("%Y-%m-%d")


def _require_frame(frame: Any, *, label: str) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise RuntimeError(f"{label} returned no publishable rows")
    return frame.copy()


def _normalize_date_column(
    frame: pd.DataFrame,
    column: str,
    *,
    label: str,
    include_time: bool = False,
) -> pd.DataFrame:
    if column not in frame.columns:
        raise RuntimeError(f"{label} omitted required coverage column {column}")
    parsed = pd.to_datetime(frame[column], errors="coerce")
    if parsed.isna().any():
        raise RuntimeError(f"{label} returned invalid {column} coverage evidence")
    clean = frame.copy()
    clean[column] = parsed.dt.strftime(
        "%Y-%m-%d %H:%M:%S" if include_time else "%Y-%m-%d"
    )
    return clean


def _validate_requested_date_range(
    frame: Any,
    *,
    date_column: str,
    requested_start: str,
    requested_end: str,
    label: str,
) -> pd.DataFrame:
    clean = _normalize_date_column(
        _require_frame(frame, label=label),
        date_column,
        label=label,
    )
    start = pd.Timestamp(requested_start).normalize()
    end = pd.Timestamp(requested_end).normalize()
    observed = pd.to_datetime(clean[date_column], errors="raise")
    outside = (observed < start) | (observed > end)
    if outside.any():
        bad = sorted(set(clean.loc[outside, date_column].astype(str)))
        raise RuntimeError(
            f"{label} date coverage mismatch: requested={requested_start}..{requested_end} "
            f"observed_outside={bad}"
        )
    return clean


def _replace_observed_business_keys(
    engine: Engine,
    frame: pd.DataFrame,
    table_name: str,
    *,
    key_columns: tuple[str, ...],
    label: str,
) -> None:
    """Atomically replace only business identities proven by returned rows."""

    clean = _require_frame(frame, label=label)
    missing = sorted(set(key_columns) - set(clean.columns))
    if missing:
        raise RuntimeError(f"{label} omitted business-key columns: {missing}")
    for column in key_columns:
        values = clean[column]
        if values.isna().any() or values.astype(str).str.strip().eq("").any():
            raise RuntimeError(f"{label} contains empty business key {column}")
        quote_identifier(column)
    if clean.duplicated(subset=list(key_columns), keep=False).any():
        raise RuntimeError(
            f"{label} returned duplicate business identities for {key_columns}"
        )

    params: dict[str, Any] = {}
    if len(key_columns) == 1:
        column = key_columns[0]
        values = sorted(clean[column].unique().tolist(), key=str)
        for index, value in enumerate(values):
            params[f"key_{index}"] = value
        where_sql = (
            f"{quote_identifier(column)} IN ("
            + ", ".join(f":key_{index}" for index in range(len(values)))
            + ")"
        )
    else:
        clauses: list[str] = []
        identities = clean[list(key_columns)].drop_duplicates().itertuples(
            index=False,
            name=None,
        )
        for row_index, identity in enumerate(identities):
            terms: list[str] = []
            for column_index, (column, value) in enumerate(
                zip(key_columns, identity)
            ):
                key = f"key_{row_index}_{column_index}"
                params[key] = value
                terms.append(f"{quote_identifier(column)} = :{key}")
            clauses.append("(" + " AND ".join(terms) + ")")
        where_sql = " OR ".join(clauses)
    if not params or not where_sql:
        raise RuntimeError(f"{label} has no proven business identities")
    replace_table_rows(
        _clean_object_df(clean),
        table_name,
        engine,
        where_sql=where_sql,
        params=params,
        chunksize=1000,
        method="multi",
    )
    logger.info("表 %s：按实际成功业务键更新 %s 行。", table_name, len(clean))


def _validate_intraday_rows(
    frame: Any,
    *,
    request_date: str,
    label: str,
    expected_rows: int | None = None,
) -> pd.DataFrame:
    clean = _normalize_date_column(
        _require_frame(frame, label=label),
        "trade_time",
        label=label,
        include_time=True,
    )
    if expected_rows is not None and len(clean) != expected_rows:
        raise RuntimeError(
            f"{label} row coverage mismatch: expected={expected_rows} observed={len(clean)}"
        )
    dates = set(clean["trade_time"].str.slice(0, 10))
    if len(dates) != 1 or next(iter(dates)) > request_date:
        raise RuntimeError(
            f"{label} request-date coverage mismatch: requested_at={request_date} "
            f"observed={sorted(dates)}"
        )
    return clean


def _validate_top_n_snapshot(
    frame: Any,
    *,
    request_date: str,
    label: str,
    expected_rows: int,
) -> pd.DataFrame:
    """Require explicit request-date and complete contiguous Top-N evidence."""

    clean = _require_frame(frame, label=label)
    required = {"rank", "stock_code"}
    missing = sorted(required - set(clean.columns))
    if missing:
        raise RuntimeError(f"{label} omitted snapshot coverage columns: {missing}")
    if "snapshot_date" in clean.columns:
        clean = _normalize_date_column(
            clean,
            "snapshot_date",
            label=label,
        )
        observed_dates = set(clean["snapshot_date"])
        if observed_dates != {request_date}:
            raise RuntimeError(
                f"{label} request-date coverage mismatch: requested={request_date} "
                f"observed={sorted(observed_dates)}"
            )
    else:
        clean["snapshot_date"] = request_date

    numeric_rank = pd.to_numeric(clean["rank"], errors="coerce")
    if numeric_rank.isna().any() or not numeric_rank.eq(numeric_rank.astype(int)).all():
        raise RuntimeError(f"{label} returned invalid rank evidence")
    clean["rank"] = numeric_rank.astype(int)
    clean["stock_code"] = clean["stock_code"].astype(str).str.strip().str.zfill(6)
    ranks = set(clean["rank"])
    codes = set(clean["stock_code"])
    expected_ranks = set(range(1, expected_rows + 1))
    if (
        len(clean) != expected_rows
        or ranks != expected_ranks
        or len(codes) != expected_rows
        or not clean["stock_code"].str.fullmatch(r"[0-9]{6}", na=False).all()
    ):
        raise RuntimeError(
            f"{label} incomplete Top-{expected_rows} snapshot: rows={len(clean)} "
            f"ranks={len(ranks)} codes={len(codes)}"
        )
    return clean


def step_lifting(engine: Engine, sentiment: Any) -> None:
    df = _normalize_date_column(
        _require_frame(
            retry_remote(sentiment.stock_lifting_last_month),
            label="stock lifting",
        ),
        "lift_date",
        label="stock lifting",
    )
    if "stock_code" not in df.columns:
        raise RuntimeError("stock lifting omitted business-key column stock_code")
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    if not df["stock_code"].str.fullmatch(r"[0-9]{6}", na=False).all():
        raise RuntimeError("stock lifting returned invalid stock_code evidence")
    for c in ("volume", "amount", "ratio", "price"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    _replace_observed_business_keys(
        engine,
        _with_etl(df),
        "st_stock_lifting_last_month",
        key_columns=("lift_date", "stock_code"),
        label="stock lifting",
    )
    _sleep()


def step_margin(engine: Engine, sentiment: Any) -> None:
    start = os.environ.get("SE_MARGIN_START", "2020-01-01")
    request_date = _request_date()
    df = _validate_requested_date_range(
        retry_remote(sentiment.securities_margin, start_date=start),
        date_column="trade_date",
        requested_start=start,
        requested_end=request_date,
        label="securities margin",
    )
    _replace_observed_business_keys(
        engine,
        _with_etl(df),
        "st_securities_margin",
        key_columns=("trade_date",),
        label="securities margin",
    )
    _sleep()


def step_north_daily(engine: Engine, sentiment: Any) -> None:
    start = os.environ.get("SE_NORTH_START", "2017-01-01")
    request_date = _request_date()
    df = _validate_requested_date_range(
        retry_remote(sentiment.north.north_flow, start_date=start),
        date_column="trade_date",
        requested_start=start,
        requested_end=request_date,
        label="north flow daily",
    )
    _replace_observed_business_keys(
        engine,
        _with_etl(df),
        "st_north_flow_daily",
        key_columns=("trade_date",),
        label="north flow daily",
    )
    _sleep()


def step_north_min(engine: Engine, sentiment: Any) -> None:
    request_date = _request_date()
    raw = retry_remote(sentiment.north.north_flow_min)
    if isinstance(raw, Exception):
        raise RuntimeError(f"north flow minute provider failed: {raw}")
    df = _validate_intraday_rows(
        raw,
        request_date=request_date,
        label="north flow minute",
    )
    _replace_observed_business_keys(
        engine,
        _with_etl(df),
        "st_north_flow_min",
        key_columns=("trade_time",),
        label="north flow minute",
    )
    _sleep()


def step_north_current(engine: Engine, sentiment: Any) -> None:
    request_date = _request_date()
    raw = retry_remote(sentiment.north.north_flow_current)
    if isinstance(raw, Exception):
        raise RuntimeError(f"north flow current provider failed: {raw}")
    df = _validate_intraday_rows(
        raw,
        request_date=request_date,
        label="north flow current",
        expected_rows=1,
    )
    _replace_observed_business_keys(
        engine,
        _with_etl(df),
        "st_north_flow_current",
        key_columns=("trade_time",),
        label="north flow current",
    )
    _sleep()


def step_hot_east(engine: Engine, sentiment: Any) -> None:
    request_date = _request_date()
    df = retry_remote(sentiment.hot.pop_rank_100_east)
    if isinstance(df, pd.DataFrame) and "change" in df.columns:
        df = df.rename(columns={"change": "price_change"})
    df = _validate_top_n_snapshot(
        df,
        request_date=request_date,
        label="Eastmoney hot rank",
        expected_rows=HOT_RANK_TOTAL,
    )
    _replace_observed_business_keys(
        engine,
        _with_etl(df),
        "st_hot_pop_rank_east",
        key_columns=("snapshot_date", "rank"),
        label="Eastmoney hot rank",
    )
    _sleep()


def step_hot_ths(engine: Engine, sentiment: Any) -> None:
    request_date = _request_date()
    df = _validate_top_n_snapshot(
        retry_remote(sentiment.hot.hot_rank_100_ths),
        request_date=request_date,
        label="THS hot rank",
        expected_rows=HOT_RANK_TOTAL,
    )
    _replace_observed_business_keys(
        engine,
        _with_etl(df),
        "st_hot_rank_ths",
        key_columns=("snapshot_date", "rank"),
        label="THS hot rank",
    )
    _sleep()


def _expand_a_list_report_dates(engine: Engine, start: datetime, end: datetime) -> list[str]:
    """按自然日或交易日展开 [start, end]。"""
    start_s = start.strftime("%Y-%m-%d")
    end_s = end.strftime("%Y-%m-%d")
    if os.environ.get("SE_A_LIST_TRADING_DAYS_ONLY", "0").strip() == "1":
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT trade_date FROM si_trade_calendar "
                        "WHERE trade_status = 1 AND trade_date >= :s AND trade_date <= :e "
                        "ORDER BY trade_date"
                    ),
                    {"s": start_s, "e": end_s},
                ).fetchall()
            if rows:
                out: list[str] = []
                for r in rows:
                    v = r[0]
                    out.append(v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v)[:10])
                return out
            logger.warning(
                "SE_A_LIST_TRADING_DAYS_ONLY=1 但 si_trade_calendar 在 %s~%s 无记录，改按自然日遍历。",
                start_s,
                end_s,
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("读取 si_trade_calendar 失败，改按自然日遍历：%s", e)
    out = []
    d = start
    while d <= end:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out


def _a_list_report_dates(engine: Engine) -> list[str]:
    """单日或区间：区间由 SE_A_LIST_DATE_START/END（可只填一端）决定。"""
    start_s = (os.environ.get("SE_A_LIST_DATE_START") or "").strip()
    end_s = (os.environ.get("SE_A_LIST_DATE_END") or "").strip()
    today = datetime.now().strftime("%Y-%m-%d")
    if start_s and not end_s:
        end_s = today
    if end_s and not start_s:
        start_s = end_s
    if start_s and end_s:
        start = datetime.strptime(start_s, "%Y-%m-%d")
        end = datetime.strptime(end_s, "%Y-%m-%d")
        if end < start:
            raise ValueError("SE_A_LIST_DATE_END 早于 SE_A_LIST_DATE_START")
        dates = _expand_a_list_report_dates(engine, start, end)
        logger.info(
            "龙虎榜日列表：区间 %s ~ %s，共 %s 个待请求日期（TRADING_ONLY=%s）。",
            start_s,
            end_s,
            len(dates),
            os.environ.get("SE_A_LIST_TRADING_DAYS_ONLY", "0"),
        )
        return dates
    report = (os.environ.get("SE_A_LIST_DATE") or "").strip() or today
    return [report]


def _coerce_a_list_daily_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
    for c in (
        "close",
        "change_cpt",
        "turnover_ratio",
        "a_net_amount",
        "a_buy_amount",
        "a_sell_amount",
        "a_amount",
        "amount",
        "net_amount_rate",
        "a_amount_rate",
    ):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _validate_a_list_shard(
    frame: Any,
    *,
    report_date: str,
    stock_code: str | None = None,
) -> pd.DataFrame:
    """Validate the date/code evidence for one independently replaceable shard."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ValueError("provider returned an empty a-list shard")
    required = {"trade_date", "stock_code"}
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        raise ValueError(f"a-list shard omitted coverage columns: {missing_columns}")
    clean = frame.copy()
    clean["trade_date"] = pd.to_datetime(clean["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    clean["stock_code"] = clean["stock_code"].astype(str).str.strip().str.zfill(6)
    if not clean["stock_code"].str.fullmatch(r"[0-9]{6}", na=False).all():
        raise ValueError("a-list shard contains invalid stock_code evidence")
    observed_dates = set(clean["trade_date"].dropna().unique())
    expected_date = str(report_date)[:10]
    if observed_dates != {expected_date}:
        raise ValueError(
            f"a-list date coverage mismatch: requested={expected_date} observed={sorted(observed_dates)}"
        )
    if stock_code is not None:
        expected_code = str(stock_code).strip().zfill(6)
        observed_codes = set(clean["stock_code"].dropna().unique())
        if observed_codes != {expected_code}:
            raise ValueError(
                f"a-list code coverage mismatch: requested={expected_code} "
                f"observed={sorted(observed_codes)}"
            )
    return clean


def _publish_hot_concept_snapshots(
    engine: Engine,
    *,
    realtime: pd.DataFrame,
    daily: pd.DataFrame,
    snapshot_date: str,
) -> None:
    """Publish RT and daily views on one connection and one transaction."""

    realtime = _require_frame(realtime, label="hot concept realtime snapshot")
    daily = _require_frame(daily, label="hot concept daily snapshot")
    if len(realtime) != len(daily):
        raise RuntimeError(
            "hot concept RT/daily row coverage mismatch before atomic publish"
        )
    observed_dates = set(daily.get("snapshot_date", pd.Series(dtype=str)).astype(str))
    if observed_dates != {snapshot_date}:
        raise RuntimeError(
            "hot concept daily request-date coverage mismatch: "
            f"requested={snapshot_date} observed={sorted(observed_dates)}"
        )

    # Both physical views become visible together. Any DELETE or INSERT error,
    # including the second table write, rolls the first table back as well.
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM `st_hot_concept_ths_rt`"))
        write_frame(
            _clean_object_df(realtime),
            "st_hot_concept_ths_rt",
            connection,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
        connection.execute(
            text(
                "DELETE FROM `st_hot_concept_ths_daily` "
                "WHERE `snapshot_date` = :snapshot_date "
                "AND `plate_type` IN (1, 2)"
            ),
            {"snapshot_date": snapshot_date},
        )
        write_frame(
            _clean_object_df(daily),
            "st_hot_concept_ths_daily",
            connection,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )


def step_hot_concept(engine: Engine, sentiment: Any) -> None:
    parts = []
    failed_shards: list[str] = []
    expected_rows = HOT_CONCEPT_ROWS_PER_TYPE
    request_date = _request_date()
    for pt in (1, 2):
        if pt == 2:
            _sleep()
        try:
            d = retry_remote(sentiment.hot.hot_concept_20_ths, plate_type=pt)
        except Exception as exc:  # pylint: disable=broad-except
            failed_shards.append(f"plate_type={pt}:{type(exc).__name__}")
            continue
        if isinstance(d, pd.DataFrame) and not d.empty:
            d = d.copy()
            if not {"rank", "concept_code"}.issubset(d.columns):
                failed_shards.append(f"plate_type={pt}:missing-rank-or-code")
                continue
            ranks = set(pd.to_numeric(d["rank"], errors="coerce").dropna().astype(int))
            codes = set(d["concept_code"].dropna().astype(str).str.strip()) - {""}
            if (
                len(d) != expected_rows
                or ranks != set(range(1, expected_rows + 1))
                or len(codes) != expected_rows
            ):
                failed_shards.append(
                    f"plate_type={pt}:coverage={len(codes)}/{expected_rows}"
                )
                continue
            d["plate_type"] = pt
            parts.append(d)
        else:
            failed_shards.append(f"plate_type={pt}:empty-or-invalid")
    if failed_shards or len(parts) != 2:
        raise RuntimeError(
            "hot concept snapshot is incomplete; source=ths "
            f"successful={len(parts)}/2 failures={failed_shards}; preserving previous snapshots"
        )
    df = pd.concat(parts, ignore_index=True)
    rt_df = _with_etl(df)

    # 日留存与实时视图在同一连接、同一事务发布。
    daily_df = rt_df.copy()
    daily_df["snapshot_date"] = request_date
    daily_snapshot = daily_df[
        [
            "snapshot_date",
            "plate_type",
            "rank",
            "concept_code",
            "concept_name",
            "change_pct",
            "hot_value",
            "hot_tag",
            "etl_sync_at",
        ]
    ]
    _publish_hot_concept_snapshots(
        engine,
        realtime=rt_df,
        daily=daily_snapshot,
        snapshot_date=request_date,
    )


def step_a_list_daily(engine: Engine, sentiment: Any) -> pd.DataFrame:
    dates = _a_list_report_dates(engine)
    parts: list[pd.DataFrame] = []
    successful_dates: list[str] = []
    for i, report in enumerate(dates):
        try:
            one = retry_remote(sentiment.hot.list_a_list_daily, report_date=report)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("龙虎榜日列表 %s 请求失败：%s", report, e)
            one = None
        if one is not None and not one.empty:
            try:
                validated = _validate_a_list_shard(
                    _coerce_a_list_daily_columns(one),
                    report_date=report,
                )
            except ValueError as exc:
                logger.warning("龙虎榜日列表 %s 覆盖校验失败：%s", report, exc)
            else:
                parts.append(validated)
                successful_dates.append(str(report)[:10])
        else:
            logger.warning("龙虎榜日列表 %s 返回空，保留该日期旧分区。", report)
        _sleep()
        n = len(dates)
        if n > 1 and (i + 1) % max(1, int(os.environ.get("SE_A_LIST_PROGRESS_EVERY", "20"))) == 0:
            logger.info("龙虎榜日列表历史进度：%s/%s（当前 %s）", i + 1, n, report)

    if not parts:
        raise RuntimeError("no a-list daily rows fetched")
    df = pd.concat(parts, ignore_index=True)
    if "trade_date" in df.columns and "stock_code" in df.columns:
        df = df.drop_duplicates(subset=["trade_date", "stock_code"], keep="last")
    df = _coerce_a_list_daily_columns(df)
    _replace_a_list_dates(
        engine,
        "st_a_list_daily",
        df,
        expected_dates=successful_dates,
    )
    _sleep()
    return df


def _finalize_a_list_info_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
    for c in ("a_net_amount", "a_buy_amount", "a_sell_amount", "a_buy_amount_rate", "a_sell_amount_rate"):
        if c in df.columns:
            # The target schema stores DECIMAL(..., 6).  Round before duplicate
            # detection so rows that differ only beyond database precision do
            # not become physical duplicates after INSERT.
            df[c] = pd.to_numeric(df[c], errors="coerce").round(6)
    # The Eastmoney BUY and SELL reports overlap: a seat that appears on both
    # lists is returned twice with identical combined buy/sell amounts.  Keep
    # one physical row so downstream SUM(a_net_amount) does not double count
    # the same seat.
    business_columns = [
        column
        for column in (
            "trade_date", "stock_code", "operate_code", "operate_name",
            "a_net_amount", "a_buy_amount", "a_sell_amount",
            "a_buy_amount_rate", "a_sell_amount_rate", "reason",
        )
        if column in df.columns
    ]
    if business_columns:
        df = df.drop_duplicates(subset=business_columns, keep="first")
    return df


def _replace_a_list_dates(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    *,
    expected_dates: list[str] | None = None,
) -> None:
    """Atomically replace only the fetched dragon-tiger trade dates.

    Daily scheduler runs and partial historical retries must never erase older
    successful dates. A failed/empty provider date is intentionally excluded
    from the replacement predicate so the last good snapshot is retained.
    """
    if df is None or df.empty or "trade_date" not in df.columns:
        raise ValueError(f"{table_name} replacement requires trade_date rows")
    trade_dates = sorted({
        str(value)[:10]
        for value in df["trade_date"].dropna().tolist()
        if str(value).strip()
    })
    if not trade_dates:
        raise ValueError(f"{table_name} replacement has no valid trade dates")
    if expected_dates is not None:
        expected = sorted({str(value)[:10] for value in expected_dates if str(value).strip()})
        if trade_dates != expected:
            raise ValueError(
                f"{table_name} date coverage mismatch: expected={expected} observed={trade_dates}"
            )
    params = {
        f"trade_date_{index}": value
        for index, value in enumerate(trade_dates)
    }
    placeholders = ", ".join(f":{key}" for key in params)
    replace_table_rows(
        _with_etl(df),
        table_name,
        engine,
        where_sql=f"trade_date IN ({placeholders})",
        params=params,
    )


def _replace_a_list_info_partitions(
    engine: Engine,
    df: pd.DataFrame,
    *,
    expected_partitions: list[tuple[str, str]],
) -> None:
    """Replace only proven (trade_date, stock_code) detail partitions."""

    if df is None or df.empty:
        raise ValueError("st_a_list_info replacement requires non-empty rows")
    if not {"trade_date", "stock_code"}.issubset(df.columns):
        raise ValueError("st_a_list_info replacement requires trade_date and stock_code")
    clean = df.copy()
    clean["trade_date"] = pd.to_datetime(clean["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    clean["stock_code"] = clean["stock_code"].astype(str).str.strip().str.zfill(6)
    observed = {
        (str(row.trade_date)[:10], str(row.stock_code).strip().zfill(6))
        for row in clean[["trade_date", "stock_code"]].dropna().itertuples(index=False)
    }
    expected = {
        (str(trade_date)[:10], str(stock_code).strip().zfill(6))
        for trade_date, stock_code in expected_partitions
    }
    if not expected or observed != expected:
        raise ValueError(
            f"st_a_list_info partition coverage mismatch: expected={sorted(expected)} "
            f"observed={sorted(observed)}"
        )
    clauses: list[str] = []
    params: dict[str, str] = {}
    for index, (trade_date, stock_code) in enumerate(sorted(expected)):
        date_key = f"trade_date_{index}"
        code_key = f"stock_code_{index}"
        clauses.append(f"(trade_date = :{date_key} AND stock_code = :{code_key})")
        params[date_key] = trade_date
        params[code_key] = stock_code
    replace_table_rows(
        _with_etl(clean),
        "st_a_list_info",
        engine,
        where_sql=" OR ".join(clauses),
        params=params,
    )


def step_a_list_info_batch_from_db(engine: Engine, sentiment: Any, start_s: str, end_s: str) -> None:
    """
    按 st_a_list_daily 在 [start_s, end_s] 内出现的每个 trade_date，逐日拉席位明细并合并写入 st_a_list_info。
    """
    if os.environ.get("SE_A_LIST_INFO", "1") != "1":
        logger.info("已设置 SE_A_LIST_INFO!=1，跳过批量龙虎榜明细。")
        return
    if start_s > end_s:
        logger.warning("SE_A_LIST_DATE_END 早于 START，跳过批量明细。")
        return
    with engine.connect() as conn:
        dates = conn.execute(
            text(
                "SELECT DISTINCT trade_date FROM st_a_list_daily "
                "WHERE trade_date >= :s AND trade_date <= :e ORDER BY trade_date"
            ),
            {"s": start_s, "e": end_s},
        ).fetchall()
    if not dates:
        logger.warning("st_a_list_daily 在 %s ~ %s 内无 trade_date，跳过明细。", start_s, end_s)
        return
    cap_raw = int(os.environ.get("SE_A_LIST_INFO_MAX", "80"))
    cap = 10**9 if cap_raw <= 0 else max(1, cap_raw)
    cap_desc = "不限制" if cap_raw <= 0 else str(cap)
    every = max(1, int(os.environ.get("SE_A_LIST_INFO_PROGRESS_EVERY", "10")))
    all_rows: list[pd.DataFrame] = []
    successful_partitions: list[tuple[str, str]] = []
    n_dates = len(dates)
    logger.info(
        "龙虎榜明细批量：区间内共 %s 个交易日待拉取（每日最多 %s 只股票；全部完成后一次性写入表）。",
        n_dates,
        cap_desc,
    )
    for idx, row in enumerate(dates):
        td = row[0]
        report = td.strftime("%Y-%m-%d") if hasattr(td, "strftime") else str(td)[:10]
        with engine.connect() as conn:
            crow = conn.execute(
                text(
                    "SELECT DISTINCT stock_code FROM st_a_list_daily "
                    "WHERE trade_date = :d ORDER BY stock_code"
                ),
                {"d": report},
            ).fetchall()
        codes = [str(r[0]).strip() for r in crow if r[0] is not None][:cap]
        if not codes:
            _sleep()
            continue
        for code in codes:
            try:
                _sleep()
                one = retry_remote(sentiment.hot.get_a_list_info, stock_code=code, report_date=report)
                if one is not None and not one.empty:
                    validated = _validate_a_list_shard(
                        one,
                        report_date=report,
                        stock_code=code,
                    )
                    all_rows.append(validated)
                    successful_partitions.append((report, code))
                else:
                    logger.warning(
                        "龙虎榜明细 %s %s 返回空，保留该股票旧分区。",
                        report,
                        code,
                    )
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("龙虎榜明细 %s %s 失败：%s", report, code, e)
        if (idx + 1) % every == 0 or idx + 1 == n_dates:
            logger.info(
                "龙虎榜明细批量进度：%s/%s 个交易日（当前 %s，已累积 %s 段明细）",
                idx + 1,
                n_dates,
                report,
                len(all_rows),
            )
    if not all_rows:
        raise RuntimeError("no a-list info rows fetched")
    df = pd.concat(all_rows, ignore_index=True)
    df = _finalize_a_list_info_df(df)
    _replace_a_list_info_partitions(
        engine,
        df,
        expected_partitions=successful_partitions,
    )


def step_a_list_info(engine: Engine, sentiment: Any, daily_df: pd.DataFrame) -> None:
    if os.environ.get("SE_A_LIST_INFO", "1") != "1":
        logger.info("已设置 SE_A_LIST_INFO!=1，跳过单股龙虎榜明细。")
        return
    if daily_df is None or daily_df.empty or "stock_code" not in daily_df.columns:
        raise RuntimeError("st_a_list_daily is empty; cannot fetch a-list info")
    explicit_date = (os.environ.get("SE_A_LIST_DATE") or "").strip()
    codes_df = daily_df
    report = explicit_date or datetime.now().strftime("%Y-%m-%d")
    if "trade_date" in daily_df.columns:
        td = daily_df["trade_date"].dropna().astype(str).str.slice(0, 10).unique()
        if len(td) == 1 and not explicit_date:
            report = td[0]
        if len(td) > 1 and not explicit_date:
            logger.warning(
                "st_a_list_daily 含多个交易日（%s 天），跳过 st_a_list_info。"
                "需要明细时请单独执行 --only a_list_info 并设置 SE_A_LIST_DATE=目标交易日。",
                len(td),
            )
            return
        if len(td) > 1 and explicit_date:
            m = daily_df["trade_date"].astype(str).str.slice(0, 10) == explicit_date[:10]
            codes_df = daily_df.loc[m]
            if codes_df.empty:
                logger.warning("SE_A_LIST_DATE=%s 在日表数据中无对应行，跳过 st_a_list_info。", explicit_date)
                return
            report = explicit_date[:10]
    cap_raw = int(os.environ.get("SE_A_LIST_INFO_MAX", "80"))
    cap = 10**9 if cap_raw <= 0 else max(1, cap_raw)
    codes = codes_df["stock_code"].dropna().astype(str).unique().tolist()[:cap]
    rows: list[pd.DataFrame] = []
    successful_partitions: list[tuple[str, str]] = []
    for code in codes:
        try:
            _sleep()
            one = retry_remote(sentiment.hot.get_a_list_info, stock_code=code, report_date=report)
            if one is not None and not one.empty:
                validated = _validate_a_list_shard(
                    one,
                    report_date=report,
                    stock_code=code,
                )
                rows.append(validated)
                successful_partitions.append((report, code))
            else:
                logger.warning(
                    "龙虎榜明细 %s %s 返回空，保留该股票旧分区。",
                    report,
                    code,
                )
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("龙虎榜明细 %s 失败：%s", code, e)
    if not rows:
        raise RuntimeError("no a-list info rows fetched")
    df = pd.concat(rows, ignore_index=True)
    df = _finalize_a_list_info_df(df)
    _replace_a_list_info_partitions(
        engine,
        df,
        expected_partitions=successful_partitions,
    )


def step_mine(engine: Engine, sentiment: Any) -> None:
    if os.environ.get("SE_SKIP_MINE") == "1":
        logger.info("已设置 SE_SKIP_MINE=1，跳过扫雷。")
        return
    lim = int(os.environ.get("SE_MAX_STOCKS", "300"))
    q = "SELECT stock_code FROM si_all_code ORDER BY stock_code"
    if lim > 0:
        q += f" LIMIT {lim}"
    with engine.connect() as conn:
        codes = [r[0] for r in conn.execute(text(q)).fetchall()]
    if not codes:
        logger.warning("si_all_code 无数据，跳过扫雷。")
        return
    parts: list[pd.DataFrame] = []
    for code in codes:
        try:
            _sleep()
            one = retry_remote(sentiment.mine.mine_clearance_tdx, stock_code=str(code))
            if one is not None and not one.empty:
                if "stock_code" not in one.columns:
                    logger.warning("扫雷 %s 结果缺少 stock_code，保留旧分区。", code)
                    continue
                validated = one.copy()
                validated["stock_code"] = validated["stock_code"].astype(str).str.strip().str.zfill(6)
                expected_code = str(code).strip().zfill(6)
                if set(validated["stock_code"].dropna().unique()) != {expected_code}:
                    logger.warning("扫雷 %s 返回错代码，保留旧分区。", code)
                    continue
                parts.append(validated)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("扫雷 %s 失败：%s", code, e)
    if not parts:
        logger.info("表 st_mine_clearance_tdx：无数据。")
        return
    df = pd.concat(parts, ignore_index=True)
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    prepared = _with_etl(df)
    if "stock_code" not in prepared.columns:
        raise RuntimeError("扫雷结果缺少 stock_code，保留上一版快照")
    refreshed_codes = sorted({
        str(value).strip().zfill(6)
        for value in prepared["stock_code"].dropna().tolist()
        if str(value).strip()
    })
    if not refreshed_codes:
        raise RuntimeError("扫雷结果没有有效股票代码，保留上一版快照")
    prepared = prepared.copy()
    prepared["stock_code"] = prepared["stock_code"].astype(str).str.zfill(6)
    params = {
        f"stock_code_{index}": code
        for index, code in enumerate(refreshed_codes)
    }
    replace_table_rows(
        prepared,
        "st_mine_clearance_tdx",
        engine,
        where_sql="stock_code IN (" + ", ".join(
            f":{name}" for name in params
        ) + ")",
        params=params,
    )


STEP_NAMES = {
    "lifting",
    "margin",
    "north_daily",
    "north_min",
    "north_current",
    "hot_east",
    "hot_ths",
    "hot_concept",
    "a_list_daily",
    "a_list_info",
    "mine",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SENTIMENT 数据同步到 MySQL（支持按步骤单独执行）")
    parser.add_argument(
        "--only",
        type=str,
        default="",
        help=(
            "只执行指定步骤，逗号分隔。"
            "可选：lifting,margin,north_daily,north_min,north_current,hot_east,hot_ths,hot_concept,a_list_daily,a_list_info,mine"
        ),
    )
    return parser.parse_args()


def _parse_only_set(only_raw: str) -> set[str]:
    if not only_raw.strip():
        return set()
    only_set = {x.strip() for x in only_raw.split(",") if x.strip()}
    bad = sorted(only_set - STEP_NAMES)
    if bad:
        raise ValueError(f"--only 存在无效步骤：{','.join(bad)}；可选：{','.join(sorted(STEP_NAMES))}")
    return only_set


def _should_run(step: str, only_set: set[str]) -> bool:
    return not only_set or step in only_set


def main() -> int:
    args = _parse_args()
    only_set = _parse_only_set(args.only)

    engine = create_batch_engine()
    run_ddl(engine)
    if os.environ.get("SE_SKIP_GLOBAL_TRUNCATE") not in {None, "", "1"}:
        logger.warning("SE_SKIP_GLOBAL_TRUNCATE 已废弃；运行时始终禁止预清表。")

    from adata.sentiment import sentiment

    steps = [
        ("lifting", "解禁-最近一月", lambda: step_lifting(engine, sentiment)),
        ("margin", "融资融券", lambda: step_margin(engine, sentiment)),
        ("north_daily", "北向日度", lambda: step_north_daily(engine, sentiment)),
        ("north_min", "北向分时", lambda: step_north_min(engine, sentiment)),
        ("north_current", "北向当前", lambda: step_north_current(engine, sentiment)),
        ("hot_east", "东财人气榜", lambda: step_hot_east(engine, sentiment)),
        ("hot_ths", "同花顺热股", lambda: step_hot_ths(engine, sentiment)),
        ("hot_concept", "同花顺热门板块", lambda: step_hot_concept(engine, sentiment)),
    ]
    failed_steps = 0
    for key, name, fn in steps:
        if not _should_run(key, only_set):
            continue
        try:
            fn()
        except Exception as e:  # pylint: disable=broad-except
            logger.exception("步骤「%s」失败：%s", name, e)
            failed_steps += 1

    explicit = (os.environ.get("SE_A_LIST_DATE") or "").strip()
    start_rng = (os.environ.get("SE_A_LIST_DATE_START") or "").strip()
    end_rng = (os.environ.get("SE_A_LIST_DATE_END") or "").strip()
    today = datetime.now().strftime("%Y-%m-%d")
    if start_rng and not end_rng:
        end_rng = today
    if end_rng and not start_rng:
        start_rng = end_rng

    batch_info = (
        _should_run("a_list_info", only_set)
        and not _should_run("a_list_daily", only_set)
        and os.environ.get("SE_A_LIST_FROM_DB", "0").strip() == "1"
        and bool(start_rng and end_rng)
    )

    daily_df = pd.DataFrame()
    if batch_info:
        if explicit:
            logger.info(
                "区间批量明细已启用（%s ~ %s），将忽略环境变量 SE_A_LIST_DATE=%s。",
                start_rng,
                end_rng,
                explicit,
            )
        try:
            step_a_list_info_batch_from_db(engine, sentiment, start_rng, end_rng)
        except Exception as e:  # pylint: disable=broad-except
            logger.exception("步骤「龙虎榜明细（批量）」失败：%s", e)
            failed_steps += 1
    else:
        if _should_run("a_list_daily", only_set):
            try:
                daily_df = step_a_list_daily(engine, sentiment)
            except Exception as e:  # pylint: disable=broad-except
                logger.exception("步骤「龙虎榜日列表」失败：%s", e)
                failed_steps += 1
        elif _should_run("a_list_info", only_set):
            if os.environ.get("SE_A_LIST_FROM_DB", "0").strip() == "1":
                try:
                    daily_df = read_frame(text("SELECT * FROM st_a_list_daily"), engine)
                    if daily_df is None or daily_df.empty:
                        logger.warning(
                            "st_a_list_daily 无数据，无法拉 st_a_list_info。"
                            "请先同步日表，或去掉 SE_A_LIST_FROM_DB 以便在拉明细前自动同步日表。"
                        )
                except Exception as e:  # pylint: disable=broad-except
                    logger.exception("从库读取 st_a_list_daily 失败：%s", e)
                    daily_df = pd.DataFrame()
                    failed_steps += 1
            else:
                try:
                    daily_df = step_a_list_daily(engine, sentiment)
                except Exception as e:  # pylint: disable=broad-except
                    logger.exception("步骤「龙虎榜日列表」失败：%s", e)
                    failed_steps += 1

        if _should_run("a_list_info", only_set):
            try:
                step_a_list_info(engine, sentiment, daily_df)
            except Exception as e:  # pylint: disable=broad-except
                logger.exception("步骤「龙虎榜明细」失败：%s", e)
                failed_steps += 1

    if _should_run("mine", only_set):
        try:
            step_mine(engine, sentiment)
        except Exception as e:  # pylint: disable=broad-except
            logger.exception("步骤「扫雷」失败：%s", e)
            failed_steps += 1

    logger.info("舆情同步流程结束。")
    return 1 if failed_steps else 0


if __name__ == "__main__":
    raise SystemExit(main())
