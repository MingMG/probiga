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
  SE_SKIP_GLOBAL_TRUNCATE  ``1`` 不在开场清空全部 ``st_*``；各步骤写入前仍会 ``TRUNCATE`` 本步目标表
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
  批量明细（与日表 ``trade_date`` 一一对应）：``SE_A_LIST_FROM_DB=1`` + ``--only a_list_info`` + 同时设 ``SE_A_LIST_DATE_START`` / ``SE_A_LIST_DATE_END``（可只设 START，END 默认今天）。将按区间内日表出现的每个 ``trade_date`` 逐日拉明细并合并写入 ``st_a_list_info``（会先 TRUNCATE 明细表）。**此模式下会忽略环境变量 ``SE_A_LIST_DATE``**（避免会话里残留的「今天」导致误走单日逻辑）。若只要库里的单日明细且不设区间，请只设 ``SE_A_LIST_DATE`` 并**不要**同时设 START/END。
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

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_sentiment_tables.sql"

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


def run_ddl(engine: Engine) -> None:
    if os.environ.get("SE_SKIP_DDL") == "1":
        logger.info("已设置 SE_SKIP_DDL=1，跳过 DDL。")
        return
    sql = DDL_PATH.read_text(encoding="utf-8")
    lines = []
    for line in sql.splitlines():
        s = line.strip()
        if s.startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    parts = [p.strip() for p in re.split(r";\s*\n", cleaned) if p.strip()]
    with engine.begin() as conn:
        for stmt in parts:
            conn.execute(text(stmt))
    logger.info("DDL 执行完成：%s", DDL_PATH)


def truncate_all_sentiment(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for t in TABLES_TRUNCATE_ORDER:
            conn.execute(text(f"TRUNCATE TABLE {quote_identifier(t)}"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    logger.info("已 TRUNCATE 共 %s 张舆情表。", len(TABLES_TRUNCATE_ORDER))


def truncate_only(engine: Engine, *table_names: str) -> None:
    if not table_names:
        return
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for t in table_names:
            conn.execute(text(f"TRUNCATE TABLE {quote_identifier(t)}"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    logger.info("已 TRUNCATE 表：%s", ", ".join(table_names))


def df_to_table(engine: Engine, df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        logger.info("表 %s：无数据，跳过写入。", table)
        return
    df = _clean_object_df(df)
    write_frame(df, table, engine, if_exists="append", index=False, chunksize=1000, method="multi")
    logger.info("表 %s：写入 %s 行。", table, len(df))


def _with_etl(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out["etl_sync_at"] = _now()
    return out


def step_lifting(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_stock_lifting_last_month")
    df = retry_remote(sentiment.stock_lifting_last_month)
    if df is None or df.empty:
        df_to_table(engine, df, "st_stock_lifting_last_month")
        return
    for c in ("volume", "amount", "ratio", "price"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df_to_table(engine, _with_etl(df), "st_stock_lifting_last_month")
    _sleep()


def step_margin(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_securities_margin")
    start = os.environ.get("SE_MARGIN_START", "2020-01-01")
    df = retry_remote(sentiment.securities_margin, start_date=start)
    if df is not None and not df.empty and "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
    df_to_table(engine, _with_etl(df), "st_securities_margin")
    _sleep()


def step_north_daily(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_north_flow_daily")
    start = os.environ.get("SE_NORTH_START", "2017-01-01")
    df = retry_remote(sentiment.north.north_flow, start_date=start)
    if df is not None and not df.empty and "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
    df_to_table(engine, _with_etl(df), "st_north_flow_daily")
    _sleep()


def step_north_min(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_north_flow_min")
    raw = retry_remote(sentiment.north.north_flow_min)
    if isinstance(raw, Exception):
        logger.warning("北向分时跳过：%s", raw)
        return
    if not isinstance(raw, pd.DataFrame):
        logger.warning("北向分时返回非 DataFrame，跳过。")
        return
    df = raw
    df_to_table(engine, _with_etl(df), "st_north_flow_min")
    _sleep()


def step_north_current(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_north_flow_current")
    raw = retry_remote(sentiment.north.north_flow_current)
    if isinstance(raw, Exception):
        logger.warning("北向当前跳过：%s", raw)
        return
    if not isinstance(raw, pd.DataFrame):
        logger.warning("北向当前返回非 DataFrame，跳过。")
        return
    df_to_table(engine, _with_etl(raw), "st_north_flow_current")
    _sleep()


def step_hot_east(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_hot_pop_rank_east")
    df = retry_remote(sentiment.hot.pop_rank_100_east)
    if df is not None and "change" in df.columns:
        df = df.rename(columns={"change": "price_change"})
    df_to_table(engine, _with_etl(df), "st_hot_pop_rank_east")
    _sleep()


def step_hot_ths(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_hot_rank_ths")
    df = retry_remote(sentiment.hot.hot_rank_100_ths)
    df_to_table(engine, _with_etl(df), "st_hot_rank_ths")
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


def step_hot_concept(engine: Engine, sentiment: Any) -> None:
    truncate_only(engine, "st_hot_concept_ths_rt")
    parts = []
    for pt in (1, 2):
        if pt == 2:
            _sleep()
        d = retry_remote(sentiment.hot.hot_concept_20_ths, plate_type=pt)
        if d is not None and not d.empty:
            d = d.copy()
            d["plate_type"] = pt
            parts.append(d)
    if not parts:
        return
    df = pd.concat(parts, ignore_index=True)
    rt_df = _with_etl(df)
    df_to_table(engine, rt_df, "st_hot_concept_ths_rt")

    # 日留存：同一天重复执行时先删当天旧数据，再写入当天快照，保证每天仅保留一版。
    daily_df = rt_df.copy()
    daily_df["snapshot_date"] = datetime.now().strftime("%Y-%m-%d")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM `st_hot_concept_ths_daily` WHERE `snapshot_date` = CURDATE()"))
    df_to_table(
        engine,
        daily_df[
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
        ],
        "st_hot_concept_ths_daily",
    )


def step_a_list_daily(engine: Engine, sentiment: Any) -> pd.DataFrame:
    dates = _a_list_report_dates(engine)
    parts: list[pd.DataFrame] = []
    for i, report in enumerate(dates):
        try:
            one = retry_remote(sentiment.hot.list_a_list_daily, report_date=report)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("龙虎榜日列表 %s 请求失败：%s", report, e)
            one = None
        if one is not None and not one.empty:
            parts.append(_coerce_a_list_daily_columns(one))
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
    replace_table_rows(_with_etl(df), "st_a_list_daily", engine)
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
                    all_rows.append(one)
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
    replace_table_rows(_with_etl(df), "st_a_list_info", engine)


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
    for code in codes:
        try:
            _sleep()
            one = retry_remote(sentiment.hot.get_a_list_info, stock_code=code, report_date=report)
            if one is not None and not one.empty:
                rows.append(one)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("龙虎榜明细 %s 失败：%s", code, e)
    if not rows:
        raise RuntimeError("no a-list info rows fetched")
    df = pd.concat(rows, ignore_index=True)
    df = _finalize_a_list_info_df(df)
    replace_table_rows(_with_etl(df), "st_a_list_info", engine)


def step_mine(engine: Engine, sentiment: Any) -> None:
    if os.environ.get("SE_SKIP_MINE") == "1":
        logger.info("已设置 SE_SKIP_MINE=1，跳过扫雷。")
        return
    truncate_only(engine, "st_mine_clearance_tdx")
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
                parts.append(one)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("扫雷 %s 失败：%s", code, e)
    if not parts:
        logger.info("表 st_mine_clearance_tdx：无数据。")
        return
    df = pd.concat(parts, ignore_index=True)
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df_to_table(engine, _with_etl(df), "st_mine_clearance_tdx")


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
    if not only_set and os.environ.get("SE_SKIP_GLOBAL_TRUNCATE") != "1":
        truncate_all_sentiment(engine)

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
