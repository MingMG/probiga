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
  MYSQL_URL                 默认 mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4
  SM_SKIP_DDL               1=跳过 DDL
  SM_SKIP_GLOBAL_TRUNCATE   1=跳过全量 TRUNCATE（单步内仍会清理目标表）
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
  SM_STOCK_KLINE_AKSHARE_TRUNCATE  1=akshare 模式启动时仍 TRUNCATE 全表；0（默认）=按股票+日期区间 DELETE 后追加
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
import logging
import os
import random
import re
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

import numpy as np
import pandas as pd
import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
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
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

DDL_PATH = Path(__file__).resolve().parent / "sql" / "02_sm_stock_market_tables.sql"
DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

TABLES_TRUNCATE_ORDER = [
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


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


def _log_mysql_target(url_str: str) -> None:
    """启动时打印实际写入的库名，避免与 a_share_daily_import 等脚本默认的 biga 混淆。"""
    try:
        u = make_url(url_str)
        logger.info(
            "MySQL 目标：database=%s host=%s（K 线落在本库的 `sm_stock_kline`；"
            "若客户端连的是别的库会以为「全是空的」）",
            u.database,
            u.host,
        )
    except Exception as e:  # pylint: disable=broad-except
        logger.info("MySQL URL 已使用（解析库名失败）：%s", e)


def _sleep() -> None:
    time.sleep(float(os.environ.get("SM_REQUEST_SLEEP", "0.2")))

def _max_workers() -> int:
    return max(1, int(os.environ.get("SM_MAX_WORKERS", "4")))


def _concurrent_run(
    codes: list[str],
    fn: Callable[[str], pd.DataFrame | None],
    *,
    label: str = "",
    log_every: int = 200,
) -> list[pd.DataFrame]:
    """线程池并发执行 per-code API 调用，收集非空 DataFrame。"""
    if not codes:
        return []
    workers = _max_workers()
    parts: list[pd.DataFrame] = []
    done_count = 0
    total = len(codes)
    lock = threading.Lock()

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
            if done_count % log_every == 0 or done_count == total:
                logger.info("%s：进度 %d/%d（并发 %d）", label, done_count, total, workers)
    return parts


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
    """旧库补列：sm_stock_kline.short_name；新建表由 DDL 已含该列。"""
    try:
        with engine.begin() as conn:
            if _sm_column_exists(conn, "sm_stock_kline", "short_name"):
                return
            conn.execute(
                text(
                    "ALTER TABLE `sm_stock_kline` ADD COLUMN `short_name` VARCHAR(128) NOT NULL DEFAULT '' "
                    "COMMENT '股票简称（来自 si_all_code）' AFTER `stock_code`"
                )
            )
        logger.info("sm_stock_kline：已为旧表增加列 short_name。")
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE `sm_stock_kline` k
                        INNER JOIN `si_all_code` s ON k.stock_code = s.stock_code
                        SET k.short_name = s.short_name
                        """
                    )
                )
            logger.info("sm_stock_kline：已从 si_all_code 回填 short_name。")
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("sm_stock_kline short_name 回填失败（请先同步 si_all_code）：%s", e)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning("sm_stock_kline 补列 short_name 失败：%s", e)


def run_ddl(engine: Engine) -> None:
    if os.environ.get("SM_SKIP_DDL") == "1":
        logger.info("已设置 SM_SKIP_DDL=1，跳过 DDL。")
        return
    sql = DDL_PATH.read_text(encoding="utf-8")
    lines = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    parts = [p.strip() for p in re.split(r";\s*\n", cleaned) if p.strip()]
    with engine.begin() as conn:
        for stmt in parts:
            conn.execute(text(stmt))
    logger.info("DDL 执行完成：%s", DDL_PATH)


def truncate_all(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for t in TABLES_TRUNCATE_ORDER:
            conn.execute(text(f"TRUNCATE TABLE `{t}`"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    logger.info("已 TRUNCATE 共 %s 张 STOCK-MARKET 表。", len(TABLES_TRUNCATE_ORDER))


def truncate_only(engine: Engine, *tables: str) -> None:
    if not tables:
        return
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for t in tables:
            conn.execute(text(f"TRUNCATE TABLE `{t}`"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    logger.info("已 TRUNCATE 表：%s", ", ".join(tables))


def df_to_table(engine: Engine, df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        logger.info("表 %s：无数据，跳过写入。", table)
        return
    _clean_df(df).to_sql(table, engine, if_exists="append", index=False, chunksize=1000, method="multi")
    logger.info("表 %s：写入 %s 行。", table, len(df))


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


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def step_dividend(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_dividend import StockDividend

    truncate_only(engine, "sm_dividend")
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

    parts = _concurrent_run(stock_codes, _fetch_dividend, label="分红", log_every=500)
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_dividend")


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

    if incremental:
        with engine.begin() as conn:
            deleted = conn.execute(
                text("DELETE FROM `sm_stock_kline` WHERE `trade_date` >= :s"),
                {"s": start},
            ).rowcount
        logger.info("增量模式：已删除 %d 条 trade_date >= %s 的旧数据", deleted, start)
    else:
        truncate_only(engine, "sm_stock_kline")
    mk = StockMarket()
    logger.info("个股K线(adata)：%d 只，区间 %s ~ %s，k_type=%s，adjust_type=%s", len(stock_codes), start, end or "至今", k_type, adjust_type)
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
    batch_size = max(500, workers * 100)
    batch: list[pd.DataFrame] = []
    done_count = 0
    total = len(stock_codes)

    def _flush() -> None:
        nonlocal batch
        if batch:
            df_to_table(engine, _with_etl(pd.concat(batch, ignore_index=True)), "sm_stock_kline")
            logger.info("K线 分批写入 %d 只，累计 %d/%d", len(batch), done_count, total)
            batch = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_kline, c): c for c in stock_codes}
        for future in as_completed(futures):
            done_count += 1
            try:
                df = future.result()
                if df is not None:
                    batch.append(df)
            except Exception as e:
                logger.warning("stock_kline %s 失败：%s", futures[future], e)
            if done_count % batch_size == 0:
                _flush()
            elif done_count % 500 == 0:
                logger.info("K线 进度 %d/%d（并发 %d）", done_count, total, workers)
    _flush()


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
        delete_kline_range,
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
        truncate_only(engine, "sm_stock_kline")
    pf = (progress_file or "").strip()
    use_flush = bool(pf) and not skip_progress
    logger.info(
        "个股K线(akshare)：%d 只，区间 %s ~ %s（API %s~%s），复权=%s，全表TRUNCATE=%s，"
        "断点进度文件(--progress-file 且未 --skip-progress)=%s",
        len(stock_codes),
        start_sql,
        end_sql,
        start_api,
        end_api,
        adjust or "不复权",
        do_truncate,
        use_flush,
    )
    if not use_flush:
        logger.info(
            "未启用断点逐只写入时，仍按「每只股票拉取后立即写入表 sm_stock_kline」，"
            "便于在库里随时看到进度（不再攒全市场后一次性写入）。"
        )

    written_flush = 0
    written_stocks = 0
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
            if not do_truncate:
                delete_kline_range(engine, code, 1, adjust_type, start_sql, end_sql)

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
                df_to_table(engine, etl, "sm_stock_kline")
                if use_flush:
                    _kline_append_progress(pf, code)
                    written_flush += 1
                written_stocks += 1
        except Exception as e:  # pylint: disable=broad-except
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
        logger.warning("个股K线：si_all_code 未读到代码，跳过 truncate 与写入。请先执行 sync_stock_info 写入 si_all_code。")
        return

    src = (kline_source or os.environ.get("SM_STOCK_KLINE_SOURCE", "adata")).strip().lower()
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


def step_stock_minute(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_market.stock_market import StockMarket

    truncate_only(engine, "sm_stock_minute")
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

    parts = _concurrent_run(stock_codes, _fetch_minute, label="个股分钟", log_every=500)
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_stock_minute")


def step_stock_current(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_market.stock_market import StockMarket

    truncate_only(engine, "sm_stock_current")
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
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_stock_current")


def step_stock_five(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_market.stock_market import StockMarket

    truncate_only(engine, "sm_stock_five_level")
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

    parts = _concurrent_run(stock_codes, _fetch_five, label="五档盘口", log_every=500)
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_stock_five_level")


def step_stock_bar(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.stock_market.stock_market import StockMarket

    truncate_only(engine, "sm_stock_bar")
    mk = StockMarket()
    now = _now()

    def _fetch_bar(code: str) -> pd.DataFrame | None:
        df = retry_remote(mk.get_market_bar, stock_code=code)
        if df is not None and not df.empty:
            df = _to_numeric(df, ["price", "volume"])
            df["snapshot_at"] = now
            return df[["stock_code", "trade_time", "price", "volume", "bs_type", "snapshot_at"]]
        return None

    parts = _concurrent_run(stock_codes, _fetch_bar, label="逐笔成交", log_every=500)
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_stock_bar")


def step_stock_flow_min(engine: Engine, stock_codes: list[str]) -> None:
    from adata.stock.market.capital_flow.stock_capital_flow import StockCapitalFlow

    truncate_only(engine, "sm_stock_capital_flow_min")
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

    parts = _concurrent_run(stock_codes, _fetch_flow_min, label="分钟资金", log_every=500)
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_stock_capital_flow_min")


def step_stock_flow_daily(engine: Engine, stock_codes: list[str], flow_date: str = "") -> None:
    from adata.stock.market.capital_flow.stock_capital_flow import StockCapitalFlow

    if flow_date:
        print(f"日度资金流向：指定日期 {flow_date}，增量模式（删除该日旧数据后写入）")
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM `sm_stock_capital_flow_daily` WHERE `trade_date` = :d"),
                {"d": flow_date[:10]}
            )
    else:
        truncate_only(engine, "sm_stock_capital_flow_daily")
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

    parts = _concurrent_run(stock_codes, _fetch_flow_daily, label="日度资金", log_every=500)
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_stock_capital_flow_daily")


def _concept_ths_instance():
    from adata.stock.market.concepth_market.concept_market_ths import ConceptMarketThs

    return ConceptMarketThs()


def _concept_east_instance():
    from adata.stock.market.concepth_market.concept_market_east import ConceptMarketEase

    return ConceptMarketEase()


def step_concept_ths_kline(
    engine: Engine, concept_codes: list[str], kline_start: str | None = None, kline_end: str | None = None
) -> None:
    truncate_only(engine, "sm_concept_ths_kline")
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

    parts = _concurrent_run(concept_codes, _fetch, label="THS概念K线")
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_concept_ths_kline")


def step_concept_ths_minute(engine: Engine, concept_codes: list[str]) -> None:
    truncate_only(engine, "sm_concept_ths_minute")
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

    parts = _concurrent_run(concept_codes, _fetch, label="THS概念分钟")
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_concept_ths_minute")


def step_concept_ths_current(engine: Engine, concept_codes: list[str]) -> None:
    truncate_only(engine, "sm_concept_ths_current")
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
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_concept_ths_current")


def step_concept_east_kline(
    engine: Engine, concept_codes: list[str], kline_start: str | None = None, kline_end: str | None = None
) -> None:
    truncate_only(engine, "sm_concept_east_kline")
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

    parts = _concurrent_run(concept_codes, _fetch, label="东财概念K线")
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_concept_east_kline")


def step_concept_east_minute(engine: Engine, concept_codes: list[str]) -> None:
    truncate_only(engine, "sm_concept_east_minute")
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

    parts = _concurrent_run(concept_codes, _fetch, label="东财概念分钟")
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_concept_east_minute")


def step_concept_east_current(engine: Engine, concept_codes: list[str]) -> None:
    truncate_only(engine, "sm_concept_east_current")
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
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_concept_east_current")


def step_concept_flow_east(engine: Engine) -> None:
    from adata.stock.market.concept_capital_flow.capital_flow_east import CapitalFlowEast

    truncate_only(engine, "sm_concept_capital_flow_east")
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
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_concept_capital_flow_east")


def step_index_kline(engine: Engine, index_codes: list[str], kline_start: str | None = None, kline_end: str | None = None) -> None:
    from adata.stock.market.index_market.market_index import StockMarketIndex

    truncate_only(engine, "sm_index_kline")
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

    parts = _concurrent_run(index_codes, _fetch, label="指数K线")
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_index_kline")


def step_index_minute(engine: Engine, index_codes: list[str]) -> None:
    from adata.stock.market.index_market.market_index import StockMarketIndex

    truncate_only(engine, "sm_index_minute")
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

    parts = _concurrent_run(index_codes, _fetch, label="指数分钟")
    if parts:
        df_to_table(engine, _with_etl(pd.concat(parts, ignore_index=True)), "sm_index_minute")


def _sina_index_symbol(index_code: str) -> str | None:
    code = str(index_code or "").strip().lower()
    if not code:
        return None
    if code.startswith(("sh", "sz")) and len(code) >= 8:
        return code
    digits = re.sub(r"\D", "", code)
    if len(digits) != 6:
        return None
    if digits.startswith(("399", "395")):
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


def step_index_current(engine: Engine, index_codes: list[str]) -> None:
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
    truncate_only(engine, "sm_index_current")
    df_to_table(engine, _with_etl(df), "sm_index_current")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="STOCK-MARKET 同步")
    parser.add_argument("--only", type=str, default="", help=f"只执行步骤，逗号分隔。可选：{','.join(sorted(STEP_NAMES))}")
    parser.add_argument("--kline-start", type=str, default="", help="K线起始日期（YYYY-MM-DD），覆盖 SM_MARKET_START/SM_INDEX_START")
    parser.add_argument("--kline-end", type=str, default="", help="K线结束日期（YYYY-MM-DD），覆盖 SM_MARKET_END/SM_INDEX_END")
    parser.add_argument("--kline-today", action="store_true", help="K线仅同步当天（收盘后常用）")
    parser.add_argument("--kline-incremental", action="store_true", help="K线增量模式：不TRUNCATE全表，只删除start之后的数据后追加")
    parser.add_argument(
        "--kline-source",
        type=str,
        choices=["adata", "akshare"],
        default=os.environ.get("SM_STOCK_KLINE_SOURCE", "adata").strip().lower() or "adata",
        help="个股 K 线数据源：adata（东财/百度）或 akshare（东财日 K API，支持区间/断点；不依赖 akshare 包）",
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
    parser.add_argument("--flow-date", type=str, default="", help="资金流向指定日期（YYYY-MM-DD），仅拉取该日数据，不 truncate 全表")
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

    if args.max_stocks is not None:
        os.environ["SM_MAX_STOCKS"] = str(args.max_stocks)
        logger.info("已设置 SM_MAX_STOCKS=%s（--max-stocks）", args.max_stocks)

    slice_limit = args.limit
    slice_offset = max(0, args.offset)
    if slice_offset > 0 and slice_limit == -1:
        slice_limit = 0
        logger.info("已指定 --offset 但未指定 --limit，已按 --limit 0（从 offset 到 si_all_code 末尾）处理。")

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    _log_mysql_target(_mysql_url())
    run_ddl(engine)
    _ensure_sm_stock_kline_short_name(engine)
    if not only_set and os.environ.get("SM_SKIP_GLOBAL_TRUNCATE") != "1":
        truncate_all(engine)

    stock_codes = read_stock_codes(engine, slice_offset=slice_offset, slice_limit=slice_limit)
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

    index_codes = read_index_codes(engine)
    concept_ths_codes = read_concept_ths_codes(engine)
    concept_east_codes = read_concept_east_codes(engine)

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
