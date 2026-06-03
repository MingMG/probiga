# -*- coding: utf-8 -*-
"""
将 adata.stock.info（STOCK-INFO 文档）下可批量同步的接口写入 MySQL 表 ``probiga``。

前置：
  pip install -e ./adata
  pip install -r requirements-platform.txt

执行（在仓库根目录 ``ProBigA``）::

  python -m biz.stock_info.sync_stock_info

环境变量（可选）：
  MYSQL_URL  默认 ``mysql+pymysql://root:123456@127.0.0.1:3306/probiga?charset=utf8mb4``
  SI_REQUEST_SLEEP  每次远程请求后的休眠秒数，默认 ``0.2``
  SI_YEAR_START / SI_YEAR_END  交易日历年份范围，默认 ``2010`` 至 ``当年+1``
  SI_MAX_STOCKS  仅调试：大于 0 时只处理前 N 只股票（股本/概念等循环）
  SI_SKIP_DDL  设为 ``1`` 则跳过执行建表 SQL（表已建好时）
  SI_INCLUDE_THS_NAME  设为 ``1`` 时对同花顺按「概念名称」拉成分（请求多、易被风控），默认 ``0``

说明：
  - ``stock.info.get_dynamic_core_index`` 在 adata 内为 TODO，无数据，不落库。
  - 全市场股本/成分/概念等为高并发 HTTP，首次全量可能极耗时；请按需调 ``SI_REQUEST_SLEEP``。
  - 若出现 ``RemoteDisconnected`` / ``Connection aborted``：多为数据源临时断连或限流，已内置重试；仍失败时可加大 ``SI_REQUEST_SLEEP``、``SI_HTTP_BACKOFF`` 后重跑（表已 TRUNCATE 需整段重跑，或先 ``SI_SKIP_DDL=1`` 仅补数）。
  - 指数列表专用：``SI_COOLDOWN_BEFORE_INDEX``（拉指数前休眠秒数）、``SI_INDEX_FALLBACK=1``（adata 失败后用浏览器头多域名重试）、``SI_CONTINUE_WITHOUT_INDEX=1``（指数仍失败则跳过 ``si_*index*``，继续后面概念/个股同步）。
  - ``SI_INDEX_EAST_BASES``  备用/轮换的东财 push2 根 URL，逗号分隔；分页请求会逐个镜像重试（默认含 33/63/81/90 等多线路）。
  - ``SI_INDEX_FALLBACK_PAGE_SLEEP``  备用线路分页间隔秒数，默认 ``0.35``；仍断连时可加大到 ``0.8~1.5``。
  - ``SI_INDEX_SINA_FALLBACK``  东财全失败时是否改用新浪财经 ``Market_Center.getHQNodeData``（默认 ``1``）；设 ``0`` 关闭。
  - ``SI_INDEX_SINA_NODE``  新浪行情中心 node，默认 ``hs_s``（沪深指数）；``SI_INDEX_SINA_PAGE_SLEEP`` 分页间隔秒，默认 ``0.25``。
  - ``SI_INDEX_PRIMARY``  设为 ``sina`` 时**跳过东财**，直接只拉新浪（适合 push2 长期不可用环境）。
  - ``SI_INDEX_CONSTITUENT_PRIMARY``  指数成分数据源顺序：``auto``（默认，先百度后新浪）、``baidu``、``sina``。百度常返回空体/非 JSON 时自动改试新浪。
  - **只补其它表、保留已有 ``si_all_code``**：``SI_SKIP_GLOBAL_TRUNCATE=1``（不开场清空 14 张表）+ ``SI_SYNC_SKIP_ALL_CODE=1``（不再调 ``all_code()``，从库读 ``si_all_code`` 跑后续）；各步骤写入前会单独 ``TRUNCATE`` 本步目标表，避免重复行。
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
from sqlalchemy import create_engine, text
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
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_si_stock_info_tables.sql"

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

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


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


def _sleep() -> None:
    time.sleep(float(os.environ.get("SI_REQUEST_SLEEP", "0.2")))


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


def run_ddl(engine: Engine) -> None:
    if os.environ.get("SI_SKIP_DDL") == "1":
        logger.info("已设置 SI_SKIP_DDL=1，跳过 DDL。")
        return
    sql = DDL_PATH.read_text(encoding="utf-8")
    # 去掉整行注释，避免误执行
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


def truncate_all(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for t in TABLES_TRUNCATE_ORDER:
            conn.execute(text(f"TRUNCATE TABLE `{t}`"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    logger.info("已 TRUNCATE 共 %s 张表。", len(TABLES_TRUNCATE_ORDER))


def truncate_only(engine: Engine, *table_names: str) -> None:
    """仅清空指定表（写入前调用，支持「保留 si_all_code、重灌其它表」）。"""
    if not table_names:
        return
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for t in table_names:
            conn.execute(text(f"TRUNCATE TABLE `{t}`"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    logger.info("已 TRUNCATE 表：%s", ", ".join(table_names))


def df_to_table(engine: Engine, df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        logger.info("表 %s：无数据，跳过写入。", table)
        return
    df = _clean_object_df(df)
    # 分批写入，避免MySQL参数超限（method="multi"遇到宽表易超参）
    total = len(df)
    written = 0
    for start in range(0, total, 500):
        chunk = df.iloc[start:start + 500]
        chunk.to_sql(table, engine, if_exists="append", index=False, chunksize=100)
        written += len(chunk)
    logger.info("表 %s：写入 %s/%s 行。", table, written, total)


def load_info():
    # 仅加载 stock.info，避免 import adata 时提前加载 fund 等（见 adata/__init__.py 按需加载）
    from adata.stock.info import info

    return info


def sync_all_code(engine: Engine, info) -> pd.DataFrame:
    truncate_only(engine, "si_all_code")
    ts = _now()
    df = retry_remote(info.all_code)
    df = _clean_object_df(df)
    df["etl_sync_at"] = ts
    df_to_table(engine, df, "si_all_code")
    _sleep()
    return df


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
    for i in range(2):
        fs = "m:1+s:2" if i == 0 else "m:0+t:5"
        curr_page = 1
        while curr_page < 88:
            res_json = fetch_page_json(fs, curr_page)
            if not res_json:
                break
            block = res_json.get("data")
            if not block:
                break
            diff = block.get("diff") or []
            if not diff:
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
            if len(diff) < 20:
                break
            curr_page += 1
            time.sleep(page_sleep)
        time.sleep(page_sleep)

    if data:
        logger.info("指数列表备用线路成功，原始行数=%s", len(data))
        out = pd.DataFrame(data)
        return out.dropna(subset=["index_code"]).drop_duplicates(subset=["index_code"], keep="first")
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
                break
            if not block:
                break
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
                break
            time.sleep(page_sleep)

    if not rows:
        raise RuntimeError("新浪财经指数列表返回空（请检查网络或 SI_INDEX_SINA_NODE）。")
    out = pd.DataFrame(rows)
    return out.dropna(subset=["index_code"]).drop_duplicates(subset=["index_code"], keep="first")


def sync_trade_calendar(engine: Engine, info) -> None:
    truncate_only(engine, "si_trade_calendar")
    ts = _now()
    y0 = int(os.environ.get("SI_YEAR_START", "2010"))
    y1 = int(os.environ.get("SI_YEAR_END", str(datetime.now().year + 1)))
    parts: list[pd.DataFrame] = []
    for y in range(y0, y1 + 1):
        df = retry_remote(info.trade_calendar, year=y)
        if df is None or df.empty:
            _sleep()
            continue
        df = df.copy()
        df["calendar_year"] = _decimal_year(y)
        df["trade_status"] = pd.to_numeric(df["trade_status"], errors="coerce")
        df["day_week"] = pd.to_numeric(df["day_week"], errors="coerce")
        df = _coerce_decimals(df, ["trade_status", "day_week", "calendar_year"])
        df["etl_sync_at"] = ts
        parts.append(df)
        logger.info("交易日历 %s 年：%s 行", y, len(df))
        _sleep()
    if parts:
        df_to_table(engine, pd.concat(parts, ignore_index=True), "si_trade_calendar")


def sync_all_index_code(engine: Engine, info) -> pd.DataFrame:
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
        if os.environ.get("SI_CONTINUE_WITHOUT_INDEX", "1") == "1":
            logger.warning(
                "已跳过指数代码表 si_all_index_code / 成分 si_index_constituent（无数据）。"
                "需要指数时可换网络/代理后重跑，或设 SI_CONTINUE_WITHOUT_INDEX=0 强制失败退出。"
            )
            return pd.DataFrame()
        raise RuntimeError(
            "东财指数列表不可用（adata 与备用线路均失败）。"
            "可换网络/代理、加大 SI_COOLDOWN_BEFORE_INDEX 后重试；或设 SI_CONTINUE_WITHOUT_INDEX=1 跳过指数两步。"
        )

    df = _clean_object_df(df)
    if "index_code" in df.columns:
        df = df.dropna(subset=["index_code"]).drop_duplicates(subset=["index_code"], keep="first")
    df["etl_sync_at"] = ts
    truncate_only(engine, "si_all_index_code")
    df_to_table(engine, df, "si_all_index_code")
    _sleep()
    return df


def sync_index_constituent(engine: Engine, info, df_index: pd.DataFrame) -> None:
    ts = _now()
    if df_index is None or df_index.empty:
        return
    truncate_only(engine, "si_index_constituent")
    codes = (
        df_index["index_code"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", np.nan)
        .dropna()
        .unique()
    )
    parts: list[pd.DataFrame] = []
    for i, ic in enumerate(codes):
        try:
            df = retry_remote(info.index_constituent, index_code=str(ic))
        except Exception as e:  # noqa: BLE001 — 单指数失败不中断整表
            logger.warning("指数 %s 成分拉取异常，已跳过：%s", ic, e)
            _sleep()
            continue
        if df is None or df.empty:
            _sleep()
            continue
        df = df.copy()
        df["etl_sync_at"] = ts
        parts.append(df)
        if (i + 1) % 20 == 0:
            logger.info("指数成分进度：%s/%s", i + 1, len(codes))
        _sleep()
    if parts:
        df_to_table(engine, pd.concat(parts, ignore_index=True), "si_index_constituent")


def sync_concept_code_east(engine: Engine, info) -> pd.DataFrame:
    truncate_only(engine, "si_concept_code_east")
    ts = _now()
    df = retry_remote(info.all_concept_code_east)
    df = _clean_object_df(df)
    if "concept_code" in df.columns:
        df = df.dropna(subset=["concept_code"]).drop_duplicates(subset=["concept_code"], keep="first")
    df["etl_sync_at"] = ts
    df_to_table(engine, df, "si_concept_code_east")
    _sleep()
    return df


def sync_concept_constituent_east(engine: Engine, info, df_codes: pd.DataFrame) -> None:
    truncate_only(engine, "si_concept_constituent_east")
    ts = _now()
    if df_codes is None or df_codes.empty:
        return
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
    for i, cc in enumerate(cset):
        df = retry_remote(info.concept_constituent_east, concept_code=str(cc))
        if df is None or df.empty:
            _sleep()
            continue
        df = df.copy()
        df["concept_code"] = str(cc)
        df["etl_sync_at"] = ts
        parts.append(df)
        if (i + 1) % 50 == 0:
            logger.info("东财概念成分进度：%s/%s", i + 1, len(cset))
        _sleep()
    if parts:
        df_to_table(engine, pd.concat(parts, ignore_index=True), "si_concept_constituent_east")


def sync_concept_code_ths(engine: Engine, info) -> pd.DataFrame:
    truncate_only(engine, "si_concept_code_ths")
    ts = _now()
    df = retry_remote(info.all_concept_code_ths)
    df = _clean_object_df(df)
    df["etl_sync_at"] = ts
    df_to_table(engine, df, "si_concept_code_ths")
    _sleep()
    return df


def sync_concept_constituent_ths(engine: Engine, info, df_ths: pd.DataFrame) -> None:
    truncate_only(engine, "si_concept_constituent_ths")
    ts = _now()
    if df_ths is None or df_ths.empty:
        return
    parts: list[pd.DataFrame] = []

    def append_result(query_type: str, query_key: str, res):
        if _is_bad_ths_result(res) or res.empty:
            return
        d = res.copy()
        d["query_type"] = query_type
        d["query_key"] = query_key
        d["etl_sync_at"] = ts
        parts.append(d)

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

    if parts:
        out = pd.concat(parts, ignore_index=True)
        df_to_table(engine, out, "si_concept_constituent_ths")


def _stock_limit(codes: list[str]) -> list[str]:
    lim = int(os.environ.get("SI_MAX_STOCKS", "0"))
    if lim > 0:
        return codes[:lim]
    return codes


def sync_per_stock_tables(engine: Engine, info, df_codes: pd.DataFrame) -> None:
    if df_codes is None or df_codes.empty:
        return
    truncate_only(
        engine,
        "si_stock_shares",
        "si_industry_sw",
        "si_stock_concept_east",
        "si_stock_plate_east",
        "si_stock_concept_baidu",
        "si_stock_concept_ths",
    )
    ts = _now()
    codes = df_codes["stock_code"].astype(str).str.zfill(6).tolist()
    codes = _stock_limit(codes)
    logger.info("个股维度同步，股票数：%s", len(codes))

    # 股本
    share_parts: list[pd.DataFrame] = []
    for i, code in enumerate(codes):
        try:
            df = retry_remote(info.get_stock_shares, stock_code=code, is_history=True)
        except Exception as e:
            logger.warning("股本 %s 失败：%s", code, e)
            df = pd.DataFrame()
        if df is not None and not df.empty:
            df = df.copy()
            df["change_date"] = pd.to_datetime(df["change_date"], errors="coerce").dt.date
            df = _coerce_decimals(df, ["total_shares", "limit_shares", "list_a_shares"])
            df["etl_sync_at"] = ts
            share_parts.append(df)
        if (i + 1) % 100 == 0:
            logger.info("股本进度：%s/%s", i + 1, len(codes))
        _sleep()
    if share_parts:
        df_to_table(engine, pd.concat(share_parts, ignore_index=True), "si_stock_shares")

    # 申万行业（批量）
    batch = 40
    ind_parts: list[pd.DataFrame] = []
    for i in range(0, len(codes), batch):
        chunk = codes[i : i + batch]
        try:
            df = retry_remote(info.get_industry_sw, stock_code=chunk)
        except Exception as e:
            logger.warning("申万行业 batch %s 失败：%s", chunk[:3], e)
            df = pd.DataFrame()
        if df is not None and not df.empty:
            df = df.copy()
            df["etl_sync_at"] = ts
            ind_parts.append(df)
        _sleep()
    if ind_parts:
        df_to_table(engine, pd.concat(ind_parts, ignore_index=True), "si_industry_sw")

    # 东财概念 / 板块
    sce, spe = [], []
    for i, code in enumerate(codes):
        try:
            d1 = retry_remote(info.get_concept_east, stock_code=code)
        except Exception as e:
            logger.warning("东财概念 %s：%s", code, e)
            d1 = pd.DataFrame()
        if d1 is not None and not d1.empty:
            d1 = d1.copy()
            d1["etl_sync_at"] = ts
            sce.append(d1)
        try:
            d2 = retry_remote(info.get_plate_east, stock_code=code, plate_type=None)
        except Exception as e:
            logger.warning("东财板块 %s：%s", code, e)
            d2 = pd.DataFrame()
        if d2 is not None and not d2.empty:
            d2 = d2.copy()
            d2["etl_sync_at"] = ts
            spe.append(d2)
        if (i + 1) % 200 == 0:
            logger.info("东财概念/板块进度：%s/%s", i + 1, len(codes))
        _sleep()
    if sce:
        df_to_table(engine, pd.concat(sce, ignore_index=True), "si_stock_concept_east")
    if spe:
        df_to_table(engine, pd.concat(spe, ignore_index=True), "si_stock_plate_east")

    # 百度概念（批量）
    scb_parts: list[pd.DataFrame] = []
    for i in range(0, len(codes), batch):
        chunk = codes[i : i + batch]
        try:
            df = retry_remote(info.get_concept_baidu, stock_code=chunk)
        except Exception as e:
            logger.warning("百度概念 batch：%s", e)
            df = pd.DataFrame()
        if df is not None and not df.empty:
            df = df.copy()
            df["etl_sync_at"] = ts
            scb_parts.append(df)
        _sleep()
    if scb_parts:
        df_to_table(engine, pd.concat(scb_parts, ignore_index=True), "si_stock_concept_baidu")

    # 同花顺个股概念（单票，易风控）
    sct_parts: list[pd.DataFrame] = []
    for i, code in enumerate(codes):
        try:
            df = retry_remote(info.get_concept_ths, stock_code=code)
        except Exception as e:
            logger.warning("同花顺个股概念 %s：%s", code, e)
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
    if sct_parts:
        df_to_table(engine, pd.concat(sct_parts, ignore_index=True), "si_stock_concept_ths")


def _step(label: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logger.exception("步骤 [%s] 失败，已跳过继续后续：%s", label, e)
        return None


def main() -> None:
    url = _mysql_url()
    logger.info("连接：%s", re.sub(r":([^:@]+)@", r":***@", url))
    engine = create_engine(url, pool_pre_ping=True, future=True)
    run_ddl(engine)
    if os.environ.get("SI_SKIP_GLOBAL_TRUNCATE") == "1":
        logger.info(
            "SI_SKIP_GLOBAL_TRUNCATE=1：跳过开场 TRUNCATE 全部 14 张表。"
            "若同时 SI_SYNC_SKIP_ALL_CODE=1，将保留现有 si_all_code，仅重灌其它表。"
        )
    else:
        truncate_all(engine)
    info = load_info()

    if os.environ.get("SI_SYNC_SKIP_ALL_CODE") == "1":
        df_code = pd.read_sql_query("SELECT stock_code FROM si_all_code ORDER BY stock_code", engine)
        if df_code.empty:
            logger.error("SI_SYNC_SKIP_ALL_CODE=1 但 si_all_code 为空，无法继续。")
            sys.exit(1)
        logger.info("SI_SYNC_SKIP_ALL_CODE=1：从库读取 si_all_code，共 %s 只股票。", len(df_code))
    else:
        df_code = sync_all_code(engine, info)

    _step("交易日历", sync_trade_calendar, engine, info)
    df_idx = _step("指数列表", sync_all_index_code, engine, info)
    if not isinstance(df_idx, pd.DataFrame):
        df_idx = pd.DataFrame()
    _step("指数成分", sync_index_constituent, engine, info, df_idx)
    df_east = _step("东财概念列表", sync_concept_code_east, engine, info)
    if not isinstance(df_east, pd.DataFrame):
        df_east = pd.DataFrame()
    _step("东财概念成分", sync_concept_constituent_east, engine, info, df_east)
    df_ths = _step("同花顺概念列表", sync_concept_code_ths, engine, info)
    if not isinstance(df_ths, pd.DataFrame):
        df_ths = pd.DataFrame()
    _step("同花顺概念成分", sync_concept_constituent_ths, engine, info, df_ths)
    _step("个股维度表", sync_per_stock_tables, engine, info, df_code)

    logger.info("STOCK-INFO 同步流程结束（若某步失败见上方 ERROR 日志，可单独重跑或调环境变量）。")


if __name__ == "__main__":
    main()
