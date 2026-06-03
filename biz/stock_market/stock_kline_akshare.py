# -*- coding: utf-8 -*-
"""
个股日 K 拉取（供 sync_stock_market --kline-source akshare）。

- 默认走**新浪**日 K（``SM_STOCK_KLINE_ENGINE=sina``，需本机 Node 解密，见 ``sina_kline_fetch``）。
- 设置 ``SM_STOCK_KLINE_ENGINE=east``（或 ``em`` / ``eastmoney``）时使用东财 ``push2his``（纯 requests）。

AkShare 的 ``stock_zh_a_daily`` 依赖 ``py_mini_racer``，在 Python 3.14 等环境常无法导入，故不依赖 akshare。
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
import requests

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger("sync_stock_market")

ADJUST_TO_INT = {"": 0, "qfq": 1, "hfq": 2}

_EAST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

_EAST_KLINE_PATH = "/api/qt/stock/kline/get"
# 东财多镜像（AkShare 等库亦有使用）；主域名被 RST 时可轮换
_DEFAULT_EAST_BASES = (
    "https://push2his.eastmoney.com",
    "https://33.push2his.eastmoney.com",
    "https://63.push2his.eastmoney.com",
    "https://81.push2his.eastmoney.com",
    "https://90.push2his.eastmoney.com",
)


def _east_host_bases() -> list[str]:
    raw = os.environ.get("SM_KLINE_EAST_HOSTS", "").strip()
    if raw:
        return [x.strip().rstrip("/") for x in raw.split(",") if x.strip()]
    return list(_DEFAULT_EAST_BASES)


def _east_chunk_days() -> int:
    return max(30, int(os.environ.get("SM_KLINE_EAST_CHUNK_DAYS", "240")))


def _east_chunk_sleep() -> float:
    return float(os.environ.get("SM_KLINE_EAST_CHUNK_SLEEP", "0.12"))


def _iter_yyyymmdd_chunks(beg: str, end: str, max_days: int):
    s = datetime.strptime(beg, "%Y%m%d").date()
    e = datetime.strptime(end, "%Y%m%d").date()
    if s > e:
        return
    cur = s
    while cur <= e:
        chunk_end = min(cur + timedelta(days=max(1, max_days) - 1), e)
        yield cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")
        cur = chunk_end + timedelta(days=1)


def _klines_json_to_df(data_json: dict[str, Any]) -> Optional[pd.DataFrame]:
    if not data_json.get("data") or not data_json["data"].get("klines"):
        return None
    rows = [item.split(",") for item in data_json["data"]["klines"]]
    if not rows:
        return None
    temp_df = pd.DataFrame(rows)
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
    ]
    temp_df = temp_df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
            "换手率": "turnover",
        }
    )
    temp_df["date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.normalize()
    for col in ("open", "high", "low", "close", "volume", "amount", "turnover"):
        temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
    temp_df["volume"] = temp_df["volume"] * 100
    temp_df["outstanding_share"] = None
    return temp_df


def _request_kline_once(
    session: requests.Session,
    host_base: str,
    params: dict[str, str],
    timeout: float,
) -> Optional[pd.DataFrame]:
    url = host_base.rstrip("/") + _EAST_KLINE_PATH
    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return _klines_json_to_df(r.json())


def em_code_to_sina_symbol(code: str) -> Optional[str]:
    """6 位 A 股代码 -> 新浪 symbol（sh600000 / sz000001 / bj43xxxx）。"""
    c = str(code).strip()
    if not c.isdigit():
        return None
    c = c.zfill(6)
    if c.startswith(("43", "83", "87", "88", "82", "92")):
        return f"bj{c}"
    if c.startswith("6") or c.startswith("9"):
        return f"sh{c}"
    if c.startswith(("0", "1", "2", "3")):
        return f"sz{c}"
    return None


def _to_yyyymmdd(d: str) -> str:
    s = str(d).strip().replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError(f"日期须为 YYYY-MM-DD 或 YYYYMMDD，收到: {d!r}")
    return s


def _to_yyyy_mm_dd(d: str) -> str:
    raw = _to_yyyymmdd(d)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _east_secid(stock_code: str) -> str:
    """东财 secid，与 AkShare stock_zh_a_hist / adata 东财 K 线一致。"""
    c = str(stock_code).strip().zfill(6)
    if c.startswith("6") or c.startswith("9"):
        return f"1.{c}"
    return f"0.{c}"


def _kline_engine() -> str:
    raw = (os.environ.get("SM_STOCK_KLINE_ENGINE") or "sina").strip().lower()
    return raw or "sina"


def fetch_stock_daily_kline(
    stock_code: str,
    start_date: str,
    end_date: str,
    adjust: str,
    *,
    timeout: float = 45.0,
) -> Optional[pd.DataFrame]:
    """
    日 K：默认新浪（``SM_STOCK_KLINE_ENGINE`` 未设或 ``sina``），可选东财（``east`` / ``em`` / ``eastmoney``）。
    start_date/end_date 为 YYYYMMDD。
    """
    eng = _kline_engine()
    if eng in ("east", "em", "eastmoney"):
        return _fetch_eastmoney_daily_kline(
            stock_code, start_date, end_date, adjust, timeout=timeout
        )
    from biz.stock_market.sina_kline_fetch import fetch_sina_a_daily_kline

    sym = em_code_to_sina_symbol(stock_code)
    if not sym:
        logger.warning("无法映射为新浪 symbol，跳过 K 线：%s", stock_code)
        return None
    return fetch_sina_a_daily_kline(sym, start_date, end_date, adjust, timeout=timeout)


def _fetch_eastmoney_daily_kline(
    stock_code: str,
    start_date: str,
    end_date: str,
    adjust: str,
    *,
    timeout: float = 45.0,
) -> Optional[pd.DataFrame]:
    """
    东财日 K（与 akshare.stock_zh_a_hist period=daily 等价）。

    - 按 ``SM_KLINE_EAST_CHUNK_DAYS``（默认 240 天）**分片**请求，减轻单次长区间被对端 RST。
    - 按 ``SM_KLINE_EAST_HOSTS`` 或内置列表 **轮换镜像**。
    - 网络异常仍由外层 ``retry_remote`` 重试整次拉取。
    """
    symbol_6 = str(stock_code).strip().zfill(6)
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    if adjust not in adjust_dict:
        raise ValueError(f"adjust 须为 ''|qfq|hfq，收到: {adjust!r}")

    params_fixed: dict[str, str] = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": adjust_dict[adjust],
        "secid": _east_secid(symbol_6),
    }
    hosts = _east_host_bases()
    chunk_days = _east_chunk_days()
    chunk_sleep = _east_chunk_sleep()
    parts: list[pd.DataFrame] = []

    with requests.Session() as session:
        session.headers.update(_EAST_HEADERS)
        for cbeg, cend in _iter_yyyymmdd_chunks(start_date, end_date, chunk_days):
            params = {**params_fixed, "beg": cbeg, "end": cend}
            last_err: Exception | None = None
            chunk_ok = False
            for base in hosts:
                try:
                    chunk_df = _request_kline_once(session, base, params, timeout)
                    if chunk_df is not None and not chunk_df.empty:
                        parts.append(chunk_df)
                    chunk_ok = True
                    break
                except (requests.RequestException, ValueError, KeyError) as e:
                    last_err = e
                    logger.debug("东财 K 线镜像失败 %s %s~%s: %s", base, cbeg, cend, e)
                    continue
            if not chunk_ok and last_err is not None:
                raise last_err
            time.sleep(chunk_sleep)

    if not parts:
        return None
    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return out


def akshare_daily_to_sm_kline(
    df: pd.DataFrame,
    stock_code: str,
    k_type: int,
    adjust_type: int,
    *,
    short_name: str = "",
) -> pd.DataFrame:
    """
    将日 K DataFrame（列 date, open, high, low, close, volume, amount, turnover）转为 sm_stock_kline 列。
    short_name：股票简称，通常由调用方从 si_all_code 传入。
    """
    if "date" not in df.columns:
        raise ValueError("K 线 DataFrame 缺少 date 列")
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"]).sort_values("date")
    work["prev_close_calc"] = work["close"].shift(1)
    work["change_calc"] = work["close"] - work["prev_close_calc"]
    work["change_pct_calc"] = (work["change_calc"] / work["prev_close_calc"]) * 100
    work["turnover_ratio"] = work["turnover"] if "turnover" in work.columns else None

    rows: list[dict[str, Any]] = []
    for _, r in work.iterrows():
        d = r["date"]
        trade_date = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        trade_time = f"{trade_date} 15:00:00"

        def num(x: Any) -> Optional[float]:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        rows.append(
            {
                "stock_code": stock_code,
                "short_name": (short_name or "")[:128],
                "trade_time": trade_time,
                "trade_date": trade_date,
                "k_type": k_type,
                "adjust_type": adjust_type,
                "open": num(r.get("open")),
                "close": num(r.get("close")),
                "high": num(r.get("high")),
                "low": num(r.get("low")),
                "volume": num(r.get("volume")),
                "amount": num(r.get("amount")),
                "change": num(r.get("change_calc")),
                "change_pct": num(r.get("change_pct_calc")),
                "turnover_ratio": num(r.get("turnover_ratio")),
                "pre_close": num(r.get("prev_close_calc")),
            }
        )
    return pd.DataFrame(rows)


def delete_kline_range(
    engine: "Engine",
    stock_code: str,
    k_type: int,
    adjust_type: int,
    trade_start: str,
    trade_end: str,
) -> None:
    """删除该股在日期区间内、指定 k_type/adjust 的 K 线，便于增量覆盖。"""
    from sqlalchemy import text

    sql = text(
        """
        DELETE FROM sm_stock_kline
        WHERE stock_code = :code AND k_type = :kt AND adjust_type = :adj
          AND trade_date >= :d0 AND trade_date <= :d1
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "code": stock_code,
                "kt": k_type,
                "adj": adjust_type,
                "d0": trade_start,
                "d1": trade_end,
            },
        )
