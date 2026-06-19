#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取指定日期的个股日度资金流向，写入 sm_stock_capital_flow_daily。
不删除历史数据，若当天已有则覆盖更新。

数据源策略（按优先级自动降级）:
  1. efinance — 东财数据的第三方封装（当前环境下比直连 push2his 更稳定）
  2. push2his.eastmoney.com — 东财历史日K接口（最快，但易被限流/断连）
  3. finance.pae.baidu.com — 百度API（备用）

限流保护:
  - 请求间隔、并发数、最小覆盖率可配置
  - 写入前检查覆盖率，避免接口雪崩时用少量脏数据覆盖完整日期
"""

import argparse
import os
import random
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests as http
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from server.common.config import get_mysql_url

_MAX_RETRIES = int(os.environ.get("FLOW_MAX_RETRIES", "1"))
_RETRY_BASE_DELAY = 3.0
_REQUEST_DELAY = float(os.environ.get("FLOW_REQUEST_DELAY", "0.5"))
_REQUEST_DELAY_JITTER = float(os.environ.get("FLOW_REQUEST_JITTER", "0.3"))  # 随机抖动范围
_BATCH_PAUSE = float(os.environ.get("FLOW_BATCH_PAUSE", "30.0"))
_BATCH_PAUSE_EVERY = int(os.environ.get("FLOW_BATCH_PAUSE_EVERY", "50"))
_WORKERS = max(1, int(os.environ.get("FLOW_WORKERS", "4")))
_MIN_COVERAGE = float(os.environ.get("FLOW_MIN_COVERAGE", "0.70"))

_UNIT_MULTIPLIERS = {"亿": 1e8, "万": 1e4}
_SOURCE_REGISTRY: dict[str, str] = {
    "efinance": "efinance",
    "push2his": "push2his",
    "baidu": "百度API",
}

# 代理配置（环境变量）
# FLOW_PROXY 示例: http://user:pass@gateway.kdlapi.com:15818
# 支持隧道代理（每次请求自动换IP）
_PROXY_URL = os.environ.get("FLOW_PROXY", "")

_SESSION = http.Session()
_SESSION.trust_env = False
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
})
if _PROXY_URL:
    _SESSION.proxies = {"http": _PROXY_URL, "https": _PROXY_URL}


@dataclass
class FetchOutcome:
    code: str
    df: pd.DataFrame | None
    source: str = ""
    error: Exception | None = None
    no_data: bool = False


def _mysql_url() -> str:
    return get_mysql_url(required=True)


def _read_stock_codes(engine):
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code FROM si_all_code ORDER BY stock_code")).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def _read_target_stock_codes(engine, target_date: str) -> list[str]:
    """资金流以目标交易日已有日K为股票池，避免历史/退市代码拖慢补数。"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT stock_code
            FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1
            ORDER BY stock_code
        """), {"d": target_date}).fetchall()
    codes = [str(r[0]).strip().zfill(6) for r in rows]
    return codes or _read_stock_codes(engine)


def _normalize_date(raw: str) -> str:
    s = raw.strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10]


def _build_sources(raw: str = ""):
    wanted = raw.strip() or os.environ.get("FLOW_SOURCES", "efinance,push2his,baidu")
    out = []
    for item in wanted.split(","):
        key = item.strip().lower()
        if not key:
            continue
        if key == "baiduapi":
            key = "baidu"
        if key not in _SOURCE_REGISTRY:
            raise ValueError(f"未知资金流数据源: {item}; 可选 efinance,push2his,baidu")
        out.append((key, _SOURCE_REGISTRY[key], {
            "efinance": _fetch_efinance,
            "push2his": _fetch_push2his,
            "baidu": _fetch_baidu,
        }[key]))
    if not out:
        raise ValueError("资金流数据源为空")
    return out


def _read_kline_stock_count(engine, target_date: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text("""
            SELECT COUNT(DISTINCT stock_code)
            FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1
        """), {"d": target_date}).scalar() or 0)


def _convert_value(val):
    if not isinstance(val, str):
        return float(val) if val is not None else 0.0
    val = val.replace("元", "").strip()
    if not val or val == "--":
        return 0.0
    for unit, mul in _UNIT_MULTIPLIERS.items():
        if unit in val:
            num = re.findall(r"([-+]?\d*\.?\d+)", val)
            return float(num[0]) * mul if num else 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _classify_error(e: Exception) -> str:
    msg = str(e).lower()
    if "connection" in msg or "timeout" in msg or "remote" in msg:
        return "网络/连接"
    if "json" in msg or "keyerror" in msg:
        return "API返回异常"
    if "429" in msg or "403" in msg:
        return "限流/被拦截"
    return type(e).__name__


def _fetch_push2his(stock_code: str, target_date: str) -> pd.DataFrame | None:
    cid = 1 if stock_code.startswith('6') else 0
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        f"lmt=0&klt=101&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&secid={cid}.{stock_code}"
    )
    resp = _SESSION.get(url, timeout=10)
    resp.raise_for_status()
    j = resp.json()
    if not j.get("data") or not j["data"].get("klines"):
        return None
    for line in j["data"]["klines"]:
        if target_date[:10] not in line:
            continue
        parts = line.split(",")
        if len(parts) < 6:
            continue
        return pd.DataFrame([{
            "stock_code": stock_code,
            "trade_date": parts[0],
            "main_net_inflow": float(parts[1]),
            "sm_net_inflow": float(parts[2]),
            "mid_net_inflow": float(parts[3]),
            "lg_net_inflow": float(parts[4]),
            "max_net_inflow": float(parts[5]),
        }])
    return None


def _fetch_baidu(stock_code: str, target_date: str) -> pd.DataFrame | None:
    dt = datetime.strptime(target_date[:10], "%Y-%m-%d")
    next_date = (dt + timedelta(days=1)).strftime("%Y%m%d")
    url = (
        "https://finance.pae.baidu.com/vapi/v1/fundsortlist?"
        f"code={stock_code}&market=ab&finance_type=stock&tab=day"
        f"&from=history&date={next_date}&pn=0&rn=1&finClientType=pc"
    )
    from adata.common.headers import baidu_headers
    resp = _SESSION.get(url, timeout=15, headers=baidu_headers.json_headers)
    resp.raise_for_status()
    j = resp.json()
    content = j.get("Result", {}).get("content", [])
    if not content:
        return None
    if isinstance(content, str):
        import json as _json
        try:
            content = _json.loads(content)
        except Exception:
            return None
        if not isinstance(content, list):
            return None
    for row in content:
        if not isinstance(row, dict):
            continue
        row_date = row.get("date", "").replace("/", "-")
        if row_date[:10] == target_date[:10]:
            return pd.DataFrame([{
                "stock_code": stock_code,
                "trade_date": row_date,
                "main_net_inflow": _convert_value(row.get("extMainIn", 0)),
                "sm_net_inflow": _convert_value(row.get("littleNetIn", 0)),
                "mid_net_inflow": _convert_value(row.get("mediumNetIn", 0)),
                "lg_net_inflow": _convert_value(row.get("largeNetIn", 0)),
                "max_net_inflow": _convert_value(row.get("superNetIn", 0)),
            }])
    return None


def _fetch_efinance(stock_code: str, target_date: str) -> pd.DataFrame | None:
    """用 efinance 获取单只股票的资金流向（走东财push2his，但有独立连接池）"""
    import efinance as ef
    df = ef.stock.get_history_bill(stock_code=stock_code)
    if df is None or df.empty:
        return None
    cols = df.columns.tolist()
    # efinance列: [股票名称, 股票代码, 日期, 主力净流入, 小单净流入, 中单净流入, 大单净流入, 超大单净流入, ...]
    df["日期"] = pd.to_datetime(df[cols[2]]).dt.strftime("%Y-%m-%d")
    matched = df[df["日期"] == target_date[:10]]
    if matched.empty:
        return None
    row = matched.iloc[0]
    return pd.DataFrame([{
        "stock_code": stock_code,
        "trade_date": row["日期"],
        "main_net_inflow": float(row[cols[3]]),
        "sm_net_inflow": float(row[cols[4]]),
        "mid_net_inflow": float(row[cols[5]]),
        "lg_net_inflow": float(row[cols[6]]),
        "max_net_inflow": float(row[cols[7]]),
    }])


def _fetch_one_stock(code: str, target_date: str, sources) -> FetchOutcome:
    last_error: Exception | None = None
    for key, name, fetch_func in sources:
        for attempt in range(1 + _MAX_RETRIES):
            try:
                df = fetch_func(code, target_date)
                if df is not None and not df.empty:
                    df = df.copy()
                    df["data_source"] = key
                    return FetchOutcome(code=code, df=df, source=name)
                break
            except Exception as e:
                last_error = e
                if attempt < _MAX_RETRIES:
                    retry_delay = _RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1.5)
                    time.sleep(retry_delay)

    if last_error is not None:
        return FetchOutcome(code=code, df=None, error=last_error)
    return FetchOutcome(code=code, df=None, no_data=True)


def _write_flow_daily(engine, target_date: str, df: pd.DataFrame) -> None:
    full_df = df.replace({np.nan: None, pd.NaT: None})
    full_df = full_df.drop_duplicates(subset=["stock_code", "trade_date"], keep="last")
    numeric_cols = ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]
    for col in numeric_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors="coerce").fillna(0)

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM `sm_stock_capital_flow_daily` WHERE `trade_date` = :d"),
            {"d": target_date}
        )

    full_df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    columns = ["stock_code", "trade_date", *numeric_cols, "etl_sync_at", "data_source"]
    full_df[columns].to_sql(
        "sm_stock_capital_flow_daily",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )


def fetch_capital_flow_daily(
    target_date: str,
    *,
    sources_raw: str = "",
    min_coverage: float | None = None,
    dry_run: bool = False,
) -> int:
    target_date = _normalize_date(target_date)
    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    stock_codes = _read_target_stock_codes(engine, target_date)
    max_stocks = int(os.environ.get("SM_MAX_STOCKS", "0"))
    if max_stocks > 0:
        stock_codes = stock_codes[:max_stocks]

    sources = _build_sources(sources_raw)
    min_coverage = _MIN_COVERAGE if min_coverage is None else min_coverage

    total = len(stock_codes)
    print(f"开始获取个股日度资金流向，目标日期: {target_date}")
    print(f"共 {total} 只股票待处理")
    print(f"数据源链: {' -> '.join(name for _, name, _ in sources)}")
    print(f"代理: {_PROXY_URL[:30] + '...' if _PROXY_URL else '无（直连）'}")
    print(f"并发: {_WORKERS}, 请求间隔: {_REQUEST_DELAY}s +/- {_REQUEST_DELAY_JITTER}s, 重试: {_MAX_RETRIES}次")
    print(f"最小覆盖率: {min_coverage:.0%}, dry_run={dry_run}")
    print()

    parts: list[pd.DataFrame] = []
    success = 0
    failed = 0
    no_data = 0
    source_counter: Counter[str] = Counter()
    error_types: Counter[str] = Counter()

    t_start = time.time()
    done = 0
    first_errors = 0

    def _worker(code: str) -> FetchOutcome:
        outcome = _fetch_one_stock(code, target_date, sources)
        time.sleep(_REQUEST_DELAY + random.uniform(0, _REQUEST_DELAY_JITTER))
        return outcome

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        futures = {pool.submit(_worker, code): code for code in stock_codes}
        for future in as_completed(futures):
            done += 1
            code = futures[future]
            try:
                outcome = future.result()
            except Exception as e:  # 理论上 worker 已兜住，这里防御线程级异常
                outcome = FetchOutcome(code=code, df=None, error=e)

            if outcome.df is not None and not outcome.df.empty:
                parts.append(outcome.df)
                success += 1
                source_counter[outcome.source] += 1
            elif outcome.error is not None:
                failed += 1
                category = _classify_error(outcome.error)
                error_types[category] += 1
                if first_errors < 15:
                    first_errors += 1
                    print(f"  {outcome.code} 失败 [{category}]: {outcome.error}")
            else:
                no_data += 1

            if done % 200 == 0 or done == total:
                elapsed = time.time() - t_start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(f"  进度: {done}/{total} 成功={success} 失败={failed} 无数据={no_data} "
                      f"{elapsed:.0f}s 已用, 预计还需 {eta:.0f}s")

            if _BATCH_PAUSE_EVERY > 0 and done % _BATCH_PAUSE_EVERY == 0 and done < total:
                pause = _BATCH_PAUSE + random.uniform(0, 10)
                print(f"  批次暂停 {pause:.0f}s（已处理 {done} 只）...")
                time.sleep(pause)

    elapsed_total = time.time() - t_start
    print(f"\n===== 汇总 =====")
    print(f"  总数: {total}")
    print(f"  成功: {success} ({success/total*100:.1f}%)")
    print(f"  失败: {failed} ({failed/total*100:.1f}%)")
    if error_types:
        print(f"  失败分类:")
        for cat, cnt in error_types.most_common():
            print(f"    - {cat}: {cnt}")
    if source_counter:
        print(f"  数据源命中:")
        for source, cnt in source_counter.most_common():
            print(f"    - {source}: {cnt}")
    print(f"  无数据: {no_data} ({no_data/total*100:.1f}%)")
    print(f"  耗时: {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")

    if not parts:
        print("未获取到任何资金流向数据")
        return 2

    full_df = pd.concat(parts, ignore_index=True)
    full_df = full_df.drop_duplicates(subset=["stock_code", "trade_date"], keep="last")
    got = len(full_df)
    kline_count = _read_kline_stock_count(engine, target_date)
    denom = kline_count or total
    coverage = got / max(denom, 1)
    print(f"  覆盖率检查: {got}/{denom} ({coverage:.1%})")

    if coverage < min_coverage:
        print(f"覆盖率 {coverage:.1%} 低于阈值 {min_coverage:.0%}，已停止写库，避免覆盖完整旧数据")
        return 3

    if dry_run:
        print("[dry-run] 覆盖率达标，但不写入数据库")
        return 0

    _write_flow_daily(engine, target_date, full_df)

    print(f"写入完成: sm_stock_capital_flow_daily, 共 {len(full_df)} 行, 日期: {target_date}")
    print(f"  成功: {success} 只股票, 失败: {failed} 只")
    return 0


def main():
    parser = argparse.ArgumentParser(description="获取指定日期的个股日度资金流向（efinance -> push2his -> 百度API）")
    parser.add_argument("date", help="目标日期，格式：YYYY-MM-DD")
    parser.add_argument(
        "--sources",
        default="",
        help="数据源顺序，逗号分隔。默认读 FLOW_SOURCES 或 efinance,push2his,baidu",
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=None,
        help="写库前最小覆盖率，默认读 FLOW_MIN_COVERAGE 或 0.70",
    )
    parser.add_argument("--dry-run", action="store_true", help="只抓取和检查覆盖率，不写库")
    args = parser.parse_args()

    date_str = _normalize_date(args.date)
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return 2

    return fetch_capital_flow_daily(
        date_str,
        sources_raw=args.sources,
        min_coverage=args.min_coverage,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
