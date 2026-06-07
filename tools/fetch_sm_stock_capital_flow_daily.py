#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取指定日期的个股日度资金流向，写入 sm_stock_capital_flow_daily。
不删除历史数据，若当天已有则覆盖更新。

数据源策略（按优先级自动降级）:
  1. push2his.eastmoney.com — 东财历史日K接口（最快，但易被限流）
  2. efinance — 东财数据的第三方封装（有独立连接池和重试）
  3. finance.pae.baidu.com — 百度API（备用）

限流保护:
  - 请求间隔可配置（默认0.15s，被封后建议0.5s+）
  - 每N只股票暂停一批
  - 连续失败达阈值自动切换数据源
  - 所有数据源都失败时等待冷却后重试
"""

import argparse
import os
import random
import re
import sys
import time
from collections import Counter
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

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

_MAX_RETRIES = 2
_RETRY_BASE_DELAY = 3.0
_REQUEST_DELAY = float(os.environ.get("FLOW_REQUEST_DELAY", "0.5"))
_REQUEST_DELAY_JITTER = float(os.environ.get("FLOW_REQUEST_JITTER", "0.3"))  # 随机抖动范围
_BATCH_PAUSE = float(os.environ.get("FLOW_BATCH_PAUSE", "30.0"))
_BATCH_PAUSE_EVERY = int(os.environ.get("FLOW_BATCH_PAUSE_EVERY", "50"))

_CONSECUTIVE_FAIL_THRESHOLD = 10
_COOLDOWN_WAIT = 180  # 所有数据源都失败时等待秒数

_UNIT_MULTIPLIERS = {"亿": 1e8, "万": 1e4}

# 代理配置（环境变量）
# FLOW_PROXY 示例: http://user:pass@gateway.kdlapi.com:15818
# 支持隧道代理（每次请求自动换IP）
_PROXY_URL = os.environ.get("FLOW_PROXY", "")

_SESSION = http.Session()
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
})
if _PROXY_URL:
    _SESSION.proxies = {"http": _PROXY_URL, "https": _PROXY_URL}


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


def _read_stock_codes(engine):
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code FROM si_all_code ORDER BY stock_code")).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


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
    resp = _SESSION.get(url, timeout=15)
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
    try:
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
    except Exception:
        return None


def fetch_capital_flow_daily(target_date: str):
    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    stock_codes = _read_stock_codes(engine)
    max_stocks = int(os.environ.get("SM_MAX_STOCKS", "0"))
    if max_stocks > 0:
        stock_codes = stock_codes[:max_stocks]

    total = len(stock_codes)
    print(f"开始获取个股日度资金流向，目标日期: {target_date}")
    print(f"共 {total} 只股票待处理")
    print(f"数据源链: {' -> '.join(name for name, _ in sources)}")
    print(f"代理: {_PROXY_URL[:30] + '...' if _PROXY_URL else '无（直连）'}")
    print(f"请求间隔: {_REQUEST_DELAY}s, 重试: {_MAX_RETRIES}次, 连续失败阈值: {_CONSECUTIVE_FAIL_THRESHOLD}")
    print()

    # 数据源链: push2his -> efinance -> 百度API
    sources = [
        ("push2his", _fetch_push2his),
        ("efinance", _fetch_efinance),
        ("百度API", _fetch_baidu),
    ]
    source_idx = 0
    current_source, fetch_func = sources[source_idx]

    parts: list[pd.DataFrame] = []
    success = 0
    failed = 0
    no_data = 0
    error_types: Counter[str] = Counter()
    consecutive_fail = 0

    t_start = time.time()

    for i, code in enumerate(stock_codes):
        df = None
        last_error = None

        for attempt in range(1 + _MAX_RETRIES):
            try:
                df = fetch_func(code, target_date)
                if df is not None:
                    break
            except Exception as e:
                last_error = e
                if attempt < _MAX_RETRIES:
                    retry_delay = _RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 2)
                    time.sleep(retry_delay)

        if df is not None:
            parts.append(df)
            success += 1
            consecutive_fail = 0
        elif last_error is not None:
            failed += 1
            consecutive_fail += 1
            category = _classify_error(last_error)
            error_types[f"[{current_source}] {category}"] += 1
            if failed <= 15:
                print(f"  {code} 失败 [{current_source}/{category}]: {last_error}")
        else:
            no_data += 1
            consecutive_fail = 0

        if consecutive_fail >= _CONSECUTIVE_FAIL_THRESHOLD:
            if source_idx < len(sources) - 1:
                source_idx += 1
                current_source, fetch_func = sources[source_idx]
                print(f"\n  [!] 连续 {consecutive_fail} 次失败，切换到数据源: {current_source}")
                consecutive_fail = 0
            else:
                cooldown = _COOLDOWN_WAIT + random.uniform(0, 60)
                print(f"\n  [!] 所有数据源都连续失败，等待 {cooldown:.0f}s 冷却...")
                time.sleep(cooldown)
                source_idx = 0
                current_source, fetch_func = sources[source_idx]
                consecutive_fail = 0

        # 随机抖动，避免固定间隔被识别为爬虫
        delay = _REQUEST_DELAY + random.uniform(0, _REQUEST_DELAY_JITTER)
        time.sleep(delay)

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t_start
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate if rate > 0 else 0
            print(f"  进度: {i+1}/{total} 成功={success} 失败={failed} 无数据={no_data} "
                  f"[{current_source}] {elapsed:.0f}s 已用, 预计还需 {eta:.0f}s")

        if _BATCH_PAUSE_EVERY > 0 and (i + 1) % _BATCH_PAUSE_EVERY == 0:
            pause = _BATCH_PAUSE + random.uniform(0, 10)
            print(f"  批次暂停 {pause:.0f}s（已处理 {i+1} 只）...")
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
    print(f"  无数据: {no_data} ({no_data/total*100:.1f}%)")
    print(f"  耗时: {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")

    if not parts:
        print("未获取到任何资金流向数据")
        return

    full_df = pd.concat(parts, ignore_index=True)
    full_df = full_df.replace({np.nan: None, pd.NaT: None})
    full_df = full_df.drop_duplicates(subset=["stock_code", "trade_date"], keep="last")

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM `sm_stock_capital_flow_daily` WHERE `trade_date` = :d"),
            {"d": target_date[:10]}
        )

    full_df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    full_df.to_sql("sm_stock_capital_flow_daily", engine, if_exists="append", index=False,
                   chunksize=500, method="multi")

    print(f"写入完成: sm_stock_capital_flow_daily, 共 {len(full_df)} 行, 日期: {target_date[:10]}")
    print(f"  成功: {success} 只股票, 失败: {failed} 只")


def main():
    parser = argparse.ArgumentParser(description="获取指定日期的个股日度资金流向（push2his -> efinance -> 百度API）")
    parser.add_argument("date", help="目标日期，格式：YYYY-MM-DD")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return

    fetch_capital_flow_daily(args.date)


if __name__ == "__main__":
    main()
