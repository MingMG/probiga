#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通过新浪财经 API 批量同步个股日K线数据（绕过东财限流）。

新浪接口：https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData
返回字段：day, open, high, low, close, volume

用法：
  python tools/sync_kline_sina.py
  python tools/sync_kline_sina.py --days 30 --limit 100

环境变量：MYSQL_URL
"""
from __future__ import annotations
from env_config import create_tool_engine, resolve_tool_mysql_url

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import write_frame

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _engine():
    return create_tool_engine(resolve_tool_mysql_url())


def fetch_kline_sina(stock_code: str, days: int = 30) -> pd.DataFrame | None:
    """通过新浪API获取单只股票日K线"""
    # 新浪用 sh/sz 前缀
    if stock_code.startswith("6"):
        symbol = f"sh{stock_code}"
    else:
        symbol = f"sz{stock_code}"

    url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen={days}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None

        rows = []
        for item in data:
            rows.append({
                "stock_code": stock_code,
                "trade_date": item["day"][:10],
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "volume": int(item["volume"]),
                "k_type": 1,
                "adjust_type": 1,
            })

        df = pd.DataFrame(rows)
        # 计算涨跌幅和换手率
        df = df.sort_values("trade_date")
        df["pre_close"] = df["close"].shift(1)
        df["change_pct"] = ((df["close"] - df["pre_close"]) / df["pre_close"] * 100).round(2)
        df["amount"] = 0  # 新浪不提供成交额
        df["turnover_ratio"] = 0  # 新浪不提供换手率
        df["short_name"] = ""
        return df
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser(description="新浪K线批量同步")
    p.add_argument("--days", type=int, default=30, help="每只股票获取最近N天（默认30）")
    p.add_argument("--limit", type=int, default=0, help="最多处理几只（0=全部）")
    p.add_argument("--sleep", type=float, default=0.2, help="每只间隔秒数")
    p.add_argument("--skip-truncate", action="store_true", help="不清空表，增量写入")
    args = p.parse_args()

    eng = _engine()

    # 获取股票列表
    with eng.connect() as conn:
        codes = [row[0] for row in conn.execute(
            text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|6)' ORDER BY stock_code")
        ).fetchall()]

    if args.limit > 0:
        codes = codes[:args.limit]

    print(f"共 {len(codes)} 只股票，获取最近 {args.days} 天K线...")

    # 清空表
    if not args.skip_truncate:
        with eng.begin() as conn:
            conn.execute(text("TRUNCATE TABLE sm_stock_kline"))
        print("已清空 sm_stock_kline")

    total_rows = 0
    success = 0
    fail = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i, code in enumerate(codes):
        code = str(code).strip().zfill(6)
        df = fetch_kline_sina(code, args.days)

        if df is not None and not df.empty:
            df["etl_sync_at"] = now_str
            # 删除该股票的旧数据
            if args.skip_truncate:
                with eng.begin() as conn:
                    conn.execute(text("DELETE FROM sm_stock_kline WHERE stock_code = :c"), {"c": code})
            write_frame(df, "sm_stock_kline", eng, if_exists="append", index=False, method="multi")
            total_rows += len(df)
            success += 1
        else:
            fail += 1

        if (i + 1) % 200 == 0:
            print(f"  进度: {i+1}/{len(codes)} | 成功: {success} | 失败: {fail} | 写入: {total_rows} 行")

        time.sleep(args.sleep)

    print(f"\n完成！成功 {success} 只，失败 {fail} 只，写入 {total_rows} 行")

    # 验证
    with eng.connect() as conn:
        r = conn.execute(text("SELECT MAX(trade_date), COUNT(DISTINCT stock_code), COUNT(*) FROM sm_stock_kline")).fetchone()
        print(f"最新日期: {r[0]} | {r[1]} 只股票 | {r[2]} 条数据")


if __name__ == "__main__":
    main()
