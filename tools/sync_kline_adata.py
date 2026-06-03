#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用 adata 同步全量K线数据（增量模式，不清空已有数据）。

用法：
  python tools/sync_kline_adata.py
  python tools/sync_kline_adata.py --start 2020-01-01 --end 2026-05-29
  python tools/sync_kline_adata.py --limit 100

环境变量：MYSQL_URL
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

DEFAULT_MYSQL_URL = "mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4"


def _engine():
    url = os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)
    return create_engine(url, pool_pre_ping=True)


def main():
    p = argparse.ArgumentParser(description="adata 同步K线数据（增量）")
    p.add_argument("--start", type=str, default="2020-01-01", help="开始日期")
    p.add_argument("--end", type=str, default="2026-05-29", help="结束日期")
    p.add_argument("--limit", type=int, default=0, help="最多处理几只（0=全部）")
    p.add_argument("--sleep", type=float, default=0.3, help="每只间隔秒数")
    p.add_argument("--skip-existing", action="store_true", help="跳过已有数据的股票")
    args = p.parse_args()

    eng = _engine()

    # 获取股票列表
    with eng.connect() as conn:
        codes = [row[0] for row in conn.execute(
            text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|6)' ORDER BY stock_code")
        ).fetchall()]

    if args.limit > 0:
        codes = codes[:args.limit]

    # 获取已有数据的股票
    existing = set()
    if args.skip_existing:
        with eng.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT stock_code FROM sm_stock_kline WHERE trade_date >= :d"
            ), {"d": args.start}).fetchall()
            existing = {str(r[0]).strip().zfill(6) for r in rows}
        print(f"已有 {len(existing)} 只股票有数据")

    print(f"共 {len(codes)} 只股票，同步 {args.start} ~ {args.end} 的K线...")

    from adata.stock.market.stock_market.stock_market import StockMarket
    sm = StockMarket()

    total_rows = 0
    success = 0
    fail = 0
    skip = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i, code in enumerate(codes):
        code = str(code).strip().zfill(6)

        # 跳过已有数据的股票
        if code in existing:
            skip += 1
            continue

        try:
            df = sm.get_market(stock_code=code, start_date=args.start, end_date=args.end)
            if df is not None and not df.empty:
                # 确保列名一致
                df["stock_code"] = code
                df["etl_sync_at"] = now_str
                df["k_type"] = 1
                df["adjust_type"] = 1

                # 删除该股票的旧数据
                with eng.begin() as conn:
                    conn.execute(text("DELETE FROM sm_stock_kline WHERE stock_code = :c"), {"c": code})

                # 写入
                cols = ["stock_code", "trade_date", "open", "close", "high", "low",
                        "volume", "amount", "change", "change_pct", "turnover_ratio",
                        "pre_close", "k_type", "adjust_type", "etl_sync_at"]
                for col in cols:
                    if col not in df.columns:
                        df[col] = None
                df[cols].to_sql("sm_stock_kline", eng, if_exists="append", index=False, method="multi")
                total_rows += len(df)
                success += 1
            else:
                fail += 1
        except Exception:
            fail += 1

        if (success + fail) % 200 == 0 and (success + fail) > 0:
            print(f"  进度: {i+1}/{len(codes)} | 成功: {success} | 失败: {fail} | 跳过: {skip} | 写入: {total_rows}")

        time.sleep(args.sleep)

    print(f"\n完成！成功 {success}，失败 {fail}，跳过 {skip}，写入 {total_rows} 行")

    # 验证
    with eng.connect() as conn:
        r = conn.execute(text("SELECT MAX(trade_date), COUNT(DISTINCT stock_code), COUNT(*) FROM sm_stock_kline")).fetchone()
        print(f"K线表: 最新={r[0]} | {r[1]} 只股票 | {r[2]} 条")


if __name__ == "__main__":
    main()
