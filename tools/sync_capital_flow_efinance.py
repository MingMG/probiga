#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用 efinance 获取真实个股资金流向数据写入 sm_stock_capital_flow_daily。

efinance 走的是东财 push2his 接口但有自己的连接池和重试机制，
在 push2 被限流时仍可能可用。

用法：
  python tools/sync_capital_flow_efinance.py
  python tools/sync_capital_flow_efinance.py --limit 100
  python tools/sync_capital_flow_efinance.py --date 2026-05-29 --limit 50

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
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import write_frame

def _engine():
    return create_tool_engine(resolve_tool_mysql_url())


def fetch_flow_efinance(stock_code: str) -> pd.DataFrame | None:
    """用 efinance 获取单只股票的120天资金流数据"""
    import efinance as ef
    try:
        df = ef.stock.get_history_bill(stock_code=stock_code)
        if df is None or df.empty:
            return None
        # 按列位置映射（efinance 返回的列名是中文，编码可能乱码）
        cols = df.columns.tolist()
        n = len(df)
        result = pd.DataFrame({
            "stock_code": [stock_code] * n,
            "trade_date": df[cols[2]].astype(str).str[:10].tolist(),
            "main_net_inflow": pd.to_numeric(df[cols[3]], errors="coerce").tolist(),
            "max_net_inflow": pd.to_numeric(df[cols[7]], errors="coerce").tolist(),
            "lg_net_inflow": pd.to_numeric(df[cols[6]], errors="coerce").tolist(),
            "mid_net_inflow": pd.to_numeric(df[cols[5]], errors="coerce").tolist(),
            "sm_net_inflow": pd.to_numeric(df[cols[4]], errors="coerce").tolist(),
        })
        return result
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser(description="efinance 同步资金流数据")
    p.add_argument("--limit", type=int, default=0, help="最多处理几只股票（0=全部）")
    p.add_argument("--date", type=str, default="", help="只保留指定日期（YYYY-MM-DD）")
    p.add_argument("--sleep", type=float, default=0.5, help="每只股票间隔秒数")
    p.add_argument("--skip-truncate", action="store_true", help="不清空表")
    args = p.parse_args()

    eng = _engine()

    # 获取股票列表
    with eng.connect() as conn:
        codes = [row[0] for row in conn.execute(
            text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|6)' ORDER BY stock_code")
        ).fetchall()]

    if args.limit > 0:
        codes = codes[:args.limit]

    print(f"共 {len(codes)} 只股票，开始同步资金流...")

    if not args.skip_truncate:
        with eng.begin() as conn:
            conn.execute(text("TRUNCATE TABLE sm_stock_capital_flow_daily"))
        print("已清空 sm_stock_capital_flow_daily")

    total_rows = 0
    success = 0
    fail = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i, code in enumerate(codes):
        code = str(code).strip().zfill(6)
        df = fetch_flow_efinance(code)

        if df is not None and not df.empty:
            if args.date:
                df = df[df["trade_date"] == args.date[:10]]

            if not df.empty:
                df["etl_sync_at"] = now_str
                write_frame(df, "sm_stock_capital_flow_daily", eng, if_exists="append", index=False, method="multi")
                total_rows += len(df)
                success += 1
        else:
            fail += 1

        if (i + 1) % 50 == 0:
            print(f"  进度: {i+1}/{len(codes)} | 成功: {success} | 失败: {fail} | 写入: {total_rows} 行")

        time.sleep(args.sleep)

    print(f"\n完成！成功 {success} 只，失败 {fail} 只，写入 {total_rows} 行")

    # 验证
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily")).scalar()
        max_d = conn.execute(text("SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily")).scalar()
        print(f"表中共 {cnt} 条，最新日期: {max_d}")


if __name__ == "__main__":
    main()
