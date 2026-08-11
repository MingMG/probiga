#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用 K 线数据估算资金流向（当真实 API 不可用时的替代方案）。

原理：
  - 上涨日：成交额 × 涨幅比例 ≈ 主力净流入
  - 下跌日：成交额 × 跌幅比例 ≈ 主力净流出
  - 用 5 日滚动窗口计算短期资金趋势

用法：
  python tools/calc_capital_flow_approx.py
  python tools/calc_capital_flow_approx.py --days 5

环境变量：MYSQL_URL
"""
from __future__ import annotations
from env_config import create_tool_engine, resolve_tool_mysql_url

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from server.common.batch_db import read_frame, write_frame

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def _engine():
    return create_tool_engine(resolve_tool_mysql_url())


def main():
    p = argparse.ArgumentParser(description="用K线数据估算资金流向")
    p.add_argument("--days", type=int, default=5, help="计算最近N天的资金流（默认5）")
    args = p.parse_args()

    eng = _engine()

    # 获取最新交易日
    with eng.connect() as conn:
        max_date = conn.execute(text("SELECT MAX(trade_date) FROM sm_stock_kline")).scalar()
    print(f"最新K线日期: {max_date}")

    # 获取最近N天的K线数据
    print(f"获取最近 {args.days} 天的K线数据...")
    df = read_frame(text(f"""
        SELECT stock_code, trade_date, open, close, high, low, volume, amount, change_pct
        FROM sm_stock_kline
        WHERE k_type = 1 AND trade_date >= DATE_SUB(:d, INTERVAL {args.days + 5} DAY)
        ORDER BY stock_code, trade_date DESC
    """), eng, params={"d": max_date})

    if df.empty:
        print("无K线数据")
        return

    print(f"获取到 {len(df)} 条K线数据")

    # 转换数值类型
    for col in ["open", "close", "high", "low", "volume", "amount", "change_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 按股票分组计算
    results = []
    for code, grp in df.groupby("stock_code"):
        grp = grp.sort_values("trade_date", ascending=False).head(args.days)
        if len(grp) < 2:
            continue

        # 估算资金流：涨日视为流入，跌日视为流出
        total_flow = 0
        for _, row in grp.iterrows():
            chg = row["change_pct"] or 0
            amt = row["amount"] or 0
            # 简化模型：涨幅比例 * 成交额 ≈ 净流入
            # 涨幅为正 = 流入，为负 = 流出
            flow = amt * (chg / 100) * 0.5  # 系数0.5因为涨幅不完全代表资金方向
            total_flow += flow

        results.append({
            "stock_code": str(code).strip().zfill(6),
            "trade_date": max_date,
            "main_net_inflow": round(total_flow, 2),
            "max_net_inflow": round(total_flow * 0.3, 2),  # 估算超大单约30%
            "lg_net_inflow": round(total_flow * 0.3, 2),
            "mid_net_inflow": round(-total_flow * 0.2, 2),  # 中单与主力反向
            "sm_net_inflow": round(-total_flow * 0.3, 2),   # 小单与主力反向
        })

    if not results:
        print("无计算结果")
        return

    result_df = pd.DataFrame(results)

    # 写入数据库
    print(f"写入 {len(result_df)} 条估算资金流数据...")
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM sm_stock_capital_flow_daily"))

    write_frame(result_df, "sm_stock_capital_flow_daily", eng, if_exists="append", index=False, method="multi", chunksize=1000)

    # 验证
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily")).scalar()
        top = conn.execute(text("""
            SELECT stock_code, main_net_inflow FROM sm_stock_capital_flow_daily
            ORDER BY main_net_inflow DESC LIMIT 10
        """)).fetchall()

    print(f"\n已写入 {cnt} 条数据")
    print("\n主力净流入 TOP10（估算）：")
    for code, flow in top:
        print(f"  {code}: {flow/10000:.0f}万")

    print("\n⚠️ 注意：这是基于K线数据的估算值，仅供参考，不替代真实资金流数据。")


if __name__ == "__main__":
    main()
