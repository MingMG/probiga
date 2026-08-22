#!/usr/bin/env python3
from sqlalchemy import text

from tools.env_config import create_tool_engine

engine = create_tool_engine()

with engine.connect() as conn:
    print("=== 最近交易日数据 ===")
    rows = conn.execute(text(
        "SELECT trade_date, COUNT(DISTINCT stock_code) AS cnt "
        "FROM sm_stock_kline WHERE trade_date >= '2026-04-27' "
        "GROUP BY trade_date ORDER BY trade_date"
    )).fetchall()
    for r in rows:
        print(f"  {r[0]} -> {r[1]}只")

    print("\n=== 最新日期 & 总量 ===")
    r = conn.execute(text(
        "SELECT MAX(trade_date), COUNT(*), COUNT(DISTINCT stock_code) FROM sm_stock_kline"
    )).fetchone()
    print(f"  最新日期: {r[0]}, 总行数: {r[1]}, 股票数: {r[2]}")
