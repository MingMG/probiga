#!/usr/bin/env python3
import sys

sys.path.insert(0, "/opt/ProBigA")

from tools.env_config import create_tool_engine

engine = create_tool_engine()
trade_date = "2026-06-02"

from tools.screen_stocks import run_trend_strong, run_low_start, run_trend, run_flow

print("=== test screening modes ===")

print("\n1. run_trend_strong (k_type=1, adjust_type=0):")
try:
    df = run_trend_strong(engine, trade_date, 10, 1, 0, 10, 0.5, 0.8, 2.5, 150.0, 0.95)
    cnt = len(df) if df is not None else 0
    print(f"   result: {cnt}")
    if df is not None and not df.empty:
        print(df[["stock_code", "short_name"]].head(3).to_string())
except Exception as e:
    print(f"   error: {e}")

print("\n2. run_low_start:")
try:
    df = run_low_start(engine, trade_date, 10, 1, 0, 60, 0.28, 1.25, 2.0, 10.5)
    cnt = len(df) if df is not None else 0
    print(f"   result: {cnt}")
except Exception as e:
    print(f"   error: {e}")

print("\n3. run_trend:")
try:
    df = run_trend(engine, trade_date, 10, 1, 0, 0)
    cnt = len(df) if df is not None else 0
    print(f"   result: {cnt}")
except Exception as e:
    print(f"   error: {e}")

print("\n4. run_flow:")
try:
    df = run_flow(engine, trade_date, 10, 5000000)
    cnt = len(df) if df is not None else 0
    print(f"   result: {cnt}")
except Exception as e:
    print(f"   error: {e}")
