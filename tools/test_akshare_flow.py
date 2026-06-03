#!/usr/bin/env python3
import akshare as ak

print("=== 测试 akshare 资金流向 ===")

# 测试个股历史资金流
print("\n1. stock_individual_fund_flow (000001):")
try:
    df = ak.stock_individual_fund_flow(stock="000001", market="sz")
    print(f"   shape: {df.shape}")
    print(f"   columns: {list(df.columns)}")
    if not df.empty:
        print(f"   latest 3 rows:")
        print(df.tail(3).to_string())
except Exception as e:
    print(f"   error: {e}")

# 测试市场资金流
print("\n2. stock_fund_flow_individual (today):")
try:
    df = ak.stock_fund_flow_individual(symbol="即时")
    print(f"   shape: {df.shape}")
    print(f"   columns: {list(df.columns)}")
    if not df.empty:
        print(f"   sample:")
        print(df.head(3).to_string())
except Exception as e:
    print(f"   error: {e}")
