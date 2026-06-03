#!/usr/bin/env python3
import efinance as ef

print("efinance functions:")
print(dir(ef.stock))

print("\nTest get_history_bill:")
try:
    df = ef.stock.get_history_bill("000001")
    print(f"  shape: {df.shape}")
    if not df.empty:
        print(f"  columns: {list(df.columns)}")
        print(df.tail(3).to_string())
except Exception as e:
    print(f"  error: {e}")
