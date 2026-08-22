#!/usr/bin/env python3
"""Manual K-line fill script - runs on server, bypasses adata session caching."""
import time
import requests
import pandas as pd
from sqlalchemy import text

from tools.env_config import create_tool_engine
START_DATE = "2026-04-28"
END_DATE = "2026-05-08"

def get_stock_codes(engine):
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code FROM si_all_code ORDER BY stock_code")).fetchall()
    return [r[0] for r in rows]

def get_existing_codes(engine):
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT stock_code FROM sm_stock_kline WHERE trade_date >= :s GROUP BY stock_code HAVING COUNT(*) >= 5"
        ), {"s": START_DATE}).fetchall()
    return set(r[0] for r in rows)

def fetch_kline_adata(code, start, end):
    """Direct HTTP call to adata API with fresh session each time."""
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    try:
        url = f"https://www.adata.com/api/stock/market/kline"
        params = {
            "stock_code": code,
            "start_date": start,
            "end_date": end,
            "k_type": 1,
            "adjust_type": 1,
        }
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 200 or not data.get("data"):
            return None
        rows = data["data"]
        df = pd.DataFrame(rows)
        return df
    except Exception as e:
        return None
    finally:
        session.close()

def main():
    engine = create_tool_engine()
    all_codes = get_stock_codes(engine)
    existing = get_existing_codes(engine)
    missing = [c for c in all_codes if c not in existing]
    print(f"Total: {len(all_codes)}, Already have data: {len(existing)}, Missing: {len(missing)}")

    if not missing:
        print("All stocks have data. Nothing to do.")
        return

    success = 0
    failed = 0
    for i, code in enumerate(missing):
        df = fetch_kline_adata(code, START_DATE, END_DATE)
        if df is not None and len(df) > 0:
            success += 1
        else:
            failed += 1
        time.sleep(0.5)
        if (i + 1) % 50 == 0:
            print(f"Progress: {i+1}/{len(missing)}, success={success}, failed={failed}")

    print(f"Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
