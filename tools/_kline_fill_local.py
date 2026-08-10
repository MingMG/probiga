#!/usr/bin/env python3
"""本地运行 K 线增量同步：补齐 2026-04-28 ~ 2026-05-08"""
import sys
import os
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

import pandas as pd
from sqlalchemy import text
from env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.batch_db import write_frame

START_DATE = "2026-04-28"
END_DATE = "2026-05-08"

def main():
    remote_mysql = os.environ.get("REMOTE_MYSQL_URL") or resolve_tool_mysql_url()
    engine = create_tool_engine(remote_mysql, connect_args={"connect_timeout": 30})

    with engine.connect() as conn:
        all_codes = [r[0] for r in conn.execute(text("SELECT stock_code FROM si_all_code ORDER BY stock_code")).fetchall()]
        existing = set(r[0] for r in conn.execute(text(
            "SELECT stock_code FROM sm_stock_kline WHERE trade_date >= :s GROUP BY stock_code HAVING COUNT(*) >= 5"
        ), {"s": START_DATE}).fetchall())

    missing = [c for c in all_codes if c not in existing]
    print(f"Total: {len(all_codes)}, Already: {len(existing)}, Missing: {len(missing)}", flush=True)

    if not missing:
        print("All stocks have K-line data for the period. Done.", flush=True)
        return

    from adata.stock.market.stock_market.stock_market import StockMarket
    mk = StockMarket()
    kline_cols = [
        "stock_code", "short_name", "trade_time", "trade_date", "k_type", "adjust_type",
        "open", "close", "high", "low", "volume", "amount", "change", "change_pct",
        "turnover_ratio", "pre_close",
    ]

    success = 0
    failed = 0
    failed_codes = []
    batch = []
    batch_size = 100

    def flush_batch():
        nonlocal batch
        if not batch:
            return
        combined = pd.concat(batch, ignore_index=True)
        combined["etl_sync_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        with engine.begin() as conn:
            for _, row in combined.iterrows():
                conn.execute(text(
                    "DELETE FROM sm_stock_kline WHERE stock_code = :c AND trade_date >= :s AND trade_date <= :e"
                ), {"c": row["stock_code"], "s": START_DATE, "e": END_DATE})
        write_frame(combined, "sm_stock_kline", engine, if_exists="append", index=False)
        print(f"  [BATCH] Wrote {len(combined)} rows ({len(batch)} stocks)", flush=True)
        batch = []

    for i, code in enumerate(missing):
        try:
            df = mk.get_market(
                stock_code=code, start_date=START_DATE, end_date=END_DATE,
                k_type=1, adjust_type=1,
            )
            if df is not None and not df.empty:
                df["k_type"] = 1
                df["adjust_type"] = 1
                df["short_name"] = df.get("short_name", "")
                batch.append(df[kline_cols])
                success += 1
            else:
                failed += 1
                failed_codes.append(code)
        except Exception as e:
            failed += 1
            failed_codes.append(code)

        if (i + 1) % batch_size == 0:
            flush_batch()
            print(f"[{i+1}/{len(missing)}] success={success}, failed={failed}", flush=True)

        time.sleep(0.2)

    flush_batch()
    print(f"\nDone! success={success}, failed={failed}", flush=True)
    if failed_codes:
        print(f"Failed codes (first 50): {failed_codes[:50]}", flush=True)

if __name__ == "__main__":
    main()
