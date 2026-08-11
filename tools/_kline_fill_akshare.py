#!/usr/bin/env python3
"""Use akshare to fill missing K-line data. Runs on server."""
import time
import sys
from pathlib import Path
import pandas as pd
import akshare as ak
from sqlalchemy import text
from env_config import create_tool_engine, resolve_tool_mysql_url

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import write_frame

START_DATE = "2026-04-28"
END_DATE = "2026-05-08"


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())

    with engine.connect() as conn:
        all_codes = [r[0] for r in conn.execute(text("SELECT stock_code FROM si_all_code ORDER BY stock_code")).fetchall()]
        existing = set(r[0] for r in conn.execute(text(
            "SELECT stock_code FROM sm_stock_kline WHERE trade_date >= :s GROUP BY stock_code HAVING COUNT(*) >= 5"
        ), {"s": START_DATE}).fetchall())

    missing = [c for c in all_codes if c not in existing]
    print(f"Total: {len(all_codes)}, Already: {len(existing)}, Missing: {len(missing)}")

    if not missing:
        print("All stocks have data. Done.")
        return

    success = 0
    failed = 0
    failed_codes = []

    for i, code in enumerate(missing):
        try:
            symbol = str(code).strip().zfill(6)
            df = ak.stock_zh_a_hist(
                symbol=symbol, period="daily",
                start_date=START_DATE.replace("-", ""),
                end_date=END_DATE.replace("-", ""),
                adjust="qfq"
            )
            if df is not None and len(df) > 0:
                df = df.rename(columns={
                    "日期": "trade_date", "开盘": "open", "收盘": "close",
                    "最高": "high", "最低": "low", "成交量": "volume",
                    "成交额": "amount", "涨跌幅": "change_pct",
                    "涨跌额": "change", "换手率": "turnover_ratio",
                })
                df["stock_code"] = symbol
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
                df["trade_time"] = df["trade_date"] + " 00:00:00"
                df["k_type"] = 1
                df["adjust_type"] = 1
                df["short_name"] = ""
                df["pre_close"] = df["close"] - df["change"]
                cols = ["stock_code", "short_name", "trade_time", "trade_date", "k_type", "adjust_type",
                        "open", "close", "high", "low", "volume", "amount", "change", "change_pct",
                        "turnover_ratio", "pre_close"]
                df = df[cols]
                with engine.begin() as conn:
                    conn.execute(text(
                        "DELETE FROM sm_stock_kline WHERE stock_code = :c AND trade_date >= :s AND trade_date <= :e"
                    ), {"c": symbol, "s": START_DATE, "e": END_DATE})
                write_frame(df, "sm_stock_kline", engine, if_exists="append", index=False, method="multi")
                success += 1
            else:
                failed += 1
                failed_codes.append(code)
        except Exception as e:
            failed += 1
            failed_codes.append(code)
            if (i + 1) % 100 == 0:
                print(f"[{i+1}/{len(missing)}] {code} failed: {e}")

        if (i + 1) % 100 == 0:
            print(f"[{i+1}/{len(missing)}] success={success}, failed={failed}")
        time.sleep(0.3)

    print(f"\nDone! success={success}, failed={failed}")
    if failed_codes:
        print(f"Failed codes (first 50): {failed_codes[:50]}")


if __name__ == "__main__":
    main()
