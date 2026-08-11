#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东财概念板块实时行情爬取
========================
从 push2delay 批量接口获取全市场东财概念板块行情数据。

用法:
  python tools/crawl_concept_east_current.py
  python tools/crawl_concept_east_current.py --dry-run
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3
from sqlalchemy import text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.batch_db import replace_table_rows, write_frame

BATCH_API = "https://push2delay.eastmoney.com/api/qt/clist/get"
PAGE_SIZE = 100
FILTER = "m:90+t:3"  # 东财概念板块
FIELDS = "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17"


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
        "Referer": "https://data.eastmoney.com/",
    })
    s.trust_env = False
    s.verify = False
    return s


def safe_float(val) -> float:
    if val is None or val == "-" or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def fetch_all(session: requests.Session) -> pd.DataFrame:
    """分页获取全市场概念板块行情"""
    all_items = []
    for pn in range(1, 20):
        params = {
            "fid": "f3", "po": "1",
            "pz": str(PAGE_SIZE), "pn": str(pn), "np": "1",
            "fltt": "2", "invt": "2",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fs": FILTER,
            "fields": FIELDS,
        }
        for attempt in range(3):
            try:
                resp = session.get(BATCH_API, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                diff = (data.get("data") or {}).get("diff")
                if diff is not None:
                    break
            except Exception:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    diff = None

        if not diff:
            break
        all_items.extend(diff)
        if len(diff) < PAGE_SIZE:
            break
        time.sleep(0.2)

    if not all_items:
        return pd.DataFrame()

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in all_items:
        code = item.get("f12", "")
        if not code:
            continue
        rows.append({
            "index_code": code,
            "trade_time": now,
            "trade_date": today,
            "open": safe_float(item.get("f17")),
            "price": safe_float(item.get("f2")),
            "high": safe_float(item.get("f15")),
            "low": safe_float(item.get("f16")),
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "change": safe_float(item.get("f4")),
            "change_pct": safe_float(item.get("f3")),
            "snapshot_at": now,
            "etl_sync_at": now,
        })

    return pd.DataFrame(rows)


def save_to_db(engine, df: pd.DataFrame):
    min_rows = int(os.environ.get("CONCEPT_EAST_MIN_ROWS", "100"))
    if df.empty or len(df) < min_rows:
        raise RuntimeError(f"concept east current returned too few rows: {len(df)} < {min_rows}")
    df = df.replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    replace_table_rows(
        df,
        "sm_concept_east_current",
        engine,
        chunksize=500,
    )


def main():
    parser = argparse.ArgumentParser(description="东财概念板块实时行情")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("date", nargs="?", default=None, help=argparse.SUPPRESS)  # 兼容调度器传入日期
    args = parser.parse_args()

    engine = create_tool_engine(resolve_tool_mysql_url())
    session = make_session()

    print(f"Fetching concept east current...", flush=True)
    df = fetch_all(session)

    if df.empty:
        raise RuntimeError("concept east current returned no rows")

    min_rows = int(os.environ.get("CONCEPT_EAST_MIN_ROWS", "100"))
    if len(df) < min_rows:
        raise RuntimeError(f"concept east current returned too few rows: {len(df)} < {min_rows}")

    print(f"Got {len(df)} concepts", flush=True)

    # 统计
    positive = (df["change_pct"] > 0).sum()
    negative = (df["change_pct"] < 0).sum()
    print(f"  Up: {positive}, Down: {negative}")

    top5 = df.nlargest(5, "change_pct")
    print(f"  Top 5:")
    for _, r in top5.iterrows():
        print(f"    {r['index_code']}: {r['change_pct']:+.2f}%")

    if args.dry_run:
        print("  --dry-run, not saving")
        return

    save_to_db(engine, df)
    print(f"  Saved {len(df)} rows to sm_concept_east_current")


if __name__ == "__main__":
    main()
