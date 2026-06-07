#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘中批量数据刷新
================
一次运行刷新：行情快照、资金流向、概念行情、指数行情。
用 push2delay 批量接口，全市场一次拿完。

用法:
  python tools/crawl_realtime_batch.py           # 刷新全部
  python tools/crawl_realtime_batch.py --only snapshot
  python tools/crawl_realtime_batch.py --only flow
  python tools/crawl_realtime_batch.py --only concept
  python tools/crawl_realtime_batch.py --only index
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
from sqlalchemy import create_engine, text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_mysql_url() -> str:
    url = os.environ.get("MYSQL_URL")
    if url:
        return url
    for env_path in [ROOT / ".env", Path("/opt/ProBigA/.env")]:
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("MYSQL_URL="):
                    return line.split("=", 1)[1].strip()
    return "mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4"


MYSQL_URL = _load_mysql_url()

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://data.eastmoney.com/",
})
SESSION.trust_env = False
SESSION.verify = False

BATCH_API = "https://push2delay.eastmoney.com/api/qt/clist/get"


def safe_float(val) -> float:
    if val is None or val == "-" or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def fetch_batch(fs: str, fields: str, page_size: int = 100) -> list[dict]:
    """分页获取批量数据"""
    all_items = []
    for pn in range(1, 200):
        params = {
            "fid": "f3", "po": "1",
            "pz": str(page_size), "pn": str(pn), "np": "1",
            "fltt": "2", "invt": "2",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fs": fs,
            "fields": fields,
        }
        for attempt in range(2):
            try:
                resp = SESSION.get(BATCH_API, params=params, timeout=15)
                data = resp.json()
                diff = (data.get("data") or {}).get("diff")
                if diff is not None:
                    break
            except Exception:
                if attempt == 0:
                    time.sleep(1)
                else:
                    diff = None
        if not diff:
            break
        all_items.extend(diff)
        if len(diff) < page_size:
            break
        time.sleep(0.1)
    return all_items


def refresh_snapshot(engine) -> int:
    """刷新个股行情快照 sm_stock_current"""
    items = fetch_batch(
        "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,"
        "m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18"
    )
    if not items:
        return 0

    now = datetime.now().replace(microsecond=0)
    rows = []
    for item in items:
        code = str(item.get("f12", "")).zfill(6)
        if not code or code == "000000":
            continue
        rows.append({
            "stock_code": code,
            "short_name": str(item.get("f14", "")),
            "price": safe_float(item.get("f2")),
            "change": safe_float(item.get("f4")),
            "change_pct": safe_float(item.get("f3")),
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "snapshot_at": now,
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code"], keep="last")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE sm_stock_current"))

    df["etl_sync_at"] = now
    df.to_sql("sm_stock_current", engine, if_exists="append",
              index=False, chunksize=1000, method="multi")
    return len(df)


def refresh_flow(engine) -> int:
    """刷新资金流向 sm_stock_capital_flow_daily（今天的数据）"""
    items = fetch_batch(
        "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,"
        "m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
        "f12,f14,f62,f66,f72,f78,f84"
    )
    if not items:
        return 0

    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().replace(microsecond=0)
    rows = []
    for item in items:
        code = str(item.get("f12", "")).zfill(6)
        if not code or code == "000000":
            continue
        rows.append({
            "stock_code": code,
            "trade_date": today,
            "main_net_inflow": safe_float(item.get("f62")),
            "sm_net_inflow": safe_float(item.get("f84")),
            "mid_net_inflow": safe_float(item.get("f78")),
            "lg_net_inflow": safe_float(item.get("f72")),
            "max_net_inflow": safe_float(item.get("f66")),
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code"], keep="last")

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM sm_stock_capital_flow_daily WHERE trade_date = :d"),
            {"d": today},
        )

    df["etl_sync_at"] = now
    df.to_sql("sm_stock_capital_flow_daily", engine, if_exists="append",
              index=False, chunksize=1000, method="multi")
    return len(df)


def refresh_concept_east(engine) -> int:
    """刷新东财概念行情 sm_concept_east_current"""
    items = fetch_batch(
        "m:90+t:3",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17"
    )
    if not items:
        return 0

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in items:
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
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE sm_concept_east_current"))

    df["etl_sync_at"] = now
    df.to_sql("sm_concept_east_current", engine, if_exists="append",
              index=False, chunksize=500, method="multi")
    return len(df)


def refresh_index(engine) -> int:
    """刷新指数行情 sm_index_current"""
    # 指数: 上证 m:1+t:2, 深证 m:0+t:2, 创业板 m:0+t:23, 科创 m:1+t:23
    items = fetch_batch(
        "m:1+t:2+f:!2,m:0+t:2+f:!2,m:1+t:23+f:!2,m:0+t:23+f:!2",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18"
    )
    if not items:
        return 0

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in items:
        code = str(item.get("f12", "")).zfill(6)
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
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE sm_index_current"))

    df["etl_sync_at"] = now
    df.to_sql("sm_index_current", engine, if_exists="append",
              index=False, chunksize=500, method="multi")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="盘中批量数据刷新")
    parser.add_argument("--only", choices=["snapshot", "flow", "concept", "index", "all"],
                        default="all")
    args = parser.parse_args()

    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    t0 = time.time()
    results = {}

    if args.only in ("snapshot", "all"):
        n = refresh_snapshot(engine)
        results["snapshot"] = n
        print(f"  snapshot: {n} stocks", flush=True)

    if args.only in ("flow", "all"):
        n = refresh_flow(engine)
        results["flow"] = n
        print(f"  flow: {n} stocks", flush=True)

    if args.only in ("concept", "all"):
        n = refresh_concept_east(engine)
        results["concept"] = n
        print(f"  concept_east: {n}", flush=True)

    if args.only in ("index", "all"):
        n = refresh_index(engine)
        results["index"] = n
        print(f"  index: {n}", flush=True)

    elapsed = time.time() - t0
    print(f"  Done in {elapsed:.1f}s", flush=True)
    return results


if __name__ == "__main__":
    main()
