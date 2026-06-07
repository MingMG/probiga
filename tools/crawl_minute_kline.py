#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分钟数据爬取脚本
================
从 push2delay 获取个股/指数/概念的当日分钟K线和分钟资金流向。

用法:
  python tools/crawl_minute_kline.py --type stock    # 个股分钟K线
  python tools/crawl_minute_kline.py --type index    # 指数分钟K线
  python tools/crawl_minute_kline.py --type concept  # 东财概念分钟K线
  python tools/crawl_minute_kline.py --type flow     # 分钟资金流向
  python tools/crawl_minute_kline.py --type all      # 全部
  python tools/crawl_minute_kline.py --type stock --limit 10
"""

import argparse
import json
import os
import random
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
    """从环境变量或 .env 文件加载 MySQL 连接串"""
    url = os.environ.get("MYSQL_URL")
    if url:
        return url
    # 尝试从 .env 文件读取
    for env_path in [ROOT / ".env", Path("/opt/ProBigA/.env")]:
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("MYSQL_URL="):
                    return line.split("=", 1)[1].strip()
    return "mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4"


MYSQL_URL = _load_mysql_url()

DELAY = 0.5
JITTER = 0.3
BATCH_EVERY = 100
BATCH_PAUSE = 20

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://quote.eastmoney.com/",
})
SESSION.trust_env = False
SESSION.verify = False


def safe_float(val) -> float:
    if val is None or val == "-" or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def fetch_minute_kline(code: str, market: int) -> list[str] | None:
    """获取分钟K线，自动尝试两个 market 值"""
    url = "https://push2delay.eastmoney.com/api/qt/stock/kline/get"
    markets = [market, 1 - market]  # 先试指定的，再试另一个
    for m in markets:
        params = {
            "secid": f"{m}.{code}",
            "klt": "1", "fqt": "1",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "lmt": "300", "end": "20500101",
        }
        try:
            resp = SESSION.get(url, params=params, timeout=10)
            data = resp.json()
            klines = (data.get("data") or {}).get("klines")
            if klines:
                return klines
        except Exception:
            pass
    return None


def fetch_minute_flow(code: str, market: int) -> list[str] | None:
    """获取分钟资金流向"""
    url = "https://push2delay.eastmoney.com/api/qt/stock/fflow/kline/get"
    params = {
        "secid": f"{market}.{code}",
        "klt": "1",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "lmt": "300",
    }
    try:
        resp = SESSION.get(url, params=params, timeout=10)
        data = resp.json()
        return (data.get("data") or {}).get("klines")
    except Exception:
        return None


def parse_kline(code: str, klines: list[str]) -> list[dict]:
    """
    解析分钟K线 → sm_stock_minute / sm_index_minute / sm_concept_east_minute
    API: datetime,open,close,high,low,volume,amount,amplitude,change_pct,change,turnover
    表: stock_code, trade_time, trade_date, price, avg_price, change, change_pct, volume, amount
    """
    rows = []
    for line in klines:
        p = line.split(",")
        if len(p) < 11:
            continue
        dt = p[0]  # "2026-06-05 09:31"
        rows.append({
            "stock_code": code,
            "trade_time": dt,
            "trade_date": dt[:10],
            "price": safe_float(p[2]),      # close
            "avg_price": None,               # API 不提供均价
            "change": safe_float(p[9]),      # 涨跌额
            "change_pct": safe_float(p[8]),  # 涨跌幅
            "volume": safe_float(p[5]) * 100,
            "amount": safe_float(p[6]),
        })
    return rows


def parse_flow(code: str, klines: list[str]) -> list[dict]:
    """
    解析分钟资金流向 → sm_stock_capital_flow_min
    API: datetime,main,sm,mid,lg,max
    表: stock_code, trade_time, main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow
    """
    rows = []
    for line in klines:
        p = line.split(",")
        if len(p) < 6:
            continue
        dt = p[0]
        rows.append({
            "stock_code": code,
            "trade_time": dt,
            "main_net_inflow": safe_float(p[1]),
            "max_net_inflow": safe_float(p[5]),
            "lg_net_inflow": safe_float(p[4]),
            "mid_net_inflow": safe_float(p[3]),
            "sm_net_inflow": safe_float(p[2]),
        })
    return rows


def get_codes(engine, table: str, code_col: str) -> list[tuple[str, int]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT {code_col} FROM {table} ORDER BY {code_col}")
        ).fetchall()
    result = []
    for r in rows:
        code = str(r[0]).strip().zfill(6)
        if code.startswith("6"):
            market = 1
        elif code.startswith(("0", "3")):
            market = 0
        else:
            market = 0
        result.append((code, market))
    return result


def save_kline(engine, rows: list[dict], table: str):
    if not rows:
        return
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")

    dates = sorted(df["trade_date"].unique())
    code_col = "index_code" if table in ("sm_index_minute", "sm_concept_east_minute") else "stock_code"
    with engine.begin() as conn:
        for d in dates:
            conn.execute(text(f"DELETE FROM {table} WHERE trade_date = :d"), {"d": d})

    df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    # sm_index_minute / sm_concept_east_minute 用 index_code 列，有 snapshot_at
    if table in ("sm_index_minute", "sm_concept_east_minute"):
        df = df.rename(columns={"stock_code": "index_code"})
        df["snapshot_at"] = datetime.now().replace(microsecond=0)

    df.to_sql(table, engine, if_exists="append", index=False, chunksize=1000, method="multi")


def save_flow(engine, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")

    # 按 stock_code 删除旧数据（表没有 trade_date 列）
    codes = sorted(df["stock_code"].unique())
    with engine.begin() as conn:
        for c in codes:
            conn.execute(text("DELETE FROM sm_stock_capital_flow_min WHERE stock_code = :c"), {"c": c})

    df["snapshot_at"] = datetime.now().replace(microsecond=0)
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    df.to_sql("sm_stock_capital_flow_min", engine, if_exists="append",
              index=False, chunksize=1000, method="multi")


def crawl_kline(engine, codes: list[tuple[str, int]], table: str, label: str, limit: int):
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    print(f"\n  {label}: {total} items", flush=True)

    buffer = []
    ok = fail = 0
    t0 = time.time()

    for i, (code, market) in enumerate(codes):
        klines = fetch_minute_kline(code, market)
        if klines:
            buffer.extend(parse_kline(code, klines))
            ok += 1
        else:
            fail += 1

        time.sleep(DELAY + random.uniform(0, JITTER))

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            eta = (total - i - 1) / (i + 1) * elapsed
            print(f"    [{i+1}/{total}] OK={ok} Fail={fail} Buf={len(buffer)} ETA={eta/60:.0f}min", flush=True)

        if (i + 1) % BATCH_EVERY == 0:
            time.sleep(BATCH_PAUSE + random.uniform(0, 5))

        if len(buffer) >= 5000:
            print(f"    Writing {len(buffer)} rows...", flush=True)
            save_kline(engine, buffer, table)
            buffer.clear()

    if buffer:
        print(f"    Writing {len(buffer)} rows...", flush=True)
        save_kline(engine, buffer, table)

    elapsed = time.time() - t0
    print(f"    Done! OK={ok} Fail={fail} Time={elapsed/60:.1f}min", flush=True)


def crawl_flow(engine, codes: list[tuple[str, int]], limit: int):
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    print(f"\n  Minute flow: {total} stocks", flush=True)

    buffer = []
    ok = fail = 0
    t0 = time.time()

    for i, (code, market) in enumerate(codes):
        klines = fetch_minute_flow(code, market)
        if klines:
            buffer.extend(parse_flow(code, klines))
            ok += 1
        else:
            fail += 1

        time.sleep(DELAY + random.uniform(0, JITTER))

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            eta = (total - i - 1) / (i + 1) * elapsed
            print(f"    [{i+1}/{total}] OK={ok} Fail={fail} Buf={len(buffer)} ETA={eta/60:.0f}min", flush=True)

        if (i + 1) % BATCH_EVERY == 0:
            time.sleep(BATCH_PAUSE + random.uniform(0, 5))

        if len(buffer) >= 5000:
            print(f"    Writing {len(buffer)} rows...", flush=True)
            save_flow(engine, buffer)
            buffer.clear()

    if buffer:
        print(f"    Writing {len(buffer)} rows...", flush=True)
        save_flow(engine, buffer)

    elapsed = time.time() - t0
    print(f"    Done! OK={ok} Fail={fail} Time={elapsed/60:.1f}min", flush=True)


def main():
    parser = argparse.ArgumentParser(description="分钟数据爬取")
    parser.add_argument("--type", required=True,
                        choices=["stock", "index", "concept", "flow", "all"])
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    engine = create_engine(MYSQL_URL, pool_pre_ping=True)

    print(f"\n{'='*60}")
    print(f"  Minute data: {args.type}")
    print(f"{'='*60}")

    if args.type in ("stock", "all"):
        codes = get_codes(engine, "si_all_code", "stock_code")
        crawl_kline(engine, codes, "sm_stock_minute", "Stock 1-min", args.limit)

    if args.type in ("index", "all"):
        codes = get_codes(engine, "si_all_index_code", "index_code")
        crawl_kline(engine, codes, "sm_index_minute", "Index 1-min", args.limit)

    if args.type in ("concept", "all"):
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT index_code FROM si_concept_code_east ORDER BY index_code")).fetchall()
        codes = [(str(r[0]), 90) for r in rows]
        crawl_kline(engine, codes, "sm_concept_east_minute", "Concept 1-min", args.limit)

    if args.type in ("flow", "all"):
        codes = get_codes(engine, "si_all_code", "stock_code")
        crawl_flow(engine, codes, args.limit)

    print(f"\n{'='*60}")
    print(f"  All done!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
