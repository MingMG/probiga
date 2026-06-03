#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""获取新浪热股榜（按关注热度），写入 st_hot_rank_sina。"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


def _run_ddl(engine):
    sql_file = ROOT / "tools" / "02_hot_rank_sina.sql"
    if not sql_file.exists():
        print("  DDL文件 02_hot_rank_sina.sql 未找到，跳过")
        return
    with engine.begin() as conn:
        for stmt in sql_file.read_text(encoding="utf-8").split(";"):
            s = stmt.strip()
            if s and not s.startswith("--"):
                try:
                    conn.execute(text(s))
                except Exception as e:
                    if "already exists" not in str(e):
                        print(f"  DDL警告: {e}")
    print("  已确保 st_hot_rank_sina 表存在")


def fetch_hot_rank_sina(snapshot_date: str, top: int = 100):
    import requests
    import time as _time

    print(f"开始获取新浪热股榜，快照日期: {snapshot_date}，top={top}")

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    _run_ddl(engine)

    url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.sina.com.cn/",
    }

    all_items = []
    page_size = 200
    max_pages = 5
    for page in range(1, max_pages + 1):
        params = {"page": page, "num": page_size, "sort": "attention", "asc": 0, "node": "hs_a"}
        data = None
        for attempt in range(1, 4):
            try:
                r = requests.get(url, params=params, headers=headers, timeout=15)
                r.raise_for_status()
                data = r.json()
                if data:
                    break
            except Exception as e:
                print(f"  第{page}页第{attempt}次请求失败: {e}")
            if attempt < 3:
                _time.sleep(attempt * 3)
        if not data:
            break
        for item in data:
            code = str(item.get("code", "")).zfill(6)
            sym = str(item.get("symbol", ""))
            if sym[:2] in ("sh", "sz") and code[0] in ("0", "3", "6"):
                all_items.append(item)
        if len(all_items) >= top:
            break
        _time.sleep(0.3)

    if not all_items:
        print("未获取到新浪热股榜数据")
        return

    rows = []
    for i, item in enumerate(all_items[:top], 1):
        rows.append({
            "rank": i,
            "stock_code": str(item.get("code", "")).zfill(6),
            "short_name": item.get("name", ""),
            "price": float(item.get("trade", 0)) if item.get("trade") else None,
            "price_change": float(item.get("pricechange", 0)) if item.get("pricechange") else None,
            "change_pct": float(item.get("changepercent", 0)) if item.get("changepercent") else None,
            "amount": float(item.get("amount", 0)) if item.get("amount") else None,
            "volume": float(item.get("volume", 0)) if item.get("volume") else None,
            "market_capital": float(item.get("mktcap", 0)) if item.get("mktcap") else None,
            "turnover_ratio": float(item.get("turnoverratio", 0)) if item.get("turnoverratio") else None,
        })

    df = pd.DataFrame(rows)
    df["snapshot_date"] = snapshot_date
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    df = df.replace({np.nan: None, pd.NaT: None})

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM st_hot_rank_sina WHERE snapshot_date = :d"), {"d": snapshot_date})
    df.to_sql("st_hot_rank_sina", engine, if_exists="append", index=False, chunksize=500, method="multi")

    print(f"写入完成: st_hot_rank_sina, 共 {len(df)} 行")
    print(f"  TOP5: {', '.join(df.head(5)['short_name'].tolist())}")


def main():
    parser = argparse.ArgumentParser(description="新浪热股榜同步")
    parser.add_argument("snapshot_date", nargs="?", default=datetime.now().strftime("%Y-%m-%d"), help="快照日期 YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=100, help="排行数量")
    args = parser.parse_args()

    fetch_hot_rank_sina(args.snapshot_date, args.top)


if __name__ == "__main__":
    main()
