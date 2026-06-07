#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股东户数爬取脚本
================
从同花顺 F10 页面解析股东户数数据。

用法:
  python tools/crawl_stock_holder.py
  python tools/crawl_stock_holder.py --limit 10
  python tools/crawl_stock_holder.py --resume
"""

import argparse
import os
import random
import re
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

MYSQL_URL = os.environ.get(
    "MYSQL_URL",
    "mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4",
)

DELAY = 0.8
JITTER = 0.4
BATCH_EVERY = 50
BATCH_PAUSE = 25
CONSECUTIVE_FAIL_COOLDOWN = 5
COOLDOWN_SECONDS = 120


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "http://basic.10jqka.com.cn/",
    })
    s.trust_env = False
    s.verify = False
    return s


def get_stock_codes(engine) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT stock_code FROM si_all_code ORDER BY stock_code")
        ).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def parse_holder_data(html_bytes: bytes, stock_code: str) -> list[dict] | None:
    """
    从同花顺 F10 股东页面 HTML 解析股东户数数据。
    返回最近 8 个报告期的股东户数。
    """
    text = html_bytes.decode("gbk", errors="replace")

    # 提取日期（报告期）
    dates = re.findall(r"(\d{4}-\d{2}-\d{2})", text)
    if not dates:
        return None

    # 提取 td_w 中的所有内容（可能混入日期和数值）
    td_all = re.findall(r'<div class="td_w">([^<]+)</div>', text)
    if not td_all:
        return None

    # 过滤：只保留纯数值（排除日期格式 YYYY-MM-DD）
    td_values = []
    for v in td_all:
        v = v.strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            continue  # 跳过日期
        # 提取数字部分
        num_match = re.match(r"^([\d.]+)", v)
        if num_match:
            td_values.append(num_match.group(1))

    if not td_values:
        return None

    # 去重日期（页面可能有重复）
    unique_dates = []
    seen = set()
    for d in dates:
        if d not in seen:
            unique_dates.append(d)
            seen.add(d)

    # 取前 min(len(unique_dates), len(td_values)) 个
    n = min(len(unique_dates), len(td_values))
    if n == 0:
        return None

    rows = []
    for i in range(n):
        date_str = unique_dates[i]
        try:
            holder_num_raw = float(td_values[i])
        except (ValueError, TypeError):
            continue

        # 单位是万，转为实际人数
        holder_num = int(holder_num_raw * 10000)

        # 计算变化
        pre_holder_num = None
        holder_num_change = None
        holder_num_ratio = None
        if i + 1 < n:
            try:
                pre_raw = float(td_values[i + 1])
                pre_holder_num = int(pre_raw * 10000)
                holder_num_change = holder_num - pre_holder_num
                if pre_holder_num > 0:
                    holder_num_ratio = round(
                        (holder_num_change / pre_holder_num) * 100, 4
                    )
            except (ValueError, TypeError):
                pass

        rows.append({
            "stock_code": stock_code,
            "report_date": date_str,
            "holder_num": holder_num,
            "holder_num_change": holder_num_change,
            "pre_holder_num": pre_holder_num,
            "holder_num_ratio": holder_num_ratio,
            "avg_free_shares": None,  # 需要额外计算
        })

    return rows if rows else None


def save_to_db(engine, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df = df.replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "report_date"], keep="last")
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    # 按 stock_code 删除旧数据
    codes = sorted(df["stock_code"].unique())
    with engine.begin() as conn:
        for c in codes:
            conn.execute(
                text("DELETE FROM si_stock_holder WHERE stock_code = :c"),
                {"c": c},
            )

    df.to_sql(
        "si_stock_holder", engine,
        if_exists="append", index=False,
        chunksize=500, method="multi",
    )


def main():
    parser = argparse.ArgumentParser(description="股东户数爬取")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    stock_codes = get_stock_codes(engine)

    if args.resume:
        with engine.connect() as conn:
            done = set(
                str(r[0]) for r in conn.execute(
                    text("SELECT DISTINCT stock_code FROM si_stock_holder")
                ).fetchall()
            )
        before = len(stock_codes)
        stock_codes = [c for c in stock_codes if c not in done]
        print(f"Resume: {before} total, {len(done)} done, {len(stock_codes)} remaining")

    if args.limit > 0:
        stock_codes = stock_codes[: args.limit]

    total = len(stock_codes)
    print(f"\n{'='*60}")
    print(f"  Stock Holder: {total} stocks")
    print(f"{'='*60}\n")

    if args.dry_run:
        return

    session = make_session()
    buffer = []
    ok = fail = 0
    consecutive_fail = 0
    t0 = time.time()

    for i, code in enumerate(stock_codes):
        rows = None
        for attempt in range(2):
            try:
                resp = session.get(
                    f"http://basic.10jqka.com.cn/{code}/holder.html",
                    timeout=10,
                )
                if resp.status_code == 200 and len(resp.content) > 1000:
                    rows = parse_holder_data(resp.content, code)
                    if rows:
                        break
            except Exception:
                if attempt == 0:
                    time.sleep(2)

        if rows:
            buffer.extend(rows)
            ok += 1
            consecutive_fail = 0
        else:
            fail += 1
            consecutive_fail += 1

        if consecutive_fail >= CONSECUTIVE_FAIL_COOLDOWN:
            cd = COOLDOWN_SECONDS + random.uniform(0, 30)
            print(f"  Cooldown {cd:.0f}s ({consecutive_fail} fails)", flush=True)
            time.sleep(cd)
            session = make_session()
            consecutive_fail = 0

        time.sleep(DELAY + random.uniform(0, JITTER))

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            eta = (total - i - 1) / (i + 1) * elapsed
            print(
                f"  [{i+1}/{total}] OK={ok} Fail={fail} "
                f"Buf={len(buffer)} ETA={eta/60:.0f}min",
                flush=True,
            )

        if (i + 1) % BATCH_EVERY == 0:
            time.sleep(BATCH_PAUSE + random.uniform(0, 5))

        if len(buffer) >= 2000:
            print(f"  Writing {len(buffer)} rows...", flush=True)
            save_to_db(engine, buffer)
            buffer.clear()

    if buffer:
        print(f"  Writing {len(buffer)} rows...", flush=True)
        save_to_db(engine, buffer)

    elapsed = time.time() - t0
    print(f"\n  Done! OK={ok} Fail={fail} Time={elapsed/60:.1f}min\n")


if __name__ == "__main__":
    main()
