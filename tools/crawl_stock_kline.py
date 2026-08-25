#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股日K线补数据脚本
====================
通过 requests + 自定义适配器（连接到 push2delay IP，Host 头写 push2his）
绕过 push2his 的 IP 封锁。

用法:
  python tools/crawl_stock_kline.py                     # 补最近缺失的数据
  python tools/crawl_stock_kline.py --beg 20260530 --end 20260605
  python tools/crawl_stock_kline.py --limit 10
  python tools/crawl_stock_kline.py --resume            # 断点续爬
"""

import argparse
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
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine, text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine
from server.common.batch_db import replace_table_rows_exact_keys
from server.common.mysql_lock import STOCK_KLINE_FREEZE_LOCK_NAME

PROXY_IP = "61.129.129.48"
TARGET_HOST = "push2his.eastmoney.com"
KLINE_PATH = "/api/qt/stock/kline/get"

DELAY = 1.2
JITTER = 0.5
BATCH_EVERY = 50
BATCH_PAUSE = 30
COOLDOWN_ON_FAIL = 5
COOLDOWN_SECONDS = 120


class ProxyAdapter(HTTPAdapter):
    """强制连接到 proxy_ip，但 HTTP Host 头用 target_host"""
    def __init__(self, proxy_ip, target_host, **kwargs):
        self.proxy_ip = proxy_ip
        self.target_host = target_host
        super().__init__(**kwargs)

    def send(self, request, **kwargs):
        request.url = request.url.replace(
            f"https://{self.target_host}", f"https://{self.proxy_ip}"
        )
        request.headers["Host"] = self.target_host
        return super().send(request, **kwargs)


def make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
    })
    s.mount(f"https://{TARGET_HOST}", ProxyAdapter(PROXY_IP, TARGET_HOST))
    return s


def get_stock_codes(engine) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT stock_code FROM si_all_code ORDER BY stock_code")
        ).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def fetch_kline(session: requests.Session, stock_code: str, beg: str, end: str) -> list[dict] | None:
    cid = 1 if stock_code.startswith("6") else 0
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": "0",
        "secid": f"{cid}.{stock_code}",
        "beg": beg,
        "end": end,
    }
    resp = session.get(
        f"https://{TARGET_HOST}{KLINE_PATH}",
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    klines = (data.get("data") or {}).get("klines")
    if not klines:
        return None

    rows = []
    for line in klines:
        p = line.split(",")
        if len(p) < 11:
            continue
        rows.append({
            "stock_code": stock_code,
            "trade_date": p[0],
            "open": float(p[1]),
            "close": float(p[2]),
            "high": float(p[3]),
            "low": float(p[4]),
            "volume": float(p[5]) * 100,
            "amount": float(p[6]),
            "turnover_ratio": float(p[10]) if p[10] != "-" else None,
        })
    return rows if rows else None


def save_to_db(engine, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_date"], keep="last")

    # 按股票分组计算涨跌
    result = []
    for code in df["stock_code"].unique():
        sub = df[df["stock_code"] == code].sort_values("trade_date").copy()
        sub["pre_close"] = sub["close"].shift(1)
        sub["change"] = sub["close"] - sub["pre_close"]
        sub["change_pct"] = ((sub["change"] / sub["pre_close"]) * 100).round(4)
        for _, r in sub.iterrows():
            td = str(r["trade_date"])
            result.append({
                "stock_code": code, "short_name": "",
                "trade_time": f"{td} 15:00:00", "trade_date": td,
                "k_type": 1, "adjust_type": 0,
                "open": r["open"], "close": r["close"],
                "high": r["high"], "low": r["low"],
                "volume": r["volume"], "amount": r["amount"],
                "change": r["change"] if pd.notna(r["change"]) else None,
                "change_pct": r["change_pct"] if pd.notna(r["change_pct"]) else None,
                "turnover_ratio": r["turnover_ratio"],
                "pre_close": r["pre_close"] if pd.notna(r["pre_close"]) else None,
            })

    out = pd.DataFrame(result).replace({np.nan: None, pd.NaT: None})
    out["etl_sync_at"] = datetime.now().replace(microsecond=0)

    replace_table_rows_exact_keys(
        out,
        "sm_stock_kline",
        engine,
        key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
        lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
        chunksize=500,
    )


def find_missing_dates(engine, lookback_days: int = 10) -> tuple[str, str] | None:
    """
    对比交易日历和已有K线数据，找出缺失的日期范围。
    返回 (beg_yyyymmdd, end_yyyymmdd)，如果没有缺失返回 None。
    """
    with engine.connect() as conn:
        # 交易日历中最近 N 个交易日（只取工作日，排除周末）
        calendar = conn.execute(text(
            "SELECT trade_date FROM si_trade_calendar "
            "WHERE trade_date <= CURDATE() "
            "AND WEEKDAY(trade_date) < 5 "
            "ORDER BY trade_date DESC LIMIT :n"
        ), {"n": lookback_days}).fetchall()
        if not calendar:
            return None

        calendar_dates = {str(r[0]) for r in calendar}
        beg_date = min(calendar_dates)
        end_date = max(calendar_dates)

        # 已有K线数据的日期（取有 5000+ 只股票的日期，说明是完整交易日）
        existing = conn.execute(text(
            "SELECT trade_date, COUNT(DISTINCT stock_code) as cnt "
            "FROM sm_stock_kline "
            "WHERE k_type=1 AND trade_date>=:d0 AND trade_date<=:d1 "
            "GROUP BY trade_date HAVING cnt >= 5000"
        ), {"d0": beg_date, "d1": end_date}).fetchall()
        existing_dates = {str(r[0]) for r in existing}

    missing = sorted(calendar_dates - existing_dates)
    if not missing:
        return None

    # 返回缺失日期的范围（取最小和最大，中间的也会被补上）
    beg_yyyymmdd = missing[0].replace("-", "")
    end_yyyymmdd = missing[-1].replace("-", "")
    return beg_yyyymmdd, end_yyyymmdd


def main():
    parser = argparse.ArgumentParser(description="个股日K线补数据")
    parser.add_argument("--beg", default=None, help="起始日期 YYYYMMDD（默认自动检测缺失）")
    parser.add_argument("--end", default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--lookback", type=int, default=10, help="自动检测回看天数（默认10）")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--resume", action="store_true", help="断点续爬")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    engine = create_tool_engine()
    stock_codes = get_stock_codes(engine)

    # 自动检测缺失日期
    if args.beg and args.end:
        beg, end = args.beg, args.end
    else:
        result = find_missing_dates(engine, args.lookback)
        if result is None:
            print("No missing K-line dates found. All up to date!")
            return
        beg, end = result
        print(f"Auto-detected missing dates: {beg} ~ {end}")

    # 默认开启 resume（跳过已有数据的股票）
    if args.resume or not args.beg:
        beg_date = f"{beg[:4]}-{beg[4:6]}-{beg[6:8]}"
        end_date = f"{end[:4]}-{end[4:6]}-{end[6:8]}"
        with engine.connect() as conn:
            # 找出在目标日期范围内数据完整的股票（已有全部交易日的数据）
            calendar = conn.execute(text(
                "SELECT trade_date FROM si_trade_calendar "
                "WHERE trade_date>=:d0 AND trade_date<=:d1 "
                "AND WEEKDAY(trade_date) < 5 "
                "ORDER BY trade_date"
            ), {"d0": beg_date, "d1": end_date}).fetchall()
            expected_dates = len(calendar)

            # 每只股票已有多少天数据
            done_stocks = conn.execute(text(
                "SELECT stock_code, COUNT(DISTINCT trade_date) as cnt "
                "FROM sm_stock_kline "
                "WHERE k_type=1 AND trade_date>=:d0 AND trade_date<=:d1 "
                "GROUP BY stock_code HAVING cnt >= :n"
            ), {"d0": beg_date, "d1": end_date, "n": expected_dates}).fetchall()
            done = {str(r[0]) for r in done_stocks}

        before = len(stock_codes)
        stock_codes = [c for c in stock_codes if c not in done]
        print(f"Resume: {before} total, {len(done)} complete, {len(stock_codes)} remaining")

    if args.limit > 0:
        stock_codes = stock_codes[: args.limit]

    total = len(stock_codes)
    print(f"\n{'='*60}")
    print(f"  K-line: {beg} ~ {end}")
    print(f"  Stocks: {total}")
    print(f"{'='*60}\n")

    if args.dry_run:
        return

    if args.dry_run:
        return

    session = make_session()
    engine = create_tool_engine()

    buffer = []
    ok = fail = 0
    consecutive_fail = 0
    t0 = time.time()

    for i, code in enumerate(stock_codes):
        rows = None
        for attempt in range(3):
            try:
                rows = fetch_kline(session, code, beg, end)
                if rows:
                    break
            except Exception:
                if attempt < 2:
                    time.sleep(3 * (2 ** attempt))
                else:
                    # 重建 session（可能连接池坏了）
                    session = make_session()

        if rows:
            buffer.extend(rows)
            ok += 1
            consecutive_fail = 0
        else:
            fail += 1
            consecutive_fail += 1

        if consecutive_fail >= COOLDOWN_ON_FAIL:
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
    print(f"\nDone! OK={ok} Fail={fail} Time={elapsed/60:.1f}min", flush=True)


if __name__ == "__main__":
    main()
