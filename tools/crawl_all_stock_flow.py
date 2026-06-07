#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量个股资金流向爬取脚本
========================
通过 push2delay IP + push2his Host 头的方式，绕过 push2his 的 IP 封锁，
获取每只股票近 120 个交易日的资金流向数据。

数据字段:
  - stock_code      股票代码（6位）
  - trade_date      交易日期
  - main_net_inflow 主力净流入（元）
  - sm_net_inflow   小单净流入
  - mid_net_inflow  中单净流入
  - lg_net_inflow   大单净流入
  - max_net_inflow  超大单净流入

用法:
  python tools/crawl_all_stock_flow.py              # 全量爬取
  python tools/crawl_all_stock_flow.py --limit 10   # 测试前10只
  python tools/crawl_all_stock_flow.py --slow        # 慢速模式
  python tools/crawl_all_stock_flow.py --resume      # 断点续爬
  python tools/crawl_all_stock_flow.py --dry-run     # 只看计划不执行

环境变量:
  MYSQL_URL          MySQL 连接串
"""

import argparse
import json
import os
import random
import socket
import ssl
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MYSQL_URL = os.environ.get(
    "MYSQL_URL",
    "mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4",
)

# ── 核心配置：通过 push2delay IP 访问 push2his ──
PROXY_IP = "61.129.129.48"       # push2delay 的 IP
TARGET_HOST = "push2his.eastmoney.com"  # 实际要访问的 Host

# ── 速率控制 ──
DELAY_NORMAL = 0.5
DELAY_SLOW = 3.0
JITTER_NORMAL = 0.3
JITTER_SLOW = 2.0
BATCH_EVERY = 50
BATCH_PAUSE_NORMAL = 25
BATCH_PAUSE_SLOW = 120
CONSECUTIVE_FAIL_COOLDOWN = 8
COOLDOWN_SECONDS = 180
MAX_RETRIES = 2

_SSL_CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def get_stock_codes(engine) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT stock_code FROM si_all_code ORDER BY stock_code")
        ).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def get_stocks_with_flow(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT stock_code FROM sm_stock_capital_flow_daily")
        ).fetchall()
    return {str(r[0]).strip().zfill(6) for r in rows}


def fetch_one_stock(stock_code: str) -> list[dict] | None:
    """通过 push2delay IP + push2his Host 头获取单只股票历史资金流向"""
    cid = 1 if stock_code.startswith("6") else 0
    path = (
        "/api/qt/stock/fflow/daykline/get?"
        "lmt=0&klt=101&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&secid={cid}.{stock_code}"
    )

    sock = None
    ssock = None
    try:
        sock = socket.create_connection((PROXY_IP, 443), timeout=15)
        ssock = _SSL_CTX.wrap_socket(sock, server_hostname=TARGET_HOST)

        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {TARGET_HOST}\r\n"
            f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36\r\n"
            f"Accept: */*\r\n"
            f"Accept-Language: zh-CN,zh;q=0.9\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        ssock.sendall(request.encode())

        response = b""
        while True:
            try:
                chunk = ssock.recv(65536)
                if not chunk:
                    break
                response += chunk
            except Exception:
                break

        if not response:
            return None

        header_end = response.find(b"\r\n\r\n")
        if header_end < 0:
            return None
        body = response[header_end + 4:]
        if not body:
            return None

        data = json.loads(body)
        klines = (data.get("data") or {}).get("klines")
        if not klines:
            return None

        rows = []
        for line in klines:
            parts = line.split(",")
            if len(parts) < 6:
                continue
            rows.append({
                "stock_code": stock_code,
                "trade_date": parts[0],
                "main_net_inflow": float(parts[1]),
                "sm_net_inflow": float(parts[2]),
                "mid_net_inflow": float(parts[3]),
                "lg_net_inflow": float(parts[4]),
                "max_net_inflow": float(parts[5]),
            })
        return rows if rows else None

    except Exception:
        return None
    finally:
        if ssock:
            try:
                ssock.close()
            except Exception:
                pass
        if sock:
            try:
                sock.close()
            except Exception:
                pass


def save_to_db(engine, rows: list[dict]):
    """批量写入数据库（按 stock_code + trade_date 去重，不误删其他股票）"""
    if not rows:
        return

    df = pd.DataFrame(rows)
    df = df.replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_date"], keep="last")

    # 按股票代码分组删除已有数据（避免误删其他股票）
    codes = sorted(df["stock_code"].unique())
    dates = sorted(df["trade_date"].unique())
    with engine.begin() as conn:
        for code in codes:
            conn.execute(
                text(
                    "DELETE FROM `sm_stock_capital_flow_daily` "
                    "WHERE `stock_code` = :c AND `trade_date` IN :dates"
                ),
                {"c": code, "dates": tuple(dates)},
            )

    df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    chunk_size = 2000
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        chunk.to_sql(
            "sm_stock_capital_flow_daily",
            engine,
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi",
        )


def main():
    parser = argparse.ArgumentParser(description="全量个股资金流向爬取")
    parser.add_argument("--limit", type=int, default=0, help="限制股票数量（0=全部）")
    parser.add_argument("--slow", action="store_true", help="慢速模式")
    parser.add_argument("--resume", action="store_true", help="断点续爬")
    parser.add_argument("--dry-run", action="store_true", help="只显示计划")
    args = parser.parse_args()

    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    stock_codes = get_stock_codes(engine)

    if args.resume:
        existing = get_stocks_with_flow(engine)
        before = len(stock_codes)
        stock_codes = [c for c in stock_codes if c not in existing]
        print(f"断点续爬: 总 {before} 只, 已有 {len(existing)} 只, 待爬 {len(stock_codes)} 只")

    if args.limit > 0:
        stock_codes = stock_codes[: args.limit]

    total = len(stock_codes)
    delay = DELAY_SLOW if args.slow else DELAY_NORMAL
    jitter = JITTER_SLOW if args.slow else JITTER_NORMAL
    batch_pause = BATCH_PAUSE_SLOW if args.slow else BATCH_PAUSE_NORMAL

    print(f"\n{'='*60}")
    print(f"  全量个股资金流向爬取")
    print(f"  数据源: push2his (via push2delay IP)")
    print(f"  待爬股票: {total} 只")
    print(f"  模式: {'慢速' if args.slow else '标准'}")
    print(f"  请求间隔: {delay}s +/- {jitter}s")
    print(f"{'='*60}\n")

    if args.dry_run:
        print("  --dry-run 模式，不执行")
        return

    # ── 主循环 ──
    buffer: list[dict] = []
    success = 0
    failed = 0
    no_data = 0
    total_rows = 0
    consecutive_fail = 0
    t_start = time.time()
    FLUSH_EVERY = 200

    for i, code in enumerate(stock_codes):
        rows = None
        last_err = None

        for attempt in range(1 + MAX_RETRIES):
            try:
                rows = fetch_one_stock(code)
                if rows is not None:
                    break
            except Exception as e:
                last_err = e
                if attempt < MAX_RETRIES:
                    time.sleep(2 * (2 ** attempt) + random.uniform(0, 1))

        if rows:
            buffer.extend(rows)
            success += 1
            total_rows += len(rows)
            consecutive_fail = 0
        elif last_err is not None:
            failed += 1
            consecutive_fail += 1
            if failed <= 20:
                print(f"  X {code}: {last_err}")
        else:
            no_data += 1
            consecutive_fail = 0

        # 连续失败冷却
        if consecutive_fail >= CONSECUTIVE_FAIL_COOLDOWN:
            cd = COOLDOWN_SECONDS + random.uniform(0, 30)
            print(f"\n  !! 连续 {consecutive_fail} 次失败，冷却 {cd:.0f}s ...\n")
            time.sleep(cd)
            consecutive_fail = 0

        # 请求间隔
        time.sleep(delay + random.uniform(0, jitter))

        # 进度
        if (i + 1) % 100 == 0:
            elapsed = time.time() - t_start
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate if rate > 0 else 0
            print(
                f"  [{i+1}/{total}] OK={success} Fail={failed} "
                f"NoData={no_data} Rows={total_rows} "
                f"Used {elapsed/60:.1f}min ETA {eta/60:.1f}min"
            )

        # 批次暂停
        if (i + 1) % BATCH_EVERY == 0:
            pause = batch_pause + random.uniform(0, 10)
            print(f"  Pause {pause:.0f}s (done {i+1})")
            time.sleep(pause)

        # 定期写库
        if len(buffer) >= FLUSH_EVERY * 150:
            print(f"  Writing {len(buffer)} rows to DB...")
            save_to_db(engine, buffer)
            buffer.clear()

    # 写入剩余数据
    if buffer:
        print(f"  Writing {len(buffer)} rows to DB...")
        save_to_db(engine, buffer)

    # ── 统计 ──
    elapsed_total = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"  Done!")
    print(f"  Total: {total}")
    if total:
        print(f"  Success: {success} ({success/total*100:.1f}%)")
    print(f"  Failed: {failed}")
    print(f"  No data: {no_data}")
    print(f"  Total rows: {total_rows}")
    print(f"  Time: {elapsed_total/60:.1f} min")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
