#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
慢速补同步个股资金流向，只用东财 push2his 数据源。
每只股票间隔 0.8~1.2s，每 30 只暂停 40~60s，每天之间休息 5 分钟。
"""

import http.client
import json
import os
import random
import ssl
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from tools.env_config import create_tool_engine, resolve_tool_mysql_url

# ── 慢速参数（东财限流极严，必须非常慢）──
REQUEST_DELAY = 3.0       # 基础间隔 3 秒
REQUEST_JITTER = 2.0      # 随机抖动 0~2s（总间隔 3~5s）
BATCH_EVERY = 10          # 每 10 只暂停
BATCH_PAUSE = 120         # 暂停 120~150s
INTER_DAY_PAUSE = 300     # 每天之间休息 5 分钟
MAX_RETRIES = 1           # 失败不重试
RETRY_DELAY = 5
COOLDOWN_ON_FAIL = 3      # 连续失败 3 次就冷却
COOLDOWN_WAIT = 180       # 冷却 180 秒

_SSL_CTX = ssl.create_default_context()
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}


def _mysql_url() -> str:
    return resolve_tool_mysql_url()


def _read_stock_codes(engine):
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code FROM si_all_code ORDER BY stock_code")).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def _fetch_push2his(stock_code: str, target_date: str) -> dict | None:
    """从东财 push2his 获取单只股票某日资金流向（用 curl 子进程绕过 Python TLS 问题）"""
    import subprocess
    cid = 1 if stock_code.startswith('6') else 0
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        "lmt=0&klt=101&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&secid={cid}.{stock_code}"
    )
    proc = subprocess.Popen(
        ["curl", "-s", "--max-time", "15", "-H", "User-Agent: Mozilla/5.0", url],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    stdout, _ = proc.communicate()
    if proc.returncode != 0 or not stdout:
        raise ConnectionError("curl 请求失败")
    data = json.loads(stdout.decode("utf-8"))
    if not data.get("data") or not data["data"].get("klines"):
        return None
    for line in data["data"]["klines"]:
        if target_date[:10] not in line:
            continue
        parts = line.split(",")
        if len(parts) < 6:
            continue
        return {
            "stock_code": stock_code,
            "trade_date": parts[0],
            "main_net_inflow": float(parts[1]),
            "sm_net_inflow": float(parts[2]),
            "mid_net_inflow": float(parts[3]),
            "lg_net_inflow": float(parts[4]),
            "max_net_inflow": float(parts[5]),
        }
    return None


def _backfill_one_day(engine, target_date: str, stock_codes: list[str]):
    """补同步一天的资金流向数据"""
    print(f"\n{'='*60}")
    print(f"  补同步: {target_date}")
    print(f"{'='*60}")

    # 检查当天已有多少数据
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily WHERE trade_date = :d"),
            {"d": target_date}
        ).scalar()
    if existing >= 5000:
        print(f"  当天已有 {existing} 条，跳过")
        return

    # 只补当天有K线（有交易）但没有资金流向数据的股票
    with engine.connect() as conn:
        traded = set(str(r[0]) for r in conn.execute(
            text("SELECT stock_code FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1"),
            {"d": target_date}
        ).fetchall())
        has_flow = set(str(r[0]) for r in conn.execute(
            text("SELECT stock_code FROM sm_stock_capital_flow_daily WHERE trade_date = :d"),
            {"d": target_date}
        ).fetchall())
    need补 = sorted(traded - has_flow)
    print(f"  当天有交易 {len(traded)} 只，已有资金流 {len(has_flow)} 只，需补 {len(need补)} 只")
    if not need补:
        print(f"  无需补数据")
        return
    stock_codes = need补

    parts = []
    success = 0
    failed = 0
    no_data = 0
    consecutive_fail = 0
    t_start = time.time()

    for i, code in enumerate(stock_codes):
        row = None
        last_err = None
        for attempt in range(1 + MAX_RETRIES):
            try:
                row = _fetch_push2his(code, target_date)
                if row is not None:
                    break
            except Exception as e:
                last_err = e
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * (2 ** attempt) + random.uniform(0, 2))

        if row is not None:
            parts.append(row)
            success += 1
            consecutive_fail = 0
        elif last_err is not None:
            failed += 1
            consecutive_fail += 1
            if failed <= 5:
                print(f"    {code} 失败: {last_err}")
        else:
            no_data += 1
            consecutive_fail = 0

        # 连续失败冷却（IP被封了，需要等）
        if consecutive_fail >= COOLDOWN_ON_FAIL:
            cooldown = COOLDOWN_WAIT + random.uniform(0, 30)
            print(f"\n    [!] 连续 {consecutive_fail} 次失败（IP可能被封），冷却 {cooldown:.0f}s ...\n")
            time.sleep(cooldown)
            consecutive_fail = 0

        # 每只间隔
        time.sleep(REQUEST_DELAY + random.uniform(0, REQUEST_JITTER))

        # 每 50 只报告进度
        if (i + 1) % 50 == 0:
            elapsed = time.time() - t_start
            rate = (i + 1) / elapsed
            eta = (len(stock_codes) - i - 1) / rate if rate > 0 else 0
            print(f"    进度: {i+1}/{len(stock_codes)}  成功={success}  失败={failed}  无数据={no_data}  "
                  f"已用 {elapsed:.0f}s  预计还需 {eta:.0f}s")

        # 批次暂停
        if (i + 1) % BATCH_EVERY == 0:
            pause = BATCH_PAUSE + random.uniform(0, 20)
            print(f"    批次暂停 {pause:.0f}s（已处理 {i+1} 只）...")
            time.sleep(pause)

    # 写入数据库
    if not parts:
        print(f"  {target_date}: 未获取到数据")
        return

    df = pd.DataFrame(parts)
    df = df.replace({np.nan: None})
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM sm_stock_capital_flow_daily WHERE trade_date = :d"), {"d": target_date})

    df.to_sql("sm_stock_capital_flow_daily", engine, if_exists="append", index=False,
              chunksize=500, method="multi")

    elapsed = time.time() - t_start
    print(f"  ✅ {target_date} 写入 {len(df)} 条  成功={success}  失败={failed}  无数据={no_data}  耗时 {elapsed:.0f}s ({elapsed/60:.1f}min)")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="慢速补同步个股资金流向（东财push2his）")
    parser.add_argument("--start", default="2026-05-06", help="起始日期")
    parser.add_argument("--end", default="2026-05-29", help="结束日期")
    parser.add_argument("--dry-run", action="store_true", help="只列出待补日期，不实际执行")
    args = parser.parse_args()

    engine = create_tool_engine(_mysql_url())
    stock_codes = _read_stock_codes(engine)

    # 找出需要补的日期
    kline = pd.read_sql(text(f"""
        SELECT trade_date FROM sm_stock_kline
        WHERE k_type = 1 AND trade_date >= '{args.start}' AND trade_date <= '{args.end}'
        GROUP BY trade_date ORDER BY trade_date
    """), engine)

    flow = pd.read_sql(text(f"""
        SELECT trade_date, COUNT(DISTINCT stock_code) as cnt
        FROM sm_stock_capital_flow_daily
        WHERE trade_date >= '{args.start}' AND trade_date <= '{args.end}'
        GROUP BY trade_date
    """), engine)
    flow_map = {str(r['trade_date']): r['cnt'] for _, r in flow.iterrows()}

    todo = []
    for _, r in kline.iterrows():
        d = str(r['trade_date'])
        if flow_map.get(d, 0) < 5000:
            todo.append(d)

    print(f"待补日期 ({len(todo)} 天): {todo}")

    if args.dry_run:
        print("--dry-run 模式，不执行")
        return

    for idx, d in enumerate(todo):
        _backfill_one_day(engine, d, stock_codes)
        # 每天之间休息
        if idx < len(todo) - 1:
            pause = INTER_DAY_PAUSE + random.uniform(0, 60)
            print(f"\n  ⏳ 休息 {pause:.0f}s 后处理下一天...\n")
            time.sleep(pause)

    print(f"\n{'='*60}")
    print(f"  全部完成！共补 {len(todo)} 天")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
