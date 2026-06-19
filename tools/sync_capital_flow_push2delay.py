#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通过东财 push2delay 接口批量获取全市场个股资金流向（绕过 push2 限流）。

一次请求获取全部股票当日资金流数据，再逐只获取历史资金流。

用法：
  python tools/sync_capital_flow_push2delay.py              # 当日数据（批量）
  python tools/sync_capital_flow_push2delay.py --history    # 含历史数据（逐只较慢）

环境变量：MYSQL_URL
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.config import get_mysql_url

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}


def _engine():
    return create_engine(get_mysql_url(required=True), pool_pre_ping=True)


def fetch_all_today() -> pd.DataFrame:
    """通过 push2delay 批量获取全市场当日资金流"""
    all_items = []
    page = 1
    while True:
        url = (
            "https://push2delay.eastmoney.com/api/qt/clist/get?"
            f"pn={page}&pz=5000&po=1&np=1&ut=b2884a393a59ad64002292a3e90d46a5"
            "&fltt=2&invt=2&fid=f62"
            "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
            "&fields=f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f124"
        )
        resp = requests.get(url, headers=HEADERS, timeout=30)
        data = resp.json()
        if not data.get("data") or not data["data"].get("diff"):
            break
        items = data["data"]["diff"]
        if not items:
            break
        all_items.extend(items)
        total = data["data"].get("total", 0)
        if len(all_items) >= total:
            break
        page += 1

    if not all_items:
        return pd.DataFrame()

    rows = []
    for item in all_items:
        code = str(item.get("f12", "")).strip().zfill(6)
        if not code or code == "000000":
            continue
        rows.append({
            "stock_code": code,
            "main_net_inflow": item.get("f62"),   # 主力净流入
            "max_net_inflow": item.get("f66"),     # 超大单净流入
            "lg_net_inflow": item.get("f72"),      # 大单净流入
            "mid_net_inflow": item.get("f78"),     # 中单净流入
            "sm_net_inflow": item.get("f84"),      # 小单净流入
        })
    df = pd.DataFrame(rows)
    for col in ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def fetch_history_single(stock_code: str) -> pd.DataFrame | None:
    """通过 push2cdn 获取单只股票历史资金流（最近120天）"""
    cid = 1 if stock_code.startswith("6") else 0
    # push2cdn 返回带 callback 的 JSONP，需要处理
    url = (
        f"https://push2cdn.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        f"lmt=0&klt=101&fields1=f1,f2,f3,f7&"
        f"fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&"
        f"secid={cid}.{stock_code}"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        text = resp.text.strip()
        # 去掉 JSONP callback wrapper
        if text.startswith("jQuery") or text.startswith("callback"):
            text = text[text.index("(") + 1 : text.rindex(")")]
        data = json.loads(text)
        if not data.get("data") or "klines" not in data["data"]:
            return None
        lines = data["data"]["klines"]
        if not lines:
            return None
        rows = []
        for line in lines:
            parts = line.split(",")
            if len(parts) >= 6:
                rows.append({
                    "stock_code": stock_code,
                    "trade_date": parts[0],
                    "main_net_inflow": float(parts[1]),
                    "max_net_inflow": float(parts[5]) if len(parts) > 5 else 0,
                    "lg_net_inflow": float(parts[4]) if len(parts) > 4 else 0,
                    "mid_net_inflow": float(parts[3]) if len(parts) > 3 else 0,
                    "sm_net_inflow": float(parts[2]) if len(parts) > 2 else 0,
                })
        return pd.DataFrame(rows) if rows else None
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser(description="push2delay 批量同步资金流")
    p.add_argument("--history", action="store_true", help="同时获取历史数据（逐只较慢）")
    p.add_argument("--limit", type=int, default=0, help="历史模式下最多处理几只（0=全部）")
    p.add_argument("--sleep", type=float, default=0.3, help="历史模式下每只间隔秒数")
    args = p.parse_args()

    eng = _engine()
    trade_date = date.today().isoformat()

    # 第一步：批量获取当日数据
    print(f"获取全市场当日资金流（{trade_date}）...")
    df_today = fetch_all_today()
    if df_today.empty:
        print("未获取到数据")
        return
    df_today["trade_date"] = trade_date
    print(f"获取到 {len(df_today)} 只股票的当日资金流")

    # 删除当日旧数据
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM sm_stock_capital_flow_daily WHERE trade_date = :d"), {"d": trade_date})

    # 写入当日数据
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_today["etl_sync_at"] = now_str
    df_today.to_sql("sm_stock_capital_flow_daily", eng, if_exists="append", index=False, method="multi", chunksize=2000)
    print(f"已写入 {len(df_today)} 条当日数据")

    # 第二步：获取历史数据（可选）
    if args.history:
        codes = df_today["stock_code"].tolist()
        if args.limit > 0:
            codes = codes[:args.limit]
        print(f"\n获取历史资金流（{len(codes)} 只）...")
        total_hist = 0
        for i, code in enumerate(codes):
            hist = fetch_history_single(code)
            if hist is not None and not hist.empty:
                hist["etl_sync_at"] = now_str
                # 删除该股票的旧历史数据（保留当日）
                with eng.begin() as conn:
                    conn.execute(text(
                        "DELETE FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date != :d"
                    ), {"c": code, "d": trade_date})
                hist.to_sql("sm_stock_capital_flow_daily", eng, if_exists="append", index=False, method="multi")
                total_hist += len(hist)
            if (i + 1) % 100 == 0:
                print(f"  进度: {i+1}/{len(codes)} | 写入: {total_hist} 条历史")
            time.sleep(args.sleep)
        print(f"历史数据完成，共写入 {total_hist} 条")

    # 验证
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily")).scalar()
        stock_cnt = conn.execute(text("SELECT COUNT(DISTINCT stock_code) FROM sm_stock_capital_flow_daily")).scalar()
        max_d = conn.execute(text("SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily")).scalar()
        print(f"\n最终：{cnt} 条，{stock_cnt} 只股票，最新日期 {max_d}")


if __name__ == "__main__":
    main()
