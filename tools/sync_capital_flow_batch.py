#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通过东财 push2 批量接口获取全市场个股日度资金流向（单次请求全量）。

用法：
  python tools/sync_capital_flow_batch.py
  python tools/sync_capital_flow_batch.py --date 2026-05-29

环境变量：MYSQL_URL
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.batch_db import replace_table_rows_exact_keys
from server.common.mysql_lock import CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}


def _engine():
    return create_tool_engine()


def fetch_all_capital_flow() -> pd.DataFrame:
    """
    通过东财 push2 批量接口获取全市场当日资金流向。
    字段说明：
      f12=股票代码, f14=名称, f2=最新价, f3=涨跌幅
      f62=主力净流入, f184=主力净流入占比
      f66=超大单净流入, f69=超大单占比
      f72=大单净流入, f75=大单占比
      f78=中单净流入, f81=中单占比
      f84=小单净流入, f87=小单占比
    """
    all_items = []
    page = 1
    while True:
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get?"
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
        # 主力 = 超大单 + 大单
        main_net = item.get("f62")   # 主力净流入
        max_net = item.get("f66")    # 超大单净流入
        lg_net = item.get("f72")     # 大单净流入
        mid_net = item.get("f78")    # 中单净流入
        sm_net = item.get("f84")     # 小单净流入
        rows.append({
            "stock_code": code,
            "main_net_inflow": main_net,
            "max_net_inflow": max_net,
            "lg_net_inflow": lg_net,
            "mid_net_inflow": mid_net,
            "sm_net_inflow": sm_net,
        })

    df = pd.DataFrame(rows)
    for col in ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def main():
    p = argparse.ArgumentParser(description="批量同步全市场资金流数据")
    p.add_argument("--date", type=str, default="", help="写入的交易日期（默认今天）")
    args = p.parse_args()

    trade_date = args.date or date.today().isoformat()
    eng = _engine()

    print(f"获取全市场资金流向数据（{trade_date}）...")
    df = fetch_all_capital_flow()

    if df.empty:
        print("未获取到数据")
        return

    df["trade_date"] = trade_date
    print(f"获取到 {len(df)} 只股票的资金流数据")

    replace_table_rows_exact_keys(
        df,
        "sm_stock_capital_flow_daily",
        eng,
        key_columns=("stock_code", "trade_date"),
        lock_name=CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
    )
    print(f"已写入 {len(df)} 行到 sm_stock_capital_flow_daily")

    # 验证
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily WHERE trade_date = :d"), {"d": trade_date}).scalar()
        top = conn.execute(text("""
            SELECT stock_code, main_net_inflow FROM sm_stock_capital_flow_daily
            WHERE trade_date = :d ORDER BY main_net_inflow DESC LIMIT 5
        """), {"d": trade_date}).fetchall()
        print(f"\n当日数据：{cnt} 条")
        print("主力净流入 TOP5：")
        for code, flow in top:
            print(f"  {code}: {flow/10000:.0f}万")


if __name__ == "__main__":
    main()
