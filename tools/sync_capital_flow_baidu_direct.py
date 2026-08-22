#!/usr/bin/env python3
"""
直接使用百度API同步资金流向（绕过adata封装）
"""
import sys
import os
import json
import time
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine

HEADERS = {
    'Host': 'finance.pae.baidu.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': '*/*',
    'Referer': 'https://gushitong.baidu.com/',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def parse_amount(val):
    """解析 '22.45亿' / '9889.26万' 格式"""
    if pd.isna(val) or val in ('', '--', '-'):
        return 0.0
    val = str(val).strip()
    try:
        if '亿' in val:
            return float(val.replace('亿', '').replace('+', '')) * 1e8
        elif '万' in val:
            return float(val.replace('万', '').replace('+', '')) * 1e4
        else:
            return float(val.replace('+', ''))
    except:
        return 0.0


def fetch_baidu_flow(stock_code, target_date):
    """从百度API获取指定日期的资金流向"""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    next_date = (dt + pd.Timedelta(days=1)).strftime("%Y%m%d")
    url = (
        f"https://finance.pae.baidu.com/vapi/v1/fundsortlist?"
        f"code={stock_code}&market=ab&finance_type=stock&tab=day"
        f"&from=history&date={next_date}&pn=0&rn=1&finClientType=pc"
    )
    try:
        resp = SESSION.get(url, timeout=10)
        data = resp.json()
        content = data.get("Result", {}).get("content", [])
        if not content:
            return None
        for row in content:
            if not isinstance(row, dict):
                continue
            row_date = row.get("date", "").replace("/", "-")[:10]
            if row_date == target_date:
                return {
                    "stock_code": stock_code,
                    "trade_date": target_date,
                    "main_net_inflow": parse_amount(row.get("extMainIn", 0)),
                    "max_net_inflow": parse_amount(row.get("superNetIn", 0)),
                    "lg_net_inflow": parse_amount(row.get("largeNetIn", 0)),
                    "mid_net_inflow": parse_amount(row.get("mediumNetIn", 0)),
                    "sm_net_inflow": parse_amount(row.get("littleNetIn", 0)),
                }
        return None
    except Exception as e:
        return None


def sync_date(engine, target_date):
    """同步指定日期的所有股票资金流向"""
    print(f"\n同步 {target_date} 的资金流向...")

    # 获取所有股票代码
    with engine.connect() as conn:
        result = conn.execute(text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|6)' ORDER BY stock_code"))
        codes = [r[0] for r in result.fetchall()]

    print(f"共 {len(codes)} 只股票")

    # 检查该日期是否已有数据
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily WHERE trade_date = :d"),
            {"d": target_date}
        ).scalar()

    if existing > 0:
        print(f"该日期已有 {existing} 条数据，跳过")
        return

    # 逐只同步
    success = 0
    fail = 0
    rows = []

    for i, code in enumerate(codes):
        data = fetch_baidu_flow(code, target_date)
        if data:
            rows.append(data)
            success += 1
        else:
            fail += 1

        if (i + 1) % 100 == 0:
            print(f"  进度: {i+1}/{len(codes)} | 成功: {success} | 失败: {fail}")

        time.sleep(0.1)  # 避免请求过快

    if rows:
        df = pd.DataFrame(rows)
        df["data_source"] = "baidu"
        df["etl_sync_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df.to_sql("sm_stock_capital_flow_daily", engine, if_exists="append", index=False, method="multi", chunksize=1000)
        print(f"写入 {len(df)} 条数据")
    else:
        print("无数据可写入")


def main():
    sys.stdout.reconfigure(line_buffering=True)

    engine = create_tool_engine()

    # 要同步的日期列表
    dates_to_sync = ["2026-05-26", "2026-05-27", "2026-05-28", "2026-05-29", "2026-05-30", "2026-06-02"]

    print("=" * 60)
    print("百度API资金流向同步")
    print("=" * 60)

    for d in dates_to_sync:
        sync_date(engine, d)

    # 验证
    print("\n" + "=" * 60)
    print("验证结果:")
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT trade_date, COUNT(*) as cnt FROM sm_stock_capital_flow_daily "
            "WHERE trade_date >= '2026-05-25' GROUP BY trade_date ORDER BY trade_date"
        ))
        for row in result.fetchall():
            print(f"  {row[0]}: {row[1]} 条")

    print("=" * 60)


if __name__ == "__main__":
    main()
