#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用同花顺数据源同步个股资金流向。

数据来源: akshare -> 同花顺 (data.10jqka.com.cn)
覆盖: 全部A股 ~5,200只
特点: 不走东财push2his，不受东财IP封禁影响
限制: 只有"净额"（流入-流出），没有按大单/中单/小单分类

用法:
    python tools/sync_capital_flow_ths.py                     # 同步今日
    python tools/sync_capital_flow_ths.py --date 2026-06-02   # 同步指定日期（仅当日有效）
    python tools/sync_capital_flow_ths.py --dry-run            # 只看数据不写库
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from sqlalchemy import text
from server.api.routers._engine import get_engine


def _parse_amount(val):
    """解析 '2.61亿' / '9889.26万' 这种格式为数值"""
    if pd.isna(val) or val == '' or val == '--':
        return 0.0
    val = str(val).strip()
    try:
        if '亿' in val:
            return float(val.replace('亿', '')) * 1e8
        elif '万' in val:
            return float(val.replace('万', '')) * 1e4
        else:
            return float(val)
    except (ValueError, TypeError):
        return 0.0


def fetch_ths_instant():
    """从同花顺获取今日全部A股资金流（即时）"""
    import akshare as ak

    print("正在从同花顺获取即时资金流数据...")
    df = ak.stock_fund_flow_individual(symbol='即时')

    if df is None or df.empty:
        print("  获取失败，无数据")
        return None

    print(f"  获取到 {len(df)} 条数据")

    # 列名: 序号, 股票代码, 股票简称, 最新价, 涨跌幅, 换手率, 流入资金, 流出资金, 净额, 成交额
    result = pd.DataFrame()
    result['stock_code'] = df['股票代码'].astype(str).str.zfill(6)
    result['trade_date'] = datetime.now().strftime('%Y-%m-%d')

    # 解析金额
    result['main_net_inflow'] = df['净额'].apply(_parse_amount)

    # 同花顺没有按大小单分类，全部归入 main_net_inflow
    # 其他字段置0，保持表结构兼容
    result['sm_net_inflow'] = 0.0
    result['mid_net_inflow'] = 0.0
    result['lg_net_inflow'] = 0.0
    result['max_net_inflow'] = 0.0

    return result


def main():
    sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

    parser = argparse.ArgumentParser(description="同花顺数据源同步个股资金流向")
    parser.add_argument("--date", type=str, default="", help="目标日期（同花顺只有当日即时数据）")
    parser.add_argument("--dry-run", action="store_true", help="只显示数据，不写入数据库")
    args = parser.parse_args()

    print("=" * 60)
    print("同花顺资金流向同步")
    print("=" * 60)

    # 获取数据
    df = fetch_ths_instant()
    if df is None or df.empty:
        print("未获取到数据")
        return

    # 数据概览
    print(f"\n数据概览:")
    print(f"  股票数: {df['stock_code'].nunique()}")
    print(f"  日期: {df['trade_date'].iloc[0]}")
    print(f"  净流入>0: {(df['main_net_inflow'] > 0).sum()} 只")
    print(f"  净流入<0: {(df['main_net_inflow'] < 0).sum()} 只")
    print(f"  总净流入: {df['main_net_inflow'].sum():,.0f}")

    # 前10名
    top10 = df.nlargest(10, 'main_net_inflow')[['stock_code', 'main_net_inflow']]
    print(f"\n净流入前10:")
    for _, row in top10.iterrows():
        print(f"  {row['stock_code']}: {row['main_net_inflow']:>15,.0f}")

    if args.dry_run:
        print("\n[dry-run] 不写入数据库")
        return

    # 写入数据库
    engine = get_engine()
    target_date = df['trade_date'].iloc[0]

    # 只删除 THS 来源的旧数据，保留东财等其他来源的数据
    with engine.begin() as conn:
        deleted = conn.execute(
            text("DELETE FROM sm_stock_capital_flow_daily WHERE trade_date = :d AND data_source = 'ths'"),
            {"d": target_date}
        ).rowcount
        if deleted > 0:
            print(f"\n已删除 {target_date} 的旧 THS 数据 {deleted} 条")

    # 获取已有东财数据的股票列表，THS 不覆盖
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT stock_code FROM sm_stock_capital_flow_daily WHERE trade_date = :d AND data_source != 'ths'"),
            {"d": target_date}
        ).fetchall()
        existing_codes = {r[0] for r in existing}

    if existing_codes:
        before_count = len(df)
        df = df[~df['stock_code'].isin(existing_codes)]
        print(f"跳过已有东财数据的股票: {before_count - len(df)} 只")

    if df.empty:
        print("无需写入（所有股票已有东财数据）")
        return

    # 写入新数据
    df['data_source'] = 'ths'
    df['etl_sync_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df.to_sql('sm_stock_capital_flow_daily', engine, if_exists='append', index=False,
              chunksize=500, method='multi')

    print(f"写入完成: {len(df)} 条")

    # 验证
    with engine.connect() as conn:
        cnt = conn.execute(text(
            "SELECT COUNT(*) FROM sm_stock_capital_flow_daily WHERE trade_date = :d"
        ), {"d": target_date}).scalar()
        print(f"验证: {target_date} 共 {cnt} 条记录")

    print("=" * 60)


if __name__ == "__main__":
    main()
