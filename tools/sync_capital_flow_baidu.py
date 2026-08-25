# -*- coding: utf-8 -*-
"""
使用百度数据源同步资金流向数据

用法：
    python tools/sync_capital_flow_baidu.py                    # 同步所有股票
    python tools/sync_capital_flow_baidu.py --code 002156      # 同步单只股票
    python tools/sync_capital_flow_baidu.py --date 2026-05-29  # 同步指定日期
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, date

# 添加项目根目录到 path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

import pandas as pd
from sqlalchemy import text
from server.common.batch_db import create_batch_engine, read_frame, replace_table_rows_exact_keys
from server.common.mysql_lock import CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME


def get_engine():
    return create_batch_engine()

try:
    from adata.stock.market.capital_flow.stock_capital_flow_baidu import StockCapitalFlowBaidu
    BaiduFlow = StockCapitalFlowBaidu
except ImportError:
    print("错误：无法导入百度资金流向模块")
    sys.exit(1)


def sync_single_stock(engine, stock_code: str, start_date: str = None, end_date: str = None):
    """同步单只股票的资金流向"""
    try:
        # 获取百度数据
        kwargs = {"stock_code": stock_code}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        df = BaiduFlow().get_capital_flow(**kwargs)

        if df is None or df.empty:
            print(f"  {stock_code}: 无数据")
            return 0

        # 确保列名正确
        required_cols = ["stock_code", "trade_date", "main_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]
        for col in required_cols:
            if col not in df.columns:
                if col == "lg_net_inflow" and "max_net_inflow" in df.columns:
                    df = df.rename(columns={"max_net_inflow": "lg_net_inflow"})
                elif col not in df.columns:
                    df[col] = 0

        # 选择需要的列
        df = df[required_cols].copy()

        # 转换日期格式
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")

        # 添加 etl_sync_at 字段
        df["etl_sync_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        replace_table_rows_exact_keys(
            df,
            "sm_stock_capital_flow_daily",
            engine,
            key_columns=("stock_code", "trade_date"),
            lock_name=CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        )

        print(f"  {stock_code}: 同步 {len(df)} 条数据")
        return len(df)

    except Exception as e:
        print(f"  {stock_code}: 错误 - {e}")
        return 0


def sync_all_stocks(engine, trade_date: str = None, max_workers: int = 4):
    """同步所有股票（并发版本）"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 获取所有股票代码
    df = read_frame(text("SELECT stock_code FROM si_all_code WHERE stock_code NOT LIKE '4%' AND stock_code NOT LIKE '8%' AND stock_code NOT LIKE '9%' ORDER BY stock_code"), engine)
    stock_codes = df["stock_code"].tolist()

    print(f"共 {len(stock_codes)} 只股票待同步，使用 {max_workers} 个线程并发")

    total = 0
    completed = 0

    def sync_one(code):
        return code, sync_single_stock(engine, code, start_date=trade_date, end_date=trade_date)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(sync_one, code): code for code in stock_codes}

        for future in as_completed(futures):
            code, count = future.result()
            completed += 1
            total += count

            if completed % 50 == 0:
                print(f"进度：{completed}/{len(stock_codes)} ({completed*100//len(stock_codes)}%)")

    print(f"同步完成，共 {total} 条数据")
    return total


def main():
    import sys
    # 确保输出立即刷新
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(description="使用百度数据源同步资金流向")
    parser.add_argument("--code", type=str, help="股票代码（单只）")
    parser.add_argument("--date", type=str, help="指定日期（YYYY-MM-DD）")
    args = parser.parse_args()

    print("=" * 60)
    print("开始同步资金流向数据（百度数据源）")
    print("=" * 60)

    engine = get_engine()
    print("数据库连接成功")

    if args.code:
        # 同步单只股票
        code = args.code.strip().zfill(6)
        print(f"同步 {code} 的资金流向...")
        sync_single_stock(engine, code, start_date=args.date, end_date=args.date)
    else:
        # 同步所有股票
        print("同步所有股票的资金流向...")
        sync_all_stocks(engine, trade_date=args.date)

    print("=" * 60)
    print("同步完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
