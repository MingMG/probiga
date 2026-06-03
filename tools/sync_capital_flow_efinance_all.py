# -*- coding: utf-8 -*-
"""
使用 efinance 同步所有A股资金流向数据。

efinance 走的是东财 push2his 接口，有独立的连接池和重试机制。
覆盖所有A股：深主板(000/001/002/003)、创业板(300/301)、沪主板(600/601/603/605)、科创板(688/689)。

用法：
    python tools/sync_capital_flow_efinance_all.py                     # 同步所有股票（当日）
    python tools/sync_capital_flow_efinance_all.py --date 2026-06-02   # 指定日期
    python tools/sync_capital_flow_efinance_all.py --limit 50          # 只处理前50只（测试用）
    python tools/sync_capital_flow_efinance_all.py --batch-size 200    # 每批写入200只
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from sqlalchemy import text
from server.api.routers._engine import get_engine


def fetch_flow(stock_code: str) -> pd.DataFrame | None:
    """用 efinance 获取单只股票的资金流向（约120天历史）"""
    import efinance as ef
    try:
        df = ef.stock.get_history_bill(stock_code=stock_code)
        if df is None or df.empty:
            return None

        cols = df.columns.tolist()
        n = len(df)
        result = pd.DataFrame({
            "stock_code": [stock_code] * n,
            "trade_date": pd.to_datetime(df[cols[2]]).dt.strftime("%Y-%m-%d").tolist(),
            "main_net_inflow": pd.to_numeric(df[cols[3]], errors="coerce").tolist(),
            "sm_net_inflow": pd.to_numeric(df[cols[4]], errors="coerce").tolist(),
            "mid_net_inflow": pd.to_numeric(df[cols[5]], errors="coerce").tolist(),
            "lg_net_inflow": pd.to_numeric(df[cols[6]], errors="coerce").tolist(),
            "max_net_inflow": pd.to_numeric(df[cols[7]], errors="coerce").tolist(),
        })
        return result
    except Exception as e:
        return None


def main():
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

    parser = argparse.ArgumentParser(description="efinance 同步所有A股资金流向")
    parser.add_argument("--date", type=str, default="", help="只保留指定日期（YYYY-MM-DD），空=全部120天")
    parser.add_argument("--limit", type=int, default=0, help="最多处理几只股票（0=全部，调试用）")
    parser.add_argument("--batch-size", type=int, default=100, help="每批写入DB的股票数")
    parser.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数（防限流）")
    parser.add_argument("--skip-truncate", action="store_true", help="不清空表，追加模式")
    args = parser.parse_args()

    engine = get_engine()
    print("数据库连接成功")

    # 获取所有A股代码（排除B股：4/8/9开头）
    with engine.connect() as conn:
        codes = [row[0] for row in conn.execute(
            text("SELECT stock_code FROM si_all_code WHERE stock_code NOT LIKE '4%' AND stock_code NOT LIKE '8%' AND stock_code NOT LIKE '9%' ORDER BY stock_code")
        ).fetchall()]

    if args.limit > 0:
        codes = codes[:args.limit]

    total_stocks = len(codes)
    print(f"共 {total_stocks} 只A股待同步")

    # 清空表（除非指定追加模式）
    if not args.skip_truncate:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE sm_stock_capital_flow_daily"))
        print("已清空 sm_stock_capital_flow_daily")

    # 按板块统计
    boards = {}
    for c in codes:
        prefix = c[:3]
        boards[prefix] = boards.get(prefix, 0) + 1
    print("板块分布:", ", ".join(f"{k}({v})" for k, v in sorted(boards.items())))

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = 0
    success = 0
    fail = 0
    batch_data = []

    for i, code in enumerate(codes):
        code = str(code).strip().zfill(6)
        df = fetch_flow(code)

        if df is not None and not df.empty:
            if args.date:
                df = df[df["trade_date"] == args.date[:10]]

            if not df.empty:
                df["etl_sync_at"] = now_str
                batch_data.append(df)
                success += 1
        else:
            fail += 1

        # 批量写入
        if len(batch_data) >= args.batch_size or (i + 1) == total_stocks:
            if batch_data:
                combined = pd.concat(batch_data, ignore_index=True)
                combined.to_sql("sm_stock_capital_flow_daily", engine, if_exists="append", index=False, method="multi")
                total_rows += len(combined)
                batch_data = []

        if (i + 1) % 100 == 0 or (i + 1) == total_stocks:
            pct = (i + 1) * 100 // total_stocks
            print(f"  [{pct}%] {i+1}/{total_stocks} | 成功: {success} | 失败: {fail} | 写入: {total_rows} 行")

        time.sleep(args.sleep)

    # 最终验证
    print()
    print("=" * 60)
    print("同步完成！")
    with engine.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily")).scalar()
        max_d = conn.execute(text("SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily")).scalar()
        stock_cnt = conn.execute(text("SELECT COUNT(DISTINCT stock_code) FROM sm_stock_capital_flow_daily")).scalar()
        print(f"表中共 {cnt} 条数据，覆盖 {stock_cnt} 只股票，最新日期: {max_d}")

        # 按日期统计
        if args.date:
            df_stat = pd.read_sql(text(
                "SELECT trade_date, COUNT(*) as cnt FROM sm_stock_capital_flow_daily WHERE trade_date = :d GROUP BY trade_date"
            ), conn, params={"d": args.date[:10]})
        else:
            df_stat = pd.read_sql(text(
                "SELECT trade_date, COUNT(*) as cnt FROM sm_stock_capital_flow_daily GROUP BY trade_date ORDER BY trade_date DESC LIMIT 5"
            ), conn)
        print()
        print("日期分布:")
        print(df_stat.to_string(index=False))
    print("=" * 60)


if __name__ == "__main__":
    main()
