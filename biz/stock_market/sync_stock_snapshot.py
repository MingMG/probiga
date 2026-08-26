"""
全市场股票快照表 刷新脚本

从同一目标日的 sm_stock_kline + sm_stock_capital_flow_daily
合并写入 sm_stock_snapshot，每只股票一行，约5000条。

用法：
    python biz/stock_market/sync_stock_snapshot.py                # 自动取最新交易日
    python biz/stock_market/sync_stock_snapshot.py --date 2025-05-30  # 指定日期
"""

import argparse
import json
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, replace_table_rows
from server.common.daily_stock_universe import (
    load_daily_stock_universe,
    validate_daily_stock_coverage,
)


def get_engine():
    return create_batch_engine(pool_size=5, max_overflow=10)


# ── 核心逻辑 ────────────────────────────────────────────────
def get_latest_trade_date(engine) -> str:
    """从 sm_stock_kline 取最新交易日"""
    sql = text("""
        SELECT MAX(trade_date) AS d
        FROM sm_stock_kline
        WHERE k_type = 1 AND adjust_type = 0
    """)
    row = pd.read_sql(sql, engine).iloc[0]
    if pd.isna(row["d"]):
        raise RuntimeError(
            "DATA_BLOCKED: sm_stock_kline 表无数据，无法确定最新交易日"
        )
    return str(row["d"])[:10]


def get_nth_trade_date(engine, trade_date: str, offset: int) -> str:
    """取第N个交易日前的日期"""
    sql = text("""
        SELECT trade_date FROM sm_stock_kline
        WHERE k_type = 1 AND trade_date <= :d
        GROUP BY trade_date ORDER BY trade_date DESC
        LIMIT 1 OFFSET :n
    """)
    rows = pd.read_sql(sql, engine, params={"d": trade_date, "n": offset})
    if rows.empty:
        return trade_date
    return str(rows.iloc[0]["trade_date"])


def fetch_snapshot(engine, trade_date: str) -> pd.DataFrame:
    """从 sm_stock_kline + sm_stock_capital_flow_daily 合并数据，预计算多日涨幅、市值、行业"""
    universe = load_daily_stock_universe(engine, trade_date)

    # 1. 主查询：当日K线
    sql = text("""
        SELECT
            k.stock_code,
            k.short_name,
            k.trade_date,
            k.open,
            k.close,
            k.high,
            k.low,
            k.pre_close,
            k.change,
            k.change_pct,
            k.volume,
            k.amount,
            k.turnover_ratio
        FROM sm_stock_kline k
        WHERE k.trade_date = :d
          AND k.k_type = 1
          AND k.adjust_type = 0
    """)
    df = pd.read_sql(sql, engine, params={"d": trade_date})
    validate_daily_stock_coverage(
        universe,
        kline_rows=df.to_dict("records"),
    )

    # 1a. 实时行情仅允许使用目标日快照；显式历史模式没有留存行情时，
    # 安全回退到同日K线收盘价，绝不拼入另一天的当前价。
    cur = pd.read_sql(text("""
        SELECT stock_code, price AS cur_price, change_pct AS cur_change_pct
        FROM sm_stock_current
        WHERE snapshot_at = (
            SELECT MAX(snapshot_at)
            FROM sm_stock_current
            WHERE DATE(snapshot_at) = :d
        )
    """), engine, params={"d": trade_date})
    cur = cur.drop_duplicates(subset=["stock_code"], keep="first")
    df = df.merge(cur, on="stock_code", how="left")
    # price = 实时价优先，回退到K线close
    df["price"] = df["cur_price"].fillna(df["close"])
    df.drop(columns=["cur_price", "cur_change_pct"], inplace=True)
    print(f"[INFO] 实时行情: {len(cur)} 条")

    # 1b. 资金流向必须与K线属于同一目标日。缺失或明显不完整时在写前阻断，
    # 保留上一份原子快照供页面继续读取。
    flow = pd.read_sql(text("""
        SELECT stock_code, main_net_inflow, max_net_inflow,
               lg_net_inflow, mid_net_inflow, sm_net_inflow
        FROM sm_stock_capital_flow_daily
        WHERE trade_date = :d
    """), engine, params={"d": trade_date})
    coverage_audit = validate_daily_stock_coverage(
        universe,
        kline_rows=df.to_dict("records"),
        flow_rows=flow.to_dict("records"),
    )
    df = df.merge(flow, on="stock_code", how="left")
    print(f"[INFO] 资金流向日期: {trade_date}, {len(flow)} 条")

    # 2. 近3/5/10日涨幅
    td3 = get_nth_trade_date(engine, trade_date, 2)
    td5 = get_nth_trade_date(engine, trade_date, 4)
    td10 = get_nth_trade_date(engine, trade_date, 9)

    for label, td_n in [("change_3d", td3), ("change_5d", td5), ("change_10d", td10)]:
        hist = pd.read_sql(
            text("SELECT stock_code, close AS close_n FROM sm_stock_kline "
                 "WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0"),
            engine, params={"d": td_n},
        )
        df = df.merge(hist, on="stock_code", how="left")
        df[label] = ((df["close"] - df["close_n"]) / df["close_n"] * 100).round(2)
        df.drop(columns=["close_n"], inplace=True)

    # 3. 总市值 = 最新收盘价 * 总股本
    shares = pd.read_sql(
        text("SELECT stock_code, total_shares FROM si_stock_shares"), engine
    )
    shares = shares.drop_duplicates(subset=["stock_code"], keep="last")
    df = df.merge(shares, on="stock_code", how="left")
    df["market_cap"] = (df["close"] * df["total_shares"]).round(2)
    df.drop(columns=["total_shares"], inplace=True)

    # 4. 行业（申万一级）
    industry = pd.read_sql(
        text("SELECT stock_code, industry_name FROM si_industry_sw WHERE industry_type = '申万一级'"),
        engine,
    )
    industry = industry.drop_duplicates(subset=["stock_code"], keep="first")
    df = df.merge(industry.rename(columns={"industry_name": "industry"}), on="stock_code", how="left")

    # 最终去重（防止 merge 产生重复行）
    df = df.drop_duplicates(subset=["stock_code"], keep="first")
    df.attrs["coverage_audit"] = coverage_audit

    return df


def write_snapshot(engine, df: pd.DataFrame) -> None:
    """在同一事务内替换完整快照。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["etl_sync_at"] = now

    # 同步自选股排序 + 持仓标记
    try:
        pdf = pd.read_sql(
            text("SELECT stock_code, sort_order, shares FROM st_user_portfolio"),
            engine,
        )
        order_map = dict(zip(pdf["stock_code"], pdf["sort_order"]))
        holding_codes = set(pdf[pdf["shares"] > 0]["stock_code"].tolist())
    except Exception:
        order_map = {}
        holding_codes = set()

    df["sort_order"] = df["stock_code"].map(order_map)
    df["is_holding"] = df["stock_code"].isin(holding_codes).astype(int)

    replace_table_rows(
        df,
        "sm_stock_snapshot",
        engine,
        chunksize=1000,
        method="multi",
    )


# ── 主流程 ──────────────────────────────────────────────────
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="刷新全市场股票快照表")
    parser.add_argument("--date", type=str, default=None, help="交易日期，格式 YYYY-MM-DD，默认自动取最新")
    args = parser.parse_args(argv)

    engine = get_engine()
    try:
        if args.date:
            try:
                trade_date = datetime.strptime(args.date, "%Y-%m-%d").date().isoformat()
            except ValueError:
                print(
                    f"[ERROR] 日期格式错误，应为 YYYY-MM-DD，输入: {args.date}",
                    file=sys.stderr,
                )
                return 2
        else:
            trade_date = get_latest_trade_date(engine)
        print(f"[INFO] 快照日期: {trade_date}")

        try:
            df = fetch_snapshot(engine, trade_date)
        except RuntimeError as exc:
            print(f"[ERROR] {exc}", file=sys.stderr)
            return 2
        print(f"[INFO] 拉取到 {len(df)} 只股票数据")
        print(
            "COVERAGE_MANIFEST="
            + json.dumps(
                df.attrs.get("coverage_audit") or {},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )

        write_snapshot(engine, df)
        print(f"[OK] sm_stock_snapshot 已刷新，共 {len(df)} 行，日期 {trade_date}")
        print(f"DATE={trade_date}")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
