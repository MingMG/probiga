"""
全市场股票快照表 刷新脚本

从 sm_stock_kline（最新日K）+ sm_stock_capital_flow_daily（最新资金流向）
合并写入 sm_stock_snapshot，每只股票一行，约5000条。

用法：
    python biz/stock_market/sync_stock_snapshot.py                # 自动取最新交易日
    python biz/stock_market/sync_stock_snapshot.py --date 2025-05-30  # 指定日期
"""

import argparse
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, read_frame, write_frame


def get_engine():
    return create_batch_engine(pool_size=5, max_overflow=10)


def _read_frame(sql, engine, params: dict | None = None) -> pd.DataFrame:
    return read_frame(sql, engine, params=params)


# ── 核心逻辑 ────────────────────────────────────────────────
def _coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def get_latest_trade_date(engine) -> str:
    """从 sm_stock_kline 取最新交易日"""
    sql = text("""
        SELECT MAX(trade_date) AS d
        FROM sm_stock_kline
        WHERE k_type = 1 AND adjust_type = 0
    """)
    row = _read_frame(sql, engine).iloc[0]
    if pd.isna(row["d"]):
        print("[ERROR] sm_stock_kline 表无数据，无法确定最新交易日")
        sys.exit(1)
    return str(row["d"])


def get_nth_trade_date(engine, trade_date: str, offset: int) -> str:
    """取第N个交易日前的日期"""
    sql = text("""
        SELECT trade_date FROM sm_stock_kline
        WHERE k_type = 1 AND trade_date <= :d
        GROUP BY trade_date ORDER BY trade_date DESC
        LIMIT 1 OFFSET :n
    """)
    rows = _read_frame(sql, engine, params={"d": trade_date, "n": offset})
    if rows.empty:
        return trade_date
    return str(rows.iloc[0]["trade_date"])


def fetch_snapshot(engine, trade_date: str) -> pd.DataFrame:
    """从 sm_stock_kline + sm_stock_capital_flow_daily 合并数据，预计算多日涨幅、市值、行业"""
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
            k.`change`,
            k.change_pct,
            k.volume,
            k.amount,
            k.turnover_ratio
        FROM sm_stock_kline k
        WHERE k.trade_date = :d
          AND k.k_type = 1
          AND k.adjust_type = 0
    """)
    df = _read_frame(sql, engine, params={"d": trade_date})
    df = _coerce_numeric_columns(
        df,
        ["open", "close", "high", "low", "pre_close", "change", "change_pct", "volume", "amount", "turnover_ratio"],
    )

    # 1a. 实时行情（sm_stock_current，取最新快照）
    cur = _read_frame(text("""
        SELECT stock_code, price AS cur_price, change_pct AS cur_change_pct,
               snapshot_at
        FROM sm_stock_current
        ORDER BY stock_code, snapshot_at DESC
    """), engine)
    if "snapshot_at" in cur.columns:
        cur["_snapshot_order"] = pd.to_datetime(cur["snapshot_at"], errors="coerce")
        cur = cur.sort_values(
            ["stock_code", "_snapshot_order"],
            ascending=[True, False],
            na_position="last",
            kind="stable",
        )
    cur = cur.drop_duplicates(subset=["stock_code"], keep="first")
    cur.drop(columns=["snapshot_at", "_snapshot_order"], errors="ignore", inplace=True)
    cur = _coerce_numeric_columns(cur, ["cur_price", "cur_change_pct"])
    df = df.merge(cur, on="stock_code", how="left")
    # price = 实时价优先，回退到K线close
    df["price"] = df["cur_price"].fillna(df["close"])
    df.drop(columns=["cur_price", "cur_change_pct"], inplace=True)
    print(f"[INFO] 实时行情: {len(cur)} 条")

    # 1b. 资金流向（取最新可用日期，不要求与K线日期一致）
    flow_td = _read_frame(text("SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily"), engine)
    flow_date = flow_td.iloc[0]["d"]
    if flow_date is not None and not pd.isna(flow_date):
        flow = _read_frame(text("""
            SELECT stock_code, main_net_inflow, max_net_inflow,
                   lg_net_inflow, mid_net_inflow, sm_net_inflow
            FROM sm_stock_capital_flow_daily
            WHERE trade_date = :d
        """), engine, params={"d": str(flow_date)})
        flow = flow.drop_duplicates(subset=["stock_code"], keep="first")
        flow = _coerce_numeric_columns(
            flow,
            ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"],
        )
        df = df.merge(flow, on="stock_code", how="left")
        print(f"[INFO] 资金流向日期: {flow_date}, {len(flow)} 条")

    # 2. 近3/5/10日涨幅
    td3 = get_nth_trade_date(engine, trade_date, 2)
    td5 = get_nth_trade_date(engine, trade_date, 4)
    td10 = get_nth_trade_date(engine, trade_date, 9)

    for label, td_n in [("change_3d", td3), ("change_5d", td5), ("change_10d", td10)]:
        hist = _read_frame(
            text("SELECT stock_code, close AS close_n FROM sm_stock_kline "
                 "WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0"),
            engine, params={"d": td_n},
        )
        hist = _coerce_numeric_columns(hist, ["close_n"])
        df = df.merge(hist, on="stock_code", how="left")
        close_n = df["close_n"].replace({0: pd.NA})
        df[label] = ((df["close"] - close_n) / close_n * 100).round(2)
        df.drop(columns=["close_n"], inplace=True)

    # 3. 总市值 = 最新收盘价 * 总股本
    shares = _read_frame(
        text("SELECT stock_code, total_shares FROM si_stock_shares"), engine
    )
    shares = shares.drop_duplicates(subset=["stock_code"], keep="last")
    shares = _coerce_numeric_columns(shares, ["total_shares"])
    df = df.merge(shares, on="stock_code", how="left")
    df["market_cap"] = (df["close"] * df["total_shares"]).round(2)
    df.drop(columns=["total_shares"], inplace=True)

    # 4. 行业（申万一级）
    industry = _read_frame(
        text("SELECT stock_code, industry_name FROM si_industry_sw WHERE industry_type = '申万一级'"),
        engine,
    )
    industry = industry.drop_duplicates(subset=["stock_code"], keep="first")
    df = df.merge(industry.rename(columns={"industry_name": "industry"}), on="stock_code", how="left")

    # 最终去重（防止 merge 产生重复行）
    df = df.drop_duplicates(subset=["stock_code"], keep="first")

    return df


def write_snapshot(engine, df: pd.DataFrame) -> None:
    """Build a staging snapshot and atomically swap it into production."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["etl_sync_at"] = now

    # 同步自选股排序 + 持仓标记
    try:
        pdf = _read_frame(
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

    stage_table = "sm_stock_snapshot_stage"
    backup_table = "sm_stock_snapshot_backup"
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
        conn.execute(text(f"CREATE TABLE {stage_table} LIKE sm_stock_snapshot"))

    try:
        write_frame(
            df,
            stage_table,
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
        with engine.connect() as conn:
            staged_count = int(conn.execute(text(f"SELECT COUNT(*) FROM {stage_table}")).scalar() or 0)
        if staged_count != len(df):
            raise RuntimeError(
                f"snapshot staging row mismatch: expected={len(df)} actual={staged_count}"
            )

        # RENAME TABLE is atomic in MySQL: readers see either the complete old
        # snapshot or the complete new one, never an empty half-written table.
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
            conn.execute(text(
                f"RENAME TABLE sm_stock_snapshot TO {backup_table}, "
                f"{stage_table} TO sm_stock_snapshot"
            ))
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
    except Exception:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
        raise


# ── 主流程 ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="刷新全市场股票快照表")
    parser.add_argument("date_arg", nargs="?", default=None, help="交易日期，兼容调度器位置参数")
    parser.add_argument("--date", type=str, default=None, help="交易日期，格式 YYYY-MM-DD，默认自动取最新")
    args = parser.parse_args()

    engine = get_engine()

    trade_date = args.date or args.date_arg or get_latest_trade_date(engine)
    print(f"[INFO] 快照日期: {trade_date}")

    df = fetch_snapshot(engine, trade_date)
    print(f"[INFO] 拉取到 {len(df)} 只股票数据")

    if df.empty:
        print("[WARN] 无数据，跳过写入")
        return

    write_snapshot(engine, df)
    print(f"[OK] sm_stock_snapshot 已刷新，共 {len(df)} 行，日期 {trade_date}")


if __name__ == "__main__":
    main()
