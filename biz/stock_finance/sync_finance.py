"""
股票财务核心指标同步脚本

调用 adata stock.finance.get_core_index() 获取东方财富财务数据，
写入 si_stock_finance 表。

用法：
    python biz/stock_finance/sync_finance.py                 # 全量同步（增量：只拉新报告期）
    python biz/stock_finance/sync_finance.py --code 600396   # 同步单只股票
    python biz/stock_finance/sync_finance.py --limit 100      # 只同步前100只
"""

import argparse
import os
import sys
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# ── 数据库连接 ──────────────────────────────────────────────
MYSQL_URL = os.getenv(
    "MYSQL_URL",
    "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga",
)


def get_engine():
    return create_engine(MYSQL_URL, pool_size=5, max_overflow=10)


def get_all_stock_codes(engine) -> list:
    """获取全市场股票代码"""
    df = pd.read_sql(
        text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|6)' ORDER BY stock_code"),
        engine,
    )
    return df["stock_code"].tolist()


def fetch_finance(stock_code: str) -> pd.DataFrame:
    """调用 adata 获取单只股票的财务核心指标"""
    try:
        from adata.stock.finance import finance
        df = finance.get_core_index(stock_code)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception as e:
        print(f"  [WARN] {stock_code} 获取失败: {e}")
        return pd.DataFrame()


def upsert_finance(engine, df: pd.DataFrame) -> int:
    """写入数据库（ON DUPLICATE KEY UPDATE）"""
    if df is None or df.empty:
        return 0

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cols = [
        "stock_code", "short_name", "report_date", "report_type", "notice_date",
        "basic_eps", "diluted_eps", "non_gaap_eps", "net_asset_ps",
        "cap_reserve_ps", "undist_profit_ps", "oper_cf_ps",
        "total_rev", "gross_profit", "net_profit_attr_sh", "non_gaap_net_profit",
        "total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr",
        "total_rev_qoq_gr", "net_profit_qoq_gr",
        "roe_wtd", "roe_non_gaap_wtd", "roa_wtd", "gross_margin", "net_margin",
        "curr_ratio", "quick_ratio", "cash_flow_ratio", "asset_liab_ratio",
    ]

    # 只保留存在的列
    available = [c for c in cols if c in df.columns]
    df = df[available].copy()

    # 数值列转为 float（防止 pandas object 类型）
    for c in df.columns:
        if c not in ("stock_code", "short_name", "report_date", "report_type", "notice_date"):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 构造 INSERT SQL
    placeholders = ", ".join([f":{c}" for c in available])
    col_names = ", ".join(available)
    update_clause = ", ".join([f"{c} = VALUES({c})" for c in available if c not in ("stock_code", "report_date")])
    update_clause += ", etl_sync_at = VALUES(etl_sync_at)"

    sql = text(f"""
        INSERT INTO si_stock_finance ({col_names}, etl_sync_at)
        VALUES ({placeholders}, :etl_sync_at)
        ON DUPLICATE KEY UPDATE {update_clause}
    """)

    count = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            params = {c: (None if pd.isna(row[c]) else row[c]) for c in available}
            params["etl_sync_at"] = now
            conn.execute(sql, params)
            count += 1

    return count


def main():
    parser = argparse.ArgumentParser(description="同步股票财务核心指标")
    parser.add_argument("--code", type=str, default=None, help="同步单只股票代码")
    parser.add_argument("--limit", type=int, default=None, help="只同步前N只")
    parser.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数（防限流）")
    args = parser.parse_args()

    engine = get_engine()

    if args.code:
        codes = [args.code.strip().zfill(6)]
    else:
        codes = get_all_stock_codes(engine)
        if args.limit:
            codes = codes[: args.limit]

    print(f"[INFO] 待同步 {len(codes)} 只股票")

    total_rows = 0
    fail_count = 0
    for i, code in enumerate(codes):
        df = fetch_finance(code)
        if df.empty:
            fail_count += 1
        else:
            rows = upsert_finance(engine, df)
            total_rows += rows

        if (i + 1) % 50 == 0:
            print(f"[PROGRESS] {i + 1}/{len(codes)}, 已写入 {total_rows} 条, 失败 {fail_count}")

        if args.sleep > 0 and i < len(codes) - 1:
            time.sleep(args.sleep)

    print(f"[OK] 同步完成: {len(codes)} 只股票, 写入 {total_rows} 条报告期, 失败 {fail_count}")


if __name__ == "__main__":
    main()
