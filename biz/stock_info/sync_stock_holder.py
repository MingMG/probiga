# -*- coding: utf-8 -*-
"""
同步股东人数数据到 si_stock_holder 表。

数据来源：东方财富 RPT_F10_EH_HOLDERSNUM 接口

用法::

  python -m biz.stock_info.sync_stock_holder              # 全量同步
  python -m biz.stock_info.sync_stock_holder --limit 50   # 只同步前50只
  python -m biz.stock_info.sync_stock_holder --code 002156  # 单只股票
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

# 复用 adata 中已封装的东财接口
from adata.stock.info.stock_info import StockInfo


def _engine():
    url = os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)
    return create_engine(url, pool_pre_ping=True, pool_size=3, max_overflow=5)


def run_ddl(engine):
    """建表（如不存在）"""
    sql_path = Path(__file__).parent / "sql" / "01_si_stock_info_tables.sql"
    ddl = sql_path.read_text(encoding="utf-8")
    stmts = [s.strip() for s in ddl.split(";\n") if s.strip() and not s.strip().startswith("--")]
    with engine.begin() as conn:
        for stmt in stmts:
            if "si_stock_holder" in stmt:
                conn.execute(text(stmt))


UPSERT_SQL = text("""
    INSERT INTO si_stock_holder
        (stock_code, report_date, holder_num, holder_num_change, pre_holder_num,
         holder_num_ratio, avg_free_shares, etl_sync_at)
    VALUES
        (:stock_code, :report_date, :holder_num, :holder_num_change, :pre_holder_num,
         :holder_num_ratio, :avg_free_shares, :etl_sync_at)
    ON DUPLICATE KEY UPDATE
        holder_num = VALUES(holder_num),
        holder_num_change = VALUES(holder_num_change),
        pre_holder_num = VALUES(pre_holder_num),
        holder_num_ratio = VALUES(holder_num_ratio),
        avg_free_shares = VALUES(avg_free_shares),
        etl_sync_at = VALUES(etl_sync_at)
""")


def sync_one(engine, info: StockInfo, stock_code: str, ts: str) -> int:
    """同步单只股票的股东人数数据，返回写入行数。"""
    try:
        df = info.get_stock_holder(stock_code=stock_code, is_history=True)
    except Exception as e:
        print(f"  [WARN] {stock_code} 拉取失败: {e}")
        return 0
    if df.empty:
        return 0

    # 清洗
    for col in ["holder_num", "holder_num_change", "pre_holder_num"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["holder_num_ratio", "avg_free_shares"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["report_date"])
    df["etl_sync_at"] = ts

    rows = 0
    with engine.begin() as conn:
        for _, r in df.iterrows():
            try:
                conn.execute(UPSERT_SQL, {
                    "stock_code": str(r["stock_code"]).strip().zfill(6),
                    "report_date": r["report_date"],
                    "holder_num": int(r["holder_num"]) if pd.notna(r["holder_num"]) else None,
                    "holder_num_change": int(r["holder_num_change"]) if pd.notna(r["holder_num_change"]) else None,
                    "pre_holder_num": int(r["pre_holder_num"]) if pd.notna(r["pre_holder_num"]) else None,
                    "holder_num_ratio": float(r["holder_num_ratio"]) if pd.notna(r["holder_num_ratio"]) else None,
                    "avg_free_shares": float(r["avg_free_shares"]) if pd.notna(r["avg_free_shares"]) else None,
                    "etl_sync_at": ts,
                })
                rows += 1
            except Exception:
                pass
    return rows


def main() -> int:
    p = argparse.ArgumentParser(description="同步股东人数数据到 si_stock_holder")
    p.add_argument("--code", type=str, default="", help="指定单只股票代码")
    p.add_argument("--limit", type=int, default=0, help="只同步前N只（默认全量）")
    p.add_argument("--sleep", type=float, default=0.2, help="每只股票间隔秒数")
    args = p.parse_args()

    engine = _engine()
    run_ddl(engine)

    info = StockInfo()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if args.code.strip():
        codes = [args.code.strip().zfill(6)]
    else:
        rows = pd.read_sql(
            text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|6)' ORDER BY stock_code"),
            engine,
        )
        codes = rows["stock_code"].tolist()
        if args.limit > 0:
            codes = codes[: args.limit]

    total = len(codes)
    ok = 0
    written = 0
    for i, code in enumerate(codes):
        n = sync_one(engine, info, code, ts)
        written += n
        if n > 0:
            ok += 1
        if (i + 1) % 100 == 0:
            print(f"  进度 {i + 1}/{total}，已同步 {ok} 只，写入 {written} 行")
        if args.sleep > 0 and i < total - 1:
            time.sleep(args.sleep)

    print(f"完成：共 {total} 只股票，{ok} 只有数据，写入 {written} 行")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
