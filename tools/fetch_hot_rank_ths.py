#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取指定快照日期的同花顺热股TOP100，写入 st_hot_rank_ths。
自动为表添加 snapshot_date 列，不删除历史数据。
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


def _ensure_snapshot_date_column(engine):
    with engine.connect() as conn:
        r = conn.execute(
            text("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'st_hot_rank_ths' AND column_name = 'snapshot_date'")
        ).scalar()
    if int(r or 0) == 0:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE `st_hot_rank_ths` ADD COLUMN `snapshot_date` DATE NOT NULL COMMENT '快照日期' AFTER `concept_tag`"))
        print("已为 st_hot_rank_ths 添加 snapshot_date 列")


def fetch_hot_rank_ths(snapshot_date: str):
    from adata.sentiment.hot import Hot

    print(f"开始获取同花顺热股TOP100，快照日期: {snapshot_date}")

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    _ensure_snapshot_date_column(engine)

    hot = Hot()
    df = hot.hot_rank_100_ths(snapshot_date=snapshot_date)

    if df is None or df.empty:
        print("未获取到热股TOP100数据")
        return

    df = df.copy()
    df["snapshot_date"] = snapshot_date
    for c in ["change_pct", "hot_value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.replace({np.nan: None, pd.NaT: None})
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    df = df[["snapshot_date", "rank", "stock_code", "short_name", "change_pct", "hot_value", "pop_tag", "concept_tag", "etl_sync_at"]]

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM st_hot_rank_ths WHERE snapshot_date = :d"), {"d": snapshot_date})
    df.to_sql("st_hot_rank_ths", engine, if_exists="append", index=False, chunksize=500, method="multi")

    print(f"写入完成: st_hot_rank_ths, 共 {len(df)} 行, 快照日期: {snapshot_date}")


def main():
    parser = argparse.ArgumentParser(description="获取指定日期的同花顺热股TOP100（写入 st_hot_rank_ths）")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return

    fetch_hot_rank_ths(args.date)


if __name__ == "__main__":
    main()