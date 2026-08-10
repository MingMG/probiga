#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取指定快照日期的同花顺热门概念/行业TOP20，写入 st_hot_concept_ths_daily。
只覆盖同花顺 plate_type=1/2，避免删除同一天的东财板块热度 plate_type=3/4。
"""

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text
from requests.exceptions import RequestException

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.batch_db import create_batch_engine, write_frame


def _call_with_retry(fn, *args, retries: int = 3, delay: float = 3.0, **kwargs):
    last = None
    for i in range(max(1, retries)):
        try:
            return fn(*args, **kwargs)
        except RequestException as e:
            last = e
            if i == retries - 1:
                break
            wait = delay * (i + 1)
            print(f"  网络请求失败，{wait:.0f}s 后重试({i + 1}/{retries}): {e}")
            time.sleep(wait)
    raise last


def fetch_hot_concept_ths_daily(snapshot_date: str):
    from adata.sentiment.hot import Hot

    print(f"开始获取同花顺热门概念/行业TOP20，快照日期: {snapshot_date}")

    engine = create_batch_engine()

    hot = Hot()
    parts = []
    for plate_type in (1, 2):
        df = _call_with_retry(hot.hot_concept_20_ths, plate_type=plate_type, snapshot_date=snapshot_date)
        if df is not None and not df.empty:
            df = df.copy()
            df["plate_type"] = plate_type
            df["snapshot_date"] = snapshot_date
            for c in ["change_pct", "hot_value"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            parts.append(df)

    if not parts:
        raise RuntimeError("no THS hot concept rows fetched")

    full_df = pd.concat(parts, ignore_index=True)
    full_df = full_df.replace({np.nan: None, pd.NaT: None})
    full_df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    full_df = full_df[["snapshot_date", "plate_type", "rank", "concept_code", "concept_name", "change_pct", "hot_value", "hot_tag", "etl_sync_at"]]

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM `st_hot_concept_ths_daily` WHERE `snapshot_date` = :d AND `plate_type` IN (1, 2)"),
            {"d": snapshot_date},
        )
        write_frame(
            full_df,
            "st_hot_concept_ths_daily",
            conn,
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi",
        )

    print(f"写入完成: st_hot_concept_ths_daily, 共 {len(full_df)} 行, 快照日期: {snapshot_date}")

    concept_count = len(full_df[full_df["plate_type"] == 1])
    industry_count = len(full_df[full_df["plate_type"] == 2])
    print(f"  概念板块: {concept_count} 条, 行业板块: {industry_count} 条")


def main() -> int:
    parser = argparse.ArgumentParser(description="获取指定日期的同花顺热门概念/行业TOP20（写入 st_hot_concept_ths_daily）")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return 1

    try:
        fetch_hot_concept_ths_daily(args.date)
    except Exception as exc:
        print(f"THS hot concept sync failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
