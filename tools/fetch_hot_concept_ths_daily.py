#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取指定快照日期的同花顺热门概念/行业TOP20，写入 st_hot_concept_ths_daily。
只覆盖同花顺 plate_type=1/2，避免删除同一天的东财板块热度 plate_type=3/4。
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

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
from server.common.ths_hot_contract import (
    THS_HOT_CONCEPT_TASK_TYPE,
    ThsHotDataBlocked,
    batch_timestamp,
    build_blocked_receipt,
    build_pass_receipt,
    require_capture_window,
    shanghai_now,
    validate_concept_inventory,
)


_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _assert_current_snapshot_date(snapshot_date: str, *, now: datetime | None = None) -> None:
    current = now or datetime.now(_SHANGHAI)
    if current.tzinfo is not None:
        current = current.astimezone(_SHANGHAI)
    current_date = current.date().isoformat()
    if snapshot_date != current_date:
        raise ThsHotDataBlocked(
            "THS hot concept is a current-snapshot endpoint and cannot backfill "
            f"{snapshot_date}; current Asia/Shanghai date is {current_date}"
        )


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


def _readback_hot_concept(connection, snapshot_date: str) -> list[dict]:
    return [
        dict(row)
        for row in connection.execute(text("""
            SELECT snapshot_date, plate_type, `rank`, concept_code,
                   concept_name, change_pct, hot_value, hot_tag, etl_sync_at
              FROM st_hot_concept_ths_daily
             WHERE snapshot_date=:snapshot_date AND plate_type IN (1,2)
             ORDER BY plate_type, `rank`, concept_code
        """), {"snapshot_date": snapshot_date}).mappings().all()
    ]


def fetch_hot_concept_ths_daily(
    snapshot_date: str,
    *,
    now: datetime | None = None,
) -> dict:
    from adata.sentiment.hot import Hot

    _assert_current_snapshot_date(snapshot_date, now=now)
    print(f"开始获取同花顺热门概念/行业TOP20，快照日期: {snapshot_date}")

    engine = create_batch_engine()
    started_at = require_capture_window(
        engine,
        task_type=THS_HOT_CONCEPT_TASK_TYPE,
        requested_date=snapshot_date,
        now=now,
    )

    hot = Hot()
    parts = []
    for plate_type in (1, 2):
        # The released adata API only exposes ``plate_type`` here.  The
        # snapshot date is metadata owned by this ingestion job, not a
        # provider-side historical query parameter.
        df = _call_with_retry(hot.hot_concept_20_ths, plate_type=plate_type)
        if df is not None and not df.empty:
            df = df.copy()
            df["plate_type"] = plate_type
            df["snapshot_date"] = snapshot_date
            for c in ["change_pct", "hot_value"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            parts.append(df)

    fetched_types = {
        int(part["plate_type"].iloc[0])
        for part in parts
        if not part.empty and "plate_type" in part.columns
    }
    if fetched_types != {1, 2}:
        missing = sorted({1, 2} - fetched_types)
        raise RuntimeError(f"incomplete THS hot concept snapshot; missing plate types: {missing}")

    full_df = pd.concat(parts, ignore_index=True)
    min_rows_per_type = int(os.environ.get("HOT_CONCEPT_MIN_ROWS_PER_TYPE", "10"))
    full_df = full_df.replace({np.nan: None, pd.NaT: None})
    source_inventory = validate_concept_inventory(
        full_df.to_dict(orient="records"),
        minimum_per_type=min_rows_per_type,
    )
    captured_at = shanghai_now()
    batch_at = captured_at.isoformat(sep=" ", timespec="seconds")
    full_df["etl_sync_at"] = captured_at
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
        persisted = _readback_hot_concept(conn, snapshot_date)
        persisted_inventory = validate_concept_inventory(
            persisted,
            target_date=snapshot_date,
            minimum_per_type=min_rows_per_type,
        )
        if (
            persisted_inventory["provider_payload_sha256"]
            != source_inventory["provider_payload_sha256"]
            or persisted_inventory["plate_type_counts"]
            != source_inventory["plate_type_counts"]
            or batch_timestamp(persisted) != batch_at
        ):
            raise RuntimeError(
                "persisted THS hot concept batch differs from provider response"
            )

    print(f"写入完成: st_hot_concept_ths_daily, 共 {len(full_df)} 行, 快照日期: {snapshot_date}")

    concept_count = len(full_df[full_df["plate_type"] == 1])
    industry_count = len(full_df[full_df["plate_type"] == 2])
    print(f"  概念板块: {concept_count} 条, 行业板块: {industry_count} 条")
    return build_pass_receipt(
        task_type=THS_HOT_CONCEPT_TASK_TYPE,
        requested_date=snapshot_date,
        started_at=started_at,
        captured_at=captured_at,
        published_at=shanghai_now(),
        batch_at=batch_at,
        inventory=persisted_inventory,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="获取指定日期的同花顺热门概念/行业TOP20（写入 st_hot_concept_ths_daily）")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return 1

    started_at = shanghai_now()
    try:
        result = fetch_hot_concept_ths_daily(args.date, now=started_at)
    except ThsHotDataBlocked as exc:
        result = build_blocked_receipt(
            task_type=THS_HOT_CONCEPT_TASK_TYPE,
            requested_date=args.date,
            started_at=started_at,
            reason=str(exc),
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        print(f"THS hot concept sync blocked: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"THS hot concept sync failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
