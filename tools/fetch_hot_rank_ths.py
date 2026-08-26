#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取指定快照日期的同花顺热股TOP100，写入 st_hot_rank_ths。
自动为表添加 snapshot_date 列，不删除历史数据。
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

from server.common.batch_db import create_batch_engine, replace_table_rows
from server.common.hot_rank_schema import validate_hot_rank_runtime_schema
from server.common.ths_hot_contract import (
    THS_HOT_RANK_TASK_TYPE,
    ThsHotDataBlocked,
    batch_timestamp,
    build_blocked_receipt,
    build_pass_receipt,
    require_capture_window,
    shanghai_now,
    validate_rank_inventory,
)


_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _assert_current_snapshot_date(snapshot_date: str, *, now: datetime | None = None) -> None:
    current = now or datetime.now(_SHANGHAI)
    if current.tzinfo is not None:
        current = current.astimezone(_SHANGHAI)
    current_date = current.date().isoformat()
    if snapshot_date != current_date:
        raise ThsHotDataBlocked(
            "THS hot rank is a current-snapshot endpoint and cannot backfill "
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


def _ensure_snapshot_date_column(engine):
    validate_hot_rank_runtime_schema(engine, tables={"st_hot_rank_ths"})


def _readback_hot_rank(engine, snapshot_date: str) -> list[dict]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(text("""
                SELECT snapshot_date, `rank`, stock_code, short_name,
                       change_pct, hot_value, pop_tag, concept_tag, etl_sync_at
                  FROM st_hot_rank_ths
                 WHERE snapshot_date=:snapshot_date
                 ORDER BY `rank`, stock_code
            """), {"snapshot_date": snapshot_date}).mappings().all()
        ]


def fetch_hot_rank_ths(
    snapshot_date: str,
    *,
    now: datetime | None = None,
) -> dict:
    from adata.sentiment.hot import Hot

    _assert_current_snapshot_date(snapshot_date, now=now)
    print(f"开始获取同花顺热股TOP100，快照日期: {snapshot_date}")

    engine = create_batch_engine()
    _ensure_snapshot_date_column(engine)
    started_at = require_capture_window(
        engine,
        task_type=THS_HOT_RANK_TASK_TYPE,
        requested_date=snapshot_date,
        now=now,
    )

    hot = Hot()
    # The released adata endpoint returns the current snapshot and accepts no
    # date argument.  Stamp the requested run date locally after the fetch.
    df = _call_with_retry(hot.hot_rank_100_ths)

    if df is None or df.empty:
        raise RuntimeError("no THS hot rank rows fetched")

    df = df.copy()
    for c in ["change_pct", "hot_value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.replace({np.nan: None, pd.NaT: None})
    minimum = int(os.environ.get("HOT_RANK_MIN_ROWS", "50"))
    source_inventory = validate_rank_inventory(
        df.to_dict(orient="records"),
        minimum=minimum,
    )
    captured_at = shanghai_now()
    batch_at = captured_at.isoformat(sep=" ", timespec="seconds")
    df["snapshot_date"] = snapshot_date
    df["etl_sync_at"] = captured_at
    df = df[["snapshot_date", "rank", "stock_code", "short_name", "change_pct", "hot_value", "pop_tag", "concept_tag", "etl_sync_at"]]

    replace_table_rows(
        df, "st_hot_rank_ths", engine,
        where_sql="snapshot_date = :d", params={"d": snapshot_date}, chunksize=500,
    )

    persisted = _readback_hot_rank(engine, snapshot_date)
    persisted_inventory = validate_rank_inventory(
        persisted,
        target_date=snapshot_date,
        minimum=minimum,
    )
    if (
        persisted_inventory["provider_payload_sha256"]
        != source_inventory["provider_payload_sha256"]
        or persisted_inventory["row_count"] != source_inventory["row_count"]
        or batch_timestamp(persisted) != batch_at
    ):
        raise RuntimeError(
            "persisted THS hot rank batch differs from provider response"
        )

    print(f"写入完成: st_hot_rank_ths, 共 {len(df)} 行, 快照日期: {snapshot_date}")
    return build_pass_receipt(
        task_type=THS_HOT_RANK_TASK_TYPE,
        requested_date=snapshot_date,
        started_at=started_at,
        captured_at=captured_at,
        published_at=shanghai_now(),
        batch_at=batch_at,
        inventory=persisted_inventory,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="获取指定日期的同花顺热股TOP100（写入 st_hot_rank_ths）")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return 1

    started_at = shanghai_now()
    try:
        result = fetch_hot_rank_ths(args.date, now=started_at)
    except ThsHotDataBlocked as exc:
        result = build_blocked_receipt(
            task_type=THS_HOT_RANK_TASK_TYPE,
            requested_date=args.date,
            started_at=started_at,
            reason=str(exc),
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        print(f"THS hot rank sync blocked: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"THS hot rank sync failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
