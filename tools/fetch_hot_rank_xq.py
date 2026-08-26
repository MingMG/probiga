#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publish Xueqiu's exact current-session A-share hot-stock Top100."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests as http
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from server.common.batch_db import create_batch_engine, replace_table_rows
from server.common.hot_rank_schema import validate_hot_rank_runtime_schema
from server.common.hot_rank_source_contract import (
    CURRENT_SNAPSHOT_ONLY,
    HOT_RANK_CURRENT_PROVIDERS,
    HOT_RANK_XQ_TASK_TYPE,
    HotRankDataBlocked,
    batch_timestamp,
    build_blocked_receipt,
    build_pass_receipt,
    require_current_capture_window,
    shanghai_now,
    validate_rank_inventory,
)


_SESSION = http.Session()
_SESSION.trust_env = False
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
})

# Xueqiu's ``type=10`` feed is a global cross-market Top100.  Filtering that
# response after the fact yields only about half an A-share list and cannot be
# relabelled as an A-share Top100.  ``type=12`` is the provider's dedicated
# Shanghai/Shenzhen A-share ranking and is still verified as an exact Top100
# before any database write.
_XQ_A_SHARE_HOT_TYPE = 12


def _ensure_snapshot_date_column(engine) -> None:
    validate_hot_rank_runtime_schema(engine, tables={"st_hot_rank_xq"})


def _init_cookie() -> None:
    for attempt in range(3):
        try:
            _SESSION.get("https://xueqiu.com/", timeout=30)
            return
        except Exception:
            if attempt == 2:
                raise


def _fetch_hot_rank_xq() -> pd.DataFrame | None:
    response = _SESSION.get(
        "https://xueqiu.com/service/v5/stock/hot_stock/list",
        params={
            "size": 100,
            "_type": _XQ_A_SHARE_HOT_TYPE,
            "type": _XQ_A_SHARE_HOT_TYPE,
        },
        headers={
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error_code", 0) != 0:
        raise RuntimeError(
            "Xueqiu API error: "
            f"code={payload.get('error_code')} "
            f"message={payload.get('error_description', '')}"
        )
    items = payload.get("data", {}).get("items", [])
    if not items:
        return None

    rows: list[dict[str, Any]] = []
    for item in items:
        symbol = str(item.get("symbol") or "").upper().strip()
        matched = re.fullmatch(r"(?:SH|SZ|BJ)([0-9]{6})", symbol)
        if not matched:
            continue
        stock_code = matched.group(1)
        if not stock_code.startswith(("0", "3", "4", "6", "8")):
            continue
        rows.append({
            "rank": len(rows) + 1,
            "stock_code": stock_code,
            "short_name": str(item.get("name") or ""),
            "current": float(item.get("current") or 0),
            "percent": float(item.get("percent") or 0),
            "chg": float(item.get("chg") or 0),
            "amount": float(item.get("amount") or 0),
            "market_capital": float(item.get("market_capital") or 0),
            "followers": int(item.get("following") or 0),
            "sector": str(item.get("level1") or ""),
            "exchange": str(item.get("exchange") or ""),
            "increment": int(item.get("increment") or 0),
            "diff": int(item.get("diff") or 0),
        })
    return pd.DataFrame(rows)


def _readback_hot_rank(engine, snapshot_date: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(text("""
                SELECT snapshot_date, `rank`, stock_code, short_name, current,
                       percent, chg, amount, market_capital, followers, sector,
                       exchange, increment, diff, etl_sync_at
                  FROM st_hot_rank_xq
                 WHERE snapshot_date=:snapshot_date
                 ORDER BY `rank`, stock_code
            """), {"snapshot_date": snapshot_date}).mappings().all()
        ]


def fetch_hot_rank_xq(
    snapshot_date: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = shanghai_now(now)
    if snapshot_date != current.date().isoformat():
        raise HotRankDataBlocked("CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED")

    print(f"开始获取雪球热股TOP100，快照日期: {snapshot_date}")
    engine = create_batch_engine()
    _ensure_snapshot_date_column(engine)
    started_at = require_current_capture_window(
        engine,
        task_type=HOT_RANK_XQ_TASK_TYPE,
        requested_date=snapshot_date,
        now=current,
    )

    _init_cookie()
    frame: pd.DataFrame | None = None
    for attempt in range(3):
        try:
            frame = _fetch_hot_rank_xq()
            break
        except Exception:
            if attempt == 2:
                raise
            time.sleep(3)
            _init_cookie()
    if frame is None or frame.empty:
        raise RuntimeError("no Xueqiu hot rank rows fetched")

    frame = frame.copy()
    source_inventory = validate_rank_inventory(
        frame.to_dict(orient="records"),
        task_type=HOT_RANK_XQ_TASK_TYPE,
    )
    captured_at = shanghai_now()
    batch_at = captured_at.isoformat(sep=" ", timespec="seconds")
    frame["snapshot_date"] = snapshot_date
    frame["etl_sync_at"] = captured_at
    for column in ("current", "percent", "chg", "amount", "market_capital"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("followers", "increment", "diff"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    frame = frame.replace({np.nan: None, pd.NaT: None})[[
        "snapshot_date", "rank", "stock_code", "short_name", "current",
        "percent", "chg", "amount", "market_capital", "followers", "sector",
        "exchange", "increment", "diff", "etl_sync_at",
    ]]

    replace_table_rows(
        frame,
        "st_hot_rank_xq",
        engine,
        where_sql="snapshot_date = :d",
        params={"d": snapshot_date},
        chunksize=500,
    )
    persisted = _readback_hot_rank(engine, snapshot_date)
    persisted_inventory = validate_rank_inventory(
        persisted,
        task_type=HOT_RANK_XQ_TASK_TYPE,
        target_date=snapshot_date,
    )
    if (
        persisted_inventory["provider_payload_sha256"]
        != source_inventory["provider_payload_sha256"]
        or batch_timestamp(persisted) != batch_at
    ):
        raise RuntimeError("persisted Xueqiu hot-rank batch differs from provider response")

    receipt = build_pass_receipt(
        task_type=HOT_RANK_XQ_TASK_TYPE,
        provider=HOT_RANK_CURRENT_PROVIDERS[HOT_RANK_XQ_TASK_TYPE],
        source_capability=CURRENT_SNAPSHOT_ONLY,
        requested_date=snapshot_date,
        started_at=started_at,
        captured_at=captured_at,
        published_at=shanghai_now(),
        batch_at=batch_at,
        inventory=persisted_inventory,
    )
    print(
        f"写入完成: st_hot_rank_xq, 共 {len(frame)} 行, "
        f"快照日期: {snapshot_date}"
    )
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description="雪球当前交易日热股Top100同步")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}", file=sys.stderr)
        return 1

    started_at = shanghai_now()
    try:
        result = fetch_hot_rank_xq(args.date, now=started_at)
    except HotRankDataBlocked as exc:
        result = build_blocked_receipt(
            task_type=HOT_RANK_XQ_TASK_TYPE,
            requested_date=args.date,
            started_at=started_at,
            reason=str(exc),
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        print(f"Xueqiu hot rank sync blocked: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Xueqiu hot rank sync failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
