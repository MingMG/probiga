#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东财概念板块实时行情爬取
========================
从 push2delay 批量接口获取全市场东财概念板块行情数据。

用法:
  python tools/crawl_concept_east_current.py
  python tools/crawl_concept_east_current.py --dry-run
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3
from sqlalchemy import create_engine, text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import replace_table_rows
from tools.env_config import create_tool_engine

BATCH_API = "https://push2delay.eastmoney.com/api/qt/clist/get"
PAGE_SIZE = 100
MAX_PAGES = 200
FILTER = "m:90+t:3"  # 东财概念板块
FIELDS = "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17"


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
        "Referer": "https://data.eastmoney.com/",
    })
    s.trust_env = False
    s.verify = False
    return s


def safe_float(val) -> float:
    if val is None or val == "-" or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _fetch_page(session: requests.Session, page_number: int) -> dict:
    params = {
        "fid": "f3", "po": "1",
        "pz": str(PAGE_SIZE), "pn": str(page_number), "np": "1",
        "fltt": "2", "invt": "2",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "fs": FILTER,
        "fields": FIELDS,
    }
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            resp = session.get(BATCH_API, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise ValueError("response JSON is not an object")
            return payload
        except Exception as exc:  # noqa: BLE001 - bounded remote retry
            last_error = exc
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(
        f"Eastmoney concept page {page_number} failed after retries"
    ) from last_error


def fetch_all(session: requests.Session) -> pd.DataFrame:
    """Fetch and prove a complete Eastmoney paginated concept snapshot."""

    all_items: list[dict] = []
    expected_total: int | None = None
    expected_pages: int | None = None
    page_number = 1
    while True:
        if page_number > MAX_PAGES:
            raise RuntimeError(
                f"Eastmoney concept snapshot exceeds the safety page limit {MAX_PAGES}"
            )
        payload = _fetch_page(session, page_number)
        data = payload.get("data")
        if not isinstance(data, dict) or "total" not in data:
            raise RuntimeError(
                f"Eastmoney concept page {page_number} omitted total-count evidence"
            )
        try:
            page_total = int(data["total"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Eastmoney concept total count is invalid") from exc
        if page_total <= 0:
            raise RuntimeError(
                "Eastmoney concept source declared an empty snapshot; preserving previous rows"
            )
        if expected_total is None:
            expected_total = page_total
            expected_pages = (expected_total + PAGE_SIZE - 1) // PAGE_SIZE
            if expected_pages > MAX_PAGES:
                raise RuntimeError(
                    f"Eastmoney concept snapshot needs {expected_pages} pages, "
                    f"above safety limit {MAX_PAGES}; preserving previous rows"
                )
        elif page_total != expected_total:
            raise RuntimeError(
                f"Eastmoney concept total changed during pagination: "
                f"expected={expected_total} observed={page_total}"
            )
        diff = data.get("diff")
        if not isinstance(diff, list) or not diff:
            raise RuntimeError(
                f"Eastmoney concept page {page_number}/{expected_pages} is empty or invalid; "
                "preserving previous rows"
            )
        all_items.extend(diff)
        assert expected_pages is not None
        if page_number >= expected_pages:
            break
        if len(diff) != PAGE_SIZE:
            raise RuntimeError(
                f"Eastmoney concept page {page_number}/{expected_pages} ended early: "
                f"rows={len(diff)} expected={PAGE_SIZE}"
            )
        page_number += 1
        time.sleep(0.2)

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in all_items:
        code = item.get("f12", "")
        if not code:
            continue
        rows.append({
            "index_code": code,
            "trade_time": now,
            "trade_date": today,
            "open": safe_float(item.get("f17")),
            "price": safe_float(item.get("f2")),
            "high": safe_float(item.get("f15")),
            "low": safe_float(item.get("f16")),
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "change": safe_float(item.get("f4")),
            "change_pct": safe_float(item.get("f3")),
            "snapshot_at": now,
            "etl_sync_at": now,
        })

    df = pd.DataFrame(rows)
    observed_codes = set(df.get("index_code", pd.Series(dtype=str)).astype(str))
    if expected_total is None or len(rows) != expected_total or len(observed_codes) != expected_total:
        raise RuntimeError(
            "Eastmoney concept code coverage is incomplete: "
            f"source={BATCH_API} expected={expected_total} rows={len(rows)} "
            f"unique_codes={len(observed_codes)}; preserving previous rows"
        )
    df.attrs["snapshot_evidence"] = {
        "complete": True,
        "source": BATCH_API,
        "filter": FILTER,
        "expected_total": expected_total,
        "pages_expected": expected_pages,
        "pages_fetched": page_number,
        "code_count": len(observed_codes),
        "snapshot_date": today,
    }
    return df


def save_to_db(engine, df: pd.DataFrame):
    if df is None or df.empty:
        raise ValueError("empty Eastmoney concept snapshot cannot be published")
    evidence = df.attrs.get("snapshot_evidence")
    if not isinstance(evidence, dict) or evidence.get("complete") is not True:
        raise ValueError("Eastmoney concept snapshot has no completeness evidence")
    observed_codes = set(df["index_code"].dropna().astype(str))
    observed_dates = set(df["trade_date"].dropna().astype(str).str.slice(0, 10))
    if (
        evidence.get("source") != BATCH_API
        or evidence.get("filter") != FILTER
        or int(evidence.get("expected_total", -1)) != len(observed_codes)
        or int(evidence.get("expected_total", -1)) != len(df)
        or int(evidence.get("code_count", -1)) != len(observed_codes)
        or int(evidence.get("pages_expected", -1)) != int(evidence.get("pages_fetched", -2))
        or observed_dates != {str(evidence.get("snapshot_date"))[:10]}
    ):
        raise ValueError("Eastmoney concept snapshot evidence does not match its rows")
    df = df.replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    replace_table_rows(
        df,
        "sm_concept_east_current",
        engine,
        chunksize=500,
        method="multi",
    )


def main():
    parser = argparse.ArgumentParser(description="东财概念板块实时行情")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("date", nargs="?", default=None, help=argparse.SUPPRESS)  # 兼容调度器传入日期
    args = parser.parse_args()

    engine = create_tool_engine()
    session = make_session()

    print(f"Fetching concept east current...", flush=True)
    df = fetch_all(session)

    if df.empty:
        print("No data fetched!")
        return

    print(f"Got {len(df)} concepts", flush=True)

    # 统计
    positive = (df["change_pct"] > 0).sum()
    negative = (df["change_pct"] < 0).sum()
    print(f"  Up: {positive}, Down: {negative}")

    top5 = df.nlargest(5, "change_pct")
    print(f"  Top 5:")
    for _, r in top5.iterrows():
        print(f"    {r['index_code']}: {r['change_pct']:+.2f}%")

    if args.dry_run:
        print("  --dry-run, not saving")
        return

    save_to_db(engine, df)
    print(f"  Saved {len(df)} rows to sm_concept_east_current")


if __name__ == "__main__":
    main()
