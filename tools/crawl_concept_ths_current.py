#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺概念板块实时行情爬取
============================
从 d.10jqka.com.cn 逐个获取同花顺概念板块行情数据。

用法:
  python tools/crawl_concept_ths_current.py
  python tools/crawl_concept_ths_current.py --limit 10
  python tools/crawl_concept_ths_current.py --dry-run
"""

import argparse
import json
import os
import random
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

DELAY = 0.6
JITTER = 0.3
BATCH_EVERY = 50
BATCH_PAUSE = 20


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "http://q.10jqka.com.cn/",
    })
    s.trust_env = False
    s.verify = False
    return s


def get_concept_codes(engine) -> list[dict]:
    """获取同花顺概念代码列表"""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT index_code, name FROM si_concept_code_ths ORDER BY index_code"
        )).fetchall()
    return [{"index_code": str(r[0]), "name": str(r[1])} for r in rows]


def fetch_concept(session: requests.Session, index_code: str) -> dict | None:
    """
    获取单个概念板块的当日行情。
    用 last5.js 获取最近5天数据，从中提取今天和昨天的收盘价计算涨跌。
    """
    url = f"http://d.10jqka.com.cn/v6/line/48_{index_code}/01/last5.js"
    resp = session.get(url, timeout=10)
    text = resp.text

    if not text or "{" not in text:
        return None

    json_str = text[text.index("{") : text.rindex("}") + 1]
    data = json.loads(json_str)

    # 解析 kline 数据
    kline_data = data.get("data", "")
    if not kline_data:
        return None

    days = kline_data.split(";")
    parsed = []
    for day in days:
        parts = day.split(",")
        if len(parts) >= 7:
            parsed.append({
                "date": parts[0],
                "open": float(parts[1]),
                "high": float(parts[2]),
                "low": float(parts[3]),
                "close": float(parts[4]),
                "volume": float(parts[5]),
                "amount": float(parts[6]),
            })

    if not parsed:
        return None

    today = parsed[-1]
    pre_close = parsed[-2]["close"] if len(parsed) >= 2 else None

    change = today["close"] - pre_close if pre_close else None
    change_pct = (change / pre_close * 100) if pre_close and pre_close != 0 else None

    return {
        "index_code": index_code,
        "open": today["open"],
        "price": today["close"],
        "high": today["high"],
        "low": today["low"],
        "volume": today["volume"],
        "amount": today["amount"],
        "change": round(change, 4) if change is not None else None,
        "change_pct": round(change_pct, 4) if change_pct is not None else None,
        "trade_date": f"{today['date'][:4]}-{today['date'][4:6]}-{today['date'][6:8]}",
    }


def save_to_db(
    engine,
    rows: list[dict],
    *,
    expected_codes: list[str],
    attempted_codes: list[str],
    failed_codes: list[str],
):
    """Publish a full snapshot, or safely replace only proven code partitions."""

    if not rows:
        raise ValueError("empty THS concept snapshot cannot be published")

    df = pd.DataFrame(rows)
    required = {"index_code", "trade_date"}
    if not required.issubset(df.columns):
        raise ValueError("THS concept rows omit code/date coverage evidence")
    df["index_code"] = df["index_code"].astype(str).str.strip()
    df["trade_date"] = df["trade_date"].astype(str).str.slice(0, 10)
    expected = {str(code).strip() for code in expected_codes if str(code).strip()}
    attempted = {str(code).strip() for code in attempted_codes if str(code).strip()}
    failed = {str(code).strip() for code in failed_codes if str(code).strip()}
    observed = set(df["index_code"].dropna().unique())
    observed_dates = set(df["trade_date"].dropna().unique())
    if len(observed_dates) != 1:
        raise ValueError(
            f"THS concept snapshot has mixed or missing trade dates: {sorted(observed_dates)}"
        )
    if not attempted.issubset(expected):
        raise ValueError("THS concept attempted codes are outside the catalog")
    if observed & failed or observed | failed != attempted:
        raise ValueError(
            "THS concept shard evidence mismatch: "
            f"attempted={len(attempted)} observed={len(observed)} failed={len(failed)}"
        )
    if not observed.issubset(expected):
        raise ValueError("THS concept response contains codes outside the catalog")
    df = df.replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    now = datetime.now().replace(microsecond=0)
    df["trade_time"] = now
    df["snapshot_at"] = now
    df["etl_sync_at"] = now

    cols = [
        "index_code", "trade_time", "trade_date",
        "open", "price", "high", "low",
        "volume", "amount", "change", "change_pct",
        "snapshot_at", "etl_sync_at",
    ]
    df = df[[c for c in cols if c in df.columns]]

    complete = not failed and attempted == expected and observed == expected
    kwargs = {"chunksize": 500, "method": "multi"}
    if not complete:
        params = {
            f"index_code_{index}": code
            for index, code in enumerate(sorted(observed))
        }
        kwargs["where_sql"] = "index_code IN (" + ", ".join(
            f":{key}" for key in params
        ) + ")"
        kwargs["params"] = params
    replace_table_rows(df, "sm_concept_ths_current", engine, **kwargs)
    return "full" if complete else "partitions"


def main():
    parser = argparse.ArgumentParser(description="同花顺概念板块实时行情")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("date", nargs="?", default=None, help=argparse.SUPPRESS)  # 兼容调度器传入日期
    args = parser.parse_args()

    engine = create_tool_engine()
    all_concepts = get_concept_codes(engine)
    concepts = all_concepts

    if args.limit > 0:
        concepts = concepts[: args.limit]

    total = len(concepts)
    print(f"\n{'='*60}")
    print(f"  THS Concept Current: {total} concepts")
    print(f"{'='*60}\n")

    if args.dry_run:
        return

    session = make_session()
    rows = []
    failed_codes: list[str] = []
    ok = fail = 0
    t0 = time.time()

    for i, concept in enumerate(concepts):
        code = concept["index_code"]
        result = None
        for attempt in range(2):
            try:
                result = fetch_concept(session, code)
                if result:
                    break
            except Exception:
                if attempt == 0:
                    time.sleep(2)

        if result and str(result.get("index_code", "")).strip() == str(code).strip():
            rows.append(result)
            ok += 1
        else:
            fail += 1
            failed_codes.append(str(code))

        time.sleep(DELAY + random.uniform(0, JITTER))

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            eta = (total - i - 1) / (i + 1) * elapsed
            print(
                f"  [{i+1}/{total}] OK={ok} Fail={fail} "
                f"ETA={eta/60:.1f}min",
                flush=True,
            )

        if (i + 1) % BATCH_EVERY == 0:
            time.sleep(BATCH_PAUSE + random.uniform(0, 5))

    if not rows:
        raise RuntimeError(
            "THS concept source returned no successful code partitions; preserving previous rows"
        )
    print(f"\n  Saving {len(rows)} rows...", flush=True)
    mode = save_to_db(
        engine,
        rows,
        expected_codes=[item["index_code"] for item in all_concepts],
        attempted_codes=[item["index_code"] for item in concepts],
        failed_codes=failed_codes,
    )
    print(f"  Publication mode: {mode}", flush=True)

    elapsed = time.time() - t0
    print(f"\n  Done! OK={ok} Fail={fail} Time={elapsed/60:.1f}min\n")


if __name__ == "__main__":
    main()
