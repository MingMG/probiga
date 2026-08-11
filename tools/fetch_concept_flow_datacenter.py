#!/usr/bin/env python3
"""从 datacenter-web.eastmoney.com 获取概念资金流向，写入 sm_concept_capital_flow_east。

push2.eastmoney.com 被服务器 IP 封了，改用 datacenter-web API。
"""

import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
REPORT_NAME = "RPT_CONCEPT_FUNDFLOW"

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://data.eastmoney.com/",
})

_FIELD_MAP = {
    "BOARD_CODE": "index_code",
    "BOARD_NAME": "index_name",
    "CHANGE_RATE": "change_pct",
    "SUPERDEAL_NET": "max_net_inflow",
    "SUPERDEAL_NET_RATIO": "max_net_inflow_rate",
    "BIGDEAL_NET": "lg_net_inflow",
    "BIGDEAL_NET_RATIO": "lg_net_inflow_rate",
    "MIDDEAL_NET": "mid_net_inflow",
    "MIDDEAL_NET_RATIO": "mid_net_inflow_rate",
    "SMALLDEAL_NET": "sm_net_inflow",
    "SMALLDEAL_NET_RATIO": "sm_net_inflow_rate",
    "MAX_NETINFLOW_SEC": "stock_name",
}


from server.common.batch_db import create_batch_engine


def _fetch_page(date_str: str, page: int, page_size: int = 500) -> dict | None:
    date_filter = f"(TRADE_DATE='{date_str}')"
    params = {
        "reportName": REPORT_NAME,
        "columns": "ALL",
        "pageNumber": page,
        "pageSize": page_size,
        "sortTypes": "-1",
        "sortColumns": "NET_INFLOW",
        "filter": date_filter,
        "source": "WEB",
        "client": "WEB",
    }
    for attempt in range(3):
        try:
            r = _SESSION.get(API_URL, params=params, timeout=20)
            r.raise_for_status()
            j = r.json()
            if j.get("success") and j.get("result"):
                return j["result"]
            return None
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
    return None


def _fetch_all_for_date(date_str: str) -> pd.DataFrame:
    result = _fetch_page(date_str, 1)
    if not result or not result.get("data"):
        return pd.DataFrame()

    all_rows = list(result["data"])
    total_pages = result.get("pages", 1)
    for page in range(2, total_pages + 1):
        time.sleep(0.15)
        result = _fetch_page(date_str, page)
        if not result or not result.get("data"):
            break
        all_rows.extend(result["data"])

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    out = pd.DataFrame()
    for src, dst in _FIELD_MAP.items():
        if src in df.columns:
            out[dst] = df[src]

    out["main_net_inflow"] = (
        pd.to_numeric(out.get("max_net_inflow", 0), errors="coerce").fillna(0)
        + pd.to_numeric(out.get("lg_net_inflow", 0), errors="coerce").fillna(0)
    )
    out["main_net_inflow_rate"] = (
        pd.to_numeric(out.get("max_net_inflow_rate", 0), errors="coerce").fillna(0)
        + pd.to_numeric(out.get("lg_net_inflow_rate", 0), errors="coerce").fillna(0)
    )

    num_cols = [
        "change_pct", "main_net_inflow", "main_net_inflow_rate",
        "max_net_inflow", "max_net_inflow_rate",
        "lg_net_inflow", "lg_net_inflow_rate",
        "mid_net_inflow", "mid_net_inflow_rate",
        "sm_net_inflow", "sm_net_inflow_rate",
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out["stock_code"] = ""
    return out


def _lookup_stock_codes(engine, names: list[str]) -> dict[str, str]:
    if not names:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT short_name, stock_code FROM si_all_code WHERE short_name IN :names"),
            {"names": tuple(names)},
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def fetch_concept_flow():
    engine = create_batch_engine()

    print("开始获取概念资金流向 (datacenter-web)")

    today = datetime.now().strftime("%Y-%m-%d")
    df = _fetch_all_for_date(today)

    if df.empty:
        print(f"  今日 {today} 无数据，尝试昨日")
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        df = _fetch_all_for_date(yesterday)
        if df.empty:
            print("  昨日也无数据，再往前一天")
            day_before = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            df = _fetch_all_for_date(day_before)

    if df.empty:
        print("未获取到概念资金流向数据")
        return

    print(f"  获取到 {len(df)} 条概念资金流向数据")

    stock_names = [n for n in df["stock_name"].dropna().unique() if n]
    name_to_code = _lookup_stock_codes(engine, stock_names)
    df["stock_code"] = df["stock_name"].map(lambda n: name_to_code.get(n, ""))

    now = datetime.now().replace(microsecond=0)
    df["days_type"] = 1
    df["snapshot_at"] = now
    df["etl_sync_at"] = now

    out_cols = [
        "days_type", "index_code", "index_name", "change_pct",
        "main_net_inflow", "main_net_inflow_rate",
        "max_net_inflow", "max_net_inflow_rate",
        "lg_net_inflow", "lg_net_inflow_rate",
        "mid_net_inflow", "mid_net_inflow_rate",
        "sm_net_inflow", "sm_net_inflow_rate",
        "stock_code", "stock_name",
        "snapshot_at", "etl_sync_at",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = None

    df = df[out_cols].replace({np.nan: None, pd.NaT: None})

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE sm_concept_capital_flow_east"))

    df.to_sql("sm_concept_capital_flow_east", engine, if_exists="append", index=False,
              chunksize=500, method="multi")

    print(f"写入完成: sm_concept_capital_flow_east, 共 {len(df)} 行")


def main():
    fetch_concept_flow()


if __name__ == "__main__":
    main()
