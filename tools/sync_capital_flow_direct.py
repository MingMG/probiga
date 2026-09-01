#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接调用东财 API 同步个股日度资金流向，绕过 adata 限流。

用法：
  python tools/sync_capital_flow_direct.py
  python tools/sync_capital_flow_direct.py --limit 100
  python tools/sync_capital_flow_direct.py --date 2026-05-29

环境变量：MYSQL_URL（必须显式配置；也可使用 DATABASE_URL）
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import replace_table_rows_exact_keys
from server.common.mysql_lock import CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME
from tools.env_config import create_tool_engine


def _engine():
    return create_tool_engine()


def fetch_flow_east(
    stock_code: str,
    *,
    attempts: int = 3,
    timeout_seconds: float = 15,
) -> pd.DataFrame | None:
    """直接调东财 push2his API 获取个股日度资金流（最近120天）"""
    cid = 1 if stock_code.startswith("6") else 0
    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        f"lmt=0&klt=101&fields1=f1,f2,f3,f7&"
        f"fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&"
        f"secid={cid}.{stock_code}"
    )
    for attempt in range(1, max(int(attempts), 1) + 1):
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 ProBigA capital-flow repair"},
                timeout=float(timeout_seconds),
            )
            resp.raise_for_status()
            data = resp.json().get("data")
            if not data or "klines" not in data:
                return None
            lines = data["klines"]
            if not lines:
                return None
            # 格式: '2026-05-29,-58234405.0,47874618.0,10359788.0,-13362003.0,-44872402.0,...'
            rows = []
            for line in lines:
                parts = line.split(",")
                if len(parts) >= 6:
                    rows.append(
                        [
                            stock_code,
                            parts[0],
                            parts[1],
                            parts[2],
                            parts[3],
                            parts[4],
                            parts[5],
                        ]
                    )
            if not rows:
                return None
            df = pd.DataFrame(
                rows,
                columns=[
                    "stock_code",
                    "trade_date",
                    "main_net_inflow",
                    "max_net_inflow",
                    "lg_net_inflow",
                    "mid_net_inflow",
                    "sm_net_inflow",
                ],
            )
            for col in [
                "main_net_inflow",
                "max_net_inflow",
                "lg_net_inflow",
                "mid_net_inflow",
                "sm_net_inflow",
            ]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        except (requests.RequestException, ValueError):
            if attempt < max(int(attempts), 1):
                time.sleep(min(0.5 * (2 ** (attempt - 1)), 2.0))
    return None


def replace_flow_partitions(engine, df: pd.DataFrame, stock_code: str) -> int:
    """Atomically replace only fetched code/date partitions."""

    if df is None or df.empty:
        return 0
    frame = df.copy()
    frame["stock_code"] = str(stock_code).strip().zfill(6)
    frame["trade_date"] = pd.to_datetime(
        frame["trade_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    frame = frame.dropna(subset=["trade_date"]).drop_duplicates(
        subset=["stock_code", "trade_date"], keep="last"
    )
    if frame.empty:
        return 0
    if "etl_sync_at" not in frame.columns:
        frame["etl_sync_at"] = datetime.now().replace(microsecond=0)
    if "data_source" not in frame.columns:
        frame["data_source"] = "eastmoney_fflow_daykline"
    return replace_table_rows_exact_keys(
        frame,
        "sm_stock_capital_flow_daily",
        engine,
        key_columns=("stock_code", "trade_date"),
        lock_name=CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        chunksize=1000,
        method="multi",
    )


def _csv_values(values: Iterable[str]) -> list[str]:
    return list(
        dict.fromkeys(
            item.strip()
            for value in values
            for item in str(value or "").split(",")
            if item.strip()
        )
    )


def _target_keys(engine, dates: list[str], codes: set[str]) -> set[tuple[str, str]]:
    if not dates or not codes:
        return set()
    statement = text(
        "SELECT stock_code,trade_date FROM sm_stock_kline "
        "WHERE trade_date IN :dates AND k_type=1 AND adjust_type=0 "
        "AND (COALESCE(volume,0)>0 OR COALESCE(amount,0)>0)"
    ).bindparams(bindparam("dates", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(statement, {"dates": dates}).fetchall()
    return {
        (str(row[0]).strip().zfill(6), str(row[1])[:10])
        for row in rows
        if str(row[0]).strip().zfill(6) in codes
    }


def _existing_keys(engine, dates: list[str]) -> set[tuple[str, str]]:
    if not dates:
        return set()
    statement = text(
        "SELECT stock_code,trade_date FROM sm_stock_capital_flow_daily "
        "WHERE trade_date IN :dates"
    ).bindparams(bindparam("dates", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(statement, {"dates": dates}).fetchall()
    return {
        (str(row[0]).strip().zfill(6), str(row[1])[:10])
        for row in rows
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="直接调东财 API 同步资金流数据")
    p.add_argument("--limit", type=int, default=0, help="最多处理几只股票（0=全部）")
    p.add_argument("--date", type=str, default="", help="只保留指定日期的数据（YYYY-MM-DD）")
    p.add_argument(
        "--dates",
        action="append",
        default=[],
        help="逗号分隔的多个目标日期；与 --date 合并",
    )
    p.add_argument(
        "--codes",
        action="append",
        default=[],
        help="逗号分隔的精确股票代码；省略时使用沪深 A 股目录",
    )
    p.add_argument(
        "--missing-only",
        action="store_true",
        help="仅写入目标 K 线集合中尚不存在的代码/日期键",
    )
    p.add_argument("--workers", type=int, default=1, help="并行请求数")
    p.add_argument("--attempts", type=int, default=3, help="单只股票最多请求次数")
    p.add_argument("--request-timeout", type=float, default=15, help="单次请求超时秒数")
    p.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数")
    p.add_argument(
        "--skip-truncate",
        action="store_true",
        help="兼容旧参数；运行时始终按股票/日期安全替换",
    )
    args = p.parse_args(argv)

    dates = _csv_values([args.date, *args.dates])
    invalid_dates = [
        value
        for value in dates
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None
    ]
    requested_codes = _csv_values(args.codes)
    invalid_codes = [
        value
        for value in requested_codes
        if re.fullmatch(r"\d{6}", value) is None
    ]
    if invalid_dates or invalid_codes:
        p.error(f"invalid dates/codes: dates={invalid_dates}, codes={invalid_codes}")
    if args.missing_only and not dates:
        p.error("--missing-only requires --date or --dates")
    if args.workers <= 0 or args.attempts <= 0 or args.request_timeout <= 0:
        p.error("--workers/--attempts/--request-timeout must be positive")

    eng = _engine()

    # 获取股票列表
    with eng.connect() as conn:
        catalog_codes = {
            str(row[0]).strip().zfill(6)
            for row in conn.execute(
                text(
                    "SELECT stock_code FROM si_all_code "
                    "WHERE stock_code REGEXP '^(0|3|6)' ORDER BY stock_code"
                )
            ).fetchall()
        }

    if requested_codes:
        unknown_codes = sorted(set(requested_codes) - catalog_codes)
        if unknown_codes:
            print(json.dumps({"status": "DATA_BLOCKED", "unknown_codes": unknown_codes}))
            return 2
        codes = sorted(set(requested_codes))
    else:
        codes = sorted(catalog_codes)

    if args.limit > 0:
        codes = codes[:args.limit]

    expected_keys = _target_keys(eng, dates, set(codes)) if dates else set()
    if dates and not expected_keys:
        print(
            json.dumps(
                {
                    "status": "DATA_BLOCKED",
                    "reason": "target_kline_keys_empty",
                    "dates": dates,
                }
            )
        )
        return 2
    existing_before = _existing_keys(eng, dates) if dates else set()
    target_keys = expected_keys - existing_before if args.missing_only else expected_keys
    if dates:
        codes = sorted({code for code, _trade_date in target_keys})
    target_dates_by_code: dict[str, set[str]] = {}
    for target_code, target_date in target_keys:
        target_dates_by_code.setdefault(target_code, set()).add(target_date)

    print(
        f"共 {len(codes)} 只股票，目标 {len(target_keys) if dates else '历史'} 个键，"
        "开始同步资金流..."
    )

    if dates and not target_keys:
        print(
            json.dumps(
                {
                    "status": "COMPLETE",
                    "dates": dates,
                    "requested_key_count": 0,
                    "remaining_key_count": 0,
                }
            )
        )
        return 0

    total_rows = 0
    success = 0
    fail = 0

    def fetch(code: str) -> tuple[str, pd.DataFrame | None]:
        frame = fetch_flow_east(
            code,
            attempts=args.attempts,
            timeout_seconds=args.request_timeout,
        )
        if args.sleep > 0:
            time.sleep(args.sleep)
        return code, frame

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        fetches = executor.map(fetch, codes)
        for i, (code, df) in enumerate(fetches):
            code = str(code).strip().zfill(6)

            if df is not None and not df.empty:
                if dates:
                    row_dates = df["trade_date"].astype(str).str[:10]
                    allowed_dates = target_dates_by_code.get(code, set())
                    df = df[row_dates.isin(allowed_dates)]

                if not df.empty:
                    total_rows += replace_flow_partitions(eng, df, code)
                    success += 1
                else:
                    fail += 1
            else:
                fail += 1

            if (i + 1) % 100 == 0:
                print(
                    f"  进度: {i+1}/{len(codes)} | 成功: {success} | "
                    f"失败: {fail} | 写入: {total_rows} 行",
                    flush=True,
                )

    remaining_keys = expected_keys - _existing_keys(eng, dates) if dates else set()
    status = "COMPLETE" if not remaining_keys else "DATA_BLOCKED"
    receipt = {
        "status": status,
        "provider": "eastmoney_fflow_daykline",
        "dates": dates,
        "requested_code_count": len(codes),
        "requested_key_count": len(target_keys) if dates else None,
        "success_code_count": success,
        "failed_code_count": fail,
        "written_row_count": total_rows,
        "remaining_key_count": len(remaining_keys),
        "remaining_key_sample": [list(key) for key in sorted(remaining_keys)[:20]],
        "automatic_order_submission": False,
    }
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True), flush=True)
    return 0 if status == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
