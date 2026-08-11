#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘中批量数据刷新
================
一次运行刷新：行情快照、资金流向、概念行情、指数行情。
用 push2delay 批量接口，全市场一次拿完。

用法:
  python tools/crawl_realtime_batch.py           # 刷新全部
  python tools/crawl_realtime_batch.py --only snapshot
  python tools/crawl_realtime_batch.py --only flow
  python tools/crawl_realtime_batch.py --only concept
  python tools/crawl_realtime_batch.py --only index
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3
from sqlalchemy import text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, replace_table_rows, write_frame
from server.common.kline_data import get_kline_engine
from server.common.mysql_lock import mysql_named_lock
from biz.stock_market.realtime_quotes import _ensure_rt_snapshot_table
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.safe_upsert import safe_upsert_rows

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://data.eastmoney.com/",
})
SESSION.trust_env = False
SESSION.verify = False

BATCH_API = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_A_SHARE_FS = (
    "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,"
    "m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2,"
    # Eastmoney moved the current BSE equity universe (including 920xxx)
    # behind this board selector. The canonical mapper below removes 810xxx
    # convertible bonds and any other non-equity codes returned by the board.
    "m:0+t:81+s:2048"
)


def _upsert_current_rows(engine, table_name: str, df: pd.DataFrame, key_columns: list[str]) -> int:
    if df is None or df.empty:
        return 0
    batch_id = f"{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    result = safe_upsert_rows(
        engine,
        table_name=table_name,
        rows=df.to_dict(orient="records"),
        key_columns=key_columns,
        batch_id=batch_id,
    )
    return int(result.accepted_rows)


def _replace_stock_current_snapshot(engine, df: pd.DataFrame) -> int:
    """Atomically replace the current quote table after the full batch is ready."""
    if df is None or df.empty:
        raise ValueError("sm_stock_current snapshot must not be empty")

    stage_table = "sm_stock_current_stage"
    backup_table = "sm_stock_current_backup"
    lock_timeout = max(0, int(os.environ.get("CURRENT_SNAPSHOT_LOCK_TIMEOUT", "0")))
    with mysql_named_lock(
        engine,
        "probiga:stock_current",
        timeout_seconds=lock_timeout,
    ):
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
            conn.execute(text(f"CREATE TABLE {stage_table} LIKE sm_stock_current"))

        try:
            write_frame(
                df,
                stage_table,
                engine,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
            with engine.connect() as conn:
                staged_count = int(
                    conn.execute(text(f"SELECT COUNT(*) FROM {stage_table}")).scalar() or 0
                )
            if staged_count != len(df):
                raise RuntimeError(
                    f"sm_stock_current staging row mismatch: expected={len(df)} actual={staged_count}"
                )

            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
                conn.execute(
                    text(
                        f"RENAME TABLE sm_stock_current TO {backup_table}, "
                        f"{stage_table} TO sm_stock_current"
                    )
                )
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
        except Exception:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
            raise
    return int(len(df))


def _replace_index_current_snapshot(engine, df: pd.DataFrame) -> int:
    """Atomically replace the current index quote table after validation."""
    if df is None or df.empty:
        raise ValueError("sm_index_current snapshot must not be empty")

    stage_table = "sm_index_current_stage"
    backup_table = "sm_index_current_backup"
    lock_timeout = max(0, int(os.environ.get("INDEX_SNAPSHOT_LOCK_TIMEOUT", "0")))
    with mysql_named_lock(engine, "probiga:index_current", timeout_seconds=lock_timeout):
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
            conn.execute(text(f"CREATE TABLE {stage_table} LIKE sm_index_current"))
        try:
            write_frame(
                df,
                stage_table,
                engine,
                if_exists="append",
                index=False,
                chunksize=500,
                method="multi",
            )
            with engine.connect() as conn:
                staged_count = int(
                    conn.execute(text(f"SELECT COUNT(*) FROM {stage_table}")).scalar() or 0
                )
            if staged_count != len(df):
                raise RuntimeError(
                    f"sm_index_current staging row mismatch: expected={len(df)} actual={staged_count}"
                )
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
                conn.execute(
                    text(
                        f"RENAME TABLE sm_index_current TO {backup_table}, "
                        f"{stage_table} TO sm_index_current"
                    )
                )
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
        except Exception:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
            raise
    return int(len(df))


def _warn(message: str, exc: Exception) -> None:
    print(f"[WARN] {message}: {exc}", file=sys.stderr)


def _is_trade_day(engine, day: date | None = None) -> bool:
    day = day or date.today()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM si_trade_calendar
                    WHERE trade_date = :d
                      AND trade_status = 1
                    """
                ),
                {"d": day.isoformat()},
            ).scalar()
        return bool(row)
    except Exception as exc:
        _warn("failed to read trade calendar, falling back to weekday", exc)
        return day.weekday() < 5


def is_trading_time(engine, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if not _is_trade_day(engine, now.date()):
        return False
    current = now.hour * 100 + now.minute
    return (925 <= current <= 1135) or (1255 <= current <= 1505)


def _latest_stock_universe_count(engine) -> int:
    return len(_read_stock_pool_codes(engine))


def _required_current_coverage(requested: float) -> float:
    """Never allow an ad-hoc low floor to publish a partial current snapshot."""
    try:
        configured = float(os.environ.get("CURRENT_MIN_COVERAGE", "0.98"))
    except (TypeError, ValueError):
        configured = 0.98
    return min(1.0, max(0.0, float(requested), configured))


def _read_stock_pool_codes(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code FROM si_all_code")).fetchall()
    return {
        code
        for row in rows
        if (code := _normalize_a_share_code(row[0]))
    }


def _read_target_stock_codes(engine, trade_date: str) -> set[str]:
    """Read the exact daily-K universe through the production history route."""
    try:
        with get_kline_engine().connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT stock_code
                    FROM sm_stock_kline
                    WHERE trade_date = (
                        SELECT MAX(trade_date)
                        FROM sm_stock_kline
                        WHERE trade_date <= :d
                          AND k_type = 1
                          AND adjust_type = 0
                    )
                      AND k_type = 1
                      AND adjust_type = 0
                    """
                ),
                {"d": trade_date},
            ).fetchall()
        codes = {
            code
            for row in rows
            if (code := _normalize_a_share_code(row[0]))
        }
        if codes:
            return codes
    except Exception as exc:
        _warn("failed to read routed daily-K universe; using current stock pool", exc)
    return _read_stock_pool_codes(engine)


def _latest_open_trade_date(engine) -> str:
    try:
        with engine.connect() as conn:
            value = conn.execute(
                text(
                    """
                    SELECT MAX(trade_date)
                    FROM si_trade_calendar
                    WHERE trade_status = 1
                      AND trade_date <= CURDATE()
                    """
                )
            ).scalar()
        if value is not None:
            return str(value)[:10]
    except Exception as exc:
        _warn("failed to read latest open trade date, falling back to today", exc)
    return date.today().isoformat()


def _latest_index_universe_count(engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text("SELECT COUNT(DISTINCT index_code) FROM si_all_index_code")).scalar() or 0)


def safe_float(val) -> float:
    if val is None or val == "-" or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _normalize_a_share_code(value) -> str:
    raw = str(value or "").strip()
    if not raw.isdigit():
        return ""
    code = raw.zfill(6)
    if len(code) != 6 or not to_qmt_symbol(code):
        return ""
    return code


def _quote_values(item: dict) -> dict[str, float]:
    """Normalize suspended/partially populated quote rows without inventing zeros."""
    raw_price = safe_float(item.get("f2"))
    pre_close = safe_float(item.get("f18"))
    price = raw_price if raw_price > 0 else pre_close
    change = safe_float(item.get("f4")) if raw_price > 0 else 0.0
    change_pct = safe_float(item.get("f3")) if raw_price > 0 else 0.0
    open_price = safe_float(item.get("f17")) or price
    raw_high = safe_float(item.get("f15"))
    raw_low = safe_float(item.get("f16"))
    positive = [value for value in (price, open_price, raw_high, raw_low) if value > 0]
    return {
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "open": open_price,
        "high": max(positive) if positive else 0.0,
        "low": min(positive) if positive else 0.0,
    }


def fetch_batch(fs: str, fields: str, page_size: int = 100) -> list[dict]:
    """分页获取批量数据"""
    all_items = []
    for pn in range(1, 200):
        params = {
            # Sort by the immutable security code.  Sorting by live change
            # percentage makes rows move between pages during the crawl,
            # which creates duplicates and silently drops valid securities.
            "fid": "f12", "po": "1",
            "pz": str(page_size), "pn": str(pn), "np": "1",
            "fltt": "2", "invt": "2",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fs": fs,
            "fields": fields,
        }
        for attempt in range(2):
            try:
                resp = SESSION.get(BATCH_API, params=params, timeout=15)
                data = resp.json()
                diff = (data.get("data") or {}).get("diff")
                if diff is not None:
                    break
            except Exception as exc:
                if attempt == 0:
                    time.sleep(1)
                else:
                    _warn(f"failed to fetch Eastmoney batch page {pn}", exc)
                    diff = None
        if not diff:
            break
        all_items.extend(diff)
        if len(diff) < page_size:
            break
        time.sleep(0.1)
    return all_items


def refresh_snapshot(
    engine,
    *,
    min_coverage: float = 0.0,
    archive_snapshot: bool = False,
) -> int:
    """刷新个股行情快照 sm_stock_current"""
    items = fetch_batch(
        EASTMONEY_A_SHARE_FS,
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18"
    )
    if not items:
        if min_coverage > 0:
            raise RuntimeError("sm_stock_current source returned no rows")
        return 0

    now = datetime.now().replace(microsecond=0)
    batch_id = f"eastmoney_realtime_{now.strftime('%Y%m%d%H%M%S')}"
    target_codes = _read_target_stock_codes(
        engine,
        _latest_open_trade_date(engine),
    )
    rows = []
    for item in items:
        code = _normalize_a_share_code(item.get("f12"))
        if not code or (target_codes and code not in target_codes):
            continue
        quote = _quote_values(item)
        if quote["price"] <= 0:
            continue
        rows.append({
            "stock_code": code,
            "short_name": str(item.get("f14", "")),
            "price": quote["price"],
            "change": quote["change"],
            "change_pct": quote["change_pct"],
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "snapshot_at": now,
            "source_time": now,
            "received_at": now,
            "data_source": "eastmoney",
            "batch_id": batch_id,
            "quality_status": "VALIDATED",
            "permission_status": "PUBLIC",
        })

    if not rows:
        if min_coverage > 0:
            raise RuntimeError("sm_stock_current source returned no valid rows")
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code"], keep="last")

    expected = len(target_codes) or _latest_stock_universe_count(engine)
    if min_coverage > 0 and expected <= 0:
        raise RuntimeError("unable to determine stock universe for coverage validation")
    required_coverage = _required_current_coverage(min_coverage)
    coverage = len(df) / max(expected, 1)
    if required_coverage > 0 and coverage < required_coverage:
        raise RuntimeError(
            f"sm_stock_current coverage below threshold: "
            f"{len(df)}/{expected} ({coverage:.1%}) < {required_coverage:.1%}"
        )

    bad_price = int((pd.to_numeric(df["price"], errors="coerce") <= 0).sum())
    bad_amount = int(
        (pd.to_numeric(df["volume"], errors="coerce") < 0).fillna(False).sum()
        + (pd.to_numeric(df["amount"], errors="coerce") < 0).fillna(False).sum()
    )
    if bad_price or bad_amount:
        raise RuntimeError(
            f"sm_stock_current source returned invalid values: "
            f"bad_price={bad_price}, bad_volume_amount={bad_amount}"
        )

    df["etl_sync_at"] = now
    written = _replace_stock_current_snapshot(engine, df)
    if archive_snapshot:
        _ensure_rt_snapshot_table(engine)
        archive_cols = [
            "stock_code",
            "short_name",
            "price",
            "change",
            "change_pct",
            "volume",
            "amount",
            "snapshot_at",
        ]
        write_frame(
            df[archive_cols],
            "sm_rt_quote_snapshot",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
    return written


def refresh_flow(
    engine,
    *,
    trade_date: str | None = None,
    min_coverage: float = 0.0,
) -> int:
    """刷新资金流向 sm_stock_capital_flow_daily（今天的数据）"""
    items = fetch_batch(
        EASTMONEY_A_SHARE_FS,
        "f12,f14,f62,f66,f72,f78,f84"
    )
    if not items:
        if min_coverage > 0:
            raise RuntimeError("sm_stock_capital_flow_daily source returned no rows")
        return 0

    today = (trade_date or _latest_open_trade_date(engine)).strip()
    target_codes = _read_target_stock_codes(engine, today)
    now = datetime.now().replace(microsecond=0)
    rows = []
    for item in items:
        code = _normalize_a_share_code(item.get("f12"))
        if not code or (target_codes and code not in target_codes):
            continue
        rows.append({
            "stock_code": code,
            "trade_date": today,
            "main_net_inflow": safe_float(item.get("f62")),
            "sm_net_inflow": safe_float(item.get("f84")),
            "mid_net_inflow": safe_float(item.get("f78")),
            "lg_net_inflow": safe_float(item.get("f72")),
            "max_net_inflow": safe_float(item.get("f66")),
            "data_source": "east_batch",
        })

    if not rows:
        if min_coverage > 0:
            raise RuntimeError("sm_stock_capital_flow_daily source returned no valid rows")
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code"], keep="last")

    expected = len(target_codes) or _latest_stock_universe_count(engine)
    if min_coverage > 0 and expected <= 0:
        raise RuntimeError("unable to determine stock universe for coverage validation")
    coverage = len(df) / max(expected, 1)
    if min_coverage > 0 and coverage < min_coverage:
        raise RuntimeError(
            f"sm_stock_capital_flow_daily coverage below threshold: "
            f"{len(df)}/{expected} ({coverage:.1%}) < {min_coverage:.1%}"
        )

    df["etl_sync_at"] = now
    lock_timeout = max(0, int(os.environ.get("FLOW_DAILY_LOCK_TIMEOUT", "30")))
    with mysql_named_lock(
        engine,
        "probiga:capital_flow_daily",
        timeout_seconds=lock_timeout,
    ):
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM sm_stock_capital_flow_daily WHERE trade_date = :d"),
                {"d": today},
            )
            write_frame(
                df,
                "sm_stock_capital_flow_daily",
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
    return len(df)


def refresh_concept_east(engine) -> int:
    """刷新东财概念行情 sm_concept_east_current"""
    items = fetch_batch(
        "m:90+t:3",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18"
    )
    min_rows = int(os.environ.get("CONCEPT_EAST_REALTIME_MIN_ROWS", "100"))
    if not items:
        raise RuntimeError("sm_concept_east_current source returned no rows")

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in items:
        code = item.get("f12", "")
        if not code:
            continue
        quote = _quote_values(item)
        if quote["price"] <= 0:
            continue
        rows.append({
            "index_code": code,
            "trade_time": now,
            "trade_date": today,
            "open": quote["open"],
            "price": quote["price"],
            "high": quote["high"],
            "low": quote["low"],
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "change": quote["change"],
            "change_pct": quote["change_pct"],
            "snapshot_at": now,
        })

    if not rows or len(rows) < min_rows:
        raise RuntimeError(
            f"sm_concept_east_current returned too few valid rows: {len(rows)} < {min_rows}"
        )

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    df["etl_sync_at"] = now
    return replace_table_rows(
        df,
        "sm_concept_east_current",
        engine,
        chunksize=500,
    )


def refresh_index(engine, *, min_coverage: float = 0.0) -> int:
    """刷新指数行情 sm_index_current"""
    # 指数: 上证 m:1+t:2, 深证 m:0+t:2, 创业板 m:0+t:23, 科创 m:1+t:23
    items = fetch_batch(
        "m:1+t:2+f:!2,m:0+t:2+f:!2,m:1+t:23+f:!2,m:0+t:23+f:!2",
        "f2,f3,f4,f5,f6,f7,f8,f12,f14,f15,f16,f17,f18"
    )
    if not items:
        if min_coverage > 0:
            raise RuntimeError("sm_index_current source returned no rows")
        return 0

    now = datetime.now().replace(microsecond=0)
    today = now.strftime("%Y-%m-%d")
    rows = []
    for item in items:
        code = str(item.get("f12", "")).zfill(6)
        if not code:
            continue
        quote = _quote_values(item)
        if quote["price"] <= 0:
            continue
        rows.append({
            "index_code": code,
            "trade_time": now,
            "trade_date": today,
            "open": quote["open"],
            "price": quote["price"],
            "high": quote["high"],
            "low": quote["low"],
            "volume": safe_float(item.get("f5")),
            "amount": safe_float(item.get("f6")),
            "change": quote["change"],
            "change_pct": quote["change_pct"],
            "snapshot_at": now,
        })

    if not rows:
        if min_coverage > 0:
            raise RuntimeError("sm_index_current source returned no valid rows")
        return 0

    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["index_code"], keep="last")

    expected = _latest_index_universe_count(engine)
    if min_coverage > 0 and expected <= 0:
        raise RuntimeError("unable to determine index universe for coverage validation")
    coverage = len(df) / max(expected, 1)
    if min_coverage > 0 and coverage < min_coverage:
        raise RuntimeError(
            f"sm_index_current coverage below threshold: "
            f"{len(df)}/{expected} ({coverage:.1%}) < {min_coverage:.1%}"
        )

    df["etl_sync_at"] = now
    return _replace_index_current_snapshot(engine, df)


def main():
    parser = argparse.ArgumentParser(description="盘中批量数据刷新")
    parser.add_argument("--only", choices=["snapshot", "flow", "concept", "index", "all"],
                        default="all")
    parser.add_argument("--min-coverage", type=float, default=0.0)
    parser.add_argument("--archive-snapshot", action="store_true")
    parser.add_argument("--skip-closed", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--trade-date", default="")
    args = parser.parse_args()

    engine = create_batch_engine()
    if args.skip_closed and not is_trading_time(engine):
        result = {
            "status": "skipped",
            "reason": "market_closed",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
        if args.json:
            print(json.dumps(result, ensure_ascii=False, default=str))
        else:
            print(f"  skipped: {result['reason']}", flush=True)
        return 0
    t0 = time.time()
    results = {}

    if args.only in ("snapshot", "all"):
        n = refresh_snapshot(
            engine,
            min_coverage=args.min_coverage,
            archive_snapshot=args.archive_snapshot,
        )
        results["snapshot"] = n
        if not args.json:
            print(f"  snapshot: {n} stocks", flush=True)

    if args.only in ("flow", "all"):
        n = refresh_flow(
            engine,
            trade_date=args.trade_date.strip() or None,
            min_coverage=args.min_coverage,
        )
        results["flow"] = n
        if not args.json:
            print(f"  flow: {n} stocks", flush=True)

    if args.only in ("concept", "all"):
        n = refresh_concept_east(engine)
        results["concept"] = n
        if not args.json:
            print(f"  concept_east: {n}", flush=True)

    if args.only in ("index", "all"):
        n = refresh_index(engine, min_coverage=args.min_coverage)
        results["index"] = n
        if not args.json:
            print(f"  index: {n}", flush=True)

    elapsed = time.time() - t0
    result = {
        "status": "success",
        "results": results,
        "elapsed_seconds": round(elapsed, 1),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(f"  Done in {elapsed:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
