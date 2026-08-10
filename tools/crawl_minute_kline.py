#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分钟数据爬取脚本
================
从 push2delay 获取个股/指数/概念的当日分钟K线和分钟资金流向。

用法:
  python tools/crawl_minute_kline.py --type stock    # 个股分钟K线
  python tools/crawl_minute_kline.py --type index    # 指数分钟K线
  python tools/crawl_minute_kline.py --type concept  # 东财概念分钟K线
  python tools/crawl_minute_kline.py --type flow     # 分钟资金流向
  python tools/crawl_minute_kline.py --type all      # 全部
  python tools/crawl_minute_kline.py --type stock --limit 10
"""

import argparse
import os
import random
import sys
import time
import uuid
from datetime import datetime
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

from server.common.batch_db import create_batch_engine, quote_identifier, write_frame
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from server.common.mysql_lock import mysql_named_lock


def _is_trade_day(engine, day: datetime | None = None) -> bool:
    day = day or datetime.now()
    try:
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM si_trade_calendar
                    WHERE trade_date = :d
                      AND trade_status = 1
                    """
                ),
                {"d": day.strftime("%Y-%m-%d")},
            ).scalar()
        return bool(count)
    except Exception:
        return day.weekday() < 5


def is_trading_time(engine, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if not _is_trade_day(engine, now):
        return False
    current = now.hour * 100 + now.minute
    return (925 <= current <= 1135) or (1255 <= current <= 1505)


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return int(default)


DELAY = _env_float("MINUTE_REQUEST_DELAY", "0.5")
JITTER = _env_float("MINUTE_REQUEST_JITTER", "0.3")
BATCH_EVERY = _env_int("MINUTE_BATCH_EVERY", "100")
BATCH_PAUSE = _env_float("MINUTE_BATCH_PAUSE", "20")
FETCH_ATTEMPTS = _env_int("MINUTE_FETCH_ATTEMPTS", "3")
RETRY_DELAY = _env_float("MINUTE_RETRY_DELAY", "1.0")

FLOW_TABLE = "sm_stock_capital_flow_min"
FLOW_WRITE_COLUMNS = (
    "stock_code",
    "trade_time",
    "main_net_inflow",
    "max_net_inflow",
    "lg_net_inflow",
    "mid_net_inflow",
    "sm_net_inflow",
    "snapshot_at",
    "etl_sync_at",
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://quote.eastmoney.com/",
})
SESSION.trust_env = False
SESSION.verify = False


def safe_float(val) -> float:
    if val is None or val == "-" or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _market_candidates(code: str, market: int | None = None) -> list[int]:
    """Eastmoney market ids to try for a stock/index/concept code."""
    if market == 90 or str(code).upper().startswith("BK"):
        return [90]

    candidates: list[int] = []
    if market is not None:
        candidates.append(int(market))

    code = str(code).strip()
    if code.startswith("6"):
        candidates.extend([1, 0])
    elif code.startswith(("4", "8", "92")):
        candidates.extend([2, 0, 1])
    else:
        candidates.extend([0, 1, 2])

    out: list[int] = []
    for item in candidates:
        if item not in out:
            out.append(item)
    return out


def _primary_market(code: str) -> int:
    if str(code).startswith("6"):
        return 1
    if str(code).startswith(("4", "8", "92")):
        return 2
    return 0


def fetch_minute_kline(code: str, market: int) -> list[str] | None:
    """获取分钟K线，自动尝试两个 market 值"""
    url = "https://push2delay.eastmoney.com/api/qt/stock/kline/get"
    for m in _market_candidates(code, market):
        params = {
            "secid": f"{m}.{code}",
            "klt": "1", "fqt": "1",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "lmt": "300", "end": "20500101",
        }
        try:
            resp = SESSION.get(url, params=params, timeout=10)
            data = resp.json()
            klines = (data.get("data") or {}).get("klines")
            if klines:
                return klines
        except Exception:
            pass
    return None


def fetch_minute_flow(code: str, market: int) -> list[str] | None:
    """获取分钟资金流向"""
    url = "https://push2delay.eastmoney.com/api/qt/stock/fflow/kline/get"
    for m in _market_candidates(code, market):
        params = {
            "secid": f"{m}.{code}",
            "klt": "1",
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "lmt": "300",
        }
        try:
            resp = SESSION.get(url, params=params, timeout=10)
            data = resp.json()
            klines = (data.get("data") or {}).get("klines")
            if klines:
                return klines
        except Exception:
            pass
    return None


def fetch_with_retries(fetcher, code: str, market: int) -> list[str] | None:
    """Retry one stock without turning a transient source miss into stale data."""
    attempts = max(1, int(FETCH_ATTEMPTS))
    for attempt in range(attempts):
        rows = fetcher(code, market)
        if rows:
            return rows
        if attempt < attempts - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))
    return None


def parse_kline(code: str, klines: list[str]) -> list[dict]:
    """
    解析分钟K线 → sm_stock_minute / sm_index_minute / sm_concept_east_minute
    API: datetime,open,close,high,low,volume,amount,amplitude,change_pct,change,turnover
    表: stock_code, trade_time, trade_date, price, avg_price, change, change_pct, volume, amount
    """
    rows = []
    for line in klines:
        p = line.split(",")
        if len(p) < 11:
            continue
        dt = p[0]  # "2026-06-05 09:31"
        rows.append({
            "stock_code": code,
            "trade_time": dt,
            "trade_date": dt[:10],
            "price": safe_float(p[2]),      # close
            "avg_price": None,               # API 不提供均价
            "change": safe_float(p[9]),      # 涨跌额
            "change_pct": safe_float(p[8]),  # 涨跌幅
            "volume": safe_float(p[5]) * 100,
            "amount": safe_float(p[6]),
        })
    return rows


def parse_flow(code: str, klines: list[str]) -> list[dict]:
    """
    解析分钟资金流向 → sm_stock_capital_flow_min
    API: datetime,main,sm,mid,lg,max
    表: stock_code, trade_time, main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow
    """
    rows = []
    for line in klines:
        p = line.split(",")
        if len(p) < 6:
            continue
        dt = p[0]
        rows.append({
            "stock_code": code,
            "trade_time": dt,
            "main_net_inflow": safe_float(p[1]),
            "max_net_inflow": safe_float(p[5]),
            "lg_net_inflow": safe_float(p[4]),
            "mid_net_inflow": safe_float(p[3]),
            "sm_net_inflow": safe_float(p[2]),
        })
    return rows


def get_codes(engine, table: str, code_col: str) -> list[tuple[str, int]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT {code_col} FROM {table} ORDER BY {code_col}")
        ).fetchall()
    result = []
    for r in rows:
        code = str(r[0]).strip().zfill(6)
        result.append((code, _primary_market(code)))
    return result


def get_latest_kline_stock_codes(engine, *, fallback_engine=None) -> list[tuple[str, int]]:
    """Use the latest daily-kline universe so delisted/stale codes do not dominate minute sync."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT stock_code
            FROM sm_stock_kline
            WHERE trade_date = (
                SELECT MAX(trade_date)
                FROM sm_stock_kline
                WHERE k_type = 1
            )
              AND k_type = 1
            ORDER BY stock_code
        """)).fetchall()
    if not rows:
        return get_codes(fallback_engine or engine, "si_all_code", "stock_code")
    return [(str(r[0]).strip().zfill(6), _primary_market(str(r[0]).strip())) for r in rows]


def _minute_code_column(table: str) -> str:
    return "index_code" if table in ("sm_index_minute", "sm_concept_east_minute") else "stock_code"


def save_kline(engine, rows: list[dict], table: str) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")

    code_col = _minute_code_column(table)
    date_codes = df[["stock_code", "trade_date"]].drop_duplicates().to_dict("records")
    with engine.begin() as conn:
        for item in date_codes:
            conn.execute(
                text(f"DELETE FROM {table} WHERE {code_col} = :code AND trade_date = :d"),
                {"code": item["stock_code"], "d": item["trade_date"]},
            )

    df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    # sm_index_minute / sm_concept_east_minute 用 index_code 列，有 snapshot_at
    if table in ("sm_index_minute", "sm_concept_east_minute"):
        df = df.rename(columns={"stock_code": "index_code"})
        df["snapshot_at"] = datetime.now().replace(microsecond=0)

    df.to_sql(table, engine, if_exists="append", index=False, chunksize=1000, method="multi")
    return len(df)


def _create_flow_stage(engine) -> str:
    stage = f"{FLOW_TABLE}_stage_{uuid.uuid4().hex[:12]}"
    with engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE TABLE {quote_identifier(stage)} "
                f"LIKE {quote_identifier(FLOW_TABLE)}"
            )
        )
    return stage


def _drop_flow_stage(engine, stage: str) -> None:
    if not stage:
        return
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {quote_identifier(stage)}"))


def _append_flow_stage(engine, stage: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")
    now = datetime.now().replace(microsecond=0)
    df["snapshot_at"] = now
    df["etl_sync_at"] = now
    write_frame(
        df[list(FLOW_WRITE_COLUMNS)],
        stage,
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
    return len(df)


def _publish_flow_stage(engine, stage: str, trade_date: str) -> int:
    """Replace today's minute-flow slice only after the staged run is complete."""
    columns = ", ".join(quote_identifier(column) for column in FLOW_WRITE_COLUMNS)
    lock_timeout = max(0, _env_int("FLOW_MINUTE_LOCK_TIMEOUT", "0"))
    with mysql_named_lock(
        engine,
        "probiga:capital_flow_minute",
        timeout_seconds=lock_timeout,
    ):
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"DELETE FROM {quote_identifier(FLOW_TABLE)} "
                    "WHERE trade_time >= :trade_date "
                    "AND trade_time < DATE_ADD(:trade_date, INTERVAL 1 DAY)"
                ),
                {"trade_date": trade_date},
            )
            result = conn.execute(
                text(
                    f"INSERT INTO {quote_identifier(FLOW_TABLE)} ({columns}) "
                    f"SELECT {columns} FROM {quote_identifier(stage)}"
                )
            )
    return int(result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0)


def crawl_kline(engine, codes: list[tuple[str, int]], table: str, label: str, limit: int) -> dict:
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    print(f"\n  {label}: {total} items", flush=True)

    buffer = []
    ok = fail = 0
    written_rows = 0
    t0 = time.time()

    for i, (code, market) in enumerate(codes):
        klines = fetch_minute_kline(code, market)
        rows = parse_kline(code, klines or []) if klines else []
        if rows:
            buffer.extend(rows)
            ok += 1
        else:
            fail += 1

        time.sleep(DELAY + random.uniform(0, JITTER))

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            eta = (total - i - 1) / (i + 1) * elapsed
            print(f"    [{i+1}/{total}] OK={ok} Fail={fail} Buf={len(buffer)} ETA={eta/60:.0f}min", flush=True)

        if BATCH_EVERY > 0 and (i + 1) % BATCH_EVERY == 0:
            time.sleep(BATCH_PAUSE + random.uniform(0, 5))

        if len(buffer) >= 5000:
            print(f"    Writing {len(buffer)} rows...", flush=True)
            written_rows += save_kline(engine, buffer, table)
            buffer.clear()

    if buffer:
        print(f"    Writing {len(buffer)} rows...", flush=True)
        written_rows += save_kline(engine, buffer, table)

    elapsed = time.time() - t0
    coverage = ok / total if total else 0
    print(f"    Done! OK={ok} Fail={fail} Rows={written_rows} Coverage={coverage:.1%} Time={elapsed/60:.1f}min", flush=True)
    return {
        "label": label,
        "table": table,
        "total": total,
        "ok": ok,
        "fail": fail,
        "rows": written_rows,
        "coverage": round(coverage, 4),
    }


def crawl_flow(
    engine,
    codes: list[tuple[str, int]],
    limit: int,
    min_coverage: float,
    *,
    trade_date: str,
) -> dict:
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    print(f"\n  Minute flow: {total} stocks", flush=True)

    buffer = []
    ok = fail = 0
    staged_rows = 0
    published_rows = 0
    t0 = time.time()
    stage = _create_flow_stage(engine)
    try:
        for i, (code, market) in enumerate(codes):
            klines = fetch_with_retries(fetch_minute_flow, code, market)
            parsed = parse_flow(code, klines or []) if klines else []
            rows = [
                row for row in parsed
                if str(row.get("trade_time") or "")[:10] == trade_date
            ]
            if rows:
                buffer.extend(rows)
                ok += 1
            else:
                fail += 1

            time.sleep(DELAY + random.uniform(0, JITTER))

            if (i + 1) % 200 == 0:
                elapsed = time.time() - t0
                eta = (total - i - 1) / (i + 1) * elapsed
                print(f"    [{i+1}/{total}] OK={ok} Fail={fail} Buf={len(buffer)} ETA={eta/60:.0f}min", flush=True)

            if BATCH_EVERY > 0 and (i + 1) % BATCH_EVERY == 0:
                time.sleep(BATCH_PAUSE + random.uniform(0, 5))

            if len(buffer) >= 5000:
                print(f"    Staging {len(buffer)} rows...", flush=True)
                staged_rows += _append_flow_stage(engine, stage, buffer)
                buffer.clear()

        if buffer:
            print(f"    Staging {len(buffer)} rows...", flush=True)
            staged_rows += _append_flow_stage(engine, stage, buffer)

        coverage = ok / total if total else 0
        if total > 0 and coverage >= min_coverage:
            published_rows = _publish_flow_stage(engine, stage, trade_date)
    except BaseException:
        # Preserve the collection/publish failure if cleanup also fails.
        try:
            _drop_flow_stage(engine, stage)
        except Exception:
            pass
        raise
    else:
        _drop_flow_stage(engine, stage)

    elapsed = time.time() - t0
    coverage = ok / total if total else 0
    print(f"    Done! OK={ok} Fail={fail} Rows={published_rows} Coverage={coverage:.1%} Time={elapsed/60:.1f}min", flush=True)
    return {
        "label": "Minute flow",
        "table": FLOW_TABLE,
        "total": total,
        "ok": ok,
        "fail": fail,
        "rows": published_rows,
        "staged_rows": staged_rows,
        "coverage": round(coverage, 4),
    }


def main():
    global DELAY, JITTER, BATCH_EVERY, BATCH_PAUSE, FETCH_ATTEMPTS, RETRY_DELAY

    parser = argparse.ArgumentParser(description="分钟数据爬取")
    parser.add_argument("--type", required=True,
                        choices=["stock", "index", "concept", "flow", "all"])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--request-delay", type=float, default=None)
    parser.add_argument("--request-jitter", type=float, default=None)
    parser.add_argument("--batch-every", type=int, default=None)
    parser.add_argument("--batch-pause", type=float, default=None)
    parser.add_argument("--fetch-attempts", type=int, default=None)
    parser.add_argument("--retry-delay", type=float, default=None)
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=_env_float("MINUTE_MIN_COVERAGE", "0.70"),
        help="最低成功覆盖率，低于该值返回非0，避免调度假成功",
    )
    parser.add_argument(
        "--skip-closed",
        action="store_true",
        help="盘中调度使用：非交易时段直接成功跳过，不写入数据。",
    )
    args = parser.parse_args()

    if args.request_delay is not None:
        DELAY = max(0.0, float(args.request_delay))
    if args.request_jitter is not None:
        JITTER = max(0.0, float(args.request_jitter))
    if args.batch_every is not None:
        BATCH_EVERY = max(0, int(args.batch_every))
    if args.batch_pause is not None:
        BATCH_PAUSE = max(0.0, float(args.batch_pause))
    if args.fetch_attempts is not None:
        FETCH_ATTEMPTS = max(1, int(args.fetch_attempts))
    if args.retry_delay is not None:
        RETRY_DELAY = max(0.0, float(args.retry_delay))

    engine = create_batch_engine()
    if args.skip_closed and not is_trading_time(engine):
        print("Minute sync skipped: market closed", flush=True)
        return 0

    print(f"\n{'='*60}")
    print(f"  Minute data: {args.type}")
    print(f"{'='*60}")

    summaries: list[dict] = []

    if args.type in ("stock", "all"):
        codes = get_latest_kline_stock_codes(engine)
        summaries.append(crawl_kline(engine, codes, "sm_stock_minute", "Stock 1-min", args.limit))

    if args.type in ("index", "all"):
        codes = get_codes(engine, "si_all_index_code", "index_code")
        summaries.append(crawl_kline(engine, codes, "sm_index_minute", "Index 1-min", args.limit))

    if args.type in ("concept", "all"):
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT index_code FROM si_concept_code_east ORDER BY index_code")).fetchall()
        codes = [(str(r[0]), 90) for r in rows]
        summaries.append(crawl_kline(engine, codes, "sm_concept_east_minute", "Concept 1-min", args.limit))

    if args.type in ("flow", "all"):
        kline_engine = get_kline_engine()
        flow_engine = get_minute_engine()
        codes = get_latest_kline_stock_codes(kline_engine, fallback_engine=engine)
        summaries.append(
            crawl_flow(
                flow_engine,
                codes,
                args.limit,
                args.min_coverage,
                trade_date=datetime.now().date().isoformat(),
            )
        )

    print(f"\n{'='*60}")
    print(f"  All done!")
    print(f"{'='*60}\n")

    failed = [
        s for s in summaries
        if s["total"] <= 0 or s["ok"] <= 0 or s["coverage"] < args.min_coverage
    ]
    if failed:
        print(
            "Minute sync coverage below threshold: "
            + "; ".join(f"{s['table']} {s['ok']}/{s['total']} ({s['coverage']:.1%})" for s in failed),
            file=sys.stderr,
            flush=True,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
