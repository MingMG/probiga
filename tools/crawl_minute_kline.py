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
import re
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3
from sqlalchemy import bindparam, text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, quote_identifier, write_frame
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from server.common.mysql_lock import mysql_named_lock
from integrations.qmt.backend import to_qmt_symbol
from tools.fetch_sm_stock_capital_flow_daily import reconcile_daily_flow_from_minute_close

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
            # Canonical daily K-lines use unadjusted prices (adjust_type=0).
            # Minute bars must use the same basis for close reconciliation.
            "klt": "1", "fqt": "0",
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
            continue
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
            continue
    return None


def fetch_with_retries(fetcher, code: str, market: int) -> list[str] | None:
    attempts = max(1, int(FETCH_ATTEMPTS))
    for attempt in range(attempts):
        rows = fetcher(code, market)
        if rows:
            return rows
        if attempt < attempts - 1:
            time.sleep(RETRY_DELAY * (attempt + 1) + random.uniform(0, max(0.0, JITTER)))
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
    quoted_table = quote_identifier(table)
    quoted_code_col = quote_identifier(code_col)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT {quoted_code_col} FROM {quoted_table} ORDER BY {quoted_code_col}")
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
    result = []
    for row in rows:
        code = str(row[0]).strip().zfill(6)
        if to_qmt_symbol(code):
            result.append((code, _primary_market(code)))
    return result


def latest_completed_stock_trade_date(kline_engine) -> str:
    with kline_engine.connect() as conn:
        value = conn.execute(
            text(
                """
                SELECT MAX(trade_date)
                FROM sm_stock_kline
                WHERE k_type = 1 AND adjust_type = 0
                """
            )
        ).scalar()
    if not value:
        raise RuntimeError("cannot resolve latest completed stock trade date")
    return str(value)[:10]


def incomplete_close_stock_codes(
    minute_engine,
    codes: list[tuple[str, int]],
    *,
    trade_date: str,
) -> list[tuple[str, int]]:
    """Return stocks whose completed day lacks a 15:00 minute bar."""
    with minute_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT stock_code, MAX(trade_time) AS last_trade_time
                FROM sm_stock_minute
                WHERE trade_date = :trade_date
                GROUP BY stock_code
                """
            ),
            {"trade_date": trade_date},
        ).fetchall()
    last_time = {
        str(code).strip().zfill(6): value
        for code, value in rows
    }
    result: list[tuple[str, int]] = []
    for code, market in codes:
        observed = last_time.get(code)
        observed_text = str(observed or "")
        if not observed or observed_text[11:19] < "15:00:00":
            result.append((code, market))
    return result


def _minute_code_column(table: str) -> str:
    return "index_code" if table in ("sm_index_minute", "sm_concept_east_minute") else "stock_code"


def save_kline(engine, rows: list[dict], table: str, *, stage_table: str | None = None) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")

    code_col = _minute_code_column(table)
    quoted_table = quote_identifier(table)
    quoted_code_col = quote_identifier(code_col)
    date_codes = df[["stock_code", "trade_date"]].drop_duplicates().to_dict("records")
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    # sm_index_minute / sm_concept_east_minute 用 index_code 列，有 snapshot_at
    if table in ("sm_index_minute", "sm_concept_east_minute"):
        df = df.rename(columns={"stock_code": "index_code"})
        df["snapshot_at"] = datetime.now().replace(microsecond=0)

    if stage_table:
        with engine.begin() as conn:
            write_frame(df, stage_table, conn, if_exists="append", index=False, chunksize=1000, method="multi")
    else:
        with mysql_named_lock(engine, "probiga:stock_minute", timeout_seconds=0):
            with engine.begin() as conn:
                for item in date_codes:
                    conn.execute(
                        text(f"DELETE FROM {quoted_table} WHERE {quoted_code_col} = :code AND trade_date = :d"),
                        {"code": item["stock_code"], "d": item["trade_date"]},
                    )
                write_frame(df, table, conn, if_exists="append", index=False, chunksize=1000, method="multi")
    return len(df)


def _create_run_stage(engine, table: str) -> str:
    stage = f"{table}_stage_{uuid.uuid4().hex[:12]}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {quote_identifier(stage)} LIKE {quote_identifier(table)}"))
    return stage


def _drop_run_stage(engine, stage: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {quote_identifier(stage)}"))


def _cleanup_mirror_stages(engine, table: str) -> list[str]:
    """Drop abandoned mirror stages while the caller owns the table mirror lock."""
    prefix = f"{table}_stage_"
    pattern = re.compile(rf"^{re.escape(prefix)}[0-9a-f]{{12}}$")
    with engine.connect() as conn:
        names = [
            str(row[0])
            for row in conn.execute(
                text(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE :prefix"
                ),
                {"prefix": f"{prefix}%"},
            ).fetchall()
        ]
    dropped = []
    for name in names:
        if pattern.fullmatch(name):
            _drop_run_stage(engine, name)
            dropped.append(name)
    return dropped


def _cleanup_abandoned_run_stages(engine, table: str) -> list[str]:
    """Drop only old generated stages left by killed or restarted collectors."""
    prefix = f"{table}_stage_"
    pattern = re.compile(rf"^{re.escape(prefix)}[0-9a-f]{{12}}$")
    max_age_minutes = max(30, int(os.environ.get("MINUTE_STAGE_MAX_AGE_MINUTES", "120")))
    cutoff = datetime.now() - timedelta(minutes=max_age_minutes)
    with engine.connect() as conn:
        names = [
            str(row[0])
            for row in conn.execute(
                text(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE :prefix "
                    "AND CREATE_TIME < :cutoff"
                ),
                {"prefix": f"{prefix}%", "cutoff": cutoff},
            ).fetchall()
        ]
    dropped = []
    for name in names:
        if pattern.fullmatch(name):
            _drop_run_stage(engine, name)
            dropped.append(name)
    return dropped


def _stage_columns(engine, table: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
                "AND EXTRA NOT LIKE '%auto_increment%' ORDER BY ORDINAL_POSITION"
            ),
            {"table": table},
        ).fetchall()
    return [str(row[0]) for row in rows]


def _engine_identity(engine) -> tuple[str, int, str]:
    url = engine.url
    return (str(url.host or ""), int(url.port or 0), str(url.database or ""))


def _minute_mirror_tables(sync_type: str) -> list[str]:
    tables: list[str] = []
    if sync_type in {"stock", "stock_flow", "all"}:
        tables.append("sm_stock_minute")
    if sync_type in {"flow", "stock_flow", "all"}:
        tables.append("sm_stock_capital_flow_min")
    if sync_type in {"index", "all"}:
        tables.append("sm_index_minute")
    if sync_type in {"concept", "all"}:
        tables.append("sm_concept_east_minute")
    return tables


def _mirror_latest_minute_day(source_engine, target_engine, table: str) -> dict:
    """Stream one complete latest day into the canonical minute database."""
    quoted_table = quote_identifier(table)
    date_expr = "DATE(trade_time)" if table == "sm_stock_capital_flow_min" else "trade_date"
    day_where = (
        "trade_time >= :d AND trade_time < DATE_ADD(:d, INTERVAL 1 DAY)"
        if table == "sm_stock_capital_flow_min"
        else "trade_date = :d"
    )
    with source_engine.connect() as conn:
        latest = conn.execute(text(f"SELECT MAX({date_expr}) FROM {quoted_table}")).scalar()
        latest_date = str(latest)[:10] if latest else ""
        expected = int(conn.execute(
            text(f"SELECT COUNT(*) FROM {quoted_table} WHERE {day_where}"),
            {"d": latest_date},
        ).scalar() or 0) if latest_date else 0
    if not latest_date or expected <= 0:
        raise RuntimeError(f"{table} has no latest day to mirror")

    stage = _create_run_stage(target_engine, table)
    target_columns = _stage_columns(target_engine, table)
    source_columns = set(_stage_columns(source_engine, table))
    copy_columns = [column for column in target_columns if column in source_columns]
    if not copy_columns:
        raise RuntimeError(f"{table} source and canonical schemas have no common writable columns")
    target_column_sql = ", ".join(quote_identifier(column) for column in target_columns)
    copy_column_sql = ", ".join(quote_identifier(column) for column in copy_columns)
    value_sql = ", ".join(f":{column}" for column in copy_columns)
    stage_insert = text(
        f"INSERT INTO {quote_identifier(stage)} ({copy_column_sql}) VALUES ({value_sql})"
    )
    written = 0
    try:
        with source_engine.connect().execution_options(stream_results=True) as conn:
            result = conn.execute(
                text(
                    f"SELECT {copy_column_sql} FROM {quoted_table} "
                    f"WHERE {day_where}"
                ),
                {"d": latest_date},
            )
            while True:
                batch = result.mappings().fetchmany(5000)
                if not batch:
                    break
                payload = [
                    {column: row[column] for column in copy_columns}
                    for row in batch
                ]
                with target_engine.begin() as target_conn:
                    insert_result = target_conn.execute(stage_insert, payload)
                inserted = int(insert_result.rowcount or 0)
                written += inserted if inserted >= 0 else len(payload)
        with target_engine.connect() as conn:
            staged = int(conn.execute(text(f"SELECT COUNT(*) FROM {quote_identifier(stage)}")).scalar() or 0)
        if staged != expected or written != expected:
            raise RuntimeError(
                f"{table} mirror staging mismatch: expected={expected} written={written} staged={staged}"
            )

        with target_engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {quoted_table} WHERE {day_where}"), {"d": latest_date})
            result = conn.execute(
                text(
                    f"INSERT INTO {quoted_table} ({target_column_sql}) "
                    f"SELECT {target_column_sql} FROM {quote_identifier(stage)}"
                )
            )
        _drop_run_stage(target_engine, stage)
        stage = ""
        committed = int(result.rowcount if result.rowcount is not None and result.rowcount >= 0 else expected)
        return {"table": table, "trade_date": latest_date, "rows": committed}
    finally:
        if stage:
            _drop_run_stage(target_engine, stage)


def _mirror_selected_tables(source_engine, sync_type: str) -> list[dict]:
    target_engine = get_minute_engine()
    if _engine_identity(source_engine) == _engine_identity(target_engine):
        return []
    results = []
    for table in _minute_mirror_tables(sync_type):
        print(f"  Mirroring {table} to canonical minute DB...", flush=True)
        with mysql_named_lock(target_engine, f"probiga:mirror:{table}", timeout_seconds=0):
            abandoned = _cleanup_mirror_stages(target_engine, table)
            if abandoned:
                print(f"    Cleaned abandoned stages: {len(abandoned)}", flush=True)
            result = _mirror_latest_minute_day(source_engine, target_engine, table)
        print(
            f"    Mirror done: {result['table']} {result['trade_date']} rows={result['rows']}",
            flush=True,
        )
        results.append(result)
    return results


def _cleanup_selected_mirror_stages(sync_type: str) -> list[str]:
    target_engine = get_minute_engine()
    dropped: list[str] = []
    for table in _minute_mirror_tables(sync_type):
        with mysql_named_lock(target_engine, f"probiga:mirror:{table}", timeout_seconds=0):
            dropped.extend(_cleanup_mirror_stages(target_engine, table))
    print(f"Cleaned abandoned minute mirror stages: {len(dropped)}", flush=True)
    return dropped


def _prune_selected_orphans(primary_engine, sync_type: str) -> list[dict]:
    codes = get_latest_kline_stock_codes(
        get_kline_engine(),
        fallback_engine=primary_engine,
    )
    if not codes:
        raise RuntimeError("refusing orphan cleanup: latest K-line universe is empty")
    results: list[dict] = []
    if sync_type in {"stock", "stock_flow", "all"}:
        pruned = _prune_latest_day_to_universe(get_minute_engine(), "sm_stock_minute", codes)
        results.append({"table": "sm_stock_minute", "pruned_rows": pruned})
    if sync_type in {"flow", "stock_flow", "all"}:
        pruned = _prune_latest_day_to_universe(primary_engine, "sm_stock_capital_flow_min", codes)
        results.append({"table": "sm_stock_capital_flow_min", "pruned_rows": pruned})
    if not results:
        raise RuntimeError(f"orphan cleanup is not defined for sync type: {sync_type}")
    for result in results:
        print(f"Pruned {result['table']} orphan rows: {result['pruned_rows']}", flush=True)
    return results


def _commit_run_stage(engine, target: str, stage: str) -> int:
    code_col = _minute_code_column(target)
    quoted_target = quote_identifier(target)
    quoted_stage = quote_identifier(stage)
    quoted_code = quote_identifier(code_col)
    lock_name = "probiga:capital_flow_minute" if target == "sm_stock_capital_flow_min" else "probiga:stock_minute"
    columns = _stage_columns(engine, target)
    if not columns:
        raise RuntimeError(f"target table has no writable columns: {target}")
    column_sql = ", ".join(quote_identifier(column) for column in columns)

    # The minute table is large (tens of millions of rows).  A direct
    # target JOIN stage DELETE makes MySQL choose a full target-table scan
    # even though the target has a trade_date-leading index.  Read the
    # staged keys first, then delete by date and bounded code batches so the
    # index can restrict the operation to the staged trading day.
    staged_keys: list[tuple[str, str]] = []
    if target != "sm_stock_capital_flow_min":
        with engine.connect() as conn:
            staged_keys = [
                (str(row[0]), str(row[1]))
                for row in conn.execute(
                    text(
                        f"SELECT DISTINCT trade_date, {quoted_code} "
                        f"FROM {quoted_stage} ORDER BY trade_date, {quoted_code}"
                    )
                ).fetchall()
            ]

    with mysql_named_lock(engine, lock_name, timeout_seconds=0):
        with engine.begin() as conn:
            if target == "sm_stock_capital_flow_min":
                conn.execute(
                    text(
                        f"DELETE t FROM {quoted_target} t "
                        f"INNER JOIN (SELECT DISTINCT {quoted_code} FROM {quoted_stage}) s "
                        f"ON t.{quoted_code} = s.{quoted_code}"
                    )
                )
            else:
                delete_sql = text(
                    f"DELETE FROM {quoted_target} "
                    f"WHERE {quoted_code} IN :codes "
                    "AND trade_time >= :trade_date "
                    "AND trade_time < DATE_ADD(:trade_date, INTERVAL 1 DAY)"
                ).bindparams(bindparam("codes", expanding=True))
                current_date = None
                date_codes: list[str] = []
                for trade_date, code in staged_keys + [(None, None)]:
                    if current_date is None:
                        current_date = trade_date
                    if trade_date != current_date or code is None:
                        for offset in range(0, len(date_codes), 500):
                            conn.execute(
                                delete_sql,
                                {"trade_date": current_date, "codes": date_codes[offset:offset + 500]},
                            )
                        current_date = trade_date
                        date_codes = []
                    if code is not None:
                        date_codes.append(code)
            result = conn.execute(
                text(
                    f"INSERT INTO {quoted_target} ({column_sql}) "
                    f"SELECT {column_sql} FROM {quoted_stage}"
                )
            )
        _drop_run_stage(engine, stage)
    return int(result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0)


def _prune_latest_day_to_universe(engine, target: str, codes: list[tuple[str, int]]) -> int:
    """Remove latest-day rows outside the authoritative full-market universe."""
    code_col = _minute_code_column(target)
    clean_codes = sorted({str(code).strip().zfill(6) for code, _market in codes if str(code).strip()})
    if not clean_codes:
        raise RuntimeError(f"refusing to prune {target}: authoritative universe is empty")
    quoted_target = quote_identifier(target)
    quoted_code = quote_identifier(code_col)
    date_expr = "DATE(trade_time)" if target == "sm_stock_capital_flow_min" else "trade_date"
    latest_sql = text(f"SELECT MAX({date_expr}) FROM {quoted_target}")
    with engine.connect() as conn:
        latest = conn.execute(latest_sql).scalar()
    latest_date = str(latest)[:10] if latest else ""
    if not latest_date:
        raise RuntimeError(f"refusing to prune {target}: table has no latest day")
    if target == "sm_stock_capital_flow_min":
        day_where = "trade_time >= :latest AND trade_time < DATE_ADD(:latest, INTERVAL 1 DAY)"
    else:
        day_where = "trade_date = :latest"
    statement = text(
        f"DELETE FROM {quoted_target} WHERE {day_where} "
        f"AND ({quoted_code} IS NULL OR {quoted_code} NOT IN :codes)"
    ).bindparams(bindparam("codes", expanding=True))
    lock_name = "probiga:capital_flow_minute" if target == "sm_stock_capital_flow_min" else "probiga:stock_minute"
    with mysql_named_lock(engine, lock_name, timeout_seconds=0):
        with engine.begin() as conn:
            result = conn.execute(statement, {"latest": latest_date, "codes": clean_codes})
    return int(result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0)


def save_flow(engine, rows: list[dict], *, stage_table: str | None = None) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows).replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_time"], keep="last")

    codes = sorted(df["stock_code"].unique())
    df["snapshot_at"] = datetime.now().replace(microsecond=0)
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    if stage_table:
        with engine.begin() as conn:
            write_frame(
                df,
                stage_table,
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
    else:
        with mysql_named_lock(engine, "probiga:capital_flow_minute", timeout_seconds=0):
            with engine.begin() as conn:
                for c in codes:
                    conn.execute(text("DELETE FROM sm_stock_capital_flow_min WHERE stock_code = :c"), {"c": c})
                write_frame(
                    df,
                    "sm_stock_capital_flow_min",
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )
    return len(df)


def crawl_kline(
    engine,
    codes: list[tuple[str, int]],
    table: str,
    label: str,
    limit: int,
    min_coverage: float,
    *,
    target_trade_date: str = "",
) -> dict:
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    print(f"\n  {label}: {total} items", flush=True)

    buffer = []
    abandoned = _cleanup_abandoned_run_stages(engine, table)
    if abandoned:
        print(f"    Cleaned abandoned stages: {len(abandoned)}", flush=True)
    stage = _create_run_stage(engine, table)
    ok = fail = 0
    written_rows = 0
    t0 = time.time()

    for i, (code, market) in enumerate(codes):
        try:
            klines = fetch_with_retries(fetch_minute_kline, code, market)
            rows = parse_kline(code, klines or []) if klines else []
            if target_trade_date:
                rows = [
                    row for row in rows
                    if str(row.get("trade_date") or "") == target_trade_date
                ]
        except Exception:
            _drop_run_stage(engine, stage)
            stage = ""
            raise
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
            try:
                written_rows += save_kline(engine, buffer, table, stage_table=stage)
            except Exception:
                _drop_run_stage(engine, stage)
                stage = ""
                raise
            buffer.clear()

    if buffer:
        print(f"    Writing {len(buffer)} rows...", flush=True)
        try:
            written_rows += save_kline(engine, buffer, table, stage_table=stage)
        except Exception:
            _drop_run_stage(engine, stage)
            stage = ""
            raise

    elapsed = time.time() - t0
    coverage = ok / total if total else 0
    committed_rows = 0
    pruned_rows = 0
    try:
        if coverage >= min_coverage:
            committed_rows = _commit_run_stage(engine, table, stage)
            stage = ""
            # A targeted close repair intentionally contains only the damaged
            # subset.  It must never be mistaken for the authoritative full
            # universe during orphan pruning.
            if not limit and not target_trade_date:
                pruned_rows = _prune_latest_day_to_universe(engine, table, codes)
        else:
            _drop_run_stage(engine, stage)
            stage = ""
    except Exception:
        _drop_run_stage(engine, stage)
        stage = ""
        raise
    print(f"    Done! OK={ok} Fail={fail} Rows={committed_rows} Coverage={coverage:.1%} Time={elapsed/60:.1f}min", flush=True)
    return {
        "label": label,
        "table": table,
        "total": total,
        "ok": ok,
        "fail": fail,
        "rows": committed_rows,
        "coverage": round(coverage, 4),
        "pruned_rows": pruned_rows,
    }


def crawl_flow(engine, codes: list[tuple[str, int]], limit: int, min_coverage: float) -> dict:
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    print(f"\n  Minute flow: {total} stocks", flush=True)

    buffer = []
    abandoned = _cleanup_abandoned_run_stages(engine, "sm_stock_capital_flow_min")
    if abandoned:
        print(f"    Cleaned abandoned stages: {len(abandoned)}", flush=True)
    stage = _create_run_stage(engine, "sm_stock_capital_flow_min")
    ok = fail = 0
    written_rows = 0
    t0 = time.time()

    for i, (code, market) in enumerate(codes):
        try:
            klines = fetch_with_retries(fetch_minute_flow, code, market)
            rows = parse_flow(code, klines or []) if klines else []
        except Exception:
            _drop_run_stage(engine, stage)
            stage = ""
            raise
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
            try:
                written_rows += save_flow(engine, buffer, stage_table=stage)
            except Exception:
                _drop_run_stage(engine, stage)
                stage = ""
                raise
            buffer.clear()

    if buffer:
        print(f"    Writing {len(buffer)} rows...", flush=True)
        try:
            written_rows += save_flow(engine, buffer, stage_table=stage)
        except Exception:
            _drop_run_stage(engine, stage)
            stage = ""
            raise

    elapsed = time.time() - t0
    coverage = ok / total if total else 0
    committed_rows = 0
    pruned_rows = 0
    daily_reconciliation: dict[str, int] = {}
    try:
        if coverage >= min_coverage:
            committed_rows = _commit_run_stage(engine, "sm_stock_capital_flow_min", stage)
            stage = ""
            if not limit:
                pruned_rows = _prune_latest_day_to_universe(
                    engine,
                    "sm_stock_capital_flow_min",
                    codes,
                )
                with engine.connect() as conn:
                    latest_flow_date = conn.execute(
                        text("SELECT DATE(MAX(trade_time)) FROM sm_stock_capital_flow_min")
                    ).scalar()
                if latest_flow_date:
                    daily_reconciliation = reconcile_daily_flow_from_minute_close(
                        engine,
                        str(latest_flow_date)[:10],
                        [code for code, _market in codes],
                    )
        else:
            _drop_run_stage(engine, stage)
            stage = ""
    except Exception:
        _drop_run_stage(engine, stage)
        stage = ""
        raise
    print(f"    Done! OK={ok} Fail={fail} Rows={committed_rows} Coverage={coverage:.1%} Time={elapsed/60:.1f}min", flush=True)
    return {
        "label": "Minute flow",
        "table": "sm_stock_capital_flow_min",
        "total": total,
        "ok": ok,
        "fail": fail,
        "rows": committed_rows,
        "coverage": round(coverage, 4),
        "pruned_rows": pruned_rows,
        "daily_reconciliation": daily_reconciliation,
    }


def main():
    global DELAY, JITTER, BATCH_EVERY, BATCH_PAUSE, FETCH_ATTEMPTS, RETRY_DELAY

    parser = argparse.ArgumentParser(description="分钟数据爬取")
    parser.add_argument("--type", required=True,
                        choices=["stock", "stock_flow", "index", "concept", "flow", "all"])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--trade-date",
        default="",
        help="Completed trade date used by --repair-incomplete-close (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--repair-incomplete-close",
        action="store_true",
        help="For stock minute data, fetch only stocks missing the completed day's 15:00 bar.",
    )
    parser.add_argument("--request-delay", type=float, default=None, help="覆盖 MINUTE_REQUEST_DELAY")
    parser.add_argument("--request-jitter", type=float, default=None, help="覆盖 MINUTE_REQUEST_JITTER")
    parser.add_argument("--batch-every", type=int, default=None, help="覆盖 MINUTE_BATCH_EVERY，0 表示不批间暂停")
    parser.add_argument("--batch-pause", type=float, default=None, help="覆盖 MINUTE_BATCH_PAUSE")
    parser.add_argument("--fetch-attempts", type=int, default=None, help="override MINUTE_FETCH_ATTEMPTS")
    parser.add_argument("--retry-delay", type=float, default=None, help="override MINUTE_RETRY_DELAY")
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
    mirror_group = parser.add_mutually_exclusive_group()
    mirror_group.add_argument(
        "--mirror-only",
        action="store_true",
        help="Mirror the latest completed day from the primary DB to the canonical minute DB.",
    )
    mirror_group.add_argument(
        "--cleanup-mirror-stages-only",
        action="store_true",
        help="Drop strictly named abandoned minute mirror stages and exit.",
    )
    mirror_group.add_argument(
        "--prune-orphans-only",
        action="store_true",
        help="Prune latest-day stock minute/flow rows outside the latest daily K-line universe.",
    )
    args = parser.parse_args()
    if args.repair_incomplete_close and args.type != "stock":
        parser.error("--repair-incomplete-close requires --type stock")

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

    if args.cleanup_mirror_stages_only:
        _cleanup_selected_mirror_stages(args.type)
        return 0

    engine = create_batch_engine()
    if args.prune_orphans_only:
        _prune_selected_orphans(engine, args.type)
        return 0
    if args.mirror_only:
        _mirror_selected_tables(engine, args.type)
        return 0
    minute_engine = get_minute_engine()
    kline_engine = get_kline_engine()
    if args.skip_closed and not is_trading_time(engine):
        print(
            '{"status": "skipped", "reason": "market_closed", '
            '"message": "Minute sync skipped: market closed"}',
            flush=True,
        )
        return 0

    print(f"\n{'='*60}")
    print(f"  Minute data: {args.type}")
    print(f"{'='*60}")

    summaries: list[dict] = []

    if args.type in ("stock", "stock_flow", "all"):
        codes = get_latest_kline_stock_codes(kline_engine, fallback_engine=engine)
        repair_date = ""
        if args.repair_incomplete_close:
            repair_date = (
                str(args.trade_date or "").strip()
                or latest_completed_stock_trade_date(kline_engine)
            )
            codes = incomplete_close_stock_codes(
                minute_engine,
                codes,
                trade_date=repair_date,
            )
            print(
                f"  Incomplete-close repair {repair_date}: {len(codes)} stocks",
                flush=True,
            )
            if not codes:
                print(
                    '{"status":"success","reason":"no_incomplete_close_stocks"}',
                    flush=True,
                )
                return 0
        summaries.append(
            crawl_kline(
                minute_engine,
                codes,
                "sm_stock_minute",
                "Stock 1-min",
                args.limit,
                args.min_coverage,
                target_trade_date=repair_date,
            )
        )

    if args.type in ("index", "all"):
        codes = get_codes(engine, "si_all_index_code", "index_code")
        summaries.append(crawl_kline(kline_engine, codes, "sm_index_minute", "Index 1-min", args.limit, args.min_coverage))

    if args.type in ("concept", "all"):
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT index_code FROM si_concept_code_east ORDER BY index_code")).fetchall()
        codes = [(str(r[0]), 90) for r in rows]
        summaries.append(crawl_kline(kline_engine, codes, "sm_concept_east_minute", "Concept 1-min", args.limit, args.min_coverage))

    if args.type in ("flow", "stock_flow", "all"):
        codes = get_latest_kline_stock_codes(kline_engine, fallback_engine=engine)
        summaries.append(crawl_flow(engine, codes, args.limit, args.min_coverage))

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
