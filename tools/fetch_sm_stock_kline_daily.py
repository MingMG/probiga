#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch one trading day's A-share daily K data into sm_stock_kline.

This script is for the daily after-market pipeline.  Its target universe comes
only from an independently captured QMT native A-share catalog.  It writes a
day only when every target member has one bar.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from biz.stock_market.stock_kline_akshare import (  # noqa: E402
    _fetch_eastmoney_daily_kline,
    akshare_daily_to_sm_kline,
    em_code_to_sina_symbol,
)
from biz.stock_market.sina_kline_fetch import fetch_sina_a_daily_kline  # noqa: E402
from server.common.config import get_mysql_url  # noqa: E402
from server.common.batch_db import replace_table_rows_exact_keys  # noqa: E402
from server.common.mysql_lock import STOCK_KLINE_FREEZE_LOCK_NAME  # noqa: E402
from server.common.qmt_attestation_contract import (  # noqa: E402
    expected_stock_set_contract,
)
from server.common.qmt_stock_catalog import (  # noqa: E402
    load_target_stock_catalog,
)

_WORKERS = max(1, int(os.environ.get("KLINE_DAILY_WORKERS", "4")))
_REQUEST_DELAY = float(os.environ.get("KLINE_DAILY_REQUEST_DELAY", "0.3"))
_REQUEST_JITTER = float(os.environ.get("KLINE_DAILY_REQUEST_JITTER", "0.2"))
_MIN_COVERAGE = 1.0
_MAX_RETRIES = max(0, int(os.environ.get("KLINE_DAILY_MAX_RETRIES", "2")))
_SOURCES = [s.strip().lower() for s in os.environ.get("KLINE_DAILY_SOURCES", "sina,east").split(",") if s.strip()]
_BATCH_PAUSE = float(os.environ.get("KLINE_DAILY_BATCH_PAUSE", "30.0"))
_BATCH_PAUSE_EVERY = int(os.environ.get("KLINE_DAILY_BATCH_PAUSE_EVERY", "100"))
_COOLDOWN_THRESHOLD = int(os.environ.get("KLINE_DAILY_COOLDOWN_THRESHOLD", "10"))
_COOLDOWN_SECONDS = float(os.environ.get("KLINE_DAILY_COOLDOWN_SECONDS", "120"))


@dataclass
class FetchOutcome:
    code: str
    df: pd.DataFrame | None
    source: str = ""
    error: Exception | None = None
    no_data: bool = False


def _normalize_date(raw: str) -> str:
    s = (raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10]


def _to_yyyymmdd(raw: str) -> str:
    return _normalize_date(raw).replace("-", "")


def _fmt_date(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)[:10]


def _mysql_url() -> str:
    return get_mysql_url(required=True)


def _expected_trade_date(engine: Engine) -> str:
    with engine.connect() as conn:
        d = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date <= :today
        """), {"today": date.today().isoformat()}).scalar()
    return _fmt_date(d)


def _read_short_name_map(engine: Engine) -> dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code, short_name FROM si_all_code")).fetchall()
    return {
        str(r[0]).strip().zfill(6): (str(r[1]).strip() if r[1] is not None else "")
        for r in rows
    }


def _read_stock_codes(
    engine: Engine,
    target_date: str,
) -> tuple[list[str], str, dict[str, object]]:
    catalog, codes = load_target_stock_catalog(
        engine,
        target_date=target_date,
        decision_known_at=datetime.now().replace(microsecond=0),
    )
    target_contract = expected_stock_set_contract(target_date, codes)
    proof: dict[str, object] = {
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_count": catalog.member_count,
        "catalog_member_set_hash": catalog.member_set_hash,
        **target_contract,
    }
    return codes, f"qmt_stock_catalog:{catalog.batch_id}", proof


def _fetch_one(code: str, target_date: str, short_name: str) -> FetchOutcome:
    last_error: Exception | None = None
    for source in _SOURCES:
        try:
            if source == "efinance":
                df = _fetch_efinance_one(code, target_date, short_name)
            elif source in ("sina", "east", "eastmoney", "em"):
                df = _fetch_builtin_one(code, target_date, short_name, source)
            else:
                raise ValueError(f"未知日K数据源: {source}")
            if df is not None and not df.empty:
                return FetchOutcome(code=code, df=df, source=source)
        except Exception as e:  # pylint: disable=broad-except
            last_error = e
            continue
    if last_error is not None:
        return FetchOutcome(code=code, df=None, error=last_error)
    return FetchOutcome(code=code, df=None, no_data=True)


def _fetch_builtin_one(code: str, target_date: str, short_name: str, source: str) -> pd.DataFrame | None:
    api_date = _to_yyyymmdd(target_date)
    last_error: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            if source == "sina":
                symbol = em_code_to_sina_symbol(code)
                raw = fetch_sina_a_daily_kline(symbol, api_date, api_date, "") if symbol else None
            else:
                raw = _fetch_eastmoney_daily_kline(code, api_date, api_date, "")
            if raw is None or raw.empty:
                return None
            out = akshare_daily_to_sm_kline(raw, code, 1, 0, short_name=short_name)
            if out is None or out.empty:
                return None
            out = out[out["trade_date"].astype(str).str[:10] == target_date]
            if out.empty:
                return None
            return out
        except Exception as e:  # pylint: disable=broad-except
            last_error = e
            if attempt < _MAX_RETRIES:
                time.sleep(1.5 * (2 ** attempt) + random.uniform(0, 0.8))
    if last_error is not None:
        raise last_error
    return None


def _fetch_efinance_one(code: str, target_date: str, short_name: str) -> pd.DataFrame | None:
    import efinance as ef

    d = _to_yyyymmdd(target_date)
    raw = ef.stock.get_quote_history(code, beg=d, end=d)
    if raw is None or raw.empty:
        return None
    rename = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "change_pct",
        "涨跌额": "change",
        "换手率": "turnover",
    }
    df = raw.rename(columns=rename)
    if "date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].dt.strftime("%Y-%m-%d") == target_date]
    if df.empty:
        return None
    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce") * 100
    for col in ("open", "close", "high", "low", "amount", "change_pct", "change", "turnover"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return akshare_daily_to_sm_kline(df, code, 1, 0, short_name=short_name)


def _write_daily_kline(engine: Engine, target_date: str, df: pd.DataFrame) -> int:
    full_df = df.replace({np.nan: None, pd.NaT: None})
    full_df = full_df.drop_duplicates(subset=["stock_code", "trade_date", "k_type", "adjust_type"], keep="last")
    full_df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    columns = [
        "stock_code", "short_name", "trade_time", "trade_date", "k_type", "adjust_type",
        "open", "close", "high", "low", "volume", "amount", "change", "change_pct",
        "turnover_ratio", "pre_close", "etl_sync_at",
    ]
    return replace_table_rows_exact_keys(
        full_df[columns],
        "sm_stock_kline",
        engine,
        key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
        lock_name=STOCK_KLINE_FREEZE_LOCK_NAME,
        lock_timeout_seconds=max(0, int(os.environ.get("KLINE_DAILY_LOCK_TIMEOUT", "30"))),
    )


def fetch_daily_kline(target_date: str = "", *, min_coverage: float | None = None, dry_run: bool = False) -> int:
    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    target_date = _normalize_date(target_date) if target_date else _expected_trade_date(engine)
    if not target_date:
        print("无法确定目标交易日：si_trade_calendar 无可用交易日")
        return 2
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {target_date}")
        return 2

    stock_codes, universe_source, frozen_universe = _read_stock_codes(
        engine, target_date
    )
    max_stocks = int(os.environ.get("KLINE_DAILY_MAX_STOCKS", "0"))
    if max_stocks > 0:
        print("KLINE_DAILY_MAX_STOCKS 会破坏权威目标池，生产日K任务已阻断")
        return 2
    requested_coverage = _MIN_COVERAGE if min_coverage is None else min_coverage
    if requested_coverage != 1.0:
        print("日K写入要求权威目标池 100% 精确覆盖，不能降低覆盖率阈值")
        return 2
    min_coverage = 1.0
    short_names = _read_short_name_map(engine)

    print(f"开始获取个股日K，目标日期: {target_date}")
    print(f"股票池: {universe_source}, 共 {len(stock_codes)} 只")
    print(
        "冻结目标池: "
        f"count={frozen_universe['stock_count']} "
        f"hash={frozen_universe['stock_set_hash']}"
    )
    print(f"数据源链: {' -> '.join(_SOURCES)}, 并发: {_WORKERS}, 最小覆盖率: {min_coverage:.0%}, dry_run={dry_run}")

    parts: list[pd.DataFrame] = []
    errors: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    success = failed = no_data = done = 0
    started = time.time()
    consecutive_fails = 0  # 连续失败计数，用于触发冷却

    def _worker(code: str) -> FetchOutcome:
        outcome = _fetch_one(code, target_date, short_names.get(code, ""))
        time.sleep(_REQUEST_DELAY + random.uniform(0, _REQUEST_JITTER))
        return outcome

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        futures = {pool.submit(_worker, code): code for code in stock_codes}
        for future in as_completed(futures):
            done += 1
            code = futures[future]
            try:
                outcome = future.result()
            except Exception as e:  # pylint: disable=broad-except
                outcome = FetchOutcome(code=code, df=None, error=e)

            if outcome.df is not None and not outcome.df.empty:
                parts.append(outcome.df)
                success += 1
                source_counter[outcome.source or "unknown"] += 1
                consecutive_fails = 0
            elif outcome.no_data:
                no_data += 1
                consecutive_fails = 0
            else:
                failed += 1
                consecutive_fails += 1
                errors[type(outcome.error).__name__ if outcome.error else "unknown"] += 1
                if failed <= 12:
                    print(f"  {code} 失败: {outcome.error}")

                # 连续失败过多时触发冷却，避免被服务器封IP
                if consecutive_fails >= _COOLDOWN_THRESHOLD:
                    print(f"  ⚠️ 连续失败 {consecutive_fails} 次，冷却 {_COOLDOWN_SECONDS:.0f}s 避免被封...")
                    time.sleep(_COOLDOWN_SECONDS)
                    consecutive_fails = 0

            if done % 500 == 0 or done == len(stock_codes):
                elapsed = time.time() - started
                speed = done / elapsed if elapsed > 0 else 0
                eta = (len(stock_codes) - done) / speed if speed > 0 else 0
                print(
                    f"  进度 {done}/{len(stock_codes)} 成功={success} 无数据={no_data} 失败={failed} "
                    f"已用 {elapsed:.0f}s 预计剩余 {eta:.0f}s"
                )

            # 每处理 N 只股票暂停一段时间
            if _BATCH_PAUSE_EVERY > 0 and done % _BATCH_PAUSE_EVERY == 0 and done < len(stock_codes):
                pause = _BATCH_PAUSE + random.uniform(0, 10)
                print(f"  批次暂停 {pause:.0f}s（已处理 {done} 只）...")
                time.sleep(pause)

    if not parts:
        print("未获取到任何日K数据")
        return 2

    full_df = pd.concat(parts, ignore_index=True)
    full_df = full_df.drop_duplicates(subset=["stock_code", "trade_date", "k_type", "adjust_type"], keep="last")
    coverage = len(full_df) / max(len(stock_codes), 1)
    observed_codes = sorted(
        {str(code).strip().zfill(6) for code in full_df["stock_code"].tolist()}
    )
    observed_contract = expected_stock_set_contract(target_date, observed_codes)

    elapsed_total = time.time() - started
    print("\n===== 汇总 =====")
    print(f"  总数: {len(stock_codes)}")
    print(f"  成功: {success}")
    print(f"  无数据: {no_data}")
    print(f"  失败: {failed}")
    if errors:
        print(f"  失败分类: {dict(errors)}")
    if source_counter:
        print(f"  数据源命中: {dict(source_counter)}")
    print(f"  去重后行数: {len(full_df)}")
    print(f"  覆盖率: {len(full_df)}/{len(stock_codes)} ({coverage:.1%})")
    print(f"  耗时: {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")

    if (
        coverage != 1.0
        or observed_codes != stock_codes
        or observed_contract != {
            "stock_count": frozen_universe["stock_count"],
            "stock_set_hash": frozen_universe["stock_set_hash"],
        }
    ):
        print("实际日K集合与冻结 QMT catalog 目标池不完全相等，已停止写库")
        return 3
    if dry_run:
        print("[dry-run] 覆盖率达标，但不写入数据库")
        return 0

    # Re-read the exact selected append-only batch before mutation.  A newer
    # catalog may appear during the fetch, but it cannot change this run's
    # frozen batch or target-set hash.
    current_catalog, current_codes = load_target_stock_catalog(
        engine,
        target_date=target_date,
        decision_known_at=datetime.now().replace(microsecond=0),
        batch_id=str(frozen_universe["catalog_batch_id"]),
    )
    if (
        current_catalog.manifest_hash
        != frozen_universe["catalog_manifest_hash"]
        or expected_stock_set_contract(target_date, current_codes)
        != observed_contract
    ):
        print("冻结 QMT catalog 批次在抓取期间发生变化，已停止写库")
        return 3
    written = _write_daily_kline(engine, target_date, full_df)
    print(f"写入完成: sm_stock_kline {target_date}, 共 {written} 行")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="获取指定交易日个股日K（并发+覆盖率保护）")
    parser.add_argument("date", nargs="?", default="", help="目标交易日 YYYY-MM-DD；不传则取交易日历最新开市日")
    parser.add_argument("--min-coverage", type=float, default=None, help="兼容参数；生产写入固定要求 1.0，不能降低")
    parser.add_argument("--dry-run", action="store_true", help="只抓取并检查覆盖率，不写库")
    args = parser.parse_args()
    return fetch_daily_kline(args.date, min_coverage=args.min_coverage, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
