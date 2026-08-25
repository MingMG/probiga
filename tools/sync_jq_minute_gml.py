#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sync JoinQuant 1-minute bars into sm_stock_minute_gml.

This job is designed for intraday scheduling. It fetches only the latest few
1-minute bars from JQData and upserts them into an independent live table, so it
does not touch the slower legacy minute tables.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.config import get_mysql_url
from server.common.jq_minute_schema import (
    JQ_MINUTE_DDL,
    JQ_MINUTE_TABLE,
    privileged_migrate_jq_minute_tables,
    validate_jq_minute_runtime,
)
from tools.jq_config import get_jq_client, jq_auth, jq_normalize_code


logger = logging.getLogger("sync_jq_minute_gml")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )


jq = None


def _get_jq_client():
    global jq
    if jq is not None:
        return jq
    client = jq_auth()
    if client is None:
        client = get_jq_client(required=False)
    if client is None:
        raise RuntimeError("未安装 jqdatasdk，无法执行聚宽分钟同步。")
    jq = client
    return jq


TABLE_NAME = JQ_MINUTE_TABLE
BAR_FIELDS = ("date", "open", "high", "low", "close", "volume", "money")
DDL_SQL = JQ_MINUTE_DDL

UPSERT_SQL = text(
    """
    INSERT INTO `sm_stock_minute_gml`
      (`stock_code`, `jq_code`, `trade_time`, `trade_date`,
       `open`, `high`, `low`, `close`, `volume`, `amount`,
       `pre_close`, `is_current_bar`, `etl_sync_at`)
    VALUES
      (:stock_code, :jq_code, :trade_time, :trade_date,
       :open, :high, :low, :close, :volume, :amount,
       :pre_close, :is_current_bar, :etl_sync_at)
    ON DUPLICATE KEY UPDATE
      `jq_code` = VALUES(`jq_code`),
      `open` = VALUES(`open`),
      `high` = VALUES(`high`),
      `low` = VALUES(`low`),
      `close` = VALUES(`close`),
      `volume` = VALUES(`volume`),
      `amount` = VALUES(`amount`),
      `pre_close` = VALUES(`pre_close`),
      `is_current_bar` = VALUES(`is_current_bar`),
      `etl_sync_at` = VALUES(`etl_sync_at`)
    """
)


def _run_ddl(engine: Engine) -> None:
    """Compatibility name: runtime validation only; never execute DDL."""

    validate_jq_minute_runtime(engine)


def _is_trade_day(engine: Engine, day: date | None = None) -> bool:
    day = day or date.today()
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
                {"d": day.isoformat()},
            ).scalar()
        return bool(count)
    except Exception:
        return day.weekday() < 5


def is_trading_time(engine: Engine, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if not _is_trade_day(engine, now.date()):
        return False
    current = now.hour * 100 + now.minute
    return (930 <= current <= 1135) or (1255 <= current <= 1505)


def _stock_code_to_jq(stock_code: str, *, include_bj: bool = False) -> str:
    code = re.sub(r"\D", "", str(stock_code)).zfill(6)
    if not code or code == "000000":
        return ""
    if code.startswith(("4", "8")) and not include_bj:
        return ""
    if code.startswith(("6", "9")):
        return f"{code}.XSHG"
    if code.startswith(("0", "2", "3")):
        return f"{code}.XSHE"
    if include_bj:
        try:
            return jq_normalize_code(code)
        except Exception:
            return ""
    return ""


def _jq_code_to_stock(jq_code: str) -> str:
    return str(jq_code).split(".")[0].zfill(6)


def _read_codes(
    engine: Engine,
    *,
    universe: str,
    codes: str,
    limit: int,
    include_bj: bool,
) -> list[str]:
    raw_codes: list[str] = []
    if codes.strip():
        raw_codes = [item.strip() for item in re.split(r"[,;\s]+", codes) if item.strip()]
    elif universe == "si-all":
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT stock_code
                    FROM si_all_code
                    WHERE stock_code REGEXP '^(0|2|3|6|9)'
                    ORDER BY stock_code
                    """
                )
            ).fetchall()
        raw_codes = [str(row[0]).zfill(6) for row in rows]
    else:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT stock_code
                    FROM sm_stock_kline
                    WHERE trade_date = (
                        SELECT MAX(trade_date)
                        FROM sm_stock_kline
                        WHERE k_type = 1
                    )
                      AND k_type = 1
                      AND stock_code REGEXP '^(0|2|3|6|9)'
                    ORDER BY stock_code
                    """
                )
            ).fetchall()
        raw_codes = [str(row[0]).zfill(6) for row in rows]
        if not raw_codes:
            return _read_codes(engine, universe="si-all", codes="", limit=limit, include_bj=include_bj)

    seen: set[str] = set()
    jq_codes: list[str] = []
    for item in raw_codes:
        jq_code = item if "." in str(item) else _stock_code_to_jq(item, include_bj=include_bj)
        if not jq_code or jq_code in seen:
            continue
        seen.add(jq_code)
        jq_codes.append(jq_code)
        if limit > 0 and len(jq_codes) >= limit:
            break
    return jq_codes


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    size = max(1, int(size))
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _looks_like_jq_code(value: Any) -> bool:
    text_value = str(value)
    return bool(re.match(r"^\d{6}\.(XSHG|XSHE|XBEI)$", text_value))


def _find_code_column(df: pd.DataFrame, requested_codes: set[str]) -> str | None:
    preferred = ["code", "security", "symbol", "level_0"]
    columns = [str(c) for c in df.columns]
    for name in preferred + columns:
        if name not in df.columns:
            continue
        sample = df[name].dropna().head(20).tolist()
        if any(str(value) in requested_codes or _looks_like_jq_code(value) for value in sample):
            return name
    return None


def _find_datetime_column(df: pd.DataFrame, code_col: str | None) -> tuple[str | None, pd.Series | None]:
    preferred = ["date", "time", "datetime", "trade_time", "level_1", "index"]
    columns = [str(c) for c in df.columns]
    for name in preferred + columns:
        if name not in df.columns or name == code_col:
            continue
        if pd.api.types.is_numeric_dtype(df[name]):
            continue
        parsed = pd.to_datetime(df[name], errors="coerce")
        if parsed.notna().any():
            return name, parsed
    return None, None


def _clean_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_int(value: Any) -> int | None:
    cleaned = _clean_float(value)
    if cleaned is None:
        return None
    return int(cleaned)


def _frame_to_rows(
    data: Any,
    requested_codes: list[str],
    *,
    include_now: bool,
    synced_at: datetime,
) -> list[dict[str, Any]]:
    if data is None:
        return []
    df = pd.DataFrame(data).replace({np.nan: None, pd.NaT: None})
    if df.empty:
        return []
    df = df.reset_index()

    requested_set = set(requested_codes)
    code_col = _find_code_column(df, requested_set)
    time_col, parsed_times = _find_datetime_column(df, code_col)
    if time_col is None or parsed_times is None:
        raise RuntimeError(f"JQData bars result has no datetime column: {list(df.columns)}")

    if code_col is None:
        if len(requested_codes) != 1:
            raise RuntimeError(f"JQData bars result has no security column: {list(df.columns)}")
        df["_jq_code"] = requested_codes[0]
        code_col = "_jq_code"

    df["_trade_time"] = parsed_times.dt.to_pydatetime()
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        trade_time = row.get("_trade_time")
        jq_code = str(row.get(code_col) or "").strip()
        if not jq_code or jq_code not in requested_set or pd.isna(trade_time):
            continue
        trade_time = trade_time.replace(second=0, microsecond=0)
        rows.append(
            {
                "stock_code": _jq_code_to_stock(jq_code),
                "jq_code": jq_code,
                "trade_time": trade_time,
                "trade_date": trade_time.date(),
                "open": _clean_float(row.get("open")),
                "high": _clean_float(row.get("high")),
                "low": _clean_float(row.get("low")),
                "close": _clean_float(row.get("close")),
                "volume": _clean_int(row.get("volume")),
                "amount": _clean_float(row.get("money")),
                "pre_close": _clean_float(row.get("pre_close")),
                "is_current_bar": 0,
                "etl_sync_at": synced_at,
            }
        )

    if include_now and rows:
        latest_by_stock: dict[str, datetime] = {}
        for row in rows:
            stock_code = row["stock_code"]
            latest_by_stock[stock_code] = max(latest_by_stock.get(stock_code, row["trade_time"]), row["trade_time"])
        for row in rows:
            if row["trade_time"] == latest_by_stock.get(row["stock_code"]):
                row["is_current_bar"] = 1
    return rows


def _save_rows(engine: Engine, rows: list[dict[str, Any]], *, chunk_size: int = 1000) -> int:
    if not rows:
        return 0
    written = 0
    with engine.begin() as conn:
        for batch in _chunks(rows, chunk_size):
            conn.execute(UPSERT_SQL, batch)
            written += len(batch)
    return written


def sync_jq_minute_gml(
    engine: Engine,
    *,
    universe: str = "latest-kline",
    codes: str = "",
    limit: int = 0,
    count: int = 3,
    batch_size: int = 200,
    include_now: bool = True,
    skip_paused: bool = True,
    skip_closed: bool = True,
    min_coverage: float = 0.0,
    include_bj: bool = False,
    dry_run: bool = False,
    skip_ddl: bool = False,
    pause_seconds: float = 0.0,
) -> dict[str, Any]:
    # ``skip_ddl`` is retained as a CLI compatibility option only.  Runtime
    # execution always proves the release-prepared schema and never creates it.
    del skip_ddl
    _run_ddl(engine)

    now = datetime.now().replace(microsecond=0)
    if skip_closed and not is_trading_time(engine, now):
        return {
            "status": "skipped",
            "reason": "market_closed",
            "table": TABLE_NAME,
            "generated_at": now.isoformat(sep=" ", timespec="seconds"),
        }

    jq_codes = _read_codes(engine, universe=universe, codes=codes, limit=limit, include_bj=include_bj)
    if not jq_codes:
        raise RuntimeError("no stock codes available for JQ minute sync")

    jq_client = _get_jq_client()
    query_count = jq_client.get_query_count()
    all_rows: list[dict[str, Any]] = []
    ok_stocks: set[str] = set()
    failed_batches = 0
    batch_errors: list[str] = []

    for batch in _chunks(jq_codes, batch_size):
        try:
            security = batch[0] if len(batch) == 1 else batch
            bars = jq_client.get_bars(
                security,
                count=max(1, int(count)),
                unit="1m",
                fields=BAR_FIELDS,
                include_now=include_now,
                fq_ref_date=None,
                df=True,
                skip_paused=skip_paused,
            )
            rows = _frame_to_rows(bars, batch, include_now=include_now, synced_at=now)
            all_rows.extend(rows)
            ok_stocks.update(row["stock_code"] for row in rows)
        except Exception as exc:  # pylint: disable=broad-except
            failed_batches += 1
            if len(batch_errors) < 5:
                batch_errors.append(f"{batch[0]}: {exc}")
            logger.warning("JQ minute batch failed: size=%s, first=%s, error=%s", len(batch), batch[0], exc)
        if pause_seconds > 0:
            time.sleep(pause_seconds)

    coverage = len(ok_stocks) / max(len(jq_codes), 1)
    if not all_rows:
        suffix = f"; first_errors={batch_errors}" if batch_errors else ""
        raise RuntimeError(f"JQ minute sync returned no rows for {len(jq_codes)} requested stocks{suffix}")
    if min_coverage > 0 and coverage < min_coverage:
        raise RuntimeError(
            f"JQ minute coverage below threshold: {len(ok_stocks)}/{len(jq_codes)} "
            f"({coverage:.1%}) < {min_coverage:.1%}"
        )

    written = 0 if dry_run else _save_rows(engine, all_rows)
    latest_trade_time = max((row["trade_time"] for row in all_rows), default=None)
    return {
        "status": "success",
        "table": TABLE_NAME,
        "requested_stocks": len(jq_codes),
        "synced_stocks": len(ok_stocks),
        "rows": len(all_rows),
        "written_rows": written,
        "coverage": round(coverage, 4),
        "failed_batches": failed_batches,
        "latest_trade_time": latest_trade_time.isoformat(sep=" ", timespec="seconds") if latest_trade_time else None,
        "include_now": include_now,
        "dry_run": dry_run,
        "query_count": query_count,
        "generated_at": now.isoformat(sep=" ", timespec="seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync JoinQuant live minute bars into sm_stock_minute_gml")
    parser.add_argument("--universe", choices=["latest-kline", "si-all"], default="latest-kline")
    parser.add_argument("--codes", default="", help="Comma/space separated stock codes, overrides --universe")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--count", type=int, default=3, help="latest 1m bars per stock")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--complete-only", action="store_true", help="do not request the currently forming bar")
    parser.add_argument("--include-paused", action="store_true")
    parser.add_argument("--skip-closed", action="store_true")
    parser.add_argument("--include-bj", action="store_true", help="include Beijing Stock Exchange codes")
    parser.add_argument("--min-coverage", type=float, default=0.0)
    parser.add_argument("--pause-seconds", type=float, default=0.0)
    parser.add_argument("--skip-ddl", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    try:
        result = sync_jq_minute_gml(
            engine,
            universe=args.universe,
            codes=args.codes,
            limit=args.limit,
            count=args.count,
            batch_size=args.batch_size,
            include_now=not args.complete_only,
            skip_paused=not args.include_paused,
            skip_closed=args.skip_closed,
            min_coverage=args.min_coverage,
            include_bj=args.include_bj,
            dry_run=args.dry_run,
            skip_ddl=args.skip_ddl,
            pause_seconds=args.pause_seconds,
        )
    except Exception as exc:  # pylint: disable=broad-except
        result = {"status": "failed", "error": str(exc), "table": TABLE_NAME}
        if args.json:
            print(json.dumps(result, ensure_ascii=False, default=str))
        else:
            print(result, file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
