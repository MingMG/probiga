#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build one daily K-line date from intraday minute bars and close snapshots.

This is the post-close fast path used when the external daily K-line source is
too slow. Rows with minute bars use real minute aggregation. Stocks without
minute bars fall back to the latest close snapshot so the UI can move to the
current trading day immediately after close.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, read_frame, write_frame  # noqa: E402
from server.common.config import get_kline_mysql_url  # noqa: E402


KLINE_COLUMNS = [
    "stock_code",
    "short_name",
    "trade_time",
    "trade_date",
    "k_type",
    "adjust_type",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "amount",
    "change",
    "change_pct",
    "turnover_ratio",
    "pre_close",
    "etl_sync_at",
]


def _normalize_date(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value[:10]


def _expected_trade_date(engine: Engine) -> str:
    today = date.today().isoformat()
    with engine.connect() as conn:
        value = conn.execute(
            text(
                """
                SELECT MAX(trade_date)
                FROM si_trade_calendar
                WHERE trade_status = 1 AND trade_date <= :today
                """
            ),
            {"today": today},
        ).scalar()
    return str(value or today)[:10]


def _read_daily_rows(engine: Engine, trade_date: str) -> pd.DataFrame:
    return read_frame(
        text(
            """
            SELECT stock_code, short_name, trade_time, trade_date, k_type, adjust_type,
                   open, close, high, low, volume, amount, `change`, change_pct,
                   turnover_ratio, pre_close, etl_sync_at
            FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
            """
        ),
        engine,
        params={"d": trade_date},
        )


def _write_daily_rows(engine: Engine, trade_date: str, rows: pd.DataFrame) -> int:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM sm_stock_kline
                WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
                """
            ),
            {"d": trade_date},
        )
    if rows.empty:
        return 0
    return write_frame(
        rows[KLINE_COLUMNS],
        "sm_stock_kline",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )


def _mirror_to_kline_read_db(trade_date: str, rows: pd.DataFrame) -> int:
    kline_url = get_kline_mysql_url().strip()
    if not kline_url:
        return 0
    kline_engine = create_batch_engine(kline_url)
    return _write_daily_rows(kline_engine, trade_date, rows)


def build_daily_kline_from_intraday(
    trade_date: str = "",
    *,
    min_current_count: int = 3000,
    min_output_count: int = 3000,
    mirror_read_db: bool = True,
) -> dict:
    engine = create_batch_engine()
    trade_date = _normalize_date(trade_date) or _expected_trade_date(engine)
    datetime.strptime(trade_date, "%Y-%m-%d")

    with engine.begin() as conn:
        minute_stats = conn.execute(
            text(
                """
                SELECT COUNT(*) AS rows_count, COUNT(DISTINCT stock_code) AS stock_count,
                       MIN(trade_time) AS min_time, MAX(trade_time) AS max_time
                FROM sm_stock_minute
                WHERE trade_date = :d AND price > 0
                """
            ),
            {"d": trade_date},
        ).mappings().first() or {}
        current_stats = conn.execute(
            text(
                """
                SELECT COUNT(DISTINCT stock_code) AS stock_count, MAX(snapshot_at) AS max_snapshot
                FROM sm_stock_current
                WHERE DATE(snapshot_at) = :d AND price > 0
                """
            ),
            {"d": trade_date},
        ).mappings().first() or {}
        current_count = int(current_stats.get("stock_count") or 0)
        if current_count < int(min_current_count):
            raise RuntimeError(
                f"current snapshot coverage too low for {trade_date}: "
                f"{current_count} < {int(min_current_count)}"
            )

        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_intraday_daily"))
        conn.execute(
            text(
                """
                CREATE TEMPORARY TABLE tmp_intraday_daily AS
                SELECT stock_code, trade_date, MIN(trade_time) AS first_time,
                       MAX(trade_time) AS last_time, MAX(price) AS high_price,
                       MIN(price) AS low_price, SUM(volume) AS total_volume,
                       SUM(amount) AS total_amount, COUNT(*) AS bar_count
                FROM sm_stock_minute
                WHERE trade_date = :d AND price > 0
                GROUP BY stock_code, trade_date
                """
            ),
            {"d": trade_date},
        )
        conn.execute(text("ALTER TABLE tmp_intraday_daily ADD PRIMARY KEY (stock_code)"))

        before = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM sm_stock_kline
                    WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
                    """
                ),
                {"d": trade_date},
            ).scalar() or 0
        )
        conn.execute(
            text(
                """
                DELETE FROM sm_stock_kline
                WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
                """
            ),
            {"d": trade_date},
        )
        inserted_minute = conn.execute(
            text(
                """
                INSERT INTO sm_stock_kline (
                    stock_code, short_name, trade_time, trade_date, k_type, adjust_type,
                    open, close, high, low, volume, amount, `change`, change_pct,
                    turnover_ratio, pre_close, etl_sync_at
                )
                SELECT
                    s.stock_code,
                    COALESCE(NULLIF(a.short_name, ''), s.stock_code) AS short_name,
                    lm.trade_time,
                    s.trade_date,
                    1,
                    0,
                    fm.price AS open,
                    lm.price AS close,
                    s.high_price AS high,
                    s.low_price AS low,
                    s.total_volume AS volume,
                    s.total_amount AS amount,
                    COALESCE(c.`change`, CASE WHEN pk.close > 0 THEN lm.price - pk.close END, lm.`change`),
                    COALESCE(c.change_pct, CASE WHEN pk.close > 0 THEN (lm.price / pk.close - 1) * 100 END, lm.change_pct),
                    NULL AS turnover_ratio,
                    COALESCE(
                        CASE WHEN c.change_pct IS NOT NULL AND c.change_pct <> 0
                            THEN lm.price / NULLIF(1 + c.change_pct / 100, 0) END,
                        CASE WHEN c.`change` IS NOT NULL AND c.`change` <> 0
                            THEN lm.price - c.`change` END,
                        pk.close
                    ) AS pre_close,
                    NOW()
                FROM tmp_intraday_daily s
                JOIN sm_stock_minute fm
                  ON fm.stock_code = s.stock_code AND fm.trade_date = s.trade_date
                 AND fm.trade_time = s.first_time
                JOIN sm_stock_minute lm
                  ON lm.stock_code = s.stock_code AND lm.trade_date = s.trade_date
                 AND lm.trade_time = s.last_time
                LEFT JOIN sm_stock_current c
                  ON c.stock_code = s.stock_code AND DATE(c.snapshot_at) = :d
                LEFT JOIN si_all_code a ON a.stock_code = s.stock_code
                LEFT JOIN sm_stock_kline pk
                  ON pk.stock_code = s.stock_code
                 AND pk.trade_date = (
                    SELECT MAX(trade_date)
                    FROM sm_stock_kline
                    WHERE trade_date < :d AND k_type = 1 AND adjust_type = 0
                 )
                 AND pk.k_type = 1 AND pk.adjust_type = 0
                """
            ),
            {"d": trade_date},
        ).rowcount or 0
        inserted_current = conn.execute(
            text(
                """
                INSERT INTO sm_stock_kline (
                    stock_code, short_name, trade_time, trade_date, k_type, adjust_type,
                    open, close, high, low, volume, amount, `change`, change_pct,
                    turnover_ratio, pre_close, etl_sync_at
                )
                SELECT
                    c.stock_code,
                    COALESCE(NULLIF(a.short_name, ''), c.stock_code),
                    CONCAT(:d, ' 15:00:00'),
                    :d,
                    1,
                    0,
                    c.price, c.price, c.price, c.price,
                    c.volume, c.amount, c.`change`, c.change_pct,
                    NULL AS turnover_ratio,
                    COALESCE(
                        CASE WHEN c.change_pct IS NOT NULL AND c.change_pct <> 0
                            THEN c.price / NULLIF(1 + c.change_pct / 100, 0) END,
                        CASE WHEN c.`change` IS NOT NULL AND c.`change` <> 0
                            THEN c.price - c.`change` END,
                        pk.close
                    ) AS pre_close,
                    NOW()
                FROM sm_stock_current c
                LEFT JOIN tmp_intraday_daily m ON m.stock_code = c.stock_code
                LEFT JOIN si_all_code a ON a.stock_code = c.stock_code
                LEFT JOIN sm_stock_kline pk
                  ON pk.stock_code = c.stock_code
                 AND pk.trade_date = (
                    SELECT MAX(trade_date)
                    FROM sm_stock_kline
                    WHERE trade_date < :d AND k_type = 1 AND adjust_type = 0
                )
                 AND pk.k_type = 1 AND pk.adjust_type = 0
                WHERE DATE(c.snapshot_at) = :d
                  AND c.price > 0
                  AND m.stock_code IS NULL
                """
            ),
            {"d": trade_date},
        ).rowcount or 0
        after_row = conn.execute(
            text(
                """
                SELECT COUNT(*) AS rows_count, COUNT(DISTINCT stock_code) AS stock_count
                FROM sm_stock_kline
                WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
                """
            ),
            {"d": trade_date},
        ).mappings().first() or {}

    rows_count = int(after_row.get("rows_count") or 0)
    if rows_count < int(min_output_count):
        raise RuntimeError(f"daily K output coverage too low for {trade_date}: {rows_count} < {min_output_count}")
    mirrored = 0
    if mirror_read_db:
        mirrored = _mirror_to_kline_read_db(trade_date, _read_daily_rows(engine, trade_date))
    return {
        "trade_date": trade_date,
        "before": before,
        "inserted_minute": int(inserted_minute),
        "inserted_current_fallback": int(inserted_current),
        "rows_count": rows_count,
        "stock_count": int(after_row.get("stock_count") or 0),
        "mirrored": int(mirrored),
        "minute_rows": int(minute_stats.get("rows_count") or 0),
        "minute_stocks": int(minute_stats.get("stock_count") or 0),
        "current_stocks": current_count,
        "current_snapshot": str(current_stats.get("max_snapshot") or ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build daily K-line from intraday minute/current data.")
    parser.add_argument("date", nargs="?", default="", help="Trade date YYYY-MM-DD; default latest trade day")
    parser.add_argument("--min-current-count", type=int, default=3000)
    parser.add_argument("--min-output-count", type=int, default=3000)
    parser.add_argument("--no-mirror-read-db", action="store_true")
    args = parser.parse_args()
    result = build_daily_kline_from_intraday(
        args.date,
        min_current_count=args.min_current_count,
        min_output_count=args.min_output_count,
        mirror_read_db=not args.no_mirror_read_db,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
