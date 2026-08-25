from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.auxiliary_runtime_schema import (
    validate_market_overview_daily_runtime_schema,
)
from server.common.kline_data import get_kline_engine
from tools.env_config import load_project_env


def _date_list(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def resolve_dates(kline_conn, *, dates: list[str], start_date: str, end_date: str) -> list[str]:
    if dates:
        return dates
    if not start_date and not end_date:
        row = kline_conn.execute(
            text(
                """
                SELECT MAX(trade_date)
                FROM sm_stock_kline
                WHERE k_type=1
                  AND adjust_type=0
                """
            )
        ).first()
        latest = row[0] if row else None
        return [str(latest)[:10]] if latest else []
    rows = kline_conn.execute(
        text(
            """
            SELECT DISTINCT trade_date
            FROM sm_stock_kline
            WHERE k_type=1
              AND adjust_type=0
              AND (:start_date='' OR trade_date >= :start_date)
              AND (:end_date='' OR trade_date <= :end_date)
            ORDER BY trade_date
            """
        ),
        {"start_date": start_date, "end_date": end_date},
    ).fetchall()
    return [str(row[0])[:10] for row in rows if row[0]]


def refresh_one(conn, kline_conn, trade_date: str) -> dict:
    row = (
        kline_conn.execute(
            text(
                """
                SELECT
                    SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt,
                    SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_cnt,
                    SUM(CASE WHEN ABS(change_pct) < 1 OR change_pct IS NULL THEN 1 ELSE 0 END) AS sideline_cnt,
                    COUNT(*) AS total,
                    COALESCE(SUM(amount), 0) AS total_amount,
                    SUM(CASE
                          WHEN (stock_code LIKE '002%%' OR stock_code LIKE '300%%' OR stock_code LIKE '301%%')
                           AND change_pct > 0 THEN 1 ELSE 0
                        END) AS small_up_cnt,
                    SUM(CASE
                          WHEN (stock_code LIKE '002%%' OR stock_code LIKE '300%%' OR stock_code LIKE '301%%')
                          THEN 1 ELSE 0
                        END) AS small_total,
                    AVG(CASE
                          WHEN (stock_code LIKE '002%%' OR stock_code LIKE '300%%' OR stock_code LIKE '301%%')
                          THEN change_pct
                        END) AS small_avg_chg,
                    MAX(quality_status) AS quality_status
                FROM sm_stock_kline
                WHERE trade_date=:trade_date
                  AND k_type=1
                  AND adjust_type=0
                """
            ),
            {"trade_date": trade_date},
        )
        .mappings()
        .first()
        or {}
    )
    total = int(row.get("total") or 0)
    if total <= 0:
        return {"date": trade_date, "status": "skip_empty"}
    conn.execute(
        text(
            """
            INSERT INTO sm_market_overview_daily (
                trade_date, up_cnt, down_cnt, sideline_cnt, total, total_amount,
                small_up_cnt, small_total, small_avg_chg, source_table, quality_status, updated_at
            )
            VALUES (
                :trade_date, :up_cnt, :down_cnt, :sideline_cnt, :total, :total_amount,
                :small_up_cnt, :small_total, :small_avg_chg, 'sm_stock_kline', :quality_status, NOW()
            )
            ON DUPLICATE KEY UPDATE
                up_cnt=VALUES(up_cnt),
                down_cnt=VALUES(down_cnt),
                sideline_cnt=VALUES(sideline_cnt),
                total=VALUES(total),
                total_amount=VALUES(total_amount),
                small_up_cnt=VALUES(small_up_cnt),
                small_total=VALUES(small_total),
                small_avg_chg=VALUES(small_avg_chg),
                source_table=VALUES(source_table),
                quality_status=VALUES(quality_status),
                updated_at=NOW()
            """
        ),
        {
            "trade_date": trade_date,
            "up_cnt": int(row.get("up_cnt") or 0),
            "down_cnt": int(row.get("down_cnt") or 0),
            "sideline_cnt": int(row.get("sideline_cnt") or 0),
            "total": total,
            "total_amount": row.get("total_amount") or 0,
            "small_up_cnt": int(row.get("small_up_cnt") or 0),
            "small_total": int(row.get("small_total") or 0),
            "small_avg_chg": row.get("small_avg_chg"),
            "quality_status": row.get("quality_status"),
        },
    )
    return {"date": trade_date, "status": "ok", "total": total}


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh daily market overview summary for monitor page.")
    parser.add_argument("date_arg", nargs="?", default="", help="Trade date, for scheduler positional-date compatibility.")
    parser.add_argument("--dates", default="", help="Comma separated trade dates.")
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    args = parser.parse_args()

    load_project_env()
    engine = create_batch_engine(future=True)
    kline_engine = get_kline_engine()
    explicit_dates = _date_list(args.dates or args.date_arg)
    validate_market_overview_daily_runtime_schema(engine)
    with engine.begin() as conn:
        with kline_engine.connect() as kline_conn:
            dates = resolve_dates(kline_conn, dates=explicit_dates, start_date=args.start_date, end_date=args.end_date)
            for trade_date in dates:
                print(refresh_one(conn, kline_conn, trade_date), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
