# -*- coding: utf-8 -*-
"""Unified batch-analysis entrypoint."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path as _Path

from sqlalchemy import text

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from biz.analysis.sync_analysis_fast import run_batch, run_batch_for_codes
from server.common.batch_db import create_batch_engine, read_frame

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_engine():
    return create_batch_engine()


def get_all_active_stocks(limit: int | None = None) -> list[str]:
    sql = """
        SELECT stock_code
        FROM si_all_code
        WHERE stock_code NOT LIKE '4%'
          AND stock_code NOT LIKE '8%'
          AND stock_code NOT LIKE '9%'
        ORDER BY stock_code
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    df = read_frame(text(sql), get_engine())
    if df.empty:
        return []
    return df["stock_code"].astype(str).str.strip().str.zfill(6).tolist()


def resolve_trade_date(trade_date: str = "") -> str:
    sql = "SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type = 1"
    params = {}
    trade_date = (trade_date or "").strip()
    if trade_date:
        sql += " AND trade_date <= :trade_date"
        params["trade_date"] = trade_date
    rows = read_frame(text(sql), get_engine(), params=params)
    if rows.empty or not rows.iloc[0]["d"]:
        raise RuntimeError(f"No daily K-line data found for {trade_date or 'latest'}")
    return str(rows.iloc[0]["d"])[:10]


def main() -> int:
    parser = argparse.ArgumentParser(description="Unified batch analysis entrypoint")
    parser.add_argument("--limit", type=int, help="Recompute only the first N stocks")
    parser.add_argument("--code", type=str, help="Recompute only one stock")
    parser.add_argument("--trade-date", type=str, default="", help="Analysis cutoff trade date")
    parser.add_argument("--top-n", type=int, default=80, help="Recommendation pool size")
    parser.add_argument("--min-score", type=float, default=62.0, help="Minimum recommendation score")
    args = parser.parse_args()

    start_time = datetime.now()
    trade_date = resolve_trade_date(args.trade_date)
    logger.info("Unified analysis started: trade_date=%s", trade_date)

    engine = get_engine()
    if args.code:
        stock_codes = [args.code.strip().zfill(6)]
        stats = run_batch_for_codes(
            engine=engine,
            stock_codes=stock_codes,
            trade_date=trade_date,
            top_n=max(args.top_n, len(stock_codes)),
            min_score=float(args.min_score),
        )
    elif args.limit:
        stock_codes = get_all_active_stocks(limit=args.limit)
        if not stock_codes:
            raise RuntimeError("No stocks available for analysis")
        stats = run_batch_for_codes(
            engine=engine,
            stock_codes=stock_codes,
            trade_date=trade_date,
            top_n=max(args.top_n, len(stock_codes)),
            min_score=float(args.min_score),
        )
    else:
        stats = run_batch(
            engine=engine,
            trade_date=trade_date,
            top_n=args.top_n,
            min_score=float(args.min_score),
        )

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(
        "Unified analysis completed: date=%s analysis=%s recommendations=%s market_mood=%.1f cost=%.1fs",
        stats.trade_date,
        stats.analysis_count,
        stats.recommendation_count,
        stats.market_mood_score,
        duration,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
