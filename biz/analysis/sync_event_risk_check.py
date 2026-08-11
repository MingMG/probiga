# -*- coding: utf-8 -*-
"""Weekend/event-risk refresh using the unified fast-analysis pipeline."""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path as _Path

import pandas as pd
from sqlalchemy import text

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from biz.analysis.sync_analysis_fast import run_batch_for_codes
from server.common.batch_db import create_batch_engine, read_frame

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_engine():
    return create_batch_engine()


def get_allowed_recommendations() -> tuple[str, list[str]]:
    engine = get_engine()
    latest_rows = read_frame(text("SELECT MAX(analysis_date) AS d FROM stock_analysis_result"), engine)
    if latest_rows.empty or not latest_rows.iloc[0]["d"]:
        return "", []
    analysis_date = str(latest_rows.iloc[0]["d"])[:10]
    df = read_frame(
        text("""
            SELECT stock_code
            FROM stock_analysis_result
            WHERE analysis_date = :analysis_date
              AND recommend_status = 'ALLOW'
            ORDER BY short_term_score DESC
        """),
        engine,
        engine,
        params={"analysis_date": analysis_date},
    )
    if df.empty:
        return analysis_date, []
    codes = df["stock_code"].astype(str).str.strip().str.zfill(6).tolist()
    return analysis_date, codes


def main() -> int:
    start_time = datetime.now()
    analysis_date, stock_codes = get_allowed_recommendations()
    if not stock_codes:
        logger.info("Event-risk refresh skipped: no ALLOW recommendations or no analysis snapshot")
        return 0

    logger.info(
        "Unified event-risk refresh started: analysis_date=%s allow_count=%s",
        analysis_date,
        len(stock_codes),
    )
    stats = run_batch_for_codes(
        engine=get_engine(),
        stock_codes=stock_codes,
        trade_date=analysis_date,
        top_n=max(80, len(stock_codes)),
        min_score=62.0,
    )

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(
        "Unified event-risk refresh completed: date=%s analysis=%s recommendations=%s market_mood=%.1f cost=%.1fs",
        stats.trade_date,
        stats.analysis_count,
        stats.recommendation_count,
        stats.market_mood_score,
        duration,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
