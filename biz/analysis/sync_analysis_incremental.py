# -*- coding: utf-8 -*-
"""Intraday incremental refresh using the unified fast-analysis pipeline."""

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
from biz.analysis.sync_analysis_result import resolve_trade_date
from server.api.routers._engine import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_portfolio_stocks() -> list[str]:
    sql = """
        SELECT stock_code
        FROM st_user_portfolio
        WHERE shares > 0 OR is_holding = 1
        ORDER BY sort_order
    """
    df = pd.read_sql(text(sql), get_engine())
    if df.empty:
        return []
    return df["stock_code"].astype(str).str.strip().str.zfill(6).tolist()


def get_recommended_stocks() -> list[str]:
    sql = """
        SELECT DISTINCT stock_code
        FROM st_recommended_stocks
        WHERE pick_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
          AND (recommend_status IS NULL OR recommend_status = 'ALLOW')
    """
    df = pd.read_sql(text(sql), get_engine())
    if df.empty:
        return []
    return df["stock_code"].astype(str).str.strip().str.zfill(6).tolist()


def main() -> int:
    start_time = datetime.now()
    portfolio_codes = get_portfolio_stocks()
    recommended_codes = get_recommended_stocks()
    stock_codes = sorted(set(portfolio_codes + recommended_codes))
    if not stock_codes:
        logger.info("Incremental refresh skipped: no holdings or recommended stocks")
        return 0

    trade_date = resolve_trade_date("")
    logger.info(
        "Unified incremental refresh started: trade_date=%s portfolio=%s recommended=%s total=%s",
        trade_date,
        len(portfolio_codes),
        len(recommended_codes),
        len(stock_codes),
    )
    stats = run_batch_for_codes(
        engine=get_engine(),
        stock_codes=stock_codes,
        trade_date=trade_date,
        top_n=max(80, len(stock_codes)),
        min_score=62.0,
    )

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(
        "Unified incremental refresh completed: date=%s analysis=%s recommendations=%s market_mood=%.1f cost=%.1fs",
        stats.trade_date,
        stats.analysis_count,
        stats.recommendation_count,
        stats.market_mood_score,
        duration,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
