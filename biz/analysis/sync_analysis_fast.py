# -*- coding: utf-8 -*-
"""
Fast end-of-day analysis and recommendation batch.

The existing deep analysis engine is useful for a small candidate list, but it is
too slow to cover the whole A-share universe every day. This job builds a
complete baseline from synchronized tables, writes stock_analysis_result for all
latest K-line rows, and refreshes st_recommended_stocks with the best ALLOW
candidates.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[dict[str, Any]], None]

CRITICAL_NOTICE_KEYWORDS = (
    "退市", "终止上市", "暂停上市", "重大违法", "被立案", "立案调查", "欺诈发行",
)
NEGATIVE_NOTICE_KEYWORDS = (
    "减持", "处罚", "问询函", "监管函", "警示函", "诉讼", "仲裁", "冻结",
    "质押", "亏损", "预亏", "业绩预降", "下修", "债务", "违约", "风险提示",
)
POSITIVE_NOTICE_KEYWORDS = (
    "回购", "增持", "中标", "签订合同", "战略合作", "股权激励", "分红", "利润分配",
    "业绩预增", "扭亏", "订单", "投资者回报",
)


@dataclass(frozen=True)
class BatchStats:
    trade_date: str
    analysis_count: int
    recommendation_count: int
    market_mood_score: float
    flow_date: str
    hot_date: str


MODEL_VERSION = "ai-rec-v3-mainwave"

STRATEGY_PROFILES: dict[str, dict[str, Any]] = {
    "ultra_short": {
        "label": "ultra-short",
        "min_score": 68.0,
        "confirm_score": 76.0,
        "max_holding_days": 3,
        "base_position": 4.0,
        "stop_loss_pct": -3.5,
        "take_profit_1_pct": 5.0,
        "take_profit_2_pct": 8.0,
        "cooldown_days": 3,
    },
    "short_term": {
        "label": "short-term",
        "min_score": 68.0,
        "confirm_score": 74.0,
        "max_holding_days": 10,
        "base_position": 6.0,
        "stop_loss_pct": -5.5,
        "take_profit_1_pct": 8.0,
        "take_profit_2_pct": 15.0,
        "cooldown_days": 5,
    },
    "swing": {
        "label": "swing",
        "min_score": 66.0,
        "confirm_score": 72.0,
        "max_holding_days": 30,
        "base_position": 8.0,
        "stop_loss_pct": -8.0,
        "take_profit_1_pct": 15.0,
        "take_profit_2_pct": 30.0,
        "cooldown_days": 10,
    },
    "main_wave": {
        "label": "main-wave",
        "min_score": 70.0,
        "confirm_score": 74.0,
        "max_holding_days": 60,
        "base_position": 7.0,
        "stop_loss_pct": -10.0,
        "take_profit_1_pct": 35.0,
        "take_profit_2_pct": 80.0,
        "cooldown_days": 15,
    },
}


def clamp_score(value: float | int | None, low: float = 0.0, high: float = 100.0) -> float:
    """Clamp a scalar score into 0-100 range."""
    if value is None:
        return 50.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 50.0
    if math.isnan(number) or math.isinf(number):
        return 50.0
    return round(max(low, min(high, number)), 1)


def linear_score(value: float | int | None, low: float, high: float, default: float = 50.0) -> float:
    """Map a scalar value linearly to a 0-100 score."""
    if high == low:
        return clamp_score(default)
    if value is None:
        return clamp_score(default)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return clamp_score(default)
    if math.isnan(number) or math.isinf(number):
        return clamp_score(default)
    return clamp_score((number - low) / (high - low) * 100.0)


def classify_notice_title(title: str | None) -> dict[str, Any]:
    """Classify one notice title for event-risk scoring."""
    title = title or ""
    critical_hits = [kw for kw in CRITICAL_NOTICE_KEYWORDS if kw in title]
    negative_hits = [kw for kw in NEGATIVE_NOTICE_KEYWORDS if kw in title]
    positive_hits = [kw for kw in POSITIVE_NOTICE_KEYWORDS if kw in title]
    return {
        "critical": len(critical_hits),
        "negative": len(negative_hits),
        "positive": len(positive_hits),
        "keywords": critical_hits + negative_hits + positive_hits,
    }


def build_data_quality(row: dict[str, Any], trade_date: str, flow_date: str) -> tuple[float, list[str]]:
    """Score whether one row has enough fresh source data to trust the ranking."""
    flags: list[str] = []
    score = 100.0

    finance_fields = ("roe_wtd", "gross_margin", "net_margin", "asset_liab_ratio")
    if not any(_safe_number(row.get(field), 0.0) for field in finance_fields):
        flags.append("missing_finance")
        score -= 28.0

    if _safe_number(row.get("main_net_inflow"), 0.0) == 0.0 and _safe_number(row.get("main_net_inflow_5d"), 0.0) == 0.0:
        flags.append("missing_flow")
        score -= 24.0
    elif flow_date and flow_date != trade_date:
        flags.append("stale_flow")
        score -= 12.0

    fused_rank = row.get("fused_rank")
    if fused_rank is None or pd.isna(fused_rank):
        flags.append("missing_hot_rank")
        score -= 10.0

    industry_name = str(row.get("industry_name") or "").strip()
    if not industry_name:
        flags.append("missing_industry")
        score -= 6.0

    if row.get("latest_notice_date") is None and int(_safe_number(row.get("notice_count"), 0.0)) == 0:
        flags.append("missing_notice_context")
        score -= 4.0

    return clamp_score(score), flags


def choose_recommend_status(
    stock_code: str,
    short_name: str,
    ai_score: float,
    short_term_score: float,
    long_term_score: float,
    event_risk_level: str,
    amount: float | None,
    change_pct: float | None,
    min_score: float,
    data_quality_score: float = 100.0,
    data_quality_flags: list[str] | None = None,
) -> tuple[str, str]:
    """Gate recommendation eligibility. It is intentionally conservative."""
    name = short_name or ""
    amount = float(amount or 0)
    change_pct = float(change_pct or 0)
    code = str(stock_code).zfill(6)
    flags = set(data_quality_flags or [])

    if "ST" in name.upper() or "退" in name:
        return "BLOCK", "ST或退市风险标的，不进入推荐池"
    if not code.startswith(("0", "3", "6")):
        return "BLOCK", "非沪深A股主代码，不进入推荐池"
    if event_risk_level == "CRITICAL":
        return "BLOCK", "公告存在重大事件风险"
    if "missing_finance" in flags or "missing_flow" in flags:
        return "SUSPENDED", "关键数据缺失，财务或资金流不完整"
    if data_quality_score < 70:
        return "SUSPENDED", f"数据质量分为{data_quality_score:.1f}，暂不进入推荐池"
    if short_term_score < 40 or long_term_score < 35:
        return "BLOCK", "基础评分过低"
    if event_risk_level == "HIGH":
        return "SUSPENDED", "公告风险较高，等待风险消化"
    if ai_score < min_score:
        return "SUSPENDED", "综合评分未达到推荐阈值"
    if "stale_flow" in flags and ai_score < min_score + 6:
        return "SUSPENDED", "资金流数据不是当日，暂缓推荐"
    if amount < 30_000_000:
        return "SUSPENDED", "成交额偏低，流动性不足"
    if change_pct >= 9.7:
        return "SUSPENDED", "当日涨幅过高，避免追高"
    return "ALLOW", "基础评分通过，仍需盘中量价确认"


def _series_score(values: pd.Series, low: float, high: float, default: float = 50.0) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce")
    if high == low:
        return pd.Series(default, index=values.index, dtype="float64")
    scores = (values - low) / (high - low) * 100.0
    return scores.clip(0, 100).fillna(default)


def _percentile_score(values: pd.Series, default: float = 50.0, ascending: bool = True) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce")
    if values.notna().sum() < 2:
        return pd.Series(default, index=values.index, dtype="float64")
    ranks = values.rank(pct=True, ascending=ascending) * 100.0
    return ranks.fillna(default)


def _round_score(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").clip(0, 100).round(1)


def _safe_number(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _valid_price(value: Any) -> bool:
    return _safe_number(value, 0.0) > 0


def _round_price(value: Any) -> float | None:
    number = _safe_number(value, 0.0)
    if number <= 0:
        return None
    return round(number, 2)


def _first_price(*values: Any, default: float = 0.0) -> float:
    for value in values:
        number = _safe_number(value, 0.0)
        if number > 0:
            return number
    return default


def _numeric_col(df: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _ensure_columns(df: pd.DataFrame, defaults: dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default
    return out


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name}).fetchall()
    return {str(r[0]) for r in rows}


def _ensure_recommended_columns(engine: Engine) -> None:
    required = {
        "long_term_score": "DECIMAL(5,1) DEFAULT NULL",
        "short_term_score": "DECIMAL(5,1) DEFAULT NULL",
        "recommend_status": "VARCHAR(10) DEFAULT 'ALLOW'",
        "recommend_reason": "VARCHAR(500) DEFAULT ''",
        "event_risk_level": "VARCHAR(10) DEFAULT 'LOW'",
        "last_check_time": "DATETIME DEFAULT NULL",
        "sentiment_score": "DECIMAL(5,1) DEFAULT NULL",
        "market_mood_score": "DECIMAL(5,1) DEFAULT NULL",
        "event_score": "DECIMAL(5,1) DEFAULT NULL",
        "ultra_short_score": "DECIMAL(5,1) DEFAULT NULL",
        "swing_score": "DECIMAL(5,1) DEFAULT NULL",
        "primary_strategy": "VARCHAR(20) DEFAULT ''",
        "strategy_profile": "VARCHAR(20) DEFAULT ''",
        "suitable_strategies": "TEXT NULL",
        "signal_status": "VARCHAR(20) DEFAULT 'WATCH'",
        "signal_reason": "VARCHAR(500) DEFAULT ''",
        "entry_price_low": "DECIMAL(12,4) DEFAULT NULL",
        "entry_price_high": "DECIMAL(12,4) DEFAULT NULL",
        "stop_loss_price": "DECIMAL(12,4) DEFAULT NULL",
        "take_profit_1": "DECIMAL(12,4) DEFAULT NULL",
        "take_profit_2": "DECIMAL(12,4) DEFAULT NULL",
        "position_weight": "DECIMAL(5,2) DEFAULT NULL",
        "max_holding_days": "INT DEFAULT NULL",
        "entry_conditions_json": "TEXT NULL",
        "sell_rules_json": "TEXT NULL",
        "invalidation_reason": "VARCHAR(500) DEFAULT ''",
        "quality_score": "DECIMAL(5,1) DEFAULT NULL",
        "entry_score": "DECIMAL(5,1) DEFAULT NULL",
        "final_trade_score": "DECIMAL(5,1) DEFAULT NULL",
        "expected_return_score": "DECIMAL(5,1) DEFAULT NULL",
        "expected_return_pct": "DECIMAL(8,2) DEFAULT NULL",
        "resistance_price": "DECIMAL(12,4) DEFAULT NULL",
        "heat_overload_score": "DECIMAL(5,1) DEFAULT NULL",
        "confidence_score": "DECIMAL(5,1) DEFAULT NULL",
        "sector_rotation_score": "DECIMAL(5,1) DEFAULT NULL",
        "failure_penalty_score": "DECIMAL(5,1) DEFAULT NULL",
        "data_quality_score": "DECIMAL(5,1) DEFAULT NULL",
        "data_quality_flags": "TEXT NULL",
        "cooldown_days_left": "INT DEFAULT 0",
        "cooldown_until": "DATE DEFAULT NULL",
        "main_wave_score": "DECIMAL(5,1) DEFAULT NULL",
        "trend_hold_score": "DECIMAL(5,1) DEFAULT NULL",
        "main_wave_stage": "VARCHAR(30) DEFAULT ''",
        "main_wave_signal": "VARCHAR(30) DEFAULT ''",
        "main_wave_reason": "VARCHAR(500) DEFAULT ''",
        "trend_stop_price": "DECIMAL(12,4) DEFAULT NULL",
        "trend_reduce_price": "DECIMAL(12,4) DEFAULT NULL",
        "model_version": "VARCHAR(20) DEFAULT ''",
    }
    existing = _table_columns(engine, "st_recommended_stocks")
    with engine.begin() as conn:
        for column, ddl in required.items():
            if column not in existing:
                logger.info("Adding st_recommended_stocks.%s", column)
                conn.execute(text(f"ALTER TABLE st_recommended_stocks ADD COLUMN `{column}` {ddl}"))


def _ensure_analysis_columns(engine: Engine) -> None:
    required = {
        "model_version": "VARCHAR(20) DEFAULT ''",
        "data_quality_score": "DECIMAL(5,1) DEFAULT NULL",
        "data_quality_flags": "TEXT NULL",
        "flow_trade_date": "DATE DEFAULT NULL",
        "hot_trade_date": "DATE DEFAULT NULL",
    }
    existing = _table_columns(engine, "stock_analysis_result")
    with engine.begin() as conn:
        for column, ddl in required.items():
            if column not in existing:
                logger.info("Adding stock_analysis_result.%s", column)
                conn.execute(text(f"ALTER TABLE stock_analysis_result ADD COLUMN `{column}` {ddl}"))


def latest_trade_date(engine: Engine) -> str:
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
        """)).scalar()
    if not value:
        raise RuntimeError("sm_stock_kline has no daily K-line data")
    return str(value)[:10]


def _parse_execution_date(execution_time: str | None = None) -> date:
    raw = (execution_time or "").strip()
    if not raw:
        return date.today()
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid execution_time: {execution_time}") from exc


def previous_trade_date(engine: Engine, execution_time: str | None = None) -> str:
    """Resolve the strict previous trading day for morning recommendation runs."""
    ref_date = _parse_execution_date(execution_time).isoformat()
    with engine.connect() as conn:
        has_calendar = bool(conn.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'si_trade_calendar'
        """)).scalar())
        if has_calendar:
            cal_columns = _table_columns(engine, "si_trade_calendar")
            status_filter = ""
            if "trade_status" in cal_columns:
                status_filter = "AND trade_status = 1"
            elif "is_open" in cal_columns:
                status_filter = "AND is_open = 1"
            value = conn.execute(text(f"""
                SELECT MAX(trade_date)
                FROM si_trade_calendar
                WHERE trade_date < :ref_date
                  {status_filter}
            """), {"ref_date": ref_date}).scalar()
            if value:
                return str(value)[:10]
        value = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date < :ref_date
        """), {"ref_date": ref_date}).scalar()
    if not value:
        raise RuntimeError(f"Cannot resolve previous trade date before {ref_date}")
    return str(value)[:10]


def assert_trade_date_ready(engine: Engine, trade_date: str, min_coverage: float = 0.80) -> dict[str, Any]:
    """Fail fast when the target trading day is missing or clearly incomplete."""
    trade_date = str(trade_date or "").strip()[:10]
    if not trade_date:
        raise ValueError("trade_date is required")
    min_coverage = max(0.0, min(1.0, float(min_coverage)))
    with engine.connect() as conn:
        latest = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
        """)).scalar()
        kline_count = int(conn.execute(text("""
            SELECT COUNT(DISTINCT stock_code)
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date = :trade_date
        """), {"trade_date": trade_date}).scalar() or 0)
        expected_count = int(conn.execute(text("""
            SELECT COUNT(DISTINCT stock_code)
            FROM si_all_code
            WHERE stock_code REGEXP '^(0|3|6)'
        """)).scalar() or 0)
    latest_s = str(latest)[:10] if latest else ""
    if latest_s and latest_s < trade_date:
        raise RuntimeError(f"K-line latest date is {latest_s}, earlier than required {trade_date}")
    minimum = int(expected_count * min_coverage) if expected_count else 1
    if kline_count < minimum:
        raise RuntimeError(
            f"K-line coverage for {trade_date} is incomplete: {kline_count}/{expected_count or '?'} "
            f"(required >= {minimum})"
        )
    return {
        "trade_date": trade_date,
        "latest_kline_date": latest_s,
        "kline_count": kline_count,
        "expected_count": expected_count,
        "min_coverage": min_coverage,
    }


def _tail_text(value: str | None, limit: int = 4000) -> str:
    text_value = value or ""
    if len(text_value) <= limit:
        return text_value
    return text_value[-limit:]


def repair_missing_qmt_kline_for_trade_date(
    trade_date: str,
    progress_callback: ProgressCallback | None = None,
    timeout_seconds: int = 7200,
) -> dict[str, Any]:
    """Fetch one full-market daily K-line date from Guojin QMT before strict recommendation runs."""
    trade_date = str(trade_date or "").strip()[:10]
    if not trade_date:
        raise ValueError("trade_date is required for QMT K-line repair")

    cmd = [
        sys.executable,
        "-m",
        "biz.stock_market.sync_stock_market",
        "--only",
        "stock_kline",
        "--kline-source",
        "qmt",
        "--kline-start",
        trade_date,
        "--kline-end",
        trade_date,
        "--kline-incremental",
        "--max-stocks",
        "0",
        "--skip-progress",
    ]
    env = os.environ.copy()
    env["SM_STOCK_KLINE_SOURCE"] = "qmt"
    env["SM_MAX_STOCKS"] = "0"
    env["SM_SKIP_GLOBAL_TRUNCATE"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not existing_pythonpath else f"{ROOT}{os.pathsep}{existing_pythonpath}"

    _emit_progress(
        progress_callback,
        stage="repair_kline_start",
        percent=3,
        step=f"strict data missing; repairing Guojin QMT K-line for {trade_date}",
        trade_date=trade_date,
        command=" ".join(cmd),
    )
    started_at = datetime.now()
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(300, int(timeout_seconds or 7200)),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        _emit_progress(
            progress_callback,
            stage="repair_kline_failed",
            percent=3,
            step=f"Guojin QMT K-line repair timed out for {trade_date}",
            trade_date=trade_date,
            error=str(exc),
        )
        raise RuntimeError(f"Guojin QMT K-line repair timed out for {trade_date}") from exc

    elapsed = (datetime.now() - started_at).total_seconds()
    payload = {
        "trade_date": trade_date,
        "returncode": completed.returncode,
        "elapsed_seconds": round(elapsed, 1),
        "stdout_tail": _tail_text(completed.stdout),
        "stderr_tail": _tail_text(completed.stderr),
        "command": " ".join(cmd),
    }
    if completed.returncode != 0:
        _emit_progress(
            progress_callback,
            stage="repair_kline_failed",
            percent=3,
            step=f"Guojin QMT K-line repair failed for {trade_date}",
            trade_date=trade_date,
            returncode=completed.returncode,
            error=_tail_text(completed.stderr or completed.stdout, 1200),
        )
        raise RuntimeError(
            f"Guojin QMT K-line repair failed for {trade_date}, returncode={completed.returncode}: "
            f"{_tail_text(completed.stderr or completed.stdout, 1200)}"
        )

    _emit_progress(
        progress_callback,
        stage="repair_kline_done",
        percent=6,
        step=f"Guojin QMT K-line repair finished for {trade_date}",
        trade_date=trade_date,
        elapsed_seconds=payload["elapsed_seconds"],
    )
    return payload


def _recent_dates(engine: Engine, table: str, column: str, end_date: str, limit: int) -> list[str]:
    limit = max(1, int(limit))
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT DISTINCT `{column}` AS d
            FROM `{table}`
            WHERE `{column}` <= :end_date
            ORDER BY `{column}` DESC
            LIMIT {limit}
        """), {"end_date": end_date}).fetchall()
    return [str(r[0])[:10] for r in rows if r[0] is not None]


def load_kline_features(engine: Engine, trade_date: str, lookback: int = 90) -> pd.DataFrame:
    dates = _recent_dates(engine, "sm_stock_kline", "trade_date", trade_date, lookback)
    if not dates:
        raise RuntimeError(f"No K-line dates found before {trade_date}")
    start_date = dates[-1]
    window_sql = """
        WITH recent AS (
            SELECT
              k.stock_code,
              COALESCE(NULLIF(k.short_name, ''), a.short_name, '') AS short_name,
              k.trade_date,
              k.open, k.high, k.low, k.close,
              k.volume, k.amount, k.change_pct, k.turnover_ratio, k.pre_close,
              AVG(k.close) OVER w5 AS ma5,
              AVG(k.close) OVER w10 AS ma10,
              AVG(k.close) OVER w20 AS ma20,
              AVG(k.close) OVER w60 AS ma60,
              AVG(k.amount) OVER w5 AS amount_ma5,
              AVG(k.amount) OVER w20 AS amount_ma20,
              LAG(k.close, 5) OVER wfull AS close_5d_ago,
              LAG(k.close, 20) OVER wfull AS close_20d_ago,
              STDDEV_SAMP(k.change_pct) OVER w20 AS volatility_20,
              MAX(k.high) OVER w20 AS high_20,
              MAX(k.high) OVER w60 AS high_60,
              MIN(k.low) OVER w60 AS low_60
            FROM sm_stock_kline k
            LEFT JOIN si_all_code a ON a.stock_code = k.stock_code
            WHERE k.k_type = 1
              AND k.adjust_type = 0
              AND k.trade_date >= :start_date
              AND k.trade_date <= :trade_date
            WINDOW
              wfull AS (PARTITION BY k.stock_code ORDER BY k.trade_date),
              w5 AS (PARTITION BY k.stock_code ORDER BY k.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
              w10 AS (PARTITION BY k.stock_code ORDER BY k.trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
              w20 AS (PARTITION BY k.stock_code ORDER BY k.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
              w60 AS (PARTITION BY k.stock_code ORDER BY k.trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
        )
        SELECT
          stock_code, short_name, trade_date,
          open, high, low, close, volume, amount, change_pct, turnover_ratio, pre_close,
          ma5, ma10, ma20, ma60, amount_ma5, amount_ma20,
          CASE WHEN close_5d_ago IS NULL OR close_5d_ago = 0 THEN NULL ELSE (close / close_5d_ago - 1) * 100 END AS pct_5,
          CASE WHEN close_20d_ago IS NULL OR close_20d_ago = 0 THEN NULL ELSE (close / close_20d_ago - 1) * 100 END AS pct_20,
          volatility_20, high_20, high_60, low_60,
          CASE WHEN high_60 IS NULL OR high_60 = 0 THEN NULL ELSE (close / high_60 - 1) * 100 END AS drawdown_60,
          CASE WHEN low_60 IS NULL OR low_60 = 0 THEN NULL ELSE (close / low_60 - 1) * 100 END AS from_low_60,
          CASE WHEN ma20 IS NULL OR ma20 = 0 THEN NULL ELSE (close / ma20 - 1) * 100 END AS dist_ma20,
          CASE WHEN amount_ma5 IS NULL OR amount_ma5 = 0 THEN NULL ELSE amount / amount_ma5 END AS amount_ratio_5,
          CASE WHEN amount_ma20 IS NULL OR amount_ma20 = 0 THEN NULL ELSE amount / amount_ma20 END AS amount_ratio_20
        FROM recent
        WHERE trade_date = :trade_date
    """
    try:
        raise RuntimeError("window query disabled; using grouped aggregate path")
        fast = pd.read_sql(
            text(window_sql),
            engine,
            params={"start_date": start_date, "trade_date": trade_date},
        )
        if not fast.empty:
            numeric_cols = [
                "open", "high", "low", "close", "volume", "amount", "change_pct",
                "turnover_ratio", "pre_close", "ma5", "ma10", "ma20", "ma60",
                "amount_ma5", "amount_ma20", "pct_5", "pct_20", "volatility_20",
                "high_20", "high_60", "low_60", "drawdown_60", "from_low_60",
                "dist_ma20", "amount_ratio_5", "amount_ratio_20",
            ]
            for col in numeric_cols:
                if col in fast.columns:
                    fast[col] = pd.to_numeric(fast[col], errors="coerce")
            fast["stock_code"] = fast["stock_code"].astype(str).str.strip().str.zfill(6)
            fast["short_name"] = fast["short_name"].fillna("").astype(str)
            fast["trade_date"] = pd.to_datetime(fast["trade_date"]).dt.date
            return fast.drop_duplicates("stock_code", keep="last").reset_index(drop=True)
    except Exception as exc:
        logger.debug("Window K-line feature query skipped, falling back to grouped aggregate: %s", exc)

    def _date_at(offset: int) -> str:
        if not dates:
            return start_date
        return dates[min(offset, len(dates) - 1)]

    latest_sql = """
        SELECT
          k.stock_code,
          COALESCE(NULLIF(k.short_name, ''), a.short_name, '') AS short_name,
          k.trade_date,
          k.open, k.high, k.low, k.close,
          k.volume, k.amount, k.change_pct, k.turnover_ratio, k.pre_close
        FROM sm_stock_kline k
        LEFT JOIN si_all_code a ON a.stock_code = k.stock_code
        WHERE k.k_type = 1
          AND k.adjust_type = 0
          AND k.trade_date = :trade_date
    """
    agg_sql = """
        SELECT
          stock_code,
          AVG(CASE WHEN trade_date >= :ma5_start THEN close END) AS ma5,
          AVG(CASE WHEN trade_date >= :ma10_start THEN close END) AS ma10,
          AVG(CASE WHEN trade_date >= :ma20_start THEN close END) AS ma20,
          AVG(CASE WHEN trade_date >= :ma60_start THEN close END) AS ma60,
          AVG(CASE WHEN trade_date >= :ma5_start THEN amount END) AS amount_ma5,
          AVG(CASE WHEN trade_date >= :ma20_start THEN amount END) AS amount_ma20,
          STDDEV_SAMP(CASE WHEN trade_date >= :ma20_start THEN change_pct END) AS volatility_20,
          MAX(CASE WHEN trade_date >= :ma20_start THEN high END) AS high_20,
          MAX(CASE WHEN trade_date >= :ma60_start THEN high END) AS high_60,
          MIN(CASE WHEN trade_date >= :ma60_start THEN low END) AS low_60
        FROM sm_stock_kline
        WHERE k_type = 1
          AND adjust_type = 0
          AND trade_date >= :ma60_start
          AND trade_date <= :trade_date
        GROUP BY stock_code
    """
    try:
        latest = pd.read_sql(text(latest_sql), engine, params={"trade_date": trade_date})
        if not latest.empty:
            agg = pd.read_sql(
                text(agg_sql),
                engine,
                params={
                    "trade_date": trade_date,
                    "ma5_start": _date_at(4),
                    "ma10_start": _date_at(9),
                    "ma20_start": _date_at(19),
                    "ma60_start": _date_at(59),
                },
            )
            latest["stock_code"] = latest["stock_code"].astype(str).str.strip().str.zfill(6)
            agg["stock_code"] = agg["stock_code"].astype(str).str.strip().str.zfill(6)
            out = latest.merge(agg, on="stock_code", how="left")
            lag_frames = []
            for lag_name, offset in (("close_5d_ago", 5), ("close_20d_ago", 20)):
                if len(dates) > offset:
                    lag = pd.read_sql(
                        text("""
                            SELECT stock_code, close AS value
                            FROM sm_stock_kline
                            WHERE k_type = 1
                              AND adjust_type = 0
                              AND trade_date = :lag_date
                        """),
                        engine,
                        params={"lag_date": dates[offset]},
                    )
                    if not lag.empty:
                        lag["stock_code"] = lag["stock_code"].astype(str).str.strip().str.zfill(6)
                        lag = lag.rename(columns={"value": lag_name})
                        lag_frames.append(lag)
            for lag in lag_frames:
                out = out.merge(lag, on="stock_code", how="left")
            numeric_cols = [
                "open", "high", "low", "close", "volume", "amount", "change_pct",
                "turnover_ratio", "pre_close", "ma5", "ma10", "ma20", "ma60",
                "amount_ma5", "amount_ma20", "volatility_20", "high_20", "high_60", "low_60",
                "close_5d_ago", "close_20d_ago",
            ]
            for col in numeric_cols:
                if col in out.columns:
                    out[col] = pd.to_numeric(out[col], errors="coerce")
            if "close_5d_ago" not in out.columns:
                out["close_5d_ago"] = np.nan
            if "close_20d_ago" not in out.columns:
                out["close_20d_ago"] = np.nan
            out["pct_5"] = (out["close"] / out.get("close_5d_ago").replace(0, np.nan) - 1.0) * 100.0
            out["pct_20"] = (out["close"] / out.get("close_20d_ago").replace(0, np.nan) - 1.0) * 100.0
            out["drawdown_60"] = (out["close"] / out["high_60"].replace(0, np.nan) - 1.0) * 100.0
            out["from_low_60"] = (out["close"] / out["low_60"].replace(0, np.nan) - 1.0) * 100.0
            out["dist_ma20"] = (out["close"] / out["ma20"].replace(0, np.nan) - 1.0) * 100.0
            out["amount_ratio_5"] = out["amount"] / out["amount_ma5"].replace(0, np.nan)
            out["amount_ratio_20"] = out["amount"] / out["amount_ma20"].replace(0, np.nan)
            out["short_name"] = out["short_name"].fillna("").astype(str)
            out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
            drop_cols = [c for c in ("close_5d_ago", "close_20d_ago") if c in out.columns]
            if drop_cols:
                out = out.drop(columns=drop_cols)
            return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)
    except Exception as exc:
        logger.warning("Grouped K-line feature query failed, falling back to pandas rolling: %s", exc)

    sql = """
        SELECT
          k.stock_code,
          COALESCE(NULLIF(k.short_name, ''), a.short_name, '') AS short_name,
          k.trade_date,
          k.open, k.high, k.low, k.close,
          k.volume, k.amount, k.change_pct, k.turnover_ratio, k.pre_close
        FROM sm_stock_kline k
        LEFT JOIN si_all_code a ON a.stock_code = k.stock_code
        WHERE k.k_type = 1
          AND k.adjust_type = 0
          AND k.trade_date >= :start_date
          AND k.trade_date <= :trade_date
        ORDER BY k.stock_code, k.trade_date
    """
    df = pd.read_sql(text(sql), engine, params={"start_date": start_date, "trade_date": trade_date})
    if df.empty:
        raise RuntimeError(f"No K-line rows found for {trade_date}")

    numeric_cols = [
        "open", "high", "low", "close", "volume", "amount", "change_pct",
        "turnover_ratio", "pre_close",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["short_name"] = df["short_name"].fillna("").astype(str)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df = df.drop_duplicates(["stock_code", "trade_date"], keep="last")
    df = df.sort_values(["stock_code", "trade_date"])

    grouped = df.groupby("stock_code", group_keys=False)
    for window in (5, 10, 20, 60):
        min_periods = max(3, min(window, window // 2))
        df[f"ma{window}"] = grouped["close"].transform(lambda s, w=window, m=min_periods: s.rolling(w, min_periods=m).mean())
    df["amount_ma5"] = grouped["amount"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    df["amount_ma20"] = grouped["amount"].transform(lambda s: s.rolling(20, min_periods=8).mean())
    df["pct_5"] = grouped["close"].pct_change(5) * 100.0
    df["pct_20"] = grouped["close"].pct_change(20) * 100.0
    df["volatility_20"] = grouped["change_pct"].transform(lambda s: s.rolling(20, min_periods=8).std())
    df["high_20"] = grouped["high"].transform(lambda s: s.rolling(20, min_periods=8).max())
    df["high_60"] = grouped["high"].transform(lambda s: s.rolling(60, min_periods=20).max())
    df["low_60"] = grouped["low"].transform(lambda s: s.rolling(60, min_periods=20).min())
    df["drawdown_60"] = (df["close"] / df["high_60"] - 1.0) * 100.0
    df["from_low_60"] = (df["close"] / df["low_60"] - 1.0) * 100.0
    df["dist_ma20"] = (df["close"] / df["ma20"] - 1.0) * 100.0
    df["amount_ratio_5"] = df["amount"] / df["amount_ma5"].replace(0, np.nan)
    df["amount_ratio_20"] = df["amount"] / df["amount_ma20"].replace(0, np.nan)

    target = pd.to_datetime(trade_date).date()
    latest = df[df["trade_date"] == target].copy()
    if latest.empty:
        raise RuntimeError(f"No K-line rows exactly on {trade_date}")
    return latest.reset_index(drop=True)


def load_finance(engine: Engine, trade_date: str) -> pd.DataFrame:
    sql = """
        SELECT
          f.stock_code, f.report_date, f.basic_eps, f.net_asset_ps, f.oper_cf_ps,
          f.total_rev_yoy_gr, f.net_profit_yoy_gr, f.non_gaap_net_profit_yoy_gr,
          f.roe_wtd, f.gross_margin, f.net_margin,
          f.curr_ratio, f.cash_flow_ratio, f.asset_liab_ratio
        FROM si_stock_finance f
        JOIN (
          SELECT stock_code, MAX(report_date) AS report_date
          FROM si_stock_finance
          WHERE report_date <= :trade_date
          GROUP BY stock_code
        ) x ON x.stock_code = f.stock_code AND x.report_date = f.report_date
    """
    df = pd.read_sql(text(sql), engine, params={"trade_date": trade_date})
    if df.empty:
        return pd.DataFrame({"stock_code": []})
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    for col in df.columns:
        if col not in {"stock_code", "report_date"}:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.drop_duplicates("stock_code", keep="last")


def load_flow_features(engine: Engine, trade_date: str, lookback: int = 25) -> tuple[pd.DataFrame, str]:
    dates = _recent_dates(engine, "sm_stock_capital_flow_daily", "trade_date", trade_date, lookback)
    if not dates:
        return pd.DataFrame({"stock_code": []}), ""
    start_date = dates[-1]
    flow_date = dates[0]
    sql = """
        SELECT stock_code, trade_date, main_net_inflow, max_net_inflow, lg_net_inflow,
               mid_net_inflow, sm_net_inflow
        FROM sm_stock_capital_flow_daily
        WHERE trade_date >= :start_date
          AND trade_date <= :trade_date
        ORDER BY stock_code, trade_date
    """
    df = pd.read_sql(text(sql), engine, params={"start_date": start_date, "trade_date": trade_date})
    if df.empty:
        return pd.DataFrame({"stock_code": []}), flow_date
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = df.drop_duplicates(["stock_code", "trade_date"], keep="last").sort_values(["stock_code", "trade_date"])
    grouped = df.groupby("stock_code", group_keys=False)
    df["main_net_inflow_5d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(5, min_periods=1).sum())
    df["main_net_inflow_20d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    latest = df.groupby("stock_code", as_index=False).tail(1).copy()
    latest = latest.rename(columns={"trade_date": "flow_trade_date"})
    return latest.reset_index(drop=True), flow_date


def load_hot_rank(engine: Engine, trade_date: str) -> tuple[pd.DataFrame, str]:
    hot_dates = _recent_dates(engine, "st_hot_rank_fused", "snapshot_date", trade_date, 1)
    if not hot_dates:
        return pd.DataFrame({"stock_code": []}), ""
    hot_date = hot_dates[0]
    sql = """
        SELECT stock_code, fused_rank, total_score, source_flag, industry_name
        FROM st_hot_rank_fused
        WHERE snapshot_date = :hot_date
    """
    df = pd.read_sql(text(sql), engine, params={"hot_date": hot_date})
    if df.empty:
        return pd.DataFrame({"stock_code": []}), hot_date
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["fused_rank"] = pd.to_numeric(df["fused_rank"], errors="coerce")
    df["hot_total_score"] = pd.to_numeric(df["total_score"], errors="coerce")
    return df.drop_duplicates("stock_code", keep="last"), hot_date


def load_notice_features(engine: Engine, trade_date: str, lookback_days: int = 14) -> pd.DataFrame:
    sql = """
        SELECT stock_code, notice_date, title, column_name
        FROM si_notice_eastmoney
        WHERE notice_date >= DATE_SUB(:trade_date, INTERVAL :lookback_days DAY)
          AND notice_date <= :trade_date
    """
    df = pd.read_sql(
        text(sql),
        engine,
        params={"trade_date": trade_date, "lookback_days": int(lookback_days)},
    )
    if df.empty:
        return pd.DataFrame({"stock_code": []})

    records: dict[str, dict[str, Any]] = {}
    for row in df.to_dict(orient="records"):
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if not code:
            continue
        title = str(row.get("title") or "")
        cls = classify_notice_title(title)
        rec = records.setdefault(code, {
            "stock_code": code,
            "notice_count": 0,
            "notice_positive": 0,
            "notice_negative": 0,
            "notice_critical": 0,
            "latest_notice_date": None,
            "risk_titles": [],
            "positive_titles": [],
        })
        rec["notice_count"] += 1
        rec["notice_positive"] += cls["positive"]
        rec["notice_negative"] += cls["negative"]
        rec["notice_critical"] += cls["critical"]
        notice_date = row.get("notice_date")
        if notice_date and (rec["latest_notice_date"] is None or str(notice_date) > str(rec["latest_notice_date"])):
            rec["latest_notice_date"] = str(notice_date)[:10]
        if (cls["negative"] or cls["critical"]) and len(rec["risk_titles"]) < 3:
            rec["risk_titles"].append(title[:80])
        if cls["positive"] and len(rec["positive_titles"]) < 3:
            rec["positive_titles"].append(title[:80])

    return pd.DataFrame(records.values())


def load_news_features(
    engine: Engine,
    trade_date: str,
    cutoff_time: str | None = None,
    lookback_days: int = 3,
) -> pd.DataFrame:
    try:
        has_news_table = _table_exists(engine, "st_news_flash")
    except Exception:
        has_news_table = False
    if not has_news_table:
        return pd.DataFrame({"stock_code": []})
    cutoff = cutoff_time or f"{trade_date} 23:59:59"
    sql = """
        SELECT title, content, publish_time, stocks
        FROM st_news_flash
        WHERE publish_time >= DATE_SUB(:cutoff_time, INTERVAL :lookback_days DAY)
          AND publish_time <= :cutoff_time
        ORDER BY publish_time DESC
        LIMIT 3000
    """
    df = pd.read_sql(
        text(sql),
        engine,
        params={"cutoff_time": cutoff, "lookback_days": int(lookback_days)},
    )
    if df.empty:
        return pd.DataFrame({"stock_code": []})

    records: dict[str, dict[str, Any]] = {}
    for row in df.to_dict(orient="records"):
        raw_stocks = row.get("stocks")
        try:
            stocks = json.loads(raw_stocks) if isinstance(raw_stocks, str) else (raw_stocks or [])
        except Exception:
            stocks = []
        codes: list[str] = []
        for item in stocks if isinstance(stocks, list) else []:
            if isinstance(item, dict):
                code = str(item.get("code") or item.get("symbol") or "").strip()
            else:
                code = str(item or "").strip()
            digits = "".join(ch for ch in code if ch.isdigit())
            if len(digits) >= 6:
                codes.append(digits[-6:])
        if not codes:
            continue
        title = str(row.get("title") or "")
        content = str(row.get("content") or "")
        cls = classify_notice_title(f"{title} {content}"[:500])
        publish_time = str(row.get("publish_time") or "")[:19]
        for code in sorted(set(codes)):
            rec = records.setdefault(code, {
                "stock_code": code,
                "news_count": 0,
                "news_positive": 0,
                "news_negative": 0,
                "news_critical": 0,
                "latest_news_time": None,
                "news_risk_titles": [],
                "news_positive_titles": [],
            })
            rec["news_count"] += 1
            rec["news_positive"] += cls["positive"]
            rec["news_negative"] += cls["negative"]
            rec["news_critical"] += cls["critical"]
            if publish_time and (rec["latest_news_time"] is None or publish_time > str(rec["latest_news_time"])):
                rec["latest_news_time"] = publish_time
            if (cls["negative"] or cls["critical"]) and len(rec["news_risk_titles"]) < 3:
                rec["news_risk_titles"].append(title[:80] or content[:80])
            if cls["positive"] and len(rec["news_positive_titles"]) < 3:
                rec["news_positive_titles"].append(title[:80] or content[:80])
    return pd.DataFrame(records.values()) if records else pd.DataFrame({"stock_code": []})


def merge_event_features(notices: pd.DataFrame, news: pd.DataFrame) -> pd.DataFrame:
    if notices is None or notices.empty:
        base = pd.DataFrame({"stock_code": []})
    else:
        base = notices.copy()
    if news is None or news.empty or "stock_code" not in news.columns:
        return base
    if base.empty or "stock_code" not in base.columns:
        base = pd.DataFrame({"stock_code": news["stock_code"].astype(str).str.zfill(6).unique()})
    out = base.merge(news, on="stock_code", how="outer")
    def _num_col(name: str) -> pd.Series:
        if name not in out.columns:
            return pd.Series(0, index=out.index, dtype="float64")
        return pd.to_numeric(out[name], errors="coerce").fillna(0)
    for col in ("notice_count", "notice_positive", "notice_negative", "notice_critical"):
        out[col] = _num_col(col)
    out["notice_count"] = out["notice_count"] + _num_col("news_count")
    out["notice_positive"] = out["notice_positive"] + _num_col("news_positive")
    out["notice_negative"] = out["notice_negative"] + _num_col("news_negative")
    out["notice_critical"] = out["notice_critical"] + _num_col("news_critical")
    latest_notice = out.get("latest_notice_date")
    latest_news = out.get("latest_news_time")
    if latest_notice is not None and latest_news is not None:
        out["latest_notice_date"] = latest_notice.fillna("").astype(str).combine(
            latest_news.fillna("").astype(str).str[:10],
            lambda a, b: max(a, b) if a and b else (a or b or None),
        )
    elif latest_news is not None:
        out["latest_notice_date"] = latest_news.fillna("").astype(str).str[:10]
    return out


def compute_market_mood(kline: pd.DataFrame) -> float:
    change = pd.to_numeric(kline.get("change_pct"), errors="coerce")
    if change.empty or change.notna().sum() == 0:
        return 50.0
    advance_ratio = float((change > 0).mean())
    limit_up_ratio = float((change >= 9.7).mean())
    limit_down_ratio = float((change <= -9.7).mean())
    avg_change = float(change.mean())
    score = 50 + (advance_ratio - 0.5) * 70 + avg_change * 4 + (limit_up_ratio - limit_down_ratio) * 120
    return clamp_score(score)


def _table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name}).scalar()
    return bool(value)


def _ensure_learning_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `st_ai_failure_samples` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `stock_code` VARCHAR(10) NOT NULL,
                `short_name` VARCHAR(40) DEFAULT '',
                `strategy_profile` VARCHAR(20) DEFAULT '',
                `signal_date` DATE DEFAULT NULL,
                `result` VARCHAR(20) DEFAULT 'fail',
                `fail_tag` VARCHAR(40) DEFAULT '',
                `fail_reason` VARCHAR(500) DEFAULT '',
                `return_pct` DECIMAL(8,4) DEFAULT NULL,
                `created_at` DATETIME DEFAULT NULL,
                KEY `idx_stock_date` (`stock_code`, `signal_date`),
                KEY `idx_fail_tag` (`fail_tag`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


def load_confidence_features(engine: Engine, trade_date: str, lookback_days: int = 5) -> pd.DataFrame:
    if not _table_exists(engine, "stock_analysis_result"):
        return pd.DataFrame({"stock_code": []})
    with engine.connect() as conn:
        dates = conn.execute(text(f"""
            SELECT DISTINCT analysis_date
            FROM stock_analysis_result
            WHERE analysis_date < :trade_date
            ORDER BY analysis_date DESC
            LIMIT {max(1, int(lookback_days))}
        """), {"trade_date": trade_date}).fetchall()
    hist_dates = [str(r[0])[:10] for r in dates if r[0] is not None]
    if not hist_dates:
        return pd.DataFrame({"stock_code": []})
    placeholders = ", ".join([f":d{i}" for i in range(len(hist_dates))])
    params = {"trade_date": trade_date, **{f"d{i}": d for i, d in enumerate(hist_dates)}}
    sql = f"""
        SELECT stock_code, analysis_date,
               COALESCE(short_term_score, 0) * 0.58 + COALESCE(long_term_score, 0) * 0.42 AS hist_score
        FROM stock_analysis_result
        WHERE analysis_date IN ({placeholders})
    """
    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        return pd.DataFrame({"stock_code": []})
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["hist_score"] = pd.to_numeric(df["hist_score"], errors="coerce")
    grouped = df.groupby("stock_code")["hist_score"]
    out = grouped.agg(["count", "std", "mean"]).reset_index()
    out["score_std_5d"] = pd.to_numeric(out["std"], errors="coerce").fillna(0.0)
    out["confidence_score"] = (100.0 - out["score_std_5d"] * 8.0).clip(35, 100)
    out.loc[out["count"] < 3, "confidence_score"] = 62.0
    return out[["stock_code", "confidence_score", "score_std_5d"]]


def load_recommendation_history(engine: Engine, trade_date: str, lookback_days: int = 30) -> pd.DataFrame:
    if not _table_exists(engine, "st_recommended_stocks"):
        return pd.DataFrame({"stock_code": []})
    columns = _table_columns(engine, "st_recommended_stocks")
    score_expr = "COALESCE(final_trade_score, ai_score, 0)" if "final_trade_score" in columns else "COALESCE(ai_score, 0)"
    strategy_expr = "COALESCE(primary_strategy, strategy_profile, '')" if "primary_strategy" in columns else "''"
    sql = f"""
        SELECT stock_code,
               MAX(pick_date) AS last_pick_date,
               MAX({score_expr}) AS max_recent_trade_score,
               MAX({strategy_expr}) AS last_strategy
        FROM st_recommended_stocks
        WHERE pick_date < :trade_date
          AND pick_date >= DATE_SUB(:trade_date, INTERVAL :lookback DAY)
        GROUP BY stock_code
    """
    df = pd.read_sql(text(sql), engine, params={"trade_date": trade_date, "lookback": int(lookback_days)})
    if df.empty:
        return pd.DataFrame({"stock_code": []})
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["max_recent_trade_score"] = pd.to_numeric(df["max_recent_trade_score"], errors="coerce").fillna(0.0)
    return df


def load_failure_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    _ensure_learning_tables(engine)
    pieces: list[pd.DataFrame] = []
    if _table_exists(engine, "st_sim_position"):
        try:
            sim = pd.read_sql(text("""
                SELECT stock_code, COUNT(*) AS fail_count
                FROM st_sim_position
                WHERE status = 'closed'
                  AND COALESCE(profit_rate, 0) < 0
                  AND COALESCE(sell_date, buy_date) >= DATE_SUB(:trade_date, INTERVAL 180 DAY)
                GROUP BY stock_code
            """), engine, params={"trade_date": trade_date})
            if not sim.empty:
                pieces.append(sim)
        except Exception:
            pass
    try:
        manual = pd.read_sql(text("""
            SELECT stock_code, COUNT(*) AS fail_count
            FROM st_ai_failure_samples
            WHERE result = 'fail'
              AND (signal_date IS NULL OR signal_date >= DATE_SUB(:trade_date, INTERVAL 180 DAY))
            GROUP BY stock_code
        """), engine, params={"trade_date": trade_date})
        if not manual.empty:
            pieces.append(manual)
    except Exception:
        pass
    if not pieces:
        return pd.DataFrame({"stock_code": []})
    df = pd.concat(pieces, ignore_index=True)
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["fail_count"] = pd.to_numeric(df["fail_count"], errors="coerce").fillna(0.0)
    out = df.groupby("stock_code", as_index=False)["fail_count"].sum()
    out["failure_penalty_score"] = (100.0 - out["fail_count"] * 12.0).clip(35, 100)
    return out


def _load_sector_industry_memberships(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Prefer a validated immutable QMT L1-industry snapshot."""
    cutoff = datetime.combine(
        date.fromisoformat(str(trade_date)[:10]) + timedelta(days=1),
        datetime.min.time(),
    )
    if (
        _table_exists(engine, "qmt_membership_snapshot_run")
        and _table_exists(engine, "qmt_industry_member_snapshot")
    ):
        try:
            run = pd.read_sql(
                text(
                    """
                    SELECT snapshot_date, source, industry_relation_count
                    FROM qmt_membership_snapshot_run
                    WHERE snapshot_date <= :trade_date
                      AND quality_status = 'QMT_VALIDATED'
                      AND captured_at < :cutoff
                    ORDER BY snapshot_date DESC, captured_at DESC, source
                    LIMIT 1
                    """
                ),
                engine,
                params={"trade_date": trade_date, "cutoff": cutoff},
            )
            if not run.empty:
                snapshot_date = run.iloc[0]["snapshot_date"]
                source = str(run.iloc[0]["source"] or "")
                expected = int(run.iloc[0]["industry_relation_count"] or 0)
                evidence = pd.read_sql(
                    text(
                        """
                        SELECT COUNT(*) AS relation_count
                        FROM qmt_industry_member_snapshot
                        WHERE snapshot_date = :snapshot_date
                          AND source = :source
                          AND quality_status = 'QMT_VALIDATED'
                          AND captured_at < :cutoff
                        """
                    ),
                    engine,
                    params={
                        "snapshot_date": snapshot_date,
                        "source": source,
                        "cutoff": cutoff,
                    },
                )
                actual = int(evidence.iloc[0]["relation_count"] or 0)
                if expected > 0 and actual == expected:
                    rows = pd.read_sql(
                        text(
                            """
                            SELECT stock_code, industry_name
                            FROM qmt_industry_member_snapshot
                            WHERE snapshot_date = :snapshot_date
                              AND source = :source
                              AND quality_status = 'QMT_VALIDATED'
                              AND captured_at < :cutoff
                              AND industry_type IN
                                  ('L1', '一级行业', '申万一级', 'SW2021')
                            """
                        ),
                        engine,
                        params={
                            "snapshot_date": snapshot_date,
                            "source": source,
                            "cutoff": cutoff,
                        },
                    )
                    if not rows.empty:
                        return rows.drop_duplicates("stock_code", keep="first")
        except Exception as exc:
            logger.debug("Immutable industry snapshot lookup skipped: %s", exc)

    if not _table_exists(engine, "si_industry_sw"):
        return pd.DataFrame({"stock_code": []})
    return pd.read_sql(
        text(
            """
            SELECT stock_code, industry_name
            FROM si_industry_sw
            WHERE industry_type IN ('L1', '一级行业', '申万一级', 'SW2021')
              AND industry_name IS NOT NULL
            """
        ),
        engine,
    ).drop_duplicates("stock_code", keep="first")


def load_sector_rotation_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    memberships = _load_sector_industry_memberships(engine, trade_date)
    if memberships.empty:
        return pd.DataFrame({"stock_code": []})
    dates = _recent_dates(engine, "sm_stock_kline", "trade_date", trade_date, 3)
    if not dates:
        return pd.DataFrame({"stock_code": []})
    start_date = dates[-1]
    flow_join = ""
    flow_select = "0 AS flow_ratio_3d"
    if _table_exists(engine, "sm_stock_capital_flow_daily"):
        flow_join = """
            LEFT JOIN sm_stock_capital_flow_daily f
              ON f.stock_code = k.stock_code AND f.trade_date = k.trade_date
        """
        flow_select = """
            SUM(COALESCE(f.main_net_inflow, 0)) / NULLIF(SUM(k.amount), 0) * 100 AS flow_ratio_3d
        """
    sql = f"""
        SELECT i.industry_name,
               AVG(k.change_pct) AS avg_change_3d,
               {flow_select}
        FROM sm_stock_kline k
        JOIN si_industry_sw i ON i.stock_code = k.stock_code
        {flow_join}
        WHERE k.k_type = 1
          AND k.adjust_type = 0
          AND k.trade_date >= :start_date
          AND k.trade_date <= :trade_date
          AND i.industry_type = '申万一级'
          AND i.industry_name IS NOT NULL
        GROUP BY i.industry_name
    """
    sector = pd.read_sql(text(sql), engine, params={"start_date": start_date, "trade_date": trade_date})
    if sector.empty:
        return pd.DataFrame({"stock_code": []})
    sector["avg_change_3d"] = pd.to_numeric(sector["avg_change_3d"], errors="coerce").fillna(0.0)
    sector["flow_ratio_3d"] = pd.to_numeric(sector["flow_ratio_3d"], errors="coerce").fillna(0.0)
    base = 55.0 + _series_score(sector["flow_ratio_3d"], -0.8, 1.8) * 0.30
    overheated = pd.Series(np.where(sector["avg_change_3d"] >= 5.0, 12.0, 0.0), index=sector.index)
    early_rotation = pd.Series(
        np.where((sector["flow_ratio_3d"] > 0.25) & (sector["avg_change_3d"].between(-1.5, 2.5)), 12.0, 0.0),
        index=sector.index,
    )
    sector["sector_rotation_score"] = (base + early_rotation - overheated).clip(30, 100)
    codes = memberships.copy()
    if codes.empty:
        return pd.DataFrame({"stock_code": []})
    codes["stock_code"] = codes["stock_code"].astype(str).str.strip().str.zfill(6)
    out = codes.merge(sector[["industry_name", "sector_rotation_score"]], on="industry_name", how="left")
    out["sector_rotation_score"] = pd.to_numeric(out["sector_rotation_score"], errors="coerce").fillna(55.0)
    return out[["stock_code", "industry_name", "sector_rotation_score"]]


def _strategy_score(row: dict[str, Any], strategy: str) -> float:
    if strategy == "ultra_short":
        return _safe_number(row.get("ultra_short_score"), 0.0)
    if strategy == "swing":
        return _safe_number(row.get("swing_score"), 0.0)
    if strategy == "main_wave":
        return _safe_number(row.get("main_wave_score"), 0.0)
    return _safe_number(row.get("short_term_score"), 0.0)


def select_primary_strategy(row: dict[str, Any]) -> str:
    if str(row.get("main_wave_signal") or "").upper() in {"REDUCE", "SELL_ALERT"}:
        return "main_wave"
    scores = {name: _strategy_score(row, name) for name in STRATEGY_PROFILES}
    qualified = {
        name: score
        for name, score in scores.items()
        if score >= STRATEGY_PROFILES[name]["min_score"]
    }
    pool = qualified or scores
    return max(pool, key=lambda name: (pool[name], STRATEGY_PROFILES[name]["confirm_score"] * -1))


def _position_weight(row: dict[str, Any], strategy: str, status: str) -> float:
    profile = STRATEGY_PROFILES[strategy]
    score = _safe_number(row.get("final_trade_score"), _strategy_score(row, strategy))
    weight = float(profile["base_position"]) + max(0.0, score - float(profile["min_score"])) * 0.16
    mood = _safe_number(row.get("market_mood_score"), 50.0)
    risk_level = str(row.get("event_risk_level") or "LOW").upper()
    if status not in {"CONFIRM", "BUY_READY"}:
        weight *= 0.55
    if mood < 35:
        weight *= 0.60
    elif mood > 65:
        weight *= 1.12
    if risk_level == "MEDIUM":
        weight *= 0.75
    return round(max(1.0, min(12.0, weight)), 2)


def _parse_date_value(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def build_strategy_trade_plan(row: dict[str, Any], strategy: str) -> dict[str, Any]:
    close = _first_price(row.get("close"))
    ma5 = _first_price(row.get("ma5"), default=close)
    ma10 = _first_price(row.get("ma10"), default=ma5 or close)
    ma20 = _first_price(row.get("ma20"), default=ma10 or close)
    ma60 = _first_price(row.get("ma60"), default=ma20 or close)
    profile = STRATEGY_PROFILES[strategy]
    score = _strategy_score(row, strategy)
    amount = _safe_number(row.get("amount"), 0.0)
    change_pct = _safe_number(row.get("change_pct"), 0.0)
    mood = _safe_number(row.get("market_mood_score"), 50.0)
    base_status = str(row.get("recommend_status") or "SUSPENDED").upper()
    event_risk = str(row.get("event_risk_level") or "LOW").upper()
    quality_score = _safe_number(row.get("quality_score"), _safe_number(row.get("ai_score"), score))
    entry_score = _safe_number(row.get("entry_score"), score)
    final_trade_score = _safe_number(row.get("final_trade_score"), score)
    expected_return_pct = _safe_number(row.get("expected_return_pct"), 0.0)
    heat_overload_score = _safe_number(row.get("heat_overload_score"), 60.0)
    confidence_score = _safe_number(row.get("confidence_score"), 62.0)
    failure_penalty_score = _safe_number(row.get("failure_penalty_score"), 100.0)
    main_wave_score = _safe_number(row.get("main_wave_score"), 0.0)
    trend_hold_score = _safe_number(row.get("trend_hold_score"), 0.0)
    main_wave_signal = str(row.get("main_wave_signal") or "NONE").upper()
    main_wave_reason = str(row.get("main_wave_reason") or "")
    max_recent_trade_score = _safe_number(row.get("max_recent_trade_score"), 0.0)
    last_pick_date = _parse_date_value(row.get("last_pick_date"))
    current_trade_date = _parse_date_value(row.get("trade_date"))
    cooldown_days = int(profile.get("cooldown_days", 0) or 0)
    cooldown_days_left = 0
    cooldown_until = None
    if last_pick_date and current_trade_date and cooldown_days > 0:
        days_since = max(0, (current_trade_date - last_pick_date).days)
        cooldown_days_left = max(0, cooldown_days - days_since)
        if cooldown_days_left > 0:
            cooldown_until = last_pick_date + timedelta(days=cooldown_days)
    cooldown_bypassed = cooldown_days_left > 0 and final_trade_score >= max_recent_trade_score + 3.0

    if close <= 0:
        return {
            "primary_strategy": strategy,
            "strategy_profile": strategy,
            "signal_status": "WATCH",
            "signal_reason": "missing close price",
            "entry_price_low": None,
            "entry_price_high": None,
            "stop_loss_price": None,
            "take_profit_1": None,
            "take_profit_2": None,
            "position_weight": None,
            "max_holding_days": int(profile["max_holding_days"]),
            "entry_conditions_json": "[]",
            "sell_rules_json": "[]",
            "invalidation_reason": "price data unavailable",
            "cooldown_days_left": 0,
            "cooldown_until": None,
        }

    if strategy == "ultra_short":
        entry_low = max(close * 0.985, ma5 * 0.995)
        entry_high = min(close * 1.018, close * 1.095)
        entry_conditions = [
            "live quote is fresh",
            "price stays near VWAP/MA5 or breaks intraday high",
            "main capital flow remains positive",
            "do not chase limit-up or extended gap",
        ]
        invalidation = "breaks MA5/VWAP, capital flow turns negative, or intraday rise is overextended"
    elif strategy == "main_wave":
        anchor = _first_price(ma10, ma5, close, default=close)
        entry_low = max(anchor * 0.985, close * 0.94)
        entry_high = min(close * 1.025, close * 1.095)
        entry_conditions = [
            "main wave score confirms breakout or trend continuation",
            "prefer pullback holding MA5/MA10 instead of chasing a limit-up candle",
            "volume remains above the 20-day average but is not a blow-off spike",
            "sector rotation score stays strong and trend hold score does not deteriorate",
        ]
        invalidation = "closes below MA20, high-volume long bearish candle appears, or main-wave signal turns SELL_ALERT"
    elif strategy == "swing":
        anchor = _first_price(ma20, ma60, close, default=close)
        entry_low = anchor * 0.98
        entry_high = min(anchor * 1.025, close * 1.095)
        entry_conditions = [
            "price holds MA20/MA60 support",
            "medium trend remains upward",
            "event risk stays LOW",
            "market breadth is not sharply weakening",
        ]
        invalidation = "closes below MA60, event risk rises, or medium trend breaks"
    else:
        anchor = _first_price(ma10, ma5, close, default=close)
        entry_low = min(close * 0.995, anchor * 0.99)
        entry_high = min(max(close * 1.012, anchor * 1.025), close * 1.095)
        entry_conditions = [
            "pullback holds MA5/MA10 or breaks 20-day platform",
            "volume remains above recent average",
            "capital flow does not reverse",
            "avoid chasing after sharp daily jump",
        ]
        invalidation = "falls below MA10/MA20, volume shrinks sharply, or capital flow weakens"

    if entry_low > entry_high:
        entry_low, entry_high = entry_high * 0.985, entry_low * 1.005

    ref_price = (entry_low + entry_high) / 2.0
    stop_loss = ref_price * (1.0 + float(profile["stop_loss_pct"]) / 100.0)
    if strategy == "main_wave":
        trend_stop = _first_price(row.get("trend_stop_price"), ma20 * 0.97, default=stop_loss)
        stop_loss = min(stop_loss, trend_stop) if trend_stop > 0 else stop_loss
    take_profit_1 = ref_price * (1.0 + float(profile["take_profit_1_pct"]) / 100.0)
    take_profit_2 = ref_price * (1.0 + float(profile["take_profit_2_pct"]) / 100.0)

    blockers: list[str] = []
    if strategy == "main_wave" and main_wave_signal in {"SELL_ALERT", "REDUCE"}:
        status = "SELL_ALERT"
        blockers.append(main_wave_reason or "main-wave risk signal triggered")
    elif base_status == "BLOCK" or event_risk == "CRITICAL":
        status = "BLOCK"
        blockers.append("blocked by base recommendation gate")
    elif strategy != "main_wave" and expected_return_pct < 5.0:
        status = "BLOCK"
        blockers.append(f"expected upside {expected_return_pct:.1f}% is below 5% threshold")
    elif base_status != "ALLOW":
        if strategy == "main_wave" and main_wave_score >= float(profile["min_score"]) and event_risk == "LOW":
            status = "WATCH"
            blockers.append(f"base status is {base_status}; main-wave candidate should wait for tradable pullback")
        else:
            status = "WATCH"
            blockers.append(f"base status is {base_status}")
    elif entry_score < 45.0:
        status = "WATCH"
        blockers.append(f"entry score {entry_score:.1f} is weak; good stock but poor buy point")
    elif cooldown_days_left > 0 and not cooldown_bypassed:
        status = "WATCH"
        blockers.append(f"cooldown active for {cooldown_days_left} more days")
    elif change_pct >= 9.7:
        status = "WATCH"
        blockers.append("near daily limit-up; wait for tradable pullback")
    elif amount < 50_000_000:
        status = "WATCH"
        blockers.append("liquidity is below intraday trading threshold")
    elif mood < 30 and strategy != "swing":
        status = "WATCH"
        blockers.append("market mood is weak")
    elif heat_overload_score < 50.0:
        status = "WATCH"
        blockers.append("heat overload is high; avoid becoming exit liquidity")
    elif confidence_score < 45.0:
        status = "WATCH"
        blockers.append("recent recommendation score is unstable")
    elif failure_penalty_score < 55.0:
        status = "WATCH"
        blockers.append("recent failure samples require caution")
    elif (
        strategy == "main_wave"
        and main_wave_signal == "BUY_READY"
        and main_wave_score >= float(profile["confirm_score"])
        and trend_hold_score >= 58.0
    ):
        status = "BUY_READY"
    elif final_trade_score >= float(profile["confirm_score"]) and entry_score >= 55.0:
        status = "CONFIRM"
    else:
        status = "WATCH"
        blockers.append("final trade score or entry score has not reached confirm threshold")

    if status == "BUY_READY":
        reason = (
            f"main-wave buy ready: score {main_wave_score:.1f}, hold {trend_hold_score:.1f}; "
            f"{main_wave_reason or 'wait for pullback/volume confirmation'}"
        )
    elif status == "CONFIRM":
        reason = (
            f"{strategy} final {final_trade_score:.1f} confirms candidate "
            f"(quality {quality_score:.1f}, entry {entry_score:.1f}); wait for intraday trigger"
        )
    else:
        reason = "; ".join(blockers) if blockers else f"{strategy} candidate needs confirmation"

    sell_rules = [
        f"stop loss below {abs(float(profile['stop_loss_pct'])):.1f}%",
        f"take profit levels {float(profile['take_profit_1_pct']):.1f}%/{float(profile['take_profit_2_pct']):.1f}%",
        f"max holding {int(profile['max_holding_days'])} trading days",
        invalidation,
    ]
    if strategy == "main_wave":
        sell_rules = [
            "do not exit only because fixed profit target is reached",
            "reduce when distance from MA20 is excessive and cumulative wave gain is high",
            "sell alert when price closes below MA20 after a main-wave advance",
            f"trend stop reference { _round_price(row.get('trend_stop_price')) or 'MA20 trailing stop' }",
        ]

    return {
        "primary_strategy": strategy,
        "strategy_profile": strategy,
        "signal_status": status,
        "signal_reason": reason[:500],
        "entry_price_low": _round_price(entry_low),
        "entry_price_high": _round_price(entry_high),
        "stop_loss_price": _round_price(stop_loss),
        "take_profit_1": _round_price(take_profit_1),
        "take_profit_2": _round_price(take_profit_2),
        "position_weight": _position_weight(row, strategy, status),
        "max_holding_days": int(profile["max_holding_days"]),
        "entry_conditions_json": json.dumps(entry_conditions, ensure_ascii=False),
        "sell_rules_json": json.dumps(sell_rules, ensure_ascii=False),
        "invalidation_reason": invalidation[:500],
        "cooldown_days_left": cooldown_days_left if not cooldown_bypassed else 0,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until and not cooldown_bypassed else None,
    }


def add_strategy_signals(
    df: pd.DataFrame,
    confidence: pd.DataFrame | None = None,
    rec_history: pd.DataFrame | None = None,
    failures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out = df.copy()
    for extra in (confidence, rec_history, failures):
        if extra is not None and not extra.empty and "stock_code" in extra.columns:
            extra = extra.copy()
            extra["stock_code"] = extra["stock_code"].astype(str).str.strip().str.zfill(6)
            out = out.merge(extra, on="stock_code", how="left")

    amount = _numeric_col(out, "amount")
    turnover = _numeric_col(out, "turnover_ratio")
    change_pct = _numeric_col(out, "change_pct")
    dist_ma20 = _numeric_col(out, "dist_ma20")
    volatility = _numeric_col(out, "volatility_20")
    close = _numeric_col(out, "close")
    amount_ratio = _numeric_col(out, "amount_ratio_5")

    liquidity_score = _round_score(
        _series_score(amount, 30_000_000, 600_000_000) * 0.70
        + _series_score(turnover, 0.5, 8.0) * 0.30
    )
    ultra_volatility_fit = (100 - (volatility - 5.0).abs() * 10).clip(35, 100).fillna(55)
    ultra_penalty = (
        pd.Series(np.where(change_pct >= 7.0, 6.0, 0.0), index=out.index)
        + pd.Series(np.where(dist_ma20 >= 14.0, 6.0, 0.0), index=out.index)
        + pd.Series(np.where(amount < 50_000_000, 8.0, 0.0), index=out.index)
    )
    out["ultra_short_score"] = _round_score(
        out["technical_score"] * 0.30
        + out["capital_score"] * 0.28
        + out["sentiment_score"] * 0.20
        + out["event_score"] * 0.10
        + liquidity_score * 0.08
        + ultra_volatility_fit * 0.04
        - ultra_penalty
    )

    swing_trend = (
        (pd.to_numeric(out.get("close"), errors="coerce") > pd.to_numeric(out.get("ma20"), errors="coerce")).astype(float) * 55
        + (pd.to_numeric(out.get("ma20"), errors="coerce") > pd.to_numeric(out.get("ma60"), errors="coerce")).astype(float) * 45
    )
    swing_penalty = (
        pd.Series(np.where(change_pct >= 8.0, 5.0, 0.0), index=out.index)
        + pd.Series(np.where(pd.to_numeric(out.get("drawdown_60"), errors="coerce") <= -25.0, 6.0, 0.0), index=out.index)
    )
    out["swing_score"] = _round_score(
        out["long_term_score"] * 0.42
        + out["technical_score"] * 0.18
        + out["capital_score"] * 0.10
        + out["event_score"] * 0.10
        + out["risk_score"] * 0.12
        + swing_trend * 0.08
        - swing_penalty
    )

    out["quality_score"] = _round_score(out["ai_score"])

    resistance = pd.concat([
        _numeric_col(out, "high_20"),
        _numeric_col(out, "high_60"),
    ], axis=1).max(axis=1)
    resistance = resistance.where(resistance > 0, np.nan)
    out["resistance_price"] = resistance.round(2)
    expected_return_pct = (resistance / close.replace(0, np.nan) - 1.0) * 100.0
    out["expected_return_pct"] = pd.to_numeric(expected_return_pct, errors="coerce").fillna(0.0).round(2)
    out["expected_return_score"] = _round_score(_series_score(out["expected_return_pct"], 5.0, 20.0, default=45.0))

    fused_rank = _numeric_col(out, "fused_rank")
    heat_score = pd.Series(60.0, index=out.index)
    heat_score = heat_score.mask((fused_rank > 0) & (fused_rank <= 5), 45.0)
    heat_score = heat_score.mask((fused_rank > 5) & (fused_rank <= 10), 55.0)
    heat_score = heat_score.mask((fused_rank > 10) & (fused_rank <= 30), 85.0)
    heat_score = heat_score.mask((fused_rank > 30) & (fused_rank <= 60), 75.0)
    heat_score = heat_score.mask((fused_rank > 60) & (fused_rank <= 100), 68.0)
    out["heat_overload_score"] = _round_score(heat_score)

    out["confidence_score"] = _numeric_col(out, "confidence_score", 62.0).fillna(62.0).clip(35, 100).round(1)
    out["failure_penalty_score"] = _numeric_col(out, "failure_penalty_score", 100.0).fillna(100.0).clip(35, 100).round(1)
    out["sector_rotation_score"] = _numeric_col(out, "sector_rotation_score", 55.0).fillna(55.0).clip(30, 100).round(1)

    chase_score = (
        100
        - _series_score(change_pct, 3.0, 8.0, default=45.0) * 0.45
        - _series_score(dist_ma20, 4.0, 15.0, default=45.0) * 0.35
        - _series_score(amount_ratio, 1.8, 3.5, default=20.0) * 0.20
    ).clip(0, 100)
    liquidity_fit = liquidity_score
    out["entry_score"] = _round_score(
        chase_score * 0.28
        + out["expected_return_score"] * 0.26
        + out["heat_overload_score"] * 0.16
        + liquidity_fit * 0.12
        + out["sector_rotation_score"] * 0.10
        + out["confidence_score"] * 0.08
    )
    out["final_trade_score"] = _round_score(out["quality_score"] * 0.70 + out["entry_score"] * 0.30)
    out["max_recent_trade_score"] = _numeric_col(out, "max_recent_trade_score", 0.0).fillna(0.0)

    ma5 = _numeric_col(out, "ma5")
    ma10 = _numeric_col(out, "ma10")
    ma20 = _numeric_col(out, "ma20")
    ma60 = _numeric_col(out, "ma60")
    high_20 = _numeric_col(out, "high_20")
    high_60 = _numeric_col(out, "high_60")
    from_low_60 = _numeric_col(out, "from_low_60", 0.0).fillna(0.0)
    amount_ratio_20 = _numeric_col(out, "amount_ratio_20", 1.0).fillna(1.0).clip(0, 5)
    dist_ma20_main = _numeric_col(out, "dist_ma20", 0.0).fillna(0.0)

    trend_alignment_score = (
        (close > ma5).astype(float) * 18.0
        + (ma5 > ma10).astype(float) * 18.0
        + (ma10 > ma20).astype(float) * 22.0
        + (ma20 >= ma60 * 0.96).astype(float) * 17.0
        + (close > ma20).astype(float) * 25.0
    )
    near_high20_score = _series_score((close / high_20.replace(0, np.nan)) * 100.0, 94.0, 101.0, default=45.0)
    near_high60_score = _series_score((close / high_60.replace(0, np.nan)) * 100.0, 88.0, 100.0, default=45.0)
    breakout_score = pd.concat([near_high20_score, near_high60_score], axis=1).max(axis=1)
    volume_wave_score = (100.0 - (amount_ratio_20 - 1.6).abs() * 35.0).clip(35, 100)
    strength_score = _series_score(_numeric_col(out, "pct_20", 0.0), 5.0, 45.0, default=45.0)
    over_extension_penalty = (
        pd.Series(np.where((dist_ma20_main >= 35.0) & (from_low_60 >= 120.0), 10.0, 0.0), index=out.index)
        + pd.Series(np.where((change_pct >= 9.7) & (amount_ratio_20 >= 2.5), 6.0, 0.0), index=out.index)
    )
    out["main_wave_score"] = _round_score(
        trend_alignment_score * 0.32
        + breakout_score * 0.18
        + volume_wave_score * 0.14
        + out["capital_score"] * 0.12
        + out["sector_rotation_score"] * 0.12
        + strength_score * 0.08
        + out["quality_score"] * 0.04
        - over_extension_penalty
    )
    hold_penalty = (
        pd.Series(np.where(close < ma10, 24.0, 0.0), index=out.index)
        + pd.Series(np.where(close < ma20, 46.0, 0.0), index=out.index)
        + pd.Series(np.where((dist_ma20_main >= 28.0) & (from_low_60 >= 120.0), 18.0, 0.0), index=out.index)
        + pd.Series(np.where((change_pct <= -6.0) & (amount_ratio_20 >= 1.15), 18.0, 0.0), index=out.index)
    )
    hold_bonus = (
        (close > ma10).astype(float) * 10.0
        + (close > ma20).astype(float) * 10.0
        + (ma10 > ma20).astype(float) * 8.0
        + (ma20 >= ma60 * 0.96).astype(float) * 6.0
    )
    out["trend_hold_score"] = _round_score(64.0 + hold_bonus - hold_penalty)
    out["trend_stop_price"] = (ma20 * 0.97).round(2)
    out["trend_reduce_price"] = (ma10 * 0.98).round(2)

    stages: list[str] = []
    signals: list[str] = []
    reasons: list[str] = []
    for row in out.to_dict(orient="records"):
        mw = _safe_number(row.get("main_wave_score"), 0.0)
        hold = _safe_number(row.get("trend_hold_score"), 0.0)
        d20 = _safe_number(row.get("dist_ma20"), 0.0)
        low60_gain = _safe_number(row.get("from_low_60"), 0.0)
        chg = _safe_number(row.get("change_pct"), 0.0)
        ar20 = _safe_number(row.get("amount_ratio_20"), 1.0)
        c = _safe_number(row.get("close"), 0.0)
        r_ma10 = _safe_number(row.get("ma10"), 0.0)
        r_ma20 = _safe_number(row.get("ma20"), 0.0)

        if mw >= 78 and d20 >= 28 and low60_gain >= 120:
            stage = "EXTENDED"
        elif mw >= 74:
            stage = "BREAKOUT"
        elif mw >= 70:
            stage = "TRENDING"
        elif mw >= 62:
            stage = "WATCH_BASE"
        else:
            stage = "NONE"

        if c > 0 and r_ma20 > 0 and c < r_ma20 and low60_gain >= 120:
            signal = "SELL_ALERT"
            reason = "主升浪跌破MA20，趋势破坏，优先清仓或大幅减仓"
        elif c > 0 and r_ma10 > 0 and c < r_ma10 and low60_gain >= 150:
            signal = "REDUCE"
            reason = "高位跌破MA10，主升斜率转弱，先减仓并观察MA20"
        elif d20 >= 28 and low60_gain >= 120:
            signal = "REDUCE"
            reason = "距离MA20过大且累计涨幅高，进入高位扩张区，提示减仓/收紧止盈"
        elif chg <= -6.0 and ar20 >= 1.15 and low60_gain >= 80:
            signal = "REDUCE"
            reason = "高位放量长阴，主升浪风险升高"
        elif mw >= 74 and hold >= 58 and chg < 9.7:
            signal = "BUY_READY"
            reason = "主升浪放量突破且趋势保持，等待回踩MA5/MA10或盘中确认"
        elif mw >= 70 and hold >= 50:
            signal = "WATCH"
            reason = "主升浪雏形成立，等待突破确认或缩量回踩"
        else:
            signal = "NONE"
            reason = "尚未形成主升浪买卖点"

        stages.append(stage)
        signals.append(signal)
        reasons.append(reason)

    out["main_wave_stage"] = stages
    out["main_wave_signal"] = signals
    out["main_wave_reason"] = reasons

    plans = []
    suitable_col: list[str] = []
    for row in out.to_dict(orient="records"):
        primary = select_primary_strategy(row)
        plan = build_strategy_trade_plan(row, primary)
        suitable = [
            name for name in STRATEGY_PROFILES
            if _strategy_score(row, name) >= float(STRATEGY_PROFILES[name]["min_score"])
        ]
        if not suitable and plan["signal_status"] != "BLOCK":
            suitable = [primary]
        plans.append(plan)
        suitable_col.append(json.dumps(suitable, ensure_ascii=False))

    for key in [
        "primary_strategy", "strategy_profile", "signal_status", "signal_reason",
        "entry_price_low", "entry_price_high", "stop_loss_price",
        "take_profit_1", "take_profit_2", "position_weight",
        "max_holding_days", "entry_conditions_json", "sell_rules_json",
        "invalidation_reason", "cooldown_days_left", "cooldown_until",
    ]:
        out[key] = [plan.get(key) for plan in plans]
    out["suitable_strategies"] = suitable_col
    out["model_version"] = MODEL_VERSION
    return out


def compute_scores(
    kline: pd.DataFrame,
    finance: pd.DataFrame,
    flow: pd.DataFrame,
    hot: pd.DataFrame,
    notices: pd.DataFrame,
    market_mood_score: float,
    flow_date: str,
    trade_date: str,
    min_score: float,
    sector: pd.DataFrame | None = None,
    confidence: pd.DataFrame | None = None,
    rec_history: pd.DataFrame | None = None,
    failures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    flow = _ensure_columns(flow, {
        "main_net_inflow": np.nan,
        "main_net_inflow_5d": np.nan,
        "main_net_inflow_20d": np.nan,
        "flow_trade_date": None,
    })
    hot = _ensure_columns(hot, {
        "fused_rank": np.nan,
        "hot_total_score": np.nan,
        "source_flag": "",
        "industry_name": "",
    })
    notices = _ensure_columns(notices, {
        "notice_count": 0,
        "notice_positive": 0,
        "notice_negative": 0,
        "notice_critical": 0,
        "latest_notice_date": None,
        "risk_titles": None,
        "positive_titles": None,
    })
    sector = _ensure_columns(sector if sector is not None else pd.DataFrame({"stock_code": []}), {
        "industry_name": "",
        "sector_rotation_score": 55.0,
    })
    if not sector.empty and "industry_name" in sector.columns:
        sector = sector.rename(columns={"industry_name": "sector_industry_name"})
    df = kline.merge(finance, on="stock_code", how="left")
    df = df.merge(flow, on="stock_code", how="left", suffixes=("", "_flow"))
    df = df.merge(hot[["stock_code", "fused_rank", "hot_total_score", "source_flag", "industry_name"]], on="stock_code", how="left")
    if not sector.empty:
        df = df.merge(sector[["stock_code", "sector_industry_name", "sector_rotation_score"]], on="stock_code", how="left")
        df["industry_name"] = df["industry_name"].fillna("").astype(str)
        df["sector_industry_name"] = df["sector_industry_name"].fillna("").astype(str)
        df["industry_name"] = df["industry_name"].where(df["industry_name"] != "", df["sector_industry_name"])
    df = df.merge(notices, on="stock_code", how="left")

    for col in ["notice_count", "notice_positive", "notice_negative", "notice_critical"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)
    df["risk_titles"] = df["risk_titles"].apply(lambda x: x if isinstance(x, list) else [])
    df["positive_titles"] = df["positive_titles"].apply(lambda x: x if isinstance(x, list) else [])

    close = _numeric_col(df, "close")
    amount = _numeric_col(df, "amount")
    turnover = _numeric_col(df, "turnover_ratio")

    trend_score = (
        (close > _numeric_col(df, "ma5")).astype(float) * 12
        + (close > _numeric_col(df, "ma10")).astype(float) * 10
        + (close > _numeric_col(df, "ma20")).astype(float) * 12
        + (_numeric_col(df, "ma5") > _numeric_col(df, "ma10")).astype(float) * 8
        + (_numeric_col(df, "ma10") > _numeric_col(df, "ma20")).astype(float) * 8
    )
    technical = (
        30
        + trend_score
        + (_series_score(df["pct_5"], -8, 12) - 50) * 0.18
        + (_series_score(df["dist_ma20"], -15, 12) - 50) * 0.12
        + (_series_score(df["amount_ratio_5"], 0.5, 2.2) - 50) * 0.10
        + (_series_score(turnover, 0.5, 8.0) - 50) * 0.08
        - (_series_score(df["volatility_20"], 2.5, 9.0) - 50).clip(lower=0) * 0.10
    )
    df["technical_score"] = _round_score(technical)

    main_ratio = _numeric_col(df, "main_net_inflow") / amount.replace(0, np.nan) * 100.0
    flow5_ratio = _numeric_col(df, "main_net_inflow_5d") / (_numeric_col(df, "amount_ma5") * 5).replace(0, np.nan) * 100.0
    flow20_ratio = _numeric_col(df, "main_net_inflow_20d") / (_numeric_col(df, "amount_ma20") * 20).replace(0, np.nan) * 100.0
    capital = (
        _percentile_score(main_ratio, default=50) * 0.45
        + _percentile_score(flow5_ratio, default=50) * 0.35
        + _percentile_score(flow20_ratio, default=50) * 0.20
    )
    if flow_date and flow_date != trade_date:
        capital = capital * 0.75 + 50 * 0.25
    df["capital_score"] = _round_score(capital)

    sentiment = pd.Series(50.0, index=df.index)
    has_hot = pd.to_numeric(df["fused_rank"], errors="coerce").notna()
    sentiment.loc[has_hot] = (101 - pd.to_numeric(df.loc[has_hot, "fused_rank"], errors="coerce")).clip(0, 100) * 0.55 + 45
    sentiment = sentiment * 0.8 + float(market_mood_score) * 0.2
    df["sentiment_score"] = _round_score(sentiment)
    df["market_mood_score"] = float(market_mood_score)

    roe = _series_score(_numeric_col(df, "roe_wtd"), -5, 18)
    gross_margin = _series_score(_numeric_col(df, "gross_margin"), 5, 45)
    net_margin = _series_score(_numeric_col(df, "net_margin"), -10, 20)
    oper_cf = _series_score(_numeric_col(df, "oper_cf_ps"), -1, 2)
    df["fundamental_score"] = _round_score(roe * 0.38 + gross_margin * 0.24 + net_margin * 0.24 + oper_cf * 0.14)

    rev_growth = _series_score(_numeric_col(df, "total_rev_yoy_gr"), -20, 40)
    profit_growth = _series_score(_numeric_col(df, "net_profit_yoy_gr"), -40, 80)
    non_gaap_growth = _series_score(_numeric_col(df, "non_gaap_net_profit_yoy_gr"), -40, 80)
    df["growth_score"] = _round_score(rev_growth * 0.35 + profit_growth * 0.45 + non_gaap_growth * 0.20)

    pb = close / _numeric_col(df, "net_asset_ps").replace(0, np.nan)
    pb_score = 100 - _series_score(pb, 1.0, 9.0) * 0.70
    pb_score = pb_score.mask(pb <= 0, 35).fillna(55)
    df["valuation_score"] = _round_score(pb_score)

    debt_score = 100 - _series_score(_numeric_col(df, "asset_liab_ratio"), 30, 85) * 0.65
    cash_score = _series_score(_numeric_col(df, "cash_flow_ratio"), -0.2, 1.5)
    curr_score = _series_score(_numeric_col(df, "curr_ratio"), 0.8, 2.0)
    df["risk_score"] = _round_score(debt_score * 0.55 + cash_score * 0.25 + curr_score * 0.20)

    event_risk_score = 100 - df["notice_critical"] * 35 - df["notice_negative"] * 14 + df["notice_positive"] * 4
    df["event_risk_score"] = _round_score(event_risk_score)
    df["event_risk_level"] = np.select(
        [
            df["notice_critical"] > 0,
            df["notice_negative"] >= 2,
            df["notice_negative"] == 1,
        ],
        ["CRITICAL", "HIGH", "MEDIUM"],
        default="LOW",
    )
    event_score = 58 + df["notice_positive"] * 8 - df["notice_negative"] * 10 - df["notice_critical"] * 25
    df["event_score"] = _round_score(event_score)

    quality_scores: list[float] = []
    quality_flags: list[str] = []
    for row in df.to_dict(orient="records"):
        score, flags = build_data_quality(row, trade_date=trade_date, flow_date=flow_date)
        quality_scores.append(score)
        quality_flags.append(json.dumps(flags, ensure_ascii=False))
    df["data_quality_score"] = quality_scores
    df["data_quality_flags"] = quality_flags

    df["long_term_score"] = _round_score(
        df["fundamental_score"] * 0.34
        + df["growth_score"] * 0.28
        + df["valuation_score"] * 0.20
        + df["risk_score"] * 0.18
    )
    df["short_term_score"] = _round_score(
        df["technical_score"] * 0.34
        + df["capital_score"] * 0.28
        + df["sentiment_score"] * 0.24
        + df["event_score"] * 0.14
    )
    df["ai_score"] = _round_score(df["short_term_score"] * 0.58 + df["long_term_score"] * 0.42)

    statuses: list[str] = []
    reasons: list[str] = []
    for row in df.to_dict(orient="records"):
        status, reason = choose_recommend_status(
            row.get("stock_code", ""),
            row.get("short_name", ""),
            row.get("ai_score", 0),
            row.get("short_term_score", 0),
            row.get("long_term_score", 0),
            row.get("event_risk_level", "LOW"),
            row.get("amount"),
            row.get("change_pct"),
            min_score,
            row.get("data_quality_score", 100.0),
            json.loads(row.get("data_quality_flags") or "[]"),
        )
        statuses.append(status)
        reasons.append(reason)
    df["recommend_status"] = statuses
    df["recommend_reason"] = reasons
    df = add_strategy_signals(df, confidence=confidence, rec_history=rec_history, failures=failures)
    return df


def _json_list(items: list[str]) -> str:
    return json.dumps(items, ensure_ascii=False)


def _build_text_fields(df: pd.DataFrame, flow_date: str, trade_date: str) -> pd.DataFrame:
    summaries: list[str] = []
    recommendations: list[str] = []
    strengths_col: list[str] = []
    risks_col: list[str] = []
    event_detail_col: list[str] = []

    for row in df.to_dict(orient="records"):
        strengths: list[str] = []
        risks: list[str] = []
        if float(row.get("technical_score") or 0) >= 68:
            strengths.append("技术趋势较强")
        if float(row.get("capital_score") or 0) >= 68:
            strengths.append("资金流排名靠前")
        if float(row.get("fundamental_score") or 0) >= 65:
            strengths.append("基本面质量较好")
        if float(row.get("growth_score") or 0) >= 65:
            strengths.append("成长性评分较高")
        if float(row.get("sentiment_score") or 0) >= 68:
            strengths.append("市场热度较高")
        if row.get("positive_titles"):
            strengths.append("近期公告偏积极")

        if row.get("event_risk_level") in ("HIGH", "CRITICAL"):
            risks.append("近期公告存在风险事项")
        if row.get("risk_titles"):
            risks.extend([f"公告风险: {t}" for t in row.get("risk_titles", [])[:2]])
        if flow_date and flow_date != trade_date:
            risks.append(f"资金流使用最近可用日期{flow_date}")
        if float(row.get("amount") or 0) < 30_000_000:
            risks.append("成交额偏低")
        if float(row.get("change_pct") or 0) >= 9.7:
            risks.append("当日涨幅过高")
        if float(row.get("volatility_20") or 0) >= 8:
            risks.append("短期波动偏大")
        if not strengths:
            strengths.append("暂无突出优势，作为基础覆盖样本")

        status = row.get("recommend_status") or "SUSPENDED"
        summary = (
            f"基础评分: 综合{row.get('ai_score'):.1f}, 短线{row.get('short_term_score'):.1f}, "
            f"长线{row.get('long_term_score'):.1f}; 状态{status}。"
        )
        if status == "ALLOW":
            recommendation = "可进入候选池，盘中等待量价和资金二次确认，并设置止损。"
        elif status == "SUSPENDED":
            recommendation = "暂缓买入，等待评分或风险项改善后再复核。"
        else:
            recommendation = "不建议进入推荐池。"

        event_detail = {
            "notice_count_14d": int(row.get("notice_count") or 0),
            "positive_count": int(row.get("notice_positive") or 0),
            "negative_count": int(row.get("notice_negative") or 0),
            "critical_count": int(row.get("notice_critical") or 0),
            "risk_titles": row.get("risk_titles") or [],
            "positive_titles": row.get("positive_titles") or [],
        }
        summaries.append(summary)
        recommendations.append(recommendation)
        strengths_col.append(_json_list(strengths[:6]))
        risks_col.append(_json_list(risks[:6]))
        event_detail_col.append(json.dumps(event_detail, ensure_ascii=False))

    out = df.copy()
    out["summary"] = summaries
    out["recommendation"] = recommendations
    out["strengths"] = strengths_col
    out["risks"] = risks_col
    out["event_risk_detail"] = event_detail_col
    return out


def _none_if_nan(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer, np.floating)):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value
    return value


def build_analysis_rows(df: pd.DataFrame, trade_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    score_cols = [
        "long_term_score", "fundamental_score", "growth_score", "valuation_score", "risk_score",
        "short_term_score", "capital_score", "technical_score", "sentiment_score", "event_score",
        "event_risk_score",
    ]
    for row in df.to_dict(orient="records"):
        latest_notice = row.get("latest_notice_date")
        if latest_notice is None or pd.isna(latest_notice) or str(latest_notice).lower() == "nan":
            last_news_time = None
        else:
            last_news_time = f"{str(latest_notice)[:10]} 00:00:00"
        item = {
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "stock_name": str(row.get("short_name") or "")[:20],
            "analysis_date": trade_date,
            "last_news_time": last_news_time,
            "flow_trade_date": row.get("flow_trade_date"),
            "hot_trade_date": row.get("hot_trade_date"),
            "event_risk_level": str(row.get("event_risk_level") or "LOW"),
            "event_risk_detail": row.get("event_risk_detail") or "[]",
            "recommend_status": str(row.get("recommend_status") or "SUSPENDED"),
            "recommend_reason": str(row.get("recommend_reason") or "")[:500],
            "summary": str(row.get("summary") or "")[:500],
            "recommendation": str(row.get("recommendation") or "")[:500],
            "strengths": row.get("strengths") or "[]",
            "risks": row.get("risks") or "[]",
            "data_quality_score": _none_if_nan(row.get("data_quality_score")),
            "data_quality_flags": row.get("data_quality_flags") or "[]",
            "model_version": row.get("model_version") or MODEL_VERSION,
        }
        for col in score_cols:
            item[col] = _none_if_nan(row.get(col))
        rows.append(item)
    return rows


def build_recommendation_rows(df: pd.DataFrame, trade_date: str, top_n: int, min_score: float) -> list[dict[str, Any]]:
    stock_code_series = df["stock_code"].astype(str).str.strip().str.zfill(6)
    main_wave_signal_series = (
        df["main_wave_signal"].fillna("").astype(str)
        if "main_wave_signal" in df.columns
        else pd.Series("", index=df.index)
    )
    recommend_status_series = (
        df["recommend_status"].fillna("BLOCK").astype(str).str.upper()
    )
    event_risk_series = (
        df["event_risk_level"].fillna("DATA_BLOCKED").astype(str).str.upper()
    )
    signal_status_series = (
        df["signal_status"].fillna("BLOCK").astype(str).str.upper()
    )
    # The recommendation table is also the ranked observation ledger.  Keep
    # soft-risk SUSPENDED/WATCH rows visible for later V4/V5/V6 evaluation,
    # while hard blocks and exit signals remain excluded.  Execution remains
    # fail-closed in the selector and execution router.
    ranked_observation_candidate = (
        recommend_status_series.isin({"ALLOW", "SUSPENDED"})
        & (_numeric_col(df, "quality_score", 0.0) >= float(min_score))
    )
    main_wave_candidate = (
        (_numeric_col(df, "main_wave_score", 0.0) >= float(STRATEGY_PROFILES["main_wave"]["min_score"]))
        & recommend_status_series.isin({"ALLOW", "SUSPENDED"})
        & (event_risk_series != "CRITICAL")
    )
    eligible = df[
        (
            ranked_observation_candidate
            | main_wave_candidate
        )
        & (recommend_status_series != "BLOCK")
        & (signal_status_series != "BLOCK")
        & (~main_wave_signal_series.isin(["REDUCE", "SELL_ALERT"]))
        & (event_risk_series != "CRITICAL")
        & (stock_code_series.str.match(r"^(0|3|6)"))
    ].copy()
    eligible["ranking_score"] = pd.concat([
        _numeric_col(eligible, "final_trade_score", 0.0),
        _numeric_col(eligible, "main_wave_score", 0.0),
    ], axis=1).max(axis=1)
    eligible = eligible.sort_values(
        ["ranking_score", "final_trade_score", "main_wave_score", "entry_score", "quality_score", "capital_score"],
        ascending=False,
    ).head(int(top_n))

    rows: list[dict[str, Any]] = []
    for row in eligible.to_dict(orient="records"):
        reason = (
            f"综合{row.get('ai_score'):.1f}: "
            f"短线{row.get('short_term_score'):.1f}/长线{row.get('long_term_score'):.1f}; "
            f"{row.get('recommend_reason')}"
        )
        sources = "fast_eod"
        if row.get("source_flag") and not pd.isna(row.get("source_flag")):
            sources += f"+hot:{row.get('source_flag')}"
        rows.append({
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "short_name": str(row.get("short_name") or "")[:20],
            "ai_score": round(float(row.get("ai_score") or 0), 1),
            "long_term_score": round(float(row.get("long_term_score") or 0), 1),
            "short_term_score": round(float(row.get("short_term_score") or 0), 1),
            "fundamental": round(float(row.get("fundamental_score") or 0), 1),
            "capital_score": round(float(row.get("capital_score") or 0), 1),
            "valuation": round(float(row.get("valuation_score") or 0), 1),
            "technical": round(float(row.get("technical_score") or 0), 1),
            "reason": reason[:500],
            "sources": sources[:100],
            "pick_date": trade_date,
            "recommend_status": row.get("recommend_status") or "BLOCK",
            "recommend_reason": str(row.get("recommend_reason") or "")[:500],
            "event_risk_level": row.get("event_risk_level") or "LOW",
            "sentiment_score": round(float(row.get("sentiment_score") or 0), 1),
            "market_mood_score": round(float(row.get("market_mood_score") or 0), 1),
            "event_score": round(float(row.get("event_score") or 0), 1),
            "ultra_short_score": round(float(row.get("ultra_short_score") or 0), 1),
            "swing_score": round(float(row.get("swing_score") or 0), 1),
            "primary_strategy": row.get("primary_strategy") or "",
            "strategy_profile": row.get("strategy_profile") or "",
            "suitable_strategies": row.get("suitable_strategies") or "[]",
            "signal_status": row.get("signal_status") or "WATCH",
            "signal_reason": str(row.get("signal_reason") or "")[:500],
            "entry_price_low": _none_if_nan(row.get("entry_price_low")),
            "entry_price_high": _none_if_nan(row.get("entry_price_high")),
            "stop_loss_price": _none_if_nan(row.get("stop_loss_price")),
            "take_profit_1": _none_if_nan(row.get("take_profit_1")),
            "take_profit_2": _none_if_nan(row.get("take_profit_2")),
            "position_weight": _none_if_nan(row.get("position_weight")),
            "max_holding_days": int(row.get("max_holding_days") or 0),
            "entry_conditions_json": row.get("entry_conditions_json") or "[]",
            "sell_rules_json": row.get("sell_rules_json") or "[]",
            "invalidation_reason": str(row.get("invalidation_reason") or "")[:500],
            "quality_score": round(float(row.get("quality_score") or 0), 1),
            "entry_score": round(float(row.get("entry_score") or 0), 1),
            "final_trade_score": round(float(row.get("final_trade_score") or 0), 1),
            "expected_return_score": round(float(row.get("expected_return_score") or 0), 1),
            "expected_return_pct": round(float(row.get("expected_return_pct") or 0), 2),
            "resistance_price": _none_if_nan(row.get("resistance_price")),
            "heat_overload_score": round(float(row.get("heat_overload_score") or 0), 1),
            "confidence_score": round(float(row.get("confidence_score") or 0), 1),
            "sector_rotation_score": round(float(row.get("sector_rotation_score") or 0), 1),
            "failure_penalty_score": round(float(row.get("failure_penalty_score") or 0), 1),
            "data_quality_score": round(float(row.get("data_quality_score") or 0), 1),
            "data_quality_flags": row.get("data_quality_flags") or "[]",
            "cooldown_days_left": int(row.get("cooldown_days_left") or 0),
            "cooldown_until": _none_if_nan(row.get("cooldown_until")),
            "main_wave_score": round(float(row.get("main_wave_score") or 0), 1),
            "trend_hold_score": round(float(row.get("trend_hold_score") or 0), 1),
            "main_wave_stage": row.get("main_wave_stage") or "",
            "main_wave_signal": row.get("main_wave_signal") or "",
            "main_wave_reason": str(row.get("main_wave_reason") or "")[:500],
            "trend_stop_price": _none_if_nan(row.get("trend_stop_price")),
            "trend_reduce_price": _none_if_nan(row.get("trend_reduce_price")),
            "model_version": row.get("model_version") or MODEL_VERSION,
        })
    return rows


def _execute_batches(conn, sql: str, rows: list[dict[str, Any]], chunk_size: int = 800) -> None:
    statement = text(sql)
    for start in range(0, len(rows), chunk_size):
        # PyMySQL executemany can hang on this schema when rows contain several
        # TEXT/JSON-like fields. Row-wise execution is slower but predictable for
        # the 5k-sized daily analysis workload.
        for row in rows[start:start + chunk_size]:
            conn.execute(statement, row)


def _normalize_stock_codes(stock_codes: list[str] | None) -> list[str]:
    if not stock_codes:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for code in stock_codes:
        value = str(code or "").strip().zfill(6)
        if value and value not in seen:
            seen.add(value)
            normalized.append(value)
    return normalized


def _filter_frame_by_codes(df: pd.DataFrame, stock_codes: list[str] | None) -> pd.DataFrame:
    codes = _normalize_stock_codes(stock_codes)
    if not codes or df is None or df.empty or "stock_code" not in df.columns:
        return df
    return df[df["stock_code"].astype(str).str.strip().str.zfill(6).isin(codes)].copy()


def _delete_scope_rows(
    conn,
    table_name: str,
    date_column: str,
    trade_date: str,
    stock_codes: list[str] | None = None,
) -> None:
    codes = _normalize_stock_codes(stock_codes)
    if not codes:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE {date_column} = :trade_date"),
            {"trade_date": trade_date},
        )
        return

    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params = {"trade_date": trade_date, **{f"code_{idx}": code for idx, code in enumerate(codes)}}
    conn.execute(
        text(
            f"DELETE FROM {table_name} "
            f"WHERE {date_column} = :trade_date AND stock_code IN ({placeholders})"
        ),
        params,
    )


def save_outputs(
    engine: Engine,
    analysis_rows: list[dict[str, Any]],
    rec_rows: list[dict[str, Any]],
    trade_date: str,
    stock_codes: list[str] | None = None,
) -> None:
    analysis_sql = """
        INSERT INTO stock_analysis_result (
            stock_code, stock_name, analysis_date, last_news_time,
            long_term_score, fundamental_score, growth_score, valuation_score, risk_score,
            short_term_score, capital_score, technical_score, sentiment_score, event_score,
            event_risk_score, event_risk_level, event_risk_detail,
            recommend_status, recommend_reason,
            summary, recommendation, strengths, risks,
            data_quality_score, data_quality_flags, flow_trade_date, hot_trade_date, model_version,
            created_at, updated_at
        ) VALUES (
            :stock_code, :stock_name, :analysis_date, :last_news_time,
            :long_term_score, :fundamental_score, :growth_score, :valuation_score, :risk_score,
            :short_term_score, :capital_score, :technical_score, :sentiment_score, :event_score,
            :event_risk_score, :event_risk_level, :event_risk_detail,
            :recommend_status, :recommend_reason,
            :summary, :recommendation, :strengths, :risks,
            :data_quality_score, :data_quality_flags, :flow_trade_date, :hot_trade_date, :model_version,
            NOW(), NOW()
        )
        ON DUPLICATE KEY UPDATE
            stock_name = VALUES(stock_name),
            last_news_time = VALUES(last_news_time),
            long_term_score = VALUES(long_term_score),
            fundamental_score = VALUES(fundamental_score),
            growth_score = VALUES(growth_score),
            valuation_score = VALUES(valuation_score),
            risk_score = VALUES(risk_score),
            short_term_score = VALUES(short_term_score),
            capital_score = VALUES(capital_score),
            technical_score = VALUES(technical_score),
            sentiment_score = VALUES(sentiment_score),
            event_score = VALUES(event_score),
            event_risk_score = VALUES(event_risk_score),
            event_risk_level = VALUES(event_risk_level),
            event_risk_detail = VALUES(event_risk_detail),
            recommend_status = VALUES(recommend_status),
            recommend_reason = VALUES(recommend_reason),
            summary = VALUES(summary),
            recommendation = VALUES(recommendation),
            strengths = VALUES(strengths),
            risks = VALUES(risks),
            data_quality_score = VALUES(data_quality_score),
            data_quality_flags = VALUES(data_quality_flags),
            flow_trade_date = VALUES(flow_trade_date),
            hot_trade_date = VALUES(hot_trade_date),
            model_version = VALUES(model_version),
            updated_at = NOW()
    """
    rec_sql = """
        INSERT INTO st_recommended_stocks (
            stock_code, short_name, ai_score, long_term_score, short_term_score,
            fundamental, capital_score, valuation, technical,
            reason, sources, pick_date,
            recommend_status, recommend_reason, event_risk_level,
            last_check_time, sentiment_score, market_mood_score, event_score,
            ultra_short_score, swing_score, primary_strategy, strategy_profile,
            suitable_strategies, signal_status, signal_reason,
            entry_price_low, entry_price_high, stop_loss_price,
            take_profit_1, take_profit_2, position_weight, max_holding_days,
            entry_conditions_json, sell_rules_json, invalidation_reason,
            quality_score, entry_score, final_trade_score,
            expected_return_score, expected_return_pct, resistance_price,
            heat_overload_score, confidence_score, sector_rotation_score,
            failure_penalty_score, data_quality_score, data_quality_flags, cooldown_days_left, cooldown_until,
            main_wave_score, trend_hold_score, main_wave_stage, main_wave_signal,
            main_wave_reason, trend_stop_price, trend_reduce_price,
            model_version,
            created_at
        ) VALUES (
            :stock_code, :short_name, :ai_score, :long_term_score, :short_term_score,
            :fundamental, :capital_score, :valuation, :technical,
            :reason, :sources, :pick_date,
            :recommend_status, :recommend_reason, :event_risk_level,
            NOW(), :sentiment_score, :market_mood_score, :event_score,
            :ultra_short_score, :swing_score, :primary_strategy, :strategy_profile,
            :suitable_strategies, :signal_status, :signal_reason,
            :entry_price_low, :entry_price_high, :stop_loss_price,
            :take_profit_1, :take_profit_2, :position_weight, :max_holding_days,
            :entry_conditions_json, :sell_rules_json, :invalidation_reason,
            :quality_score, :entry_score, :final_trade_score,
            :expected_return_score, :expected_return_pct, :resistance_price,
            :heat_overload_score, :confidence_score, :sector_rotation_score,
            :failure_penalty_score, :data_quality_score, :data_quality_flags, :cooldown_days_left, :cooldown_until,
            :main_wave_score, :trend_hold_score, :main_wave_stage, :main_wave_signal,
            :main_wave_reason, :trend_stop_price, :trend_reduce_price,
            :model_version,
            NOW()
        )
    """
    _ensure_analysis_columns(engine)
    _ensure_recommended_columns(engine)
    scoped_codes = _normalize_stock_codes(stock_codes)
    with engine.begin() as conn:
        logger.info("Writing %s analysis rows for %s", len(analysis_rows), trade_date)
        _delete_scope_rows(
            conn,
            table_name="stock_analysis_result",
            date_column="analysis_date",
            trade_date=trade_date,
            stock_codes=scoped_codes,
        )
        _execute_batches(conn, analysis_sql, analysis_rows)
        logger.info("Refreshing %s recommendation rows for %s", len(rec_rows), trade_date)
        _delete_scope_rows(
            conn,
            table_name="st_recommended_stocks",
            date_column="pick_date",
            trade_date=trade_date,
            stock_codes=scoped_codes,
        )
        if rec_rows:
            _execute_batches(conn, rec_sql, rec_rows)


def _emit_progress(progress_callback: ProgressCallback | None, **payload: Any) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(payload)
    except Exception:
        logger.debug("progress callback failed", exc_info=True)


def _prepare_batch_outputs(
    engine: Engine,
    trade_date: str,
    min_score: float,
    top_n: int,
    stock_codes: list[str] | None = None,
    progress_callback: ProgressCallback | None = None,
    news_cutoff_time: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float, str, str]:
    scoped_codes = _normalize_stock_codes(stock_codes)

    _emit_progress(progress_callback, stage="load_kline", percent=5, step="鍔犺浇鏃鐗瑰緛...", trade_date=trade_date)
    kline_all = load_kline_features(engine, trade_date)
    kline = _filter_frame_by_codes(kline_all, scoped_codes)
    if scoped_codes and kline.empty:
        raise RuntimeError(f"No K-line rows found for requested stock codes on {trade_date}")

    _emit_progress(progress_callback, stage="load_finance", percent=14, step="鍔犺浇璐㈠姟鍥犲瓙...", trade_date=trade_date)
    finance = _filter_frame_by_codes(load_finance(engine, trade_date), scoped_codes)
    _emit_progress(progress_callback, stage="load_flow", percent=23, step="鍔犺浇璧勯噾娴佹暟鎹?..", trade_date=trade_date)
    flow, flow_date = load_flow_features(engine, trade_date)
    flow = _filter_frame_by_codes(flow, scoped_codes)
    _emit_progress(progress_callback, stage="load_hot", percent=32, step="鍔犺浇鐑害鎺掕...", trade_date=trade_date)
    hot, hot_date = load_hot_rank(engine, trade_date)
    hot = _filter_frame_by_codes(hot, scoped_codes)
    _emit_progress(progress_callback, stage="load_notices", percent=40, step="鍔犺浇鍏憡浜嬩欢...", trade_date=trade_date)
    notices = _filter_frame_by_codes(load_notice_features(engine, trade_date), scoped_codes)
    news = _filter_frame_by_codes(load_news_features(engine, trade_date, cutoff_time=news_cutoff_time), scoped_codes)
    notices = merge_event_features(notices, news)
    _emit_progress(progress_callback, stage="load_confidence", percent=48, step="鍔犺浇浜ゆ槗缃俊搴?..", trade_date=trade_date)
    confidence = _filter_frame_by_codes(load_confidence_features(engine, trade_date), scoped_codes)
    _emit_progress(progress_callback, stage="load_history", percent=56, step="鍔犺浇鍘嗗彶鎺ㄨ崘琛ㄧ幇...", trade_date=trade_date)
    rec_history = _filter_frame_by_codes(load_recommendation_history(engine, trade_date), scoped_codes)
    _emit_progress(progress_callback, stage="load_failures", percent=62, step="鍔犺浇澶辫触鎯╃綒鍥犲瓙...", trade_date=trade_date)
    failures = _filter_frame_by_codes(load_failure_features(engine, trade_date), scoped_codes)
    _emit_progress(progress_callback, stage="load_sector", percent=68, step="鍔犺浇鏉垮潡杞姩鍥犲瓙...", trade_date=trade_date)
    sector = _filter_frame_by_codes(load_sector_rotation_features(engine, trade_date), scoped_codes)
    market_mood_score = compute_market_mood(kline_all)

    logger.info(
        "Loaded data: scope=%s kline=%s finance=%s flow=%s notices=%s hot=%s confidence=%s history=%s failures=%s sector=%s market_mood=%.1f",
        "all" if not scoped_codes else len(scoped_codes),
        len(kline), len(finance), len(flow), len(notices), len(hot),
        len(confidence), len(rec_history), len(failures), len(sector), market_mood_score,
    )

    _emit_progress(progress_callback, stage="compute_scores", percent=78, step="璁＄畻鍏ㄥ競鍦鸿瘎鍒?..", trade_date=trade_date)
    scored = compute_scores(
        kline=kline,
        finance=finance,
        flow=flow,
        hot=hot,
        notices=notices,
        market_mood_score=market_mood_score,
        flow_date=flow_date,
        trade_date=trade_date,
        min_score=min_score,
        sector=sector,
        confidence=confidence,
        rec_history=rec_history,
        failures=failures,
    )
    scored["flow_trade_date"] = flow_date
    scored["hot_trade_date"] = hot_date
    _emit_progress(progress_callback, stage="build_rows", percent=88, step="鐢熸垚鍒嗘瀽涓庢帹鑽愮粨鏋?..", trade_date=trade_date)
    scored = _build_text_fields(scored, flow_date=flow_date, trade_date=trade_date)
    analysis_rows = build_analysis_rows(scored, trade_date)
    rec_rows = build_recommendation_rows(scored, trade_date, top_n=top_n, min_score=min_score)
    return analysis_rows, rec_rows, market_mood_score, flow_date, hot_date


def run_batch(
    engine: Engine,
    trade_date: str | None = None,
    top_n: int = 80,
    min_score: float = 62.0,
    progress_callback: ProgressCallback | None = None,
    strict_prev_trade_day: bool = False,
    execution_time: str | None = None,
    min_kline_coverage: float = 0.80,
    auto_repair_missing_kline: bool = False,
) -> BatchStats:
    if strict_prev_trade_day:
        execution_time = execution_time or datetime.now().replace(microsecond=0).isoformat(sep=" ")
        resolved_trade_date = previous_trade_date(engine, execution_time)
        if trade_date and str(trade_date)[:10] != resolved_trade_date:
            raise RuntimeError(
                f"Strict morning run requires previous trade date {resolved_trade_date}, "
                f"got {str(trade_date)[:10]}"
            )
        trade_date = resolved_trade_date
        try:
            readiness = assert_trade_date_ready(engine, trade_date, min_coverage=min_kline_coverage)
        except Exception as exc:
            if not auto_repair_missing_kline:
                raise
            _emit_progress(
                progress_callback,
                stage="strict_date_missing",
                percent=2,
                step=f"strict previous trade date missing; repairing before analysis: {exc}",
                trade_date=trade_date,
                error=str(exc),
            )
            repair_missing_qmt_kline_for_trade_date(
                trade_date,
                progress_callback=progress_callback,
            )
            readiness = assert_trade_date_ready(engine, trade_date, min_coverage=min_kline_coverage)
        _emit_progress(
            progress_callback,
            stage="strict_date_ready",
            percent=2,
            step="strict previous trade date ready",
            trade_date=trade_date,
            latest_kline_date=readiness.get("latest_kline_date"),
            kline_count=readiness.get("kline_count"),
            expected_count=readiness.get("expected_count"),
        )
    else:
        trade_date = trade_date or latest_trade_date(engine)
    logger.info("Fast analysis batch started for %s", trade_date)

    analysis_rows, rec_rows, market_mood_score, flow_date, hot_date = _prepare_batch_outputs(
        engine=engine,
        trade_date=trade_date,
        min_score=min_score,
        top_n=top_n,
        stock_codes=None,
        progress_callback=progress_callback,
        news_cutoff_time=execution_time if strict_prev_trade_day else None,
    )
    _emit_progress(
        progress_callback,
        stage="save_outputs",
        percent=94,
        step="save outputs",
        trade_date=trade_date,
        analysis_count=len(analysis_rows),
        recommendation_count=len(rec_rows),
    )
    save_outputs(engine, analysis_rows, rec_rows, trade_date)

    stats = BatchStats(
        trade_date=trade_date,
        analysis_count=len(analysis_rows),
        recommendation_count=len(rec_rows),
        market_mood_score=market_mood_score,
        flow_date=flow_date,
        hot_date=hot_date,
    )
    _emit_progress(
        progress_callback,
        stage="done",
        percent=100,
        step="done",
        trade_date=stats.trade_date,
        analysis_count=stats.analysis_count,
        recommendation_count=stats.recommendation_count,
        market_mood_score=stats.market_mood_score,
        flow_date=stats.flow_date,
        hot_date=stats.hot_date,
        done=stats.analysis_count,
    )
    logger.info("Fast analysis completed: %s", stats)
    return stats


def run_batch_for_codes(
    engine: Engine,
    stock_codes: list[str],
    trade_date: str | None = None,
    top_n: int = 80,
    min_score: float = 62.0,
    progress_callback: ProgressCallback | None = None,
) -> BatchStats:
    scoped_codes = _normalize_stock_codes(stock_codes)
    if not scoped_codes:
        raise ValueError("stock_codes must not be empty")

    trade_date = trade_date or latest_trade_date(engine)
    logger.info("Fast scoped analysis started for %s with %s codes", trade_date, len(scoped_codes))
    analysis_rows, rec_rows, market_mood_score, flow_date, hot_date = _prepare_batch_outputs(
        engine=engine,
        trade_date=trade_date,
        min_score=min_score,
        top_n=max(int(top_n), len(scoped_codes)),
        stock_codes=scoped_codes,
        progress_callback=progress_callback,
    )
    _emit_progress(
        progress_callback,
        stage="save_outputs",
        percent=94,
        step="save outputs",
        trade_date=trade_date,
        analysis_count=len(analysis_rows),
        recommendation_count=len(rec_rows),
        done=len(analysis_rows),
    )
    save_outputs(engine, analysis_rows, rec_rows, trade_date, stock_codes=scoped_codes)

    stats = BatchStats(
        trade_date=trade_date,
        analysis_count=len(analysis_rows),
        recommendation_count=len(rec_rows),
        market_mood_score=market_mood_score,
        flow_date=flow_date,
        hot_date=hot_date,
    )
    _emit_progress(
        progress_callback,
        stage="done",
        percent=100,
        step="done",
        trade_date=stats.trade_date,
        analysis_count=stats.analysis_count,
        recommendation_count=stats.recommendation_count,
        market_mood_score=stats.market_mood_score,
        flow_date=stats.flow_date,
        hot_date=stats.hot_date,
        done=stats.analysis_count,
    )
    logger.info("Fast scoped analysis completed: %s", stats)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Fast full-market EOD analysis and recommendation batch")
    parser.add_argument("--date", default="", help="Analysis trade date, default latest sm_stock_kline date")
    parser.add_argument("--top-n", type=int, default=80, help="Number of recommendation rows to keep")
    parser.add_argument("--min-score", type=float, default=62.0, help="Minimum AI score for recommendation eligibility")
    parser.add_argument("--strict-prev-trade-day", action="store_true", help="Use only the previous trading day of execution time and fail if data is incomplete")
    parser.add_argument("--execution-time", default="", help="Execution timestamp/date used by --strict-prev-trade-day, default today")
    parser.add_argument("--min-kline-coverage", type=float, default=0.80, help="Minimum K-line coverage ratio for strict runs")
    parser.add_argument("--auto-repair-missing-kline", action="store_true", help="For strict runs, first repair the target day full-market K-line from Guojin QMT when missing")
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    engine = create_batch_engine()
    stats = run_batch(
        engine=engine,
        trade_date=args.date.strip() or None,
        top_n=args.top_n,
        min_score=args.min_score,
        strict_prev_trade_day=args.strict_prev_trade_day,
        execution_time=args.execution_time.strip() or None,
        min_kline_coverage=args.min_kline_coverage,
        auto_repair_missing_kline=args.auto_repair_missing_kline,
    )
    payload = {
        "trade_date": stats.trade_date,
        "analysis_count": stats.analysis_count,
        "recommendation_count": stats.recommendation_count,
        "market_mood_score": stats.market_mood_score,
        "flow_date": stats.flow_date,
        "hot_date": stats.hot_date,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"fast analysis done: date={stats.trade_date}, "
            f"analysis={stats.analysis_count}, recommendations={stats.recommendation_count}, "
            f"market_mood={stats.market_mood_score:.1f}, flow_date={stats.flow_date or '-'}, hot_date={stats.hot_date or '-'}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
