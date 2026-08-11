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
import gc
import hashlib
import json
import logging
import math
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, quote_identifier, read_frame
from server.common.kline_data import get_kline_engine, should_use_kline_engine
from server.common.minute_data import (
    get_minute_engine,
    get_stock_minute_prices,
    minute_source_info,
)
from server.common.process_env import build_child_env
from server.common.versioned_strategy_config import stock_strategy_profiles
from biz.market_context.external_market import load_latest_external_market_context

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[dict[str, Any]], None]

CRITICAL_NOTICE_KEYWORDS = (
    "退市", "终止上市", "暂停上市", "重大违法", "被立案", "立案调查", "欺诈发行",
)
NEGATIVE_NOTICE_KEYWORDS = (
    "减持", "处罚", "问询函", "监管函", "警示函", "诉讼", "仲裁", "冻结",
    "质押", "亏损", "预亏", "业绩预降", "下修", "债务", "违约", "风险提示",
    "集采", "限产", "政策利空", "监管趋严", "补贴退坡", "产能过剩", "反垄断", "价格管制",
    "业绩变脸", "大额减值", "商誉减值", "计提减值", "坏账准备", "存货跌价", "应收账款坏账",
)
POSITIVE_NOTICE_KEYWORDS = (
    "回购", "增持", "中标", "签订合同", "战略合作", "股权激励", "分红", "利润分配",
    "业绩预增", "扭亏", "订单", "投资者回报",
    "业绩快报", "预盈", "净利润增长", "扣非增长", "大幅增长", "持续增长", "高增长",
)


@dataclass(frozen=True)
class BatchStats:
    trade_date: str
    analysis_count: int
    recommendation_count: int
    market_mood_score: float
    flow_date: str
    hot_date: str


MODEL_VERSION = "ai-rec-v3-ext-v1"
MODEL_VERSION_COLUMN_LENGTH = 64
CHINA_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
CHASE_POLICY_VERSION = "EXTREME_EXTENSION_POLICY_V2"
CHASE_RECENT_SURGE_LOOKBACK = 20
CHASE_DANGER_SURGE_STREAK = 3
CHASE_EXTREME_SURGE_STREAK = 4
CHASE_REBASE_MIN_SESSIONS = 5
CHASE_REBASE_DRAWDOWN_PCT = -10.0
CHASE_REBASE_MA20_DISTANCE_PCT = 3.0
CHASE_REBASE_PULLBACK_ATR = 2.0
# These legacy enrichments either read mutable current-state tables or bound
# rows only by business/report date.  Until their source acquisition timestamp
# and revision history can prove knowledge-time eligibility, their contribution
# to cutoff decisions is deliberately neutral.
LEGACY_PIT_DISABLED_FACTOR_INVENTORY: tuple[str, ...] = (
    "dividend",
    "research_theme",
    "business_purity",
    "institutional",
    "industry_prosperity",
    "investor_interaction",
    "confidence_history",
    "recommendation_history",
    "failure_samples",
    "chip_capital",
    "stock_north_holding",
    "size_liquidity",
    "market_margin",
    "market_style",
    "market_north_flow",
    "etf_flow",
    "retail_sentiment",
    "macro_indicator",
    "event_relation_rules",
    "minute_chan_enrichment",
    "strategy_runtime_params",
)
LEGACY_PIT_DISABLED_FACTOR_REASON = (
    "neutralized because the legacy source has no provable acquisition-time "
    "revision bounded by the decision knowledge cutoff"
)
EXCLUDED_RECOMMEND_PREFIXES = ("688",)
MIN_EXECUTABLE_RISK_REWARD = 3.0
MIN_SECTOR_FLOW_AMOUNT_3D = 500_000_000.0
MIN_SECTOR_ROTATION_SCORE = 50.0
PRICE_CROSSCHECK_TOLERANCE_PCT = 1.0
POSITION_RISK_CAPS = {
    "LOW": 30.0,
    "MEDIUM": 20.0,
    "HIGH": 10.0,
}
SYSTEM_SINGLE_POSITION_CAP = 12.0
MAJOR_UNLOCK_RATIO_PCT = 10.0
MAJOR_UNLOCK_MARKET_CAP_RATIO_PCT = 5.0
PLEDGE_RATIO_CAP_PCT = 50.0
SHAREHOLDER_REDUCTION_RATIO_CAP_PCT = 2.0
GOODWILL_RATIO_WATCH_PCT = 20.0
GOODWILL_RATIO_HIGH_PCT = 30.0
NORTH_STOCK_HOLDING_MIN_RATIO_PCT = 1.0
NORTH_STOCK_REDUCTION_DELTA_PCT = -0.30
BUSINESS_PURITY_LOW_SCORE = 35.0
INDUSTRY_PROSPERITY_LOW_SCORE = 35.0
INSTITUTIONAL_WEAK_SCORE = 35.0
ETF_FLOW_PRESSURE_AMOUNT_3D = -3_000_000_000.0
ETF_FLOW_SUPPORT_AMOUNT_3D = 3_000_000_000.0
INTERACTION_RISK_LOW_SCORE = 35.0
RETAIL_BULLISH_EXTREME_PCT = 75.0
RETAIL_BEARISH_EXTREME_PCT = 75.0
DEFAULT_RUNTIME_PARAMS: dict[str, float] = {
    "min_risk_reward": MIN_EXECUTABLE_RISK_REWARD,
    "min_sector_flow_amount_3d": MIN_SECTOR_FLOW_AMOUNT_3D,
    "min_sector_rotation_score": MIN_SECTOR_ROTATION_SCORE,
    "price_crosscheck_tolerance_pct": PRICE_CROSSCHECK_TOLERANCE_PCT,
}
ACTIVE_RUNTIME_PARAMS: dict[str, float] = DEFAULT_RUNTIME_PARAMS.copy()


def runtime_threshold(key: str, default: float | None = None) -> float:
    fallback = DEFAULT_RUNTIME_PARAMS.get(key, default if default is not None else 0.0)
    return _safe_number(ACTIVE_RUNTIME_PARAMS.get(key, fallback), _safe_number(fallback, 0.0))


def set_active_runtime_params(params: dict[str, Any] | None) -> dict[str, float]:
    global ACTIVE_RUNTIME_PARAMS
    merged = DEFAULT_RUNTIME_PARAMS.copy()
    for key, value in (params or {}).items():
        if key in DEFAULT_RUNTIME_PARAMS:
            merged[key] = _safe_number(value, DEFAULT_RUNTIME_PARAMS[key])
    ACTIVE_RUNTIME_PARAMS = merged
    return ACTIVE_RUNTIME_PARAMS.copy()


def _query_engine(engine: Engine, sql: str) -> Engine:
    return get_kline_engine() if should_use_kline_engine(sql) else engine


def _read_frame(sql, engine: Engine, params: dict[str, Any] | None = None) -> pd.DataFrame:
    return read_frame(sql, engine, params=params)


_INDEX_HINT_CACHE: dict[tuple[int, str, tuple[str, ...]], str] = {}
_TRANSIENT_DB_ERRNOS = {1205, 1213, 2003, 2013}


def _db_errno(exc: BaseException) -> int | None:
    orig = getattr(exc, "orig", None)
    args = getattr(orig, "args", ()) or getattr(exc, "args", ())
    if not args:
        return None
    try:
        return int(args[0])
    except (TypeError, ValueError):
        return None


def _db_read_attempts() -> int:
    try:
        return max(1, int(float(os.environ.get("PROBIGA_BATCH_DB_READ_RETRIES", "3"))))
    except (TypeError, ValueError):
        return 3


def _mysql_force_index_hint(engine: Engine, table_name: str, *index_names: str) -> str:
    """Return a MySQL FORCE INDEX hint only when one of the indexes exists."""
    if not index_names or getattr(engine.dialect, "name", "") not in {"mysql", "mariadb"}:
        return ""
    cache_key = (id(engine), table_name, tuple(index_names))
    if cache_key in _INDEX_HINT_CACHE:
        return _INDEX_HINT_CACHE[cache_key]
    hint = ""
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT index_name
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND table_name = :table_name
                    """
                ),
                {"table_name": table_name},
            ).scalars().all()
        available = set(str(row) for row in rows)
        for index_name in index_names:
            if index_name in available:
                hint = f" FORCE INDEX ({quote_identifier(index_name)})"
                break
    except Exception:
        logger.debug("Failed to resolve index hint for %s", table_name, exc_info=True)
    _INDEX_HINT_CACHE[cache_key] = hint
    return hint

STRATEGY_PROFILES: dict[str, dict[str, Any]] = stock_strategy_profiles()


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


VALUATION_STYLE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "growth": (
        "科技", "软件", "电子", "半导体", "芯片", "新能源", "光伏", "电池", "医药", "生物",
        "AI", "人工智能", "航天", "航空", "军工", "计算机", "通信", "机器人",
        "technology", "software", "semiconductor", "chip", "new energy", "pharma", "ai",
    ),
    "stable": (
        "消费", "食品", "饮料", "家电", "制造", "机械", "轻工", "纺织", "汽车", "乳品",
        "consumer", "food", "beverage", "manufacturing", "machinery", "auto",
    ),
    "cyclical": (
        "煤炭", "钢铁", "有色", "化工", "石化", "建材", "水泥", "航运", "矿", "铜", "铝",
        "coal", "steel", "chemical", "materials", "cement", "shipping", "metal",
    ),
    "value": (
        "银行", "保险", "证券", "房地产", "地产", "公用事业", "电力", "燃气", "水务", "高速",
        "交通运输", "bank", "insurance", "broker", "real estate", "utility", "power",
    ),
}

PEG_STYLE_RANGES: dict[str, tuple[float, float, float]] = {
    "growth": (0.8, 1.2, 1.5),
    "stable": (0.7, 1.0, 1.2),
    "value": (0.5, 0.8, 1.0),
    "general": (0.8, 1.3, 1.8),
}

VALUATION_STYLE_LABELS = {
    "growth": "成长股",
    "stable": "稳定增长股",
    "cyclical": "周期股",
    "value": "低速价值股",
    "general": "通用类型",
}

DEFENSIVE_INDUSTRY_KEYWORDS = (
    "医药", "公用事业", "电力", "燃气", "水务", "银行", "保险", "煤炭", "石油", "黄金",
    "pharma", "utility", "power", "bank", "insurance", "coal", "oil", "gold",
)
FINANCIAL_INDUSTRY_KEYWORDS = ("银行", "保险", "证券", "bank", "insurance", "broker")
MACRO_POLICY_RISK_KEYWORDS = (
    "加息", "流动性收紧", "监管趋严", "制裁", "关税", "贸易摩擦", "地缘", "战争", "冲突",
    "通胀", "CPI超预期", "PPI超预期", "PMI下行", "社融低于预期", "人民币贬值", "汇率贬值",
    "集采", "限产", "补贴退坡", "产能过剩", "反垄断", "价格管制", "黑天鹅",
)
MACRO_POLICY_SUPPORT_KEYWORDS = (
    "降准", "降息", "LPR下调", "MLF加量", "流动性投放", "稳增长", "扩内需", "财政刺激",
    "设备更新", "以旧换新", "消费券", "新质生产力", "政策支持", "产业扶持", "减税降费",
)
MACRO_POLICY_CRITICAL_KEYWORDS = ("战争", "冲突", "制裁", "黑天鹅", "系统性风险", "金融风险")


def classify_valuation_style(industry_name: str | None) -> str:
    """Map industry text into the PEG categories from stock.txt."""
    text = str(industry_name or "").strip()
    if not text:
        return "general"
    text_lower = text.lower()
    for style in ("cyclical", "value", "growth", "stable"):
        if any(keyword.lower() in text_lower for keyword in VALUATION_STYLE_KEYWORDS[style]):
            return style
    return "general"


def is_defensive_industry(industry_name: str | None) -> bool:
    text = str(industry_name or "").lower()
    return any(keyword.lower() in text for keyword in DEFENSIVE_INDUSTRY_KEYWORDS)


def is_financial_industry(industry_name: str | None) -> bool:
    text = str(industry_name or "").lower()
    return any(keyword.lower() in text for keyword in FINANCIAL_INDUSTRY_KEYWORDS)


def classify_market_style_context(index_metrics: dict[str, dict[str, Any]] | None) -> dict[str, Any]:
    """Classify market regime/style from HS300 and ChiNext trend metrics."""
    metrics = index_metrics or {}
    hs300 = metrics.get("000300") or metrics.get("hs300") or {}
    chinext = metrics.get("399006") or metrics.get("chinext") or {}
    hs_close = _safe_number(hs300.get("close"), 0.0)
    hs_ma20 = _safe_number(hs300.get("ma20"), 0.0)
    hs_ma60 = _safe_number(hs300.get("ma60"), 0.0)
    hs_pct20 = _safe_number(hs300.get("pct_20"), 0.0)
    cy_pct20 = _safe_number(chinext.get("pct_20"), 0.0)
    growth_relative = cy_pct20 - hs_pct20

    if hs_close > 0 and hs_ma20 > 0 and hs_ma60 > 0 and hs_close > hs_ma20 > hs_ma60 and hs_pct20 >= 0:
        regime = "BULL"
    elif hs_close > 0 and hs_ma20 > 0 and hs_ma60 > 0 and hs_close < hs_ma20 < hs_ma60 and hs_pct20 <= 0:
        regime = "BEAR"
    else:
        regime = "RANGE"

    if regime == "BEAR":
        bias = "defensive"
        style = "bear_defensive"
    elif growth_relative >= 3.0:
        bias = "growth"
        style = "bull_growth" if regime == "BULL" else "range_growth"
    elif growth_relative <= -3.0:
        bias = "large_value"
        style = "bull_large_value" if regime == "BULL" else "range_large_value"
    else:
        bias = "balanced"
        style = "bull_balanced" if regime == "BULL" else "range_balanced"

    confidence = "LOW" if not hs_close or not hs_ma20 else ("HIGH" if hs_ma60 else "MEDIUM")
    reason = (
        f"沪深300 close/MA20/MA60={hs_close:.2f}/{hs_ma20:.2f}/{hs_ma60:.2f}，"
        f"20日{hs_pct20:.1f}%；创业板相对沪深300{growth_relative:.1f}%，"
        f"风格={style}"
    )
    return {
        "market_regime": regime,
        "market_style": style,
        "style_bias": bias,
        "style_confidence": confidence,
        "style_growth_allowed": regime != "BEAR",
        "hs300_pct_20": round(hs_pct20, 2),
        "chinext_pct_20": round(cy_pct20, 2),
        "growth_relative_strength": round(growth_relative, 2),
        "market_style_reason": reason,
    }


def classify_north_flow_context(rows: Any) -> dict[str, Any]:
    """Summarize recent northbound flow pressure for market-level scoring."""
    if rows is None:
        df = pd.DataFrame()
    elif isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        df = pd.DataFrame(rows)

    if df.empty or "net_tgt" not in df.columns:
        return {
            "north_net_1d": 0.0,
            "north_net_3d": 0.0,
            "north_net_5d": 0.0,
            "north_flow_status": "UNKNOWN",
            "north_flow_trade_date": "",
            "north_flow_reason": "北向资金数据不足",
        }

    if "trade_date" not in df.columns:
        df["trade_date"] = ""
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["net_tgt"] = pd.to_numeric(df["net_tgt"], errors="coerce").fillna(0.0)
    df = df.sort_values("trade_date").tail(5)
    if df.empty:
        return {
            "north_net_1d": 0.0,
            "north_net_3d": 0.0,
            "north_net_5d": 0.0,
            "north_flow_status": "UNKNOWN",
            "north_flow_trade_date": "",
            "north_flow_reason": "北向资金数据不足",
        }

    latest = float(df["net_tgt"].iloc[-1])
    net_3d = float(df["net_tgt"].tail(3).sum())
    net_5d = float(df["net_tgt"].tail(5).sum())
    latest_date = df["trade_date"].iloc[-1]
    trade_date = "" if pd.isna(latest_date) else str(latest_date.date())

    if net_3d >= 3_000_000_000 or latest >= 1_500_000_000:
        status = "INFLOW"
    elif net_3d <= -3_000_000_000 or latest <= -1_500_000_000:
        status = "OUTFLOW"
    else:
        status = "NEUTRAL"
    reason = (
        f"北向资金1日{latest/1e8:.2f}亿，3日{net_3d/1e8:.2f}亿，"
        f"5日{net_5d/1e8:.2f}亿，状态{status}"
    )
    return {
        "north_net_1d": round(latest, 2),
        "north_net_3d": round(net_3d, 2),
        "north_net_5d": round(net_5d, 2),
        "north_flow_status": status,
        "north_flow_trade_date": trade_date,
        "north_flow_reason": reason,
    }

def classify_etf_flow_context(rows: Any) -> dict[str, Any]:
    """Classify market-level ETF flow as a risk appetite signal."""
    if rows is None:
        df = pd.DataFrame()
    elif isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        df = pd.DataFrame(rows)
    if df.empty:
        return {
            "etf_net_1d": 0.0,
            "etf_net_3d": 0.0,
            "etf_net_5d": 0.0,
            "etf_flow_status": "UNKNOWN",
            "etf_flow_trade_date": "",
            "etf_flow_score": 50.0,
            "etf_flow_reason": "ETF flow data unavailable",
        }
    if "trade_date" not in df.columns:
        df["trade_date"] = ""
    if "net_amount" not in df.columns:
        df["net_amount"] = 0.0
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0.0)
    df = df.sort_values("trade_date").tail(5)
    if df.empty:
        return classify_etf_flow_context(None)
    latest = float(df["net_amount"].iloc[-1])
    net_3d = float(df["net_amount"].tail(3).sum())
    net_5d = float(df["net_amount"].tail(5).sum())
    if net_3d <= ETF_FLOW_PRESSURE_AMOUNT_3D:
        status = "OUTFLOW"
    elif net_3d >= ETF_FLOW_SUPPORT_AMOUNT_3D:
        status = "INFLOW"
    else:
        status = "NEUTRAL"
    latest_date = df["trade_date"].iloc[-1]
    trade_date = "" if pd.isna(latest_date) else str(latest_date.date())
    score = clamp_score(50.0 + max(min(net_3d / 100_000_000.0, 30.0), -30.0) * 0.8)
    return {
        "etf_net_1d": round(latest, 2),
        "etf_net_3d": round(net_3d, 2),
        "etf_net_5d": round(net_5d, 2),
        "etf_flow_status": status,
        "etf_flow_trade_date": trade_date,
        "etf_flow_score": score,
        "etf_flow_reason": f"ETF flow 1d={latest/1e8:.2f}e8, 3d={net_3d/1e8:.2f}e8, 5d={net_5d/1e8:.2f}e8",
    }


def classify_retail_sentiment_context(rows: Any) -> dict[str, Any]:
    """Classify retail bullish/bearish survey extremes as a contrarian signal."""
    if rows is None:
        df = pd.DataFrame()
    elif isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        df = pd.DataFrame(rows)

    empty = {
        "retail_bullish_pct": 0.0,
        "retail_bearish_pct": 0.0,
        "retail_sentiment_trade_date": "",
        "retail_sentiment_status": "UNKNOWN",
        "retail_sentiment_score": 50.0,
        "retail_sentiment_reason": "retail bullish/bearish sentiment data unavailable",
        "retail_sentiment_sample_size": 0.0,
    }
    if df.empty:
        return empty
    if "trade_date" not in df.columns:
        df["trade_date"] = ""
    if "bullish_pct" not in df.columns:
        df["bullish_pct"] = np.nan
    if "bearish_pct" not in df.columns:
        df["bearish_pct"] = np.nan
    if "sample_size" not in df.columns:
        df["sample_size"] = 0.0

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["bullish_pct"] = pd.to_numeric(df["bullish_pct"], errors="coerce")
    df["bearish_pct"] = pd.to_numeric(df["bearish_pct"], errors="coerce")
    df["sample_size"] = pd.to_numeric(df["sample_size"], errors="coerce").fillna(0.0)
    df = df.sort_values("trade_date").tail(5)
    if df.empty:
        return empty

    latest = df.iloc[-1]
    bullish = float(latest.get("bullish_pct") or 0.0)
    bearish = float(latest.get("bearish_pct") or 0.0)
    if 0 < bullish <= 1.0:
        bullish *= 100.0
    if 0 < bearish <= 1.0:
        bearish *= 100.0
    if bullish > 0 and bearish <= 0 and bullish <= 100.0:
        bearish = max(0.0, 100.0 - bullish)
    if bearish > 0 and bullish <= 0 and bearish <= 100.0:
        bullish = max(0.0, 100.0 - bearish)
    bullish = clamp_score(bullish)
    bearish = clamp_score(bearish)

    if bullish >= RETAIL_BULLISH_EXTREME_PCT and bullish - bearish >= 20.0:
        status = "EXTREME_BULLISH"
        score = 36.0
    elif bearish >= RETAIL_BEARISH_EXTREME_PCT and bearish - bullish >= 20.0:
        status = "EXTREME_BEARISH"
        score = 62.0
    elif max(bullish, bearish) >= 65.0:
        status = "ELEVATED"
        score = 46.0 if bullish > bearish else 55.0
    else:
        status = "NEUTRAL"
        score = 50.0

    latest_date = latest.get("trade_date")
    trade_date = "" if pd.isna(latest_date) else str(latest_date.date())
    sample_size = _safe_number(latest.get("sample_size"), 0.0)
    return {
        "retail_bullish_pct": round(bullish, 2),
        "retail_bearish_pct": round(bearish, 2),
        "retail_sentiment_trade_date": trade_date,
        "retail_sentiment_status": status,
        "retail_sentiment_score": clamp_score(score),
        "retail_sentiment_reason": (
            f"retail bullish={bullish:.1f}%, bearish={bearish:.1f}%, sample={sample_size:.0f}, status={status}"
        ),
        "retail_sentiment_sample_size": round(sample_size, 2),
    }


def classify_macro_policy_context(rows: Any) -> dict[str, Any]:
    """Classify market-level macro/policy support and pressure from recent news."""
    if rows is None:
        df = pd.DataFrame()
    elif isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        df = pd.DataFrame(rows)
    if df.empty:
        return {
            "macro_policy_status": "UNKNOWN",
            "macro_policy_score": 50.0,
            "macro_policy_risk_count": 0.0,
            "macro_policy_support_count": 0.0,
            "macro_policy_critical_count": 0.0,
            "macro_policy_latest_title": "",
            "macro_policy_reason": "宏观政策新闻数据不足",
        }

    risk_count = 0
    support_count = 0
    critical_count = 0
    latest_title = ""
    latest_time = ""
    for row in df.to_dict(orient="records"):
        title = str(row.get("title") or "")
        content = str(row.get("content") or "")
        text_value = f"{title} {content}"[:1000]
        risk_hits = sum(1 for keyword in MACRO_POLICY_RISK_KEYWORDS if keyword and keyword in text_value)
        support_hits = sum(1 for keyword in MACRO_POLICY_SUPPORT_KEYWORDS if keyword and keyword in text_value)
        critical_hits = sum(1 for keyword in MACRO_POLICY_CRITICAL_KEYWORDS if keyword and keyword in text_value)
        if risk_hits:
            risk_count += min(risk_hits, 3)
        if support_hits:
            support_count += min(support_hits, 3)
        if critical_hits:
            critical_count += min(critical_hits, 2)
        publish_time = str(row.get("publish_time") or "")
        if title and (not latest_time or publish_time > latest_time):
            latest_time = publish_time
            latest_title = title[:80]

    if critical_count > 0 or risk_count >= max(3, support_count + 2):
        status = "RISK"
    elif support_count >= max(2, risk_count + 1):
        status = "SUPPORT"
    else:
        status = "NEUTRAL"
    score = clamp_score(50.0 + support_count * 4.0 - risk_count * 5.0 - critical_count * 8.0)
    reason = (
        f"近3日宏观/政策支持词{support_count}，压力词{risk_count}，"
        f"黑天鹅词{critical_count}，状态{status}"
    )
    return {
        "macro_policy_status": status,
        "macro_policy_score": round(score, 1),
        "macro_policy_risk_count": float(risk_count),
        "macro_policy_support_count": float(support_count),
        "macro_policy_critical_count": float(critical_count),
        "macro_policy_latest_title": latest_title,
        "macro_policy_reason": reason,
    }


def classify_macro_indicator_context(rows: Any) -> dict[str, Any]:
    """Classify structured macro data such as PMI, CPI, PPI, GDP, social finance and FX."""
    if rows is None:
        data = []
    elif isinstance(rows, pd.DataFrame):
        data = rows.to_dict(orient="records")
    else:
        data = list(rows or [])
    if not data:
        return {
            "macro_indicator_status": "UNKNOWN",
            "macro_indicator_score": 50.0,
            "macro_indicator_risk_count": 0.0,
            "macro_indicator_support_count": 0.0,
            "macro_indicator_latest_name": "",
            "macro_indicator_latest_period": "",
            "macro_cycle": "UNKNOWN",
            "macro_cycle_reason": "structured macro indicators unavailable",
            "macro_indicator_reason": "structured macro indicators unavailable",
        }

    support = 0
    risk = 0
    latest_name = ""
    latest_period = ""
    notes: list[str] = []
    macro_values = {"pmi": np.nan, "gdp": np.nan, "cpi": np.nan, "ppi": np.nan}
    liquidity_signal = 0
    for row in data[:80]:
        name = str(row.get("indicator_name") or row.get("name") or row.get("indicator") or "").upper()
        period = str(row.get("period_date") or row.get("publish_date") or row.get("trade_date") or row.get("date") or "")[:10]
        value = _safe_number(row.get("value"), np.nan)
        yoy = _safe_number(row.get("yoy"), np.nan)
        mom = _safe_number(row.get("mom"), np.nan)
        expected = _safe_number(row.get("expected_value"), np.nan)
        previous = _safe_number(row.get("previous_value"), np.nan)
        if not latest_period or period > latest_period:
            latest_period = period
            latest_name = name
        def _beat(metric: float, margin: float = 0.0) -> bool:
            if not math.isnan(expected):
                return metric >= expected + margin
            if not math.isnan(previous):
                return metric >= previous + margin
            return False
        def _miss(metric: float, margin: float = 0.0) -> bool:
            if not math.isnan(expected):
                return metric <= expected - margin
            if not math.isnan(previous):
                return metric <= previous - margin
            return False

        metric = value if not math.isnan(value) else yoy
        if math.isnan(metric):
            continue
        if "PMI" in name:
            macro_values["pmi"] = metric
            if metric >= 50.5 or _beat(metric, 0.2):
                support += 1
                notes.append(f"PMI {metric:.1f} expansion")
            elif metric < 49.0 or _miss(metric, 0.3):
                risk += 1
                notes.append(f"PMI {metric:.1f} contraction")
        elif "GDP" in name:
            macro_values["gdp"] = metric
            if metric >= 5.0 or _beat(metric, 0.2):
                support += 1
                notes.append(f"GDP {metric:.1f}")
            elif metric < 4.0 or _miss(metric, 0.2):
                risk += 1
                notes.append(f"GDP {metric:.1f} weak")
        elif "CPI" in name:
            macro_values["cpi"] = metric
            if 0.0 <= metric <= 2.8:
                support += 1
                notes.append(f"CPI {metric:.1f} stable")
            elif metric > 3.0 or metric < -0.5:
                risk += 1
                notes.append(f"CPI {metric:.1f} pressure")
        elif "PPI" in name:
            macro_values["ppi"] = metric
            if metric >= 0.0 and not _miss(metric, 0.5):
                support += 1
                notes.append(f"PPI {metric:.1f} improving")
            elif metric <= -3.0 or _miss(metric, 0.8):
                risk += 1
                notes.append(f"PPI {metric:.1f} weak")
        elif "SOCIAL" in name or "TSF" in name or "FINANCING" in name or "M2" in name:
            if _beat(metric, 0.0) or (not math.isnan(yoy) and yoy > 0):
                support += 1
                liquidity_signal += 1
                notes.append(f"{name} liquidity support")
            elif _miss(metric, 0.0) or (not math.isnan(yoy) and yoy < 0):
                risk += 1
                liquidity_signal -= 1
                notes.append(f"{name} liquidity miss")
        elif "FX" in name or "USD" in name or "CNY" in name or "CNH" in name or "EXCHANGE" in name:
            change = mom if not math.isnan(mom) else (metric - previous if not math.isnan(previous) else np.nan)
            if metric >= 7.35 or (not math.isnan(change) and change >= 0.08):
                risk += 1
                notes.append(f"FX {metric:.3f} depreciation pressure")
            elif metric <= 7.10 or (not math.isnan(change) and change <= -0.05):
                support += 1
                notes.append(f"FX {metric:.3f} stable")

    pmi = macro_values["pmi"]
    gdp = macro_values["gdp"]
    cpi = macro_values["cpi"]
    ppi = macro_values["ppi"]
    weak_growth = (not math.isnan(pmi) and pmi < 49.0) or (not math.isnan(gdp) and gdp < 4.0)
    growth_repair = (not math.isnan(pmi) and pmi >= 50.5) or (not math.isnan(gdp) and gdp >= 5.0)
    inflation_pressure = (not math.isnan(cpi) and cpi > 3.0) or (not math.isnan(ppi) and ppi > 5.0)
    deflation_pressure = (not math.isnan(cpi) and cpi < -0.5) or (not math.isnan(ppi) and ppi <= -3.0)
    if weak_growth and inflation_pressure:
        macro_cycle = "STAGFLATION"
    elif weak_growth or (deflation_pressure and liquidity_signal <= 0):
        macro_cycle = "RECESSION"
    elif growth_repair and inflation_pressure:
        macro_cycle = "OVERHEAT"
    elif growth_repair or liquidity_signal > 0:
        macro_cycle = "RECOVERY"
    else:
        macro_cycle = "NEUTRAL"

    score = clamp_score(50.0 + support * 4.0 - risk * 5.0)
    if risk >= max(2, support + 1):
        status = "RISK"
    elif support >= max(2, risk + 1):
        status = "SUPPORT"
    else:
        status = "NEUTRAL"
    reason = f"macro hard data support={support}, risk={risk}"
    if notes:
        reason = f"{reason}; " + "; ".join(notes[:4])
    return {
        "macro_indicator_status": status,
        "macro_indicator_score": score,
        "macro_indicator_risk_count": float(risk),
        "macro_indicator_support_count": float(support),
        "macro_indicator_latest_name": latest_name,
        "macro_indicator_latest_period": latest_period,
        "macro_cycle": macro_cycle,
        "macro_cycle_reason": f"cycle={macro_cycle}, pmi={pmi if not math.isnan(pmi) else '-'}, gdp={gdp if not math.isnan(gdp) else '-'}, cpi={cpi if not math.isnan(cpi) else '-'}, ppi={ppi if not math.isnan(ppi) else '-'}",
        "macro_indicator_reason": reason,
    }


def evaluate_business_purity(row: dict[str, Any]) -> dict[str, Any]:
    """Estimate whether the company's business text matches the selected theme/industry."""
    text_value = " ".join(str(row.get(key) or "") for key in (
        "business_scope", "main_business", "business_desc", "concept_names", "research_verification"
    ))
    industry = str(row.get("industry_name") or row.get("sector_industry_name") or "")
    theme = str(row.get("research_theme_name") or "")
    role = str(row.get("research_theme_role") or "")
    if not text_value.strip():
        return {
            "business_purity_status": "UNKNOWN",
            "business_purity_score": 50.0,
            "business_purity_match_count": 0.0,
            "business_purity_reason": "business description unavailable",
        }
    lowered = text_value.lower()
    tokens: list[str] = []
    for raw in (industry, theme, role):
        raw = str(raw or "").replace("/", " ").replace("|", " ").replace("·", " ")
        for token in re.split(r"[\s,;，；、/]+", raw):
            token = token.strip()
            if len(token) >= 2:
                tokens.append(token)
    theme_keywords = []
    for token in tokens:
        if token.lower() not in theme_keywords:
            theme_keywords.append(token.lower())
    matched = [token for token in theme_keywords if token and token in lowered]
    weak_terms = (
        "trade", "trading", "investment", "property", "real estate", "consulting",
        "multi business", "agency", "leasing", "commodity",
    )
    weak_count = sum(1 for term in weak_terms if term in lowered)
    score = 50.0 + min(len(matched), 5) * 9.0 - weak_count * 8.0
    if len(matched) >= 2:
        status = "PASS"
    elif weak_count >= 2 and not matched:
        status = "RISK"
    else:
        status = "WATCH"
    if clamp_score(score) <= BUSINESS_PURITY_LOW_SCORE:
        status = "RISK"
    reason = f"business matches={len(matched)}, weak_terms={weak_count}"
    if matched:
        reason += "; matched=" + ",".join(matched[:5])
    return {
        "business_purity_status": status,
        "business_purity_score": clamp_score(score),
        "business_purity_match_count": float(len(matched)),
        "business_purity_reason": reason,
    }


def evaluate_industry_prosperity(row: dict[str, Any]) -> dict[str, Any]:
    """Score structured industry prosperity: product prices, utilization, orders/contracts."""
    price_change = _safe_number(row.get("industry_price_change_30d"), 0.0)
    utilization = _ratio_to_pct(row.get("capacity_utilization"), 0.0)
    contract_ratio = _safe_number(row.get("order_contract_to_revenue_pct"), 0.0)
    contract_amount = _safe_number(row.get("order_contract_amount_180d"), 0.0)
    score = 50.0
    support = 0
    risk = 0
    if price_change >= 5.0:
        score += 10.0
        support += 1
    elif price_change <= -5.0:
        score -= 12.0
        risk += 1
    if utilization >= 80.0:
        score += 8.0
        support += 1
    elif 0.0 < utilization < 60.0:
        score -= 10.0
        risk += 1
    if contract_ratio >= 20.0:
        score += 10.0
        support += 1
    elif contract_amount > 0:
        score += 4.0
        support += 1
    status = "PASS" if support >= 2 and score >= 65 else ("RISK" if risk >= 2 or score <= INDUSTRY_PROSPERITY_LOW_SCORE else "WATCH")
    reason = (
        f"price30d={price_change:.1f}%, utilization={utilization:.1f}%, "
        f"contract/revenue={contract_ratio:.1f}%"
    )
    return {
        "industry_prosperity_status": status,
        "industry_prosperity_score": clamp_score(score),
        "industry_prosperity_reason": reason,
        "industry_prosperity_flags": ["industry_prosperity_weak"] if status == "RISK" else [],
    }


def evaluate_institutional_profile(row: dict[str, Any]) -> dict[str, Any]:
    """Score fund/QFII/research-rating/survey evidence."""
    fund_ratio = _ratio_to_pct(row.get("fund_hold_ratio"), 0.0)
    qfii_ratio = _ratio_to_pct(row.get("qfii_hold_ratio"), 0.0)
    rqfii_ratio = _ratio_to_pct(row.get("rqfii_hold_ratio"), 0.0)
    social_security_ratio = _ratio_to_pct(row.get("social_security_hold_ratio"), 0.0)
    private_fund_ratio = _ratio_to_pct(row.get("private_fund_hold_ratio"), 0.0)
    inst_ratio = _ratio_to_pct(row.get("institution_hold_ratio"), 0.0)
    rating_up = _safe_number(row.get("rating_upgrade_count_90d"), 0.0)
    rating_down = _safe_number(row.get("rating_downgrade_count_90d"), 0.0)
    target_upside = _safe_number(row.get("target_price_upside_pct"), 0.0)
    survey_count = _safe_number(row.get("survey_count_90d"), 0.0)
    broker_gold_count = _safe_number(row.get("broker_gold_count_90d"), 0.0)
    score = 50.0
    score += min(fund_ratio + qfii_ratio + rqfii_ratio + social_security_ratio + private_fund_ratio + inst_ratio, 25.0) * 0.8
    score += min(rating_up, 5.0) * 4.0
    score -= min(rating_down, 5.0) * 5.0
    if target_upside >= 20.0:
        score += 8.0
    elif target_upside <= -5.0 and _safe_number(row.get("target_price"), 0.0) > 0:
        score -= 8.0
    score += min(survey_count, 8.0) * 1.2
    score += min(broker_gold_count, 4.0) * 2.0
    if rating_down >= 2 or score <= INSTITUTIONAL_WEAK_SCORE:
        status = "RISK"
    elif score >= 68:
        status = "PASS"
    else:
        status = "WATCH"
    return {
        "institutional_status": status,
        "institutional_score": clamp_score(score),
        "institutional_reason": (
            f"fund={fund_ratio:.1f}%, qfii={qfii_ratio:.1f}%, rqfii={rqfii_ratio:.1f}%, "
            f"social_security={social_security_ratio:.1f}%, private={private_fund_ratio:.1f}%, "
            f"rating_up={rating_up:.0f}, rating_down={rating_down:.0f}, "
            f"target_upside={target_upside:.1f}%, surveys={survey_count:.0f}, gold_pool={broker_gold_count:.0f}"
        ),
        "institutional_flags": ["institutional_profile_weak"] if status == "RISK" else [],
    }


def evaluate_investor_interaction_profile(row: dict[str, Any]) -> dict[str, Any]:
    """Score investor-interaction evidence for demand validation and hidden risks."""
    count = _safe_number(row.get("investor_interaction_count_180d"), 0.0)
    support_count = _safe_number(row.get("investor_interaction_support_count"), 0.0)
    risk_count = _safe_number(row.get("investor_interaction_risk_count"), 0.0)
    latest_text = str(row.get("latest_investor_interaction") or "")
    if count <= 0 and not latest_text:
        return {
            "investor_interaction_status": "UNKNOWN",
            "investor_interaction_score": 50.0,
            "investor_interaction_reason": "investor interaction data unavailable",
            "investor_interaction_flags": [],
        }
    score = 50.0 + min(count, 10.0) * 0.8 + min(support_count, 8.0) * 4.0 - min(risk_count, 8.0) * 6.0
    if risk_count >= 2 and risk_count >= support_count:
        status = "RISK"
    elif support_count >= 2 and score >= 62.0:
        status = "PASS"
    else:
        status = "WATCH"
    if clamp_score(score) <= INTERACTION_RISK_LOW_SCORE:
        status = "RISK"
    return {
        "investor_interaction_status": status,
        "investor_interaction_score": clamp_score(score),
        "investor_interaction_reason": (
            f"interactions={count:.0f}, support={support_count:.0f}, risk={risk_count:.0f}; "
            f"latest={latest_text[:80]}"
        ),
        "investor_interaction_flags": ["investor_interaction_risk"] if status == "RISK" else [],
    }


def evaluate_liquidity_profile(row: dict[str, Any]) -> dict[str, Any]:
    """Apply stock.txt liquidity thresholds using 20-day average amount and turnover."""
    amount = _safe_number(row.get("amount"), 0.0)
    amount_ma20 = _safe_number(row.get("amount_ma20"), amount)
    turnover = _safe_number(row.get("turnover_ratio"), 0.0)
    market_regime = str(row.get("market_regime") or "").upper()
    min_avg_amount = 800_000_000.0 if market_regime == "BEAR" else 500_000_000.0
    hard_floor = 100_000_000.0
    status = "PASS"
    flags: list[str] = []
    if amount_ma20 < hard_floor or amount < hard_floor:
        status = "BLOCK"
        flags.append("liquidity_hard_floor")
    elif amount_ma20 < min_avg_amount:
        status = "WATCH"
        flags.append("liquidity_avg_amount_low")
    if turnover > 0 and not (5.0 <= turnover <= 15.0):
        if status == "PASS":
            status = "WATCH"
        flags.append("turnover_out_of_range")
    score = 72.0
    score += linear_score(amount_ma20, hard_floor, min_avg_amount * 1.4) * 0.18 - 9.0
    if turnover > 0:
        turnover_fit = 100.0 - min(abs(turnover - 10.0) * 10.0, 55.0)
        score = score * 0.76 + turnover_fit * 0.24
    if status == "BLOCK":
        score = min(score, 35.0)
    elif status == "WATCH":
        score = min(score, 62.0)
    reason = (
        f"20日日均成交额{amount_ma20/1e8:.2f}亿，当日{amount/1e8:.2f}亿，"
        f"换手{turnover:.1f}%，阈值{min_avg_amount/1e8:.1f}亿/硬底{hard_floor/1e8:.1f}亿"
    )
    return {
        "liquidity_status": status,
        "liquidity_score": clamp_score(score),
        "liquidity_flags": flags,
        "liquidity_reason": reason,
        "min_avg_amount": min_avg_amount,
    }


def evaluate_order_book_depth(row: dict[str, Any]) -> dict[str, Any]:
    """Evaluate five-level order-book depth from stock.txt liquidity rules."""
    bid_amount = _safe_number(row.get("bid5_amount"), 0.0)
    ask_amount = _safe_number(row.get("ask5_amount"), 0.0)
    depth_amount = bid_amount + ask_amount
    imbalance = bid_amount / ask_amount if ask_amount > 0 else (10.0 if bid_amount > 0 else 0.0)
    flags: list[str] = []
    if depth_amount <= 0:
        return {
            "order_book_status": "UNKNOWN",
            "order_book_score": 60.0,
            "order_book_flags": flags,
            "order_book_reason": "五档盘口数据不足",
            "bid_ask_imbalance": 0.0,
            "min_order_book_depth": 100_000_000.0,
        }

    status = "PASS"
    min_depth = 100_000_000.0
    if depth_amount < min_depth:
        status = "WATCH"
        flags.append("order_book_depth_low")
    if imbalance > 0 and (imbalance < 0.35 or imbalance > 3.0):
        if status == "PASS":
            status = "WATCH"
        flags.append("order_book_imbalance")

    depth_score = linear_score(depth_amount, 20_000_000.0, min_depth * 2.0, default=60.0)
    balance_score = 100.0 - min(abs(math.log(max(imbalance, 0.01))) * 32.0, 65.0)
    score = depth_score * 0.72 + balance_score * 0.28
    if flags:
        score = min(score, 62.0)
    reason = (
        f"五档买盘{bid_amount/1e8:.2f}亿，卖盘{ask_amount/1e8:.2f}亿，"
        f"合计{depth_amount/1e8:.2f}亿，买卖比{imbalance:.2f}"
    )
    return {
        "order_book_status": status,
        "order_book_score": clamp_score(score),
        "order_book_flags": flags,
        "order_book_reason": reason,
        "bid_ask_imbalance": round(imbalance, 2),
        "min_order_book_depth": min_depth,
    }


def evaluate_unlock_pressure(row: dict[str, Any]) -> dict[str, Any]:
    """Classify upcoming unlock pressure by size instead of blocking every unlock."""
    count = _safe_number(row.get("lifting_count_30d"), 0.0)
    amount = _safe_number(row.get("lifting_amount_30d"), 0.0)
    ratio = _safe_number(row.get("lifting_max_ratio_30d"), 0.0)
    effective_cap = _safe_number(row.get("effective_market_cap"), 0.0)
    if effective_cap <= 0:
        effective_cap = _safe_number(row.get("float_market_cap"), 0.0)
    if effective_cap <= 0:
        effective_cap = _safe_number(row.get("market_cap"), 0.0)
    amount_ratio = amount / effective_cap * 100.0 if amount > 0 and effective_cap > 0 else 0.0

    if count <= 0:
        return {
            "unlock_status": "PASS",
            "unlock_flags": [],
            "unlock_amount_ratio_pct": round(amount_ratio, 2),
            "unlock_reason": "未来30日未检测到解禁压力",
        }

    flags: list[str] = []
    if ratio >= MAJOR_UNLOCK_RATIO_PCT or amount_ratio >= MAJOR_UNLOCK_MARKET_CAP_RATIO_PCT:
        status = "BLOCK"
        flags.append("unlock_risk")
    else:
        status = "WATCH"
        flags.append("minor_unlock_watch")

    reason = (
        f"未来30日解禁{count:.0f}笔，最大占比{ratio:.1f}%，"
        f"金额/有效市值{amount_ratio:.1f}%"
    )
    return {
        "unlock_status": status,
        "unlock_flags": flags,
        "unlock_amount_ratio_pct": round(amount_ratio, 2),
        "unlock_reason": reason,
    }


def evaluate_size_liquidity_profile(row: dict[str, Any]) -> dict[str, Any]:
    """Apply stock.txt float-market-cap floor when share data is available."""
    float_cap = _safe_number(row.get("float_market_cap"), 0.0)
    market_cap = _safe_number(row.get("market_cap"), 0.0)
    effective_cap = float_cap if float_cap > 0 else market_cap
    min_float_cap = 5_000_000_000.0
    flags: list[str] = []
    if effective_cap <= 0:
        return {
            "size_liquidity_status": "UNKNOWN",
            "size_liquidity_score": 60.0,
            "size_liquidity_flags": flags,
            "size_liquidity_reason": "市值/流通股本数据不足",
            "effective_market_cap": 0.0,
            "min_float_market_cap": min_float_cap,
        }
    status = "PASS"
    score = linear_score(effective_cap, 2_000_000_000.0, 10_000_000_000.0, default=60.0)
    if effective_cap < min_float_cap:
        status = "WATCH"
        flags.append("float_market_cap_low")
        score = min(score, 58.0)
    reason = (
        f"流通/有效市值{effective_cap/1e8:.2f}亿，"
        f"策略底线{min_float_cap/1e8:.0f}亿"
    )
    return {
        "size_liquidity_status": status,
        "size_liquidity_score": clamp_score(score),
        "size_liquidity_flags": flags,
        "size_liquidity_reason": reason,
        "effective_market_cap": effective_cap,
        "min_float_market_cap": min_float_cap,
    }


def evaluate_volume_temperature_profile(row: dict[str, Any]) -> dict[str, Any]:
    """Detect moderate volume expansion versus blow-off/shrinkage traps."""
    amount_ratio_20 = _safe_number(row.get("amount_ratio_20"), 1.0)
    turnover = _safe_number(row.get("turnover_ratio"), 0.0)
    change_pct = _safe_number(row.get("change_pct"), 0.0)
    pct_5 = _safe_number(row.get("pct_5"), 0.0)
    main_10d = _safe_number(row.get("main_net_inflow_10d"), 0.0)
    flags: list[str] = []
    status = "PASS"
    if amount_ratio_20 >= 3.0 and (turnover >= 15.0 or change_pct < 0.0 or pct_5 >= 15.0):
        status = "RISK"
        flags.append("blowoff_volume_risk")
    elif amount_ratio_20 < 0.6 and change_pct <= 0.0 and main_10d <= 0.0:
        status = "WATCH"
        flags.append("volume_shrink_weak")
    elif not (0.8 <= amount_ratio_20 <= 2.5):
        status = "WATCH"
        flags.append("volume_not_moderate")

    moderate_fit = 100.0 - min(abs(amount_ratio_20 - 1.5) * 35.0, 60.0)
    turnover_fit = 100.0 - min(abs(turnover - 10.0) * 8.0, 55.0) if turnover > 0 else 60.0
    score = moderate_fit * 0.70 + turnover_fit * 0.30
    if "blowoff_volume_risk" in flags:
        score = min(score, 42.0)
    elif flags:
        score = min(score, 62.0)
    reason = (
        f"量能倍数{amount_ratio_20:.2f}，换手{turnover:.1f}%，"
        f"日涨跌{change_pct:.1f}%，状态{status}"
    )
    return {
        "volume_temperature_status": status,
        "volume_temperature_score": clamp_score(score),
        "volume_temperature_flags": flags,
        "volume_temperature_reason": reason,
    }


def evaluate_fundamental_quality(row: dict[str, Any]) -> dict[str, Any]:
    """Apply stock.txt fundamental thresholds as explainable gates."""
    industry = str(row.get("industry_name") or row.get("sector_industry_name") or "")
    style = str(row.get("valuation_style") or classify_valuation_style(industry)).lower()
    market_regime = str(row.get("market_regime") or "").upper()
    eps = _safe_number(row.get("basic_eps"), 0.0)
    roe = _safe_number(row.get("roe_wtd"), 0.0)
    roe_non_gaap = _safe_number(row.get("roe_non_gaap_wtd"), 0.0)
    effective_roe = max(roe, roe_non_gaap)
    roa = _safe_number(row.get("roa_wtd"), 0.0)
    gross_margin = _safe_number(row.get("gross_margin"), 0.0)
    net_margin = _safe_number(row.get("net_margin"), 0.0)
    asset_liab = _safe_number(row.get("asset_liab_ratio"), 0.0)
    quick_ratio = _safe_number(row.get("quick_ratio"), 0.0)
    roic = _safe_number(row.get("roic"), 0.0)
    acct_recv_to_rev = _safe_number(row.get("acct_recv_to_rev"), 0.0)
    prepayment_yoy_gr = _safe_number(row.get("prepayment_yoy_gr"), 0.0)
    related_transaction_to_rev = _safe_number(row.get("related_transaction_to_rev"), 0.0)
    rev_growth = _safe_number(row.get("total_rev_yoy_gr"), 0.0)
    rev_qoq_growth = _safe_number(row.get("total_rev_qoq_gr"), 0.0)
    profit_qoq_growth = _safe_number(row.get("net_profit_qoq_gr"), 0.0)
    profit_growth = max(
        _safe_number(row.get("net_profit_yoy_gr"), 0.0),
        _safe_number(row.get("non_gaap_net_profit_yoy_gr"), 0.0),
    )

    if style == "growth":
        min_profit_growth, min_rev_growth, min_roe, min_gross = 20.0, 20.0, 10.0, 25.0
    elif style == "value":
        min_profit_growth, min_rev_growth, min_roe, min_gross = 10.0, 5.0, 8.0, 0.0
    elif style == "cyclical":
        min_profit_growth, min_rev_growth, min_roe, min_gross = 0.0, 0.0, 6.0, 10.0
    else:
        min_profit_growth, min_rev_growth, min_roe, min_gross = 10.0, 10.0, 8.0, 20.0

    debt_cap = 50.0 if market_regime == "BEAR" else 60.0
    flags: list[str] = []
    score = 70.0
    if eps < 0 or net_margin < -5.0:
        flags.append("fundamental_loss")
        score -= 35.0
    if profit_growth < -20.0 and rev_growth < 0:
        flags.append("performance_deterioration")
        score -= 24.0
    if profit_qoq_growth < -20.0 and rev_qoq_growth < 0:
        flags.append("qoq_performance_drop")
        score -= 18.0
    if style == "growth" and 0 < profit_qoq_growth < 10.0 and profit_growth < min_profit_growth:
        flags.append("profit_momentum_weak")
        score -= 8.0
    if style == "growth" and profit_growth < min_profit_growth and rev_growth < min_rev_growth:
        flags.append("growth_threshold_miss")
        score -= 18.0
    elif style != "growth" and profit_growth < min_profit_growth and rev_growth < min_rev_growth:
        flags.append("fundamental_growth_weak")
        score -= 12.0
    if effective_roe > 0 and effective_roe < min_roe:
        flags.append("roe_below_threshold")
        score -= 10.0
    if roa > 0 and roa < 2.0 and not is_financial_industry(industry):
        flags.append("roa_below_threshold")
        score -= 6.0
    if min_gross > 0 and gross_margin > 0 and gross_margin < min_gross:
        flags.append("gross_margin_below_threshold")
        score -= 8.0
    if asset_liab > debt_cap and not is_financial_industry(industry):
        flags.append("debt_ratio_over_cap")
        score -= 16.0
    if quick_ratio > 0 and quick_ratio < 0.8 and not is_financial_industry(industry):
        flags.append("quick_ratio_low")
        score -= 8.0
    if roic > 0 and roic < 15.0 and not is_financial_industry(industry):
        flags.append("roic_below_threshold")
        score -= 8.0
    if acct_recv_to_rev > 30.0:
        flags.append("receivable_ratio_high")
        score -= 8.0
    if prepayment_yoy_gr > 50.0:
        flags.append("prepayment_growth_high")
        score -= 6.0
    if related_transaction_to_rev > 20.0:
        flags.append("related_transaction_ratio_high")
        score -= 7.0

    status = "PASS"
    if "fundamental_loss" in flags:
        status = "BLOCK"
    elif flags:
        status = "WATCH"
    reason = (
        f"ROE {roe:.1f}%/{min_roe:.1f}%，毛利率{gross_margin:.1f}%/{min_gross:.1f}%，"
        f"营收增速{rev_growth:.1f}%/{min_rev_growth:.1f}%，利润增速{profit_growth:.1f}%/{min_profit_growth:.1f}%，"
        f"资产负债率{asset_liab:.1f}%/{debt_cap:.1f}%"
    )
    reason = (
        f"ROE {effective_roe:.1f}%/{min_roe:.1f}%, ROA {roa:.1f}%, gross margin {gross_margin:.1f}%/{min_gross:.1f}%, "
        f"revenue YoY {rev_growth:.1f}%/{min_rev_growth:.1f}%, profit YoY {profit_growth:.1f}%/{min_profit_growth:.1f}%, "
        f"revenue QoQ {rev_qoq_growth:.1f}%, profit QoQ {profit_qoq_growth:.1f}%, "
        f"quick ratio {quick_ratio:.2f}, ROIC {roic:.1f}%, receivable/revenue {acct_recv_to_rev:.1f}%, "
        f"prepayment YoY {prepayment_yoy_gr:.1f}%, asset-liability {asset_liab:.1f}%/{debt_cap:.1f}%"
    )
    return {
        "fundamental_quality_status": status,
        "fundamental_quality_score": clamp_score(score),
        "fundamental_quality_flags": flags,
        "fundamental_quality_reason": reason,
    }


def parse_dividend_cash_per_share(plan: str | None) -> float:
    """Parse cash dividend per share from common A-share dividend-plan text."""
    text_value = str(plan or "").replace(" ", "")
    if not text_value:
        return 0.0
    patterns = [
        (r"(?:每)?10股(?:派发现金红利|派息|派现|派|现金红利)([0-9]+(?:\.[0-9]+)?)元?", 10.0),
        (r"10派([0-9]+(?:\.[0-9]+)?)", 10.0),
        (r"每股(?:派发现金红利|派息|派现|派|现金红利)([0-9]+(?:\.[0-9]+)?)元?", 1.0),
    ]
    for pattern, divisor in patterns:
        match = re.search(pattern, text_value, flags=re.IGNORECASE)
        if match:
            return round(_safe_number(match.group(1), 0.0) / divisor, 4)
    return 0.0


def build_research_theme_features(themes: list[dict[str, Any]] | None) -> pd.DataFrame:
    """Map research-radar themes into stock-level evidence features."""
    rows: list[dict[str, Any]] = []
    for theme in themes or []:
        theme_score = _safe_number(theme.get("score"), 0.0)
        theme_status = str(theme.get("status") or "")
        if theme_score <= 0:
            theme_score = {
                "财报兑现强": 88.0,
                "产业催化强，财报分化": 80.0,
                "需求明确，个股分化": 76.0,
                "国产替代强，利润弹性不均": 78.0,
            }.get(str(theme.get("evidence_level") or ""), 70.0)
        # The radar keeps weakening themes visible for coverage, but they must
        # not receive the same stock-level bonus as an active positive theme.
        if theme_status == "逻辑转弱":
            theme_score = min(theme_score, 42.0)
        elif theme_status == "常规观察":
            theme_score = min(theme_score, 62.0)
        for stock in theme.get("stocks", []) or []:
            code = str(stock.get("code") or "").strip().zfill(6)
            if not code or code == "000000":
                continue
            tier = str(stock.get("tier") or "")
            tier_bonus = 8.0 if "核心" in tier else (4.0 if "弹性" in tier else 2.0)
            rows.append({
                "stock_code": code,
                "research_theme_score": clamp_score(theme_score + tier_bonus),
                "research_theme_name": str(theme.get("name") or ""),
                "research_theme_id": str(theme.get("id") or ""),
                "research_theme_trend": str(theme.get("trend") or ""),
                "research_evidence_level": str(theme.get("evidence_level") or ""),
                "research_theme_role": str(stock.get("role") or ""),
                "research_theme_tier": tier,
                "research_verification": str(theme.get("verification") or ""),
                "research_risk": str(theme.get("risk") or ""),
            })
    if not rows:
        return pd.DataFrame({"stock_code": []})
    out = pd.DataFrame(rows)
    out = out.sort_values("research_theme_score", ascending=False).drop_duplicates("stock_code", keep="first")
    return out.reset_index(drop=True)


def _score_pe_ratio(pe_ttm: float) -> float:
    if pe_ttm <= 0:
        return 55.0
    return clamp_score(100.0 - linear_score(pe_ttm, 8.0, 80.0) * 0.60)


def _score_pb_ratio(pb_ratio: float) -> float:
    if pb_ratio <= 0:
        return 55.0
    return clamp_score(100.0 - linear_score(pb_ratio, 1.0, 9.0) * 0.70)


def evaluate_peg_valuation(
    pe_ttm: float | None,
    pb_ratio: float | None,
    growth_pct: float | None,
    industry_name: str | None = "",
) -> dict[str, Any]:
    """Score valuation by matching PEG thresholds to industry style."""
    pe = _safe_number(pe_ttm, 0.0)
    pb = _safe_number(pb_ratio, 0.0)
    growth = _safe_number(growth_pct, 0.0)
    style = classify_valuation_style(industry_name)
    label = VALUATION_STYLE_LABELS.get(style, VALUATION_STYLE_LABELS["general"])
    pe_score = _score_pe_ratio(pe)
    pb_score = _score_pb_ratio(pb)
    fallback_score = clamp_score(pb_score * 0.62 + pe_score * 0.38)

    payload: dict[str, Any] = {
        "valuation_style": style,
        "valuation_style_label": label,
        "pe_ttm": round(pe, 2) if pe > 0 else None,
        "pb_ratio": round(pb, 2) if pb > 0 else None,
        "peg_ratio": None,
        "peg_upper": None,
        "valuation_score": fallback_score,
        "valuation_status": "PASS" if fallback_score >= 70 else ("RISK" if fallback_score <= 40 else "WATCH"),
        "valuation_reason": f"{label}: PEG缺失，回退PE/PB；PE={pe:.1f}，PB={pb:.2f}，成长={growth:.1f}%",
    }

    if style == "cyclical":
        payload["valuation_reason"] = (
            f"{label}: PEG不适用，按PE/PB和供需周期观察；PE={pe:.1f}，PB={pb:.2f}，成长={growth:.1f}%"
        )
        return payload

    if pe <= 0 or growth <= 0:
        return payload

    low_bar, fair_bar, high_bar = PEG_STYLE_RANGES.get(style, PEG_STYLE_RANGES["general"])
    peg = pe / growth
    if peg < low_bar:
        peg_score = 86.0
        band = "低估"
    elif peg <= fair_bar:
        peg_score = 78.0
        band = "合理"
    elif peg <= high_bar:
        peg_score = 62.0
        band = "偏贵"
    elif peg <= high_bar * 1.5:
        peg_score = 42.0
        band = "高估"
    else:
        peg_score = 28.0
        band = "严重高估"

    blended = clamp_score(peg_score * 0.62 + pb_score * 0.38)
    payload.update({
        "peg_ratio": round(peg, 2),
        "peg_upper": high_bar,
        "valuation_score": blended,
        "valuation_status": "PASS" if blended >= 70 else ("RISK" if blended <= 40 else "WATCH"),
        "valuation_reason": (
            f"{label}: PEG={peg:.2f}({band})，阈值<{low_bar:.1f}/"
            f"{low_bar:.1f}-{fair_bar:.1f}/>{high_bar:.1f}；PE={pe:.1f}，PB={pb:.2f}，成长={growth:.1f}%"
        ),
    })
    return payload


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

    price_status = str(row.get("price_check_status") or "").upper()
    if price_status == "FAIL":
        flags.append("price_mismatch")
        score -= 35.0
    elif price_status in {"STALE_SOURCE", "MISSING_SOURCE"}:
        flags.append("missing_price_crosscheck")
        score -= 10.0
    elif price_status == "SINGLE_SOURCE":
        flags.append("single_price_source")
        score -= 6.0

    return clamp_score(score), flags


def build_rule_flags(row: dict[str, Any]) -> list[str]:
    """Return hard strategy flags derived from stock.txt constraints."""
    flags: list[str] = []
    code = str(row.get("stock_code") or "").strip().zfill(6)
    if code.startswith(EXCLUDED_RECOMMEND_PREFIXES):
        flags.append("excluded_688")

    close = _safe_number(row.get("close"), 0.0)
    ma5 = _safe_number(row.get("ma5"), 0.0)
    ma10 = _safe_number(row.get("ma10"), 0.0)
    ma20 = _safe_number(row.get("ma20"), 0.0)
    ma60 = _safe_number(row.get("ma60"), 0.0)
    if all(v > 0 for v in (close, ma5, ma10, ma20)):
        if close < ma20 and ma5 < ma10 < ma20 and (ma60 <= 0 or ma20 < ma60 or close < ma60):
            flags.append("downtrend_clock")

    oper_cf_ps = row.get("oper_cf_ps")
    if oper_cf_ps is not None and not pd.isna(oper_cf_ps) and _safe_number(oper_cf_ps, 0.0) < 0:
        flags.append("negative_oper_cash_flow")

    cash_flow_ratio = row.get("cash_flow_ratio")
    if cash_flow_ratio is not None and not pd.isna(cash_flow_ratio) and _safe_number(cash_flow_ratio, 0.0) < 0:
        flags.append("negative_cash_flow_ratio")

    free_cash_flow = row.get("free_cash_flow", row.get("fcf"))
    if free_cash_flow is not None and not pd.isna(free_cash_flow) and _safe_number(free_cash_flow, 0.0) < 0:
        flags.append("negative_free_cash_flow")

    ebit_margin = row.get("ebit_margin")
    if ebit_margin is not None and not pd.isna(ebit_margin) and _safe_number(ebit_margin, 0.0) < 0:
        flags.append("negative_ebit_margin")

    if _safe_number(row.get("main_outflow_days_3d"), 0.0) >= 3:
        flags.append("main_outflow_3d")

    if _safe_number(row.get("main_outflow_days_10d"), 0.0) >= 7 and _safe_number(row.get("main_net_inflow_10d"), 0.0) < 0:
        flags.append("main_outflow_10d")

    if str(row.get("sector_gate_status") or "").upper() == "BLOCK":
        flags.append("weak_sector")

    if 0.0 < _safe_number(row.get("theme_continuity_score_10"), 5.5) <= 5.0:
        flags.append("theme_continuity_low")

    if classify_event_fulfillment(row).get("event_fulfillment_status") == "PRICED_IN":
        flags.append("positive_event_priced_in")

    if _safe_number(row.get("pct_5"), 0.0) >= 20.0:
        flags.append("weekly_overheat")

    if _safe_number(row.get("holder_num_ratio"), 0.0) >= 10.0:
        flags.append("holder_spread")

    if _safe_number(row.get("lhb_inst_count_20d"), 0.0) > 0 and _safe_number(row.get("lhb_inst_net_amount_20d"), 0.0) <= -100_000_000.0:
        flags.append("institutional_lhb_outflow")

    if (
        _safe_number(row.get("margin_contracting_days_3d"), 0.0) >= 3.0
        and _safe_number(row.get("margin_balance_delta_3d"), 0.0) <= -50_000_000.0
    ):
        flags.append("margin_deleveraging_3d")

    unlock_pressure = evaluate_unlock_pressure(row)
    flags.extend(unlock_pressure.get("unlock_flags", []))

    if _safe_number(row.get("pledge_ratio"), 0.0) >= PLEDGE_RATIO_CAP_PCT:
        flags.append("pledge_ratio_high")

    if _safe_number(row.get("reduction_max_ratio_90d"), 0.0) >= SHAREHOLDER_REDUCTION_RATIO_CAP_PCT:
        flags.append("shareholder_reduction_high")

    goodwill_ratio = _safe_number(row.get("goodwill_to_net_asset_pct"), 0.0)
    if goodwill_ratio >= GOODWILL_RATIO_HIGH_PCT:
        flags.append("goodwill_ratio_high")
    elif goodwill_ratio >= GOODWILL_RATIO_WATCH_PCT:
        flags.append("goodwill_ratio_watch")

    if _safe_number(row.get("mine_clearance_score"), 0.0) >= 70.0:
        flags.append("mine_clearance_risk")

    if str(row.get("market_extreme_status") or "").upper() == "OVERHEAT" and (
        _safe_number(row.get("dist_ma20"), 0.0) >= 12.0 or _safe_number(row.get("pct_5"), 0.0) >= 12.0
    ):
        flags.append("market_extreme_overheat")

    if str(row.get("external_market_status") or "").upper() == "RISK":
        flags.append("external_market_risk")

    if str(row.get("kline_pattern_direction") or "").lower() in {"bearish", "risk"} and (
        _safe_number(row.get("dist_ma20"), 0.0) >= 8.0 or _safe_number(row.get("pct_5"), 0.0) >= 12.0
    ):
        flags.append("bearish_kline_pattern")

    valuation_score = _safe_number(row.get("valuation_score"), 55.0)
    peg_ratio = _safe_number(row.get("peg_ratio"), 0.0)
    peg_upper = _safe_number(row.get("peg_upper"), 0.0)
    pe_ttm = _safe_number(row.get("pe_ttm"), 0.0)
    pb_ratio = _safe_number(row.get("pb_ratio"), 0.0)
    if valuation_score <= 35.0 and (
        (peg_ratio > 0 and peg_upper > 0 and peg_ratio >= peg_upper * 1.3)
        or (pe_ttm >= 90.0 and pb_ratio >= 8.0)
    ):
        flags.append("valuation_overpriced")

    if _safe_number(row.get("pe_industry_multiple"), 0.0) >= 1.8 and valuation_score <= 45.0:
        flags.append("industry_relative_overvalued")

    if _safe_number(row.get("ps_industry_multiple"), 0.0) >= 2.5 and valuation_score <= 45.0:
        flags.append("industry_relative_ps_overvalued")

    if _safe_number(row.get("valuation_history_percentile_250d"), 0.0) >= 85.0 and valuation_score <= 45.0:
        flags.append("valuation_history_percentile_high")

    market_regime = str(row.get("market_regime") or "").upper()
    valuation_style = str(row.get("valuation_style") or "").lower()
    industry_for_style = str(row.get("industry_name") or row.get("sector_industry_name") or "")
    if market_regime == "BEAR" and valuation_style == "growth" and not is_defensive_industry(industry_for_style):
        flags.append("bear_market_growth_pause")

    if market_regime == "BEAR" and _safe_number(row.get("north_net_3d"), 0.0) <= -5_000_000_000.0:
        flags.append("north_flow_pressure")

    if market_regime == "BEAR" and _safe_number(row.get("etf_net_3d"), 0.0) <= ETF_FLOW_PRESSURE_AMOUNT_3D:
        flags.append("etf_flow_pressure")

    if str(row.get("macro_policy_status") or "").upper() == "RISK" and (
        market_regime == "BEAR" or str(row.get("market_extreme_status") or "").upper() == "OVERHEAT"
    ):
        flags.append("macro_policy_pressure")

    if str(row.get("macro_indicator_status") or "").upper() == "RISK" and (
        market_regime == "BEAR" or str(row.get("market_extreme_status") or "").upper() == "OVERHEAT"
    ):
        flags.append("macro_indicator_pressure")

    if (
        _safe_number(row.get("north_holding_ratio"), 0.0) > 0
        and _safe_number(row.get("north_holding_ratio"), 0.0) < NORTH_STOCK_HOLDING_MIN_RATIO_PCT
        and _safe_number(row.get("north_net_buy_amount_3d"), 0.0) <= 0
    ):
        flags.append("north_stock_underweight")

    if (
        _safe_number(row.get("north_holding_ratio_delta_3d"), 0.0) <= NORTH_STOCK_REDUCTION_DELTA_PCT
        or _safe_number(row.get("north_net_buy_amount_3d"), 0.0) <= -50_000_000.0
    ):
        flags.append("north_stock_outflow")

    if str(row.get("institutional_status") or "").upper() == "RISK":
        flags.append("institutional_profile_weak")

    if str(row.get("investor_interaction_status") or "").upper() == "RISK":
        flags.append("investor_interaction_risk")

    if str(row.get("retail_sentiment_status") or "").upper() == "EXTREME_BULLISH" and (
        str(row.get("institutional_status") or "").upper() == "RISK"
        or str(row.get("north_flow_status") or "").upper() == "OUTFLOW"
        or str(row.get("north_stock_status") or "").upper() == "RISK"
        or _safe_number(row.get("north_net_3d"), 0.0) <= -3_000_000_000.0
        or _safe_number(row.get("north_net_buy_amount_3d"), 0.0) <= -50_000_000.0
    ):
        flags.append("retail_institution_contrarian_risk")

    if str(row.get("business_purity_status") or "").upper() == "RISK" or (
        _safe_number(row.get("business_purity_score"), 50.0) <= BUSINESS_PURITY_LOW_SCORE
    ):
        flags.append("business_purity_low")

    prosperity = evaluate_industry_prosperity(row)
    flags.extend(prosperity.get("industry_prosperity_flags", []))

    if str(row.get("classic_pattern_direction") or "").lower() in {"bearish", "risk"} and (
        str(row.get("classic_pattern_status") or "").upper() == "CONFIRMED"
        or _safe_number(row.get("dist_ma20"), 0.0) >= 8.0
    ):
        flags.append("classic_top_breakdown")

    if _safe_number(row.get("relative_hs300_20"), 0.0) <= -10.0 and _safe_number(row.get("pct_20"), 0.0) < 0:
        flags.append("market_relative_weak")

    liquidity = evaluate_liquidity_profile(row)
    flags.extend(liquidity.get("liquidity_flags", []))
    order_book_depth = evaluate_order_book_depth(row)
    flags.extend(order_book_depth.get("order_book_flags", []))
    size_liquidity = evaluate_size_liquidity_profile(row)
    flags.extend(size_liquidity.get("size_liquidity_flags", []))
    volume_temperature = evaluate_volume_temperature_profile(row)
    flags.extend(volume_temperature.get("volume_temperature_flags", []))
    fundamental_quality = evaluate_fundamental_quality(row)
    flags.extend(fundamental_quality.get("fundamental_quality_flags", []))

    return flags


_CHASE_GATE_UNSET = object()


def _normalized_chase_gate_status(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip().upper()


def _is_explicit_true(value: Any) -> bool:
    return value is True or (isinstance(value, np.bool_) and bool(value))


def _safe_text_value(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        missing = pd.isna(value)
        if isinstance(missing, (bool, np.bool_)) and bool(missing):
            return default
    except (TypeError, ValueError):
        pass
    rendered = str(value).strip()
    return rendered or default


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
    chase_risk_status: Any = _CHASE_GATE_UNSET,
    ordinary_buy_eligible: Any = _CHASE_GATE_UNSET,
    chase_risk_reason: str | None = None,
) -> tuple[str, str]:
    """Gate recommendation eligibility. It is intentionally conservative."""
    name = short_name or ""
    amount = float(amount or 0)
    change_pct = float(change_pct or 0)
    code = str(stock_code).zfill(6)
    flags = set(data_quality_flags or [])

    # Pipeline callers supply this gate.  Once supplied, missing/unknown values
    # are not allowed to fall through to a high alpha score.
    chase_gate_supplied = (
        chase_risk_status is not _CHASE_GATE_UNSET
        or ordinary_buy_eligible is not _CHASE_GATE_UNSET
    )
    if chase_gate_supplied:
        chase_status = _normalized_chase_gate_status(chase_risk_status)
        chase_reason = _safe_text_value(chase_risk_reason)
        if chase_status in {"EXECUTION_BLOCKED", "DATA_BLOCKED"}:
            return "BLOCK", chase_reason or f"追高/可交易门禁为 {chase_status}"
        if chase_status in {"WATCH", "CONDITIONAL"}:
            return "SUSPENDED", chase_reason or f"追高风险门禁为 {chase_status}"
        if chase_status != "ALLOW" or not _is_explicit_true(ordinary_buy_eligible):
            return "BLOCK", chase_reason or "追高/可交易资格缺失或未经明确验证"

    if "ST" in name.upper() or "退" in name:
        return "BLOCK", "ST或退市风险标的，不进入推荐池"
    if not code.startswith(("0", "3", "6")):
        return "BLOCK", "非沪深A股主代码，不进入推荐池"
    if code.startswith(EXCLUDED_RECOMMEND_PREFIXES) or "excluded_688" in flags:
        return "BLOCK", "688开头科创板标的按策略要求过滤"
    if event_risk_level == "CRITICAL":
        return "BLOCK", "公告存在重大事件风险"
    if "negative_oper_cash_flow" in flags or "negative_cash_flow_ratio" in flags:
        return "BLOCK", "经营现金流为负，触发现金流底线"
    if "negative_free_cash_flow" in flags:
        return "BLOCK", "自由现金流为负，触发现金流底线"
    if "negative_ebit_margin" in flags:
        return "BLOCK", "EBIT利润率为负，触发盈利质量底线"
    if "mine_clearance_risk" in flags:
        return "BLOCK", "扫雷数据提示财务或监管异常风险"
    if "unlock_risk" in flags:
        return "BLOCK", "未来30日存在大额解禁压力，按策略要求剔除"
    if "minor_unlock_watch" in flags:
        return "SUSPENDED", "未来30日存在小额解禁压力，先等待抛压确认"
    if "pledge_ratio_high" in flags:
        return "SUSPENDED", "大股东质押比例超过50%，存在平仓或治理风险"
    if "shareholder_reduction_high" in flags:
        return "SUSPENDED", "近3个月单一股东减持比例超过2%，筹码供给压力偏高"
    if "goodwill_ratio_high" in flags:
        return "SUSPENDED", "商誉占净资产比例超过30%，存在减值风险"
    if "goodwill_ratio_watch" in flags:
        return "SUSPENDED", "商誉占净资产比例超过20%，财务安全边际不足"
    if "main_outflow_3d" in flags:
        return "BLOCK", "主力资金连续3日净流出"
    if "main_outflow_10d" in flags:
        return "SUSPENDED", "近10日主力资金持续净流出，资金承接不足"
    if "downtrend_clock" in flags:
        return "BLOCK", "日线处于4-6点钟下降趋势"
    if "weak_sector" in flags:
        return "SUSPENDED", "所属板块资金或延续性不合格，先不进入执行候选"
    if "theme_continuity_low" in flags:
        return "SUSPENDED", "题材延续性评分低于6分观察线，先等待主线确认"
    if "weekly_overheat" in flags:
        return "SUSPENDED", "近一周涨幅超过20%，按短线策略避免追高"
    if "positive_event_priced_in" in flags:
        return "SUSPENDED", "正向事件已被短线涨幅或乖离消化，按利好兑现处理"
    if "holder_spread" in flags:
        return "SUSPENDED", "股东人数明显增加，筹码集中度下降"
    if "institutional_lhb_outflow" in flags:
        return "SUSPENDED", "近20日龙虎榜机构席位大额净卖出，等待资金承接修复"
    if "margin_deleveraging_3d" in flags:
        return "SUSPENDED", "近3日两融余额连续收缩且金额较大，杠杆资金承接偏弱"
    if "market_extreme_overheat" in flags:
        return "SUSPENDED", "全市场宽度过热且个股已扩张，等待回调"
    if "bearish_kline_pattern" in flags:
        return "SUSPENDED", "K线出现高位看空形态，等待量价修复"
    if "valuation_overpriced" in flags:
        return "SUSPENDED", "PEG/PE/PB估值明显过热，等待业绩消化或价格回落"
    if "industry_relative_overvalued" in flags:
        return "SUSPENDED", "PE(TTM)显著高于行业中位数，等待估值回落或业绩确认"
    if "industry_relative_ps_overvalued" in flags:
        return "SUSPENDED", "PS显著高于行业中位数，等待营收兑现或估值回落"
    if "valuation_history_percentile_high" in flags:
        return "SUSPENDED", "250日估值分位处于高位，等待估值拥挤度回落"
    if "bear_market_growth_pause" in flags:
        return "SUSPENDED", "沪深300空头环境下暂停非防御成长股筛选"
    if "north_flow_pressure" in flags:
        return "SUSPENDED", "熊市叠加北向资金连续净流出，先降低开仓优先级"
    if "etf_flow_pressure" in flags:
        return "SUSPENDED", "bear market with ETF flow outflow; wait for market risk appetite repair"
    if "macro_policy_pressure" in flags:
        return "SUSPENDED", "宏观/政策压力偏高且市场环境不友好，先进入观察池"
    if "macro_indicator_pressure" in flags:
        return "SUSPENDED", "PMI/CPI/PPI/GDP/TSF/FX macro data is under pressure; downgrade to watch"
    if "north_stock_outflow" in flags:
        return "SUSPENDED", "stock-level northbound holding or net buying is weakening; wait for repair"
    if "north_stock_underweight" in flags:
        return "SUSPENDED", "stock-level northbound holding is below strategy floor and not increasing"
    if "institutional_profile_weak" in flags:
        return "SUSPENDED", "institutional holding/rating/survey evidence is weak or downgraded"
    if "investor_interaction_risk" in flags:
        return "SUSPENDED", "investor interaction evidence contains concentrated risk signals"
    if "retail_institution_contrarian_risk" in flags:
        return "SUSPENDED", "retail sentiment is extremely bullish while institutional/northbound evidence is weakening"
    if "business_purity_low" in flags:
        return "SUSPENDED", "business purity is low versus theme/industry; concept validation is insufficient"
    if "industry_prosperity_weak" in flags:
        return "SUSPENDED", "industry prosperity data is weak; wait for price/order/utilization repair"
    if "classic_top_breakdown" in flags:
        return "SUSPENDED", "classic top/bottom structure shows bearish breakdown risk"
    if "market_relative_weak" in flags:
        return "SUSPENDED", "20日明显跑输沪深300且自身收益为负，等待相对强度修复"
    if "float_market_cap_low" in flags:
        return "SUSPENDED", "流通/有效市值低于50亿，流动性和冲击成本风险偏高"
    if "blowoff_volume_risk" in flags:
        return "SUSPENDED", "量能异常放大且换手/涨幅风险偏高，避免天量追高或放量出货"
    if "liquidity_hard_floor" in flags:
        return "BLOCK", "成交额低于1亿流动性硬底线，突发风险时可能无法退出"
    if "liquidity_avg_amount_low" in flags or "turnover_out_of_range" in flags:
        return "SUSPENDED", "20日日均成交额或换手率不符合流动性要求"
    if "order_book_depth_low" in flags or "order_book_imbalance" in flags:
        return "SUSPENDED", "五档盘口深度或买卖盘平衡度不足，先等待承接改善"
    if "fundamental_loss" in flags or "performance_deterioration" in flags:
        return "BLOCK", "业绩亏损或收入利润同步恶化，未通过基本面底线"
    if (
        "growth_threshold_miss" in flags
        or "qoq_performance_drop" in flags
        or "profit_momentum_weak" in flags
        or "debt_ratio_over_cap" in flags
        or "roe_below_threshold" in flags
        or "roa_below_threshold" in flags
        or "gross_margin_below_threshold" in flags
        or "quick_ratio_low" in flags
        or "roic_below_threshold" in flags
        or "receivable_ratio_high" in flags
        or "prepayment_growth_high" in flags
        or "related_transaction_ratio_high" in flags
    ):
        return "SUSPENDED", "ROE/ROA/ROIC/毛利率/成长性/环比延续性/偿债能力或财务雷区未达到策略阈值"
    if "price_mismatch" in flags:
        return "SUSPENDED", "价格双源校验偏差过大，需等待行情源修复"
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


def _ratio_to_pct(value: Any, default: float = 0.0) -> float:
    number = _safe_number(value, default)
    if 0.0 < abs(number) <= 1.0:
        return number * 100.0
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


def _select_existing_column(
    columns: set[str],
    column: str,
    default_sql: str = "NULL",
    alias: str | None = None,
    table_alias: str = "",
) -> str:
    out_alias = alias or column
    if column in columns:
        prefix = f"{table_alias}." if table_alias else ""
        return f"{prefix}`{column}` AS `{out_alias}`"
    return f"{default_sql} AS `{out_alias}`"


def _first_existing(columns: set[str], candidates: tuple[str, ...]) -> str:
    for column in candidates:
        if column in columns:
            return column
    return ""


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name}).fetchall()
    return {str(r[0]) for r in rows}


def _character_column_length(engine: Engine, table_name: str, column_name: str) -> int | None:
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
              AND COLUMN_NAME = :column_name
        """), {"table_name": table_name, "column_name": column_name}).scalar()
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _ensure_model_version_capacity(
    conn: Any,
    *,
    table_name: str,
    existing_columns: set[str],
    current_length: int | None,
) -> None:
    allowed_tables = {"stock_analysis_result", "st_recommended_stocks"}
    if table_name not in allowed_tables:
        raise ValueError(f"Unsupported model_version table: {table_name}")
    if "model_version" not in existing_columns:
        return
    if current_length is not None and current_length >= MODEL_VERSION_COLUMN_LENGTH:
        return
    logger.warning(
        "Expanding %s.model_version from %s to VARCHAR(%s)",
        table_name,
        current_length if current_length is not None else "unknown",
        MODEL_VERSION_COLUMN_LENGTH,
    )
    conn.execute(text(
        f"ALTER TABLE `{table_name}` MODIFY COLUMN `model_version` "
        f"VARCHAR({MODEL_VERSION_COLUMN_LENGTH}) DEFAULT ''"
    ))


def _ensure_recommended_columns(engine: Engine) -> None:
    required = {
        "industry_name": "VARCHAR(128) DEFAULT ''",
        "long_term_score": "DECIMAL(5,1) DEFAULT NULL",
        "short_term_score": "DECIMAL(5,1) DEFAULT NULL",
        "recommend_status": "VARCHAR(10) DEFAULT 'BLOCK'",
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
        "investment_rating": "VARCHAR(20) DEFAULT '中性'",
        "rating_reason": "VARCHAR(500) DEFAULT ''",
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
        "risk_reward_ratio": "DECIMAL(8,2) DEFAULT NULL",
        "resistance_price": "DECIMAL(12,4) DEFAULT NULL",
        "sector_gate_status": "VARCHAR(20) DEFAULT 'WATCH'",
        "sector_gate_reason": "VARCHAR(500) DEFAULT ''",
        "sector_flow_3d": "DECIMAL(18,2) DEFAULT NULL",
        "sector_width_pct": "DECIMAL(8,2) DEFAULT NULL",
        "technical_evidence_json": "TEXT NULL",
        "evidence_chain_json": "TEXT NULL",
        "review_1d_pct": "DECIMAL(8,2) DEFAULT NULL",
        "review_3d_pct": "DECIMAL(8,2) DEFAULT NULL",
        "review_5d_pct": "DECIMAL(8,2) DEFAULT NULL",
        "review_10d_pct": "DECIMAL(8,2) DEFAULT NULL",
        "failure_tags_json": "TEXT NULL",
        "heat_overload_score": "DECIMAL(5,1) DEFAULT NULL",
        "confidence_score": "DECIMAL(5,1) DEFAULT NULL",
        "chip_capital_score": "DECIMAL(5,1) DEFAULT NULL",
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
        "chase_policy_version": "VARCHAR(64) DEFAULT ''",
        "surge_streak_lower_bound": "INT DEFAULT NULL",
        "recent_max_surge_streak": "INT DEFAULT NULL",
        "latest_danger_surge_streak": "INT DEFAULT NULL",
        "sessions_since_extreme_surge": "INT DEFAULT NULL",
        "recent_extreme_run_return_pct": "DECIMAL(10,4) DEFAULT NULL",
        "drawdown_from_recent_peak_pct": "DECIMAL(10,4) DEFAULT NULL",
        "rebase_confirmed": "TINYINT(1) DEFAULT 0",
        "exact_limit_up_streak": "INT DEFAULT NULL",
        "trailing_untradeable_sessions": "INT DEFAULT NULL",
        "latest_tradable_date": "DATE DEFAULT NULL",
        "limit_rule_status": "VARCHAR(30) DEFAULT ''",
        "capacity_state": "VARCHAR(30) DEFAULT 'UNKNOWN'",
        "one_price_limit_up_proxy": "TINYINT(1) DEFAULT NULL",
        "extreme_extension_flag": "TINYINT(1) DEFAULT NULL",
        "ordinary_buy_eligible": "TINYINT(1) DEFAULT 0",
        "chase_risk_status": "VARCHAR(30) DEFAULT 'DATA_BLOCKED'",
        "chase_risk_reason": "VARCHAR(500) DEFAULT ''",
        "chase_risk_evidence_json": "TEXT NULL",
        "model_version": f"VARCHAR({MODEL_VERSION_COLUMN_LENGTH}) DEFAULT ''",
    }
    existing = _table_columns(engine, "st_recommended_stocks")
    model_version_length = (
        _character_column_length(engine, "st_recommended_stocks", "model_version")
        if "model_version" in existing
        else None
    )
    with engine.begin() as conn:
        for column, ddl in required.items():
            if column not in existing:
                logger.info("Adding st_recommended_stocks.%s", column)
                conn.execute(text(f"ALTER TABLE st_recommended_stocks ADD COLUMN `{column}` {ddl}"))
        if "recommend_status" in existing:
            conn.execute(text(
                "ALTER TABLE st_recommended_stocks MODIFY COLUMN "
                "`recommend_status` VARCHAR(10) DEFAULT 'BLOCK'"
            ))
        _ensure_model_version_capacity(
            conn,
            table_name="st_recommended_stocks",
            existing_columns=existing,
            current_length=model_version_length,
        )


def _ensure_analysis_columns(engine: Engine) -> None:
    required = {
        "model_version": f"VARCHAR({MODEL_VERSION_COLUMN_LENGTH}) DEFAULT ''",
        "data_quality_score": "DECIMAL(5,1) DEFAULT NULL",
        "data_quality_flags": "TEXT NULL",
        "flow_trade_date": "DATE DEFAULT NULL",
        "hot_trade_date": "DATE DEFAULT NULL",
        "chase_policy_version": "VARCHAR(64) DEFAULT ''",
        "surge_streak_lower_bound": "INT DEFAULT NULL",
        "recent_max_surge_streak": "INT DEFAULT NULL",
        "latest_danger_surge_streak": "INT DEFAULT NULL",
        "sessions_since_extreme_surge": "INT DEFAULT NULL",
        "recent_extreme_run_return_pct": "DECIMAL(10,4) DEFAULT NULL",
        "drawdown_from_recent_peak_pct": "DECIMAL(10,4) DEFAULT NULL",
        "rebase_confirmed": "TINYINT(1) DEFAULT 0",
        "exact_limit_up_streak": "INT DEFAULT NULL",
        "trailing_untradeable_sessions": "INT DEFAULT NULL",
        "latest_tradable_date": "DATE DEFAULT NULL",
        "limit_rule_status": "VARCHAR(30) DEFAULT ''",
        "capacity_state": "VARCHAR(30) DEFAULT 'UNKNOWN'",
        "one_price_limit_up_proxy": "TINYINT(1) DEFAULT NULL",
        "extreme_extension_flag": "TINYINT(1) DEFAULT NULL",
        "ordinary_buy_eligible": "TINYINT(1) DEFAULT 0",
        "chase_risk_status": "VARCHAR(30) DEFAULT 'DATA_BLOCKED'",
        "chase_risk_reason": "VARCHAR(500) DEFAULT ''",
        "chase_risk_evidence_json": "TEXT NULL",
    }
    existing = _table_columns(engine, "stock_analysis_result")
    model_version_length = (
        _character_column_length(engine, "stock_analysis_result", "model_version")
        if "model_version" in existing
        else None
    )
    with engine.begin() as conn:
        for column, ddl in required.items():
            if column not in existing:
                logger.info("Adding stock_analysis_result.%s", column)
                conn.execute(text(f"ALTER TABLE stock_analysis_result ADD COLUMN `{column}` {ddl}"))
        if "recommend_status" in existing:
            conn.execute(text(
                "ALTER TABLE stock_analysis_result MODIFY COLUMN "
                "`recommend_status` VARCHAR(10) DEFAULT 'BLOCK'"
            ))
        _ensure_model_version_capacity(
            conn,
            table_name="stock_analysis_result",
            existing_columns=existing,
            current_length=model_version_length,
        )


def _ensure_output_schema(engine: Engine) -> None:
    """Repair output-table compatibility before the expensive analysis starts."""
    _ensure_analysis_columns(engine)
    _ensure_recommended_columns(engine)


def latest_trade_date(engine: Engine) -> str:
    sql = """
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
        """
    with _query_engine(engine, sql).connect() as conn:
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
        kline_sql = """
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date < :ref_date
        """
        with _query_engine(engine, kline_sql).connect() as kline_conn:
            value = kline_conn.execute(text(kline_sql), {"ref_date": ref_date}).scalar()
    if not value:
        raise RuntimeError(f"Cannot resolve previous trade date before {ref_date}")
    return str(value)[:10]


def assert_trade_date_ready(engine: Engine, trade_date: str, min_coverage: float = 0.80) -> dict[str, Any]:
    """Fail fast when the target trading day is missing or clearly incomplete."""
    trade_date = str(trade_date or "").strip()[:10]
    if not trade_date:
        raise ValueError("trade_date is required")
    min_coverage = max(0.0, min(1.0, float(min_coverage)))
    with get_kline_engine().connect() as conn:
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
    with engine.connect() as conn:
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
    env = build_child_env(ROOT)
    env["SM_STOCK_KLINE_SOURCE"] = "qmt"
    env["SM_MAX_STOCKS"] = "0"
    env["SM_SKIP_GLOBAL_TRUNCATE"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

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


def _recent_dates(
    engine: Engine,
    table: str,
    column: str,
    end_date: str,
    limit: int,
    *,
    as_of_at: str | None = None,
) -> list[str]:
    limit = max(1, int(limit))
    knowledge_clause = ""
    params: dict[str, Any] = {"end_date": end_date}
    if as_of_at is not None:
        if table != "sm_stock_kline":
            raise ValueError("knowledge-time date filtering is only supported for sm_stock_kline")
        knowledge_clause = """
              AND CASE
                    WHEN received_at IS NULL AND etl_sync_at IS NULL THEN NULL
                    WHEN received_at IS NULL THEN etl_sync_at
                    WHEN etl_sync_at IS NULL THEN received_at
                    WHEN received_at >= etl_sync_at THEN received_at
                    ELSE etl_sync_at
                  END <= :as_of_at
        """
        params["as_of_at"] = as_of_at
    sql = f"""
            SELECT DISTINCT `{column}` AS d
            FROM `{table}`
            WHERE `{column}` <= :end_date
            {knowledge_clause}
            ORDER BY `{column}` DESC
            LIMIT {limit}
        """
    read_engine = _query_engine(engine, sql)
    attempts = _db_read_attempts()
    for attempt in range(1, attempts + 1):
        try:
            with read_engine.connect() as conn:
                rows = conn.execute(text(sql), params).fetchall()
            return [str(r[0])[:10] for r in rows if r[0] is not None]
        except DBAPIError as exc:
            errno = _db_errno(exc)
            if errno not in _TRANSIENT_DB_ERRNOS or attempt >= attempts:
                raise
            delay = min(5.0, 0.5 * (2 ** (attempt - 1)))
            logger.warning(
                "Transient recent-date SQL failed errno=%s attempt=%s/%s; retrying in %.1fs",
                errno,
                attempt,
                attempts,
                delay,
            )
            try:
                read_engine.dispose()
            except Exception:
                logger.debug("Failed to dispose engine after recent-date error", exc_info=True)
            time.sleep(delay)
    return []


def estimate_volume_profile_levels(rows: Any, lookback: int = 90, bins: int = 12) -> dict[str, Any]:
    """Approximate chip-price support/resistance from recent amount-weighted price bins."""
    if rows is None:
        df = pd.DataFrame()
    elif isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        df = pd.DataFrame(rows)
    if df.empty:
        return {
            "volume_profile_peak_price": None,
            "volume_profile_support_price": None,
            "volume_profile_resistance_price": None,
            "volume_profile_peak_density": 0.0,
        }
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df.sort_values("trade_date")
    df = df.tail(int(lookback)).copy()
    for col in ("high", "low", "close", "amount", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close"])
    if df.empty:
        return {
            "volume_profile_peak_price": None,
            "volume_profile_support_price": None,
            "volume_profile_resistance_price": None,
            "volume_profile_peak_density": 0.0,
        }
    high = df["high"].where(df.get("high", df["close"]) > 0, df["close"]) if "high" in df.columns else df["close"]
    low = df["low"].where(df.get("low", df["close"]) > 0, df["close"]) if "low" in df.columns else df["close"]
    typical_price = (high.fillna(df["close"]) + low.fillna(df["close"]) + df["close"]) / 3.0
    weights = pd.to_numeric(df.get("amount", pd.Series(np.nan, index=df.index)), errors="coerce")
    if weights.fillna(0.0).sum() <= 0:
        weights = pd.to_numeric(df.get("volume", pd.Series(1.0, index=df.index)), errors="coerce")
    weights = weights.fillna(0.0)
    if weights.sum() <= 0:
        weights = pd.Series(1.0, index=df.index)
    price_min = float(typical_price.min())
    price_max = float(typical_price.max())
    current = _safe_number(df["close"].iloc[-1], 0.0)
    if price_min <= 0 or price_max <= 0 or price_max <= price_min or current <= 0:
        peak = _safe_number(typical_price.iloc[-1], current)
        return {
            "volume_profile_peak_price": round(peak, 2) if peak > 0 else None,
            "volume_profile_support_price": round(peak, 2) if peak > 0 else None,
            "volume_profile_resistance_price": round(peak, 2) if peak > 0 else None,
            "volume_profile_peak_density": 1.0 if peak > 0 else 0.0,
        }
    edges = np.linspace(price_min, price_max, max(3, int(bins)) + 1)
    bucket = pd.cut(typical_price, bins=edges, include_lowest=True, labels=False)
    profile = pd.DataFrame({"bucket": bucket, "weight": weights, "price": typical_price}).dropna(subset=["bucket"])
    if profile.empty:
        return {
            "volume_profile_peak_price": None,
            "volume_profile_support_price": None,
            "volume_profile_resistance_price": None,
            "volume_profile_peak_density": 0.0,
        }
    grouped = profile.groupby("bucket", as_index=False).agg(weight=("weight", "sum"), price=("price", "mean"))
    grouped["center"] = grouped["bucket"].astype(int).map(lambda idx: (edges[idx] + edges[idx + 1]) / 2.0)
    total_weight = float(grouped["weight"].sum())
    peak_row = grouped.sort_values("weight", ascending=False).iloc[0]
    below = grouped[grouped["center"] <= current * 1.01].sort_values("weight", ascending=False)
    above = grouped[grouped["center"] >= current * 0.99].sort_values("weight", ascending=False)
    support = below.iloc[0]["center"] if not below.empty else peak_row["center"]
    resistance = above.iloc[0]["center"] if not above.empty else peak_row["center"]
    return {
        "volume_profile_peak_price": round(float(peak_row["center"]), 2),
        "volume_profile_support_price": round(float(support), 2),
        "volume_profile_resistance_price": round(float(resistance), 2),
        "volume_profile_peak_density": round(float(peak_row["weight"]) / total_weight, 4) if total_weight > 0 else 0.0,
    }


def estimate_latest_close_percentile(rows: Any, lookback: int = 250, min_count: int = 60) -> dict[str, Any]:
    """Estimate current valuation crowding by the latest close percentile in recent history."""
    if rows is None:
        df = pd.DataFrame()
    elif isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        df = pd.DataFrame(rows)
    if df.empty or "close" not in df.columns:
        return {"close_percentile_250d": None, "close_history_count": 0}
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df.sort_values("trade_date")
    closes = pd.to_numeric(df["close"], errors="coerce").dropna().tail(int(lookback))
    closes = closes[closes > 0]
    if closes.empty:
        return {"close_percentile_250d": None, "close_history_count": 0}
    count = int(closes.count())
    if count < int(min_count):
        return {"close_percentile_250d": None, "close_history_count": count}
    current = float(closes.iloc[-1])
    percentile = float((closes <= current).mean() * 100.0)
    return {
        "close_percentile_250d": round(percentile, 1),
        "close_history_count": count,
    }


def _normalize_chase_as_of(
    value: str | date | datetime | pd.Timestamp,
    *,
    allow_naive_local: bool = False,
) -> pd.Timestamp:
    """Return an exact Asia/Shanghai knowledge-time cutoff.

    A date-only value has an explicit end-of-local-day research meaning.  Any
    timestamp supplied directly to the factor helper must be timezone-aware;
    command/pipeline boundaries may opt into interpreting their documented
    naive values as Asia/Shanghai wall time before calling the helper.
    """
    date_only: date | None = None
    if isinstance(value, date) and not isinstance(value, datetime):
        date_only = value
    elif isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value.strip()):
        parsed_date = pd.to_datetime(value.strip(), errors="coerce")
        if pd.isna(parsed_date):
            raise ValueError(f"invalid chase-risk cutoff: {value!r}")
        date_only = parsed_date.date()
    if date_only is not None:
        start = pd.Timestamp(date_only).tz_localize(CHINA_MARKET_TIMEZONE)
        return start + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    cutoff = pd.Timestamp(value)
    if pd.isna(cutoff):
        raise ValueError(f"invalid chase-risk cutoff: {value!r}")
    if cutoff.tzinfo is None:
        if not allow_naive_local:
            raise ValueError(
                "exact chase-risk cutoff must be timezone-aware; "
                "use a date-only end-of-day cutoff or include an offset"
            )
        cutoff = cutoff.tz_localize(CHINA_MARKET_TIMEZONE)
    else:
        cutoff = cutoff.tz_convert(CHINA_MARKET_TIMEZONE)
    return cutoff


def _normalize_acquisition_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    try:
        parsed_timezone = parsed.dt.tz
    except AttributeError:
        def normalize_one(value: Any) -> pd.Timestamp:
            if value is None or pd.isna(value):
                return pd.NaT
            timestamp = pd.Timestamp(value)
            if timestamp.tzinfo is None:
                return timestamp.tz_localize(CHINA_MARKET_TIMEZONE)
            return timestamp.tz_convert(CHINA_MARKET_TIMEZONE)

        return series.map(normalize_one)
    if parsed_timezone is None:
        return parsed.dt.tz_localize(
            CHINA_MARKET_TIMEZONE,
            ambiguous="NaT",
            nonexistent="NaT",
        )
    return parsed.dt.tz_convert(CHINA_MARKET_TIMEZONE)


def _attach_effective_acquisition_time(
    source: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    prepared = source.copy()
    acquisition_columns = [
        column for column in ("received_at", "etl_sync_at")
        if column in prepared.columns
    ]
    if not acquisition_columns:
        return prepared, []
    for column in acquisition_columns:
        prepared[column] = _normalize_acquisition_series(prepared[column])
    prepared["_chase_acquired_at"] = prepared[acquisition_columns].max(axis=1)
    return prepared, acquisition_columns


def _pit_cutoff_sql_clause(
    alias: str,
    available_columns: set[str],
    *,
    candidates: tuple[str, ...] = ("received_at", "etl_sync_at"),
    param_name: str = "knowledge_cutoff",
) -> str:
    """Build a portable fail-closed acquisition-time predicate.

    Every present timestamp must be at or before the cutoff, and at least one
    timestamp must exist.  If a table has no acquisition column at all the
    predicate is deliberately impossible instead of silently treating an
    event/report date as knowledge time.
    """
    prefix = f"{alias}." if alias else ""
    columns = [column for column in candidates if column in available_columns]
    if not columns:
        return "1 = 0"
    evidence = " OR ".join(f"{prefix}`{column}` IS NOT NULL" for column in columns)
    bounded = " AND ".join(
        f"({prefix}`{column}` IS NULL OR {prefix}`{column}` <= :{param_name})"
        for column in columns
    )
    return f"({evidence}) AND {bounded}"


def _build_chase_risk_features_from_rows(
    rows: Any,
    as_of_date: str | date | datetime | pd.Timestamp,
) -> pd.DataFrame:
    """Build a conservative, prefix-invariant chase/tradability gate.

    ``sm_stock_kline`` does not currently preserve the exchange's effective
    price-limit rule (ST state, IPO age, board and rule revision) for every
    historical row.  Therefore this helper deliberately does *not* invent an
    exact consecutive-limit-up count.  It emits a lower-bound streak of
    tradable sessions that gained at least 9.5% and closed at the session high,
    while ``exact_limit_up_streak`` remains null until an official rule source
    is available.

    Rows after the trade-date *or knowledge-time* cutoff are discarded before
    revision selection and calculation.  Date-only calls mean end-of-local-day;
    exact timestamps must be timezone-aware.  Missing acquisition evidence is
    a data block, never permission to use an unprovable historical revision.
    """
    columns = [
        "stock_code",
        "chase_policy_version",
        "surge_streak_lower_bound",
        "recent_max_surge_streak",
        "latest_danger_surge_streak",
        "sessions_since_extreme_surge",
        "recent_extreme_run_return_pct",
        "drawdown_from_recent_peak_pct",
        "rebase_confirmed",
        "exact_limit_up_streak",
        "trailing_untradeable_sessions",
        "latest_tradable_date",
        "limit_rule_status",
        "capacity_state",
        "one_price_limit_up_proxy",
        "return_5d_pct",
        "gap_pct",
        "crowding_detected",
        "extreme_extension_flag",
        "ordinary_buy_eligible",
        "chase_risk_status",
        "chase_risk_reason",
        "chase_risk_evidence_json",
    ]
    if rows is None:
        return pd.DataFrame(columns=columns)
    source = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    identity_columns = {"stock_code", "trade_date"}
    required = identity_columns | {
        "open", "high", "low", "close", "volume", "amount",
        "change_pct", "pre_close",
    }
    if source.empty or not identity_columns.issubset(source.columns):
        return pd.DataFrame(columns=columns)

    cutoff = _normalize_chase_as_of(as_of_date)
    cutoff_date = cutoff.date()
    source = source.copy()
    source["trade_date"] = pd.to_datetime(source["trade_date"], errors="coerce").dt.date
    source = source[source["trade_date"].notna() & (source["trade_date"] <= cutoff_date)]
    if source.empty:
        return pd.DataFrame(columns=columns)
    source["stock_code"] = source["stock_code"].astype(str).str.strip()
    source = source[source["stock_code"].str.fullmatch(r"\d{1,6}", na=False)]
    source["stock_code"] = source["stock_code"].str.zfill(6)
    if source.empty:
        return pd.DataFrame(columns=columns)
    all_codes = set(source["stock_code"].unique())

    def blocked_record(
        stock_code: str,
        reason: str,
        *,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "stock_code": stock_code,
            "chase_policy_version": CHASE_POLICY_VERSION,
            "surge_streak_lower_bound": None,
            "recent_max_surge_streak": None,
            "latest_danger_surge_streak": None,
            "sessions_since_extreme_surge": None,
            "recent_extreme_run_return_pct": None,
            "drawdown_from_recent_peak_pct": None,
            "rebase_confirmed": False,
            "exact_limit_up_streak": None,
            "trailing_untradeable_sessions": None,
            "latest_tradable_date": None,
            "limit_rule_status": "SOURCE_TIME_MISSING",
            "capacity_state": "UNKNOWN",
            "one_price_limit_up_proxy": None,
            "return_5d_pct": None,
            "gap_pct": None,
            "crowding_detected": None,
            "extreme_extension_flag": None,
            "ordinary_buy_eligible": False,
            "chase_risk_status": "DATA_BLOCKED",
            "chase_risk_reason": reason,
            "chase_risk_evidence_json": json.dumps(
                {
                    **evidence,
                    "knowledge_cutoff": cutoff.isoformat(),
                    "policy_version": CHASE_POLICY_VERSION,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }

    missing_columns = sorted(required - set(source.columns))
    if missing_columns:
        reason = "missing chase-risk source fields: " + ",".join(missing_columns)
        return pd.DataFrame(
            [
                blocked_record(
                    stock_code,
                    reason,
                    evidence={"missing_fields": missing_columns},
                )
                for stock_code in sorted(all_codes)
            ],
            columns=columns,
        )

    duplicate_identity_codes = set(
        source.loc[
            source.duplicated(["stock_code", "trade_date"], keep=False),
            "stock_code",
        ]
    )
    source, acquisition_columns = _attach_effective_acquisition_time(source)
    if not acquisition_columns:
        reason = "daily revisions lack received_at/etl_sync_at acquisition evidence"
        return pd.DataFrame(
            [
                blocked_record(
                    stock_code,
                    reason,
                    evidence={"missing_fields": ["received_at", "etl_sync_at"]},
                )
                for stock_code in sorted(all_codes)
            ],
            columns=columns,
        )

    blocked: dict[str, dict[str, Any]] = {}
    missing_acquisition = source["_chase_acquired_at"].isna()
    for stock_code in sorted(set(source.loc[missing_acquisition, "stock_code"])):
        if stock_code in duplicate_identity_codes:
            reason = "duplicate daily revisions lack received_at/etl_sync_at ordering evidence"
        else:
            reason = "one or more daily revisions lack acquisition-time evidence"
        blocked[stock_code] = blocked_record(
            stock_code,
            reason,
            evidence={"missing_acquisition_time": True},
        )
    source = source[
        ~source["stock_code"].isin(blocked)
        & source["_chase_acquired_at"].notna()
        & source["_chase_acquired_at"].le(cutoff)
    ].copy()
    available_codes = set(source["stock_code"].unique())
    for stock_code in sorted(all_codes - set(blocked) - available_codes):
        blocked[stock_code] = blocked_record(
            stock_code,
            "no daily revision was known at the requested cutoff",
            evidence={"available_revision_count": 0},
        )

    for column in (
        "open", "high", "low", "close", "volume", "amount",
        "change_pct", "pre_close", "turnover_ratio",
    ):
        if column not in source.columns:
            source[column] = np.nan
        source[column] = pd.to_numeric(source[column], errors="coerce")
    duplicate_mask = source.duplicated(["stock_code", "trade_date"], keep=False)
    ambiguous_revision_codes = set(source.loc[duplicate_mask, "stock_code"])
    duplicate_evidence = source.loc[
        duplicate_mask,
        ["stock_code", "trade_date", "_chase_acquired_at"],
    ]
    for stock_code, revisions in duplicate_evidence.groupby("stock_code", sort=False):
        tied = revisions.duplicated(
            ["trade_date", "_chase_acquired_at"], keep=False
        ).any()
        if not bool(tied):
            ambiguous_revision_codes.discard(stock_code)
    source = source.sort_values(
        ["stock_code", "trade_date", "_chase_acquired_at", *acquisition_columns],
        kind="mergesort",
    )
    source = source.drop_duplicates(
        ["stock_code", "trade_date"], keep="last"
    ).sort_values(["stock_code", "trade_date"], kind="mergesort")
    source = source.drop(columns=["_chase_acquired_at"], errors="ignore")

    results: list[dict[str, Any]] = []
    for stock_code, code_rows in source.groupby("stock_code", sort=False):
        ordered = code_rows.sort_values("trade_date").reset_index(drop=True)
        valid_ohlc = (
            ordered[["open", "high", "low", "close"]]
            .notna().all(axis=1)
            & (ordered[["open", "high", "low", "close"]] > 0).all(axis=1)
            & ordered["high"].ge(ordered[["open", "low", "close"]].max(axis=1))
            & ordered["low"].le(ordered[["open", "high", "close"]].min(axis=1))
        )
        valid_prices = (
            valid_ohlc
            & ordered["pre_close"].notna()
            & ordered["pre_close"].gt(0)
        )
        valid_flow = (
            ordered[["volume", "amount"]].notna().all(axis=1)
            & ordered["volume"].ge(0)
            & ordered["amount"].ge(0)
        )
        return_from_pre_close = (
            ordered["close"] / ordered["pre_close"] - 1.0
        ) * 100.0
        same_price = (
            ordered[["open", "high", "low", "close"]].max(axis=1)
            / ordered[["open", "high", "low", "close"]].min(axis=1)
        ).le(1.0005)
        one_price_proxy = (
            valid_prices
            & same_price
            & ordered["change_pct"].ge(9.5)
            & return_from_pre_close.ge(9.5)
        ).fillna(False)
        price_discovery = (
            valid_prices & valid_flow
            & ordered["volume"].gt(0) & ordered["amount"].gt(0)
        )
        known_no_capacity = (
            (valid_ohlc & valid_flow & (
                ordered["volume"].eq(0) | ordered["amount"].eq(0)
            ))
            | one_price_proxy
        )
        known_capacity = price_discovery & ~one_price_proxy
        capacity_state = pd.Series("UNKNOWN", index=ordered.index, dtype="object")
        capacity_state.loc[known_no_capacity] = "KNOWN_NO_CAPACITY"
        capacity_state.loc[known_capacity] = "KNOWN_CAPACITY"
        latest_intraday_ohlc_status = str(
            ordered.iloc[-1].get("intraday_ohlc_status") or ""
        ).upper()

        trailing_untradeable = 0
        for state in reversed(capacity_state.tolist()):
            if state == "KNOWN_CAPACITY":
                break
            if state == "UNKNOWN":
                break
            trailing_untradeable += 1

        # Positive prints still carry price-path evidence for the conservative
        # surge streak, including one-price limit-up sessions.  Capacity is a
        # separate gate: those sessions may prove the nine-board path while
        # remaining ineligible for an ordinary buy.
        tradable_rows = ordered[price_discovery].copy()
        if tradable_rows.empty:
            latest_state = str(capacity_state.iloc[-1])
            if latest_state == "UNKNOWN":
                status = "DATA_BLOCKED"
                reason = (
                    "intraday OHLC path is unavailable at the knowledge cutoff"
                    if latest_intraday_ohlc_status == "DATA_BLOCKED"
                    else "daily-bar price/capacity fields are incomplete"
                )
            else:
                status = "EXECUTION_BLOCKED"
                reason = "no session with verified ordinary-buy capacity at or before cutoff"
            results.append({
                "stock_code": stock_code,
                "chase_policy_version": CHASE_POLICY_VERSION,
                "surge_streak_lower_bound": None,
                "recent_max_surge_streak": None,
                "latest_danger_surge_streak": None,
                "sessions_since_extreme_surge": None,
                "recent_extreme_run_return_pct": None,
                "drawdown_from_recent_peak_pct": None,
                "rebase_confirmed": False,
                "exact_limit_up_streak": None,
                "trailing_untradeable_sessions": int(trailing_untradeable),
                "latest_tradable_date": None,
                "limit_rule_status": (
                    "PROXY_ONLY" if bool(one_price_proxy.iloc[-1]) else "RULE_MISSING"
                ),
                "capacity_state": latest_state,
                "one_price_limit_up_proxy": bool(one_price_proxy.iloc[-1]),
                "return_5d_pct": None,
                "gap_pct": None,
                "crowding_detected": None,
                "extreme_extension_flag": None,
                "ordinary_buy_eligible": False,
                "chase_risk_status": status,
                "chase_risk_reason": reason,
                "chase_risk_evidence_json": json.dumps(
                    {
                        "capacity_state": latest_state,
                        "intraday_ohlc_status": latest_intraday_ohlc_status or None,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            })
            continue

        close_at_high = (
            tradable_rows["high"].gt(0)
            & (tradable_rows["close"] / tradable_rows["high"]).ge(0.995)
        )
        tradable_return_from_pre_close = (
            tradable_rows["close"] / tradable_rows["pre_close"] - 1.0
        ) * 100.0
        conservative_surge = (
            close_at_high
            & tradable_rows["change_pct"].ge(9.5)
            & tradable_return_from_pre_close.ge(9.5)
        )
        streak = 0
        for is_surge in reversed(conservative_surge.fillna(False).tolist()):
            if not bool(is_surge):
                break
            streak += 1

        surge_flags = [bool(value) for value in conservative_surge.fillna(False)]
        recent_offset = max(0, len(surge_flags) - CHASE_RECENT_SURGE_LOOKBACK)
        surge_runs: list[tuple[int, int, int]] = []
        run_start: int | None = None
        for position, is_surge in enumerate(surge_flags):
            if is_surge:
                if run_start is None:
                    run_start = position
            elif run_start is not None:
                surge_runs.append((run_start, position - 1, position - run_start))
                run_start = None
        if run_start is not None:
            surge_runs.append((run_start, len(surge_flags) - 1, len(surge_flags) - run_start))

        recent_runs = [run for run in surge_runs if run[1] >= recent_offset]
        recent_max_streak = max((run[2] for run in recent_runs), default=0)
        danger_runs = [
            run for run in surge_runs if run[2] >= CHASE_DANGER_SURGE_STREAK
        ]
        latest_danger = danger_runs[-1] if danger_runs else None
        last_extreme_start = latest_danger[0] if latest_danger is not None else None
        last_extreme_end = latest_danger[1] if latest_danger is not None else None
        latest_danger_streak = latest_danger[2] if latest_danger is not None else 0

        sessions_since_extreme = (
            None
            if last_extreme_end is None
            else len(tradable_rows) - 1 - last_extreme_end
        )
        extreme_run_return = None
        if last_extreme_start is not None and last_extreme_end is not None:
            start_row = tradable_rows.iloc[last_extreme_start]
            base_price = float(start_row["pre_close"])
            if base_price <= 0 and last_extreme_start > 0:
                base_price = float(tradable_rows.iloc[last_extreme_start - 1]["close"])
            if base_price > 0:
                extreme_run_return = (
                    float(tradable_rows.iloc[last_extreme_end]["close"])
                    / base_price - 1.0
                ) * 100.0

        current_close = float(tradable_rows.iloc[-1]["close"])
        recent_peak_close = None
        recent_peak_position = None
        drawdown_from_peak = None
        sessions_since_peak = None
        if last_extreme_end is not None:
            post_extreme_closes = tradable_rows.iloc[last_extreme_end:]["close"].astype(float)
            peak_relative_position = int(np.argmax(post_extreme_closes.to_numpy()))
            recent_peak_position = last_extreme_end + peak_relative_position
            recent_peak_close = float(tradable_rows.iloc[recent_peak_position]["close"])
            if recent_peak_close > 0:
                drawdown_from_peak = (current_close / recent_peak_close - 1.0) * 100.0
                sessions_since_peak = len(tradable_rows) - 1 - recent_peak_position

        ma20_distance = None
        if len(tradable_rows) >= 20:
            ma20 = float(tradable_rows["close"].astype(float).tail(20).mean())
            if ma20 > 0:
                ma20_distance = (current_close / ma20 - 1.0) * 100.0
        true_ranges: list[float] = []
        for _, bar in tradable_rows.tail(14).iterrows():
            previous_close = float(bar["pre_close"])
            candidates = [float(bar["high"]) - float(bar["low"])]
            if previous_close > 0:
                candidates.extend([
                    abs(float(bar["high"]) - previous_close),
                    abs(float(bar["low"]) - previous_close),
                ])
            true_ranges.append(max(candidates))
        atr14 = float(np.mean(true_ranges)) if len(true_ranges) >= 5 else None
        pullback_atr = (
            (recent_peak_close - current_close) / atr14
            if recent_peak_close is not None and atr14 is not None and atr14 > 0
            else None
        )
        rebase_signal = bool(
            (drawdown_from_peak is not None and drawdown_from_peak <= CHASE_REBASE_DRAWDOWN_PCT)
            or (ma20_distance is not None and abs(ma20_distance) <= CHASE_REBASE_MA20_DISTANCE_PCT)
            or (pullback_atr is not None and pullback_atr >= CHASE_REBASE_PULLBACK_ATR)
        )
        rebase_confirmed = bool(
            latest_danger_streak >= CHASE_DANGER_SURGE_STREAK
            and sessions_since_extreme is not None
            and sessions_since_extreme >= CHASE_REBASE_MIN_SESSIONS
            and rebase_signal
        )
        unresolved_danger_episode = bool(
            latest_danger_streak >= CHASE_DANGER_SURGE_STREAK
            and sessions_since_extreme is not None
            and sessions_since_extreme > 0
            and not rebase_confirmed
        )

        latest_state = str(capacity_state.iloc[-1])
        return_5d = None
        if len(tradable_rows) >= 6:
            base_close = float(tradable_rows.iloc[-6]["close"])
            if base_close > 0:
                return_5d = (
                    float(tradable_rows.iloc[-1]["close"]) / base_close - 1.0
                ) * 100.0
        latest = ordered.iloc[-1]
        gap_pct = (
            (float(latest["open"]) / float(latest["pre_close"]) - 1.0) * 100.0
            if bool(valid_prices.iloc[-1])
            else None
        )
        latest_turnover = latest.get("turnover_ratio")
        crowding = (
            None if pd.isna(latest_turnover) else bool(float(latest_turnover) >= 20.0)
        )
        compound_extension = bool(
            return_5d is not None and return_5d >= 35.0
            and gap_pct is not None and gap_pct >= 5.0
            and crowding is True
        )
        extreme_extension = bool(
            streak >= CHASE_EXTREME_SURGE_STREAK
            or compound_extension
            or unresolved_danger_episode
        )

        if stock_code in ambiguous_revision_codes:
            status = "DATA_BLOCKED"
            eligible = False
            reason = "duplicate daily revisions lack received_at/etl_sync_at ordering evidence"
        elif latest_state == "UNKNOWN":
            status = "DATA_BLOCKED"
            eligible = False
            reason = (
                "intraday OHLC path is unavailable at the knowledge cutoff"
                if latest_intraday_ohlc_status == "DATA_BLOCKED"
                else "latest daily-bar capacity is unknown because required values are incomplete"
            )
        elif latest_state == "KNOWN_NO_CAPACITY":
            status = "EXECUTION_BLOCKED"
            eligible = False
            reason = (
                f"latest {max(trailing_untradeable, 1)} daily row(s) have no executable capacity; "
                f"prior conservative surge streak lower bound is {streak}"
            )
        elif extreme_extension:
            status = "WATCH"
            eligible = False
            reason = (
                f"extreme extension/cooldown is unresolved (tail_streak={streak}, "
                f"recent_max_streak={recent_max_streak}, "
                f"latest_danger_streak={latest_danger_streak}, "
                f"sessions_since_extreme={sessions_since_extreme}, "
                f"drawdown_from_peak_pct={drawdown_from_peak}, "
                f"compound_5d_gap_crowding={compound_extension}); "
                "ordinary buy is disabled until a tradable pullback/rebase"
            )
        elif streak == 3:
            status = "CONDITIONAL"
            eligible = False
            reason = "three-session conservative surge streak; ordinary buy requires rebase confirmation"
        else:
            status = "ALLOW"
            eligible = True
            reason = (
                f"conservative surge streak lower bound is {streak}; "
                f"rebase_confirmed={rebase_confirmed}"
            )

        evidence = {
            "capacity_state": latest_state,
            "compound_extension": compound_extension,
            "crowding_detected": crowding,
            "exact_limit_rule_available": False,
            "gap_pct": None if gap_pct is None else round(gap_pct, 6),
            "knowledge_cutoff": cutoff.isoformat(),
            "intraday_ohlc_status": latest_intraday_ohlc_status or None,
            "ma20_distance_pct": None if ma20_distance is None else round(ma20_distance, 6),
            "one_price_limit_up_proxy": bool(one_price_proxy.iloc[-1]),
            "policy_thresholds": {
                "recent_surge_lookback": CHASE_RECENT_SURGE_LOOKBACK,
                "danger_surge_streak": CHASE_DANGER_SURGE_STREAK,
                "extreme_surge_streak": CHASE_EXTREME_SURGE_STREAK,
                "rebase_min_sessions": CHASE_REBASE_MIN_SESSIONS,
                "rebase_drawdown_pct": CHASE_REBASE_DRAWDOWN_PCT,
                "rebase_ma20_distance_pct": CHASE_REBASE_MA20_DISTANCE_PCT,
                "rebase_pullback_atr": CHASE_REBASE_PULLBACK_ATR,
            },
            "policy_version": CHASE_POLICY_VERSION,
            "pullback_atr_multiple": None if pullback_atr is None else round(pullback_atr, 6),
            "rebase_confirmed": rebase_confirmed,
            "latest_danger_surge_streak": int(latest_danger_streak),
            "recent_extreme_run_return_pct": (
                None if extreme_run_return is None else round(extreme_run_return, 6)
            ),
            "recent_max_surge_streak": int(recent_max_streak),
            "recent_peak_close": recent_peak_close,
            "return_5d_pct": None if return_5d is None else round(return_5d, 6),
            "sessions_since_extreme_surge": sessions_since_extreme,
            "sessions_since_recent_peak": sessions_since_peak,
            "surge_streak_lower_bound": int(streak),
            "trailing_untradeable_sessions": int(trailing_untradeable),
        }
        results.append({
            "stock_code": stock_code,
            "chase_policy_version": CHASE_POLICY_VERSION,
            "surge_streak_lower_bound": int(streak),
            "recent_max_surge_streak": int(recent_max_streak),
            "latest_danger_surge_streak": int(latest_danger_streak),
            "sessions_since_extreme_surge": sessions_since_extreme,
            "recent_extreme_run_return_pct": (
                None if extreme_run_return is None else round(extreme_run_return, 6)
            ),
            "drawdown_from_recent_peak_pct": (
                None if drawdown_from_peak is None else round(drawdown_from_peak, 6)
            ),
            "rebase_confirmed": rebase_confirmed,
            # Exact streak is intentionally unknown without an effective rule
            # history; never coerce this field to zero.
            "exact_limit_up_streak": None,
            "trailing_untradeable_sessions": int(trailing_untradeable),
            "latest_tradable_date": tradable_rows.iloc[-1]["trade_date"],
            "limit_rule_status": "PROXY_ONLY",
            "capacity_state": latest_state,
            "one_price_limit_up_proxy": bool(one_price_proxy.iloc[-1]),
            "return_5d_pct": None if return_5d is None else round(return_5d, 6),
            "gap_pct": None if gap_pct is None else round(gap_pct, 6),
            "crowding_detected": crowding,
            "extreme_extension_flag": extreme_extension,
            "ordinary_buy_eligible": bool(eligible),
            "chase_risk_status": status,
            "chase_risk_reason": reason,
            "chase_risk_evidence_json": json.dumps(
                evidence,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        })
    results.extend(blocked[stock_code] for stock_code in sorted(blocked))
    return pd.DataFrame(results, columns=columns)


def _assert_chase_risk_coverage(frame: pd.DataFrame, trade_date: str) -> None:
    """Abort output refresh when the risk gate failed to cover the universe."""
    if frame is None or frame.empty:
        raise RuntimeError(f"chase-risk gate returned no rows for {trade_date}")
    required = {
        "stock_code",
        "chase_risk_status",
        "ordinary_buy_eligible",
        "chase_risk_reason",
        "chase_risk_evidence_json",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"chase-risk gate columns missing for {trade_date}: {','.join(missing)}"
        )
    status = frame["chase_risk_status"].map(_normalized_chase_gate_status)
    valid_statuses = {
        "ALLOW", "CONDITIONAL", "WATCH", "DATA_BLOCKED",
        "EXECUTION_BLOCKED",
    }
    invalid = ~status.isin(valid_statuses)
    eligible_missing = frame["ordinary_buy_eligible"].isna()
    eligible_true = frame["ordinary_buy_eligible"].map(_is_explicit_true)
    eligible_mismatch = (
        status.eq("ALLOW") & ~eligible_true
    ) | (
        ~status.eq("ALLOW") & eligible_true
    )
    reason_missing = frame["chase_risk_reason"].fillna("").astype(str).str.strip().eq("")
    evidence_missing = (
        frame["chase_risk_evidence_json"].fillna("").astype(str).str.strip().eq("")
    )
    invalid_rows = (
        invalid | eligible_missing | eligible_mismatch
        | reason_missing | evidence_missing
    )
    if bool(invalid_rows.any()):
        affected = (
            frame.loc[
                invalid_rows,
                "stock_code",
            ]
            .astype(str)
            .head(20)
            .tolist()
        )
        raise RuntimeError(
            f"chase-risk gate coverage is incomplete for {trade_date}: {affected}"
        )


def _build_latest_kline_features_from_rows(
    rows: pd.DataFrame,
    trade_date: str,
    names: pd.DataFrame | None = None,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Build latest per-stock K-line features from an already-scoped row set."""
    if rows is None or rows.empty:
        return pd.DataFrame()
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    df = rows.copy()
    if "short_name" not in df.columns:
        df["short_name"] = ""
    if names is not None and not names.empty:
        df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
        name_frame = names.copy()
        name_frame["stock_code"] = name_frame["stock_code"].astype(str).str.strip().str.zfill(6)
        name_frame = name_frame.drop_duplicates("stock_code", keep="last").rename(columns={"short_name": "name_from_code"})
        df = df.merge(name_frame[["stock_code", "name_from_code"]], on="stock_code", how="left")
        df["short_name"] = df["short_name"].replace("", np.nan).fillna(df["name_from_code"]).fillna("")
        df = df.drop(columns=["name_from_code"])

    # Keep every source revision for the risk gate.  Technical calculations
    # use only revisions that were knowable at the same cutoff, selected by
    # acquisition time before the per-date de-duplication.
    chase_source = df.copy()
    df, acquisition_columns = _attach_effective_acquisition_time(df)
    if acquisition_columns:
        df = df[
            df["_chase_acquired_at"].notna()
            & df["_chase_acquired_at"].le(cutoff)
        ].copy()
        if df.empty:
            return pd.DataFrame()

    numeric_cols = [
        "open", "high", "low", "close", "volume", "amount", "change_pct",
        "turnover_ratio", "pre_close",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce", downcast="float")
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["short_name"] = df["short_name"].fillna("").astype(str)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    if acquisition_columns:
        df = df.sort_values(
            ["stock_code", "trade_date", "_chase_acquired_at", *acquisition_columns],
            kind="mergesort",
        )
    df = df.drop_duplicates(["stock_code", "trade_date"], keep="last")
    df = df.drop(columns=["_chase_acquired_at"], errors="ignore")
    df = df.sort_values(["stock_code", "trade_date"], kind="mergesort")

    grouped = df.groupby("stock_code", group_keys=False)
    for window in (5, 10, 20, 60, 120, 250):
        min_periods = max(3, min(window, window // 2))
        df[f"ma{window}"] = grouped["close"].transform(lambda s, w=window, m=min_periods: s.rolling(w, min_periods=m).mean())
    df["ema12"] = grouped["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    df["ema26"] = grouped["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    df["amount_ma5"] = grouped["amount"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    df["amount_ma20"] = grouped["amount"].transform(lambda s: s.rolling(20, min_periods=8).mean())
    df["pct_5"] = grouped["close"].pct_change(5) * 100.0
    df["pct_20"] = grouped["close"].pct_change(20) * 100.0
    df["deduction_price_20"] = grouped["close"].shift(20)
    df["deduction_price_60"] = grouped["close"].shift(60)
    df["deduction_date_20"] = grouped["trade_date"].shift(20)
    df["deduction_date_60"] = grouped["trade_date"].shift(60)
    df["volatility_20"] = grouped["change_pct"].transform(lambda s: s.rolling(20, min_periods=8).std())
    df["high_20"] = grouped["high"].transform(lambda s: s.rolling(20, min_periods=8).max())
    df["high_60"] = grouped["high"].transform(lambda s: s.rolling(60, min_periods=20).max())
    df["low_60"] = grouped["low"].transform(lambda s: s.rolling(60, min_periods=20).min())
    df["drawdown_60"] = (df["close"] / df["high_60"] - 1.0) * 100.0
    df["from_low_60"] = (df["close"] / df["low_60"] - 1.0) * 100.0
    df["dist_ma20"] = (df["close"] / df["ma20"] - 1.0) * 100.0
    df["amount_ratio_5"] = df["amount"] / df["amount_ma5"].replace(0, np.nan)
    df["amount_ratio_20"] = df["amount"] / df["amount_ma20"].replace(0, np.nan)
    df["dif"] = df["ema12"] - df["ema26"]
    df["dea"] = grouped["dif"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = (df["dif"] - df["dea"]) * 2.0
    df["macd_dif"] = df["dif"]
    df["macd_dea"] = df["dea"]
    df["ma3_calc"] = grouped["close"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    df["ma6_calc"] = grouped["close"].transform(lambda s: s.rolling(6, min_periods=1).mean())
    df["ma12_calc"] = grouped["close"].transform(lambda s: s.rolling(12, min_periods=1).mean())
    df["ma24_calc"] = grouped["close"].transform(lambda s: s.rolling(24, min_periods=1).mean())
    df["bbi"] = (df["ma3_calc"] + df["ma6_calc"] + df["ma12_calc"] + df["ma24_calc"]) / 4.0
    df["bias6"] = (df["close"] / df["ma6_calc"].replace(0, np.nan) - 1.0) * 100.0
    df["bias12"] = (df["close"] / df["ma12_calc"].replace(0, np.nan) - 1.0) * 100.0
    df["bias24"] = (df["close"] / df["ma24_calc"].replace(0, np.nan) - 1.0) * 100.0
    close_10 = grouped["close"].shift(10)
    df["mtm10"] = df["close"] - close_10
    df["mtm10_pct"] = (df["close"] / close_10.replace(0, np.nan) - 1.0) * 100.0
    high_9 = grouped["high"].transform(lambda s: s.rolling(9, min_periods=1).max())
    low_9 = grouped["low"].transform(lambda s: s.rolling(9, min_periods=1).min())
    df["lwr9"] = ((high_9 - df["close"]) / (high_9 - low_9).replace(0, np.nan)) * 100.0
    df["kdj_rsv"] = ((df["close"] - low_9) / (high_9 - low_9).replace(0, np.nan)) * 100.0
    df["kdj_k"] = grouped["kdj_rsv"].transform(
        lambda s: s.fillna(50.0).ewm(alpha=1 / 3, adjust=False).mean()
    )
    df["kdj_d"] = grouped["kdj_k"].transform(lambda s: s.ewm(alpha=1 / 3, adjust=False).mean())
    df["kdj_j"] = 3.0 * df["kdj_k"] - 2.0 * df["kdj_d"]
    delta = grouped["close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta.clip(upper=0))
    for window in (6, 12, 24):
        avg_gain = gain.groupby(df["stock_code"]).transform(
            lambda s, w=window: s.rolling(w, min_periods=max(2, w // 2)).mean()
        )
        avg_loss = loss.groupby(df["stock_code"]).transform(
            lambda s, w=window: s.rolling(w, min_periods=max(2, w // 2)).mean()
        )
        df[f"rsi{window}"] = 100.0 * avg_gain / (avg_gain + avg_loss).replace(0, np.nan)
    df["boll_mid"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    df["boll_std"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=10).std())
    df["boll_upper"] = df["boll_mid"] + df["boll_std"] * 2.0
    df["boll_lower"] = df["boll_mid"] - df["boll_std"] * 2.0
    df["boll_width_pct"] = (df["boll_upper"] - df["boll_lower"]) / df["boll_mid"].replace(0, np.nan) * 100.0

    prev_high = grouped["high"].shift(1)
    prev_low = grouped["low"].shift(1)
    prev_close = grouped["close"].shift(1)
    up_move = df["high"] - prev_high
    down_move = prev_low - df["low"]
    df["plus_dm"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    df["minus_dm"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr_parts = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1)
    df["tr"] = tr_parts.max(axis=1)
    df["tr14"] = grouped["tr"].transform(lambda s: s.rolling(14, min_periods=1).sum())
    df["plus_dm14"] = grouped["plus_dm"].transform(lambda s: s.rolling(14, min_periods=1).sum())
    df["minus_dm14"] = grouped["minus_dm"].transform(lambda s: s.rolling(14, min_periods=1).sum())
    df["pdi14"] = 100.0 * df["plus_dm14"] / df["tr14"].replace(0, np.nan)
    df["mdi14"] = 100.0 * df["minus_dm14"] / df["tr14"].replace(0, np.nan)
    df["dx14"] = 100.0 * (df["pdi14"] - df["mdi14"]).abs() / (df["pdi14"] + df["mdi14"]).replace(0, np.nan)
    df["adx14"] = grouped["dx14"].transform(lambda s: s.rolling(14, min_periods=1).mean())

    target = pd.to_datetime(trade_date).date()
    latest = df[df["trade_date"] == target].copy()
    if latest.empty:
        return latest
    chan_rows = []
    for code, code_rows in df.groupby("stock_code"):
        payload = build_chan_structure(code_rows[["trade_date", "high", "low", "close", "macd_hist"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        chan_rows.append(payload)
    if chan_rows:
        latest = latest.merge(pd.DataFrame(chan_rows), on="stock_code", how="left")
    pattern_rows = []
    for code, code_rows in df.groupby("stock_code"):
        payload = detect_kline_pattern(code_rows[["trade_date", "open", "high", "low", "close"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        pattern_rows.append(payload)
    if pattern_rows:
        latest = latest.merge(pd.DataFrame(pattern_rows), on="stock_code", how="left")
    classic_rows = []
    for code, code_rows in df.groupby("stock_code"):
        payload = detect_classic_top_bottom_structure(code_rows[["trade_date", "high", "low", "close"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        classic_rows.append(payload)
    if classic_rows:
        latest = latest.merge(pd.DataFrame(classic_rows), on="stock_code", how="left")
    profile_rows = []
    for code, code_rows in df.groupby("stock_code"):
        payload = estimate_volume_profile_levels(
            code_rows[["trade_date", "high", "low", "close", "amount"]].to_dict(orient="records")
        )
        payload["stock_code"] = str(code).zfill(6)
        profile_rows.append(payload)
    if profile_rows:
        latest = latest.merge(pd.DataFrame(profile_rows), on="stock_code", how="left")
    percentile_rows = []
    for code, code_rows in df.groupby("stock_code"):
        payload = estimate_latest_close_percentile(code_rows[["trade_date", "close"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        percentile_rows.append(payload)
    if percentile_rows:
        latest = latest.merge(pd.DataFrame(percentile_rows), on="stock_code", how="left")
    chase_risk = _build_chase_risk_features_from_rows(chase_source, cutoff)
    if not chase_risk.empty:
        latest = latest.merge(chase_risk, on="stock_code", how="left")
    return latest.reset_index(drop=True)


def _read_intraday_current_quotes(
    engine: Engine,
    trade_date: str,
    *,
    as_of_at: str | datetime,
) -> tuple[pd.DataFrame, str]:
    cutoff = pd.Timestamp(as_of_at)
    if pd.isna(cutoff):
        raise ValueError("intraday quote cutoff must be a valid timestamp")
    if cutoff.tzinfo is not None:
        cutoff = cutoff.tz_convert("Asia/Shanghai").tz_localize(None)
    if cutoff.date() != pd.Timestamp(trade_date).date():
        raise ValueError("intraday quote cutoff must belong to trade_date")
    day_start = f"{trade_date} 00:00:00"
    day_end = (pd.to_datetime(trade_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    cutoff_text = cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")
    sources = [
        (
            "sm_stock_current",
            """
                SELECT stock_code, short_name, price, `change`, change_pct, volume, amount, snapshot_at
                FROM sm_stock_current
                WHERE snapshot_at >= :day_start
                  AND snapshot_at < :day_end
                  AND snapshot_at <= :as_of_at
                  AND price > 0
                ORDER BY stock_code, snapshot_at
            """,
        ),
        (
            "sm_rt_quote_snapshot",
            """
                SELECT q.stock_code, q.short_name, q.price, q.`change`, q.change_pct,
                       q.volume, q.amount, q.snapshot_at
                FROM sm_rt_quote_snapshot q
                JOIN (
                    SELECT stock_code, MAX(snapshot_at) AS snapshot_at
                    FROM sm_rt_quote_snapshot
                    WHERE snapshot_at >= :day_start
                      AND snapshot_at < :day_end
                      AND snapshot_at <= :as_of_at
                    GROUP BY stock_code
                ) x ON x.stock_code = q.stock_code AND x.snapshot_at = q.snapshot_at
                WHERE q.price > 0
            """,
        ),
    ]
    for source, sql in sources:
        try:
            if not _table_exists(engine, source):
                continue
            quotes = _read_frame(
                text(sql),
                engine,
                params={
                    "day_start": day_start,
                    "day_end": day_end,
                    "as_of_at": cutoff_text,
                },
            )
            if not quotes.empty:
                quotes = quotes.copy()
                quotes["snapshot_at"] = pd.to_datetime(
                    quotes["snapshot_at"], errors="coerce"
                )
                quotes = quotes[
                    quotes["snapshot_at"].notna()
                    & quotes["snapshot_at"].ge(pd.Timestamp(day_start))
                    & quotes["snapshot_at"].lt(pd.Timestamp(day_end))
                    & quotes["snapshot_at"].le(cutoff)
                ]
                if quotes.empty:
                    continue
                latest_snapshot = quotes.groupby("stock_code")[
                    "snapshot_at"
                ].transform("max")
                quotes = quotes[quotes["snapshot_at"].eq(latest_snapshot)].copy()
                payload_columns = [
                    column for column in (
                        "short_name", "price", "change", "change_pct",
                        "volume", "amount",
                    )
                    if column in quotes.columns
                ]
                duplicated_latest = quotes.duplicated("stock_code", keep=False)
                if bool(duplicated_latest.any()):
                    conflicting = (
                        quotes.loc[duplicated_latest]
                        .groupby("stock_code")[payload_columns]
                        .nunique(dropna=False)
                        .gt(1)
                        .any(axis=1)
                    )
                    if bool(conflicting.any()):
                        raise RuntimeError(
                            "conflicting intraday quote revisions share the latest snapshot_at"
                        )
                quotes = quotes.sort_values(
                    ["stock_code", "snapshot_at"], kind="mergesort"
                ).drop_duplicates("stock_code", keep="last")
                return quotes, source
        except Exception as exc:
            logger.debug("Intraday current quote source %s skipped: %s", source, exc)
    return pd.DataFrame(), ""


def _read_intraday_path_rows(
    engine: Engine,
    trade_date: str,
    *,
    stock_codes: list[str],
    quote_source: str,
    as_of_at: pd.Timestamp,
) -> pd.DataFrame:
    """Read observable intraday price paths known by one exact cutoff.

    Minute bars are preferred, while the already selected quote source is
    appended as a second path source.  Every query is bounded by the same
    event/knowledge cutoff; an in-place minute revision received later is not
    eligible for replay.
    """
    codes = _normalize_stock_codes(stock_codes)
    if not codes:
        return pd.DataFrame()
    cutoff = _normalize_chase_as_of(as_of_at)
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    day_start = f"{trade_date} 00:00:00"
    frames: list[pd.DataFrame] = []

    try:
        source = minute_source_info()
        table = quote_identifier(str(source.get("table") or "sm_stock_minute"))
        kind = str(source.get("kind") or "legacy").lower()
        if kind == "ohlc":
            value_columns = "open, high, low, close, volume, amount, pre_close"
        else:
            value_columns = (
                "NULL AS open, NULL AS high, NULL AS low, price AS close, "
                "volume, amount, NULL AS pre_close"
            )
        minute_sql = text(f"""
            SELECT stock_code, trade_time AS observed_at,
                   {value_columns}, etl_sync_at AS acquired_at,
                   'minute' AS path_source
            FROM {table}
            WHERE stock_code IN :codes
              AND trade_time >= :day_start
              AND trade_time <= :as_of_at
              AND CASE
                    WHEN etl_sync_at IS NULL THEN trade_time
                    WHEN etl_sync_at >= trade_time THEN etl_sync_at
                    ELSE trade_time
                  END <= :as_of_at
            ORDER BY stock_code, trade_time, etl_sync_at
        """).bindparams(bindparam("codes", expanding=True))
        minute_engine = get_minute_engine()
        for offset in range(0, len(codes), 600):
            part = read_frame(
                minute_sql,
                minute_engine,
                params={
                    "codes": codes[offset : offset + 600],
                    "day_start": day_start,
                    "as_of_at": cutoff_text,
                },
            )
            if not part.empty:
                part["path_kind"] = kind
                frames.append(part)
    except Exception as exc:
        logger.warning("Intraday minute path unavailable at %s: %s", cutoff.isoformat(), exc)

    if quote_source in {"sm_stock_current", "sm_rt_quote_snapshot"}:
        snapshot_sql = text(f"""
            SELECT stock_code, snapshot_at AS observed_at,
                   NULL AS open, NULL AS high, NULL AS low, price AS close,
                   volume, amount, NULL AS pre_close,
                   snapshot_at AS acquired_at, 'snapshot' AS path_source
            FROM {quote_identifier(quote_source)}
            WHERE stock_code IN :codes
              AND snapshot_at >= :day_start
              AND snapshot_at <= :as_of_at
              AND price IS NOT NULL
              AND price > 0
            ORDER BY stock_code, snapshot_at
        """).bindparams(bindparam("codes", expanding=True))
        try:
            for offset in range(0, len(codes), 600):
                part = _read_frame(
                    snapshot_sql,
                    engine,
                    params={
                        "codes": codes[offset : offset + 600],
                        "day_start": day_start,
                        "as_of_at": cutoff_text,
                    },
                )
                if not part.empty:
                    part["path_kind"] = "snapshot"
                    frames.append(part)
        except Exception as exc:
            logger.warning("Intraday snapshot path unavailable at %s: %s", cutoff.isoformat(), exc)

    if not frames:
        return pd.DataFrame()
    paths = pd.concat(frames, ignore_index=True, sort=False)
    paths["stock_code"] = paths["stock_code"].astype(str).str.strip().str.zfill(6)
    paths["observed_at"] = _normalize_acquisition_series(paths["observed_at"])
    paths["acquired_at"] = _normalize_acquisition_series(paths["acquired_at"])
    paths = paths[
        paths["observed_at"].notna()
        & paths["acquired_at"].notna()
        & paths["observed_at"].le(cutoff)
        & paths["acquired_at"].le(cutoff)
    ].copy()
    return paths.sort_values(
        ["stock_code", "observed_at", "acquired_at"], kind="mergesort"
    ).reset_index(drop=True)


def _aggregate_intraday_current_bars(
    quotes: pd.DataFrame,
    path_rows: pd.DataFrame,
    trade_date: str,
    *,
    as_of_at: str | datetime | pd.Timestamp,
) -> pd.DataFrame:
    """Aggregate honest partial-day bars; never manufacture OHLC from one tick."""
    if quotes is None or quotes.empty:
        return pd.DataFrame()
    cutoff = _normalize_chase_as_of(as_of_at, allow_naive_local=True)
    latest_quotes = quotes.copy()
    latest_quotes["stock_code"] = (
        latest_quotes["stock_code"].astype(str).str.strip().str.zfill(6)
    )
    latest_quotes["snapshot_at"] = _normalize_acquisition_series(
        latest_quotes["snapshot_at"]
    )
    latest_quotes = latest_quotes[
        latest_quotes["snapshot_at"].notna()
        & latest_quotes["snapshot_at"].le(cutoff)
    ].sort_values(["stock_code", "snapshot_at"], kind="mergesort")
    latest_quotes = latest_quotes.drop_duplicates("stock_code", keep="last")

    paths = path_rows.copy() if path_rows is not None else pd.DataFrame()
    if not paths.empty:
        paths["stock_code"] = paths["stock_code"].astype(str).str.strip().str.zfill(6)
        paths["observed_at"] = _normalize_acquisition_series(paths["observed_at"])
        paths["acquired_at"] = _normalize_acquisition_series(paths["acquired_at"])
        for column in ("open", "high", "low", "close", "volume", "amount", "pre_close"):
            if column not in paths.columns:
                paths[column] = np.nan
            paths[column] = pd.to_numeric(paths[column], errors="coerce")
        paths = paths[
            paths["observed_at"].notna()
            & paths["acquired_at"].notna()
            & paths["observed_at"].le(cutoff)
            & paths["acquired_at"].le(cutoff)
        ].sort_values(["stock_code", "observed_at", "acquired_at"], kind="mergesort")

    records: list[dict[str, Any]] = []
    for quote in latest_quotes.to_dict(orient="records"):
        stock_code = str(quote.get("stock_code") or "").zfill(6)
        code_paths = (
            paths[paths["stock_code"].eq(stock_code)].copy()
            if not paths.empty
            else pd.DataFrame()
        )
        quote_price = _safe_number(quote.get("price"), 0.0)
        snapshot_at = pd.Timestamp(quote["snapshot_at"])
        if quote_price > 0:
            quote_path = pd.DataFrame([{
                "stock_code": stock_code,
                "observed_at": snapshot_at,
                "acquired_at": snapshot_at,
                "open": np.nan,
                "high": np.nan,
                "low": np.nan,
                "close": quote_price,
                "volume": _none_if_nan(quote.get("volume")),
                "amount": _none_if_nan(quote.get("amount")),
                "pre_close": np.nan,
                "path_source": "selected_quote",
                "path_kind": "snapshot",
            }])
            code_paths = pd.concat([code_paths, quote_path], ignore_index=True, sort=False)
        code_paths = code_paths.sort_values(
            ["observed_at", "acquired_at"], kind="mergesort"
        ).drop_duplicates(["observed_at", "close"], keep="last")

        reliable_open = False
        open_price = high_price = low_price = np.nan
        if not code_paths.empty:
            first = code_paths.iloc[0]
            first_time = pd.Timestamp(first["observed_at"]).tz_convert(
                CHINA_MARKET_TIMEZONE
            )
            first_open = _safe_number(first.get("open"), 0.0)
            has_bar_ohlc = bool(
                first_open > 0
                and _safe_number(first.get("high"), 0.0) > 0
                and _safe_number(first.get("low"), 0.0) > 0
            )
            if has_bar_ohlc and first_time.time() <= datetime.strptime("09:31:00", "%H:%M:%S").time():
                open_price = first_open
                reliable_open = True
            elif first_time.time() <= datetime.strptime("09:30:30", "%H:%M:%S").time():
                open_price = _safe_number(first.get("close"), 0.0)
                reliable_open = open_price > 0

            high_candidates = pd.concat(
                [code_paths["high"], code_paths["close"]], ignore_index=True
            )
            low_candidates = pd.concat(
                [code_paths["low"], code_paths["close"]], ignore_index=True
            )
            high_candidates = pd.to_numeric(high_candidates, errors="coerce")
            low_candidates = pd.to_numeric(low_candidates, errors="coerce")
            high_candidates = high_candidates[high_candidates > 0]
            low_candidates = low_candidates[low_candidates > 0]
            if not high_candidates.empty:
                high_price = float(high_candidates.max())
            if not low_candidates.empty:
                low_price = float(low_candidates.min())

        pre_close = quote_price - _safe_number(quote.get("change"), 0.0)
        if pre_close <= 0:
            change_pct = _safe_number(quote.get("change_pct"), 0.0)
            factor = 1.0 + change_pct / 100.0
            pre_close = quote_price / factor if quote_price > 0 and factor > 0 else np.nan
        ohlc_known = bool(
            reliable_open
            and quote_price > 0
            and not pd.isna(high_price)
            and not pd.isna(low_price)
            and high_price >= max(open_price, low_price, quote_price)
            and low_price <= min(open_price, high_price, quote_price)
        )
        records.append({
            "stock_code": stock_code,
            "short_name": str(quote.get("short_name") or ""),
            "trade_date": trade_date,
            "open": open_price if ohlc_known else np.nan,
            "high": high_price if ohlc_known else np.nan,
            "low": low_price if ohlc_known else np.nan,
            "close": quote_price,
            "volume": _none_if_nan(quote.get("volume")),
            "amount": _none_if_nan(quote.get("amount")),
            "change_pct": _none_if_nan(quote.get("change_pct")),
            # No outstanding-share denominator is available in these path
            # sources, so zero would be fabricated evidence.
            "turnover_ratio": np.nan,
            "pre_close": pre_close,
            "received_at": snapshot_at,
            "etl_sync_at": snapshot_at,
            "intraday_ohlc_status": "KNOWN" if ohlc_known else "DATA_BLOCKED",
        })
    return pd.DataFrame(records)


def _expected_intraday_universe_size(engine: Engine, latest_history_date: str) -> int:
    try:
        row = _read_frame(
            text("""
                SELECT COUNT(DISTINCT stock_code) AS cnt
                FROM sm_stock_kline
                WHERE k_type = 1
                  AND adjust_type = 0
                  AND trade_date = :trade_date
            """),
            engine,
            params={"trade_date": latest_history_date},
        )
        if not row.empty:
            return int(row.iloc[0].get("cnt") or 0)
    except Exception as exc:
        logger.debug("Intraday universe size lookup failed: %s", exc)
    return 0


def _intraday_min_coverage() -> float:
    try:
        return max(0.0, min(1.0, float(os.environ.get("PROBIGA_INTRADAY_RECOMMEND_MIN_COVERAGE", "0.70"))))
    except (TypeError, ValueError):
        return 0.70


def _load_intraday_current_kline_features(
    engine: Engine,
    trade_date: str,
    lookback: int = 260,
    progress_callback: ProgressCallback | None = None,
    as_of_at: str | datetime | None = None,
) -> pd.DataFrame:
    if as_of_at is None:
        raise ValueError("intraday K-line features require an explicit as_of_at cutoff")
    cutoff = _normalize_chase_as_of(as_of_at, allow_naive_local=True)
    if cutoff.date() != pd.Timestamp(trade_date).date():
        raise ValueError("intraday K-line cutoff must belong to trade_date")
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    dates = _recent_dates(
        engine,
        "sm_stock_kline",
        "trade_date",
        trade_date,
        max(int(lookback), 260) + 1,
        as_of_at=cutoff_text,
    )
    history_dates = [d for d in dates if d < trade_date]
    if not history_dates:
        raise RuntimeError(f"No historical K-line dates found before intraday trade date {trade_date}")
    latest_history_date = history_dates[0]
    expected = _expected_intraday_universe_size(engine, latest_history_date)

    quotes, quote_source = _read_intraday_current_quotes(
        engine,
        trade_date,
        as_of_at=cutoff_text,
    )
    if quotes.empty:
        raise RuntimeError(f"No intraday current quote rows found for {trade_date}")
    quotes = quotes.copy()
    quotes["stock_code"] = quotes["stock_code"].astype(str).str.strip().str.zfill(6)
    quotes = quotes.drop_duplicates("stock_code", keep="last")
    min_coverage = _intraday_min_coverage()
    if expected > 0 and len(quotes) < int(expected * min_coverage):
        raise RuntimeError(
            f"Intraday quote coverage below threshold for {trade_date}: "
            f"{len(quotes)}/{expected} ({len(quotes) / max(expected, 1):.1%}) < {min_coverage:.1%}"
        )

    path_rows = _read_intraday_path_rows(
        engine,
        trade_date,
        stock_codes=quotes["stock_code"].tolist(),
        quote_source=quote_source,
        as_of_at=cutoff,
    )
    current = _aggregate_intraday_current_bars(
        quotes,
        path_rows,
        trade_date,
        as_of_at=cutoff,
    )
    current = current.dropna(subset=["stock_code", "close"])
    current = current[current["close"] > 0]
    if current.empty:
        raise RuntimeError(f"No valid intraday current quote prices found for {trade_date}")

    start_date = history_dates[min(max(int(lookback), 260) - 1, len(history_dates) - 1)]
    names = _read_frame(text("SELECT stock_code, short_name FROM si_all_code"), engine)
    try:
        batch_size = max(
            50,
            int(os.environ.get(
                "PROBIGA_INTRADAY_KLINE_FEATURE_BATCH_SIZE",
                os.environ.get("PROBIGA_KLINE_FEATURE_BATCH_SIZE", "600"),
            )),
        )
    except (TypeError, ValueError):
        batch_size = 80
    codes = (
        current["stock_code"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.zfill(6)
        .drop_duplicates()
        .tolist()
    )
    hist_sql = text("""
        SELECT
          stock_code,
          COALESCE(NULLIF(short_name, ''), '') AS short_name,
          trade_date,
          open, high, low, close,
          volume, amount, change_pct, turnover_ratio, pre_close,
          received_at, etl_sync_at
        FROM sm_stock_kline
        WHERE stock_code IN :codes
          AND k_type = 1
          AND adjust_type = 0
          AND trade_date >= :start_date
          AND trade_date < :trade_date
          AND CASE
                WHEN received_at IS NULL AND etl_sync_at IS NULL THEN NULL
                WHEN received_at IS NULL THEN etl_sync_at
                WHEN etl_sync_at IS NULL THEN received_at
                WHEN received_at >= etl_sync_at THEN received_at
                ELSE etl_sync_at
              END <= :as_of_at
        ORDER BY stock_code, trade_date, received_at, etl_sync_at
    """).bindparams(bindparam("codes", expanding=True))
    latest_frames: list[pd.DataFrame] = []
    total_batches = math.ceil(len(codes) / batch_size) if codes else 0
    logger.info(
        "Loading intraday current K-line features via batches: codes=%s batch_size=%s batches=%s source=%s",
        len(codes),
        batch_size,
        total_batches,
        quote_source,
    )
    for batch_no, offset in enumerate(range(0, len(codes), batch_size), start=1):
        batch_codes = codes[offset : offset + batch_size]
        hist = _read_frame(
            hist_sql,
            engine,
            params={
                "start_date": start_date,
                "trade_date": trade_date,
                "codes": batch_codes,
                "as_of_at": cutoff_text,
            },
        )
        current_part = current[current["stock_code"].isin(batch_codes)].copy()
        if hist.empty or current_part.empty:
            continue
        combined = pd.concat([hist, current_part], ignore_index=True, sort=False)
        part_latest = _build_latest_kline_features_from_rows(
            combined,
            trade_date,
            names,
            as_of_at=cutoff,
        )
        if not part_latest.empty:
            latest_frames.append(part_latest)
        if batch_no == 1 or batch_no == total_batches or batch_no % 5 == 0:
            logger.info(
                "Loaded intraday K-line feature batch %s/%s history_rows=%s latest=%s",
                batch_no,
                total_batches,
                len(hist),
                len(part_latest),
            )
            _emit_progress(
                progress_callback,
                stage="load_kline_intraday_batch",
                percent=min(13, 5 + int(8 * batch_no / max(total_batches, 1))),
                step=f"加载盘中实时K线特征 {batch_no}/{total_batches}",
                trade_date=trade_date,
                done=batch_no,
                total=total_batches,
            )
        del hist, current_part, combined, part_latest
        if batch_no % 10 == 0:
            gc.collect()
    latest = pd.concat(latest_frames, ignore_index=True) if latest_frames else pd.DataFrame()
    if latest.empty:
        raise RuntimeError(f"Intraday current K-line feature build returned no rows for {trade_date}")
    logger.info(
        "Loaded intraday current K-line features for %s from %s quotes=%s expected=%s history_start=%s",
        trade_date,
        quote_source,
        len(current),
        expected,
        start_date,
    )
    return latest.drop_duplicates("stock_code", keep="last").reset_index(drop=True)


def load_kline_features(
    engine: Engine,
    trade_date: str,
    lookback: int = 260,
    use_intraday_current: bool = False,
    progress_callback: ProgressCallback | None = None,
    as_of_at: str | datetime | None = None,
) -> pd.DataFrame:
    if use_intraday_current:
        return _load_intraday_current_kline_features(
            engine,
            trade_date,
            lookback=lookback,
            progress_callback=progress_callback,
            as_of_at=as_of_at,
        )
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    dates = _recent_dates(
        engine,
        "sm_stock_kline",
        "trade_date",
        trade_date,
        max(int(lookback), 260),
        as_of_at=cutoff_text,
    )
    if not dates:
        raise RuntimeError(f"No K-line dates found before {trade_date}")
    start_date = dates[-1]
    kline_engine = _query_engine(engine, "SELECT * FROM sm_stock_kline")
    kline_range_hint = _mysql_force_index_hint(
        kline_engine,
        "sm_stock_kline",
        "idx_kline_type_adjust_date_code",
        "idx_date_ktype",
    )
    kline_code_hint = _mysql_force_index_hint(
        kline_engine,
        "sm_stock_kline",
        "idx_kline_code_type_date",
        "idx_sk_code_date",
    )
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
        fast = _read_frame(
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
          COALESCE(NULLIF(k.short_name, ''), '') AS short_name,
          k.trade_date,
          k.open, k.high, k.low, k.close,
          k.volume, k.amount, k.change_pct, k.turnover_ratio, k.pre_close
        FROM sm_stock_kline k
        WHERE k.k_type = 1
          AND k.adjust_type = 0
          AND k.trade_date = :trade_date
    """
    agg_sql = f"""
        SELECT
          stock_code,
          AVG(CASE WHEN trade_date >= :ma5_start THEN close END) AS ma5,
          AVG(CASE WHEN trade_date >= :ma10_start THEN close END) AS ma10,
          AVG(CASE WHEN trade_date >= :ma20_start THEN close END) AS ma20,
          AVG(CASE WHEN trade_date >= :ma60_start THEN close END) AS ma60,
          AVG(CASE WHEN trade_date >= :ma120_start THEN close END) AS ma120,
          AVG(CASE WHEN trade_date >= :ma250_start THEN close END) AS ma250,
          AVG(CASE WHEN trade_date >= :ma5_start THEN amount END) AS amount_ma5,
          AVG(CASE WHEN trade_date >= :ma20_start THEN amount END) AS amount_ma20,
          STDDEV_SAMP(CASE WHEN trade_date >= :ma20_start THEN change_pct END) AS volatility_20,
          MAX(CASE WHEN trade_date >= :ma20_start THEN high END) AS high_20,
          MAX(CASE WHEN trade_date >= :ma60_start THEN high END) AS high_60,
          MIN(CASE WHEN trade_date >= :ma60_start THEN low END) AS low_60
        FROM sm_stock_kline{kline_range_hint}
        WHERE k_type = 1
          AND adjust_type = 0
          AND trade_date >= :ma250_start
          AND trade_date <= :trade_date
        GROUP BY stock_code
    """
    try:
        # The aggregate shortcut cannot prove revision ordering per stock/date.
        # Keep it disabled until it is rewritten around an immutable revision
        # ledger; the streaming path below enforces acquisition-time PIT rules.
        raise RuntimeError(
            "grouped K-line feature query disabled; using revision-safe streaming batches"
        )
        latest = _read_frame(text(latest_sql), engine, params={"trade_date": trade_date})
        if not latest.empty:
            names = _read_frame(
                text("SELECT stock_code, short_name FROM si_all_code"),
                engine,
            )
            if not names.empty:
                latest["stock_code"] = latest["stock_code"].astype(str).str.strip().str.zfill(6)
                names["stock_code"] = names["stock_code"].astype(str).str.strip().str.zfill(6)
                names = names.drop_duplicates("stock_code", keep="last").rename(columns={"short_name": "name_from_code"})
                latest = latest.merge(names, on="stock_code", how="left")
                latest["short_name"] = latest["short_name"].replace("", np.nan).fillna(latest["name_from_code"]).fillna("")
                latest = latest.drop(columns=["name_from_code"])
            agg = _read_frame(
                text(agg_sql),
                engine,
                params={
                    "trade_date": trade_date,
                    "ma5_start": _date_at(4),
                    "ma10_start": _date_at(9),
                    "ma20_start": _date_at(19),
                    "ma60_start": _date_at(59),
                    "ma120_start": _date_at(119),
                    "ma250_start": _date_at(249),
                },
            )
            latest["stock_code"] = latest["stock_code"].astype(str).str.strip().str.zfill(6)
            agg["stock_code"] = agg["stock_code"].astype(str).str.strip().str.zfill(6)
            out = latest.merge(agg, on="stock_code", how="left")
            lag_frames = []
            for lag_name, offset in (("close_5d_ago", 5), ("close_20d_ago", 20), ("close_60d_ago", 60)):
                if len(dates) > offset:
                    lag = _read_frame(
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
                "turnover_ratio", "pre_close", "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
                "amount_ma5", "amount_ma20", "volatility_20", "high_20", "high_60", "low_60",
                "close_5d_ago", "close_20d_ago", "close_60d_ago",
            ]
            for col in numeric_cols:
                if col in out.columns:
                    out[col] = pd.to_numeric(out[col], errors="coerce")
            if "close_5d_ago" not in out.columns:
                out["close_5d_ago"] = np.nan
            if "close_20d_ago" not in out.columns:
                out["close_20d_ago"] = np.nan
            if "close_60d_ago" not in out.columns:
                out["close_60d_ago"] = np.nan
            out["pct_5"] = (out["close"] / out.get("close_5d_ago").replace(0, np.nan) - 1.0) * 100.0
            out["pct_20"] = (out["close"] / out.get("close_20d_ago").replace(0, np.nan) - 1.0) * 100.0
            out["deduction_price_20"] = out["close_20d_ago"]
            out["deduction_price_60"] = out["close_60d_ago"]
            out["deduction_date_20"] = _date_at(20) if len(dates) > 20 else ""
            out["deduction_date_60"] = _date_at(60) if len(dates) > 60 else ""
            out["drawdown_60"] = (out["close"] / out["high_60"].replace(0, np.nan) - 1.0) * 100.0
            out["from_low_60"] = (out["close"] / out["low_60"].replace(0, np.nan) - 1.0) * 100.0
            out["dist_ma20"] = (out["close"] / out["ma20"].replace(0, np.nan) - 1.0) * 100.0
            out["amount_ratio_5"] = out["amount"] / out["amount_ma5"].replace(0, np.nan)
            out["amount_ratio_20"] = out["amount"] / out["amount_ma20"].replace(0, np.nan)
            ema = _read_frame(
                text(f"""
                    SELECT stock_code, trade_date, open, high, low, close,
                           volume, amount, change_pct, pre_close
                    FROM sm_stock_kline{kline_range_hint}
                    WHERE k_type = 1
                      AND adjust_type = 0
                      AND trade_date >= :start_date
                      AND trade_date <= :trade_date
                    ORDER BY stock_code, trade_date
                """),
                engine,
                params={"start_date": start_date, "trade_date": trade_date},
            )
            if not ema.empty:
                ema["stock_code"] = ema["stock_code"].astype(str).str.strip().str.zfill(6)
                ema["trade_date"] = pd.to_datetime(ema["trade_date"])
                for col in ("open", "high", "low", "close", "volume", "amount", "change_pct", "pre_close"):
                    ema[col] = pd.to_numeric(ema[col], errors="coerce")
                ema["open"] = ema["open"].fillna(ema["close"])
                ema["high"] = ema["high"].fillna(ema["close"])
                ema["low"] = ema["low"].fillna(ema["close"])
                ema = ema.dropna(subset=["close"]).sort_values(["stock_code", "trade_date"])
                ema_grouped = ema.groupby("stock_code", group_keys=False)
                ema["ema12"] = ema_grouped["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
                ema["ema26"] = ema_grouped["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
                ema["dif"] = ema["ema12"] - ema["ema26"]
                ema["dea"] = ema_grouped["dif"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
                ema["macd_hist"] = (ema["dif"] - ema["dea"]) * 2.0
                ema["macd_dif"] = ema["dif"]
                ema["macd_dea"] = ema["dea"]
                ema["ma3_calc"] = ema_grouped["close"].transform(lambda s: s.rolling(3, min_periods=1).mean())
                ema["ma6_calc"] = ema_grouped["close"].transform(lambda s: s.rolling(6, min_periods=1).mean())
                ema["ma12_calc"] = ema_grouped["close"].transform(lambda s: s.rolling(12, min_periods=1).mean())
                ema["ma24_calc"] = ema_grouped["close"].transform(lambda s: s.rolling(24, min_periods=1).mean())
                ema["bbi"] = (ema["ma3_calc"] + ema["ma6_calc"] + ema["ma12_calc"] + ema["ma24_calc"]) / 4.0
                ema["bias6"] = (ema["close"] / ema["ma6_calc"].replace(0, np.nan) - 1.0) * 100.0
                ema["bias12"] = (ema["close"] / ema["ma12_calc"].replace(0, np.nan) - 1.0) * 100.0
                ema["bias24"] = (ema["close"] / ema["ma24_calc"].replace(0, np.nan) - 1.0) * 100.0
                close_10 = ema_grouped["close"].shift(10)
                ema["mtm10"] = ema["close"] - close_10
                ema["mtm10_pct"] = (ema["close"] / close_10.replace(0, np.nan) - 1.0) * 100.0
                high_9 = ema_grouped["high"].transform(lambda s: s.rolling(9, min_periods=1).max())
                low_9 = ema_grouped["low"].transform(lambda s: s.rolling(9, min_periods=1).min())
                ema["lwr9"] = ((high_9 - ema["close"]) / (high_9 - low_9).replace(0, np.nan)) * 100.0
                ema["kdj_rsv"] = ((ema["close"] - low_9) / (high_9 - low_9).replace(0, np.nan)) * 100.0
                ema["kdj_k"] = ema_grouped["kdj_rsv"].transform(
                    lambda s: s.fillna(50.0).ewm(alpha=1 / 3, adjust=False).mean()
                )
                ema["kdj_d"] = ema_grouped["kdj_k"].transform(lambda s: s.ewm(alpha=1 / 3, adjust=False).mean())
                ema["kdj_j"] = 3.0 * ema["kdj_k"] - 2.0 * ema["kdj_d"]
                delta = ema_grouped["close"].diff()
                gain = delta.clip(lower=0)
                loss = (-delta.clip(upper=0))
                for window in (6, 12, 24):
                    avg_gain = gain.groupby(ema["stock_code"]).transform(
                        lambda s, w=window: s.rolling(w, min_periods=max(2, w // 2)).mean()
                    )
                    avg_loss = loss.groupby(ema["stock_code"]).transform(
                        lambda s, w=window: s.rolling(w, min_periods=max(2, w // 2)).mean()
                    )
                    ema[f"rsi{window}"] = 100.0 * avg_gain / (avg_gain + avg_loss).replace(0, np.nan)
                ema["boll_mid"] = ema_grouped["close"].transform(lambda s: s.rolling(20, min_periods=10).mean())
                ema["boll_std"] = ema_grouped["close"].transform(lambda s: s.rolling(20, min_periods=10).std())
                ema["boll_upper"] = ema["boll_mid"] + ema["boll_std"] * 2.0
                ema["boll_lower"] = ema["boll_mid"] - ema["boll_std"] * 2.0
                ema["boll_width_pct"] = (ema["boll_upper"] - ema["boll_lower"]) / ema["boll_mid"].replace(0, np.nan) * 100.0

                prev_high = ema_grouped["high"].shift(1)
                prev_low = ema_grouped["low"].shift(1)
                prev_close = ema_grouped["close"].shift(1)
                up_move = ema["high"] - prev_high
                down_move = prev_low - ema["low"]
                ema["plus_dm"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
                ema["minus_dm"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
                tr_parts = pd.concat([
                    (ema["high"] - ema["low"]).abs(),
                    (ema["high"] - prev_close).abs(),
                    (ema["low"] - prev_close).abs(),
                ], axis=1)
                ema["tr"] = tr_parts.max(axis=1)
                ema["tr14"] = ema_grouped["tr"].transform(lambda s: s.rolling(14, min_periods=1).sum())
                ema["plus_dm14"] = ema_grouped["plus_dm"].transform(lambda s: s.rolling(14, min_periods=1).sum())
                ema["minus_dm14"] = ema_grouped["minus_dm"].transform(lambda s: s.rolling(14, min_periods=1).sum())
                ema["pdi14"] = 100.0 * ema["plus_dm14"] / ema["tr14"].replace(0, np.nan)
                ema["mdi14"] = 100.0 * ema["minus_dm14"] / ema["tr14"].replace(0, np.nan)
                ema["dx14"] = 100.0 * (ema["pdi14"] - ema["mdi14"]).abs() / (ema["pdi14"] + ema["mdi14"]).replace(0, np.nan)
                ema["adx14"] = ema_grouped["dx14"].transform(lambda s: s.rolling(14, min_periods=1).mean())
                latest_ema = ema.groupby("stock_code", as_index=False).tail(1)[[
                    "stock_code", "ema12", "ema26", "bbi", "bias6", "bias12", "bias24",
                    "mtm10", "mtm10_pct", "lwr9", "macd_dif", "macd_dea", "macd_hist",
                    "kdj_k", "kdj_d", "kdj_j", "rsi6", "rsi12", "rsi24",
                    "boll_mid", "boll_upper", "boll_lower", "boll_width_pct",
                    "pdi14", "mdi14", "adx14",
                ]]
                out = out.merge(latest_ema, on="stock_code", how="left")
                chan_rows = []
                for code, rows in ema.groupby("stock_code"):
                    payload = build_chan_structure(rows[["trade_date", "high", "low", "close", "macd_hist"]].to_dict(orient="records"))
                    payload["stock_code"] = str(code).zfill(6)
                    chan_rows.append(payload)
                if chan_rows:
                    out = out.merge(pd.DataFrame(chan_rows), on="stock_code", how="left")
                pattern_rows = []
                for code, rows in ema.groupby("stock_code"):
                    payload = detect_kline_pattern(rows[["trade_date", "open", "high", "low", "close"]].to_dict(orient="records"))
                    payload["stock_code"] = str(code).zfill(6)
                    pattern_rows.append(payload)
                if pattern_rows:
                    out = out.merge(pd.DataFrame(pattern_rows), on="stock_code", how="left")
                classic_rows = []
                for code, rows in ema.groupby("stock_code"):
                    payload = detect_classic_top_bottom_structure(rows[["trade_date", "high", "low", "close"]].to_dict(orient="records"))
                    payload["stock_code"] = str(code).zfill(6)
                    classic_rows.append(payload)
                if classic_rows:
                    out = out.merge(pd.DataFrame(classic_rows), on="stock_code", how="left")
                profile_rows = []
                for code, rows in ema.groupby("stock_code"):
                    payload = estimate_volume_profile_levels(
                        rows[["trade_date", "high", "low", "close", "amount"]].to_dict(orient="records")
                    )
                    payload["stock_code"] = str(code).zfill(6)
                    profile_rows.append(payload)
                if profile_rows:
                    out = out.merge(pd.DataFrame(profile_rows), on="stock_code", how="left")
                percentile_rows = []
                for code, rows in ema.groupby("stock_code"):
                    payload = estimate_latest_close_percentile(
                        rows[["trade_date", "close"]].to_dict(orient="records")
                    )
                    payload["stock_code"] = str(code).zfill(6)
                    percentile_rows.append(payload)
                if percentile_rows:
                    out = out.merge(pd.DataFrame(percentile_rows), on="stock_code", how="left")
                chase_risk = _build_chase_risk_features_from_rows(ema, trade_date)
                if not chase_risk.empty:
                    out = out.merge(chase_risk, on="stock_code", how="left")
            out["short_name"] = out["short_name"].fillna("").astype(str)
            out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
            drop_cols = [c for c in ("close_5d_ago", "close_20d_ago", "close_60d_ago") if c in out.columns]
            if drop_cols:
                out = out.drop(columns=drop_cols)
            return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)
    except Exception as exc:
        logger.warning("Grouped K-line feature query failed, falling back to pandas rolling: %s", exc)

    code_sql = f"""
        SELECT DISTINCT stock_code
        FROM sm_stock_kline{kline_range_hint}
        WHERE k_type = 1
          AND adjust_type = 0
          AND trade_date = :trade_date
          AND CASE
                WHEN received_at IS NULL AND etl_sync_at IS NULL THEN NULL
                WHEN received_at IS NULL THEN etl_sync_at
                WHEN etl_sync_at IS NULL THEN received_at
                WHEN received_at >= etl_sync_at THEN received_at
                ELSE etl_sync_at
              END <= :as_of_at
    """
    code_df = _read_frame(
        text(code_sql),
        engine,
        params={"trade_date": trade_date, "as_of_at": cutoff_text},
    )
    codes = (
        code_df.get("stock_code", pd.Series(dtype=str))
        .dropna()
        .astype(str)
        .str.strip()
        .str.zfill(6)
        .drop_duplicates()
        .tolist()
    )
    if not codes:
        raise RuntimeError(f"No K-line rows found for {trade_date}")
    try:
        batch_size = max(50, int(os.environ.get("PROBIGA_KLINE_FEATURE_BATCH_SIZE", "300")))
    except (TypeError, ValueError):
        batch_size = 300
    sql = text(f"""
        SELECT
          stock_code,
          COALESCE(NULLIF(short_name, ''), '') AS short_name,
          trade_date,
          open, high, low, close,
          volume, amount, change_pct, turnover_ratio, pre_close,
          received_at, etl_sync_at
        FROM sm_stock_kline{kline_code_hint}
        WHERE stock_code IN :codes
          AND k_type = 1
          AND adjust_type = 0
          AND trade_date >= :start_date
          AND trade_date <= :trade_date
          AND CASE
                WHEN received_at IS NULL AND etl_sync_at IS NULL THEN NULL
                WHEN received_at IS NULL THEN etl_sync_at
                WHEN etl_sync_at IS NULL THEN received_at
                WHEN received_at >= etl_sync_at THEN received_at
                ELSE etl_sync_at
              END <= :as_of_at
        ORDER BY stock_code, trade_date, received_at, etl_sync_at
    """).bindparams(bindparam("codes", expanding=True))
    stream_batches = os.environ.get("PROBIGA_KLINE_FEATURE_STREAM_BATCHES", "1").strip().lower() not in {
        "0",
        "false",
        "no",
    }
    if stream_batches:
        names = _read_frame(text("SELECT stock_code, short_name FROM si_all_code"), engine)
        latest_frames: list[pd.DataFrame] = []
        total_batches = math.ceil(len(codes) / batch_size)
        logger.info(
            "Loading K-line features via streaming batched pandas path: codes=%s batch_size=%s batches=%s",
            len(codes),
            batch_size,
            total_batches,
        )
        for batch_no, offset in enumerate(range(0, len(codes), batch_size), start=1):
            batch_codes = codes[offset : offset + batch_size]
            part = _read_frame(
                sql,
                engine,
                params={
                    "start_date": start_date,
                    "trade_date": trade_date,
                    "codes": batch_codes,
                    "as_of_at": cutoff_text,
                },
            )
            part_latest = _build_latest_kline_features_from_rows(
                part,
                trade_date,
                names,
                as_of_at=cutoff,
            )
            if not part_latest.empty:
                latest_frames.append(part_latest)
            if batch_no == 1 or batch_no == total_batches or batch_no % 5 == 0:
                logger.info(
                    "Loaded K-line feature batch %s/%s rows=%s latest=%s",
                    batch_no,
                    total_batches,
                    len(part),
                    len(part_latest),
                )
                _emit_progress(
                    progress_callback,
                    stage="load_kline_batch",
                    percent=min(13, 5 + int(8 * batch_no / max(total_batches, 1))),
                    step=f"加载日K特征 {batch_no}/{total_batches}",
                    trade_date=trade_date,
                    done=batch_no,
                    total=total_batches,
                )
            del part, part_latest
            if batch_no % 10 == 0:
                gc.collect()
        latest = pd.concat(latest_frames, ignore_index=True) if latest_frames else pd.DataFrame()
        if latest.empty:
            raise RuntimeError(f"No K-line rows exactly on {trade_date}")
        return latest.drop_duplicates("stock_code", keep="last").reset_index(drop=True)

    frames: list[pd.DataFrame] = []
    total_batches = math.ceil(len(codes) / batch_size)
    logger.info(
        "Loading K-line features via batched pandas path: codes=%s batch_size=%s batches=%s",
        len(codes),
        batch_size,
        total_batches,
    )
    for batch_no, offset in enumerate(range(0, len(codes), batch_size), start=1):
        batch_codes = codes[offset : offset + batch_size]
        part = _read_frame(
            sql,
            engine,
            params={
                "start_date": start_date,
                "trade_date": trade_date,
                "codes": batch_codes,
                "as_of_at": cutoff_text,
            },
        )
        if not part.empty:
            frames.append(part)
        if batch_no == 1 or batch_no == total_batches or batch_no % 5 == 0:
            logger.info(
                "Loaded K-line feature batch %s/%s rows=%s",
                batch_no,
                total_batches,
                len(part),
            )
            _emit_progress(
                progress_callback,
                stage="load_kline_batch",
                percent=min(13, 5 + int(8 * batch_no / max(total_batches, 1))),
                step=f"加载日K特征 {batch_no}/{total_batches}",
                trade_date=trade_date,
                done=batch_no,
                total=total_batches,
            )
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        raise RuntimeError(f"No K-line rows found for {trade_date}")
    names = _read_frame(text("SELECT stock_code, short_name FROM si_all_code"), engine)
    if not names.empty:
        df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
        names["stock_code"] = names["stock_code"].astype(str).str.strip().str.zfill(6)
        names = names.drop_duplicates("stock_code", keep="last").rename(columns={"short_name": "name_from_code"})
        df = df.merge(names, on="stock_code", how="left")
        df["short_name"] = df["short_name"].replace("", np.nan).fillna(df["name_from_code"]).fillna("")
        df = df.drop(columns=["name_from_code"])

    chase_source = df.copy()
    df, acquisition_columns = _attach_effective_acquisition_time(df)

    numeric_cols = [
        "open", "high", "low", "close", "volume", "amount", "change_pct",
        "turnover_ratio", "pre_close",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["short_name"] = df["short_name"].fillna("").astype(str)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    if acquisition_columns:
        df = df.sort_values(
            ["stock_code", "trade_date", "_chase_acquired_at", *acquisition_columns],
            kind="mergesort",
        )
    df = df.drop_duplicates(["stock_code", "trade_date"], keep="last")
    df = df.drop(columns=["_chase_acquired_at"], errors="ignore")
    df = df.sort_values(["stock_code", "trade_date"], kind="mergesort")

    grouped = df.groupby("stock_code", group_keys=False)
    for window in (5, 10, 20, 60, 120, 250):
        min_periods = max(3, min(window, window // 2))
        df[f"ma{window}"] = grouped["close"].transform(lambda s, w=window, m=min_periods: s.rolling(w, min_periods=m).mean())
    df["ema12"] = grouped["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    df["ema26"] = grouped["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    df["amount_ma5"] = grouped["amount"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    df["amount_ma20"] = grouped["amount"].transform(lambda s: s.rolling(20, min_periods=8).mean())
    df["pct_5"] = grouped["close"].pct_change(5) * 100.0
    df["pct_20"] = grouped["close"].pct_change(20) * 100.0
    df["deduction_price_20"] = grouped["close"].shift(20)
    df["deduction_price_60"] = grouped["close"].shift(60)
    df["deduction_date_20"] = grouped["trade_date"].shift(20)
    df["deduction_date_60"] = grouped["trade_date"].shift(60)
    df["volatility_20"] = grouped["change_pct"].transform(lambda s: s.rolling(20, min_periods=8).std())
    df["high_20"] = grouped["high"].transform(lambda s: s.rolling(20, min_periods=8).max())
    df["high_60"] = grouped["high"].transform(lambda s: s.rolling(60, min_periods=20).max())
    df["low_60"] = grouped["low"].transform(lambda s: s.rolling(60, min_periods=20).min())
    df["drawdown_60"] = (df["close"] / df["high_60"] - 1.0) * 100.0
    df["from_low_60"] = (df["close"] / df["low_60"] - 1.0) * 100.0
    df["dist_ma20"] = (df["close"] / df["ma20"] - 1.0) * 100.0
    df["amount_ratio_5"] = df["amount"] / df["amount_ma5"].replace(0, np.nan)
    df["amount_ratio_20"] = df["amount"] / df["amount_ma20"].replace(0, np.nan)
    df["dif"] = df["ema12"] - df["ema26"]
    df["dea"] = grouped["dif"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = (df["dif"] - df["dea"]) * 2.0
    df["macd_dif"] = df["dif"]
    df["macd_dea"] = df["dea"]
    df["ma3_calc"] = grouped["close"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    df["ma6_calc"] = grouped["close"].transform(lambda s: s.rolling(6, min_periods=1).mean())
    df["ma12_calc"] = grouped["close"].transform(lambda s: s.rolling(12, min_periods=1).mean())
    df["ma24_calc"] = grouped["close"].transform(lambda s: s.rolling(24, min_periods=1).mean())
    df["bbi"] = (df["ma3_calc"] + df["ma6_calc"] + df["ma12_calc"] + df["ma24_calc"]) / 4.0
    df["bias6"] = (df["close"] / df["ma6_calc"].replace(0, np.nan) - 1.0) * 100.0
    df["bias12"] = (df["close"] / df["ma12_calc"].replace(0, np.nan) - 1.0) * 100.0
    df["bias24"] = (df["close"] / df["ma24_calc"].replace(0, np.nan) - 1.0) * 100.0
    close_10 = grouped["close"].shift(10)
    df["mtm10"] = df["close"] - close_10
    df["mtm10_pct"] = (df["close"] / close_10.replace(0, np.nan) - 1.0) * 100.0
    high_9 = grouped["high"].transform(lambda s: s.rolling(9, min_periods=1).max())
    low_9 = grouped["low"].transform(lambda s: s.rolling(9, min_periods=1).min())
    df["lwr9"] = ((high_9 - df["close"]) / (high_9 - low_9).replace(0, np.nan)) * 100.0
    df["kdj_rsv"] = ((df["close"] - low_9) / (high_9 - low_9).replace(0, np.nan)) * 100.0
    df["kdj_k"] = grouped["kdj_rsv"].transform(
        lambda s: s.fillna(50.0).ewm(alpha=1 / 3, adjust=False).mean()
    )
    df["kdj_d"] = grouped["kdj_k"].transform(lambda s: s.ewm(alpha=1 / 3, adjust=False).mean())
    df["kdj_j"] = 3.0 * df["kdj_k"] - 2.0 * df["kdj_d"]
    delta = grouped["close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta.clip(upper=0))
    for window in (6, 12, 24):
        avg_gain = gain.groupby(df["stock_code"]).transform(
            lambda s, w=window: s.rolling(w, min_periods=max(2, w // 2)).mean()
        )
        avg_loss = loss.groupby(df["stock_code"]).transform(
            lambda s, w=window: s.rolling(w, min_periods=max(2, w // 2)).mean()
        )
        df[f"rsi{window}"] = 100.0 * avg_gain / (avg_gain + avg_loss).replace(0, np.nan)
    df["boll_mid"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    df["boll_std"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=10).std())
    df["boll_upper"] = df["boll_mid"] + df["boll_std"] * 2.0
    df["boll_lower"] = df["boll_mid"] - df["boll_std"] * 2.0
    df["boll_width_pct"] = (df["boll_upper"] - df["boll_lower"]) / df["boll_mid"].replace(0, np.nan) * 100.0

    prev_high = grouped["high"].shift(1)
    prev_low = grouped["low"].shift(1)
    prev_close = grouped["close"].shift(1)
    up_move = df["high"] - prev_high
    down_move = prev_low - df["low"]
    df["plus_dm"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    df["minus_dm"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr_parts = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1)
    df["tr"] = tr_parts.max(axis=1)
    df["tr14"] = grouped["tr"].transform(lambda s: s.rolling(14, min_periods=1).sum())
    df["plus_dm14"] = grouped["plus_dm"].transform(lambda s: s.rolling(14, min_periods=1).sum())
    df["minus_dm14"] = grouped["minus_dm"].transform(lambda s: s.rolling(14, min_periods=1).sum())
    df["pdi14"] = 100.0 * df["plus_dm14"] / df["tr14"].replace(0, np.nan)
    df["mdi14"] = 100.0 * df["minus_dm14"] / df["tr14"].replace(0, np.nan)
    df["dx14"] = 100.0 * (df["pdi14"] - df["mdi14"]).abs() / (df["pdi14"] + df["mdi14"]).replace(0, np.nan)
    df["adx14"] = grouped["dx14"].transform(lambda s: s.rolling(14, min_periods=1).mean())

    target = pd.to_datetime(trade_date).date()
    latest = df[df["trade_date"] == target].copy()
    if latest.empty:
        raise RuntimeError(f"No K-line rows exactly on {trade_date}")
    chan_rows = []
    for code, rows in df.groupby("stock_code"):
        payload = build_chan_structure(rows[["trade_date", "high", "low", "close", "macd_hist"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        chan_rows.append(payload)
    if chan_rows:
        latest = latest.merge(pd.DataFrame(chan_rows), on="stock_code", how="left")
    pattern_rows = []
    for code, rows in df.groupby("stock_code"):
        payload = detect_kline_pattern(rows[["trade_date", "open", "high", "low", "close"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        pattern_rows.append(payload)
    if pattern_rows:
        latest = latest.merge(pd.DataFrame(pattern_rows), on="stock_code", how="left")
    classic_rows = []
    for code, rows in df.groupby("stock_code"):
        payload = detect_classic_top_bottom_structure(rows[["trade_date", "high", "low", "close"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        classic_rows.append(payload)
    if classic_rows:
        latest = latest.merge(pd.DataFrame(classic_rows), on="stock_code", how="left")
    profile_rows = []
    for code, rows in df.groupby("stock_code"):
        payload = estimate_volume_profile_levels(
            rows[["trade_date", "high", "low", "close", "amount"]].to_dict(orient="records")
        )
        payload["stock_code"] = str(code).zfill(6)
        profile_rows.append(payload)
    if profile_rows:
        latest = latest.merge(pd.DataFrame(profile_rows), on="stock_code", how="left")
    percentile_rows = []
    for code, rows in df.groupby("stock_code"):
        payload = estimate_latest_close_percentile(rows[["trade_date", "close"]].to_dict(orient="records"))
        payload["stock_code"] = str(code).zfill(6)
        percentile_rows.append(payload)
    if percentile_rows:
        latest = latest.merge(pd.DataFrame(percentile_rows), on="stock_code", how="left")
    chase_risk = _build_chase_risk_features_from_rows(chase_source, cutoff)
    if not chase_risk.empty:
        latest = latest.merge(chase_risk, on="stock_code", how="left")
    return latest.reset_index(drop=True)


def load_finance(
    engine: Engine,
    trade_date: str,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    columns = _table_columns(engine, "si_stock_finance")
    if "notice_date" not in columns:
        logger.warning("Finance factors disabled: notice_date availability evidence is missing")
        return pd.DataFrame({"stock_code": []})
    pit_f = _pit_cutoff_sql_clause("f", columns)
    pit_x = _pit_cutoff_sql_clause("x0", columns)
    def optional_numeric_expr(column: str) -> str:
        return f"f.`{column}` AS {column}" if column in columns else f"NULL AS {column}"

    def optional_alias_expr(candidates: tuple[str, ...], alias: str) -> str:
        column = _first_existing(columns, candidates)
        return f"f.`{column}` AS {alias}" if column else f"NULL AS {alias}"

    free_cash_col = _first_existing(columns, ("free_cash_flow", "free_cash_flow_ttm", "fcf"))
    goodwill_col = _first_existing(columns, ("goodwill", "good_will", "goodwill_amount", "goodwill_balance"))
    net_assets_col = _first_existing(columns, (
        "net_assets", "net_asset", "total_owner_equity", "total_hldr_eqy_exc_min_int",
        "total_shareholder_equity", "shareholders_equity",
    ))
    ebit_margin_col = _first_existing(columns, ("ebit_margin", "ebit_profit_margin", "oper_profit_margin"))
    if ebit_margin_col:
        ebit_margin_expr = f"f.`{ebit_margin_col}` AS ebit_margin"
    elif "ebit" in columns and "total_rev" in columns:
        ebit_margin_expr = "CASE WHEN COALESCE(f.`total_rev`, 0) <> 0 THEN f.`ebit` / f.`total_rev` * 100 ELSE NULL END AS ebit_margin"
    else:
        ebit_margin_expr = "NULL AS ebit_margin"
    free_cash_expr = f"f.`{free_cash_col}` AS free_cash_flow" if free_cash_col else "NULL AS free_cash_flow"
    goodwill_expr = f"f.`{goodwill_col}` AS goodwill" if goodwill_col else "NULL AS goodwill"
    net_assets_expr = f"f.`{net_assets_col}` AS net_assets" if net_assets_col else "NULL AS net_assets"
    acct_recv_to_rev_col = _first_existing(columns, (
        "acct_recv_to_rev", "ar_to_rev", "acct_receivable_to_rev",
        "account_receivable_to_revenue", "receivable_to_revenue_ratio",
    ))
    if acct_recv_to_rev_col:
        acct_recv_to_rev_expr = f"f.`{acct_recv_to_rev_col}` AS acct_recv_to_rev"
    elif "acct_recv" in columns and "total_rev" in columns:
        acct_recv_to_rev_expr = "CASE WHEN COALESCE(f.`total_rev`, 0) <> 0 THEN f.`acct_recv` / f.`total_rev` * 100 ELSE NULL END AS acct_recv_to_rev"
    else:
        acct_recv_to_rev_expr = "NULL AS acct_recv_to_rev"
    sql = f"""
        SELECT
          f.stock_code, f.report_date, f.notice_date,
          f.basic_eps, f.net_asset_ps, f.oper_cf_ps,
          {optional_numeric_expr("total_rev")},
          f.total_rev_yoy_gr, f.net_profit_yoy_gr, f.non_gaap_net_profit_yoy_gr,
          {optional_numeric_expr("total_rev_qoq_gr")},
          {optional_numeric_expr("net_profit_qoq_gr")},
          f.roe_wtd,
          {optional_numeric_expr("roe_non_gaap_wtd")},
          {optional_numeric_expr("roa_wtd")},
          f.gross_margin, f.net_margin,
          f.curr_ratio,
          {optional_numeric_expr("quick_ratio")},
          f.cash_flow_ratio, f.asset_liab_ratio,
          {free_cash_expr},
          {goodwill_expr},
          {net_assets_expr},
          {optional_alias_expr(("roic", "roic_ttm", "return_on_invested_capital"), "roic")},
          {acct_recv_to_rev_expr},
          {optional_alias_expr(("prepayment_yoy_gr", "prepayments_yoy_gr", "prepay_yoy_gr", "advance_payment_yoy_gr"), "prepayment_yoy_gr")},
          {optional_alias_expr(("related_transaction_to_rev", "related_party_transaction_to_rev", "related_trade_to_rev"), "related_transaction_to_rev")},
          {ebit_margin_expr},
          'KNOWN_AT_CUTOFF' AS finance_pit_status
        FROM si_stock_finance f
        JOIN (
          SELECT stock_code, MAX(report_date) AS report_date
          FROM si_stock_finance x0
          WHERE report_date <= :trade_date
            AND notice_date IS NOT NULL
            AND notice_date < :cutoff_date
            AND {pit_x}
          GROUP BY stock_code
        ) x ON x.stock_code = f.stock_code AND x.report_date = f.report_date
        WHERE f.notice_date IS NOT NULL
          AND f.notice_date < :cutoff_date
          AND {pit_f}
    """
    df = _read_frame(
        text(sql),
        engine,
        params={
            "trade_date": trade_date,
            "cutoff_date": cutoff.date().isoformat(),
            "knowledge_cutoff": cutoff_text,
        },
    )
    if df.empty:
        return pd.DataFrame({"stock_code": []})
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    for col in df.columns:
        if col not in {"stock_code", "report_date", "notice_date", "finance_pit_status"}:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.drop_duplicates("stock_code", keep="last")


def load_flow_features(
    engine: Engine,
    trade_date: str,
    lookback: int = 25,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, str]:
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    columns = _table_columns(engine, "sm_stock_capital_flow_daily")
    pit_clause = _pit_cutoff_sql_clause("", columns)
    start_date = (
        pd.Timestamp(trade_date) - pd.Timedelta(days=max(40, int(lookback) * 2))
    ).date().isoformat()
    sql = f"""
        SELECT stock_code, trade_date, main_net_inflow, max_net_inflow, lg_net_inflow,
               mid_net_inflow, sm_net_inflow, received_at, etl_sync_at
        FROM sm_stock_capital_flow_daily
        WHERE trade_date >= :start_date
          AND trade_date <= :trade_date
          AND {pit_clause}
        ORDER BY stock_code, trade_date
    """
    df = _read_frame(
        text(sql),
        engine,
        params={
            "start_date": start_date,
            "trade_date": trade_date,
            "knowledge_cutoff": cutoff_text,
        },
    )
    if df.empty:
        return pd.DataFrame({"stock_code": []}), ""
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df, acquisition_columns = _attach_effective_acquisition_time(df)
    if acquisition_columns:
        df = df.sort_values(
            ["stock_code", "trade_date", "_chase_acquired_at", *acquisition_columns],
            kind="mergesort",
        )
    df = df.drop_duplicates(["stock_code", "trade_date"], keep="last").sort_values(["stock_code", "trade_date"])
    flow_dates = sorted(df["trade_date"].dropna().unique(), reverse=True)
    if len(flow_dates) > int(lookback):
        keep_dates = set(flow_dates[: int(lookback)])
        df = df[df["trade_date"].isin(keep_dates)].copy()
    flow_date = str(flow_dates[0])[:10] if flow_dates else ""
    grouped = df.groupby("stock_code", group_keys=False)
    df["main_net_inflow_3d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    df["main_net_inflow_5d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(5, min_periods=1).sum())
    df["main_net_inflow_10d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(10, min_periods=1).sum())
    df["main_net_inflow_20d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    df["main_outflow_days_3d"] = grouped["main_net_inflow"].transform(lambda s: (s < 0).rolling(3, min_periods=1).sum())
    df["main_outflow_days_5d"] = grouped["main_net_inflow"].transform(lambda s: (s < 0).rolling(5, min_periods=1).sum())
    df["main_outflow_days_10d"] = grouped["main_net_inflow"].transform(lambda s: (s < 0).rolling(10, min_periods=1).sum())
    df["main_inflow_days_3d"] = grouped["main_net_inflow"].transform(lambda s: (s > 0).rolling(3, min_periods=1).sum())
    df["main_inflow_days_5d"] = grouped["main_net_inflow"].transform(lambda s: (s > 0).rolling(5, min_periods=1).sum())
    df["main_inflow_days_10d"] = grouped["main_net_inflow"].transform(lambda s: (s > 0).rolling(10, min_periods=1).sum())
    latest = df.groupby("stock_code", as_index=False).tail(1).copy()
    latest = latest.rename(columns={"trade_date": "flow_trade_date"})
    return latest.reset_index(drop=True), flow_date


def load_price_validation_features(
    engine: Engine,
    trade_date: str,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Load optional independent quote sources for K-line close validation."""
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    pieces: list[pd.DataFrame] = []
    try:
        if _table_exists(engine, "sm_stock_snapshot"):
            columns = _table_columns(engine, "sm_stock_snapshot")
            pit_s = _pit_cutoff_sql_clause("s", columns)
            pit_x = _pit_cutoff_sql_clause("s0", columns)
            snapshot = _read_frame(text(f"""
                SELECT s.stock_code, s.trade_date AS snapshot_trade_date,
                       s.price AS snapshot_price, s.close AS snapshot_close
                FROM sm_stock_snapshot s
                JOIN (
                    SELECT s0.stock_code, MAX(s0.trade_date) AS trade_date
                    FROM sm_stock_snapshot s0
                    WHERE s0.trade_date <= :trade_date
                      AND {pit_x}
                    GROUP BY s0.stock_code
                ) x ON x.stock_code = s.stock_code AND x.trade_date = s.trade_date
                WHERE {pit_s}
            """), engine, params={
                "trade_date": trade_date,
                "knowledge_cutoff": cutoff_text,
            })
            if not snapshot.empty:
                pieces.append(snapshot)
    except Exception as exc:
        logger.debug("Price snapshot validation source skipped: %s", exc)

    try:
        if _table_exists(engine, "sm_stock_current"):
            columns = _table_columns(engine, "sm_stock_current")
            pit_q = _pit_cutoff_sql_clause("q", columns)
            pit_x = _pit_cutoff_sql_clause("q0", columns)
            current = _read_frame(text(f"""
                SELECT q.stock_code, q.price AS current_price,
                       q.snapshot_at AS current_snapshot_at
                FROM sm_stock_current q
                JOIN (
                    SELECT q0.stock_code, MAX(q0.snapshot_at) AS snapshot_at
                    FROM sm_stock_current q0
                    WHERE q0.snapshot_at <= :knowledge_cutoff
                      AND {pit_x}
                    GROUP BY q0.stock_code
                ) x ON x.stock_code = q.stock_code AND x.snapshot_at = q.snapshot_at
                WHERE q.snapshot_at <= :knowledge_cutoff
                  AND {pit_q}
            """), engine, params={"knowledge_cutoff": cutoff_text})
            if not current.empty:
                pieces.append(current)
    except Exception as exc:
        logger.debug("Current quote validation source skipped: %s", exc)

    if not pieces:
        return pd.DataFrame({"stock_code": []})
    out = pieces[0].copy()
    for piece in pieces[1:]:
        out = out.merge(piece, on="stock_code", how="outer")
    out["stock_code"] = out["stock_code"].astype(str).str.strip().str.zfill(6)
    return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)


def load_size_liquidity_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load stock size context for float-market-cap gates."""
    pieces: list[pd.DataFrame] = []
    try:
        if _table_exists(engine, "sm_stock_snapshot"):
            columns = _table_columns(engine, "sm_stock_snapshot")
            if {"stock_code", "trade_date"}.issubset(columns):
                snapshot_dates = _recent_dates(engine, "sm_stock_snapshot", "trade_date", trade_date, 1)
                if snapshot_dates:
                    price_col = _first_existing(columns, ("price", "close", "current"))
                    market_cap_col = _first_existing(columns, ("market_cap", "market_capital", "total_market_cap"))
                    snapshot = _read_frame(text(f"""
                        SELECT stock_code,
                               trade_date AS size_trade_date,
                               COALESCE({f"`{price_col}`" if price_col else "NULL"}, 0) AS size_price,
                               COALESCE({f"`{market_cap_col}`" if market_cap_col else "NULL"}, 0) AS market_cap
                        FROM sm_stock_snapshot
                        WHERE trade_date = :snapshot_date
                    """), engine, params={"snapshot_date": snapshot_dates[0]})
                    if not snapshot.empty:
                        pieces.append(snapshot)
    except Exception as exc:
        logger.debug("Size snapshot context skipped: %s", exc)

    try:
        if _table_exists(engine, "si_stock_shares"):
            columns = _table_columns(engine, "si_stock_shares")
            if "stock_code" in columns:
                total_col = _first_existing(columns, ("total_shares", "total_volume"))
                float_col = _first_existing(columns, ("list_a_shares", "float_shares", "float_volume", "free_float_shares"))
                date_col = _first_existing(columns, ("change_date", "report_date", "trade_date"))
                if date_col:
                    shares = _read_frame(text(f"""
                        SELECT s.stock_code,
                               s.`{date_col}` AS share_report_date,
                               COALESCE({f"s.`{total_col}`" if total_col else "NULL"}, 0) AS total_shares,
                               COALESCE({f"s.`{float_col}`" if float_col else "NULL"}, 0) AS float_shares
                        FROM si_stock_shares s
                        JOIN (
                            SELECT stock_code, MAX(`{date_col}`) AS report_date
                            FROM si_stock_shares
                            WHERE `{date_col}` <= :trade_date
                            GROUP BY stock_code
                        ) x ON x.stock_code = s.stock_code AND x.report_date = s.`{date_col}`
                    """), engine, params={"trade_date": trade_date})
                else:
                    shares = _read_frame(text(f"""
                        SELECT stock_code,
                               NULL AS share_report_date,
                               COALESCE({f"`{total_col}`" if total_col else "NULL"}, 0) AS total_shares,
                               COALESCE({f"`{float_col}`" if float_col else "NULL"}, 0) AS float_shares
                        FROM si_stock_shares
                    """), engine)
                if not shares.empty:
                    pieces.append(shares)
    except Exception as exc:
        logger.debug("Share-size context skipped: %s", exc)

    out = _merge_context_pieces(pieces)
    if out.empty:
        return pd.DataFrame({"stock_code": []})
    for col in ("size_price", "market_cap", "total_shares", "float_shares"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if "size_price" not in out.columns:
        out["size_price"] = 0.0
    if "market_cap" not in out.columns:
        out["market_cap"] = 0.0
    if "total_shares" not in out.columns:
        out["total_shares"] = 0.0
    if "float_shares" not in out.columns:
        out["float_shares"] = 0.0
    out["market_cap"] = out["market_cap"].where(
        out["market_cap"] > 0,
        (out["size_price"] * out["total_shares"]).where((out["size_price"] > 0) & (out["total_shares"] > 0), 0.0),
    )
    out["float_market_cap"] = (out["size_price"] * out["float_shares"]).where(
        (out["size_price"] > 0) & (out["float_shares"] > 0),
        0.0,
    )
    out["float_market_cap"] = out["float_market_cap"].where(out["float_market_cap"] > 0, out["market_cap"])
    return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)


def load_order_book_features(
    engine: Engine,
    trade_date: str,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Load latest five-level order-book depth when sm_stock_five_level is available."""
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    try:
        if not _table_exists(engine, "sm_stock_five_level"):
            return pd.DataFrame({"stock_code": []})
        columns = _table_columns(engine, "sm_stock_five_level")
        needed = {"stock_code", "snapshot_at", "b1", "bv1", "s1", "sv1"}
        if not needed.issubset(columns):
            return pd.DataFrame({"stock_code": []})
        pit_q = _pit_cutoff_sql_clause("q", columns)
        pit_x = _pit_cutoff_sql_clause("q0", columns)
        rows = _read_frame(text(f"""
            SELECT q.*
            FROM sm_stock_five_level q
            JOIN (
                SELECT q0.stock_code, MAX(q0.snapshot_at) AS snapshot_at
                FROM sm_stock_five_level q0
                WHERE q0.snapshot_at <= :knowledge_cutoff
                  AND {pit_x}
                GROUP BY q0.stock_code
            ) x ON x.stock_code = q.stock_code AND x.snapshot_at = q.snapshot_at
            WHERE q.snapshot_at <= :knowledge_cutoff
              AND {pit_q}
        """), engine, params={"knowledge_cutoff": cutoff_text})
    except Exception as exc:
        logger.debug("Order-book context skipped: %s", exc)
        return pd.DataFrame({"stock_code": []})

    if rows.empty:
        return pd.DataFrame({"stock_code": []})
    rows["stock_code"] = rows["stock_code"].astype(str).str.strip().str.zfill(6)
    for col in [f"{side}{level}" for side in ("b", "s") for level in range(1, 6)] + [
        f"{side}v{level}" for side in ("b", "s") for level in range(1, 6)
    ]:
        if col not in rows.columns:
            rows[col] = 0.0
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0.0)
    lot_multiplier = 100.0
    bid_amount = pd.Series(0.0, index=rows.index)
    ask_amount = pd.Series(0.0, index=rows.index)
    for level in range(1, 6):
        bid_amount += rows[f"b{level}"] * rows[f"bv{level}"] * lot_multiplier
        ask_amount += rows[f"s{level}"] * rows[f"sv{level}"] * lot_multiplier
    out = rows[["stock_code", "snapshot_at"]].copy()
    out = out.rename(columns={"snapshot_at": "order_book_snapshot_at"})
    out["bid5_amount"] = bid_amount.round(2)
    out["ask5_amount"] = ask_amount.round(2)
    out["order_book_depth_amount"] = (bid_amount + ask_amount).round(2)
    out["bid_ask_imbalance"] = np.where(ask_amount > 0, bid_amount / ask_amount.replace(0, np.nan), np.nan)
    out["bid_ask_imbalance"] = pd.to_numeric(out["bid_ask_imbalance"], errors="coerce").round(2)
    return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)


def load_dividend_features(engine: Engine, trade_date: str, lookback_years: int = 3) -> pd.DataFrame:
    """Load cash-dividend continuity from sm_dividend when available."""
    try:
        if not _table_exists(engine, "sm_dividend"):
            return pd.DataFrame({"stock_code": []})
        columns = _table_columns(engine, "sm_dividend")
        if not {"stock_code", "report_date", "dividend_plan"}.issubset(columns):
            return pd.DataFrame({"stock_code": []})
        start_date = (pd.to_datetime(trade_date) - pd.DateOffset(years=int(lookback_years))).strftime("%Y-%m-%d")
        rows = _read_frame(text("""
            SELECT stock_code, report_date, dividend_plan, ex_dividend_date
            FROM sm_dividend
            WHERE report_date <= :trade_date
              AND report_date >= :start_date
        """), engine, params={"trade_date": trade_date, "start_date": start_date})
        if rows.empty:
            return pd.DataFrame({"stock_code": []})
        rows["stock_code"] = rows["stock_code"].astype(str).str.strip().str.zfill(6)
        rows["report_date"] = pd.to_datetime(rows["report_date"], errors="coerce")
        rows["dividend_cash_per_share"] = rows["dividend_plan"].apply(parse_dividend_cash_per_share)
        rows = rows[rows["dividend_cash_per_share"] > 0].copy()
        if rows.empty:
            return pd.DataFrame({"stock_code": []})
        latest = rows.sort_values(["stock_code", "report_date"]).groupby("stock_code", as_index=False).tail(1)
        grouped = rows.groupby("stock_code", as_index=False).agg(
            dividend_count_3y=("report_date", "nunique"),
            dividend_cash_per_share_3y=("dividend_cash_per_share", "sum"),
        )
        latest = latest.rename(columns={
            "report_date": "latest_dividend_report_date",
            "dividend_plan": "latest_dividend_plan",
            "dividend_cash_per_share": "latest_dividend_cash_per_share",
        })
        out = grouped.merge(
            latest[[
                "stock_code", "latest_dividend_report_date", "latest_dividend_plan",
                "latest_dividend_cash_per_share", "ex_dividend_date",
            ]],
            on="stock_code",
            how="left",
        )
        out["latest_dividend_report_date"] = out["latest_dividend_report_date"].dt.strftime("%Y-%m-%d")
        return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)
    except Exception as exc:
        logger.debug("Dividend features skipped: %s", exc)
        return pd.DataFrame({"stock_code": []})


def load_research_theme_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load stock-level research theme context from the website research radar."""
    try:
        from biz.research_radar.radar import build_research_radar

        radar = build_research_radar(engine, trade_date)
        return build_research_theme_features(radar.get("themes", []))
    except Exception as exc:
        logger.debug("Research theme features skipped: %s", exc)
        return pd.DataFrame({"stock_code": []})


def load_hot_rank(
    engine: Engine,
    trade_date: str,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, str]:
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    columns = _table_columns(engine, "st_hot_rank_fused")
    pit_clause = _pit_cutoff_sql_clause("", columns)
    sql = f"""
        SELECT stock_code, snapshot_date, fused_rank, total_score, source_flag, industry_name
        FROM st_hot_rank_fused
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM st_hot_rank_fused
            WHERE snapshot_date <= :trade_date
              AND {_pit_cutoff_sql_clause('', columns)}
        )
          AND {pit_clause}
    """
    df = _read_frame(
        text(sql),
        engine,
        params={"trade_date": trade_date, "knowledge_cutoff": cutoff_text},
    )
    hot_date = (
        str(pd.to_datetime(df["snapshot_date"], errors="coerce").max().date())
        if not df.empty and pd.to_datetime(df["snapshot_date"], errors="coerce").notna().any()
        else ""
    )
    if df.empty:
        return pd.DataFrame({"stock_code": []}), hot_date
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["fused_rank"] = pd.to_numeric(df["fused_rank"], errors="coerce")
    df["hot_total_score"] = pd.to_numeric(df["total_score"], errors="coerce")
    return df.drop_duplicates("stock_code", keep="last"), hot_date


def load_notice_features(
    engine: Engine,
    trade_date: str,
    lookback_days: int = 14,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    columns = _table_columns(engine, "si_notice_eastmoney")
    if "association_validated" not in columns:
        logger.warning(
            "Notice features disabled: si_notice_eastmoney.association_validated is missing"
        )
        return pd.DataFrame({"stock_code": []})
    display_predicate = (
        "((display_time IS NOT NULL AND display_time <= :knowledge_cutoff) "
        "OR (display_time IS NULL AND notice_date < :cutoff_date))"
        if "display_time" in columns
        else "notice_date < :cutoff_date"
    )
    pit_clause = _pit_cutoff_sql_clause("", columns)
    sql = f"""
        SELECT stock_code, notice_date, title, column_name
        FROM si_notice_eastmoney
        WHERE notice_date >= DATE_SUB(:cutoff_date, INTERVAL :lookback_days DAY)
          AND notice_date <= :cutoff_date
          AND {display_predicate}
          AND {pit_clause}
          AND association_validated = 1
    """
    df = _read_frame(
        text(sql),
        engine,
        params={
            "cutoff_date": cutoff.date().isoformat(),
            "knowledge_cutoff": cutoff_text,
            "lookback_days": int(lookback_days),
        },
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
    cutoff = _normalize_chase_as_of(
        cutoff_time if cutoff_time is not None else trade_date,
        allow_naive_local=cutoff_time is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    columns = _table_columns(engine, "st_news_flash")
    pit_clause = _pit_cutoff_sql_clause("", columns)
    sql = f"""
        SELECT title, content, publish_time, stocks
        FROM st_news_flash
        WHERE publish_time >= DATE_SUB(:cutoff_time, INTERVAL :lookback_days DAY)
          AND publish_time <= :cutoff_time
          AND {pit_clause}
        ORDER BY publish_time DESC
        LIMIT 3000
    """
    df = _read_frame(
        text(sql),
        engine,
        params={
            "cutoff_time": cutoff_text,
            "knowledge_cutoff": cutoff_text,
            "lookback_days": int(lookback_days),
        },
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


def load_event_source_health(
    engine: Engine,
    trade_date: str,
    *,
    as_of_at: str | date | datetime | pd.Timestamp,
) -> dict[str, Any]:
    """Return fail-closed global health for required announcement/news feeds."""
    cutoff = _normalize_chase_as_of(as_of_at, allow_naive_local=True)
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    try:
        news_max_age_minutes = max(
            1, int(float(os.environ.get("PROBIGA_NEWS_SOURCE_MAX_AGE_MINUTES", "180")))
        )
    except (TypeError, ValueError):
        news_max_age_minutes = 180
    try:
        notice_max_age_minutes = max(
            60, int(float(os.environ.get("PROBIGA_NOTICE_SOURCE_MAX_AGE_MINUTES", "7200")))
        )
    except (TypeError, ValueError):
        notice_max_age_minutes = 7200

    specs = (
        {
            "key": "notice",
            "table": "si_notice_eastmoney",
            "event_column": "notice_date",
            "max_age_minutes": notice_max_age_minutes,
            "required": {"notice_date", "etl_sync_at", "association_validated"},
        },
        {
            "key": "news",
            "table": "st_news_flash",
            "event_column": "publish_time",
            "max_age_minutes": news_max_age_minutes,
            "required": {"publish_time", "etl_sync_at"},
        },
    )
    sources: dict[str, dict[str, Any]] = {}
    for spec in specs:
        key = str(spec["key"])
        table = str(spec["table"])
        max_age = int(spec["max_age_minutes"])
        health: dict[str, Any] = {
            "status": "MISSING",
            "row_count": 0,
            "latest_acquired_at": None,
            "age_minutes": None,
            "max_age_minutes": max_age,
            "reason": "",
        }
        try:
            if not _table_exists(engine, table):
                health["reason"] = f"required event table {table} is missing"
                sources[key] = health
                continue
            columns = _table_columns(engine, table)
            missing = sorted(set(spec["required"]) - columns)
            if missing:
                health["reason"] = f"required event fields missing: {','.join(missing)}"
                sources[key] = health
                continue
            pit_clause = _pit_cutoff_sql_clause("", columns)
            window_start = cutoff - pd.Timedelta(minutes=max_age)
            event_column = quote_identifier(str(spec["event_column"]))
            extra = "AND association_validated = 1" if key == "notice" else ""
            row = _read_frame(
                text(f"""
                    SELECT COUNT(*) AS row_count,
                           MAX(etl_sync_at) AS latest_acquired_at
                    FROM {quote_identifier(table)}
                    WHERE {event_column} <= :knowledge_cutoff
                      AND {event_column} >= :window_start
                      AND {pit_clause}
                      {extra}
                """),
                engine,
                params={
                    "knowledge_cutoff": cutoff_text,
                    "window_start": window_start.tz_localize(None).strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    ),
                },
            )
            if row.empty:
                health["reason"] = "event health query returned no watermark row"
                sources[key] = health
                continue
            count = int(_safe_number(row.iloc[0].get("row_count"), 0.0))
            latest_raw = row.iloc[0].get("latest_acquired_at")
            latest = (
                _normalize_acquisition_series(pd.Series([latest_raw])).iloc[0]
                if latest_raw is not None and not pd.isna(latest_raw)
                else pd.NaT
            )
            health["row_count"] = count
            if pd.isna(latest) or count <= 0:
                health["reason"] = "no cutoff-eligible event rows in the required SLA window"
                sources[key] = health
                continue
            age_minutes = max(0.0, (cutoff - latest).total_seconds() / 60.0)
            health.update({
                "latest_acquired_at": latest.isoformat(),
                "age_minutes": round(age_minutes, 1),
            })
            if age_minutes > max_age:
                health["status"] = "STALE"
                health["reason"] = (
                    f"event watermark age {age_minutes:.1f}m exceeds {max_age}m"
                )
            else:
                health["status"] = "HEALTHY"
                health["reason"] = "cutoff-eligible event watermark is fresh"
        except Exception as exc:
            health["status"] = "MISSING"
            health["reason"] = f"event source health query failed: {exc}"
        sources[key] = health

    overall = (
        "HEALTHY"
        if sources and all(item.get("status") == "HEALTHY" for item in sources.values())
        else "DATA_BLOCKED"
    )
    return {
        "event_source_status": overall,
        "event_source_cutoff": cutoff.isoformat(),
        "event_sources": sources,
        "event_source_reason": "; ".join(
            f"{key}={value.get('status')}:{value.get('reason')}"
            for key, value in sources.items()
        ),
    }


def _apply_event_source_health_gate(
    frame: pd.DataFrame,
    health: Mapping[str, Any],
) -> pd.DataFrame:
    """Prevent new-buy eligibility when required event feeds are not healthy."""
    out = frame.copy()
    status = str(health.get("event_source_status") or "DATA_BLOCKED").upper()
    reason = _safe_text_value(
        health.get("event_source_reason"),
        "required event-source health is unavailable",
    )
    out["event_source_status"] = status
    out["event_source_reason"] = reason
    if out.empty or status == "HEALTHY":
        return out
    out["chase_risk_status"] = "DATA_BLOCKED"
    out["ordinary_buy_eligible"] = False
    out["chase_risk_reason"] = "required event source is not healthy: " + reason

    def enrich_evidence(raw: Any) -> str:
        try:
            payload = json.loads(raw) if isinstance(raw, str) and raw.strip() else {}
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload["event_source_health"] = dict(health)
        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    evidence = (
        out["chase_risk_evidence_json"]
        if "chase_risk_evidence_json" in out.columns
        else pd.Series("{}", index=out.index)
    )
    out["chase_risk_evidence_json"] = evidence.map(enrich_evidence)
    return out


def _read_intraday_quote_for_stock_at_cutoff(
    engine: Engine,
    stock_code: str,
    trade_date: str,
    cutoff: pd.Timestamp,
) -> tuple[pd.DataFrame, str]:
    code = str(stock_code).strip().zfill(6)
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    day_start = f"{trade_date} 00:00:00"
    for source in ("sm_stock_current", "sm_rt_quote_snapshot"):
        try:
            if not _table_exists(engine, source):
                continue
            rows = _read_frame(
                text(f"""
                    SELECT stock_code, short_name, price, `change`, change_pct,
                           volume, amount, snapshot_at
                    FROM {quote_identifier(source)}
                    WHERE stock_code = :stock_code
                      AND snapshot_at >= :day_start
                      AND snapshot_at <= :knowledge_cutoff
                      AND price IS NOT NULL
                      AND price > 0
                    ORDER BY snapshot_at DESC
                    LIMIT 2
                """),
                engine,
                params={
                    "stock_code": code,
                    "day_start": day_start,
                    "knowledge_cutoff": cutoff_text,
                },
            )
            if rows.empty:
                continue
            rows["snapshot_at"] = pd.to_datetime(rows["snapshot_at"], errors="coerce")
            rows = rows[rows["snapshot_at"].notna()].sort_values("snapshot_at")
            if not rows.empty:
                return rows.tail(1).reset_index(drop=True), source
        except Exception as exc:
            logger.debug("Single-stock quote source %s skipped: %s", source, exc)
    return pd.DataFrame(), ""


_VOLATILE_DECISION_EVIDENCE_KEYS = frozenset({
    "knowledge_cutoff",
    "event_source_cutoff",
    "evaluated_at",
    "valid_until",
    "age_minutes",
    "event_source_reason",
    "reason",
})


def _is_temporal_evidence_field(field_name: str) -> bool:
    name = str(field_name or "").strip().lower()
    return bool(
        name == "date"
        or name.endswith("_date")
        or name.endswith("_at")
        or name.endswith("_time")
        or "timestamp" in name
        or name in {"observed_at", "acquired_at", "trade_time", "publish_time"}
    )


def _canonical_timestamp_text(value: Any, field_name: str = "") -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    name = str(field_name or "").strip().lower()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    raw = str(value).strip()
    is_date_field = bool(
        (name == "date" or name.endswith("_date"))
        and not name.endswith("_time")
        and not name.endswith("_at")
    )
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        return raw or None
    if pd.isna(timestamp):
        return None
    if is_date_field:
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert(CHINA_MARKET_TIMEZONE)
        return timestamp.date().isoformat()
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(
            CHINA_MARKET_TIMEZONE,
            ambiguous="NaT",
            nonexistent="NaT",
        )
    else:
        timestamp = timestamp.tz_convert(CHINA_MARKET_TIMEZONE)
    if pd.isna(timestamp):
        return None
    return timestamp.tz_convert("UTC").isoformat()


def _canonical_decision_evidence(value: Any, field_name: str = "") -> Any:
    """Return a JSON-stable evidence value without query-clock fields.

    A decision fingerprint must identify the evidence version, not the instant
    at which an otherwise identical read was repeated.  Watermarks, snapshot
    timestamps, policy versions and computed factor outcomes remain included.
    """
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_decision_evidence(item, str(key))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _VOLATILE_DECISION_EVIDENCE_KEYS
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_canonical_decision_evidence(item, field_name) for item in value]
        if isinstance(value, (set, frozenset)):
            return sorted(items, key=lambda item: json.dumps(item, sort_keys=True, default=str))
        return items
    if isinstance(value, (pd.Timestamp, datetime, date, np.datetime64)):
        return _canonical_timestamp_text(value, field_name)
    if isinstance(value, str) and _is_temporal_evidence_field(field_name):
        return _canonical_timestamp_text(value, field_name)
    if isinstance(value, (np.integer, np.floating, np.bool_)):
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if value is not None:
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
    return value


def _decision_evidence_hash(
    *,
    stock_code: str,
    trade_date: str,
    outcome: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> str:
    payload = {
        "stock_code": stock_code,
        "trade_date": trade_date,
        "outcome": _canonical_decision_evidence(outcome),
        "evidence": _canonical_decision_evidence(evidence),
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _frame_evidence_hash(frame: pd.DataFrame | None) -> str | None:
    """Fingerprint selected cutoff-visible rows, including acquisition versions."""
    if frame is None or frame.empty:
        return None
    stable = frame.copy()
    stable = stable.reindex(sorted(stable.columns), axis=1)
    for column in stable.columns:
        if _is_temporal_evidence_field(column):
            stable[column] = stable[column].map(
                lambda value, name=column: _canonical_timestamp_text(value, name)
            )
    sort_columns = [
        column
        for column in (
            "stock_code",
            "trade_date",
            "snapshot_at",
            "observed_at",
            "received_at",
            "etl_sync_at",
            "acquired_at",
        )
        if column in stable.columns
    ]
    if sort_columns:
        stable = stable.sort_values(sort_columns, kind="mergesort", na_position="first")
    records = [
        _canonical_decision_evidence(record)
        for record in stable.to_dict(orient="records")
    ]
    return hashlib.sha256(
        json.dumps(
            records,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _read_stock_daily_rows_at_cutoff(
    engine: Engine,
    stock_code: str,
    trade_date: str,
    cutoff: pd.Timestamp,
) -> tuple[pd.DataFrame, str]:
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    start_date = (
        pd.Timestamp(trade_date) - pd.Timedelta(days=500)
    ).date().isoformat()
    try:
        rows = _read_frame(
            text("""
                SELECT stock_code,
                       COALESCE(NULLIF(short_name, ''), '') AS short_name,
                       trade_date, open, high, low, close, volume, amount,
                       change_pct, turnover_ratio, pre_close,
                       received_at, etl_sync_at
                FROM sm_stock_kline
                WHERE stock_code = :stock_code
                  AND k_type = 1
                  AND adjust_type = 0
                  AND trade_date >= :start_date
                  AND trade_date <= :trade_date
                  AND CASE
                        WHEN received_at IS NULL AND etl_sync_at IS NULL THEN NULL
                        WHEN received_at IS NULL THEN etl_sync_at
                        WHEN etl_sync_at IS NULL THEN received_at
                        WHEN received_at >= etl_sync_at THEN received_at
                        ELSE etl_sync_at
                      END <= :knowledge_cutoff
                ORDER BY trade_date, received_at, etl_sync_at
            """),
            engine,
            params={
                "stock_code": stock_code,
                "start_date": start_date,
                "trade_date": trade_date,
                "knowledge_cutoff": cutoff_text,
            },
        )
    except Exception as exc:
        return pd.DataFrame(), str(exc)
    return rows, ""


def evaluate_stock_buy_gate_at_cutoff(
    engine: Engine,
    stock_code: str,
    trade_date: str,
    knowledge_cutoff: str | datetime | pd.Timestamp,
) -> dict[str, Any]:
    """Read-only exact-cutoff new-buy gate for order/fill revalidation.

    This helper never writes state.  It combines cutoff-eligible daily
    revisions, the observable intraday path/current quote, and required event
    source health.  Any missing proof returns ``DATA_BLOCKED``.
    """
    cutoff = _normalize_chase_as_of(knowledge_cutoff)
    code = str(stock_code or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("stock_code must be a six-digit A-share code")
    target_date = str(trade_date or "")[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", target_date):
        raise ValueError("trade_date must be YYYY-MM-DD")
    if cutoff.date() < pd.Timestamp(target_date).date():
        raise ValueError("knowledge_cutoff cannot precede trade_date")

    daily, daily_error = _read_stock_daily_rows_at_cutoff(
        engine, code, target_date, cutoff
    )

    current = pd.DataFrame()
    quote_source = ""
    if cutoff.date() == pd.Timestamp(target_date).date():
        quote, quote_source = _read_intraday_quote_for_stock_at_cutoff(
            engine, code, target_date, cutoff
        )
        if not quote.empty:
            paths = _read_intraday_path_rows(
                engine,
                target_date,
                stock_codes=[code],
                quote_source=quote_source,
                as_of_at=cutoff,
            )
            current = _aggregate_intraday_current_bars(
                quote,
                paths,
                target_date,
                as_of_at=cutoff,
            )

    pieces = [piece for piece in (daily, current) if piece is not None and not piece.empty]
    if pieces:
        source_rows = pd.concat(pieces, ignore_index=True, sort=False)
        risk = _build_chase_risk_features_from_rows(source_rows, cutoff)
    else:
        risk = pd.DataFrame()
    event_health = load_event_source_health(
        engine,
        target_date,
        as_of_at=cutoff,
    )
    risk = _apply_event_source_health_gate(risk, event_health)

    if risk.empty or code not in set(risk.get("stock_code", pd.Series(dtype=str)).astype(str)):
        status = "DATA_BLOCKED"
        eligible = False
        reason = daily_error or "no cutoff-eligible daily/current price evidence"
        evidence: dict[str, Any] = {
            "daily_read_error": daily_error or None,
            "event_source_health": event_health,
            "knowledge_cutoff": cutoff.isoformat(),
            "price_input_hash": _frame_evidence_hash(source_rows) if pieces else None,
            "quote_source": quote_source or None,
        }
    else:
        row = risk[risk["stock_code"].astype(str).eq(code)].iloc[-1]
        status = _normalized_chase_gate_status(row.get("chase_risk_status")) or "DATA_BLOCKED"
        eligible = bool(
            status == "ALLOW" and _is_explicit_true(row.get("ordinary_buy_eligible"))
        )
        reason = _safe_text_value(row.get("chase_risk_reason"), "buy gate is unverified")
        try:
            evidence = json.loads(str(row.get("chase_risk_evidence_json") or "{}"))
        except Exception:
            evidence = {}
        if not isinstance(evidence, dict):
            evidence = {}
        evidence.update({
            "event_source_health": event_health,
            "knowledge_cutoff": cutoff.isoformat(),
            "price_input_hash": _frame_evidence_hash(source_rows),
            "quote_source": quote_source or None,
        })

    try:
        validity_seconds = max(
            1, int(float(os.environ.get("PROBIGA_BUY_GATE_VALID_SECONDS", "60")))
        )
    except (TypeError, ValueError):
        validity_seconds = 60
    context = {
        "stock_code": code,
        "trade_date": target_date,
        "knowledge_cutoff": cutoff.isoformat(),
        "status": status,
        "ordinary_buy_eligible": eligible,
        "reason": reason,
        "evidence": evidence,
    }
    evidence_hash = _decision_evidence_hash(
        stock_code=code,
        trade_date=target_date,
        outcome={
            "status": status,
            "ordinary_buy_eligible": eligible,
        },
        evidence=evidence,
    )
    return {
        **context,
        "eligible": eligible,
        "evaluated_at": cutoff.isoformat(),
        # Keep context_hash as a compatibility alias for existing execution
        # callers; unlike the prior implementation it is evidence-stable.
        "context_hash": evidence_hash,
        "evidence_hash": evidence_hash,
        "valid_until": (cutoff + pd.Timedelta(seconds=validity_seconds)).isoformat(),
    }


def _read_latest_decision_output_at_cutoff(
    engine: Engine,
    *,
    table_name: str,
    date_column: str,
    stock_code: str,
    trade_date: str,
    cutoff: pd.Timestamp,
    requested_columns: tuple[str, ...],
) -> tuple[dict[str, Any], str]:
    """Read one persisted decision row without admitting later in-place updates."""
    try:
        if not _table_exists(engine, table_name):
            return {}, f"{table_name} is missing"
        available = _table_columns(engine, table_name)
        required = {"stock_code", date_column}
        missing = sorted(required - available)
        if missing:
            return {}, f"{table_name} missing fields: {','.join(missing)}"
        acquisition_columns = tuple(
            column
            for column in ("updated_at", "created_at", "etl_sync_at", "received_at")
            if column in available
        )
        if not acquisition_columns:
            return {}, f"{table_name} has no acquisition timestamp"
        selected = list(dict.fromkeys(
            ["stock_code", date_column, *requested_columns, *acquisition_columns]
        ))
        selected = [column for column in selected if column in available]
        select_sql = ", ".join(quote_identifier(column) for column in selected)
        pit_clause = _pit_cutoff_sql_clause(
            "",
            available,
            candidates=acquisition_columns,
        )
        order_sql = ", ".join(
            f"{quote_identifier(column)} DESC"
            for column in (date_column, *acquisition_columns)
        )
        rows = _read_frame(
            text(f"""
                SELECT {select_sql}
                FROM {quote_identifier(table_name)}
                WHERE stock_code = :stock_code
                  AND {quote_identifier(date_column)} <= :trade_date
                  AND {pit_clause}
                ORDER BY {order_sql}
                LIMIT 1
            """),
            engine,
            params={
                "stock_code": stock_code,
                "trade_date": trade_date,
                "knowledge_cutoff": cutoff.tz_localize(None).strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                ),
            },
        )
    except Exception as exc:
        return {}, str(exc)
    if rows.empty:
        return {}, f"no cutoff-eligible {table_name} row"
    return rows.iloc[0].to_dict(), ""


def _stock_price_context_at_cutoff(
    daily: pd.DataFrame,
    quote: pd.DataFrame,
    cutoff: pd.Timestamp,
) -> dict[str, Any]:
    """Build a minimal exit-price context from cutoff-visible revisions."""
    prepared = daily.copy() if daily is not None else pd.DataFrame()
    if not prepared.empty:
        prepared, acquisition_columns = _attach_effective_acquisition_time(prepared)
        if acquisition_columns:
            prepared = prepared[
                prepared["_chase_acquired_at"].notna()
                & prepared["_chase_acquired_at"].le(cutoff)
            ].copy()
            prepared = prepared.sort_values(
                ["trade_date", "_chase_acquired_at", *acquisition_columns],
                kind="mergesort",
            )
        prepared["trade_date"] = pd.to_datetime(
            prepared.get("trade_date"), errors="coerce"
        )
        prepared["close"] = pd.to_numeric(prepared.get("close"), errors="coerce")
        prepared = prepared.dropna(subset=["trade_date", "close"])
        prepared = prepared[prepared["close"] > 0]
        prepared = prepared.drop_duplicates("trade_date", keep="last")
        prepared = prepared.sort_values("trade_date", kind="mergesort")

    quote_price = None
    quote_snapshot = None
    if quote is not None and not quote.empty:
        quote_price = _safe_number(quote.iloc[-1].get("price"), 0.0)
        if quote_price <= 0:
            quote_price = None
        quote_snapshot = _none_if_nan(quote.iloc[-1].get("snapshot_at"))
    daily_price = (
        _safe_number(prepared.iloc[-1].get("close"), 0.0)
        if not prepared.empty
        else 0.0
    )
    closes = prepared["close"].tail(20) if not prepared.empty else pd.Series(dtype=float)
    ma20 = float(closes.mean()) if len(closes) >= 10 else None
    price = quote_price if quote_price is not None else (daily_price if daily_price > 0 else None)
    return {
        "latest_price": None if price is None else round(float(price), 6),
        "price_source": "intraday_quote" if quote_price is not None else (
            "daily_close" if daily_price > 0 else ""
        ),
        "quote_snapshot_at": quote_snapshot,
        "ma20": None if ma20 is None or not math.isfinite(ma20) else round(ma20, 6),
        "daily_session_count": int(len(prepared)),
    }


def evaluate_stock_holding_exit_at_cutoff(
    engine: Engine,
    stock_code: str,
    trade_date: str,
    knowledge_cutoff: str | datetime | pd.Timestamp,
) -> dict[str, Any]:
    """Read-only holding exit monitor that does not depend on the buy universe.

    Explicit sell/reduce signals, known severe events and observable price stops
    take precedence over source-health failures.  When no explicit exit exists,
    incomplete required event or price evidence yields ``WAIT_DATA`` rather than
    a false ``HOLD``.
    """
    cutoff = _normalize_chase_as_of(knowledge_cutoff)
    code = str(stock_code or "").strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("stock_code must be a six-digit A-share code")
    target_date = str(trade_date or "")[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", target_date):
        raise ValueError("trade_date must be YYYY-MM-DD")
    if cutoff.date() < pd.Timestamp(target_date).date():
        raise ValueError("knowledge_cutoff cannot precede trade_date")

    analysis, analysis_error = _read_latest_decision_output_at_cutoff(
        engine,
        table_name="stock_analysis_result",
        date_column="analysis_date",
        stock_code=code,
        trade_date=target_date,
        cutoff=cutoff,
        requested_columns=(
            "event_risk_level",
            "event_risk_detail",
            "recommend_status",
            "recommend_reason",
            "model_version",
        ),
    )
    recommendation, recommendation_error = _read_latest_decision_output_at_cutoff(
        engine,
        table_name="st_recommended_stocks",
        date_column="pick_date",
        stock_code=code,
        trade_date=target_date,
        cutoff=cutoff,
        requested_columns=(
            "event_risk_level",
            "signal_status",
            "signal_reason",
            "main_wave_signal",
            "main_wave_reason",
            "trend_stop_price",
            "trend_reduce_price",
            "stop_loss_price",
            "invalidation_reason",
            "recommend_status",
            "model_version",
        ),
    )

    daily, daily_error = _read_stock_daily_rows_at_cutoff(
        engine, code, target_date, cutoff
    )
    quote = pd.DataFrame()
    quote_source = ""
    if cutoff.date() == pd.Timestamp(target_date).date():
        quote, quote_source = _read_intraday_quote_for_stock_at_cutoff(
            engine, code, target_date, cutoff
        )
    price_context = _stock_price_context_at_cutoff(daily, quote, cutoff)

    event_health = load_event_source_health(
        engine,
        target_date,
        as_of_at=cutoff,
    )
    event_error = ""
    try:
        notice_frame = load_notice_features(
            engine, target_date, as_of_at=cutoff
        )
        news_frame = load_news_features(
            engine, target_date, cutoff_time=cutoff.isoformat()
        )
    except Exception as exc:
        event_error = str(exc)
        notice_frame = pd.DataFrame()
        news_frame = pd.DataFrame()

    def stock_event_record(frame: pd.DataFrame) -> dict[str, Any]:
        if frame is None or frame.empty or "stock_code" not in frame.columns:
            return {}
        matched = frame[
            frame["stock_code"].astype(str).str.strip().str.zfill(6).eq(code)
        ]
        return matched.iloc[-1].to_dict() if not matched.empty else {}

    notice = stock_event_record(notice_frame)
    news = stock_event_record(news_frame)
    critical_count = int(
        _safe_number(notice.get("notice_critical"), 0.0)
        + _safe_number(news.get("news_critical"), 0.0)
    )
    negative_count = int(
        _safe_number(notice.get("notice_negative"), 0.0)
        + _safe_number(news.get("news_negative"), 0.0)
    )

    signals = {
        str(recommendation.get("signal_status") or "").upper(),
        str(recommendation.get("main_wave_signal") or "").upper(),
    }
    risk_levels = {
        str(analysis.get("event_risk_level") or "").upper(),
        str(recommendation.get("event_risk_level") or "").upper(),
    }
    latest_price = _safe_number(price_context.get("latest_price"), 0.0)
    ma20 = _safe_number(price_context.get("ma20"), 0.0)
    stop_loss = _safe_number(recommendation.get("stop_loss_price"), 0.0)
    trend_stop = _safe_number(recommendation.get("trend_stop_price"), 0.0)
    trend_reduce = _safe_number(recommendation.get("trend_reduce_price"), 0.0)
    computed_trend_stop = ma20 * 0.97 if ma20 > 0 else 0.0

    exit_intent = "HOLD"
    reason = "cutoff-visible analysis, price and event sources do not require an exit"
    explicit_exit = False
    if "SELL_ALERT" in signals:
        exit_intent = "SELL"
        reason = "persisted strategy signal is SELL_ALERT"
        explicit_exit = True
    elif "CRITICAL" in risk_levels or critical_count > 0:
        exit_intent = "SELL"
        reason = "cutoff-visible critical event risk requires exit"
        explicit_exit = True
    elif latest_price > 0 and stop_loss > 0 and latest_price <= stop_loss:
        exit_intent = "SELL"
        reason = f"latest price {latest_price:.4f} breached stop loss {stop_loss:.4f}"
        explicit_exit = True
    elif latest_price > 0 and trend_stop > 0 and latest_price <= trend_stop:
        exit_intent = "SELL"
        reason = f"latest price {latest_price:.4f} breached trend stop {trend_stop:.4f}"
        explicit_exit = True
    elif latest_price > 0 and computed_trend_stop > 0 and latest_price <= computed_trend_stop:
        exit_intent = "SELL"
        reason = (
            f"latest price {latest_price:.4f} invalidated MA20 trend stop "
            f"{computed_trend_stop:.4f}"
        )
        explicit_exit = True
    elif "REDUCE" in signals:
        exit_intent = "REDUCE"
        reason = "persisted strategy signal is REDUCE"
        explicit_exit = True
    elif "HIGH" in risk_levels or negative_count > 0:
        exit_intent = "REDUCE"
        reason = "cutoff-visible high/negative event risk requires reduction"
        explicit_exit = True
    elif latest_price > 0 and trend_reduce > 0 and latest_price <= trend_reduce:
        exit_intent = "REDUCE"
        reason = f"latest price {latest_price:.4f} breached reduction line {trend_reduce:.4f}"
        explicit_exit = True

    if not explicit_exit:
        if str(event_health.get("event_source_status") or "DATA_BLOCKED").upper() != "HEALTHY":
            exit_intent = "WAIT_DATA"
            reason = _safe_text_value(
                event_health.get("event_source_reason"),
                "required event source is unavailable",
            )
        elif event_error:
            exit_intent = "WAIT_DATA"
            reason = f"stock event evidence query failed: {event_error}"
        elif not analysis:
            exit_intent = "WAIT_DATA"
            reason = analysis_error or "no cutoff-eligible analysis row"
        elif latest_price <= 0:
            exit_intent = "WAIT_DATA"
            reason = daily_error or "no cutoff-eligible holding price"

    evidence: dict[str, Any] = {
        "analysis": analysis,
        "analysis_error": analysis_error or None,
        # A stock does not have to be present in the recommendation pool.  The
        # optional row/error is retained for audit but never gates monitoring.
        "recommendation": recommendation,
        "recommendation_error": recommendation_error or None,
        "price": price_context,
        "price_input_hash": _frame_evidence_hash(
            pd.concat(
                [item for item in (daily, quote) if item is not None and not item.empty],
                ignore_index=True,
                sort=False,
            )
        ) if (daily is not None and not daily.empty) or (quote is not None and not quote.empty) else None,
        "daily_read_error": daily_error or None,
        "quote_source": quote_source or None,
        "events": {
            "notice": notice,
            "news": news,
            "critical_count": critical_count,
            "negative_count": negative_count,
            "query_error": event_error or None,
        },
        "event_source_health": event_health,
        "knowledge_cutoff": cutoff.isoformat(),
        "thresholds": {
            "stop_loss_price": stop_loss or None,
            "trend_stop_price": trend_stop or None,
            "trend_reduce_price": trend_reduce or None,
            "computed_ma20_trend_stop": computed_trend_stop or None,
        },
    }
    evidence_hash = _decision_evidence_hash(
        stock_code=code,
        trade_date=target_date,
        outcome={"exit_intent": exit_intent},
        evidence=evidence,
    )
    try:
        validity_seconds = max(
            1, int(float(os.environ.get("PROBIGA_EXIT_GATE_VALID_SECONDS", "60")))
        )
    except (TypeError, ValueError):
        validity_seconds = 60
    return {
        "stock_code": code,
        "trade_date": target_date,
        "knowledge_cutoff": cutoff.isoformat(),
        "exit_intent": exit_intent,
        "reason": reason,
        "evidence": evidence,
        "evaluated_at": cutoff.isoformat(),
        "context_hash": evidence_hash,
        "evidence_hash": evidence_hash,
        "valid_until": (cutoff + pd.Timedelta(seconds=validity_seconds)).isoformat(),
    }


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


def _event_relation_rules(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
    return []


def _relation_name_match(needle: str, values: list[str]) -> bool:
    needle = str(needle or "").strip()
    if not needle or needle.lower() == "all":
        return True
    for value in values:
        value = str(value or "").strip()
        if value and (needle == value or needle in value or value in needle):
            return True
    return False


def _append_unique_event_item(items: list[dict[str, str]], item: dict[str, str]) -> bool:
    marker = (item.get("target") or item.get("scope") or "", item.get("reason") or "", item.get("keywords") or "")
    for existing in items:
        existing_marker = (
            existing.get("target") or existing.get("scope") or "",
            existing.get("reason") or "",
            existing.get("keywords") or "",
        )
        if existing_marker == marker:
            return False
    items.append(item)
    return True


def classify_event_fulfillment(row: dict[str, Any]) -> dict[str, Any]:
    """Classify whether a positive event is fresh, confirmed, or already priced in."""
    positive_titles = row.get("positive_titles") if isinstance(row.get("positive_titles"), list) else []
    risk_titles = row.get("risk_titles") if isinstance(row.get("risk_titles"), list) else []
    pct_5 = _safe_number(row.get("pct_5"), 0.0)
    change_pct = _safe_number(row.get("change_pct"), 0.0)
    dist_ma20 = _safe_number(row.get("dist_ma20"), 0.0)
    main_net_3d = _safe_number(row.get("stock_main_net_inflow_3d"), _safe_number(row.get("main_net_inflow_3d"), 0.0))
    if risk_titles:
        return {
            "event_fulfillment_status": "RISK_EVENT",
            "event_fulfillment_score": 30.0,
            "event_fulfillment_reason": f"存在风险公告/新闻: {str(risk_titles[0])[:80]}",
        }
    if not positive_titles:
        return {
            "event_fulfillment_status": "NO_CLEAR_EVENT",
            "event_fulfillment_score": 50.0,
            "event_fulfillment_reason": "暂无明确正向事件，不能按利好催化交易",
        }
    if pct_5 >= 15.0 or change_pct >= 7.0 or dist_ma20 >= 15.0:
        return {
            "event_fulfillment_status": "PRICED_IN",
            "event_fulfillment_score": 35.0,
            "event_fulfillment_reason": (
                f"正向事件已被价格扩张消化: 5日涨幅{pct_5:.1f}%，"
                f"当日涨幅{change_pct:.1f}%，距MA20 {dist_ma20:.1f}%"
            ),
        }
    if main_net_3d > 0:
        return {
            "event_fulfillment_status": "CONFIRMED",
            "event_fulfillment_score": 68.0,
            "event_fulfillment_reason": f"正向事件后3日资金仍净流入{main_net_3d/1e8:.2f}亿，利好仍有承接",
        }
    return {
        "event_fulfillment_status": "FRESH_WATCH",
        "event_fulfillment_score": 58.0,
        "event_fulfillment_reason": "正向事件尚未明显兑现，等待资金和量价确认",
    }


def build_event_impact(row: dict[str, Any]) -> dict[str, Any]:
    """Classify event impact with optional structured industry-chain relation rules."""
    risk_titles = row.get("risk_titles") if isinstance(row.get("risk_titles"), list) else []
    positive_titles = row.get("positive_titles") if isinstance(row.get("positive_titles"), list) else []
    name = str(row.get("short_name") or row.get("stock_code") or "当前标的")
    code = str(row.get("stock_code") or "").zfill(6)
    industry = str(row.get("industry_name") or row.get("sector_industry_name") or "")
    sector = str(row.get("sector_industry_name") or row.get("industry_name") or "")
    concepts = row.get("concept_names")
    if isinstance(concepts, str):
        concept_values = [part.strip() for part in concepts.replace("，", ",").split(",") if part.strip()]
    elif isinstance(concepts, list):
        concept_values = [str(part).strip() for part in concepts if str(part).strip()]
    else:
        concept_values = []
    beneficiaries: list[dict[str, str]] = []
    damaged: list[dict[str, str]] = []

    for title in positive_titles[:3]:
        hit = [kw for kw in POSITIVE_NOTICE_KEYWORDS if kw in str(title)]
        _append_unique_event_item(beneficiaries, {
            "target": name,
            "reason": f"正向事件: {title}",
            "keywords": ",".join(hit) or "positive_event",
        })
    for title in risk_titles[:3]:
        hit = [kw for kw in CRITICAL_NOTICE_KEYWORDS + NEGATIVE_NOTICE_KEYWORDS if kw in str(title)]
        _append_unique_event_item(damaged, {
            "target": name,
            "reason": f"风险事件: {title}",
            "keywords": ",".join(hit) or "risk_event",
        })

    alternatives = []
    relation_hits = 0
    titles = [str(title) for title in positive_titles + risk_titles if str(title).strip()]
    for rule in _event_relation_rules(row.get("event_relation_rules")):
        trigger = str(rule.get("trigger_keyword") or "").strip()
        if trigger and not any(trigger in title for title in titles):
            continue
        scope = str(rule.get("source_scope") or "all").strip().lower()
        source_key = str(rule.get("source_key") or "").strip()
        if scope in {"industry", "sector", "board"}:
            if not _relation_name_match(source_key, [industry, sector]):
                continue
        elif scope == "concept":
            if not _relation_name_match(source_key, concept_values):
                continue
        elif scope == "stock":
            if not _relation_name_match(source_key, [code, name]):
                continue
        elif scope not in {"", "all", "market"} and source_key:
            if not _relation_name_match(source_key, [industry, sector, code, name] + concept_values):
                continue

        target = str(rule.get("target_name") or rule.get("target_key") or "").strip()
        if not target:
            continue
        impact_type = str(rule.get("impact_type") or "beneficiary").strip().lower()
        reason = str(rule.get("reason") or "产业链事件关系规则命中").strip()
        keywords = trigger or str(rule.get("trigger_keyword") or "event_relation").strip() or "event_relation"
        if impact_type in {"damaged", "damage", "negative", "bearish", "risk", "受损"}:
            if _append_unique_event_item(damaged, {"target": target, "reason": reason, "keywords": keywords}):
                relation_hits += 1
        elif impact_type in {"alternative", "substitute", "replacement", "替代"}:
            if _append_unique_event_item(alternatives, {
                "scope": target,
                "condition": reason,
            }):
                relation_hits += 1
        else:
            if _append_unique_event_item(beneficiaries, {"target": target, "reason": reason, "keywords": keywords}):
                relation_hits += 1

    if damaged and industry:
        _append_unique_event_item(alternatives, {
            "scope": industry,
            "condition": "仅在同板块资金闸门通过且个股无同类风险公告时替代筛选",
        })
    fulfillment = classify_event_fulfillment(row)
    return {
        "beneficiaries": beneficiaries,
        "damaged": damaged,
        "alternative_scope": alternatives,
        "confidence": "rules_with_relation_graph" if relation_hits else "rules_based",
        **fulfillment,
    }


def _date_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        logger.debug("Failed to evaluate nullable date value.", exc_info=True)
    text_value = str(value)
    return text_value[:10] if text_value else ""


def attach_price_crosscheck(df: pd.DataFrame, trade_date: str, tolerance_pct: float | None = None) -> pd.DataFrame:
    """Compare K-line close with an independent current/snapshot quote when available."""
    out = df.copy()
    for col in ("snapshot_price", "snapshot_close", "current_price"):
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ("snapshot_trade_date", "current_snapshot_at"):
        if col not in out.columns:
            out[col] = ""

    statuses: list[str] = []
    reasons: list[str] = []
    diffs: list[float | None] = []
    sources: list[str] = []
    source_counts: list[int] = []
    trade_date = str(trade_date or "")[:10]
    tolerance_pct = (
        runtime_threshold("price_crosscheck_tolerance_pct", PRICE_CROSSCHECK_TOLERANCE_PCT)
        if tolerance_pct is None
        else _safe_number(tolerance_pct, PRICE_CROSSCHECK_TOLERANCE_PCT)
    )
    if tolerance_pct <= 0:
        tolerance_pct = PRICE_CROSSCHECK_TOLERANCE_PCT

    for row in out.to_dict(orient="records"):
        close = _safe_number(row.get("close"), 0.0)
        snapshot_price = _safe_number(row.get("snapshot_price"), 0.0)
        snapshot_close = _safe_number(row.get("snapshot_close"), 0.0)
        current_price = _safe_number(row.get("current_price"), 0.0)
        snapshot_date = _date_text(row.get("snapshot_trade_date"))
        current_date = _date_text(row.get("current_snapshot_at"))

        source_price = 0.0
        source = ""
        source_count = 1
        stale_date = ""
        if current_price > 0:
            source_price = current_price
            source = "sm_stock_current"
            stale_date = current_date
            source_count = 2 if current_date == trade_date else 1
        elif snapshot_price > 0:
            source_price = snapshot_price
            source = "sm_stock_snapshot.price"
            stale_date = snapshot_date
            source_count = 2 if snapshot_close > 0 and abs(snapshot_price - snapshot_close) > 0.0001 else 1

        diff_pct = round(abs(source_price / close - 1.0) * 100.0, 3) if close > 0 and source_price > 0 else None
        if close <= 0:
            status = "MISSING_KLINE"
            reason = "K线收盘价缺失，无法校验"
        elif source_price <= 0:
            status = "MISSING_SOURCE"
            reason = "缺少第二行情源，当前仅使用K线价格"
        elif stale_date and stale_date != trade_date:
            status = "STALE_SOURCE"
            reason = f"第二行情源日期{stale_date}，不同于分析日{trade_date}"
        elif source_count < 2:
            status = "SINGLE_SOURCE"
            reason = "快照价格与K线同源或无法确认独立性，已按单源标记"
        elif diff_pct is not None and diff_pct <= tolerance_pct:
            status = "PASS"
            reason = f"{source} 与K线收盘价偏差{diff_pct:.2f}%"
        else:
            status = "FAIL"
            reason = f"{source} 与K线收盘价偏差{diff_pct:.2f}%，超过{tolerance_pct:.2f}%"

        statuses.append(status)
        reasons.append(reason)
        diffs.append(diff_pct)
        sources.append(source or "sm_stock_kline")
        source_counts.append(source_count)

    out["price_check_status"] = statuses
    out["price_check_reason"] = reasons
    out["price_check_diff_pct"] = diffs
    out["price_check_source"] = sources
    out["price_check_source_count"] = source_counts
    return out


def detect_kline_pattern(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Detect recent daily candle patterns used as supporting evidence, not standalone signals."""
    cleaned: list[dict[str, Any]] = []
    for row in records:
        close = _safe_number(row.get("close"), 0.0)
        open_price = _safe_number(row.get("open"), close)
        high = _safe_number(row.get("high"), max(open_price, close))
        low = _safe_number(row.get("low"), min(open_price, close))
        if close <= 0 or open_price <= 0:
            continue
        high = max(high, open_price, close)
        low = min(low, open_price, close)
        cleaned.append({
            "trade_date": _date_text(row.get("trade_date")),
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
        })

    def _neutral(reason: str = "recent candles do not form a clear reversal pattern") -> dict[str, Any]:
        return {
            "kline_pattern": "none",
            "kline_pattern_direction": "neutral",
            "kline_pattern_strength": 0.0,
            "kline_pattern_reason": reason,
        }

    if not cleaned:
        return _neutral("no valid daily candle records")
    cleaned = sorted(cleaned, key=lambda item: item.get("trade_date") or "")

    def _metrics(candle: dict[str, Any]) -> dict[str, float | bool]:
        open_price = float(candle["open"])
        close = float(candle["close"])
        high = float(candle["high"])
        low = float(candle["low"])
        body = abs(close - open_price)
        full_range = max(high - low, close * 0.002)
        real_body = max(body, close * 0.002)
        upper = high - max(open_price, close)
        lower = min(open_price, close) - low
        return {
            "green": close >= open_price,
            "red": close < open_price,
            "body": body,
            "real_body": real_body,
            "range": full_range,
            "upper": upper,
            "lower": lower,
            "body_pct": body / close * 100.0 if close else 0.0,
        }

    def _pattern(name: str, direction: str, strength: float, reason: str) -> dict[str, Any]:
        return {
            "kline_pattern": name,
            "kline_pattern_direction": direction,
            "kline_pattern_strength": round(max(0.0, min(100.0, strength)), 1),
            "kline_pattern_reason": reason,
        }

    if len(cleaned) >= 3:
        first, middle, last = cleaned[-3], cleaned[-2], cleaned[-1]
        first_m = _metrics(first)
        middle_m = _metrics(middle)
        last_m = _metrics(last)
        first_mid = (float(first["open"]) + float(first["close"])) / 2.0
        middle_small = float(middle_m["body"]) <= max(float(first_m["body"]), float(last_m["body"]), float(last["close"]) * 0.01) * 0.65
        if first_m["red"] and middle_small and last_m["green"] and float(last["close"]) > first_mid:
            return _pattern(
                "morning_star",
                "bullish",
                76.0,
                "三日结构接近早晨星，空方衰竭后收复前阴线实体中位",
            )
        if first_m["green"] and middle_small and last_m["red"] and float(last["close"]) < first_mid:
            return _pattern(
                "evening_star",
                "bearish",
                78.0,
                "三日结构接近黄昏星，冲高后跌破前阳线实体中位",
            )
        recent = cleaned[-3:]
        recent_m = [_metrics(item) for item in recent]
        closes = [float(item["close"]) for item in recent]
        if all(item["green"] for item in recent_m) and closes[0] < closes[1] < closes[2]:
            return _pattern("three_red_soldiers", "bullish", 70.0, "近三日连续阳线并逐日抬高收盘价")
        if all(item["red"] for item in recent_m) and closes[0] > closes[1] > closes[2]:
            return _pattern("three_black_crows", "bearish", 76.0, "近三日连续阴线并逐日降低收盘价")

    if len(cleaned) >= 2:
        previous, current = cleaned[-2], cleaned[-1]
        previous_m = _metrics(previous)
        current_m = _metrics(current)
        prev_body_high = max(float(previous["open"]), float(previous["close"]))
        prev_body_low = min(float(previous["open"]), float(previous["close"]))
        cur_body_high = max(float(current["open"]), float(current["close"]))
        cur_body_low = min(float(current["open"]), float(current["close"]))
        if (
            previous_m["red"]
            and current_m["green"]
            and cur_body_low <= prev_body_low * 1.01
            and cur_body_high >= prev_body_high * 0.99
            and float(current_m["body"]) >= float(previous_m["body"]) * 0.80
        ):
            return _pattern("bullish_engulfing", "bullish", 72.0, "最新阳线实体吞没前一日阴线实体")
        if (
            previous_m["green"]
            and current_m["red"]
            and cur_body_high >= prev_body_high * 0.99
            and cur_body_low <= prev_body_low * 1.01
            and float(current_m["body"]) >= float(previous_m["body"]) * 0.80
        ):
            return _pattern("bearish_engulfing", "bearish", 74.0, "最新阴线实体吞没前一日阳线实体")

    latest = cleaned[-1]
    latest_m = _metrics(latest)
    lower = float(latest_m["lower"])
    upper = float(latest_m["upper"])
    real_body = float(latest_m["real_body"])
    candle_range = float(latest_m["range"])
    if lower >= real_body * 2.0 and upper <= max(real_body * 1.2, candle_range * 0.25):
        return _pattern("hammer", "bullish", 64.0, "单日长下影，低位承接明显，需量价二次确认")
    if upper >= real_body * 2.0 and lower <= max(real_body * 1.2, candle_range * 0.25):
        return _pattern("shooting_star", "bearish", 68.0, "单日长上影，冲高回落压力明显")

    return _neutral()


def detect_classic_top_bottom_structure(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Detect classic double top/bottom, head-and-shoulders and rounded structures."""
    cleaned: list[dict[str, Any]] = []
    for row in records:
        high = _safe_number(row.get("high"), 0.0)
        low = _safe_number(row.get("low"), 0.0)
        close = _safe_number(row.get("close"), 0.0)
        if high > 0 and low > 0 and close > 0:
            cleaned.append({
                "trade_date": _date_text(row.get("trade_date")),
                "high": high,
                "low": low,
                "close": close,
            })
    def _wave_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {
                "classic_pattern_wave_high": None,
                "classic_pattern_wave_high_date": "",
                "classic_pattern_wave_low": None,
                "classic_pattern_wave_low_date": "",
                "classic_pattern_wave_pct": None,
                "classic_pattern_wave_direction": "UNKNOWN",
            }
        window = rows[-60:]
        high_idx, high_row = max(enumerate(window), key=lambda item: float(item[1]["high"]))
        low_idx, low_row = min(enumerate(window), key=lambda item: float(item[1]["low"]))
        high = float(high_row["high"])
        low = float(low_row["low"])
        if low > 0 and low_idx <= high_idx:
            wave_pct = (high / low - 1.0) * 100.0
            wave_direction = "UP"
        elif high > 0:
            wave_pct = (low / high - 1.0) * 100.0
            wave_direction = "DOWN"
        else:
            wave_pct = 0.0
            wave_direction = "UNKNOWN"
        return {
            "classic_pattern_wave_high": round(high, 2),
            "classic_pattern_wave_high_date": str(high_row.get("trade_date") or "")[:10],
            "classic_pattern_wave_low": round(low, 2),
            "classic_pattern_wave_low_date": str(low_row.get("trade_date") or "")[:10],
            "classic_pattern_wave_pct": round(wave_pct, 2),
            "classic_pattern_wave_direction": wave_direction,
        }

    def _neutral(reason: str = "no clear classic top/bottom structure") -> dict[str, Any]:
        wave = _wave_summary(cleaned)
        if reason == "no clear classic top/bottom structure" and wave.get("classic_pattern_wave_high"):
            reason = (
                "当前无明确顶底结构，处于趋势延续中；"
                f"波段高点{wave['classic_pattern_wave_high']}({wave['classic_pattern_wave_high_date']})，"
                f"低点{wave['classic_pattern_wave_low']}({wave['classic_pattern_wave_low_date']})，"
                f"波段涨跌幅{wave['classic_pattern_wave_pct']}%"
            )
        return {
            "classic_pattern": "none",
            "classic_pattern_direction": "neutral",
            "classic_pattern_status": "NONE",
            "classic_pattern_strength": 0.0,
            "classic_pattern_neckline": None,
            "classic_pattern_support": None,
            "classic_pattern_resistance": None,
            "classic_pattern_reason": reason,
            **wave,
        }

    if len(cleaned) < 18:
        return _neutral("insufficient bars for classic pattern")
    cleaned = sorted(cleaned, key=lambda item: item.get("trade_date") or "")
    recent = cleaned[-90:]
    highs = np.array([float(item["high"]) for item in recent], dtype="float64")
    lows = np.array([float(item["low"]) for item in recent], dtype="float64")
    closes = np.array([float(item["close"]) for item in recent], dtype="float64")
    latest_close = float(closes[-1])

    pivot_highs: list[tuple[int, float]] = []
    pivot_lows: list[tuple[int, float]] = []
    for i in range(2, len(recent) - 2):
        if highs[i] >= max(highs[i - 2:i].max(), highs[i + 1:i + 3].max()):
            pivot_highs.append((i, float(highs[i])))
        if lows[i] <= min(lows[i - 2:i].min(), lows[i + 1:i + 3].min()):
            pivot_lows.append((i, float(lows[i])))

    def _result(name: str, direction: str, status: str, strength: float, neckline: float | None, reason: str) -> dict[str, Any]:
        support = float(np.nanmin(lows[-20:])) if len(lows) >= 5 else None
        resistance = float(np.nanmax(highs[-20:])) if len(highs) >= 5 else None
        wave = _wave_summary(recent)
        return {
            "classic_pattern": name,
            "classic_pattern_direction": direction,
            "classic_pattern_status": status,
            "classic_pattern_strength": round(max(0.0, min(100.0, strength)), 1),
            "classic_pattern_neckline": round(float(neckline), 2) if neckline else None,
            "classic_pattern_support": round(support, 2) if support else None,
            "classic_pattern_resistance": round(resistance, 2) if resistance else None,
            "classic_pattern_reason": reason,
            **wave,
        }

    recent_highs = pivot_highs[-6:]
    recent_lows = pivot_lows[-6:]
    for first_idx, first_price in recent_highs[:-1]:
        for second_idx, second_price in recent_highs:
            if second_idx - first_idx < 5:
                continue
            similarity = abs(first_price - second_price) / max(first_price, second_price)
            if similarity <= 0.035:
                neckline = float(np.nanmin(lows[first_idx:second_idx + 1]))
                confirmed = latest_close < neckline * 0.985
                return _result(
                    "double_top",
                    "bearish",
                    "CONFIRMED" if confirmed else "FORMING",
                    82.0 if confirmed else 66.0,
                    neckline,
                    f"double top highs {first_price:.2f}/{second_price:.2f}, neckline {neckline:.2f}",
                )
    for first_idx, first_price in recent_lows[:-1]:
        for second_idx, second_price in recent_lows:
            if second_idx - first_idx < 5:
                continue
            similarity = abs(first_price - second_price) / max(first_price, second_price)
            if similarity <= 0.035:
                neckline = float(np.nanmax(highs[first_idx:second_idx + 1]))
                confirmed = latest_close > neckline * 1.015
                return _result(
                    "double_bottom",
                    "bullish",
                    "CONFIRMED" if confirmed else "FORMING",
                    82.0 if confirmed else 66.0,
                    neckline,
                    f"double bottom lows {first_price:.2f}/{second_price:.2f}, neckline {neckline:.2f}",
                )

    if len(recent_highs) >= 3:
        left, head, right = recent_highs[-3], recent_highs[-2], recent_highs[-1]
        left_price, head_price, right_price = left[1], head[1], right[1]
        shoulders_close = abs(left_price - right_price) / max(left_price, right_price) <= 0.08
        if head_price >= max(left_price, right_price) * 1.04 and shoulders_close and left[0] < head[0] < right[0]:
            neckline = float(np.nanmin(lows[left[0]:right[0] + 1]))
            confirmed = latest_close < neckline * 0.985
            return _result(
                "head_shoulders_top",
                "bearish",
                "CONFIRMED" if confirmed else "FORMING",
                86.0 if confirmed else 70.0,
                neckline,
                f"head-and-shoulders top, neckline {neckline:.2f}",
            )
    if len(recent_lows) >= 3:
        left, head, right = recent_lows[-3], recent_lows[-2], recent_lows[-1]
        left_price, head_price, right_price = left[1], head[1], right[1]
        shoulders_close = abs(left_price - right_price) / max(left_price, right_price) <= 0.08
        if head_price <= min(left_price, right_price) * 0.96 and shoulders_close and left[0] < head[0] < right[0]:
            neckline = float(np.nanmax(highs[left[0]:right[0] + 1]))
            confirmed = latest_close > neckline * 1.015
            return _result(
                "head_shoulders_bottom",
                "bullish",
                "CONFIRMED" if confirmed else "FORMING",
                86.0 if confirmed else 70.0,
                neckline,
                f"head-and-shoulders bottom, neckline {neckline:.2f}",
            )

    if len(closes) >= 45:
        window = closes[-45:]
        left_avg = float(np.nanmean(window[:12]))
        mid_avg = float(np.nanmean(window[16:29]))
        right_avg = float(np.nanmean(window[-12:]))
        if mid_avg > left_avg * 1.04 and mid_avg > right_avg * 1.04 and latest_close < right_avg * 0.985:
            return _result("rounding_top", "bearish", "FORMING", 62.0, right_avg, "rounded top pressure is forming")
        if mid_avg < left_avg * 0.96 and mid_avg < right_avg * 0.96 and latest_close > right_avg * 1.015:
            return _result("rounding_bottom", "bullish", "FORMING", 62.0, right_avg, "rounded bottom repair is forming")

    return _neutral()


def build_chan_structure(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Approximate daily Chan structure: pivots, center overlap, divergence, and buy/sell point."""
    cleaned = []
    for row in records:
        high = _safe_number(row.get("high"), 0.0)
        low = _safe_number(row.get("low"), 0.0)
        close = _safe_number(row.get("close"), 0.0)
        if high > 0 and low > 0 and close > 0:
            cleaned.append({
                "trade_date": _date_text(row.get("trade_date")),
                "high": high,
                "low": low,
                "close": close,
                "macd_hist": _safe_number(row.get("macd_hist"), 0.0),
            })
    if len(cleaned) < 7:
        return {
            "chan_pivot_status": "INSUFFICIENT",
            "chan_signal": "observe",
            "chan_divergence": "none",
            "chan_center_low": None,
            "chan_center_high": None,
            "chan_support_price": None,
            "chan_resistance_price": None,
            "chan_invalidation_price": None,
            "chan_pivot_count": 0,
            "chan_summary": "K线数量不足，无法识别中枢",
        }

    raw_pivots: list[dict[str, Any]] = []
    for idx in range(1, len(cleaned) - 1):
        prev_row, cur, next_row = cleaned[idx - 1], cleaned[idx], cleaned[idx + 1]
        is_top = cur["high"] >= prev_row["high"] and cur["high"] >= next_row["high"] and cur["low"] > min(prev_row["low"], next_row["low"])
        is_bottom = cur["low"] <= prev_row["low"] and cur["low"] <= next_row["low"] and cur["high"] < max(prev_row["high"], next_row["high"])
        if is_top:
            raw_pivots.append({"type": "top", "idx": idx, "date": cur["trade_date"], "price": cur["high"], "macd_hist": cur["macd_hist"]})
        elif is_bottom:
            raw_pivots.append({"type": "bottom", "idx": idx, "date": cur["trade_date"], "price": cur["low"], "macd_hist": cur["macd_hist"]})

    pivots: list[dict[str, Any]] = []
    for pivot in raw_pivots:
        if pivots and pivots[-1]["type"] == pivot["type"]:
            prev = pivots[-1]
            if (pivot["type"] == "top" and pivot["price"] >= prev["price"]) or (pivot["type"] == "bottom" and pivot["price"] <= prev["price"]):
                pivots[-1] = pivot
        else:
            pivots.append(pivot)

    center_low = None
    center_high = None
    pivot_status = "NO_CENTER"
    if len(pivots) >= 4:
        intervals = []
        for left, right in zip(pivots[-4:-1], pivots[-3:]):
            intervals.append((min(left["price"], right["price"]), max(left["price"], right["price"])))
        overlap_low = max(low for low, _ in intervals)
        overlap_high = min(high for _, high in intervals)
        if overlap_low < overlap_high:
            center_low = round(overlap_low, 2)
            center_high = round(overlap_high, 2)
            pivot_status = "CENTER_FORMED"

    latest = cleaned[-1]
    if center_high and latest["close"] > center_high:
        pivot_status = "UP_BREAK"
    elif center_low and latest["close"] < center_low:
        pivot_status = "DOWN_BREAK"

    divergence = "none"
    tops = [p for p in pivots if p["type"] == "top"]
    bottoms = [p for p in pivots if p["type"] == "bottom"]
    if len(tops) >= 2 and tops[-1]["price"] > tops[-2]["price"] and abs(tops[-1]["macd_hist"]) < abs(tops[-2]["macd_hist"]):
        divergence = "top_divergence"
    if len(bottoms) >= 2 and bottoms[-1]["price"] < bottoms[-2]["price"] and abs(bottoms[-1]["macd_hist"]) < abs(bottoms[-2]["macd_hist"]):
        divergence = "bottom_divergence"

    latest_top = tops[-1] if tops else None
    latest_bottom = bottoms[-1] if bottoms else None
    support_price = None
    resistance_price = None
    if latest_bottom:
        support_price = latest_bottom["price"]
    elif center_low:
        support_price = center_low
    if latest_top:
        resistance_price = latest_top["price"]
    elif center_high:
        resistance_price = center_high

    signal = "observe"
    if pivot_status == "UP_BREAK":
        signal = "third_buy"
    elif pivot_status == "DOWN_BREAK":
        signal = "third_sell"
    elif divergence == "bottom_divergence":
        signal = "first_buy_watch"
    elif divergence == "top_divergence":
        signal = "first_sell_watch"
    elif latest_bottom and center_high and latest["close"] >= latest_bottom["price"] * 1.03 and latest["close"] <= center_high:
        signal = "second_buy_watch"
    elif latest_top and center_low and latest["close"] <= latest_top["price"] * 0.97 and latest["close"] >= center_low:
        signal = "second_sell_watch"

    invalidation_price = None
    if signal in {"first_buy_watch", "second_buy_watch", "third_buy", "observe"}:
        invalidation_price = support_price if support_price else center_low
    elif signal in {"first_sell_watch", "second_sell_watch", "third_sell"}:
        invalidation_price = resistance_price if resistance_price else center_high

    summary_parts = [f"分型{len(pivots)}个"]
    if center_low and center_high:
        summary_parts.append(f"中枢{center_low:.2f}-{center_high:.2f}")
    summary_parts.append(f"状态{pivot_status}")
    if divergence != "none":
        summary_parts.append(f"背驰{divergence}")
    summary_parts.append(f"信号{signal}")
    if support_price:
        summary_parts.append(f"支撑{support_price:.2f}")
    if resistance_price:
        summary_parts.append(f"压力{resistance_price:.2f}")
    if invalidation_price:
        summary_parts.append(f"失效{invalidation_price:.2f}")
    return {
        "chan_pivot_status": pivot_status,
        "chan_center_low": center_low,
        "chan_center_high": center_high,
        "chan_support_price": round(support_price, 2) if support_price else None,
        "chan_resistance_price": round(resistance_price, 2) if resistance_price else None,
        "chan_invalidation_price": round(invalidation_price, 2) if invalidation_price else None,
        "chan_divergence": divergence,
        "chan_signal": signal,
        "chan_pivot_count": len(pivots),
        "chan_summary": "，".join(summary_parts),
    }


def build_minute_chan_structure(minute_rows: list[dict[str, Any]], frame_minutes: int = 30) -> dict[str, Any]:
    """Aggregate 1-minute rows into 30/60-minute bars and reuse the Chan structure detector."""
    frame_minutes = max(1, int(frame_minutes or 30))
    cleaned: list[dict[str, Any]] = []
    for row in minute_rows or []:
        close = _first_price(row.get("close"), row.get("price"))
        high = _first_price(row.get("high"), close)
        low = _first_price(row.get("low"), close)
        if close <= 0 or high <= 0 or low <= 0:
            continue
        trade_date_text = _date_text(row.get("trade_date") or row.get("trade_time"))
        trade_time_text = str(row.get("trade_time") or "")
        cleaned.append({
            "trade_date": trade_date_text,
            "trade_time": trade_time_text,
            "high": high,
            "low": low,
            "close": close,
        })
    if not cleaned:
        base = build_chan_structure([])
        base.update({"frame": f"{frame_minutes}m", "bar_count": 0, "source_rows": 0})
        return base

    cleaned.sort(key=lambda item: (item["trade_date"], item["trade_time"]))
    df = pd.DataFrame(cleaned)
    df["bar_no"] = np.arange(len(df)) // frame_minutes
    bars = (
        df.groupby("bar_no", as_index=False)
        .agg(
            trade_date=("trade_date", "last"),
            trade_time=("trade_time", "last"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
        )
    )
    closes = pd.to_numeric(bars["close"], errors="coerce")
    ema12 = closes.ewm(span=12, adjust=False).mean()
    ema26 = closes.ewm(span=26, adjust=False).mean()
    diff = ema12 - ema26
    dea = diff.ewm(span=9, adjust=False).mean()
    bars["macd_hist"] = (diff - dea) * 2.0
    records = bars.to_dict(orient="records")
    chan = build_chan_structure(records)
    chan.update({
        "frame": f"{frame_minutes}m",
        "bar_count": int(len(records)),
        "source_rows": int(len(cleaned)),
        "latest_time": str(records[-1].get("trade_time") or "") if records else "",
    })
    if chan.get("chan_pivot_status") == "INSUFFICIENT":
        chan["chan_summary"] = f"{frame_minutes}分钟K线数量不足，{chan.get('chan_summary') or '继续观察'}"
    return chan


def classify_intraday_behavior(minute_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Classify intraday accumulation, distribution, or wash action from minute prices."""
    cleaned: list[dict[str, Any]] = []
    for row in minute_rows or []:
        close = _first_price(row.get("close"), row.get("price"))
        high = _first_price(row.get("high"), close)
        low = _first_price(row.get("low"), close)
        if close <= 0:
            continue
        trade_time = str(row.get("trade_time") or row.get("datetime") or row.get("time") or "")
        time_part = trade_time[-8:] if len(trade_time) >= 8 else trade_time
        cleaned.append({
            "trade_date": _date_text(row.get("trade_date") or trade_time),
            "trade_time": trade_time,
            "time_part": time_part,
            "close": close,
            "high": high,
            "low": low,
        })
    if len(cleaned) < 20:
        return {
            "pattern": "insufficient",
            "direction": "neutral",
            "confidence": 0.0,
            "reason": "分钟线数量不足，无法判断分时主力行为",
        }
    cleaned.sort(key=lambda item: (item["trade_date"], item["trade_time"]))
    first = cleaned[0]["close"]
    last = cleaned[-1]["close"]
    morning = [x for x in cleaned if str(x.get("time_part") or "").startswith(("09:", "10:", "11:"))]
    afternoon = [x for x in cleaned if str(x.get("time_part") or "").startswith(("13:", "14:", "15:"))]
    first_half = morning or cleaned[: max(1, len(cleaned) // 2)]
    second_half = afternoon or cleaned[max(1, len(cleaned) // 2):]
    morning_first = first_half[0]["close"]
    morning_last = first_half[-1]["close"]
    afternoon_first = second_half[0]["close"] if second_half else morning_last
    day_high = max(x["high"] for x in cleaned)
    day_low = min(x["low"] for x in cleaned)
    am_return = (morning_last / morning_first - 1.0) * 100.0 if morning_first > 0 else 0.0
    pm_return = (last / afternoon_first - 1.0) * 100.0 if afternoon_first > 0 else 0.0
    day_return = (last / first - 1.0) * 100.0 if first > 0 else 0.0
    close_from_high = (last / day_high - 1.0) * 100.0 if day_high > 0 else 0.0
    close_from_low = (last / day_low - 1.0) * 100.0 if day_low > 0 else 0.0

    pattern = "balanced"
    direction = "neutral"
    confidence = 45.0
    reason = "分时上下午强弱未形成明确主力行为特征"
    if am_return <= 0.3 and pm_return >= 1.2 and close_from_high >= -0.8:
        pattern = "accumulation"
        direction = "bullish"
        confidence = 72.0
        reason = "早盘承接后午后走强，收盘贴近日内高位，偏吸筹/抢筹"
    elif am_return >= 1.2 and pm_return <= -1.0 and close_from_high <= -1.5:
        pattern = "distribution"
        direction = "risk"
        confidence = 76.0
        reason = "早盘拉高后午后明显回落，收盘远离高点，偏出货"
    elif am_return >= 0.8 and pm_return <= -0.5 and day_return > -1.5:
        pattern = "wash"
        direction = "neutral"
        confidence = 64.0
        reason = "上午拉高、下午回落但未明显破位，偏洗盘观察"
    elif day_return <= -2.5 and close_from_low <= 1.0:
        pattern = "weak_selloff"
        direction = "risk"
        confidence = 68.0
        reason = "全天弱势并贴近日内低位收盘，短线负反馈较强"

    return {
        "pattern": pattern,
        "direction": direction,
        "confidence": confidence,
        "reason": reason,
        "am_return_pct": round(am_return, 2),
        "pm_return_pct": round(pm_return, 2),
        "day_return_pct": round(day_return, 2),
        "close_from_high_pct": round(close_from_high, 2),
        "close_from_low_pct": round(close_from_low, 2),
    }


def build_minute_chan_features(minute_rows: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        source = minute_source_info()
    except Exception as exc:
        source = {"source": "minute", "table": "", "kind": "unknown", "error": str(exc)}
    return {
        "30m": build_minute_chan_structure(minute_rows, frame_minutes=30),
        "60m": build_minute_chan_structure(minute_rows, frame_minutes=60),
        "behavior": classify_intraday_behavior(minute_rows),
        "source": source,
    }


def _parse_json_field(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except Exception:
            return default
    return default


def _minute_chan_direction(signal: str) -> str:
    signal = str(signal or "").lower()
    if "buy" in signal:
        return "bullish"
    if "sell" in signal:
        return "risk"
    return "neutral"


def enrich_recommendation_minute_chan(
    rec_rows: list[dict[str, Any]],
    trade_date: str,
    max_rows: int = 80,
) -> list[dict[str, Any]]:
    """Add 30/60-minute Chan evidence to recommendation rows when minute data is available."""
    if not rec_rows:
        return rec_rows
    trade_date = str(trade_date or "")[:10]
    try:
        start_date = (datetime.strptime(trade_date, "%Y-%m-%d").date() - timedelta(days=10)).isoformat()
    except Exception:
        start_date = trade_date
    enriched: list[dict[str, Any]] = []
    for idx, row in enumerate(rec_rows):
        item = dict(row)
        if idx >= int(max_rows or 0):
            enriched.append(item)
            continue
        code = str(item.get("stock_code") or "").strip().zfill(6)
        if not code:
            enriched.append(item)
            continue
        try:
            minute_rows = get_stock_minute_prices(code, start_date, trade_date)
        except Exception as exc:
            logger.debug("Minute Chan data unavailable for %s on %s: %s", code, trade_date, exc)
            enriched.append(item)
            continue
        if not minute_rows:
            enriched.append(item)
            continue

        features = build_minute_chan_features(minute_rows)
        source = features.get("source") if isinstance(features.get("source"), dict) else {}
        source_name = f"minute:{source.get('table') or 'stock_minute'}"
        chain = _parse_json_field(item.get("evidence_chain_json"), [])
        if not isinstance(chain, list):
            chain = []
        technical = _parse_json_field(item.get("technical_evidence_json"), {})
        if not isinstance(technical, dict):
            technical = {}
        technical_items = technical.get("items")
        if not isinstance(technical_items, list):
            technical_items = []

        behavior = features.get("behavior")
        if isinstance(behavior, dict) and behavior.get("pattern") != "insufficient":
            chain.append({
                "module": "intraday_behavior",
                "status": str(behavior.get("pattern") or "balanced"),
                "text": str(behavior.get("reason") or ""),
                "source": source_name,
                "date": trade_date,
                "confidence": behavior.get("confidence"),
            })
            technical_items.append({
                "kind": "intraday_behavior",
                "direction": behavior.get("direction") or "neutral",
                "value": {
                    "pattern": behavior.get("pattern"),
                    "confidence": behavior.get("confidence"),
                    "am_return_pct": behavior.get("am_return_pct"),
                    "pm_return_pct": behavior.get("pm_return_pct"),
                    "day_return_pct": behavior.get("day_return_pct"),
                },
                "text": str(behavior.get("reason") or ""),
                "threshold": "分时吸筹/出货/洗盘只用于盘中二次确认，不能替代日线趋势和风险门禁",
            })

        minute_summary: dict[str, Any] = {}
        for frame_key in ("30m", "60m"):
            frame = features.get(frame_key)
            if not isinstance(frame, dict):
                continue
            summary = str(frame.get("chan_summary") or f"{frame_key}分钟缠论结构继续观察")
            status = str(frame.get("chan_signal") or "observe")
            chain.append({
                "module": f"minute_chan_{frame_key}",
                "status": status,
                "text": summary,
                "source": source_name,
                "date": trade_date,
                "bar_count": frame.get("bar_count"),
                "source_rows": frame.get("source_rows"),
            })
            technical_items.append({
                "kind": "minute_chan",
                "frame": frame_key,
                "direction": _minute_chan_direction(status),
                "value": status,
                "text": f"{frame_key} {summary}",
                "threshold": "30/60分钟中枢、背驰和买卖点用于盘中二次确认",
            })
            minute_summary[frame_key] = frame

        if minute_summary:
            technical["items"] = technical_items[:24]
            technical["minute_chan"] = {
                "frames": minute_summary,
                "behavior": behavior if isinstance(behavior, dict) else {},
                "source": source,
            }
            item["technical_evidence_json"] = json.dumps(technical, ensure_ascii=False)
            item["evidence_chain_json"] = json.dumps(chain[:40], ensure_ascii=False)
        enriched.append(item)
    return enriched


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


def compute_market_breadth_features(kline: pd.DataFrame) -> dict[str, Any]:
    """Summarize full-market breadth for stock.txt extreme-position gates."""
    if kline is None or kline.empty:
        return {
            "market_width_ma20_pct": 50.0,
            "market_advance_pct": 50.0,
            "market_limit_up_pct": 0.0,
            "market_limit_down_pct": 0.0,
            "market_extreme_status": "NEUTRAL",
            "market_breadth_reason": "市场宽度数据不足",
        }
    close = pd.to_numeric(kline.get("close"), errors="coerce")
    ma20 = pd.to_numeric(kline.get("ma20"), errors="coerce")
    change = pd.to_numeric(kline.get("change_pct"), errors="coerce")
    valid_ma = close.notna() & ma20.notna() & (close > 0) & (ma20 > 0)
    if valid_ma.any():
        width = float((close[valid_ma] > ma20[valid_ma]).mean() * 100.0)
    else:
        width = 50.0
    valid_change = change.dropna()
    advance = float((valid_change > 0).mean() * 100.0) if not valid_change.empty else 50.0
    limit_up = float((valid_change >= 9.7).mean() * 100.0) if not valid_change.empty else 0.0
    limit_down = float((valid_change <= -9.7).mean() * 100.0) if not valid_change.empty else 0.0
    if width >= 85.0:
        status = "OVERHEAT"
        reason = f"全市场站上MA20比例{width:.1f}%，超过85%拥挤阈值"
    elif width <= 15.0:
        status = "OVERSOLD"
        reason = f"全市场站上MA20比例{width:.1f}%，低于15%恐慌阈值"
    else:
        status = "NEUTRAL"
        reason = f"全市场站上MA20比例{width:.1f}%，处于常态区间"
    return {
        "market_width_ma20_pct": round(width, 2),
        "market_advance_pct": round(advance, 2),
        "market_limit_up_pct": round(limit_up, 2),
        "market_limit_down_pct": round(limit_down, 2),
        "market_extreme_status": status,
        "market_breadth_reason": reason,
    }


def _table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name}).scalar()
    return bool(value)


def load_market_style_context(engine: Engine, trade_date: str) -> dict[str, Any]:
    """Load index trend metrics for stock.txt market-style adaptation."""
    try:
        if not _table_exists(engine, "sm_index_kline"):
            return classify_market_style_context({})
        rows = _read_frame(text("""
            SELECT index_code, trade_date, close, change_pct
            FROM sm_index_kline
            WHERE k_type = 1
              AND index_code IN ('000300', '399006')
              AND trade_date <= :trade_date
            ORDER BY index_code, trade_date
        """), engine, params={"trade_date": trade_date})
        if rows.empty:
            return classify_market_style_context({})
        rows["index_code"] = rows["index_code"].astype(str).str.strip()
        rows["trade_date"] = pd.to_datetime(rows["trade_date"], errors="coerce")
        rows["close"] = pd.to_numeric(rows["close"], errors="coerce")
        metrics: dict[str, dict[str, Any]] = {}
        for code, group in rows.dropna(subset=["trade_date", "close"]).groupby("index_code"):
            ordered = group.sort_values("trade_date").tail(80)
            if ordered.empty:
                continue
            close_series = pd.to_numeric(ordered["close"], errors="coerce").dropna()
            if close_series.empty:
                continue
            latest_close = float(close_series.iloc[-1])
            ma20 = float(close_series.tail(20).mean()) if len(close_series) >= 5 else 0.0
            ma60 = float(close_series.tail(60).mean()) if len(close_series) >= 20 else ma20
            base20 = float(close_series.tail(20).iloc[0]) if len(close_series) >= 20 else 0.0
            pct20 = (latest_close / base20 - 1.0) * 100.0 if base20 > 0 else 0.0
            metrics[str(code)] = {
                "close": latest_close,
                "ma20": ma20,
                "ma60": ma60,
                "pct_20": pct20,
            }
        return classify_market_style_context(metrics)
    except Exception as exc:
        logger.debug("Market style context skipped: %s", exc)
        return classify_market_style_context({})


def load_market_north_flow_features(engine: Engine, trade_date: str) -> dict[str, Any]:
    """Load recent northbound flow from sentiment tables."""
    try:
        if not _table_exists(engine, "st_north_flow_daily"):
            return classify_north_flow_context(None)
        columns = _table_columns(engine, "st_north_flow_daily")
        if "trade_date" not in columns:
            return classify_north_flow_context(None)
        net_col = _first_existing(columns, ("net_tgt", "net_hgt", "net_sgt"))
        if not net_col:
            return classify_north_flow_context(None)
        rows = _read_frame(text(f"""
            SELECT trade_date,
                   COALESCE(`{net_col}`, 0) AS net_tgt
            FROM st_north_flow_daily
            WHERE trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT 5
        """), engine, params={"trade_date": trade_date})
        return classify_north_flow_context(rows)
    except Exception as exc:
        logger.debug("Northbound flow context skipped: %s", exc)
        return classify_north_flow_context(None)


def load_etf_flow_context(engine: Engine, trade_date: str) -> dict[str, Any]:
    """Load broad-market ETF flow from optional local tables."""
    tables = (
        "st_etf_flow_daily", "st_market_etf_flow", "st_fund_etf_flow",
        "sm_etf_flow_daily", "st_etf_fund_flow",
    )
    for table_name in tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            date_col = _first_existing(columns, ("trade_date", "flow_date", "snapshot_date", "date"))
            if not date_col:
                continue
            net_col = _first_existing(columns, (
                "net_amount", "net_inflow", "net_flow", "fund_net_inflow",
                "etf_net_inflow", "amount_net",
            ))
            buy_col = _first_existing(columns, ("buy_amount", "inflow_amount", "subscribe_amount"))
            sell_col = _first_existing(columns, ("sell_amount", "outflow_amount", "redeem_amount"))
            if not net_col and not (buy_col and sell_col):
                continue
            net_expr = f"`{net_col}`" if net_col else f"COALESCE(`{buy_col}`, 0) - COALESCE(`{sell_col}`, 0)"
            rows = _read_frame(text(f"""
                SELECT `{date_col}` AS trade_date,
                       SUM(COALESCE({net_expr}, 0)) AS net_amount
                FROM `{table_name}`
                WHERE `{date_col}` <= :trade_date
                  AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 10 DAY)
                GROUP BY `{date_col}`
                ORDER BY `{date_col}` DESC
                LIMIT 5
            """), engine, params={"trade_date": trade_date})
            if not rows.empty:
                return classify_etf_flow_context(rows)
        except Exception as exc:
            logger.debug("ETF flow table %s skipped: %s", table_name, exc)
    return classify_etf_flow_context(None)


def load_retail_sentiment_context(engine: Engine, trade_date: str) -> dict[str, Any]:
    """Load optional retail bullish/bearish sentiment survey data."""
    tables = (
        "st_retail_sentiment", "st_investor_sentiment", "st_market_sentiment_survey",
        "st_sentiment_survey", "st_market_retail_sentiment",
    )
    for table_name in tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            date_col = _first_existing(columns, ("trade_date", "survey_date", "stat_date", "publish_date", "date"))
            bullish_col = _first_existing(columns, (
                "retail_bullish_pct", "bullish_pct", "bullish_ratio", "bull_ratio",
                "long_pct", "long_ratio", "optimistic_pct", "optimistic_ratio",
            ))
            bearish_col = _first_existing(columns, (
                "retail_bearish_pct", "bearish_pct", "bearish_ratio", "bear_ratio",
                "short_pct", "short_ratio", "pessimistic_pct", "pessimistic_ratio",
            ))
            sample_col = _first_existing(columns, (
                "sample_size", "survey_count", "respondent_count", "count", "total_count",
            ))
            if not date_col or not (bullish_col or bearish_col):
                continue
            bullish_expr = f"`{bullish_col}`" if bullish_col else "NULL"
            bearish_expr = f"`{bearish_col}`" if bearish_col else "NULL"
            sample_expr = f"`{sample_col}`" if sample_col else "0"
            rows = _read_frame(text(f"""
                SELECT `{date_col}` AS trade_date,
                       {bullish_expr} AS bullish_pct,
                       {bearish_expr} AS bearish_pct,
                       {sample_expr} AS sample_size
                FROM `{table_name}`
                WHERE `{date_col}` <= :trade_date
                ORDER BY `{date_col}` DESC
                LIMIT 5
            """), engine, params={"trade_date": trade_date})
            if not rows.empty:
                return classify_retail_sentiment_context(rows)
        except Exception as exc:
            logger.debug("Retail sentiment table %s skipped: %s", table_name, exc)
    return classify_retail_sentiment_context(None)


def load_macro_policy_context(engine: Engine, trade_date: str, cutoff_time: str | None = None) -> dict[str, Any]:
    """Load market-level macro/policy pressure from recent news flash rows."""
    try:
        if not _table_exists(engine, "st_news_flash"):
            return classify_macro_policy_context(None)
        columns = _table_columns(engine, "st_news_flash")
        if "publish_time" not in columns or "title" not in columns:
            return classify_macro_policy_context(None)
        content_expr = "content" if "content" in columns else "''"
        cutoff = cutoff_time or f"{trade_date} 23:59:59"
        rows = _read_frame(text(f"""
            SELECT title,
                   {content_expr} AS content,
                   publish_time
            FROM st_news_flash
            WHERE publish_time >= DATE_SUB(:cutoff_time, INTERVAL 3 DAY)
              AND publish_time <= :cutoff_time
            ORDER BY publish_time DESC
            LIMIT 500
        """), engine, params={"cutoff_time": cutoff})
        return classify_macro_policy_context(rows)
    except Exception as exc:
        logger.debug("Macro policy context skipped: %s", exc)
        return classify_macro_policy_context(None)


def load_macro_indicator_context(engine: Engine, trade_date: str) -> dict[str, Any]:
    """Load structured macro indicators from optional local tables."""
    tables = (
        "st_macro_indicator", "st_macro_economic_data", "st_macro_china_daily",
        "st_macro_calendar", "si_macro_indicator",
    )
    for table_name in tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            name_col = _first_existing(columns, ("indicator_name", "name", "indicator", "item_name", "metric_name"))
            value_col = _first_existing(columns, ("value", "actual_value", "data_value", "current_value", "latest_value"))
            if not name_col or not value_col:
                continue
            date_col = _first_existing(columns, ("period_date", "publish_date", "trade_date", "date", "report_date"))
            yoy_col = _first_existing(columns, ("yoy", "yoy_pct", "year_on_year", "growth_yoy"))
            mom_col = _first_existing(columns, ("mom", "mom_pct", "month_on_month", "change_pct"))
            expected_col = _first_existing(columns, ("expected_value", "forecast_value", "consensus_value"))
            previous_col = _first_existing(columns, ("previous_value", "prev_value", "last_value"))
            date_expr = f"`{date_col}` AS period_date" if date_col else "NULL AS period_date"
            where = f"WHERE `{date_col}` <= :trade_date AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 180 DAY)" if date_col else ""
            order = f"ORDER BY `{date_col}` DESC" if date_col else ""
            rows = _read_frame(text(f"""
                SELECT `{name_col}` AS indicator_name,
                       {date_expr},
                       `{value_col}` AS value,
                       {_select_existing_column(columns, yoy_col, "NULL", alias="yoy") if yoy_col else "NULL AS yoy"},
                       {_select_existing_column(columns, mom_col, "NULL", alias="mom") if mom_col else "NULL AS mom"},
                       {_select_existing_column(columns, expected_col, "NULL", alias="expected_value") if expected_col else "NULL AS expected_value"},
                       {_select_existing_column(columns, previous_col, "NULL", alias="previous_value") if previous_col else "NULL AS previous_value"}
                FROM `{table_name}`
                {where}
                {order}
                LIMIT 200
            """), engine, params={"trade_date": trade_date})
            if not rows.empty:
                return classify_macro_indicator_context(rows)
        except Exception as exc:
            logger.debug("Macro indicator table %s skipped: %s", table_name, exc)
    return classify_macro_indicator_context(None)


def load_stock_north_holding_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load stock-level northbound holding ratio and recent change from optional tables."""
    tables = (
        "st_stock_north_holding", "st_north_stock_holding", "st_hsgt_stock_holding",
        "st_hk_hold", "si_stock_north_holding",
    )
    for table_name in tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            if not code_col:
                continue
            date_col = _first_existing(columns, ("trade_date", "holding_date", "report_date", "date", "publish_date"))
            ratio_col = _first_existing(columns, (
                "north_holding_ratio", "holding_ratio", "hold_ratio", "shareholding_ratio",
                "hsgt_holding_ratio", "north_hold_pct", "hold_pct",
            ))
            value_col = _first_existing(columns, (
                "north_holding_market_value", "holding_market_value", "hold_market_value",
                "market_value", "holding_value",
            ))
            shares_col = _first_existing(columns, ("holding_shares", "hold_shares", "shareholding_shares", "north_holding_shares"))
            net_buy_col = _first_existing(columns, (
                "north_net_buy_amount", "net_buy_amount", "change_amount", "net_amount", "buy_amount_net",
            ))
            date_expr = f"`{date_col}` AS north_stock_trade_date" if date_col else "NULL AS north_stock_trade_date"
            where = f"WHERE `{date_col}` <= :trade_date AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 30 DAY)" if date_col else ""
            order = f"ORDER BY `{code_col}`, `{date_col}`" if date_col else f"ORDER BY `{code_col}`"
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       {date_expr},
                       {f"`{ratio_col}`" if ratio_col else "NULL"} AS north_holding_ratio,
                       {f"`{value_col}`" if value_col else "NULL"} AS north_holding_market_value,
                       {f"`{shares_col}`" if shares_col else "NULL"} AS north_holding_shares,
                       {f"`{net_buy_col}`" if net_buy_col else "NULL"} AS north_net_buy_amount
                FROM `{table_name}`
                {where}
                {order}
            """), engine, params={"trade_date": trade_date})
            if rows.empty:
                continue
            rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
            rows = rows[rows["stock_code"].str.len() == 6].copy()
            if rows.empty:
                continue
            for col in ("north_holding_ratio", "north_holding_market_value", "north_holding_shares", "north_net_buy_amount"):
                rows[col] = pd.to_numeric(rows[col], errors="coerce")
            rows["north_holding_ratio"] = rows["north_holding_ratio"].apply(lambda v: _ratio_to_pct(v, np.nan))
            rows["north_stock_trade_date"] = rows["north_stock_trade_date"].fillna("").astype(str)
            out_rows = []
            for code, grp in rows.groupby("stock_code"):
                grp = grp.sort_values("north_stock_trade_date")
                latest = grp.iloc[-1]
                lag3 = grp.iloc[-4] if len(grp) >= 4 else grp.iloc[0]
                lag5 = grp.iloc[-6] if len(grp) >= 6 else grp.iloc[0]
                ratio = _safe_number(latest.get("north_holding_ratio"), 0.0)
                delta3 = ratio - _safe_number(lag3.get("north_holding_ratio"), ratio)
                delta5 = ratio - _safe_number(lag5.get("north_holding_ratio"), ratio)
                net3 = pd.to_numeric(grp.tail(3)["north_net_buy_amount"], errors="coerce").fillna(0.0).sum()
                net5 = pd.to_numeric(grp.tail(5)["north_net_buy_amount"], errors="coerce").fillna(0.0).sum()
                status = "PASS" if (ratio >= NORTH_STOCK_HOLDING_MIN_RATIO_PCT and (delta3 >= 0.1 or net3 > 0)) else "WATCH"
                if delta3 <= NORTH_STOCK_REDUCTION_DELTA_PCT or net3 <= -50_000_000.0:
                    status = "RISK"
                score = 55.0 + min(max(ratio - NORTH_STOCK_HOLDING_MIN_RATIO_PCT, 0.0), 6.0) * 2.0 + delta3 * 8.0
                if net3 > 50_000_000.0:
                    score += 8.0
                elif net3 < -50_000_000.0:
                    score -= 10.0
                out_rows.append({
                    "stock_code": code,
                    "north_stock_trade_date": str(latest.get("north_stock_trade_date") or "")[:10],
                    "north_holding_ratio": round(ratio, 3),
                    "north_holding_ratio_delta_3d": round(delta3, 3),
                    "north_holding_ratio_delta_5d": round(delta5, 3),
                    "north_holding_market_value": _safe_number(latest.get("north_holding_market_value"), 0.0),
                    "north_holding_shares": _safe_number(latest.get("north_holding_shares"), 0.0),
                    "north_net_buy_amount_3d": float(net3),
                    "north_net_buy_amount_5d": float(net5),
                    "north_stock_status": status,
                    "north_stock_score": clamp_score(score),
                    "north_stock_reason": f"ratio={ratio:.2f}%, delta3={delta3:.2f}pct, net3={net3/1e8:.2f}e8",
                })
            return pd.DataFrame(out_rows)
        except Exception as exc:
            logger.debug("Stock north holding table %s skipped: %s", table_name, exc)
    return pd.DataFrame({"stock_code": []})


def load_institutional_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load fund/QFII holding, sell-side rating and institution survey evidence."""
    pieces: list[pd.DataFrame] = []
    holding_tables = (
        "st_stock_institution_holding", "st_institution_holding", "st_fund_holding",
        "st_qfii_holding", "si_stock_institution_holding",
    )
    for table_name in holding_tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            if not code_col:
                continue
            date_col = _first_existing(columns, ("report_date", "trade_date", "holding_date", "date", "publish_date"))
            fund_col = _first_existing(columns, ("fund_hold_ratio", "fund_holding_ratio", "mutual_fund_hold_ratio"))
            qfii_col = _first_existing(columns, ("qfii_hold_ratio", "qfii_holding_ratio"))
            rqfii_col = _first_existing(columns, ("rqfii_hold_ratio", "rqfii_holding_ratio"))
            social_col = _first_existing(columns, ("social_security_hold_ratio", "ssf_hold_ratio", "social_security_ratio"))
            private_col = _first_existing(columns, ("private_fund_hold_ratio", "private_hold_ratio", "pe_hold_ratio"))
            inst_col = _first_existing(columns, ("institution_hold_ratio", "inst_hold_ratio", "hold_ratio", "holding_ratio"))
            type_col = _first_existing(columns, ("holder_type", "institution_type", "type"))
            date_expr = f"`{date_col}` AS institutional_trade_date" if date_col else "NULL AS institutional_trade_date"
            where = f"WHERE `{date_col}` <= :trade_date AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 370 DAY)" if date_col else ""
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       {date_expr},
                       {f"`{fund_col}`" if fund_col else "NULL"} AS fund_hold_ratio,
                       {f"`{qfii_col}`" if qfii_col else "NULL"} AS qfii_hold_ratio,
                       {f"`{rqfii_col}`" if rqfii_col else "NULL"} AS rqfii_hold_ratio,
                       {f"`{social_col}`" if social_col else "NULL"} AS social_security_hold_ratio,
                       {f"`{private_col}`" if private_col else "NULL"} AS private_fund_hold_ratio,
                       {f"`{inst_col}`" if inst_col else "NULL"} AS institution_hold_ratio,
                       {f"`{type_col}`" if type_col else "NULL"} AS institution_type
                FROM `{table_name}`
                {where}
            """), engine, params={"trade_date": trade_date})
            if rows.empty:
                continue
            rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
            rows = rows[rows["stock_code"].str.len() == 6].copy()
            if rows.empty:
                continue
            ratio_cols = (
                "fund_hold_ratio", "qfii_hold_ratio", "rqfii_hold_ratio",
                "social_security_hold_ratio", "private_fund_hold_ratio",
                "institution_hold_ratio",
            )
            for col in ratio_cols:
                rows[col] = pd.to_numeric(rows[col], errors="coerce").apply(lambda v: _ratio_to_pct(v, np.nan))
            type_text = rows["institution_type"].fillna("").astype(str).str.lower()
            base_ratio = pd.to_numeric(rows["institution_hold_ratio"], errors="coerce")
            rows.loc[type_text.str.contains("社保|social|ssf", regex=True), "social_security_hold_ratio"] = rows["social_security_hold_ratio"].fillna(base_ratio)
            rows.loc[type_text.str.contains("私募|private", regex=True), "private_fund_hold_ratio"] = rows["private_fund_hold_ratio"].fillna(base_ratio)
            rows.loc[type_text.str.contains("rqfii", regex=True), "rqfii_hold_ratio"] = rows["rqfii_hold_ratio"].fillna(base_ratio)
            rows["institutional_trade_date"] = rows["institutional_trade_date"].fillna("").astype(str)
            if date_col:
                latest_dates = rows.groupby("stock_code")["institutional_trade_date"].transform("max")
                rows = rows[rows["institutional_trade_date"] == latest_dates].copy()
            grouped = rows.groupby("stock_code", as_index=False).agg(
                institutional_trade_date=("institutional_trade_date", "max"),
                fund_hold_ratio=("fund_hold_ratio", "sum"),
                qfii_hold_ratio=("qfii_hold_ratio", "sum"),
                rqfii_hold_ratio=("rqfii_hold_ratio", "sum"),
                social_security_hold_ratio=("social_security_hold_ratio", "sum"),
                private_fund_hold_ratio=("private_fund_hold_ratio", "sum"),
                institution_hold_ratio=("institution_hold_ratio", "sum"),
            )
            pieces.append(grouped)
            break
        except Exception as exc:
            logger.debug("Institution holding table %s skipped: %s", table_name, exc)

    rating_tables = ("st_stock_research_rating", "st_research_rating", "st_stock_research_report", "si_stock_research_rating")
    for table_name in rating_tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            date_col = _first_existing(columns, ("report_date", "publish_date", "trade_date", "date"))
            rating_col = _first_existing(columns, ("rating", "rating_name", "recommend_rating", "investment_rating"))
            change_col = _first_existing(columns, ("rating_change", "change_type", "rating_adjustment"))
            target_col = _first_existing(columns, ("target_price", "target_price_latest", "target"))
            if not code_col or not date_col or not (rating_col or change_col or target_col):
                continue
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       `{date_col}` AS rating_date,
                       {f"`{rating_col}`" if rating_col else "''"} AS rating,
                       {f"`{change_col}`" if change_col else "''"} AS rating_change,
                       {f"`{target_col}`" if target_col else "NULL"} AS target_price
                FROM `{table_name}`
                WHERE `{date_col}` <= :trade_date
                  AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 90 DAY)
            """), engine, params={"trade_date": trade_date})
            if rows.empty:
                continue
            rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
            rows = rows[rows["stock_code"].str.len() == 6].copy()
            rows["target_price"] = pd.to_numeric(rows["target_price"], errors="coerce")
            text_col = (rows["rating"].fillna("").astype(str) + " " + rows["rating_change"].fillna("").astype(str)).str.lower()
            rows["rating_upgrade"] = text_col.str.contains("buy|overweight|outperform|upgrade|上调|买入|增持", regex=True).astype(int)
            rows["rating_downgrade"] = text_col.str.contains("sell|reduce|underperform|downgrade|下调|卖出|减持", regex=True).astype(int)
            latest_target = rows.sort_values("rating_date").groupby("stock_code", as_index=False).tail(1)[["stock_code", "rating_date", "target_price"]]
            counts = rows.groupby("stock_code", as_index=False).agg(
                rating_upgrade_count_90d=("rating_upgrade", "sum"),
                rating_downgrade_count_90d=("rating_downgrade", "sum"),
            )
            pieces.append(counts.merge(latest_target, on="stock_code", how="left"))
            break
        except Exception as exc:
            logger.debug("Institution rating table %s skipped: %s", table_name, exc)

    survey_tables = ("st_institution_survey", "st_stock_survey", "st_investor_relations", "st_stock_investor_survey")
    for table_name in survey_tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            date_col = _first_existing(columns, ("survey_date", "receive_date", "publish_date", "trade_date", "date"))
            if not code_col or not date_col:
                continue
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       COUNT(*) AS survey_count_90d,
                       MAX(`{date_col}`) AS latest_survey_date
                FROM `{table_name}`
                WHERE `{date_col}` <= :trade_date
                  AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 90 DAY)
                GROUP BY `{code_col}`
            """), engine, params={"trade_date": trade_date})
            if not rows.empty:
                rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
                pieces.append(rows[rows["stock_code"].str.len() == 6])
                break
        except Exception as exc:
            logger.debug("Institution survey table %s skipped: %s", table_name, exc)

    gold_tables = ("st_broker_gold_stock", "st_gold_stock_pool", "st_broker_monthly_gold_stock", "si_broker_gold_stock")
    for table_name in gold_tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            date_col = _first_existing(columns, ("publish_date", "report_date", "trade_date", "date", "month"))
            if not code_col or not date_col:
                continue
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       COUNT(*) AS broker_gold_count_90d,
                       MAX(`{date_col}`) AS broker_gold_latest_date
                FROM `{table_name}`
                WHERE `{date_col}` <= :trade_date
                  AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 90 DAY)
                GROUP BY `{code_col}`
            """), engine, params={"trade_date": trade_date})
            if not rows.empty:
                rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
                pieces.append(rows[rows["stock_code"].str.len() == 6])
                break
        except Exception as exc:
            logger.debug("Broker gold stock table %s skipped: %s", table_name, exc)

    if not pieces:
        return pd.DataFrame({"stock_code": []})
    out = pieces[0]
    for piece in pieces[1:]:
        out = out.merge(piece, on="stock_code", how="outer")
    defaults = {
        "fund_hold_ratio": 0.0,
        "qfii_hold_ratio": 0.0,
        "rqfii_hold_ratio": 0.0,
        "social_security_hold_ratio": 0.0,
        "private_fund_hold_ratio": 0.0,
        "institution_hold_ratio": 0.0,
        "rating_upgrade_count_90d": 0.0,
        "rating_downgrade_count_90d": 0.0,
        "target_price": np.nan,
        "survey_count_90d": 0.0,
        "broker_gold_count_90d": 0.0,
    }
    out = _ensure_columns(out, defaults)
    for col, default in defaults.items():
        if col != "target_price":
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(default)
    return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)


def load_industry_prosperity_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load structured product price, utilization and stock order/contract data."""
    pieces: list[pd.DataFrame] = []
    order_tables = ("st_stock_order_contract", "st_major_contract", "st_stock_contract_order", "st_stock_order")
    for table_name in order_tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            date_col = _first_existing(columns, ("announcement_date", "publish_date", "trade_date", "date", "sign_date"))
            amount_col = _first_existing(columns, ("contract_amount", "order_amount", "amount", "contract_value", "sales_amount"))
            if not code_col or not amount_col:
                continue
            where = f"WHERE `{date_col}` <= :trade_date AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 180 DAY)" if date_col else ""
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       SUM(COALESCE(`{amount_col}`, 0)) AS order_contract_amount_180d,
                       COUNT(*) AS order_contract_count_180d,
                       {f"MAX(`{date_col}`)" if date_col else "NULL"} AS order_contract_latest_date
                FROM `{table_name}`
                {where}
                GROUP BY `{code_col}`
            """), engine, params={"trade_date": trade_date})
            if not rows.empty:
                rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
                pieces.append(rows[rows["stock_code"].str.len() == 6])
                break
        except Exception as exc:
            logger.debug("Order contract table %s skipped: %s", table_name, exc)

    industry_tables = ("st_industry_prosperity", "st_industry_product_price", "st_industry_capacity_utilization")
    for table_name in industry_tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            industry_col = _first_existing(columns, ("industry_name", "sector_name", "plate_name", "name"))
            date_col = _first_existing(columns, ("trade_date", "publish_date", "date", "report_date"))
            if not industry_col:
                continue
            price_col = _first_existing(columns, ("industry_price_change_30d", "product_price_change_30d", "price_change_30d", "change_pct_30d", "price_change_pct"))
            util_col = _first_existing(columns, ("capacity_utilization", "utilization_rate", "capacity_rate"))
            score_col = _first_existing(columns, ("prosperity_score", "industry_prosperity_score", "boom_score"))
            if not (price_col or util_col or score_col):
                continue
            where = f"WHERE `{date_col}` <= :trade_date AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 90 DAY)" if date_col else ""
            rows = _read_frame(text(f"""
                SELECT `{industry_col}` AS industry_name,
                       {f"`{date_col}`" if date_col else "NULL"} AS prosperity_date,
                       {f"`{price_col}`" if price_col else "NULL"} AS industry_price_change_30d,
                       {f"`{util_col}`" if util_col else "NULL"} AS capacity_utilization,
                       {f"`{score_col}`" if score_col else "NULL"} AS external_prosperity_score
                FROM `{table_name}`
                {where}
            """), engine, params={"trade_date": trade_date})
            if rows.empty:
                continue
            rows = rows.sort_values("prosperity_date").drop_duplicates("industry_name", keep="last")
            if _table_exists(engine, "si_industry_sw"):
                mapping = _read_frame(text("""
                    SELECT stock_code, industry_name
                    FROM si_industry_sw
                    WHERE industry_type = '申万一级'
                      AND industry_name IS NOT NULL
                """), engine)
                if not mapping.empty:
                    mapping["stock_code"] = mapping["stock_code"].astype(str).str.strip().str.zfill(6)
                    mapped = mapping.merge(rows, on="industry_name", how="inner")
                    if not mapped.empty:
                        pieces.append(mapped)
                        break
        except Exception as exc:
            logger.debug("Industry prosperity table %s skipped: %s", table_name, exc)

    if not pieces:
        return pd.DataFrame({"stock_code": []})
    out = pieces[0]
    for piece in pieces[1:]:
        out = out.merge(piece, on="stock_code", how="outer", suffixes=("", "_industry"))
    out = _ensure_columns(out, {
        "industry_price_change_30d": 0.0,
        "capacity_utilization": 0.0,
        "external_prosperity_score": np.nan,
        "order_contract_amount_180d": 0.0,
        "order_contract_count_180d": 0.0,
        "order_contract_latest_date": None,
    })
    for col in ("industry_price_change_30d", "capacity_utilization", "external_prosperity_score", "order_contract_amount_180d", "order_contract_count_180d"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)


def load_business_purity_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load main business text from optional profile/business tables."""
    tables = ("si_stock_business", "st_stock_business", "si_stock_profile", "st_stock_profile", "si_stock_basic", "si_all_code")
    text_candidates = (
        "business_scope", "main_business", "business_desc", "company_profile",
        "profile", "main_products", "concept_names",
    )
    for table_name in tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            text_cols = [col for col in text_candidates if col in columns]
            if not code_col or not text_cols:
                continue
            expr = "CONCAT_WS(' ', " + ", ".join(f"`{col}`" for col in text_cols[:4]) + ")"
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       {expr} AS business_scope,
                       '{table_name}' AS business_profile_source
                FROM `{table_name}`
            """), engine)
            if rows.empty:
                continue
            rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
            rows["business_scope"] = rows["business_scope"].fillna("").astype(str)
            rows = rows[(rows["stock_code"].str.len() == 6) & (rows["business_scope"].str.len() > 0)]
            if not rows.empty:
                return rows.drop_duplicates("stock_code", keep="last").reset_index(drop=True)
        except Exception as exc:
            logger.debug("Business purity table %s skipped: %s", table_name, exc)
    return pd.DataFrame({"stock_code": []})


def load_investor_interaction_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load investor-interaction question/answer signals from optional tables."""
    tables = (
        "st_investor_interaction", "st_stock_interaction", "st_ir_interaction",
        "st_cninfo_interaction", "si_investor_interaction", "st_investor_relations",
    )
    support_pattern = "订单|量产|产能|客户|增长|中标|合作|国产|AI|芯片|新产品|投产|认证|出货"
    risk_pattern = "亏损|下滑|减值|监管|问询|延迟|延期|终止|诉讼|处罚|不确定|产能过剩|价格下降|毛利率下降"
    for table_name in tables:
        try:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            code_col = _first_existing(columns, ("stock_code", "security_code", "code", "ts_code"))
            date_col = _first_existing(columns, ("publish_date", "question_date", "reply_date", "trade_date", "date", "survey_date"))
            text_cols = [
                col for col in (
                    "question", "answer", "title", "content", "reply_content",
                    "interaction_content", "summary",
                )
                if col in columns
            ]
            if not code_col or not date_col or not text_cols:
                continue
            text_expr = "CONCAT_WS(' ', " + ", ".join(f"`{col}`" for col in text_cols[:4]) + ")"
            rows = _read_frame(text(f"""
                SELECT `{code_col}` AS stock_code,
                       COUNT(*) AS investor_interaction_count_180d,
                       SUM(CASE WHEN {text_expr} REGEXP :support_pattern THEN 1 ELSE 0 END) AS investor_interaction_support_count,
                       SUM(CASE WHEN {text_expr} REGEXP :risk_pattern THEN 1 ELSE 0 END) AS investor_interaction_risk_count,
                       MAX(`{date_col}`) AS latest_investor_interaction_date,
                       SUBSTRING_INDEX(GROUP_CONCAT({text_expr} ORDER BY `{date_col}` DESC SEPARATOR ' || '), ' || ', 1) AS latest_investor_interaction
                FROM `{table_name}`
                WHERE `{date_col}` <= :trade_date
                  AND `{date_col}` >= DATE_SUB(:trade_date, INTERVAL 180 DAY)
                GROUP BY `{code_col}`
            """), engine, params={
                "trade_date": trade_date,
                "support_pattern": support_pattern,
                "risk_pattern": risk_pattern,
            })
            if rows.empty:
                continue
            rows["stock_code"] = rows["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
            rows = rows[rows["stock_code"].str.len() == 6].copy()
            if rows.empty:
                continue
            for col in ("investor_interaction_count_180d", "investor_interaction_support_count", "investor_interaction_risk_count"):
                rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0.0)
            profiles = [evaluate_investor_interaction_profile(row) for row in rows.to_dict(orient="records")]
            profile_df = pd.DataFrame(profiles, index=rows.index)
            rows["investor_interaction_status"] = profile_df["investor_interaction_status"]
            rows["investor_interaction_score"] = pd.to_numeric(profile_df["investor_interaction_score"], errors="coerce").fillna(50.0)
            rows["investor_interaction_reason"] = profile_df["investor_interaction_reason"]
            return rows.drop_duplicates("stock_code", keep="last").reset_index(drop=True)
        except Exception as exc:
            logger.debug("Investor interaction table %s skipped: %s", table_name, exc)
    return pd.DataFrame({"stock_code": []})


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
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `st_strategy_threshold_calibration` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `calibration_date` DATE NOT NULL,
                `window_days` INT NOT NULL DEFAULT 90,
                `scope_type` VARCHAR(30) NOT NULL,
                `scope_key` VARCHAR(80) NOT NULL,
                `sample_count` INT NOT NULL DEFAULT 0,
                `avg_return_5d` DECIMAL(8,4) DEFAULT NULL,
                `win_rate_5d` DECIMAL(8,4) DEFAULT NULL,
                `avg_return_10d` DECIMAL(8,4) DEFAULT NULL,
                `win_rate_10d` DECIMAL(8,4) DEFAULT NULL,
                `suggestion` VARCHAR(500) DEFAULT '',
                `created_at` DATETIME DEFAULT NULL,
                KEY `idx_calibration_scope` (`calibration_date`, `window_days`, `scope_type`, `scope_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `st_strategy_runtime_params` (
                `param_key` VARCHAR(80) NOT NULL PRIMARY KEY,
                `param_value` DECIMAL(18,4) NOT NULL,
                `value_type` VARCHAR(20) DEFAULT 'float',
                `source` VARCHAR(40) DEFAULT 'default',
                `effective_date` DATE DEFAULT NULL,
                `status` VARCHAR(20) DEFAULT 'active',
                `metadata_json` TEXT NULL,
                `created_at` DATETIME DEFAULT NULL,
                `updated_at` DATETIME DEFAULT NULL,
                KEY `idx_runtime_status_effective` (`status`, `effective_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `st_event_impact_relations` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `trigger_keyword` VARCHAR(100) NOT NULL DEFAULT '',
                `source_scope` VARCHAR(30) DEFAULT 'all',
                `source_key` VARCHAR(120) DEFAULT '',
                `target_type` VARCHAR(30) DEFAULT 'sector',
                `target_key` VARCHAR(120) DEFAULT '',
                `target_name` VARCHAR(120) DEFAULT '',
                `impact_type` VARCHAR(30) DEFAULT 'beneficiary',
                `reason` VARCHAR(500) DEFAULT '',
                `enabled` TINYINT(1) DEFAULT 1,
                `created_at` DATETIME DEFAULT NULL,
                `updated_at` DATETIME DEFAULT NULL,
                KEY `idx_event_relation_enabled` (`enabled`, `trigger_keyword`),
                KEY `idx_event_relation_source` (`source_scope`, `source_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


def load_strategy_runtime_params(engine: Engine, as_of_date: str) -> dict[str, float]:
    params = DEFAULT_RUNTIME_PARAMS.copy()
    as_of_date = str(as_of_date or "")[:10]
    _ensure_learning_tables(engine)
    if not _table_exists(engine, "st_strategy_runtime_params"):
        return params
    rows = _read_frame(text("""
        SELECT param_key, param_value
        FROM st_strategy_runtime_params
        WHERE status = 'active'
          AND (effective_date IS NULL OR effective_date <= :as_of_date)
        ORDER BY effective_date DESC, updated_at DESC
    """), engine, params={"as_of_date": as_of_date})
    if rows.empty:
        return params
    for row in rows.to_dict(orient="records"):
        key = str(row.get("param_key") or "")
        if key in params:
            params[key] = _safe_number(row.get("param_value"), params[key])
    return params


def load_event_relation_rules(engine: Engine) -> list[dict[str, Any]]:
    _ensure_learning_tables(engine)
    if not _table_exists(engine, "st_event_impact_relations"):
        return []
    rows = _read_frame(text("""
        SELECT trigger_keyword, source_scope, source_key,
               target_type, target_key, target_name, impact_type, reason
        FROM st_event_impact_relations
        WHERE enabled = 1
        ORDER BY id ASC
    """), engine)
    if rows.empty:
        return []
    return rows.fillna("").to_dict(orient="records")


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
    df = _read_frame(text(sql), engine, params=params)
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
    df = _read_frame(text(sql), engine, params={"trade_date": trade_date, "lookback": int(lookback_days)})
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
            sim = _read_frame(text("""
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
            logger.debug("Failed to read simulated trade failure samples.", exc_info=True)
    try:
        manual = _read_frame(text("""
            SELECT stock_code, COUNT(*) AS fail_count
            FROM st_ai_failure_samples
            WHERE result = 'fail'
              AND (signal_date IS NULL OR signal_date >= DATE_SUB(:trade_date, INTERVAL 180 DAY))
            GROUP BY stock_code
        """), engine, params={"trade_date": trade_date})
        if not manual.empty:
            pieces.append(manual)
    except Exception:
        logger.debug("Failed to read manual failure samples.", exc_info=True)
    if not pieces:
        return pd.DataFrame({"stock_code": []})
    df = pd.concat(pieces, ignore_index=True)
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["fail_count"] = pd.to_numeric(df["fail_count"], errors="coerce").fillna(0.0)
    out = df.groupby("stock_code", as_index=False)["fail_count"].sum()
    out["failure_penalty_score"] = (100.0 - out["fail_count"] * 12.0).clip(35, 100)
    return out


def _merge_context_pieces(pieces: list[pd.DataFrame]) -> pd.DataFrame:
    if not pieces:
        return pd.DataFrame({"stock_code": []})
    out = pieces[0].copy()
    for piece in pieces[1:]:
        if piece is not None and not piece.empty and "stock_code" in piece.columns:
            out = out.merge(piece, on="stock_code", how="outer")
    if "stock_code" not in out.columns:
        return pd.DataFrame({"stock_code": []})
    out["stock_code"] = out["stock_code"].astype(str).str.strip().str.zfill(6)
    return out.drop_duplicates("stock_code", keep="last").reset_index(drop=True)


def load_chip_capital_features(engine: Engine, trade_date: str) -> pd.DataFrame:
    """Load optional stock-level chip/capital context used by stock.txt gates."""
    pieces: list[pd.DataFrame] = []

    try:
        if _table_exists(engine, "si_stock_holder"):
            columns = _table_columns(engine, "si_stock_holder")
            if {"stock_code", "report_date"}.issubset(columns):
                holder = _read_frame(text(f"""
                    SELECT h.`stock_code`,
                           h.`report_date` AS `holder_report_date`,
                           {_select_existing_column(columns, "holder_num", "NULL", table_alias="h")},
                           {_select_existing_column(columns, "holder_num_change", "NULL", table_alias="h")},
                           {_select_existing_column(columns, "pre_holder_num", "NULL", table_alias="h")},
                           {_select_existing_column(columns, "holder_num_ratio", "NULL", table_alias="h")},
                           {_select_existing_column(columns, "avg_free_shares", "NULL", table_alias="h")}
                    FROM si_stock_holder h
                    JOIN (
                        SELECT stock_code, MAX(report_date) AS report_date
                        FROM si_stock_holder
                        WHERE report_date <= :trade_date
                        GROUP BY stock_code
                    ) x ON x.stock_code = h.stock_code AND x.report_date = h.report_date
                """), engine, params={"trade_date": trade_date})
                if not holder.empty:
                    pieces.append(holder)
    except Exception as exc:
        logger.debug("Holder context skipped: %s", exc)

    try:
        if _table_exists(engine, "st_a_list_daily"):
            columns = _table_columns(engine, "st_a_list_daily")
            if {"stock_code", "trade_date"}.issubset(columns):
                net_col = "a_net_amount" if "a_net_amount" in columns else "0"
                lhb = _read_frame(text(f"""
                    SELECT stock_code,
                           COUNT(*) AS lhb_count_20d,
                           SUM(COALESCE({net_col}, 0)) AS lhb_net_amount_20d,
                           MAX(trade_date) AS lhb_latest_date
                    FROM st_a_list_daily
                    WHERE trade_date <= :trade_date
                      AND trade_date >= DATE_SUB(:trade_date, INTERVAL 20 DAY)
                    GROUP BY stock_code
                """), engine, params={"trade_date": trade_date})
                if not lhb.empty:
                    pieces.append(lhb)
    except Exception as exc:
        logger.debug("Dragon-tiger context skipped: %s", exc)

    try:
        if _table_exists(engine, "st_a_list_info"):
            columns = _table_columns(engine, "st_a_list_info")
            if {"stock_code", "trade_date", "operate_name"}.issubset(columns):
                net_col = _first_existing(columns, ("a_net_amount",))
                buy_col = _first_existing(columns, ("a_buy_amount",))
                sell_col = _first_existing(columns, ("a_sell_amount",))
                lhb_inst = _read_frame(text(f"""
                    SELECT stock_code,
                           COUNT(*) AS lhb_inst_count_20d,
                           COUNT(DISTINCT CASE WHEN COALESCE({f"`{net_col}`" if net_col else "0"}, 0) > 0 THEN trade_date END) AS lhb_inst_positive_days_20d,
                           SUM(COALESCE({f"`{net_col}`" if net_col else "0"}, 0)) AS lhb_inst_net_amount_20d,
                           SUM(COALESCE({f"`{buy_col}`" if buy_col else "0"}, 0)) AS lhb_inst_buy_amount_20d,
                           SUM(COALESCE({f"`{sell_col}`" if sell_col else "0"}, 0)) AS lhb_inst_sell_amount_20d,
                           MAX(trade_date) AS lhb_inst_latest_date
                    FROM st_a_list_info
                    WHERE trade_date <= :trade_date
                      AND trade_date >= DATE_SUB(:trade_date, INTERVAL 20 DAY)
                      AND COALESCE(operate_name, '') LIKE :inst_keyword
                    GROUP BY stock_code
                """), engine, params={"trade_date": trade_date, "inst_keyword": "%机构%"})
                if not lhb_inst.empty:
                    pieces.append(lhb_inst)
    except Exception as exc:
        logger.debug("Institutional dragon-tiger context skipped: %s", exc)

    try:
        if _table_exists(engine, "st_stock_lifting_last_month"):
            columns = _table_columns(engine, "st_stock_lifting_last_month")
            if {"stock_code", "lift_date"}.issubset(columns):
                lifting = _read_frame(text(f"""
                    SELECT stock_code,
                           COUNT(*) AS lifting_count_30d,
                           MIN(lift_date) AS lifting_next_date,
                           SUM(COALESCE({_first_existing(columns, ("amount",)) or "0"}, 0)) AS lifting_amount_30d,
                           MAX(COALESCE({_first_existing(columns, ("ratio",)) or "0"}, 0)) AS lifting_max_ratio_30d
                    FROM st_stock_lifting_last_month
                    WHERE lift_date >= :trade_date
                      AND lift_date <= DATE_ADD(:trade_date, INTERVAL 30 DAY)
                    GROUP BY stock_code
                """), engine, params={"trade_date": trade_date})
                if not lifting.empty:
                    pieces.append(lifting)
    except Exception as exc:
        logger.debug("Unlock context skipped: %s", exc)

    try:
        if _table_exists(engine, "st_mine_clearance_tdx"):
            columns = _table_columns(engine, "st_mine_clearance_tdx")
            if "stock_code" in columns:
                score_col = _first_existing(columns, ("score", "risk_score"))
                reason_col = _first_existing(columns, ("reason", "f_type", "s_type", "t_type"))
                mine = _read_frame(text(f"""
                    SELECT stock_code,
                           MAX(COALESCE({score_col or "0"}, 0)) AS mine_clearance_score,
                           MAX(COALESCE({reason_col or "''"}, '')) AS mine_clearance_reason
                    FROM st_mine_clearance_tdx
                    GROUP BY stock_code
                """), engine)
                if not mine.empty:
                    pieces.append(mine)
    except Exception as exc:
        logger.debug("Mine-clearance context skipped: %s", exc)

    try:
        pledge_tables = (
            "st_stock_pledge", "st_stock_pledge_ratio", "si_stock_pledge",
            "st_share_pledge", "st_equity_pledge",
        )
        for table_name in pledge_tables:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            if "stock_code" not in columns:
                continue
            ratio_col = _first_existing(columns, (
                "pledge_ratio", "pledge_rate", "pledged_ratio", "pledge_percent",
                "share_pledge_ratio", "major_holder_pledge_ratio", "total_pledge_ratio",
                "overall_pledge_ratio", "zybl",
            ))
            if not ratio_col:
                continue
            date_col = _first_existing(columns, ("report_date", "trade_date", "end_date", "stat_date", "update_date"))
            quoted_table = quote_identifier(table_name)
            quoted_ratio_col = quote_identifier(ratio_col)
            if date_col:
                quoted_date_col = quote_identifier(date_col)
                pledge = _read_frame(text(f"""
                    SELECT p.stock_code,
                           p.{quoted_date_col} AS pledge_report_date,
                           COALESCE(p.{quoted_ratio_col}, 0) AS pledge_ratio
                    FROM {quoted_table} p
                    JOIN (
                        SELECT stock_code, MAX({quoted_date_col}) AS report_date
                        FROM {quoted_table}
                        WHERE {quoted_date_col} <= :trade_date
                        GROUP BY stock_code
                    ) x ON x.stock_code = p.stock_code AND x.report_date = p.{quoted_date_col}
                """), engine, params={"trade_date": trade_date})
            else:
                pledge = _read_frame(text(f"""
                    SELECT stock_code,
                           NULL AS pledge_report_date,
                           MAX(COALESCE({quoted_ratio_col}, 0)) AS pledge_ratio
                    FROM {quoted_table}
                    GROUP BY stock_code
                """), engine)
            if not pledge.empty:
                pieces.append(pledge)
                break
    except Exception as exc:
        logger.debug("Pledge context skipped: %s", exc)

    try:
        reduction_tables = (
            "st_stock_shareholder_reduction", "st_stock_holder_reduction",
            "st_stock_holder_reduce", "st_stock_share_reduce", "si_stock_holder_reduction",
        )
        for table_name in reduction_tables:
            if not _table_exists(engine, table_name):
                continue
            columns = _table_columns(engine, table_name)
            if "stock_code" not in columns:
                continue
            ratio_col = _first_existing(columns, (
                "reduction_ratio", "reduce_ratio", "shareholder_reduction_ratio",
                "sell_ratio", "change_ratio", "ratio", "reduction_percent",
            ))
            amount_col = _first_existing(columns, ("amount", "reduction_amount", "sell_amount", "change_amount"))
            date_col = _first_existing(columns, (
                "announcement_date", "notice_date", "trade_date", "publish_date",
                "report_date", "start_date", "end_date",
            ))
            if not ratio_col and not amount_col:
                continue
            quoted_table = quote_identifier(table_name)
            ratio_expr = quote_identifier(ratio_col) if ratio_col else "0"
            amount_expr = quote_identifier(amount_col) if amount_col else "0"
            if date_col:
                quoted_date_col = quote_identifier(date_col)
                reduction = _read_frame(text(f"""
                    SELECT stock_code,
                           COUNT(*) AS reduction_count_90d,
                           MAX(COALESCE({ratio_expr}, 0)) AS reduction_max_ratio_90d,
                           SUM(COALESCE({amount_expr}, 0)) AS reduction_amount_90d,
                           MAX({quoted_date_col}) AS reduction_latest_date
                    FROM {quoted_table}
                    WHERE {quoted_date_col} <= :trade_date
                      AND {quoted_date_col} >= DATE_SUB(:trade_date, INTERVAL 90 DAY)
                    GROUP BY stock_code
                """), engine, params={"trade_date": trade_date})
            else:
                reduction = _read_frame(text(f"""
                    SELECT stock_code,
                           COUNT(*) AS reduction_count_90d,
                           MAX(COALESCE({ratio_expr}, 0)) AS reduction_max_ratio_90d,
                           SUM(COALESCE({amount_expr}, 0)) AS reduction_amount_90d,
                           NULL AS reduction_latest_date
                    FROM {quoted_table}
                    GROUP BY stock_code
                """), engine)
            if not reduction.empty:
                pieces.append(reduction)
                break
    except Exception as exc:
        logger.debug("Shareholder reduction context skipped: %s", exc)

    try:
        if _table_exists(engine, "st_securities_margin"):
            columns = _table_columns(engine, "st_securities_margin")
            if {"stock_code", "trade_date"}.issubset(columns):
                balance_col = _first_existing(columns, ("rzrqye", "margin_balance", "balance", "financing_balance"))
                financing_col = _first_existing(columns, ("rzye", "financing_balance", "fin_balance"))
                delta_col = _first_existing(columns, ("rzrqyecz", "margin_balance_delta", "balance_delta", "financing_balance_delta"))
                buy_col = _first_existing(columns, ("rzmre", "financing_buy_amount", "fin_buy_amount", "buy_amount"))
                margin = _read_frame(text(f"""
                    SELECT stock_code,
                           trade_date AS margin_trade_date,
                           COALESCE({f"`{balance_col}`" if balance_col else "NULL"}, 0) AS margin_balance,
                           COALESCE({f"`{financing_col}`" if financing_col else "NULL"}, 0) AS financing_balance,
                           COALESCE({f"`{delta_col}`" if delta_col else "NULL"}, 0) AS margin_balance_delta,
                           COALESCE({f"`{buy_col}`" if buy_col else "NULL"}, 0) AS financing_buy_amount
                    FROM st_securities_margin
                    WHERE trade_date <= :trade_date
                      AND trade_date >= DATE_SUB(:trade_date, INTERVAL 10 DAY)
                    ORDER BY stock_code, trade_date
                """), engine, params={"trade_date": trade_date})
                if not margin.empty:
                    margin["stock_code"] = margin["stock_code"].astype(str).str.strip().str.zfill(6)
                    margin["margin_trade_date"] = pd.to_datetime(margin["margin_trade_date"], errors="coerce")
                    for col in ("margin_balance", "financing_balance", "margin_balance_delta", "financing_buy_amount"):
                        margin[col] = pd.to_numeric(margin[col], errors="coerce").fillna(0.0)
                    margin = margin.drop_duplicates(["stock_code", "margin_trade_date"], keep="last")
                    margin = margin.sort_values(["stock_code", "margin_trade_date"])
                    latest = margin.groupby("stock_code", as_index=False).tail(1).copy()
                    recent3 = margin.groupby("stock_code", group_keys=False).tail(3)
                    trend = recent3.groupby("stock_code", as_index=False).agg(
                        margin_balance_delta_3d=("margin_balance_delta", "sum"),
                        financing_buy_amount_3d=("financing_buy_amount", "sum"),
                        margin_expanding_days_3d=("margin_balance_delta", lambda s: int((s > 0).sum())),
                        margin_contracting_days_3d=("margin_balance_delta", lambda s: int((s < 0).sum())),
                    )
                    latest = latest.merge(trend, on="stock_code", how="left")
                    latest["margin_trade_date"] = latest["margin_trade_date"].dt.date.astype(str)
                    pieces.append(latest)
    except Exception as exc:
        logger.debug("Stock margin context skipped: %s", exc)

    out = _merge_context_pieces(pieces)
    for col in (
        "holder_num", "holder_num_change", "pre_holder_num", "holder_num_ratio", "avg_free_shares",
        "lhb_count_20d", "lhb_net_amount_20d", "lhb_inst_count_20d", "lhb_inst_positive_days_20d",
        "lhb_inst_net_amount_20d", "lhb_inst_buy_amount_20d", "lhb_inst_sell_amount_20d",
        "lifting_count_30d", "lifting_amount_30d",
        "lifting_max_ratio_30d", "mine_clearance_score", "pledge_ratio",
        "reduction_count_90d", "reduction_max_ratio_90d", "reduction_amount_90d",
        "margin_balance", "financing_balance",
        "margin_balance_delta", "financing_buy_amount", "margin_balance_delta_3d",
        "financing_buy_amount_3d", "margin_expanding_days_3d", "margin_contracting_days_3d",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "pledge_ratio" in out.columns:
        pledge_ratio = pd.to_numeric(out["pledge_ratio"], errors="coerce")
        out["pledge_ratio"] = pledge_ratio.where((pledge_ratio <= 0) | (pledge_ratio > 1.0), pledge_ratio * 100.0)
    if "reduction_max_ratio_90d" in out.columns:
        reduction_ratio = pd.to_numeric(out["reduction_max_ratio_90d"], errors="coerce")
        out["reduction_max_ratio_90d"] = reduction_ratio.where(
            (reduction_ratio <= 0) | (reduction_ratio > 1.0),
            reduction_ratio * 100.0,
        )
    return out


def load_market_margin_features(engine: Engine, trade_date: str) -> dict[str, Any]:
    """Load market-level margin context when only aggregate margin data is available."""
    try:
        if not _table_exists(engine, "st_securities_margin"):
            return {}
        columns = _table_columns(engine, "st_securities_margin")
        if "stock_code" in columns or "trade_date" not in columns:
            return {}
        balance_col = _first_existing(columns, ("rzrqye", "margin_balance", "balance"))
        financing_col = _first_existing(columns, ("rzye", "financing_balance", "fin_balance"))
        if not balance_col and not financing_col:
            return {}
        rows = _read_frame(text(f"""
            SELECT trade_date,
                   COALESCE({balance_col or "NULL"}, 0) AS market_margin_balance,
                   COALESCE({financing_col or "NULL"}, 0) AS market_financing_balance
            FROM st_securities_margin
            WHERE trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT 2
        """), engine, params={"trade_date": trade_date})
        if rows.empty:
            return {}
        rows["market_margin_balance"] = pd.to_numeric(rows["market_margin_balance"], errors="coerce").fillna(0.0)
        rows["market_financing_balance"] = pd.to_numeric(rows["market_financing_balance"], errors="coerce").fillna(0.0)
        latest = rows.iloc[0]
        prev = rows.iloc[1] if len(rows) > 1 else None
        margin_delta = (
            float(latest["market_margin_balance"] - prev["market_margin_balance"])
            if prev is not None
            else 0.0
        )
        financing_delta = (
            float(latest["market_financing_balance"] - prev["market_financing_balance"])
            if prev is not None
            else 0.0
        )
        status = "EXPANDING" if margin_delta > 0 else ("CONTRACTING" if margin_delta < 0 else "FLAT")
        return {
            "market_margin_trade_date": str(latest.get("trade_date") or "")[:10],
            "market_margin_balance": float(latest["market_margin_balance"]),
            "market_financing_balance": float(latest["market_financing_balance"]),
            "market_margin_balance_delta": margin_delta,
            "market_financing_balance_delta": financing_delta,
            "market_margin_status": status,
        }
    except Exception as exc:
        logger.debug("Market margin context skipped: %s", exc)
        return {}


def load_sector_rotation_features(
    engine: Engine,
    trade_date: str,
    *,
    as_of_at: str | date | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    cutoff = _normalize_chase_as_of(
        as_of_at if as_of_at is not None else trade_date,
        allow_naive_local=as_of_at is not None,
    )
    cutoff_text = cutoff.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S.%f")
    if not _table_exists(engine, "si_industry_sw"):
        return pd.DataFrame({"stock_code": []})
    kline_columns = _table_columns(engine, "sm_stock_kline")
    industry_columns = _table_columns(engine, "si_industry_sw")
    dates = _recent_dates(
        engine,
        "sm_stock_kline",
        "trade_date",
        trade_date,
        3,
        as_of_at=cutoff_text,
    )
    if not dates:
        return pd.DataFrame({"stock_code": []})
    start_date = dates[-1]
    kline = _read_frame(text(f"""
        SELECT stock_code, trade_date, change_pct, amount
        FROM sm_stock_kline
        WHERE k_type = 1
          AND adjust_type = 0
          AND trade_date >= :start_date
          AND trade_date <= :trade_date
          AND {_pit_cutoff_sql_clause('', kline_columns)}
    """), engine, params={
        "start_date": start_date,
        "trade_date": trade_date,
        "knowledge_cutoff": cutoff_text,
    })
    codes = _read_frame(text(f"""
        SELECT stock_code, industry_name
        FROM si_industry_sw
        WHERE industry_type = '申万一级'
          AND industry_name IS NOT NULL
          AND {_pit_cutoff_sql_clause('', industry_columns)}
    """), engine, params={"knowledge_cutoff": cutoff_text})
    if kline.empty or codes.empty:
        return pd.DataFrame({"stock_code": []})
    kline["stock_code"] = kline["stock_code"].astype(str).str.strip().str.zfill(6)
    codes["stock_code"] = codes["stock_code"].astype(str).str.strip().str.zfill(6)
    kline["change_pct"] = pd.to_numeric(kline["change_pct"], errors="coerce").fillna(0.0)
    kline["amount"] = pd.to_numeric(kline["amount"], errors="coerce").fillna(0.0)
    merged = kline.merge(codes.drop_duplicates("stock_code", keep="first"), on="stock_code", how="inner")
    if merged.empty:
        return pd.DataFrame({"stock_code": []})
    if _table_exists(engine, "sm_stock_capital_flow_daily"):
        flow_columns = _table_columns(engine, "sm_stock_capital_flow_daily")
        flow = _read_frame(text(f"""
            SELECT stock_code, trade_date, main_net_inflow
            FROM sm_stock_capital_flow_daily
            WHERE trade_date >= :start_date AND trade_date <= :trade_date
              AND {_pit_cutoff_sql_clause('', flow_columns)}
        """), engine, params={
            "start_date": start_date,
            "trade_date": trade_date,
            "knowledge_cutoff": cutoff_text,
        })
        if not flow.empty:
            flow["stock_code"] = flow["stock_code"].astype(str).str.strip().str.zfill(6)
            flow["main_net_inflow"] = pd.to_numeric(flow["main_net_inflow"], errors="coerce").fillna(0.0)
            merged = merged.merge(flow, on=["stock_code", "trade_date"], how="left")
        else:
            merged["main_net_inflow"] = 0.0
    else:
        merged["main_net_inflow"] = 0.0
    sector = (
        merged.groupby("industry_name", as_index=False)
        .agg(
            avg_change_3d=("change_pct", "mean"),
            main_net_inflow=("main_net_inflow", "sum"),
            amount=("amount", "sum"),
            stock_count=("stock_code", "nunique"),
            active_days=("trade_date", "nunique"),
            positive_days=("change_pct", lambda s: int((pd.to_numeric(s, errors="coerce") > 0).sum())),
        )
    )
    sector["flow_ratio_3d"] = sector["main_net_inflow"] / sector["amount"].replace(0, np.nan) * 100.0
    sector["avg_change_3d"] = pd.to_numeric(sector["avg_change_3d"], errors="coerce").fillna(0.0)
    sector["flow_ratio_3d"] = pd.to_numeric(sector["flow_ratio_3d"], errors="coerce").fillna(0.0)
    sector["stock_count"] = pd.to_numeric(sector["stock_count"], errors="coerce").fillna(0.0)
    sector["active_days"] = pd.to_numeric(sector["active_days"], errors="coerce").fillna(1.0).clip(lower=1.0)
    sector["positive_days"] = pd.to_numeric(sector["positive_days"], errors="coerce").fillna(0.0)
    sector["sector_width_pct"] = (
        sector["positive_days"] / (sector["stock_count"] * sector["active_days"]).replace(0, np.nan) * 100.0
    ).fillna(0.0)
    base = 55.0 + _series_score(sector["flow_ratio_3d"], -0.8, 1.8) * 0.30
    overheated = pd.Series(np.where(sector["avg_change_3d"] >= 5.0, 12.0, 0.0), index=sector.index)
    early_rotation = pd.Series(
        np.where((sector["flow_ratio_3d"] > 0.25) & (sector["avg_change_3d"].between(-1.5, 2.5)), 12.0, 0.0),
        index=sector.index,
    )
    width_bonus = _series_score(sector["sector_width_pct"], 35.0, 68.0, default=45.0) * 0.10
    sector["sector_rotation_score"] = (base + early_rotation + width_bonus - overheated).clip(30, 100)
    sector["theme_continuity_score_10"] = (sector["sector_rotation_score"] / 10.0).clip(0.0, 10.0).round(1)
    sector["theme_continuity_level"] = np.select(
        [
            sector["theme_continuity_score_10"] >= 8.0,
            sector["theme_continuity_score_10"] >= 6.0,
        ],
        ["HIGH", "MEDIUM"],
        default="LOW",
    )
    sector["theme_continuity_reason"] = sector.apply(
        lambda r: (
            f"题材延续性{float(r['theme_continuity_score_10']):.1f}/10，"
            f"3日资金{float(r['main_net_inflow'])/1e8:.2f}亿，"
            f"宽度{float(r['sector_width_pct']):.1f}%，"
            f"板块3日均涨幅{float(r['avg_change_3d']):.1f}%"
        ),
        axis=1,
    )
    min_sector_flow = runtime_threshold("min_sector_flow_amount_3d", MIN_SECTOR_FLOW_AMOUNT_3D)
    min_sector_rotation = runtime_threshold("min_sector_rotation_score", MIN_SECTOR_ROTATION_SCORE)
    sector["sector_gate_status"] = np.select(
        [
            (sector["main_net_inflow"] >= min_sector_flow)
            & (sector["sector_width_pct"].between(35.0, 85.0))
            & (sector["sector_rotation_score"] >= min_sector_rotation),
            (sector["main_net_inflow"] < 0)
            & (sector["avg_change_3d"] < 0)
            & (sector["sector_width_pct"] < 35.0),
        ],
        ["PASS", "BLOCK"],
        default="WATCH",
    )
    sector["sector_gate_reason"] = sector.apply(
        lambda r: (
            f"3日主力净流入{float(r['main_net_inflow'])/1e8:.2f}亿，宽度{float(r['sector_width_pct']):.1f}%，"
            f"轮动{float(r['sector_rotation_score']):.1f}，延续性{float(r['theme_continuity_score_10']):.1f}/10，"
            f"阈值{min_sector_flow/1e8:.1f}亿/{min_sector_rotation:.1f}"
        ),
        axis=1,
    )
    stock_sector = (
        merged.groupby(["stock_code", "industry_name"], as_index=False)
        .agg(
            stock_change_3d=("change_pct", "sum"),
            stock_amount_3d=("amount", "sum"),
            stock_main_net_inflow_3d=("main_net_inflow", "sum"),
        )
    )

    def _rank_strength(series: pd.Series) -> pd.Series:
        values = pd.to_numeric(series, errors="coerce").fillna(0.0)
        ranks = values.rank(method="min", ascending=False)
        count = float(len(values))
        if count <= 1:
            return pd.Series(100.0, index=series.index)
        return ((count - ranks) / (count - 1.0) * 100.0).clip(0.0, 100.0)

    grouped_stock = stock_sector.groupby("industry_name", group_keys=False)
    stock_sector["sector_amount_rank_pct"] = grouped_stock["stock_amount_3d"].transform(_rank_strength)
    stock_sector["sector_flow_rank_pct"] = grouped_stock["stock_main_net_inflow_3d"].transform(_rank_strength)
    stock_sector["sector_change_rank_pct"] = grouped_stock["stock_change_3d"].transform(_rank_strength)
    stock_sector["sector_amount_rank"] = grouped_stock["stock_amount_3d"].rank(method="min", ascending=False)
    stock_sector["sector_leadership_score"] = (
        stock_sector["sector_amount_rank_pct"] * 0.40
        + stock_sector["sector_flow_rank_pct"] * 0.35
        + stock_sector["sector_change_rank_pct"] * 0.25
    ).clip(0.0, 100.0)
    stock_sector["sector_leadership_tier"] = np.select(
        [
            (stock_sector["sector_leadership_score"] >= 85.0) & (stock_sector["sector_amount_rank"] <= 3),
            stock_sector["sector_leadership_score"] >= 65.0,
            stock_sector["sector_leadership_score"] >= 45.0,
        ],
        ["leader", "front", "middle"],
        default="follower",
    )
    out = codes.merge(
        sector[[
            "industry_name", "sector_rotation_score", "sector_gate_status", "sector_gate_reason",
            "main_net_inflow", "sector_width_pct", "avg_change_3d",
            "theme_continuity_score_10", "theme_continuity_level", "theme_continuity_reason",
        ]],
        on="industry_name",
        how="left",
    )
    out = out.merge(
        stock_sector[[
            "stock_code", "sector_leadership_score", "sector_leadership_tier",
            "sector_amount_rank", "stock_change_3d", "stock_amount_3d", "stock_main_net_inflow_3d",
        ]],
        on="stock_code",
        how="left",
    )
    out["sector_rotation_score"] = pd.to_numeric(out["sector_rotation_score"], errors="coerce").fillna(55.0)
    out["sector_gate_status"] = out["sector_gate_status"].fillna("WATCH")
    out["sector_gate_reason"] = out["sector_gate_reason"].fillna("板块数据不足，先按观察处理")
    out["sector_flow_3d"] = pd.to_numeric(out["main_net_inflow"], errors="coerce").fillna(0.0)
    out["sector_width_pct"] = pd.to_numeric(out["sector_width_pct"], errors="coerce").fillna(0.0)
    out["sector_avg_change_3d"] = pd.to_numeric(out["avg_change_3d"], errors="coerce").fillna(0.0)
    out["theme_continuity_score_10"] = pd.to_numeric(out["theme_continuity_score_10"], errors="coerce").fillna(5.5)
    out["theme_continuity_level"] = out["theme_continuity_level"].fillna("LOW").astype(str)
    out["theme_continuity_reason"] = out["theme_continuity_reason"].fillna("题材延续性数据不足，按低延续观察").astype(str)
    out["sector_leadership_score"] = pd.to_numeric(out["sector_leadership_score"], errors="coerce").fillna(50.0)
    out["sector_leadership_tier"] = out["sector_leadership_tier"].fillna("middle").astype(str)
    out["sector_amount_rank"] = pd.to_numeric(out["sector_amount_rank"], errors="coerce").fillna(0.0)
    out["stock_change_3d"] = pd.to_numeric(out["stock_change_3d"], errors="coerce").fillna(0.0)
    out["stock_amount_3d"] = pd.to_numeric(out["stock_amount_3d"], errors="coerce").fillna(0.0)
    out["stock_main_net_inflow_3d"] = pd.to_numeric(out["stock_main_net_inflow_3d"], errors="coerce").fillna(0.0)
    return out[[
        "stock_code", "industry_name", "sector_rotation_score", "sector_gate_status",
        "sector_gate_reason", "sector_flow_3d", "sector_width_pct", "sector_avg_change_3d",
        "theme_continuity_score_10", "theme_continuity_level", "theme_continuity_reason",
        "sector_leadership_score", "sector_leadership_tier", "sector_amount_rank",
        "stock_change_3d", "stock_amount_3d", "stock_main_net_inflow_3d",
    ]]


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


def derive_position_risk_level(row: dict[str, Any], status: str | None = None) -> str:
    event_risk = str(row.get("event_risk_level") or "LOW").upper()
    signal_status = str(status or row.get("signal_status") or "").upper()
    raw_flags = row.get("data_quality_flags")
    flags: set[str] = set()
    if isinstance(raw_flags, list):
        flags = {str(item) for item in raw_flags}
    elif isinstance(raw_flags, str):
        try:
            parsed = json.loads(raw_flags)
            if isinstance(parsed, list):
                flags = {str(item) for item in parsed}
        except Exception:
            flags = set()

    high_flags = {
        "market_extreme_overheat",
        "weekly_overheat",
        "blowoff_volume_risk",
        "margin_deleveraging_3d",
        "liquidity_hard_floor",
        "fundamental_loss",
        "performance_deterioration",
        "negative_oper_cash_flow",
        "negative_free_cash_flow",
        "negative_ebit_margin",
        "mine_clearance_risk",
        "unlock_risk",
        "pledge_ratio_high",
        "shareholder_reduction_high",
        "goodwill_ratio_high",
        "classic_top_breakdown",
    }
    medium_flags = {
        "valuation_overpriced",
        "industry_relative_overvalued",
        "industry_relative_ps_overvalued",
        "valuation_history_percentile_high",
        "liquidity_avg_amount_low",
        "turnover_out_of_range",
        "order_book_depth_low",
        "order_book_imbalance",
        "float_market_cap_low",
        "macro_policy_pressure",
        "macro_indicator_pressure",
        "etf_flow_pressure",
        "north_flow_pressure",
        "north_stock_outflow",
        "north_stock_underweight",
        "market_relative_weak",
        "institutional_profile_weak",
        "investor_interaction_risk",
        "retail_institution_contrarian_risk",
        "business_purity_low",
        "industry_prosperity_weak",
        "qoq_performance_drop",
        "quick_ratio_low",
        "roa_below_threshold",
        "roic_below_threshold",
        "receivable_ratio_high",
        "prepayment_growth_high",
        "related_transaction_ratio_high",
        "institutional_lhb_outflow",
        "theme_continuity_low",
        "positive_event_priced_in",
        "minor_unlock_watch",
        "goodwill_ratio_watch",
    }
    if event_risk in {"HIGH", "CRITICAL"} or signal_status in {"BLOCK", "SELL_ALERT"} or flags & high_flags:
        return "HIGH"
    if event_risk == "MEDIUM" or signal_status in {"WATCH", "SUSPENDED"} or flags & medium_flags:
        return "MEDIUM"
    return "LOW"


def _position_weight(row: dict[str, Any], strategy: str, status: str) -> float:
    profile = STRATEGY_PROFILES[strategy]
    position_risk = derive_position_risk_level(row, status)
    if status in {"BLOCK", "SELL_ALERT"}:
        return 0.0
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
    risk_cap = POSITION_RISK_CAPS.get(position_risk, POSITION_RISK_CAPS["MEDIUM"])
    max_weight = min(SYSTEM_SINGLE_POSITION_CAP, risk_cap)
    return round(max(1.0, min(max_weight, weight)), 2)


def _position_cap_pct(row: dict[str, Any], status: str | None = None) -> float:
    risk_level = derive_position_risk_level(row, status)
    return min(SYSTEM_SINGLE_POSITION_CAP, POSITION_RISK_CAPS.get(risk_level, POSITION_RISK_CAPS["MEDIUM"]))


def _ma_direction_from_deduction(close: float, deduction_price: float) -> str:
    if close <= 0 or deduction_price <= 0:
        return "UNKNOWN"
    if close > deduction_price:
        return "向上"
    if close < deduction_price:
        return "向下"
    return "走平"


def _ma_position_and_deviation(close: float, average: float) -> tuple[str, float | None]:
    if close <= 0 or average <= 0:
        return "UNKNOWN", None
    deviation = (close / average - 1.0) * 100.0
    if deviation > 0:
        position = "上方"
    elif deviation < 0:
        position = "下方"
    else:
        position = "贴合"
    return position, round(deviation, 2)


def _ma_stage(
    label: str,
    *,
    close: float,
    average: float,
    deviation_pct: float | None,
    direction: str,
    ma5: float,
    ma10: float,
    ma20: float,
    ma60: float,
    ema12: float,
    ema26: float,
) -> str:
    if close <= 0 or average <= 0:
        return "数据不足"
    if deviation_pct is not None and abs(deviation_pct) >= 18.0:
        return "⑤大幅偏离"
    if label in {"EMA12", "EMA26"}:
        if ema12 > 0 and ema26 > 0:
            if ema12 >= ema26 and close >= average:
                return "④多头排列"
            if ema12 < ema26 and close < average:
                return "④空头排列"
        return "②拐头待验证"
    if label == "SMA20":
        if close > ma20 > 0 and ma5 >= ma10 >= ma20:
            return "④多头排列"
        if close < ma20 and ma5 < ma10 < ma20:
            return "④空头排列"
        if ma20 > 0 and ma60 > 0 and ma20 >= ma60:
            return "③金叉后"
        if ma20 > 0 and ma60 > 0 and ma20 < ma60:
            return "③死叉后"
    if label == "SMA60":
        if ma20 > 0 and ma60 > 0 and close >= ma60 and ma20 >= ma60:
            return "③金叉后"
        if ma20 > 0 and ma60 > 0 and close < ma60 and ma20 < ma60:
            return "③死叉后"
    if direction == "向上":
        return "②拐头向上"
    if direction == "向下":
        return "②拐头向下"
    return "①破线/贴线"


def build_moving_average_table(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Build the stock.txt moving-average table with stage and direction."""
    close = _safe_number(row.get("close"), 0.0)
    ma5 = _safe_number(row.get("ma5"), 0.0)
    ma10 = _safe_number(row.get("ma10"), 0.0)
    ma20 = _safe_number(row.get("ma20"), 0.0)
    ma60 = _safe_number(row.get("ma60"), 0.0)
    ema12 = _safe_number(row.get("ema12"), 0.0)
    ema26 = _safe_number(row.get("ema26"), 0.0)
    specs = [
        ("EMA12", ema12, 0.0),
        ("EMA26", ema26, 0.0),
        ("SMA20", ma20, _safe_number(row.get("deduction_price_20"), 0.0)),
        ("SMA60", ma60, _safe_number(row.get("deduction_price_60"), 0.0)),
        ("SMA120", _safe_number(row.get("ma120"), 0.0), _safe_number(row.get("deduction_price_120"), 0.0)),
        ("SMA250", _safe_number(row.get("ma250"), 0.0), _safe_number(row.get("deduction_price_250"), 0.0)),
    ]
    table: list[dict[str, Any]] = []
    for label, average, deduction in specs:
        if average <= 0:
            continue
        position, deviation = _ma_position_and_deviation(close, average)
        if label.startswith("SMA"):
            direction = _ma_direction_from_deduction(close, deduction)
        elif ema12 > 0 and ema26 > 0:
            direction = "向上" if ema12 >= ema26 and close >= average else ("向下" if ema12 < ema26 and close < average else "走平")
        else:
            direction = "UNKNOWN"
        table.append({
            "name": label,
            "value": round(average, 2),
            "price_position": position,
            "deviation_pct": deviation,
            "stage": _ma_stage(
                label,
                close=close,
                average=average,
                deviation_pct=deviation,
                direction=direction,
                ma5=ma5,
                ma10=ma10,
                ma20=ma20,
                ma60=ma60,
                ema12=ema12,
                ema26=ema26,
            ),
            "direction": direction,
            "deduction_price": round(deduction, 2) if deduction > 0 else None,
        })
    return table


def classify_ema_sma_divergence(row: dict[str, Any]) -> dict[str, Any]:
    """Compare fast EMA momentum with SMA trend confirmation."""
    close = _safe_number(row.get("close"), 0.0)
    ema12 = _safe_number(row.get("ema12"), 0.0)
    ema26 = _safe_number(row.get("ema26"), 0.0)
    ma20 = _safe_number(row.get("ma20"), 0.0)
    ma60 = _safe_number(row.get("ma60"), 0.0)
    if min(close, ema12, ema26, ma20, ma60) <= 0:
        return {
            "status": "UNKNOWN",
            "text": "EMA/SMA data unavailable for divergence validation",
            "ema_bias": "UNKNOWN",
            "sma_bias": "UNKNOWN",
        }
    ema_bull = ema12 >= ema26
    sma_bull = close >= ma20 and ma20 >= ma60
    if ema_bull and sma_bull:
        status = "SYNC_BULLISH"
        text_value = "EMA与SMA同步偏多"
    elif (not ema_bull) and (not sma_bull):
        status = "SYNC_BEARISH"
        text_value = "EMA与SMA同步偏弱"
    elif ema_bull and not sma_bull:
        status = "EMA_LEADS_SMA_PENDING"
        text_value = "EMA领先拐头，SMA待验证"
    else:
        status = "EMA_WEAK_SMA_SUPPORT"
        text_value = "EMA转弱，SMA趋势仍待破坏确认"
    return {
        "status": status,
        "text": text_value,
        "ema_bias": "BULLISH" if ema_bull else "BEARISH",
        "sma_bias": "BULLISH" if sma_bull else "BEARISH",
    }


def build_deduction_projection(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Build stock.txt SMA20/SMA60 deduction-price projection details."""
    close = _safe_number(row.get("close"), 0.0)
    projections: list[dict[str, Any]] = []
    for period in (20, 60):
        deduction_price = _safe_number(row.get(f"deduction_price_{period}"), 0.0)
        if close <= 0 or deduction_price <= 0:
            continue
        diff = close - deduction_price
        if diff > 0:
            direction = "向上"
            forecast = f"若未来3日收盘维持在{close:.2f}元附近，SMA{period}预计继续向上拐"
        elif diff < 0:
            direction = "向下"
            forecast = f"若未来3日收盘维持在{close:.2f}元附近，SMA{period}预计继续向下拐"
        else:
            direction = "走平"
            forecast = f"若未来3日收盘维持在{close:.2f}元附近，SMA{period}预计维持走平"
        projections.append({
            "period": period,
            "deduction_price": round(deduction_price, 2),
            "deduction_date": str(row.get(f"deduction_date_{period}") or "")[:10],
            "current_close": round(close, 2),
            "diff": round(diff, 2),
            "direction": direction,
            "forecast_3d": forecast,
            "definition": f"SMA{period}今日值 = 过去{period - 1}日收盘价与今日收盘价之和 / {period}",
        })
    return projections


def build_technical_evidence(row: dict[str, Any]) -> dict[str, Any]:
    close = _safe_number(row.get("close"), 0.0)
    ma5 = _safe_number(row.get("ma5"), 0.0)
    ma10 = _safe_number(row.get("ma10"), 0.0)
    ma20 = _safe_number(row.get("ma20"), 0.0)
    ma60 = _safe_number(row.get("ma60"), 0.0)
    ma120 = _safe_number(row.get("ma120"), 0.0)
    ma250 = _safe_number(row.get("ma250"), 0.0)
    ema12 = _safe_number(row.get("ema12"), 0.0)
    ema26 = _safe_number(row.get("ema26"), 0.0)
    deduction20 = _safe_number(row.get("deduction_price_20"), 0.0)
    deduction60 = _safe_number(row.get("deduction_price_60"), 0.0)
    dist_ma20 = _safe_number(row.get("dist_ma20"), 0.0)
    amount_ratio_20 = _safe_number(row.get("amount_ratio_20"), 0.0)
    pct_5 = _safe_number(row.get("pct_5"), 0.0)
    pct_20 = _safe_number(row.get("pct_20"), 0.0)
    bbi = _safe_number(row.get("bbi"), 0.0)
    bias6 = _safe_number(row.get("bias6"), 0.0)
    bias12 = _safe_number(row.get("bias12"), 0.0)
    bias24 = _safe_number(row.get("bias24"), 0.0)
    raw_mtm10_pct = row.get("mtm10_pct")
    raw_lwr9 = row.get("lwr9")
    raw_macd_dif = row.get("macd_dif", row.get("dif"))
    raw_macd_dea = row.get("macd_dea", row.get("dea"))
    raw_macd_hist = row.get("macd_hist")
    macd_dif = _safe_number(raw_macd_dif, 0.0)
    macd_dea = _safe_number(raw_macd_dea, 0.0)
    macd_hist = _safe_number(raw_macd_hist, 0.0)
    mtm10_pct = _safe_number(raw_mtm10_pct, 0.0)
    lwr9 = _safe_number(raw_lwr9, 0.0)
    kdj_k = _safe_number(row.get("kdj_k"), 0.0)
    kdj_d = _safe_number(row.get("kdj_d"), 0.0)
    kdj_j = _safe_number(row.get("kdj_j"), 0.0)
    rsi6 = _safe_number(row.get("rsi6"), 0.0)
    rsi12 = _safe_number(row.get("rsi12"), 0.0)
    rsi24 = _safe_number(row.get("rsi24"), 0.0)
    boll_mid = _safe_number(row.get("boll_mid"), 0.0)
    boll_upper = _safe_number(row.get("boll_upper"), 0.0)
    boll_lower = _safe_number(row.get("boll_lower"), 0.0)
    boll_width = _safe_number(row.get("boll_width_pct"), 0.0)
    pdi14 = _safe_number(row.get("pdi14"), 0.0)
    mdi14 = _safe_number(row.get("mdi14"), 0.0)
    adx14 = _safe_number(row.get("adx14"), 0.0)
    chan_signal = str(row.get("chan_signal") or "")
    chan_summary = str(row.get("chan_summary") or "")
    chan_status = str(row.get("chan_pivot_status") or "")
    kline_pattern = str(row.get("kline_pattern") or "")
    kline_pattern_direction = str(row.get("kline_pattern_direction") or "neutral").lower()
    kline_pattern_reason = str(row.get("kline_pattern_reason") or "")
    kline_pattern_strength = _safe_number(row.get("kline_pattern_strength"), 0.0)
    classic_pattern = str(row.get("classic_pattern") or "")
    classic_pattern_direction = str(row.get("classic_pattern_direction") or "neutral").lower()
    classic_pattern_status = str(row.get("classic_pattern_status") or "")
    classic_pattern_reason = str(row.get("classic_pattern_reason") or "")
    classic_pattern_strength = _safe_number(row.get("classic_pattern_strength"), 0.0)
    moving_average_table = build_moving_average_table(row)
    ema_sma_divergence = classify_ema_sma_divergence(row)
    deduction_projection = build_deduction_projection(row)

    items: list[dict[str, Any]] = []
    if close > 0 and ma20 > 0:
        if close > ma20 and ma5 >= ma10 >= ma20:
            clock = "1-2点钟上升趋势"
            direction = "bullish"
        elif close < ma20 and ma5 < ma10 < ma20:
            clock = "4-6点钟下降趋势"
            direction = "bearish"
        else:
            clock = "震荡/切换区"
            direction = "neutral"
        items.append({
            "kind": "trend_clock",
            "direction": direction,
            "value": clock,
            "text": f"收盘{close:.2f}，MA5/10/20={ma5:.2f}/{ma10:.2f}/{ma20:.2f}",
            "threshold": "上升需 close>MA20 且 MA5>=MA10>=MA20；下降反之",
        })
    else:
        clock = "趋势证据不足"
        direction = "neutral"

    if moving_average_table:
        items.append({
            "kind": "moving_average_table",
            "direction": "bullish" if ema_sma_divergence.get("status") == "SYNC_BULLISH" else (
                "bearish" if ema_sma_divergence.get("status") == "SYNC_BEARISH" else "neutral"
            ),
            "value": {
                "rows": moving_average_table,
                "ema_sma_divergence": ema_sma_divergence,
            },
            "text": str(ema_sma_divergence.get("text") or "EMA/SMA divergence validation unavailable"),
            "threshold": "按 EMA12/EMA26、SMA20/60/120/250 输出现价位置、偏离、五步法阶段和方向",
        })

    if close > 0 and ma5 > 0:
        dist_ma5 = (close / ma5 - 1.0) * 100.0
        pullback_ok = abs(dist_ma5) <= 3.0 and pct_5 < 20.0
        items.append({
            "kind": "ma5_pullback",
            "direction": "bullish" if pullback_ok and close >= ma5 * 0.985 else ("risk" if pct_5 >= 20.0 else "neutral"),
            "value": {"dist_ma5": round(dist_ma5, 2), "pct_5": round(pct_5, 2)},
            "text": f"距MA5 {dist_ma5:.1f}%，近5日涨幅{pct_5:.1f}%",
            "threshold": "短线优先等回调到MA5附近，近一周涨幅超过20%视为追高风险",
        })

    profile_peak = _safe_number(row.get("volume_profile_peak_price"), 0.0)
    profile_support = _safe_number(row.get("volume_profile_support_price"), 0.0)
    profile_resistance = _safe_number(row.get("volume_profile_resistance_price"), 0.0)
    profile_density = _safe_number(row.get("volume_profile_peak_density"), 0.0)
    if close > 0 and (profile_peak > 0 or profile_support > 0 or profile_resistance > 0):
        upside_to_profile_resistance = (
            (profile_resistance / close - 1.0) * 100.0 if profile_resistance > close else 0.0
        )
        support_distance = (
            (close / profile_support - 1.0) * 100.0 if profile_support > 0 else 0.0
        )
        profile_direction = "bullish" if upside_to_profile_resistance >= 5.0 and support_distance <= 12.0 else (
            "risk" if profile_resistance > 0 and close > profile_resistance * 1.03 else "neutral"
        )
        items.append({
            "kind": "volume_profile",
            "direction": profile_direction,
            "value": {
                "peak": _round_price(profile_peak),
                "support": _round_price(profile_support),
                "resistance": _round_price(profile_resistance),
                "peak_density": round(profile_density, 4),
            },
            "text": (
                f"成交密集峰{_round_price(profile_peak) or '-'}，"
                f"支撑{_round_price(profile_support) or '-'}，压力{_round_price(profile_resistance) or '-'}"
            ),
            "threshold": "优先参考近90日成交密集区作为支撑/压力，避免只看前高前低",
        })

    if kline_pattern and kline_pattern not in {"none", "nan"}:
        pattern_direction = "risk" if kline_pattern_direction in {"bearish", "risk"} else (
            "bullish" if kline_pattern_direction == "bullish" else "neutral"
        )
        items.append({
            "kind": "kline_pattern",
            "direction": pattern_direction,
            "value": {
                "pattern": kline_pattern,
                "strength": round(kline_pattern_strength, 1),
            },
            "text": kline_pattern_reason or f"K线形态 {kline_pattern}",
            "threshold": "K线形态只作为量价和趋势的确认项；高位看空形态触发暂缓追高",
        })

    if classic_pattern and classic_pattern not in {"none", "nan"}:
        classic_direction = "risk" if classic_pattern_direction in {"bearish", "risk"} else (
            "bullish" if classic_pattern_direction == "bullish" else "neutral"
        )
        items.append({
            "kind": "classic_pattern",
            "direction": classic_direction,
            "value": {
                "pattern": classic_pattern,
                "status": classic_pattern_status,
                "strength": round(classic_pattern_strength, 1),
                "neckline": _round_price(row.get("classic_pattern_neckline")),
                "support": _round_price(row.get("classic_pattern_support")),
                "resistance": _round_price(row.get("classic_pattern_resistance")),
                "wave_high": _round_price(row.get("classic_pattern_wave_high")),
                "wave_high_date": str(row.get("classic_pattern_wave_high_date") or "")[:10],
                "wave_low": _round_price(row.get("classic_pattern_wave_low")),
                "wave_low_date": str(row.get("classic_pattern_wave_low_date") or "")[:10],
                "wave_pct": _none_if_nan(row.get("classic_pattern_wave_pct")),
                "wave_direction": str(row.get("classic_pattern_wave_direction") or "UNKNOWN"),
            },
            "text": classic_pattern_reason or f"classic pattern {classic_pattern}",
            "threshold": "double top/bottom, head-and-shoulders and rounding structures require neckline confirmation",
        })

    if ema12 > 0 and ema26 > 0:
        items.append({
            "kind": "ema_momentum",
            "direction": "bullish" if ema12 >= ema26 else "bearish",
            "value": round(ema12 - ema26, 4),
            "text": f"EMA12 {ema12:.2f} {'高于' if ema12 >= ema26 else '低于'} EMA26 {ema26:.2f}",
            "threshold": "EMA12 高于 EMA26 表示短期动能占优",
        })

    if raw_macd_dif is not None and raw_macd_dea is not None and raw_macd_hist is not None:
        if not (pd.isna(raw_macd_dif) or pd.isna(raw_macd_dea) or pd.isna(raw_macd_hist)):
            macd_direction = "bullish" if macd_dif >= macd_dea and macd_hist >= 0 else ("bearish" if macd_dif < macd_dea and macd_hist < 0 else "neutral")
            items.append({
                "kind": "macd",
                "direction": macd_direction,
                "value": {"dif": round(macd_dif, 4), "dea": round(macd_dea, 4), "hist": round(macd_hist, 4)},
                "text": f"MACD DIF/DEA/HIST={macd_dif:.3f}/{macd_dea:.3f}/{macd_hist:.3f}",
                "threshold": "MACD 只作动能确认，不能脱离趋势和资金单独触发买入",
            })

    if ma120 > 0 or ma250 > 0:
        items.append({
            "kind": "long_ma",
            "direction": "bullish" if close > max(ma120, ma250, 0) else "neutral",
            "value": {"ma120": round(ma120, 2) if ma120 else None, "ma250": round(ma250, 2) if ma250 else None},
            "text": f"长周期均线 MA120={ma120:.2f} / MA250={ma250:.2f}",
            "threshold": "站上长均线优于长均线压制",
        })

    if bbi > 0:
        items.append({
            "kind": "bbi",
            "direction": "bullish" if close >= bbi else "bearish",
            "value": round(bbi, 2),
            "text": f"BBI {bbi:.2f}，收盘{close:.2f}",
            "threshold": "收盘价站上 BBI 表示多周期均价承接较好",
        })

    if any(abs(v) > 0 for v in (bias6, bias12, bias24)):
        bias_direction = "risk" if bias6 >= 10.0 or bias12 >= 15.0 else ("bullish" if bias6 > 0 and bias12 > 0 else "neutral")
        items.append({
            "kind": "bias",
            "direction": bias_direction,
            "value": {"bias6": round(bias6, 2), "bias12": round(bias12, 2), "bias24": round(bias24, 2)},
            "text": f"BIAS6/12/24={bias6:.1f}%/{bias12:.1f}%/{bias24:.1f}%",
            "threshold": "短期乖离过大时降低追高优先级",
        })

    if raw_mtm10_pct is not None and not pd.isna(raw_mtm10_pct):
        items.append({
            "kind": "mtm",
            "direction": "bullish" if mtm10_pct >= 0 else "bearish",
            "value": round(mtm10_pct, 2),
            "text": f"MTM10 {mtm10_pct:.1f}%",
            "threshold": "10日动量为正表示短期价格仍在扩张",
        })

    if raw_lwr9 is not None and not pd.isna(raw_lwr9):
        items.append({
            "kind": "lwr",
            "direction": "bullish" if lwr9 <= 20.0 else ("risk" if lwr9 >= 80.0 else "neutral"),
            "value": round(lwr9, 2),
            "text": f"LWR9 {lwr9:.1f}",
            "threshold": "LWR 接近低位表示收盘靠近近期高位，接近高位表示短线转弱",
        })

    if any(v > 0 for v in (kdj_k, kdj_d, kdj_j)):
        if kdj_k >= kdj_d and kdj_j < 95.0:
            kdj_direction = "bullish"
        elif kdj_j >= 100.0 or kdj_k < kdj_d:
            kdj_direction = "risk"
        else:
            kdj_direction = "neutral"
        items.append({
            "kind": "kdj",
            "direction": kdj_direction,
            "value": {"k": round(kdj_k, 2), "d": round(kdj_d, 2), "j": round(kdj_j, 2)},
            "text": f"KDJ K/D/J={kdj_k:.1f}/{kdj_d:.1f}/{kdj_j:.1f}",
            "threshold": "K 上穿 D 且 J 不极端时确认短线动能，J 过高提示冲高风险",
        })

    if any(v > 0 for v in (rsi6, rsi12, rsi24)):
        if rsi6 >= 80.0:
            rsi_direction = "risk"
        elif rsi6 >= 45.0 and rsi6 <= 72.0 and (rsi12 <= 0 or rsi6 >= rsi12):
            rsi_direction = "bullish"
        elif 0 < rsi6 <= 35.0:
            rsi_direction = "bearish"
        else:
            rsi_direction = "neutral"
        items.append({
            "kind": "rsi",
            "direction": rsi_direction,
            "value": {"rsi6": round(rsi6, 2), "rsi12": round(rsi12, 2), "rsi24": round(rsi24, 2)},
            "text": f"RSI6/12/24={rsi6:.1f}/{rsi12:.1f}/{rsi24:.1f}",
            "threshold": "RSI 位于中强区且未过热更适合跟踪，80以上降低追高优先级",
        })

    if boll_mid > 0:
        if boll_upper > 0 and close > boll_upper:
            boll_direction = "risk"
        elif boll_lower > 0 and close < boll_lower:
            boll_direction = "bearish"
        elif close >= boll_mid:
            boll_direction = "bullish"
        else:
            boll_direction = "neutral"
        items.append({
            "kind": "boll",
            "direction": boll_direction,
            "value": {
                "upper": round(boll_upper, 2) if boll_upper else None,
                "mid": round(boll_mid, 2),
                "lower": round(boll_lower, 2) if boll_lower else None,
                "width_pct": round(boll_width, 2) if boll_width else None,
            },
            "text": f"BOLL 上/中/下={boll_upper:.2f}/{boll_mid:.2f}/{boll_lower:.2f}",
            "threshold": "站上中轨偏强，突破上轨需结合量价防止冲高回落",
        })

    if pdi14 > 0 or mdi14 > 0:
        if pdi14 > mdi14 and adx14 >= 20.0:
            dmi_direction = "bullish"
        elif mdi14 > pdi14 and adx14 >= 20.0:
            dmi_direction = "bearish"
        else:
            dmi_direction = "neutral"
        items.append({
            "kind": "dmi",
            "direction": dmi_direction,
            "value": {"pdi14": round(pdi14, 2), "mdi14": round(mdi14, 2), "adx14": round(adx14, 2)},
            "text": f"DMI PDI/MDI/ADX={pdi14:.1f}/{mdi14:.1f}/{adx14:.1f}",
            "threshold": "PDI 高于 MDI 且 ADX 达到20以上表示趋势强度更可靠",
        })

    if chan_summary:
        if chan_signal in {"third_buy", "second_buy_watch", "first_buy_watch"}:
            chan_direction = "bullish"
        elif chan_signal in {"third_sell", "second_sell_watch", "first_sell_watch"}:
            chan_direction = "risk"
        else:
            chan_direction = "neutral"
        items.append({
            "kind": "chan_structure",
            "direction": chan_direction,
            "value": {
                "status": chan_status,
                "signal": chan_signal or "observe",
                "center_low": _round_price(row.get("chan_center_low")),
                "center_high": _round_price(row.get("chan_center_high")),
                "support_price": _round_price(row.get("chan_support_price")),
                "resistance_price": _round_price(row.get("chan_resistance_price")),
                "invalidation_price": _round_price(row.get("chan_invalidation_price")),
                "divergence": row.get("chan_divergence") or "none",
            },
            "text": chan_summary,
            "threshold": "中枢上破看三买，下破看三卖；新高/新低动能收敛提示背驰",
        })

    if deduction20 > 0 or deduction60 > 0:
        items.append({
            "kind": "deduction_price",
            "direction": "bullish" if close > max(deduction20, deduction60, 0) else "risk",
            "value": {
                "deduction_price_20": round(deduction20, 2) if deduction20 else None,
                "deduction_price_60": round(deduction60, 2) if deduction60 else None,
                "projection": deduction_projection,
            },
            "text": f"20/60日抵扣价 {deduction20:.2f}/{deduction60:.2f}，现价{close:.2f}",
            "threshold": "现价站上抵扣价表示筹码换手后的趋势承接较好；未来3日维持现价附近则延续对应拐头方向",
        })

    items.append({
        "kind": "extension",
        "direction": "risk" if dist_ma20 >= 18.0 else "neutral",
        "value": round(dist_ma20, 2),
        "text": f"距离MA20 {dist_ma20:.1f}%，20日涨幅{pct_20:.1f}%，量能倍数{amount_ratio_20:.2f}",
        "threshold": "距离MA20过大且涨幅高时降低追高优先级",
    })
    return {
        "trend_clock": clock,
        "direction": direction,
        "moving_average_table": moving_average_table,
        "ema_sma_divergence": ema_sma_divergence,
        "deduction_projection": deduction_projection,
        "items": items[:24],
    }


def build_failure_tags(row: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    flags = []
    raw_flags = row.get("data_quality_flags")
    if isinstance(raw_flags, str):
        try:
            flags = json.loads(raw_flags)
        except Exception:
            flags = []
    elif isinstance(raw_flags, list):
        flags = raw_flags
    if "downtrend_clock" in flags:
        tags.append("trend_break")
    if "main_outflow_3d" in flags or "main_outflow_10d" in flags or _safe_number(row.get("main_outflow_days_3d"), 0.0) >= 3:
        tags.append("capital_outflow")
    if "weak_sector" in flags or str(row.get("sector_gate_status") or "").upper() == "BLOCK":
        tags.append("sector_weak")
    if "theme_continuity_low" in flags:
        tags.append("theme_continuity_low")
    if _safe_number(row.get("change_pct"), 0.0) >= 7.0 or _safe_number(row.get("dist_ma20"), 0.0) >= 18.0:
        tags.append("chasing_risk")
    if "weekly_overheat" in flags or _safe_number(row.get("pct_5"), 0.0) >= 20.0:
        tags.append("weekly_overheat")
    if "positive_event_priced_in" in flags:
        tags.append("event_priced_in")
    if "holder_spread" in flags:
        tags.append("holder_spread")
    if "institutional_lhb_outflow" in flags:
        tags.append("institutional_outflow")
    if "margin_deleveraging_3d" in flags:
        tags.append("margin_deleveraging")
    if "unlock_risk" in flags:
        tags.append("unlock_risk")
    if "minor_unlock_watch" in flags:
        tags.append("unlock_watch")
    if "pledge_ratio_high" in flags:
        tags.append("pledge_risk")
    if "shareholder_reduction_high" in flags:
        tags.append("shareholder_reduction")
    if "mine_clearance_risk" in flags:
        tags.append("mine_clearance_risk")
    if "market_extreme_overheat" in flags:
        tags.append("market_extreme_overheat")
    if (
        "valuation_overpriced" in flags
        or "industry_relative_overvalued" in flags
        or "industry_relative_ps_overvalued" in flags
        or "valuation_history_percentile_high" in flags
        or _safe_number(row.get("valuation_score"), 55.0) <= 35.0
    ):
        tags.append("valuation_expensive")
    if "goodwill_ratio_high" in flags or "goodwill_ratio_watch" in flags:
        tags.append("goodwill_risk")
    if "bear_market_growth_pause" in flags:
        tags.append("style_mismatch")
    if "north_flow_pressure" in flags:
        tags.append("north_flow_pressure")
    if "macro_policy_pressure" in flags:
        tags.append("macro_policy_pressure")
    if "macro_indicator_pressure" in flags:
        tags.append("macro_indicator_pressure")
    if "etf_flow_pressure" in flags:
        tags.append("etf_flow_pressure")
    if "north_stock_outflow" in flags or "north_stock_underweight" in flags:
        tags.append("north_stock_weak")
    if "institutional_profile_weak" in flags:
        tags.append("institutional_profile_weak")
    if "investor_interaction_risk" in flags:
        tags.append("investor_interaction_risk")
    if "retail_institution_contrarian_risk" in flags:
        tags.append("retail_institution_contrarian_risk")
    if "business_purity_low" in flags:
        tags.append("business_purity_low")
    if "industry_prosperity_weak" in flags:
        tags.append("industry_prosperity_weak")
    if "classic_top_breakdown" in flags:
        tags.append("classic_pattern_risk")
    if "market_relative_weak" in flags:
        tags.append("relative_weak")
    if (
        "liquidity_hard_floor" in flags
        or "liquidity_avg_amount_low" in flags
        or "turnover_out_of_range" in flags
        or "order_book_depth_low" in flags
        or "order_book_imbalance" in flags
    ):
        tags.append("liquidity_risk")
    if "float_market_cap_low" in flags:
        tags.append("float_market_cap_low")
    if "blowoff_volume_risk" in flags:
        tags.append("volume_overheat")
    if "volume_shrink_weak" in flags:
        tags.append("volume_shrink")
    if (
        "fundamental_loss" in flags
        or "performance_deterioration" in flags
        or "qoq_performance_drop" in flags
        or "profit_momentum_weak" in flags
        or "growth_threshold_miss" in flags
        or "debt_ratio_over_cap" in flags
        or "roe_below_threshold" in flags
        or "roa_below_threshold" in flags
        or "gross_margin_below_threshold" in flags
        or "quick_ratio_low" in flags
        or "roic_below_threshold" in flags
        or "receivable_ratio_high" in flags
        or "prepayment_growth_high" in flags
        or "related_transaction_ratio_high" in flags
    ):
        tags.append("fundamental_weak")
    if str(row.get("event_risk_level") or "").upper() in {"HIGH", "CRITICAL"}:
        tags.append("event_risk")
    min_rr = runtime_threshold("min_risk_reward", MIN_EXECUTABLE_RISK_REWARD)
    if _safe_number(row.get("risk_reward_ratio"), min_rr) < min_rr:
        tags.append("low_risk_reward")
    return sorted(set(tags))


def estimate_trade_probabilities(row: dict[str, Any]) -> dict[str, Any]:
    """Estimate upside/downside probabilities from rule scores; not a prediction guarantee."""
    score = _safe_number(row.get("final_trade_score"), _safe_number(row.get("ai_score"), 55.0))
    risk_reward = _safe_number(row.get("risk_reward_ratio"), 0.0)
    quality = _safe_number(row.get("data_quality_score"), 80.0)
    volatility = _safe_number(row.get("volatility_20"), 5.0)
    position_risk = str(row.get("position_risk_level") or derive_position_risk_level(row, row.get("signal_status"))).upper()
    event_risk = str(row.get("event_risk_level") or "LOW").upper()
    status = str(row.get("recommend_status") or row.get("signal_status") or "WATCH").upper()

    upside = 42.0 + (score - 60.0) * 0.55 + min(max(risk_reward - 2.0, -1.0), 4.0) * 3.0
    upside += (quality - 70.0) * 0.08
    if status in {"ALLOW", "CONFIRM", "BUY_READY"}:
        upside += 4.0
    if position_risk == "MEDIUM":
        upside -= 5.0
    elif position_risk == "HIGH":
        upside -= 12.0
    if event_risk == "MEDIUM":
        upside -= 4.0
    elif event_risk == "HIGH":
        upside -= 10.0
    elif event_risk == "CRITICAL":
        upside -= 18.0
    upside -= max(volatility - 6.0, 0.0) * 0.8
    upside = clamp_score(upside, 10.0, 85.0)

    downside = 30.0 + max(60.0 - score, 0.0) * 0.35 + max(volatility - 4.0, 0.0) * 1.1
    if position_risk == "MEDIUM":
        downside += 5.0
    elif position_risk == "HIGH":
        downside += 13.0
    if event_risk == "MEDIUM":
        downside += 3.0
    elif event_risk == "HIGH":
        downside += 9.0
    elif event_risk == "CRITICAL":
        downside += 18.0
    downside = clamp_score(downside, 8.0, 80.0)
    if upside + downside > 96.0:
        scale = 96.0 / (upside + downside)
        upside = round(upside * scale, 1)
        downside = round(downside * scale, 1)
    sideways = round(max(0.0, 100.0 - upside - downside), 1)
    confidence = "HIGH" if quality >= 85.0 and score >= 72.0 else ("MEDIUM" if quality >= 70.0 else "LOW")
    return {
        "upside_probability_pct": round(upside, 1),
        "downside_probability_pct": round(downside, 1),
        "sideways_probability_pct": sideways,
        "probability_confidence": confidence,
        "probability_model": "rule_heuristic_v1",
        "probability_reason": (
            f"score={score:.1f}, rr={risk_reward:.2f}, risk={position_risk}, "
            f"event={event_risk}, quality={quality:.1f}, volatility={volatility:.1f}"
        ),
    }


def build_evidence_chain(row: dict[str, Any], trade_date: str = "") -> list[dict[str, Any]]:
    technical = build_technical_evidence(row)
    probabilities = estimate_trade_probabilities(row)
    margin_status = str(row.get("market_margin_status") or "").upper()
    margin_delta_3d = _safe_number(row.get("margin_balance_delta_3d"), 0.0)
    if margin_delta_3d > 0:
        margin_status = "EXPANDING"
    elif margin_delta_3d < 0:
        margin_status = "CONTRACTING"
    if not margin_status or margin_status == "UNKNOWN":
        margin_delta = _safe_number(row.get("margin_balance_delta"), 0.0)
        margin_status = "EXPANDING" if margin_delta > 0 else ("CONTRACTING" if margin_delta < 0 else "UNKNOWN")
    unlock_pressure = evaluate_unlock_pressure(row)
    chain: list[dict[str, Any]] = [
        {
            "module": "data",
            "status": "PASS" if _safe_number(row.get("data_quality_score"), 0.0) >= 70 else "WATCH",
            "text": f"数据质量{_safe_number(row.get('data_quality_score'), 0.0):.1f}，资金日{row.get('flow_trade_date') or '-'}，热度日{row.get('hot_trade_date') or '-'}",
            "source": "stock_analysis_result",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
        },
        {
            "module": "price_crosscheck",
            "status": str(row.get("price_check_status") or "MISSING_SOURCE"),
            "text": str(row.get("price_check_reason") or "缺少第二行情源，当前仅使用K线价格"),
            "source": str(row.get("price_check_source") or "sm_stock_kline"),
            "date": trade_date or str(row.get("trade_date") or "")[:10],
        },
        {
            "module": "sector",
            "status": str(row.get("sector_gate_status") or "WATCH"),
            "text": str(row.get("sector_gate_reason") or "板块数据不足"),
            "source": "si_industry_sw/sm_stock_kline/sm_stock_capital_flow_daily",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "rotation_score": _safe_number(row.get("sector_rotation_score"), 55.0),
                "width_pct": _safe_number(row.get("sector_width_pct"), 0.0),
                "flow_3d": _safe_number(row.get("sector_flow_3d"), 0.0),
                "theme_continuity_score_10": _safe_number(row.get("theme_continuity_score_10"), 5.5),
                "theme_continuity_level": str(row.get("theme_continuity_level") or "LOW"),
                "theme_continuity_reason": str(row.get("theme_continuity_reason") or ""),
                "leadership_score": _safe_number(row.get("sector_leadership_score"), 50.0),
                "leadership_tier": str(row.get("sector_leadership_tier") or "middle"),
                "amount_rank": _safe_number(row.get("sector_amount_rank"), 0.0),
                "stock_change_3d": _safe_number(row.get("stock_change_3d"), 0.0),
                "stock_main_net_inflow_3d": _safe_number(row.get("stock_main_net_inflow_3d"), 0.0),
            },
        },
        {
            "module": "market_breadth",
            "status": str(row.get("market_extreme_status") or "NEUTRAL"),
            "text": str(row.get("market_breadth_reason") or "市场宽度数据不足"),
            "source": "sm_stock_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "width_ma20_pct": _safe_number(row.get("market_width_ma20_pct"), 50.0),
        },
        {
            "module": "market_style",
            "status": str(row.get("market_regime") or "RANGE"),
            "text": str(row.get("market_style_reason") or "指数风格数据不足，按均衡震荡处理"),
            "source": "sm_index_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "style": str(row.get("market_style") or "range_balanced"),
                "bias": str(row.get("style_bias") or "balanced"),
                "confidence": str(row.get("style_confidence") or "LOW"),
                "hs300_pct_20": _safe_number(row.get("hs300_pct_20"), 0.0),
                "chinext_pct_20": _safe_number(row.get("chinext_pct_20"), 0.0),
                "growth_relative_strength": _safe_number(row.get("growth_relative_strength"), 0.0),
                "adjustment": _safe_number(row.get("market_style_adjustment"), 0.0),
            },
        },
        {
            "module": "macro_policy",
            "status": str(row.get("macro_policy_status") or "UNKNOWN"),
            "text": str(row.get("macro_policy_reason") or "宏观政策新闻数据不足"),
            "source": "st_news_flash",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "score": _safe_number(row.get("macro_policy_score"), 50.0),
                "risk_count": _safe_number(row.get("macro_policy_risk_count"), 0.0),
                "support_count": _safe_number(row.get("macro_policy_support_count"), 0.0),
                "critical_count": _safe_number(row.get("macro_policy_critical_count"), 0.0),
                "latest_title": str(row.get("macro_policy_latest_title") or ""),
            },
        },
        {
            "module": "macro_indicator",
            "status": str(row.get("macro_indicator_status") or "UNKNOWN"),
            "text": str(row.get("macro_indicator_reason") or "structured macro indicators unavailable"),
            "source": "st_macro_indicator/st_macro_economic_data/st_macro_calendar",
            "date": str(row.get("macro_indicator_latest_period") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "score": _safe_number(row.get("macro_indicator_score"), 50.0),
                "risk_count": _safe_number(row.get("macro_indicator_risk_count"), 0.0),
                "support_count": _safe_number(row.get("macro_indicator_support_count"), 0.0),
                "latest_name": str(row.get("macro_indicator_latest_name") or ""),
                "macro_cycle": str(row.get("macro_cycle") or "UNKNOWN"),
                "macro_cycle_reason": str(row.get("macro_cycle_reason") or ""),
            },
        },
        {
            "module": "external_market",
            "status": str(row.get("external_market_status") or "UNKNOWN"),
            "text": str(row.get("external_market_reason") or "外围市场数据未抓取"),
            "source": str(row.get("external_market_source") or "akshare_eastmoney"),
            "date": str(row.get("external_market_captured_at") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "score": _safe_number(row.get("external_market_score"), 50.0),
                "adjustment": _safe_number(row.get("external_market_adjustment"), 0.0),
                "data_quality": str(row.get("external_market_data_quality") or "UNKNOWN"),
                "captured_at": str(row.get("external_market_captured_at") or ""),
                "items": json.loads(row.get("external_market_items_json") or "[]")
                if isinstance(row.get("external_market_items_json"), str)
                else row.get("external_market_items_json") or [],
            },
        },
        {
            "module": "relative_strength",
            "status": "PASS" if _safe_number(row.get("relative_hs300_20"), 0.0) >= 10.0 else (
                "RISK" if _safe_number(row.get("relative_hs300_20"), 0.0) <= -10.0 else "WATCH"
            ),
            "text": (
                f"20日相对沪深300{_safe_number(row.get('relative_hs300_20'), 0.0):.1f}个百分点，"
                f"个股20日{_safe_number(row.get('pct_20'), 0.0):.1f}%"
            ),
            "source": "sm_stock_kline/sm_index_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "relative_hs300_20": _safe_number(row.get("relative_hs300_20"), 0.0),
                "stock_pct_20": _safe_number(row.get("pct_20"), 0.0),
                "hs300_pct_20": _safe_number(row.get("hs300_pct_20"), 0.0),
            },
        },
        {
            "module": "north_flow",
            "status": str(row.get("north_flow_status") or "UNKNOWN"),
            "text": str(row.get("north_flow_reason") or "北向资金数据不足"),
            "source": "st_north_flow_daily",
            "date": str(row.get("north_flow_trade_date") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "net_1d": _safe_number(row.get("north_net_1d"), 0.0),
                "net_3d": _safe_number(row.get("north_net_3d"), 0.0),
                "net_5d": _safe_number(row.get("north_net_5d"), 0.0),
            },
        },
        {
            "module": "etf_flow",
            "status": str(row.get("etf_flow_status") or "UNKNOWN"),
            "text": str(row.get("etf_flow_reason") or "ETF flow data unavailable"),
            "source": "st_etf_flow_daily/st_market_etf_flow",
            "date": str(row.get("etf_flow_trade_date") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "net_1d": _safe_number(row.get("etf_net_1d"), 0.0),
                "net_3d": _safe_number(row.get("etf_net_3d"), 0.0),
                "net_5d": _safe_number(row.get("etf_net_5d"), 0.0),
                "score": _safe_number(row.get("etf_flow_score"), 50.0),
            },
        },
        {
            "module": "retail_sentiment",
            "status": str(row.get("retail_sentiment_status") or "UNKNOWN"),
            "text": str(row.get("retail_sentiment_reason") or "retail bullish/bearish sentiment data unavailable"),
            "source": "st_retail_sentiment/st_market_sentiment_survey",
            "date": str(row.get("retail_sentiment_trade_date") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "bullish_pct": _safe_number(row.get("retail_bullish_pct"), 0.0),
                "bearish_pct": _safe_number(row.get("retail_bearish_pct"), 0.0),
                "sample_size": _safe_number(row.get("retail_sentiment_sample_size"), 0.0),
                "score": _safe_number(row.get("retail_sentiment_score"), 50.0),
            },
        },
        {
            "module": "north_stock",
            "status": str(row.get("north_stock_status") or "UNKNOWN"),
            "text": str(row.get("north_stock_reason") or "stock-level northbound data unavailable"),
            "source": "st_stock_north_holding/st_hsgt_stock_holding",
            "date": str(row.get("north_stock_trade_date") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "holding_ratio": _safe_number(row.get("north_holding_ratio"), 0.0),
                "holding_ratio_delta_3d": _safe_number(row.get("north_holding_ratio_delta_3d"), 0.0),
                "holding_ratio_delta_5d": _safe_number(row.get("north_holding_ratio_delta_5d"), 0.0),
                "holding_market_value": _safe_number(row.get("north_holding_market_value"), 0.0),
                "net_buy_amount_3d": _safe_number(row.get("north_net_buy_amount_3d"), 0.0),
                "net_buy_amount_5d": _safe_number(row.get("north_net_buy_amount_5d"), 0.0),
                "score": _safe_number(row.get("north_stock_score"), 50.0),
            },
        },
        {
            "module": "research_theme",
            "status": "PASS" if _safe_number(row.get("research_theme_score"), 0.0) >= 82 else (
                "WATCH" if _safe_number(row.get("research_theme_score"), 0.0) > 0 else "UNKNOWN"
            ),
            "text": (
                f"{row.get('research_theme_name') or '未命中研报主题'}"
                f" / {row.get('research_theme_tier') or '-'}"
                f" / {row.get('research_theme_role') or '-'}；"
                f"验证: {row.get('research_verification') or '-'}；"
                f"风险: {row.get('research_risk') or '-'}"
            ),
            "source": "research_radar",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "theme_id": str(row.get("research_theme_id") or ""),
                "trend": str(row.get("research_theme_trend") or ""),
                "evidence_level": str(row.get("research_evidence_level") or ""),
                "score": _safe_number(row.get("research_theme_score"), 0.0),
            },
        },
        {
            "module": "institutional_profile",
            "status": str(row.get("institutional_status") or "UNKNOWN"),
            "text": str(row.get("institutional_reason") or "institutional data unavailable"),
            "source": "institution_holding/research_rating/institution_survey",
            "date": str(row.get("rating_date") or row.get("institutional_trade_date") or row.get("latest_survey_date") or trade_date or "")[:10],
            "value": {
                "fund_hold_ratio": _safe_number(row.get("fund_hold_ratio"), 0.0),
                "qfii_hold_ratio": _safe_number(row.get("qfii_hold_ratio"), 0.0),
                "rqfii_hold_ratio": _safe_number(row.get("rqfii_hold_ratio"), 0.0),
                "social_security_hold_ratio": _safe_number(row.get("social_security_hold_ratio"), 0.0),
                "private_fund_hold_ratio": _safe_number(row.get("private_fund_hold_ratio"), 0.0),
                "institution_hold_ratio": _safe_number(row.get("institution_hold_ratio"), 0.0),
                "rating_upgrade_count_90d": _safe_number(row.get("rating_upgrade_count_90d"), 0.0),
                "rating_downgrade_count_90d": _safe_number(row.get("rating_downgrade_count_90d"), 0.0),
                "target_price": _none_if_nan(row.get("target_price")),
                "target_price_upside_pct": _none_if_nan(row.get("target_price_upside_pct")),
                "survey_count_90d": _safe_number(row.get("survey_count_90d"), 0.0),
                "broker_gold_count_90d": _safe_number(row.get("broker_gold_count_90d"), 0.0),
                "score": _safe_number(row.get("institutional_score"), 50.0),
            },
        },
        {
            "module": "investor_interaction",
            "status": str(row.get("investor_interaction_status") or "UNKNOWN"),
            "text": str(row.get("investor_interaction_reason") or "investor interaction data unavailable"),
            "source": "investor_interaction/ir_interaction/cninfo_interaction",
            "date": str(row.get("latest_investor_interaction_date") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "count_180d": _safe_number(row.get("investor_interaction_count_180d"), 0.0),
                "support_count": _safe_number(row.get("investor_interaction_support_count"), 0.0),
                "risk_count": _safe_number(row.get("investor_interaction_risk_count"), 0.0),
                "latest": str(row.get("latest_investor_interaction") or "")[:160],
                "score": _safe_number(row.get("investor_interaction_score"), 50.0),
            },
        },
        {
            "module": "business_purity",
            "status": str(row.get("business_purity_status") or "UNKNOWN"),
            "text": str(row.get("business_purity_reason") or "business description unavailable"),
            "source": str(row.get("business_profile_source") or "stock_business/profile"),
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "match_count": _safe_number(row.get("business_purity_match_count"), 0.0),
                "score": _safe_number(row.get("business_purity_score"), 50.0),
            },
        },
        {
            "module": "industry_prosperity",
            "status": str(row.get("industry_prosperity_status") or "WATCH"),
            "text": str(row.get("industry_prosperity_reason") or "industry prosperity data unavailable"),
            "source": "industry_prosperity/product_price/capacity/order_contract",
            "date": str(row.get("order_contract_latest_date") or trade_date or row.get("trade_date") or "")[:10],
            "value": {
                "price_change_30d": _safe_number(row.get("industry_price_change_30d"), 0.0),
                "capacity_utilization": _ratio_to_pct(row.get("capacity_utilization"), 0.0),
                "order_contract_amount_180d": _safe_number(row.get("order_contract_amount_180d"), 0.0),
                "order_contract_to_revenue_pct": _safe_number(row.get("order_contract_to_revenue_pct"), 0.0),
                "score": _safe_number(row.get("industry_prosperity_score"), 50.0),
            },
        },
        {
            "module": "technical",
            "status": "PASS" if technical.get("direction") == "bullish" else "WATCH",
            "text": technical.get("trend_clock") or "",
            "source": "sm_stock_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
        },
        {
            "module": "chan",
            "status": str(row.get("chan_signal") or "observe"),
            "text": str(row.get("chan_summary") or "缠论结构证据不足"),
            "source": "sm_stock_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
        },
        {
            "module": "capital",
            "status": "PASS" if _safe_number(row.get("capital_score"), 0.0) >= 68 else "WATCH",
            "text": f"资金分{_safe_number(row.get('capital_score'), 0.0):.1f}，当日主力{_safe_number(row.get('main_net_inflow'), 0.0)/1e8:.2f}亿，3日连续流出{_safe_number(row.get('main_outflow_days_3d'), 0.0):.0f}天",
            "source": "sm_stock_capital_flow_daily",
            "date": str(row.get("flow_trade_date") or trade_date or "")[:10],
            "value": {
                "main_net_inflow": _safe_number(row.get("main_net_inflow"), 0.0),
                "main_net_inflow_3d": _safe_number(row.get("main_net_inflow_3d"), 0.0),
                "main_net_inflow_5d": _safe_number(row.get("main_net_inflow_5d"), 0.0),
                "main_net_inflow_10d": _safe_number(row.get("main_net_inflow_10d"), 0.0),
                "main_net_inflow_20d": _safe_number(row.get("main_net_inflow_20d"), 0.0),
                "main_inflow_days_10d": _safe_number(row.get("main_inflow_days_10d"), 0.0),
                "main_outflow_days_10d": _safe_number(row.get("main_outflow_days_10d"), 0.0),
            },
        },
        {
            "module": "volume_temperature",
            "status": str(row.get("volume_temperature_status") or "WATCH"),
            "text": str(row.get("volume_temperature_reason") or "量能温度数据不足"),
            "source": "sm_stock_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "amount_ratio_20": _safe_number(row.get("amount_ratio_20"), 1.0),
                "turnover_ratio": _safe_number(row.get("turnover_ratio"), 0.0),
                "change_pct": _safe_number(row.get("change_pct"), 0.0),
                "score": _safe_number(row.get("volume_temperature_score"), 60.0),
            },
        },
        {
            "module": "liquidity",
            "status": str(row.get("liquidity_status") or "WATCH"),
            "text": str(row.get("liquidity_reason") or "流动性数据不足"),
            "source": "sm_stock_kline/sm_stock_five_level",
            "date": trade_date or str(row.get("trade_date") or row.get("order_book_snapshot_at") or "")[:10],
            "value": {
                "amount": _safe_number(row.get("amount"), 0.0),
                "amount_ma20": _safe_number(row.get("amount_ma20"), 0.0),
                "turnover_ratio": _safe_number(row.get("turnover_ratio"), 0.0),
                "bid5_amount": _safe_number(row.get("bid5_amount"), 0.0),
                "ask5_amount": _safe_number(row.get("ask5_amount"), 0.0),
                "order_book_depth_amount": _safe_number(row.get("order_book_depth_amount"), 0.0),
                "bid_ask_imbalance": _none_if_nan(row.get("bid_ask_imbalance")),
                "order_book_status": str(row.get("order_book_status") or "UNKNOWN"),
                "score": _safe_number(row.get("liquidity_score"), 50.0),
            },
        },
        {
            "module": "size_liquidity",
            "status": str(row.get("size_liquidity_status") or "UNKNOWN"),
            "text": str(row.get("size_liquidity_reason") or "市值/流通股本数据不足"),
            "source": "sm_stock_snapshot/si_stock_shares",
            "date": trade_date or str(row.get("size_trade_date") or row.get("trade_date") or "")[:10],
            "value": {
                "market_cap": _safe_number(row.get("market_cap"), 0.0),
                "float_market_cap": _safe_number(row.get("float_market_cap"), 0.0),
                "effective_market_cap": _safe_number(row.get("effective_market_cap"), 0.0),
                "total_shares": _safe_number(row.get("total_shares"), 0.0),
                "float_shares": _safe_number(row.get("float_shares"), 0.0),
                "score": _safe_number(row.get("size_liquidity_score"), 60.0),
            },
        },
        {
            "module": "chip_capital",
            "status": "BLOCK" if (
                unlock_pressure.get("unlock_status") == "BLOCK" or _safe_number(row.get("mine_clearance_score"), 0.0) >= 70
            ) else (
                "PASS" if _safe_number(row.get("chip_capital_score"), 60.0) >= 68 else "WATCH"
            ),
            "text": (
                f"筹码资金分{_safe_number(row.get('chip_capital_score'), 60.0):.1f}，"
                f"股东变化{_safe_number(row.get('holder_num_ratio'), 0.0):.1f}%，"
                f"龙虎榜20日净买{_safe_number(row.get('lhb_net_amount_20d'), 0.0)/1e8:.2f}亿，"
                f"两融方向{margin_status}，质押{_safe_number(row.get('pledge_ratio'), 0.0):.1f}%"
            ),
            "source": "si_stock_holder/st_a_list_daily/st_a_list_info/st_securities_margin/st_stock_lifting_last_month/st_stock_pledge/st_stock_holder_reduction/st_mine_clearance_tdx",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "lhb_count_20d": _safe_number(row.get("lhb_count_20d"), 0.0),
                "lhb_net_amount_20d": _safe_number(row.get("lhb_net_amount_20d"), 0.0),
                "lhb_inst_count_20d": _safe_number(row.get("lhb_inst_count_20d"), 0.0),
                "lhb_inst_net_amount_20d": _safe_number(row.get("lhb_inst_net_amount_20d"), 0.0),
                "lhb_inst_positive_days_20d": _safe_number(row.get("lhb_inst_positive_days_20d"), 0.0),
                "margin_balance": _safe_number(row.get("margin_balance"), 0.0),
                "margin_balance_delta": _safe_number(row.get("margin_balance_delta"), 0.0),
                "margin_balance_delta_3d": _safe_number(row.get("margin_balance_delta_3d"), 0.0),
                "financing_buy_amount_3d": _safe_number(row.get("financing_buy_amount_3d"), 0.0),
                "margin_expanding_days_3d": _safe_number(row.get("margin_expanding_days_3d"), 0.0),
                "margin_contracting_days_3d": _safe_number(row.get("margin_contracting_days_3d"), 0.0),
                "pledge_ratio": _safe_number(row.get("pledge_ratio"), 0.0),
                "reduction_count_90d": _safe_number(row.get("reduction_count_90d"), 0.0),
                "reduction_max_ratio_90d": _safe_number(row.get("reduction_max_ratio_90d"), 0.0),
                "reduction_amount_90d": _safe_number(row.get("reduction_amount_90d"), 0.0),
                "unlock_status": str(unlock_pressure.get("unlock_status") or "PASS"),
                "unlock_amount_ratio_pct": _safe_number(unlock_pressure.get("unlock_amount_ratio_pct"), 0.0),
                "lifting_count_30d": _safe_number(row.get("lifting_count_30d"), 0.0),
                "lifting_max_ratio_30d": _safe_number(row.get("lifting_max_ratio_30d"), 0.0),
            },
        },
        {
            "module": "valuation",
            "status": str(row.get("valuation_status") or (
                "PASS" if _safe_number(row.get("valuation_score"), 55.0) >= 70
                else ("RISK" if _safe_number(row.get("valuation_score"), 55.0) <= 40 else "WATCH")
            )),
            "text": str(row.get("valuation_reason") or "估值数据不足，回退综合评分"),
            "source": "si_stock_finance/sm_stock_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "style": str(row.get("valuation_style") or "general"),
                "pe_ttm": _none_if_nan(row.get("pe_ttm")),
                "pb_ratio": _none_if_nan(row.get("pb_ratio")),
                "ps_ratio": _none_if_nan(row.get("ps_ratio")),
                "peg_ratio": _none_if_nan(row.get("peg_ratio")),
                "industry_pe_median": _none_if_nan(row.get("industry_pe_median")),
                "pe_industry_multiple": _none_if_nan(row.get("pe_industry_multiple")),
                "industry_ps_median": _none_if_nan(row.get("industry_ps_median")),
                "ps_industry_multiple": _none_if_nan(row.get("ps_industry_multiple")),
                "valuation_history_percentile_250d": _none_if_nan(row.get("valuation_history_percentile_250d")),
                "pe_percentile_250d": _none_if_nan(row.get("pe_percentile_250d")),
                "pb_percentile_250d": _none_if_nan(row.get("pb_percentile_250d")),
                "close_history_count": _none_if_nan(row.get("close_history_count")),
                "score": _safe_number(row.get("valuation_score"), 55.0),
            },
        },
        {
            "module": "fundamental_quality",
            "status": str(row.get("fundamental_quality_status") or "WATCH"),
            "text": str(row.get("fundamental_quality_reason") or "基本面阈值数据不足"),
            "source": "si_stock_finance",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "roe_wtd": _none_if_nan(row.get("roe_wtd")),
                "roe_non_gaap_wtd": _none_if_nan(row.get("roe_non_gaap_wtd")),
                "roa_wtd": _none_if_nan(row.get("roa_wtd")),
                "roic": _none_if_nan(row.get("roic")),
                "gross_margin": _none_if_nan(row.get("gross_margin")),
                "net_profit_yoy_gr": _none_if_nan(row.get("net_profit_yoy_gr")),
                "total_rev_yoy_gr": _none_if_nan(row.get("total_rev_yoy_gr")),
                "net_profit_qoq_gr": _none_if_nan(row.get("net_profit_qoq_gr")),
                "total_rev_qoq_gr": _none_if_nan(row.get("total_rev_qoq_gr")),
                "quick_ratio": _none_if_nan(row.get("quick_ratio")),
                "asset_liab_ratio": _none_if_nan(row.get("asset_liab_ratio")),
                "acct_recv_to_rev": _none_if_nan(row.get("acct_recv_to_rev")),
                "prepayment_yoy_gr": _none_if_nan(row.get("prepayment_yoy_gr")),
                "related_transaction_to_rev": _none_if_nan(row.get("related_transaction_to_rev")),
                "goodwill": _none_if_nan(row.get("goodwill")),
                "net_assets": _none_if_nan(row.get("net_assets")),
                "goodwill_to_net_asset_pct": _none_if_nan(row.get("goodwill_to_net_asset_pct")),
                "score": _safe_number(row.get("fundamental_quality_score"), 50.0),
            },
        },
        {
            "module": "dividend",
            "status": "PASS" if _safe_number(row.get("dividend_count_3y"), 0.0) >= 3 else (
                "WATCH" if _safe_number(row.get("latest_dividend_cash_per_share"), 0.0) > 0 else "UNKNOWN"
            ),
            "text": (
                f"近3年现金分红{_safe_number(row.get('dividend_count_3y'), 0.0):.0f}次，"
                f"最新方案{row.get('latest_dividend_plan') or '-'}，"
                f"近3年平均股息率估算{_safe_number(row.get('avg_dividend_yield_pct_3y'), 0.0):.2f}%"
            ),
            "source": "sm_dividend/sm_stock_kline",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": {
                "dividend_count_3y": _safe_number(row.get("dividend_count_3y"), 0.0),
                "latest_dividend_yield_pct": _safe_number(row.get("latest_dividend_yield_pct"), 0.0),
                "avg_dividend_yield_pct_3y": _safe_number(row.get("avg_dividend_yield_pct_3y"), 0.0),
                "score": _safe_number(row.get("dividend_score"), 50.0),
            },
        },
        {
            "module": "event",
            "status": "BLOCK" if str(row.get("event_risk_level") or "").upper() == "CRITICAL" else str(row.get("event_risk_level") or "LOW"),
            "text": (
                f"事件风险{row.get('event_risk_level') or 'LOW'}，"
                f"公告/新闻风险数{int(_safe_number(row.get('notice_negative'), 0.0) + _safe_number(row.get('notice_critical'), 0.0))}；"
                f"{classify_event_fulfillment(row).get('event_fulfillment_reason')}"
            ),
            "source": "si_notice_eastmoney/st_news_flash",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": classify_event_fulfillment(row),
        },
        {
            "module": "probability",
            "status": probabilities.get("probability_confidence", "LOW"),
            "text": (
                f"上涨概率{probabilities['upside_probability_pct']:.1f}%，"
                f"下跌概率{probabilities['downside_probability_pct']:.1f}%"
            ),
            "source": "rule_heuristic_v1",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "value": probabilities,
        },
        {
            "module": "trade_plan",
            "status": str(row.get("signal_status") or "WATCH"),
            "text": f"入场{_round_price(row.get('entry_price_low')) or '-'}-{_round_price(row.get('entry_price_high')) or '-'}，止损{_round_price(row.get('stop_loss_price')) or '-'}，盈亏比{_safe_number(row.get('risk_reward_ratio'), 0.0):.2f}:1",
            "source": "strategy_trade_plan",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
        },
    ]
    disabled_raw = row.get("disabled_factor_inventory")
    if isinstance(disabled_raw, str):
        disabled = _parse_json_field(disabled_raw, [])
    elif isinstance(disabled_raw, (list, tuple, set, frozenset)):
        disabled = [str(item) for item in disabled_raw]
    else:
        disabled = []
    if disabled:
        chain.append({
            "module": "disabled_factor_inventory",
            "status": "NEUTRALIZED",
            "text": str(
                row.get("disabled_factor_reason")
                or LEGACY_PIT_DISABLED_FACTOR_REASON
            ),
            "reason": str(
                row.get("disabled_factor_reason")
                or LEGACY_PIT_DISABLED_FACTOR_REASON
            ),
            "source": "legacy_v3_cutoff_guard",
            "date": trade_date or str(row.get("trade_date") or "")[:10],
            "disabled_factor_inventory": sorted({str(item) for item in disabled}),
        })
    return chain


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
    sector_gate_status = str(row.get("sector_gate_status") or "WATCH").upper()
    chase_gate_supplied = (
        "chase_risk_status" in row or "ordinary_buy_eligible" in row
    )
    chase_risk_status = _normalized_chase_gate_status(
        row.get("chase_risk_status")
    )
    ordinary_buy_eligible = _is_explicit_true(
        row.get("ordinary_buy_eligible")
    )
    chase_risk_reason = _safe_text_value(row.get("chase_risk_reason"))
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
            "risk_reward_ratio": None,
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
    # A wide moving-average entry band can otherwise place the fixed-percent
    # stop inside the band or the first target below its upper edge.  Enforce
    # executable boundaries before computing risk/reward.
    stop_loss = min(stop_loss, entry_low * 0.98)
    take_profit_1 = max(take_profit_1, entry_high * 1.02)
    take_profit_2 = max(take_profit_2, take_profit_1 * 1.03)
    downside_pct = max(0.1, (ref_price - stop_loss) / ref_price * 100.0)
    target_upside_pct = (
        expected_return_pct
        if expected_return_pct > 0
        else (take_profit_2 / ref_price - 1.0) * 100.0
    )
    risk_reward_ratio = round(max(0.0, target_upside_pct) / downside_pct, 2)
    min_rr = runtime_threshold("min_risk_reward", MIN_EXECUTABLE_RISK_REWARD)

    blockers: list[str] = []
    if strategy == "main_wave" and main_wave_signal in {"SELL_ALERT", "REDUCE"}:
        status = "SELL_ALERT"
        blockers.append(main_wave_reason or "main-wave risk signal triggered")
    elif chase_gate_supplied and chase_risk_status in {"WATCH", "CONDITIONAL"}:
        status = "WATCH"
        blockers.append(chase_risk_reason or f"chase-risk gate is {chase_risk_status}")
    elif chase_gate_supplied and (
        chase_risk_status != "ALLOW" or not ordinary_buy_eligible
    ):
        status = "BLOCK"
        blockers.append(
            chase_risk_reason
            or f"chase/tradability gate is {chase_risk_status or 'MISSING'}"
        )
    elif base_status == "BLOCK" or event_risk == "CRITICAL":
        status = "BLOCK"
        blockers.append("blocked by base recommendation gate")
    elif sector_gate_status == "BLOCK":
        status = "BLOCK"
        blockers.append("sector gate is BLOCK; board-first rule failed")
    elif strategy != "main_wave" and expected_return_pct < 5.0:
        status = "BLOCK"
        blockers.append(f"expected upside {expected_return_pct:.1f}% is below 5% threshold")
    elif strategy != "main_wave" and risk_reward_ratio < min_rr:
        status = "BLOCK"
        blockers.append(
            f"risk/reward {risk_reward_ratio:.2f}:1 is below {min_rr:.2f}:1 threshold"
        )
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

    position_risk_level = derive_position_risk_level(row, status)
    position_cap_pct = _position_cap_pct(row, status)
    sell_rules = [
        f"stop loss below {abs(float(profile['stop_loss_pct'])):.1f}%",
        f"take profit levels {float(profile['take_profit_1_pct']):.1f}%/{float(profile['take_profit_2_pct']):.1f}%",
        f"max holding {int(profile['max_holding_days'])} trading days",
        f"position risk {position_risk_level}, single-stock cap <= {position_cap_pct:.1f}%",
        invalidation,
    ]
    if strategy == "main_wave":
        sell_rules = [
            "do not exit only because fixed profit target is reached",
            "reduce when distance from MA20 is excessive and cumulative wave gain is high",
            "sell alert when price closes below MA20 after a main-wave advance",
            f"position risk {position_risk_level}, single-stock cap <= {position_cap_pct:.1f}%",
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
        "risk_reward_ratio": risk_reward_ratio,
        "position_weight": _position_weight(row, strategy, status),
        "position_risk_level": position_risk_level,
        "position_cap_pct": position_cap_pct,
        "max_holding_days": int(profile["max_holding_days"]),
        "entry_conditions_json": json.dumps(entry_conditions, ensure_ascii=False),
        "sell_rules_json": json.dumps(sell_rules, ensure_ascii=False),
        "invalidation_reason": invalidation[:500],
        "cooldown_days_left": cooldown_days_left if not cooldown_bypassed else 0,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until and not cooldown_bypassed else None,
    }


def derive_investment_rating(row: dict[str, Any]) -> tuple[str, str]:
    """Map strategy status to the stock.txt five-level rating vocabulary."""
    signal = str(row.get("signal_status") or row.get("recommend_status") or "WATCH").upper()
    recommend_status = str(row.get("recommend_status") or "SUSPENDED").upper()
    event_risk = str(row.get("event_risk_level") or "LOW").upper()
    final_score = _safe_number(row.get("final_trade_score"), _safe_number(row.get("ai_score"), 0.0))
    entry_score = _safe_number(row.get("entry_score"), 0.0)
    expected_return = _safe_number(row.get("expected_return_pct"), 0.0)
    risk_reward = _safe_number(row.get("risk_reward_ratio"), 0.0)
    main_wave_signal = str(row.get("main_wave_signal") or "").upper()

    if signal == "SELL_ALERT" or main_wave_signal == "SELL_ALERT" or event_risk == "CRITICAL" or recommend_status == "BLOCK":
        return "卖出", "强风险或硬闸门触发，预期明显跑输，建议回避"
    if main_wave_signal == "REDUCE":
        return "减持", "主升浪高位或趋势转弱，预期跑输，建议降低仓位"
    if signal in {"BUY_READY", "CONFIRM"} and final_score >= 78 and expected_return >= 10 and risk_reward >= runtime_threshold("min_risk_reward", MIN_EXECUTABLE_RISK_REWARD):
        return "买入", "买点、交易分、预期空间和盈亏比同时达标，预期显著跑赢"
    if signal in {"BUY_READY", "CONFIRM"} or (recommend_status == "ALLOW" and final_score >= 68 and entry_score >= 55):
        return "增持", "整体仍偏多，但强度或买点质量弱于买入评级"
    if signal == "WATCH" or recommend_status == "SUSPENDED":
        return "中性", "等待回调、数据修复或风险项消化，暂不做方向强化"
    return "中性", "没有形成明确跑赢或跑输证据"


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
        _numeric_col(out, "chan_resistance_price"),
        _numeric_col(out, "volume_profile_resistance_price"),
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
    out["sector_leadership_score"] = _numeric_col(out, "sector_leadership_score", 50.0).fillna(50.0).clip(0, 100).round(1)
    leadership_tier = out.get("sector_leadership_tier", pd.Series("middle", index=out.index)).fillna("middle").astype(str).str.lower()
    leadership_adjust = pd.Series(
        np.select(
            [
                leadership_tier.eq("leader"),
                leadership_tier.eq("front"),
                leadership_tier.eq("follower"),
            ],
            [3.0, 1.5, -2.0],
            default=0.0,
        ),
        index=out.index,
    )
    out["chip_capital_score"] = _numeric_col(out, "chip_capital_score", 60.0).fillna(60.0).clip(20, 100).round(1)

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
        + out["chip_capital_score"] * 0.08
        + out["confidence_score"] * 0.04
        + leadership_adjust
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
        + out["sector_leadership_score"] * 0.04
        + strength_score * 0.08
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
        "take_profit_1", "take_profit_2", "risk_reward_ratio", "position_weight",
        "position_risk_level", "position_cap_pct", "max_holding_days", "entry_conditions_json", "sell_rules_json",
        "invalidation_reason", "cooldown_days_left", "cooldown_until",
    ]:
        out[key] = [plan.get(key) for plan in plans]
    out["suitable_strategies"] = suitable_col
    ratings = [derive_investment_rating(row) for row in out.to_dict(orient="records")]
    out["investment_rating"] = [item[0] for item in ratings]
    out["rating_reason"] = [item[1] for item in ratings]
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
    price_validation: pd.DataFrame | None = None,
    confidence: pd.DataFrame | None = None,
    rec_history: pd.DataFrame | None = None,
    failures: pd.DataFrame | None = None,
    chip_context: pd.DataFrame | None = None,
    size_context: pd.DataFrame | None = None,
    order_book_context: pd.DataFrame | None = None,
    dividend_context: pd.DataFrame | None = None,
    research_context: pd.DataFrame | None = None,
    north_stock_context: pd.DataFrame | None = None,
    institutional_context: pd.DataFrame | None = None,
    prosperity_context: pd.DataFrame | None = None,
    business_context: pd.DataFrame | None = None,
    interaction_context: pd.DataFrame | None = None,
    market_breadth: dict[str, Any] | None = None,
    market_context: dict[str, Any] | None = None,
    event_relation_rules: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    flow = _ensure_columns(flow, {
        "main_net_inflow": np.nan,
        "main_net_inflow_3d": np.nan,
        "main_net_inflow_5d": np.nan,
        "main_net_inflow_10d": np.nan,
        "main_net_inflow_20d": np.nan,
        "main_outflow_days_3d": 0.0,
        "main_outflow_days_5d": 0.0,
        "main_outflow_days_10d": 0.0,
        "main_inflow_days_3d": 0.0,
        "main_inflow_days_5d": 0.0,
        "main_inflow_days_10d": 0.0,
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
        "sector_gate_status": "WATCH",
        "sector_gate_reason": "板块数据不足，先按观察处理",
        "sector_flow_3d": 0.0,
        "sector_width_pct": 0.0,
        "sector_avg_change_3d": 0.0,
        "theme_continuity_score_10": 5.5,
        "theme_continuity_level": "LOW",
        "theme_continuity_reason": "题材延续性数据不足，按低延续观察",
        "sector_leadership_score": 50.0,
        "sector_leadership_tier": "middle",
        "sector_amount_rank": 0.0,
        "stock_change_3d": 0.0,
        "stock_amount_3d": 0.0,
        "stock_main_net_inflow_3d": 0.0,
    })
    price_validation = _ensure_columns(price_validation if price_validation is not None else pd.DataFrame({"stock_code": []}), {
        "snapshot_trade_date": "",
        "snapshot_price": np.nan,
        "snapshot_close": np.nan,
        "current_price": np.nan,
        "current_snapshot_at": "",
    })
    chip_context = _ensure_columns(chip_context if chip_context is not None else pd.DataFrame({"stock_code": []}), {
        "holder_report_date": None,
        "holder_num": np.nan,
        "holder_num_change": np.nan,
        "pre_holder_num": np.nan,
        "holder_num_ratio": np.nan,
        "avg_free_shares": np.nan,
        "lhb_count_20d": 0.0,
        "lhb_net_amount_20d": 0.0,
        "lhb_latest_date": None,
        "lhb_inst_count_20d": 0.0,
        "lhb_inst_positive_days_20d": 0.0,
        "lhb_inst_net_amount_20d": 0.0,
        "lhb_inst_buy_amount_20d": 0.0,
        "lhb_inst_sell_amount_20d": 0.0,
        "lhb_inst_latest_date": None,
        "lifting_count_30d": 0.0,
        "lifting_next_date": None,
        "lifting_amount_30d": 0.0,
        "lifting_max_ratio_30d": 0.0,
        "pledge_report_date": None,
        "pledge_ratio": 0.0,
        "reduction_count_90d": 0.0,
        "reduction_max_ratio_90d": 0.0,
        "reduction_amount_90d": 0.0,
        "reduction_latest_date": None,
        "mine_clearance_score": 0.0,
        "mine_clearance_reason": "",
        "margin_trade_date": None,
        "margin_balance": 0.0,
        "financing_balance": 0.0,
        "margin_balance_delta": 0.0,
        "financing_buy_amount": 0.0,
        "margin_balance_delta_3d": 0.0,
        "financing_buy_amount_3d": 0.0,
        "margin_expanding_days_3d": 0.0,
        "margin_contracting_days_3d": 0.0,
    })
    size_context = _ensure_columns(size_context if size_context is not None else pd.DataFrame({"stock_code": []}), {
        "size_trade_date": None,
        "share_report_date": None,
        "size_price": 0.0,
        "market_cap": 0.0,
        "float_market_cap": 0.0,
        "total_shares": 0.0,
        "float_shares": 0.0,
    })
    order_book_context = _ensure_columns(order_book_context if order_book_context is not None else pd.DataFrame({"stock_code": []}), {
        "order_book_snapshot_at": None,
        "bid5_amount": 0.0,
        "ask5_amount": 0.0,
        "order_book_depth_amount": 0.0,
        "bid_ask_imbalance": np.nan,
    })
    dividend_context = _ensure_columns(dividend_context if dividend_context is not None else pd.DataFrame({"stock_code": []}), {
        "dividend_count_3y": 0.0,
        "dividend_cash_per_share_3y": 0.0,
        "latest_dividend_report_date": None,
        "latest_dividend_plan": "",
        "latest_dividend_cash_per_share": 0.0,
        "ex_dividend_date": None,
    })
    research_context = _ensure_columns(research_context if research_context is not None else pd.DataFrame({"stock_code": []}), {
        "research_theme_score": 0.0,
        "research_theme_name": "",
        "research_theme_id": "",
        "research_theme_trend": "",
        "research_evidence_level": "",
        "research_theme_role": "",
        "research_theme_tier": "",
        "research_verification": "",
        "research_risk": "",
    })
    north_stock_context = _ensure_columns(north_stock_context if north_stock_context is not None else pd.DataFrame({"stock_code": []}), {
        "north_stock_trade_date": "",
        "north_holding_ratio": 0.0,
        "north_holding_ratio_delta_3d": 0.0,
        "north_holding_ratio_delta_5d": 0.0,
        "north_holding_market_value": 0.0,
        "north_holding_shares": 0.0,
        "north_net_buy_amount_3d": 0.0,
        "north_net_buy_amount_5d": 0.0,
        "north_stock_status": "UNKNOWN",
        "north_stock_score": 50.0,
        "north_stock_reason": "stock-level northbound data unavailable",
    })
    institutional_context = _ensure_columns(institutional_context if institutional_context is not None else pd.DataFrame({"stock_code": []}), {
        "institutional_trade_date": "",
        "fund_hold_ratio": 0.0,
        "qfii_hold_ratio": 0.0,
        "rqfii_hold_ratio": 0.0,
        "social_security_hold_ratio": 0.0,
        "private_fund_hold_ratio": 0.0,
        "institution_hold_ratio": 0.0,
        "rating_upgrade_count_90d": 0.0,
        "rating_downgrade_count_90d": 0.0,
        "rating_date": "",
        "target_price": np.nan,
        "survey_count_90d": 0.0,
        "latest_survey_date": "",
        "broker_gold_count_90d": 0.0,
        "broker_gold_latest_date": "",
    })
    prosperity_context = _ensure_columns(prosperity_context if prosperity_context is not None else pd.DataFrame({"stock_code": []}), {
        "industry_price_change_30d": 0.0,
        "capacity_utilization": 0.0,
        "external_prosperity_score": np.nan,
        "order_contract_amount_180d": 0.0,
        "order_contract_count_180d": 0.0,
        "order_contract_latest_date": "",
    })
    business_context = _ensure_columns(business_context if business_context is not None else pd.DataFrame({"stock_code": []}), {
        "business_scope": "",
        "business_profile_source": "",
    })
    interaction_context = _ensure_columns(interaction_context if interaction_context is not None else pd.DataFrame({"stock_code": []}), {
        "investor_interaction_count_180d": 0.0,
        "investor_interaction_support_count": 0.0,
        "investor_interaction_risk_count": 0.0,
        "latest_investor_interaction_date": "",
        "latest_investor_interaction": "",
        "investor_interaction_status": "UNKNOWN",
        "investor_interaction_score": 50.0,
        "investor_interaction_reason": "investor interaction data unavailable",
    })
    if not sector.empty and "industry_name" in sector.columns:
        sector = sector.rename(columns={"industry_name": "sector_industry_name"})
    df = kline.merge(finance, on="stock_code", how="left")
    df = df.merge(flow, on="stock_code", how="left", suffixes=("", "_flow"))
    df = df.merge(hot[["stock_code", "fused_rank", "hot_total_score", "source_flag", "industry_name"]], on="stock_code", how="left")
    if not price_validation.empty:
        df = df.merge(
            price_validation[[
                "stock_code", "snapshot_trade_date", "snapshot_price", "snapshot_close",
                "current_price", "current_snapshot_at",
            ]],
            on="stock_code",
            how="left",
        )
    df = attach_price_crosscheck(df, trade_date=trade_date)
    if not chip_context.empty:
        df = df.merge(
            chip_context[[
                "stock_code", "holder_report_date", "holder_num", "holder_num_change",
                "pre_holder_num", "holder_num_ratio", "avg_free_shares",
                "lhb_count_20d", "lhb_net_amount_20d", "lhb_latest_date",
                "lhb_inst_count_20d", "lhb_inst_positive_days_20d",
                "lhb_inst_net_amount_20d", "lhb_inst_buy_amount_20d",
                "lhb_inst_sell_amount_20d", "lhb_inst_latest_date",
                "lifting_count_30d", "lifting_next_date", "lifting_amount_30d",
                "lifting_max_ratio_30d", "pledge_report_date", "pledge_ratio",
                "reduction_count_90d", "reduction_max_ratio_90d", "reduction_amount_90d",
                "reduction_latest_date",
                "mine_clearance_score", "mine_clearance_reason",
                "margin_trade_date", "margin_balance", "financing_balance",
                "margin_balance_delta", "financing_buy_amount", "margin_balance_delta_3d",
                "financing_buy_amount_3d", "margin_expanding_days_3d", "margin_contracting_days_3d",
            ]],
            on="stock_code",
            how="left",
        )
    if not size_context.empty:
        df = df.merge(
            size_context[[
                "stock_code", "size_trade_date", "share_report_date", "size_price",
                "market_cap", "float_market_cap", "total_shares", "float_shares",
            ]],
            on="stock_code",
            how="left",
        )
    if not order_book_context.empty:
        df = df.merge(
            order_book_context[[
                "stock_code", "order_book_snapshot_at", "bid5_amount", "ask5_amount",
                "order_book_depth_amount", "bid_ask_imbalance",
            ]],
            on="stock_code",
            how="left",
        )
    if not dividend_context.empty:
        df = df.merge(
            dividend_context[[
                "stock_code", "dividend_count_3y", "dividend_cash_per_share_3y",
                "latest_dividend_report_date", "latest_dividend_plan",
                "latest_dividend_cash_per_share", "ex_dividend_date",
            ]],
            on="stock_code",
            how="left",
        )
    if not research_context.empty:
        df = df.merge(
            research_context[[
                "stock_code", "research_theme_score", "research_theme_name", "research_theme_id",
                "research_theme_trend", "research_evidence_level", "research_theme_role",
                "research_theme_tier", "research_verification", "research_risk",
            ]],
            on="stock_code",
            how="left",
        )
    if not north_stock_context.empty:
        df = df.merge(
            north_stock_context[[
                "stock_code", "north_stock_trade_date", "north_holding_ratio",
                "north_holding_ratio_delta_3d", "north_holding_ratio_delta_5d",
                "north_holding_market_value", "north_holding_shares",
                "north_net_buy_amount_3d", "north_net_buy_amount_5d",
                "north_stock_status", "north_stock_score", "north_stock_reason",
            ]],
            on="stock_code",
            how="left",
        )
    if not institutional_context.empty:
        df = df.merge(
            institutional_context[[
                "stock_code", "institutional_trade_date", "fund_hold_ratio", "qfii_hold_ratio",
                "rqfii_hold_ratio", "social_security_hold_ratio", "private_fund_hold_ratio",
                "institution_hold_ratio", "rating_upgrade_count_90d", "rating_downgrade_count_90d",
                "rating_date", "target_price", "survey_count_90d", "latest_survey_date",
                "broker_gold_count_90d", "broker_gold_latest_date",
            ]],
            on="stock_code",
            how="left",
        )
    if not prosperity_context.empty:
        df = df.merge(
            prosperity_context[[
                "stock_code", "industry_price_change_30d", "capacity_utilization",
                "external_prosperity_score", "order_contract_amount_180d",
                "order_contract_count_180d", "order_contract_latest_date",
            ]],
            on="stock_code",
            how="left",
        )
    if not business_context.empty:
        df = df.merge(
            business_context[["stock_code", "business_scope", "business_profile_source"]],
            on="stock_code",
            how="left",
        )
    if not interaction_context.empty:
        df = df.merge(
            interaction_context[[
                "stock_code", "investor_interaction_count_180d", "investor_interaction_support_count",
                "investor_interaction_risk_count", "latest_investor_interaction_date",
                "latest_investor_interaction", "investor_interaction_status",
                "investor_interaction_score", "investor_interaction_reason",
            ]],
            on="stock_code",
            how="left",
        )
    if not sector.empty:
        df = df.merge(
            sector[[
                "stock_code", "sector_industry_name", "sector_rotation_score", "sector_gate_status",
                "sector_gate_reason", "sector_flow_3d", "sector_width_pct", "sector_avg_change_3d",
                "theme_continuity_score_10", "theme_continuity_level", "theme_continuity_reason",
                "sector_leadership_score", "sector_leadership_tier", "sector_amount_rank",
                "stock_change_3d", "stock_amount_3d", "stock_main_net_inflow_3d",
            ]],
            on="stock_code",
            how="left",
        )
        df["industry_name"] = df["industry_name"].fillna("").astype(str)
        df["sector_industry_name"] = df["sector_industry_name"].fillna("").astype(str)
        df["industry_name"] = df["industry_name"].where(df["industry_name"] != "", df["sector_industry_name"])
    if "sector_gate_status" not in df.columns:
        df["sector_gate_status"] = "WATCH"
    if "sector_gate_reason" not in df.columns:
        df["sector_gate_reason"] = "板块数据不足，先按观察处理"
    df["sector_gate_status"] = df["sector_gate_status"].fillna("WATCH").astype(str)
    df["sector_gate_reason"] = df["sector_gate_reason"].fillna("板块数据不足，先按观察处理").astype(str)
    for col, default in (
        ("sector_flow_3d", 0.0),
        ("sector_width_pct", 0.0),
        ("sector_avg_change_3d", 0.0),
        ("sector_rotation_score", 55.0),
        ("theme_continuity_score_10", 5.5),
        ("sector_leadership_score", 50.0),
        ("sector_amount_rank", 0.0),
        ("stock_change_3d", 0.0),
        ("stock_amount_3d", 0.0),
        ("stock_main_net_inflow_3d", 0.0),
    ):
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
    if "sector_leadership_tier" not in df.columns:
        df["sector_leadership_tier"] = "middle"
    df["sector_leadership_tier"] = df["sector_leadership_tier"].fillna("middle").astype(str)
    if "theme_continuity_level" not in df.columns:
        df["theme_continuity_level"] = "LOW"
    if "theme_continuity_reason" not in df.columns:
        df["theme_continuity_reason"] = "题材延续性数据不足，按低延续观察"
    df["theme_continuity_level"] = df["theme_continuity_level"].fillna("LOW").astype(str)
    df["theme_continuity_reason"] = df["theme_continuity_reason"].fillna("题材延续性数据不足，按低延续观察").astype(str)
    df = df.merge(notices, on="stock_code", how="left")

    market_breadth = market_breadth or {}
    market_context = market_context or {}
    for key, default in {
        "market_width_ma20_pct": 50.0,
        "market_advance_pct": 50.0,
        "market_limit_up_pct": 0.0,
        "market_limit_down_pct": 0.0,
    }.items():
        df[key] = _safe_number(market_breadth.get(key), default)
    df["market_extreme_status"] = str(market_breadth.get("market_extreme_status") or "NEUTRAL")
    df["market_breadth_reason"] = str(market_breadth.get("market_breadth_reason") or "市场宽度数据不足")
    for key, default in {
        "market_margin_balance": 0.0,
        "market_financing_balance": 0.0,
        "market_margin_balance_delta": 0.0,
        "market_financing_balance_delta": 0.0,
    }.items():
        df[key] = _safe_number(market_context.get(key), default)
    df["market_margin_trade_date"] = str(market_context.get("market_margin_trade_date") or "")
    df["market_margin_status"] = str(market_context.get("market_margin_status") or "UNKNOWN")
    df["market_regime"] = str(market_context.get("market_regime") or "RANGE")
    df["market_style"] = str(market_context.get("market_style") or "range_balanced")
    df["style_bias"] = str(market_context.get("style_bias") or "balanced")
    df["style_confidence"] = str(market_context.get("style_confidence") or "LOW")
    df["style_growth_allowed"] = bool(market_context.get("style_growth_allowed", True))
    df["market_style_reason"] = str(market_context.get("market_style_reason") or "指数风格数据不足，按均衡震荡处理")
    for key, default in {
        "hs300_pct_20": 0.0,
        "chinext_pct_20": 0.0,
        "growth_relative_strength": 0.0,
    }.items():
        df[key] = _safe_number(market_context.get(key), default)
    for key, default in {
        "north_net_1d": 0.0,
        "north_net_3d": 0.0,
        "north_net_5d": 0.0,
    }.items():
        df[key] = _safe_number(market_context.get(key), default)
    df["north_flow_status"] = str(market_context.get("north_flow_status") or "UNKNOWN")
    df["north_flow_trade_date"] = str(market_context.get("north_flow_trade_date") or "")
    df["north_flow_reason"] = str(market_context.get("north_flow_reason") or "北向资金数据不足")
    for key, default in {
        "etf_net_1d": 0.0,
        "etf_net_3d": 0.0,
        "etf_net_5d": 0.0,
        "etf_flow_score": 50.0,
    }.items():
        df[key] = _safe_number(market_context.get(key), default)
    df["etf_flow_status"] = str(market_context.get("etf_flow_status") or "UNKNOWN")
    df["etf_flow_trade_date"] = str(market_context.get("etf_flow_trade_date") or "")
    df["etf_flow_reason"] = str(market_context.get("etf_flow_reason") or "ETF flow data unavailable")
    for key, default in {
        "retail_bullish_pct": 0.0,
        "retail_bearish_pct": 0.0,
        "retail_sentiment_score": 50.0,
        "retail_sentiment_sample_size": 0.0,
    }.items():
        df[key] = _safe_number(market_context.get(key), default)
    df["retail_sentiment_status"] = str(market_context.get("retail_sentiment_status") or "UNKNOWN")
    df["retail_sentiment_trade_date"] = str(market_context.get("retail_sentiment_trade_date") or "")
    df["retail_sentiment_reason"] = str(
        market_context.get("retail_sentiment_reason") or "retail bullish/bearish sentiment data unavailable"
    )
    for key, default in {
        "macro_policy_score": 50.0,
        "macro_policy_risk_count": 0.0,
        "macro_policy_support_count": 0.0,
        "macro_policy_critical_count": 0.0,
    }.items():
        df[key] = _safe_number(market_context.get(key), default)
    df["macro_policy_status"] = str(market_context.get("macro_policy_status") or "UNKNOWN")
    df["macro_policy_reason"] = str(market_context.get("macro_policy_reason") or "宏观政策新闻数据不足")
    df["macro_policy_latest_title"] = str(market_context.get("macro_policy_latest_title") or "")
    for key, default in {
        "macro_indicator_score": 50.0,
        "macro_indicator_risk_count": 0.0,
        "macro_indicator_support_count": 0.0,
    }.items():
        df[key] = _safe_number(market_context.get(key), default)
    df["macro_indicator_status"] = str(market_context.get("macro_indicator_status") or "UNKNOWN")
    df["macro_indicator_reason"] = str(market_context.get("macro_indicator_reason") or "structured macro indicators unavailable")
    df["macro_indicator_latest_name"] = str(market_context.get("macro_indicator_latest_name") or "")
    df["macro_indicator_latest_period"] = str(market_context.get("macro_indicator_latest_period") or "")
    df["macro_cycle"] = str(market_context.get("macro_cycle") or "UNKNOWN")
    df["macro_cycle_reason"] = str(market_context.get("macro_cycle_reason") or "structured macro indicators unavailable")
    df["external_market_score"] = _safe_number(market_context.get("external_market_score"), 50.0)
    df["external_market_status"] = str(market_context.get("external_market_status") or "UNKNOWN")
    df["external_market_reason"] = str(
        market_context.get("external_market_reason") or "外围市场数据未抓取"
    )
    df["external_market_data_quality"] = str(market_context.get("external_market_data_quality") or "UNKNOWN")
    df["external_market_captured_at"] = str(market_context.get("external_market_captured_at") or "")
    df["external_market_source"] = str(market_context.get("external_market_source") or "")
    df["external_market_items_json"] = str(market_context.get("external_market_items_json") or "[]")
    liquidity_profiles = [evaluate_liquidity_profile(row) for row in df.to_dict(orient="records")]
    liquidity_df = pd.DataFrame(liquidity_profiles, index=df.index)
    df["liquidity_status"] = liquidity_df["liquidity_status"]
    df["liquidity_score"] = _round_score(liquidity_df["liquidity_score"])
    df["liquidity_reason"] = liquidity_df["liquidity_reason"]
    df["liquidity_flags_json"] = liquidity_df["liquidity_flags"].apply(lambda flags: json.dumps(flags, ensure_ascii=False))
    for col, default in {
        "bid5_amount": 0.0,
        "ask5_amount": 0.0,
        "order_book_depth_amount": 0.0,
        "bid_ask_imbalance": np.nan,
    }.items():
        if col not in df.columns:
            df[col] = default
        df[col] = pd.to_numeric(df[col], errors="coerce")
    order_book_profiles = [evaluate_order_book_depth(row) for row in df.to_dict(orient="records")]
    order_book_df = pd.DataFrame(order_book_profiles, index=df.index)
    df["order_book_status"] = order_book_df["order_book_status"]
    df["order_book_score"] = _round_score(order_book_df["order_book_score"])
    df["order_book_reason"] = order_book_df["order_book_reason"]
    df["bid_ask_imbalance"] = pd.to_numeric(order_book_df["bid_ask_imbalance"], errors="coerce")
    df["order_book_flags_json"] = order_book_df["order_book_flags"].apply(lambda flags: json.dumps(flags, ensure_ascii=False))
    for col, default in {
        "market_cap": 0.0,
        "float_market_cap": 0.0,
        "total_shares": 0.0,
        "float_shares": 0.0,
    }.items():
        if col not in df.columns:
            df[col] = default
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
    size_profiles = [evaluate_size_liquidity_profile(row) for row in df.to_dict(orient="records")]
    size_df = pd.DataFrame(size_profiles, index=df.index)
    df["size_liquidity_status"] = size_df["size_liquidity_status"]
    df["size_liquidity_score"] = _round_score(size_df["size_liquidity_score"])
    df["size_liquidity_reason"] = size_df["size_liquidity_reason"]
    df["effective_market_cap"] = pd.to_numeric(size_df["effective_market_cap"], errors="coerce").fillna(0.0)
    goodwill = _numeric_col(df, "goodwill", 0.0).fillna(0.0)
    net_assets = _numeric_col(df, "net_assets", 0.0).fillna(0.0)
    net_assets_from_ps = _numeric_col(df, "net_asset_ps", 0.0).fillna(0.0) * _numeric_col(df, "total_shares", 0.0).fillna(0.0)
    effective_net_assets = net_assets.where(net_assets > 0, net_assets_from_ps)
    df["goodwill"] = goodwill
    df["net_assets"] = effective_net_assets.where(effective_net_assets > 0, net_assets)
    df["goodwill_to_net_asset_pct"] = (
        goodwill / effective_net_assets.replace(0, np.nan) * 100.0
    ).where((goodwill > 0) & (effective_net_assets > 0)).round(2)
    df["size_liquidity_flags_json"] = size_df["size_liquidity_flags"].apply(lambda flags: json.dumps(flags, ensure_ascii=False))
    df["liquidity_score"] = _round_score(
        df["liquidity_score"] * 0.80 + df["size_liquidity_score"] * 0.10 + df["order_book_score"] * 0.10
    )
    volume_temperature_profiles = [evaluate_volume_temperature_profile(row) for row in df.to_dict(orient="records")]
    volume_temperature_df = pd.DataFrame(volume_temperature_profiles, index=df.index)
    df["volume_temperature_status"] = volume_temperature_df["volume_temperature_status"]
    df["volume_temperature_score"] = _round_score(volume_temperature_df["volume_temperature_score"])
    df["volume_temperature_reason"] = volume_temperature_df["volume_temperature_reason"]
    df["volume_temperature_flags_json"] = volume_temperature_df["volume_temperature_flags"].apply(
        lambda flags: json.dumps(flags, ensure_ascii=False)
    )

    for col in ["notice_count", "notice_positive", "notice_negative", "notice_critical"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)
    df["risk_titles"] = df["risk_titles"].apply(lambda x: x if isinstance(x, list) else [])
    df["positive_titles"] = df["positive_titles"].apply(lambda x: x if isinstance(x, list) else [])
    relation_rules = event_relation_rules or []
    df["event_relation_rules"] = [relation_rules for _ in range(len(df))]

    close = _numeric_col(df, "close")
    amount = _numeric_col(df, "amount")
    turnover = _numeric_col(df, "turnover_ratio")
    df["relative_hs300_20"] = (
        _numeric_col(df, "pct_20", 0.0) - _safe_number(market_context.get("hs300_pct_20"), 0.0)
    ).round(2)
    # The preceding joins create many pandas blocks. Consolidate before the
    # wide optional-feature expansion to avoid fragmented-frame CPU overhead.
    df = df.copy()
    for col, default in {
        "dividend_count_3y": 0.0,
        "dividend_cash_per_share_3y": 0.0,
        "latest_dividend_cash_per_share": 0.0,
        "latest_dividend_plan": "",
        "latest_dividend_report_date": None,
        "ex_dividend_date": None,
    }.items():
        if col not in df.columns:
            df[col] = default
    df["dividend_count_3y"] = pd.to_numeric(df["dividend_count_3y"], errors="coerce").fillna(0.0)
    df["dividend_cash_per_share_3y"] = pd.to_numeric(df["dividend_cash_per_share_3y"], errors="coerce").fillna(0.0)
    df["latest_dividend_cash_per_share"] = pd.to_numeric(
        df["latest_dividend_cash_per_share"], errors="coerce"
    ).fillna(0.0)
    df["latest_dividend_yield_pct"] = (
        df["latest_dividend_cash_per_share"] / close.replace(0, np.nan) * 100.0
    ).fillna(0.0).round(2)
    df["avg_dividend_yield_pct_3y"] = (
        (df["dividend_cash_per_share_3y"] / 3.0) / close.replace(0, np.nan) * 100.0
    ).fillna(0.0).round(2)
    dividend_score = (
        48.0
        + _series_score(df["avg_dividend_yield_pct_3y"], 0.0, 4.0, default=30.0) * 0.32
        + _series_score(df["dividend_count_3y"], 0.0, 3.0, default=0.0) * 0.20
    )
    df["dividend_score"] = _round_score(dividend_score)
    df = df.copy()
    for col, default in {
        "research_theme_score": 0.0,
        "research_theme_name": "",
        "research_theme_id": "",
        "research_theme_trend": "",
        "research_evidence_level": "",
        "research_theme_role": "",
        "research_theme_tier": "",
        "research_verification": "",
        "research_risk": "",
    }.items():
        if col not in df.columns:
            df[col] = default
    df["research_theme_score"] = pd.to_numeric(df["research_theme_score"], errors="coerce").fillna(0.0)

    df = df.copy()
    for col, default in {
        "north_holding_ratio": 0.0,
        "north_holding_ratio_delta_3d": 0.0,
        "north_holding_ratio_delta_5d": 0.0,
        "north_holding_market_value": 0.0,
        "north_holding_shares": 0.0,
        "north_net_buy_amount_3d": 0.0,
        "north_net_buy_amount_5d": 0.0,
        "north_stock_score": 50.0,
        "fund_hold_ratio": 0.0,
        "qfii_hold_ratio": 0.0,
        "rqfii_hold_ratio": 0.0,
        "social_security_hold_ratio": 0.0,
        "private_fund_hold_ratio": 0.0,
        "institution_hold_ratio": 0.0,
        "rating_upgrade_count_90d": 0.0,
        "rating_downgrade_count_90d": 0.0,
        "target_price": np.nan,
        "survey_count_90d": 0.0,
        "broker_gold_count_90d": 0.0,
        "investor_interaction_count_180d": 0.0,
        "investor_interaction_support_count": 0.0,
        "investor_interaction_risk_count": 0.0,
        "investor_interaction_score": 50.0,
        "industry_price_change_30d": 0.0,
        "capacity_utilization": 0.0,
        "external_prosperity_score": np.nan,
        "order_contract_amount_180d": 0.0,
        "order_contract_count_180d": 0.0,
    }.items():
        if col not in df.columns:
            df[col] = default
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
    for col, default in {
        "north_stock_trade_date": "",
        "north_stock_status": "UNKNOWN",
        "north_stock_reason": "stock-level northbound data unavailable",
        "institutional_trade_date": "",
        "rating_date": "",
        "latest_survey_date": "",
        "broker_gold_latest_date": "",
        "latest_investor_interaction_date": "",
        "latest_investor_interaction": "",
        "investor_interaction_status": "UNKNOWN",
        "investor_interaction_reason": "investor interaction data unavailable",
        "order_contract_latest_date": "",
        "business_scope": "",
        "business_profile_source": "",
    }.items():
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default).astype(str)

    df = df.copy()
    df["target_price_upside_pct"] = (
        (df["target_price"] / close.replace(0, np.nan) - 1.0) * 100.0
    ).where(df["target_price"] > 0).round(2)
    df["order_contract_to_revenue_pct"] = (
        df["order_contract_amount_180d"] / _numeric_col(df, "total_rev", 0.0).replace(0, np.nan) * 100.0
    ).where(df["order_contract_amount_180d"] > 0).fillna(0.0).round(2)
    business_profiles = [evaluate_business_purity(row) for row in df.to_dict(orient="records")]
    business_df = pd.DataFrame(business_profiles, index=df.index)
    df["business_purity_status"] = business_df["business_purity_status"]
    df["business_purity_score"] = _round_score(business_df["business_purity_score"])
    df["business_purity_match_count"] = pd.to_numeric(business_df["business_purity_match_count"], errors="coerce").fillna(0.0)
    df["business_purity_reason"] = business_df["business_purity_reason"]
    prosperity_profiles = [evaluate_industry_prosperity(row) for row in df.to_dict(orient="records")]
    prosperity_df = pd.DataFrame(prosperity_profiles, index=df.index)
    df["industry_prosperity_status"] = prosperity_df["industry_prosperity_status"]
    base_prosperity_score = pd.to_numeric(prosperity_df["industry_prosperity_score"], errors="coerce").fillna(50.0)
    external_prosperity = _numeric_col(df, "external_prosperity_score")
    df["industry_prosperity_score"] = _round_score(
        base_prosperity_score.where(external_prosperity.isna(), base_prosperity_score * 0.70 + external_prosperity * 0.30)
    )
    df["industry_prosperity_reason"] = prosperity_df["industry_prosperity_reason"]
    df["industry_prosperity_flags_json"] = prosperity_df["industry_prosperity_flags"].apply(
        lambda flags: json.dumps(flags, ensure_ascii=False)
    )
    institutional_profiles = [evaluate_institutional_profile(row) for row in df.to_dict(orient="records")]
    institutional_df = pd.DataFrame(institutional_profiles, index=df.index)
    df["institutional_status"] = institutional_df["institutional_status"]
    df["institutional_score"] = _round_score(institutional_df["institutional_score"])
    df["institutional_reason"] = institutional_df["institutional_reason"]
    df["institutional_flags_json"] = institutional_df["institutional_flags"].apply(
        lambda flags: json.dumps(flags, ensure_ascii=False)
    )
    interaction_profiles = [evaluate_investor_interaction_profile(row) for row in df.to_dict(orient="records")]
    interaction_df = pd.DataFrame(interaction_profiles, index=df.index)
    df["investor_interaction_status"] = interaction_df["investor_interaction_status"]
    df["investor_interaction_score"] = _round_score(interaction_df["investor_interaction_score"])
    df["investor_interaction_reason"] = interaction_df["investor_interaction_reason"]
    df["investor_interaction_flags_json"] = interaction_df["investor_interaction_flags"].apply(
        lambda flags: json.dumps(flags, ensure_ascii=False)
    )

    df = df.copy()
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
        + (_series_score(df["relative_hs300_20"], -10, 15, default=50.0) - 50) * 0.10
        + (_series_score(df["dist_ma20"], -15, 12) - 50) * 0.12
        + (_series_score(df["amount_ratio_5"], 0.5, 2.2) - 50) * 0.10
        + (_series_score(turnover, 0.5, 8.0) - 50) * 0.08
        - (_series_score(df["volatility_20"], 2.5, 9.0) - 50).clip(lower=0) * 0.10
    )
    df["technical_score"] = _round_score(technical)
    df["technical_score"] = _round_score(
        df["technical_score"] * 0.94 + _numeric_col(df, "volume_temperature_score", 60.0) * 0.06
    )

    main_ratio = _numeric_col(df, "main_net_inflow") / amount.replace(0, np.nan) * 100.0
    flow5_ratio = _numeric_col(df, "main_net_inflow_5d") / (_numeric_col(df, "amount_ma5") * 5).replace(0, np.nan) * 100.0
    flow10_ratio = _numeric_col(df, "main_net_inflow_10d") / (_numeric_col(df, "amount_ma20") * 10).replace(0, np.nan) * 100.0
    flow20_ratio = _numeric_col(df, "main_net_inflow_20d") / (_numeric_col(df, "amount_ma20") * 20).replace(0, np.nan) * 100.0
    capital = (
        _percentile_score(main_ratio, default=50) * 0.35
        + _percentile_score(flow5_ratio, default=50) * 0.28
        + _percentile_score(flow10_ratio, default=50) * 0.22
        + _percentile_score(flow20_ratio, default=50) * 0.15
    )
    if flow_date and flow_date != trade_date:
        capital = capital * 0.75 + 50 * 0.25
    df["capital_score"] = _round_score(capital)

    holder_ratio = _numeric_col(df, "holder_num_ratio")
    lhb_net_ratio = _numeric_col(df, "lhb_net_amount_20d", 0.0) / amount.replace(0, np.nan) * 100.0
    lhb_inst_net = _numeric_col(df, "lhb_inst_net_amount_20d", 0.0).fillna(0.0)
    lhb_inst_net_ratio = lhb_inst_net / amount.replace(0, np.nan) * 100.0
    margin_delta = _numeric_col(df, "margin_balance_delta", 0.0).fillna(0.0)
    margin_delta_3d = _numeric_col(df, "margin_balance_delta_3d", 0.0).fillna(0.0)
    financing_buy_3d = _numeric_col(df, "financing_buy_amount_3d", 0.0).fillna(0.0)
    margin_expanding_days_3d = _numeric_col(df, "margin_expanding_days_3d", 0.0).fillna(0.0)
    margin_contracting_days_3d = _numeric_col(df, "margin_contracting_days_3d", 0.0).fillna(0.0)
    pledge_ratio = _numeric_col(df, "pledge_ratio", 0.0).fillna(0.0)
    reduction_ratio = _numeric_col(df, "reduction_max_ratio_90d", 0.0).fillna(0.0)
    financing_buy_ratio_3d = financing_buy_3d / (amount.replace(0, np.nan) * 3.0) * 100.0
    has_stock_margin = df.get("margin_trade_date", pd.Series("", index=df.index)).fillna("").astype(str).str.len() > 0
    financing_buy_ratio_3d = financing_buy_ratio_3d.where(has_stock_margin)
    market_margin_delta = _numeric_col(df, "market_margin_balance_delta", 0.0).fillna(0.0)
    north_net_3d = _numeric_col(df, "north_net_3d", 0.0).fillna(0.0)
    holder_bonus = pd.Series(
        np.select(
            [holder_ratio <= -5.0, holder_ratio >= 10.0],
            [10.0, -12.0],
            default=0.0,
        ),
        index=df.index,
    )
    margin_bonus = pd.Series(
        np.select(
            [
                (margin_delta_3d >= 50_000_000.0) | (margin_expanding_days_3d >= 2.0),
                (margin_delta_3d <= -50_000_000.0) | (margin_contracting_days_3d >= 2.0),
                margin_delta > 0,
                margin_delta < 0,
                market_margin_delta > 0,
                market_margin_delta < 0,
            ],
            [7.0, -8.0, 4.0, -4.0, 3.0, -3.0],
            default=0.0,
        ),
        index=df.index,
    )
    lhb_inst_bonus = pd.Series(
        np.select(
            [lhb_inst_net >= 100_000_000.0, lhb_inst_net > 0.0, lhb_inst_net <= -100_000_000.0, lhb_inst_net < 0.0],
            [7.0, 4.0, -8.0, -5.0],
            default=0.0,
        ),
        index=df.index,
    )
    pledge_penalty = pd.Series(
        np.select([pledge_ratio >= PLEDGE_RATIO_CAP_PCT, pledge_ratio >= 35.0], [-10.0, -5.0], default=0.0),
        index=df.index,
    )
    reduction_penalty = pd.Series(
        np.select([reduction_ratio >= SHAREHOLDER_REDUCTION_RATIO_CAP_PCT, reduction_ratio > 0.0], [-9.0, -4.0], default=0.0),
        index=df.index,
    )
    chip_capital = (
        60.0
        + holder_bonus
        + (_series_score(lhb_net_ratio, -8.0, 8.0, default=50.0) - 50.0) * 0.18
        + (_series_score(lhb_inst_net_ratio, -5.0, 5.0, default=50.0) - 50.0) * 0.16
        + (_series_score(financing_buy_ratio_3d, 0.0, 25.0, default=50.0) - 50.0) * 0.06
        + lhb_inst_bonus
        + margin_bonus
        + pledge_penalty
        + reduction_penalty
    )
    df["chip_capital_score"] = _round_score(chip_capital)
    df["capital_score"] = _round_score(df["capital_score"] * 0.84 + df["chip_capital_score"] * 0.16)
    df["capital_score"] = _round_score(df["capital_score"] * 0.90 + _numeric_col(df, "liquidity_score", 60.0) * 0.10)
    north_capital_adjust = pd.Series(
        np.select([north_net_3d >= 3_000_000_000.0, north_net_3d <= -3_000_000_000.0], [2.5, -3.0], default=0.0),
        index=df.index,
    )
    df["capital_score"] = _round_score(df["capital_score"] + north_capital_adjust)
    north_stock_adjust = pd.Series(
        np.select(
            [
                (_numeric_col(df, "north_holding_ratio", 0.0) >= NORTH_STOCK_HOLDING_MIN_RATIO_PCT)
                & ((_numeric_col(df, "north_holding_ratio_delta_3d", 0.0) >= 0.10) | (_numeric_col(df, "north_net_buy_amount_3d", 0.0) > 0)),
                (_numeric_col(df, "north_holding_ratio_delta_3d", 0.0) <= NORTH_STOCK_REDUCTION_DELTA_PCT)
                | (_numeric_col(df, "north_net_buy_amount_3d", 0.0) <= -50_000_000.0),
            ],
            [3.0, -4.0],
            default=0.0,
        ),
        index=df.index,
    )
    df["capital_score"] = _round_score(df["capital_score"] + north_stock_adjust)
    institutional_adjust = (_numeric_col(df, "institutional_score", 50.0) - 50.0).clip(-20.0, 25.0)
    df["capital_score"] = _round_score(df["capital_score"] + institutional_adjust * 0.12)
    interaction_adjust = (_numeric_col(df, "investor_interaction_score", 50.0) - 50.0).clip(-20.0, 20.0)

    sentiment = pd.Series(50.0, index=df.index)
    has_hot = pd.to_numeric(df["fused_rank"], errors="coerce").notna()
    sentiment.loc[has_hot] = (101 - pd.to_numeric(df.loc[has_hot, "fused_rank"], errors="coerce")).clip(0, 100) * 0.55 + 45
    sentiment = sentiment * 0.8 + float(market_mood_score) * 0.2
    sentiment = sentiment + pd.Series(
        np.select(
            [
                df["market_extreme_status"].astype(str).str.upper() == "OVERHEAT",
                df["market_extreme_status"].astype(str).str.upper() == "OVERSOLD",
            ],
            [-4.0, 3.0],
            default=0.0,
        ),
        index=df.index,
    )
    sentiment = sentiment + pd.Series(
        np.select([north_net_3d >= 3_000_000_000.0, north_net_3d <= -3_000_000_000.0], [2.0, -2.5], default=0.0),
        index=df.index,
    )
    etf_net_3d = _numeric_col(df, "etf_net_3d", 0.0).fillna(0.0)
    sentiment = sentiment + pd.Series(
        np.select(
            [etf_net_3d >= ETF_FLOW_SUPPORT_AMOUNT_3D, etf_net_3d <= ETF_FLOW_PRESSURE_AMOUNT_3D],
            [1.5, -2.0],
            default=0.0,
        ),
        index=df.index,
    )
    macro_status = df["macro_policy_status"].astype(str).str.upper()
    sentiment = sentiment + pd.Series(
        np.select(
            [macro_status == "SUPPORT", macro_status == "RISK"],
            [2.0, -3.5],
            default=0.0,
        ),
        index=df.index,
    )
    sentiment = sentiment - _numeric_col(df, "macro_policy_critical_count", 0.0).clip(upper=3.0) * 1.5
    macro_indicator_status = df["macro_indicator_status"].astype(str).str.upper()
    sentiment = sentiment + pd.Series(
        np.select(
            [macro_indicator_status == "SUPPORT", macro_indicator_status == "RISK"],
            [1.8, -3.0],
            default=0.0,
        ),
        index=df.index,
    )
    macro_cycle = df["macro_cycle"].astype(str).str.upper()
    sentiment = sentiment + pd.Series(
        np.select(
            [macro_cycle == "RECOVERY", macro_cycle == "OVERHEAT", macro_cycle.isin(["STAGFLATION", "RECESSION"])],
            [1.5, -1.0, -2.0],
            default=0.0,
        ),
        index=df.index,
    )
    external_status = df["external_market_status"].astype(str).str.upper()
    external_score = _numeric_col(df, "external_market_score", 50.0).fillna(50.0)
    external_adjustment = (external_score - 50.0).clip(-8.0, 8.0) * 0.28
    external_adjustment = external_adjustment.where(
        external_status.isin(["SUPPORT", "RISK"]),
        0.0,
    )
    sentiment = sentiment + external_adjustment
    df["external_market_adjustment"] = external_adjustment.round(2)
    retail_status = df["retail_sentiment_status"].astype(str).str.upper()
    institutional_or_north_support = (
        (institutional_adjust >= 4.0)
        | (north_net_3d >= 3_000_000_000.0)
        | (north_stock_adjust > 0.0)
    )
    institutional_or_north_weak = (
        (institutional_adjust <= -4.0)
        | (north_net_3d <= -3_000_000_000.0)
        | (north_stock_adjust < 0.0)
    )
    retail_sentiment_adjust = pd.Series(
        np.select(
            [
                (retail_status == "EXTREME_BULLISH") & institutional_or_north_weak,
                retail_status == "EXTREME_BULLISH",
                (retail_status == "EXTREME_BEARISH") & institutional_or_north_support,
                retail_status == "EXTREME_BEARISH",
                retail_status == "ELEVATED",
            ],
            [-3.5, -1.5, 1.8, 0.8, -0.4],
            default=0.0,
        ),
        index=df.index,
    )
    sentiment = sentiment + retail_sentiment_adjust
    research_sentiment_bonus = pd.Series(
        np.where(df["research_theme_score"] >= 80.0, (df["research_theme_score"] - 70.0).clip(0, 20) * 0.18, 0.0),
        index=df.index,
    )
    sentiment = sentiment + research_sentiment_bonus
    sentiment = sentiment + institutional_adjust * 0.06 + north_stock_adjust * 0.35 + interaction_adjust * 0.06
    df["sentiment_score"] = _round_score(sentiment)
    df["market_mood_score"] = float(market_mood_score)

    roe = _series_score(_numeric_col(df, "roe_wtd"), -5, 18)
    gross_margin = _series_score(_numeric_col(df, "gross_margin"), 5, 45)
    net_margin = _series_score(_numeric_col(df, "net_margin"), -10, 20)
    oper_cf = _series_score(_numeric_col(df, "oper_cf_ps"), -1, 2)
    base_fundamental = roe * 0.38 + gross_margin * 0.24 + net_margin * 0.24 + oper_cf * 0.14
    roic_score = _series_score(_numeric_col(df, "roic"), 0, 20, default=np.nan)
    df["fundamental_score"] = _round_score(
        base_fundamental.where(roic_score.isna(), base_fundamental * 0.92 + roic_score * 0.08)
    )

    rev_growth = _series_score(_numeric_col(df, "total_rev_yoy_gr"), -20, 40)
    profit_growth = _series_score(_numeric_col(df, "net_profit_yoy_gr"), -40, 80)
    non_gaap_growth = _series_score(_numeric_col(df, "non_gaap_net_profit_yoy_gr"), -40, 80)
    rev_qoq_growth = _series_score(_numeric_col(df, "total_rev_qoq_gr"), -30, 40)
    profit_qoq_growth = _series_score(_numeric_col(df, "net_profit_qoq_gr"), -40, 60)
    df["growth_score"] = _round_score(
        rev_growth * 0.28
        + profit_growth * 0.36
        + non_gaap_growth * 0.18
        + rev_qoq_growth * 0.08
        + profit_qoq_growth * 0.10
    )
    research_growth_bonus = pd.Series(
        np.where(
            (df["research_theme_score"] >= 82.0)
            & df["research_evidence_level"].fillna("").astype(str).str.contains("财报|需求|国产|产业", regex=True),
            3.0,
            0.0,
        ),
        index=df.index,
    )
    df["growth_score"] = _round_score(df["growth_score"] + research_growth_bonus)
    prosperity_adjust = (_numeric_col(df, "industry_prosperity_score", 50.0) - 50.0).clip(-25.0, 30.0)
    business_adjust = (_numeric_col(df, "business_purity_score", 50.0) - 50.0).clip(-25.0, 25.0)
    df["growth_score"] = _round_score(df["growth_score"] + prosperity_adjust * 0.16 + business_adjust * 0.08 + interaction_adjust * 0.05)
    df["fundamental_score"] = _round_score(
        df["fundamental_score"] + prosperity_adjust * 0.05 + business_adjust * 0.04 + institutional_adjust * 0.04
    )

    eps = _numeric_col(df, "basic_eps")
    pe = close / eps.replace(0, np.nan)
    pe = pe.mask(eps <= 0)
    pb = close / _numeric_col(df, "net_asset_ps").replace(0, np.nan)
    pb = pb.mask(pb <= 0)
    revenue = _numeric_col(df, "total_rev")
    market_cap_for_ps = _numeric_col(df, "effective_market_cap")
    market_cap_for_ps = market_cap_for_ps.where(market_cap_for_ps > 0, _numeric_col(df, "market_cap"))
    ps_ratio = market_cap_for_ps / revenue.replace(0, np.nan)
    ps_ratio = ps_ratio.mask((ps_ratio <= 0) | (revenue <= 0) | (market_cap_for_ps <= 0))
    growth_for_peg = pd.concat([
        _numeric_col(df, "net_profit_yoy_gr"),
        _numeric_col(df, "non_gaap_net_profit_yoy_gr"),
        _numeric_col(df, "total_rev_yoy_gr"),
    ], axis=1).max(axis=1, skipna=True)
    valuation_payloads = []
    for idx, row in enumerate(df.to_dict(orient="records")):
        valuation_payloads.append(evaluate_peg_valuation(
            pe_ttm=pe.iloc[idx],
            pb_ratio=pb.iloc[idx],
            growth_pct=growth_for_peg.iloc[idx],
            industry_name=row.get("industry_name") or row.get("sector_industry_name") or "",
        ))
    valuation_df = pd.DataFrame(valuation_payloads, index=df.index)
    if valuation_df.empty:
        valuation_df = pd.DataFrame({
            "pe_ttm": pd.Series(np.nan, index=df.index),
            "pb_ratio": pd.Series(np.nan, index=df.index),
            "peg_ratio": pd.Series(np.nan, index=df.index),
            "peg_upper": pd.Series(np.nan, index=df.index),
            "valuation_style": pd.Series("general", index=df.index),
            "valuation_style_label": pd.Series("通用类型", index=df.index),
            "valuation_status": pd.Series("WATCH", index=df.index),
            "valuation_reason": pd.Series("估值数据不足，回退综合评分", index=df.index),
            "valuation_score": pd.Series(55.0, index=df.index),
        })
    df = df.copy()
    for col in [
        "pe_ttm", "pb_ratio", "peg_ratio", "peg_upper",
        "valuation_style", "valuation_style_label", "valuation_status", "valuation_reason",
    ]:
        df[col] = valuation_df[col]
    industry_key = df["industry_name"].fillna("").astype(str).where(df["industry_name"].fillna("").astype(str) != "")
    pe_ttm_series = pd.to_numeric(valuation_df["pe_ttm"], errors="coerce")
    valid_pe = pe_ttm_series.where(pe_ttm_series > 0)
    industry_pe_median = valid_pe.groupby(industry_key).transform(
        lambda s: s.median() if s.count() >= 3 else np.nan
    )
    pe_industry_multiple = valid_pe / industry_pe_median.replace(0, np.nan)
    relative_penalty = ((pe_industry_multiple - 1.5) * 28.0).clip(lower=0.0, upper=22.0).fillna(0.0)
    valid_ps = pd.to_numeric(ps_ratio, errors="coerce").where(ps_ratio > 0)
    industry_ps_median = valid_ps.groupby(industry_key).transform(
        lambda s: s.median() if s.count() >= 3 else np.nan
    )
    ps_industry_multiple = valid_ps / industry_ps_median.replace(0, np.nan)
    ps_relative_penalty = ((ps_industry_multiple - 2.0) * 12.0).clip(lower=0.0, upper=12.0).fillna(0.0)
    ps_absolute_penalty = ((valid_ps - 20.0) * 0.5).clip(lower=0.0, upper=8.0).fillna(0.0)
    valuation_history_percentile = _numeric_col(df, "close_percentile_250d")
    history_percentile_penalty = ((valuation_history_percentile - 80.0) * 0.5).clip(lower=0.0, upper=10.0).fillna(0.0)
    df["industry_pe_median"] = industry_pe_median.round(2)
    df["pe_industry_multiple"] = pe_industry_multiple.round(2)
    df["ps_ratio"] = valid_ps.round(2)
    df["industry_ps_median"] = industry_ps_median.round(2)
    df["ps_industry_multiple"] = ps_industry_multiple.round(2)
    df["valuation_history_percentile_250d"] = valuation_history_percentile.round(1)
    df["pe_percentile_250d"] = valuation_history_percentile.where(valid_pe > 0).round(1)
    df["pb_percentile_250d"] = valuation_history_percentile.where(pb > 0).round(1)
    df["valuation_score"] = _round_score(
        valuation_df["valuation_score"]
        - relative_penalty
        - ps_relative_penalty
        - ps_absolute_penalty
        - history_percentile_penalty
    )
    df["valuation_status"] = np.select(
        [
            df["valuation_score"] >= 70,
            df["valuation_score"] <= 40,
        ],
        ["PASS", "RISK"],
        default="WATCH",
    )
    relative_reasons = []
    for idx, reason in enumerate(df["valuation_reason"].fillna("").astype(str).tolist()):
        multiple = _safe_number(pe_industry_multiple.iloc[idx], 0.0)
        median = _safe_number(industry_pe_median.iloc[idx], 0.0)
        if multiple > 1.5 and median > 0:
            reason = f"{reason}；PE为行业中位数{multiple:.2f}倍，高于1.5倍上限"
        ps_multiple = _safe_number(ps_industry_multiple.iloc[idx], 0.0)
        ps_median = _safe_number(industry_ps_median.iloc[idx], 0.0)
        if ps_multiple > 2.0 and ps_median > 0:
            reason = f"{reason}；PS为行业中位数{ps_multiple:.2f}倍，高于2.0倍观察线"
        hist_pct = _safe_number(valuation_history_percentile.iloc[idx], 0.0)
        if hist_pct >= 80.0:
            reason = f"{reason}；250日估值/价格分位{hist_pct:.1f}%，处于历史偏高区"
        relative_reasons.append(reason)
    df["valuation_reason"] = relative_reasons
    df = df.copy()
    fundamental_profiles = [evaluate_fundamental_quality(row) for row in df.to_dict(orient="records")]
    fundamental_quality_df = pd.DataFrame(fundamental_profiles, index=df.index)
    df["fundamental_quality_status"] = fundamental_quality_df["fundamental_quality_status"]
    df["fundamental_quality_score"] = _round_score(fundamental_quality_df["fundamental_quality_score"])
    df["fundamental_quality_reason"] = fundamental_quality_df["fundamental_quality_reason"]
    df["fundamental_quality_flags_json"] = fundamental_quality_df["fundamental_quality_flags"].apply(
        lambda flags: json.dumps(flags, ensure_ascii=False)
    )
    df["fundamental_score"] = _round_score(
        df["fundamental_score"] * 0.86 + df["fundamental_quality_score"] * 0.14
    )
    dividend_relevant = (
        df["valuation_style"].fillna("").astype(str).str.lower().isin(["value", "stable"])
        | df["industry_name"].fillna("").astype(str).apply(is_defensive_industry)
    )
    df["fundamental_score"] = _round_score(
        df["fundamental_score"].where(~dividend_relevant, df["fundamental_score"] * 0.94 + df["dividend_score"] * 0.06)
    )

    debt_score = 100 - _series_score(_numeric_col(df, "asset_liab_ratio"), 30, 85) * 0.65
    cash_score = _series_score(_numeric_col(df, "cash_flow_ratio"), -0.2, 1.5)
    curr_score = _series_score(_numeric_col(df, "curr_ratio"), 0.8, 2.0)
    quick_score = _series_score(_numeric_col(df, "quick_ratio"), 0.6, 1.5)
    df["risk_score"] = _round_score(debt_score * 0.48 + cash_score * 0.22 + curr_score * 0.18 + quick_score * 0.12)
    goodwill_penalty = (
        (_numeric_col(df, "goodwill_to_net_asset_pct", 0.0) - GOODWILL_RATIO_WATCH_PCT)
        .clip(lower=0.0, upper=25.0)
        * 0.8
    ).fillna(0.0)
    df["risk_score"] = _round_score(df["risk_score"] - goodwill_penalty)
    df["risk_score"] = _round_score(
        df["risk_score"].where(~dividend_relevant, df["risk_score"] * 0.96 + df["dividend_score"] * 0.04)
    )

    df = df.copy()
    regime = str(market_context.get("market_regime") or "RANGE").upper()
    style_bias = str(market_context.get("style_bias") or "balanced").lower()
    valuation_style_series = df["valuation_style"].fillna("general").astype(str).str.lower()
    defensive_mask = df["industry_name"].fillna("").astype(str).apply(is_defensive_industry)
    style_adjustment = pd.Series(0.0, index=df.index)
    if regime == "BULL":
        if style_bias == "growth":
            growth_bonus = pd.Series(np.where(valuation_style_series == "growth", 5.0, 0.0), index=df.index)
            df["growth_score"] = _round_score(df["growth_score"] + growth_bonus)
            style_adjustment += growth_bonus
        elif style_bias == "large_value":
            value_bonus = pd.Series(
                np.where(valuation_style_series.isin(["value", "stable"]), 4.0, 0.0),
                index=df.index,
            )
            df["valuation_score"] = _round_score(df["valuation_score"] + value_bonus)
            style_adjustment += value_bonus
    elif regime == "BEAR":
        growth_penalty = pd.Series(
            np.where((valuation_style_series == "growth") & (~defensive_mask), -8.0, 0.0),
            index=df.index,
        )
        defense_bonus = pd.Series(np.where(defensive_mask, 5.0, 0.0), index=df.index)
        df["growth_score"] = _round_score(df["growth_score"] + growth_penalty)
        df["risk_score"] = _round_score(df["risk_score"] + defense_bonus + growth_penalty * 0.5)
        style_adjustment += growth_penalty + defense_bonus
    else:
        range_chip_bonus = pd.Series(
            np.where(
                (_numeric_col(df, "chip_capital_score", 60.0) >= 68.0)
                & (_numeric_col(df, "amount_ratio_20", 1.0).between(0.8, 2.2)),
                4.0,
                0.0,
            ),
            index=df.index,
        )
        df["capital_score"] = _round_score(df["capital_score"] + range_chip_bonus)
        style_adjustment += range_chip_bonus
    df["market_style_adjustment"] = style_adjustment.round(1)

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
        flags.extend(build_rule_flags(row))
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
            chase_risk_status=row.get("chase_risk_status"),
            ordinary_buy_eligible=row.get("ordinary_buy_eligible"),
            chase_risk_reason=row.get("chase_risk_reason"),
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
    technical_evidence_col: list[str] = []
    evidence_chain_col: list[str] = []
    failure_tags_col: list[str] = []
    min_rr = runtime_threshold("min_risk_reward", MIN_EXECUTABLE_RISK_REWARD)

    for row in df.to_dict(orient="records"):
        strengths: list[str] = []
        risks: list[str] = []
        raw_flags = row.get("data_quality_flags")
        if isinstance(raw_flags, str):
            try:
                flags = {str(item) for item in json.loads(raw_flags)}
            except Exception:
                flags = set()
        elif isinstance(raw_flags, (list, tuple, set)):
            flags = {str(item) for item in raw_flags}
        else:
            flags = set()
        external_status = str(row.get("external_market_status") or "").upper()
        external_reason = str(row.get("external_market_reason") or "").strip()
        external_capture = str(row.get("external_market_captured_at") or "").strip()
        if external_status == "SUPPORT":
            strengths.append(f"外围市场偏支持：{external_reason[:180]}")
        elif external_status == "RISK":
            risks.append(f"外围市场偏谨慎：{external_reason[:180]}")
        elif external_status == "UNKNOWN":
            risks.append("外围市场数据不可用，未将其作为买入依据")
        if external_capture:
            strengths.append(f"外围数据采集于{external_capture[:19]}")
        if float(row.get("technical_score") or 0) >= 68:
            strengths.append("技术趋势较强")
        if str(row.get("volume_temperature_status") or "").upper() == "PASS":
            strengths.append("量能处于温和放大区间")
        if float(row.get("capital_score") or 0) >= 68:
            strengths.append("资金流排名靠前")
        if float(row.get("main_inflow_days_10d") or 0) >= 7 and float(row.get("main_net_inflow_10d") or 0) > 0:
            strengths.append(f"近10日主力资金净流入{float(row.get('main_net_inflow_10d') or 0)/1e8:.2f}亿")
        if str(row.get("liquidity_status") or "").upper() == "PASS":
            strengths.append("流动性满足20日日均成交额与换手要求")
        if str(row.get("order_book_status") or "").upper() == "PASS":
            strengths.append(f"五档盘口深度{float(row.get('order_book_depth_amount') or 0)/1e8:.2f}亿，承接充足")
        if str(row.get("size_liquidity_status") or "").upper() == "PASS":
            strengths.append(f"流通/有效市值{float(row.get('effective_market_cap') or 0)/1e8:.1f}亿，满足50亿底线")
        if float(row.get("fundamental_score") or 0) >= 65:
            strengths.append("基本面质量较好")
        if str(row.get("fundamental_quality_status") or "").upper() == "PASS":
            strengths.append("基本面阈值通过")
        if float(row.get("avg_dividend_yield_pct_3y") or 0) >= 2.0 and float(row.get("dividend_count_3y") or 0) >= 2:
            strengths.append("分红连续性和股息率具备防御属性")
        if float(row.get("growth_score") or 0) >= 65:
            strengths.append("成长性评分较高")
        if float(row.get("valuation_score") or 0) >= 70:
            strengths.append("估值与成长匹配度较好")
        if float(row.get("sentiment_score") or 0) >= 68:
            strengths.append("市场热度较高")
        if str(row.get("sector_gate_status") or "").upper() == "PASS":
            strengths.append("所属板块资金和延续性通过")
        if float(row.get("theme_continuity_score_10") or 0) >= 8.0:
            strengths.append(f"题材延续性{float(row.get('theme_continuity_score_10') or 0):.1f}/10，高延续性")
        if str(row.get("sector_leadership_tier") or "").lower() in {"leader", "front"}:
            strengths.append(f"板块内位置{row.get('sector_leadership_tier')}，领导力{float(row.get('sector_leadership_score') or 0):.1f}")
        if float(row.get("market_style_adjustment") or 0) > 0:
            strengths.append(f"市场风格匹配: {row.get('market_style') or 'balanced'}")
        if float(row.get("research_theme_score") or 0) >= 82:
            strengths.append(f"研报主题命中: {row.get('research_theme_name') or 'research'}")
        if row.get("positive_titles"):
            strengths.append("近期公告偏积极")
        event_fulfillment = classify_event_fulfillment(row)
        if event_fulfillment.get("event_fulfillment_status") == "CONFIRMED":
            strengths.append(str(event_fulfillment.get("event_fulfillment_reason") or "正向事件仍有资金承接"))
        if float(row.get("chip_capital_score") or 0) >= 68:
            strengths.append("筹码/龙虎榜/两融证据偏积极")
        if row.get("holder_num_ratio") is not None and not pd.isna(row.get("holder_num_ratio")) and float(row.get("holder_num_ratio") or 0) <= -5:
            strengths.append("股东人数减少，筹码集中度改善")
        if float(row.get("lhb_net_amount_20d") or 0) > 0:
            strengths.append("近20日龙虎榜净买入为正")
        if float(row.get("lhb_inst_net_amount_20d") or 0) > 0:
            strengths.append(f"近20日龙虎榜机构席位净买入{float(row.get('lhb_inst_net_amount_20d') or 0)/1e8:.2f}亿")
        if float(row.get("north_net_3d") or 0) >= 3_000_000_000.0:
            strengths.append(f"北向资金3日净流入{float(row.get('north_net_3d') or 0)/1e8:.2f}亿")
        if float(row.get("relative_hs300_20") or 0) >= 10.0:
            strengths.append(f"20日跑赢沪深300 {float(row.get('relative_hs300_20') or 0):.1f}个百分点")
        if str(row.get("macro_policy_status") or "").upper() == "SUPPORT":
            strengths.append("宏观/政策新闻偏支持风险偏好")
        if str(row.get("macro_indicator_status") or "").upper() == "SUPPORT":
            strengths.append(f"macro hard data support, cycle={row.get('macro_cycle') or 'UNKNOWN'}")
        if float(row.get("etf_net_3d") or 0) >= ETF_FLOW_SUPPORT_AMOUNT_3D:
            strengths.append(f"ETF 3d net inflow {float(row.get('etf_net_3d') or 0)/1e8:.2f}e8")
        if str(row.get("investor_interaction_status") or "").upper() == "PASS":
            strengths.append("investor interaction validates demand/order clues")
        if str(row.get("retail_sentiment_status") or "").upper() == "EXTREME_BEARISH":
            strengths.append("散户情绪极度谨慎，具备反向情绪观察价值")
        if str(row.get("kline_pattern_direction") or "").lower() == "bullish":
            strengths.append(f"K线形态偏多: {row.get('kline_pattern') or 'pattern'}")
        if float(row.get("margin_balance_delta_3d") or 0) > 0:
            strengths.append(f"近3日两融余额增加{float(row.get('margin_balance_delta_3d') or 0)/1e8:.2f}亿")
        elif str(row.get("market_margin_status") or "").upper() == "EXPANDING" or float(row.get("margin_balance_delta") or 0) > 0:
            strengths.append("两融余额方向扩张")

        if row.get("event_risk_level") in ("HIGH", "CRITICAL"):
            risks.append("近期公告存在风险事项")
        if row.get("risk_titles"):
            risks.extend([f"公告风险: {t}" for t in row.get("risk_titles", [])[:2]])
        if classify_event_fulfillment(row).get("event_fulfillment_status") == "PRICED_IN":
            risks.append(str(classify_event_fulfillment(row).get("event_fulfillment_reason") or "正向事件已被价格消化"))
        if flow_date and flow_date != trade_date:
            risks.append(f"资金流使用最近可用日期{flow_date}")
        if float(row.get("main_outflow_days_10d") or 0) >= 7 and float(row.get("main_net_inflow_10d") or 0) < 0:
            risks.append(f"近10日主力资金净流出{abs(float(row.get('main_net_inflow_10d') or 0))/1e8:.2f}亿")
        if "margin_deleveraging_3d" in flags:
            risks.append(f"近3日两融余额收缩{abs(float(row.get('margin_balance_delta_3d') or 0))/1e8:.2f}亿")
        if float(row.get("amount") or 0) < 30_000_000:
            risks.append("成交额偏低")
        if str(row.get("volume_temperature_status") or "").upper() == "RISK":
            risks.append(str(row.get("volume_temperature_reason") or "量能异常放大，存在追高/出货风险"))
        if str(row.get("liquidity_status") or "").upper() in {"WATCH", "BLOCK"}:
            risks.append(str(row.get("liquidity_reason") or "流动性不满足策略阈值"))
        if str(row.get("order_book_status") or "").upper() == "WATCH":
            risks.append(str(row.get("order_book_reason") or "五档盘口深度或买卖盘平衡度不足"))
        if str(row.get("size_liquidity_status") or "").upper() == "WATCH":
            risks.append(str(row.get("size_liquidity_reason") or "流通/有效市值低于50亿"))
        if str(row.get("fundamental_quality_status") or "").upper() in {"WATCH", "BLOCK"}:
            risks.append(str(row.get("fundamental_quality_reason") or "基本面阈值未完全通过"))
        if float(row.get("roic") or 0) > 0 and float(row.get("roic") or 0) < 15:
            risks.append(f"ROIC {float(row.get('roic') or 0):.1f}%低于15%资本效率线")
        if float(row.get("acct_recv_to_rev") or 0) > 30:
            risks.append(f"应收账款/营收{float(row.get('acct_recv_to_rev') or 0):.1f}%，回款质量需复核")
        if float(row.get("prepayment_yoy_gr") or 0) > 50:
            risks.append(f"预付账款同比增长{float(row.get('prepayment_yoy_gr') or 0):.1f}%，存在资金占用风险")
        if float(row.get("related_transaction_to_rev") or 0) > 20:
            risks.append(f"关联交易/营收{float(row.get('related_transaction_to_rev') or 0):.1f}%，治理透明度需复核")
        if str(row.get("valuation_style") or "").lower() in {"value", "stable"} and float(row.get("dividend_count_3y") or 0) == 0:
            risks.append("价值/稳定风格缺少近3年现金分红记录")
        if float(row.get("research_theme_score") or 0) >= 82 and row.get("research_risk"):
            risks.append(f"研报主题风险: {row.get('research_risk')}")
        if float(row.get("change_pct") or 0) >= 9.7:
            risks.append("当日涨幅过高")
        if float(row.get("pct_5") or 0) >= 20:
            risks.append("近一周涨幅超过20%，追高风险较高")
        if float(row.get("volatility_20") or 0) >= 8:
            risks.append("短期波动偏大")
        if str(row.get("sector_gate_status") or "").upper() == "BLOCK":
            risks.append("所属板块资金和延续性不合格")
        if 0.0 < float(row.get("theme_continuity_score_10") or 0) <= 5.0:
            risks.append(str(row.get("theme_continuity_reason") or "题材延续性低于6分观察线"))
        if str(row.get("sector_leadership_tier") or "").lower() == "follower":
            risks.append("板块内位置偏后，容易成为跟风补涨或冲高回落标的")
        if str(row.get("market_extreme_status") or "").upper() == "OVERHEAT":
            risks.append("全市场宽度超过85%，拥挤度偏高")
        if str(row.get("market_regime") or "").upper() == "BEAR" and str(row.get("valuation_style") or "").lower() == "growth":
            risks.append("沪深300空头环境下成长风格胜率下降")
        if float(row.get("valuation_score") or 55) <= 40:
            risks.append(str(row.get("valuation_reason") or "估值偏贵，需等待业绩消化"))
        if row.get("holder_num_ratio") is not None and not pd.isna(row.get("holder_num_ratio")) and float(row.get("holder_num_ratio") or 0) >= 10:
            risks.append("股东人数明显增加，筹码分散")
        if float(row.get("lhb_net_amount_20d") or 0) < 0:
            risks.append("近20日龙虎榜净卖出")
        if float(row.get("lhb_inst_net_amount_20d") or 0) < 0:
            risks.append(f"近20日龙虎榜机构席位净卖出{abs(float(row.get('lhb_inst_net_amount_20d') or 0))/1e8:.2f}亿")
        if float(row.get("pledge_ratio") or 0) >= PLEDGE_RATIO_CAP_PCT:
            risks.append(f"大股东质押比例{float(row.get('pledge_ratio') or 0):.1f}%，超过50%风控线")
        if float(row.get("reduction_max_ratio_90d") or 0) >= SHAREHOLDER_REDUCTION_RATIO_CAP_PCT:
            risks.append(f"近3个月单一股东减持比例{float(row.get('reduction_max_ratio_90d') or 0):.1f}%，超过2%风控线")
        if float(row.get("goodwill_to_net_asset_pct") or 0) >= GOODWILL_RATIO_WATCH_PCT:
            risks.append(f"商誉/净资产{float(row.get('goodwill_to_net_asset_pct') or 0):.1f}%，存在减值敏感性")
        if float(row.get("north_net_3d") or 0) <= -3_000_000_000.0:
            risks.append(f"北向资金3日净流出{abs(float(row.get('north_net_3d') or 0))/1e8:.2f}亿")
        if float(row.get("relative_hs300_20") or 0) <= -10.0:
            risks.append(f"20日跑输沪深300 {abs(float(row.get('relative_hs300_20') or 0)):.1f}个百分点")
        if str(row.get("macro_policy_status") or "").upper() == "RISK":
            risks.append(str(row.get("macro_policy_latest_title") or row.get("macro_policy_reason") or "宏观/政策新闻偏压力"))
        if str(row.get("macro_indicator_status") or "").upper() == "RISK":
            risks.append(str(row.get("macro_indicator_reason") or "macro hard data pressure"))
        if float(row.get("etf_net_3d") or 0) <= ETF_FLOW_PRESSURE_AMOUNT_3D:
            risks.append(f"ETF 3d net outflow {abs(float(row.get('etf_net_3d') or 0))/1e8:.2f}e8")
        if str(row.get("investor_interaction_status") or "").upper() == "RISK":
            risks.append(str(row.get("investor_interaction_reason") or "investor interaction risk"))
        if str(row.get("retail_sentiment_status") or "").upper() == "EXTREME_BULLISH":
            risks.append(str(row.get("retail_sentiment_reason") or "散户情绪极度看多，注意机构承接是否同步"))
        unlock_pressure = evaluate_unlock_pressure(row)
        if unlock_pressure.get("unlock_status") in {"BLOCK", "WATCH"}:
            risks.append(str(unlock_pressure.get("unlock_reason") or "未来30日存在解禁压力"))
        if float(row.get("mine_clearance_score") or 0) >= 70:
            risks.append("扫雷数据提示财务或监管异常")
        if str(row.get("kline_pattern_direction") or "").lower() in {"bearish", "risk"}:
            risks.append(f"K线形态偏空: {row.get('kline_pattern') or 'pattern'}")
        if float(row.get("risk_reward_ratio") or 0) and float(row.get("risk_reward_ratio") or 0) < min_rr:
            risks.append(f"盈亏比低于{min_rr:.1f}:1")
        if not strengths:
            strengths.append("暂无突出优势，作为基础覆盖样本")

        status = row.get("recommend_status") or "SUSPENDED"
        probabilities = estimate_trade_probabilities(row)
        summary = (
            f"基础评分: 综合{row.get('ai_score'):.1f}, 短线{row.get('short_term_score'):.1f}, "
            f"长线{row.get('long_term_score'):.1f}; "
            f"上涨概率{probabilities['upside_probability_pct']:.1f}%，"
            f"下跌概率{probabilities['downside_probability_pct']:.1f}%；状态{status}。"
        )
        if status == "ALLOW":
            recommendation = "可进入候选池，盘中等待量价和资金二次确认，并设置止损。"
        elif status == "SUSPENDED":
            recommendation = "暂缓买入，等待评分或风险项改善后再复核。"
        else:
            recommendation = "不建议进入推荐池。"

        risk_titles = row.get("risk_titles") if isinstance(row.get("risk_titles"), list) else []
        positive_titles = row.get("positive_titles") if isinstance(row.get("positive_titles"), list) else []
        event_title = (risk_titles or positive_titles or ["暂无明确事件"])[0]
        event_impact = build_event_impact({**row, "risk_titles": risk_titles, "positive_titles": positive_titles})
        event_detail = {
            "notice_count_14d": int(row.get("notice_count") or 0),
            "positive_count": int(row.get("notice_positive") or 0),
            "negative_count": int(row.get("notice_negative") or 0),
            "critical_count": int(row.get("notice_critical") or 0),
            "risk_titles": risk_titles,
            "positive_titles": positive_titles,
            "event_impact": event_impact,
            "event_report": {
                "who": str(row.get("short_name") or row.get("stock_code") or ""),
                "what": event_title,
                "when": str(row.get("latest_notice_date") or row.get("trade_date") or trade_date)[:10],
                "where": str(row.get("industry_name") or row.get("sector_industry_name") or "未分组"),
                "why": "重大风险优先规避" if row.get("event_risk_level") == "CRITICAL" else "结合公告/新闻与板块资金判断影响",
                "how": recommendation,
                "how_much": f"事件分{float(row.get('event_score') or 0):.1f}，风险等级{row.get('event_risk_level') or 'LOW'}",
            },
        }
        technical_evidence = build_technical_evidence(row)
        evidence_chain = build_evidence_chain(row, trade_date=trade_date)
        failure_tags = build_failure_tags(row)
        summaries.append(summary)
        recommendations.append(recommendation)
        strengths_col.append(_json_list(strengths[:6]))
        risks_col.append(_json_list(risks[:6]))
        event_detail_col.append(json.dumps(event_detail, ensure_ascii=False))
        technical_evidence_col.append(json.dumps(technical_evidence, ensure_ascii=False))
        evidence_chain_col.append(json.dumps(evidence_chain, ensure_ascii=False))
        failure_tags_col.append(json.dumps(failure_tags, ensure_ascii=False))

    out = df.copy()
    out["summary"] = summaries
    out["recommendation"] = recommendations
    out["strengths"] = strengths_col
    out["risks"] = risks_col
    out["event_risk_detail"] = event_detail_col
    out["technical_evidence_json"] = technical_evidence_col
    out["evidence_chain_json"] = evidence_chain_col
    out["failure_tags_json"] = failure_tags_col
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
            "chase_policy_version": _safe_text_value(row.get("chase_policy_version"))[:64],
            "surge_streak_lower_bound": _none_if_nan(row.get("surge_streak_lower_bound")),
            "recent_max_surge_streak": _none_if_nan(row.get("recent_max_surge_streak")),
            "latest_danger_surge_streak": _none_if_nan(row.get("latest_danger_surge_streak")),
            "sessions_since_extreme_surge": _none_if_nan(row.get("sessions_since_extreme_surge")),
            "recent_extreme_run_return_pct": _none_if_nan(row.get("recent_extreme_run_return_pct")),
            "drawdown_from_recent_peak_pct": _none_if_nan(row.get("drawdown_from_recent_peak_pct")),
            "rebase_confirmed": bool(_is_explicit_true(row.get("rebase_confirmed"))),
            "exact_limit_up_streak": _none_if_nan(row.get("exact_limit_up_streak")),
            "trailing_untradeable_sessions": _none_if_nan(row.get("trailing_untradeable_sessions")),
            "latest_tradable_date": _none_if_nan(row.get("latest_tradable_date")),
            "limit_rule_status": _safe_text_value(row.get("limit_rule_status"))[:30],
            "capacity_state": _safe_text_value(row.get("capacity_state"), "UNKNOWN")[:30],
            "one_price_limit_up_proxy": _none_if_nan(row.get("one_price_limit_up_proxy")),
            "extreme_extension_flag": _none_if_nan(row.get("extreme_extension_flag")),
            "ordinary_buy_eligible": bool(_is_explicit_true(row.get("ordinary_buy_eligible"))),
            "chase_risk_status": _normalized_chase_gate_status(row.get("chase_risk_status")) or "DATA_BLOCKED",
            "chase_risk_reason": _safe_text_value(row.get("chase_risk_reason"))[:500],
            "chase_risk_evidence_json": _safe_text_value(
                row.get("chase_risk_evidence_json"), "{}"
            ),
            "model_version": str(row.get("model_version") or MODEL_VERSION)[:MODEL_VERSION_COLUMN_LENGTH],
        }
        for col in score_cols:
            item[col] = _none_if_nan(row.get(col))
        rows.append(item)
    return rows


def build_recommendation_rows(df: pd.DataFrame, trade_date: str, top_n: int, min_score: float) -> list[dict[str, Any]]:
    stock_code_series = df["stock_code"].astype(str).str.strip().str.zfill(6)
    excluded_prefix = stock_code_series.str.slice(0, 3).isin(EXCLUDED_RECOMMEND_PREFIXES)
    main_wave_signal_series = (
        df["main_wave_signal"].fillna("").astype(str)
        if "main_wave_signal" in df.columns
        else pd.Series("", index=df.index)
    )
    sector_gate_series = (
        df["sector_gate_status"].fillna("WATCH").astype(str).str.upper()
        if "sector_gate_status" in df.columns
        else pd.Series("WATCH", index=df.index)
    )
    chase_risk_series = (
        df["chase_risk_status"].fillna("DATA_BLOCKED").astype(str).str.upper()
        if "chase_risk_status" in df.columns
        else pd.Series("DATA_BLOCKED", index=df.index)
    )
    ordinary_buy_eligible = (
        df["ordinary_buy_eligible"].map(_is_explicit_true).fillna(False)
        if "ordinary_buy_eligible" in df.columns
        else pd.Series(False, index=df.index)
    )
    main_wave_candidate = (
        (_numeric_col(df, "main_wave_score", 0.0) >= float(STRATEGY_PROFILES["main_wave"]["min_score"]))
        & (df["recommend_status"] == "ALLOW")
        & (df["event_risk_level"] != "CRITICAL")
    )
    eligible = df[
        (
            ((df["recommend_status"] == "ALLOW") & (df["quality_score"] >= float(min_score)))
            | main_wave_candidate
        )
        & (df["signal_status"] != "BLOCK")
        & (~main_wave_signal_series.isin(["REDUCE", "SELL_ALERT"]))
        & (stock_code_series.str.match(r"^(0|3|6)"))
        & (~excluded_prefix)
        & (sector_gate_series != "BLOCK")
        & (chase_risk_series == "ALLOW")
        & ordinary_buy_eligible
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
        sources = "premarket_external" if str(row.get("external_market_status") or "").upper() != "UNKNOWN" else "fast_eod"
        if row.get("external_market_captured_at"):
            sources += "+external:akshare_eastmoney"
        if row.get("source_flag") and not pd.isna(row.get("source_flag")):
            sources += f"+hot:{row.get('source_flag')}"
        rows.append({
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "short_name": str(row.get("short_name") or "")[:20],
            "industry_name": str(
                row.get("industry_name")
                or row.get("sector_industry_name")
                or ""
            )[:128],
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
            "investment_rating": row.get("investment_rating") or "中性",
            "rating_reason": str(row.get("rating_reason") or "")[:500],
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
            "risk_reward_ratio": round(float(row.get("risk_reward_ratio") or 0), 2),
            "resistance_price": _none_if_nan(row.get("resistance_price")),
            "sector_gate_status": row.get("sector_gate_status") or "WATCH",
            "sector_gate_reason": str(row.get("sector_gate_reason") or "")[:500],
            "sector_flow_3d": _none_if_nan(row.get("sector_flow_3d")),
            "sector_width_pct": _none_if_nan(row.get("sector_width_pct")),
            "chase_policy_version": _safe_text_value(row.get("chase_policy_version"))[:64],
            "surge_streak_lower_bound": _none_if_nan(row.get("surge_streak_lower_bound")),
            "recent_max_surge_streak": _none_if_nan(row.get("recent_max_surge_streak")),
            "latest_danger_surge_streak": _none_if_nan(row.get("latest_danger_surge_streak")),
            "sessions_since_extreme_surge": _none_if_nan(row.get("sessions_since_extreme_surge")),
            "recent_extreme_run_return_pct": _none_if_nan(row.get("recent_extreme_run_return_pct")),
            "drawdown_from_recent_peak_pct": _none_if_nan(row.get("drawdown_from_recent_peak_pct")),
            "rebase_confirmed": bool(_is_explicit_true(row.get("rebase_confirmed"))),
            "exact_limit_up_streak": _none_if_nan(row.get("exact_limit_up_streak")),
            "trailing_untradeable_sessions": _none_if_nan(row.get("trailing_untradeable_sessions")),
            "latest_tradable_date": _none_if_nan(row.get("latest_tradable_date")),
            "limit_rule_status": _safe_text_value(row.get("limit_rule_status"))[:30],
            "capacity_state": _safe_text_value(row.get("capacity_state"), "UNKNOWN")[:30],
            "one_price_limit_up_proxy": _none_if_nan(row.get("one_price_limit_up_proxy")),
            "extreme_extension_flag": _none_if_nan(row.get("extreme_extension_flag")),
            "ordinary_buy_eligible": bool(_is_explicit_true(row.get("ordinary_buy_eligible"))),
            "chase_risk_status": _normalized_chase_gate_status(row.get("chase_risk_status")) or "DATA_BLOCKED",
            "chase_risk_reason": _safe_text_value(row.get("chase_risk_reason"))[:500],
            "chase_risk_evidence_json": _safe_text_value(
                row.get("chase_risk_evidence_json"), "{}"
            ),
            "technical_evidence_json": row.get("technical_evidence_json") or "{}",
            "evidence_chain_json": row.get("evidence_chain_json") or "[]",
            "review_1d_pct": _none_if_nan(row.get("review_1d_pct")),
            "review_3d_pct": _none_if_nan(row.get("review_3d_pct")),
            "review_5d_pct": _none_if_nan(row.get("review_5d_pct")),
            "review_10d_pct": _none_if_nan(row.get("review_10d_pct")),
            "failure_tags_json": row.get("failure_tags_json") or "[]",
            "heat_overload_score": round(float(row.get("heat_overload_score") or 0), 1),
            "confidence_score": round(float(row.get("confidence_score") or 0), 1),
            "chip_capital_score": round(float(row.get("chip_capital_score") or 0), 1),
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
            "model_version": str(row.get("model_version") or MODEL_VERSION)[:MODEL_VERSION_COLUMN_LENGTH],
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
    quoted_table = quote_identifier(table_name)
    quoted_date_column = quote_identifier(date_column)
    quoted_stock_code = quote_identifier("stock_code")
    if not codes:
        conn.execute(
            text(f"DELETE FROM {quoted_table} WHERE {quoted_date_column} = :trade_date"),
            {"trade_date": trade_date},
        )
        return

    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params = {"trade_date": trade_date, **{f"code_{idx}": code for idx, code in enumerate(codes)}}
    conn.execute(
        text(
            f"DELETE FROM {quoted_table} "
            f"WHERE {quoted_date_column} = :trade_date AND {quoted_stock_code} IN ({placeholders})"
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
            chase_policy_version, surge_streak_lower_bound,
            recent_max_surge_streak, latest_danger_surge_streak,
            sessions_since_extreme_surge, recent_extreme_run_return_pct,
            drawdown_from_recent_peak_pct, rebase_confirmed,
            exact_limit_up_streak,
            trailing_untradeable_sessions, latest_tradable_date, limit_rule_status,
            capacity_state, one_price_limit_up_proxy, extreme_extension_flag,
            ordinary_buy_eligible, chase_risk_status, chase_risk_reason,
            chase_risk_evidence_json,
            created_at, updated_at
        ) VALUES (
            :stock_code, :stock_name, :analysis_date, :last_news_time,
            :long_term_score, :fundamental_score, :growth_score, :valuation_score, :risk_score,
            :short_term_score, :capital_score, :technical_score, :sentiment_score, :event_score,
            :event_risk_score, :event_risk_level, :event_risk_detail,
            :recommend_status, :recommend_reason,
            :summary, :recommendation, :strengths, :risks,
            :data_quality_score, :data_quality_flags, :flow_trade_date, :hot_trade_date, :model_version,
            :chase_policy_version, :surge_streak_lower_bound,
            :recent_max_surge_streak, :latest_danger_surge_streak,
            :sessions_since_extreme_surge, :recent_extreme_run_return_pct,
            :drawdown_from_recent_peak_pct, :rebase_confirmed,
            :exact_limit_up_streak,
            :trailing_untradeable_sessions, :latest_tradable_date, :limit_rule_status,
            :capacity_state, :one_price_limit_up_proxy, :extreme_extension_flag,
            :ordinary_buy_eligible, :chase_risk_status, :chase_risk_reason,
            :chase_risk_evidence_json,
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
            chase_policy_version = VALUES(chase_policy_version),
            surge_streak_lower_bound = VALUES(surge_streak_lower_bound),
            recent_max_surge_streak = VALUES(recent_max_surge_streak),
            latest_danger_surge_streak = VALUES(latest_danger_surge_streak),
            sessions_since_extreme_surge = VALUES(sessions_since_extreme_surge),
            recent_extreme_run_return_pct = VALUES(recent_extreme_run_return_pct),
            drawdown_from_recent_peak_pct = VALUES(drawdown_from_recent_peak_pct),
            rebase_confirmed = VALUES(rebase_confirmed),
            exact_limit_up_streak = VALUES(exact_limit_up_streak),
            trailing_untradeable_sessions = VALUES(trailing_untradeable_sessions),
            latest_tradable_date = VALUES(latest_tradable_date),
            limit_rule_status = VALUES(limit_rule_status),
            capacity_state = VALUES(capacity_state),
            one_price_limit_up_proxy = VALUES(one_price_limit_up_proxy),
            extreme_extension_flag = VALUES(extreme_extension_flag),
            ordinary_buy_eligible = VALUES(ordinary_buy_eligible),
            chase_risk_status = VALUES(chase_risk_status),
            chase_risk_reason = VALUES(chase_risk_reason),
            chase_risk_evidence_json = VALUES(chase_risk_evidence_json),
            updated_at = NOW()
    """
    rec_sql = """
        INSERT INTO st_recommended_stocks (
            stock_code, short_name, industry_name,
            ai_score, long_term_score, short_term_score,
            fundamental, capital_score, valuation, technical,
            reason, sources, pick_date,
            recommend_status, recommend_reason, event_risk_level,
            last_check_time, sentiment_score, market_mood_score, event_score,
            ultra_short_score, swing_score, primary_strategy, strategy_profile,
            suitable_strategies, signal_status, signal_reason,
            investment_rating, rating_reason,
            entry_price_low, entry_price_high, stop_loss_price,
            take_profit_1, take_profit_2, position_weight, max_holding_days,
            entry_conditions_json, sell_rules_json, invalidation_reason,
            quality_score, entry_score, final_trade_score,
            expected_return_score, expected_return_pct, risk_reward_ratio, resistance_price,
            sector_gate_status, sector_gate_reason, sector_flow_3d, sector_width_pct,
            chase_policy_version, surge_streak_lower_bound,
            recent_max_surge_streak, latest_danger_surge_streak,
            sessions_since_extreme_surge, recent_extreme_run_return_pct,
            drawdown_from_recent_peak_pct, rebase_confirmed,
            exact_limit_up_streak,
            trailing_untradeable_sessions, latest_tradable_date, limit_rule_status,
            capacity_state, one_price_limit_up_proxy, extreme_extension_flag,
            ordinary_buy_eligible, chase_risk_status, chase_risk_reason,
            chase_risk_evidence_json,
            technical_evidence_json, evidence_chain_json,
            review_1d_pct, review_3d_pct, review_5d_pct, review_10d_pct, failure_tags_json,
            heat_overload_score, confidence_score, sector_rotation_score,
            chip_capital_score, failure_penalty_score, data_quality_score, data_quality_flags, cooldown_days_left, cooldown_until,
            main_wave_score, trend_hold_score, main_wave_stage, main_wave_signal,
            main_wave_reason, trend_stop_price, trend_reduce_price,
            model_version,
            created_at
        ) VALUES (
            :stock_code, :short_name, :industry_name,
            :ai_score, :long_term_score, :short_term_score,
            :fundamental, :capital_score, :valuation, :technical,
            :reason, :sources, :pick_date,
            :recommend_status, :recommend_reason, :event_risk_level,
            NOW(), :sentiment_score, :market_mood_score, :event_score,
            :ultra_short_score, :swing_score, :primary_strategy, :strategy_profile,
            :suitable_strategies, :signal_status, :signal_reason,
            :investment_rating, :rating_reason,
            :entry_price_low, :entry_price_high, :stop_loss_price,
            :take_profit_1, :take_profit_2, :position_weight, :max_holding_days,
            :entry_conditions_json, :sell_rules_json, :invalidation_reason,
            :quality_score, :entry_score, :final_trade_score,
            :expected_return_score, :expected_return_pct, :risk_reward_ratio, :resistance_price,
            :sector_gate_status, :sector_gate_reason, :sector_flow_3d, :sector_width_pct,
            :chase_policy_version, :surge_streak_lower_bound,
            :recent_max_surge_streak, :latest_danger_surge_streak,
            :sessions_since_extreme_surge, :recent_extreme_run_return_pct,
            :drawdown_from_recent_peak_pct, :rebase_confirmed,
            :exact_limit_up_streak,
            :trailing_untradeable_sessions, :latest_tradable_date, :limit_rule_status,
            :capacity_state, :one_price_limit_up_proxy, :extreme_extension_flag,
            :ordinary_buy_eligible, :chase_risk_status, :chase_risk_reason,
            :chase_risk_evidence_json,
            :technical_evidence_json, :evidence_chain_json,
            :review_1d_pct, :review_3d_pct, :review_5d_pct, :review_10d_pct, :failure_tags_json,
            :heat_overload_score, :confidence_score, :sector_rotation_score,
            :chip_capital_score, :failure_penalty_score, :data_quality_score, :data_quality_flags, :cooldown_days_left, :cooldown_until,
            :main_wave_score, :trend_hold_score, :main_wave_stage, :main_wave_signal,
            :main_wave_reason, :trend_stop_price, :trend_reduce_price,
            :model_version,
            NOW()
        )
    """
    _ensure_output_schema(engine)
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


def _json_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if not value:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
    return []


def _pick_review_tag(returns: dict[str, float | None]) -> str:
    if returns.get("review_10d_pct") is not None and float(returns["review_10d_pct"]) <= -7.0:
        return "review_loss_10d"
    if returns.get("review_5d_pct") is not None and float(returns["review_5d_pct"]) <= -5.0:
        return "review_loss_5d"
    if returns.get("review_3d_pct") is not None and float(returns["review_3d_pct"]) <= -3.0:
        return "review_loss_3d"
    if returns.get("review_1d_pct") is not None and float(returns["review_1d_pct"]) <= -3.0:
        return "review_loss_1d"
    return ""


def backfill_recommendation_reviews(
    engine: Engine,
    as_of_date: str,
    lookback_days: int = 90,
    stock_codes: list[str] | None = None,
) -> dict[str, int]:
    """Backfill 1/3/5/10 day recommendation returns and generated failure samples."""
    as_of_date = str(as_of_date or "")[:10]
    if not as_of_date:
        return {"checked": 0, "updated": 0, "failure_samples": 0}
    _ensure_recommended_columns(engine)
    _ensure_learning_tables(engine)

    codes = _normalize_stock_codes(stock_codes)
    conditions = [
        "pick_date <= :as_of_date",
        "pick_date >= DATE_SUB(:as_of_date, INTERVAL :lookback DAY)",
    ]
    params: dict[str, Any] = {"as_of_date": as_of_date, "lookback": max(1, int(lookback_days))}
    if codes:
        placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
        conditions.append(f"stock_code IN ({placeholders})")
        params.update({f"code_{idx}": code for idx, code in enumerate(codes)})

    recs = _read_frame(text(f"""
        SELECT stock_code, short_name, pick_date, primary_strategy, strategy_profile, failure_tags_json
        FROM st_recommended_stocks
        WHERE {" AND ".join(conditions)}
        ORDER BY pick_date DESC, stock_code
    """), engine, params=params)
    if recs.empty:
        return {"checked": 0, "updated": 0, "failure_samples": 0}

    recs["stock_code"] = recs["stock_code"].astype(str).str.strip().str.zfill(6)
    recs["pick_date"] = pd.to_datetime(recs["pick_date"]).dt.strftime("%Y-%m-%d")
    min_pick_date = str(recs["pick_date"].min())[:10]
    rec_codes = sorted(recs["stock_code"].dropna().unique().tolist())
    kline_placeholders = ", ".join(f":kcode_{idx}" for idx in range(len(rec_codes)))
    kline_params: dict[str, Any] = {
        "start_date": min_pick_date,
        "end_date": as_of_date,
        **{f"kcode_{idx}": code for idx, code in enumerate(rec_codes)},
    }
    kline = _read_frame(text(f"""
        SELECT stock_code, trade_date, close
        FROM sm_stock_kline
        WHERE k_type = 1
          AND adjust_type = 0
          AND trade_date >= :start_date
          AND trade_date <= :end_date
          AND stock_code IN ({kline_placeholders})
        ORDER BY stock_code, trade_date
    """), engine, params=kline_params)
    if kline.empty:
        return {"checked": len(recs), "updated": 0, "failure_samples": 0}

    kline["stock_code"] = kline["stock_code"].astype(str).str.strip().str.zfill(6)
    kline["trade_date"] = pd.to_datetime(kline["trade_date"]).dt.strftime("%Y-%m-%d")
    kline["close"] = pd.to_numeric(kline["close"], errors="coerce")
    by_code = {
        code: rows.sort_values("trade_date").to_dict(orient="records")
        for code, rows in kline.dropna(subset=["close"]).groupby("stock_code")
    }

    updates: list[dict[str, Any]] = []
    generated_failures: list[dict[str, Any]] = []
    for rec in recs.to_dict(orient="records"):
        code = str(rec.get("stock_code") or "").zfill(6)
        pick_date = str(rec.get("pick_date") or "")[:10]
        rows = [row for row in by_code.get(code, []) if str(row.get("trade_date") or "") >= pick_date]
        if not rows:
            continue
        base_close = _safe_number(rows[0].get("close"), 0.0)
        if base_close <= 0:
            continue

        returns: dict[str, float | None] = {}
        for window in (1, 3, 5, 10):
            key = f"review_{window}d_pct"
            if len(rows) > window:
                target_close = _safe_number(rows[window].get("close"), 0.0)
                returns[key] = round((target_close / base_close - 1.0) * 100.0, 2) if target_close > 0 else None
            else:
                returns[key] = None

        tags = set(_json_string_list(rec.get("failure_tags_json")))
        fail_tag = _pick_review_tag(returns)
        if fail_tag:
            tags.add(fail_tag)
            generated_failures.append({
                "stock_code": code,
                "short_name": str(rec.get("short_name") or "")[:40],
                "strategy_profile": str(rec.get("primary_strategy") or rec.get("strategy_profile") or "")[:20],
                "signal_date": pick_date,
                "result": "fail",
                "fail_tag": fail_tag,
                "fail_reason": (
                    f"推荐后收益回撤: 1d={returns['review_1d_pct']}, 3d={returns['review_3d_pct']}, "
                    f"5d={returns['review_5d_pct']}, 10d={returns['review_10d_pct']}"
                )[:500],
                "return_pct": next(
                    (returns[key] for key in ("review_10d_pct", "review_5d_pct", "review_3d_pct", "review_1d_pct") if returns[key] is not None),
                    None,
                ),
            })

        updates.append({
            "stock_code": code,
            "pick_date": pick_date,
            "review_1d_pct": returns["review_1d_pct"],
            "review_3d_pct": returns["review_3d_pct"],
            "review_5d_pct": returns["review_5d_pct"],
            "review_10d_pct": returns["review_10d_pct"],
            "failure_tags_json": json.dumps(sorted(tags), ensure_ascii=False),
        })

    if not updates:
        return {"checked": len(recs), "updated": 0, "failure_samples": 0}

    generated_tags = ("review_loss_1d", "review_loss_3d", "review_loss_5d", "review_loss_10d")
    with engine.begin() as conn:
        statement = text("""
            UPDATE st_recommended_stocks
            SET review_1d_pct = :review_1d_pct,
                review_3d_pct = :review_3d_pct,
                review_5d_pct = :review_5d_pct,
                review_10d_pct = :review_10d_pct,
                failure_tags_json = :failure_tags_json
            WHERE stock_code = :stock_code AND pick_date = :pick_date
        """)
        for row in updates:
            conn.execute(statement, row)

        tag_placeholders = ", ".join(f":generated_tag_{idx}" for idx in range(len(generated_tags)))
        delete_conditions = [
            "signal_date >= :min_pick_date",
            "signal_date <= :as_of_date",
            f"fail_tag IN ({tag_placeholders})",
        ]
        delete_params: dict[str, Any] = {
            "min_pick_date": min_pick_date,
            "as_of_date": as_of_date,
            **{f"generated_tag_{idx}": tag for idx, tag in enumerate(generated_tags)},
        }
        if codes:
            placeholders = ", ".join(f":dcode_{idx}" for idx in range(len(codes)))
            delete_conditions.append(f"stock_code IN ({placeholders})")
            delete_params.update({f"dcode_{idx}": code for idx, code in enumerate(codes)})
        conn.execute(text(f"""
            DELETE FROM st_ai_failure_samples
            WHERE {" AND ".join(delete_conditions)}
        """), delete_params)
        insert = text("""
            INSERT INTO st_ai_failure_samples (
                stock_code, short_name, strategy_profile, signal_date,
                result, fail_tag, fail_reason, return_pct, created_at
            ) VALUES (
                :stock_code, :short_name, :strategy_profile, :signal_date,
                :result, :fail_tag, :fail_reason, :return_pct, NOW()
            )
        """)
        for row in generated_failures:
            conn.execute(insert, row)

    return {"checked": len(recs), "updated": len(updates), "failure_samples": len(generated_failures)}


def _calibration_suggestion(row: dict[str, Any]) -> str:
    count = int(row.get("sample_count") or 0)
    if row.get("avg_return_5d") is None or row.get("win_rate_5d") is None:
        return "5日复盘样本不足，继续观察，不调整线上阈值"
    avg5 = _safe_number(row.get("avg_return_5d"), 0.0)
    win5 = _safe_number(row.get("win_rate_5d"), 0.0)
    avg10 = _safe_number(row.get("avg_return_10d"), 0.0)
    if count < 10:
        return "样本不足，继续观察，不调整线上阈值"
    if avg5 < -1.0 or win5 < 42.0:
        return "建议收紧：提高最低盈亏比/板块闸门，降低追高权重"
    if avg5 > 2.0 and win5 >= 55.0 and avg10 >= 1.0:
        return "建议保持或小幅放宽：当前阈值有效，优先扩大同类样本"
    if avg10 < avg5 - 2.0:
        return "建议缩短持有期或提前止盈：10日收益衰减明显"
    return "建议保持：表现中性，等待更多样本"


def calibrate_strategy_thresholds(
    engine: Engine,
    as_of_date: str,
    lookback_days: int = 90,
) -> dict[str, int]:
    """Persist review-based threshold suggestions for the runtime publisher."""
    as_of_date = str(as_of_date or "")[:10]
    if not as_of_date:
        return {"samples": 0, "calibrations": 0}
    _ensure_learning_tables(engine)
    if not _table_exists(engine, "st_recommended_stocks"):
        return {"samples": 0, "calibrations": 0}
    columns = _table_columns(engine, "st_recommended_stocks")
    if not {"review_5d_pct", "review_10d_pct"}.issubset(columns):
        return {"samples": 0, "calibrations": 0}

    strategy_expr = (
        "COALESCE(NULLIF(primary_strategy,''), NULLIF(strategy_profile,''), 'unknown')"
        if "primary_strategy" in columns
        else "COALESCE(NULLIF(strategy_profile,''), 'unknown')"
    )
    sector_expr = "COALESCE(NULLIF(sector_gate_status,''), 'WATCH')" if "sector_gate_status" in columns else "'WATCH'"
    rr_expr = "COALESCE(risk_reward_ratio, 0)" if "risk_reward_ratio" in columns else "0"
    recs = _read_frame(text(f"""
        SELECT stock_code, pick_date,
               {strategy_expr} AS strategy_key,
               {sector_expr} AS sector_key,
               {rr_expr} AS risk_reward_ratio,
               review_5d_pct, review_10d_pct
        FROM st_recommended_stocks
        WHERE pick_date <= :as_of_date
          AND pick_date >= DATE_SUB(:as_of_date, INTERVAL :lookback DAY)
          AND (review_5d_pct IS NOT NULL OR review_10d_pct IS NOT NULL)
    """), engine, params={"as_of_date": as_of_date, "lookback": max(1, int(lookback_days))})
    if recs.empty:
        return {"samples": 0, "calibrations": 0}

    recs["review_5d_pct"] = pd.to_numeric(recs["review_5d_pct"], errors="coerce")
    recs["review_10d_pct"] = pd.to_numeric(recs["review_10d_pct"], errors="coerce")
    recs["risk_reward_ratio"] = pd.to_numeric(recs["risk_reward_ratio"], errors="coerce").fillna(0.0)
    recs["risk_reward_bucket"] = pd.cut(
        recs["risk_reward_ratio"],
        bins=[-0.01, 2.999, 3.999, 9999.0],
        labels=["rr_lt_3", "rr_3_4", "rr_ge_4"],
    ).astype(str)

    scopes: list[tuple[str, str, pd.DataFrame]] = [("overall", "all", recs)]
    for key, rows in recs.groupby("strategy_key"):
        scopes.append(("strategy", str(key or "unknown"), rows))
    for key, rows in recs.groupby("sector_key"):
        scopes.append(("sector_gate", str(key or "WATCH"), rows))
    for key, rows in recs.groupby("risk_reward_bucket"):
        scopes.append(("risk_reward", str(key or "unknown"), rows))

    calibration_rows: list[dict[str, Any]] = []
    for scope_type, scope_key, rows in scopes:
        avg5 = rows["review_5d_pct"].mean(skipna=True)
        avg10 = rows["review_10d_pct"].mean(skipna=True)
        win5 = (rows["review_5d_pct"] > 0).mean(skipna=True) * 100.0 if rows["review_5d_pct"].notna().any() else np.nan
        win10 = (rows["review_10d_pct"] > 0).mean(skipna=True) * 100.0 if rows["review_10d_pct"].notna().any() else np.nan
        item = {
            "calibration_date": as_of_date,
            "window_days": int(lookback_days),
            "scope_type": scope_type,
            "scope_key": scope_key[:80],
            "sample_count": int(len(rows)),
            "avg_return_5d": round(float(avg5), 4) if not pd.isna(avg5) else None,
            "win_rate_5d": round(float(win5), 4) if not pd.isna(win5) else None,
            "avg_return_10d": round(float(avg10), 4) if not pd.isna(avg10) else None,
            "win_rate_10d": round(float(win10), 4) if not pd.isna(win10) else None,
        }
        item["suggestion"] = _calibration_suggestion(item)
        calibration_rows.append(item)

    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM st_strategy_threshold_calibration
            WHERE calibration_date = :calibration_date AND window_days = :window_days
        """), {"calibration_date": as_of_date, "window_days": int(lookback_days)})
        insert = text("""
            INSERT INTO st_strategy_threshold_calibration (
                calibration_date, window_days, scope_type, scope_key, sample_count,
                avg_return_5d, win_rate_5d, avg_return_10d, win_rate_10d, suggestion, created_at
            ) VALUES (
                :calibration_date, :window_days, :scope_type, :scope_key, :sample_count,
                :avg_return_5d, :win_rate_5d, :avg_return_10d, :win_rate_10d, :suggestion, NOW()
            )
        """)
        for row in calibration_rows:
            conn.execute(insert, row)
    return {"samples": int(len(recs)), "calibrations": len(calibration_rows)}


def publish_strategy_runtime_params(
    engine: Engine,
    as_of_date: str,
    stable_days: int = 3,
    min_sample_count: int = 30,
) -> dict[str, Any]:
    """Publish stable calibration suggestions into runtime parameters."""
    as_of_date = str(as_of_date or "")[:10]
    if not as_of_date:
        return {"published": 0, "direction": "hold"}
    stable_days = max(1, int(stable_days or 3))
    min_sample_count = max(1, int(min_sample_count or 30))
    _ensure_learning_tables(engine)
    if not _table_exists(engine, "st_strategy_threshold_calibration"):
        return {"published": 0, "direction": "hold"}

    calibrations = _read_frame(text(f"""
        SELECT calibration_date, sample_count, avg_return_5d, win_rate_5d, suggestion
        FROM st_strategy_threshold_calibration
        WHERE scope_type = 'overall'
          AND scope_key = 'all'
          AND calibration_date <= :as_of_date
        ORDER BY calibration_date DESC
        LIMIT {stable_days}
    """), engine, params={"as_of_date": as_of_date})
    if len(calibrations) < stable_days:
        return {"published": 0, "direction": "hold", "reason": "stable_sample_not_enough"}
    calibrations["sample_count"] = pd.to_numeric(calibrations["sample_count"], errors="coerce").fillna(0)
    if bool((calibrations["sample_count"] < min_sample_count).any()):
        return {"published": 0, "direction": "hold", "reason": "sample_count_not_enough"}

    suggestions = [str(value or "") for value in calibrations["suggestion"].tolist()]
    if all("收紧" in item for item in suggestions):
        direction = "tighten"
    elif all("放宽" in item for item in suggestions):
        direction = "loosen"
    else:
        return {"published": 0, "direction": "hold", "reason": "suggestion_not_stable"}

    current = load_strategy_runtime_params(engine, as_of_date)
    if direction == "tighten":
        updates = {
            "min_risk_reward": min(4.5, max(current["min_risk_reward"], DEFAULT_RUNTIME_PARAMS["min_risk_reward"]) + 0.25),
            "min_sector_flow_amount_3d": min(1_000_000_000.0, current["min_sector_flow_amount_3d"] + 50_000_000.0),
            "min_sector_rotation_score": min(70.0, current["min_sector_rotation_score"] + 5.0),
        }
    else:
        updates = {
            "min_risk_reward": max(3.0, current["min_risk_reward"] - 0.25),
            "min_sector_flow_amount_3d": max(400_000_000.0, current["min_sector_flow_amount_3d"] - 50_000_000.0),
            "min_sector_rotation_score": max(45.0, current["min_sector_rotation_score"] - 3.0),
        }

    metadata = {
        "direction": direction,
        "stable_days": stable_days,
        "min_sample_count": min_sample_count,
        "calibration_dates": [str(value)[:10] for value in calibrations["calibration_date"].tolist()],
        "suggestions": suggestions,
    }
    upsert = text("""
        INSERT INTO st_strategy_runtime_params (
            param_key, param_value, value_type, source, effective_date,
            status, metadata_json, created_at, updated_at
        ) VALUES (
            :param_key, :param_value, 'float', 'auto_calibration', :effective_date,
            'active', :metadata_json, NOW(), NOW()
        )
        ON DUPLICATE KEY UPDATE
            param_value = VALUES(param_value),
            value_type = VALUES(value_type),
            source = VALUES(source),
            effective_date = VALUES(effective_date),
            status = VALUES(status),
            metadata_json = VALUES(metadata_json),
            updated_at = NOW()
    """)
    rows = [
        {
            "param_key": key,
            "param_value": round(float(value), 4),
            "effective_date": as_of_date,
            "metadata_json": json.dumps(metadata, ensure_ascii=False),
        }
        for key, value in updates.items()
    ]
    with engine.begin() as conn:
        for row in rows:
            conn.execute(upsert, row)
    return {"published": len(rows), "direction": direction, "params": updates}


def _emit_progress(progress_callback: ProgressCallback | None, **payload: Any) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(payload)
    except Exception:
        logger.debug("progress callback failed", exc_info=True)


def _activate_runtime_params(engine: Engine, trade_date: str, progress_callback: ProgressCallback | None = None) -> None:
    # st_strategy_runtime_params is a mutable current-state table keyed only by
    # param_key.  effective_date cannot reconstruct the value that was actually
    # knowable before a later in-place update, so historical/exact-cutoff runs
    # must use immutable code defaults until versioned config rows are stored.
    active = set_active_runtime_params({})
    _emit_progress(
        progress_callback,
        stage="runtime_params_neutralized",
        percent=3,
        step="unversioned strategy runtime params neutralized",
        trade_date=trade_date,
        reason=LEGACY_PIT_DISABLED_FACTOR_REASON,
        params=active,
    )


def _publish_runtime_params_after_calibration(
    engine: Engine,
    trade_date: str,
    progress_callback: ProgressCallback | None = None,
) -> None:
    try:
        publish_stats = publish_strategy_runtime_params(engine, trade_date)
        if int(publish_stats.get("published") or 0) > 0:
            set_active_runtime_params(load_strategy_runtime_params(engine, trade_date))
        _emit_progress(
            progress_callback,
            stage="runtime_param_publish",
            percent=99,
            step="strategy runtime params published",
            trade_date=trade_date,
            **publish_stats,
        )
    except Exception as exc:
        logger.warning("Strategy runtime param publish failed for %s: %s", trade_date, exc)
        _emit_progress(
            progress_callback,
            stage="runtime_param_publish_failed",
            percent=99,
            step="strategy runtime param publish failed",
            trade_date=trade_date,
            error=str(exc),
        )


def _prepare_batch_outputs(
    engine: Engine,
    trade_date: str,
    min_score: float,
    top_n: int,
    stock_codes: list[str] | None = None,
    progress_callback: ProgressCallback | None = None,
    news_cutoff_time: str | None = None,
    use_intraday_current: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float, str, str]:
    scoped_codes = _normalize_stock_codes(stock_codes)
    knowledge_cutoff = _normalize_chase_as_of(
        news_cutoff_time if news_cutoff_time is not None else trade_date,
        allow_naive_local=news_cutoff_time is not None,
    )

    _emit_progress(progress_callback, stage="load_kline", percent=5, step="鍔犺浇鏃鐗瑰緛...", trade_date=trade_date)
    kline_all = load_kline_features(
        engine,
        trade_date,
        use_intraday_current=bool(use_intraday_current),
        progress_callback=progress_callback,
        # Reuse the exact execution/news cutoff when one was supplied.  A
        # normal EOD run intentionally resolves to trade-date end-of-day.
        as_of_at=knowledge_cutoff,
    )
    event_source_health = load_event_source_health(
        engine,
        trade_date,
        as_of_at=knowledge_cutoff,
    )
    kline_all = _apply_event_source_health_gate(kline_all, event_source_health)
    _assert_chase_risk_coverage(kline_all, trade_date)
    kline = _filter_frame_by_codes(kline_all, scoped_codes)
    if scoped_codes and kline.empty:
        raise RuntimeError(f"No K-line rows found for requested stock codes on {trade_date}")

    _emit_progress(progress_callback, stage="load_finance", percent=14, step="鍔犺浇璐㈠姟鍥犲瓙...", trade_date=trade_date)
    finance = _filter_frame_by_codes(
        load_finance(engine, trade_date, as_of_at=knowledge_cutoff), scoped_codes
    )
    neutral_stock_context = pd.DataFrame({"stock_code": []})
    dividend_context = neutral_stock_context.copy()
    research_context = neutral_stock_context.copy()
    business_context = neutral_stock_context.copy()
    institutional_context = neutral_stock_context.copy()
    prosperity_context = neutral_stock_context.copy()
    interaction_context = neutral_stock_context.copy()
    _emit_progress(progress_callback, stage="load_flow", percent=23, step="鍔犺浇璧勯噾娴佹暟鎹?..", trade_date=trade_date)
    flow, flow_date = load_flow_features(
        engine, trade_date, as_of_at=knowledge_cutoff
    )
    flow = _filter_frame_by_codes(flow, scoped_codes)
    _emit_progress(progress_callback, stage="load_hot", percent=32, step="鍔犺浇鐑害鎺掕...", trade_date=trade_date)
    hot, hot_date = load_hot_rank(
        engine, trade_date, as_of_at=knowledge_cutoff
    )
    hot = _filter_frame_by_codes(hot, scoped_codes)
    _emit_progress(progress_callback, stage="load_notices", percent=40, step="鍔犺浇鍏憡浜嬩欢...", trade_date=trade_date)
    notices = _filter_frame_by_codes(
        load_notice_features(engine, trade_date, as_of_at=knowledge_cutoff), scoped_codes
    )
    news = _filter_frame_by_codes(
        load_news_features(
            engine,
            trade_date,
            cutoff_time=knowledge_cutoff.isoformat(),
        ),
        scoped_codes,
    )
    notices = merge_event_features(notices, news)
    _emit_progress(progress_callback, stage="load_event_relations", percent=44, step="加载事件产业链关系...", trade_date=trade_date)
    event_relation_rules: list[dict[str, Any]] = []
    _emit_progress(
        progress_callback,
        stage="pit_optional_factors_neutralized",
        percent=44,
        step="unversioned optional factors neutralized",
        trade_date=trade_date,
        disabled_factor_inventory=list(LEGACY_PIT_DISABLED_FACTOR_INVENTORY),
        reason=LEGACY_PIT_DISABLED_FACTOR_REASON,
    )
    _emit_progress(progress_callback, stage="load_confidence", percent=48, step="鍔犺浇浜ゆ槗缃俊搴?..", trade_date=trade_date)
    confidence = neutral_stock_context.copy()
    _emit_progress(progress_callback, stage="load_history", percent=56, step="鍔犺浇鍘嗗彶鎺ㄨ崘琛ㄧ幇...", trade_date=trade_date)
    rec_history = neutral_stock_context.copy()
    _emit_progress(progress_callback, stage="load_failures", percent=62, step="鍔犺浇澶辫触鎯╃綒鍥犲瓙...", trade_date=trade_date)
    failures = neutral_stock_context.copy()
    _emit_progress(progress_callback, stage="load_chip_capital", percent=65, step="加载筹码与两融上下文...", trade_date=trade_date)
    chip_context = neutral_stock_context.copy()
    north_stock_context = neutral_stock_context.copy()
    _emit_progress(progress_callback, stage="load_sector", percent=68, step="鍔犺浇鏉垮潡杞姩鍥犲瓙...", trade_date=trade_date)
    sector = _filter_frame_by_codes(
        load_sector_rotation_features(engine, trade_date, as_of_at=knowledge_cutoff), scoped_codes
    )
    _emit_progress(progress_callback, stage="load_price_crosscheck", percent=72, step="加载价格双源校验...", trade_date=trade_date)
    price_validation = _filter_frame_by_codes(
        load_price_validation_features(engine, trade_date, as_of_at=knowledge_cutoff), scoped_codes
    )
    size_context = neutral_stock_context.copy()
    order_book_context = _filter_frame_by_codes(
        load_order_book_features(engine, trade_date, as_of_at=knowledge_cutoff), scoped_codes
    )
    market_mood_score = compute_market_mood(kline_all)
    market_breadth = compute_market_breadth_features(kline_all)
    market_context = {
        **load_macro_policy_context(
            engine,
            trade_date,
            cutoff_time=knowledge_cutoff.isoformat(),
        ),
        **load_latest_external_market_context(
            engine,
            as_of=knowledge_cutoff.to_pydatetime(),
        ),
    }

    logger.info(
        "Loaded data: scope=%s kline=%s finance=%s dividend=%s research=%s business=%s institutional=%s prosperity=%s interaction=%s flow=%s notices=%s hot=%s confidence=%s history=%s failures=%s chip_context=%s north_stock=%s size_context=%s order_book=%s sector=%s price_validation=%s market_mood=%.1f breadth=%s style=%s north=%s etf=%s retail=%s macro=%s macro_indicator=%s cycle=%s external=%s external_captured_at=%s",
        "all" if not scoped_codes else len(scoped_codes),
        len(kline), len(finance), len(dividend_context), len(research_context), len(business_context), len(institutional_context), len(prosperity_context), len(interaction_context), len(flow), len(notices), len(hot),
        len(confidence), len(rec_history), len(failures), len(chip_context), len(north_stock_context), len(size_context), len(order_book_context), len(sector), len(price_validation),
        market_mood_score, market_breadth.get("market_extreme_status"), market_context.get("market_style"),
        market_context.get("north_flow_status"), market_context.get("etf_flow_status"), market_context.get("retail_sentiment_status"), market_context.get("macro_policy_status"), market_context.get("macro_indicator_status"), market_context.get("macro_cycle"), market_context.get("external_market_status"), market_context.get("external_market_captured_at"),
    )

    _emit_progress(progress_callback, stage="compute_scores", percent=80, step="璁＄畻鍏ㄥ競鍦鸿瘎鍒?..", trade_date=trade_date)
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
        price_validation=price_validation,
        confidence=confidence,
        rec_history=rec_history,
        failures=failures,
        chip_context=chip_context,
        size_context=size_context,
        order_book_context=order_book_context,
        dividend_context=dividend_context,
        research_context=research_context,
        north_stock_context=north_stock_context,
        institutional_context=institutional_context,
        prosperity_context=prosperity_context,
        business_context=business_context,
        interaction_context=interaction_context,
        market_breadth=market_breadth,
        market_context=market_context,
        event_relation_rules=event_relation_rules,
    )
    scored["disabled_factor_inventory"] = json.dumps(
        list(LEGACY_PIT_DISABLED_FACTOR_INVENTORY),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    scored["disabled_factor_reason"] = LEGACY_PIT_DISABLED_FACTOR_REASON
    scored["decision_knowledge_cutoff"] = knowledge_cutoff.isoformat()
    scored["flow_trade_date"] = flow_date
    scored["hot_trade_date"] = hot_date
    _emit_progress(progress_callback, stage="build_rows", percent=88, step="鐢熸垚鍒嗘瀽涓庢帹鑽愮粨鏋?..", trade_date=trade_date)
    scored = _build_text_fields(scored, flow_date=flow_date, trade_date=trade_date)
    analysis_rows = build_analysis_rows(scored, trade_date)
    rec_rows = build_recommendation_rows(scored, trade_date, top_n=top_n, min_score=min_score)
    _emit_progress(progress_callback, stage="minute_chan", percent=91, step="补充分时缠论证据...", trade_date=trade_date)
    _emit_progress(
        progress_callback,
        stage="minute_chan_neutralized",
        percent=91,
        step="unversioned minute Chan evidence neutralized",
        trade_date=trade_date,
        reason=LEGACY_PIT_DISABLED_FACTOR_REASON,
    )
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
    use_intraday_current: bool = False,
) -> BatchStats:
    _ensure_output_schema(engine)
    if use_intraday_current and not execution_time:
        execution_time = datetime.now(CHINA_MARKET_TIMEZONE).replace(
            microsecond=0
        ).isoformat(sep=" ")
    if strict_prev_trade_day:
        execution_time = execution_time or datetime.now(CHINA_MARKET_TIMEZONE).replace(
            microsecond=0
        ).isoformat(sep=" ")
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
    _activate_runtime_params(engine, trade_date, progress_callback)

    analysis_rows, rec_rows, market_mood_score, flow_date, hot_date = _prepare_batch_outputs(
        engine=engine,
        trade_date=trade_date,
        min_score=min_score,
        top_n=top_n,
        stock_codes=None,
        progress_callback=progress_callback,
        news_cutoff_time=execution_time if (strict_prev_trade_day or use_intraday_current) else None,
        use_intraday_current=bool(use_intraday_current),
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
    try:
        review_stats = backfill_recommendation_reviews(engine, trade_date)
        _emit_progress(
            progress_callback,
            stage="review_backfill",
            percent=97,
            step="recommendation review backfilled",
            trade_date=trade_date,
            **review_stats,
        )
    except Exception as exc:
        logger.warning("Recommendation review backfill failed for %s: %s", trade_date, exc)
        _emit_progress(
            progress_callback,
            stage="review_backfill_failed",
            percent=97,
            step="recommendation review backfill failed",
            trade_date=trade_date,
            error=str(exc),
        )
    try:
        calibration_stats = calibrate_strategy_thresholds(engine, trade_date)
        _emit_progress(
            progress_callback,
            stage="threshold_calibration",
            percent=98,
            step="strategy threshold calibration updated",
            trade_date=trade_date,
            **calibration_stats,
        )
    except Exception as exc:
        logger.warning("Strategy threshold calibration failed for %s: %s", trade_date, exc)
        _emit_progress(
            progress_callback,
            stage="threshold_calibration_failed",
            percent=98,
            step="strategy threshold calibration failed",
            trade_date=trade_date,
            error=str(exc),
        )
    _publish_runtime_params_after_calibration(engine, trade_date, progress_callback)

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
    use_intraday_current: bool = False,
    execution_time: str | None = None,
) -> BatchStats:
    scoped_codes = _normalize_stock_codes(stock_codes)
    if not scoped_codes:
        raise ValueError("stock_codes must not be empty")

    _ensure_output_schema(engine)
    trade_date = trade_date or latest_trade_date(engine)
    if use_intraday_current and not execution_time:
        execution_time = datetime.now(CHINA_MARKET_TIMEZONE).replace(
            microsecond=0
        ).isoformat(sep=" ")
    logger.info("Fast scoped analysis started for %s with %s codes", trade_date, len(scoped_codes))
    _activate_runtime_params(engine, trade_date, progress_callback)
    analysis_rows, rec_rows, market_mood_score, flow_date, hot_date = _prepare_batch_outputs(
        engine=engine,
        trade_date=trade_date,
        min_score=min_score,
        top_n=max(int(top_n), len(scoped_codes)),
        stock_codes=scoped_codes,
        progress_callback=progress_callback,
        news_cutoff_time=execution_time if use_intraday_current else None,
        use_intraday_current=bool(use_intraday_current),
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
    try:
        review_stats = backfill_recommendation_reviews(engine, trade_date, stock_codes=scoped_codes)
        _emit_progress(
            progress_callback,
            stage="review_backfill",
            percent=97,
            step="scoped recommendation review backfilled",
            trade_date=trade_date,
            **review_stats,
        )
    except Exception as exc:
        logger.warning("Scoped recommendation review backfill failed for %s: %s", trade_date, exc)
        _emit_progress(
            progress_callback,
            stage="review_backfill_failed",
            percent=97,
            step="scoped recommendation review backfill failed",
            trade_date=trade_date,
            error=str(exc),
        )
    try:
        calibration_stats = calibrate_strategy_thresholds(engine, trade_date)
        _emit_progress(
            progress_callback,
            stage="threshold_calibration",
            percent=98,
            step="strategy threshold calibration updated",
            trade_date=trade_date,
            **calibration_stats,
        )
    except Exception as exc:
        logger.warning("Strategy threshold calibration failed for %s: %s", trade_date, exc)
        _emit_progress(
            progress_callback,
            stage="threshold_calibration_failed",
            percent=98,
            step="strategy threshold calibration failed",
            trade_date=trade_date,
            error=str(exc),
        )
    _publish_runtime_params_after_calibration(engine, trade_date, progress_callback)

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
    parser.add_argument("--use-intraday-current", action="store_true", help="Build target-day features from intraday current quote snapshots")
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
        use_intraday_current=args.use_intraday_current,
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
