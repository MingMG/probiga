from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Callable

from .domain import RawSignal
from .right_side_policy import right_side_setup_ready
from .structural_mainline import weak_market_structural_mainline


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _scaled(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return _clamp((value - lower) / (upper - lower))


def _band(
    value: float,
    lower: float,
    upper: float,
    shoulder: float,
) -> float:
    if lower <= value <= upper:
        return 1.0
    if value < lower:
        return _clamp((value - (lower - shoulder)) / shoulder)
    return _clamp(((upper + shoulder) - value) / shoulder)


def _value(features: dict[str, Any], key: str) -> float:
    return float(features.get(key) or 0.0)


def _theme_label(features: dict[str, Any]) -> str:
    """Use the human-readable theme while retaining code fallback."""

    return str(
        features.get("theme_name")
        or features.get("theme_code")
        or ""
    )


def _feature_snapshot(features: dict[str, Any]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    for key, value in features.items():
        if isinstance(value, (int, float)):
            number = float(value)
            if math.isfinite(number):
                snapshot[key] = number
        elif key in {
            "theme_code",
            "theme_name",
            "theme_source",
            "theme_feature_key",
            "theme_news_novelty_status",
            "theme_news_novelty_match_key",
            "industry_code",
            "industry_name",
        } and isinstance(value, str):
            snapshot[key] = value
        elif key in {
            "theme_codes",
            "theme_names",
            "theme_cluster_keys",
            "theme_cluster_labels",
            "all_theme_cluster_keys",
            "all_theme_cluster_labels",
            "paper_research_groups",
            "finance_missing_fields",
        } and isinstance(value, (list, tuple)):
            snapshot[key] = [
                str(item)
                for item in value
                if str(item)
            ]
    return snapshot


def _signal(
    *,
    stock_code: str,
    stock_name: str,
    strategy_key: str,
    horizon_days: int,
    score: float,
    feature_time: datetime,
    valid_until: datetime,
    initial_stop_pct: float,
    theme_code: str,
    reasons: list[str],
    features: dict[str, Any],
    required: tuple[str, ...],
) -> RawSignal:
    missing = tuple(key for key in required if features.get(key) is None)
    # A forecast horizon describes how long the outcome is measured, not how
    # long an old signal may be reused.  Bound the caller's broad safety cap by
    # a sleeve-specific calendar window so one-day intraday signals cannot
    # remain actionable for a month.
    horizon_valid_until = feature_time + timedelta(
        days=max(3, min(30, int(horizon_days * 1.6) + 2))
    )
    return RawSignal(
        stock_code=stock_code,
        stock_name=stock_name,
        strategy_key=strategy_key,
        horizon_days=horizon_days,
        score=round(_clamp(score), 8),
        feature_time=feature_time,
        valid_until=min(valid_until, horizon_valid_until),
        initial_stop_pct=initial_stop_pct,
        theme_code=theme_code,
        status="INSUFFICIENT_DATA" if missing else "SCORED",
        reasons=(
            ("缺少特征：" + "、".join(missing),)
            if missing
            else tuple(reasons)
        ),
        features=_feature_snapshot(features),
    )


def theme_diffusion(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    breadth = _value(features, "sector_breadth_pct")
    acceleration = _value(features, "sector_breadth_acceleration_pct")
    relative = _value(features, "sector_relative_return_pct")
    amount_accel = _value(features, "sector_amount_acceleration_pct")
    leadership = _value(features, "leadership_quality")
    opportunity = _value(features, "theme_opportunity_score")
    distance = _value(features, "distance_ma20_pct")
    latest_change = _value(features, "latest_change_pct")
    return_5d = _value(features, "return_5d_pct")
    amount_ratio = _value(features, "amount_ratio_5_20")
    catalyst = _value(features, "event_surprise")
    news_theme = _value(features, "news_theme_context_score")
    market_news_risk = _value(
        features,
        "market_news_risk_score",
    )
    crowding = _value(features, "sector_crowding")
    market_return = _value(features, "market_return_20d_pct")
    aligned = bool(
        _value(features, "close_above_ma20") >= 1
        and _value(features, "ma20_above_ma60") >= 1
    )
    entry_quality = (
        0.32 * _band(distance, -2.0, 8.0, 8.0)
        + 0.24 * _band(latest_change, -1.0, 6.0, 5.0)
        + 0.22 * _band(return_5d, 0.0, 15.0, 12.0)
        + 0.22 * _band(amount_ratio, 0.8, 2.5, 1.5)
    )
    score = (
        0.19 * _scaled(breadth, 45, 78)
        + 0.17 * _scaled(acceleration, 0, 22)
        + 0.14 * _scaled(relative, 0, 5)
        + 0.11 * _scaled(amount_accel, 0, 65)
        + 0.16 * _clamp(leadership)
        + 0.08 * _clamp(opportunity)
        + 0.05 * _clamp(catalyst)
        + 0.10 * entry_quality
        + 0.05 * max(0.0, news_theme)
        - 0.07 * max(0.0, -news_theme)
        - 0.04 * _clamp(market_news_risk)
        - 0.12 * _scaled(breadth, 85, 100)
        - 0.10 * _scaled(crowding, 0.78, 1.0)
    )
    signal = _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="theme_diffusion",
        horizon_days=5,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=-5.0,
        theme_code=_theme_label(features),
        reasons=[
            f"板块上涨宽度{breadth:.1f}%",
            f"宽度变化{acceleration:.1f}个百分点",
            f"板块相对收益{relative:.2f}%",
            f"个股板块领导力{leadership:.1%}",
            f"入场质量{entry_quality:.1%}",
            f"消息面对所属主题的结构化影响{news_theme:+.2f}",
        ],
        features=features,
        required=(
            "sector_breadth_pct",
            "sector_breadth_acceleration_pct",
            "sector_relative_return_pct",
            "sector_amount_acceleration_pct",
            "leadership_quality",
            "distance_ma20_pct",
            "latest_change_pct",
            "return_5d_pct",
            "amount_ratio_5_20",
            "close_above_ma20",
            "ma20_above_ma60",
            "market_return_20d_pct",
        ),
    )
    sector_ready = bool(
        50.0 <= breadth <= 88.0
        and relative >= 0.5
        and (acceleration >= 1.0 or amount_accel >= 8.0)
        and leadership >= 0.35
    )
    entry_ready = bool(
        -1.0 <= distance <= 8.0
        and -2.0 <= latest_change <= 6.5
        and -2.0 <= return_5d <= 15.0
        and 0.8 <= amount_ratio <= 2.5
    )
    if (
        signal.status == "SCORED"
        and not (aligned and sector_ready and entry_ready)
    ):
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": "SETUP_NOT_READY",
                "reasons": signal.reasons + (
                    "板块扩散、个股趋势或非追高入场条件尚未同时成立",
                ),
            }
        )
    if signal.status == "SCORED" and market_return < 0.0:
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": "WEAK_MARKET_THEME_WATCH",
                "reasons": signal.reasons + (
                    "弱市中的结构性板块机会，仅进入观察池并积累前向证据",
                ),
            }
        )
    return signal


def low_base_ignition(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    """Detect sector capital ignition before a stock becomes a mature trend.

    This sleeve is intentionally independent from ``right_side_trend``.  It
    accepts a weak broad market and a stock below its moving averages only
    when the stock starts strengthening at the same time as its sector's
    breadth, relative return and turnover accelerate.  A raw score is still
    research-only until this exact formula has its own positive OOS
    calibration.
    """

    breadth = _value(features, "sector_breadth_pct")
    breadth_accel = _value(
        features,
        "sector_breadth_acceleration_pct",
    )
    sector_relative = _value(
        features,
        "sector_relative_return_pct",
    )
    amount_accel = _value(
        features,
        "sector_amount_acceleration_pct",
    )
    opportunity = _value(features, "theme_opportunity_score")
    crowding = _value(features, "sector_crowding")
    ret20 = _value(features, "return_20d_pct")
    ret60 = _value(features, "return_60d_pct")
    ret5 = _value(features, "return_5d_pct")
    distance = _value(features, "distance_ma20_pct")
    latest_change = _value(features, "latest_change_pct")
    amount_ratio = _value(features, "amount_ratio_5_20")
    latest_amount = _value(features, "latest_amount")
    average_amount = _value(features, "average_amount_20d")
    latest_amount_ratio = (
        latest_amount / average_amount
        if latest_amount > 0.0 and average_amount > 0.0
        else 0.0
    )
    atr = _value(features, "atr_14d_pct")
    breakout = _value(features, "breakout_20d_proximity")
    leadership = _value(features, "stock_leadership_score")
    relative_to_theme = _value(
        features,
        "stock_relative_to_theme_5d_pct",
    )
    news_theme = _value(features, "news_theme_context_score")
    market_news_risk = _value(
        features,
        "market_news_risk_score",
    )

    sector_ignition = (
        0.25 * _scaled(breadth, 45.0, 78.0)
        + 0.23 * _scaled(breadth_accel, 5.0, 25.0)
        + 0.20 * _scaled(sector_relative, 1.0, 8.0)
        + 0.20 * _scaled(amount_accel, 10.0, 80.0)
        + 0.12 * _clamp(opportunity)
    )
    stock_turn = (
        0.12 * _band(ret20, -35.0, 3.0, 16.0)
        + 0.08 * _band(ret60, -30.0, 10.0, 22.0)
        + 0.11 * _band(ret5, -4.0, 1.5, 4.0)
        + 0.15 * _band(distance, -12.0, 2.0, 8.0)
        + 0.15 * _band(latest_change, 1.5, 5.5, 3.5)
        + 0.16 * _band(latest_amount_ratio, 1.5, 3.5, 1.2)
        + 0.07 * _band(amount_ratio, 0.70, 1.80, 1.0)
        + 0.06 * _band(atr, 2.0, 7.0, 3.0)
        + 0.06 * _band(breakout, 0.62, 0.93, 0.25)
        + 0.04 * _band(relative_to_theme, -8.0, 0.5, 6.0)
    )
    score = (
        0.58 * sector_ignition
        + 0.36 * stock_turn
        + 0.06 * _clamp(leadership)
        + 0.06 * max(0.0, news_theme)
        - 0.08 * max(0.0, -news_theme)
        - 0.04 * _clamp(market_news_risk)
        - 0.12 * _scaled(breadth, 88.0, 100.0)
        - 0.12 * _scaled(crowding, 0.80, 1.0)
    )
    signal = _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="low_base_ignition",
        horizon_days=5,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=-max(4.0, min(8.0, atr * 1.35)),
        theme_code=_theme_label(features),
        reasons=[
            (
                f"板块上涨宽度{breadth:.1f}%，"
                f"较5日前加速{breadth_accel:.1f}个百分点"
            ),
            (
                f"板块相对大盘强{sector_relative:.2f}%，"
                f"成交额加速{amount_accel:.1f}%"
            ),
            (
                f"个股20日涨幅{ret20:.2f}%，"
                f"距20日线{distance:.2f}%"
            ),
            (
                f"当日转强{latest_change:.2f}%，"
                f"当日成交额/20日均额{latest_amount_ratio:.2f}，"
                f"接近20日高点程度{breakout:.1%}"
            ),
            "板块资金先动、个股低位转强，属于预判型候选",
            f"消息面对所属主题的结构化影响{news_theme:+.2f}",
        ],
        features=features,
        required=(
            "sector_breadth_pct",
            "sector_breadth_acceleration_pct",
            "sector_relative_return_pct",
            "sector_amount_acceleration_pct",
            "theme_opportunity_score",
            "sector_crowding",
            "return_5d_pct",
            "return_20d_pct",
            "return_60d_pct",
            "distance_ma20_pct",
            "latest_change_pct",
            "amount_ratio_5_20",
            "latest_amount",
            "average_amount_20d",
            "atr_14d_pct",
            "breakout_20d_proximity",
            "stock_leadership_score",
            "stock_relative_to_theme_5d_pct",
        ),
    )
    setup_ready = bool(
        # This is a pre-diffusion entry.  Once four fifths of a sector are
        # already rising it belongs to the continuation sleeve instead.
        50.0 <= breadth <= 78.0
        and breadth_accel >= 10.0
        and sector_relative >= 3.0
        and amount_accel >= 25.0
        and opportunity >= 0.65
        and crowding <= 0.82
        and -4.0 <= ret5 <= 1.5
        and -35.0 <= ret20 <= 5.0
        and -30.0 <= ret60 <= 12.0
        and -12.0 <= distance <= 2.0
        and 1.5 <= latest_change <= 5.5
        and 1.50 <= latest_amount_ratio <= 4.0
        and 0.70 <= amount_ratio <= 1.80
        and 2.0 <= atr <= 7.0
        and 0.62 <= breakout <= 0.93
        and -8.0 <= relative_to_theme <= 0.5
    )
    if signal.status == "SCORED" and not setup_ready:
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": "SETUP_NOT_READY",
                "reasons": signal.reasons + (
                    "板块点火和个股低位转强条件尚未同时成立",
                ),
            }
        )
    return signal


def right_side_trend(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    ret20 = _value(features, "return_20d_pct")
    ret60 = _value(features, "return_60d_pct")
    slope = _value(features, "ma20_slope_5d_pct")
    breakout = _value(features, "breakout_20d_proximity")
    volume = _value(features, "amount_ratio_5_20")
    relative = _value(features, "relative_strength_20d_pct")
    extension = max(0.0, _value(features, "distance_ma20_pct") - 12.0)
    trend_alignment = (
        1.0
        if (
            _value(features, "close_above_ma20") >= 1
            and _value(features, "ma20_above_ma60") >= 1
        )
        else 0.0
    )
    score = (
        0.22 * trend_alignment
        + 0.17 * _scaled(ret20, 3, 30)
        + 0.11 * _scaled(ret60, 8, 65)
        + 0.15 * _scaled(slope, 0, 8)
        + 0.13 * _clamp(breakout)
        + 0.10 * _scaled(volume, 0.8, 2.2)
        + 0.12 * _scaled(relative, 0, 20)
        - 0.18 * _scaled(extension, 0, 25)
    )
    stop = -max(
        3.5,
        min(8.0, _value(features, "atr_14d_pct") * 2.2),
    )
    return _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="right_side_trend",
        horizon_days=10,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=stop,
        theme_code=_theme_label(features),
        reasons=[
            f"20日涨幅{ret20:.2f}%",
            f"60日涨幅{ret60:.2f}%",
            f"20日均线5日斜率{slope:.2f}%",
            f"相对强度{relative:.2f}%",
        ],
        features=features,
        required=(
            "return_20d_pct",
            "return_60d_pct",
            "ma20_slope_5d_pct",
            "breakout_20d_proximity",
            "amount_ratio_5_20",
            "relative_strength_20d_pct",
            "close_above_ma20",
            "ma20_above_ma60",
            "atr_14d_pct",
        ),
    )


def event_drift(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    surprise = _value(features, "event_surprise")
    novelty = _value(features, "event_novelty")
    reliability = _value(features, "event_source_reliability")
    confirmation = _value(features, "event_price_confirmation")
    priced_in = _value(features, "event_priced_in")
    decay = _value(features, "event_decay")
    score = (
        0.30 * _clamp(surprise)
        + 0.20 * _clamp(novelty)
        + 0.15 * _clamp(reliability)
        + 0.20 * _clamp(confirmation)
        + 0.15 * (1.0 - _clamp(decay))
        - 0.25 * _clamp(priced_in)
    )
    return _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="event_drift",
        horizon_days=10,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=-6.0,
        theme_code=_theme_label(features),
        reasons=[
            f"事件惊喜度{surprise:.2f}",
            f"首次性{novelty:.2f}",
            f"价格确认{confirmation:.2f}",
        ],
        features=features,
        required=(
            "event_surprise",
            "event_novelty",
            "event_source_reliability",
            "event_price_confirmation",
            "event_priced_in",
            "event_decay",
        ),
    )


def quality_momentum(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    quality = _value(features, "quality_percentile")
    growth = _value(features, "growth_percentile")
    cashflow = _value(features, "cashflow_quality_percentile")
    valuation = _value(features, "valuation_percentile")
    momentum = _value(features, "momentum_60d_percentile")
    low_vol = 1.0 - _value(features, "volatility_20d_percentile")
    score = (
        0.24 * _clamp(quality)
        + 0.18 * _clamp(growth)
        + 0.18 * _clamp(cashflow)
        + 0.12 * _clamp(valuation)
        + 0.20 * _clamp(momentum)
        + 0.08 * _clamp(low_vol)
    )
    return _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="quality_momentum",
        horizon_days=20,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=-8.0,
        theme_code=_theme_label(features),
        reasons=[
            f"质量分位{quality:.1%}",
            f"增长分位{growth:.1%}",
            f"现金流质量分位{cashflow:.1%}",
            f"中期动量分位{momentum:.1%}",
        ],
        features=features,
        required=(
            "quality_percentile",
            "growth_percentile",
            "cashflow_quality_percentile",
            "valuation_percentile",
            "momentum_60d_percentile",
            "volatility_20d_percentile",
        ),
    )


def intraday_surprise(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    volume_z = _value(features, "intraday_amount_surprise_z")
    price_vwap = _value(features, "price_vs_vwap_pct")
    interval_return = _value(features, "interval_return_pct")
    sector_breadth = _value(features, "sector_breadth_pct")
    sector_relative = _value(features, "sector_relative_return_pct")
    fill_probability = _value(features, "fill_probability")
    spread = _value(features, "spread_bps")
    score = (
        0.24 * _scaled(volume_z, 1.0, 5.0)
        + 0.16 * _scaled(price_vwap, 0, 3)
        + 0.16 * _scaled(interval_return, 0.3, 4.0)
        + 0.16 * _scaled(sector_breadth, 50, 90)
        + 0.12 * _scaled(sector_relative, 0, 3)
        + 0.16 * _clamp(fill_probability)
        - 0.15 * _scaled(spread, 15, 80)
    )
    return _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="intraday_surprise",
        horizon_days=1,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=-3.5,
        theme_code=_theme_label(features),
        reasons=[
            f"同时间段成交额异常{volume_z:.2f}σ",
            f"价格高于VWAP {price_vwap:.2f}%",
            f"板块上涨宽度{sector_breadth:.1f}%",
            f"预计成交概率{fill_probability:.1%}",
        ],
        features=features,
        required=(
            "intraday_amount_surprise_z",
            "price_vs_vwap_pct",
            "interval_return_pct",
            "sector_breadth_pct",
            "sector_relative_return_pct",
            "fill_probability",
            "spread_bps",
        ),
    )


def right_side_trend_v301(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    """Right-side confirmation without buying an already overheated move."""

    ret20 = _value(features, "return_20d_pct")
    ret60 = _value(features, "return_60d_pct")
    slope = _value(features, "ma20_slope_5d_pct")
    volume = _value(features, "amount_ratio_5_20")
    relative = _value(features, "relative_strength_20d_pct")
    distance = _value(features, "distance_ma20_pct")
    atr = _value(features, "atr_14d_pct")
    latest_change = _value(features, "latest_change_pct")
    latest_change = _value(features, "latest_change_pct")
    trend_alignment = (
        1.0
        if (
            _value(features, "close_above_ma20") >= 1
            and _value(features, "ma20_above_ma60") >= 1
        )
        else 0.0
    )
    score = (
        0.28 * trend_alignment
        + 0.18 * _band(relative, 5, 22, 8)
        + 0.15 * _band(distance, 0, 7, 6)
        + 0.12 * _band(atr, 1, 5, 3)
        + 0.10 * _band(latest_change, -1, 5, 4)
        + 0.07 * _band(ret20, 2, 28, 15)
        + 0.05 * _band(ret60, 8, 65, 30)
        + 0.03 * _band(volume, 0.7, 2.2, 1.0)
        + 0.02 * _band(slope, 0, 6, 4)
    )
    stop = -max(3.5, min(8.0, atr * 2.2))
    return _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="right_side_trend",
        horizon_days=20,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=stop,
        theme_code=_theme_label(features),
        reasons=[
            f"20日涨幅{ret20:.2f}%，60日涨幅{ret60:.2f}%",
            f"相对强度{relative:.2f}%",
            f"距20日线{distance:.2f}%，ATR{atr:.2f}%",
            f"当日涨跌{latest_change:.2f}%，量能比{volume:.2f}",
        ],
        features=features,
        required=(
            "return_20d_pct",
            "return_60d_pct",
            "ma20_slope_5d_pct",
            "amount_ratio_5_20",
            "relative_strength_20d_pct",
            "distance_ma20_pct",
            "latest_change_pct",
            "close_above_ma20",
            "ma20_above_ma60",
            "atr_14d_pct",
        ),
    )


# Public name points to the current immutable formula; the V3.0.0 function
# above is retained only so historical research artifacts remain reproducible.
right_side_trend_v300 = right_side_trend


def right_side_trend_v302(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    market_return = _value(features, "market_return_20d_pct")
    ret20 = _value(features, "return_20d_pct")
    ret60 = _value(features, "return_60d_pct")
    slope = _value(features, "ma20_slope_5d_pct")
    volume = _value(features, "amount_ratio_5_20")
    relative = _value(features, "relative_strength_20d_pct")
    distance = _value(features, "distance_ma20_pct")
    atr = _value(features, "atr_14d_pct")
    aligned = (
        _value(features, "close_above_ma20") >= 1
        and _value(features, "ma20_above_ma60") >= 1
    )
    score = (
        0.24 * float(aligned)
        + 0.16 * _scaled(ret20, 2, 22)
        + 0.14 * _scaled(ret60, 12, 55)
        + 0.15 * _scaled(slope, 0.2, 4)
        + 0.12 * _scaled(relative, 2, 22)
        + 0.08 * _scaled(volume, 0.9, 1.8)
        + 0.07 * (
            1.0 - _scaled(abs(distance - 4.0), 0, 8)
        )
        + 0.04 * (
            1.0 - _scaled(atr, 1, 5)
        )
    )
    stop = -max(3.5, min(8.0, atr * 2.2))
    signal = _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="right_side_trend",
        horizon_days=10,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=stop,
        theme_code=_theme_label(features),
        reasons=[
            f"市场20日收益{market_return:.2f}%",
            f"个股20日涨幅{ret20:.2f}%，60日涨幅{ret60:.2f}%",
            f"20日线斜率{slope:.2f}%，距20日线{distance:.2f}%",
            f"量能比{volume:.2f}，相对强度{relative:.2f}%",
        ],
        features=features,
        required=(
            "market_return_20d_pct",
            "return_20d_pct",
            "return_60d_pct",
            "ma20_slope_5d_pct",
            "amount_ratio_5_20",
            "relative_strength_20d_pct",
            "distance_ma20_pct",
            "close_above_ma20",
            "ma20_above_ma60",
            "atr_14d_pct",
        ),
    )
    if signal.status == "SCORED" and market_return < 2.0:
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": "MARKET_REGIME_BLOCKED",
                "reasons": signal.reasons + (
                    "市场20日收益低于2%，趋势策略不新开仓",
                ),
            }
        )
    return signal


def right_side_trend_v303(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    """Allow selective sector trends when the broad market is weak."""

    market_return = _value(features, "market_return_20d_pct")
    ret20 = _value(features, "return_20d_pct")
    ret60 = _value(features, "return_60d_pct")
    slope = _value(features, "ma20_slope_5d_pct")
    volume = _value(features, "amount_ratio_5_20")
    relative = _value(features, "relative_strength_20d_pct")
    distance = _value(features, "distance_ma20_pct")
    atr = _value(features, "atr_14d_pct")
    latest_change = _value(features, "latest_change_pct")
    sector_relative = _value(features, "sector_relative_return_pct")
    sector_breadth = _value(features, "sector_breadth_pct")
    sector_acceleration = _value(
        features,
        "sector_breadth_acceleration_pct",
    )
    sector_amount = _value(
        features,
        "sector_amount_acceleration_pct",
    )
    leadership = _value(features, "stock_leadership_score")
    aligned = (
        _value(features, "close_above_ma20") >= 1
        and _value(features, "ma20_above_ma60") >= 1
    )
    sector_confirmation = (
        0.34 * _scaled(sector_relative, 0.0, 5.0)
        + 0.26 * _scaled(sector_breadth, 48.0, 78.0)
        + 0.20 * _scaled(sector_acceleration, 0.0, 18.0)
        + 0.20 * _scaled(sector_amount, 0.0, 50.0)
    )
    score = (
        0.22 * float(aligned)
        + 0.14 * _band(relative, 4, 24, 10)
        + 0.13 * _band(distance, -1, 8, 7)
        + 0.09 * _band(latest_change, -1, 6, 5)
        + 0.08 * _band(ret20, 2, 30, 18)
        + 0.06 * _band(ret60, 8, 65, 35)
        + 0.06 * _band(volume, 0.8, 2.3, 1.2)
        + 0.05 * _band(slope, 0, 6, 4)
        + 0.04 * _band(atr, 1, 5, 3)
        + 0.09 * sector_confirmation
        + 0.04 * _clamp(leadership)
    )
    stop = -max(3.5, min(8.0, atr * 2.2))
    signal = _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="right_side_trend",
        horizon_days=10,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=stop,
        theme_code=_theme_label(features),
        reasons=[
            f"市场20日收益{market_return:.2f}%",
            f"个股20日涨幅{ret20:.2f}%，相对强度{relative:.2f}%",
            (
                f"板块相对收益{sector_relative:.2f}%，"
                f"上涨宽度{sector_breadth:.1f}%"
            ),
            (
                f"板块宽度加速度{sector_acceleration:.1f}个百分点，"
                f"个股领导力{leadership:.1%}"
            ),
        ],
        features=features,
        required=(
            "market_return_20d_pct",
            "return_20d_pct",
            "return_60d_pct",
            "ma20_slope_5d_pct",
            "amount_ratio_5_20",
            "relative_strength_20d_pct",
            "distance_ma20_pct",
            "latest_change_pct",
            "close_above_ma20",
            "ma20_above_ma60",
            "atr_14d_pct",
            "sector_relative_return_pct",
            "sector_breadth_pct",
            "sector_breadth_acceleration_pct",
            "sector_amount_acceleration_pct",
            "stock_leadership_score",
        ),
    )
    selective_theme_watch = bool(
        aligned
        and sector_relative >= 1.0
        and sector_breadth >= 52.0
        and (
            sector_acceleration >= 2.0
            or sector_amount >= 10.0
        )
        and leadership >= 0.42
    )
    trend_reacceleration_ready = bool(
        aligned
        and market_return >= 0.0
        and 2.0 <= ret20 <= 22.0
        and 12.0 <= ret60 <= 55.0
        and 0.2 <= slope <= 4.0
        and 0.0 <= distance <= 8.0
        and -2.0 <= latest_change <= 6.5
        and 0.9 <= volume <= 1.8
        and 1.0 <= atr <= 5.5
    )
    if signal.status == "SCORED" and market_return < 0.0:
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": (
                    "WEAK_MARKET_THEME_WATCH"
                    if selective_theme_watch
                    else "MARKET_REGIME_BLOCKED"
                ),
                "reasons": signal.reasons + (
                    (
                        "大盘仍弱，但板块扩散和个股领导力成立；"
                        "列入结构性机会观察，不冒充已验证买点"
                    )
                    if selective_theme_watch
                    else "大盘为负且细分板块未形成足够独立行情"
                ,),
            }
        )
    if (
        signal.status == "SCORED"
        and not trend_reacceleration_ready
    ):
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": "SETUP_NOT_READY",
                "reasons": signal.reasons + (
                    "趋势存在但入场位置过热、过弱或波动不适合新开仓",
                ),
            }
        )
    return signal


def right_side_trend_v304(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    """V3.0.4 enforces the same setup universe used by its backtest."""

    signal = right_side_trend_v302(
        code,
        name,
        features,
        feature_time,
        valid_until,
    )
    if signal.status != "SCORED" or right_side_setup_ready(features):
        return signal
    return RawSignal(
        **{
            **signal.as_dict(),
            "status": "SETUP_NOT_READY",
            "reasons": signal.reasons + (
                "当前个股不属于右侧趋势样本外校准使用的资格区间",
            ),
        }
    )


right_side_trend = right_side_trend_v304


def oversold_reversal(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    """Identify an oversold bottoming process before a full right-side turn.

    ``horizon_days`` is the forward evaluation window.  A paper position is
    still exited dynamically when the signal disappears or its stop breaks.
    """

    ret2 = _value(features, "return_2d_pct")
    ret5 = _value(features, "return_5d_pct")
    ret20 = _value(features, "return_20d_pct")
    drawdown = _value(features, "drawdown_20d_pct")
    distance_ma20 = _value(features, "distance_ma20_pct")
    distance_ma5 = _value(features, "distance_ma5_pct")
    slope = _value(features, "ma20_slope_5d_pct")
    latest_change = _value(features, "latest_change_pct")
    previous_change = _value(features, "previous_change_pct")
    amount_ratio = _value(features, "amount_ratio_1_20")
    rebound = _value(features, "rebound_from_low_pct")
    relative_market = _value(
        features,
        "latest_relative_to_market_pct",
    )
    atr = _value(features, "atr_14d_pct")
    sector_relative = _value(
        features,
        "sector_relative_return_pct",
    )
    sector_breadth = _value(features, "sector_breadth_pct")
    opportunity = _value(features, "theme_opportunity_score")
    leadership = _value(features, "stock_leadership_score")

    oversold_quality = (
        0.32 * _band(ret20, -45.0, -10.0, 15.0)
        + 0.27 * _band(drawdown, -50.0, -12.0, 15.0)
        + 0.24 * _band(distance_ma20, -30.0, -3.0, 10.0)
        + 0.10 * _band(ret5, -14.0, 5.0, 8.0)
        + 0.07 * _band(slope, -12.0, 1.0, 7.0)
    )
    reversal_quality = (
        0.25 * _scaled(latest_change, 0.5, 6.5)
        + 0.22 * _scaled(amount_ratio, 0.8, 2.8)
        + 0.20 * _scaled(rebound, 0.5, 4.5)
        + 0.18 * _scaled(relative_market, 0.0, 5.0)
        + 0.10 * _band(previous_change, -9.5, 2.5, 5.0)
        + 0.05 * _band(distance_ma5, -5.0, 5.0, 5.0)
    )
    context_quality = (
        0.28 * _scaled(sector_relative, 0.0, 5.0)
        + 0.24 * _scaled(sector_breadth, 40.0, 72.0)
        + 0.24 * _clamp(opportunity)
        + 0.16 * _clamp(leadership)
        + 0.08 * _band(atr, 1.2, 8.0, 3.0)
    )
    score = (
        0.42 * oversold_quality
        + 0.38 * reversal_quality
        + 0.20 * context_quality
    )
    signal = _signal(
        stock_code=code,
        stock_name=name,
        strategy_key="oversold_reversal",
        horizon_days=5,
        score=score,
        feature_time=feature_time,
        valid_until=valid_until,
        initial_stop_pct=-max(3.5, min(6.0, atr * 1.25)),
        theme_code=_theme_label(features),
        reasons=[
            (
                f"20日涨幅{ret20:.2f}%，20日最大回撤位置"
                f"{drawdown:.2f}%，距20日线{distance_ma20:.2f}%"
            ),
            (
                f"当日涨幅{latest_change:.2f}%，较市场强"
                f"{relative_market:.2f}%，低点回拉{rebound:.2f}%"
            ),
            (
                f"当日成交额/20日均额{amount_ratio:.2f}，"
                f"板块相对收益{sector_relative:.2f}%，"
                f"板块上涨宽度{sector_breadth:.1f}%"
            ),
            "这是左侧超跌修复实验；先观察、再触发小仓模拟，不冒充确定性抄底",
        ],
        features=features,
        required=(
            "return_2d_pct",
            "return_5d_pct",
            "return_20d_pct",
            "drawdown_20d_pct",
            "distance_ma20_pct",
            "distance_ma5_pct",
            "ma20_slope_5d_pct",
            "latest_change_pct",
            "previous_change_pct",
            "amount_ratio_1_20",
            "rebound_from_low_pct",
            "latest_relative_to_market_pct",
            "atr_14d_pct",
            "sector_relative_return_pct",
            "sector_breadth_pct",
            "theme_opportunity_score",
            "stock_leadership_score",
        ),
    )
    if signal.status != "SCORED":
        return signal
    prepare_zone = bool(
        -52.0 <= ret20 <= -8.0
        and -55.0 <= drawdown <= -10.0
        and -34.0 <= distance_ma20 <= -2.0
        and -25.0 <= ret5 <= 12.0
        and 1.2 <= atr <= 15.0
    )
    if not prepare_zone:
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": "SETUP_NOT_READY",
                "reasons": signal.reasons + (
                    "尚未进入超跌修复策略定义的准备区",
                ),
            }
        )
    sector_confirmed = bool(
        (
            sector_relative >= 0.5
            and sector_breadth >= 45.0
        )
        or opportunity >= 0.55
        or leadership >= 0.45
    )
    trigger_ready = bool(
        score >= 0.68
        and 1.2 <= latest_change <= 8.8
        and -9.8 <= previous_change <= 3.0
        and -5.0 <= ret2 <= 11.0
        and amount_ratio >= 1.05
        and amount_ratio <= 5.0
        and rebound >= 1.0
        and relative_market >= 1.0
        and distance_ma5 >= -5.0
        and sector_confirmed
    )
    if not trigger_ready:
        return RawSignal(
            **{
                **signal.as_dict(),
                "status": "LEFT_SIDE_PREPARE",
                "reasons": signal.reasons + (
                    "已进入抄底准备区，但止跌、放量或板块共振未齐，只观察不买",
                ),
            }
        )
    return RawSignal(
        **{
            **signal.as_dict(),
            "reasons": signal.reasons + (
                "止跌回拉、相对强度、量能和板块确认同时触发；仅允许小仓模拟试错",
            ),
        }
    )


SLEEVE_BUILDERS: dict[str, Callable[..., RawSignal]] = {
    "theme_diffusion": theme_diffusion,
    "low_base_ignition": low_base_ignition,
    "weak_market_structural_mainline": weak_market_structural_mainline,
    "right_side_trend": right_side_trend_v304,
    "event_drift": event_drift,
    "quality_momentum": quality_momentum,
    "oversold_reversal": oversold_reversal,
    "intraday_surprise": intraday_surprise,
}


def generate_raw_signals(
    stock_code: str,
    stock_name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> tuple[RawSignal, ...]:
    return tuple(
        builder(
            stock_code,
            stock_name,
            features,
            feature_time,
            valid_until,
        )
        for builder in SLEEVE_BUILDERS.values()
    )
