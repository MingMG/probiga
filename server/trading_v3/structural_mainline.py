from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

from .domain import RawSignal


STRATEGY_KEY = "weak_market_structural_mainline"


REQUIRED_FEATURES = (
    "market_return_20d_pct",
    "sector_breadth_pct",
    "sector_breadth_acceleration_pct",
    "sector_amount_acceleration_pct",
    "sector_relative_return_pct",
    "sector_leadership_depth",
    "theme_news_novelty_score",
    "theme_topk_member_score_median",
    "theme_composite_score",
    "theme_score_news_novelty_available",
    "stock_leadership_score",
    "relative_strength_20d_pct",
    "return_5d_pct",
    "amount_ratio_5_20",
    "latest_change_pct",
    "distance_ma20_pct",
    "atr_14d_pct",
    "entry_eligible",
    "latest_tradable",
)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _scaled(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return _clamp((float(value) - lower) / (upper - lower))


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


def _number(features: dict[str, Any], key: str) -> float:
    return float(features.get(key) or 0.0)


def _missing_features(features: dict[str, Any]) -> tuple[str, ...]:
    missing = []
    for key in REQUIRED_FEATURES:
        value = features.get(key)
        if value is None:
            missing.append(key)
            continue
        try:
            if not math.isfinite(float(value)):
                missing.append(key)
        except (TypeError, ValueError):
            missing.append(key)
    return tuple(missing)


def _data_quality_failures(features: dict[str, Any]) -> tuple[str, ...]:
    failures = []
    if _number(features, "entry_eligible") < 1.0:
        failures.append("STOCK_ENTRY_DATA_NOT_ELIGIBLE")
    if _number(features, "latest_tradable") < 1.0:
        failures.append("LATEST_BAR_NOT_TRADABLE")
    for key in (
        "data_quality_status",
        "market_data_quality_status",
        "theme_feature_quality_status",
    ):
        if key in features and str(features.get(key) or "").upper() != "PASS":
            failures.append(f"{key.upper()}_NOT_PASS")
    if (
        "qmt_attestation_current" in features
        and not bool(features.get("qmt_attestation_current"))
    ):
        failures.append("QMT_DAILY_KLINE_ATTESTATION_NOT_CURRENT")
    return tuple(failures)


def _feature_snapshot(features: dict[str, Any]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    for key, value in features.items():
        if isinstance(value, (int, float)):
            number = float(value)
            if math.isfinite(number):
                snapshot[key] = number
        elif isinstance(value, str) and key in {
            "theme_code",
            "theme_name",
            "theme_source",
            "data_quality_status",
            "market_data_quality_status",
            "theme_feature_quality_status",
            "theme_news_novelty_status",
            "theme_news_novelty_match_key",
            "theme_feature_key",
        }:
            snapshot[key] = value
        elif isinstance(value, (list, tuple)) and key in {
            "theme_codes",
            "theme_names",
            "theme_cluster_keys",
            "theme_cluster_labels",
            "all_theme_cluster_keys",
            "all_theme_cluster_labels",
        }:
            snapshot[key] = [str(item) for item in value if str(item)]
    return snapshot


def weak_market_structural_mainline(
    code: str,
    name: str,
    features: dict[str, Any],
    feature_time: datetime,
    valid_until: datetime,
) -> RawSignal:
    """Research a sector-led mainline without borrowing trend validation.

    The sleeve is eligible only in a weak broad market.  It can score a stock
    when sector-internal evidence is strong, but its unique strategy key means
    downstream calibration and OOS gates cannot reuse ``right_side_trend``.
    """

    missing = _missing_features(features)
    if missing:
        return RawSignal(
            stock_code=code,
            stock_name=name,
            strategy_key=STRATEGY_KEY,
            horizon_days=5,
            score=0.0,
            feature_time=feature_time,
            valid_until=min(valid_until, feature_time + timedelta(days=10)),
            initial_stop_pct=-6.0,
            theme_code=str(
                features.get("theme_name")
                or features.get("theme_code")
                or ""
            ),
            status="INSUFFICIENT_DATA",
            reasons=("缺少特征：" + "、".join(missing),),
            features=_feature_snapshot(features),
        )

    market_return = _number(features, "market_return_20d_pct")
    breadth = _number(features, "sector_breadth_pct")
    breadth_acceleration = _number(
        features,
        "sector_breadth_acceleration_pct",
    )
    capital = _number(features, "sector_amount_acceleration_pct")
    sector_relative = _number(features, "sector_relative_return_pct")
    leadership_depth = _number(features, "sector_leadership_depth")
    novelty = _number(features, "theme_news_novelty_score")
    topk_median = _number(features, "theme_topk_member_score_median")
    composite = _number(features, "theme_composite_score")
    leadership = _number(features, "stock_leadership_score")
    relative = _number(features, "relative_strength_20d_pct")
    return_5d = _number(features, "return_5d_pct")
    amount_ratio = _number(features, "amount_ratio_5_20")
    latest_change = _number(features, "latest_change_pct")
    distance = _number(features, "distance_ma20_pct")
    atr = _number(features, "atr_14d_pct")
    crowding = _number(features, "sector_crowding")
    novelty_available = _number(
        features,
        "theme_score_news_novelty_available",
    )

    breadth_exhaustion = _scaled(breadth, 88.0, 100.0)
    crowding_pressure = (
        0.55 * _scaled(crowding, 0.78, 1.0)
        + 0.45 * breadth_exhaustion
    )
    persistence_relief = _clamp(
        0.55 * _scaled(breadth_acceleration, -2.0, 10.0)
        + 0.45 * _clamp(leadership_depth)
    )
    exhaustion_penalty = crowding_pressure * (
        1.0 - 0.65 * persistence_relief
    )

    sector_internal_strength = (
        0.40 * _clamp(composite)
        + 0.20 * _scaled(sector_relative, 0.0, 6.0)
        + 0.15 * _scaled(capital, 0.0, 65.0)
        + 0.10 * _scaled(breadth, 45.0, 82.0)
        + 0.075
        * (
            _clamp(novelty)
            if novelty_available >= 1.0
            else _clamp(composite)
        )
        + 0.075 * _clamp(topk_median)
    )
    stock_confirmation = (
        0.28 * _clamp(leadership)
        + 0.20 * _scaled(relative, 3.0, 20.0)
        + 0.16 * _band(return_5d, -2.0, 15.0, 8.0)
        + 0.14 * _band(amount_ratio, 0.9, 2.5, 1.2)
        + 0.12 * _band(latest_change, -1.0, 6.5, 5.0)
        + 0.10 * _band(distance, -4.0, 10.0, 8.0)
    )
    score = _clamp(
        0.72 * sector_internal_strength
        + 0.28 * stock_confirmation
        - 0.10 * _scaled(-market_return, 8.0, 18.0)
        - 0.12 * exhaustion_penalty
    )
    enriched = {
        **features,
        "structural_sector_internal_strength": sector_internal_strength,
        "structural_stock_confirmation": stock_confirmation,
        "structural_crowding_pressure": crowding_pressure,
        "structural_persistence_relief": persistence_relief,
        "structural_exhaustion_penalty": exhaustion_penalty,
    }
    reasons = (
        f"弱市大盘20日收益{market_return:.2f}%",
        f"板块上涨宽度{breadth:.1f}%，资金加速{capital:.1f}%",
        f"宽度加速度{breadth_acceleration:.1f}%，领导层深度{leadership_depth:.1%}",
        f"板块相对强度{sector_relative:.2f}%",
        f"新闻新颖度{novelty:.1%}，Top-K成员中位分{topk_median:.1%}",
        f"板块综合分{composite:.1%}，个股领导力{leadership:.1%}",
        f"拥挤/扩散耗竭软惩罚{exhaustion_penalty:.1%}",
        "独立弱市结构性主线信号，不继承右侧趋势模型身份或校准",
    )
    status = "SCORED"
    status_reason = ""
    quality_failures = _data_quality_failures(features)
    if quality_failures:
        status = "DATA_QUALITY_BLOCKED"
        status_reason = "数据质量门未通过：" + "、".join(quality_failures)
    elif market_return >= 0.0:
        status = "MARKET_REGIME_NOT_APPLICABLE"
        status_reason = "大盘不处于弱市，本策略不与右侧趋势策略争夺身份"
    elif market_return < -12.0:
        status = "RISK_OFF_BLOCKED"
        status_reason = "大盘跌幅进入极端风险区，板块相对强度不足以放行"
    else:
        sector_ready = bool(
            breadth >= 52.0
            and capital >= 15.0
            and sector_relative >= 2.0
            and composite >= 0.62
            and topk_median >= 0.58
            and sector_internal_strength >= 0.64
        )
        stock_ready = bool(
            leadership >= 0.45
            and relative >= 6.0
            and -2.0 <= return_5d <= 15.0
            and 0.9 <= amount_ratio <= 2.5
            and -1.0 <= latest_change <= 6.5
            and -4.0 <= distance <= 10.0
            and 1.0 <= atr <= 7.0
        )
        if not (sector_ready and stock_ready and score >= 0.64):
            status = "SETUP_NOT_READY"
            status_reason = "板块内部强度或个股确认尚未达到冻结触发线"

    return RawSignal(
        stock_code=code,
        stock_name=name,
        strategy_key=STRATEGY_KEY,
        horizon_days=5,
        score=round(score, 8),
        feature_time=feature_time,
        valid_until=min(valid_until, feature_time + timedelta(days=10)),
        initial_stop_pct=-max(4.0, min(7.0, atr * 1.5)),
        theme_code=str(
            features.get("theme_name")
            or features.get("theme_code")
            or ""
        ),
        status=status,
        reasons=reasons + ((status_reason,) if status_reason else ()),
        features=_feature_snapshot(enriched),
    )
