from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from .domain import (
    AlphaForecast,
    HypothesisEvidence,
    RegimeProbabilities,
    TradeHypothesis,
)


STRATEGY_LABELS = {
    "theme_diffusion": "板块扩散",
    "low_base_ignition": "低位点火",
    "right_side_trend": "右侧主升",
    "event_drift": "事件漂移",
    "quality_momentum": "质量动量",
    "intraday_surprise": "盘中超预期",
    "oversold_reversal": "超跌修复",
    "weak_market_structural_mainline": "弱市结构性主线",
}

ALPHA_HALF_LIFE_MINUTES = {
    "intraday_surprise": 25,
    "theme_diffusion": 720,
    "low_base_ignition": 960,
    "oversold_reversal": 720,
    "event_drift": 1440,
    "right_side_trend": 2400,
    "quality_momentum": 7200,
    "weak_market_structural_mainline": 1440,
}

STATUS_ADJUSTMENTS = {
    "VALIDATED_POSITIVE": 0.16,
    "PAPER_DISCOVERY_CANDIDATE": 0.08,
    "LEFT_SIDE_PREPARE": 0.02,
    "WEAK_MARKET_THEME_WATCH": -0.02,
    "SETUP_NOT_READY": -0.05,
    "MARKET_REGIME_BLOCKED": -0.09,
    "INSUFFICIENT_DATA": -0.18,
    "RESEARCH_ONLY_PROFIT_GATE_FAILED": -0.08,
    "RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED": -0.16,
    "RESEARCH_ONLY_MODEL_VERSION_MISMATCH": -0.10,
    "DATA_QUALITY_BLOCKED": -0.20,
}

SIGNAL_STATUS_PRIORITY = {
    "VALIDATED_POSITIVE": 0,
    "PAPER_DISCOVERY_CANDIDATE": 1,
    "LEFT_SIDE_PREPARE": 2,
    "WEAK_MARKET_THEME_WATCH": 3,
    "RESEARCH_ONLY_UNCALIBRATED": 4,
}

NON_SUPPORTING_STATUS_LABELS = {
    "SETUP_NOT_READY": "条件尚未触发",
    "MARKET_REGIME_BLOCKED": "市场状态不匹配",
    "INSUFFICIENT_DATA": "数据不足",
    "RESEARCH_ONLY_PROFIT_GATE_FAILED": "样本外收益闸门未通过",
    "RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED": "历史高分组排序失真",
    "RESEARCH_ONLY_MODEL_VERSION_MISMATCH": "校准版本与当前公式不匹配",
    "DATA_QUALITY_BLOCKED": "数据质量门未通过",
}


def _clamp(value: float, lower: float = 0.03, upper: float = 0.97) -> float:
    return max(lower, min(upper, float(value)))


def _logit(probability: float) -> float:
    probability = _clamp(probability, 0.001, 0.999)
    return math.log(probability / (1.0 - probability))


def _logistic(value: float) -> float:
    if value >= 0:
        exp = math.exp(-value)
        return 1.0 / (1.0 + exp)
    exp = math.exp(value)
    return exp / (1.0 + exp)


def _value(features: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = features.get(key)
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _probability_kind(items: list[AlphaForecast]) -> str:
    if any(item.status == "VALIDATED_POSITIVE" for item in items):
        return "OOS_CALIBRATED"
    if any(item.status == "PAPER_DISCOVERY_CANDIDATE" for item in items):
        return "PAPER_FORWARD_PRIOR"
    return "STRUCTURED_RESEARCH_PRIOR"


def _signal_items(items: list[AlphaForecast]) -> list[AlphaForecast]:
    """Return only sleeves that actually support the current hypothesis.

    Every stock receives one forecast row per sleeve for auditability.  A row
    being calculated does not mean that sleeve supports the stock.  Keeping
    blocked/setup-not-ready rows out of ``strategy_keys`` prevents the UI from
    presenting seven-strategy consensus where only one sleeve actually fired.
    """

    notable = [
        item
        for item in items
        if item.status
        in {
            "VALIDATED_POSITIVE",
            "PAPER_DISCOVERY_CANDIDATE",
            "LEFT_SIDE_PREPARE",
            "WEAK_MARKET_THEME_WATCH",
        }
    ]
    if notable:
        selected = notable
    else:
        selected = [
            item
            for item in items
            if item.status == "RESEARCH_ONLY_UNCALIBRATED"
            and float(item.raw_score or 0.0) >= 0.58
        ]
    selected.sort(
        key=lambda item: (
            SIGNAL_STATUS_PRIORITY.get(item.status, 99),
            -float(item.raw_score or 0.0),
            item.strategy_key,
        )
    )
    return selected[:3]


def _non_supporting_evidence(
    items: list[AlphaForecast],
    selected: list[AlphaForecast],
) -> tuple[str, ...]:
    selected_ids = {id(item) for item in selected}
    values: list[str] = []
    for item in sorted(
        items,
        key=lambda row: -float(row.raw_score or 0.0),
    ):
        if id(item) in selected_ids:
            continue
        label = NON_SUPPORTING_STATUS_LABELS.get(item.status)
        if not label:
            continue
        values.append(
            f"{STRATEGY_LABELS.get(item.strategy_key, item.strategy_key)}：{label}"
        )
        if len(values) >= 3:
            break
    return tuple(values)


def _base_probability(items: list[AlphaForecast]) -> float:
    calibrated = [
        float(item.probability_positive)
        for item in items
        if item.probability_positive is not None
        and item.status == "VALIDATED_POSITIVE"
    ]
    if calibrated:
        return _clamp(sum(calibrated) / len(calibrated))
    scores = [
        _clamp(float(item.raw_score or 0.0), 0.0, 1.0)
        for item in items
    ]
    if not scores:
        return 0.35
    strongest = max(scores)
    mean_score = sum(scores) / len(scores)
    status_adjustment = max(
        STATUS_ADJUSTMENTS.get(item.status, -0.03)
        for item in items
    )
    # This is deliberately labelled a structured prior, not a calibrated
    # win probability. It is only used to order observations and to decide
    # when fresh evidence deserves the user's attention.
    return _clamp(0.18 + 0.42 * strongest + 0.18 * mean_score + status_adjustment)


def _regime_alignment(
    items: list[AlphaForecast],
    regime: RegimeProbabilities,
) -> float:
    probabilities = regime.probabilities
    strategy_keys = {item.strategy_key for item in items}
    alignment = 0.0
    if "right_side_trend" in strategy_keys:
        alignment += 0.18 * probabilities.get("TREND_UP", 0.0)
        alignment -= 0.12 * probabilities.get("RISK_OFF", 0.0)
    if {"theme_diffusion", "low_base_ignition"} & strategy_keys:
        alignment += 0.17 * probabilities.get("THEME_ROTATION", 0.0)
        alignment += 0.06 * probabilities.get("PANIC_RECOVERY", 0.0)
    if "oversold_reversal" in strategy_keys:
        alignment += 0.20 * probabilities.get("PANIC_RECOVERY", 0.0)
        alignment += 0.06 * probabilities.get("RANGE", 0.0)
        alignment -= 0.10 * probabilities.get("TREND_UP", 0.0)
    if "quality_momentum" in strategy_keys:
        alignment += 0.08 * probabilities.get("RANGE", 0.0)
        alignment += 0.06 * probabilities.get("TREND_UP", 0.0)
    if "intraday_surprise" in strategy_keys:
        alignment += 0.08 * probabilities.get("THEME_ROTATION", 0.0)
        alignment += 0.06 * probabilities.get("PANIC_RECOVERY", 0.0)
    if "weak_market_structural_mainline" in strategy_keys:
        alignment += 0.10 * probabilities.get("RANGE", 0.0)
        alignment += 0.12 * probabilities.get("PANIC_RECOVERY", 0.0)
        alignment += 0.08 * probabilities.get("THEME_ROTATION", 0.0)
        alignment -= 0.08 * probabilities.get("RISK_OFF", 0.0)
    return alignment


def _role(features: dict[str, Any]) -> str:
    leadership = _value(features, "stock_leadership_score")
    relative = _value(features, "stock_relative_to_theme_5d_pct")
    if leadership >= 0.78:
        return "LEADER"
    if leadership >= 0.48:
        return "CORE"
    if relative <= 0.5 and _value(features, "theme_opportunity_score") >= 0.65:
        return "FOLLOWER_CANDIDATE"
    return "INDEPENDENT"


def _evidence(features: dict[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    positive: list[str] = []
    negative: list[str] = []
    breadth = _value(features, "sector_breadth_pct")
    breadth_accel = _value(features, "sector_breadth_acceleration_pct")
    sector_relative = _value(features, "sector_relative_return_pct")
    amount_accel = _value(features, "sector_amount_acceleration_pct")
    relative = _value(features, "relative_strength_20d_pct")
    latest_relative = _value(features, "latest_relative_to_market_pct")
    amount_ratio = max(
        _value(features, "amount_ratio_1_20"),
        _value(features, "amount_ratio_5_20"),
    )
    distance = _value(features, "distance_ma20_pct")
    crowding = _value(features, "sector_crowding")
    event_surprise = _value(features, "event_surprise")
    event_priced_in = _value(features, "event_priced_in")

    if breadth >= 60:
        positive.append(f"板块红盘宽度{breadth:.1f}%，不是单票孤立上涨")
    elif breadth > 0:
        negative.append(f"板块红盘宽度只有{breadth:.1f}%，扩散尚未确认")
    if breadth_accel >= 8:
        positive.append(f"板块宽度较前一交易日加速{breadth_accel:.1f}个百分点")
    elif breadth_accel <= -8:
        negative.append(f"板块宽度回落{abs(breadth_accel):.1f}个百分点")
    if sector_relative >= 1.5:
        positive.append(f"板块相对市场强{sector_relative:.1f}%")
    elif sector_relative <= -1:
        negative.append(f"板块相对市场弱{abs(sector_relative):.1f}%")
    if amount_accel >= 20:
        positive.append(f"板块成交额加速{amount_accel:.1f}%")
    if relative >= 5 or latest_relative >= 2:
        positive.append("个股相对指数保持主动强势")
    if amount_ratio >= 1.35:
        positive.append(f"个股量能达到近期均值的{amount_ratio:.2f}倍")
    elif amount_ratio > 0 and amount_ratio < 0.70:
        negative.append(f"个股量能只有近期均值的{amount_ratio:.2f}倍")
    if distance >= 14:
        negative.append(f"价格高于MA20约{distance:.1f}%，追高风险上升")
    if crowding >= 0.70:
        negative.append("板块交易拥挤，后手接力风险偏高")
    if event_surprise >= 0.65:
        positive.append("存在尚需量价确认的事件催化")
    if event_priced_in >= 0.70:
        negative.append("事件可能已经被价格充分消化")
    if not positive:
        positive.append("已进入多策略观察范围，等待新的量价证据")
    if not negative:
        negative.append("当前未发现硬失效，但仍需盘中确认承接")
    return tuple(positive[:8]), tuple(negative[:8])


def _triggers(
    strategy_keys: set[str],
    role: str,
) -> tuple[str, ...]:
    values = [
        "个股站稳VWAP或关键突破位，回踩时不出现放量破位",
        "板块宽度和相对强度同步改善，至少两只核心股共同确认",
        "实时量能高于同时间基线，主动买盘不被卖盘迅速压回",
    ]
    if "oversold_reversal" in strategy_keys:
        values.insert(0, "不再创新低并从日内低点有效回拉，出现止跌承接")
    if "event_drift" in strategy_keys:
        values.append("事件来源可靠、仍具新颖性，价格反应未一次性透支")
    if role == "FOLLOWER_CANDIDATE":
        values.append("龙一不可成交或高位拥挤，龙二获得独立增量资金而非被动跟随")
    return tuple(values)


def _invalidations(strategy_keys: set[str]) -> tuple[str, ...]:
    values = [
        "跌破当日关键低点或保护位且无法快速收回",
        "板块相对强度转负，核心股同步走弱",
        "盘口卖压持续占优，放量却不能推动价格",
    ]
    if "event_drift" in strategy_keys:
        values.append("公告或权威消息否定原事件逻辑")
    if "right_side_trend" in strategy_keys:
        values.append("趋势结构破坏，不因原计划持有天数而继续死拿")
    return tuple(values)


def _state_and_action(
    probability: float,
    items: list[AlphaForecast],
) -> tuple[str, str, float]:
    statuses = {item.status for item in items}
    if "VALIDATED_POSITIVE" in statuses and probability >= 0.64:
        return "TRIGGER_READY", "WAIT_INTRADAY_CONFIRM", 0.08
    if "PAPER_DISCOVERY_CANDIDATE" in statuses and probability >= 0.60:
        return "TRIGGER_READY", "PAPER_PROBE_IF_CONFIRMED", 0.025
    if probability >= 0.58:
        return "PREPARE", "WATCH_CLOSELY", 0.0
    if probability >= 0.40:
        return "WATCH", "NO_TRADE", 0.0
    return "WEAKEN", "NO_TRADE", 0.0


def build_stock_hypotheses(
    forecasts: Iterable[AlphaForecast],
    *,
    run_uid: str,
    trade_date: date,
    decision_at: datetime,
    regime: RegimeProbabilities,
    limit: int = 300,
) -> tuple[TradeHypothesis, ...]:
    grouped: dict[str, list[AlphaForecast]] = defaultdict(list)
    for forecast in forecasts:
        grouped[forecast.stock_code].append(forecast)
    hypotheses: list[TradeHypothesis] = []
    for stock_code, items in grouped.items():
        signal_items = _signal_items(items)
        if not signal_items:
            continue
        strongest_score = max(
            float(item.raw_score or 0.0) for item in signal_items
        )
        probability_kind = _probability_kind(signal_items)
        base = _base_probability(signal_items)
        probability = _clamp(
            base + _regime_alignment(signal_items, regime)
        )
        if probability_kind == "PAPER_FORWARD_PRIOR":
            probability = min(probability, 0.74)
        elif probability_kind == "STRUCTURED_RESEARCH_PRIOR":
            probability = min(probability, 0.69)
        primary = max(
            signal_items,
            key=lambda item: (
                item.status == "VALIDATED_POSITIVE",
                item.status == "PAPER_DISCOVERY_CANDIDATE",
                float(item.raw_score or 0.0),
            ),
        )
        features = dict(primary.features or {})
        strategy_keys = tuple(
            sorted({item.strategy_key for item in signal_items})
        )
        role = _role(features)
        supporting, opposing = _evidence(features)
        opposing = tuple(
            list(opposing)
            + list(_non_supporting_evidence(items, signal_items))
        )[:8]
        state, action, max_weight = _state_and_action(
            probability,
            signal_items,
        )
        labels = "、".join(STRATEGY_LABELS.get(key, key) for key in strategy_keys)
        theme = str(primary.theme_code or features.get("theme_name") or "")
        horizon_minutes = max(240, int(primary.horizon_days) * 240)
        half_life = min(
            ALPHA_HALF_LIFE_MINUTES.get(key, horizon_minutes)
            for key in strategy_keys
        )
        hypothesis_key = f"STOCK:{trade_date.isoformat()}:{stock_code}"
        hypotheses.append(
            TradeHypothesis(
                hypothesis_key=hypothesis_key,
                run_uid=run_uid,
                trade_date=trade_date.isoformat(),
                scope_type="STOCK",
                scope_code=stock_code,
                scope_name=primary.stock_name,
                direction="LONG",
                state=state,
                probability=round(probability, 6),
                prior_probability=round(base, 6),
                probability_kind=probability_kind,
                confidence=round(
                    max(
                        max(
                            float(item.confidence or 0.0)
                            for item in signal_items
                        ),
                        min(0.75, 0.20 + strongest_score * 0.55),
                    ),
                    6,
                ),
                score=round(strongest_score, 8),
                horizon_minutes=horizon_minutes,
                alpha_half_life_minutes=half_life,
                proposed_action=action,
                max_position_weight=max_weight,
                theme_code=theme,
                role=role,
                thesis=(
                    f"{primary.stock_name}进入{labels}观察框架；"
                    f"当前先验证板块、个股和盘口是否共振，不因单一指标直接买入"
                ),
                counter_thesis=(
                    "如果板块不能扩散、个股相对强度转弱或主动买盘无法推动价格，"
                    "原假设即降级或失效"
                ),
                supporting_evidence=supporting,
                opposing_evidence=opposing,
                triggers=_triggers(set(strategy_keys), role),
                invalidations=_invalidations(set(strategy_keys)),
                strategy_keys=strategy_keys,
                feature_time=primary.feature_time,
                valid_until=min(
                    max(item.valid_until for item in signal_items),
                    decision_at + timedelta(days=30),
                ),
                source_forecast_count=len(items),
            )
        )
    hypotheses.sort(
        key=lambda item: (
            item.state not in {"TRIGGER_READY", "PREPARE"},
            -item.probability,
            -item.score,
            item.scope_code,
        )
    )
    return tuple(hypotheses[: max(1, int(limit))])


def build_market_hypothesis(
    *,
    run_uid: str,
    trade_date: date,
    decision_at: datetime,
    regime: RegimeProbabilities,
) -> TradeHypothesis:
    dominant = regime.dominant_state
    probability = float(regime.probabilities.get(dominant) or 0.0)
    labels = {
        "TREND_UP": "趋势向上",
        "THEME_ROTATION": "题材轮动",
        "RANGE": "区间震荡",
        "PANIC_RECOVERY": "恐慌修复",
        "RISK_OFF": "风险收缩",
    }
    state = "ACTIVE" if probability >= 0.50 else "PREPARE"
    action = (
        "CONTROLLED_RISK_ON"
        if dominant in {"TREND_UP", "THEME_ROTATION"}
        else "SELECTIVE_PROBES"
        if dominant == "PANIC_RECOVERY"
        else "CASH_FIRST"
    )
    return TradeHypothesis(
        hypothesis_key=f"MARKET:{trade_date.isoformat()}:A_SHARE",
        run_uid=run_uid,
        trade_date=trade_date.isoformat(),
        scope_type="MARKET",
        scope_code="A_SHARE",
        scope_name="A股整体",
        direction="CONTEXT",
        state=state,
        probability=round(probability, 6),
        prior_probability=round(probability, 6),
        probability_kind="REGIME_MIXTURE",
        confidence=round(regime.confidence, 6),
        score=round(probability, 8),
        horizon_minutes=240,
        alpha_half_life_minutes=60,
        proposed_action=action,
        max_position_weight=round(regime.risk_asset_cap, 6),
        theme_code="",
        role="MARKET_CONTEXT",
        thesis=(
            f"当前最高概率状态为{labels.get(dominant, dominant)}，"
            f"组合风险仓位按全部状态概率加权为{regime.risk_asset_cap:.1%}"
        ),
        counter_thesis="盘中宽度、成交和风险事件若反向变化，市场假设必须即时重估",
        supporting_evidence=tuple(regime.evidence),
        opposing_evidence=("日级状态不是盘中结论，开盘后必须用实时市场宽度复核",),
        triggers=("实时上涨宽度、等权收益和核心板块形成持续确认",),
        invalidations=("跌停比例、波动或新闻风险显著抬升",),
        strategy_keys=(),
        feature_time=decision_at,
        valid_until=decision_at + timedelta(days=2),
        source_forecast_count=0,
    )


def strategy_weights_for_regime(
    regime: RegimeProbabilities,
) -> dict[str, float]:
    state_weights = {
        "TREND_UP": {
            "right_side_trend": 1.30,
            "theme_diffusion": 1.10,
            "low_base_ignition": 0.70,
            "event_drift": 0.95,
            "quality_momentum": 0.95,
            "intraday_surprise": 0.80,
            "oversold_reversal": 0.25,
            "weak_market_structural_mainline": 0.20,
        },
        "THEME_ROTATION": {
            "right_side_trend": 1.05,
            "theme_diffusion": 1.35,
            "low_base_ignition": 1.25,
            "event_drift": 1.00,
            "quality_momentum": 0.65,
            "intraday_surprise": 1.15,
            "oversold_reversal": 0.55,
            "weak_market_structural_mainline": 0.85,
        },
        "RANGE": {
            "right_side_trend": 0.60,
            "theme_diffusion": 0.70,
            "low_base_ignition": 0.80,
            "event_drift": 0.85,
            "quality_momentum": 0.95,
            "intraday_surprise": 0.95,
            "oversold_reversal": 1.00,
            "weak_market_structural_mainline": 1.20,
        },
        "PANIC_RECOVERY": {
            "right_side_trend": 0.45,
            "theme_diffusion": 0.75,
            "low_base_ignition": 1.00,
            "event_drift": 0.70,
            "quality_momentum": 0.55,
            "intraday_surprise": 1.00,
            "oversold_reversal": 1.35,
            "weak_market_structural_mainline": 1.35,
        },
        "RISK_OFF": {
            "right_side_trend": 0.25,
            "theme_diffusion": 0.35,
            "low_base_ignition": 0.35,
            "event_drift": 0.45,
            "quality_momentum": 0.65,
            "intraday_surprise": 0.35,
            "oversold_reversal": 0.30,
            "weak_market_structural_mainline": 0.55,
        },
    }
    result: dict[str, float] = defaultdict(float)
    for state, probability in regime.probabilities.items():
        for strategy_key, weight in state_weights.get(state, {}).items():
            result[strategy_key] += float(probability) * weight
    return {
        key: round(max(0.05, value), 6)
        for key, value in result.items()
    }


def apply_evidence(
    hypothesis: TradeHypothesis,
    *,
    observed_at: datetime,
    evidence_type: str,
    source: str,
    summary: str,
    strength: float,
    polarity: str,
    payload: dict[str, Any] | None = None,
    hard_invalidation: bool = False,
    trigger_confirmed: bool = False,
) -> tuple[TradeHypothesis, HypothesisEvidence]:
    before_probability = hypothesis.probability
    before_state = hypothesis.state
    signed_strength = abs(float(strength))
    if polarity.upper() == "NEGATIVE":
        signed_strength *= -1.0
    elif polarity.upper() == "NEUTRAL":
        signed_strength = 0.0
    after_probability = _clamp(
        _logistic(_logit(before_probability) + signed_strength)
    )
    if hard_invalidation:
        after_probability = min(after_probability, 0.18)
        after_state = "INVALIDATED"
        action = "EXIT_OR_AVOID"
        max_weight = 0.0
    elif trigger_confirmed and after_probability >= 0.62:
        after_state = "ACTIVE"
        if hypothesis.max_position_weight <= 0:
            action = "ALERT_ONLY"
        elif hypothesis.max_position_weight <= 0.025:
            action = "PAPER_PROBE"
        else:
            action = "BUY_OR_HOLD"
        max_weight = hypothesis.max_position_weight
    elif after_probability >= 0.68:
        after_state = "TRIGGER_READY"
        action = "WAIT_PRICE_CONFIRM"
        max_weight = hypothesis.max_position_weight
    elif after_probability >= 0.50:
        after_state = "PREPARE"
        action = "WATCH_CLOSELY"
        max_weight = hypothesis.max_position_weight
    elif after_probability >= 0.34:
        after_state = "WEAKEN"
        action = "NO_NEW_BUY"
        max_weight = 0.0
    else:
        after_state = "INVALIDATED"
        action = "EXIT_OR_AVOID"
        max_weight = 0.0
    supporting = hypothesis.supporting_evidence
    opposing = hypothesis.opposing_evidence
    if polarity.upper() == "POSITIVE":
        supporting = (summary,) + supporting
    elif polarity.upper() == "NEGATIVE":
        opposing = (summary,) + opposing
    updated = replace(
        hypothesis,
        state=after_state,
        probability=round(after_probability, 6),
        proposed_action=action,
        max_position_weight=max_weight,
        supporting_evidence=supporting[:10],
        opposing_evidence=opposing[:10],
    )
    event = HypothesisEvidence(
        hypothesis_key=hypothesis.hypothesis_key,
        observed_at=observed_at,
        evidence_type=evidence_type,
        polarity=polarity.upper(),
        strength=round(abs(float(strength)), 6),
        source=source,
        summary=summary,
        probability_before=round(before_probability, 6),
        probability_after=round(after_probability, 6),
        state_before=before_state,
        state_after=after_state,
        payload=payload or {},
    )
    return updated, event
