"""Point-in-time sector preheat and leadership signals.

The module uses only QMT membership snapshots and QMT daily bars that were
available no later than ``decision_at``.  It deliberately keeps historical
research labelling separate from production decisions: production callers
cannot ask it to use a membership snapshot captured after the decision time.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.kline_data import get_kline_engine
from server.trading_v2.candidate_context import apply_candidate_context


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "strategies" / "sector_preheat_v1.json"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _mean(values: Iterable[float]) -> float:
    numbers = list(values)
    return sum(numbers) / len(numbers) if numbers else 0.0


def _canonical_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def load_sector_preheat_config() -> dict[str, Any]:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "probiga.sector-preheat-strategy.v1":
        raise ValueError("unsupported sector preheat strategy schema")
    if not str(payload.get("strategy_version") or ""):
        raise ValueError("sector preheat strategy_version is required")
    return payload


def sector_preheat_config_hash() -> str:
    return _canonical_hash(load_sector_preheat_config())


def _daily_limit_threshold(stock_code: str, short_name: str) -> float:
    name = str(short_name or "").upper()
    if "ST" in name:
        return 4.8
    if str(stock_code).startswith(("300", "301", "688")):
        return 19.5
    return 9.7


def _stock_features(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if len(rows) < 6:
        return None
    ordered = sorted(rows, key=lambda item: str(item.get("trade_date") or ""))
    latest = ordered[-1]
    close = _float(latest.get("close"))
    if close <= 0:
        return None
    closes = [_float(item.get("close")) for item in ordered]
    if any(value <= 0 for value in closes[-6:]):
        return None
    pre_close = _float(latest.get("pre_close"), closes[-2])
    return_1d = (
        _float(latest.get("change_pct"))
        if latest.get("change_pct") is not None
        else (close / pre_close - 1.0) * 100.0
    )
    return_3d = (close / closes[-4] - 1.0) * 100.0
    return_5d = (close / closes[-6] - 1.0) * 100.0
    ma5 = _mean(closes[-5:])
    ma10 = _mean(closes[-10:]) if len(closes) >= 10 else ma5
    amounts = [_float(item.get("amount")) for item in ordered]
    comparison_amounts = [
        value for value in amounts[-6:-1] if value > 0
    ]
    amount = amounts[-1]
    amount_ratio = (
        amount / median(comparison_amounts)
        if amount > 0 and comparison_amounts
        else 0.0
    )
    code = str(latest.get("stock_code") or "").zfill(6)
    name = str(latest.get("short_name") or code)
    limit_threshold = _daily_limit_threshold(code, name)
    prior_rows = list(reversed(ordered[:-1][-3:]))
    recent_prior_returns_1d = [
        _float(item.get("change_pct")) for item in prior_rows
    ]
    recent_prior_limit_flags = [
        _float(item.get("change_pct")) >= limit_threshold
        for item in prior_rows
    ]
    recent_returns_1d = [
        _float(item.get("change_pct"))
        for item in ordered[-3:]
    ]
    high = _float(latest.get("high"), close)
    low = _float(latest.get("low"), close)
    close_location = (
        _clamp((close - low) / (high - low), 0.0, 1.0)
        if high > low
        else 1.0
    )
    return {
        "stock_code": code,
        "short_name": name,
        "trade_date": str(latest.get("trade_date") or "")[:10],
        "close": close,
        "pre_close": pre_close,
        "return_1d_pct": return_1d,
        "return_3d_pct": return_3d,
        "return_5d_pct": return_5d,
        "above_ma5": close >= ma5,
        "above_ma10": close >= ma10,
        "ma5": ma5,
        "ma10": ma10,
        "amount": amount,
        "amount_ratio_5": amount_ratio,
        "near_limit_up": return_1d >= limit_threshold,
        "recent_prior_returns_1d_pct": recent_prior_returns_1d,
        "recent_prior_limit_flags": recent_prior_limit_flags,
        "recent_returns_1d_pct": recent_returns_1d,
        "positive_sessions_3": sum(
            value > 0.0 for value in recent_returns_1d
        ),
        "close_location": close_location,
        "history_sessions": len(ordered),
        "st_or_special_treatment": "ST" in name.upper(),
    }


def _is_orderly_right_side_startup(
    feature: dict[str, Any],
    *,
    config: dict[str, Any],
) -> bool:
    """Recognise a tradable right-side launch without treating it as chasing.

    A continuous, volume-backed advance is materially different from a
    one-day vertical spike.  The old implementation applied the same 1/3-day
    extension ceiling to both and therefore hid valid launches such as a
    three-session stair-step breakout.
    """
    rule = dict(config.get("right_side_startup") or {})
    if not bool(rule.get("enabled", True)):
        return False
    return (
        not bool(feature.get("near_limit_up"))
        and not bool(feature.get("st_or_special_treatment"))
        and int(feature.get("history_sessions") or 0)
        >= int(rule.get("minimum_history_sessions", 10))
        and int(feature.get("positive_sessions_3") or 0)
        >= int(rule.get("minimum_positive_sessions_3", 3))
        and float(feature.get("return_1d_pct") or 0.0)
        >= float(rule.get("minimum_return_1d_pct", 2.0))
        and float(feature.get("return_1d_pct") or 0.0)
        <= float(rule.get("maximum_return_1d_pct", 9.2))
        and float(feature.get("return_3d_pct") or 0.0)
        >= float(rule.get("minimum_return_3d_pct", 6.0))
        and float(feature.get("return_3d_pct") or 0.0)
        <= float(rule.get("maximum_return_3d_pct", 25.0))
        and float(feature.get("amount_ratio_5") or 0.0)
        >= float(rule.get("minimum_amount_ratio_5", 1.2))
        and float(feature.get("close_location") or 0.0)
        >= float(rule.get("minimum_close_location", 0.7))
        and (
            not bool(rule.get("require_above_ma5", True))
            or bool(feature.get("above_ma5"))
        )
        and (
            not bool(rule.get("require_above_ma10", True))
            or bool(feature.get("above_ma10"))
        )
    )


def _sector_metrics(
    member_codes: set[str],
    features: dict[str, dict[str, Any]],
    config: dict[str, Any],
    *,
    sector_type: str,
    market_return_1d_pct: float,
    market_return_5d_pct: float,
) -> dict[str, Any] | None:
    available = [features[code] for code in member_codes if code in features]
    member_count = len(member_codes)
    available_count = len(available)
    type_floor_key = (
        "minimum_industry_members"
        if sector_type == "industry"
        else "minimum_concept_members"
    )
    member_floor = int(
        config.get(type_floor_key, config["minimum_sector_members"])
    )
    if member_count < member_floor:
        return None
    coverage = available_count / member_count if member_count else 0.0
    if (
        available_count < member_floor
        or coverage < float(config["minimum_member_coverage"])
    ):
        return None

    positive_pct = (
        sum(item["return_1d_pct"] > 0 for item in available)
        / available_count
        * 100.0
    )
    above_ma5_pct = (
        sum(bool(item["above_ma5"]) for item in available)
        / available_count
        * 100.0
    )
    average_1d = _mean(item["return_1d_pct"] for item in available)
    average_3d = _mean(item["return_3d_pct"] for item in available)
    average_5d = _mean(item["return_5d_pct"] for item in available)
    previous_returns = [
        item["recent_prior_returns_1d_pct"][0]
        for item in available
        if item["recent_prior_returns_1d_pct"]
    ]
    previous_positive_pct = (
        sum(value > 0 for value in previous_returns)
        / len(previous_returns)
        * 100.0
        if previous_returns
        else 0.0
    )
    previous_average_1d = _mean(previous_returns)
    breadth_acceleration = positive_pct - previous_positive_pct
    momentum_acceleration = average_1d - previous_average_1d
    relative_return_1d = average_1d - market_return_1d_pct
    relative_return_5d = average_5d - market_return_5d_pct
    amount_ratios = [
        item["amount_ratio_5"]
        for item in available
        if item["amount_ratio_5"] > 0
    ]
    median_amount_ratio = median(amount_ratios) if amount_ratios else 0.0
    limit_count = sum(bool(item["near_limit_up"]) for item in available)
    strong_count = sum(item["return_1d_pct"] >= 3.0 for item in available)

    breadth_score = positive_pct
    trend_score = above_ma5_pct
    momentum_score = _clamp(
        50.0 + average_1d * 8.0 + average_3d * 3.0 + average_5d * 1.0
    )
    amount_score = _clamp(50.0 + (median_amount_ratio - 1.0) * 50.0)
    leadership_score = _clamp(
        strong_count / available_count * 150.0
        + limit_count / available_count * 350.0
    )
    weights = config["factor_weights"]
    score = (
        breadth_score * float(weights["positive_breadth"])
        + trend_score * float(weights["above_ma5_breadth"])
        + momentum_score * float(weights["sector_momentum"])
        + amount_score * float(weights["amount_expansion"])
        + leadership_score * float(weights["leadership"])
    )
    thresholds = config["sector_thresholds"]
    cooldown_sessions = int(
        thresholds.get("post_spike_cooldown_sessions") or 0
    )
    prior_sector_returns: list[float] = []
    prior_sector_limit_ratios: list[float] = []
    prior_sector_positive_breadths: list[float] = []
    for lag in range(cooldown_sessions):
        lag_returns = [
            item["recent_prior_returns_1d_pct"][lag]
            for item in available
            if len(item["recent_prior_returns_1d_pct"]) > lag
        ]
        lag_limits = [
            item["recent_prior_limit_flags"][lag]
            for item in available
            if len(item["recent_prior_limit_flags"]) > lag
        ]
        if lag_returns:
            prior_sector_returns.append(_mean(lag_returns))
            prior_sector_positive_breadths.append(
                sum(value > 0 for value in lag_returns)
                / len(lag_returns)
                * 100.0
            )
        if lag_limits:
            prior_sector_limit_ratios.append(
                sum(bool(value) for value in lag_limits) / len(lag_limits)
            )
    maximum_average_1d = float(
        thresholds.get("maximum_average_return_1d_pct", 100.0)
    )
    maximum_limit_ratio = float(
        thresholds.get("maximum_limit_up_ratio", 1.0)
    )
    recent_spike = (
        any(value > maximum_average_1d for value in prior_sector_returns)
        or any(
            value >= maximum_limit_ratio
            for value in prior_sector_limit_ratios
        )
    )
    limit_up_ratio = limit_count / available_count
    acceleration_confirmed = (
        breadth_acceleration
        >= float(
            thresholds.get(
                "ignition_minimum_breadth_acceleration_pct",
                thresholds.get("minimum_breadth_acceleration_pct", -100.0),
            )
        )
        or momentum_acceleration
        >= float(
            thresholds.get(
                "ignition_minimum_momentum_acceleration_pct",
                thresholds.get("minimum_momentum_acceleration_pct", -100.0),
            )
        )
    )
    prior_strength_sessions = sum(
        breadth
        >= float(thresholds.get("minimum_positive_breadth_pct", 52.0))
        and prior_return
        >= float(thresholds.get("minimum_average_return_1d_pct", 0.35))
        for breadth, prior_return in zip(
            prior_sector_positive_breadths,
            prior_sector_returns,
        )
    )
    ignition_evidence = {
        "positive_breadth": positive_pct
        >= float(
            thresholds.get(
                "ignition_minimum_positive_breadth_pct",
                thresholds["minimum_positive_breadth_pct"],
            )
        ),
        "above_ma5_breadth": above_ma5_pct
        >= float(
            thresholds.get(
                "ignition_minimum_above_ma5_pct",
                thresholds["minimum_above_ma5_pct"],
            )
        ),
        "positive_return": average_1d
        >= float(
            thresholds.get(
                "ignition_minimum_average_return_1d_pct",
                thresholds["minimum_average_return_1d_pct"],
            )
        ),
        "recoverable_5d_base": average_5d
        >= float(
            thresholds.get(
                "ignition_minimum_average_return_5d_pct",
                thresholds.get("minimum_average_return_5d_pct", -100.0),
            )
        ),
        "relative_strength": relative_return_1d
        >= float(
            thresholds.get(
                "ignition_minimum_relative_return_1d_pct",
                thresholds.get("minimum_relative_return_1d_pct", -100.0),
            )
        ),
        "amount_confirmation": median_amount_ratio
        >= float(
            thresholds.get(
                "ignition_minimum_median_amount_ratio_5",
                thresholds.get("minimum_median_amount_ratio_5", 0.0),
            )
        ),
        "acceleration_or_persistence": (
            acceleration_confirmed or prior_strength_sessions >= 1
        ),
    }
    ignition_evidence_count = sum(ignition_evidence.values())
    ignition_qualifies = (
        score >= float(thresholds.get("ignition_score", 100.0))
        and positive_pct
        >= float(thresholds.get("minimum_positive_breadth_pct", 52.0))
        and average_1d
        >= float(
            thresholds.get(
                "ignition_minimum_average_return_1d_pct",
                thresholds["minimum_average_return_1d_pct"],
            )
        )
        and relative_return_1d
        >= float(
            thresholds.get(
                "ignition_minimum_relative_return_1d_pct",
                thresholds.get("minimum_relative_return_1d_pct", -100.0),
            )
        )
        and (
            acceleration_confirmed or prior_strength_sessions >= 1
        )
        and ignition_evidence_count
        >= int(thresholds.get("ignition_minimum_evidence_count", 7))
    )
    first_ignition_spike = (
        average_1d > maximum_average_1d
        and not recent_spike
        and average_5d
        <= float(
            thresholds.get(
                "first_ignition_maximum_average_return_5d_pct",
                thresholds["maximum_average_return_5d_pct"],
            )
        )
        and limit_up_ratio < maximum_limit_ratio
        and ignition_qualifies
    )
    legacy_overheated = (
        average_1d > maximum_average_1d
        or limit_up_ratio >= maximum_limit_ratio
        or average_5d
        > float(thresholds["maximum_average_return_5d_pct"])
    )
    overheated = (
        (average_1d > maximum_average_1d and not first_ignition_spike)
        or limit_up_ratio >= maximum_limit_ratio
        or average_5d
        > float(thresholds["maximum_average_return_5d_pct"])
    )
    cooling = (
        recent_spike
        and average_5d
        >= float(
            thresholds.get(
                "cooldown_minimum_average_return_5d_pct",
                100.0,
            )
        )
    )
    qualifies = (
        positive_pct >= float(thresholds["minimum_positive_breadth_pct"])
        and above_ma5_pct >= float(thresholds["minimum_above_ma5_pct"])
        and average_1d >= float(thresholds["minimum_average_return_1d_pct"])
        and average_5d
        >= float(
            thresholds.get(
                "minimum_average_return_5d_pct",
                -100.0,
            )
        )
        and relative_return_1d
        >= float(
            thresholds.get(
                "minimum_relative_return_1d_pct",
                -100.0,
            )
        )
        and median_amount_ratio
        >= float(
            thresholds.get(
                "minimum_median_amount_ratio_5",
                0.0,
            )
        )
        and (
            breadth_acceleration
            >= float(
                thresholds.get(
                    "minimum_breadth_acceleration_pct",
                    -100.0,
                )
            )
            or momentum_acceleration
            >= float(
                thresholds.get(
                    "minimum_momentum_acceleration_pct",
                    -100.0,
                )
            )
        )
    )
    if overheated:
        stage = "OVERHEATED"
        stage_reason = "OVERHEATED_AFTER_EXTENSION"
    elif cooling:
        stage = "COOLDOWN"
        stage_reason = "POST_SPIKE_COOLDOWN"
    elif first_ignition_spike:
        stage = "PREHEAT"
        stage_reason = "FIRST_IGNITION_OBSERVATION"
    elif qualifies and score >= float(thresholds["confirmed_score"]):
        stage = "CONFIRMED"
        stage_reason = "STRICT_CONFIRMATION"
    elif (
        ignition_qualifies
        and prior_strength_sessions >= 1
        and score >= float(thresholds["confirmed_score"])
    ):
        stage = "CONFIRMED"
        stage_reason = "PERSISTENT_IGNITION_CONFIRMED"
    elif (
        qualifies and score >= float(thresholds["preheat_score"])
    ) or ignition_qualifies:
        stage = "PREHEAT"
        stage_reason = (
            "WEAK_TO_STRONG_IGNITION"
            if ignition_qualifies
            else "STRICT_PREHEAT"
        )
    else:
        stage = "NEUTRAL"
        stage_reason = "EVIDENCE_NOT_SUFFICIENT"
    if legacy_overheated:
        execution_stage = "OVERHEATED"
        execution_stage_reason = "LEGACY_OVERHEATED"
    elif cooling:
        execution_stage = "COOLDOWN"
        execution_stage_reason = "LEGACY_POST_SPIKE_COOLDOWN"
    elif qualifies and score >= float(thresholds["confirmed_score"]):
        execution_stage = "CONFIRMED"
        execution_stage_reason = "LEGACY_STRICT_CONFIRMATION"
    elif qualifies and score >= float(thresholds["preheat_score"]):
        execution_stage = "PREHEAT"
        execution_stage_reason = "LEGACY_STRICT_PREHEAT"
    else:
        execution_stage = "NEUTRAL"
        execution_stage_reason = "LEGACY_EVIDENCE_NOT_SUFFICIENT"
    return {
        "member_count": member_count,
        "available_count": available_count,
        "coverage_pct": round(coverage * 100.0, 2),
        "positive_breadth_pct": round(positive_pct, 2),
        "above_ma5_pct": round(above_ma5_pct, 2),
        "average_return_1d_pct": round(average_1d, 3),
        "average_return_3d_pct": round(average_3d, 3),
        "average_return_5d_pct": round(average_5d, 3),
        "market_return_1d_pct": round(market_return_1d_pct, 3),
        "market_return_5d_pct": round(market_return_5d_pct, 3),
        "relative_return_1d_pct": round(relative_return_1d, 3),
        "relative_return_5d_pct": round(relative_return_5d, 3),
        "previous_positive_breadth_pct": round(
            previous_positive_pct,
            2,
        ),
        "breadth_acceleration_pct": round(
            breadth_acceleration,
            2,
        ),
        "momentum_acceleration_pct": round(
            momentum_acceleration,
            3,
        ),
        "median_amount_ratio_5": round(median_amount_ratio, 3),
        "limit_up_count": limit_count,
        "strong_stock_count": strong_count,
        "recent_peak_average_return_1d_pct": round(
            max(prior_sector_returns, default=0.0),
            3,
        ),
        "recent_peak_limit_up_ratio": round(
            max(prior_sector_limit_ratios, default=0.0),
            4,
        ),
        "prior_strength_sessions": prior_strength_sessions,
        "ignition_evidence_count": ignition_evidence_count,
        "ignition_evidence": ignition_evidence,
        "first_ignition_spike": first_ignition_spike,
        "score": round(score, 2),
        "stage": stage,
        "stage_reason": stage_reason,
        "execution_stage": execution_stage,
        "execution_stage_reason": execution_stage_reason,
        "_members": available,
    }


def _candidate_score(
    feature: dict[str, Any],
    *,
    sector_score: float,
    rank: int,
    candidate_count: int,
    config: dict[str, Any],
) -> float:
    rank_score = (
        100.0
        if candidate_count <= 1
        else 100.0 * (candidate_count - rank - 1) / (candidate_count - 1)
    )
    momentum_score = _clamp(
        50.0
        + feature["return_1d_pct"] * 4.0
        + feature["return_3d_pct"] * 2.0
        + feature["return_5d_pct"]
    )
    volume_score = _clamp(
        50.0 + (feature["amount_ratio_5"] - 1.0) * 50.0
    )
    liquidity_floor = float(
        config["candidate_thresholds"]["minimum_amount_cny"]
    )
    liquidity_score = _clamp(
        50.0
        + math.log10(max(feature["amount"], 1.0) / liquidity_floor) * 25.0
    )
    trend_score = 75.0 if feature["above_ma5"] else 30.0
    score = (
        sector_score * 0.35
        + (rank_score * 0.55 + momentum_score * 0.45) * 0.25
        + volume_score * 0.15
        + trend_score * 0.15
        + liquidity_score * 0.10
    )
    if feature["near_limit_up"]:
        score -= 5.0
    return round(_clamp(score), 2)


def _candidate_pre_entry_eligible(
    feature: dict[str, Any],
    *,
    config: dict[str, Any],
) -> bool:
    """Return whether a stock can be used as an executable sector proxy.

    This check deliberately excludes model score and market regime.  It is
    used while building the leadership ladder so an untradeable limit-up/ST
    leader remains visible without crowding out a liquid, lower-extension
    core stock from the candidate list.
    """
    thresholds = config["candidate_thresholds"]
    orderly_right_side = bool(
        feature.get("orderly_right_side_startup")
    )
    return (
        not feature["near_limit_up"]
        and not feature["st_or_special_treatment"]
        and feature["amount"]
        >= float(thresholds["minimum_amount_cny"])
        and feature["above_ma5"]
        and feature["amount_ratio_5"]
        >= float(thresholds.get("minimum_amount_ratio_5", 0.0))
        and feature["return_1d_pct"]
        >= float(thresholds.get("minimum_return_1d_pct", -100.0))
        and (
            orderly_right_side
            or feature["return_1d_pct"]
            <= float(thresholds.get("maximum_return_1d_pct", 100.0))
        )
        and (
            orderly_right_side
            or feature["return_3d_pct"]
            <= float(thresholds["maximum_return_3d_pct"])
        )
        and feature["return_5d_pct"]
        <= float(thresholds["maximum_return_5d_pct"])
    )


def _candidate_signal(
    feature: dict[str, Any],
    *,
    sector: dict[str, Any],
    raw_score: float,
    role: str,
    rank: int,
    market_regime: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    thresholds = config["candidate_thresholds"]
    execution_confirmation = config.get("execution_confirmation") or {}
    sector_stage_reason = str(sector.get("stage_reason") or "")
    strict_preheat_entry = (
        sector["stage"] == "PREHEAT"
        and sector_stage_reason
        in {"STRICT_PREHEAT", "LEGACY_STRICT_PREHEAT"}
        and bool(
            execution_confirmation.get(
                "allow_strict_preheat_entry",
                True,
            )
        )
    )
    ignition_preheat_entry = (
        sector["stage"] == "PREHEAT"
        and sector_stage_reason
        in {
            "FIRST_IGNITION_OBSERVATION",
            "WEAK_TO_STRONG_IGNITION",
        }
        and bool(
            execution_confirmation.get(
                "allow_ignition_preheat_entry",
                False,
            )
        )
    )
    orderly_right_side_entry = (
        bool(feature.get("orderly_right_side_startup"))
        and sector["stage"] in {"PREHEAT", "CONFIRMED"}
        and bool(
            (config.get("right_side_startup") or {}).get(
                "promote_discovery_to_execution",
                True,
            )
        )
    )
    confirmed_entry = (
        sector["stage"] == "CONFIRMED"
        and (
            sector_stage_reason != "PERSISTENT_IGNITION_CONFIRMED"
            or bool(
                execution_confirmation.get(
                    "allow_persistent_ignition_entry",
                    False,
                )
            )
        )
    )
    close = float(feature["close"])
    entry_high = close * (1.0 + float(thresholds["no_chase_pct"]) / 100.0)
    initial_stop = close * (
        1.0 + float(thresholds["initial_stop_pct"]) / 100.0
    )
    target_1 = close * (
        1.0 + float(thresholds["take_profit_1_pct"]) / 100.0
    )
    target_2 = close * (
        1.0 + float(thresholds["take_profit_2_pct"]) / 100.0
    )
    risk = max(0.000001, entry_high - initial_stop)
    risk_reward = (target_2 - entry_high) / risk
    minimum_risk_reward = float(thresholds["minimum_risk_reward"])
    risk_reward_confirmed = (
        risk_reward + 1e-9 >= minimum_risk_reward
    )
    orderly_right_side = bool(
        feature.get("orderly_right_side_startup")
    )
    extended = (
        not orderly_right_side
        and (
            feature["return_3d_pct"]
            > float(thresholds["maximum_return_3d_pct"])
            or feature["return_5d_pct"]
            > float(thresholds["maximum_return_5d_pct"])
        )
    )
    liquid = feature["amount"] >= float(thresholds["minimum_amount_cny"])
    volume_confirmed = feature["amount_ratio_5"] >= float(
        thresholds.get("minimum_amount_ratio_5", 0.0)
    )
    return_1d_in_range = (
        feature["return_1d_pct"]
        >= float(thresholds.get("minimum_return_1d_pct", -100.0))
        and (
            orderly_right_side
            or feature["return_1d_pct"]
            <= float(thresholds.get("maximum_return_1d_pct", 100.0))
        )
    )
    eligible_role = (
        rank <= 2
        or role in {"龙头", "中军", "低位核心"}
    )
    sector_entry_stage = (
        confirmed_entry
        or strict_preheat_entry
        or ignition_preheat_entry
        or orderly_right_side_entry
    )
    ready = (
        raw_score >= float(thresholds["ready_score"])
        and sector_entry_stage
        and not feature["near_limit_up"]
        and not extended
        and not feature["st_or_special_treatment"]
        and liquid
        and feature["above_ma5"]
        and volume_confirmed
        and return_1d_in_range
        and eligible_role
        and risk_reward_confirmed
    )
    regime = str(market_regime or "").upper()
    regime_multiplier = float(
        (config.get("market_regime_entry_multiplier") or {}).get(
            regime,
            1.0,
        )
    )
    blocked_regime = regime_multiplier <= 0.0
    panic_recovery_entry_mode = str(
        execution_confirmation.get(
            "panic_recovery_entry_mode",
            "SHADOW_ONLY",
        )
    ).upper()
    panic_recovery_allowed = (
        regime != "PANIC_RECOVERY"
        or (
            panic_recovery_entry_mode == "PAPER_ACTIVE"
            and (
                (
                    sector["stage"] == "CONFIRMED"
                    and not bool(sector.get("first_ignition_spike"))
                    and bool(
                        execution_confirmation.get(
                            "allow_confirmed_in_panic_recovery",
                            False,
                        )
                    )
                )
                or (
                    orderly_right_side
                    and bool(
                        execution_confirmation.get(
                            "allow_orderly_right_side_in_panic_recovery",
                            True,
                        )
                    )
                )
            )
            and float(sector["score"])
            >= float(
                execution_confirmation.get(
                    "panic_recovery_minimum_sector_score",
                    100.0,
                )
            )
            and raw_score
            >= float(
                execution_confirmation.get(
                    "panic_recovery_minimum_candidate_score",
                    100.0,
                )
            )
            and role
            in set(
                execution_confirmation.get(
                    "panic_recovery_allowed_roles",
                    [],
                )
            )
        )
    )
    if blocked_regime:
        direction, status, gate_status = "HOLD", "BLOCKED", "BLOCK"
        gate_reason = "当前市场风险状态禁止新增风险仓位"
    elif feature["st_or_special_treatment"]:
        direction, status, gate_status = "HOLD", "BLOCKED", "BLOCK"
        gate_reason = "ST或特别处理股票不进入板块预热模拟买入"
    elif feature["near_limit_up"]:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = "收盘接近涨停，保留板块龙头观察但禁止次日盲目追板"
    elif extended:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = "近3至5日涨幅过大，等待回踩或换手确认"
    elif not liquid:
        direction, status, gate_status = "HOLD", "WATCH", "REDUCE"
        gate_reason = "成交额不足，模拟成交可能失真"
    elif not feature["above_ma5"]:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = "个股尚未站上5日均线，只观察板块强度，不提前买入"
    elif not volume_confirmed:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = "个股量能没有同步放大，等待资金确认后再买"
    elif not return_1d_in_range:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = "个股当日强度不在可执行区间，避免买弱或追高"
    elif not risk_reward_confirmed:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = (
            f"按实际入场、保护位和目标价计算的盈亏比"
            f"{risk_reward:.2f}低于门槛{minimum_risk_reward:.2f}"
        )
    elif role == "低位替补":
        direction, status, gate_status = "BUY", "WATCH", "PASS"
        gate_reason = "原龙头和中军不可执行，低位替补先进入影子跟踪，不自动开仓"
    elif not eligible_role:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = "该股票只作为强度观察锚点，系统将递补可执行中军"
    elif not sector_entry_stage:
        direction, status, gate_status = "BUY", "WATCH", "PASS"
        gate_reason = "板块属于新增预判信号，先做影子跟踪，等待前向样本验证后再开仓"
    elif not panic_recovery_allowed:
        direction, status, gate_status = "BUY", "WATCH", "REDUCE"
        gate_reason = "市场处于恐慌修复，候选进入影子跟踪，不自动占用模拟盘仓位"
    elif ready:
        direction, status, gate_status = "BUY", "READY", "PASS"
        gate_reason = (
            f"{sector['sector_name']}处于"
            f"{'预热' if sector['stage'] == 'PREHEAT' else '确认'}阶段，"
            f"{role}满足次日价格和板块强度条件"
        )
        if regime_multiplier < 1.0:
            gate_reason += (
                f"；当前市场仅按{regime_multiplier:.0%}风险额度试仓"
            )
    else:
        direction, status, gate_status = "BUY", "WATCH", "PASS"
        gate_reason = "板块已进入观察范围，个股强度尚未达到条件买入线"

    theme_code = str(sector["sector_code"])[:80]
    evidence = [
        {
            "module": "sector_preheat",
            "text": (
                f"{sector['sector_name']}：板块分{sector['score']:.2f}，"
                f"上涨宽度{sector['positive_breadth_pct']:.2f}%，"
                f"站上MA5占比{sector['above_ma5_pct']:.2f}%"
            ),
            "source": "qmt_membership+qmt_daily_kline",
        },
        {
            "module": "sector_leadership",
            "text": (
                f"{role}第{rank}位：1日{feature['return_1d_pct']:.2f}%，"
                f"3日{feature['return_3d_pct']:.2f}%，"
                f"5日{feature['return_5d_pct']:.2f}%，"
                f"量比{feature['amount_ratio_5']:.2f}"
            ),
            "source": "qmt_daily_kline",
        },
    ]
    return {
        "stock_code": feature["stock_code"],
        "stock_name": feature["short_name"],
        "strategy_key": "sector_preheat",
        "strategy_name": "板块预热与龙头梯队",
        "strategy_version": config["strategy_version"],
        "market_state": str(market_regime or ""),
        "signal_direction": direction,
        "signal_status": status,
        "raw_score": raw_score,
        "effective_score": raw_score,
        "model_confidence": raw_score,
        "today_signal": gate_reason,
        "entry_low": round(close, 3),
        "entry_high": round(entry_high, 3),
        "trigger_conditions": [
            f"次日价格不高于{entry_high:.3f}",
            "不是涨停封单或停牌",
            f"{sector['sector_name']}盘中方向仍为上涨",
        ],
        "stop_loss": round(initial_stop, 3),
        "take_profit_1": round(target_1, 3),
        "take_profit_2": round(target_2, 3),
        "no_chase_price": round(entry_high, 3),
        "risk_level": "MEDIUM",
        "risk_reward_ratio": round(risk_reward, 2),
        "gate_status": gate_status,
        "gate_reason": gate_reason,
        "effective_weight": regime_multiplier,
        "weight_detail": {
            "base_weight": 1.0,
            "state_multiplier": regime_multiplier,
            "effective_weight": regime_multiplier,
        },
        "evidence_chain": evidence,
        "data_date": feature["trade_date"],
        "data_quality_score": 100.0,
        "adapter_mode": "qmt_point_in_time_sector_preheat",
        "model_version": config["strategy_version"],
        "theme_code": theme_code,
        "theme_name": sector["sector_name"],
        "theme_type": sector["sector_type"],
        "sector_stage": sector["stage"],
        "sector_stage_reason": sector.get("stage_reason") or "",
        "sector_score": sector["score"],
        "sector_positive_breadth_pct": sector["positive_breadth_pct"],
        "sector_above_ma5_pct": sector["above_ma5_pct"],
        "sector_ignition_evidence_count": sector.get(
            "ignition_evidence_count",
            0,
        ),
        "sector_ignition_evidence": sector.get(
            "ignition_evidence",
            {},
        ),
        "sector_role": role,
        "sector_rank": rank,
        "candidate_return_1d_pct": round(
            feature["return_1d_pct"],
            3,
        ),
        "candidate_return_3d_pct": round(
            feature["return_3d_pct"],
            3,
        ),
        "candidate_return_5d_pct": round(
            feature["return_5d_pct"],
            3,
        ),
        "candidate_amount_ratio_5": round(
            feature["amount_ratio_5"],
            3,
        ),
        "candidate_amount_cny": round(
            feature["amount"],
            2,
        ),
        "candidate_above_ma5": bool(feature["above_ma5"]),
        "candidate_above_ma10": bool(feature.get("above_ma10")),
        "candidate_positive_sessions_3": int(
            feature.get("positive_sessions_3") or 0
        ),
        "candidate_close_location": round(
            float(feature.get("close_location") or 0.0),
            3,
        ),
        "orderly_right_side_startup": orderly_right_side,
        "db_verified": True,
        "db_close": close,
        "db_verification_reason": "国金QMT点时可得日K与成员快照",
    }


def score_sector_preheat(
    *,
    memberships: list[dict[str, Any]],
    bars: list[dict[str, Any]],
    trade_date: str,
    market_regime: str = "",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Pure scoring entrypoint used by production and deterministic tests."""
    config = config or load_sector_preheat_config()
    by_code_date: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in bars:
        code = str(row.get("stock_code") or "").zfill(6)
        day = str(row.get("trade_date") or "")[:10]
        if code and day and day <= trade_date:
            by_code_date[code][day] = dict(row)
    features: dict[str, dict[str, Any]] = {}
    for code, rows in by_code_date.items():
        ordered = [rows[key] for key in sorted(rows)]
        if not ordered or str(ordered[-1].get("trade_date") or "")[:10] != trade_date:
            continue
        feature = _stock_features(ordered)
        if feature:
            feature["orderly_right_side_startup"] = (
                _is_orderly_right_side_startup(
                    feature,
                    config=config,
                )
            )
            features[code] = feature
    market_return_1d_pct = (
        median(
            item["return_1d_pct"]
            for item in features.values()
        )
        if features
        else 0.0
    )
    market_return_5d_pct = (
        median(
            item["return_5d_pct"]
            for item in features.values()
        )
        if features
        else 0.0
    )

    grouped: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in memberships:
        sector_type = str(row.get("sector_type") or "")
        sector_code = str(row.get("sector_code") or "")
        sector_name = str(row.get("sector_name") or sector_code)
        stock_code = str(row.get("stock_code") or "").zfill(6)
        if sector_type and sector_code and stock_code:
            grouped[(sector_type, sector_code, sector_name)].add(stock_code)

    sectors: list[dict[str, Any]] = []
    for (sector_type, raw_code, sector_name), member_codes in grouped.items():
        metrics = _sector_metrics(
            member_codes,
            features,
            config,
            sector_type=sector_type,
            market_return_1d_pct=market_return_1d_pct,
            market_return_5d_pct=market_return_5d_pct,
        )
        if not metrics:
            continue
        prefix = "INDUSTRY" if sector_type == "industry" else "CONCEPT"
        sectors.append(
            {
                "sector_code": f"{prefix}:{raw_code}",
                "raw_sector_code": raw_code,
                "sector_name": sector_name,
                "sector_type": sector_type,
                **metrics,
            }
        )
    sectors.sort(
        key=lambda item: (
            item["stage"] not in {"PREHEAT", "CONFIRMED"},
            -float(item["score"]),
            item["sector_code"],
        )
    )
    discovery_hot_pool = [
        item
        for item in sectors
        if item["stage"] in {"PREHEAT", "CONFIRMED"}
    ]
    execution_hot_pool = sorted(
        (
            item
            for item in sectors
            if item["execution_stage"] in {"PREHEAT", "CONFIRMED"}
        ),
        key=lambda item: (
            -float(item["score"]),
            item["sector_code"],
        ),
    )
    hot_theme_matches_by_code: dict[str, list[dict[str, Any]]] = (
        defaultdict(list)
    )
    for item in discovery_hot_pool:
        theme_match = {
            "theme_code": item["sector_code"],
            "theme_name": item["sector_name"],
            "theme_type": item["sector_type"],
            "sector_stage": item["stage"],
            "sector_stage_reason": item.get("stage_reason") or "",
            "sector_score": item["score"],
        }
        for member in item.get("_members") or []:
            hot_theme_matches_by_code[member["stock_code"]].append(
                dict(theme_match)
            )

    maximum_overlap = float(
        config.get("maximum_member_overlap_ratio", 1.0)
    )

    def select_hot(
        pool: list[dict[str, Any]],
        *,
        maximum_count: int,
    ) -> list[dict[str, Any]]:
        selected_hot: list[dict[str, Any]] = []
        for item in pool:
            item_codes = {
                member["stock_code"]
                for member in item.get("_members") or []
            }
            is_duplicate = False
            for selected in selected_hot:
                selected_codes = {
                    member["stock_code"]
                    for member in selected.get("_members") or []
                }
                union = item_codes | selected_codes
                overlap = (
                    len(item_codes & selected_codes) / len(union)
                    if union
                    else 0.0
                )
                if overlap >= maximum_overlap:
                    is_duplicate = True
                    break
            if is_duplicate:
                continue
            selected_hot.append(item)
            if len(selected_hot) >= maximum_count:
                break
        return selected_hot

    execution_hot = select_hot(
        execution_hot_pool,
        maximum_count=int(config["maximum_hot_sectors"]),
    )
    execution_hot_codes = {
        item["sector_code"] for item in execution_hot
    }
    # Discovery is intentionally not member-overlap deduplicated.  Related
    # concepts such as innovative medicine and medical services, or power and
    # utility reform, are distinct information to an observer even when their
    # constituent sets overlap.  Stock-level deduplication below still keeps
    # the page bounded and the execution lane remains unchanged.
    discovery_reason_priority = (
        "PERSISTENT_IGNITION_CONFIRMED",
        "FIRST_IGNITION_OBSERVATION",
        "WEAK_TO_STRONG_IGNITION",
        "STRICT_CONFIRMATION",
        "STRICT_PREHEAT",
    )
    discovery_reason_caps = dict(
        config.get("maximum_discovery_sectors_by_reason") or {}
    )
    discovery_pool_without_execution = [
        item
        for item in discovery_hot_pool
        if item["sector_code"] not in execution_hot_codes
    ]
    discovery_hot: list[dict[str, Any]] = []
    for reason in discovery_reason_priority:
        reason_rows = sorted(
            (
                item
                for item in discovery_pool_without_execution
                if item.get("stage_reason") == reason
            ),
            key=lambda item: (
                -int(item.get("prior_strength_sessions") or 0),
                -float(item["score"]),
                item["sector_code"],
            ),
        )
        reason_cap = int(
            discovery_reason_caps.get(
                reason,
                config.get(
                    "maximum_discovery_hot_sectors",
                    config["maximum_hot_sectors"],
                ),
            )
        )
        for reason_rank, item in enumerate(reason_rows, start=1):
            item["discovery_reason_rank"] = reason_rank
        discovery_hot.extend(reason_rows[:reason_cap])
    discovery_hot = discovery_hot[: int(
        config.get(
            "maximum_discovery_hot_sectors",
            config["maximum_hot_sectors"],
        )
    )]

    signals: list[dict[str, Any]] = []
    signal_sectors = [
        ("EXECUTION", item) for item in execution_hot
    ] + [
        ("DISCOVERY_SHADOW", item) for item in discovery_hot
    ]
    for signal_lane, sector in signal_sectors:
        members = list(sector.get("_members") or [])
        signal_sector = dict(sector)
        if signal_lane == "EXECUTION":
            signal_sector["stage"] = sector["execution_stage"]
            signal_sector["stage_reason"] = sector[
                "execution_stage_reason"
            ]
        ranked = sorted(
            members,
            key=lambda item: (
                item["return_1d_pct"] * 4.0
                + item["return_3d_pct"] * 2.0
                + item["return_5d_pct"]
                + min(3.0, item["amount_ratio_5"]) * 4.0,
                item["amount"],
                item["stock_code"],
            ),
            reverse=True,
        )
        scored: list[tuple[dict[str, Any], float]] = []
        fallback_minimum_score = float(
            config["candidate_thresholds"].get(
                "fallback_minimum_score",
                config["candidate_thresholds"]["minimum_score"],
            )
        )
        if signal_lane == "DISCOVERY_SHADOW":
            fallback_minimum_score = float(
                config["candidate_thresholds"].get(
                    "discovery_minimum_score",
                    fallback_minimum_score,
                )
            )
        for rank, feature in enumerate(ranked):
            score = _candidate_score(
                feature,
                sector_score=float(sector["score"]),
                rank=rank,
                candidate_count=len(ranked),
                config=config,
            )
            if score >= fallback_minimum_score:
                scored.append((feature, score))
        if not scored:
            continue

        selected: list[tuple[dict[str, Any], float, str]] = []
        selected_codes: set[str] = set()

        def select(
            item: tuple[dict[str, Any], float],
            role: str,
        ) -> None:
            code = item[0]["stock_code"]
            if code in selected_codes:
                return
            selected.append((*item, role))
            selected_codes.add(code)

        minimum_score = float(
            config["candidate_thresholds"]["minimum_score"]
        )
        primary_scored = [
            item for item in scored if item[1] >= minimum_score
        ]
        leader = (primary_scored or scored)[0]
        leader_role = (
            "龙头"
            if _candidate_pre_entry_eligible(
                leader[0],
                config=config,
            )
            else "观察龙头"
        )
        select(leader, leader_role)

        remaining_primary = [
            item
            for item in primary_scored
            if item[0]["stock_code"] not in selected_codes
        ]
        if remaining_primary:
            core = max(
                remaining_primary,
                key=lambda item: (
                    item[0]["amount"],
                    item[1],
                    item[0]["stock_code"],
                ),
            )
            core_role = (
                "中军"
                if _candidate_pre_entry_eligible(
                    core[0],
                    config=config,
                )
                else "观察龙头"
            )
            select(core, core_role)

        remaining_primary = [
            item
            for item in primary_scored
            if item[0]["stock_code"] not in selected_codes
        ]
        if remaining_primary:
            if signal_lane == "EXECUTION":
                follower = min(
                    remaining_primary,
                    key=lambda item: (
                        item[0]["return_3d_pct"]
                        > float(
                            config["candidate_thresholds"][
                                "maximum_return_3d_pct"
                            ]
                        ),
                        -item[1],
                    ),
                )
            else:
                follower = max(
                    remaining_primary,
                    key=lambda item: (
                        item[1],
                        -item[0]["return_3d_pct"],
                        item[0]["amount"],
                        item[0]["stock_code"],
                    ),
                )
            select(follower, "跟随")

        executable_fallbacks = [
            item
            for item in scored
            if item[0]["stock_code"] not in selected_codes
            and _candidate_pre_entry_eligible(
                item[0],
                config=config,
            )
        ]
        selected_has_executable = any(
            role in {"龙头", "中军", "低位核心"}
            and _candidate_pre_entry_eligible(
                feature,
                config=config,
            )
            for feature, _score, role in selected
        )
        if not selected_has_executable and executable_fallbacks:
            fallback = max(
                executable_fallbacks,
                key=lambda item: (
                    item[1]
                    - max(0.0, item[0]["return_1d_pct"] - 3.0) * 1.5
                    - max(0.0, item[0]["return_3d_pct"] - 10.0) * 0.5,
                    item[0]["amount_ratio_5"],
                    item[0]["amount"],
                ),
            )
            if len(selected) >= int(
                config["maximum_candidates_per_sector"]
            ):
                selected[-1] = (*fallback, "低位替补")
            else:
                select(fallback, "低位替补")
        selected = selected[: int(config["maximum_candidates_per_sector"])]
        for rank, (feature, score, role) in enumerate(selected, start=1):
            signal = _candidate_signal(
                feature,
                sector=signal_sector,
                raw_score=score,
                role=role,
                rank=rank,
                market_regime=market_regime,
                config=config,
            )
            signal["signal_lane"] = signal_lane
            if signal_lane == "DISCOVERY_SHADOW":
                promote_right_side = (
                    signal["signal_status"] == "READY"
                    and bool(
                        signal.get("orderly_right_side_startup")
                    )
                    and bool(
                        (config.get("right_side_startup") or {}).get(
                            "promote_discovery_to_execution",
                            True,
                        )
                    )
                )
                if promote_right_side:
                    signal["signal_lane"] = "EXECUTION"
                    signal["today_signal"] = (
                        "连续三日换手增强、量价齐升并站上5日和10日线，"
                        "按右侧启动通道进入小仓模拟执行；盘中跌破启动结构立即退出。"
                    )
                    signal["gate_reason"] = signal["today_signal"]
                    signal["effective_weight"] = min(
                        float(signal.get("effective_weight") or 0.0),
                        float(
                            (config.get("right_side_startup") or {}).get(
                                "maximum_opening_weight",
                                0.25,
                            )
                        ),
                    )
                else:
                    signal["signal_direction"] = "HOLD"
                if (
                    signal["signal_status"] == "READY"
                    and not promote_right_side
                ):
                    signal["signal_status"] = "WATCH"
                    signal["gate_status"] = "PASS"
                    signal["gate_reason"] = (
                        "新增预判通道仅做影子跟踪，不参与执行候选和组合抢位"
                    )
                    signal["today_signal"] = signal["gate_reason"]
            signals.append(signal)

    # Full-market discovery is intentionally independent from sector ranking.
    # A stock that is already moving abnormally must remain visible even when
    # its membership is missing, its theme is only warming up, or the global
    # hot-sector cap is full.  This lane discovers first and decides second:
    # limit attacks remain WATCH; only an orderly right-side launch with a
    # validated theme may be promoted to the paper execution lane.
    discovery_rule = dict(config.get("daily_market_discovery") or {})
    if bool(discovery_rule.get("enabled", True)):
        sector_by_code = {
            str(item["sector_code"]): item for item in sectors
        }
        abnormal_features = sorted(
            (
                feature
                for feature in features.values()
                if not feature["st_or_special_treatment"]
                and (
                    feature["near_limit_up"]
                    or (
                        feature["return_1d_pct"]
                        >= float(
                            discovery_rule.get(
                                "minimum_return_1d_pct",
                                7.5,
                            )
                        )
                        and feature["amount"]
                        >= float(
                            discovery_rule.get(
                                "minimum_amount_cny",
                                config["candidate_thresholds"][
                                    "minimum_amount_cny"
                                ],
                            )
                        )
                    )
                )
                and (
                    not feature["near_limit_up"]
                    or feature["amount"]
                    >= float(
                        discovery_rule.get(
                            "minimum_limit_locked_amount_cny",
                            5_000_000.0,
                        )
                    )
                )
            ),
            key=lambda item: (
                item["near_limit_up"],
                item["return_1d_pct"],
                item["amount"],
            ),
            reverse=True,
        )[: int(discovery_rule.get("maximum_candidates", 120))]
        for feature in abnormal_features:
            code = feature["stock_code"]
            theme_matches = sorted(
                hot_theme_matches_by_code.get(code) or [],
                key=lambda item: (
                    -float(item.get("sector_score") or 0.0),
                    str(item.get("theme_code") or ""),
                ),
            )
            source_sector = (
                sector_by_code.get(str(theme_matches[0]["theme_code"]))
                if theme_matches
                else None
            )
            if source_sector is None:
                source_sector = {
                    "sector_code": "MARKET:DAILY_ANOMALY",
                    "sector_name": "全市场强势异动",
                    "sector_type": "market",
                    "stage": "PREHEAT",
                    "stage_reason": "DAILY_MARKET_ANOMALY",
                    "score": 50.0,
                    "positive_breadth_pct": 0.0,
                    "above_ma5_pct": 0.0,
                    "first_ignition_spike": True,
                }
            raw_score = round(
                _clamp(
                    48.0
                    + min(10.0, feature["return_1d_pct"]) * 2.5
                    + min(3.0, feature["amount_ratio_5"]) * 5.0
                    + (
                        8.0
                        if feature.get("orderly_right_side_startup")
                        else 0.0
                    )
                ),
                2,
            )
            signal = _candidate_signal(
                feature,
                sector=source_sector,
                raw_score=raw_score,
                role="全市场异动",
                rank=1,
                market_regime=market_regime,
                config=config,
            )
            signal["theme_matches"] = theme_matches
            promote_right_side = (
                bool(feature.get("orderly_right_side_startup"))
                and bool(theme_matches)
                and signal["signal_status"] == "READY"
                and bool(
                    (config.get("right_side_startup") or {}).get(
                        "promote_discovery_to_execution",
                        True,
                    )
                )
            )
            if promote_right_side:
                signal["signal_lane"] = "EXECUTION"
                signal["today_signal"] = (
                    "全市场异动扫描确认连续换手右侧启动，已进入小仓模拟执行；"
                    "盘中量价结构失效则动态退出。"
                )
                signal["gate_reason"] = signal["today_signal"]
            else:
                signal["signal_lane"] = "DAILY_MARKET_DISCOVERY"
                signal["signal_direction"] = "HOLD"
                signal["signal_status"] = (
                    "WATCH"
                    if signal["signal_status"] != "BLOCKED"
                    else "BLOCKED"
                )
                signal["today_signal"] = (
                    "全市场已发现当日强势异动；"
                    + (
                        "接近涨停，不追板，转交盘中雷达判断封单和龙二套利。"
                        if feature["near_limit_up"]
                        else "尚未同时满足主题、位置和风险回报条件，先观察。"
                    )
                )
                signal["gate_reason"] = signal["today_signal"]
            signals.append(signal)

    # Execution and discovery are deliberately independent pools.  Discovery
    # rows must remain visible when their sectors fire, but they must never
    # consume the frozen execution candidate cap or alter portfolio ordering.
    def signal_priority(
        signal: dict[str, Any],
    ) -> tuple[int, float, float, str]:
        status_priority = {
            "READY": 3,
            "WATCH": 2,
            "BLOCKED": 1,
        }.get(str(signal.get("signal_status") or ""), 0)
        return (
            status_priority,
            float(signal["sector_score"]),
            float(signal["raw_score"]),
            str(signal["theme_code"]),
        )

    def deduplicate_signals(
        lane_signals: list[dict[str, Any]],
        *,
        execution_lane: bool,
    ) -> dict[str, dict[str, Any]]:
        best_by_code: dict[str, dict[str, Any]] = {}
        for signal in lane_signals:
            code = signal["stock_code"]
            current = best_by_code.get(code)
            if current is None:
                best_by_code[code] = dict(signal)
                continue
            current_priority = (
                (
                    float(current["sector_score"]),
                    float(current["raw_score"]),
                    str(current["theme_code"]),
                )
                if execution_lane
                else signal_priority(current)
            )
            next_priority = (
                (
                    float(signal["sector_score"]),
                    float(signal["raw_score"]),
                    str(signal["theme_code"]),
                )
                if execution_lane
                else signal_priority(signal)
            )
            if next_priority > current_priority:
                best_by_code[code] = dict(signal)
        return best_by_code

    def attach_theme_matches(
        best_by_code: dict[str, dict[str, Any]],
    ) -> None:
        # A stock can belong to several concepts and industries.  Keep one
        # primary theme for portfolio caps, but retain every qualifying alias
        # so innovative medicine does not disappear under a sibling label.
        for code, candidate in best_by_code.items():
            matches = {
                str(item.get("theme_code") or ""): dict(item)
                for item in candidate.get("theme_matches") or []
                if item.get("theme_code")
            }
            for item in hot_theme_matches_by_code.get(code) or []:
                matches[str(item["theme_code"])] = dict(item)
            candidate["theme_matches"] = sorted(
                matches.values(),
                key=lambda item: (
                    item["sector_stage"] != "CONFIRMED",
                    -float(item["sector_score"]),
                    str(item["theme_code"]),
                ),
            )
            candidate["theme_count"] = len(candidate["theme_matches"])
            if candidate["theme_matches"]:
                primary = max(
                    candidate["theme_matches"],
                    key=lambda item: (
                        float(item.get("sector_score") or 0.0),
                        item.get("theme_type") == "industry",
                        str(item.get("theme_code") or ""),
                    ),
                )
                candidate.update(
                    {
                        "theme_code": primary["theme_code"],
                        "theme_name": primary["theme_name"],
                        "theme_type": primary["theme_type"],
                        "sector_stage": primary["sector_stage"],
                        "sector_stage_reason": primary[
                            "sector_stage_reason"
                        ],
                        "sector_score": primary["sector_score"],
                    }
                )
                primary_sector = next(
                    (
                        item
                        for item in sectors
                        if item["sector_code"]
                        == primary["theme_code"]
                    ),
                    None,
                )
                if primary_sector is not None:
                    candidate.update(
                        {
                            "sector_positive_breadth_pct": primary_sector[
                                "positive_breadth_pct"
                            ],
                            "sector_above_ma5_pct": primary_sector[
                                "above_ma5_pct"
                            ],
                            "sector_ignition_evidence_count": (
                                primary_sector.get(
                                    "ignition_evidence_count",
                                    0,
                                )
                            ),
                            "sector_ignition_evidence": (
                                primary_sector.get(
                                    "ignition_evidence",
                                    {},
                                )
                            ),
                        }
                    )
                    evidence = list(
                        candidate.get("evidence_chain") or []
                    )
                    if evidence:
                        evidence[0] = {
                            "module": "sector_preheat",
                            "text": (
                                f"{primary['theme_name']}：板块分"
                                f"{float(primary_sector['score']):.2f}，"
                                "上涨宽度"
                                f"{float(primary_sector['positive_breadth_pct']):.2f}%，"
                                "站上MA5占比"
                                f"{float(primary_sector['above_ma5_pct']):.2f}%"
                            ),
                            "source": (
                                "qmt_membership+qmt_daily_kline"
                            ),
                        }
                        candidate["evidence_chain"] = evidence
                triggers = list(
                    candidate.get("trigger_conditions") or []
                )
                if triggers:
                    triggers[-1] = (
                        f"{primary['theme_name']}盘中方向仍为上涨"
                    )
                    candidate["trigger_conditions"] = triggers

    execution_by_code = deduplicate_signals(
        [
            signal
            for signal in signals
            if signal.get("signal_lane") == "EXECUTION"
        ],
        execution_lane=True,
    )
    attach_theme_matches(execution_by_code)
    execution_candidates = sorted(
        execution_by_code.values(),
        key=lambda item: (
            item["signal_status"] != "READY",
            -float(item["raw_score"]),
            item["stock_code"],
        ),
    )[: int(config["maximum_candidates"])]
    execution_candidate_codes = {
        candidate["stock_code"] for candidate in execution_candidates
    }

    discovery_lane_signals = [
        signal
        for signal in signals
        if signal.get("signal_lane")
        in {"DISCOVERY_SHADOW", "DAILY_MARKET_DISCOVERY"}
        and signal["stock_code"] not in execution_candidate_codes
    ]
    discovery_by_code = deduplicate_signals(
        discovery_lane_signals,
        execution_lane=False,
    )
    attach_theme_matches(discovery_by_code)
    discovery_ranked = sorted(
        discovery_by_code.values(),
        key=lambda item: (
            item["signal_status"] == "BLOCKED",
            -float(item["raw_score"]),
            -float(item["sector_score"]),
            item["stock_code"],
        ),
    )
    maximum_discovery_candidates = int(
        config.get("maximum_discovery_candidates", 24)
    )
    discovery_candidates: list[dict[str, Any]] = []
    discovery_selected_codes: set[str] = set()

    # Always reserve the strongest full-market anomalies first.  On a broad
    # rally there can be more hot sectors than the display cap; sector-fairness
    # must not push actual limit attacks or +7.5% movers off the page.
    direct_discovery_codes = []
    for signal in sorted(
        (
            item
            for item in discovery_lane_signals
            if item.get("signal_lane") == "DAILY_MARKET_DISCOVERY"
        ),
        key=lambda item: (
            -float(item.get("candidate_return_1d_pct") or 0.0),
            -float(item.get("raw_score") or 0.0),
            str(item.get("stock_code") or ""),
        ),
    ):
        code = str(signal["stock_code"])
        if code in direct_discovery_codes:
            continue
        direct_discovery_codes.append(code)
    for code in direct_discovery_codes:
        candidate = discovery_by_code.get(code)
        if not candidate:
            continue
        discovery_candidates.append(dict(candidate))
        discovery_selected_codes.add(code)
        if len(discovery_candidates) >= maximum_discovery_candidates:
            break

    # Reserve one visible representative for each selected discovery sector.
    # Without this fairness pass a crowded medical day can occupy every global
    # observation slot and make an independently detected power theme vanish.
    for sector in discovery_hot:
        if len(discovery_candidates) >= maximum_discovery_candidates:
            break
        sector_signals = sorted(
            (
                signal
                for signal in discovery_lane_signals
                if signal["theme_code"] == sector["sector_code"]
                and signal["stock_code"] not in discovery_selected_codes
            ),
            key=lambda item: (
                item["signal_status"] == "BLOCKED",
                -float(item["raw_score"]),
                item["stock_code"],
            ),
        )
        if not sector_signals:
            continue
        representative = sector_signals[0]
        candidate = dict(
            discovery_by_code[representative["stock_code"]]
        )
        for key in (
            "theme_code",
            "theme_name",
            "theme_type",
            "sector_stage",
            "sector_stage_reason",
            "sector_score",
            "sector_role",
            "sector_rank",
        ):
            candidate[key] = representative[key]
        discovery_candidates.append(candidate)
        discovery_selected_codes.add(candidate["stock_code"])
        if (
            len(discovery_candidates)
            >= maximum_discovery_candidates
        ):
            break

    for candidate in discovery_ranked:
        if len(discovery_candidates) >= maximum_discovery_candidates:
            break
        if candidate["stock_code"] in discovery_selected_codes:
            continue
        discovery_candidates.append(candidate)
        discovery_selected_codes.add(candidate["stock_code"])
    candidates = execution_candidates + discovery_candidates
    public_sectors = [
        {key: value for key, value in item.items() if key != "_members"}
        for item in sectors
    ]
    execution_hot_sector_codes = {
        item["sector_code"] for item in execution_hot
    }
    discovery_hot_sector_codes = {
        item["sector_code"] for item in discovery_hot
    }
    execution_signal_sector_codes = {
        signal["theme_code"]
        for signal in signals
        if signal.get("signal_lane") == "EXECUTION"
    }
    discovery_signal_sector_codes = {
        signal["theme_code"]
        for signal in signals
        if signal.get("signal_lane")
        in {"DISCOVERY_SHADOW", "DAILY_MARKET_DISCOVERY"}
    }
    return {
        "status": "ok",
        "trade_date": trade_date,
        "strategy_version": config["strategy_version"],
        "config_hash": _canonical_hash(config),
        "sector_count": len(public_sectors),
        "hot_sector_count": len(execution_hot),
        "discovery_hot_sector_count": len(discovery_hot),
        "execution_candidate_count": len(execution_candidates),
        "discovery_candidate_count": len(discovery_candidates),
        "candidate_count": len(candidates),
        "ready_count": sum(
            item["signal_status"] == "READY"
            for item in execution_candidates
        ),
        "sectors": public_sectors,
        "execution_hot_sector_codes": sorted(
            execution_hot_sector_codes
        ),
        "discovery_hot_sector_codes": sorted(
            discovery_hot_sector_codes
        ),
        "execution_signal_sector_codes": sorted(
            execution_signal_sector_codes
        ),
        "discovery_signal_sector_codes": sorted(
            discovery_signal_sector_codes
        ),
        "execution_candidates": execution_candidates,
        "discovery_candidates": discovery_candidates,
        "candidates": candidates,
    }


def _latest_snapshot_date(
    engine: Engine,
    *,
    table_name: str,
    target_date: str,
    decision_at: datetime,
) -> str:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                f"""
                SELECT MAX(snapshot_date)
                FROM `{table_name}`
                WHERE snapshot_date <= :target_date
                  AND captured_at <= :decision_at
                  AND quality_status = 'QMT_VALIDATED'
                """
            ),
            {
                "target_date": target_date,
                "decision_at": decision_at,
            },
        ).scalar()
    return str(value or "")[:10]


def _sector_sources_are_fresh(
    *,
    target_date: str,
    industry_snapshot_date: str,
    concept_snapshot_date: str,
    kline_snapshot_date: str,
    membership_row_count: int,
    kline_row_count: int,
) -> bool:
    return (
        industry_snapshot_date == target_date
        and concept_snapshot_date == target_date
        and kline_snapshot_date == target_date
        and membership_row_count > 0
        and kline_row_count > 0
    )


def build_sector_preheat_snapshot(
    *,
    trade_date: str,
    decision_at: datetime,
    market_regime: str = "",
    engine: Engine | None = None,
    context_engine: Engine | None = None,
) -> dict[str, Any]:
    """Load point-in-time QMT facts and build production sector signals."""
    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    config = load_sector_preheat_config()
    source_engine = engine or get_kline_engine()
    try:
        industry_date = _latest_snapshot_date(
            source_engine,
            table_name="qmt_industry_member_snapshot",
            target_date=target,
            decision_at=decision_at,
        )
        concept_date = _latest_snapshot_date(
            source_engine,
            table_name="qmt_concept_member_snapshot",
            target_date=target,
            decision_at=decision_at,
        )
        memberships: list[dict[str, Any]] = []
        with source_engine.connect() as connection:
            if industry_date:
                rows = connection.execute(
                    text(
                        """
                        SELECT 'industry' AS sector_type,
                               industry_code AS sector_code,
                               industry_name AS sector_name,
                               stock_code, short_name
                        FROM qmt_industry_member_snapshot
                        WHERE snapshot_date = :snapshot_date
                          AND captured_at <= :decision_at
                          AND quality_status = 'QMT_VALIDATED'
                        """
                    ),
                    {
                        "snapshot_date": industry_date,
                        "decision_at": decision_at,
                    },
                ).mappings().all()
                memberships.extend(dict(row) for row in rows)
            if concept_date:
                rows = connection.execute(
                    text(
                        """
                        SELECT 'concept' AS sector_type,
                               concept_code AS sector_code,
                               concept_name AS sector_name,
                               stock_code, short_name
                        FROM qmt_concept_member_snapshot
                        WHERE snapshot_date = :snapshot_date
                          AND captured_at <= :decision_at
                          AND quality_status = 'QMT_VALIDATED'
                        """
                    ),
                    {
                        "snapshot_date": concept_date,
                        "decision_at": decision_at,
                    },
                ).mappings().all()
                memberships.extend(dict(row) for row in rows)

            start_date = (
                date.fromisoformat(target) - timedelta(days=30)
            ).isoformat()
            bars = connection.execute(
                text(
                    """
                    SELECT stock_code, short_name, trade_date, open, close,
                           high, low, pre_close, change_pct, amount,
                           received_at, quality_status
                    FROM sm_stock_kline
                    WHERE trade_date BETWEEN :start_date AND :target_date
                      AND k_type = 1
                      AND adjust_type = 0
                      AND data_source = 'gj_big_qmt_inner'
                      AND qmt_code IS NOT NULL
                      AND qmt_code <> ''
                      AND quality_status IN ('VERIFIED', 'QMT_ATTESTED')
                      AND permission_status IN ('SUPPORTED', 'CONFIRMED')
                      AND COALESCE(received_at, source_time, etl_sync_at)
                          <= :decision_at
                      AND stock_code REGEXP '^[036][0-9]{5}$'
                    ORDER BY stock_code, trade_date, received_at
                    """
                ),
                {
                    "start_date": start_date,
                    "target_date": target,
                    "decision_at": decision_at,
                },
            ).mappings().all()
        kline_date = max(
            (str(row.get("trade_date") or "")[:10] for row in bars),
            default="",
        )
        result = score_sector_preheat(
            memberships=memberships,
            bars=[dict(row) for row in bars],
            trade_date=target,
            market_regime=market_regime,
            config=config,
        )
        if context_engine is not None:
            result = apply_candidate_context(
                result,
                engine=context_engine,
                trade_date=target,
                decision_at=decision_at,
                config=config,
            )
        result.update(
            {
                "source_status": (
                    "fresh"
                    if _sector_sources_are_fresh(
                        target_date=target,
                        industry_snapshot_date=industry_date,
                        concept_snapshot_date=concept_date,
                        kline_snapshot_date=kline_date,
                        membership_row_count=len(memberships),
                        kline_row_count=len(bars),
                    )
                    else "degraded"
                ),
                "industry_snapshot_date": industry_date,
                "concept_snapshot_date": concept_date,
                "kline_snapshot_date": kline_date,
                "membership_row_count": len(memberships),
                "kline_row_count": len(bars),
                "available_at_rule": (
                    "captured_at/received_at <= decision_at"
                ),
                "data_source": "gj_big_qmt_inner",
                "quality_statuses": ["VERIFIED", "QMT_ATTESTED"],
            }
        )
        result["snapshot_hash"] = _canonical_hash(
            {
                "trade_date": target,
                "decision_at": decision_at,
                "industry_snapshot_date": industry_date,
                "concept_snapshot_date": concept_date,
                "kline_snapshot_date": kline_date,
                "membership_row_count": len(memberships),
                "kline_row_count": len(bars),
                "config_hash": result["config_hash"],
                "context_hash": result.get("context_hash") or "",
                "context_sources": result.get("context_sources") or {},
                "sectors": result["sectors"],
                "candidates": result["candidates"],
            }
        )
        return result
    except Exception as exc:
        return {
            "status": "unavailable",
            "source_status": "unavailable",
            "trade_date": target,
            "strategy_version": config["strategy_version"],
            "config_hash": _canonical_hash(config),
            "snapshot_hash": "",
            "sector_count": 0,
            "hot_sector_count": 0,
            "discovery_hot_sector_count": 0,
            "execution_candidate_count": 0,
            "discovery_candidate_count": 0,
            "candidate_count": 0,
            "ready_count": 0,
            "sectors": [],
            "execution_hot_sector_codes": [],
            "discovery_hot_sector_codes": [],
            "execution_signal_sector_codes": [],
            "discovery_signal_sector_codes": [],
            "execution_candidates": [],
            "discovery_candidates": [],
            "candidates": [],
            "error": f"{type(exc).__name__}: {exc}"[:1000],
        }


def _explicit_gate_true(value: Any) -> bool:
    return value is True or (type(value) is int and value == 1)


def _candidate_canonical_chase_gate(
    candidate: dict[str, Any] | None,
) -> tuple[str, bool]:
    """Resolve one conservative, canonical new-buy gate for a stock."""
    if not candidate:
        return "DATA_BLOCKED", False
    statuses: list[str] = []
    eligibilities: list[bool] = []
    for signal in candidate.get("strategy_signals") or []:
        status = str(
            signal.get("chase_risk_status")
            or signal.get("source_chase_risk_status")
            or ""
        ).upper()
        if not status:
            continue
        statuses.append(status)
        eligibilities.append(
            _explicit_gate_true(
                signal.get(
                    "ordinary_buy_eligible",
                    signal.get("source_ordinary_buy_eligible"),
                )
            )
        )
    if not statuses:
        status = str(
            candidate.get("chase_risk_status")
            or candidate.get("source_chase_risk_status")
            or "DATA_BLOCKED"
        ).upper()
        return status, _explicit_gate_true(
            candidate.get(
                "ordinary_buy_eligible",
                candidate.get("source_ordinary_buy_eligible"),
            )
        )
    if all(status == "ALLOW" for status in statuses) and all(eligibilities):
        return "ALLOW", True
    first_block = next(
        (status for status in statuses if status != "ALLOW"),
        "DATA_BLOCKED",
    )
    return first_block, False


def _candidate_canonical_source_gate(
    candidate: dict[str, Any] | None,
) -> tuple[str, str]:
    """Resolve upstream recommendation/signal facts without synthesizing them."""

    if not candidate:
        return "DATA_BLOCKED", "WATCH"
    recommendations: list[str] = []
    signals: list[str] = []
    for item in candidate.get("strategy_signals") or []:
        recommend_status = str(
            item.get("source_recommend_status")
            or item.get("recommend_status")
            or ""
        ).upper()
        signal_status = str(
            item.get("source_signal_status") or ""
        ).upper()
        if recommend_status:
            recommendations.append(recommend_status)
        if signal_status:
            signals.append(signal_status)
    if not recommendations:
        recommendations.append(
            str(
                candidate.get("source_recommend_status")
                or candidate.get("recommend_status")
                or "DATA_BLOCKED"
            ).upper()
        )
    if not signals:
        signals.append(
            str(
                candidate.get("source_signal_status")
                or "WATCH"
            ).upper()
        )
    recommend_status = (
        "ALLOW"
        if all(value == "ALLOW" for value in recommendations)
        else next(
            (value for value in recommendations if value != "ALLOW"),
            "DATA_BLOCKED",
        )
    )
    actionable = {"CONFIRM", "BUY_READY"}
    signal_status = (
        "BUY_READY"
        if all(value in actionable for value in signals)
        and "BUY_READY" in signals
        else "CONFIRM"
        if all(value in actionable for value in signals)
        else next(
            (value for value in signals if value not in actionable),
            "WATCH",
        )
    )
    return recommend_status, signal_status


def _candidate_has_explicit_exit(candidate: dict[str, Any] | None) -> bool:
    if not candidate:
        return False
    if str(candidate.get("final_direction") or "").upper() in {
        "SELL",
        "REDUCE",
        "EXIT",
    }:
        return True
    if str(candidate.get("final_status") or "").upper() in {
        "SELL_ALERT",
        "REDUCE",
        "EXIT",
    }:
        return True
    return any(
        str(signal.get("signal_direction") or "").upper()
        in {"SELL", "REDUCE", "EXIT"}
        or str(signal.get("signal_status") or "").upper()
        in {"SELL_ALERT", "REDUCE", "EXIT"}
        for signal in candidate.get("strategy_signals") or []
    )


def _gate_sector_signal_with_canonical_candidate(
    signal: dict[str, Any],
    candidate: dict[str, Any] | None,
) -> dict[str, Any]:
    gated = dict(signal)
    direction = str(gated.get("signal_direction") or "HOLD").upper()
    if direction not in {"BUY"}:
        return gated
    recommend_status, source_signal_status = (
        _candidate_canonical_source_gate(candidate)
    )
    chase_status, eligible = _candidate_canonical_chase_gate(candidate)
    gated["source_recommend_status"] = recommend_status
    gated["source_signal_status"] = source_signal_status
    gated["chase_risk_status"] = chase_status
    gated["ordinary_buy_eligible"] = eligible
    gated["canonical_new_buy_gate_source"] = "legacy_batch_by_stock_code"
    if _candidate_has_explicit_exit(candidate):
        reason_code = "CONFLICTING_EXIT_SIGNAL"
        reason = "同批次已有明确卖出或减仓信号，禁止新增买入"
    elif recommend_status != "ALLOW":
        reason_code = "CANONICAL_RECOMMEND_GATE_NOT_ALLOWED"
        reason = f"canonical recommend gate is {recommend_status}"
    elif source_signal_status not in {"CONFIRM", "BUY_READY"}:
        reason_code = "CANONICAL_SIGNAL_NOT_CONFIRMED"
        reason = f"canonical signal is {source_signal_status}"
    elif chase_status != "ALLOW" or not eligible:
        reason_code = "CANONICAL_CHASE_GATE_NOT_ALLOWED"
        reason = f"同批次追高与成交能力硬门未通过：{chase_status}"
    else:
        gated["canonical_new_buy_gate_status"] = "PASS"
        return gated
    gated["requested_signal_direction"] = direction
    gated["signal_direction"] = "HOLD"
    gated["signal_status"] = "BLOCKED"
    gated["gate_status"] = "BLOCK"
    gated["gate_reason"] = reason
    gated["canonical_new_buy_gate_status"] = "BLOCK"
    gated["canonical_new_buy_rejection_code"] = reason_code
    evidence = list(gated.get("evidence_chain") or [])
    evidence.append(
        {
            "module": "canonical_new_buy_gate",
            "status": "BLOCK",
            "text": reason,
            "source": "legacy_batch_by_stock_code",
        }
    )
    gated["evidence_chain"] = evidence
    return gated


def merge_sector_preheat_candidates(
    legacy_snapshot: dict[str, Any],
    sector_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """Attach independent sector signals without hiding legacy signals."""
    merged = dict(legacy_snapshot)
    candidates = [
        {
            **item,
            "strategy_signals": list(item.get("strategy_signals") or []),
        }
        for item in (legacy_snapshot.get("candidates") or [])
    ]
    by_code = {
        str(item.get("stock_code") or "").zfill(6): item
        for item in candidates
    }
    canonical_ready_count = 0
    for raw_signal in sector_snapshot.get("candidates") or []:
        signal = dict(raw_signal)
        code = str(signal.get("stock_code") or "").zfill(6)
        if not code:
            continue
        existing_candidate = by_code.get(code)
        signal = _gate_sector_signal_with_canonical_candidate(
            signal,
            existing_candidate,
        )
        if (
            str(signal.get("signal_direction") or "").upper() == "BUY"
            and str(signal.get("signal_status") or "").upper() == "READY"
        ):
            canonical_ready_count += 1
        if code in by_code:
            candidate = by_code[code]
            candidate["strategy_signals"].append(signal)
            if (
                signal.get("signal_status") == "READY"
                and candidate.get("final_status") != "SELL_ALERT"
            ):
                candidate["final_direction"] = "BUY"
                candidate["final_status"] = "READY"
                candidate["dominant_strategy"] = "sector_preheat"
                candidate["theme_code"] = signal.get("theme_code") or ""
                candidate["today_signal"] = signal.get("today_signal") or ""
        else:
            candidate = {
                "priority": (
                    "A" if signal.get("signal_status") == "READY" else "B"
                ),
                "stock_code": code,
                "stock_name": signal.get("stock_name") or code,
                "final_direction": signal.get("signal_direction") or "HOLD",
                "final_status": signal.get("signal_status") or "WATCH",
                "model_confidence": signal.get("model_confidence"),
                "today_signal": signal.get("today_signal") or "",
                "entry_low": signal.get("entry_low"),
                "entry_high": signal.get("entry_high"),
                "trigger_conditions": signal.get("trigger_conditions") or [],
                "stop_loss": signal.get("stop_loss"),
                "take_profit_1": signal.get("take_profit_1"),
                "take_profit_2": signal.get("take_profit_2"),
                "no_chase_price": signal.get("no_chase_price"),
                "risk_level": signal.get("risk_level") or "MEDIUM",
                "risk_reward_ratio": signal.get("risk_reward_ratio"),
                "dominant_strategy": "sector_preheat",
                "strategies": ["sector_preheat"],
                "buy_score": signal.get("raw_score") or 0,
                "sell_score": 0,
                "hold_score": 0,
                "conflict": False,
                "conflict_summary": signal.get("today_signal") or "",
                "blocking_reasons": (
                    [signal.get("gate_reason")]
                    if signal.get("gate_status") == "BLOCK"
                    else []
                ),
                "strategy_signals": [signal],
                "data_date": signal.get("data_date"),
                "adapter_mode": signal.get("adapter_mode"),
                "theme_code": signal.get("theme_code") or "",
                "db_verified": True,
                "db_close": signal.get("db_close"),
                "db_verification_reason": signal.get(
                    "db_verification_reason"
                ),
            }
            candidates.append(candidate)
            by_code[code] = candidate
    candidates.sort(
        key=lambda item: (
            {"READY": 0, "WATCH": 1, "BLOCKED": 2}.get(
                str(item.get("final_status") or ""),
                3,
            ),
            -_float(item.get("model_confidence")),
            str(item.get("stock_code") or ""),
        )
    )
    merged["candidates"] = candidates
    merged["sector_preheat"] = sector_snapshot
    summary = dict(legacy_snapshot.get("summary") or {})
    summary["candidate_count"] = len(candidates)
    summary["sector_preheat_candidate_count"] = int(
        sector_snapshot.get("candidate_count") or 0
    )
    summary["sector_preheat_raw_ready_count"] = int(
        sector_snapshot.get("ready_count") or 0
    )
    summary["sector_preheat_ready_count"] = canonical_ready_count
    merged["summary"] = summary
    sources = list(legacy_snapshot.get("data_sources") or [])
    sources.append("qmt_point_in_time_sector_preheat")
    merged["data_sources"] = list(dict.fromkeys(sources))
    return merged
