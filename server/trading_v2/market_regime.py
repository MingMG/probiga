"""Exact seven-state V2 market-regime classifier."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .config import load_frozen_json
from .domain import decimal_value


REGIME_PRIORITY = {
    "DATA_BLOCKED": 0,
    "EXTREME": 1,
    "RISK_OFF": 2,
    "PANIC_RECOVERY": 3,
    "TREND_UP": 4,
    "THEME_ROTATION": 5,
    "RANGE": 6,
}


@dataclass(frozen=True)
class RegimeDecision:
    candidate_state: str
    final_state: str
    confidence: Decimal
    cooldown_remaining: int
    input_quality: str
    evidence: tuple[str, ...]
    config_version: str
    config_hash: str


def classify_market_regime(
    inputs: dict[str, Any],
    *,
    previous_state: str = "",
    previous_state_days: int = 0,
    previous_candidate_state: str = "",
    previous_candidate_streak: int = 0,
    extreme_cooldown_remaining: int = 0,
) -> RegimeDecision:
    config, config_hash = load_frozen_json("strategies/market_regime_v2.json")
    missing = [
        key
        for key in config["required_inputs"]
        if inputs.get(key) is None or inputs.get(key) == ""
    ]
    if missing:
        return RegimeDecision(
            candidate_state="DATA_BLOCKED",
            final_state="DATA_BLOCKED",
            confidence=Decimal("100"),
            cooldown_remaining=max(0, int(extreme_cooldown_remaining)),
            input_quality="BLOCK",
            evidence=(f"missing_required_inputs:{','.join(sorted(missing))}",),
            config_version=config["config_version"],
            config_hash=config_hash,
        )

    risk = decimal_value(inputs["risk_score"])
    change = decimal_value(inputs["market_change_pct"])
    breadth = decimal_value(inputs["breadth_pct"])
    trend = decimal_value(inputs["trend_score"])
    switch = decimal_value(inputs["switch_score"])
    t = config["thresholds"]
    evidence: list[str] = []

    if (
        risk >= decimal_value(t["EXTREME"]["risk_score_gte"])
        or change <= decimal_value(t["EXTREME"]["market_change_pct_lte"])
    ):
        candidate = "EXTREME"
        evidence.append("extreme_threshold")
    elif (
        risk >= decimal_value(t["RISK_OFF"]["risk_score_gte"])
        or change < decimal_value(t["RISK_OFF"]["market_change_pct_lt"])
    ):
        candidate = "RISK_OFF"
        evidence.append("risk_off_threshold")
    elif (
        (previous_state == "EXTREME" or extreme_cooldown_remaining > 0)
        and risk < decimal_value(t["PANIC_RECOVERY"]["risk_score_lt"])
        and change > decimal_value(t["PANIC_RECOVERY"]["market_change_pct_gt"])
    ):
        candidate = "PANIC_RECOVERY"
        evidence.append("post_extreme_cooldown")
    elif (
        trend >= decimal_value(t["TREND_UP"]["trend_score_gte"])
        and breadth >= decimal_value(t["TREND_UP"]["breadth_pct_gte"])
        and risk < decimal_value(t["TREND_UP"]["risk_score_lt"])
    ):
        candidate = "TREND_UP"
        evidence.append("trend_up_thresholds")
    elif (
        switch >= decimal_value(t["THEME_ROTATION"]["switch_score_gte"])
        and breadth >= decimal_value(t["THEME_ROTATION"]["breadth_pct_gte"])
        and breadth < decimal_value(t["THEME_ROTATION"]["breadth_pct_lt"])
        and risk < decimal_value(t["THEME_ROTATION"]["risk_score_lt"])
    ):
        candidate = "THEME_ROTATION"
        evidence.append("theme_rotation_thresholds")
    else:
        candidate = "RANGE"
        evidence.append("range_fallback")

    streak = (
        previous_candidate_streak + 1
        if candidate == previous_candidate_state
        else 1
    )
    confirm_days = int(config["transition"]["confirm_days"][candidate])
    minimum_days = int(
        config["transition"]["minimum_state_days"].get(previous_state, 1)
    )
    worsening = (
        bool(previous_state)
        and REGIME_PRIORITY[candidate] < REGIME_PRIORITY.get(previous_state, 99)
    )
    if candidate == previous_state:
        final = candidate
    elif worsening:
        final = candidate
        evidence.append("worsening_transition_immediate")
    elif previous_state and previous_state_days < minimum_days:
        final = previous_state
        evidence.append("minimum_state_days_hold")
    elif streak < confirm_days:
        final = previous_state or candidate
        evidence.append("candidate_confirmation_pending")
    else:
        final = candidate

    if candidate == "EXTREME":
        cooldown = int(config["transition"]["extreme_cooldown_trade_days"])
    elif extreme_cooldown_remaining > 0:
        cooldown = max(0, int(extreme_cooldown_remaining) - 1)
    else:
        cooldown = 0
    confidence = min(
        Decimal("100"),
        Decimal("55") + Decimal(streak * 10) + (Decimal("15") if worsening else 0),
    )
    return RegimeDecision(
        candidate_state=candidate,
        final_state=final,
        confidence=confidence,
        cooldown_remaining=cooldown,
        input_quality="PASS",
        evidence=tuple(evidence),
        config_version=config["config_version"],
        config_hash=config_hash,
    )
