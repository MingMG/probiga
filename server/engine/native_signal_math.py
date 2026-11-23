"""Pure native SHADOW candidate qualification."""
from __future__ import annotations
import json
import math
from typing import Any, Mapping

def _v3_signal(strategy: Mapping[str, Any], forecast: Mapping[str, Any], target: str) -> dict | None:
    policy = strategy["evaluator_config"]["shadow_trial_routing"]
    if forecast["strategy_key"] != strategy["strategy_key"]:
        raise ValueError("SHADOW_V3_FORECAST_OWNER_MISMATCH")
    # The forecast engine only assigns these statuses after the native builder
    # returns SCORED. Missing facts, preparation states and hard blocks cannot
    # enter this trial. Calibration failure is observed, never called profit.
    if str(forecast["forecast_status"]) not in policy["allowed_forecast_statuses"]:
        return None
    score = float(forecast.get("raw_score") or 0)
    if not math.isfinite(score) or not float(policy["minimum_raw_score"]) <= score <= 1:
        return None
    features = json.loads(str(forecast.get("features_json") or "{}"))
    if any(features.get(key) not in (1, True, 1.0) for key in ("entry_eligible", "latest_tradable")):
        return None
    price = float(features.get("price") or 0)
    stop_pct = abs(float(forecast.get("initial_stop_pct") or 0))
    if not math.isfinite(price) or price <= 0 or not 0 < stop_pct < 100:
        raise ValueError("SHADOW_V3_TARGET_EXECUTION_FACTS_INVALID")
    if int(forecast["horizon_days"]) != int(strategy["parameters"]["horizon_days"]):
        raise ValueError("SHADOW_V3_FORECAST_HORIZON_MISMATCH")
    expected = forecast.get("expected_return_net_pct")
    return {
        "stock_code": str(forecast["stock_code"]), "stock_name": str(forecast.get("short_name") or ""),
        "strategy_key": str(strategy["strategy_key"]), "signal_direction": "BUY", "signal_status": "READY",
        "model_confidence": float(forecast.get("confidence") or 0) * 100,
        "effective_score": score * 100,
        "effective_weight": float(strategy["parameters"]["default_risk_weight"]),
        "gate_status": "PASS",
        "risk_reward_ratio": float(expected) / stop_pct if expected is not None else None,
        "entry_low": price, "entry_high": price, "stop_loss": round(price * (1-stop_pct/100), 3),
        "initial_stop_pct": stop_pct, "today_signal": str(forecast.get("reasons_json") or ""),
        "trade_date": target, "data_date": target,
    }


def _manifest_signal(
    strategy: Mapping[str, Any], raw: Mapping[str, Any], *, target: str,
    market: Mapping[str, Any],
) -> dict[str, Any]:
    from server.engine.manifest_signal_math import strategy_signal_basis

    evaluator = strategy["evaluator_config"]
    policy = evaluator["shadow_trial_routing"]
    if policy["signal_policy"] != "FROZEN_INDEPENDENT_SCORE_V1":
        raise ValueError("SHADOW_MANIFEST_SIGNAL_POLICY_INVALID")
    key = str(strategy["strategy_key"])
    row = {**dict(raw), "publication_status": "ACTIVE"}
    raw_score = raw.get(str(evaluator["score_field"]))
    score = float(raw_score) if raw_score is not None else None
    if score is not None and (not math.isfinite(score) or not 0 <= score <= 100):
        raise ValueError("SHADOW_MANIFEST_SCORE_INVALID")
    basis = strategy_signal_basis(
        row, key, score, manifest={
            "paper_trial_routing": policy,
            "strategies": [{"key": key, "parameters": strategy["parameters"]}],
        },
    )
    state = str(market.get("market_state") or "")
    multiplier = evaluator["market_regime_multipliers"].get(state)
    ready = (basis["direction"] == "BUY" and not basis["hard_block"]
             and multiplier is not None and float(multiplier) > 0 and state != "extreme_event")
    return {
        "stock_code": str(raw["stock_code"]).zfill(6),
        "stock_name": str(raw.get("short_name") or raw.get("stock_name") or ""),
        "strategy_key": key, "signal_direction": "BUY" if ready else "HOLD",
        "signal_status": "READY" if ready else "WATCH",
        "model_confidence": raw.get("confidence_score"), "effective_score": score,
        "effective_weight": float(multiplier) if multiplier is not None else 0.0,
        "gate_status": "PASS" if ready else "BLOCK",
        "risk_reward_ratio": raw.get("risk_reward_ratio"),
        "entry_low": raw.get("entry_price_low"), "entry_high": raw.get("entry_price_high"),
        "stop_loss": raw.get("stop_loss_price") or raw.get("trend_stop_price"),
        "initial_stop_pct": None, "today_signal": str(basis["reason"]),
        "trade_date": target, "data_date": target,
    }
