"""Immutable execution rules for internal SHADOW trials.

The trial risk policy is deliberately independent of funding eligibility. A
trial observes one frozen strategy version under a declared risk limit; it
never supplies broker authority or manufactures a mature observation.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping


SHADOW_CAPACITY_POLICY = {
    "schema": "probiga.shadow-capacity-policy.v1",
    "evaluation_sessions": 120,
    "required_mature_trades": 80,
    "rolling_window_sessions": 60,
    "planning_utilization": 0.8,
    "maximum_positions": 12,
    "maximum_active_versions": 1,
    "selection_order": "HOLDING_HORIZON_THEN_VERSION_CREATED_AT_THEN_STRATEGY_KEY",
    "early_exit_statuses": ["ACTIVE", "REDUCE", "SUSPENDED", "RETIRED"],
    "daily_return_used_for_selection": False,
}


SHADOW_EXECUTION_POLICY = {
    "schema": "probiga.shadow-trial-execution-policy.v1",
    "maximum_initial_stop_pct": 8.0,
    "maximum_target_bp": 100,
    "entry_timing": "NEXT_AUTHORITATIVE_SESSION",
    "exit_timing": "NEXT_EXECUTABLE_SESSION_AFTER_CLOSE",
    "holding_age_basis": "AUTHORITATIVE_OPEN_SESSIONS_INCLUDING_ENTRY",
    "automatic_real_order_submission": False,
    "real_order_authority": False,
    "capacity_policy": SHADOW_CAPACITY_POLICY,
}


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")).hexdigest()


def frozen_shadow_execution_contract(
    *, strategy_key: str, strategy_version: str, version_hash: str,
    source_kind: str, parameters: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind version-specific holding/stop facts to the common trial policy."""
    policy = parameters.get("shadow_execution_policy")
    if not isinstance(policy, Mapping):
        raise ValueError("SHADOW_FROZEN_EXECUTION_POLICY_MISSING")
    policy = json.loads(json.dumps(policy))
    raw_horizon = parameters.get("max_holding_days", parameters.get("horizon_days"))
    if isinstance(raw_horizon, bool):
        raise ValueError("SHADOW_HOLDING_HORIZON_INVALID")
    try:
        horizon = int(raw_horizon)
    except (TypeError, ValueError) as exc:
        raise ValueError("SHADOW_HOLDING_HORIZON_MISSING") from exc
    if not 1 <= horizon <= 250 or float(raw_horizon) != horizon:
        raise ValueError("SHADOW_HOLDING_HORIZON_INVALID")
    initial_stop = candidate.get("stop_loss") or candidate.get("stop_loss_price")
    initial_stop = float(initial_stop) if initial_stop is not None else None
    raw_stop_pct = candidate.get("initial_stop_pct")
    stop_pct = float(raw_stop_pct) if raw_stop_pct is not None else None
    if initial_stop is not None and (not math.isfinite(initial_stop) or initial_stop <= 0):
        raise ValueError("SHADOW_INITIAL_STOP_INVALID")
    if stop_pct is not None and (not math.isfinite(stop_pct) or not 0 < stop_pct < 100):
        raise ValueError("SHADOW_INITIAL_STOP_INVALID")
    # The explicit policy cap is a risk overlay on the frozen alpha, not an
    # inferred alpha stop. Both the alpha stop and cap remain in the contract.
    payload = {
        "schema": "probiga.shadow-trial-execution-contract.v1",
        "strategy_key": str(strategy_key),
        "strategy_version": str(strategy_version),
        "strategy_version_hash": str(version_hash),
        "strategy_source_kind": str(source_kind),
        "maximum_holding_sessions": horizon,
        "signal_stop_price": initial_stop,
        "signal_stop_pct": stop_pct,
        "policy": policy,
        "policy_hash": _hash(policy),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return validate_shadow_execution_contract({**payload, "contract_hash": _hash(payload)})


def validate_shadow_execution_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("SHADOW_EXECUTION_CONTRACT_MISSING")
    payload = dict(value)
    digest = payload.pop("contract_hash", "")
    policy = payload.get("policy")
    if (
        payload.get("schema") != "probiga.shadow-trial-execution-contract.v1"
        or digest != _hash(payload)
        or not isinstance(policy, dict)
        or payload.get("policy_hash") != _hash(policy)
        or policy.get("schema") != "probiga.shadow-trial-execution-policy.v1"
        or policy.get("entry_timing") != "NEXT_AUTHORITATIVE_SESSION"
        or policy.get("exit_timing") != "NEXT_EXECUTABLE_SESSION_AFTER_CLOSE"
        or policy.get("holding_age_basis") != "AUTHORITATIVE_OPEN_SESSIONS_INCLUDING_ENTRY"
        or not 0 < float(policy.get("maximum_initial_stop_pct") or 0) < 100
        or not 1 <= int(policy.get("maximum_target_bp") or 0) <= 100
        or policy.get("automatic_real_order_submission") is not False
        or policy.get("real_order_authority") is not False
        or payload.get("automatic_real_order_submission") is not False
        or payload.get("real_order_authority") is not False
        or type(payload.get("maximum_holding_sessions")) is not int
        or not 1 <= payload["maximum_holding_sessions"] <= 250
    ):
        raise ValueError("SHADOW_EXECUTION_CONTRACT_INVALID")
    return {**payload, "contract_hash": digest}


def shadow_initial_stop(contract: Mapping[str, Any], reference_price: float) -> float:
    frozen = validate_shadow_execution_contract(contract)
    if not math.isfinite(reference_price) or reference_price <= 0:
        raise ValueError("SHADOW_REFERENCE_PRICE_INVALID")
    stops = [reference_price * (1 - frozen["policy"]["maximum_initial_stop_pct"] / 100)]
    if frozen["signal_stop_price"] is not None:
        stops.append(float(frozen["signal_stop_price"]))
    if frozen["signal_stop_pct"] is not None:
        stops.append(reference_price * (1 - float(frozen["signal_stop_pct"]) / 100))
    result = round(max(stops), 3)
    if not 0 < result < reference_price:
        raise ValueError("SHADOW_INITIAL_STOP_NOT_BELOW_ENTRY")
    return result


def shadow_exit_reason(
    contract: Mapping[str, Any], *, holding_sessions: int,
    session_low: float | None, protective_stop: float,
) -> str | None:
    frozen = validate_shadow_execution_contract(contract)
    if session_low is not None and math.isfinite(session_low) and session_low <= protective_stop:
        return "HARD_STOP"
    # Decisions execute next session. Request the exit one session before the
    # maximum admissible age; T+1 can defer a one-session strategy's execution.
    if holding_sessions >= max(1, frozen["maximum_holding_sessions"] - 1):
        return "SHADOW_MAXIMUM_HOLDING_SESSIONS"
    return None
