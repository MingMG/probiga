# -*- coding: utf-8 -*-
"""Deterministic V2 market-state classifier with hysteresis and cooldown."""
from __future__ import annotations

import math
from typing import Any

from server.common.versioned_strategy_config import (
    load_market_state_config,
    market_state_config_hash,
)


STATE_SEVERITY = {
    "trend_bullish": 0,
    "high_range": 1,
    "risk_declining": 2,
    "extreme_event": 3,
    "unknown": 4,
}


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def classify_market_state(
    snapshot: dict[str, Any] | None,
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = snapshot or {}
    config = config or load_market_state_config()
    thresholds = config["thresholds"]
    risk = _number(snapshot.get("risk_score"))
    change = _number(snapshot.get("market_change_pct"))
    breadth = _number(snapshot.get("breadth_pct"))
    trend = _number(snapshot.get("trend_score"))
    switch = _number(snapshot.get("switch_score"))
    evidence: list[str] = []

    extreme_cfg = thresholds["extreme_event"]
    extreme_flag = any(bool(snapshot.get(flag)) for flag in extreme_cfg["explicit_flags"])
    if (
        extreme_flag
        or (risk is not None and risk >= float(extreme_cfg["risk_score_enter_gte"]))
        or (change is not None and change <= float(extreme_cfg["market_change_enter_lte"]))
    ):
        if extreme_flag:
            evidence.append("极端事件标志已触发")
        if risk is not None and risk >= float(extreme_cfg["risk_score_enter_gte"]):
            evidence.append(f"风险分数{risk:.1f}达到极端阈值")
        if change is not None and change <= float(extreme_cfg["market_change_enter_lte"]):
            evidence.append(f"市场变动{change:.2f}%达到极端阈值")
        return {"candidate_state": "extreme_event", "evidence": evidence, "source_status": "fresh"}

    risk_cfg = thresholds["risk_declining"]
    risk_flag = any(bool(snapshot.get(flag)) for flag in risk_cfg["explicit_flags"])
    if (
        risk_flag
        or (risk is not None and risk >= float(risk_cfg["risk_score_enter_gte"]))
        or (change is not None and change < float(risk_cfg["market_change_enter_lt"]))
    ):
        if risk_flag:
            evidence.append("风险/技术风控标志已触发")
        if risk is not None and risk >= float(risk_cfg["risk_score_enter_gte"]):
            evidence.append(f"风险分数{risk:.1f}偏高")
        if change is not None and change < float(risk_cfg["market_change_enter_lt"]):
            evidence.append(f"市场变动{change:.2f}%偏弱")
        return {"candidate_state": "risk_declining", "evidence": evidence, "source_status": "fresh"}

    high_cfg = thresholds["high_range"]
    high_flag = any(bool(snapshot.get(flag)) for flag in high_cfg["explicit_flags"])
    if (
        high_flag
        or (breadth is not None and breadth < float(high_cfg["breadth_enter_lt"]))
        or (switch is not None and switch >= float(high_cfg["switch_score_enter_gte"]))
    ):
        if high_flag:
            evidence.append("市场处于高位区间")
        if breadth is not None and breadth < float(high_cfg["breadth_enter_lt"]):
            evidence.append(f"市场宽度{breadth:.1f}%收窄")
        if switch is not None and switch >= float(high_cfg["switch_score_enter_gte"]):
            evidence.append(f"风格切换分数{switch:.1f}偏高")
        return {"candidate_state": "high_range", "evidence": evidence, "source_status": "fresh"}

    bull_cfg = thresholds["trend_bullish"]
    if risk is not None and breadth is not None and trend is not None:
        if (
            trend >= float(bull_cfg["trend_score_enter_gte"])
            and breadth >= float(bull_cfg["breadth_enter_gte"])
            and risk < float(bull_cfg["risk_score_enter_lt"])
        ):
            evidence.extend(
                [
                    f"趋势分数{trend:.1f}达到偏多阈值",
                    f"市场宽度{breadth:.1f}%支持趋势",
                    f"风险分数{risk:.1f}低于偏多上限",
                ]
            )
            return {"candidate_state": "trend_bullish", "evidence": evidence, "source_status": "fresh"}
        evidence.append("有效数据未满足趋势偏多，按高位震荡观察")
        return {"candidate_state": "high_range", "evidence": evidence, "source_status": "fresh"}

    available = {
        "risk_score": risk,
        "market_change_pct": change,
        "breadth_pct": breadth,
        "trend_score": trend,
        "switch_score": switch,
    }
    missing = [key for key, value in available.items() if value is None]
    return {
        "candidate_state": "unknown",
        "evidence": ["缺少状态模型必要输入：" + "、".join(missing)],
        "source_status": "missing",
        "missing_inputs": missing,
    }


def transition_market_state(
    snapshot: dict[str, Any] | None,
    *,
    previous: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = config or load_market_state_config()
    classified = classify_market_state(snapshot, config=config)
    candidate = str(classified["candidate_state"])
    transition = config["transition"]
    previous = previous or {}
    previous_final = str(previous.get("final_state") or "")
    previous_candidate = str(previous.get("candidate_state") or "")
    previous_streak = int(previous.get("candidate_streak") or 0)
    previous_days = int(previous.get("state_days") or 0)
    previous_cooldown = int(previous.get("cooldown_remaining") or 0)

    result = {
        **classified,
        "key": candidate,
        "final_state": candidate,
        "candidate_streak": 1,
        "state_days": 1,
        "cooldown_remaining": 0,
        "config_version": str(config["config_version"]),
        "config_hash": market_state_config_hash(),
        "transition_reason": "initial_state",
    }
    if candidate == "unknown":
        result["key"] = "unknown"
        result["transition_reason"] = "missing_required_inputs"
        return result
    if not previous_final or previous_final == "unknown":
        if candidate == "extreme_event":
            result["cooldown_remaining"] = int(
                transition["cooldown_after_extreme_trade_days"]
            )
            result["transition_reason"] = "initial_extreme_event"
        return result

    candidate_streak = previous_streak + 1 if candidate == previous_candidate else 1
    result["candidate_streak"] = candidate_streak

    if candidate == "extreme_event":
        result["key"] = "extreme_event"
        result["final_state"] = "extreme_event"
        result["state_days"] = previous_days + 1 if previous_final == candidate else 1
        result["cooldown_remaining"] = int(
            transition["cooldown_after_extreme_trade_days"]
        )
        result["transition_reason"] = "extreme_event_immediate"
        return result

    if previous_final == "extreme_event":
        result["key"] = str(transition["cooldown_state"])
        result["final_state"] = str(transition["cooldown_state"])
        result["state_days"] = 1
        result["cooldown_remaining"] = int(
            transition["cooldown_after_extreme_trade_days"]
        )
        result["transition_reason"] = "post_extreme_cooldown_started"
        return result

    if previous_cooldown > 0:
        result["key"] = str(transition["cooldown_state"])
        result["final_state"] = str(transition["cooldown_state"])
        result["state_days"] = (
            previous_days + 1
            if previous_final == result["final_state"]
            else 1
        )
        result["cooldown_remaining"] = max(0, previous_cooldown - 1)
        result["transition_reason"] = "post_extreme_cooldown_active"
        return result

    # Apply the frozen exit thresholds before evaluating a new state's enter
    # threshold. This is the hysteresis band that prevents a state from
    # oscillating near a single cut-off.
    risk = _number((snapshot or {}).get("risk_score"))
    change = _number((snapshot or {}).get("market_change_pct"))
    breadth = _number((snapshot or {}).get("breadth_pct"))
    trend = _number((snapshot or {}).get("trend_score"))
    switch = _number((snapshot or {}).get("switch_score"))
    thresholds = config["thresholds"]
    keep_previous = False
    if previous_final == "risk_declining":
        cfg = thresholds["risk_declining"]
        risk_flag = any(bool((snapshot or {}).get(flag)) for flag in cfg["explicit_flags"])
        keep_previous = (
            risk_flag
            or risk is None
            or change is None
            or risk >= float(cfg["risk_score_exit_lt"])
            or change < float(cfg["market_change_exit_gte"])
        )
    elif previous_final == "high_range":
        cfg = thresholds["high_range"]
        high_flag = any(bool((snapshot or {}).get(flag)) for flag in cfg["explicit_flags"])
        keep_previous = (
            high_flag
            or breadth is None
            or switch is None
            or breadth < float(cfg["breadth_exit_gte"])
            or switch >= float(cfg["switch_score_exit_lt"])
        )
    elif previous_final == "trend_bullish":
        cfg = thresholds["trend_bullish"]
        keep_previous = (
            trend is not None
            and breadth is not None
            and risk is not None
            and trend >= float(cfg["trend_score_exit_lt"])
            and breadth >= float(cfg["breadth_exit_lt"])
            and risk < float(cfg["risk_score_exit_gte"])
        )
    if keep_previous and candidate != "extreme_event":
        result["key"] = previous_final
        result["final_state"] = previous_final
        result["state_days"] = previous_days + 1
        result["transition_reason"] = "hysteresis_exit_threshold_not_met"
        return result

    if candidate == previous_final:
        result["key"] = previous_final
        result["final_state"] = previous_final
        result["state_days"] = previous_days + 1
        result["transition_reason"] = "state_continues"
        return result

    worsening = STATE_SEVERITY.get(candidate, 4) > STATE_SEVERITY.get(previous_final, 4)
    if worsening and bool(transition.get("worsening_transition_is_immediate")):
        result["state_days"] = 1
        result["transition_reason"] = "risk_worsening_immediate"
        return result

    confirm_days = int((transition["confirm_days"] or {}).get(candidate, 1))
    minimum_days = int((transition["minimum_state_days"] or {}).get(previous_final, 1))
    if candidate_streak >= confirm_days and previous_days >= minimum_days:
        result["state_days"] = 1
        result["transition_reason"] = "confirmed_transition"
        return result

    result["key"] = previous_final
    result["final_state"] = previous_final
    result["state_days"] = previous_days + 1
    result["transition_reason"] = (
        f"transition_pending:{candidate_streak}/{confirm_days};"
        f"state_days:{previous_days}/{minimum_days}"
    )
    return result
