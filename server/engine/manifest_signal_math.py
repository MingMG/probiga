"""Pure qualification of frozen original-score strategy candidates."""
from __future__ import annotations
import json
import math
from typing import Any
_ACTIONABLE_NEW_BUY_SIGNAL_STATUSES = frozenset({"CONFIRM", "BUY_READY"})

def _num(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _is_explicit_database_true(value: Any) -> bool:
    """Recognize only a real bool or a MySQL TINYINT(1) value."""
    return value is True or (type(value) is int and value == 1)


def _json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def strategy_signal_basis(
    row: dict[str, Any],
    strategy_key: str,
    score: float | None,
    *, manifest: dict[str, Any],
) -> dict[str, Any]:
    """Separate strategy-specific confirmation from the generic recommendation.

    The generic recommendation is deliberately conservative and may suspend a
    trend trade for valuation or a short trade for weak fundamentals.  Those
    are strategy-relative concerns, not universal data failures.  This helper
    keeps universal hard blocks authoritative while applying the frozen
    per-strategy block list before confirming BUY.
    """
    routing = manifest.get("paper_trial_routing") or {}
    raw_flags = _json_value(row.get("data_quality_flags"), [])
    flags = {
        str(value).strip()
        for value in (raw_flags if isinstance(raw_flags, list) else [])
        if str(value).strip()
    }
    hard_flags = {
        str(value)
        for value in (routing.get("hard_block_flags") or [])
    }
    strategy_flags = {
        str(value)
        for value in (
            (routing.get("strategy_block_flags") or {}).get(
                strategy_key,
                [],
            )
        )
    }
    hard_hits = sorted(flags & hard_flags)
    strategy_hits = sorted(flags & strategy_flags)
    recommend_status = str(
        row.get("recommend_status") or "SUSPENDED"
    ).upper()
    source_status = str(
        row.get("signal_status") or recommend_status or "WATCH"
    ).upper()
    risk_level = str(row.get("event_risk_level") or "DATA_BLOCKED").upper()
    chase_risk_status = str(
        row.get("chase_risk_status") or "DATA_BLOCKED"
    ).upper()
    ordinary_buy_eligible = _is_explicit_database_true(
        row.get("ordinary_buy_eligible")
    )
    publication_status = str(
        row.get("publication_status") or "PENDING"
    ).upper()
    item = next(
        (
            value
            for value in (manifest.get("strategies") or [])
            if str(value.get("key") or "") == strategy_key
        ),
        {},
    )
    parameters = item.get("parameters") or {}
    min_score = _num(parameters.get("min_score"), 100.0) or 100.0
    confirm_score = (
        _num(parameters.get("confirm_score"), min_score) or min_score
    )
    explicit_strategy_signal = (
        str(row.get("main_wave_signal") or "").upper()
        if strategy_key == "main_wave"
        else ""
    )

    # Exit/reduce signals are never suppressed by a new-buy qualification gate.
    if source_status in {"SELL_ALERT", "REDUCE"} or (
        strategy_key == "main_wave"
        and explicit_strategy_signal in {"SELL_ALERT", "REDUCE"}
    ):
        return {
            "direction": "SELL",
            "hard_block": False,
            "reason": "上游趋势失效或减仓信号已触发",
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if publication_status != "ACTIVE":
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": "推荐票池尚未通过调度后验证并激活",
            "hard_hits": [*hard_hits, "publication_not_active"],
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if recommend_status != "ALLOW":
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": str(
                row.get("recommend_reason")
                or f"recommend gate is {recommend_status}, not ALLOW"
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if source_status not in _ACTIONABLE_NEW_BUY_SIGNAL_STATUSES:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": f"upstream signal {source_status} is not confirmed",
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if chase_risk_status != "ALLOW" or not ordinary_buy_eligible:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": (
                "追高与成交能力硬门未显式通过："
                f"{chase_risk_status}"
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    if recommend_status == "BLOCK":
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": str(
                row.get("recommend_reason")
                or "基础数据或个股硬风险门槛未通过"
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }
    if risk_level in {"HIGH", "CRITICAL", "DATA_BLOCKED", "UNKNOWN"}:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": f"个股事件风险为 {risk_level}",
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }
    if hard_hits:
        return {
            "direction": "HOLD",
            "hard_block": True,
            "reason": "通用硬门槛触发：" + "、".join(hard_hits[:4]),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }
    if strategy_hits:
        return {
            "direction": "HOLD",
            "hard_block": False,
            "reason": (
                f"{strategy_key} 专属门槛触发："
                + "、".join(strategy_hits[:4])
            ),
            "hard_hits": hard_hits,
            "strategy_hits": strategy_hits,
            "min_score": min_score,
            "confirm_score": confirm_score,
        }

    confirmed = score is not None and score >= confirm_score
    if (
        strategy_key == "main_wave"
        and explicit_strategy_signal == "BUY_READY"
        and score is not None
        and score >= min_score
    ):
        confirmed = True
    return {
        "direction": "BUY" if confirmed else "HOLD",
        "hard_block": False,
        "reason": (
            f"{strategy_key} 独立得分 {float(score or 0):.1f}"
            f"/确认线 {confirm_score:.1f}"
        ),
        "hard_hits": hard_hits,
        "strategy_hits": strategy_hits,
        "min_score": min_score,
        "confirm_score": confirm_score,
    }
