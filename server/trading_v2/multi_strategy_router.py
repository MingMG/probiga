"""Regime-aware paper-trial routing for the unified stock strategy family.

This module does not invent a buy signal.  It only decides whether an upstream
BUY signal may enter the V2 paper portfolio competition under the current
market regime.  Hard event/data gates remain authoritative.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from .config import load_frozen_json


HARD_RISK_LEVELS = {"HIGH", "CRITICAL"}
NO_NEW_BUY_REGIMES = {"EXTREME", "DATA_BLOCKED"}


@lru_cache(maxsize=1)
def _routing_config() -> dict[str, Any]:
    manifest, _ = load_frozen_json("strategies/stock_strategy_v2.json")
    return dict(manifest.get("paper_trial_routing") or {})


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _reject(code: str, reason: str) -> dict[str, Any]:
    return {
        "eligible": False,
        "reason_code": code,
        "route_reason": reason,
        "opening_target_fraction": 0.0,
        "competition_score": 0.0,
    }


def evaluate_signal_route(
    signal: dict[str, Any],
    market_regime: str,
) -> dict[str, Any]:
    """Evaluate one already-computed strategy signal for paper competition."""
    regime = str(market_regime or "").upper()
    strategy_key = str(signal.get("strategy_key") or "")
    direction = str(signal.get("signal_direction") or "").upper()
    status = str(signal.get("signal_status") or "").upper()
    gate = str(signal.get("gate_status") or "").upper()
    risk = str(signal.get("risk_level") or "LOW").upper()

    if regime in NO_NEW_BUY_REGIMES:
        return _reject(
            f"MULTI_STRATEGY_REGIME_{regime}",
            f"{regime} 状态禁止新增股票仓位",
        )
    if direction != "BUY":
        return _reject("MULTI_STRATEGY_NOT_BUY", "上游没有形成买入方向")
    if gate == "BLOCK":
        return _reject(
            "MULTI_STRATEGY_HARD_GATE_BLOCK",
            str(signal.get("gate_reason") or "个股硬风险门槛未通过"),
        )
    if risk in HARD_RISK_LEVELS:
        return _reject(
            "MULTI_STRATEGY_EVENT_RISK_BLOCK",
            f"个股事件风险为 {risk}，不允许新增买入",
        )

    # Sector preheat is an independent, already-frozen strategy.  Keep its
    # original READY/PASS semantics, but pass its regime weight to sizing.
    if strategy_key == "sector_preheat":
        if status != "READY" or gate not in {"PASS", ""}:
            return _reject(
                "MULTI_STRATEGY_SECTOR_SIGNAL_NOT_READY",
                "板块预热信号尚未满足买入确认",
            )
        opening = min(
            0.5,
            max(0.0, _number(signal.get("effective_weight"), 0.5)),
        )
        return {
            "eligible": opening > 0,
            "reason_code": "",
            "route_reason": "板块预热信号通过原有确认门槛",
            "opening_target_fraction": round(opening, 4),
            "competition_score": round(
                _number(signal.get("raw_score"))
                * max(0.05, _number(signal.get("effective_weight"), 1.0)),
                4,
            ),
        }

    routing = _routing_config()
    regime_rules = dict(
        (routing.get("regimes") or {}).get(regime) or {}
    )
    rule = dict(regime_rules.get(strategy_key) or {})
    if not rule or not bool(rule.get("enabled", False)):
        return _reject(
            "MULTI_STRATEGY_DISABLED_FOR_REGIME",
            f"{strategy_key} 在 {regime} 状态不参与新增仓位竞争",
        )

    market_only_downgrade = bool(signal.get("market_only_downgrade"))
    allowed_statuses = {"READY"}
    if bool(rule.get("allow_market_reduced_watch")):
        allowed_statuses.add("WATCH")
    if status not in allowed_statuses:
        return _reject(
            "MULTI_STRATEGY_SIGNAL_NOT_CONFIRMED",
            f"信号状态 {status or 'UNKNOWN'} 未达到当前市场确认要求",
        )
    if gate == "REDUCE" and not (
        market_only_downgrade
        and bool(rule.get("allow_market_reduced_watch"))
    ):
        return _reject(
            "MULTI_STRATEGY_REDUCE_NOT_ROUTABLE",
            "该观察信号不是单纯由市场状态降权，不能升级为模拟买入",
        )
    if gate not in {"PASS", "", "REDUCE"}:
        return _reject(
            "MULTI_STRATEGY_GATE_NOT_ROUTABLE",
            f"门槛状态 {gate or 'UNKNOWN'} 不可参与竞争",
        )

    score = _number(signal.get("raw_score"))
    risk_reward = _number(signal.get("risk_reward_ratio"))
    data_quality = _number(signal.get("data_quality_score"))
    min_score = _number(rule.get("min_score"))
    min_risk_reward = _number(rule.get("min_risk_reward"))
    min_data_quality = _number(rule.get("min_data_quality"), 75.0)
    if score < min_score:
        return _reject(
            "MULTI_STRATEGY_SCORE_BELOW_REGIME_MIN",
            f"策略分 {score:.2f} 低于 {regime} 门槛 {min_score:.2f}",
        )
    if risk_reward < min_risk_reward:
        return _reject(
            "MULTI_STRATEGY_RISK_REWARD_BELOW_MIN",
            f"盈亏比 {risk_reward:.2f} 低于当前门槛 {min_risk_reward:.2f}",
        )
    if data_quality < min_data_quality:
        return _reject(
            "MULTI_STRATEGY_DATA_QUALITY_BELOW_MIN",
            f"数据质量 {data_quality:.1f} 低于当前门槛 {min_data_quality:.1f}",
        )

    effective_weight = max(
        0.0,
        _number(signal.get("effective_weight"), 1.0),
    )
    opening = min(
        0.5,
        max(0.0, _number(rule.get("opening_target_fraction"))),
        effective_weight,
    )
    if opening <= 0:
        return _reject(
            "MULTI_STRATEGY_ZERO_REGIME_WEIGHT",
            "策略在当前市场状态的有效权重为零",
        )
    competition_score = (
        score * effective_weight
        + min(max(risk_reward, 0.0), 5.0) * 2.0
    )
    return {
        "eligible": True,
        "reason_code": "",
        "route_reason": (
            f"{strategy_key} 通过 {regime} 动态门槛："
            f"策略分 {score:.2f}，盈亏比 {risk_reward:.2f}，"
            f"首仓系数 {opening:.2f}"
        ),
        "opening_target_fraction": round(opening, 4),
        "competition_score": round(competition_score, 4),
        "market_only_downgrade_accepted": bool(
            gate == "REDUCE" and market_only_downgrade
        ),
    }


def clear_routing_config_cache() -> None:
    """Test/deployment helper for frozen-manifest replacement."""
    _routing_config.cache_clear()
