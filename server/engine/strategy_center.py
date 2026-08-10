# -*- coding: utf-8 -*-
"""Strategy-center domain logic.

The module deliberately keeps the decision layer deterministic and explainable.
It adapts existing recommendation rows into a normalized, multi-strategy view;
it does not place orders or connect to a broker.
"""
from __future__ import annotations

import json
import logging
import math
import uuid
import hashlib
import copy
import threading
import time
from collections import OrderedDict, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.kline_data import get_kline_engine
from server.common.sql_reader import read_sql_rows
from server.common.versioned_strategy_config import (
    legacy_strategy_merge_map,
    load_market_state_config,
    load_stock_manifest,
    market_state_config_hash,
    register_versioned_strategy_configs,
    stock_manifest_hash,
    stock_strategy_catalog,
    strategy_score_field_map,
)
from server.engine.market_state_v2 import transition_market_state

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_REFERENCE_POOL_DIR = _PROJECT_ROOT / "data" / "strategy_center"
_MARKET_SNAPSHOT_CACHE: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
_MARKET_SNAPSHOT_CACHE_LOCK = threading.Lock()


STRATEGY_CATALOG: tuple[dict[str, Any], ...] = stock_strategy_catalog()

MARKET_STATES: dict[str, dict[str, Any]] = {
    "trend_bullish": {"name": "趋势偏多", "color": "#dc2626", "description": "指数趋势和市场宽度支持顺势研究"},
    "high_range": {"name": "高位震荡", "color": "#d97706", "description": "位置偏高但宽度收窄，降低追高"},
    "risk_declining": {"name": "风险下降", "color": "#2563eb", "description": "风险释放后出现止跌或修复，逐步恢复确认"},
    "extreme_event": {"name": "极端事件", "color": "#b91c1c", "description": "事件或波动异常，停止新增买入"},
}

STATE_MULTIPLIERS: dict[str, dict[str, float]] = {
    str(state): {str(key): float(value) for key, value in values.items()}
    for state, values in load_market_state_config()["strategy_multipliers"].items()
}

LEGACY_STRATEGY_MAP = legacy_strategy_merge_map()
_STRATEGY_SCORE_FIELDS = strategy_score_field_map()

_STRATEGY_BY_KEY = {item["key"]: item for item in STRATEGY_CATALOG}
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


def _clamp(value: Any, low: float = 0.0, high: float = 100.0, default: float | None = None) -> float | None:
    number = _num(value, default)
    if number is None:
        return default
    return round(max(low, min(high, number)), 2)


def _json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _json_text(value: Any, default: Any) -> str:
    return json.dumps(_json_value(value, default), ensure_ascii=False, separators=(",", ":"))


def normalize_trade_date(value: str | None) -> str:
    raw = str(value or "").strip()[:10]
    try:
        return date.fromisoformat(raw).isoformat() if raw else ""
    except ValueError:
        return ""


def strategy_item(strategy_key: str) -> dict[str, Any] | None:
    return _STRATEGY_BY_KEY.get(str(strategy_key or "").strip())


def market_state_info(state: str) -> dict[str, Any]:
    key = state if state in MARKET_STATES else "risk_declining"
    return {"key": key, **MARKET_STATES[key]}


def infer_market_state(
    snapshot: dict[str, Any] | None,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply the frozen V2 classifier, including hysteresis and cooldown."""
    result = transition_market_state(snapshot, previous=previous)
    key = str(result.get("final_state") or result.get("key") or "unknown")
    if key == "unknown":
        return {
            **result,
            "key": "unknown",
            "name": "数据不足",
            "color": "#64748b",
            "description": "缺少必要市场输入，禁止生成确定性新增买入动作",
            "confidence": 0.0,
        }
    confidence = {
        "extreme_event": 92.0,
        "risk_declining": 76.0,
        "high_range": 72.0,
        "trend_bullish": 80.0,
    }.get(key, 0.0)
    return {
        **market_state_info(key),
        **result,
        "key": key,
        "confidence": confidence,
    }


def performance_multiplier(metric: dict[str, Any] | None) -> float:
    metric = metric or {}
    sample_count = int(_num(metric.get("sample_count"), 0) or 0)
    if sample_count < 10:
        return 1.0
    win_rate = _num(metric.get("win_rate_pct", metric.get("win_rate")), 50.0) or 50.0
    avg_return = _num(metric.get("avg_return_pct", metric.get("avg_profit_rate")), 0.0) or 0.0
    if win_rate < 42 or avg_return < -1:
        return 0.72
    if win_rate >= 58 and avg_return >= 1:
        return 1.08
    return 1.0


def effective_weight(strategy_key: str, state: str, config: dict[str, Any] | None = None, metric: dict[str, Any] | None = None, data_quality: float = 1.0) -> dict[str, float | str]:
    strategy_key = LEGACY_STRATEGY_MAP.get(strategy_key, strategy_key) or ""
    item = strategy_item(strategy_key) or {"base_weight": 0.0}
    config = config or {}
    base = _num(config.get("base_weight"), _num(item.get("base_weight"), 1.0)) or 0.0
    state_multiplier = (STATE_MULTIPLIERS.get(state) or {}).get(strategy_key, 0.0 if state == "extreme_event" else 1.0)
    quality_multiplier = _clamp(data_quality, 0.0, 1.0, 1.0) or 1.0
    perf_multiplier = performance_multiplier(metric)
    enabled = bool(config.get("enabled", True))
    weight = base * state_multiplier * perf_multiplier * quality_multiplier
    if not enabled:
        weight = 0.0
    weight = round(max(0.0, min(1.5, weight)), 4)
    reason = []
    if not enabled:
        reason.append("策略已停用")
    if state_multiplier < 1:
        reason.append(f"市场状态系数 {state_multiplier:.2f}")
    elif state_multiplier > 1:
        reason.append(f"市场状态加权 {state_multiplier:.2f}")
    if perf_multiplier != 1:
        reason.append(f"复盘表现系数 {perf_multiplier:.2f}")
    if quality_multiplier < 1:
        reason.append(f"数据质量系数 {quality_multiplier:.2f}")
    return {
        "base_weight": round(base, 4),
        "state_multiplier": round(state_multiplier, 4),
        "performance_multiplier": round(perf_multiplier, 4),
        "data_quality_multiplier": round(quality_multiplier, 4),
        "effective_weight": weight,
        "weight_reason": "；".join(reason) or "按基础权重运行",
    }


def resolve_conflict(signals: Iterable[dict[str, Any]], market_state: str = "") -> dict[str, Any]:
    """Resolve opposing signals without hiding any source signal."""
    source = [dict(item) for item in signals if isinstance(item, dict)]
    if not source:
        return {
            "final_direction": "NO_SIGNAL", "final_status": "INSUFFICIENT_DATA", "buy_score": 0.0,
            "sell_score": 0.0, "hold_score": 0.0, "dominant_strategy": "", "conflict": False,
            "blocking_reasons": ["没有策略信号"], "conflict_summary": "暂无可用策略信号",
        }

    hard_blocks = [
        item for item in source
        if str(item.get("gate_status") or "").upper() == "BLOCK"
        or str(item.get("risk_level") or "").upper() == "CRITICAL"
    ]
    buy_score = sum((_num(item.get("effective_weight"), 0.0) or 0.0) * (_num(item.get("model_confidence"), 0.0) or 0.0) for item in source if str(item.get("signal_direction")) == "BUY")
    sell_score = sum((_num(item.get("effective_weight"), 0.0) or 0.0) * (_num(item.get("model_confidence"), 0.0) or 0.0) for item in source if str(item.get("signal_direction")) == "SELL")
    hold_score = sum((_num(item.get("effective_weight"), 0.0) or 0.0) * (_num(item.get("model_confidence"), 0.0) or 0.0) for item in source if str(item.get("signal_direction")) in {"HOLD", "NO_SIGNAL"})
    best = max(source, key=lambda item: (_num(item.get("effective_score"), 0.0) or 0.0, _num(item.get("model_confidence"), 0.0) or 0.0))
    reasons = [str(item.get("gate_reason") or "") for item in hard_blocks if item.get("gate_reason")]

    if hard_blocks:
        final_direction = "SELL" if any(str(item.get("signal_direction")) == "SELL" for item in hard_blocks) else "HOLD"
        return {
            "final_direction": final_direction, "final_status": "BLOCKED", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": bool(buy_score and sell_score),
            "blocking_reasons": reasons or ["存在硬性风险门禁"],
            "conflict_summary": "硬性风险门禁优先，阻断新增买入",
        }

    gap = abs(buy_score - sell_score)
    conflict = buy_score > 0 and sell_score > 0
    if conflict and gap < max(10.0, max(buy_score, sell_score) * 0.2):
        return {
            "final_direction": "HOLD", "final_status": "CONFLICT", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": True,
            "blocking_reasons": [], "conflict_summary": "多空有效权重接近，暂不形成新增买入信号",
        }

    if market_state == "extreme_event" and buy_score >= sell_score:
        return {
            "final_direction": "HOLD", "final_status": "BLOCKED", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
            "blocking_reasons": ["极端事件模式停止新增买入"], "conflict_summary": "极端事件模式下仅保留观察和防守信息",
        }

    if buy_score > sell_score and buy_score >= max(30.0, hold_score):
        status = "READY" if not conflict or gap >= 18 else "WATCH"
        if market_state in {"high_range", "risk_declining"}:
            status = "WATCH"
        return {
            "final_direction": "BUY", "final_status": status, "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
            "blocking_reasons": [], "conflict_summary": "有效权重偏多，但仍需满足价格与板块触发条件",
        }
    if sell_score > buy_score and sell_score >= max(30.0, hold_score):
        return {
            "final_direction": "SELL", "final_status": "SELL_ALERT", "buy_score": round(buy_score, 2),
            "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
            "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
            "blocking_reasons": [], "conflict_summary": "有效权重偏空，优先查看止损/减仓条件",
        }
    return {
        "final_direction": "HOLD", "final_status": "WATCH", "buy_score": round(buy_score, 2),
        "sell_score": round(sell_score, 2), "hold_score": round(hold_score, 2),
        "dominant_strategy": best.get("strategy_key", ""), "conflict": conflict,
        "blocking_reasons": [], "conflict_summary": "信号强度不足，维持观察",
    }


def _legacy_keys(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    raw = _json_value(row.get("suitable_strategies"), [])
    if not isinstance(raw, list):
        raw = [part.strip() for part in str(row.get("suitable_strategies") or "").split(",") if part.strip()]
    for value in raw + [row.get("primary_strategy"), row.get("strategy_profile")]:
        key = str(value or "").strip()
        if not key:
            continue
        mapped = LEGACY_STRATEGY_MAP.get(key, key)
        if mapped and mapped in _STRATEGY_BY_KEY and mapped not in values:
            values.append(mapped)
    if not values and (
        row.get("short_term_score") is not None
        or row.get("final_trade_score") is not None
    ):
        values.append("short_term")
    return values


def _score_for_strategy(row: dict[str, Any], strategy_key: str) -> float | None:
    field = _STRATEGY_SCORE_FIELDS.get(strategy_key)
    if not field:
        return None
    value = _clamp(row.get(field), default=None)
    return value if value is not None and value > 0 else None


def _strategy_signal_basis(
    row: dict[str, Any],
    strategy_key: str,
    score: float | None,
) -> dict[str, Any]:
    """Separate strategy-specific confirmation from the generic recommendation.

    The generic recommendation is deliberately conservative and may suspend a
    trend trade for valuation or a short trade for weak fundamentals.  Those
    are strategy-relative concerns, not universal data failures.  This helper
    keeps universal hard blocks authoritative while applying the frozen
    per-strategy block list before confirming BUY.
    """
    manifest = load_stock_manifest()
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


def adapt_recommendation_row(row: dict[str, Any], strategy_key: str, market: dict[str, Any], config: dict[str, Any] | None = None, metric: dict[str, Any] | None = None) -> dict[str, Any]:
    strategy_key = LEGACY_STRATEGY_MAP.get(strategy_key, strategy_key) or ""
    state = str(market.get("market_state") or "risk_declining")
    is_reference = bool(row.get("reference_fixture"))
    score = _score_for_strategy(row, strategy_key)
    data_quality_score = _clamp(row.get("data_quality_score"), default=0.0) or 0.0
    weight = effective_weight(strategy_key, state, config=config, metric=metric, data_quality=data_quality_score / 100.0)
    risk_level = str(row.get("event_risk_level") or "DATA_BLOCKED").upper()
    row_status = str(row.get("signal_status") or row.get("recommend_status") or "WATCH").upper()
    signal_basis = _strategy_signal_basis(row, strategy_key, score)
    direction = str(signal_basis["direction"])

    gate_status = "PASS"
    gate_reason = ""
    if not bool((config or {}).get("enabled", True)):
        gate_status, gate_reason = "BLOCK", "策略已停用"
    elif bool(signal_basis.get("hard_block")):
        gate_status, gate_reason = "BLOCK", str(
            signal_basis.get("reason") or "个股硬风险门槛未通过"
        )
    elif state == "extreme_event" and direction == "BUY":
        gate_status, gate_reason = "BLOCK", "极端事件模式停止新增买入"
    elif state in {"high_range", "risk_declining"} and direction == "BUY":
        gate_status, gate_reason = "REDUCE", f"{MARKET_STATES[state]['name']}模式自动降权，需二次确认"
    elif str(row.get("recommend_status") or "").upper() == "BLOCK":
        gate_status, gate_reason = "BLOCK", str(row.get("recommend_reason") or "基础推荐门禁未通过")
    elif direction == "HOLD" and signal_basis.get("strategy_hits"):
        gate_status, gate_reason = "REDUCE", str(
            signal_basis.get("reason") or "策略专属条件尚未满足"
        )

    confidence = _clamp(row.get("confidence_score"), default=None)
    if confidence is None and score is not None:
        confidence = _clamp(50 + abs(score - 50) * 0.6, default=50.0)
    if score is None and not is_reference:
        status = "INSUFFICIENT_DATA"
        direction = "NO_SIGNAL"
        confidence = None
        gate_status = "BLOCK"
        gate_reason = "当前策略缺少独立可追溯分数"
    elif gate_status == "BLOCK":
        status = "BLOCKED"
    elif gate_status == "REDUCE":
        status = "WATCH"
    elif direction == "BUY":
        status = "READY"
    elif direction == "SELL":
        status = "SELL_ALERT"
    else:
        status = "WATCH"

    if is_reference and score is None:
        # A dated reference pool is deliberately a watchlist, not a fabricated
        # model score. It remains visible while the price/sector confirmation
        # gates are pending.
        direction = str(row.get("reference_signal_direction") or "HOLD").upper()
        if direction not in {"BUY", "SELL", "HOLD"}:
            direction = "HOLD"
        if gate_status != "BLOCK":
            gate_status = "REDUCE"
            gate_reason = "日期化研究候选，等待盘前/盘中价格与板块条件确认"
        status = "WATCH" if gate_status != "BLOCK" else "BLOCKED"
        confidence = None

    evidence = _json_value(row.get("evidence_chain_json"), [])
    if not isinstance(evidence, list):
        evidence = [evidence]
    evidence = evidence[:30]
    evidence.append({
        "module": "strategy_center",
        "text": gate_reason or "兼容现有推荐数据生成策略信号",
        "source": "dated_reference_pool" if is_reference else "existing_recommendation",
    })
    return {
        "stock_code": str(row.get("stock_code") or "").zfill(6),
        "stock_name": row.get("short_name") or row.get("stock_name") or row.get("stock_code") or "",
        "strategy_key": strategy_key,
        "strategy_name": (_STRATEGY_BY_KEY.get(strategy_key) or {}).get("name", strategy_key),
        "market_state": state,
        "signal_direction": direction,
        "signal_status": status,
        "raw_score": score,
        "effective_score": round((score or 0.0) * float(weight["effective_weight"]), 2) if score is not None else None,
        "model_confidence": confidence,
        "today_signal": str(row.get("signal_reason") or row.get("recommend_reason") or row.get("reason") or "")[:500],
        "entry_low": row.get("entry_price_low"),
        "entry_high": row.get("entry_price_high"),
        "trigger_conditions": _json_value(row.get("entry_conditions_json"), []),
        "stop_loss": row.get("stop_loss_price") or row.get("trend_stop_price"),
        "take_profit_1": row.get("take_profit_1"),
        "take_profit_2": row.get("take_profit_2"),
        "no_chase_price": row.get("no_chase_price") or row.get("resistance_price"),
        "risk_level": risk_level,
        # Preserve the upstream fact. A strategy score may consume a
        # CONFIRM/BUY_READY signal, but must not manufacture one from WATCH.
        "source_signal_status": row_status,
        "source_recommend_status": str(
            row.get("recommend_status") or ""
        ).upper(),
        "source_chase_risk_status": str(
            row.get("chase_risk_status") or "DATA_BLOCKED"
        ).upper(),
        "source_ordinary_buy_eligible": _is_explicit_database_true(
            row.get("ordinary_buy_eligible")
        ),
        "chase_risk_status": str(
            row.get("chase_risk_status") or "DATA_BLOCKED"
        ).upper(),
        "ordinary_buy_eligible": _is_explicit_database_true(
            row.get("ordinary_buy_eligible")
        ),
        "market_only_downgrade": bool(
            gate_status == "REDUCE"
            and direction == "BUY"
            and state in {"high_range", "risk_declining"}
            and not bool(signal_basis.get("hard_block"))
            and not bool(signal_basis.get("strategy_hits"))
        ),
        "signal_basis": signal_basis,
        "risk_reward_ratio": row.get("risk_reward_ratio"),
        "gate_status": gate_status,
        "gate_reason": gate_reason,
        "effective_weight": weight["effective_weight"],
        "weight_detail": weight,
        "evidence_chain": evidence,
        "data_date": str(row.get("pick_date") or "")[:10],
        "data_quality_score": data_quality_score,
        "adapter_mode": "dated_reference_pool_adapter" if is_reference else "legacy_recommendation_adapter",
        "model_version": row.get("model_version") or "legacy-adapter",
        "reference_fixture": is_reference,
        "reference_priority": row.get("reference_priority"),
        "reference_source": row.get("reference_source"),
        "reference_as_of_date": row.get("reference_as_of_date"),
        "theme_code": (
            row.get("sector_industry_name")
            or row.get("industry_name")
            or row.get("industry")
            or ""
        ),
        "db_verified": bool(row.get("db_verified")),
        "db_close": row.get("db_close"),
        "db_verification_reason": row.get("db_verification_reason"),
        "position_cap_pct": row.get("position_cap_pct"),
        "pool_cap_pct": row.get("pool_cap_pct"),
        "global_invalidation_condition": row.get("global_invalidation_condition"),
    }


def aggregate_candidates(rows: Iterable[dict[str, Any]], market: dict[str, Any], configs: dict[str, dict[str, Any]] | None = None, metrics: dict[str, dict[str, Any]] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    configs = configs or {}
    metrics = metrics or {}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for key in _legacy_keys(row):
            signal = adapt_recommendation_row(row, key, market, configs.get(key), metrics.get(key))
            if signal.get("stock_code"):
                grouped[signal["stock_code"]].append(signal)

    candidates: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for code, signals in grouped.items():
        decision = resolve_conflict(signals, str(market.get("market_state") or ""))
        best = max(signals, key=lambda item: (_num(item.get("effective_score"), 0.0) or 0.0, _num(item.get("model_confidence"), 0.0) or 0.0))
        candidate = {
            "priority": best.get("reference_priority") or ("A" if decision["final_status"] in {"READY", "WATCH"} and decision["final_direction"] == "BUY" else "B"),
            "stock_code": code,
            "stock_name": best.get("stock_name") or code,
            "final_direction": decision["final_direction"],
            "final_status": decision["final_status"],
            "model_confidence": max((_num(item.get("model_confidence"), 0.0) or 0.0 for item in signals), default=0.0) or None,
            "today_signal": best.get("today_signal") or decision.get("conflict_summary"),
            "entry_low": best.get("entry_low"),
            "entry_high": best.get("entry_high"),
            "trigger_conditions": best.get("trigger_conditions") or [],
            "stop_loss": best.get("stop_loss"),
            "take_profit_1": best.get("take_profit_1"),
            "take_profit_2": best.get("take_profit_2"),
            "no_chase_price": best.get("no_chase_price"),
            "risk_level": max((str(item.get("risk_level") or "LOW") for item in signals), key=lambda value: {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}.get(value, 0)),
            "risk_reward_ratio": best.get("risk_reward_ratio"),
            "dominant_strategy": decision.get("dominant_strategy") or best.get("strategy_key"),
            "strategies": sorted({item.get("strategy_key") for item in signals if item.get("strategy_key")}),
            "buy_score": decision["buy_score"],
            "sell_score": decision["sell_score"],
            "hold_score": decision["hold_score"],
            "conflict": decision["conflict"],
            "conflict_summary": decision["conflict_summary"],
            "blocking_reasons": decision["blocking_reasons"],
            "strategy_signals": signals,
            "data_date": best.get("data_date"),
            "adapter_mode": best.get("adapter_mode") or "legacy_recommendation_adapter",
            "reference_fixture": bool(best.get("reference_fixture")),
            "reference_source": best.get("reference_source"),
            "reference_as_of_date": best.get("reference_as_of_date"),
            "theme_code": best.get("theme_code") or "",
            "db_verified": all(bool(item.get("db_verified")) for item in signals if item.get("reference_fixture")) if any(item.get("reference_fixture") for item in signals) else None,
            "db_close": best.get("db_close"),
            "db_verification_reason": best.get("db_verification_reason"),
            "position_cap_pct": best.get("position_cap_pct"),
            "pool_cap_pct": best.get("pool_cap_pct"),
            "global_invalidation_condition": best.get("global_invalidation_condition"),
        }
        candidates.append(candidate)
        if decision["conflict"] or decision["final_status"] in {"BLOCKED", "CONFLICT"}:
            conflicts.append({
                "stock_code": code,
                "stock_name": candidate["stock_name"],
                "market_state": market.get("market_state"),
                "decision": decision,
                "signals": signals,
            })
    candidates.sort(key=lambda item: ({"READY": 0, "WATCH": 1, "CONFLICT": 2, "SELL_ALERT": 3, "BLOCKED": 4, "INSUFFICIENT_DATA": 5}.get(item.get("final_status"), 9), -(item.get("model_confidence") or 0), item.get("stock_code", "")))
    return candidates, conflicts


def _db_read(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    engine = get_engine()
    return read_sql_rows(engine, sql, params, context="strategy_center")


def _db_write(sql: str, params: dict[str, Any] | None = None) -> None:
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(text(sql), params or {})


def _table_exists(table_name: str) -> bool:
    try:
        rows = _db_read("""
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = :table_name
        """, {"table_name": table_name})
        return bool(rows and int(rows[0].get("cnt") or 0))
    except Exception:
        return False


def _kline_table_exists(table_name: str) -> bool:
    try:
        rows = read_sql_rows(
            get_kline_engine(),
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = :table_name
            """,
            {"table_name": table_name},
            context="strategy_center_kline_table",
        )
        return bool(rows and int(rows[0].get("cnt") or 0))
    except Exception:
        return False


def ensure_strategy_center_tables() -> None:
    """Create only strategy-center tables; existing recommendation tables are untouched."""
    ddl = (
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_config (
            strategy_key VARCHAR(40) PRIMARY KEY,
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            base_weight DECIMAL(8,4) NOT NULL DEFAULT 1.0,
            config_json LONGTEXT,
            version INT NOT NULL DEFAULT 1,
            updated_by VARCHAR(80) NOT NULL DEFAULT 'system',
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_run (
            run_uid VARCHAR(40) PRIMARY KEY,
            trade_date DATE NOT NULL,
            market_state VARCHAR(40) NOT NULL DEFAULT 'unknown',
            state_confidence DECIMAL(6,2) DEFAULT NULL,
            source_status VARCHAR(20) NOT NULL DEFAULT 'degraded',
            model_version VARCHAR(40) DEFAULT '',
            status VARCHAR(20) NOT NULL DEFAULT 'running',
            signal_count INT NOT NULL DEFAULT 0,
            candidate_count INT NOT NULL DEFAULT 0,
            conflict_count INT NOT NULL DEFAULT 0,
            error_message VARCHAR(500) DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at DATETIME DEFAULT NULL,
            KEY idx_strategy_center_run_date (trade_date, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_signal (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_uid VARCHAR(40) NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(10) NOT NULL,
            stock_name VARCHAR(80) DEFAULT '',
            strategy_key VARCHAR(40) NOT NULL,
            market_state VARCHAR(40) DEFAULT '',
            signal_direction VARCHAR(20) NOT NULL DEFAULT 'NO_SIGNAL',
            signal_status VARCHAR(30) NOT NULL DEFAULT 'INSUFFICIENT_DATA',
            raw_score DECIMAL(8,2) DEFAULT NULL,
            effective_score DECIMAL(8,2) DEFAULT NULL,
            model_confidence DECIMAL(8,2) DEFAULT NULL,
            effective_weight DECIMAL(8,4) DEFAULT NULL,
            risk_level VARCHAR(20) DEFAULT 'LOW',
            gate_status VARCHAR(20) DEFAULT 'PASS',
            gate_reason VARCHAR(500) DEFAULT '',
            entry_low DECIMAL(12,4) DEFAULT NULL,
            entry_high DECIMAL(12,4) DEFAULT NULL,
            stop_loss DECIMAL(12,4) DEFAULT NULL,
            take_profit_1 DECIMAL(12,4) DEFAULT NULL,
            take_profit_2 DECIMAL(12,4) DEFAULT NULL,
            no_chase_price DECIMAL(12,4) DEFAULT NULL,
            risk_reward_ratio DECIMAL(8,2) DEFAULT NULL,
            today_signal VARCHAR(500) DEFAULT '',
            trigger_conditions_json LONGTEXT,
            evidence_chain_json LONGTEXT,
            data_snapshot_json LONGTEXT,
            model_version VARCHAR(40) DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_strategy_center_signal_date (trade_date, strategy_key),
            KEY idx_strategy_center_signal_stock (trade_date, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_conflict (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_uid VARCHAR(40) NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(10) NOT NULL,
            stock_name VARCHAR(80) DEFAULT '',
            market_state VARCHAR(40) DEFAULT '',
            final_direction VARCHAR(20) DEFAULT 'NO_SIGNAL',
            final_status VARCHAR(30) DEFAULT 'INSUFFICIENT_DATA',
            buy_score DECIMAL(10,2) DEFAULT 0,
            sell_score DECIMAL(10,2) DEFAULT 0,
            hold_score DECIMAL(10,2) DEFAULT 0,
            decision_json LONGTEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_strategy_center_conflict_date (trade_date, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_metric (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            as_of_date DATE NOT NULL,
            strategy_key VARCHAR(40) NOT NULL,
            sample_count INT NOT NULL DEFAULT 0,
            today_signal_count INT NOT NULL DEFAULT 0,
            return_pct DECIMAL(10,4) DEFAULT NULL,
            max_drawdown_pct DECIMAL(10,4) DEFAULT NULL,
            win_rate_pct DECIMAL(10,4) DEFAULT NULL,
            profit_factor DECIMAL(10,4) DEFAULT NULL,
            avg_return_pct DECIMAL(10,4) DEFAULT NULL,
            source VARCHAR(40) DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_strategy_center_metric (as_of_date, strategy_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_strategy_center_audit (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            strategy_key VARCHAR(40) NOT NULL,
            action VARCHAR(40) NOT NULL,
            old_value VARCHAR(500) DEFAULT '',
            new_value VARCHAR(500) DEFAULT '',
            reason VARCHAR(500) DEFAULT '',
            operator VARCHAR(80) DEFAULT 'api',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_strategy_center_audit (strategy_key, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    )
    for statement in ddl:
        _db_write(statement)
    registration = register_versioned_strategy_configs(get_engine())
    manifest = load_stock_manifest()
    manifest_items = {
        str(item["key"]): item for item in manifest["strategies"]
    }
    for item in STRATEGY_CATALOG:
        _db_write("""
            INSERT INTO st_strategy_center_config (strategy_key, enabled, base_weight, config_json, version, updated_by)
            VALUES (:strategy_key, 1, :base_weight, :config_json, 2, 'manifest_registry')
            ON DUPLICATE KEY UPDATE
                config_json = VALUES(config_json),
                version = GREATEST(version, VALUES(version)),
                updated_by = VALUES(updated_by)
        """, {
            "strategy_key": item["key"],
            "base_weight": float(item["base_weight"]),
            "config_json": json.dumps(
                {
                    **manifest_items[item["key"]],
                    "manifest_version": registration["stock_manifest_version"],
                    "config_hash": registration["stock_manifest_hash"],
                },
                ensure_ascii=False,
            ),
        })
    active_keys = sorted(_STRATEGY_BY_KEY)
    placeholders = ", ".join(f":active_{index}" for index in range(len(active_keys)))
    _db_write(
        f"""
        UPDATE st_strategy_center_config
        SET enabled = 0, updated_by = 'legacy_merge_v2'
        WHERE strategy_key NOT IN ({placeholders})
        """,
        {f"active_{index}": key for index, key in enumerate(active_keys)},
    )


def load_strategy_configs() -> dict[str, dict[str, Any]]:
    result = {
        item["key"]: {"enabled": True, "base_weight": item["base_weight"], "version": 1, "updated_by": "default"}
        for item in STRATEGY_CATALOG
    }
    if not _table_exists("st_strategy_center_config"):
        return result
    try:
        rows = _db_read("SELECT strategy_key, enabled, base_weight, config_json, version, updated_by, updated_at FROM st_strategy_center_config")
        for row in rows:
            key = str(row.get("strategy_key") or "")
            if key in result:
                result[key].update({
                    "enabled": bool(int(row.get("enabled") or 0)),
                    "base_weight": _num(row.get("base_weight"), result[key]["base_weight"]),
                    "version": int(row.get("version") or 1),
                    "updated_by": row.get("updated_by") or "system",
                    "updated_at": str(row.get("updated_at") or ""),
                    "config": _json_value(row.get("config_json"), {}),
                })
    except Exception as exc:
        logger.debug("strategy center config fallback: %s", exc)
    return result


def load_strategy_metrics(as_of_date: str) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    if _table_exists("st_strategy_center_metric"):
        try:
            rows = _db_read("""
                SELECT m.*
                FROM st_strategy_center_metric m
                INNER JOIN (
                    SELECT strategy_key, MAX(as_of_date) AS latest_date
                    FROM st_strategy_center_metric
                    WHERE as_of_date <= :as_of_date
                    GROUP BY strategy_key
                ) latest ON latest.strategy_key = m.strategy_key AND latest.latest_date = m.as_of_date
            """, {"as_of_date": as_of_date})
            for row in rows:
                metrics[str(row.get("strategy_key") or "")] = row
        except Exception as exc:
            logger.debug("strategy center metric fallback: %s", exc)

    # Backfill visible metrics from the existing simulated-trade ledger until
    # the strategy-center metric job has produced its first snapshot. The
    # drawdown is calculated on the realized-return curve, not from a single
    # losing trade.
    if _table_exists("st_sim_position"):
        try:
            rows = _db_read("""
                SELECT strategy_type, sell_date, id, profit, profit_rate
                FROM st_sim_position
                WHERE status = 'sold' AND sell_date IS NOT NULL AND sell_date <= :as_of_date
                ORDER BY strategy_type, sell_date, id
                LIMIT 10000
            """, {"as_of_date": as_of_date})
            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                raw_key = str(row.get("strategy_type") or "")
                key = LEGACY_STRATEGY_MAP.get(raw_key, raw_key)
                if key in _STRATEGY_BY_KEY:
                    grouped[key].append(row)
            for key, items in grouped.items():
                if key in metrics or not items:
                    continue
                returns = [_num(item.get("profit_rate"), 0.0) or 0.0 for item in items]
                gross_profit = sum(value for value in returns if value > 0)
                gross_loss = abs(sum(value for value in returns if value < 0))
                cumulative = 0.0
                peak = 0.0
                max_drawdown = 0.0
                for value in returns:
                    cumulative += value
                    peak = max(peak, cumulative)
                    max_drawdown = min(max_drawdown, cumulative - peak)
                metrics[key] = {
                    "strategy_key": key,
                    "as_of_date": as_of_date,
                    "sample_count": len(items),
                    "return_pct": round(cumulative, 4),
                    "max_drawdown_pct": round(max_drawdown, 4),
                    "win_rate_pct": round(sum(value > 0 for value in returns) / len(returns) * 100, 4),
                    "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss else None,
                    "avg_return_pct": round(sum(returns) / len(returns), 4),
                    "source": "st_sim_position",
                }
        except Exception as exc:
            logger.debug("strategy center simulated metric fallback: %s", exc)

    # The recommendation ledger contains historical forward-review labels.
    # Use them only for the four registered strategies; disabled legacy labels
    # must not silently reappear as synthetic strategy cards.
    if _table_exists("st_recommended_stocks"):
        try:
            available = _table_columns("st_recommended_stocks")
            review_field = next((field for field in ("review_5d_pct", "review_3d_pct", "review_1d_pct") if field in available), "")
            if {"suitable_strategies", "pick_date", review_field}.issubset(available):
                rows = _db_read(
                    f"SELECT suitable_strategies, primary_strategy, strategy_profile, `{review_field}` AS forward_return "
                    "FROM st_recommended_stocks "
                    f"WHERE pick_date <= :as_of_date AND `{review_field}` IS NOT NULL LIMIT 20000",
                    {"as_of_date": as_of_date},
                )
                grouped: dict[str, list[float]] = defaultdict(list)
                for row in rows:
                    raw = _json_value(row.get("suitable_strategies"), [])
                    if not isinstance(raw, list):
                        raw = [part.strip() for part in str(raw or "").split(",") if part.strip()]
                    raw.extend([row.get("primary_strategy"), row.get("strategy_profile")])
                    keys = {LEGACY_STRATEGY_MAP.get(str(value or "").strip(), str(value or "").strip()) for value in raw}
                    value = _num(row.get("forward_return"), None)
                    if value is None:
                        continue
                    for key in keys:
                        if key in _STRATEGY_BY_KEY:
                            grouped[key].append(value)
                for key, returns in grouped.items():
                    if key in metrics or not returns:
                        continue
                    wins = [value for value in returns if value > 0]
                    losses = [value for value in returns if value < 0]
                    cumulative = sum(returns)
                    peak = 0.0
                    curve = 0.0
                    drawdown = 0.0
                    for value in returns:
                        curve += value
                        peak = max(peak, curve)
                        drawdown = min(drawdown, curve - peak)
                    metrics[key] = {
                        "strategy_key": key,
                        "as_of_date": as_of_date,
                        "sample_count": len(returns),
                        "return_pct": round(cumulative / len(returns), 4),
                        "max_drawdown_pct": round(min(returns), 4),
                        "win_rate_pct": round(len(wins) / len(returns) * 100, 4),
                        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else None,
                        "avg_return_pct": round(cumulative / len(returns), 4),
                        "source": f"st_recommended_stocks_{review_field}",
                        "model_status": "historical_review",
                        "metric_note": f"{review_field} 横截面复盘基线，不等同于独立模型回测",
                    }
        except Exception as exc:
            logger.debug("strategy center recommendation review metric fallback: %s", exc)
    return metrics


def load_reference_candidate_pool(trade_date: str) -> dict[str, Any] | None:
    """Load an exact-date research pool without changing production signals.

    The pool is a dated research artifact, not a permanent stock-code rule. It
    is only eligible for the exact ``trade_date`` encoded in its filename and
    remains explicitly marked as reference data in every downstream signal.
    """
    target = normalize_trade_date(trade_date)
    if not target:
        return None
    path = _REFERENCE_POOL_DIR / f"a_share_pool_{target}.json"
    try:
        if not path.is_file():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or normalize_trade_date(payload.get("trade_date")) != target:
            return None
        candidates = payload.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            return None
        payload["_path"] = str(path.relative_to(_PROJECT_ROOT))
        return payload
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("strategy center reference pool unavailable: %s", exc)
        return None


def _table_columns(table_name: str) -> set[str]:
    try:
        return {
            str(row.get("COLUMN_NAME") or "")
            for row in _db_read("""
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
            """, {"table_name": table_name})
            if row.get("COLUMN_NAME")
        }
    except Exception:
        return set()


def _reference_db_crosscheck(candidates: list[dict[str, Any]], as_of_date: str) -> dict[str, dict[str, Any]]:
    """Cross-check dated reference prices against the operational market tables."""
    codes = [str(item.get("stock_code") or "").zfill(6) for item in candidates if item.get("stock_code")]
    if not codes or not as_of_date:
        return {}
    params = {f"code_{index}": code for index, code in enumerate(codes)}
    placeholders = ", ".join(f":code_{index}" for index in range(len(codes)))
    result: dict[str, dict[str, Any]] = {}
    for table_name in ("sm_stock_snapshot", "sm_stock_kline"):
        columns = _table_columns(table_name)
        if not {"stock_code", "trade_date"}.issubset(columns):
            continue
        selected = ["stock_code", "trade_date"]
        for column in ("close", "price", "change_pct", "amount", "k_type"):
            if column in columns:
                selected.append(column)
        where = f"trade_date = :as_of_date AND stock_code IN ({placeholders})"
        if table_name == "sm_stock_kline" and "k_type" in columns:
            where += " AND (k_type = 1 OR k_type IS NULL)"
        params_with_date = {**params, "as_of_date": as_of_date}
        try:
            rows = _db_read(
                f"SELECT {', '.join(f'`{column}`' for column in selected)} FROM {table_name} WHERE {where}",
                params_with_date,
            )
        except Exception as exc:
            logger.debug("reference pool database cross-check failed for %s: %s", table_name, exc)
            continue
        for row in rows:
            code = str(row.get("stock_code") or "").zfill(6)
            if code in result:
                continue
            result[code] = {
                "db_verified": True,
                "db_table": table_name,
                "db_trade_date": str(row.get("trade_date") or "")[:10],
                "db_close": row.get("close") if row.get("close") is not None else row.get("price"),
                "db_price": row.get("price") if row.get("price") is not None else row.get("close"),
                "db_change_pct": row.get("change_pct"),
                "db_amount": row.get("amount"),
                "price": row.get("price") if row.get("price") is not None else row.get("close"),
                "change_pct": row.get("change_pct"),
                "db_verification_reason": f"已从生产库 {table_name} 交叉验证 {as_of_date} 收盘记录",
            }
    for code in codes:
        result.setdefault(code, {
            "db_verified": False,
            "db_verification_reason": f"生产库中未找到 {as_of_date} 的收盘记录",
        })
    return result


def _reference_candidate_rows(pool: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    as_of_date = normalize_trade_date(pool.get("reference_as_of_date") or pool.get("data_date"))
    candidates = [item for item in pool.get("candidates", []) if isinstance(item, dict)]
    crosscheck = _reference_db_crosscheck(candidates, as_of_date)
    position_limits = pool.get("position_limits") if isinstance(pool.get("position_limits"), dict) else {}
    global_gate = pool.get("global_gate") if isinstance(pool.get("global_gate"), dict) else {}
    rows: list[dict[str, Any]] = []
    for item in candidates[:max(1, min(500, int(limit)))]:
        code = str(item.get("stock_code") or "").zfill(6)
        strategy_keys = [
            str(value).strip() for value in (item.get("strategy_keys") or [])
            if str(value).strip() in _STRATEGY_BY_KEY
        ]
        verified = crosscheck.get(code) or {}
        selection_reason = str(item.get("selection_reason") or item.get("strategy_label") or "")[:500]
        rows.append({
            "stock_code": code,
            "short_name": item.get("stock_name") or code,
            "pick_date": as_of_date,
            "suitable_strategies": json.dumps(strategy_keys, ensure_ascii=False),
            "primary_strategy": strategy_keys[0] if strategy_keys else "",
            "strategy_profile": strategy_keys[0] if strategy_keys else "",
            "signal_status": "WATCH",
            "recommend_status": "WATCH",
            "signal_reason": selection_reason,
            "recommend_reason": selection_reason,
            "reason": selection_reason,
            "entry_price_low": item.get("observation_low"),
            "entry_price_high": item.get("observation_high"),
            "stop_loss_price": item.get("stop_loss"),
            "take_profit_1": item.get("take_profit_1"),
            "take_profit_2": item.get("take_profit_2"),
            "resistance_price": item.get("no_chase_price"),
            "no_chase_price": item.get("no_chase_price"),
            "entry_conditions_json": json.dumps(item.get("trigger_conditions") or [], ensure_ascii=False),
            "event_risk_level": str(item.get("risk_level") or "HIGH").upper(),
            "model_version": "reference-pool-v1",
            "data_quality_score": 100 if verified.get("db_verified") else 75,
            "reference_fixture": True,
            "reference_priority": item.get("priority") or "B",
            "reference_source": pool.get("source") or "dated_reference_pool",
            "reference_trade_date": pool.get("trade_date"),
            "reference_as_of_date": as_of_date,
            "reference_strategy_label": item.get("strategy_label") or "",
            "reference_signal_direction": item.get("reference_signal_direction") or "HOLD",
            "position_cap_pct": position_limits.get("single_pct"),
            "pool_cap_pct": position_limits.get("aggregate_pct"),
            "global_invalidation_condition": global_gate.get("invalidation_condition") or "",
            **verified,
        })
    return rows


def latest_recommendation_date(requested: str = "") -> str:
    requested = normalize_trade_date(requested)
    if requested:
        return requested
    try:
        rows = _db_read("SELECT MAX(pick_date) AS trade_date FROM st_recommended_stocks")
        return normalize_trade_date(str(rows[0].get("trade_date") or "")[:10]) if rows else ""
    except Exception:
        return ""


def load_recommendation_rows(trade_date: str, limit: int = 200) -> list[dict[str, Any]]:
    target = normalize_trade_date(trade_date) or latest_recommendation_date()
    if target:
        reference_pool = load_reference_candidate_pool(target)
        if reference_pool:
            return _reference_candidate_rows(reference_pool, limit)
    if not _table_exists("st_recommended_stocks"):
        return []
    columns = {
        "stock_code", "short_name", "pick_date", "ai_score", "final_trade_score", "long_term_score", "short_term_score",
        "ultra_short_score", "swing_score", "main_wave_score", "trend_hold_score", "main_wave_signal", "main_wave_reason",
        "quality_score", "entry_score", "valuation", "fundamental", "technical", "sector_rotation_score", "sector_width_pct",
        "heat_overload_score", "confidence_score", "event_score", "event_risk_level", "recommend_status", "recommend_reason",
        "chase_risk_status", "ordinary_buy_eligible",
        "signal_status", "signal_reason", "primary_strategy", "strategy_profile", "suitable_strategies", "entry_price_low",
        "entry_price_high", "stop_loss_price", "trend_stop_price", "take_profit_1", "take_profit_2", "resistance_price",
        "risk_reward_ratio", "entry_conditions_json", "evidence_chain_json", "data_quality_score", "data_quality_flags", "model_version",
        "price", "change_pct", "max_holding_days", "suggested_position", "no_chase_price",
        "industry_name",
    }
    try:
        available = {row["COLUMN_NAME"] for row in _db_read("""
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'st_recommended_stocks'
        """)}
        selected = [column for column in columns if column in available]
        if "stock_code" not in selected or "pick_date" not in selected:
            return []
        if not target:
            return []
        limit = max(1, min(500, int(limit)))
        select_sql = ", ".join(f"`{column}`" for column in sorted(selected))
        order_column = "final_trade_score" if "final_trade_score" in selected else ("ai_score" if "ai_score" in selected else "stock_code")
        rows = _db_read(
            f"SELECT {select_sql} FROM st_recommended_stocks "
            f"WHERE pick_date = :trade_date "
            f"ORDER BY `{order_column}` DESC LIMIT {limit}",
            {"trade_date": target},
        )
        if rows and not all(
            str(row.get("industry_name") or "").strip() for row in rows
        ):
            try:
                industry_rows = _db_read(
                    """
                    SELECT stock_code, industry_name
                    FROM qmt_industry_member_snapshot
                    WHERE snapshot_date = (
                        SELECT MAX(snapshot_date)
                        FROM qmt_industry_member_snapshot
                        WHERE snapshot_date <= :trade_date
                    )
                      AND industry_name IS NOT NULL
                      AND industry_name <> ''
                    ORDER BY stock_code, industry_code
                    """,
                    {"trade_date": target},
                )
                industry_by_code: dict[str, str] = {}
                for item in industry_rows:
                    code = str(item.get("stock_code") or "").zfill(6)
                    name = str(item.get("industry_name") or "").strip()
                    if code and name and code not in industry_by_code:
                        industry_by_code[code] = name
                for row in rows:
                    if not str(row.get("industry_name") or "").strip():
                        row["industry_name"] = industry_by_code.get(
                            str(row.get("stock_code") or "").zfill(6),
                            "",
                        )
            except Exception as exc:
                logger.debug(
                    "strategy center industry snapshot fallback: %s",
                    exc,
                )
        return rows
    except Exception as exc:
        logger.debug("strategy center recommendation fallback: %s", exc)
        return []


def _previous_market_state(trade_date: str) -> dict[str, Any] | None:
    if not trade_date or not _table_exists("st_market_state_daily"):
        return None
    try:
        rows = _db_read(
            """
            SELECT trade_date, candidate_state, final_state, candidate_streak,
                   state_days, cooldown_remaining, source_status
            FROM st_market_state_daily
            WHERE config_version = :config_version
              AND trade_date < :trade_date
            ORDER BY trade_date DESC, id DESC
            LIMIT 1
            """,
            {
                "config_version": load_market_state_config()["config_version"],
                "trade_date": trade_date,
            },
        )
        return rows[0] if rows else None
    except Exception as exc:
        logger.debug("market state history fallback: %s", exc)
        return None


def _kline_market_features(trade_date: str) -> dict[str, Any]:
    """Build deterministic market-state inputs from the dedicated K-line DB."""
    if not trade_date or not _kline_table_exists("sm_stock_kline"):
        return {}
    config = load_market_state_config()
    fallback = config.get("feature_fallbacks") or {}
    try:
        rows = read_sql_rows(
            get_kline_engine(),
            """
            SELECT trade_date,
                   AVG(change_pct) AS market_change_pct,
                   SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END)
                     * 100.0 / NULLIF(COUNT(*), 0) AS breadth_pct,
                   COUNT(*) AS universe_count
            FROM sm_stock_kline
            WHERE trade_date <= :trade_date
              AND trade_date >= DATE_SUB(:trade_date, INTERVAL 45 DAY)
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^(0|3|6)'
              AND change_pct IS NOT NULL
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 20
            """,
            {"trade_date": trade_date},
            context="strategy_center_market_features",
        )
    except Exception as exc:
        logger.debug("strategy center K-line market feature fallback failed: %s", exc)
        return {}
    minimum_count = int(fallback.get("minimum_daily_universe_count") or 1000)
    valid = [
        row for row in rows
        if int(_num(row.get("universe_count"), 0) or 0) >= minimum_count
    ]
    if not valid:
        return {}
    changes = [float(_num(row.get("market_change_pct"), 0.0) or 0.0) for row in valid]
    breadths = [float(_num(row.get("breadth_pct"), 0.0) or 0.0) for row in valid]
    ret_5d = sum(changes[:5])
    ret_20d = sum(changes[:20])
    breadth = breadths[0]
    breadth_5d = sum(breadths[:5]) / max(1, min(5, len(breadths)))
    market_change = changes[0]
    trend = _clamp(50 + ret_5d * 3 + ret_20d + (breadth - 50) * 0.4, default=50.0)
    switch = _clamp(abs(breadth - breadth_5d) * 2 + abs(ret_5d) * 3, default=0.0)
    risk = _clamp(
        20
        + max(0.0, -market_change) * 10
        + max(0.0, 50 - breadth) * 1.2
        + max(0.0, -ret_5d) * 4,
        default=20.0,
    )
    latest_date = normalize_trade_date(str(valid[0].get("trade_date") or "")[:10])
    return {
        "data_date": latest_date,
        "market_change_pct": round(market_change, 4),
        "breadth_pct": round(breadth, 2),
        "trend_score": trend,
        "switch_score": switch,
        "risk_score": risk,
        "ret_5d_pct": round(ret_5d, 4),
        "ret_20d_pct": round(ret_20d, 4),
        "universe_count": int(_num(valid[0].get("universe_count"), 0) or 0),
        "source": str(fallback.get("source") or "sm_stock_kline_equal_weight_a_share"),
        "is_current": latest_date == trade_date,
    }


def _fuse_event_and_tape_risk(
    snapshot: dict[str, Any],
    kline: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Require QMT tape confirmation before a news alert blocks the market."""
    config = config or load_market_state_config()
    policy = config.get("event_fusion") or {}
    kline_current = bool(kline.get("is_current"))
    if kline_current:
        for key in (
            "market_change_pct",
            "breadth_pct",
            "trend_score",
            "switch_score",
        ):
            if kline.get(key) is not None:
                snapshot[key] = kline[key]
        snapshot["market_input_source"] = (
            "qmt_attested_daily_equal_weight"
        )

    kline_risk = _num(kline.get("risk_score"), None)
    event_scores = [
        _num(snapshot.get("risk_off_score"), None),
        _num(snapshot.get("tech_risk_score"), None),
    ]
    event_scores = [value for value in event_scores if value is not None]
    event_risk = max(event_scores) if event_scores else None
    event_alert = bool(snapshot.get("tech_triggered")) or (
        event_risk is not None
        and event_risk
        >= float(policy.get("event_alert_score_gte", 70))
    )
    change = _num(snapshot.get("market_change_pct"), None)
    breadth = _num(snapshot.get("breadth_pct"), None)
    price_stress = (
        (
            change is not None
            and change
            <= float(policy.get("price_stress_market_change_lte", -1.5))
        )
        or (
            breadth is not None
            and breadth
            <= float(policy.get("price_stress_breadth_lte", 35))
        )
        or (
            kline_risk is not None
            and kline_risk
            >= float(policy.get("price_stress_kline_risk_gte", 70))
        )
    )
    systemic_event = (
        event_alert
        and event_risk is not None
        and event_risk
        >= float(policy.get("systemic_event_score_gte", 82))
        and (price_stress or not kline_current)
    )
    hard_event = bool(snapshot.get("hard_event"))
    risk_candidates = [
        value for value in (kline_risk, event_risk) if value is not None
    ]
    if (
        kline_current
        and event_alert
        and not systemic_event
        and not hard_event
    ):
        event_cap = float(
            policy.get("unconfirmed_event_risk_score_cap", 54)
        )
        risk_candidates = [
            value
            for value in (
                kline_risk,
                min(event_risk, event_cap)
                if event_risk is not None
                else None,
            )
            if value is not None
        ]
        trend_cap = float(
            policy.get("unconfirmed_event_trend_score_cap", 67)
        )
        if snapshot.get("trend_score") is not None:
            snapshot["trend_score"] = min(
                float(snapshot["trend_score"]),
                trend_cap,
            )
        snapshot["event_risk_status"] = (
            "SECTOR_CAUTION_TAPE_NOT_CONFIRMED"
        )
    elif systemic_event or hard_event:
        snapshot["event_risk_status"] = "SYSTEMIC_CONFIRMED"
    else:
        snapshot["event_risk_status"] = "NO_SYSTEMIC_EVENT"
    snapshot["event_risk_score"] = event_risk
    snapshot["price_stress_confirmed"] = bool(price_stress)
    snapshot["tech_triggered_raw"] = bool(
        snapshot.get("tech_triggered")
    )
    snapshot["tech_triggered"] = bool(systemic_event or hard_event)
    snapshot["risk_score"] = (
        max(risk_candidates) if risk_candidates else None
    )
    snapshot["extreme_event"] = bool(systemic_event or hard_event)
    return snapshot


def load_market_snapshot(trade_date: str) -> dict[str, Any]:
    """Combine current adapters with frozen, reproducible K-line fallbacks."""
    cache_ttl = 120 if trade_date == date.today().isoformat() else 600
    now_monotonic = time.monotonic()
    with _MARKET_SNAPSHOT_CACHE_LOCK:
        cached = _MARKET_SNAPSHOT_CACHE.get(trade_date)
        if cached and now_monotonic - cached[0] < cache_ttl:
            _MARKET_SNAPSHOT_CACHE.move_to_end(trade_date)
            result = copy.deepcopy(cached[1])
            result["cache_status"] = "hit"
            return result
        if cached:
            _MARKET_SNAPSHOT_CACHE.pop(trade_date, None)
    snapshot: dict[str, Any] = {
        "trade_date": trade_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_status": "degraded",
        "evidence": [],
    }
    adapter_errors: list[str] = []
    try:
        from server.api.routers.hot_data import (
            market_sentiment,
            style_switch_signal,
            tech_risk_signal,
        )

        sentiment = market_sentiment(days=20, date=trade_date, top=8, include_signal=True)
        style = (
            sentiment.get("style_switch_signal") or {}
            if isinstance(sentiment, dict)
            else {}
        )
        if not style:
            style = style_switch_signal(date=trade_date, days=20)
        tech = style.get("tech_risk_signal") if isinstance(style, dict) else {}
        if not isinstance(tech, dict) or not tech:
            tech = tech_risk_signal(date=trade_date, days=2)
        snapshot.update({
            "sentiment": sentiment if isinstance(sentiment, dict) else {},
            "style": style if isinstance(style, dict) else {},
            "tech": tech if isinstance(tech, dict) else {},
        })
        style = snapshot["style"]
        tech = snapshot["tech"]
        theme = (snapshot["sentiment"].get("theme_analysis") or {}) if isinstance(snapshot["sentiment"], dict) else {}
        breadth = (snapshot["sentiment"].get("breadth") or {}) if isinstance(snapshot["sentiment"], dict) else {}
        snapshot.update({
            "risk_off_score": _num(style.get("risk_off_score"), None),
            "switch_score": _num(style.get("switch_score"), None),
            "tech_risk_score": _num(tech.get("score"), None),
            "tech_triggered": bool(tech.get("triggered")),
            "breadth_pct": _num(breadth.get("up_ratio"), _num(breadth.get("up_pct"), None)),
            "trend_score": _num(theme.get("trend_score"), _num(theme.get("score"), None)),
        })
        if (
            snapshot.get("breadth_pct") is not None
            and 0 <= float(snapshot["breadth_pct"]) <= 1
        ):
            snapshot["breadth_pct"] = round(float(snapshot["breadth_pct"]) * 100, 2)
        adapter_errors = [
            str(item.get("error"))
            for item in snapshot.values()
            if isinstance(item, dict) and item.get("error")
        ]
        snapshot["evidence"] = (style.get("evidence") or [])[:8] + (tech.get("reasons") or tech.get("evidence") or [])[:8]
    except Exception as exc:
        adapter_errors = [str(exc)[:300]]
        snapshot["adapter_error"] = adapter_errors[0]
        snapshot["evidence"] = ["现有市场状态接口暂不可用，转用冻结K线公式"]

    state_config = load_market_state_config()
    kline = _kline_market_features(trade_date)
    snapshot["kline_fallback"] = kline
    for key in ("market_change_pct", "breadth_pct", "trend_score", "switch_score"):
        if snapshot.get(key) is None and kline.get(key) is not None:
            snapshot[key] = kline[key]
    snapshot = _fuse_event_and_tape_risk(
        snapshot,
        kline,
        config=state_config,
    )
    required = state_config["required_inputs"]
    missing = [key for key in required if _num(snapshot.get(key), None) is None]
    if missing:
        snapshot["source_status"] = "missing"
        snapshot["missing_inputs"] = missing
    elif kline and not bool(kline.get("is_current")):
        snapshot["source_status"] = "degraded"
        snapshot["evidence"].append(
            f"K线市场输入最新日期为{kline.get('data_date')}，晚于该日的数据尚未到库"
        )
    else:
        snapshot["source_status"] = "fresh"
    if kline:
        snapshot["evidence"].append(
            f"K线等权市场输入：{kline.get('data_date')}，有效股票{kline.get('universe_count')}只"
        )
    if adapter_errors:
        snapshot["adapter_errors"] = adapter_errors[:5]
    previous = _previous_market_state(trade_date)
    state = infer_market_state(snapshot, previous=previous)
    state["previous_state"] = previous
    state["input"] = {
        key: snapshot.get(key)
        for key in state_config["required_inputs"]
    }
    snapshot["market_state"] = state["key"]
    snapshot["state"] = state
    snapshot["cache_status"] = "fresh_compute"
    with _MARKET_SNAPSHOT_CACHE_LOCK:
        _MARKET_SNAPSHOT_CACHE[trade_date] = (time.monotonic(), copy.deepcopy(snapshot))
        _MARKET_SNAPSHOT_CACHE.move_to_end(trade_date)
        while len(_MARKET_SNAPSHOT_CACHE) > 32:
            _MARKET_SNAPSHOT_CACHE.popitem(last=False)
    return snapshot


def build_strategy_cards(market: dict[str, Any], candidates: list[dict[str, Any]], configs: dict[str, dict[str, Any]], metrics: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    counts = defaultdict(int)
    for candidate in candidates:
        for key in candidate.get("strategies") or []:
            counts[key] += 1
    cards = []
    state = market.get("market_state") or "risk_declining"
    for item in STRATEGY_CATALOG:
        key = item["key"]
        manifest_item = next(
            (
                value
                for value in load_stock_manifest()["strategies"]
                if value["key"] == key
            ),
            {},
        )
        metric = metrics.get(key) or {}
        weight = effective_weight(key, state, configs.get(key), metric, 1.0 if market.get("source_status") == "fresh" else 0.75)
        cards.append({
            **item,
            "enabled": bool((configs.get(key) or {}).get("enabled", True)),
            "version": (configs.get(key) or {}).get("version", 1),
            **weight,
            "today_signal_count": counts.get(key, 0),
            "sample_count": int(_num(metric.get("sample_count"), 0) or 0),
            "return_pct": metric.get("return_pct"),
            "max_drawdown_pct": metric.get("max_drawdown_pct"),
            "win_rate_pct": metric.get("win_rate_pct"),
            "profit_factor": metric.get("profit_factor"),
            "metric_source": metric.get("source") or "暂无复盘样本",
            "metric_as_of_date": str(metric.get("as_of_date") or "")[:10],
            "metric_note": metric.get("metric_note") or "",
            "model_status": metric.get("model_status") or "frozen_manifest_adapter",
            "model_version": metric.get("model_version") or load_stock_manifest()["manifest_version"],
            "manifest_hash": stock_manifest_hash(),
            "formula": manifest_item.get("score_formula"),
            "hold_formula": manifest_item.get("hold_formula"),
            "parameters": manifest_item.get("parameters"),
            "entry_rules": manifest_item.get("entry_rules") or [],
            "exit_rules": manifest_item.get("exit_rules") or {},
        })
    return cards


def versioned_strategy_configuration() -> dict[str, Any]:
    stock = load_stock_manifest()
    market = load_market_state_config()
    return {
        "status": "ok",
        "stock": {
            "manifest_version": stock["manifest_version"],
            "config_hash": stock_manifest_hash(),
            "schema_version": stock["schema_version"],
            "status": stock["status"],
            "frozen_at": stock["frozen_at"],
            "strategies": stock["strategies"],
            "legacy_merge_map": stock.get("legacy_merge_map") or {},
            "disabled_labels": stock.get("disabled_labels") or {},
        },
        "market_state": {
            "config_version": market["config_version"],
            "config_hash": market_state_config_hash(),
            "schema_version": market["schema_version"],
            "status": market["status"],
            "frozen_at": market["frozen_at"],
            "required_inputs": market["required_inputs"],
            "feature_fallbacks": market.get("feature_fallbacks") or {},
            "thresholds": market["thresholds"],
            "transition": market["transition"],
            "strategy_multipliers": market["strategy_multipliers"],
            "calibration": market.get("calibration") or {},
        },
        "automatic_order_submission": False,
    }


def load_etf_forward_ledger(limit: int = 100) -> dict[str, Any]:
    """Expose the append-only QMT ETF forward ledger from the K-line DB."""
    limit = max(1, min(500, int(limit)))
    engine = get_kline_engine()
    if not _kline_table_exists("st_etf_forward_strategy"):
        return {
            "status": "not_registered",
            "message": "ETF前向策略尚未在QMT本地库注册",
            "strategies": [],
            "observations": [],
            "observation_count": 0,
            "automatic_order_submission": False,
        }
    strategies = read_sql_rows(
        engine,
        """
        SELECT strategy_version, config_hash, frozen_at, forward_start_date,
               mode, status, registered_at
        FROM st_etf_forward_strategy
        ORDER BY registered_at DESC
        """,
        context="strategy_center_etf_registry",
        stringify_datetime=True,
    )
    observations: list[dict[str, Any]] = []
    if _kline_table_exists("st_etf_forward_observation"):
        observations = read_sql_rows(
            engine,
            f"""
            SELECT strategy_version, config_hash, data_date, observed_at,
                   data_source, input_hash, signal_type, execution_date,
                   target_json, context_json, created_at
            FROM st_etf_forward_observation
            ORDER BY data_date DESC, id DESC
            LIMIT {limit}
            """,
            context="strategy_center_etf_observations",
            stringify_datetime=True,
        )
        for row in observations:
            row["target"] = _json_value(row.pop("target_json", None), {})
            row["context"] = _json_value(row.pop("context_json", None), {})
    latest_etf_date = ""
    try:
        rows = read_sql_rows(
            engine,
            """
            SELECT MAX(trade_date) AS data_date
            FROM sm_etf_kline
            WHERE k_type = 1 AND adjust_type = 1
              AND validation_status = 'passed'
              AND quality_status = 'validated'
            """,
            context="strategy_center_etf_latest",
            stringify_datetime=True,
        )
        latest_etf_date = str((rows[0] if rows else {}).get("data_date") or "")[:10]
    except Exception as exc:
        logger.debug("ETF forward latest date unavailable: %s", exc)
    if observations:
        status = "collecting"
        message = "已产生真实前向观察记录"
    else:
        starts = [
            normalize_trade_date(str(item.get("forward_start_date") or "")[:10])
            for item in strategies
        ]
        valid_starts = [item for item in starts if item]
        earliest = min(valid_starts) if valid_starts else ""
        status = "waiting_forward_start" if earliest and date.today().isoformat() < earliest else "waiting_validated_close"
        message = (
            f"冻结完成，等待{earliest}及之后的真实收盘数据自然产生"
            if earliest
            else "冻结完成，等待真实收盘数据自然产生"
        )
    return {
        "status": status,
        "message": message,
        "strategies": strategies,
        "observations": observations,
        "observation_count": len(observations),
        "latest_validated_etf_date": latest_etf_date,
        "backfill": "prohibited",
        "automatic_order_submission": False,
    }


def load_membership_snapshot_history(
    *,
    snapshot_date: str = "",
    member_type: str = "concept",
    group_code: str = "",
    stock_code: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    """Read immutable BigQMT concept/industry membership history."""
    member_type = str(member_type or "concept").strip().lower()
    if member_type not in {"concept", "industry"}:
        raise ValueError("member_type must be concept or industry")
    limit = max(1, min(1000, int(limit)))
    if not _kline_table_exists("qmt_membership_snapshot_run"):
        return {
            "status": "not_initialized",
            "snapshot_date": "",
            "member_type": member_type,
            "runs": [],
            "data": [],
        }
    engine = get_kline_engine()
    runs = read_sql_rows(
        engine,
        """
        SELECT snapshot_date, source, quality_status, capture_mode,
               concept_count, concept_relation_count, industry_count,
               industry_relation_count, concept_hash, industry_hash, captured_at
        FROM qmt_membership_snapshot_run
        ORDER BY snapshot_date DESC, id DESC
        LIMIT 60
        """,
        context="strategy_center_membership_runs",
        stringify_datetime=True,
    )
    requested = normalize_trade_date(snapshot_date)
    available_dates = [
        normalize_trade_date(str(item.get("snapshot_date") or "")[:10])
        for item in runs
    ]
    available_dates = [item for item in available_dates if item]
    target = (
        max((item for item in available_dates if not requested or item <= requested), default="")
        if available_dates
        else ""
    )
    if not target:
        return {
            "status": "empty",
            "snapshot_date": "",
            "member_type": member_type,
            "runs": runs,
            "data": [],
        }
    if member_type == "concept":
        table = "qmt_concept_member_snapshot"
        group_column = "concept_code"
        select_columns = "concept_code AS group_code, concept_name AS group_name"
    else:
        table = "qmt_industry_member_snapshot"
        group_column = "industry_code"
        select_columns = (
            "industry_code AS group_code, industry_name AS group_name, industry_type"
        )
    conditions = ["snapshot_date = :snapshot_date"]
    params: dict[str, Any] = {"snapshot_date": target}
    if group_code:
        conditions.append(f"`{group_column}` = :group_code")
        params["group_code"] = str(group_code).strip()
    if stock_code:
        conditions.append("stock_code = :stock_code")
        params["stock_code"] = str(stock_code).strip().split(".", 1)[0].zfill(6)
    rows = read_sql_rows(
        engine,
        f"""
        SELECT snapshot_date, source, {select_columns},
               stock_code, short_name, quality_status, captured_at
        FROM `{table}`
        WHERE {' AND '.join(conditions)}
        ORDER BY `{group_column}`, stock_code
        LIMIT {limit}
        """,
        params,
        context="strategy_center_membership_history",
        stringify_datetime=True,
    )
    return {
        "status": "ok",
        "snapshot_date": target,
        "requested_date": requested,
        "member_type": member_type,
        "runs": runs,
        "total_returned": len(rows),
        "data": rows,
    }


def load_qmt_kline_attestation_status(limit: int = 30) -> dict[str, Any]:
    """Expose row-level BigQMT attestation runs from the K-line database."""
    limit = max(1, min(200, int(limit)))
    if not _kline_table_exists("qmt_kline_attestation_run"):
        return {
            "status": "not_initialized",
            "message": "旧日K逐行QMT补证尚未在Windows本地数据边界运行",
            "runs": [],
            "mismatches": [],
        }
    engine = get_kline_engine()
    runs = read_sql_rows(
        engine,
        f"""
        SELECT run_id, provider, start_date, end_date, status,
               target_rows, qmt_rows, matched_rows, missing_qmt_rows,
               mismatched_rows, already_attested_rows, updated_rows,
               tolerance_json, started_at, finished_at, error_message
        FROM qmt_kline_attestation_run
        ORDER BY started_at DESC
        LIMIT {limit}
        """,
        context="strategy_center_qmt_attestation_runs",
        stringify_datetime=True,
    )
    for row in runs:
        row["tolerances"] = _json_value(row.pop("tolerance_json", None), {})
        target_rows = int(_num(row.get("target_rows"), 0) or 0)
        matched_rows = int(_num(row.get("matched_rows"), 0) or 0)
        row["coverage_pct"] = round(matched_rows * 100.0 / target_rows, 4) if target_rows else 0.0
    mismatches: list[dict[str, Any]] = []
    if runs and _kline_table_exists("qmt_kline_attestation_mismatch"):
        mismatches = read_sql_rows(
            engine,
            """
            SELECT run_id, trade_date, stock_code, reason,
                   target_close, qmt_close, target_volume, qmt_volume,
                   target_amount, qmt_amount, created_at
            FROM qmt_kline_attestation_mismatch
            WHERE run_id = :run_id
            ORDER BY trade_date, stock_code
            LIMIT 200
            """,
            {"run_id": runs[0]["run_id"]},
            context="strategy_center_qmt_attestation_mismatch",
            stringify_datetime=True,
        )
    latest = runs[0] if runs else None
    status = (
        "complete"
        if latest and latest.get("status") == "COMPLETED"
        else "partial"
        if latest
        else "empty"
    )
    return {
        "status": status,
        "provider_required": "gj_big_qmt_inner",
        "quality_status_on_match": "QMT_ATTESTED",
        "unmatched_rows_are_modified": False,
        "runs": runs,
        "latest_mismatches": mismatches,
    }


def load_persisted_strategy_center_compact(
    trade_date: str = "",
    limit: int = 200,
) -> dict[str, Any] | None:
    """Rebuild the small candidate view from the latest completed persisted run."""
    limit = max(1, min(500, int(limit)))
    requested = normalize_trade_date(trade_date)
    run_where = "AND trade_date = :trade_date" if requested else ""
    params = {"trade_date": requested} if requested else {}
    runs = _db_read(
        f"""
        SELECT run_uid, trade_date, market_state, state_confidence,
               source_status, candidate_count, conflict_count, finished_at
        FROM st_strategy_center_run
        WHERE status = 'done' {run_where}
        ORDER BY trade_date DESC, finished_at DESC, created_at DESC
        LIMIT 1
        """,
        params,
    )
    if not runs:
        return None
    run = runs[0]
    signals = _db_read(
        """
        SELECT stock_code, stock_name, strategy_key, market_state,
               signal_direction, signal_status, effective_score,
               model_confidence, risk_level, gate_status, gate_reason,
               entry_low, entry_high, stop_loss, today_signal,
               data_snapshot_json
        FROM st_strategy_center_signal
        WHERE run_uid = :run_uid
        ORDER BY stock_code, strategy_key
        """,
        {"run_uid": run["run_uid"]},
    )
    if not signals:
        return None
    state_key = str(run.get("market_state") or "unknown")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        code = str(signal.get("stock_code") or "").zfill(6)
        if not code:
            continue
        data_snapshot = _json_value(signal.pop("data_snapshot_json", None), {})
        signal["data_date"] = normalize_trade_date(data_snapshot.get("data_date"))
        grouped[code].append(signal)

    candidates: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    risk_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    for code, stock_signals in grouped.items():
        decision = resolve_conflict(stock_signals, state_key)
        best = max(
            stock_signals,
            key=lambda item: (
                _num(item.get("effective_score"), 0.0) or 0.0,
                _num(item.get("model_confidence"), 0.0) or 0.0,
            ),
        )
        candidate = {
            "priority": (
                "A"
                if decision["final_status"] in {"READY", "WATCH"}
                and decision["final_direction"] == "BUY"
                else "B"
            ),
            "stock_code": code,
            "stock_name": best.get("stock_name") or code,
            "final_direction": decision["final_direction"],
            "final_status": decision["final_status"],
            "model_confidence": max(
                (
                    _num(item.get("model_confidence"), 0.0) or 0.0
                    for item in stock_signals
                ),
                default=0.0,
            )
            or None,
            "today_signal": best.get("today_signal") or decision.get("conflict_summary"),
            "entry_low": best.get("entry_low"),
            "entry_high": best.get("entry_high"),
            "stop_loss": best.get("stop_loss"),
            "risk_level": max(
                (str(item.get("risk_level") or "LOW") for item in stock_signals),
                key=lambda value: risk_order.get(value, 0),
            ),
            "dominant_strategy": decision.get("dominant_strategy")
            or best.get("strategy_key"),
            "strategies": sorted(
                {
                    item.get("strategy_key")
                    for item in stock_signals
                    if item.get("strategy_key")
                }
            ),
            "conflict_summary": decision["conflict_summary"],
            "blocking_reasons": decision["blocking_reasons"],
            "data_date": best.get("data_date")
            or normalize_trade_date(run.get("trade_date")),
        }
        candidates.append(candidate)
        if decision["conflict"] or decision["final_status"] in {"BLOCKED", "CONFLICT"}:
            conflicts.append(
                {
                    "stock_code": code,
                    "stock_name": candidate["stock_name"],
                    "conflict_summary": decision["conflict_summary"],
                    "strategies": candidate["strategies"],
                }
            )
    candidates.sort(
        key=lambda item: (
            {
                "READY": 0,
                "WATCH": 1,
                "CONFLICT": 2,
                "SELL_ALERT": 3,
                "BLOCKED": 4,
                "INSUFFICIENT_DATA": 5,
            }.get(item.get("final_status"), 9),
            -(item.get("model_confidence") or 0),
            item.get("stock_code", ""),
        )
    )
    candidates = candidates[:limit]
    selected_codes = {item["stock_code"] for item in candidates}
    conflicts = [
        item for item in conflicts if item.get("stock_code") in selected_codes
    ]
    market_state = market_state_info(state_key)
    market_state["confidence"] = _num(run.get("state_confidence"))
    if state_key == "extreme_event":
        gate_status = "BLOCK_NEW_BUY"
        gate_reason = "极端事件模式自动停止新增买入信号"
    elif state_key in {"high_range", "risk_declining"}:
        gate_status = "REDUCE_NEW_BUY"
        gate_reason = f"{market_state.get('name')}模式自动降权新增买入"
    else:
        gate_status = "ALLOW_NEW_BUY"
        gate_reason = "市场状态允许生成研究候选，仍需价格和板块条件确认"
    return {
        "status": "ok",
        "trade_date": normalize_trade_date(run.get("trade_date")),
        "data_date": max(
            (item.get("data_date") or "" for item in candidates),
            default=normalize_trade_date(run.get("trade_date")),
        ),
        "source_status": run.get("source_status") or "degraded",
        "is_stale": False,
        "market_state": market_state,
        "global_gate": {"status": gate_status, "reason": gate_reason},
        "candidates": candidates,
        "conflicts": conflicts,
        "summary": {
            "candidate_count": len(candidates),
            "conflict_count": len(conflicts),
        },
        "disclaimer": "仅用于研究候选和风险提示；未经明确确认不会执行任何交易。",
        "persisted_run_uid": run["run_uid"],
    }


def build_strategy_center_snapshot(trade_date: str = "", limit: int = 200) -> dict[str, Any]:
    target = latest_recommendation_date(trade_date)
    if not target:
        target = normalize_trade_date(trade_date) or date.today().isoformat()
    reference_pool = load_reference_candidate_pool(target)
    market = load_market_snapshot(target)
    configs = load_strategy_configs()
    metrics = load_strategy_metrics(target)
    rows = load_recommendation_rows(target, limit)
    candidates, conflicts = aggregate_candidates(rows, {**market, "market_state": (market.get("state") or {}).get("key", market.get("market_state"))}, configs, metrics)
    state = market.get("state") or infer_market_state(market)
    gate_status = "ALLOW_NEW_BUY"
    gate_reason = "市场状态允许生成研究候选，仍需价格和板块条件确认"
    if state.get("key") == "extreme_event":
        gate_status, gate_reason = "BLOCK_NEW_BUY", "极端事件模式自动停止新增买入信号"
    elif state.get("key") in {"high_range", "risk_declining"}:
        gate_status, gate_reason = "REDUCE_NEW_BUY", f"{state.get('name')}模式自动降权新增买入"
    if market.get("source_status") == "missing" or state.get("key") == "unknown":
        gate_status, gate_reason = "DATA_NOT_READY", "市场状态数据缺失，不生成确定性动作"
    if reference_pool:
        reference_gate = reference_pool.get("global_gate") if isinstance(reference_pool.get("global_gate"), dict) else {}
        gate_status = "REVIEW_REQUIRED"
        gate_reason = str(reference_gate.get("reason") or "参考池仅用于研究，需盘前/盘中复核后才可更新信号")
        source_status = "reference_verified" if all(bool(row.get("db_verified")) for row in rows) else "reference_unverified"
        data_date = normalize_trade_date(reference_pool.get("reference_as_of_date")) or target
    else:
        source_status = market.get("source_status", "degraded")
        data_date = target
    reference_meta = {
        "enabled": bool(reference_pool),
        "source": reference_pool.get("source") if reference_pool else None,
        "path": reference_pool.get("_path") if reference_pool else None,
        "reference_as_of_date": normalize_trade_date(reference_pool.get("reference_as_of_date")) if reference_pool else None,
        "recheck_after": reference_pool.get("recheck_after") if reference_pool else None,
        "position_limits": reference_pool.get("position_limits") if reference_pool else None,
        "global_gate": reference_pool.get("global_gate") if reference_pool else None,
    }
    return {
        "status": "ok" if rows or market.get("source_status") != "missing" else "degraded",
        "trade_date": target,
        "data_date": data_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_status": source_status,
        "is_stale": bool(reference_pool) or market.get("source_status") != "fresh",
        "data_sources": [
            "st_recommended_stocks" if not reference_pool else "dated_reference_pool",
            "sm_stock_snapshot/sm_stock_kline" if reference_pool else "existing_market_adapters+sm_stock_kline",
        ],
        "configuration": {
            "stock_manifest_version": load_stock_manifest()["manifest_version"],
            "stock_manifest_hash": stock_manifest_hash(),
            "market_state_config_version": load_market_state_config()["config_version"],
            "market_state_config_hash": market_state_config_hash(),
            "automatic_order_submission": False,
        },
        "reference_pool": reference_meta,
        "market_state": state,
        "global_gate": {
            "status": gate_status,
            "reason": gate_reason,
            "recheck_after": reference_meta.get("recheck_after"),
            "position_limits": reference_meta.get("position_limits"),
            "invalidation_condition": (reference_meta.get("global_gate") or {}).get("invalidation_condition") if reference_pool else None,
        },
        "strategies": build_strategy_cards(market, candidates, configs, metrics),
        "candidates": candidates,
        "conflicts": conflicts,
        "summary": {
            "strategy_count": len(STRATEGY_CATALOG),
            "enabled_count": sum(1 for value in configs.values() if value.get("enabled", True)),
            "candidate_count": len(candidates),
            "conflict_count": len(conflicts),
            "buy_count": sum(1 for item in candidates if item.get("final_direction") == "BUY"),
            "blocked_count": sum(1 for item in candidates if item.get("final_status") in {"BLOCKED", "SELL_ALERT"}),
        },
        "disclaimer": "仅用于研究候选和风险提示；未经明确确认不会执行任何交易。",
    }


def persist_strategy_center_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    ensure_strategy_center_tables()
    run_uid = uuid.uuid4().hex
    state = snapshot.get("market_state") or {}
    candidates = snapshot.get("candidates") or []
    conflicts = snapshot.get("conflicts") or []
    signal_count = sum(len(item.get("strategy_signals") or []) for item in candidates)
    _db_write("""
        INSERT INTO st_strategy_center_run
        (run_uid, trade_date, market_state, state_confidence, source_status, model_version, status, signal_count, candidate_count, conflict_count)
        VALUES (:run_uid, :trade_date, :market_state, :state_confidence, :source_status, :model_version, 'running', :signal_count, :candidate_count, :conflict_count)
    """, {
        "run_uid": run_uid,
        "trade_date": snapshot.get("trade_date"),
        "market_state": state.get("key") or "unknown",
        "state_confidence": state.get("confidence"),
        "source_status": snapshot.get("source_status") or "degraded",
        "model_version": str(load_stock_manifest()["manifest_version"])[:40],
        "signal_count": signal_count,
        "candidate_count": len(candidates),
        "conflict_count": len(conflicts),
    })
    for candidate in candidates:
        for signal in candidate.get("strategy_signals") or []:
            _db_write("""
                INSERT INTO st_strategy_center_signal
                (run_uid, trade_date, stock_code, stock_name, strategy_key, market_state, signal_direction, signal_status,
                 raw_score, effective_score, model_confidence, effective_weight, risk_level, gate_status, gate_reason,
                 entry_low, entry_high, stop_loss, take_profit_1, take_profit_2, no_chase_price, risk_reward_ratio,
                 today_signal, trigger_conditions_json, evidence_chain_json, data_snapshot_json, model_version)
                VALUES (:run_uid, :trade_date, :stock_code, :stock_name, :strategy_key, :market_state, :signal_direction, :signal_status,
                        :raw_score, :effective_score, :model_confidence, :effective_weight, :risk_level, :gate_status, :gate_reason,
                        :entry_low, :entry_high, :stop_loss, :take_profit_1, :take_profit_2, :no_chase_price, :risk_reward_ratio,
                        :today_signal, :trigger_conditions_json, :evidence_chain_json, :data_snapshot_json, :model_version)
            """, {
                "run_uid": run_uid, "trade_date": snapshot.get("trade_date"), "stock_code": signal.get("stock_code"), "stock_name": signal.get("stock_name"),
                "strategy_key": signal.get("strategy_key"), "market_state": signal.get("market_state"), "signal_direction": signal.get("signal_direction"), "signal_status": signal.get("signal_status"),
                "raw_score": signal.get("raw_score"), "effective_score": signal.get("effective_score"), "model_confidence": signal.get("model_confidence"), "effective_weight": signal.get("effective_weight"),
                "risk_level": signal.get("risk_level"), "gate_status": signal.get("gate_status"), "gate_reason": signal.get("gate_reason"), "entry_low": signal.get("entry_low"), "entry_high": signal.get("entry_high"),
                "stop_loss": signal.get("stop_loss"), "take_profit_1": signal.get("take_profit_1"), "take_profit_2": signal.get("take_profit_2"), "no_chase_price": signal.get("no_chase_price"),
                "risk_reward_ratio": signal.get("risk_reward_ratio"), "today_signal": signal.get("today_signal"), "trigger_conditions_json": _json_text(signal.get("trigger_conditions"), []),
                "evidence_chain_json": _json_text(signal.get("evidence_chain"), []), "data_snapshot_json": _json_text({"data_date": signal.get("data_date"), "adapter_mode": signal.get("adapter_mode")}, {}),
                "model_version": signal.get("model_version") or str(load_stock_manifest()["manifest_version"])[:40],
            })
    for conflict in conflicts:
        decision = conflict.get("decision") or {}
        _db_write("""
            INSERT INTO st_strategy_center_conflict
            (run_uid, trade_date, stock_code, stock_name, market_state, final_direction, final_status, buy_score, sell_score, hold_score, decision_json)
            VALUES (:run_uid, :trade_date, :stock_code, :stock_name, :market_state, :final_direction, :final_status, :buy_score, :sell_score, :hold_score, :decision_json)
        """, {
            "run_uid": run_uid, "trade_date": snapshot.get("trade_date"), "stock_code": conflict.get("stock_code"), "stock_name": conflict.get("stock_name"),
            "market_state": conflict.get("market_state"), "final_direction": decision.get("final_direction"), "final_status": decision.get("final_status"),
            "buy_score": decision.get("buy_score"), "sell_score": decision.get("sell_score"), "hold_score": decision.get("hold_score"), "decision_json": _json_text(conflict, {}),
        })
    market_input = {
        key: (snapshot.get("market_state") or {}).get("input", {}).get(key)
        for key in load_market_state_config()["required_inputs"]
    }
    if not any(value is not None for value in market_input.values()):
        # Older adapters put raw inputs directly in the market-state payload.
        market_input = {
            key: (snapshot.get("market_state") or {}).get(key)
            for key in load_market_state_config()["required_inputs"]
        }
    state_payload = {
        "market_input": market_input,
        "source_status": snapshot.get("source_status"),
        "trade_date": snapshot.get("trade_date"),
    }
    input_hash = hashlib.sha256(
        json.dumps(
            state_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    _db_write(
        """
        INSERT INTO st_market_state_daily
        (trade_date, run_uid, config_version, config_hash, input_hash,
         candidate_state, final_state, candidate_streak, state_days,
         cooldown_remaining, source_status, input_json, evidence_json)
        VALUES
        (:trade_date, :run_uid, :config_version, :config_hash, :input_hash,
         :candidate_state, :final_state, :candidate_streak, :state_days,
         :cooldown_remaining, :source_status, :input_json, :evidence_json)
        ON DUPLICATE KEY UPDATE id = id
        """,
        {
            "trade_date": snapshot.get("trade_date"),
            "run_uid": run_uid,
            "config_version": state.get("config_version")
            or load_market_state_config()["config_version"],
            "config_hash": state.get("config_hash") or market_state_config_hash(),
            "input_hash": input_hash,
            "candidate_state": state.get("candidate_state") or state.get("key") or "unknown",
            "final_state": state.get("final_state") or state.get("key") or "unknown",
            "candidate_streak": int(state.get("candidate_streak") or 1),
            "state_days": int(state.get("state_days") or 1),
            "cooldown_remaining": int(state.get("cooldown_remaining") or 0),
            "source_status": snapshot.get("source_status") or "degraded",
            "input_json": json.dumps(state_payload, ensure_ascii=False, default=str),
            "evidence_json": json.dumps(state.get("evidence") or [], ensure_ascii=False),
        },
    )
    _db_write("UPDATE st_strategy_center_run SET status = 'done', finished_at = NOW() WHERE run_uid = :run_uid", {"run_uid": run_uid})
    return {
        **snapshot,
        "run_uid": run_uid,
        "execution_status": "done",
    }


def set_strategy_enabled(strategy_key: str, enabled: bool, reason: str = "", operator: str = "api") -> dict[str, Any]:
    if strategy_key not in _STRATEGY_BY_KEY:
        raise ValueError(f"unknown strategy_key: {strategy_key}")
    ensure_strategy_center_tables()
    configs = load_strategy_configs()
    old = configs.get(strategy_key) or {}
    _db_write("""
        UPDATE st_strategy_center_config
        SET enabled = :enabled, version = version + 1, updated_by = :operator, updated_at = NOW()
        WHERE strategy_key = :strategy_key
    """, {"enabled": 1 if enabled else 0, "operator": operator[:80], "strategy_key": strategy_key})
    _db_write("""
        INSERT INTO st_strategy_center_audit (strategy_key, action, old_value, new_value, reason, operator)
        VALUES (:strategy_key, 'toggle', :old_value, :new_value, :reason, :operator)
    """, {
        "strategy_key": strategy_key, "old_value": json.dumps({"enabled": old.get("enabled", True)}, ensure_ascii=False),
        "new_value": json.dumps({"enabled": bool(enabled)}, ensure_ascii=False), "reason": str(reason or "")[:500], "operator": operator[:80],
    })
    return {"strategy_key": strategy_key, "enabled": bool(enabled), "reason": reason, "updated_by": operator}
