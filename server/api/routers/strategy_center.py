# -*- coding: utf-8 -*-
"""Research-only strategy center API."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, Path, Query
from pydantic import BaseModel, Field

from server.engine.strategy_center import (
    MARKET_STATES,
    STRATEGY_CATALOG,
    build_strategy_center_snapshot,
    ensure_strategy_center_tables,
    load_market_snapshot,
    load_etf_forward_ledger,
    load_membership_snapshot_history,
    load_persisted_strategy_center_compact,
    load_qmt_kline_attestation_status,
    load_strategy_configs,
    load_strategy_metrics,
    latest_recommendation_date,
    normalize_trade_date,
    persist_strategy_center_snapshot,
    set_strategy_enabled,
    versioned_strategy_configuration,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class StrategyToggleRequest(BaseModel):
    enabled: bool
    reason: str = Field(default="", max_length=500)
    operator: str = Field(default="api", max_length=80)


class StrategyRunRequest(BaseModel):
    trade_date: str = ""
    limit: int = Field(default=200, ge=1, le=500)


def _research_only_candidate(row: dict[str, Any]) -> dict[str, Any]:
    """Expose direction for research while never labelling it executable BUY."""

    projected = dict(row)
    direction = str(projected.get("final_direction") or "HOLD").upper()
    projected["decision_scope"] = "RESEARCH_ONLY"
    projected["new_buy_eligible"] = False
    projected["display_action"] = (
        direction if direction in {"SELL", "REDUCE", "EXIT"} else "WATCH"
    )
    return projected


def _research_only_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        **snapshot,
        "candidates": [
            _research_only_candidate(row)
            for row in snapshot.get("candidates", [])
        ],
    }


def _degraded(error: Exception | str, trade_date: str = "") -> dict[str, Any]:
    target = normalize_trade_date(trade_date) or latest_recommendation_date() or trade_date or ""
    return {
        "status": "degraded",
        "trade_date": target,
        "data_date": target,
        "generated_at": "",
        "source_status": "missing",
        "is_stale": True,
        "market_state": {
            "key": "unknown", "name": "数据不足", "confidence": 0,
            "evidence": ["策略中心数据暂不可用"], "source_status": "missing",
        },
        "global_gate": {"status": "DATA_NOT_READY", "reason": "数据不足，不生成确定性动作"},
        "strategies": [
            {**item, "enabled": True, "effective_weight": None, "today_signal_count": 0, "sample_count": 0,
             "return_pct": None, "max_drawdown_pct": None, "win_rate_pct": None, "profit_factor": None,
             "metric_source": "暂无数据"}
            for item in STRATEGY_CATALOG
        ],
        "candidates": [], "conflicts": [],
        "summary": {"strategy_count": len(STRATEGY_CATALOG), "enabled_count": len(STRATEGY_CATALOG), "candidate_count": 0, "conflict_count": 0, "buy_count": 0, "blocked_count": 0},
        "error": str(error)[:500],
        "disclaimer": "仅用于研究候选和风险提示；未经明确确认不会执行任何交易。",
    }


@router.get("/strategy-center/overview")
def strategy_center_overview(
    trade_date: str = Query(default=""),
    limit: int = Query(default=200, ge=1, le=500),
):
    try:
        return _research_only_snapshot(
            build_strategy_center_snapshot(trade_date, limit)
        )
    except Exception as exc:
        logger.warning("strategy center overview degraded: %s", exc)
        return _degraded(exc, trade_date)


@router.get("/strategy-center/market-state")
def strategy_center_market_state(trade_date: str = Query(default="")):
    try:
        target = latest_recommendation_date(trade_date) or normalize_trade_date(trade_date)
        snapshot = load_market_snapshot(target)
        return {
            "status": "ok",
            "trade_date": target,
            "data_date": target,
            "generated_at": snapshot.get("generated_at") or "",
            "source_status": snapshot.get("source_status", "degraded"),
            "is_stale": snapshot.get("source_status") != "fresh",
            "market_state": snapshot.get("state") or {"key": snapshot.get("market_state", "unknown")},
            "raw": {key: snapshot.get(key) for key in ("risk_score", "risk_off_score", "switch_score", "tech_risk_score", "market_change_pct", "breadth_pct", "trend_score", "evidence", "kline_fallback")},
        }
    except Exception as exc:
        return {"status": "degraded", "market_state": {"key": "unknown", "name": "数据不足", "confidence": 0}, "error": str(exc)[:500]}


@router.get("/strategy-center/configuration")
def strategy_center_configuration():
    try:
        return versioned_strategy_configuration()
    except Exception as exc:
        return {"status": "error", "error": str(exc)[:500]}
@router.get("/strategy-center/etf-forward")
def strategy_center_etf_forward(
    limit: int = Query(default=100, ge=1, le=500),
):
    try:
        return load_etf_forward_ledger(limit)
    except Exception as exc:
        return {
            "status": "degraded",
            "message": "ETF真实前向记录暂不可读取",
            "observations": [],
            "error": str(exc)[:500],
        }


@router.get("/strategy-center/membership-history")
def strategy_center_membership_history(
    snapshot_date: str = Query(default=""),
    member_type: str = Query(default="concept", pattern="^(concept|industry)$"),
    group_code: str = Query(default="", max_length=80),
    stock_code: str = Query(default="", max_length=10),
    limit: int = Query(default=200, ge=1, le=1000),
):
    try:
        return load_membership_snapshot_history(
            snapshot_date=snapshot_date,
            member_type=member_type,
            group_code=group_code,
            stock_code=stock_code,
            limit=limit,
        )
    except Exception as exc:
        return {
            "status": "degraded",
            "member_type": member_type,
            "data": [],
            "error": str(exc)[:500],
        }


@router.get("/strategy-center/qmt-kline-attestation")
def strategy_center_qmt_kline_attestation(
    limit: int = Query(default=30, ge=1, le=200),
):
    try:
        return load_qmt_kline_attestation_status(limit)
    except Exception as exc:
        return {
            "status": "degraded",
            "runs": [],
            "error": str(exc)[:500],
        }


@router.get("/strategy-center/strategies")
def strategy_center_strategies(trade_date: str = Query(default="")):
    try:
        snapshot = build_strategy_center_snapshot(trade_date, limit=500)
        return {"status": snapshot.get("status"), "trade_date": snapshot.get("trade_date"), "market_state": snapshot.get("market_state"), "strategies": snapshot.get("strategies", []), "disclaimer": snapshot.get("disclaimer")}
    except Exception as exc:
        return _degraded(exc, trade_date)


@router.post("/strategy-center/strategies/{strategy_key}/toggle")
def strategy_center_toggle(
    payload: StrategyToggleRequest,
    strategy_key: str = Path(..., min_length=1, max_length=40),
):
    try:
        return {"status": "ok", **set_strategy_enabled(strategy_key, payload.enabled, payload.reason, payload.operator)}
    except ValueError as exc:
        return {"status": "error", "error": "invalid_strategy", "message": str(exc)}
    except Exception as exc:
        return {"status": "error", "error": "strategy_config_unavailable", "message": str(exc)[:500]}


@router.get("/strategy-center/candidates")
def strategy_center_candidates(
    trade_date: str = Query(default=""),
    strategy: str = Query(default=""),
    category: str = Query(default=""),
    market_state: str = Query(default=""),
    signal_status: str = Query(default=""),
    signal_direction: str = Query(default=""),
    risk_level: str = Query(default=""),
    limit: int = Query(default=200, ge=1, le=500),
    compact: bool = Query(default=False),
):
    try:
        snapshot = (
            load_persisted_strategy_center_compact(trade_date, limit)
            if compact
            else None
        ) or build_strategy_center_snapshot(trade_date, limit)
        category_keys = {item["key"] for item in STRATEGY_CATALOG if not category or item["category"] == category}
        rows = []
        for row in snapshot.get("candidates", []):
            strategies = set(row.get("strategies") or [])
            if strategy and strategy not in strategies:
                continue
            if not strategies.intersection(category_keys):
                continue
            if market_state and snapshot.get("market_state", {}).get("key") != market_state:
                continue
            if signal_status and str(row.get("final_status") or "") != signal_status.upper():
                continue
            if signal_direction and str(row.get("final_direction") or "") != signal_direction.upper():
                continue
            if risk_level and str(row.get("risk_level") or "") != risk_level.upper():
                continue
            rows.append(_research_only_candidate(row))
        conflicts = [
            item
            for item in snapshot.get("conflicts", [])
            if any(row.get("stock_code") == item.get("stock_code") for row in rows)
        ]
        if compact:
            candidate_fields = (
                "priority", "stock_code", "stock_name", "final_direction",
                "final_status", "model_confidence", "today_signal", "entry_low",
                "entry_high", "stop_loss", "risk_level", "dominant_strategy",
                "blocking_reasons", "conflict_summary", "data_date",
                "decision_scope", "new_buy_eligible", "display_action",
            )
            conflict_fields = (
                "stock_code", "stock_name", "conflict_summary", "strategies",
            )
            rows = [
                {field: row.get(field) for field in candidate_fields}
                for row in rows
            ]
            conflicts = [
                {field: item.get(field) for field in conflict_fields}
                for item in conflicts
            ]
        return {"status": snapshot.get("status"), "trade_date": snapshot.get("trade_date"), "data_date": snapshot.get("data_date"), "source_status": snapshot.get("source_status"), "is_stale": snapshot.get("is_stale"), "market_state": snapshot.get("market_state"), "global_gate": snapshot.get("global_gate"), "total": len(rows), "data": rows, "conflicts": conflicts, "disclaimer": snapshot.get("disclaimer")}
    except Exception as exc:
        return {"status": "degraded", "trade_date": trade_date, "total": 0, "data": [], "error": str(exc)[:500]}


@router.get("/strategy-center/stock/{stock_code}")
def strategy_center_stock(
    stock_code: str = Path(..., min_length=6, max_length=10),
    trade_date: str = Query(default=""),
):
    try:
        snapshot = build_strategy_center_snapshot(trade_date, limit=500)
        code = str(stock_code).strip().zfill(6)
        rows = [
            _research_only_candidate(row)
            for row in snapshot.get("candidates", [])
            if row.get("stock_code") == code
        ]
        return {"status": snapshot.get("status"), "trade_date": snapshot.get("trade_date"), "market_state": snapshot.get("market_state"), "data": rows[0] if rows else None, "disclaimer": snapshot.get("disclaimer")}
    except Exception as exc:
        return {"status": "degraded", "stock_code": stock_code, "data": None, "error": str(exc)[:500]}


@router.get("/strategy-center/compare")
def strategy_center_compare(
    trade_date: str = Query(default=""),
    strategies: str = Query(default=""),
):
    try:
        snapshot = build_strategy_center_snapshot(trade_date, limit=500)
        selected = {item.strip() for item in str(strategies or "").split(",") if item.strip()}
        data = [item for item in snapshot.get("strategies", []) if not selected or item.get("key") in selected]
        return {"status": snapshot.get("status"), "trade_date": snapshot.get("trade_date"), "market_state": snapshot.get("market_state"), "data": data, "disclaimer": snapshot.get("disclaimer")}
    except Exception as exc:
        return {"status": "degraded", "data": [], "error": str(exc)[:500]}


@router.get("/strategy-center/conflicts")
def strategy_center_conflicts(trade_date: str = Query(default=""), limit: int = Query(default=100, ge=1, le=500)):
    try:
        snapshot = build_strategy_center_snapshot(trade_date, limit)
        return {"status": snapshot.get("status"), "trade_date": snapshot.get("trade_date"), "market_state": snapshot.get("market_state"), "total": len(snapshot.get("conflicts", [])), "data": snapshot.get("conflicts", []), "disclaimer": snapshot.get("disclaimer")}
    except Exception as exc:
        return {"status": "degraded", "total": 0, "data": [], "error": str(exc)[:500]}


@router.post("/strategy-center/run")
def strategy_center_run(payload: StrategyRunRequest | None = Body(default=None)):
    payload = payload or StrategyRunRequest()
    try:
        snapshot = build_strategy_center_snapshot(payload.trade_date, payload.limit)
        if snapshot.get("source_status") == "missing":
            return {"status": "blocked", "reason": "strategy center data is not ready", "snapshot": snapshot}
        return persist_strategy_center_snapshot(snapshot)
    except Exception as exc:
        logger.error("strategy center run failed: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc)[:500]}
