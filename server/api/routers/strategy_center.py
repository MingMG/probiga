# -*- coding: utf-8 -*-
"""Research-only strategy center API."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, Path, Query, Request
from fastapi.responses import JSONResponse
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
    versioned_strategy_configuration,
)
from server.engine.strategy_governance import (
    GovernanceEvidenceNotReady,
    governance_history,
    governance_snapshot,
    metric_evidence_detail,
    record_metric_input,
    review_metric_input,
    register_combination,
    register_strategy,
    transition_lifecycle,
    toggle_strategy_enabled,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class StrategyToggleRequest(BaseModel):
    enabled: bool
    reason: str = Field(default="", max_length=500)


class StrategyRunRequest(BaseModel):
    trade_date: str = ""
    limit: int = Field(default=200, ge=1, le=500)


class StrategyRegistrationRequest(BaseModel):
    strategy_key: str = Field(min_length=3, max_length=80)
    strategy_name: str = Field(min_length=1, max_length=120)
    version: str = Field(min_length=1, max_length=160)
    category: str = Field(default="未分类", max_length=80)
    family_key: str = Field(default="", max_length=80)
    description: str = Field(default="", max_length=1000)
    evaluator_type: str = Field(default="external_evidence", max_length=40)
    evaluator_config: dict[str, Any] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(default="注册策略版本", max_length=500)


class StrategyCombinationRequest(BaseModel):
    combination_key: str = Field(min_length=3, max_length=80)
    combination_name: str = Field(min_length=1, max_length=120)
    version: str = Field(min_length=1, max_length=160)
    description: str = Field(default="", max_length=1000)
    members: list[dict[str, Any]] = Field(min_length=2, max_length=50)
    constraints: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(default="注册组合版本", max_length=500)


class LifecycleTransitionRequest(BaseModel):
    next_status: str = Field(min_length=1, max_length=24)
    reason: str = Field(min_length=1, max_length=500)
    evidence: dict[str, Any] = Field(default_factory=dict)


class StrategyMetricEvidenceRequest(BaseModel):
    strategy_key: str = Field(min_length=3, max_length=80)
    entity_type: str = Field(
        default="STRATEGY", pattern="^(STRATEGY|COMBINATION)$"
    )
    bound_strategy_version: str = Field(min_length=1, max_length=160)
    as_of_date: str = Field(min_length=10, max_length=10)
    window_days: int = Field(default=60)
    metrics: dict[str, Any] = Field(default_factory=dict)
    source: str = Field(default="manual_evidence", max_length=80)
    evidence_protocol: str = Field(min_length=1, max_length=80)
    artifact_hash: str = Field(
        min_length=64, max_length=64, pattern="^[0-9a-f]{64}$"
    )
    artifact_manifest: dict[str, Any]
    evidence_revision_at: str = Field(min_length=10, max_length=40)
    reason: str = Field(default="新增验证证据", max_length=500)


class StrategyMetricReviewRequest(BaseModel):
    decision: str = Field(pattern="^(CONFIRM|REJECT)$")
    reason: str = Field(min_length=1, max_length=500)


def _request_actor(request: Request) -> str:
    user = getattr(request.state, "auth_user", None)
    user_id = getattr(user, "id", None)
    if isinstance(user_id, int) and user_id > 0:
        return f"user-id:{user_id}"[:80]
    username = str(getattr(user, "username", "") or "").strip()
    if username:
        return f"user:{username}"[:80]
    auth_kind = str(getattr(request.state, "auth_kind", "") or "").strip()
    if auth_kind:
        return f"auth:{auth_kind}"[:80]
    return "api"


def _request_role_actor(
    request: Request,
    *,
    allowed_roles: frozenset[str],
    action_label: str,
) -> str:
    """Require one active named account with an explicit governance role."""

    user = getattr(request.state, "auth_user", None)
    user_id = getattr(user, "id", None)
    auth_kind = str(getattr(request.state, "auth_kind", "") or "").strip()
    role = str(getattr(user, "role", "") or "").strip().upper()
    if (
        auth_kind == "account_session"
        and isinstance(user_id, int)
        and user_id > 0
        and getattr(user, "is_active", False) is True
        and role in allowed_roles
    ):
        return f"user-id:{user_id}"[:80]
    roles = "、".join(sorted(allowed_roles))
    raise PermissionError(
        f"{action_label}仅允许实名账户角色 {roles}；旧管理令牌或其他角色无此权限"
    )


def _request_admin_actor(request: Request, action_label: str) -> str:
    return _request_role_actor(
        request,
        allowed_roles=frozenset({"ADMIN"}),
        action_label=action_label,
    )


def _request_reviewer_actor(request: Request) -> str:
    return _request_role_actor(
        request,
        allowed_roles=frozenset({"EVIDENCE_REVIEWER"}),
        action_label="指标证据独立复核",
    )


def _governance_api_error(
    status_code: int, error: str, message: str,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error": error,
            "message": str(message)[:500],
            "automatic_real_order_submission": False,
        },
    )


_INTERNAL_GOVERNANCE_ERROR = "服务器内部错误；详细原因已写入服务日志"


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


@router.get("/strategy-center/governance")
def strategy_center_governance(trade_date: str = Query(default="")):
    """Return the dynamic registry, two arenas, layered pools and allocations."""

    try:
        return governance_snapshot(trade_date=trade_date, persist=False)
    except Exception as exc:
        logger.error("strategy governance snapshot failed: %s", exc, exc_info=True)
        return {
            "status": "degraded",
            "trade_date": trade_date,
            "summary": {},
            "strategies": [],
            "combinations": [],
            "pools": {"observation": [], "confirmation": [], "tradable": []},
            "allocations": [
                {
                    "target_type": "CASH",
                    "target_key": "cash",
                    "name": "现金",
                    "simulated_weight_pct": 100.0,
                    "reason": "治理数据不可用，保持现金",
                    "real_order_authority": False,
                }
            ],
            "automatic_real_order_submission": False,
            "error": str(exc)[:500],
        }


@router.get("/strategy-center/governance/history")
def strategy_center_governance_history(
    limit: int = Query(default=100, ge=1, le=500),
):
    try:
        return governance_history(limit)
    except Exception as exc:
        return {
            "status": "degraded",
            "metric_evidence": [],
            "lifecycle_events": [],
            "audit_events": [],
            "runs": [],
            "error": str(exc)[:500],
        }


@router.post("/strategy-center/registry")
def strategy_center_register_strategy(payload: StrategyRegistrationRequest, request: Request):
    try:
        data = payload.model_dump()
        return {
            "status": "ok",
            "strategy": register_strategy(
                data,
                operator=_request_admin_actor(request, "策略或版本注册"),
            ),
            "automatic_real_order_submission": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_strategy_registration", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        logger.error("strategy registration failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "strategy_registration_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.post("/strategy-center/combinations")
def strategy_center_register_combination(payload: StrategyCombinationRequest, request: Request):
    try:
        return {
            "status": "ok",
            "combination": register_combination(
                payload.model_dump(),
                operator=_request_admin_actor(request, "策略组合或版本注册"),
            ),
            "automatic_real_order_submission": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_combination_registration", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        logger.error("strategy combination registration failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "combination_registration_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.post("/strategy-center/strategies/{strategy_key}/lifecycle")
def strategy_center_transition_lifecycle(
    payload: LifecycleTransitionRequest,
    request: Request,
    strategy_key: str = Path(..., min_length=3, max_length=80),
):
    try:
        return {
            "status": "ok",
            "transition": transition_lifecycle(
                strategy_key,
                payload.next_status,
                reason=payload.reason,
                operator=_request_admin_actor(request, "策略生命周期变更"),
                evidence=payload.evidence,
            ),
            "automatic_real_order_submission": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_lifecycle_transition", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        logger.error("strategy lifecycle transition failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "lifecycle_transition_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.post("/strategy-center/combinations/{combination_key}/lifecycle")
def strategy_center_transition_combination_lifecycle(
    payload: LifecycleTransitionRequest,
    request: Request,
    combination_key: str = Path(..., min_length=3, max_length=80),
):
    try:
        return {
            "status": "ok",
            "transition": transition_lifecycle(
                combination_key,
                payload.next_status,
                reason=payload.reason,
                operator=_request_admin_actor(request, "组合生命周期变更"),
                evidence=payload.evidence,
                entity_type="COMBINATION",
            ),
            "automatic_real_order_submission": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_lifecycle_transition", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        logger.error("combination lifecycle transition failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "lifecycle_transition_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.post("/strategy-center/metrics")
def strategy_center_add_metric_evidence(payload: StrategyMetricEvidenceRequest, request: Request):
    try:
        return {
            "status": "ok",
            "evidence": record_metric_input(
                payload.model_dump(),
                operator=_request_admin_actor(request, "指标证据提交"),
            ),
            "automatic_real_order_submission": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_metric_evidence", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "metric_evidence_admin_required", str(exc))
    except Exception as exc:
        logger.error("strategy metric evidence failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "metric_evidence_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.get("/strategy-center/metrics/{evidence_id}")
def strategy_center_metric_evidence_detail(
    evidence_id: str = Path(..., min_length=32, max_length=32),
):
    try:
        return {"status": "ok", "evidence": metric_evidence_detail(evidence_id)}
    except ValueError as exc:
        return _governance_api_error(404, "invalid_metric_evidence", str(exc))
    except Exception as exc:
        logger.error("strategy metric detail failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "metric_evidence_detail_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.post("/strategy-center/metrics/{evidence_id}/review")
def strategy_center_review_metric_evidence(
    payload: StrategyMetricReviewRequest,
    request: Request,
    evidence_id: str = Path(..., min_length=32, max_length=32),
):
    try:
        return {
            "status": "ok",
            "evidence": review_metric_input(
                evidence_id,
                decision=payload.decision,
                reason=payload.reason,
                operator=_request_reviewer_actor(request),
            ),
            "automatic_real_order_submission": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_metric_review", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "metric_reviewer_role_required", str(exc))
    except Exception as exc:
        logger.error("strategy metric review failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "metric_review_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.post("/strategy-center/governance/run")
def strategy_center_run_governance(
    payload: StrategyRunRequest | None = Body(default=None),
    request: Request = None,
):
    payload = payload or StrategyRunRequest()
    try:
        operator = _request_admin_actor(request, "手工执行策略治理")
        return governance_snapshot(
            trade_date=payload.trade_date,
            persist=True,
            operator=operator,
            strategy_limit=payload.limit,
        )
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except GovernanceEvidenceNotReady as exc:
        return {
            "status": "blocked",
            "reason": str(exc),
            "allocations": [{
                "target_type": "CASH",
                "target_key": "cash",
                "name": "现金",
                "simulated_weight_pct": 100.0,
                "reason": "权威治理证据未就绪，保持现金",
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": False,
        }
    except Exception as exc:
        logger.error("strategy governance run failed: %s", exc, exc_info=True)
        return _governance_api_error(500, "governance_run_failed", _INTERNAL_GOVERNANCE_ERROR)


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
    strategy_key: str = Path(..., min_length=1, max_length=80),
    request: Request = None,
):
    try:
        operator = _request_admin_actor(request, "策略启停")
        return {"status": "ok", **toggle_strategy_enabled(
            strategy_key, payload.enabled,
            reason=payload.reason,
            operator=operator,
        )}
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except ValueError as exc:
        return _governance_api_error(422, "invalid_strategy", str(exc))
    except Exception as exc:
        logger.error("strategy toggle failed: %s", exc, exc_info=True)
        return _governance_api_error(
            500,
            "strategy_config_unavailable",
            _INTERNAL_GOVERNANCE_ERROR,
        )


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
def strategy_center_run(
    payload: StrategyRunRequest | None = Body(default=None),
    request: Request = None,
):
    payload = payload or StrategyRunRequest()
    try:
        _request_admin_actor(request, "手工刷新策略中心快照")
        snapshot = build_strategy_center_snapshot(payload.trade_date, payload.limit)
        if snapshot.get("source_status") == "missing":
            return {"status": "blocked", "reason": "strategy center data is not ready", "snapshot": snapshot}
        return persist_strategy_center_snapshot(snapshot)
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        logger.error("strategy center run failed: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc)[:500]}
