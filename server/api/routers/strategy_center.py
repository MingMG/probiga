# -*- coding: utf-8 -*-
"""Research-only strategy center API."""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any
import uuid

from fastapi import APIRouter, Body, Path, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from server.engine.strategy_center import (
    MARKET_STATES,
    STRATEGY_CATALOG,
    build_strategy_center_snapshot,
    load_market_snapshot,
    load_etf_forward_ledger,
    load_membership_snapshot_history,
    load_persisted_strategy_center_compact,
    load_qmt_kline_attestation_status,
    load_strategy_configs,
    load_strategy_metrics,
    latest_recommendation_date,
    normalize_trade_date,
    versioned_strategy_configuration,
)
from server.engine.strategy_governance import (
    FUNDING_DETAIL_PAGE_MAX_BYTES,
    FundingDetailItemTooLarge,
    MetricArtifactTooLarge,
    _combination_recipe_detail_page,
    _funding_checkpoint_detail_page,
    governance_history,
    governance_history_section_page,
    governance_snapshot,
    load_canonical_governance_snapshot,
    load_verified_combination_recipe_detail_source,
    load_verified_funding_checkpoint_detail_source,
    metric_evidence_artifact_page,
    metric_evidence_detail,
    record_metric_input,
    review_metric_input,
    register_combination,
    transition_lifecycle,
    toggle_strategy_enabled,
)
from server.engine.strategy_execution_adapters import (
    strategy_execution_adapter_capabilities,
)
from server.engine.strategy_governance_orchestrator import (
    COMPLETED,
    INTEGRITY_ERROR,
    NOT_DUE,
    NOT_READY,
    PROGRAM_ERROR,
    canonical_unavailable_context,
    orchestrate_strategy_governance,
    validate_governance_completion_contract,
    validate_governance_safety_contract,
)
from server.engine.strategy_challenger_factory import (
    StrategyAlreadyRegisteredError,
    list_strategy_challengers,
    promote_strategy_challenger,
    register_new_strategy,
    register_strategy_challenger,
    review_strategy_challenger,
    submit_strategy_challenger_evidence,
)
from server.common.strategy_governance_mode import (
    strategy_governance_base_schema_declared_ready,
    strategy_governance_database_deferred,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _safe_exception_log(operation: str, exc: BaseException) -> str:
    """Log an incident identity without exception text or traceback secrets."""

    incident_id = uuid.uuid4().hex
    logger.error(
        "Strategy center operation failed: incident_id=%s "
        "exception_type=%s operation=%s",
        incident_id,
        type(exc).__name__,
        operation,
    )
    return incident_id


class StrictStrategyWriteModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class StrategyToggleRequest(StrictStrategyWriteModel):
    enabled: bool
    reason: str = Field(default="", max_length=500)


class StrategyRunRequest(StrictStrategyWriteModel):
    trade_date: str = ""
    limit: int = Field(
        default=200,
        ge=1,
        le=500,
        description=(
            "历史客户端兼容参数；不作为任何权威输入的读取上限，"
            "不截断动态策略注册表、适配器发现、治理健康计算、"
            "候选票事实或竞技排名"
        ),
    )


class StrategyRegistrationRequest(StrictStrategyWriteModel):
    strategy_key: str = Field(min_length=3, max_length=80)
    strategy_name: str = Field(min_length=1, max_length=120)
    version: str = Field(min_length=1, max_length=160)
    category: str = Field(default="未分类", max_length=80)
    family_key: str = Field(default="", max_length=80)
    description: str = Field(default="", max_length=1000)
    evaluator_type: str = Field(default="external_evidence", max_length=40)
    evaluator_config: dict[str, Any] = Field(default_factory=dict)
    execution_binding: dict[str, Any] | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(default="注册策略版本", max_length=500)


class StrategyCombinationMemberRequest(StrictStrategyWriteModel):
    strategy_key: str = Field(min_length=3, max_length=80)
    strategy_version: str = Field(default="", max_length=160)
    weight: float = Field(gt=0)


class StrategyCombinationRequest(StrictStrategyWriteModel):
    combination_key: str = Field(min_length=3, max_length=80)
    combination_name: str = Field(min_length=1, max_length=120)
    version: str = Field(min_length=1, max_length=160)
    description: str = Field(default="", max_length=1000)
    members: list[StrategyCombinationMemberRequest] = Field(
        min_length=2, max_length=50,
    )
    constraints: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(default="注册组合版本", max_length=500)


class StrategyChallengerRegistrationRequest(StrategyRegistrationRequest):
    reason: str = Field(default="登记挑战者版本", max_length=500)


class StrategyChallengerReviewRequest(StrictStrategyWriteModel):
    decision: str = Field(pattern="^(CONFIRM|REJECT)$")
    reason: str = Field(min_length=1, max_length=500)


class StrategyChallengerEvidenceRequest(StrictStrategyWriteModel):
    as_of_date: str = Field(min_length=10, max_length=10)
    window_days: int = Field(default=120, ge=20, le=120)
    metrics: dict[str, Any]
    evidence_protocol: str = Field(min_length=1, max_length=80)
    artifact_hash: str = Field(
        min_length=64, max_length=64, pattern="^[0-9a-f]{64}$"
    )
    artifact_manifest: dict[str, Any]
    evidence_revision_at: str = Field(min_length=10, max_length=40)
    reason: str = Field(
        default="提交挑战者可重算验证产物", max_length=500
    )


class StrategyChallengerPromotionRequest(StrictStrategyWriteModel):
    reason: str = Field(default="挑战者晋级为新影子版本", max_length=500)


class LifecycleTransitionRequest(StrictStrategyWriteModel):
    next_status: str = Field(min_length=1, max_length=24)
    reason: str = Field(min_length=1, max_length=500)
    evidence: dict[str, Any] = Field(default_factory=dict)


class StrategyMetricEvidenceRequest(StrictStrategyWriteModel):
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


class StrategyMetricReviewRequest(StrictStrategyWriteModel):
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
            "real_order_authority": False,
        },
    )


_INTERNAL_GOVERNANCE_ERROR = "服务器内部错误；详细原因已写入服务日志"
_STRATEGY_CENTER_READ_ERROR_CODE = "strategy_center_read_unavailable"
_STRATEGY_CENTER_READ_ERROR_MESSAGE = "策略中心数据暂不可读取，请稍后重试"
_GOVERNANCE_RANKING_PAGE_SIZE = 50
_GOVERNANCE_RANKING_PAGE_MAX = 100


def _governance_page_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def _governance_ranking_cursor(
    *, run_uid: str, result_hash: str, entity_type: str,
    query: str, offset: int,
) -> str:
    digest = _governance_page_digest({
        "schema": "probiga.governance-ranking-cursor.v1",
        "run_uid": run_uid,
        "canonical_result_hash": result_hash,
        "entity_type": entity_type,
        "query": query,
        "offset": offset,
    })[:32]
    return f"{offset}.{digest}"


def _parse_governance_ranking_cursor(
    cursor: str, *, run_uid: str, result_hash: str,
    entity_type: str, query: str,
) -> int:
    if not cursor:
        return 0
    raw_offset, separator, supplied_hash = str(cursor).partition(".")
    if (
        separator != "."
        or not raw_offset.isdigit()
        or len(supplied_hash) != 32
    ):
        raise ValueError("治理排名游标格式无效")
    offset = int(raw_offset)
    expected = _governance_ranking_cursor(
        run_uid=run_uid,
        result_hash=result_hash,
        entity_type=entity_type,
        query=query,
        offset=offset,
    )
    if expected != cursor:
        raise ValueError("治理排名游标与canonical修订不一致")
    return offset


def _governance_ranking_page(
    snapshot: dict[str, Any], *, entity_type: str,
    cursor: str = "", limit: int = _GOVERNANCE_RANKING_PAGE_SIZE,
    query: str = "",
) -> dict[str, Any]:
    normalized_type = str(entity_type or "").upper()
    if normalized_type == "STRATEGY":
        collection_key = "strategies"
        search_fields = (
            "strategy_key", "strategy_name", "current_version", "category",
            "primary_industry", "lane", "status_label",
        )
    elif normalized_type == "COMBINATION":
        collection_key = "combinations"
        search_fields = (
            "combination_key", "combination_name", "current_version",
            "lane", "status_label",
        )
    else:
        raise ValueError("治理排名对象只能是策略或组合")
    run_uid = str(snapshot.get("run_uid") or "")
    result_hash = str(snapshot.get("canonical_result_hash") or "")
    if (
        len(run_uid) != 32
        or len(result_hash) != 64
        or snapshot.get("is_canonical") is not True
    ):
        raise ValueError("治理排名缺少已验证canonical修订身份")
    if isinstance(limit, bool) or not 1 <= int(limit) <= (
        _GOVERNANCE_RANKING_PAGE_MAX
    ):
        raise ValueError("治理排名每页只能读取1至100条")
    normalized_query = str(query or "").strip().casefold()
    if len(normalized_query) > 80:
        raise ValueError("治理排名搜索词过长")
    rows = snapshot.get(collection_key)
    if not isinstance(rows, list) or any(
        not isinstance(row, dict) for row in rows
    ):
        raise ValueError("canonical治理排名集合无效")
    filtered = rows
    if normalized_query:
        filtered = [
            row for row in rows
            if any(
                normalized_query in str(row.get(field) or "").casefold()
                for field in search_fields
            )
        ]
    offset = _parse_governance_ranking_cursor(
        cursor,
        run_uid=run_uid,
        result_hash=result_hash,
        entity_type=normalized_type,
        query=normalized_query,
    )
    if offset > len(filtered):
        raise ValueError("治理排名游标超出当前结果范围")
    page_limit = int(limit)
    page_rows = filtered[offset:offset + page_limit]
    next_offset = offset + len(page_rows)
    previous_offset = max(0, offset - page_limit)
    page = {
        "schema": "probiga.governance-ranking-page.v1",
        "run_uid": run_uid,
        "canonical_result_hash": result_hash,
        "trade_date": str(snapshot.get("trade_date") or ""),
        "entity_type": normalized_type,
        "query": normalized_query,
        "offset": offset,
        "limit": page_limit,
        "total_count": len(filtered),
        "unfiltered_total_count": len(rows),
        "rows": page_rows,
        "previous_cursor": (
            _governance_ranking_cursor(
                run_uid=run_uid,
                result_hash=result_hash,
                entity_type=normalized_type,
                query=normalized_query,
                offset=previous_offset,
            ) if offset > 0 else None
        ),
        "next_cursor": (
            _governance_ranking_cursor(
                run_uid=run_uid,
                result_hash=result_hash,
                entity_type=normalized_type,
                query=normalized_query,
                offset=next_offset,
            ) if next_offset < len(filtered) else None
        ),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**page, "page_hash": _governance_page_digest(page)}


def _bounded_governance_overview(
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    if (
        snapshot.get("automatic_real_order_submission") is not False
        or snapshot.get("real_order_authority") is not False
    ):
        raise ValueError("canonical治理结果未显式关闭真实下单权限")
    strategy_page = _governance_ranking_page(
        snapshot, entity_type="STRATEGY",
    )
    combination_page = _governance_ranking_page(
        snapshot, entity_type="COMBINATION",
    )
    strategy_page_metadata = {
        key: value for key, value in strategy_page.items() if key != "rows"
    }
    combination_page_metadata = {
        key: value for key, value in combination_page.items() if key != "rows"
    }
    return {
        **snapshot,
        "strategies": strategy_page["rows"],
        "combinations": combination_page["rows"],
        "ranking_pages": {
            "strategy": strategy_page_metadata,
            "combination": combination_page_metadata,
        },
        "ranking_response_bounded": True,
    }


def _deferred_database_governance_overview(
    trade_date: str,
) -> dict[str, Any]:
    """Return a fixed cash-only view without touching governance storage."""

    return {
        "status": "degraded",
        "strategy_governance_mode": "DEFERRED_DB",
        "base_schema_ready": strategy_governance_base_schema_declared_ready(),
        "schema_ready": False,
        "governance_ready": False,
        "activation_enabled": False,
        "result_mode": "CANONICAL_UNAVAILABLE",
        "is_canonical": False,
        "trade_date": str(trade_date or "")[:10],
        "authoritative_trade_date": "",
        "last_canonical": {},
        "last_canonical_summary": {},
        "input_ready": False,
        "input_reason": "治理数据库迁移待完成，当前保持100%现金",
        "reason_code": "GOVERNANCE_DATABASE_DEFERRED",
        "blocking_stage": "DATABASE_MIGRATION",
        "statistical_funding_eligible": False,
        "new_buy_allowed": False,
        "summary": {
            "strategy_count": 0,
            "combination_count": 0,
            "tradable_count": 0,
            "cash_weight_pct": 100.0,
        },
        "strategies": [],
        "combinations": [],
        "pools": {"observation": [], "confirmation": [], "tradable": []},
        "allocations": [{
            "target_type": "CASH",
            "target_key": "cash",
            "name": "现金",
            "simulated_weight_pct": 100.0,
            "reason": "治理数据库迁移待完成，禁止新增买入",
            "real_order_authority": False,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _canonical_funding_entity(
    *, trade_date: str, run_uid: str, canonical_result_hash: str,
    entity_type: str, entity_key: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve a detail ref only from the exact current canonical result."""

    snapshot = load_canonical_governance_snapshot(trade_date=trade_date)
    if (
        not isinstance(snapshot, dict)
        or snapshot.get("automatic_real_order_submission") is not False
        or snapshot.get("real_order_authority") is not False
    ):
        raise RuntimeError("规范治理结果真实下单边界无效")
    if snapshot.get("statistical_funding_eligible") is not True:
        raise RuntimeError("旧版治理结果仅可展示，不能重新授予资金或展开资金明细")
    if (
        str(snapshot.get("run_uid") or "") != run_uid
        or str(snapshot.get("canonical_result_hash") or "")
        != canonical_result_hash
        or str(snapshot.get("trade_date") or "") != trade_date
    ):
        raise LookupError("canonical_revision_changed")
    normalized_type = str(entity_type or "").upper()
    if normalized_type == "STRATEGY":
        rows = snapshot.get("strategies")
        key_field = "strategy_key"
    elif normalized_type == "COMBINATION":
        rows = snapshot.get("combinations")
        key_field = "combination_key"
    else:
        raise ValueError("资金明细对象类型无效")
    matching = [
        row for row in (rows or [])
        if isinstance(row, dict)
        and str(row.get(key_field) or "") == entity_key
    ] if isinstance(rows, list) else []
    if len(matching) != 1:
        raise KeyError(entity_key)
    return snapshot, matching[0]


def _funding_detail_api_response(page: dict[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(page, dict)
        or page.get("automatic_real_order_submission") is not False
        or page.get("real_order_authority") is not False
        or page.get("response_byte_boxed") is not True
        or not isinstance(page.get("items"), list)
        or page.get("row_count") != len(page["items"])
        or page.get("page_hash") != _governance_page_digest({
            key: value for key, value in page.items() if key != "page_hash"
        })
    ):
        raise RuntimeError("资金明细分页安全合同无效")
    response = {
        "status": "ok",
        "page": page,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    serialized_bytes = len(json.dumps(
        jsonable_encoder(response), ensure_ascii=False,
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8"))
    if serialized_bytes > 4 * 1024 * 1024:
        raise RuntimeError("资金明细API响应超过4MiB硬上限")
    return response


def _safe_read_degraded(
    exc: Exception,
    *,
    operation: str,
    status: str = "degraded",
    **payload: Any,
) -> dict[str, Any]:
    """Log internal read failures without reflecting secrets to clients."""

    incident_id = _safe_exception_log(operation, exc)
    return {
        "status": status,
        **payload,
        "error": _STRATEGY_CENTER_READ_ERROR_CODE,
        "message": _STRATEGY_CENTER_READ_ERROR_MESSAGE,
        "incident_id": incident_id,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _governance_orchestration_response(result: Any) -> dict[str, Any] | JSONResponse:
    """Map the canonical orchestration state to truthful HTTP semantics."""

    if not isinstance(result, dict):
        logger.error(
            "strategy governance orchestrator returned non-object payload: %s",
            type(result).__name__,
        )
        return _governance_api_error(
            500, "invalid_governance_result", _INTERNAL_GOVERNANCE_ERROR,
        )
    status = str(result.get("orchestration_status") or "").upper()
    successful = status in {COMPLETED, NOT_DUE}
    error_status_codes = {
        NOT_READY: 503,
        INTEGRITY_ERROR: 409,
        PROGRAM_ERROR: 500,
    }
    status_code = error_status_codes.get(status)
    if not successful and status_code is None:
        logger.error(
            "strategy governance orchestrator returned unknown status: %r",
            status,
        )
        return _governance_api_error(
            500, "unknown_governance_status", _INTERNAL_GOVERNANCE_ERROR,
        )
    expected_public_status = {
        COMPLETED: "ok",
        NOT_DUE: "not_due",
        NOT_READY: "blocked",
        INTEGRITY_ERROR: "blocked",
        PROGRAM_ERROR: "blocked",
    }[status]
    if result.get("status") != expected_public_status:
        logger.error(
            "strategy governance status contract mismatch: %s/%r",
            status,
            result.get("status"),
        )
        return _governance_api_error(
            500,
            "invalid_governance_status_contract",
            _INTERNAL_GOVERNANCE_ERROR,
        )
    try:
        validate_governance_safety_contract(result)
    except (TypeError, ValueError) as exc:
        _safe_exception_log("governance_orchestrator_safety_contract", exc)
        return _governance_api_error(
            409, "unsafe_governance_result", _INTERNAL_GOVERNANCE_ERROR,
        )
    if status == COMPLETED:
        try:
            validate_governance_completion_contract(
                result,
                target_trade_date=str(result.get("target_trade_date") or ""),
            )
        except (TypeError, ValueError, RuntimeError) as exc:
            _safe_exception_log("governance_completion_contract", exc)
            return _governance_api_error(
                500,
                "invalid_governance_completion_contract",
                _INTERNAL_GOVERNANCE_ERROR,
            )
    if status in {COMPLETED, NOT_DUE}:
        return result
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder(result),
    )


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


def _degraded(
    error: Exception, trade_date: str = "", *, operation: str,
) -> dict[str, Any]:
    try:
        target = normalize_trade_date(trade_date) or str(trade_date or "")[:10]
    except Exception:
        target = str(trade_date or "")[:10]
    return _safe_read_degraded(
        error,
        operation=operation,
        **{
            "trade_date": target,
            "data_date": target,
            "generated_at": "",
            "source_status": "missing",
            "is_stale": True,
            "market_state": {
                "key": "unknown", "name": "数据不足", "confidence": 0,
                "evidence": ["策略中心数据暂不可用"],
                "source_status": "missing",
            },
            "global_gate": {
                "status": "DATA_NOT_READY",
                "reason": "数据不足，不生成确定性动作",
            },
            "strategies": [
                {
                    **item,
                    "enabled": True,
                    "effective_weight": None,
                    "today_signal_count": 0,
                    "sample_count": 0,
                    "return_pct": None,
                    "max_drawdown_pct": None,
                    "win_rate_pct": None,
                    "profit_factor": None,
                    "metric_source": "暂无数据",
                }
                for item in STRATEGY_CATALOG
            ],
            "candidates": [],
            "conflicts": [],
            "summary": {
                "strategy_count": len(STRATEGY_CATALOG),
                "enabled_count": len(STRATEGY_CATALOG),
                "candidate_count": 0,
                "conflict_count": 0,
                "buy_count": 0,
                "blocked_count": 0,
            },
            "disclaimer": (
                "仅用于研究候选和风险提示；"
                "未经明确确认不会执行任何交易。"
            ),
        },
    )


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
        return _degraded(exc, trade_date, operation="overview")


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
        return _safe_read_degraded(
            exc,
            operation="market_state",
            market_state={
                "key": "unknown", "name": "数据不足", "confidence": 0,
            },
        )


@router.get("/strategy-center/configuration")
def strategy_center_configuration():
    try:
        return versioned_strategy_configuration()
    except Exception as exc:
        return _safe_read_degraded(
            exc, operation="configuration", status="error",
        )


@router.get("/strategy-center/governance")
def strategy_center_governance(trade_date: str = Query(default="")):
    """Return a bounded overview of the verified immutable canonical result."""

    if strategy_governance_database_deferred():
        return _deferred_database_governance_overview(trade_date)
    try:
        return _bounded_governance_overview(
            load_canonical_governance_snapshot(trade_date=trade_date)
        )
    except Exception as exc:
        _safe_exception_log("governance_snapshot", exc)
        unavailable = canonical_unavailable_context()
        return {
            "status": "degraded",
            "result_mode": "CANONICAL_UNAVAILABLE",
            "is_canonical": False,
            "trade_date": trade_date,
            "input_ready": False,
            "input_reason": "规范治理结果暂不可读取，当前保持现金",
            "reason_code": "CANONICAL_UNAVAILABLE",
            "blocking_stage": "CANONICAL_READ",
            "authoritative_trade_date": unavailable[
                "authoritative_trade_date"
            ],
            "last_canonical": unavailable["last_canonical"],
            "last_canonical_summary": unavailable["last_canonical"],
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
            "real_order_authority": False,
        }


@router.get("/strategy-center/governance/rankings/{entity_type}")
def strategy_center_governance_rankings(
    entity_type: str = Path(pattern="^(STRATEGY|COMBINATION)$"),
    trade_date: str = Query(default="", max_length=10),
    run_uid: str = Query(min_length=32, max_length=32),
    canonical_result_hash: str = Query(min_length=64, max_length=64),
    cursor: str = Query(default="", max_length=80),
    limit: int = Query(default=50, ge=1, le=100),
    query: str = Query(default="", max_length=80),
):
    """Page one ranking without allowing rows from another canonical revision."""

    try:
        snapshot = load_canonical_governance_snapshot(trade_date=trade_date)
        if (
            str(snapshot.get("run_uid") or "") != run_uid
            or str(snapshot.get("canonical_result_hash") or "")
            != canonical_result_hash
        ):
            return _governance_api_error(
                409,
                "canonical_governance_revision_changed",
                "规范治理结果已更新，请刷新总览后继续翻页",
            )
        return {
            "status": "ok",
            "page": _governance_ranking_page(
                snapshot,
                entity_type=entity_type,
                cursor=cursor,
                limit=limit,
                query=query,
            ),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(
            422, "invalid_governance_ranking_page", str(exc),
        )
    except Exception as exc:
        _safe_exception_log("governance_rankings", exc)
        return _governance_api_error(
            500,
            "governance_ranking_page_failed",
            _INTERNAL_GOVERNANCE_ERROR,
        )


@router.get(
    "/strategy-center/governance/funding/strategies/{strategy_key}"
)
def strategy_center_strategy_funding_detail(
    strategy_key: str = Path(..., min_length=1, max_length=80),
    trade_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    run_uid: str = Query(..., pattern=r"^[0-9a-f]{32}$"),
    canonical_result_hash: str = Query(..., pattern=r"^[0-9a-f]{64}$"),
    series: str = Query(
        default="daily_records",
        pattern=(
            "^(daily_records|equity_curve|daily_stock_market_values|"
            "closed_evidence_by_day|fact_members|holdings)$"
        ),
    ),
    window_days: int = Query(default=60),
    cursor: str = Query(default="", max_length=80),
    limit: int = Query(default=50, ge=1, le=50),
):
    """Page one strategy's current canonical V3 funding facts."""

    if window_days not in {20, 60, 120}:
        return _governance_api_error(
            422, "invalid_funding_detail_window", "窗口只能是20、60或120日",
        )
    try:
        _snapshot, row = _canonical_funding_entity(
            trade_date=trade_date,
            run_uid=run_uid,
            canonical_result_hash=canonical_result_hash,
            entity_type="STRATEGY",
            entity_key=strategy_key,
        )
    except LookupError:
        return _governance_api_error(
            409, "canonical_governance_revision_changed",
            "规范治理结果已更新，请刷新总览后继续读取资金明细",
        )
    except KeyError:
        return _governance_api_error(
            404, "strategy_funding_detail_not_found", "规范结果中没有该策略",
        )
    except Exception as exc:
        _safe_exception_log("strategy_funding_canonical_lookup", exc)
        return _governance_api_error(
            500, "strategy_funding_detail_failed", _INTERNAL_GOVERNANCE_ERROR,
        )
    checkpoint_ref = row.get("funding_checkpoint_ref")
    if (
        not isinstance(checkpoint_ref, dict)
        or checkpoint_ref.get("strategy_key") != strategy_key
        or checkpoint_ref.get("strategy_version")
        != row.get("current_version")
        or checkpoint_ref.get("automatic_real_order_submission") is not False
        or checkpoint_ref.get("real_order_authority") is not False
    ):
        return _governance_api_error(
            404,
            "strategy_funding_detail_not_available",
            "该策略当前没有可由规范化V3资金事实链展开的明细",
        )
    try:
        source = load_verified_funding_checkpoint_detail_source(
            checkpoint_ref, allow_superseded_revision=False,
        )
        if (
            source.get("anchor_run_uid") != run_uid
            or source.get("anchor_current_canonical") is not True
            or source.get("automatic_real_order_submission") is not False
            or source.get("real_order_authority") is not False
        ):
            raise RuntimeError("资金检查点没有锚定当前canonical运行")
        page = _funding_checkpoint_detail_page(
            source["verified_checkpoint"],
            source["verified_fact_chain"],
            series=series,
            window_days=window_days,
            cursor=cursor,
            limit=limit,
        )
        if (
            page.get("strategy_key") != strategy_key
            or page.get("strategy_version") != row.get("current_version")
            or page.get("checkpoint_id")
            != checkpoint_ref.get("checkpoint_id")
        ):
            raise RuntimeError("资金明细页越出策略版本或检查点身份")
        return _funding_detail_api_response(page)
    except FundingDetailItemTooLarge:
        return _governance_api_error(
            413,
            "funding_detail_item_too_large",
            "单条已验证资金事实超过4MiB，拒绝截断嵌套事实",
        )
    except ValueError as exc:
        if "游标" in str(exc):
            return _governance_api_error(
                422, "invalid_funding_detail_cursor", "资金明细游标无效",
            )
        _safe_exception_log("strategy_funding_integrity", exc)
        return _governance_api_error(
            409,
            "strategy_funding_detail_integrity_failed",
            "策略资金事实链完整性校验失败，请刷新规范结果",
        )
    except Exception as exc:
        _safe_exception_log("strategy_funding_detail", exc)
        return _governance_api_error(
            500, "strategy_funding_detail_failed", _INTERNAL_GOVERNANCE_ERROR,
        )


@router.get(
    "/strategy-center/governance/funding/combinations/{combination_key}"
)
def strategy_center_combination_funding_detail(
    combination_key: str = Path(..., min_length=1, max_length=80),
    trade_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    run_uid: str = Query(..., pattern=r"^[0-9a-f]{32}$"),
    canonical_result_hash: str = Query(..., pattern=r"^[0-9a-f]{64}$"),
    series: str = Query(
        default="daily_records",
        pattern="^(daily_records|equity_curve|members)$",
    ),
    window_days: int = Query(default=60),
    cursor: str = Query(default="", max_length=80),
    limit: int = Query(default=50, ge=1, le=50),
):
    """Rebuild and page one funded combination's frozen member recipe."""

    if window_days not in {20, 60, 120}:
        return _governance_api_error(
            422, "invalid_combination_detail_window",
            "窗口只能是20、60或120日",
        )
    try:
        _snapshot, row = _canonical_funding_entity(
            trade_date=trade_date,
            run_uid=run_uid,
            canonical_result_hash=canonical_result_hash,
            entity_type="COMBINATION",
            entity_key=combination_key,
        )
    except LookupError:
        return _governance_api_error(
            409, "canonical_governance_revision_changed",
            "规范治理结果已更新，请刷新总览后继续读取组合配方",
        )
    except KeyError:
        return _governance_api_error(
            404, "combination_funding_detail_not_found", "规范结果中没有该组合",
        )
    except Exception as exc:
        _safe_exception_log("combination_funding_canonical_lookup", exc)
        return _governance_api_error(
            500, "combination_funding_detail_failed", _INTERNAL_GOVERNANCE_ERROR,
        )
    recipe_ref = row.get("combination_recipe_ref")
    if (
        row.get("paper_allocation_eligible") is not True
        or row.get("funding_recipe_ready") is not True
        or not isinstance(recipe_ref, dict)
        or recipe_ref.get("combination_key") != combination_key
        or recipe_ref.get("combination_version") != row.get("current_version")
        or recipe_ref.get("automatic_real_order_submission") is not False
        or recipe_ref.get("real_order_authority") is not False
    ):
        return _governance_api_error(
            404,
            "combination_funding_detail_not_available",
            "组合未取得当前轮模拟资金配方资格，不能展开资金明细",
        )
    try:
        source = load_verified_combination_recipe_detail_source(
            run_uid=run_uid,
            recipe_ref=recipe_ref,
            allow_superseded_revision=False,
        )
        page = _combination_recipe_detail_page(
            source,
            series=series,
            window_days=window_days,
            cursor=cursor,
            limit=limit,
        )
        if (
            page.get("run_uid") != run_uid
            or page.get("combination_key") != combination_key
            or page.get("combination_version") != row.get("current_version")
            or page.get("recipe_hash") != recipe_ref.get("recipe_hash")
            or page.get("cash_fact_materialized") is not False
            or page.get("detail_funding_authority") is not False
        ):
            raise RuntimeError("组合资金配方页越出canonical身份或权限")
        return _funding_detail_api_response(page)
    except FundingDetailItemTooLarge:
        return _governance_api_error(
            413,
            "combination_detail_item_too_large",
            "单条已验证组合配方事实超过4MiB，拒绝截断",
        )
    except ValueError as exc:
        if "游标" in str(exc):
            return _governance_api_error(
                422, "invalid_combination_detail_cursor", "组合明细游标无效",
            )
        _safe_exception_log("combination_funding_integrity", exc)
        return _governance_api_error(
            409,
            "combination_funding_detail_integrity_failed",
            "组合成员资金事实链完整性校验失败，请刷新规范结果",
        )
    except Exception as exc:
        _safe_exception_log("combination_funding_detail", exc)
        return _governance_api_error(
            500, "combination_funding_detail_failed", _INTERNAL_GOVERNANCE_ERROR,
        )


@router.get("/strategy-center/governance/preview")
def strategy_center_governance_preview(
    trade_date: str = Query(default=""),
):
    """Explicit, noncanonical realtime recomputation for research diagnostics."""

    try:
        result = governance_snapshot(trade_date=trade_date, persist=False)
        return {
            **result,
            "result_mode": "PREVIEW_REALTIME",
            "is_canonical": False,
        }
    except Exception as exc:
        _safe_exception_log("governance_preview", exc)
        return _governance_api_error(
            503, "governance_preview_failed", _INTERNAL_GOVERNANCE_ERROR,
        )


@router.get("/strategy-center/governance/adapter-capabilities")
def strategy_center_governance_adapter_capabilities():
    try:
        capabilities = strategy_execution_adapter_capabilities()
        adapters = (
            capabilities.get("adapters")
            if isinstance(capabilities, dict) else None
        )
        dynamic_versions = (
            capabilities.get("dynamic_version_readiness")
            if isinstance(capabilities, dict) else None
        )
        declared_rows = (
            [*adapters, *dynamic_versions]
            if isinstance(adapters, list)
            and isinstance(dynamic_versions, list)
            else None
        )
        if (
            not isinstance(capabilities, dict)
            or capabilities.get("automatic_real_order_submission") is not False
            or capabilities.get("real_order_authority") is not False
            or declared_rows is None
            or any(
                not isinstance(row, dict)
                or row.get("real_order_submission_enabled") is not False
                or row.get("automatic_real_order_submission") is not False
                or row.get("real_order_authority") is not False
                for row in declared_rows
            )
        ):
            raise ValueError("执行适配器能力未显式关闭真实下单权限")
        return {"status": "ok", **capabilities}
    except ValueError as exc:
        _safe_exception_log("adapter_capability_contract", exc)
        return _governance_api_error(
            500,
            "invalid_adapter_capability_contract",
            _INTERNAL_GOVERNANCE_ERROR,
        )


@router.get("/strategy-center/governance/history")
def strategy_center_governance_history(
    limit: int = Query(default=100, ge=1, le=500),
    entity_type: str = Query(default="", max_length=24),
    entity_key: str = Query(default="", max_length=80),
    action: str = Query(default="", max_length=80),
    date_from: str = Query(default="", max_length=10),
    date_to: str = Query(default="", max_length=10),
    before: str = Query(default="", max_length=40),
):
    try:
        history = governance_history(
            limit,
            entity_type=entity_type,
            entity_key=entity_key,
            action=action,
            date_from=date_from,
            date_to=date_to,
            before=before,
        )
    except ValueError as exc:
        return _governance_api_error(
            422, "invalid_governance_history_query", str(exc),
        )
    except Exception as exc:
        return _safe_read_degraded(
            exc,
            operation="governance_history",
            metric_evidence=[],
            adapter_run_receipts=[],
            lifecycle_events=[],
            audit_events=[],
            runs=[],
        )
    if (
        not isinstance(history, dict)
        or history.get("automatic_real_order_submission") is not False
        or history.get("real_order_authority") is not False
    ):
        logger.error("strategy governance history authority contract invalid")
        return _governance_api_error(
            500,
            "invalid_governance_history_contract",
            _INTERNAL_GOVERNANCE_ERROR,
        )
    return history


@router.get("/strategy-center/governance/history/{section}")
def strategy_center_governance_history_section_page(
    section: str = Path(
        pattern=(
            "^(lifecycle|audit|metric-evidence|"
            "adapter-run-receipts|runs)$"
        )
    ),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str = Query(default="", max_length=1200),
    entity_type: str = Query(default="", max_length=24),
    entity_key: str = Query(default="", max_length=80),
    action: str = Query(default="", max_length=40),
    date_from: str = Query(default="", max_length=10),
    date_to: str = Query(default="", max_length=10),
):
    """Return one compact, revision-bound governance-ledger page."""

    normalized_section = section.replace("-", "_")
    try:
        page = governance_history_section_page(
            normalized_section,
            limit=limit,
            cursor=cursor,
            entity_type=entity_type,
            entity_key=entity_key,
            action=action,
            date_from=date_from,
            date_to=date_to,
        )
    except ValueError as exc:
        return _governance_api_error(
            422, "invalid_governance_history_page", str(exc),
        )
    except Exception as exc:
        _safe_exception_log("governance_history_page", exc)
        return _governance_api_error(
            500,
            "governance_history_page_failed",
            _INTERNAL_GOVERNANCE_ERROR,
        )
    rows = page.get("rows") if isinstance(page, dict) else None
    page_hash = page.get("page_hash") if isinstance(page, dict) else None
    revision_hash = (
        page.get("history_revision_hash") if isinstance(page, dict) else None
    )
    total_count = page.get("total_count") if isinstance(page, dict) else None
    forbidden_inline_fields = {
        "payload_json", "evidence_json", "before_json", "after_json",
        "artifact_json", "metrics_json", "receipt_json",
        "candidate_identity_json", "candidate_identity", "result_json",
    }
    if (
        not isinstance(page, dict)
        or page.get("schema")
        != "probiga.strategy-governance-history-section-page.v1"
        or page.get("section") != normalized_section
        or page.get("automatic_real_order_submission") is not False
        or page.get("real_order_authority") is not False
        or page.get("raw_payload_inline") is not False
        or not isinstance(rows, list)
        or len(rows) > limit
        or page.get("row_count") != len(rows)
        or isinstance(total_count, bool)
        or not isinstance(total_count, int)
        or total_count < len(rows)
        or any(
            not isinstance(row, dict)
            or bool(forbidden_inline_fields.intersection(row))
            for row in rows
        )
        or not isinstance(page_hash, str)
        or len(page_hash) != 64
        or any(character not in "0123456789abcdef" for character in page_hash)
        or page_hash != _governance_page_digest({
            key: value for key, value in page.items() if key != "page_hash"
        })
        or not isinstance(revision_hash, str)
        or len(revision_hash) != 64
        or any(
            character not in "0123456789abcdef"
            for character in revision_hash
        )
        or (
            page.get("next_cursor") is not None
            and not isinstance(page.get("next_cursor"), str)
        )
    ):
        logger.error("strategy governance history page contract invalid")
        return _governance_api_error(
            500,
            "invalid_governance_history_page_contract",
            _INTERNAL_GOVERNANCE_ERROR,
        )
    return {
        "status": "ok",
        "page": page,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


@router.post("/strategy-center/registry")
def strategy_center_register_strategy(payload: StrategyRegistrationRequest, request: Request):
    try:
        data = payload.model_dump()
        return {
            "status": "ok",
            "strategy": register_new_strategy(
                data,
                operator=_request_admin_actor(request, "策略或版本注册"),
            ),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except StrategyAlreadyRegisteredError as exc:
        return _governance_api_error(
            409, "strategy_challenger_required", str(exc),
        )
    except ValueError as exc:
        return _governance_api_error(422, "invalid_strategy_registration", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        _safe_exception_log("strategy_registration", exc)
        return _governance_api_error(500, "strategy_registration_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.get("/strategy-center/challengers")
def strategy_center_challengers(
    strategy_key: str = Query(default="", max_length=80),
):
    try:
        return list_strategy_challengers(strategy_key)
    except ValueError as exc:
        return _governance_api_error(422, "invalid_challenger_query", str(exc))
    except Exception as exc:
        _safe_exception_log("challenger_query", exc)
        return _governance_api_error(
            500, "challenger_query_failed", _INTERNAL_GOVERNANCE_ERROR,
        )


@router.post("/strategy-center/challengers")
def strategy_center_register_challenger(
    payload: StrategyChallengerRegistrationRequest,
    request: Request,
):
    try:
        return {
            "status": "ok",
            "challenger": register_strategy_challenger(
                payload.model_dump(),
                operator=_request_admin_actor(request, "登记挑战者版本"),
            ),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(
            422, "invalid_challenger_registration", str(exc),
        )
    except PermissionError as exc:
        return _governance_api_error(
            403, "strategy_admin_required", str(exc),
        )
    except Exception as exc:
        _safe_exception_log("challenger_registration", exc)
        return _governance_api_error(
            500, "challenger_registration_failed", _INTERNAL_GOVERNANCE_ERROR,
        )


@router.post("/strategy-center/challengers/{challenger_id}/review")
def strategy_center_review_challenger(
    payload: StrategyChallengerReviewRequest,
    request: Request,
    challenger_id: str = Path(..., min_length=32, max_length=32),
):
    try:
        return {
            "status": "ok",
            "challenger": review_strategy_challenger(
                challenger_id,
                payload.decision,
                operator=_request_reviewer_actor(request),
                reason=payload.reason,
            ),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_challenger_review", str(exc))
    except PermissionError as exc:
        return _governance_api_error(
            403, "metric_reviewer_role_required", str(exc),
        )
    except Exception as exc:
        _safe_exception_log("challenger_review", exc)
        return _governance_api_error(
            500, "challenger_review_failed", _INTERNAL_GOVERNANCE_ERROR,
        )


@router.post("/strategy-center/challengers/{challenger_id}/evidence")
def strategy_center_submit_challenger_evidence(
    payload: StrategyChallengerEvidenceRequest,
    request: Request,
    challenger_id: str = Path(..., min_length=32, max_length=32),
):
    try:
        data = payload.model_dump()
        reason = str(data.pop("reason") or "")
        return {
            "status": "ok",
            "challenger": submit_strategy_challenger_evidence(
                challenger_id,
                data,
                operator=_request_admin_actor(
                    request, "提交挑战者验证产物"
                ),
                reason=reason,
            ),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except MetricArtifactTooLarge as exc:
        return _governance_api_error(
            413, "challenger_evidence_artifact_too_large", str(exc),
        )
    except ValueError as exc:
        return _governance_api_error(
            422, "invalid_challenger_evidence", str(exc),
        )
    except PermissionError as exc:
        return _governance_api_error(
            403, "strategy_admin_required", str(exc),
        )
    except Exception as exc:
        _safe_exception_log("challenger_evidence_submission", exc)
        return _governance_api_error(
            500,
            "challenger_evidence_submission_failed",
            _INTERNAL_GOVERNANCE_ERROR,
        )


@router.post("/strategy-center/challengers/{challenger_id}/promote")
def strategy_center_promote_challenger(
    payload: StrategyChallengerPromotionRequest,
    request: Request,
    challenger_id: str = Path(..., min_length=32, max_length=32),
):
    try:
        return {
            "status": "ok",
            **promote_strategy_challenger(
                challenger_id,
                operator=_request_admin_actor(request, "晋级挑战者版本"),
                reason=payload.reason,
            ),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(
            422, "invalid_challenger_promotion", str(exc),
        )
    except PermissionError as exc:
        return _governance_api_error(
            403, "strategy_admin_required", str(exc),
        )
    except Exception as exc:
        _safe_exception_log("challenger_promotion", exc)
        return _governance_api_error(
            500, "challenger_promotion_failed", _INTERNAL_GOVERNANCE_ERROR,
        )


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
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_combination_registration", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        _safe_exception_log("combination_registration", exc)
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
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_lifecycle_transition", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        _safe_exception_log("strategy_lifecycle_transition", exc)
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
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_lifecycle_transition", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        _safe_exception_log("combination_lifecycle_transition", exc)
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
            "real_order_authority": False,
        }
    except MetricArtifactTooLarge as exc:
        return _governance_api_error(
            413, "metric_evidence_artifact_too_large", str(exc),
        )
    except ValueError as exc:
        return _governance_api_error(422, "invalid_metric_evidence", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "metric_evidence_admin_required", str(exc))
    except Exception as exc:
        _safe_exception_log("metric_evidence_submission", exc)
        return _governance_api_error(500, "metric_evidence_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.get("/strategy-center/metrics/{evidence_id}")
def strategy_center_metric_evidence_detail(
    evidence_id: str = Path(..., min_length=32, max_length=32),
):
    try:
        evidence = metric_evidence_detail(evidence_id)
        if (
            not isinstance(evidence, dict)
            or evidence.get("automatic_real_order_submission") is not False
            or evidence.get("real_order_authority") is not False
        ):
            logger.error("strategy metric detail authority contract invalid")
            return _governance_api_error(
                500,
                "invalid_metric_evidence_authority_contract",
                _INTERNAL_GOVERNANCE_ERROR,
            )
        return {
            "status": "ok",
            "evidence": evidence,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(404, "invalid_metric_evidence", str(exc))
    except Exception as exc:
        _safe_exception_log("metric_evidence_detail", exc)
        return _governance_api_error(500, "metric_evidence_detail_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.get("/strategy-center/metrics/{evidence_id}/artifact/{section}")
def strategy_center_metric_evidence_artifact_page(
    evidence_id: str = Path(..., min_length=32, max_length=32),
    section: str = Path(
        pattern="^(trades|equity_curve|segments|segment_train_dataset)$"
    ),
    cursor: str = Query(default="", max_length=80),
    limit: int = Query(default=50, ge=1, le=100),
    segment_index: int | None = Query(default=None, ge=1, le=100),
):
    """Return one canonical, response-bounded page of a metric artifact."""

    try:
        page = metric_evidence_artifact_page(
            evidence_id,
            section=section,
            cursor=cursor,
            limit=limit,
            segment_index=segment_index,
        )
        if (
            not isinstance(page, dict)
            or page.get("automatic_real_order_submission") is not False
            or page.get("real_order_authority") is not False
        ):
            logger.error("strategy metric artifact page authority invalid")
            return _governance_api_error(
                500,
                "invalid_metric_artifact_authority_contract",
                _INTERNAL_GOVERNANCE_ERROR,
            )
        return {
            "status": "ok",
            "page": page,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(
            422, "invalid_metric_artifact_page", str(exc),
        )
    except Exception as exc:
        _safe_exception_log("metric_artifact_page", exc)
        return _governance_api_error(
            500,
            "metric_artifact_page_failed",
            _INTERNAL_GOVERNANCE_ERROR,
        )


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
            "real_order_authority": False,
        }
    except ValueError as exc:
        return _governance_api_error(422, "invalid_metric_review", str(exc))
    except PermissionError as exc:
        return _governance_api_error(403, "metric_reviewer_role_required", str(exc))
    except Exception as exc:
        _safe_exception_log("metric_review", exc)
        return _governance_api_error(500, "metric_review_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.post("/strategy-center/governance/run")
def strategy_center_run_governance(
    payload: StrategyRunRequest | None = Body(default=None),
    request: Request = None,
):
    payload = payload or StrategyRunRequest()
    try:
        operator = _request_admin_actor(request, "手工执行策略治理")
        result = orchestrate_strategy_governance(
            requested_trade_date=payload.trade_date,
            strategy_limit=payload.limit,
            operator=operator,
            allow_revision=True,
            governance_runner=governance_snapshot,
        )
        return _governance_orchestration_response(result)
    except PermissionError as exc:
        return _governance_api_error(403, "strategy_admin_required", str(exc))
    except Exception as exc:
        _safe_exception_log("governance_run", exc)
        return _governance_api_error(500, "governance_run_failed", _INTERNAL_GOVERNANCE_ERROR)


@router.get("/strategy-center/etf-forward")
def strategy_center_etf_forward(
    limit: int = Query(default=100, ge=1, le=500),
):
    try:
        return load_etf_forward_ledger(limit)
    except Exception as exc:
        return _safe_read_degraded(
            exc,
            operation="etf_forward",
            observations=[],
        )


@router.get("/strategy-center/membership-history")
def strategy_center_membership_history(
    snapshot_date: str = Query(default=""),
    member_type: str = Query(default="concept", pattern="^(concept|industry)$"),
    group_code: str = Query(default="", max_length=80),
    stock_code: str = Query(default="", max_length=10),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """Return exact-date constituent membership, never heat/rotation data."""

    try:
        return load_membership_snapshot_history(
            snapshot_date=snapshot_date,
            member_type=member_type,
            group_code=group_code,
            stock_code=stock_code,
            limit=limit,
        )
    except Exception as exc:
        return _safe_read_degraded(
            exc,
            operation="membership_history",
            **{
                "status_label": "成员快照历史暂不可读取",
                "snapshot_date": snapshot_date,
                "member_type": member_type,
                "data_category": "POINT_IN_TIME_CONSTITUENT_MEMBERSHIP",
                "data_category_label": (
                    "概念成分归属历史"
                    if member_type == "concept"
                    else "行业成分归属历史"
                ),
                "data_semantics": (
                    "指定交易日收盘后的全量成分归属关系；"
                    "不代表板块热度、资金强弱或轮动信号"
                ),
                "excluded_data_categories": [
                    "SECTOR_HEAT_HISTORY",
                    "SECTOR_ROTATION_HISTORY",
                ],
                "snapshot_complete": False,
                "data": [],
                "automatic_real_order_submission": False,
            },
        )


@router.get("/strategy-center/qmt-kline-attestation")
def strategy_center_qmt_kline_attestation(
    limit: int = Query(default=30, ge=1, le=200),
):
    try:
        return load_qmt_kline_attestation_status(limit)
    except Exception as exc:
        return _safe_read_degraded(
            exc,
            operation="qmt_kline_attestation",
            runs=[],
        )


@router.get("/strategy-center/strategies")
def strategy_center_strategies(trade_date: str = Query(default="")):
    try:
        snapshot = build_strategy_center_snapshot(trade_date, limit=500)
        return {"status": snapshot.get("status"), "trade_date": snapshot.get("trade_date"), "market_state": snapshot.get("market_state"), "strategies": snapshot.get("strategies", []), "disclaimer": snapshot.get("disclaimer")}
    except Exception as exc:
        return _degraded(exc, trade_date, operation="strategies")


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
        _safe_exception_log("strategy_toggle", exc)
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
        if compact:
            snapshot = load_persisted_strategy_center_compact(
                trade_date, limit,
            )
            if snapshot is None:
                return JSONResponse(
                    status_code=503,
                    content={
                        "status": "blocked",
                        "reason_code": (
                            "CANONICAL_STRATEGY_CENTER_RUN_UNAVAILABLE"
                        ),
                        "reason": (
                            "当前没有可与规范治理结果精确绑定的策略中心票池。"
                        ),
                        "trade_date": trade_date,
                        "total": 0,
                        "data": [],
                        "conflicts": [],
                        "automatic_real_order_submission": False,
                        "real_order_authority": False,
                    },
                )
        else:
            snapshot = build_strategy_center_snapshot(trade_date, limit)
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
        degraded = _safe_read_degraded(
            exc,
            operation="candidates",
            trade_date=trade_date,
            total=0,
            data=[],
        )
        if compact:
            return JSONResponse(status_code=503, content=degraded)
        return degraded


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
        return _safe_read_degraded(
            exc,
            operation="stock",
            stock_code=stock_code,
            data=None,
        )


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
        return _safe_read_degraded(
            exc, operation="compare", data=[],
        )


@router.get("/strategy-center/conflicts")
def strategy_center_conflicts(trade_date: str = Query(default=""), limit: int = Query(default=100, ge=1, le=500)):
    try:
        snapshot = build_strategy_center_snapshot(trade_date, limit)
        return {"status": snapshot.get("status"), "trade_date": snapshot.get("trade_date"), "market_state": snapshot.get("market_state"), "total": len(snapshot.get("conflicts", [])), "data": snapshot.get("conflicts", []), "disclaimer": snapshot.get("disclaimer")}
    except Exception as exc:
        return _safe_read_degraded(
            exc, operation="conflicts", total=0, data=[],
        )


@router.post("/strategy-center/run")
def strategy_center_run(
    payload: StrategyRunRequest | None = Body(default=None),
    request: Request = None,
):
    del payload, request
    return JSONResponse(
        status_code=410,
        content={
            "status": "retired",
            "error": "legacy_strategy_center_run_retired",
            "reason": (
                "旧策略中心快照写入口已退役；请使用规范治理运行入口，"
                "票池只展示规范治理结果精确绑定的快照。"
            ),
            "replacement": "/api/strategy-center/governance/run",
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )
