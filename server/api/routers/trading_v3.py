"""V3 positive-expectancy decision APIs and protected manual actions."""
from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import sys
import uuid
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import fields
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, HTTPException, Path as ApiPath, Query
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from server.api.routers._engine import get_engine
from server.api.scheduler_runtime import launch_scheduler_task
from server.common.config import get_scheduler_runtime_config
from server.common.readiness_snapshot import ReadinessSnapshot
from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)
from server.common.scheduler_runtime_health import (
    check_linux_standalone_active_release,
    check_qmt_windows_edge_release_receipt,
)
from server.common.strategy_governance_mode import (
    strategy_governance_database_deferred,
)
from server.common.canonical_decision_bridge import (
    canonical_governance_decision,
    canonical_governance_decision_for_run,
)
from server.trading_v3.config import config_hash, load_v3_config
from server.trading_v3.decision_intelligence import (
    DecisionIntelligenceError,
    analyze_replacement_opportunities,
    diff_run_batches,
    optimize_advisory_portfolio,
)
from server.trading_v3.decision_truth import canonical_hash
from server.trading_v3.horizon_contracts import (
    CalibrationEvidence,
    HORIZON_CONTRACT_SCHEMA,
    HorizonContractError,
    HorizonForecastContract,
    validate_independent_horizon_suite,
)
from server.trading_v3.horizon_candidate_ledger_schema import (
    CANDIDATE_EVALUATION_LEDGER_SCHEMA as HORIZON_CANDIDATE_LEDGER_SCHEMA,
    CANDIDATE_LEDGER_BINDING_PROTOCOL as HORIZON_CANDIDATE_LEDGER_BINDING_PROTOCOL,
    CANDIDATE_LEDGER_ENCODING as HORIZON_CANDIDATE_LEDGER_ENCODING,
    CURRENT_HORIZON_ARTIFACT_SCHEMA as HORIZON_ARTIFACT_SCHEMA,
    CURRENT_HORIZON_MODEL_PROTOCOL as HORIZON_MODEL_PROTOCOL,
    CURRENT_HORIZON_SELECTION_PROTOCOL as HORIZON_SELECTION_PROTOCOL,
    CURRENT_HORIZON_SUITE_SCHEMA as HORIZON_SUITE_SCHEMA,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
    HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
    HISTORICAL_HORIZON_SUITE_SCHEMA_V2,
)
from server.trading_v3.horizon_models import (
    CALIBRATION_PROTOCOL as HORIZON_CALIBRATION_PROTOCOL,
    CONTRACT_ELIGIBILITY_SCOPE as HORIZON_CONTRACT_ELIGIBILITY_SCOPE,
)
from server.trading_v3.learning_intelligence import (
    LearningIntelligenceError,
    build_counterfactual_samples,
    counterfactual_learning_metrics,
)
from server.trading_v3.premarket_gate import build_premarket_gate
from server.trading_v3.release_governance import (
    ContinuousCalibrationEvidence,
    ReleaseGovernanceError,
    evaluate_continuous_calibration,
    transition_shadow_release,
)
from server.trading_v3.repository import TradingV3Repository
from server.trading_v3.shadow_intelligence_repository import (
    ShadowIntelligenceRepository,
)
from server.trading_v3.versioning import code_version


router = APIRouter(prefix="/v3", tags=["trading-v3"])
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DAILY_RESULT_CACHE_SECONDS = 15.0
_DAILY_RESULT_CACHE_LOCK = Lock()
_DAILY_RESULT_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_DAILY_CANONICAL_RESULT_HASH = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)
_DAILY_BUILD_SHA = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
_DAILY_REAL_TRADING_GUARDS = (
    "account_insert",
    "account_update",
    "execution_plan_insert",
    "execution_plan_update",
)

_HYPOTHESIS_NEW_BUY_ACTIONS = frozenset(
    {
        "BUY_OR_HOLD",
        "PAPER_PROBE",
        "PAPER_PROBE_IF_CONFIRMED",
        "PAPER_ORDER_CREATED",
    }
)


def _research_hypothesis_projection(row: dict[str, Any]) -> dict[str, Any]:
    projected = dict(row)
    action = str(projected.get("proposed_action") or "").upper()
    projected["source_proposed_action"] = action
    projected["decision_scope"] = "RESEARCH_ONLY"
    projected["new_buy_eligible"] = False
    if action in _HYPOTHESIS_NEW_BUY_ACTIONS:
        projected["proposed_action"] = "WATCH_CLOSELY"
    return projected


def _research_target_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "decision_scope": "RESEARCH_ONLY",
        "new_buy_eligible": False,
        "display_action": "WATCH",
    }


_GOVERNANCE_DATABASE_DEFERRED_REASON = "GOVERNANCE_DATABASE_DEFERRED"

_ANALYSIS_DATA_BLOCK_REASONS = {
    "KLINE_FEATURE_STAGE_TIMEOUT": (
        "90 日 K 线特征阶段超过运行上限，系统已主动停止本批次，"
        "没有形成策略池或票池。"
    ),
    "KLINE_FEATURE_QUERY_TIMEOUT": (
        "90 日 K 线单批查询超过运行上限，系统已停止本批次以避免"
        "继续占用数据库。"
    ),
    "KLINE_FEATURE_QUERY_CONNECTION_LOST": (
        "读取 90 日 K 线时数据库连接中断，本批次没有形成策略池或票池。"
    ),
    "KLINE_FEATURE_CHUNK_READ_FAILED": (
        "90 日 K 线分批读取失败，本批次没有形成策略池或票池。"
    ),
    "KLINE_FEATURE_EMPTY": (
        "目标日期缺少可用的 90 日 K 线数据，本批次没有形成策略池或票池。"
    ),
}


def _analysis_data_block_reason(error: Any) -> tuple[str, str] | None:
    rendered = " ".join(str(error or "").split())
    upper = rendered.upper()
    if upper.startswith("DATA_BLOCKED:"):
        reason_code = upper.split(":", 1)[1].split(";", 1)[0].strip()
        if reason_code:
            return (
                reason_code,
                _ANALYSIS_DATA_BLOCK_REASONS.get(
                    reason_code,
                    "日级分析所需数据未通过完整性检查，本批次没有形成策略池或票池。",
                ),
            )
    if "LOST CONNECTION TO MYSQL SERVER DURING QUERY" in upper:
        reason_code = "KLINE_FEATURE_QUERY_CONNECTION_LOST"
        return reason_code, _ANALYSIS_DATA_BLOCK_REASONS[reason_code]
    return None


def _analysis_runtime_context(
    engine: Any,
    *,
    requested_date: date | None,
) -> dict[str, Any] | None:
    """Project upstream daily-analysis state before using an old batch."""

    if engine is None:
        return None
    where = "WHERE trade_date = :trade_date" if requested_date else ""
    params = (
        {"trade_date": requested_date.isoformat()}
        if requested_date
        else {}
    )
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT run_uid, trade_date, status, progress_percent,
                           message, error, started_at, finished_at,
                           duration_seconds, updated_at
                    FROM st_recommended_run_history
                    {where}
                    ORDER BY trade_date DESC, started_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()
    except SQLAlchemyError:
        return None
    if not row:
        return None

    row = dict(row)
    run_status = str(row.get("status") or "").strip().lower()
    active = run_status in {"queued", "submitted", "running"}
    blocked_reason = (
        _analysis_data_block_reason(row.get("error"))
        if run_status == "error"
        else None
    )
    if not active and blocked_reason is None:
        return None

    source_date = _iso_date(row.get("trade_date"))
    requested = requested_date.isoformat() if requested_date else source_date
    projected = _decision_context_projection(
        None,
        requested_date=requested_date,
    )
    reason_codes = [
        code
        for code in list(projected.get("reason_codes") or [])
        if code != "DECISION_RUN_NOT_FOUND"
    ]
    if active:
        reason_code = "ANALYSIS_RUN_IN_PROGRESS"
        reason = str(row.get("message") or "日级策略批次正在生成。")
        decision_status = "LOADING"
        data_status = "LOADING"
        envelope_status = "loading"
        display_run_status = "RUNNING"
    else:
        reason_code, reason = blocked_reason
        reason_codes.append("ANALYSIS_DATA_BLOCKED")
        decision_status = "BLOCKED"
        data_status = "DATA_BLOCKED"
        envelope_status = "blocked"
        display_run_status = "DATA_BLOCKED"
    reason_codes.append(reason_code)
    return {
        **projected,
        "requested_date": requested,
        "decision_session_date": requested,
        "data_date": None,
        "expected_data_date": requested,
        "context_mode": "ANALYSIS_RUNTIME",
        "context_date_matches": True,
        "run_uid": None,
        "upstream_run_uid": str(row.get("run_uid") or "") or None,
        "run_status": display_run_status,
        "data_status": data_status,
        "decision_status": decision_status,
        "decision_scope": "RESEARCH_ONLY",
        "paper_order_authority": "NONE",
        "order_authority": False,
        "real_order_authority": "DISABLED",
        "real_order_allowed": False,
        "actionable_output_allowed": False,
        "actionable_status": "DATA_BLOCKED" if blocked_reason else "LOADING",
        "decision_integrity_verified": False,
        "decision_integrity_reason": reason_code,
        "data_blocked_reason": reason,
        "analysis_progress_percent": int(row.get("progress_percent") or 0),
        "analysis_stage": str(row.get("message") or ""),
        "analysis_started_at": _iso_datetime(row.get("started_at")),
        "analysis_finished_at": _iso_datetime(row.get("finished_at")),
        "analysis_duration_seconds": row.get("duration_seconds"),
        "reason_codes": reason_codes,
        "_envelope_status": envelope_status,
    }


def _deferred_decision_context(
    *,
    requested_date: date | None,
) -> dict[str, Any]:
    """Return a cash-only V3 context without consulting decision storage."""

    projected = _decision_context_projection(
        None,
        requested_date=requested_date,
    )
    reason_codes = list(projected.get("reason_codes") or [])
    if _GOVERNANCE_DATABASE_DEFERRED_REASON not in reason_codes:
        reason_codes.append(_GOVERNANCE_DATABASE_DEFERRED_REASON)
    return {
        **projected,
        "data_status": "BLOCKED",
        "decision_status": "BLOCKED",
        "decision_scope": "RESEARCH_ONLY",
        "paper_order_authority": "NONE",
        "order_authority": False,
        "real_order_authority": "DISABLED",
        "real_order_allowed": False,
        "actionable_output_allowed": False,
        "actionable_status": "GOVERNANCE_DATABASE_DEFERRED",
        "strategy_governance_mode": "DEFERRED_DB",
        "governance_deferred": True,
        "activation_enabled": False,
        "reason_codes": reason_codes,
    }


def _deferred_stock_pool_projection(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep immutable audit rows while removing every current action plan."""

    projected_items: list[dict[str, Any]] = []
    for raw_item in list(payload.get("items") or []):
        if not isinstance(raw_item, Mapping):
            continue
        item = dict(raw_item)
        raw_plan = item.get("action_plan")
        plan = dict(raw_plan) if isinstance(raw_plan, Mapping) else {}
        source_actionability = str(
            item.get("actionability")
            or plan.get("actionability")
            or "RESEARCH_ONLY"
        ).upper()
        item["source_actionability"] = source_actionability
        item["actionability"] = "RESEARCH_ONLY"
        item["action_plan"] = {
            **plan,
            "source_actionability": source_actionability,
            "actionability": "RESEARCH_ONLY",
            "label": "治理数据库迁移延期；当前仅保留只读研究审计",
            "buy_range": None,
            "sell_range": None,
            "protective_stop": None,
            "range_status": "GOVERNANCE_DATABASE_DEFERRED",
            "execution_authority": "NONE",
        }
        projected_items.append(item)

    summary = dict(payload.get("summary") or {})
    for field in (
        "display_count",
        "buy_zone_count",
        "wait_trigger_count",
        "paper_only_count",
    ):
        summary[field] = 0
    reason_codes = list(payload.get("reason_codes") or [])
    if _GOVERNANCE_DATABASE_DEFERRED_REASON not in reason_codes:
        reason_codes.append(_GOVERNANCE_DATABASE_DEFERRED_REASON)
    return {
        **dict(payload),
        "strategy_governance_mode": "DEFERRED_DB",
        "governance_deferred": True,
        "activation_enabled": False,
        "decision_scope": "RESEARCH_ONLY",
        "paper_order_authority": "NONE",
        "order_authority": False,
        "real_order_authority": "DISABLED",
        "real_order_allowed": False,
        "actionable_output_allowed": False,
        "reason_codes": reason_codes,
        "items": projected_items,
        "summary": summary,
    }


def _deferred_readiness_projection() -> dict[str, Any]:
    """Expose an explicit non-activatable V3 boundary in deferred mode."""

    config = load_v3_config()
    portfolio = dict(config.get("portfolio") or {})
    paper_discovery = dict(config.get("paper_discovery") or {})
    paper_execution = dict(config.get("paper_execution") or {})
    return {
        "schema": {},
        "production_columns": {},
        "real_trading_database_guards": {},
        "active_calibrated_sleeves": [],
        "active_oos_models": [],
        "incompatible_calibrated_sleeves": [],
        "calibration_rejections": {},
        "validated_portfolio_ready": False,
        "paper_discovery_ready": False,
        "structural_ready": False,
        "data_ready": False,
        "decision_ready": False,
        "paper_authority_ready": False,
        "execution_ready": False,
        "execution_readiness_source": "GOVERNANCE_DATABASE_DEFERRED",
        "execution_blocks": [_GOVERNANCE_DATABASE_DEFERRED_REASON],
        "learning_ready": False,
        "learning_status": "BLOCKED",
        "learning_runtime": {},
        "learning_task_healthy": False,
        "learning_task_status": "BLOCKED",
        "latest_context": None,
        "paper_ready": False,
        "portfolio_limits": {
            "minimum_positions": int(portfolio.get("minimum_positions", 0)),
            "maximum_positions": int(portfolio.get("maximum_positions", 0)),
            "maximum_add_count": int(portfolio.get("maximum_add_count", 0)),
            "maximum_paper_discovery_positions": int(
                paper_discovery.get("maximum_positions", 0)
            ),
            "maximum_live_positions": int(
                paper_execution.get("maximum_live_positions", 0)
            ),
        },
        "real_trading_enabled": False,
        "strategy_governance_mode": "DEFERRED_DB",
        "governance_deferred": True,
        "activation_enabled": False,
        "paper_order_authority": "NONE",
        "order_authority": False,
        "real_order_authority": "DISABLED",
        "real_order_allowed": False,
        "actionable_output_allowed": False,
        "blocks": [_GOVERNANCE_DATABASE_DEFERRED_REASON],
        "warnings": [
            "治理数据库迁移待完成；策略输出仅供只读审计，禁止新增买入"
        ],
    }


def _repo() -> TradingV3Repository:
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V3 database unavailable")
    return TradingV3Repository(engine)


def _shadow_repo() -> ShadowIntelligenceRepository:
    engine = get_engine()
    if engine is None:
        raise HTTPException(
            status_code=503,
            detail="V3 Shadow intelligence database unavailable",
        )
    return ShadowIntelligenceRepository(engine)


def _envelope(
    data: Any,
    *,
    status: str = "ok",
) -> dict[str, Any]:
    config = load_v3_config()
    resolved_code_version, code_version_source = code_version()
    return {
        "status": status,
        "trace_id": uuid.uuid4().hex,
        "generated_at": datetime.now().astimezone().isoformat(
            timespec="seconds"
        ),
        "code_commit_sha": resolved_code_version,
        "code_version_source": code_version_source,
        "config_version": config["strategy_version"],
        "config_hash": config_hash(),
        "real_trading_enabled": False,
        "data": data,
    }


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    normalized = str(value).strip()
    return normalized[:10] or None


def _valid_daily_build_sha(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return bool(
        _DAILY_BUILD_SHA.fullmatch(normalized)
        and normalized != "0" * 40
    )


def _daily_result_trade_date(
    engine: Any,
    requested_date: date | None,
) -> tuple[date | None, str, str | None]:
    """Bind an omitted daily-result date to the authoritative closed session."""

    if requested_date is not None:
        return requested_date, "EXPLICIT_DECISION_DATE", None
    try:
        raw = authoritative_closed_trade_date(engine)
        resolved = date.fromisoformat(str(raw or ""))
    except Exception as exc:
        return None, "AUTHORITATIVE_CLOSED_TRADE_DATE", type(exc).__name__
    return resolved, "AUTHORITATIVE_CLOSED_TRADE_DATE", None


def _daily_result_authoritative_trade_date(
    engine: Any,
    resolved_trade_date: date | None,
    date_resolution: str,
) -> date | None:
    if date_resolution == "AUTHORITATIVE_CLOSED_TRADE_DATE":
        return resolved_trade_date
    try:
        raw = authoritative_closed_trade_date(engine)
        return date.fromisoformat(str(raw or ""))
    except Exception:
        return None


def _adjacent_trade_session_date(
    engine: Any,
    anchor_date: date,
    *,
    direction: str,
) -> date:
    if direction not in {"next", "previous"}:
        raise ValueError("trade-session direction is invalid")
    aggregate = "MIN" if direction == "next" else "MAX"
    comparator = ">" if direction == "next" else "<"
    with engine.connect() as connection:
        raw = connection.execute(
            text(
                f"SELECT {aggregate}(trade_date) FROM si_trade_calendar "
                "WHERE trade_status=1 "
                f"AND trade_date {comparator} :anchor_date "
                "AND EXISTS ("
                "SELECT 1 FROM si_trade_calendar anchor "
                "WHERE anchor.trade_date=:anchor_date "
                "AND anchor.trade_status=1)"
            ),
            {"anchor_date": anchor_date.isoformat()},
        ).scalar()
    if raw is None:
        raise RuntimeError(
            "NEXT_TRADE_SESSION_UNAVAILABLE"
            if direction == "next"
            else "PREVIOUS_TRADE_SESSION_UNAVAILABLE"
        )
    try:
        return raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])
    except ValueError as exc:
        raise RuntimeError("TRADE_SESSION_DATE_INVALID") from exc


def _next_execution_session_date(engine: Any, decision_date: date) -> date:
    return _adjacent_trade_session_date(
        engine,
        decision_date,
        direction="next",
    )


def _decision_date_for_execution_session(
    engine: Any,
    execution_session_date: date,
) -> date:
    return _adjacent_trade_session_date(
        engine,
        execution_session_date,
        direction="previous",
    )


def _iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=_SHANGHAI)
        return normalized.astimezone(_SHANGHAI).isoformat(timespec="seconds")
    normalized = str(value).strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return normalized
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_SHANGHAI)
    return parsed.astimezone(_SHANGHAI).isoformat(timespec="seconds")


def _canonical_payload_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _current_calibration_policy_hash() -> str:
    return _canonical_payload_hash(
        dict(load_v3_config().get("continuous_calibration") or {})
    )


def _utc_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(
                str(value).strip().replace("Z", "+00:00")
            )
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _shadow_evidence_is_fresh(
    observed_at: Any,
    *,
    valid_until: Any = None,
    now: datetime | None = None,
) -> bool:
    observed = _utc_datetime(observed_at)
    if observed is None:
        return False
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if observed > current:
        return False
    policy = dict(load_v3_config().get("continuous_calibration") or {})
    raw_maximum = policy.get("maximum_evidence_age_days", 0)
    if isinstance(raw_maximum, dict):
        maximums = [float(value) for value in raw_maximum.values()]
        maximum_age_days = min(maximums) if maximums else 0.0
    else:
        maximum_age_days = float(raw_maximum or 0)
    if maximum_age_days <= 0:
        return False
    if (current - observed).total_seconds() > maximum_age_days * 86400:
        return False
    expiry = _utc_datetime(valid_until)
    return expiry is None or current <= expiry


def _learning_runtime_truth(
    repository: ShadowIntelligenceRepository,
) -> dict[str, Any]:
    row = repository.latest_learning_run()
    if row is None:
        return {
            "row": None,
            "verified": None,
            "metrics": {},
            "evidence_verified": False,
            "reason_codes": ["LEARNING_RUN_NOT_AVAILABLE"],
        }
    verified = repository.verified_learning_run(
        str(row.get("learning_run_id") or "")
    )
    if verified is None:
        return {
            "row": row,
            "verified": None,
            "metrics": {},
            "evidence_verified": False,
            "reason_codes": ["LEARNING_RUN_PROVENANCE_INVALID"],
        }
    metrics = dict(verified.get("metrics") or {})
    provenance = dict(metrics.get("provenance") or {})
    current_code_version, _source = code_version()
    policy_current = str(verified.get("policy_hash") or "") == (
        _current_calibration_policy_hash()
    )
    config_current = str(
        provenance.get("decision_config_hash")
        or provenance.get("config_hash")
        or ""
    ) == config_hash()
    code_current = str(
        provenance.get("code_commit_sha")
        or provenance.get("code_version")
        or ""
    ) == str(current_code_version)
    fresh = _shadow_evidence_is_fresh(verified.get("evaluated_at"))
    reason_codes: list[str] = []
    if not policy_current:
        reason_codes.append("LEARNING_POLICY_STALE")
    if not config_current:
        reason_codes.append("LEARNING_CONFIG_STALE")
    if not code_current:
        reason_codes.append("LEARNING_CODE_STALE")
    if not fresh:
        reason_codes.append("LEARNING_EVIDENCE_STALE")
    return {
        "row": row,
        "verified": verified,
        "metrics": metrics,
        "evidence_verified": not reason_codes,
        "policy_current": policy_current,
        "config_current": config_current,
        "code_current": code_current,
        "fresh": fresh,
        "reason_codes": reason_codes,
    }


def _decision_context_projection(
    run: dict[str, Any] | None,
    *,
    requested_date: date | None,
) -> dict[str, Any]:
    """Expose one honest, immutable page context.

    The read model deliberately separates data health, decision outcome and
    authority.  A missing/blocked run must never be rendered as a deliberate
    empty portfolio, and a V3 target never grants order authority by itself.
    """

    requested = requested_date.isoformat() if requested_date else None
    today = datetime.now(_SHANGHAI).date().isoformat()
    if not run:
        historical_read_only = bool(requested and requested != today)
        return {
            "requested_date": requested,
            "decision_session_date": requested,
            "data_date": None,
            "expected_data_date": None,
            "context_mode": "UNKNOWN",
            "context_date_matches": True,
            "run_uid": None,
            "decision_at": None,
            "knowledge_cutoff_at": None,
            "evidence_as_of": None,
            "valid_until": None,
            "run_status": "NOT_RUN",
            "data_status": "UNAVAILABLE",
            "decision_status": "UNAVAILABLE",
            "decision_scope": "RESEARCH_ONLY",
            "ranking_authority": "V3_READ_MODEL",
            "execution_authority": "V2_CANONICAL_LEDGER",
            "paper_order_authority": "NONE",
            "order_authority": False,
            "real_order_authority": "DISABLED",
            "real_order_allowed": False,
            "actionable_output_allowed": False,
            "actionable_status": "NOT_AVAILABLE",
            "decision_integrity_verified": False,
            "decision_integrity_reason": "DECISION_RUN_NOT_FOUND",
            "snapshot_manifest_hash": "",
            "historical_read_only": historical_read_only,
            "reason_codes": ["DECISION_RUN_NOT_FOUND"],
        }

    run_status = str(run.get("status") or "UNKNOWN").upper()
    regime = str(
        run.get("dominant_regime")
        or (run.get("regime") or {}).get("dominant_state")
        or "UNKNOWN"
    ).upper()
    target_count = int(run.get("target_count") or 0)
    lifecycle = str(run.get("lifecycle_status") or "RESEARCH_ONLY").upper()
    portfolio = (
        run.get("portfolio")
        if isinstance(run.get("portfolio"), dict)
        else {}
    )
    snapshot = (
        portfolio.get("decision_snapshot")
        if isinstance(portfolio.get("decision_snapshot"), dict)
        else {}
    )
    decision_truth = (
        portfolio.get("decision_truth")
        if isinstance(portfolio.get("decision_truth"), dict)
        else {}
    )
    truth_envelope_verified = bool(
        run.get("decision_integrity_verified") is True
        and not str(run.get("decision_integrity_reason") or "")
        and str(snapshot.get("manifest_hash") or "")
        and str(decision_truth.get("schema_version") or "")
        == "probiga.trading-v3.decision-truth.v1"
        and decision_truth.get("order_authority") is False
        and decision_truth.get("real_order_allowed") is False
        and str(decision_truth.get("execution_authority") or "")
        == "V2_CANONICAL_LEDGER"
    )
    data_date = _iso_date(run.get("trade_date"))
    decision_session_date = _iso_date(
        run.get("requested_as_of")
        or snapshot.get("requested_as_of")
        or run.get("decision_at")
        or run.get("trade_date")
    )
    context_date_mismatch = bool(
        requested
        and decision_session_date
        and requested != decision_session_date
    )
    historical_read_only = bool(
        decision_session_date and decision_session_date != today
    )
    reasons: list[str] = []
    failed = run_status in {"FAILED", "FAILED_DOWNSTREAM", "ERROR"}
    blocked = "BLOCKED" in run_status or regime == "DATA_BLOCKED"
    running = run_status in {
        "CREATED",
        "PROCESSING",
        "RUNNING",
        "DECISION_COMMITTED",
        "POSITIONS_SYNCED",
    }
    if failed:
        data_status = "UNAVAILABLE"
        decision_status = "UNAVAILABLE"
        reasons.append("DECISION_RUN_FAILED")
    elif running:
        data_status = "LOADING"
        decision_status = "LOADING"
        reasons.append("DECISION_RUN_IN_PROGRESS")
    elif blocked:
        data_status = "BLOCKED"
        decision_status = "BLOCKED"
        reasons.append("MARKET_OR_DATA_BLOCKED")
    elif run_status != "COMPLETED":
        data_status = "UNAVAILABLE"
        decision_status = "UNAVAILABLE"
        reasons.append("DECISION_RUN_STATUS_UNTRUSTED")
    elif not truth_envelope_verified:
        data_status = "UNAVAILABLE"
        decision_status = "UNAVAILABLE"
        reasons.append("DECISION_TRUTH_UNVERIFIED")
    elif target_count == 0:
        data_status = "READY"
        decision_status = "EMPTY"
        reasons.append("VALID_RUN_WITHOUT_TARGETS")
    else:
        data_status = "READY"
        decision_status = "CANDIDATE_AVAILABLE"
    if context_date_mismatch:
        data_status = "UNAVAILABLE"
        decision_status = "UNAVAILABLE"
        reasons.append("DECISION_SESSION_DATE_MISMATCH")
    if historical_read_only:
        reasons.append("HISTORICAL_CONTEXT_READ_ONLY")
    valid_values = [
        str(item.get("valid_until"))
        for item in (portfolio.get("targets") or [])
        if isinstance(item, dict) and item.get("valid_until")
    ]
    decision_at = _iso_datetime(
        snapshot.get("decision_at") or run.get("decision_at")
    )
    knowledge_cutoff_at = _iso_datetime(
        snapshot.get("knowledge_cutoff_at") or decision_at
    )
    evidence_as_of = _iso_datetime(
        snapshot.get("feature_time") or decision_at
    )
    paper_review_eligible = bool(
        not historical_read_only
        and not context_date_mismatch
        and not failed
        and not blocked
        and not running
        and lifecycle in {"PAPER_TRIAL", "PAPER_ACTIVE"}
        and truth_envelope_verified
        and str(decision_truth.get("run_status") or "").upper()
        == "COMPLETED"
        and str(decision_truth.get("actionable_status") or "").upper()
        == "PAPER_ACTIONABLE"
        and str(decision_truth.get("paper_order_authority") or "")
        == "V2_GATED"
        and target_count > 0
    )
    if not truth_envelope_verified and "DECISION_TRUTH_UNVERIFIED" not in reasons:
        reasons.append("DECISION_TRUTH_UNVERIFIED")
    if lifecycle not in {"PAPER_TRIAL", "PAPER_ACTIVE"}:
        reasons.append("RESEARCH_LIFECYCLE")
    return {
        "requested_date": requested or decision_session_date,
        "decision_session_date": decision_session_date,
        "data_date": data_date,
        "expected_data_date": data_date,
        "context_mode": str(run.get("mode") or "UNKNOWN").upper(),
        "context_date_matches": not context_date_mismatch,
        "run_uid": str(run.get("run_uid") or "") or None,
        "decision_at": decision_at,
        "knowledge_cutoff_at": knowledge_cutoff_at,
        "evidence_as_of": evidence_as_of,
        "valid_until": min(valid_values) if valid_values else None,
        "run_status": run_status,
        "data_status": data_status,
        "decision_status": decision_status,
        "decision_scope": (
            "INTERNAL_PAPER_TRIAL"
            if not historical_read_only
            and lifecycle in {"PAPER_TRIAL", "PAPER_ACTIVE"}
            else "RESEARCH_ONLY"
        ),
        "ranking_authority": "V3_READ_MODEL",
        "execution_authority": "V2_CANONICAL_LEDGER",
        "paper_order_authority": (
            "V2_GATED" if paper_review_eligible else "NONE"
        ),
        "order_authority": False,
        "real_order_authority": "DISABLED",
        "real_order_allowed": False,
        "actionable_output_allowed": False,
        "actionable_status": str(
            decision_truth.get("actionable_status") or "UNKNOWN"
        ),
        "model_version": str(run.get("model_version") or ""),
        "config_hash": str(run.get("config_hash") or ""),
        "data_snapshot_hash": str(run.get("data_snapshot_hash") or ""),
        "result_hash": str(run.get("result_hash") or ""),
        "decision_integrity_verified": truth_envelope_verified,
        "decision_integrity_reason": str(
            run.get("decision_integrity_reason") or ""
        ),
        "snapshot_manifest_hash": str(
            snapshot.get("manifest_hash") or ""
        ),
        "historical_read_only": historical_read_only,
        "target_count": target_count,
        "reason_codes": reasons,
    }


def _json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _load_run_batch(engine: Any, run_uid: str) -> dict[str, Any]:
    with engine.connect() as connection:
        run = connection.execute(
            text(
                """
                SELECT run_uid, requested_as_of, trade_date, decision_at,
                       status
                FROM st_decision_run_v3
                WHERE run_uid = :run_uid
                LIMIT 1
                """
            ),
            {"run_uid": run_uid},
        ).mappings().first()
        if not run:
            raise HTTPException(
                status_code=404,
                detail=f"decision run not found: {run_uid}",
            )
        if str(run.get("status") or "").upper() != "COMPLETED":
            raise HTTPException(
                status_code=409,
                detail=(
                    "decision batch is not an immutable COMPLETED run: "
                    f"{run_uid}"
                ),
            )
        rows = connection.execute(
            text(
                """
                SELECT f.forecast_id, f.stock_code, f.strategy_key,
                       f.horizon_days, f.forecast_status,
                       f.expected_return_net_pct, f.model_version,
                       f.feature_time, f.valid_until,
                       f.reasons_json, f.features_json, f.theme_code,
                       t.target_weight, t.conservative_return_pct,
                       t.status AS target_status,
                       t.theme_codes_json,
                       t.strategy_keys_json AS target_strategy_keys_json
                FROM st_alpha_forecast_v3 f
                LEFT JOIN st_target_portfolio_v3 t
                  ON t.run_uid = f.run_uid
                 AND t.stock_code = f.stock_code
                WHERE f.run_uid = :run_uid
                ORDER BY f.rank_no, f.stock_code, f.strategy_key
                """
            ),
            {"run_uid": run_uid},
        ).mappings().all()
    items: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        features = _json_value(row.get("features_json"), {})
        reasons = _json_value(row.get("reasons_json"), [])
        target_strategies = _json_value(
            row.get("target_strategy_keys_json"), []
        )
        portfolio_selected = bool(
            isinstance(target_strategies, list)
            and str(row.get("strategy_key") or "") in {
                str(item) for item in target_strategies
            }
        )
        themes = (
            _json_value(row.get("theme_codes_json"), [])
            if portfolio_selected
            else []
        )
        if not themes:
            theme = str(
                row.get("theme_code") or features.get("theme_code") or ""
            ).strip()
            themes = [theme] if theme else []
        items.append(
            {
                "forecast_id": str(row.get("forecast_id") or ""),
                "stock_code": str(row.get("stock_code") or ""),
                "strategy_key": str(row.get("strategy_key") or ""),
                "horizon_days": int(row.get("horizon_days") or 0),
                "selection_status": (
                    str(row.get("target_status") or "TARGET")
                    if portfolio_selected
                    else str(row.get("forecast_status") or "")
                ),
                "portfolio_selected": portfolio_selected,
                "grade": features.get("candidate_grade"),
                "action": features.get("action"),
                "target_weight": (
                    float(row["target_weight"])
                    if portfolio_selected
                    else None
                ),
                "expected_return_net_pct": (
                    float(row["expected_return_net_pct"])
                    if row.get("expected_return_net_pct") is not None
                    else None
                ),
                "conservative_return_pct": (
                    float(row["conservative_return_pct"])
                    if portfolio_selected
                    and row.get("conservative_return_pct") is not None
                    else None
                ),
                "gate_codes": reasons if isinstance(reasons, list) else [],
                "theme_codes": themes if isinstance(themes, list) else [],
                "model_version": str(row.get("model_version") or ""),
                "evidence_as_of": _iso_datetime(row.get("feature_time")),
                "valid_until": _iso_datetime(row.get("valid_until")),
            }
        )
    return {
        "run_uid": str(run["run_uid"]),
        "decision_as_of": _iso_datetime(run["decision_at"]),
        "decision_session_date": _iso_date(
            run.get("requested_as_of") or run.get("decision_at")
        ),
        "data_date": _iso_date(run.get("trade_date")),
        "items": items,
    }


def _one_way_cost_percentages(
    order_value: float,
    account_policy: dict[str, Any],
) -> tuple[float, float]:
    if order_value <= 0:
        raise DecisionIntelligenceError(
            "verified order value is required for cost projection"
        )
    commission = max(
        float(account_policy.get("minimum_commission_cny") or 0),
        order_value * float(account_policy.get("commission_rate") or 0),
    )
    transfer = order_value * float(
        account_policy.get("transfer_fee_rate") or 0
    )
    slippage = order_value * float(
        account_policy.get("default_slippage_rate") or 0
    )
    stamp = order_value * float(
        account_policy.get("sell_stamp_duty_rate") or 0
    )
    return (
        (commission + transfer + slippage) / order_value * 100,
        (commission + transfer + slippage + stamp) / order_value * 100,
    )


def _server_decision_intelligence_snapshot(
    engine: Any,
    *,
    run_uid: str | None,
) -> dict[str, Any]:
    """Project verified server data into advisory replacement/optimizer inputs."""

    with engine.connect() as connection:
        if run_uid:
            run = connection.execute(
                text(
                    """
                    SELECT * FROM st_decision_run_v3
                    WHERE run_uid = :run_uid
                    LIMIT 1
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().first()
        else:
            run = connection.execute(
                text(
                    """
                    SELECT * FROM st_decision_run_v3
                    WHERE status = 'COMPLETED'
                    ORDER BY COALESCE(
                                 requested_as_of, DATE(decision_at)
                             ) DESC,
                             decision_at DESC, run_uid DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()
        if not run:
            raise DecisionIntelligenceError("completed decision run not found")
        run = dict(run)
        if str(run.get("status") or "").upper() != "COMPLETED":
            raise DecisionIntelligenceError(
                "decision intelligence requires a COMPLETED immutable run"
            )
        forecast_rows = connection.execute(
            text(
                """
                SELECT f.forecast_id, f.stock_code, f.short_name,
                       f.strategy_key, f.horizon_days, f.raw_score,
                       f.expected_return_net_pct, f.return_q10_pct,
                       f.expected_mae_pct, f.initial_stop_pct,
                       f.forecast_status, f.theme_code,
                       f.features_json, f.feature_time, f.valid_until,
                       t.target_weight, t.estimated_roundtrip_cost_pct,
                       t.conservative_return_pct,
                       t.primary_strategy_key,
                       t.strategy_keys_json, t.theme_codes_json
                FROM st_alpha_forecast_v3 f
                LEFT JOIN st_target_portfolio_v3 t
                  ON t.run_uid = f.run_uid
                 AND t.stock_code = f.stock_code
                WHERE f.run_uid = :run_uid
                  AND f.raw_score IS NOT NULL
                  AND f.forecast_status NOT IN (
                      'DATA_BLOCKED', 'FEATURE_QUALITY_BLOCKED',
                      'INSUFFICIENT_DATA'
                  )
                ORDER BY f.stock_code, f.expected_return_net_pct DESC,
                         f.raw_score DESC, f.strategy_key
                """
            ),
            {"run_uid": str(run["run_uid"])},
        ).mappings().all()

    portfolio = _json_value(run.get("portfolio_json"), {})
    if not isinstance(portfolio, dict):
        raise DecisionIntelligenceError("decision portfolio manifest is invalid")
    manifest = dict(portfolio.get("decision_snapshot") or {})
    stored_manifest_hash = str(manifest.pop("manifest_hash", ""))
    if (
        not stored_manifest_hash
        or canonical_hash(manifest) != stored_manifest_hash
    ):
        raise DecisionIntelligenceError(
            "decision snapshot provenance cannot be verified"
        )
    manifest["manifest_hash"] = stored_manifest_hash
    equity = float((manifest.get("equity") or {}).get("total_equity") or 0)
    if equity <= 0:
        raise DecisionIntelligenceError("verified decision equity is missing")
    valuation_prices = {
        str(code): float(value or 0)
        for code, value in dict(manifest.get("valuation_prices") or {}).items()
        if str(code) and float(value or 0) > 0
    }
    position_map: dict[str, dict[str, Any]] = {}
    decision_session = _iso_date(
        run.get("requested_as_of")
        or manifest.get("requested_as_of")
        or run.get("decision_at")
    )
    for raw in manifest.get("positions") or []:
        if not isinstance(raw, dict):
            continue
        code = str(raw.get("stock_code") or "")
        quantity = int(raw.get("remaining_quantity") or 0)
        if not code or quantity <= 0:
            continue
        price = float(valuation_prices.get(code) or 0)
        if price <= 0:
            raise DecisionIntelligenceError(
                f"verified valuation price missing for holding {code}"
            )
        item = position_map.setdefault(code, {
            "stock_code": code,
            "quantity": 0,
            "market_value": 0.0,
            "theme_codes": set(),
            "sell_locked": False,
        })
        item["quantity"] += quantity
        item["market_value"] += quantity * price
        theme = str(raw.get("theme_code") or "").strip()
        if theme:
            item["theme_codes"].add(theme)
        settlement = _iso_date(raw.get("settlement_date"))
        if settlement and decision_session and settlement > decision_session:
            item["sell_locked"] = True
    current_positions = [
        {
            "stock_code": code,
            "current_weight": item["market_value"] / equity,
            "theme_codes": sorted(item["theme_codes"]),
            "cluster_key": (
                sorted(item["theme_codes"])[0]
                if item["theme_codes"]
                else ""
            ),
        }
        for code, item in sorted(position_map.items())
    ]

    config = load_v3_config()
    account_policy = dict(config.get("account") or {})
    portfolio_policy = dict(config.get("portfolio") or {})
    intelligence_policy = dict(config.get("decision_intelligence") or {})
    optimizer_policy_cfg = dict(
        intelligence_policy.get("portfolio_optimizer") or {}
    )
    by_stock_horizon: dict[tuple[str, int], dict[str, Any]] = {}
    primary_by_stock: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    for raw in forecast_rows:
        row = dict(raw)
        features = _json_value(row.get("features_json"), {})
        if not isinstance(features, dict):
            features = {}
        row["features"] = features
        code = str(row.get("stock_code") or "")
        horizon = int(row.get("horizon_days") or 0)
        key = (code, horizon)
        current = by_stock_horizon.get(key)
        ranking = (
            float(row.get("expected_return_net_pct") or -10**9),
            float(row.get("raw_score") or -10**9),
            str(row.get("strategy_key") or ""),
        )
        current_ranking = (
            float(current.get("expected_return_net_pct") or -10**9),
            float(current.get("raw_score") or -10**9),
            str(current.get("strategy_key") or ""),
        ) if current else None
        if current is None or ranking > current_ranking:
            by_stock_horizon[key] = row
        is_primary_target = bool(
            str(row.get("primary_strategy_key") or "")
            and str(row.get("primary_strategy_key"))
            == str(row.get("strategy_key") or "")
        )
        current_primary = primary_by_stock.get(code)
        if is_primary_target or current_primary is None:
            primary_by_stock[code] = row

    optimizer_candidates: list[dict[str, Any]] = []
    cost_by_stock_horizon: dict[tuple[str, int], tuple[float, float]] = {}
    for code, row in sorted(primary_by_stock.items()):
        if code in position_map:
            continue
        features = dict(row.get("features") or {})
        price = float(features.get("price") or 0)
        adv = float(features.get("average_amount_20d") or 0)
        stop = float(row.get("initial_stop_pct") or 0)
        expected_net = row.get("expected_return_net_pct")
        conservative_net = (
            row.get("conservative_return_pct")
            if row.get("conservative_return_pct") is not None
            else row.get("return_q10_pct")
        )
        desired_weight = float(
            row.get("target_weight")
            or portfolio_policy.get("initial_probe_position_weight")
            or portfolio_policy.get("normal_position_weight")
            or 0
        )
        if (
            price <= 0
            or adv <= 0
            or stop == 0
            or expected_net is None
            or conservative_net is None
            or desired_weight <= 0
        ):
            warnings.append(f"CANDIDATE_INPUT_INCOMPLETE:{code}")
            continue
        order_value = max(
            float(portfolio_policy.get("minimum_economic_order_cny") or 0),
            equity * desired_weight,
        )
        buy_cost, sell_cost = _one_way_cost_percentages(
            order_value,
            account_policy,
        )
        cost_by_stock_horizon[(code, int(row.get("horizon_days") or 0))] = (
            buy_cost,
            sell_cost,
        )
        theme_codes = _json_value(row.get("theme_codes_json"), [])
        if not isinstance(theme_codes, list) or not theme_codes:
            theme_codes = list(features.get("theme_codes") or [])
        if not theme_codes and row.get("theme_code"):
            theme_codes = [str(row["theme_code"])]
        clusters = list(features.get("theme_cluster_keys") or [])
        optimizer_candidates.append({
            "stock_code": code,
            "stock_name": str(row.get("short_name") or ""),
            "selection_score": float(row.get("raw_score") or 0),
            "conservative_return_gross_pct": (
                float(conservative_net) + buy_cost + sell_cost
            ),
            "price": price,
            "average_daily_value_cny": adv,
            "initial_stop_pct": stop,
            "desired_weight": desired_weight,
            "theme_codes": theme_codes,
            "cluster_key": str(clusters[0]) if clusters else "",
        })

    optimizer = optimize_advisory_portfolio(
        optimizer_candidates,
        policy={
            "equity_cny": equity,
            "risk_asset_cap": float(run.get("risk_asset_cap") or 0),
            "maximum_positions": int(
                portfolio_policy.get("maximum_positions") or 0
            ),
            "maximum_single_weight": float(
                portfolio_policy.get("maximum_single_position_weight") or 0
            ),
            "maximum_theme_weight": float(
                portfolio_policy.get("maximum_theme_weight") or 0
            ),
            "maximum_cluster_weight": float(
                optimizer_policy_cfg.get("maximum_cluster_weight") or 0
            ),
            "maximum_turnover_weight": float(
                portfolio_policy.get("maximum_daily_turnover") or 0
            ),
            "maximum_participation_rate": float(
                optimizer_policy_cfg.get("maximum_participation_rate") or 0
            ),
            "capacity_sessions": int(
                optimizer_policy_cfg.get("capacity_sessions") or 0
            ),
            "minimum_order_cny": float(
                portfolio_policy.get("minimum_economic_order_cny") or 0
            ),
            "minimum_edge_to_cost_multiple": float(
                optimizer_policy_cfg.get("minimum_edge_to_cost_multiple") or 0
            ),
            "standard_trade_risk": float(
                portfolio_policy.get("standard_trade_risk") or 0
            ),
            "board_lot": int(optimizer_policy_cfg.get("board_lot") or 100),
            "fees": account_policy,
        },
        current_positions=current_positions,
    )

    replacement_options: list[dict[str, Any]] = []
    for horizon in sorted({key[1] for key in by_stock_horizon if key[1] > 0}):
        candidate_inputs = []
        for (code, row_horizon), row in sorted(by_stock_horizon.items()):
            if row_horizon != horizon or code in position_map:
                continue
            expected_net = row.get("expected_return_net_pct")
            if expected_net is None:
                continue
            features = dict(row.get("features") or {})
            adv = float(features.get("average_amount_20d") or 0)
            if adv <= 0:
                continue
            costs = cost_by_stock_horizon.get((code, horizon))
            if costs is None:
                desired = float(
                    portfolio_policy.get("initial_probe_position_weight") or 0
                )
                if desired <= 0:
                    continue
                costs = _one_way_cost_percentages(
                    max(
                        float(portfolio_policy.get(
                            "minimum_economic_order_cny"
                        ) or 0),
                        equity * desired,
                    ),
                    account_policy,
                )
            buy_cost, sell_cost = costs
            q10 = row.get("return_q10_pct")
            theme = str(row.get("theme_code") or "").strip()
            candidate_inputs.append({
                "stock_code": code,
                "expected_return_gross_pct": (
                    float(expected_net) + buy_cost + sell_cost
                ),
                "entry_cost_pct": buy_cost,
                "exit_cost_pct": sell_cost,
                "uncertainty_haircut_pct": max(
                    0.0,
                    float(expected_net) - float(q10)
                    if q10 is not None
                    else 0.0,
                ),
                "average_daily_value_cny": adv,
                "theme_codes": [theme] if theme else [],
            })
        holding_inputs = []
        for code, position in sorted(position_map.items()):
            row = by_stock_horizon.get((code, horizon))
            if row is None or row.get("expected_return_net_pct") is None:
                continue
            order_value = max(1.0, float(position["market_value"]))
            buy_cost, sell_cost = _one_way_cost_percentages(
                order_value,
                account_policy,
            )
            expected_net = float(row["expected_return_net_pct"])
            q10 = row.get("return_q10_pct")
            holding_inputs.append({
                "stock_code": code,
                "current_weight": position["market_value"] / equity,
                "expected_return_gross_pct": (
                    expected_net + buy_cost + sell_cost
                ),
                "exit_cost_pct": sell_cost,
                "uncertainty_haircut_pct": max(
                    0.0,
                    expected_net - float(q10)
                    if q10 is not None
                    else 0.0,
                ),
                "theme_codes": sorted(position["theme_codes"]),
                "sell_locked": bool(position["sell_locked"]),
            })
        if not candidate_inputs or not holding_inputs:
            continue
        analysis = analyze_replacement_opportunities(
            candidate_inputs,
            holding_inputs,
            equity_cny=equity,
            maximum_participation_rate=float(
                optimizer_policy_cfg.get("maximum_participation_rate") or 0
            ),
            capacity_sessions=int(
                optimizer_policy_cfg.get("capacity_sessions") or 0
            ),
            maximum_theme_weight=float(
                portfolio_policy.get("maximum_theme_weight") or 0
            ),
            minimum_incremental_net_edge_pct=float(
                intelligence_policy.get(
                    "minimum_replacement_net_edge_pct", 0.5
                )
            ),
        )
        replacement_options.extend({
            **item,
            "horizon_days": horizon,
        } for item in analysis.get("options") or [])

    return {
        "status": "READY",
        "run": {
            "run_uid": str(run["run_uid"]),
            "decision_session_date": decision_session,
            "data_date": _iso_date(run.get("trade_date")),
            "decision_at": _iso_datetime(run.get("decision_at")),
            "snapshot_manifest_hash": stored_manifest_hash,
            "result_hash": str(run.get("result_hash") or ""),
        },
        "replacement_analysis": {
            "status": "READY" if replacement_options else "VALID_EMPTY",
            "options": replacement_options,
            "eligible_count": sum(
                bool(item.get("eligible")) for item in replacement_options
            ),
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        },
        "portfolio_optimization": optimizer,
        "input_summary": {
            "candidate_count": len(optimizer_candidates),
            "holding_count": len(current_positions),
            "equity_cny": equity,
        },
        "warnings": sorted(set(warnings)),
        "execution_revalidation_required": True,
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
    }


def _horizon_contract(value: dict[str, Any]) -> HorizonForecastContract:
    payload = dict(value)
    derived_fields = {
        "schema_version": HORIZON_CONTRACT_SCHEMA,
        "sample_maturity": "PENDING_UNTIL_OUTCOME_MATURES",
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
    }
    for key, expected in derived_fields.items():
        if key not in payload:
            continue
        observed = payload.pop(key)
        if observed != expected:
            raise HorizonContractError(
                f"{key} is a server-derived read-only field"
            )
    allowed = {item.name for item in fields(HorizonForecastContract)}
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise HorizonContractError(
            "unsupported horizon contract fields: " + ", ".join(unknown)
        )
    calibration = payload.get("calibration_evidence")
    if isinstance(calibration, dict):
        payload["calibration_evidence"] = CalibrationEvidence(**calibration)
    return HorizonForecastContract(**payload)


def _research_error(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail=str(exc)[:1000],
    )


def _research_object(value: Any, field: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise DecisionIntelligenceError(f"{field} must be an object")
    return dict(value)


def _research_object_list(value: Any, field: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise DecisionIntelligenceError(f"{field} must be an array")
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise DecisionIntelligenceError(
                f"{field}[{index}] must be an object"
            )
        rows.append(dict(item))
    return rows


_READINESS_SNAPSHOT = ReadinessSnapshot()


def _load_readiness_snapshot():
    # A separate read-only process gives deep checks a real killable deadline;
    # timing out a Python thread alone would leave a stuck DB check running.
    probe = Path(__file__).resolve().parents[3] / "tools" / "read_v3_readiness.py"
    result = subprocess.run(
        [sys.executable, "-B", str(probe)], cwd=probe.parent.parent,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=30, check=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


@router.get("/readiness")
def readiness_snapshot():
    payload, observation = _READINESS_SNAPSHOT.read(_load_readiness_snapshot)
    if payload is None:
        data = _deferred_readiness_projection()
        reason = "READINESS_CHECK_FAILED" if observation["error_type"] else "READINESS_CHECK_RUNNING"
        data.update(execution_readiness_source="READ_ONLY_SNAPSHOT",
                    blocks=[reason], execution_blocks=[reason])
        payload = _envelope(data, status="blocked")
    payload["data"]["readiness_check"] = observation
    return payload


def readiness():
    if strategy_governance_database_deferred():
        return _envelope(
            _deferred_readiness_projection(),
            status="blocked",
        )
    repository = _repo()
    config = load_v3_config()
    tables = repository.table_readiness()
    missing = [name for name, ready in tables.items() if not ready]
    column_reader = getattr(
        repository,
        "production_column_readiness",
        None,
    )
    columns = (
        column_reader()
        if column_reader is not None and not missing
        else {}
    )
    guard_reader = getattr(
        repository,
        "real_trading_guard_readiness",
        None,
    )
    guards = (
        guard_reader()
        if guard_reader is not None and not missing
        else {"insert": False, "update": False}
    )
    calibration_status_reader = getattr(
        repository,
        "active_calibration_status",
        None,
    )
    if not missing and calibration_status_reader is not None:
        calibration_status = calibration_status_reader()
        calibrations = dict(
            calibration_status.get("calibrations") or {}
        )
        calibration_rejections = dict(
            calibration_status.get("rejections") or {}
        )
    else:
        calibrations = (
            repository.active_calibrations() if not missing else {}
        )
        calibration_rejections = {}
    version_token = str(
        config.get("calibration_version_token") or ""
    )
    version_tokens = dict(
        config.get("calibration_version_tokens") or {}
    )
    compatible = {
        key: value
        for key, value in calibrations.items()
        if (
            not str(version_tokens.get(key) or version_token)
            or str(version_tokens.get(key) or version_token)
            in str(getattr(value, "model_version", ""))
        )
        and value.has_valid_score_direction()
    }
    validation_reader = getattr(
        repository,
        "latest_validations_for_models",
        None,
    )
    matching_validations = (
        validation_reader(
            getattr(value, "model_version", "")
            for value in compatible.values()
        )
        if validation_reader is not None
        else {}
    )
    active_oos_models = []
    for strategy_key, calibration in sorted(compatible.items()):
        model_version = str(
            getattr(calibration, "model_version", "")
        )
        validation = matching_validations.get(model_version) or {}
        active_oos_models.append(
            {
                "strategy_key": strategy_key,
                "model_version": model_version,
                "dataset_hash": str(
                    getattr(calibration, "dataset_hash", "")
                ),
                "validation_status": str(
                    validation.get("result_status") or "MISSING"
                ),
                "validation_id": str(
                    validation.get("validation_id") or ""
                ),
                "validation_created_at": validation.get(
                    "created_at"
                ),
            }
        )
    discovery_ready = bool(
        config.get("paper_discovery", {}).get("enabled")
    )
    blocks = []
    warnings = []
    if missing:
        blocks.append("V3_SCHEMA_INCOMPLETE")
    if columns and not all(columns.values()):
        blocks.append("V3_PRODUCTION_COLUMNS_INCOMPLETE")
    if not all(guards.values()):
        blocks.append("REAL_TRADING_DATABASE_GUARD_MISSING")
    if not compatible:
        if discovery_ready:
            warnings.append("NO_COMPATIBLE_OOS_CALIBRATION")
        else:
            blocks.append("NO_COMPATIBLE_OOS_CALIBRATION")
    validated_portfolio_ready = bool(
        not missing
        and (not columns or all(columns.values()))
        and all(guards.values())
        and compatible
    )
    paper_discovery_ready = bool(
        discovery_ready
        and not missing
        and (not columns or all(columns.values()))
        and all(guards.values())
    )
    structural_ready = bool(
        not missing
        and (not columns or all(columns.values()))
        and all(guards.values())
        and (compatible or discovery_ready)
    )
    latest_run_reader = getattr(
        repository,
        "latest_run_metadata",
        None,
    )
    latest_run_error = ""
    latest_run = None
    latest_context = None
    if latest_run_reader is not None:
        try:
            latest_run = latest_run_reader()
            latest_context = _decision_context_projection(
                latest_run,
                requested_date=None,
            )
        except Exception as exc:
            latest_run_error = type(exc).__name__
    data_ready = bool(
        latest_context
        and latest_context.get("data_status") == "READY"
        and latest_context.get("decision_integrity_verified") is True
        and not latest_context.get("historical_read_only")
    )
    paper_authority_ready = bool(
        latest_context
        and latest_context.get("decision_status")
        == "CANDIDATE_AVAILABLE"
        and latest_context.get("decision_integrity_verified") is True
        and int(latest_context.get("target_count") or 0) > 0
        and latest_context.get("paper_order_authority") == "V2_GATED"
        and latest_context.get("execution_authority")
        == "V2_CANONICAL_LEDGER"
        and latest_context.get("order_authority") is False
        and latest_context.get("real_order_allowed") is False
    )
    if latest_run_reader is None:
        blocks.append("DECISION_CONTEXT_READER_UNAVAILABLE")
    elif latest_run_error:
        blocks.append("LATEST_DECISION_CONTEXT_UNAVAILABLE")
    elif latest_run is None:
        blocks.append("NO_DECISION_RUN")
    elif latest_context and latest_context.get("data_status") == "BLOCKED":
        blocks.append("LATEST_DECISION_DATA_BLOCKED")
    elif latest_context and latest_context.get("data_status") == "UNAVAILABLE":
        blocks.append("LATEST_DECISION_UNAVAILABLE")
    elif latest_context and latest_context.get("data_status") == "LOADING":
        blocks.append("LATEST_DECISION_IN_PROGRESS")
    elif latest_context and latest_context.get("historical_read_only"):
        blocks.append("LATEST_DECISION_HISTORICAL_ONLY")
    elif (
        data_ready
        and latest_context
        and latest_context.get("decision_status") == "EMPTY"
    ):
        warnings.append("LATEST_DECISION_NO_ACTION")
    elif data_ready and not paper_authority_ready:
        blocks.append("LATEST_DECISION_PAPER_AUTHORITY_UNVERIFIED")

    learning_ready: bool | None = None
    learning_status = "UNKNOWN"
    learning_task_healthy: bool | None = None
    learning_task_status = "UNKNOWN"
    learning_runtime_summary: dict[str, Any] = {}
    repository_engine = getattr(repository, "engine", None)
    if repository_engine is not None:
        try:
            with repository_engine.connect() as connection:
                learning_task = connection.execute(
                    text(
                        """
                        SELECT enabled, last_run_status
                        FROM st_scheduled_tasks
                        WHERE task_type = 'trading_v3_counterfactual_audit'
                        LIMIT 1
                        """
                    )
                ).mappings().first()
            if learning_task:
                learning_task_status = str(
                    learning_task.get("last_run_status") or "NOT_RUN"
                ).upper()
                learning_task_healthy = bool(
                    int(learning_task.get("enabled") or 0) == 1
                    and learning_task_status
                    in {"SUCCESS", "COMPLETED", "SUCCEEDED"}
                )
            else:
                learning_task_healthy = False
                learning_task_status = "NOT_REGISTERED"
        except Exception:
            learning_task_healthy = False
            learning_task_status = "UNAVAILABLE"
        try:
            learning_runtime = _learning_runtime_truth(
                ShadowIntelligenceRepository(repository_engine)
            )
            verified_learning = learning_runtime.get("verified") or {}
            learning_metrics = dict(
                learning_runtime.get("metrics") or {}
            )
            horizon_readiness = dict(
                learning_metrics.get("horizon_readiness") or {}
            )
            each_horizon_ready = all(
                bool(
                    dict(
                        horizon_readiness.get(f"T+{horizon}") or {}
                    ).get("ready")
                )
                for horizon in (1, 5, 20)
            )
            learning_status = str(
                verified_learning.get("learning_status") or "COLLECTING"
            )
            learning_ready = bool(
                learning_runtime.get("evidence_verified")
                and learning_status == "EVIDENCE_READY"
                and each_horizon_ready
            )
            learning_runtime_summary = {
                "learning_run_id": verified_learning.get(
                    "learning_run_id"
                ),
                "evidence_verified": bool(
                    learning_runtime.get("evidence_verified")
                ),
                "per_horizon": horizon_readiness,
                "reason_codes": list(
                    learning_runtime.get("reason_codes") or []
                ),
            }
        except Exception as exc:
            learning_ready = False
            learning_status = "UNAVAILABLE"
            learning_runtime_summary = {
                "evidence_verified": False,
                "reason_codes": ["LEARNING_RUNTIME_UNAVAILABLE"],
                "detail": str(exc)[:300],
            }
    if learning_ready is False:
        warnings.append("COUNTERFACTUAL_LEARNING_NOT_READY")
    if learning_task_healthy is False:
        warnings.append("COUNTERFACTUAL_TASK_NOT_HEALTHY")

    execution_ready: bool | None = None
    execution_blocks: list[str] = []
    if repository_engine is not None:
        try:
            from server.api.routers import trading_v2

            execution_payload = trading_v2.readiness()
            execution_data = dict(execution_payload.get("data") or {})
            execution_ready = bool(
                execution_data.get("ready_for_new_positions")
            )
            execution_blocks = list(execution_data.get("blocks") or [])
        except Exception:
            execution_ready = False
            execution_blocks = ["V2_EXECUTION_READINESS_UNAVAILABLE"]
    if execution_ready is False:
        blocks.append("V2_EXECUTION_NOT_READY")
    elif execution_ready is None:
        blocks.append("V2_EXECUTION_READINESS_UNAVAILABLE")

    decision_ready = bool(structural_ready and data_ready)
    paper_ready = bool(
        decision_ready
        and paper_authority_ready
        and execution_ready is True
    )
    return _envelope(
        {
            "schema": tables,
            "production_columns": columns,
            "real_trading_database_guards": guards,
            "active_calibrated_sleeves": sorted(compatible),
            "active_oos_models": active_oos_models,
            "incompatible_calibrated_sleeves": sorted(
                set(calibrations)
                - set(compatible)
                | set(calibration_rejections)
            ),
            "calibration_rejections": calibration_rejections,
            "validated_portfolio_ready": (
                validated_portfolio_ready
            ),
            "paper_discovery_ready": paper_discovery_ready,
            "structural_ready": structural_ready,
            "data_ready": data_ready,
            "decision_ready": decision_ready,
            "paper_authority_ready": paper_authority_ready,
            "execution_ready": execution_ready,
            "execution_readiness_source": "V2_CANONICAL_LEDGER",
            "execution_blocks": execution_blocks,
            "learning_ready": learning_ready,
            "learning_status": learning_status,
            "learning_runtime": learning_runtime_summary,
            "learning_task_healthy": learning_task_healthy,
            "learning_task_status": learning_task_status,
            "latest_context": latest_context,
            "paper_ready": paper_ready,
            "portfolio_limits": {
                "minimum_positions": int(
                    config.get("portfolio", {}).get(
                        "minimum_positions", 0
                    )
                ),
                "maximum_positions": int(
                    config.get("portfolio", {}).get(
                        "maximum_positions", 0
                    )
                ),
                "maximum_add_count": int(
                    config.get("portfolio", {}).get(
                        "maximum_add_count", 0
                    )
                ),
                "maximum_paper_discovery_positions": int(
                    config.get("paper_discovery", {}).get(
                        "maximum_positions", 0
                    )
                ),
                "maximum_live_positions": int(
                    config.get("paper_execution", {}).get(
                        "maximum_live_positions", 0
                    )
                ),
            },
            "real_trading_enabled": False,
            "blocks": blocks,
            "warnings": warnings,
        },
        status="ok" if paper_ready else "blocked",
    )
@router.get("/context")
def decision_context(
    trade_date: date | None = None,
):
    """Resolve the one immutable context every trading page must share."""

    if strategy_governance_database_deferred():
        return _envelope(
            _deferred_decision_context(requested_date=trade_date),
            status="blocked",
        )
    repository = _repo()
    run = repository.latest_run_metadata(trade_date)
    projected = _decision_context_projection(
        run,
        requested_date=trade_date,
    )
    if str(projected.get("decision_status") or "") not in {
        "CANDIDATE_AVAILABLE", "EMPTY",
    }:
        analysis_runtime = _analysis_runtime_context(
            getattr(repository, "engine", None),
            requested_date=trade_date,
        )
        if analysis_runtime is not None:
            envelope_status = str(
                analysis_runtime.pop("_envelope_status", "blocked")
            )
            return _envelope(analysis_runtime, status=envelope_status)
        governance = canonical_governance_decision(
            trade_date,
            latest_as_of=True,
        )
        if governance is not None:
            return _envelope(governance["context"], status="ok")
    status = {
        "READY": "ok",
        "CANDIDATE_AVAILABLE": "ok",
        "EMPTY": "empty",
        "BLOCKED": "blocked",
        "LOADING": "loading",
        "UNAVAILABLE": "unavailable",
    }.get(str(projected.get("decision_status") or ""), "unavailable")
    return _envelope(projected, status=status)
@router.get("/overview")
def overview(
    compact: bool = False,
    trade_date: date | None = None,
):
    repository = _repo()
    if trade_date is None:
        data = repository.overview()
    else:
        data = {
            "run": repository.latest_run_metadata(trade_date),
            "validation": None,
            "positions": [],
            "requested_date": trade_date.isoformat(),
            "account_position_scope": "CURRENT_ONLY_NOT_INCLUDED_IN_HISTORY",
            "real_trading_enabled": False,
        }
    v3_projection = _decision_context_projection(
        data.get("run"), requested_date=trade_date,
    )
    if str(v3_projection.get("decision_status") or "") not in {
        "CANDIDATE_AVAILABLE", "EMPTY",
    }:
        governance = canonical_governance_decision(
            trade_date,
            latest_as_of=True,
        )
        if governance is not None:
            data = {
                **data,
                "run": governance["run"],
                "requested_date": (
                    trade_date.isoformat()
                    if trade_date else governance["run"]["trade_date"]
                ),
                "account_position_scope": (
                    "CURRENT_ONLY_NOT_INCLUDED_IN_HISTORY"
                ),
                "real_trading_enabled": False,
                "source_system": "STRATEGY_GOVERNANCE",
            }
    if compact:
        run = dict(data.get("run") or {})
        portfolio = dict(run.get("portfolio") or {})
        # The full opportunity audit can exceed hundreds of kilobytes.  The
        # main trading desk needs only a short rejection sample; dedicated V3
        # overview pages continue to receive the complete immutable snapshot.
        portfolio.pop("opportunity_audit", None)
        portfolio["rejected"] = list(
            portfolio.get("rejected") or []
        )[:12]
        run["portfolio"] = portfolio
        data = {**data, "run": run}
    return _envelope(data)


@router.get("/forecasts/latest")
def latest_forecasts(
    limit: int = Query(default=200, ge=1, le=5000),
    status: str = Query(default="", max_length=48),
    trade_date: date | None = Query(default=None),
    strategy_key: str = Query(default="", max_length=64),
    q: str = Query(default="", max_length=64),
):
    return _envelope(
        _repo().latest_forecasts(
            limit=limit,
            status=status,
            trade_date=trade_date,
            strategy_key=strategy_key.strip(),
            query=q.strip(),
        )
    )


@router.get("/research/stock-pool")
def research_stock_pool(trade_date: date = Query()):
    """Read exact-date research observations without changing formal pool truth."""
    from server.trading_v3.research_pool import read_research_pool

    return _envelope(read_research_pool(trade_date))


@router.get("/stock-pool")
def stock_pool(
    trade_date: date | None = Query(default=None),
    before_session_date: date | None = None,
):
    """Read-only, per-stock projection of the latest V3 decision run."""
    if trade_date is not None and before_session_date is not None:
        raise HTTPException(
            status_code=422,
            detail=(
                "trade_date and before_session_date are mutually exclusive"
            ),
        )
    payload = _stock_pool_payload(
        trade_date=trade_date,
        before_session_date=before_session_date,
    )
    return _envelope(payload)


def _stock_pool_payload(
    *,
    trade_date: date | None,
    before_session_date: date | None = None,
    repository: TradingV3Repository | None = None,
) -> dict[str, Any]:
    source = repository or _repo()
    payload = source.stock_pool(
        trade_date=trade_date,
        before_session_date=before_session_date,
    )
    if (
        payload.get("pool_readable") is not True
        and before_session_date is None
    ):
        governance = canonical_governance_decision(
            trade_date,
            latest_as_of=True,
        )
        if governance is not None:
            payload = governance["pool"]
    if strategy_governance_database_deferred():
        payload = _deferred_stock_pool_projection(payload)
    verified_pool = bool(
        payload.get("pool_readable") is True
        and payload.get("decision_integrity_verified") is True
        and str(payload.get("run_status") or "").upper() == "COMPLETED"
    )
    if (
        verified_pool
        or str(payload.get("source_system") or "").upper()
        == "STRATEGY_GOVERNANCE"
    ):
        decision_day = _iso_date(
            payload.get("trade_date") or payload.get("data_date")
        )
        try:
            parsed_decision_day = date.fromisoformat(str(decision_day or ""))
            execution_day = _next_execution_session_date(
                getattr(source, "engine", None),
                parsed_decision_day,
            )
        except Exception:
            reason_codes = list(payload.get("reason_codes") or [])
            if "EXECUTION_SESSION_DATE_UNAVAILABLE" not in reason_codes:
                reason_codes.append("EXECUTION_SESSION_DATE_UNAVAILABLE")
            payload = {
                **payload,
                "decision_date": decision_day,
                "decision_session_date": decision_day,
                "execution_session_date": None,
                "pool_readable": False,
                "decision_integrity_verified": False,
                "reason_codes": reason_codes,
            }
        else:
            payload = {
                **payload,
                "decision_date": decision_day,
                "decision_session_date": decision_day,
                "execution_session_date": execution_day.isoformat(),
            }
    return payload


def _daily_result_cache_get(cache_key: str) -> dict[str, Any] | None:
    now = monotonic()
    with _DAILY_RESULT_CACHE_LOCK:
        cached = _DAILY_RESULT_CACHE.get(cache_key)
        if cached is None:
            return None
        if now - cached[0] >= _DAILY_RESULT_CACHE_SECONDS:
            _DAILY_RESULT_CACHE.pop(cache_key, None)
            return None
        return deepcopy(cached[1])


def _daily_result_cache_set(
    cache_key: str,
    payload: dict[str, Any],
) -> None:
    with _DAILY_RESULT_CACHE_LOCK:
        _DAILY_RESULT_CACHE[cache_key] = (monotonic(), deepcopy(payload))
        if len(_DAILY_RESULT_CACHE) > 16:
            oldest = min(
                _DAILY_RESULT_CACHE,
                key=lambda key: _DAILY_RESULT_CACHE[key][0],
            )
            _DAILY_RESULT_CACHE.pop(oldest, None)


def _daily_canonical_result_hash(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not _DAILY_CANONICAL_RESULT_HASH.fullmatch(raw):
        return None
    return raw.lower()


def _daily_real_trading_safety(repository: Any) -> dict[str, Any]:
    """Read durable account switches and database guards, failing closed."""

    checked_at = datetime.now(_SHANGHAI).isoformat(timespec="seconds")
    base = {
        "schema": "probiga.trading-v3.real-trading-safety.v1",
        "checked_at": checked_at,
        "switch_source": "st_trade_account_v2",
        "guard_source": "information_schema.TRIGGERS",
        "required_guards": list(_DAILY_REAL_TRADING_GUARDS),
    }
    try:
        guard_reader = getattr(
            repository,
            "real_trading_guard_readiness",
            None,
        )
        if not callable(guard_reader):
            raise RuntimeError("real trading guard reader unavailable")
        raw_guards = guard_reader()
        if not isinstance(raw_guards, Mapping):
            raise ValueError("real trading guard evidence is invalid")
        guards = {
            name: raw_guards.get(name) is True
            for name in _DAILY_REAL_TRADING_GUARDS
        }
        engine = getattr(repository, "engine", None)
        if engine is None:
            raise RuntimeError("real trading account engine unavailable")
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT account_id, real_trading_enabled, updated_at
                    FROM st_trade_account_v2
                    ORDER BY account_id
                    """
                )
            ).mappings().all()
    except Exception as exc:
        return {
            **base,
            "status": "UNAVAILABLE",
            "verified": False,
            "real_trading_enabled": None,
            "account_count": None,
            "enabled_account_count": None,
            "accounts": [],
            "guards": {},
            "reason_codes": [
                "REAL_TRADING_SAFETY_READ_FAILED",
                type(exc).__name__,
            ],
        }

    accounts: list[dict[str, Any]] = []
    invalid_switch_evidence = False
    for row in rows:
        account_id = str(row.get("account_id") or "").strip()
        raw_enabled = row.get("real_trading_enabled")
        if not account_id or type(raw_enabled) not in {bool, int}:
            invalid_switch_evidence = True
            continue
        enabled_value = int(raw_enabled)
        if enabled_value not in {0, 1}:
            invalid_switch_evidence = True
            continue
        accounts.append({
            "account_id": account_id,
            "real_trading_enabled": bool(enabled_value),
            "updated_at": _iso_datetime(row.get("updated_at")),
        })

    reason_codes: list[str] = []
    if not rows:
        reason_codes.append("REAL_TRADING_ACCOUNT_EVIDENCE_MISSING")
    if invalid_switch_evidence or len(accounts) != len(rows):
        reason_codes.append("REAL_TRADING_SWITCH_EVIDENCE_INVALID")
    missing_guards = [name for name, present in guards.items() if not present]
    if missing_guards:
        reason_codes.append("REAL_TRADING_GUARD_MISSING")
    enabled_account_count = sum(
        account["real_trading_enabled"] is True for account in accounts
    )
    if enabled_account_count:
        reason_codes.append("REAL_TRADING_SWITCH_ENABLED")
    verified = bool(accounts) and not reason_codes
    return {
        **base,
        "status": "SAFE" if verified else "BLOCKED",
        "verified": verified,
        "real_trading_enabled": (
            None if invalid_switch_evidence or not rows
            else bool(enabled_account_count)
        ),
        "account_count": len(rows),
        "enabled_account_count": enabled_account_count,
        "accounts": accounts,
        "guards": guards,
        "missing_guards": missing_guards,
        "reason_codes": reason_codes,
    }


def _daily_scheduler_health(
    engine: Any,
    *,
    expected_build_sha: str,
) -> dict[str, Any]:
    """Validate both executor identities with the release health contract."""

    try:
        expected_poll_seconds = int(
            get_scheduler_runtime_config()["poll_seconds"]
        )
        with engine.connect() as connection:
            linux_healthy, linux_detail = (
                check_linux_standalone_active_release(
                    connection,
                    expected_build_sha=expected_build_sha,
                    expected_poll_seconds=expected_poll_seconds,
                )
            )
            qmt_healthy, raw_qmt_detail = check_qmt_windows_edge_release_receipt(
                connection,
                expected_build_sha=expected_build_sha,
                expected_poll_seconds=expected_poll_seconds,
            )
    except Exception as exc:
        return {
            "status": "UNAVAILABLE",
            "healthy": False,
            "expected_build_sha": expected_build_sha,
            "roles": {},
            "reason_codes": [
                "SCHEDULER_HEARTBEAT_READ_FAILED",
                type(exc).__name__,
            ],
        }
    qmt_identity = (
        raw_qmt_detail.get("identity")
        if isinstance(raw_qmt_detail, Mapping)
        else None
    )
    qmt_current = (
        qmt_identity.get("current")
        if isinstance(qmt_identity, Mapping)
        else None
    )
    qmt_detail = {
        **dict(raw_qmt_detail),
        # Preserve the role shape consumed by the daily build-identity gate
        # while retaining the complete immutable release-receipt proof.
        "current": qmt_current,
        "immutable_reference_verified": bool(
            qmt_healthy
            and raw_qmt_detail.get("immutable_reference_verified") is True
        ),
    }
    roles = {
        "linux_standalone": {
            **linux_detail,
            "healthy": linux_healthy,
        },
        "qmt_windows_edge": {
            **qmt_detail,
            "healthy": qmt_healthy,
        },
    }
    reason_codes = [
        f"{role.upper()}_{str(error).upper()}"
        for role, detail in roles.items()
        for error in list(detail.get("errors") or [])
    ]
    healthy = linux_healthy and qmt_healthy and not reason_codes
    return {
        "status": "HEALTHY" if healthy else "UNHEALTHY",
        "healthy": healthy,
        "expected_build_sha": str(expected_build_sha or "").lower(),
        "expected_poll_seconds": expected_poll_seconds,
        "roles": roles,
        "reason_codes": reason_codes,
    }


def _daily_context_from_pool(
    pool: Mapping[str, Any],
    *,
    requested_date: date | None,
    expected_build_sha: str | None = None,
    expected_execution_session_date: date | None = None,
) -> dict[str, Any]:
    summary = dict(pool.get("summary") or {})
    raw_items = pool.get("items")
    items = list(raw_items) if isinstance(raw_items, list) else []
    decision_date = _iso_date(
        pool.get("decision_date")
        or pool.get("decision_session_date")
        or pool.get("trade_date")
    )
    data_date = _iso_date(pool.get("trade_date") or pool.get("data_date"))
    execution_session_date = _iso_date(pool.get("execution_session_date"))
    requested = (
        requested_date.isoformat() if requested_date else decision_date
    )
    pool_status = str(pool.get("pool_status") or "UNAVAILABLE").upper()
    raw_canonical_result_hash = str(
        pool.get("canonical_result_hash") or ""
    ).strip()
    canonical_result_hash = _daily_canonical_result_hash(
        raw_canonical_result_hash
    )
    def verified_count(value: Any) -> int | None:
        return value if type(value) is int and value >= 0 else None

    stock_count = verified_count(summary.get("stock_count"))
    target_count = verified_count(summary.get("target_count"))
    candidate_count = verified_count(summary.get("strategy_candidate_count"))
    actual_candidate_count = sum(
        item.get("is_strategy_candidate") is True
        for item in items
        if isinstance(item, Mapping)
    )
    actual_target_count = sum(
        isinstance(item.get("target"), Mapping)
        for item in items
        if isinstance(item, Mapping)
    )
    validation_reasons: list[str] = []
    if not str(pool.get("run_uid") or "").strip():
        validation_reasons.append("DAILY_RESULT_RUN_UID_MISSING")
    if not decision_date or requested != decision_date:
        validation_reasons.append("DAILY_RESULT_DECISION_DATE_MISMATCH")
    try:
        parsed_data_date = date.fromisoformat(str(data_date or ""))
        parsed_decision_date = date.fromisoformat(str(decision_date or ""))
        if parsed_data_date != parsed_decision_date:
            raise ValueError
    except (TypeError, ValueError):
        validation_reasons.append("DAILY_RESULT_DATA_DATE_INVALID")
    try:
        parsed_execution_date = date.fromisoformat(
            str(execution_session_date or "")
        )
        parsed_decision_date = date.fromisoformat(str(decision_date or ""))
        if parsed_execution_date <= parsed_decision_date:
            raise ValueError
    except (TypeError, ValueError):
        validation_reasons.append("DAILY_RESULT_EXECUTION_SESSION_DATE_INVALID")
    expected_execution = _iso_date(expected_execution_session_date)
    if (
        expected_execution is not None
        and execution_session_date != expected_execution
    ):
        validation_reasons.append("DAILY_RESULT_EXECUTION_SESSION_DATE_MISMATCH")
    if not isinstance(raw_items, list):
        validation_reasons.append("DAILY_RESULT_ITEMS_INVALID")
    elif any(not isinstance(item, Mapping) for item in items):
        validation_reasons.append("DAILY_RESULT_ITEM_SHAPE_INVALID")
    if stock_count is None or stock_count != len(items):
        validation_reasons.append("DAILY_RESULT_STOCK_COUNT_MISMATCH")
    if (
        candidate_count is None
        or candidate_count != actual_candidate_count
    ):
        validation_reasons.append("DAILY_RESULT_CANDIDATE_COUNT_MISMATCH")
    if target_count is None or target_count != actual_target_count:
        validation_reasons.append("DAILY_RESULT_TARGET_COUNT_MISMATCH")
    if pool_status == "READY" and not actual_candidate_count:
        validation_reasons.append("DAILY_RESULT_READY_WITHOUT_CANDIDATE")
    if pool_status == "EMPTY" and actual_candidate_count:
        validation_reasons.append("DAILY_RESULT_EMPTY_WITH_CANDIDATE")
    if pool_status not in {"READY", "EMPTY"}:
        validation_reasons.append("DAILY_RESULT_POOL_STATUS_INVALID")
    if pool.get("pool_readable") is not True:
        validation_reasons.append("DAILY_RESULT_POOL_NOT_READABLE")
    if pool.get("decision_integrity_verified") is not True:
        validation_reasons.append("DAILY_RESULT_INTEGRITY_UNVERIFIED")
    if str(pool.get("run_status") or "").upper() != "COMPLETED":
        validation_reasons.append("DAILY_RESULT_RUN_NOT_COMPLETED")
    if str(pool.get("source_system") or "").upper() != "STRATEGY_GOVERNANCE":
        validation_reasons.append("DAILY_RESULT_NOT_CANONICAL_GOVERNANCE")
    if str(pool.get("decision_scope") or "").upper() != "CANONICAL_GOVERNANCE":
        validation_reasons.append("DAILY_RESULT_GOVERNANCE_SCOPE_INVALID")
    if not raw_canonical_result_hash:
        validation_reasons.append("DAILY_RESULT_CANONICAL_HASH_MISSING")
    elif canonical_result_hash is None:
        validation_reasons.append("DAILY_RESULT_CANONICAL_HASH_INVALID")
    if requested and data_date != requested:
        validation_reasons.append("DAILY_RESULT_CANONICAL_DATE_MISMATCH")
    pool_build_sha = str(pool.get("build_commit_sha") or "").strip().lower()
    normalized_expected_build_sha = str(expected_build_sha or "").strip().lower()
    if expected_build_sha is not None:
        if not _valid_daily_build_sha(pool_build_sha):
            validation_reasons.append("DAILY_RESULT_CANONICAL_BUILD_INVALID")
        elif pool_build_sha != normalized_expected_build_sha:
            validation_reasons.append("DAILY_RESULT_CANONICAL_BUILD_MISMATCH")
    if (
        pool.get("is_historical_fallback") is True
        or pool.get("historical_read_only") is True
    ):
        validation_reasons.append("DAILY_RESULT_FALLBACK_NOT_EXACT")
    readable = not validation_reasons
    target_count = target_count or 0
    candidate_count = candidate_count or 0
    valid_until_values = sorted(
        str(item.get("valid_until"))
        for item in items
        if isinstance(item, Mapping) and item.get("valid_until")
    )
    reason_codes = list(pool.get("reason_codes") or [])
    for reason_code in validation_reasons:
        if reason_code not in reason_codes:
            reason_codes.append(reason_code)
    historical_read_only = bool(
        not readable
        or (
            execution_session_date
            and execution_session_date
            < datetime.now(_SHANGHAI).date().isoformat()
        )
    )
    if (
        historical_read_only
        and readable
        and "HISTORICAL_CONTEXT_READ_ONLY" not in reason_codes
    ):
        reason_codes.append("HISTORICAL_CONTEXT_READ_ONLY")
    if readable:
        data_status = "READY"
        decision_status = (
            "CANDIDATE_AVAILABLE" if target_count else "EMPTY"
        )
        run_status = "COMPLETED"
    else:
        data_status = (
            "DATA_BLOCKED"
            if any("BLOCK" in str(code).upper() for code in reason_codes)
            else "UNAVAILABLE"
        )
        decision_status = (
            "BLOCKED" if data_status == "DATA_BLOCKED" else "UNAVAILABLE"
        )
        run_status = "DATA_BLOCKED" if data_status == "DATA_BLOCKED" else "NOT_RUN"
    return {
        "requested_date": requested,
        "decision_date": decision_date,
        "decision_session_date": decision_date,
        "data_date": data_date,
        "expected_data_date": decision_date,
        "execution_session_date": execution_session_date,
        "expected_execution_session_date": expected_execution,
        "build_commit_sha": pool_build_sha or None,
        "expected_build_sha": normalized_expected_build_sha or None,
        "context_mode": "ATOMIC_DAILY_RESULT",
        "context_date_matches": bool(requested and requested == decision_date),
        "run_uid": pool.get("run_uid"),
        "decision_at": _iso_datetime(pool.get("decision_at")),
        "knowledge_cutoff_at": _iso_datetime(pool.get("decision_at")),
        "evidence_as_of": _iso_datetime(pool.get("decision_at")),
        "valid_until": valid_until_values[0] if valid_until_values else None,
        "run_status": run_status,
        "data_status": data_status,
        "decision_status": decision_status,
        "decision_scope": (
            "RESEARCH_ONLY"
            if historical_read_only
            else str(pool.get("decision_scope") or "RESEARCH_ONLY").upper()
        ),
        "ranking_authority": "STRATEGY_GOVERNANCE_CANONICAL",
        "execution_authority": str(
            pool.get("execution_authority") or "NONE"
        ),
        "paper_order_authority": (
            "NONE"
            if historical_read_only
            else str(pool.get("paper_order_authority") or "NONE")
        ),
        "order_authority": False,
        "real_order_authority": "DISABLED",
        "real_order_allowed": False,
        "actionable_output_allowed": bool(
            readable
            and not historical_read_only
            and pool.get("actionable_output_allowed") is True
        ),
        "actionable_status": (
            "HISTORICAL_READ_ONLY" if readable and historical_read_only else
            "PAPER_ACTIONABLE" if readable and target_count else
            "EMPTY" if readable else "DATA_BLOCKED"
        ),
        "decision_integrity_verified": readable,
        "decision_integrity_reason": (
            "" if readable else validation_reasons[0]
        ),
        "historical_read_only": historical_read_only,
        "target_count": target_count,
        "strategy_candidate_count": candidate_count,
        "reason_codes": reason_codes,
        "source_system": str(pool.get("source_system") or ""),
        "canonical_result_hash": canonical_result_hash or "",
    }


def _daily_unavailable_stock_pool(
    requested_date: date | None,
) -> dict[str, Any]:
    requested = requested_date.isoformat() if requested_date else None
    return {
        "run_uid": None,
        "trade_date": None,
        "decision_date": requested,
        "decision_session_date": requested,
        "execution_session_date": None,
        "requested_trade_date": requested,
        "build_commit_sha": None,
        "pool_status": "UNAVAILABLE",
        "pool_readable": False,
        "run_status": None,
        "decision_integrity_verified": False,
        "is_historical_fallback": False,
        "historical_read_only": False,
        "source_system": "STRATEGY_GOVERNANCE",
        "decision_scope": "CANONICAL_GOVERNANCE",
        "canonical_result_hash": "",
        "reason_codes": ["NO_EXACT_CANONICAL_GOVERNANCE_RUN"],
        "items": [],
        "summary": {
            "stock_count": 0,
            "forecast_count": 0,
            "strategy_candidate_count": 0,
            "target_count": 0,
            "rejected_count": 0,
        },
        "strategy_execution": {
            "strategy_count": 0,
            "completed_count": 0,
            "blocked_count": 0,
            "candidate_strategy_count": 0,
            "strategies": [],
        },
    }


def _daily_strategy_pool_projection(
    pool: Mapping[str, Any],
    *,
    exact_pool: bool,
) -> dict[str, Any]:
    raw = pool.get("strategy_execution")
    projection = deepcopy(dict(raw)) if isinstance(raw, Mapping) else {}
    strategies = projection.get("strategies")
    strategy_count = projection.get("strategy_count")
    readable = bool(
        exact_pool
        and isinstance(strategies, list)
        and type(strategy_count) is int
        and strategy_count >= 0
        and strategy_count == len(strategies)
        and all(isinstance(row, Mapping) for row in strategies)
    )
    return {
        **projection,
        "run_uid": pool.get("run_uid"),
        "trade_date": _iso_date(pool.get("trade_date") or pool.get("data_date")),
        "decision_date": _iso_date(
            pool.get("decision_date")
            or pool.get("decision_session_date")
            or pool.get("trade_date")
        ),
        "decision_session_date": _iso_date(
            pool.get("decision_session_date") or pool.get("trade_date")
        ),
        "execution_session_date": _iso_date(
            pool.get("execution_session_date")
        ),
        "build_commit_sha": str(pool.get("build_commit_sha") or "").lower(),
        "run_status": str(pool.get("run_status") or "UNAVAILABLE").upper(),
        "pool_status": (
            "READY" if readable and strategy_count else
            "EMPTY" if readable else "UNAVAILABLE"
        ),
        "pool_readable": readable,
        "decision_integrity_verified": readable,
        "source_system": str(pool.get("source_system") or ""),
        "canonical_result_hash": (
            _daily_canonical_result_hash(pool.get("canonical_result_hash"))
            or ""
        ),
        "reason_codes": (
            [] if readable else ["STRATEGY_POOL_PROJECTION_INVALID"]
        ),
    }


def _daily_stock_pool_projection(pool: Mapping[str, Any]) -> dict[str, Any]:
    """Keep first-screen pool evidence bounded without losing candidates."""

    projection = {
        key: deepcopy(value)
        for key, value in pool.items()
        if key != "items"
    }
    projection["canonical_result_hash"] = (
        _daily_canonical_result_hash(pool.get("canonical_result_hash")) or ""
    )
    raw_items = list(pool.get("items") or [])
    items: list[dict[str, Any]] = []
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            continue
        if (
            raw.get("is_strategy_candidate") is not True
            and not isinstance(raw.get("rejection"), Mapping)
        ):
            continue
        item = {
            key: deepcopy(value)
            for key, value in raw.items()
            if key != "features"
        }
        features = raw.get("features")
        item["features"] = (
            {
                key: deepcopy(features[key])
                for key in (
                    "paper_research_groups",
                    "theme_names",
                    "theme_name",
                )
                if key in features
            }
            if isinstance(features, Mapping)
            else {}
        )
        items.append(item)
    summary = dict(projection.get("summary") or {})
    source_stock_count = summary.get("stock_count")
    summary.update({
        "source_stock_count": source_stock_count,
        "stock_count": len(items),
        "projected_stock_count": len(items),
    })
    projection.update({
        "items": items,
        "summary": summary,
        "projection": "DAILY_RESULT_CANDIDATES_AND_REJECTIONS_V1",
        "source_items_complete": len(items) == len(raw_items),
        "omitted_non_candidate_count": len(raw_items) - len(items),
    })
    return projection


def _daily_overview_from_pool(
    pool: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    real_trading_safety: Mapping[str, Any],
) -> dict[str, Any]:
    items = [
        dict(item)
        for item in list(pool.get("items") or [])
        if isinstance(item, Mapping)
    ]
    targets = [
        {
            "stock_code": item.get("stock_code"),
            "short_name": item.get("stock_name"),
            **dict(item.get("target") or {}),
        }
        for item in items
        if isinstance(item.get("target"), Mapping)
    ]
    rejected = [
        {
            "stock_code": item.get("stock_code"),
            "short_name": item.get("stock_name"),
            **dict(item.get("rejection") or {}),
        }
        for item in items
        if isinstance(item.get("rejection"), Mapping)
    ]
    return {
        "run": {
            "run_uid": context.get("run_uid"),
            "trade_date": context.get("data_date"),
            "decision_date": context.get("decision_date"),
            "decision_session_date": context.get("decision_session_date"),
            "execution_session_date": context.get("execution_session_date"),
            "decision_at": context.get("decision_at"),
            "status": context.get("run_status"),
            "target_count": context.get("target_count"),
            "decision_integrity_verified": context.get(
                "decision_integrity_verified"
            ),
            "canonical_result_hash": context.get("canonical_result_hash"),
            "portfolio": {
                "targets": targets,
                "rejected": rejected[:12],
            },
        },
        "validation": None,
        "positions": [],
        "requested_date": context.get("requested_date"),
        "account_position_scope": "LAZY_LOADED_SEPARATELY",
        "real_trading_enabled": real_trading_safety.get(
            "real_trading_enabled"
        ),
        "real_trading_safety_verified": (
            real_trading_safety.get("verified") is True
        ),
        "bootstrap_projection": True,
    }


@router.get("/daily-result")
def daily_result(
    trade_date: date | None = Query(default=None),
    force: bool = Query(default=False),
):
    """Return one atomic, exact-date first-screen delivery receipt.

    This endpoint intentionally excludes readiness, account and market-clock
    scans.  It proves one verified decision/stock-pool identity first, then the
    browser may load non-critical panels lazily without turning their timeout
    into a false empty-pool conclusion.
    """

    requested_trade_date = trade_date if isinstance(trade_date, date) else None
    force_refresh = force if isinstance(force, bool) else False
    started = monotonic()
    repository = _repo()
    resolved_trade_date, date_resolution, date_resolution_error = (
        _daily_result_trade_date(
            getattr(repository, "engine", None),
            requested_trade_date,
        )
    )
    cache_key = (
        resolved_trade_date.isoformat()
        if resolved_trade_date is not None
        else "authoritative-closed-unavailable"
    )
    if not force_refresh:
        cached = _daily_result_cache_get(cache_key)
        if cached is not None:
            cached["cache"] = {"hit": True, "ttl_seconds": _DAILY_RESULT_CACHE_SECONDS}
            return _envelope(cached, status=str(cached.get("envelope_status") or "ok"))

    authoritative_trade_date = _daily_result_authoritative_trade_date(
        getattr(repository, "engine", None),
        resolved_trade_date,
        date_resolution,
    )
    build_sha, _build_source = code_version()
    pool_started = monotonic()
    canonical = (
        canonical_governance_decision(
            resolved_trade_date,
            latest_as_of=False,
        )
        if resolved_trade_date is not None
        else None
    )
    pool = (
        dict(canonical.get("pool") or {})
        if isinstance(canonical, Mapping)
        else _daily_unavailable_stock_pool(resolved_trade_date)
    )
    execution_session_date: date | None = None
    execution_session_error: str | None = None
    if resolved_trade_date is not None:
        try:
            execution_session_date = _next_execution_session_date(
                getattr(repository, "engine", None),
                resolved_trade_date,
            )
        except Exception as exc:
            execution_session_error = type(exc).__name__
    pool.update({
        "decision_date": _iso_date(
            pool.get("trade_date") or pool.get("data_date")
        ),
        "decision_session_date": _iso_date(
            pool.get("trade_date") or pool.get("data_date")
        ),
        "execution_session_date": _iso_date(execution_session_date),
    })
    if strategy_governance_database_deferred():
        pool = _deferred_stock_pool_projection(pool)
    pool_ms = int((monotonic() - pool_started) * 1000)
    pool_context = _daily_context_from_pool(
        pool,
        requested_date=resolved_trade_date,
        expected_build_sha=(build_sha if canonical is not None else None),
        expected_execution_session_date=execution_session_date,
    )
    if date_resolution_error:
        pool_context["reason_codes"].append(
            "AUTHORITATIVE_CLOSED_TRADE_DATE_UNAVAILABLE"
        )
    if execution_session_error:
        pool_context["reason_codes"].append(
            "DAILY_RESULT_EXECUTION_SESSION_UNAVAILABLE"
        )
    context = pool_context

    if canonical is None and context.get("decision_integrity_verified") is not True:
        runtime = _analysis_runtime_context(
            getattr(repository, "engine", None),
            requested_date=resolved_trade_date,
        )
        if runtime is not None:
            runtime.pop("_envelope_status", None)
            context = runtime

    safety_started = monotonic()
    real_trading_safety = _daily_real_trading_safety(repository)
    safety_ms = int((monotonic() - safety_started) * 1000)
    overview = _daily_overview_from_pool(
        pool,
        context,
        real_trading_safety=real_trading_safety,
    )
    exact_pool = bool(
        pool_context.get("decision_integrity_verified") is True
    )
    strategy_pool = _daily_strategy_pool_projection(
        pool,
        exact_pool=exact_pool,
    )
    projected_stock_pool = _daily_stock_pool_projection(pool)
    same_run_uid = bool(
        exact_pool
        and context.get("run_uid")
        and context.get("run_uid") == pool.get("run_uid")
        and context.get("run_uid") == strategy_pool.get("run_uid")
    )
    scheduler_started = monotonic()
    scheduler = _daily_scheduler_health(
        getattr(repository, "engine", None),
        expected_build_sha=build_sha,
    )
    scheduler_ms = int((monotonic() - scheduler_started) * 1000)
    api_build_sha = str(build_sha or "").strip().lower()
    canonical_pool_build_sha = str(
        pool.get("build_commit_sha") or ""
    ).strip().lower()
    scheduler_roles = dict(scheduler.get("roles") or {})
    linux_scheduler_build_sha = str(
        (
            dict(scheduler_roles.get("linux_standalone") or {}).get("current")
            or {}
        ).get("build_sha")
        or ""
    ).strip().lower()
    qmt_scheduler_build_sha = str(
        (
            dict(scheduler_roles.get("qmt_windows_edge") or {}).get("current")
            or {}
        ).get("build_sha")
        or ""
    ).strip().lower()
    canonical_pool_build_matches_api = bool(
        _valid_daily_build_sha(canonical_pool_build_sha)
        and _valid_daily_build_sha(api_build_sha)
        and canonical_pool_build_sha == api_build_sha
    )
    both_schedulers_match_api = bool(
        _valid_daily_build_sha(api_build_sha)
        and _valid_daily_build_sha(linux_scheduler_build_sha)
        and _valid_daily_build_sha(qmt_scheduler_build_sha)
        and linux_scheduler_build_sha == api_build_sha
        and qmt_scheduler_build_sha == api_build_sha
    )
    build_reason_codes: list[str] = []
    if not _valid_daily_build_sha(api_build_sha):
        build_reason_codes.append("API_BUILD_INVALID")
    if not _valid_daily_build_sha(canonical_pool_build_sha):
        build_reason_codes.append("CANONICAL_POOL_BUILD_INVALID")
    elif canonical_pool_build_sha != api_build_sha:
        build_reason_codes.append("CANONICAL_POOL_BUILD_MISMATCH")
    if not _valid_daily_build_sha(linux_scheduler_build_sha):
        build_reason_codes.append("LINUX_SCHEDULER_BUILD_INVALID")
    elif linux_scheduler_build_sha != api_build_sha:
        build_reason_codes.append("LINUX_SCHEDULER_BUILD_MISMATCH")
    if not _valid_daily_build_sha(qmt_scheduler_build_sha):
        build_reason_codes.append("QMT_SCHEDULER_BUILD_INVALID")
    elif qmt_scheduler_build_sha != api_build_sha:
        build_reason_codes.append("QMT_SCHEDULER_BUILD_MISMATCH")
    build_identity = {
        "api_build_sha": api_build_sha or None,
        "canonical_pool_build_sha": canonical_pool_build_sha or None,
        "linux_scheduler_build_sha": linux_scheduler_build_sha or None,
        "qmt_scheduler_build_sha": qmt_scheduler_build_sha or None,
        "canonical_pool_build_matches_api": canonical_pool_build_matches_api,
        "both_schedulers_match_api": both_schedulers_match_api,
        "all_match": bool(
            canonical_pool_build_matches_api
            and both_schedulers_match_api
        ),
        "reason_codes": build_reason_codes,
    }
    qmt_release_receipt_verified = bool(
        dict(scheduler_roles.get("qmt_windows_edge") or {}).get(
            "immutable_reference_verified"
        )
        is True
        and dict(scheduler_roles.get("qmt_windows_edge") or {}).get(
            "healthy"
        )
        is True
    )

    data_status = str(context.get("data_status") or "UNAVAILABLE").upper()
    decision_status = str(
        context.get("decision_status") or "UNAVAILABLE"
    ).upper()
    if data_status == "LOADING" or decision_status == "LOADING":
        delivery_status = "LOADING"
        reason_code = "DAILY_PIPELINE_IN_PROGRESS"
        envelope_status = "loading"
    elif data_status == "DATA_BLOCKED" or decision_status == "BLOCKED":
        delivery_status = "DATA_BLOCKED"
        reason_code = str(
            (context.get("reason_codes") or ["DATA_BLOCKED"])[-1]
        )
        envelope_status = "blocked"
    elif canonical is not None and not canonical_pool_build_matches_api:
        delivery_status = "DATA_BLOCKED"
        reason_code = (
            "DAILY_RESULT_CANONICAL_BUILD_INVALID"
            if not _valid_daily_build_sha(canonical_pool_build_sha)
            else "DAILY_RESULT_CANONICAL_BUILD_MISMATCH"
        )
        envelope_status = "blocked"
    elif date_resolution_error:
        delivery_status = "DATA_BLOCKED"
        reason_code = "AUTHORITATIVE_CLOSED_TRADE_DATE_UNAVAILABLE"
        envelope_status = "blocked"
    elif execution_session_error:
        delivery_status = "DATA_BLOCKED"
        reason_code = "DAILY_RESULT_EXECUTION_SESSION_UNAVAILABLE"
        envelope_status = "blocked"
    elif not exact_pool:
        delivery_status = "UNAVAILABLE"
        if "DAILY_RESULT_CANONICAL_BUILD_INVALID" in list(
            pool_context.get("reason_codes") or []
        ):
            reason_code = "DAILY_RESULT_CANONICAL_BUILD_INVALID"
        elif "DAILY_RESULT_CANONICAL_BUILD_MISMATCH" in list(
            pool_context.get("reason_codes") or []
        ):
            reason_code = "DAILY_RESULT_CANONICAL_BUILD_MISMATCH"
        else:
            reason_code = "EXACT_CANONICAL_POOL_NOT_AVAILABLE"
        envelope_status = "unavailable"
    elif strategy_pool.get("pool_readable") is not True:
        delivery_status = "UNAVAILABLE"
        reason_code = "STRATEGY_POOL_NOT_READABLE"
        envelope_status = "unavailable"
    elif not same_run_uid:
        delivery_status = "UNAVAILABLE"
        reason_code = "DAILY_RESULT_RUN_IDENTITY_MISMATCH"
        envelope_status = "unavailable"
    elif build_identity["all_match"] is not True:
        delivery_status = "DATA_BLOCKED"
        reason_code = "DAILY_RESULT_RELEASE_BUILD_IDENTITY_MISMATCH"
        envelope_status = "blocked"
    elif real_trading_safety.get("verified") is not True:
        delivery_status = "DATA_BLOCKED"
        reason_code = str(
            (
                real_trading_safety.get("reason_codes")
                or ["REAL_TRADING_SAFETY_UNVERIFIED"]
            )[0]
        )
        envelope_status = "blocked"
    elif not qmt_release_receipt_verified:
        delivery_status = "DATA_BLOCKED"
        reason_code = "QMT_EDGE_RELEASE_RECEIPT_UNAVAILABLE"
        envelope_status = "blocked"
    elif scheduler.get("healthy") is not True:
        delivery_status = "DEGRADED"
        reason_code = "SCHEDULER_NOT_HEALTHY"
        envelope_status = "blocked"
    else:
        delivery_status = "COMPLETED"
        reason_code = "EXACT_DAILY_RESULT_VERIFIED"
        envelope_status = "ok"

    elapsed_ms = int((monotonic() - started) * 1000)
    payload = {
        "schema": "probiga.trading-v3.daily-result.v1",
        "delivery_status": delivery_status,
        "reason_code": reason_code,
        "requested_trade_date": (
            resolved_trade_date.isoformat()
            if resolved_trade_date
            else None
        ),
        "authoritative_closed_trade_date": (
            authoritative_trade_date.isoformat()
            if authoritative_trade_date
            else None
        ),
        "date_resolution": date_resolution,
        "decision_date": context.get("decision_date"),
        "decision_session_date": context.get("decision_session_date"),
        "data_trade_date": context.get("data_date"),
        "execution_session_date": context.get("execution_session_date"),
        "run_uid": context.get("run_uid"),
        "source_system": context.get("source_system"),
        "canonical_result_hash": context.get("canonical_result_hash"),
        "context": context,
        "overview": overview,
        "strategy_pool": strategy_pool,
        "stock_pool": projected_stock_pool,
        "scheduler": scheduler,
        "build_identity": build_identity,
        "real_trading_safety": real_trading_safety,
        "acceptance": {
            "same_run_uid": same_run_uid,
            "exact_trade_date": exact_pool,
            "canonical_completed": exact_pool,
            "strategy_pool_readable": (
                strategy_pool.get("pool_readable") is True
            ),
            "stock_pool_readable": exact_pool,
            "canonical_pool_build_matches_api": (
                canonical_pool_build_matches_api
            ),
            "both_schedulers_match_api": both_schedulers_match_api,
            "release_build_identity_matches": build_identity["all_match"] is True,
            "execution_session_mapped": bool(
                context.get("execution_session_date")
            ),
            "scheduler_healthy": scheduler.get("healthy") is True,
            "real_trading_off": (
                real_trading_safety.get("verified") is True
            ),
            "accepted": delivery_status == "COMPLETED",
        },
        "stage_timings_ms": {
            "stock_pool": pool_ms,
            "real_trading_safety": safety_ms,
            "scheduler": scheduler_ms,
            "total": elapsed_ms,
        },
        "cache": {"hit": False, "ttl_seconds": _DAILY_RESULT_CACHE_SECONDS},
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "envelope_status": envelope_status,
    }
    _daily_result_cache_set(cache_key, payload)
    return _envelope(payload, status=envelope_status)


@router.get("/premarket/auction-gate")
def premarket_auction_gate(
    execution_session_date: date | None = Query(default=None),
    trade_date: date | None = Query(default=None),
):
    """Re-rank decision-day T's pool with execution-day T+1 auction facts."""

    now = datetime.now(_SHANGHAI).replace(tzinfo=None, microsecond=0)
    execution_date_value = (
        execution_session_date
        if isinstance(execution_session_date, date)
        else None
    )
    legacy_trade_date = trade_date if isinstance(trade_date, date) else None
    if execution_date_value is not None and legacy_trade_date is not None:
        raise HTTPException(
            status_code=422,
            detail=(
                "execution_session_date and legacy trade_date are mutually "
                "exclusive"
            ),
        )
    session_date = execution_date_value or legacy_trade_date or now.date()
    repository = _repo()
    try:
        decision_date = _decision_date_for_execution_session(
            repository.engine,
            session_date,
        )
    except Exception as exc:
        return _envelope({
            "schema": "probiga.trading-v3.premarket-gate.v1",
            "status": "DATA_BLOCKED",
            "decision_date": None,
            "data_date": None,
            "execution_session_date": session_date.isoformat(),
            "session_date": session_date.isoformat(),
            "cutoff_at": None,
            "source_run_uid": None,
            "reason_code": "EXECUTION_SESSION_CALENDAR_INVALID",
            "reason": (
                "执行日不是可验证的交易日，或无法取得严格前一交易日："
                f"{type(exc).__name__}"
            ),
            "assessments": [],
            "summary": {"candidate_count": 0, "reviewed_count": 0},
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
            "automatic_substitution": False,
            "evidence_mode": "CALENDAR_BLOCKED",
        }, status="blocked")
    canonical = canonical_governance_decision(
        decision_date,
        latest_as_of=False,
    )
    pool = (
        dict(canonical.get("pool") or {})
        if isinstance(canonical, Mapping)
        else _daily_unavailable_stock_pool(decision_date)
    )
    identity = {
        "decision_date": decision_date.isoformat(),
        "data_date": _iso_date(pool.get("trade_date") or pool.get("data_date")),
        "execution_session_date": session_date.isoformat(),
        "session_date": session_date.isoformat(),
    }
    if canonical is None or pool.get("decision_integrity_verified") is not True:
        return _envelope({
            "schema": "probiga.trading-v3.premarket-gate.v1",
            "status": "DATA_BLOCKED",
            **identity,
            "cutoff_at": None,
            "source_run_uid": None,
            "reason_code": "EXACT_CANONICAL_DECISION_POOL_NOT_AVAILABLE",
            "reason": "执行日对应的严格前一交易日没有可验证 canonical 票池",
            "assessments": [],
            "summary": {"candidate_count": 0, "reviewed_count": 0},
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
            "automatic_substitution": False,
            "evidence_mode": "EXACT_CANONICAL_POOL_BLOCKED",
        }, status="blocked")
    pool.update(identity)
    persisted = dict(pool.get("premarket_gate") or {})
    if (
        persisted.get("status") in {"COMPLETED", "VALID_EMPTY"}
        and str(persisted.get("session_date") or "")
        == session_date.isoformat()
        and str(persisted.get("source_run_uid") or "")
        == str(pool.get("run_uid") or "")
    ):
        return _envelope({
            **persisted,
            **identity,
            "evidence_mode": "PERSISTED_IMMUTABLE_RUN",
        })

    candidate_count = sum(
        item.get("is_strategy_candidate") is True
        for item in list(pool.get("items") or [])
        if isinstance(item, Mapping)
    )
    if session_date > now.date():
        return _envelope({
            "schema": "probiga.trading-v3.premarket-gate.v1",
            "status": "WAITING_FOR_SESSION",
            **identity,
            "cutoff_at": None,
            "source_run_uid": pool.get("run_uid"),
            "reason": "目标交易日尚未到达，不能提前生成竞价结论",
            "assessments": [],
            "summary": {
                "candidate_count": candidate_count,
                "reviewed_count": 0,
            },
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
            "automatic_substitution": False,
        })
    if session_date == now.date() and now.time() < datetime.strptime(
        "09:15", "%H:%M"
    ).time():
        return _envelope({
            "schema": "probiga.trading-v3.premarket-gate.v1",
            "status": "WAITING_FOR_AUCTION",
            **identity,
            "cutoff_at": None,
            "source_run_uid": pool.get("run_uid"),
            "reason": "集合竞价尚未开始，当前只展示盘后策略池",
            "assessments": [],
            "summary": {
                "candidate_count": candidate_count,
                "reviewed_count": 0,
            },
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
            "automatic_substitution": False,
        })
    final_cutoff = datetime.combine(
        session_date,
        datetime.strptime("09:25:59", "%H:%M:%S").time(),
    )
    cutoff_at = (
        min(now, final_cutoff) if session_date == now.date()
        else final_cutoff
    )
    try:
        gate = build_premarket_gate(
            repository.engine,
            pool,
            session_date=session_date,
            cutoff_at=cutoff_at,
        )
    except SQLAlchemyError as exc:
        gate = {
            "schema": "probiga.trading-v3.premarket-gate.v1",
            "status": "UPSTREAM_UNAVAILABLE",
            **identity,
            "cutoff_at": cutoff_at.isoformat(sep=" "),
            "source_run_uid": pool.get("run_uid"),
            "reason": f"集合竞价行情账本不可用：{type(exc).__name__}",
            "assessments": [],
            "summary": {
                "candidate_count": candidate_count,
                "reviewed_count": 0,
            },
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
            "automatic_substitution": False,
        }
    return _envelope({
        **gate,
        **identity,
        "evidence_mode": "POINT_IN_TIME_REPLAY",
    })


@router.get("/hypotheses/latest")
def latest_hypotheses(
    limit: int = Query(default=300, ge=1, le=1000),
    trade_date: date | None = Query(default=None),
    scope_type: str = Query(
        default="",
        pattern=r"^(|MARKET|THEME|STOCK)$",
    ),
    state: str = Query(default="", max_length=32),
    q: str = Query(default="", max_length=64),
):
    rows = _repo().latest_hypotheses(
            limit=limit,
            trade_date=trade_date,
            scope_type=scope_type,
            state=state.strip(),
            query=q.strip(),
        )
    return _envelope(
        [_research_hypothesis_projection(row) for row in rows]
    )


@router.get("/hypotheses/{hypothesis_id}/timeline")
def hypothesis_timeline(
    hypothesis_id: str = ApiPath(
        ...,
        pattern=r"^[a-fA-F0-9]{32,64}$",
    ),
    limit: int = Query(default=500, ge=1, le=2000),
):
    result = _repo().hypothesis_timeline(
        hypothesis_id,
        limit=limit,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="trade hypothesis not found",
        )
    hypothesis = result.get("hypothesis")
    if isinstance(hypothesis, dict):
        result = {
            **result,
            "hypothesis": _research_hypothesis_projection(hypothesis),
        }
    return _envelope(result)


@router.get("/decision-runs")
def decision_runs(
    limit: int = Query(default=60, ge=1, le=500),
):
    return _envelope(_repo().decision_runs(limit=limit))


@router.get("/decision-runs/{run_uid}/diff")
def decision_run_diff(
    run_uid: str = ApiPath(
        ...,
        pattern=r"^[a-zA-Z0-9:_-]{8,64}$",
    ),
    previous_run_uid: str = Query(
        default="",
        pattern=r"^(|[a-zA-Z0-9:_-]{8,64})$",
    ),
):
    """Compare immutable batches; this endpoint never mutates execution."""

    repository = _repo()
    current = _load_run_batch(repository.engine, run_uid)
    previous_uid = previous_run_uid.strip()
    if not previous_uid:
        with repository.engine.connect() as connection:
            previous = connection.execute(
                text(
                    """
                    SELECT run_uid
                    FROM st_decision_run_v3
                    WHERE status = 'COMPLETED'
                      AND (
                          COALESCE(requested_as_of, DATE(decision_at)),
                          decision_at,
                          run_uid
                      ) < (
                        SELECT COALESCE(
                                   requested_as_of, DATE(decision_at)
                               ),
                               decision_at,
                               run_uid
                        FROM st_decision_run_v3
                        WHERE run_uid = :run_uid
                      )
                    ORDER BY COALESCE(
                                 requested_as_of, DATE(decision_at)
                             ) DESC,
                             decision_at DESC, run_uid DESC
                    LIMIT 1
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().first()
        previous_uid = str((previous or {}).get("run_uid") or "")
    if not previous_uid:
        return _envelope(
            {
                "status": "NO_PREVIOUS_BATCH",
                "current_run_uid": run_uid,
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            status="empty",
        )
    try:
        result = diff_run_batches(
            _load_run_batch(repository.engine, previous_uid),
            current,
        )
    except DecisionIntelligenceError as exc:
        raise _research_error(exc) from exc
    return _envelope(result)


@router.get("/decision-runs/{run_uid}/lineage")
def decision_lineage(
    run_uid: str = ApiPath(
        ...,
        pattern=r"^[a-zA-Z0-9:_-]{8,64}$",
    ),
):
    """Return the exact V3 target -> V2 intent/order/fill/lot chain."""

    governance = canonical_governance_decision_for_run(run_uid)
    if governance is not None:
        return _envelope(governance["lineage"])

    repository = _repo()
    try:
        with repository.engine.connect() as connection:
            run = connection.execute(
                text(
                    """
                    SELECT run_uid, trade_date, decision_at, status,
                           dominant_regime, target_count, result_hash
                    FROM st_decision_run_v3
                    WHERE run_uid = :run_uid
                    LIMIT 1
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().first()
            if not run:
                raise HTTPException(
                    status_code=404,
                    detail="V3 decision run not found",
                )
            targets = connection.execute(
                text(
                    """
                    SELECT run_uid, rank_no, stock_code, short_name,
                           target_weight, target_value, target_quantity,
                           primary_strategy_key, strategy_keys_json,
                           theme_codes_json, reason
                    FROM st_target_portfolio_v3
                    WHERE run_uid = :run_uid
                    ORDER BY rank_no, stock_code
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
            intents = connection.execute(
                text(
                    """
                    SELECT i.*, r.decision_status,
                           r.requested_quantity, r.approved_quantity,
                           r.trade_risk, r.post_single_weight,
                           r.post_total_weight, r.post_theme_weight,
                           r.post_open_risk, r.post_cash,
                           r.checks_json, r.first_failure,
                           r.decision_hash AS risk_decision_hash
                    FROM st_trade_intent_v2 i
                    LEFT JOIN st_risk_decision_v2 r
                      ON r.intent_id = i.intent_id
                    WHERE i.decision_run_uid = :run_uid
                    ORDER BY i.created_at, i.intent_id
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
            orders = connection.execute(
                text(
                    """
                    SELECT o.*
                    FROM st_order_v2 o
                    JOIN st_trade_intent_v2 i
                      ON i.intent_id = o.intent_id
                    WHERE i.decision_run_uid = :run_uid
                    ORDER BY o.created_at, o.order_id
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
            fills = connection.execute(
                text(
                    """
                    SELECT f.*, o.intent_id
                    FROM st_fill_v2 f
                    JOIN st_order_v2 o ON o.order_id = f.order_id
                    JOIN st_trade_intent_v2 i
                      ON i.intent_id = o.intent_id
                    WHERE i.decision_run_uid = :run_uid
                    ORDER BY f.filled_at, f.fill_id
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
            lots = connection.execute(
                text(
                    """
                    SELECT l.*
                    FROM st_position_lot_v2 l
                    JOIN st_fill_v2 f
                      ON f.fill_id = l.opened_fill_id
                    JOIN st_order_v2 o ON o.order_id = f.order_id
                    JOIN st_trade_intent_v2 i
                      ON i.intent_id = o.intent_id
                    WHERE i.decision_run_uid = :run_uid
                    ORDER BY l.created_at, l.lot_id
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
            fill_events = connection.execute(
                text(
                    """
                    SELECT e.entity_id AS fill_id, e.event_payload_json,
                           e.payload_hash, e.occurred_at
                    FROM st_trade_event_v2 e
                    JOIN st_fill_v2 f ON f.fill_id = e.entity_id
                    JOIN st_order_v2 o ON o.order_id = f.order_id
                    JOIN st_trade_intent_v2 i
                      ON i.intent_id = o.intent_id
                    WHERE i.decision_run_uid = :run_uid
                      AND e.event_type = 'PAPER_FILL_APPLIED'
                      AND e.entity_type = 'FILL'
                    ORDER BY e.occurred_at, e.event_id
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"decision lineage unavailable: {str(exc)[:300]}",
        ) from exc

    def _rows(values: Any) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for value in values:
            item = dict(value)
            for key in (
                "strategy_keys_json",
                "theme_codes_json",
                "checks_json",
                "evidence_json",
            ):
                if key not in item:
                    continue
                raw = item.pop(key)
                try:
                    default_json = (
                        "[]"
                        if key in {
                            "strategy_keys_json",
                            "theme_codes_json",
                        }
                        else "{}"
                    )
                    item[key.removesuffix("_json")] = json.loads(
                        str(raw or default_json)
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    item[key.removesuffix("_json")] = None
            result.append(item)
        return result

    intent_rows = _rows(intents)
    order_rows = _rows(orders)
    fill_rows = _rows(fills)
    lot_rows = _rows(lots)
    lot_close_allocations: list[dict[str, Any]] = []
    fill_event_evidence: list[dict[str, Any]] = []
    sell_fill_ids = {
        str(item.get("fill_id") or "")
        for item in fill_rows
        if str(item.get("side") or "").upper() == "SELL"
    }
    invalid_sell_fill_ids: set[str] = set()
    lot_close_integrity_reasons: list[str] = []
    observed_lots_by_fill: set[tuple[str, str]] = set()
    observed_sell_event_fill_ids: set[str] = set()
    for raw in fill_events:
        event = dict(raw)
        fill_id = str(event.get("fill_id") or "")
        raw_payload = event.pop("event_payload_json", None)
        event_reasons: list[str] = []
        if fill_id in sell_fill_ids:
            if fill_id in observed_sell_event_fill_ids:
                event_reasons.append("LOT_CLOSE_EVENT_DUPLICATE_FOR_FILL")
            observed_sell_event_fill_ids.add(fill_id)
        payload: dict[str, Any] | None = None
        if isinstance(raw_payload, dict):
            payload = dict(raw_payload)
        elif isinstance(raw_payload, (str, bytes, bytearray)):
            try:
                decoded = json.loads(raw_payload)
                if isinstance(decoded, dict):
                    payload = decoded
                else:
                    event_reasons.append("EVENT_PAYLOAD_NOT_OBJECT")
            except (TypeError, ValueError, json.JSONDecodeError):
                event_reasons.append("EVENT_PAYLOAD_JSON_INVALID")
        else:
            event_reasons.append("EVENT_PAYLOAD_JSON_INVALID")

        computed_payload_hash = ""
        if payload is not None:
            try:
                computed_payload_hash = _canonical_payload_hash(payload)
            except (TypeError, ValueError, OverflowError):
                event_reasons.append("EVENT_PAYLOAD_CANONICALIZATION_INVALID")
            if computed_payload_hash != str(event.get("payload_hash") or ""):
                event_reasons.append("EVENT_PAYLOAD_HASH_MISMATCH")

        allocations = (
            payload.get("lot_close_allocations")
            if payload is not None
            else None
        )
        normalized_allocations: list[dict[str, Any]] = []
        if not isinstance(allocations, list):
            if fill_id in sell_fill_ids:
                event_reasons.append("LOT_CLOSE_ALLOCATIONS_INVALID")
        else:
            event_lot_ids: set[str] = set()
            for item in allocations:
                if not isinstance(item, dict):
                    event_reasons.append("LOT_CLOSE_ALLOCATION_INVALID")
                    continue
                allocation = dict(item)
                lot_id = str(allocation.get("lot_id") or "").strip()
                consumed = allocation.get("consumed_quantity")
                if not lot_id:
                    event_reasons.append("LOT_CLOSE_ALLOCATION_LOT_ID_MISSING")
                if type(consumed) is not int or consumed <= 0:
                    event_reasons.append(
                        "LOT_CLOSE_ALLOCATION_QUANTITY_NOT_POSITIVE"
                    )
                lot_key = (fill_id, lot_id)
                if lot_id and (
                    lot_id in event_lot_ids or lot_key in observed_lots_by_fill
                ):
                    event_reasons.append("LOT_CLOSE_ALLOCATION_DUPLICATE_LOT")
                event_lot_ids.add(lot_id)
                if lot_id:
                    observed_lots_by_fill.add(lot_key)
                normalized_allocations.append(allocation)

        event_reasons = list(dict.fromkeys(event_reasons))
        event_verified = not event_reasons
        if fill_id in sell_fill_ids and not event_verified:
            invalid_sell_fill_ids.add(fill_id)
            lot_close_integrity_reasons.extend(event_reasons)
        if fill_id in sell_fill_ids and event_verified:
            for allocation in normalized_allocations:
                lot_close_allocations.append(
                    {
                        **allocation,
                        "fill_id": fill_id,
                        "event_payload_hash": str(
                            event.get("payload_hash") or ""
                        ),
                        "occurred_at": event.get("occurred_at"),
                    }
                )
        fill_event_evidence.append(
            {
                **event,
                "computed_payload_hash": computed_payload_hash or None,
                "payload_hash_verified": bool(
                    computed_payload_hash
                    and computed_payload_hash
                    == str(event.get("payload_hash") or "")
                ),
                "evidence_verified": event_verified,
                "reason_codes": event_reasons,
                "lot_close_allocations": normalized_allocations,
            }
        )
    sell_fill_quantities = {
        str(item.get("fill_id") or ""): int(item.get("quantity") or 0)
        for item in fill_rows
        if str(item.get("side") or "").upper() == "SELL"
    }
    allocated_by_fill: dict[str, int] = {}
    for item in lot_close_allocations:
        fill_id = str(item.get("fill_id") or "")
        allocated_by_fill[fill_id] = allocated_by_fill.get(fill_id, 0) + int(
            item.get("consumed_quantity") or 0
        )
    quantity_mismatch_fill_ids = {
        fill_id
        for fill_id, quantity in sell_fill_quantities.items()
        if allocated_by_fill.get(fill_id, 0) != quantity
    }
    if quantity_mismatch_fill_ids:
        lot_close_integrity_reasons.append(
            "LOT_CLOSE_ALLOCATION_QUANTITY_MISMATCH"
        )
    incomplete_sell_fill_ids = sorted(
        fill_id
        for fill_id in sell_fill_quantities
        if (
            fill_id in invalid_sell_fill_ids
            or fill_id in quantity_mismatch_fill_ids
        )
    )
    lot_close_evidence_status = (
        "NO_SELL_FILL"
        if not sell_fill_quantities
        else "INCOMPLETE"
        if incomplete_sell_fill_ids
        else "COMPLETE"
    )
    return _envelope(
        {
            "run": dict(run),
            "targets": _rows(targets),
            "intents": intent_rows,
            "orders": order_rows,
            "fills": fill_rows,
            "lots": lot_rows,
            "fill_event_evidence": fill_event_evidence,
            "lot_close_allocations": lot_close_allocations,
            "lot_close_evidence": {
                "status": lot_close_evidence_status,
                "complete": (
                    lot_close_evidence_status == "COMPLETE"
                    if sell_fill_quantities
                    else None
                ),
                "sell_fill_count": len(sell_fill_quantities),
                "allocation_count": len(lot_close_allocations),
                "incomplete_sell_fill_ids": incomplete_sell_fill_ids,
                "invalid_sell_fill_ids": sorted(invalid_sell_fill_ids),
                "reason_codes": list(dict.fromkeys(
                    lot_close_integrity_reasons
                )),
            },
            "summary": {
                "target_count": len(targets),
                "intent_count": len(intent_rows),
                "exit_intent_count": sum(
                    str(item.get("action") or "").upper()
                    in {"SELL", "REDUCE", "EXIT"}
                    for item in intent_rows
                ),
                "approved_intent_count": sum(
                    str(item.get("decision_status") or "").upper()
                    == "APPROVED"
                    for item in intent_rows
                ),
                "order_count": len(order_rows),
                "fill_count": len(fill_rows),
                "lot_close_allocation_count": len(lot_close_allocations),
                "lot_close_evidence_status": lot_close_evidence_status,
                "open_lot_count": sum(
                    int(item.get("remaining_quantity") or 0) > 0
                    for item in lot_rows
                ),
            },
        }
    )


@router.get("/research/decision-intelligence/latest")
def latest_decision_intelligence_runtime(
    run_uid: str = Query(
        default="",
        pattern=r"^(|[a-zA-Z0-9:_-]{8,64})$",
    ),
):
    """Run advisory optimization from one verified persisted snapshot."""

    repository = _repo()
    try:
        result = _server_decision_intelligence_snapshot(
            repository.engine,
            run_uid=run_uid.strip() or None,
        )
    except DecisionIntelligenceError as exc:
        return _envelope(
            {
                "status": "UNAVAILABLE",
                "reason_codes": ["DECISION_INTELLIGENCE_INPUT_UNAVAILABLE"],
                "detail": str(exc)[:300],
                "replacement_analysis": {
                    "status": "UNAVAILABLE",
                    "options": [],
                    "order_authority": False,
                },
                "portfolio_optimization": {
                    "status": "UNAVAILABLE",
                    "targets": [],
                    "order_authority": False,
                },
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            status="unavailable",
        )
    except Exception as exc:
        return _envelope(
            {
                "status": "UNAVAILABLE",
                "reason_codes": ["DECISION_INTELLIGENCE_RUNTIME_ERROR"],
                "detail": str(exc)[:300],
                "replacement_analysis": {
                    "status": "UNAVAILABLE",
                    "options": [],
                    "order_authority": False,
                },
                "portfolio_optimization": {
                    "status": "UNAVAILABLE",
                    "targets": [],
                    "order_authority": False,
                },
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            status="unavailable",
        )
    return _envelope(result)


@router.post("/research/replacement-analysis")
def replacement_analysis(
    payload: dict[str, Any] = Body(...),
):
    """Preview cost/capacity/T+1-aware substitutions without orders."""

    try:
        request = _research_object(payload, "payload")
        policy = _research_object(request.get("policy"), "policy")
        result = analyze_replacement_opportunities(
            _research_object_list(request.get("candidates"), "candidates"),
            _research_object_list(request.get("holdings"), "holdings"),
            equity_cny=policy.get("equity_cny"),
            maximum_participation_rate=policy.get(
                "maximum_participation_rate"
            ),
            capacity_sessions=policy.get("capacity_sessions"),
            maximum_theme_weight=policy.get("maximum_theme_weight"),
            minimum_incremental_net_edge_pct=policy.get(
                "minimum_incremental_net_edge_pct"
            ),
        )
    except (
        DecisionIntelligenceError,
        ArithmeticError,
        TypeError,
        ValueError,
    ) as exc:
        raise _research_error(exc) from exc
    response = _envelope(
        {
            **result,
            "status": "UNVERIFIED_PREVIEW",
            "persisted": False,
            "persisted_evidence_verified": False,
            "reason_codes": ["CLIENT_SUPPLIED_INPUT_UNVERIFIED"],
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        },
        status="UNVERIFIED_PREVIEW",
    )
    response.update({"persisted": False, "order_authority": False})
    return response


@router.post("/research/portfolio-optimization")
def portfolio_optimization(
    payload: dict[str, Any] = Body(...),
):
    """Return a deterministic advisory portfolio; V2 must revalidate it."""

    try:
        request = _research_object(payload, "payload")
        result = optimize_advisory_portfolio(
            _research_object_list(request.get("candidates"), "candidates"),
            policy=_research_object(request.get("policy"), "policy"),
            current_positions=_research_object_list(
                request.get("current_positions"),
                "current_positions",
            ),
        )
    except (
        DecisionIntelligenceError,
        ArithmeticError,
        TypeError,
        ValueError,
    ) as exc:
        raise _research_error(exc) from exc
    response = _envelope(
        {
            **result,
            "status": "UNVERIFIED_PREVIEW",
            "persisted": False,
            "persisted_evidence_verified": False,
            "reason_codes": ["CLIENT_SUPPLIED_INPUT_UNVERIFIED"],
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        },
        status="UNVERIFIED_PREVIEW",
    )
    response.update({"persisted": False, "order_authority": False})
    return response


@router.post("/research/horizons/validate")
def validate_horizon_contracts(
    payload: dict[str, Any] = Body(...),
):
    """Validate genuinely independent T+1/T+5/T+20 contracts."""

    try:
        contracts = [
            _horizon_contract(dict(item))
            for item in (payload.get("forecasts") or [])
        ]
        result = validate_independent_horizon_suite(contracts)
    except (HorizonContractError, TypeError, ValueError) as exc:
        raise _research_error(exc) from exc
    return _envelope(
        {
            **result,
            "diagnostic_status": result.get("status"),
            "status": "UNVERIFIED_PREVIEW",
            "persisted_evidence_verified": False,
            "reason_codes": ["PERSISTED_CONTRACT_LEDGER_REQUIRED"],
            "order_authority": False,
        },
        status="preview",
    )


@router.post("/research/counterfactual-learning")
def counterfactual_learning(
    payload: dict[str, Any] = Body(...),
):
    """Build four-quadrant samples and metrics without auto-activation."""

    try:
        contracts = [
            _horizon_contract(dict(item))
            for item in (payload.get("forecasts") or [])
        ]
        samples = build_counterfactual_samples(
            contracts,
            payload.get("selections") or {},
            payload.get("outcomes") or {},
            evaluation_date=payload.get("evaluation_date"),
            winner_threshold_net_pct=payload.get(
                "winner_threshold_net_pct",
                0.0,
            ),
        )
        calibration_policy = dict(
            load_v3_config().get("continuous_calibration") or {}
        )
        thresholds = dict(
            calibration_policy.get("minimum_mature_samples") or {}
        )
        metrics = counterfactual_learning_metrics(
            samples.get("samples") or [],
            minimum_mature_samples=sum(
                int(value) for value in thresholds.values()
            ),
            minimum_mature_samples_by_horizon=thresholds,
        )
    except (
        HorizonContractError,
        LearningIntelligenceError,
        TypeError,
        ValueError,
    ) as exc:
        raise _research_error(exc) from exc
    return _envelope(
        {
            "status": "UNVERIFIED_PREVIEW",
            "ledger": {
                **samples,
                "diagnostic_status": samples.get("status"),
                "status": "UNVERIFIED_PREVIEW",
            },
            "metrics": {
                **metrics,
                "diagnostic_status": metrics.get("status"),
                "status": "UNVERIFIED_PREVIEW",
            },
            "persisted_evidence_verified": False,
            "reason_codes": ["PERSISTED_OUTCOME_LEDGER_REQUIRED"],
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        },
        status="preview",
    )


@router.post("/research/shadow/calibration-gate")
def shadow_calibration_gate(
    payload: dict[str, Any] = Body(...),
):
    """Preview raw metrics without manufacturing a persisted PASS gate."""

    try:
        if payload.get("policy") is not None:
            raise ReleaseGovernanceError(
                "client calibration policy is not accepted; the frozen "
                "trading_v3 configuration is authoritative"
            )
        evidence = ContinuousCalibrationEvidence(
            **dict(payload.get("evidence") or {})
        )
        decision = evaluate_continuous_calibration(
            evidence,
            policy=dict(
                load_v3_config().get("continuous_calibration") or {}
            ),
            evaluated_at=payload.get("evaluated_at"),
        )
    except (ReleaseGovernanceError, TypeError, ValueError) as exc:
        raise _research_error(exc) from exc
    preview = decision.as_dict()
    preview.update({
        "provisional_status": preview.get("status"),
        "provisional_passed": preview.get("status") == "PASS",
        "status": "UNVERIFIED_PREVIEW",
        "passed": False,
        "recommended_stage": "CALIBRATION_REVIEW",
        "evidence_provenance_status": "UNVERIFIED_PREVIEW",
        "external_execution_grant_required": False,
        "failure_codes": list(preview.get("failure_codes") or [])
        + ["PERSISTED_EVIDENCE_REQUIRED"],
    })
    return _envelope(preview, status="preview")


@router.get("/research/governance")
def research_governance():
    """Expose the configured research and Shadow gates without activating them."""

    config = load_v3_config()
    return _envelope(
        {
            "strategy_version": config.get("strategy_version"),
            "decision_scope": "RESEARCH_ONLY",
            "release_mode": "SHADOW_RESEARCH_ONLY",
            "order_authority": False,
            "real_trading_enabled": False,
            "decision_intelligence": config.get("decision_intelligence") or {},
            "multi_horizon_forecasts": config.get("multi_horizon_forecasts")
            or {},
            "shadow_release": config.get("shadow_release") or {},
            "continuous_calibration": config.get("continuous_calibration")
            or {},
        }
    )


def _artifact_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} unavailable")
    result = value.strip()
    if not result or result != value:
        raise ValueError(f"{field} unavailable")
    return result


def _artifact_number(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} unavailable") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field} unavailable")
    return result


def _artifact_digest(value: Any, field: str) -> str:
    result = _artifact_text(value, field)
    if len(result) != 64 or any(
        character not in "0123456789abcdef" for character in result
    ):
        raise ValueError(f"{field} unavailable")
    return result


def _artifact_count(value: Any, field: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} unavailable") from exc
    if result < 0:
        raise ValueError(f"{field} unavailable")
    return result


def _unavailable_horizon_artifact_projection(
    row: Mapping[str, Any],
    artifact: Mapping[str, Any],
    *,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "artifact_id": row.get("artifact_id"),
        "artifact_schema": artifact.get("schema_version"),
        "release_id": artifact.get("release_id"),
        "suite_release_id": artifact.get("suite_release_id"),
        "model_key": artifact.get("model_key"),
        "model_version": artifact.get("model_version"),
        "horizon_days": artifact.get("horizon_days"),
        "artifact_hash": artifact.get("artifact_hash"),
        "artifact_status": "UNAVAILABLE",
        "evidence_status": "UNAVAILABLE",
        "gate_status": "UNAVAILABLE",
        "eligibility_boundary": {
            "contract_eligibility_scope": "UNAVAILABLE",
            "contract_eligible": False,
            "paper_eligible": False,
            "production_eligible": False,
            "evidence_status": "UNAVAILABLE",
        },
        "candidate_ledger": {
            "schema_version": "UNAVAILABLE",
            "registration_verified": False,
            "evidence_status": "UNAVAILABLE",
        },
        "candidate_ledger_schema_version": None,
        "candidate_ledger_content_sha256": None,
        "candidate_ledger_row_count": None,
        "ledger_registration_evidence_hash": None,
        "registration_verification_hash": None,
        "registration_verified": False,
        "reason_codes": [reason_code],
        "current_runtime_eligible": False,
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
    }


def _horizon_artifact_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    artifact = dict(row.get("artifact") or {})
    schema = str(artifact.get("schema_version") or "")
    if schema == HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1:
        if (
            row.get("protocol_status") != "HISTORICAL_AUDIT_ONLY"
            or row.get("runtime_eligible") is not False
        ):
            return _unavailable_horizon_artifact_projection(
                row,
                artifact,
                reason_code="V1_AUDIT_BOUNDARY_INVALID",
            )
        return {
            "artifact_id": row.get("artifact_id"),
            "artifact_schema": schema,
            "release_id": artifact.get("release_id"),
            "suite_release_id": artifact.get("suite_release_id"),
            "model_key": artifact.get("model_key"),
            "model_version": artifact.get("model_version"),
            "horizon_days": artifact.get("horizon_days"),
            "artifact_hash": artifact.get("artifact_hash"),
            "artifact_status": "HISTORICAL_AUDIT_ONLY",
            "evidence_status": "HISTORICAL_AUDIT_ONLY",
            "gate_status": "HISTORICAL_AUDIT_ONLY",
            "eligibility_boundary": {
                "contract_eligibility_scope": "HISTORICAL_AUDIT_ONLY",
                "contract_eligible": False,
                "paper_eligible": False,
                "production_eligible": False,
                "evidence_status": "HISTORICAL_AUDIT_ONLY",
            },
            "protocols": {
                "artifact": schema,
                "suite": HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
                "model": artifact.get("model_protocol") or "V1_LEGACY",
                "selection": "UNAVAILABLE_IN_V1",
                "calibration": "UNAVAILABLE_IN_V1",
            },
            "candidate_ledger": {
                "schema_version": "UNAVAILABLE_IN_V1",
                "registration_verified": False,
                "evidence_status": "HISTORICAL_AUDIT_ONLY",
            },
            "candidate_ledger_schema_version": None,
            "candidate_ledger_content_sha256": None,
            "candidate_ledger_row_count": None,
            "ledger_registration_evidence_hash": None,
            "registration_verification_hash": None,
            "registration_verified": False,
            "reason_codes": ["LEGACY_V1_HISTORICAL_AUDIT_ONLY"],
            "current_runtime_eligible": False,
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
    if schema == HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2:
        if (
            row.get("protocol_status") != "PRE_LEDGER_V2_AUDIT_ONLY"
            or row.get("runtime_eligible") is not False
        ):
            return _unavailable_horizon_artifact_projection(
                row,
                artifact,
                reason_code="V2_PRE_LEDGER_AUDIT_BOUNDARY_INVALID",
            )
        return {
            "artifact_id": row.get("artifact_id"),
            "artifact_schema": schema,
            "release_id": artifact.get("release_id"),
            "suite_release_id": artifact.get("suite_release_id"),
            "model_key": artifact.get("model_key"),
            "model_version": artifact.get("model_version"),
            "horizon_days": artifact.get("horizon_days"),
            "artifact_hash": artifact.get("artifact_hash"),
            "artifact_status": "PRE_LEDGER_V2_AUDIT_ONLY",
            "evidence_status": "PRE_LEDGER_V2_AUDIT_ONLY",
            "gate_status": "PRE_LEDGER_V2_AUDIT_ONLY",
            "eligibility_boundary": {
                "contract_eligibility_scope": "PRE_LEDGER_V2_AUDIT_ONLY",
                "contract_eligible": False,
                "paper_eligible": False,
                "production_eligible": False,
                "evidence_status": "PRE_LEDGER_V2_AUDIT_ONLY",
            },
            "protocols": {
                "artifact": schema,
                "suite": HISTORICAL_HORIZON_SUITE_SCHEMA_V2,
                "model": artifact.get("model_protocol") or "V2_LEGACY",
                "selection": "PRE_LEDGER_V2_AUDIT_ONLY",
                "calibration": "PRE_LEDGER_V2_AUDIT_ONLY",
            },
            "candidate_ledger": {
                "schema_version": "UNAVAILABLE_IN_PRE_LEDGER_V2",
                "registration_verified": False,
                "evidence_status": "PRE_LEDGER_V2_AUDIT_ONLY",
            },
            "candidate_ledger_schema_version": None,
            "candidate_ledger_content_sha256": None,
            "candidate_ledger_row_count": None,
            "ledger_registration_evidence_hash": None,
            "registration_verification_hash": None,
            "registration_verified": False,
            "reason_codes": ["PRE_LEDGER_V2_AUDIT_ONLY"],
            "current_runtime_eligible": False,
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
    if schema != HORIZON_ARTIFACT_SCHEMA:
        return _unavailable_horizon_artifact_projection(
            row,
            artifact,
            reason_code="ARTIFACT_PROTOCOL_UNSUPPORTED_OR_MISSING",
        )
    try:
        gate = dict(artifact["gate"])
        evidence = dict(artifact["oos_evidence"])
        selection_policy = dict(artifact["selection_policy"])
        selection = dict(evidence["selection_evidence"])
        direction = dict(evidence["direction_evidence"])
        calibration = dict(artifact["calibration"])
        model_spec = dict(artifact["model_spec"])
        features = list(model_spec["features"])
        candidate_ledger_reference = dict(
            artifact["candidate_evaluation_ledger"]
        )
        candidate_ledger_schema = _artifact_text(
            candidate_ledger_reference.get("schema_version"),
            "candidate_evaluation_ledger.schema_version",
        )
        candidate_ledger_content_sha256 = _artifact_digest(
            candidate_ledger_reference.get("content_sha256"),
            "candidate_evaluation_ledger.content_sha256",
        )
        candidate_ledger_canonical_records_sha256 = _artifact_digest(
            candidate_ledger_reference.get("canonical_records_sha256"),
            "candidate_evaluation_ledger.canonical_records_sha256",
        )
        candidate_ledger_reference_hash = _artifact_digest(
            candidate_ledger_reference.get("reference_hash"),
            "candidate_evaluation_ledger.reference_hash",
        )
        candidate_ledger_row_count = _artifact_count(
            candidate_ledger_reference.get("row_count"),
            "candidate_evaluation_ledger.row_count",
        )
        candidate_ledger_session_count = _artifact_count(
            candidate_ledger_reference.get("session_count"),
            "candidate_evaluation_ledger.session_count",
        )
        candidate_ledger_evaluation_row_count = _artifact_count(
            candidate_ledger_reference.get("evaluation_row_count"),
            "candidate_evaluation_ledger.evaluation_row_count",
        )
        candidate_ledger_evaluation_session_count = _artifact_count(
            candidate_ledger_reference.get("evaluation_session_count"),
            "candidate_evaluation_ledger.evaluation_session_count",
        )
        candidate_ledger_fold_count = _artifact_count(
            candidate_ledger_reference.get("fold_count"),
            "candidate_evaluation_ledger.fold_count",
        )
        ledger_registration_evidence_hash = _artifact_digest(
            row.get("ledger_registration_evidence_hash"),
            "ledger_registration_evidence_hash",
        )
        registration_verification_hash = _artifact_digest(
            row.get("registration_verification_hash"),
            "registration_verification_hash",
        )
        artifact_registration_evidence_hash = _artifact_digest(
            row.get("registration_evidence_hash"),
            "registration_evidence_hash",
        )
        if (
            row.get("protocol_status") != "CURRENT_V3_LEDGER_VERIFIED"
            or artifact.get("order_authority") is not False
            or gate.get("order_authority") is not False
            or selection_policy.get("order_authority") is not False
            or selection.get("order_authority") is not False
            or artifact.get("prediction_kind") != "CALIBRATED_OOS"
            or artifact.get("model_protocol") != HORIZON_MODEL_PROTOCOL
            or selection_policy.get("protocol")
            != HORIZON_SELECTION_PROTOCOL
            or selection.get("protocol") != HORIZON_SELECTION_PROTOCOL
            or calibration.get("protocol")
            != HORIZON_CALIBRATION_PROTOCOL
            or selection.get("economic_evaluation_scope")
            != selection_policy.get("candidate_domain")
            or gate.get("gate_scope")
            != selection_policy.get("candidate_domain")
            or gate.get("deployment_gate") is not False
            or artifact.get("contract_eligibility_scope")
            != HORIZON_CONTRACT_ELIGIBILITY_SCOPE
            or gate.get("contract_eligibility_scope")
            != HORIZON_CONTRACT_ELIGIBILITY_SCOPE
            or artifact.get("paper_eligible") is not False
            or gate.get("paper_eligible") is not False
            or artifact.get("production_eligible") is not False
            or gate.get("production_eligible") is not False
            or selection.get("deployment_candidate_domain_verified")
            is not False
            or evidence.get("calibration_is_oos_only") is not True
            or evidence.get("calibration_evaluation_is_prequential")
            is not True
            or evidence.get("calibration_labels_purged_by_maturity")
            is not True
            or evidence.get("economic_metrics_use_frozen_selection_ledger")
            is not True
            or artifact.get("candidate_ledger_registration_required")
            is not True
            or candidate_ledger_schema != HORIZON_CANDIDATE_LEDGER_SCHEMA
            or candidate_ledger_reference.get("binding_protocol")
            != HORIZON_CANDIDATE_LEDGER_BINDING_PROTOCOL
            or candidate_ledger_reference.get("encoding")
            != HORIZON_CANDIDATE_LEDGER_ENCODING
            or candidate_ledger_reference.get(
                "registration_verification_required"
            ) is not True
            or str(row.get("candidate_ledger_schema_version") or "")
            != candidate_ledger_schema
            or _artifact_digest(
                row.get("candidate_ledger_content_sha256"),
                "candidate_ledger_content_sha256",
            )
            != candidate_ledger_content_sha256
            or _artifact_count(
                row.get("candidate_ledger_row_count"),
                "candidate_ledger_row_count",
            )
            != candidate_ledger_row_count
            or candidate_ledger_row_count <= 0
            or candidate_ledger_session_count <= 0
            or str(row.get("training_receipt_status") or "")
            != "PROCESS_VERIFIED"
            or selection.get("candidate_ledger_schema")
            != candidate_ledger_schema
            or selection.get("candidate_ledger_content_sha256")
            != candidate_ledger_content_sha256
            or selection.get("candidate_ledger_canonical_records_sha256")
            != candidate_ledger_canonical_records_sha256
            or selection.get("candidate_ledger_reference_hash")
            != candidate_ledger_reference_hash
            or evidence.get("candidate_evaluation_ledger_reference_hash")
            != candidate_ledger_reference_hash
            or candidate_ledger_row_count
            != _artifact_count(
                evidence.get("oos_sample_count"), "oos_sample_count"
            )
            or candidate_ledger_session_count
            != _artifact_count(
                evidence.get("distinct_oos_sessions"),
                "distinct_oos_sessions",
            )
            or candidate_ledger_evaluation_row_count
            != _artifact_count(
                evidence.get("calibration_evaluation_sample_count"),
                "calibration_evaluation_sample_count",
            )
            or candidate_ledger_evaluation_session_count
            != _artifact_count(
                evidence.get("distinct_calibration_evaluation_sessions"),
                "distinct_calibration_evaluation_sessions",
            )
            or candidate_ledger_fold_count
            != _artifact_count(
                evidence.get("walk_forward_fold_count"),
                "walk_forward_fold_count",
            )
            or not features
            or any(not isinstance(item, str) or not item for item in features)
        ):
            raise ValueError("V3 candidate ledger protocol evidence differs")
        source_gate_status = str(gate.get("status") or "")
        registry_status = str(row.get("artifact_status") or "")
        if source_gate_status == "PASS" and registry_status == "OOS_VERIFIED":
            if (
                artifact.get("contract_eligible") is not True
                or gate.get("contract_eligible") is not True
            ):
                raise ValueError("Shadow contract eligibility differs")
            if row.get("runtime_eligible") is not True:
                raise ValueError("repository runtime eligibility differs")
            evidence_status = "CURRENT_V3_LEDGER_RESEARCH_EVIDENCE"
            effective_gate_status = "RESEARCH_EVIDENCE_VERIFIED"
            current_runtime_eligible = True
        elif source_gate_status == "BLOCK" and registry_status == "BLOCKED":
            if (
                artifact.get("contract_eligible") is not False
                or gate.get("contract_eligible") is not False
            ):
                raise ValueError("Shadow contract eligibility differs")
            if row.get("runtime_eligible") is not False:
                raise ValueError("repository runtime eligibility differs")
            evidence_status = "BLOCKED_V3_LEDGER_RESEARCH_ARTIFACT"
            effective_gate_status = "BLOCKED"
            current_runtime_eligible = False
        else:
            raise ValueError("registry/gate status differs")
        selected_economics = {
            "selected_oos_sample_count": _artifact_count(
                evidence.get("selected_oos_sample_count"),
                "selected_oos_sample_count",
            ),
            "selected_oos_session_count": _artifact_count(
                evidence.get("selected_oos_session_count"),
                "selected_oos_session_count",
            ),
            "net_expectancy_after_cost_pct": _artifact_number(
                evidence.get("net_expectancy_after_cost_pct"),
                "net_expectancy_after_cost_pct",
            ),
            "profit_factor": _artifact_number(
                evidence.get("profit_factor"), "profit_factor"
            ),
            "cost_coverage_ratio": _artifact_number(
                evidence.get("cost_coverage_ratio"),
                "cost_coverage_ratio",
            ),
        }
        unconditional_baseline = {
            "net_expectancy_after_cost_pct": _artifact_number(
                evidence.get(
                    "unconditional_baseline_net_expectancy_after_cost_pct"
                ),
                "unconditional_baseline_net_expectancy_after_cost_pct",
            ),
            "profit_factor": _artifact_number(
                evidence.get("unconditional_baseline_profit_factor"),
                "unconditional_baseline_profit_factor",
            ),
            "cost_coverage_ratio": _artifact_number(
                evidence.get("unconditional_baseline_cost_coverage_ratio"),
                "unconditional_baseline_cost_coverage_ratio",
            ),
        }
        session_direction = {
            "protocol": _artifact_text(
                direction.get("protocol"), "direction.protocol"
            ),
            "session_count": _artifact_count(
                direction.get("session_count"), "direction.session_count"
            ),
            "valid_session_count": _artifact_count(
                direction.get("valid_session_count"),
                "direction.valid_session_count",
            ),
            "expected_return_rank_ic": _artifact_number(
                direction.get("expected_return_rank_ic"),
                "direction.expected_return_rank_ic",
            ),
            "probability_rank_ic": _artifact_number(
                direction.get("probability_rank_ic"),
                "direction.probability_rank_ic",
            ),
            "gate_direction_rank_ic": _artifact_number(
                direction.get("gate_direction_rank_ic"),
                "direction.gate_direction_rank_ic",
            ),
        }
        calibration_evidence = {
            "protocol": HORIZON_CALIBRATION_PROTOCOL,
            "walk_forward_protocol": _artifact_text(
                dict(artifact["walk_forward"]).get("protocol"),
                "walk_forward.protocol",
            ),
            "oos_only": True,
            "prequential": True,
            "labels_purged_by_maturity": True,
            "evaluation_sample_count": _artifact_count(
                evidence.get("calibration_evaluation_sample_count"),
                "calibration_evaluation_sample_count",
            ),
            "evaluation_session_count": _artifact_count(
                evidence.get("distinct_calibration_evaluation_sessions"),
                "distinct_calibration_evaluation_sessions",
            ),
        }
        candidate_economic_scope = {
            "candidate_scope": _artifact_text(
                selection_policy.get("candidate_domain"),
                "selection_policy.candidate_domain",
            ),
            "economic_evaluation_scope": _artifact_text(
                selection.get("economic_evaluation_scope"),
                "selection.economic_evaluation_scope",
            ),
            "gate_scope": _artifact_text(
                gate.get("gate_scope"), "gate.gate_scope"
            ),
            "deployment_gate": False,
            "deployment_candidate_domain_verified": False,
            "candidate_sample_count": _artifact_count(
                selection.get("candidate_sample_count"),
                "selection.candidate_sample_count",
            ),
            "eligible_candidate_count": _artifact_count(
                selection.get("eligible_candidate_count"),
                "selection.eligible_candidate_count",
            ),
        }
        candidate_ledger = {
            "schema_version": candidate_ledger_schema,
            "binding_protocol": HORIZON_CANDIDATE_LEDGER_BINDING_PROTOCOL,
            "encoding": HORIZON_CANDIDATE_LEDGER_ENCODING,
            "content_sha256": candidate_ledger_content_sha256,
            "canonical_records_sha256": (
                candidate_ledger_canonical_records_sha256
            ),
            "reference_hash": candidate_ledger_reference_hash,
            "row_count": candidate_ledger_row_count,
            "session_count": candidate_ledger_session_count,
            "evaluation_row_count": candidate_ledger_evaluation_row_count,
            "evaluation_session_count": (
                candidate_ledger_evaluation_session_count
            ),
            "fold_count": candidate_ledger_fold_count,
            "ledger_registration_evidence_hash": (
                ledger_registration_evidence_hash
            ),
            "registration_verification_hash": (
                registration_verification_hash
            ),
            "artifact_registration_evidence_hash": (
                artifact_registration_evidence_hash
            ),
            "registration_verified": True,
            "evidence_status": "REGISTERED_CONTENT_VERIFIED",
        }
        block_reasons = list(gate.get("block_reasons") or ())
        if any(not isinstance(item, str) or not item for item in block_reasons):
            raise ValueError("block reasons unavailable")
        return {
            "artifact_id": row.get("artifact_id"),
            "artifact_schema": schema,
            "release_id": _artifact_text(
                artifact.get("release_id"), "release_id"
            ),
            "suite_release_id": _artifact_text(
                artifact.get("suite_release_id"), "suite_release_id"
            ),
            "model_key": _artifact_text(artifact.get("model_key"), "model_key"),
            "model_version": _artifact_text(
                artifact.get("model_version"), "model_version"
            ),
            "horizon_days": int(artifact["horizon_days"]),
            "prediction_kind": "CALIBRATED_OOS",
            "artifact_status": registry_status,
            "evidence_status": evidence_status,
            "artifact_hash": _artifact_text(
                artifact.get("artifact_hash"), "artifact_hash"
            ),
            "feature_protocol_hash": _artifact_text(
                artifact.get("feature_protocol_hash"),
                "feature_protocol_hash",
            ),
            "calibration_evidence_hash": _artifact_text(
                artifact.get("oos_evidence_hash"),
                "oos_evidence_hash",
            ),
            "config_hash": _artifact_text(
                artifact.get("config_hash"), "config_hash"
            ),
            "code_version": _artifact_text(
                artifact.get("code_version"), "code_version"
            ),
            "created_at": _artifact_text(
                artifact.get("created_at"), "created_at"
            ),
            "valid_until": _artifact_text(
                artifact.get("valid_until"), "valid_until"
            ),
            "gate_status": effective_gate_status,
            "eligibility_boundary": {
                "contract_eligibility_scope": (
                    HORIZON_CONTRACT_ELIGIBILITY_SCOPE
                ),
                "contract_eligible": (
                    source_gate_status == "PASS"
                ),
                "paper_eligible": False,
                "production_eligible": False,
                "evidence_status": "VERIFIED_SHADOW_BOUNDARY",
            },
            "block_reasons": block_reasons,
            "protocols": {
                "artifact": schema,
                "suite": HORIZON_SUITE_SCHEMA,
                "model": HORIZON_MODEL_PROTOCOL,
                "selection": HORIZON_SELECTION_PROTOCOL,
                "calibration": HORIZON_CALIBRATION_PROTOCOL,
            },
            "candidate_economic_scope": candidate_economic_scope,
            "candidate_ledger": candidate_ledger,
            "candidate_ledger_schema_version": candidate_ledger_schema,
            "candidate_ledger_content_sha256": (
                candidate_ledger_content_sha256
            ),
            "candidate_ledger_row_count": candidate_ledger_row_count,
            "ledger_registration_evidence_hash": (
                ledger_registration_evidence_hash
            ),
            "registration_verification_hash": (
                registration_verification_hash
            ),
            "registration_verified": True,
            "selected_economics": selected_economics,
            "unconditional_baseline": unconditional_baseline,
            "session_direction": session_direction,
            "calibration_evidence": calibration_evidence,
            "feature_count": len(features),
            "distinct_train_sessions": _artifact_count(
                evidence.get("distinct_train_sessions"),
                "distinct_train_sessions",
            ),
            "distinct_oos_sessions": _artifact_count(
                evidence.get("distinct_oos_sessions"),
                "distinct_oos_sessions",
            ),
            "oos_sample_count": _artifact_count(
                evidence.get("oos_sample_count"), "oos_sample_count"
            ),
            "walk_forward_fold_count": _artifact_count(
                evidence.get("walk_forward_fold_count"),
                "walk_forward_fold_count",
            ),
            "reason_codes": [],
            "current_runtime_eligible": current_runtime_eligible,
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
    except (KeyError, TypeError, ValueError, OverflowError):
        return _unavailable_horizon_artifact_projection(
            row,
            artifact,
            reason_code="V3_ARTIFACT_OR_LEDGER_EVIDENCE_MISSING_OR_INVALID",
        )


def _contract_imputation_projection(row: Mapping[str, Any]) -> list[str]:
    return _contract_imputation_evidence(row)[0]


def _contract_imputation_evidence(
    row: Mapping[str, Any],
) -> tuple[list[str], str]:
    raw = row.get("contract_json")
    if raw is None:
        return [], "UNAVAILABLE"
    try:
        parsed = raw if isinstance(raw, Mapping) else json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("HORIZON_CONTRACT_JSON_INVALID") from exc
    if not isinstance(parsed, Mapping):
        raise RuntimeError("HORIZON_CONTRACT_JSON_INVALID")
    document = dict(parsed)
    if "imputed_feature_keys" not in document:
        return [], "HISTORICAL_FIELD_UNAVAILABLE"
    values = document["imputed_feature_keys"]
    if not isinstance(values, list) or any(
        not isinstance(item, str)
        or not item.strip()
        or item != item.strip()
        for item in values
    ):
        raise RuntimeError("HORIZON_CONTRACT_IMPUTATION_EVIDENCE_INVALID")
    return sorted(set(values)), "VERIFIED"


@router.get("/research/horizons/latest")
def latest_horizon_runtime(
    run_uid: str = Query(
        default="",
        pattern=r"^(|[a-zA-Z0-9:_-]{8,64})$",
    ),
    limit: int = Query(default=1000, ge=1, le=10000),
):
    """Read server-persisted horizon contracts and outcome provenance."""

    try:
        repository = _shadow_repo()
        contract_probe_limit = min(limit + 1, 10000)
        contract_rows = repository.horizon_contracts(
            run_uid=run_uid.strip() or None,
            limit=contract_probe_limit,
        )
        contracts = contract_rows[:limit]
        contracts_truncated = len(contract_rows) > limit
        truncation_unknown = bool(
            limit == 10000 and len(contract_rows) == limit
        )
        contract_ids = {
            str(item.get("contract_id") or "")
            for item in contracts
            if str(item.get("contract_id") or "")
        }
        outcomes = repository.horizon_outcomes(
            contract_ids=contract_ids,
            limit=max(1, len(contract_ids)),
        )
    except HTTPException:
        raise
    except Exception as exc:
        return _envelope(
            {
                "status": "UNAVAILABLE",
                "reason_codes": ["SHADOW_RUNTIME_UNAVAILABLE"],
                "error_code": "HORIZON_RUNTIME_READ_FAILED",
                "contracts": [],
                "outcomes": [],
                "artifact_registry": {
                    "status": "UNAVAILABLE",
                    "reason_codes": ["ARTIFACT_REGISTRY_UNAVAILABLE"],
                    "artifacts": [],
                },
                "runtime_model_selection": {},
                "model_suite_runtime": {
                    "status": "UNAVAILABLE",
                    "order_authority": False,
                },
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            status="unavailable",
        )
    registry_status = "AVAILABLE"
    registry_reason_codes: list[str] = []
    artifact_projections: list[dict[str, Any]] = []
    artifact_reader = getattr(repository, "horizon_model_artifacts", None)
    if not callable(artifact_reader):
        registry_status = "UNAVAILABLE"
        registry_reason_codes = ["ARTIFACT_REGISTRY_UNAVAILABLE"]
    else:
        try:
            artifact_projections = [
                _horizon_artifact_projection(item)
                for item in artifact_reader(limit=1000)
            ]
        except Exception:
            registry_status = "UNAVAILABLE"
            registry_reason_codes = ["ARTIFACT_REGISTRY_UNAVAILABLE"]
            artifact_projections = []
    current_v3_artifact_count = sum(
        item.get("evidence_status")
        in {
            "CURRENT_V3_LEDGER_RESEARCH_EVIDENCE",
            "BLOCKED_V3_LEDGER_RESEARCH_ARTIFACT",
        }
        for item in artifact_projections
    )
    historical_v1_artifact_count = sum(
        item.get("evidence_status") == "HISTORICAL_AUDIT_ONLY"
        for item in artifact_projections
    )
    pre_ledger_v2_artifact_count = sum(
        item.get("evidence_status") == "PRE_LEDGER_V2_AUDIT_ONLY"
        for item in artifact_projections
    )
    unavailable_artifact_count = sum(
        item.get("evidence_status") == "UNAVAILABLE"
        for item in artifact_projections
    )
    if registry_status == "AVAILABLE" and unavailable_artifact_count:
        registry_status = "AVAILABLE_WITH_UNAVAILABLE_ARTIFACTS"
        registry_reason_codes = ["SOME_ARTIFACT_EVIDENCE_UNAVAILABLE"]
    elif (
        registry_status == "AVAILABLE"
        and (historical_v1_artifact_count or pre_ledger_v2_artifact_count)
        and current_v3_artifact_count == 0
    ):
        registry_status = "HISTORICAL_AUDIT_ONLY"
        registry_reason_codes = ["ONLY_PRE_V3_AUDIT_ARTIFACTS_AVAILABLE"]
    try:
        for item in contracts:
            _contract_imputation_projection(item)
    except RuntimeError:
        return _envelope(
            {
                "status": "UNAVAILABLE",
                "reason_codes": ["CONTRACT_EVIDENCE_UNAVAILABLE"],
                "error_code": "HORIZON_CONTRACT_EVIDENCE_INVALID",
                "contracts": [],
                "outcomes": [],
                "artifact_registry": {
                    "status": "UNAVAILABLE",
                    "reason_codes": ["CONTRACT_EVIDENCE_UNAVAILABLE"],
                    "artifacts": [],
                },
                "runtime_model_selection": {},
                "model_suite_runtime": {
                    "status": "UNAVAILABLE",
                    "order_authority": False,
                },
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            status="unavailable",
        )
    per_horizon: dict[str, dict[str, Any]] = {}
    runtime_model_selection: dict[str, dict[str, Any]] = {}
    for horizon in (1, 5, 20):
        horizon_rows = [
            item
            for item in contracts
            if int(item.get("horizon_days") or 0) == horizon
        ]
        statuses: dict[str, int] = {}
        for item in horizon_rows:
            status = str(
                item.get("derived_contract_status") or "UNKNOWN"
            )
            statuses[status] = statuses.get(status, 0) + 1
        per_horizon[f"T+{horizon}"] = {
            "contract_count": len(horizon_rows),
            "status_counts": statuses,
        }
        prediction_kinds = sorted({
            str(item.get("prediction_kind") or "")
            for item in horizon_rows
            if str(item.get("prediction_kind") or "")
        })
        artifact_hashes = sorted({
            str(item.get("model_artifact_hash") or "")
            for item in horizon_rows
            if str(item.get("model_artifact_hash") or "")
        })
        matching_artifacts = [
            item for item in artifact_projections
            if int(item.get("horizon_days") or 0) == horizon
            and str(item.get("artifact_hash") or "") in artifact_hashes
        ]
        imputation_evidence = [
            _contract_imputation_evidence(item) for item in horizon_rows
        ]
        imputation_rows = [item[0] for item in imputation_evidence]
        imputation_statuses = [item[1] for item in imputation_evidence]
        imputed_keys = {
            key for values in imputation_rows for key in values
        }
        verified_matches = [
            item for item in matching_artifacts
            if item.get("current_runtime_eligible") is True
            and item.get("evidence_status")
            == "CURRENT_V3_LEDGER_RESEARCH_EVIDENCE"
        ]
        historical_matches = [
            item for item in matching_artifacts
            if item.get("evidence_status")
            in {"HISTORICAL_AUDIT_ONLY", "PRE_LEDGER_V2_AUDIT_ONLY"}
        ]
        unavailable_matches = [
            item for item in matching_artifacts
            if item.get("evidence_status") == "UNAVAILABLE"
        ]
        calibrated_rows = [
            item for item in horizon_rows
            if str(item.get("prediction_kind") or "") == "CALIBRATED_OOS"
        ]
        verified_artifact = (
            verified_matches[0] if len(verified_matches) == 1 else None
        )
        contract_artifact_binding_verified = bool(
            verified_artifact
            and calibrated_rows
            and all(
                str(item.get("model_artifact_hash") or "")
                == str(verified_artifact.get("artifact_hash") or "")
                and str(item.get("model_key") or "")
                == str(verified_artifact.get("model_key") or "")
                and str(item.get("model_version") or "")
                == str(verified_artifact.get("model_version") or "")
                and str(item.get("feature_protocol_hash") or "")
                == str(verified_artifact.get("feature_protocol_hash") or "")
                and str(item.get("calibration_evidence_hash") or "")
                == str(
                    verified_artifact.get("calibration_evidence_hash") or ""
                )
                for item in calibrated_rows
            )
        )
        if registry_status == "UNAVAILABLE":
            selection_status = "REGISTRY_UNAVAILABLE"
            selection_reasons = list(registry_reason_codes)
        elif "CALIBRATED_OOS" in prediction_kinds:
            if historical_matches:
                selection_status = str(
                    historical_matches[0].get("evidence_status")
                )
                selection_reasons = list(
                    historical_matches[0].get("reason_codes") or ()
                )
            elif unavailable_matches or not verified_matches:
                selection_status = "REGISTRY_UNAVAILABLE"
                selection_reasons = [
                    "CONTRACT_ARTIFACT_V3_LEDGER_EVIDENCE_UNAVAILABLE"
                ]
            elif (
                prediction_kinds == ["CALIBRATED_OOS"]
                and len(artifact_hashes) == 1
                and len(verified_matches) == 1
            ):
                if contract_artifact_binding_verified:
                    selection_status = "REAL_OOS_MODEL"
                    selection_reasons = []
                else:
                    selection_status = "MIXED_MODEL_EVIDENCE_BLOCKED"
                    selection_reasons = [
                        "CONTRACT_ARTIFACT_BINDING_MISMATCH"
                    ]
            else:
                selection_status = "MIXED_MODEL_EVIDENCE_BLOCKED"
                selection_reasons = ["MIXED_PREDICTION_OR_ARTIFACT_EVIDENCE"]
        elif prediction_kinds == ["PROXY_SCORE"]:
            selection_status = "PROXY_FALLBACK"
            selection_reasons = ["NO_CALIBRATED_OOS_CONTRACT_FOR_RUN"]
        elif prediction_kinds:
            selection_status = "REGISTRY_UNAVAILABLE"
            selection_reasons = ["PREDICTION_KIND_UNSUPPORTED"]
        else:
            selection_status = "COLLECTING"
            selection_reasons = ["HORIZON_CONTRACT_NOT_AVAILABLE"]
        artifact = (
            verified_matches[0]
            if len(verified_matches) == 1
            else historical_matches[0]
            if len(historical_matches) == 1
            else unavailable_matches[0]
            if len(unavailable_matches) == 1
            else {}
        )
        feature_count = int(artifact.get("feature_count") or 0)
        imputed_occurrences = sum(len(values) for values in imputation_rows)
        fully_imputed_contract_count = sum(
            feature_count > 0 and len(values) >= feature_count
            for values in imputation_rows
        )
        imputation_denominator = len(horizon_rows) * feature_count
        imputation_ratio = (
            imputed_occurrences / imputation_denominator
            if imputation_denominator > 0 else None
        )
        if selection_status == "REAL_OOS_MODEL":
            if any(status != "VERIFIED" for status in imputation_statuses):
                selection_status = "REGISTRY_UNAVAILABLE"
                selection_reasons.append(
                    "V3_CONTRACT_IMPUTATION_EVIDENCE_UNAVAILABLE"
                )
            elif fully_imputed_contract_count:
                selection_status = "MODEL_INPUT_EVIDENCE_BLOCKED"
                selection_reasons.append("FULL_MEDIAN_IMPUTATION_OBSERVED")
            elif imputation_ratio is not None and imputation_ratio >= 0.5:
                selection_status = "REAL_OOS_MODEL_DEGRADED"
                selection_reasons.append("HIGH_MEDIAN_IMPUTATION_RATIO")
        runtime_model_selection[f"T+{horizon}"] = {
            "status": selection_status,
            "prediction_kinds": prediction_kinds,
            "reason_codes": selection_reasons,
            "model_key": (
                artifact.get("model_key")
                if artifact
                else horizon_rows[0].get("model_key")
                if horizon_rows
                else None
            ),
            "model_version": (
                artifact.get("model_version")
                if artifact
                else horizon_rows[0].get("model_version")
                if horizon_rows
                else None
            ),
            "artifact_hashes": artifact_hashes,
            "artifact_schema": artifact.get("artifact_schema"),
            "artifact_evidence_status": artifact.get("evidence_status"),
            "suite_release_id": artifact.get("suite_release_id"),
            "artifact_valid_until": artifact.get("valid_until"),
            "artifact_gate_status": artifact.get("gate_status"),
            "eligibility_boundary": artifact.get("eligibility_boundary"),
            "candidate_ledger": artifact.get("candidate_ledger"),
            "candidate_ledger_schema_version": artifact.get(
                "candidate_ledger_schema_version"
            ),
            "candidate_ledger_content_sha256": artifact.get(
                "candidate_ledger_content_sha256"
            ),
            "candidate_ledger_row_count": artifact.get(
                "candidate_ledger_row_count"
            ),
            "ledger_registration_evidence_hash": artifact.get(
                "ledger_registration_evidence_hash"
            ),
            "registration_verification_hash": artifact.get(
                "registration_verification_hash"
            ),
            "registration_verified": artifact.get("registration_verified"),
            "protocols": artifact.get("protocols"),
            "candidate_economic_scope": artifact.get(
                "candidate_economic_scope"
            ),
            "selected_economics": artifact.get("selected_economics"),
            "unconditional_baseline": artifact.get(
                "unconditional_baseline"
            ),
            "session_direction": artifact.get("session_direction"),
            "calibration_evidence": artifact.get(
                "calibration_evidence"
            ),
            "contract_artifact_binding_status": (
                "VERIFIED"
                if contract_artifact_binding_verified
                else selection_status
                if selection_status
                in {"HISTORICAL_AUDIT_ONLY", "PRE_LEDGER_V2_AUDIT_ONLY"}
                else "NOT_APPLICABLE"
                if selection_status in {"PROXY_FALLBACK", "COLLECTING"}
                else "UNAVAILABLE"
            ),
            "imputed_feature_keys": sorted(imputed_keys),
            "imputation": {
                "evidence_status": (
                    selection_status
                    if selection_status
                    in {
                        "HISTORICAL_AUDIT_ONLY",
                        "PRE_LEDGER_V2_AUDIT_ONLY",
                    }
                    else "VERIFIED"
                    if imputation_statuses
                    and all(
                        status == "VERIFIED"
                        for status in imputation_statuses
                    )
                    else "UNAVAILABLE"
                ),
                "feature_count": feature_count or None,
                "imputed_feature_occurrence_count": imputed_occurrences,
                "fully_imputed_contract_count": (
                    fully_imputed_contract_count
                ),
                "imputed_feature_ratio": imputation_ratio,
            },
            "contract_count": len(horizon_rows),
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
    v3_suite_ids = {
        str(item.get("suite_release_id") or "")
        for item in runtime_model_selection.values()
        if item.get("artifact_evidence_status")
        == "CURRENT_V3_LEDGER_RESEARCH_EVIDENCE"
    }
    all_three_current_v3 = len(v3_suite_ids) > 0 and all(
        item.get("artifact_evidence_status")
        == "CURRENT_V3_LEDGER_RESEARCH_EVIDENCE"
        for item in runtime_model_selection.values()
    )
    if all_three_current_v3 and len(v3_suite_ids) != 1:
        for item in runtime_model_selection.values():
            item["status"] = "CROSS_SUITE_MODEL_EVIDENCE_BLOCKED"
            item["reason_codes"] = list(item["reason_codes"]) + [
                "T1_T5_T20_SUITE_RELEASE_MISMATCH"
            ]
    model_suite_runtime = {
        "status": (
            "CURRENT_V3_SINGLE_SUITE_RESEARCH_ONLY"
            if all_three_current_v3 and len(v3_suite_ids) == 1
            else "CROSS_SUITE_MODEL_EVIDENCE_BLOCKED"
            if all_three_current_v3
            else "INCOMPLETE_OR_FALLBACK"
        ),
        "suite_release_ids": sorted(v3_suite_ids),
        "all_three_current_v3": all_three_current_v3,
        "single_suite": all_three_current_v3 and len(v3_suite_ids) == 1,
        "protocol": HORIZON_SUITE_SCHEMA,
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
    }
    projected_contracts = [
        {
            key: item.get(key)
            for key in (
                "contract_id",
                "source_forecast_id",
                "run_uid",
                "stock_code",
                "model_key",
                "model_version",
                "model_artifact_hash",
                "feature_protocol_hash",
                "horizon_days",
                "prediction_kind",
                "decision_as_of",
                "feature_as_of",
                "decision_session_date",
                "entry_trade_date",
                "outcome_matures_on",
                "score",
                "expected_return_net_pct",
                "probability_positive",
                "calibration_evidence_hash",
                "contract_hash",
                "derived_contract_status",
            )
        } | {
            "imputed_feature_keys": _contract_imputation_projection(item),
            "imputation_evidence_status": (
                _contract_imputation_evidence(item)[1]
            ),
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        }
        for item in contracts
    ]
    projected_outcomes = [
        {
            key: item.get(key)
            for key in (
                "outcome_id",
                "contract_id",
                "horizon_days",
                "entry_trade_date",
                "exit_trade_date",
                "realized_net_return_pct",
                "realized_cost_pct",
                "market_data_source",
                "market_evidence_hash",
                "outcome_hash",
                "outcome_status",
                "observed_at",
            )
        }
        for item in outcomes
    ]
    result_partial = contracts_truncated or truncation_unknown
    runtime_status = (
        "COLLECTING"
        if not contracts
        else "TRUNCATED"
        if result_partial
        else "READY"
    )
    return _envelope(
        {
            "status": runtime_status,
            "run_uid": run_uid.strip() or None,
            "contracts": projected_contracts,
            "outcomes": projected_outcomes,
            "reason_codes": (
                ["CONTRACT_RESULT_TRUNCATED"]
                if contracts_truncated
                else ["CONTRACT_TRUNCATION_UNKNOWN_AT_MAX_LIMIT"]
                if truncation_unknown
                else []
            ),
            "artifact_registry": {
                "status": registry_status,
                "reason_codes": registry_reason_codes,
                "artifact_count": len(artifact_projections),
                "current_v3_artifact_count": current_v3_artifact_count,
                "pre_ledger_v2_artifact_count": (
                    pre_ledger_v2_artifact_count
                ),
                "historical_v1_artifact_count": (
                    historical_v1_artifact_count
                ),
                "unavailable_artifact_count": unavailable_artifact_count,
                "artifacts": artifact_projections,
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            "runtime_model_selection": runtime_model_selection,
            "model_suite_runtime": model_suite_runtime,
            "pagination": {
                "limit": limit,
                "returned_contract_count": len(contracts),
                "returned_outcome_count": len(outcomes),
                "contract_limit_reached": len(contracts) == limit,
                "truncated": contracts_truncated,
                "truncation_unknown": truncation_unknown,
            },
            "summary": {
                "contract_count": len(contracts),
                "outcome_count": len(outcomes),
                "verified_outcome_count": sum(
                    str(item.get("outcome_status") or "")
                    == "MATURED_VERIFIED"
                    for item in outcomes
                ),
                "per_horizon": per_horizon,
            },
            "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        },
        status=(
            "partial"
            if result_partial
            else "ok"
            if contracts
            else "collecting"
        ),
    )


@router.get("/research/learning/latest")
def latest_counterfactual_learning_runtime():
    """Read the latest hash-verified learning run; never infer from a task exit."""

    try:
        repository = _shadow_repo()
        runtime = _learning_runtime_truth(repository)
        row = runtime["row"]
        if row is None:
            return _envelope(
                {
                    "status": "COLLECTING",
                    "reason_codes": ["LEARNING_RUN_NOT_AVAILABLE"],
                    "per_horizon": {},
                    "evidence_verified": False,
                    "decision_scope": "RESEARCH_ONLY",
                    "order_authority": False,
                },
                status="collecting",
            )
        verified = runtime["verified"]
    except HTTPException:
        raise
    except Exception as exc:
        return _envelope(
            {
                "status": "UNAVAILABLE",
                "reason_codes": ["LEARNING_RUNTIME_UNAVAILABLE"],
                "detail": str(exc)[:300],
                "evidence_verified": False,
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            status="unavailable",
        )
    if verified is None:
        return _envelope(
            {
                "status": "BLOCKED",
                "reason_codes": runtime["reason_codes"],
                "learning_run_id": row.get("learning_run_id"),
                "evidence_verified": False,
                "decision_scope": "RESEARCH_ONLY",
                "order_authority": False,
            },
            status="blocked",
        )
    metrics = runtime["metrics"]
    raw_status = str(verified.get("learning_status") or "COLLECTING")
    evidence_verified = bool(runtime["evidence_verified"])
    effective_status = (
        "BLOCKED"
        if raw_status == "EVIDENCE_READY" and not evidence_verified
        else raw_status
    )
    return _envelope(
        {
            "status": effective_status,
            "audit_status": raw_status,
            "learning_run_id": verified.get("learning_run_id"),
            "evaluation_date": verified.get("evaluation_date"),
            "evaluated_at": verified.get("evaluated_at"),
            "sample_count": verified.get("sample_count"),
            "per_horizon": metrics.get("horizon_readiness") or {},
            "metrics": metrics.get("overall") or {},
            "evidence_source": verified.get("evidence_source"),
            "evidence_hash": verified.get("evidence_hash"),
            "policy_hash": verified.get("policy_hash"),
            "learning_result_hash": verified.get("learning_result_hash"),
            "evidence_verified": evidence_verified,
            "policy_current": bool(runtime.get("policy_current")),
            "config_current": bool(runtime.get("config_current")),
            "code_current": bool(runtime.get("code_current")),
            "fresh": bool(runtime.get("fresh")),
            "reason_codes": runtime["reason_codes"],
            "can_activate_model": False,
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
        },
        status=(
            "ok"
            if effective_status == "EVIDENCE_READY"
            else "blocked"
            if effective_status == "BLOCKED"
            else "collecting"
        ),
    )


@router.get("/research/shadow/status")
def shadow_release_runtime_status():
    """Read append-only release transitions and their persisted gate ids."""

    try:
        repository = _shadow_repo()
        audit = repository.release_audit()
        learning_runtime = _learning_runtime_truth(repository)
        latest_learning = learning_runtime.get("verified") or {}
        latest_learning_id = str(
            latest_learning.get("learning_run_id") or ""
        )
        current_policy_hash = _current_calibration_policy_hash()
        current_config_hash = config_hash()
        current_code_version, _code_version_source = code_version()
        releases = []
        for raw in audit.get("releases") or []:
            release = dict(raw)
            audit_stage = str(
                release.get("audit_stage")
                or release.get("current_stage")
                or "DRAFT"
            )
            repository_effective_stage = str(
                release.get("effective_stage") or audit_stage
            )
            repository_blockers = [
                str(item)
                for item in (
                    release.get("effective_blockers") or []
                )
                if str(item)
            ]
            gate = repository.latest_calibration_gate(
                str(release.get("release_id") or "")
            )
            gate_projection = None
            gate_effective_pass = False
            gate_reason_codes: list[str] = list(repository_blockers)
            if gate is not None:
                gate_reason_codes.extend(_json_value(
                    gate.get("failure_codes_json"), []
                ))
                policy_current = str(gate.get("policy_hash") or "") == (
                    current_policy_hash
                )
                release_config_current = str(
                    release.get("config_hash") or ""
                ) == current_config_hash
                gate_config_current = str(
                    gate.get("config_hash") or ""
                ) == current_config_hash
                gate_code_current = str(
                    gate.get("code_version") or ""
                ) == str(current_code_version)
                model_artifact_current = bool(
                    len(str(gate.get("model_artifact_hash") or "")) == 64
                    and "GATE_MODEL_ARTIFACT_STALE"
                    not in repository_blockers
                )
                gate_fresh = _shadow_evidence_is_fresh(
                    gate.get("evidence_observed_at"),
                    valid_until=gate.get("evidence_valid_until"),
                )
                learning_current = bool(
                    latest_learning_id
                    and str(gate.get("learning_run_id") or "")
                    == latest_learning_id
                    and learning_runtime.get("evidence_verified")
                    and str(
                        latest_learning.get("learning_status") or ""
                    )
                    == "EVIDENCE_READY"
                )
                if not policy_current:
                    gate_reason_codes.append("GATE_POLICY_STALE")
                if not release_config_current:
                    gate_reason_codes.append("RELEASE_CONFIG_STALE")
                if not gate_config_current:
                    gate_reason_codes.append("GATE_CONFIG_STALE")
                if not gate_code_current:
                    gate_reason_codes.append("GATE_CODE_VERSION_STALE")
                if not model_artifact_current:
                    gate_reason_codes.append("GATE_MODEL_ARTIFACT_STALE")
                if not gate_fresh:
                    gate_reason_codes.append("GATE_EVIDENCE_STALE")
                if not learning_current:
                    gate_reason_codes.append("GATE_LEARNING_RUN_STALE")
                if str(
                    gate.get("evidence_provenance_status") or ""
                ) != "PERSISTED_VERIFIED":
                    gate_reason_codes.append("GATE_PROVENANCE_UNVERIFIED")
                gate_result_hash_present = (
                    len(str(gate.get("gate_result_hash") or "")) == 64
                )
                if not gate_result_hash_present:
                    gate_reason_codes.append("GATE_RESULT_HASH_MISSING")
                gate_effective_pass = bool(
                    str(gate.get("gate_status") or "") == "PASS"
                    and str(
                        gate.get("evidence_provenance_status") or ""
                    )
                    == "PERSISTED_VERIFIED"
                    and policy_current
                    and release_config_current
                    and gate_config_current
                    and gate_code_current
                    and model_artifact_current
                    and gate_fresh
                    and learning_current
                    and gate_result_hash_present
                    and not gate_reason_codes
                )
                gate_projection = {
                    "gate_evaluation_id": gate.get("gate_evaluation_id"),
                    "gate_status": gate.get("gate_status"),
                    "recommended_stage": gate.get("recommended_stage"),
                    "learning_run_id": gate.get("learning_run_id"),
                    "evidence_provenance_status": gate.get(
                        "evidence_provenance_status"
                    ),
                    "failure_codes": list(dict.fromkeys(gate_reason_codes)),
                    "evidence_hash": gate.get("evidence_hash"),
                    "policy_hash": gate.get("policy_hash"),
                    "gate_result_hash": gate.get("gate_result_hash"),
                    "evidence_observed_at": gate.get(
                        "evidence_observed_at"
                    ),
                    "evidence_valid_until": gate.get(
                        "evidence_valid_until"
                    ),
                    "evaluated_at": gate.get("evaluated_at"),
                    "policy_current": policy_current,
                    "config_current": (
                        release_config_current and gate_config_current
                    ),
                    "release_config_current": release_config_current,
                    "gate_config_current": gate_config_current,
                    "code_current": gate_code_current,
                    "model_artifact_current": model_artifact_current,
                    "fresh": gate_fresh,
                    "learning_run_current": learning_current,
                    "effective_pass": gate_effective_pass,
                    "order_authority": False,
                }
            else:
                gate_reason_codes.append("CALIBRATION_GATE_MISSING")
            gate_reason_codes = list(dict.fromkeys(gate_reason_codes))
            effective_stage = repository_effective_stage
            if (
                audit_stage == "PAPER_ELIGIBLE"
                and (
                    repository_effective_stage != "PAPER_ELIGIBLE"
                    or not gate_effective_pass
                )
            ):
                effective_stage = "BLOCKED"
            releases.append({
                **release,
                "audit_stage": audit_stage,
                "effective_stage": effective_stage,
                "effective_blockers": gate_reason_codes,
                "latest_gate": gate_projection,
                "order_authority": False,
            })
    except HTTPException:
        raise
    except Exception as exc:
        return _envelope(
            {
                "status": "UNAVAILABLE",
                "reason_codes": ["SHADOW_RELEASE_RUNTIME_UNAVAILABLE"],
                "detail": str(exc)[:300],
                "releases": [],
                "order_authority": False,
            },
            status="unavailable",
        )
    audit_status = str(audit.get("status") or "COLLECTING").upper()
    effective_blocked = bool(
        releases
        and audit_status not in {"READY", "COLLECTING"}
    ) or any(
        str(item.get("effective_stage") or "") == "BLOCKED"
        for item in releases
    )
    return _envelope(
        {
            "status": (
                "BLOCKED"
                if effective_blocked
                else audit_status
            ),
            "audit_status": audit_status,
            "releases": releases,
            "paper_eligible_count": sum(
                str(item.get("effective_stage") or "")
                == "PAPER_ELIGIBLE"
                for item in releases
            ),
            "automatic_promotion_allowed": False,
            "external_execution_grant_required": True,
            "decision_scope": "RESEARCH_ONLY",
            "order_authority": False,
            "real_order_allowed": False,
        },
        status=(
            "blocked"
            if effective_blocked
            else "ok"
            if releases
            else "collecting"
        ),
    )


@router.post("/research/shadow/transition-preview")
def shadow_transition_preview(
    payload: dict[str, Any] = Body(...),
):
    """Preview a Shadow transition; no release or order state is written."""

    try:
        if payload.get("calibration_gate") is not None:
            raise ReleaseGovernanceError(
                "client-supplied calibration_gate is not trusted; provide "
                "raw evidence and policy for server-side evaluation"
            )
        if payload.get("policy") is not None:
            raise ReleaseGovernanceError(
                "client calibration policy is not accepted; the frozen "
                "trading_v3 configuration is authoritative"
            )
        if payload.get("evidence") is not None:
            raise ReleaseGovernanceError(
                "client-supplied raw evidence is UNVERIFIED_PREVIEW only; "
                "a persisted server-evaluated gate_id is required for "
                "release transitions"
            )
        transition = transition_shadow_release(
            payload.get("current_stage"),
            payload.get("event"),
            calibration_gate=None,
        )
    except (ReleaseGovernanceError, TypeError, ValueError) as exc:
        raise _research_error(exc) from exc
    return _envelope(transition.as_dict())


@router.get("/portfolio/latest")
def latest_portfolio(
    trade_date: date | None = None,
):
    repository = _repo()
    if trade_date is None:
        rows = repository.latest_targets()
    else:
        pool = repository.stock_pool(trade_date=trade_date)
        if pool.get("pool_readable") is not True:
            governance = canonical_governance_decision(
                trade_date,
                latest_as_of=True,
            )
            if governance is not None:
                pool = governance["pool"]
        rows = []
        for item in pool.get("items") or []:
            target = item.get("target")
            if not isinstance(target, dict):
                continue
            rows.append(
                {
                    **target,
                    "run_uid": pool.get("run_uid"),
                    "trade_date": pool.get("trade_date"),
                    "stock_code": item.get("stock_code"),
                    "short_name": item.get("stock_name"),
                }
            )
    return _envelope(
        [
            _research_target_projection(row)
            for row in rows
        ]
    )


@router.get("/paper-ledger")
def paper_ledger(
    account_id: str = Query(default="paper-main-v2", min_length=3, max_length=64),
    limit: int = Query(default=200, ge=1, le=500),
):
    """Return the two internal paper ledgers as one honest read model.

    The legacy event simulator is still the active scheduled executor while
    V2/V3 owns the new immutable plan/order ledger.  This endpoint merges only
    their display projections; it never copies, invents or executes a fill.
    """
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="paper ledger database unavailable")
    from server.trading_v2.repository import TradingV2ReadRepository

    v2 = TradingV2ReadRepository(engine)
    account = v2.account(account_id) or {}
    v2_positions = v2.positions(account_id)
    v2_orders = v2.orders(account_id, limit)
    v2_fills = v2.fills(account_id, max(int(limit), 500))
    with engine.connect() as connection:
        legacy_positions = [dict(row) for row in connection.execute(
            text(
                """
                SELECT id, stock_code, short_name, strategy_type, buy_price,
                       buy_shares, buy_date, buy_time, buy_reason, status
                FROM st_sim_position
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND status = 'holding'
                ORDER BY buy_date DESC, buy_time DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()]
        legacy_today_sold = [dict(row) for row in connection.execute(
            text(
                """
                SELECT id, stock_code, short_name, strategy_type, buy_price,
                       buy_shares, buy_date, buy_time, buy_reason, status,
                       sell_price, sell_date, sell_time, sell_reason,
                       profit, profit_rate
                FROM st_sim_position
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND status = 'sold'
                  AND sell_date = CURDATE()
                ORDER BY sell_time DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()]
        # Test doubles and old compatibility views can return broader rows than
        # the SQL predicate.  Keep the read model strict: only today's closed
        # positions may appear beside open holdings.
        legacy_today_sold = [
            row for row in legacy_today_sold
            if str(row.get("status") or "").lower() == "sold"
            and str(row.get("sell_date") or "")[:10] == date.today().isoformat()
        ]
        legacy_orders = [dict(row) for row in connection.execute(
            text(
                """
                SELECT id, stock_code, short_name, strategy_type, side,
                       requested_shares, filled_shares, remaining_shares,
                       limit_price, target_price, filled_price, status, reason,
                       reject_reason, last_match_reason, order_date,
                       order_time, filled_at, created_at
                FROM st_sim_order
                WHERE COALESCE(trade_mode, 'live') = 'live'
                ORDER BY COALESCE(filled_at, created_at) DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()]
        legacy_profit_rows = [dict(row) for row in connection.execute(
            text(
                """
                SELECT COALESCE(SUM(profit), 0) AS realized_profit
                FROM st_sim_position
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND status = 'sold'
                """
            ),
            {},
        ).mappings().all()]
        legacy_capital_rows = [dict(row) for row in connection.execute(
            text(
                """
                SELECT initial_capital
                FROM st_sim_risk_budget
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND initial_capital > 0
                ORDER BY budget_date DESC, updated_at DESC
                LIMIT 1
                """
            ),
            {},
        ).mappings().all()]

        position_codes = sorted({
            str(row.get("stock_code") or "").zfill(6)
            for row in [*v2_positions, *legacy_positions]
            if row.get("stock_code")
        })
        latest_quotes: dict[str, dict[str, Any]] = {}
        if position_codes:
            quote_params: dict[str, Any] = {}
            quote_placeholders: list[str] = []
            for index, stock_code in enumerate(position_codes):
                key = f"quote_code_{index}"
                quote_params[key] = stock_code
                quote_placeholders.append(f":{key}")
            quote_rows = connection.execute(
                text(
                    f"""
                    SELECT c.stock_code, c.short_name, c.price,
                           c.snapshot_at, c.data_source
                    FROM sm_stock_current c
                    JOIN (
                        SELECT stock_code, MAX(snapshot_at) AS snapshot_at
                        FROM sm_stock_current
                        WHERE stock_code IN ({','.join(quote_placeholders)})
                        GROUP BY stock_code
                    ) latest
                      ON latest.stock_code = c.stock_code
                     AND latest.snapshot_at = c.snapshot_at
                    """
                ),
                quote_params,
            ).mappings().all()
            latest_quotes = {
                str(row.get("stock_code") or "").zfill(6): dict(row)
                for row in quote_rows
            }

    def enrich_position(row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        stock_code = str(result.get("stock_code") or "").zfill(6)
        quote = latest_quotes.get(stock_code) or {}
        quantity = int(result.get("remaining_quantity") or result.get("quantity") or 0)
        cost_price = float(result.get("cost_price") or result.get("average_cost") or 0)
        current_price = float(quote.get("price") or 0)
        result["current_price"] = round(current_price, 4) if current_price > 0 else None
        result["quote_at"] = quote.get("snapshot_at")
        result["quote_source"] = quote.get("data_source")
        result["market_value"] = round(current_price * quantity, 2) if current_price > 0 else None
        result["unrealized_pnl"] = round((current_price - cost_price) * quantity, 2) if current_price > 0 and cost_price > 0 else None
        result["unrealized_pnl_pct"] = round((current_price / cost_price - 1.0) * 100.0, 2) if current_price > 0 and cost_price > 0 else None
        if quote.get("short_name"):
            result["short_name"] = quote.get("short_name")
        return result

    def display_datetime(date_value: Any, time_value: Any = None) -> str | None:
        if date_value in (None, ""):
            return None
        date_text = str(date_value).strip()
        if "T" in date_text or " " in date_text:
            return date_text.replace("T", " ")
        time_text = str(time_value or "").strip()
        return f"{date_text} {time_text}".strip()

    def date_token(value: Any) -> str:
        if value in (None, ""):
            return ""
        return str(value).strip().replace("T", " ")[:10]

    fill_by_id = {
        str(row.get("fill_id") or ""): row
        for row in v2_fills
        if row.get("fill_id")
    }
    v2_buy_times: dict[str, list[str]] = {}
    for fill in v2_fills:
        if str(fill.get("side") or "").upper() != "BUY":
            continue
        code = str(fill.get("stock_code") or "").zfill(6)
        filled_at = display_datetime(fill.get("filled_at"))
        if code and filled_at:
            v2_buy_times.setdefault(code, []).append(filled_at)

    position_lots = []
    for row in v2_positions:
        opened_fill = fill_by_id.get(str(row.get("opened_fill_id") or "")) or {}
        position_lots.append(enrich_position({
            **row,
            "ledger_source": "V2_CANONICAL",
            "buy_at": display_datetime(
                opened_fill.get("filled_at") or row.get("opened_trade_date")
            ),
        }))
    for row in legacy_positions:
        position_lots.append(enrich_position({
            **row,
            "ledger_source": "LEGACY_EVENT_SIM",
            "position_state": "HOLDING",
            "quantity": int(row.get("buy_shares") or 0),
            "remaining_quantity": int(row.get("buy_shares") or 0),
            # Legacy event-sim lots are display-only evidence.  They are not
            # canonical V2 inventory and must never inflate executable shares.
            "sellable_quantity": 0,
            "cost_price": row.get("buy_price"),
            "buy_at": display_datetime(row.get("buy_date"), row.get("buy_time")),
            "last_reason": row.get("buy_reason") or "事件驱动模拟成交",
        }))

    def merge_position_lots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Merge display positions by security without changing the fill ledger."""
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            stock_code = str(row.get("stock_code") or "").zfill(6)
            grouped.setdefault(stock_code, []).append(row)

        merged: list[dict[str, Any]] = []
        for stock_code, lots in grouped.items():
            result = dict(lots[0])
            quantities = [
                int(lot.get("remaining_quantity") or lot.get("quantity") or 0)
                for lot in lots
            ]
            quantity = sum(quantities)
            cost_amount = sum(
                float(lot.get("cost_price") or lot.get("average_cost") or 0) * lot_quantity
                for lot, lot_quantity in zip(lots, quantities)
            )
            cost_price = cost_amount / quantity if quantity > 0 and cost_amount > 0 else 0.0
            current_prices = [
                float(lot.get("current_price") or 0)
                for lot in lots
                if float(lot.get("current_price") or 0) > 0
            ]
            current_price = current_prices[0] if current_prices else 0.0
            ledger_sources = list(dict.fromkeys(
                str(lot.get("ledger_source") or "") for lot in lots if lot.get("ledger_source")
            ))
            reasons = list(dict.fromkeys(
                str(lot.get("invalidation_condition") or lot.get("last_reason") or "")
                for lot in lots
                if lot.get("invalidation_condition") or lot.get("last_reason")
            ))
            stops = [
                float(lot.get("protective_stop") or 0)
                for lot in lots
                if float(lot.get("protective_stop") or 0) > 0
            ]
            quote_lot = max(lots, key=lambda lot: str(lot.get("quote_at") or ""))
            buy_times = sorted(
                str(lot.get("buy_at")) for lot in lots if lot.get("buy_at")
            )
            lot_details = [
                {
                    "ledger_source": lot.get("ledger_source"),
                    "quantity": lot_quantity,
                    "cost_price": lot.get("cost_price") or lot.get("average_cost"),
                    "buy_at": lot.get("buy_at"),
                    "protective_stop": lot.get("protective_stop"),
                    "note": lot.get("invalidation_condition") or lot.get("last_reason") or "",
                }
                for lot, lot_quantity in zip(lots, quantities)
            ]

            result.update({
                "stock_code": stock_code,
                "short_name": quote_lot.get("short_name") or result.get("short_name"),
                "position_state": result.get("position_state") or result.get("state") or "HOLDING",
                "quantity": quantity,
                "remaining_quantity": quantity,
                "sellable_quantity": sum(int(lot.get("sellable_quantity") or 0) for lot in lots),
                "cost_price": round(cost_price, 4) if cost_price > 0 else None,
                "average_cost": round(cost_price, 4) if cost_price > 0 else None,
                "current_price": round(current_price, 4) if current_price > 0 else None,
                "market_value": round(current_price * quantity, 2) if current_price > 0 else None,
                "unrealized_pnl": round(current_price * quantity - cost_amount, 2)
                if current_price > 0 and cost_amount > 0 else None,
                "unrealized_pnl_pct": round((current_price * quantity / cost_amount - 1.0) * 100.0, 2)
                if current_price > 0 and cost_amount > 0 else None,
                "protective_stop": round(max(stops), 4) if stops else None,
                "add_count": sum(int(lot.get("add_count") or 0) for lot in lots) + max(0, len(lots) - 1),
                "quote_at": quote_lot.get("quote_at"),
                "quote_source": quote_lot.get("quote_source"),
                "ledger_source": ledger_sources[0] if len(ledger_sources) == 1 else "MERGED_LEDGER",
                "ledger_sources": ledger_sources,
                "position_lot_count": len(lots),
                "holding_notes": reasons,
                "last_reason": "；".join(reasons),
                "buy_at": buy_times[0] if buy_times else None,
                "sell_at": None,
                "sell_price": None,
                "sold_quantity_today": 0,
                "lot_details": lot_details,
            })
            merged.append(result)
        return merged

    positions = merge_position_lots(position_lots)

    sale_events: list[dict[str, Any]] = []
    for row in legacy_today_sold:
        sale_events.append({
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "short_name": row.get("short_name"),
            "ledger_source": "LEGACY_EVENT_SIM",
            "sold_quantity": int(row.get("buy_shares") or 0),
            "cost_price": row.get("buy_price"),
            "buy_at": display_datetime(row.get("buy_date"), row.get("buy_time")),
            "sell_at": display_datetime(row.get("sell_date"), row.get("sell_time")),
            "sell_price": row.get("sell_price"),
            "realized_pnl": row.get("profit"),
            "realized_pnl_pct": row.get("profit_rate"),
            "note": row.get("sell_reason") or row.get("buy_reason") or "今日已卖出",
        })
    today_text = date.today().isoformat()
    for row in v2_fills:
        if (
            str(row.get("side") or "").upper() != "SELL"
            or date_token(row.get("filled_at")) != today_text
        ):
            continue
        stock_code = str(row.get("stock_code") or "").zfill(6)
        buy_times = sorted(v2_buy_times.get(stock_code) or [])
        sale_events.append({
            "stock_code": stock_code,
            "short_name": row.get("short_name"),
            "ledger_source": "V2_CANONICAL",
            "sold_quantity": int(row.get("quantity") or 0),
            "cost_price": None,
            "buy_at": buy_times[0] if buy_times else None,
            "sell_at": display_datetime(row.get("filled_at")),
            "sell_price": row.get("price"),
            "realized_pnl": None,
            "realized_pnl_pct": None,
            "note": "V2/V3 模拟账本今日卖出成交",
        })

    def merge_sale_events(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            stock_code = str(row.get("stock_code") or "").zfill(6)
            if stock_code:
                grouped.setdefault(stock_code, []).append(row)
        merged: dict[str, dict[str, Any]] = {}
        for stock_code, events in grouped.items():
            quantities = [int(event.get("sold_quantity") or 0) for event in events]
            sold_quantity = sum(quantities)
            sell_amount = sum(
                float(event.get("sell_price") or 0) * quantity
                for event, quantity in zip(events, quantities)
            )
            cost_amount = sum(
                float(event.get("cost_price") or 0) * quantity
                for event, quantity in zip(events, quantities)
            )
            cost_quantity = sum(
                quantity
                for event, quantity in zip(events, quantities)
                if float(event.get("cost_price") or 0) > 0
            )
            sell_times = sorted(
                str(event.get("sell_at")) for event in events if event.get("sell_at")
            )
            buy_times = sorted(
                str(event.get("buy_at")) for event in events if event.get("buy_at")
            )
            notes = list(dict.fromkeys(
                str(event.get("note") or "") for event in events if event.get("note")
            ))
            sources = list(dict.fromkeys(
                str(event.get("ledger_source") or "")
                for event in events if event.get("ledger_source")
            ))
            realized_values = [
                float(event.get("realized_pnl") or 0)
                for event in events if event.get("realized_pnl") is not None
            ]
            merged[stock_code] = {
                "stock_code": stock_code,
                "short_name": next(
                    (event.get("short_name") for event in events if event.get("short_name")),
                    None,
                ),
                "position_state": "SOLD_TODAY",
                "quantity": 0,
                "remaining_quantity": 0,
                "sellable_quantity": 0,
                "sold_quantity_today": sold_quantity,
                "cost_price": round(cost_amount / cost_quantity, 4)
                if cost_quantity > 0 and cost_amount > 0 else None,
                "buy_at": buy_times[0] if buy_times else None,
                "sell_at": sell_times[-1] if sell_times else None,
                "sell_price": round(sell_amount / sold_quantity, 4)
                if sold_quantity > 0 and sell_amount > 0 else None,
                "realized_pnl": round(sum(realized_values), 2) if realized_values else None,
                "holding_notes": notes,
                "last_reason": "；".join(notes) or "今日已卖出",
                "ledger_source": sources[0] if len(sources) == 1 else "MERGED_LEDGER",
                "ledger_sources": sources,
                "position_lot_count": len(events),
                "lot_details": [
                    {
                        "ledger_source": event.get("ledger_source"),
                        "quantity": event.get("sold_quantity"),
                        "cost_price": event.get("cost_price"),
                        "buy_at": event.get("buy_at"),
                        "sell_at": event.get("sell_at"),
                        "sell_price": event.get("sell_price"),
                        "realized_pnl": event.get("realized_pnl"),
                        "note": event.get("note"),
                    }
                    for event in events
                ],
            }
        return merged

    sales_by_code = merge_sale_events(sale_events)
    open_codes = {str(row.get("stock_code") or "").zfill(6) for row in positions}
    for position in positions:
        sale = sales_by_code.get(str(position.get("stock_code") or "").zfill(6))
        if not sale:
            continue
        position.update({
            "sell_at": sale.get("sell_at"),
            "sell_price": sale.get("sell_price"),
            "sold_quantity_today": sale.get("sold_quantity_today") or 0,
            "lot_details": [
                *(position.get("lot_details") or []),
                *(sale.get("lot_details") or []),
            ],
        })
    today_closed_positions = [
        sale for stock_code, sale in sales_by_code.items()
        if stock_code not in open_codes
    ]

    orders = []
    for row in v2_orders:
        orders.append({**row, "ledger_source": "V2_CANONICAL"})
    for row in legacy_orders:
        orders.append({
            **row,
            "ledger_source": "LEGACY_EVENT_SIM",
            "order_id": f"legacy-{row.get('id')}",
            "quantity": int(row.get("requested_shares") or 0),
            "filled_quantity": int(row.get("filled_shares") or 0),
            "waiting_reason": row.get("last_match_reason") or row.get("reject_reason") or row.get("reason") or "",
            "earliest_at": f"{row.get('order_date') or ''} {row.get('order_time') or ''}".strip(),
            "expires_at": row.get("filled_at") or "",
        })
    initial_cash = float(account.get("initial_cash") or 0)
    latest_equity = account.get("latest_equity") or {}
    legacy_market_value = sum(
        float(row.get("buy_price") or 0) * int(row.get("buy_shares") or 0)
        for row in legacy_positions
    )
    legacy_current_market_value = sum(
        float(
            (latest_quotes.get(str(row.get("stock_code") or "").zfill(6)) or {}).get("price")
            or row.get("buy_price")
            or 0
        ) * int(row.get("buy_shares") or 0)
        for row in legacy_positions
    )
    legacy_initial_cash = float(
        (legacy_capital_rows[0] if legacy_capital_rows else {}).get("initial_capital")
        or 1_000_000
    )
    legacy_realized_pnl = float(
        (legacy_profit_rows[0] if legacy_profit_rows else {}).get("realized_profit")
        or 0
    )

    def pending_buy_amount(row: dict[str, Any]) -> float:
        if str(row.get("status") or "").upper() not in {"PENDING", "PARTIAL"}:
            return 0.0
        if str(row.get("side") or "").upper() != "BUY":
            return 0.0
        remaining = int(row.get("remaining_shares") or 0)
        if remaining <= 0:
            remaining = max(
                0,
                int(row.get("requested_shares") or 0)
                - int(row.get("filled_shares") or 0),
            )
        price = float(row.get("limit_price") or row.get("target_price") or 0)
        return remaining * price

    legacy_pending_buy_amount = sum(
        pending_buy_amount(row) for row in legacy_orders
    )
    legacy_unrealized_pnl = legacy_current_market_value - legacy_market_value
    legacy_cash_balance = (
        legacy_initial_cash
        + legacy_realized_pnl
        - legacy_market_value
        - legacy_pending_buy_amount
    )
    legacy_total_equity = (
        legacy_initial_cash + legacy_realized_pnl + legacy_unrealized_pnl
    )
    account_cash = account.get("cash_balance")
    v2_cash = float(account_cash if account_cash is not None else initial_cash)
    equity_cash = latest_equity.get("cash_balance")
    canonical_cash = float(equity_cash if equity_cash is not None else v2_cash)
    equity_market_value = latest_equity.get("market_value")
    canonical_market_value = float(
        equity_market_value if equity_market_value is not None else 0
    )
    equity_total = latest_equity.get("total_equity")
    canonical_total_equity = float(
        equity_total
        if equity_total is not None
        else canonical_cash + canonical_market_value
    )
    total_market_value = round(sum(float(row.get("market_value") or 0) for row in positions), 2)
    total_unrealized_pnl = round(sum(float(row.get("unrealized_pnl") or 0) for row in positions), 2)
    legacy_account_present = bool(
        legacy_capital_rows or legacy_positions or legacy_orders
    )
    if legacy_account_present and v2_positions:
        display_account_scope = "MERGED_LEDGER"
        display_cash_balance = canonical_cash + legacy_cash_balance
        display_total_equity = canonical_total_equity + legacy_total_equity
    elif legacy_account_present:
        display_account_scope = "LEGACY_EVENT_SIM_ACTIVE"
        display_cash_balance = legacy_cash_balance
        display_total_equity = legacy_total_equity
    else:
        display_account_scope = "V2_CANONICAL"
        display_cash_balance = canonical_cash
        display_total_equity = canonical_total_equity
    return _envelope({
        "account_id": account_id,
        "account": account,
        "positions": positions,
        "today_closed_positions": today_closed_positions,
        "orders": orders,
        "summary": {
            "position_count": len(positions),
            "position_lot_count": len(position_lots),
            "order_count": len(orders),
            "v2_position_count": len(v2_positions),
            "legacy_position_count": len(legacy_positions),
            "today_sold_count": len(sales_by_code),
            "today_closed_position_count": len(today_closed_positions),
            "v2_order_count": len(v2_orders),
            "legacy_order_count": len(legacy_orders),
            "cash_balance": v2_cash,
            "canonical_initial_cash": initial_cash,
            "canonical_cash_balance": round(canonical_cash, 2),
            "canonical_market_value": round(canonical_market_value, 2),
            "canonical_total_equity": round(canonical_total_equity, 2),
            "canonical_equity_trade_date": latest_equity.get("trade_date"),
            "canonical_account_name": account.get("account_name") or "V2 主模拟账户",
            "canonical_account_scope": "V2_CANONICAL_ONLY",
            "legacy_initial_cash": round(legacy_initial_cash, 2),
            "legacy_realized_pnl": round(legacy_realized_pnl, 2),
            "legacy_pending_buy_amount": round(legacy_pending_buy_amount, 2),
            "legacy_cash_balance": round(legacy_cash_balance, 2),
            "legacy_market_value": round(legacy_current_market_value, 2),
            "legacy_total_equity": round(legacy_total_equity, 2),
            "legacy_cost_market_value": round(legacy_market_value, 2),
            "current_market_value": total_market_value,
            "total_unrealized_pnl": total_unrealized_pnl,
            "display_account_scope": display_account_scope,
            "display_cash_balance": round(display_cash_balance, 2),
            "display_total_equity": round(display_total_equity, 2),
        },
        "ledger_sources": ["V2_CANONICAL", "LEGACY_EVENT_SIM"],
        "real_trading_enabled": False,
        "merge_policy": "READ_ONLY_GROUP_BY_STOCK_CODE_WEIGHTED_COST",
    })


@router.get("/validation/latest")
def latest_validation():
    result = _repo().latest_validation()
    return _envelope(result, status="ok" if result else "empty")


@router.get("/opportunity-recall/latest")
def latest_opportunity_recall():
    result = _repo().latest_opportunity_recall()
    return _envelope(result, status="ok" if result else "collecting")


@router.get("/learning/{strategy_key}")
def strategy_learning(
    strategy_key: str = ApiPath(
        ...,
        pattern=r"^[a-z0-9_]{3,64}$",
    ),
):
    result = _repo().strategy_learning_summary(strategy_key)
    return _envelope(
        result,
        status=(
            "ok" if result["observed_count"] else "collecting"
        ),
    )


@router.post("/actions/{action_key}")
def run_manual_action(
    action_key: str = ApiPath(
        ...,
        pattern=r"^(daily|intraday)$",
    ),
):
    """Launch one explicitly allow-listed paper-trading action."""
    if action_key == "intraday":
        raise HTTPException(
            status_code=409,
            detail=(
                "V3_ONLY_ROUTE: V2盘中激活已从V3生产入口隔离；"
                "V3盘中模型通过独立样本外和组合级验收前不允许手动触发买入"
            ),
        )
    task_types = {
        "daily": "trading_v3_close_decision",
    }
    engine = get_engine()
    if engine is None:
        raise HTTPException(
            status_code=503,
            detail="scheduler database unavailable",
        )
    with engine.connect() as connection:
        task = connection.execute(
            text(
                """
                SELECT *
                FROM st_scheduled_tasks
                WHERE task_type = :task_type
                LIMIT 1
                """
            ),
            {"task_type": task_types[action_key]},
        ).mappings().first()
    if not task:
        raise HTTPException(
            status_code=404,
            detail="manual action task is not registered",
        )
    row = dict(task)
    if action_key == "daily":
        row["script_args"] = (
            "--mode manual --universe-limit 5000 "
            "--per-sleeve-limit 5000"
        )
    result = launch_scheduler_task(
        row,
        root=Path(__file__).resolve().parents[3],
        engine=engine,
    )
    return _envelope(
        {
            **result,
            "action": action_key,
            "real_trading_enabled": False,
        },
        status=str(result["status"]),
    )
@router.get("/actions/jobs/{job_id}")
def manual_action_job(
    job_id: str = ApiPath(
        ...,
        pattern=r"^[a-fA-F0-9]{16,64}$",
    ),
):
    """Read the exact scheduler-history row returned by a manual action."""

    engine = get_engine()
    if engine is None:
        raise HTTPException(
            status_code=503,
            detail="scheduler database unavailable",
        )
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT run_uid, task_id, task_name, task_type,
                           run_at, finished_at, status, duration,
                           exit_code, output, trigger_source
                    FROM st_scheduled_task_history
                    WHERE run_uid = :job_id
                    LIMIT 1
                    """
                ),
                {"job_id": job_id},
            ).mappings().first()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"scheduler job history unavailable: {str(exc)[:300]}",
        ) from exc
    if not row:
        raise HTTPException(
            status_code=404,
            detail="manual action job not found",
        )
    data = dict(row)
    raw_status = str(data.get("status") or "unknown").lower()
    data["job_id"] = str(data.pop("run_uid"))
    data["state"] = {
        "completed": "succeeded",
        "success": "succeeded",
        "succeeded": "succeeded",
        "failed": "failed",
        "error": "failed",
        "timeout": "failed",
        "stopped": "cancelled",
        "cancelled": "cancelled",
        "running": "running",
        "queued": "queued",
    }.get(raw_status, raw_status)
    data["terminal"] = data["state"] in {
        "succeeded",
        "failed",
        "cancelled",
    }
    return _envelope(
        data,
        status=(
            "ok"
            if data["state"] == "succeeded"
            else str(data["state"])
        ),
    )
