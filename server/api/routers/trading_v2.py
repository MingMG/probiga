"""Read-only V2 trading APIs. Recalculation is never performed by GET."""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.engine.strategy_center import (
    load_membership_snapshot_history,
    load_qmt_kline_attestation_status,
)
from server.trading_v2.policy import load_portfolio_policy
from server.trading_v2.paper_configuration import (
    is_internal_paper_configuration,
)
from server.trading_v2.repository import TradingV2ReadRepository
from server.trading_v2.jobs import enqueue_job, transition_strategy


router = APIRouter(prefix="/v2", tags=["trading-v2"])
logger = logging.getLogger(__name__)

_NEW_BUY_ACTIONS = frozenset({"BUY", "OPEN", "ADD", "BUY_READY"})
_ACTIONABLE_SIGNAL_STATUSES = frozenset({"CONFIRM", "BUY_READY"})
_BUY_COMPETITION_STATUSES = frozenset(
    {"ELIGIBLE", "PAPER_TRIAL_ELIGIBLE"}
)


def _degraded_read_error(operation: str, exc: Exception) -> dict[str, str]:
    """Log private diagnostics while returning a stable public error."""

    incident_id = uuid.uuid4().hex
    logger.error(
        "Trading V2 read failed: incident_id=%s exception_type=%s operation=%s",
        incident_id,
        type(exc).__name__,
        operation,
    )
    return {
        "error_code": f"{operation}_unavailable",
        "error": "数据暂不可用，请稍后重试",
        "incident_id": incident_id,
    }


class BacktestJobRequest(BaseModel):
    strategy_version: str = Field(min_length=3, max_length=160)
    start_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    random_seed: int


class DecisionJobRequest(BaseModel):
    trade_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    account_id: str = Field(default="paper-main-v2", min_length=3, max_length=64)


class LifecycleRequest(BaseModel):
    strategy_version: str = Field(min_length=3, max_length=160)
    reason: str = Field(min_length=3, max_length=500)
    operator: str = Field(default="admin", min_length=2, max_length=80)


def _explicit_database_true(value: Any) -> bool:
    return value is True or (type(value) is int and value == 1)


def _candidate_display_projection(row: dict[str, Any]) -> dict[str, Any]:
    """Keep research scores visible without exposing an unqualified BUY label."""

    projected = dict(row)
    action = str(projected.get("action") or "WATCH").upper()
    projected["source_action"] = action
    projected["new_buy_eligible"] = False
    if action in {"SELL", "REDUCE", "EXIT"}:
        projected["display_action"] = action
        return projected
    if action not in _NEW_BUY_ACTIONS:
        projected["display_action"] = action
        return projected

    raw = projected.get("raw_features")
    raw = raw if isinstance(raw, dict) else {}
    recommend_status = str(
        raw.get("source_recommend_status")
        or raw.get("recommend_status")
        or "DATA_BLOCKED"
    ).upper()
    signal_status = str(
        raw.get("source_signal_status") or "WATCH"
    ).upper()
    chase_status = str(
        raw.get("source_chase_risk_status")
        or raw.get("chase_risk_status")
        or "DATA_BLOCKED"
    ).upper()
    ordinary_eligible = _explicit_database_true(
        raw.get(
            "source_ordinary_buy_eligible",
            raw.get("ordinary_buy_eligible"),
        )
    )
    competition_status = str(
        projected.get("competition_status") or ""
    ).upper()
    eligible = bool(
        recommend_status == "ALLOW"
        and signal_status in _ACTIONABLE_SIGNAL_STATUSES
        and chase_status == "ALLOW"
        and ordinary_eligible
        and competition_status in _BUY_COMPETITION_STATUSES
        and projected.get("rejection_code") in {None, ""}
    )
    projected.update(
        {
            "canonical_recommend_status": recommend_status,
            "canonical_signal_status": signal_status,
            "canonical_chase_risk_status": chase_status,
            "canonical_ordinary_buy_eligible": ordinary_eligible,
            "new_buy_eligible": eligible,
        }
    )
    if eligible:
        safe_action = "ADD" if action == "ADD" else "BUY_READY"
    else:
        safe_action = (
            "RESEARCH_ONLY"
            if competition_status == "RESEARCH_ONLY"
            else "WATCH"
        )
    projected["action"] = safe_action
    projected["display_action"] = safe_action
    return projected


def _repo() -> TradingV2ReadRepository:
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V2 database is unavailable")
    return TradingV2ReadRepository(engine)


def _envelope(
    data: Any,
    *,
    status: str = "ok",
    snapshot: dict[str, Any] | None = None,
    config_version: str = "",
    code_commit_sha: str = "",
) -> dict[str, Any]:
    policy = load_portfolio_policy()
    snapshot = snapshot or {}
    return {
        "status": status,
        "trace_id": uuid.uuid4().hex,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "data_snapshot_id": str(snapshot.get("snapshot_id") or ""),
        "data_snapshot_hash": str(snapshot.get("data_snapshot_hash") or ""),
        "code_commit_sha": str(
            code_commit_sha
            or snapshot.get("code_commit_sha")
            or os.getenv("PROBIGA_BUILD_COMMIT_SHA", "WORKTREE")
        ),
        "config_version": config_version or policy.version,
        "data": data,
    }


@router.get("/system/readiness")
def readiness():
    repository = _repo()
    tables = repository.table_readiness()
    missing_tables = [name for name, exists in tables.items() if not exists]
    snapshot = (
        repository.latest_snapshot()
        if tables.get("st_data_snapshot_v2")
        else None
    )
    account = (
        repository.account("paper-main-v2")
        if tables.get("st_trade_account_v2")
        and tables.get("st_equity_daily_v2")
        and tables.get("st_reconciliation_v2")
        else None
    )
    paper_blocks: list[str] = []
    if missing_tables:
        paper_blocks.append("V2_SCHEMA_INCOMPLETE")
    if not snapshot:
        paper_blocks.append("DATA_SNAPSHOT_MISSING")
    elif snapshot.get("quality_status") != "PASS":
        paper_blocks.extend(
            snapshot.get("blocked_capabilities")
            or ["DATA_QUALITY_BLOCK"]
        )
    level1_capability = (
        repository.execution_capability(
            "B-003_RELIABLE_LEVEL1_BID_ASK"
        )
        if tables.get("st_execution_capability_v2")
        else None
    )
    fee_confirmation = (
        repository.fee_profile_confirmation(
            str(account.get("fee_profile_version") or "")
        )
        if account and tables.get("st_fee_profile_v2")
        else None
    )
    actual_fee_confirmed = (
        int(
            (fee_confirmation or {}).get(
                "confirmed_required_type_count"
            )
            or 0
        )
        >= 2
    )
    real_trading_blocks: list[str] = []
    if not account:
        paper_blocks.append("V2_ACCOUNT_MISSING")
        real_trading_blocks.extend(
            [
                "B-001_ACTUAL_BROKER_FEES",
                "B-002_ACCOUNT_INSTRUMENT_PERMISSIONS",
            ]
        )
    else:
        if not account.get("fee_profile_version"):
            paper_blocks.append("PAPER_FEE_PROFILE_MISSING")
        if not account.get("instrument_rule_version"):
            paper_blocks.append("PAPER_INSTRUMENT_RULES_MISSING")
        if account.get("status") != "ACTIVE":
            paper_blocks.append(
                f"ACCOUNT_{account.get('status') or 'UNKNOWN'}"
            )
        if (
            account.get("latest_reconciliation")
            and account["latest_reconciliation"].get("status") != "PASS"
        ):
            paper_blocks.append("RECONCILIATION_BLOCKED")
        if is_internal_paper_configuration(account):
            if not actual_fee_confirmed:
                real_trading_blocks.append(
                    "B-001_ACTUAL_BROKER_FEES"
                )
            real_trading_blocks.append(
                "B-002_ACCOUNT_INSTRUMENT_PERMISSIONS"
            )
    if (
        not level1_capability
        or level1_capability.get("status") != "PASS"
    ):
        real_trading_blocks.append(
            "B-003_RELIABLE_LEVEL1_BID_ASK"
        )
    strategies = (
        repository.strategies()
        if tables.get("st_strategy_version_v2")
        else []
    )
    if not any(
        item.get("lifecycle_status")
        in {"PAPER_TRIAL", "PAPER_ACTIVE"}
        for item in strategies
    ):
        paper_blocks.append("NO_PAPER_ENABLED_STRATEGY")
    worker_heartbeats = (
        repository.worker_heartbeats()
        if tables.get("st_worker_heartbeat_v2")
        else []
    )
    job_worker = next(
        (
            item
            for item in worker_heartbeats
            if item.get("worker_name") == "trading-v2-job-worker"
        ),
        None,
    )
    if not job_worker:
        paper_blocks.append("V2_JOB_WORKER_MISSING")
    else:
        raw_heartbeat = job_worker.get("heartbeat_at")
        try:
            heartbeat_at = datetime.fromisoformat(
                str(raw_heartbeat).replace(" ", "T")
            )
        except (TypeError, ValueError):
            heartbeat_at = datetime.min
        if datetime.now() - heartbeat_at > timedelta(minutes=3):
            paper_blocks.append("V2_JOB_WORKER_STALE")
    regime = (
        repository.latest_regime()
        if tables.get("st_decision_run_v2")
        else None
    )
    market_blocks: list[str] = []
    market_regime = str(
        (regime or {}).get("market_regime") or "DATA_BLOCKED"
    )
    if market_regime in {"EXTREME", "DATA_BLOCKED"}:
        market_blocks.append(f"MARKET_REGIME_{market_regime}")
    paper_blocks = sorted(set(paper_blocks))
    market_blocks = sorted(set(market_blocks))
    real_trading_blocks = sorted(set(real_trading_blocks))
    paper_infrastructure_ready = not paper_blocks
    ready_for_new_positions = (
        paper_infrastructure_ready and not market_blocks
    )
    data = {
        "ready_for_research": not missing_tables,
        "paper_infrastructure_ready": paper_infrastructure_ready,
        "ready_for_new_positions": ready_for_new_positions,
        "real_trading_enabled": False,
        "execution_mode": "PROBIGA_INTERNAL_PAPER",
        "paper_price_policy": {
            "primary": "QMT_LEVEL1",
            "fallback": "FRESH_QMT_SNAPSHOT_WITH_FROZEN_SLIPPAGE",
            "broker_fill": False,
        },
        "schema": tables,
        "account": account,
        "fee_confirmation": fee_confirmation,
        "execution_capabilities": {
            "B-003_RELIABLE_LEVEL1_BID_ASK": level1_capability
            if account
            else None
        },
        "workers": worker_heartbeats,
        "paper_blocks": paper_blocks,
        "market_blocks": market_blocks,
        "real_trading_blocks": real_trading_blocks,
        "blocks": sorted(set(paper_blocks + market_blocks)),
    }
    return _envelope(
        data,
        status="ok" if paper_infrastructure_ready else "blocked",
        snapshot=snapshot,
    )


@router.get("/market-regime/latest")
def latest_market_regime():
    repository = _repo()
    snapshot = repository.latest_snapshot()
    regime = repository.latest_regime()
    return _envelope(
        regime,
        status="ok" if regime else "empty",
        snapshot=snapshot,
        config_version=str((regime or {}).get("market_regime_version") or ""),
        code_commit_sha=str((regime or {}).get("code_commit_sha") or ""),
    )


@router.get("/strategies")
def strategies():
    repository = _repo()
    snapshot = repository.latest_snapshot()
    return _envelope(repository.strategies(), snapshot=snapshot)


@router.get("/decision-runs")
def decision_runs(
    trade_date: str = Query(
        default="",
        pattern=r"^(|\d{4}-\d{2}-\d{2})$",
    ),
    limit: int = Query(default=200, ge=1, le=500),
):
    repository = _repo()
    snapshot = repository.latest_snapshot()
    return _envelope(
        repository.decision_runs(
            trade_date=trade_date,
            limit=limit,
        ),
        snapshot=snapshot,
    )


@router.get("/decision-runs/{run_uid}")
def decision_run(run_uid: str = Path(..., min_length=8, max_length=64)):
    repository = _repo()
    snapshot = repository.latest_snapshot()
    data = repository.decision_run(run_uid)
    if data is None:
        raise HTTPException(status_code=404, detail="decision run not found")
    return _envelope(
        data,
        snapshot=snapshot,
        config_version=str(data.get("config_version") or ""),
        code_commit_sha=str(data.get("code_commit_sha") or ""),
    )


@router.get("/candidates")
def candidates(
    run_uid: str = Query(default="", max_length=64),
    limit: int = Query(default=200, ge=1, le=500),
):
    repository = _repo()
    snapshot = repository.latest_snapshot()
    rows = repository.candidates(run_uid=run_uid, limit=limit)
    return _envelope(
        [_candidate_display_projection(row) for row in rows],
        snapshot=snapshot,
    )


@router.get("/accounts/{account_id}/intraday")
def intraday_decisions(
    account_id: str = Path(..., min_length=3, max_length=64),
    limit: int = Query(default=200, ge=1, le=500),
):
    repository = _repo()
    try:
        data = repository.intraday_summary(
            account_id=account_id,
            limit=limit,
        )
        status = str(data.get("status") or "ok")
    except Exception as exc:
        data = {
            "status": "degraded",
            "market_state": None,
            "decisions": [],
            "decision_count": 0,
            "order_created_count": 0,
            "automatic_real_order_submission": False,
            **_degraded_read_error("intraday_summary", exc),
        }
        status = "degraded"
    return _envelope(
        data,
        status=status,
        snapshot=repository.latest_snapshot(),
    )


@router.get("/accounts/{account_id}")
def account(account_id: str = Path(..., min_length=3, max_length=64)):
    repository = _repo()
    snapshot = repository.latest_snapshot()
    data = repository.account(account_id)
    if data is None:
        raise HTTPException(status_code=404, detail="V2 account not found")
    return _envelope(data, snapshot=snapshot)


@router.get("/accounts/{account_id}/plan")
def plan(account_id: str):
    repository = _repo()
    snapshot = repository.latest_snapshot()
    current = repository.current_plan(account_id)
    return _envelope(
        current,
        status="ok" if current else "empty",
        snapshot=snapshot,
    )


@router.get("/accounts/{account_id}/positions")
def positions(account_id: str):
    repository = _repo()
    return _envelope(repository.positions(account_id), snapshot=repository.latest_snapshot())


@router.get("/accounts/{account_id}/orders")
def orders(account_id: str, limit: int = Query(default=200, ge=1, le=500)):
    repository = _repo()
    return _envelope(repository.orders(account_id, limit), snapshot=repository.latest_snapshot())


@router.get("/accounts/{account_id}/fills")
def fills(account_id: str, limit: int = Query(default=200, ge=1, le=500)):
    repository = _repo()
    return _envelope(repository.fills(account_id, limit), snapshot=repository.latest_snapshot())


@router.get("/accounts/{account_id}/cash-ledger")
def cash_ledger(account_id: str, limit: int = Query(default=500, ge=1, le=1000)):
    repository = _repo()
    return _envelope(repository.cash_ledger(account_id, limit), snapshot=repository.latest_snapshot())


@router.get("/accounts/{account_id}/reconciliation")
def reconciliation(account_id: str, limit: int = Query(default=100, ge=1, le=500)):
    repository = _repo()
    return _envelope(repository.reconciliations(account_id, limit), snapshot=repository.latest_snapshot())


@router.get("/reports/daily")
def daily_report(limit: int = Query(default=100, ge=1, le=500)):
    repository = _repo()
    return _envelope(repository.daily_reports(limit), snapshot=repository.latest_snapshot())


@router.get("/jobs/{job_id}")
def job_status(job_id: str = Path(..., min_length=8, max_length=64)):
    repository = _repo()
    data = repository.job(job_id)
    if data is None:
        raise HTTPException(status_code=404, detail="V2 job not found")
    return _envelope(data, snapshot=repository.latest_snapshot())


@router.get("/research/backtests/{backtest_uid}")
def backtest_result(
    backtest_uid: str = Path(..., min_length=8, max_length=64),
):
    repository = _repo()
    data = repository.backtest(backtest_uid)
    if data is None:
        raise HTTPException(status_code=404, detail="V2 backtest not found")
    return _envelope(
        data,
        snapshot=repository.latest_snapshot(),
        code_commit_sha=str(data.get("code_commit_sha") or ""),
    )


@router.get("/system/workers")
def worker_status():
    repository = _repo()
    return _envelope(
        repository.worker_heartbeats(),
        snapshot=repository.latest_snapshot(),
    )


@router.get("/operations/tomorrow")
def tomorrow_action(account_id: str = Query(default="paper-main-v2")):
    repository = _repo()
    snapshot = repository.latest_snapshot()
    data = repository.tomorrow_action(account_id)
    return _envelope(
        data,
        status="ok" if data.get("execution_trade_date") else "empty",
        snapshot=snapshot,
    )


@router.get("/research/etf-forward")
def etf_forward(limit: int = Query(default=100, ge=1, le=500)):
    repository = _repo()
    snapshot = repository.latest_snapshot()
    try:
        data = repository.etf_forward_summary(limit)
        status = str(data.get("status") or "ok")
    except Exception as exc:
        data = {
            "status": "degraded",
            **_degraded_read_error("etf_forward", exc),
            "strategies": [],
            "observations": [],
            "observation_count": 0,
            "security_names": {},
            "automatic_order_submission": False,
        }
        status = "degraded"
    return _envelope(data, status=status, snapshot=snapshot)


@router.get("/system/data-evidence")
def data_evidence():
    """Expose component truth without flattening nested errors into OK."""

    repository = _repo()
    snapshot = repository.latest_snapshot()
    errors: list[str] = []
    try:
        qmt_attestation = load_qmt_kline_attestation_status(limit=3)
    except Exception as exc:
        qmt_attestation = {"status": "degraded", "runs": []}
        _degraded_read_error("qmt_attestation", exc)
        errors.append("qmt_attestation_unavailable")
    membership: dict[str, Any] = {}
    for member_type in ("concept", "industry"):
        try:
            membership[member_type] = load_membership_snapshot_history(
                member_type=member_type,
                limit=1,
            )
        except Exception as exc:
            membership[member_type] = {
                "status": "degraded",
                "data_category": "POINT_IN_TIME_CONSTITUENT_MEMBERSHIP",
                "data_category_label": (
                    "概念成分归属历史"
                    if member_type == "concept"
                    else "行业成分归属历史"
                ),
                "excluded_data_categories": [
                    "SECTOR_HEAT_HISTORY",
                    "SECTOR_ROTATION_HISTORY",
                ],
                "runs": [],
                "data": [],
                "snapshot_complete": False,
            }
            _degraded_read_error(f"{member_type}_membership", exc)
            errors.append(f"{member_type}_membership_unavailable")
    level1 = repository.execution_capability(
        "B-003_RELIABLE_LEVEL1_BID_ASK"
    )
    component_status = {
        "qmt_kline_attestation": str(
            qmt_attestation.get("status") or "unavailable"
        ).lower(),
        "concept_membership": str(
            (membership.get("concept") or {}).get("status")
            or "unavailable"
        ).lower(),
        "industry_membership": str(
            (membership.get("industry") or {}).get("status")
            or "unavailable"
        ).lower(),
        "level1_execution_capability": str(
            (level1 or {}).get("status") or "unavailable"
        ).upper(),
    }
    component_issues: list[str] = []
    if component_status["qmt_kline_attestation"] != "complete":
        component_issues.append(
            "QMT旧日K线逐行补证尚未完整"
        )
    for member_type in ("concept", "industry"):
        result = membership.get(member_type) or {}
        if (
            str(result.get("status") or "").lower() != "verified"
            or result.get("snapshot_complete") is not True
        ):
            component_issues.append(
                f"{member_type}成员快照未完整验真"
            )
    membership_and_kline_history_ready = not errors and not component_issues
    excluded_historical_scopes = [
        "SECTOR_HEAT_HISTORY",
        "SECTOR_ROTATION_HISTORY",
        "QMT_NATIVE_SECTOR_INDEX_REALTIME",
        "QMT_NATIVE_SECTOR_INDEX_MINUTE",
        "QMT_NATIVE_SECTOR_INDEX_DAILY_HISTORY",
    ]
    data = {
        "qmt_kline_attestation": qmt_attestation,
        "membership": membership,
        "level1": level1,
        "errors": errors,
        "component_status": component_status,
        "component_issues": component_issues,
        "membership_and_kline_history_ready": (
            membership_and_kline_history_ready
        ),
        # Backward-compatible bool. Its exact, deliberately narrow scope is
        # published beside it; it must never be interpreted as all industry
        # history being available.
        "historical_data_ready": membership_and_kline_history_ready,
        "historical_data_ready_scope": (
            "QMT_KLINE_ATTESTATION_AND_POINT_IN_TIME_MEMBERSHIP_ONLY"
        ),
        "verified_historical_scopes": [
            "QMT_DAILY_KLINE_ATTESTATION",
            "POINT_IN_TIME_CONCEPT_MEMBERSHIP",
            "POINT_IN_TIME_INDUSTRY_MEMBERSHIP",
        ],
        "all_historical_data_ready": False,
        "unverified_or_excluded_historical_scopes": (
            excluded_historical_scopes
        ),
        "membership_data_boundary": {
            "category": "POINT_IN_TIME_CONSTITUENT_MEMBERSHIP",
            "description": "概念/行业成分归属快照，与热度历史、轮动历史分开",
            "excluded_categories": excluded_historical_scopes,
        },
        "industry_history_evidence_categories": {
            "point_in_time_constituent_membership": {
                "label": "行业/概念成分归属历史",
                "semantics": "指定日期的证券与板块归属关系，不含板块强弱或价格",
                "ready": membership_and_kline_history_ready,
                "source": "QMT_POINT_IN_TIME_MEMBERSHIP_SNAPSHOT",
                "native_sector_index": False,
            },
            "source_specific_sector_heat_history": {
                "label": "来源特定板块热度历史",
                "semantics": "第三方来源自己的热度/排名口径，不等于QMT板块指数",
                "ready": False,
                "status": "NOT_VERIFIED_BY_THIS_ENDPOINT",
                "native_sector_index": False,
            },
            "constituent_aggregated_strength_history": {
                "label": "成分股聚合强弱历史",
                "semantics": "由成分股行情按冻结成员集合派生，不是原生板块指数价格",
                "ready": False,
                "status": "NOT_VERIFIED_BY_THIS_ENDPOINT",
                "native_sector_index": False,
            },
            "qmt_native_bkzs_index_history": {
                "label": "QMT原生.BKZS板块指数历史",
                "semantics": "仅接受完成代码识别、合约和逐行行情认证的原生指数数据",
                "ready": False,
                "status": "NOT_ATTESTED",
                "synthetic_substitution_allowed": False,
                "native_sector_index": True,
            },
        },
    }
    return _envelope(
        data,
        status="ok" if membership_and_kline_history_ready else "degraded",
        snapshot=snapshot,
    )


@router.get("/system/operations")
def system_operations():
    repository = _repo()
    snapshot = repository.latest_snapshot()
    try:
        data = repository.operations_summary()
        status = "ok"
    except Exception as exc:
        data = {
            "tasks": [],
            "backtests": [],
            "jobs": [],
            "workers": [],
            "real_trading_guards": [],
            "running_backtest_count": None,
            **_degraded_read_error("operations", exc),
        }
        status = "degraded"
    return _envelope(data, status=status, snapshot=snapshot)


@router.post("/research/backtests", status_code=202)
def create_backtest_job(payload: BacktestJobRequest):
    if payload.end_date < payload.start_date:
        raise HTTPException(status_code=422, detail="end_date must not precede start_date")
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V2 database is unavailable")
    job = enqueue_job(
        engine,
        job_type="BACKTEST",
        request=payload.model_dump(),
        requested_by="api-admin",
    )
    return _envelope(job, status="accepted")


@router.post("/decision-runs", status_code=202)
def create_decision_job(payload: DecisionJobRequest):
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V2 database is unavailable")
    job = enqueue_job(
        engine,
        job_type="DECISION_RUN",
        request=payload.model_dump(),
        requested_by="api-admin",
    )
    return _envelope(job, status="accepted")


@router.post("/admin/strategies/{strategy_id}/promote")
def promote_strategy(
    payload: LifecycleRequest,
    strategy_id: str = Path(..., min_length=2, max_length=80),
):
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V2 database is unavailable")
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT lifecycle_status FROM st_strategy_version_v2
                WHERE strategy_id = :strategy_id AND version = :version
                """
            ),
            {"strategy_id": strategy_id, "version": payload.strategy_version},
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="strategy version not found")
    next_by_status = {
        "DRAFT_BLOCKED": "DRAFT",
        "DRAFT": "RESEARCH",
        "RESEARCH": "OOS_PASSED",
        "OOS_PASSED": "SHADOW",
        "SHADOW": "PAPER_ACTIVE",
    }
    next_status = next_by_status.get(str(row["lifecycle_status"]))
    if not next_status:
        raise HTTPException(status_code=409, detail="strategy cannot be promoted from current state")
    try:
        data = transition_strategy(
            engine,
            strategy_id=strategy_id,
            strategy_version=payload.strategy_version,
            next_status=next_status,
            reason=payload.reason,
            operator=payload.operator,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _envelope(data)
@router.post("/admin/strategies/{strategy_id}/suspend")
def suspend_strategy(
    payload: LifecycleRequest,
    strategy_id: str = Path(..., min_length=2, max_length=80),
):
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V2 database is unavailable")
    try:
        data = transition_strategy(
            engine,
            strategy_id=strategy_id,
            strategy_version=payload.strategy_version,
            next_status="SUSPENDED",
            reason=payload.reason,
            operator=payload.operator,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _envelope(data)
