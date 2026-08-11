"""V3 positive-expectancy decision APIs and protected manual actions."""
from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Path as ApiPath, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.api.scheduler_runtime import launch_scheduler_task
from server.trading_v3.config import config_hash, load_v3_config
from server.trading_v3.repository import TradingV3Repository
from server.trading_v3.versioning import code_version


router = APIRouter(prefix="/v3", tags=["trading-v3"])

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


def _repo() -> TradingV3Repository:
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V3 database unavailable")
    return TradingV3Repository(engine)


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


@router.get("/readiness")
def readiness():
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
    paper_ready = bool(
        not missing
        and (not columns or all(columns.values()))
        and all(guards.values())
        and (compatible or discovery_ready)
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


@router.get("/overview")
def overview(compact: bool = Query(default=False)):
    data = _repo().overview()
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


@router.get("/portfolio/latest")
def latest_portfolio():
    return _envelope(
        [
            _research_target_projection(row)
            for row in _repo().latest_targets()
        ]
    )


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

