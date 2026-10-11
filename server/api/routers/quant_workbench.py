"""Read-only quantitative workflow discovery and evidence projection."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from fastapi import APIRouter
from sqlalchemy import text

from biz.intelligence.exposure_risk_watch import (
    read_exposure_watch_checkpoint_progress,
)
from biz.intelligence.quant_research import load_quant_research_candidates
from server.api.routers._engine import get_engine
from server.common.exposure_risk_watch_task_contract import (
    CHECKPOINT_ROOT,
    TASK_TYPE as SCHEDULER_TASK_TYPE,
)
from server.common.quant_workbench_capabilities import (
    quant_workbench_capability_contract,
)


router = APIRouter(prefix="/quant-workbench", tags=["quant-workbench"])


def _rows(engine: Any, statement: str, params: Mapping[str, Any]) -> list[dict]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(text(statement), dict(params)).mappings().all()
        ]


def _terminal_json(output: Any) -> dict[str, Any]:
    for line in reversed(str(output or "").splitlines()):
        try:
            value = json.loads(line.strip())
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            return dict(value)
    return {}


def _complete_risk_receipt(receipt: Mapping[str, Any]) -> bool:
    try:
        total = int(receipt.get("total_units") or 0)
        completed = int(receipt.get("completed_units") or 0)
        coverage = int(receipt.get("coverage_count") or 0)
    except (TypeError, ValueError):
        return False
    no_exposure = str(receipt.get("reason") or "") == "NO_CURRENT_EXPOSURE"
    return bool(
        receipt.get("status") == "COMPLETE"
        and receipt.get("coverage_readback") is True
        and receipt.get("automatic_real_order_submission") is False
        and receipt.get("real_order_authority") is False
        and total == completed
        and (no_exposure and total == 0 or total > 0 and coverage == total)
    )


def _project_risk_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    allowed = (
        "schema",
        "status",
        "reason",
        "stage",
        "batch_id",
        "batch_root_hash",
        "fact_cutoff_at",
        "received_at",
        "total_units",
        "completed_units",
        "current_unit",
        "last_progress_at",
        "event_count",
        "coverage_count",
        "coverage_readback",
        "database_writes",
        "risk_hit_count",
        "risk_hits",
        "automatic_real_order_submission",
        "real_order_authority",
    )
    return {key: receipt[key] for key in allowed if key in receipt}


def load_risk_watch_status(
    engine: Any,
    *,
    checkpoint_root: Path = Path(CHECKPOINT_ROOT),
) -> dict[str, Any]:
    """Project scheduler identity, durable progress and business completion."""

    try:
        tasks = _rows(
            engine,
            """
            SELECT id, task_name, task_type, enabled, last_run_status,
                   last_run_at, last_triggered_at, last_run_duration,
                   last_run_output
              FROM st_scheduled_tasks
             WHERE task_type=:task_type
             ORDER BY id
             LIMIT 2
            """,
            {"task_type": SCHEDULER_TASK_TYPE},
        )
    except Exception:
        return {
            "status": "BLOCKED",
            "reason": "SCHEDULER_REGISTRY_UNAVAILABLE",
            "task_type": SCHEDULER_TASK_TYPE,
            "registration_count": 0,
        }
    if len(tasks) != 1:
        return {
            "status": "BLOCKED",
            "reason": (
                "TASK_REGISTRATION_MISSING"
                if not tasks
                else "TASK_REGISTRATION_AMBIGUOUS"
            ),
            "task_type": SCHEDULER_TASK_TYPE,
            "registration_count": len(tasks),
        }
    task = tasks[0]
    try:
        histories = _rows(
            engine,
            """
            SELECT run_uid, status, run_at, finished_at, host_name,
                   scheduler_instance_id, build_sha, trigger_source,
                   exit_code, output
              FROM st_scheduled_task_history
             WHERE task_type=:task_type
             ORDER BY run_at DESC, id DESC
             LIMIT 1
            """,
            {"task_type": SCHEDULER_TASK_TYPE},
        )
    except Exception:
        histories = []
    history = histories[0] if histories else {}
    history_matches_task = bool(
        history
        and str(history.get("run_at") or "")[:19]
        == str(task.get("last_run_at") or "")[:19]
    )
    receipt = _terminal_json(
        history.get("output") or task.get("last_run_output")
    )
    task_status = str(task.get("last_run_status") or "").strip().lower()
    history_status = str(history.get("status") or "").strip().lower()
    scheduler_status = (
        task_status
        if task_status == "running" or not history_matches_task
        else history_status or task_status
    )
    if scheduler_status == "running":
        business_status = "RUNNING"
        reason = "BUSINESS_JOB_RUNNING"
    elif scheduler_status in {"success", "completed"}:
        business_status = (
            "COMPLETE"
            if history_matches_task and _complete_risk_receipt(receipt)
            else "FAILED"
        )
        reason = "" if business_status == "COMPLETE" else "BUSINESS_RECEIPT_UNPROVEN"
    elif scheduler_status in {"failed", "timeout", "stopped", "cancelled"}:
        business_status = "FAILED"
        reason = str(receipt.get("stage") or scheduler_status).upper()
    elif not scheduler_status:
        business_status = "NOT_STARTED"
        reason = "NO_RUN_HISTORY"
    else:
        business_status = "BLOCKED"
        reason = f"UNKNOWN_SCHEDULER_STATUS:{scheduler_status}"
    try:
        checkpoint = read_exposure_watch_checkpoint_progress(checkpoint_root)
    except Exception:
        checkpoint = {
            "status": "BLOCKED",
            "reason": "CHECKPOINT_UNAVAILABLE",
            "total_units": 0,
            "completed_units": 0,
            "current_unit": None,
            "last_progress_at": None,
            "checkpoint_complete": False,
        }
    return {
        "status": business_status,
        "reason": reason,
        "task_type": SCHEDULER_TASK_TYPE,
        "task_id": task.get("id"),
        "task_name": str(task.get("task_name") or ""),
        "enabled": bool(task.get("enabled")),
        "scheduler_status": scheduler_status or "not_started",
        "last_run_at": task.get("last_run_at"),
        "last_triggered_at": task.get("last_triggered_at"),
        "last_run_duration": task.get("last_run_duration"),
        "run_identity": {
            "run_uid": str(history.get("run_uid") or ""),
            "host_name": str(history.get("host_name") or ""),
            "scheduler_instance_id": str(
                history.get("scheduler_instance_id") or ""
            ),
            "build_sha": str(history.get("build_sha") or ""),
            "trigger_source": str(history.get("trigger_source") or ""),
            "matches_task_summary": history_matches_task,
        },
        "progress": checkpoint,
        "receipt": _project_risk_receipt(receipt),
        "completion_proven": business_status == "COMPLETE",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_research_status(engine: Any) -> dict[str, Any]:
    try:
        result = load_quant_research_candidates(engine, limit=20)
    except Exception:
        return {
            "status": "BLOCKED",
            "reason": "QUANT_RESEARCH_LEDGER_UNAVAILABLE",
            "candidate_count": 0,
            "candidates": [],
            "research_only": True,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    projected = []
    for candidate in result.get("candidates") or []:
        reproducibility = str(
            candidate.get("reproducibility_status") or "NOT_REPRODUCED"
        )
        independently_reproduced = (
            reproducibility == "INDEPENDENTLY_REPRODUCED"
        )
        qmt_feasible = candidate.get("qmt_feasibility") != "NOT_FEASIBLE"
        projected.append({
            **dict(candidate),
            "source_reported_metrics": dict(
                candidate.get("performance_metrics") or {}
            ),
            "system_validation": {
                "status": str(
                    reproducibility
                ),
                "metrics": None,
                "completed": independently_reproduced,
            },
            "research_workflow": {
                "stage": (
                    "GOVERNANCE_PROPOSAL_REQUIRED"
                    if independently_reproduced and qmt_feasible
                    else "REPRODUCTION_REQUIRED"
                ),
                "next_gate": (
                    "FROZEN_STRATEGY_CHALLENGER"
                    if independently_reproduced and qmt_feasible
                    else "INDEPENDENT_REPRODUCTION"
                ),
                "governance_entry_tab": "strategy-center",
                "automatic_transition": False,
                "real_order_authority": False,
            },
        })
    return {**result, "candidates": projected}


@router.get("")
def quant_workbench_overview():
    engine = get_engine()
    return {
        **quant_workbench_capability_contract(),
        "status": "AVAILABLE",
        "evidence": {
            "risk_watch": load_risk_watch_status(engine),
            "quant_research": load_research_status(engine),
        },
        "execution_policy": {
            "scheduler_authority": "standalone_scheduler",
            "manual_run_available": False,
            "reason": "PERSISTENT_MANUAL_REQUEST_CONTRACT_NOT_AVAILABLE",
            "accepted_is_completion": False,
        },
    }


__all__ = [
    "load_research_status",
    "load_risk_watch_status",
    "quant_workbench_overview",
    "router",
]
