"""Quantitative workflow controls and evidence projection."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from biz.intelligence.exposure_risk_watch import (
    read_exposure_watch_checkpoint_progress,
)
from biz.intelligence.quant_research import load_quant_research_candidates
from server.api.qmt_strategy_results import read_strategy_results
from server.api.scheduler_runtime import (
    launch_scheduler_task,
    read_release_analysis_recovery_status,
    release_analysis_recovery_readiness,
    request_release_analysis_recovery,
    request_stop_owned_scheduler_task,
    scheduler_task_owned_by_current_host,
)
from server.api.routers._engine import get_engine
from server.api.routers.market_outlook import build_market_outlook
from server.common.analysis_pool_receipt import is_executable_recommendation
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.exposure_risk_watch_task_contract import (
    CHECKPOINT_ROOT,
    TASK_TYPE as SCHEDULER_TASK_TYPE,
)
from server.common.quant_workbench_capabilities import (
    quant_workbench_capability_contract,
)
from server.common.quant_workbench_lifecycle import (
    build_quant_workbench_lifecycle,
)
from server.common.quant_workbench_operations import (
    DIRECT_EXECUTION,
    SCHEDULER_TARGET_EXECUTION,
    direct_operation_task_types,
    quant_workbench_operation,
    quant_workbench_operation_contract,
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


def _datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    raw = str(value or "").strip()
    if not raw:
        return None
    for candidate in (raw.replace("Z", "+00:00"), raw.replace(" ", "T")):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        return parsed.replace(tzinfo=None)
    return None


def _running_is_fresh(value: Any, *, hours: int = 24) -> bool:
    started = _datetime_value(value)
    if started is None:
        return False
    age_seconds = (datetime.now() - started).total_seconds()
    return -300 <= age_seconds <= hours * 3600


def _project_receipt(output: Any) -> dict[str, Any]:
    receipt = _terminal_json(output)
    fields = (
        "schema",
        "status",
        "task_type",
        "target_trade_date",
        "trade_date",
        "run_uid",
        "analysis_count",
        "recommendation_count",
        "canonical_pool_sha256",
        "ready",
        "automatic_real_order_submission",
        "real_order_authority",
    )
    return {key: receipt[key] for key in fields if key in receipt}


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _operation_state(
    scheduler_status: str,
    run_at: Any,
) -> tuple[str, str]:
    normalized = str(scheduler_status or "").strip().lower()
    if normalized in {"running", "submitted", "queued"}:
        if _running_is_fresh(run_at):
            return "RUNNING", ""
        return "STALE_RUNNING", "运行状态超过 24 小时未结束，需要先核对或停止"
    if normalized in {"success", "done", "completed"}:
        return "SUCCEEDED", ""
    if normalized in {"failed", "error"}:
        return "FAILED", ""
    if normalized in {"timeout", "stopped", "cancelled"}:
        return "FAILED", normalized.upper()
    if normalized in {"blocked", "degraded", "skipped"}:
        return "BLOCKED", normalized.upper()
    if normalized in {"pending", "not_started"}:
        return "NOT_STARTED", ""
    if not normalized:
        return "NOT_STARTED", ""
    return "BLOCKED", f"UNKNOWN_SCHEDULER_STATUS:{normalized}"


def _load_registered_operation(engine: Any, spec: Mapping[str, Any]) -> dict[str, Any]:
    task_type = str(spec.get("task_type") or "").strip()
    tasks = _rows(
        engine,
        """
        SELECT id, task_name, task_type, enabled, cron_time, script_path,
               script_args, date_param, last_run_status, last_run_at,
               last_triggered_at, last_run_duration, last_run_output
          FROM st_scheduled_tasks
         WHERE task_type=:task_type
         ORDER BY id
        """,
        {"task_type": task_type},
    )
    registration_count = len(tasks)
    task = tasks[0] if registration_count == 1 else {}
    histories: list[dict[str, Any]] = []
    if registration_count == 1:
        histories = _rows(
            engine,
            """
            SELECT run_uid, task_id, status, run_at, finished_at, host_name,
                   scheduler_instance_id, build_sha, trigger_source,
                   exit_code, output
              FROM st_scheduled_task_history
             WHERE task_type=:task_type
             ORDER BY run_at DESC, id DESC
             LIMIT 1
            """,
            {"task_type": task_type},
        )
    history = histories[0] if histories else {}
    scheduler_status = (
        str(task.get("last_run_status") or history.get("status") or "")
        if task
        else str(history.get("status") or "")
    )
    run_at = (
        task.get("last_run_at")
        or history.get("run_at")
        or task.get("last_triggered_at")
    )
    state, state_reason = _operation_state(scheduler_status, run_at)
    running = state == "RUNNING"
    stoppable = state in {"RUNNING", "STALE_RUNNING"}

    blocker = ""
    if registration_count == 0:
        blocker = "任务尚未注册"
    elif registration_count > 1:
        blocker = "任务注册不唯一，已拒绝执行"
    elif int(task.get("enabled") or 0) != 1:
        blocker = "任务未启用"
    elif state == "STALE_RUNNING":
        blocker = state_reason
    elif running:
        blocker = "任务正在运行"

    execution_mode = str(spec.get("execution_mode") or "")
    owned = bool(task) and scheduler_task_owned_by_current_host(task)
    if (
        not blocker
        and execution_mode == DIRECT_EXECUTION
        and not owned
    ):
        blocker = "任务由另一运行时节点持有，需要在对应节点执行"
    if (
        not blocker
        and execution_mode == DIRECT_EXECUTION
        and task_type not in direct_operation_task_types()
    ):
        blocker = "任务不在工作台直接运行白名单"

    return {
        **dict(spec),
        "registration_count": registration_count,
        "task_id": task.get("id"),
        "task_name": str(task.get("task_name") or ""),
        "enabled": int(task.get("enabled") or 0) == 1 if task else False,
        "cron_time": str(task.get("cron_time") or ""),
        "script_path": str(task.get("script_path") or ""),
        "script_args": str(task.get("script_args") or ""),
        "date_param": str(task.get("date_param") or ""),
        "state": state,
        "scheduler_status": str(scheduler_status or "").strip().lower(),
        "blocking_reason": blocker,
        "can_run": not blocker,
        "can_stop": stoppable and owned,
        "latest_run": {
            "run_uid": str(history.get("run_uid") or ""),
            "task_id": history.get("task_id"),
            "status": str(history.get("status") or "").strip().lower(),
            "run_at": history.get("run_at"),
            "finished_at": history.get("finished_at"),
            "host_name": str(history.get("host_name") or ""),
            "scheduler_instance_id": str(
                history.get("scheduler_instance_id") or ""
            ),
            "build_sha": str(history.get("build_sha") or ""),
            "trigger_source": str(history.get("trigger_source") or ""),
            "exit_code": history.get("exit_code"),
            "duration_seconds": task.get("last_run_duration"),
            "receipt": _project_receipt(
                history.get("output") or task.get("last_run_output")
            ),
        },
        "result_entry_tab": str(spec.get("result_entry_tab") or ""),
        "result_label": str(spec.get("result_label") or "查看结果"),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_operation_status(engine: Any) -> list[dict[str, Any]]:
    operations = quant_workbench_operation_contract()["operations"]
    projected = []
    for spec in operations:
        try:
            item = _load_registered_operation(engine, spec)
        except Exception:
            item = {
                **spec,
                "registration_count": 0,
                "task_id": None,
                "task_name": "",
                "enabled": False,
                "state": "BLOCKED",
                "scheduler_status": "",
                "blocking_reason": "任务注册表不可用",
                "can_run": False,
                "can_stop": False,
                "latest_run": {},
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
        if item["operation_id"] == "strategy_pool":
            item["request_status"] = read_release_analysis_recovery_status()
        projected.append(item)
    return projected


def load_strategy_pool_status(engine: Any) -> dict[str, Any]:
    try:
        latest = _rows(
            engine,
            """
            SELECT pick_date, COUNT(*) AS row_count,
                   SUM(CASE WHEN publication_status='ACTIVE' THEN 1 ELSE 0 END)
                       AS active_count,
                   COUNT(DISTINCT publisher_run_uid) AS publisher_count
              FROM st_recommended_stocks
             GROUP BY pick_date
             ORDER BY pick_date DESC
             LIMIT 1
            """,
            {},
        )
    except Exception:
        return {
            "status": "BLOCKED",
            "reason": "STRATEGY_POOL_READ_FAILED",
            "trade_date": "",
            "active_count": 0,
            "run_uid": "",
            "result_entry_tab": "recommended",
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    if not latest:
        return {
            "status": "NOT_STARTED",
            "reason": "NO_STRATEGY_POOL",
            "trade_date": "",
            "active_count": 0,
            "run_uid": "",
            "result_entry_tab": "recommended",
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    row = latest[0]
    trade_date = str(row.get("pick_date") or "")[:10]
    identity = _rows(
        engine,
        """
        SELECT publisher_run_uid, model_version
          FROM st_recommended_stocks
         WHERE pick_date=:trade_date
         GROUP BY publisher_run_uid, model_version
         ORDER BY publisher_run_uid
         LIMIT 2
        """,
        {"trade_date": trade_date},
    )
    run_uid = str(identity[0].get("publisher_run_uid") or "") if len(identity) == 1 else ""
    model_version = str(identity[0].get("model_version") or "") if len(identity) == 1 else ""
    analysis_count = 0
    try:
        analysis_count = int(_rows(
            engine,
            """
            SELECT COUNT(*) AS analysis_count
              FROM stock_analysis_result
             WHERE analysis_date=:trade_date
            """,
            {"trade_date": trade_date},
        )[0].get("analysis_count") or 0)
    except Exception:
        analysis_count = 0
    row_count = int(row.get("row_count") or 0)
    active_count = int(row.get("active_count") or 0)
    complete = bool(
        trade_date
        and run_uid
        and int(row.get("publisher_count") or 0) == 1
        and active_count == row_count
    )
    return {
        "status": "ACTIVE" if complete else "BLOCKED",
        "reason": "" if complete else "STRATEGY_POOL_IDENTITY_OR_ACTIVATION_UNPROVEN",
        "trade_date": trade_date,
        "row_count": row_count,
        "active_count": active_count,
        "analysis_count": analysis_count,
        "run_uid": run_uid,
        "model_version": model_version,
        "result_entry_tab": "recommended",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_strategy_target_status(engine: Any) -> dict[str, Any]:
    try:
        target = authoritative_closed_trade_date(
            engine,
            now=datetime.now(PRODUCTION_TIMEZONE),
        )
    except Exception:
        return {
            "default_trade_date": "",
            "authoritative_trade_date": "",
            "ready": False,
            "reason": "AUTHORITATIVE_CLOSED_SESSION_UNAVAILABLE",
            "evidence": {},
        }
    ready, reason, evidence = release_analysis_recovery_readiness(engine, target)
    return {
        "default_trade_date": target,
        "authoritative_trade_date": target,
        "ready": ready,
        "reason": reason,
        "evidence": evidence,
    }


def load_market_lifecycle_status(engine: Any, trade_date: str) -> dict[str, Any]:
    try:
        result = build_market_outlook(engine, trade_date=trade_date)
    except Exception as exc:
        return {
            "status": "DATA_BLOCKED",
            "reason": f"MARKET_OUTLOOK_UNAVAILABLE:{type(exc).__name__}",
            "stocks": [],
        }
    return dict(result) if isinstance(result, Mapping) else {
        "status": "DATA_BLOCKED",
        "reason": "MARKET_OUTLOOK_INVALID",
        "stocks": [],
    }


def load_decision_gate_status(
    engine: Any,
    *,
    trade_date: str,
    run_uid: str,
) -> dict[str, Any]:
    if not trade_date:
        return {
            "status": "BLOCKED",
            "trade_date": "",
            "run_uid": run_uid,
            "candidate_count": 0,
            "executable_count": 0,
            "blocked_count": 0,
            "blocking_reason": "STRATEGY_POOL_TRADE_DATE_UNAVAILABLE",
            "top_reasons": [],
        }
    try:
        rows = _rows(
            engine,
            """
            SELECT stock_code, short_name, recommend_status,
                   recommend_reason, signal_status, signal_reason,
                   chase_risk_status, ordinary_buy_eligible,
                   event_risk_level
              FROM st_recommended_stocks
             WHERE pick_date=:trade_date
               AND publication_status='ACTIVE'
               AND (:run_uid='' OR publisher_run_uid=:run_uid)
             ORDER BY stock_code
            """,
            {"trade_date": trade_date, "run_uid": run_uid},
        )
    except Exception as exc:
        return {
            "status": "BLOCKED",
            "trade_date": trade_date,
            "run_uid": run_uid,
            "candidate_count": 0,
            "executable_count": 0,
            "blocked_count": 0,
            "blocking_reason": f"DECISION_GATE_READ_FAILED:{type(exc).__name__}",
            "top_reasons": [],
        }
    executable_count = sum(
        1 for row in rows if is_executable_recommendation(row)
    )
    reason_counts: dict[str, int] = {}
    for row in rows:
        if is_executable_recommendation(row):
            continue
        label = str(
            row.get("recommend_reason")
            or row.get("signal_reason")
            or (
                "执行数据门禁未通过"
                if str(row.get("chase_risk_status") or "").upper()
                != "ALLOW"
                else "普通买入资格未通过"
            )
        ).strip()
        reason_counts[label] = reason_counts.get(label, 0) + 1
    top_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(
            reason_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:6]
    ]
    blocked_count = max(0, len(rows) - executable_count)
    return {
        "status": "READY" if executable_count > 0 else "BLOCKED",
        "trade_date": trade_date,
        "run_uid": run_uid,
        "candidate_count": len(rows),
        "executable_count": executable_count,
        "blocked_count": blocked_count,
        "blocking_reason": (
            "" if executable_count > 0 else "NO_CANDIDATE_PASSED_EXECUTION_GATES"
        ),
        "top_reasons": top_reasons,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_governance_lifecycle_status(engine: Any) -> dict[str, Any]:
    try:
        rows = _rows(
            engine,
            """
            SELECT run_uid, trade_date, market_state, source_status,
                   input_ready, build_commit_sha, status, strategy_count,
                   formal_count, shadow_count, combination_count,
                   observation_count, confirmation_count, tradable_count,
                   created_at, finished_at
              FROM st_strategy_governance_run
             WHERE is_canonical=1
             ORDER BY trade_date DESC, created_at DESC, run_uid DESC
             LIMIT 1
            """,
            {},
        )
    except Exception as exc:
        return {
            "status": "BLOCKED",
            "trade_date": "",
            "blocking_reason": f"GOVERNANCE_READ_FAILED:{type(exc).__name__}",
        }
    if not rows:
        return {
            "status": "BLOCKED",
            "trade_date": "",
            "blocking_reason": "CANONICAL_DECISION_NOT_RUN",
        }
    row = dict(rows[0])
    row["trade_date"] = str(row.get("trade_date") or "")[:10]
    row["status"] = str(row.get("status") or "").upper()
    row["input_ready"] = bool(row.get("input_ready"))
    row["run_uid"] = str(row.get("run_uid") or "")
    return row


def load_simulation_lifecycle_status(
    engine: Any,
    trade_date: str,
) -> dict[str, Any]:
    try:
        result = read_strategy_results(trade_date, limit=5)
    except Exception as exc:
        return {
            "status": "BLOCKED",
            "trade_date": trade_date,
            "reason": f"QMT_SIMULATION_READ_FAILED:{type(exc).__name__}",
            "owner": "WINDOWS_QMT",
        }
    latest = result.get("latest")
    schedule = dict(result.get("schedule") or {})
    latest_status = str(
        latest.get("status") if isinstance(latest, Mapping) else ""
    ).upper()
    status = (
        latest_status
        if latest_status
        else str(result.get("status") or "UNAVAILABLE").upper()
    )
    if status == "AVAILABLE" and latest_status:
        status = latest_status
    return {
        "status": status,
        "trade_date": str(result.get("trade_date") or trade_date)[:10],
        "run_uid": str(
            latest.get("run_uid") if isinstance(latest, Mapping) else ""
        ),
        "latest_status": latest_status,
        "selected_count": (
            latest.get("selected_count") if isinstance(latest, Mapping) else None
        ),
        "blocked_count": (
            latest.get("blocked_count") if isinstance(latest, Mapping) else None
        ),
        "reason": str(result.get("reason") or ""),
        "owner": str(schedule.get("owner") or "WINDOWS_QMT"),
        "last_run_status": schedule.get("last_run_status"),
        "last_run_at": schedule.get("last_run_at"),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def load_review_lifecycle_status(
    engine: Any,
    trade_date: str,
) -> dict[str, Any]:
    try:
        rows = _rows(
            engine,
            """
            SELECT review_date, publish_status, quality_json
              FROM st_quant_review_digest
             WHERE review_date <= :trade_date
             ORDER BY review_date DESC, adjust_type ASC
             LIMIT 1
            """,
            {"trade_date": trade_date},
        )
    except Exception as exc:
        return {
            "status": "BLOCKED",
            "review_date": "",
            "publish_status": "blocked",
            "blocking_reason": f"REVIEW_READ_FAILED:{type(exc).__name__}",
            "blocked_gates": [],
        }
    if not rows:
        return {
            "status": "BLOCKED",
            "review_date": "",
            "publish_status": "missing",
            "blocking_reason": "REVIEW_NOT_AVAILABLE",
            "blocked_gates": [],
        }
    row = dict(rows[0])
    quality = _json_mapping(row.get("quality_json"))
    blocked_gates = [
        {
            "name": str(gate.get("name") or ""),
            "message": str(gate.get("message") or gate.get("expected") or ""),
        }
        for gate in list(quality.get("gates") or [])
        if isinstance(gate, Mapping)
        and str(gate.get("status") or "").strip().lower() == "blocked"
    ]
    publish_status = str(row.get("publish_status") or "").strip().lower()
    blocking_reason = ""
    if publish_status != "ready":
        blocking_reason = (
            blocked_gates[0]["message"]
            if blocked_gates
            else "REVIEW_QUALITY_GATE_BLOCKED"
        )
    return {
        "status": "READY" if publish_status == "ready" else "BLOCKED",
        "review_date": str(row.get("review_date") or "")[:10],
        "publish_status": publish_status,
        "blocking_reason": blocking_reason,
        "blocked_gates": blocked_gates[:8],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _require_operation(operation_id: str) -> dict[str, Any]:
    spec = quant_workbench_operation(operation_id)
    if spec is None:
        raise HTTPException(status_code=404, detail="量化工作台任务不存在")
    return spec


def _require_single_task(engine: Any, spec: Mapping[str, Any]) -> dict[str, Any]:
    operation = _load_registered_operation(engine, spec)
    if int(operation.get("registration_count") or 0) != 1:
        raise HTTPException(
            status_code=409,
            detail={
                "status": "BLOCKED",
                "reason": "TASK_REGISTRATION_MISSING_OR_AMBIGUOUS",
                "operation_id": spec["operation_id"],
            },
        )
    tasks = _rows(
        engine,
        "SELECT * FROM st_scheduled_tasks WHERE id=:task_id LIMIT 1",
        {"task_id": operation["task_id"]},
    )
    if len(tasks) != 1:
        raise HTTPException(status_code=409, detail="任务注册读取不一致")
    return tasks[0]


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
        projected.append({
            **dict(candidate),
            "source_reported_metrics": dict(
                candidate.get("performance_metrics") or {}
            ),
            "system_validation": {
                "status": "REPRODUCTION_UNPROVEN",
                "reported_reproducibility_status": reproducibility,
                "metrics": None,
                "completed": False,
            },
            "research_workflow": {
                "stage": "REPRODUCTION_REQUIRED",
                "next_gate": "INDEPENDENT_REPRODUCTION",
                "governance_entry_tab": "strategy-center",
                "automatic_transition": False,
                "real_order_authority": False,
            },
        })
    return {**result, "candidates": projected}


@router.get("")
def quant_workbench_overview():
    engine = get_engine()
    operations = load_operation_status(engine)
    strategy_pool = load_strategy_pool_status(engine)
    strategy_target = load_strategy_target_status(engine)
    lifecycle_trade_date = str(
        strategy_pool.get("trade_date")
        or strategy_target.get("default_trade_date")
        or ""
    )[:10]
    market = load_market_lifecycle_status(engine, lifecycle_trade_date)
    decision_gate = load_decision_gate_status(
        engine,
        trade_date=lifecycle_trade_date,
        run_uid=str(strategy_pool.get("run_uid") or ""),
    )
    governance = load_governance_lifecycle_status(engine)
    simulation = load_simulation_lifecycle_status(engine, lifecycle_trade_date)
    review = load_review_lifecycle_status(engine, lifecycle_trade_date)
    risk_watch = load_risk_watch_status(engine)
    quant_research = load_research_status(engine)
    lifecycle = build_quant_workbench_lifecycle(
        trade_date=lifecycle_trade_date,
        target=strategy_target,
        outlook=market,
        pool=strategy_pool,
        decision_gate=decision_gate,
        governance=governance,
        simulation=simulation,
        review=review,
    )
    operation_by_capability: dict[str, dict[str, Any]] = {}
    for operation in operations:
        for code in operation.get("capability_codes") or ():
            operation_by_capability[str(code)] = operation
    contract = quant_workbench_capability_contract()
    for capability in contract["capabilities"]:
        operation = operation_by_capability.get(str(capability.get("code") or ""))
        if operation:
            capability["can_run_now"] = bool(operation.get("can_run"))
            capability["workbench_operation"] = {
                "operation_id": operation["operation_id"],
                "task_id": operation.get("task_id"),
                "state": operation.get("state"),
                "blocking_reason": operation.get("blocking_reason"),
            }
    summary = {
        "operation_count": len(operations),
        "runnable_count": sum(1 for item in operations if item.get("can_run")),
        "running_count": sum(
            1 for item in operations if item.get("state") == "RUNNING"
        ),
        "failed_count": sum(
            1 for item in operations if item.get("state") == "FAILED"
        ),
        "strategy_pool": strategy_pool,
        "strategy_target": strategy_target,
        "lifecycle_status": lifecycle["overall_status"],
        "ready_stage_count": lifecycle["ready_stage_count"],
        "stage_count": lifecycle["stage_count"],
        "first_blocking_stage": lifecycle["first_blocking_stage"],
        "next_action": lifecycle["next_action"],
    }
    return {
        **contract,
        "operations": operations,
        "operation_contract": quant_workbench_operation_contract()["schema"],
        "summary": summary,
        "lifecycle": lifecycle,
        "status": "AVAILABLE",
        "evidence": {
            "market": market,
            "strategy_decision_gate": decision_gate,
            "strategy_governance": governance,
            "qmt_simulation": simulation,
            "review": review,
            "risk_watch": risk_watch,
            "quant_research": quant_research,
        },
        "execution_policy": {
            "scheduler_authority": "standalone_scheduler",
            "manual_run_available": True,
            "direct_operation_task_types": sorted(direct_operation_task_types()),
            "target_bound_operations": ["strategy_pool"],
            "reason": "",
            "accepted_is_completion": False,
        },
    }


@router.get("/operations/{operation_id}")
def quant_workbench_operation_status(operation_id: str):
    spec = _require_operation(operation_id)
    operation = load_operation_status(get_engine())
    for item in operation:
        if item["operation_id"] == spec["operation_id"]:
            return item
    raise HTTPException(status_code=404, detail="量化工作台任务不存在")


@router.post("/operations/{operation_id}/run")
def run_quant_workbench_operation(
    operation_id: str,
    trade_date: str = Query(default=""),
):
    spec = _require_operation(operation_id)
    engine = get_engine()
    current = _load_registered_operation(engine, spec)
    if current.get("state") == "RUNNING":
        raise HTTPException(status_code=409, detail="任务正在运行，不能重复提交")
    if current.get("state") == "STALE_RUNNING":
        raise HTTPException(
            status_code=409,
            detail=f"任务存在陈旧运行回执：{current.get('blocking_reason')}",
        )
    if int(current.get("registration_count") or 0) != 1:
        raise HTTPException(status_code=409, detail="任务注册缺失或不唯一")
    if not current.get("enabled"):
        raise HTTPException(status_code=409, detail="任务未启用")

    mode = str(spec.get("execution_mode") or "")
    if mode == SCHEDULER_TARGET_EXECUTION:
        target_status = load_strategy_target_status(engine)
        target = str(trade_date or target_status.get("default_trade_date") or "").strip()
        if not target:
            raise HTTPException(status_code=409, detail="无法确定权威收盘交易日")
        ready, reason, evidence = release_analysis_recovery_readiness(engine, target)
        if not ready:
            raise HTTPException(
                status_code=409,
                detail={
                    "status": "DATA_BLOCKED",
                    "reason": reason,
                    "evidence": evidence,
                },
            )
        request = request_release_analysis_recovery(engine, target, mode="formal")
        return {
            "accepted": True,
            "status": "queued",
            "operation_id": spec["operation_id"],
            "task_type": spec["task_type"],
            "target_trade_date": target,
            "request_id": request["request_id"],
            "prior_run_uid": str(
                (current.get("latest_run") or {}).get("run_uid") or ""
            ),
            "accepted_is_completion": False,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }

    if mode != DIRECT_EXECUTION or spec["task_type"] not in direct_operation_task_types():
        raise HTTPException(status_code=409, detail="任务不在工作台直接运行白名单")
    task = _require_single_task(engine, spec)
    if not scheduler_task_owned_by_current_host(task):
        raise HTTPException(status_code=409, detail="任务由另一运行时节点持有")
    root = Path(__file__).resolve().parents[3]
    result = launch_scheduler_task(task, root=root, engine=engine)
    return {
        **result,
        "operation_id": spec["operation_id"],
        "task_type": spec["task_type"],
        "accepted_is_completion": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


@router.post("/operations/{operation_id}/stop")
def stop_quant_workbench_operation(operation_id: str):
    spec = _require_operation(operation_id)
    engine = get_engine()
    task = _require_single_task(engine, spec)
    if not scheduler_task_owned_by_current_host(task):
        raise HTTPException(status_code=409, detail="任务由另一运行时节点持有")
    result = request_stop_owned_scheduler_task(int(task["id"]))
    return {
        **result,
        "operation_id": spec["operation_id"],
        "task_type": spec["task_type"],
    }


__all__ = [
    "load_decision_gate_status",
    "load_governance_lifecycle_status",
    "load_market_lifecycle_status",
    "load_operation_status",
    "load_research_status",
    "load_review_lifecycle_status",
    "load_risk_watch_status",
    "load_simulation_lifecycle_status",
    "load_strategy_pool_status",
    "load_strategy_target_status",
    "quant_workbench_overview",
    "router",
]
