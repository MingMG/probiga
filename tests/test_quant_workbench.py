from __future__ import annotations

import json
import shutil
import subprocess

import pytest
from fastapi import HTTPException

from server.api.routers import quant_workbench
from server.common.quant_workbench_capabilities import (
    quant_workbench_capability_contract,
)
from server.common.quant_workbench_lifecycle import (
    build_quant_workbench_lifecycle,
)
from server.common.quant_workbench_operations import (
    DIRECT_EXECUTION,
    SCHEDULER_TARGET_EXECUTION,
    quant_workbench_operation,
    quant_workbench_operation_contract,
)


def test_quant_workbench_contract_has_all_stable_codes_and_real_boundaries():
    contract = quant_workbench_capability_contract()
    capabilities = {item["code"]: item for item in contract["capabilities"]}

    assert contract["schema"] == "probiga.quant-workbench-capabilities.v1"
    assert contract["category_count"] == 7
    assert contract["capability_count"] == len(capabilities) == 28
    assert contract["recommended_workflows"][:4] == ["R3", "K4", "R4", "D1"]
    assert capabilities["R3"]["operation"] == "SCHEDULED"
    assert capabilities["R3"]["can_run_now"] is False
    assert capabilities["R4"]["availability"] == "PARTIAL"
    assert capabilities["V1"]["availability"] == "UNAVAILABLE"
    assert capabilities["V2"]["entry_tab"] == ""
    assert all(item["real_order_authority"] is False for item in capabilities.values())


def test_qmt_and_release_checks_do_not_claim_complete_runtime_evidence():
    capabilities = {
        item["code"]: item
        for item in quant_workbench_capability_contract()["capabilities"]
    }

    assert capabilities["O1"]["availability"] == "PARTIAL"
    assert capabilities["O4"]["availability"] == "PARTIAL"


def _scheduler_rows(receipt):
    def rows(_engine, statement, _params):
        if "FROM st_scheduled_tasks" in statement:
            return [{
                "id": 32,
                "task_name": "当前暴露股票重大公告监测",
                "task_type": "exposure_event_risk_watch",
                "enabled": 1,
                "last_run_status": "success",
                "last_run_at": "2026-10-11 10:00:00",
                "last_triggered_at": "2026-10-11 10:00:00",
                "last_run_duration": 12.0,
                "last_run_output": "",
            }]
        return [{
            "run_uid": "a" * 32,
            "status": "success",
            "run_at": "2026-10-11 10:00:00",
            "finished_at": "2026-10-11 10:00:12",
            "host_name": "linux",
            "scheduler_instance_id": "scheduler-1",
            "build_sha": "b" * 40,
            "trigger_source": "scheduled",
            "exit_code": 0,
            "output": json.dumps(receipt),
        }]

    return rows


def test_risk_watch_complete_requires_exact_business_receipt(monkeypatch, tmp_path):
    receipt = {
        "status": "COMPLETE",
        "total_units": 2,
        "completed_units": 2,
        "coverage_count": 2,
        "coverage_readback": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    monkeypatch.setattr(quant_workbench, "_rows", _scheduler_rows(receipt))
    monkeypatch.setattr(
        quant_workbench,
        "read_exposure_watch_checkpoint_progress",
        lambda _root: {
            "status": "COMPLETE",
            "total_units": 2,
            "completed_units": 2,
            "checkpoint_complete": True,
        },
    )

    status = quant_workbench.load_risk_watch_status(
        object(), checkpoint_root=tmp_path
    )

    assert status["status"] == "COMPLETE"
    assert status["completion_proven"] is True
    assert status["run_identity"]["run_uid"] == "a" * 32

    receipt["coverage_count"] = 1
    failed = quant_workbench.load_risk_watch_status(
        object(), checkpoint_root=tmp_path
    )
    assert failed["status"] == "FAILED"
    assert failed["reason"] == "BUSINESS_RECEIPT_UNPROVEN"


def test_research_projection_separates_source_metrics_from_system_validation(
    monkeypatch,
):
    monkeypatch.setattr(
        quant_workbench,
        "load_quant_research_candidates",
        lambda *_args, **_kwargs: {
            "status": "AVAILABLE",
            "candidate_count": 1,
            "research_only": True,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
            "candidates": [{
                "title": "Paper",
                "performance_metrics": {"reported_sharpe": 1.2},
                "reproducibility_status": "NOT_REPRODUCED",
                "real_order_authority": False,
            }],
        },
    )

    result = quant_workbench.load_research_status(object())

    candidate = result["candidates"][0]
    assert candidate["source_reported_metrics"] == {"reported_sharpe": 1.2}
    assert candidate["system_validation"] == {
        "status": "REPRODUCTION_UNPROVEN",
        "reported_reproducibility_status": "NOT_REPRODUCED",
        "metrics": None,
        "completed": False,
    }
    assert candidate["research_workflow"] == {
        "stage": "REPRODUCTION_REQUIRED",
        "next_gate": "INDEPENDENT_REPRODUCTION",
        "governance_entry_tab": "strategy-center",
        "automatic_transition": False,
        "real_order_authority": False,
    }
    assert result["research_only"] is True


def test_external_reproduction_claim_does_not_prove_system_validation(monkeypatch):
    monkeypatch.setattr(
        quant_workbench,
        "load_quant_research_candidates",
        lambda *_args, **_kwargs: {
            "status": "AVAILABLE",
            "candidate_count": 1,
            "research_only": True,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
            "candidates": [{
                "title": "Paper with a declared reproduction status",
                "performance_metrics": {"reported_sharpe": 1.2},
                "reproducibility_status": "INDEPENDENTLY_REPRODUCED",
                "qmt_feasibility": "DIRECT",
                "real_order_authority": False,
            }],
        },
    )

    result = quant_workbench.load_research_status(object())
    candidate = result["candidates"][0]

    assert candidate["source_reported_metrics"] == {"reported_sharpe": 1.2}
    assert candidate["system_validation"]["completed"] is False
    assert candidate["system_validation"]["metrics"] is None
    assert candidate["system_validation"]["status"] != "INDEPENDENTLY_REPRODUCED"
    assert candidate["research_workflow"]["stage"] == "REPRODUCTION_REQUIRED"
    assert candidate["research_workflow"]["next_gate"] == "INDEPENDENT_REPRODUCTION"
    assert candidate["research_workflow"]["automatic_transition"] is False
    assert candidate["research_workflow"]["real_order_authority"] is False


def test_quant_workbench_frontend_is_a_real_selectable_entry():
    index = open("server/static/index.html", encoding="utf-8").read()
    source = open("server/static/js/app.js", encoding="utf-8").read()

    assert 'id="tab-quant-workbench"' in index
    assert "loadQuantWorkbenchPage" in source
    assert "/api/quant-workbench" in source
    assert "量化工作台" in source
    assert "来源报告指标" in source
    assert "本系统验证" in source


def test_operation_contract_routes_strategy_pool_through_scheduler_target():
    contract = quant_workbench_operation_contract()
    operations = {
        item["operation_id"]: item for item in contract["operations"]
    }

    assert contract["schema"] == "probiga.quant-workbench-operations.v1"
    assert operations["strategy_pool"]["task_type"] == "analysis_fast"
    assert (
        operations["strategy_pool"]["execution_mode"]
        == SCHEDULER_TARGET_EXECUTION
    )
    assert operations["strategy_pool"]["requires_trade_date"] is True
    assert operations["qmt_strategy_simulation"]["execution_mode"] == DIRECT_EXECUTION
    assert contract["automatic_real_order_submission"] is False
    assert contract["real_order_authority"] is False
    assert quant_workbench_operation("not-registered") is None


def test_operation_projection_blocks_ambiguous_disabled_and_stale_tasks(monkeypatch):
    spec = quant_workbench_operation("daily_review")
    monkeypatch.setattr(
        quant_workbench,
        "_rows",
        lambda _engine, statement, _params: (
            [{}, {}]
            if "FROM st_scheduled_tasks" in statement
            else []
        ),
    )
    ambiguous = quant_workbench._load_registered_operation(object(), spec)
    assert ambiguous["registration_count"] == 2
    assert ambiguous["can_run"] is False
    assert "不唯一" in ambiguous["blocking_reason"]

    def disabled_rows(_engine, statement, _params):
        if "FROM st_scheduled_tasks" in statement:
            return [{
                "id": 32,
                "task_name": "盘后量化复盘",
                "task_type": "daily_review",
                "enabled": 0,
                "last_run_status": "success",
                "last_run_at": "2026-10-20 18:00:00",
            }]
        return []

    monkeypatch.setattr(quant_workbench, "_rows", disabled_rows)
    disabled = quant_workbench._load_registered_operation(object(), spec)
    assert disabled["can_run"] is False
    assert disabled["blocking_reason"] == "任务未启用"

    def stale_rows(_engine, statement, _params):
        if "FROM st_scheduled_tasks" in statement:
            return [{
                "id": 32,
                "task_name": "盘后量化复盘",
                "task_type": "daily_review",
                "enabled": 1,
                "last_run_status": "running",
                "last_run_at": "2026-09-23 21:36:23",
            }]
        return [{
            "run_uid": "a" * 32,
            "task_id": 32,
            "status": "running",
            "run_at": "2026-09-23 21:36:23",
            "output": "",
        }]

    monkeypatch.setattr(quant_workbench, "_rows", stale_rows)
    monkeypatch.setattr(
        quant_workbench,
        "scheduler_task_owned_by_current_host",
        lambda _task: True,
    )
    stale = quant_workbench._load_registered_operation(object(), spec)
    assert stale["state"] == "STALE_RUNNING"
    assert stale["can_stop"] is True
    assert stale["can_run"] is False


def test_registered_operation_projects_real_run_identity_and_receipt(monkeypatch):
    spec = quant_workbench_operation("strategy_pool")
    monkeypatch.setattr(
        quant_workbench,
        "scheduler_task_owned_by_current_host",
        lambda _task: True,
    )

    def rows(_engine, statement, _params):
        if "FROM st_scheduled_tasks" in statement:
            return [{
                "id": 37,
                "task_name": "盘后快速分析推荐",
                "task_type": "analysis_fast",
                "enabled": 1,
                "cron_time": "22:20",
                "script_path": "tools/run_ai_recommendation_premarket.py",
                "script_args": "--top-n 80 --min-score 62 --json",
                "last_run_status": "success",
                "last_run_at": "2026-10-25 21:57:26",
                "last_run_duration": 400,
                "last_run_output": "",
            }]
        return [{
            "run_uid": "b" * 32,
            "task_id": 37,
            "status": "success",
            "run_at": "2026-10-25 21:57:26",
            "finished_at": "2026-10-25 22:04:03",
            "host_name": "linux",
            "scheduler_instance_id": "scheduler-1",
            "build_sha": "c" * 40,
            "trigger_source": "scheduled",
            "exit_code": 0,
            "output": json.dumps({
                "schema": "probiga.release-analysis-fast-result.v1",
                "status": "COMPLETE",
                "task_type": "analysis_fast",
                "target_trade_date": "2026-10-23",
                "run_uid": "b" * 32,
                "analysis_count": 5578,
                "recommendation_count": 80,
                "canonical_pool_sha256": "d" * 64,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }),
        }]

    monkeypatch.setattr(quant_workbench, "_rows", rows)
    projected = quant_workbench._load_registered_operation(object(), spec)

    assert projected["task_id"] == 37
    assert projected["state"] == "SUCCEEDED"
    assert projected["latest_run"]["run_uid"] == "b" * 32
    assert projected["latest_run"]["receipt"]["status"] == "COMPLETE"
    assert projected["latest_run"]["receipt"]["recommendation_count"] == 80
    assert projected["can_run"] is True


def test_strategy_pool_projection_requires_active_identity(monkeypatch):
    def rows(_engine, statement, _params):
        if "GROUP BY pick_date" in statement:
            return [{
                "pick_date": "2026-10-23",
                "row_count": 80,
                "active_count": 80,
                "publisher_count": 1,
            }]
        if "SELECT publisher_run_uid" in statement:
            return [{
                "publisher_run_uid": "e" * 32,
                "model_version": "ai-rec-v3-tactical-v4",
            }]
        return [{"analysis_count": 5578}]

    monkeypatch.setattr(quant_workbench, "_rows", rows)
    result = quant_workbench.load_strategy_pool_status(object())

    assert result["status"] == "ACTIVE"
    assert result["active_count"] == 80
    assert result["analysis_count"] == 5578
    assert result["run_uid"] == "e" * 32
    assert result["real_order_authority"] is False


def test_lifecycle_joins_data_pool_decision_simulation_and_review_truthfully():
    lifecycle = build_quant_workbench_lifecycle(
        trade_date="2026-10-23",
        target={
            "ready": True,
            "reason": "ready",
            "evidence": {
                "kline": 5578,
                "capital_flow": 5570,
                "snapshot": 5578,
            },
        },
        outlook={
            "status": "READY",
            "market_mode": "PANIC_REBOUND",
            "market_mode_label": "恐慌修复",
            "tactical_style": "龙头反核确认",
            "next_trade_date": "2026-10-26",
            "stocks": [{"stock_code": "603278", "short_name": "大业股份"}],
        },
        pool={
            "status": "ACTIVE",
            "trade_date": "2026-10-23",
            "active_count": 80,
            "analysis_count": 5578,
            "run_uid": "a" * 32,
        },
        decision_gate={
            "candidate_count": 80,
            "executable_count": 0,
            "blocked_count": 80,
            "top_reasons": [{"reason": "关键数据缺失", "count": 80}],
        },
        governance={
            "trade_date": "2026-08-31",
            "run_uid": "b" * 32,
            "status": "COMPLETED",
            "input_ready": True,
            "observation_count": 80,
            "confirmation_count": 0,
            "tradable_count": 0,
        },
        simulation={"status": "PENDING", "reason": "尚无模拟输入"},
        review={
            "publish_status": "blocked",
            "review_date": "2026-10-23",
            "blocking_reason": "目标行情覆盖率不足",
        },
    )
    stages = {item["code"]: item for item in lifecycle["stages"]}

    assert lifecycle["schema"] == "probiga.quant-workbench-lifecycle.v1"
    assert lifecycle["overall_status"] == "BLOCKED"
    assert lifecycle["first_blocking_stage"] == "CANONICAL_DECISION"
    assert lifecycle["next_action"]["entry_tab"] == "strategy-center"
    assert stages["DATA"]["state"] == "READY"
    assert stages["MARKET"]["state"] == "READY"
    assert stages["STRATEGY_POOL"]["state"] == "READY"
    assert stages["CANONICAL_DECISION"]["state"] == "STALE"
    assert stages["EXECUTION_GATE"]["state"] == "BLOCKED"
    assert stages["SIMULATION"]["state"] == "WAITING"
    assert stages["REVIEW"]["state"] == "BLOCKED"
    assert lifecycle["real_order_authority"] is False


def test_decision_gate_counts_real_non_executable_reasons(monkeypatch):
    rows = [
        {
            "stock_code": "603278",
            "recommend_status": "ALLOW",
            "recommend_reason": "",
            "signal_status": "CONFIRM",
            "signal_reason": "",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": 1,
        },
        {
            "stock_code": "002724",
            "recommend_status": "BLOCK",
            "recommend_reason": "关键数据缺失",
            "signal_status": "WATCH",
            "signal_reason": "",
            "chase_risk_status": "DATA_BLOCKED",
            "ordinary_buy_eligible": 0,
        },
    ]
    monkeypatch.setattr(
        quant_workbench,
        "_rows",
        lambda _engine, _statement, _params: rows,
    )
    monkeypatch.setattr(
        quant_workbench,
        "is_executable_recommendation",
        lambda row: row["stock_code"] == "603278",
    )

    result = quant_workbench.load_decision_gate_status(
        object(),
        trade_date="2026-10-23",
        run_uid="a" * 32,
    )

    assert result["candidate_count"] == 2
    assert result["executable_count"] == 1
    assert result["blocked_count"] == 1
    assert result["top_reasons"][0] == {"reason": "关键数据缺失", "count": 1}
    assert result["real_order_authority"] is False


def test_review_loader_surfaces_first_blocked_quality_gate(monkeypatch):
    monkeypatch.setattr(
        quant_workbench,
        "_rows",
        lambda _engine, _statement, _params: [{
            "review_date": "2026-10-23",
            "publish_status": "blocked",
            "quality_json": json.dumps({
                "status": "blocked",
                "gates": [
                    {"name": "weekday", "status": "pass"},
                    {
                        "name": "target_coverage",
                        "status": "blocked",
                        "message": "目标日有效行情覆盖率0.00%低于门槛",
                    },
                ],
            }),
        }],
    )

    result = quant_workbench.load_review_lifecycle_status(
        object(),
        "2026-10-23",
    )

    assert result["status"] == "BLOCKED"
    assert result["publish_status"] == "blocked"
    assert result["blocking_reason"] == "目标日有效行情覆盖率0.00%低于门槛"
    assert result["blocked_gates"][0]["name"] == "target_coverage"


def test_overview_exposes_one_integrated_lifecycle_and_next_action(monkeypatch):
    monkeypatch.setattr(quant_workbench, "get_engine", lambda: object())
    monkeypatch.setattr(
        quant_workbench,
        "load_operation_status",
        lambda _engine: [{
            "operation_id": "strategy_pool",
            "task_type": "analysis_fast",
            "can_run": True,
            "state": "FAILED",
            "capability_codes": ["S3"],
        }],
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_strategy_pool_status",
        lambda _engine: {
            "status": "ACTIVE",
            "trade_date": "2026-10-23",
            "active_count": 80,
            "analysis_count": 5578,
            "run_uid": "a" * 32,
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_strategy_target_status",
        lambda _engine: {
            "default_trade_date": "2026-10-23",
            "ready": True,
            "reason": "ready",
            "evidence": {"kline": 5578},
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_market_lifecycle_status",
        lambda _engine, _date: {
            "status": "READY",
            "market_mode": "PANIC_REBOUND",
            "market_mode_label": "恐慌修复",
            "stocks": [],
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_decision_gate_status",
        lambda _engine, **_kwargs: {
            "candidate_count": 80,
            "executable_count": 0,
            "blocked_count": 80,
            "top_reasons": [],
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_governance_lifecycle_status",
        lambda _engine: {
            "trade_date": "2026-08-31",
            "run_uid": "b" * 32,
            "status": "COMPLETED",
            "input_ready": True,
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_simulation_lifecycle_status",
        lambda _engine, _date: {"status": "PENDING", "reason": "尚无模拟输入"},
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_review_lifecycle_status",
        lambda _engine, _date: {
            "publish_status": "blocked",
            "blocking_reason": "目标行情覆盖率不足",
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_risk_watch_status",
        lambda _engine: {"status": "COMPLETE"},
    )
    monkeypatch.setattr(
        quant_workbench,
        "load_research_status",
        lambda _engine: {"status": "AVAILABLE", "candidates": []},
    )

    result = quant_workbench.quant_workbench_overview()

    assert result["lifecycle"]["schema"] == "probiga.quant-workbench-lifecycle.v1"
    assert result["lifecycle"]["overall_status"] == "BLOCKED"
    assert result["summary"]["first_blocking_stage"] == "CANONICAL_DECISION"
    assert result["summary"]["next_action"]["entry_tab"] == "strategy-center"
    assert result["execution_policy"]["manual_run_available"] is True
    assert result["execution_policy"]["accepted_is_completion"] is False


def test_run_operation_rejects_unknown_and_uses_scheduler_for_direct_task(monkeypatch):
    with pytest.raises(HTTPException) as unknown:
        quant_workbench.run_quant_workbench_operation("not-registered")
    assert unknown.value.status_code == 404

    spec = quant_workbench_operation("daily_review")
    monkeypatch.setattr(quant_workbench, "get_engine", lambda: object())
    monkeypatch.setattr(
        quant_workbench,
        "_load_registered_operation",
        lambda _engine, _spec: {
            "registration_count": 1,
            "enabled": True,
            "state": "NOT_STARTED",
            "blocking_reason": "",
            "latest_run": {},
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "_require_single_task",
        lambda _engine, _spec: {
            "id": 32,
            "task_type": spec["task_type"],
            "enabled": 1,
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "scheduler_task_owned_by_current_host",
        lambda _task: True,
    )
    launch_calls = []
    monkeypatch.setattr(
        quant_workbench,
        "launch_scheduler_task",
        lambda task, **kwargs: (
            launch_calls.append((task, kwargs))
            or {
                "accepted": True,
                "status": "running",
                "task_id": 32,
                "job_id": "f" * 32,
            }
        ),
    )

    result = quant_workbench.run_quant_workbench_operation("daily_review")

    assert result["accepted"] is True
    assert result["job_id"] == "f" * 32
    assert result["accepted_is_completion"] is False
    assert len(launch_calls) == 1
    assert launch_calls[0][0]["task_type"] == "daily_review"


def test_strategy_pool_run_waits_for_readiness_and_requests_scheduler(monkeypatch):
    monkeypatch.setattr(quant_workbench, "get_engine", lambda: object())
    monkeypatch.setattr(
        quant_workbench,
        "_load_registered_operation",
        lambda _engine, _spec: {
            "registration_count": 1,
            "enabled": True,
            "state": "NOT_STARTED",
            "latest_run": {"run_uid": "a" * 32},
        },
    )
    monkeypatch.setattr(
        quant_workbench,
        "release_analysis_recovery_readiness",
        lambda _engine, date: (
            False,
            "target_differs_from_authoritative_closed_session",
            {"target_trade_date": date},
        ),
    )
    with pytest.raises(HTTPException) as blocked:
        quant_workbench.run_quant_workbench_operation(
            "strategy_pool",
            trade_date="2026-10-23",
        )
    assert blocked.value.status_code == 409

    monkeypatch.setattr(
        quant_workbench,
        "release_analysis_recovery_readiness",
        lambda _engine, date: (
            True,
            "ready",
            {"target_trade_date": date, "kline": 5500},
        ),
    )
    requested = []
    monkeypatch.setattr(
        quant_workbench,
        "request_release_analysis_recovery",
        lambda _engine, target, mode: (
            requested.append((target, mode))
            or {"request_id": "1" * 32}
        ),
    )

    result = quant_workbench.run_quant_workbench_operation(
        "strategy_pool",
        trade_date="2026-10-23",
    )

    assert requested == [("2026-10-23", "formal")]
    assert result["accepted"] is True
    assert result["status"] == "queued"
    assert result["target_trade_date"] == "2026-10-23"
    assert result["prior_run_uid"] == "a" * 32
    assert result["accepted_is_completion"] is False


def test_quant_workbench_frontend_operates_tasks_instead_of_only_navigation():
    source = open("server/static/js/app.js", encoding="utf-8").read()
    start = source.index("    function loadQuantWorkbenchPage(d, c)")
    section = source[start:source.index("    function hasExplicitNewBuyGate", start)]

    assert "/api/quant-workbench/operations/" in section
    assert "window.runQuantOperation" in section
    assert "window.stopQuantOperation" in section
    assert "/run" in section
    assert "/stop" in section
    assert "_startQuantOperationPoll" in section
    assert "openQuantOperationResult" in section
    assert "accepted 不等于 COMPLETE" in section
    assert "MarketOutlook.summary" not in section


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_quant_workbench_renders_real_task_actions_and_active_pool():
    source = open("server/static/js/app.js", encoding="utf-8").read()
    start = source.index("    function loadQuantWorkbenchPage(d, c)")
    loader = source[start:source.index("    function hasExplicitNewBuyGate", start)]
    harness = r"""
const assert = require('assert');
global.window = {};
let rendered = '';
const container = {
  get innerHTML() { return rendered; },
  set innerHTML(value) { rendered = String(value); }
};
function escHtml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function escAttr(value) { return escHtml(value); }
function currentDateValue() { return '2026-10-23'; }
function recommendationDateValue() { return '2026-10-23'; }
function switchTab() {}
function fetchRawJsonWithTimeout() {
  return Promise.resolve({
    schema: 'probiga.quant-workbench-capabilities.v1',
    operation_contract: 'probiga.quant-workbench-operations.v1',
    capabilities: [],
    recommended_workflows: [],
    operations: [{
      operation_id: 'strategy_pool',
      task_type: 'analysis_fast',
      task_id: 37,
      task_name: '盘后快速分析推荐',
      label: '生成正式策略票池',
      category: '策略执行',
      description: '生成票池',
      execution_mode: 'SCHEDULER_TARGET',
      requires_trade_date: true,
      result_entry_tab: 'recommended',
      result_label: '查看策略票池',
      enabled: true,
      can_run: true,
      can_stop: false,
      state: 'SUCCEEDED',
      request_status: {status: 'ATTACHED', reason: 'ready_for_scheduler_dispatch'},
      latest_run: {
        run_uid: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        run_at: '2026-10-25 21:57:26',
        receipt: {status: 'COMPLETE', analysis_count: 5578, recommendation_count: 80}
      }
    }],
    summary: {
      operation_count: 1,
      runnable_count: 1,
      running_count: 0,
      failed_count: 0,
      strategy_pool: {
        status: 'ACTIVE', trade_date: '2026-10-23', analysis_count: 5578,
        active_count: 80, model_version: 'ai-rec-v3-tactical-v4',
        run_uid: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
      },
      strategy_target: {default_trade_date: '2026-10-23', ready: true}
      ,next_action: {
        stage_code: 'CANONICAL_DECISION',
        stage_label: '规范治理决策',
        reason: 'CANONICAL_DECISION_TRADE_DATE_STALE',
        operation_ids: [],
        entry_tab: 'strategy-center',
        entry_label: '打开策略治理'
      }
    },
    lifecycle: {
      schema: 'probiga.quant-workbench-lifecycle.v1',
      trade_date: '2026-10-23',
      overall_status: 'BLOCKED',
      ready_stage_count: 3,
      stage_count: 7,
      first_blocking_stage: 'CANONICAL_DECISION',
      stages: [
        {code:'DATA',label:'完整交易日数据',state:'READY',summary:'日线 5578',evidence:{kline:5578,capital_flow:5570,snapshot:5578},operation_ids:[],entry_tab:'datasource'},
        {code:'MARKET',label:'市场状态与情景',state:'READY',summary:'恐慌修复',evidence:{market_mode_label:'恐慌修复'},operation_ids:[],entry_tab:'market-outlook'},
        {code:'STRATEGY_POOL',label:'策略候选池',state:'READY',summary:'80 只 ACTIVE',evidence:{active_count:80,trade_date:'2026-10-23'},operation_ids:['strategy_pool'],entry_tab:'recommended'},
        {code:'CANONICAL_DECISION',label:'规范治理决策',state:'STALE',summary:'规范运行日 2026-08-31',blocking_reason:'CANONICAL_DECISION_TRADE_DATE_STALE',evidence:{trade_date:'2026-08-31'},operation_ids:[],entry_tab:'strategy-center',entry_label:'打开策略治理'},
        {code:'EXECUTION_GATE',label:'候选执行门禁',state:'BLOCKED',summary:'0 / 80 只通过全部执行门禁',evidence:{executable_count:0,candidate_count:80,top_reasons:[{reason:'关键数据缺失',count:80}]},operation_ids:[],entry_tab:'trading-v3-candidates'},
        {code:'SIMULATION',label:'QMT 策略模拟',state:'WAITING',summary:'尚无模拟输入',evidence:{reason:'尚无模拟输入'},operation_ids:[],entry_tab:'qmt-strategy-results'},
        {code:'REVIEW',label:'复盘与反馈',state:'BLOCKED',summary:'复盘阻断',evidence:{blocking_reason:'目标行情覆盖率不足'},operation_ids:[],entry_tab:'review'}
      ]
    },
    evidence: {risk_watch: {}, quant_research: {}}
  });
}
""" + loader + r"""
loadQuantWorkbenchPage('2026-10-23', container);
setTimeout(function() {
  assert.match(rendered, /80 只票池已出，0 只可执行/);
  assert.match(rendered, /规范治理结果仍停留在 2026-08-31/);
  assert.match(rendered, /恢复请求已进入调度队列/);
  assert.match(rendered, /生成票池/);
  assert.match(rendered, /查看策略票池/);
  assert.match(rendered, /当前有效票池/);
  assert.match(rendered, /量化生产闭环/);
  assert.match(rendered, /当前第一断点/);
  assert.match(rendered, /规范治理决策/);
  assert.match(rendered, /0 \/ 80 可执行/);
  assert.match(rendered, /打开策略治理/);
  assert.match(rendered, /5578/);
  assert.match(rendered, /aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/);
  assert.match(rendered, /accepted 不等于 COMPLETE/);
}, 0);
"""
    result = subprocess.run(
        [shutil.which("node"), "-"],
        input=harness,
        text=True,
        encoding="utf-8",
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
