from __future__ import annotations

import json

from server.api.routers import quant_workbench
from server.common.quant_workbench_capabilities import (
    quant_workbench_capability_contract,
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
