from __future__ import annotations

import json
import inspect
import sys
from datetime import datetime

import pytest

from tools import run_strategy_governance_daily as daily
from server.engine import strategy_center as strategy_center_engine
from server.engine import strategy_governance as governance_engine
from server.engine import strategy_governance_orchestrator as orchestrator
from server.engine.strategy_industry_history import IndustrySnapshotNotReady


@pytest.fixture(autouse=True)
def _completed_industry_capture(monkeypatch):
    monkeypatch.setattr(
        daily, "_bootstrap_execution_adapters", lambda: {"registry_sealed": True},
    )
    monkeypatch.setattr(
        daily,
        "_capture_industry_history",
        lambda target: {"status": "COMPLETED", "trade_date": target},
    )
    # Calendar-only fakes in this legacy CLI test do not model governance
    # history/audit tables.  Dedicated orchestrator tests exercise both against
    # a real SQL database and verify idempotent RUN_BLOCKED persistence.
    monkeypatch.setattr(orchestrator, "_canonical_run", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_calendar_status", lambda *_args: 0)
    monkeypatch.setattr(
        orchestrator,
        "persist_blocked_attempt",
        lambda *_args, **_kwargs: {
            "audit_id": "a" * 32,
            "audit_hash": "b" * 64,
            "idempotent_replay": False,
        },
    )


def test_daily_entrypoint_delegates_build_and_persistence_to_locked_governance():
    source = inspect.getsource(daily.main)
    assert "orchestrate_strategy_governance" in source
    assert "build_strategy_center_snapshot" not in source
    assert "persist_strategy_center_snapshot" not in source
    assert "strategy_snapshot=" not in source
    assert "strategy_limit=" in source


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _CalendarConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        self.engine.sql = str(statement)
        self.engine.params = dict(params)
        if self.engine.error is not None:
            raise self.engine.error
        return _ScalarResult(self.engine.value)


class _CalendarEngine:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error
        self.sql = ""
        self.params = {}
        self.disposed = False

    def connect(self):
        return _CalendarConnection(self)

    def dispose(self):
        self.disposed = True


@pytest.mark.parametrize(
    ("now", "calendar_value", "expected_operator", "expected"),
    [
        (
            datetime(2026, 8, 21, 17, 59, 59),
            "2026-08-20",
            "trade_date < :today",
            "2026-08-20",
        ),
        (
            datetime(2026, 8, 21, 18, 0, 0),
            "2026-08-21",
            "trade_date <= :today",
            "2026-08-21",
        ),
        (
            # Saturday: the calendar itself selects the preceding Friday.
            datetime(2026, 8, 22, 12, 0, 0),
            "2026-08-21",
            "trade_date < :today",
            "2026-08-21",
        ),
        (
            # National Day holiday: the last open date remains authoritative.
            datetime(2026, 10, 1, 22, 35, 0),
            "2026-09-30",
            "trade_date <= :today",
            "2026-09-30",
        ),
    ],
)
def test_authoritative_trade_date_obeys_close_weekend_and_holiday(
    now, calendar_value, expected_operator, expected
):
    engine = _CalendarEngine(calendar_value)
    assert daily.authoritative_closed_trade_date(engine, now=now) == expected
    assert expected_operator in engine.sql
    assert engine.params == {"today": now.date().isoformat()}


def test_every_persist_path_requires_the_authoritative_closed_trade_date(
    monkeypatch,
):
    marker_engine = object()
    monkeypatch.setattr(governance_engine, "get_engine", lambda: marker_engine)
    monkeypatch.setattr(
        governance_engine,
        "authoritative_closed_trade_date",
        lambda engine: "2026-08-21" if engine is marker_engine else "",
    )

    assert (
        governance_engine._require_authoritative_closed_trade_date(
            "2026-08-21"
        )
        == "2026-08-21"
    )
    with pytest.raises(
        governance_engine.GovernanceEvidenceNotReady,
        match="不是权威已收盘交易日",
    ):
        governance_engine._require_authoritative_closed_trade_date(
            "2026-08-22"
        )


def test_persist_guard_blocks_when_authoritative_calendar_is_unavailable(
    monkeypatch,
):
    monkeypatch.setattr(governance_engine, "get_engine", lambda: object())
    monkeypatch.setattr(
        governance_engine,
        "authoritative_closed_trade_date",
        lambda _engine: "",
    )
    with pytest.raises(
        governance_engine.GovernanceEvidenceNotReady,
        match="没有已收盘交易日",
    ):
        governance_engine._require_authoritative_closed_trade_date(
            "2026-08-21"
        )


def test_missing_authoritative_calendar_is_structured_blocked(
    monkeypatch, capsys
):
    engine = _CalendarEngine(None)
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["target_trade_date"] == ""
    assert payload["input_trade_date"] == ""
    assert payload["automatic_real_order_submission"] is False
    assert engine.disposed is True


def test_calendar_read_error_is_structured_blocked(monkeypatch, capsys):
    engine = _CalendarEngine(error=RuntimeError("calendar unavailable"))
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["target_trade_date"] == ""
    assert payload["input_trade_date"] == ""
    assert "RuntimeError" in payload["reason"]
    assert payload["automatic_real_order_submission"] is False
    assert engine.disposed is True


def test_explicit_trade_date_cannot_bypass_authoritative_day(
    monkeypatch, capsys
):
    engine = _CalendarEngine("2026-08-21")
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_strategy_governance_daily.py", "--trade-date", "2026-08-20"],
    )

    assert daily.main() == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["orchestration_status"] == "INTEGRITY_ERROR"
    assert payload["reason_code"] == "TARGET_NOT_AUTHORITATIVE"
    assert payload["retryable"] is False
    assert payload["target_trade_date"] == "2026-08-21"
    assert payload["requested_trade_date"] == "2026-08-20"
    assert payload["automatic_real_order_submission"] is False
    assert engine.disposed is True


def test_industry_capture_failure_blocks_before_governance_write(
    monkeypatch, capsys,
):
    engine = _CalendarEngine("2026-08-21")
    governance_called = False

    def fail_capture(_target):
        raise IndustrySnapshotNotReady("QMT exact-date snapshot missing")

    def governance_must_not_run(**_kwargs):
        nonlocal governance_called
        governance_called = True
        return {"status": "ok"}

    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(daily, "_capture_industry_history", fail_capture)
    monkeypatch.setattr(governance_engine, "governance_snapshot", governance_must_not_run)
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["orchestration_status"] == "NOT_READY"
    assert payload["reason_code"] == "QMT_INDUSTRY_SNAPSHOT_NOT_READY"
    assert payload["automatic_real_order_submission"] is False
    assert governance_called is False


def test_old_self_consistent_snapshot_is_not_accepted_for_target_date():
    snapshot = {
        "trade_date": "2026-08-20",
        "data_date": "2026-08-20",
        "source_status": "fresh",
    }
    reason = daily._input_block_reason(
        snapshot,
        "2026-08-21",
        True,
        "底层票池数据新鲜且日期一致",
    )
    assert "要求2026-08-21" in reason
    assert "实际交易日2026-08-20" in reason


def test_target_date_snapshot_has_no_additional_block_reason():
    snapshot = {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "source_status": "fresh",
    }
    assert daily._input_block_reason(
        snapshot,
        "2026-08-21",
        True,
        "底层票池数据新鲜且日期一致",
    ) == ""


def test_governance_window_gap_is_structured_blocked_and_keeps_cash(
    monkeypatch, capsys
):
    engine = _CalendarEngine("2026-08-21")
    base = {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "source_status": "fresh",
    }
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        strategy_center_engine,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: base,
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "persist_strategy_center_snapshot",
        lambda snapshot: snapshot,
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_input_ready",
        lambda _snapshot: (True, "ready"),
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            governance_engine.GovernanceEvidenceNotReady(
                "QMT权威会话缺少11个交易日"
            )
        ),
    )
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["target_trade_date"] == "2026-08-21"
    assert payload["input_trade_date"] == "2026-08-21"
    assert "QMT权威会话缺少11个交易日" in payload["reason"]
    assert payload["automatic_real_order_submission"] is False


def test_unexpected_governance_failure_is_not_waived_as_input_not_ready(
    monkeypatch, capsys,
):
    engine = _CalendarEngine("2026-08-21")
    base = {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "source_status": "fresh",
    }
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        strategy_center_engine,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: base,
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "persist_strategy_center_snapshot",
        lambda snapshot: snapshot,
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_input_ready",
        lambda _snapshot: (True, "ready"),
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("schema drift must fail the release")
        ),
    )
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 4
    payload = json.loads(capsys.readouterr().out)
    assert payload["orchestration_status"] == "PROGRAM_ERROR"
    assert payload["error_class"] == "PROGRAM"
    assert payload["retryable"] is False
    assert payload["reason_code"] == "GOVERNANCE_PROGRAM_ERROR"


def _deploy_validation_payload(orchestration_status: str) -> tuple[dict, int, str]:
    common = {
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "allocations": [{
            "target_type": "CASH",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }],
    }
    if orchestration_status == "COMPLETED":
        return ({
            **common,
            "status": "ok",
            "orchestration_status": "COMPLETED",
            "reason_code": "GOVERNANCE_COMPLETED",
            "run_uid": "a" * 32,
            "trade_date": "2026-08-21",
            "build_commit_sha": "a" * 40,
            "summary": {},
        }, 0, "completed")
    if orchestration_status == orchestrator.NOT_DUE:
        return ({
            **common,
            "status": "not_due",
            "orchestration_status": orchestrator.NOT_DUE,
            "error_class": "NONE",
            "retryable": False,
            "input_ready": False,
            "reason": "权威交易日已经完成",
            "reason_code": "CANONICAL_RUN_EXISTS",
            "target_trade_date": "2026-08-21",
            "requested_trade_date": "",
            "current_run": {"build_commit_sha": "a" * 40},
        }, 0, "not_due")
    expected = {
        orchestrator.NOT_READY: (2, "NOT_READY", True, "not_ready"),
        orchestrator.INTEGRITY_ERROR: (3, "INTEGRITY", False, "integrity_error"),
        orchestrator.PROGRAM_ERROR: (4, "PROGRAM", False, "program_error"),
    }[orchestration_status]
    exit_code, error_class, retryable, disposition = expected
    return ({
        **common,
        "status": "blocked",
        "orchestration_status": orchestration_status,
        "error_class": error_class,
        "retryable": retryable,
        "input_ready": False,
        "reason": "结构化阻断",
        "reason_code": "TEST_BLOCKED",
        "blocking_stage": "TEST",
        "target_trade_date": "2026-08-21",
        "requested_trade_date": "",
        "input_trade_date": "",
    }, exit_code, disposition)


@pytest.mark.parametrize(
    "orchestration_status",
    [
        "COMPLETED",
        orchestrator.NOT_DUE,
        orchestrator.NOT_READY,
        orchestrator.INTEGRITY_ERROR,
        orchestrator.PROGRAM_ERROR,
    ],
)
def test_deploy_validator_accepts_only_exact_status_exit_contract(
    orchestration_status,
):
    payload, exit_code, disposition = _deploy_validation_payload(
        orchestration_status
    )
    assert daily.validate_cli_result(payload, exit_code) == disposition


@pytest.mark.parametrize(
    ("mutation", "exit_delta"),
    [
        (lambda value: value.update({"automatic_real_order_submission": True}), 0),
        (lambda value: value.pop("automatic_real_order_submission"), 0),
        (lambda value: value.update({"real_order_authority": True}), 0),
        (lambda value: value.pop("real_order_authority"), 0),
        (lambda value: value.update({
            "automatic_real_order_submission": "false"
        }), 0),
        (lambda value: value["allocations"][0].pop(
            "real_order_authority"
        ), 0),
        (lambda value: value["allocations"][0].update({
            "real_order_authority": "false",
        }), 0),
        (lambda value: value["allocations"][0].update({
            "simulated_weight_pct": 99.99,
        }), 0),
        (lambda value: value["allocations"][0].update({
            "real_order_authority": True,
        }), 0),
        (lambda _value: None, 1),
    ],
)
def test_deploy_validator_rejects_unsafe_or_mismatched_completed_output(
    mutation, exit_delta,
):
    payload, exit_code, _disposition = _deploy_validation_payload("COMPLETED")
    mutation(payload)
    with pytest.raises(ValueError):
        daily.validate_cli_result(payload, exit_code + exit_delta)


@pytest.mark.parametrize("authority_field", [
    "automatic_real_order_submission",
    "real_order_authority",
    "real_order_submission_enabled",
    "real_order_submission",
    "real_orders_allowed",
    "real_trading_enabled",
    "order_authority",
])
def test_all_governance_read_and_completion_paths_share_authority_synonyms(
    authority_field,
):
    from server.engine import strategy_governance as governance

    nested = {"outer": [{"inner": {authority_field: True}}]}
    with pytest.raises(RuntimeError, match=authority_field):
        governance._require_no_real_order_authority(nested)

    orchestration_payload, exit_code, _ = _deploy_validation_payload(
        "COMPLETED"
    )
    orchestration_payload["nested_contract"] = nested
    with pytest.raises(ValueError, match=authority_field):
        orchestrator.validate_governance_safety_contract(
            orchestration_payload
        )
    with pytest.raises(ValueError, match="安全字段无效"):
        daily.validate_cli_result(orchestration_payload, exit_code)


@pytest.mark.parametrize("orchestration_status", ["COMPLETED", orchestrator.NOT_DUE])
def test_deploy_validator_binds_success_to_exact_expected_build_sha(
    orchestration_status,
):
    payload, exit_code, disposition = _deploy_validation_payload(
        orchestration_status
    )
    expected_sha = "a" * 40

    assert daily.validate_cli_result(
        payload, exit_code, expected_build_sha=expected_sha,
    ) == disposition

    if orchestration_status == "COMPLETED":
        payload["build_commit_sha"] = "b" * 40
    else:
        payload["current_run"]["build_commit_sha"] = "b" * 40
    with pytest.raises(ValueError):
        daily.validate_cli_result(
            payload, exit_code, expected_build_sha=expected_sha,
        )


def test_daily_release_argument_forces_exact_build_orchestration(
    monkeypatch, capsys,
):
    engine = _CalendarEngine("2026-08-21")
    expected_sha = "c" * 40
    captured = {}

    def orchestrate(**kwargs):
        captured.update(kwargs)
        return {
            "status": "ok",
            "orchestration_status": "COMPLETED",
            "reason_code": "GOVERNANCE_COMPLETED",
            "run_uid": "a" * 32,
            "trade_date": "2026-08-21",
            "build_commit_sha": expected_sha,
            "summary": {},
            "allocations": [{
                "target_type": "CASH",
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }

    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(daily, "orchestrate_strategy_governance", orchestrate)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_strategy_governance_daily.py",
            "--expected-build-sha",
            expected_sha,
        ],
    )

    assert daily.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert captured["ensure_build_commit_sha"] == expected_sha
    assert captured["allow_revision"] is False
    assert payload["build_commit_sha"] == expected_sha
    assert payload["automatic_real_order_submission"] is False


def test_daily_completed_output_does_not_mask_real_order_authority_drift(
    monkeypatch, capsys,
):
    engine = _CalendarEngine("2026-08-21")

    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        daily,
        "orchestrate_strategy_governance",
        lambda **_kwargs: {
            "status": "ok",
            "orchestration_status": "COMPLETED",
            "reason_code": "GOVERNANCE_COMPLETED",
            "run_uid": "a" * 32,
            "trade_date": "2026-08-21",
            "build_commit_sha": "c" * 40,
            "summary": {},
            "allocations": [{
                "target_type": "CASH",
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": True,
            "real_order_authority": False,
        },
    )
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 4
    payload = json.loads(capsys.readouterr().out)
    assert payload["automatic_real_order_submission"] is False
    assert payload["real_order_authority"] is False
    assert payload["orchestration_status"] == daily.PROGRAM_ERROR
    assert payload["reason_code"] == "INVALID_ORCHESTRATION_OUTPUT_CONTRACT"
    assert payload["allocations"] == [{
        "target_type": "CASH",
        "simulated_weight_pct": 100.0,
        "real_order_authority": False,
    }]
    assert daily.validate_cli_result(payload, 4) == "program_error"
