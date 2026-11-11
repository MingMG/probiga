import hashlib
import json
import sys
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime
from server.common import authoritative_market_clock, daily_delivery_control
from server.common.analysis_pool_receipt import (
    FROZEN_INITIAL_ANNOUNCEMENT_SCHEMA,
    build_pool_manifest,
    build_publication_receipt,
    build_score_snapshot,
    decode_score_snapshot,
)
from server.engine import strategy_governance_orchestrator
from tools import run_strategy_governance_daily as governance_tool
from tools.strategy_governance_task_contract import TASK
from tests.score_snapshot_helpers import scored_publication


DAY = "2026-11-11"
BUILD = "a" * 40
SCORE_RUN = "b" * 32


def _natural_governance_launch(monkeypatch, *, hour, minute, day=11):
    now = datetime(2026, 11, day, hour, minute, tzinfo=authoritative_market_clock.PRODUCTION_TIMEZONE)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INT)"))
        connection.execute(text(
            "INSERT INTO si_trade_calendar VALUES ('2026-11-10',1),('2026-11-11',1),('2026-11-12',1)"
        ))
    row = {**TASK, "_trigger_source": "scheduled"}
    if day != 11:
        # The nightly recovery selects the existing delivery session across midnight.
        row["_scheduler_target_trade_date"] = DAY
    target = scheduler_runtime._task_dispatch_date(row, engine, now=now)
    assert target == DAY
    row = scheduler_runtime._task_argument_row(row, now=now, target_date=target, engine=engine)
    row = scheduler_runtime._bind_release_validation_target(row, engine, dispatch_date=target, now=now)
    args = scheduler_runtime._build_task_args(row, TASK["script_path"], target)
    assert args == ["--limit", "500", "--trade-date", DAY]
    monkeypatch.setattr(sys, "argv", [TASK["script_path"], *args])
    monkeypatch.setenv("PROBIGA_SCHEDULER_HISTORY_RUN_UID", "c" * 32)
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_ID", "85")
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", TASK["task_type"])
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD)
    monkeypatch.setattr(governance_tool, "_load_project_env", lambda: None)
    monkeypatch.setattr(governance_tool, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(governance_tool, "strategy_governance_database_deferred", lambda: False)
    return engine, now


def _initial_score_publication(cutoff):
    decision = datetime.fromisoformat(cutoff)
    context = {"trade_date": DAY, "decision_known_at": cutoff}
    context["context_sha256"] = daily_delivery_control.canonical_sha256(context)
    announcement = {
        "schema": FROZEN_INITIAL_ANNOUNCEMENT_SCHEMA,
        "task_type": "qmt_announcement_pit",
        "trade_date": DAY,
        "scheduler_run_uid": "1" * 32,
        "evidence_sha256": "2" * 64,
        "input_receipt_root_sha256": "3" * 64,
        "producer_build_sha": BUILD,
        "contract_release_id": BUILD,
        "batch_id": "verified-announcement-batch",
        "batch_root_hash": "4" * 64,
        "source": "cninfo.announcement",
        "fact_cutoff_at": f"{DAY}T18:20:00",
        "received_at": (decision - timedelta(minutes=10)).isoformat(),
        "finished_at": (decision - timedelta(minutes=5)).isoformat(),
    }
    core = {
        "kline": {"stage": "qmt_stock_daily_canonical", "dataset_id": "daily", "root": "5" * 64},
        "turnover": {"stage": "target_turnover_snapshot", "dataset_id": "turnover", "root": "6" * 64},
        "financial": {"stage": "stock_finance", "dataset_id": "finance", "root": "7" * 64},
        "announcement": {
            "stage": "qmt_announcement_pit", "dataset_id": announcement["scheduler_run_uid"],
            "root": announcement["evidence_sha256"], "input_root": announcement["batch_root_hash"],
        },
    }
    baseline = decode_score_snapshot(scored_publication(trade_date=DAY)["score_snapshot"])
    rows = baseline["analysis_rows"]
    envelope = build_score_snapshot(
        trade_date=DAY, decision_at=cutoff, run_uid=SCORE_RUN, build_sha=BUILD,
        analysis_rows=rows,
        scored_rows=[{**row, "flow_input_decision_at": cutoff} for row in baseline["scored_rows"]],
        fixed_market_context=context, frozen_announcement_source=announcement, frozen_core_inputs=core,
    )
    manifest = build_pool_manifest(
        trade_date=DAY, analysis_rows=rows, recommendation_rows=[], score_snapshot=envelope,
    )
    receipt = build_publication_receipt(
        manifest=manifest, run_uid=SCORE_RUN, publisher_task_type="analysis_fast", build_sha=BUILD,
        published_at=(decision + timedelta(minutes=1)).isoformat(), score_snapshot=envelope,
    )
    return receipt, decode_score_snapshot(envelope)


def _mock_published_score(monkeypatch, *, receipt, snapshot):
    def load_score(_engine, target, *, capture_score):
        assert target == DAY
        capture_score(snapshot)
        return receipt

    monkeypatch.setattr(daily_delivery_control, "load_published_analysis_receipt", load_score)


@pytest.mark.parametrize(("day", "hour", "minute", "cutoff"), [
    (11, 22, 35, f"{DAY}T22:20:00"),
    (11, 23, 48, f"{DAY}T23:42:00"),
    (12, 0, 48, "2026-11-12T00:42:00"),
], ids=["on-time", "same-day-recovery", "next-day-recovery"])
def test_natural_governance_child_consumes_the_scheduler_builds_frozen_score(
    monkeypatch, day, hour, minute, cutoff,
):
    engine, now = _natural_governance_launch(monkeypatch, day=day, hour=hour, minute=minute)
    assert authoritative_market_clock.authoritative_closed_trade_date(engine, now=now) == DAY
    receipt, snapshot = _initial_score_publication(cutoff)
    _mock_published_score(monkeypatch, receipt=receipt, snapshot=snapshot)
    observed = {}

    class GovernanceEntered(Exception):
        pass

    def capture_governance(**kwargs):
        observed.update(kwargs)
        raise GovernanceEntered

    monkeypatch.setattr(governance_tool, "orchestrate_strategy_governance", capture_governance)
    with pytest.raises(GovernanceEntered):
        governance_tool.main()
    assert observed["ensure_build_commit_sha"] == BUILD
    assert observed["requested_trade_date"] == DAY
    assert observed["fixed_scoring"] == {
        "trade_date": DAY, "decision_known_at": cutoff, "analysis_run_uid": SCORE_RUN,
        "fixed_market_context": snapshot["fixed_market_context"],
    }
    assert observed["allow_revision"] is False


@pytest.mark.parametrize(("field", "value"), [
    ("input_decision_at", f"{DAY}T22:20:00"),
    ("frozen_announcement_source", None),
    ("frozen_core_inputs", None),
    ("fixed_market_context", None),
])
def test_governance_rejects_mismatched_or_missing_frozen_input_before_writing(
    monkeypatch, capsys, field, value,
):
    _engine, now = _natural_governance_launch(monkeypatch, hour=23, minute=48)
    receipt, snapshot = _initial_score_publication(f"{DAY}T23:42:00")
    _mock_published_score(monkeypatch, receipt=receipt, snapshot={**snapshot, field: value})
    monkeypatch.setattr(strategy_governance_orchestrator, "_exchange_now", lambda _value: now)
    monkeypatch.setattr(strategy_governance_orchestrator, "_canonical_run", lambda *_: None)
    monkeypatch.setattr(
        strategy_governance_orchestrator, "persist_blocked_attempt",
        lambda *_args, **_kwargs: {"audit_id": "a" * 32, "audit_hash": "b" * 64},
    )
    monkeypatch.setattr(governance_tool, "_bootstrap_execution_adapters", lambda: None)
    monkeypatch.setattr(governance_tool, "_capture_industry_history", lambda *_: pytest.fail("industry writer entered"))
    monkeypatch.setattr(
        "server.engine.strategy_governance.governance_snapshot", lambda **_: pytest.fail("governance writer entered"),
    )
    assert governance_tool.main() != 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["reason_code"] == "FROZEN_SCORING_UNAVAILABLE"
    assert payload["target_trade_date"] == DAY


def test_governance_does_not_publish_another_day_while_the_claimed_session_is_open(monkeypatch, capsys):
    engine, now = _natural_governance_launch(monkeypatch, hour=16, minute=0)
    assert authoritative_market_clock.authoritative_closed_trade_date(engine, now=now) == "2026-11-10"
    monkeypatch.setattr(strategy_governance_orchestrator, "_exchange_now", lambda _value: now)
    monkeypatch.setattr(governance_tool, "_bootstrap_execution_adapters", lambda: None)
    monkeypatch.setattr(governance_tool, "_capture_industry_history", lambda *_: pytest.fail("industry writer entered"))

    def unavailable_score(_engine, target, *, capture_score):
        assert target == DAY
        raise daily_delivery_control.DailyDeliveryControlError("the claimed session has no sealed score yet")

    monkeypatch.setattr(daily_delivery_control, "load_published_analysis_receipt", unavailable_score)
    observed = {}
    orchestrate = governance_tool.orchestrate_strategy_governance

    def capture_governance(**kwargs):
        result = orchestrate(**kwargs)
        observed.update(result)
        return result

    monkeypatch.setattr(governance_tool, "orchestrate_strategy_governance", capture_governance)
    governance_tool.main()
    result = json.loads(capsys.readouterr().out)
    assert observed["orchestration_status"] == "NOT_DUE"
    assert observed["reason_code"] == "SESSION_NOT_CLOSED"
    assert result["status"] in {"blocked", "not_due"}
    assert result["requested_trade_date"] == DAY
    assert result["target_trade_date"] == "2026-11-10"


@pytest.mark.parametrize("scheduled_build, explicit_args", [
    ("", []), (BUILD, ["--expected-build-sha", "e" * 40]),
])
def test_natural_governance_requires_its_bound_build_before_opening_database(
    monkeypatch, scheduled_build, explicit_args,
):
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", TASK["task_type"])
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", scheduled_build)
    monkeypatch.setattr(sys, "argv", [TASK["script_path"], "--limit", "500", *explicit_args])
    monkeypatch.setattr(governance_tool, "_create_tool_engine", lambda: pytest.fail("database opened"))
    with pytest.raises(SystemExit) as rejected:
        governance_tool.main()
    assert rejected.value.code == 2


@pytest.mark.parametrize("changed_field, changed_value", [
    (None, None),
    ("publication_proof", None),
    ("decision_known_at", f"{DAY}T22:35:00"),
    ("analysis_score_snapshot_sha256", "f" * 64),
    ("publisher_run_uid", "e" * 32),
    ("build_sha", "e" * 40),
])
@pytest.mark.parametrize("cutoff", [f"{DAY}T22:20:00", "2026-11-12T00:42:00"])
def test_frozen_delivery_reads_the_governance_runs_consumed_score_from_the_database(
    cutoff, changed_field, changed_value,
):
    snapshot = {"payload_sha256": "d" * 64, "input_decision_at": cutoff}
    proof = {
        "status": "COMPLETED", "publisher_run_uid": SCORE_RUN, "build_sha": BUILD,
        "analysis_score_snapshot_sha256": snapshot["payload_sha256"],
        "decision_known_at": snapshot["input_decision_at"],
    }
    if changed_field == "publication_proof":
        proof = None
    elif changed_field is not None:
        proof[changed_field] = changed_value
    result_json = json.dumps({"candidate_source": {"publication_proof": proof}})
    governance_row = {
        "run_uid": "c" * 32, "build_commit_sha": BUILD,
        "result_hash": hashlib.sha256(result_json.encode("utf-8")).hexdigest(),
    }
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_strategy_governance_run (
                run_uid TEXT PRIMARY KEY, result_hash TEXT, result_json TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run VALUES (:run_uid, :result_hash, :result_json)
        """), {**governance_row, "result_json": result_json})
        kwargs = dict(governance_row=governance_row, analysis_run_uid=SCORE_RUN, score_snapshot=snapshot)
        if changed_field is None:
            scheduler_runtime._require_governance_frozen_score(connection, **kwargs)
        else:
            with pytest.raises(RuntimeError, match="did not consume the exact frozen score"):
                scheduler_runtime._require_governance_frozen_score(connection, **kwargs)
    engine.dispose()
