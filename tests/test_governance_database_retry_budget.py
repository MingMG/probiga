"""Governance retry budgets are scoped to the exact authorized scoring publication."""

from datetime import datetime
import json

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime as runtime
from server.common import daily_delivery_control as control


DAY = "2026-11-13"
BUILD = "a" * 40
CONTRACT = "b" * 40
SCORE_UID = "c" * 32
SCORE_SHA = "d" * 64
TRANSIENT = "GOVERNANCE_DATABASE_TRANSIENT"
UNAVAILABLE = (False, "governance_database_retry_evidence_unavailable")
EXHAUSTED = (False, "governance_database_retry_exhausted")
NOW = datetime(2026, 11, 16, 0, 45)


@pytest.fixture
def attempts(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'governance.db'}")
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TABLE st_daily_stage_attempt (
                session_uid TEXT, stage_name TEXT, shard_id TEXT, scheduler_run_uid TEXT,
                status TEXT, started_at TEXT, finished_at TEXT, lease_owner TEXT,
                error_code TEXT, fencing_token INTEGER)
        """)
        conn.exec_driver_sql("""
            CREATE TABLE st_scheduled_task_history (
                run_uid TEXT PRIMARY KEY, scheduler_instance_id TEXT, status TEXT,
                exit_code INTEGER, finished_at TEXT, build_sha TEXT, task_type TEXT, output TEXT)
        """)
    selected = {
        "publisher_task_type": "analysis_fast", "trade_date": DAY,
        "run_uid": SCORE_UID, "build_sha": BUILD, "published_at": "2026-11-16T00:01:00",
        "score_snapshot": {"payload_sha256": SCORE_SHA},
    }

    def load_exact(connection, target, *, decision_at, run_uid):
        assert connection.engine is engine and decision_at == NOW
        assert target == selected["trade_date"] and run_uid == selected["run_uid"]
        return selected

    monkeypatch.setattr(runtime, "runtime_contract_build_sha", lambda **_: CONTRACT)
    monkeypatch.setattr(control, "load_published_analysis_receipt", load_exact)

    def append(
        number, *, build=BUILD, uid=SCORE_UID, score_sha=SCORE_SHA, target=DAY,
        error_code=TRANSIENT, start="2026-11-16 00:10:00", audit_owner="exact-owner", missing=False,
    ):
        audit = {
            "schema": "probiga.strategy-governance-daily-result.v1",
            "status": "blocked", "orchestration_status": "NOT_READY", "retryable": True,
            "reason_code": error_code, "target_trade_date": target, "blocking_record": {
                "analysis_run_uid": uid, "analysis_score_snapshot_sha256": score_sha,
            },
        }
        payload = json.dumps(audit)
        params = {
            "session": control.daily_session_identity(target, CONTRACT)["session_uid"],
            "uid": f"{number:032x}", "fence": number, "reason": error_code,
            "started": start, "build": build, "owner": audit_owner,
            "output": runtime._terminal_history_output(payload, "connection diagnostic", payload)
            + "\n" + runtime._retryable_blocked_marker(payload),
        }
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO st_daily_stage_attempt VALUES
                    (:session,'strategy_governance_daily','main',:uid,'BLOCKED',
                     :started,'2026-11-16 00:12:00','exact-owner',:reason,:fence)
            """), params)
            if not missing:
                conn.execute(text("""
                    INSERT INTO st_scheduled_task_history VALUES
                        (:uid,:owner,'blocked',2,'2026-11-16 00:11:59',:build,'strategy_governance_daily',:output)
                """), params)

    def allowed():
        return runtime._governance_database_retry_allowed(
            engine, trade_date=selected["trade_date"], build_sha=selected["build_sha"],
            score_run_uid=selected["run_uid"], now=NOW,
        )

    try:
        yield engine, selected, append, allowed
    finally:
        engine.dispose()


def test_only_one_retry_after_first_transient_for_same_authorized_score(attempts):
    engine, _, append, allowed = attempts
    assert allowed() == (True, "ready")
    append(1)
    assert allowed() == (True, "ready")
    append(2)
    with engine.connect() as conn:
        audit_before = conn.execute(text("SELECT output FROM st_scheduled_task_history ORDER BY run_uid")).all()
    assert allowed() == EXHAUSTED
    assert allowed() == EXHAUSTED
    with engine.connect() as conn:
        assert conn.execute(text("SELECT output FROM st_scheduled_task_history ORDER BY run_uid")).all() == audit_before


@pytest.mark.parametrize("change", ["score", "build", "trade_date"])
def test_new_authorized_scope_has_its_own_budget(attempts, change):
    _, selected, append, allowed = attempts
    append(1)
    append(2)
    if change == "score":
        selected.update(run_uid="e" * 32, published_at="2026-11-16T00:13:00",
                        score_snapshot={"payload_sha256": "f" * 64})
    elif change == "build":
        selected["build_sha"] = "e" * 40
    else:
        selected["trade_date"] = "2026-11-16"
    assert allowed() == (True, "ready")


def test_verified_different_older_scope_terminalizes_while_new_score_is_published(attempts):
    _, selected, append, allowed = attempts
    append(1)
    selected.update(run_uid="e" * 32, published_at="2026-11-16T00:11:00",
                    score_snapshot={"payload_sha256": "f" * 64})
    assert allowed() == (True, "ready")
    append(2, uid=selected["run_uid"], score_sha="f" * 64, start="2026-11-16 00:10:30")
    assert allowed() == (True, "ready")
    append(3, uid=selected["run_uid"], score_sha="f" * 64, start="2026-11-16 00:11:30")
    assert allowed() == EXHAUSTED


@pytest.mark.parametrize("fields", [
    {"uid": None}, {"score_sha": "0" * 64}, {"score_sha": "e" * 64},
    {"uid": "e" * 32}, {"missing": True}, {"audit_owner": "other-owner"},
])
def test_fails_closed_on_missing_or_mismatched_attempt_proof(attempts, fields):
    _, _, append, allowed = attempts
    append(1, **fields)
    assert allowed() == UNAVAILABLE


def test_other_blocked_reason_does_not_consume_governance_database_budget(attempts):
    _, _, append, allowed = attempts
    for attempt_no in range(1, 5):
        append(attempt_no, error_code="GOVERNANCE_EVIDENCE_NOT_READY")
    assert allowed() == (True, "ready")
