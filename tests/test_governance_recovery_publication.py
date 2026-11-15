"""Resume governance only from a proven newer formal score publication."""

import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime as runtime
from server.common import component_release, daily_delivery_control as control, formal_score_recovery as recovery
from server.common.formal_score_publication import FORMAL_SCORE_WORK_SCHEMA, FormalScoreContractError
from tests.score_snapshot_helpers import scored_publication


DAY = "2026-11-13"
NOW = datetime(2026, 11, 15, 22, 45)
OLD_START = datetime(2026, 11, 15, 21, 42, 50)
OLD_FINISH = datetime(2026, 11, 15, 21, 46, 10)
NEW_START = datetime(2026, 11, 15, 22, 32, 44)
NEW_FINISH = datetime(2026, 11, 15, 22, 38, 30)
CONTRACT, OLD_BUILD, PUBLISHER, NEXT_BUILD = ("a" * 40, "b" * 40, "c" * 40, "d" * 40)
STRATEGY = "e" * 64
OLD_UID, NEW_UID = "1" * 32, "2" * 32


@pytest.fixture
def rerun(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'governance-recovery.db'}")
    control.privileged_migrate_daily_delivery_schema(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INT)")
        conn.execute(text("INSERT INTO si_trade_calendar VALUES (:date, 1)"), {"date": DAY})
        conn.exec_driver_sql("""
            CREATE TABLE st_scheduled_task_history (
                run_uid TEXT PRIMARY KEY, task_type TEXT, status TEXT, finished_at TEXT, exit_code INT,
                build_sha TEXT, scheduler_instance_id TEXT, output TEXT)
        """)
        conn.exec_driver_sql("""
            CREATE TABLE st_recommended_run_history (
                run_uid TEXT PRIMARY KEY, scheduler_job_id TEXT, trade_date TEXT,
                status TEXT, published_at TEXT, canonical_pool_sha256 TEXT)
        """)

    session = {"trade_date": DAY, "release_id": CONTRACT, "strategy_release_id": STRATEGY}
    wall_clock = [OLD_START]
    monkeypatch.setattr(control, "_control_now", lambda: wall_clock[0])
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    control.start_daily_stage_attempt(engine, **session, scheduler_run_uid=OLD_UID,
                                      stage_name="strategy_governance_daily", lease_owner="old-owner")
    with engine.begin() as conn:
        control.finish_daily_stage_attempt(
            conn, scheduler_run_uid=OLD_UID, status="failed",
            error_code="GOVERNANCE_PROGRAM_ERROR", now=OLD_FINISH,
        )
        terminal = control.persist_terminal_delivery_receipt(
            conn, now=OLD_FINISH,
            receipt=control.build_terminal_delivery_receipt(
                session=control.ensure_daily_delivery_session(conn, **session),
                scheduler_run_uid=OLD_UID, stage_name="PROGRAM", status="BLOCKED",
                strategy_release_id=STRATEGY, retryable=False, error_code="GOVERNANCE_PROGRAM_ERROR",
            ),
        )
        conn.execute(text("""
            INSERT INTO st_scheduled_task_history
            VALUES (:uid,'strategy_governance_daily','failed','2026-11-15 21:46:09',4,:build,'old-owner','')
        """), {"uid": OLD_UID, "build": OLD_BUILD})

    wall_clock[0] = NEW_START
    control.start_daily_stage_attempt(engine, **session, scheduler_run_uid=NEW_UID,
                                      stage_name="analysis_fast", lease_owner="new-owner", lease_seconds=900)
    receipt = scored_publication(trade_date=DAY, run_uid=NEW_UID, build_sha=PUBLISHER)
    receipt = {**receipt, "published_at": "2026-11-15T22:38:29"}
    receipt = {**receipt, "receipt_id": control.canonical_sha256(
        {key: value for key, value in receipt.items() if key != "receipt_id"}
    )}
    work = {**receipt, "schema": FORMAL_SCORE_WORK_SCHEMA}
    audit_output = json.dumps({
        "replay_output": json.dumps(work) + "\n" + json.dumps(control.analysis_publication_reference(receipt)),
    })
    with engine.begin() as conn:
        control.finish_daily_stage_attempt(
            conn, scheduler_run_uid=NEW_UID, status="success", output_dataset_id=NEW_UID, now=NEW_FINISH,
        )
        conn.execute(text("""
            INSERT INTO st_scheduled_task_history
            VALUES (:uid,'analysis_fast','success','2026-11-15 22:33:13',0,:build,'new-owner',:output)
        """), {"uid": NEW_UID, "build": PUBLISHER, "output": audit_output})
        conn.execute(text("""
            INSERT INTO st_recommended_run_history
            VALUES (:uid,:uid,:date,'done',:stamp,:pool)
        """), {"uid": NEW_UID, "date": DAY, "stamp": receipt["published_at"], "pool": receipt["canonical_pool_sha256"]})

    live = {"scope": "LINUX", "linux_build_sha": NEXT_BUILD, "contract_build_sha": CONTRACT,
            "parent_linux_build_sha": PUBLISHER}
    monkeypatch.setattr(runtime, "authoritative_closed_trade_date", lambda *_a, **_k: DAY)
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: live["linux_build_sha"])
    monkeypatch.setattr(runtime, "runtime_contract_build_sha", lambda **_: CONTRACT)
    monkeypatch.setattr(runtime, "_daily_delivery_requires_production_runtime", lambda: True)
    monkeypatch.setattr(runtime, "_scheduler_executor_role", lambda _: runtime.SCHEDULER_OWNER_LINUX)
    monkeypatch.setattr(runtime, "strategy_release_identity", lambda: STRATEGY)
    monkeypatch.setattr(component_release, "load_runtime_component_release", lambda: live)
    monkeypatch.setattr(recovery, "_compatible_recovery_builds",
                        lambda conn, active: {active["linux_build_sha"], OLD_BUILD, PUBLISHER})
    reads = []

    def published(connection, date, *, decision_at, run_uid):
        assert connection.engine is engine and (date, decision_at, run_uid) == (DAY, NOW, NEW_UID)
        reads.append(run_uid)
        return receipt

    monkeypatch.setattr(recovery, "load_published_analysis_receipt", published)
    try:
        yield engine, live, terminal, reads
    finally:
        engine.dispose()


@pytest.mark.parametrize("published_by_active_runtime", [True, False])
def test_reopens_exact_nonretryable_governance_on_fenced_new_publication(rerun, published_by_active_runtime):
    engine, live, terminal, reads = rerun
    if published_by_active_runtime:
        live.update(linux_build_sha=PUBLISHER, parent_linux_build_sha=OLD_BUILD)

    assert runtime._daily_result_recovery_target(engine, now=NOW) == DAY
    assert reads == [NEW_UID]
    with engine.connect() as conn:
        status, owner, generation = conn.execute(text(
            "SELECT status, canonical_receipt_uid, latest_generation FROM st_daily_delivery_session"
        )).one()
        stored = conn.execute(text("SELECT receipt_json FROM st_daily_delivery_receipt")).scalar_one()
    assert (status, owner, generation) == ("RUNNING", terminal["receipt_uid"], terminal["generation"])
    assert json.loads(stored) == terminal


@pytest.mark.parametrize(("statement", "expected_reads"), [
    ("UPDATE st_daily_stage_attempt SET status='RUNNING' WHERE scheduler_run_uid=:old", []),
    ("UPDATE st_scheduled_task_history SET scheduler_instance_id='different' WHERE run_uid=:old", []),
    ("UPDATE st_daily_stage_attempt SET fencing_token=1 WHERE scheduler_run_uid=:new", []),
    ("UPDATE st_daily_stage_attempt SET shard_id='other' WHERE scheduler_run_uid=:new", []),
    ("UPDATE st_daily_stage_attempt SET status='RUNNING' WHERE scheduler_run_uid=:new", []),
    ("UPDATE st_scheduled_task_history SET build_sha=:publisher WHERE run_uid=:old", []),
    ("UPDATE st_scheduled_task_history SET exit_code=1 WHERE run_uid=:new", []),
    ("UPDATE st_recommended_run_history SET canonical_pool_sha256=:wrong_sha WHERE run_uid=:new", [NEW_UID]),
])
def test_keeps_terminal_governance_block_without_exact_supersession(rerun, statement, expected_reads):
    engine, _, _, reads = rerun
    with engine.begin() as conn:
        conn.execute(text(statement), {"old": OLD_UID, "new": NEW_UID, "publisher": PUBLISHER, "wrong_sha": "5" * 64})
    with pytest.raises((RuntimeError, FormalScoreContractError), match="seal differs|completed publication differs"):
        runtime._daily_result_recovery_target(engine, now=NOW)
    assert reads == expected_reads



def test_windows_owner_keeps_existing_terminal_block_behavior(rerun, monkeypatch):
    engine, _, _, reads = rerun
    monkeypatch.setattr(runtime, "_scheduler_executor_role", lambda _: runtime.SCHEDULER_OWNER_WINDOWS_QMT)
    with pytest.raises(RuntimeError, match="receipt seal differs"):
        runtime._daily_result_recovery_target(engine, now=NOW)
    assert reads == []


def test_requires_published_score_from_current_linux_or_direct_parent(rerun):
    engine, live, _, reads = rerun
    live["parent_linux_build_sha"] = "f" * 40
    with pytest.raises(RuntimeError, match="receipt seal differs"):
        runtime._daily_result_recovery_target(engine, now=NOW)
    assert reads == []
