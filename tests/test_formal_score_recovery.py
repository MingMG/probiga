"""Recovery reuses only the attested original score under the next scheduler fence."""
import json
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.analysis.sync_analysis_fast import MODEL_VERSION
from server.common import formal_score_recovery as recovery
from server.common import formal_score_source_evidence as source_evidence
from server.common.analysis_pool_receipt import FROZEN_INITIAL_ANNOUNCEMENT_SCHEMA, decode_score_snapshot
from server.common.daily_delivery_control import canonical_sha256
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.formal_score_publication import (
    FormalScoreContractError, load_initial_selection, seal_initial_selection,
)
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.score_snapshot_helpers import scored_publication


TARGET = "2026-11-13"
CUTOFF = "2026-11-15T19:28:45"
PROPOSAL = "2026-11-15T20:15:00"
PARENT, CURRENT, INTERMEDIATE = "1" * 40, "2" * 40, "f" * 40
ORIGINAL_RUN, FIRST_RETRY, NEXT_RETRY = "3" * 32, "4" * 32, "5" * 32
ATTEMPTS = {
    "qmt_stock_daily_canonical": "6" * 32, "target_turnover_snapshot": "7" * 32,
    "capital_flow_batch_fast": "8" * 32, "qmt_announcement_pit": "9" * 32,
    "stock_finance": "a" * 32,
}
RUNTIME = {
    "scope": "LINUX", "linux_build_sha": CURRENT, "windows_build_sha": PARENT,
    "contract_build_sha": PARENT, "parent_linux_build_sha": PARENT, "contract_sha256": "a" * 64,
}
ORIGIN_COMPONENT = {**RUNTIME, "scope": "COORDINATED", "linux_build_sha": PARENT}


def _record(engine, *, run_uid, build, historical=None, published=False, started=PROPOSAL):
    status = ("SUCCESS" if published else "FAILED") if historical else "RUNNING"
    finish = "2026-11-15T20:02:00.987654" if historical else None
    score_cutoff = historical["decision_at"] if historical and build == PARENT else started.split(".")[0]
    output = json.dumps(historical) if historical else ""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO st_daily_stage_attempt
                (session_uid, scheduler_run_uid, stage_name, status, started_at, finished_at)
            VALUES ('session', :uid, 'analysis_fast', :status, :started, :finish)
        """), {"uid": run_uid, "status": status, "started": started, "finish": finish})
        conn.execute(text("""
            INSERT INTO st_scheduled_task_history
                (run_uid, task_type, build_sha, status, finished_at, exit_code, output)
            VALUES (:uid, 'analysis_fast', :build, :status, :finish, :exit, :output)
        """), {"uid": run_uid, "build": build, "status": status.lower(), "finish": finish,
               "exit": 0 if historical else None, "output": output})
        conn.execute(text("""
            INSERT INTO st_recommended_run_history
                (run_uid, scheduler_job_id, trade_date, min_score, top_n, execution_time, published_at, canonical_pool_sha256)
            VALUES (:uid, :uid, :date, 62, 80, :cutoff, :published, NULL)
        """), {"uid": run_uid, "date": TARGET, "cutoff": score_cutoff, "published": finish if published else None})


@pytest.fixture
def archived(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'score-recovery.db'}")
    privileged_migrate_formal_analysis_schema(engine)
    with engine.begin() as conn:
        for ddl in (
            "CREATE TABLE st_daily_delivery_session (session_uid TEXT PRIMARY KEY,trade_date TEXT,release_id TEXT,strategy_release_id TEXT)",
            "CREATE TABLE st_daily_stage_attempt (id INTEGER PRIMARY KEY,session_uid TEXT,scheduler_run_uid TEXT,stage_name TEXT,status TEXT,started_at TEXT,finished_at TEXT)",
            "CREATE TABLE st_scheduled_task_history (run_uid TEXT PRIMARY KEY,task_type TEXT,build_sha TEXT,status TEXT,finished_at TEXT,exit_code INTEGER,output TEXT)",
            "CREATE TABLE st_recommended_run_history (run_uid TEXT PRIMARY KEY,scheduler_job_id TEXT,trade_date TEXT,min_score REAL,top_n INTEGER,execution_time TEXT,published_at TEXT,canonical_pool_sha256 TEXT)",
        ):
            conn.exec_driver_sql(ddl)
        conn.execute(text("INSERT INTO st_daily_delivery_session VALUES ('session',:day,:contract,:strategy)"),
                     {"day": TARGET, "contract": PARENT, "strategy": "b" * 64})
    monkeypatch.setattr(recovery, "load_runtime_component_release", lambda: RUNTIME)
    monkeypatch.setattr(recovery, "load_component_attestation", lambda _, build: RUNTIME if build == CURRENT else ORIGIN_COMPONENT)
    monkeypatch.setattr(recovery, "require_compatible_component_build", lambda _, parent, current: ORIGIN_COMPONENT)
    monkeypatch.setattr(recovery, "strategy_release_identity", lambda: "b" * 64)
    monkeypatch.setattr(recovery, "_same_scorer", lambda parent, current: (parent, current) == (PARENT, CURRENT))

    def original_sources(authority, *, trade_date, decision_at, build_sha):
        assert authority is engine and exact_time(decision_at) == CUTOFF
        assert trade_date == TARGET and build_sha == PARENT
        return ATTEMPTS

    monkeypatch.setattr(source_evidence, "frozen_initial_source_runs", original_sources)
    snapshot = decode_score_snapshot(scored_publication(trade_date=TARGET)["score_snapshot"])
    scored = pd.DataFrame([{**row, "flow_input_decision_at": CUTOFF} for row in snapshot["scored_rows"]])
    context = {"trade_date": TARGET, "decision_known_at": CUTOFF}
    announcement = {
        "schema": FROZEN_INITIAL_ANNOUNCEMENT_SCHEMA, "task_type": "qmt_announcement_pit",
        "trade_date": TARGET, "scheduler_run_uid": ATTEMPTS["qmt_announcement_pit"],
        "evidence_sha256": "c" * 64, "input_receipt_root_sha256": "d" * 64,
        "producer_build_sha": PARENT, "contract_release_id": PARENT,
        "batch_id": "retained", "batch_root_hash": "e" * 64, "source": "cninfo.announcement",
        "fact_cutoff_at": TARGET + "T23:59:59.999999", "received_at": "2026-11-15T13:47:45.256897",
        "finished_at": "2026-11-15T19:27:56.123456",
    }
    scored.attrs.update(
        fixed_market_context={**context, "context_sha256": canonical_sha256(context)},
        frozen_announcement_source=announcement,
        frozen_core_inputs={
            "kline": {"stage": "qmt_stock_daily_canonical", "dataset_id": "daily", "root": "7" * 64},
            "turnover": {"stage": "target_turnover_snapshot", "dataset_id": "turnover", "root": "8" * 64},
            "financial": {"stage": "stock_finance", "dataset_id": "finance", "root": "9" * 64},
            "announcement": {"stage": "qmt_announcement_pit", "dataset_id": announcement["scheduler_run_uid"],
                             "root": announcement["evidence_sha256"], "input_root": announcement["batch_root_hash"]},
        },
    )
    generation = canonical_sha256({
        "schema": "probiga.initial-selection-generation.v1", "trade_date": TARGET,
        "decision_at": CUTOFF, "build_sha": PARENT, "model_version": MODEL_VERSION,
        "source_runs_sha256": canonical_sha256(ATTEMPTS), "min_score": 62.0, "top_n": 80,
    })
    original = seal_initial_selection(
        FormalAnalysisStore(engine), generation=generation, trade_date=TARGET,
        decision_at=CUTOFF, run_uid=ORIGINAL_RUN, build_sha=PARENT, analysis_rows=snapshot["analysis_rows"],
        recommendation_rows=[], scored=scored, market_mood_score=66, flow_date=TARGET, hot_date=TARGET,
    )["reference"]
    _record(engine, run_uid=ORIGINAL_RUN, build=PARENT, historical=original, started=CUTOFF)
    _record(engine, run_uid=FIRST_RETRY, build=CURRENT)
    try:
        yield engine, original
    finally:
        engine.dispose()


def exact_time(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def _recover(engine, run_uid=FIRST_RETRY):
    return recovery.recover_initial_selection(
        engine, trade_date=TARGET, proposed_cutoff=PROPOSAL,
        run_uid=run_uid, build_sha=CURRENT, min_score=62.0, top_n=80, model_version=MODEL_VERSION,
    )


def test_rebind_older_cutoff_same_immutable_selection_with_fractional_audit_times(archived, monkeypatch):
    engine, original = archived
    monkeypatch.setattr(FormalAnalysisStore, "load_artifacts", lambda *_: pytest.fail("recovery reloaded score rows"))
    recovered = _recover(engine)["reference"]
    assert recovered["run_uid"] == FIRST_RETRY and recovered["build_sha"] == CURRENT
    assert recovered["decision_at"] == CUTOFF
    assert recovered["selection_sha256"] == original["selection_sha256"]
    assert recovered["generation_sha256"] == original["generation_sha256"]
    recovery.verify_recovered_initial_selection(engine, recovered, proposed_cutoff=PROPOSAL)
    with pytest.raises(FormalScoreContractError, match="earlier"):
        recovery.verify_recovered_initial_selection(engine, recovered, proposed_cutoff=CUTOFF)


@pytest.mark.parametrize("failed_build", [CURRENT, PARENT])
def test_recover_failed_rebind_but_never_search_past_a_success(archived, failed_build):
    engine, original = archived
    if failed_build == CURRENT:
        first = _recover(engine)["reference"]
    else:
        first = load_initial_selection(
            FormalAnalysisStore(engine), generation=original["generation_sha256"],
            run_uid=FIRST_RETRY, build_sha=PARENT,
        )["reference"]
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM st_daily_stage_attempt WHERE scheduler_run_uid=:uid"), {"uid": FIRST_RETRY})
        conn.execute(text("DELETE FROM st_scheduled_task_history WHERE run_uid=:uid"), {"uid": FIRST_RETRY})
        conn.execute(text("DELETE FROM st_recommended_run_history WHERE run_uid=:uid"), {"uid": FIRST_RETRY})
    _record(engine, run_uid=FIRST_RETRY, build=failed_build, historical=first)
    _record(engine, run_uid=NEXT_RETRY, build=CURRENT)
    assert _recover(engine, NEXT_RETRY)["reference"]["selection_sha256"] == first["selection_sha256"]
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_daily_stage_attempt SET status='SUCCESS' WHERE scheduler_run_uid=:uid"),
                     {"uid": FIRST_RETRY})
    assert _recover(engine, NEXT_RETRY) is None


def test_recovery_fails_closed_for_wrong_source_lineage(archived, monkeypatch):
    engine, _ = archived
    wrong = {**ATTEMPTS, "stock_finance": "f" * 32}
    monkeypatch.setattr(source_evidence, "frozen_initial_source_runs", lambda *_a, **_kw: wrong)
    with pytest.raises(FormalScoreContractError, match="original input generation differs"):
        _recover(engine)


def test_only_provably_absent_current_ledger_can_use_existing_signed_lite(monkeypatch):
    def load(_connection, build):
        if build == CURRENT:
            raise RuntimeError("current unavailable")
        assert build == PARENT
        return ORIGIN_COMPONENT

    called = []
    monkeypatch.setattr(recovery, "load_component_attestation", load)
    monkeypatch.setattr(recovery, "component_release_row_state", lambda *_: "absent")
    monkeypatch.setattr(recovery, "load_runtime_component_release_lite", lambda runtime: called.append(runtime))
    assert recovery._verify_component_lineage(object(), RUNTIME) == ORIGIN_COMPONENT
    monkeypatch.setattr(recovery, "component_release_row_state", lambda *_: "present")
    with pytest.raises(FormalScoreContractError, match="cannot authenticate"):
        recovery._verify_component_lineage(object(), RUNTIME)
    assert called == [RUNTIME]


def test_recover_original_across_signed_parent_and_unpublished_interrupted_attempts(archived, monkeypatch):
    engine, original = archived
    expired_uid, stopped_uid = "e" * 32, "f" * 32
    intermediate = {**RUNTIME, "linux_build_sha": INTERMEDIATE}
    current_release = {**RUNTIME, "parent_linux_build_sha": INTERMEDIATE}
    manifests = {CURRENT: current_release, INTERMEDIATE: intermediate, PARENT: ORIGIN_COMPONENT}
    monkeypatch.setattr(recovery, "load_runtime_component_release", lambda: current_release)
    monkeypatch.setattr(recovery, "load_component_attestation", lambda _connection, build: manifests[build])
    monkeypatch.setattr(recovery, "require_compatible_component_build",
                        lambda _connection, producer, current: manifests[producer])

    first = load_initial_selection(
        FormalAnalysisStore(engine), generation=original["generation_sha256"],
        run_uid=FIRST_RETRY, build_sha=PARENT,
    )["reference"]
    expired = load_initial_selection(
        FormalAnalysisStore(engine), generation=original["generation_sha256"],
        run_uid=expired_uid, build_sha=INTERMEDIATE,
    )["reference"]
    with engine.begin() as conn:
        for table, key in (("st_daily_stage_attempt", "scheduler_run_uid"), ("st_scheduled_task_history", "run_uid"),
                           ("st_recommended_run_history", "run_uid")):
            conn.execute(text(f"DELETE FROM {table} WHERE {key}=:uid"), {"uid": FIRST_RETRY})
    _record(engine, run_uid=FIRST_RETRY, build=PARENT, historical=first)
    _record(engine, run_uid=expired_uid, build=INTERMEDIATE, historical=expired)
    _record(engine, run_uid=stopped_uid, build=INTERMEDIATE, historical=expired)
    owner_expired = (
        "TASK_OWNER_LEASE_EXPIRED: exact run lease expired; "
        "original process result retained in OBSERVED; no publication retried."
    )
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_scheduled_task_history SET output=:o,exit_code=NULL WHERE run_uid=:uid"),
                     {"uid": expired_uid, "o": json.dumps(expired) + "\n" + owner_expired})
        conn.execute(text("UPDATE st_scheduled_task_history SET output=:output,status='stopped',exit_code=-15 WHERE run_uid=:uid"),
                     {"uid": stopped_uid, "output": "调度服务停止；子进程已确认退出。\npartial log"})
        conn.execute(text("UPDATE st_daily_stage_attempt SET status='STOPPED' WHERE scheduler_run_uid=:uid"),
                     {"uid": stopped_uid})
    _record(engine, run_uid=NEXT_RETRY, build=CURRENT)

    rebound = _recover(engine, NEXT_RETRY)["reference"]
    assert rebound["run_uid"] == NEXT_RETRY
    assert rebound["selection_sha256"] == original["selection_sha256"]
    assert rebound["decision_at"] == CUTOFF
    recovery.verify_recovered_initial_selection(engine, rebound, proposed_cutoff=PROPOSAL)

    with engine.begin() as conn:
        conn.execute(text("UPDATE st_scheduled_task_history SET output=:o WHERE run_uid=:uid"),
                     {"uid": expired_uid, "o": json.dumps(expired)})
    assert _recover(engine, NEXT_RETRY) is None


@pytest.mark.parametrize("status, published", [("running", False), ("stopped", True), ("success", False)])
def test_cannot_search_past_active_or_published_attempts(archived, status, published):
    engine, _ = archived
    stopped = "调度服务停止；子进程已确认退出。"
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_scheduled_task_history SET status=:status,output=:log,finished_at=:finish "
                          "WHERE run_uid=:uid"),
                     {"uid": FIRST_RETRY, "status": status, "log": stopped, "finish": "2026-11-15T20:09:59"})
        conn.execute(text("UPDATE st_daily_stage_attempt SET status=:status,finished_at=:finish WHERE scheduler_run_uid=:uid"),
                     {"uid": FIRST_RETRY, "status": status.upper(), "finish": "2026-11-15T20:10:00"})
        if published:
            conn.execute(text("UPDATE st_recommended_run_history SET published_at=:stamp WHERE run_uid=:uid"),
                         {"uid": FIRST_RETRY, "stamp": "2026-11-15T20:10:00"})
    _record(engine, run_uid=NEXT_RETRY, build=CURRENT)
    assert _recover(engine, NEXT_RETRY) is None


def test_component_path_fails_closed_for_unrelated_ancestor(monkeypatch):
    current = {**RUNTIME, "parent_linux_build_sha": INTERMEDIATE}
    intermediate = {**RUNTIME, "linux_build_sha": INTERMEDIATE}
    monkeypatch.setattr(recovery, "_verify_component_lineage", lambda *_: intermediate)
    monkeypatch.setattr(recovery, "load_component_attestation",
                        lambda _connection, build: {**ORIGIN_COMPONENT, "windows_build_sha": "e" * 40})
    with pytest.raises(FormalScoreContractError, match="parent scope differs"):
        recovery._compatible_recovery_builds(object(), current)


def test_scheduler_delta_must_be_confined_to_terminal_projection_body():
    source = b"""from pathlib import Path
VERSION = 1

@wrapper
def _terminal_snapshot(conn):
    return conn.get()

def calculate(conn):
    return conn.score()
"""
    normalized = recovery._scheduler_outside_terminal_projection
    assert normalized(source) == normalized(source.replace(b"return conn.get()", b"return normalize(conn.get())"))
    for old, new in [
        (b"VERSION = 1", b"VERSION = 2"),
        (b"return conn.score()", b"return 0"),
        (b"@wrapper", b"@other_wrapper"),
        (b"_terminal_snapshot(conn)", b"_terminal_snapshot(conn, extra=None)"),
    ]:
        assert normalized(source) != normalized(source.replace(old, new))
