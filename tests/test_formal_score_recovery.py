"""Recovery reuses only the attested original score under the next scheduler fence."""
import json
import subprocess
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.analysis.sync_analysis_fast import MODEL_VERSION
from server.common import formal_score_recovery as recovery
from server.common import formal_score_source_evidence as source_evidence
from server.common.analysis_pool_receipt import (
    ANALYSIS_POOL_RECEIPT_SCHEMA, FROZEN_INITIAL_ANNOUNCEMENT_SCHEMA, build_publication_receipt,
    decode_score_snapshot,
)
from server.common.daily_delivery_control import analysis_publication_reference, canonical_sha256
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
                (session_uid, scheduler_run_uid, stage_name, status, started_at, finished_at,
                 shard_id, lease_owner, fencing_token)
            VALUES ('session', :uid, 'analysis_fast', :status, :started, :finish,
                    'main', :owner, (SELECT COALESCE(MAX(fencing_token), 0) + 1 FROM st_daily_stage_attempt))
        """), {"uid": run_uid, "status": status, "started": started, "finish": finish, "owner": "linux-" + run_uid[:6]})
        conn.execute(text("""
            INSERT INTO st_scheduled_task_history
                (run_uid, task_type, build_sha, status, finished_at, exit_code, output, scheduler_instance_id)
            VALUES (:uid, 'analysis_fast', :build, :status, :finish, :exit, :output, :owner)
        """), {"uid": run_uid, "build": build, "status": status.lower(), "finish": finish,
               "exit": 0 if historical else None, "output": output, "owner": "linux-" + run_uid[:6]})
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
            "CREATE TABLE st_daily_stage_attempt (id INTEGER PRIMARY KEY,session_uid TEXT,scheduler_run_uid TEXT,stage_name TEXT,status TEXT,started_at TEXT,finished_at TEXT,shard_id TEXT,lease_owner TEXT,fencing_token INTEGER,lease_until TEXT,error_code TEXT,output_dataset_id TEXT)",
            "CREATE TABLE st_scheduled_task_history (run_uid TEXT PRIMARY KEY,task_type TEXT,build_sha TEXT,status TEXT,finished_at TEXT,exit_code INTEGER,output TEXT,scheduler_instance_id TEXT)",
            "CREATE TABLE st_recommended_run_history (run_uid TEXT PRIMARY KEY,scheduler_job_id TEXT,trade_date TEXT,min_score REAL,top_n INTEGER,execution_time TEXT,published_at TEXT,canonical_pool_sha256 TEXT,status TEXT)",
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


def _published_record(
    engine, work, *, published_at="2026-11-15T20:01:59", finished_at="2026-11-15T20:02:00",
):
    manifest = {
        "schema": ANALYSIS_POOL_RECEIPT_SCHEMA, "trade_date": TARGET,
        "canonical_pool_sha256": "7" * 64, "score_snapshot_sha256": work["score_snapshot"]["payload_sha256"],
        **{field: work[field] for field in ("analysis_count", "recommendation_count", "executable_count")},
    }
    receipt = build_publication_receipt(
        manifest=manifest, run_uid=work["run_uid"], build_sha=work["build_sha"],
        publisher_task_type="analysis_fast", published_at=published_at,
        score_snapshot=work["score_snapshot"], engine=engine,
    )
    audit = json.dumps({
        "replay_output": json.dumps(work) + "\n" + json.dumps(analysis_publication_reference(receipt, engine=engine)),
    })
    params = {
        "uid": work["run_uid"], "audit": audit, "published_at": published_at,
        "finished_at": finished_at, "pool": receipt["canonical_pool_sha256"],
    }
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE st_daily_stage_attempt
            SET status='SUCCESS',output_dataset_id=:uid,finished_at=:finished_at
            WHERE scheduler_run_uid=:uid
        """), params)
        conn.execute(text("""
            UPDATE st_scheduled_task_history
            SET status='success',finished_at=:published_at,exit_code=0,output=:audit
            WHERE run_uid=:uid
        """), params)
        conn.execute(text("""
            UPDATE st_recommended_run_history
            SET status='done',published_at=:published_at,canonical_pool_sha256=:pool
            WHERE run_uid=:uid
        """), params)
    return receipt


def test_rebind_published_original_after_compatible_linux_release(archived, monkeypatch):
    engine, original = archived
    receipt = _published_record(engine, original)

    def load_exact(conn, target, *, decision_at, run_uid):
        assert conn.engine is engine and target == TARGET
        assert exact_time(decision_at) == PROPOSAL and run_uid == ORIGINAL_RUN
        return receipt

    monkeypatch.setattr(recovery, "load_published_analysis_receipt", load_exact)
    monkeypatch.setattr(FormalAnalysisStore, "load_artifacts", lambda *_: pytest.fail("recovery reloaded score rows"))
    rebound = _recover(engine)["reference"]
    assert rebound["run_uid"] == FIRST_RETRY and rebound["build_sha"] == CURRENT
    assert rebound["decision_at"] == CUTOFF
    assert rebound["generation_sha256"] == original["generation_sha256"]
    assert rebound["selection_sha256"] == original["selection_sha256"]
    recovery.verify_recovered_initial_selection(engine, rebound, proposed_cutoff=PROPOSAL)


@pytest.mark.parametrize("published_origin", [False, True])
def test_rebind_newest_publication_with_earlier_verified_origin(archived, monkeypatch, published_origin):
    engine, original = archived
    rebound = load_initial_selection(
        FormalAnalysisStore(engine), generation=original["generation_sha256"],
        run_uid=FIRST_RETRY, build_sha=INTERMEDIATE,
    )["reference"]
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_scheduled_task_history SET build_sha=:build WHERE run_uid=:uid"),
                     {"build": INTERMEDIATE, "uid": FIRST_RETRY})
        conn.execute(text("UPDATE st_daily_stage_attempt SET started_at='2026-11-15T20:10:00' WHERE scheduler_run_uid=:uid"),
                     {"uid": FIRST_RETRY})
    receipts = {
        FIRST_RETRY: _published_record(
            engine, rebound, published_at="2026-11-15T20:11:00", finished_at="2026-11-15T20:12:00",
        ),
    }
    if published_origin:
        receipts[ORIGINAL_RUN] = _published_record(engine, original)
    _record(engine, run_uid=NEXT_RETRY, build=CURRENT)
    monkeypatch.setattr(recovery, "_compatible_recovery_builds", lambda *_: {CURRENT, PARENT, INTERMEDIATE})
    monkeypatch.setattr(recovery, "_same_scorer", lambda old, new: old in {PARENT, INTERMEDIATE} and new == CURRENT)
    reads = []

    def load_exact(conn, target, *, decision_at, run_uid):
        assert conn.engine is engine and target == TARGET
        expected = PROPOSAL if run_uid == FIRST_RETRY else "2026-11-15T20:10:00"
        assert exact_time(decision_at) == expected
        reads.append(run_uid)
        return receipts[run_uid]

    monkeypatch.setattr(recovery, "load_published_analysis_receipt", load_exact)
    latest = _recover(engine, NEXT_RETRY)["reference"]
    assert latest["generation_sha256"] == original["generation_sha256"]
    assert latest["selection_sha256"] == original["selection_sha256"]
    assert reads == ([FIRST_RETRY, ORIGINAL_RUN] if published_origin else [FIRST_RETRY])


@pytest.mark.parametrize("change", ["audit_reference", "pool_hash", "dataset_id", "unchanged_build", "scorer"])
def test_published_recovery_rejects_wrong_evidence_or_unapproved_scorer(archived, monkeypatch, change):
    engine, original = archived
    receipt = _published_record(engine, original)
    monkeypatch.setattr(recovery, "load_published_analysis_receipt", lambda *_a, **_k: receipt)
    with engine.begin() as conn:
        params = {"uid": ORIGINAL_RUN, "wrong_uid": "8" * 32, "hash": "6" * 64,
                  "work": json.dumps(original), "current": CURRENT}
        if change == "audit_reference":
            conn.execute(text("UPDATE st_scheduled_task_history SET output=:work WHERE run_uid=:uid"), params)
        elif change == "pool_hash":
            conn.execute(text("UPDATE st_recommended_run_history SET canonical_pool_sha256=:hash WHERE run_uid=:uid"), params)
        elif change == "dataset_id":
            conn.execute(text("UPDATE st_daily_stage_attempt SET output_dataset_id=:wrong_uid WHERE scheduler_run_uid=:uid"), params)
        elif change == "unchanged_build":
            conn.execute(text("UPDATE st_scheduled_task_history SET build_sha=:current WHERE run_uid=:uid"), params)
    if change == "scorer":
        monkeypatch.setattr(recovery, "_same_scorer", lambda *_: False)
    if change in {"audit_reference", "pool_hash"}:
        with pytest.raises(FormalScoreContractError, match="completed publication differs"):
            _recover(engine)
    else:
        assert _recover(engine) is None


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


def test_recovery_retains_compatible_history_after_more_than_eight_releases(monkeypatch):
    builds = [f"{number:040x}" for number in range(1, 25)]
    anchor = builds[-1]
    manifests = {
        build: {**RUNTIME, "linux_build_sha": build, "windows_build_sha": anchor,
                "contract_build_sha": anchor,
                "parent_linux_build_sha": builds[min(index + 1, len(builds) - 1)],
                "scope": "COORDINATED" if build == anchor else "LINUX"}
        for index, build in enumerate(builds)
    }
    monkeypatch.setattr(recovery, "_verify_component_lineage", lambda *_: manifests[builds[1]])
    monkeypatch.setattr(recovery, "load_component_attestation", lambda _, build: manifests[build])
    assert recovery._compatible_recovery_builds(object(), manifests[builds[0]]) == set(builds)


def test_component_lineage_has_a_time_deadline_even_when_parents_keep_arriving(monkeypatch):
    ticks = iter((0, 1, 61))
    monkeypatch.setattr(recovery, "monotonic", lambda: next(ticks))
    current = {**RUNTIME, "parent_linux_build_sha": INTERMEDIATE}
    parent = {**RUNTIME, "linux_build_sha": INTERMEDIATE}
    monkeypatch.setattr(recovery, "_verify_component_lineage", lambda *_: parent)
    monkeypatch.setattr(recovery, "load_component_attestation", lambda _, build: ORIGIN_COMPONENT)
    with pytest.raises(RuntimeError, match="lineage validation deadline exceeded"):
        recovery._compatible_recovery_builds(object(), current)


@pytest.mark.parametrize(
    "path,definition",
    [(path, name) for path, names in recovery._VALIDATION_ONLY_BODIES.items() for name in names],
)
def test_validation_delta_must_be_confined_to_exact_function_body(path, definition):
    functions = "".join(
        f"@wrapper\ndef {name}(conn):\n    return conn.get()\n\n"
        for name in recovery._VALIDATION_ONLY_BODIES[path]
    )
    source = ("from pathlib import Path\nVERSION = 1\n\n"
              + functions + "def calculate(conn):\n    return conn.score()\n").encode("ascii")
    def normalized(data):
        return recovery._without_validation_body(path, data)

    assert normalized(source) == normalized(source.replace(b"return conn.get()", b"return normalize(conn.get())"))
    for old, new in [
        (b"VERSION = 1", b"VERSION = 2"),
        (b"return conn.score()", b"return 0"),
        (b"@wrapper", b"@other_wrapper"),
        (f"{definition}(conn)".encode("ascii"), f"{definition}(conn, extra=None)".encode("ascii")),
    ]:
        assert normalized(source) != normalized(source.replace(old, new))


def test_display_reader_accepts_only_the_exact_optional_publication_argument():
    path = "server/api/routers/hot_data.py"
    original = b"""
def _recommended_stocks_v2(trade_date):
    return display(trade_date)

def _recommended_data_freshness(*, total=0):
    return total
"""
    allowed = original.replace(b"total=0", b'total=0, publication_status: str = ""')
    with_new_required_input = allowed.replace(b'str = ""', b"str")
    assert recovery._without_validation_body(path, original) == recovery._without_validation_body(path, allowed)
    assert recovery._without_validation_body(path, original) != recovery._without_validation_body(path, with_new_required_input)


def test_pruned_previous_checkout_reuses_only_immutable_git_and_current_scorer(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()

    def git(*args, cwd=source):
        return subprocess.run(
            ["git", "-c", "core.autocrlf=false", "-c", "user.name=Test",
             "-c", "user.email=test@example.invalid", *args],
            cwd=cwd, capture_output=True, check=True, timeout=10,
        ).stdout.decode("ascii").strip()

    git("init")
    for relative in recovery._SCORER_FILES:
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"def scorer():\n    return 7\n")
        if relative == "server/common/pit_facts.py":
            path.write_bytes(path.read_bytes() + b"\ndef _bind_historical_event_snapshot(conn):\n    return conn.all()\n")
    scheduler = "server/api/scheduler_runtime.py"
    scheduler_source = b"""
def _terminal_snapshot(conn):
    return conn.get()

def _daily_result_recovery_target(conn):
    return conn.older()

def _daily_delivery_runtime_health(conn):
    return conn.health()

def _build_daily_result_delivery_receipt(conn):
    return conn.receipt()

def calculate(conn):
    return conn.score()
"""
    scheduler_file = source / scheduler
    scheduler_file.parent.mkdir(parents=True, exist_ok=True)
    scheduler_file.write_bytes(scheduler_source)
    git("add", ".")
    git("commit", "-m", "verified prior selection")
    previous = git("rev-parse", "HEAD")

    scheduler_file.write_bytes(scheduler_source.replace(b"conn.older()", b"conn.exact()"))
    historical = source / "server/common/pit_facts.py"
    historical.write_bytes(historical.read_bytes().replace(b"conn.all()", b"conn.bounded()"))
    for relative in (
        "server/static/js/app.js", "server/api/main.py", "server/api/routers/market_home.py",
        "server/api/routers/research_futures.py", "server/static/js/research-tools.js",
        "server/static/js/market-home.js",
        "server/static/css/research-tools.css",
        "server/engine/governance_daily_market.py",
        "server/engine/strategy_native_backtest.py",
        "server/engine/native_research_jobs.py",
        "server/engine/native_research_prepare.py",
        "tools/run_trading_v2_job_worker.py",
        "server/common/historical_announcement_read.py",
        "deploy/production_deploy.sh",
    ):
        display_file = source / relative
        display_file.parent.mkdir(parents=True, exist_ok=True)
        display_file.write_bytes(b"display_only = True\n")
    git("add", ".")
    git("commit", "-m", "display and exact recovery selector")
    current = git("rev-parse", "HEAD")

    releases = tmp_path / "releases"
    releases.mkdir()
    cache = tmp_path / "release.git"
    git("clone", "--bare", str(source), str(cache))
    git("--git-dir", str(cache), "worktree", "add", "--detach", str(releases / previous), previous)
    git("--git-dir", str(cache), "worktree", "add", "--detach", str(releases / current), current)
    git("--git-dir", str(cache), "worktree", "remove", str(releases / previous))
    assert not (releases / previous).exists()
    assert git("--git-dir", str(cache), "cat-file", "-t", previous) == "commit"

    checkout = releases / current
    monkeypatch.setattr(recovery, "_RELEASE_ROOT", releases)
    monkeypatch.setattr(recovery, "_RELEASE_GIT_CACHE", cache)
    monkeypatch.setattr(recovery, "__file__", str(checkout / "server/common/formal_score_recovery.py"))
    # The same proof runs on developer hosts that cannot create root-owned Unix files.
    monkeypatch.setattr(recovery, "_protected_release_path", lambda path, **_: path.lstat())
    assert recovery._same_scorer(previous, current)
    assert recovery._same_scorer(current, current)

    score_file = recovery._SCORER_FILES[0]
    git("update-index", "--assume-unchanged", score_file, cwd=checkout)
    (checkout / score_file).write_bytes(b"def scorer():\n    return 8\n")
    assert git("status", "--porcelain=v1", "--untracked-files=no", cwd=checkout) == ""
    for build in (previous, current):
        with pytest.raises(FormalScoreContractError, match="current scorer differs from its Git blob"):
            recovery._same_scorer(build, current)


def _record_takeover(engine, *, old_uid, old_build, new_build, new_claim_at, expired_at, task_expired_at, audit_finished_at):
    owner = "linux-" + old_uid[:6]
    output = (
        "INTERRUPTED_OWNER_GONE: exact scheduler task owner exited before completion; "
        f"previous_instance={owner}; previous_build={old_build}; current_build={new_build}; "
        f"owner_evidence=expired_exact_run_lease:{task_expired_at}; released_for_scheduler_catchup=true"
    )
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE st_scheduled_task_history
            SET status='failed', finished_at=:finished, exit_code=NULL, output=:output
            WHERE run_uid=:uid
        """), {"uid": old_uid, "finished": audit_finished_at, "output": output})
        conn.execute(text("""
            UPDATE st_daily_stage_attempt
            SET status='SUPERSEDED', finished_at=:new_claim, lease_until=:expiration,
                error_code='LEASE_EXPIRED_TAKEOVER'
            WHERE scheduler_run_uid=:uid
        """), {"uid": old_uid, "new_claim": new_claim_at, "expiration": expired_at})


def test_recover_original_after_repeated_expired_takeovers(archived, monkeypatch):
    engine, original = archived
    third_uid = "a" * 32
    intermediate = {**RUNTIME, "linux_build_sha": INTERMEDIATE}
    current_release = {**RUNTIME, "parent_linux_build_sha": INTERMEDIATE}
    manifests = {CURRENT: current_release, INTERMEDIATE: intermediate, PARENT: ORIGIN_COMPONENT}
    monkeypatch.setattr(recovery, "load_runtime_component_release", lambda: current_release)
    monkeypatch.setattr(recovery, "load_component_attestation", lambda _connection, build: manifests[build])
    monkeypatch.setattr(recovery, "require_compatible_component_build",
                        lambda _connection, producer, current: manifests[producer])

    with engine.begin() as conn:
        conn.execute(text("UPDATE st_daily_stage_attempt SET started_at=:stamp WHERE scheduler_run_uid=:uid"),
                     {"uid": FIRST_RETRY, "stamp": "2026-11-15T20:03:00"})
        conn.execute(text("UPDATE st_scheduled_task_history SET build_sha=:build WHERE run_uid=:uid"),
                     {"uid": FIRST_RETRY, "build": PARENT})
    _record(engine, run_uid=third_uid, build=INTERMEDIATE, started="2026-11-15T20:10:00")
    _record(engine, run_uid=NEXT_RETRY, build=CURRENT)

    _record_takeover(
        engine, old_uid=FIRST_RETRY, old_build=PARENT, new_build=INTERMEDIATE,
        new_claim_at="2026-11-15T20:10:00", expired_at="2026-11-15T20:09:51",
        task_expired_at="2026-11-15T20:09:48", audit_finished_at="2026-11-15T20:09:56",
    )
    _record_takeover(
        engine, old_uid=third_uid, old_build=INTERMEDIATE, new_build=CURRENT,
        new_claim_at=PROPOSAL, expired_at="2026-11-15T20:14:41",
        task_expired_at="2026-11-15T20:14:38", audit_finished_at="2026-11-15T20:14:52",
    )

    recovered = _recover(engine, NEXT_RETRY)
    assert recovered is not None
    rebound = recovered["reference"]
    assert rebound["run_uid"] == NEXT_RETRY
    assert rebound["selection_sha256"] == original["selection_sha256"]
    assert rebound["decision_at"] == CUTOFF
    recovery.verify_recovered_initial_selection(engine, rebound, proposed_cutoff=PROPOSAL)

    for field, changed in (
        ("error_code", "UNKNOWN_TAKEOVER"), ("fencing_token", 4), ("lease_owner", "wrong-owner"),
        ("lease_until", "2026-11-15T20:14:59"), ("finished_at", "2026-11-15T20:14:59"),
        ("output_dataset_id", "already-written"),
    ):
        with engine.begin() as conn:
            previous = conn.execute(text(f"SELECT {field} FROM st_daily_stage_attempt WHERE scheduler_run_uid=:uid"),
                                    {"uid": third_uid}).scalar_one()
            conn.execute(text(f"UPDATE st_daily_stage_attempt SET {field}=:value WHERE scheduler_run_uid=:uid"),
                         {"uid": third_uid, "value": changed})
        assert _recover(engine, NEXT_RETRY) is None
        with engine.begin() as conn:
            conn.execute(text(f"UPDATE st_daily_stage_attempt SET {field}=:value WHERE scheduler_run_uid=:uid"),
                         {"uid": third_uid, "value": previous})
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_scheduled_task_history SET output=REPLACE(output,:before,:after) WHERE run_uid=:uid"),
                     {"uid": FIRST_RETRY, "before": "current_build=" + INTERMEDIATE, "after": "current_build=" + CURRENT})
    assert _recover(engine, NEXT_RETRY) is None
