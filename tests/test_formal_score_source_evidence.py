import json
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from server.common import formal_score_source_evidence as source
from server.common.daily_delivery_control import (
    DailyDeliveryFenceLost,
    FrozenInitialAnnouncement,
    SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
    canonical_sha256,
    daily_session_identity,
)


TRADE_DATE = "2026-11-13"
BUILD = "a" * 40
FINANCE_ROOT = "b" * 64
FINANCE_ID = "c" * 64
CUTOFF = datetime(2026, 11, 15, 19, 28, 45)
STAGES = (
    "capital_flow_batch_fast", "qmt_announcement_pit", "qmt_stock_daily_canonical",
    "stock_finance", "target_turnover_snapshot",
)


@pytest.fixture
def original_run(monkeypatch):
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    engine = create_engine("sqlite:///:memory:")
    session = daily_session_identity(TRADE_DATE, BUILD)
    histories = {}

    def add_terminal(connection, task, index, *, finished_at, uid=None):
        run_at = finished_at - timedelta(seconds=15)
        run_uid = uid or str(index) * 32
        status = "degraded" if task == "stock_finance" else "success"
        payload = {"trade_date": TRADE_DATE, "status": status}
        if task == "stock_finance":
            payload = {"schema": "probiga.finance-sync-result.v2", "atomic_batch": {
                "as_of_date": TRADE_DATE, "seal_coverage_id": FINANCE_ID, "batch_root_sha256": FINANCE_ROOT,
                "completed_known_at": finished_at.isoformat(),
            }}
        replay = json.dumps(payload, separators=(",", ":"))
        import hashlib
        root = hashlib.sha256(replay.encode()).hexdigest()
        core = {
            "schema": SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
            "run_uid": run_uid, "task_id": index, "task_type": task,
            "target_trade_date": TRADE_DATE, "build_sha": BUILD, "contract_release_id": BUILD,
            "status": status, "exit_code": 0, "validation_checked": True, "validation_ok": True,
            "input_receipt_root_sha256": root, "replay_output_sha256": root, "replay_output": replay,
        }
        evidence = {**core, "evidence_sha256": canonical_sha256(core)}
        args = {
            "task_id": index, "task": task, "uid": run_uid, "build": BUILD,
            "start": run_at, "finish": finished_at, "status": status, "stage_status": status.upper(),
            "evidence": json.dumps(evidence), "session": session["session_uid"], "root": root,
        }
        connection.execute(text("""
            INSERT INTO st_scheduled_task_history(task_id,run_uid,task_type,run_at,finished_at,status,exit_code,output,build_sha)
                VALUES(:task_id,:uid,:task,:start,:finish,:status,0,:evidence,:build)
        """), args)
        connection.execute(text("""
            INSERT INTO st_daily_stage_attempt VALUES(:session,:task,:uid,'main',:stage_status,:start,:finish,:root,:evidence)
        """), args)
        return evidence

    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE st_daily_delivery_session(session_uid TEXT PRIMARY KEY,trade_date TEXT,release_id TEXT)"))
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history(
                id INTEGER PRIMARY KEY, task_id INTEGER, run_uid TEXT UNIQUE,task_type TEXT,
                run_at TEXT,finished_at TEXT,status TEXT,exit_code INTEGER,output TEXT,build_sha TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_daily_stage_attempt(
                session_uid TEXT,stage_name TEXT,scheduler_run_uid TEXT UNIQUE,shard_id TEXT,status TEXT,
                started_at TEXT,finished_at TEXT,input_root_sha256 TEXT,checkpoint_json TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_pit_source_coverage(
                coverage_id TEXT PRIMARY KEY,fact_kind TEXT, source TEXT,stock_code TEXT,
                coverage_status TEXT,result_count INTEGER,window_start TEXT,window_end TEXT,
                known_at TEXT,received_at TEXT,covered_through_at TEXT,batch_id TEXT,payload_json TEXT
            )
        """))
        connection.execute(text("INSERT INTO st_daily_delivery_session VALUES(:session_uid,:trade_date,:release_id)"), session)
        for index, task in enumerate(STAGES, 1):
            histories[task] = add_terminal(connection, task, index, finished_at=CUTOFF - timedelta(minutes=10))
        # An exact target stage can finish after scoring. Its higher audit id
        # must not alter the source map at the frozen decision.
        add_terminal(connection, STAGES[-1], 9, finished_at=CUTOFF + timedelta(minutes=1))
        connection.execute(text("""
            INSERT INTO st_pit_source_coverage VALUES(
             :id,'finance',:source,:code,'COMPLETE',0,'1900-01-01',:trade_date,
             :known,:known,:known,:batch,:payload
            )
        """), {
            "id": FINANCE_ID, "source": source.FINANCE_ATOMIC_BATCH_SOURCE, "code": source.FINANCE_ATOMIC_BATCH_CODE,
            "trade_date": TRADE_DATE, "known": CUTOFF - timedelta(minutes=10),
            "batch": f"finance-atomic-{FINANCE_ROOT[:32]}",
            "payload": json.dumps({"watermark": {"evidence": {"batch_root_sha256": FINANCE_ROOT}}}),
        })

    monkeypatch.setattr(source, "load_component_attestation", lambda _conn, sha: {
        "linux_build_sha": sha, "windows_build_sha": sha, "contract_build_sha": sha,
    })
    original_announcement = histories["qmt_announcement_pit"]

    def select(_connection, *, trade_date, decision_at, consumer_release_id):
        assert trade_date == TRADE_DATE and decision_at == CUTOFF and consumer_release_id == BUILD
        return FrozenInitialAnnouncement({}, {
            "evidence_sha256": original_announcement["evidence_sha256"],
            "input_receipt_root_sha256": original_announcement["input_receipt_root_sha256"],
            "scheduler_run_uid": original_announcement["run_uid"], "producer_build_sha": BUILD,
            "contract_release_id": BUILD,
        })

    monkeypatch.setattr(source, "select_frozen_initial_announcement_source", select)
    yield engine, histories
    engine.dispose()


def test_reconstructs_original_five_stage_immutable_history_at_cutoff(original_run):
    engine, histories = original_run
    expected = {stage: histories[stage]["run_uid"] for stage in STAGES}
    with engine.connect() as connection:
        assert source.frozen_initial_source_runs(
            connection, trade_date=TRADE_DATE, decision_at=CUTOFF, build_sha=BUILD,
        ) == expected
    assert source.frozen_initial_source_runs(
        engine, trade_date=TRADE_DATE, decision_at=CUTOFF, build_sha=BUILD,
    ) == expected


@pytest.mark.parametrize("damage,expected", [
    ("missing_finance_seal", "finance seal"),
    ("tampered_stage_receipt", "completed daily stage checkpoint identity"),
    ("tampered_scheduler_audit", "terminal evidence differs"),
])
def test_declines_incomplete_signed_inputs(original_run, damage, expected):
    engine, histories = original_run
    with engine.begin() as connection:
        if damage == "missing_finance_seal":
            connection.execute(text("DELETE FROM st_pit_source_coverage WHERE coverage_id=:id"), {"id": FINANCE_ID})
        elif damage == "tampered_stage_receipt":
            connection.execute(text("""
                UPDATE st_daily_stage_attempt SET input_root_sha256=:bad WHERE scheduler_run_uid=:uid
            """), {"bad": "0" * 64, "uid": histories["capital_flow_batch_fast"]["run_uid"]})
        else:
            connection.execute(text("""
                UPDATE st_scheduled_task_history SET output='{}' WHERE run_uid=:uid
            """), {"uid": histories["target_turnover_snapshot"]["run_uid"]})
    with pytest.raises((ValueError, DailyDeliveryFenceLost), match=expected):
        source.frozen_initial_source_runs(engine, trade_date=TRADE_DATE, decision_at=CUTOFF, build_sha=BUILD)
