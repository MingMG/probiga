import re

import pytest
from sqlalchemy import create_engine, text

from server.api.routers import hot_data


def test_old_scheduler_terminal_receipt_survives_a_new_api_release(monkeypatch):
    engine = create_engine("sqlite://")
    old_build, current_build, uid = "a" * 40, "b" * 40, "c" * 32
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE st_recommended_run_history (
                run_uid TEXT PRIMARY KEY, scheduler_job_id TEXT, build_sha TEXT,
                status TEXT, trade_date TEXT, started_at TEXT, finished_at TEXT,
                duration_seconds INTEGER, progress_percent INTEGER, done_count INTEGER,
                total INTEGER, passed INTEGER, flow_date TEXT, hot_date TEXT,
                market_mood_score REAL, message TEXT, error TEXT, trigger_source TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                run_uid TEXT PRIMARY KEY, build_sha TEXT, status TEXT,
                finished_at TEXT, output TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO st_recommended_run_history
            (run_uid,scheduler_job_id,build_sha,status,trade_date,started_at,
             progress_percent,done_count,total,passed,flow_date,hot_date,market_mood_score)
            VALUES (:uid,:uid,:build,'running','2026-11-19','2026-11-19 16:00:00',
                    75,4200,5573,0,'2026-11-19','2026-11-19',40)
        """), {"uid": uid, "build": old_build})
        conn.execute(text("""
            INSERT INTO st_scheduled_task_history VALUES
            (:uid,:build,'timeout','2026-11-19 16:30:00','NO_PROGRESS')
        """), {"uid": uid, "build": old_build})

    def read(sql, params=None):
        with engine.connect() as conn:
            return [dict(row) for row in conn.execute(text(sql), params or {}).mappings()]

    def write(sql, params=None):
        # Only translate MySQL clock functions; run the real mutation and predicates.
        sql = re.sub(r"TIMESTAMPDIFF\(SECOND, started_at, (?:NOW\(\)|COALESCE\(:finished_at, NOW\(\)\))\)", "1800", sql)
        sql = sql.replace("NOW()", "CURRENT_TIMESTAMP")
        with engine.begin() as conn:
            return conn.execute(text(sql), params or {}).rowcount

    monkeypatch.setattr(hot_data, "_read_sql", read)
    monkeypatch.setattr(hot_data, "_exec_sql", write)
    monkeypatch.setattr(hot_data, "_ensure_recommended_run_history_table", lambda: None)
    monkeypatch.setattr(hot_data, "_recommendation_build_sha", lambda: current_build)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.delenv("PROBIGA_SCHEDULER_HISTORY_RUN_UID", raising=False)

    hot_data._recommended_run_history_reconcile_scheduler_terminal()
    row = read("SELECT * FROM st_recommended_run_history")[0]
    assert row["status"] == "error"
    assert row["build_sha"] == old_build
    assert row["total"] == 5573
    assert row["done_count"] == 4200
    assert row["progress_percent"] == 75
    assert row["flow_date"] == "2026-11-19"
    assert row["error"] == "NO_PROGRESS"
    assert row["finished_at"] == "2026-11-19 16:30:00"

    # Reads are idempotent; unrelated releases cannot rewrite the receipt.
    hot_data._recommended_run_history_reconcile_scheduler_terminal()
    assert read("SELECT * FROM st_recommended_run_history")[0] == row

    # A live producer still cannot finish a run belonging to another build.
    with pytest.raises(RuntimeError):
        hot_data._recommended_run_history_finish(uid, status="done")
    assert read("SELECT * FROM st_recommended_run_history")[0] == row

    with engine.begin() as conn:
        conn.execute(text("UPDATE st_recommended_run_history SET status='running'"))
        conn.execute(text("UPDATE st_scheduled_task_history SET build_sha=:build"), {"build": "d" * 40})
    before = read("SELECT * FROM st_recommended_run_history")[0]
    with pytest.raises(RuntimeError):
        hot_data._recommended_run_history_reconcile_scheduler_terminal()
    assert read("SELECT * FROM st_recommended_run_history")[0] == before
