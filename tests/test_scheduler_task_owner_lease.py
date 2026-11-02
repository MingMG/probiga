"""Lease and orphan recovery regressions using persisted audit/task rows."""
from datetime import datetime, timedelta
import json
import subprocess
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, event, text

from server.api import scheduler_runtime as runtime
from server.common import scheduler_task_lease as lease
from server.common.scheduler_tasks import claim_scheduler_task_run


@pytest.fixture
def case(tmp_path):
    clock = [datetime(2026, 11, 1, 14, 36, 19)]
    engine = create_engine("sqlite:///" + str(tmp_path / "leases.sqlite"),
        connect_args={"check_same_thread": False, "timeout": 2})

    @event.listens_for(engine, "connect")
    def functions(db, _):
        db.create_function("NOW", 0, lambda: clock[0].isoformat(sep=" "))
        db.create_function("GREATEST", -1, max)
        db.create_function("TIMESTAMPDIFF", 3, lambda unit, start, end:
            int((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()))

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def mysql_statement_boundary(conn, cursor, sql, params, ctx, many):
        return sql.replace(" FOR UPDATE", "").replace(
            "TIMESTAMPDIFF(SECOND,", "TIMESTAMPDIFF('SECOND',"), params

    original = dict(run_uid="a" * 32, task_id=112, task_name="stock minutes",
        task_type="qmt_stock_minute_canonical", run_at=clock[0].isoformat(sep=" "),
        host_name="windows-host", scheduler_instance_id="windows-host-58860",
        build_sha="c" * 40, trigger_source="manual")
    marker = lease.encode(original, clock[0])
    with engine.begin() as c:
        c.execute(text("""CREATE TABLE st_scheduled_task_history (
          id INTEGER PRIMARY KEY,run_uid TEXT UNIQUE,task_id INTEGER,task_name TEXT,
          task_type TEXT,run_at DATETIME,finished_at DATETIME,status TEXT,duration INTEGER,
          exit_code INTEGER,host_name TEXT,scheduler_instance_id TEXT,build_sha TEXT,
          trigger_source TEXT,output TEXT)"""))
        c.execute(text("""CREATE TABLE st_scheduled_tasks (
          id INTEGER PRIMARY KEY,enabled INTEGER,task_name TEXT,task_type TEXT,
          last_run_status TEXT,last_run_at DATETIME,last_triggered_at DATETIME,
          last_run_output TEXT,last_run_duration INTEGER,updated_at DATETIME)"""))
        c.execute(text("""CREATE TABLE st_daily_stage_attempt (
          scheduler_run_uid TEXT,status TEXT,lease_until DATETIME)"""))
        c.execute(text("""INSERT INTO st_scheduled_task_history
          (id,run_uid,task_id,task_name,task_type,run_at,status,host_name,
           scheduler_instance_id,build_sha,trigger_source,output)
          VALUES (79494,:run_uid,:task_id,:task_name,:task_type,:run_at,'running',
                  :host_name,:scheduler_instance_id,:build_sha,:trigger_source,:output)"""),
          {**original, "output": marker})
        c.execute(text("""INSERT INTO st_scheduled_tasks VALUES
          (112,1,'stock minutes','qmt_stock_minute_canonical','running',:at,:at,NULL,NULL,:at)"""),
          {"at": original["run_at"]})
    yield engine, clock, original, marker
    engine.dispose()


def _history(engine):
    with engine.connect() as c:
        return dict(c.execute(text("SELECT * FROM st_scheduled_task_history WHERE id=79494")).mappings().one())


def test_renew_updates_exact_running_audit_and_extends_expiry(case):
    engine, clock, original, marker = case
    owner = lease.TaskOwnerLease(engine, original, marker, clock=lambda: clock[0], on_lost=lambda: None)
    clock[0] += timedelta(seconds=30)
    assert owner.renew()
    persisted = lease.read(_history(engine))
    assert persisted["renewed_at"] == clock[0]
    assert persisted["lease_until"] == clock[0] + timedelta(seconds=lease.LEASE_SECONDS)
    assert owner.authorized()


def test_terminal_receipt_wins_renew_race_and_is_not_overwritten(case):
    engine, clock, original, marker = case
    owner = lease.TaskOwnerLease(engine, original, marker, clock=lambda: clock[0], on_lost=lambda: None)
    with engine.begin() as c:
        c.execute(text("UPDATE st_scheduled_task_history SET status='success',finished_at=:at,output='business-receipt'"),
                  {"at": clock[0]})
    assert not owner.renew()
    assert _history(engine)["output"] == "business-receipt"


@pytest.mark.parametrize("mutation", ["identity", "output", "missing", "expired"])
def test_renew_cannot_resurrect_changed_or_expired_audit(case, mutation):
    engine, clock, original, marker = case
    owner = lease.TaskOwnerLease(engine, original, marker, clock=lambda: clock[0], on_lost=lambda: None)
    with engine.begin() as c:
        if mutation == "identity":
            c.execute(text("UPDATE st_scheduled_task_history SET scheduler_instance_id='windows-host-12345'"))
        elif mutation == "output":
            c.execute(text("UPDATE st_scheduled_task_history SET output='changed'"))
        elif mutation == "missing":
            c.execute(text("DELETE FROM st_scheduled_task_history"))
        else:
            clock[0] += timedelta(seconds=lease.LEASE_SECONDS + 1)
    with pytest.raises(RuntimeError):
        owner.renew()


@pytest.mark.parametrize("expired,absent,stage_live,expected", [
    (True, True, False, True), (False, True, False, False),
    (True, False, False, False), (True, True, True, False),
])
def test_same_build_manual_orphan_recovery_needs_expired_lease_and_absent_owner(
        case, monkeypatch, expired, absent, stage_live, expected):
    engine, clock, original, marker = case
    started = clock[0]
    clock[0] += timedelta(seconds=lease.LEASE_SECONDS + 1 if expired else 30)
    monkeypatch.setattr(runtime, "gethostname", lambda: "windows-host")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "c" * 40)
    monkeypatch.setattr(runtime, "_now_shanghai_naive", lambda: clock[0])
    monkeypatch.setattr(runtime, "_windows_boot_started_at", lambda: datetime(2026, 10, 6))
    monkeypatch.setattr(runtime, "_owner_pid_is_absent", lambda *a, **kw: absent)
    if stage_live:
        with engine.begin() as c:
            c.execute(text("INSERT INTO st_daily_stage_attempt VALUES (:uid,'RUNNING',:until)"),
                dict(uid=original["run_uid"], until=clock[0] + timedelta(minutes=1)))
    assert not claim_scheduler_task_run(engine, 112)
    assert runtime._recover_interrupted_manual_claim(engine, {"id": 112}, started) is expected
    history = _history(engine)
    assert history["status"] == ("failed" if expected else "running")
    if expected:
        assert "expired_exact_run_lease" in history["output"]
        assert claim_scheduler_task_run(engine, 112)
    else:
        assert history["output"] == marker


def test_invalid_lease_identity_is_not_downgraded_to_unleased_recovery(case, monkeypatch):
    engine, clock, original, marker = case
    changed = json.loads(marker)
    changed["identity"]["run_uid"] = "b" * 32
    with engine.begin() as c:
        c.execute(text("UPDATE st_scheduled_task_history SET output=:marker,build_sha=:build"),
            dict(marker=json.dumps(changed), build="d" * 40))
    monkeypatch.setattr(runtime, "gethostname", lambda: "windows-host")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "c" * 40)
    absent = MagicMock(return_value=True)
    monkeypatch.setattr(runtime, "_owner_pid_is_absent", absent)
    assert not runtime._recover_interrupted_manual_claim(engine, {"id": 112}, clock[0])
    absent.assert_not_called()
    assert _history(engine)["status"] == "running"


def test_lease_loss_stops_only_child_owned_by_exact_run(monkeypatch):
    old, current = MagicMock(), MagicMock()
    monkeypatch.setattr(runtime, "_running_history_uids", {112: "new-run"})
    monkeypatch.setattr(runtime, "_running_procs", {112: current, 999: old})
    terminate = MagicMock(return_value=True)
    monkeypatch.setattr(runtime, "_terminate_process_and_confirm", terminate)
    runtime._stop_exact_child_after_owner_lease_loss(112, "old-run")
    terminate.assert_not_called()
    runtime._stop_exact_child_after_owner_lease_loss(112, "new-run")
    terminate.assert_called_once_with(current, timeout_seconds=5.0)


def test_renewal_failure_has_bounded_stop_of_its_own_live_child(case, monkeypatch):
    engine, clock, original, marker = case
    process = subprocess.Popen([sys.executable, "-B", "-c", "import time;time.sleep(20)"])
    stopped = threading.Event()
    try:
        def stop_child():
            process.terminate()
            process.wait(timeout=3)
            stopped.set()
        owner = lease.TaskOwnerLease(engine, original, marker, clock=lambda: clock[0], on_lost=stop_child)
        monkeypatch.setattr(lease, "RENEW_SECONDS", 0.01)
        owner.start()
        monkeypatch.setattr(owner, "renew", MagicMock(side_effect=RuntimeError("database unavailable")))
        assert stopped.wait(timeout=3)
        assert owner.lost.is_set() and process.poll() is not None
        assert owner.renew.call_count == 2
        owner.stop()
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=3)
