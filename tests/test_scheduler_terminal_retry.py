from contextlib import contextmanager

from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.api import scheduler_runtime as runtime


def test_database_disconnect_does_not_leave_shutdown_waiting_forever(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    event.listen(engine, "connect", lambda db, _: db.create_function("NOW", 0, lambda: "2026-09-17 00:00:00"))
    uid = "a" * 32
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE st_scheduled_task_history (task_id INTEGER, run_uid TEXT, scheduler_instance_id TEXT, status TEXT, finished_at TEXT, duration INTEGER, exit_code INTEGER, output TEXT)"))
        conn.execute(text("INSERT INTO st_scheduled_task_history VALUES (55,:uid,:owner,'running',NULL,NULL,NULL,NULL)"),
                     {"uid": uid, "owner": runtime._scheduler_instance_id})
        conn.execute(text("INSERT INTO st_scheduled_task_history VALUES (56,'foreign','other-owner','running',NULL,NULL,NULL,NULL)"))
    monkeypatch.setattr(runtime, "_pending_terminal_writes", {})
    monkeypatch.setattr(runtime, "_running_history_uids", {55: uid})
    monkeypatch.setattr(runtime, "_last_terminal_retry", 0)
    clock = [100.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(runtime, "get_engine", lambda: engine)
    begin = engine.begin

    @contextmanager
    def disconnected():
        raise OperationalError("UPDATE history", {}, ConnectionError("tunnel closed"))
        yield

    monkeypatch.setattr(engine, "begin", disconnected)
    runtime._task_history_finish(engine, uid, status="stopped", duration=61,
                                 exit_code=0xFFFFFFFF, output="confirmed stopped; token=secret-value",
                                 task_type="qmt_local_history_2024")
    assert uid in runtime._pending_terminal_writes
    assert "secret-value" not in runtime._pending_terminal_writes[uid]["output"]
    # The original worker remains the writer until it releases ownership.
    monkeypatch.setattr(engine, "begin", begin)
    assert not runtime._owned_shutdown_runs_are_terminal({55: uid})
    assert uid in runtime._pending_terminal_writes
    runtime._running_history_uids.clear()
    clock[0] += 6
    assert runtime._owned_shutdown_runs_are_terminal({55: uid})
    assert runtime._pending_terminal_writes == {}
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT run_uid,status,exit_code,output FROM st_scheduled_task_history ORDER BY task_id")).mappings().all()
    assert rows[0]["status"] == "stopped"
    assert rows[0]["exit_code"] == -1
    assert "confirmed stopped" in rows[0]["output"]
    assert rows[1]["status"] == "running"
    engine.dispose()


def test_non_database_error_is_not_replayed(monkeypatch):
    class InvalidEngine:
        def begin(self):
            raise ValueError("invalid terminal contract")
    monkeypatch.setattr(runtime, "_pending_terminal_writes", {})
    runtime._task_history_finish(InvalidEngine(), "b" * 32, status="failed",
                                 duration=0, exit_code=1, output="invalid",
                                 task_type="qmt_local_history_2024")
    assert runtime._pending_terminal_writes == {}
