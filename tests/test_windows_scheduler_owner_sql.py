from datetime import datetime, timedelta
import threading
import time
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from server.api import scheduler_runtime as runtime


@pytest.fixture
def owner_store(monkeypatch):
    """Execute the real owner predicates against a private transactional DB."""
    clock = [datetime(2026, 10, 11, 16, 0)]
    engine = create_engine("sqlite+pysqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def functions(connection, _record):
        connection.create_function("NOW", 0, lambda: clock[0].isoformat(" "))
        connection.create_function("TIMESTAMPDIFF", 3, lambda _unit, start, end:
                                   int((datetime.fromisoformat(end)
                                        - datetime.fromisoformat(start)).total_seconds()))

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def sqlite_syntax(_connection, _cursor, statement, parameters, _context, _many):
        return (statement.replace("TIMESTAMPDIFF(SECOND,", "TIMESTAMPDIFF('SECOND',")
                .replace(" FOR UPDATE", ""), parameters)

    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE st_scheduler_runtime (
            instance_id TEXT PRIMARY KEY, mode TEXT, host_name TEXT, pid INTEGER,
            build_sha TEXT, executor_role TEXT, started_at TEXT, heartbeat_at TEXT,
            poll_seconds INTEGER, max_concurrent_tasks INTEGER)"""))
    monkeypatch.setattr(runtime, "_scheduler_instance_id", "owner-host-321")
    monkeypatch.setattr(runtime, "_scheduler_started_at", clock[0])
    monkeypatch.setattr(runtime, "gethostname", lambda: "owner-host")
    monkeypatch.setattr(runtime.os, "getpid", lambda: 321)
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "a" * 40)
    monkeypatch.setattr(runtime, "_scheduler_executor_role", lambda _mode: "qmt_windows_edge")
    monkeypatch.setattr(runtime, "get_scheduler_runtime_config", lambda:
                        {"poll_seconds": 60, "max_concurrent_tasks": 2})
    owner = runtime._scheduler_heartbeat_identity("standalone")
    try:
        yield engine, clock, owner
    finally:
        engine.dispose()


def _row(engine):
    with engine.connect() as connection:
        return dict(connection.execute(text("SELECT * FROM st_scheduler_runtime"))
                    .mappings().one())


def test_renew_commits_same_owner_without_upserting(owner_store):
    engine, clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    before = _row(engine)
    clock[0] += timedelta(seconds=10)
    runtime._renew_windows_scheduler_heartbeat(engine, owner)
    after = _row(engine)
    assert after["heartbeat_at"] == clock[0].isoformat(" ")
    assert {key: value for key, value in after.items() if key != "heartbeat_at"} == {
        key: value for key, value in before.items() if key != "heartbeat_at"}


@pytest.mark.parametrize("field,value", [
    ("instance_id", "other-owner-999"), ("mode", "embedded"),
    ("host_name", "other-host"), ("pid", 999), ("build_sha", "b" * 40),
    ("executor_role", "linux_standalone"),
    ("started_at", "2026-10-11 15:59:59"), ("poll_seconds", 30),
    ("max_concurrent_tasks", 3),
    ("heartbeat_at", "2026-10-11 15:57:59"),
    ("heartbeat_at", "2026-10-11 16:00:01"),
])
def test_renew_cannot_revive_changed_expired_or_future_owner(owner_store, field, value):
    engine, _clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    with engine.begin() as connection:
        connection.execute(text(f"UPDATE st_scheduler_runtime SET {field}=:value"),
                           {"value": value})
    before = _row(engine)
    with pytest.raises(runtime.SchedulerOwnerLeaseLost):
        runtime._renew_windows_scheduler_heartbeat(engine, owner)
    assert _row(engine) == before


def test_missing_owner_is_not_recreated(owner_store):
    engine, _clock, owner = owner_store
    with pytest.raises(runtime.SchedulerOwnerLeaseLost):
        runtime._renew_windows_scheduler_heartbeat(engine, owner)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM st_scheduler_runtime")).scalar_one() == 0


def test_failed_commit_does_not_prove_renewal(owner_store):
    engine, clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    before = _row(engine)
    clock[0] += timedelta(seconds=5)

    def fail_commit(_connection):
        raise RuntimeError("commit transport unavailable")

    event.listen(engine, "commit", fail_commit)
    try:
        with pytest.raises(RuntimeError, match="commit transport"):
            runtime._renew_windows_scheduler_heartbeat(engine, owner)
    finally:
        event.remove(engine, "commit", fail_commit)
    assert _row(engine) == before


@pytest.mark.parametrize("age_seconds", [0, 121])
def test_registration_cannot_restart_the_same_process_generation(owner_store, age_seconds):
    engine, clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    before = _row(engine)
    clock[0] += timedelta(seconds=age_seconds)
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="already_registered"):
        runtime._register_windows_scheduler_heartbeat(engine, owner)
    assert _row(engine) == before


def test_only_a_later_actual_process_can_replace_expired_pid_generation(owner_store):
    engine, clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    clock[0] += timedelta(seconds=121)
    later_owner = {**owner, "started_at": clock[0]}
    runtime._register_windows_scheduler_heartbeat(engine, later_owner)
    assert _row(engine)["started_at"] == clock[0].isoformat(" ")
    with pytest.raises(runtime.SchedulerOwnerLeaseLost):
        runtime._renew_windows_scheduler_heartbeat(engine, owner)


def test_later_process_cannot_overwrite_still_fresh_generation(owner_store):
    engine, clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    clock[0] += timedelta(seconds=1)
    with pytest.raises(runtime.SchedulerOwnerLeaseLost):
        runtime._register_windows_scheduler_heartbeat(engine, {**owner, "started_at": clock[0]})


def test_registration_inventory_is_read_only_until_all_prior_leases_expire(owner_store):
    engine, clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    before = _row(engine)
    assert runtime._windows_scheduler_registration_ready(engine, owner) is False
    assert _row(engine) == before
    clock[0] += timedelta(seconds=120)
    assert runtime._windows_scheduler_registration_ready(engine, owner) is False
    clock[0] += timedelta(seconds=1)
    assert runtime._windows_scheduler_registration_ready(engine, owner) is True
    assert _row(engine) == before


@pytest.mark.parametrize("field,value", [
    ("heartbeat_at", "2026-10-11 16:00:01"),
    ("poll_seconds", 0), ("mode", "embedded"), ("host_name", ""),
    ("pid", 0), ("instance_id", "malformed"), ("build_sha", "0" * 40),
    ("started_at", None), ("max_concurrent_tasks", 0),
])
def test_registration_inventory_rejects_future_or_invalid_fresh_identity(
    owner_store, field, value,
):
    engine, _clock, owner = owner_store
    runtime._register_windows_scheduler_heartbeat(engine, owner)
    with engine.begin() as connection:
        connection.execute(text(f"UPDATE st_scheduler_runtime SET {field}=:value"),
                           {"value": value})
    before = _row(engine)
    with pytest.raises(runtime.SchedulerOwnerLeaseLost):
        runtime._windows_scheduler_registration_ready(engine, owner)
    assert _row(engine) == before


def test_owner_pool_and_socket_calls_share_a_bounded_cycle(owner_store, monkeypatch):
    from server.common import engine_factory

    monkeypatch.setattr(runtime, "get_mysql_url", lambda **_kwargs:
                        "mysql+pymysql://unit:unit@127.0.0.1/owner_test")
    monkeypatch.setattr(engine_factory, "_get_runtime_tls_config", lambda:
                        {"required": False, "ssl_ca": None})
    lease = runtime._WindowsSchedulerLease(threading.Event())
    # Verify the real owner's engine/events without opening any socket.
    monkeypatch.setattr(lease.engine.dialect.loaded_dbapi.connections.Connection,
                        "connect", lambda _self: None)
    try:
        assert lease.engine.pool.size() == 1
        assert lease.engine.pool._max_overflow == 0
        assert lease.engine.pool._timeout == 1.0
        lease.deadline = time.monotonic() + 0.5
        params = {}
        for callback in lease.engine.dialect.dispatch.do_connect:
            callback(lease.engine.dialect, None, [], params)
        assert all(0 < params[key] <= 0.5 for key in
                   ("connect_timeout", "read_timeout", "write_timeout"))
        driver = SimpleNamespace()
        connection = SimpleNamespace(connection=SimpleNamespace(driver_connection=driver))
        for callback in lease.engine.dispatch.before_cursor_execute:
            callback(connection, None, "SELECT 1", None, None, False)
        assert 0 < driver._read_timeout <= 0.5
        assert 0 < driver._write_timeout <= 0.5
        lease.deadline = time.monotonic() - 1
        with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="cycle_deadline"):
            for callback in lease.engine.dispatch.commit:
                callback(connection)
    finally:
        lease.stop()


def test_completed_owner_shutdown_cannot_begin_another_database_claim(monkeypatch):
    monkeypatch.setattr(runtime, "_scheduler_stopping", True)
    monkeypatch.setattr(runtime, "_windows_scheduler_lease", None)

    def forbidden_claim(*_args, **_kwargs):
        pytest.fail("a stopped scheduler attempted a new database claim")

    monkeypatch.setattr(runtime, "claim_scheduler_task_run", forbidden_claim)
    assert runtime._claim_task_run({"id": 123}, None) is False


def test_stop_during_activation_cannot_begin_registration(owner_store, monkeypatch):
    engine, _clock, _owner = owner_store
    stop = threading.Event()
    monkeypatch.setattr(runtime, "_scheduler_stopping", False)
    monkeypatch.setattr(runtime._WindowsSchedulerLease, "_create_engine", lambda _self: engine)
    lease = runtime._WindowsSchedulerLease(stop)

    def stopped_activation(*_args, **_kwargs):
        stop.set()
        return True, "ready"

    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", stopped_activation)
    try:
        with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="stopping"):
            lease.start()
        with engine.connect() as connection:
            assert connection.execute(text("SELECT COUNT(*) FROM st_scheduler_runtime")).scalar_one() == 0
    finally:
        lease.stop()
