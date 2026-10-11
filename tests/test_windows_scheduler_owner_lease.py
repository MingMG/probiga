"""Real commits and threads for the Windows scheduler's existing owner lease.

Only the MySQL dialect spelling and first registration are adapted to a private
SQLite database. Renewal executes the production owner/freshness UPDATE, and
dispatch-authority validation reads its committed rows through another engine.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
import threading
from time import monotonic
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, event, text

from server.api import scheduler_runtime as runtime


class _DatabaseClock:
    def __init__(self):
        self.value = datetime.now().replace(microsecond=0)
        self.lock = threading.Lock()

    def now(self):
        with self.lock:
            return self.value.isoformat(" ")

    def advance(self, seconds=1):
        with self.lock:
            self.value += timedelta(seconds=seconds)


def _sqlite_engine(path, clock):
    engine = create_engine(
        f"sqlite+pysqlite:///{path.as_posix()}",
        connect_args={"check_same_thread": False, "timeout": 1},
        future=True,
    )

    def connect(connection, _record):
        connection.create_function("NOW", 0, clock.now)
        connection.create_function(
            "TIMESTAMPDIFF", 3,
            lambda unit, before, after: int(
                (datetime.fromisoformat(after) - datetime.fromisoformat(before)).total_seconds()
            ) if unit == "SECOND" else None,
        )

    def mysql_second_literal(_connection, _cursor, statement, parameters, _context, _many):
        return re.sub(
            r"TIMESTAMPDIFF\(\s*SECOND\s*,", "TIMESTAMPDIFF('SECOND',",
            statement, flags=re.IGNORECASE,
        ).replace(" FOR UPDATE", ""), parameters

    event.listen(engine, "connect", connect)
    event.listen(engine, "before_cursor_execute", mysql_second_literal, retval=True)
    return engine


@pytest.fixture
def lease_case(tmp_path, monkeypatch):
    clock = _DatabaseClock()
    path = tmp_path / "scheduler-lease.sqlite"
    lease_engine = _sqlite_engine(path, clock)
    work_engine = _sqlite_engine(path, clock)
    with work_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduler_runtime (
                instance_id TEXT PRIMARY KEY, mode TEXT NOT NULL,
                host_name TEXT NOT NULL, pid INTEGER NOT NULL,
                build_sha TEXT NOT NULL, executor_role TEXT NOT NULL,
                started_at DATETIME NOT NULL, heartbeat_at DATETIME NOT NULL,
                poll_seconds INTEGER NOT NULL, max_concurrent_tasks INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE lease_commits (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, heartbeat_at TEXT NOT NULL
            )
        """))
        for mutation in ("INSERT", "UPDATE OF heartbeat_at"):
            connection.execute(text(f"""
                CREATE TRIGGER lease_{mutation.split()[0].lower()} AFTER {mutation}
                ON st_scheduler_runtime BEGIN
                    INSERT INTO lease_commits (heartbeat_at) VALUES (NEW.heartbeat_at);
                END
            """))
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY, task_name TEXT, task_type TEXT,
                script_path TEXT, script_args TEXT, cron_time TEXT,
                interval_minutes INTEGER, enabled INTEGER, date_param TEXT,
                last_run_at DATETIME, last_triggered_at DATETIME,
                last_run_status TEXT, last_run_duration INTEGER,
                last_run_output TEXT, sort_order INTEGER
            )
        """))

    case = SimpleNamespace(
        clock=clock, lease_engine=lease_engine, work_engine=work_engine,
        stop=threading.Event(), work_entered=threading.Event(),
        release_work=threading.Event(), renewal_committed=threading.Event(),
        leases=[], drivers=[], errors=[], registrations=[], renewals=[], disposals=[],
    )

    def committed_count():
        with work_engine.connect() as connection:
            return connection.execute(text("SELECT COUNT(*) FROM lease_commits")).scalar_one()

    def heartbeat_row():
        with work_engine.connect() as connection:
            return dict(connection.execute(text("SELECT * FROM st_scheduler_runtime")).mappings().one())

    def register(engine, owner):
        assert engine is lease_engine
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO st_scheduler_runtime
                    (instance_id, mode, host_name, pid, build_sha, executor_role,
                     started_at, heartbeat_at, poll_seconds, max_concurrent_tasks)
                VALUES (:instance_id, :mode, :host_name, :pid, :build_sha,
                        :executor_role, :started_at, NOW(), :poll_seconds,
                        :max_concurrent_tasks)
            """), owner)
        case.registrations.append(dict(owner))

    original_renew = runtime._renew_windows_scheduler_heartbeat

    def observe_real_renew(engine, owner):
        assert engine is lease_engine
        clock.advance()
        original_renew(engine, owner)
        # This separate connection sees the trigger row only after the real
        # renewal transaction commits, not merely after cursor.execute.
        assert committed_count() >= 2
        case.renewals.append((threading.get_ident(), dict(owner)))
        case.renewal_committed.set()

    original_dispose = lease_engine.dispose

    def dispose(*args, **kwargs):
        case.disposals.append(True)
        return original_dispose(*args, **kwargs)

    original_init = runtime._WindowsSchedulerLease.__init__

    def fast_init(lease, stop_event):
        original_init(lease, stop_event)
        lease.interval_seconds = 0.01
        lease.cycle_seconds = 0.5
        lease.startup_seconds = min(lease.startup_seconds, 1.0)
        lease.startup_poll_seconds = 0.01
        lease.SOCKET_TIMEOUT_SECONDS = 0.1
        case.leases.append(lease)

    monkeypatch.setattr(runtime._WindowsSchedulerLease, "_create_engine", lambda _lease: lease_engine)
    monkeypatch.setattr(runtime._WindowsSchedulerLease, "__init__", fast_init)
    monkeypatch.setattr(runtime, "_register_windows_scheduler_heartbeat", register)
    monkeypatch.setattr(runtime, "_renew_windows_scheduler_heartbeat", observe_real_renew)
    monkeypatch.setattr(runtime, "get_engine", lambda: work_engine)
    monkeypatch.setattr(runtime, "get_scheduler_runtime_config", lambda: {
        "poll_seconds": 15, "max_concurrent_tasks": 1,
    })
    monkeypatch.setattr(runtime, "_scheduler_executor_role", lambda _mode: "qmt_windows_edge")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "a" * 40)
    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", lambda *_a, **_k: (True, "ready"))
    monkeypatch.setattr(runtime, "_qmt_windows_dispatch_preflight", lambda *_a, **_k: (True, "ready"))
    monkeypatch.setattr(runtime, "_windows_scheduler_lease", None)
    monkeypatch.setattr(runtime, "_scheduler_stopping", False)
    monkeypatch.setattr(runtime, "_shutdown_owned_runs", None)
    monkeypatch.setattr(runtime, "_scheduler_wake_event", threading.Event())
    for name, value in (
        ("_running_task_ids", set()), ("_running_history_uids", {}),
        ("_running_procs", {}), ("_running_timeout_minutes", {}),
        ("_stop_pending_task_ids", set()), ("_stop_requested_task_ids", set()),
        ("_service_stop_requested_task_ids", set()),
        ("_timeout_pending_task_ids", set()), ("_timeout_requested_task_ids", set()),
        ("_core_running_task_ids", set()),
        ("_fast_lane_running_task_ids", set()), ("_bulk_history_running_task_ids", set()),
        ("_quote_lane_running_task_ids", set()), ("_alert_lane_running_task_ids", set()),
        ("_delivery_lane_running_task_ids", set()), ("_exclusive_running_task_ids", set()),
    ):
        monkeypatch.setattr(runtime, name, value)
    for name in (
        "_retry_pending_terminal_writes", "_maybe_cleanup_history",
        "_attach_daily_recovery_targets", "_attach_research_pool_recovery_target",
    ):
        monkeypatch.setattr(runtime, name, lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_release_catchup_disabled_for_deferred_database", lambda: True)
    for name in (
        "_release_build_catchup_allowed", "_release_build_catchup_pending",
        "_membership_ordinary_publish_due", "_should_skip_task_for_host",
        "_should_skip_non_trading_day", "_should_skip_outside_intraday_window",
    ):
        monkeypatch.setattr(runtime, name, lambda *_a, **_k: False)
    monkeypatch.setattr(runtime, "scheduler_task_host_owner", lambda _row: runtime.SCHEDULER_OWNER_WINDOWS_QMT)
    monkeypatch.setattr(runtime, "strategy_governance_task_block_reason", lambda _row: "")
    monkeypatch.setattr(runtime, "_strategy_pipeline_dependencies_ready", lambda *_a, **_k: (True, "ready"))
    monkeypatch.setattr(runtime, "_scheduler_lane_has_capacity", lambda *_a, **_k: True)
    case.claim = MagicMock(return_value=False)
    case.history_start = MagicMock()
    case.business_worker = MagicMock()
    case.original_claim = runtime._claim_task_run
    case.original_history_start = runtime._task_history_start
    monkeypatch.setattr(runtime, "_claim_task_run", case.claim)
    monkeypatch.setattr(runtime, "_task_history_start", case.history_start)
    monkeypatch.setattr(runtime, "_run_task_async", case.business_worker)
    monkeypatch.setattr(lease_engine, "dispose", dispose)
    case.committed_count = committed_count
    case.heartbeat_row = heartbeat_row
    case.register = register
    case.original_renew = original_renew

    def blocked_cleanup(_engine):
        assert _engine is work_engine
        case.work_entered.set()
        assert case.release_work.wait(2), "test did not release the blocked work-loop"

    monkeypatch.setattr(runtime, "_cleanup_stale_running_tasks", blocked_cleanup)

    def start_driver():
        def run():
            try:
                runtime.run_scheduler_forever(stop_event=case.stop)
            except BaseException as error:
                case.errors.append(error)

        driver = threading.Thread(target=run, name="test-scheduler-work-loop")
        case.drivers.append(driver)
        driver.start()
        return driver

    case.start_driver = start_driver
    try:
        yield case
    finally:
        # Stop and unblock before restoring globals or disposing the database.
        case.stop.set()
        case.release_work.set()
        for lease in case.leases:
            lease.done.set()
        for driver in case.drivers:
            driver.join(timeout=2)
            assert not driver.is_alive(), "work-loop test left a running thread"
        for lease in case.leases:
            if lease.thread is not None:
                lease.thread.join(timeout=2)
                assert not lease.thread.is_alive(), "lease test left a running thread"
            lease.stop()
        original_dispose()
        work_engine.dispose()


def _due_task(case):
    with case.work_engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_scheduled_tasks
                (id, task_name, task_type, script_path, script_args, cron_time,
                 interval_minutes, enabled, date_param, last_run_status, sort_order)
            VALUES (1, 'lease regression probe', 'news_daily',
                    'biz/news/sync_news.py', '', '00:00', 1, 1, '', '', 1)
        """))


def _other_owner(case, *, poll_seconds=15):
    """Seed a distinct real committed owner without using registration's stub."""
    owner = {
        **runtime._scheduler_heartbeat_identity("standalone"),
        "instance_id": "other-scheduler-host-24680", "host_name": "other-scheduler-host",
        "pid": 24680, "build_sha": "b" * 40,
        "started_at": datetime(2000, 1, 1), "poll_seconds": poll_seconds,
    }
    with case.work_engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_scheduler_runtime
                (instance_id, mode, host_name, pid, build_sha, executor_role,
                 started_at, heartbeat_at, poll_seconds, max_concurrent_tasks)
            VALUES (:instance_id, :mode, :host_name, :pid, :build_sha,
                    :executor_role, :started_at, NOW(), :poll_seconds,
                    :max_concurrent_tasks)
        """), owner)
    return owner


def _observe_owner_scans(case):
    scanned = threading.Event()

    def completed_scan(_connection, _cursor, statement, _params, _context, _many):
        if (statement.lstrip().upper().startswith("SELECT")
                and "FROM st_scheduler_runtime" in statement
                and "executor_role" in statement):
            scanned.set()

    event.listen(case.lease_engine, "after_cursor_execute", completed_scan)
    return scanned


def _assert_owner_not_registered(case):
    assert not case.registrations
    assert not case.renewals
    with case.work_engine.connect() as connection:
        assert connection.execute(text("""
            SELECT COUNT(*) FROM st_scheduler_runtime WHERE instance_id=:instance_id
        """), {"instance_id": runtime._scheduler_instance_id}).scalar_one() == 0


def test_startup_waits_for_other_owner_expiry_without_self_pollution(lease_case):
    case = lease_case
    old_owner = _other_owner(case)
    old_heartbeat = case.heartbeat_row()["heartbeat_at"]
    scanned = _observe_owner_scans(case)
    before = case.committed_count()
    driver = case.start_driver()
    assert scanned.wait(2)
    _assert_owner_not_registered(case)
    assert case.committed_count() == before
    assert not case.work_entered.is_set()
    assert driver.is_alive()
    assert case.leases[0].thread is None

    case.clock.advance(2 * old_owner["poll_seconds"] + 1)
    assert case.work_entered.wait(6), "expired owner prevented the new lease from starting"
    assert case.renewal_committed.wait(2)
    assert len(case.registrations) == 1
    assert case.registrations[0]["instance_id"] == runtime._scheduler_instance_id
    with case.work_engine.connect() as connection:
        rows = connection.execute(text(
            "SELECT instance_id, build_sha, heartbeat_at FROM st_scheduler_runtime"
        )).mappings().all()
    assert len(rows) == 2
    old = next(row for row in rows if row["instance_id"] == old_owner["instance_id"])
    assert old["heartbeat_at"] == old_heartbeat
    assert old["build_sha"] == old_owner["build_sha"]
    case.stop.set()
    case.release_work.set()
    driver.join(timeout=2)
    assert not driver.is_alive()
    assert not case.errors


def test_two_new_start_entries_cannot_register_while_first_waits_for_old_owner(lease_case):
    case = lease_case
    old_owner = _other_owner(case)
    scanned = _observe_owner_scans(case)
    first = case.start_driver()
    assert scanned.wait(2)
    _assert_owner_not_registered(case)
    # This exercises the real public entry's existing same-process ownership
    # gate while startup is waiting, not merely after its worker has started.
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="already_running"):
        runtime.run_scheduler_forever(stop_event=threading.Event())
    assert len(case.leases) == 1
    _assert_owner_not_registered(case)
    assert case.committed_count() == 1
    case.clock.advance(2 * old_owner["poll_seconds"] + 1)
    assert case.work_entered.wait(6)
    assert case.renewal_committed.wait(2)
    assert len(case.registrations) == 1
    case.stop.set()
    case.release_work.set()
    first.join(timeout=2)
    assert not first.is_alive()
    assert not case.errors


def test_live_other_owner_renewals_end_startup_at_total_deadline_without_registration(
    lease_case, monkeypatch,
):
    case = lease_case
    old_owner = _other_owner(case)
    monkeypatch.setattr(runtime._WindowsSchedulerLease, "MAX_STARTUP_SECONDS", 0.15)
    renewal_stop = threading.Event()
    renewed = threading.Event()
    renewals = []
    renewal_errors = []

    def keep_old_owner_alive():
        try:
            while not renewal_stop.wait(0.005):
                case.clock.advance()
                # Execute the real conditional renewal SQL with the old
                # owner's exact identity, not a mocked readiness result.
                case.original_renew(case.work_engine, old_owner)
                renewals.append(case.committed_count())
                renewed.set()
        except BaseException as error:
            renewal_errors.append(error)

    old_worker = threading.Thread(target=keep_old_owner_alive, name="test-live-prior-owner")
    case.drivers.append(old_worker)
    old_worker.start()
    try:
        assert renewed.wait(2)
        started = monotonic()
        driver = case.start_driver()
        driver.join(timeout=2)
        elapsed = monotonic() - started
        assert not driver.is_alive()
        assert elapsed < 1.0, "startup exceeded its shortened total deadline"
        assert len(case.errors) == 1
        assert isinstance(case.errors[0], runtime.SchedulerOwnerLeaseLost)
        assert str(case.errors[0]) == "scheduler_lease_startup_owner_deadline_exceeded"
        assert len(renewals) >= 2, "the old owner was not independently renewing"
        assert not renewal_errors
        _assert_owner_not_registered(case)
        assert not case.work_entered.is_set()
        lease = case.leases[0]
        assert lease.thread is None
        assert not lease.verified.is_set()
        assert not lease.authorized()
        assert lease.disposed
        assert runtime._windows_scheduler_lease is None
        with case.work_engine.connect() as connection:
            assert connection.execute(text(
                "SELECT COUNT(*) FROM st_scheduler_runtime"
            )).scalar_one() == 1
    finally:
        renewal_stop.set()
        old_worker.join(timeout=2)
        assert not old_worker.is_alive()


def test_activation_is_rechecked_and_fails_closed_while_waiting_for_prior_owner(
    lease_case, monkeypatch,
):
    case = lease_case
    _other_owner(case)
    scanned = _observe_owner_scans(case)
    revoked = threading.Event()
    activations = []

    def activation(_engine, *, mode):
        assert _engine is case.lease_engine and mode == "standalone"
        activations.append(not revoked.is_set())
        return (False, "startup_hold") if revoked.is_set() else (True, "ready")

    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", activation)
    driver = case.start_driver()
    assert scanned.wait(2)
    _assert_owner_not_registered(case)
    revoked.set()
    driver.join(timeout=2)
    assert not driver.is_alive()
    assert len(case.errors) == 1
    assert isinstance(case.errors[0], runtime.SchedulerOwnerLeaseLost)
    assert str(case.errors[0]) == "scheduler_lease_activation_unavailable:startup_hold"
    assert len(activations) >= 2
    assert activations[0] is True and activations[-1] is False
    _assert_owner_not_registered(case)
    assert case.committed_count() == 1
    assert not case.work_entered.is_set()
    assert case.leases[0].thread is None
    assert not case.leases[0].verified.is_set()
    assert case.leases[0].disposed


def test_work_loop_blocked_after_registration_still_commits_owner_renewal(lease_case):
    case = lease_case
    driver = case.start_driver()
    assert case.work_entered.wait(2)
    assert not case.release_work.is_set()
    assert case.renewal_committed.wait(2), "renewal waited for the business work-loop"
    assert not case.release_work.is_set()
    assert case.committed_count() >= 2
    assert len(case.registrations) == 1
    lease = case.leases[0]
    assert lease.authorized()
    assert lease.engine is not case.work_engine
    assert all(owner == lease.owner for _thread, owner in case.renewals)
    assert case.heartbeat_row()["heartbeat_at"] > case.registrations[0]["started_at"].isoformat(" ")
    case.stop.set()
    case.release_work.set()
    driver.join(timeout=2)
    assert not driver.is_alive()
    assert not case.errors


def test_lost_lease_fences_claim_when_blocked_work_loop_returns(lease_case):
    case = lease_case
    _due_task(case)
    driver = case.start_driver()
    assert case.work_entered.wait(2)
    lease = case.leases[0]
    # Mutate the durable owner, not the renewal mock. The production CAS
    # must reject it and the independent owner must fence the blocked loop.
    with case.work_engine.begin() as connection:
        connection.execute(text("UPDATE st_scheduler_runtime SET build_sha=:other"), {"other": "b" * 40})
    assert lease.lost.wait(2)
    # lost is published before the owner completes its shutdown fence; wait
    # for that independent postcondition before releasing the business loop.
    assert case.stop.wait(2)
    assert runtime._scheduler_stopping
    assert not lease.authorized()
    case.release_work.set()
    driver.join(timeout=2)
    assert not driver.is_alive()
    assert not case.errors
    case.claim.assert_not_called()
    case.history_start.assert_not_called()
    case.business_worker.assert_not_called()
    assert case.heartbeat_row()["build_sha"] == "b" * 40


def test_same_due_task_reaches_claim_when_owner_lease_remains_valid(lease_case):
    case = lease_case
    _due_task(case)
    case.claim.side_effect = lambda *_a, **_k: case.stop.set() or False
    driver = case.start_driver()
    assert case.work_entered.wait(2)
    assert case.renewal_committed.wait(2)
    case.release_work.set()
    driver.join(timeout=2)
    assert not driver.is_alive()
    assert not case.errors
    case.claim.assert_called_once()
    case.history_start.assert_not_called()
    case.business_worker.assert_not_called()


def test_only_one_owner_worker_exists_and_lifecycle_clears_it(lease_case):
    case = lease_case
    driver = case.start_driver()
    assert case.work_entered.wait(2)
    assert case.renewal_committed.wait(2)
    lease = case.leases[0]
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="already_running"):
        runtime.run_scheduler_forever(stop_event=threading.Event())
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="already_started"):
        lease.start()
    assert len(case.leases) == 1
    assert len({thread_id for thread_id, _owner in case.renewals}) == 1
    case.stop.set()
    case.release_work.set()
    driver.join(timeout=2)
    assert not driver.is_alive()
    assert not case.errors
    assert not lease.thread.is_alive()
    assert lease.disposed
    assert not lease.authorized()
    assert runtime._windows_scheduler_lease is None
    assert len(case.disposals) == 1
    lease.stop()
    assert len(case.disposals) == 1


@pytest.mark.parametrize("loop_raises", [False, True])
def test_owner_lifecycle_stops_and_disposes_on_loop_return_or_error(lease_case, monkeypatch, loop_raises):
    case = lease_case

    def loop(**_kwargs):
        assert runtime._windows_scheduler_lease.authorized()
        if loop_raises:
            raise RuntimeError("test work-loop failed")

    monkeypatch.setattr(runtime, "_check_and_run_tasks", loop)
    if loop_raises:
        with pytest.raises(RuntimeError, match="test work-loop failed"):
            runtime.run_scheduler_forever(stop_event=case.stop)
    else:
        runtime.run_scheduler_forever(stop_event=case.stop)
    lease = case.leases[0]
    assert not lease.thread.is_alive()
    assert lease.disposed
    assert not lease.authorized()
    assert runtime._windows_scheduler_lease is None
    assert len(case.disposals) == 1


def test_initial_activation_failure_never_registers_or_leaves_worker(lease_case, monkeypatch):
    case = lease_case
    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", lambda *_a, **_k: (False, "new_hold"))
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="activation_(?:revoked|unavailable):new_hold"):
        runtime.run_scheduler_forever(stop_event=case.stop)
    assert not case.registrations
    assert case.committed_count() == 0
    assert case.leases[0].thread is None
    assert case.leases[0].disposed
    assert runtime._windows_scheduler_lease is None


def test_stopped_lifecycle_cannot_register_or_start_again(lease_case):
    case = lease_case
    lease = runtime._WindowsSchedulerLease(case.stop)
    lease.stop()
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="stopping"):
        lease.start()
    assert not case.registrations
    assert case.committed_count() == 0
    assert lease.thread is None


def test_shutdown_stops_lease_but_waits_for_original_durable_terminal(lease_case, monkeypatch):
    case = lease_case
    run_uid = "c" * 32
    lease = runtime._WindowsSchedulerLease(case.stop)
    lease.start()
    monkeypatch.setattr(runtime, "_windows_scheduler_lease", lease)
    with case.work_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                task_id INTEGER, run_uid TEXT, scheduler_instance_id TEXT,
                status TEXT, finished_at DATETIME
            )
        """))
        connection.execute(text("""
            INSERT INTO st_scheduled_task_history VALUES
                (1, :uid, :owner, 'running', NULL)
        """), {"uid": run_uid, "owner": runtime._scheduler_instance_id})
    runtime._running_task_ids.add(1)
    runtime._running_history_uids[1] = run_uid
    audit_checked = threading.Event()
    finished = threading.Event()

    def record_read(_conn, _cursor, statement, _parameters, _context, _many):
        if "SELECT COUNT(*) FROM st_scheduled_task_history" in statement:
            audit_checked.set()

    event.listen(case.work_engine, "before_cursor_execute", record_read)

    def drain():
        try:
            runtime.wait_for_owned_scheduler_tasks(poll_seconds=0.01, stop_owned=True)
            finished.set()
        except BaseException as error:
            case.errors.append(error)

    driver = threading.Thread(target=drain, name="test-original-owner-drain")
    case.drivers.append(driver)
    driver.start()
    assert lease.done.wait(2)
    # A locally empty registry is not a terminal receipt. The original
    # immutable run identity must remain pending until its DB audit closes.
    with runtime._running_lock:
        runtime._running_task_ids.clear()
        runtime._running_history_uids.clear()
    assert audit_checked.wait(2)
    assert not finished.is_set()
    assert lease.disposed
    assert not lease.thread.is_alive()
    assert not lease.authorized()
    with case.work_engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_scheduled_task_history SET status='stopped', finished_at=NOW()
            WHERE run_uid=:uid AND scheduler_instance_id=:owner
        """), {"uid": run_uid, "owner": runtime._scheduler_instance_id})
    assert finished.wait(2)
    driver.join(timeout=2)
    assert not driver.is_alive()
    assert not case.errors
    assert runtime._shutdown_owned_runs == {1: run_uid}


@pytest.mark.parametrize("known_run", [False, True])
def test_loss_between_reservation_and_claim_does_not_leave_phantom_drain(
    lease_case, monkeypatch, known_run,
):
    case = lease_case
    _due_task(case)
    run_uid = "d" * 32
    with case.work_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                task_id INTEGER, run_uid TEXT, scheduler_instance_id TEXT,
                status TEXT, finished_at DATETIME
            )
        """))
    claim_entered = threading.Event()
    release_claim = threading.Event()
    store_claim = MagicMock(side_effect=AssertionError("a fenced owner reached the DB claim"))
    monkeypatch.setattr(runtime, "claim_scheduler_task_run", store_claim)

    def claim_after_barrier(row, engine):
        assert int(row["id"]) in runtime._running_task_ids
        claim_entered.set()
        assert release_claim.wait(2)
        return case.original_claim(row, engine)

    monkeypatch.setattr(runtime, "_claim_task_run", claim_after_barrier)
    driver = case.start_driver()
    try:
        assert case.work_entered.wait(2)
        assert case.renewal_committed.wait(2)
        case.release_work.set()
        assert claim_entered.wait(2)
        if known_run:
            # Preserve an original committed identity that became visible
            # while the reservation was waiting, rather than erasing it just
            # because the current claim is fenced before submission.
            with case.work_engine.begin() as connection:
                connection.execute(text("""
                    INSERT INTO st_scheduled_task_history VALUES
                        (1, :uid, :owner, 'running', NULL)
                """), {"uid": run_uid, "owner": runtime._scheduler_instance_id})
            with runtime._running_lock:
                runtime._running_history_uids[1] = run_uid
        lease = case.leases[0]
        lease._lose("test_loss_after_reservation_before_claim")
        assert runtime._shutdown_owned_runs == {1: run_uid if known_run else ""}
        release_claim.set()
        driver.join(timeout=2)
        assert not driver.is_alive()
        assert not case.errors
        store_claim.assert_not_called()
        case.history_start.assert_not_called()
        case.business_worker.assert_not_called()
        assert not runtime._running_task_ids
        if known_run:
            assert runtime._shutdown_owned_runs == {1: run_uid}
            assert not runtime._owned_shutdown_runs_are_terminal({1: run_uid})
            with case.work_engine.begin() as connection:
                connection.execute(text("""
                    UPDATE st_scheduled_task_history
                    SET status='failed', finished_at=NOW()
                    WHERE run_uid=:uid AND scheduler_instance_id=:owner
                """), {"uid": run_uid, "owner": runtime._scheduler_instance_id})
            assert runtime._owned_shutdown_runs_are_terminal({1: run_uid})
        else:
            # No business run was created, so there is no UID to fabricate
            # or terminal receipt to wait for after a denied reservation.
            assert runtime._shutdown_owned_runs == {}
            with case.work_engine.connect() as connection:
                assert connection.execute(text(
                    "SELECT COUNT(*) FROM st_scheduled_task_history"
                )).scalar_one() == 0
                assert connection.execute(text(
                    "SELECT last_run_status FROM st_scheduled_tasks WHERE id=1"
                )).scalar_one() == ""
        # Assertions above establish the finite drain precondition before
        # invoking the actual owner-only durable shutdown verifier.
        runtime.wait_for_owned_scheduler_tasks(poll_seconds=0.01, stop_owned=True)
    finally:
        release_claim.set()
        case.stop.set()
        case.release_work.set()
        driver.join(timeout=2)
        assert not driver.is_alive()


def test_committed_history_identity_is_owned_before_business_worker_starts(
    lease_case, monkeypatch,
):
    case = lease_case
    _due_task(case)
    run_uid = "e" * 32
    with case.work_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, run_uid TEXT NOT NULL UNIQUE,
                task_id INTEGER, task_name TEXT, task_type TEXT, run_at DATETIME,
                status TEXT, host_name TEXT, scheduler_instance_id TEXT,
                build_sha TEXT, trigger_source TEXT, finished_at DATETIME
            )
        """))
        connection.execute(text("""
            UPDATE st_scheduled_tasks
            SET last_run_status='running', last_run_at=NOW(), last_triggered_at=NOW()
            WHERE id=1
        """))
    monkeypatch.setattr(runtime, "_ensure_task_history_table", lambda _engine: None)
    lease = runtime._WindowsSchedulerLease(case.stop)
    lease.start()
    monkeypatch.setattr(runtime, "_windows_scheduler_lease", lease)
    with runtime._running_lock:
        runtime._running_task_ids.add(1)
    lease._lose("test_loss_before_history_commit")
    assert runtime._shutdown_owned_runs == {1: ""}
    committing = []

    def check_pre_commit(_connection):
        # SQLAlchemy fires this before the DBAPI commit; identity must not
        # be advertised as owned until that actual transaction succeeds.
        committing.append(runtime._running_history_uids.get(1))
        assert runtime._shutdown_owned_runs[1] == ""

    event.listen(case.work_engine, "commit", check_pre_commit)
    try:
        uid = case.original_history_start(case.work_engine, {
            "id": 1, "task_name": "lease regression probe",
            "task_type": "news_daily", "_trigger_source": "scheduled",
        }, run_uid=run_uid)
    finally:
        event.remove(case.work_engine, "commit", check_pre_commit)
    assert uid == run_uid
    assert committing == [None]
    # A different connection must observe the exact original running audit
    # before a business Thread.start could fail or wait for its semaphore.
    with case.work_engine.connect() as connection:
        row = connection.execute(text(
            "SELECT * FROM st_scheduled_task_history WHERE run_uid=:uid"
        ), {"uid": run_uid}).mappings().one()
    assert row["status"] == "running"
    assert row["scheduler_instance_id"] == runtime._scheduler_instance_id
    assert runtime._running_history_uids[1] == run_uid
    assert runtime._shutdown_owned_runs == {1: run_uid}
    case.business_worker.assert_not_called()
    with runtime._running_lock:
        runtime._running_task_ids.clear()
        runtime._running_history_uids.clear()
    assert not runtime._owned_shutdown_runs_are_terminal({1: run_uid})
    with case.work_engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_scheduled_task_history SET status='failed', finished_at=NOW()
            WHERE run_uid=:uid AND scheduler_instance_id=:owner
        """), {"uid": run_uid, "owner": runtime._scheduler_instance_id})
    runtime.wait_for_owned_scheduler_tasks(poll_seconds=0.01, stop_owned=True)
    assert runtime._shutdown_owned_runs == {1: run_uid}


def _news_child_boundary(monkeypatch):
    """Replace only external launch inputs and the terminal write boundary."""
    script = Path(__file__).resolve()
    row = {
        "id": 1, "task_name": "lease child launch probe", "task_type": "news_daily",
        "script_path": str(script), "script_args": "", "interval_minutes": 0,
    }
    finish = MagicMock()
    popen = MagicMock()
    monkeypatch.setattr(runtime, "resolve_scheduler_script", lambda *_args: script)
    monkeypatch.setattr(runtime, "build_child_env", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(runtime, "_build_task_args", lambda *_args: [])
    monkeypatch.setattr(runtime, "update_scheduler_task", MagicMock())
    monkeypatch.setattr(runtime, "_task_history_finish", finish)
    monkeypatch.setattr(runtime.subprocess, "Popen", popen)
    return row, script.parent, finish, popen


def test_lost_owner_cannot_launch_child_and_finishes_original_audit(lease_case, monkeypatch):
    case = lease_case
    row, root, finish, popen = _news_child_boundary(monkeypatch)
    run_uid = "f" * 32
    lease = runtime._WindowsSchedulerLease(case.stop)
    lease.start()
    monkeypatch.setattr(runtime, "_windows_scheduler_lease", lease)
    with runtime._running_lock:
        runtime._running_task_ids.add(1)
        runtime._running_history_uids[1] = run_uid
    assert lease.authorized()
    lease._lose("test_loss_before_child_launch")

    runtime._run_task_impl(row, root, case.work_engine, history_run_uid=run_uid)

    popen.assert_not_called()
    finish.assert_called_once()
    assert finish.call_args.args == (case.work_engine, run_uid)
    assert finish.call_args.kwargs["status"] == "failed"
    assert "scheduler_owner_lost_before_child_launch" in finish.call_args.kwargs["output"]
    assert runtime._shutdown_owned_runs == {1: run_uid}
    assert not runtime._running_procs


def test_pre_loss_approved_popen_is_registered_before_shutdown_snapshot(lease_case, monkeypatch):
    case = lease_case
    row, root, finish, popen = _news_child_boundary(monkeypatch)
    run_uid = "1" * 32
    lease = runtime._WindowsSchedulerLease(case.stop)
    lease.start()
    monkeypatch.setattr(runtime, "_windows_scheduler_lease", lease)
    with runtime._running_lock:
        runtime._running_task_ids.add(1)
        runtime._running_history_uids[1] = run_uid
    popen_entered = threading.Event()
    release_popen = threading.Event()
    communicate_entered = threading.Event()
    release_child = threading.Event()
    loss_finished = threading.Event()
    child_finished = threading.Event()
    proc = MagicMock()
    proc.returncode = None
    proc.poll.side_effect = lambda: proc.returncode

    def approved_popen(*_args, **_kwargs):
        popen_entered.set()
        assert release_popen.wait(2), "test did not release the approved Popen"
        return proc

    def communicate(*_args, **_kwargs):
        communicate_entered.set()
        assert release_child.wait(2), "test did not release the owned child"
        proc.returncode = -15
        return "", ""

    popen.side_effect = approved_popen
    proc.communicate.side_effect = communicate
    terminate = MagicMock(return_value=True)
    monkeypatch.setattr(runtime, "_terminate_process_and_confirm", terminate)

    def run_child():
        try:
            runtime._run_task_impl(row, root, case.work_engine, history_run_uid=run_uid)
            child_finished.set()
        except BaseException as error:
            case.errors.append(error)

    def lose_owner():
        try:
            lease._lose("test_loss_inside_preapproved_popen")
            loss_finished.set()
        except BaseException as error:
            case.errors.append(error)

    child = threading.Thread(target=run_child, name="test-preapproved-child-launch")
    loss = threading.Thread(target=lose_owner, name="test-loss-during-approved-popen")
    case.drivers.extend((child, loss))
    try:
        child.start()
        assert popen_entered.wait(2)
        loss.start()
        assert lease.lost.wait(2)
        # Popen's authorization check occurred while holding _running_lock.
        # Loss can revoke new calls now, but its shutdown snapshot must wait
        # for this already-approved call to publish its exact owned child.
        assert not loss_finished.is_set()
        assert runtime._shutdown_owned_runs is None
        release_popen.set()
        assert communicate_entered.wait(2)
        assert loss_finished.wait(2)
        with runtime._running_lock:
            assert runtime._running_procs[1] is proc
            assert runtime._running_history_uids[1] == run_uid
            assert runtime._shutdown_owned_runs == {1: run_uid}
        stopped = runtime.request_stop_owned_scheduler_task(
            1, timeout_seconds=0.1, reason="scheduler_shutdown",
        )
        assert stopped["accepted"] is True
        assert stopped["job_id"] == run_uid
        terminate.assert_called_once_with(proc, timeout_seconds=0.1)
        release_child.set()
        assert child_finished.wait(2)
        child.join(timeout=2)
        loss.join(timeout=2)
        assert not child.is_alive() and not loss.is_alive()
        assert not case.errors
        popen.assert_called_once()
        finish.assert_called_once()
        assert finish.call_args.args == (case.work_engine, run_uid)
        assert finish.call_args.kwargs["status"] == "stopped"
        assert not runtime._running_procs
        assert runtime._shutdown_owned_runs == {1: run_uid}
    finally:
        release_popen.set()
        release_child.set()
        case.stop.set()
        for driver in (child, loss):
            if driver.ident is not None:
                driver.join(timeout=2)
                assert not driver.is_alive()


@pytest.mark.parametrize("column,value", [
    ("instance_id", "different-instance"), ("mode", "embedded"),
    ("host_name", "different-host"), ("pid", -1), ("build_sha", "b" * 40),
    ("executor_role", "linux_standalone"), ("started_at", "2000-01-01 00:00:00"),
    ("poll_seconds", 30), ("max_concurrent_tasks", 2),
])
def test_real_renewal_cas_rejects_every_changed_owner_field(lease_case, column, value):
    case = lease_case
    lease = runtime._WindowsSchedulerLease(case.stop)
    case.register(case.lease_engine, lease.owner)
    with case.work_engine.begin() as connection:
        connection.execute(text(f"UPDATE st_scheduler_runtime SET {column}=:value"), {"value": value})
    before = case.committed_count()
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="missing_changed_or_expired"):
        case.original_renew(case.lease_engine, lease.owner)
    assert case.committed_count() == before
    assert case.heartbeat_row()[column] == value


@pytest.mark.parametrize("age,allowed", [(0, True), (30, True), (31, False), (-1, False)])
def test_real_renewal_cas_obeys_database_lease_boundary(lease_case, age, allowed):
    case = lease_case
    lease = runtime._WindowsSchedulerLease(case.stop)
    case.register(case.lease_engine, lease.owner)
    old = datetime.fromisoformat(case.clock.now()) - timedelta(seconds=age)
    with case.work_engine.begin() as connection:
        connection.execute(text("UPDATE st_scheduler_runtime SET heartbeat_at=:old"), {"old": old})
    before = case.committed_count()
    if allowed:
        case.original_renew(case.lease_engine, lease.owner)
        assert case.committed_count() == before + 1
    else:
        with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="missing_changed_or_expired"):
            case.original_renew(case.lease_engine, lease.owner)
        assert case.committed_count() == before
        assert case.heartbeat_row()["heartbeat_at"] == old.isoformat(" ")


def test_real_renewal_cannot_insert_a_missing_owner_row(lease_case):
    case = lease_case
    lease = runtime._WindowsSchedulerLease(case.stop)
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="missing_changed_or_expired"):
        case.original_renew(case.lease_engine, lease.owner)
    assert case.committed_count() == 0
    with case.work_engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM st_scheduler_runtime")).scalar_one() == 0
