from __future__ import annotations

import inspect
import json
import os
import threading
from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from server.common import trading_v3_maintenance as maintenance_lock
from server.common.scheduler_authority import (
    DEFERRED_PAPER_BUY_WRITER_TASK_TYPES,
    DEFERRED_RELEASE_WRITER_TASK_TYPES,
    LAYER4_WRITER_TASK_TYPES,
)
from server.trading_v3 import (
    counterfactual_worker,
    decision_worker,
    shadow_intelligence_worker,
)
from tools import add_trading_v3_tasks as task_deployment
from tools import trading_v3_layer4_maintenance as maintenance_cli
from tools import verify_trading_v3_production as production_verifier


def test_mysql_identity_query_avoids_reserved_current_user_alias() -> None:
    source = inspect.getsource(maintenance_cli._identity)
    assert "CURRENT_USER() AS effective_user" in source
    assert "CURRENT_USER() AS current_user" not in source


class _Engine:
    def __init__(self, dialect_name: str = "mysql") -> None:
        self.dialect = type("Dialect", (), {"name": dialect_name})()
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


class _QuiescenceClock:
    def __init__(self, monkeypatch) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []
        monkeypatch.setattr(maintenance_cli.time, "monotonic", lambda: self.now)
        monkeypatch.setattr(maintenance_cli.time, "sleep", self.sleep)

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _connection_failure(errno: int) -> OperationalError:
    return OperationalError(None, None, Exception(errno, "database unavailable"))


@pytest.mark.parametrize("errno", [2003, 2006, 2013])
@pytest.mark.parametrize("failed_stage", ["identity", "writers"])
def test_writer_quiescence_reconnects_and_rechecks_identity_before_inventory(
    monkeypatch, errno, failed_stage,
) -> None:
    clock = _QuiescenceClock(monkeypatch)
    events: list[str] = []
    failed = False
    engine = _Engine()

    def read(stage, result):
        nonlocal failed
        events.append(stage)
        if stage == failed_stage and not failed:
            failed = True
            raise _connection_failure(errno)
        return result

    monkeypatch.setattr(
        maintenance_cli, "_identity",
        lambda _: read("identity", {"server_uuid": "original"}),
    )
    monkeypatch.setattr(
        maintenance_cli, "read_fresh_scheduler_writers",
        lambda _: read("writers", ()),
    )
    monkeypatch.setattr(engine, "dispose", lambda: events.append("dispose"))

    result = maintenance_cli.wait_for_writer_quiescence(
        engine, timeout_seconds=5, poll_seconds=1,
    )

    assert result == {
        "status": "ok", "ready": True, "live_writer_count": 0,
        "live_writers": [],
    }
    before_failure = ["identity"] if failed_stage == "identity" else ["identity", "writers"]
    assert events == before_failure + ["dispose", "identity", "writers"]
    assert clock.sleeps == [1]


def test_writer_quiescence_persistent_disconnect_stops_at_original_deadline(
    monkeypatch,
) -> None:
    clock = _QuiescenceClock(monkeypatch)
    attempts: list[float] = []
    engine = _Engine()

    def unavailable(_engine):
        attempts.append(clock.now)
        raise _connection_failure(2003)

    monkeypatch.setattr(maintenance_cli, "_identity", unavailable)
    monkeypatch.setattr(
        maintenance_cli, "read_fresh_scheduler_writers",
        lambda _: pytest.fail("unverified database cannot report writers"),
    )

    with pytest.raises(
        maintenance_cli.MaintenanceBlocked,
        match="LAYER4_WRITER_QUIESCENCE_DATABASE_UNAVAILABLE",
    ) as failure:
        maintenance_cli.wait_for_writer_quiescence(
            engine, timeout_seconds=2.5, poll_seconds=1,
        )

    assert attempts == [0, 1, 2]
    assert clock.now == 2.5
    assert clock.sleeps == [1, 1, 0.5]
    assert isinstance(failure.value.__cause__, OperationalError)
    assert engine.disposed


@pytest.mark.parametrize("changed_field", ["server_uuid", "current_user"])
def test_writer_quiescence_rejects_changed_database_identity_after_reconnect(
    monkeypatch, changed_field,
) -> None:
    clock = _QuiescenceClock(monkeypatch)
    original = {"server_uuid": "original", "current_user": "runtime@localhost"}
    identities = iter([original, {**original, changed_field: "different"}])
    writer_reads: list[float] = []
    monkeypatch.setattr(maintenance_cli, "_identity", lambda _: next(identities))

    def disconnected(_engine):
        writer_reads.append(clock.now)
        raise _connection_failure(2013)

    monkeypatch.setattr(maintenance_cli, "read_fresh_scheduler_writers", disconnected)
    with pytest.raises(
        maintenance_cli.MaintenanceBlocked, match="LAYER4_DATABASE_IDENTITY_CHANGED",
    ):
        maintenance_cli.wait_for_writer_quiescence(
            _Engine(), timeout_seconds=10, poll_seconds=1,
        )

    assert writer_reads == [0]
    assert clock.sleeps == [1]


def test_writer_quiescence_reconnect_does_not_erase_live_writers_or_reset_deadline(
    monkeypatch,
) -> None:
    clock = _QuiescenceClock(monkeypatch)
    reads: list[float] = []
    monkeypatch.setattr(maintenance_cli, "_identity", lambda _: {"server_uuid": "same"})

    def read_writers(_engine):
        reads.append(clock.now)
        if len(reads) == 2:
            raise _connection_failure(2013)
        return ({"instance_id": "windows-42"}, {"instance_id": "linux-81"})

    monkeypatch.setattr(maintenance_cli, "read_fresh_scheduler_writers", read_writers)
    with pytest.raises(
        maintenance_cli.MaintenanceBlocked,
        match="LAYER4_FRESH_SCHEDULER_WRITERS_REMAIN:windows-42,linux-81",
    ):
        maintenance_cli.wait_for_writer_quiescence(
            _Engine(), timeout_seconds=3, poll_seconds=1,
        )

    assert reads == [0, 1, 2]
    assert clock.now == 3


@pytest.mark.parametrize("errno", [1044, 1045, 1142, 1146, 1205, 1213])
def test_writer_quiescence_does_not_retry_non_transport_database_errors(
    monkeypatch, errno,
) -> None:
    clock = _QuiescenceClock(monkeypatch)
    failure = _connection_failure(errno)
    engine = _Engine()

    def invalid(_engine):
        raise failure

    monkeypatch.setattr(maintenance_cli, "_identity", invalid)
    with pytest.raises(OperationalError) as captured:
        maintenance_cli.wait_for_writer_quiescence(
            engine, timeout_seconds=10, poll_seconds=1,
        )

    assert captured.value is failure
    assert not engine.disposed
    assert clock.sleeps == []


def test_writer_quiescence_never_reports_success_after_deadline(monkeypatch) -> None:
    clock = _QuiescenceClock(monkeypatch)
    monkeypatch.setattr(maintenance_cli, "_identity", lambda _: {"server_uuid": "same"})

    def slow_read(_engine):
        clock.now += 3
        return ()

    monkeypatch.setattr(maintenance_cli, "read_fresh_scheduler_writers", slow_read)
    with pytest.raises(
        maintenance_cli.MaintenanceBlocked, match="LAYER4_WRITER_QUIESCENCE_TIMEOUT",
    ):
        maintenance_cli.wait_for_writer_quiescence(
            _Engine(), timeout_seconds=2, poll_seconds=1,
        )

    assert clock.sleeps == []


def test_writer_quiescence_zero_timeout_permits_one_immediate_observation(
    monkeypatch,
) -> None:
    _QuiescenceClock(monkeypatch)
    monkeypatch.setattr(maintenance_cli, "_identity", lambda _: {"server_uuid": "same"})
    monkeypatch.setattr(maintenance_cli, "read_fresh_scheduler_writers", lambda _: ())
    assert maintenance_cli.wait_for_writer_quiescence(
        _Engine(), timeout_seconds=0, poll_seconds=1,
    )["ready"] is True


def test_fence_only_is_atomic_disable_without_upsert_or_schema_changes(
    monkeypatch,
    capsys,
) -> None:
    engine = _Engine()
    calls: list[str] = []
    monkeypatch.setattr(task_deployment, "load_project_env", lambda: None)
    monkeypatch.setattr(task_deployment, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        task_deployment,
        "enforce_layer4_writer_fence_atomically",
        lambda _engine: calls.append("disable") or 3,
    )
    monkeypatch.setattr(
        task_deployment,
        "upsert_scheduler_task",
        lambda *_args, **_kwargs: pytest.fail("fence-only must not upsert"),
    )
    monkeypatch.setattr(
        task_deployment,
        "layer4_activation_preconditions",
        lambda *_args, **_kwargs: pytest.fail("fence-only must not inspect schema"),
    )

    assert task_deployment.main(["--fence-only"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert calls == ["disable"]
    assert payload == {
        "status": "ok",
        "mode": "fence-only",
        "writer_fence_active": True,
        "fenced_row_count": 3,
        "layer4_writers_enabled": False,
        "writer_quiescence": {
            "checked": False,
            "ready": None,
            "live_writers": [],
        },
        "migration_readiness": {
            "checked": False,
            "ready": None,
            "reason_codes": [],
        },
        "tasks": [],
    }
    assert engine.disposed is True


def test_deferred_release_fence_only_disables_every_required_writer(
    monkeypatch,
    capsys,
) -> None:
    engine = _Engine()
    calls: list[str] = []
    monkeypatch.setattr(task_deployment, "load_project_env", lambda: None)
    monkeypatch.setattr(task_deployment, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        task_deployment,
        "enforce_deferred_release_writer_fence_atomically",
        lambda _engine: calls.append("disable-deferred-release")
        or len(DEFERRED_RELEASE_WRITER_TASK_TYPES),
    )
    monkeypatch.setattr(
        task_deployment,
        "enforce_layer4_writer_fence_atomically",
        lambda *_args: pytest.fail("deferred release fence must be one transaction"),
    )
    monkeypatch.setattr(
        task_deployment,
        "upsert_scheduler_task",
        lambda *_args, **_kwargs: pytest.fail("isolated fence must not upsert"),
    )

    assert task_deployment.main(["--deferred-release-fence-only"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert calls == ["disable-deferred-release"]
    assert payload["mode"] == "deferred-release-fence-only"
    assert payload["fenced_row_count"] == len(DEFERRED_RELEASE_WRITER_TASK_TYPES)
    assert set(payload["fenced_task_types"]) == set(
        DEFERRED_RELEASE_WRITER_TASK_TYPES
    )
    assert payload["layer4_writers_enabled"] is False
    assert payload["paper_buy_writers_enabled"] is False
    assert payload["tasks"] == []
    assert engine.disposed is True


def test_deferred_release_fence_satisfies_real_trading_closed_task_contract() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_type TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_scheduled_tasks (id, task_type, enabled)
                VALUES (:id, :task_type, 1)
            """),
            [
                {"id": index, "task_type": task_type}
                for index, task_type in enumerate(
                    DEFERRED_RELEASE_WRITER_TASK_TYPES,
                    start=1,
                )
            ],
        )

    assert task_deployment.enforce_deferred_release_writer_fence_atomically(
        engine
    ) == len(DEFERRED_RELEASE_WRITER_TASK_TYPES)
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(text("""
                SELECT task_type, enabled
                FROM st_scheduled_tasks
                WHERE task_type IN (
                    'trading_v3_close_decision',
                    'trading_v3_premarket_review'
                )
                ORDER BY task_type
            """)).mappings()
        ]
    assert {row["task_type"] for row in rows} == set(
        DEFERRED_PAPER_BUY_WRITER_TASK_TYPES
    )
    assert production_verifier._deferred_paper_buy_writer_rows_valid(rows)
    engine.dispose()


def test_deferred_release_fence_only_rejects_drain_options_before_database_access(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        task_deployment,
        "create_tool_engine",
        lambda: pytest.fail("invalid fence-only arguments must not open DB"),
    )
    with pytest.raises(SystemExit) as captured:
        task_deployment.main([
            "--deferred-release-fence-only",
            "--require-no-live-scheduler-writers",
        ])
    assert captured.value.code == 2


def test_fence_only_drains_writers_without_upsert_or_schema_changes(
    monkeypatch,
    capsys,
) -> None:
    engine = _Engine()
    calls: list[object] = []
    monkeypatch.setattr(task_deployment, "load_project_env", lambda: None)
    monkeypatch.setattr(task_deployment, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        task_deployment,
        "enforce_layer4_writer_fence_atomically",
        lambda _engine: calls.append("disable") or 4,
    )
    monkeypatch.setattr(
        task_deployment,
        "wait_for_scheduler_writer_quiescence",
        lambda _engine, **kwargs: calls.append(("drain", kwargs)) or (),
    )
    monkeypatch.setattr(
        task_deployment,
        "upsert_scheduler_task",
        lambda *_args, **_kwargs: pytest.fail("fence-only must not upsert"),
    )
    monkeypatch.setattr(
        task_deployment,
        "layer4_activation_preconditions",
        lambda *_args, **_kwargs: pytest.fail("fence-only must not inspect schema"),
    )

    assert task_deployment.main([
        "--fence-only",
        "--require-no-live-scheduler-writers",
        "--writer-drain-timeout-seconds",
        "150",
        "--writer-drain-poll-seconds",
        "2",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert calls == [
        "disable",
        (
            "drain",
            {"timeout_seconds": 150.0, "poll_seconds": 2.0},
        ),
    ]
    assert payload["status"] == "ok"
    assert payload["mode"] == "fence-only"
    assert payload["writer_quiescence"] == {
        "checked": True,
        "ready": True,
        "reason_codes": [],
        "live_writers": [],
    }
    assert payload["tasks"] == []
    assert engine.disposed is True


def test_writer_decorator_holds_shared_mysql_lock_for_complete_call(
    monkeypatch,
) -> None:
    events: list[str] = []

    @contextmanager
    def fake_lock(_engine, name, *, timeout_seconds):
        assert name == maintenance_lock.TRADING_V3_MAINTENANCE_LOCK_NAME
        assert timeout_seconds == 0
        events.append("acquire")
        try:
            yield object()
        finally:
            events.append("release")

    monkeypatch.setattr(maintenance_lock, "mysql_named_lock", fake_lock)

    @maintenance_lock.trading_v3_writer
    def writer(engine, value):
        events.append(f"write:{value}")
        return value + 1

    assert writer(_Engine(), 4) == 5
    assert events == ["acquire", "write:4", "release"]
    assert inspect.unwrap(writer).__name__ == "writer"


def test_writer_decorator_fails_closed_when_maintenance_lock_is_busy(
    monkeypatch,
) -> None:
    @contextmanager
    def busy(*_args, **_kwargs):
        raise TimeoutError("busy")
        yield  # pragma: no cover

    monkeypatch.setattr(maintenance_lock, "mysql_named_lock", busy)

    @maintenance_lock.trading_v3_writer
    def writer(_engine):
        pytest.fail("writer body must not run")

    with pytest.raises(
        maintenance_lock.TradingV3WriterLeaseUnavailable,
        match="MAINTENANCE_WINDOW_ACTIVE_OR_WRITER_BUSY",
    ):
        writer(_Engine())


def test_all_production_v3_writer_entrypoints_are_guarded() -> None:
    guarded = (
        decision_worker.run_daily_decision_v3,
        counterfactual_worker.drain_counterfactual_backlog,
        shadow_intelligence_worker.run_shadow_intelligence_cycle,
        shadow_intelligence_worker.run_continuous_model_lifecycle_cycle,
    )
    assert all(hasattr(function, "__wrapped__") for function in guarded)


def test_non_mysql_writer_tests_do_not_claim_a_cross_process_lock(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        maintenance_lock,
        "mysql_named_lock",
        lambda *_args, **_kwargs: pytest.fail("SQLite must not claim MySQL lock"),
    )
    events: list[str] = []

    @maintenance_lock.trading_v3_writer
    def writer(_engine):
        events.append("called")

    writer(_Engine("sqlite"))
    assert events == ["called"]


def test_task_state_requires_exact_two_rows_and_expected_enabled_bit(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(maintenance_cli, "_identity", lambda _engine: {})
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_scheduled_tasks ("
            "id INTEGER PRIMARY KEY, task_type TEXT, enabled INTEGER, "
            "script_path TEXT, script_args TEXT, date_param TEXT, "
            "cron_time TEXT, interval_minutes INTEGER)"
        ))
        for index, task_type in enumerate(LAYER4_WRITER_TASK_TYPES, start=1):
            connection.execute(text(
                "INSERT INTO st_scheduled_tasks VALUES "
                "(:id,:task_type,0,'x.py','','','00:00',0)"
            ), {"id": index, "task_type": task_type})

    result = maintenance_cli.collect_task_state(
        engine,
        expected_enabled=False,
    )
    assert result["task_count"] == 2
    with pytest.raises(maintenance_cli.MaintenanceBlocked):
        maintenance_cli.collect_task_state(engine, expected_enabled=True)
    engine.dispose()


def test_hold_lock_publishes_ready_file_and_releases_on_signal_file(
    monkeypatch,
    tmp_path,
) -> None:
    engine = _Engine()
    monkeypatch.setattr(
        maintenance_cli,
        "_identity",
        lambda _engine: {"server_uuid": "a" * 36},
    )

    class _Scalar:
        def scalar_one(self):
            return 42

    class _Connection:
        def execute(self, _statement):
            return _Scalar()

    @contextmanager
    def held(*_args, **_kwargs):
        yield _Connection()

    monkeypatch.setattr(maintenance_cli, "mysql_named_lock", held)
    ready = tmp_path / "ready.json"
    release = tmp_path / "release"

    def signal_release() -> None:
        for _ in range(100):
            if ready.exists():
                release.touch()
                return
            threading.Event().wait(0.01)
        raise AssertionError("ready file was not published")

    thread = threading.Thread(target=signal_release)
    thread.start()
    result = maintenance_cli.hold_maintenance_lock(
        engine,
        ready_file=ready,
        release_file=release,
        timeout_seconds=0,
        max_hold_seconds=30,
        parent_pid=os.getpid(),
    )
    thread.join(timeout=2)
    assert result["status"] == "released"
    assert json.loads(ready.read_text(encoding="utf-8"))["connection_id"] == 42


def test_process_liveness_probe_never_signals_current_process() -> None:
    assert maintenance_cli._process_is_alive(os.getpid()) is True


def test_target_migration_contract_is_exact_and_forward_only() -> None:
    assert [item["version"] for item in maintenance_cli.TARGET_MIGRATIONS] == [
        "20260804_000_shadow_intelligence_runtime",
        "20260817_000_horizon_protocol_v2_governance",
        "20260817_001_horizon_candidate_ledger_registration",
    ]
    assert [item["statement_count"] for item in maintenance_cli.TARGET_MIGRATIONS] == [
        10,
        2,
        1,
    ]
    assert all(
        len(str(item["checksum"])) == 64
        for item in maintenance_cli.TARGET_MIGRATIONS
    )
