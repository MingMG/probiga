from __future__ import annotations

import inspect
import json
import os
import threading
from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine, text

from server.common import trading_v3_maintenance as maintenance_lock
from server.common.scheduler_authority import LAYER4_WRITER_TASK_TYPES
from server.trading_v3 import (
    counterfactual_worker,
    decision_worker,
    shadow_intelligence_worker,
)
from tools import add_trading_v3_tasks as task_deployment
from tools import trading_v3_layer4_maintenance as maintenance_cli


class _Engine:
    def __init__(self, dialect_name: str = "mysql") -> None:
        self.dialect = type("Dialect", (), {"name": dialect_name})()
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


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


def test_fence_only_rejects_drain_options_before_database_access(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        task_deployment,
        "create_tool_engine",
        lambda: pytest.fail("invalid fence-only arguments must not open DB"),
    )
    with pytest.raises(SystemExit) as captured:
        task_deployment.main([
            "--fence-only",
            "--require-no-live-scheduler-writers",
        ])
    assert captured.value.code == 2


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
        46,
        10,
        9,
    ]
    assert all(
        len(str(item["checksum"])) == 64
        for item in maintenance_cli.TARGET_MIGRATIONS
    )
