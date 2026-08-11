from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from server.trading_v4.infrastructure import (
    RuntimeControlConflictError,
    RuntimeControlRepository,
)
from server.trading_v4.infrastructure import control_plane


NOW = datetime(2026, 8, 4, 2, 3, 4, tzinfo=timezone.utc)


@pytest.fixture()
def concurrent_engine(tmp_path):
    # File SQLite gives each worker a real independent connection and an
    # analogous transient "database is locked" path.  It is a local contract
    # test, not evidence of Oracle MySQL 5.7 concurrency acceptance.
    database = tmp_path / "runtime-control-concurrency.sqlite"
    engine = create_engine(
        f"sqlite+pysqlite:///{database.as_posix()}",
        future=True,
        connect_args={"check_same_thread": False, "timeout": 0.02},
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_runtime_control_v4 (
                    control_key TEXT PRIMARY KEY,
                    control_value_json TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    updated_by TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_runtime_control_transition_v4 (
                    transition_id TEXT PRIMARY KEY,
                    control_key TEXT NOT NULL,
                    previous_value_json TEXT,
                    next_value_json TEXT NOT NULL,
                    next_version INTEGER NOT NULL,
                    changed_by TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE,
                    changed_at DATETIME NOT NULL,
                    UNIQUE (control_key, next_version)
                )
                """
            )
        )
    try:
        yield engine
    finally:
        engine.dispose()


def _race(repository: RuntimeControlRepository, key: str, arguments: list[dict[str, Any]]):
    barrier = Barrier(len(arguments))

    def invoke(values: dict[str, Any]):
        barrier.wait(timeout=5)
        return repository.compare_and_set_control(key, **values)

    with ThreadPoolExecutor(max_workers=len(arguments)) as pool:
        futures = [pool.submit(invoke, values) for values in arguments]
        return futures


def test_same_create_command_converges_to_one_change_and_one_replay(
    concurrent_engine,
) -> None:
    repository = RuntimeControlRepository(concurrent_engine)
    arguments = {
        "expected_version": 0,
        "next_value": {"enabled": False, "batch_size": 20},
        "changed_by": "two-thread-test",
        "reason": "same create command",
        "occurred_at": NOW,
    }
    futures = _race(repository, "concurrent-create", [arguments, arguments])
    results = [future.result(timeout=10) for future in futures]

    assert sorted(result.changed for result in results) == [False, True]
    assert {result.transition.event_hash for result in results} == {
        results[0].transition.event_hash
    }
    assert all(result.control.version == 1 for result in results)
    assert all(result.superseded is False for result in results)
    with concurrent_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_runtime_control_v4")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_runtime_control_transition_v4")
        ).scalar_one() == 1


def test_same_update_command_converges_to_one_change_and_one_replay(
    concurrent_engine,
) -> None:
    repository = RuntimeControlRepository(concurrent_engine)
    repository.compare_and_set_control(
        "concurrent-update",
        expected_version=0,
        next_value={"batch_size": 20},
        changed_by="setup",
        reason="seed version one",
        occurred_at=NOW,
    )
    arguments = {
        "expected_version": 1,
        "next_value": {"batch_size": 10},
        "changed_by": "two-thread-test",
        "reason": "same update command",
        "occurred_at": NOW + timedelta(seconds=1),
    }
    futures = _race(repository, "concurrent-update", [arguments, arguments])
    results = [future.result(timeout=10) for future in futures]

    assert sorted(result.changed for result in results) == [False, True]
    assert all(result.control.version == 2 for result in results)
    assert len({result.transition.event_hash for result in results}) == 1
    with concurrent_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_runtime_control_transition_v4")
        ).scalar_one() == 2


def test_different_commands_at_same_version_do_not_replay_each_other(
    concurrent_engine,
) -> None:
    repository = RuntimeControlRepository(concurrent_engine)
    common = {
        "expected_version": 0,
        "changed_by": "two-thread-test",
        "occurred_at": NOW,
    }
    futures = _race(
        repository,
        "concurrent-conflict",
        [
            {
                **common,
                "next_value": {"batch_size": 10},
                "reason": "command A",
            },
            {
                **common,
                "next_value": {"batch_size": 30},
                "reason": "command B",
            },
        ],
    )
    outcomes: list[object] = []
    for future in futures:
        try:
            outcomes.append(future.result(timeout=10))
        except RuntimeControlConflictError as exc:
            outcomes.append(exc)

    assert sum(not isinstance(item, Exception) for item in outcomes) == 1
    assert sum(isinstance(item, RuntimeControlConflictError) for item in outcomes) == 1
    with concurrent_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_runtime_control_transition_v4")
        ).scalar_one() == 1


class _TransientDBAPIError(Exception):
    def __init__(self, code: object, message: str) -> None:
        super().__init__(code, message)
        self.errno = code


@pytest.mark.parametrize(
    "original",
    (
        _TransientDBAPIError(1205, "Lock wait timeout exceeded"),
        _TransientDBAPIError(1213, "Deadlock found"),
        _TransientDBAPIError("40001", "serialization failure"),
        _TransientDBAPIError(None, "database is locked"),
    ),
)
def test_transient_lock_classifier_is_narrow(original) -> None:
    error = OperationalError("UPDATE control", {}, original)
    assert control_plane._is_transient_lock_error(error) is True

    unrelated = OperationalError(
        "UPDATE control",
        {},
        _TransientDBAPIError(2006, "server has gone away"),
    )
    assert control_plane._is_transient_lock_error(unrelated) is False


def test_transient_deadlock_retries_are_bounded_and_use_fresh_attempts(
    concurrent_engine,
    monkeypatch,
) -> None:
    repository = RuntimeControlRepository(concurrent_engine)
    calls = 0
    sentinel = object()

    def attempt(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OperationalError(
                "UPDATE control",
                {},
                _TransientDBAPIError(1213, "Deadlock found"),
            )
        return sentinel

    monkeypatch.setattr(repository, "_compare_and_set_once", attempt)
    monkeypatch.setattr(
        repository,
        "_resolve_replay_after_rollback",
        lambda **_kwargs: None,
    )

    result = repository.compare_and_set_control(
        "transient-retry",
        expected_version=0,
        next_value={"enabled": False},
        changed_by="test",
        reason="bounded retry",
        occurred_at=NOW,
    )
    assert result is sentinel
    assert calls == 3


def test_transient_retry_exhaustion_fails_closed(
    concurrent_engine,
    monkeypatch,
) -> None:
    repository = RuntimeControlRepository(concurrent_engine)
    calls = 0

    def always_locked(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise OperationalError(
            "UPDATE control",
            {},
            _TransientDBAPIError(1205, "Lock wait timeout exceeded"),
        )

    monkeypatch.setattr(repository, "_compare_and_set_once", always_locked)
    monkeypatch.setattr(
        repository,
        "_resolve_replay_after_rollback",
        lambda **_kwargs: None,
    )

    with pytest.raises(RuntimeControlConflictError, match="retry exhausted"):
        repository.compare_and_set_control(
            "transient-exhaustion",
            expected_version=0,
            next_value={"enabled": False},
            changed_by="test",
            reason="bounded retry exhaustion",
            occurred_at=NOW,
        )
    assert calls == control_plane._RUNTIME_CONTROL_MAX_ATTEMPTS
