from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import inspect
from threading import Barrier

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from server.db import migrations_v4
from server.trading_v4.infrastructure import (
    JOB_LEASE_MAX_DURATION_SECONDS,
    JobClockSkewError,
)
from server.trading_v4.infrastructure import job_store as job_store_module
from server.trading_v4.infrastructure.job_store import (
    EXHAUSTED_LEASE_ERROR_CODE,
    JOB_FAILED,
    JOB_PENDING,
    JOB_RUNNING,
    JOB_SUCCEEDED,
    ClaimJobResult,
    JobAlreadyTerminalError,
    JobConflictError,
    JobSnapshot,
    JobStoreIntegrityError,
    JobStoreRepository,
)
from server.trading_v4.ports import JobStorePort


UTC = timezone.utc
CREATED_AT = datetime(2026, 8, 4, 1, 0, 0, tzinfo=UTC)
SCHEDULED_FOR = CREATED_AT + timedelta(seconds=1)
CLAIM_AT = CREATED_AT + timedelta(seconds=2)
LEASE_UNTIL = CLAIM_AT + timedelta(seconds=30)


class _SyntheticDatabaseError(Exception):
    def __init__(self, code: int | str) -> None:
        super().__init__(code, "synthetic database failure")
        self.errno = code
        self.sqlstate = code if isinstance(code, str) else None


def _operational_error(code: int | str) -> OperationalError:
    return OperationalError(
        "synthetic statement",
        {},
        _SyntheticDatabaseError(code),
    )


SQLITE_JOB_SCHEMA = """
CREATE TABLE st_job_run_v4 (
    job_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    job_type TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    input_context_id TEXT NOT NULL DEFAULT '',
    input_hash TEXT NOT NULL DEFAULT '',
    run_uid TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_token TEXT NULL,
    lease_until TEXT NULL,
    next_attempt_at TEXT NULL,
    error_code TEXT NULL,
    error_message TEXT NULL,
    started_at TEXT NULL,
    completed_at TEXT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (lease_token)
)
"""
SQLITE_CLAIM_TOKEN_SCHEMA = """
CREATE TABLE st_job_claim_token_v4 (
    lease_token TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    lease_owner TEXT NOT NULL,
    claimed_at TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    UNIQUE (job_id, attempt_count),
    FOREIGN KEY (job_id) REFERENCES st_job_run_v4 (job_id)
)
"""


@pytest.fixture()
def store(tmp_path) -> JobStoreRepository:
    database = tmp_path / "v4-job-store.sqlite3"
    engine = create_engine(
        f"sqlite+pysqlite:///{database.as_posix()}",
        future=True,
        connect_args={"check_same_thread": False, "timeout": 10},
    )
    with engine.begin() as connection:
        connection.execute(text("PRAGMA journal_mode = WAL"))
        connection.execute(text(SQLITE_JOB_SCHEMA))
        connection.execute(text(SQLITE_CLAIM_TOKEN_SCHEMA))
    repository = JobStoreRepository(engine)
    try:
        yield repository
    finally:
        engine.dispose()


def _create(
    store: JobStoreRepository,
    *,
    job_id: str = "job-1",
    key: str = "a" * 64,
    job_type: str = "DAILY_DECISION",
    max_attempts: int = 3,
    scheduled_for: datetime = SCHEDULED_FOR,
    created_at: datetime = CREATED_AT,
):
    return store.create_job(
        job_id=job_id,
        idempotency_key=key,
        job_type=job_type,
        scheduled_for=scheduled_for,
        max_attempts=max_attempts,
        created_at=created_at,
        input_context_id="context-1",
        input_hash="b" * 64,
    )


def _claim(
    store: JobStoreRepository,
    *,
    token: str = "c" * 64,
    worker: str = "worker-1",
    now: datetime = CLAIM_AT,
    lease_until: datetime = LEASE_UNTIL,
    job_type: str | None = None,
) -> ClaimJobResult | None:
    return store.claim_due_job(
        worker_id=worker,
        lease_token=token,
        now=now,
        lease_until=lease_until,
        job_type=job_type,
    )


def _claim_token_rows(store: JobStoreRepository) -> tuple[dict, ...]:
    with store.engine.connect() as connection:
        return tuple(
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT lease_token, job_id, attempt_count, lease_owner, "
                    "claimed_at, lease_until FROM st_job_claim_token_v4 "
                    "ORDER BY job_id, attempt_count"
                )
            ).mappings()
        )


def test_job_store_implements_port_and_create_is_exact_idempotent(store) -> None:
    assert isinstance(store, JobStorePort)

    first = _create(store)
    replay = _create(store)

    assert first.created is True
    assert replay.created is False
    assert replay.job == first.job
    assert first.job.status == JOB_PENDING
    assert first.job.attempt_count == 0
    assert first.job.max_attempts == 3
    assert first.job.next_attempt_at == SCHEDULED_FOR
    assert store.get_job("job-1") == first.job
    assert _claim_token_rows(store) == ()

    with pytest.raises(JobConflictError) as raised:
        _create(store, job_type="INTRADAY_RECALC")
    assert raised.value.current == first.job


def test_public_contract_freezes_exhaustion_code_and_token_freshness() -> None:
    assert issubclass(JobClockSkewError, RuntimeError)
    assert (
        EXHAUSTED_LEASE_ERROR_CODE
        == migrations_v4.JOB_LEASE_EXHAUSTED_ERROR_CODE
    )
    assert (
        JOB_LEASE_MAX_DURATION_SECONDS
        == migrations_v4.JOB_LEASE_MAX_DURATION_SECONDS
        == 900
    )
    migration = next(
        item
        for item in migrations_v4.MIGRATIONS
        if item["version"] == migrations_v4.JOB_LEASE_MIGRATION_VERSION
    )
    assert EXHAUSTED_LEASE_ERROR_CODE in "\n".join(migration["statements"])
    claim_contract = inspect.getdoc(JobStorePort.claim_due_job) or ""
    assert "globally fresh, never-reused" in claim_contract
    assert "append-only registry" in claim_contract
    assert "historical" in claim_contract


def test_create_replay_returns_current_lifecycle_state(store) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None

    replay = _create(store)

    assert replay.created is False
    assert replay.job == claimed.job
    assert replay.job.status == JOB_RUNNING


def test_create_retries_only_recognized_transient_transaction_errors(
    store,
    monkeypatch,
) -> None:
    original = job_store_module._job_by_idempotency_key
    calls = 0

    def transient_then_read(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _operational_error(1213)
        return original(*args, **kwargs)

    monkeypatch.setattr(
        job_store_module,
        "_job_by_idempotency_key",
        transient_then_read,
    )
    created = _create(store)
    assert created.created is True
    assert calls == 3

    def unrelated_failure(*_args, **_kwargs):
        raise _operational_error(9999)

    monkeypatch.setattr(
        job_store_module,
        "_job_by_idempotency_key",
        unrelated_failure,
    )
    with pytest.raises(OperationalError) as raised:
        _create(store, job_id="job-2", key="2" * 64)
    assert getattr(raised.value.orig, "errno", None) == 9999


def test_claim_retries_recognized_transient_transaction_error(
    store,
    monkeypatch,
) -> None:
    _create(store)
    original = job_store_module._claim_token_by_token
    calls = 0

    def transient_then_read(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _operational_error("40001")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        job_store_module,
        "_claim_token_by_token",
        transient_then_read,
    )
    claimed = _claim(store)
    assert claimed is not None
    assert claimed.claimed is True
    assert calls == 4


@pytest.mark.parametrize(
    "overrides",
    (
        {"idempotency_key": "A" * 64},
        {"max_attempts": True},
        {"created_at": CREATED_AT.replace(tzinfo=None)},
        {"job_id": " job-1"},
        {"input_hash": "not-a-hash"},
    ),
)
def test_create_rejects_non_exact_inputs(store, overrides) -> None:
    values = {
        "job_id": "job-1",
        "idempotency_key": "a" * 64,
        "job_type": "DAILY_DECISION",
        "scheduled_for": SCHEDULED_FOR,
        "max_attempts": 3,
        "created_at": CREATED_AT,
        "input_context_id": "",
        "input_hash": "",
    }
    values.update(overrides)
    with pytest.raises((TypeError, ValueError)):
        store.create_job(**values)


def test_claim_respects_due_time_and_job_type_then_replays_same_token(store) -> None:
    _create(store)
    assert _claim(store, now=CREATED_AT + timedelta(microseconds=1)) is None
    assert _claim(store, job_type="OTHER") is None

    claimed = _claim(store, job_type="DAILY_DECISION")
    assert claimed is not None
    assert claimed.claimed is True
    assert claimed.replayed is False
    assert claimed.job.status == JOB_RUNNING
    assert claimed.job.attempt_count == 1
    assert claimed.job.lease_owner == "worker-1"
    assert claimed.job.lease_token == "c" * 64
    assert claimed.job.lease_until == LEASE_UNTIL
    assert claimed.job.started_at == CLAIM_AT
    assert claimed.job.next_attempt_at is None
    registry_rows = _claim_token_rows(store)
    assert len(registry_rows) == 1
    registry_row = registry_rows[0]
    assert {
        key: registry_row[key]
        for key in (
            "lease_token",
            "job_id",
            "attempt_count",
            "lease_owner",
        )
    } == {
        "lease_token": "c" * 64,
        "job_id": "job-1",
        "attempt_count": 1,
        "lease_owner": "worker-1",
    }
    assert datetime.fromisoformat(str(registry_row["claimed_at"])).replace(
        tzinfo=UTC
    ) == CLAIM_AT
    assert datetime.fromisoformat(str(registry_row["lease_until"])).replace(
        tzinfo=UTC
    ) == LEASE_UNTIL

    replay = _claim(
        store,
        now=CLAIM_AT,
        lease_until=LEASE_UNTIL,
        job_type="DAILY_DECISION",
    )
    assert replay is not None
    assert replay.claimed is False
    assert replay.replayed is True
    assert replay.job == claimed.job

    with pytest.raises(JobConflictError) as changed_lease:
        _claim(
            store,
            now=CLAIM_AT,
            lease_until=LEASE_UNTIL + timedelta(minutes=5),
            job_type="DAILY_DECISION",
        )
    assert changed_lease.value.current == claimed.job

    with pytest.raises(JobConflictError) as changed_now:
        _claim(
            store,
            now=CLAIM_AT + timedelta(seconds=1),
            lease_until=LEASE_UNTIL,
            job_type="DAILY_DECISION",
        )
    assert changed_now.value.current == claimed.job

    with pytest.raises(JobConflictError) as raised:
        _claim(
            store,
            worker="worker-2",
            now=CLAIM_AT + timedelta(seconds=1),
        )
    assert raised.value.current == claimed.job


def test_historical_claim_token_cannot_be_reused_after_terminal_clear(
    store,
) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None
    completed = store.complete(
        "job-1",
        worker_id="worker-1",
        lease_token="c" * 64,
        attempt_count=1,
        observed_lease_until=LEASE_UNTIL,
        run_uid="run-1",
        now=CLAIM_AT + timedelta(seconds=5),
    )
    _create(
        store,
        job_id="job-2",
        key="2" * 64,
        scheduled_for=CLAIM_AT + timedelta(seconds=6),
        created_at=CLAIM_AT + timedelta(seconds=5, microseconds=1),
    )

    with pytest.raises(JobConflictError) as reused:
        _claim(
            store,
            token="c" * 64,
            worker="worker-2",
            now=CLAIM_AT + timedelta(seconds=7),
            lease_until=CLAIM_AT + timedelta(seconds=37),
        )

    assert reused.value.current == completed
    second = store.get_job("job-2")
    assert second is not None
    assert second.status == JOB_PENDING
    assert second.attempt_count == 0
    assert len(_claim_token_rows(store)) == 1


def test_registry_attempt_collision_rolls_back_job_claim_cas(store) -> None:
    created = _create(store)
    with store.engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO st_job_claim_token_v4 ("
                "lease_token, job_id, attempt_count, lease_owner, "
                "claimed_at, lease_until) VALUES ("
                ":lease_token, :job_id, 1, :lease_owner, "
                ":claimed_at, :lease_until)"
            ),
            {
                "lease_token": "d" * 64,
                "job_id": "job-1",
                "lease_owner": "preexisting",
                "claimed_at": CLAIM_AT.replace(tzinfo=None),
                "lease_until": LEASE_UNTIL.replace(tzinfo=None),
            },
        )

    with pytest.raises(JobConflictError, match="collided"):
        _claim(store, token="c" * 64)

    assert store.get_job("job-1") == created.job
    assert _claim_token_rows(store)[0]["lease_token"] == "d" * 64


def test_exact_claim_replay_bypasses_new_mutation_clock_gate(
    store,
    monkeypatch,
) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None

    def reject_new_mutation(_connection, _caller_now):
        raise JobClockSkewError("synthetic skew")

    monkeypatch.setattr(
        job_store_module,
        "_assert_database_clock",
        reject_new_mutation,
    )
    replay = _claim(store, now=CLAIM_AT, lease_until=LEASE_UNTIL)
    assert replay is not None
    assert replay.replayed is True
    assert replay.job == claimed.job


def test_exact_claim_replay_uses_locking_current_read(store, monkeypatch) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None
    original = job_store_module._job_by_id
    observed_lock_modes: list[bool] = []

    def guarded_job_read(connection, job_id, *, for_update):
        observed_lock_modes.append(for_update)
        if not for_update:
            raise AssertionError(
                "claim replay must not use a repeatable-read snapshot"
            )
        return original(connection, job_id, for_update=for_update)

    monkeypatch.setattr(job_store_module, "_job_by_id", guarded_job_read)

    replay = _claim(store, now=CLAIM_AT, lease_until=LEASE_UNTIL)

    assert replay is not None
    assert replay.replayed is True
    assert replay.job == claimed.job
    assert observed_lock_modes == [True]


def test_mysql_live_lease_replay_check_uses_locking_current_read(store) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None

    class SyntheticResult:
        def __init__(self, row):
            self.row = row

        def mappings(self):
            return self

        def first(self):
            return self.row

    class RepeatableReadConnection:
        class Dialect:
            name = "mysql"

        dialect = Dialect()

        def __init__(self):
            self.statements: list[str] = []
            self.params: list[dict] = []

        def execute(self, statement, params):
            rendered = str(statement)
            self.statements.append(rendered)
            self.params.append(dict(params))
            # Model the production failure: an ordinary consistent read sees
            # the old snapshot, while a locking/current read sees the winner.
            if "FOR UPDATE" not in rendered.upper():
                return SyntheticResult(None)
            return SyntheticResult({"job_id": claimed.job.job_id})

    connection = RepeatableReadConnection()

    assert job_store_module._lease_is_live_at_database(
        connection, claimed.job
    ) is True
    assert len(connection.statements) == 1
    assert connection.statements[0].rstrip().upper().endswith("FOR UPDATE")
    assert connection.params == [{
        "job_id": claimed.job.job_id,
        "lease_token": claimed.job.lease_token,
    }]


def test_claim_and_heartbeat_enforce_single_lease_duration_boundary(store) -> None:
    _create(store)
    with pytest.raises(ValueError, match="maximum single-lease duration"):
        _claim(
            store,
            lease_until=CLAIM_AT
            + timedelta(seconds=JOB_LEASE_MAX_DURATION_SECONDS, microseconds=1),
        )

    boundary_until = CLAIM_AT + timedelta(
        seconds=JOB_LEASE_MAX_DURATION_SECONDS
    )
    claimed = _claim(store, lease_until=boundary_until)
    assert claimed is not None
    assert claimed.job.lease_until == boundary_until

    heartbeat_at = CLAIM_AT + timedelta(seconds=1)
    with pytest.raises(ValueError, match="maximum single-lease duration"):
        store.heartbeat(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=boundary_until,
            now=heartbeat_at,
            lease_until=heartbeat_at
            + timedelta(seconds=JOB_LEASE_MAX_DURATION_SECONDS, microseconds=1),
        )

    heartbeat_boundary = heartbeat_at + timedelta(
        seconds=JOB_LEASE_MAX_DURATION_SECONDS
    )
    renewed = store.heartbeat(
        "job-1",
        worker_id="worker-1",
        lease_token="c" * 64,
        attempt_count=1,
        observed_lease_until=boundary_until,
        now=heartbeat_at,
        lease_until=heartbeat_boundary,
    )
    assert renewed.lease_until == heartbeat_boundary


def test_expired_lease_requires_fresh_token_and_increments_attempt(store) -> None:
    _create(store)
    first = _claim(store)
    assert first is not None

    with pytest.raises(JobConflictError) as raised:
        _claim(
            store,
            token="c" * 64,
            now=LEASE_UNTIL,
            lease_until=LEASE_UNTIL + timedelta(seconds=30),
        )
    assert raised.value.current == first.job

    reclaimed = _claim(
        store,
        token="d" * 64,
        worker="worker-2",
        now=LEASE_UNTIL,
        lease_until=LEASE_UNTIL + timedelta(seconds=30),
    )
    assert reclaimed is not None
    assert reclaimed.claimed is True
    assert reclaimed.job.attempt_count == 2
    assert reclaimed.job.lease_owner == "worker-2"
    assert reclaimed.job.lease_token == "d" * 64
    assert reclaimed.job.started_at == first.job.started_at
    assert tuple(
        (row["lease_token"], row["attempt_count"])
        for row in _claim_token_rows(store)
    ) == (("c" * 64, 1), ("d" * 64, 2))

    with pytest.raises(JobConflictError) as stale:
        store.heartbeat(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=LEASE_UNTIL + timedelta(seconds=1),
            lease_until=LEASE_UNTIL + timedelta(seconds=40),
        )
    assert stale.value.current == reclaimed.job


def test_heartbeat_is_full_cas_and_only_extends_lease(store) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None
    heartbeat_at = CLAIM_AT + timedelta(seconds=5)
    extended_until = LEASE_UNTIL + timedelta(seconds=15)

    renewed = store.heartbeat(
        "job-1",
        worker_id="worker-1",
        lease_token="c" * 64,
        attempt_count=1,
        observed_lease_until=LEASE_UNTIL,
        now=heartbeat_at,
        lease_until=extended_until,
    )

    assert renewed == replace(
        claimed.job,
        lease_until=extended_until,
        updated_at=heartbeat_at,
    )
    with pytest.raises(JobConflictError) as stale:
        store.heartbeat(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=heartbeat_at + timedelta(seconds=1),
            lease_until=extended_until + timedelta(seconds=1),
        )
    assert stale.value.current == renewed

    with pytest.raises(ValueError, match="strictly extend"):
        store.heartbeat(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=extended_until,
            now=heartbeat_at + timedelta(seconds=1),
            lease_until=extended_until,
        )


def test_mutations_require_strictly_increasing_updated_at(store) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None

    with pytest.raises(ValueError, match="strictly later"):
        store.heartbeat(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=CLAIM_AT,
            lease_until=LEASE_UNTIL + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="strictly later"):
        store.complete(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            run_uid="run-equal-time",
            now=CLAIM_AT,
        )
    with pytest.raises(ValueError, match="strictly later"):
        store.fail(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=CLAIM_AT,
            retryable=False,
            next_attempt_at=None,
            error_code="EQUAL_TIME",
        )


def test_claim_requires_now_after_current_updated_at(store) -> None:
    _create(store, scheduled_for=CREATED_AT)
    with pytest.raises(ValueError, match="strictly later"):
        _claim(
            store,
            now=CREATED_AT,
            lease_until=CREATED_AT + timedelta(seconds=30),
        )


def test_complete_clears_token_and_terminal_retry_is_not_idempotent(store) -> None:
    _create(store)
    claimed = _claim(store)
    assert claimed is not None
    completed_at = CLAIM_AT + timedelta(seconds=10)

    completed = store.complete(
        "job-1",
        worker_id="worker-1",
        lease_token="c" * 64,
        attempt_count=1,
        observed_lease_until=LEASE_UNTIL,
        run_uid="run-1",
        now=completed_at,
    )

    assert completed.status == JOB_SUCCEEDED
    assert completed.run_uid == "run-1"
    assert completed.completed_at == completed_at
    assert completed.updated_at == completed_at
    assert completed.lease_owner == ""
    assert completed.lease_token is None
    assert completed.lease_until is None

    with pytest.raises(JobAlreadyTerminalError) as replay:
        store.complete(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            run_uid="run-1",
            now=completed_at + timedelta(seconds=1),
        )
    assert replay.value.current == completed

    with pytest.raises(JobAlreadyTerminalError) as failed_replay:
        store.fail(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=completed_at + timedelta(seconds=1),
            retryable=False,
            next_attempt_at=None,
            error_code="LATE_FAILURE",
        )
    assert failed_replay.value.current == completed


def test_terminal_retry_precedes_clock_skew_gate(store, monkeypatch) -> None:
    _create(store)
    _claim(store)
    completed_at = CLAIM_AT + timedelta(seconds=10)
    completed = store.complete(
        "job-1",
        worker_id="worker-1",
        lease_token="c" * 64,
        attempt_count=1,
        observed_lease_until=LEASE_UNTIL,
        run_uid="run-1",
        now=completed_at,
    )

    def reject_new_mutation(_connection, _caller_now):
        raise JobClockSkewError("synthetic skew")

    monkeypatch.setattr(
        job_store_module,
        "_assert_database_clock",
        reject_new_mutation,
    )
    with pytest.raises(JobAlreadyTerminalError) as replay:
        store.complete(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            run_uid="run-1",
            now=completed_at + timedelta(hours=1),
        )
    assert replay.value.current == completed


def test_retryable_failure_schedules_pending_then_terminal_failure(store) -> None:
    _create(store, max_attempts=2)
    first = _claim(store)
    assert first is not None
    failed_at = CLAIM_AT + timedelta(seconds=5)
    retry_at = failed_at + timedelta(seconds=10)

    pending = store.fail(
        "job-1",
        worker_id="worker-1",
        lease_token="c" * 64,
        attempt_count=1,
        observed_lease_until=LEASE_UNTIL,
        now=failed_at,
        retryable=True,
        next_attempt_at=retry_at,
        error_code="TRANSIENT",
        error_message="temporary source failure",
    )

    assert pending.status == JOB_PENDING
    assert pending.attempt_count == 1
    assert pending.next_attempt_at == retry_at
    assert pending.completed_at is None
    assert pending.error_code == "TRANSIENT"
    assert pending.lease_token is None
    assert _claim(
        store,
        token="d" * 64,
        now=retry_at - timedelta(microseconds=1),
        lease_until=retry_at + timedelta(seconds=30),
    ) is None

    second = _claim(
        store,
        token="d" * 64,
        now=retry_at,
        lease_until=retry_at + timedelta(seconds=30),
    )
    assert second is not None
    assert second.job.attempt_count == 2
    terminal_at = retry_at + timedelta(seconds=5)
    terminal = store.fail(
        "job-1",
        worker_id="worker-1",
        lease_token="d" * 64,
        attempt_count=2,
        observed_lease_until=retry_at + timedelta(seconds=30),
        now=terminal_at,
        retryable=True,
        next_attempt_at=terminal_at + timedelta(seconds=10),
        error_code="STILL_BROKEN",
    )
    assert terminal.status == JOB_FAILED
    assert terminal.completed_at == terminal_at
    assert terminal.next_attempt_at is None
    assert terminal.attempt_count == 2


def test_failure_retry_time_and_boolean_are_strict(store) -> None:
    _create(store)
    _claim(store)
    with pytest.raises(TypeError, match="exactly bool"):
        store.fail(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=CLAIM_AT + timedelta(seconds=1),
            retryable=1,
            next_attempt_at=None,
            error_code="BROKEN",
        )
    with pytest.raises(ValueError, match="strictly later"):
        store.fail(
            "job-1",
            worker_id="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=CLAIM_AT + timedelta(seconds=1),
            retryable=True,
            next_attempt_at=CLAIM_AT + timedelta(seconds=1),
            error_code="BROKEN",
        )


def test_expired_final_attempt_is_failed_before_claim_returns(store) -> None:
    _create(store, max_attempts=1)
    claimed = _claim(store)
    assert claimed is not None
    reap_at = LEASE_UNTIL

    result = _claim(
        store,
        worker="worker-2",
        token="d" * 64,
        now=reap_at,
        lease_until=reap_at + timedelta(seconds=30),
    )

    assert result is None
    terminal = store.get_job("job-1")
    assert terminal is not None
    assert terminal.status == JOB_FAILED
    assert terminal.attempt_count == 1
    assert terminal.error_code == EXHAUSTED_LEASE_ERROR_CODE
    assert terminal.completed_at == reap_at
    assert terminal.lease_token is None


def test_exhausted_reaper_is_bounded_and_makes_committed_progress(
    store,
    monkeypatch,
) -> None:
    for index in range(3):
        _create(
            store,
            job_id=f"job-{index}",
            key=f"{index + 1:064x}",
            max_attempts=1,
        )
        claimed = _claim(
            store,
            token=f"{index + 100:064x}",
            worker=f"worker-{index}",
        )
        assert claimed is not None
        assert claimed.job.job_id == f"job-{index}"

    monkeypatch.setattr(job_store_module, "EXHAUSTED_REAP_BATCH_SIZE", 2)
    first_pass = _claim(
        store,
        token="e" * 64,
        worker="reaper-1",
        now=LEASE_UNTIL,
        lease_until=LEASE_UNTIL + timedelta(seconds=30),
    )
    assert first_pass is None
    first_states = tuple(
        store.get_job(f"job-{index}").status  # type: ignore[union-attr]
        for index in range(3)
    )
    assert first_states.count(JOB_FAILED) == 2
    assert first_states.count(JOB_RUNNING) == 1

    second_pass = _claim(
        store,
        token="f" * 64,
        worker="reaper-2",
        now=LEASE_UNTIL + timedelta(microseconds=1),
        lease_until=LEASE_UNTIL + timedelta(seconds=30),
    )
    assert second_pass is None
    assert all(
        store.get_job(f"job-{index}").status == JOB_FAILED  # type: ignore[union-attr]
        for index in range(3)
    )


def test_expire_exhausted_job_uses_full_observed_lease_cas(store) -> None:
    _create(store, max_attempts=1)
    claimed = _claim(store)
    assert claimed is not None

    with pytest.raises(JobConflictError) as early:
        store.expire_exhausted_job(
            "job-1",
            lease_owner="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=LEASE_UNTIL - timedelta(microseconds=1),
        )
    assert early.value.current == claimed.job

    expired = store.expire_exhausted_job(
        "job-1",
        lease_owner="worker-1",
        lease_token="c" * 64,
        attempt_count=1,
        observed_lease_until=LEASE_UNTIL,
        now=LEASE_UNTIL,
    )
    assert expired.status == JOB_FAILED
    with pytest.raises(JobAlreadyTerminalError) as replay:
        store.expire_exhausted_job(
            "job-1",
            lease_owner="worker-1",
            lease_token="c" * 64,
            attempt_count=1,
            observed_lease_until=LEASE_UNTIL,
            now=LEASE_UNTIL + timedelta(seconds=1),
        )
    assert replay.value.current == expired


def test_two_workers_claim_one_due_job_exactly_once(store) -> None:
    _create(store)
    barrier = Barrier(2)

    def claim(worker: str, token: str):
        barrier.wait()
        return _claim(store, worker=worker, token=token)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(
            future.result()
            for future in (
                executor.submit(claim, "worker-1", "c" * 64),
                executor.submit(claim, "worker-2", "d" * 64),
            )
        )

    claimed = tuple(result for result in results if result is not None)
    assert len(claimed) == 1
    assert claimed[0].claimed is True
    assert store.get_job("job-1") == claimed[0].job
    assert claimed[0].job.attempt_count == 1


def test_concurrent_same_token_resolves_as_one_claim_and_one_replay(store) -> None:
    _create(store)
    barrier = Barrier(2)

    def claim():
        barrier.wait()
        return _claim(store, worker="worker-1", token="c" * 64)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(
            future.result()
            for future in (executor.submit(claim), executor.submit(claim))
        )

    assert all(result is not None for result in results)
    assert sum(bool(result and result.claimed) for result in results) == 1
    assert sum(bool(result and result.replayed) for result in results) == 1
    assert results[0].job == results[1].job  # type: ignore[union-attr]


def test_concurrent_complete_has_one_success_and_terminal_loser(store) -> None:
    _create(store)
    _claim(store)
    barrier = Barrier(2)

    def complete(run_uid: str):
        barrier.wait()
        try:
            return store.complete(
                "job-1",
                worker_id="worker-1",
                lease_token="c" * 64,
                attempt_count=1,
                observed_lease_until=LEASE_UNTIL,
                run_uid=run_uid,
                now=CLAIM_AT + timedelta(seconds=5),
            )
        except JobAlreadyTerminalError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(
            future.result()
            for future in (
                executor.submit(complete, "run-a"),
                executor.submit(complete, "run-b"),
            )
        )

    successes = tuple(item for item in results if isinstance(item, JobSnapshot))
    losers = tuple(
        item for item in results if isinstance(item, JobAlreadyTerminalError)
    )
    assert len(successes) == 1
    assert len(losers) == 1
    assert losers[0].current == successes[0]


def test_get_job_rejects_malformed_persisted_state(store) -> None:
    with store.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_job_run_v4 (
                    job_id, idempotency_key, job_type, scheduled_for,
                    status, attempt_count, max_attempts, lease_owner,
                    next_attempt_at, created_at, updated_at
                ) VALUES (
                    'bad', :key, 'TEST', :scheduled, 'PENDING',
                    0, 0, '', :scheduled, :created, :created
                )
                """
            ),
            {
                "key": "f" * 64,
                "scheduled": SCHEDULED_FOR.replace(tzinfo=None),
                "created": CREATED_AT.replace(tzinfo=None),
            },
        )
    with pytest.raises(JobStoreIntegrityError, match="max_attempts"):
        store.get_job("bad")
