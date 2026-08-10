"""Strict SQL persistence for the Stage-2 V4 job lease state machine.

The adapter is intentionally inert: it persists jobs and performs fenced CAS
transitions, but it does not run a worker or produce actionable output.  The
caller must generate a globally fresh, never-reused lowercase SHA-256 lease
token for every logical claim command.  Every committed claim appends that
token to the V4 claim-token registry in the same transaction as the job CAS.
The registry is never updated or deleted, so historical reuse remains
detectable after terminal transitions clear the live token.  A token that
still identifies the same live RUNNING lease can be resolved as an exact claim
replay; terminal retries are reported as terminal/conflict rather than
fabricated as idempotent success.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Iterator, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, OperationalError


JOB_PENDING = "PENDING"
JOB_RUNNING = "RUNNING"
JOB_SUCCEEDED = "SUCCEEDED"
JOB_FAILED = "FAILED"
JOB_CANCELLED = "CANCELLED"

JOB_STATUSES = frozenset(
    {
        JOB_PENDING,
        JOB_RUNNING,
        JOB_SUCCEEDED,
        JOB_FAILED,
        JOB_CANCELLED,
    }
)
TERMINAL_JOB_STATUSES = frozenset(
    {JOB_SUCCEEDED, JOB_FAILED, JOB_CANCELLED}
)
EXHAUSTED_LEASE_ERROR_CODE = "LEASE_EXPIRED_MAX_ATTEMPTS"
EXHAUSTED_LEASE_ERROR_MESSAGE = "lease expired after maximum attempts"
EXHAUSTED_REAP_BATCH_SIZE = 32
JOB_CALLER_CLOCK_MAX_SKEW_SECONDS = 5
JOB_LEASE_MAX_DURATION_SECONDS = 900
_JOB_STORE_TRANSACTION_MAX_ATTEMPTS = 4
_MYSQL_TRANSIENT_LOCK_CODES = frozenset({1205, 1213})
_TRANSIENT_LOCK_SQLSTATES = frozenset({"40001", "40P01"})

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SUPPORTED_DIALECTS = frozenset({"sqlite", "mysql", "mariadb"})

_JOB_COLUMNS = (
    "job_id",
    "idempotency_key",
    "job_type",
    "scheduled_for",
    "input_context_id",
    "input_hash",
    "run_uid",
    "status",
    "attempt_count",
    "max_attempts",
    "lease_owner",
    "lease_token",
    "lease_until",
    "next_attempt_at",
    "error_code",
    "error_message",
    "started_at",
    "completed_at",
    "created_at",
    "updated_at",
)
_JOB_SELECT = (
    "job_id, idempotency_key, job_type, scheduled_for, input_context_id, "
    "input_hash, run_uid, status, attempt_count, max_attempts, lease_owner, "
    "lease_token, lease_until, next_attempt_at, error_code, error_message, "
    "started_at, completed_at, created_at, updated_at"
)
_JOB_BY_ID_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 WHERE job_id = :job_id"
)
_JOB_BY_IDEMPOTENCY_KEY_SELECT = (
    "SELECT " + _JOB_SELECT
    + " FROM st_job_run_v4 WHERE idempotency_key = :idempotency_key"
)
_JOB_BY_LEASE_TOKEN_SELECT = (
    "SELECT " + _JOB_SELECT
    + " FROM st_job_run_v4 WHERE lease_token = :lease_token"
)
_CLAIM_TOKEN_COLUMNS = (
    "lease_token",
    "job_id",
    "attempt_count",
    "lease_owner",
    "claimed_at",
    "lease_until",
)
_CLAIM_TOKEN_SELECT = (
    "SELECT lease_token, job_id, attempt_count, lease_owner, claimed_at, "
    "lease_until FROM st_job_claim_token_v4 "
    "WHERE lease_token = :lease_token"
)
_EXHAUSTED_JOB_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 "
    "WHERE status = 'RUNNING' AND lease_until <= :now "
    "AND attempt_count >= max_attempts "
    "ORDER BY lease_until, scheduled_for, job_id LIMIT 1"
)
_EXHAUSTED_JOB_TYPE_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 "
    "WHERE status = 'RUNNING' AND lease_until <= :now "
    "AND attempt_count >= max_attempts AND job_type = :job_type "
    "ORDER BY lease_until, scheduled_for, job_id LIMIT 1"
)
_MYSQL_EXHAUSTED_JOB_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 "
    "WHERE status = 'RUNNING' AND lease_until <= :now "
    "AND lease_until <= UTC_TIMESTAMP(6) "
    "AND attempt_count >= max_attempts "
    "ORDER BY lease_until, scheduled_for, job_id LIMIT 1"
)
_MYSQL_EXHAUSTED_JOB_TYPE_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 "
    "WHERE status = 'RUNNING' AND lease_until <= :now "
    "AND lease_until <= UTC_TIMESTAMP(6) "
    "AND attempt_count >= max_attempts AND job_type = :job_type "
    "ORDER BY lease_until, scheduled_for, job_id LIMIT 1"
)
_DUE_JOB_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 WHERE ("
    "(status = 'PENDING' "
    "AND COALESCE(next_attempt_at, scheduled_for) <= :now "
    "AND attempt_count < max_attempts) OR "
    "(status = 'RUNNING' AND lease_until <= :now "
    "AND attempt_count < max_attempts)) "
    "ORDER BY CASE WHEN status = 'PENDING' "
    "THEN COALESCE(next_attempt_at, scheduled_for) "
    "ELSE lease_until END, scheduled_for, job_id LIMIT 1"
)
_DUE_JOB_TYPE_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 WHERE ("
    "(status = 'PENDING' "
    "AND COALESCE(next_attempt_at, scheduled_for) <= :now "
    "AND attempt_count < max_attempts) OR "
    "(status = 'RUNNING' AND lease_until <= :now "
    "AND attempt_count < max_attempts)) "
    "AND job_type = :job_type "
    "ORDER BY CASE WHEN status = 'PENDING' "
    "THEN COALESCE(next_attempt_at, scheduled_for) "
    "ELSE lease_until END, scheduled_for, job_id LIMIT 1"
)
_MYSQL_DUE_JOB_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 WHERE ("
    "(status = 'PENDING' "
    "AND COALESCE(next_attempt_at, scheduled_for) <= :now "
    "AND COALESCE(next_attempt_at, scheduled_for) <= UTC_TIMESTAMP(6) "
    "AND attempt_count < max_attempts) OR "
    "(status = 'RUNNING' AND lease_until <= :now "
    "AND lease_until <= UTC_TIMESTAMP(6) "
    "AND attempt_count < max_attempts)) "
    "ORDER BY CASE WHEN status = 'PENDING' "
    "THEN COALESCE(next_attempt_at, scheduled_for) "
    "ELSE lease_until END, scheduled_for, job_id LIMIT 1"
)
_MYSQL_DUE_JOB_TYPE_SELECT = (
    "SELECT " + _JOB_SELECT + " FROM st_job_run_v4 WHERE ("
    "(status = 'PENDING' "
    "AND COALESCE(next_attempt_at, scheduled_for) <= :now "
    "AND COALESCE(next_attempt_at, scheduled_for) <= UTC_TIMESTAMP(6) "
    "AND attempt_count < max_attempts) OR "
    "(status = 'RUNNING' AND lease_until <= :now "
    "AND lease_until <= UTC_TIMESTAMP(6) "
    "AND attempt_count < max_attempts)) "
    "AND job_type = :job_type "
    "ORDER BY CASE WHEN status = 'PENDING' "
    "THEN COALESCE(next_attempt_at, scheduled_for) "
    "ELSE lease_until END, scheduled_for, job_id LIMIT 1"
)


class JobStoreError(RuntimeError):
    """Base error for strict V4 job persistence."""


class JobStoreIntegrityError(JobStoreError):
    """A persisted row or exact read-back violated the frozen contract."""


class JobNotFoundError(JobStoreError):
    """The requested job does not exist."""


class JobConflictError(JobStoreError):
    """The supplied CAS no longer identifies the current job state."""

    def __init__(
        self,
        message: str,
        *,
        current: JobSnapshot | None,
    ) -> None:
        super().__init__(message)
        self.current = current


class JobAlreadyTerminalError(JobConflictError):
    """A token-clearing terminal job cannot be presented as an exact replay."""


class JobClockSkewError(JobStoreError):
    """The caller clock is too far from the authoritative MySQL UTC clock."""


@dataclass(frozen=True, slots=True)
class JobSnapshot:
    job_id: str
    idempotency_key: str
    job_type: str
    scheduled_for: datetime
    input_context_id: str
    input_hash: str
    run_uid: str
    status: str
    attempt_count: int
    max_attempts: int
    lease_owner: str
    lease_token: str | None
    lease_until: datetime | None
    next_attempt_at: datetime | None
    error_code: str | None
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class CreateJobResult:
    created: bool
    job: JobSnapshot


@dataclass(frozen=True, slots=True)
class ClaimJobResult:
    claimed: bool
    replayed: bool
    job: JobSnapshot


@dataclass(frozen=True, slots=True)
class _ClaimTokenRecord:
    lease_token: str
    job_id: str
    attempt_count: int
    lease_owner: str
    claimed_at: datetime
    lease_until: datetime


def _exact_text(
    value: object,
    *,
    field: str,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if type(value) is not str or value != value.strip():
        raise TypeError(f"{field} must be exact text without surrounding whitespace")
    if not value and not allow_empty:
        raise ValueError(f"{field} must not be empty")
    if len(value) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    return value


def _optional_text(
    value: object,
    *,
    field: str,
    maximum: int,
    allow_empty: bool = False,
) -> str | None:
    if value is None:
        return None
    return _exact_text(
        value,
        field=field,
        maximum=maximum,
        allow_empty=allow_empty,
    )


def _sha256(value: object, *, field: str) -> str:
    candidate = _exact_text(value, field=field, maximum=64)
    if _SHA256_RE.fullmatch(candidate) is None:
        raise ValueError(f"{field} must be lowercase 64-character SHA-256 hex")
    return candidate


def _optional_sha256(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, field=field)


def _input_hash(value: object) -> str:
    candidate = _exact_text(
        value,
        field="input_hash",
        maximum=64,
        allow_empty=True,
    )
    if candidate and _SHA256_RE.fullmatch(candidate) is None:
        raise ValueError("input_hash must be empty or lowercase SHA-256 hex")
    return candidate


def _positive_int(value: object, *, field: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be an exact positive int")
    return value


def _nonnegative_int(value: object, *, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be an exact non-negative int")
    return value


def _utc_datetime(value: object, *, field: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _db_datetime(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _assert_lease_duration(now: datetime, lease_until: datetime) -> None:
    if lease_until > now + timedelta(seconds=JOB_LEASE_MAX_DURATION_SECONDS):
        raise ValueError(
            "lease_until exceeds the maximum single-lease duration of "
            f"{JOB_LEASE_MAX_DURATION_SECONDS} seconds"
        )


def _stored_datetime(
    value: object,
    *,
    field: str,
    nullable: bool = False,
) -> datetime | None:
    if value is None:
        if nullable:
            return None
        raise JobStoreIntegrityError(f"{field} must not be NULL")
    parsed: datetime
    if type(value) is datetime:
        parsed = value
    elif type(value) is str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise JobStoreIntegrityError(
                f"{field} is not an ISO database timestamp"
            ) from exc
    else:
        raise JobStoreIntegrityError(f"{field} has an invalid timestamp type")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _stored_text(
    value: object,
    *,
    field: str,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    try:
        return _exact_text(
            value,
            field=field,
            maximum=maximum,
            allow_empty=allow_empty,
        )
    except (TypeError, ValueError) as exc:
        raise JobStoreIntegrityError(str(exc)) from exc


def _stored_optional_text(
    value: object,
    *,
    field: str,
    maximum: int,
    allow_empty: bool = False,
) -> str | None:
    if value is None:
        return None
    return _stored_text(
        value,
        field=field,
        maximum=maximum,
        allow_empty=allow_empty,
    )


def _stored_sha256(value: object, *, field: str) -> str:
    try:
        return _sha256(value, field=field)
    except (TypeError, ValueError) as exc:
        raise JobStoreIntegrityError(str(exc)) from exc


def _stored_optional_sha256(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    return _stored_sha256(value, field=field)


def _stored_int(value: object, *, field: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise JobStoreIntegrityError(
            f"{field} must be an exact int >= {minimum}"
        )
    return value


def _job_snapshot(row: Mapping[str, Any]) -> JobSnapshot:
    if not isinstance(row, Mapping) or set(row) != set(_JOB_COLUMNS):
        raise JobStoreIntegrityError("job read-back columns are not exact")
    status = _stored_text(row["status"], field="status", maximum=24)
    if status not in JOB_STATUSES:
        raise JobStoreIntegrityError(f"unsupported persisted job status: {status}")
    attempt_count = _stored_int(row["attempt_count"], field="attempt_count")
    max_attempts = _stored_int(
        row["max_attempts"],
        field="max_attempts",
        minimum=1,
    )
    if attempt_count > max_attempts:
        raise JobStoreIntegrityError("attempt_count exceeds max_attempts")
    try:
        input_hash = _input_hash(row["input_hash"])
    except (TypeError, ValueError) as exc:
        raise JobStoreIntegrityError(str(exc)) from exc
    lease_owner = _stored_text(
        row["lease_owner"],
        field="lease_owner",
        maximum=160,
        allow_empty=True,
    )
    lease_token = _stored_optional_sha256(
        row["lease_token"],
        field="lease_token",
    )
    lease_until = _stored_datetime(
        row["lease_until"],
        field="lease_until",
        nullable=True,
    )
    next_attempt_at = _stored_datetime(
        row["next_attempt_at"],
        field="next_attempt_at",
        nullable=True,
    )
    started_at = _stored_datetime(
        row["started_at"],
        field="started_at",
        nullable=True,
    )
    completed_at = _stored_datetime(
        row["completed_at"],
        field="completed_at",
        nullable=True,
    )
    created_at = _stored_datetime(row["created_at"], field="created_at")
    updated_at = _stored_datetime(row["updated_at"], field="updated_at")
    assert created_at is not None and updated_at is not None
    snapshot = JobSnapshot(
        job_id=_stored_text(row["job_id"], field="job_id", maximum=64),
        idempotency_key=_stored_sha256(
            row["idempotency_key"], field="idempotency_key"
        ),
        job_type=_stored_text(row["job_type"], field="job_type", maximum=80),
        scheduled_for=_stored_datetime(
            row["scheduled_for"], field="scheduled_for"
        ),
        input_context_id=_stored_text(
            row["input_context_id"],
            field="input_context_id",
            maximum=64,
            allow_empty=True,
        ),
        input_hash=input_hash,
        run_uid=_stored_text(
            row["run_uid"],
            field="run_uid",
            maximum=64,
            allow_empty=True,
        ),
        status=status,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        lease_owner=lease_owner,
        lease_token=lease_token,
        lease_until=lease_until,
        next_attempt_at=next_attempt_at,
        error_code=_stored_optional_text(
            row["error_code"], field="error_code", maximum=100
        ),
        error_message=_stored_optional_text(
            row["error_message"],
            field="error_message",
            maximum=1000,
            allow_empty=True,
        ),
        started_at=started_at,
        completed_at=completed_at,
        created_at=created_at,
        updated_at=updated_at,
    )
    if snapshot.updated_at < snapshot.created_at:
        raise JobStoreIntegrityError("updated_at precedes created_at")
    if snapshot.started_at is not None and not (
        snapshot.created_at <= snapshot.started_at <= snapshot.updated_at
    ):
        raise JobStoreIntegrityError("started_at violates job chronology")
    if snapshot.completed_at is not None and not (
        snapshot.created_at <= snapshot.completed_at <= snapshot.updated_at
    ):
        raise JobStoreIntegrityError("completed_at violates job chronology")
    if snapshot.error_message is not None and snapshot.error_code is None:
        raise JobStoreIntegrityError("error_message requires error_code")
    if snapshot.attempt_count == 0 and snapshot.started_at is not None:
        raise JobStoreIntegrityError("an unattempted job cannot have started_at")
    if snapshot.attempt_count > 0 and snapshot.started_at is None:
        raise JobStoreIntegrityError("an attempted job requires started_at")
    if snapshot.status == JOB_RUNNING:
        if (
            not snapshot.lease_owner
            or snapshot.lease_token is None
            or snapshot.lease_until is None
            or snapshot.lease_until <= snapshot.updated_at
            or snapshot.attempt_count < 1
            or snapshot.started_at is None
            or snapshot.completed_at is not None
            or snapshot.next_attempt_at is not None
            or snapshot.run_uid
            or snapshot.error_code is not None
            or snapshot.error_message is not None
        ):
            raise JobStoreIntegrityError("RUNNING job state is internally inconsistent")
    else:
        if (
            snapshot.lease_owner != ""
            or snapshot.lease_token is not None
            or snapshot.lease_until is not None
        ):
            raise JobStoreIntegrityError("non-RUNNING job retains lease state")
    if snapshot.status == JOB_PENDING:
        if (
            snapshot.completed_at is not None
            or snapshot.next_attempt_at is None
            or snapshot.run_uid
        ):
            raise JobStoreIntegrityError("PENDING job state is internally inconsistent")
        if snapshot.attempt_count == 0:
            if (
                snapshot.next_attempt_at != snapshot.scheduled_for
                or snapshot.error_code is not None
                or snapshot.error_message is not None
                or snapshot.started_at is not None
                or snapshot.updated_at != snapshot.created_at
            ):
                raise JobStoreIntegrityError(
                    "initial PENDING job state is internally inconsistent"
                )
        elif (
            snapshot.attempt_count >= snapshot.max_attempts
            or snapshot.started_at is None
            or snapshot.error_code is None
            or snapshot.next_attempt_at <= snapshot.updated_at
        ):
            raise JobStoreIntegrityError(
                "retry PENDING job state is internally inconsistent"
            )
    elif snapshot.status in TERMINAL_JOB_STATUSES:
        if snapshot.completed_at is None or snapshot.next_attempt_at is not None:
            raise JobStoreIntegrityError("terminal job state is internally inconsistent")
        if snapshot.completed_at != snapshot.updated_at:
            raise JobStoreIntegrityError("terminal completion and update times differ")
        if snapshot.status == JOB_SUCCEEDED:
            if (
                snapshot.attempt_count < 1
                or not snapshot.run_uid
                or snapshot.started_at is None
                or snapshot.error_code is not None
                or snapshot.error_message is not None
            ):
                raise JobStoreIntegrityError("SUCCEEDED job result is inconsistent")
        elif snapshot.run_uid:
            raise JobStoreIntegrityError("non-success terminal job carries run_uid")
        if snapshot.status == JOB_FAILED and snapshot.error_code is None:
            raise JobStoreIntegrityError("FAILED job requires error_code")
    return snapshot


def _claim_token_record(row: Mapping[str, Any]) -> _ClaimTokenRecord:
    if not isinstance(row, Mapping) or set(row) != set(_CLAIM_TOKEN_COLUMNS):
        raise JobStoreIntegrityError(
            "claim token registry read-back columns are not exact"
        )
    claimed_at = _stored_datetime(row["claimed_at"], field="claimed_at")
    lease_until = _stored_datetime(row["lease_until"], field="lease_until")
    assert claimed_at is not None and lease_until is not None
    record = _ClaimTokenRecord(
        lease_token=_stored_sha256(
            row["lease_token"],
            field="lease_token",
        ),
        job_id=_stored_text(row["job_id"], field="job_id", maximum=64),
        attempt_count=_stored_int(
            row["attempt_count"],
            field="attempt_count",
            minimum=1,
        ),
        lease_owner=_stored_text(
            row["lease_owner"],
            field="lease_owner",
            maximum=160,
        ),
        claimed_at=claimed_at,
        lease_until=lease_until,
    )
    if record.lease_until <= record.claimed_at:
        raise JobStoreIntegrityError(
            "claim token registry lease must end after claimed_at"
        )
    if record.lease_until > record.claimed_at + timedelta(
        seconds=JOB_LEASE_MAX_DURATION_SECONDS
    ):
        raise JobStoreIntegrityError(
            "claim token registry lease exceeds maximum duration"
        )
    return record


@contextmanager
def _write_connection(engine: Engine) -> Iterator[Connection]:
    """Use BEGIN IMMEDIATE on SQLite so thread tests exercise one writer."""

    with engine.connect() as connection:
        if connection.dialect.name.lower() == "sqlite":
            connection.execute(text("BEGIN IMMEDIATE"))
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
            return
        with connection.begin():
            yield connection


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    row = result.mappings().first()
    return None if row is None else dict(row)


def _changed_exactly_one_row(result: Any) -> bool:
    rowcount = getattr(result, "rowcount", None)
    return type(rowcount) is int and rowcount == 1


def _is_transient_transaction_error(error: OperationalError) -> bool:
    """Recognize only explicit deadlock/serialization codes for retry."""

    if not isinstance(error, OperationalError):
        return False
    original = getattr(error, "orig", None)
    candidates: tuple[object, ...] = (
        getattr(original, "errno", None),
        getattr(original, "sqlstate", None),
        getattr(original, "pgcode", None),
    )
    args = getattr(original, "args", ())
    if isinstance(args, tuple):
        candidates += args[:2]
    for value in candidates:
        if type(value) is int and value in _MYSQL_TRANSIENT_LOCK_CODES:
            return True
        normalized = str(value or "").strip().upper()
        if normalized in _TRANSIENT_LOCK_SQLSTATES:
            return True
        if normalized.isdigit() and int(normalized) in _MYSQL_TRANSIENT_LOCK_CODES:
            return True
    return False


def _assert_database_clock(
    connection: Connection,
    caller_now: datetime,
) -> datetime:
    """Bind production lease commands to the authoritative MySQL UTC clock.

    SQLite is used only by the deterministic concurrency tests in this module;
    production MySQL additionally has UTC predicates on every lease mutation.
    """

    if connection.dialect.name.lower() == "sqlite":
        return caller_now
    row = _first_mapping(
        connection.execute(
            text("SELECT UTC_TIMESTAMP(6) AS database_now")
        )
    )
    if row is None or set(row) != {"database_now"}:
        raise JobStoreIntegrityError("database UTC clock read-back is not exact")
    database_now = _stored_datetime(row["database_now"], field="database_now")
    assert database_now is not None
    skew_seconds = abs((caller_now - database_now).total_seconds())
    if skew_seconds > JOB_CALLER_CLOCK_MAX_SKEW_SECONDS:
        raise JobClockSkewError(
            "caller now differs from the MySQL UTC clock by more than "
            f"{JOB_CALLER_CLOCK_MAX_SKEW_SECONDS} seconds"
        )
    return database_now


def _lease_is_live_at_database(
    connection: Connection,
    current: JobSnapshot,
) -> bool:
    if current.status != JOB_RUNNING or current.lease_until is None:
        return False
    if connection.dialect.name.lower() == "sqlite":
        return True
    # Keep this as a locking/current read.  claim_due_job may have established
    # an older REPEATABLE READ snapshot with its initial non-locking token miss;
    # a plain SELECT here could therefore hide the concurrent winning lease
    # even after resolve_token locked and observed that exact job generation.
    row = _first_mapping(
        connection.execute(
            text(
                "SELECT job_id FROM st_job_run_v4 "
                "WHERE job_id = :job_id AND status = 'RUNNING' "
                "AND lease_token = :lease_token "
                "AND lease_until > UTC_TIMESTAMP(6) FOR UPDATE"
            ),
            {
                "job_id": current.job_id,
                "lease_token": current.lease_token,
            },
        )
    )
    return row == {"job_id": current.job_id}


def _job_by_id(
    connection: Connection,
    job_id: str,
    *,
    for_update: bool,
) -> JobSnapshot | None:
    lock_suffix = (
        " FOR UPDATE"
        if for_update and connection.dialect.name.lower() != "sqlite"
        else ""
    )
    row = _first_mapping(
        connection.execute(
            text(
                _JOB_BY_ID_SELECT + lock_suffix
            ),
            {"job_id": job_id},
        )
    )
    return None if row is None else _job_snapshot(row)


def _job_by_idempotency_key(
    connection: Connection,
    idempotency_key: str,
    *,
    for_update: bool,
) -> JobSnapshot | None:
    lock_suffix = (
        " FOR UPDATE"
        if for_update and connection.dialect.name.lower() != "sqlite"
        else ""
    )
    row = _first_mapping(
        connection.execute(
            text(
                _JOB_BY_IDEMPOTENCY_KEY_SELECT + lock_suffix
            ),
            {"idempotency_key": idempotency_key},
        )
    )
    return None if row is None else _job_snapshot(row)


def _job_by_lease_token(
    connection: Connection,
    lease_token: str,
    *,
    for_update: bool,
) -> JobSnapshot | None:
    lock_suffix = (
        " FOR UPDATE"
        if for_update and connection.dialect.name.lower() != "sqlite"
        else ""
    )
    row = _first_mapping(
        connection.execute(
            text(
                _JOB_BY_LEASE_TOKEN_SELECT + lock_suffix
            ),
            {"lease_token": lease_token},
        )
    )
    return None if row is None else _job_snapshot(row)


def _claim_token_by_token(
    connection: Connection,
    lease_token: str,
    *,
    for_update: bool,
) -> _ClaimTokenRecord | None:
    lock_suffix = (
        " FOR UPDATE"
        if for_update and connection.dialect.name.lower() != "sqlite"
        else ""
    )
    row = _first_mapping(
        connection.execute(
            text(_CLAIM_TOKEN_SELECT + lock_suffix),
            {"lease_token": lease_token},
        )
    )
    return None if row is None else _claim_token_record(row)


def _insert_claim_token_record(
    connection: Connection,
    *,
    lease_token: str,
    job_id: str,
    attempt_count: int,
    lease_owner: str,
    claimed_at: datetime,
    lease_until: datetime,
) -> _ClaimTokenRecord:
    expected = _ClaimTokenRecord(
        lease_token=lease_token,
        job_id=job_id,
        attempt_count=attempt_count,
        lease_owner=lease_owner,
        claimed_at=claimed_at,
        lease_until=lease_until,
    )
    result = connection.execute(
        text(
            "INSERT INTO st_job_claim_token_v4 ("
            "lease_token, job_id, attempt_count, lease_owner, claimed_at, "
            "lease_until) VALUES ("
            ":lease_token, :job_id, :attempt_count, :lease_owner, "
            ":claimed_at, :lease_until)"
        ),
        {
            "lease_token": lease_token,
            "job_id": job_id,
            "attempt_count": attempt_count,
            "lease_owner": lease_owner,
            "claimed_at": _db_datetime(claimed_at),
            "lease_until": _db_datetime(lease_until),
        },
    )
    if not _changed_exactly_one_row(result):
        raise JobStoreIntegrityError(
            "claim token registry inserted no exact row"
        )
    stored = _claim_token_by_token(
        connection,
        lease_token,
        for_update=False,
    )
    if stored != expected:
        raise JobStoreIntegrityError(
            "claim token registry did not produce exact persisted read-back"
        )
    return stored


def _exact_readback(
    actual: JobSnapshot | None,
    expected: JobSnapshot,
    *,
    operation: str,
) -> JobSnapshot:
    if actual is None or actual != expected:
        raise JobStoreIntegrityError(
            f"{operation} did not produce an exact persisted read-back"
        )
    return actual


def _raise_state_conflict(
    job_id: str,
    current: JobSnapshot | None,
    *,
    operation: str,
) -> None:
    if current is None:
        raise JobNotFoundError(f"V4 job not found: {job_id}")
    if current.status in TERMINAL_JOB_STATUSES:
        raise JobAlreadyTerminalError(
            f"{operation} cannot replay terminal job {job_id}: {current.status}",
            current=current,
        )
    raise JobConflictError(
        f"{operation} CAS no longer matches job {job_id}: {current.status}",
        current=current,
    )


def _lease_matches(
    current: JobSnapshot,
    *,
    worker_id: str,
    lease_token: str,
    attempt_count: int,
    observed_lease_until: datetime,
) -> bool:
    return (
        current.status == JOB_RUNNING
        and current.lease_owner == worker_id
        and current.lease_token == lease_token
        and current.attempt_count == attempt_count
        and current.lease_until == observed_lease_until
    )


class JobStoreRepository:
    """Own only V4 job scheduling and lease CAS state."""

    def __init__(self, engine: Engine) -> None:
        if not isinstance(engine, Engine):
            raise TypeError("engine must be a SQLAlchemy Engine")
        dialect = engine.dialect.name.lower()
        if dialect not in _SUPPORTED_DIALECTS:
            raise ValueError(
                "job store supports only sqlite, mysql, or mariadb dialects"
            )
        self.engine = engine

    @staticmethod
    def _assert_create_replay(
        current: JobSnapshot,
        *,
        job_id: str,
        idempotency_key: str,
        job_type: str,
        scheduled_for: datetime,
        input_context_id: str,
        input_hash: str,
        max_attempts: int,
        created_at: datetime,
    ) -> None:
        identity = (
            current.job_id,
            current.idempotency_key,
            current.job_type,
            current.scheduled_for,
            current.input_context_id,
            current.input_hash,
            current.max_attempts,
            current.created_at,
        )
        expected = (
            job_id,
            idempotency_key,
            job_type,
            scheduled_for,
            input_context_id,
            input_hash,
            max_attempts,
            created_at,
        )
        if identity != expected:
            raise JobConflictError(
                "job idempotency key was reused with different immutable input",
                current=current,
            )

    def create_job(
        self,
        *,
        job_id: str,
        idempotency_key: str,
        job_type: str,
        scheduled_for: datetime,
        max_attempts: int,
        created_at: datetime,
        input_context_id: str = "",
        input_hash: str = "",
    ) -> CreateJobResult:
        normalized_job_id = _exact_text(job_id, field="job_id", maximum=64)
        normalized_key = _sha256(idempotency_key, field="idempotency_key")
        normalized_type = _exact_text(job_type, field="job_type", maximum=80)
        scheduled = _utc_datetime(scheduled_for, field="scheduled_for")
        created = _utc_datetime(created_at, field="created_at")
        attempts = _positive_int(max_attempts, field="max_attempts")
        context_id = _exact_text(
            input_context_id,
            field="input_context_id",
            maximum=64,
            allow_empty=True,
        )
        normalized_input_hash = _input_hash(input_hash)

        def existing_result(current: JobSnapshot) -> CreateJobResult:
            self._assert_create_replay(
                current,
                job_id=normalized_job_id,
                idempotency_key=normalized_key,
                job_type=normalized_type,
                scheduled_for=scheduled,
                input_context_id=context_id,
                input_hash=normalized_input_hash,
                max_attempts=attempts,
                created_at=created,
            )
            return CreateJobResult(created=False, job=current)

        def create_once() -> CreateJobResult:
            try:
                with _write_connection(self.engine) as connection:
                    current = _job_by_idempotency_key(
                        connection,
                        normalized_key,
                        for_update=False,
                    )
                    if current is not None:
                        return existing_result(current)
                    _assert_database_clock(connection, created)
                    result = connection.execute(
                        text(
                            """
                            INSERT INTO st_job_run_v4 (
                                job_id, idempotency_key, job_type, scheduled_for,
                                input_context_id, input_hash, run_uid, status,
                                attempt_count, max_attempts, lease_owner,
                                lease_token, lease_until, next_attempt_at,
                                error_code, error_message, started_at,
                                completed_at, created_at, updated_at
                            ) VALUES (
                                :job_id, :idempotency_key, :job_type,
                                :scheduled_for, :input_context_id, :input_hash,
                                '', 'PENDING', 0, :max_attempts, '', NULL, NULL,
                                :next_attempt_at, NULL, NULL, NULL, NULL,
                                :created_at, :updated_at
                            )
                            """
                        ),
                        {
                            "job_id": normalized_job_id,
                            "idempotency_key": normalized_key,
                            "job_type": normalized_type,
                            "scheduled_for": _db_datetime(scheduled),
                            "input_context_id": context_id,
                            "input_hash": normalized_input_hash,
                            "max_attempts": attempts,
                            "next_attempt_at": _db_datetime(scheduled),
                            "created_at": _db_datetime(created),
                            "updated_at": _db_datetime(created),
                        },
                    )
                    if not _changed_exactly_one_row(result):
                        raise JobStoreIntegrityError(
                            "create_job inserted no exact row"
                        )
                    expected = JobSnapshot(
                        job_id=normalized_job_id,
                        idempotency_key=normalized_key,
                        job_type=normalized_type,
                        scheduled_for=scheduled,
                        input_context_id=context_id,
                        input_hash=normalized_input_hash,
                        run_uid="",
                        status=JOB_PENDING,
                        attempt_count=0,
                        max_attempts=attempts,
                        lease_owner="",
                        lease_token=None,
                        lease_until=None,
                        next_attempt_at=scheduled,
                        error_code=None,
                        error_message=None,
                        started_at=None,
                        completed_at=None,
                        created_at=created,
                        updated_at=created,
                    )
                    stored = _exact_readback(
                        _job_by_id(
                            connection,
                            normalized_job_id,
                            for_update=False,
                        ),
                        expected,
                        operation="create_job",
                    )
                    return CreateJobResult(created=True, job=stored)
            except IntegrityError as exc:
                with _write_connection(self.engine) as connection:
                    current = _job_by_idempotency_key(
                        connection,
                        normalized_key,
                        for_update=False,
                    )
                    if current is not None:
                        return existing_result(current)
                    collision = _job_by_id(
                        connection,
                        normalized_job_id,
                        for_update=False,
                    )
                raise JobConflictError(
                    "job identity collided with a different create command",
                    current=collision,
                ) from exc

        for transaction_attempt in range(_JOB_STORE_TRANSACTION_MAX_ATTEMPTS):
            try:
                return create_once()
            except OperationalError as exc:
                if not _is_transient_transaction_error(exc):
                    raise
                if transaction_attempt + 1 >= _JOB_STORE_TRANSACTION_MAX_ATTEMPTS:
                    raise JobConflictError(
                        "create_job transient transaction retry exhausted",
                        current=None,
                    ) from exc
        raise JobStoreIntegrityError("create_job retry state exhausted unexpectedly")

    def get_job(self, job_id: str) -> JobSnapshot | None:
        normalized_job_id = _exact_text(job_id, field="job_id", maximum=64)
        with self.engine.connect() as connection:
            return _job_by_id(connection, normalized_job_id, for_update=False)

    @staticmethod
    def _expire_locked(
        connection: Connection,
        current: JobSnapshot,
        *,
        now: datetime,
    ) -> JobSnapshot:
        if (
            current.status != JOB_RUNNING
            or current.lease_token is None
            or current.lease_until is None
            or current.lease_until > now
            or current.attempt_count < current.max_attempts
        ):
            raise JobConflictError(
                "job is not an expired exhausted RUNNING lease",
                current=current,
            )
        if now <= current.updated_at:
            raise ValueError(
                "expiration now must be strictly later than current updated_at"
            )
        expected = replace(
            current,
            status=JOB_FAILED,
            lease_owner="",
            lease_token=None,
            lease_until=None,
            next_attempt_at=None,
            error_code=EXHAUSTED_LEASE_ERROR_CODE,
            error_message=EXHAUSTED_LEASE_ERROR_MESSAGE,
            completed_at=now,
            updated_at=now,
        )
        if connection.dialect.name.lower() == "sqlite":
            statement = text(
                """
                UPDATE st_job_run_v4
                SET status = 'FAILED', lease_owner = '', lease_token = NULL,
                    lease_until = NULL, next_attempt_at = NULL,
                    error_code = :error_code, error_message = :error_message,
                    completed_at = :now, updated_at = :now
                WHERE job_id = :job_id AND status = 'RUNNING'
                  AND lease_owner = :lease_owner
                  AND lease_token = :lease_token
                  AND attempt_count = :attempt_count
                  AND max_attempts = :max_attempts
                  AND lease_until = :observed_lease_until
                  AND lease_until <= :now
                  AND updated_at < :now
                  AND attempt_count >= max_attempts
                """
            )
        else:
            statement = text(
                """
                UPDATE st_job_run_v4
                SET status = 'FAILED', lease_owner = '', lease_token = NULL,
                    lease_until = NULL, next_attempt_at = NULL,
                    error_code = :error_code, error_message = :error_message,
                    completed_at = :now, updated_at = :now
                WHERE job_id = :job_id AND status = 'RUNNING'
                  AND lease_owner = :lease_owner
                  AND lease_token = :lease_token
                  AND attempt_count = :attempt_count
                  AND max_attempts = :max_attempts
                  AND lease_until = :observed_lease_until
                  AND lease_until <= :now
                  AND lease_until <= UTC_TIMESTAMP(6)
                  AND updated_at < :now
                  AND attempt_count >= max_attempts
                """
            )
        result = connection.execute(
            statement,
            {
                "job_id": current.job_id,
                "lease_owner": current.lease_owner,
                "lease_token": current.lease_token,
                "attempt_count": current.attempt_count,
                "max_attempts": current.max_attempts,
                "observed_lease_until": _db_datetime(current.lease_until),
                "now": _db_datetime(now),
                "error_code": EXHAUSTED_LEASE_ERROR_CODE,
                "error_message": EXHAUSTED_LEASE_ERROR_MESSAGE,
            },
        )
        if not _changed_exactly_one_row(result):
            latest = _job_by_id(connection, current.job_id, for_update=False)
            _raise_state_conflict(
                current.job_id,
                latest,
                operation="expire_exhausted_job",
            )
        return _exact_readback(
            _job_by_id(connection, current.job_id, for_update=False),
            expected,
            operation="expire_exhausted_job",
        )

    @staticmethod
    def _reap_exhausted_locked(
        connection: Connection,
        *,
        now: datetime,
        job_type: str | None,
    ) -> tuple[JobSnapshot, ...]:
        expired: list[JobSnapshot] = []
        previous_job_id: str | None = None
        lock_suffix = (
            "" if connection.dialect.name.lower() == "sqlite" else " FOR UPDATE"
        )
        for _ in range(EXHAUSTED_REAP_BATCH_SIZE):
            params: dict[str, Any] = {"now": _db_datetime(now)}
            if job_type is not None:
                params["job_type"] = job_type
            if connection.dialect.name.lower() == "sqlite":
                if job_type is None:
                    statement = text(_EXHAUSTED_JOB_SELECT + lock_suffix)
                else:
                    statement = text(_EXHAUSTED_JOB_TYPE_SELECT + lock_suffix)
            elif job_type is None:
                statement = text(_MYSQL_EXHAUSTED_JOB_SELECT + lock_suffix)
            else:
                statement = text(_MYSQL_EXHAUSTED_JOB_TYPE_SELECT + lock_suffix)
            row = _first_mapping(
                connection.execute(statement, params)
            )
            if row is None:
                return tuple(expired)
            current = _job_snapshot(row)
            if current.job_id == previous_job_id:
                raise JobStoreIntegrityError(
                    "exhausted lease reaper made no forward progress"
                )
            expired.append(
                JobStoreRepository._expire_locked(
                    connection,
                    current,
                    now=now,
                )
            )
            previous_job_id = current.job_id
        return tuple(expired)

    @staticmethod
    def _due_candidate_locked(
        connection: Connection,
        *,
        now: datetime,
        job_type: str | None,
    ) -> JobSnapshot | None:
        params: dict[str, Any] = {"now": _db_datetime(now)}
        if job_type is not None:
            params["job_type"] = job_type
        lock_suffix = (
            "" if connection.dialect.name.lower() == "sqlite" else " FOR UPDATE"
        )
        if connection.dialect.name.lower() == "sqlite":
            if job_type is None:
                statement = text(_DUE_JOB_SELECT + lock_suffix)
            else:
                statement = text(_DUE_JOB_TYPE_SELECT + lock_suffix)
        elif job_type is None:
            statement = text(_MYSQL_DUE_JOB_SELECT + lock_suffix)
        else:
            statement = text(_MYSQL_DUE_JOB_TYPE_SELECT + lock_suffix)
        row = _first_mapping(
            connection.execute(statement, params)
        )
        return None if row is None else _job_snapshot(row)

    @staticmethod
    def _claim_locked(
        connection: Connection,
        current: JobSnapshot,
        *,
        worker_id: str,
        lease_token: str,
        now: datetime,
        lease_until: datetime,
    ) -> JobSnapshot:
        if now <= current.updated_at:
            raise ValueError("claim now must be strictly later than current updated_at")
        next_attempt = current.attempt_count + 1
        if next_attempt > current.max_attempts:
            raise JobStoreIntegrityError("claim would exceed persisted max_attempts")
        started_at = current.started_at or now
        expected = replace(
            current,
            status=JOB_RUNNING,
            attempt_count=next_attempt,
            lease_owner=worker_id,
            lease_token=lease_token,
            lease_until=lease_until,
            next_attempt_at=None,
            error_code=None,
            error_message=None,
            started_at=started_at,
            completed_at=None,
            updated_at=now,
        )
        common = {
            "job_id": current.job_id,
            "old_attempt_count": current.attempt_count,
            "max_attempts": current.max_attempts,
            "next_attempt_count": next_attempt,
            "worker_id": worker_id,
            "lease_token": lease_token,
            "lease_until": _db_datetime(lease_until),
            "now": _db_datetime(now),
            "started_at": _db_datetime(started_at),
        }
        if current.status == JOB_PENDING:
            if connection.dialect.name.lower() == "sqlite":
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET status = 'RUNNING', attempt_count = :next_attempt_count,
                        lease_owner = :worker_id, lease_token = :lease_token,
                        lease_until = :lease_until, next_attempt_at = NULL,
                        error_code = NULL, error_message = NULL,
                        started_at = :started_at, completed_at = NULL,
                        updated_at = :now
                    WHERE job_id = :job_id AND status = 'PENDING'
                      AND attempt_count = :old_attempt_count
                      AND max_attempts = :max_attempts
                      AND lease_owner = '' AND lease_token IS NULL
                      AND lease_until IS NULL AND run_uid = ''
                      AND completed_at IS NULL
                      AND COALESCE(next_attempt_at, scheduled_for) <= :now
                      AND updated_at < :now
                      AND attempt_count < max_attempts
                    """
                )
            else:
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET status = 'RUNNING', attempt_count = :next_attempt_count,
                        lease_owner = :worker_id, lease_token = :lease_token,
                        lease_until = :lease_until, next_attempt_at = NULL,
                        error_code = NULL, error_message = NULL,
                        started_at = :started_at, completed_at = NULL,
                        updated_at = :now
                    WHERE job_id = :job_id AND status = 'PENDING'
                      AND attempt_count = :old_attempt_count
                      AND max_attempts = :max_attempts
                      AND lease_owner = '' AND lease_token IS NULL
                      AND lease_until IS NULL AND run_uid = ''
                      AND completed_at IS NULL
                      AND COALESCE(next_attempt_at, scheduled_for) <= :now
                      AND COALESCE(next_attempt_at, scheduled_for)
                          <= UTC_TIMESTAMP(6)
                      AND :lease_until > UTC_TIMESTAMP(6)
                      AND :lease_until <= DATE_ADD(
                          UTC_TIMESTAMP(6), INTERVAL 900 SECOND
                      )
                      AND updated_at < :now
                      AND attempt_count < max_attempts
                    """
                )
            params = common
        elif current.status == JOB_RUNNING:
            if current.lease_token is None or current.lease_until is None:
                raise JobStoreIntegrityError("RUNNING reclaim lacks lease identity")
            if connection.dialect.name.lower() == "sqlite":
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET attempt_count = :next_attempt_count,
                        lease_owner = :worker_id, lease_token = :lease_token,
                        lease_until = :lease_until, next_attempt_at = NULL,
                        error_code = NULL, error_message = NULL,
                        started_at = :started_at, completed_at = NULL,
                        updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND attempt_count = :old_attempt_count
                      AND max_attempts = :max_attempts
                      AND lease_owner = :old_lease_owner
                      AND lease_token = :old_lease_token
                      AND lease_token <> :lease_token
                      AND lease_until = :old_lease_until
                      AND lease_until <= :now
                      AND updated_at < :now
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                      AND attempt_count < max_attempts
                    """
                )
            else:
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET attempt_count = :next_attempt_count,
                        lease_owner = :worker_id, lease_token = :lease_token,
                        lease_until = :lease_until, next_attempt_at = NULL,
                        error_code = NULL, error_message = NULL,
                        started_at = :started_at, completed_at = NULL,
                        updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND attempt_count = :old_attempt_count
                      AND max_attempts = :max_attempts
                      AND lease_owner = :old_lease_owner
                      AND lease_token = :old_lease_token
                      AND lease_token <> :lease_token
                      AND lease_until = :old_lease_until
                      AND lease_until <= :now
                      AND lease_until <= UTC_TIMESTAMP(6)
                      AND :lease_until > UTC_TIMESTAMP(6)
                      AND :lease_until <= DATE_ADD(
                          UTC_TIMESTAMP(6), INTERVAL 900 SECOND
                      )
                      AND updated_at < :now
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                      AND attempt_count < max_attempts
                    """
                )
            params = {
                **common,
                "old_lease_owner": current.lease_owner,
                "old_lease_token": current.lease_token,
                "old_lease_until": _db_datetime(current.lease_until),
            }
        else:
            raise JobConflictError(
                "selected job is not claimable",
                current=current,
            )
        result = connection.execute(statement, params)
        if not _changed_exactly_one_row(result):
            latest = _job_by_id(connection, current.job_id, for_update=False)
            _raise_state_conflict(current.job_id, latest, operation="claim_due_job")
        _insert_claim_token_record(
            connection,
            lease_token=lease_token,
            job_id=current.job_id,
            attempt_count=next_attempt,
            lease_owner=worker_id,
            claimed_at=now,
            lease_until=lease_until,
        )
        return _exact_readback(
            _job_by_id(connection, current.job_id, for_update=False),
            expected,
            operation="claim_due_job",
        )

    def claim_due_job(
        self,
        *,
        worker_id: str,
        lease_token: str,
        now: datetime,
        lease_until: datetime,
        job_type: str | None = None,
    ) -> ClaimJobResult | None:
        worker = _exact_text(worker_id, field="worker_id", maximum=160)
        token = _sha256(lease_token, field="lease_token")
        observed_now = _utc_datetime(now, field="now")
        expires = _utc_datetime(lease_until, field="lease_until")
        if expires <= observed_now:
            raise ValueError("lease_until must be strictly later than now")
        _assert_lease_duration(observed_now, expires)
        type_filter = (
            None
            if job_type is None
            else _exact_text(job_type, field="job_type", maximum=80)
        )

        def resolve_token(
            connection: Connection,
            record: _ClaimTokenRecord,
        ) -> ClaimJobResult:
            # MySQL REPEATABLE READ can retain the snapshot established by the
            # initial non-locking registry miss.  A concurrent winner may then
            # be visible through the locking registry read while a plain job
            # read still returns the pre-claim PENDING row.  Use a locking
            # current read so both halves of the replay proof observe the same
            # committed lease generation.
            current = _job_by_id(
                connection,
                record.job_id,
                for_update=True,
            )
            if current is None:
                raise JobStoreIntegrityError(
                    "claim token registry references a missing job"
                )
            if (
                record.lease_token == token
                and record.lease_owner == worker
                and record.claimed_at == observed_now
                and record.lease_until == expires
                and current.status == JOB_RUNNING
                and current.job_id == record.job_id
                and current.lease_owner == worker
                and current.lease_token == token
                and current.attempt_count == record.attempt_count
                and current.lease_until is not None
                and current.lease_until > observed_now
                and current.lease_until == expires
                and current.updated_at == observed_now
                and (type_filter is None or current.job_type == type_filter)
                and _lease_is_live_at_database(connection, current)
            ):
                return ClaimJobResult(claimed=False, replayed=True, job=current)
            raise JobConflictError(
                "lease_token already identifies a different or expired lease",
                current=current,
            )

        def claim_once() -> ClaimJobResult | None:
            try:
                with _write_connection(self.engine) as connection:
                    # A non-locking miss avoids a MySQL unique-index gap lock
                    # before the due-candidate row lock.  Integrity handling
                    # below resolves a concurrent same-token winner exactly.
                    replay = _claim_token_by_token(
                        connection,
                        token,
                        for_update=False,
                    )
                    if replay is not None:
                        return resolve_token(connection, replay)
                    database_now = _assert_database_clock(
                        connection,
                        observed_now,
                    )
                    _assert_lease_duration(database_now, expires)
                    # A maxed-out expired lease is a terminal failure, never an
                    # invisible RUNNING row and never attempt N+1.
                    self._reap_exhausted_locked(
                        connection,
                        now=observed_now,
                        job_type=type_filter,
                    )
                    current = self._due_candidate_locked(
                        connection,
                        now=observed_now,
                        job_type=type_filter,
                    )
                    if current is None:
                        late_replay = _claim_token_by_token(
                            connection,
                            token,
                            for_update=True,
                        )
                        if late_replay is not None:
                            return resolve_token(connection, late_replay)
                        return None
                    stored = self._claim_locked(
                        connection,
                        current,
                        worker_id=worker,
                        lease_token=token,
                        now=observed_now,
                        lease_until=expires,
                    )
                    return ClaimJobResult(
                        claimed=True,
                        replayed=False,
                        job=stored,
                    )
            except IntegrityError as exc:
                with _write_connection(self.engine) as connection:
                    record = _claim_token_by_token(
                        connection,
                        token,
                        for_update=True,
                    )
                    if record is not None:
                        return resolve_token(connection, record)
                    unregistered_live = _job_by_lease_token(
                        connection,
                        token,
                        for_update=True,
                    )
                    if unregistered_live is not None:
                        raise JobStoreIntegrityError(
                            "live lease token lacks append-only registry row"
                        ) from exc
                raise JobConflictError(
                    "lease_token collided but no exact live replay was provable",
                    current=None,
                ) from exc

        for transaction_attempt in range(_JOB_STORE_TRANSACTION_MAX_ATTEMPTS):
            try:
                return claim_once()
            except OperationalError as exc:
                if not _is_transient_transaction_error(exc):
                    raise
                if transaction_attempt + 1 >= _JOB_STORE_TRANSACTION_MAX_ATTEMPTS:
                    raise JobConflictError(
                        "claim_due_job transient transaction retry exhausted",
                        current=None,
                    ) from exc
        raise JobStoreIntegrityError("claim_due_job retry state exhausted unexpectedly")

    def heartbeat(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        now: datetime,
        lease_until: datetime,
    ) -> JobSnapshot:
        normalized_job_id = _exact_text(job_id, field="job_id", maximum=64)
        worker = _exact_text(worker_id, field="worker_id", maximum=160)
        token = _sha256(lease_token, field="lease_token")
        attempt = _positive_int(attempt_count, field="attempt_count")
        observed = _utc_datetime(
            observed_lease_until,
            field="observed_lease_until",
        )
        observed_now = _utc_datetime(now, field="now")
        expires = _utc_datetime(lease_until, field="lease_until")
        if expires <= observed:
            raise ValueError("heartbeat lease_until must strictly extend the lease")
        _assert_lease_duration(observed_now, expires)
        with _write_connection(self.engine) as connection:
            current = _job_by_id(connection, normalized_job_id, for_update=True)
            if current is None:
                raise JobNotFoundError(f"V4 job not found: {normalized_job_id}")
            if current.status in TERMINAL_JOB_STATUSES:
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="heartbeat",
                )
            if not _lease_matches(
                current,
                worker_id=worker,
                lease_token=token,
                attempt_count=attempt,
                observed_lease_until=observed,
            ):
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="heartbeat",
                )
            if observed <= observed_now:
                raise JobConflictError(
                    "heartbeat cannot renew an already expired observed lease",
                    current=current,
                )
            database_now = _assert_database_clock(connection, observed_now)
            _assert_lease_duration(database_now, expires)
            if observed_now <= current.updated_at:
                raise ValueError(
                    "heartbeat now must be strictly later than current updated_at"
                )
            expected = replace(
                current,
                lease_until=expires,
                updated_at=observed_now,
            )
            if connection.dialect.name.lower() == "sqlite":
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET lease_until = :lease_until, updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND lease_owner = :worker_id
                      AND lease_token = :lease_token
                      AND attempt_count = :attempt_count
                      AND lease_until = :observed_lease_until
                      AND updated_at = :old_updated_at
                      AND updated_at < :now
                      AND lease_until > :now
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                    """
                )
            else:
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET lease_until = :lease_until, updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND lease_owner = :worker_id
                      AND lease_token = :lease_token
                      AND attempt_count = :attempt_count
                      AND lease_until = :observed_lease_until
                      AND updated_at = :old_updated_at
                      AND updated_at < :now
                      AND lease_until > :now
                      AND lease_until > UTC_TIMESTAMP(6)
                      AND :lease_until > UTC_TIMESTAMP(6)
                      AND :lease_until <= DATE_ADD(
                          UTC_TIMESTAMP(6), INTERVAL 900 SECOND
                      )
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                    """
                )
            result = connection.execute(
                statement,
                {
                    "job_id": normalized_job_id,
                    "worker_id": worker,
                    "lease_token": token,
                    "attempt_count": attempt,
                    "observed_lease_until": _db_datetime(observed),
                    "old_updated_at": _db_datetime(current.updated_at),
                    "lease_until": _db_datetime(expires),
                    "now": _db_datetime(observed_now),
                },
            )
            if not _changed_exactly_one_row(result):
                latest = _job_by_id(
                    connection,
                    normalized_job_id,
                    for_update=False,
                )
                _raise_state_conflict(
                    normalized_job_id,
                    latest,
                    operation="heartbeat",
                )
            return _exact_readback(
                _job_by_id(connection, normalized_job_id, for_update=False),
                expected,
                operation="heartbeat",
            )

    def complete(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        run_uid: str,
        now: datetime,
    ) -> JobSnapshot:
        normalized_job_id = _exact_text(job_id, field="job_id", maximum=64)
        worker = _exact_text(worker_id, field="worker_id", maximum=160)
        token = _sha256(lease_token, field="lease_token")
        attempt = _positive_int(attempt_count, field="attempt_count")
        observed = _utc_datetime(
            observed_lease_until,
            field="observed_lease_until",
        )
        completed = _utc_datetime(now, field="now")
        normalized_run_uid = _exact_text(run_uid, field="run_uid", maximum=64)
        with _write_connection(self.engine) as connection:
            current = _job_by_id(connection, normalized_job_id, for_update=True)
            if current is None:
                raise JobNotFoundError(f"V4 job not found: {normalized_job_id}")
            if current.status in TERMINAL_JOB_STATUSES:
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="complete",
                )
            if not _lease_matches(
                current,
                worker_id=worker,
                lease_token=token,
                attempt_count=attempt,
                observed_lease_until=observed,
            ):
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="complete",
                )
            if observed <= completed:
                raise JobConflictError(
                    "complete cannot use an expired lease",
                    current=current,
                )
            if completed <= current.updated_at:
                raise ValueError(
                    "complete now must be strictly later than current updated_at"
                )
            _assert_database_clock(connection, completed)
            expected = replace(
                current,
                run_uid=normalized_run_uid,
                status=JOB_SUCCEEDED,
                lease_owner="",
                lease_token=None,
                lease_until=None,
                next_attempt_at=None,
                error_code=None,
                error_message=None,
                completed_at=completed,
                updated_at=completed,
            )
            if connection.dialect.name.lower() == "sqlite":
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET status = 'SUCCEEDED', run_uid = :run_uid,
                        lease_owner = '', lease_token = NULL,
                        lease_until = NULL, next_attempt_at = NULL,
                        error_code = NULL, error_message = NULL,
                        completed_at = :now, updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND lease_owner = :worker_id
                      AND lease_token = :lease_token
                      AND attempt_count = :attempt_count
                      AND lease_until = :observed_lease_until
                      AND updated_at = :old_updated_at
                      AND updated_at < :now
                      AND lease_until > :now
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                    """
                )
            else:
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET status = 'SUCCEEDED', run_uid = :run_uid,
                        lease_owner = '', lease_token = NULL,
                        lease_until = NULL, next_attempt_at = NULL,
                        error_code = NULL, error_message = NULL,
                        completed_at = :now, updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND lease_owner = :worker_id
                      AND lease_token = :lease_token
                      AND attempt_count = :attempt_count
                      AND lease_until = :observed_lease_until
                      AND updated_at = :old_updated_at
                      AND updated_at < :now
                      AND lease_until > :now
                      AND lease_until > UTC_TIMESTAMP(6)
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                    """
                )
            result = connection.execute(
                statement,
                {
                    "job_id": normalized_job_id,
                    "worker_id": worker,
                    "lease_token": token,
                    "attempt_count": attempt,
                    "observed_lease_until": _db_datetime(observed),
                    "old_updated_at": _db_datetime(current.updated_at),
                    "run_uid": normalized_run_uid,
                    "now": _db_datetime(completed),
                },
            )
            if not _changed_exactly_one_row(result):
                latest = _job_by_id(
                    connection,
                    normalized_job_id,
                    for_update=False,
                )
                _raise_state_conflict(
                    normalized_job_id,
                    latest,
                    operation="complete",
                )
            return _exact_readback(
                _job_by_id(connection, normalized_job_id, for_update=False),
                expected,
                operation="complete",
            )

    def fail(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        now: datetime,
        retryable: bool,
        next_attempt_at: datetime | None,
        error_code: str,
        error_message: str = "",
    ) -> JobSnapshot:
        normalized_job_id = _exact_text(job_id, field="job_id", maximum=64)
        worker = _exact_text(worker_id, field="worker_id", maximum=160)
        token = _sha256(lease_token, field="lease_token")
        attempt = _positive_int(attempt_count, field="attempt_count")
        observed = _utc_datetime(
            observed_lease_until,
            field="observed_lease_until",
        )
        failed_at = _utc_datetime(now, field="now")
        if type(retryable) is not bool:
            raise TypeError("retryable must be exactly bool")
        retry_at = (
            None
            if next_attempt_at is None
            else _utc_datetime(next_attempt_at, field="next_attempt_at")
        )
        code = _exact_text(error_code, field="error_code", maximum=100)
        message = _exact_text(
            error_message,
            field="error_message",
            maximum=1000,
            allow_empty=True,
        )
        if not retryable and retry_at is not None:
            raise ValueError("non-retryable failure cannot set next_attempt_at")
        with _write_connection(self.engine) as connection:
            current = _job_by_id(connection, normalized_job_id, for_update=True)
            if current is None:
                raise JobNotFoundError(f"V4 job not found: {normalized_job_id}")
            if current.status in TERMINAL_JOB_STATUSES:
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="fail",
                )
            if not _lease_matches(
                current,
                worker_id=worker,
                lease_token=token,
                attempt_count=attempt,
                observed_lease_until=observed,
            ):
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="fail",
                )
            if observed <= failed_at:
                raise JobConflictError("fail cannot use an expired lease", current=current)
            if failed_at <= current.updated_at:
                raise ValueError("fail now must be strictly later than current updated_at")
            _assert_database_clock(connection, failed_at)
            will_retry = retryable and current.attempt_count < current.max_attempts
            if will_retry:
                if retry_at is None:
                    raise ValueError(
                        "retryable failure with attempts remaining requires next_attempt_at"
                    )
                if retry_at <= failed_at:
                    raise ValueError(
                        "next_attempt_at must be strictly later than failure time"
                    )
                next_status = JOB_PENDING
                completed_at = None
            else:
                next_status = JOB_FAILED
                completed_at = failed_at
                retry_at = None
            expected = replace(
                current,
                status=next_status,
                lease_owner="",
                lease_token=None,
                lease_until=None,
                next_attempt_at=retry_at,
                error_code=code,
                error_message=message,
                completed_at=completed_at,
                updated_at=failed_at,
            )
            if connection.dialect.name.lower() == "sqlite":
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET status = :next_status, lease_owner = '',
                        lease_token = NULL, lease_until = NULL,
                        next_attempt_at = :next_attempt_at,
                        error_code = :error_code,
                        error_message = :error_message,
                        completed_at = :completed_at, updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND lease_owner = :worker_id
                      AND lease_token = :lease_token
                      AND attempt_count = :attempt_count
                      AND lease_until = :observed_lease_until
                      AND updated_at = :old_updated_at
                      AND updated_at < :now
                      AND lease_until > :now
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                      AND (:next_status <> 'PENDING'
                           OR :next_attempt_at > :now)
                    """
                )
            else:
                statement = text(
                    """
                    UPDATE st_job_run_v4
                    SET status = :next_status, lease_owner = '',
                        lease_token = NULL, lease_until = NULL,
                        next_attempt_at = :next_attempt_at,
                        error_code = :error_code,
                        error_message = :error_message,
                        completed_at = :completed_at, updated_at = :now
                    WHERE job_id = :job_id AND status = 'RUNNING'
                      AND lease_owner = :worker_id
                      AND lease_token = :lease_token
                      AND attempt_count = :attempt_count
                      AND lease_until = :observed_lease_until
                      AND updated_at = :old_updated_at
                      AND updated_at < :now
                      AND lease_until > :now
                      AND lease_until > UTC_TIMESTAMP(6)
                      AND run_uid = '' AND next_attempt_at IS NULL
                      AND error_code IS NULL AND error_message IS NULL
                      AND completed_at IS NULL
                      AND (:next_status <> 'PENDING'
                           OR :next_attempt_at > :now)
                    """
                )
            result = connection.execute(
                statement,
                {
                    "job_id": normalized_job_id,
                    "worker_id": worker,
                    "lease_token": token,
                    "attempt_count": attempt,
                    "observed_lease_until": _db_datetime(observed),
                    "old_updated_at": _db_datetime(current.updated_at),
                    "next_status": next_status,
                    "next_attempt_at": (
                        None if retry_at is None else _db_datetime(retry_at)
                    ),
                    "error_code": code,
                    "error_message": message,
                    "completed_at": (
                        None
                        if completed_at is None
                        else _db_datetime(completed_at)
                    ),
                    "now": _db_datetime(failed_at),
                },
            )
            if not _changed_exactly_one_row(result):
                latest = _job_by_id(
                    connection,
                    normalized_job_id,
                    for_update=False,
                )
                _raise_state_conflict(
                    normalized_job_id,
                    latest,
                    operation="fail",
                )
            return _exact_readback(
                _job_by_id(connection, normalized_job_id, for_update=False),
                expected,
                operation="fail",
            )

    def expire_exhausted_job(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        now: datetime,
    ) -> JobSnapshot:
        normalized_job_id = _exact_text(job_id, field="job_id", maximum=64)
        owner = _exact_text(lease_owner, field="lease_owner", maximum=160)
        token = _sha256(lease_token, field="lease_token")
        attempt = _positive_int(attempt_count, field="attempt_count")
        observed = _utc_datetime(
            observed_lease_until,
            field="observed_lease_until",
        )
        expired_at = _utc_datetime(now, field="now")
        with _write_connection(self.engine) as connection:
            current = _job_by_id(connection, normalized_job_id, for_update=True)
            if current is None:
                raise JobNotFoundError(f"V4 job not found: {normalized_job_id}")
            if current.status in TERMINAL_JOB_STATUSES:
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="expire_exhausted_job",
                )
            if not _lease_matches(
                current,
                worker_id=owner,
                lease_token=token,
                attempt_count=attempt,
                observed_lease_until=observed,
            ):
                _raise_state_conflict(
                    normalized_job_id,
                    current,
                    operation="expire_exhausted_job",
                )
            if (
                current.lease_until is None
                or current.lease_until > expired_at
                or current.attempt_count < current.max_attempts
            ):
                raise JobConflictError(
                    "job is not an expired exhausted RUNNING lease",
                    current=current,
                )
            _assert_database_clock(connection, expired_at)
            return self._expire_locked(connection, current, now=expired_at)


__all__ = (
    "EXHAUSTED_LEASE_ERROR_CODE",
    "EXHAUSTED_REAP_BATCH_SIZE",
    "JOB_CANCELLED",
    "JOB_FAILED",
    "JOB_PENDING",
    "JOB_RUNNING",
    "JOB_STATUSES",
    "JOB_SUCCEEDED",
    "JOB_CALLER_CLOCK_MAX_SKEW_SECONDS",
    "JOB_LEASE_MAX_DURATION_SECONDS",
    "TERMINAL_JOB_STATUSES",
    "ClaimJobResult",
    "CreateJobResult",
    "JobAlreadyTerminalError",
    "JobClockSkewError",
    "JobConflictError",
    "JobNotFoundError",
    "JobSnapshot",
    "JobStoreError",
    "JobStoreIntegrityError",
    "JobStoreRepository",
)
