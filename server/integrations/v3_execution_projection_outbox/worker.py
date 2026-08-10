"""Lease/retry worker that projects committed V2 outbox events into V3."""
from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import hmac
import json
import os
import secrets
from typing import Any
import uuid

from sqlalchemy import text

from server.integrations.v3_execution_projection import (
    ProjectionApplyStatus,
    V3ProjectionApplyResult,
    apply_v3_execution_projection,
)

from .outbox import V3ProjectionOutboxError, projection_from_payload
from . import legacy_guard
from .schema import validate_v3_projection_outbox_schema


class V3ProjectionWorkerError(RuntimeError):
    pass


class V3ProjectionWorkerDisabledError(V3ProjectionWorkerError):
    pass


_TEST_CAPABILITY_CONSTRUCTION_KEY = object()
_TEST_CAPABILITY_SIGNING_KEY = secrets.token_bytes(32)
_RUNTIME_ENVIRONMENT_VARIABLE = "PROBIGA_RUNTIME_ENVIRONMENT"
_NON_PRODUCTION_ENVIRONMENTS = frozenset({"TEST", "CI"})
_TEST_CAPABILITY_SCOPE = "V3_EXECUTION_PROJECTION_OUTBOX_TEST"
_MAXIMUM_TEST_CAPABILITY_LIFETIME = timedelta(hours=1)


def _system_utc_now() -> datetime:
    """Return the process host clock; business timestamps cannot move the gate."""

    return datetime.now(timezone.utc)


def _runtime_environment() -> str:
    return str(os.environ.get(_RUNTIME_ENVIRONMENT_VARIABLE) or "").strip().upper()


def _aware_utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256")
    return normalized


def _test_capability_digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {
            "namespace": "v3.execution-projection-outbox.test-capability.v1",
            "payload": payload,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _audit_digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class _V3ProjectionWorkerTestCapability:
    """Process-bound capability minted only by the private TEST/CI seam."""

    test_run_id: str
    acceptance_report_hash: str
    environment: str
    issued_at: datetime
    expires_at: datetime
    scope: str = _TEST_CAPABILITY_SCOPE
    production_activation_allowed: bool = False
    capability_hash: str
    _issuer_mac: str = field(repr=False)

    def __init__(
        self,
        *,
        test_run_id: str,
        acceptance_report_hash: str,
        environment: str,
        issued_at: datetime,
        expires_at: datetime,
        _construction_key: object | None = None,
    ) -> None:
        if _construction_key is not _TEST_CAPABILITY_CONSTRUCTION_KEY:
            raise V3ProjectionWorkerDisabledError(
                "worker test capabilities require the trusted in-process issuer"
            )
        normalized_test_run_id = _text(test_run_id, "test_run_id")
        report_hash = _sha256(
            acceptance_report_hash,
            "acceptance_report_hash",
        )
        normalized_environment = _text(environment, "environment").upper()
        if normalized_environment not in _NON_PRODUCTION_ENVIRONMENTS:
            raise ValueError("worker capability environment must be TEST or CI")
        normalized_issued_at = _aware_utc(issued_at, "issued_at")
        normalized_expires_at = _aware_utc(expires_at, "expires_at")
        lifetime = normalized_expires_at - normalized_issued_at
        if lifetime <= timedelta(0):
            raise ValueError("worker capability must expire after issue time")
        if lifetime > _MAXIMUM_TEST_CAPABILITY_LIFETIME:
            raise ValueError("worker capability lifetime exceeds one hour")
        payload = {
            "test_run_id": normalized_test_run_id,
            "acceptance_report_hash": report_hash,
            "environment": normalized_environment,
            "issued_at": normalized_issued_at.isoformat(timespec="microseconds"),
            "expires_at": normalized_expires_at.isoformat(timespec="microseconds"),
            "scope": _TEST_CAPABILITY_SCOPE,
            "production_activation_allowed": False,
        }
        capability_hash = _test_capability_digest(payload)
        issuer_mac = hmac.new(
            _TEST_CAPABILITY_SIGNING_KEY,
            capability_hash.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        object.__setattr__(self, "test_run_id", normalized_test_run_id)
        object.__setattr__(self, "acceptance_report_hash", report_hash)
        object.__setattr__(self, "environment", normalized_environment)
        object.__setattr__(self, "issued_at", normalized_issued_at)
        object.__setattr__(self, "expires_at", normalized_expires_at)
        object.__setattr__(self, "scope", _TEST_CAPABILITY_SCOPE)
        object.__setattr__(self, "production_activation_allowed", False)
        object.__setattr__(self, "capability_hash", capability_hash)
        object.__setattr__(self, "_issuer_mac", issuer_mac)


def _issue_trusted_test_runtime_capability(
    *,
    test_run_id: str,
    acceptance_report_hash: str,
    valid_for: timedelta = timedelta(minutes=15),
) -> _V3ProjectionWorkerTestCapability:
    """Private acceptance-test seam; environment and time come from the process."""

    if type(valid_for) is not timedelta:
        raise TypeError("valid_for must be exactly timedelta")
    if (
        valid_for <= timedelta(0)
        or valid_for > _MAXIMUM_TEST_CAPABILITY_LIFETIME
    ):
        raise ValueError("valid_for must be greater than zero and at most one hour")
    environment = _runtime_environment()
    if environment not in _NON_PRODUCTION_ENVIRONMENTS:
        raise V3ProjectionWorkerDisabledError(
            "trusted worker capability issuance requires the TEST or CI process environment"
        )
    issued_at = _aware_utc(_system_utc_now(), "system UTC clock")
    return _V3ProjectionWorkerTestCapability(
        test_run_id=test_run_id,
        acceptance_report_hash=acceptance_report_hash,
        environment=environment,
        issued_at=issued_at,
        expires_at=issued_at + valid_for,
        _construction_key=_TEST_CAPABILITY_CONSTRUCTION_KEY,
    )


class V3ProjectionWorkerStatus(str, Enum):
    IDLE = "IDLE"
    PUBLISHED = "PUBLISHED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    DEAD_LETTER = "DEAD_LETTER"


class V3ProjectionBaselineStatus(str, Enum):
    INSERTED = "INSERTED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class V3ProjectionOutboxLease:
    outbox_sequence: int
    outbox_id: str
    projection_id: str
    projection_payload_hash: str
    canonical_payload_hash: str
    payload_json: str
    source_order_id: str
    source_transition_id: str
    source_sequence: int
    attempt_count: int
    lease_owner: str
    lease_token: str
    lease_until: datetime


@dataclass(frozen=True, slots=True)
class V3ProjectionWorkerResult:
    status: V3ProjectionWorkerStatus
    outbox_id: str | None
    projection_id: str | None
    attempt_count: int
    subscriber_result: V3ProjectionApplyResult | None = None
    error: str = ""
    production_activation_allowed: bool = False


@dataclass(frozen=True, slots=True)
class V3ProjectionBaselineResult:
    status: V3ProjectionBaselineStatus
    source_order_id: str
    baseline_sequence: int
    baseline_audit_hash: str
    production_activation_allowed: bool = False


@dataclass(frozen=True, slots=True)
class V3ProjectionDeadLetterRequeueResult:
    outbox_id: str
    source_order_id: str
    source_sequence: int
    previous_attempt_count: int
    reconciliation_audit_hash: str
    production_activation_allowed: bool = False


TransactionFactory = Callable[[], AbstractContextManager[Any]]
SubscriberPort = Callable[..., V3ProjectionApplyResult]


@dataclass(frozen=True, slots=True)
class V3ProjectionWorkerPorts:
    outbox_transaction: TransactionFactory
    projection_transaction: TransactionFactory
    subscriber: SubscriberPort = apply_v3_execution_projection

    def __post_init__(self) -> None:
        for callback, name in (
            (self.outbox_transaction, "outbox_transaction"),
            (self.projection_transaction, "projection_transaction"),
            (self.subscriber, "subscriber"),
        ):
            if not callable(callback):
                raise TypeError(f"{name} must be callable")


def _aware_utc_naive(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _bounded_text(value: object, field_name: str, maximum: int) -> str:
    normalized = _text(value, field_name)
    if len(normalized) > maximum:
        raise ValueError(f"{field_name} exceeds {maximum} characters")
    return normalized


def _positive_int(value: object, field_name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive exact int")
    return value


def _stored_utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise V3ProjectionWorkerError(f"stored {field_name} must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _baseline_payload(
    *,
    source_order_id: str,
    baseline_sequence: int,
    baseline_transition_id: str,
    baseline_order_state_hash: str,
    reconciliation_evidence_hash: str,
    reconciled_by: str,
    reconciled_at: datetime,
) -> dict[str, Any]:
    return {
        "source_order_id": source_order_id,
        "baseline_sequence": baseline_sequence,
        "baseline_transition_id": baseline_transition_id,
        "baseline_order_state_hash": baseline_order_state_hash,
        "reconciliation_evidence_hash": reconciliation_evidence_hash,
        "reconciled_by": reconciled_by,
        "reconciled_at": reconciled_at.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ),
    }


def _baseline_audit_hash(**payload: Any) -> str:
    return _audit_digest(
        "v3.execution-projection-order-baseline.v1",
        _baseline_payload(**payload),
    )


def _stored_baseline_record(stored: Any) -> dict[str, Any]:
    return {
        "source_order_id": str(stored["source_order_id"]),
        "baseline_sequence": int(stored["baseline_sequence"]),
        "baseline_transition_id": str(stored["baseline_transition_id"]),
        "baseline_order_state_hash": str(stored["baseline_order_state_hash"]),
        "reconciliation_evidence_hash": str(
            stored["reconciliation_evidence_hash"]
        ),
        "baseline_audit_hash": str(stored["baseline_audit_hash"]),
        "reconciled_by": str(stored["reconciled_by"]),
        "reconciled_at": _stored_utc(
            stored["reconciled_at"],
            "baseline reconciled_at",
        ),
    }


def _validate_subscriber_result(
    result: object,
    projection: Any,
) -> V3ProjectionApplyResult:
    if type(result) is not V3ProjectionApplyResult:
        raise V3ProjectionOutboxError(
            "projection subscriber returned an invalid durable receipt"
        )
    if type(result.status) is not ProjectionApplyStatus or result.status not in {
        ProjectionApplyStatus.APPLIED,
        ProjectionApplyStatus.IDEMPOTENT,
    }:
        raise V3ProjectionOutboxError(
            "projection subscriber receipt status is invalid"
        )
    expected = (
        ("projection_id", projection.projection_id),
        ("execution_plan_id", projection.execution_plan_id),
        ("source_sequence", projection.source_sequence),
        ("plan_state", projection.state.value),
    )
    for field_name, expected_value in expected:
        if getattr(result, field_name) != expected_value:
            raise V3ProjectionOutboxError(
                f"projection subscriber receipt differs: {field_name}"
            )
    return result


def _require_runtime_enabled(
    runtime_override: _V3ProjectionWorkerTestCapability | None,
) -> None:
    if type(legacy_guard.OUTBOX_RUNTIME_ENABLED) is not bool:
        raise V3ProjectionWorkerDisabledError("outbox runtime flag is invalid")
    if type(legacy_guard.PRODUCTION_ACTIVATION_ALLOWED) is not bool:
        raise V3ProjectionWorkerDisabledError(
            "projection production activation flag is invalid"
        )
    if legacy_guard.PRODUCTION_ACTIVATION_ALLOWED:
        raise V3ProjectionWorkerDisabledError(
            "projection outbox may not enable production actions"
        )
    environment = _runtime_environment()
    if environment not in _NON_PRODUCTION_ENVIRONMENTS:
        raise V3ProjectionWorkerDisabledError(
            "projection outbox worker requires the TEST or CI process environment"
        )
    if legacy_guard.OUTBOX_RUNTIME_ENABLED:
        try:
            legacy_guard.require_outbox_replacement_safe()
        except legacy_guard.LegacyDirectSyncStillActiveError as exc:
            raise V3ProjectionWorkerDisabledError(
                "projection outbox replacement is not safe to activate"
            ) from exc
        return
    _validate_test_capability(runtime_override)


def _validate_test_capability(
    runtime_override: _V3ProjectionWorkerTestCapability | None,
) -> _V3ProjectionWorkerTestCapability:
    environment = _runtime_environment()
    if type(runtime_override) is not _V3ProjectionWorkerTestCapability:
        raise V3ProjectionWorkerDisabledError(
            "projection outbox runtime is disabled without a trusted TEST/CI capability"
        )
    if environment != runtime_override.environment:
        raise V3ProjectionWorkerDisabledError(
            "worker capability does not match the current TEST/CI process environment"
        )
    payload = {
        "test_run_id": runtime_override.test_run_id,
        "acceptance_report_hash": runtime_override.acceptance_report_hash,
        "environment": runtime_override.environment,
        "issued_at": runtime_override.issued_at.isoformat(timespec="microseconds"),
        "expires_at": runtime_override.expires_at.isoformat(timespec="microseconds"),
        "scope": runtime_override.scope,
        "production_activation_allowed": (
            runtime_override.production_activation_allowed
        ),
    }
    expected_hash = _test_capability_digest(payload)
    expected_mac = hmac.new(
        _TEST_CAPABILITY_SIGNING_KEY,
        expected_hash.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_hash, runtime_override.capability_hash):
        raise V3ProjectionWorkerDisabledError("worker capability was tampered")
    if not hmac.compare_digest(expected_mac, runtime_override._issuer_mac):
        raise V3ProjectionWorkerDisabledError("worker capability issuer is invalid")
    now = _aware_utc(_system_utc_now(), "system UTC clock")
    if now < runtime_override.issued_at or now >= runtime_override.expires_at:
        raise V3ProjectionWorkerDisabledError("worker capability is not active")
    return runtime_override


def _active(connection: Any) -> Any:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise V3ProjectionWorkerError("worker transaction connection is required")
    probe = getattr(connection, "in_transaction", None)
    if not callable(probe) or probe() is not True:
        raise V3ProjectionWorkerError("worker requires an active transaction")
    return connection


def register_v3_projection_order_baseline(
    connection: Any,
    *,
    source_order_id: str,
    baseline_sequence: int,
    baseline_transition_id: str,
    baseline_order_state_hash: str,
    reconciliation_evidence_hash: str,
    reconciled_by: str,
    reconciled_at: datetime,
    runtime_override: _V3ProjectionWorkerTestCapability | None = None,
) -> V3ProjectionBaselineResult:
    """Record one immutable, audited cut-over point for an existing order."""

    _require_runtime_enabled(runtime_override)
    _validate_test_capability(runtime_override)
    active = _active(connection)
    validate_v3_projection_outbox_schema(active)
    normalized_order_id = _bounded_text(source_order_id, "source_order_id", 64)
    normalized_sequence = _positive_int(baseline_sequence, "baseline_sequence")
    normalized_transition_id = _sha256(
        baseline_transition_id,
        "baseline_transition_id",
    )
    normalized_state_hash = _sha256(
        baseline_order_state_hash,
        "baseline_order_state_hash",
    )
    normalized_evidence_hash = _sha256(
        reconciliation_evidence_hash,
        "reconciliation_evidence_hash",
    )
    normalized_actor = _bounded_text(reconciled_by, "reconciled_by", 120)
    normalized_at = _aware_utc(reconciled_at, "reconciled_at")
    audit_hash = _baseline_audit_hash(
        source_order_id=normalized_order_id,
        baseline_sequence=normalized_sequence,
        baseline_transition_id=normalized_transition_id,
        baseline_order_state_hash=normalized_state_hash,
        reconciliation_evidence_hash=normalized_evidence_hash,
        reconciled_by=normalized_actor,
        reconciled_at=normalized_at,
    )
    stored = active.execute(
        text(
            "SELECT source_order_id, baseline_sequence, "
            "baseline_transition_id, baseline_order_state_hash, "
            "reconciliation_evidence_hash, baseline_audit_hash, "
            "reconciled_by, reconciled_at "
            "FROM st_execution_projection_order_baseline_v3 "
            "WHERE source_order_id = :source_order_id FOR UPDATE"
        ),
        {"source_order_id": normalized_order_id},
    ).mappings().first()
    expected = {
        "source_order_id": normalized_order_id,
        "baseline_sequence": normalized_sequence,
        "baseline_transition_id": normalized_transition_id,
        "baseline_order_state_hash": normalized_state_hash,
        "reconciliation_evidence_hash": normalized_evidence_hash,
        "baseline_audit_hash": audit_hash,
        "reconciled_by": normalized_actor,
        "reconciled_at": normalized_at,
    }
    if stored is not None:
        observed = _stored_baseline_record(stored)
        if observed != expected:
            raise V3ProjectionWorkerError(
                "projection order baseline already carries different content"
            )
        polluted = active.execute(
            text(
                "SELECT outbox_id, source_sequence, status "
                "FROM st_execution_projection_outbox_v2 "
                "WHERE source_order_id = :source_order_id "
                "AND source_sequence <= :baseline_sequence "
                "ORDER BY source_sequence LIMIT 1 FOR UPDATE"
            ),
            {
                "source_order_id": normalized_order_id,
                "baseline_sequence": normalized_sequence,
            },
        ).mappings().first()
        if polluted is not None:
            raise V3ProjectionWorkerError(
                "projection baseline is polluted by skipped outbox history"
            )
        return V3ProjectionBaselineResult(
            status=V3ProjectionBaselineStatus.IDEMPOTENT,
            source_order_id=normalized_order_id,
            baseline_sequence=normalized_sequence,
            baseline_audit_hash=audit_hash,
        )
    existing_outbox = active.execute(
        text(
            "SELECT outbox_id, source_sequence, status "
            "FROM st_execution_projection_outbox_v2 "
            "WHERE source_order_id = :source_order_id "
            "ORDER BY source_sequence LIMIT 1 FOR UPDATE"
        ),
        {"source_order_id": normalized_order_id},
    ).mappings().first()
    if existing_outbox is not None:
        raise V3ProjectionWorkerError(
            "projection baseline cannot skip an existing outbox predecessor"
        )
    inserted = active.execute(
        text(
            """
            INSERT INTO st_execution_projection_order_baseline_v3 (
                source_order_id, baseline_sequence, baseline_transition_id,
                baseline_order_state_hash, reconciliation_evidence_hash,
                baseline_audit_hash, reconciled_by, reconciled_at
            ) VALUES (
                :source_order_id, :baseline_sequence, :baseline_transition_id,
                :baseline_order_state_hash, :reconciliation_evidence_hash,
                :baseline_audit_hash, :reconciled_by, :reconciled_at
            )
            """
        ),
        {
            **expected,
            "reconciled_at": normalized_at.replace(tzinfo=None),
        },
    )
    if int(inserted.rowcount or 0) != 1:
        raise V3ProjectionWorkerError("projection baseline insert was not durable")
    persisted = active.execute(
        text(
            "SELECT source_order_id, baseline_sequence, "
            "baseline_transition_id, baseline_order_state_hash, "
            "reconciliation_evidence_hash, baseline_audit_hash, "
            "reconciled_by, reconciled_at "
            "FROM st_execution_projection_order_baseline_v3 "
            "WHERE source_order_id = :source_order_id FOR UPDATE"
        ),
        {"source_order_id": normalized_order_id},
    ).mappings().first()
    if persisted is None or _stored_baseline_record(persisted) != expected:
        raise V3ProjectionWorkerError("projection baseline readback differs")
    return V3ProjectionBaselineResult(
        status=V3ProjectionBaselineStatus.INSERTED,
        source_order_id=normalized_order_id,
        baseline_sequence=normalized_sequence,
        baseline_audit_hash=audit_hash,
    )


def _validate_candidate_baseline(row: Any) -> int:
    raw_sequence = row.get("baseline_sequence")
    if raw_sequence is None:
        if any(
            row.get(field_name) is not None
            for field_name in (
                "baseline_transition_id",
                "baseline_order_state_hash",
                "reconciliation_evidence_hash",
                "baseline_audit_hash",
                "baseline_reconciled_by",
                "baseline_reconciled_at",
            )
        ):
            raise V3ProjectionWorkerError("projection baseline is partially null")
        return 0
    sequence = _positive_int(raw_sequence, "stored baseline_sequence")
    order_id = _bounded_text(row.get("source_order_id"), "source_order_id", 64)
    transition_id = _sha256(
        row.get("baseline_transition_id"),
        "stored baseline_transition_id",
    )
    order_state_hash = _sha256(
        row.get("baseline_order_state_hash"),
        "stored baseline_order_state_hash",
    )
    evidence_hash = _sha256(
        row.get("reconciliation_evidence_hash"),
        "stored reconciliation_evidence_hash",
    )
    reconciled_by = _bounded_text(
        row.get("baseline_reconciled_by"),
        "stored baseline_reconciled_by",
        120,
    )
    reconciled_at = _stored_utc(
        row.get("baseline_reconciled_at"),
        "baseline_reconciled_at",
    )
    expected_hash = _baseline_audit_hash(
        source_order_id=order_id,
        baseline_sequence=sequence,
        baseline_transition_id=transition_id,
        baseline_order_state_hash=order_state_hash,
        reconciliation_evidence_hash=evidence_hash,
        reconciled_by=reconciled_by,
        reconciled_at=reconciled_at,
    )
    if not hmac.compare_digest(
        expected_hash,
        _sha256(row.get("baseline_audit_hash"), "stored baseline_audit_hash"),
    ):
        raise V3ProjectionWorkerError("projection baseline audit hash differs")
    if int(row["source_sequence"]) <= sequence:
        raise V3ProjectionWorkerError(
            "projection candidate does not advance its audited baseline"
        )
    return sequence


def requeue_v3_projection_dead_letter(
    connection: Any,
    *,
    outbox_id: str,
    reason: str,
    reconciled_by: str,
    reconciled_at: datetime,
    runtime_override: _V3ProjectionWorkerTestCapability | None = None,
) -> V3ProjectionDeadLetterRequeueResult:
    """Requeue the failed predecessor itself; never advance past it."""

    _require_runtime_enabled(runtime_override)
    capability = _validate_test_capability(runtime_override)
    active = _active(connection)
    validate_v3_projection_outbox_schema(active)
    normalized_outbox_id = _sha256(outbox_id, "outbox_id")
    normalized_reason = _bounded_text(reason, "reason", 1000)
    normalized_actor = _bounded_text(reconciled_by, "reconciled_by", 120)
    normalized_at = _aware_utc(reconciled_at, "reconciled_at")
    row = active.execute(
        text(
            "SELECT outbox_id, source_order_id, source_sequence, status, "
            "attempt_count FROM st_execution_projection_outbox_v2 "
            "WHERE outbox_id = :outbox_id FOR UPDATE"
        ),
        {"outbox_id": normalized_outbox_id},
    ).mappings().first()
    if row is None:
        raise V3ProjectionWorkerError("dead-letter outbox row is missing")
    if str(row["status"]) != "DEAD_LETTER":
        raise V3ProjectionWorkerError(
            "only a DEAD_LETTER row may be reconciled for requeue"
        )
    source_order_id = _bounded_text(
        row["source_order_id"],
        "stored source_order_id",
        64,
    )
    source_sequence = _positive_int(
        row["source_sequence"],
        "stored source_sequence",
    )
    previous_attempt_count = _positive_int(
        row["attempt_count"],
        "stored attempt_count",
    )
    payload = {
        "outbox_id": normalized_outbox_id,
        "source_order_id": source_order_id,
        "source_sequence": source_sequence,
        "previous_attempt_count": previous_attempt_count,
        "action": "REQUEUE",
        "reason": normalized_reason,
        "reconciled_by": normalized_actor,
        "capability_hash": capability.capability_hash,
        "reconciled_at": normalized_at.isoformat(timespec="microseconds"),
    }
    audit_hash = _audit_digest(
        "v3.execution-projection-dead-letter-reconciliation.v1",
        payload,
    )
    updated = active.execute(
        text(
            """
            UPDATE st_execution_projection_outbox_v2
            SET status = 'PENDING', attempt_count = 0,
                available_at = :reconciled_at, lease_owner = NULL,
                lease_token = NULL, lease_until = NULL, last_error = NULL,
                updated_at = :reconciled_at
            WHERE outbox_id = :outbox_id AND status = 'DEAD_LETTER'
              AND attempt_count = :previous_attempt_count
            """
        ),
        {
            "outbox_id": normalized_outbox_id,
            "previous_attempt_count": previous_attempt_count,
            "reconciled_at": normalized_at.replace(tzinfo=None),
        },
    )
    if int(updated.rowcount or 0) != 1:
        raise V3ProjectionWorkerError("dead-letter requeue CAS failed")
    audit_parameters = {
        **payload,
        "reconciliation_audit_hash": audit_hash,
        "reconciled_at": normalized_at.replace(tzinfo=None),
    }
    audit_insert = active.execute(
        text(
            """
            INSERT INTO
                st_execution_projection_dead_letter_reconciliation_v3 (
                reconciliation_audit_hash, outbox_id, source_order_id,
                source_sequence, previous_attempt_count, action, reason,
                reconciled_by, capability_hash, reconciled_at
            ) VALUES (
                :reconciliation_audit_hash, :outbox_id, :source_order_id,
                :source_sequence, :previous_attempt_count, 'REQUEUE', :reason,
                :reconciled_by, :capability_hash, :reconciled_at
            )
            """
        ),
        audit_parameters,
    )
    if int(audit_insert.rowcount or 0) != 1:
        raise V3ProjectionWorkerError(
            "dead-letter reconciliation audit insert was not durable"
        )
    persisted = active.execute(
        text(
            "SELECT reconciliation_audit_hash, outbox_id, source_order_id, "
            "source_sequence, previous_attempt_count, action, reason, "
            "reconciled_by, capability_hash, reconciled_at "
            "FROM st_execution_projection_dead_letter_reconciliation_v3 "
            "WHERE reconciliation_audit_hash = :reconciliation_audit_hash "
            "FOR UPDATE"
        ),
        {"reconciliation_audit_hash": audit_hash},
    ).mappings().first()
    expected_audit = {
        "reconciliation_audit_hash": audit_hash,
        "outbox_id": normalized_outbox_id,
        "source_order_id": source_order_id,
        "source_sequence": source_sequence,
        "previous_attempt_count": previous_attempt_count,
        "action": "REQUEUE",
        "reason": normalized_reason,
        "reconciled_by": normalized_actor,
        "capability_hash": capability.capability_hash,
        "reconciled_at": normalized_at,
    }
    if persisted is None:
        raise V3ProjectionWorkerError(
            "dead-letter reconciliation audit readback is missing"
        )
    observed_audit = {
        "reconciliation_audit_hash": str(
            persisted["reconciliation_audit_hash"]
        ),
        "outbox_id": str(persisted["outbox_id"]),
        "source_order_id": str(persisted["source_order_id"]),
        "source_sequence": int(persisted["source_sequence"]),
        "previous_attempt_count": int(persisted["previous_attempt_count"]),
        "action": str(persisted["action"]),
        "reason": str(persisted["reason"]),
        "reconciled_by": str(persisted["reconciled_by"]),
        "capability_hash": str(persisted["capability_hash"]),
        "reconciled_at": _stored_utc(
            persisted["reconciled_at"],
            "dead-letter reconciliation reconciled_at",
        ),
    }
    if observed_audit != expected_audit:
        raise V3ProjectionWorkerError(
            "dead-letter reconciliation audit readback differs"
        )
    return V3ProjectionDeadLetterRequeueResult(
        outbox_id=normalized_outbox_id,
        source_order_id=source_order_id,
        source_sequence=source_sequence,
        previous_attempt_count=previous_attempt_count,
        reconciliation_audit_hash=audit_hash,
    )


def lease_v3_projection_outbox(
    connection: Any,
    *,
    worker_id: str,
    now: datetime,
    lease_seconds: int,
    runtime_override: _V3ProjectionWorkerTestCapability | None = None,
) -> V3ProjectionOutboxLease | None:
    normalized_worker = _text(worker_id, "worker_id")
    normalized_now = _aware_utc_naive(now, "now")
    _require_runtime_enabled(runtime_override)
    if type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
        raise ValueError("lease_seconds must be an int between 1 and 3600")
    active = _active(connection)
    validate_v3_projection_outbox_schema(active)
    row = active.execute(
        text(
            """
            SELECT candidate.outbox_sequence, candidate.outbox_id,
                   candidate.projection_id, candidate.projection_payload_hash,
                   candidate.canonical_payload_hash, candidate.payload_json,
                   candidate.source_order_id, candidate.source_transition_id,
                   candidate.source_sequence, candidate.attempt_count,
                   baseline.baseline_sequence,
                   baseline.baseline_transition_id,
                   baseline.baseline_order_state_hash,
                   baseline.reconciliation_evidence_hash,
                   baseline.baseline_audit_hash,
                   baseline.reconciled_by AS baseline_reconciled_by,
                   baseline.reconciled_at AS baseline_reconciled_at
            FROM st_execution_projection_outbox_v2 candidate
            LEFT JOIN st_execution_projection_order_baseline_v3 baseline
              ON BINARY baseline.source_order_id
                   = BINARY candidate.source_order_id
            WHERE (
                    (
                        candidate.status = 'PENDING'
                        AND candidate.available_at <= :now
                    ) OR (
                        candidate.status = 'LEASED'
                        AND candidate.lease_until <= :now
                    )
                  )
              AND candidate.source_sequence
                    > COALESCE(baseline.baseline_sequence, 0)
              AND NOT EXISTS (
                    SELECT 1
                    FROM st_execution_projection_outbox_v2 predecessor
                    WHERE BINARY predecessor.source_order_id
                            = BINARY candidate.source_order_id
                      AND predecessor.source_sequence < candidate.source_sequence
                      AND predecessor.source_sequence
                            > COALESCE(baseline.baseline_sequence, 0)
                      AND (
                            predecessor.status <> 'PUBLISHED'
                            OR predecessor.status IS NULL
                      )
              )
              AND (
                    candidate.source_sequence
                        = COALESCE(baseline.baseline_sequence, 0) + 1
                    OR EXISTS (
                        SELECT 1
                        FROM st_execution_projection_outbox_v2 immediate_predecessor
                        WHERE BINARY immediate_predecessor.source_order_id
                                = BINARY candidate.source_order_id
                          AND immediate_predecessor.source_sequence
                                = candidate.source_sequence - 1
                          AND immediate_predecessor.source_sequence
                                > COALESCE(baseline.baseline_sequence, 0)
                          AND immediate_predecessor.status = 'PUBLISHED'
                    )
              )
              AND (
                    SELECT COUNT(*)
                    FROM st_execution_projection_outbox_v2 published_history
                    WHERE BINARY published_history.source_order_id
                            = BINARY candidate.source_order_id
                      AND published_history.source_sequence BETWEEN
                            COALESCE(baseline.baseline_sequence, 0) + 1
                            AND candidate.source_sequence - 1
                      AND published_history.status = 'PUBLISHED'
              ) = candidate.source_sequence
                    - COALESCE(baseline.baseline_sequence, 0) - 1
            ORDER BY candidate.outbox_sequence
            LIMIT 1 FOR UPDATE
            """
        ),
        {"now": normalized_now},
    ).mappings().first()
    if row is None:
        return None
    _validate_candidate_baseline(row)
    outbox_id = str(row["outbox_id"])
    attempt_count = int(row["attempt_count"] or 0) + 1
    lease_until = normalized_now + timedelta(seconds=lease_seconds)
    lease_token = hashlib.sha256(
        f"{normalized_worker}:{outbox_id}:{attempt_count}:{uuid.uuid4().hex}".encode(
            "utf-8"
        )
    ).hexdigest()
    result = active.execute(
        text(
            """
            UPDATE st_execution_projection_outbox_v2
            SET status = 'LEASED', attempt_count = :attempt_count,
                lease_owner = :lease_owner, lease_token = :lease_token,
                lease_until = :lease_until, updated_at = :now
            WHERE outbox_id = :outbox_id
              AND (
                    (status = 'PENDING' AND available_at <= :now)
                 OR (status = 'LEASED' AND lease_until <= :now)
              )
            """
        ),
        {
            "outbox_id": outbox_id,
            "attempt_count": attempt_count,
            "lease_owner": normalized_worker,
            "lease_token": lease_token,
            "lease_until": lease_until,
            "now": normalized_now,
        },
    )
    if int(result.rowcount or 0) != 1:
        raise V3ProjectionWorkerError("outbox lease CAS failed")
    return V3ProjectionOutboxLease(
        outbox_sequence=int(row["outbox_sequence"]),
        outbox_id=outbox_id,
        projection_id=str(row["projection_id"]),
        projection_payload_hash=str(row["projection_payload_hash"]),
        canonical_payload_hash=str(row["canonical_payload_hash"]),
        payload_json=str(row["payload_json"]),
        source_order_id=str(row["source_order_id"]),
        source_transition_id=str(row["source_transition_id"]),
        source_sequence=int(row["source_sequence"]),
        attempt_count=attempt_count,
        lease_owner=normalized_worker,
        lease_token=lease_token,
        lease_until=lease_until,
    )


def _acknowledge_projection(
    connection: Any,
    lease: V3ProjectionOutboxLease,
    *,
    published_at: datetime,
) -> None:
    active = _active(connection)
    normalized_at = _aware_utc_naive(published_at, "published_at")
    result = active.execute(
        text(
            """
            UPDATE st_execution_projection_outbox_v2
            SET status = 'PUBLISHED', published_at = :published_at,
                lease_owner = NULL, lease_token = NULL, lease_until = NULL,
                last_error = NULL, updated_at = :published_at
            WHERE outbox_id = :outbox_id AND status = 'LEASED'
              AND lease_owner = :lease_owner AND lease_token = :lease_token
            """
        ),
        {
            "outbox_id": lease.outbox_id,
            "lease_owner": lease.lease_owner,
            "lease_token": lease.lease_token,
            "published_at": normalized_at,
        },
    )
    if int(result.rowcount or 0) != 1:
        raise V3ProjectionWorkerError("outbox acknowledgement CAS failed")
    checkpoint = active.execute(
        text(
            "SELECT last_outbox_sequence FROM "
            "st_execution_projection_worker_checkpoint_v3 "
            "WHERE worker_id = :worker_id FOR UPDATE"
        ),
        {"worker_id": lease.lease_owner},
    ).scalar()
    if checkpoint is None:
        active.execute(
            text(
                """
                INSERT INTO st_execution_projection_worker_checkpoint_v3 (
                    worker_id, last_outbox_sequence, last_outbox_id,
                    last_projection_id, updated_at
                ) VALUES (
                    :worker_id, :outbox_sequence, :outbox_id,
                    :projection_id, :updated_at
                )
                """
            ),
            {
                "worker_id": lease.lease_owner,
                "outbox_sequence": lease.outbox_sequence,
                "outbox_id": lease.outbox_id,
                "projection_id": lease.projection_id,
                "updated_at": normalized_at,
            },
        )
    elif lease.outbox_sequence > int(checkpoint):
        updated = active.execute(
            text(
                """
                UPDATE st_execution_projection_worker_checkpoint_v3
                SET last_outbox_sequence = :outbox_sequence,
                    last_outbox_id = :outbox_id,
                    last_projection_id = :projection_id,
                    updated_at = :updated_at
                WHERE worker_id = :worker_id
                  AND last_outbox_sequence = :previous_sequence
                """
            ),
            {
                "worker_id": lease.lease_owner,
                "previous_sequence": int(checkpoint),
                "outbox_sequence": lease.outbox_sequence,
                "outbox_id": lease.outbox_id,
                "projection_id": lease.projection_id,
                "updated_at": normalized_at,
            },
        )
        if int(updated.rowcount or 0) != 1:
            raise V3ProjectionWorkerError("worker checkpoint CAS failed")


def _record_projection_failure(
    connection: Any,
    lease: V3ProjectionOutboxLease,
    *,
    failed_at: datetime,
    error: str,
    max_attempts: int,
    retry_delay_seconds: int,
) -> V3ProjectionWorkerStatus:
    active = _active(connection)
    normalized_at = _aware_utc_naive(failed_at, "failed_at")
    dead = lease.attempt_count >= max_attempts
    status = "DEAD_LETTER" if dead else "PENDING"
    available_at = normalized_at + timedelta(seconds=retry_delay_seconds)
    result = active.execute(
        text(
            """
            UPDATE st_execution_projection_outbox_v2
            SET status = :status, available_at = :available_at,
                lease_owner = NULL, lease_token = NULL, lease_until = NULL,
                last_error = :last_error, updated_at = :updated_at
            WHERE outbox_id = :outbox_id AND status = 'LEASED'
              AND lease_owner = :lease_owner AND lease_token = :lease_token
            """
        ),
        {
            "status": status,
            "available_at": available_at,
            "last_error": str(error)[:1000],
            "updated_at": normalized_at,
            "outbox_id": lease.outbox_id,
            "lease_owner": lease.lease_owner,
            "lease_token": lease.lease_token,
        },
    )
    if int(result.rowcount or 0) != 1:
        raise V3ProjectionWorkerError("outbox failure-state CAS failed")
    return (
        V3ProjectionWorkerStatus.DEAD_LETTER
        if dead
        else V3ProjectionWorkerStatus.RETRY_SCHEDULED
    )


def run_v3_projection_worker_once(
    ports: V3ProjectionWorkerPorts,
    *,
    worker_id: str,
    now: datetime,
    lease_seconds: int = 30,
    max_attempts: int = 5,
    retry_delay_seconds: int = 10,
    after_projection_commit: Callable[[V3ProjectionOutboxLease], None] | None = None,
    runtime_override: _V3ProjectionWorkerTestCapability | None = None,
) -> V3ProjectionWorkerResult:
    """Lease, independently project, then acknowledge one committed event."""

    if type(ports) is not V3ProjectionWorkerPorts:
        raise TypeError("ports must be exactly V3ProjectionWorkerPorts")
    if type(max_attempts) is not int or max_attempts < 1:
        raise ValueError("max_attempts must be a positive int")
    if type(retry_delay_seconds) is not int or retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be a non-negative int")
    normalized_now = _aware_utc_naive(now, "now").replace(tzinfo=timezone.utc)
    _require_runtime_enabled(runtime_override)
    with ports.outbox_transaction() as connection:
        lease = lease_v3_projection_outbox(
            connection,
            worker_id=worker_id,
            now=normalized_now,
            lease_seconds=lease_seconds,
            runtime_override=runtime_override,
        )
    if lease is None:
        return V3ProjectionWorkerResult(
            status=V3ProjectionWorkerStatus.IDLE,
            outbox_id=None,
            projection_id=None,
            attempt_count=0,
        )

    try:
        observed_hash = hashlib.sha256(lease.payload_json.encode("utf-8")).hexdigest()
        if observed_hash != lease.canonical_payload_hash:
            raise V3ProjectionOutboxError("outbox canonical payload hash differs")
        projection = projection_from_payload(lease.payload_json)
        if projection.projection_id != lease.projection_id:
            raise V3ProjectionOutboxError("leased projection identity differs")
        if projection.payload_hash != lease.projection_payload_hash:
            raise V3ProjectionOutboxError("leased projection payload hash differs")
        if projection.source_transition_id != lease.source_transition_id:
            raise V3ProjectionOutboxError("leased transition identity differs")
        if projection.source_order_id != lease.source_order_id:
            raise V3ProjectionOutboxError("leased order identity differs")
        if projection.source_sequence != lease.source_sequence:
            raise V3ProjectionOutboxError("leased source sequence differs")
        with ports.projection_transaction() as projection_connection:
            subscriber_result = _validate_subscriber_result(
                ports.subscriber(
                    projection_connection,
                    projection,
                    applied_at=normalized_now,
                ),
                projection,
            )
    except Exception as exc:
        with ports.outbox_transaction() as failure_connection:
            status = _record_projection_failure(
                failure_connection,
                lease,
                failed_at=normalized_now,
                error=f"{type(exc).__name__}: {exc}",
                max_attempts=max_attempts,
                retry_delay_seconds=retry_delay_seconds,
            )
        return V3ProjectionWorkerResult(
            status=status,
            outbox_id=lease.outbox_id,
            projection_id=lease.projection_id,
            attempt_count=lease.attempt_count,
            error=f"{type(exc).__name__}: {exc}",
        )

    # This hook runs only after the V3 transaction has committed.  A raised
    # exception intentionally leaves the lease unacknowledged for expiry and
    # idempotent subscriber replay after worker restart.
    if after_projection_commit is not None:
        after_projection_commit(lease)
    with ports.outbox_transaction() as acknowledgement_connection:
        _acknowledge_projection(
            acknowledgement_connection,
            lease,
            published_at=normalized_now,
        )
    return V3ProjectionWorkerResult(
        status=V3ProjectionWorkerStatus.PUBLISHED,
        outbox_id=lease.outbox_id,
        projection_id=lease.projection_id,
        attempt_count=lease.attempt_count,
        subscriber_result=subscriber_result,
    )


__all__ = [
    "V3ProjectionBaselineResult",
    "V3ProjectionBaselineStatus",
    "V3ProjectionDeadLetterRequeueResult",
    "V3ProjectionOutboxLease",
    "V3ProjectionWorkerError",
    "V3ProjectionWorkerDisabledError",
    "V3ProjectionWorkerPorts",
    "V3ProjectionWorkerResult",
    "V3ProjectionWorkerStatus",
    "lease_v3_projection_outbox",
    "register_v3_projection_order_baseline",
    "requeue_v3_projection_dead_letter",
    "run_v3_projection_worker_once",
]
