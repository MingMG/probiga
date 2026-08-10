"""Canonical V2 commit coordinator, deliberately disabled by default.

This boundary owns neither an engine nor a transaction.  It serializes one
already validated session-gated snapshot batch onto the existing V2 account,
order, fact and evidence ledgers.  The caller supplies the active connection
and every mutation callback.  Exceptions escape unchanged so the caller's
outer transaction is the only rollback boundary.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import hmac
import json
import os
import secrets
from typing import Any, Callable

from sqlalchemy import text

from server.trading_core.execution.session_gate import (
    SessionGateReason,
    SessionGatedSnapshotBatchDecision,
    validate_session_gated_snapshot_batch_decision,
)
from server.trading_v2.execution_evidence_schema_gate import (
    V2EvidenceMaintenanceFenceError,
    assert_v2_evidence_maintenance_fence_inactive,
)


class CanonicalCommitDisabledError(RuntimeError):
    pass


class CanonicalCommitInvariantError(RuntimeError):
    pass


_ACTIVATION_CONSTRUCTION_CAPABILITY = object()
_ACTIVATION_SIGNING_KEY = secrets.token_bytes(32)
_RUNTIME_ENVIRONMENT_VARIABLE = "PROBIGA_RUNTIME_ENVIRONMENT"
_NON_PRODUCTION_ENVIRONMENTS = frozenset({"TEST", "CI"})
_MAXIMUM_ACTIVATION_LIFETIME = timedelta(hours=1)


def _system_utc_now() -> datetime:
    """Return the process host clock; callers may not supply gate time."""

    return datetime.now(timezone.utc)


def _runtime_environment() -> str:
    """Resolve the actual process environment used by the activation gate."""

    return str(os.environ.get(_RUNTIME_ENVIRONMENT_VARIABLE) or "").strip().upper()


def _aware_utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name).lower()
    if len(normalized) != 64 or any(c not in "0123456789abcdef" for c in normalized):
        raise ValueError(f"{field_name} must be a lowercase sha256")
    return normalized


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class AcceptanceActivationToken:
    """Process-bound capability minted only by the trusted acceptance path.

    Direct construction is intentionally unavailable.  The issuer MAC is
    keyed by process-local entropy, so copying public fields or recomputing
    ``token_hash`` cannot create a capability accepted by this process.
    """

    token_id: str
    acceptance_report_hash: str
    environment: str
    issued_at: datetime
    expires_at: datetime
    scope: str = "V2_CANONICAL_PAPER_COMMIT"
    acceptance_status: str = "PASSED"
    production_activation_allowed: bool = False
    token_hash: str
    _issuer_mac: str = field(repr=False)

    def __init__(
        self,
        *,
        token_id: str,
        acceptance_report_hash: str,
        environment: str,
        issued_at: datetime,
        expires_at: datetime,
        scope: str = "V2_CANONICAL_PAPER_COMMIT",
        acceptance_status: str = "PASSED",
        production_activation_allowed: bool = False,
        _construction_capability: object | None = None,
    ) -> None:
        if _construction_capability is not _ACTIVATION_CONSTRUCTION_CAPABILITY:
            raise CanonicalCommitDisabledError(
                "activation tokens require the trusted in-process issuer"
            )
        normalized_token_id = _text(token_id, "token_id")
        report_hash = _sha256(
            acceptance_report_hash,
            "acceptance_report_hash",
        )
        normalized_environment = _text(environment, "environment").upper()
        if normalized_environment not in _NON_PRODUCTION_ENVIRONMENTS:
            raise ValueError("activation token environment must be TEST or CI")
        if scope != "V2_CANONICAL_PAPER_COMMIT":
            raise ValueError("activation token scope is invalid")
        if acceptance_status != "PASSED":
            raise ValueError("activation token requires PASSED acceptance")
        if type(production_activation_allowed) is not bool:
            raise TypeError("production_activation_allowed must be bool")
        if production_activation_allowed:
            raise ValueError("canonical commit token can never enable production")
        normalized_issued_at = _aware_utc(issued_at, "issued_at")
        normalized_expires_at = _aware_utc(expires_at, "expires_at")
        lifetime = normalized_expires_at - normalized_issued_at
        if lifetime <= timedelta(0):
            raise ValueError("activation token must expire after issue time")
        if lifetime > _MAXIMUM_ACTIVATION_LIFETIME:
            raise ValueError("activation token lifetime exceeds one hour")
        token_hash = _digest(
            "v2.canonical-commit.acceptance-token.v2",
            {
                "token_id": normalized_token_id,
                "acceptance_report_hash": report_hash,
                "environment": normalized_environment,
                "issued_at": normalized_issued_at.isoformat(
                    timespec="microseconds"
                ),
                "expires_at": normalized_expires_at.isoformat(
                    timespec="microseconds"
                ),
                "scope": scope,
                "acceptance_status": acceptance_status,
                "production_activation_allowed": False,
            },
        )
        issuer_mac = hmac.new(
            _ACTIVATION_SIGNING_KEY,
            token_hash.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        object.__setattr__(self, "token_id", normalized_token_id)
        object.__setattr__(self, "acceptance_report_hash", report_hash)
        object.__setattr__(self, "environment", normalized_environment)
        object.__setattr__(self, "issued_at", normalized_issued_at)
        object.__setattr__(self, "expires_at", normalized_expires_at)
        object.__setattr__(self, "scope", scope)
        object.__setattr__(self, "acceptance_status", acceptance_status)
        object.__setattr__(self, "production_activation_allowed", False)
        object.__setattr__(self, "token_hash", token_hash)
        object.__setattr__(self, "_issuer_mac", issuer_mac)


def _issue_trusted_acceptance_activation_token(
    *,
    token_id: str,
    acceptance_report_hash: str,
    valid_for: timedelta = timedelta(minutes=15),
) -> AcceptanceActivationToken:
    """Mint a short-lived test capability from the private acceptance path.

    This intentionally remains a module-private integration hook until a
    concrete acceptance runner owns it.  Environment and issue time come from
    the process, never from the caller.
    """

    if type(valid_for) is not timedelta:
        raise TypeError("valid_for must be exactly timedelta")
    if valid_for <= timedelta(0) or valid_for > _MAXIMUM_ACTIVATION_LIFETIME:
        raise ValueError("valid_for must be greater than zero and at most one hour")
    environment = _runtime_environment()
    if environment not in _NON_PRODUCTION_ENVIRONMENTS:
        raise CanonicalCommitDisabledError(
            "trusted activation issuance requires the TEST or CI process environment"
        )
    issued_at = _aware_utc(_system_utc_now(), "system UTC clock")
    return AcceptanceActivationToken(
        token_id=token_id,
        acceptance_report_hash=acceptance_report_hash,
        environment=environment,
        issued_at=issued_at,
        expires_at=issued_at + valid_for,
        _construction_capability=_ACTIVATION_CONSTRUCTION_CAPABILITY,
    )


@dataclass(frozen=True, slots=True)
class SharedCapacityReservation:
    source_receipt_hash: str
    instrument_id: str
    snapshot_id: str
    shared_cap_before: int
    reserved_quantity: int
    shared_cap_after: int
    allocations: tuple[tuple[str, int], ...]
    reservation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        receipt_hash = _sha256(self.source_receipt_hash, "source_receipt_hash")
        instrument_id = _text(self.instrument_id, "instrument_id")
        snapshot_id = _text(self.snapshot_id, "snapshot_id")
        for name in (
            "shared_cap_before",
            "reserved_quantity",
            "shared_cap_after",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative exact int")
        if self.shared_cap_before - self.reserved_quantity != self.shared_cap_after:
            raise ValueError("reservation capacity transition is inconsistent")
        if type(self.allocations) is not tuple or not self.allocations:
            raise TypeError("allocations must be a non-empty exact tuple")
        normalized: list[tuple[str, int]] = []
        seen: set[str] = set()
        for raw in self.allocations:
            if type(raw) is not tuple or len(raw) != 2:
                raise TypeError("each reservation allocation must be a pair")
            order_id = _text(raw[0], "allocation order_id")
            quantity = raw[1]
            if type(quantity) is not int or quantity < 0:
                raise ValueError("allocation quantity must be a non-negative int")
            if order_id in seen:
                raise ValueError("reservation order ids must be unique")
            seen.add(order_id)
            normalized.append((order_id, quantity))
        if sum(quantity for _, quantity in normalized) != self.reserved_quantity:
            raise ValueError("reservation allocations do not sum to reserved quantity")
        object.__setattr__(self, "source_receipt_hash", receipt_hash)
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "snapshot_id", snapshot_id)
        object.__setattr__(self, "allocations", tuple(normalized))
        object.__setattr__(
            self,
            "reservation_hash",
            _digest(
                "v2.canonical-commit.shared-capacity-reservation.v1",
                {
                    "source_receipt_hash": receipt_hash,
                    "instrument_id": instrument_id,
                    "snapshot_id": snapshot_id,
                    "shared_cap_before": self.shared_cap_before,
                    "reserved_quantity": self.reserved_quantity,
                    "shared_cap_after": self.shared_cap_after,
                    "allocations": normalized,
                },
            ),
        )


class SharedCapacityReservationStatus(str, Enum):
    RESERVED = "RESERVED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class SharedCapacityReservationResult:
    status: SharedCapacityReservationStatus
    reservation_hash: str

    def __post_init__(self) -> None:
        if type(self.status) is not SharedCapacityReservationStatus:
            raise TypeError("reservation result status is invalid")
        object.__setattr__(
            self,
            "reservation_hash",
            _sha256(self.reservation_hash, "reservation_hash"),
        )


@dataclass(frozen=True, slots=True)
class CanonicalCommitReceipt:
    account_id: str
    order_ids: tuple[str, ...]
    session_decision_hash: str
    reservation_hash: str
    reservation_status: SharedCapacityReservationStatus
    fact_result: Any
    evidence_result: Any
    outbox_result: Any
    activation_token_hash: str
    production_activation_allowed: bool = False


ReservationCallback = Callable[
    [Any, SharedCapacityReservation],
    SharedCapacityReservationResult,
]
FactMutationCallback = Callable[
    [Any, SessionGatedSnapshotBatchDecision, SharedCapacityReservation],
    Any,
]
EvidenceAppendCallback = Callable[
    [Any, SessionGatedSnapshotBatchDecision, SharedCapacityReservation, Any],
    Any,
]
OutboxAppendCallback = Callable[
    [Any, SessionGatedSnapshotBatchDecision, Any, Any],
    Any,
]


def _active_connection(connection: Any) -> Any:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise CanonicalCommitInvariantError(
            "canonical commit requires a caller-owned connection"
        )
    probe = getattr(connection, "in_transaction", None)
    if not callable(probe) or probe() is not True:
        raise CanonicalCommitInvariantError(
            "canonical commit requires an active caller-owned transaction"
        )
    return connection


def _validate_activation(
    token: AcceptanceActivationToken | None,
) -> AcceptanceActivationToken:
    if token is None:
        raise CanonicalCommitDisabledError(
            "canonical V2 commit is runtime-disabled without an explicit "
            "acceptance token"
        )
    if type(token) is not AcceptanceActivationToken:
        raise CanonicalCommitDisabledError("activation token type is invalid")
    try:
        rebuilt = AcceptanceActivationToken(
            token_id=token.token_id,
            acceptance_report_hash=token.acceptance_report_hash,
            environment=token.environment,
            issued_at=token.issued_at,
            expires_at=token.expires_at,
            scope=token.scope,
            acceptance_status=token.acceptance_status,
            production_activation_allowed=(
                token.production_activation_allowed
            ),
            _construction_capability=_ACTIVATION_CONSTRUCTION_CAPABILITY,
        )
        public_fields_match = all(
            getattr(rebuilt, field_name) == getattr(token, field_name)
            for field_name in (
                "token_id",
                "acceptance_report_hash",
                "environment",
                "issued_at",
                "expires_at",
                "scope",
                "acceptance_status",
                "production_activation_allowed",
                "token_hash",
            )
        )
        issuer_mac_matches = hmac.compare_digest(
            rebuilt._issuer_mac,
            token._issuer_mac,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise CanonicalCommitDisabledError(
            "activation token structure is invalid"
        ) from exc
    if not public_fields_match or not issuer_mac_matches:
        raise CanonicalCommitDisabledError("activation token was tampered")
    runtime_environment = _runtime_environment()
    if runtime_environment not in _NON_PRODUCTION_ENVIRONMENTS:
        raise CanonicalCommitDisabledError(
            "canonical commit requires the TEST or CI process environment"
        )
    if token.environment != runtime_environment:
        raise CanonicalCommitDisabledError(
            "activation token does not match the process environment"
        )
    now = _aware_utc(_system_utc_now(), "system UTC clock")
    if now < token.issued_at or now >= token.expires_at:
        raise CanonicalCommitDisabledError("activation token is not currently valid")
    return token


def _validate_decision_and_reservation(
    decision: SessionGatedSnapshotBatchDecision,
    reservation: SharedCapacityReservation,
) -> tuple[str, ...]:
    if type(decision) is not SessionGatedSnapshotBatchDecision:
        raise TypeError(
            "session_decision must be exactly SessionGatedSnapshotBatchDecision"
        )
    validate_session_gated_snapshot_batch_decision(decision)
    if decision.gate_reason != SessionGateReason.NONE:
        raise CanonicalCommitInvariantError("only an ungated ACTIVE decision may commit")
    if str(decision.assessment.state.value) != "ACTIVE":
        raise CanonicalCommitInvariantError("session decision is not ACTIVE")
    if type(reservation) is not SharedCapacityReservation:
        raise TypeError("reservation must be exactly SharedCapacityReservation")
    batch = decision.batch_result
    expected_allocations = tuple(
        (item.order_id, item.decision.fill_quantity)
        for item in batch.allocations
    )
    expected = (
        reservation.source_receipt_hash == batch.source_receipt_hash
        and reservation.instrument_id == batch.instrument_id
        and reservation.snapshot_id == batch.snapshot_id
        and reservation.shared_cap_before == batch.shared_liquidity_cap
        and reservation.reserved_quantity == batch.total_fill_quantity
        and reservation.shared_cap_after
        == batch.shared_liquidity_cap - batch.total_fill_quantity
        and reservation.allocations == expected_allocations
    )
    if not expected:
        raise CanonicalCommitInvariantError(
            "shared-capacity reservation does not bind the session batch"
        )
    return tuple(sorted(order_id for order_id, _ in expected_allocations))


def _lock_orders(
    connection: Any,
    order_ids: tuple[str, ...],
    account_id: str,
) -> None:
    binds = ", ".join(f":order_id_{index}" for index in range(len(order_ids)))
    parameters = {
        f"order_id_{index}": order_id
        for index, order_id in enumerate(order_ids)
    }
    rows = tuple(
        (str(row["order_id"]), str(row["account_id"]))
        for row in connection.execute(
            text(
                "SELECT order_id, account_id FROM st_order_v2 "
                f"WHERE order_id IN ({binds}) ORDER BY order_id FOR UPDATE"
            ),
            parameters,
        ).mappings()
    )
    if tuple(order_id for order_id, _ in rows) != order_ids:
        raise CanonicalCommitInvariantError(
            "canonical order lock set is missing or differs from the batch"
        )
    if any(stored_account_id != account_id for _, stored_account_id in rows):
        raise CanonicalCommitInvariantError(
            "canonical order lock set belongs to another V2 account"
        )


def _lock_account(connection: Any, account_id: str) -> None:
    row = connection.execute(
        text(
            "SELECT account_id, real_trading_enabled "
            "FROM st_trade_account_v2 WHERE account_id = :account_id FOR UPDATE"
        ),
        {"account_id": account_id},
    ).mappings().first()
    if row is None or str(row["account_id"]) != account_id:
        raise CanonicalCommitInvariantError("canonical V2 account is missing")
    if bool(row["real_trading_enabled"]):
        raise CanonicalCommitInvariantError("real trading must remain disabled")


def coordinate_v2_canonical_commit(
    connection: Any,
    *,
    account_id: str,
    session_decision: SessionGatedSnapshotBatchDecision,
    reservation: SharedCapacityReservation,
    reserve_shared_capacity: ReservationCallback,
    mutate_facts: FactMutationCallback,
    append_evidence: EvidenceAppendCallback,
    append_transition_outbox: OutboxAppendCallback,
    activation_token: AcceptanceActivationToken | None = None,
) -> CanonicalCommitReceipt:
    """Run order -> account -> capacity -> facts -> evidence -> outbox.

    This function never opens or commits a transaction.  Any exception is
    intentionally propagated so the caller rolls back the entire V2 unit.
    """

    active = _active_connection(connection)
    token = _validate_activation(activation_token)
    normalized_account_id = _text(account_id, "account_id")
    order_ids = _validate_decision_and_reservation(session_decision, reservation)
    for callback, name in (
        (reserve_shared_capacity, "reserve_shared_capacity"),
        (mutate_facts, "mutate_facts"),
        (append_evidence, "append_evidence"),
        (append_transition_outbox, "append_transition_outbox"),
    ):
        if not callable(callback):
            raise TypeError(f"{name} must be callable")

    try:
        assert_v2_evidence_maintenance_fence_inactive(active)
    except V2EvidenceMaintenanceFenceError as exc:
        raise CanonicalCommitInvariantError(
            "canonical commit is blocked by the V2 evidence maintenance fence"
        ) from exc

    _lock_orders(active, order_ids, normalized_account_id)
    _lock_account(active, normalized_account_id)
    reservation_result = reserve_shared_capacity(active, reservation)
    if type(reservation_result) is not SharedCapacityReservationResult:
        raise CanonicalCommitInvariantError(
            "capacity callback returned an invalid reservation result"
        )
    if reservation_result.reservation_hash != reservation.reservation_hash:
        raise CanonicalCommitInvariantError(
            "persisted shared-capacity reservation hash differs"
        )
    fact_result = mutate_facts(active, session_decision, reservation)
    if fact_result is None:
        raise CanonicalCommitInvariantError(
            "canonical fact callback returned no durable mutation receipt"
        )
    evidence_result = append_evidence(
        active,
        session_decision,
        reservation,
        fact_result,
    )
    if evidence_result is None:
        raise CanonicalCommitInvariantError(
            "evidence callback returned no durable append receipt"
        )
    outbox_result = append_transition_outbox(
        active,
        session_decision,
        fact_result,
        evidence_result,
    )
    if outbox_result is None:
        raise CanonicalCommitInvariantError(
            "transition outbox callback returned no durable append receipt"
        )
    return CanonicalCommitReceipt(
        account_id=normalized_account_id,
        order_ids=order_ids,
        session_decision_hash=session_decision.decision_hash,
        reservation_hash=reservation.reservation_hash,
        reservation_status=reservation_result.status,
        fact_result=fact_result,
        evidence_result=evidence_result,
        outbox_result=outbox_result,
        activation_token_hash=token.token_hash,
        production_activation_allowed=False,
    )


__all__ = [
    "AcceptanceActivationToken",
    "CanonicalCommitDisabledError",
    "CanonicalCommitInvariantError",
    "CanonicalCommitReceipt",
    "SharedCapacityReservation",
    "SharedCapacityReservationResult",
    "SharedCapacityReservationStatus",
    "coordinate_v2_canonical_commit",
]
