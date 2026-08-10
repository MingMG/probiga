"""Strict prepared-commit seam for the existing V2 execution transaction.

The legacy canonical coordinator accepts only a complete
``SessionGatedSnapshotBatchDecision``.  The live V2 paper executor instead
owns one already-persisted order mutation at a time.  This module bridges
that exact seam without manufacturing a batch decision and without opening,
committing, or rolling back a transaction.

The seam is deliberately unavailable in production.  A short-lived,
process-bound TEST/CI capability and an externally verified V3 baseline hash
are both required.  The caller must run :func:`preflight_prepared_commit`
before taking the order/account locks and then pass the returned transaction
context back to :func:`commit_prepared_canonical_execution`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Callable

from server.integrations.v2_accounting_evidence_writer import (
    append_fill_accounting_outcome,
)
from server.integrations.v2_execution_evidence_authority import (
    AuthorityReceiptReference,
    EvidenceAuthorityVerifier,
)
from server.integrations.v2_execution_evidence_writer import append_evidence
from server.integrations.v3_execution_projection.projector import (
    V3ExecutionPlanBinding,
    V3ExecutionProjection,
    project_execution_result,
)
from server.integrations.v3_execution_projection_outbox import (
    append_v3_transition_outbox,
)
from server.trading_core.contracts import (
    ExecutionEventKind,
    OrderStatus as CoreOrderStatus,
)
from server.trading_core.execution import (
    OrderTransitionReceipt,
    validate_order_transition_receipt,
)
from server.trading_v2.accounting_evidence import FillAccountingOutcome
from server.trading_v2.domain import OrderStatus
from server.trading_v2.execution_evidence import (
    CashEventBinding,
    FillExecutionEvidence,
    MarketCalendarEvidence,
    OrderTransitionEvidence,
    OrderTransitionKind,
    QuoteReceiptEvidence,
)
from server.trading_v2.execution_evidence_schema_gate import (
    V2EvidenceMaintenanceFenceError,
    assert_v2_evidence_maintenance_fence_inactive,
)
from server.trading_v2.oms import ACTIVE_TRANSITIONS

from .coordinator import (
    AcceptanceActivationToken,
    CanonicalCommitDisabledError,
    CanonicalCommitInvariantError,
    _issue_trusted_acceptance_activation_token,
    _validate_activation,
)


PREPARED_COMMIT_RUNTIME_ENABLED = False
PRODUCTION_ACTIVATION_ALLOWED = False
_CUTOVER_CONSTRUCTION_CAPABILITY = object()


def _text(value: object, field_name: str, *, maximum: int = 256) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be exactly str")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    if len(normalized) > maximum:
        raise ValueError(f"{field_name} exceeds {maximum} characters")
    return normalized


def _optional_text(
    value: object,
    field_name: str,
    *,
    maximum: int = 256,
) -> str | None:
    if value is None:
        return None
    return _text(value, field_name, maximum=maximum)


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name, maximum=64).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256")
    return normalized


def _aware_utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _active_connection(connection: Any) -> tuple[Any, Any]:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise CanonicalCommitInvariantError(
            "prepared commit requires a caller-owned connection"
        )
    in_transaction = getattr(connection, "in_transaction", None)
    get_transaction = getattr(connection, "get_transaction", None)
    if (
        not callable(in_transaction)
        or in_transaction() is not True
        or not callable(get_transaction)
    ):
        raise CanonicalCommitInvariantError(
            "prepared commit requires an active identifiable caller transaction"
        )
    transaction = get_transaction()
    if transaction is None:
        raise CanonicalCommitInvariantError(
            "prepared commit cannot identify the caller transaction"
        )
    return connection, transaction


@dataclass(frozen=True, slots=True)
class V3BaselineExternalAttestation:
    """Exact externally recorded baseline equality used by the cutover gate."""

    expected_manifest_hash: str
    verified_manifest_hash: str
    verification_report_hash: str
    external_trusted_hash_recorded: bool
    production_activation_allowed: bool = False

    def __post_init__(self) -> None:
        expected = _sha256(
            self.expected_manifest_hash,
            "expected_manifest_hash",
        )
        verified = _sha256(
            self.verified_manifest_hash,
            "verified_manifest_hash",
        )
        report = _sha256(
            self.verification_report_hash,
            "verification_report_hash",
        )
        if expected != verified:
            raise CanonicalCommitDisabledError(
                "expected and externally verified V3 baseline hashes differ"
            )
        if type(self.external_trusted_hash_recorded) is not bool:
            raise TypeError("external_trusted_hash_recorded must be exactly bool")
        if self.external_trusted_hash_recorded is not True:
            raise CanonicalCommitDisabledError(
                "the externally trusted V3 baseline hash is not recorded"
            )
        if type(self.production_activation_allowed) is not bool:
            raise TypeError("production_activation_allowed must be exactly bool")
        if self.production_activation_allowed:
            raise CanonicalCommitDisabledError(
                "prepared canonical commit cannot enable production"
            )
        object.__setattr__(self, "expected_manifest_hash", expected)
        object.__setattr__(self, "verified_manifest_hash", verified)
        object.__setattr__(self, "verification_report_hash", report)
        object.__setattr__(self, "production_activation_allowed", False)


@dataclass(frozen=True, slots=True)
class CanonicalMechanicalTransition:
    """One state change that the existing V2 executor actually persisted."""

    order_id: str
    account_id: str
    from_status: OrderStatus
    to_status: OrderStatus
    previous_filled_quantity: int
    next_filled_quantity: int
    previous_waiting_reason: str | None
    next_waiting_reason: str | None
    transition_kind: OrderTransitionKind
    source_event_type: str
    source_event_id: str
    source_event_hash: str
    occurred_at: datetime
    related_fill_id: str | None = None

    def __post_init__(self) -> None:
        order_id = _text(self.order_id, "order_id", maximum=64)
        account_id = _text(self.account_id, "account_id", maximum=64)
        if type(self.from_status) is not OrderStatus:
            raise TypeError("from_status must be exactly V2 OrderStatus")
        if type(self.to_status) is not OrderStatus:
            raise TypeError("to_status must be exactly V2 OrderStatus")
        if type(self.transition_kind) is not OrderTransitionKind:
            raise TypeError("transition_kind must be exactly OrderTransitionKind")
        for field_name in (
            "previous_filled_quantity",
            "next_filled_quantity",
        ):
            value = getattr(self, field_name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{field_name} must be a non-negative exact int")
        previous_waiting = _optional_text(
            self.previous_waiting_reason,
            "previous_waiting_reason",
            maximum=40,
        )
        next_waiting = _optional_text(
            self.next_waiting_reason,
            "next_waiting_reason",
            maximum=40,
        )
        fill_id = _optional_text(
            self.related_fill_id,
            "related_fill_id",
            maximum=64,
        )
        if self.transition_kind is OrderTransitionKind.FILL_APPLIED:
            if (
                fill_id is None
                or self.next_filled_quantity <= self.previous_filled_quantity
                or self.to_status
                not in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}
            ):
                raise CanonicalCommitInvariantError(
                    "FILL_APPLIED mechanical transition is inconsistent"
                )
        elif (
            fill_id is not None
            or self.next_filled_quantity != self.previous_filled_quantity
        ):
            raise CanonicalCommitInvariantError(
                "only a fill transition may bind a fill or increase quantity"
            )
        if self.from_status != self.to_status:
            if self.to_status not in ACTIVE_TRANSITIONS[self.from_status]:
                raise CanonicalCommitInvariantError(
                    "mechanical transition violates the V2 order state machine"
                )
        elif self.transition_kind is not OrderTransitionKind.WAITING_REASON_CHANGED:
            raise CanonicalCommitInvariantError(
                "same-status mechanical transition must change waiting reason"
            )
        if (
            self.transition_kind is OrderTransitionKind.WAITING_REASON_CHANGED
            and previous_waiting == next_waiting
        ):
            raise CanonicalCommitInvariantError(
                "waiting-reason transition did not change the reason"
            )
        source_type = _text(
            self.source_event_type,
            "source_event_type",
            maximum=80,
        )
        source_id = _text(self.source_event_id, "source_event_id", maximum=128)
        source_hash = _sha256(self.source_event_hash, "source_event_hash")
        occurred = _aware_utc(self.occurred_at, "occurred_at")
        object.__setattr__(self, "order_id", order_id)
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "previous_waiting_reason", previous_waiting)
        object.__setattr__(self, "next_waiting_reason", next_waiting)
        object.__setattr__(self, "related_fill_id", fill_id)
        object.__setattr__(self, "source_event_type", source_type)
        object.__setattr__(self, "source_event_id", source_id)
        object.__setattr__(self, "source_event_hash", source_hash)
        object.__setattr__(self, "occurred_at", occurred)


@dataclass(frozen=True, slots=True)
class CanonicalMechanicalMutation:
    """Durable receipt for facts already written by ``_execute_one``."""

    order_id: str
    account_id: str
    transitions: tuple[CanonicalMechanicalTransition, ...]
    result_status: str
    recorded_at: datetime
    fill_id: str | None = None
    mutation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        order_id = _text(self.order_id, "order_id", maximum=64)
        account_id = _text(self.account_id, "account_id", maximum=64)
        if type(self.transitions) is not tuple or not self.transitions:
            raise CanonicalCommitInvariantError(
                "mechanical mutation requires at least one real transition"
            )
        transitions: list[CanonicalMechanicalTransition] = []
        previous: CanonicalMechanicalTransition | None = None
        source_events: set[tuple[str, str]] = set()
        for raw in self.transitions:
            if type(raw) is not CanonicalMechanicalTransition:
                raise TypeError(
                    "transitions must contain exact CanonicalMechanicalTransition"
                )
            if raw.order_id != order_id or raw.account_id != account_id:
                raise CanonicalCommitInvariantError(
                    "mechanical transition identity differs from mutation"
                )
            source_key = (raw.source_event_type, raw.source_event_id)
            if source_key in source_events:
                raise CanonicalCommitInvariantError(
                    "mechanical mutation repeats a source event"
                )
            source_events.add(source_key)
            if previous is not None and (
                raw.from_status is not previous.to_status
                or raw.previous_filled_quantity
                != previous.next_filled_quantity
                or raw.previous_waiting_reason
                != previous.next_waiting_reason
                or raw.occurred_at < previous.occurred_at
            ):
                raise CanonicalCommitInvariantError(
                    "mechanical transition chain is discontinuous"
                )
            transitions.append(raw)
            previous = raw
        result_status = _text(self.result_status, "result_status", maximum=40)
        if result_status != transitions[-1].to_status.value:
            raise CanonicalCommitInvariantError(
                "result status differs from the final mechanical transition"
            )
        recorded = _aware_utc(self.recorded_at, "recorded_at")
        if recorded < transitions[-1].occurred_at:
            raise CanonicalCommitInvariantError(
                "mechanical mutation cannot be recorded before its transition"
            )
        fill_id = _optional_text(self.fill_id, "fill_id", maximum=64)
        fill_transitions = tuple(
            item
            for item in transitions
            if item.transition_kind is OrderTransitionKind.FILL_APPLIED
        )
        if fill_id is None and fill_transitions:
            raise CanonicalCommitInvariantError(
                "fill transition requires mutation fill_id"
            )
        if fill_id is not None and (
            len(fill_transitions) != 1
            or fill_transitions[0].related_fill_id != fill_id
        ):
            raise CanonicalCommitInvariantError(
                "mutation fill identity differs from its fill transition"
            )
        object.__setattr__(self, "order_id", order_id)
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "transitions", tuple(transitions))
        object.__setattr__(self, "result_status", result_status)
        object.__setattr__(self, "recorded_at", recorded)
        object.__setattr__(self, "fill_id", fill_id)
        object.__setattr__(
            self,
            "mutation_hash",
            _digest(
                "v2.canonical-prepared.mechanical-mutation.v1",
                {
                    "order_id": order_id,
                    "account_id": account_id,
                    "result_status": result_status,
                    "fill_id": fill_id,
                    "recorded_at": recorded,
                    "transitions": [
                        {
                            "from_status": item.from_status.value,
                            "to_status": item.to_status.value,
                            "previous_filled_quantity": (
                                item.previous_filled_quantity
                            ),
                            "next_filled_quantity": item.next_filled_quantity,
                            "previous_waiting_reason": (
                                item.previous_waiting_reason
                            ),
                            "next_waiting_reason": item.next_waiting_reason,
                            "transition_kind": item.transition_kind.value,
                            "source_event_type": item.source_event_type,
                            "source_event_id": item.source_event_id,
                            "source_event_hash": item.source_event_hash,
                            "occurred_at": item.occurred_at,
                            "related_fill_id": item.related_fill_id,
                        }
                        for item in transitions
                    ],
                },
            ),
        )


@dataclass(frozen=True, slots=True)
class PreparedCanonicalCommitBundle:
    """Fully reconstructed evidence/output for one mechanical mutation."""

    mutation_hash: str
    baseline_manifest_hash: str
    execution_evidence: tuple[Any, ...]
    projections: tuple[V3ExecutionProjection, ...]
    projection_bindings: tuple[V3ExecutionPlanBinding, ...]
    projection_receipts: tuple[OrderTransitionReceipt, ...]
    accounting_outcome: FillAccountingOutcome | None = None
    authority_verifier: EvidenceAuthorityVerifier | None = None
    instrument_rule_authority_reference: AuthorityReceiptReference | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "mutation_hash",
            _sha256(self.mutation_hash, "mutation_hash"),
        )
        object.__setattr__(
            self,
            "baseline_manifest_hash",
            _sha256(self.baseline_manifest_hash, "baseline_manifest_hash"),
        )
        if type(self.execution_evidence) is not tuple:
            raise TypeError("execution_evidence must be exactly tuple")
        allowed = (
            MarketCalendarEvidence,
            QuoteReceiptEvidence,
            FillExecutionEvidence,
            CashEventBinding,
            OrderTransitionEvidence,
        )
        if any(type(item) not in allowed for item in self.execution_evidence):
            raise TypeError("execution_evidence contains an unsupported exact type")
        if type(self.projections) is not tuple or any(
            type(item) is not V3ExecutionProjection for item in self.projections
        ):
            raise TypeError("projections must contain exact V3ExecutionProjection")
        if type(self.projection_bindings) is not tuple or any(
            type(item) is not V3ExecutionPlanBinding
            for item in self.projection_bindings
        ):
            raise TypeError(
                "projection_bindings must contain exact V3ExecutionPlanBinding"
            )
        if type(self.projection_receipts) is not tuple or any(
            type(item) is not OrderTransitionReceipt
            for item in self.projection_receipts
        ):
            raise TypeError(
                "projection_receipts must contain exact OrderTransitionReceipt"
            )
        if self.accounting_outcome is not None and type(
            self.accounting_outcome
        ) is not FillAccountingOutcome:
            raise TypeError("accounting_outcome must be exact FillAccountingOutcome")


PrepareCommitCallback = Callable[
    [CanonicalMechanicalMutation],
    PreparedCanonicalCommitBundle,
]


@dataclass(frozen=True, slots=True, init=False)
class CanonicalExecutionCutover:
    """Unforgeable TEST/CI capability carrying the trusted bundle builder."""

    activation_token: AcceptanceActivationToken
    baseline_attestation: V3BaselineExternalAttestation
    prepare_commit: PrepareCommitCallback = field(repr=False)

    def __init__(
        self,
        *,
        activation_token: AcceptanceActivationToken,
        baseline_attestation: V3BaselineExternalAttestation,
        prepare_commit: PrepareCommitCallback,
        _construction_capability: object | None = None,
    ) -> None:
        if _construction_capability is not _CUTOVER_CONSTRUCTION_CAPABILITY:
            raise CanonicalCommitDisabledError(
                "canonical execution cutover requires the trusted private issuer"
            )
        token = _validate_activation(activation_token)
        if type(baseline_attestation) is not V3BaselineExternalAttestation:
            raise TypeError(
                "baseline_attestation must be exact V3BaselineExternalAttestation"
            )
        if (
            baseline_attestation.verification_report_hash
            != token.acceptance_report_hash
        ):
            raise CanonicalCommitDisabledError(
                "baseline verification report differs from activation acceptance"
            )
        if not callable(prepare_commit):
            raise TypeError("prepare_commit must be callable")
        object.__setattr__(self, "activation_token", token)
        object.__setattr__(self, "baseline_attestation", baseline_attestation)
        object.__setattr__(self, "prepare_commit", prepare_commit)


def _issue_trusted_test_canonical_execution_cutover(
    *,
    token_id: str,
    acceptance_report_hash: str,
    expected_baseline_manifest_hash: str,
    verified_baseline_manifest_hash: str,
    baseline_verification_report_hash: str,
    external_trusted_hash_recorded: bool,
    prepare_commit: PrepareCommitCallback,
    valid_for: timedelta = timedelta(minutes=15),
) -> CanonicalExecutionCutover:
    """Private acceptance hook; intentionally unusable outside TEST/CI."""

    token = _issue_trusted_acceptance_activation_token(
        token_id=token_id,
        acceptance_report_hash=acceptance_report_hash,
        valid_for=valid_for,
    )
    attestation = V3BaselineExternalAttestation(
        expected_manifest_hash=expected_baseline_manifest_hash,
        verified_manifest_hash=verified_baseline_manifest_hash,
        verification_report_hash=baseline_verification_report_hash,
        external_trusted_hash_recorded=external_trusted_hash_recorded,
        production_activation_allowed=False,
    )
    return CanonicalExecutionCutover(
        activation_token=token,
        baseline_attestation=attestation,
        prepare_commit=prepare_commit,
        _construction_capability=_CUTOVER_CONSTRUCTION_CAPABILITY,
    )


@dataclass(frozen=True, slots=True)
class PreparedCommitTransactionContext:
    connection_identity: int
    transaction_identity: int
    activation_token_hash: str
    baseline_manifest_hash: str
    checked_at: datetime


def _validate_cutover(cutover: object) -> CanonicalExecutionCutover:
    if type(cutover) is not CanonicalExecutionCutover:
        raise CanonicalCommitDisabledError(
            "prepared canonical commit is disabled without its exact TEST/CI capability"
        )
    token = _validate_activation(cutover.activation_token)
    attestation = cutover.baseline_attestation
    if (
        type(attestation) is not V3BaselineExternalAttestation
        or attestation.expected_manifest_hash != attestation.verified_manifest_hash
        or attestation.verification_report_hash != token.acceptance_report_hash
        or attestation.external_trusted_hash_recorded is not True
        or attestation.production_activation_allowed is not False
    ):
        raise CanonicalCommitDisabledError(
            "prepared canonical commit baseline attestation is invalid"
        )
    if not callable(cutover.prepare_commit):
        raise CanonicalCommitDisabledError(
            "prepared canonical commit builder is invalid"
        )
    return cutover


def preflight_prepared_commit(
    connection: Any,
    *,
    cutover: CanonicalExecutionCutover | None,
    now: datetime,
) -> PreparedCommitTransactionContext:
    """Lock the maintenance fence before any order/account lock is acquired."""

    active, transaction = _active_connection(connection)
    enabled = _validate_cutover(cutover)
    checked_at = _aware_utc(now, "now")
    try:
        assert_v2_evidence_maintenance_fence_inactive(active)
    except V2EvidenceMaintenanceFenceError as exc:
        raise CanonicalCommitInvariantError(
            "prepared commit is blocked by the V2 evidence maintenance fence"
        ) from exc
    return PreparedCommitTransactionContext(
        connection_identity=id(active),
        transaction_identity=id(transaction),
        activation_token_hash=enabled.activation_token.token_hash,
        baseline_manifest_hash=(
            enabled.baseline_attestation.verified_manifest_hash
        ),
        checked_at=checked_at,
    )


def _validate_preflight(
    connection: Any,
    transaction: Any,
    cutover: CanonicalExecutionCutover,
    context: PreparedCommitTransactionContext,
) -> None:
    if type(context) is not PreparedCommitTransactionContext:
        raise CanonicalCommitInvariantError(
            "prepared commit requires its exact preflight transaction context"
        )
    if (
        context.connection_identity != id(connection)
        or context.transaction_identity != id(transaction)
        or context.activation_token_hash != cutover.activation_token.token_hash
        or context.baseline_manifest_hash
        != cutover.baseline_attestation.verified_manifest_hash
    ):
        raise CanonicalCommitInvariantError(
            "prepared commit preflight belongs to a different transaction or gate"
        )


def _order_evidence(
    evidence: tuple[Any, ...],
) -> tuple[OrderTransitionEvidence, ...]:
    return tuple(
        item for item in evidence if type(item) is OrderTransitionEvidence
    )


def _validate_bundle(
    mutation: CanonicalMechanicalMutation,
    bundle: PreparedCanonicalCommitBundle,
    baseline_hash: str,
) -> None:
    if type(bundle) is not PreparedCanonicalCommitBundle:
        raise CanonicalCommitInvariantError(
            "prepare callback returned an invalid bundle"
        )
    if bundle.mutation_hash != mutation.mutation_hash:
        raise CanonicalCommitInvariantError(
            "prepared bundle binds a different mechanical mutation"
        )
    if bundle.baseline_manifest_hash != baseline_hash:
        raise CanonicalCommitInvariantError(
            "prepared bundle binds a different verified V3 baseline"
        )
    transitions = _order_evidence(bundle.execution_evidence)
    if len(transitions) != len(mutation.transitions):
        raise CanonicalCommitInvariantError(
            "prepared order evidence count differs from mechanical transitions"
        )
    for mechanical, evidence in zip(mutation.transitions, transitions):
        observed = (
            evidence.order_id,
            evidence.account_id,
            evidence.from_status,
            evidence.to_status,
            evidence.previous_filled_quantity,
            evidence.next_filled_quantity,
            evidence.waiting_reason,
            evidence.transition_kind,
            evidence.source_event_type,
            evidence.source_event_id,
            evidence.source_event_hash,
            evidence.occurred_at,
            evidence.related_fill_id,
        )
        expected = (
            mechanical.order_id,
            mechanical.account_id,
            mechanical.from_status,
            mechanical.to_status,
            mechanical.previous_filled_quantity,
            mechanical.next_filled_quantity,
            mechanical.next_waiting_reason,
            mechanical.transition_kind,
            mechanical.source_event_type,
            mechanical.source_event_id,
            mechanical.source_event_hash,
            mechanical.occurred_at,
            mechanical.related_fill_id,
        )
        if observed != expected:
            raise CanonicalCommitInvariantError(
                "prepared order evidence differs from the mechanical transition"
            )

    fills = tuple(
        item
        for item in bundle.execution_evidence
        if type(item) is FillExecutionEvidence
    )
    cash_bindings = tuple(
        item
        for item in bundle.execution_evidence
        if type(item) is CashEventBinding
    )
    calendars = tuple(
        item
        for item in bundle.execution_evidence
        if type(item) is MarketCalendarEvidence
    )
    quotes = tuple(
        item
        for item in bundle.execution_evidence
        if type(item) is QuoteReceiptEvidence
    )
    if mutation.fill_id is None:
        if fills or cash_bindings or calendars or quotes:
            raise CanonicalCommitInvariantError(
                "non-fill mutation cannot carry fill-input evidence"
            )
        if bundle.accounting_outcome is not None:
            raise CanonicalCommitInvariantError(
                "non-fill mutation cannot carry accounting finalization"
            )
        if bundle.instrument_rule_authority_reference is not None:
            raise CanonicalCommitInvariantError(
                "non-fill mutation cannot carry instrument-rule authority"
            )
    else:
        if not (
            len(fills)
            == len(cash_bindings)
            == len(calendars)
            == len(quotes)
            == 1
        ):
            raise CanonicalCommitInvariantError(
                "fill mutation requires one calendar, quote, fill and cash evidence"
            )
        if bundle.accounting_outcome is None:
            raise CanonicalCommitInvariantError(
                "fill mutation requires accounting finalization"
            )
        fill = fills[0]
        cash = cash_bindings[0]
        fill_transition = next(
            item
            for item in transitions
            if item.transition_kind is OrderTransitionKind.FILL_APPLIED
        )
        outcome = bundle.accounting_outcome
        if (
            fill.fill_id != mutation.fill_id
            or fill.order_id != mutation.order_id
            or fill.account_id != mutation.account_id
            or cash.related_fill_id != mutation.fill_id
            or cash.related_order_id != mutation.order_id
            or outcome.fill_execution_evidence.evidence_hash
            != fill.evidence_hash
            or outcome.cash_binding.binding_hash != cash.binding_hash
            or outcome.order_transition.transition_hash
            != fill_transition.transition_hash
        ):
            raise CanonicalCommitInvariantError(
                "fill evidence/accounting identities differ from mechanical facts"
            )

    if not (
        len(bundle.projections)
        == len(bundle.projection_bindings)
        == len(bundle.projection_receipts)
        == len(mutation.transitions)
    ):
        raise CanonicalCommitInvariantError(
            "V3 projections require one exact binding and receipt per mutation"
        )
    core_status = {
        OrderStatus.RISK_APPROVED: CoreOrderStatus.ACCEPTED,
        OrderStatus.QUEUED: CoreOrderStatus.QUEUED,
        OrderStatus.PARTIALLY_FILLED: CoreOrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED: CoreOrderStatus.FILLED,
        OrderStatus.CANCELLED: CoreOrderStatus.CANCELLED,
        OrderStatus.REJECTED: CoreOrderStatus.REJECTED,
        OrderStatus.EXPIRED: CoreOrderStatus.EXPIRED,
    }
    previous_projection_receipt: OrderTransitionReceipt | None = None
    previous_projection_binding: V3ExecutionPlanBinding | None = None
    for mechanical, projection, binding, receipt in zip(
        mutation.transitions,
        bundle.projections,
        bundle.projection_bindings,
        bundle.projection_receipts,
    ):
        if not validate_order_transition_receipt(receipt):
            raise CanonicalCommitInvariantError(
                "V3 projection receipt failed canonical revalidation"
            )
        if previous_projection_receipt is not None and (
            receipt.previous_state != previous_projection_receipt.current_state
            or binding != previous_projection_binding
        ):
            raise CanonicalCommitInvariantError(
                "V3 projection receipts or bindings are not one continuous chain"
            )
        expected_projection = project_execution_result(
            binding=binding,
            transition=receipt,
        )
        if projection != expected_projection:
            raise CanonicalCommitInvariantError(
                "V3 projection is not the exact result of its receipt and binding"
            )
        expected_from_status = core_status[mechanical.from_status]
        expected_to_status = core_status[mechanical.to_status]
        expected_previous_reason = mechanical.previous_waiting_reason or ""
        expected_next_reason = mechanical.next_waiting_reason or ""
        if mechanical.transition_kind is OrderTransitionKind.WAITING_REASON_CHANGED:
            expected_event_kind = ExecutionEventKind.WAITING_REASON_CHANGED
        else:
            expected_event_kind = ExecutionEventKind.STATUS_TRANSITION
        if (
            receipt.previous_state.order_id != mutation.order_id
            or receipt.current_state.order_id != mutation.order_id
            or receipt.previous_state.status is not expected_from_status
            or receipt.current_state.status is not expected_to_status
            or receipt.previous_state.cumulative_filled_quantity
            != mechanical.previous_filled_quantity
            or receipt.current_state.cumulative_filled_quantity
            != mechanical.next_filled_quantity
            or receipt.previous_state.waiting_reason
            != expected_previous_reason
            or receipt.current_state.waiting_reason != expected_next_reason
            or receipt.result.event_kind is not expected_event_kind
            or (
                expected_event_kind
                is ExecutionEventKind.WAITING_REASON_CHANGED
                and receipt.result.reason_code != expected_next_reason
            )
            or receipt.result.event_id != mechanical.source_event_id
            or receipt.result.occurred_at != mechanical.occurred_at
            or projection.source_order_id != mutation.order_id
            or projection.source_event_id != mechanical.source_event_id
            or projection.source_order_status is not expected_to_status
            or projection.cumulative_filled_quantity
            != mechanical.next_filled_quantity
            or projection.occurred_at != mechanical.occurred_at
        ):
            raise CanonicalCommitInvariantError(
                "V3 projection differs from the mechanical transition"
            )
        previous_projection_receipt = receipt
        previous_projection_binding = binding


@dataclass(frozen=True, slots=True)
class PreparedCanonicalCommitReceipt:
    mutation_hash: str
    activation_token_hash: str
    baseline_manifest_hash: str
    evidence_results: tuple[Any, ...]
    accounting_result: Any | None
    outbox_results: tuple[Any, ...]
    production_activation_allowed: bool = False


def commit_prepared_canonical_execution(
    connection: Any,
    *,
    cutover: CanonicalExecutionCutover,
    preflight: PreparedCommitTransactionContext,
    mutation: CanonicalMechanicalMutation,
) -> PreparedCanonicalCommitReceipt:
    """Append evidence -> accounting finalization -> V3 outbox atomically.

    Facts must already have been mutated by the existing executor in this
    same transaction.  This function performs no transaction lifecycle call.
    """

    active, transaction = _active_connection(connection)
    enabled = _validate_cutover(cutover)
    _validate_preflight(active, transaction, enabled, preflight)
    if type(mutation) is not CanonicalMechanicalMutation:
        raise TypeError("mutation must be exact CanonicalMechanicalMutation")
    # The preparer is intentionally a pure mutation-to-bundle callback.  It is
    # never given the caller's raw Connection, so it cannot accidentally call
    # commit(), rollback(), begin(), or otherwise split the canonical facts
    # from their evidence/accounting/outbox transaction.
    bundle = enabled.prepare_commit(mutation)
    enabled = _validate_cutover(enabled)
    active_after_prepare, transaction_after_prepare = _active_connection(active)
    _validate_preflight(
        active_after_prepare,
        transaction_after_prepare,
        enabled,
        preflight,
    )
    _validate_bundle(
        mutation,
        bundle,
        enabled.baseline_attestation.verified_manifest_hash,
    )

    evidence_results: list[Any] = []
    for evidence in bundle.execution_evidence:
        evidence_results.append(
            append_evidence(
                active,
                evidence,
                authority_verifier=bundle.authority_verifier,
                instrument_rule_authority_reference=(
                    bundle.instrument_rule_authority_reference
                    if type(evidence) is FillExecutionEvidence
                    else None
                ),
            )
        )
    accounting_result = None
    if bundle.accounting_outcome is not None:
        accounting_result = append_fill_accounting_outcome(
            active,
            bundle.accounting_outcome,
        )
    outbox_results = tuple(
        append_v3_transition_outbox(
            active,
            projection,
            created_at=mutation.recorded_at,
        )
        for projection in bundle.projections
    )
    return PreparedCanonicalCommitReceipt(
        mutation_hash=mutation.mutation_hash,
        activation_token_hash=enabled.activation_token.token_hash,
        baseline_manifest_hash=(
            enabled.baseline_attestation.verified_manifest_hash
        ),
        evidence_results=tuple(evidence_results),
        accounting_result=accounting_result,
        outbox_results=outbox_results,
        production_activation_allowed=False,
    )


__all__ = [
    "PREPARED_COMMIT_RUNTIME_ENABLED",
    "PRODUCTION_ACTIVATION_ALLOWED",
    "CanonicalExecutionCutover",
    "CanonicalMechanicalMutation",
    "CanonicalMechanicalTransition",
    "PreparedCanonicalCommitBundle",
    "PreparedCanonicalCommitReceipt",
    "PreparedCommitTransactionContext",
    "V3BaselineExternalAttestation",
    "commit_prepared_canonical_execution",
    "preflight_prepared_commit",
]
