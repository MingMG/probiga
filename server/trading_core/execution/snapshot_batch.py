"""Pure, strategy-neutral coordination of one shared snapshot liquidity cap.

Every candidate must refer to the same externally verified snapshot receipt.
The coordinator orders candidates only by mechanical order facts, derives a
remaining shared cap for each call to the existing snapshot matcher, and owns
no repository, account, strategy, clock, broker, or write capability.

One request is a closed, one-shot allocation set. A caller opening a separate
set for the same receipt must advance trusted already_filled_quantity and
deduplicate the deterministic request/result hashes outside this pure module;
the function cannot prove consumption performed by an omitted prior request.

Receipt and content hashes are integrity references, not authority proofs.  An
integration boundary must verify the external receipt before constructing the
input objects accepted here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import Any

from ..contracts import ExecutionResult
from .matcher import LimitDayOrder, MatchDecision, MatchPriceBand
from .snapshot_matcher import (
    AttestedSnapshotQuote,
    SnapshotEvidenceKind,
    SnapshotLiquidityEvidence,
    SnapshotMatchRule,
    match_attested_snapshot,
    snapshot_attestation_hash,
)


def _text(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be exactly str")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _aware(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be exactly int")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


def _canonical(value: Any) -> Any:
    if type(value) is datetime:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is date:
        return value.isoformat()
    if type(value) is timedelta:
        return (
            value.days * 86_400_000_000
            + value.seconds * 1_000_000
            + value.microseconds
        )
    if type(value) is Decimal:
        sign, digits, exponent = value.as_tuple()
        if not any(digits):
            return "0"
        while digits[-1] == 0:
            digits = digits[:-1]
            exponent += 1
        coefficient = "".join(str(digit) for digit in digits)
        point = len(coefficient) + exponent
        if point <= 0:
            rendered = "0." + "0" * (-point) + coefficient
        elif point >= len(coefficient):
            rendered = coefficient + "0" * (point - len(coefficient))
        else:
            rendered = coefficient[:point] + "." + coefficient[point:]
        return f"-{rendered}" if sign else rendered
    if isinstance(value, Enum):
        return value.value
    if type(value) in {list, tuple}:
        return [_canonical(item) for item in value]
    if type(value) is dict:
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if value is None or type(value) in {str, int, bool}:
        return value
    raise TypeError(f"unsupported snapshot batch hash value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _band_payload(band: MatchPriceBand | None) -> dict[str, Any] | None:
    if band is None:
        return None
    return {
        "instrument_id": band.instrument_id,
        "trade_date": band.trade_date,
        "as_of": band.as_of,
        "source": band.source,
        "lower": band.lower,
        "upper": band.upper,
    }


def _order_payload(order: LimitDayOrder) -> dict[str, Any]:
    return {
        "order_id": order.order_id,
        "intent_id": order.intent_id,
        "instrument_id": order.instrument_id,
        "side": order.side,
        "requested_quantity": order.requested_quantity,
        "approved_quantity": order.approved_quantity,
        "cumulative_filled_quantity": order.cumulative_filled_quantity,
        "limit_price": order.limit_price,
        "earliest_at": order.earliest_at,
        "expires_at": order.expires_at,
        "updated_at": order.updated_at,
        "last_source_sequence": order.last_source_sequence,
        "status": order.status,
        "applied_events": order.applied_events,
    }


def _evidence_payload(evidence: SnapshotLiquidityEvidence) -> dict[str, Any]:
    return {
        "evidence_kind": evidence.evidence_kind,
        "source_provider": evidence.source_provider,
        "source_batch_id": evidence.source_batch_id,
        "source_payload_hash": evidence.source_payload_hash,
        "source_receipt_hash": evidence.source_receipt_hash,
        "quality_status": evidence.quality_status,
        "source_count": evidence.source_count,
        "source_volume": evidence.source_volume,
        "lot_size": evidence.lot_size,
        "participation_rate": evidence.participation_rate,
        "already_filled_quantity": evidence.already_filled_quantity,
        "standalone_compatibility_quantity": (
            evidence.standalone_compatibility_quantity
        ),
        "liquidity_quantity": evidence.liquidity_quantity,
        "evidence_hash": evidence.evidence_hash,
    }


def _quote_payload(quote: AttestedSnapshotQuote) -> dict[str, Any]:
    return {
        "instrument_id": quote.instrument_id,
        "snapshot_id": quote.snapshot_id,
        "observed_at": quote.observed_at,
        "received_at": quote.received_at,
        "last_price": quote.last_price,
        "source": quote.source,
        "attestation_hash": quote.attestation_hash,
        "liquidity_evidence": _evidence_payload(quote.liquidity_evidence),
        "suspended": quote.suspended,
    }


def _rule_payload(rule: SnapshotMatchRule) -> dict[str, Any]:
    return {
        "rule_version": rule.rule_version,
        "enabled": rule.enabled,
        "tick_size": rule.tick_size,
        "quote_max_age": rule.quote_max_age,
        "allowed_sources": rule.allowed_sources,
        "allow_synthetic_compatibility_evidence": (
            rule.allow_synthetic_compatibility_evidence
        ),
        "slippage_rate": rule.slippage_rate,
        "price_band": _band_payload(rule.price_band),
        "price_band_max_age": rule.price_band_max_age,
        "require_complete_price_band": rule.require_complete_price_band,
        "enforce_price_band_bounds": rule.enforce_price_band_bounds,
        "block_adverse_limit_lock": rule.block_adverse_limit_lock,
    }


def _execution_payload(result: ExecutionResult | None) -> dict[str, Any] | None:
    if result is None:
        return None
    return {
        "intent_id": result.intent_id,
        "order_id": result.order_id,
        "event_id": result.event_id,
        "status": result.status,
        "occurred_at": result.occurred_at,
        "received_at": result.received_at,
        "source_sequence": result.source_sequence,
        "idempotency_key": result.idempotency_key,
        "last_fill_quantity": result.last_fill_quantity,
        "last_fill_price": result.last_fill_price,
        "reason_code": result.reason_code,
    }


def _decision_payload(decision: MatchDecision) -> dict[str, Any]:
    return {
        "status": decision.status,
        "reason": decision.reason,
        "updated_order": _order_payload(decision.updated_order),
        "quote_id": decision.quote_id,
        "fill_quantity": decision.fill_quantity,
        "fill_price": decision.fill_price,
        "execution_result": _execution_payload(decision.execution_result),
        "explanation": decision.explanation,
    }


def _rebuild_order(order: LimitDayOrder) -> LimitDayOrder:
    if type(order) is not LimitDayOrder:
        raise TypeError("order must be exactly LimitDayOrder")
    rebuilt = LimitDayOrder(
        order_id=order.order_id,
        intent_id=order.intent_id,
        instrument_id=order.instrument_id,
        side=order.side,
        requested_quantity=order.requested_quantity,
        approved_quantity=order.approved_quantity,
        cumulative_filled_quantity=order.cumulative_filled_quantity,
        limit_price=order.limit_price,
        earliest_at=order.earliest_at,
        expires_at=order.expires_at,
        updated_at=order.updated_at,
        last_source_sequence=order.last_source_sequence,
        status=order.status,
        applied_events=order.applied_events,
    )
    if rebuilt != order:
        raise ValueError("order contains non-canonical or tampered fields")
    return rebuilt


def _rebuild_band(band: MatchPriceBand | None) -> MatchPriceBand | None:
    if band is None:
        return None
    if type(band) is not MatchPriceBand:
        raise TypeError("price_band must be exactly MatchPriceBand or None")
    rebuilt = MatchPriceBand(
        instrument_id=band.instrument_id,
        trade_date=band.trade_date,
        as_of=band.as_of,
        source=band.source,
        lower=band.lower,
        upper=band.upper,
    )
    if rebuilt != band:
        raise ValueError("price band contains non-canonical or tampered fields")
    return rebuilt


def _rebuild_rule(rule: SnapshotMatchRule) -> SnapshotMatchRule:
    if type(rule) is not SnapshotMatchRule:
        raise TypeError("rule must be exactly SnapshotMatchRule")
    rebuilt = SnapshotMatchRule(
        rule_version=rule.rule_version,
        enabled=rule.enabled,
        tick_size=rule.tick_size,
        quote_max_age=rule.quote_max_age,
        allowed_sources=rule.allowed_sources,
        allow_synthetic_compatibility_evidence=(
            rule.allow_synthetic_compatibility_evidence
        ),
        slippage_rate=rule.slippage_rate,
        price_band=_rebuild_band(rule.price_band),
        price_band_max_age=rule.price_band_max_age,
        require_complete_price_band=rule.require_complete_price_band,
        enforce_price_band_bounds=rule.enforce_price_band_bounds,
        block_adverse_limit_lock=rule.block_adverse_limit_lock,
    )
    if rebuilt != rule:
        raise ValueError("snapshot rule contains non-canonical or tampered fields")
    return rebuilt


def _rebuild_evidence(
    evidence: SnapshotLiquidityEvidence,
) -> SnapshotLiquidityEvidence:
    if type(evidence) is not SnapshotLiquidityEvidence:
        raise TypeError(
            "liquidity_evidence must be exactly SnapshotLiquidityEvidence"
        )
    rebuilt = SnapshotLiquidityEvidence(
        evidence_kind=evidence.evidence_kind,
        source_provider=evidence.source_provider,
        source_batch_id=evidence.source_batch_id,
        source_payload_hash=evidence.source_payload_hash,
        source_receipt_hash=evidence.source_receipt_hash,
        quality_status=evidence.quality_status,
        source_count=evidence.source_count,
        source_volume=evidence.source_volume,
        lot_size=evidence.lot_size,
        participation_rate=evidence.participation_rate,
        already_filled_quantity=evidence.already_filled_quantity,
        standalone_compatibility_quantity=(
            evidence.standalone_compatibility_quantity
        ),
    )
    if rebuilt != evidence:
        raise ValueError(
            "snapshot liquidity evidence derived fields or hash were tampered"
        )
    return rebuilt


def _rebuild_quote(quote: AttestedSnapshotQuote) -> AttestedSnapshotQuote:
    if type(quote) is not AttestedSnapshotQuote:
        raise TypeError("quote must be exactly AttestedSnapshotQuote")
    evidence = _rebuild_evidence(quote.liquidity_evidence)
    rebuilt = AttestedSnapshotQuote(
        instrument_id=quote.instrument_id,
        snapshot_id=quote.snapshot_id,
        observed_at=quote.observed_at,
        received_at=quote.received_at,
        last_price=quote.last_price,
        source=quote.source,
        attestation_hash=quote.attestation_hash,
        liquidity_evidence=evidence,
        suspended=quote.suspended,
    )
    expected_attestation = snapshot_attestation_hash(
        instrument_id=quote.instrument_id,
        snapshot_id=quote.snapshot_id,
        observed_at=quote.observed_at,
        received_at=quote.received_at,
        last_price=quote.last_price,
        source=quote.source,
        liquidity_evidence_hash=evidence.evidence_hash,
        suspended=quote.suspended,
    )
    if quote.attestation_hash != expected_attestation:
        raise ValueError("snapshot attestation hash was tampered")
    if rebuilt != quote:
        raise ValueError("snapshot quote contains non-canonical or tampered fields")
    return rebuilt


def _candidate_hash(candidate: SnapshotBatchCandidate) -> str:
    return _digest(
        "trading-core.snapshot-batch-candidate.v1",
        {
            "created_at": candidate.created_at,
            "evaluated_at": candidate.evaluated_at,
            "order": _order_payload(candidate.order),
            "quote": _quote_payload(candidate.quote),
            "rule": _rule_payload(candidate.rule),
        },
    )


def _priority_key(
    candidate: SnapshotBatchCandidate,
) -> tuple[datetime, datetime, str]:
    return (
        candidate.created_at.astimezone(timezone.utc),
        candidate.order.earliest_at.astimezone(timezone.utc),
        candidate.order.order_id,
    )


@dataclass(frozen=True, slots=True)
class SnapshotBatchCandidate:
    """One order and its shared snapshot facts; no rank or signal is accepted."""

    created_at: datetime
    evaluated_at: datetime
    order: LimitDayOrder
    quote: AttestedSnapshotQuote
    rule: SnapshotMatchRule
    candidate_hash: str = field(init=False)

    def __post_init__(self) -> None:
        created_at = _aware(self.created_at, "created_at")
        evaluated_at = _aware(self.evaluated_at, "evaluated_at")
        order = _rebuild_order(self.order)
        quote = _rebuild_quote(self.quote)
        rule = _rebuild_rule(self.rule)
        if created_at > order.earliest_at:
            raise ValueError("created_at cannot follow order earliest_at")
        if created_at > evaluated_at:
            raise ValueError("created_at cannot follow evaluated_at")
        if order.instrument_id != quote.instrument_id:
            raise ValueError("order and snapshot instruments must match")
        evidence = quote.liquidity_evidence
        if (
            evidence.evidence_kind
            != SnapshotEvidenceKind.EXTERNAL_RECEIPT_REFERENCE
        ):
            raise ValueError(
                "snapshot batch accepts only EXTERNAL_RECEIPT_REFERENCE evidence"
            )
        if evidence.source_receipt_hash is None:
            raise ValueError("external snapshot receipt hash is required")
        if evidence.source_provider != quote.source:
            raise ValueError(
                "snapshot evidence provider must match snapshot source"
            )
        if rule.allow_synthetic_compatibility_evidence:
            raise ValueError(
                "snapshot batch production rule cannot allow synthetic evidence"
            )
        if quote.source not in rule.allowed_sources:
            raise ValueError("snapshot source is not allowed by the rule")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "evaluated_at", evaluated_at)
        object.__setattr__(self, "order", order)
        object.__setattr__(self, "quote", quote)
        object.__setattr__(self, "rule", rule)
        object.__setattr__(self, "candidate_hash", _candidate_hash(self))


def _validate_candidate(candidate: SnapshotBatchCandidate) -> None:
    if type(candidate) is not SnapshotBatchCandidate:
        raise TypeError("candidate must be exactly SnapshotBatchCandidate")
    rebuilt = SnapshotBatchCandidate(
        created_at=candidate.created_at,
        evaluated_at=candidate.evaluated_at,
        order=candidate.order,
        quote=candidate.quote,
        rule=candidate.rule,
    )
    if rebuilt != candidate:
        raise ValueError("snapshot batch candidate fields or hash were tampered")


def _validate_shared_batch(candidates: tuple[SnapshotBatchCandidate, ...]) -> None:
    base = candidates[0]
    base_quote = base.quote
    base_evidence = base_quote.liquidity_evidence
    base_rule = base.rule
    base_receipt = base_evidence.source_receipt_hash
    for candidate in candidates:
        if base_quote.snapshot_id in dict(candidate.order.applied_events):
            raise ValueError(
                "snapshot batch is one-shot and cannot mix replayed snapshot "
                "events with shared-cap allocation"
            )
    for candidate in candidates[1:]:
        quote = candidate.quote
        evidence = quote.liquidity_evidence
        if candidate.order.instrument_id != base.order.instrument_id:
            raise ValueError("batch orders must share one instrument")
        if quote.instrument_id != base_quote.instrument_id:
            raise ValueError("batch snapshots must share one instrument")
        if quote.snapshot_id != base_quote.snapshot_id:
            raise ValueError("batch candidates must share one snapshot_id")
        if evidence.source_receipt_hash != base_receipt:
            raise ValueError("batch candidates must share one external receipt")
        if quote.source != base_quote.source:
            raise ValueError("batch snapshot sources are inconsistent")
        if (
            quote.observed_at != base_quote.observed_at
            or quote.received_at != base_quote.received_at
            or candidate.evaluated_at != base.evaluated_at
        ):
            raise ValueError("batch snapshot or evaluation times are inconsistent")
        if quote != base_quote:
            raise ValueError("batch snapshot content is inconsistent")
        if candidate.rule != base_rule:
            raise ValueError("batch rules are inconsistent")


@dataclass(frozen=True, slots=True)
class SnapshotBatchRequest:
    candidates: tuple[SnapshotBatchCandidate, ...]
    request_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.candidates) is not tuple:
            raise TypeError("candidates must be exactly tuple")
        if not self.candidates:
            raise ValueError("snapshot batch requires at least one candidate")
        for candidate in self.candidates:
            _validate_candidate(candidate)
        order_ids = [candidate.order.order_id for candidate in self.candidates]
        if len(order_ids) != len(set(order_ids)):
            raise ValueError("snapshot batch order_id values must be unique")
        ordered = tuple(sorted(self.candidates, key=_priority_key))
        _validate_shared_batch(ordered)
        object.__setattr__(self, "candidates", ordered)
        object.__setattr__(
            self,
            "request_hash",
            _digest(
                "trading-core.snapshot-batch-request.v1",
                {"candidate_hashes": tuple(c.candidate_hash for c in ordered)},
            ),
        )


def _allocation_hash(allocation: SnapshotBatchAllocation) -> str:
    return _digest(
        "trading-core.snapshot-batch-allocation.v1",
        {
            "priority": allocation.priority,
            "order_id": allocation.order_id,
            "created_at": allocation.created_at,
            "shared_cap_before": allocation.shared_cap_before,
            "shared_cap_after": allocation.shared_cap_after,
            "effective_evidence_hash": allocation.effective_evidence_hash,
            "decision": _decision_payload(allocation.decision),
        },
    )


@dataclass(frozen=True, slots=True)
class SnapshotBatchAllocation:
    priority: int
    order_id: str
    created_at: datetime
    shared_cap_before: int
    shared_cap_after: int
    effective_evidence_hash: str
    decision: MatchDecision
    allocation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _integer(self.priority, "priority")
        order_id = _text(self.order_id, "order_id")
        created_at = _aware(self.created_at, "created_at")
        cap_before = _integer(self.shared_cap_before, "shared_cap_before")
        cap_after = _integer(self.shared_cap_after, "shared_cap_after")
        evidence_hash = _sha256(
            self.effective_evidence_hash,
            "effective_evidence_hash",
        )
        if type(self.decision) is not MatchDecision:
            raise TypeError("decision must be exactly MatchDecision")
        if self.decision.updated_order.order_id != order_id:
            raise ValueError("allocation order_id does not match decision order")
        if cap_after > cap_before:
            raise ValueError("shared cap cannot increase during allocation")
        if cap_before - cap_after != self.decision.fill_quantity:
            raise ValueError("shared cap delta must equal decision fill quantity")
        object.__setattr__(self, "order_id", order_id)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "effective_evidence_hash", evidence_hash)
        object.__setattr__(self, "allocation_hash", _allocation_hash(self))


def _validate_allocation(allocation: SnapshotBatchAllocation) -> None:
    if type(allocation) is not SnapshotBatchAllocation:
        raise TypeError("allocation must be exactly SnapshotBatchAllocation")
    expected = _allocation_hash(allocation)
    if allocation.allocation_hash != expected:
        raise ValueError("snapshot batch allocation hash was tampered")
    if (
        allocation.shared_cap_before - allocation.shared_cap_after
        != allocation.decision.fill_quantity
    ):
        raise ValueError("snapshot batch allocation cap transition was tampered")


def _result_hash(result: SnapshotBatchResult) -> str:
    return _digest(
        "trading-core.snapshot-batch-result.v1",
        {
            "request_hash": result.request_hash,
            "instrument_id": result.instrument_id,
            "snapshot_id": result.snapshot_id,
            "source_receipt_hash": result.source_receipt_hash,
            "shared_liquidity_cap": result.shared_liquidity_cap,
            "total_fill_quantity": result.total_fill_quantity,
            "allocation_hashes": tuple(
                allocation.allocation_hash for allocation in result.allocations
            ),
        },
    )


@dataclass(frozen=True, slots=True)
class SnapshotBatchResult:
    request_hash: str
    instrument_id: str
    snapshot_id: str
    source_receipt_hash: str
    shared_liquidity_cap: int
    total_fill_quantity: int
    allocations: tuple[SnapshotBatchAllocation, ...]
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        request_hash = _sha256(self.request_hash, "request_hash")
        instrument_id = _text(self.instrument_id, "instrument_id")
        snapshot_id = _text(self.snapshot_id, "snapshot_id")
        receipt_hash = _sha256(
            self.source_receipt_hash,
            "source_receipt_hash",
        )
        shared_cap = _integer(
            self.shared_liquidity_cap,
            "shared_liquidity_cap",
        )
        total_fill = _integer(self.total_fill_quantity, "total_fill_quantity")
        if type(self.allocations) is not tuple or not self.allocations:
            raise TypeError("allocations must be a non-empty exact tuple")
        for allocation in self.allocations:
            _validate_allocation(allocation)
        priorities = tuple(allocation.priority for allocation in self.allocations)
        if priorities != tuple(range(len(self.allocations))):
            raise ValueError("allocation priorities must be contiguous and ordered")
        order_ids = tuple(allocation.order_id for allocation in self.allocations)
        if len(order_ids) != len(set(order_ids)):
            raise ValueError("allocation order_id values must be unique")
        if self.allocations[0].shared_cap_before != shared_cap:
            raise ValueError("first allocation must start at the shared cap")
        for previous, current in zip(self.allocations, self.allocations[1:]):
            if previous.shared_cap_after != current.shared_cap_before:
                raise ValueError("allocation shared-cap transitions must be contiguous")
        computed_fill = sum(
            allocation.decision.fill_quantity for allocation in self.allocations
        )
        if computed_fill != total_fill:
            raise ValueError("total fill does not match allocation decisions")
        if total_fill > shared_cap:
            raise ValueError("total fill cannot exceed shared liquidity cap")
        object.__setattr__(self, "request_hash", request_hash)
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "snapshot_id", snapshot_id)
        object.__setattr__(self, "source_receipt_hash", receipt_hash)
        object.__setattr__(self, "result_hash", _result_hash(self))


def validate_snapshot_batch_result(result: SnapshotBatchResult) -> None:
    """Raise when a result or one of its derived hashes was altered."""

    if type(result) is not SnapshotBatchResult:
        raise TypeError("result must be exactly SnapshotBatchResult")
    rebuilt = SnapshotBatchResult(
        request_hash=result.request_hash,
        instrument_id=result.instrument_id,
        snapshot_id=result.snapshot_id,
        source_receipt_hash=result.source_receipt_hash,
        shared_liquidity_cap=result.shared_liquidity_cap,
        total_fill_quantity=result.total_fill_quantity,
        allocations=result.allocations,
    )
    if rebuilt != result:
        raise ValueError("snapshot batch result fields or hash were tampered")


def _derived_quote(
    quote: AttestedSnapshotQuote,
    *,
    batch_filled_quantity: int,
) -> AttestedSnapshotQuote:
    base = quote.liquidity_evidence
    if type(base.already_filled_quantity) is not int:
        raise TypeError("external evidence already_filled_quantity must be int")
    evidence = SnapshotLiquidityEvidence(
        evidence_kind=SnapshotEvidenceKind.EXTERNAL_RECEIPT_REFERENCE,
        source_provider=base.source_provider,
        source_batch_id=base.source_batch_id,
        source_payload_hash=base.source_payload_hash,
        source_receipt_hash=base.source_receipt_hash,
        quality_status=base.quality_status,
        source_count=base.source_count,
        source_volume=base.source_volume,
        lot_size=base.lot_size,
        participation_rate=base.participation_rate,
        already_filled_quantity=(
            base.already_filled_quantity + batch_filled_quantity
        ),
    )
    attestation = snapshot_attestation_hash(
        instrument_id=quote.instrument_id,
        snapshot_id=quote.snapshot_id,
        observed_at=quote.observed_at,
        received_at=quote.received_at,
        last_price=quote.last_price,
        source=quote.source,
        liquidity_evidence_hash=evidence.evidence_hash,
        suspended=quote.suspended,
    )
    return AttestedSnapshotQuote(
        instrument_id=quote.instrument_id,
        snapshot_id=quote.snapshot_id,
        observed_at=quote.observed_at,
        received_at=quote.received_at,
        last_price=quote.last_price,
        source=quote.source,
        attestation_hash=attestation,
        liquidity_evidence=evidence,
        suspended=quote.suspended,
    )


def match_snapshot_batch(
    *,
    request: SnapshotBatchRequest,
) -> SnapshotBatchResult:
    """Match a mechanically ordered batch against one shared snapshot cap."""

    if type(request) is not SnapshotBatchRequest:
        raise TypeError("request must be exactly SnapshotBatchRequest")
    rebuilt_request = SnapshotBatchRequest(candidates=request.candidates)
    if rebuilt_request != request:
        raise ValueError("snapshot batch request fields or hash were tampered")
    candidates = rebuilt_request.candidates
    base_quote = candidates[0].quote
    base_evidence = base_quote.liquidity_evidence
    shared_cap = base_evidence.liquidity_quantity
    total_fill = 0
    allocations: list[SnapshotBatchAllocation] = []
    for priority, candidate in enumerate(candidates):
        cap_before = shared_cap - total_fill
        derived_quote = _derived_quote(
            candidate.quote,
            batch_filled_quantity=total_fill,
        )
        decision = match_attested_snapshot(
            order=candidate.order,
            quote=derived_quote,
            rule=candidate.rule,
            evaluated_at=candidate.evaluated_at,
        )
        if decision.fill_quantity > cap_before:
            raise RuntimeError("snapshot matcher exceeded the shared liquidity cap")
        total_fill += decision.fill_quantity
        cap_after = shared_cap - total_fill
        allocations.append(
            SnapshotBatchAllocation(
                priority=priority,
                order_id=candidate.order.order_id,
                created_at=candidate.created_at,
                shared_cap_before=cap_before,
                shared_cap_after=cap_after,
                effective_evidence_hash=(
                    derived_quote.liquidity_evidence.evidence_hash
                ),
                decision=decision,
            )
        )
    receipt_hash = base_evidence.source_receipt_hash
    assert receipt_hash is not None
    return SnapshotBatchResult(
        request_hash=rebuilt_request.request_hash,
        instrument_id=base_quote.instrument_id,
        snapshot_id=base_quote.snapshot_id,
        source_receipt_hash=receipt_hash,
        shared_liquidity_cap=shared_cap,
        total_fill_quantity=total_fill,
        allocations=tuple(allocations),
    )


__all__ = [
    "SnapshotBatchAllocation",
    "SnapshotBatchCandidate",
    "SnapshotBatchRequest",
    "SnapshotBatchResult",
    "match_snapshot_batch",
    "validate_snapshot_batch_result",
]
