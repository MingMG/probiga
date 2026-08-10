"""Opt-in exchange-session gate for neutral execution matchers.

The gate is a pure entry layer.  It accepts explicit calendar evidence,
defaults to external receipt *references*, and delegates to existing matchers
only for an ACTIVE session or the non-filling expiry/terminal path.  The
caller remains responsible for resolving each reference against a trusted
boundary; neither a receipt-shaped hash nor this module's content hashes prove
source authority.
Market events presented while a session is inactive are content-bound into the
order's immutable event state so they cannot be changed or reused for a later
fill through this opt-in entry.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import Any

from ..contracts import ExecutionResult
from .matcher import (
    Level1Quote,
    LimitDayMatchRule,
    LimitDayOrder,
    MatchDecision,
    MatchPriceBand,
    MatchReason,
    MatchStatus,
    match_limit_day,
)
from .session_window import (
    SessionWindowAssessment,
    SessionWindowState,
    TradingSessionCalendarEvidence,
    assess_session_window,
)
from .snapshot_batch import (
    SnapshotBatchAllocation,
    SnapshotBatchRequest,
    SnapshotBatchResult,
    match_snapshot_batch,
    validate_snapshot_batch_result,
)
from .snapshot_matcher import (
    AttestedSnapshotQuote,
    SnapshotLiquidityEvidence,
    SnapshotMatchRule,
    match_attested_snapshot,
    snapshot_attestation_hash,
)


class SessionExecutionMode(str, Enum):
    LEVEL1 = "LEVEL1"
    SNAPSHOT = "SNAPSHOT"
    SNAPSHOT_BATCH = "SNAPSHOT_BATCH"


class SessionGateReason(str, Enum):
    NONE = ""
    SESSION_NOT_OBSERVABLE = "SESSION_NOT_OBSERVABLE"
    SESSION_PRE_OPEN = "SESSION_PRE_OPEN"
    SESSION_BREAK = "SESSION_BREAK"
    SESSION_CLOSED = "SESSION_CLOSED"
    EVENT_OUTSIDE_ACTIVE_SESSION = "EVENT_OUTSIDE_ACTIVE_SESSION"
    DUPLICATE_EVENT = "DUPLICATE_EVENT"


_STATE_REASONS = {
    SessionWindowState.NOT_OBSERVABLE: (
        SessionGateReason.SESSION_NOT_OBSERVABLE,
        "calendar evidence was not observable at evaluated_at",
    ),
    SessionWindowState.PRE_OPEN: (
        SessionGateReason.SESSION_PRE_OPEN,
        "exchange session has not opened",
    ),
    SessionWindowState.BREAK: (
        SessionGateReason.SESSION_BREAK,
        "exchange session is in an explicit break",
    ),
    SessionWindowState.CLOSED: (
        SessionGateReason.SESSION_CLOSED,
        "exchange session is closed",
    ),
}


def _aware(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


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
    raise TypeError(f"unsupported session gate hash value: {type(value).__name__}")


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


def _stable_order_payload(order: LimitDayOrder) -> dict[str, Any]:
    return {
        "order_id": order.order_id,
        "intent_id": order.intent_id,
        "instrument_id": order.instrument_id,
        "side": order.side,
        "requested_quantity": order.requested_quantity,
        "approved_quantity": order.approved_quantity,
        "limit_price": order.limit_price,
        "earliest_at": order.earliest_at,
        "expires_at": order.expires_at,
    }


def _order_state_payload(order: LimitDayOrder) -> dict[str, Any]:
    return {
        **_stable_order_payload(order),
        "cumulative_filled_quantity": order.cumulative_filled_quantity,
        "updated_at": order.updated_at,
        "last_source_sequence": order.last_source_sequence,
        "status": order.status,
        "applied_events": order.applied_events,
    }


def _level1_quote_payload(quote: Level1Quote) -> dict[str, Any]:
    return {
        "instrument_id": quote.instrument_id,
        "quote_id": quote.quote_id,
        "observed_at": quote.observed_at,
        "received_at": quote.received_at,
        "bid_price": quote.bid_price,
        "bid_quantity": quote.bid_quantity,
        "ask_price": quote.ask_price,
        "ask_quantity": quote.ask_quantity,
        "suspended": quote.suspended,
    }


def _level1_rule_payload(rule: LimitDayMatchRule) -> dict[str, Any]:
    return {
        "rule_version": rule.rule_version,
        "tick_size": rule.tick_size,
        "quote_max_age": rule.quote_max_age,
        "visible_volume_participation": rule.visible_volume_participation,
        "maximum_fill_quantity": rule.maximum_fill_quantity,
        "price_band": _band_payload(rule.price_band),
        "price_band_max_age": rule.price_band_max_age,
        "require_complete_price_band": rule.require_complete_price_band,
        "enforce_price_band_bounds": rule.enforce_price_band_bounds,
        "slippage_rate": rule.slippage_rate,
        "impact_rate": rule.impact_rate,
        "block_adverse_limit_lock": rule.block_adverse_limit_lock,
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


def _snapshot_quote_payload(quote: AttestedSnapshotQuote) -> dict[str, Any]:
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


def _snapshot_rule_payload(rule: SnapshotMatchRule) -> dict[str, Any]:
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
        "updated_order": _order_state_payload(decision.updated_order),
        "quote_id": decision.quote_id,
        "fill_quantity": decision.fill_quantity,
        "fill_price": decision.fill_price,
        "execution_result": _execution_payload(decision.execution_result),
        "explanation": decision.explanation,
    }


def _reconstruct(value: Any, expected_type: type, field_name: str) -> Any:
    if type(value) is not expected_type:
        raise TypeError(f"{field_name} must be exactly {expected_type.__name__}")
    try:
        rebuilt = replace(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} cannot be reconstructed") from exc
    if rebuilt != value:
        raise ValueError(f"{field_name} contains non-canonical or tampered fields")
    return value


def _validate_band(band: MatchPriceBand | None) -> None:
    if band is not None:
        _reconstruct(band, MatchPriceBand, "price_band")


def _validate_order(order: LimitDayOrder) -> None:
    _reconstruct(order, LimitDayOrder, "order")
    for field_name in (
        "requested_quantity",
        "approved_quantity",
        "cumulative_filled_quantity",
        "last_source_sequence",
    ):
        if type(getattr(order, field_name)) is not int:
            raise TypeError(f"order {field_name} must be exactly int")
    if type(order.applied_events) is not tuple:
        raise TypeError("order applied_events must be exactly tuple")
    for event in order.applied_events:
        if (
            type(event) is not tuple
            or len(event) != 2
            or type(event[0]) is not str
            or type(event[1]) is not str
        ):
            raise TypeError(
                "order applied_events must contain exact (str, str) tuples"
            )


def _validate_level1(
    *,
    order: LimitDayOrder,
    quote: Level1Quote | None,
    rule: LimitDayMatchRule,
) -> None:
    _validate_order(order)
    _reconstruct(rule, LimitDayMatchRule, "rule")
    _validate_band(rule.price_band)
    if quote is not None:
        _reconstruct(quote, Level1Quote, "quote")
        for field_name in ("bid_quantity", "ask_quantity"):
            value = getattr(quote, field_name)
            if value is not None and type(value) is not int:
                raise TypeError(f"quote {field_name} must be exactly int or None")
        if quote.instrument_id != order.instrument_id:
            raise ValueError("quote instrument does not match order")


def _rebuild_snapshot_evidence(
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
        raise ValueError("snapshot evidence fields or derived hashes were tampered")
    return rebuilt


def _validate_snapshot(
    *,
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote | None,
    rule: SnapshotMatchRule,
) -> None:
    _validate_order(order)
    _reconstruct(rule, SnapshotMatchRule, "rule")
    _validate_band(rule.price_band)
    if quote is None:
        return
    _reconstruct(quote, AttestedSnapshotQuote, "quote")
    evidence = _rebuild_snapshot_evidence(quote.liquidity_evidence)
    if quote.instrument_id != order.instrument_id:
        raise ValueError("snapshot instrument does not match order")
    if evidence.source_provider != quote.source:
        raise ValueError(
            "snapshot evidence provider does not match snapshot source"
        )
    expected = snapshot_attestation_hash(
        instrument_id=quote.instrument_id,
        snapshot_id=quote.snapshot_id,
        observed_at=quote.observed_at,
        received_at=quote.received_at,
        last_price=quote.last_price,
        source=quote.source,
        liquidity_evidence_hash=evidence.evidence_hash,
        suspended=quote.suspended,
    )
    if quote.attestation_hash != expected:
        raise ValueError("snapshot attestation hash was tampered")


def _assessment(
    calendar_evidence: TradingSessionCalendarEvidence,
    *,
    evaluated_at: datetime,
    require_external_receipt: bool,
) -> SessionWindowAssessment:
    if type(calendar_evidence) is not TradingSessionCalendarEvidence:
        raise TypeError(
            "calendar_evidence must be exactly TradingSessionCalendarEvidence"
        )
    return assess_session_window(
        calendar_evidence,
        evaluated_at=_aware(evaluated_at, "evaluated_at"),
        require_external_receipt=require_external_receipt,
    )


def _validate_order_calendar_window(
    order: LimitDayOrder,
    calendar_evidence: TradingSessionCalendarEvidence,
) -> None:
    if order.expires_at != calendar_evidence.last_close:
        raise ValueError(
            "DAY order expires_at must equal the calendar final session close"
        )
    boundaries = tuple(
        (
            datetime.combine(
                calendar_evidence.trade_date,
                session.opens_at,
                tzinfo=calendar_evidence.timezone,
            ),
            datetime.combine(
                calendar_evidence.trade_date,
                session.closes_at,
                tzinfo=calendar_evidence.timezone,
            ),
        )
        for session in calendar_evidence.sessions
    )
    if not any(
        opens_at <= order.earliest_at < closes_at
        for opens_at, closes_at in boundaries
    ):
        raise ValueError(
            "DAY order earliest_at must fall inside a calendar execution session"
        )


def _assessment_gate_reason(
    assessment: SessionWindowAssessment,
) -> SessionGateReason:
    if assessment.state == SessionWindowState.ACTIVE:
        return SessionGateReason.NONE
    return _STATE_REASONS[assessment.state][0]


def _gate_fingerprint(
    *,
    mode: SessionExecutionMode,
    order: LimitDayOrder,
    quote_payload: dict[str, Any],
    rule_payload: dict[str, Any],
    calendar_evidence_hash: str,
) -> str:
    return _digest(
        f"trading-core.session-gate-{mode.value.lower()}-event.v1",
        {
            "mode": mode,
            "calendar_evidence_hash": calendar_evidence_hash,
            "order": _stable_order_payload(order),
            "quote": quote_payload,
            "rule": rule_payload,
        },
    )


def _level1_matcher_fingerprint(
    order: LimitDayOrder,
    quote: Level1Quote,
    rule: LimitDayMatchRule,
) -> str:
    """Reproduce the neutral matcher's public event semantics.

    Session-gate events use their own namespace because they additionally bind
    calendar evidence.  This second fingerprint lets the gate safely recognize
    events that were already applied by the underlying matcher before the
    opt-in gate was introduced (or through an ACTIVE invocation).
    """

    return _digest(
        "trading-core.limit-day-match-event.v1",
        {
            "order": _stable_order_payload(order),
            "quote": _level1_quote_payload(quote),
            "rule": _level1_rule_payload(rule),
        },
    )


def _snapshot_matcher_fingerprint(
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote,
    rule: SnapshotMatchRule,
) -> str:
    return _digest(
        "trading-core.snapshot-match-event.v1",
        {
            "order": _stable_order_payload(order),
            "quote": {
                "snapshot_id": quote.snapshot_id,
                "instrument_id": quote.instrument_id,
                "observed_at": quote.observed_at,
                "received_at": quote.received_at,
                "last_price": quote.last_price,
                "source": quote.source,
                "attestation_hash": quote.attestation_hash,
                "liquidity_evidence_hash": (
                    quote.liquidity_evidence.evidence_hash
                ),
                "suspended": quote.suspended,
            },
            "rule": _snapshot_rule_payload(rule),
        },
    )


def _gate_event_key(mode: SessionExecutionMode, event_id: str) -> str:
    return f"session-gate:{mode.value}:{event_id}"


def _duplicate_decision(
    order: LimitDayOrder,
    *,
    event_id: str,
) -> MatchDecision:
    return MatchDecision(
        status=MatchStatus.DUPLICATE,
        reason=MatchReason.DUPLICATE_EVENT,
        updated_order=order,
        quote_id=event_id,
        explanation="identical market event was already observed by session gate",
    )


def _prior_gate_decision(
    order: LimitDayOrder,
    *,
    mode: SessionExecutionMode,
    event_id: str,
    fingerprint: str,
    matcher_fingerprint: str,
) -> MatchDecision | None:
    events = dict(order.applied_events)
    prior = events.get(_gate_event_key(mode, event_id))
    if prior is None:
        matcher_prior = events.get(event_id)
        if matcher_prior is None:
            return None
        if matcher_prior != matcher_fingerprint:
            raise ValueError(
                "market event id was already applied with different semantics"
            )
    elif prior != fingerprint:
        raise ValueError(
            "market event id was already session-gated with different semantics"
        )
    return _duplicate_decision(order, event_id=event_id)


def _session_wait(
    order: LimitDayOrder,
    *,
    mode: SessionExecutionMode,
    event_id: str | None,
    fingerprint: str | None,
    matcher_fingerprint: str | None,
    explanation: str,
) -> MatchDecision:
    updated = order
    quote_id = ""
    if event_id is not None:
        assert fingerprint is not None
        assert matcher_fingerprint is not None
        prior = _prior_gate_decision(
            order,
            mode=mode,
            event_id=event_id,
            fingerprint=fingerprint,
            matcher_fingerprint=matcher_fingerprint,
        )
        if prior is not None:
            return prior
        events = dict(order.applied_events)
        events[_gate_event_key(mode, event_id)] = fingerprint
        updated = replace(order, applied_events=tuple(sorted(events.items())))
        quote_id = event_id
    return MatchDecision(
        status=MatchStatus.WAITING,
        reason=MatchReason.WAIT_NOT_ACTIVE,
        updated_order=updated,
        quote_id=quote_id,
        explanation=explanation,
    )


def _event_matches_current_active_session(
    *,
    calendar_evidence: TradingSessionCalendarEvidence,
    assessment: SessionWindowAssessment,
    observed_at: datetime,
    received_at: datetime,
    require_external_receipt: bool,
) -> bool:
    """Require event observation, receipt, and evaluation in one session.

    Merely evaluating during an ACTIVE session is insufficient: otherwise a
    pre-open, lunch-break, or prior-session quote can be carried into a later
    ACTIVE window and fill.  Future events that belong to the same session are
    still delegated so the neutral matcher can return its existing
    WAIT_FUTURE_QUOTE decision.
    """

    if assessment.state != SessionWindowState.ACTIVE:
        return False
    observed = _assessment(
        calendar_evidence,
        evaluated_at=observed_at,
        require_external_receipt=require_external_receipt,
    )
    received = _assessment(
        calendar_evidence,
        evaluated_at=received_at,
        require_external_receipt=require_external_receipt,
    )
    return (
        observed.state == SessionWindowState.ACTIVE
        and received.state == SessionWindowState.ACTIVE
        and observed.session_id == assessment.session_id
        and received.session_id == assessment.session_id
    )


def _gated_decision_hash(result: SessionGatedMatchDecision) -> str:
    return _digest(
        "trading-core.session-gated-match-decision.v1",
        {
            "mode": result.mode,
            "assessment_hash": result.assessment.assessment_hash,
            "gate_reason": result.gate_reason,
            "decision": _decision_payload(result.decision),
        },
    )


def _validate_gate_reason(
    *,
    assessment: SessionWindowAssessment,
    gate_reason: SessionGateReason,
    has_fill: bool,
    has_duplicate: bool,
) -> None:
    if has_fill and (
        assessment.state != SessionWindowState.ACTIVE
        or gate_reason != SessionGateReason.NONE
    ):
        raise ValueError("only an ACTIVE session with no gate reason may fill")
    if gate_reason == SessionGateReason.NONE:
        if assessment.state != SessionWindowState.ACTIVE:
            raise ValueError("an ungated decision requires an ACTIVE session")
        return
    if gate_reason == SessionGateReason.EVENT_OUTSIDE_ACTIVE_SESSION:
        if assessment.state != SessionWindowState.ACTIVE or has_fill:
            raise ValueError(
                "event-session mismatch requires an ACTIVE assessment and no fill"
            )
        return
    if gate_reason == SessionGateReason.DUPLICATE_EVENT:
        if not has_duplicate or has_fill:
            raise ValueError("duplicate gate reason requires a no-fill duplicate")
        return
    expected = _STATE_REASONS.get(assessment.state)
    if expected is None or expected[0] != gate_reason or has_fill:
        raise ValueError("gate reason does not match the assessed session state")


@dataclass(frozen=True, slots=True)
class SessionGatedMatchDecision:
    """Content-bound gate output; its hash is not a source-authority proof."""

    mode: SessionExecutionMode
    assessment: SessionWindowAssessment
    gate_reason: SessionGateReason
    decision: MatchDecision
    decision_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.mode) is not SessionExecutionMode:
            raise TypeError("mode must be exactly SessionExecutionMode")
        _reconstruct(self.assessment, SessionWindowAssessment, "assessment")
        if type(self.gate_reason) is not SessionGateReason:
            raise TypeError("gate_reason must be exactly SessionGateReason")
        _reconstruct(self.decision, MatchDecision, "decision")
        _validate_order(self.decision.updated_order)
        if self.decision.execution_result is not None:
            _reconstruct(
                self.decision.execution_result,
                ExecutionResult,
                "decision execution_result",
            )
        _validate_gate_reason(
            assessment=self.assessment,
            gate_reason=self.gate_reason,
            has_fill=self.decision.fill_quantity > 0,
            has_duplicate=self.decision.status == MatchStatus.DUPLICATE,
        )
        object.__setattr__(self, "decision_hash", _gated_decision_hash(self))


def _gated_batch_hash(result: SessionGatedSnapshotBatchDecision) -> str:
    return _digest(
        "trading-core.session-gated-snapshot-batch-decision.v1",
        {
            "assessment_hash": result.assessment.assessment_hash,
            "gate_reason": result.gate_reason,
            "batch_result_hash": result.batch_result.result_hash,
        },
    )


@dataclass(frozen=True, slots=True)
class SessionGatedSnapshotBatchDecision:
    """Content-bound batch gate output; hashes do not attest receipt authority."""

    assessment: SessionWindowAssessment
    gate_reason: SessionGateReason
    batch_result: SnapshotBatchResult
    decision_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _reconstruct(self.assessment, SessionWindowAssessment, "assessment")
        if type(self.gate_reason) is not SessionGateReason:
            raise TypeError("gate_reason must be exactly SessionGateReason")
        validate_snapshot_batch_result(self.batch_result)
        _validate_gate_reason(
            assessment=self.assessment,
            gate_reason=self.gate_reason,
            has_fill=self.batch_result.total_fill_quantity > 0,
            has_duplicate=any(
                allocation.decision.status == MatchStatus.DUPLICATE
                for allocation in self.batch_result.allocations
            ),
        )
        object.__setattr__(self, "decision_hash", _gated_batch_hash(self))


def validate_session_gated_match_decision(
    result: SessionGatedMatchDecision,
) -> None:
    """Detect low-level mutation of a gate result or its derived hash."""

    if type(result) is not SessionGatedMatchDecision:
        raise TypeError("result must be exactly SessionGatedMatchDecision")
    rebuilt = SessionGatedMatchDecision(
        mode=result.mode,
        assessment=result.assessment,
        gate_reason=result.gate_reason,
        decision=result.decision,
    )
    if rebuilt != result:
        raise ValueError("session-gated match decision fields or hash were tampered")


def validate_session_gated_snapshot_batch_decision(
    result: SessionGatedSnapshotBatchDecision,
) -> None:
    """Detect low-level mutation of a batch gate result or derived hashes."""

    if type(result) is not SessionGatedSnapshotBatchDecision:
        raise TypeError(
            "result must be exactly SessionGatedSnapshotBatchDecision"
        )
    rebuilt = SessionGatedSnapshotBatchDecision(
        assessment=result.assessment,
        gate_reason=result.gate_reason,
        batch_result=result.batch_result,
    )
    if rebuilt != result:
        raise ValueError(
            "session-gated snapshot batch fields or hash were tampered"
        )


def _wrap_match(
    *,
    mode: SessionExecutionMode,
    assessment: SessionWindowAssessment,
    gate_reason: SessionGateReason,
    decision: MatchDecision,
) -> SessionGatedMatchDecision:
    return SessionGatedMatchDecision(
        mode=mode,
        assessment=assessment,
        gate_reason=gate_reason,
        decision=decision,
    )


def match_limit_day_in_session(
    *,
    order: LimitDayOrder,
    quote: Level1Quote | None,
    rule: LimitDayMatchRule,
    evaluated_at: datetime,
    calendar_evidence: TradingSessionCalendarEvidence,
    require_external_receipt: bool = True,
) -> SessionGatedMatchDecision:
    """Gate one Level-1 match by an explicit exchange-session assessment."""

    _validate_level1(order=order, quote=quote, rule=rule)
    assessment = _assessment(
        calendar_evidence,
        evaluated_at=evaluated_at,
        require_external_receipt=require_external_receipt,
    )
    _validate_order_calendar_window(order, calendar_evidence)
    if assessment.evaluated_at < order.updated_at:
        raise ValueError("evaluated_at cannot precede order updated_at")
    if order.is_terminal or assessment.evaluated_at >= order.expires_at:
        return _wrap_match(
            mode=SessionExecutionMode.LEVEL1,
            assessment=assessment,
            gate_reason=_assessment_gate_reason(assessment),
            decision=match_limit_day(
                order=order,
                quote=quote,
                rule=rule,
                evaluated_at=assessment.evaluated_at,
            ),
        )
    event_id = quote.quote_id if quote is not None else None
    fingerprint = (
        None
        if quote is None
        else _gate_fingerprint(
            mode=SessionExecutionMode.LEVEL1,
            order=order,
            quote_payload=_level1_quote_payload(quote),
            rule_payload=_level1_rule_payload(rule),
            calendar_evidence_hash=assessment.evidence_hash,
        )
    )
    matcher_fingerprint = (
        None
        if quote is None
        else _level1_matcher_fingerprint(order, quote, rule)
    )
    if event_id is not None:
        assert matcher_fingerprint is not None
        prior = _prior_gate_decision(
            order,
            mode=SessionExecutionMode.LEVEL1,
            event_id=event_id,
            fingerprint=fingerprint,
            matcher_fingerprint=matcher_fingerprint,
        )
        if prior is not None:
            return _wrap_match(
                mode=SessionExecutionMode.LEVEL1,
                assessment=assessment,
                gate_reason=SessionGateReason.DUPLICATE_EVENT,
                decision=prior,
            )
    if assessment.state == SessionWindowState.ACTIVE and (
        quote is None
        or _event_matches_current_active_session(
            calendar_evidence=calendar_evidence,
            assessment=assessment,
            observed_at=quote.observed_at,
            received_at=quote.received_at,
            require_external_receipt=require_external_receipt,
        )
    ):
        return _wrap_match(
            mode=SessionExecutionMode.LEVEL1,
            assessment=assessment,
            gate_reason=SessionGateReason.NONE,
            decision=match_limit_day(
                order=order,
                quote=quote,
                rule=rule,
                evaluated_at=assessment.evaluated_at,
            ),
        )
    if assessment.state == SessionWindowState.ACTIVE:
        assert quote is not None
        return _wrap_match(
            mode=SessionExecutionMode.LEVEL1,
            assessment=assessment,
            gate_reason=SessionGateReason.EVENT_OUTSIDE_ACTIVE_SESSION,
            decision=_session_wait(
                order,
                mode=SessionExecutionMode.LEVEL1,
                event_id=event_id,
                fingerprint=fingerprint,
                matcher_fingerprint=matcher_fingerprint,
                explanation=(
                    "market event observation and receipt must belong to the "
                    "currently evaluated ACTIVE exchange session"
                ),
            ),
        )
    gate_reason, explanation = _STATE_REASONS[assessment.state]
    return _wrap_match(
        mode=SessionExecutionMode.LEVEL1,
        assessment=assessment,
        gate_reason=gate_reason,
        decision=_session_wait(
            order,
            mode=SessionExecutionMode.LEVEL1,
            event_id=event_id,
            fingerprint=fingerprint,
            matcher_fingerprint=matcher_fingerprint,
            explanation=explanation,
        ),
    )


def match_attested_snapshot_in_session(
    *,
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote | None,
    rule: SnapshotMatchRule,
    evaluated_at: datetime,
    calendar_evidence: TradingSessionCalendarEvidence,
    require_external_receipt: bool = True,
) -> SessionGatedMatchDecision:
    """Gate one explicitly enabled snapshot match by exchange session."""

    _validate_snapshot(order=order, quote=quote, rule=rule)
    assessment = _assessment(
        calendar_evidence,
        evaluated_at=evaluated_at,
        require_external_receipt=require_external_receipt,
    )
    _validate_order_calendar_window(order, calendar_evidence)
    if assessment.evaluated_at < order.updated_at:
        raise ValueError("evaluated_at cannot precede order updated_at")
    if order.is_terminal or assessment.evaluated_at >= order.expires_at:
        return _wrap_match(
            mode=SessionExecutionMode.SNAPSHOT,
            assessment=assessment,
            gate_reason=_assessment_gate_reason(assessment),
            decision=match_attested_snapshot(
                order=order,
                quote=quote,
                rule=rule,
                evaluated_at=assessment.evaluated_at,
            ),
        )
    event_id = quote.snapshot_id if quote is not None else None
    fingerprint = (
        None
        if quote is None
        else _gate_fingerprint(
            mode=SessionExecutionMode.SNAPSHOT,
            order=order,
            quote_payload=_snapshot_quote_payload(quote),
            rule_payload=_snapshot_rule_payload(rule),
            calendar_evidence_hash=assessment.evidence_hash,
        )
    )
    matcher_fingerprint = (
        None
        if quote is None
        else _snapshot_matcher_fingerprint(order, quote, rule)
    )
    if event_id is not None:
        assert matcher_fingerprint is not None
        prior = _prior_gate_decision(
            order,
            mode=SessionExecutionMode.SNAPSHOT,
            event_id=event_id,
            fingerprint=fingerprint,
            matcher_fingerprint=matcher_fingerprint,
        )
        if prior is not None:
            return _wrap_match(
                mode=SessionExecutionMode.SNAPSHOT,
                assessment=assessment,
                gate_reason=SessionGateReason.DUPLICATE_EVENT,
                decision=prior,
            )
    if assessment.state == SessionWindowState.ACTIVE and (
        quote is None
        or _event_matches_current_active_session(
            calendar_evidence=calendar_evidence,
            assessment=assessment,
            observed_at=quote.observed_at,
            received_at=quote.received_at,
            require_external_receipt=require_external_receipt,
        )
    ):
        return _wrap_match(
            mode=SessionExecutionMode.SNAPSHOT,
            assessment=assessment,
            gate_reason=SessionGateReason.NONE,
            decision=match_attested_snapshot(
                order=order,
                quote=quote,
                rule=rule,
                evaluated_at=assessment.evaluated_at,
            ),
        )
    if assessment.state == SessionWindowState.ACTIVE:
        assert quote is not None
        return _wrap_match(
            mode=SessionExecutionMode.SNAPSHOT,
            assessment=assessment,
            gate_reason=SessionGateReason.EVENT_OUTSIDE_ACTIVE_SESSION,
            decision=_session_wait(
                order,
                mode=SessionExecutionMode.SNAPSHOT,
                event_id=event_id,
                fingerprint=fingerprint,
                matcher_fingerprint=matcher_fingerprint,
                explanation=(
                    "market event observation and receipt must belong to the "
                    "currently evaluated ACTIVE exchange session"
                ),
            ),
        )
    gate_reason, explanation = _STATE_REASONS[assessment.state]
    return _wrap_match(
        mode=SessionExecutionMode.SNAPSHOT,
        assessment=assessment,
        gate_reason=gate_reason,
        decision=_session_wait(
            order,
            mode=SessionExecutionMode.SNAPSHOT,
            event_id=event_id,
            fingerprint=fingerprint,
            matcher_fingerprint=matcher_fingerprint,
            explanation=explanation,
        ),
    )


def _manual_batch_result(
    request: SnapshotBatchRequest,
    decisions: tuple[MatchDecision, ...],
) -> SnapshotBatchResult:
    if len(decisions) != len(request.candidates):
        raise ValueError("manual batch decisions must match candidate count")
    shared_cap = request.candidates[0].quote.liquidity_evidence.liquidity_quantity
    allocations = tuple(
        SnapshotBatchAllocation(
            priority=priority,
            order_id=candidate.order.order_id,
            created_at=candidate.created_at,
            shared_cap_before=shared_cap,
            shared_cap_after=shared_cap,
            effective_evidence_hash=(
                candidate.quote.liquidity_evidence.evidence_hash
            ),
            decision=decision,
        )
        for priority, (candidate, decision) in enumerate(
            zip(request.candidates, decisions)
        )
    )
    receipt_hash = (
        request.candidates[0].quote.liquidity_evidence.source_receipt_hash
    )
    assert receipt_hash is not None
    return SnapshotBatchResult(
        request_hash=request.request_hash,
        instrument_id=request.candidates[0].quote.instrument_id,
        snapshot_id=request.candidates[0].quote.snapshot_id,
        source_receipt_hash=receipt_hash,
        shared_liquidity_cap=shared_cap,
        total_fill_quantity=0,
        allocations=allocations,
    )


def match_snapshot_batch_in_session(
    *,
    request: SnapshotBatchRequest,
    calendar_evidence: TradingSessionCalendarEvidence,
    require_external_receipt: bool = True,
) -> SessionGatedSnapshotBatchDecision:
    """Gate one closed snapshot batch by an explicit exchange session."""

    if type(request) is not SnapshotBatchRequest:
        raise TypeError("request must be exactly SnapshotBatchRequest")
    rebuilt = SnapshotBatchRequest(candidates=request.candidates)
    if rebuilt != request:
        raise ValueError("snapshot batch request fields or hash were tampered")
    evaluated_at = rebuilt.candidates[0].evaluated_at
    assessment = _assessment(
        calendar_evidence,
        evaluated_at=evaluated_at,
        require_external_receipt=require_external_receipt,
    )
    for candidate in rebuilt.candidates:
        _validate_order_calendar_window(candidate.order, calendar_evidence)
        if evaluated_at < candidate.order.updated_at:
            raise ValueError("evaluated_at cannot precede order updated_at")

    live: list[tuple[Any, str, str, str, MatchDecision | None]] = []
    for candidate in rebuilt.candidates:
        order = candidate.order
        if order.is_terminal or evaluated_at >= order.expires_at:
            continue
        fingerprint = _gate_fingerprint(
            mode=SessionExecutionMode.SNAPSHOT,
            order=order,
            quote_payload=_snapshot_quote_payload(candidate.quote),
            rule_payload=_snapshot_rule_payload(candidate.rule),
            calendar_evidence_hash=assessment.evidence_hash,
        )
        matcher_fingerprint = _snapshot_matcher_fingerprint(
            order,
            candidate.quote,
            candidate.rule,
        )
        prior = _prior_gate_decision(
            order,
            mode=SessionExecutionMode.SNAPSHOT,
            event_id=candidate.quote.snapshot_id,
            fingerprint=fingerprint,
            matcher_fingerprint=matcher_fingerprint,
        )
        live.append(
            (
                candidate,
                candidate.quote.snapshot_id,
                fingerprint,
                matcher_fingerprint,
                prior,
            )
        )

    event_in_active_session = (
        assessment.state == SessionWindowState.ACTIVE
        and _event_matches_current_active_session(
            calendar_evidence=calendar_evidence,
            assessment=assessment,
            observed_at=rebuilt.candidates[0].quote.observed_at,
            received_at=rebuilt.candidates[0].quote.received_at,
            require_external_receipt=require_external_receipt,
        )
    )
    if assessment.state == SessionWindowState.ACTIVE:
        prior_count = sum(
            1 for _, _, _, _, prior in live if prior is not None
        )
        if prior_count and prior_count != len(live):
            raise ValueError(
                "active snapshot batch cannot mix session-gated replay and "
                "new candidates"
            )
        if not prior_count and event_in_active_session:
            return SessionGatedSnapshotBatchDecision(
                assessment=assessment,
                gate_reason=SessionGateReason.NONE,
                batch_result=match_snapshot_batch(request=rebuilt),
            )

    decisions: list[MatchDecision] = []
    any_new_gate = False
    any_duplicate = False
    live_by_order = {
        candidate.order.order_id: (
            event_id,
            fingerprint,
            matcher_fingerprint,
            prior,
        )
        for candidate, event_id, fingerprint, matcher_fingerprint, prior in live
    }
    for candidate in rebuilt.candidates:
        state = live_by_order.get(candidate.order.order_id)
        if state is None:
            decision = match_attested_snapshot(
                order=candidate.order,
                quote=candidate.quote,
                rule=candidate.rule,
                evaluated_at=evaluated_at,
            )
        else:
            event_id, fingerprint, matcher_fingerprint, prior = state
            if prior is not None:
                decision = prior
                any_duplicate = True
            else:
                if assessment.state == SessionWindowState.ACTIVE:
                    explanation = (
                        "market event observation and receipt must belong to "
                        "the currently evaluated ACTIVE exchange session"
                    )
                else:
                    _, explanation = _STATE_REASONS[assessment.state]
                decision = _session_wait(
                    candidate.order,
                    mode=SessionExecutionMode.SNAPSHOT,
                    event_id=event_id,
                    fingerprint=fingerprint,
                    matcher_fingerprint=matcher_fingerprint,
                    explanation=explanation,
                )
                any_new_gate = True
        decisions.append(decision)

    if any_new_gate:
        gate_reason = (
            SessionGateReason.EVENT_OUTSIDE_ACTIVE_SESSION
            if assessment.state == SessionWindowState.ACTIVE
            else _STATE_REASONS[assessment.state][0]
        )
    elif any_duplicate:
        gate_reason = SessionGateReason.DUPLICATE_EVENT
    else:
        gate_reason = _assessment_gate_reason(assessment)
    return SessionGatedSnapshotBatchDecision(
        assessment=assessment,
        gate_reason=gate_reason,
        batch_result=_manual_batch_result(rebuilt, tuple(decisions)),
    )


__all__ = [
    "SessionExecutionMode",
    "SessionGateReason",
    "SessionGatedMatchDecision",
    "SessionGatedSnapshotBatchDecision",
    "match_attested_snapshot_in_session",
    "match_limit_day_in_session",
    "match_snapshot_batch_in_session",
    "validate_session_gated_match_decision",
    "validate_session_gated_snapshot_batch_decision",
]
