"""Conservative matching against an explicitly attested market snapshot.

A snapshot is not Level-1 liquidity and must never be selected implicitly.
This pure matcher therefore requires an enabled rule, an allowed source, a
content attestation, and content-bound liquidity evidence.  The hashes in this
module detect changed content; they do not authenticate a provider or prove
that a receipt came from an authoritative repository.  That verification must
happen at the integration boundary before it constructs an external-receipt
reference.  The matcher owns no clock, account, repository, strategy, or
broker I/O.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from enum import Enum
import hashlib
import json
from typing import Any

from ..contracts import (
    ExecutionResult,
    OrderSide,
    OrderStatus,
    execution_result_idempotency_key,
)
from .matcher import (
    LimitDayOrder,
    MatchDecision,
    MatchPriceBand,
    MatchReason,
    MatchStatus,
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


def _decimal(
    value: object,
    field_name: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-like")
    if type(value) is Decimal:
        converted = value
    elif isinstance(value, Decimal):
        raise TypeError(f"{field_name} must not be a Decimal subclass")
    elif type(value) in {str, int, float}:
        try:
            converted = Decimal(str(value))
        except Exception as exc:
            raise TypeError(f"{field_name} must be decimal-like") from exc
    else:
        raise TypeError(f"{field_name} must be decimal-like")
    if not converted.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if positive and converted <= 0:
        raise ValueError(f"{field_name} must be positive")
    if non_negative and converted < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return converted


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


def _integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be exactly int")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _require_reconstructable(
    value: Any,
    expected_type: type,
    field_name: str,
) -> Any:
    """Rerun frozen dataclass invariants before trusting executable content."""

    if type(value) is not expected_type:
        raise TypeError(
            f"{field_name} must be exactly {expected_type.__name__}"
        )
    try:
        reconstructed = replace(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} cannot be reconstructed") from exc
    if reconstructed != value:
        raise ValueError(
            f"{field_name} differs from its canonical reconstructed value"
        )
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
    raise TypeError(f"unsupported snapshot hash value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class SnapshotEvidenceKind(str, Enum):
    EXTERNAL_RECEIPT_REFERENCE = "EXTERNAL_RECEIPT_REFERENCE"
    SYNTHETIC_STANDALONE_COMPATIBILITY = "SYNTHETIC_STANDALONE_COMPATIBILITY"


@dataclass(frozen=True, slots=True)
class SnapshotLiquidityEvidence:
    """Content-bound inputs used to derive a snapshot quantity cap.

    ``SYNTHETIC_STANDALONE_COMPATIBILITY`` exists only for read-only golden
    tests against V2's standalone matcher.  Neutral production rules reject it
    by default.  It carries the frozen V2 matcher's naked cap in the explicitly
    named ``standalone_compatibility_quantity`` field and must not invent a
    receipt or pretend that the cap came from source volume.

    ``EXTERNAL_RECEIPT_REFERENCE`` records the digest of a receipt that an
    integration boundary has already verified.  This dataclass checks format
    and binds content only.  Neither ``source_payload_hash``,
    ``source_receipt_hash``, nor ``evidence_hash`` is authority proof by itself.
    """

    evidence_kind: SnapshotEvidenceKind
    source_provider: str
    source_batch_id: str
    source_payload_hash: str
    source_receipt_hash: str | None
    quality_status: str
    source_count: int
    source_volume: int | None = None
    lot_size: int | None = None
    participation_rate: Decimal | None = None
    already_filled_quantity: int | None = None
    standalone_compatibility_quantity: int | None = None
    liquidity_quantity: int = field(init=False)
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.evidence_kind) is SnapshotEvidenceKind:
            evidence_kind = self.evidence_kind
        elif type(self.evidence_kind) is str:
            evidence_kind = SnapshotEvidenceKind(self.evidence_kind)
        else:
            raise TypeError("evidence_kind must be SnapshotEvidenceKind or str")
        object.__setattr__(self, "evidence_kind", evidence_kind)
        for field_name in (
            "source_provider",
            "source_batch_id",
            "quality_status",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "source_payload_hash",
            _sha256(self.source_payload_hash, "source_payload_hash"),
        )
        source_count = _integer(self.source_count, "source_count", minimum=1)
        if evidence_kind == SnapshotEvidenceKind.EXTERNAL_RECEIPT_REFERENCE:
            if self.source_receipt_hash is None:
                raise ValueError(
                    "external receipt evidence requires source_receipt_hash"
                )
            receipt_hash = _sha256(
                self.source_receipt_hash,
                "source_receipt_hash",
            )
            if self.quality_status != "PASS":
                raise ValueError(
                    "external snapshot liquidity evidence quality must be PASS"
                )
            required_derivation = {
                "source_volume": self.source_volume,
                "lot_size": self.lot_size,
                "participation_rate": self.participation_rate,
                "already_filled_quantity": self.already_filled_quantity,
            }
            missing = sorted(
                name for name, value in required_derivation.items() if value is None
            )
            if missing:
                raise ValueError(
                    "external receipt evidence requires " + ", ".join(missing)
                )
            if self.standalone_compatibility_quantity is not None:
                raise ValueError(
                    "external receipt evidence cannot carry a standalone "
                    "compatibility quantity"
                )
            source_volume = _integer(self.source_volume, "source_volume")
            lot_size = _integer(self.lot_size, "lot_size", minimum=1)
            already_filled = _integer(
                self.already_filled_quantity,
                "already_filled_quantity",
            )
            participation = _decimal(
                self.participation_rate,
                "participation_rate",
                non_negative=True,
            )
            if participation > 1:
                raise ValueError("participation_rate cannot exceed one")
            participation_cap = int(Decimal(source_volume) * participation)
            participation_cap -= participation_cap % lot_size
            liquidity_quantity = max(0, participation_cap - already_filled)
            compatibility_quantity = None
        else:
            if self.source_receipt_hash is not None:
                raise ValueError(
                    "synthetic standalone evidence must not invent a receipt hash"
                )
            receipt_hash = None
            if self.quality_status != "NOT_ASSESSED":
                raise ValueError(
                    "synthetic standalone evidence quality must be NOT_ASSESSED"
                )
            derivation_values = {
                "source_volume": self.source_volume,
                "lot_size": self.lot_size,
                "participation_rate": self.participation_rate,
                "already_filled_quantity": self.already_filled_quantity,
            }
            supplied = sorted(
                name for name, value in derivation_values.items() if value is not None
            )
            if supplied:
                raise ValueError(
                    "synthetic standalone evidence cannot claim derived inputs: "
                    + ", ".join(supplied)
                )
            if self.standalone_compatibility_quantity is None:
                raise ValueError(
                    "synthetic standalone evidence requires "
                    "standalone_compatibility_quantity"
                )
            compatibility_quantity = _integer(
                self.standalone_compatibility_quantity,
                "standalone_compatibility_quantity",
            )
            source_volume = None
            lot_size = None
            participation = None
            already_filled = None
            liquidity_quantity = compatibility_quantity
        evidence_hash = _digest(
            "trading-core.snapshot-liquidity-evidence.v1",
            {
                "evidence_kind": self.evidence_kind,
                "source_provider": self.source_provider,
                "source_batch_id": self.source_batch_id,
                "source_payload_hash": self.source_payload_hash,
                "source_receipt_hash": receipt_hash,
                "quality_status": self.quality_status,
                "source_count": source_count,
                "source_volume": source_volume,
                "lot_size": lot_size,
                "participation_rate": participation,
                "already_filled_quantity": already_filled,
                "standalone_compatibility_quantity": compatibility_quantity,
                "liquidity_quantity": liquidity_quantity,
            },
        )
        object.__setattr__(self, "source_receipt_hash", receipt_hash)
        object.__setattr__(self, "source_volume", source_volume)
        object.__setattr__(self, "lot_size", lot_size)
        object.__setattr__(self, "participation_rate", participation)
        object.__setattr__(self, "already_filled_quantity", already_filled)
        object.__setattr__(
            self,
            "standalone_compatibility_quantity",
            compatibility_quantity,
        )
        object.__setattr__(self, "liquidity_quantity", liquidity_quantity)
        object.__setattr__(self, "evidence_hash", evidence_hash)


@dataclass(frozen=True, slots=True)
class AttestedSnapshotQuote:
    """Executable snapshot fields bound by a deterministic content digest.

    ``attestation_hash`` detects mutation.  It does not authenticate ``source``;
    source authority and any external receipt must be verified by the adapter.
    """

    instrument_id: str
    snapshot_id: str
    observed_at: datetime
    received_at: datetime
    last_price: Decimal | None
    source: str
    attestation_hash: str
    liquidity_evidence: SnapshotLiquidityEvidence
    suspended: bool = False

    def __post_init__(self) -> None:
        for field_name in ("instrument_id", "snapshot_id", "source"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        _aware(self.observed_at, "observed_at")
        _aware(self.received_at, "received_at")
        if self.received_at < self.observed_at:
            raise ValueError("received_at cannot precede observed_at")
        if self.last_price is not None:
            object.__setattr__(
                self,
                "last_price",
                _decimal(self.last_price, "last_price", positive=True),
            )
        object.__setattr__(
            self,
            "attestation_hash",
            _sha256(self.attestation_hash, "attestation_hash"),
        )
        if type(self.liquidity_evidence) is not SnapshotLiquidityEvidence:
            raise TypeError(
                "liquidity_evidence must be exactly SnapshotLiquidityEvidence"
            )
        _require_reconstructable(
            self.liquidity_evidence,
            SnapshotLiquidityEvidence,
            "liquidity_evidence",
        )
        if type(self.suspended) is not bool:
            raise TypeError("suspended must be exactly bool")


@dataclass(frozen=True, slots=True)
class SnapshotMatchRule:
    rule_version: str
    enabled: bool
    tick_size: Decimal
    quote_max_age: timedelta
    allowed_sources: tuple[str, ...]
    allow_synthetic_compatibility_evidence: bool = False
    slippage_rate: Decimal = Decimal("0")
    price_band: MatchPriceBand | None = None
    price_band_max_age: timedelta | None = None
    require_complete_price_band: bool = True
    enforce_price_band_bounds: bool = True
    block_adverse_limit_lock: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rule_version",
            _text(self.rule_version, "rule_version"),
        )
        if type(self.enabled) is not bool:
            raise TypeError("enabled must be exactly bool")
        object.__setattr__(
            self,
            "tick_size",
            _decimal(self.tick_size, "tick_size", positive=True),
        )
        if type(self.quote_max_age) is not timedelta:
            raise TypeError("quote_max_age must be exactly timedelta")
        if self.quote_max_age < timedelta(0):
            raise ValueError("quote_max_age cannot be negative")
        if type(self.allowed_sources) is not tuple:
            raise TypeError("allowed_sources must be a tuple")
        sources = tuple(_text(item, "allowed_source") for item in self.allowed_sources)
        if len(sources) != len(set(sources)):
            raise ValueError("allowed_sources must be unique")
        if self.enabled and not sources:
            raise ValueError("enabled snapshot matching requires an allowed source")
        object.__setattr__(self, "allowed_sources", tuple(sorted(sources)))
        object.__setattr__(
            self,
            "slippage_rate",
            _decimal(
                self.slippage_rate,
                "slippage_rate",
                non_negative=True,
            ),
        )
        if self.slippage_rate >= 1:
            raise ValueError("slippage_rate must be below one")
        if self.price_band is not None and type(self.price_band) is not MatchPriceBand:
            raise TypeError("price_band must be exactly MatchPriceBand or None")
        if self.price_band is not None:
            _require_reconstructable(
                self.price_band,
                MatchPriceBand,
                "price_band",
            )
        if self.price_band_max_age is not None:
            if type(self.price_band_max_age) is not timedelta:
                raise TypeError("price_band_max_age must be exactly timedelta or None")
            if self.price_band_max_age < timedelta(0):
                raise ValueError("price_band_max_age cannot be negative")
        for field_name in (
            "allow_synthetic_compatibility_evidence",
            "require_complete_price_band",
            "enforce_price_band_bounds",
            "block_adverse_limit_lock",
        ):
            if type(getattr(self, field_name)) is not bool:
                raise TypeError(f"{field_name} must be exactly bool")


def snapshot_attestation_hash(
    *,
    instrument_id: str,
    snapshot_id: str,
    observed_at: datetime,
    received_at: datetime,
    last_price: Decimal | None,
    source: str,
    liquidity_evidence_hash: str,
    suspended: bool,
) -> str:
    """Hash executable fields for mutation detection, not source authority."""

    if type(suspended) is not bool:
        raise TypeError("suspended must be exactly bool")

    return _digest(
        "trading-core.market-snapshot-attestation.v1",
        {
            "instrument_id": _text(instrument_id, "instrument_id"),
            "snapshot_id": _text(snapshot_id, "snapshot_id"),
            "observed_at": _aware(observed_at, "observed_at"),
            "received_at": _aware(received_at, "received_at"),
            "last_price": (
                None
                if last_price is None
                else _decimal(last_price, "last_price", positive=True)
            ),
            "source": _text(source, "source"),
            "liquidity_evidence_hash": _sha256(
                liquidity_evidence_hash,
                "liquidity_evidence_hash",
            ),
            "suspended": suspended,
        },
    )


def _event_fingerprint(
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote,
    rule: SnapshotMatchRule,
) -> str:
    band = rule.price_band
    return _digest(
        "trading-core.snapshot-match-event.v1",
        {
            "order": {
                "order_id": order.order_id,
                "intent_id": order.intent_id,
                "instrument_id": order.instrument_id,
                "side": order.side,
                "requested_quantity": order.requested_quantity,
                "approved_quantity": order.approved_quantity,
                "limit_price": order.limit_price,
                "earliest_at": order.earliest_at,
                "expires_at": order.expires_at,
            },
            "quote": {
                "snapshot_id": quote.snapshot_id,
                "instrument_id": quote.instrument_id,
                "observed_at": quote.observed_at,
                "received_at": quote.received_at,
                "last_price": quote.last_price,
                "source": quote.source,
                "attestation_hash": quote.attestation_hash,
                "liquidity_evidence_hash": quote.liquidity_evidence.evidence_hash,
                "suspended": quote.suspended,
            },
            "rule": {
                "rule_version": rule.rule_version,
                "enabled": rule.enabled,
                "tick_size": rule.tick_size,
                "quote_max_age": rule.quote_max_age,
                "allowed_sources": rule.allowed_sources,
                "allow_synthetic_compatibility_evidence": (
                    rule.allow_synthetic_compatibility_evidence
                ),
                "slippage_rate": rule.slippage_rate,
                "price_band": (
                    None
                    if band is None
                    else {
                        "instrument_id": band.instrument_id,
                        "trade_date": band.trade_date,
                        "as_of": band.as_of,
                        "source": band.source,
                        "lower": band.lower,
                        "upper": band.upper,
                    }
                ),
                "price_band_max_age": rule.price_band_max_age,
                "require_complete_price_band": rule.require_complete_price_band,
                "enforce_price_band_bounds": rule.enforce_price_band_bounds,
                "block_adverse_limit_lock": rule.block_adverse_limit_lock,
            },
        },
    )


def _wait(
    order: LimitDayOrder,
    reason: MatchReason,
    explanation: str,
    quote: AttestedSnapshotQuote | None = None,
) -> MatchDecision:
    return MatchDecision(
        status=MatchStatus.WAITING,
        reason=reason,
        updated_order=order,
        quote_id=quote.snapshot_id if quote is not None else "",
        explanation=explanation,
    )


def _wait_seen(
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote,
    fingerprint: str,
    reason: MatchReason,
    explanation: str,
) -> MatchDecision:
    events = dict(order.applied_events)
    prior = events.get(quote.snapshot_id)
    if prior is not None:
        if prior != fingerprint:
            raise ValueError(
                "snapshot_id was already applied with different semantics"
            )
        return MatchDecision(
            status=MatchStatus.DUPLICATE,
            reason=MatchReason.DUPLICATE_EVENT,
            updated_order=order,
            quote_id=quote.snapshot_id,
            explanation="identical snapshot event was already applied",
        )
    events[quote.snapshot_id] = fingerprint
    return _wait(
        replace(order, applied_events=tuple(sorted(events.items()))),
        reason,
        explanation,
        quote,
    )


def _expire(order: LimitDayOrder, *, evaluated_at: datetime) -> MatchDecision:
    event_id = (
        "limit-day-expiry:"
        f"{order.order_id}:"
        f"{order.expires_at.astimezone(timezone.utc).isoformat(timespec='microseconds')}"
    )
    result = ExecutionResult(
        intent_id=order.intent_id,
        order_id=order.order_id,
        event_id=event_id,
        status=OrderStatus.EXPIRED,
        occurred_at=order.expires_at,
        received_at=evaluated_at,
        source_sequence=order.last_source_sequence + 1,
        idempotency_key=execution_result_idempotency_key(
            order_id=order.order_id,
            event_id=event_id,
        ),
        reason_code="DAY_EXPIRED",
    )
    fingerprint = _digest(
        "trading-core.limit-day-expiry-event.v1",
        {
            "order_id": order.order_id,
            "intent_id": order.intent_id,
            "expires_at": order.expires_at,
        },
    )
    events = dict(order.applied_events)
    events[event_id] = fingerprint
    updated = replace(
        order,
        status=OrderStatus.EXPIRED,
        updated_at=order.expires_at,
        last_source_sequence=result.source_sequence,
        applied_events=tuple(sorted(events.items())),
    )
    return MatchDecision(
        status=MatchStatus.EXPIRED,
        reason=MatchReason.NONE,
        updated_order=updated,
        execution_result=result,
        explanation="DAY order reached its exclusive expiry boundary",
    )


def _adverse_price(
    price: Decimal,
    *,
    side: OrderSide,
    tick_size: Decimal,
    slippage_rate: Decimal,
) -> Decimal:
    multiplier = (
        Decimal("1") + slippage_rate
        if side == OrderSide.BUY
        else Decimal("1") - slippage_rate
    )
    rounding = ROUND_UP if side == OrderSide.BUY else ROUND_DOWN
    return (
        (price * multiplier / tick_size).to_integral_value(rounding=rounding)
        * tick_size
    )


def _usable_band(
    *,
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote,
    rule: SnapshotMatchRule,
    evaluated_at: datetime,
) -> tuple[MatchPriceBand | None, str]:
    band = rule.price_band
    if band is None:
        if rule.require_complete_price_band:
            return None, "a complete price band is required but unavailable"
        return None, ""
    if band.instrument_id != order.instrument_id:
        raise ValueError("price band instrument does not match order")
    if band.trade_date != quote.observed_at.date():
        return None, "price band trade_date does not match snapshot"
    if rule.require_complete_price_band and not band.complete:
        return None, "a complete price band is required"
    if rule.price_band_max_age is None:
        return None, "price band maximum age is required"
    if not band.is_fresh(
        evaluated_at=evaluated_at,
        max_age=rule.price_band_max_age,
    ):
        return None, "price band is future-dated or stale"
    if rule.enforce_price_band_bounds and not band.contains(order.limit_price):
        return None, "order limit price is outside the price band"
    return band, ""


def _validate_snapshot_content(
    *,
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote,
) -> None:
    if quote.instrument_id != order.instrument_id:
        raise ValueError("snapshot instrument does not match order")
    expected_attestation = snapshot_attestation_hash(
        instrument_id=quote.instrument_id,
        snapshot_id=quote.snapshot_id,
        observed_at=quote.observed_at,
        received_at=quote.received_at,
        last_price=quote.last_price,
        source=quote.source,
        liquidity_evidence_hash=quote.liquidity_evidence.evidence_hash,
        suspended=quote.suspended,
    )
    if quote.attestation_hash != expected_attestation:
        raise ValueError("snapshot attestation does not match executable fields")
    if quote.liquidity_evidence.source_provider != quote.source:
        raise ValueError(
            "snapshot liquidity evidence provider does not match snapshot source"
        )


def match_attested_snapshot(
    *,
    order: LimitDayOrder,
    quote: AttestedSnapshotQuote | None,
    rule: SnapshotMatchRule,
    evaluated_at: datetime,
) -> MatchDecision:
    """Match one LIMIT+DAY order against one explicitly attested snapshot."""

    if type(order) is not LimitDayOrder:
        raise TypeError("order must be exactly LimitDayOrder")
    if quote is not None and type(quote) is not AttestedSnapshotQuote:
        raise TypeError("quote must be exactly AttestedSnapshotQuote or None")
    if type(rule) is not SnapshotMatchRule:
        raise TypeError("rule must be exactly SnapshotMatchRule")
    _require_reconstructable(order, LimitDayOrder, "order")
    if quote is not None:
        _require_reconstructable(
            quote,
            AttestedSnapshotQuote,
            "quote",
        )
    _require_reconstructable(rule, SnapshotMatchRule, "rule")
    evaluated_at = _aware(evaluated_at, "evaluated_at")
    if evaluated_at < order.updated_at:
        raise ValueError("evaluated_at cannot precede order updated_at")

    fingerprint = None
    prior = None
    if quote is not None:
        fingerprint = _event_fingerprint(order, quote, rule)
        prior = dict(order.applied_events).get(quote.snapshot_id)
        if prior is not None and prior != fingerprint:
            raise ValueError(
                "snapshot_id was already applied with different semantics"
            )
    if order.is_terminal:
        if quote is not None and prior is not None:
            return MatchDecision(
                status=MatchStatus.DUPLICATE,
                reason=MatchReason.DUPLICATE_EVENT,
                updated_order=order,
                quote_id=quote.snapshot_id,
                explanation="identical snapshot event was already applied",
            )
        return MatchDecision(
            status=MatchStatus.TERMINAL,
            reason=MatchReason.ORDER_TERMINAL,
            updated_order=order,
            quote_id=quote.snapshot_id if quote is not None else "",
            explanation=f"order is already terminal with status {order.status.value}",
        )
    if evaluated_at < order.earliest_at:
        if quote is None:
            return _wait(
                order,
                MatchReason.WAIT_NOT_ACTIVE,
                "order has not reached earliest_at",
            )
        _validate_snapshot_content(order=order, quote=quote)
        assert fingerprint is not None
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_NOT_ACTIVE,
            "order has not reached earliest_at",
        )
    if evaluated_at >= order.expires_at:
        return _expire(order, evaluated_at=evaluated_at)
    if not rule.enabled:
        return _wait(
            order,
            MatchReason.WAIT_NO_QUOTE,
            "snapshot matching is explicitly disabled",
        )
    if quote is None:
        return _wait(
            order,
            MatchReason.WAIT_NO_QUOTE,
            "attested market snapshot is missing",
        )
    _validate_snapshot_content(order=order, quote=quote)
    assert fingerprint is not None
    if prior is not None:
        return MatchDecision(
            status=MatchStatus.DUPLICATE,
            reason=MatchReason.DUPLICATE_EVENT,
            updated_order=order,
            quote_id=quote.snapshot_id,
            explanation="identical snapshot event was already applied",
        )
    if (
        quote.liquidity_evidence.evidence_kind
        == SnapshotEvidenceKind.SYNTHETIC_STANDALONE_COMPATIBILITY
        and not rule.allow_synthetic_compatibility_evidence
    ):
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_NO_QUOTE,
            "synthetic snapshot evidence is disabled",
        )
    if quote.source not in rule.allowed_sources:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_NO_QUOTE,
            "snapshot source is not allowed by the rule",
        )
    if quote.last_price is None:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_NO_QUOTE,
            "snapshot last price is missing",
        )
    if quote.observed_at > evaluated_at or quote.received_at > evaluated_at:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_FUTURE_QUOTE,
            "snapshot was not observable at evaluated_at",
        )
    if quote.observed_at < order.earliest_at:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_PRE_ORDER_QUOTE,
            "snapshot precedes the order execution window",
        )
    if quote.observed_at < order.updated_at:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_OUT_OF_ORDER_QUOTE,
            "snapshot precedes the current order state",
        )
    if evaluated_at - quote.observed_at > rule.quote_max_age:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_STALE_QUOTE,
            "snapshot exceeds quote_max_age",
        )
    if quote.suspended:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_SUSPENDED,
            "instrument is suspended",
        )
    band, band_error = _usable_band(
        order=order,
        quote=quote,
        rule=rule,
        evaluated_at=evaluated_at,
    )
    if band_error:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_PRICE_BAND,
            band_error,
        )
    if rule.block_adverse_limit_lock and band is not None:
        if (
            order.side == OrderSide.BUY
            and band.upper is not None
            and quote.last_price >= band.upper
        ) or (
            order.side == OrderSide.SELL
            and band.lower is not None
            and quote.last_price <= band.lower
        ):
            return _wait_seen(
                order,
                quote,
                fingerprint,
                MatchReason.WAIT_LIMIT_LOCK,
                "snapshot is locked at the adverse price-band edge",
            )

    fill_price = _adverse_price(
        quote.last_price,
        side=order.side,
        tick_size=rule.tick_size,
        slippage_rate=rule.slippage_rate,
    )
    if fill_price <= 0:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_LIQUIDITY,
            "adverse snapshot price is not executable",
        )
    if (
        band is not None
        and rule.enforce_price_band_bounds
        and not band.contains(fill_price)
    ):
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_PRICE_BAND,
            "adverse fill price is outside the price band",
        )
    if (
        order.side == OrderSide.BUY
        and fill_price > order.limit_price
    ) or (
        order.side == OrderSide.SELL
        and fill_price < order.limit_price
    ):
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_LIQUIDITY,
            "adverse snapshot price does not satisfy the order limit",
        )

    fill_quantity = min(
        order.remaining_quantity,
        order.approved_remaining_quantity,
        quote.liquidity_evidence.liquidity_quantity,
    )
    if fill_quantity <= 0:
        return _wait_seen(
            order,
            quote,
            fingerprint,
            MatchReason.WAIT_LIQUIDITY,
            "snapshot executable quantity is zero",
        )
    next_cumulative = order.cumulative_filled_quantity + fill_quantity
    next_status = (
        OrderStatus.FILLED
        if next_cumulative == order.requested_quantity
        else OrderStatus.PARTIALLY_FILLED
    )
    result = ExecutionResult(
        intent_id=order.intent_id,
        order_id=order.order_id,
        event_id=quote.snapshot_id,
        status=next_status,
        occurred_at=evaluated_at,
        received_at=evaluated_at,
        source_sequence=order.last_source_sequence + 1,
        idempotency_key=execution_result_idempotency_key(
            order_id=order.order_id,
            event_id=quote.snapshot_id,
        ),
        last_fill_quantity=fill_quantity,
        last_fill_price=fill_price,
    )
    events = dict(order.applied_events)
    events[quote.snapshot_id] = fingerprint
    updated = replace(
        order,
        status=next_status,
        cumulative_filled_quantity=next_cumulative,
        updated_at=evaluated_at,
        last_source_sequence=result.source_sequence,
        applied_events=tuple(sorted(events.items())),
    )
    return MatchDecision(
        status=MatchStatus(next_status.value),
        reason=MatchReason.NONE,
        updated_order=updated,
        quote_id=quote.snapshot_id,
        fill_quantity=fill_quantity,
        fill_price=fill_price,
        execution_result=result,
        explanation=(
            "matched against an explicitly enabled, content-bound snapshot "
            "under a hard quantity cap; hashes do not prove source authority "
            "and this is not Level-1 liquidity"
        ),
    )


__all__ = [
    "AttestedSnapshotQuote",
    "SnapshotEvidenceKind",
    "SnapshotLiquidityEvidence",
    "SnapshotMatchRule",
    "match_attested_snapshot",
    "snapshot_attestation_hash",
]
