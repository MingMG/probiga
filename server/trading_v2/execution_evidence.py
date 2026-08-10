"""Immutable contracts for evidence that binds existing V2 execution facts.

The values in this module are not an account, order, fill, cash, or position
ledger.  They only describe hashes and provenance that can be persisted next
to the existing V2 facts in the same caller-owned transaction.  This module
owns no engine, opens no transaction, and performs no I/O.

A SHA-256 digest proves content equality only.  Source authority and history
completeness are independent, explicit properties; neither is inferred from a
hash or from the existence of a row.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timezone
from enum import Enum
import hashlib
import json
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .domain import OrderStatus
from .oms import ACTIVE_TRANSITIONS


class ExecutionEvidenceInvariantError(ValueError):
    """Raised when evidence cannot be reconstructed from canonical inputs."""


class HistoryOrigin(str, Enum):
    UNKNOWN = "UNKNOWN"
    START_AFTER_UNKNOWN = "START_AFTER_UNKNOWN"
    COMPLETE_FROM_DECLARED_ORIGIN = "COMPLETE_FROM_DECLARED_ORIGIN"


class AuthorityStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    CONTENT_HASH_ONLY = "CONTENT_HASH_ONLY"
    EXTERNAL_RECEIPT_VERIFIED = "EXTERNAL_RECEIPT_VERIFIED"


class QuoteReceiptType(str, Enum):
    NONE = "NONE"
    QMT_MINUTE = "QMT_MINUTE"
    QMT_REALTIME = "QMT_REALTIME"
    PUBLIC_CONSENSUS = "PUBLIC_CONSENSUS"
    OTHER = "OTHER"


class OrderTransitionKind(str, Enum):
    ORDER_CREATED = "ORDER_CREATED"
    STATUS_CHANGE = "STATUS_CHANGE"
    FILL_APPLIED = "FILL_APPLIED"
    WAITING_REASON_CHANGED = "WAITING_REASON_CHANGED"


def _text(value: object, field_name: str, *, maximum: int = 1000) -> str:
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
    maximum: int = 1000,
) -> str | None:
    if value is None:
        return None
    return _text(value, field_name, maximum=maximum)


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name, maximum=64).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


def _optional_sha256(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, field_name)


def _aware(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _date(value: object, field_name: str) -> date:
    if type(value) is not date:
        raise TypeError(f"{field_name} must be exactly date")
    return value


def _integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be exactly int")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _zone(value: object) -> ZoneInfo:
    name = _text(value, "market_timezone", maximum=64)
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("market_timezone is unknown") from exc


def _canonical(value: Any) -> Any:
    if type(value) is datetime:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is date:
        return value.isoformat()
    if type(value) is time:
        return value.isoformat(timespec="microseconds")
    if isinstance(value, Enum):
        return _canonical(value.value)
    if type(value) in {list, tuple}:
        return [_canonical(item) for item in value]
    if type(value) is dict:
        for key in value:
            if type(key) is not str:
                raise TypeError("canonical JSON object keys must be exactly str")
        return {
            key: _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: pair[0])
        }
    if value is None or type(value) in {str, bool}:
        if type(value) is str:
            try:
                value.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError("canonical JSON strings must be valid UTF-8") from exc
        return value
    if type(value) is int:
        if not -(2**63) <= value <= 2**63 - 1:
            raise ValueError("canonical JSON integers must fit signed 64-bit")
        return value
    if type(value) is float:
        raise TypeError(
            "canonical evidence JSON forbids binary floats; use integer or text"
        )
    raise TypeError(f"unsupported evidence hash value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON number: {value}")


def _reject_float(value: str) -> None:
    raise ValueError(f"binary-float JSON number is not permitted: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _load_strict_json(value: str) -> Any:
    return json.loads(
        value,
        parse_constant=_reject_constant,
        parse_float=_reject_float,
        object_pairs_hook=_unique_object,
    )


@dataclass(frozen=True, slots=True)
class CanonicalJson:
    """Canonical JSON text plus a deterministic namespaced content hash."""

    json_text: str
    payload_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.json_text) is not str:
            raise TypeError("json_text must be exactly str")
        text_value = self.json_text
        if not text_value or len(text_value) > 1_000_000:
            raise ValueError("json_text must contain 1 to 1000000 characters")
        try:
            parsed = _load_strict_json(text_value)
        except (TypeError, ValueError, json.JSONDecodeError, RecursionError) as exc:
            raise ValueError("json_text must contain valid strict JSON") from exc
        normalized = json.dumps(
            _canonical(parsed),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if text_value != normalized:
            raise ValueError("json_text must already be canonical JSON")
        object.__setattr__(self, "json_text", normalized)
        object.__setattr__(
            self,
            "payload_hash",
            _digest("trading-v2.canonical-json.v1", {"value": parsed}),
        )

    @classmethod
    def from_value(cls, value: Any) -> "CanonicalJson":
        normalized = json.dumps(
            _canonical(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return cls(normalized)

    def value(self) -> Any:
        return _load_strict_json(self.json_text)


@dataclass(frozen=True, slots=True)
class EvidenceProvenance:
    """History completeness and content source authority are orthogonal."""

    history_origin: HistoryOrigin
    authority_status: AuthorityStatus
    history_origin_id: str | None = None
    history_origin_at: datetime | None = None
    authority_receipt_hash: str | None = None
    provenance_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.history_origin) is not HistoryOrigin:
            raise TypeError("history_origin must be exactly HistoryOrigin")
        if type(self.authority_status) is not AuthorityStatus:
            raise TypeError("authority_status must be exactly AuthorityStatus")
        origin_id = _optional_text(
            self.history_origin_id,
            "history_origin_id",
            maximum=128,
        )
        origin_at = (
            None
            if self.history_origin_at is None
            else _aware(self.history_origin_at, "history_origin_at")
        )
        if self.history_origin is HistoryOrigin.UNKNOWN:
            if origin_id is not None or origin_at is not None:
                raise ValueError("UNKNOWN history cannot declare an origin")
        elif origin_id is None or origin_at is None:
            raise ValueError("known forward history requires an id and timestamp")
        receipt_hash = _optional_sha256(
            self.authority_receipt_hash,
            "authority_receipt_hash",
        )
        if self.authority_status is AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED:
            if receipt_hash is None:
                raise ValueError("external authority requires a receipt hash")
        elif receipt_hash is not None:
            raise ValueError("only external authority may claim a receipt hash")
        object.__setattr__(self, "history_origin_id", origin_id)
        object.__setattr__(self, "history_origin_at", origin_at)
        object.__setattr__(self, "authority_receipt_hash", receipt_hash)
        object.__setattr__(
            self,
            "provenance_hash",
            _digest(
                "trading-v2.execution-evidence-provenance.v1",
                {
                    "history_origin": self.history_origin,
                    "history_origin_id": origin_id,
                    "history_origin_at": origin_at,
                    "authority_status": self.authority_status,
                    "authority_receipt_hash": receipt_hash,
                },
            ),
        )

    @property
    def history_is_complete(self) -> bool:
        return self.history_origin is HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN

    @property
    def source_authority_is_verified(self) -> bool:
        return self.authority_status is AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED


def _reconstruct(value: Any, expected_type: type, field_name: str) -> Any:
    if type(value) is not expected_type:
        raise TypeError(f"{field_name} must be exactly {expected_type.__name__}")
    try:
        reconstructed = replace(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ExecutionEvidenceInvariantError(
            f"{field_name} cannot be reconstructed"
        ) from exc
    if reconstructed != value:
        raise ExecutionEvidenceInvariantError(
            f"{field_name} differs from its canonical reconstruction"
        )
    return value


def _validate_origin_time(provenance: EvidenceProvenance, event_at: datetime) -> None:
    _reconstruct(provenance, EvidenceProvenance, "provenance")
    if (
        provenance.history_origin_at is not None
        and event_at < provenance.history_origin_at
    ):
        raise ValueError("evidence predates its declared history origin")


def _same_history_origin(
    left: EvidenceProvenance,
    right: EvidenceProvenance,
) -> bool:
    return (
        left.history_origin == right.history_origin
        and left.history_origin_id == right.history_origin_id
        and left.history_origin_at == right.history_origin_at
    )


def _receipt_pair(
    receipt_id: object,
    receipt_hash: object,
) -> tuple[str | None, str | None]:
    normalized_id = _optional_text(receipt_id, "source_receipt_id", maximum=128)
    normalized_hash = _optional_sha256(receipt_hash, "source_receipt_hash")
    if (normalized_id is None) != (normalized_hash is None):
        raise ValueError("source receipt id and hash must be provided together")
    return normalized_id, normalized_hash


@dataclass(frozen=True, slots=True)
class MarketCalendarEvidence:
    market_code: str
    trade_date: date
    calendar_version: str
    market_timezone: str
    calendar_payload: CanonicalJson
    source_provider: str
    source_payload: CanonicalJson
    available_at: datetime
    provenance: EvidenceProvenance
    source_receipt_id: str | None = None
    source_receipt_hash: str | None = None
    evidence_hash: str = field(init=False)
    calendar_evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name, maximum in (
            ("market_code", 16),
            ("calendar_version", 80),
            ("market_timezone", 64),
            ("source_provider", 80),
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name, maximum=maximum),
            )
        trade_date = _date(self.trade_date, "trade_date")
        zone = _zone(self.market_timezone)
        _reconstruct(self.calendar_payload, CanonicalJson, "calendar_payload")
        _reconstruct(self.source_payload, CanonicalJson, "source_payload")
        available_at = _aware(self.available_at, "available_at")
        receipt_id, receipt_hash = _receipt_pair(
            self.source_receipt_id,
            self.source_receipt_hash,
        )
        if self.provenance.source_authority_is_verified:
            if receipt_hash is None:
                raise ValueError("authoritative calendar requires a source receipt")
            if self.provenance.authority_receipt_hash != receipt_hash:
                raise ValueError("calendar authority must bind the source receipt")
            if receipt_hash != self.source_payload.payload_hash:
                raise ValueError(
                    "authoritative calendar receipt must hash the bound source payload"
                )
        payload = self.calendar_payload.value()
        if type(payload) is not dict:
            raise TypeError("calendar_payload must be a JSON object")
        trading_days = payload.get("trading_days")
        sessions = payload.get("sessions")
        if type(trading_days) is not list or trade_date.isoformat() not in trading_days:
            raise ValueError("calendar payload must explicitly contain trade_date")
        if type(sessions) is not list or not sessions:
            raise ValueError("calendar payload requires non-empty sessions")
        coverage_start = _payload_datetime(
            payload.get("coverage_start_at"),
            "calendar coverage_start_at",
        )
        coverage_end = _payload_datetime(
            payload.get("coverage_end_at"),
            "calendar coverage_end_at",
        )
        if coverage_start >= coverage_end:
            raise ValueError("calendar coverage window must be increasing")
        source = self.source_payload.value()
        if type(source) is not dict:
            raise TypeError("source_payload must be a JSON object")
        source_published_at = _payload_datetime(
            source.get("published_at"),
            "calendar source published_at",
        )
        if source_published_at > available_at:
            raise ValueError("calendar source cannot be available before publication")
        _validate_origin_time(self.provenance, source_published_at)
        previous_close: time | None = None
        for item in sessions:
            if type(item) is not dict:
                raise TypeError("calendar sessions must be JSON objects")
            _text(item.get("session_id"), "session_id", maximum=80)
            try:
                opens = time.fromisoformat(_text(item.get("opens_at"), "opens_at"))
                closes = time.fromisoformat(_text(item.get("closes_at"), "closes_at"))
            except ValueError as exc:
                raise ValueError("calendar session times must be ISO local times") from exc
            if opens.tzinfo is not None or closes.tzinfo is not None or opens >= closes:
                raise ValueError("calendar sessions require increasing local wall times")
            if previous_close is not None and previous_close > opens:
                raise ValueError("calendar sessions cannot overlap")
            previous_close = closes
            session_open = datetime.combine(trade_date, opens, zone)
            session_close = datetime.combine(trade_date, closes, zone)
            if coverage_start > session_open or coverage_end < session_close:
                raise ValueError("calendar coverage must contain every bound session")
        local_available = available_at.astimezone(zone)
        if local_available.tzinfo is None:
            raise ValueError("calendar availability could not be localized")
        object.__setattr__(self, "source_receipt_id", receipt_id)
        object.__setattr__(self, "source_receipt_hash", receipt_hash)
        evidence_hash = _digest(
            "trading-v2.market-calendar-evidence.v1",
            {
                "market_code": self.market_code,
                "trade_date": trade_date,
                "calendar_version": self.calendar_version,
                "market_timezone": self.market_timezone,
                "calendar_payload_hash": self.calendar_payload.payload_hash,
                "source_provider": self.source_provider,
                "source_payload_hash": self.source_payload.payload_hash,
                "source_receipt_id": receipt_id,
                "source_receipt_hash": receipt_hash,
                "available_at": available_at,
                "provenance_hash": self.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "evidence_hash", evidence_hash)
        object.__setattr__(self, "calendar_evidence_id", evidence_hash)

    def contains_session_time(self, value: datetime) -> bool:
        candidate = _aware(value, "session candidate").astimezone(
            _zone(self.market_timezone)
        )
        if candidate.date() != self.trade_date:
            return False
        local_time = candidate.timetz().replace(tzinfo=None)
        return any(
            time.fromisoformat(item["opens_at"])
            <= local_time
            < time.fromisoformat(item["closes_at"])
            for item in self.calendar_payload.value()["sessions"]
        )


@dataclass(frozen=True, slots=True)
class QuoteReceiptEvidence:
    quote_event_id: str
    stock_code: str
    trade_date: date
    market_timezone: str
    quote_at: datetime
    received_at: datetime
    available_at: datetime
    source_provider: str
    source_batch_id: str
    source_payload_hash: str
    receipt_type: QuoteReceiptType
    receipt_payload: CanonicalJson
    provenance: EvidenceProvenance
    source_receipt_id: str | None = None
    source_receipt_hash: str | None = None
    evidence_hash: str = field(init=False)
    quote_evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "quote_event_id", _sha256(self.quote_event_id, "quote_event_id")
        )
        for field_name, maximum in (
            ("stock_code", 16),
            ("market_timezone", 64),
            ("source_provider", 80),
            ("source_batch_id", 120),
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name, maximum=maximum),
            )
        trade_date = _date(self.trade_date, "trade_date")
        zone = _zone(self.market_timezone)
        quote_at = _aware(self.quote_at, "quote_at")
        received_at = _aware(self.received_at, "received_at")
        available_at = _aware(self.available_at, "available_at")
        if not quote_at <= received_at <= available_at:
            raise ValueError("quote_at <= received_at <= available_at is required")
        if quote_at.astimezone(zone).date() != trade_date:
            raise ValueError("quote_at must fall on trade_date in market timezone")
        object.__setattr__(
            self,
            "source_payload_hash",
            _sha256(self.source_payload_hash, "source_payload_hash"),
        )
        if type(self.receipt_type) is not QuoteReceiptType:
            raise TypeError("receipt_type must be exactly QuoteReceiptType")
        _reconstruct(self.receipt_payload, CanonicalJson, "receipt_payload")
        _validate_origin_time(self.provenance, quote_at)
        receipt_id, receipt_hash = _receipt_pair(
            self.source_receipt_id,
            self.source_receipt_hash,
        )
        if self.receipt_type is QuoteReceiptType.NONE:
            if receipt_id is not None:
                raise ValueError("NONE receipt type cannot carry a receipt reference")
            if self.receipt_payload.value() != {}:
                raise ValueError("NONE receipt type requires an empty receipt payload")
        elif receipt_id is None:
            raise ValueError("named quote receipt type requires a receipt reference")
        if self.provenance.source_authority_is_verified:
            if receipt_hash is None:
                raise ValueError("authoritative quote requires a source receipt")
            if self.provenance.authority_receipt_hash != receipt_hash:
                raise ValueError("quote authority must bind the source receipt")
            if receipt_hash != self.receipt_payload.payload_hash:
                raise ValueError(
                    "authoritative quote receipt must hash the bound receipt payload"
                )
        if self.quote_event_id != self.source_payload_hash:
            raise ValueError("quote event id must bind the V2 quote payload hash")
        if self.receipt_type is not QuoteReceiptType.NONE:
            _required_payload_values(
                self.receipt_payload,
                "receipt_payload",
                {
                    "quote_event_id": self.quote_event_id,
                    "source_payload_hash": self.source_payload_hash,
                    "source_provider": self.source_provider,
                    "source_batch_id": self.source_batch_id,
                },
            )
        object.__setattr__(self, "source_receipt_id", receipt_id)
        object.__setattr__(self, "source_receipt_hash", receipt_hash)
        evidence_hash = _digest(
            "trading-v2.quote-receipt-evidence.v1",
            {
                "quote_event_id": self.quote_event_id,
                "stock_code": self.stock_code,
                "trade_date": trade_date,
                "market_timezone": self.market_timezone,
                "quote_at": quote_at,
                "received_at": received_at,
                "available_at": available_at,
                "source_provider": self.source_provider,
                "source_batch_id": self.source_batch_id,
                "source_payload_hash": self.source_payload_hash,
                "receipt_type": self.receipt_type,
                "source_receipt_id": receipt_id,
                "source_receipt_hash": receipt_hash,
                "receipt_payload_hash": self.receipt_payload.payload_hash,
                "provenance_hash": self.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "evidence_hash", evidence_hash)
        object.__setattr__(self, "quote_evidence_id", evidence_hash)


def _required_payload_values(
    payload: CanonicalJson,
    field_name: str,
    expected: dict[str, Any],
) -> None:
    _reconstruct(payload, CanonicalJson, field_name)
    value = payload.value()
    if type(value) is not dict:
        raise TypeError(f"{field_name} must be a JSON object")
    for key, expected_value in expected.items():
        canonical_expected = _canonical(expected_value)
        if (
            key not in value
            or type(value[key]) is not type(canonical_expected)
            or value[key] != canonical_expected
        ):
            raise ValueError(f"{field_name} does not bind {key}")


def _payload_object(payload: CanonicalJson, field_name: str) -> dict[str, Any]:
    _reconstruct(payload, CanonicalJson, field_name)
    value = payload.value()
    if type(value) is not dict:
        raise TypeError(f"{field_name} must be a JSON object")
    return value


def _payload_date(value: object, field_name: str) -> date:
    text_value = _text(value, field_name, maximum=10)
    try:
        return date.fromisoformat(text_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date") from exc


def _payload_datetime(value: object, field_name: str) -> datetime:
    text_value = _text(value, field_name, maximum=40)
    try:
        parsed = datetime.fromisoformat(text_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO datetime") from exc
    return _aware(parsed, field_name)


def _effective_on(
    *,
    trade_date: date,
    effective_from: date,
    effective_to: date | None,
    field_name: str,
) -> None:
    if trade_date < effective_from or (
        effective_to is not None and trade_date > effective_to
    ):
        raise ValueError(f"{field_name} is not effective on execution trade_date")


@dataclass(frozen=True, slots=True)
class FillExecutionEvidence:
    fill_id: str
    order_id: str
    order_fill_sequence: int
    account_id: str
    stock_code: str
    fill_payload: CanonicalJson
    order_payload: CanonicalJson
    quote_evidence: QuoteReceiptEvidence
    calendar_evidence: MarketCalendarEvidence
    fee_profile_version: str
    fee_security_type: str
    fee_effective_from: date
    fee_effective_to: date | None
    fee_created_at: datetime
    fee_schedule: CanonicalJson
    instrument_rule_version: str
    instrument_rule_effective_from: date
    instrument_rule_effective_to: date | None
    instrument_rule_created_at: datetime
    instrument_rule: CanonicalJson
    matcher_version: str
    matcher_request: CanonicalJson
    matcher_response: CanonicalJson
    accounting_request: CanonicalJson
    settlement_evidence: CanonicalJson
    executed_at: datetime
    bound_at: datetime
    provenance: EvidenceProvenance
    evidence_hash: str = field(init=False)
    fill_execution_evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name, maximum in (
            ("fill_id", 64),
            ("order_id", 64),
            ("account_id", 64),
            ("stock_code", 16),
            ("fee_profile_version", 80),
            ("fee_security_type", 40),
            ("instrument_rule_version", 80),
            ("matcher_version", 80),
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name, maximum=maximum),
            )
        _integer(self.order_fill_sequence, "order_fill_sequence", minimum=1)
        executed_at = _aware(self.executed_at, "executed_at")
        bound_at = _aware(self.bound_at, "bound_at")
        quote = _reconstruct(
            self.quote_evidence, QuoteReceiptEvidence, "quote_evidence"
        )
        calendar = _reconstruct(
            self.calendar_evidence, MarketCalendarEvidence, "calendar_evidence"
        )
        if quote.stock_code != self.stock_code:
            raise ValueError("quote evidence stock_code differs from fill")
        if quote.trade_date != calendar.trade_date:
            raise ValueError("quote and calendar trade dates differ")
        if quote.market_timezone != calendar.market_timezone:
            raise ValueError("quote and calendar timezones differ")
        if not max(quote.available_at, calendar.available_at) <= executed_at <= bound_at:
            raise ValueError(
                "quote and calendar evidence must both be available before execution"
            )
        if not calendar.contains_session_time(executed_at):
            raise ValueError("execution must be inside a bound calendar session")

        fill_payload = _payload_object(self.fill_payload, "fill_payload")
        order_payload = _payload_object(self.order_payload, "order_payload")
        fill_created_at = _payload_datetime(
            fill_payload.get("created_at"), "fill_payload.created_at"
        )
        if not executed_at <= fill_created_at <= bound_at:
            raise ValueError("fill creation must be between execution and evidence binding")
        _required_payload_values(
            self.fill_payload,
            "fill_payload",
            {
                "fill_id": self.fill_id,
                "order_id": self.order_id,
                "account_id": self.account_id,
                "stock_code": self.stock_code,
                "quote_event_id": quote.quote_event_id,
                "filled_at": executed_at,
                "created_at": fill_created_at,
            },
        )
        side = _text(fill_payload.get("side"), "fill_payload.side", maximum=8)
        if side not in {"BUY", "SELL"}:
            raise ValueError("fill_payload.side must be BUY or SELL")
        quantity = _integer(fill_payload.get("quantity"), "fill_payload.quantity", minimum=1)
        price = _text(fill_payload.get("price"), "fill_payload.price", maximum=40)
        gross_amount = _text(
            fill_payload.get("gross_amount"), "fill_payload.gross_amount", maximum=40
        )
        fee_amount = _text(
            fill_payload.get("fee_amount"), "fill_payload.fee_amount", maximum=40
        )
        net_cash_amount = _text(
            fill_payload.get("net_cash_amount"),
            "fill_payload.net_cash_amount",
            maximum=40,
        )
        match_event_id = _sha256(
            fill_payload.get("match_event_id"), "fill_payload.match_event_id"
        )
        idempotency_key = _sha256(
            fill_payload.get("idempotency_key"), "fill_payload.idempotency_key"
        )
        expected_idempotency_key = hashlib.sha256(
            f"{self.order_id}|{quote.quote_event_id}|{match_event_id}".encode("utf-8")
        ).hexdigest()
        if idempotency_key != expected_idempotency_key:
            raise ValueError("fill_payload.idempotency_key does not bind order and events")

        _required_payload_values(
            self.order_payload,
            "order_payload",
            {
                "order_id": self.order_id,
                "account_id": self.account_id,
                "stock_code": self.stock_code,
                "side": side,
            },
        )
        if order_payload.get("order_type") != "LIMIT":
            raise ValueError("order_payload must bind a LIMIT order")
        order_quantity = _integer(
            order_payload.get("quantity"), "order_payload.quantity", minimum=1
        )
        if quantity > order_quantity:
            raise ValueError("fill quantity cannot exceed order quantity")
        order_created_at = _payload_datetime(
            order_payload.get("created_at"), "order_payload.created_at"
        )
        earliest_at = _payload_datetime(
            order_payload.get("earliest_at"), "order_payload.earliest_at"
        )
        expires_at = _payload_datetime(
            order_payload.get("expires_at"), "order_payload.expires_at"
        )
        if not max(order_created_at, earliest_at) <= executed_at < expires_at:
            raise ValueError("execution falls outside the immutable order window")
        fee_effective = _date(self.fee_effective_from, "fee_effective_from")
        fee_effective_to = (
            None
            if self.fee_effective_to is None
            else _date(self.fee_effective_to, "fee_effective_to")
        )
        fee_created_at = _aware(self.fee_created_at, "fee_created_at")
        rule_effective = _date(
            self.instrument_rule_effective_from,
            "instrument_rule_effective_from",
        )
        rule_effective_to = (
            None
            if self.instrument_rule_effective_to is None
            else _date(self.instrument_rule_effective_to, "instrument_rule_effective_to")
        )
        rule_created_at = _aware(
            self.instrument_rule_created_at, "instrument_rule_created_at"
        )
        _effective_on(
            trade_date=quote.trade_date,
            effective_from=fee_effective,
            effective_to=fee_effective_to,
            field_name="fee schedule",
        )
        _effective_on(
            trade_date=quote.trade_date,
            effective_from=rule_effective,
            effective_to=rule_effective_to,
            field_name="instrument rule",
        )
        if fee_created_at > executed_at or rule_created_at > executed_at:
            raise ValueError("fee and rule rows must be visible before execution")
        _required_payload_values(
            self.fee_schedule,
            "fee_schedule",
            {
                "fee_profile_version": self.fee_profile_version,
                "security_type": self.fee_security_type,
                "effective_from": fee_effective,
                "effective_to": fee_effective_to,
                "created_at": fee_created_at,
            },
        )
        _required_payload_values(
            self.instrument_rule,
            "instrument_rule",
            {
                "stock_code": self.stock_code,
                "rule_version": self.instrument_rule_version,
                "effective_from": rule_effective,
                "effective_to": rule_effective_to,
                "created_at": rule_created_at,
                "fee_profile_version": self.fee_profile_version,
            },
        )
        rule_payload = _payload_object(self.instrument_rule, "instrument_rule")
        settlement_days = _integer(
            rule_payload.get("settlement_days"),
            "instrument_rule.settlement_days",
        )
        settlement_payload = _payload_object(
            self.settlement_evidence, "settlement_evidence"
        )
        trading_days = sorted(
            {
                _payload_date(item, "calendar trading day")
                for item in calendar.calendar_payload.value()["trading_days"]
            }
        )
        try:
            trade_index = trading_days.index(quote.trade_date)
            expected_settlement_date = trading_days[trade_index + settlement_days]
        except (ValueError, IndexError) as exc:
            raise ValueError(
                "calendar evidence does not cover the rule settlement horizon"
            ) from exc
        _required_payload_values(
            self.settlement_evidence,
            "settlement_evidence",
            {
                "stock_code": self.stock_code,
                "trade_date": quote.trade_date,
                "calendar_evidence_hash": calendar.evidence_hash,
                "instrument_rule_hash": self.instrument_rule.payload_hash,
                "settlement_days": settlement_days,
                "settlement_date": expected_settlement_date,
            },
        )
        _required_payload_values(
            self.matcher_request,
            "matcher_request",
            {
                "order_id": self.order_id,
                "order_payload_hash": self.order_payload.payload_hash,
                "quote_event_id": quote.quote_event_id,
                "quote_evidence_hash": quote.evidence_hash,
                "calendar_evidence_hash": calendar.evidence_hash,
                "matcher_version": self.matcher_version,
            },
        )
        _required_payload_values(
            self.matcher_response,
            "matcher_response",
            {
                "order_id": self.order_id,
                "quote_event_id": quote.quote_event_id,
                "matcher_request_hash": self.matcher_request.payload_hash,
                "match_event_id": match_event_id,
                "fill_quantity": quantity,
                "fill_price": price,
                "side": side,
            },
        )
        matcher_status = _payload_object(
            self.matcher_response, "matcher_response"
        ).get("status")
        if matcher_status not in {"PARTIALLY_FILLED", "FILLED"}:
            raise ValueError("matcher_response must bind a filled status")
        _required_payload_values(
            self.accounting_request,
            "accounting_request",
            {
                "fill_id": self.fill_id,
                "order_id": self.order_id,
                "account_id": self.account_id,
                "stock_code": self.stock_code,
                "side": side,
                "quantity": quantity,
                "price": price,
                "gross_amount": gross_amount,
                "fee_amount": fee_amount,
                "net_cash_amount": net_cash_amount,
                "matcher_output_hash": self.matcher_response.payload_hash,
                "fee_schedule_hash": self.fee_schedule.payload_hash,
                "instrument_rule_hash": self.instrument_rule.payload_hash,
                "settlement_evidence_hash": self.settlement_evidence.payload_hash,
                "quote_evidence_hash": quote.evidence_hash,
                "calendar_evidence_hash": calendar.evidence_hash,
            },
        )
        _validate_origin_time(self.provenance, executed_at)
        if self.provenance.source_authority_is_verified:
            raise ValueError(
                "fill hashes cannot independently claim external source authority"
            )
        evidence_hash = _digest(
            "trading-v2.fill-execution-evidence.v1",
            {
                "fill_id": self.fill_id,
                "order_id": self.order_id,
                "order_fill_sequence": self.order_fill_sequence,
                "account_id": self.account_id,
                "stock_code": self.stock_code,
                "fill_payload_hash": self.fill_payload.payload_hash,
                "order_payload_hash": self.order_payload.payload_hash,
                "quote_evidence_id": quote.quote_evidence_id,
                "quote_evidence_hash": quote.evidence_hash,
                "calendar_evidence_id": calendar.calendar_evidence_id,
                "calendar_evidence_hash": calendar.evidence_hash,
                "fee_profile_version": self.fee_profile_version,
                "fee_security_type": self.fee_security_type,
                "fee_effective_from": fee_effective,
                "fee_effective_to": fee_effective_to,
                "fee_created_at": fee_created_at,
                "fee_schedule_hash": self.fee_schedule.payload_hash,
                "instrument_rule_version": self.instrument_rule_version,
                "instrument_rule_effective_from": rule_effective,
                "instrument_rule_effective_to": rule_effective_to,
                "instrument_rule_created_at": rule_created_at,
                "instrument_rule_hash": self.instrument_rule.payload_hash,
                "matcher_version": self.matcher_version,
                "matcher_request_hash": self.matcher_request.payload_hash,
                "matcher_output_hash": self.matcher_response.payload_hash,
                "accounting_request_hash": self.accounting_request.payload_hash,
                "settlement_evidence_hash": self.settlement_evidence.payload_hash,
                "executed_at": executed_at,
                "bound_at": bound_at,
                "provenance_hash": self.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "evidence_hash", evidence_hash)
        object.__setattr__(self, "fill_execution_evidence_id", evidence_hash)

    @property
    def upstream_market_authority_is_verified(self) -> bool:
        return (
            self.quote_evidence.provenance.source_authority_is_verified
            and self.calendar_evidence.provenance.source_authority_is_verified
        )


@dataclass(frozen=True, slots=True)
class CashEventBinding:
    cash_event_id: str
    account_id: str
    account_sequence: int
    cash_event_type: str
    cash_event_payload: CanonicalJson
    occurred_at: datetime
    bound_at: datetime
    provenance: EvidenceProvenance
    related_order_id: str | None = None
    related_fill_id: str | None = None
    reversal_of: str | None = None
    fill_execution_evidence: FillExecutionEvidence | None = None
    previous_cash_event_id: str | None = None
    previous_binding_id: str | None = None
    previous_binding_hash: str | None = None
    cash_event_payload_hash: str = field(init=False)
    binding_hash: str = field(init=False)
    cash_binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name, maximum in (("cash_event_id", 64), ("account_id", 64)):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name, maximum=maximum),
            )
        sequence = _integer(self.account_sequence, "account_sequence")
        event_type = _text(self.cash_event_type, "cash_event_type", maximum=40)
        if event_type not in {"INITIAL_DEPOSIT", "BUY_FILL", "SELL_FILL"}:
            raise ValueError("cash_event_type is not a canonical V2 cash event")
        if event_type == "INITIAL_DEPOSIT" and sequence != 0:
            raise ValueError("INITIAL_DEPOSIT must be cash sequence zero")
        _reconstruct(self.cash_event_payload, CanonicalJson, "cash_event_payload")
        object.__setattr__(self, "cash_event_type", event_type)
        object.__setattr__(
            self, "cash_event_payload_hash", self.cash_event_payload.payload_hash
        )
        occurred_at = _aware(self.occurred_at, "occurred_at")
        bound_at = _aware(self.bound_at, "bound_at")
        if occurred_at > bound_at:
            raise ValueError("cash event cannot be bound before it occurred")
        _validate_origin_time(self.provenance, occurred_at)
        if self.provenance.source_authority_is_verified:
            raise ValueError("cash hash chains cannot claim external authority")
        order_id = _optional_text(self.related_order_id, "related_order_id", maximum=64)
        fill_id = _optional_text(self.related_fill_id, "related_fill_id", maximum=64)
        reversal_of = _optional_text(self.reversal_of, "reversal_of", maximum=64)
        if reversal_of is not None:
            raise ValueError("canonical V2 cash event types do not include reversals")
        payload_value = _payload_object(self.cash_event_payload, "cash_event_payload")
        cash_created_at = _payload_datetime(
            payload_value.get("created_at"), "cash_event_payload.created_at"
        )
        if not occurred_at <= cash_created_at <= bound_at:
            raise ValueError("cash row creation must be between occurrence and binding")
        business_event_key = _text(
            payload_value.get("business_event_key"),
            "cash_event_payload.business_event_key",
            maximum=160,
        )
        _text(payload_value.get("amount"), "cash_event_payload.amount", maximum=40)
        _text(
            payload_value.get("balance_after"),
            "cash_event_payload.balance_after",
            maximum=40,
        )
        _required_payload_values(
            self.cash_event_payload,
            "cash_event_payload",
            {
                "cash_event_id": self.cash_event_id,
                "account_id": self.account_id,
                "event_type": event_type,
                "related_order_id": order_id,
                "related_fill_id": fill_id,
                "reversal_of": reversal_of,
                "occurred_at": occurred_at,
                "created_at": cash_created_at,
            },
        )
        fill_evidence = self.fill_execution_evidence
        is_fill_event = event_type in {"BUY_FILL", "SELL_FILL"}
        if is_fill_event:
            if fill_id is None or order_id is None or fill_evidence is None:
                raise ValueError(
                    "fill cash event requires order, fill, and execution evidence"
                )
            fill_evidence = _reconstruct(
                fill_evidence,
                FillExecutionEvidence,
                "fill_execution_evidence",
            )
            if fill_evidence.fill_id != fill_id:
                raise ValueError("cash binding fill id differs from fill evidence")
            if fill_evidence.account_id != self.account_id:
                raise ValueError("cash binding account differs from fill evidence")
            if fill_evidence.order_id != order_id:
                raise ValueError("cash binding order differs from fill evidence")
            if occurred_at != fill_evidence.executed_at:
                raise ValueError("cash fill time differs from fill execution")
            if bound_at < fill_evidence.bound_at:
                raise ValueError("cash binding cannot precede fill evidence binding")
            fill_side = _payload_object(
                fill_evidence.fill_payload, "fill_payload"
            )["side"]
            if event_type != f"{fill_side}_FILL":
                raise ValueError("cash event type differs from fill side")
            expected_business_key = (
                "FILL:"
                + _payload_object(fill_evidence.fill_payload, "fill_payload")[
                    "idempotency_key"
                ]
            )
            if business_event_key != expected_business_key:
                raise ValueError("cash fill business key differs from fill idempotency")
        elif fill_id is not None or fill_evidence is not None:
            raise ValueError("only BUY_FILL or SELL_FILL may bind fill evidence")
        previous_event_id = _optional_text(
            self.previous_cash_event_id,
            "previous_cash_event_id",
            maximum=64,
        )
        previous_binding_id = _optional_sha256(
            self.previous_binding_id, "previous_binding_id"
        )
        previous_binding_hash = _optional_sha256(
            self.previous_binding_hash, "previous_binding_hash"
        )
        if len(
            {
                item is None
                for item in (
                    previous_event_id,
                    previous_binding_id,
                    previous_binding_hash,
                )
            }
        ) != 1:
            raise ValueError("previous cash id and hashes must be provided together")
        has_previous = previous_event_id is not None
        if sequence == 0 and has_previous:
            raise ValueError("cash genesis cannot reference a previous binding")
        if self.provenance.history_is_complete:
            if sequence == 0:
                if (
                    event_type != "INITIAL_DEPOSIT"
                    or order_id is not None
                    or fill_id is not None
                    or reversal_of is not None
                ):
                    raise ValueError(
                        "complete cash genesis must be an unlinked INITIAL_DEPOSIT"
                    )
            elif not has_previous:
                raise ValueError("complete cash history must reference its predecessor")
        object.__setattr__(self, "related_order_id", order_id)
        object.__setattr__(self, "related_fill_id", fill_id)
        object.__setattr__(self, "reversal_of", reversal_of)
        object.__setattr__(self, "previous_cash_event_id", previous_event_id)
        object.__setattr__(self, "previous_binding_id", previous_binding_id)
        object.__setattr__(self, "previous_binding_hash", previous_binding_hash)
        binding_hash = _digest(
            "trading-v2.cash-event-binding.v1",
            {
                "cash_event_id": self.cash_event_id,
                "account_id": self.account_id,
                "account_sequence": sequence,
                "cash_event_type": event_type,
                "related_order_id": order_id,
                "related_fill_id": fill_id,
                "reversal_of": reversal_of,
                "fill_execution_evidence_id": (
                    None
                    if fill_evidence is None
                    else fill_evidence.fill_execution_evidence_id
                ),
                "fill_execution_evidence_hash": (
                    None if fill_evidence is None else fill_evidence.evidence_hash
                ),
                "previous_cash_event_id": previous_event_id,
                "previous_binding_id": previous_binding_id,
                "previous_binding_hash": previous_binding_hash,
                "cash_event_payload_hash": self.cash_event_payload_hash,
                "occurred_at": occurred_at,
                "bound_at": bound_at,
                "provenance_hash": self.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "binding_hash", binding_hash)
        object.__setattr__(self, "cash_binding_id", binding_hash)

    @property
    def history_is_complete(self) -> bool:
        return (
            self.provenance.history_is_complete
            and self.account_sequence == 0
            and self.cash_event_type == "INITIAL_DEPOSIT"
            and self.previous_binding_id is None
        )


def validate_cash_event_binding_chain(
    bindings: tuple[CashEventBinding, ...],
) -> bool:
    """Validate one ordered cash chain in linear time.

    The boolean is true only when the supplied tuple itself proves a complete
    chain from the declared INITIAL_DEPOSIT genesis.  A later row alone never
    inherits a completeness claim from unvalidated identifiers.
    """

    if type(bindings) is not tuple:
        raise TypeError("cash bindings must be exactly tuple")
    if not bindings:
        return False
    previous: CashEventBinding | None = None
    event_ids: set[str] = set()
    binding_ids: set[str] = set()
    for current in bindings:
        current = _reconstruct(current, CashEventBinding, "cash binding")
        if current.cash_event_id in event_ids or current.cash_binding_id in binding_ids:
            raise ValueError("cash chain repeats an event or binding id")
        event_ids.add(current.cash_event_id)
        binding_ids.add(current.cash_binding_id)
        if previous is None:
            if current.previous_binding_id is not None:
                raise ValueError("first supplied cash binding has an unknown predecessor")
        else:
            if current.account_id != previous.account_id:
                raise ValueError("cash chain cannot cross accounts")
            if current.account_sequence != previous.account_sequence + 1:
                raise ValueError("cash sequence must advance by exactly one")
            if current.previous_cash_event_id != previous.cash_event_id:
                raise ValueError("cash chain previous event id is discontinuous")
            if current.previous_binding_id != previous.cash_binding_id:
                raise ValueError("cash chain previous binding id is discontinuous")
            if current.previous_binding_hash != previous.binding_hash:
                raise ValueError("cash chain previous binding hash is discontinuous")
            if current.occurred_at < previous.occurred_at:
                raise ValueError("cash event time cannot move backwards")
            if current.bound_at < previous.bound_at:
                raise ValueError("cash binding time cannot move backwards")
            if not _same_history_origin(previous.provenance, current.provenance):
                raise ValueError("cash chain history origin cannot change")
        previous = current
    first = bindings[0]
    return (
        first.history_is_complete
        and all(item.provenance.history_is_complete for item in bindings)
    )


@dataclass(frozen=True, slots=True)
class OrderTransitionEvidence:
    order_id: str
    account_id: str
    order_payload: CanonicalJson
    transition_sequence: int
    from_status: OrderStatus
    to_status: OrderStatus
    previous_filled_quantity: int
    next_filled_quantity: int
    transition_kind: OrderTransitionKind
    source_event_type: str
    source_event_id: str
    source_event_hash: str
    occurred_at: datetime
    recorded_at: datetime
    provenance: EvidenceProvenance
    waiting_reason: str | None = None
    related_fill_id: str | None = None
    fill_execution_evidence: FillExecutionEvidence | None = None
    previous_transition_id: str | None = None
    previous_transition_hash: str | None = None
    transition_hash: str = field(init=False)
    transition_id: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name, maximum in (
            ("order_id", 64),
            ("account_id", 64),
            ("source_event_type", 80),
            ("source_event_id", 128),
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name, maximum=maximum),
            )
        sequence = _integer(self.transition_sequence, "transition_sequence")
        order_payload = _payload_object(self.order_payload, "order_payload")
        _required_payload_values(
            self.order_payload,
            "order_payload",
            {"order_id": self.order_id, "account_id": self.account_id},
        )
        order_quantity = _integer(
            order_payload.get("quantity"), "order_payload.quantity", minimum=1
        )
        order_created_at = _payload_datetime(
            order_payload.get("created_at"), "order_payload.created_at"
        )
        previous_quantity = _integer(
            self.previous_filled_quantity, "previous_filled_quantity"
        )
        next_quantity = _integer(self.next_filled_quantity, "next_filled_quantity")
        if next_quantity < previous_quantity:
            raise ValueError("filled quantity cannot decrease")
        if next_quantity > order_quantity:
            raise ValueError("filled quantity cannot exceed immutable order quantity")
        if type(self.from_status) is not OrderStatus:
            raise TypeError("from_status must be exactly OrderStatus")
        if type(self.to_status) is not OrderStatus:
            raise TypeError("to_status must be exactly OrderStatus")
        if type(self.transition_kind) is not OrderTransitionKind:
            raise TypeError("transition_kind must be exactly OrderTransitionKind")
        waiting_reason = _optional_text(
            self.waiting_reason,
            "waiting_reason",
            maximum=40,
        )
        fill_id = _optional_text(self.related_fill_id, "related_fill_id", maximum=64)
        if self.transition_kind is OrderTransitionKind.FILL_APPLIED:
            if fill_id is None or next_quantity <= previous_quantity:
                raise ValueError("FILL_APPLIED requires a fill and quantity increase")
            if self.to_status not in {
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
            }:
                raise ValueError("FILL_APPLIED must produce a filled status")
        elif fill_id is not None or next_quantity != previous_quantity:
            raise ValueError("only FILL_APPLIED may bind a fill or increase quantity")
        fill_evidence = self.fill_execution_evidence
        if self.transition_kind is OrderTransitionKind.FILL_APPLIED:
            fill_evidence = _reconstruct(
                fill_evidence,
                FillExecutionEvidence,
                "fill_execution_evidence",
            )
            if (
                fill_evidence.fill_id != fill_id
                or fill_evidence.order_id != self.order_id
                or fill_evidence.account_id != self.account_id
            ):
                raise ValueError("order transition differs from fill evidence identity")
            if fill_evidence.order_payload.payload_hash != self.order_payload.payload_hash:
                raise ValueError("order transition and fill bind different order payloads")
            if next_quantity - previous_quantity != _payload_object(
                fill_evidence.fill_payload, "fill_payload"
            )["quantity"]:
                raise ValueError("order transition quantity differs from fill evidence")
        elif fill_evidence is not None:
            raise ValueError("only FILL_APPLIED may carry fill execution evidence")
        if self.from_status != self.to_status:
            if self.to_status not in ACTIVE_TRANSITIONS[self.from_status]:
                raise ValueError("illegal V2 order status transition")
        elif self.transition_kind not in {
            OrderTransitionKind.ORDER_CREATED,
            OrderTransitionKind.FILL_APPLIED,
            OrderTransitionKind.WAITING_REASON_CHANGED,
        }:
            raise ValueError("same-status transition requires fill or waiting evidence")
        occurred_at = _aware(self.occurred_at, "occurred_at")
        recorded_at = _aware(self.recorded_at, "recorded_at")
        if occurred_at > recorded_at:
            raise ValueError("order transition cannot be recorded before it occurred")
        if occurred_at < order_created_at:
            raise ValueError("order transition cannot predate immutable order creation")
        _validate_origin_time(self.provenance, occurred_at)
        if self.provenance.source_authority_is_verified:
            raise ValueError("order hash chains cannot claim external authority")
        object.__setattr__(
            self,
            "source_event_hash",
            _sha256(self.source_event_hash, "source_event_hash"),
        )
        if fill_evidence is not None and fill_evidence.executed_at != occurred_at:
            raise ValueError("fill transition occurrence must equal fill execution time")
        if fill_evidence is not None and recorded_at < fill_evidence.bound_at:
            raise ValueError("fill transition cannot predate fill evidence binding")
        previous_id = _optional_sha256(
            self.previous_transition_id, "previous_transition_id"
        )
        previous_hash = _optional_sha256(
            self.previous_transition_hash, "previous_transition_hash"
        )
        if (previous_id is None) != (previous_hash is None):
            raise ValueError("previous transition id and hash must be provided together")
        has_previous = previous_id is not None
        if sequence == 0 and has_previous:
            raise ValueError("order genesis cannot reference a previous transition")
        is_genesis = (
            self.transition_kind is OrderTransitionKind.ORDER_CREATED
            and sequence == 0
            and self.from_status is OrderStatus.CREATED
            and self.to_status is OrderStatus.CREATED
            and previous_quantity == 0
            and next_quantity == 0
            and waiting_reason is None
            and fill_id is None
        )
        if self.transition_kind is OrderTransitionKind.ORDER_CREATED and not is_genesis:
            raise ValueError("ORDER_CREATED must be the canonical zero-state genesis")
        if self.provenance.history_is_complete:
            if sequence == 0 and not is_genesis:
                raise ValueError("complete order history requires ORDER_CREATED genesis")
            if sequence > 0 and not has_previous:
                raise ValueError("complete order history must reference its predecessor")
        object.__setattr__(self, "waiting_reason", waiting_reason)
        object.__setattr__(self, "related_fill_id", fill_id)
        object.__setattr__(self, "previous_transition_id", previous_id)
        object.__setattr__(self, "previous_transition_hash", previous_hash)
        transition_hash = _digest(
            "trading-v2.order-transition-evidence.v1",
            {
                "order_id": self.order_id,
                "account_id": self.account_id,
                "order_payload_hash": self.order_payload.payload_hash,
                "transition_sequence": sequence,
                "previous_transition_id": previous_id,
                "previous_transition_hash": previous_hash,
                "from_status": self.from_status,
                "to_status": self.to_status,
                "previous_filled_quantity": previous_quantity,
                "next_filled_quantity": next_quantity,
                "waiting_reason": waiting_reason,
                "transition_kind": self.transition_kind,
                "related_fill_id": fill_id,
                "fill_execution_evidence_id": (
                    None
                    if fill_evidence is None
                    else fill_evidence.fill_execution_evidence_id
                ),
                "fill_execution_evidence_hash": (
                    None if fill_evidence is None else fill_evidence.evidence_hash
                ),
                "source_event_type": self.source_event_type,
                "source_event_id": self.source_event_id,
                "source_event_hash": self.source_event_hash,
                "occurred_at": occurred_at,
                "recorded_at": recorded_at,
                "provenance_hash": self.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "transition_hash", transition_hash)
        object.__setattr__(self, "transition_id", transition_hash)

    @property
    def history_is_complete(self) -> bool:
        return (
            self.provenance.history_is_complete
            and self.transition_sequence == 0
            and self.transition_kind is OrderTransitionKind.ORDER_CREATED
        )


def validate_order_transition_chain(
    transitions: tuple[OrderTransitionEvidence, ...],
) -> bool:
    """Validate one ordered order-transition chain in linear time."""

    if type(transitions) is not tuple:
        raise TypeError("order transitions must be exactly tuple")
    if not transitions:
        return False
    previous: OrderTransitionEvidence | None = None
    transition_ids: set[str] = set()
    source_events: set[tuple[str, str]] = set()
    for current in transitions:
        current = _reconstruct(current, OrderTransitionEvidence, "order transition")
        source_key = (current.source_event_type, current.source_event_id)
        if current.transition_id in transition_ids or source_key in source_events:
            raise ValueError("order chain repeats a transition or source event")
        transition_ids.add(current.transition_id)
        source_events.add(source_key)
        if previous is None:
            if current.previous_transition_id is not None:
                raise ValueError("first supplied order transition has an unknown predecessor")
        else:
            if current.order_id != previous.order_id or current.account_id != previous.account_id:
                raise ValueError("order transition chain cannot change identity")
            if current.order_payload.payload_hash != previous.order_payload.payload_hash:
                raise ValueError("order transition chain cannot change order payload")
            if current.transition_sequence != previous.transition_sequence + 1:
                raise ValueError("order transition sequence must advance by one")
            if current.previous_transition_id != previous.transition_id:
                raise ValueError("order transition previous id is discontinuous")
            if current.previous_transition_hash != previous.transition_hash:
                raise ValueError("order transition previous hash is discontinuous")
            if current.from_status is not previous.to_status:
                raise ValueError("order transition status chain is discontinuous")
            if current.previous_filled_quantity != previous.next_filled_quantity:
                raise ValueError("order transition quantity chain is discontinuous")
            if current.occurred_at < previous.occurred_at:
                raise ValueError("order transition event time cannot move backwards")
            if current.recorded_at < previous.recorded_at:
                raise ValueError("order transition record time cannot move backwards")
            if not _same_history_origin(previous.provenance, current.provenance):
                raise ValueError("order transition history origin cannot change")
        previous = current
    first = transitions[0]
    return (
        first.history_is_complete
        and all(item.provenance.history_is_complete for item in transitions)
    )


def validate_market_calendar_evidence(value: MarketCalendarEvidence) -> None:
    _reconstruct(value, MarketCalendarEvidence, "calendar evidence")


def validate_quote_receipt_evidence(value: QuoteReceiptEvidence) -> None:
    _reconstruct(value, QuoteReceiptEvidence, "quote evidence")


def validate_fill_execution_evidence(value: FillExecutionEvidence) -> None:
    _reconstruct(value, FillExecutionEvidence, "fill evidence")


def validate_cash_event_binding(value: CashEventBinding) -> None:
    _reconstruct(value, CashEventBinding, "cash binding")


def validate_order_transition_evidence(value: OrderTransitionEvidence) -> None:
    _reconstruct(value, OrderTransitionEvidence, "order transition")


__all__ = [
    "AuthorityStatus",
    "CanonicalJson",
    "CashEventBinding",
    "EvidenceProvenance",
    "ExecutionEvidenceInvariantError",
    "FillExecutionEvidence",
    "HistoryOrigin",
    "MarketCalendarEvidence",
    "OrderTransitionEvidence",
    "OrderTransitionKind",
    "QuoteReceiptEvidence",
    "QuoteReceiptType",
    "validate_cash_event_binding",
    "validate_cash_event_binding_chain",
    "validate_fill_execution_evidence",
    "validate_market_calendar_evidence",
    "validate_order_transition_evidence",
    "validate_order_transition_chain",
    "validate_quote_receipt_evidence",
]
