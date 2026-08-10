"""Immutable contracts for strategy-neutral order execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum


class ValueStrEnum(str, Enum):
    def __str__(self) -> str:
        return str(self.value)


class OrderSide(ValueStrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(ValueStrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class TimeInForce(ValueStrEnum):
    DAY = "DAY"
    IOC = "IOC"
    FOK = "FOK"


class OrderStatus(ValueStrEnum):
    CREATED = "CREATED"
    ACCEPTED = "ACCEPTED"
    QUEUED = "QUEUED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class ExecutionEventKind(ValueStrEnum):
    """Distinguish state transitions from explicit same-state reason events."""

    STATUS_TRANSITION = "STATUS_TRANSITION"
    WAITING_REASON_CHANGED = "WAITING_REASON_CHANGED"


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _require_aware(value: datetime, field_name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _decimal(value: Decimal | str | int | float, field_name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            if type(value) is not Decimal:
                raise TypeError(f"{field_name} must not be a Decimal subclass")
            converted = value
        elif type(value) in {str, int, float}:
            converted = Decimal(str(value))
        else:
            raise TypeError(f"{field_name} must be decimal-like")
    except Exception as exc:
        raise ValueError(f"{field_name} must be a decimal value") from exc
    if not converted.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return converted


@dataclass(frozen=True)
class ExecutionIntent:
    """A caller's immutable request to create one executable order.

    The intent contains execution semantics only.  It intentionally has no
    strategy score, forecast, thesis, or portfolio-sleeve field.
    """

    intent_id: str
    account_id: str
    decision_id: str
    instrument_id: str
    side: OrderSide
    quantity: int
    order_type: OrderType
    time_in_force: TimeInForce
    created_at: datetime
    earliest_at: datetime
    expires_at: datetime
    idempotency_key: str
    rule_version: str
    fee_profile_version: str
    execution_policy_version: str
    intent_version: int = 1
    limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "intent_id",
            "account_id",
            "decision_id",
            "instrument_id",
            "idempotency_key",
            "rule_version",
            "fee_profile_version",
            "execution_policy_version",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        for field_name in ("created_at", "earliest_at", "expires_at"):
            _require_aware(getattr(self, field_name), field_name)
        object.__setattr__(self, "side", OrderSide(self.side))
        object.__setattr__(self, "order_type", OrderType(self.order_type))
        object.__setattr__(
            self,
            "time_in_force",
            TimeInForce(self.time_in_force),
        )
        _integer(self.quantity, "quantity", minimum=1)
        _integer(self.intent_version, "intent_version", minimum=1)
        if self.created_at > self.earliest_at:
            raise ValueError("created_at cannot follow earliest_at")
        if self.earliest_at >= self.expires_at:
            raise ValueError("earliest_at must precede expires_at")
        if self.limit_price is not None:
            object.__setattr__(
                self,
                "limit_price",
                _decimal(self.limit_price, "limit_price"),
            )
        if self.order_type == OrderType.LIMIT:
            if self.limit_price is None or self.limit_price <= 0:
                raise ValueError("positive limit_price required for LIMIT order")
        elif self.limit_price is not None:
            raise ValueError("limit_price is only valid for LIMIT orders")


@dataclass(frozen=True)
class ExecutionResult:
    """One immutable venue or simulator event for an order.

    ``last_fill_quantity`` is the delta carried by this event, not the order's
    cumulative fill.  The OMS owns cumulative quantity and average price.
    ``source_sequence`` is the contiguous, per-order canonical event sequence:
    it starts at one and increments by exactly one for each distinct event.
    """

    intent_id: str
    order_id: str
    event_id: str
    status: OrderStatus
    occurred_at: datetime
    received_at: datetime
    source_sequence: int
    idempotency_key: str
    last_fill_quantity: int = 0
    last_fill_price: Decimal | None = None
    reason_code: str = ""
    event_kind: ExecutionEventKind = ExecutionEventKind.STATUS_TRANSITION

    def __post_init__(self) -> None:
        for field_name in (
            "intent_id",
            "order_id",
            "event_id",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        _require_aware(self.occurred_at, "occurred_at")
        _require_aware(self.received_at, "received_at")
        if self.received_at < self.occurred_at:
            raise ValueError("received_at cannot precede occurred_at")
        _integer(self.source_sequence, "source_sequence", minimum=1)
        object.__setattr__(self, "status", OrderStatus(self.status))
        object.__setattr__(
            self,
            "event_kind",
            ExecutionEventKind(self.event_kind),
        )
        if not isinstance(self.reason_code, str):
            raise TypeError("reason_code must be a string")
        object.__setattr__(self, "reason_code", self.reason_code.strip())
        if self.status == OrderStatus.CREATED:
            raise ValueError("ExecutionResult cannot transition to CREATED")
        _integer(self.last_fill_quantity, "last_fill_quantity")
        if self.last_fill_price is not None:
            object.__setattr__(
                self,
                "last_fill_price",
                _decimal(self.last_fill_price, "last_fill_price"),
            )
        if self.event_kind is ExecutionEventKind.WAITING_REASON_CHANGED:
            if self.status not in {
                OrderStatus.QUEUED,
                OrderStatus.PARTIALLY_FILLED,
            }:
                raise ValueError(
                    "waiting-reason event requires QUEUED or PARTIALLY_FILLED"
                )
            if not self.reason_code:
                raise ValueError("waiting-reason event requires a non-empty reason")
            if self.last_fill_quantity != 0 or self.last_fill_price is not None:
                raise ValueError("waiting-reason event cannot carry a fill")
        else:
            fill_statuses = {
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
            }
            if self.status in fill_statuses and self.last_fill_quantity <= 0:
                raise ValueError("fill status requires a positive fill delta")
            if self.last_fill_quantity:
                if self.status not in fill_statuses:
                    raise ValueError("fill delta requires a fill status")
                if self.last_fill_price is None or self.last_fill_price <= 0:
                    raise ValueError("positive last_fill_price required for fill")
            elif self.last_fill_price is not None:
                raise ValueError("last_fill_price requires a positive fill delta")


@dataclass(frozen=True)
class PositionLot:
    """Remaining quantity acquired in one trading-session lot."""

    acquired_on: date
    quantity: int
    lot_id: str = ""

    def __post_init__(self) -> None:
        if type(self.acquired_on) is not date:
            raise TypeError("acquired_on must be exactly date")
        _integer(self.quantity, "quantity", minimum=1)
        if self.lot_id and not isinstance(self.lot_id, str):
            raise TypeError("lot_id must be a string")
