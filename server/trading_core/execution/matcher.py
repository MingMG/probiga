"""Deterministic, strategy-neutral LIMIT + DAY Level-1 matching.

The matcher is deliberately an in-memory pure function.  All facts that can
change the answer are supplied through immutable ``Quote``, ``Order`` and
``Rule`` values; the function reads no clock, configuration, strategy module,
database, account, or ledger.

Callers must carry the returned ``updated_order`` into the next invocation.
That immutable state is what makes quote retries idempotent and event sequence
numbers contiguous without introducing a second persistent order store.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from enum import Enum
from typing import Any

from ..contracts import (
    ExecutionResult,
    OrderSide,
    OrderStatus,
    execution_result_idempotency_key,
)


class ValueStrEnum(str, Enum):
    def __str__(self) -> str:
        return str(self.value)


class MatchStatus(ValueStrEnum):
    WAITING = "WAITING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    EXPIRED = "EXPIRED"
    DUPLICATE = "DUPLICATE"
    TERMINAL = "TERMINAL"


class MatchReason(ValueStrEnum):
    NONE = ""
    WAIT_NOT_ACTIVE = "WAIT_NOT_ACTIVE"
    WAIT_NO_QUOTE = "WAIT_NO_QUOTE"
    WAIT_FUTURE_QUOTE = "WAIT_FUTURE_QUOTE"
    WAIT_PRE_ORDER_QUOTE = "WAIT_PRE_ORDER_QUOTE"
    WAIT_OUT_OF_ORDER_QUOTE = "WAIT_OUT_OF_ORDER_QUOTE"
    WAIT_STALE_QUOTE = "WAIT_STALE_QUOTE"
    WAIT_SUSPENDED = "WAIT_SUSPENDED"
    WAIT_PRICE_BAND = "WAIT_PRICE_BAND"
    WAIT_LIMIT_LOCK = "WAIT_LIMIT_LOCK"
    WAIT_LIQUIDITY = "WAIT_LIQUIDITY"
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    ORDER_TERMINAL = "ORDER_TERMINAL"


def _text(value: object, field_name: str) -> str:
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
    if positive and converted <= 0:
        raise ValueError(f"{field_name} must be positive")
    if non_negative and converted < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return converted


@dataclass(frozen=True)
class Level1Quote:
    """One immutable opposing Level-1 market-data event.

    There is intentionally no last-price field: a LIMIT order cannot silently
    fall back from an executable bid/ask to a trade or snapshot price.
    """

    instrument_id: str
    quote_id: str
    observed_at: datetime
    received_at: datetime
    bid_price: Decimal | None
    bid_quantity: int | None
    ask_price: Decimal | None
    ask_quantity: int | None
    suspended: bool = False

    def __post_init__(self) -> None:
        for field_name in ("instrument_id", "quote_id"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        _aware(self.observed_at, "observed_at")
        _aware(self.received_at, "received_at")
        if self.received_at < self.observed_at:
            raise ValueError("received_at cannot precede observed_at")
        for field_name in ("bid_price", "ask_price"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _decimal(value, field_name, positive=True),
                )
        for field_name in ("bid_quantity", "ask_quantity"):
            value = getattr(self, field_name)
            if value is not None:
                _integer(value, field_name)
        if not isinstance(self.suspended, bool):
            raise TypeError("suspended must be a bool")


@dataclass(frozen=True)
class MatchPriceBand:
    """A point-in-time price-band fact used by the matcher.

    A partial band is supported only for compatibility mapping.  Neutral rules
    default to ``require_complete_price_band=True`` and therefore fail closed
    unless both bounds are present.
    """

    instrument_id: str
    trade_date: date
    as_of: datetime
    source: str
    lower: Decimal | None
    upper: Decimal | None

    def __post_init__(self) -> None:
        for field_name in ("instrument_id", "source"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        if type(self.trade_date) is not date:
            raise TypeError("trade_date must be exactly date")
        _aware(self.as_of, "as_of")
        if self.lower is None and self.upper is None:
            raise ValueError("price band must contain at least one bound")
        for field_name in ("lower", "upper"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _decimal(value, field_name, positive=True),
                )
        if (
            self.lower is not None
            and self.upper is not None
            and self.upper < self.lower
        ):
            raise ValueError("price band upper cannot be below lower")

    @property
    def complete(self) -> bool:
        return self.lower is not None and self.upper is not None

    def contains(self, price: Decimal) -> bool:
        return (self.lower is None or price >= self.lower) and (
            self.upper is None or price <= self.upper
        )

    def is_fresh(self, *, evaluated_at: datetime, max_age: timedelta) -> bool:
        _aware(evaluated_at, "evaluated_at")
        if type(max_age) is not timedelta:
            raise TypeError("max_age must be exactly timedelta")
        if max_age < timedelta(0):
            raise ValueError("max_age cannot be negative")
        return self.as_of <= evaluated_at and evaluated_at - self.as_of <= max_age


@dataclass(frozen=True)
class LimitDayMatchRule:
    """Caller-supplied mechanical rules for one match evaluation."""

    rule_version: str
    tick_size: Decimal
    quote_max_age: timedelta
    visible_volume_participation: Decimal
    maximum_fill_quantity: int | None = None
    price_band: MatchPriceBand | None = None
    price_band_max_age: timedelta | None = None
    require_complete_price_band: bool = True
    enforce_price_band_bounds: bool = True
    slippage_rate: Decimal = Decimal("0")
    impact_rate: Decimal = Decimal("0")
    block_adverse_limit_lock: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rule_version",
            _text(self.rule_version, "rule_version"),
        )
        object.__setattr__(
            self,
            "tick_size",
            _decimal(self.tick_size, "tick_size", positive=True),
        )
        if type(self.quote_max_age) is not timedelta:
            raise TypeError("quote_max_age must be exactly timedelta")
        if self.quote_max_age < timedelta(0):
            raise ValueError("quote_max_age cannot be negative")
        participation = _decimal(
            self.visible_volume_participation,
            "visible_volume_participation",
            non_negative=True,
        )
        if participation > 1:
            raise ValueError("visible_volume_participation cannot exceed one")
        object.__setattr__(self, "visible_volume_participation", participation)
        if self.maximum_fill_quantity is not None:
            _integer(self.maximum_fill_quantity, "maximum_fill_quantity")
        if self.price_band is not None and type(self.price_band) is not MatchPriceBand:
            raise TypeError("price_band must be exactly MatchPriceBand or None")
        if self.price_band_max_age is not None:
            if type(self.price_band_max_age) is not timedelta:
                raise TypeError("price_band_max_age must be exactly timedelta or None")
            if self.price_band_max_age < timedelta(0):
                raise ValueError("price_band_max_age cannot be negative")
        for field_name in (
            "require_complete_price_band",
            "enforce_price_band_bounds",
            "block_adverse_limit_lock",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a bool")
        slippage = _decimal(
            self.slippage_rate,
            "slippage_rate",
            non_negative=True,
        )
        impact = _decimal(
            self.impact_rate,
            "impact_rate",
            non_negative=True,
        )
        if slippage + impact >= 1:
            raise ValueError("slippage_rate plus impact_rate must be below one")
        object.__setattr__(self, "slippage_rate", slippage)
        object.__setattr__(self, "impact_rate", impact)


_MATCHABLE_STATUSES = frozenset(
    {
        OrderStatus.ACCEPTED,
        OrderStatus.QUEUED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.CANCEL_PENDING,
    }
)


@dataclass(frozen=True)
class LimitDayOrder:
    """The complete immutable order state required by the pure matcher."""

    order_id: str
    intent_id: str
    instrument_id: str
    side: OrderSide
    requested_quantity: int
    approved_quantity: int
    cumulative_filled_quantity: int
    limit_price: Decimal
    earliest_at: datetime
    expires_at: datetime
    updated_at: datetime
    last_source_sequence: int = 0
    status: OrderStatus = OrderStatus.QUEUED
    applied_events: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("order_id", "intent_id", "instrument_id"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "side", OrderSide(self.side))
        object.__setattr__(self, "status", OrderStatus(self.status))
        _integer(self.requested_quantity, "requested_quantity", minimum=1)
        _integer(self.approved_quantity, "approved_quantity")
        _integer(
            self.cumulative_filled_quantity,
            "cumulative_filled_quantity",
        )
        _integer(self.last_source_sequence, "last_source_sequence")
        if self.approved_quantity > self.requested_quantity:
            raise ValueError("approved_quantity cannot exceed requested_quantity")
        if self.cumulative_filled_quantity > self.approved_quantity:
            raise ValueError("cumulative fill cannot exceed approved quantity")
        object.__setattr__(
            self,
            "limit_price",
            _decimal(self.limit_price, "limit_price", positive=True),
        )
        for field_name in ("earliest_at", "expires_at", "updated_at"):
            _aware(getattr(self, field_name), field_name)
        if self.earliest_at >= self.expires_at:
            raise ValueError("earliest_at must precede expires_at")
        if self.status in _MATCHABLE_STATUSES and self.updated_at >= self.expires_at:
            raise ValueError("active order updated_at must precede expires_at")
        if self.status == OrderStatus.FILLED:
            if self.cumulative_filled_quantity != self.requested_quantity:
                raise ValueError("FILLED order requires the requested quantity")
        elif self.status == OrderStatus.PARTIALLY_FILLED:
            if not 0 < self.cumulative_filled_quantity < self.requested_quantity:
                raise ValueError(
                    "PARTIALLY_FILLED order requires an incomplete positive fill"
                )
        elif (
            self.status in {OrderStatus.ACCEPTED, OrderStatus.QUEUED}
            and self.cumulative_filled_quantity
        ):
            raise ValueError("unfilled active status cannot carry a cumulative fill")
        normalized_events: list[tuple[str, str]] = []
        for event in self.applied_events:
            if not isinstance(event, tuple) or len(event) != 2:
                raise TypeError("applied_events entries must be (event_id, fingerprint)")
            normalized_events.append(
                (_text(event[0], "event_id"), _text(event[1], "fingerprint"))
            )
        event_ids = [event_id for event_id, _ in normalized_events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("applied event ids must be unique")
        object.__setattr__(self, "applied_events", tuple(sorted(normalized_events)))

    @property
    def remaining_quantity(self) -> int:
        return self.requested_quantity - self.cumulative_filled_quantity

    @property
    def approved_remaining_quantity(self) -> int:
        return self.approved_quantity - self.cumulative_filled_quantity

    @property
    def is_terminal(self) -> bool:
        return self.status not in _MATCHABLE_STATUSES


@dataclass(frozen=True)
class MatchDecision:
    """One pure match decision and the state to carry into the next call."""

    status: MatchStatus
    reason: MatchReason
    updated_order: LimitDayOrder
    quote_id: str = ""
    fill_quantity: int = 0
    fill_price: Decimal | None = None
    execution_result: ExecutionResult | None = None
    explanation: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", MatchStatus(self.status))
        object.__setattr__(self, "reason", MatchReason(self.reason))
        if type(self.updated_order) is not LimitDayOrder:
            raise TypeError("updated_order must be exactly LimitDayOrder")
        if not isinstance(self.quote_id, str):
            raise TypeError("quote_id must be a string")
        _integer(self.fill_quantity, "fill_quantity")
        if not isinstance(self.explanation, str):
            raise TypeError("explanation must be a string")
        if self.fill_price is not None:
            object.__setattr__(
                self,
                "fill_price",
                _decimal(self.fill_price, "fill_price", positive=True),
            )
        fill_statuses = {
            MatchStatus.PARTIALLY_FILLED,
            MatchStatus.FILLED,
        }
        if self.status in fill_statuses:
            if self.fill_quantity <= 0 or self.fill_price is None:
                raise ValueError("fill decision requires positive quantity and price")
            if self.execution_result is None:
                raise ValueError("fill decision requires an execution result")
            if type(self.execution_result) is not ExecutionResult:
                raise TypeError("execution_result must be exactly ExecutionResult")
        elif self.fill_quantity or self.fill_price is not None:
            raise ValueError("non-fill decision cannot carry fill fields")
        if self.status == MatchStatus.EXPIRED and self.execution_result is None:
            raise ValueError("expiry decision requires an execution result")
        if (
            self.status == MatchStatus.EXPIRED
            and type(self.execution_result) is not ExecutionResult
        ):
            raise TypeError("execution_result must be exactly ExecutionResult")
        if self.status in {
            MatchStatus.WAITING,
            MatchStatus.DUPLICATE,
            MatchStatus.TERMINAL,
        } and self.execution_result is not None:
            raise ValueError("no-op decision cannot carry an execution result")

    @property
    def source_sequence(self) -> int | None:
        return (
            self.execution_result.source_sequence
            if self.execution_result is not None
            else None
        )

    @property
    def idempotency_key(self) -> str:
        return (
            self.execution_result.idempotency_key
            if self.execution_result is not None
            else ""
        )


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
    if isinstance(value, Decimal):
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
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _match_event_fingerprint(
    order: LimitDayOrder,
    quote: Level1Quote,
    rule: LimitDayMatchRule,
) -> str:
    band = rule.price_band
    return _digest(
        "trading-core.limit-day-match-event.v1",
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
                "quote_id": quote.quote_id,
                "instrument_id": quote.instrument_id,
                "observed_at": quote.observed_at,
                "received_at": quote.received_at,
                "bid_price": quote.bid_price,
                "bid_quantity": quote.bid_quantity,
                "ask_price": quote.ask_price,
                "ask_quantity": quote.ask_quantity,
                "suspended": quote.suspended,
            },
            "rule": {
                "rule_version": rule.rule_version,
                "tick_size": rule.tick_size,
                "quote_max_age": rule.quote_max_age,
                "visible_volume_participation": rule.visible_volume_participation,
                "maximum_fill_quantity": rule.maximum_fill_quantity,
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
                "slippage_rate": rule.slippage_rate,
                "impact_rate": rule.impact_rate,
                "block_adverse_limit_lock": rule.block_adverse_limit_lock,
            },
        },
    )


def _expiry_event_id(order: LimitDayOrder) -> str:
    suffix = order.expires_at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    return f"limit-day-expiry:{order.order_id}:{suffix}"


def _append_event(
    order: LimitDayOrder,
    *,
    event_id: str,
    fingerprint: str,
) -> tuple[tuple[str, str], ...]:
    events = dict(order.applied_events)
    events[event_id] = fingerprint
    return tuple(sorted(events.items()))


def _wait(
    order: LimitDayOrder,
    reason: MatchReason,
    explanation: str,
    quote: Level1Quote | None = None,
) -> MatchDecision:
    return MatchDecision(
        status=MatchStatus.WAITING,
        reason=reason,
        updated_order=order,
        quote_id=quote.quote_id if quote is not None else "",
        explanation=explanation,
    )


def _expire_order(
    order: LimitDayOrder,
    *,
    evaluated_at: datetime,
) -> MatchDecision:
    event_id = _expiry_event_id(order)
    fingerprint = _digest(
        "trading-core.limit-day-expiry-event.v1",
        {
            "order_id": order.order_id,
            "intent_id": order.intent_id,
            "expires_at": order.expires_at,
        },
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
    updated = replace(
        order,
        status=OrderStatus.EXPIRED,
        updated_at=order.expires_at,
        last_source_sequence=result.source_sequence,
        applied_events=_append_event(
            order,
            event_id=event_id,
            fingerprint=fingerprint,
        ),
    )
    return MatchDecision(
        status=MatchStatus.EXPIRED,
        reason=MatchReason.NONE,
        updated_order=updated,
        execution_result=result,
        explanation="DAY order reached its exclusive expiry boundary",
    )


def _adverse_price(
    base_price: Decimal,
    *,
    side: OrderSide,
    tick_size: Decimal,
    slippage_rate: Decimal,
    impact_rate: Decimal,
) -> Decimal:
    total_rate = slippage_rate + impact_rate
    multiplier = Decimal("1") + total_rate if side == OrderSide.BUY else Decimal("1") - total_rate
    adjusted = base_price * multiplier
    rounding = ROUND_UP if side == OrderSide.BUY else ROUND_DOWN
    return (adjusted / tick_size).to_integral_value(rounding=rounding) * tick_size


def _usable_price_band(
    *,
    order: LimitDayOrder,
    quote: Level1Quote,
    rule: LimitDayMatchRule,
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
        return None, "price band trade_date does not match quote"
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


def match_limit_day(
    *,
    order: LimitDayOrder,
    quote: Level1Quote | None,
    rule: LimitDayMatchRule,
    evaluated_at: datetime,
) -> MatchDecision:
    """Match one LIMIT + DAY order against one Level-1 quote.

    The validity window is half-open: ``earliest_at <= t < expires_at``.
    Expiry wins at the exact ``expires_at`` boundary, matching the current V2
    service behavior and preventing a fill from racing a DAY expiry.
    """

    if type(order) is not LimitDayOrder:
        raise TypeError("order must be exactly LimitDayOrder")
    if quote is not None and type(quote) is not Level1Quote:
        raise TypeError("quote must be exactly Level1Quote or None")
    if type(rule) is not LimitDayMatchRule:
        raise TypeError("rule must be exactly LimitDayMatchRule")
    evaluated_at = _aware(evaluated_at, "evaluated_at")
    if evaluated_at < order.updated_at:
        raise ValueError("evaluated_at cannot precede order updated_at")

    if order.is_terminal:
        if quote is not None:
            prior = dict(order.applied_events).get(quote.quote_id)
            if prior is not None:
                fingerprint = _match_event_fingerprint(order, quote, rule)
                if prior != fingerprint:
                    raise ValueError(
                        "quote_id was already applied with different order, "
                        "quote, or rule semantics"
                    )
                return MatchDecision(
                    status=MatchStatus.DUPLICATE,
                    reason=MatchReason.DUPLICATE_EVENT,
                    updated_order=order,
                    quote_id=quote.quote_id,
                    explanation="identical quote event was already applied",
                )
        return MatchDecision(
            status=MatchStatus.TERMINAL,
            reason=MatchReason.ORDER_TERMINAL,
            updated_order=order,
            quote_id=quote.quote_id if quote is not None else "",
            explanation=f"order is already terminal with status {order.status.value}",
        )
    if evaluated_at < order.earliest_at:
        return _wait(
            order,
            MatchReason.WAIT_NOT_ACTIVE,
            "order has not reached earliest_at",
            quote,
        )
    if evaluated_at >= order.expires_at:
        return _expire_order(order, evaluated_at=evaluated_at)
    if quote is not None and quote.instrument_id != order.instrument_id:
        raise ValueError("quote instrument does not match order")

    event_fingerprint = None
    if quote is not None:
        event_fingerprint = _match_event_fingerprint(order, quote, rule)
        prior = dict(order.applied_events).get(quote.quote_id)
        if prior is not None:
            if prior != event_fingerprint:
                raise ValueError(
                    "quote_id was already applied with different order, quote, or rule semantics"
                )
            return MatchDecision(
                status=MatchStatus.DUPLICATE,
                reason=MatchReason.DUPLICATE_EVENT,
                updated_order=order,
                quote_id=quote.quote_id,
                explanation="identical quote event was already applied",
            )
    if quote is None:
        return _wait(
            order,
            MatchReason.WAIT_NO_QUOTE,
            "Level-1 quote is missing",
        )
    if quote.observed_at > evaluated_at or quote.received_at > evaluated_at:
        return _wait(
            order,
            MatchReason.WAIT_FUTURE_QUOTE,
            "quote was not observable at evaluated_at",
            quote,
        )
    if quote.observed_at < order.earliest_at:
        return _wait(
            order,
            MatchReason.WAIT_PRE_ORDER_QUOTE,
            "quote precedes the order execution window",
            quote,
        )
    if quote.observed_at < order.updated_at:
        return _wait(
            order,
            MatchReason.WAIT_OUT_OF_ORDER_QUOTE,
            "quote precedes the current order state",
            quote,
        )
    if evaluated_at - quote.observed_at > rule.quote_max_age:
        return _wait(
            order,
            MatchReason.WAIT_STALE_QUOTE,
            "quote exceeds quote_max_age",
            quote,
        )
    if quote.suspended:
        return _wait(
            order,
            MatchReason.WAIT_SUSPENDED,
            "instrument is suspended",
            quote,
        )

    if order.side == OrderSide.BUY:
        base_price = quote.ask_price
        visible_quantity = quote.ask_quantity
    else:
        base_price = quote.bid_price
        visible_quantity = quote.bid_quantity
    if base_price is None or visible_quantity is None or visible_quantity <= 0:
        return _wait(
            order,
            MatchReason.WAIT_NO_QUOTE,
            "opposing Level-1 price/quantity is missing",
            quote,
        )

    band, band_error = _usable_price_band(
        order=order,
        quote=quote,
        rule=rule,
        evaluated_at=evaluated_at,
    )
    if band_error:
        return _wait(
            order,
            MatchReason.WAIT_PRICE_BAND,
            band_error,
            quote,
        )
    if rule.block_adverse_limit_lock and band is not None:
        if (
            order.side == OrderSide.BUY
            and band.upper is not None
            and base_price >= band.upper
        ) or (
            order.side == OrderSide.SELL
            and band.lower is not None
            and base_price <= band.lower
        ):
            return _wait(
                order,
                MatchReason.WAIT_LIMIT_LOCK,
                "opposing quote is locked at the adverse price-band edge",
                quote,
            )

    fill_price = _adverse_price(
        base_price,
        side=order.side,
        tick_size=rule.tick_size,
        slippage_rate=rule.slippage_rate,
        impact_rate=rule.impact_rate,
    )
    if fill_price <= 0:
        return _wait(
            order,
            MatchReason.WAIT_LIQUIDITY,
            "adverse price is not executable",
            quote,
        )
    if (
        band is not None
        and rule.enforce_price_band_bounds
        and not band.contains(fill_price)
    ):
        return _wait(
            order,
            MatchReason.WAIT_PRICE_BAND,
            "adverse fill price is outside the price band",
            quote,
        )
    if (
        order.side == OrderSide.BUY
        and (base_price > order.limit_price or fill_price > order.limit_price)
    ) or (
        order.side == OrderSide.SELL
        and (base_price < order.limit_price or fill_price < order.limit_price)
    ):
        return _wait(
            order,
            MatchReason.WAIT_LIQUIDITY,
            "opposing/adverse price does not satisfy the order limit",
            quote,
        )

    visible_cap = int(
        Decimal(visible_quantity) * rule.visible_volume_participation
    )
    quantity_caps = [
        order.remaining_quantity,
        order.approved_remaining_quantity,
        visible_cap,
    ]
    if rule.maximum_fill_quantity is not None:
        quantity_caps.append(rule.maximum_fill_quantity)
    fill_quantity = min(quantity_caps)
    if fill_quantity <= 0:
        return _wait(
            order,
            MatchReason.WAIT_LIQUIDITY,
            "executable quantity is zero",
            quote,
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
        event_id=quote.quote_id,
        status=next_status,
        occurred_at=evaluated_at,
        received_at=evaluated_at,
        source_sequence=order.last_source_sequence + 1,
        idempotency_key=execution_result_idempotency_key(
            order_id=order.order_id,
            event_id=quote.quote_id,
        ),
        last_fill_quantity=fill_quantity,
        last_fill_price=fill_price,
    )
    assert event_fingerprint is not None
    updated = replace(
        order,
        status=next_status,
        cumulative_filled_quantity=next_cumulative,
        updated_at=evaluated_at,
        last_source_sequence=result.source_sequence,
        applied_events=_append_event(
            order,
            event_id=quote.quote_id,
            fingerprint=event_fingerprint,
        ),
    )
    return MatchDecision(
        status=MatchStatus(next_status.value),
        reason=MatchReason.NONE,
        updated_order=updated,
        quote_id=quote.quote_id,
        fill_quantity=fill_quantity,
        fill_price=fill_price,
        execution_result=result,
        explanation="matched against a fresh opposing Level-1 quote",
    )


# Concise neutral aliases for callers that already import from this module.
Quote = Level1Quote
Order = LimitDayOrder
Rule = LimitDayMatchRule


__all__ = [
    "Level1Quote",
    "LimitDayMatchRule",
    "LimitDayOrder",
    "MatchDecision",
    "MatchPriceBand",
    "MatchReason",
    "MatchStatus",
    "Order",
    "Quote",
    "Rule",
    "match_limit_day",
]
