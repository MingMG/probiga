"""Versioned instrument rules and side-effect-free order validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum

from ..contracts import ExecutionIntent, OrderSide, OrderType, TimeInForce


class RuleViolation(str, Enum):
    INSTRUMENT_MISMATCH = "INSTRUMENT_MISMATCH"
    RULE_NOT_EFFECTIVE = "RULE_NOT_EFFECTIVE"
    INSTRUMENT_SUSPENDED = "INSTRUMENT_SUSPENDED"
    BUY_QUANTITY_INVALID = "BUY_QUANTITY_INVALID"
    SELL_QUANTITY_INVALID = "SELL_QUANTITY_INVALID"
    INSUFFICIENT_POSITION = "INSUFFICIENT_POSITION"
    POSITION_DATA_UNAVAILABLE = "POSITION_DATA_UNAVAILABLE"
    T1_QUANTITY_LOCKED = "T1_QUANTITY_LOCKED"
    ORDER_QUANTITY_EXCEEDS_MAX = "ORDER_QUANTITY_EXCEEDS_MAX"
    LIMIT_PRICE_OFF_TICK = "LIMIT_PRICE_OFF_TICK"
    LIMIT_PRICE_OUTSIDE_BAND = "LIMIT_PRICE_OUTSIDE_BAND"
    PRICE_BAND_UNAVAILABLE = "PRICE_BAND_UNAVAILABLE"
    DYNAMIC_PRICE_CAGE_UNAVAILABLE = "DYNAMIC_PRICE_CAGE_UNAVAILABLE"
    LIMIT_PRICE_OUTSIDE_DYNAMIC_CAGE = "LIMIT_PRICE_OUTSIDE_DYNAMIC_CAGE"
    UNSUPPORTED_EXECUTION_MODE = "UNSUPPORTED_EXECUTION_MODE"
    RULE_VERSION_MISMATCH = "RULE_VERSION_MISMATCH"
    FEE_PROFILE_VERSION_MISMATCH = "FEE_PROFILE_VERSION_MISMATCH"


@dataclass(frozen=True)
class InstrumentRule:
    instrument_id: str
    rule_version: str
    fee_profile_version: str
    effective_from: date
    effective_to: date | None
    buy_lot_size: int
    minimum_buy_quantity: int
    sell_lot_size: int
    settlement_days: int
    tick_size: Decimal
    price_limit_ratio: Decimal | None
    maximum_order_quantity: int = 1_000_000
    allow_odd_lot_liquidation: bool = True
    requires_dynamic_price_cage: bool = False

    def __post_init__(self) -> None:
        for name in ("instrument_id", "rule_version", "fee_profile_version"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
            normalized = value.strip()
            if not normalized:
                raise ValueError(f"{name} is required")
            object.__setattr__(self, name, normalized)
        if type(self.effective_from) is not date:
            raise TypeError("effective_from must be exactly date")
        if self.effective_to is not None and (
            type(self.effective_to) is not date
        ):
            raise TypeError("effective_to must be exactly date or None")
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("effective_to cannot precede effective_from")
        for name, minimum in (
            ("buy_lot_size", 1),
            ("minimum_buy_quantity", 1),
            ("sell_lot_size", 1),
            ("settlement_days", 0),
            ("maximum_order_quantity", 1),
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{name} must be an integer")
            if value < minimum:
                raise ValueError(f"{name} must be at least {minimum}")
        if isinstance(self.tick_size, Decimal) and type(self.tick_size) is not Decimal:
            raise TypeError("tick_size must not be a Decimal subclass")
        object.__setattr__(self, "tick_size", Decimal(str(self.tick_size)))
        if self.price_limit_ratio is not None:
            if (
                isinstance(self.price_limit_ratio, Decimal)
                and type(self.price_limit_ratio) is not Decimal
            ):
                raise TypeError(
                    "price_limit_ratio must not be a Decimal subclass"
                )
            object.__setattr__(
                self,
                "price_limit_ratio",
                Decimal(str(self.price_limit_ratio)),
            )
        if not self.tick_size.is_finite() or self.tick_size <= 0:
            raise ValueError("tick_size must be positive")
        if self.price_limit_ratio is not None and (
            not self.price_limit_ratio.is_finite()
            or self.price_limit_ratio <= 0
            or self.price_limit_ratio >= 1
        ):
            raise ValueError("price_limit_ratio must be between zero and one")
        for name in (
            "allow_odd_lot_liquidation",
            "requires_dynamic_price_cage",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")

    def applies_on(self, trade_date: date) -> bool:
        if type(trade_date) is not date:
            raise TypeError("trade_date must be exactly date")
        return self.effective_from <= trade_date and (
            self.effective_to is None or trade_date <= self.effective_to
        )


@dataclass(frozen=True)
class PriceBand:
    instrument_id: str
    trade_date: date
    as_of: datetime
    source: str
    lower: Decimal
    upper: Decimal

    def __post_init__(self) -> None:
        for name in ("instrument_id", "source"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
            normalized = value.strip()
            if not normalized:
                raise ValueError(f"{name} is required")
            object.__setattr__(self, name, normalized)
        if not isinstance(self.trade_date, date) or isinstance(
            self.trade_date,
            datetime,
        ):
            raise TypeError("trade_date must be a date")
        if not isinstance(self.as_of, datetime):
            raise TypeError("as_of must be a datetime")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        lower = Decimal(str(self.lower))
        upper = Decimal(str(self.upper))
        if (
            not lower.is_finite()
            or not upper.is_finite()
            or lower <= 0
            or upper < lower
        ):
            raise ValueError("price band must be finite, positive and ordered")
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)

    def contains(self, price: Decimal) -> bool:
        return self.lower <= price <= self.upper

    def is_fresh(
        self,
        *,
        evaluated_at: datetime,
        max_age: timedelta,
    ) -> bool:
        if not isinstance(evaluated_at, datetime):
            raise TypeError("evaluated_at must be a datetime")
        if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
            raise ValueError("evaluated_at must be timezone-aware")
        if type(max_age) is not timedelta:
            raise TypeError("max_age must be exactly timedelta")
        if max_age < timedelta(0):
            raise ValueError("max_age cannot be negative")
        return self.as_of <= evaluated_at and evaluated_at - self.as_of <= max_age


@dataclass(frozen=True)
class RuleCheck:
    allowed: bool
    violations: tuple[RuleViolation, ...]


def _tick_round(value: Decimal, tick_size: Decimal) -> Decimal:
    units = (value / tick_size).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return units * tick_size


def calculate_price_band(
    *,
    instrument_id: str,
    trade_date: date,
    as_of: datetime,
    source: str,
    previous_close: Decimal,
    limit_ratio: Decimal | None,
    tick_size: Decimal,
) -> PriceBand | None:
    if previous_close <= 0 or tick_size <= 0:
        raise ValueError("previous_close and tick_size must be positive")
    if limit_ratio is None:
        return None
    if limit_ratio <= 0 or limit_ratio >= 1:
        raise ValueError("limit_ratio must be between zero and one")
    return PriceBand(
        instrument_id=instrument_id,
        trade_date=trade_date,
        as_of=as_of,
        source=source,
        lower=_tick_round(previous_close * (Decimal("1") - limit_ratio), tick_size),
        upper=_tick_round(previous_close * (Decimal("1") + limit_ratio), tick_size),
    )


def is_tick_aligned(price: Decimal, tick_size: Decimal) -> bool:
    if price <= 0 or tick_size <= 0:
        return False
    return price % tick_size == 0


def floor_buy_quantity(quantity: int, rule: InstrumentRule) -> int:
    if not isinstance(quantity, int) or isinstance(quantity, bool):
        raise TypeError("quantity must be an integer")
    if quantity < rule.minimum_buy_quantity:
        return 0
    return quantity - quantity % rule.buy_lot_size


def _sell_quantity_valid(
    *,
    quantity: int,
    total_position_quantity: int,
    sellable_quantity: int,
    rule: InstrumentRule,
) -> bool:
    if quantity <= 0 or quantity > sellable_quantity:
        return False
    if quantity % rule.sell_lot_size == 0:
        return True
    odd_remainder = total_position_quantity % rule.sell_lot_size
    return (
        rule.allow_odd_lot_liquidation
        and odd_remainder > 0
        and quantity % rule.sell_lot_size == odd_remainder
    )


def _price_band_is_usable(
    band: PriceBand,
    *,
    instrument_id: str,
    trade_date: date,
    evaluated_at: datetime,
    max_age: timedelta | None,
) -> bool:
    if (
        band.instrument_id != instrument_id
        or band.trade_date != trade_date
        or max_age is None
    ):
        return False
    return band.is_fresh(evaluated_at=evaluated_at, max_age=max_age)


def validate_intent_against_rule(
    intent: ExecutionIntent,
    *,
    rule: InstrumentRule,
    trade_date: date,
    evaluated_at: datetime,
    total_position_quantity: int | None = None,
    broker_sellable_quantity: int | None = None,
    locally_computed_sellable_quantity: int | None = None,
    authoritative_price_band: PriceBand | None = None,
    authoritative_price_band_max_age: timedelta | None = None,
    dynamic_price_cage: PriceBand | None = None,
    dynamic_price_cage_max_age: timedelta | None = None,
    suspended: bool = False,
) -> RuleCheck:
    violations: list[RuleViolation] = []
    if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
        raise TypeError("trade_date must be a date")
    if not isinstance(evaluated_at, datetime):
        raise TypeError("evaluated_at must be a datetime")
    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        raise ValueError("evaluated_at must be timezone-aware")
    for field_name, value in (
        ("total_position_quantity", total_position_quantity),
        ("broker_sellable_quantity", broker_sellable_quantity),
        (
            "locally_computed_sellable_quantity",
            locally_computed_sellable_quantity,
        ),
    ):
        if value is not None and (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
        ):
            raise ValueError(
                f"{field_name} must be a non-negative integer or None"
            )
    if (
        total_position_quantity is not None
        and broker_sellable_quantity is not None
        and broker_sellable_quantity > total_position_quantity
    ):
        raise ValueError(
            "broker_sellable_quantity cannot exceed total position"
        )
    if (
        total_position_quantity is not None
        and locally_computed_sellable_quantity is not None
        and locally_computed_sellable_quantity > total_position_quantity
    ):
        raise ValueError(
            "locally computed sellable quantity cannot exceed total position"
        )
    if intent.instrument_id != rule.instrument_id:
        violations.append(RuleViolation.INSTRUMENT_MISMATCH)
    if intent.rule_version != rule.rule_version:
        violations.append(RuleViolation.RULE_VERSION_MISMATCH)
    if intent.fee_profile_version != rule.fee_profile_version:
        violations.append(RuleViolation.FEE_PROFILE_VERSION_MISMATCH)
    if not rule.applies_on(trade_date):
        violations.append(RuleViolation.RULE_NOT_EFFECTIVE)
    if suspended:
        violations.append(RuleViolation.INSTRUMENT_SUSPENDED)
    if intent.quantity > rule.maximum_order_quantity:
        violations.append(RuleViolation.ORDER_QUANTITY_EXCEEDS_MAX)
    if (
        intent.order_type != OrderType.LIMIT
        or intent.time_in_force != TimeInForce.DAY
    ):
        violations.append(RuleViolation.UNSUPPORTED_EXECUTION_MODE)
    if intent.side == OrderSide.BUY:
        if floor_buy_quantity(intent.quantity, rule) != intent.quantity:
            violations.append(RuleViolation.BUY_QUANTITY_INVALID)
    elif (
        total_position_quantity is None
        or broker_sellable_quantity is None
        or locally_computed_sellable_quantity is None
    ):
        violations.append(RuleViolation.POSITION_DATA_UNAVAILABLE)
    elif intent.quantity > total_position_quantity:
        violations.append(RuleViolation.INSUFFICIENT_POSITION)
    else:
        effective_sellable = min(
            broker_sellable_quantity,
            locally_computed_sellable_quantity,
        )
        if intent.quantity > effective_sellable:
            violations.append(RuleViolation.T1_QUANTITY_LOCKED)
        elif not _sell_quantity_valid(
            quantity=intent.quantity,
            total_position_quantity=total_position_quantity,
            sellable_quantity=effective_sellable,
            rule=rule,
        ):
            violations.append(RuleViolation.SELL_QUANTITY_INVALID)

    if intent.order_type == OrderType.LIMIT:
        assert intent.limit_price is not None
        if not is_tick_aligned(intent.limit_price, rule.tick_size):
            violations.append(RuleViolation.LIMIT_PRICE_OFF_TICK)
        band = authoritative_price_band
        if band is not None:
            if not _price_band_is_usable(
                band,
                instrument_id=intent.instrument_id,
                trade_date=trade_date,
                evaluated_at=evaluated_at,
                max_age=authoritative_price_band_max_age,
            ):
                violations.append(RuleViolation.PRICE_BAND_UNAVAILABLE)
            elif not band.contains(intent.limit_price):
                violations.append(RuleViolation.LIMIT_PRICE_OUTSIDE_BAND)
        elif rule.price_limit_ratio is not None:
            violations.append(RuleViolation.PRICE_BAND_UNAVAILABLE)
        if rule.requires_dynamic_price_cage:
            if dynamic_price_cage is None:
                violations.append(RuleViolation.DYNAMIC_PRICE_CAGE_UNAVAILABLE)
            elif not _price_band_is_usable(
                dynamic_price_cage,
                instrument_id=intent.instrument_id,
                trade_date=trade_date,
                evaluated_at=evaluated_at,
                max_age=dynamic_price_cage_max_age,
            ):
                violations.append(RuleViolation.DYNAMIC_PRICE_CAGE_UNAVAILABLE)
            elif not dynamic_price_cage.contains(intent.limit_price):
                violations.append(
                    RuleViolation.LIMIT_PRICE_OUTSIDE_DYNAMIC_CAGE
                )
    return RuleCheck(allowed=not violations, violations=tuple(violations))
