"""Deterministic order-level fee calculations using Decimal arithmetic."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

from ..contracts import OrderSide


def _decimal(value: Decimal | str | int | float) -> Decimal:
    if isinstance(value, Decimal):
        if type(value) is not Decimal:
            raise TypeError("fee values must not use Decimal subclasses")
        result = value
    elif type(value) in {str, int, float}:
        result = Decimal(str(value))
    else:
        raise TypeError("fee values must be decimal-like primitives")
    if not result.is_finite():
        raise ValueError("fee values must be finite")
    return result


@dataclass(frozen=True)
class FeeSchedule:
    profile_version: str
    buy_commission_rate: Decimal
    sell_commission_rate: Decimal
    minimum_commission: Decimal
    stamp_duty_sell_rate: Decimal
    transfer_fee_buy_rate: Decimal
    transfer_fee_sell_rate: Decimal
    other_buy_rate: Decimal = Decimal("0")
    other_sell_rate: Decimal = Decimal("0")
    other_buy_fixed: Decimal = Decimal("0")
    other_sell_fixed: Decimal = Decimal("0")
    other_buy_per_share: Decimal = Decimal("0")
    other_sell_per_share: Decimal = Decimal("0")
    currency_quantum: Decimal = Decimal("0.01")
    fee_rounding_mode: str = ROUND_HALF_UP
    aggregate_fee_rounding: bool = False
    round_notional_before_fees: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.profile_version, str):
            raise TypeError("profile_version must be a string")
        if not self.profile_version.strip():
            raise ValueError("profile_version is required")
        object.__setattr__(self, "profile_version", self.profile_version.strip())
        names = (
            "buy_commission_rate",
            "sell_commission_rate",
            "minimum_commission",
            "stamp_duty_sell_rate",
            "transfer_fee_buy_rate",
            "transfer_fee_sell_rate",
            "other_buy_rate",
            "other_sell_rate",
            "other_buy_fixed",
            "other_sell_fixed",
            "other_buy_per_share",
            "other_sell_per_share",
            "currency_quantum",
        )
        for name in names:
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        values = tuple(getattr(self, name) for name in names[:-1])
        if any(not value.is_finite() or value < 0 for value in values):
            raise ValueError("fee schedule values must be finite and non-negative")
        if not self.currency_quantum.is_finite() or self.currency_quantum <= 0:
            raise ValueError("currency_quantum must be positive")
        if not isinstance(self.fee_rounding_mode, str):
            raise TypeError("fee_rounding_mode must be a string")
        if self.fee_rounding_mode not in {ROUND_HALF_UP, ROUND_DOWN}:
            raise ValueError("unsupported fee_rounding_mode")
        if not isinstance(self.aggregate_fee_rounding, bool):
            raise TypeError("aggregate_fee_rounding must be a bool")
        if not isinstance(self.round_notional_before_fees, bool):
            raise TypeError("round_notional_before_fees must be a bool")


@dataclass(frozen=True)
class FeeBreakdown:
    """Auditable components whose sum equals the actually charged fee.

    ``rounding_adjustment`` may be signed because aggregate-level rounding can
    differ from the sum of independently rounded components.  The resulting
    ``total`` is nevertheless required to be non-negative.
    """

    fee_profile_version: str
    notional: Decimal
    commission: Decimal
    stamp_duty: Decimal
    transfer_fee: Decimal
    other_fee: Decimal = Decimal("0")
    rounding_adjustment: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if not isinstance(self.fee_profile_version, str):
            raise TypeError("fee_profile_version must be a string")
        if not self.fee_profile_version.strip():
            raise ValueError("fee_profile_version is required")
        object.__setattr__(
            self,
            "fee_profile_version",
            self.fee_profile_version.strip(),
        )
        for name in (
            "notional",
            "commission",
            "stamp_duty",
            "transfer_fee",
            "other_fee",
        ):
            value = _decimal(getattr(self, name))
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        adjustment = _decimal(self.rounding_adjustment)
        object.__setattr__(self, "rounding_adjustment", adjustment)
        if self.total < 0:
            raise ValueError("total fee must be non-negative")

    @property
    def total(self) -> Decimal:
        return (
            self.commission
            + self.stamp_duty
            + self.transfer_fee
            + self.other_fee
            + self.rounding_adjustment
        )


def _money(value: Decimal, schedule: FeeSchedule) -> Decimal:
    return value.quantize(
        schedule.currency_quantum,
        rounding=schedule.fee_rounding_mode,
    )


def _quantity(value: int | None, *, required: bool) -> int:
    if value is None:
        if required:
            raise ValueError("quantity is required when per-share fees are configured")
        return 0
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("quantity must be an integer")
    if value <= 0:
        raise ValueError("quantity must be positive")
    return value


def calculate_order_fees(
    *,
    side: OrderSide,
    schedule: FeeSchedule,
    price: Decimal | str | int | float | None = None,
    quantity: int | None = None,
    notional: Decimal | str | int | float | None = None,
) -> FeeBreakdown:
    """Calculate fees once for an order's aggregate executed notional.

    Callers must not apply the minimum commission independently to every
    partial fill.  They should aggregate the order's fills and call this
    function with the resulting notional.  A cumulative quantity is also
    required when the selected side has a non-zero per-share fee.
    """

    side = OrderSide(side)
    if notional is None:
        if price is None or quantity is None:
            raise ValueError("provide notional or both price and quantity")
        executed_quantity = _quantity(quantity, required=True)
        gross = _decimal(price) * executed_quantity
        if schedule.round_notional_before_fees:
            gross = _money(gross, schedule)
    else:
        if price is not None:
            raise ValueError("notional is mutually exclusive with price")
        gross = _decimal(notional)
    if gross <= 0:
        raise ValueError("notional must be positive")

    commission_rate = (
        schedule.buy_commission_rate
        if side == OrderSide.BUY
        else schedule.sell_commission_rate
    )
    transfer_rate = (
        schedule.transfer_fee_buy_rate
        if side == OrderSide.BUY
        else schedule.transfer_fee_sell_rate
    )
    other_rate = (
        schedule.other_buy_rate
        if side == OrderSide.BUY
        else schedule.other_sell_rate
    )
    other_fixed = (
        schedule.other_buy_fixed
        if side == OrderSide.BUY
        else schedule.other_sell_fixed
    )
    other_per_share = (
        schedule.other_buy_per_share
        if side == OrderSide.BUY
        else schedule.other_sell_per_share
    )
    executed_quantity = _quantity(
        quantity,
        required=other_per_share > 0,
    )
    raw_commission = max(
        schedule.minimum_commission,
        gross * commission_rate,
    )
    raw_transfer_fee = gross * transfer_rate
    raw_stamp_duty = (
        gross * schedule.stamp_duty_sell_rate
        if side == OrderSide.SELL
        else Decimal("0")
    )
    raw_other_fee = (
        gross * other_rate
        + other_fixed
        + Decimal(executed_quantity) * other_per_share
    )
    commission = _money(raw_commission, schedule)
    transfer_fee = _money(raw_transfer_fee, schedule)
    stamp_duty = _money(raw_stamp_duty, schedule)
    other_fee = _money(raw_other_fee, schedule)
    rounding_adjustment = Decimal("0")
    if schedule.aggregate_fee_rounding:
        rounded_total = _money(
            raw_commission
            + raw_transfer_fee
            + raw_stamp_duty
            + raw_other_fee,
            schedule,
        )
        rounding_adjustment = rounded_total - (
            commission + transfer_fee + stamp_duty + other_fee
        )
    return FeeBreakdown(
        fee_profile_version=schedule.profile_version,
        notional=_money(gross, schedule),
        commission=commission,
        stamp_duty=stamp_duty,
        transfer_fee=transfer_fee,
        other_fee=other_fee,
        rounding_adjustment=rounding_adjustment,
    )


def cash_effect(*, side: OrderSide, fees: FeeBreakdown) -> Decimal:
    """Signed account-cash change caused by the aggregate fill."""

    side = OrderSide(side)
    if side == OrderSide.BUY:
        return -(fees.notional + fees.total)
    return fees.notional - fees.total


def incremental_order_fee_delta(
    *,
    side: OrderSide,
    schedule: FeeSchedule,
    previous_notional: Decimal | str | int | float,
    new_total_notional: Decimal | str | int | float,
    previous_quantity: int = 0,
    new_total_quantity: int | None = None,
) -> FeeBreakdown:
    """Return the fee delta from cumulative notional and quantity snapshots."""

    previous = _decimal(previous_notional)
    total = _decimal(new_total_notional)
    if previous < 0 or total <= previous:
        raise ValueError(
            "new_total_notional must be positive and exceed previous_notional"
        )
    if not isinstance(previous_quantity, int) or isinstance(
        previous_quantity,
        bool,
    ):
        raise TypeError("previous_quantity must be an integer")
    if previous_quantity < 0:
        raise ValueError("previous_quantity cannot be negative")
    if previous == 0 and previous_quantity != 0:
        raise ValueError("zero previous_notional requires zero previous_quantity")
    if new_total_quantity is not None:
        if not isinstance(new_total_quantity, int) or isinstance(
            new_total_quantity,
            bool,
        ):
            raise TypeError("new_total_quantity must be an integer")
        if new_total_quantity <= previous_quantity:
            raise ValueError(
                "new_total_quantity must exceed previous_quantity"
            )
    total_fees = calculate_order_fees(
        side=side,
        schedule=schedule,
        notional=total,
        quantity=new_total_quantity,
    )
    if previous == 0:
        prior_fees = FeeBreakdown(
            fee_profile_version=schedule.profile_version,
            notional=Decimal("0"),
            commission=Decimal("0"),
            stamp_duty=Decimal("0"),
            transfer_fee=Decimal("0"),
            other_fee=Decimal("0"),
            rounding_adjustment=Decimal("0"),
        )
    else:
        prior_fees = calculate_order_fees(
            side=side,
            schedule=schedule,
            notional=previous,
            quantity=(
                previous_quantity if previous_quantity > 0 else None
            ),
        )
    return FeeBreakdown(
        fee_profile_version=schedule.profile_version,
        notional=total_fees.notional - prior_fees.notional,
        commission=total_fees.commission - prior_fees.commission,
        stamp_duty=total_fees.stamp_duty - prior_fees.stamp_duty,
        transfer_fee=total_fees.transfer_fee - prior_fees.transfer_fee,
        other_fee=total_fees.other_fee - prior_fees.other_fee,
        rounding_adjustment=(
            total_fees.rounding_adjustment
            - prior_fees.rounding_adjustment
        ),
    )
