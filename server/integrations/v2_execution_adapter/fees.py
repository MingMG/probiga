"""Side-effect-free V2 fee-profile compatibility adapter."""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from server.trading_core.market_rules import FeeSchedule
from server.trading_v2.ledger import FeeProfile


V2_MONEY_QUANTUM = Decimal("0.01")


def v2_fill_gross(
    *,
    price: Decimal | str | int | float,
    quantity: int,
) -> Decimal:
    """Return one V2 fill's positive, cent-rounded gross amount.

    V2 rounds each fill's ``price * quantity`` down before calculating or
    accumulating fees.  This helper exposes that boundary without importing a
    ledger, submitting an order, or touching storage.
    """

    if not isinstance(quantity, int) or isinstance(quantity, bool):
        raise TypeError("quantity must be an integer")
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if isinstance(price, bool) or not isinstance(
        price,
        (Decimal, str, int, float),
    ):
        raise TypeError("price must be a decimal-compatible value")
    try:
        converted = price if isinstance(price, Decimal) else Decimal(str(price))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise ValueError("price must be a decimal-compatible value") from exc
    if not converted.is_finite() or converted <= 0:
        raise ValueError("price must be finite and positive")
    try:
        gross = (converted * quantity).quantize(
            V2_MONEY_QUANTUM,
            rounding=ROUND_DOWN,
        )
    except ArithmeticError as exc:
        raise ValueError("fill gross cannot be represented as V2 money") from exc
    if gross <= 0:
        raise ValueError("rounded fill gross must be positive")
    return gross


def v2_fee_profile_to_neutral_schedule(profile: FeeProfile) -> FeeSchedule:
    """Map one frozen V2 profile without submitting orders or touching storage.

    V2 rounds the aggregate fee down to cents after adding every component.
    Both that behavior and all optional ``other_*`` fields are made explicit
    in the neutral schedule; invalid V2 values fail through its strict
    constructor rather than being silently replaced with defaults.
    """

    if not isinstance(profile, FeeProfile):
        raise TypeError("profile must be a V2 FeeProfile")
    return FeeSchedule(
        profile_version=profile.version,
        buy_commission_rate=profile.buy_commission_rate,
        sell_commission_rate=profile.sell_commission_rate,
        minimum_commission=profile.minimum_commission,
        stamp_duty_sell_rate=profile.stamp_tax_sell_rate,
        transfer_fee_buy_rate=profile.transfer_fee_buy_rate,
        transfer_fee_sell_rate=profile.transfer_fee_sell_rate,
        other_buy_rate=profile.other_buy_rate,
        other_sell_rate=profile.other_sell_rate,
        other_buy_fixed=profile.other_buy_fixed,
        other_sell_fixed=profile.other_sell_fixed,
        other_buy_per_share=profile.other_buy_per_share,
        other_sell_per_share=profile.other_sell_per_share,
        fee_rounding_mode=ROUND_DOWN,
        aggregate_fee_rounding=True,
        round_notional_before_fees=True,
    )
