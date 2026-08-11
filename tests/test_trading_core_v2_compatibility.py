from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_DOWN

import pytest

from server.integrations.v2_execution_adapter import (
    v2_fee_profile_to_neutral_schedule,
    v2_fill_gross,
)
from server.trading_core.contracts import OrderSide as NeutralOrderSide
from server.trading_core.market_rules import (
    calculate_order_fees,
    incremental_order_fee_delta,
)
from server.trading_v2.domain import OrderSide as V2OrderSide
from server.trading_v2.ledger import FeeProfile


def _v2_profile() -> FeeProfile:
    return FeeProfile(
        version="v2-differential-fee-v1",
        buy_commission_rate=Decimal("0.00027"),
        sell_commission_rate=Decimal("0.00031"),
        minimum_commission=Decimal("5.00"),
        stamp_tax_sell_rate=Decimal("0.0005"),
        transfer_fee_buy_rate=Decimal("0.00001"),
        transfer_fee_sell_rate=Decimal("0.00002"),
        other_buy_rate=Decimal("0.000013"),
        other_sell_rate=Decimal("0.000017"),
        other_buy_fixed=Decimal("0.37"),
        other_sell_fixed=Decimal("0.41"),
        other_buy_per_share=Decimal("0.0007"),
        other_sell_per_share=Decimal("0.0009"),
    )


def test_v2_fee_adapter_maps_every_field_and_fails_closed_on_bad_profile():
    profile = _v2_profile()
    schedule = v2_fee_profile_to_neutral_schedule(profile)

    assert schedule.profile_version == profile.version
    for neutral_name, v2_name in (
        ("buy_commission_rate", "buy_commission_rate"),
        ("sell_commission_rate", "sell_commission_rate"),
        ("minimum_commission", "minimum_commission"),
        ("stamp_duty_sell_rate", "stamp_tax_sell_rate"),
        ("transfer_fee_buy_rate", "transfer_fee_buy_rate"),
        ("transfer_fee_sell_rate", "transfer_fee_sell_rate"),
        ("other_buy_rate", "other_buy_rate"),
        ("other_sell_rate", "other_sell_rate"),
        ("other_buy_fixed", "other_buy_fixed"),
        ("other_sell_fixed", "other_sell_fixed"),
        ("other_buy_per_share", "other_buy_per_share"),
        ("other_sell_per_share", "other_sell_per_share"),
    ):
        assert getattr(schedule, neutral_name) == getattr(profile, v2_name)
    assert schedule.fee_rounding_mode == ROUND_DOWN
    assert schedule.aggregate_fee_rounding is True
    assert schedule.round_notional_before_fees is True

    with pytest.raises(ValueError, match="non-negative"):
        v2_fee_profile_to_neutral_schedule(
            replace(profile, other_buy_rate=Decimal("-0.01"))
        )
    with pytest.raises(TypeError, match="V2 FeeProfile"):
        v2_fee_profile_to_neutral_schedule(object())
    with pytest.raises(TypeError, match="round_notional_before_fees"):
        replace(schedule, round_notional_before_fees=1)


@pytest.mark.parametrize(
    ("neutral_side", "v2_side", "notional", "quantity"),
    (
        (NeutralOrderSide.BUY, V2OrderSide.BUY, Decimal("1000"), 100),
        (NeutralOrderSide.SELL, V2OrderSide.SELL, Decimal("1000"), 100),
        (NeutralOrderSide.BUY, V2OrderSide.BUY, Decimal("12345.67"), 1200),
        (NeutralOrderSide.SELL, V2OrderSide.SELL, Decimal("12345.67"), 1200),
        (NeutralOrderSide.BUY, V2OrderSide.BUY, Decimal("100000"), 10000),
        (NeutralOrderSide.SELL, V2OrderSide.SELL, Decimal("100000"), 10000),
    ),
)
def test_v2_and_neutral_full_fee_profiles_are_cent_exact(
    neutral_side: NeutralOrderSide,
    v2_side: V2OrderSide,
    notional: Decimal,
    quantity: int,
):
    profile = _v2_profile()
    schedule = v2_fee_profile_to_neutral_schedule(profile)

    expected = profile.calculate(v2_side, notional, quantity=quantity)
    actual = calculate_order_fees(
        side=neutral_side,
        schedule=schedule,
        notional=notional,
        quantity=quantity,
    )
    assert actual.total == expected
    assert actual.total == actual.total.quantize(Decimal("0.01"))


def test_price_quantity_path_rounds_v2_odd_lot_gross_before_fees():
    profile = _v2_profile()
    schedule = v2_fee_profile_to_neutral_schedule(profile)
    price = Decimal("0.985")
    quantity = 7
    gross = v2_fill_gross(price=price, quantity=quantity)

    assert gross == Decimal("6.89")
    expected = profile.calculate(
        V2OrderSide.SELL,
        gross,
        quantity=quantity,
    )
    actual = calculate_order_fees(
        side=NeutralOrderSide.SELL,
        schedule=schedule,
        price=price,
        quantity=quantity,
    )
    assert expected == Decimal("5.41")
    assert actual.notional == gross
    assert actual.total == expected


@pytest.mark.parametrize(
    ("price", "quantity", "expected_exception"),
    (
        (Decimal("0"), 1, ValueError),
        (Decimal("-1"), 1, ValueError),
        (Decimal("NaN"), 1, ValueError),
        (Decimal("Infinity"), 1, ValueError),
        (Decimal("0.001"), 1, ValueError),
        (object(), 1, TypeError),
        (Decimal("1"), 0, ValueError),
        (Decimal("1"), -1, ValueError),
        (Decimal("1"), 1.5, TypeError),
        (Decimal("1"), True, TypeError),
    ),
)
def test_v2_fill_gross_rejects_invalid_inputs(
    price: object,
    quantity: object,
    expected_exception: type[Exception],
):
    with pytest.raises(expected_exception):
        v2_fill_gross(price=price, quantity=quantity)


@pytest.mark.parametrize(
    ("neutral_side", "v2_side"),
    (
        (NeutralOrderSide.BUY, V2OrderSide.BUY),
        (NeutralOrderSide.SELL, V2OrderSide.SELL),
    ),
)
def test_minimum_commission_stamp_and_transfer_match_v2(
    neutral_side: NeutralOrderSide,
    v2_side: V2OrderSide,
):
    profile = FeeProfile(
        version="v2-standard-fee-v1",
        buy_commission_rate=Decimal("0.0001"),
        sell_commission_rate=Decimal("0.0001"),
        minimum_commission=Decimal("5"),
        stamp_tax_sell_rate=Decimal("0.0005"),
        transfer_fee_buy_rate=Decimal("0.00001"),
        transfer_fee_sell_rate=Decimal("0.00001"),
    )
    schedule = v2_fee_profile_to_neutral_schedule(profile)

    small = calculate_order_fees(
        side=neutral_side,
        schedule=schedule,
        notional=Decimal("10000"),
    )
    large = calculate_order_fees(
        side=neutral_side,
        schedule=schedule,
        notional=Decimal("100000"),
    )
    assert small.total == profile.calculate(v2_side, Decimal("10000"))
    assert large.total == profile.calculate(v2_side, Decimal("100000"))
    assert small.commission == Decimal("5.00")
    assert small.transfer_fee == Decimal("0.10")
    assert small.stamp_duty == (
        Decimal("5.00")
        if neutral_side == NeutralOrderSide.SELL
        else Decimal("0.00")
    )


def test_nonzero_other_fees_are_charged_and_quantity_is_required():
    profile = _v2_profile()
    schedule = v2_fee_profile_to_neutral_schedule(profile)
    without_other = v2_fee_profile_to_neutral_schedule(
        replace(
            profile,
            other_buy_rate=Decimal("0"),
            other_sell_rate=Decimal("0"),
            other_buy_fixed=Decimal("0"),
            other_sell_fixed=Decimal("0"),
            other_buy_per_share=Decimal("0"),
            other_sell_per_share=Decimal("0"),
        )
    )

    with_other = calculate_order_fees(
        side=NeutralOrderSide.BUY,
        schedule=schedule,
        notional=Decimal("12345.67"),
        quantity=1200,
    )
    baseline = calculate_order_fees(
        side=NeutralOrderSide.BUY,
        schedule=without_other,
        notional=Decimal("12345.67"),
    )
    assert with_other.total > baseline.total
    assert with_other.total == profile.calculate(
        V2OrderSide.BUY,
        Decimal("12345.67"),
        quantity=1200,
    )
    with pytest.raises(ValueError, match="quantity is required"):
        calculate_order_fees(
            side=NeutralOrderSide.BUY,
            schedule=schedule,
            notional=Decimal("12345.67"),
        )


@pytest.mark.parametrize(
    (
        "neutral_side",
        "v2_side",
        "first_price",
        "first_quantity",
        "second_price",
        "second_quantity",
    ),
    (
        (
            NeutralOrderSide.BUY,
            V2OrderSide.BUY,
            Decimal("0.985"),
            100,
            Decimal("1.237"),
            200,
        ),
        (
            NeutralOrderSide.SELL,
            V2OrderSide.SELL,
            Decimal("0.985"),
            7,
            Decimal("1.237"),
            13,
        ),
    ),
)
def test_two_partial_fills_match_v2_using_cumulative_notional_and_quantity(
    neutral_side: NeutralOrderSide,
    v2_side: V2OrderSide,
    first_price: Decimal,
    first_quantity: int,
    second_price: Decimal,
    second_quantity: int,
):
    profile = _v2_profile()
    schedule = v2_fee_profile_to_neutral_schedule(profile)
    first_gross = v2_fill_gross(
        price=first_price,
        quantity=first_quantity,
    )
    second_gross = v2_fill_gross(
        price=second_price,
        quantity=second_quantity,
    )
    total_gross = first_gross + second_gross
    total_quantity = first_quantity + second_quantity

    expected_first = profile.calculate_incremental(
        v2_side,
        previous_gross=Decimal("0"),
        fill_gross=first_gross,
        previous_quantity=0,
        fill_quantity=first_quantity,
    )
    expected_second = profile.calculate_incremental(
        v2_side,
        previous_gross=first_gross,
        fill_gross=second_gross,
        previous_quantity=first_quantity,
        fill_quantity=second_quantity,
    )
    actual_first = incremental_order_fee_delta(
        side=neutral_side,
        schedule=schedule,
        previous_notional=Decimal("0"),
        new_total_notional=first_gross,
        previous_quantity=0,
        new_total_quantity=first_quantity,
    )
    actual_second = incremental_order_fee_delta(
        side=neutral_side,
        schedule=schedule,
        previous_notional=first_gross,
        new_total_notional=total_gross,
        previous_quantity=first_quantity,
        new_total_quantity=total_quantity,
    )

    assert actual_first.total == expected_first
    assert actual_second.total == expected_second
    assert actual_first.total + actual_second.total == profile.calculate(
        v2_side,
        total_gross,
        quantity=total_quantity,
    )
    assert actual_first.total + actual_second.total == calculate_order_fees(
        side=neutral_side,
        schedule=schedule,
        notional=total_gross,
        quantity=total_quantity,
    ).total

    with pytest.raises(ValueError, match="quantity is required"):
        incremental_order_fee_delta(
            side=neutral_side,
            schedule=schedule,
            previous_notional=Decimal("0"),
            new_total_notional=first_gross,
        )
