from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.integrations.v2_execution_adapter import (
    apply_v2_compatible_fill,
    build_v2_accounting_fill_request,
    empty_v2_accounting_state,
)
from server.trading_core.accounting import (
    AccountingFillRequest,
    AccountingIdempotencyConflict,
    AccountingInvariantError,
    AccountingState,
    InsufficientSellableQuantityError,
)
from server.trading_core.contracts import OrderSide as NeutralSide
from server.trading_v2.domain import InstrumentRule, OrderSide as V2Side
from server.trading_v2.ledger import FeeProfile, LedgerBook


UTC = timezone.utc
DAY_1 = date(2026, 8, 3)
DAY_2 = date(2026, 8, 4)
DAY_3 = date(2026, 8, 5)
TRADING_DAYS = (DAY_1, DAY_2, DAY_3)
NOW = datetime(2026, 8, 3, 2, 0, tzinfo=UTC)


def _profile(**overrides: object) -> FeeProfile:
    values: dict[str, object] = {
        "version": "v2-accounting-golden-v1",
        "buy_commission_rate": Decimal("0.00027"),
        "sell_commission_rate": Decimal("0.00031"),
        "minimum_commission": Decimal("5"),
        "stamp_tax_sell_rate": Decimal("0.0005"),
        "transfer_fee_buy_rate": Decimal("0.00001"),
        "transfer_fee_sell_rate": Decimal("0.00002"),
        "other_buy_rate": Decimal("0.000013"),
        "other_sell_rate": Decimal("0.000017"),
        "other_buy_fixed": Decimal("0.37"),
        "other_sell_fixed": Decimal("0.41"),
        "other_buy_per_share": Decimal("0.0007"),
        "other_sell_per_share": Decimal("0.0009"),
    }
    values.update(overrides)
    return FeeProfile(**values)  # type: ignore[arg-type]


def _rule(**overrides: object) -> InstrumentRule:
    values: dict[str, object] = {
        "stock_code": "600001.SH",
        "rule_version": "instrument-rule-v1",
        "security_type": "A_SHARE",
        "exchange": "SH",
        "effective_from": date(2026, 1, 1),
        "effective_to": None,
        "can_buy": True,
        "first_buy_minimum": 100,
        "buy_lot_size": 100,
        "sell_lot_size": 100,
        "settlement_days": 1,
        "tick_size": Decimal("0.01"),
        "limit_ratio": Decimal("0.10"),
        "fee_profile_version": _profile().version,
    }
    values.update(overrides)
    return InstrumentRule(**values)  # type: ignore[arg-type]


def _request(
    *,
    fill_id: str,
    order_id: str,
    side: NeutralSide,
    quantity: int,
    price: str,
    trade_date: date,
    filled_at: datetime,
    profile: FeeProfile | None = None,
    instrument_rule: InstrumentRule | None = None,
    trading_days: tuple[date, ...] = TRADING_DAYS,
) -> AccountingFillRequest:
    selected_profile = profile or _profile()
    return build_v2_accounting_fill_request(
        fill_id=fill_id,
        idempotency_key=f"key:{fill_id}",
        order_id=order_id,
        instrument_id="600001.SH",
        side=side,
        quantity=quantity,
        price=Decimal(price),
        trade_date=trade_date,
        filled_at=filled_at,
        fee_profile=selected_profile,
        trading_days=trading_days,
        calendar_version="calendar-v1",
        instrument_rule=instrument_rule or _rule(),
    )


def _apply_both(
    neutral: AccountingState,
    legacy: LedgerBook,
    request: AccountingFillRequest,
) -> AccountingState:
    profile = _profile()
    result = apply_v2_compatible_fill(
        neutral,
        request,
        fee_profile=profile,
    )
    v2_fill = legacy.apply_fill(
        fill_id=request.fill_id,
        idempotency_key=request.idempotency_key,
        order_id=request.order_id,
        stock_code=request.instrument_id,
        side=(V2Side.BUY if request.side == NeutralSide.BUY else V2Side.SELL),
        quantity=request.quantity,
        price=request.price,
        trade_date=request.trade_date,
        filled_at=request.filled_at,
        fee_profile=profile,
        settlement_date=request.settlement_date,
    )

    assert result.fill.gross_amount == v2_fill.gross_amount
    assert result.fill.fee_amount == v2_fill.fee_amount
    assert result.fill.net_cash_amount == v2_fill.net_cash_amount
    assert result.state.cash_balance == legacy.cash_balance
    assert result.state.position_quantity(request.instrument_id) == (
        legacy.position_quantity(request.instrument_id)
    )
    assert result.state.available_to_sell(request.instrument_id, request.trade_date) == (
        legacy.available_to_sell(request.instrument_id, request.trade_date)
    )
    assert result.state.reconcile()["status"] == legacy.reconcile()["status"] == "PASS"
    neutral_lots = [
        (
            lot.lot_id,
            lot.remaining_quantity,
            lot.cost_price,
            lot.allocated_buy_fee,
            lot.sellable_on,
        )
        for lot in result.state.lots
    ]
    legacy_lots = [
        (
            lot.lot_id,
            lot.remaining_quantity,
            lot.cost_price,
            lot.allocated_buy_fee,
            lot.settlement_date,
        )
        for lot in legacy.lots
    ]
    assert neutral_lots == legacy_lots
    return result.state


def test_partial_buy_and_t1_sell_are_cent_and_lot_exact_to_v2():
    neutral = empty_v2_accounting_state(
        Decimal("100000"), fee_profile=_profile()
    )
    legacy = LedgerBook(initial_cash=Decimal("100000"))
    neutral = _apply_both(
        neutral,
        legacy,
        _request(
            fill_id="buy-fill-1",
            order_id="buy-order",
            side=NeutralSide.BUY,
            quantity=100,
            price="0.985",
            trade_date=DAY_1,
            filled_at=NOW,
        ),
    )
    neutral = _apply_both(
        neutral,
        legacy,
        _request(
            fill_id="buy-fill-2",
            order_id="buy-order",
            side=NeutralSide.BUY,
            quantity=200,
            price="1.237",
            trade_date=DAY_1,
            filled_at=NOW + timedelta(seconds=1),
        ),
    )
    assert neutral.available_to_sell("600001.SH", DAY_1) == 0

    neutral = _apply_both(
        neutral,
        legacy,
        _request(
            fill_id="sell-fill-1",
            order_id="sell-order",
            side=NeutralSide.SELL,
            quantity=150,
            price="1.500",
            trade_date=DAY_2,
            filled_at=NOW + timedelta(days=1),
        ),
    )
    assert neutral.position_quantity("600001.SH") == 150


def test_fractional_cent_fill_gross_is_rounded_like_v2_before_fees():
    neutral = empty_v2_accounting_state(
        Decimal("10000"), fee_profile=_profile()
    )
    legacy = LedgerBook(initial_cash=Decimal("10000"))
    neutral = _apply_both(
        neutral,
        legacy,
        _request(
            fill_id="buy-odd-fill",
            order_id="buy-odd-order",
            side=NeutralSide.BUY,
            quantity=7,
            price="0.985",
            trade_date=DAY_1,
            filled_at=NOW,
        ),
    )

    assert neutral.fills[0].gross_amount == Decimal("6.89")


def test_fill_retry_is_idempotent_and_payload_change_is_a_conflict():
    state = empty_v2_accounting_state(
        Decimal("10000"), fee_profile=_profile()
    )
    request = _request(
        fill_id="buy-fill-1",
        order_id="buy-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW,
    )
    first = apply_v2_compatible_fill(state, request, fee_profile=_profile())
    retry = apply_v2_compatible_fill(
        first.state,
        request,
        fee_profile=_profile(),
    )

    assert retry.idempotent is True
    assert retry.state is first.state
    with pytest.raises(AccountingIdempotencyConflict):
        apply_v2_compatible_fill(
            first.state,
            _request(
                fill_id=request.fill_id,
                order_id=request.order_id,
                side=request.side,
                quantity=200,
                price=str(request.price),
                trade_date=request.trade_date,
                filled_at=request.filled_at,
            ),
            fee_profile=_profile(),
        )


def test_decimal_request_hash_does_not_collapse_beyond_context_precision():
    first_request = _request(
        fill_id="precise-fill",
        order_id="precise-order",
        side=NeutralSide.BUY,
        quantity=1,
        price="1.12345678901234567890123456781",
        trade_date=DAY_1,
        filled_at=NOW,
    )
    conflicting_request = _request(
        fill_id="precise-fill",
        order_id="precise-order",
        side=NeutralSide.BUY,
        quantity=1,
        price="1.12345678901234567890123456782",
        trade_date=DAY_1,
        filled_at=NOW,
    )
    assert first_request.request_hash != conflicting_request.request_hash

    first = apply_v2_compatible_fill(
        empty_v2_accounting_state(
            Decimal("10000"), fee_profile=_profile()
        ),
        first_request,
        fee_profile=_profile(),
    )
    with pytest.raises(AccountingIdempotencyConflict):
        apply_v2_compatible_fill(
            first.state,
            conflicting_request,
            fee_profile=_profile(),
        )


def test_t1_sell_is_blocked_before_settlement():
    bought = apply_v2_compatible_fill(
        empty_v2_accounting_state(
            Decimal("10000"), fee_profile=_profile()
        ),
        _request(
            fill_id="buy-fill-1",
            order_id="buy-order",
            side=NeutralSide.BUY,
            quantity=100,
            price="10",
            trade_date=DAY_1,
            filled_at=NOW,
        ),
        fee_profile=_profile(),
    ).state

    with pytest.raises(InsufficientSellableQuantityError):
        apply_v2_compatible_fill(
            bought,
            _request(
                fill_id="sell-fill-1",
                order_id="sell-order",
                side=NeutralSide.SELL,
                quantity=100,
                price="10",
                trade_date=DAY_1,
                filled_at=NOW + timedelta(seconds=1),
            ),
            fee_profile=_profile(),
        )


def test_calendar_evidence_skips_non_trading_days():
    friday = date(2026, 8, 7)
    monday = date(2026, 8, 10)
    request = _request(
        fill_id="friday-buy",
        order_id="friday-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=friday,
        filled_at=datetime(2026, 8, 7, 2, 0, tzinfo=UTC),
        trading_days=(friday, monday),
    )

    assert request.settlement_date == monday
    assert request.settlement_evidence is not None
    assert request.settlement_evidence.calendar_hash


def test_same_version_changed_fee_parameters_are_rejected():
    original = _profile()
    changed = _profile(minimum_commission=Decimal("0"))
    state = empty_v2_accounting_state(Decimal("10000"), fee_profile=original)
    original_request = _request(
        fill_id="original-fee-fill",
        order_id="original-fee-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW,
        profile=original,
    )
    state = apply_v2_compatible_fill(
        state,
        original_request,
        fee_profile=original,
    ).state
    request = _request(
        fill_id="changed-fee-fill",
        order_id="changed-fee-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW + timedelta(seconds=1),
        profile=changed,
    )

    with pytest.raises(ValueError, match="different parameters"):
        apply_v2_compatible_fill(state, request, fee_profile=changed)
    assert len(state.fee_schedules) == 1
    assert state.requests == (original_request,)


def test_different_orders_may_use_a_new_fee_profile_version():
    profile_v1 = _profile(
        version="fees-v1",
        minimum_commission=Decimal("0"),
    )
    profile_v2 = _profile(
        version="fees-v2",
        minimum_commission=Decimal("5"),
    )
    state = empty_v2_accounting_state(
        Decimal("10000"),
        fee_profile=profile_v1,
    )
    legacy = LedgerBook(initial_cash=Decimal("10000"))
    first_request = _request(
        fill_id="v1-fill",
        order_id="v1-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW,
        profile=profile_v1,
        instrument_rule=_rule(fee_profile_version="fees-v1"),
    )
    first = apply_v2_compatible_fill(
        state,
        first_request,
        fee_profile=profile_v1,
    )
    first_v2 = legacy.apply_fill(
        fill_id=first_request.fill_id,
        idempotency_key=first_request.idempotency_key,
        order_id=first_request.order_id,
        stock_code=first_request.instrument_id,
        side=V2Side.BUY,
        quantity=first_request.quantity,
        price=first_request.price,
        trade_date=first_request.trade_date,
        filled_at=first_request.filled_at,
        fee_profile=profile_v1,
        settlement_date=first_request.settlement_date,
    )
    state = first.state
    second_request = _request(
        fill_id="v2-fill",
        order_id="v2-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW + timedelta(seconds=1),
        profile=profile_v2,
        instrument_rule=_rule(fee_profile_version="fees-v2"),
    )
    second = apply_v2_compatible_fill(
        state,
        second_request,
        fee_profile=profile_v2,
    )
    second_v2 = legacy.apply_fill(
        fill_id=second_request.fill_id,
        idempotency_key=second_request.idempotency_key,
        order_id=second_request.order_id,
        stock_code=second_request.instrument_id,
        side=V2Side.BUY,
        quantity=second_request.quantity,
        price=second_request.price,
        trade_date=second_request.trade_date,
        filled_at=second_request.filled_at,
        fee_profile=profile_v2,
        settlement_date=second_request.settlement_date,
    )

    assert first.fill.fee_amount == first_v2.fee_amount
    assert second.fill.gross_amount == second_v2.gross_amount
    assert second.fill.fee_amount == second_v2.fee_amount
    assert second.fill.net_cash_amount == second_v2.net_cash_amount
    assert second.state.cash_balance == legacy.cash_balance
    assert len(second.state.fee_schedules) == 2
    assert tuple(
        schedule.profile_version for schedule in second.state.fee_schedules
    ) == ("fees-v1", "fees-v2")
    assert second.state.reconcile()["status"] == "PASS"


def test_partial_fills_of_one_order_cannot_change_fee_profile_version():
    profile_v1 = _profile(version="partial-fees-v1")
    profile_v2 = _profile(
        version="partial-fees-v2",
        minimum_commission=Decimal("0"),
    )
    first_request = _request(
        fill_id="partial-v1-fill",
        order_id="one-partial-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW,
        profile=profile_v1,
        instrument_rule=_rule(fee_profile_version=profile_v1.version),
    )
    state = apply_v2_compatible_fill(
        empty_v2_accounting_state(
            Decimal("10000"),
            fee_profile=profile_v1,
        ),
        first_request,
        fee_profile=profile_v1,
    ).state
    second_request = _request(
        fill_id="partial-v2-fill",
        order_id=first_request.order_id,
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW + timedelta(seconds=1),
        profile=profile_v2,
        instrument_rule=_rule(fee_profile_version=profile_v2.version),
    )

    with pytest.raises(AccountingInvariantError, match="partial fills"):
        apply_v2_compatible_fill(
            state,
            second_request,
            fee_profile=profile_v2,
        )

    assert len(state.requests) == 1
    assert len(state.fee_schedules) == 1


def test_derived_state_is_replayed_and_tampering_cannot_report_pass():
    first = apply_v2_compatible_fill(
        empty_v2_accounting_state(
            Decimal("10000"), fee_profile=_profile()
        ),
        _request(
            fill_id="buy-fill-1",
            order_id="buy-order",
            side=NeutralSide.BUY,
            quantity=100,
            price="10",
            trade_date=DAY_1,
            filled_at=NOW,
        ),
        fee_profile=_profile(),
    ).state
    fill = replace(
        first.fills[0],
        fee_amount=Decimal("0.00"),
        net_cash_amount=-first.fills[0].gross_amount,
    )
    movement = replace(
        first.cash_movements[0],
        amount=fill.net_cash_amount,
        balance_after=first.opening_cash + fill.net_cash_amount,
    )
    lot = replace(first.lots[0], allocated_buy_fee=Decimal("0.00"))

    with pytest.raises(AccountingInvariantError, match="canonical request replay"):
        replace(
            first,
            cash_balance=movement.balance_after,
            fills=(fill,),
            lots=(lot,),
            cash_movements=(movement,),
        )


def test_low_level_frozen_object_bypass_is_revalidated_before_retry_or_reconcile():
    request = _request(
        fill_id="frozen-bypass-fill",
        order_id="frozen-bypass-order",
        side=NeutralSide.BUY,
        quantity=100,
        price="10",
        trade_date=DAY_1,
        filled_at=NOW,
    )
    state = apply_v2_compatible_fill(
        empty_v2_accounting_state(
            Decimal("10000"),
            fee_profile=_profile(),
        ),
        request,
        fee_profile=_profile(),
    ).state

    forged_state = replace(state)
    object.__setattr__(forged_state, "cash_balance", Decimal("9999.99"))
    with pytest.raises(AccountingInvariantError):
        forged_state.reconcile()
    with pytest.raises(AccountingInvariantError):
        apply_v2_compatible_fill(
            forged_state,
            request,
            fee_profile=_profile(),
        )

    forged_request = replace(request)
    object.__setattr__(forged_request, "quantity", 200)
    with pytest.raises(AccountingInvariantError):
        apply_v2_compatible_fill(
            state,
            forged_request,
            fee_profile=_profile(),
        )
