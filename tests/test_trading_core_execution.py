from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.trading_core.contracts import (
    ExecutionIntent,
    ExecutionResult,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionLot,
    TimeInForce,
    execution_intent_idempotency_key,
    execution_result_idempotency_key,
)
from server.trading_core.execution import (
    ACTIVE_TRANSITIONS,
    TERMINAL_STATUSES,
    OrderState,
    apply_execution_result,
    can_transition,
    new_order_state,
    transition_order,
)
from server.trading_core.market_rules import (
    FeeSchedule,
    InstrumentRule,
    PriceBand,
    RuleViolation,
    calculate_order_fees,
    calculate_price_band,
    cash_effect,
    earliest_sell_date,
    floor_buy_quantity,
    incremental_order_fee_delta,
    is_lot_sellable,
    locked_quantity,
    sellable_quantity,
    validate_intent_against_rule,
)


UTC = timezone.utc
BASE_TIME = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)
TRADE_DATE = date(2026, 8, 3)
EVALUATED_AT = BASE_TIME + timedelta(seconds=1)
DAILY_BAND_MAX_AGE = timedelta(days=4)
DYNAMIC_CAGE_MAX_AGE = timedelta(seconds=5)


def _intent(
    *,
    side: OrderSide = OrderSide.BUY,
    quantity: int = 1_000,
    limit_price: Decimal = Decimal("10.00"),
    instrument_id: str = "600000.SH",
    intent_version: int = 1,
    earliest_at: datetime = BASE_TIME,
    expires_at: datetime = BASE_TIME + timedelta(hours=6),
) -> ExecutionIntent:
    key = execution_intent_idempotency_key(
        account_id="paper-1",
        decision_id="decision-1",
        instrument_id=instrument_id,
        side=side,
        quantity=quantity,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        earliest_at=earliest_at,
        expires_at=expires_at,
        limit_price=limit_price,
        rule_version="cn-a-rule-v1",
        fee_profile_version="fee-v1",
        execution_policy_version="execution-v1",
        intent_version=intent_version,
    )
    return ExecutionIntent(
        intent_id="intent-1",
        account_id="paper-1",
        decision_id="decision-1",
        instrument_id=instrument_id,
        side=side,
        quantity=quantity,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        created_at=earliest_at - timedelta(minutes=1),
        earliest_at=earliest_at,
        expires_at=expires_at,
        limit_price=limit_price,
        idempotency_key=key,
        rule_version="cn-a-rule-v1",
        fee_profile_version="fee-v1",
        execution_policy_version="execution-v1",
        intent_version=intent_version,
    )


def _result(
    *,
    event_id: str,
    status: OrderStatus,
    seconds: int,
    quantity: int = 0,
    price: Decimal | None = None,
    reason_code: str = "",
    source_sequence: int | None = None,
) -> ExecutionResult:
    occurred_at = BASE_TIME + timedelta(seconds=seconds)
    return ExecutionResult(
        intent_id="intent-1",
        order_id="order-1",
        event_id=event_id,
        status=status,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(milliseconds=1),
        source_sequence=source_sequence or max(1, seconds),
        idempotency_key=execution_result_idempotency_key(
            order_id="order-1",
            event_id=event_id,
        ),
        last_fill_quantity=quantity,
        last_fill_price=price,
        reason_code=reason_code,
    )


def _a_share_rule() -> InstrumentRule:
    return InstrumentRule(
        instrument_id="600000.SH",
        rule_version="cn-a-rule-v1",
        fee_profile_version="fee-v1",
        effective_from=date(2026, 1, 1),
        effective_to=None,
        buy_lot_size=100,
        minimum_buy_quantity=100,
        sell_lot_size=100,
        settlement_days=1,
        tick_size=Decimal("0.01"),
        price_limit_ratio=Decimal("0.10"),
        maximum_order_quantity=1_000_000,
        allow_odd_lot_liquidation=True,
        requires_dynamic_price_cage=True,
    )


def _a_share_fees() -> FeeSchedule:
    return FeeSchedule(
        profile_version="fee-v1",
        buy_commission_rate=Decimal("0.0001"),
        sell_commission_rate=Decimal("0.0001"),
        minimum_commission=Decimal("5.00"),
        stamp_duty_sell_rate=Decimal("0.0005"),
        transfer_fee_buy_rate=Decimal("0.00001"),
        transfer_fee_sell_rate=Decimal("0.00001"),
    )


def _price_band(
    lower: Decimal | str,
    upper: Decimal | str,
    *,
    instrument_id: str = "600000.SH",
    trade_date: date = TRADE_DATE,
    as_of: datetime = BASE_TIME,
    source: str = "test-authoritative-feed",
) -> PriceBand:
    return PriceBand(
        instrument_id=instrument_id,
        trade_date=trade_date,
        as_of=as_of,
        source=source,
        lower=Decimal(lower),
        upper=Decimal(upper),
    )


def _market_facts() -> dict[str, object]:
    return {
        "evaluated_at": EVALUATED_AT,
        "authoritative_price_band": _price_band("9", "11"),
        "authoritative_price_band_max_age": DAILY_BAND_MAX_AGE,
        "dynamic_price_cage": _price_band("9", "11"),
        "dynamic_price_cage_max_age": DYNAMIC_CAGE_MAX_AGE,
    }


def test_intent_idempotency_is_canonical_across_decimal_and_timezone_forms():
    shanghai = timezone(timedelta(hours=8))
    utc_key = _intent(
        limit_price=Decimal("10.0"),
        earliest_at=BASE_TIME,
        expires_at=BASE_TIME + timedelta(hours=6),
    ).idempotency_key
    china_key = _intent(
        limit_price=Decimal("10.00"),
        earliest_at=BASE_TIME.astimezone(shanghai),
        expires_at=(BASE_TIME + timedelta(hours=6)).astimezone(shanghai),
    ).idempotency_key
    assert utc_key == china_key
    assert len(utc_key) == 64


def test_intent_idempotency_changes_with_business_semantics():
    keys = {
        _intent(quantity=quantity, intent_version=version).idempotency_key
        for quantity in range(100, 1_001, 100)
        for version in (1, 2)
    }
    assert len(keys) == 20


def test_execution_quantities_and_identifiers_are_strictly_typed():
    with pytest.raises(TypeError, match="quantity must be an integer"):
        execution_intent_idempotency_key(
            account_id="paper-1",
            decision_id="decision-1",
            instrument_id="600000.SH",
            side=OrderSide.BUY,
            quantity=100.9,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.DAY,
            earliest_at=BASE_TIME,
            expires_at=BASE_TIME + timedelta(hours=1),
            limit_price=Decimal("10"),
            rule_version="rule-v1",
            fee_profile_version="fee-v1",
            execution_policy_version="execution-v1",
        )
    with pytest.raises(TypeError, match="intent_id must be a string"):
        original = _intent()
        ExecutionIntent(
            intent_id=None,
            account_id=original.account_id,
            decision_id=original.decision_id,
            instrument_id=original.instrument_id,
            side=original.side,
            quantity=original.quantity,
            order_type=original.order_type,
            time_in_force=original.time_in_force,
            created_at=original.created_at,
            earliest_at=original.earliest_at,
            expires_at=original.expires_at,
            limit_price=original.limit_price,
            idempotency_key=original.idempotency_key,
            rule_version=original.rule_version,
            fee_profile_version=original.fee_profile_version,
            execution_policy_version=original.execution_policy_version,
        )


def test_execution_contract_rejects_naive_time_and_invalid_limit_shape():
    with pytest.raises(ValueError, match="timezone-aware"):
        ExecutionIntent(
            intent_id="i",
            account_id="a",
            decision_id="d",
            instrument_id="600000.SH",
            side=OrderSide.BUY,
            quantity=100,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.DAY,
            created_at=datetime(2026, 8, 3, 9, 29),
            earliest_at=datetime(2026, 8, 3, 9, 30),
            expires_at=datetime(2026, 8, 3, 15, 0),
            limit_price=Decimal("10"),
            idempotency_key="x",
            rule_version="r",
            fee_profile_version="f",
            execution_policy_version="e",
        )
    with pytest.raises(ValueError, match="positive limit_price"):
        _intent(limit_price=Decimal("0"))


def test_execution_contract_coerces_wire_enums_and_rejects_empty_fill_events():
    original = _intent()
    restored = ExecutionIntent(
        intent_id=original.intent_id,
        account_id=original.account_id,
        decision_id=original.decision_id,
        instrument_id=original.instrument_id,
        side="BUY",
        quantity=original.quantity,
        order_type="LIMIT",
        time_in_force="DAY",
        created_at=original.created_at,
        earliest_at=original.earliest_at,
        expires_at=original.expires_at,
        limit_price="10.00",
        idempotency_key=original.idempotency_key,
        rule_version=original.rule_version,
        fee_profile_version=original.fee_profile_version,
        execution_policy_version=original.execution_policy_version,
    )
    assert restored.side is OrderSide.BUY
    assert restored.order_type is OrderType.LIMIT
    assert restored.time_in_force is TimeInForce.DAY
    assert restored.limit_price == Decimal("10.00")

    with pytest.raises(ValueError, match="positive fill delta"):
        _result(
            event_id="empty-fill",
            status=OrderStatus.PARTIALLY_FILLED,
            seconds=1,
        )


def test_order_state_rejects_impossible_restored_state():
    with pytest.raises(ValueError, match="FILLED state"):
        OrderState(
            order_id="order-1",
            intent_id="intent-1",
            status=OrderStatus.FILLED,
            requested_quantity=100,
            cumulative_filled_quantity=50,
            average_fill_price=Decimal("10"),
            created_at=BASE_TIME,
            updated_at=BASE_TIME,
            earliest_at=BASE_TIME,
            expires_at=BASE_TIME + timedelta(hours=1),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.DAY,
            last_source_sequence=1,
            version=2,
        )


def test_oms_golden_lifecycle_is_retry_safe_and_computes_weighted_average():
    state = new_order_state(order_id="order-1", intent=_intent())
    accepted = _result(event_id="accepted", status=OrderStatus.ACCEPTED, seconds=1)
    queued = _result(event_id="queued", status=OrderStatus.QUEUED, seconds=2)
    fill_1 = _result(
        event_id="fill-1",
        status=OrderStatus.PARTIALLY_FILLED,
        seconds=3,
        quantity=300,
        price=Decimal("10"),
    )
    fill_2 = _result(
        event_id="fill-2",
        status=OrderStatus.PARTIALLY_FILLED,
        seconds=4,
        quantity=200,
        price=Decimal("11"),
    )
    fill_3 = _result(
        event_id="fill-3",
        status=OrderStatus.FILLED,
        seconds=5,
        quantity=500,
        price=Decimal("9.60"),
    )
    for event in (accepted, queued, fill_1, fill_2, fill_3):
        state = apply_execution_result(state, event)

    assert state.status == OrderStatus.FILLED
    assert state.cumulative_filled_quantity == 1_000
    assert state.remaining_quantity == 0
    assert state.average_fill_price == Decimal("10")
    assert state.version == 6

    retried = apply_execution_result(state, fill_3)
    assert retried is state


def test_oms_redelivery_ignores_local_receive_time_but_detects_business_conflict():
    state = new_order_state(order_id="order-1", intent=_intent())
    accepted = _result(
        event_id="accepted-redelivery",
        status=OrderStatus.ACCEPTED,
        seconds=1,
    )
    applied = apply_execution_result(state, accepted)

    redelivered = replace(
        accepted,
        received_at=accepted.received_at + timedelta(seconds=30),
    )
    assert apply_execution_result(applied, redelivered) is applied

    conflicting = replace(redelivered, reason_code="CHANGED_BUSINESS_PAYLOAD")
    with pytest.raises(ValueError, match="different result payload"):
        apply_execution_result(applied, conflicting)


def test_oms_replays_history_before_trusting_restored_or_retry_state():
    accepted_event = _result(
        event_id="accepted-state-validation",
        status=OrderStatus.ACCEPTED,
        seconds=1,
    )
    accepted = apply_execution_result(
        new_order_state(order_id="order-1", intent=_intent()),
        accepted_event,
    )

    with pytest.raises(ValueError, match="differs from execution history"):
        replace(
            accepted,
            updated_at=accepted.updated_at + timedelta(microseconds=1),
        )

    # Frozen dataclasses are not a security boundary: low-level Python code can
    # still mutate an exact instance.  The idempotent retry shortcut must not
    # return such a forged state before replaying its complete history.
    forged = replace(accepted)
    object.__setattr__(forged, "cumulative_filled_quantity", 100)
    object.__setattr__(forged, "average_fill_price", Decimal("10"))
    with pytest.raises(ValueError):
        apply_execution_result(forged, accepted_event)


def test_oms_rejects_key_conflict_overfill_out_of_order_and_terminal_transition():
    state = new_order_state(order_id="order-1", intent=_intent())
    state = apply_execution_result(
        state,
        _result(event_id="accepted", status=OrderStatus.ACCEPTED, seconds=1),
    )
    event = _result(
        event_id="cancel",
        status=OrderStatus.CANCELLED,
        seconds=2,
        reason_code="USER_CANCEL",
    )
    cancelled = apply_execution_result(state, event)
    conflicting_retry = _result(
        event_id="cancel",
        status=OrderStatus.CANCELLED,
        seconds=2,
        reason_code="VENUE_CANCEL",
    )
    with pytest.raises(ValueError, match="different result payload"):
        apply_execution_result(cancelled, conflicting_retry)
    with pytest.raises(ValueError, match="illegal order transition"):
        apply_execution_result(
            cancelled,
            _result(event_id="late", status=OrderStatus.QUEUED, seconds=3),
        )


def test_oms_enforces_execution_window_sequence_and_initial_release_scope():
    intent = _intent()
    state = new_order_state(order_id="order-1", intent=intent)
    with pytest.raises(ValueError, match="execution window"):
        apply_execution_result(
            state,
            ExecutionResult(
                intent_id="intent-1",
                order_id="order-1",
                event_id="too-early",
                status=OrderStatus.ACCEPTED,
                occurred_at=intent.earliest_at - timedelta(microseconds=1),
                received_at=intent.earliest_at,
                source_sequence=1,
                idempotency_key=execution_result_idempotency_key(
                    order_id="order-1",
                    event_id="too-early",
                ),
            ),
        )

    accepted = apply_execution_result(
        state,
        _result(
            event_id="accepted-window",
            status=OrderStatus.ACCEPTED,
            seconds=1,
            source_sequence=1,
        ),
    )
    with pytest.raises(ValueError, match="source_sequence"):
        apply_execution_result(
            accepted,
            _result(
                event_id="stale-sequence",
                status=OrderStatus.QUEUED,
                seconds=2,
                source_sequence=1,
            ),
        )

    unsupported_key = execution_intent_idempotency_key(
        account_id=intent.account_id,
        decision_id=intent.decision_id,
        instrument_id=intent.instrument_id,
        side=intent.side,
        quantity=intent.quantity,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.IOC,
        earliest_at=intent.earliest_at,
        expires_at=intent.expires_at,
        limit_price=None,
        rule_version=intent.rule_version,
        fee_profile_version=intent.fee_profile_version,
        execution_policy_version=intent.execution_policy_version,
    )
    unsupported = ExecutionIntent(
        intent_id="unsupported-intent",
        account_id=intent.account_id,
        decision_id=intent.decision_id,
        instrument_id=intent.instrument_id,
        side=intent.side,
        quantity=intent.quantity,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.IOC,
        created_at=intent.created_at,
        earliest_at=intent.earliest_at,
        expires_at=intent.expires_at,
        idempotency_key=unsupported_key,
        rule_version=intent.rule_version,
        fee_profile_version=intent.fee_profile_version,
        execution_policy_version=intent.execution_policy_version,
    )
    with pytest.raises(ValueError, match=r"LIMIT \+ DAY"):
        new_order_state(order_id="order-unsupported", intent=unsupported)

    live = apply_execution_result(
        new_order_state(order_id="order-1", intent=_intent()),
        _result(event_id="accepted-2", status=OrderStatus.ACCEPTED, seconds=1),
    )
    with pytest.raises(ValueError, match="exceeds requested"):
        apply_execution_result(
            live,
            _result(
                event_id="overfill",
                status=OrderStatus.FILLED,
                seconds=2,
                quantity=1_100,
                price=Decimal("10"),
            ),
        )


def test_oms_sequence_gap_is_rejected_before_a_fill_can_be_lost():
    state = new_order_state(order_id="order-1", intent=_intent())
    accepted = apply_execution_result(
        state,
        _result(
            event_id="gap-accepted",
            status=OrderStatus.ACCEPTED,
            seconds=1,
            source_sequence=1,
        ),
    )

    with pytest.raises(ValueError, match=r"expected 2, got 3"):
        apply_execution_result(
            accepted,
            _result(
                event_id="gap-cancelled",
                status=OrderStatus.CANCELLED,
                seconds=3,
                source_sequence=3,
            ),
        )

    partially_filled = apply_execution_result(
        accepted,
        _result(
            event_id="gap-fill",
            status=OrderStatus.PARTIALLY_FILLED,
            seconds=2,
            source_sequence=2,
            quantity=40,
            price=Decimal("10"),
        ),
    )
    cancelled = apply_execution_result(
        partially_filled,
        _result(
            event_id="gap-cancelled",
            status=OrderStatus.CANCELLED,
            seconds=3,
            source_sequence=3,
        ),
    )
    assert cancelled.status == OrderStatus.CANCELLED
    assert cancelled.cumulative_filled_quantity == 40


def test_oms_allows_late_terminal_ack_but_not_late_fill_or_time_regression():
    intent = _intent()
    accepted = apply_execution_result(
        new_order_state(order_id="order-1", intent=intent),
        _result(
            event_id="late-terminal-accepted",
            status=OrderStatus.ACCEPTED,
            seconds=1,
            source_sequence=1,
        ),
    )
    after_expiry = int((intent.expires_at - BASE_TIME).total_seconds()) + 1
    cancelled_event = _result(
        event_id="late-terminal-cancelled",
        status=OrderStatus.CANCELLED,
        seconds=after_expiry,
        source_sequence=2,
    )
    cancelled = apply_execution_result(accepted, cancelled_event)
    assert cancelled.status == OrderStatus.CANCELLED

    with pytest.raises(ValueError, match="outside the execution window"):
        apply_execution_result(
            accepted,
            _result(
                event_id="late-fill",
                status=OrderStatus.PARTIALLY_FILLED,
                seconds=after_expiry,
                source_sequence=2,
                quantity=100,
                price=Decimal("10"),
            ),
        )

    exact_expiry = int((intent.expires_at - BASE_TIME).total_seconds())
    with pytest.raises(ValueError, match="outside the execution window"):
        apply_execution_result(
            accepted,
            _result(
                event_id="exact-expiry-fill",
                status=OrderStatus.PARTIALLY_FILLED,
                seconds=exact_expiry,
                source_sequence=2,
                quantity=100,
                price=Decimal("10"),
            ),
        )

    with pytest.raises(ValueError, match="cannot precede"):
        apply_execution_result(
            accepted,
            ExecutionResult(
                intent_id="intent-1",
                order_id="order-1",
                event_id="late-received-old-terminal",
                status=OrderStatus.CANCELLED,
                occurred_at=BASE_TIME,
                received_at=intent.expires_at + timedelta(seconds=2),
                source_sequence=2,
                idempotency_key=execution_result_idempotency_key(
                    order_id="order-1",
                    event_id="late-received-old-terminal",
                ),
            ),
        )
def test_oms_transition_relation_has_absorbing_terminal_states():
    for previous, allowed in ACTIVE_TRANSITIONS.items():
        for next_status in OrderStatus:
            assert can_transition(previous, next_status) is (next_status in allowed)
    for terminal in TERMINAL_STATUSES:
        assert not ACTIVE_TRANSITIONS[terminal]
        with pytest.raises(ValueError, match="illegal order transition"):
            transition_order(terminal, OrderStatus.QUEUED)


def test_t1_uses_trading_sessions_not_calendar_days_and_tracks_lots():
    friday = date(2026, 8, 7)
    monday = date(2026, 8, 10)
    tuesday = date(2026, 8, 11)
    calendar = (friday, monday, tuesday)
    old_lot = PositionLot(friday, 100, "old")
    new_lot = PositionLot(monday, 200, "new")

    assert earliest_sell_date(
        acquired_on=friday,
        trading_days=calendar,
        settlement_days=1,
    ) == monday
    assert not is_lot_sellable(
        old_lot, on_date=friday, trading_days=calendar, settlement_days=1
    )
    assert is_lot_sellable(
        old_lot, on_date=monday, trading_days=calendar, settlement_days=1
    )
    assert sellable_quantity(
        (old_lot, new_lot),
        on_date=monday,
        trading_days=calendar,
        settlement_days=1,
    ) == 100
    assert locked_quantity(
        (old_lot, new_lot),
        on_date=monday,
        trading_days=calendar,
        settlement_days=1,
    ) == 200
    assert is_lot_sellable(
        new_lot, on_date=monday, trading_days=calendar, settlement_days=0
    )


def test_t1_sellable_quantity_is_monotone_over_a_fixed_lot_set():
    days = tuple(date(2026, 8, day) for day in (3, 4, 5, 6, 7))
    lots = tuple(PositionLot(day, (index + 1) * 100) for index, day in enumerate(days))
    quantities = [
        sellable_quantity(
            lots,
            on_date=day,
            trading_days=days,
            settlement_days=1,
        )
        for day in days
    ]
    assert quantities == sorted(quantities)
    assert quantities[0] == 0
    assert quantities[-1] == sum(lot.quantity for lot in lots[:-1])


def test_fee_golden_case_is_side_aware_and_order_aggregated():
    schedule = _a_share_fees()
    buy = calculate_order_fees(
        side=OrderSide.BUY,
        schedule=schedule,
        price=Decimal("10"),
        quantity=1_000,
    )
    sell = calculate_order_fees(
        side=OrderSide.SELL,
        schedule=schedule,
        notional=Decimal("10000"),
    )
    assert (buy.notional, buy.commission, buy.transfer_fee, buy.stamp_duty) == (
        Decimal("10000.00"),
        Decimal("5.00"),
        Decimal("0.10"),
        Decimal("0.00"),
    )
    assert sell.total == Decimal("10.10")
    assert cash_effect(side=OrderSide.BUY, fees=buy) == Decimal("-10005.10")
    assert cash_effect(side=OrderSide.SELL, fees=sell) == Decimal("9989.90")

    aggregate = calculate_order_fees(
        side=OrderSide.BUY, schedule=schedule, notional=Decimal("10000")
    )
    two_partial_minimums = sum(
        (
            calculate_order_fees(
                side=OrderSide.BUY,
                schedule=schedule,
                notional=Decimal("5000"),
            ).total
            for _ in range(2)
        ),
        Decimal("0"),
    )
    assert aggregate.total == Decimal("5.10")
    assert aggregate.total < two_partial_minimums

    first_fill = incremental_order_fee_delta(
        side=OrderSide.BUY,
        schedule=schedule,
        previous_notional=Decimal("0"),
        new_total_notional=Decimal("5000"),
    )
    second_fill = incremental_order_fee_delta(
        side=OrderSide.BUY,
        schedule=schedule,
        previous_notional=Decimal("5000"),
        new_total_notional=Decimal("10000"),
    )
    assert first_fill.fee_profile_version == schedule.profile_version
    assert first_fill.total + second_fill.total == aggregate.total


def test_fee_total_is_non_negative_and_monotone_in_notional():
    schedule = _a_share_fees()
    for side in OrderSide:
        totals = [
            calculate_order_fees(
                side=side,
                schedule=schedule,
                notional=Decimal(notional),
            ).total
            for notional in range(100, 100_001, 137)
        ]
        assert all(total >= 0 for total in totals)
        assert totals == sorted(totals)


def test_fee_contract_rejects_unknown_side_and_accepts_decimal_wire_values():
    schedule = FeeSchedule(
        profile_version="fee-v1",
        buy_commission_rate="0.0001",
        sell_commission_rate="0.0001",
        minimum_commission="5",
        stamp_duty_sell_rate="0.0005",
        transfer_fee_buy_rate="0.00001",
        transfer_fee_sell_rate="0.00001",
    )
    assert schedule.minimum_commission == Decimal("5")
    with pytest.raises(ValueError):
        calculate_order_fees(
            side="HOLD",
            schedule=schedule,
            notional=Decimal("10000"),
        )


def test_versioned_market_rule_golden_price_band_and_quantity_checks():
    rule = _a_share_rule()
    band = calculate_price_band(
        instrument_id="600000.SH",
        trade_date=TRADE_DATE,
        as_of=BASE_TIME,
        source="test-daily-band",
        previous_close=Decimal("10.01"),
        limit_ratio=Decimal("0.10"),
        tick_size=Decimal("0.01"),
    )
    assert band is not None
    assert (band.lower, band.upper) == (Decimal("9.01"), Decimal("11.01"))
    assert floor_buy_quantity(99, rule) == 0
    assert floor_buy_quantity(250, rule) == 200

    valid = validate_intent_against_rule(
        _intent(quantity=200, limit_price=Decimal("11.01")),
        rule=rule,
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        authoritative_price_band=band,
        authoritative_price_band_max_age=DAILY_BAND_MAX_AGE,
        dynamic_price_cage=_price_band("9", "12"),
        dynamic_price_cage_max_age=DYNAMIC_CAGE_MAX_AGE,
    )
    assert valid.allowed
    assert valid.violations == ()

    invalid = validate_intent_against_rule(
        _intent(quantity=250, limit_price=Decimal("11.02")),
        rule=rule,
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        authoritative_price_band=band,
        authoritative_price_band_max_age=DAILY_BAND_MAX_AGE,
        dynamic_price_cage=_price_band("9", "12"),
        dynamic_price_cage_max_age=DYNAMIC_CAGE_MAX_AGE,
    )
    assert not invalid.allowed
    assert RuleViolation.BUY_QUANTITY_INVALID in invalid.violations
    assert RuleViolation.LIMIT_PRICE_OUTSIDE_BAND in invalid.violations


def test_sell_rule_distinguishes_t1_lock_from_valid_odd_lot_liquidation():
    rule = _a_share_rule()
    locked = validate_intent_against_rule(
        _intent(side=OrderSide.SELL, quantity=100),
        rule=rule,
        trade_date=TRADE_DATE,
        total_position_quantity=100,
        broker_sellable_quantity=0,
        locally_computed_sellable_quantity=0,
        **_market_facts(),
    )
    assert locked.violations == (RuleViolation.T1_QUANTITY_LOCKED,)

    odd_lot_exit = validate_intent_against_rule(
        _intent(side=OrderSide.SELL, quantity=50),
        rule=rule,
        trade_date=TRADE_DATE,
        total_position_quantity=50,
        broker_sellable_quantity=50,
        locally_computed_sellable_quantity=50,
        **_market_facts(),
    )
    assert odd_lot_exit.allowed

    odd_remainder_with_round_lot_left = validate_intent_against_rule(
        _intent(side=OrderSide.SELL, quantity=50),
        rule=rule,
        trade_date=TRADE_DATE,
        total_position_quantity=150,
        broker_sellable_quantity=150,
        locally_computed_sellable_quantity=150,
        **_market_facts(),
    )
    assert odd_remainder_with_round_lot_left.allowed

    invalid_odd_quantity = validate_intent_against_rule(
        _intent(side=OrderSide.SELL, quantity=75),
        rule=rule,
        trade_date=TRADE_DATE,
        total_position_quantity=250,
        broker_sellable_quantity=250,
        locally_computed_sellable_quantity=250,
        **_market_facts(),
    )
    assert invalid_odd_quantity.violations == (
        RuleViolation.SELL_QUANTITY_INVALID,
    )

    insufficient = validate_intent_against_rule(
        _intent(side=OrderSide.SELL, quantity=200),
        rule=rule,
        trade_date=TRADE_DATE,
        total_position_quantity=100,
        broker_sellable_quantity=100,
        locally_computed_sellable_quantity=100,
        **_market_facts(),
    )
    assert RuleViolation.INSUFFICIENT_POSITION in insufficient.violations


def test_dynamic_price_cage_is_fail_closed_and_authoritative_when_required():
    rule = _a_share_rule()
    missing = validate_intent_against_rule(
        _intent(quantity=100, limit_price=Decimal("10")),
        rule=rule,
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        authoritative_price_band=_price_band("9", "11"),
        authoritative_price_band_max_age=DAILY_BAND_MAX_AGE,
    )
    assert RuleViolation.DYNAMIC_PRICE_CAGE_UNAVAILABLE in missing.violations

    outside = validate_intent_against_rule(
        _intent(quantity=100, limit_price=Decimal("10.30")),
        rule=rule,
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        authoritative_price_band=_price_band("9", "11"),
        authoritative_price_band_max_age=DAILY_BAND_MAX_AGE,
        dynamic_price_cage=_price_band("9.80", "10.20"),
        dynamic_price_cage_max_age=DYNAMIC_CAGE_MAX_AGE,
    )
    assert (
        RuleViolation.LIMIT_PRICE_OUTSIDE_DYNAMIC_CAGE
        in outside.violations
    )


def test_rule_validation_fails_closed_on_missing_band_or_wrong_instrument():
    rule = _a_share_rule()
    check = validate_intent_against_rule(
        _intent(instrument_id="000001.SZ"),
        rule=rule,
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        dynamic_price_cage=_price_band("9", "11"),
        dynamic_price_cage_max_age=DYNAMIC_CAGE_MAX_AGE,
    )
    assert RuleViolation.INSTRUMENT_MISMATCH in check.violations
    assert RuleViolation.PRICE_BAND_UNAVAILABLE in check.violations


@pytest.mark.parametrize(
    "bad_band",
    (
        _price_band("9", "11", instrument_id="000001.SZ"),
        _price_band("9", "11", trade_date=date(2026, 8, 4)),
        _price_band("9", "11", as_of=EVALUATED_AT + timedelta(seconds=1)),
        _price_band("9", "11", as_of=EVALUATED_AT - timedelta(days=5)),
    ),
    ids=("wrong-instrument", "wrong-date", "future", "stale"),
)
def test_authoritative_daily_band_fails_closed_on_wrong_context_or_time(
    bad_band: PriceBand,
):
    check = validate_intent_against_rule(
        _intent(quantity=100, limit_price=Decimal("10")),
        rule=_a_share_rule(),
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        authoritative_price_band=bad_band,
        authoritative_price_band_max_age=DAILY_BAND_MAX_AGE,
        dynamic_price_cage=_price_band("9", "11"),
        dynamic_price_cage_max_age=DYNAMIC_CAGE_MAX_AGE,
    )
    assert RuleViolation.PRICE_BAND_UNAVAILABLE in check.violations


@pytest.mark.parametrize(
    "bad_cage",
    (
        _price_band("9", "11", instrument_id="000001.SZ"),
        _price_band("9", "11", trade_date=date(2026, 8, 4)),
        _price_band("9", "11", as_of=EVALUATED_AT + timedelta(seconds=1)),
        _price_band("9", "11", as_of=EVALUATED_AT - timedelta(seconds=6)),
    ),
    ids=("wrong-instrument", "wrong-date", "future", "stale"),
)
def test_dynamic_cage_fails_closed_on_wrong_context_or_time(
    bad_cage: PriceBand,
):
    check = validate_intent_against_rule(
        _intent(quantity=100, limit_price=Decimal("10")),
        rule=_a_share_rule(),
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        authoritative_price_band=_price_band("9", "11"),
        authoritative_price_band_max_age=DAILY_BAND_MAX_AGE,
        dynamic_price_cage=bad_cage,
        dynamic_price_cage_max_age=DYNAMIC_CAGE_MAX_AGE,
    )
    assert RuleViolation.DYNAMIC_PRICE_CAGE_UNAVAILABLE in check.violations


def test_present_price_fact_without_max_age_is_not_treated_as_fresh():
    check = validate_intent_against_rule(
        _intent(quantity=100, limit_price=Decimal("10")),
        rule=_a_share_rule(),
        trade_date=TRADE_DATE,
        evaluated_at=EVALUATED_AT,
        authoritative_price_band=_price_band("9", "11"),
        dynamic_price_cage=_price_band("9", "11"),
    )
    assert RuleViolation.PRICE_BAND_UNAVAILABLE in check.violations
    assert RuleViolation.DYNAMIC_PRICE_CAGE_UNAVAILABLE in check.violations
