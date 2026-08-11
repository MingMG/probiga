from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.trading_core.contracts import OrderSide, OrderStatus
from server.trading_core.execution.matcher import (
    Level1Quote,
    LimitDayMatchRule,
    LimitDayOrder,
    MatchPriceBand,
    MatchReason,
    MatchStatus,
    match_limit_day,
)


CHINA_TZ = timezone(timedelta(hours=8))
BASE_TIME = datetime(2026, 8, 3, 9, 31, tzinfo=CHINA_TZ)


def _order(**overrides: object) -> LimitDayOrder:
    values: dict[str, object] = {
        "order_id": "order-1",
        "intent_id": "intent-1",
        "instrument_id": "600000.SH",
        "side": OrderSide.BUY,
        "requested_quantity": 1_000,
        "approved_quantity": 1_000,
        "cumulative_filled_quantity": 0,
        "limit_price": Decimal("10.10"),
        "earliest_at": BASE_TIME,
        "expires_at": BASE_TIME + timedelta(hours=6),
        "updated_at": BASE_TIME,
        "last_source_sequence": 0,
        "status": OrderStatus.QUEUED,
    }
    values.update(overrides)
    return LimitDayOrder(**values)  # type: ignore[arg-type]


def _quote(**overrides: object) -> Level1Quote:
    values: dict[str, object] = {
        "instrument_id": "600000.SH",
        "quote_id": "quote-1",
        "observed_at": BASE_TIME,
        "received_at": BASE_TIME,
        "bid_price": Decimal("9.99"),
        "bid_quantity": 10_000,
        "ask_price": Decimal("10.00"),
        "ask_quantity": 10_000,
        "suspended": False,
    }
    values.update(overrides)
    return Level1Quote(**values)  # type: ignore[arg-type]


def _band(**overrides: object) -> MatchPriceBand:
    values: dict[str, object] = {
        "instrument_id": "600000.SH",
        "trade_date": BASE_TIME.date(),
        "as_of": BASE_TIME,
        "source": "authoritative-test-band",
        "lower": Decimal("9.00"),
        "upper": Decimal("11.00"),
    }
    values.update(overrides)
    return MatchPriceBand(**values)  # type: ignore[arg-type]


def _rule(**overrides: object) -> LimitDayMatchRule:
    values: dict[str, object] = {
        "rule_version": "matcher-rule-v1",
        "tick_size": Decimal("0.01"),
        "quote_max_age": timedelta(seconds=15),
        "visible_volume_participation": Decimal("0.20"),
        "maximum_fill_quantity": None,
        "price_band": _band(),
        "price_band_max_age": timedelta(days=1),
    }
    values.update(overrides)
    return LimitDayMatchRule(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "quote",
    (
        _quote(
            observed_at=BASE_TIME + timedelta(seconds=1),
            received_at=BASE_TIME + timedelta(seconds=1),
        ),
        _quote(received_at=BASE_TIME + timedelta(seconds=1)),
    ),
    ids=("future-market-time", "future-receipt-time"),
)
def test_future_quote_is_never_matched_or_sequenced(quote: Level1Quote):
    order = _order(last_source_sequence=7)

    result = match_limit_day(
        order=order,
        quote=quote,
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )

    assert result.status == MatchStatus.WAITING
    assert result.reason == MatchReason.WAIT_FUTURE_QUOTE
    assert result.updated_order is order
    assert result.execution_result is None
    assert result.source_sequence is None


def test_price_band_is_fail_closed_and_blocks_adverse_edge():
    missing = match_limit_day(
        order=_order(),
        quote=_quote(),
        rule=_rule(price_band=None, price_band_max_age=None),
        evaluated_at=BASE_TIME,
    )
    locked = match_limit_day(
        order=_order(limit_price=Decimal("11.00")),
        quote=_quote(ask_price=Decimal("11.00")),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )
    outside_order = match_limit_day(
        order=_order(limit_price=Decimal("11.01")),
        quote=_quote(),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )

    assert missing.reason == MatchReason.WAIT_PRICE_BAND
    assert locked.reason == MatchReason.WAIT_LIMIT_LOCK
    assert outside_order.reason == MatchReason.WAIT_PRICE_BAND
    assert not any(item.fill_quantity for item in (missing, locked, outside_order))


def test_price_band_blocks_adverse_rounding_outside_the_band():
    result = match_limit_day(
        order=_order(limit_price=Decimal("11.00")),
        quote=_quote(ask_price=Decimal("10.99")),
        rule=_rule(slippage_rate=Decimal("0.002")),
        evaluated_at=BASE_TIME,
    )

    assert result.reason == MatchReason.WAIT_PRICE_BAND
    assert result.fill_quantity == 0


@pytest.mark.parametrize(
    "band",
    (
        _band(as_of=BASE_TIME + timedelta(seconds=1)),
        _band(as_of=BASE_TIME - timedelta(days=2)),
        _band(trade_date=(BASE_TIME + timedelta(days=1)).date()),
    ),
    ids=("future-band", "stale-band", "wrong-trade-date"),
)
def test_future_stale_or_wrong_session_price_band_fails_closed(
    band: MatchPriceBand,
):
    result = match_limit_day(
        order=_order(),
        quote=_quote(),
        rule=_rule(price_band=band),
        evaluated_at=BASE_TIME,
    )

    assert result.status == MatchStatus.WAITING
    assert result.reason == MatchReason.WAIT_PRICE_BAND


def test_volume_caps_partial_fills_and_sequences_accumulate_contiguously():
    first = match_limit_day(
        order=_order(last_source_sequence=7),
        quote=_quote(ask_quantity=2_000),
        rule=_rule(maximum_fill_quantity=300),
        evaluated_at=BASE_TIME,
    )

    assert first.status == MatchStatus.PARTIALLY_FILLED
    assert first.fill_quantity == 300
    assert first.fill_price == Decimal("10.00")
    assert first.source_sequence == 8
    assert first.updated_order.cumulative_filled_quantity == 300
    assert first.updated_order.remaining_quantity == 700

    second_quote = _quote(
        quote_id="quote-2",
        observed_at=BASE_TIME + timedelta(seconds=1),
        received_at=BASE_TIME + timedelta(seconds=1),
        ask_quantity=10_000,
    )
    second = match_limit_day(
        order=first.updated_order,
        quote=second_quote,
        rule=_rule(),
        evaluated_at=BASE_TIME + timedelta(seconds=1),
    )

    assert second.status == MatchStatus.FILLED
    assert second.fill_quantity == 700
    assert second.source_sequence == 9
    assert second.updated_order.cumulative_filled_quantity == 1_000
    assert second.updated_order.status == OrderStatus.FILLED


def test_approval_is_an_independent_fill_cap():
    result = match_limit_day(
        order=_order(approved_quantity=250),
        quote=_quote(),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )

    assert result.status == MatchStatus.PARTIALLY_FILLED
    assert result.fill_quantity == 250
    assert result.updated_order.approved_remaining_quantity == 0


def test_fill_is_deterministic_and_identical_retry_cannot_double_fill():
    order = _order()
    quote = _quote()
    rule = _rule(maximum_fill_quantity=400)

    first = match_limit_day(
        order=order,
        quote=quote,
        rule=rule,
        evaluated_at=BASE_TIME,
    )
    same_inputs = match_limit_day(
        order=order,
        quote=quote,
        rule=rule,
        evaluated_at=BASE_TIME,
    )
    retry = match_limit_day(
        order=first.updated_order,
        quote=quote,
        rule=rule,
        evaluated_at=BASE_TIME,
    )

    assert same_inputs == first
    assert retry.status == MatchStatus.DUPLICATE
    assert retry.reason == MatchReason.DUPLICATE_EVENT
    assert retry.updated_order == first.updated_order
    assert retry.execution_result is None
    assert first.idempotency_key

    with pytest.raises(ValueError, match="already applied with different"):
        match_limit_day(
            order=first.updated_order,
            quote=replace(quote, ask_price=Decimal("10.01")),
            rule=rule,
            evaluated_at=BASE_TIME,
        )
    with pytest.raises(ValueError, match="already applied with different"):
        match_limit_day(
            order=first.updated_order,
            quote=quote,
            rule=replace(rule, rule_version="matcher-rule-v2"),
            evaluated_at=BASE_TIME,
        )


def test_exact_day_expiry_wins_over_a_quote_and_advances_once():
    expires_at = BASE_TIME + timedelta(seconds=10)
    order = _order(expires_at=expires_at, last_source_sequence=3)
    boundary_quote = _quote(
        observed_at=expires_at,
        received_at=expires_at,
    )

    expired = match_limit_day(
        order=order,
        quote=boundary_quote,
        rule=_rule(),
        evaluated_at=expires_at,
    )

    assert expired.status == MatchStatus.EXPIRED
    assert expired.fill_quantity == 0
    assert expired.source_sequence == 4
    assert expired.updated_order.status == OrderStatus.EXPIRED
    assert expired.execution_result is not None
    assert expired.execution_result.occurred_at == expires_at

    later_delivery = match_limit_day(
        order=order,
        quote=None,
        rule=_rule(),
        evaluated_at=expires_at + timedelta(seconds=2),
    )
    assert later_delivery.idempotency_key == expired.idempotency_key
    assert later_delivery.execution_result is not None
    assert later_delivery.execution_result.occurred_at == expires_at
    assert later_delivery.source_sequence == expired.source_sequence

    retry = match_limit_day(
        order=expired.updated_order,
        quote=None,
        rule=_rule(),
        evaluated_at=expires_at + timedelta(seconds=1),
    )
    assert retry.status == MatchStatus.TERMINAL
    assert retry.updated_order.last_source_sequence == 4
    assert retry.execution_result is None


def test_expiry_is_not_suppressed_by_retrying_a_previously_filled_quote():
    expires_at = BASE_TIME + timedelta(seconds=10)
    first = match_limit_day(
        order=_order(expires_at=expires_at),
        quote=_quote(),
        rule=_rule(maximum_fill_quantity=100),
        evaluated_at=BASE_TIME,
    )
    assert first.status == MatchStatus.PARTIALLY_FILLED

    expired = match_limit_day(
        order=first.updated_order,
        quote=_quote(),
        rule=_rule(maximum_fill_quantity=100),
        evaluated_at=expires_at,
    )

    assert expired.status == MatchStatus.EXPIRED
    assert expired.source_sequence == 2
    assert expired.updated_order.cumulative_filled_quantity == 100


def test_fill_occurs_when_processed_and_state_time_cannot_move_backwards():
    quote = _quote(
        observed_at=BASE_TIME + timedelta(seconds=1),
        received_at=BASE_TIME + timedelta(seconds=5),
    )
    evaluated_at = BASE_TIME + timedelta(seconds=10)
    matched = match_limit_day(
        order=_order(),
        quote=quote,
        rule=_rule(),
        evaluated_at=evaluated_at,
    )

    assert matched.execution_result is not None
    assert matched.execution_result.occurred_at == evaluated_at
    assert matched.updated_order.updated_at == evaluated_at
    with pytest.raises(ValueError, match="precede order updated_at"):
        match_limit_day(
            order=matched.updated_order,
            quote=None,
            rule=_rule(),
            evaluated_at=evaluated_at - timedelta(seconds=1),
        )


def test_matcher_rejects_subclass_contract_bypass():
    class ForgedOrder(LimitDayOrder):
        @property
        def approved_remaining_quantity(self) -> int:
            return 100

    forged = object.__new__(ForgedOrder)
    with pytest.raises(TypeError, match="exactly LimitDayOrder"):
        match_limit_day(
            order=forged,
            quote=_quote(),
            rule=_rule(),
            evaluated_at=BASE_TIME + timedelta(seconds=2),
        )
def test_stale_pre_order_and_out_of_order_quotes_never_fill():
    stale_time = BASE_TIME - timedelta(seconds=16)
    stale_order = _order(earliest_at=stale_time, updated_at=stale_time)
    stale = match_limit_day(
        order=stale_order,
        quote=_quote(observed_at=stale_time, received_at=stale_time),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )
    pre_order = match_limit_day(
        order=_order(),
        quote=_quote(
            observed_at=BASE_TIME - timedelta(seconds=1),
            received_at=BASE_TIME,
        ),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )
    live_order = _order(
        earliest_at=BASE_TIME - timedelta(seconds=10),
        updated_at=BASE_TIME,
    )
    out_of_order = match_limit_day(
        order=live_order,
        quote=_quote(
            observed_at=BASE_TIME - timedelta(seconds=1),
            received_at=BASE_TIME,
        ),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )

    assert stale.reason == MatchReason.WAIT_STALE_QUOTE
    assert pre_order.reason == MatchReason.WAIT_PRE_ORDER_QUOTE
    assert out_of_order.reason == MatchReason.WAIT_OUT_OF_ORDER_QUOTE


def test_buy_requires_ask_and_sell_requires_bid_without_last_price_fallback():
    buy = match_limit_day(
        order=_order(),
        quote=_quote(ask_price=None, ask_quantity=None),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )
    sell = match_limit_day(
        order=_order(side=OrderSide.SELL, limit_price=Decimal("9.90")),
        quote=_quote(bid_price=None, bid_quantity=None),
        rule=_rule(),
        evaluated_at=BASE_TIME,
    )

    assert buy.reason == MatchReason.WAIT_NO_QUOTE
    assert sell.reason == MatchReason.WAIT_NO_QUOTE
