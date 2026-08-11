from __future__ import annotations

import ast
import inspect
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

import server.integrations.v2_execution_adapter.matcher as adapter_module
from server.integrations.v2_execution_adapter import (
    map_v2_level1_match_inputs,
    match_v2_level1_read_only,
)
from server.trading_core.execution.matcher import (
    MatchReason,
    MatchStatus,
    match_limit_day,
)
from server.trading_v2.domain import OrderSide, Quote
from server.trading_v2.matcher import PaperMatcher
from server.trading_v2.policy import load_portfolio_policy


NOW = datetime(2026, 7, 27, 9, 31)
TICK = Decimal("0.01")
POLICY = load_portfolio_policy()


def _quote(**overrides: object) -> Quote:
    values: dict[str, object] = {
        "stock_code": "000001",
        "event_id": "quote-1",
        "quote_at": NOW,
        "received_at": NOW,
        "bid1": Decimal("9.99"),
        "bid1_volume": 10_000,
        "ask1": Decimal("10.00"),
        "ask1_volume": 10_000,
        "last_price": Decimal("9.995"),
        "upper_limit": Decimal("11.00"),
        "lower_limit": Decimal("9.00"),
        "suspended": False,
    }
    values.update(overrides)
    return Quote(**values)  # type: ignore[arg-type]


def test_naive_frozen_v2_times_default_to_asia_shanghai_not_utc():
    mapped = map_v2_level1_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10"),
        quote=_quote(),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )

    assert mapped.evaluated_at.tzinfo == ZoneInfo("Asia/Shanghai")
    assert mapped.quote is not None
    assert mapped.quote.observed_at.tzinfo == ZoneInfo("Asia/Shanghai")


CASES = (
    pytest.param({}, {}, id="buy-full"),
    pytest.param(
        {"remaining_quantity": 5_000},
        {"ask1_volume": 2_000},
        id="buy-visible-participation-partial",
    ),
    pytest.param(
        {
            "side": OrderSide.SELL,
            "limit_price": Decimal("9.90"),
            "slippage_rate": Decimal("0.0005"),
            "impact_rate": Decimal("0.0005"),
        },
        {"bid1": Decimal("10.00")},
        id="sell-adverse-tick-rounding",
    ),
    pytest.param({}, None, id="missing-quote"),
    pytest.param({}, {"ask1": None, "ask1_volume": None}, id="no-last-fallback"),
    pytest.param(
        {},
        {"quote_at": NOW - timedelta(seconds=16), "received_at": NOW},
        id="stale-quote",
    ),
    pytest.param(
        {},
        {
            "quote_at": NOW + timedelta(seconds=1),
            "received_at": NOW + timedelta(seconds=1),
        },
        id="future-quote",
    ),
    pytest.param({}, {"suspended": True}, id="suspended"),
    pytest.param(
        {"limit_price": Decimal("11.00")},
        {"ask1": Decimal("11.00")},
        id="buy-upper-lock",
    ),
    pytest.param(
        {"side": OrderSide.SELL, "limit_price": Decimal("9.00")},
        {"bid1": Decimal("9.00")},
        id="sell-lower-lock",
    ),
    pytest.param(
        {"limit_price": Decimal("9.99")},
        {},
        id="buy-base-above-limit",
    ),
    pytest.param(
        {
            "limit_price": Decimal("10.00"),
            "slippage_rate": Decimal("0.001"),
        },
        {},
        id="buy-adverse-price-above-limit",
    ),
    pytest.param(
        {"side": OrderSide.SELL, "limit_price": Decimal("10.00")},
        {},
        id="sell-base-below-limit",
    ),
    pytest.param({}, {"ask1_volume": 0}, id="zero-visible-volume"),
    pytest.param(
        {"remaining_quantity": 1_000, "liquidity_quantity": 125},
        {},
        id="external-liquidity-cap",
    ),
    pytest.param(
        {"remaining_quantity": 1_000, "approved_remaining_quantity": 175},
        {},
        id="approval-cap",
    ),
    pytest.param(
        {},
        {"upper_limit": None, "lower_limit": None},
        id="v2-absent-price-band",
    ),
    pytest.param(
        {"limit_price": Decimal("12.00")},
        {},
        id="v2-does-not-validate-order-limit-inside-band",
    ),
    pytest.param(
        {
            "limit_price": Decimal("12.00"),
            "slippage_rate": Decimal("0.002"),
        },
        {"ask1": Decimal("10.99")},
        id="v2-allows-adverse-price-outside-band-if-order-limit-allows",
    ),
    pytest.param(
        {"limit_price": Decimal("11.00")},
        {"lower_limit": None, "ask1": Decimal("11.00")},
        id="v2-one-sided-upper-band",
    ),
)


@pytest.mark.parametrize(("call_overrides", "quote_overrides"), CASES)
def test_read_only_adapter_is_differentially_equal_to_v2_level1_matcher(
    call_overrides: dict[str, object],
    quote_overrides: dict[str, object] | None,
):
    call: dict[str, object] = {
        "side": OrderSide.BUY,
        "remaining_quantity": 1_000,
        "approved_remaining_quantity": 1_000,
        "limit_price": Decimal("10.10"),
        "quote": None if quote_overrides is None else _quote(**quote_overrides),
        "now": NOW,
        "tick_size": TICK,
        "liquidity_quantity": 1_000,
        "slippage_rate": Decimal("0"),
        "impact_rate": Decimal("0"),
    }
    call.update(call_overrides)

    expected = PaperMatcher(POLICY).match(**call)  # type: ignore[arg-type]
    actual = match_v2_level1_read_only(policy=POLICY, **call)

    assert actual.status == expected.status
    assert actual.waiting_reason == expected.waiting_reason
    assert actual.fill_quantity == expected.fill_quantity
    assert actual.fill_price == expected.fill_price
    assert actual.event_id == expected.event_id


def test_v2_mapping_reconstructs_cumulative_order_state_and_sequence():
    mapped = map_v2_level1_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=900,
        approved_remaining_quantity=900,
        limit_price=Decimal("10.10"),
        quote=_quote(ask1_volume=2_000),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=900,
        policy=POLICY,
        prior_filled_quantity=100,
        last_source_sequence=3,
        assume_timezone=timezone(timedelta(hours=8)),
    )
    decision = match_limit_day(
        order=mapped.order,
        quote=mapped.quote,
        rule=mapped.rule,
        evaluated_at=mapped.evaluated_at,
    )

    assert mapped.evaluated_at.utcoffset() == timedelta(hours=8)
    assert mapped.order.requested_quantity == 1_000
    assert mapped.order.cumulative_filled_quantity == 100
    assert decision.status == MatchStatus.PARTIALLY_FILLED
    assert decision.fill_quantity == 400
    assert decision.source_sequence == 4
    assert decision.updated_order.cumulative_filled_quantity == 500


def test_neutral_matcher_intentionally_rejects_a_future_receipt_v2_ignores():
    quote = _quote(received_at=NOW + timedelta(seconds=1))
    legacy = PaperMatcher(POLICY).match(
        side=OrderSide.BUY,
        remaining_quantity=1_000,
        approved_remaining_quantity=1_000,
        limit_price=Decimal("10.10"),
        quote=quote,
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=1_000,
    )
    mapped = map_v2_level1_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=1_000,
        approved_remaining_quantity=1_000,
        limit_price=Decimal("10.10"),
        quote=quote,
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=1_000,
        policy=POLICY,
    )
    neutral = match_limit_day(
        order=mapped.order,
        quote=mapped.quote,
        rule=mapped.rule,
        evaluated_at=mapped.evaluated_at,
    )

    assert legacy.status == "FILLED"
    assert neutral.status == MatchStatus.WAITING
    assert neutral.reason == MatchReason.WAIT_FUTURE_QUOTE


def test_v2_matcher_adapter_has_no_write_capable_imports():
    tree = ast.parse(inspect.getsource(adapter_module))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    forbidden = (
        "server.trading_v2.execution",
        "server.trading_v2.ledger",
        "server.trading_v2.repository",
        "sqlalchemy",
    )
    assert not {
        module
        for module in imported
        if module.startswith(forbidden)
    }


def test_adapter_rejects_invalid_non_equivalent_v2_state():
    with pytest.raises(ValueError, match="remaining_quantity"):
        match_v2_level1_read_only(
            side=OrderSide.BUY,
            remaining_quantity=0,
            approved_remaining_quantity=0,
            limit_price=Decimal("10.10"),
            quote=_quote(),
            now=NOW,
            tick_size=TICK,
            liquidity_quantity=0,
            policy=POLICY,
        )

    with pytest.raises(ValueError, match="non-negative"):
        match_v2_level1_read_only(
            side=OrderSide.BUY,
            remaining_quantity=100,
            approved_remaining_quantity=100,
            limit_price=Decimal("10.10"),
            quote=_quote(),
            now=NOW,
            tick_size=TICK,
            liquidity_quantity=100,
            policy=POLICY,
            slippage_rate=Decimal("-0.01"),
        )
