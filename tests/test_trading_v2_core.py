from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from server.trading_v2.domain import (
    AccountSnapshot,
    InstrumentRule,
    IntentAction,
    OrderSide,
    PositionFacts,
    PositionState,
    Quote,
    RiskDecisionStatus,
    TradeIntent,
)
from server.trading_v2.matcher import PaperMatcher, PaperSnapshotMatcher
from server.trading_v2.historical_matcher import (
    DailyBar,
    HistoricalDailyMatcher,
)
from server.trading_v2.ledger import FeeProfile, LedgerBook
from server.trading_v2.oms import (
    fill_idempotency_key,
    order_idempotency_key,
    transition_order,
)
from server.trading_v2.domain import OrderStatus
from server.trading_v2.market_regime import classify_market_regime
from server.trading_v2.versioning import code_version
from server.trading_v2.policy import RiskAdjudicator, load_portfolio_policy
from server.trading_v2.planner import (
    _initial_target_quantity,
    _reserve_pending_entry_exposure,
)
from server.trading_v2.positions import evaluate_position, monotonic_protective_stop
from server.trading_v2.quotes import build_quote_event
from server.trading_v2.research import (
    CompletedTrade,
    evaluate_oos_gate,
    trade_metrics,
)
from tools.add_trading_v2_tasks import TASKS as TRADING_V2_TASKS


def _confirmed_policy():
    return replace(
        load_portfolio_policy(),
        fee_profile_version="fee-confirmed-v1",
        instrument_rule_version="instrument-rule-v1",
    )


def _rule(**overrides):
    data = {
        "stock_code": "000001",
        "rule_version": "instrument-rule-v1",
        "security_type": "A_SHARE",
        "exchange": "SZSE",
        "effective_from": date(2026, 1, 1),
        "effective_to": None,
        "can_buy": True,
        "first_buy_minimum": 100,
        "buy_lot_size": 100,
        "sell_lot_size": 1,
        "settlement_days": 1,
        "tick_size": Decimal("0.01"),
        "limit_ratio": Decimal("0.10"),
        "permission_required": "A_SHARE",
        "permission_confirmed": True,
        "fee_profile_version": "fee-confirmed-v1",
    }
    data.update(overrides)
    return InstrumentRule(**data)


def _intent(**overrides):
    now = datetime(2026, 7, 27, 9, 20)
    data = {
        "intent_id": "intent-1",
        "account_id": "paper-main-v2",
        "decision_run_uid": "run-1",
        "strategy_version": "stock_strategy_v2.0.0:short_term",
        "stock_code": "000001",
        "action": IntentAction.OPEN,
        "current_quantity": 0,
        "target_quantity": 4400,
        "target_weight": Decimal("0.22"),
        "earliest_at": now,
        "expires_at": now + timedelta(days=1),
        "limit_price": Decimal("10.20"),
        "worst_price": Decimal("10.00"),
        "initial_stop": Decimal("9.50"),
        "protective_stop": Decimal("9.50"),
        "invalidation_condition": "close below frozen trend boundary",
        "reason_code": "OOS_ELIGIBLE",
        "evidence": tuple(),
        "idempotency_key": "key-1",
        "theme_code": "bank",
    }
    data.update(overrides)
    return TradeIntent(**data)


def _account(**overrides):
    data = {
        "account_id": "paper-main-v2",
        "equity": Decimal("200000"),
        "available_cash": Decimal("200000"),
        "peak_equity": Decimal("200000"),
        "current_market_value": Decimal("0"),
        "current_open_risk": Decimal("0"),
        "position_count": 0,
        "theme_position_counts": {},
        "theme_market_values": {},
    }
    data.update(overrides)
    return AccountSnapshot(**data)


def test_retail_radar_probe_can_round_up_to_one_risk_capped_board_lot():
    quantity = _initial_target_quantity(
        rule=_rule(stock_code="603629"),
        raw_target=39,
        worst_price=Decimal("110.28"),
        equity=Decimal("200000"),
        allow_minimum_board_lot=True,
        minimum_board_lot_max_weight=Decimal("0.08"),
    )

    assert quantity == 100


def test_retail_radar_probe_does_not_force_an_oversized_board_lot():
    quantity = _initial_target_quantity(
        rule=_rule(stock_code="600519"),
        raw_target=2,
        worst_price=Decimal("1600"),
        equity=Decimal("200000"),
        allow_minimum_board_lot=True,
        minimum_board_lot_max_weight=Decimal("0.08"),
    )

    assert quantity == 0


def test_pending_buy_reserves_cash_risk_theme_and_position_slot():
    updated, pending_codes = _reserve_pending_entry_exposure(
        _account(),
        [
            {
                "stock_code": "002326",
                "limit_price": Decimal("20"),
                "remaining_quantity": 800,
                "theme_code": "CONCEPT:新材料概念",
                "initial_stop": Decimal("18.5"),
            }
        ],
    )
    assert pending_codes == {"002326"}
    assert updated.available_cash == Decimal("184000")
    assert updated.current_market_value == Decimal("16000")
    assert updated.current_open_risk == Decimal("1200")
    assert updated.position_count == 1
    assert updated.theme_position_counts == {
        "CONCEPT:新材料概念": 1
    }
    assert updated.theme_market_values == {
        "CONCEPT:新材料概念": Decimal("16000")
    }


def test_policy_is_exact_200k_four_position_configuration():
    policy = load_portfolio_policy()
    assert policy.initial_cash == Decimal("200000.00")
    assert policy.maximum_positions == 4
    assert policy.single_initial_cap == Decimal("0.22")
    assert policy.hard_trade_risk_cap == Decimal("0.006")
    assert policy.portfolio_open_risk_cap == Decimal("0.02")


def test_close_decision_waits_for_qmt_close_evidence_pipeline():
    by_type = {
        str(item["task_type"]): item for item in TRADING_V2_TASKS
    }
    assert (
        by_type["trading_v2_close_decision"]["cron_time"] == "15:45"
    )
    assert (
        by_type["trading_v2_strategy_health"]["cron_time"] == "15:50"
    )


def test_unconfirmed_external_configuration_fails_closed():
    policy = replace(
        load_portfolio_policy(),
        fee_profile_version="",
        instrument_rule_version="",
    )
    decision = RiskAdjudicator(policy).adjudicate(
        _intent(),
        _account(),
        _rule(),
        market_regime="TREND_UP",
    )
    assert decision.status == RiskDecisionStatus.REJECTED
    assert decision.first_failure == "POLICY_EXTERNAL_CONFIG_CONFIRMED"


def test_risk_quantity_uses_half_percent_budget_and_legal_lot():
    decision = RiskAdjudicator(_confirmed_policy()).adjudicate(
        _intent(),
        _account(),
        _rule(),
        market_regime="TREND_UP",
    )
    # 200,000 * 0.5% / (10.00 - 9.50) = 2,000 shares.
    assert decision.status == RiskDecisionStatus.REDUCED
    assert decision.approved_quantity == 2000
    assert decision.approved_quantity % 100 == 0
    assert decision.trade_risk == Decimal("1000.00")


def test_fifth_position_is_rejected():
    decision = RiskAdjudicator(_confirmed_policy()).adjudicate(
        _intent(),
        _account(position_count=4),
        _rule(),
        market_regime="TREND_UP",
    )
    assert decision.status == RiskDecisionStatus.REJECTED
    assert decision.first_failure == "POSITION_COUNT_CAP"


def test_eight_percent_drawdown_blocks_open_and_add():
    decision = RiskAdjudicator(_confirmed_policy()).adjudicate(
        _intent(),
        _account(equity=Decimal("184000"), peak_equity=Decimal("200000")),
        _rule(),
        market_regime="TREND_UP",
    )
    assert decision.status == RiskDecisionStatus.REJECTED
    assert decision.first_failure == "DRAWDOWN_GATE"


def test_trend_break_exits_immediately_without_holding_day_check():
    facts = PositionFacts(
        current_state=PositionState.VALID_STRONG,
        current_quantity=1000,
        approved_target_quantity=2000,
        add_count=0,
        average_cost=Decimal("10"),
        last_price=Decimal("10.3"),
        current_protective_stop=Decimal("9.8"),
        proposed_protective_stop=Decimal("9.9"),
        trend_valid=False,
    )
    decision = evaluate_position(facts)
    assert decision.next_state == PositionState.BROKEN
    assert decision.action == IntentAction.EXIT
    assert decision.target_quantity == 0


def test_strong_profitable_short_trade_can_continue_and_add_once():
    facts = PositionFacts(
        current_state=PositionState.VALID,
        current_quantity=1000,
        approved_target_quantity=2000,
        add_count=0,
        average_cost=Decimal("10"),
        last_price=Decimal("10.8"),
        current_protective_stop=Decimal("9.8"),
        proposed_protective_stop=Decimal("10.1"),
        trend_strong=True,
        trend_valid=True,
    )
    decision = evaluate_position(facts)
    assert decision.next_state == PositionState.VALID_STRONG
    assert decision.action == IntentAction.ADD
    assert decision.target_quantity == 2000
    assert decision.protective_stop == Decimal("10.1")


def test_losing_position_is_never_added():
    facts = PositionFacts(
        current_state=PositionState.VALID,
        current_quantity=1000,
        approved_target_quantity=2000,
        add_count=0,
        average_cost=Decimal("10"),
        last_price=Decimal("9.9"),
        current_protective_stop=Decimal("9.5"),
        proposed_protective_stop=Decimal("9.4"),
        trend_strong=True,
    )
    decision = evaluate_position(facts)
    assert decision.action == IntentAction.HOLD
    assert decision.protective_stop == Decimal("9.5")


def test_protective_stop_never_moves_down():
    assert monotonic_protective_stop(
        Decimal("10.25"), Decimal("9.80")
    ) == Decimal("10.25")


def _quote(now, **overrides):
    data = {
        "stock_code": "000001",
        "event_id": "quote-1",
        "quote_at": now,
        "received_at": now,
        "bid1": Decimal("9.99"),
        "bid1_volume": 10000,
        "ask1": Decimal("10.00"),
        "ask1_volume": 10000,
        "last_price": Decimal("9.995"),
        "upper_limit": Decimal("11.00"),
        "lower_limit": Decimal("9.00"),
    }
    data.update(overrides)
    return Quote(**data)


def test_matcher_never_uses_last_price_when_bid_ask_missing():
    now = datetime(2026, 7, 27, 9, 31)
    result = PaperMatcher(_confirmed_policy()).match(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=1000,
        limit_price=Decimal("10.10"),
        quote=_quote(now, ask1=None, ask1_volume=None),
        now=now,
        tick_size=Decimal("0.01"),
        liquidity_quantity=1000,
    )
    assert result.status == "WAITING"
    assert result.waiting_reason == "WAIT_NO_QUOTE"
    assert result.fill_quantity == 0


def test_matcher_blocks_stale_quote_and_limit_lock():
    now = datetime(2026, 7, 27, 9, 31)
    matcher = PaperMatcher(_confirmed_policy())
    stale = matcher.match(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=1000,
        limit_price=Decimal("10.10"),
        quote=_quote(now - timedelta(seconds=16)),
        now=now,
        tick_size=Decimal("0.01"),
        liquidity_quantity=1000,
    )
    locked = matcher.match(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=1000,
        limit_price=Decimal("11.00"),
        quote=_quote(now, ask1=Decimal("11.00"), upper_limit=Decimal("11.00")),
        now=now,
        tick_size=Decimal("0.01"),
        liquidity_quantity=1000,
    )
    assert stale.waiting_reason == "WAIT_STALE_QUOTE"
    assert locked.waiting_reason == "WAIT_LIMIT_LOCK"


def test_matcher_caps_fill_at_twenty_percent_of_visible_level1():
    now = datetime(2026, 7, 27, 9, 31)
    result = PaperMatcher(_confirmed_policy()).match(
        side=OrderSide.BUY,
        remaining_quantity=5000,
        approved_remaining_quantity=5000,
        limit_price=Decimal("10.10"),
        quote=_quote(now, ask1_volume=2000),
        now=now,
        tick_size=Decimal("0.01"),
        liquidity_quantity=5000,
    )
    assert result.status == "PARTIALLY_FILLED"
    assert result.fill_quantity == 400


def test_paper_snapshot_matcher_applies_adverse_frozen_slippage():
    now = datetime(2026, 7, 27, 9, 31)
    result = PaperSnapshotMatcher(_confirmed_policy()).match(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=1000,
        limit_price=Decimal("10.10"),
        quote=_quote(
            now,
            bid1=None,
            bid1_volume=None,
            ask1=None,
            ask1_volume=None,
            last_price=Decimal("10.00"),
        ),
        now=now,
        tick_size=Decimal("0.01"),
        liquidity_quantity=600,
    )
    assert result.status == "PARTIALLY_FILLED"
    assert result.fill_quantity == 600
    assert result.fill_price == Decimal("10.01")
    assert "not a Level-1 or broker fill" in result.explanation


def test_paper_snapshot_matcher_rejects_stale_and_limit_locked_price():
    now = datetime(2026, 7, 27, 9, 31)
    matcher = PaperSnapshotMatcher(_confirmed_policy())
    stale = matcher.match(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=1000,
        limit_price=Decimal("10.10"),
        quote=_quote(
            now - timedelta(seconds=181),
            bid1=None,
            bid1_volume=None,
            ask1=None,
            ask1_volume=None,
            last_price=Decimal("10.00"),
        ),
        now=now,
        tick_size=Decimal("0.01"),
        liquidity_quantity=1000,
    )
    locked = matcher.match(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=1000,
        limit_price=Decimal("11.00"),
        quote=_quote(
            now,
            bid1=None,
            bid1_volume=None,
            ask1=None,
            ask1_volume=None,
            last_price=Decimal("11.00"),
            upper_limit=Decimal("11.00"),
        ),
        now=now,
        tick_size=Decimal("0.01"),
        liquidity_quantity=1000,
    )
    assert stale.waiting_reason == "WAIT_STALE_QUOTE"
    assert locked.waiting_reason == "WAIT_LIMIT_LOCK"


def test_exit_pending_t1_never_resurrects_after_price_recovery():
    facts = PositionFacts(
        current_state=PositionState.EXIT_PENDING_T1,
        current_quantity=1000,
        approved_target_quantity=1000,
        add_count=0,
        average_cost=Decimal("10"),
        last_price=Decimal("10"),
        current_protective_stop=Decimal("9"),
        proposed_protective_stop=Decimal("9"),
        trend_strong=True,
    )
    decision = evaluate_position(facts)
    assert decision.next_state == PositionState.EXIT_PENDING_T1
    assert decision.action == IntentAction.EXIT
    assert decision.target_quantity == 0


@pytest.mark.parametrize(
    "current_state",
    [
        PositionState.OPENING,
        PositionState.VALID_STRONG,
        PositionState.VALID,
        PositionState.WEAKENED,
    ],
)
def test_active_position_can_commit_directly_to_t1_exit_wait(
    current_state,
):
    facts = PositionFacts(
        current_state=current_state,
        current_quantity=1000,
        approved_target_quantity=1000,
        add_count=0,
        average_cost=Decimal("10"),
        last_price=Decimal("9"),
        current_protective_stop=Decimal("9.50"),
        proposed_protective_stop=Decimal("9.50"),
        invalidated=True,
        can_sell_today=False,
    )
    decision = evaluate_position(facts)
    assert decision.next_state == PositionState.EXIT_PENDING_T1
    assert decision.action == IntentAction.EXIT
    assert decision.target_quantity == 0
    assert decision.reason_code == "EXIT_BLOCKED_T1"


@pytest.mark.parametrize(
    "current_state",
    [
        PositionState.OPENING,
        PositionState.VALID_STRONG,
        PositionState.VALID,
        PositionState.WEAKENED,
    ],
)
def test_active_position_can_commit_directly_to_liquidity_exit_wait(
    current_state,
):
    facts = PositionFacts(
        current_state=current_state,
        current_quantity=1000,
        approved_target_quantity=1000,
        add_count=0,
        average_cost=Decimal("10"),
        last_price=Decimal("9"),
        current_protective_stop=Decimal("9.50"),
        proposed_protective_stop=Decimal("9.50"),
        risk_event=True,
        liquidity_available=False,
    )
    decision = evaluate_position(facts)
    assert decision.next_state == PositionState.EXIT_PENDING_LIQUIDITY
    assert decision.action == IntentAction.EXIT
    assert decision.target_quantity == 0
    assert decision.reason_code == "EXIT_BLOCKED_LIQUIDITY"


def test_add_risk_uses_raised_protective_stop_not_original_stop():
    decision = RiskAdjudicator(_confirmed_policy()).adjudicate(
        _intent(
            action=IntentAction.ADD,
            current_quantity=1000,
            target_quantity=2000,
            worst_price=Decimal("10.80"),
            initial_stop=Decimal("9.50"),
            protective_stop=Decimal("10.10"),
        ),
        _account(
            available_cash=Decimal("189000"),
            current_market_value=Decimal("10800"),
            current_open_risk=Decimal("700"),
            position_count=1,
            theme_position_counts={"bank": 1},
            theme_market_values={"bank": Decimal("10800")},
        ),
        _rule(),
        market_regime="TREND_UP",
        current_stock_market_value=Decimal("10800"),
    )
    assert decision.status == RiskDecisionStatus.APPROVED
    assert decision.approved_quantity == 1000
    assert decision.trade_risk == Decimal("700.00")


def _fee_profile():
    return FeeProfile(
        version="research-fee-v1",
        buy_commission_rate=Decimal("0.0003"),
        sell_commission_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5"),
        stamp_tax_sell_rate=Decimal("0.0005"),
    )


def test_guojin_confirmed_fee_math_for_a_share_and_etf():
    stock = FeeProfile(
        version="guojin_fee_v2.0.0",
        buy_commission_rate=Decimal("0.0001"),
        sell_commission_rate=Decimal("0.0001"),
        minimum_commission=Decimal("5"),
        stamp_tax_sell_rate=Decimal("0.0005"),
        transfer_fee_buy_rate=Decimal("0.00001"),
        transfer_fee_sell_rate=Decimal("0.00001"),
    )
    etf = replace(
        stock,
        stamp_tax_sell_rate=Decimal("0"),
        transfer_fee_buy_rate=Decimal("0"),
        transfer_fee_sell_rate=Decimal("0"),
    )

    assert stock.calculate(OrderSide.BUY, Decimal("10000")) == Decimal(
        "5.10"
    )
    assert stock.calculate(OrderSide.SELL, Decimal("10000")) == Decimal(
        "10.10"
    )
    assert stock.calculate(OrderSide.BUY, Decimal("100000")) == Decimal(
        "11.00"
    )
    assert stock.calculate(OrderSide.SELL, Decimal("100000")) == Decimal(
        "61.00"
    )
    assert etf.calculate(OrderSide.BUY, Decimal("10000")) == Decimal(
        "5.00"
    )
    assert etf.calculate(OrderSide.SELL, Decimal("10000")) == Decimal(
        "5.00"
    )


def test_partial_fills_charge_order_minimum_commission_only_once():
    profile = _fee_profile()
    first = profile.calculate_incremental(
        OrderSide.BUY,
        previous_gross=Decimal("0"),
        fill_gross=Decimal("1000"),
        fill_quantity=100,
    )
    second = profile.calculate_incremental(
        OrderSide.BUY,
        previous_gross=Decimal("1000"),
        fill_gross=Decimal("1000"),
        previous_quantity=100,
        fill_quantity=100,
    )
    assert first == Decimal("5.00")
    assert second == Decimal("0.00")
    assert first + second == profile.calculate(
        OrderSide.BUY,
        Decimal("2000"),
        quantity=200,
    )


def test_ledger_buy_sell_t1_and_reconciliation():
    book = LedgerBook(initial_cash=Decimal("200000"))
    buy_date = date(2026, 7, 27)
    book.apply_fill(
        fill_id="fill-buy-1",
        idempotency_key="fill-key-buy-1",
        order_id="order-buy-1",
        stock_code="000001",
        side=OrderSide.BUY,
        quantity=1000,
        price=Decimal("10"),
        trade_date=buy_date,
        filled_at=datetime(2026, 7, 27, 9, 31),
        fee_profile=_fee_profile(),
        settlement_date=date(2026, 7, 28),
    )
    assert book.cash_balance == Decimal("189995.00")
    assert book.available_to_sell("000001", buy_date) == 0
    with pytest.raises(ValueError, match="T\\+N"):
        book.apply_fill(
            fill_id="fill-sell-too-early",
            idempotency_key="fill-key-sell-too-early",
            order_id="order-sell-too-early",
            stock_code="000001",
            side=OrderSide.SELL,
            quantity=1000,
            price=Decimal("10.50"),
            trade_date=buy_date,
            filled_at=datetime(2026, 7, 27, 14, 30),
            fee_profile=_fee_profile(),
            settlement_date=buy_date,
        )
    book.apply_fill(
        fill_id="fill-sell-1",
        idempotency_key="fill-key-sell-1",
        order_id="order-sell-1",
        stock_code="000001",
        side=OrderSide.SELL,
        quantity=1000,
        price=Decimal("10.50"),
        trade_date=date(2026, 7, 28),
        filled_at=datetime(2026, 7, 28, 9, 31),
        fee_profile=_fee_profile(),
        settlement_date=date(2026, 7, 28),
    )
    assert book.position_quantity("000001") == 0
    assert book.reconcile()["status"] == "PASS"


def test_account_open_date_accepts_database_datetime_and_iso_text():
    from server.trading_v2.reconciliation import _account_open_date

    expected = date(2026, 7, 25)
    assert _account_open_date(datetime(2026, 7, 25, 22, 3, 18)) == expected
    assert _account_open_date("2026-07-25 22:03:18") == expected


def test_duplicate_fill_is_idempotent():
    book = LedgerBook(initial_cash=Decimal("200000"))
    kwargs = {
        "fill_id": "fill-buy-1",
        "idempotency_key": "same-fill-key",
        "order_id": "order-buy-1",
        "stock_code": "000001",
        "side": OrderSide.BUY,
        "quantity": 1000,
        "price": Decimal("10"),
        "trade_date": date(2026, 7, 27),
        "filled_at": datetime(2026, 7, 27, 9, 31),
        "fee_profile": _fee_profile(),
        "settlement_date": date(2026, 7, 28),
    }
    first = book.apply_fill(**kwargs)
    second = book.apply_fill(**kwargs)
    assert first == second
    assert len(book.fills) == 1
    assert book.cash_balance == Decimal("189995.00")


def test_order_and_fill_idempotency_keys_are_exact_and_stable():
    one = order_idempotency_key(
        account_id="paper-main-v2",
        decision_run_uid="run-1",
        intent_id="intent-1",
        stock_code="000001",
        side="BUY",
        target_quantity=1000,
        intent_version=1,
    )
    two = order_idempotency_key(
        account_id="paper-main-v2",
        decision_run_uid="run-1",
        intent_id="intent-1",
        stock_code="000001",
        side="BUY",
        target_quantity=1000,
        intent_version=1,
    )
    assert one == two
    assert len(one) == 64
    assert fill_idempotency_key(
        order_id="order-1",
        quote_event_id="quote-1",
        match_event_id="tick-1",
    ) == fill_idempotency_key(
        order_id="order-1",
        quote_event_id="quote-1",
        match_event_id="tick-1",
    )


def test_terminal_order_state_cannot_reopen():
    with pytest.raises(ValueError, match="illegal order transition"):
        transition_order(OrderStatus.FILLED, OrderStatus.QUEUED)


def test_market_regime_fails_closed_when_any_required_input_is_missing():
    result = classify_market_regime(
        {
            "risk_score": 20,
            "market_change_pct": 1,
            "breadth_pct": 60,
            "trend_score": 75,
            "switch_score": None,
        }
    )
    assert result.final_state == "DATA_BLOCKED"
    assert result.input_quality == "BLOCK"


def test_market_regime_priority_and_exact_states():
    extreme = classify_market_regime(
        {
            "risk_score": 85,
            "market_change_pct": -0.5,
            "breadth_pct": 70,
            "trend_score": 80,
            "switch_score": 80,
        }
    )
    trend = classify_market_regime(
        {
            "risk_score": 30,
            "market_change_pct": 1.2,
            "breadth_pct": 65,
            "trend_score": 75,
            "switch_score": 30,
        },
        previous_state="RANGE",
        previous_state_days=3,
        previous_candidate_state="TREND_UP",
        previous_candidate_streak=1,
    )
    rotation = classify_market_regime(
        {
            "risk_score": 30,
            "market_change_pct": 0.2,
            "breadth_pct": 50,
            "trend_score": 60,
            "switch_score": 70,
        },
        previous_state="RANGE",
        previous_state_days=3,
        previous_candidate_state="THEME_ROTATION",
        previous_candidate_streak=1,
    )
    assert extreme.final_state == "EXTREME"
    assert trend.final_state == "TREND_UP"
    assert rotation.final_state == "THEME_ROTATION"


def test_source_artifact_version_is_frozen_sha256():
    version, kind = code_version()
    assert len(version) >= 7
    assert kind in {"git_commit", "source_artifact_sha256"}


def test_daily_matcher_uses_next_open_and_blocks_limit_lock():
    matcher = HistoricalDailyMatcher()
    locked = DailyBar(
        event_id="bar-lock",
        open=Decimal("11.00"),
        high=Decimal("11.00"),
        low=Decimal("11.00"),
        close=Decimal("11.00"),
        volume=100000,
        upper_limit=Decimal("11.00"),
        lower_limit=Decimal("9.00"),
    )
    result = matcher.match_open(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=1000,
        limit_price=Decimal("11.00"),
        bar=locked,
        tick_size=Decimal("0.01"),
    )
    assert result.status == "WAITING"
    assert result.waiting_reason == "WAIT_LIMIT_LOCK"

    tradable = DailyBar(
        event_id="bar-open",
        open=Decimal("10.01"),
        high=Decimal("10.50"),
        low=Decimal("9.80"),
        close=Decimal("10.20"),
        volume=100000,
        upper_limit=Decimal("11.00"),
        lower_limit=Decimal("9.00"),
    )
    filled = matcher.match_open(
        side=OrderSide.BUY,
        remaining_quantity=1000,
        approved_remaining_quantity=800,
        limit_price=Decimal("10.10"),
        bar=tradable,
        tick_size=Decimal("0.01"),
        slippage_rate=Decimal("0.001"),
    )
    assert filled.status == "PARTIALLY_FILLED"
    assert filled.fill_quantity == 800
    assert filled.fill_price == Decimal("10.03")


def test_daily_bar_stop_wins_when_stop_and_target_are_both_touched():
    event = HistoricalDailyMatcher.resolve_long_exit(
        bar=DailyBar(
            event_id="bar-both",
            open=Decimal("10.00"),
            high=Decimal("11.20"),
            low=Decimal("9.20"),
            close=Decimal("10.80"),
            volume=100000,
            upper_limit=Decimal("11.50"),
            lower_limit=Decimal("8.50"),
        ),
        protective_stop=Decimal("9.50"),
        target_price=Decimal("11.00"),
        tick_size=Decimal("0.01"),
    )
    assert event.triggered is True
    assert event.reason_code == "STOP_BEFORE_TARGET_CONSERVATIVE"
    assert event.price == Decimal("9.50")


def test_qmt_level1_quote_event_preserves_opposing_prices_and_sizes():
    event = build_quote_event(
        {
            "stock_code": "000001",
            "source_time": datetime(2026, 7, 24, 10, 0, 0),
            "received_at": datetime(2026, 7, 24, 10, 0, 1),
            "bid1": 10.0,
            "bid1_volume": 1200,
            "ask1": 10.01,
            "ask1_volume": 1300,
            "price": 10.0,
            "pre_close": 9.9,
            "data_source": "gj_big_qmt_inner",
            "batch_id": "batch-1",
            "stock_status": 0,
        }
    )
    assert event is not None
    assert event["bid1"] == Decimal("10.0")
    assert event["bid1_volume"] == 1200
    assert event["ask1"] == Decimal("10.01")
    assert event["ask1_volume"] == 1300
    assert event["suspended"] is False


def test_qmt_level1_quote_event_identity_ignores_transport_metadata():
    row = {
        "stock_code": "000001",
        "source_time": datetime(2026, 7, 24, 10, 0, 0),
        "received_at": datetime(2026, 7, 24, 10, 0, 1),
        "bid1": 10.0,
        "bid1_volume": 1200,
        "ask1": 10.01,
        "ask1_volume": 1300,
        "price": 10.0,
        "pre_close": 9.9,
        "data_source": "gj_big_qmt_inner",
        "stock_status": 0,
    }
    first = build_quote_event({**row, "batch_id": "batch-1"})
    repeated = build_quote_event(
        {
            **row,
            "received_at": datetime(2026, 7, 24, 10, 1, 15),
            "batch_id": "batch-2",
        }
    )

    assert first is not None
    assert repeated is not None
    assert first["quote_event_id"] == repeated["quote_event_id"]
    assert first["received_at"] != repeated["received_at"]


def test_qmt_level1_quote_event_treats_non_finite_book_values_as_missing():
    event = build_quote_event(
        {
            "stock_code": "000001",
            "source_time": datetime(2026, 7, 24, 10, 0, 0),
            "received_at": datetime(2026, 7, 24, 10, 0, 1),
            "bid1": float("nan"),
            "bid1_volume": float("nan"),
            "ask1": float("inf"),
            "ask1_volume": float("inf"),
            "price": 10.0,
            "pre_close": 9.9,
            "data_source": "gj_big_qmt_inner",
            "batch_id": "batch-non-finite",
        }
    )

    assert event is not None
    assert event["bid1"] is None
    assert event["bid1_volume"] is None
    assert event["ask1"] is None
    assert event["ask1_volume"] is None


def test_trade_metrics_keep_undefined_values_null_and_use_exact_pnl():
    empty = trade_metrics([], max_drawdown=Decimal("0"))
    assert empty["expectancy_cny"] is None
    assert empty["profit_factor"] is None
    assert empty["payoff_ratio"] is None

    metrics = trade_metrics(
        [
            CompletedTrade(
                "t1",
                "000001",
                Decimal("300"),
                Decimal("100"),
            ),
            CompletedTrade(
                "t2",
                "000002",
                Decimal("-100"),
                Decimal("100"),
            ),
        ],
        max_drawdown=Decimal("0.05"),
    )
    assert metrics["expectancy_cny"] == "100"
    assert metrics["expectancy_r"] == "1"
    assert metrics["profit_factor"] == "3"
    assert metrics["payoff_ratio"] == "3"


def test_oos_gate_requires_full_history_stress_and_robustness():
    metrics = {
        "completed_trade_count": 120,
        "expectancy_cny": "100",
        "expectancy_r": "0.2",
        "profit_factor": "1.5",
        "payoff_ratio": "1.8",
        "max_drawdown": "0.10",
        "maximum_single_security_profit_contribution": "0.20",
    }
    passed = evaluate_oos_gate(
        security_scope="A_SHARE",
        trading_days=500,
        oos_windows=120,
        metrics=metrics,
        doubled_cost_metrics={
            "expectancy_cny": "20",
            "profit_factor": "1.2",
        },
        remove_best_three_net_pnl=Decimal("1"),
        robustness={
            "complete": True,
            "block_bootstrap_paths": 2000,
            "positive_parameter_neighborhood_ratio": "0.61",
        },
        future_data_violations=0,
        impossible_fill_profit=Decimal("0"),
    )
    assert passed["status"] == "PASS"

    blocked = evaluate_oos_gate(
        security_scope="A_SHARE",
        trading_days=499,
        oos_windows=119,
        metrics=metrics,
        doubled_cost_metrics={
            "expectancy_cny": "20",
            "profit_factor": "1.2",
        },
        remove_best_three_net_pnl=Decimal("1"),
        robustness={
            "complete": False,
            "block_bootstrap_paths": 0,
            "positive_parameter_neighborhood_ratio": "0",
        },
        future_data_violations=0,
        impossible_fill_profit=Decimal("0"),
    )
    assert blocked["status"] == "BLOCK"
