from server.trading_v2.bootstrap import _strategy_manifests
from server.trading_v2.multi_strategy_router import evaluate_signal_route
from server.trading_v2.planner import _candidate_competition_order_key
from server.engine.strategy_center import adapt_recommendation_row


def _signal(**overrides):
    signal = {
        "strategy_key": "short_term",
        "signal_direction": "BUY",
        "signal_status": "WATCH",
        "gate_status": "REDUCE",
        "gate_reason": "恐慌修复模式自动降权，需二次确认",
        "risk_level": "LOW",
        "raw_score": 84.0,
        "risk_reward_ratio": 3.6,
        "data_quality_score": 88.0,
        "effective_weight": 0.60,
        "market_only_downgrade": True,
    }
    signal.update(overrides)
    return signal


def test_panic_recovery_can_route_strict_market_reduced_signal():
    route = evaluate_signal_route(_signal(), "PANIC_RECOVERY")

    assert route["eligible"] is True
    assert route["opening_target_fraction"] == 0.20
    assert route["market_only_downgrade_accepted"] is True
    assert route["competition_score"] > 0


def test_market_reduced_watch_cannot_bypass_stock_specific_gate():
    route = evaluate_signal_route(
        _signal(
            market_only_downgrade=False,
            gate_reason="个股质量门槛未通过",
        ),
        "PANIC_RECOVERY",
    )

    assert route["eligible"] is False
    assert route["reason_code"] == "MULTI_STRATEGY_REDUCE_NOT_ROUTABLE"


def test_hard_event_risk_is_never_overridden():
    route = evaluate_signal_route(
        _signal(risk_level="HIGH", gate_status="PASS"),
        "TREND_UP",
    )

    assert route["eligible"] is False
    assert route["reason_code"] == "MULTI_STRATEGY_EVENT_RISK_BLOCK"


def test_risk_off_only_allows_exceptionally_strong_swing_signal():
    short_route = evaluate_signal_route(
        _signal(signal_status="READY", gate_status="PASS"),
        "RISK_OFF",
    )
    swing_route = evaluate_signal_route(
        _signal(
            strategy_key="swing",
            signal_status="WATCH",
            gate_status="REDUCE",
            raw_score=84.0,
            risk_reward_ratio=4.0,
            data_quality_score=90.0,
            effective_weight=0.8,
        ),
        "RISK_OFF",
    )

    assert short_route["eligible"] is False
    assert short_route["reason_code"] == "MULTI_STRATEGY_DISABLED_FOR_REGIME"
    assert swing_route["eligible"] is True
    assert swing_route["opening_target_fraction"] == 0.10


def test_extreme_market_blocks_every_new_buy():
    route = evaluate_signal_route(
        _signal(signal_status="READY", gate_status="PASS"),
        "EXTREME",
    )

    assert route["eligible"] is False
    assert route["reason_code"] == "MULTI_STRATEGY_REGIME_EXTREME"


def test_all_stock_profiles_are_registered_for_paper_trial():
    stock_manifests = [
        item
        for item in _strategy_manifests()
        if item["strategy_id"]
        in {"ultra_short", "short_term", "swing", "main_wave"}
    ]

    assert len(stock_manifests) == 4
    assert {
        item["strategy_version"].split(":")[0]
        for item in stock_manifests
    } == {"stock_strategy_v2.3.0"}
    assert {
        item["validation_protocol"]["status"]
        for item in stock_manifests
    } == {"PAPER_TRIAL"}


def test_competition_uses_regime_adjusted_score_before_raw_score():
    stronger_for_regime = {
        "stock_code": "000001",
        "strategy_version": "a",
        "expected_return_lower_bound": None,
        "competition_score": 70,
        "raw_score": 78,
        "risk_reward_ratio": 3,
    }
    higher_raw_but_weaker_for_regime = {
        "stock_code": "000002",
        "strategy_version": "b",
        "expected_return_lower_bound": None,
        "competition_score": 60,
        "raw_score": 90,
        "risk_reward_ratio": 4,
    }

    ranked = sorted(
        [higher_raw_but_weaker_for_regime, stronger_for_regime],
        key=_candidate_competition_order_key,
    )
    assert ranked[0]["stock_code"] == "000001"


def test_soft_macro_suspension_cannot_be_revived_by_strong_main_wave_signal():
    signal = adapt_recommendation_row(
        {
            "stock_code": "002326",
            "short_name": "永太科技",
            "pick_date": "2026-07-27",
            "main_wave_score": 86,
            "main_wave_signal": "BUY_READY",
            "signal_status": "WATCH",
            "recommend_status": "SUSPENDED",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
            "recommend_reason": "宏观环境偏弱，进入观察池",
            "event_risk_level": "LOW",
            "data_quality_score": 88,
            "data_quality_flags": '["macro_policy_pressure"]',
        },
        "main_wave",
        {"market_state": "risk_declining"},
    )

    assert signal["signal_direction"] == "HOLD"
    assert signal["signal_status"] == "BLOCKED"
    assert signal["gate_status"] == "BLOCK"
    assert signal["market_only_downgrade"] is False
    # The main-wave observation remains available for research/explanation,
    # but it cannot override an explicit recommendation/signal suspension.
    assert signal["raw_score"] == 86.0
    assert signal["source_recommend_status"] == "SUSPENDED"
    assert signal["source_signal_status"] == "WATCH"


def test_universal_hard_flag_still_blocks_strategy_specific_score():
    signal = adapt_recommendation_row(
        {
            "stock_code": "002326",
            "short_name": "永太科技",
            "pick_date": "2026-07-27",
            "main_wave_score": 90,
            "main_wave_signal": "BUY_READY",
            "signal_status": "WATCH",
            "recommend_status": "SUSPENDED",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
            "event_risk_level": "LOW",
            "data_quality_score": 90,
            "data_quality_flags": '["downtrend_clock"]',
        },
        "main_wave",
        {"market_state": "risk_declining"},
    )

    assert signal["signal_direction"] == "HOLD"
    assert signal["signal_status"] == "BLOCKED"
    assert signal["gate_status"] == "BLOCK"


def test_chase_watch_cannot_become_ready_from_a_high_strategy_score():
    signal = adapt_recommendation_row(
        {
            "stock_code": "603221",
            "short_name": "爱丽家居",
            "pick_date": "2026-08-04",
            "short_term_score": 99,
            "signal_status": "BUY_READY",
            "recommend_status": "SUSPENDED",
            "chase_risk_status": "WATCH",
            "ordinary_buy_eligible": False,
            "event_risk_level": "LOW",
            "data_quality_score": 99,
        },
        "short_term",
        {"market_state": "trend_bullish"},
    )

    assert signal["signal_direction"] == "HOLD"
    assert signal["signal_status"] == "BLOCKED"
    assert signal["gate_status"] == "BLOCK"


def test_sell_alert_is_not_suppressed_by_missing_new_buy_gate():
    signal = adapt_recommendation_row(
        {
            "stock_code": "600001",
            "short_name": "测试持仓",
            "pick_date": "2026-08-04",
            "short_term_score": 80,
            "signal_status": "SELL_ALERT",
            "recommend_status": "BLOCK",
            "event_risk_level": "CRITICAL",
        },
        "short_term",
        {"market_state": "extreme_event"},
    )

    assert signal["signal_direction"] == "SELL"
    assert signal["signal_status"] == "BLOCKED"
