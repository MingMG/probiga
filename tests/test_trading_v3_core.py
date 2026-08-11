from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from server.db.migrations_v3 import MIGRATIONS as V3_MIGRATIONS
from server.trading_v3.calibration import fit_calibration
from server.trading_v3.engine import TradingV3Engine
from server.trading_v3.metrics import trade_metrics
from server.trading_v3.portfolio import estimate_roundtrip_cost_pct
from server.trading_v3.regime import classify_regime_probabilities
from server.trading_v3.sleeves import (
    intraday_surprise,
    low_base_ignition,
    oversold_reversal,
    right_side_trend,
    theme_diffusion,
)
from server.trading_v3.config import load_v3_config


NOW = datetime(2026, 7, 28, 15, 0)


def test_v3_migration_restores_real_trading_database_guards():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"]
        == "20260729_002_restore_real_trading_hard_guard"
    )
    sql = "\n".join(migration["statements"])
    assert "real_trading_enabled = 0" in sql
    assert "trg_trade_account_v2_real_disabled_bi" in sql
    assert "trg_trade_account_v2_real_disabled_bu" in sql


def test_v3_migration_blocks_real_execution_plans():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"]
        == "20260801_001_block_real_execution_plans"
    )
    sql = "\n".join(migration["statements"])
    assert "real_order_allowed = 0" in sql
    assert "trg_execution_plan_v3_real_disabled_bi" in sql
    assert "trg_execution_plan_v3_real_disabled_bu" in sql


def test_v3_migration_repairs_strategy_level_counterfactual_attribution():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"]
        == "20260801_002_repair_counterfactual_attribution"
    )
    sql = "\n".join(migration["statements"])
    assert "JSON_CONTAINS" in sql
    assert "c.accepted = 0" in sql
    assert "c.false_positive = 0" in sql


def test_v3_migration_creates_counterfactual_retry_queue():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"]
        == "20260801_006_counterfactual_backlog_queue"
    )
    sql = "\n".join(migration["statements"])
    assert "st_counterfactual_queue_v3" in sql
    assert "attempt_count" in sql
    assert "next_retry_at" in sql


def test_v3_migration_hard_separates_executed_and_shadow_evidence():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"]
        == "20260802_001_shadow_portfolio_evidence_isolation"
    )
    sql = "\n".join(migration["statements"])
    assert "st_shadow_portfolio_v3" in sql
    assert "uk_v3_counterfactual_forecast" in sql
    assert "NEW.evidence_kind <> 'EXECUTED_PAPER'" in sql
    assert "NEW.evidence_kind <> 'SHADOW'" in sql
    assert "NEW.order_allowed <> 0" in sql
    assert "NEW.can_activate_model <> 0" in sql


def test_v3_migration_adds_independent_generic_theme_signal_ledger():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"]
        == "20260802_002_generic_theme_signal_ledger"
    )
    sql = "\n".join(migration["statements"])
    assert "st_theme_signal_v3" in sql
    assert "theme_feature_key" in sql
    assert "source_theme_signal_id" in sql
    assert "theme shadow source signal is invalid" in sql


def test_v3_migration_adds_news_knowledge_time_boundary():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"]
        == "20260802_003_news_point_in_time_knowledge"
    )
    sql = "\n".join(migration["statements"])
    assert "first_seen_at" in sql
    assert "COALESCE(etl_sync_at, publish_time)" in sql
    assert "idx_news_publish_first_seen" in sql
    assert "trg_news_first_seen_bi" in sql
    assert "OLD.first_seen_at" in sql


def test_v3_actual_paper_position_cap_is_configurable_without_lowering_oos_gate():
    config = load_v3_config()
    assert config["portfolio"]["maximum_positions"] == 12
    assert config["paper_execution"]["maximum_live_positions"] == 12
    assert config["paper_discovery"]["maximum_positions"] == 10
    assert config["profit_gate"]["minimum_portfolio_trades"] == 80
    assert config["profit_gate"]["minimum_profit_factor"] == 1.30


def _trend_features():
    return {
        "return_5d_pct": 8.0,
        "return_20d_pct": 18.0,
        "return_60d_pct": 35.0,
        "ma20_slope_5d_pct": 4.0,
        "breakout_20d_proximity": 0.95,
        "amount_ratio_5_20": 1.4,
        "relative_strength_20d_pct": 10.0,
        "distance_ma20_pct": 6.0,
        "close_above_ma20": 1.0,
        "ma20_above_ma60": 1.0,
        "atr_14d_pct": 2.2,
        "latest_change_pct": 2.0,
        "price": 25.0,
        "latest_amount": 100_000_000.0,
        "average_amount_20d": 100_000_000.0,
        "latest_tradable": 1.0,
        "entry_eligible": 1.0,
        "market_return_20d_pct": 5.0,
        "sector_relative_return_pct": 2.0,
        "sector_breadth_pct": 65.0,
        "sector_breadth_acceleration_pct": 8.0,
        "sector_amount_acceleration_pct": 25.0,
        "stock_leadership_score": 0.75,
        "leadership_quality": 0.75,
        "theme_opportunity_score": 0.80,
        "sector_crowding": 0.25,
        "theme_code": "创新药",
    }


def _market_features():
    return {
        "market_return_20d_pct": 5.0,
        "market_breadth_pct": 68.0,
        "breadth_change_5d_pct": 8.0,
        "realized_volatility_20d_pct": 1.8,
        "limit_down_ratio_pct": 0.1,
        "sector_concentration_pct": 20.0,
    }


def _oversold_features():
    features = _trend_features()
    features.update({
        "return_2d_pct": 4.0,
        "return_5d_pct": -5.0,
        "return_20d_pct": -25.0,
        "return_60d_pct": -18.0,
        "drawdown_20d_pct": -28.0,
        "distance_ma20_pct": -12.0,
        "distance_ma5_pct": 2.0,
        "ma20_slope_5d_pct": -5.0,
        "latest_change_pct": 5.0,
        "previous_change_pct": -4.0,
        "amount_ratio_1_20": 2.0,
        "rebound_from_low_pct": 4.0,
        "market_latest_change_pct": 1.0,
        "latest_relative_to_market_pct": 4.0,
        "atr_14d_pct": 4.0,
        "sector_relative_return_pct": 4.0,
        "sector_breadth_pct": 60.0,
        "theme_opportunity_score": 0.80,
        "stock_leadership_score": 0.50,
        "theme_code": "光通信",
    })
    return features


def test_v3_uncalibrated_score_cannot_enter_portfolio():
    signal = right_side_trend(
        "002326",
        "永太科技",
        _trend_features(),
        NOW,
        NOW + timedelta(days=10),
    )
    forecast = TradingV3Engine().forecast(signal)
    assert forecast.status == "RESEARCH_ONLY_UNCALIBRATED"
    assert forecast.expected_return_net_pct is None


def test_v3_right_side_rejects_setup_outside_backtest_universe():
    features = _trend_features()
    features["distance_ma20_pct"] = 8.01

    signal = right_side_trend(
        "002326",
        "sample",
        features,
        NOW,
        NOW + timedelta(days=10),
    )

    assert signal.status == "SETUP_NOT_READY"


def test_v3_right_side_accepts_setup_inside_backtest_universe():
    signal = right_side_trend(
        "002326",
        "sample",
        _trend_features(),
        NOW,
        NOW + timedelta(days=10),
    )

    assert signal.status == "SCORED"


def test_v3_forecast_uses_readable_theme_name_instead_of_numeric_code():
    features = _trend_features()
    features.update({
        "theme_code": "760000",
        "theme_name": "环保设备",
    })
    signal = right_side_trend(
        "300929",
        "华骐环保",
        features,
        NOW,
        NOW + timedelta(days=10),
    )
    assert signal.theme_code == "环保设备"


def test_v3_weak_market_theme_is_watch_only():
    features = _trend_features()
    features["market_return_20d_pct"] = -1.9
    signal = theme_diffusion(
        "002326",
        "永太科技",
        features,
        NOW,
        NOW + timedelta(days=10),
    )
    assert signal.status == "WEAK_MARKET_THEME_WATCH"


def test_v3_low_base_ignition_catches_sector_led_early_turn():
    features = _trend_features()
    features.update({
        "market_return_20d_pct": -2.5,
        "return_5d_pct": -1.44,
        "return_20d_pct": -25.49,
        "return_60d_pct": -12.76,
        "ma20_slope_5d_pct": -8.95,
        "distance_ma20_pct": -7.62,
        "latest_change_pct": 3.012,
        "amount_ratio_5_20": 0.833,
        "latest_amount": 1_027_041_360.0,
        "average_amount_20d": 602_846_561.5,
        "atr_14d_pct": 5.305,
        "breakout_20d_proximity": 0.729,
        "close_above_ma20": 0.0,
        "ma20_above_ma60": 0.0,
        "sector_breadth_pct": 59.26,
        "sector_breadth_acceleration_pct": 19.91,
        "sector_relative_return_pct": 7.39,
        "sector_amount_acceleration_pct": 49.79,
        "theme_opportunity_score": 0.776,
        "sector_crowding": 0.35,
        "stock_leadership_score": 0.05,
        "stock_relative_to_theme_5d_pct": -3.28,
        "theme_code": "SW1公用事业",
    })
    signal = low_base_ignition(
        "000767",
        "晋控电力",
        features,
        NOW,
        NOW + timedelta(days=10),
    )
    assert signal.status == "SCORED"
    assert signal.strategy_key == "low_base_ignition"
    assert signal.score >= 0.70


def test_v3_low_base_ignition_rejects_stock_without_sector_capital():
    features = _trend_features()
    features.update({
        "return_20d_pct": -20.0,
        "return_60d_pct": -10.0,
        "distance_ma20_pct": -6.0,
        "latest_change_pct": 3.0,
        "amount_ratio_5_20": 0.9,
        "latest_amount": 120_000_000.0,
        "average_amount_20d": 100_000_000.0,
        "atr_14d_pct": 4.0,
        "breakout_20d_proximity": 0.75,
        "sector_breadth_pct": 48.0,
        "sector_breadth_acceleration_pct": 2.0,
        "sector_relative_return_pct": 0.5,
        "sector_amount_acceleration_pct": 5.0,
        "theme_opportunity_score": 0.35,
        "sector_crowding": 0.35,
        "stock_relative_to_theme_5d_pct": -2.0,
    })
    signal = low_base_ignition(
        "000767",
        "晋控电力",
        features,
        NOW,
        NOW + timedelta(days=10),
    )
    assert signal.status == "SETUP_NOT_READY"


def test_v3_trend_sleeve_blocks_weak_sector_in_weak_market():
    features = _trend_features()
    features.update({
        "market_return_20d_pct": -3.0,
        "sector_relative_return_pct": 0.2,
        "sector_breadth_pct": 46.0,
        "sector_breadth_acceleration_pct": -4.0,
        "sector_amount_acceleration_pct": -8.0,
        "stock_leadership_score": 0.25,
    })
    signal = right_side_trend(
        "002326",
        "永太科技",
        features,
        NOW,
        NOW + timedelta(days=10),
    )
    assert signal.status == "MARKET_REGIME_BLOCKED"


def test_v3_positive_calibration_can_enter_retail_portfolio():
    samples = [
        {
            "score": 0.72 + bucket * 0.02,
            "net_return_pct": 2.4 if index < 70 else -1.0,
            "mae_pct": -1.2,
            "mfe_pct": 3.5,
        }
        for bucket in range(5)
        for index in range(100)
    ]
    table = fit_calibration(
        "right_side_trend",
        samples,
        model_version="right_side_trend.v3.4.1-test-v1",
        bucket_count=5,
    )
    engine = TradingV3Engine({"right_side_trend": table})
    signal = right_side_trend(
        "002326",
        "永太科技",
        _trend_features(),
        NOW,
        NOW + timedelta(days=10),
    )
    forecast = engine.forecast(signal)
    assert forecast.status == "VALIDATED_POSITIVE"
    result = engine.decide(
        [forecast],
        market_features=_market_features(),
        prices={"002326": 25.0},
        equity=200_000.0,
    )
    assert result["portfolio"]["targets"]
    assert result["portfolio"]["targets"][0]["target_quantity"] % 100 == 0
    assert result["portfolio"]["status"] == "CASH_OR_ETF_PREFERRED"


def test_v3_failed_profit_factor_stays_research_only():
    samples = [
        {
            "score": 0.72 + bucket * 0.02,
            "net_return_pct": 1.0 if index < 52 else -1.0,
            "mae_pct": -1.0,
            "mfe_pct": 1.0,
        }
        for bucket in range(5)
        for index in range(100)
    ]
    table = fit_calibration(
        "right_side_trend",
        samples,
        model_version="right_side_trend.v3.4.1-test-bad",
        bucket_count=5,
    )
    signal = right_side_trend(
        "002326",
        "永太科技",
        _trend_features(),
        NOW,
        NOW + timedelta(days=10),
    )
    forecast = TradingV3Engine(
        {"right_side_trend": table}
    ).forecast(signal)
    assert forecast.status == "RESEARCH_ONLY_PROFIT_GATE_FAILED"


def test_v3_old_formula_calibration_is_never_reused():
    samples = [
        {
            "score": 0.9,
            "net_return_pct": 3.0,
            "mae_pct": -1.0,
            "mfe_pct": 4.0,
        }
        for _ in range(240)
    ]
    old_table = fit_calibration(
        "right_side_trend",
        samples,
        model_version="right_side_trend.v3.0.1-old",
        bucket_count=1,
    )
    signal = right_side_trend(
        "002326",
        "永太科技",
        _trend_features(),
        NOW,
        NOW + timedelta(days=10),
    )
    forecast = TradingV3Engine({
        "right_side_trend": old_table
    }).forecast(signal)
    assert forecast.status == "RESEARCH_ONLY_MODEL_VERSION_MISMATCH"
    assert forecast.expected_return_net_pct is None


def test_v3_theme_trend_research_requires_explicit_paper_enable():
    features = _trend_features()
    features.update({
        "sector_breadth_pct": 85.0,
        "sector_breadth_acceleration_pct": 20.0,
        "sector_relative_return_pct": 5.0,
        "sector_amount_acceleration_pct": 60.0,
        "stock_leadership_score": 1.0,
        "leadership_quality": 1.0,
        "theme_opportunity_score": 1.0,
        "sector_crowding": 0.2,
        "event_surprise": 1.0,
    })
    engine = TradingV3Engine()
    forecasts = engine.evaluate_stock(
        "002326",
        "永太科技",
        features,
        NOW,
        NOW + timedelta(days=10),
    )
    blocked = engine.decide(
        forecasts,
        market_features=_market_features(),
        prices={"002326": 25.0},
        equity=200_000.0,
        allow_paper_discovery=False,
    )
    assert not blocked["portfolio"]["targets"]
    paper = engine.decide(
        forecasts,
        market_features=_market_features(),
        prices={"002326": 25.0},
        equity=200_000.0,
        allow_paper_discovery=True,
    )
    assert len(paper["portfolio"]["targets"]) == 1
    assert paper["portfolio"]["status"] == "PAPER_DISCOVERY_READY"
    assert "paper_discovery" in paper["portfolio"]["targets"][0][
        "strategy_keys"
    ]


def test_v3_oversold_reversal_has_prepare_stage_without_buying():
    features = _oversold_features()
    features.update({
        "latest_change_pct": -2.0,
        "amount_ratio_1_20": 0.75,
        "rebound_from_low_pct": 0.4,
        "latest_relative_to_market_pct": -1.0,
    })
    signal = oversold_reversal(
        "600522",
        "中天科技",
        features,
        NOW,
        NOW + timedelta(days=10),
    )
    assert signal.status == "LEFT_SIDE_PREPARE"


def test_v3_oversold_reversal_can_open_only_a_small_paper_probe():
    signal = oversold_reversal(
        "300308",
        "中际旭创",
        _oversold_features(),
        NOW,
        NOW + timedelta(days=10),
    )
    assert signal.status == "SCORED"
    assert signal.score >= 0.68
    forecast = TradingV3Engine().forecast(signal)
    assert forecast.status == "PAPER_DISCOVERY_CANDIDATE"
    result = TradingV3Engine().decide(
        [forecast],
        market_features=_market_features(),
        prices={"300308": 25.0},
        equity=200_000.0,
        allow_paper_discovery=True,
    )
    target = result["portfolio"]["targets"][0]
    assert target["stock_code"] == "300308"
    assert target["target_weight"] <= 0.025
    assert target["strategy_keys"] == (
        "oversold_reversal",
        "paper_discovery",
    )
    assert result["portfolio"]["status"] == "PAPER_DISCOVERY_READY"


def test_v3_active_oversold_probe_emits_hold_target_without_turnover():
    forecast = TradingV3Engine().forecast(oversold_reversal(
        "300308",
        "涓檯鏃垱",
        _oversold_features(),
        NOW,
        NOW + timedelta(days=10),
    ))
    result = TradingV3Engine().decide(
        [forecast],
        market_features=_market_features(),
        prices={"300308": 25.0},
        equity=200_000.0,
        current_position_weights={"300308": 0.025},
        current_position_themes={"300308": ("鍏夐€氫俊",)},
        current_paper_discovery_codes={"300308"},
        allow_paper_discovery=True,
    )
    target = result["portfolio"]["targets"][0]
    assert target["stock_code"] == "300308"
    assert target["target_weight"] == 0.025
    assert target["target_quantity"] == 200
    assert target["estimated_roundtrip_cost_pct"] == 0.0
    assert result["portfolio"]["estimated_one_way_turnover_weight"] == 0.0


def test_v3_failed_forward_samples_tighten_score_and_size_not_stock_count():
    first = TradingV3Engine().forecast(oversold_reversal(
        "300308",
        "中际旭创",
        _oversold_features(),
        NOW,
        NOW + timedelta(days=10),
    ))
    second = TradingV3Engine().forecast(oversold_reversal(
        "300502",
        "新易盛",
        _oversold_features(),
        NOW,
        NOW + timedelta(days=10),
    ))
    result = TradingV3Engine().decide(
        [first, second],
        market_features=_market_features(),
        prices={"300308": 25.0, "300502": 25.0},
        equity=200_000.0,
        allow_paper_discovery=True,
        paper_discovery_learning={
            "accepted_count": 10,
            "profit_factor": 0.6,
            "average_net_return_pct": -1.5,
        },
    )
    assert len(result["portfolio"]["targets"]) <= 2
    if result["portfolio"]["targets"]:
        assert "本轮信号阈值0.86" in result["portfolio"]["targets"][0][
            "reason"
        ]
        assert result["portfolio"]["targets"][0]["target_weight"] <= 0.015


def test_v3_rejects_calibration_when_higher_scores_lose():
    table = fit_calibration(
        "right_side_trend",
        [
            {
                "score": 0.70 + index / 1000,
                "net_return_pct": 2.0 if index < 100 else -2.0,
                "mae_pct": -1.0,
                "mfe_pct": 3.0,
            }
                for index in range(500)
            ],
        model_version="right_side_trend.v3.4.1-inverted",
        bucket_count=5,
    )
    assert table.has_valid_score_direction() is False
    signal = right_side_trend(
        "002326",
        "永太科技",
        _trend_features(),
        NOW,
        NOW + timedelta(days=10),
    )
    forecast = TradingV3Engine({
        "right_side_trend": table,
    }).forecast(signal)
    assert (
        forecast.status
        == "RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED"
    )


def test_v3_calibration_pools_local_direction_noise_without_lowering_gate():
    outcomes = (-1.4, 0.9, 0.5, 3.3, -1.0)
    table = fit_calibration(
        "right_side_trend",
        [
            {
                "score": 0.60 + bucket * 0.05 + index / 100_000,
                "net_return_pct": outcome,
                "mae_pct": -2.0,
                "mfe_pct": 4.0,
            }
            for bucket, outcome in enumerate(outcomes)
            for index in range(100)
        ],
        model_version="right_side_trend.v3.4.2-monotonic-test",
        bucket_count=5,
    )

    assert len(table.buckets) == 3
    assert table.has_valid_score_direction() is True
    assert [item.sample_count for item in table.buckets] == [100, 200, 200]
    assert table.buckets[-1].expected_return_net_pct > 0


def test_v3_monotonic_pooling_does_not_rescue_fully_inverted_score():
    table = fit_calibration(
        "right_side_trend",
        [
            {
                "score": 0.60 + bucket * 0.05 + index / 100_000,
                "net_return_pct": 3.0 - bucket,
                "mae_pct": -2.0,
                "mfe_pct": 4.0,
            }
            for bucket in range(5)
            for index in range(100)
        ],
        model_version="right_side_trend.v3.4.2-inverted-test",
        bucket_count=5,
    )

    assert len(table.buckets) == 1
    assert table.has_valid_score_direction() is False


def test_v3_right_side_contract_binds_calibration_protocol(monkeypatch):
    from server.trading_v3 import right_side_policy

    config = load_v3_config()
    original = right_side_policy.right_side_model_contract_hash(config)
    monkeypatch.setattr(
        right_side_policy,
        "CALIBRATION_PROTOCOL",
        "DIFFERENT_CALIBRATION_PROTOCOL",
    )

    assert right_side_policy.right_side_model_contract_hash(config) != original


def test_v3_regime_is_probability_mixture():
    result = classify_regime_probabilities(_market_features())
    assert result.quality_status == "PASS"
    assert abs(sum(result.probabilities.values()) - 1.0) < 1e-6
    assert 0 < result.risk_asset_cap < 1
    assert result.dominant_state in result.probabilities


def test_v3_regime_blocks_sparse_market_cross_section():
    features = _market_features()
    features.update({
        "market_eligible_stock_count": 4024.0,
        "market_latest_coverage_ratio": 0.62,
        "market_tradable_coverage_ratio": 0.60,
    })
    result = classify_regime_probabilities(features)
    assert result.quality_status == "BLOCK"
    assert result.risk_asset_cap == 0.0
    assert any(
        item.startswith("MARKET_LATEST_COVERAGE_")
        for item in result.evidence
    )


def test_v3_regime_blocks_stale_qmt_attestation():
    features = _market_features()
    features.update({
        "qmt_attestation_current": False,
        "qmt_attestation_status": "BLOCKED_SOURCE_INCOMPLETE",
    })
    result = classify_regime_probabilities(features)
    assert result.quality_status == "BLOCK"
    assert result.risk_asset_cap == 0.0
    assert (
        "QMT_DAILY_KLINE_ATTESTATION_BLOCKED_SOURCE_INCOMPLETE"
        in result.evidence
    )


def test_v3_minimum_commission_is_in_roundtrip_cost():
    cost = estimate_roundtrip_cost_pct(
        4_000,
        commission_rate=0.0001,
        minimum_commission=5.0,
        transfer_fee_rate=0.00001,
        sell_stamp_duty_rate=0.0005,
        slippage_rate=0.0005,
    )
    assert cost > 0.40


def test_v3_trade_metrics_distinguish_payoff_and_profit_factor():
    metrics = trade_metrics([8, -4, -4, 8])
    assert metrics["net_expectancy_pct"] == 2
    assert metrics["payoff_ratio"] == 2
    assert metrics["profit_factor"] == 2


def test_intraday_signal_cannot_remain_actionable_for_a_month():
    signal = intraday_surprise(
        "002326",
        "永太科技",
        {
            "intraday_amount_surprise_z": 3.0,
            "price_vs_vwap_pct": 1.2,
            "interval_return_pct": 2.0,
            "sector_breadth_pct": 70.0,
            "sector_relative_return_pct": 1.0,
            "fill_probability": 0.8,
            "spread_bps": 12.0,
        },
        NOW,
        NOW + timedelta(days=30),
    )
    assert signal.horizon_days == 1
    assert signal.valid_until == NOW + timedelta(days=3)


def test_trend_signal_keeps_a_longer_but_bounded_validity_window():
    signal = right_side_trend(
        "002326",
        "永太科技",
        _trend_features(),
        NOW,
        NOW + timedelta(days=30),
    )
    assert signal.horizon_days == 10
    assert signal.valid_until == NOW + timedelta(days=18)


def test_backtest_never_fills_zero_amount_suspension_bars():
    source = (
        Path(__file__).resolve().parents[1]
        / "server"
        / "trading_v3"
        / "backtest.py"
    ).read_text(encoding="utf-8")
    assert 'day_bars.loc[code]["amount"]' in source
    assert 'row["amount"]' in source
