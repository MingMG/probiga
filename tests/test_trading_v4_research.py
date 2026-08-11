from __future__ import annotations

import math
import json
from copy import deepcopy
from pathlib import Path

import pandas as pd

from server.trading_v3.research_v4 import (
    _rolling_percentile,
    attach_point_in_time_finance,
    bounded_signal_outcome,
    classify_research_regime,
    feature_availability_report,
    fit_hurdle_return_model,
    fit_regime_expert_model,
    fit_ridge_return_model,
    portfolio_capacity_training_rows,
    predict_hurdle_return,
    predict_regime_expert_return,
    predict_ridge_return,
    prediction_to_score,
    select_top_per_day,
)
from server.trading_v3.config import load_v3_config
from server.trading_v3.regime import classify_regime_probabilities
from tools.research_trading_v4_ml_campaign import (
    PIT_FINANCE_FEATURES,
    _build_candidate_bases,
)


def test_rolling_percentile_uses_only_prior_252_observations():
    increasing = pd.Series(range(254), dtype=float)
    ranked = _rolling_percentile(increasing)
    assert ranked.iloc[:252].isna().all()
    assert ranked.iloc[252] == 1.0
    assert ranked.iloc[253] == 1.0

    shocked = pd.Series([*range(252), -1.0], dtype=float)
    ranked_shock = _rolling_percentile(shocked)
    assert ranked_shock.iloc[252] == 0.0


def test_select_top_per_day_is_deterministic_on_stock_code():
    day = pd.Timestamp("2026-01-05")
    frame = pd.DataFrame({
        "trade_date": [day, day, day],
        "score": [0.8, 0.8, 0.7],
        "stock_code": ["000002", "000001", "000003"],
    })
    selected = select_top_per_day(frame, top_per_day=2)
    assert selected["stock_code"].tolist() == ["000001", "000002"]


def test_finance_ranking_excludes_future_and_invalid_notice_rows():
    signal_day = pd.Timestamp("2024-06-03")
    market = pd.DataFrame({
        "trade_date": [signal_day, signal_day],
        "stock_code": ["000001", "000002"],
        "amount20": [100_000_000.0, 100_000_000.0],
        "amount": [10_000_000.0, 10_000_000.0],
        "raw_close": [10.0, 12.0],
        "name_excluded": [0, 0],
        "change_pct": [1.0, 1.0],
    })
    reversal = market.iloc[[0]].copy()
    finance = pd.DataFrame([
        {
            "id": 1,
            "stock_code": "000001",
            "report_date": "2024-03-31",
            "notice_date": "2024-04-30",
            "net_asset_ps": 5.0,
            "oper_cf_ps": 1.0,
            "net_profit_yoy_gr": 10.0,
            "roe_wtd": 12.0,
            "gross_margin": 20.0,
            "net_margin": 8.0,
            "cash_flow_ratio": 5.0,
            "asset_liab_ratio": 40.0,
        },
        {
            "id": 2,
            "stock_code": "000002",
            "report_date": "2024-09-30",
            "notice_date": "1900-01-01",
            "net_asset_ps": 6.0,
            "oper_cf_ps": 2.0,
            "net_profit_yoy_gr": 20.0,
            "roe_wtd": 20.0,
            "gross_margin": 30.0,
            "net_margin": 10.0,
            "cash_flow_ratio": 8.0,
            "asset_liab_ratio": 30.0,
        },
    ])
    enriched = attach_point_in_time_finance(
        reversal,
        market_frame=market,
        finance_rows=finance,
    )
    assert len(enriched) == 1
    assert math.isclose(float(enriched.iloc[0]["quality_percentile"]), 1.0)
    assert float(enriched.iloc[0]["asset_liab_ratio_pit"]) == 40.0


def test_multi_sleeve_builder_attaches_pit_finance_to_every_candidate():
    signal_day = pd.Timestamp("2026-01-05")
    market = pd.DataFrame({
        "trade_date": [signal_day, signal_day],
        "stock_code": ["000001", "000002"],
        "amount20": [100_000_000.0, 120_000_000.0],
        "amount": [20_000_000.0, 30_000_000.0],
        "raw_close": [10.0, 12.0],
        "name_excluded": [0, 0],
        "change_pct": [1.0, 1.0],
    })
    trend = market.iloc[[0]].assign(
        score=0.8,
        candidate_id="rs_hpb_no_health_v1",
        exit_sleeve="trend",
    )
    reversal = market.iloc[[1]].assign(
        score=0.9,
        candidate_id="nvcr_price_reversal_v1",
        exit_sleeve="reversal",
        drawdown_20d_pct=-20.0,
        reversal_confirmation_score=0.8,
        market_repair_score=0.7,
    )
    finance = pd.DataFrame([
        {
            "id": 1, "stock_code": "000001", "report_date": "2025-09-30",
            "notice_date": "2025-10-31", "net_asset_ps": 4.0,
            "oper_cf_ps": 0.5, "net_profit_yoy_gr": 5.0, "roe_wtd": 8.0,
            "gross_margin": 15.0, "net_margin": 5.0, "cash_flow_ratio": 3.0,
            "asset_liab_ratio": 50.0,
        },
        {
            "id": 2, "stock_code": "000002", "report_date": "2025-09-30",
            "notice_date": "2025-10-31", "net_asset_ps": 8.0,
            "oper_cf_ps": 2.0, "net_profit_yoy_gr": 20.0, "roe_wtd": 18.0,
            "gross_margin": 30.0, "net_margin": 12.0, "cash_flow_ratio": 9.0,
            "asset_liab_ratio": 30.0,
        },
    ])
    candidates = [
        {
            "id": "candidate_a",
            "sleeve": "trend",
            "base_universe_id": "rs_hpb_no_health_v1",
        },
        {
            "id": "candidate_b",
            "sleeve": "reversal",
            "base_universe_id": "qfbr_quality_reversal_v1",
        },
    ]
    bases, reports = _build_candidate_bases(
        candidates=candidates,
        universes={
            "rs_hpb_no_health_v1": trend,
            "nvcr_price_reversal_v1": reversal,
        },
        market_frame=market,
        finance_rows=finance,
        top_per_day=50,
    )

    assert set(bases) == {"candidate_a", "candidate_b"}
    assert bases["candidate_a"]["exit_sleeve"].tolist() == ["trend"]
    assert bases["candidate_b"]["exit_sleeve"].tolist() == ["reversal"]
    for candidate_id, base in bases.items():
        assert set(PIT_FINANCE_FEATURES).issubset(base.columns)
        assert base[list(PIT_FINANCE_FEATURES)].notna().all().all()
        assert base["research_candidate_id"].eq(candidate_id).all()
        report = reports[candidate_id]
        assert report["exit_sleeve_preserved"] is True
        assert report["declaration_source"] == "base_universe_id"
        assert all(
            value == 1.0
            for value in report["point_in_time_finance"]
            ["coverage_before_train_fold_gate"].values()
        )
        assert report["historical_context"] == {
            "concept_membership_added": False,
            "news_features_added": False,
            "reason": "NO_VERIFIED_POINT_IN_TIME_HISTORY",
        }


def test_v6_multi_sleeve_campaign_is_exploratory_and_non_activating():
    campaign_path = (
        Path(__file__).resolve().parents[1]
        / "strategies"
        / "trading_v6_multi_sleeve_pit_finance_campaign.json"
    )
    campaign = json.loads(campaign_path.read_text(encoding="utf-8"))
    candidates = campaign["candidate_control"]["candidates"]

    assert 2 <= len(candidates) <= 3
    assert campaign["candidate_control"]["maximum_new_candidate_count"] <= 3
    assert {item["sleeve"] for item in candidates} == {"trend", "reversal"}
    assert all(item.get("base_universe_id") for item in candidates)
    assert campaign["execution_boundary"]["real_order_submission"] is False
    assert campaign["evidence_status"]["campaign_mode"] == "EXPLORATORY_ONLY"
    assert campaign["evidence_status"]["historical_pass_cannot_activate_model"] is True
    assert campaign["evidence_status"][
        "all_history_through_2026_07_31_is_contaminated_by_prior_inspection"
    ] is True
    assert campaign["profit_gate"]["minimum_portfolio_profit_factor"] == 1.3
    assert campaign["profit_gate"]["minimum_positive_outer_folds"] == 4
    assert set(PIT_FINANCE_FEATURES).issubset(
        campaign["predictor_protocol"]["features"]
    )
    assert campaign["predictor_protocol"]["feature_availability"] == (
        "TRAINING_FOLD_ONLY_FAIL_CLOSED"
    )
    assert campaign["research_scope"]["historical_concept_membership"].startswith(
        "DISABLED"
    )
    assert campaign["research_scope"]["historical_news_features"].startswith(
        "DISABLED"
    )


def test_ridge_model_learns_order_without_external_dependency():
    frame = pd.DataFrame({
        "x": [-2.0, -1.0, 0.0, 1.0, 2.0],
        "net_return_pct": [-4.0, -2.0, 0.0, 2.0, 4.0],
    })
    model = fit_ridge_return_model(
        frame,
        features=["x"],
        ridge_lambda=0.1,
        target_clip=(-12.0, 20.0),
    )
    prediction = predict_ridge_return(model, frame)
    assert list(prediction) == sorted(prediction)
    scores = prediction_to_score(prediction)
    assert (scores > 0).all() and (scores < 1).all()
    assert list(scores) == sorted(scores)


def test_hurdle_model_separates_win_probability_and_payoff_magnitude():
    values = [float(index - 60) / 10.0 for index in range(120)]
    returns = [
        (1.0 + value * 0.8) if value >= 0 else (-1.5 + value * 0.35)
        for value in values
    ]
    frame = pd.DataFrame({"x": values, "net_return_pct": returns})
    model = fit_hurdle_return_model(
        frame,
        features=["x"],
        ridge_lambda=1.0,
        target_clip=(-12.0, 20.0),
        minimum_feature_coverage=1.0,
    )
    prediction = predict_hurdle_return(model, frame)
    assert model.positive_sample_count > 20
    assert model.negative_sample_count > 20
    assert prediction[0] < prediction[-1]
    assert model.as_dict()["protocol"] == "POINT_IN_TIME_HURDLE_RIDGE_V1"


def test_feature_gate_drops_all_nan_and_low_coverage_columns():
    frame = pd.DataFrame({
        "good": [1.0, 2.0, 3.0, 4.0, 5.0],
        "sparse": [1.0, None, None, None, None],
        "empty": [None, None, None, None, None],
        "net_return_pct": [-2.0, -1.0, 0.0, 1.0, 2.0],
    })
    report = feature_availability_report(
        frame,
        ["good", "sparse", "empty", "missing"],
        minimum_coverage=0.8,
    )
    assert report["accepted"] == ["good"]
    assert report["dropped"] == {
        "sparse": "TRAINING_COVERAGE_TOO_LOW",
        "empty": "NO_FINITE_TRAINING_VALUE",
        "missing": "MISSING_COLUMN",
    }
    model = fit_ridge_return_model(
        frame,
        features=["good", "sparse", "empty"],
        ridge_lambda=1.0,
        target_clip=(-12.0, 20.0),
        minimum_feature_coverage=0.8,
    )
    assert model.features == ("good",)
    assert model.dropped_features == ("sparse", "empty")


def test_regime_experts_route_each_row_to_its_train_only_model():
    frame = pd.DataFrame({
        "x": [-3.0, -2.0, -1.0, 1.0, 2.0, 3.0],
        "research_regime": [
            "RISK_OFF", "RISK_OFF", "RISK_OFF",
            "TREND_UP", "TREND_UP", "TREND_UP",
        ],
        "net_return_pct": [-3.0, -2.0, -1.0, 1.0, 2.0, 3.0],
    })
    model = fit_regime_expert_model(
        frame,
        features=["x"],
        ridge_lambda=0.1,
        target_clip=(-12.0, 20.0),
        minimum_regime_samples=3,
        minimum_feature_coverage=1.0,
    )
    prediction = predict_regime_expert_return(model, frame)
    assert set(dict(model.experts)) == {"RISK_OFF", "TREND_UP"}
    assert prediction[0] < prediction[2] < prediction[3] < prediction[5]


def test_market_regime_and_capacity_filter_are_deterministic():
    market = pd.DataFrame({
        "market_return_20d_pct": [5.0, 8.0, -8.0, -10.0],
        "market_breadth_pct": [60.0, 30.0, 50.0, 15.0],
        "breadth_change_5d_pct": [1.0, 0.0, 20.0, -5.0],
        "realized_volatility_20d_pct": [1.0, 2.0, 2.0, 4.0],
        "limit_down_ratio_pct": [0.0, 0.0, 0.0, 2.0],
    })
    assert classify_research_regime(market).astype(str).tolist() == [
        "TREND_UP", "THEME_ROTATION", "PANIC_RECOVERY", "RISK_OFF",
    ]
    production_states = [
        classify_regime_probabilities(row).dominant_state
        for row in market.to_dict(orient="records")
    ]
    assert production_states == classify_research_regime(market).astype(str).tolist()

    config = deepcopy(load_v3_config())
    config["portfolio"]["maximum_positions"] = 2
    labels = pd.DataFrame({
        "stock_code": ["000001", "000002", "000003"],
        "entry_date": pd.to_datetime(["2026-01-05"] * 3),
        "exit_date": pd.to_datetime(["2026-01-10"] * 3),
        "entry_open": [10.0, 10.0, 10.0],
        "score": [0.9, 0.8, 0.7],
        "research_regime": ["TREND_UP"] * 3,
    })
    selected = portfolio_capacity_training_rows(labels, config=config)
    assert selected["stock_code"].tolist() == ["000001", "000002"]
    assert set(selected["training_objective_protocol"]) == {
        "AFTER_COST_PRODUCTION_CONSTRAINT_PARITY_V2"
    }


def test_bounded_exit_uses_next_open_after_target_touch():
    dates = pd.date_range("2026-01-05", periods=4, freq="B")
    frame = pd.DataFrame([
        {"trade_date": dates[0], "raw_open": 10.0, "raw_high": 10.1, "raw_low": 9.9, "raw_close": 10.0, "amount": 1e8},
        {"trade_date": dates[1], "raw_open": 10.0, "raw_high": 10.3, "raw_low": 9.9, "raw_close": 10.2, "amount": 1e8},
        {"trade_date": dates[2], "raw_open": 10.2, "raw_high": 11.1, "raw_low": 10.1, "raw_close": 11.0, "amount": 1e8},
        {"trade_date": dates[3], "raw_open": 10.8, "raw_high": 10.9, "raw_low": 10.7, "raw_close": 10.8, "amount": 1e8},
    ])
    outcome = bounded_signal_outcome(
        frame,
        signal_index=0,
        config=load_v3_config(),
        stop_pct=5.0,
        take_profit_pct=10.0,
        maximum_holding_sessions=10,
        maximum_entry_gap_pct=1.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "BOUNDED_TARGET_TOUCH"
    assert outcome["exit_date"] == dates[3]
    assert outcome["exit_close"] == 10.8
