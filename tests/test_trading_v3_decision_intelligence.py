from __future__ import annotations

from datetime import datetime, timezone

import pytest

from server.trading_v3.decision_intelligence import (
    DecisionIntelligenceError,
    analyze_replacement_opportunities,
    diff_run_batches,
    optimize_advisory_portfolio,
)


UTC = timezone.utc


def test_batch_diff_reports_added_removed_and_material_field_changes():
    previous = {
        "run_uid": "run-old",
        "decision_as_of": "2026-08-14T07:00:00Z",
        "items": [
            {
                "forecast_id": "f-keep",
                "stock_code": "600001",
                "strategy_key": "t1",
                "horizon_days": 1,
                "selection_status": "WATCH",
                "target_weight": 0.0,
                "gate_codes": ["EDGE_LOW"],
            },
            {
                "forecast_id": "f-remove",
                "stock_code": "600002",
                "strategy_key": "t5",
                "horizon_days": 5,
                "selection_status": "REJECT",
            },
        ],
    }
    current = {
        "run_uid": "run-new",
        "decision_as_of": "2026-08-15T07:00:00Z",
        "items": [
            {
                "forecast_id": "f-keep",
                "stock_code": "600001",
                "strategy_key": "t1",
                "horizon_days": 1,
                "selection_status": "PAPER_ACTIONABLE",
                "target_weight": 0.05,
                "gate_codes": [],
            },
            {
                "forecast_id": "f-add",
                "stock_code": "600003",
                "strategy_key": "t20",
                "horizon_days": 20,
                "selection_status": "WATCH",
            },
        ],
    }

    result = diff_run_batches(previous, current)

    assert result["status"] == "CHANGED"
    assert [item["forecast_id"] for item in result["added"]] == ["f-add"]
    assert [item["forecast_id"] for item in result["removed"]] == ["f-remove"]
    assert result["summary"]["changed_count"] == 1
    assert result["summary"]["field_change_counts"] == {
        "gate_codes": 1,
        "selection_status": 1,
        "target_weight": 1,
    }
    assert result["decision_scope"] == "RESEARCH_ONLY"
    assert result["order_authority"] is False


def test_batch_diff_fails_closed_on_duplicate_or_time_reversal():
    duplicate = {
        "run_uid": "one",
        "decision_as_of": datetime(2026, 8, 15, tzinfo=UTC),
        "items": [
            {
                "forecast_id": "same",
                "stock_code": "1",
                "strategy_key": "x",
                "horizon_days": 1,
            },
            {
                "forecast_id": "same",
                "stock_code": "1",
                "strategy_key": "x",
                "horizon_days": 1,
            },
        ],
    }
    empty = {
        "run_uid": "two",
        "decision_as_of": datetime(2026, 8, 16, tzinfo=UTC),
        "items": [],
    }
    with pytest.raises(DecisionIntelligenceError, match="duplicate"):
        diff_run_batches(duplicate, empty)

    later = {**empty, "decision_as_of": "2026-08-17T00:00:00Z"}
    earlier = {**empty, "decision_as_of": "2026-08-16T00:00:00Z"}
    with pytest.raises(DecisionIntelligenceError, match="must not precede"):
        diff_run_batches(later, earlier)


def test_batch_diff_uses_stable_economic_key_not_per_run_forecast_uuid():
    previous = {
        "run_uid": "previous",
        "decision_as_of": "2026-08-14T07:00:00Z",
        "items": [
            {
                "forecast_id": "uuid-from-previous-run",
                "stock_code": "600001",
                "strategy_key": "t1",
                "horizon_days": 1,
                "selection_status": "WATCH",
            }
        ],
    }
    current = {
        "run_uid": "current",
        "decision_as_of": "2026-08-15T07:00:00Z",
        "items": [
            {
                "forecast_id": "different-uuid-from-current-run",
                "stock_code": "600001",
                "strategy_key": "t1",
                "horizon_days": 1,
                "selection_status": "TARGET",
            }
        ],
    }

    result = diff_run_batches(previous, current)

    assert result["summary"]["added_count"] == 0
    assert result["summary"]["removed_count"] == 0
    assert result["summary"]["changed_count"] == 1


def test_replacement_analysis_deducts_cost_and_enforces_t1_capacity_and_theme():
    holdings = [
        {
            "stock_code": "OLD-A",
            "current_weight": 0.10,
            "expected_return_gross_pct": 0.4,
            "exit_cost_pct": 0.1,
            "theme_codes": ["AI"],
            "sell_locked": False,
        },
        {
            "stock_code": "OLD-B",
            "current_weight": 0.10,
            "expected_return_gross_pct": -0.2,
            "exit_cost_pct": 0.1,
            "theme_codes": ["BANK"],
            "sell_locked": True,
        },
    ]
    candidates = [
        {
            "stock_code": "NEW",
            "expected_return_gross_pct": 2.0,
            "entry_cost_pct": 0.2,
            "exit_cost_pct": 0.2,
            "uncertainty_haircut_pct": 0.1,
            "average_daily_value_cny": 2_000_000,
            "theme_codes": ["AI"],
        }
    ]

    result = analyze_replacement_opportunities(
        candidates,
        holdings,
        equity_cny=1_000_000,
        maximum_participation_rate=0.05,
        capacity_sessions=1,
        maximum_theme_weight=0.15,
        minimum_incremental_net_edge_pct=0.5,
    )

    by_incumbent = {item["incumbent_code"]: item for item in result["options"]}
    assert by_incumbent["OLD-A"]["eligible"] is True
    assert "CANDIDATE_CAPACITY_TOO_LOW" not in by_incumbent["OLD-A"]["reason_codes"]
    assert "THEME_CONCENTRATION_CAP" not in by_incumbent["OLD-A"]["reason_codes"]
    assert by_incumbent["OLD-B"]["eligible"] is False
    assert "INCUMBENT_T1_SELL_LOCKED" in by_incumbent["OLD-B"]["reason_codes"]
    assert "CANDIDATE_CAPACITY_TOO_LOW" not in by_incumbent["OLD-B"]["reason_codes"]
    assert "THEME_CONCENTRATION_CAP" in by_incumbent["OLD-B"]["reason_codes"]
    assert result["order_authority"] is False


def test_replacement_edge_does_not_add_back_incumbent_liquidation_cost():
    result = analyze_replacement_opportunities(
        [{
            "stock_code": "NEW",
            "expected_return_gross_pct": 1.0,
            "entry_cost_pct": 0.1,
            "exit_cost_pct": 0.1,
            "uncertainty_haircut_pct": 0.0,
            "average_daily_value_cny": 10_000_000,
            "theme_codes": [],
        }],
        [{
            "stock_code": "OLD",
            "current_weight": 0.1,
            "expected_return_gross_pct": 0.5,
            "exit_cost_pct": 0.6,
            "uncertainty_haircut_pct": 0.0,
            "theme_codes": [],
            "sell_locked": False,
        }],
        equity_cny=1_000_000,
        maximum_participation_rate=0.05,
        capacity_sessions=1,
        maximum_theme_weight=0.5,
        minimum_incremental_net_edge_pct=0.5,
    )

    option = result["options"][0]
    assert option["candidate_forward_net_pct"] == 0.2
    assert option["incumbent_forward_net_pct"] == -0.1
    assert option["incremental_net_edge_pct"] == 0.3
    assert option["eligible"] is False
    assert "INCREMENTAL_NET_EDGE_TOO_LOW" in option["reason_codes"]


def _optimizer_policy() -> dict:
    return {
        "equity_cny": 1_000_000,
        "risk_asset_cap": 0.60,
        "maximum_positions": 4,
        "maximum_single_weight": 0.15,
        "maximum_theme_weight": 0.20,
        "maximum_cluster_weight": 0.25,
        "maximum_turnover_weight": 0.30,
        "maximum_participation_rate": 0.05,
        "capacity_sessions": 2,
        "minimum_order_cny": 5_000,
        "minimum_edge_to_cost_multiple": 3.0,
        "standard_trade_risk": 0.004,
        "board_lot": 100,
        "fees": {
            "commission_rate": 0.0001,
            "minimum_commission_cny": 5.0,
            "transfer_fee_rate": 0.00001,
            "sell_stamp_duty_rate": 0.0005,
            "default_slippage_rate": 0.0005,
        },
    }


def test_optimizer_is_capacity_cost_and_concentration_aware_and_advisory_only():
    positions = [
        {
            "stock_code": "HELD",
            "current_weight": 0.10,
            "theme_codes": ["AI"],
            "cluster_key": "GROWTH",
        }
    ]
    candidates = [
        {
            "stock_code": "GOOD",
            "stock_name": "Good",
            "selection_score": 0.95,
            "conservative_return_gross_pct": 2.0,
            "price": 20.0,
            "average_daily_value_cny": 10_000_000,
            "initial_stop_pct": -4.0,
            "desired_weight": 0.10,
            "theme_codes": ["AI"],
            "cluster_key": "GROWTH",
        },
        {
            "stock_code": "ILLQ",
            "selection_score": 0.90,
            "conservative_return_gross_pct": 3.0,
            "price": 50.0,
            "average_daily_value_cny": 10_000,
            "initial_stop_pct": 4.0,
            "desired_weight": 0.10,
            "theme_codes": ["OTHER"],
            "cluster_key": "OTHER",
        },
        {
            "stock_code": "NOEDGE",
            "selection_score": 0.80,
            "conservative_return_gross_pct": 0.05,
            "price": 10.0,
            "average_daily_value_cny": 10_000_000,
            "initial_stop_pct": 4.0,
            "desired_weight": 0.10,
            "theme_codes": ["OTHER"],
            "cluster_key": "OTHER",
        },
    ]

    result = optimize_advisory_portfolio(
        candidates,
        policy=_optimizer_policy(),
        current_positions=positions,
    )

    assert [item["stock_code"] for item in result["targets"]] == ["GOOD"]
    assert result["targets"][0]["target_weight"] <= 0.10
    assert result["targets"][0]["initial_stop_distance_pct"] == 4.0
    reasons = {item["stock_code"]: item["reason_code"] for item in result["rejected"]}
    assert reasons["ILLQ"] == "ORDER_NOT_ECONOMIC"
    assert reasons["NOEDGE"] == "NET_EDGE_BELOW_COST_BUFFER"
    assert result["execution_revalidation_required"] is True
    assert result["decision_scope"] == "RESEARCH_ONLY"
    assert result["order_authority"] is False


def test_optimizer_fails_closed_when_equity_is_missing():
    policy = _optimizer_policy()
    policy["equity_cny"] = 0
    with pytest.raises(DecisionIntelligenceError, match="equity_cny must be positive"):
        optimize_advisory_portfolio([], policy=policy)
