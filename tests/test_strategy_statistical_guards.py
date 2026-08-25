from __future__ import annotations

import math
import random
from datetime import date, timedelta

from server.engine.strategy_statistical_guards import (
    benjamini_yekutieli_fdr,
    newey_west_nav_statistics,
    spaced_consecutive_gate_confirmations,
)


def _nav_records(values: list[float]) -> list[dict[str, object]]:
    start = date(2025, 1, 1)
    return [
        {
            "trade_date": (start + timedelta(days=index)).isoformat(),
            "return_pct": value,
        }
        for index, value in enumerate(values)
    ]


def _assert_no_order_authority(result: dict[str, object]) -> None:
    assert result["automatic_real_order_submission"] is False
    assert result["real_order_authority"] is False
    assert len(str(result["result_hash"])) == 64


def test_iid_strong_positive_nav_passes_hac_confidence_bounds():
    generator = random.Random(123)
    values = [generator.gauss(0.8, 1.2) for _ in range(240)]

    result = newey_west_nav_statistics(_nav_records(values))

    assert result["valid"] is True
    assert result["passed"] is True
    assert result["parameters"]["resolved_bandwidth"] == math.floor(
        math.sqrt(len(values))
    )
    assert result["net_expectancy_one_sided_95_lcb_pct"] > 0
    assert result["net_expectancy_one_sided_p_value"] < 0.05
    assert result["profit_factor"]["one_sided_95_lcb"] > 1
    assert result["payoff_ratio"]["one_sided_95_lcb"] > 1
    assert 1 <= result["effective_sample_size"] <= len(values)
    assert len(result["input_hash"]) == 64
    assert len(result["parameter_hash"]) == 64
    _assert_no_order_authority(result)


def test_positive_autocorrelation_reduces_ess_and_expectancy_lcb():
    independent_like = [1.0] * 100 + [-0.5] * 100
    random.Random(9).shuffle(independent_like)
    autocorrelated = [1.0] * 100 + [-0.5] * 100

    independent_result = newey_west_nav_statistics(
        _nav_records(independent_like)
    )
    autocorrelated_result = newey_west_nav_statistics(
        _nav_records(autocorrelated)
    )

    assert independent_result["valid"] is True
    assert autocorrelated_result["valid"] is True
    assert independent_result["net_expectancy_pct"] == (
        autocorrelated_result["net_expectancy_pct"]
    )
    assert autocorrelated_result["effective_sample_size"] < (
        independent_result["effective_sample_size"]
    )
    assert autocorrelated_result["net_expectancy_one_sided_95_lcb_pct"] < (
        independent_result["net_expectancy_one_sided_95_lcb_pct"]
    )
    _assert_no_order_authority(autocorrelated_result)


def test_point_profit_factor_above_one_can_fail_log_lcb():
    result = newey_west_nav_statistics(
        _nav_records([1.1, -1.0] * 40)
    )

    assert result["valid"] is True
    assert result["profit_factor"]["estimate"] > 1
    assert result["profit_factor"]["one_sided_95_lcb"] < 1
    assert result["profit_factor"]["one_sided_p_value_vs_one"] > 0.05
    assert result["passed"] is False
    _assert_no_order_authority(result)


def test_nonfinite_or_one_sided_daily_samples_fail_closed():
    nonfinite = newey_west_nav_statistics(
        _nav_records([1.0, -0.5, float("nan"), -0.25])
    )
    all_wins = newey_west_nav_statistics(_nav_records([0.2] * 20))

    assert nonfinite["valid"] is False
    assert nonfinite["passed"] is False
    assert all_wins["valid"] is False
    assert all_wins["passed"] is False
    assert "negative" in all_wins["reason"]
    _assert_no_order_authority(nonfinite)
    _assert_no_order_authority(all_wins)


def test_by_fdr_tightens_with_total_trial_inventory():
    small = benjamini_yekutieli_fdr(
        {"candidate_a": 0.01, "candidate_b": 0.5},
        total_hypotheses=2,
        trial_inventory=["candidate_a", "candidate_b"],
    )
    large_inventory = ["candidate_a", "candidate_b"] + [
        f"unreported_{index:02d}" for index in range(18)
    ]
    large = benjamini_yekutieli_fdr(
        {"candidate_a": 0.01, "candidate_b": 0.5},
        total_hypotheses=20,
        trial_inventory=large_inventory,
    )

    assert small["valid"] is True
    assert large["valid"] is True
    assert next(
        item for item in small["decisions"] if item["key"] == "candidate_a"
    )["passed"] is True
    assert next(
        item for item in large["decisions"] if item["key"] == "candidate_a"
    )["passed"] is False
    assert small["trial_inventory_hash"] != large["trial_inventory_hash"]
    _assert_no_order_authority(large)


def test_by_fdr_ties_have_stable_key_order_and_hashes():
    first = benjamini_yekutieli_fdr(
        {"zeta": 0.01, "alpha": 0.01},
        total_hypotheses=2,
        trial_inventory=["zeta", "alpha"],
    )
    second = benjamini_yekutieli_fdr(
        {"alpha": 0.01, "zeta": 0.01},
        total_hypotheses=2,
        trial_inventory=["alpha", "zeta"],
    )

    assert first == second
    decisions = {item["key"]: item for item in first["decisions"]}
    assert decisions["alpha"]["rank"] == 1
    assert decisions["zeta"]["rank"] == 2
    _assert_no_order_authority(first)


def test_by_fdr_rejects_keys_that_collide_after_stable_normalization():
    result = benjamini_yekutieli_fdr(
        {1: 0.01, "1": 0.02},
        total_hypotheses=2,
        trial_inventory=["1", "another_trial"],
    )

    assert result["valid"] is False
    assert result["passed"] is False
    assert "collide" in result["reason"]
    _assert_no_order_authority(result)


def _sessions(count: int) -> list[str]:
    latest = date(2026, 8, 24)
    return [(latest - timedelta(days=index)).isoformat() for index in range(count)]


def _confirmation(day: str, index: int, *, passed: bool = True) -> dict[str, object]:
    return {
        "trade_date": day,
        "passed": passed,
        "funding_gate_hash": f"{index + 1:064x}",
        "evidence_revision_at": f"{day}T15:00:00",
    }


def test_adjacent_120_day_rolling_update_is_not_a_new_confirmation():
    sessions = _sessions(2)
    result = spaced_consecutive_gate_confirmations(
        _confirmation(sessions[0], 0),
        [_confirmation(sessions[1], 1)],
        sessions,
        minimum_new_sessions=20,
        required_total_confirmations=2,
    )

    assert result["valid"] is True
    assert result["passed"] is False
    assert result["prior_confirmation_count"] == 0
    assert result["continuous_session_count"] == 2
    _assert_no_order_authority(result)


def test_twenty_new_sessions_can_form_one_prior_milestone():
    sessions = _sessions(21)
    result = spaced_consecutive_gate_confirmations(
        _confirmation(sessions[0], 0),
        [
            _confirmation(day, index)
            for index, day in enumerate(sessions[1:], 1)
        ],
        sessions,
        minimum_new_sessions=20,
        required_total_confirmations=2,
    )

    assert result["valid"] is True
    assert result["passed"] is True
    assert result["prior_confirmation_count"] == 1
    assert result["milestones"][-1]["session_index"] == 20
    _assert_no_order_authority(result)


def test_missing_session_or_reused_hash_fails_closed():
    sessions = _sessions(21)
    missing = spaced_consecutive_gate_confirmations(
        _confirmation(sessions[0], 0),
        [
            _confirmation(day, index)
            for index, day in enumerate(sessions[1:], 1)
            if index != 7
        ],
        sessions,
        minimum_new_sessions=20,
        required_total_confirmations=2,
    )
    duplicate_history = [
        _confirmation(day, index)
        for index, day in enumerate(sessions[1:], 1)
    ]
    duplicate_history[5]["funding_gate_hash"] = duplicate_history[4][
        "funding_gate_hash"
    ]
    duplicate_hash = spaced_consecutive_gate_confirmations(
        _confirmation(sessions[0], 0),
        duplicate_history,
        sessions,
        minimum_new_sessions=20,
        required_total_confirmations=2,
    )

    assert missing["valid"] is False
    assert missing["prior_confirmation_count"] == 0
    assert duplicate_hash["valid"] is False
    assert duplicate_hash["prior_confirmation_count"] == 0
    _assert_no_order_authority(missing)
    _assert_no_order_authority(duplicate_hash)
