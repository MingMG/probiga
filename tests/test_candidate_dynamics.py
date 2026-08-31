from server.trading_v3.candidate_dynamics import (
    build_strategy_execution_summary,
    enrich_candidate_dynamics,
)


def test_daily_dynamics_explain_continuity_and_related_candidates() -> None:
    previous = [
        {
            "stock_code": "000001",
            "stock_name": "甲",
            "rank_no": 5,
            "raw_score": 0.72,
            "theme_codes": ["算力"],
            "strategy_keys": ["right_side_trend"],
            "is_strategy_candidate": True,
            "actionability": "WAIT_TRIGGER",
        }
    ]
    current = [
        {
            "stock_code": "000001",
            "stock_name": "甲",
            "rank_no": 2,
            "raw_score": 0.81,
            "theme_codes": ["算力"],
            "strategy_keys": ["right_side_trend"],
            "is_strategy_candidate": True,
            "actionability": "BUY_ZONE",
            "features": {"stock_leadership_score": 0.82},
        },
        {
            "stock_code": "000002",
            "stock_name": "乙",
            "rank_no": 3,
            "raw_score": 0.76,
            "theme_codes": ["算力"],
            "strategy_keys": ["theme_diffusion"],
            "is_strategy_candidate": True,
            "actionability": "WAIT_TRIGGER",
        },
    ]

    rows, summary = enrich_candidate_dynamics(
        current,
        previous_items=previous,
    )
    by_code = {row["stock_code"]: row for row in rows}

    assert by_code["000001"]["daily_change"] == "UPGRADED"
    assert "排名提升3位" in by_code["000001"]["continuity_explanation"]
    assert by_code["000001"]["dynamic_role"] == "LEADER"
    assert by_code["000002"]["daily_change"] == "NEW"
    assert by_code["000001"]["related_candidates"] == [{
        "stock_code": "000002",
        "stock_name": "乙",
        "theme_rank": 2,
        "relation": "SAME_SCENARIO_CANDIDATE",
    }]
    assert summary["new_count"] == 1
    assert summary["upgraded_count"] == 1


def test_strategy_execution_summary_distinguishes_zero_candidates_from_block() -> None:
    summary = build_strategy_execution_summary([
        {
            "strategy_key": "right_side_trend",
            "forecast_status": "VALIDATED_POSITIVE",
            "stock_code": "000001",
            "short_name": "甲",
            "raw_score": 0.8,
        },
        {
            "strategy_key": "theme_diffusion",
            "forecast_status": "SETUP_NOT_READY",
            "stock_code": "000002",
            "short_name": "乙",
            "raw_score": 0.6,
        },
        {
            "strategy_key": "event_drift",
            "forecast_status": "INSUFFICIENT_DATA",
            "stock_code": "000003",
            "short_name": "丙",
            "raw_score": 0.4,
        },
    ])
    by_key = {row["strategy_key"]: row for row in summary["strategies"]}

    assert by_key["right_side_trend"]["status"] == (
        "COMPLETED_WITH_CANDIDATES"
    )
    assert by_key["theme_diffusion"]["status"] == "COMPLETED_NO_CANDIDATE"
    assert by_key["event_drift"]["status"] == "DATA_BLOCKED"
    assert summary["completed_count"] == 2
    assert summary["blocked_count"] == 1


def test_first_verified_pool_is_a_baseline_not_a_false_new_claim() -> None:
    rows, summary = enrich_candidate_dynamics([{
        "stock_code": "000001",
        "stock_name": "甲",
        "rank_no": 1,
        "raw_score": 0.8,
        "strategy_keys": ["right_side_trend"],
        "is_strategy_candidate": True,
        "actionability": "BUY_ZONE",
    }])

    assert rows[0]["daily_change"] == "BASELINE"
    assert summary["status"] == "NO_PREVIOUS_BATCH"
    assert summary["baseline_count"] == 1
    assert summary["new_count"] == 0
