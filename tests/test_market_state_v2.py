from server.engine.market_state_v2 import (
    classify_market_state,
    transition_market_state,
)


def _bull():
    return {
        "risk_score": 30,
        "market_change_pct": 0.8,
        "breadth_pct": 65,
        "trend_score": 75,
        "switch_score": 20,
    }


def test_missing_breadth_can_never_be_bullish():
    snapshot = _bull()
    snapshot["breadth_pct"] = None
    result = classify_market_state(snapshot)
    assert result["candidate_state"] == "unknown"
    assert "breadth_pct" in result["missing_inputs"]


def test_extreme_event_is_immediate():
    result = transition_market_state(
        {**_bull(), "risk_score": 90},
        previous={
            "final_state": "trend_bullish",
            "candidate_state": "trend_bullish",
            "candidate_streak": 4,
            "state_days": 8,
        },
    )
    assert result["final_state"] == "extreme_event"
    assert result["cooldown_remaining"] == 3


def test_initial_extreme_event_also_starts_cooldown():
    result = transition_market_state({**_bull(), "risk_score": 90})
    assert result["final_state"] == "extreme_event"
    assert result["cooldown_remaining"] == 3
    assert result["transition_reason"] == "initial_extreme_event"


def test_improving_transition_requires_confirmation():
    previous = {
        "final_state": "high_range",
        "candidate_state": "high_range",
        "candidate_streak": 3,
        "state_days": 4,
    }
    first = transition_market_state(_bull(), previous=previous)
    assert first["candidate_state"] == "trend_bullish"
    assert first["final_state"] == "high_range"
    second = transition_market_state(
        _bull(),
        previous={
            "final_state": first["final_state"],
            "candidate_state": first["candidate_state"],
            "candidate_streak": first["candidate_streak"],
            "state_days": first["state_days"],
        },
    )
    assert second["final_state"] == "trend_bullish"


def test_post_extreme_cooldown_forces_risk_state():
    result = transition_market_state(
        _bull(),
        previous={
            "final_state": "extreme_event",
            "candidate_state": "extreme_event",
            "candidate_streak": 1,
            "state_days": 1,
            "cooldown_remaining": 3,
        },
    )
    assert result["candidate_state"] == "trend_bullish"
    assert result["final_state"] == "risk_declining"
    assert result["cooldown_remaining"] == 3
