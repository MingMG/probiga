from datetime import date, datetime, timedelta
import json

import pytest

from server.trading_v3 import market_stage_replay as replay


@pytest.fixture
def history(monkeypatch):
    first_day = date(2026, 6, 1)
    sessions = [first_day + timedelta(days=index) for index in range(90)
                if (first_day + timedelta(days=index)).weekday() < 5]
    bars = [dict(trade_date=str(day), available_at=f"{day} 16:00:00",
                 open=100, low=99, high=101, close=100) for day in sessions]

    def assess(known_bars, *, as_of, trading_sessions, breadth):
        assert all(datetime.fromisoformat(bar["available_at"]) <= as_of for bar in known_bars)
        assert all(date.fromisoformat(bar["trade_date"]) <= as_of.date() for bar in known_bars)
        assert known_bars[-1]["trade_date"] == str(as_of.date())
        state = "REVERSAL_CONFIRMED" if as_of.date() == sessions[25] else "TECHNICAL_REBOUND"
        return {"status": "READY", "trade_date": str(as_of.date()), "state": state,
                "breadth": {"quality_status": "VERIFIED", "above_ma20_change_5d_pp": 2},
                "periods": {"daily": {"features": {"resistance": 101, "turnover_ratio": 1.15}},
                            "weekly": {"features": {"close": 100}}, "monthly": {"features": {"close": 100}}},
                "model_version": "fixture", "input_days": len(known_bars), "last_close": known_bars[-1]["close"]}

    monkeypatch.setattr(replay, "evaluate_market_stage", assess)
    return bars, sessions


def run(history, *, target=20, cutoff=50):
    bars, sessions = history
    return replay.replay_market_stage(
        bars, trading_sessions=sessions, start_date=sessions[target], end_date=sessions[target],
        outcome_as_of=f"{sessions[cutoff]} 20:00:00",
    )


def test_replay_separates_frozen_assessment_and_realized_price_outcomes(history):
    bars, sessions = history
    original = run(history)
    bars[24].update(high=110, close=108)
    bars[25].update(low=97, close=98)
    revised = run(history)
    before, after = original["rows"][0], revised["rows"][0]
    assert before["assessment"] == after["assessment"]
    assert after["decision_known_at"] == f"{sessions[20]}T20:00:00"
    outcome = after["outcomes"]["5"]
    assert outcome["end_date"] == str(sessions[25])
    assert outcome["return_pct"] == -2
    assert outcome["max_upside_pct"] == 10
    assert outcome["max_downside_pct"] == -3
    assert outcome["max_drawdown_pct"] == pytest.approx(-9.259259)
    assert outcome["new_20_session_low"] is True
    assert outcome["decision_resistance"] == 101
    assert outcome["resistance_broken"] is True
    assert outcome["sessions_from_assessment_to_window_low"] == 5
    assert outcome["first_reversal_confirmation_date"] == str(sessions[25])
    summary = revised["summary"]["5"][0]
    assert summary["matured_count"] == summary["new_low_count"] == 1
    assert summary["transition_evaluated_count"] == summary["reversal_confirmation_count"] == 1
    assert summary["observed_reversal_confirmation_rate"] == 1
    json.dumps(revised, allow_nan=False)


@pytest.mark.parametrize("case", ["missing_session", "late_publication", "future_cutoff", "invalid_session"])
def test_replay_censors_missing_or_unavailable_future_bars(history, case):
    bars, sessions = history
    cutoff = 50
    if case == "missing_session":
        del bars[28]
    elif case == "late_publication":
        bars[28]["available_at"] = f"{sessions[51]} 16:00:00"
    elif case == "invalid_session":
        bars[28]["open"] = None
    else:
        cutoff = 27
    result = run(history, cutoff=cutoff)
    outcomes = result["rows"][0]["outcomes"]
    assert outcomes["5"]["end_date"] == str(sessions[25])
    assert outcomes["10"] is outcomes["20"] is None
    assert result["summary"]["10"][0]["matured_count"] == 0
    assert result["summary"]["10"][0]["new_low_rate"] is None


def test_replay_rejects_after_cutoff_decision_but_allows_late_bar_in_future_label(history):
    bars, sessions = history
    bars[25]["available_at"] = f"{sessions[25]} 20:30:00"
    label = run(history)["rows"][0]["outcomes"]["5"]
    assert label["end_date"] == str(sessions[25])
    assert label["transition_evaluable"] is False
    assert label["first_reversal_confirmation_date"] is None
    after_cutoff = run(history, target=25)["rows"][0]
    assert after_cutoff["status"] == "DATA_BLOCKED"
    assert after_cutoff["assessment"] is None


def test_replay_rejects_duplicate_benchmark_day(history):
    bars, _ = history
    bars.append(dict(bars[3]))
    with pytest.raises(ValueError, match="duplicate or out-of-calendar"):
        run(history)


def test_replay_runs_real_market_stage_rules_for_a_declining_benchmark():
    first_day = date(2025, 1, 1)
    sessions = [first_day + timedelta(days=index) for index in range(320)
                if (first_day + timedelta(days=index)).weekday() < 5]
    bars = [dict(trade_date=str(day), available_at=f"{day} 16:00:00", open=450 - index,
                 high=451 - index, low=448 - index, close=449 - index, amount=2000)
            for index, day in enumerate(sessions)]
    result = replay.replay_market_stage(
        bars, trading_sessions=sessions, start_date=sessions[200], end_date=sessions[201],
        outcome_as_of=f"{sessions[222]} 20:00:00",
    )
    assert result["status"] == "READY"
    for row in result["rows"]:
        assessment = row["assessment"]
        assert assessment["trade_date"] == row["trade_date"]
        assert assessment["state"] in {"DOWNTREND", "ACCELERATING_DECLINE"}
        assert all(period["end_date"] <= row["trade_date"] for period in assessment["periods"].values())
        assert row["outcomes"]["20"]["new_20_session_low"] is True
