from datetime import date, timedelta

import pytest

from server.trading_v3.market_stage import MODEL_VERSION, evaluate_market_stage


DAY = date(2026, 11, 20)
CUTOFF = f"{DAY} 20:00:00"
CALENDAR = tuple(
    day for i in range(700)
    if (day := date(2025, 1, 1) + timedelta(days=i)).weekday() < 5
)
SESSIONS = [day for day in CALENDAR if day <= DAY]
DECLINE = [100, 101, 102, 103, 104, 105, 104, 103, 102, 100, 98, 97, 98, 99, 100]
CONFIRMED = DECLINE + [103, 107, 105.4, 107, 109]


def _bar(day, close, *, price_range=0.002, amount=100):
    return dict(trade_date=day, available_at=f"{day} 15:00:01", open=close,
                high=close * (1 + price_range), low=close * (1 - price_range),
                close=close, amount=amount)


def _bars(tail, *, contract_ranges=False):
    offset = len(SESSIONS) - len(tail)
    values = [60 + i * 0.08 for i in range(offset)] + list(tail)
    return [_bar(day, close, price_range=0.004 if contract_ranges and i < len(values) - 5 else 0.002,
                 amount=150 if i >= len(values) - 5 else 100)
            for i, (day, close) in enumerate(zip(SESSIONS, values))]


def _breadth(**overrides):
    return {**dict(trade_date=DAY, available_at=f"{DAY} 17:00:00", quality_status="VERIFIED",
                   above_ma20_pct=65, above_ma60_pct=59, new_low_20d_pct=3, new_low_60d_pct=1,
                   advancing_pct=64, above_ma20_change_5d_pp=6), **overrides}


@pytest.mark.parametrize(("state", "tail"), [
    ("UPTREND", list(range(100, 120))),
    ("TOP_CANDIDATE", list(range(100, 115)) + [114.1, 114.2, 114.2, 114.2, 114.2]),
    ("DOWNTREND", [120 - i * 0.7 for i in range(20)]),
    ("ACCELERATING_DECLINE", list(range(120, 105, -1)) + [103, 100, 96, 91, 85]),
    ("STABILIZATION_ATTEMPT", DECLINE + [98.4, 98.6, 98.8, 98.9, 99]),
    ("BOTTOMING_CANDIDATE", DECLINE[:-1] + [97.8, 97.2, 97.4, 97.3, 97.5, 97.6]),
    ("TECHNICAL_REBOUND", DECLINE + [97, 98, 99, 100, 102]),
    ("REVERSAL_CANDIDATE", DECLINE + [102, 103, 104, 105, 106]),
    ("REVERSAL_CONFIRMED", CONFIRMED),
])
def test_rule_model_distinguishes_the_nine_states(state, tail):
    snapshot = evaluate_market_stage(_bars(tail, contract_ranges=state == "BOTTOMING_CANDIDATE"),
                                     as_of=CUTOFF, trading_sessions=CALENDAR, breadth=_breadth())
    assert snapshot["state"] == state
    assert snapshot["status"] == "READY"
    assert snapshot["model_version"] == MODEL_VERSION
    assert snapshot["evidence"] and snapshot["confirmation_conditions"] and snapshot["invalidation_conditions"]
    assert "probability" not in snapshot


@pytest.mark.parametrize("breadth", [None, _breadth(quality_status="FAILED"),
    _breadth(trade_date=DAY - timedelta(days=1)), _breadth(available_at=f"{DAY} 21:00:00"),
    _breadth(above_ma20_pct=38), _breadth(above_ma20_change_5d_pp=None),
    _breadth(new_low_60d_pct=9), _breadth(above_ma60_pct=float("nan"))])
def test_structural_retest_without_verified_broad_follow_through_cannot_confirm_reversal(breadth):
    snapshot = evaluate_market_stage(_bars(CONFIRMED), as_of=CUTOFF,
                                     trading_sessions=CALENDAR, breadth=breadth)
    assert snapshot["periods"]["daily"]["features"]["breakout_retested"] is True
    assert snapshot["state"] == "REVERSAL_CANDIDATE"


def test_future_sessions_and_late_corrections_do_not_change_historical_decision():
    bars = _bars(CONFIRMED)
    late_correction = {**_bar(DAY, 42), "available_at": "2026-11-23 16:00:00"}
    partial = {**_bar(DAY, 44), "available_at": f"{DAY} 14:59:00"}
    future = _bar(date(2026, 11, 23), 35)
    expected = evaluate_market_stage(bars, as_of=CUTOFF, trading_sessions=CALENDAR)
    actual = evaluate_market_stage([future, late_correction, partial, *reversed(bars)],
                                   as_of=CUTOFF, trading_sessions=CALENDAR)
    assert actual == expected
    corrected = evaluate_market_stage([*bars, late_correction], as_of="2026-11-21 21:00:00",
                                      trading_sessions=CALENDAR)
    assert corrected["periods"]["daily"]["features"]["close"] == 109
    accepted = evaluate_market_stage([*bars, late_correction], as_of="2026-11-23 16:00:00",
                                     trading_sessions=CALENDAR)
    assert accepted["status"] == "INSUFFICIENT"  # The expected 11-23 bar is still absent.


def test_calendar_closes_actual_last_session_and_excludes_incomplete_month():
    # Friday is absent from the supplied exchange calendar. Thursday closes the week.
    calendar = tuple(day for day in CALENDAR if day != DAY)
    bars = _bars(CONFIRMED)[:-1]
    before_close = evaluate_market_stage(bars, as_of="2026-11-19 14:59:59", trading_sessions=calendar)
    after_close = evaluate_market_stage(bars, as_of="2026-11-19 16:00:00", trading_sessions=calendar)
    without_calendar = evaluate_market_stage(bars, as_of="2026-11-19 16:00:00")
    assert before_close["periods"]["weekly"]["end_date"] == "2026-11-13"
    assert after_close["periods"]["weekly"]["end_date"] == "2026-11-19"
    assert without_calendar["periods"]["weekly"]["end_date"] == "2026-11-13"
    assert after_close["periods"]["monthly"]["end_date"] == "2026-10-30"


def test_calendar_without_a_following_session_cannot_confirm_reversal():
    result = evaluate_market_stage(_bars(CONFIRMED), as_of=CUTOFF,
                                   trading_sessions=SESSIONS, breadth=_breadth())
    assert result["state"] == "REVERSAL_CANDIDATE"
    assert result["periods"]["weekly"]["end_date"] == "2026-11-13"
    assert any("未覆盖后续交易日" in note for note in result["uncertainties"])


def test_expected_session_gap_cannot_be_filled_with_an_older_observation():
    bars = _bars(CONFIRMED)
    latest_missing = evaluate_market_stage(bars[:-1], as_of=CUTOFF, trading_sessions=CALENDAR)
    recent_gap = evaluate_market_stage(bars[:-3] + bars[-2:], as_of=CUTOFF, trading_sessions=CALENDAR)
    assert latest_missing["trade_date"] == DAY.isoformat()
    assert latest_missing["periods"]["daily"]["observation_count"] == 0
    assert recent_gap["periods"]["daily"]["observation_count"] == 2
    assert latest_missing["status"] == recent_gap["status"] == "INSUFFICIENT"
    assert recent_gap["periods"]["weekly"]["end_date"] is None


def test_duplicate_versions_and_malformed_rows_are_deterministic():
    bars = _bars(CONFIRMED)
    earlier = {**_bar(DAY, 71), "available_at": f"{DAY} 15:00:00", "low": 200}
    assert evaluate_market_stage([earlier, *bars], as_of=CUTOFF) == evaluate_market_stage(
        [*bars, earlier], as_of=CUTOFF)
    with pytest.raises(ValueError, match="conflicting market stage bars"):
        evaluate_market_stage([*bars, _bar(DAY, 71)], as_of=CUTOFF)
    with pytest.raises(ValueError, match="invalid market stage OHLC"):
        evaluate_market_stage([*bars[:-1], earlier], as_of=CUTOFF)


def test_flat_index_does_not_become_a_false_bottom_or_top():
    bars = [_bar(day, 100) for day in SESSIONS]
    result = evaluate_market_stage(bars, as_of=CUTOFF, trading_sessions=CALENDAR, breadth=_breadth())
    assert result["status"] == "READY" and result["state"] is None
