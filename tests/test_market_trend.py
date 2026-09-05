# -*- coding: utf-8 -*-
from datetime import date, timedelta

from server.engine.market_trend import (
    DEFAULT_INDEX_NAMES,
    build_market_trend,
    compact_market_trend_observation,
)
from server.engine import strategy_center


def test_default_indices_match_the_existing_market_style_coverage():
    assert list(DEFAULT_INDEX_NAMES) == [
        "000016",
        "000300",
        "000905",
        "000852",
        "399303",
        "399006",
        "000688",
    ]


def _business_rows(values, *, start=date(2024, 1, 2), code="000300"):
    rows = []
    current = start
    for value in values:
        while current.weekday() >= 5:
            current += timedelta(days=1)
        rows.append({"index_code": code, "trade_date": current.isoformat(), "close": value})
        current += timedelta(days=1)
    return rows


def test_trend_keeps_low_bottoming_and_strengthening_as_separate_states():
    values = [3000 - index * 4 for index in range(90)]
    result = build_market_trend(
        _business_rows(values),
        requested_date="2024-05-10",
        generated_at="2024-05-10 16:00:00",
    )

    index = result["indices"][0]
    weekly = index["periods"]["weekly"]
    assert weekly["position"] == "low"
    assert weekly["bottoming"] == "not_seen"
    assert weekly["strengthening"] == "not_confirmed"
    assert "低位不等于底部" in index["summary"]["position"]
    assert result["methodology"]["reference_chart_indicator"]["status"] == "unknown"


def test_open_week_and_month_are_provisional_and_keep_confirmed_state():
    values = [2500 + index * 3 for index in range(180)]
    rows = _business_rows(values)
    latest = rows[-1]["trade_date"]
    latest_date = date.fromisoformat(latest)
    next_date = latest_date + timedelta(days=1)
    while next_date.weekday() >= 5:
        next_date += timedelta(days=1)
    result = build_market_trend(
        rows,
        requested_date=latest,
        daily_closed=False,
        next_trade_date=next_date,
    )
    periods = result["indices"][0]["periods"]

    assert periods["daily"]["confirmation_status"] == "provisional"
    assert periods["weekly"]["confirmation_status"] in {"final", "provisional"}
    assert periods["monthly"]["confirmation_status"] in {"final", "provisional"}
    if periods["monthly"]["confirmation_status"] == "provisional":
        assert "暂时变化" in result["indices"][0]["summary"]["monthly"]
    for period in periods.values():
        if period["confirmation_status"] == "provisional":
            assert period["confirmed_state"] is not None


def test_effective_trade_calendar_closes_shortened_week_and_month():
    rows = _business_rows([1800 + index * 2 for index in range(180)])
    # Replace the latest observation with a Thursday which is also the last
    # effective trading day of the month; the next session belongs to both a
    # new ISO week and a new month.
    rows[-1]["trade_date"] = "2026-04-30"
    result = build_market_trend(
        rows,
        requested_date="2026-04-30",
        next_trade_date="2026-05-04",
    )
    periods = result["indices"][0]["periods"]
    assert periods["weekly"]["confirmation_status"] == "final"
    assert periods["monthly"]["confirmation_status"] == "final"
    assert periods["weekly"]["closure_basis"] == "next_effective_trade_date:2026-05-04"


def test_missing_calendar_fact_never_guesses_high_period_confirmation():
    rows = _business_rows([1800 + index * 2 for index in range(180)])
    result = build_market_trend(rows, requested_date=rows[-1]["trade_date"])
    periods = result["indices"][0]["periods"]
    assert periods["weekly"]["confirmation_status"] == "provisional"
    assert periods["monthly"]["confirmation_status"] == "provisional"
    assert periods["weekly"]["closure_basis"] == "next_effective_trade_date_unavailable"


def test_recomputed_history_reports_state_changes_without_a_new_table():
    values = [2000 + index * 5 for index in range(90)] + [2450 - index * 8 for index in range(70)]
    result = build_market_trend(_business_rows(values), requested_date="2024-09-30")
    daily = result["indices"][0]["periods"]["daily"]

    assert daily["direction"] == "down"
    assert daily["trend_started_at"]
    assert daily["trend_duration_bars"] > 1
    assert daily["history"]
    assert any("趋势:" in item["reason"] for item in daily["history"])


def test_missing_and_stale_source_states_are_explicit():
    missing = build_market_trend([], requested_date="2026-09-05")
    assert missing["status"] == "unavailable"
    assert len(missing["coverage"]["missing_indices"]) == 7

    stale = build_market_trend(
        _business_rows([1000 + index for index in range(80)]),
        requested_date="2026-09-05",
    )
    assert stale["status"] == "partial"
    assert stale["indices"][0]["source_status"] == "stale"


def test_long_market_holiday_does_not_make_last_close_stale():
    rows = _business_rows([1000 + index for index in range(80)])
    rows[-1]["trade_date"] = "2026-09-30"
    result = build_market_trend(
        rows,
        requested_date="2026-10-07",
        next_trade_date="2026-10-09",
    )
    assert result["indices"][0]["source_status"] == "fresh"


def test_invalid_rows_do_not_create_synthetic_indices():
    result = build_market_trend(
        [
            {"index_code": "000001", "trade_date": "bad", "close": 3200},
            {"index_code": "000001", "trade_date": "2026-09-05", "close": None},
        ],
        requested_date="2026-09-05",
    )
    assert result["indices"] == []
    assert result["coverage"]["available_index_count"] == 0


def test_unclosed_week_never_describes_current_strength_as_confirmed():
    rows = _business_rows([1000 + index * 5 for index in range(320)])
    latest = date.fromisoformat(rows[-1]["trade_date"])
    next_trade = latest + timedelta(days=1)
    while next_trade.weekday() >= 5:
        next_trade += timedelta(days=1)
    result = build_market_trend(
        rows,
        requested_date=latest,
        daily_closed=False,
        next_trade_date=next_trade,
    )
    item = result["indices"][0]

    assert item["periods"]["weekly"]["confirmation_status"] == "provisional"
    if item["periods"]["weekly"]["strengthening"] == "confirmed":
        assert "暂时满足转强条件" in item["summary"]["watch"]
        assert "需等待周线收盘确认" in item["summary"]["watch"]


def test_stale_index_cannot_confirm_an_incomplete_high_period_bar():
    current_rows = _business_rows([1000 + index * 2 for index in range(320)])
    stale_rows = [{**row, "index_code": "000016"} for row in current_rows[:-8]]
    requested = current_rows[-1]["trade_date"]
    next_trade = date.fromisoformat(requested) + timedelta(days=1)
    while next_trade.weekday() >= 5:
        next_trade += timedelta(days=1)
    result = build_market_trend(
        current_rows + stale_rows,
        requested_date=requested,
        daily_closed=True,
        next_trade_date=next_trade,
    )
    stale = next(item for item in result["indices"] if item["index_code"] == "000016")

    assert stale["source_status"] == "stale"
    assert stale["periods"]["weekly"]["confirmation_status"] == "provisional"
    assert stale["periods"]["monthly"]["confirmation_status"] == "provisional"


def test_strengthening_requires_the_slow_trend_evidence():
    result = build_market_trend(
        _business_rows([1000 + index * 5 for index in range(30)]),
        requested_date="2024-03-01",
    )
    daily = result["indices"][0]["periods"]["daily"]

    assert daily["direction"] == "up"
    assert daily["metrics"]["sma60"] is None
    assert daily["strengthening"] == "not_confirmed"


def test_compact_observation_retains_original_states_without_derived_history():
    trend = build_market_trend(
        _business_rows([1000 + index for index in range(100)]),
        requested_date="2024-06-01",
    )
    observation = compact_market_trend_observation(trend)

    assert observation["evidence_type"] == "market_trend_snapshot"
    assert observation["indices"][0]["summary"] == trend["indices"][0]["summary"]
    assert "history" not in observation["indices"][0]["periods"]["daily"]


def test_strategy_center_reuses_daily_market_record_for_original_trend(monkeypatch):
    writes = []
    monkeypatch.setattr(
        strategy_center,
        "_db_write",
        lambda sql, params=None: writes.append((sql, params or {})),
    )
    trend = build_market_trend(
        _business_rows([1000 + index for index in range(100)]),
        requested_date="2024-06-01",
    )
    observation = compact_market_trend_observation(trend)
    required = strategy_center.load_market_state_config()["required_inputs"]
    snapshot = {
        "trade_date": "2024-06-01",
        "source_status": "fresh",
        "market_state": {
            "key": "high_range",
            "evidence": ["原有市场状态证据"],
            "input": {key: 50 for key in required},
        },
        "long_term_market_trend": observation,
        "candidates": [],
        "conflicts": [],
    }

    strategy_center.persist_strategy_center_snapshot(snapshot, ensure_tables=False)

    market_write = next(
        params for sql, params in writes if "INSERT INTO st_market_state_daily" in sql
    )
    evidence = __import__("json").loads(market_write["evidence_json"])
    assert evidence[0] == "原有市场状态证据"
    assert evidence[-1]["evidence_type"] == "market_trend_snapshot"
