import copy
import json
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine, text

from server.api.routers import hot_data
from server.engine import strategy_center as center


TRADE_DATE = "2026-11-10"
CUTOFF = "2026-11-10T22:20:00+08:00"
REVISION_CUTOFF = "2026-11-11T08:30:00+08:00"


@pytest.fixture
def market_inputs():
    end = date.fromisoformat(TRADE_DATE)
    dates = [end - timedelta(days=offset) for offset in range(45, -1, -1)]
    sessions = [day.isoformat() for day in dates if day.weekday() < 5][-20:]
    rows = [{
        "trade_date": day, "market_change_pct": 1.0,
        "breadth_pct": 60.0, "universe_count": 5000,
    } for day in sessions]
    window = {
        "schema": "probiga.strategy-daily-input-window.v1", "sessions": sessions,
        "daily_partition_roots": {day: f"{idx + 1:064x}" for idx, day in enumerate(sessions)},
        "decision_known_at": CUTOFF,
    }
    return window, rows


@pytest.fixture
def state_database(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_market_state_daily (
                id INTEGER PRIMARY KEY, trade_date DATE, run_uid TEXT,
                config_version TEXT, config_hash TEXT, input_hash TEXT, input_json TEXT,
                candidate_state TEXT, final_state TEXT, candidate_streak INTEGER,
                state_days INTEGER, cooldown_remaining INTEGER, source_status TEXT, created_at DATETIME
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_strategy_center_run (
                run_uid TEXT PRIMARY KEY, trade_date DATE, finished_at DATETIME, status TEXT
            )
        """))
    monkeypatch.setattr(center, "get_engine", lambda: engine)
    yield engine
    engine.dispose()


def _append_previous_state(engine, *, run_uid, trade_date, created_at, finished_at, status="done"):
    payload = {"trade_date": trade_date, "source_status": "fresh", "market_input": {"risk_score": 86.0}}
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_strategy_center_run VALUES (:run_uid, :trade_date, :finished_at, :status)
        """), {"run_uid": run_uid, "trade_date": trade_date, "finished_at": finished_at, "status": status})
        connection.execute(text("""
            INSERT INTO st_market_state_daily VALUES (
                NULL, :trade_date, :run_uid, :config_version, :config_hash, :input_hash, :input_json,
                'extreme_event', 'extreme_event', 1, 1, 2, 'fresh', :created_at
            )
        """), {
            "trade_date": trade_date, "run_uid": run_uid,
            "config_version": center.load_market_state_config()["config_version"],
            "config_hash": center.market_state_config_hash(),
            "input_hash": center._canonical_hash(payload), "input_json": json.dumps(payload),
            "created_at": created_at,
        })


def _build(market_inputs, cutoff=CUTOFF):
    window, rows = market_inputs
    return center.build_fixed_market_context(
        trade_date=TRADE_DATE, decision_known_at=cutoff,
        daily_input_window=window, daily_market_rows=rows,
    )


def test_fixed_slot_ignores_late_prior_state_and_never_reads_live_market(
    monkeypatch, state_database, market_inputs,
):
    _append_previous_state(
        state_database, run_uid="before-cutoff", trade_date="2026-11-09",
        created_at="2026-11-10 22:19:00", finished_at="2026-11-10 22:19:30",
    )
    _append_previous_state(
        state_database, run_uid="after-cutoff", trade_date="2026-11-09",
        created_at="2026-11-10 22:19:40", finished_at="2026-11-10 22:21:00",
    )
    _append_previous_state(
        state_database, run_uid="unfinished", trade_date="2026-11-09",
        created_at="2026-11-10 22:19:45", finished_at="2026-11-10 22:19:55", status="running",
    )
    context = _build(market_inputs)

    def live_read(*_args, **_kwargs):
        pytest.fail("fixed-slot governance consulted a live market source")

    for name in ("market_sentiment", "style_switch_signal", "tech_risk_signal", "market_trend"):
        monkeypatch.setattr(hot_data, name, live_read)
    for name in ("_kline_market_features", "_previous_market_state", "_db_read"):
        monkeypatch.setattr(center, name, live_read)
    monkeypatch.setitem(center._MARKET_SNAPSHOT_CACHE, TRADE_DATE, (center.time.monotonic(), {"risk_score": 99}))

    market = center.load_market_snapshot(TRADE_DATE, fixed_market_context=context, decision_known_at=CUTOFF)

    assert market["risk_score"] == 20
    assert market["breadth_pct"] == 60
    assert market["state"]["previous_state"]["run_uid"] == "before-cutoff"
    assert market["state"]["transition_reason"] == "post_extreme_cooldown_started"
    assert market["state"]["as_of_context_sha256"] == context["context_sha256"]
    assert market["state"]["optional_context_status"]["news_event_risk"] == "UNAVAILABLE_AS_OF"
    assert market["long_term_trend"]["status"] == "UNAVAILABLE_AS_OF"


def test_premarket_tape_can_use_new_verified_correction_without_replacing_initial(
    state_database, market_inputs,
):
    initial = _build(market_inputs)
    window, rows = copy.deepcopy(market_inputs)
    window["decision_known_at"] = REVISION_CUTOFF
    window["daily_partition_roots"][TRADE_DATE] = "a" * 64
    rows[-1]["market_change_pct"] = 0.0
    revised = _build((window, rows), REVISION_CUTOFF)
    assert initial["context_sha256"] != revised["context_sha256"]
    assert initial["tape"][-1]["market_change_pct"] == 1.0
    assert revised["tape"][-1]["market_change_pct"] == 0.0
    assert revised["tape"][-1]["daily_partition_root"] == "a" * 64
    market = center.load_market_snapshot(TRADE_DATE, fixed_market_context=revised, decision_known_at=REVISION_CUTOFF)
    assert market["market_change_pct"] == 0.0
    assert market["state"]["previous_state_status"] == "UNAVAILABLE_AS_OF"


@pytest.mark.parametrize("field", ["market_change_pct", "breadth_pct", "universe_count"])
def test_unavailable_required_tape_value_cannot_become_zero(state_database, market_inputs, field):
    market_inputs[1][-1][field] = None
    with pytest.raises(ValueError, match="incomplete session evidence"):
        _build(market_inputs)


def test_fixed_context_rejects_other_cutoff_and_tampered_values(state_database, market_inputs):
    context = _build(market_inputs)
    with pytest.raises(ValueError, match="identity or cutoff differs"):
        center.load_market_snapshot(TRADE_DATE, fixed_market_context=context, decision_known_at=REVISION_CUTOFF)
    context["tape"][-1]["breadth_pct"] = 88.0
    with pytest.raises(ValueError, match="identity or cutoff differs"):
        center.load_market_snapshot(TRADE_DATE, fixed_market_context=context, decision_known_at=CUTOFF)
    with pytest.raises(ValueError, match="both required"):
        center.load_market_snapshot(TRADE_DATE, decision_known_at=CUTOFF)


def test_governance_snapshot_consumes_sealed_market_before_candidate_scoring(
    monkeypatch, state_database, market_inputs,
):
    context = _build(market_inputs)
    monkeypatch.setattr(center, "load_reference_candidate_pool", lambda _day: None)
    monkeypatch.setattr(center, "load_strategy_configs", lambda: {})
    monkeypatch.setattr(center, "load_strategy_metrics", lambda _day, **_kwargs: {})
    monkeypatch.setattr(center, "_load_fixed_recommendation_inputs", lambda *_args, **_kwargs: (
        [], {"status": "COMPLETED"}, ({}, {}),
    ))
    monkeypatch.setattr(center, "_candidate_source_contract", lambda *_args, **_kwargs: {"status": "COMPLETED"})

    def dynamic_signals(*, market, analysis_run_uid, decision_known_at, **_kwargs):
        assert market["state"]["as_of_context_sha256"] == context["context_sha256"]
        assert market["market_state"] == "trend_bullish"
        assert analysis_run_uid == "a" * 32
        assert decision_known_at == "2026-11-10T22:20:00"
        return [], []

    monkeypatch.setattr(center, "_dynamic_execution_signals", dynamic_signals)
    snapshot = center.build_strategy_center_snapshot(
        TRADE_DATE, fixed_market_context=context, decision_known_at=CUTOFF,
        analysis_run_uid="a" * 32,
    )
    assert snapshot["data_sources"][-1] == "cutoff_verified_daily_equal_weight"
    assert snapshot["source_status"] == "fresh"
    assert snapshot["market_state"]["as_of_cutoff"] == "2026-11-10T22:20:00"
    assert snapshot["long_term_market_trend"]["status"] == "UNAVAILABLE_AS_OF"
    assert snapshot["configuration"]["strategy_policy_basis"] == "CURRENT_OPERATIONAL_POLICY"
    assert all(card["metric_scope"] == "UNAVAILABLE_AS_OF" for card in snapshot["strategies"])
    assert all(card["sample_count"] is None for card in snapshot["strategies"])

    with center.bind_external_market_overlay({"external_market_score": 99}):
        with pytest.raises(ValueError, match="live external market overlay"):
            center.build_strategy_center_snapshot(
                TRADE_DATE, fixed_market_context=context, decision_known_at=CUTOFF,
                analysis_run_uid="a" * 32,
            )
