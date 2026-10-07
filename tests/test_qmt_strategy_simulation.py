from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
import json

import pandas as pd
import pytest

from biz.analysis import sync_analysis_fast as production
from server.engine import qmt_strategy_simulation as simulation
from server.trading_v3.sleeves import SLEEVE_BUILDERS


TARGET = "2026-09-30"


def seal(snapshot):
    snapshot["input_hash"] = simulation.snapshot_input_hash(snapshot)
    return snapshot


def snapshot():
    return seal({
        "schema": simulation.INPUT_SCHEMA, "trade_date": TARGET, "mode": "REPLAY",
        "prepared_at": "2026-10-08T18:20:00+08:00", "decision_at": TARGET + "T23:59:59",
        "market_clock": {"expected_trade_date": TARGET, "current_closed_trade_date": "2026-10-08",
                         "observed_at": "2026-10-08T18:20:00+08:00"},
        "formula_contract": simulation.formula_contract(),
        "simulation_only": True, "real_order_allowed": False,
        "v2": {"status": "DATA_BLOCKED", "trade_date": TARGET, "reasons": ["PIT_BATCH_MISSING"],
               "frames": {}, "proofs": {}},
        "v3": {"status": "DATA_BLOCKED", "trade_date": TARGET, "reasons": ["QMT_DAILY_WINDOW_MISSING"],
               "stocks": [], "market_features": {}},
    })


def ready_v2():
    code = "000001"
    bars = {
        "stock_code": code, "short_name": "测试公司", "trade_date": TARGET,
        "close": 20.0, "amount": 600_000_000.0, "turnover_ratio": 6.0,
        "change_pct": 2.0, "ma5": 19.5, "ma10": 18.0, "ma20": 17.0, "ma60": 15.0,
        "pct_5": 12.0, "pct_20": 30.0, "dist_ma20": 10.0, "volatility_20": 5.0,
        "amount_ratio_5": 1.6, "amount_ratio_20": 1.6,
        "amount_ma5": 400_000_000.0, "amount_ma20": 375_000_000.0,
        "high_20": 20.2, "high_60": 28.0, "drawdown_60": -10.0, "from_low_60": 30.0,
        "chase_evidence_status": "ALLOW",
    }
    finance = {
        "stock_code": code, "finance_pit_status": "AVAILABLE",
        "roe_wtd": 18.0, "gross_margin": 45.0, "net_margin": 20.0,
        "oper_cf_ps": 2.0, "total_rev_yoy_gr": 40.0, "net_profit_yoy_gr": 80.0,
        "non_gaap_net_profit_yoy_gr": 80.0, "net_asset_ps": 10.0,
        "asset_liab_ratio": 30.0, "cash_flow_ratio": 1.5, "curr_ratio": 2.0,
    }
    flow = {"stock_code": code, "flow_trade_date": TARGET,
            "main_net_inflow": 60_000_000.0, "main_net_inflow_5d": 100_000_000.0,
            "main_net_inflow_20d": 400_000_000.0}
    notices = {"stock_code": code, "notice_count": 5, "notice_positive": 5,
               "notice_negative": 0, "notice_critical": 0, "event_pit_status": "AVAILABLE"}
    sector = {"stock_code": code, "industry_name": "测试行业", "sector_rotation_score": 100.0,
              "industry_pit_status": "AVAILABLE", "industry_snapshot_date": TARGET}
    return {
        "status": "READY", "reasons": [], "trade_date": TARGET, "flow_date": TARGET,
        "market_mood_score": 95.0,
        "frames": {"kline": [bars], "finance": [finance], "flow": [flow], "notices": [notices],
                   "sector": [sector], "hot": [], "confidence": [], "rec_history": [], "failures": []},
        "proofs": {"qmt_daily_input_window": {"sessions": [TARGET]}},
    }


def ready_v3(features=None):
    stock = {
        "stock_code": "000001", "stock_name": "测试公司", "price": 20.0,
        "finance_pit_status": "AVAILABLE", "event_pit_status": "AVAILABLE",
        "industry_pit_status": "AVAILABLE", "entry_eligible": 1.0, "latest_tradable": 1.0,
        "market_return_20d_pct": 5.0, "return_20d_pct": 22.0, "return_60d_pct": 55.0,
        "ma20_slope_5d_pct": 4.0, "amount_ratio_5_20": 1.8,
        "relative_strength_20d_pct": 22.0, "distance_ma20_pct": 4.0,
        "close_above_ma20": 1.0, "ma20_above_ma60": 1.0, "atr_14d_pct": 1.0,
        "latest_change_pct": 2.0, "latest_amount": 100_000_000.0,
        "average_amount_20d": 80_000_000.0,
    }
    stock.update(features or {})
    return {
        "status": "READY", "reasons": [], "trade_date": TARGET,
        "feature_time": TARGET + "T15:00:00", "source": "QMT_ATTESTED",
        "data_snapshot_hash": "a" * 64, "stocks": [stock],
        "calibration_inputs": {"status": "READY", "authority": "TradingV3Repository.active_calibration_status",
                               "observed_at": TARGET + "T18:20:00+08:00", "calibrations": {},
                               "rejections": {}, "registry": []},
        "market_features": {
            "market_return_20d_pct": 5.0, "market_breadth_pct": 70.0,
            "breadth_change_5d_pct": 5.0, "realized_volatility_20d_pct": 2.0,
            "limit_down_ratio_pct": 0.1, "market_eligible_stock_count": 4500,
            "market_latest_coverage_ratio": 0.98, "market_tradable_coverage_ratio": 0.95,
            "concept_snapshot_age_days": 0, "qmt_attestation_current": True,
            "qmt_daily_input_window": {"sessions": [TARGET]},
        },
    }


def test_catalog_uses_exact_original_ten_and_four_recipes():
    catalog = simulation.strategy_catalog()
    assert [item["strategy_key"] for item in catalog["strategies"]] == list(simulation.STRATEGY_KEYS)
    assert len(catalog["combinations"]) == 4
    attack = next(item for item in catalog["combinations"] if item["strategy_key"] == "trend_attack")
    assert {item["strategy_key"]: item["weight"] for item in attack["members"]} == {
        "main_wave": 0.5, "short_term": 0.3, "ultra_short": 0.2}
    assert {item["strategy_key"] for item in catalog["excluded"]} == set(simulation.EXCLUDED_KEYS)
    assert not any(set(simulation.EXCLUDED_KEYS) & {member["strategy_key"] for member in item["members"]}
                   for item in catalog["combinations"])


def test_all_blocked_inputs_never_create_selections_or_fake_return():
    result = simulation.evaluate_snapshot(snapshot())
    assert result["status"] == "DATA_BLOCKED"
    assert len(result["strategy_rows"]) == 10
    assert len(result["combination_rows"]) == 4
    assert all(row["status"] == "DATA_BLOCKED" and not row["selected"]
               for row in result["strategy_rows"] + result["combination_rows"])
    assert result["performance"] == {"status": "NO_FILL_EVIDENCE", "return_pct": None}
    assert result["simulation_only"] is True
    assert result["real_order_allowed"] is False


def test_snapshot_tamper_and_local_formula_drift_rejected():
    inputs = snapshot()
    inputs["v3"]["reasons"] = ["tampered"]
    with pytest.raises(ValueError, match="input hash differs"):
        simulation.evaluate_snapshot(inputs)
    inputs = snapshot()
    inputs["formula_contract"]["v3_right_side_builder"] = "right_side_trend_v303"
    seal(inputs)
    with pytest.raises(ValueError, match="formula contract differs"):
        simulation.evaluate_snapshot(inputs)


def test_replay_future_knowledge_cutoff_and_excluded_key_rejected():
    inputs = snapshot()
    inputs["decision_at"] = "2026-10-08T18:20:00"
    seal(inputs)
    with pytest.raises(ValueError, match="historical knowledge cutoff"):
        simulation.evaluate_snapshot(inputs)
    with pytest.raises(ValueError, match="excluded"):
        simulation.evaluate_snapshot(snapshot(), ["intraday_surprise"])


def test_v2_json_roundtrip_is_numerically_identical_to_actual_production_functions():
    inputs = snapshot()
    inputs["v2"] = ready_v2()
    seal(inputs)
    inputs = json.loads(simulation.canonical_json(inputs))
    result = simulation.evaluate_snapshot(inputs, list(simulation.V2_KEYS))
    source = inputs["v2"]
    original = production.compute_scores(
        **{key: pd.DataFrame(rows) if rows else pd.DataFrame({"stock_code": []})
           for key, rows in source["frames"].items()},
        market_mood_score=source["market_mood_score"], flow_date=TARGET,
        trade_date=TARGET, min_score=62.0,
    )
    original = production.apply_canonical_execution_eligibility(original).iloc[0].to_dict()
    manifest, _ = simulation._configs()
    specs = {item["key"]: item for item in manifest["strategies"]}
    for row in result["strategy_rows"]:
        key = row["strategy_key"]
        plan = production.build_strategy_trade_plan(original, key)
        assert row["selected_count"] == 1, (key, row["rejected_summary"])
        selected = row["selected"][0]
        assert selected["score"] == original[specs[key]["score_field"]]
        assert selected["status"] == plan["signal_status"]
        assert selected["reasons"][0] == plan["signal_reason"]
        assert selected["stop_loss_price"] == plan["stop_loss_price"]
        assert selected["entry_price_low"] == plan["entry_price_low"]
    assert simulation.evaluate_snapshot(inputs, list(simulation.V2_KEYS)) == result


def test_v2_missing_real_factor_blocks_despite_production_neutral_score_default():
    inputs = snapshot()
    inputs["v2"] = ready_v2()
    inputs["v2"]["frames"]["finance"][0]["oper_cf_ps"] = None
    seal(inputs)
    result = simulation.evaluate_snapshot(inputs, list(simulation.V2_KEYS))
    assert all(not row["selected"] and row["status"] == "DATA_BLOCKED"
               for row in result["strategy_rows"])
    assert "REQUIRED_FACTOR_MISSING:oper_cf_ps" in result["strategy_rows"][0]["rejected_summary"][0]["reasons"]


def test_v2_exact_target_date_and_canonical_chase_gate_are_required():
    inputs = snapshot()
    inputs["v2"] = ready_v2()
    inputs["v2"]["flow_date"] = "2026-09-29"
    seal(inputs)
    with pytest.raises(ValueError, match="capital flow date differs"):
        simulation.evaluate_snapshot(inputs, ["main_wave"])
    inputs["v2"]["flow_date"] = TARGET
    inputs["v2"]["frames"]["kline"][0]["chase_evidence_status"] = "DATA_BLOCKED"
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["main_wave"])["strategy_rows"][0]
    assert not row["selected"]
    assert any("QMT_CANONICAL_CHASE_GATE_NOT_ALLOW" in item["reasons"] for item in row["rejected_summary"])


@pytest.mark.parametrize("key,features", [
    ("right_side_trend", {}),
    ("theme_diffusion", {"sector_breadth_pct": 78.0, "sector_breadth_acceleration_pct": 22.0,
                         "sector_relative_return_pct": 5.0, "sector_amount_acceleration_pct": 65.0,
                         "leadership_quality": 1.0, "theme_opportunity_score": 1.0,
                         "sector_crowding": 0.2, "return_5d_pct": 8.0,
                         "event_surprise": 1.0, "news_theme_context_score": 0.0,
                         "market_news_risk_score": 0.0}),
    ("low_base_ignition", {"sector_breadth_pct": 70.0, "sector_breadth_acceleration_pct": 25.0,
                           "sector_relative_return_pct": 8.0, "sector_amount_acceleration_pct": 80.0,
                           "theme_opportunity_score": 0.95, "sector_crowding": 0.2,
                           "return_5d_pct": -1.0, "return_20d_pct": -10.0, "return_60d_pct": 0.0,
                           "distance_ma20_pct": 0.0, "latest_change_pct": 3.0,
                           "latest_amount": 200_000_000.0, "amount_ratio_5_20": 1.2,
                           "atr_14d_pct": 4.0, "breakout_20d_proximity": 0.8,
                           "stock_leadership_score": 0.9, "stock_relative_to_theme_5d_pct": -1.0,
                           "news_theme_context_score": 0.0, "market_news_risk_score": 0.0}),
    ("oversold_reversal", {"return_2d_pct": 3.0, "return_5d_pct": -2.0,
                           "return_20d_pct": -25.0, "drawdown_20d_pct": -30.0,
                           "distance_ma20_pct": -10.0, "distance_ma5_pct": 2.0,
                           "ma20_slope_5d_pct": -4.0, "latest_change_pct": 5.0,
                           "previous_change_pct": -2.0, "amount_ratio_1_20": 2.8,
                           "rebound_from_low_pct": 4.5, "latest_relative_to_market_pct": 5.0,
                           "atr_14d_pct": 4.0, "sector_relative_return_pct": 5.0,
                           "sector_breadth_pct": 72.0, "theme_opportunity_score": 0.9,
                           "stock_leadership_score": 0.9}),
    ("quality_momentum", {"quality_percentile": 1.0, "growth_percentile": 1.0,
                          "cashflow_quality_percentile": 1.0, "valuation_percentile": 1.0,
                          "momentum_60d_percentile": 1.0, "volatility_20d_percentile": 0.0}),
    ("event_drift", {"event_surprise": 1.0, "event_novelty": 1.0,
                     "event_source_reliability": 1.0, "event_price_confirmation": 1.0,
                     "event_priced_in": 0.0, "event_decay": 0.0}),
])
def test_v3_registered_builder_exact_score_reasons_and_stop(key, features):
    from server.trading_v3.engine import TradingV3Engine
    inputs = snapshot()
    inputs["v3"] = ready_v3(features)
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, [key])["strategy_rows"][0]
    feature_time = datetime.fromisoformat(inputs["v3"]["feature_time"])
    original = SLEEVE_BUILDERS[key]("000001", "测试公司", inputs["v3"]["stocks"][0],
                                   feature_time, feature_time.replace(month=10, day=30))
    assert row["selected_count"] == 1
    selected = row["selected"][0]
    assert selected["score"] == original.score
    forecast = TradingV3Engine().forecast(original)
    assert selected["status"] == forecast.status
    assert selected["reasons"] == list(forecast.reasons)
    assert selected["ranking_basis"] == "UNCALIBRATED_RAW_SCORE_RESEARCH_ONLY"
    assert selected["initial_stop_pct"] == original.initial_stop_pct
    assert row["version"] == inputs["formula_contract"]["v3_version"] or row["version"].startswith("v3.")


def test_v3_old_right_side_formula_is_not_used_and_missing_pit_does_not_select():
    # v303 accepts a weak-market sector watch, while registered v304 rejects a
    # broad market below 2%. Preserve the actual registered formula's status.
    inputs = snapshot()
    inputs["v3"] = ready_v3({"market_return_20d_pct": -1.0})
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["right_side_trend"])["strategy_rows"][0]
    # The current original shadow ledger records even shape-blocked complete
    # observations. It must be labelled as blocked observation, never a buy.
    assert row["selected_count"] == 1
    assert row["selected"][0]["status"] == "MARKET_REGIME_BLOCKED"
    assert row["selected"][0]["selection_kind"] == "ORIGINAL_V3_SHADOW_PORTFOLIO_OBSERVATION"
    inputs["v3"] = ready_v3({"finance_pit_status": "DATA_BLOCKED"})
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["right_side_trend"])["strategy_rows"][0]
    assert row["status"] == "DATA_BLOCKED" and not row["selected"]


def test_v3_market_coverage_is_checked_and_never_invented():
    inputs = snapshot()
    inputs["v3"] = ready_v3()
    inputs["v3"]["market_features"]["market_latest_coverage_ratio"] = 0.5
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["right_side_trend"])["strategy_rows"][0]
    assert row["status"] == "DATA_BLOCKED" and not row["selected"]
    assert any("MARKET_LATEST_COVERAGE" in reason for reason in row["blocked_reasons"])
    inputs["v3"]["market_features"].pop("market_latest_coverage_ratio")
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["right_side_trend"])["strategy_rows"][0]
    assert any("MARKET_PROOF_MISSING" in reason for reason in row["blocked_reasons"])


def test_missing_v3_formula_feature_is_explicit_data_block_not_empty_selection():
    inputs = snapshot()
    inputs["v3"] = ready_v3()
    inputs["v3"]["stocks"][0].pop("ma20_slope_5d_pct")
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["right_side_trend"])["strategy_rows"][0]
    assert row["status"] == "DATA_BLOCKED"
    assert not row["selected"]
    assert any("ma20_slope_5d_pct" in reason
               for item in row["rejected_summary"] for reason in item["reasons"])


def test_theme_diffusion_uses_same_multi_theme_winner_as_production_engine():
    from datetime import timedelta
    from server.trading_v3.engine import TradingV3Engine

    strong = {"theme_feature_key": "z-strong", "theme_name": "强主题",
              "sector_breadth_pct": 78.0, "sector_breadth_acceleration_pct": 22.0,
              "sector_relative_return_pct": 5.0, "sector_amount_acceleration_pct": 65.0,
              "leadership_quality": 1.0, "theme_opportunity_score": 1.0,
              "sector_crowding": 0.2, "return_5d_pct": 8.0, "event_surprise": 1.0,
              "news_theme_context_score": 0.0, "market_news_risk_score": 0.0}
    inputs = snapshot()
    inputs["v3"] = ready_v3()
    inputs["v3"]["stocks"][0]["theme_signal_candidates"] = [
        {**strong, "theme_feature_key": "a-ineligible", "sector_breadth_pct": 99.0}, strong]
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["theme_diffusion"])["strategy_rows"][0]
    feature_time = datetime.fromisoformat(inputs["v3"]["feature_time"])
    forecasts, _theme_rows = TradingV3Engine().evaluate_stock_with_theme_signals(
        "000001", "测试公司", inputs["v3"]["stocks"][0], feature_time,
        feature_time + timedelta(days=30))
    original = next(item for item in forecasts if item.strategy_key == "theme_diffusion")
    assert row["selected_count"] == 1
    assert row["selected"][0]["score"] == original.raw_score
    assert row["selected"][0]["reasons"] == list(original.reasons)
    assert row["selected"][0]["theme_code"] == "强主题"


def test_combination_uses_original_weights_without_silently_replacing_missing_member():
    inputs = snapshot()
    inputs["v2"] = ready_v2()
    seal(inputs)
    result = simulation.evaluate_snapshot(inputs)
    attack = next(row for row in result["combination_rows"] if row["strategy_key"] == "trend_attack")
    assert attack["selected_count"] == 1
    fact = attack["selected"][0]
    assert fact["selection_kind"] == "FROZEN_MEMBER_UNION"
    assert {item["strategy_key"]: item["weight"] for item in fact["member_contributions"]} == {
        "main_wave": 0.5, "short_term": 0.3, "ultra_short": 0.2}
    expected = sum(item["weight"] * item["score"] / item["score_scale"]
                   for item in fact["member_contributions"])
    assert fact["score"] == round(expected, 8)
    mainline = next(row for row in result["combination_rows"] if row["strategy_key"] == "v3_mainline_attack")
    assert mainline["status"] == "DATA_BLOCKED" and not mainline["selected"]


def test_prepare_replay_uses_historical_cutoff_and_missing_family_can_coexist(monkeypatch):
    from server.common import authoritative_market_clock as clock
    captured = []

    def closed(_engine, now=None):
        return TARGET if now.date() == date.fromisoformat(TARGET) else "2026-10-08"

    def v2(_engine, target, decision):
        captured.append((target, decision))
        raise RuntimeError("PIT_FINANCE_COVERAGE_UNPROVEN")

    def v3(_primary, _kline, target, decision):
        captured.append((target, decision))
        return ready_v3()

    monkeypatch.setattr(clock, "authoritative_closed_trade_date", closed)
    monkeypatch.setattr(simulation, "_prepare_v2", v2)
    monkeypatch.setattr(simulation, "_prepare_v3", v3)
    inputs = simulation.prepare_inputs(object(), object(), TARGET)
    assert inputs["mode"] == "REPLAY"
    assert inputs["v2"]["status"] == "DATA_BLOCKED"
    assert inputs["v3"]["status"] == "READY"
    assert all(cutoff == datetime(2026, 9, 30, 23, 59, 59) for _target, cutoff in captured)
    assert simulation.evaluate_snapshot(inputs)["status"] == "PARTIAL_DATA_BLOCKED"


def test_public_block_reason_does_not_expose_sql_connection_details():
    failure = simulation._failure("V2_INPUTS", ValueError("secret-db-password=example"))
    assert failure["reasons"] == ["V2_INPUTS: ValueError"]


@pytest.mark.parametrize("calibrated", [False, True])
def test_v3_selection_exactly_matches_current_shadow_top_k_and_calibrated_ranking(calibrated):
    from datetime import timedelta
    from server.trading_v3.calibration import CalibrationBucket, CalibrationTable
    from server.trading_v3.engine import TradingV3Engine
    from server.trading_v3.shadow_portfolio import build_shadow_portfolio_rows

    inputs = snapshot()
    source = ready_v3()
    source["stocks"] = []
    for index in range(25):
        value = (index + 1) / 26
        stock = ready_v3({"quality_percentile": value, "growth_percentile": value,
                          "cashflow_quality_percentile": value, "valuation_percentile": value,
                          "momentum_60d_percentile": value, "volatility_20d_percentile": 1 - value})["stocks"][0]
        stock["stock_code"] = str(index + 1).zfill(6)
        source["stocks"].append(stock)
    calibrations = {}
    if calibrated:
        # The existing tolerance permits a 0.2% adjacent decline: calibrated
        # ordering therefore differs from raw ordering while remaining valid.
        def bucket(lower, upper, expected):
            return CalibrationBucket(lower, upper, 500, expected, -2, 1, 4, .7, -3, 5, 3, 2)
        table = CalibrationTable("quality_momentum", "v3.3.0-test", "b" * 64,
                                 (bucket(0, .35, 2), bucket(.350001, .7, 1.8), bucket(.700001, 1, 2.2)))
        assert table.has_valid_score_direction()
        calibrations = {"quality_momentum": table}
        source["calibration_inputs"]["calibrations"] = {key: value.as_dict() for key, value in calibrations.items()}
    inputs["v3"] = source
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["quality_momentum"])["strategy_rows"][0]
    feature_time = datetime.fromisoformat(source["feature_time"])
    engine = TradingV3Engine(calibrations)
    forecasts = [engine.forecast(SLEEVE_BUILDERS["quality_momentum"](
        item["stock_code"], item["stock_name"], item, feature_time,
        feature_time + timedelta(days=30))) for item in source["stocks"]]
    identifiers = {(item.stock_code, item.strategy_key): item.stock_code for item in forecasts}
    expected = build_shadow_portfolio_rows(forecasts, run_uid="test", trade_date=date.fromisoformat(TARGET),
                                          forecast_ids=identifiers,
                                          policy=simulation._configs()[1]["shadow_portfolios"])
    assert row["selected_count"] == 20
    assert [item["stock_code"] for item in row["selected"]] == [item["stock_code"] for item in expected]
    assert [item["selection_score"] for item in row["selected"]] == [item["selection_score"] for item in expected]
    assert any(item["score"] < .76 for item in row["selected"])
    assert all(item["ranking_basis"] == ("CALIBRATED_EXPECTED_RETURN_NET_PCT" if calibrated
                                         else "UNCALIBRATED_RAW_SCORE_RESEARCH_ONLY") for item in row["selected"])


def test_missing_v3_calibration_snapshot_does_not_silently_switch_to_raw_selection():
    inputs = snapshot()
    inputs["v3"] = ready_v3()
    inputs["v3"].pop("calibration_inputs")
    seal(inputs)
    row = simulation.evaluate_snapshot(inputs, ["right_side_trend"])["strategy_rows"][0]
    assert row["status"] == "DATA_BLOCKED"
    assert not row["selected"]
    assert row["blocked_reasons"] == ["ORIGINAL_V3_CALIBRATION_INPUTS_UNPROVEN"]


def test_explicit_replay_latest_closed_date_uses_day_end_not_today(monkeypatch):
    from server.common import authoritative_market_clock as clock
    captured = []
    monkeypatch.setattr(clock, "authoritative_closed_trade_date", lambda _engine, now=None: TARGET)
    monkeypatch.setattr(simulation, "_prepare_v2", lambda _engine, target, decision: captured.append(decision) or ready_v2())
    monkeypatch.setattr(simulation, "_prepare_v3", lambda _engine, _kline, target, decision: captured.append(decision) or ready_v3())
    inputs = simulation.prepare_inputs(object(), object(), TARGET, run_mode="REPLAY")
    assert inputs["mode"] == "REPLAY"
    assert captured == [datetime(2026, 9, 30, 23, 59, 59)] * 2
    with pytest.raises(ValueError, match="mode is invalid"):
        simulation.prepare_inputs(object(), object(), TARGET, run_mode="UNKNOWN")


def test_target_day_truth_missing_blocks_both_sources_before_any_history_load(monkeypatch):
    from contextlib import contextmanager
    from server.common import qmt_daily_market_truth as truth_module
    from server.common import strategy_daily_input_window as window_module
    from server.trading_v3 import daily_features

    calls = []
    @contextmanager
    def view(engine):
        yield engine
    def missing(connection, **kwargs):
        calls.append((connection, kwargs))
        raise RuntimeError("no completed QMT daily attestation covers the requested range")
    def must_not_load(*args, **kwargs):
        pytest.fail("Missing target-day truth must not run expensive historical loaders")
    monkeypatch.setattr(window_module, "daily_input_snapshot", view)
    monkeypatch.setattr(truth_module, "load_qmt_daily_market_truth", missing)
    monkeypatch.setattr(production, "load_kline_features", must_not_load)
    monkeypatch.setattr(daily_features, "load_daily_feature_universe", must_not_load)
    cutoff = datetime(2026, 9, 30, 23, 59, 59)
    primary, kline = object(), object()
    with pytest.raises(RuntimeError, match="V2_PRIMARY_TARGET_DAY_TRUTH_UNAVAILABLE:2026-09-30"):
        simulation._prepare_v2(primary, TARGET, cutoff)
    with pytest.raises(RuntimeError, match="V3_KLINE_TARGET_DAY_TRUTH_UNAVAILABLE:2026-09-30"):
        simulation._prepare_v3(primary, kline, TARGET, cutoff)
    assert [item[0] for item in calls] == [primary, kline]
    assert all(item[1] == {"start_date": TARGET, "end_date": TARGET,
                           "decision_known_at": cutoff} for item in calls)


def test_target_day_truth_success_still_calls_original_full_history_loaders(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from server.common import qmt_daily_market_truth as truth_module
    from server.common import strategy_daily_input_window as window_module
    from server.trading_v3 import daily_features

    calls = []
    @contextmanager
    def view(engine):
        yield engine
    monkeypatch.setattr(window_module, "daily_input_snapshot", view)
    monkeypatch.setattr(truth_module, "load_qmt_daily_market_truth", lambda *args, **kwargs:
                        SimpleNamespace(requested_sessions=(TARGET,), attested_row_count=5000))
    def v2_loader(engine, target, **kwargs):
        calls.append(("V2", engine, target, kwargs))
        raise RuntimeError("FULL_60_SESSION_LOADER_CALLED")
    def v3_loader(primary, kline, **kwargs):
        calls.append(("V3", primary, kline, kwargs))
        raise RuntimeError("FULL_70_SESSION_LOADER_CALLED")
    monkeypatch.setattr(production, "load_kline_features", v2_loader)
    monkeypatch.setattr(daily_features, "load_daily_feature_universe", v3_loader)
    cutoff = datetime(2026, 9, 30, 23, 59, 59)
    primary, kline = object(), object()
    with pytest.raises(RuntimeError, match="FULL_60_SESSION_LOADER_CALLED"):
        simulation._prepare_v2(primary, TARGET, cutoff)
    with pytest.raises(RuntimeError, match="FULL_70_SESSION_LOADER_CALLED"):
        simulation._prepare_v3(primary, kline, TARGET, cutoff)
    assert calls == [("V2", primary, TARGET, {"decision_known_at": cutoff}),
                     ("V3", primary, kline, {"as_of": date.fromisoformat(TARGET),
                                              "context_cutoff_at": cutoff, "limit": 5000})]


def test_target_day_error_never_exposes_connection_exception_text(monkeypatch):
    from contextlib import contextmanager
    from server.common import qmt_daily_market_truth as truth_module
    from server.common import strategy_daily_input_window as window_module

    @contextmanager
    def view(engine):
        yield engine
    def failure(*args, **kwargs):
        raise ValueError("connection-password=example")
    monkeypatch.setattr(window_module, "daily_input_snapshot", view)
    monkeypatch.setattr(truth_module, "load_qmt_daily_market_truth", failure)
    with pytest.raises(RuntimeError) as raised:
        simulation._require_target_daily_truth(object(), TARGET, datetime(2026, 9, 30, 23, 59, 59), "V3_KLINE")
    assert "password" not in str(raised.value)
    assert str(raised.value).endswith("ValueError")
