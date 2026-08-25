import inspect
from datetime import date, timedelta

import pandas as pd

from server.common.pit_execution_guard import build_open_execution_receipt
from tools.backtest_unified_screener import (
    HORIZONS,
    PRESETS,
    _benchmark_comparison,
    _catalog_coverage_failures,
    _data_audit,
    _execution_summary,
    _execution_benchmark_comparison,
    _forward_execution_outcome,
    _forward_return,
    _load_prices,
    _load_persisted_screener_runs,
    _market_truth_window,
    _release_decision,
    _freeze_screener_run_receipt,
    _collect_signals,
    _simulate_shared_account,
    _screener_run_failures,
    _source_screener_run_key,
    _statistical_family_gate,
    _summary,
    _validate_screener_run_receipt,
)


def test_unified_backtest_uses_immutable_calendar_and_catalog_not_prefix_regex():
    calendar_source = inspect.getsource(_market_truth_window)
    price_source = inspect.getsource(_load_prices)

    assert "load_trade_calendar_receipt" in calendar_source
    assert "load_stock_catalog" in calendar_source
    assert "load_qmt_daily_market_truth" in calendar_source
    assert "si_trade_calendar" not in calendar_source
    assert "qmt_stock_catalog_member" in price_source
    assert "qmt_kline_attestation_row" in price_source
    assert "attestation.created_at<=:run_finished_at" in price_source
    assert "attestation.run_id=BINARY :selected_run_id" in price_source
    assert "attestation.source_data_version" in price_source
    assert "REGEXP" not in price_source


def test_historical_screener_loader_is_read_only_and_never_recomputes_today():
    source = inspect.getsource(_load_persisted_screener_runs)

    assert "st_screener_run_history" in source
    assert "st_screener_run_result" in source
    assert "_run_preset" not in source
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "INSERT INTO" not in source


def test_catalog_coverage_keeps_beijing_members_and_blocks_missing_day():
    class _Catalog:
        def eligible_codes(self, _day):
            return ["000001", "830001", "920001"]

    frame = pd.DataFrame([
        {"trade_date": "2026-08-24", "stock_code": "000001"},
        {"trade_date": "2026-08-24", "stock_code": "830001"},
    ])

    failures = _catalog_coverage_failures(
        frame,
        trade_dates=["2026-08-24"],
        catalog=_Catalog(),
    )

    assert failures["2026-08-24"]["missing"] == ["920001"]


def test_forward_return_chains_official_reference_prices():
    dates = ["2026-07-10", "2026-07-13", "2026-07-14", "2026-07-15"]
    prices = {
        ("000001", "2026-07-13"): {
            "open": 10.0,
            "high": 10.6,
            "low": 9.9,
            "close": 10.5,
            "volume": 100_000,
            "amount": 1_000_000,
            "pre_close": 9.8,
        },
        ("000001", "2026-07-14"): {
            "open": 5.1,
            "high": 5.3,
            "low": 5.0,
            "close": 5.25,
            "volume": 100_000,
            "amount": 500_000,
            # Official ex-right reference is 5.0, not prior raw close 10.5.
            "pre_close": 5.0,
        },
        ("000001", "2026-07-15"): {
            "open": 5.2,
            "high": 5.3,
            "low": 4.9,
            "close": 5.0,
            "volume": 100_000,
            "amount": 500_000,
            "pre_close": 5.25,
        },
    }

    value, reason = _forward_return(
        prices,
        dates,
        {value: index for index, value in enumerate(dates)},
        "2026-07-10",
        "000001",
        2,
    )

    assert reason == "ok"
    assert value is not None
    assert round(value, 6) == 0.05


def test_forward_return_rejects_missing_official_reference():
    dates = ["2026-07-10", "2026-07-13", "2026-07-14"]
    prices = {
        ("000001", "2026-07-13"): {
            "open": 10,
            "high": 10.3,
            "low": 9.9,
            "close": 10.2,
            "volume": 100_000,
            "amount": 1_000_000,
            "pre_close": 9.9,
        },
        ("000001", "2026-07-14"): {
            "open": 10.2,
            "high": 10.4,
            "low": 10.1,
            "close": 10.3,
            "volume": 100_000,
            "amount": 1_000_000,
            "pre_close": None,
        },
    }

    value, reason = _forward_return(
        prices,
        dates,
        {value: index for index, value in enumerate(dates)},
        "2026-07-10",
        "000001",
        1,
    )

    assert value is None
    assert reason == "missing_official_reference_price"


def test_summary_reports_gross_and_cost_adjusted_metrics():
    result = _summary([0.01, -0.005], round_trip_cost=0.002)

    assert result["sample"] == 2
    assert result["gross_average_pct"] == 0.25
    assert result["net_average_pct"] == 0.05
    assert result["net_average_win_loss"] == 1.1429
    assert result["net_max_drawdown_pct"] == 0.7


def test_backtest_uses_confirmed_multi_horizon_contract():
    assert HORIZONS == (1, 5, 20)


def test_release_decision_requires_all_evidence_and_never_grants_orders():
    guard = {
        "valid": True,
        "passed": True,
        "net_expectancy_one_sided_95_lcb_pct": 0.1,
        "effective_sample_size": 80,
        "profit_factor": {"one_sided_95_lcb": 1.2},
        "payoff_ratio": {"one_sided_95_lcb": 1.1},
    }
    metrics = {
        horizon: {
            "sample": 100,
            "net_profit_factor": 1.5,
            "net_average_win_loss": 1.2,
            "execution_evidence_valid": True,
            "execution_disposition_coverage": 1.0,
            "execution_status_counts": {"FILLED": 100},
            "fixed_capital_verified": True,
            "cash_constraint_breach_count": 0,
            "initial_capital_cny": 1_000_000,
            "minimum_effective_sample_size": 60,
            "statistical_guard": guard,
        }
        for horizon in ("T+1", "T+5", "T+20")
    }
    audit = {
        "expected_trade_dates": [str(index) for index in range(20)],
        "actual_trade_dates": [str(index) for index in range(20)],
        "row_count": 1000,
        "duplicate_business_keys": 0,
        "bad_ohlc": 0,
        "invalid_prices": 0,
        "missing_pre_close_rows": 0,
        "inconsistent_reference_return_rows": 0,
    }

    family = {
        "complete_frozen_family": True,
        "decisions_by_key": {
            f"__combined__|{horizon}": {"passed": True}
            for horizon in ("T+1", "T+5", "T+20")
        },
    }
    result = _release_decision(
        metrics,
        audit,
        20,
        statistical_family=family,
    )

    assert result["status"] == "PASS_ADVISORY_RELEASE"
    assert result["passed"] is True
    assert result["order_authority"] is False
    assert result["automatic_real_order_submission"] is False

    metrics["T+20"]["sample"] = 79
    blocked = _release_decision(
        metrics,
        audit,
        20,
        statistical_family=family,
    )
    assert blocked["status"] == "SHADOW_ONLY"
    assert blocked["checks"]["T+20_evidence"] is False


def test_release_decision_rejects_high_profit_factor_when_execution_data_blocked():
    metrics = {
        horizon: {
            "sample": 100,
            "net_profit_factor": 9.0,
            "net_average_win_loss": 4.0,
            "execution_evidence_valid": True,
            "execution_disposition_coverage": 1.0,
            "execution_status_counts": {"FILLED": 100},
        }
        for horizon in ("T+1", "T+5", "T+20")
    }
    metrics["T+5"]["execution_evidence_valid"] = False
    metrics["T+5"]["execution_status_counts"] = {
        "FILLED": 99,
        "DATA_BLOCKED": 1,
    }
    audit = {
        "expected_trade_dates": [str(index) for index in range(20)],
        "actual_trade_dates": [str(index) for index in range(20)],
        "row_count": 1000,
        "duplicate_business_keys": 0,
        "bad_ohlc": 0,
        "invalid_prices": 0,
        "missing_pre_close_rows": 0,
        "inconsistent_reference_return_rows": 0,
    }

    result = _release_decision(metrics, audit, 20)

    assert result["passed"] is False
    assert result["horizons"]["T+5"]["no_data_blocked"] is False


def test_forward_execution_records_locked_entry_and_unresolved_locked_exit():
    dates = ["2026-07-10", "2026-07-13", "2026-07-14"]
    index = {value: position for position, value in enumerate(dates)}
    locked_entry = {
        ("000001", "2026-07-13"): {
            "open": 10.5, "high": 10.5, "low": 10.5, "close": 10.5,
            "pre_close": 10.0, "volume": 100_000, "amount": 1_000_000,
        },
        ("000001", "2026-07-14"): {
            "open": 10.6, "high": 10.8, "low": 10.4, "close": 10.7,
            "pre_close": 10.5, "volume": 100_000, "amount": 1_000_000,
        },
    }
    entry_outcome = _forward_execution_outcome(
        locked_entry, dates, index, "2026-07-10", "000001", 1,
        order_value_cny=10_000, base_round_trip_cost=0.002,
    )
    locked_exit = {
        ("000001", "2026-07-13"): {
            "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.1,
            "pre_close": 10.0, "volume": 100_000, "amount": 1_000_000,
        },
        ("000001", "2026-07-14"): {
            "open": 9.5, "high": 9.5, "low": 9.5, "close": 9.5,
            "pre_close": 10.1, "volume": 100_000, "amount": 1_000_000,
        },
    }
    exit_outcome = _forward_execution_outcome(
        locked_exit, dates, index, "2026-07-10", "000001", 1,
        order_value_cny=10_000, base_round_trip_cost=0.002,
    )

    assert entry_outcome["status"] == "KNOWN_UNFILLED"
    assert entry_outcome["reason"] == "locked_limit_up"
    assert exit_outcome["status"] == "HYPOTHETICAL_ONLY"
    assert exit_outcome["reason"] == "missing_immutable_open_receipt"


def test_execution_summary_nulls_net_metrics_when_any_signal_has_unknown_truth():
    result = _execution_summary([
        {
            "status": "FILLED",
            "reason": "ok",
            "gross_return": 0.05,
            "net_return": 0.04,
            "estimated_cost_rate": 0.01,
        },
        {
            "status": "DATA_BLOCKED",
            "reason": "missing_holding_bar",
            "gross_return": None,
            "net_return": None,
        },
    ])

    assert result["execution_disposition_coverage"] == 1.0
    assert result["execution_evidence_valid"] is False
    assert result["execution_status_counts"]["DATA_BLOCKED"] == 1
    assert result["net_average_pct"] is None
    assert result["net_profit_factor"] is None


def test_entry_capacity_uses_prior_adv_not_future_full_day_turnover():
    dates = [f"2026-07-{day:02d}" for day in range(6, 14)]
    prices = {}
    for index, day in enumerate(dates):
        prices[("000001", day)] = {
            "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.1,
            "pre_close": 10.0, "volume": 100_000,
            # Entry-day afternoon volume is enormous but must not set capacity.
            "amount": 1_000_000_000 if index == 6 else 1_000_000,
        }
    prices[("000001", dates[6])]["open_execution_receipt"] = (
        build_open_execution_receipt(
            stock_code="000001",
            trade_date=dates[6],
            execution_price=10.0,
            observed_at=f"{dates[6]}T09:31:00",
            source_provider="QMT_FIRST_1MIN",
            source_payload_hash="b" * 64,
        )
    )

    outcome = _forward_execution_outcome(
        prices,
        dates,
        {value: index for index, value in enumerate(dates)},
        dates[5],
        "000001",
        1,
        order_value_cny=100_000,
        base_round_trip_cost=0.002,
    )

    assert outcome["status"] == "FILLED"
    assert outcome["accepted_order_value_cny"] == 50_000
    assert outcome["entry_participation_rate"] == 0.05


def test_benchmark_comparison_reports_cost_adjusted_excess_return():
    result = _benchmark_comparison(
        [(0.01, 0.004), (-0.005, -0.01)],
        round_trip_cost=0.002,
    )

    assert result["benchmark_sample"] == 2
    assert result["market_average_pct"] == -0.3
    assert result["gross_excess_average_pct"] == 0.55
    assert result["net_excess_average_pct"] == 0.35


def test_execution_benchmark_uses_each_fill_actual_capacity_cost():
    result = _execution_benchmark_comparison([
        (0.02, 0.015, 0.01),
        (0.01, 0.002, 0.004),
    ])

    assert result["gross_excess_average_pct"] == 0.8
    assert result["net_excess_average_pct"] == 0.15
    assert result["net_excess_win_rate_pct"] == 50.0


def test_data_audit_detects_inconsistent_pre_close_return():
    frame = pd.DataFrame([{
        "stock_code": "000001",
        "trade_date": "2026-07-13",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "volume": 100,
        "amount": 1000,
        "pre_close": 10,
        "change_pct": 1,
    }])

    audit = _data_audit(frame, ["2026-07-13"])

    assert audit["inconsistent_reference_return_rows"] == 1


def test_backtest_blocks_if_any_expected_screener_run_silently_disappears():
    day = "2026-07-13"
    audit = [
        {
            "date": day,
            "preset": str(preset["key"]),
            "status": "accepted",
            "receipt_valid": True,
            "receipt_root_hash": "a" * 64,
            "pit_common_receipt_root_hash": "b" * 64,
            "decision_at": f"{day}T15:30:00",
        }
        for preset in PRESETS
    ]
    assert _screener_run_failures([day], audit)["valid"] is True

    missing = _screener_run_failures([day], audit[:-1])
    rejected_audit = [dict(item) for item in audit]
    rejected_audit[0]["status"] = "error"
    rejected = _screener_run_failures([day], rejected_audit)

    assert missing["valid"] is False
    assert missing["missing_run_count"] == 1
    assert rejected["valid"] is False
    assert rejected["rejected_run_count"] == 1


def _stored_run(day, preset, *, blocked=False, score=88.0):
    row = {
        "rank": 1,
        "stock_code": "000001",
        "stock_name": "样本",
        "score": score,
        "sw_industry_code": "801010",
        "pit_score_binding_verified": True,
        "finance_pit_verified": True,
        "event_pit_verified": True,
        "pit_strategy_status": "PIT_AVAILABLE",
        "pit_common_cutoff_status": "PIT_AVAILABLE",
        "pit_decision_at": f"{day}T15:30:00",
        "pit_fact_cutoff_at": f"{day}T15:20:00",
        "pit_common_receipt_root_hash": "c" * 64,
    }
    if blocked:
        row["pit_strategy_status"] = "PIT_DATA_BLOCKED"
    result = {
        "status": "ok",
        "data_date": day,
        "evidence_date": None,
        "observed_at": None,
        "freshness": "historical_close",
        "selector": {"model_fingerprint": "selector-v1"},
        "data": [row],
    }
    request = {
        "preset": preset,
        "as_of_date": day,
        "universe": "market",
        "top": 10,
        "filters": {},
    }
    run_key = _source_screener_run_key(request, result)
    return {
        "result": result,
        "source_run_uid": run_key[:32],
        "source_run_key": run_key,
        "source_generated_at": f"{day}T15:31:00",
        "request_payload": request,
    }


def test_screener_receipt_freezes_exact_decision_and_survives_later_mutation():
    day = "2026-08-24"
    stored = _stored_run(day, "trend_breakout")
    receipt = _freeze_screener_run_receipt(
        target_date=day,
        preset="trend_breakout",
        result=stored["result"],
        source_run_uid=stored["source_run_uid"],
        source_run_key=stored["source_run_key"],
        source_generated_at=stored["source_generated_at"],
        request_payload=stored["request_payload"],
    )

    stored["result"]["data"][0]["score"] = 999
    stored["result"]["data"][0]["pit_common_receipt_root_hash"] = "d" * 64
    stored["result"]["data"][0]["pit_strategy_status"] = "PIT_DATA_BLOCKED"

    assert _validate_screener_run_receipt(receipt) is True
    assert receipt["decision_at"] == f"{day}T15:30:00"
    assert receipt["candidates"][0]["score"] == 88.0
    assert receipt["pit_common_receipt_root_hash"] == "c" * 64


def test_one_pit_blocked_candidate_blocks_the_whole_preset_day_denominator():
    day = "2026-08-24"
    persisted = {
        (day, str(preset["key"])): _stored_run(
            day,
            str(preset["key"]),
            blocked=str(preset["key"]) == "trend_breakout",
        )
        for preset in PRESETS
    }

    signals, audit = _collect_signals(
        [day],
        10,
        persisted_runs=persisted,
    )
    contract = _screener_run_failures([day], audit)
    blocked = next(
        item for item in audit if item["preset"] == "trend_breakout"
    )

    assert blocked["status"] == "excluded"
    assert blocked["count"] == 0
    assert "trend_breakout" not in signals
    assert contract["valid"] is False
    assert contract["rejected_run_count"] == 1


def _shared_account_fixture():
    start = date(2026, 8, 10)
    dates = [(start + timedelta(days=index)).isoformat() for index in range(8)]
    codes = ["000001", "000002", "000003"]
    prices = {}
    for code in codes:
        for day in dates:
            prices[(code, day)] = {
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "pre_close": 10.0,
                "volume": 10_000_000,
                "amount": 100_000_000,
            }
        entry_day = dates[6]
        prices[(code, entry_day)]["open_execution_receipt"] = (
            build_open_execution_receipt(
                stock_code=code,
                trade_date=entry_day,
                execution_price=10.0,
                observed_at=f"{entry_day}T09:31:00",
                source_provider="QMT_FIRST_1MIN",
                source_payload_hash="e" * 64,
            )
        )
    signals = [
        {
            "signal_date": dates[5],
            "stock_code": code,
            "stock_name": code,
            "rank": rank,
            "score": 100 - rank,
            "industry": "801010",
            "preset": "trend_breakout",
            "screener_receipt_root_hash": "f" * 64,
        }
        for rank, code in enumerate(codes, 1)
    ]
    return dates, prices, signals


def test_shared_account_prevents_clone_inflation_and_same_day_over_cash():
    dates, prices, signals = _shared_account_fixture()
    index = {day: offset for offset, day in enumerate(dates)}
    base = _simulate_shared_account(
        signals,
        prices,
        dates,
        index,
        1,
        initial_capital_cny=100_000,
        maximum_stock_weight=0.60,
        maximum_industry_weight=1.0,
    )
    cloned = _simulate_shared_account(
        [*signals, dict(signals[0])],
        prices,
        dates,
        index,
        1,
        initial_capital_cny=100_000,
        maximum_stock_weight=0.60,
        maximum_industry_weight=1.0,
    )

    accepted = sum(
        row.get("accepted_notional_cny", 0.0)
        for row in base["deterministic_priority"]
    )
    assert accepted == 100_000
    assert base["cash_constraint_breach_count"] == 0
    assert base["maximum_observed_concurrent_positions"] == 2
    assert base["daily_nav_records"] == cloned["daily_nav_records"]
    assert base["statistical_guard"]["input_hash"] == (
        cloned["statistical_guard"]["input_hash"]
    )
    assert cloned["execution_reason_counts"]["duplicate_signal_identity"] == 1


def test_afternoon_turnover_without_open_receipt_is_hypothetical_only():
    dates, prices, signals = _shared_account_fixture()
    entry_day = dates[6]
    prices[("000001", entry_day)].pop("open_execution_receipt")
    prices[("000001", entry_day)]["amount"] = 10_000_000_000

    outcome = _forward_execution_outcome(
        prices,
        dates,
        {day: offset for offset, day in enumerate(dates)},
        signals[0]["signal_date"],
        "000001",
        1,
        order_value_cny=50_000,
        base_round_trip_cost=0.002,
    )

    assert outcome["status"] == "HYPOTHETICAL_ONLY"
    assert outcome["funding_eligible"] is False


def test_statistical_family_keeps_every_failed_preset_horizon_sibling():
    family = _statistical_family_gate({}, {})
    expected_count = (len(PRESETS) + 1) * len(HORIZONS)

    assert family["valid"] is True
    assert family["complete_frozen_family"] is True
    assert len(family["trial_inventory"]) == expected_count
    assert len(family["p_values"]) == expected_count
    assert set(family["p_values"].values()) == {1.0}
