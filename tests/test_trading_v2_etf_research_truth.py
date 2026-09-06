from __future__ import annotations

import inspect
from datetime import date, timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.db.migrations_v2 import MIGRATIONS
from server.trading_v2 import job_worker
from tools import backtest_etf_ensemble as ensemble
from tools import backtest_etf_robust as robust


def _snapshot_row(**overrides):
    row = {
        "etf_code": "510300",
        "short_name": "300ETF",
        "trade_date": "2026-08-24",
        "adjust_type": 0,
        "data_source": job_worker.ETF_RESEARCH_DATA_SOURCE,
        "data_version": "a" * 64,
        "received_at": "2026-08-24 16:00:00",
        "open": 4.0,
        "close": 4.1,
        "pre_close": 4.05,
        "amount": 100_000_000,
        "validation_status": "passed",
        "quality_status": "validated",
        "asset_class": "A股宽基",
        "classification_updated_at": "2026-08-24 16:30:00",
    }
    row.update(overrides)
    return row


def _dependency_snapshot_rows(
    session_count: int,
    *,
    low_liquidity_secondary: bool = False,
    secondary_listing_offset: int = 0,
):
    sessions = [
        (date(2019, 1, 1) + timedelta(days=offset)).isoformat()
        for offset in range(session_count)
    ]
    rows = []
    for code in ("510300", "511880", "159915"):
        first_offset = secondary_listing_offset if code == "159915" else 0
        for offset, session in enumerate(sessions[first_offset:], start=first_offset):
            amount = (
                1_000_000
                if code == "159915" and low_liquidity_secondary
                else 10_000_000
            )
            price = 10.0 + offset / 1000
            rows.append(_snapshot_row(
                etf_code=code,
                short_name=code,
                trade_date=session,
                open=price,
                close=price,
                pre_close=max(0.01, price - 0.001),
                amount=amount,
                list_date=sessions[first_offset],
                last_trade_date=None,
                instrument_status="active",
            ))
    return sessions, rows


def test_etf_truth_quarantines_current_classification_and_missing_revision_ledger():
    truth = job_worker._etf_research_truth_contract([_snapshot_row()])

    assert truth["native_unadjusted_prices_only"] is True
    assert truth["adjusted_history_rows_consumed"] is False
    assert truth["historical_classification_verified"] is False
    assert truth["current_classification_can_authorize_promotion"] is False
    assert truth["activation_eligible"] is False
    assert set(truth["promotion_blockers"]) == set(
        job_worker.ETF_MUTABLE_INPUT_BLOCKERS
    )
    assert len(truth["contract_hash"]) == 64


@pytest.mark.parametrize(
    "replacement",
    [
        {"adjust_type": 1},
        {"data_version": ""},
        {"received_at": None},
        {"pre_close": 0},
        {"classification_updated_at": None},
        {"data_source": "unexpected_provider"},
    ],
)
def test_etf_truth_rejects_non_native_or_unversioned_rows(replacement):
    with pytest.raises(RuntimeError):
        job_worker._etf_research_truth_contract([
            _snapshot_row(**replacement)
        ])


def test_etf_loader_derives_continuous_prices_without_adjusted_history(monkeypatch):
    rows = [
        {
            "etf_code": "510300",
            "short_name": "300ETF",
            "trade_date": "2026-08-21",
            "open": 9.8,
            "close": 10.0,
            "pre_close": 9.5,
            "amount": 10_000_000,
            "asset_class": "A股宽基",
        },
        {
            "etf_code": "510300",
            "short_name": "300ETF",
            "trade_date": "2026-08-24",
            "open": 10.5,
            "close": 11.0,
            "pre_close": 10.0,
            "amount": 12_000_000,
            "asset_class": "A股宽基",
        },
    ]
    observed = {}

    def reader(_engine, sql, **_kwargs):
        observed["sql"] = " ".join(sql.split())
        return rows

    monkeypatch.setattr(ensemble, "read_sql_rows", reader)

    data = ensemble.load_market_data(
        object(), "2026-08-21", "2026-08-24"
    )

    assert "k.adjust_type = 0" in observed["sql"]
    assert "k.adjust_type = 1" not in observed["sql"]
    assert data.close["510300"].tolist() == pytest.approx([10.0, 11.0])
    assert data.open["510300"].tolist() == pytest.approx([9.8, 10.5])


def test_etf_job_snapshot_never_selects_adjusted_history():
    source = inspect.getsource(job_worker._etf_backtest)

    assert "WHERE k.adjust_type = 0" in source
    assert "WHERE k.adjust_type = 1" not in source
    assert "ETF_MUTABLE_INPUT_BLOCKERS" in source


def test_formal_etf_replay_hashes_and_simulates_the_same_source_rows():
    source = inspect.getsource(job_worker._etf_backtest)
    contract_source = inspect.getsource(
        job_worker.registered_etf_dependency_data_contract
    )

    assert "with engine.begin() as connection" in source
    assert "k.data_source = :data_source" in source
    assert "registered_etf_dependency_data_contract(" in source
    assert "_etf_research_truth_contract(rows)" in contract_source
    assert "market_data_from_rows(rows)" in contract_source
    assert "_confirmed_etf_fee_profile(\n            connection" in source


def test_confirmed_fee_must_be_account_bound_and_is_usable_by_simulator():
    fee_row = {
        "fee_profile_version": "fee-v1",
        "effective_from": "2026-01-01",
        "effective_to": None,
        "security_type": "ETF",
        "buy_commission_rate": "0.001",
        "sell_commission_rate": "0.002",
        "minimum_commission": "7",
        "stamp_tax_sell_rate": "0.003",
        "transfer_fee_buy_rate": "0.004",
        "transfer_fee_sell_rate": "0.005",
        "other_fee_json": "{}",
        "evidence_hash": "b" * 64,
        "confirmation_status": "CONFIRMED",
    }

    class Result:
        def mappings(self):
            return self

        def all(self):
            return [fee_row]

    class Connection:
        def __init__(self):
            self.params = None

        def execute(self, _sql, params):
            self.params = params
            return Result()

    connection = Connection()
    profile = job_worker._confirmed_etf_fee_profile(
        connection,
        start_date="2026-08-01",
        end_date="2026-08-31",
    )
    assert connection.params["account_id"] == job_worker.ETF_RESEARCH_FEE_ACCOUNT_ID
    assert profile["usable"] is True
    assumptions = robust.ExecutionAssumptions(
        minimum_commission=profile["minimum_commission"],
        buy_commission_rate=profile["buy_commission_rate"],
        sell_commission_rate=profile["sell_commission_rate"],
        stamp_tax_sell_rate=profile["stamp_tax_sell_rate"],
        transfer_fee_buy_rate=profile["transfer_fee_buy_rate"],
        transfer_fee_sell_rate=profile["transfer_fee_sell_rate"],
    )
    market = ensemble.MarketData(
        open=pd.DataFrame(),
        close=pd.DataFrame(),
        amount=pd.DataFrame(),
        names={},
        asset_classes={},
    )
    assert robust._commission(
        "510300", 1_000, market, assumptions, side="BUY"
    ) == pytest.approx(11.0)
    assert robust._commission(
        "510300", 1_000, market, assumptions, side="SELL"
    ) == pytest.approx(15.0)


def test_confirmed_fee_query_runs_against_the_migrated_account_table():
    migration_sql = "\n".join(
        str(statement)
        for migration in MIGRATIONS
        for statement in migration["statements"]
    )
    assert "CREATE TABLE IF NOT EXISTS st_trade_account_v2" in migration_sql
    assert "CREATE TABLE IF NOT EXISTS st_fee_profile_v2" in migration_sql

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE st_trade_account_v2 (
                account_id TEXT PRIMARY KEY,
                fee_profile_version TEXT
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE st_fee_profile_v2 (
                fee_profile_version TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT,
                security_type TEXT NOT NULL,
                buy_commission_rate NUMERIC NOT NULL,
                sell_commission_rate NUMERIC NOT NULL,
                minimum_commission NUMERIC NOT NULL,
                stamp_tax_sell_rate NUMERIC NOT NULL,
                transfer_fee_buy_rate NUMERIC NOT NULL,
                transfer_fee_sell_rate NUMERIC NOT NULL,
                other_fee_json TEXT NOT NULL,
                evidence_hash TEXT NOT NULL,
                confirmation_status TEXT NOT NULL
            )
            """
        ))
        connection.execute(
            text(
                """
                INSERT INTO st_trade_account_v2
                    (account_id, fee_profile_version)
                VALUES (:account_id, :fee_profile_version)
                """
            ),
            {
                "account_id": job_worker.ETF_RESEARCH_FEE_ACCOUNT_ID,
                "fee_profile_version": "fee-v1",
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO st_fee_profile_v2
                    (fee_profile_version, effective_from, effective_to,
                     security_type, buy_commission_rate, sell_commission_rate,
                     minimum_commission, stamp_tax_sell_rate,
                     transfer_fee_buy_rate, transfer_fee_sell_rate,
                     other_fee_json, evidence_hash, confirmation_status)
                VALUES
                    ('fee-v1', '2026-01-01', NULL, 'ETF', 0.0001, 0.0001,
                     5, 0, 0, 0, '{}', :evidence_hash, 'CONFIRMED')
                """
            ),
            {"evidence_hash": "c" * 64},
        )
        profile = job_worker._confirmed_etf_fee_profile(
            connection,
            start_date="2026-08-01",
            end_date="2026-08-31",
        )

    assert profile["usable"] is True
    assert profile["fee_profile_version"] == "fee-v1"
    assert profile["evidence_hash"] == "c" * 64


def test_double_cost_scales_all_confirmed_fees_and_slippage():
    market = ensemble.MarketData(
        open=pd.DataFrame(),
        close=pd.DataFrame(),
        amount=pd.DataFrame(),
        names={},
        asset_classes={"510300": "A股宽基"},
    )
    base = robust.ExecutionAssumptions(
        minimum_commission=7,
        buy_commission_rate=0.001,
        sell_commission_rate=0.002,
        stamp_tax_sell_rate=0.003,
        transfer_fee_buy_rate=0.004,
        transfer_fee_sell_rate=0.005,
    )
    doubled = robust.ExecutionAssumptions(
        minimum_commission=7,
        buy_commission_rate=0.001,
        sell_commission_rate=0.002,
        stamp_tax_sell_rate=0.003,
        transfer_fee_buy_rate=0.004,
        transfer_fee_sell_rate=0.005,
        cost_multiplier=2.0,
    )

    for notional in (1_000, 100_000):
        for side in ("BUY", "SELL"):
            base_fee = robust._commission(
                "510300", notional, market, base, side=side
            )
            doubled_fee = robust._commission(
                "510300", notional, market, doubled, side=side
            )
            assert doubled_fee == pytest.approx(base_fee * 2)
    base_slippage, participation = robust._slippage_rate(
        "510300", 1_000, 100_000, market, base
    )
    doubled_slippage, doubled_participation = robust._slippage_rate(
        "510300", 1_000, 100_000, market, doubled
    )
    assert doubled_participation == pytest.approx(participation)
    assert doubled_slippage == pytest.approx(base_slippage * 2)


def test_formal_equity_curve_starts_with_initial_capital():
    dates = pd.to_datetime(["2026-08-03", "2026-08-04", "2026-08-05"])
    data = ensemble.MarketData(
        open=pd.DataFrame({"510300": [10.0, 10.0, 10.0]}, index=dates),
        close=pd.DataFrame({"510300": [10.0, 10.0, 9.0]}, index=dates),
        amount=pd.DataFrame(
            {"510300": [1_000_000.0, 1_000_000.0, 1_000_000.0]},
            index=dates,
        ),
        names={"510300": "300ETF"},
        asset_classes={"510300": "A股宽基"},
    )
    equity, _rebalances, _trades = robust.simulate_realistic(
        data,
        {dates[-1]: pd.Series({"510300": 1.0})},
        contexts=None,
        start_date="2026-08-05",
        end_date="2026-08-05",
        assumptions=robust.ExecutionAssumptions(initial_capital=10_000),
    )

    assert equity.iloc[0] == pytest.approx(10_000)
    assert equity.index[0] < dates[-1]
    performance = ensemble.performance_metrics(
        equity / 10_000,
        evaluation_start_date="2026-08-05",
    )
    assert performance["start_date"] == "2026-08-05"
    assert performance["trading_days"] == 1
    assert performance["max_drawdown"] < 0


def test_first_replay_day_uses_each_etfs_last_available_close_for_limit_check():
    dates = pd.to_datetime(["2026-08-03", "2026-08-04", "2026-08-05"])
    data = ensemble.MarketData(
        open=pd.DataFrame(
            {
                "510300": [10.0, 10.0, 10.0],
                "159915": [10.0, float("nan"), 12.0],
            },
            index=dates,
        ),
        close=pd.DataFrame(
            {
                "510300": [10.0, 10.0, 10.0],
                "159915": [10.0, float("nan"), 12.0],
            },
            index=dates,
        ),
        amount=pd.DataFrame(
            {
                "510300": [10_000_000.0] * 3,
                "159915": [10_000_000.0, float("nan"), 10_000_000.0],
            },
            index=dates,
        ),
        names={"510300": "300ETF", "159915": "创业板ETF"},
        asset_classes={"510300": "A股宽基", "159915": "A股宽基"},
    )
    _equity, _rebalances, fills = robust.simulate_realistic(
        data,
        {dates[-1]: pd.Series({"510300": 0.0, "159915": 1.0})},
        contexts=None,
        start_date="2026-08-05",
        end_date="2026-08-05",
        assumptions=robust.ExecutionAssumptions(initial_capital=10_000),
    )

    row = fills.loc[fills["etf_code"] == "159915"].iloc[0]
    assert row["status"] == "blocked_limit_up"
    assert int(row["requested_units"]) > 0


def test_incomplete_requested_end_is_rejected_instead_of_silently_clamped():
    calendar = pd.to_datetime(["2026-08-03", "2026-08-04"])

    with pytest.raises(ValueError, match="2026-08-04"):
        job_worker._require_complete_etf_end(
            calendar,
            start_date="2026-08-03",
            end_date="2026-08-05",
        )

    assert job_worker._require_complete_etf_end(
        calendar,
        start_date="2026-08-03",
        end_date="2026-08-04",
    ) == "2026-08-04"


def _target_coverage_fixture(*, stale_secondary: bool):
    dates = pd.to_datetime(["2026-08-03", "2026-08-04", "2026-08-05"])
    secondary_close = [20.0, 20.5, float("nan") if stale_secondary else 21.0]
    data = ensemble.MarketData(
        open=pd.DataFrame(
            {"510300": [10.0, 10.1, 10.2], "513500": [20.0, 20.4, 20.8]},
            index=dates,
        ),
        close=pd.DataFrame(
            {"510300": [10.0, 10.1, 10.2], "513500": secondary_close},
            index=dates,
        ),
        amount=pd.DataFrame(
            {"510300": [1_000_000.0] * 3, "513500": [1_000_000.0] * 3},
            index=dates,
        ),
        names={"510300": "300ETF", "513500": "标普ETF"},
        asset_classes={"510300": "A股宽基", "513500": "海外权益"},
    )
    targets = {
        dates[0]: pd.Series({"510300": 0.5, "513500": 0.5}),
    }
    return data, targets


def test_formal_target_coverage_rejects_a_target_etf_that_stops_early():
    data, targets = _target_coverage_fixture(stale_secondary=True)
    audit = job_worker._target_data_coverage_audit(
        data,
        targets,
        end_date="2026-08-05",
    )

    assert audit["per_code"]["510300"]["close_cutoff"] == "2026-08-05"
    assert audit["per_code"]["513500"]["close_cutoff"] == "2026-08-04"
    assert audit["per_code"]["513500"]["gaps"] == [
        "CLOSE_CUTOFF_BEFORE_END"
    ]
    with pytest.raises(RuntimeError, match="513500.*2026-08-04"):
        job_worker._require_target_data_coverage(audit)

    worker_source = inspect.getsource(job_worker._etf_backtest)
    assert "_require_target_data_coverage(monthly_data_audit)" in worker_source


def test_formal_target_coverage_passes_when_every_target_reaches_end():
    data, targets = _target_coverage_fixture(stale_secondary=False)
    audit = job_worker._target_data_coverage_audit(
        data,
        targets,
        end_date="2026-08-05",
    )

    job_worker._require_target_data_coverage(audit)
    assert audit["complete"] is True
    assert audit["gaps"] == []
    assert audit["per_code"]["513500"][
        "required_execution_open_dates"
    ] == ["2026-08-03"]


def test_formal_target_coverage_rejects_an_interior_native_bar_gap():
    data, targets = _target_coverage_fixture(stale_secondary=False)
    expected = ["2026-08-03", "2026-08-04", "2026-08-05"]
    native_sessions = {
        "510300": set(expected),
        "513500": {"2026-08-03", "2026-08-05"},
    }

    audit = job_worker._target_data_coverage_audit(
        data,
        targets,
        end_date="2026-08-05",
        expected_sessions=expected,
        native_session_dates=native_sessions,
    )

    assert audit["per_code"]["513500"]["close_cutoff"] == "2026-08-05"
    assert audit["per_code"]["513500"][
        "missing_native_close_pre_close_dates"
    ] == ["2026-08-04"]
    assert "NATIVE_CLOSE_PRE_CLOSE_MISSING" in audit["per_code"][
        "513500"
    ]["gaps"]
    with pytest.raises(RuntimeError, match="2026-08-04"):
        job_worker._require_target_data_coverage(audit)

    worker_source = inspect.getsource(job_worker._etf_backtest)
    contract_source = inspect.getsource(
        job_worker.registered_etf_dependency_data_contract
    )
    assert "registered_etf_dependency_data_contract(" in worker_source
    assert "native_session_dates = _native_etf_session_dates" in contract_source


def test_formal_native_coverage_includes_pre_start_dependency_warmup():
    data, targets = _target_coverage_fixture(stale_secondary=False)
    dependency_sessions = ["2026-08-03", "2026-08-04", "2026-08-05"]
    expected_by_code = {
        "510300": set(dependency_sessions),
        "513500": set(dependency_sessions),
    }
    native_sessions = {
        "510300": set(dependency_sessions),
        # The missing session is before a hypothetical 2026-08-05 request
        # start, but it still affects the first signal's warmup features.
        "513500": {"2026-08-04", "2026-08-05"},
    }

    audit = job_worker._target_data_coverage_audit(
        data,
        targets,
        end_date="2026-08-05",
        expected_sessions=dependency_sessions,
        expected_sessions_by_code=expected_by_code,
        native_session_dates=native_sessions,
    )

    assert audit["expected_session_start"] == "2026-08-03"
    assert audit["per_code"]["513500"][
        "missing_native_close_pre_close_dates"
    ] == ["2026-08-03"]
    with pytest.raises(RuntimeError, match="2026-08-03"):
        job_worker._require_target_data_coverage(audit)

    worker_source = inspect.getsource(job_worker._etf_backtest)
    contract_source = inspect.getsource(
        job_worker.registered_etf_dependency_data_contract
    )
    assert "start_date=dependency_start" in contract_source
    assert "expected_sessions_by_code=expected_sessions_by_code" in worker_source


@pytest.mark.parametrize(
    ("low_liquidity", "listing_offset"),
    [(True, 0), (False, 1)],
)
def test_shared_dependency_contract_enforces_exact_frozen_universe(
    monkeypatch,
    low_liquidity,
    listing_offset,
):
    sessions, rows = _dependency_snapshot_rows(
        120,
        low_liquidity_secondary=low_liquidity,
        secondary_listing_offset=listing_offset,
    )
    monkeypatch.setattr(
        job_worker,
        "_formal_etf_expected_sessions",
        lambda *_args, **_kwargs: sessions,
    )

    with pytest.raises(RuntimeError, match="missing=159915"):
        job_worker.registered_etf_dependency_data_contract(
            object(),
            eligible_codes=("159915", "510300", "511880"),
            dependency_start=sessions[0],
            end_date=sessions[-1],
            snapshot_rows=rows,
        )


def test_shared_dependency_contract_rejects_short_window_without_execution(
    monkeypatch,
):
    sessions, rows = _dependency_snapshot_rows(300)
    monkeypatch.setattr(
        job_worker,
        "_formal_etf_expected_sessions",
        lambda *_args, **_kwargs: sessions,
    )
    final_month_midpoint = date.fromisoformat(sessions[-1]).replace(
        day=15
    ).isoformat()

    with pytest.raises(RuntimeError, match="no target schedule"):
        job_worker.registered_etf_dependency_data_contract(
            object(),
            eligible_codes=("159915", "510300", "511880"),
            dependency_start=sessions[0],
            end_date=sessions[-1],
            snapshot_rows=rows,
            backtest_start=final_month_midpoint,
        )
