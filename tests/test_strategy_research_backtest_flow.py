from __future__ import annotations

import inspect
import json
from datetime import date

import pytest
import pandas as pd
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import create_engine, text

from server.api.routers import sim_trade
from server.api.routers import trading_v2 as trading_v2_api
from server.db.migrations_v2 import MIGRATIONS
from server.trading_v2 import job_worker
from server.trading_v2 import versioning
from server.trading_v2.repository import TradingV2ReadRepository
from tools import backtest_unified_screener


def _etf_request(**overrides):
    request = {
        "strategy_id": "etf_trend_risk",
        "strategy_version": "etf_trend_risk_v2.0.0",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "random_seed": 7,
        "initial_capital": 250_000,
        "top_per_day": 12,
    }
    request.update(overrides)
    return request


def _etf_manifest(*codes):
    return {
        "strategy_id": "etf_trend_risk",
        "strategy_version": "etf_trend_risk_v2.0.0",
        "instrument_scope": "EXCHANGE_TRADED_FUND",
        "universe": {"eligible_codes": list(codes or ("510300",))},
    }


def _manifest_config_hash(manifest):
    return job_worker.canonical_json_hash({
        key: value
        for key, value in manifest.items()
        if key not in {"code_commit_sha", "config_hash"}
    })


def _fee_coverage(start="2021-01-04", end=None, *, usable=True):
    return {
        "formal_fee_coverage_usable": usable,
        "formal_fee_coverage_status": "CONFIRMED" if usable else "MISSING",
        "earliest_fee_covered_start": start if usable else None,
        "latest_fee_covered_end": end,
        "fee_profile_version": "fee-v1" if usable else None,
    }


def _complete_dependency_contract():
    return {"contract_hash": "d" * 64}


@pytest.mark.parametrize(
    ("strategy_id", "version"),
    [
        ("ultra_short", "stock_strategy_v2.3.0:ultra_short"),
        ("short_term", "stock_strategy_v2.3.0:short_term"),
        ("swing", "stock_strategy_v2.3.0:swing"),
        ("main_wave", "stock_strategy_v2.3.0:main_wave"),
        ("sector_preheat", "sector_preheat_v1.0.0"),
        ("intraday_dynamic_activation", "intraday_dynamic_activation_v2.0.0"),
        ("trend_breakout", "stock_strategy_v2.0.0:trend_breakout"),
    ],
)
def test_only_an_exact_reproducible_strategy_can_use_stock_adapter(
    strategy_id,
    version,
):
    contract = job_worker.research_backtest_adapter(
        strategy_id=strategy_id,
        strategy_version=version,
        instrument_scope="A_SHARE",
    )

    assert contract["supported"] is False
    assert contract["adapter"] is None


def test_only_etf_version_exposes_an_existing_formal_backtest_adapter():
    etf = job_worker.research_backtest_adapter(
        strategy_id="etf_trend_risk",
        strategy_version="etf_trend_risk_v2.0.0",
        instrument_scope="EXCHANGE_TRADED_FUND",
    )

    assert etf["supported"] is True
    assert etf["adapter"] == "etf_trade_level_replay_v2"

    future_etf = job_worker.research_backtest_adapter(
        strategy_id="etf_trend_risk",
        strategy_version="etf_trend_risk_v2.1.0",
        instrument_scope="EXCHANGE_TRADED_FUND",
    )
    assert future_etf["supported"] is False


def test_registered_etf_universe_is_exact_and_manifest_bound():
    manifest = _etf_manifest("510300", "159915")
    strategy = {
        "strategy_id": "etf_trend_risk",
        "version": "etf_trend_risk_v2.0.0",
        "instrument_scope": "EXCHANGE_TRADED_FUND",
        "config_hash": _manifest_config_hash(manifest),
        "manifest_json": json.dumps(manifest),
    }

    contract = job_worker.registered_etf_universe_contract(strategy)

    assert contract["eligible_codes"] == ("159915", "510300")
    assert len(contract["universe_hash"]) == 64
    with pytest.raises(ValueError, match="identity mismatch"):
        job_worker.registered_etf_universe_contract({
            **strategy,
            "strategy_id": "other",
        })
    with pytest.raises(ValueError, match="config hash mismatch"):
        job_worker.registered_etf_universe_contract({
            **strategy,
            "config_hash": "f" * 64,
        })


def test_registered_etf_universe_cannot_silently_gain_or_lose_a_code():
    exact = pd.DataFrame([
        {"etf_code": "159915", "eligible": True},
        {"etf_code": "510300", "eligible": True},
    ])
    assert job_worker._require_registered_etf_universe(
        exact,
        ("159915", "510300"),
    ) == ["159915", "510300"]

    missing = exact.loc[exact["etf_code"] != "159915"]
    with pytest.raises(RuntimeError, match="missing=159915"):
        job_worker._require_registered_etf_universe(
            missing,
            ("159915", "510300"),
        )
    unexpected = pd.concat([
        exact,
        pd.DataFrame([{"etf_code": "588000", "eligible": True}]),
    ])
    with pytest.raises(RuntimeError, match="unexpected=588000"):
        job_worker._require_registered_etf_universe(
            unexpected,
            ("159915", "510300"),
        )

    source = inspect.getsource(job_worker._etf_backtest)
    assert "k.etf_code IN :eligible_codes" in source
    assert '"registered_universe_hash": registered_universe_hash' in source
    assert "registered_data_audit = _target_data_coverage_audit" in source
    assert '"registered_code_coverage": registered_data_audit' in source


def test_worker_has_no_generic_stock_screener_dispatch():
    source = inspect.getsource(job_worker._run_backtest_job_impl)

    assert "unified_screener_point_in_time_v2" not in source
    assert "_stock_backtest" not in source
    assert "BINARY strategy_id = BINARY :strategy_id" in source
    assert "BINARY version = BINARY :version" in source
    assert '"run_request_uid": str(request.get("run_request_uid") or "")' in source


def test_unified_screener_optional_capital_keeps_old_default_and_is_used():
    signature = inspect.signature(backtest_unified_screener.run_backtest)
    assert signature.parameters["initial_capital_cny"].default == (
        backtest_unified_screener.DEFAULT_INITIAL_RESEARCH_CAPITAL_CNY
    )
    source = inspect.getsource(backtest_unified_screener.run_backtest)
    assert source.count("initial_capital_cny=initial_capital") >= 2
    assert '"initial_research_capital_cny": initial_capital' in source


def test_etf_execution_inputs_fix_confirmed_cost_baseline_to_one_x():
    assert job_worker._resolved_execution_inputs(
        {"initial_capital": 320_000},
        instrument_scope="EXCHANGE_TRADED_FUND",
    ) == (320_000.0, 1.0)
    assert job_worker._resolved_execution_inputs(
        {},
        instrument_scope="EXCHANGE_TRADED_FUND",
    ) == (200_000.0, 1.0)
    for override in (
        {"round_trip_cost": 0},
        {"round_trip_cost": 0.004},
        {"cost_scenario_multiplier": 0},
        {"cost_scenario_multiplier": 2},
    ):
        with pytest.raises(ValueError):
            job_worker._resolved_execution_inputs(
                override,
                instrument_scope="EXCHANGE_TRADED_FUND",
            )

    source = inspect.getsource(job_worker._etf_backtest)
    assert '"initial_capital": initial_capital' in source
    assert '"initial_capital_cny": initial_capital' in source
    assert '"cost_scenario_multiplier": cost_multiplier' in source
    assert '"stress_cost_scenario_multiplier": 2.0' in source
    assert "cost_multiplier=cost_multiplier * 2.0" in source
    assert "requested_round_trip_cost" not in source
    assert '"final_equity_cny"' in source
    assert '"open_position_count"' in source


def test_backtest_post_rejects_user_supplied_cost_override():
    for value in (0, 0.004):
        with pytest.raises(ValidationError):
            trading_v2_api.BacktestJobRequest(
                **_etf_request(round_trip_cost=value)
            )


def test_exact_etf_version_uses_monthly_only_reentry_policy():
    source = inspect.getsource(job_worker._etf_backtest)

    assert 'reentry_mode: str = "none"' in source
    assert '"reentry": "next_monthly_rebalance_only"' in source
    assert '"reentry": "ma20_and_return20_recovery"' not in source


def test_source_fingerprint_covers_the_formal_etf_simulator():
    assert "tools/backtest_etf_ensemble.py" in versioning.SOURCE_ARTIFACT_PATHS
    assert "tools/backtest_etf_robust.py" in versioning.SOURCE_ARTIFACT_PATHS


def test_etf_recent_window_keeps_frozen_history_and_blocks_pre_cutoff_start():
    assert job_worker._etf_dependency_start("2025-01-01") == "2019-01-01"
    assert job_worker._etf_dependency_start("2021-01-04") == "2019-01-01"

    with pytest.raises(ValueError, match="2021-01-04"):
        job_worker._etf_dependency_start("2020-12-31")

    source = inspect.getsource(job_worker._etf_backtest)
    contract_source = inspect.getsource(
        job_worker.registered_etf_dependency_data_contract
    )
    assert "dependency_start = _etf_dependency_start(start)" in source
    assert "cutoff_date=ETF_UNIVERSE_CUTOFF" in contract_source


def test_create_backtest_keeps_identical_clicks_as_independent_runs(monkeypatch):
    captured = []
    cutoff_code_sets = []

    class FakeRepository:
        def __init__(self, _engine):
            pass

        def strategies(self):
            return [
                {
                    "strategy_id": "etf_trend_risk",
                    "version": "etf_trend_risk_v2.0.0",
                    "instrument_scope": "EXCHANGE_TRADED_FUND",
                    "config_hash": _manifest_config_hash(
                        _etf_manifest("159915", "510300")
                    ),
                    "manifest": _etf_manifest("159915", "510300"),
                }
            ]

    def fake_enqueue(_engine, **kwargs):
        captured.append(kwargs["request"])
        return {"job_id": f"job-{len(captured):08d}", "status": "PENDING"}

    def fake_latest_session(_engine, codes):
        cutoff_code_sets.append(codes)
        return "2025-12-31"

    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api,
        "TradingV2ReadRepository",
        FakeRepository,
    )
    monkeypatch.setattr(trading_v2_api, "enqueue_job", fake_enqueue)
    monkeypatch.setattr(
        trading_v2_api,
        "_latest_validated_etf_session",
        fake_latest_session,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_complete_validated_etf_session",
        lambda _engine, _codes, *, on_or_before=None: on_or_before,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_formal_etf_dependency_data_contract",
        lambda _engine, _codes, _end, *_args: _complete_dependency_contract(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_confirmed_etf_fee_coverage",
        lambda _engine: _fee_coverage(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_envelope",
        lambda data, **_kwargs: {"data": data},
    )
    payload = trading_v2_api.BacktestJobRequest(**_etf_request())

    first = trading_v2_api.create_backtest_job(payload)["data"]
    second = trading_v2_api.create_backtest_job(payload)["data"]

    assert captured[0]["strategy_id"] == "etf_trend_risk"
    assert captured[0]["strategy_version"] == "etf_trend_risk_v2.0.0"
    assert captured[0]["initial_capital"] == 250_000
    assert "round_trip_cost" not in captured[0]
    assert captured[0]["top_per_day"] == 12
    assert len(captured[0]["expected_config_hash"]) == 64
    assert len(captured[0]["expected_universe_hash"]) == 64
    assert captured[0]["expected_dependency_contract_hash"] == "d" * 64
    assert cutoff_code_sets == [
        ("159915", "510300"),
        ("159915", "510300"),
    ]
    assert captured[0]["run_request_uid"] != captured[1]["run_request_uid"]
    assert first["run_request_uid"] != second["run_request_uid"]


def test_create_backtest_rejects_registered_strategy_without_adapter(monkeypatch):
    class FakeRepository:
        def __init__(self, _engine):
            pass

        def strategies(self):
            return [
                {
                    "strategy_id": "sector_preheat",
                    "version": "sector_preheat_v1.0.0",
                    "instrument_scope": "A_SHARE",
                }
            ]

    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api,
        "TradingV2ReadRepository",
        FakeRepository,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "enqueue_job",
        lambda *_args, **_kwargs: pytest.fail("unsupported job was enqueued"),
    )
    payload = trading_v2_api.BacktestJobRequest(
        **_etf_request(
            strategy_id="sector_preheat",
            strategy_version="sector_preheat_v1.0.0",
        )
    )

    with pytest.raises(HTTPException) as error:
        trading_v2_api.create_backtest_job(payload)

    assert error.value.status_code == 422
    assert error.value.detail == job_worker.research_backtest_adapter(
        strategy_id="sector_preheat",
        strategy_version="sector_preheat_v1.0.0",
        instrument_scope="A_SHARE",
    )["reason"]


def test_create_backtest_rejects_period_before_exact_etf_adapter(monkeypatch):
    class FakeRepository:
        def __init__(self, _engine):
            pass

        def strategies(self):
            return [
                {
                    "strategy_id": "etf_trend_risk",
                    "version": "etf_trend_risk_v2.0.0",
                    "instrument_scope": "EXCHANGE_TRADED_FUND",
                    "config_hash": _manifest_config_hash(
                        _etf_manifest("510300")
                    ),
                    "manifest": _etf_manifest("510300"),
                }
            ]

    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api,
        "TradingV2ReadRepository",
        FakeRepository,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "enqueue_job",
        lambda *_args, **_kwargs: pytest.fail("invalid period was enqueued"),
    )
    payload = trading_v2_api.BacktestJobRequest(
        **_etf_request(start_date="2020-12-31")
    )

    with pytest.raises(HTTPException) as error:
        trading_v2_api.create_backtest_job(payload)

    assert error.value.status_code == 422
    assert "2021-01-04" in error.value.detail


@pytest.mark.parametrize(
    ("latest_session", "expected"),
    [
        ("2025-12-30", "2025-12-30"),
        (None, "no complete validated data session"),
    ],
)
def test_create_backtest_rejects_end_after_formal_data_or_missing_data(
    monkeypatch,
    latest_session,
    expected,
):
    class FakeRepository:
        def __init__(self, _engine):
            pass

        def strategies(self):
            return [{
                "strategy_id": "etf_trend_risk",
                "version": "etf_trend_risk_v2.0.0",
                "instrument_scope": "EXCHANGE_TRADED_FUND",
                "config_hash": _manifest_config_hash(_etf_manifest("510300")),
                "manifest": _etf_manifest("510300"),
            }]

    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api, "TradingV2ReadRepository", FakeRepository
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_latest_validated_etf_session",
        lambda _engine, _codes: latest_session,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_confirmed_etf_fee_coverage",
        lambda _engine: _fee_coverage(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "enqueue_job",
        lambda *_args, **_kwargs: pytest.fail("invalid period was enqueued"),
    )
    payload = trading_v2_api.BacktestJobRequest(**_etf_request())

    with pytest.raises(HTTPException) as error:
        trading_v2_api.create_backtest_job(payload)

    assert error.value.status_code == 422
    assert expected in error.value.detail


def test_latest_validated_session_uses_complete_registered_universe_cutoff():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE sm_etf_kline (
                etf_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                adjust_type INTEGER NOT NULL,
                k_type INTEGER NOT NULL,
                validation_status TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                data_source TEXT NOT NULL
            )
            """
        ))
        connection.execute(text(
            """
            INSERT INTO sm_etf_kline VALUES
              ('510300', '2025-12-30', 0, 1, 'passed', 'validated', 'gj_big_qmt_inner'),
              ('510300', '2025-12-31', 0, 1, 'passed', 'validated', 'gj_big_qmt_inner'),
              ('159915', '2025-12-29', 0, 1, 'passed', 'validated', 'gj_big_qmt_inner'),
              ('159915', '2025-12-30', 0, 1, 'passed', 'validated', 'gj_big_qmt_inner'),
              ('510300', '2025-12-31', 1, 1, 'passed', 'validated', 'gj_big_qmt_inner'),
              ('510300', '2026-01-02', 0, 1, 'passed', 'validated', 'other_provider'),
              ('510300', '2026-01-05', 0, 1, 'failed', 'validated', 'gj_big_qmt_inner')
            """
        ))

    assert trading_v2_api._latest_validated_etf_session(
        engine,
        ("159915", "510300"),
    ) == "2025-12-30"
    assert trading_v2_api._latest_validated_etf_session(
        engine,
        ("159915", "510300", "511880"),
    ) is None
    assert trading_v2_api._complete_validated_etf_session(
        engine,
        ("159915", "510300"),
        on_or_before="2025-12-31",
    ) == "2025-12-30"


def test_worker_rejects_non_session_end_without_rewriting_it():
    data = type("Data", (), {})()
    data.close = pd.DataFrame(
        {
            "510300": [10.0, 10.1, 10.2],
            "159915": [20.0, 20.1, float("nan")],
        },
        index=pd.to_datetime(["2025-12-29", "2025-12-30", "2025-12-31"]),
    )

    assert job_worker._require_registered_etf_end_session(
        data,
        end_date="2025-12-30",
        eligible_codes=("159915", "510300"),
    ) == "2025-12-30"
    with pytest.raises(ValueError, match="2025-12-30"):
        job_worker._require_registered_etf_end_session(
            data,
            end_date="2025-12-31",
            eligible_codes=("159915", "510300"),
        )


def test_create_backtest_rejects_non_session_end_before_enqueue(monkeypatch):
    class FakeRepository:
        def __init__(self, _engine):
            pass

        def strategies(self):
            return [{
                "strategy_id": "etf_trend_risk",
                "version": "etf_trend_risk_v2.0.0",
                "instrument_scope": "EXCHANGE_TRADED_FUND",
                "config_hash": _manifest_config_hash(
                    _etf_manifest("159915", "510300")
                ),
                "manifest": _etf_manifest("159915", "510300"),
            }]

    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api, "TradingV2ReadRepository", FakeRepository
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_confirmed_etf_fee_coverage",
        lambda _engine: _fee_coverage(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_latest_validated_etf_session",
        lambda _engine, _codes: "2026-01-02",
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_complete_validated_etf_session",
        lambda _engine, _codes, *, on_or_before=None: "2025-12-30",
    )
    monkeypatch.setattr(
        trading_v2_api,
        "enqueue_job",
        lambda *_args, **_kwargs: pytest.fail("non-session run was enqueued"),
    )

    with pytest.raises(HTTPException) as error:
        trading_v2_api.create_backtest_job(
            trading_v2_api.BacktestJobRequest(**_etf_request())
        )

    assert error.value.status_code == 422
    assert "nearest previous session is 2025-12-30" in error.value.detail


def test_registered_etf_codes_accepts_normalized_manifest_shape():
    assert trading_v2_api._registered_etf_eligible_codes({
        "strategy_id": "etf_trend_risk",
        "version": "etf_trend_risk_v2.0.0",
        "instrument_scope": "EXCHANGE_TRADED_FUND",
        "manifest": {
            "strategy_id": "etf_trend_risk",
            "strategy_version": "etf_trend_risk_v2.0.0",
            "instrument_scope": "EXCHANGE_TRADED_FUND",
            "universe_definition": {
                "eligible_codes": ["510300", "159915"]
            }
        }
    }) == ("159915", "510300")


def test_strategies_exposes_latest_session_only_for_supported_adapter(monkeypatch):
    fee_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with fee_engine.begin() as connection:
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
        connection.execute(text(
            """
            INSERT INTO st_trade_account_v2 VALUES
                ('paper-main-v2', 'fee-v1')
            """
        ))
        connection.execute(
            text(
                """
                INSERT INTO st_fee_profile_v2 VALUES
                    ('fee-v1', '2026-07-27', NULL, 'ETF',
                     0.0001, 0.0001, 5, 0, 0, 0,
                     '{}', :evidence_hash, 'CONFIRMED')
                """
            ),
            {"evidence_hash": "f" * 64},
        )

    class FakeRepository:
        engine = fee_engine

        def latest_snapshot(self):
            return {}

        def strategies(self):
            return [
                {
                    "strategy_id": "etf_trend_risk",
                    "version": "etf_trend_risk_v2.0.0",
                    "instrument_scope": "EXCHANGE_TRADED_FUND",
                    "config_hash": _manifest_config_hash(
                        _etf_manifest("159915", "510300")
                    ),
                    "manifest": _etf_manifest("159915", "510300"),
                },
                {
                    "strategy_id": "swing",
                    "version": "stock_strategy_v2.3.0:swing",
                    "instrument_scope": "A_SHARE",
                },
            ]

    monkeypatch.setattr(trading_v2_api, "_repo", lambda: FakeRepository())
    monkeypatch.setattr(
        trading_v2_api,
        "_latest_validated_etf_session",
        lambda _engine, codes: (
            "2026-09-04" if codes == ("159915", "510300") else None
        ),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_formal_etf_dependency_data_contract",
        lambda _engine, _codes, _end, *_args: _complete_dependency_contract(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_envelope",
        lambda data, **_kwargs: {"data": data},
    )

    rows = trading_v2_api.strategies()["data"]

    assert rows[0]["backtest_adapter_supported"] is True
    assert rows[0]["latest_validated_session"] == "2026-09-04"
    assert rows[0]["latest_runnable_session"] == "2026-09-04"
    assert rows[0]["earliest_fee_covered_start"] == "2026-07-27"
    assert rows[0]["formal_fee_coverage_usable"] is True
    assert rows[0]["formal_data_contract_status"] == "COMPLETE"
    assert rows[1]["backtest_adapter_supported"] is False
    assert "latest_validated_session" not in rows[1]


@pytest.mark.parametrize(
    ("fee_end", "complete_session", "expected_runnable"),
    [
        ("2026-08-02", "2026-07-31", None),
        ("2026-08-03", "2026-08-03", "2026-08-03"),
    ],
)
def test_strategy_latest_runnable_session_respects_schedule_after_fee_end(
    monkeypatch,
    fee_end,
    complete_session,
    expected_runnable,
):
    calls = []

    class FakeRepository:
        engine = object()

        def latest_snapshot(self):
            return {}

        def strategies(self):
            return [{
                "strategy_id": "etf_trend_risk",
                "version": "etf_trend_risk_v2.0.0",
                "instrument_scope": "EXCHANGE_TRADED_FUND",
                "config_hash": _manifest_config_hash(
                    _etf_manifest("159915", "510300")
                ),
                "manifest": _etf_manifest("159915", "510300"),
            }]

    monkeypatch.setattr(trading_v2_api, "_repo", lambda: FakeRepository())
    monkeypatch.setattr(
        trading_v2_api,
        "_confirmed_etf_fee_coverage",
        lambda _engine: _fee_coverage(
            start="2026-07-27",
            end=fee_end,
        ),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_latest_validated_etf_session",
        lambda _engine, _codes: "2026-08-04",
    )

    def latest_complete_session(_engine, codes, *, on_or_before=None):
        calls.append((codes, on_or_before))
        return complete_session

    monkeypatch.setattr(
        trading_v2_api,
        "_complete_validated_etf_session",
        latest_complete_session,
    )

    def dependency_contract(_engine, _codes, end_date, backtest_start=None):
        assert backtest_start == "2026-07-27"
        if end_date == "2026-07-31":
            raise RuntimeError("no target schedule generated for trend_risk")
        return _complete_dependency_contract()

    monkeypatch.setattr(
        trading_v2_api,
        "_formal_etf_dependency_data_contract",
        dependency_contract,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_envelope",
        lambda data, **_kwargs: {"data": data},
    )

    row = trading_v2_api.strategies()["data"][0]

    assert calls == [(('159915', '510300'), fee_end)]
    assert row["latest_validated_session"] == "2026-08-04"
    assert row["latest_runnable_session"] == expected_runnable
    assert row["formal_data_contract_status"] == (
        "COMPLETE" if expected_runnable else "BLOCKED"
    )


@pytest.mark.parametrize(
    ("failure_mode", "expected_reason"),
    [
        ("missing_warmup_bar", "dependency coverage incomplete"),
        ("missing_metadata", "listing/status contract unavailable"),
        ("invalid_amount", "invalid amount"),
    ],
)
def test_api_preflight_blocks_incomplete_dependency_window_before_enqueue(
    monkeypatch,
    failure_mode,
    expected_reason,
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE si_trade_calendar (
                trade_date TEXT NOT NULL,
                trade_status INTEGER NOT NULL
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE si_etf_code (
                etf_code TEXT PRIMARY KEY,
                list_date TEXT,
                last_trade_date TEXT,
                status TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE sm_etf_kline (
                etf_code TEXT NOT NULL,
                short_name TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                adjust_type INTEGER NOT NULL,
                k_type INTEGER NOT NULL,
                validation_status TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                data_source TEXT NOT NULL,
                data_version TEXT NOT NULL,
                received_at TEXT NOT NULL,
                open NUMERIC NOT NULL,
                close NUMERIC NOT NULL,
                pre_close NUMERIC NOT NULL,
                amount NUMERIC NOT NULL
            )
            """
        ))
        connection.execute(text(
            """
            INSERT INTO si_trade_calendar VALUES
                ('2019-01-02', 1),
                ('2026-09-04', 1)
            """
        ))
        connection.execute(
            text(
                """
                INSERT INTO si_etf_code VALUES
                    ('510300', '2019-01-01', NULL, 'active',
                     'A股宽基', '2026-09-04 16:00:00'),
                    ('159915', :secondary_list_date, NULL, 'active',
                     'A股宽基', '2026-09-04 16:00:00')
                """
            ),
            {
                "secondary_list_date": (
                    None if failure_mode == "missing_metadata" else "2019-01-01"
                )
            },
        )
        bars = [
            ("510300", "2019-01-02"),
            ("510300", "2026-09-04"),
            ("159915", "2026-09-04"),
        ]
        if failure_mode in {"missing_metadata", "invalid_amount"}:
            bars.append(("159915", "2019-01-02"))
        for code, trade_date in bars:
            amount = (
                -1
                if failure_mode == "invalid_amount"
                and code == "159915"
                and trade_date == "2019-01-02"
                else 1_000_000
            )
            connection.execute(
                text(
                    """
                    INSERT INTO sm_etf_kline VALUES
                        (:code, :short_name, :trade_date, 0, 1,
                         'passed', 'validated', 'gj_big_qmt_inner',
                         :data_version, '2026-09-04 16:01:00',
                         10, 10, 9.9, :amount)
                    """
                ),
                {
                    "code": code,
                    "short_name": code,
                    "trade_date": trade_date,
                    "data_version": "a" * 64,
                    "amount": amount,
                },
            )

    class FakeRepository:
        def __init__(self, _engine=None):
            self.engine = engine

        def latest_snapshot(self):
            return {}

        def strategies(self):
            return [{
                "strategy_id": "etf_trend_risk",
                "version": "etf_trend_risk_v2.0.0",
                "instrument_scope": "EXCHANGE_TRADED_FUND",
                "config_hash": _manifest_config_hash(
                    _etf_manifest("159915", "510300")
                ),
                "manifest": _etf_manifest("159915", "510300"),
            }]

    monkeypatch.setattr(trading_v2_api, "_repo", lambda: FakeRepository())
    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: engine)
    monkeypatch.setattr(
        trading_v2_api,
        "TradingV2ReadRepository",
        FakeRepository,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_confirmed_etf_fee_coverage",
        lambda _engine: _fee_coverage(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_envelope",
        lambda data, **_kwargs: {"data": data},
    )
    monkeypatch.setattr(
        trading_v2_api,
        "enqueue_job",
        lambda *_args, **_kwargs: pytest.fail("invalid dependency run enqueued"),
    )

    row = trading_v2_api.strategies()["data"][0]
    assert row["latest_validated_session"] == "2026-09-04"
    assert row["latest_runnable_session"] is None
    assert row["formal_data_contract_status"] == "BLOCKED"
    assert expected_reason in row["formal_data_contract_reason"]

    with pytest.raises(HTTPException) as error:
        trading_v2_api.create_backtest_job(
            trading_v2_api.BacktestJobRequest(
                **_etf_request(
                    start_date="2025-01-01",
                    end_date="2026-09-04",
                )
            )
        )
    assert error.value.status_code == 422
    assert expected_reason in error.value.detail


@pytest.mark.parametrize(
    "preflight_error",
    [
        "registered ETF universe freeze evidence mismatch: missing=159915",
        "no target schedule generated for trend_risk",
    ],
)
def test_api_blocks_shared_universe_or_schedule_failure(
    monkeypatch,
    preflight_error,
):
    manifest = _etf_manifest("159915", "510300", "511880")

    class FakeRepository:
        def __init__(self, _engine=None):
            self.engine = object()

        def latest_snapshot(self):
            return {}

        def strategies(self):
            return [{
                "strategy_id": "etf_trend_risk",
                "version": "etf_trend_risk_v2.0.0",
                "instrument_scope": "EXCHANGE_TRADED_FUND",
                "config_hash": _manifest_config_hash(manifest),
                "manifest": manifest,
            }]

    monkeypatch.setattr(trading_v2_api, "_repo", lambda: FakeRepository())
    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api, "TradingV2ReadRepository", FakeRepository
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_confirmed_etf_fee_coverage",
        lambda _engine: _fee_coverage(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_latest_validated_etf_session",
        lambda _engine, _codes: "2025-12-31",
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_complete_validated_etf_session",
        lambda _engine, _codes, *, on_or_before=None: on_or_before,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_formal_etf_dependency_data_contract",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError(preflight_error)
        ),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_envelope",
        lambda data, **_kwargs: {"data": data},
    )
    monkeypatch.setattr(
        trading_v2_api,
        "enqueue_job",
        lambda *_args, **_kwargs: pytest.fail("invalid formal run enqueued"),
    )

    row = trading_v2_api.strategies()["data"][0]
    assert row["formal_data_contract_status"] == "BLOCKED"
    assert row["latest_runnable_session"] is None
    assert preflight_error in row["formal_data_contract_reason"]

    with pytest.raises(HTTPException) as error:
        trading_v2_api.create_backtest_job(
            trading_v2_api.BacktestJobRequest(**_etf_request())
        )
    assert error.value.status_code == 422
    assert preflight_error in error.value.detail


def test_worker_rejects_registration_hashes_changed_after_preflight():
    with pytest.raises(ValueError, match="config changed"):
        job_worker._require_expected_registration_binding(
            {"expected_config_hash": "a" * 64},
            config_hash="b" * 64,
            universe_hash="c" * 64,
        )
    with pytest.raises(ValueError, match="universe changed"):
        job_worker._require_expected_registration_binding(
            {"expected_universe_hash": "a" * 64},
            config_hash="b" * 64,
            universe_hash="c" * 64,
        )
    worker_source = inspect.getsource(job_worker._run_backtest_job_impl)
    assert "_require_expected_registration_binding(" in worker_source
    etf_source = inspect.getsource(job_worker._etf_backtest)
    assert "expected_dependency_contract_hash" in etf_source


def test_create_backtest_validates_confirmed_fee_coverage_before_enqueue(
    monkeypatch,
):
    captured = []

    class FakeRepository:
        def __init__(self, _engine):
            pass

        def strategies(self):
            return [{
                "strategy_id": "etf_trend_risk",
                "version": "etf_trend_risk_v2.0.0",
                "instrument_scope": "EXCHANGE_TRADED_FUND",
                "config_hash": _manifest_config_hash(_etf_manifest("510300")),
                "manifest": _etf_manifest("510300"),
            }]

    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api, "TradingV2ReadRepository", FakeRepository
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_latest_validated_etf_session",
        lambda _engine, _codes: "2026-09-04",
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_complete_validated_etf_session",
        lambda _engine, _codes, *, on_or_before=None: on_or_before,
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_formal_etf_dependency_data_contract",
        lambda _engine, _codes, _end, *_args: _complete_dependency_contract(),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_confirmed_etf_fee_coverage",
        lambda _engine: _fee_coverage(start="2026-07-27"),
    )
    monkeypatch.setattr(
        trading_v2_api,
        "enqueue_job",
        lambda _engine, **kwargs: captured.append(kwargs["request"])
        or {"job_id": "job-1", "status": "PENDING"},
    )
    monkeypatch.setattr(
        trading_v2_api,
        "_envelope",
        lambda data, **_kwargs: {"data": data},
    )

    with pytest.raises(HTTPException, match="confirmed fee coverage"):
        trading_v2_api.create_backtest_job(
            trading_v2_api.BacktestJobRequest(**_etf_request())
        )
    assert captured == []

    accepted = trading_v2_api.create_backtest_job(
        trading_v2_api.BacktestJobRequest(
            **_etf_request(
                start_date="2026-07-27",
                end_date="2026-09-04",
            )
        )
    )["data"]
    assert accepted["job_id"] == "job-1"
    assert captured[0]["start_date"] == "2026-07-27"


def test_backtest_request_identity_resolves_all_comparison_parameters():
    first = job_worker._backtest_request_identity(
        _etf_request(run_request_uid="run-a")
    )
    second = job_worker._backtest_request_identity(
        _etf_request(run_request_uid="run-b")
    )

    assert first["initial_capital"] == 250_000
    assert first["cost_scenario_multiplier"] == 1.0
    assert first["top_per_day"] == 12
    assert first != second

    assert job_worker._backtest_request_identity(first) == first


def test_orphan_repair_requires_run_uid_and_all_normalized_parameters():
    identity_a = job_worker._backtest_request_identity(
        _etf_request(run_request_uid="run-a")
    )
    identity_b = job_worker._backtest_request_identity(
        _etf_request(run_request_uid="run-b")
    )
    assert identity_a["cost_scenario_multiplier"] == 1.0
    running = [
        {
            "backtest_uid": "backtest-a",
            "strategy_version": identity_a["strategy_version"],
            "start_date": identity_a["start_date"],
            "end_date": identity_a["end_date"],
            "random_seed": identity_a["random_seed"],
            "started_at": "2025-01-01 00:00:00",
            "result_json": json.dumps(
                {
                    "run_request_uid": "run-a",
                    "request_identity": identity_a,
                }
            ),
        },
        {
            "backtest_uid": "backtest-b",
            "strategy_version": identity_b["strategy_version"],
            "start_date": identity_b["start_date"],
            "end_date": identity_b["end_date"],
            "random_seed": identity_b["random_seed"],
            "started_at": "2025-01-01 00:00:00",
            "result_json": json.dumps(
                {
                    "run_request_uid": "run-b",
                    "request_identity": identity_b,
                }
            ),
        },
    ]
    failed = [
        {
            "job_id": "job-a",
            "request_json": json.dumps(_etf_request(run_request_uid="run-a")),
            "error_code": "FAILED",
            "error_message": "worker stopped",
            "finished_at": "2025-01-01 00:01:00",
        },
        {
            "job_id": "job-b-wrong-capital",
            "request_json": json.dumps(
                _etf_request(run_request_uid="run-b", initial_capital=999_999)
            ),
            "error_code": "FAILED",
            "error_message": "worker stopped",
            "finished_at": "2025-01-01 00:01:00",
        },
    ]

    class Result:
        def __init__(self, rows=(), rowcount=0):
            self.rows = list(rows)
            self.rowcount = rowcount

        def mappings(self):
            return self

        def all(self):
            return self.rows

    class Connection:
        def __init__(self):
            self.updated = []

        def execute(self, statement, params=None):
            sql = str(statement)
            if "FROM st_backtest_run_v2" in sql:
                return Result(running)
            if "FROM st_job_v2" in sql:
                return Result(failed)
            if "UPDATE st_backtest_run_v2" in sql:
                self.updated.append(dict(params or {}))
                return Result(rowcount=1)
            raise AssertionError(sql)

    class Context:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self.connection

        def __exit__(self, *_args):
            return False

    class Engine:
        def __init__(self):
            self.connection = Connection()

        def connect(self):
            return Context(self.connection)

        def begin(self):
            return Context(self.connection)

    engine = Engine()
    repaired = job_worker.repair_orphaned_backtests(engine, now=date.today())

    assert repaired == [
        {"backtest_uid": "backtest-a", "job_id": "job-a", "status": "FAILED"}
    ]
    assert [row["backtest_uid"] for row in engine.connection.updated] == [
        "backtest-a"
    ]


def test_legacy_backtest_post_is_read_only_and_preserves_previous_result():
    result = sim_trade.sim_trade_backtest(
        start_date="2025-01-01",
        end_date="2025-12-31",
        strategy_types="trend_breakout",
        initial_capital=100_000,
    )

    assert result["status"] == "compatibility_only"
    assert result["mutated"] is False
    assert result["qualification_eligible"] is False
    assert result["formal_endpoint"] == "/api/v2/research/backtests"
    source = inspect.getsource(sim_trade.sim_trade_backtest)
    assert "DELETE FROM" not in source
    assert "_exec_sql" not in source


def test_research_ui_uses_one_formal_chain_and_same_condition_comparison():
    html = open(
        "server/static/trading-v3.html",
        encoding="utf-8",
    ).read()
    javascript = open(
        "server/static/js/trading-v3.js",
        encoding="utf-8",
    ).read()

    assert 'id="view-validation"' in html
    assert 'id="formalBacktestForm"' in html
    assert 'id="formalBacktestComparison"' in html
    assert "/api/v2/research/backtests" in javascript
    assert "/research/backtests?limit=30" in javascript
    assert "data-compare-backtest" in javascript
    assert "最多选择两条回测记录" in javascript
    assert "不可直接比较" in javascript
    for condition in (
        "start_date",
        "end_date",
        "random_seed",
        "initial_capital_cny",
        "cost_scenario_multiplier",
        "top_per_day",
        "adapter",
        "protocol_version",
        "code_commit_sha",
        "data_snapshot_hash",
    ):
        assert condition in javascript
    assert "backtest_adapter_supported" in javascript
    assert "backtest_adapter_minimum_start_date" in javascript
    assert "filter(function(item){return formalBacktestRunnable(item.row)}" in javascript
    assert "没有可复算回测适配器" in javascript
    assert "历史适配器不匹配，仅保留记录" in javascript
    assert "performance.total_return)*100" in javascript
    assert "Math.abs(Number(performance.max_drawdown))*100" in javascript


def test_backtest_history_route_is_declared_before_dynamic_result_route():
    source = inspect.getsource(trading_v2_api)

    assert source.index('@router.get("/research/backtests")') < source.index(
        '@router.get("/research/backtests/{backtest_uid}")'
    )


def test_backtest_identity_migration_adds_nullable_strategy_id():
    migration = next(
        item
        for item in MIGRATIONS
        if item["version"] == "20260906_017_backtest_strategy_identity"
    )
    sql = "\n".join(migration["statements"])

    assert "ALTER TABLE st_backtest_run_v2" in sql
    assert "ADD COLUMN strategy_id VARCHAR(80) NULL" in sql


def test_backtest_repository_does_not_cross_bind_same_version_and_config():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    shared_version = "shared-v1"
    shared_config = "a" * 64
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE st_strategy_version_v2 (
                strategy_id TEXT NOT NULL,
                version TEXT NOT NULL,
                config_hash TEXT NOT NULL
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE st_backtest_run_v2 (
                backtest_uid TEXT PRIMARY KEY,
                strategy_id TEXT,
                strategy_version TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                code_commit_sha TEXT,
                protocol_version TEXT,
                status TEXT,
                result_json TEXT,
                started_at TEXT NOT NULL
            )
            """
        ))
        connection.execute(
            text(
                """
                INSERT INTO st_strategy_version_v2
                    (strategy_id, version, config_hash)
                VALUES
                    ('alpha', :version, :config_hash),
                    ('beta', :version, :config_hash)
                """
            ),
            {"version": shared_version, "config_hash": shared_config},
        )
        rows = [
            {
                "uid": "new-alpha",
                "strategy_id": "alpha",
                "result": json.dumps({
                    "strategy_binding": {"strategy_id": "alpha"}
                }),
            },
            {
                "uid": "conflicting-binding",
                "strategy_id": "alpha",
                "result": json.dumps({
                    "strategy_binding": {"strategy_id": "beta"}
                }),
            },
            {
                "uid": "legacy-beta",
                "strategy_id": None,
                "result": json.dumps({
                    "strategy_binding": {"strategy_id": "beta"}
                }),
            },
            {
                "uid": "legacy-unavailable",
                "strategy_id": None,
                "result": json.dumps({}),
            },
        ]
        for index, row in enumerate(rows):
            connection.execute(
                text(
                    """
                    INSERT INTO st_backtest_run_v2
                        (backtest_uid, strategy_id, strategy_version,
                         config_hash, result_json, started_at)
                    VALUES
                        (:uid, :strategy_id, :version,
                         :config_hash, :result, :started_at)
                    """
                ),
                {
                    **row,
                    "version": shared_version,
                    "config_hash": shared_config,
                    "started_at": f"2026-09-06 10:00:0{index}",
                },
            )
        connection.execute(
            text(
                """
                INSERT INTO st_backtest_run_v2
                    (backtest_uid, strategy_id, strategy_version,
                     config_hash, code_commit_sha, protocol_version,
                     status, result_json, started_at)
                VALUES
                    ('corrupt-versioned-binding', 'alpha', :version,
                     :config_hash, :code_sha, :protocol_version,
                     'COMPLETED', :result, '2026-09-06 10:00:09')
                """
            ),
            {
                "version": shared_version,
                "config_hash": shared_config,
                "code_sha": "c" * 64,
                "protocol_version": job_worker.RESEARCH_PROTOCOL_VERSION,
                "result": json.dumps({
                    "strategy_binding": {
                        "strategy_id": "alpha",
                        "strategy_version": "wrong-version",
                        "config_hash": "b" * 64,
                        "code_commit_sha": "c" * 64,
                        "protocol_version": job_worker.RESEARCH_PROTOCOL_VERSION,
                    }
                }),
            },
        )

    repository = TradingV2ReadRepository(engine)
    listed = {row["backtest_uid"]: row for row in repository.backtests(10)}

    assert len(listed) == 5
    assert listed["new-alpha"]["strategy_id"] == "alpha"
    assert listed["new-alpha"]["strategy_identity_source"] == "RUN_ROW"
    assert listed["conflicting-binding"]["strategy_id"] is None
    assert (
        listed["conflicting-binding"]["strategy_identity_status"]
        == "UNAVAILABLE"
    )
    assert listed["legacy-beta"]["strategy_id"] == "beta"
    assert listed["legacy-beta"]["strategy_identity_source"] == "RESULT_BINDING"
    assert listed["legacy-unavailable"]["strategy_id"] is None
    assert (
        listed["legacy-unavailable"]["strategy_identity_status"]
        == "UNAVAILABLE"
    )
    assert listed["corrupt-versioned-binding"]["strategy_id"] is None
    assert (
        listed["corrupt-versioned-binding"]["strategy_identity_status"]
        == "UNAVAILABLE"
    )

    assert repository.backtest("new-alpha")["strategy_id"] == "alpha"
    assert repository.backtest("conflicting-binding")["strategy_id"] is None
    assert repository.backtest("legacy-beta")["strategy_id"] == "beta"
    assert repository.backtest("legacy-unavailable")["strategy_id"] is None


def test_worker_persists_strategy_id_on_new_backtest_run(monkeypatch):
    captured_insert = {}
    manifest = _etf_manifest("510300")

    class Result:
        def __init__(self, rows=(), rowcount=0):
            self.rows = list(rows)
            self.rowcount = rowcount

        def mappings(self):
            return self

        def all(self):
            return self.rows

        def first(self):
            return self.rows[0] if self.rows else None

    class Connection:
        def execute(self, statement, params=None):
            sql = str(statement)
            if "FROM st_strategy_version_v2" in sql:
                return Result([{
                    "strategy_id": "etf_trend_risk",
                    "version": "etf_trend_risk_v2.0.0",
                    "instrument_scope": "EXCHANGE_TRADED_FUND",
                    "config_hash": _manifest_config_hash(manifest),
                    "manifest_json": json.dumps(manifest),
                }])
            if "SELECT status, result_hash FROM st_backtest_run_v2" in sql:
                return Result()
            if "INSERT INTO st_backtest_run_v2" in sql:
                captured_insert["sql"] = sql
                captured_insert["params"] = dict(params or {})
                return Result(rowcount=1)
            if "UPDATE st_backtest_run_v2" in sql:
                return Result(rowcount=1)
            raise AssertionError(sql)

    class Context:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self.connection

        def __exit__(self, *_args):
            return False

    class Engine:
        def __init__(self):
            self.connection = Connection()

        def connect(self):
            return Context(self.connection)

        def begin(self):
            return Context(self.connection)

    monkeypatch.setattr(job_worker, "code_version", lambda: ("c" * 64, "test"))
    monkeypatch.setattr(
        job_worker,
        "_etf_backtest",
        lambda _engine, _request: {
            "promotion_protocol": {"status": "BLOCK"},
            "_trade_rows": [],
            "_data_snapshot_hash": "d" * 64,
        },
    )

    result = job_worker._run_backtest_job_impl(
        Engine(),
        _etf_request(run_request_uid="persist-strategy-id"),
    )

    assert result["status"] == "COMPLETED"
    assert "backtest_uid, strategy_id, strategy_version" in captured_insert["sql"]
    assert captured_insert["params"]["strategy_id"] == "etf_trend_risk"
    running = json.loads(captured_insert["params"]["result_json"])
    assert running["strategy_binding"]["registered_universe"][
        "eligible_codes"
    ] == ["510300"]
    assert len(
        running["strategy_binding"]["registered_universe_hash"]
    ) == 64
