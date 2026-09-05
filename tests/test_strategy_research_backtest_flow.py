from __future__ import annotations

import inspect

import pytest
from fastapi import HTTPException

from server.api.routers import sim_trade
from server.api.routers import trading_v2 as trading_v2_api
from server.trading_v2 import job_worker
from tools import backtest_unified_screener


def _etf_request(**overrides):
    request = {
        "strategy_id": "etf_trend_risk",
        "strategy_version": "etf_trend_risk_v2.0.0",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "random_seed": 7,
        "initial_capital": 250_000,
        "round_trip_cost": 0.003,
        "top_per_day": 12,
    }
    request.update(overrides)
    return request


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


def test_etf_execution_inputs_change_capital_and_cost_used_by_replay():
    assert job_worker._resolved_execution_inputs(
        {"initial_capital": 320_000, "round_trip_cost": 0.004},
        instrument_scope="EXCHANGE_TRADED_FUND",
    ) == (320_000.0, 0.004)
    assert job_worker._resolved_execution_inputs(
        {},
        instrument_scope="EXCHANGE_TRADED_FUND",
    ) == (200_000.0, 0.001)

    source = inspect.getsource(job_worker._etf_backtest)
    assert "initial_capital=initial_capital" in source
    assert '"initial_capital_cny": initial_capital' in source
    assert "resolved_round_trip_cost / base_round_trip_cost" in source
    assert '"final_equity_cny"' in source
    assert '"open_position_count"' in source


def test_etf_recent_window_keeps_frozen_history_and_blocks_pre_cutoff_start():
    assert job_worker._etf_dependency_start("2025-01-01") == "2019-01-01"
    assert job_worker._etf_dependency_start("2021-01-04") == "2019-01-01"

    with pytest.raises(ValueError, match="2021-01-04"):
        job_worker._etf_dependency_start("2020-12-31")

    source = inspect.getsource(job_worker._etf_backtest)
    assert "dependency_start = _etf_dependency_start(start)" in source
    assert "cutoff_date=ETF_UNIVERSE_CUTOFF" in source


def test_create_backtest_keeps_identical_clicks_as_independent_runs(monkeypatch):
    captured = []

    class FakeRepository:
        def __init__(self, _engine):
            pass

        def strategies(self):
            return [
                {
                    "strategy_id": "etf_trend_risk",
                    "version": "etf_trend_risk_v2.0.0",
                    "instrument_scope": "EXCHANGE_TRADED_FUND",
                }
            ]

    def fake_enqueue(_engine, **kwargs):
        captured.append(kwargs["request"])
        return {"job_id": f"job-{len(captured):08d}", "status": "PENDING"}

    monkeypatch.setattr(trading_v2_api, "get_engine", lambda: object())
    monkeypatch.setattr(
        trading_v2_api,
        "TradingV2ReadRepository",
        FakeRepository,
    )
    monkeypatch.setattr(trading_v2_api, "enqueue_job", fake_enqueue)
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
    assert captured[0]["round_trip_cost"] == 0.003
    assert captured[0]["top_per_day"] == 12
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
        "round_trip_cost",
        "top_per_day",
        "adapter",
        "protocol_version",
        "code_commit_sha",
        "data_snapshot_hash",
    ):
        assert condition in javascript
    assert "backtest_adapter_supported" in javascript
    assert "backtest_adapter_minimum_start_date" in javascript
    assert "filter(function(item){return formalAdapterSupported(item.row)}" in javascript
    assert "没有可复算回测适配器" in javascript
    assert "历史适配器不匹配，仅保留记录" in javascript
    assert "performance.total_return)*100" in javascript
    assert "Math.abs(Number(performance.max_drawdown))*100" in javascript


def test_backtest_history_route_is_declared_before_dynamic_result_route():
    source = inspect.getsource(trading_v2_api)

    assert source.index('@router.get("/research/backtests")') < source.index(
        '@router.get("/research/backtests/{backtest_uid}")'
    )
