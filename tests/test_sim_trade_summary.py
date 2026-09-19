import pytest

from server.api.routers.sim_trade import (
    STRATEGY_CONFIG,
    SIM_INITIAL_CAPITAL,
    _calc_trade_metrics,
    _extract_signal_date_from_reason,
    _infer_position_signal_date,
    _normalize_strategy_filter,
    _return_metrics,
)


def test_extract_signal_date_from_reason():
    reason = "信号日2026-06-17，次交易日开盘买入；最终交易评分80分"
    assert _extract_signal_date_from_reason(reason) == "2026-06-17"


def test_infer_position_signal_date_for_backtest_reason():
    row = {
        "buy_reason": "盘中验证：信号日2026-06-18，验证日2026-06-19，09:31首条分时买入",
        "buy_date": "2026-06-19",
    }
    assert _infer_position_signal_date(row, "forward") == "2026-06-18"


def test_infer_position_signal_date_for_live_defaults_to_buy_date():
    row = {
        "buy_reason": "实时模拟买入；最终交易评分82分",
        "buy_date": "2026-06-19",
    }
    assert _infer_position_signal_date(row, "live") == "2026-06-19"


def test_normalize_strategy_filter_defaults_to_all():
    assert _normalize_strategy_filter("") == list(STRATEGY_CONFIG.keys())


def test_normalize_strategy_filter_keeps_valid_unique_order():
    assert _normalize_strategy_filter("short_term, main_wave, bad, short_term") == [
        "short_term",
        "main_wave",
    ]


def test_calc_trade_metrics_empty_rows():
    metrics = _calc_trade_metrics([], SIM_INITIAL_CAPITAL)
    assert metrics["closed_count"] == 0
    assert metrics["total_profit"] == 0
    assert metrics["win_rate"] is None
    assert metrics["profit_factor"] is None
    assert metrics["profit_loss_ratio"] is None
    assert metrics["metric_status"] == "NO_SAMPLES"


@pytest.mark.parametrize("rows, factor, ratio, factor_status, ratio_status", [
    ([{"profit": 300, "profit_rate": 3}, {"profit": 700, "profit_rate": 7}], None, None, "NO_LOSING_TRADES", "NO_LOSING_TRADES"),
    ([{"profit": -100, "profit_rate": -2}], 0, None, "AVAILABLE", "NO_WINNING_TRADES"),
    ([{"profit": 0, "profit_rate": 0}], None, None, "NO_LOSING_TRADES", "NO_LOSING_TRADES"),
    ([{"profit": 300, "profit_rate": 3}, {"profit": -100, "profit_rate": -1}], 3, 3, "AVAILABLE", "AVAILABLE"),
])
def test_profit_ratios_keep_dimensionless_units(rows, factor, ratio, factor_status, ratio_status):
    metrics = _calc_trade_metrics(rows)
    assert metrics["profit_factor"] == factor
    assert metrics["profit_loss_ratio"] == ratio
    assert metrics["profit_factor_status"] == factor_status
    assert metrics["profit_loss_ratio_status"] == ratio_status
    recent = _return_metrics(rows)
    assert recent["profit_factor_3m"] == factor
    assert recent["profit_loss_ratio_3m"] == ratio


def test_missing_nonfinite_and_breakeven_outcomes_are_not_losses():
    metrics = _calc_trade_metrics([
        {"profit": 10, "profit_rate": 1},
        {"profit": 0, "profit_rate": 0},
        {"profit": None, "profit_rate": None},
        {"profit": float("inf"), "profit_rate": 1},
    ])
    assert metrics["closed_count"] == 4
    assert metrics["evaluated_count"] == 2
    assert metrics["missing_outcome_count"] == 2
    assert metrics["win_rate"] == 50
    assert metrics["lose_count"] == 0
    assert metrics["breakeven_count"] == 1
    assert metrics["metric_status"] == "INCOMPLETE_OUTCOMES"


def test_independent_trade_returns_do_not_claim_portfolio_drawdown_or_sharpe():
    metrics = _return_metrics([{"profit": 100, "profit_rate": 10}, {"profit": -100, "profit_rate": -10}])
    assert metrics["avg_return_3m"] == 0
    assert metrics["max_drawdown_3m"] is None
    assert metrics["sharpe_ratio_3m"] is None
    assert metrics["risk_metrics_status"] == "PORTFOLIO_NAV_UNAVAILABLE"


@pytest.mark.parametrize("quote", [15, None, float("inf"), float("nan"), -1, 0])
def test_dashboard_uses_one_portfolio_accounting_state(monkeypatch, quote):
    from server.api.routers import sim_trade
    from server.engine import sim_trade_engine

    seen = {}

    class Engine:
        def portfolio_state(self, mode, *, price_map):
            seen.update(price_map)
            return {"cash_available": 999000, "total_equity": 1000500, "position_usage_rate": 0.15,
                    "cash_buffer_amount": 100050, "cash_available_after_buffer": 898950,
                    "max_total_position_amount": 900450, "holding_value": 1500,
                    "holdings": [{"stock_code": "000001", "strategy_type": "short_term", "cur_price": 15, "market_value": 1500, "unrealized_profit": 500}],
                    "used_by_strategy": {"short_term": 1500}, "used_by_stock": {"000001": 1500}}

    def read(sql, params=None):
        if "COUNT(*)" in sql:
            return [{"cnt": 0}]
        if "sm_stock_kline" in sql:
            return [{"close": quote}] if quote is not None else []
        return [{"id": 1, "stock_code": "000001", "buy_price": 10, "buy_shares": 100, "buy_date": "2026-09-01"}]

    monkeypatch.setattr(sim_trade_engine, "_ensure_tables", lambda: None)
    monkeypatch.setattr(sim_trade_engine, "_is_trading_time", lambda: False)
    monkeypatch.setattr(sim_trade, "SimTradeEngine", Engine)
    monkeypatch.setattr(sim_trade, "STRATEGY_CONFIG", {"short_term": {"name": "短线"}})
    monkeypatch.setattr(sim_trade, "_read_sql", read)
    monkeypatch.setattr(sim_trade, "_recent_closed_trade_rows", lambda *args: [])
    result = sim_trade.sim_trade_dashboard("backtest")
    assert result["summary"]["cash_available"] == 999000
    assert result["strategies"]["short_term"]["avg_profit_rate"] is None
    assert result["strategies"]["short_term"]["max_profit_rate"] is None
    assert result["strategies"]["short_term"]["max_loss_rate"] is None
    if quote == 15:
        assert seen == {"000001": {"price": 15}}
        assert result["summary"]["total_equity"] == 1000500
        assert result["summary"]["total_return_rate"] == 0.05
    else:
        assert seen == {}
        assert result["summary"]["total_equity"] is None
        assert result["summary"]["valuation_status"] == "MISSING_HOLDING_PRICES"
        assert result["strategies"]["short_term"]["holdings"][0]["pnl"] is None
        assert result["strategies"]["short_term"]["holding_amount"] is None
        state = result["portfolio_state"]
        assert state["cash_available"] == 999000
        assert state["total_equity"] is None
        assert state["holdings"][0]["market_value"] is None
        assert state["used_by_strategy"]["short_term"] is None
        assert state["used_by_stock"]["000001"] is None
        assert result["summary"]["risk_budget"]["max_total_position_amount"] is None


def test_risk_budget_read_does_not_publish_cost_based_budget(monkeypatch):
    from server.api.routers import sim_trade

    class Engine:
        def portfolio_state(self, *args):
            return {"cash_available": 999000, "total_equity": 1000000,
                    "holdings": [{"stock_code": "000001", "strategy_type": "short_term", "cur_price": 10, "market_value": 1000}],
                    "used_by_strategy": {"short_term": 1000}}

        def _save_risk_budget_snapshot(self, *args):
            pytest.fail("GET must not publish a risk budget")

    saved = [{"strategy_type": "short_term", "total_equity": 1000500, "updated_at": "2026-09-18T10:00:00"}]
    monkeypatch.setattr(sim_trade, "_ensure_tables", lambda: None)
    monkeypatch.setattr(sim_trade, "SimTradeEngine", Engine)
    monkeypatch.setattr(sim_trade, "_read_sql", lambda *args: saved)
    result = sim_trade.sim_trade_risk_budget("live", "2026-09-18")
    assert result["status"] == "ok"
    assert result["budgets"] == saved
    assert result["budgets_basis"] == "PERSISTED_RISK_BUDGET_SNAPSHOTS"
    assert result["portfolio_state"]["cash_available"] == 999000
    assert result["portfolio_state"]["total_equity"] is None
    assert result["portfolio_state"]["holdings"][0]["cur_price"] is None


def test_stats_aggregate_all_strategies_and_exclude_missing_outcomes(monkeypatch):
    from server.api.routers import sim_trade

    by_strategy = {
        "short_term": [{"profit": 100, "profit_rate": 4, "sell_date": "2026-09-17"},
                       {"profit": 200, "profit_rate": 8, "sell_date": "2026-09-17"},
                       {"profit": None, "profit_rate": None, "sell_date": "2026-09-17"}],
        "swing": [{"profit": -50, "profit_rate": -2, "sell_date": "2026-09-17"},
                  {"profit": 0, "profit_rate": 0, "sell_date": "2026-09-18"}],
    }

    def read(sql, params):
        assert "sell_date <= :today" in sql
        return by_strategy[params["st"]]

    monkeypatch.setattr(sim_trade, "_ensure_tables", lambda: None)
    monkeypatch.setattr(sim_trade, "_read_sql", read)
    monkeypatch.setattr(sim_trade, "_recent_closed_trade_rows", lambda *args: [])
    monkeypatch.setattr(sim_trade, "STRATEGY_CONFIG", {key: {} for key in by_strategy})
    result = sim_trade.sim_trade_stats("backtest")
    assert result["by_strategy"]["short_term"]["median_rate"] == 6
    assert result["by_strategy"]["short_term"]["count"] == 2
    assert result["by_strategy"]["short_term"]["missing_outcome_count"] == 1
    assert result["by_strategy"]["swing"]["lose"] == 1
    assert result["by_strategy"]["swing"]["breakeven_count"] == 1
    assert sum(row["count"] for row in result["profit_distribution"]) == 4
    assert result["daily_pnl"] == [
        {"date": "2026-09-17", "pnl": 250, "count": 3},
        {"date": "2026-09-18", "pnl": 0, "count": 1},
    ]
