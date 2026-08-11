from server.api.routers.sim_trade import (
    STRATEGY_CONFIG,
    SIM_INITIAL_CAPITAL,
    _calc_trade_metrics,
    _extract_signal_date_from_reason,
    _infer_position_signal_date,
    _normalize_strategy_filter,
    _recommendation_window_returns,
    _runtime_config_snapshot,
    sim_trade_automation_status,
)
from unittest.mock import MagicMock, patch


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
    assert metrics["win_rate"] == 0


def test_runtime_config_snapshot_exposes_effective_rules():
    out = _runtime_config_snapshot()

    assert out["status"] == "ok"
    assert out["strategy_config"]["ultra_short"]["min_ai_score"] == 70
    assert out["risk_config"]["cash_buffer_pct"] == 0.20
    assert out["global_rules"]["min_executable_risk_reward"] == 3.0
    assert "688" in out["global_rules"]["excluded_recommend_prefixes"]
    assert out["intraday_windows"]["entry_windows"]


def test_recommendation_window_returns_calculates_1_3_5_10_day_metrics():
    rows = [
        {"stock_code": "000001", "trade_date": "2026-06-01", "close": 10.0},
        {"stock_code": "000001", "trade_date": "2026-06-02", "close": 10.5},
        {"stock_code": "000001", "trade_date": "2026-06-03", "close": 10.4},
        {"stock_code": "000001", "trade_date": "2026-06-04", "close": 10.8},
        {"stock_code": "000001", "trade_date": "2026-06-05", "close": 11.0},
        {"stock_code": "000001", "trade_date": "2026-06-08", "close": 11.2},
        {"stock_code": "000001", "trade_date": "2026-06-09", "close": 11.1},
        {"stock_code": "000001", "trade_date": "2026-06-10", "close": 11.0},
        {"stock_code": "000001", "trade_date": "2026-06-11", "close": 10.9},
        {"stock_code": "000001", "trade_date": "2026-06-12", "close": 11.3},
        {"stock_code": "000001", "trade_date": "2026-06-15", "close": 12.0},
    ]

    with patch("server.api.routers.sim_trade._read_sql", return_value=rows):
        out = _recommendation_window_returns("2026-06-01", [{"stock_code": "000001"}])

    assert out["sample_count"] == 1
    assert out["windows"]["1d"]["avg_return_pct"] == 5.0
    assert out["windows"]["10d"]["avg_return_pct"] == 20.0


def test_sim_trade_automation_status_reports_sim_ready_and_real_disabled():
    engine = MagicMock()
    engine.signal_pool_counts.return_value = {"total": 3, "NEW": 1}
    engine.order_counts.return_value = {"total": 2, "PENDING": 1}
    task_rows = [
        {
            "task_name": "盘中模拟交易执行Tick",
            "task_type": "sim_trade",
            "interval_minutes": 1,
            "enabled": 1,
            "last_run_status": "success",
            "last_run_at": "2026-06-28 10:00:00",
        },
        {
            "task_name": "盘前模拟交易信号池准备",
            "task_type": "sim_trade_signal_prepare",
            "cron_time": "09:20",
            "enabled": 1,
            "last_run_status": "success",
            "last_run_at": "2026-06-28 09:20:00",
        },
    ]

    with patch("server.api.routers.sim_trade._ensure_tables"), \
         patch("server.api.routers.sim_trade.SimTradeEngine", return_value=engine), \
         patch("server.api.routers.sim_trade._scheduler_online_status", return_value={
             "standalone_online": True,
             "embedded_running": False,
             "api_restart_safe": True,
         }), \
         patch("server.api.routers.sim_trade._read_sql", side_effect=[task_rows, []]) as read_sql:
        out = sim_trade_automation_status()

    assert out["sim_auto_ready"] is True
    assert out["tasks"]["intraday_tick"]["status"] == "ok"
    assert out["tasks"]["signal_prepare"]["enabled"] is True
    assert out["real_trading_enabled"] is False
    assert out["signal_counts"]["total"] == 3
    event_sql = read_sql.call_args_list[1].args[0]
    assert "LEFT JOIN si_all_code" in event_sql
    assert "BINARY e.stock_code = BINARY s.stock_code" in event_sql
    assert "st_sim_event e" in event_sql
