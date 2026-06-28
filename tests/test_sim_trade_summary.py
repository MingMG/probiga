from server.api.routers.sim_trade import (
    STRATEGY_CONFIG,
    SIM_INITIAL_CAPITAL,
    _calc_trade_metrics,
    _extract_signal_date_from_reason,
    _infer_position_signal_date,
    _normalize_strategy_filter,
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
    assert metrics["win_rate"] == 0
