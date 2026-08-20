from datetime import datetime
from unittest.mock import patch

from server.api.routers import holding_strategy


def test_yak_technology_august_17_sell_alert_is_an_immediate_exit():
    recommendation = {
        "signal_status": "SELL_ALERT",
        "signal_reason": "主升结构转弱，退出持仓",
        "main_wave_signal": "REDUCE",
        "entry_price_low": 145.0,
        "entry_price_high": 148.0,
        "stop_loss_price": 149.0,
        "trend_stop_price": 151.0,
        "take_profit_1": 158.0,
        "take_profit_2": 162.0,
    }

    def latest_row(_engine, **kwargs):
        if kwargs["table_name"] == "st_recommended_stocks":
            return recommendation, ""
        return {"event_risk_level": "LOW", "recommend_status": "HOLD"}, ""

    with patch.object(holding_strategy, "_latest_pit_row", side_effect=latest_row), patch.object(
        holding_strategy,
        "_daily_price_context",
        return_value=({"latest_price": 153.69, "price_source": "cutoff_quote", "ma20": 154.0}, ""),
    ):
        decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
            object(),
            "002409",
            "2026-08-17",
            "2026-08-17T10:00:00+08:00",
        )

    assert decision["exit_intent"] == "SELL"
    assert decision["knowledge_cutoff"] == "2026-08-17T10:00:00"
    assert decision["evidence"]["recommendation"]["signal_status"] == "SELL_ALERT"

    row = holding_strategy.build_watchlist_holding_strategy(
        {
            "stock_code": "002409",
            "display_name": "雅克科技",
            "cost_price": 147,
            "shares": 100,
            "sellable_shares": 100,
            "position_date": "2026-08-07",
        },
        decision,
    )

    assert row["action"] == "立即卖出"
    assert row["direct_exit"] is True
    assert row["position_date"] == "2026-08-07"
    assert row["pnl"] == 669.0
    assert row["pnl_pct"] == 4.55
    assert "下一交易日优先退出" in row["next_session_plan"]


def test_timezone_cutoff_is_always_interpreted_as_shanghai_time():
    parsed = holding_strategy._cutoff_datetime("2026-08-17T02:00:00+00:00")
    assert parsed == datetime(2026, 8, 17, 10, 0, 0)


def test_same_day_sell_signal_becomes_next_session_t1_exit():
    decision = {
        "trade_date": "2026-08-17",
        "exit_intent": "SELL",
        "reason": "persisted strategy signal is SELL_ALERT",
        "evidence": {
            "recommendation": {"signal_status": "SELL_ALERT"},
            "price": {"latest_price": 153.69},
            "thresholds": {"trend_stop_price": 151.0},
        },
    }
    row = holding_strategy.build_watchlist_holding_strategy(
        {
            "stock_code": "002409",
            "display_name": "雅克科技",
            "cost_price": 153,
            "shares": 100,
            "position_date": "2026-08-17",
        },
        decision,
    )

    assert row["action"] == "明日优先卖出（T+1）"
    assert row["t1_blocked"] is True
    assert row["direct_exit"] is False
    assert row["sellable_shares"] == 0
    assert "下一交易日" in row["next_session_plan"]


def test_watchlist_holding_summary_separates_exit_actions():
    result = holding_strategy.summarize_watchlist_holding_strategies(
        [
            {"exit_intent": "SELL"},
            {"exit_intent": "REDUCE"},
            {"exit_intent": "HOLD"},
            {"exit_intent": "WAIT_DATA"},
        ]
    )

    assert result == {
        "holding_count": 4,
        "sell_count": 1,
        "reduce_count": 1,
        "hold_count": 1,
        "wait_data_count": 1,
        "urgent_count": 2,
    }
