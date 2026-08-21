from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine, text

from server.api.routers import holding_strategy


def _daily_run(
    *,
    dominant_state="TREND_UP",
    risk_asset_cap=0.8,
    quality_status="PASS",
    status="COMPLETED",
):
    return {
        "run_uid": "daily-run-20260821",
        "status": status,
        "decision_integrity_verified": True,
        "requested_as_of": "2026-08-21",
        "trade_date": "2026-08-20",
        "decision_at": "2026-08-21 09:15:00",
        "dominant_regime": dominant_state,
        "regime": {
            "dominant_state": dominant_state,
            "risk_asset_cap": risk_asset_cap,
            "quality_status": quality_status,
        },
    }


def test_daily_market_context_reduces_risk_off_holdings():
    context = holding_strategy.build_daily_market_holding_context(
        _daily_run(dominant_state="RISK_OFF", risk_asset_cap=0.10),
        "2026-08-21",
    )

    assert context["status"] == "READY"
    assert context["market_action"] == "REDUCE"
    assert context["dominant_state"] == "RISK_OFF"
    assert context["risk_asset_cap"] == 0.10
    assert "不再死拿" in context["reason"]


def test_missing_daily_market_context_fails_closed():
    context = holding_strategy.build_daily_market_holding_context(
        None,
        "2026-08-21",
    )

    assert context["status"] == "BLOCKED"
    assert context["market_action"] == "WAIT_DATA"
    assert "DAILY_DECISION_RUN_MISSING" in context["blockers"]


def test_daily_market_risk_overrides_an_individual_hold(monkeypatch):
    def latest_row(_engine, **kwargs):
        if kwargs["table_name"] == "st_recommended_stocks":
            return {
                "pick_date": "2026-08-20",
                "signal_status": "WATCH",
                "main_wave_signal": "WATCH",
            }, ""
        return {
            "analysis_date": "2026-08-20",
            "event_risk_level": "LOW",
            "recommend_status": "HOLD",
        }, ""

    monkeypatch.setattr(holding_strategy, "_latest_pit_row", latest_row)
    monkeypatch.setattr(
        holding_strategy,
        "_daily_price_context",
        lambda *_args, **_kwargs: ({
            "latest_price": 10.5,
            "price_trade_date": "2026-08-21",
            "same_session": True,
            "ma20": 10.0,
        }, ""),
    )
    market_context = holding_strategy.build_daily_market_holding_context(
        _daily_run(dominant_state="RISK_OFF", risk_asset_cap=0.10),
        "2026-08-21",
    )

    decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
        object(),
        "000001",
        "2026-08-21",
        "2026-08-21T10:00:00+08:00",
        cost_price=10.0,
        market_context=market_context,
    )

    assert decision["exit_intent"] == "REDUCE"
    assert decision["evidence"]["market_context"]["dominant_state"] == "RISK_OFF"
    assert "不再死拿" in decision["reason"]


def test_hard_stop_remains_authoritative_when_daily_context_is_blocked(monkeypatch):
    monkeypatch.setattr(
        holding_strategy,
        "_latest_pit_row",
        lambda *_args, **_kwargs: ({}, "missing"),
    )
    monkeypatch.setattr(
        holding_strategy,
        "_daily_price_context",
        lambda *_args, **_kwargs: ({
            "latest_price": 9.4,
            "price_trade_date": "2026-08-21",
            "same_session": True,
            "ma20": 10.0,
        }, ""),
    )
    market_context = holding_strategy.build_daily_market_holding_context(
        None,
        "2026-08-21",
    )

    decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
        object(),
        "000001",
        "2026-08-21",
        "2026-08-21T10:00:00+08:00",
        cost_price=10.0,
        market_context=market_context,
    )

    assert decision["exit_intent"] == "SELL"
    assert "成本保护位" in decision["reason"]


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


def test_historical_holding_uses_validated_same_session_quote(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE sm_stock_kline (stock_code TEXT, trade_date TEXT, "
            "close REAL, k_type INTEGER, adjust_type INTEGER, etl_sync_at TEXT)"
        ))
        connection.execute(
            text("INSERT INTO sm_stock_kline VALUES "
                 "('002165','2026-08-19',7.41,1,0,'2026-08-20 09:00:00')")
        )

    monkeypatch.setattr(
        holding_strategy,
        "_table_columns",
        lambda _engine, table: {
            "stock_code", "trade_date", "close", "k_type",
            "adjust_type", "etl_sync_at",
        } if table == "sm_stock_kline" else set(),
    )
    monkeypatch.setattr(
        holding_strategy,
        "_same_session_quote",
        lambda *_args, **_kwargs: ({
            "price": 7.79,
            "observed_at": "2026-08-20 15:00:00",
            "data_source": "gj_big_qmt_inner",
            "quality_status": "VALIDATED",
        }, ""),
    )

    price, error = holding_strategy._daily_price_context(
        engine,
        stock_code="002165",
        trade_date="2026-08-20",
        cutoff=datetime(2026, 8, 20, 15, 10),
        current_price=None,
    )

    assert error == ""
    assert price["latest_price"] == 7.79
    assert price["price_trade_date"] == "2026-08-20"
    assert price["latest_daily_trade_date"] == "2026-08-19"
    assert price["same_session"] is True
    assert price["price_source"] == "validated_same_session_quote"


def test_stale_watch_signal_cannot_authorize_continued_holding(monkeypatch):
    def latest_row(_engine, **kwargs):
        if kwargs["table_name"] == "st_recommended_stocks":
            return {
                "pick_date": "2026-07-30",
                "signal_status": "WATCH",
                "main_wave_signal": "BUY_READY",
            }, ""
        return {
            "analysis_date": "2026-08-20",
            "event_risk_level": "LOW",
            "recommend_status": "HOLD",
        }, ""

    monkeypatch.setattr(holding_strategy, "_latest_pit_row", latest_row)
    monkeypatch.setattr(
        holding_strategy,
        "_daily_price_context",
        lambda *_args, **_kwargs: ({
            "latest_price": 6.05,
            "price_trade_date": "2026-08-20",
            "same_session": True,
            "ma20": 5.94,
        }, ""),
    )

    decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
        object(),
        "601988",
        "2026-08-20",
        "2026-08-20T15:10:00+08:00",
    )

    assert decision["exit_intent"] == "WAIT_DATA"
    assert decision["evidence"]["freshness"]["recommendation_stale"] is True
    assert "已经过期" in decision["reason"]


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
