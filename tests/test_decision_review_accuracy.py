from datetime import datetime

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text

from server.api.commentary_utils import build_rule_checks, build_verdict
from server.api.routers import commentary, holding_strategy
from server.engine import strategy_center


def test_zero_quality_and_zero_win_rate_are_not_promoted():
    key = strategy_center.STRATEGY_CATALOG[0]["key"]
    result = strategy_center.effective_weight(key, "trend_bullish", data_quality=0)
    assert result["data_quality_multiplier"] == 0
    assert result["effective_weight"] == 0
    assert strategy_center.performance_multiplier({"sample_count": 12, "win_rate_pct": 0, "avg_return_pct": 0}) == 0.72


def test_descriptive_outcomes_do_not_change_strategy_weights():
    assert strategy_center.performance_multiplier({
        "sample_count": 100, "win_rate_pct": 90, "avg_return_pct": 10,
        "performance_weight_eligible": False,
    }) == 1


def test_market_confidence_is_labelled_without_changing_its_rule_score(monkeypatch):
    monkeypatch.setattr(strategy_center, "transition_market_state", lambda *args, **kwargs: {"final_state": "trend_bullish"})
    state = strategy_center.infer_market_state({})
    assert state["confidence"] == 80
    assert state["confidence_basis"] == "FIXED_MARKET_STATE_RULE_SCORE"
    assert state["confidence_semantics"] == "UNCALIBRATED_RULE_SCORE"
    assert "非概率" in state["confidence_label"]


def _metric_database(monkeypatch, ddl):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        for statement in ddl:
            connection.execute(text(statement))

    def read(sql, params=None):
        with engine.connect() as connection:
            return [dict(row) for row in connection.execute(text(sql), params or {}).mappings()]

    monkeypatch.setattr(strategy_center, "_db_read", read)
    return engine


def test_review_metrics_exclude_unmatured_and_late_written_outcomes(monkeypatch):
    key = strategy_center.STRATEGY_CATALOG[0]["key"]
    engine = _metric_database(monkeypatch, [
        "CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)",
        "CREATE TABLE st_recommended_stocks (stock_code TEXT, suitable_strategies TEXT, pick_date TEXT, review_5d_pct REAL, updated_at TEXT)",
    ])
    with engine.begin() as connection:
        for day in ["2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14"]:
            connection.execute(text("INSERT INTO si_trade_calendar VALUES (:day, 1)"), {"day": day})
        for code, day, value, known in [
            ("000001", "2026-09-07", -2, "2026-09-14 16:00:00"),
            ("000002", "2026-09-08", 100, "2026-09-14 16:00:00"),
            ("000003", "2026-09-07", 100, "2026-09-15 16:00:00"),
        ]:
            connection.execute(text("INSERT INTO st_recommended_stocks VALUES (:code, :strategies, :day, :value, :known)"),
                               {"code": code, "strategies": '["' + key + '"]', "day": day, "value": value, "known": known})
    monkeypatch.setattr(strategy_center, "_table_exists", lambda name: name == "st_recommended_stocks")
    monkeypatch.setattr(strategy_center, "_table_columns", lambda name: {"stock_code", "suitable_strategies", "pick_date", "review_5d_pct", "updated_at"})
    metric = strategy_center.load_strategy_metrics("2026-09-14")[key]
    assert metric["sample_count"] == 1
    assert metric["avg_return_pct"] == -2
    assert metric["win_rate_pct"] == 0
    assert metric["return_pct"] is None
    assert metric["max_drawdown_pct"] is None
    assert metric["performance_weight_eligible"] is False


def test_closed_trade_metrics_do_not_mix_replays_or_fabricate_nav(monkeypatch):
    key = strategy_center.STRATEGY_CATALOG[0]["key"]
    engine = _metric_database(monkeypatch, [
        "CREATE TABLE st_sim_position (id INTEGER, strategy_type TEXT, status TEXT, sell_date TEXT, profit REAL, profit_rate REAL, trade_mode TEXT, updated_at TEXT)",
    ])
    with engine.begin() as connection:
        for index, profit, rate, mode, known in [(1, 10, 2, "live", "2026-09-14 16:00:00"), (2, -5, -1, "live", "2026-09-14 16:00:00"), (3, 1000, 100, "backtest", "2026-09-14 16:00:00"), (4, 1000, 100, "live", "2026-09-15 16:00:00"), (5, 0, None, "live", "2026-09-14 16:00:00")]:
            connection.execute(text("INSERT INTO st_sim_position VALUES (:id, :key, 'sold', '2026-09-14', :profit, :rate, :mode, :known)"),
                               {"id": index, "key": key, "profit": profit, "rate": rate, "mode": mode, "known": known})
    monkeypatch.setattr(strategy_center, "_table_exists", lambda name: name == "st_sim_position")
    metric = strategy_center.load_strategy_metrics("2026-09-14")[key]
    assert metric["sample_count"] == 2
    assert metric["profit_factor"] == 2
    assert metric["avg_return_pct"] == 0.5
    assert metric["return_pct"] is None
    assert metric["max_drawdown_pct"] is None


@pytest.mark.parametrize("intent", ["HOLD", "WAIT_DATA", "REDUCE"])
def test_stale_sell_label_cannot_override_current_exit_intent(intent):
    row = holding_strategy.build_watchlist_holding_strategy(
        {"stock_code": "000001", "shares": 100, "sellable_shares": 100},
        {"trade_date": "2026-09-14", "exit_intent": intent, "reason": "当前结论",
         "evidence": {"recommendation": {"signal_status": "SELL_ALERT", "signal_reason": "旧结论"}}},
    )
    assert row["direct_exit"] is False
    assert row["emergency_exit"]["direct"] is False
    assert row["reason"] == "当前结论"


@pytest.mark.parametrize("intent", ["SELL", "REDUCE"])
def test_zero_sellable_quantity_is_preserved(intent):
    row = holding_strategy.build_watchlist_holding_strategy(
        {"stock_code": "000001", "shares": 100, "sellable_shares": 0, "position_date": "2026-09-11"},
        {"trade_date": "2026-09-14", "exit_intent": intent, "evidence": {}},
    )
    assert row["sellable_shares"] == 0
    assert row["direct_exit"] is False
    assert row["urgency"] == "SELLABILITY_BLOCKED"
    assert "待可卖" in row["action"]


def test_expired_sell_recommendation_does_not_issue_a_new_exit(monkeypatch):
    def latest(_engine, **kwargs):
        if kwargs["table_name"] == "st_recommended_stocks":
            return {"pick_date": "2026-09-07", "signal_status": "SELL_ALERT", "stop_loss_price": 100, "event_risk_level": "CRITICAL"}, ""
        return {"analysis_date": "2026-09-14", "event_risk_level": "LOW"}, ""

    monkeypatch.setattr(holding_strategy, "_latest_pit_row", latest)
    monkeypatch.setattr(holding_strategy, "_daily_price_context", lambda *args, **kwargs: ({"latest_price": 10, "same_session": True, "ma20": 9}, ""))
    decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
        object(), "000001", "2026-09-14", "2026-09-14T10:00:00", cost_price=9,
        market_context={"data_date": "2026-09-14", "market_action": "HOLD"},
    )
    assert decision["exit_intent"] == "HOLD"
    assert decision["evidence"]["thresholds"]["stop_loss_price"] is None
    assert decision["evidence"]["freshness"]["recommendation_stale"] is True


@pytest.mark.parametrize("risk", ["CRITICAL", "HIGH"])
def test_stale_analysis_risk_cannot_override_current_recommendation(monkeypatch, risk):
    def latest(_engine, **kwargs):
        if kwargs["table_name"] == "st_recommended_stocks":
            return {"pick_date": "2026-09-14", "signal_status": "HOLD", "event_risk_level": "LOW"}, ""
        return {"analysis_date": "2026-09-07", "event_risk_level": risk}, ""

    monkeypatch.setattr(holding_strategy, "_latest_pit_row", latest)
    monkeypatch.setattr(holding_strategy, "_daily_price_context", lambda *args, **kwargs: ({"latest_price": 10, "same_session": True, "ma20": 9}, ""))
    decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
        object(), "000001", "2026-09-14", "2026-09-14T10:00:00", cost_price=9,
        market_context={"data_date": "2026-09-14", "market_action": "HOLD"},
    )
    assert decision["exit_intent"] == "HOLD"
    assert decision["evidence"]["freshness"]["analysis_stale"] is True
    assert decision["evidence"]["freshness"]["recommendation_stale"] is False


def test_historical_intraday_commentary_rejects_current_quote_substitution(monkeypatch):
    monkeypatch.setattr(commentary, "_read_sql", lambda *args: pytest.fail("historical intraday must not read live data"))
    with pytest.raises(HTTPException) as error:
        commentary._assess_commentary_core(commentary.CommentaryAssessRequest(text="1 000001 平安银行", phase="intraday", as_of_date="2025-01-01"))
    assert error.value.status_code == 422


def test_commentary_anchor_does_not_substitute_an_unrelated_bar():
    bars = [{"trade_date": "2026-09-14", "low": 10}]
    assert commentary._pick_anchor_bar(bars, ["2026-09-01"], "2026-09-14") is None
    assert commentary._pick_anchor_bar(bars, [], None) is None


def test_missing_anchor_prevents_positive_commentary_verdict():
    checks = build_rule_checks(phase="premarket", current_price=100, ma5=99, ma10=98, support=97, anchor_low=None, anchor_volume=None, latest_volume=500, news_count=2)
    assert build_verdict(checks)["status"] == "WATCH"
    assert any(check["status"] == "unknown" for check in checks)


@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf")])
def test_invalid_price_cannot_produce_commentary_confirmation(price):
    checks = build_rule_checks(phase="premarket", current_price=price, ma5=99, ma10=98, support=97, anchor_low=96, anchor_volume=1000, latest_volume=500, news_count=2)
    assert build_verdict(checks)["status"] == "NO_DATA"


def test_premarket_day_is_strictly_prior_session_and_news_has_cutoff(monkeypatch):
    seen = []

    def read(sql, params):
        seen.append((sql, params))
        return [{"d": "2026-09-11"}] if "MAX(trade_date)" in sql else []

    monkeypatch.setattr(commentary, "_read_sql", read)
    assert commentary._latest_trade_date("2026-09-14", before_session=True) == "2026-09-11"
    commentary._load_news_items("000001", "平安银行", cutoff=datetime(2026, 9, 14, 9, 25))
    assert "trade_date < :d" in seen[0][0]
    assert "publish_time <= :cutoff" in seen[1][0]
    assert "notice_date < :session_date" in seen[2][0]


def test_intraday_commentary_requires_a_fresh_price(monkeypatch):
    class Loader:
        def load_full_data(self, code, **kwargs):
            assert kwargs == {"trade_date": "2026-09-11", "use_realtime": False}
            return {"market": {"price": 100}, "technical": {"ma": {"ma5": 90, "ma10": 80}}}

    monkeypatch.setattr(commentary, "_read_sql", lambda *args: [])
    monkeypatch.setattr(commentary, "_load_daily_bars", lambda *args: [])
    monkeypatch.setattr(commentary, "_load_news_items", lambda *args, **kwargs: [])
    item = commentary._assess_one({"index": 1, "stock_code": "000001", "stock_name": "平安银行"}, phase="intraday", trade_date="2026-09-11", loader=Loader(), cutoff=datetime(2026, 9, 14, 10))
    assert item["current"]["price"] is None
    assert item["verdict"]["status"] == "NO_DATA"
