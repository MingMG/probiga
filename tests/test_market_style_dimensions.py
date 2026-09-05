# -*- coding: utf-8 -*-
import pandas as pd
import inspect

from server.api.routers import hot_data
from tools import market_sentiment
from tools.market_sentiment import build_style_dimensions


def test_market_sentiment_route_allows_today_window():
    days_query = inspect.signature(hot_data.market_sentiment).parameters["days"].default
    constraints = {type(item).__name__: item for item in days_query.metadata}
    assert constraints["Ge"].ge == 1


def _stock_rows():
    rows = []
    for trade_date in ("2026-09-03", "2026-09-04"):
        for index in range(6):
            rows.append(
                {
                    "trade_date": trade_date,
                    "change_pct": index - 2.5,
                    "amount": (index + 1) * 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def test_style_dimensions_keep_evidence_streams_separate():
    dimensions = build_style_dimensions(
        {"status": "ok", "rotation_score": 63.5, "phase": "快速轮动"},
        {
            "size_style": {
                "status": "available",
                "bias": "小盘占优",
                "small_minus_large_pct": 2.1,
            },
            "market_activity": {"recent_up_ratio": 57.2, "recent_avg_chg": 0.4},
            "growth_value_style": {
                "status": "unavailable",
                "reason": "RELIABLE_GROWTH_VALUE_CLASSIFICATION_MISSING",
            },
        },
        {
            "status": "ok",
            "flow_style": "资金小幅净流入",
            "recent_trend": "近期资金边际改善",
            "inflow_ratio": 54.0,
        },
    )

    assert dimensions["size"]["bias"] == "小盘占优"
    assert dimensions["capital"]["status"] == "available"
    assert dimensions["rotation"]["score"] == 63.5
    assert dimensions["breadth"]["recent_up_ratio_pct"] == 57.2
    assert dimensions["growth_value"]["status"] == "unavailable"


def test_breadth_reports_its_actual_source_window():
    dimensions = build_style_dimensions(
        {"status": "ok", "lookback_days": 20, "rotation_score": 20, "phase": "主线行情"},
        {"lookback_days": 20, "date_range": ["2026-08-10", "2026-09-04"], "market_activity": {"window_up_ratio": 51, "window_avg_chg": 0.1, "window_days": 20}},
        {"status": "no_data"},
    )
    assert dimensions["breadth"]["lookback_days"] == 20
    assert dimensions["rotation"]["lookback_days"] == 20


def test_style_dimensions_do_not_guess_when_sources_are_missing():
    dimensions = build_style_dimensions(
        {"status": "no_data"},
        {"market_activity": {}, "size_style": {"status": "unavailable"}},
        {"status": "no_data"},
    )
    assert dimensions["capital"]["status"] == "unavailable"
    assert dimensions["rotation"]["status"] == "unavailable"
    assert dimensions["breadth"]["status"] == "unavailable"


def test_size_style_does_not_turn_turnover_groups_into_market_cap(monkeypatch):
    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_kline" in statement:
            return _stock_rows()
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(object(), ["2026-09-03", "2026-09-04"])

    assert result["size_style"]["status"] == "unavailable"
    assert result["indices"] == []
    assert result["liquidity_activity_proxy"]["status"] == "available"
    assert "不代表大盘/小盘" in result["liquidity_activity_proxy"]["note"]


def test_size_style_uses_existing_broad_indices_when_both_groups_exist(monkeypatch):
    index_rows = pd.DataFrame(
        [
            {"index_code": "000300", "trade_date": "2026-09-03", "close": 4000, "change_pct": 0},
            {"index_code": "000300", "trade_date": "2026-09-04", "close": 3960, "change_pct": -1},
            {"index_code": "000852", "trade_date": "2026-09-03", "close": 6000, "change_pct": 0},
            {"index_code": "000852", "trade_date": "2026-09-04", "close": 6120, "change_pct": 2},
        ]
    )

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_kline" in statement:
            return _stock_rows()
        if "FROM sm_index_kline" in statement:
            return index_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(object(), ["2026-09-03", "2026-09-04"])

    assert result["size_style"]["status"] == "partial"
    assert result["size_style"]["bias"] == "小盘占优"
    assert result["size_style"]["small_minus_large_pct"] == 3.0
    assert result["size_style"]["date_range"] == ["2026-09-03", "2026-09-04"]
    assert result["size_style"]["reason"] == "INCOMPLETE_BROAD_INDEX_GROUP"


def test_today_size_style_uses_same_day_index_changes(monkeypatch):
    index_rows = pd.DataFrame(
        [
            {"index_code": "000300", "trade_date": "2026-09-04", "close": 3960, "change_pct": -1},
            {"index_code": "000852", "trade_date": "2026-09-04", "close": 6120, "change_pct": 2},
        ]
    )
    stock_rows = _stock_rows()
    stock_rows = stock_rows[stock_rows["trade_date"] == "2026-09-04"]

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_kline" in statement:
            return stock_rows
        if "FROM sm_index_kline" in statement:
            return index_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(object(), ["2026-09-04"])

    assert result["size_style"]["status"] == "partial"
    assert result["size_style"]["small_minus_large_pct"] == 3.0
    assert result["size_style"]["lookback_days"] == 1


def test_size_style_rejects_indices_without_a_common_requested_interval(monkeypatch):
    index_rows = pd.DataFrame(
        [
            {"index_code": "000300", "trade_date": "2026-09-02", "close": 4000, "change_pct": 0},
            {"index_code": "000300", "trade_date": "2026-09-04", "close": 3960, "change_pct": -1},
            {"index_code": "000852", "trade_date": "2026-09-03", "close": 6000, "change_pct": 0},
            {"index_code": "000852", "trade_date": "2026-09-04", "close": 6120, "change_pct": 2},
        ]
    )

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_kline" in statement:
            return _stock_rows()
        if "FROM sm_index_kline" in statement:
            return index_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(object(), ["2026-09-03", "2026-09-04"])

    assert result["size_style"]["status"] == "unavailable"
    assert result["size_style"]["reason"] == "RELIABLE_BROAD_INDEX_PAIR_MISSING"


def test_single_actual_theme_day_does_not_invent_rotation_or_advice(monkeypatch):
    theme_rows = pd.DataFrame(
        [
            {
                "snapshot_date": "2026-09-04",
                "plate_type": 1,
                "rank": 1,
                "concept_code": "C1",
                "concept_name": "测试概念",
                "change_pct": 1.2,
                "hot_value": 100,
                "hot_tag": "",
            }
        ]
    )
    monkeypatch.setattr(market_sentiment.pd, "read_sql", lambda *_args, **_kwargs: theme_rows)

    result = market_sentiment.analyze_main_theme(
        object(), ["2026-09-03", "2026-09-04"]
    )

    assert result["status"] == "insufficient_history"
    assert result["rotation_score"] is None
    assert result["phase"] is None
    assert "低吸" not in result["phase_desc"]


def test_historical_style_news_is_bounded_by_requested_date(monkeypatch):
    captured = {}

    def fake_read(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(hot_data, "_read_sql", fake_read)
    result = hot_data._market_sentiment_news_rows("2024-06-10")

    assert result == []
    assert "NOW()" not in captured["sql"]
    assert "publish_time <= :news_end" in captured["sql"]
    assert captured["params"]["news_end"] == "2024-06-10 23:59:59"
