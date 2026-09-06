# -*- coding: utf-8 -*-
import pandas as pd
import inspect
from pathlib import Path

from server.api.routers import hot_data
from server.common import tech_risk
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
            "market_activity": {
                "status": "available",
                "data_cutoff": "2026-09-04",
                "recent_up_ratio": 57.2,
                "recent_avg_chg": 0.4,
            },
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
        {"lookback_days": 20, "date_range": ["2026-08-10", "2026-09-04"], "market_activity": {"status": "available", "data_cutoff": "2026-09-04", "window_up_ratio": 51, "window_avg_chg": 0.1, "window_days": 20}},
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


def test_style_nan_changes_fail_closed_instead_of_reporting_balance(monkeypatch):
    trade_date = "2026-09-04"
    stock_rows = pd.DataFrame(
        [
            {"trade_date": trade_date, "change_pct": float("nan"), "amount": index + 1}
            for index in range(9)
        ]
    )
    index_rows = pd.DataFrame(
        [
            {
                "index_code": code,
                "trade_date": trade_date,
                "close": 1000,
                "change_pct": float("nan"),
            }
            for code in ("000016", "000300", "000905", "000852", "399303", "399006", "000688")
        ]
    )

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_kline" in statement:
            return stock_rows
        if "FROM sm_index_kline" in statement:
            return index_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(object(), [trade_date])
    dimensions = build_style_dimensions({"status": "no_data"}, result, {"status": "no_data"})

    assert result["size_style"]["status"] == "unavailable"
    assert result["indices"] == []
    assert result["market_activity"]["status"] == "unavailable"
    assert result["market_activity"]["data_cutoff"] is None
    assert result["liquidity_activity_proxy"]["status"] == "unavailable"
    assert dimensions["breadth"]["status"] == "unavailable"


def test_breadth_latest_day_missing_is_partial_and_uses_actual_cutoff(monkeypatch):
    trade_dates = ["2026-09-03", "2026-09-04"]
    stock_rows = _stock_rows()
    stock_rows = stock_rows[stock_rows["trade_date"] == "2026-09-03"]

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_kline" in statement:
            return stock_rows
        if "FROM si_all_code" in statement:
            return pd.DataFrame([{"expected_stock_cnt": 6}])
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(object(), trade_dates)
    dimensions = build_style_dimensions({"status": "no_data"}, result, {"status": "no_data"})

    assert result["market_activity"]["status"] == "partial"
    assert result["market_activity"]["reason"] == "MARKET_BREADTH_REQUESTED_CUTOFF_MISSING"
    assert result["market_activity"]["data_cutoff"] == "2026-09-03"
    assert result["market_activity"]["coverage"]["requested_cutoff_covered"] is False
    assert dimensions["breadth"]["status"] == "partial"
    assert dimensions["breadth"]["recent_up_ratio_pct"] is None
    assert dimensions["breadth"]["data_cutoff"] == "2026-09-03"


def test_breadth_sparse_rows_do_not_claim_full_market_width(monkeypatch):
    trade_dates = ["2026-09-03", "2026-09-04"]
    stock_rows = pd.DataFrame(
        [
            {"stock_code": "000001", "trade_date": trade_date, "change_pct": 1.0, "amount": 1_000_000}
            for trade_date in trade_dates
        ]
    )

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_kline" in statement:
            return stock_rows
        if "FROM si_all_code" in statement:
            return pd.DataFrame([{"expected_stock_cnt": 100}])
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(object(), trade_dates)
    dimensions = build_style_dimensions({"status": "no_data"}, result, {"status": "no_data"})

    assert result["market_activity"]["status"] == "partial"
    assert result["market_activity"]["reason"] == "MARKET_BREADTH_STOCK_COVERAGE_INCOMPLETE"
    assert all(
        item["coverage_pct"] == 1.0
        for item in result["market_activity"]["coverage"]["stock_coverage_by_date"]
    )
    assert dimensions["breadth"]["status"] == "partial"
    assert dimensions["breadth"]["recent_up_ratio_pct"] is None


def test_main_theme_ui_uses_actual_concept_sample_days_as_denominator():
    script = (Path(__file__).resolve().parents[1] / "server/static/js/app.js").read_text(
        encoding="utf-8"
    )
    sentiment_renderer = script.split("function loadSentimentPage", 1)[1].split(
        "function loadCommandPage", 1
    )[0]

    assert "var themeLookback = Number(theme.lookback_days || ((theme.coverage || {}).available_concept_days) || 0);" in sentiment_renderer
    assert "card('回顾天数', themeLookback" in sentiment_renderer
    assert "escHtml(t.appear_days) + '/' + escHtml(themeLookback)" in sentiment_renderer
    assert "escHtml(t.appear_days) + '/' + escHtml(res.lookback_days)" not in sentiment_renderer


def test_text_report_handles_missing_latest_change_north_flow_and_partial_breadth():
    report = market_sentiment.format_report(
        {
            "analysis_date": "2026-09-04",
            "lookback_days": 20,
            "trade_dates": ["2026-09-03", "2026-09-04"],
            "theme_analysis": {
                "status": "ok",
                "phase": "主线行情",
                "rotation_score": 20,
                "phase_desc": "两日样本",
                "lookback_days": 2,
                "main_themes": [
                    {
                        "name": "测试",
                        "type": "概念",
                        "appear_days": 2,
                        "avg_rank": 1.0,
                        "avg_change_pct": 1.0,
                        "score": 10.0,
                    }
                ],
            },
            "style_analysis": {
                "status": "ok",
                "bias": "大小盘数据不可用",
                "bias_desc": "缺少可靠指数",
                "large_small_diff": None,
                "indices": [
                    {
                        "name": "测试指数",
                        "category": "大",
                        "total_change_pct": 1.0,
                        "win_rate": None,
                        "momentum": "走平",
                        "last_price": 100.0,
                        "last_change_pct": None,
                    }
                ],
                "market_activity": {
                    "status": "partial",
                    "reason": "MARKET_BREADTH_STOCK_COVERAGE_INCOMPLETE",
                    "recent_avg_chg": 9.9,
                    "recent_up_ratio": 99.9,
                },
            },
            "capital_analysis": {
                "status": "ok",
                "flow_style": "主力资金净流入",
                "recent_trend": "近期资金偏流入",
                "total_main_flow": 1,
                "avg_daily_flow": 1,
                "inflow_ratio": 50,
                "north_flow_note": "北向资金数据不可用",
                "north_total_flow": None,
            },
        }
    )

    assert "2/2" in report
    assert "市场宽度不可用" in report
    assert "99.9%" not in report
    assert "北向资金数据不可用" in report


def test_index_last_change_does_not_borrow_an_older_valid_day(monkeypatch):
    index_rows = pd.DataFrame(
        [
            {"index_code": "000300", "trade_date": "2026-09-03", "close": 4000, "change_pct": 1.0},
            {"index_code": "000300", "trade_date": "2026-09-04", "close": 4040, "change_pct": float("nan")},
            {"index_code": "000852", "trade_date": "2026-09-03", "close": 6000, "change_pct": 2.0},
            {"index_code": "000852", "trade_date": "2026-09-04", "close": 6060, "change_pct": float("nan")},
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

    assert result["indices"]
    assert all(item["last_change_pct"] is None for item in result["indices"])


def test_liquidity_proxy_counts_zero_days_and_only_uses_available_days(monkeypatch):
    rows = []
    for trade_date, large_change, small_change in (
        ("2026-09-01", 0.0, 0.0),
        ("2026-09-03", 2.0, 4.0),
    ):
        for index in range(6):
            change = large_change if index == 5 else small_change if index == 0 else 0.0
            rows.append(
                {
                    "trade_date": trade_date,
                    "change_pct": change,
                    "amount": (index + 1) * 1_000_000,
                }
            )

    def fake_read(sql, _engine, params=None):
        if "FROM sm_stock_kline" in str(sql):
            return pd.DataFrame(rows)
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_style(
        object(),
        ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"],
    )

    assert result["large_avg_daily"] == 1.0
    assert result["small_avg_daily"] == 2.0
    assert result["liquidity_activity_proxy"]["status"] == "available"


def test_capital_style_fails_closed_for_missing_dates_and_empty_northbound(monkeypatch):
    trade_dates = ["2026-09-01", "2026-09-02", "2026-09-03"]
    flow_rows = pd.DataFrame(
        [
            {
                "trade_date": "2026-09-01",
                "total_main_flow": 2_000_000_000,
                "stock_cnt": 100,
                "inflow_cnt": 100,
            }
        ]
    )
    expected_rows = pd.DataFrame(
        [{"trade_date": value, "expected_stock_cnt": 100} for value in trade_dates]
    )

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_capital_flow_daily" in statement:
            return flow_rows
        if "COUNT(DISTINCT stock_code)" in statement:
            return expected_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_capital_style(object(), trade_dates)

    assert result["status"] == "partial"
    assert result["data_cutoff"] == "2026-09-01"
    assert result["flow_style"] is None
    assert result["recent_trend"] is None
    assert result["coverage"]["available_trade_days"] == 1
    assert result["coverage"]["date_coverage_pct"] == 33.3
    assert result["north_flow_status"] == "unavailable"
    assert "持平" not in result["north_flow_note"]
    assert result["north_total_flow"] is None


def test_capital_style_fails_closed_when_stock_coverage_is_incomplete(monkeypatch):
    trade_dates = ["2026-09-03", "2026-09-04"]
    flow_rows = pd.DataFrame(
        [
            {"trade_date": value, "total_main_flow": 1_000_000, "stock_cnt": 1, "inflow_cnt": 1}
            for value in trade_dates
        ]
    )
    expected_rows = pd.DataFrame(
        [{"trade_date": value, "expected_stock_cnt": 100} for value in trade_dates]
    )

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_capital_flow_daily" in statement:
            return flow_rows
        if "COUNT(DISTINCT stock_code)" in statement:
            return expected_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_capital_style(object(), trade_dates)

    assert result["status"] == "partial"
    assert result["reason"] == "CAPITAL_FLOW_STOCK_COVERAGE_INCOMPLETE"
    assert result["flow_style"] is None
    assert result["coverage"]["stock_coverage_status"] == "incomplete"
    assert all(
        item["coverage_pct"] == 1.0
        for item in result["coverage"]["stock_coverage_by_date"]
    )


def test_capital_style_does_not_self_validate_against_incomplete_kline(monkeypatch):
    trade_dates = ["2026-09-03", "2026-09-04"]
    flow_rows = pd.DataFrame(
        [
            {
                "trade_date": value,
                "total_main_flow": 2_000_000_000,
                "stock_cnt": 10,
                "inflow_cnt": 10,
            }
            for value in trade_dates
        ]
    )
    incomplete_kline_rows = pd.DataFrame(
        [{"trade_date": value, "expected_stock_cnt": 10} for value in trade_dates]
    )
    universe_rows = pd.DataFrame([{"expected_stock_cnt": 100}])
    statements = []

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        statements.append(statement)
        if "FROM sm_stock_capital_flow_daily" in statement:
            return flow_rows
        if "FROM si_all_code" in statement:
            return universe_rows
        if "FROM sm_stock_kline" in statement:
            return incomplete_kline_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_capital_style(object(), trade_dates)

    assert result["status"] == "partial"
    assert result["reason"] == "CAPITAL_FLOW_STOCK_COVERAGE_INCOMPLETE"
    assert result["flow_style"] is None
    assert result["coverage"]["minimum_stock_coverage_pct"] == 80.0
    assert result["coverage"]["stock_coverage_status"] == "incomplete"
    assert all(
        item["expected_stock_count"] == 100 and item["coverage_pct"] == 10.0
        for item in result["coverage"]["stock_coverage_by_date"]
    )
    assert not any("FROM sm_stock_kline" in statement for statement in statements)


def test_capital_style_only_calls_flow_sustained_when_each_recent_day_agrees(monkeypatch):
    trade_dates = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"]
    flows = [2_000_000_000, 2_000_000_000, -1_000_000_000, 3_000_000_000]
    flow_rows = pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "total_main_flow": flow,
                "stock_cnt": 100,
                "inflow_cnt": 60,
            }
            for trade_date, flow in zip(trade_dates, flows)
        ]
    )
    expected_rows = pd.DataFrame(
        [{"trade_date": value, "expected_stock_cnt": 100} for value in trade_dates]
    )
    north_rows = pd.DataFrame(
        [{"trade_date": value, "net_flow": 0} for value in trade_dates]
    )

    def fake_read(sql, _engine, params=None):
        statement = str(sql)
        if "FROM sm_stock_capital_flow_daily" in statement:
            return flow_rows
        if "COUNT(DISTINCT stock_code)" in statement:
            return expected_rows
        if "FROM st_north_flow_daily" in statement:
            return north_rows
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read)
    result = market_sentiment.analyze_capital_style(object(), trade_dates)

    assert result["status"] == "ok"
    assert result["recent_trend"] == "近期资金偏流入"
    assert "持续" not in result["recent_trend"]
    assert result["north_flow_status"] == "available"
    assert result["north_flow_note"] == "北向资金持平"


def test_style_switch_is_unavailable_when_every_evidence_stream_is_empty():
    result = hot_data._style_switch_signal_from_inputs({}, [])

    assert result["status"] == "unavailable"
    assert result["reason"] == "STYLE_EVIDENCE_MISSING"
    assert result["risk_off_score"] is None
    assert result["switch_score"] is None
    assert result["evidence"] == []


def test_style_switch_ignores_unrelated_news_when_market_dimensions_are_missing():
    result = hot_data._style_switch_signal_from_inputs(
        {},
        [{"title": "某公司发布年度报告", "content": "经营情况保持稳定"}],
    )

    assert result["status"] == "unavailable"
    assert result["risk_off_score"] is None
    assert result["switch_score"] is None


def test_style_switch_accepts_keyword_matched_news_as_explicit_evidence():
    result = hot_data._style_switch_signal_from_inputs(
        {},
        [{"title": "央行宣布降息", "content": "政策落地"}],
    )

    assert result["status"] == "balanced"
    assert result["risk_off_score"] == 20.0
    assert result["switch_score"] == 4.0
    assert "证据尚不足" in result["summary"]
    assert "维持主线" not in result["summary"]
    assert result["news_counts"]["policy"] == 1
    assert any("政策类新闻1条" in item for item in result["evidence"])


def test_balanced_size_style_is_not_misread_as_large_cap_defensive():
    result = hot_data._style_switch_signal_from_inputs(
        {
            "style_analysis": {
                "size_style": {
                    "status": "available",
                    "small_minus_large_pct": 0.2,
                },
                "bias": "大小盘均衡",
                "bias_desc": "小盘和大盘宽基表现接近",
            }
        },
        [],
    )

    assert result["risk_off_score"] == 20.0
    assert "风格偏向大盘/核心资产" not in result["evidence"]


def test_style_switch_scores_and_returns_the_same_full_risk_signal(monkeypatch):
    full_signal = {
        "status": "reduce",
        "triggered": True,
        "score": 80,
        "headline": "持仓与板块风险同时触发",
    }
    monkeypatch.setattr(hot_data, "_market_sentiment_news_rows", lambda _date: [])
    monkeypatch.setattr(
        hot_data,
        "fetch_tech_risk_signal",
        lambda *_args, **_kwargs: full_signal,
    )

    result = hot_data._build_style_switch_signal("2026-09-04", 20, sentiment={})

    assert result["risk_off_score"] == 42.4
    assert result["switch_score"] == 10.0
    assert result["tech_risk_signal"] is full_signal
    assert result["decision_radar"] is full_signal
    assert "持仓与板块风险同时触发" in result["evidence"]


def test_historical_risk_news_query_stops_at_requested_day_end():
    captured = {}

    def query(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return []

    assert tech_risk._load_recent_news(query, "2026-09-04", days=2) == []
    assert "publish_time < DATE_ADD(DATE(:trade_date), INTERVAL 1 DAY)" in captured["sql"]
    assert "<= DATE_ADD(:trade_date, INTERVAL 1 DAY)" not in captured["sql"]
    assert captured["params"] == {"trade_date": "2026-09-04"}


def test_historical_risk_context_never_loads_current_index_or_current_portfolio():
    statements = []

    def query(sql, params):
        statements.append(str(sql))
        assert "st_user_portfolio" not in str(sql)
        return []

    result = tech_risk.fetch_black_swan_signal(
        query,
        "2026-09-04",
        news_rows=[],
        sector_rows=[],
        candidate_rows=[],
    )

    assert result["as_of_date"] == "2026-09-04"
    assert result["exposure_status"] == "unavailable_no_historical_snapshot"
    assert any("FROM sm_index_kline" in statement for statement in statements)
    assert all("FROM sm_index_current" not in statement for statement in statements)


def test_historical_candidate_context_does_not_join_current_stock_snapshot():
    statements = []

    def query(sql, _params):
        statements.append(str(sql))
        return []

    tech_risk._load_candidate_rows(query, "2026-09-04")

    recommended = next(
        statement for statement in statements if "FROM st_recommended_stocks r" in statement
    )
    assert "LEFT JOIN sm_stock_snapshot s ON 1 = 0" in recommended
    assert "snapshot_date <= :trade_date" in recommended
    assert "s.stock_code = r.stock_code" not in recommended


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


def test_stale_theme_table_does_not_claim_requested_cutoff_or_drive_switch(monkeypatch):
    theme_rows = pd.DataFrame(
        [
            {
                "snapshot_date": trade_date,
                "plate_type": 1,
                "rank": rank,
                "concept_code": f"C{rank}",
                "concept_name": f"测试概念{rank}",
                "change_pct": 1.0,
                "hot_value": 100 - rank,
                "hot_tag": "",
            }
            for trade_date in ("2026-09-01", "2026-09-02")
            for rank in (1, 2)
        ]
    )
    monkeypatch.setattr(
        market_sentiment.pd,
        "read_sql",
        lambda *_args, **_kwargs: theme_rows,
    )

    result = market_sentiment.analyze_main_theme(
        object(),
        ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"],
    )
    signal = hot_data._style_switch_signal_from_inputs(
        {
            "theme_analysis": result,
            "style_analysis": {"size_style": {"status": "unavailable"}},
            "capital_analysis": {"status": "no_data"},
        },
        [],
    )

    assert result["status"] == "partial"
    assert result["reason"] == "HOT_THEME_REQUESTED_CUTOFF_MISSING"
    assert result["data_cutoff"] == "2026-09-02"
    assert result["lookback_days"] == 2
    assert result["requested_lookback_days"] == 4
    assert result["rotation_score"] is None
    assert result["coverage"]["requested_cutoff_covered"] is False
    assert signal["status"] == "unavailable"


def _theme_rows(trade_dates, *, items_per_day=10):
    return pd.DataFrame(
        [
            {
                "snapshot_date": trade_date,
                "plate_type": 1,
                "rank": rank,
                "concept_code": f"C{rank:02d}",
                "concept_name": f"测试概念{rank:02d}",
                "change_pct": 1.0,
                "hot_value": 100 - rank,
                "hot_tag": "",
            }
            for trade_date in trade_dates
            for rank in range(1, items_per_day + 1)
        ]
    )


def test_sparse_daily_theme_top10_cannot_drive_style_switch(monkeypatch):
    trade_dates = ["2026-09-03", "2026-09-04"]
    monkeypatch.setattr(
        market_sentiment.pd,
        "read_sql",
        lambda *_args, **_kwargs: _theme_rows(trade_dates, items_per_day=1),
    )

    result = market_sentiment.analyze_main_theme(object(), trade_dates)
    signal = hot_data._style_switch_signal_from_inputs(
        {
            "theme_analysis": result,
            "style_analysis": {"size_style": {"status": "unavailable"}},
            "capital_analysis": {"status": "no_data"},
        },
        [],
    )

    assert result["status"] == "partial"
    assert result["reason"] == "HOT_THEME_DAILY_TOP10_INCOMPLETE"
    assert result["rotation_score"] is None
    assert result["coverage"]["daily_top10_status"] == "incomplete"
    assert signal["status"] == "unavailable"
    assert signal["switch_score"] is None


def test_missing_middle_theme_date_cannot_drive_style_switch(monkeypatch):
    requested_dates = ["2026-09-02", "2026-09-03", "2026-09-04"]
    available_dates = ["2026-09-02", "2026-09-04"]
    monkeypatch.setattr(
        market_sentiment.pd,
        "read_sql",
        lambda *_args, **_kwargs: _theme_rows(available_dates),
    )

    result = market_sentiment.analyze_main_theme(object(), requested_dates)
    signal = hot_data._style_switch_signal_from_inputs(
        {
            "theme_analysis": result,
            "style_analysis": {"size_style": {"status": "unavailable"}},
            "capital_analysis": {"status": "no_data"},
        },
        [],
    )

    assert result["status"] == "partial"
    assert result["reason"] == "HOT_THEME_DATE_COVERAGE_INCOMPLETE"
    assert result["rotation_score"] is None
    assert result["coverage"]["date_coverage_pct"] == 66.7
    assert result["coverage"]["date_coverage_status"] == "incomplete"
    assert result["coverage"]["missing_trade_dates"] == ["2026-09-03"]
    assert signal["status"] == "unavailable"
    assert signal["switch_score"] is None


def test_complete_theme_window_produces_rotation_evidence(monkeypatch):
    trade_dates = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"]
    monkeypatch.setattr(
        market_sentiment.pd,
        "read_sql",
        lambda *_args, **_kwargs: _theme_rows(trade_dates),
    )

    result = market_sentiment.analyze_main_theme(object(), trade_dates)
    signal = hot_data._style_switch_signal_from_inputs(
        {
            "theme_analysis": result,
            "style_analysis": {"size_style": {"status": "unavailable"}},
            "capital_analysis": {"status": "no_data"},
        },
        [],
    )

    assert result["status"] == "ok"
    assert result["rotation_score"] is not None
    assert result["coverage"]["date_coverage_status"] == "complete"
    assert result["coverage"]["daily_top10_status"] == "complete"
    assert signal["status"] != "unavailable"
    assert signal["switch_score"] is not None


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
