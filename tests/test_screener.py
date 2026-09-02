from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import screener


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(screener.router, prefix="/api")
    return TestClient(app)


def test_screener_catalog_exposes_unified_presets():
    response = _client().get("/api/screener/catalog")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert {item["key"] for item in body["presets"]} >= {
        "trend_breakout",
        "capital_support",
        "technical_cross",
    }
    versions = {item["version"]: item for item in body["versions"]}
    assert set(versions) == {"V2", "V3", "V4", "V5", "V6"}
    assert versions["V3"]["production_selector"] is True
    for version in ("V4", "V5", "V6"):
        assert versions[version]["lifecycle"] == "PRODUCTION_ADVISORY"
        assert versions[version]["decision"] == "ACTIVE_BOUNDED"
        assert versions[version]["production_selector"] is True
        assert versions[version]["research_release_gate"] == "BLOCK_ORDER_AUTHORITY"
        assert versions[version]["order_authority"] is False
    assert body["execution_boundary"] == {
        "production_ranking_active": True,
        "research_models_order_blocked": True,
        "paper_orders_allowed": False,
        "real_orders_allowed": False,
    }
    assert body["selector"]["mode"] == "V3_V4_V5_V6_GATED_MULTI_HORIZON_ENSEMBLE"
    assert body["selector"]["order_authority"] is False


def test_screener_status_blocks_stale_base_data(monkeypatch):
    dates = {
        "daily_kline": "2026-08-06",
        "current_quote": "2026-08-07",
        "capital_flow": "2026-08-07",
        "analysis": "2026-08-07",
        "recommendation": "2026-08-07",
        "news": "2026-08-07",
        "notice": "2026-08-07",
    }
    monkeypatch.setattr(
        screener,
        "_safe_runtime_latest_date",
        lambda source: dates[source],
    )

    result = screener._runtime_status(datetime(2026, 8, 10, 17, 0, 0))

    assert result["expected_completed_session"] == "2026-08-10"
    assert result["selection_ready"] is False
    assert result["recommendation_ready"] is False
    assert result["gate"] == "DATA_STALE"
    assert result["actionable_output_allowed"] is False


def test_screener_status_activates_bounded_v456_but_keeps_order_gate_blocked(monkeypatch):
    monkeypatch.setattr(
        screener,
        "_safe_runtime_latest_date",
        lambda _source: "2026-08-10",
    )

    response = _client().get("/api/screener/status")

    assert response.status_code == 200
    body = response.json()
    active = {
        item["version"]
        for item in body["versions"]
        if item["decision"] == "ACTIVE_BOUNDED"
    }
    assert active == {"V4", "V5", "V6"}
    assert body["selector"]["research_release_gates"] == {
        "V4": "BLOCK_ORDER_AUTHORITY",
        "V5": "BLOCK_ORDER_AUTHORITY",
        "V6": "BLOCK_ORDER_AUTHORITY",
    }
    assert body["actionable_output_allowed"] is False


def test_screener_ui_loads_status_and_labels_production_ensemble():
    root = Path(__file__).resolve().parents[1]
    script = (root / "server/static/js/app.js").read_text(encoding="utf-8")
    index = (root / "server/static/index.html").read_text(encoding="utf-8")
    trading_script = (root / "server/static/js/trading-v3.js").read_text(
        encoding="utf-8"
    )
    trading_page = (root / "server/static/trading-v3.html").read_text(
        encoding="utf-8"
    )
    execution_script = (root / "server/static/js/trading-v2.js").read_text(
        encoding="utf-8"
    )
    execution_page = (root / "server/static/trading-v2.html").read_text(
        encoding="utf-8"
    )

    assert "screenerJson('/api/screener/status')" in script
    assert "V3-V6 生产融合选股" in script
    assert "V4 硬门禁、V5 全局市场状态、V6 PIT 财务证据参与生产排序" in script
    assert "screenerVersionScores" in script
    assert "row.action || 'WATCH'" in script
    assert "style.css?v=45" in index
    assert "app.js?v=120" in index
    assert 'data-tab="trading-v3-candidates" data-trading-view="candidates"' in index
    assert 'data-tab="trading-v3-ledger"' not in index
    assert 'data-trading-view="candidates"' in index
    assert "{id:'trading-v3-candidates', modulePage:'v3', tradingView:'candidates'" in script
    assert "if (item.candidateDecision)" in script
    assert "loadCandidateDecisionPage(d, container);" in script
    assert "if (item.candidateCenter)" in script
    assert "loadCandidateCenterPage(d, container);" in script
    assert "var isTradingItem = !!it.tradingSection;" in script
    assert "V3.6 生产真值" not in script
    assert "统一决策、研究验证与模拟账本" in script
    assert "保留原有完整账本视图" in script
    assert "V4 硬拒绝会计入证据覆盖" in script
    assert 'id="candidateCenterDateFilter"' in script
    assert 'id="candidateCenterStockFilter"' in script
    assert "queryButton.addEventListener('click', queryCandidateCenter)" in script
    assert "stockInput.addEventListener('input', applyStockFilter)" in script
    assert "今日大盘预期" in script
    assert "今天怎么操作" in script
    assert "数据回退，禁止推荐" in script
    assert "premarketForecastIsFresh" in script
    assert "历史回退主题不参与今天的市场预期" in script
    assert "trading-v3.js?v=37" in trading_page
    assert "自选股里的真实持仓，今天怎么操作" in trading_page
    assert "买入范围" in trading_page
    assert "卖出范围" in trading_page
    assert "/api/portfolio/holding-strategy" in trading_script
    assert "V2 执行门禁" not in script
    assert "当前 V2 快照" not in script
    assert "TODAY'S STRATEGY · 统一结论" in trading_page
    assert "必须通过统一执行门禁" in trading_page
    assert "当前模拟账户快照" in trading_page
    assert "function decisionRead(url, timeoutMs)" in script
    assert "!!requested && !!dataDate" in script
    assert "context.historical_read_only === true" in script
    assert "premarketForecastIsFresh" in script
    assert "return !!sessionDate && sessionDate ===" in script
    assert "state.loading=true" in trading_script
    assert "runRequestQueue(bootstrapRequest,1)" in trading_script
    assert "'/daily-result'" in trading_script
    assert "var bootstrapResult=results[0]||{}" in trading_script
    assert "if(!bootstrapResult.ok)" in trading_script
    assert "var daily=bootstrapResult.value||{}" in trading_script
    assert "api3(withDate('/overview'" not in trading_script
    assert "API_DOWN：每日结果接口不可用" in trading_script
    assert "if(delivery==='DEGRADED')" in trading_script
    assert "daily.acceptance.accepted!==true" in trading_script
    assert "runRequestQueue(requests,3)" in trading_script
    assert "ctx.data_blocked_reason" in trading_script
    assert "数据阻断（DATA_BLOCKED），本批次未形成策略池" in trading_script
    assert "请求超时（" in trading_script
    assert "ctx.historical_read_only===true" in trading_script
    assert "requestedFrameDate()" in trading_script
    assert "if(dateChanged||viewChanged)load()" in trading_script
    assert "V2 复验阻断" not in trading_script
    assert "V2 执行门禁" not in trading_script
    assert "V3 尚未产生首个决策" not in trading_script
    assert "trading-v2.js?v=15" in execution_page
    assert "模拟执行账本（内部 V2 协议）" in execution_page
    assert "正在读取统一模拟账本" in execution_page
    assert "V2 回测与决策执行器" not in execution_script
    assert "尚无完整 V2 快照" not in execution_script
    assert "today_closed_positions" in trading_script
    assert "data-position-detail-index" in trading_script
    assert "买入时间" in trading_page
    assert "卖出时间" in trading_page
    assert "卖出价格" in trading_page


def test_apply_filters_normalizes_rows_and_excludes_st():
    request = screener.ScreenerRunRequest(
        top=10,
        filters={"min_score": 50, "exclude_st": True},
    )
    rows, stats = screener._apply_filters(
        [
            {"stock_code": "1", "short_name": "Alpha", "change_pct": 2, "vol_ratio": 1.5},
            {"stock_code": "2", "short_name": "ST Beta", "change_pct": 8, "vol_ratio": 2.0},
            {"stock_code": "920305", "short_name": "云创退", "change_pct": 30, "vol_ratio": 3.0},
        ],
        request,
    )
    assert stats["input_count"] == 3
    assert [row["stock_code"] for row in rows] == ["000001"]
    assert rows[0]["stock_name"] == "Alpha"
    assert rows[0]["matched_conditions"]
    assert rows[0]["score"] > 50
    assert rows[0]["decision_scope"] == "PRODUCTION_SELECTION_ADVISORY"
    assert rows[0]["selector_mode"] == "V3_V4_V5_V6_GATED_MULTI_HORIZON_ENSEMBLE"
    assert rows[0]["action"] == "WATCH"
    assert rows[0]["actionable"] is False


def test_apply_filters_rejects_non_stock_and_future_listing_codes():
    request = screener.ScreenerRunRequest(top=10)
    rows, _stats = screener._apply_filters(
        [
            {"stock_code": "899050", "short_name": "北证指数"},
            {"stock_code": "810011", "short_name": "定转A"},
            {"stock_code": "301677", "short_name": "未上市"},
            {"stock_code": "600000", "short_name": "浦发银行"},
        ],
        request,
        listed_codes={"600000"},
    )

    assert [row["stock_code"] for row in rows] == ["600000"]


def test_run_preset_keeps_data_date_and_requested_date(monkeypatch):
    from server.api.routers import hot_data

    received = {}

    def fake_screen_stocks(**kwargs):
        received.update(kwargs)
        return {
            "data": [{"stock_code": "600001", "short_name": "Demo", "change_pct": 3}],
            "date": "2026-07-22",
            "data_date": "2026-07-22",
            "freshness": "fallback",
        }

    monkeypatch.setattr(screener, "_listed_codes", lambda _date: {"600001"})
    monkeypatch.setattr(
        screener,
        "_enrich_selector_evidence",
        lambda rows, _date, **_kwargs: rows,
    )
    monkeypatch.setattr(screener, "_attach_correlation_clusters", lambda rows, _date: rows)
    monkeypatch.setattr(hot_data, "screen_stocks", fake_screen_stocks)
    request = screener.ScreenerRunRequest(
        preset="trend_breakout",
        as_of_date="2026-07-23",
        filters={"vol_boost": 1.7, "min_change": 1.2, "max_change": 8.5},
    )
    result = screener._run_preset(request, "2026-07-23")
    assert result["requested_date"] == "2026-07-23"
    assert result["data_date"] == "2026-07-22"
    assert result["freshness"] == "fallback"
    assert result["data"][0]["data_date"] == "2026-07-22"
    assert result["decision_scope"] == "PRODUCTION_SELECTION_ADVISORY"
    assert result["actionable_output_allowed"] is False
    assert received["top"] == 6000
    assert received["vol_boost"] == 1.7
    assert received["min_change"] == 1.2
    assert received["max_change"] == 8.5


def test_enrich_selector_evidence_joins_analysis_recommendation_and_market_mood(monkeypatch):
    def fake_rows(sql, _params=None, context=""):
        if context == "screener_selector_market_evidence":
            return [{
                "stock_code": "600001", "short_name": "Demo", "close": 10,
                "high": 10.2, "low": 9.8, "volume": 1000, "amount": 10_000_000,
                "turnover_ratio": 2.5, "change_pct": 3.2,
            }]
        if context == "screener_selector_analysis_evidence":
            return [{"stock_code": "600001", "fundamental": 70, "growth_score": 65, "entry_score": 72}]
        if context == "screener_selector_recommendation_evidence":
            return [{"stock_code": "600001", "final_trade_score": 81, "quality_score": 75}]
        if context == "screener_selector_market_mood":
            return [{"market_mood_score": 66}]
        if context == "screener_selector_pit_finance":
            assert "notice_date <= :target_date" in sql
            assert "etl_sync_at < DATE_ADD(:target_date, INTERVAL 1 DAY)" in sql
            return [{
                "stock_code": "600001",
                "finance_report_date": "2026-06-30",
                "finance_notice_date": "2026-07-20",
                "finance_knowledge_at": "2026-07-20 09:00:00",
                "roe_wtd": 12,
            }]
        raise AssertionError(context)

    monkeypatch.setattr(screener, "_engine_rows", fake_rows)

    rows = screener._enrich_selector_evidence(
        [{"stock_code": "600001", "change_pct": 3.2}],
        "2026-08-10",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["stock_code"] == "600001"
    assert row["final_trade_score"] == 81
    assert row["fundamental"] == 70
    assert row["global_market_regime_score"] == 66
    assert row["finance_pit_verified"] is False
    assert row["finance_pit_status"] == "DATA_BLOCKED"
    assert row["finance_pit_reason"] == (
        "PIT_COMMON_CUTOFF_EXACT_DECISION_TIME_REQUIRED"
    )
    assert "finance_report_date" not in row
    assert row["data_date"] == "2026-08-10"
    assert row["limit_trigger_pct"] == 9.5
    assert row["limit_up_locked"] is False


def test_correlation_clusters_use_overlapping_official_returns(monkeypatch):
    history = []
    for index in range(12):
        day = f"2026-07-{index + 1:02d}"
        for code, value in (("600001", 0.01 + index * 0.0001), ("600002", 0.02 + index * 0.0002), ("600003", -0.01 - index * 0.0001)):
            history.append({
                "stock_code": code,
                "trade_date": day,
                "pre_close": 100,
                "close": 100 * (1 + value),
            })

    monkeypatch.setattr(
        screener,
        "_engine_rows",
        lambda _sql, _params=None, context="": history
        if context == "screener_selector_correlation_history"
        else [],
    )

    rows = screener._attach_correlation_clusters(
        [{"stock_code": "600001"}, {"stock_code": "600002"}, {"stock_code": "600003"}],
        "2026-08-10",
    )

    by_code = {row["stock_code"]: row for row in rows}
    assert by_code["600001"]["correlation_cluster"] == by_code["600002"]["correlation_cluster"]
    assert by_code["600003"]["correlation_cluster"] != by_code["600001"]["correlation_cluster"]
    assert all(row["correlation_cluster_status"] == "VERIFIED_60D" for row in rows)


def test_candidate_center_passes_plain_values_to_recommendation_source(monkeypatch):
    from server.api.routers import hot_data

    received = {}

    def fake_recommended_stocks(**kwargs):
        received.update(kwargs)
        return {
            "date": "2026-07-24",
            "freshness": {"status": "exact"},
            "data": [
                {
                    "stock_code": "600001",
                    "stock_name": "Demo",
                    "final_trade_score": 80,
                    "signal_status": "WATCH",
                }
            ],
        }

    monkeypatch.setattr(hot_data, "recommended_stocks", fake_recommended_stocks)
    monkeypatch.setattr(screener, "_engine_rows", lambda *_args, **_kwargs: [])

    result = screener.screener_candidate_center("2026-07-24", 10)

    assert result["status"] == "ok"
    assert result["summary"]["candidate_count"] == 1
    assert result["summary"]["buy_ready_count"] == 0
    assert result["candidates"][0]["action"] == "DATA_BLOCKED"
    assert result["candidates"][0]["new_buy_eligible"] is False
    assert received == {
        "trade_date": "2026-07-24",
        "strategy": "",
        "signal_status": "",
        "start_date": "",
        "end_date": "",
        "prefer_latest": True,
    }


def test_candidate_center_counts_buy_ready_only_when_all_four_gates_pass(monkeypatch):
    from server.api.routers import hot_data

    monkeypatch.setattr(
        hot_data,
        "recommended_stocks",
        lambda **_kwargs: {
            "date": "2026-08-04",
            "data": [
                {
                    "stock_code": "000001",
                    "signal_status": "CONFIRM",
                    "recommend_status": "ALLOW",
                    "chase_risk_status": "ALLOW",
                    "ordinary_buy_eligible": 1,
                },
                {
                    "stock_code": "603221",
                    "signal_status": "BUY_READY",
                    "recommend_status": "ALLOW",
                    "chase_risk_status": "EXECUTION_BLOCKED",
                    "ordinary_buy_eligible": 0,
                },
            ],
        },
    )
    monkeypatch.setattr(screener, "_engine_rows", lambda *_args, **_kwargs: [])

    result = screener.screener_candidate_center("2026-08-04", 10)

    assert result["summary"]["buy_ready_count"] == 1
    assert result["candidates"][0]["new_buy_eligible"] is True
    assert result["candidates"][1]["new_buy_eligible"] is False
    assert result["candidates"][1]["action"] == "EXECUTION_BLOCKED"
    assert result["actionable_output_allowed"] is False
