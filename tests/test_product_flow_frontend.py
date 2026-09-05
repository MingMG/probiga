from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _script() -> str:
    return (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")


def test_watchlist_keeps_existing_table_and_adds_live_sequence_numbers():
    script = _script()
    portfolio = script.split("portfolio: function", 1)[1].split(
        "datasource: function", 1
    )[0]

    assert '<th class="pf-row-number-col">编号</th>' in portfolio
    assert "'<td class=\\\"pf-row-number\\\">'+(idx + 1)+'</td>'" in portfolio
    assert "function pfRenumberRows(table)" in portfolio
    assert "pfRenumberRows(tb);" in portfolio
    assert "tbody tr[draggable] .pf-row-number" in portfolio
    for existing_column in (
        "代码",
        "名称",
        "现价",
        "涨跌%",
        "成本",
        "持有",
        "当日盈亏",
        "持仓盈亏",
        "收益率",
        "资金态度/净流",
        "盯盘建议",
        "数据时刻",
        "来源",
        "操作",
        "分析",
        "历史",
    ):
        assert existing_column in portfolio


def test_combined_hot_rank_keeps_original_order_and_adds_non_ranking_context():
    script = _script()
    renderer = script.split("function renderFusedData(container, res)", 1)[1].split(
        "/* ===== 合并Tab辅助函数 ===== */", 1
    )[0]

    original_columns = (
        "['排名', '代码', '名称', '行业', '人气标签', '概念板块', '涨跌幅', "
        "'东财排', '同花顺', '雪球', '新浪', '综合分', '来源', '分时', '我的关联']"
    )
    assert original_columns in renderer
    assert ".sort(" not in renderer
    assert "r.fused_rank" in renderer
    assert "r.total_score" in renderer
    assert "沿用系统现有综合分与排名" in renderer
    assert "自选和策略关联不参与排序" in renderer
    assert "查看市场趋势与风格" in renderer
    assert "switchTab(\\'sentiment\\')" in renderer
    assert "apiGet('/portfolio/codes')" in renderer
    assert "fetchRawJsonWithTimeout(path, 12000)" in renderer
    assert "'/api/v3/stock-pool'" in renderer
    assert "item.is_strategy_candidate !== true" in renderer
    assert "candidateCenterStockPoolIsReadable(pool)" in renderer
    assert "策略关联未提供" not in renderer
    assert "card('4源'" not in renderer

    quick_add = renderer.split("window.hotRankAddWatch = function", 1)[1]
    assert "'/api/portfolio/add'" in quick_add
    assert "window.hotRankUndoWatch = function" in quick_add
    assert "'/api/portfolio/remove/'" in quick_add
    assert "window.pfAddWithCode" not in quick_add
    assert "confirm(" not in quick_add
    assert "alert(" not in quick_add


def test_navigation_distinguishes_research_from_strategy_stock_results():
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    script = _script()

    for text in ("交易决策总览", "策略选股结果", "条件选股（研究）", "策略研究与竞技"):
        assert text in index
        assert text in script
    assert 'data-tab="trading-v3-candidates"' in index
    assert 'data-tab="screen"' in index
    assert 'data-tab="strategy-center"' in index
    assert "PAGE_TITLES['sentiment'] = '🧠 市场趋势与风格'" in script


def test_new_navigation_has_six_primary_entry_points_and_keeps_secondary_pages():
    script = _script()
    layout = script.split("var LAYOUT_NEW = [", 1)[1].split(
        "];\n    var TRADING_MODULE_NAV_ITEMS", 1
    )[0]
    primary = layout.split("{group:'主要入口', items:[", 1)[1].split(
        "]},", 1
    )[0]
    expected_ids = (
        "portfolio",
        "fused",
        "trading-v3-candidates",
        "strategy-center",
        "sentiment",
        "trading",
    )
    assert primary.count("{id:") == 6
    assert [primary.index("id:'" + item + "'") for item in expected_ids] == sorted(
        primary.index("id:'" + item + "'") for item in expected_ids
    )
    for secondary in (
        "strategy-backtest",
        "market-radar",
        "screen",
        "datasource",
        "ai-stock",
    ):
        assert "id:'" + secondary + "'" in layout
    assert "arrangePrimaryNavigation(LAYOUT_NEW)" not in script


def test_market_observation_uses_real_trend_and_explicit_style_availability():
    script = _script()
    section = script.split("function marketTrendPayload", 1)[1].split(
        "function loadCommandPage", 1
    )[0]

    assert "'/api/hot-data/market-trend?date='" in section
    assert "['daily', 'weekly', 'monthly']" in section
    for label in ("日线", "周线", "所处位置", "综合判断", "后续观察"):
        assert label in section
    assert "confirmation_status === 'provisional'" in section
    assert "sourceStatus === 'stale'" in section
    assert "retained_history" in section
    assert "保留的当日判断与随后走势" in section
    assert "subsequent_change_pct" in section
    assert "missing_indices" in section
    assert "公式、参数与证据" in section
    for dimension in ("size", "growth_value", "breadth", "rotation"):
        assert "['" + dimension + "'," in section
    assert "data-status=" in section
    assert "item.status === 'partial'" in section
    assert "证据不完整" in section
    assert "days=1" in section
    assert "days=5" in section
    assert "风格窗口对照" in section
    assert "styleSignal.status" in section
    assert "独立证据文本未提供" in section
    assert "switchTab(\\'sector\\')" in section
    assert "switchTab(\\'market-radar\\')" in section


def test_market_radar_supports_truthful_relation_scopes_and_next_steps():
    script = _script()
    radar = script.split("function loadMarketRadarPage", 1)[1].split(
        "/* ===== 策略与模拟", 1
    )[0]

    for scope in ("all", "watchlist", "holding", "strategy_candidate"):
        assert scope in radar
    assert "scopeQuery" in radar
    assert "relation_context" in radar
    assert "candidate_date" in radar
    assert "部分关系不可用" in radar
    assert "板块关联使用现有概念成分；不可用时退回展示股关系" in radar
    assert "关系不完整" in radar
    assert "概念成员关系不可用，当前筛选可能漏计" in radar
    assert "openStockDetail" in radar
    assert "window.marketRadarAddWatch" in radar
    assert "openTradingModule(\\'trading-v3-candidates\\')" in radar


def test_sector_auto_refresh_preserves_expansion_filters_and_scroll():
    script = _script()
    sector = script.split("/* ===== 板块异动 ===== */", 1)[1].split(
        "function renderJqMinutePanel", 1
    )[0]

    assert "var sectorMoveExpanded = {};" in sector
    assert "sectorMoveGroupBy + ':'" in sector
    assert "data-sector-key" in sector
    assert "sectorMoveExpanded[sectorKey]" in sector
    assert "sectorMoveExpanded[key] = willOpen" in sector
    assert "savedScrollTop" in sector
    assert "scrollRoot.scrollTop = savedScrollTop" in sector
    refresh_body = sector.split("sectorMoveTimer = setInterval", 1)[1]
    assert "sectorMoveFilter = 'all'" not in refresh_body
    assert "sectorMoveGroupBy = 'industry'" not in refresh_body


def test_backtest_page_reuses_the_single_versioned_trading_v3_validation_view():
    script = _script()
    loader = script.split("'strategy-backtest': function (d, c)", 1)[1].split(
        "}\n    };", 1
    )[0]

    assert "loadTradingModulePage(c" in loader
    assert "tradingView:'validation'" in loader
    assert "modulePage:'v3'" in loader
    assert "function loadStrategyBacktestPage" not in script
    assert "function loadSimTradePageLegacy" not in script
    assert "/api/sim-trade/backtest" not in script
    assert "window.simTradeBacktest = function()" in script
    assert "switchTab('strategy-backtest')" in script
