import json
from pathlib import Path
import shutil
import subprocess

import pytest


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


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_hot_rank_strategy_relations_ignore_old_date_responses():
    script = _script()
    start = script.index("    function refreshHotRankStrategyRelations(force)")
    end = script.index("    function refreshHotRankContext()", start)
    helper = script[start:end]
    harness = f"""
const assert = require('assert');
global.window = {{}};
let target = '2026-09-04';
let calls = [];
function recommendationDateValue() {{ return target; }}
function currentDateValue() {{ return target; }}
function updateHotRankRelationCells() {{}}
function candidateCenterStockPoolIsReadable() {{ return true; }}
function deferred() {{
  let resolve, reject;
  const promise = new Promise((yes, no) => {{ resolve = yes; reject = no; }});
  return {{promise, resolve, reject}};
}}
function fetchRawJsonWithTimeout(path) {{
  const request = deferred();
  calls.push({{path, request}});
  return request.promise;
}}
{helper}
function pool(day, code) {{
  return {{data:{{
    decision_session_date:day,
    items:[{{stock_code:code,is_strategy_candidate:true,strategy_keys:['策略'+code]}}]
  }}}};
}}
(async function() {{
  const oldRequest = refreshHotRankStrategyRelations(false);
  target = '2026-09-05';
  const newRequest = refreshHotRankStrategyRelations(false);
  assert.strictEqual(calls.length, 2, 'a new date must start its own request');
  calls[1].request.resolve(pool('2026-09-05', '000002'));
  await newRequest;
  calls[0].request.resolve(pool('2026-09-04', '000001'));
  await oldRequest;
  assert.strictEqual(window._hotStrategyDate, '2026-09-05');
  assert.deepStrictEqual(Object.keys(window._hotStrategyCodes), ['000002']);
  assert.strictEqual(window._hotStrategyCodesReady, true);

  const sameTarget = refreshHotRankStrategyRelations(true);
  const reused = refreshHotRankStrategyRelations(false);
  assert.strictEqual(sameTarget, reused, 'the same date should reuse its in-flight request');
  calls[2].request.resolve(pool('2026-09-05', '000003'));
  await sameTarget;

  target = '2026-09-06';
  const mismatched = refreshHotRankStrategyRelations(true);
  calls[3].request.resolve(pool('2026-09-05', '000004'));
  await mismatched;
  assert.strictEqual(window._hotStrategyCodesReady, false);
  assert.deepStrictEqual(window._hotStrategyCodes, {{}});
  assert.match(window._hotStrategyCodesError, /日期与当前热榜日期不一致/);
  process.stdout.write(JSON.stringify({{status:'PASS'}}));
}})().catch(function(error) {{ console.error(error); process.exit(1); }});
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_hot_rank_subview_ignores_stale_view_and_date_responses():
    script = _script()
    start = script.index("    function hotRankRequestContext(viewId, dateValue)")
    end = script.index("    function marketTrendPayload(payload)", start)
    helpers = script[start:end]
    loader = script.split("        fused: function (d, c) {", 1)[1].split(
        "        /* ── 板块分析", 1
    )[0]

    assert "hotRankRequestContext(vid, requestDate)" in loader
    for call in (
        "loadFusedTab(requestDate, body, liveRefresh, requestContext)",
        "loadEastTab(requestDate, body, requestContext)",
        "loadThsTab(requestDate, body, requestContext)",
        "loadXqTab(requestDate, body, requestContext)",
        "loadSinaTab(requestDate, body, requestContext)",
    ):
        assert call in loader

    harness = """
const assert = require('assert');
global.window = {_subViewState:{fused:'fused'}};
let activeTab = 'fused';
let activeDate = '2026-09-04';
let MARKET_CLOCK = {is_intraday:false, ui_trade_date:'2026-09-05'};
const requests = [];
const container = {innerHTML:'initial'};
function activeTabId() { return activeTab; }
function currentDateValue() { return activeDate; }
function isTradingTime() { return MARKET_CLOCK.is_intraday === true; }
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
function apiGet(path) {
  const request = deferred();
  requests.push({path, request});
  return request.promise;
}
function syncDateFromResponse() {}
function renderFusedData(target) { target.innerHTML = 'fused'; }
function card() { return ''; }
function rankBadge() { return ''; }
function nameLink() { return ''; }
function clsPct() { return ''; }
function pct() { return ''; }
function fmt() { return ''; }
function minuteBtn() { return ''; }
function sourceTag() { return ''; }
function fmtMoney() { return ''; }
function escHtml(value) { return String(value || ''); }
""" + helpers + """
(async function() {
  const oldFusedContext = hotRankRequestContext('fused', activeDate);
  const oldFused = loadFusedTab(activeDate, container, false, oldFusedContext);

  window._subViewState.fused = 'east';
  const eastContext = hotRankRequestContext('east', activeDate);
  const east = loadEastTab(activeDate, container, eastContext);
  requests[1].request.resolve({data:[]});
  await east;
  assert.match(container.innerHTML, /暂无数据/);

  requests[0].request.resolve({data:[{stock_code:'000001'}]});
  await oldFused;
  assert.match(container.innerHTML, /暂无数据/, 'old fused response must not overwrite the selected East view');

  window._subViewState.fused = 'fused';
  const oldDateContext = hotRankRequestContext('fused', activeDate);
  const oldDate = loadFusedTab(activeDate, container, false, oldDateContext);
  activeDate = '2026-09-05';
  requests[2].request.resolve({data:[{stock_code:'000002'}]});
  await oldDate;
  assert.match(container.innerHTML, /暂无数据/, 'response for the previous date must not write the DOM');
  process.stdout.write(JSON.stringify({status:'PASS'}));
})().catch(function(error) { console.error(error); process.exit(1); });
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_hot_rank_fused_uses_live_only_for_the_clock_live_date_and_rejects_mismatches():
    script = _script()
    start = script.index("    function hotRankRequestContext(viewId, dateValue)")
    end = script.index("    function marketTrendPayload(payload)", start)
    helpers = script[start:end]
    harness = """
const assert = require('assert');
global.window = {_subViewState:{fused:'fused'}};
let activeDate = '2026-09-04';
let MARKET_CLOCK = {is_intraday:true, ui_trade_date:'2026-09-06', recommendation_trade_date:'2026-09-06'};
const requests = [];
const renderedDates = [];
const container = {innerHTML:'initial'};
function activeTabId() { return 'fused'; }
function currentDateValue() { return activeDate; }
function isTradingTime() { return MARKET_CLOCK.is_intraday === true; }
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
function apiGet(path) {
  const request = deferred();
  requests.push({path, request});
  return request.promise;
}
function syncDateFromResponse() {}
function renderFusedData(target, response) { renderedDates.push(response.date); target.innerHTML = response.date; }
function card() { return ''; }
function rankBadge() { return ''; }
function nameLink() { return ''; }
function clsPct() { return ''; }
function pct() { return ''; }
function fmt() { return ''; }
function minuteBtn() { return ''; }
function sourceTag() { return ''; }
function fmtMoney() { return ''; }
function escHtml(value) { return String(value || ''); }
""" + helpers + """
(async function() {
  let context = hotRankRequestContext('fused', activeDate);
  const historical = loadFusedTab(activeDate, container, false, context);
  assert.strictEqual(requests[0].path, '/fused?snapshot_date=2026-09-04&top=100');
  requests[0].request.resolve({date:'2026-09-04', data:[{stock_code:'000001'}]});
  await historical;
  assert.deepStrictEqual(renderedDates, ['2026-09-04']);

  activeDate = '2026-09-06';
  context = hotRankRequestContext('fused', activeDate);
  const mismatchedLive = loadFusedTab(activeDate, container, false, context);
  assert.strictEqual(requests[1].path, '/fused-live?top=100');
  requests[1].request.resolve({date:'2026-09-05', live:true, data:[{stock_code:'000002'}]});
  await Promise.resolve();
  assert.strictEqual(requests[2].path, '/fused?snapshot_date=2026-09-06&top=100');
  assert.deepStrictEqual(renderedDates, ['2026-09-04'], 'wrong-date live data must never render');
  requests[2].request.resolve({date:'2026-09-06', data:[{stock_code:'000003'}]});
  await mismatchedLive;
  assert.deepStrictEqual(renderedDates, ['2026-09-04', '2026-09-06']);

  MARKET_CLOCK.is_intraday = false;
  context = hotRankRequestContext('fused', activeDate);
  const closed = loadFusedTab(activeDate, container, false, context);
  assert.strictEqual(requests[3].path, '/fused?snapshot_date=2026-09-06&top=100');
  requests[3].request.resolve({date:'2026-09-06', data:[{stock_code:'000004'}]});
  await closed;
  process.stdout.write(JSON.stringify({status:'PASS'}));
})().catch(function(error) { console.error(error); process.exit(1); });
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_hot_rank_manual_request_is_not_preempted_by_interval_refresh():
    script = _script()
    start = script.index("    function hotRankRequestContext(viewId, dateValue)")
    end = script.index("    function marketTrendPayload(payload)", start)
    helpers = script[start:end]
    loader = script.split("        fused: function (d, c) {", 1)[1].split(
        "        /* ── 板块分析", 1
    )[0]

    assert "return runHotRankRequest(requestContext" in loader
    assert "!hotRankCanUseLiveDate(currentDateValue()) || hotRankRequestPending()" in loader
    assert ".finally(function () { window._hotRankLiveInFlight = false; })" not in loader

    harness = """
const assert = require('assert');
global.window = {_subViewState:{fused:'east'}};
let activeDate = '2026-09-04';
const requests = [];
const container = {innerHTML:'fused-old'};
function activeTabId() { return 'fused'; }
function currentDateValue() { return activeDate; }
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
function apiGet(path) {
  const request = deferred();
  requests.push({path, request});
  return request.promise;
}
function syncDateFromResponse() {}
function renderFusedData(target) { target.innerHTML = 'fused'; }
function card() { return ''; }
function rankBadge() { return ''; }
function nameLink() { return ''; }
function clsPct() { return ''; }
function pct() { return ''; }
function fmt() { return ''; }
function minuteBtn() { return ''; }
function sourceTag() { return ''; }
function fmtMoney() { return ''; }
function escHtml(value) { return String(value || ''); }
window.renderTable = function(target, id) { target.innerHTML = id; };
""" + helpers + """
(async function() {
  let refreshFactories = 0;
  function intervalTick() {
    if (hotRankRequestPending()) return Promise.resolve('skipped');
    const context = hotRankRequestContext('east', activeDate);
    return runHotRankRequest(context, function() {
      refreshFactories += 1;
      return loadEastTab(activeDate, container, context);
    }).catch(function() { return 'failed'; });
  }

  const manualContext = hotRankRequestContext('east', activeDate);
  const manual = runHotRankRequest(manualContext, function() {
    return loadEastTab(activeDate, container, manualContext);
  });
  assert.strictEqual(hotRankRequestPending(), true);
  assert.strictEqual(await intervalTick(), 'skipped');
  assert.strictEqual(refreshFactories, 0);
  assert.strictEqual(requests.length, 1);

  requests[0].request.resolve({data:[{stock_code:'000001', change_pct:1}]});
  await manual;
  assert.strictEqual(container.innerHTML, 'east');
  assert.strictEqual(hotRankRequestPending(), false);

  const failedRefresh = intervalTick();
  assert.strictEqual(refreshFactories, 1);
  requests[1].request.reject(new Error('refresh failed'));
  assert.strictEqual(await failedRefresh, 'failed');
  assert.strictEqual(container.innerHTML, 'east');
  assert.strictEqual(hotRankRequestPending(), false);

  const olderGate = deferred();
  const olderContext = hotRankRequestContext('east', activeDate);
  const older = runHotRankRequest(olderContext, function() { return olderGate.promise; });
  const newerGate = deferred();
  const newerContext = hotRankRequestContext('east', activeDate);
  const newer = runHotRankRequest(newerContext, function() { return newerGate.promise; });
  olderGate.resolve();
  await older;
  assert.strictEqual(hotRankRequestPending(), true, 'older finally must not clear the newer token');
  newerGate.resolve();
  await newer;
  assert.strictEqual(hotRankRequestPending(), false);
  process.stdout.write(JSON.stringify({status:'PASS'}));
})().catch(function(error) { console.error(error); process.exit(1); });
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_hot_rank_membership_does_not_lose_an_add_completed_during_slow_read():
    script = _script()
    membership_start = script.index("    function refreshHotRankMembership(force)")
    membership_end = script.index("    function refreshHotRankStrategyRelations(force)", membership_start)
    membership = script[membership_start:membership_end]
    add_start = script.index("    window.hotRankAddWatch = function (code)")
    add_end = script.index("    window.hotRankUndoWatch = function (code)", add_start)
    add = script[add_start:add_end]
    harness = f"""
const assert = require('assert');
global.window = {{}};
function deferred() {{
  let resolve, reject;
  const promise = new Promise((yes, no) => {{ resolve = yes; reject = no; }});
  return {{promise, resolve, reject}};
}}
const slowMembership = deferred();
function apiGet(path) {{
  assert.strictEqual(path, '/portfolio/codes');
  return slowMembership.promise;
}}
function fetchRawJsonWithTimeout(path) {{
  assert.strictEqual(path, '/api/portfolio/add');
  return Promise.resolve({{status:'ok', short_name:'测试股票', position_preserved:false}});
}}
function updateHotRankRelationCells() {{}}
function hotRankFeedback() {{}}
{membership}
{add}
(async function() {{
  window._hotWatchCodes = {{'000099':true}};
  window._hotWatchMutationGeneration = 1;
  window._hotWatchMutationState = {{'000099':{{generation:1, present:true}}}};
  const oldRead = refreshHotRankMembership(false);
  await window.hotRankAddWatch('000001');
  assert.strictEqual(window._hotWatchCodes['000001'], true);
  slowMembership.resolve({{data:[]}});
  await oldRead;
  assert.strictEqual(window._hotWatchCodes['000001'], true, 'old membership snapshot must not erase the successful add');
  assert.strictEqual(window._hotWatchAddedByPage['000001'], true, 'undo remains available');
  assert.strictEqual(window._hotWatchCodes['000099'], undefined, 'the authoritative read must clear mutations older than the request');
  assert.strictEqual(window._hotWatchMutationState['000099'], undefined, 'covered mutation history must be discarded');
  process.stdout.write(JSON.stringify({{status:'PASS'}}));
}})().catch(function(error) {{ console.error(error); process.exit(1); }});
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_hot_rank_serializes_each_stock_watchlist_mutation():
    script = _script()
    add_start = script.index("    window.hotRankAddWatch = function (code)")
    undo_start = script.index("    window.hotRankUndoWatch = function (code)", add_start)
    end = script.index("    /* ===== 合并Tab辅助函数 ===== */", undo_start)
    mutations = script[add_start:end]
    harness = f"""
const assert = require('assert');
global.window = {{_hotWatchCodes:{{}}}};
const requests = [];
function deferred() {{
  let resolve, reject;
  const promise = new Promise((yes, no) => {{ resolve = yes; reject = no; }});
  return {{promise, resolve, reject}};
}}
function fetchRawJsonWithTimeout(path) {{
  const request = deferred();
  requests.push({{path, request}});
  return request.promise;
}}
function hotRankFeedback() {{}}
function updateHotRankRelationCells() {{}}
{mutations}
(async function() {{
  const firstAdd = window.hotRankAddWatch('000001');
  const duplicateAdd = window.hotRankAddWatch('000001');
  assert.strictEqual(firstAdd, duplicateAdd);
  assert.strictEqual(requests.length, 1, 'double click must send one add request');
  requests[0].request.resolve({{status:'ok', short_name:'测试股票', position_preserved:false}});
  await firstAdd;
  assert.strictEqual(window._hotWatchCodes['000001'], true);

  const firstUndo = window.hotRankUndoWatch('000001');
  const duplicateUndo = window.hotRankUndoWatch('000001');
  assert.strictEqual(firstUndo, duplicateUndo);
  assert.strictEqual(requests.length, 2, 'double click must send one delete request');
  requests[1].request.resolve({{status:'ok'}});
  await firstUndo;
  assert.strictEqual(window._hotWatchCodes['000001'], undefined);
  assert.strictEqual(window._hotWatchAddedByPage['000001'], undefined);
  process.stdout.write(JSON.stringify({{status:'PASS'}}));
}})().catch(function(error) {{ console.error(error); process.exit(1); }});
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


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


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_market_observation_ignores_slow_responses_for_an_old_selected_date():
    script = _script()
    start = script.index("    function loadSentimentPage(d, c)")
    end = script.index("    function loadCommandPage(d, c)", start)
    loader = script[start:end]
    harness = f"""
const assert = require('assert');
global.window = {{stopMonitorRefresh:function(){{}}}};
const container = {{innerHTML:''}};
const picker = {{value:'2026-09-04'}};
const requests = [];
function el(id) {{ return id === 'datePicker' ? picker : null; }}
function deferred() {{
  let resolve, reject;
  const promise = new Promise((yes, no) => {{ resolve = yes; reject = no; }});
  return {{promise, resolve, reject}};
}}
function request() {{ const item=deferred(); requests.push(item); return item.promise; }}
function fetchJsonWithTimeout() {{ return request(); }}
function fetchRawJsonWithTimeout() {{ return request(); }}
function marketTrendPayload(value) {{ return value; }}
function renderMarketTrendPanel(value) {{ return '<trend>'+String(value.marker||'')+'</trend>'; }}
function renderMarketStyleWindowComparison() {{ return ''; }}
function renderMarketStyleDimensions() {{ return ''; }}
function escHtml(value) {{ return String(value == null ? '' : value); }}
function card() {{ return ''; }}
function fmt(value) {{ return String(value == null ? '' : value); }}
function pct(value) {{ return String(value == null ? '' : value); }}
{loader}
function resolveRun(offset, day, marker) {{
  requests[offset].resolve({{error:'style unavailable', analysis_date:day, style_dimensions:{{}}, theme_analysis:{{}}}});
  requests[offset+1].resolve({{marker:marker, indices:[]}});
  requests[offset+2].resolve({{style_dimensions:{{}}}});
  requests[offset+3].resolve({{style_dimensions:{{}}}});
}}
(async function() {{
  const oldRun = loadSentimentPage('2026-09-04', container);
  picker.value = '2026-09-05';
  const newRun = loadSentimentPage('2026-09-05', container);
  resolveRun(4, '2026-09-05', 'new');
  await newRun;
  const newHtml = container.innerHTML;
  assert.strictEqual(window._marketTrendData.marker, 'new');
  assert.match(newHtml, /2026-09-05/);
  resolveRun(0, '2026-09-04', 'old');
  await oldRun;
  assert.strictEqual(window._marketTrendData.marker, 'new');
  assert.strictEqual(container.innerHTML, newHtml);
  process.stdout.write(JSON.stringify({{status:'PASS'}}));
}})().catch(function(error) {{ console.error(error); process.exit(1); }});
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


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
