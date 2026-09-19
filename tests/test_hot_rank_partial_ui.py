import json
import re
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")


def _section(start, end):
    return SCRIPT.split(start, 1)[1].split(end, 1)[0]


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_hot_rank_partial_sources_failures_and_refresh_results():
    helpers = "function hotRankEmptyState" + _section(
        "function hotRankEmptyState", "    function renderHotRankTable"
    ) + "function hotRankRequestContext" + _section(
        "function hotRankRequestContext", "    function marketTrendPayload"
    )
    summary = "function fusedSourceSummary" + _section(
        "function fusedSourceSummary", "    function tableHeadHtml"
    )
    loader = "function (d, c) {" + _section(
        "        fused: function (d, c) {", "        /* ── 板块分析"
    ).rstrip().rstrip(",")
    harness = r"""
const assert = require('assert');
global.window = {_subViewState:{fused:'fused'}};
let activeDate = '2026-09-07';
let MARKET_CLOCK = {is_intraday:true,ui_trade_date:activeDate};
const requests = [], responses = [];
const body = {innerHTML:'', insertAdjacentHTML(where, html) { this.innerHTML = html + this.innerHTML; }};
let interval, intervalMs, clockNext = null;
function setInterval(callback, ms) { interval = callback; intervalMs = ms; return 1; }
function clearInterval() {}
function activeTabId() { return 'fused'; }
function currentDateValue() { return activeDate; }
function isTradingTime() { return true; }
function refreshMarketClockSilently() { if (clockNext) { MARKET_CLOCK = clockNext; clockNext = null; } return Promise.resolve(MARKET_CLOCK); }
function syncDateFromResponse() {}
function escHtml(value) { return String(value).replace(/</g, '&lt;'); }
function renderFusedData(target, res) { target.innerHTML = '<table>' + res.data[0].stock_code + '</table>'; }
function card(label, value) { return label + ':' + value + ';'; }
function renderHotRankTable(target, id, cols, rows, render, size, stats) { target.innerHTML = stats + '<table>' + rows[0].stock_code + '</table>'; }
function apiGet(path) {
  requests.push(path);
  const value = responses.shift();
  return value instanceof Error ? Promise.reject(value) : Promise.resolve(value);
}
function prepareSubViewContainer() { return {body, state:window._subViewState, activeId:window._subViewState.fused}; }
""" + helpers + summary + "\nconst loader = " + loader + r""";
(async function() {
  const valid = {status:'OK', partial:true, date:activeDate, live:true,
    data:[{stock_code:'000001'}], source_counts:{east:100,ths:0}, errors:{ths:'timeout'}};
  const source = fusedSourceSummary(valid);
  assert.match(source, /东财人气榜/);
  assert.doesNotMatch(source, /同花顺热股/);
  assert.match(source, /部分来源暂不可用/);
  responses.push(valid);
  const request = loader(activeDate, {}, {force:true});
  assert.strictEqual(typeof request.then, 'function');
  await request;
  assert.match(body.innerHTML, /000001/);
  assert.strictEqual(requests[0], '/fused-live?top=100');
  assert.doesNotMatch(requests[0], /fresh=/, 'manual refresh must not bypass the acquisition cache');
  assert.strictEqual(intervalMs, 60000);

  responses.push({status:'DATA_UNAVAILABLE',message:'东财连接失败',data:[]},
    {status:'DATA_UNAVAILABLE',message:'历史查询失败',data:[]});
  const failed = await loader(activeDate, {}, {force:true});
  assert.match(failed.loadError, /东财连接失败.*历史查询失败/);
  assert.strictEqual(failed.retained, true);
  assert.match(body.innerHTML, /上次成功数据/);
  assert.match(body.innerHTML, /000001/);
  assert.doesNotMatch(body.innerHTML, /暂无数据/);

  responses.push(valid);
  interval();
  await new Promise(resolve => setImmediate(resolve));
  assert.doesNotMatch(requests[requests.length - 1], /fresh=1/);

  activeDate = '2026-09-04';
  responses.push({status:'DATA_UNAVAILABLE',message:'所选日期查询失败',data:[]});
  const differentDate = await loader(activeDate, {});
  assert.strictEqual(differentDate.retained, false);
  assert.doesNotMatch(body.innerHTML, /000001/);
  assert.match(body.innerHTML, /所选日期查询失败/);

  activeDate = '2026-09-07';
  window._subViewState.fused = 'east';
  responses.push({status:'DATA_UNAVAILABLE',error:'来源不可用<script>',data:[]});
  const differentView = await loader(activeDate, {});
  assert.strictEqual(differentView.retained, false);
  assert.doesNotMatch(body.innerHTML, /000001|<script>/);
  assert.match(body.innerHTML, /来源不可用&lt;script>/);

  // A dated fallback is labelled without changing the user's chosen day.
  responses.push({date:'2026-09-04',fallback:true,data:[{stock_code:'000002',change_pct:null},
    {stock_code:'000003',change_pct:0},{stock_code:'000004',change_pct:1}]});
  const fallback = await loader(activeDate, {});
  assert.strictEqual(fallback.loaded, true);
  assert.strictEqual(activeDate, '2026-09-07');
  assert.match(body.innerHTML, /所选 2026-09-07 · 最近可用 2026-09-04/);
  assert.match(body.innerHTML, /上涨:1;下跌:0;平盘:1;涨跌未知:1;/);

  responses.push({date:'2026-09-08',fallback:true,data:[{stock_code:'000099'}]});
  const future = await loader(activeDate, {});
  assert.match(future.loadError, /日期与所选日期不一致/);
  assert.doesNotMatch(body.innerHTML, /000099/);
  assert.match(body.innerHTML, /000002/);

  responses.push({data:[{stock_code:'000088'}]});
  const undated = await loader(activeDate, {});
  assert.match(undated.loadError, /未提供数据日期/);
  assert.doesNotMatch(body.innerHTML, /000088/);

  window._subViewState.fused = 'sina';
  responses.push({status:'DATA_UNAVAILABLE',error:'未返回可验证的关注度字段',data:[]});
  const disabled = await loader(activeDate, {});
  assert.strictEqual(disabled.retained, false);
  assert.match(body.innerHTML, /未返回可验证的关注度字段/);
  assert.doesNotMatch(body.innerHTML, /暂无数据/);

  const saved = fusedSourceSummary({date:activeDate,source_evidence:{east:{rows:100},ths:{rows:100}}});
  assert.match(saved, /东财人气榜.*同花顺热股/);
  assert.doesNotMatch(saved, /雪球|新浪/);
  assert.doesNotMatch(fusedSourceSummary({source_label:'<script>'}), /<script>/);

  // A page opened before trading must evaluate the freshly read clock.
  window._subViewState.fused = 'fused';
  MARKET_CLOCK = {is_intraday:false,ui_trade_date:activeDate};
  clockNext = {is_intraday:true,ui_trade_date:activeDate};
  const beforeOpen = requests.length;
  responses.push(valid);
  interval();
  await new Promise(resolve => setImmediate(resolve));
  assert.strictEqual(requests.length,beforeOpen+1);
  assert.strictEqual(requests[requests.length-1],'/fused-live?top=100');
  process.stdout.write(JSON.stringify({status:'PASS'}));
})().catch(error => { console.error(error); process.exit(1); });
"""
    result = subprocess.run(
        [shutil.which("node"), "-"], input=harness,
        text=True, encoding="utf-8", capture_output=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


def test_manual_fused_refresh_and_asset_cache_version():
    assert "refreshLoadTab(id, id === 'fused' ? {force:true} : undefined)" in SCRIPT
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    assert re.search(r'app\.js\?v=[^"\s>]+', index)
