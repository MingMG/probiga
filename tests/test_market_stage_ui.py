"""Exercise the market stage renderer and its real request lifecycle."""

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "server/static/js/market-stage.js"


def node(body):
    executable = shutil.which("node")
    if not executable:
        pytest.skip("Node.js unavailable")
    script = "const assert=require('node:assert/strict'); const page=require(" + json.dumps(str(SCRIPT)) + ");\n" + body
    result = subprocess.run([executable, "-e", script], capture_output=True, text=True, encoding="utf-8", timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_current_distinguishes_periods_missing_evidence_and_an_actual_zero():
    node(r"""
const current = {
    status:'READY', source:'<img src=x onerror=alert(1)>',
    snapshot:{status:'READY', state:'TECHNICAL_REBOUND', trade_date:'2026-11-05',
        model_version:'test-model', as_of:'2026-11-05 20:00:00',
        periods:{daily:{state:'STABILIZATION_ATTEMPT', end_date:'2026-11-05'},
            weekly:{state:'DOWNTREND', end_date:'2026-10-30'}, monthly:{observation_count:0}},
        breadth:{advancing_pct:0}, confirmation_conditions:['周线确认'], invalidation_conditions:['跌破支撑']}
};
const html = page.renderCurrent(current, '2026-11-06');
for (const expected of ['技术性反弹','止跌尝试','下跌趋势','证据不足','周线确认','跌破支撑',
    '样本期数 0','0.0%','请求日期为 2026-11-06','test-model','&lt;img']) assert.ok(html.includes(expected), expected);
assert.doesNotMatch(html, /<img|NaN|undefined/);
assert.match(page.renderCurrent({status:'BLOCKED',reason:'行情尚未认证'}), /行情尚未认证/);
""")


def test_replay_uses_horizon_and_preserves_right_censored_outcomes():
    node(r"""
const data = {source:'index', model_version:'model', replay:{
    start_date:'2026-10-09', end_date:'2026-11-05', outcome_as_of:'2026-11-05 20:00:00',
    summary:{5:[{state:'STABILIZATION_ATTEMPT', assessment_count:1, matured_count:1, mean_return_pct:0,
        new_low_count:0, new_low_evaluated_count:1}]}, rows:[{trade_date:'2026-10-09',
        decision_known_at:'2026-10-09 20:00:00', assessment:{state:'STABILIZATION_ATTEMPT', evidence:['低点止跌']},
        outcomes:{5:{return_pct:0, max_drawdown_pct:-3, new_20_session_low:false}, 20:null}}]}};
const matured = page.renderReplay(data, 5);
assert.match(matured, /0\.00%/);
assert.match(matured, /-3\.00%/);
assert.match(matured, /低点止跌/);
assert.match(matured, /2026-10-09 20:00:00/);
assert.match(page.renderReplay(data, 20), /待观察或证据不足/);
assert.doesNotMatch(page.renderReplay(data, 20), /0\.00%/);
""")


def test_load_queries_selected_day_and_discards_a_stale_response():
    node(r"""
function makeNode() {
    return {innerHTML:'', listeners:{}, value:'20', addEventListener(type, callback) {this.listeners[type]=callback;}};
}
const selectors = ['[data-ms-current]','[data-ms-history]','[data-ms-replay]','[data-ms-horizon]','[data-ms-latest]'];
const container = {set innerHTML(_value) {this.nodes=Object.fromEntries(selectors.map(selector=>[selector,makeNode()]));},
    querySelector(selector) {return this.nodes[selector];}, contains(node) {return Object.values(this.nodes).includes(node);}};
const calls = [];
const request = url => new Promise(resolve=>calls.push({url,resolve}));
(async function () {
    const old = page.load('2026-11-05', container, request);
    const current = page.load('2026-11-06', container, request);
    assert.equal(calls.length, 2);
    assert.equal(calls[0].url, '/api/market-stage/current?trade_date=2026-11-05');
    calls[1].resolve({reason:'新请求结果'}); await current;
    calls[0].resolve({reason:'旧请求结果'}); await old;
    assert.match(container.nodes['[data-ms-current]'].innerHTML, /新请求结果/);
    const action = container.nodes['[data-ms-replay]'].listeners.click();
    assert.equal(calls[2].url, '/api/market-stage/replay?trade_date=2026-11-06');
    calls[2].resolve({reason:'回放结果'}); await action;
    assert.match(container.nodes['[data-ms-history]'].innerHTML, /回放结果/);
})().catch(error=>{console.error(error);process.exitCode=1;});
""")


def test_market_stage_is_mounted_in_product_navigation():
    html = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    app = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    assert 'id="tab-market-stage"' in html
    assert "/static/js/market-stage.js" in html
    assert "tradingModuleDateMode(d) === 'DEFAULT' ? '' : d" in app
    assert "window.MarketStage.load(target, c" in app
    assert app.count("{id:'market-stage', icon:") == 1
