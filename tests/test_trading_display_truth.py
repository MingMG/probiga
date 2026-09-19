"""Execute the shipped renderers: missing evidence is never a zero or live advice."""
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node.js unavailable")


def run_js(script: str) -> None:
    result = subprocess.run([NODE, "-e", script], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    assert result.returncode == 0, result.stdout + result.stderr


def section(script: str, start: str, end: str) -> str:
    return script[script.index(start):script.index(end, script.index(start))]


def source(name: str) -> str:
    return (ROOT / "server/static/js" / name).read_text(encoding="utf-8")


def test_unknown_metrics_and_research_scores_remain_distinct_from_probability():
    js = source("trading-v3.js")
    functions = section(js, "  function metricNumber", "  function unwrap")
    functions += section(js, "  function hypothesisEstimate", "  function actionName")
    functions += section(js, "  function hasCalibratedExpectation", "  function strategyText")
    run_js("const assert=require('assert');\n" + functions + """
for(const value of [null,undefined,'',' ',false,true,[],{},'bad',Infinity]){
  assert.strictEqual(num(value), '—');
  assert.strictEqual(pct(value), '—');
  assert.strictEqual(fractionPct(value), '—');
  assert.strictEqual(ratio(value), '—');
  assert.strictEqual(money(value), '—');
}
assert.strictEqual(pct(0), '0.00%');
assert.strictEqual(fractionPct('0.65',1), '65.0%');
assert.strictEqual(firstNumber([null,'',false,0,100]),0);
assert.strictEqual(hypothesisEstimate(null,'OOS_CALIBRATED'),'—');
assert.strictEqual(hypothesisEstimate(0.65,'OOS_CALIBRATED'),'65.0% · 历史校准');
for(const kind of ['STRUCTURED_RESEARCH_PRIOR','PAPER_FORWARD_PRIOR','REGIME_MIXTURE','UNKNOWN']){
  assert.strictEqual(hypothesisEstimate(0.65,kind),'65.0 / 100 · 研究评分');
}
assert.strictEqual(hypothesisEstimate(1.1,'OOS_CALIBRATED'),'—');
assert.strictEqual(hasCalibratedExpectation({forecast_status:'VALIDATED_POSITIVE',sample_count:100,expected_return_net_pct:null}),false);
assert.strictEqual(hasCalibratedExpectation({forecast_status:'VALIDATED_POSITIVE',sample_count:100,expected_return_net_pct:0}),true);
""")


def test_cockpit_never_promotes_missing_or_historical_holdings_to_today_actions():
    js = source("trading-v3.js")
    functions = section(js, "  function metricNumber", "  function unwrap")
    functions += section(js, "  function isDiscoveryTarget", "  function hasCalibratedExpectation")
    functions += section(js, "  function renderChrome", "  function renderOverview")
    run_js("const assert=require('assert');\n" + functions + """
const nodes={};
function el(id){return nodes[id]||(nodes[id]={textContent:'',className:'',classList:{add(){},remove(){}}});}
function strategy(value){return value;}
function truthState(){return {code:'READY'};}
function baseline(){return {overview:{run:{run_uid:'verified',decision_scope:'PAPER',portfolio:{targets:[{stock_code:'000001',new_buy_eligible:true}]}}},readiness:{paper_ready:true,paper_authority_ready:true,execution_ready:true},paperLedger:{summary:{}},context:{historical_read_only:false},watchlistStrategy:{historical_read_only:false,summary:{holding_count:1,sell_count:0,reduce_count:0,wait_data_count:0},rows:[{stock_code:'000002',exit_intent:'HOLD'}]},errors:{},pendingKeys:{},loadedKeys:{watchlistStrategy:true},requestedDate:'2026-09-18'};}
let state=baseline();
renderChrome();
assert.strictEqual(el('actualHoldingCount').textContent,'1 只');
assert.strictEqual(el('allowedBuyCount').textContent,'1 只');
state.errors.watchlistStrategy='timeout';
state.watchlistStrategy={};
renderChrome();
assert.strictEqual(el('actualHoldingCount').textContent,'—');
assert.strictEqual(el('urgentActionCount').textContent,'—');
assert.strictEqual(el('allowedBuyCount').textContent,'待核验');
assert.strictEqual(el('heroTitle').textContent,'持仓风险尚无法核验');
assert(!el('urgentActionSummary').textContent.includes('无强制卖出'));
state=baseline();state.watchlistStrategy.summary.wait_data_count=1;
renderChrome();
assert.strictEqual(el('allowedBuyCount').textContent,'待核验');
assert(el('todayFirstAction').textContent.includes('缺失的证据'));
assert(!el('todayFirstActionReason').textContent.includes('前继续持有'));
state=baseline();state.context.historical_read_only=true;state.watchlistStrategy.historical_read_only=true;
state.watchlistStrategy.summary.sell_count=1;state.watchlistStrategy.rows[0].exit_intent='SELL';
renderChrome();
assert(el('heroTitle').textContent.startsWith('历史证据复核'));
assert.strictEqual(el('allowedBuyCount').textContent,'历史只读');
assert.strictEqual(el('urgentActionCount').textContent,'—');
assert(el('todayFirstActionReason').textContent.includes('不还原当日持仓'));
assert(!el('heroTitle').textContent.includes('立即退出'));
""")


def test_security_links_preserve_system_context_and_reject_invalid_codes():
    js = source("trading-v3.js")
    functions = section(js, "  function esc(v)", "  function metricNumber")
    functions += section(js, "  function security(code", "  function empty(cols")
    run_js("const assert=require('assert');\n" + functions + """
let messages=[],assigned=[];
function parentMessage(type,payload){messages.push({type,payload});}
const window={parent:{},location:{assign(url){assigned.push(url);}}};
assert(security('000001.SZ','平安银行').includes('/?tab=workbench&stock_code=000001'));
requestStockChart('000001.SZ','平安银行');
assert.strictEqual(messages[0].type,'probiga-open-stock-detail');
assert.strictEqual(messages[0].payload.stock_code,'000001');
requestStockChart('javascript:evil','bad');
assert.strictEqual(messages.length,1);
assert(!security('javascript:evil','bad').includes('<a'));
window.parent=window;requestStockChart('600000.SH','浦发银行');
assert.deepStrictEqual(assigned,['/?tab=workbench&stock_code=600000']);
""")


def test_hypothesis_filter_responses_cannot_overwrite_newer_selections():
    js = source("trading-v3.js")
    functions = section(js, "  function reloadHypotheses", "  function renderResearchObservationPool")
    run_js("const assert=require('assert');\n" + functions + """
const nodes={hypothesisDate:{value:'2026-09-17'},hypothesisState:{value:''},hypothesisSearch:{value:''},hypothesisDetail:{innerHTML:''},hypothesisDetailTitle:{textContent:''}};
function el(id){return nodes[id];}function syncFilters(){}function renderHypotheses(){}function renderTruthContext(){}function unwrap(v){return v.data;}function errorText(e){return e.message;}
let state={loadSeq:1,errors:{},pendingKeys:{}},calls=[];
function api3(path){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b});calls.push({path,resolve,reject});return promise;}
async function flush(){await Promise.resolve();await Promise.resolve();}
(async()=>{
 reloadHypotheses();nodes.hypothesisDate.value='2026-09-18';reloadHypotheses();
 calls[1].resolve({data:[{scope_code:'000002'}]});await flush();
 calls[0].resolve({data:[{scope_code:'000001'}]});await flush();
 assert.strictEqual(state.hypotheses[0].scope_code,'000002');
 reloadHypotheses();calls[2].reject(new Error('offline'));await flush();
 assert.deepStrictEqual(state.hypotheses,[]);assert.strictEqual(state.errors.hypotheses,'offline');
 assert.strictEqual(state.pendingKeys.hypotheses,undefined);
})().catch(e=>{console.error(e);process.exit(1)});
""")


def test_simulation_api_failure_is_explicit_and_missing_account_values_are_unknown():
    js = source("trading-v2.js")
    functions = section(js, "  function n(v)", "  function scheduleText")
    run_js("const assert=require('assert');\n" + functions + """
function esc(v){return String(v);}
function fetch(){return Promise.reject(new Error('offline'));}
(async()=>{
 const result=await safeApi('/operations/tomorrow');
 assert.strictEqual(result.status,'unavailable');assert.strictEqual(result.request_error,'offline');
 assert.strictEqual(unwrap(result),null);
 for(const value of [null,undefined,'',false,[],{}]){assert.strictEqual(n(value),null);assert.strictEqual(money(value),'¥—');assert.strictEqual(percent(value),'—');}
 assert.strictEqual(money(0),'¥0.00');assert.strictEqual(yesNo(null),'未确认');assert.strictEqual(yesNo(false),'否');
})().catch(e=>{console.error(e);process.exit(1)});
""")


def test_current_simulation_holdings_preserve_zero_and_fail_closed_on_missing_ledger():
    js = source("trading-v3.js")
    functions = section(js, "  function esc(v)", "  function unwrap")
    functions += section(js, "  function fact(label", "  function priceRange")
    functions += section(js, "  function ledgerSourceName", "  function closePositionDetail")
    functions += section(js, "  function renderPositions", "  function renderOrders")
    run_js("const assert=require('assert');\n" + functions + """
const nodes={};function el(id){return nodes[id]||(nodes[id]={innerHTML:''});}
function status(v){return v;}function sourceName(v){return v;}function security(code){return code;}function empty(c,text){return text;}function emptyFor(k,c,text){return text;}
const state={errors:{},pendingKeys:{},loadedKeys:{paperLedger:true},forecasts:[],targets:[],paperLedger:{summary:{position_count:1,position_lot_count:1,today_sold_count:0}},positions:[{stock_code:'000001',state:'HOLDING',remaining_quantity:0,quantity:100,sellable_quantity:0,cost_price:0,average_cost:18,ledger_source:'V2_CANONICAL'}]};
renderPositions();
assert(el('positionRows').innerHTML.includes('<td>0</td><td>0</td><td>¥0.00</td>'));
assert(!el('positionRows').innerHTML.includes('¥18.00'));
state.errors.paperLedger='offline';renderPositions();
assert(el('positionSummary').innerHTML.includes('不能据此判断为空仓'));
assert(el('positionRows').innerHTML.includes('当前模拟持仓不可用'));
assert(!el('positionRows').innerHTML.includes('000001'));
""")


def test_holding_response_requires_matching_record_count_before_displaying_empty():
    js = source("trading-v3.js")
    start = js.index("  function holdingStrategyPayload")
    functions = js[start:js.index("\n", start)]
    run_js("const assert=require('assert');\n" + functions + """
(async()=>{
 for(const bad of [{},{status:'ok'},{status:'ok',data:[],summary:{holding_count:2}},{status:'ok',data:[],summary:{holding_count:'0'}}]){
  await assert.rejects(holdingStrategyPayload(Promise.resolve(bad)));
 }
 const empty=await holdingStrategyPayload(Promise.resolve({status:'ok',data:[],summary:{holding_count:0}}));
 assert.deepStrictEqual(empty.rows,[]);
 assert.strictEqual(empty.summary.holding_count,0);
})().catch(e=>{console.error(e);process.exit(1)});
""")
