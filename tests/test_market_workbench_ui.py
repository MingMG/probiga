"""Execute the homepage model and request lifecycle against adverse source states."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'server/static/js/market-workbench.js'


def node(body):
    if not shutil.which('node'):
        pytest.skip('Node.js unavailable')
    result = subprocess.run(['node', '-e', "const assert=require('node:assert/strict'); const w=require(" + json.dumps(str(SCRIPT)) + ");\n" + body], capture_output=True, text=True, encoding='utf-8')
    assert result.returncode == 0, result.stdout + result.stderr


FIXTURE = r"""
const ready=value=>({status:'ready',value});
const date='2026-09-18';
const clock={active_trade_date:date,is_intraday:false};
const state=()=>({
 market:ready({trade_date:date,total_count:100,up_count:0,down_count:100,total_amount:0,
  history:{trade_dates:['2026-09-16','2026-09-17',date],heat:[650,null,0],amount:[100,null,0]},
  top_industries:[{name:'old',trade_date:'2026-09-17',change:3},{name:'current',trade_date:date,change:0}]}),
 context:ready({run_uid:'a',decision_session_date:date,data_date:date,decision_status:'EMPTY'}),
 pool:ready({run_uid:'a',decision_session_date:date,trade_date:date,pool_readable:true,
  run_status:'COMPLETED',decision_integrity_verified:true,pool_status:'EMPTY',items:[],
  summary:{stock_count:0,strategy_candidate_count:0}})
});
"""


def test_missing_and_zero_are_distinct_and_breadth_uses_its_real_denominator():
    node(FIXTURE + r"""
for(const v of [null,undefined,'',false,true,' ',[],{},Infinity,'NaN'])assert.equal(w.number(v),null);
assert.equal(w.number(0),0);
let s=state(), m=w.model(s,date,clock);
assert.equal(m.breadth,0);assert.equal(m.candidateCount,0);
assert.equal(m.sectors.length,1);assert.equal(m.sectors[0].name,'current');
assert.equal(m.points[1].breadth,null);assert.equal(m.points[2].breadth,0);
assert.match(w.render(s,date,clock),/0\.0%/);
s.market.value.up_count=200;
assert.equal(w.model(s,date,clock).breadth,null);
s.market={status:'error'};s.pool={status:'error'};
m=w.model(s,date,clock);assert.equal(m.breadth,null);assert.equal(m.candidateCount,null);
assert.doesNotMatch(w.render(s,date,clock),/本批次没有新增研究候选/);
""")


def test_candidates_require_same_date_batch_and_complete_accounting():
    node(FIXTURE + r"""
for(const patch of [
 {run_uid:'b'}, {decision_session_date:'2026-09-17'}, {is_historical_fallback:true},
 {run_status:'PROCESSING'}, {decision_integrity_verified:false},
 {summary:{stock_count:1,strategy_candidate_count:0}}, {pool_status:'READY'}
]) {const s=state();Object.assign(s.pool.value,patch);assert.equal(w.model(s,date,clock).candidateCount,null,JSON.stringify(patch));}
const s=state();s.context.value.decision_status='BLOCKED';assert.equal(w.model(s,date,clock).candidateCount,null);
""")


def test_future_market_data_and_late_retained_judgments_are_not_historical_evidence():
    node(FIXTURE + r"""
let s=state();s.market.value.trade_date='2026-09-21';s.market.value.total_amount=987654321;
const m=w.model(s,date,clock);assert.equal(m.breadth,null);assert.equal(m.monitor.total_amount,undefined);assert.deepEqual(m.points,[]);
s=state();s.trend=ready({indices:[{index_code:'000300',index_name:'index',data_cutoff:date}],retained_history:[
 {trade_date:'2026-09-16',retained_at:'2026-09-21',indices:[{index_code:'000300',subsequent_change_pct:4}]},
 {trade_date:date,retained_at:date,indices:[{index_code:'000300',subsequent_change_pct:0}]},
 {trade_date:'2026-09-17',retained_at:'2026-09-17',indices:[{index_code:'000300',subsequent_change_pct:0}]}
]});
const observed=w.retainedRows(w.model(s,date,clock));assert.equal(observed.length,1);assert.equal(observed[0].change,0);
""")


def test_external_names_and_reasons_cannot_inject_markup():
    node(FIXTURE + r"""
const s=state();s.market.value.top_industries[1].name='<img src=x onerror=alert(1)>';
s.watch=ready({data:[{stock_code:'600000',short_name:'<script>alert(1)</script>',change_pct:null,shares:0}]});
const html=w.render(s,date,clock);
assert.doesNotMatch(html,/<img|<script/);assert.match(html,/&lt;img/);assert.match(html,/data-mw-stock="600000"/);
""")


def test_watch_quote_dates_stale_state_and_missing_security_identity():
    node(FIXTURE + r"""
const s=state();s.watch=ready({data:[
 {stock_code:'600000',short_name:'stale-name',change_pct:5,quote_trade_date:date,quote_status:'stale'},
 {stock_code:'',short_name:'missing-code',change_pct:0,quote_status:'fresh'}
]});
const html=w.render(s,date,clock);
assert.match(html,/2026-09-18 · 旧报价/);
assert.doesNotMatch(html,/data-mw-stock="000000"/);
assert.doesNotMatch(html,/class="mw-up">\+5\.00%/);
s.market.value.down_count=0;
assert.match(w.model(s,date,clock).headline,/多数个股持平/);
""")


def test_history_requires_coherent_evidence_and_is_kept_read_only():
    node(FIXTURE + r"""
const s=state();s.contextTruth=()=> 'STALE';
Object.assign(s.context.value,{historical_read_only:true,run_status:'COMPLETED',data_status:'READY',decision_integrity_verified:true});
let m=w.model(s,date,{active_trade_date:'2026-09-21'});
assert.equal(m.historical,true);assert.equal(m.candidateCount,0);
m=w.model(s,date,{active_trade_date:date,today:'2026-09-19',phase:'closed'});
assert.equal(m.historical,true);assert.equal(m.candidateCount,0);
s.pool.value.trade_date='2026-09-16';
assert.equal(w.model(s,date,{active_trade_date:'2026-09-21'}).candidateCount,null);
""")


def test_cancel_while_clock_is_loading_does_not_restart_the_old_page():
    node(r"""
global.document={activeElement:null,hidden:false};
let timers=new Map(),next=0,calls=0,resolveClock;
global.setTimeout=fn=>{const id=++next;timers.set(id,fn);return id};global.clearTimeout=id=>timers.delete(id);
const c={innerHTML:'',querySelectorAll(){return []},contains(){return false}};
const options={request:()=>{calls++;return Promise.resolve({})},clock:()=>({is_intraday:true,active_trade_date:'2026-09-18'}),
 refreshClock:()=>new Promise(resolve=>{resolveClock=resolve}),navigate(){},stock(){}};
(async()=>{
 await w.load('2026-09-18',c,options);
 const entry=Array.from(timers.entries())[0];timers.delete(entry[0]);entry[1]();
 await w.load('2026-09-18',c,options);assert.equal(calls,12);
 resolveClock();await new Promise(resolve=>setImmediate(resolve));
 assert.equal(calls,12);assert.equal(timers.size,1);w.stop();assert.equal(timers.size,0);
})().catch(e=>{w.stop();console.error(e);process.exitCode=1});
""")


def test_archive_holdings_keep_unknown_valuation_and_sort_high_risk_first():
    app = (ROOT / 'server/static/js/app.js').read_text(encoding='utf-8')
    functions = app[app.index('        function holdingRiskTag(ph)'):app.index('        function renderEventStrip(rows)')]
    node("function num(v){return Number(v)||0;} function blendedAnalysisRowScore(ph){return ph.score;}\n" + functions + r"""
const rows=buildHoldingGroups([
 {stock_code:'600001',strategy_type:'s',buy_shares:100,buy_price:10,cur_price:null,pnl:null,pnl_rate:null,score:70},
 {stock_code:'600002',strategy_type:'s',buy_shares:100,buy_price:10,cur_price:9,pnl:-100,pnl_rate:-10,score:50}
]);
assert.equal(rows[0].stock_code,'600002');assert.equal(rows[0].risk.tone,'bad');
assert.equal(rows[1].market_value,null);assert.equal(rows[1].pnl,null);assert.equal(rows[1].pnl_rate,null);
assert.equal(rows[1].cur_price,null);
""")


def test_premarket_clock_is_refreshed_before_deciding_whether_to_poll():
    node(r"""
global.document={activeElement:null,hidden:false};
let timer, clock={is_intraday:false,active_trade_date:'2026-09-18'}, calls=0, refreshed=0;
global.setTimeout=(fn)=>{timer=fn;return 1};global.clearTimeout=()=>{};
const c={innerHTML:'',querySelectorAll(){return []},contains(){return false}};
(async()=>{
 await w.load('2026-09-18',c,{request:()=>{calls++;return Promise.resolve({})},clock:()=>clock,
 refreshClock:()=>{refreshed++;clock.is_intraday=true;return Promise.resolve()},navigate(){},stock(){}});
 assert.equal(calls,6);timer();
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(refreshed,1);assert.equal(calls,12);w.stop();
})().catch(e=>{w.stop();console.error(e);process.exitCode=1});
""")


def test_app_clock_loader_composes_the_production_url_once():
    app = (ROOT / 'server/static/js/app.js').read_text(encoding='utf-8')
    wrapper = app[app.index('    function fetchJsonWithTimeout('):app.index('    var silentRefreshDepth')]
    loader = app[app.index('    function loadMarketClock()'):app.index('    function refreshMarketClockSilently(')]
    node("const API_BASE='/api/hot-data';let urls=[],received;\n"
         "function fetchRawJsonWithTimeout(url){urls.push(url);return Promise.resolve({phase_label:'盘中'});}\n"
         "function applyMarketClock(clock){received=clock;}\n" + wrapper + loader +
         "loadMarketClock().then(()=>{assert.deepEqual(urls,['/api/hot-data/market-clock']);assert.equal(received.phase_label,'盘中');}).catch(e=>{console.error(e);process.exitCode=1});")


def test_progressive_load_prevents_previous_date_overwriting_new_page():
    node(r"""
global.document={activeElement:null,hidden:true};
function container(){return {html:'',set innerHTML(v){this.html=v},get innerHTML(){return this.html},querySelectorAll(){return []},contains(){return false}};}
const c=container(), pending=[];
const options={request:url=>new Promise(resolve=>pending.push({url,resolve})),clock:()=>({}),navigate(){},stock(){}};
(async()=>{
 const first=w.load('2026-09-17',c,options);
 const second=w.load('2026-09-18',c,options);
 const newRequests=pending.slice(6);
 newRequests.forEach(p=>p.resolve({}));await second;
 assert.match(c.html,/2026-09-18 \/ A股/);
 const latest=c.html;
 pending.slice(0,6).forEach(p=>p.resolve({trade_date:'2026-09-17',up_count:100,down_count:0,total_count:100}));await first;
 assert.equal(c.html,latest);w.stop();
})().catch(e=>{w.stop();console.error(e);process.exitCode=1});
""")
