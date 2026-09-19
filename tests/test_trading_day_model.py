"""Exercise presentation truth boundaries with the real API response shapes."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "server/static/js/trading-day-model.js"


def node(body):
    executable = shutil.which("node")
    if not executable:
        pytest.skip("Node.js unavailable")
    result = subprocess.run(
        [executable, "-e", "const assert=require('node:assert/strict'); const w=require(" + json.dumps(str(SCRIPT)) + ");\n" + FIXTURE + body],
        capture_output=True, text=True, encoding="utf-8",
    )
    assert result.returncode == 0, result.stdout + result.stderr


FIXTURE = r"""
const ready=value=>({status:'ready',value});
const date='2026-09-18';
const clock={today:date,server_time:date+' 10:30:00',active_trade_date:date,is_trade_day:true};
const afterClose={...clock,server_time:date+' 18:00:00'};
const nextDay={...clock,today:'2026-09-19',server_time:'2026-09-19 18:00:00',is_trade_day:false};
const status=(m,key)=>m.sourceStatus.find(s=>s.key===key);
function state(){return {
 forecast:ready({requested_date:date,session_date:date,source_trade_date:'2026-09-17',
  cutoff_at:date+' 09:08:00',generated_at:date+' 09:08:02',stage:'PREMARKET_0908',run_uid:'forecast-a',
  decision_scope:'RESEARCH_DISPLAY_ONLY',actionable_output_allowed:false,
  themes:[{theme_key:'theme-a',theme_name:'真实主题',evidence:['公告线索'],stock_candidates:[
   {stock_code:'600001',stock_name:'观察一',reason:'原始盘前理由',trigger:'保持承接',invalidation:'失去承接',candidate_score:99}]}]}),
 auction:ready({data:{status:'COMPLETED',execution_session_date:date,session_date:date,
  decision_date:'2026-09-17',data_date:'2026-09-17',source_run_uid:'canonical-b',evidence_mode:'POINT_IN_TIME_REPLAY',
  cutoff_at:date+' 09:25:59',summary:{candidate_count:2,reviewed_count:2},assessments:[
   {stock_code:'600001',stock_name:'观察一',gate_status:'CONFIRMED',quote_at:date+' 09:25:00',gap_pct:4,reasons:['其他批次确认'],trigger:'另一批次触发'},
   {stock_code:'600002',stock_name:'观察二',gate_status:'CONFIRMED',quote_at:date+' 09:25:00',gap_pct:0,reasons:['独立观察']}
  ]}}),
 market:ready({trade_date:date,requested_date:date,data_time:date+' 10:29:55',freshness_status:'realtime',
  is_realtime:true,total_count:100,up_count:0,down_count:100,total_amount:250000000}),
 holdings:ready({trade_date:date,knowledge_cutoff:date+' 10:29:55',historical_read_only:false,
  position_scope:'WATCHLIST_CURRENT_HOLDINGS',data:[{stock_code:'600003',short_name:'持仓',shares:100,
   trade_date:date,knowledge_cutoff:date+' 10:29:55',exit_intent:'SELL',action:'退出观察',reason:'风险变化',
   price_trade_date:date,same_session_price:true,latest_price:10,quote_observed_at:date+' 10:29:50',
   sellable_shares:0,t1_blocked:true,sell_plan:{label:'T+1待处理'},emergency_exit:{label:'核实可卖',price:9},next_session_plan:'明日处理'}]}),
 review:ready({date:date,requested_date:date,fallback:false,data:[{review_date:date,publish_status:'ready',
  generated_at:date+' 17:00:00',data_cutoff_at:date+' 16:00:00',compact_review:'收盘才知道的正文',
  factor_validation_json:JSON.stringify({selection_review:{compact_review:'选股结果'}})}]}),
 journal:ready({trade_date:date,revision:0,plans:[],review:{text:''}})
};}
"""


def test_missing_zero_amount_units_and_real_breadth_denominator():
    node(r"""
for(const v of [null,undefined,'',false,true,' ',[],{},Infinity,'NaN']) assert.equal(w.number(v),null);
let s=state(),m=w.model(s,date,'live',clock);
assert.equal(m.facts[0].value,'0.0%');assert.equal(m.facts[1].value,'100');assert.equal(m.facts[2].value,'3 亿');
assert.equal(m.themes[1].stocks[0].changePct,0);assert.equal(m.holdings[0].sellableShares,0);
s.market.value.total_amount=0;assert.equal(w.model(s,date,'live',clock).facts[2].value,'0 亿');
for(const v of [null,'',false,-100]){s.market.value.total_amount=v;assert.equal(w.model(s,date,'live',clock).facts[2].value,'—');}
for(const patch of [{total_count:0},{total_count:99},{up_count:null},{down_count:false},{up_count:0.5}]){
 s=state();Object.assign(s.market.value,patch);m=w.model(s,date,'live',clock);assert.notEqual(status(m,'market').status,'可读');
}
""")


def test_phase_views_do_not_use_later_stage_evidence():
    node(r"""
const s=state();
let m=w.model(s,date,'pre',afterClose);
assert.equal(m.themes.length,1);assert.equal(m.themes[0].stocks[0].changePct,null);assert.deepEqual(m.holdings,[]);
assert.equal(m.review.text,'');assert.notEqual(status(m,'auction').status,'可读');assert.notEqual(status(m,'market').status,'可读');
m=w.model(s,date,'live',afterClose);assert.equal(m.review.text,'');assert.equal(m.review.selection,'');
m=w.model(s,date,'post',afterClose);assert.equal(m.review.text,'收盘才知道的正文');assert.equal(m.review.selection,'选股结果');
""")


def test_independent_auction_cannot_upgrade_forecast_stock_or_conditions():
    node(r"""
const s=state(),m=w.model(s,date,'live',clock),t=m.themes[0],r=t.stocks[0];
assert.equal(r.sourceRunUid,'forecast-a');assert.equal(r.status,'待核验');assert.equal(r.reason,'原始盘前理由');
assert.equal(r.trigger,'保持承接');assert.equal(r.invalidation,'失去承接');assert.equal(r.changePct,null);
assert.match(t.extraEvidence[0],/canonical-b/);assert.match(t.extraEvidence[0],/不改变盘前主线/);
assert.equal(m.themes[1].stocks[0].sourceRunUid,'canonical-b');assert.equal(m.themes[1].stocks[0].status,'等待开盘确认');
assert.equal(m.themes[1].stocks.length,1);assert.equal(r.confidence,undefined);assert.equal(r.buyAllowed,undefined);
""")


def test_forecast_requires_exact_frozen_day_batch_and_publication_time():
    node(r"""
for(const patch of [{fallback:true},{session_date:'2026-09-17'},{requested_date:'2026-09-19'},
 {stage:'POSTMARKET'},{source_trade_date:date},{run_uid:''},{cutoff_at:date+' 10:00:00'},
 {generated_at:date+' 18:00:00'},{generated_at:''},{generated_at:date+' 09:07:00'}]){
 const s=state();Object.assign(s.forecast.value,patch);assert.equal(w.model(s,date,'pre',afterClose).themes.length,0,JSON.stringify(patch));
}
for(const patch of [{run_uid:'other'},{source_run_uid:'other'},{quote_at:date+' 09:10:00'},
 {trade_date:'2026-09-19'},{stock_code:'000000'}]){
 const s=state();Object.assign(s.forecast.value.themes[0].stock_candidates[0],patch);
 assert.equal(w.model(s,date,'pre',clock).themes[0].stocks.length,0,JSON.stringify(patch));
}
assert.equal(w.model(state(),date,'pre',{...clock,server_time:date+' 09:08:01'}).themes.length,0);
""")


def test_auction_checks_date_batch_completeness_and_unknown_gate():
    node(r"""
for(const patch of [{session_date:'2026-09-17'},{execution_session_date:'2026-09-19'},
 {source_run_uid:''},{status:'DATA_BLOCKED'},{decision_date:date},{data_date:date},
 {cutoff_at:date+' 09:30:00'},{evidence_mode:'UNVERIFIED'},
 {summary:{candidate_count:3,reviewed_count:2}},{status:'VALID_EMPTY'}]){
 const s=state();Object.assign(s.auction.value.data,patch);const m=w.model(s,date,'live',clock);
 assert.notEqual(status(m,'auction').status,'可读',JSON.stringify(patch));assert.equal(m.themes[0].extraEvidence.length,0);
}
const s=state();s.auction.value.data.assessments[1].gate_status='NEW_UNKNOWN_GATE';
assert.equal(w.model(s,date,'live',clock).themes[1].stocks[0].status,'待核验');
s.auction.value.data.assessments[1].quote_at=date+' 09:26:00';
assert.equal(w.model(s,date,'live',clock).themes.length,1);
""")


def test_history_does_not_receive_current_market_positions_or_new_plans():
    node(r"""
const s=state();let m=w.model(s,date,'live',nextDay);
assert.equal(m.historical,true);assert.equal(m.canPlan,false);assert.deepEqual(m.holdings,[]);
assert.notEqual(status(m,'market').status,'可读');assert.equal(m.themes.length,2);
s.market.value.trade_date='2026-09-19';s.market.value.data_time='2026-09-19 10:00:00';
m=w.model(s,date,'post',nextDay);assert.notEqual(status(m,'market').status,'可读');assert.deepEqual(m.holdings,[]);
""")


def test_clock_and_future_dates_cannot_authorize_or_display_future_evidence():
    node(r"""
for(const broken of [{},{...clock,server_time:'invalid'},{...clock,today:'2026-09-17'},
 {...clock,is_trade_day:undefined},{...clock,active_trade_date:''}]){
 const m=w.model(state(),date,'live',broken);assert.equal(m.canPlan,false);assert.deepEqual(m.themes,[]);assert.deepEqual(m.holdings,[]);
}
const futureClock={...clock,today:'2026-09-17',server_time:'2026-09-17 18:00:00',active_trade_date:'2026-09-17'};
const m=w.model(state(),date,'post',futureClock);assert.equal(m.future,true);assert.deepEqual(m.themes,[]);assert.equal(m.review.text,'');
assert.equal(w.model(state(),date,'live',clock).canPlan,true);
assert.equal(w.model(state(),date,'live',{...clock,is_trade_day:false}).canPlan,false);
assert.equal(w.stamp('2026-09-18T01:08:00Z'),'2026-09-18 09:08:00');
assert.equal(w.stamp('2026-02-30 09:08:00'),'');assert.equal(w.day('2026-02-30'),'');
""")


def test_market_close_date_only_is_supported_without_early_or_stale_market_claims():
    node(r"""
const s=state();s.market.value.data_time=date;s.market.value.freshness_status='close';
assert.equal(status(w.model(s,date,'post',afterClose),'market').status,'可读');
assert.notEqual(status(w.model(s,date,'post',clock),'market').status,'可读');
s.market.value.data_time=date+' 10:29:55';s.market.value.freshness_status='stale';s.market.value.is_realtime=true;
assert.notEqual(status(w.model(s,date,'live',clock),'market').status,'可读');
s.market.value.freshness_status='close';assert.notEqual(status(w.model(s,date,'post',afterClose),'market').status,'可读');
s.market.value.freshness_status='realtime';s.market.value.data_time=date+' 10:30:01';
assert.notEqual(status(w.model(s,date,'live',clock),'market').status,'可读');
""")


def test_holding_timestamps_quotes_and_t1_limits_are_preserved():
    node(r"""
let s=state(),m=w.model(s,date,'live',clock),h=m.holdings[0];
assert.equal(h.t1Blocked,true);assert.equal(h.sellableShares,0);assert.equal(h.sellPlan,'T+1待处理');assert.equal(h.latestPrice,10);
for(const patch of [{same_session_price:false},{price_trade_date:'2026-09-17'},{quote_observed_at:'bad'},
 {quote_observed_at:date+' 10:30:01'}]){
 s=state();Object.assign(s.holdings.value.data[0],patch);assert.equal(w.model(s,date,'live',clock).holdings[0].latestPrice,null);
}
for(const cutoff of ['bad','2026-09-17 10:00:00',date+' 10:30:01']){
 s=state();s.holdings.value.data[0].knowledge_cutoff=cutoff;assert.deepEqual(w.model(s,date,'live',clock).holdings,[]);
}
s=state();s.holdings.value.knowledge_cutoff=date+' 10:30:01';assert.deepEqual(w.model(s,date,'live',clock).holdings,[]);
""")


def test_review_late_publication_is_labelled_by_actual_time_without_leaking_to_earlier_views():
    node(r"""
const s=state(),r=s.review.value.data[0];
r.generated_at='2026-09-19 10:00:00';r.data_cutoff_at='2026-09-19 09:00:00';
let m=w.model(s,date,'post',nextDay);assert.equal(m.review.text,'收盘才知道的正文');
assert.equal(status(m,'review').asOf,'2026-09-19 10:00:00');
assert.equal(w.model(s,date,'post',afterClose).review.text,'');
assert.equal(w.model(s,date,'pre',nextDay).review.text,'');assert.equal(w.model(s,date,'live',nextDay).review.text,'');
for(const patch of [{review_date:'2026-09-17'},{publish_status:'blocked'},
 {data_cutoff_at:''},{data_cutoff_at:'2026-09-19 11:00:00'}, {generated_at:'bad'}]){
 const copy=state();Object.assign(copy.review.value.data[0],patch);assert.equal(w.model(copy,date,'post',nextDay).review.text,'');
}
s.review.value.fallback=true;assert.equal(w.model(s,date,'post',nextDay).review.text,'');
""")


def test_source_failures_are_not_empty_successes():
    node(r"""
const s=state();for(const key of Object.keys(s))s[key]={status:'error',value:s[key].value};
const m=w.model(s,date,'post',afterClose);assert.deepEqual(m.themes,[]);assert.deepEqual(m.holdings,[]);assert.equal(m.review.text,'');
assert.ok(m.sourceStatus.every(x=>x.status==='读取失败'));
assert.ok(m.facts.every(x=>x.value==='—'||x.value==='待核验'));
const j=state();j.journal.value={};assert.notEqual(status(w.model(j,date,'live',clock),'journal').status,'可读');
""")
