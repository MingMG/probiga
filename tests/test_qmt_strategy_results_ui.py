"""Verify the daily-results page preserves execution and data truth."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "server/static/js/qmt-strategy-results.js"


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
const date='2026-10-08';
const selected={stock_code:'600001.SH',stock_name:'实测股票',score:0,score_scale:100,reasons:['原始选中理由'],
 conditions:[{key:'flow',label:'资金流',status:'PASS',value:0,required:'>=0'}],feature_values:{volume_ratio:0,missing:null}};
function receipt(){return {status:'AVAILABLE',trade_date:date,dates:[{trade_date:date,run_count:1}],runs:[{run_uid:'run-a',trade_date:date,status:'COMPLETED',run_mode:'DAILY'}],
 schedule:{owner:'WINDOWS_QMT',cron_time:'22:50',timezone:'Asia/Shanghai',enabled:true,status:'REGISTERED'},
 catalog:{strategies:[{strategy_key:'main_wave',name:'主升浪趋势'}],combinations:[],excluded:[{strategy_key:'intraday_surprise',name:'盘中超预期',reason:'用户排除'}]},
 latest:{run_uid:'run-a',trade_date:date,status:'COMPLETED',run_mode:'DAILY',origin:'WINDOWS_DAILY',
 input:{prepared_at:date+' 22:50:00',decision_at:date+' 15:00:00',v2:{status:'READY',flow_date:date,proofs:{market_latest_coverage_ratio:0.96}},v3:{status:'DATA_BLOCKED',reasons:['财务数据不完整']}},
 input_hash:'input-proof',result_hash:'result-proof',edge_build_sha:'release-proof',
 result:{trade_date:date,formula_contract:'formula-v1',simulation_only:true,real_order_allowed:false,
 strategy_rows:[{strategy_key:'main_wave',name:'主升浪趋势',version:'V2',status:'COMPLETED',selected:[selected],candidate_count:1,selected_count:1},
 {strategy_key:'short_term',name:'短线多因子',status:'COMPLETED_EMPTY',selected:[],candidate_count:0,selected_count:0},
 {strategy_key:'event_drift',name:'事件漂移',status:'DATA_BLOCKED',selected:[],selected_count:0,blocked_reasons:['公告窗口缺失']}],combination_rows:[]}}};}
"""


def test_both_navigation_layouts_route_to_the_read_only_qmt_page():
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    app = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    assert 'id="tab-qmt-strategy-results" class="tab-content"' in index
    assert "/static/js/qmt-strategy-results.js?v=" in index
    assert "/static/css/qmt-strategy-results.css?v=" in index
    assert app.count("id:'qmt-strategy-results',icon:'◫',label:'QMT每日模拟'") == 2
    assert "'qmt-strategy-results': function (d, c)" in app
    assert "window.QmtStrategyResults.stop()" in app


def test_verified_empty_missing_and_failure_are_distinct():
    node(r"""
const data=receipt(), m=w.model(data,date),html=w.render(data,date,{});
assert.equal(m.completed,2);assert.equal(m.empty,1);assert.equal(m.blocked,1);assert.equal(m.unique,1);
assert.match(html,/本策略已执行完成，本批次没有满足全部条件的股票/);
assert.match(html,/公告窗口缺失/);assert.match(html,/原始选中理由/);
data.latest.result.strategy_rows[0].status='FAILED';
const failed=w.render(data,date,{});assert.match(failed,/执行失败/);assert.doesNotMatch(failed,/原始选中理由/);
""")


def test_all_blocked_is_unjudged_but_verified_completed_empty_is_a_real_zero():
    node(r"""
const data=receipt();
data.latest.result.strategy_rows=Array.from({length:10},(_,i)=>({strategy_key:'s'+i,name:'独立策略'+i,status:'DATA_BLOCKED',selected:[],selected_count:0,candidate_count:0,blocked_reasons:['输入缺失']}));
data.latest.result.combination_rows=Array.from({length:4},(_,i)=>({strategy_key:'c'+i,name:'组合'+i,status:'DATA_BLOCKED',selected:[],selected_count:0,candidate_count:0,blocked_reasons:['成员数据缺失']}));
assert.equal(w.model(data,date).unique,0);assert.equal(w.model(data,date).completed,0);
let html=w.render(data,date,{});
assert.match(html,/<span>选中股票去重<\/span><strong>未判定<\/strong><small>尚无可核验的入选结果<\/small>/);
assert.doesNotMatch(html,/0次策略选中记录|0\.00%/);
for(const row of [...data.latest.result.strategy_rows,...data.latest.result.combination_rows])row.status='COMPLETED_EMPTY';
assert.equal(w.model(data,date).completed,14);html=w.render(data,date,{});
assert.match(html,/<span>选中股票去重<\/span><strong>0<\/strong><small>0次策略选中记录<\/small>/);
assert.doesNotMatch(html,/尚无可核验的入选结果|0\.00%/);
""")


def test_failed_scheduler_without_uploaded_result_is_visible_and_not_a_selection():
    node(r"""
const data=receipt();data.latest=null;data.runs=[];
data.schedule={...data.schedule,last_run_status:'failed',last_run_at:date+' 22:50:00',last_run_duration:0,
 last_run_summary:'最近一次执行失败，尚未产生完整结果'};
const html=w.render(data,date,{});
assert.match(html,/尚无执行结果/);assert.match(html,/最近调度记录/);assert.match(html,/执行失败/);
assert.match(html,/2026-10-08 22:50:00/);assert.match(html,/用时 0 秒/);assert.doesNotMatch(html,/原始选中理由/);
data.latest={run_uid:'pending',trade_date:date,status:'AWAITING_EXECUTION',result:null};
assert.match(w.render(data,date,{}),/输入已保存，等待模拟执行结果/);
data.schedule.last_run_summary='<script>untrusted</script>';
assert.doesNotMatch(w.render(data,date,{}),/<script>/);
""")


def test_input_signing_and_actual_execution_times_are_not_conflated():
    node(r"""
const d=receipt();d.latest.issued_at='2026-10-08T21:00:00Z';d.latest.completed_at='2026-10-08T22:59:00+08:00';
d.latest.execution={started_at:'2026-10-08T22:50:00+08:00',finished_at:'2026-10-08T22:55:00+08:00',bridge_identity:{strategy_build_sha:'native-proof'}};
let html=w.render(d,date,{});
assert.match(html,/<dt>输入签发时间<\/dt><dd><code>2026-10-08T21:00:00Z/);
assert.match(html,/<dt>执行开始<\/dt><dd><code>2026-10-08T22:50:00\+08:00/);
assert.match(html,/<dt>执行完成<\/dt><dd><code>2026-10-08T22:55:00\+08:00/);
assert.match(html,/<dt>输入准备时间<\/dt><dd><code>2026-10-08 22:50:00/);
assert.match(html,/<dt>Windows应用版本<\/dt><dd><code>release-proof/);
assert.match(html,/<dt>QMT原生模型版本<\/dt><dd><code>native-proof/);
delete d.latest.execution.started_at;delete d.latest.execution.finished_at;
html=w.render(d,date,{});assert.match(html,/<dt>执行开始<\/dt><dd><code>未提供/);
assert.match(html,/<dt>执行完成<\/dt><dd><code>2026-10-08T22:59:00\+08:00/);
delete d.latest.completed_at;html=w.render(d,date,{});assert.match(html,/<dt>执行完成<\/dt><dd><code>未提供/);
assert.doesNotMatch(html,/<dt>执行(?:开始|完成)<\/dt><dd><code>2026-10-08T21:00:00Z/);
""")


def test_preparation_queue_running_failure_and_issued_are_visible_without_fictitious_picks():
    node(r"""
const job={request_id:'a'.repeat(32),trade_date:date,run_mode:'DAILY',edge_build_sha:'release-proof',status:'QUEUED',snapshot_id:null,
 attempt_count:0,error_code:null,created_at:'2026-10-08T22:50:00Z',updated_at:'2026-10-08T22:51:00Z',heartbeat_at:null,reason:'本请求等待事实准备容量',lease_stale:false,
 simulation_only:true,real_order_allowed:false,automatic_real_order_submission:false,real_order_authority:false};
const data={status:'PENDING',trade_date:date,dates:[{trade_date:date,run_count:0}],runs:[],latest:null,preparation_jobs:[job],reason:'本请求等待事实准备容量'};
let html=w.render(data,date,{});assert.match(html,/策略事实输入准备/);assert.match(html,/策略输入排队等待准备/);assert.match(html,/输入准备排队中/);
assert.match(html,/本请求等待事实准备容量/);assert.match(html,/固定交易日/);assert.match(html,/输入准备记录/);
for(const [stage,title,reason] of [['PREPARING','正在准备策略事实输入','正在读取冻结事实'],['FAILED','策略输入准备失败','策略事实输入准备失败；本请求失败记录保留'],['ISSUED','输入准备完成，等待执行结果','策略事实输入已发出']]){
 job.status=stage;job.reason=reason;data.reason=reason;job.error_code=stage==='FAILED'?'INPUT_PREPARATION_FAILED':null;
 html=w.render(data,date,{});assert.match(html,new RegExp(title));assert.match(html,new RegExp(reason));
 assert.doesNotMatch(html,/data-qr-stock=|0\.00%|已完成 · 无符合股票|<th[^>]*>模拟入选股票|执行完成<\/dt>/);
 assert.match(html,/准备输入不等于策略已经执行/);assert.match(html,/最近状态更新/);
}
job.status='PREPARING';job.lease_stale=true;assert.match(w.render(data,date,{}),/准备租约已过期/);
job.status='FAILED';job.reason='<script>unsafe reason</script>';assert.doesNotMatch(w.render(data,date,{}),/<script>/);
""")


def test_preparation_jobs_cannot_impersonate_other_dates_or_grant_order_authority():
    node(r"""
const job={request_id:'a'.repeat(32),trade_date:'2026-10-07',run_mode:'DAILY',status:'PREPARING',reason:'other-date-private-details',
 simulation_only:true,real_order_allowed:false,automatic_real_order_submission:false,real_order_authority:false};
const data={status:'PENDING',latest:null,preparation_jobs:[job]};
assert.doesNotMatch(w.render(data,date,{}),/other-date-private-details|策略事实输入准备<\/h3>/);
job.trade_date=date;job.real_order_allowed=true;
const html=w.render(data,date,{});assert.match(html,/本准备记录的模拟范围或请求身份尚待核验/);
assert.doesNotMatch(html,/正在准备策略事实输入|other-date-private-details|0\.00%|data-qr-stock=/);
""")


def test_pending_preparation_refreshes_read_only_and_stops_on_failure_or_navigation():
    node(r"""
global.document={activeElement:null};let timerId=0,timers=[],cleared=[];
global.setTimeout=(callback,ms)=>{assert.equal(ms,15000);timers.push(callback);return ++timerId};
global.clearTimeout=id=>cleared.push(id);
const c={innerHTML:'',querySelector(){return null}},job={request_id:'a'.repeat(32),trade_date:date,run_mode:'DAILY',status:'PREPARING',
 reason:'准备读取事实',simulation_only:true,real_order_allowed:false,automatic_real_order_submission:false,real_order_authority:false};
let calls=0,data={status:'PENDING',trade_date:date,latest:null,preparation_jobs:[job]};
const options={request:url=>{assert.equal(url,'/api/strategy-center/qmt-results?trade_date='+date);calls++;return Promise.resolve(data)}};
(async()=>{
 await w.load(date,c,options);assert.equal(calls,1);assert.equal(timers.length,1);assert.match(c.innerHTML,/正在准备策略事实输入/);
 assert.doesNotMatch(c.innerHTML,/结果读取失败|data-qr-stock=|0\.00%/);
 job.status='FAILED';job.reason='事实准备失败';await timers[0]();assert.equal(calls,2);assert.equal(timers.length,1);assert.match(c.innerHTML,/策略输入准备失败/);
 job.status='PREPARING';await w.load(date,c,options);assert.equal(timers.length,2);w.stop();assert.deepEqual(cleared,[2]);
 const old=c.innerHTML;await timers[1]();assert.equal(calls,3);assert.equal(c.innerHTML,old);
})().catch(e=>{console.error(e);process.exitCode=1});
""")


def test_missing_and_invalid_values_never_become_zero_performance():
    node(r"""
for(const v of [null,undefined,'',false,true,' ',[],{},Infinity,'NaN']) assert.equal(w.number(v),null);
assert.equal(w.number(0),0);
const html=w.render(receipt(),date,{});assert.match(html,/资金流：0/);assert.match(html,/未取得/);
assert.match(html,/等待后续真实行情/);assert.doesNotMatch(html,/0\.00%/);
assert.doesNotMatch(w.performanceHTML({status:'PENDING_FORWARD_DATA',return_pct:0},{}),/0\.00%/);
assert.match(w.performanceHTML({status:'AVAILABLE',return_pct:0,entry_price:10,last_price:10},{}),/0\.00%/);
""")


def test_stale_date_wrong_simulation_scope_and_inconsistent_counts_are_not_results():
    node(r"""
for(const change of [d=>d.latest.trade_date='2026-10-07',d=>d.latest.result.trade_date='2026-10-07',
 d=>d.latest.result.simulation_only=false,d=>d.latest.result.real_order_allowed=true]){
 const d=receipt();change(d);assert.equal(w.model(d,date).readable,false);assert.doesNotMatch(w.render(d,date,{}),/原始选中理由/);
}
for(const change of [r=>r.selected_count=2,r=>r.selected_count=null,r=>r.status='COMPLETED_EMPTY',r=>r.status='DATA_BLOCKED']){
 const d=receipt();change(d.latest.result.strategy_rows[0]);assert.doesNotMatch(w.render(d,date,{}),/原始选中理由/);
}
""")


def test_external_stock_names_reasons_features_and_hashes_are_escaped():
    node(r"""
const d=receipt();d.latest.result.strategy_rows[0].selected=[{...selected,stock_name:'<img src=x onerror=alert(1)>',
 reasons:['<script>attack</script>'],conditions:[{label:'<svg>',value:'<iframe>',required:'<a>',status:'PASS'}],feature_values:{'<img>':'<script>'}}];
d.latest.input_hash='<script>hash</script>';const html=w.render(d,date,{});
assert.doesNotMatch(html,/<img|<script|<svg|<iframe/);assert.match(html,/&lt;img/);assert.match(html,/data-qr-stock="600001"/);
""")


def test_performance_requires_exact_run_date_and_complete_real_price_coverage():
    node(r"""
const performance={run_uid:'run-a',trade_date:date,status:'AVAILABLE',entities:[{strategy_key:'main_wave',name:'主升浪趋势',status:'AVAILABLE',selected_count:1,verified_count:1,
 avg_return_pct:7.89,horizons:[{sessions:5,status:'AVAILABLE',verified_count:1,avg_return_pct:4.56}],picks:[{stock_code:'600001.SH',status:'AVAILABLE',return_pct:7.89}]}]};
assert.match(w.render(receipt(),date,{performance}),/\+7\.89%/);
for(const patch of [{run_uid:'other'},{trade_date:'2026-10-07'}])assert.doesNotMatch(w.render(receipt(),date,{performance:{...performance,...patch}}),/\+7\.89%/);
const partial=JSON.parse(JSON.stringify(performance));partial.entities[0].verified_count=0;partial.entities[0].picks=[];partial.entities[0].horizons[0].verified_count=0;
const html=w.render(receipt(),date,{performance:partial});assert.doesNotMatch(html,/\+7\.89%|\+4\.56%/);assert.match(html,/等待行情/);
""")


def test_v3_shadow_observation_shows_original_ranking_forecast_and_unmet_conditions():
    node(r"""
const d=receipt(), p={...selected,selection_kind:'ORIGINAL_V3_SHADOW_PORTFOLIO_OBSERVATION',status:'UNCALIBRATED',
 score:0.48,score_scale:1,rank_no:7,selection_score:0.48,ranking_basis:'UNCALIBRATED_RAW_SCORE_RESEARCH_ONLY',
 expected_return_net_pct:null,model_version:'v3-original',dataset_hash:null,sample_count:0,confidence:0,
 reasons:['原观察组合名额排序入选'],conditions:[{key:'shadow_rank',label:'原策略影子观察组合排序',status:'PASS',value:7,required:20},
 {key:'forecast_status',label:'原策略预测状态（观察不代表买入）',status:'BLOCK',value:'UNCALIBRATED',required:'VALIDATED_POSITIVE 才有校准正期望证据'}]};
d.latest.result.strategy_rows=[{strategy_key:'quality_momentum',name:'质量动量',family:'V3',status:'COMPLETED',selected:[p],selected_count:1}];
const html=w.render(d,date,{});
assert.match(html,/V3模拟组合观察入选（非买入指令）/);assert.match(html,/执行完成不等于满足买入条件/);
assert.match(html,/观察排名 7/);assert.match(html,/排序分 0\.48/);assert.match(html,/未校准原始评分排序（仅研究）/);
assert.match(html,/原观察组合名额排序入选/);assert.match(html,/原策略影子观察组合排序：7 · 要求 20/);
assert.match(html,/预测状态：UNCALIBRATED/);assert.match(html,/class="block">未满足/);
assert.match(html,/未取得校准预测/);assert.match(html,/模型校准净期望（预测，非已实现收益）/);
assert.match(html,/预测模型版本/);assert.match(html,/v3-original/);assert.match(html,/校准样本数/);
assert.match(html,/等待后续真实行情/);assert.match(html,/后续价格观察（非成交收益）/);
assert.doesNotMatch(html,/信号：UNCALIBRATED|0\.00%|买入按钮|确认买点/);
p.ranking_basis='CALIBRATED_EXPECTED_RETURN_NET_PCT';p.expected_return_net_pct=3.21;p.selection_score=3.21;
const calibrated=w.render(d,date,{});assert.match(calibrated,/校准净期望排序（预测口径）/);assert.match(calibrated,/\+3\.21%/);
assert.match(calibrated,/预测，非已实现收益/);assert.match(calibrated,/等待后续真实行情/);
""")


def test_v3_empty_and_combination_members_do_not_imply_failed_buy_signals():
    node(r"""
const d=receipt();d.latest.result.strategy_rows=[{strategy_key:'quality_momentum',name:'质量动量',family:'V3',status:'COMPLETED_EMPTY',selected:[],selected_count:0}];
let html=w.render(d,date,{});assert.match(html,/没有进入原模拟观察组合名额的股票/);assert.doesNotMatch(html,/没有满足全部条件的股票/);
d.latest.result.combination_rows=[{strategy_key:'combo',name:'模拟组合',status:'COMPLETED',selected_count:1,members:[{name:'质量动量',strategy_key:'quality_momentum',weight:1}],
 selected:[{...selected,selection_kind:'FROZEN_MEMBER_UNION',reasons:['原观察成员的保存理由']}]}];
html=w.render(d,date,{});assert.match(html,/成员策略合并观察（非买入指令）/);assert.match(html,/可能包含V3观察成员/);assert.match(html,/原观察成员的保存理由/);
""")


def test_observed_price_changes_are_not_presented_as_filled_trade_returns():
    node(r"""
const performance={run_uid:'run-a',trade_date:date,status:'AVAILABLE',is_execution_return:false,label:'税费前价格观察',
 entities:[{strategy_key:'main_wave',name:'主升浪趋势',status:'AVAILABLE',selected_count:1,verified_count:1,avg_return_pct:7.89,
 picks:[{stock_code:'600001.SH',status:'AVAILABLE',return_pct:7.89,entry_price:10,last_price:10.789}]}]};
const html=w.render(receipt(),date,{performance});assert.match(html,/\+7\.89%/);assert.match(html,/价格变化 · 非成交收益/);
assert.match(html,/没有成交、持仓或费用证据，不代表实际交易收益/);
assert.doesNotMatch(w.render(receipt(),date,{performance:{...performance,is_execution_return:true}}),/\+7\.89%/);
""")


def test_empty_page_shows_real_schedule_and_explicit_latest_date_link():
    node(r"""
const d=receipt();d.latest=null;d.status='PENDING';d.dates=[{trade_date:'2026-10-07',run_count:1}];d.runs=[];
const html=w.render(d,date,{});assert.match(html,/尚无执行结果/);assert.match(html,/22:50 · 北京时间/);
assert.match(html,/data-qr-date="2026-10-07"/);assert.match(html,/主升浪趋势/);assert.match(html,/盘中超预期（用户排除）/);
assert.doesNotMatch(html,/原始选中理由|已完成 · 无符合股票/);
""")


def test_late_request_cannot_overwrite_a_new_date_or_a_stopped_page():
    node(r"""
global.document={activeElement:null};const c={innerHTML:'',querySelector(){return null}}, pending=[];
const options={request:url=>new Promise(resolve=>pending.push({url,resolve}))};
(async()=>{
 const first=w.load('2026-10-07',c,options),second=w.load(date,c,options);
 pending[1].resolve(receipt());await second;assert.match(c.innerHTML,/原始选中理由/);
 pending[0].resolve({status:'PENDING',latest:null});await first;assert.match(c.innerHTML,/原始选中理由/);
 const third=w.load(date,c,options);w.stop();const old=c.innerHTML;
 pending[2].resolve(receipt());await third;assert.equal(c.innerHTML,old);
})().catch(e=>{console.error(e);process.exitCode=1});
""")


def test_performance_request_errors_preserve_the_executed_picks():
    node(r"""
global.document={activeElement:null};const c={innerHTML:'',querySelector(){return null}},calls=[];
const d=receipt();d.latest.performance={endpoint:'/api/strategy-center/qmt-results/performance?run_uid=run-a'};
(async()=>{
 await w.load(date,c,{request:url=>{calls.push(url);return url.includes('/performance?')?Promise.reject(new Error('行情读取失败')):Promise.resolve(d)}});
 assert.equal(calls.length,2);assert.match(c.innerHTML,/原始选中理由/);assert.match(c.innerHTML,/后续表现读取失败：行情读取失败/);
 w.stop();
})().catch(e=>{console.error(e);process.exitCode=1});
""")
