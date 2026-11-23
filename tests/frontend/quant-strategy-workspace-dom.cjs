const assert = require('node:assert/strict');
const {parseHTML} = require('linkedom');
const {window} = parseHTML('<html><body><main id="workspace"></main></body></html>');
global.window=window;global.document=window.document;
const pin='a'.repeat(64), other='b'.repeat(64), day='2026-11-20';
global.location=new URL('http://unit.test/?experiment_id='+pin);
global.sessionStorage=new Map();sessionStorage.setItem=sessionStorage.set.bind(sessionStorage);sessionStorage.getItem=sessionStorage.get.bind(sessionStorage);
global.history={state:null,replaceState(){}};
Object.defineProperty(document,'activeElement',{value:null,writable:true});
window.HTMLElement.prototype.focus=function(){document.activeElement=this;};
window.HTMLElement.prototype.scrollIntoView=function(){};
const timers=new Map();let timerId=0;
global.setTimeout=(callback,delay)=>{const id=++timerId;timers.set(id,{callback,delay});return id;};
global.clearTimeout=id=>timers.delete(id);
require('../../server/static/js/quant-strategy-workspace.js');
const mount=document.getElementById('workspace');
const workspace={trade_date:day,strategies:[],strategy_count:0,governance:{},tasks:[]};
const resultId='native:'+pin+':rule';
const original={trade_date:day,experiment_id:pin,
    experiments:[{run_id:pin,status:'RUNNING',allocation_policy:'FIXED_SHADOW',requested_at:'2026-11-22 18:05:19'}],
    strategies:[{strategy_key:'trend',strategy_name:'已验证策略',strategy_version:'v1',result_id:resultId,backtest_status:'PARTIAL'}],
    sources:Object.fromEntries(['native_backtests','autonomous_research','historical_backtests','win_rate_history'].map(key=>[key,{status:'COMPLETE',reason:''}])),
    results:[{result_id:resultId,strategy_key:'trend',strategy_name:'已验证策略',strategy_version:'v1',kind:'NATIVE_SHADOW_RETROSPECTIVE',
        status:'PARTIAL',return_scope:'VERIFIED_INPUT_SUBSET',reason:'有效的原结果',start_date:'2026-09-07',end_date:'2026-10-20',
        phase:'REPLAY',last_progress_at:'2026-11-24 04:40:24',completed_units:100,total_units:112,
        win_rate_history:[{date:'2026-09-08',win_rate_pct:50,winning_trades:1,completed_trades:2}],
        evaluation_sessions:['2026-09-07'],execution_end_date:'2026-10-20',experiment_status:'BLOCKED',
        source_family:'immutable_v3_sleeve',allocation_policy:'FIXED_SHADOW'}],leaderboards:[],method:'真实读取的测试快照'};
let payload=structuredClone(original), head='version-one', bodyCalls=0, http200Calls=0, bodyWait=null;
const request=async url=>{
    let value;
    if(url.includes('strategy-performance-version'))value={version:head};
    else if(url.includes('/strategy-performance/'+encodeURIComponent(resultId)))value={summary:payload.results[0],trades:[],offset:0,limit:50,trade_count:0};
    else if(url.includes('/strategy-performance?')){bodyCalls++;value=payload;if(bodyWait)await bodyWait;}
    else if(url.includes('/strategies?'))value=workspace;
    else throw Error('unexpected request '+url);
    // A successful HTTP envelope must not turn a failed domain read into data.
    const response=new Response(JSON.stringify(value),{status:200});
    assert.equal(response.status,200);http200Calls++;
    return response.json();
};
const flush=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){for(let i=0;i<6;i++)await flush();}
async function poll(){
    const timer=timers.get(mount._strategyWorkspace.performanceTimer);
    assert.ok(timer,'performance refresh was not scheduled');
    assert.ok(timer.delay>0&&timer.delay<=20000,'retry must use the existing 20-second cadence');
    timers.delete(mount._strategyWorkspace.performanceTimer);
    await timer.callback();await settle();
}
(async()=>{
    window.QuantStrategyWorkspace.load(day,mount,request,()=>{},()=>{},()=>true);
    await settle();const state=mount._strategyWorkspace;
    assert.equal(bodyCalls,1);assert.equal(state.performanceHeadVersion,'version-one');
    await mount.onclick({target:mount.querySelector('[data-qsw-result]')});
    const legend=mount.querySelector('[data-qsw-series="trend"]');legend.checked=false;
    legend.dispatchEvent(new window.Event('change',{bubbles:true}));
    const detail=mount.querySelector('[data-qsw-key="backtest-archive"]');detail.open=true;
    const form=mount.querySelector('[data-qsw-experiment-form]');
    const draft=form.querySelector('[data-qsw-experiment-field="execution_end_date"]');draft.value='2026-10-19';
    draft.dispatchEvent(new window.Event('input',{bubbles:true}));draft.focus();
    mount.querySelector('.qsw-result-scroll').scrollLeft=137;
    const kept=state.performance,keptVersion=state.performanceVersion,keptDetail=state.resultDetail;
    function assertPreserved(){
        assert.equal(state.performance,kept,'an invalid HTTP 200 snapshot replaced verified data');
        assert.equal(state.performanceVersion,keptVersion);assert.equal(state.performanceHeadVersion,'version-one');
        assert.equal(state.resultDetail,keptDetail);assert.equal(state.experimentId,pin);
        assert.deepEqual(state.chartHidden,['trend']);
        assert.equal(state.experimentDraft.values.execution_end_date,'2026-10-19');
        assert.equal(mount.querySelector('[data-qsw-key="backtest-archive"]').open,true);
        assert.equal(mount.querySelector('.qsw-result-scroll').scrollLeft,137);
        assert.equal(document.activeElement.dataset.qswExperimentField,'execution_end_date');
        assert.equal(document.activeElement.value,'2026-10-19');
        assert.match(mount.textContent,/2026-11-24 04:40:24/);
        assert.match(mount.textContent,/有效的原结果/);
        assert.ok(state.performanceError);assert.match(mount.textContent,/保留上次结果/);
    }
    head='version-two';payload={...structuredClone(original),experiments:[],results:[],strategies:[],
        sources:{...original.sources,native_backtests:{status:'FAILED',reason:'原生回测审计读取失败'}}};
    await poll();assert.equal(bodyCalls,2);assertPreserved();
    assert.match(mount.textContent,/原生回测审计读取失败/);
    // Even the last successful head must retry while a body read remains invalid.
    head='version-one';payload={...structuredClone(original),experiments:[]};
    await poll();assert.equal(bodyCalls,3);assertPreserved();
    assert.match(state.performanceError,/所选实验/);
    payload={...structuredClone(original),experiment_id:other};
    await poll();assert.equal(bodyCalls,4);assertPreserved();
    for(const key of ['historical_backtests','autonomous_research','win_rate_history']){
        payload=structuredClone(original);payload.sources[key]={status:'FAILED',reason:key+'暂时读取失败'};
        await poll();assertPreserved();assert.match(state.performanceError,/暂时读取失败/);
    }
    payload=structuredClone(original);
    payload.experiments[0].status='FAILED';payload.results[0].status='FAILED';
    payload.results[0].reason='已读回真实失败回执';payload.results[0].last_progress_at='2026-11-24 04:55:00';
    await poll();assert.equal(state.performanceError,'');assert.notEqual(state.performance,kept);
    assert.equal(state.performanceHeadVersion,'version-one');
    assert.match(mount.textContent,/已读回真实失败回执/);assert.match(mount.textContent,/2026-11-24 04:55:00/);
    assert.equal(state.experimentId,pin);assert.deepEqual(state.chartHidden,['trend']);
    assert.equal(state.experimentDraft.values.execution_end_date,'2026-10-19');
    assert.equal(mount.querySelector('[data-qsw-key="backtest-archive"]').open,true);
    assert.equal(mount.querySelector('.qsw-result-scroll').scrollLeft,137);
    assert.equal(document.activeElement.dataset.qswExperimentField,'execution_end_date');
    assert.equal(document.activeElement.value,'2026-10-19');
    const recoveredCalls=bodyCalls;await poll();assert.equal(bodyCalls,recoveredCalls,'a healthy unchanged head should skip the body');
    const recovered=state.performance;let releaseBody;
    bodyWait=new Promise(resolve=>{releaseBody=resolve;});head='version-three';
    const inFlight=poll();await settle();assert.equal(bodyCalls,recoveredCalls+1);
    const selector=mount.querySelector('[data-qsw-experiment]');
    Object.defineProperty(selector,'value',{value:other});
    selector.dispatchEvent(new window.Event('change',{bubbles:true}));
    assert.equal(state.experimentId,other);releaseBody();await inFlight;bodyWait=null;
    assert.equal(state.performance,recovered,'a late response replaced the newly selected experiment');
    assert.equal(state.experimentId,other);assert.equal(state.performanceHeadVersion,'');
    assert.equal(state.performanceError,'');assert.ok(timers.has(state.performanceTimer));
    assert.ok(http200Calls>bodyCalls);
    mount.remove();
    // An empty, successfully read dataset remains a legitimate first observation.
    const emptyMount=document.createElement('main');document.body.appendChild(emptyMount);
    global.location=new URL('http://unit.test/');sessionStorage.clear();
    payload={...structuredClone(original),experiment_id:null,experiments:[],results:[],strategies:[]};
    window.QuantStrategyWorkspace.load(day,emptyMount,request,()=>{},()=>{},()=>true);await settle();
    assert.equal(emptyMount._strategyWorkspace.performanceError,'');
    assert.deepEqual(emptyMount._strategyWorkspace.performance.results,[]);
    assert.match(emptyMount.textContent,/暂无日期、盈亏和笔数/);
    console.log('PASS: HTTP 200 domain failures preserve results, time and interactions; unchanged-version retries recover');
})().catch(error=>{console.error(error);process.exitCode=1;});
