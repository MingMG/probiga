"""Exercise rendered evidence and refresh boundaries for the market homepage."""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "server/static/js/market-home.js"


def run_node(body):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable")
    program = "const assert = require('node:assert/strict'); const page = require(" + json.dumps(str(SCRIPT)) + ");\n" + body
    result = subprocess.run([node, "-e", program], capture_output=True, text=True, encoding="utf-8", timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_drawdown_panel_keeps_formula_samples_and_requested_date():
    run_node(r"""
const date = '2026-11-06';
const snapshot = {status:'available',trade_date:date,as_of:date+'T15:00:00+08:00',as_of_basis:'exchange_close',
    close_drawdowns:{count:1,sample_count:9,expected_sample_count:10,formula:'(最高 - 收盘) / 最高 > 7%',
        rows:[{stock_code:'000001',short_name:'回撤样本',high:100,price:92,drawdown_pct:8}]}};
const html = page.render({snapshot}, date, {});
assert.match(html,/大幅回撤/);
assert.match(html,/回撤样本/);
assert.match(html,/-8\.00%/);
assert.match(html,/有效最高价 9只 \/ 行情样本 10只/);
assert.match(html,/最高 &gt; 7%/);
const wrongDate = page.render({snapshot}, '2026-11-09', {});
assert.doesNotMatch(wrongDate,/回撤样本/);
""");


def test_dashboard_distinguishes_current_stale_and_missing_evidence():
    run_node(r"""
const date = '2026-11-06';
const market = {
    status:'available', trade_date:date, data_time:date + ' 10:29:55', timezone:'Asia/Shanghai',
    data_source:'有效行情', freshness_status:'realtime', is_realtime:true,
    up_count:0, flat_count:1, down_count:9, total_count:10, total_amount:100000000,
    heat_percentile:0, heat_percentile_sample_count:19
};
const heatmap = {requested_date:date, trade_date:date,
    concept:{change:{status:'available',rows:[{code:'A',name:'<b>可核验主题</b>',value:-1.25,source:'板块源'}]}}};
const snapshot = {
    status:'partial', trade_date:date, as_of:date, source:'日行情样本', sample_size:10,
    distribution:[{key:'lt_neg_3', label:'跌3%以上', upper:-3, count:6}, {key:'flat', label:'持平', count:1}],
    strong:{up_3pct:0,down_3pct:6},
    indices:[{index_code:'000001.SH', trade_date:date, price:3999.01, change_pct:0, source:'指数日行情'},
        {index_code:'399001.SZ', trade_date:'2026-11-09', price:12345.67, change_pct:1}],
    limit_ladder:{trade_date:date, source:'东财涨停池', scope:'东财收录样本', rows:[{streak:5,count:2}]},
    turnover_coverage:{verified_count:9,sample_size:10},
    liquidity:{turnover_median_pct:2.34,turnover_sample_count:9,volume_ratio5:0.91,volume_sample_count:8},
    rankings:{gainers:[{stock_code:'600001', short_name:'当日股票', trade_date:date, change_pct:0},
        {stock_code:'600002', short_name:'错日排名', trade_date:'2026-11-05', change_pct:20}],
        turnover:[{stock_code:'600003', short_name:'有换手率股票', trade_date:date, turnover_rate:12.34}]}
};
const trend = {source:{table:'指数历史表'}, indices:[{index_code:'000300',data_cutoff:'2026-11-05',
    periods:{daily:{metrics:{close:4600.25,sma20:4500,sma60:4400,sma20_slope_5_pct:0.2}}}}]};
const html = page.render({market,snapshot,trend,heatmap}, date, {server_time:date + ' 10:30:00'});
for (const expected of ['<section class="market-home"', '盘中快照', '延迟 5秒', '行情 2026-11-06 10:29:55',
    '0.0%', '1.00亿', 'P0', '涨跌幅达到 ±3%', '跌3%以上', '分布样本 10只', '所选交易日的异动事件证据待核验',
    '沪深300 · 2026-11-05', '4,600.25', '3,999.01', '指数日行情', '&lt;b&gt;可核验主题&lt;/b&gt;',
    '当日股票', 'data-mh-stock="600001"', '12.34%', '已核验 9/10只', '5板', '东财收录样本',
    'data-mh-tab="market-context"', '换手中位 / 收盘日量比', '2.34% / 0.91倍', '日量比同样本 8只']) assert.ok(html.includes(expected), expected);
assert.doesNotMatch(html, /12,345\.67|未来主题|错日排名|<b>可核验主题|NaN|undefined/);
assert.doesNotMatch(page.render({market,snapshot:{...snapshot,limit_ladder:{...snapshot.limit_ladder,trade_date:'2026-11-05'}}}, date), /5板/);
assert.doesNotMatch(page.render({market}, date), /延迟/);
assert.doesNotMatch(page.render({market}, date, {server_time:date + ' 10:29:54'}), /延迟/);
const later = page.render({market,snapshot,heatmap}, '2026-11-09', {server_time:'2026-11-09 10:30:00'});
assert.match(later, /历史回退/);
assert.match(later, /有效行情/);
assert.doesNotMatch(later, /延迟|跌3%以上|当日股票|可核验主题|2\.34% \/ 0\.91倍/);
""")


def test_board_distribution_survives_failed_aggregate_without_inventing_values():
    run_node(r"""
const date = '2026-11-06';
const snapshot = {status:'partial', trade_date:date, source:'独立分布样本', sample_size:1,
    distribution:[{label:'独立区间', lower:3, count:1}], rankings:{}};
const html = page.render({market:{error:'失败'}, snapshot}, date);
assert.match(html, /独立区间/);
assert.match(html, /独立分布样本/);
assert.match(html, /涨跌家数待核验/);
assert.match(html, /涨跌停证据待核验/);
assert.match(html, /缺少所选交易日可核验的排名/);
assert.doesNotMatch(html, /0\.0%|0\.00亿/);
const invalid = page.render({market:{trade_date:date, up_count:1, flat_count:1, down_count:1, total_count:4}}, date);
assert.match(invalid, /暂无可用涨跌分布/);
assert.doesNotMatch(invalid, /上涨占比 25/);
""")


def test_home_requests_read_only_sources_and_discards_prior_date_response():
    run_node(r"""
global.document = {activeElement:null};
const container = {innerHTML:'', listeners:new Map(), contains:() => false,
    addEventListener(type, listener) {this.listeners.set(type, listener);},
    removeEventListener(type, listener) {if (this.listeners.get(type) === listener) this.listeners.delete(type);}};
const calls = [];
const request = (url, timeout, options) => new Promise(resolve => calls.push({url,timeout,options,resolve}));
(async function () {
    try {
        const old = page.load('2026-11-05', container, {request});
        const current = page.load('2026-11-06', container, {request});
        const paths = ['/api/monitor/board', '/api/monitor/sector-heatmap', '/api/monitor/liquidity', '/api/monitor/events', '/api/hot-data/market-trend'];
        const expected = ['2026-11-05','2026-11-06'].flatMap(date => [
            ...paths.map(path => path + '?date=' + date), '/api/hot-data/daily-review-dates',
            '/api/scheduler/daily-delivery?trade_date=' + date + '&attempt_limit=12'
        ]);
        assert.deepEqual(calls.map(call => call.url), expected);
        assert.ok(calls.every(call => call.timeout === (/market-trend|sector-heatmap|liquidity/.test(call.url) ? 30000 : /monitor\/board/.test(call.url) ? 20000 : 15000) && call.options.cache === 'no-store'));
        assert.equal(container.listeners.size, 3);
        calls.slice(paths.length + 2).forEach(call => call.resolve({status:'partial',trade_date:'2026-11-06',source:'新日期来源',as_of:'2026-11-06T10:30:00+08:00'}));
        await current;
        const listener = container.listeners.get('click');
        container.contains = target => Boolean(target);
        listener({target:{closest:() => ({dataset:{mhHeatMode:'concept:net_inflow'}})}});
        assert.match(container.innerHTML, /aria-pressed="true"[^>]*data-mh-heat-mode="concept:net_inflow"/);
        assert.match(container.innerHTML, /aria-pressed="true"[^>]*data-mh-heat-mode="industry:change"/);
        assert.equal(calls.length, expected.length);
        calls.slice(0,paths.length + 2).forEach(call => call.resolve({status:'partial',trade_date:'2026-11-05',source:'旧日期来源',as_of:'2026-11-05T10:30:00+08:00'}));
        await old;
        assert.match(container.innerHTML, /新日期来源/);
        assert.doesNotMatch(container.innerHTML, /旧日期来源/);
    } finally {
        page.stop();
        assert.equal(container.listeners.size, 0);
    }
})().catch(error => {console.error(error); process.exitCode = 1;});
""")


def test_home_hub_only_claims_accepted_delivery_with_matching_receipt():
    run_node(r"""
const date = '2026-11-06';
const clock = {today:'2026-11-10'};
const attempt = {status:'SUCCESS',stage_name:'qmt_stock_daily_canonical',finished_at:date + ' 15:40:00'};
const overnight = {status:'SUCCESS',stage_name:'target_turnover_snapshot',finished_at:'2026-11-07T01:17:54'};
const finance = {status:'DEGRADED',stage_name:'stock_finance',finished_at:date+' 20:00:00'};
const reviewDates = {dates:[date]};
const delivery = {trade_date:date,status:'PASS',data:{session:{updated_at:date + ' 15:42:00'},attempts:[attempt,overnight,finance]}};
const snapshot = {status:'partial',trade_date:date,as_of:date+' 15:00:00',unavailable_fields:['limit_ladder']};
const html = page.render({reviewDates,delivery,snapshot},date,clock);
for (const text of ['A股大看板','更新时间流','研究日历','信息和数据看板','选股专题','策略跟踪',
    '15:40','股票日线 · 阶段完成','11-07 01:17','换手率快照 · 阶段完成','有复盘记录','data-mh-tab="trading-v3-candidates"']) {
    assert.ok(html.includes(text),text);
}
assert.match(html, /data-mh-date="2026-11-06"[^>]*aria-pressed="true"/);
assert.match(html, /data-mh-date="2026-11-11"[^>]*disabled/);
assert.match(html, /data-mh-tab="datasource"[^>]*>\s*<time[^>]*>20:00<\/time><span>交付<\/span><strong>财务数据 · 部分可用/);
assert.match(html, /data-mh-tab="stock-list"[^>]*>\s*<time[^>]*>15:40<\/time>/);
assert.match(html, /待补：连板梯队/);
assert.match(html, /data-mh-tab="sector" data-mh-view="heat" aria-label="打开行业与板块热度"/);
assert.doesNotMatch(html,/交付已验收|target_turnover_snapshot|qmt_stock_daily_canonical/);
delivery.data.receipt = {schema:'probiga.daily-delivery.v1',status:'PASS',trade_date:date};
assert.match(page.render({reviewDates,delivery},date,clock),/交付已验收/);
assert.doesNotMatch(page.render({reviewDates,delivery},'2026-11-05',clock),/交付已验收|股票日线 · 阶段完成/);
assert.doesNotMatch(page.render({reviewDates,delivery},date,clock,{feed:'复盘'}),/股票日线 · 阶段完成/);
""")


def test_sector_heatmap_uses_exact_signed_samples_and_only_observed_series():
    run_node(r"""
const date = '2026-11-06';
const heatmap = {requested_date:date,trade_date:date,
    concept:{
        change:{status:'available',note:'同日完整来源',rows:[
            {code:'A',name:'正向',value:2.35,source:'可信报价',series:[1,4,2],series_label:'当日分时'},
            {code:'B',name:'负向',value:-1.25,source:'可信报价',series_label:'无数据不画线'},
            {code:'C',name:'持平',value:-0,source:'可信报价',series:[1,null,4]},
            {code:'D',name:'空值主题',value:null}
        ]},
        net_inflow:{status:'available',note:'主力资金口径',rows:[
            {code:'A',name:'资金净流入样本',value:123000000},
            {code:'B',name:'资金净流出样本',value:-25000}
        ]},
        five_day_return:{status:'unavailable',note:'缺少连续窗口',rows:[{code:'C',name:'不可用伪排名',value:10}]}
    },
    industry:{change:{status:'available',rows:[{code:'E',name:'行业行情样本',value:-0.5}]},
        five_day_return:{status:'available',rows:[{code:'E',name:'行业5日样本',value:7.25,series:[1,2,3,5,7.25]}]}}
};
const state = {heatmap};
const quotes = page.render(state,date);
assert.match(quotes, /data-mh-board="concept" data-mh-board-code="A">正向/);
assert.match(quotes, /class="mh-heat-cell mh-up"[^>]*>.*正向.*\+2\.35%/);
assert.match(quotes, /class="mh-heat-cell mh-down"[^>]*>.*负向.*-1\.25%/);
assert.match(quotes, /class="mh-heat-cell mh-neutral"[^>]*>.*持平.*0\.00%/);
assert.match(quotes, /points="0\.00,19\.00 50\.00,1\.00 100\.00,13\.00"/);
assert.equal((quotes.match(/<polyline/g)||[]).length,1);
assert.match(quotes, /可信报价|同日完整来源/);
assert.match(quotes, /当日分时/);
assert.doesNotMatch(quotes, /空值主题|资金净流入样本|不可用伪排名|行业5日样本|无数据不画线|NaN/);
const ranked = page.render(state,date,null,{concept:'net_inflow',industry:'five_day_return'});
for(const value of ['+1.23亿','-2.50万','主力资金口径','行业5日样本','+7.25%']) assert.ok(ranked.includes(value),value);
const unavailable = page.render(state,date,null,{concept:'five_day_return'});
assert.match(unavailable, /暂无可核验的5日涨幅榜数据/);
assert.match(unavailable, /缺少连续窗口/);
assert.doesNotMatch(unavailable, /不可用伪排名/);
const wrongDate = page.render({heatmap:{...heatmap,trade_date:'2026-11-05'}},date);
assert.doesNotMatch(wrongDate, /行业行情样本|可信报价|同日完整来源/);
""")


def test_sector_heatmap_asset_versions_match_served_bytes():
    root = SCRIPT.parents[1]
    index = (root / "index.html").read_text(encoding="utf-8")
    for relative in ("js/market-home.js", "css/market-home.css"):
        digest = hashlib.sha256((root / relative).read_bytes()).hexdigest()[:16]
        assert f"/static/{relative}?v={digest}" in index


def test_monitor_scroll_loads_older_records_and_retains_scroll_position():
    run_node(r"""
global.document = {activeElement:null};
const list = {scrollTop:0,clientHeight:100,scrollHeight:400,hasAttribute:name=>name==='data-mh-events'};
const container = {innerHTML:'',listeners:new Map(),contains:target=>Boolean(target),querySelector:()=>list,
    addEventListener(type,listener) {this.listeners.set(type,listener);},
    removeEventListener(type) {this.listeners.delete(type);}};
const date='2026-11-06';
const event=i=>({stock_code:String(i).padStart(6,'0'),snapshot_at:date+'T10:00:00',detail_json:{short_name:'事件'+i}});
let olderCalls=0;
const request=async url=>{
    if(url.includes('/monitor/events')) {
        if(url.includes('&before=')) {olderCalls++;return {requested_date:date,events:[event(99)],next_cursor:null};}
        return {requested_date:date,events:Array.from({length:50},(_,i)=>event(i)),next_cursor:'cursor'};
    }
    return {};
};
(async()=>{
    try {
        await page.load(date,container,{request});
        assert.ok(container.innerHTML.includes('事件49'));
        assert.ok(container.innerHTML.includes('mh-event-scroll'));
        list.scrollTop=280;
        container.listeners.get('scroll')({target:list});
        container.listeners.get('scroll')({target:list});
        await new Promise(resolve=>setImmediate(resolve));
        assert.equal(olderCalls,1);
        assert.equal(list.scrollTop,280);
        assert.ok(container.innerHTML.includes('事件99'));
        assert.ok(container.innerHTML.includes('事件0'));
        assert.ok(container.innerHTML.includes('已显示当天全部事件'));
    } finally {page.stop();}
})().catch(e=>{console.error(e);process.exitCode=1;});
""")


def test_all_seven_indices_show_intraday_quotes_and_visible_source_time_without_breadth():
    run_node(r"""
const date='2026-11-19';
const codes=['000001','399001','399006','000680','000688','000852','000300'];
const snapshot={status:'partial',trade_date:date,as_of:date+'T10:30:00+08:00',as_of_basis:'public_index_snapshot',
    indices:codes.map((index_code,i)=>({index_code,trade_date:date,price:1000+i,change_pct:1.25,
        as_of:date+'T10:29:59+08:00',source:'eastmoney.push2.index',evidence_mode:'LIVE_OBSERVATION'}))};
const html=page.render({snapshot,market:{status:'unavailable'}},date);
assert.equal((html.match(/mh-index-tile/g)||[]).length,7);
assert.equal((html.match(/\+1\.25%/g)||[]).length,7);
assert.equal((html.match(/2026-11-19 10:29:59<\/span>/g)||[]).length,7);
assert.ok(html.includes('盘中指数'));
assert.ok(html.includes('1,006.00'));
""")


def test_turnover_ranking_keeps_its_own_intraday_source_and_timestamp():
    run_node(r"""
const date='2026-11-19';
const snapshot={status:'partial',trade_date:date,as_of:date+'T10:30:00+08:00',as_of_basis:'qmt_snapshot',source:'sm_stock_current',
    ranking_observations:{turnover:{trade_date:date,as_of:date+'T10:29:50+08:00',source:'eastmoney.push2.f8',scope:'东财盘中换手样本'}},
    rankings:{turnover:[{stock_code:'002685',short_name:'华东重机',trade_date:date,turnover_rate:10.32,turnover_rate_source:'eastmoney.push2.f8'}]}};
const html=page.render({snapshot},date);
assert.ok(html.includes('东财盘中换手样本 · 2026-11-19T10:29:50+08:00 · eastmoney.push2.f8'));
assert.ok(html.includes('华东重机'));
assert.ok(html.includes('10.32%'));
""")


def test_intraday_metrics_use_verified_board_when_aggregate_fails():
    run_node(r"""
const date='2026-11-19';
const overview={status:'available',trade_date:date,data_source:'sm_stock_current',scope_label:'已核验行情样本',
    data_time:date+'T10:30:00+08:00',total_count:10,up_count:6,flat_count:1,down_count:3,total_amount:123000000,
    freshness_status:'realtime'};
const snapshot={status:'partial',trade_date:date,as_of:date+'T10:30:00+08:00',market_overview:overview};
const liquidity={status:'available',trade_date:date,as_of:date+'T10:29:50+08:00',turnover_median_pct:2.35,
    turnover_sample_count:9,volume_ratio_median:1.23,volume_sample_count:8,definition:'源端逐股量比中位数'};
const html=page.render({snapshot,market:{status:'unavailable'},liquidity},date);
for(const value of ['上涨占比 60.0%','1.23亿','2.35% / 1.23倍','盘中量比中位','已核验行情样本','10:29:50']) assert.ok(html.includes(value),value);
assert.ok(!html.includes('市场情绪观察待核验'));
assert.ok(!html.includes('涨跌家数待核验'));
const prior={...overview,trade_date:'2026-11-18',data_source:'yesterday_only',up_count:1,flat_count:1,down_count:8};
const current=page.render({snapshot,market:prior,liquidity},date);
assert.ok(current.includes('上涨占比 60.0%'));
assert.ok(!current.includes('yesterday_only'));
assert.ok(!page.render({snapshot,liquidity:{...liquidity,trade_date:'2026-11-18'}},date).includes('2.35% / 1.23倍'));
""")


def test_home_keeps_refreshing_through_lunch_and_after_close():
    run_node(r"""
global.document={activeElement:null,hidden:false};
let tick, delay, now=0;
global.performance={now:()=>now};
global.setTimeout=(fn,ms)=>{tick=fn;delay=ms;return 1;};global.clearTimeout=()=>{};
const calls=[];
const container={innerHTML:'',contains:()=>false,addEventListener:()=>{},removeEventListener:()=>{}};
const clock={phase:'midday_break',active_trade_date:'2026-11-19',is_intraday:false};
let complete;
let hold=false;
const request=async url=>{calls.push(url);if(hold && url.includes('/monitor/board')) await new Promise(resolve=>{complete=resolve;});return {};};
(async()=>{try {
 await page.load('2026-11-19',container,{request,refreshClock:async()=>clock});
 assert.equal(delay,20000);
 calls.length=0;hold=true;
 const previous=tick;const pending=tick();
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(tick,previous); // No next timer while this batch is in flight.
 now+=7000;complete();await pending;hold=false;
 assert.equal(delay,13000); // Request duration is part of the 20-second cycle.
 assert.ok(calls.some(url=>url.includes('/monitor/liquidity')));
 assert.ok(calls.some(url=>url.includes('/monitor/board')));
 calls.length=0;clock.phase='postmarket';await tick();
 assert.equal(delay,20000);
 assert.ok(calls.some(url=>url.includes('/monitor/liquidity')));
 assert.ok(calls.some(url=>url.includes('/monitor/board')));
 calls.length=0;clock.is_intraday=true;clock.phase='morning';hold=true;
 const slow=tick();await new Promise(resolve=>setImmediate(resolve));
 now+=25000;complete();await slow;hold=false;
 assert.equal(delay,0); // A slow batch finishes before the next cycle can start.
 calls.length=0;document.hidden=true;await tick();
 assert.equal(calls.length,0);assert.equal(delay,20000);
}finally{page.stop();}})().catch(e=>{console.error(e);process.exitCode=1;});
""")


def test_refresh_keeps_observed_data_on_failure_and_paints_one_complete_cycle():
    run_node(r"""
global.document={activeElement:null,hidden:false};
let tick, paints=0, html='', fail=false, version=1;
global.setTimeout=fn=>{tick=fn;return 1;};global.clearTimeout=()=>{};
const date='2026-11-20';
const container={get innerHTML(){return html;},set innerHTML(value){html=value;paints++;},
 contains:()=>false,addEventListener:()=>{},removeEventListener:()=>{}};
const clock={is_intraday:true,active_trade_date:date,today:date};
const request=async url=>{
 if(url.includes('/monitor/board')){
  if(fail)throw new Error('network timeout');
  return {status:'available',trade_date:date,as_of:date+'T10:30:00+08:00',source:'verified source',
   rankings:{gainers:[{stock_code:'600001',short_name:'已取得行情'+version,trade_date:date,change_pct:3}]}};
 }
 return {};
};
(async()=>{try{
 await page.load(date,container,{request,clock:()=>clock});
 assert.ok(html.includes('已取得行情1'));
 const before=paints;fail=true;await tick();
 assert.ok(html.includes('已取得行情1'),'a failed request erased the last observed data');
 assert.ok(html.includes('刷新失败'),'retained observations must not masquerade as a successful refresh');
 assert.ok(html.includes('10:30:00'),'the observation time must remain visible');
 assert.equal(paints-before,1,'a refresh rebuilt the page for every source response');
 fail=false;version=2;await tick();
 assert.ok(html.includes('已取得行情2'));
 assert.ok(!html.includes('已取得行情1'));
 assert.ok(!html.includes('刷新失败'));
}finally{page.stop();}})().catch(e=>{console.error(e);process.exitCode=1;});
""")


def test_live_cards_render_before_slow_reference_and_never_poll_full_plate_aggregate():
    run_node(r"""
global.document={activeElement:null,hidden:false};
let tick;
global.setTimeout=fn=>{tick=fn;return 1;};global.clearTimeout=()=>{};
const date='2026-11-20', calls=[];
const container={innerHTML:'',contains:()=>false,addEventListener:()=>{},removeEventListener:()=>{}};
const clock={is_intraday:true,active_trade_date:date,today:date};
const request=url=>new Promise(resolve=>calls.push({url,resolve}));
(async()=>{try{
 const loading=page.load(date,container,{request,clock:()=>clock});
 const value={status:'available',trade_date:date,as_of:date+'T10:30:00+08:00',source:'当前核验行情',
  market_overview:{trade_date:date,up_count:6,flat_count:1,down_count:3,total_count:10,data_source:'当前核验行情'}};
 calls.slice(0,4).forEach(call=>call.resolve(value));
 await new Promise(resolve=>setImmediate(resolve));
 assert.ok(container.innerHTML.includes('当前核验行情'));
 assert.ok(container.innerHTML.includes('上涨占比 60.0%'),'slow history hid the current stock snapshot');
 assert.ok(!calls.some(call=>call.url.includes('/monitor/data')));
 calls.slice(4).forEach(call=>call.resolve({}));await loading;
 calls.length=0;
 const refreshing=tick();await new Promise(resolve=>setImmediate(resolve));
 assert.equal(calls.length,4);
 assert.ok(!calls.some(call=>/market-trend|daily-review|daily-delivery|monitor\/data/.test(call.url)));
 calls.forEach(call=>call.resolve(value));await refreshing;
}finally{page.stop();}})().catch(e=>{console.error(e);process.exitCode=1;});
""")


def test_partial_http_200_cannot_erase_observed_rankings_or_relabel_old_timestamp():
    run_node(r"""
global.document={activeElement:null,hidden:false};let tick, partial=false;
global.setTimeout=fn=>{tick=fn;return 1;};global.clearTimeout=()=>{};
const date='2026-11-20';
const container={innerHTML:'',contains:()=>false,addEventListener:()=>{},removeEventListener:()=>{}};
const request=async url=>url.includes('/monitor/board') ? partial ?
 {status:'partial',trade_date:date,as_of:date+'T10:30:20+08:00',unavailable_fields:['rankings.gainers'],rankings:{gainers:[]}} :
 {status:'partial',trade_date:date,as_of:date+'T10:30:00+08:00',unavailable_fields:[],
  rankings:{gainers:[{stock_code:'600001',short_name:'已核验股票',trade_date:date,change_pct:3}]}} : {};
(async()=>{try{
 await page.load(date,container,{request,clock:()=>({is_intraday:true,active_trade_date:date})});
 partial=true;await tick();
 assert.ok(container.innerHTML.includes('已核验股票'));
 assert.ok(container.innerHTML.includes('刷新失败'));
 assert.ok(container.innerHTML.includes('上次观测'));
 assert.ok(container.innerHTML.includes('10:30:00'));
 const ranking=container.innerHTML.split('<h3>涨幅榜</h3>')[1].split('</section>')[0];
 assert.ok(ranking.includes('10:30:00'));
 assert.ok(!ranking.includes('10:30:20'),'old rows were relabelled with the failed new observation time');
}finally{page.stop();}})().catch(e=>{console.error(e);process.exitCode=1;});
""")


def test_failed_index_and_industry_do_not_block_new_stock_and_concept_observations():
    run_node(r"""
global.document={activeElement:null,hidden:false};let tick, version=1;
global.setTimeout=fn=>{tick=fn;return 1;};global.clearTimeout=()=>{};
const date='2026-11-20';
const container={innerHTML:'',contains:()=>false,addEventListener:()=>{},removeEventListener:()=>{}};
const metric=(name,stamp)=>({status:'available',note:name,as_of:date+' '+stamp,rows:[{code:'BK0001',name,value:1,source:name}]});
const request=async url=>{
 if(url.includes('/monitor/board'))return {status:'partial',trade_date:date,as_of:date+'T10:30:0'+version+'+08:00',
  unavailable_fields:version===1?[]:['indices'],rankings:{gainers:[{stock_code:'600001',short_name:'当前股票'+version,trade_date:date,change_pct:version}]},
  indices:version===1?[{index_code:'000001',trade_date:date,price:3999,as_of:date+'T10:30:01+08:00',source:'公开指数'}]:[]};
 if(url.includes('/monitor/sector-heatmap'))return {requested_date:date,trade_date:date,
  concept:{change:metric('当前概念'+version,'10:30:0'+version)},
  industry:{change:version===1?metric('原行业观测','10:30:01'):{status:'unavailable',rows:[]}}};
 return {};
};
(async()=>{try{
 await page.load(date,container,{request,clock:()=>({is_intraday:true,active_trade_date:date})});
 version=2;await tick();
 assert.ok(container.innerHTML.includes('当前股票2'));
 assert.ok(!container.innerHTML.includes('当前股票1'));
 assert.ok(container.innerHTML.includes('当前概念2'));
 assert.ok(container.innerHTML.includes('原行业观测'));
 assert.ok(container.innerHTML.includes('3,999.00'));
 const industry=container.innerHTML.split('<h3>行业热度</h3>')[1].split('</section>')[0];
 assert.ok(industry.includes('上次观测'));
 assert.ok(industry.includes('10:30:01'));
 assert.ok(!industry.includes('10:30:02'));
}finally{page.stop();}})().catch(e=>{console.error(e);process.exitCode=1;});
""")
