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
        const paths = ['/api/monitor/data', '/api/hot-data/market-trend', '/api/monitor/board', '/api/monitor/sector-heatmap'];
        const expected = ['2026-11-05','2026-11-06'].flatMap(date => [
            ...paths.map(path => path + '?date=' + date), '/api/hot-data/daily-review-dates',
            '/api/scheduler/daily-delivery?trade_date=' + date + '&attempt_limit=12'
        ]);
        assert.deepEqual(calls.map(call => call.url), expected);
        assert.ok(calls.every(call => call.timeout === (call.url.includes('sector-heatmap') ? 30000 : 15000) && call.options.cache === 'no-store'));
        assert.equal(container.listeners.size, 2);
        calls.slice(paths.length + 2).forEach(call => call.resolve({trade_date:'2026-11-06', data_source:'新日期来源'}));
        await current;
        const listener = container.listeners.get('click');
        container.contains = target => Boolean(target);
        listener({target:{closest:() => ({dataset:{mhHeatMode:'concept:net_inflow'}})}});
        assert.match(container.innerHTML, /aria-pressed="true"[^>]*data-mh-heat-mode="concept:net_inflow"/);
        assert.match(container.innerHTML, /aria-pressed="true"[^>]*data-mh-heat-mode="industry:change"/);
        assert.equal(calls.length, expected.length);
        calls.slice(0,paths.length + 2).forEach(call => call.resolve({trade_date:'2026-11-05', data_source:'旧日期来源'}));
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
