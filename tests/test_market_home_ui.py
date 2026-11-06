"""Exercise rendered evidence and refresh boundaries for the market homepage."""

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


def test_dashboard_distinguishes_current_stale_and_missing_evidence():
    run_node(r"""
const date = '2026-11-06';
const market = {
    status:'available', trade_date:date, data_time:date + ' 10:29:55', timezone:'Asia/Shanghai',
    data_source:'有效行情', freshness_status:'realtime', is_realtime:true,
    up_count:0, flat_count:1, down_count:9, total_count:10, total_amount:100000000,
    heat_percentile:0, heat_percentile_sample_count:19,
    concept_rows:[{name:'<b>可核验主题</b>', trade_date:date, change:-1.25, data_source:'板块源'},
        {name:'未来主题', trade_date:'2026-11-09', change:5}]
};
const snapshot = {
    status:'partial', trade_date:date, as_of:date, source:'日行情样本', sample_size:10,
    distribution:[{key:'lt_neg_3', label:'跌3%以上', upper:-3, count:6}, {key:'flat', label:'持平', count:1}],
    strong:{up_3pct:0,down_3pct:6},
    indices:[{index_code:'000001.SH', trade_date:date, price:3999.01, change_pct:0, source:'指数日行情'},
        {index_code:'399001.SZ', trade_date:'2026-11-09', price:12345.67, change_pct:1}],
    rankings:{gainers:[{stock_code:'600001', short_name:'当日股票', trade_date:date, change_pct:0},
        {stock_code:'600002', short_name:'错日排名', trade_date:'2026-11-05', change_pct:20}],
        turnover:[{stock_code:'600003', short_name:'有换手率股票', trade_date:date, turnover_rate:12.34}]}
};
const trend = {source:{table:'指数历史表'}, indices:[{index_code:'000300',data_cutoff:'2026-11-05',
    periods:{daily:{metrics:{close:4600.25,sma20:4500,sma60:4400,sma20_slope_5_pct:0.2}}}}]};
const html = page.render({market,snapshot,trend}, date, {server_time:date + ' 10:30:00'});
for (const expected of ['<section class="market-home"', '盘中快照', '延迟 5秒', '行情 2026-11-06 10:29:55',
    '0.0%', '1.00亿', 'P0', '涨跌幅达到 ±3%', '跌3%以上', '分布样本 10只', '所选交易日的异动事件证据待核验',
    '沪深300 · 2026-11-05', '4,600.25', '3,999.01', '指数日行情', '&lt;b&gt;可核验主题&lt;/b&gt;',
    '当日股票', 'data-mh-stock="600001"', '12.34%', 'data-mh-tab="market-context"']) assert.ok(html.includes(expected), expected);
assert.doesNotMatch(html, /12,345\.67|未来主题|错日排名|<b>可核验主题|NaN|undefined/);
assert.doesNotMatch(page.render({market}, date), /延迟/);
assert.doesNotMatch(page.render({market}, date, {server_time:date + ' 10:29:54'}), /延迟/);
const later = page.render({market,snapshot}, '2026-11-09', {server_time:'2026-11-09 10:30:00'});
assert.match(later, /历史回退/);
assert.match(later, /有效行情/);
assert.doesNotMatch(later, /延迟|跌3%以上|当日股票/);
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
const container = {innerHTML:'', listeners:new Set(), contains:() => false,
    addEventListener(_type, listener) {this.listeners.add(listener);},
    removeEventListener(_type, listener) {this.listeners.delete(listener);}};
const calls = [];
const request = (url, timeout, options) => new Promise(resolve => calls.push({url,timeout,options,resolve}));
(async function () {
    try {
        const old = page.load('2026-11-05', container, {request});
        const current = page.load('2026-11-06', container, {request});
        const paths = ['/api/monitor/data', '/api/hot-data/market-trend', '/api/monitor/board'];
        const expected = ['2026-11-05','2026-11-06'].flatMap(date => paths.map(path => path + '?date=' + date));
        assert.deepEqual(calls.map(call => call.url), expected);
        assert.ok(calls.every(call => call.timeout === 15000 && call.options.cache === 'no-store'));
        assert.equal(container.listeners.size, 1);
        calls.slice(3).forEach(call => call.resolve({trade_date:'2026-11-06', data_source:'新日期来源'}));
        await current;
        calls.slice(0,3).forEach(call => call.resolve({trade_date:'2026-11-05', data_source:'旧日期来源'}));
        await old;
        assert.match(container.innerHTML, /新日期来源/);
        assert.doesNotMatch(container.innerHTML, /旧日期来源/);
    } finally {
        page.stop();
        assert.equal(container.listeners.size, 0);
    }
})().catch(error => {console.error(error); process.exitCode = 1;});
""")
