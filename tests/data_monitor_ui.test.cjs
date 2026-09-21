/* Run with Node and Playwright available on NODE_PATH. No production requests. */
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const root = path.resolve(__dirname, '..');
global.window = {};
const {summarize, esc, shift} = require('../server/static/js/data-monitor.js');

test('summary excludes rest days and preserves unknown evidence', () => {
    const dates = [{date:'2026-09-18',trade_status:1},{date:'2026-09-19',trade_status:0},{date:'2026-09-20',trade_status:null}];
    const s = summarize([{days:[{status:'partial'},{status:'closed'},{status:'unknown'}]}], dates);
    assert.deepEqual(s, {due:1,complete:0,gaps:1,unknown:1,earliest:'2026-09-18',continuous:''});
    assert.equal(shift('2026-03-01',-1), '2026-02-28');
    assert.equal(esc('<script>'), '&lt;script&gt;');
});

test('browser: day selection, range races, failure state, and mobile layout', async () => {
    const browser = await chromium.launch({headless:true, ...(process.platform === 'win32' ? {channel:'msedge'} : {})});
    try {
        const page = await browser.newPage({viewport:{width:1400,height:1050}});
        const errors = [];
        page.on('pageerror', e => errors.push(e.message));
        let fail = false;
        const cell = {dataset:'minute',trade_date:'2026-09-18',status:'partial',reason:'缺少有效记录',expected_count:241,actual_count:240,missing_count:1,observed_count:240,invalid_count:0,coverage_ratio:240/241,checked_at:'2026-09-21 15:00:00',check_state:'checked',missing_total:1};
        const dataset = {key:'minute',name:'股票分钟线',scope:'全市场 · 1 分钟',group:'market',task_type:'qmt_stock_minute_canonical',table:'sm_stock_minute'};
        await page.route('http://monitor.test/**', async route => {
            const url = new URL(route.request().url());
            if (url.pathname === '/') return route.fulfill({contentType:'text/html',body:'<html lang="zh-CN"><meta charset="utf-8"><body style="margin:20px;background:#f5f7fa;font-family:Arial,Microsoft YaHei,sans-serif"><main id="app" class="active"></main></body></html>'});
            if (fail) return route.fulfill({status:503,json:{detail:'测试连接中断'}});
            if (url.pathname.endsWith('/detail')) return route.fulfill({json:{...cell,trade_date:url.searchParams.get('trade_date'),definition:dataset,unit:'条',missing:[{stock_code:'600000',missing_count:1,missing_times:['14:59'],reason:'缺少有效记录'}],gaps:[],errors:[],tasks:[]}});
            const start = url.searchParams.get('start_date'), end = url.searchParams.get('end_date');
            const dates = []; for(let d=start;d<=end;d=shift(d,1)) dates.push({date:d,trade_status:[0,6].includes(new Date(d+'T00:00:00Z').getUTCDay())?0:1});
            // Delay one obsolete response to ensure it cannot overwrite a later range.
            if (start.endsWith('-01-01')) await new Promise(r=>setTimeout(r,180));
            return route.fulfill({json:{generated_at:'2026-09-21 15:00:00',dates,datasets:[{...dataset,days:dates.map(d=>({...cell,trade_date:d.date,status:d.trade_status===0?'closed':'partial'}))}],summary:{},tasks:[],errors:[],scan:{checked_days:dates.length,pending_days:0}}});
        });
        await page.goto('http://monitor.test/');
        await page.addStyleTag({content:fs.readFileSync(path.join(root,'server/static/css/data-monitor.css'),'utf8')});
        await page.addScriptTag({content:fs.readFileSync(path.join(root,'server/static/js/data-monitor.js'),'utf8')});
        await page.evaluate(()=>window.ProBigADataMonitor.mount(document.querySelector('#app')));
        assert.equal(await page.locator('.dm-cell').count(),30);
        await page.locator('.dm-cell').first().click();
        await page.waitForFunction(()=>document.querySelector('.dm-detail').textContent.includes('600000'));
        await page.locator('[data-range="year"]').click();
        await page.locator('[data-range="30"]').click();
        await page.waitForTimeout(250);
        assert.equal(await page.locator('.dm-cell').count(),30);
        if (process.env.MONITOR_SCREENSHOT) await page.screenshot({path:process.env.MONITOR_SCREENSHOT,fullPage:true});
        fail = true;
        await page.locator('[data-action="refresh"]').click();
        await page.waitForSelector('.dm-disconnected');
        assert.match(await page.locator('[data-dm="errors"]').textContent(),/状态读取失败/);
        await page.setViewportSize({width:390,height:844});
        assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth <= innerWidth),true);
        assert.deepEqual(errors,[]);
    } finally { await browser.close(); }
});
