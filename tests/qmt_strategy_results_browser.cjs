/* Visual/interaction QA only. Fixtures never enter production storage. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const {chromium} = require(process.argv[2] || 'playwright');
const root = path.resolve(__dirname, '..');
const day = '2026-09-30';
const names = ['超短线', '短线', '波段', '主升浪', '板块扩散', '低位点火', '右侧主升', '事件漂移', '质量动量', '超跌修复', '组合一', '组合二', '组合三', '组合四'];
const rows = names.map((name, i) => ({strategy_key:'test_'+i,name,version:'测试夹具',family:i>=4&&i<10?'V3':'V2',status:'DATA_BLOCKED',selected:[],selected_count:0,candidate_count:0,blocked_reasons:['测试夹具：目标交易日日线证明不足，不能认定没有符合股票。']}));
let data = {trade_date:day,dates:[{trade_date:day,run_count:1}],runs:[{run_uid:'run-a',trade_date:day,run_mode:'DAILY',status:'DATA_BLOCKED'}],
  schedule:{status:'REGISTERED',owner:'WINDOWS_QMT',enabled:true,cron_time:'22:50',timezone:'Asia/Shanghai'},
  catalog:{strategies:rows.slice(0,10),combinations:rows.slice(10),excluded:[{name:'盘中超预期',reason:'用户排除'},{name:'弱市结构性主线',reason:'用户排除'}]},
  latest:{run_uid:'run-a',trade_date:day,run_mode:'DAILY',origin:'WINDOWS_DAILY',status:'DATA_BLOCKED',
    execution:{started_at:'2026-10-08T01:15:01+08:00',finished_at:'2026-10-08T01:15:02+08:00'},
    issued_at:'2026-10-07T17:14:59Z',completed_at:'2026-10-07T17:15:02Z',
    input:{prepared_at:'2026-10-08T01:14:59+08:00',decision_at:'2026-10-08T01:14:50',v2:{status:'DATA_BLOCKED',reasons:['测试V2缺数']},v3:{status:'DATA_BLOCKED',reasons:['测试V3缺数']}},
    input_hash:'input-test-proof',result_hash:'result-test-proof',edge_build_sha:'release-test-proof',
    result:{trade_date:day,simulation_only:true,real_order_allowed:false,strategy_rows:rows.slice(0,10),combination_rows:rows.slice(10)}}};
const html='<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><link rel="stylesheet" href="/static/css/style.css"><link rel="stylesheet" href="/static/css/qmt-strategy-results.css"><body style="margin:0;padding:20px"><p>浏览器测试夹具 · 非线上执行结果</p><main id="desk"></main><script src="/static/js/qmt-strategy-results.js"></script><script>window.opened=[];window.loadPage=()=>QmtStrategyResults.load("'+day+'",document.getElementById("desk"),{stock:c=>opened.push(c)});window.ready=loadPage();</script></body></html>';
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost');
  if(url.pathname==='/'){res.setHeader('content-type','text/html; charset=utf-8');res.end(html);return;}
  if(url.pathname.startsWith('/api/strategy-center/qmt-results')){res.setHeader('content-type','application/json');res.end(JSON.stringify(data));return;}
  const file=path.resolve(root,'server','.'+url.pathname);
  if(!file.startsWith(path.join(root,'server','static')+path.sep)||!fs.existsSync(file)){res.writeHead(404);res.end();return;}
  res.setHeader('content-type',file.endsWith('.css')?'text/css':'text/javascript');res.end(fs.readFileSync(file));
});
(async()=>{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const browser=await chromium.launch({headless:true,channel:'chrome'});
  const errors=[], artifacts=[];
  const output=fs.mkdtempSync(path.join(os.tmpdir(),'probiga-qmt-results-ui-'));
  try{
    for(const width of [1365,390]){
      const page=await browser.newPage({viewport:{width,height:1000}});
      page.on('pageerror',e=>errors.push(e.message));
      await page.goto('http://127.0.0.1:'+server.address().port);await page.evaluate(()=>ready);
      assert.equal(await page.locator('details.qr-model').count(),14);
      await page.locator('details.qr-model').first().locator('summary').click();
      assert.match(await page.locator('details.qr-model').first().innerText(),/目标交易日日线证明不足/);
      await page.locator('[data-qr="filter"]').selectOption('DATA_BLOCKED');
      assert.equal(await page.locator('details.qr-model').count(),14);
      await page.locator('[data-qr="search"]').fill('低位点火');
      assert.equal(await page.locator('details.qr-model').count(),1);
      await page.locator('[data-qr="search"]').fill('');
      const horizontal=await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth+1);
      assert.equal(horizontal,false,'page must fit its viewport');
      const shot=path.join(output,'blocked-'+width+'.png');await page.screenshot({path:shot,fullPage:true});artifacts.push(shot);
      await page.close();
    }
    data=structuredClone(data);
    const observation={stock_code:'600001',stock_name:'测试观察股',score:.75,score_scale:1,rank_no:1,selection_score:.75,
      selection_kind:'ORIGINAL_V3_SHADOW_PORTFOLIO_OBSERVATION',ranking_basis:'UNCALIBRATED_RAW_SCORE_RESEARCH_ONLY',status:'UNCALIBRATED',
      expected_return_net_pct:null,reasons:['测试原策略观察排名第1'],conditions:[{label:'校准正期望',status:'BLOCK',value:'未校准',required:'VALIDATED_POSITIVE'}]};
    data.latest.result.strategy_rows[4]={...rows[4],status:'COMPLETED',selected:[observation],selected_count:1,candidate_count:1,blocked_reasons:[]};
    const page=await browser.newPage({viewport:{width:1365,height:1000}});page.on('pageerror',e=>errors.push(e.message));
    await page.goto('http://127.0.0.1:'+server.address().port);await page.evaluate(()=>ready);
    assert.match(await page.locator('body').innerText(),/V3模拟组合观察入选（非买入指令）/);
    assert.match(await page.locator('body').innerText(),/未取得校准预测/);
    assert.match(await page.locator('body').innerText(),/未满足/);
    await page.locator('[data-qr-stock="600001"]').click();assert.deepEqual(await page.evaluate(()=>opened),['600001']);
    assert.doesNotMatch(await page.locator('body').innerText(),/0\.00%/);
    const shot=path.join(output,'observation-1365.png');await page.screenshot({path:shot,fullPage:true});artifacts.push(shot);
    assert.deepEqual(errors,[]);console.log(JSON.stringify({status:'PASS',artifacts}));
  }finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
})().catch(e=>{console.error(e);process.exitCode=1;server.close();});
