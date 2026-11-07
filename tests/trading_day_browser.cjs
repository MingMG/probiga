/* Run: node tests/trading_day_browser.cjs [absolute path to playwright module] [--engineering-acceptance-only]. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const os = require('node:os');
const flags = process.argv.slice(2);
const {chromium} = require(flags.find(arg=>!arg.startsWith('--')) || 'playwright');
const root = path.resolve(__dirname, '..');
const day = '2026-09-18';
let clock = {server_time:day+' 10:30:00',today:day,active_trade_date:day,ui_trade_date:day,expected_trade_date:day,is_trade_day:true,is_intraday:true,phase:'intraday'};
let journal = {trade_date:day,revision:0,plans:[],review:{text:'',updated_at:null},updated_at:null};
let conflict=false, failSave=false, slowDate=false, writes=0, saveBarrier=null;
const clone=v=>JSON.parse(JSON.stringify(v));
const candidates=Array.from({length:5},(_,i)=>({stock_code:'60000'+i,stock_name:'观察股'+(i+1),reason:'测试研究依据 <script>window.injected=true</script>',trigger:'量价条件 '+i,invalidation:'失效条件 '+i}));
const navigation=[
 ['今日工作台',[['workbench','市场看板'],['trading-day','今日看盘'],['command','智能决策'],['trading','交易与复盘总览']]],
 ['大盘与外围',[['market-context','市场环境'],['market-stage','大盘走势阶段'],['sentiment','市场情绪与风格'],['market-outlook','明日推演']]],
 ['板块与题材',[['sector','板块分析'],['concept','概念与行业']]],
 ['盘中监控与风险',[['monitor','盘中监控详情'],['market-radar','异动雷达'],['intraday-alert','盘中播报'],['trading-v3-intraday','盘中应急'],['intraday-battle','盘中作战'],['hunter','狩猎场']]],
 ['资金与龙虎榜',[['capital','个股资金'],['broad-etf-flow','宽基 ETF 资金'],['mainforce','主力行为'],['alist','龙虎榜']]],
 ['个股与自选',[['stock-list','全市场股票'],['portfolio','自选股'],['strong','强势股'],['fused','个股热榜']]],
 ['资讯公告与事件',[['news','财经快讯'],['notice','个股公告'],['research-radar','研报雷达'],['commentary','股评监控']]],
 ['选股与交易计划',[['daily-pool','每日研究票池'],['trading-v3-candidates','策略选股结果'],['trading-v3-overview','今日策略'],['recommended','AI 研究建议'],['screen','条件选股（研究）'],['jq-picks','聚宽策略']]],
 ['策略研究与回测',[['strategy-center','策略研究与竞技'],['strategy-backtest','策略回测'],['trading-v3-hypotheses','连续跟踪'],['quant-workbench','量化工作台']]],
 ['持仓与模拟执行',[['trading-v3-positions','我的持仓'],['sim-trade','旧模拟盘（可操作）'],['qmt-strategy-results','QMT每日模拟']]],
 ['复盘、早报与日志',[['review','每日复盘与早报']]],
 ['AI 问答',[['ai-stock','股票问答'],['ai-general','通用问答']]],
 ['数据中心',[['datasource','数据获取监控']]],
 ['调度、权限与部署',[['scheduler','调度管理']]]
];
async function assertNavigation(page,expected){
 const groups=await page.locator('#sidebar .sidebar-group').evaluateAll(nodes=>nodes.map(group=>({
  title:group.querySelector('.sidebar-group-title span').textContent.trim(),
  items:Array.from(group.querySelectorAll('.sidebar-item[data-tab]')).map(item=>({id:item.dataset.tab,text:item.textContent.trim()}))
 })));
 assert.deepEqual(groups.map(group=>group.title),expected.map(group=>group[0]));
 for(let i=0;i<expected.length;i++){
  assert.deepEqual(groups[i].items.map(item=>item.id),expected[i][1].map(item=>item[0]),expected[i][0]);
  for(let j=0;j<expected[i][1].length;j++)assert.ok(groups[i].items[j].text.endsWith(expected[i][1][j][1]),groups[i].items[j].text+' should retain '+expected[i][1][j][1]);
 }
 const ids=groups.flatMap(group=>group.items.map(item=>item.id));assert.equal(new Set(ids).size,ids.length);
 for(const view of ['overview','positions','candidates','intraday','hypotheses']){
  const item=page.locator('#sidebar [data-tab="trading-v3-'+view+'"]');
  assert.equal(await item.getAttribute('data-trading-view'),view);assert.equal(await item.getAttribute('data-module-page'),'v3');
 }
}
function payload(url){
 if(url.includes('/market-clock'))return clock;
 if(url.includes('premarket-theme-forecast'))return {requested_date:day,session_date:day,source_trade_date:'2026-09-17',stage:'PREMARKET_0908',run_uid:'forecast-test',cutoff_at:day+' 09:08:00',generated_at:day+' 09:08:20',decision_scope:'RESEARCH_DISPLAY_ONLY',themes:[{theme_key:'t1',theme_name:'测试主线',reason:'盘前研究方向',stock_candidates:candidates}]};
 if(url.includes('auction-gate'))return {data:{status:'COMPLETED',decision_date:'2026-09-17',execution_session_date:day,source_run_uid:'separate-auction',evidence_mode:'PERSISTED_IMMUTABLE_RUN',cutoff_at:day+' 09:25:00',summary:{reviewed_count:1,candidate_count:1},assessments:[{stock_code:'600000',stock_name:'观察股1',gate_status:'CONFIRMED',quote_at:day+' 09:25:00',reasons:['独立竞价依据']}]}};
 if(url.includes('/monitor/data'))return {trade_date:day,total_count:100,up_count:65,down_count:30,total_amount:22000000000,data_time:day+' 10:29:00',is_realtime:true,freshness_status:'fresh'};
 if(url.includes('holding-strategy'))return {trade_date:day,knowledge_cutoff:day+' 10:29:00',historical_read_only:false,data:[{stock_code:'600010',short_name:'已有持仓',shares:100,sellable_shares:0,t1_blocked:true,trade_date:day,latest_price:10,price_trade_date:day,same_session_price:true,action:'核对风险',reason:'持仓观察说明'}]};
 if(url.includes('daily-review/quant'))return {date:day,data:[{review_date:day,publish_status:'ready',generated_at:day+' 17:00:00',data_cutoff_at:day+' 16:00:00',compact_review:'盘后研究回顾',factor_validation_json:'{}'}]};
 if(url.includes('/auth/status'))return {authenticated:true,account_initialized:true};
 if(url.includes('/latest-trade-date'))return {latest_date:day};
 return {};
}
const harness='<!doctype html><html lang="zh"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/static/css/trading-day.css"><style>body{margin:0;background:#f3f5f8;font-family:Arial,"Microsoft YaHei",sans-serif}#desk{padding:24px;max-width:1200px;margin:auto}h2,h3,p{margin:0}*{box-sizing:border-box}@media(max-width:440px){#desk{padding:12px}}</style><div id="desk"></div><script src="/static/js/trading-day-model.js"></script><script src="/static/js/trading-day.js"></script><script>window.clock='+JSON.stringify(clock)+';window.date="'+day+'";window.options={clock:()=>clock,request:async(url,timeout,options)=>{const r=await fetch(url,options);const data=await r.json();if(!r.ok){const e=Error("request failed");e.httpStatus=r.status;throw e;}return data;},navigate:tab=>window.navigation=tab,stock:code=>window.stock=code,refreshClock:async()=>{clock=await (await fetch("/api/hot-data/market-clock")).json();}};window.start=(d)=>TradingDayDesk.load(d,document.getElementById("desk"),options);window.loaded=start(date);</script></html>';
const server=http.createServer((req,res)=>{
 const url=new URL(req.url,'http://localhost');
 if(url.pathname==='/harness'){res.setHeader('content-type','text/html;charset=utf-8');res.end(harness);return;}
 const file=url.pathname==='/'?path.join(root,'server/static/index.html'):path.join(root,'server',url.pathname);
 if(!file.startsWith(path.join(root,'server')) || !fs.existsSync(file) || !fs.statSync(file).isFile()){res.writeHead(404);res.end();return;}
 res.setHeader('content-type',file.endsWith('.css')?'text/css':file.endsWith('.js')?'text/javascript':'text/html');res.end(fs.readFileSync(file));
});
(async()=>{
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const base='http://127.0.0.1:'+server.address().port;
 const browser=await chromium.launch({headless:true,channel:process.env.PLAYWRIGHT_CHANNEL || 'chrome'});
 const page=await browser.newPage({viewport:{width:1280,height:1000}});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/api/**',async route=>{
  const req=route.request(), url=req.url();
  if(url.includes('/trading-day/journal')){
   const requested=new URL(url).searchParams.get('trade_date');
   if(slowDate && requested!==day)await new Promise(r=>setTimeout(r,120));
   if(req.method()==='PUT'){
    writes++;const body=req.postDataJSON();
    if(saveBarrier)await saveBarrier;
    assert.deepEqual(Object.keys(body).sort(),['plans','review','revision']);
    assert.deepEqual(Object.keys(body.review),['text']);
    for(const p of body.plans)assert.deepEqual(Object.keys(p).sort(),['invalidation','note','reason','source_as_of','source_run_uid','status','stock_code','stock_name','theme','trigger'].sort());
    if(failSave){await route.fulfill({status:503,json:{error:'unavailable'}});return;}
    if(conflict){conflict=false;journal.revision++;journal.plans.push({stock_code:'600099',stock_name:'其他页面计划',theme:'other',reason:'',trigger:'不同页面条件',invalidation:'',source_as_of:'',source_run_uid:'',status:'WATCHING',note:'',created_at:day+'T10:29:00+08:00',updated_at:day+'T10:29:00+08:00',original:{}});await route.fulfill({status:409,json:{error:'conflict'}});return;}
    assert.equal(body.revision,journal.revision);
    journal={trade_date:day,revision:journal.revision+1,plans:body.plans.map(p=>{const old=journal.plans.find(x=>x.stock_code===p.stock_code);return {...p,created_at:old?.created_at || day+'T10:30:00+08:00',updated_at:day+'T10:30:00+08:00',original:old?.original || clone(p)};}),review:{text:body.review.text,updated_at:day+'T10:30:00+08:00'},updated_at:day+'T10:30:00+08:00'};
   }
   await route.fulfill({json:requested===day?journal:{trade_date:requested,revision:0,plans:[],review:{text:''}}});return;
  }
  await route.fulfill({json:payload(url)});
 });
 try {
  await page.goto(base+'/harness');await page.evaluate(()=>loaded);
  assert.equal(await page.locator('.td-stockrow:visible').count(),3);
  assert.equal(await page.locator('.td-stockstate').first().textContent(),'待核验');
  assert.match(await page.locator('.td-facts').innerText(),/220 亿/);
  await page.locator('[data-td-more]').click();assert.equal(await page.locator('.td-stockrow:visible').count(),5);
  await page.locator('[data-td-stock]').first().click();assert.equal(await page.locator('dialog').evaluate(e=>e.open),true);
  assert.equal(await page.evaluate(()=>window.injected),undefined);
  await page.locator('[name=trigger]').fill('我的确认条件');
  await page.evaluate(()=>start(date));
  assert.equal(await page.locator('[name=trigger]').inputValue(),'我的确认条件');
  await page.locator('[data-td-save-plan]').click();await page.waitForFunction(()=>!document.querySelector('dialog').open);
  assert.equal(journal.plans.length,1);assert.equal(journal.plans[0].trigger,'我的确认条件');
  assert.equal(await page.locator('[data-td-plan]').count(),1);
  let releaseSave;saveBarrier=new Promise(resolve=>releaseSave=resolve);
  await page.locator('[data-td-plan]').click();await page.locator('[data-td-save-plan]').click();
  await page.waitForFunction(()=>document.querySelector('[data-td-save-plan]').disabled);
  await page.locator('[data-td-close]').click();await page.locator('[data-td-stock]').nth(1).click();
  await page.locator('[name=note]').fill('新的抽屉草稿');releaseSave();saveBarrier=null;
  await page.waitForFunction(()=>!document.querySelector('[data-td-save-plan]').disabled);
  assert.equal(await page.locator('dialog').evaluate(e=>e.open),true);
  assert.equal(await page.locator('[name=note]').inputValue(),'新的抽屉草稿');
  assert.match(await page.locator('#td-dialog-title').innerText(),/观察股2/);await page.keyboard.press('Escape');
  await page.locator('[data-td-plan]').click();await page.locator('[name=note]').fill('保存失败保留此输入');failSave=true;
  await page.locator('[data-td-save-plan]').click();await page.waitForFunction(()=>document.querySelector('.td-dialog-message').textContent.includes('保存未确认'));
  assert.equal(await page.locator('[name=note]').inputValue(),'保存失败保留此输入');failSave=false;conflict=true;
  await page.locator('[data-td-save-plan]').click();await page.waitForFunction(()=>document.querySelector('.td-dialog-message').textContent.includes('其他页面更新'));
  await page.waitForFunction(()=>!document.querySelector('[data-td-save-plan]').disabled);
  await page.locator('[data-td-save-plan]').click();await page.waitForFunction(()=>!document.querySelector('dialog').open);
  assert.equal(journal.plans.length,2);assert.equal(journal.plans.find(p=>p.stock_code==='600000').note,'保存失败保留此输入');
  await page.locator('[data-td-plan="600000"]').click();await page.locator('[data-td-remove-plan]').click();await page.waitForFunction(()=>!document.querySelector('dialog').open);
  assert.equal(journal.plans.length,1);
  await page.locator('[data-td-phase=pre]').click();assert.equal(await page.locator('.td-stockrow:visible').count(),5);
  assert.doesNotMatch(await page.locator('.td-facts').innerText(),/220 亿/);
  await page.locator('[data-td-phase=pre]').press('End');assert.equal(await page.locator('[data-td-phase=post]').getAttribute('aria-selected'),'true');
  assert.doesNotMatch(await page.locator('.td-review').innerText(),/盘后研究回顾/);
  clock={...clock,server_time:day+' 17:30:00',is_intraday:false,phase:'postmarket'};await page.evaluate(()=>start(date));
  assert.match(await page.locator('.td-review').innerText(),/盘后研究回顾/);
  await page.locator('#td-review-text').fill('个人复盘 <img src=x onerror=alert(1)>');await page.evaluate(()=>start(date));
  assert.equal(await page.locator('#td-review-text').inputValue(),'个人复盘 <img src=x onerror=alert(1)>');
  await page.locator('[data-td-save-review]').click();await page.waitForFunction(()=>document.querySelector('.td-message').textContent.includes('已保存'));
  await page.reload();await page.evaluate(()=>loaded);await page.locator('[data-td-phase=post]').click();
  assert.equal(await page.locator('#td-review-text').inputValue(),'个人复盘 <img src=x onerror=alert(1)>');
  const out=path.join(os.tmpdir(),'probiga-trading-day-qa');fs.mkdirSync(out,{recursive:true});
  await page.screenshot({path:path.join(out,'desktop.png'),fullPage:true});
  for(const width of [1024,736,360,320]){await page.setViewportSize({width,height:1000});assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'overflow '+width);}
  await page.screenshot({path:path.join(out,'mobile.png'),fullPage:true});
  await page.setViewportSize({width:1280,height:1000});
  clock={...clock,today:'2026-09-21',server_time:'2026-09-21 10:30:00',active_trade_date:'2026-09-21'};await page.evaluate(()=>start(date));
  await page.locator('[data-td-plan]').click();assert.equal(await page.locator('[name=trigger]').getAttribute('readonly'),'');assert.equal(await page.locator('[data-td-remove-plan]').count(),0);await page.keyboard.press('Escape');
  slowDate=true;await page.evaluate(()=>{start('2026-09-17');window.lastLoad=start('2026-09-16');});await page.evaluate(()=>lastLoad);
  assert.match(await page.locator('.td-pagehead').innerText(),/2026-09-16/);assert.equal(await page.locator('[data-td-plan]').count(),0);
  await page.evaluate(()=>TradingDayDesk.stop());assert.deepEqual(errors,[]);
  // Exercise the real app: group toggles must not discard page state.
  await page.route('https://cdn.jsdelivr.net/**',route=>route.fulfill({body:'window.Chart=function(){};'}));
  clock={...clock,today:day,server_time:day+' 17:30:00',active_trade_date:day,is_intraday:false,phase:'postmarket'};
  await page.evaluate(()=>localStorage.clear());
  await page.goto(base+'/?tab=trading-day&trade_date='+day);await page.waitForSelector('#tab-trading-day .td-hero');
  await assertNavigation(page,navigation);
  const ops = page.locator('#sidebar .sidebar-group[data-group-key="ops"]');
  await ops.locator('.sidebar-group-toggle').click();
  const acceptance = ops.getByRole('link', {name:'工程验收', exact:true});
  const consoleUrl = 'https://probiga-evidence-console.babeiming0.chatgpt.site/';
  await page.context().route(consoleUrl, route=>route.fulfill({body:'Isolated console fixture'}));
  const [consolePage] = await Promise.all([page.waitForEvent('popup'), acceptance.click()]);
  await consolePage.waitForLoadState('domcontentloaded');
  assert.equal(consolePage.url(),consoleUrl);
  assert.equal(await consolePage.evaluate(()=>window.opener===null),true);
  await consolePage.close();
  if (flags.includes('--engineering-acceptance-only')) {
   assert.deepEqual(errors,[]);
   console.log(JSON.stringify({ok:true,engineeringAcceptanceUrl:consoleUrl}));
   return;
  }
  assert.equal(await page.locator('#btnLayoutToggle').count(),0);
  assert.equal(await page.locator('.sidebar-item.active').getAttribute('data-tab'),'trading-day');
  assert.equal(await page.locator('#sidebar [data-tab="workbench"]').count(),1);
  assert.equal(await page.locator('.sidebar-logo').evaluate(node=>node.tagName),'DIV');
  assert.equal(await page.locator('.sidebar-logo a,.sidebar-logo small').count(),0);
  assert.equal(await page.locator('#sidebar').evaluate(node=>getComputedStyle(node).width),'200px');
  assert.equal(await page.locator('.sidebar-logo').evaluate(node=>getComputedStyle(node).textAlign),'center');
  await page.locator('[data-td-phase=post]').click();
  await page.locator('#td-review-text').fill('展开或折叠菜单必须保留这份未保存草稿');
  await page.evaluate(()=>window.navigationDraftNode=document.querySelector('#td-review-text'));
  const draftUrl=page.url(),draftWrites=writes;
  const marketGroup=page.locator('.sidebar-group[data-group-key="market"]');
  assert.equal(await marketGroup.locator('.sidebar-group-toggle').getAttribute('aria-expanded'),'false');
  await marketGroup.locator('.sidebar-group-toggle').click();
  assert.equal(await marketGroup.locator('.sidebar-group-toggle').getAttribute('aria-expanded'),'true');
  await marketGroup.locator('.sidebar-group-toggle').click();
  assert.equal(await marketGroup.locator('.sidebar-group-toggle').getAttribute('aria-expanded'),'false');
  await assertNavigation(page,navigation);
  assert.equal(await page.locator('#datePicker').inputValue(),day);
  assert.equal(page.url(),draftUrl);assert.equal(writes,draftWrites);
  assert.equal(await page.locator('#td-review-text').inputValue(),'展开或折叠菜单必须保留这份未保存草稿');
  assert.equal(await page.evaluate(()=>window.navigationDraftNode===document.querySelector('#td-review-text')),true);
  await page.goto(base+'/?tab=multi3&trade_date='+day);await page.waitForSelector('#tab-multi3.active');
  assert.equal(await page.evaluate(()=>localStorage.getItem('probiga_current_tab')),'multi3');
  await page.reload();await page.waitForSelector('#tab-multi3.active');await assertNavigation(page,navigation);
  assert.equal(await marketGroup.locator('.sidebar-group-toggle').getAttribute('aria-expanded'),'false');
  assert.match(await page.locator('#pageTitle').innerText(),/近3天/);
  assert.equal(await page.locator('#sidebar .sidebar-item.active').getAttribute('data-tab'),'strong');
  assert.equal(await page.locator('#sidebar .sidebar-item.active').getAttribute('aria-current'),'page');
  const [legacyRefresh]=await Promise.all([
   page.waitForRequest(req=>new URL(req.url()).pathname==='/api/hot-data/multi-day'),
   page.locator('.header .btn-refresh').click()
  ]);
  assert.equal(new URL(legacyRefresh.url()).searchParams.get('stat_date'),day);
  assert.equal(new URL(legacyRefresh.url()).searchParams.get('days'),'3');
  assert.equal(await page.locator('#tab-multi3').evaluate(node=>node.classList.contains('active')),true);
  // The trading workbench links to the dashboard without losing the date.
  await page.goto(base+'/?tab=trading-day&trade_date='+day);await page.waitForSelector('#tab-trading-day.active .td-hero');
  await page.locator('[data-td-tab="workbench"]').click();await page.waitForSelector('#tab-workbench.active .market-home');
  assert.equal(await page.locator('#sidebar [data-tab="workbench"]').count(),1);
  const [overviewRefresh]=await Promise.all([
   page.waitForRequest(req=>new URL(req.url()).pathname==='/api/monitor/data'),
   page.locator('.header .btn-refresh').click()
  ]);
  assert.equal(new URL(overviewRefresh.url()).searchParams.get('date'),day);
  await page.locator('#datePicker').fill('2026-09-17');
  const [overviewDateChange]=await Promise.all([
   page.waitForRequest(req=>new URL(req.url()).pathname==='/api/monitor/data' && new URL(req.url()).searchParams.get('date')==='2026-09-17'),
   page.locator('#datePicker').dispatchEvent('change')
  ]);
  assert.equal(new URL(overviewDateChange.url()).searchParams.get('date'),'2026-09-17');
  await page.waitForFunction(()=>document.querySelector('#tab-workbench .mh-head .mh-meta')?.textContent.includes('2026-09-17'));
  assert.match(page.url(),/trade_date=2026-09-17/);
  await page.goBack();await page.waitForSelector('#tab-trading-day.active .td-hero');
  assert.equal(await page.locator('.sidebar-item.active').getAttribute('data-tab'),'trading-day');
  await page.goForward();await page.waitForSelector('#tab-workbench.active .market-home');
  await page.reload();await page.waitForSelector('#tab-workbench.active .market-home');
  await page.goBack();await page.waitForSelector('#tab-trading-day.active .td-hero');
  await page.locator('#datePicker').fill('2026-09-17');await page.locator('#datePicker').dispatchEvent('change');
  await page.waitForFunction(()=>document.querySelector('#tab-trading-day .td-pagehead').textContent.includes('2026-09-17'));
  assert.match(page.url(),/trade_date=2026-09-17/);
  await assertNavigation(page,navigation);
  assert.equal(await page.locator('#datePicker').inputValue(),'2026-09-17');
  await page.reload();await page.waitForSelector('#tab-trading-day.active .td-hero');await assertNavigation(page,navigation);
  // A direct legacy route still selects its parent; the bare URL is always the dashboard.
  await page.evaluate(()=>{localStorage.setItem('probiga_layout','old');localStorage.setItem('probiga_current_tab','capital-rt');});
  await page.goto(base+'/?tab=capital-rt');await page.waitForSelector('#tab-capital-rt.active #rtCode');await assertNavigation(page,navigation);
  assert.equal(await page.locator('.sidebar-item.active').getAttribute('data-tab'),'capital');
  await page.reload();await page.waitForSelector('#tab-capital-rt.active #rtCode');await assertNavigation(page,navigation);
  await page.goto(base+'/');await page.waitForSelector('#tab-workbench.active .market-home');
  assert.equal(await page.locator('.sidebar-item.active').getAttribute('data-tab'),'workbench');
  await page.goto(base+'/?tab=market-context&trade_date='+day);await page.waitForSelector('#tab-market-context.active .mw-hero');
  for(const tab of ['multi3','sector-rotation']){
   await page.goto(base+'/?tab='+tab+'&trade_date='+day);await page.waitForSelector('#tab-'+tab+'.active');
   for(const width of [769,800,900]){
    await page.setViewportSize({width,height:1000});
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),tab+' overflow '+width);
   }
  }
  await page.goto(base+'/?tab=trading-day&trade_date='+day);await page.waitForSelector('#tab-trading-day.active .td-hero');
  for(const width of [1280,1024,900,769]){
   await page.setViewportSize({width,height:1000});
   assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'desktop app overflow '+width);
  }
  await page.setViewportSize({width:1280,height:1000});
  await page.screenshot({path:path.join(out,'navigation-desktop.png'),fullPage:true});
  for(const width of [768,760,736,360,320]){
   await page.setViewportSize({width,height:1000});
   assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'app overflow '+width);
   await page.locator('#navigationToggle').click();
   assert.equal(await page.locator('#navigationToggle').getAttribute('aria-expanded'),'true');
   await page.waitForFunction(()=>Math.round(document.querySelector('#sidebar').getBoundingClientRect().left)===0);
   assert.ok(await page.locator('#sidebar').evaluate(node=>Math.abs(node.getBoundingClientRect().left)<1));
   if(width===320)await page.screenshot({path:path.join(out,'navigation-mobile-open.png'),fullPage:true});
   await page.locator('#sidebar [data-tab="trading-day"]').click();
   assert.equal(await page.locator('#navigationToggle').getAttribute('aria-expanded'),'false');
   await page.waitForFunction(()=>document.querySelector('#sidebar').getBoundingClientRect().right<=1);
  }
  await page.screenshot({path:path.join(out,'navigation-mobile.png'),fullPage:true});
  assert.deepEqual(errors,[]);console.log(JSON.stringify({ok:true,writes,screenshots:out}));
 } finally {await browser.close();server.close();}
})().catch(error=>{console.error(error);server.close();process.exitCode=1;});
