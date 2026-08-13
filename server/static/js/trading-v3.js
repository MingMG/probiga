(function(){
  'use strict';
  var state={readiness:{},overview:{},forecasts:[],unifiedRun:{},oversold:[],targets:[],validation:null,recall:null,learning:{},account:{},paperLedger:{},positions:[],orders:[],runs:[],intraday:{},intradayRadar:{},hypotheses:[],hypothesisDetail:null,tasks:[],dataEvidence:{},candidatePage:1,actionMessage:''};
  var titles={overview:'执行总览',hypotheses:'交易假设',candidates:'候选与拒绝',intraday:'盘中机会',portfolio:'目标组合',positions:'当前持仓',orders:'模拟订单',validation:'回测验收',missed:'漏抓复盘',evidence:'数据与系统'};
  var strategyNames={theme_diffusion:'板块扩散预热',low_base_ignition:'板块点火预判',right_side_trend:'右侧趋势启动',event_drift:'事件后漂移',quality_momentum:'质量与动量',oversold_reversal:'超跌抄底实验',intraday_surprise:'盘中超预期',weak_market_structural_mainline:'弱市结构性主线',ai_application_research:'AI应用纸面研究',robotics_research:'机器人纸面研究',paper_discovery:'模拟试错'};
  var statusNames={VALIDATED_POSITIVE:'扣费后正期望，允许进入组合',PAPER_DISCOVERY_CANDIDATE:'触发小仓模拟试错',LEFT_SIDE_PREPARE:'已进入抄底准备区，暂不买',RESEARCH_ONLY_UNCALIBRATED:'没有样本外校准，只记录不交易',RESEARCH_ONLY_MODEL_VERSION_MISMATCH:'旧公式校准已隔离，仅允许模拟盘重新验证',RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED:'高分组反而亏损，排序失真，禁止自动买入',RESEARCH_ONLY_PROFIT_GATE_FAILED:'样本外收益闸门失败，只研究',INSUFFICIENT_DATA:'所需事实不完整，不计算',SETUP_NOT_READY:'板块、趋势和入场位置尚未同时确认',WEAK_MARKET_THEME_WATCH:'弱市结构性机会，进入观察池但暂不自动买入',MARKET_REGIME_BLOCKED:'大盘偏弱且细分板块扩散或个股领导力不足',NO_ACTIVE_OOS_CALIBRATION:'没有策略通过样本外校准',NO_COMPATIBLE_OOS_CALIBRATION:'当前没有与冻结公式匹配且排序可信的正期望模型',V3_SCHEMA_INCOMPLETE:'V3独立账本尚未完成迁移',PAPER_ACTIVE:'模拟盘已启用',PAPER_TRIAL:'模拟观察',PAPER_DISCOVERY_READY:'模拟盘小仓试错已就绪',READY_WITH_PAPER_DISCOVERY:'正式组合与模拟试错并行',RESEARCH:'研究验证中，不会发出交易指令',RESEARCH_ONLY:'仅研究，不执行交易',QUEUED:'等待模拟撮合',CREATED:'已创建',PENDING:'等待发送',NOT_REQUESTED:'页面查询未触发推送',SENT:'已发送到早报机器人',WAITING:'等待条件满足',RUNNING:'执行中',SUCCESS:'成功',FAILED:'失败',SKIPPED:'已跳过',TIMEOUT:'已超时',STOPPED:'已停止',ENABLED:'已启用',DISABLED:'已停用',FILLED:'已成交',PARTIALLY_FILLED:'部分成交',CANCELLED:'已取消',EXPIRED:'已过有效期',REJECTED:'已拒绝',RISK_REJECTED:'风控拒绝',RISK_APPROVED:'风控通过',HOLDING:'持仓中',holding:'持仓中',ACTIVE:'已激活',VALID_STRONG:'强势有效',WATCH:'观察中',PREPARE:'准备中',TRIGGER_READY:'等待触发',INVALIDATED:'已失效',LIVE:'实时有效',FRESH:'数据新鲜',STALE:'数据已过期',STALE_DURING_SESSION:'盘中数据已过期',UNAVAILABLE:'暂不可用',MARKET_CLOSED:'已收盘',HISTORICAL_SNAPSHOT:'历史快照',DATA_BLOCKED:'数据未达标',PASS:'通过',WARN:'警告',BLOCK:'阻断',ATTENTION:'需要关注',COVERED:'已有覆盖',HIGH_SCORE_UNSELECTED:'高分但未入选',BELOW_ALERT:'未达预警线',V3_PAPER_DISCOVERY:'模拟盘小仓前向验证',V3_VALIDATED_POSITIVE:'正期望组合模拟委托',V3_PROFIT_GATE_MIGRATION:'V3正期望闸门启用，旧买单已撤销',EXIT_PENDING_T1:'趋势已失效，T+1 后立即卖出'};
  var reasonNames={MARKET_NOT_CONFIRMED:'实时行情覆盖率或市场确认未通过，只提醒不买入',OUTSIDE_DAILY_ENTRY_WINDOW:'已过日级候选的盘中开仓窗口，只观察',DUPLICATE_ENTRY_SAME_DAY_BLOCKED:'当天已处理过同一证券，不重复开仓',DATA_QUALITY_BLOCK:'实时数据质量未通过',RISK_REJECTED:'模拟风控拒绝',ACTIVATE_PROBE:'盘中超预期，小仓试单',ACTIVATE_SUBSTITUTE:'龙头无法成交，选择同题材替补',ACTIVATE_REVERSAL_PROBE:'水下修复并放量，小仓试单',ACTIVATE_VOLUME_PROBE:'突然爆量上攻，小仓试单',WATCH:'观察，不下单','paper order expired before fill':'模拟订单在成交前已过有效期','event driven simulation fill':'事件驱动模拟成交','test fill':'测试模拟成交'};
  var hypothesisNames={ACTIVE:'已经触发',TRIGGER_READY:'等待价格确认',PREPARE:'重点准备',WATCH:'普通观察',WEAKEN:'正在转弱',INVALIDATED:'已经失效'};
  var probabilityNames={OOS_CALIBRATED:'样本外校准概率',PAPER_FORWARD_PRIOR:'模拟前向先验',STRUCTURED_RESEARCH_PRIOR:'结构化研究先验',INTRADAY_STRUCTURED_PRIOR:'盘中结构化先验',REGIME_MIXTURE:'市场状态混合概率'};
  var actionNames={BUY_OR_HOLD:'模拟买入或继续持有',PAPER_PROBE:'模拟小仓试单',PAPER_PROBE_IF_CONFIRMED:'确认后模拟小仓试单',PAPER_ORDER_CREATED:'模拟订单已创建',ALERT_ONLY:'只提醒，不下单',WAIT_INTRADAY_CONFIRM:'等待盘中确认',WAIT_PRICE_CONFIRM:'等待价格确认',WATCH_CLOSELY:'重点观察',NO_TRADE:'不交易',NO_NEW_BUY:'禁止新买',EXIT_OR_AVOID:'退出或回避',CONTROLLED_RISK_ON:'控制仓位参与',SELECTIVE_PROBES:'只做精选试单',CASH_FIRST:'现金优先'};
  function el(id){return document.getElementById(id)}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function num(v,d){var n=Number(v);return Number.isFinite(n)?n.toFixed(d==null?2:d):'—'}
  function pct(v,d){var n=Number(v);return Number.isFinite(n)?n.toFixed(d==null?2:d)+'%':'—'}
  function ratio(v){var n=Number(v);return Number.isFinite(n)?n.toFixed(2):'—'}
  function money(v){if(v==null||v==='')return '—';var n=Number(v);return Number.isFinite(n)?'¥'+n.toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:2}):'—'}
  function firstNumber(values){for(var i=0;i<values.length;i+=1){if(values[i]!=null&&values[i]!==''&&Number.isFinite(Number(values[i])))return Number(values[i])}return null}
  function unwrap(v){return v&&v.data}
  function fetchJson(path){return fetch(path,{headers:{Accept:'application/json'}}).then(function(r){if(!r.ok)throw new Error(r.status+' '+path);return r.json()})}
  function postJson(path,body){var options={method:'POST',headers:{Accept:'application/json'}};if(body!==undefined){options.headers['Content-Type']='application/json';options.body=JSON.stringify(body)}return fetch(path,options).then(function(r){if(!r.ok)throw new Error(r.status+' '+path);return r.json()})}
  function api3(path){return fetchJson('/api/v3'+path)}
  function api2(path){return fetchJson('/api/v2'+path)}
  function security(code,name){code=String(code||'').split('.')[0];name=String(name||code||'—');return '<span class="security"><a class="name" href="#" data-stock-code="'+esc(code)+'" data-stock-name="'+esc(name)+'">'+esc(name)+'</a><a class="code" href="#" data-stock-code="'+esc(code)+'" data-stock-name="'+esc(name)+'">'+esc(code)+'</a></span>'}
  function requestStockChart(code,name){
    code=String(code||'').split('.')[0];name=String(name||code||'');
    if(window.parent&&window.parent!==window){
      var targetOrigin='*';try{var origin=new URL(document.referrer).origin;if(origin&&origin!=='null')targetOrigin=origin}catch(ignore){}
      window.parent.postMessage({type:'probiga-open-kline',stock_code:code,short_name:name},targetOrigin);return
    }
    var market=code.indexOf('6')===0?'sh':'sz';window.open('https://quote.eastmoney.com/'+market+code+'.html#fullScreenChart','_blank','noopener')
  }
  function empty(cols,text){return '<tr><td class="empty" colspan="'+cols+'">'+esc(text)+'</td></tr>'}
  function fact(label,value){return '<div><span>'+esc(label)+'</span><strong>'+esc(value==null?'—':value)+'</strong></div>'}
  function pnlClass(value){var n=Number(value);return Number.isFinite(n)?(n>0?'pnl-gain':n<0?'pnl-loss':''):''}
  function pnlFact(label,value){return '<div><span>'+esc(label)+'</span><strong class="'+pnlClass(value)+'">'+esc(money(value))+'</strong></div>'}
  function status(v){var raw=String(v||'');if(!raw)return '—';if(statusNames[raw])return statusNames[raw];if(reasonNames[raw])return reasonNames[raw];if(/[\u3400-\u9fff]/.test(raw))return raw;return '其他状态'}
  function strategy(v){return strategyNames[String(v||'')]||String(v||'—')}
  function isDiscoveryTarget(x){return (x.strategy_keys||[]).indexOf('paper_discovery')>=0||String(x.reason||'').indexOf('PAPER_DISCOVERY')===0}
  function isExecutableTarget(x){return x&&x.new_buy_eligible===true}
  function hasCalibratedExpectation(x){
    var uncalibrated=['RESEARCH_ONLY_UNCALIBRATED','PAPER_DISCOVERY_CANDIDATE','LEFT_SIDE_PREPARE','WEAK_MARKET_THEME_WATCH','SETUP_NOT_READY','MARKET_REGIME_BLOCKED','INSUFFICIENT_DATA'];
    return uncalibrated.indexOf(String(x.forecast_status||''))<0&&Number(x.sample_count||0)>0&&Number.isFinite(Number(x.expected_return_net_pct));
  }
  function themeText(x){
    x=x||{};var features=x.features||{},groups=features.paper_research_groups||[],names=features.theme_names||x.theme_codes||[],values=[];
    groups.forEach(function(v){var label='【'+String(v)+'】';if(values.indexOf(label)<0)values.push(label)});
    [x.theme_code||features.theme_name].concat(names||[]).forEach(function(v){v=String(v||'');if(v&&values.indexOf(v)<0)values.push(v)});
    return values.join(' / ')||'未归属主题';
  }
  function reason(v){return reasonNames[String(v||'')]||status(v)}
  function hypothesisName(v){return hypothesisNames[String(v||'')]||String(v||'—')}
  function probabilityName(v){return probabilityNames[String(v||'')]||String(v||'—')}
  function actionName(v){return actionNames[String(v||'')]||String(v||'—')}
  function sourceName(v){var k=String(v||'').toUpperCase();if(k==='GJ_BIG_QMT_INNER')return '国金QMT主源';if(k==='PUBLIC_QUOTE_QUORUM_V1')return '公共双源替补（新浪+腾讯）';if(k==='UNATTESTED_MINUTE_SOURCE')return '未补证分钟数据（禁止交易）';if(k==='SM_STOCK_CURRENT')return '生产行情快照';if(k==='TENCENT')return '腾讯行情';if(k==='SINA')return '新浪行情';if(k==='EASTMONEY')return '东方财富行情';return v&&/[\u3400-\u9fff]/.test(String(v))?v:'其他数据源'}
  function snapshotAge(observed){if(!observed)return null;var t=new Date(String(observed).replace(' ','T')),seconds=(Date.now()-t.getTime())/1000;return Number.isFinite(seconds)?Math.max(0,seconds):null}
  function ageText(seconds){if(seconds==null)return '无时间';if(seconds<60)return Math.round(seconds)+'秒前';if(seconds<3600)return Math.round(seconds/60)+'分钟前';return (seconds/3600).toFixed(1)+'小时前'}
  function isTradingNow(){var d=new Date(),day=d.getDay(),n=d.getHours()*100+d.getMinutes();return day>0&&day<6&&((n>=931&&n<=1130)||(n>=1301&&n<=1500))}
  function task(type){return state.tasks.find(function(x){return x.task_type===type})||{}}
  function requestedView(){
    var view='overview';
    try{view=(window.frameElement&&window.frameElement.dataset.pendingView)||view}catch(ignore){}
    if(location.hash&&titles[location.hash.slice(1)])view=location.hash.slice(1);
    return titles[view]?view:'overview';
  }
  function activateView(view){
    view=titles[view]?view:'overview';
    document.documentElement.dataset.embeddedView=view;
    document.querySelectorAll('.nav').forEach(function(x){x.classList.toggle('active',x.dataset.view===view)});
    document.querySelectorAll('.view').forEach(function(x){x.classList.toggle('active',x.id==='view-'+view)});
    el('pageTitle').textContent=titles[view];
  }
  function notifyParentResize(){
    if(window.parent===window)return;
    var height=Math.max(document.body.scrollHeight,document.documentElement.scrollHeight);
    var targetOrigin='*';
    try{var referrerOrigin=new URL(document.referrer).origin;if(referrerOrigin&&referrerOrigin!=='null')targetOrigin=referrerOrigin}catch(ignore){}
    try{window.parent.postMessage({type:'probiga-trading-v3-resize',height:height},targetOrigin)}catch(ignore){}
  }
  function load(){
    el('updatedAt').textContent='读取中';
    var view=requestedView(),requests=[
      ['readiness',api3('/readiness'),{}],
      ['overview',api3('/overview'+(view==='overview'?'':'?compact=true')),{}],
      ['account',api2('/accounts/paper-main-v2'),{}],
      ['paperLedger',api3('/paper-ledger?account_id=paper-main-v2&limit=200'),{}],
      ['tasks',fetchJson('/api/scheduler/tasks'),[]]
    ];
    function add(key,promise,fallback){requests.push([key,promise,fallback])}
    if(view==='overview'){add('forecasts',api3('/forecasts/latest?limit=200'),[]);add('oversold',api3('/forecasts/latest?strategy_key=oversold_reversal&limit=200'),[]);add('targets',api3('/portfolio/latest'),[])}
    if(view==='hypotheses')add('hypotheses',api3('/hypotheses/latest?limit=300'),[]);
    if(view==='candidates'){
      add('forecasts',api3('/forecasts/latest?limit=200'),[]);
      add('runs',api3('/decision-runs?limit=100'),[]);
      add('unifiedRun',postJson('/api/screener/run',{preset:'intraday_sector',as_of_date:'',universe:'market',top:100,filters:{exclude_st:true}}).then(function(payload){return {data:payload}}),{status:'error',error:'生产统一候选接口读取失败',data:[],stats:{}})
    }
    if(view==='intraday'){
      add('intraday',api2('/accounts/paper-main-v2/intraday?limit=200'),{});
      add('intradayRadar',postJson('/api/screener/run',{preset:'intraday_sector',as_of_date:'',universe:'market',top:200,filters:{exclude_st:true}}).then(function(payload){return {data:payload}}),{status:'degraded',freshness:'unavailable',data:[],error:'盘中雷达读取失败'});
    }
    if(view==='portfolio')add('targets',api3('/portfolio/latest'),[]);
    if(view==='positions')add('targets',api3('/portfolio/latest'),[]);
    if(view==='validation'){add('validation',api3('/validation/latest'),null);add('dataEvidence',api2('/system/data-evidence'),{})}
    if(view==='missed'){add('recall',api3('/opportunity-recall/latest'),null);add('learning',api3('/learning/oversold_reversal'),{})}
    if(view==='evidence')add('dataEvidence',api2('/system/data-evidence'),{});
    return Promise.all(requests.map(function(item){return item[1].then(function(payload){var value=unwrap(payload);return [item[0],value==null?item[2]:value]}).catch(function(){return [item[0],item[2]]})})).then(function(v){
      v.forEach(function(item){state[item[0]]=item[1]});
      var ledger=state.paperLedger||{};
      if(ledger.account)state.account=ledger.account;
      if(view==='positions')state.positions=ledger.positions||[];
      if(view==='orders')state.orders=ledger.orders||[];
      el('updatedAt').textContent='刷新于 '+new Date().toLocaleTimeString('zh-CN',{hour12:false});
      renderAll();activateView(view);notifyParentResize();
    }).catch(function(err){el('updatedAt').textContent='读取失败';el('heroTitle').textContent='接口读取失败';el('heroReason').textContent=err.message;el('hero').classList.add('blocked')})
  }
  function renderAll(){renderChrome();renderActions();renderOverview();renderThemeAudit();renderHypotheses();renderCandidates();renderIntraday();renderPortfolio();renderPositions();renderOrders();renderValidation();renderRecall();renderLearning();renderEvidence()}
  function renderActions(){
    var daily=task('trading_v3_close_decision'),live=task('trading_v2_intraday_activation');
    el('runDaily').disabled=daily.last_run_status==='running';el('runIntraday').disabled=live.last_run_status==='running';
    if(state.actionMessage){el('actionStatus').textContent=state.actionMessage;return}
    function brief(row,label){if(!row.id)return label+'：任务未注册';var s=row.last_run_status==='success'?'成功':row.last_run_status==='running'?'执行中':row.last_run_status==='failed'?'失败':row.last_run_status||'未执行';return label+'：'+s+' '+(row.last_run_at||'')}
    el('actionStatus').textContent=brief(daily,'日级')+'；'+brief(live,'盘中');
  }
  function renderChrome(){
    var run=state.overview.run||{},portfolio=run.portfolio||{},regime=run.regime||{},account=state.account||{},equity=account.latest_equity||{},ledgerSummary=(state.paperLedger||{}).summary||{};
    var ready=state.readiness.paper_ready,targets=portfolio.targets||state.targets||[],actionableTargets=targets.filter(isExecutableTarget);
    var discoveryTargets=actionableTargets.filter(isDiscoveryTarget);
    var formalTargets=actionableTargets.filter(function(x){return discoveryTargets.indexOf(x)<0});
    var displayEquity=firstNumber([ledgerSummary.display_total_equity,ledgerSummary.canonical_total_equity,equity.total_equity,account.cash_balance]);
    var displayCash=firstNumber([ledgerSummary.display_cash_balance,ledgerSummary.canonical_cash_balance,equity.cash_balance,account.cash_balance]);
    var displayPnl=firstNumber([ledgerSummary.total_unrealized_pnl])||0;
    var accountScope=String(ledgerSummary.display_account_scope||'V2_CANONICAL');
    var scopeText=accountScope==='LEGACY_EVENT_SIM_ACTIVE'?'当前持仓来自事件模拟账本；空仓 V2 账本未重复叠加':accountScope==='MERGED_LEDGER'?'V2 与事件模拟账本均有持仓，按独立账户合计':'当前持仓来自 V2 主模拟账本';
    var positionCount=Number(ledgerSummary.position_count||0),positionLots=Number(ledgerSummary.position_lot_count||0);
    el('regime').textContent=run.dominant_regime||regime.dominant_state||'尚无决策';
    el('riskCap').textContent='风险仓位上限 '+pct(Number(run.risk_asset_cap||regime.risk_asset_cap||0)*100,1);
    el('equity').textContent=money(displayEquity);
    el('cash').textContent=money(displayCash);
    el('equitySource').textContent=scopeText;
    el('cashSource').textContent='待买资金 '+money(ledgerSummary.legacy_pending_buy_amount||0)+'；已实现盈亏 '+money(ledgerSummary.legacy_realized_pnl||0);
    el('unrealizedPnl').textContent=(displayPnl>0?'+':'')+money(displayPnl);
    el('unrealizedPnl').className=pnlClass(displayPnl);
    el('marketValue').textContent='最新持仓市值 '+money(ledgerSummary.current_market_value||0);
    var limits=state.readiness.portfolio_limits||{};
    el('positionCount').textContent=String(positionCount)+' 只'+(positionLots!==positionCount?' / '+String(positionLots)+' 笔':'');
    el('targetPolicy').textContent='本次决策目标 '+String(targets.length)+' 只；其中可执行 '+String(actionableTargets.length)+' 只；正式上限 '+String(limits.maximum_positions||limits.maximum_live_positions||'—')+' 只';
    if(!run.run_uid){el('heroTitle').textContent='V3 尚未产生首个决策';el('heroReason').textContent='完成样本外验收后才会启用模拟组合。';el('hero').classList.add('blocked')}
    else if(ready&&formalTargets.length){el('heroTitle').textContent='发现扣费后正期望目标，进入模拟组合';el('heroReason').textContent=discoveryTargets.length?'正式目标通过全部闸门；同时有左侧实验小仓收集前向证据。':'目标已经通过样本外、成本、仓位、主题和开放风险约束。';el('hero').classList.remove('blocked')}
    else if(ready&&discoveryTargets.length){el('heroTitle').textContent='左侧实验触发，进入模拟盘小仓试错';el('heroReason').textContent='这不是“已证明能赚钱”的推荐；系统会动态止损，并把成功与失败都写入下一版校准样本。';el('hero').classList.remove('blocked')}
    else if(positionCount){el('heroTitle').textContent='本次没有新增目标，继续管理现有 '+String(positionCount)+' 只持仓';el('heroReason').textContent='现有持仓和盈亏继续按模拟账本管理；本次 V3 决策没有新增可执行买入目标。';el('hero').classList.add('blocked')}
    else{el('heroTitle').textContent='当前没有值得出手的组合，保持现金';el('heroReason').textContent=ready?'没有股票同时通过净期望和组合约束，空仓不是故障。':'新公式尚未形成排序可信的正期望模型，旧目标已隔离，自动模拟买入暂停。';el('hero').classList.add('blocked')}
  }
  function renderOverview(){
    var run=state.overview.run||{},portfolio=run.portfolio||{},regime=run.regime||{},rawTargets=portfolio.targets||state.targets||[],targets=state.readiness.paper_ready?rawTargets:[];
    var discoveryTargets=targets.filter(isDiscoveryTarget),uncalibratedOnly=targets.length>0&&discoveryTargets.length===targets.length;
    el('decisionStatus').textContent=portfolio.status||'尚无组合';el('decisionStatus').className='badge '+(targets.length?'safe':'warning');
    el('decisionFacts').innerHTML=[
      fact('决策日期',run.trade_date||'—'),fact('市场状态',run.dominant_regime||regime.dominant_state||'—'),fact('目标风险资产',pct(Number(portfolio.target_risk_asset_weight||0)*100,1)),fact('V3 本次决策预算现金',money(portfolio.target_cash)),fact('组合预期收益',uncalibratedOnly?'未校准（仅模拟试错）':pct(portfolio.expected_portfolio_return_pct)),fact('最坏开放风险',money(portfolio.worst_case_loss_cny))
    ].join('');
    var counts={};state.forecasts.forEach(function(x){counts[x.strategy_key]=(counts[x.strategy_key]||0)+(x.forecast_status==='VALIDATED_POSITIVE'?1:0)});
    el('sleeveCards').innerHTML=Object.keys(strategyNames).map(function(k){var calibrated=(state.readiness.active_calibrated_sleeves||[]).indexOf(k)>=0;return '<div class="card"><strong>'+esc(strategy(k))+'</strong><span>'+esc(calibrated?'已有样本外校准':k==='oversold_reversal'?'独立模拟试错并积累前向样本':'未校准时只观察，不借用别的策略结论')+'</span><em>'+String(counts[k]||0)+' 个通过</em></div>'}).join('');
    var oversold=(state.oversold||[]).filter(function(x){return x.forecast_status==='LEFT_SIDE_PREPARE'||x.forecast_status==='PAPER_DISCOVERY_CANDIDATE'}).sort(function(a,b){var pa=a.forecast_status==='PAPER_DISCOVERY_CANDIDATE'?0:1,pb=b.forecast_status==='PAPER_DISCOVERY_CANDIDATE'?0:1;return pa-pb||Number(b.raw_score||0)-Number(a.raw_score||0)}).slice(0,20);
    var rejected=((run.portfolio||{}).rejected||[]),targetCodes={};targets.forEach(function(x){targetCodes[String(x.stock_code).slice(0,6)]=true});var rejectionByCode={};rejected.forEach(function(x){rejectionByCode[String(x.stock_code).slice(0,6)]=x});
    el('oversoldRows').innerHTML=oversold.length?oversold.map(function(x){var triggered=x.forecast_status==='PAPER_DISCOVERY_CANDIDATE',code=String(x.stock_code).slice(0,6),reject=rejectionByCode[code],selected=targetCodes[code],action=triggered?(selected?'已进入纸面研究组合':reject?'未入选：'+(reject.reason||reject.reason_code):'等待组合分配'):'准备观察，不买';return '<tr><td>'+security(x.stock_code,x.short_name)+'</td><td>'+esc(themeText(x))+'</td><td>'+esc(status(x.forecast_status))+'</td><td class="'+(selected?'safe-text':'')+'">'+esc(action)+'</td><td>'+num(x.raw_score,3)+'</td><td>'+pct(x.initial_stop_pct)+'</td><td class="reason">'+esc((x.reasons||[]).join('；'))+'</td></tr>'}).join(''):empty(7,'当前没有股票进入左侧抄底准备区；系统仍会保留全部未触发记录供后续反事实复盘');
    el('overviewTargets').innerHTML=targets.length?targets.map(function(x){var calibrated=!isDiscoveryTarget(x);return '<tr><td>'+security(x.stock_code,x.stock_name||x.short_name)+'</td><td>'+pct(Number(x.target_weight)*100,1)+'</td><td>'+num(x.target_quantity,0)+'</td><td class="'+(calibrated?'safe-text':'warning-text')+'">'+(calibrated?pct(x.expected_return_net_pct):'未校准')+'</td><td>'+(calibrated?pct(x.conservative_return_pct):'未校准')+'</td><td>'+pct(x.estimated_roundtrip_cost_pct)+'</td><td class="reason">'+esc(x.reason||'—')+'</td></tr>'}).join(''):empty(7,'本次没有股票通过正式闸门，也没有左侧实验触发模拟试错');
  }
  function renderThemeAudit(){
    var run=state.overview.run||{},audit=(run.portfolio||{}).opportunity_audit||{},dynamic=audit.dynamic_theme_radar||[],groups=audit.research_groups||[],themes=audit.opportunity_themes||[],warnings=audit.warnings||[];
    function diversifyDynamic(rows,limit){var selected=[],deferred=[],leaders={};(rows||[]).forEach(function(x){var code=String((x.top_signal||{}).stock_code||'');if(code&&leaders[code])deferred.push(x);else{selected.push(x);if(code)leaders[code]=true}});return selected.concat(deferred).slice(0,limit)}
    if(audit.research_group_mode!=='DYNAMIC_EACH_DECISION'&&dynamic.length){groups=diversifyDynamic(dynamic,20).map(function(x){var score=Number((x.top_signal||{}).score||0),selected=Number(x.selected_count||0),candidates=Number(x.candidate_count||0);return {group:x.theme,source:'DYNAMIC_ALL_MARKET_THEME',universe_stock_count:x.universe_stock_count,forecast_count:x.forecast_count,candidate_count:candidates,selected_count:selected,top_signal:x.top_signal,top_candidate:x.top_candidate,status:selected?'COVERED':score>=Number(audit.minimum_alert_score||0.82)?'HIGH_SCORE_UNSELECTED':candidates?'BELOW_ALERT':'NO_CANDIDATE'}})}
    var warningNames={UNEXPLAINED_CANDIDATE_OMISSION:'存在无解释落选',CANDIDATE_THEME_MISSING:'存在主题标签缺失',TARGET_THEME_CONCENTRATION:'入选目标共享主题较多'};
    var label=audit.status==='PASS'?'覆盖正常':audit.status==='ATTENTION'?'需要主动复核':'等待审计';
    el('themeAuditStatus').textContent=label;el('themeAuditStatus').className='badge '+(audit.status==='PASS'?'safe':audit.status==='ATTENTION'?'warning':'');
    el('themeAuditFacts').innerHTML=[
      fact('全量覆盖',String(audit.universe_stock_count||0)+' 只 / '+String(audit.forecast_count||0)+' 条策略预测'),
      fact('纸面候选',String(audit.candidate_count||0)+' 只'),
      fact('已入选 / 已解释落选',String(audit.selected_count||0)+' / '+String(audit.rejected_count||0)),
      fact('无原因落选',String(audit.unexplained_unselected_count||0)+' 只'),
      fact('主题标签缺失',String(audit.missing_theme_count||0)+' 只'),
      fact('研究方向模式','随每次决策动态更新'),
      fact('共同集中主题',(audit.selected_concentration_themes||[]).join(' / ')||'无'),
      fact('主动预警',warnings.map(function(x){if(String(x).indexOf('HIGH_SCORE_RESEARCH_GROUP_UNSELECTED:')===0)return String(x).split(':').slice(1).join(':')+'高分候选全部落选';return warningNames[x]||x}).join('；')||'无')
    ].join('');
    el('researchGroupRows').innerHTML=groups.length?groups.map(function(x){var top=x.top_candidate||{},signal=x.top_signal||{},reason=x.status==='COVERED'?'已有候选入选':x.status==='HIGH_SCORE_UNSELECTED'?'高分方向尚未入选：'+(top.reason||top.reason_code||'当前未形成纸面候选，需复核信号门槛'):x.status==='BELOW_ALERT'?'有候选但未达到预警分':'全市场已覆盖，当前未形成纸面候选';return '<tr><td>'+esc(x.group||x.theme)+'</td><td>'+num(x.universe_stock_count,0)+'</td><td>'+num(x.forecast_count,0)+'</td><td>'+num(x.candidate_count,0)+'</td><td>'+num(x.selected_count,0)+'</td><td>'+(signal.stock_code?security(signal.stock_code,signal.short_name)+'<span>'+esc(strategy(signal.strategy_key))+' · '+num(signal.score,3)+' · '+esc(status(signal.status))+'</span>':'—')+'</td><td class="reason '+(x.status==='HIGH_SCORE_UNSELECTED'?'warning-text':'')+'">'+esc(reason)+'</td></tr>'}).join(''):empty(7,'当前决策快照没有可审计的动态主题');
    el('opportunityThemeRows').innerHTML=themes.length?themes.slice(0,12).map(function(x){var top=x.top_candidate||{};return '<tr><td>'+esc(x.theme)+'</td><td>'+num(x.candidate_count,0)+'</td><td>'+num(x.selected_count,0)+'</td><td>'+(top.stock_code?security(top.stock_code,top.short_name)+'<span>'+num(top.score,3)+'</span>':'—')+'</td></tr>'}).join(''):empty(4,'当前没有达到纸面候选门槛的主题');
  }
  function evidenceText(items){return (items||[]).slice(0,2).join('；')||'尚无新增证据'}
  function renderHypotheses(){
    var market=state.hypotheses.find(function(x){return x.scope_type==='MARKET'})||{};
    var marketState=market.state?hypothesisName(market.state):'等待日级决策';
    el('marketHypothesisState').textContent=marketState;
    el('marketHypothesisState').className='badge '+(market.state==='ACTIVE'?'safe':market.state==='INVALIDATED'?'danger':'warning');
    el('marketHypothesisFacts').innerHTML=market.hypothesis_id?[
      fact('当前结论',market.thesis||'—'),
      fact('当前概率',pct(Number(market.probability||0)*100,1)),
      fact('概率性质',probabilityName(market.probability_kind)),
      fact('建议动作',actionName(market.proposed_action)),
      fact('最大风险仓位',pct(Number(market.max_position_weight||0)*100,1)),
      fact('最近证据',market.last_evidence_at||market.updated_at||market.feature_time||'—')
    ].join(''):fact('当前状态','尚未形成市场假设；请先执行日级选股');
    var rows=state.hypotheses.filter(function(x){return x.scope_type==='STOCK'});
    el('hypothesisRows').innerHTML=rows.length?rows.map(function(x){
      var cls=x.state==='ACTIVE'?'safe-text':x.state==='INVALIDATED'?'danger-text':'';
      return '<tr><td>'+security(x.scope_code,x.scope_name)+'</td><td>'+esc((x.theme_code||'未归属主题')+' / '+(x.role||'观察'))+'</td><td class="'+cls+'">'+esc(hypothesisName(x.state))+'</td><td>'+pct(Number(x.probability||0)*100,1)+'</td><td>'+esc(probabilityName(x.probability_kind))+'</td><td>'+esc(actionName(x.proposed_action))+'</td><td class="reason">'+esc(evidenceText(x.supporting_evidence))+'</td><td class="reason">'+esc(evidenceText(x.opposing_evidence))+'</td><td>'+esc(x.last_evidence_at||x.updated_at||x.feature_time||'—')+'</td><td><button class="evidence-button" data-hypothesis-id="'+esc(x.hypothesis_id)+'">看证据</button></td></tr>'
    }).join(''):empty(10,'该条件下没有交易假设；盘中新发现的机会也会自动进入这里');
  }
  function showHypothesis(hypothesisId){
    api3('/hypotheses/'+encodeURIComponent(hypothesisId)+'/timeline?limit=500').then(function(payload){
      var detail=unwrap(payload)||{},h=detail.hypothesis||{},events=detail.events||[];
      state.hypothesisDetail=detail;
      el('hypothesisDetailTitle').textContent=(h.scope_name||h.scope_code||'交易假设')+' · 完整证据链';
      var blocks=[
        '<div class="hypothesis-summary"><div><span>核心假设</span><strong>'+esc(h.thesis||'—')+'</strong></div><div><span>反方假设</span><strong>'+esc(h.counter_thesis||'—')+'</strong></div><div><span>触发条件</span><strong>'+esc((h.triggers||[]).join('；')||'—')+'</strong></div><div><span>失效条件</span><strong>'+esc((h.invalidations||[]).join('；')||'—')+'</strong></div></div>'
      ];
      blocks.push(events.length?'<ol class="evidence-timeline">'+events.map(function(x){return '<li><time>'+esc(x.observed_at||'—')+'</time><div><strong>'+esc(hypothesisName(x.state_before))+' → '+esc(hypothesisName(x.state_after))+'</strong><span>'+pct(Number(x.probability_before||0)*100,1)+' → '+pct(Number(x.probability_after||0)*100,1)+'</span><p>'+esc(x.summary||'—')+'</p></div></li>'}).join('')+'</ol>':'<p class="note">当前只有日级初始假设，盘中证据尚未写入。</p>');
      el('hypothesisDetail').innerHTML=blocks.join('');
      el('hypothesisDetailPanel').scrollIntoView({behavior:'smooth',block:'start'});
    }).catch(function(err){el('hypothesisDetail').innerHTML='<p class="note">证据读取失败：'+esc(err.message)+'</p>'})
  }
  function reloadHypotheses(){
    var params=['limit=1000'],day=el('hypothesisDate').value,st=el('hypothesisState').value,q=el('hypothesisSearch').value.trim();
    if(day)params.push('trade_date='+encodeURIComponent(day));
    if(st)params.push('state='+encodeURIComponent(st));
    if(q)params.push('q='+encodeURIComponent(q));
    api3('/hypotheses/latest?'+params.join('&')).then(function(v){state.hypotheses=unwrap(v)||[];renderHypotheses()})
  }
  function renderUnifiedCandidates(){
    var run=state.unifiedRun||{},rows=Array.isArray(run.data)?run.data:[],query=el('unifiedCandidateSearch').value.trim().toLowerCase(),summary=((run.stats||{}).selector_summary)||{},coverage=summary.version_evidence_coverage_rate||{},activation=summary.version_activation_rate||{},grades=summary.grades||{};
    if(query)rows=rows.filter(function(row){return String(row.stock_code||'').toLowerCase().indexOf(query)>=0||String(row.stock_name||row.short_name||'').toLowerCase().indexOf(query)>=0||String(row.primary_concept||row.concept_name||'').toLowerCase().indexOf(query)>=0});
    var statusNode=el('unifiedCandidateStatus');
    if(run.status==='error'){
      statusNode.textContent='生产候选读取失败';statusNode.className='badge danger';
      el('unifiedCandidateSummary').innerHTML=fact('接口状态',run.error||'生产统一候选接口读取失败')+fact('执行边界','没有可靠数据时不显示候选，也不会自动下单');
      el('unifiedCandidateCards').innerHTML='<p class="empty">生产统一排序器暂时不可用，请查看“数据与系统”中的接口状态。</p>';
      el('unifiedCandidateRows').innerHTML=empty(12,'生产统一候选读取失败');
      return
    }
    statusNode.textContent=rows.length?'已加载 '+rows.length+' 只':'当前无生产候选';statusNode.className='badge '+(rows.length?'safe':'warning');
    function rateText(bucket,version){return pct(Number(bucket[version]||0)*100,1)}
    el('unifiedCandidateSummary').innerHTML=[
      fact('数据日期',run.session_date||run.data_date||'—'),fact('数据新鲜度',run.freshness==='live'?'实时':run.freshness==='historical_close'?'收盘复盘':run.freshness==='recovered'?'已从生产证据恢复':status(run.freshness)),fact('候选等级','A '+(grades.A||0)+' / B '+(grades.B||0)+' / C '+(grades.C||0)+' / 拒绝 '+(grades.REJECT||0)),
      fact('V4 证据 / 激活',rateText(coverage,'V4')+' / '+rateText(activation,'V4')),fact('V5 证据 / 激活',rateText(coverage,'V5')+' / '+rateText(activation,'V5')),fact('V6 证据 / 激活',rateText(coverage,'V6')+' / '+rateText(activation,'V6')),
      fact('组合可纳入',String(summary.portfolio_eligible_count||0)+' 只'),fact('历史批次',(run.run||{}).persisted?'已固定落库 · '+String((run.run||{}).run_uid||'').slice(0,8):'尚未确认落库'),fact('机器人交付',status((run.run||{}).push_status||(run.notification||{}).status||'PENDING')),fact('自动下单','固定关闭')
    ].join('');
    function versionCell(row,version){var item=((row.selector_versions||{})[version])||{},label=item.status?status(item.status):'无证据';return '<strong>'+num(item.score,1)+'</strong><span>'+esc(label)+'</span>'}
    function horizonText(row){var scores=((row.multi_horizon||{}).scores)||{};return ['T+1','T+5','T+20'].map(function(key){return key+' '+num(scores[key],1)}).join(' / ')}
    function reasonLabel(value){var labels={industry_concentration:'行业集中度限制',theme_concentration:'主题集中度限制',correlation_concentration:'相关性集中度限制',correlation_evidence_missing:'60日相关性证据不足',capacity_blocked:'容量不足',hard_gate_reject:'V4硬门禁拒绝'};return labels[value]||value}
    function reasonText(row){var reasons=[];((row.risk_gate||{}).reject_reasons||[]).concat(row.portfolio_reject_reasons||[]).forEach(function(value){value=reasonLabel(String(value||''));if(value&&reasons.indexOf(value)<0)reasons.push(value)});if(!reasons.length)(row.matched_conditions||[]).slice(0,3).forEach(function(value){if(value)reasons.push(String(value))});return reasons.join('；')||'V3-V6 证据完整，等待人工复核'}
    function readinessText(row){var ready=row.decision_readiness||{};return ready.new_buy_ready===true?'新买闸门通过':ready.recommend_status==='SUSPENDED'?'推荐暂停':'仅观察'}
    function portfolioText(row){return row.portfolio_eligible===true?'可纳入 #'+String(row.portfolio_rank||'—'):'未纳入组合'}
    function card(row){var versions=row.selector_versions||{},ready=row.decision_readiness||{};return '<article class="candidate-card"><div class="candidate-card-head"><span class="candidate-rank">#'+esc(row.rank||'—')+'</span>'+security(row.stock_code,row.stock_name||row.short_name)+'<span class="badge '+(row.candidate_grade==='A'?'safe':row.candidate_grade==='B'?'warning':'')+'">'+esc(row.candidate_grade||'—')+' · '+num(row.ensemble_score||row.score,1)+'</span></div><div class="candidate-card-route"><strong>V3 '+num(((versions.V3||{}).score),1)+' / V4 '+num(((versions.V4||{}).score),1)+' / V5 '+num(((versions.V5||{}).score),1)+' / V6 '+num(((versions.V6||{}).score),1)+'</strong><span>'+esc(horizonText(row))+'</span></div><div class="candidate-card-metrics"><div><span>执行门</span><strong>'+esc(status((row.execution_diagnostics||{}).status))+'</strong></div><div><span>新买资格</span><strong class="'+(ready.new_buy_ready===true?'safe-text':'warning-text')+'">'+esc(readinessText(row))+'</strong></div><div><span>组合结论</span><strong>'+esc(portfolioText(row))+'</strong></div><div><span>证据完整度</span><strong>'+pct(Number(row.evidence_completeness||0)*100,1)+'</strong></div></div><p class="candidate-card-reason"><b>判断依据：</b>'+esc(reasonText(row))+'</p></article>'}
    el('unifiedCandidateCards').innerHTML=rows.length?rows.slice(0,12).map(card).join(''):'<p class="empty">当前没有股票通过生产统一排序器。</p>';
    el('unifiedCandidateRows').innerHTML=rows.length?rows.map(function(row){var execution=row.execution_diagnostics||{};return '<tr><td>'+esc(row.rank||'—')+'</td><td>'+security(row.stock_code,row.stock_name||row.short_name)+'</td><td><strong>'+esc(row.candidate_grade||'—')+' / '+num(row.ensemble_score||row.score,1)+'</strong></td><td>'+versionCell(row,'V3')+'</td><td>'+versionCell(row,'V4')+'</td><td>'+versionCell(row,'V5')+'</td><td>'+versionCell(row,'V6')+'</td><td>'+esc(horizonText(row))+'</td><td>'+esc(status(execution.status))+(Number.isFinite(Number(execution.estimated_round_trip_cost_bps))?' · 往返成本 '+num(execution.estimated_round_trip_cost_bps,1)+' 基点':'')+'</td><td>'+esc(readinessText(row))+'</td><td>'+esc(portfolioText(row))+'</td><td class="reason">'+esc(reasonText(row))+'</td></tr>'}).join(''):empty(12,'当前没有生产统一候选');
  }
  function renderCandidates(){
    renderUnifiedCandidates();
    var strategies=Object.keys(strategyNames);if(el('strategyFilter').options.length===1){strategies.forEach(function(k){el('strategyFilter').insertAdjacentHTML('beforeend','<option value="'+k+'">'+esc(strategy(k))+'</option>')})}
    var sf=el('strategyFilter').value,st=el('statusFilter').value,q=el('candidateSearch').value.trim().toLowerCase();
    var rows=state.forecasts.filter(function(x){return(!sf||x.strategy_key===sf)&&(!st||x.forecast_status===st)&&(!q||String(x.stock_code).toLowerCase().indexOf(q)>=0||String(x.short_name).toLowerCase().indexOf(q)>=0||themeText(x).toLowerCase().indexOf(q)>=0)});
    var pageSize=40,pageCount=Math.max(1,Math.ceil(rows.length/pageSize));state.candidatePage=Math.max(1,Math.min(pageCount,Number(state.candidatePage||1)));var start=(state.candidatePage-1)*pageSize,pageRows=rows.slice(start,start+pageSize);
    function themeSummary(x){var full=themeText(x),parts=full.split(' / ').filter(Boolean);return parts.slice(0,3).join(' / ')+(parts.length>3?' / +'+(parts.length-3)+'项':'')}
    function card(x){var calibrated=hasCalibratedExpectation(x),reasons=(x.reasons||[]).join('；')||status(x.forecast_status),fullTheme=themeText(x);return '<article class="candidate-card"><div class="candidate-card-head"><span class="candidate-rank">#'+esc(num(x.rank_no,0))+'</span>'+security(x.stock_code,x.short_name)+'<span class="badge '+(x.forecast_status==='VALIDATED_POSITIVE'?'safe':x.forecast_status==='PAPER_DISCOVERY_CANDIDATE'?'warning':'')+'">'+esc(status(x.forecast_status))+'</span></div><div class="candidate-card-route"><strong>'+esc(strategy(x.strategy_key))+'</strong><span title="'+esc(fullTheme)+'">'+esc(themeSummary(x))+'</span></div><div class="candidate-card-metrics"><div><span>综合分</span><strong>'+num(x.raw_score,3)+'</strong></div><div><span>扣费后净期望</span><strong class="'+(calibrated&&Number(x.expected_return_net_pct)>0?'safe-text':calibrated?'':'warning-text')+'">'+(calibrated?pct(x.expected_return_net_pct):'未校准')+'</strong></div><div><span>Profit Factor</span><strong>'+(calibrated?ratio(x.profit_factor):'未校准')+'</strong></div><div><span>样本</span><strong>'+num(x.sample_count,0)+'</strong></div></div><p class="candidate-card-reason"><b>判断依据：</b>'+esc(reasons)+'</p></article>'}
    el('candidateCards').innerHTML=pageRows.length?pageRows.map(card).join(''):'<p class="empty">该条件下没有已落库候选</p>';
    el('candidateRows').innerHTML=pageRows.length?pageRows.map(function(x){var calibrated=hasCalibratedExpectation(x);return '<tr><td>'+num(x.rank_no,0)+'</td><td>'+security(x.stock_code,x.short_name)+'</td><td>'+esc(strategy(x.strategy_key))+'</td><td title="'+esc(themeText(x))+'">'+esc(themeSummary(x))+'</td><td>'+num(x.raw_score,3)+'</td><td class="'+(calibrated&&Number(x.expected_return_net_pct)>0?'safe-text':calibrated?'':'warning-text')+'">'+(calibrated?pct(x.expected_return_net_pct):'未校准')+'</td><td>'+(calibrated?pct(Number(x.probability_positive)*100,1):'未校准')+'</td><td>'+(calibrated?ratio(x.profit_factor):'未校准')+'</td><td>'+(calibrated?ratio(x.payoff_ratio):'未校准')+'</td><td>'+num(x.sample_count,0)+'</td><td>'+esc(status(x.forecast_status))+'</td><td class="reason">'+esc((x.reasons||[]).join('；'))+'</td></tr>'}).join(''):empty(12,'该条件下没有已落库候选');
    el('candidateSummary').textContent='共 '+rows.length+' 条 · 当前 '+(rows.length?start+1:0)+'–'+Math.min(start+pageSize,rows.length);el('candidatePageStatus').textContent='第 '+state.candidatePage+' / '+pageCount+' 页';el('candidatePrev').disabled=state.candidatePage<=1;el('candidateNext').disabled=state.candidatePage>=pageCount;el('candidatePager').hidden=rows.length<=pageSize;
  }
  function renderIntraday(){
    var data=state.intraday||{},radar=state.intradayRadar||{},realtime=data.current_realtime_state||{},history=data.latest_historical_snapshot||{},market=realtime.snapshot||{},isLive=realtime.status==='LIVE',liveStale=realtime.status==='STALE',radarRows=radar.data||[],isReview=!isLive&&radar.freshness==='historical_close',rows=isLive?(data.decisions||[]):radarRows,evidence=market.evidence||[];
    if(!rows.length&&isLive&&radar.freshness==='live')rows=radarRows;
    var age=realtime.snapshot_age_seconds,isFallback=String(market.source_provider||'').toUpperCase()==='PUBLIC_QUOTE_QUORUM_V1';
    var snapshotState=isLive?(isFallback?'替补行情有效，模拟盘降仓':'主源实时快照'):isReview?'收盘复盘快照，禁止下单':liveStale?'结果已过期，禁止下单':realtime.status==='MARKET_CLOSED'?'已收盘，当前无实时状态':'尚未收到实时快照';
    var label=isReview?'收盘复盘（只读）':liveStale?'结果已过期':isLive&&data.status==='actionable'?'允许模拟竞争':isLive&&data.status==='blocked'?'数据门禁阻断':isLive?'观察中':'当前无可用快照';
    el('intradayStatus').textContent=label;el('intradayStatus').className='badge '+(isLive&&data.status==='actionable'?'safe':liveStale||data.status==='blocked'?'danger':'warning');
    el('intradayCount').textContent=String(rows.length)+' 条'+(isReview?' · 只读':'');
    el('intradayFacts').innerHTML=[
      fact('数据状态',snapshotState),fact('观察时间',isLive?(market.observed_at||radar.observed_at||'—'):(radar.observed_at||'—')),fact('数据年龄',isLive?(age==null?'—':ageText(age)):(isReview?'收盘定格':'—')),fact('市场状态',isLive?status(market.state):isReview?'已收盘，只读复盘':'暂不可用'),fact('行情来源',isLive?sourceName(market.source_provider):(isReview?'当日全市场收盘快照':'—')),fact('行情能力',isLive?(isFallback?'仅模拟盘，降仓50%，无Level-1':'QMT正常时优先Level-1'):(isReview?'只允许复盘，禁止下单':'当前不可执行')),fact('候选数量',String(rows.length)+' 条'),fact('数据日期',radar.data_date||'—'),fact('当前结论',isReview?'展示当日最后一次板块联动扫描，不代表次日买点':(realtime.reason||radar.error||'等待实时数据'))
    ].join('');
    var displayed=isReview?['这是收盘后的只读复盘，不具备下单权限','次日仍需重新检查价格、板块宽度、量能和风控']:!isLive?[realtime.reason||radar.error||'当前没有可执行的实时状态']:evidence;
    el('intradayEvidence').innerHTML=displayed.length?displayed.slice(0,8).map(function(x,i){return fact(i===displayed.length-1?'当前结论':'门禁 '+(i+1),x)}).join(''):[fact('当前结论','尚无盘中扫描结果；交易时段可点击“立即扫描盘中机会”')].join('');
    el('intradayHistoricalFacts').innerHTML=history&&history.observed_at?[
      fact('历史快照时间',history.observed_at),fact('身份','历史/收盘快照，仅供复盘'),fact('市场状态',history.state||'—'),fact('行情来源',sourceName(history.source_provider)),fact('有效覆盖率',pct(Number(history.coverage||0)*100,1)),fact('执行权限','禁止下单')
    ].join(''):fact('历史快照','尚无已落库历史快照');
    el('intradayRows').innerHTML=rows.length?rows.map(function(x){var p=Number(x.current_price||x.price),amount=Number(x.intraday_amount_ratio),change=x.current_return_pct==null?x.change_pct:x.current_return_pct,breadth=x.theme_positive_breadth_pct==null?Number(x.intraday_theme_positive_breadth)*100:x.theme_positive_breadth_pct,score=x.raw_score==null?x.score:x.raw_score,why=x.reason_code||((x.matched_conditions||[]).join('；'))||(isReview?'收盘板块联动复盘':'观察');return '<tr><td>'+security(x.stock_code,x.short_name||x.stock_name)+'</td><td>'+esc(x.theme_name||x.primary_concept||x.theme_code||'未归属主题')+'</td><td>'+esc(isReview?'收盘复盘':x.role||'观察')+'</td><td>'+esc(isReview?'只读观察':reason(x.action))+'</td><td>'+(p>0?'¥'+num(p,2):'—')+'</td><td>'+pct(change)+'</td><td>'+pct(x.relative_strength_pct)+'</td><td>'+(amount>0?ratio(amount):'—')+'</td><td>'+pct(breadth,1)+'</td><td>'+num(score,2)+'</td><td>'+ratio(x.risk_reward_ratio)+'</td><td class=\"reason\">'+esc(reason(why))+'</td><td>'+esc(x.observed_at||x.intraday_observed_at||radar.observed_at||'—')+'</td></tr>'}).join(''):empty(13,'当前没有可用的盘中或收盘复盘记录');
  }
  function renderPortfolio(){var run=state.overview.run||{},portfolio=run.portfolio||{},rows=state.targets||[];el('portfolioStatus').textContent=rows.length?'仅研究 · 入队仍需四道门禁':'尚无研究组合';el('portfolioRows').innerHTML=rows.length?rows.map(function(x){return '<tr><td>'+security(x.stock_code,x.short_name)+'</td><td>'+esc((x.strategy_keys||[]).map(strategy).join(' / '))+'</td><td>'+esc(themeText(x))+'</td><td>'+money(x.target_value)+'</td><td>'+pct(Number(x.target_weight)*100,1)+'</td><td>'+num(x.target_quantity,0)+'</td><td>'+pct(x.expected_mae_pct)+'</td><td class="reason">仅研究 · '+esc(x.reason||'—')+'</td></tr>'}).join(''):empty(8,'本次没有研究目标；研究分数不作为买入依据')}
  function renderPositions(){var names={},summary=(state.paperLedger||{}).summary||{},stockCount=Number(summary.position_count||state.positions.length),lotCount=Number(summary.position_lot_count||stockCount);state.forecasts.forEach(function(x){names[x.stock_code]=x.short_name});state.targets.forEach(function(x){names[x.stock_code]=x.short_name});el('positionSummary').innerHTML=[fact('当前持仓',String(stockCount)+' 只'+(lotCount>stockCount?'（'+lotCount+' 笔成交批次）':'')),fact('合计持仓市值',money(summary.current_market_value)),pnlFact('合计浮动盈亏',summary.total_unrealized_pnl),fact('数据说明','同一股票按代码合并展示；成本价按持仓数量加权，底层每笔成交记录仍完整保留')].join('');el('positionRows').innerHTML=state.positions.length?state.positions.map(function(x){var sources=x.ledger_sources||[x.ledger_source],source=sources.map(function(v){return v==='LEGACY_EVENT_SIM'?'事件模拟账本':v==='V2_CANONICAL'?'V2/V3账本':'合并账本'}).filter(function(v,i,a){return a.indexOf(v)===i}).join(' + '),lots=Number(x.position_lot_count||1),rowPnlClass=pnlClass(x.unrealized_pnl),quote=x.current_price==null?'暂无行情':money(x.current_price),quoteAt=x.quote_at?String(x.quote_at)+' · '+sourceName(x.quote_source):'暂无行情时间',lotText=lots>1?'已合并 '+lots+' 笔成交批次；加权成本已重算。':'单笔持仓批次。';return '<tr><td>'+security(x.stock_code,x.short_name||names[x.stock_code])+'</td><td>'+esc(status(x.position_state||x.state))+'</td><td>'+num(x.remaining_quantity||x.quantity,0)+'</td><td>'+num(x.sellable_quantity,0)+'</td><td>'+money(x.cost_price||x.average_cost)+'</td><td>'+quote+'</td><td>'+money(x.market_value)+'</td><td class="'+rowPnlClass+'">'+money(x.unrealized_pnl)+'</td><td class="'+rowPnlClass+'">'+(x.unrealized_pnl_pct==null?'—':pct(x.unrealized_pnl_pct))+'</td><td>'+money(x.protective_stop)+'</td><td>'+num(x.add_count,0)+'</td><td>'+esc(quoteAt)+'</td><td class="reason">'+esc('['+source+'] '+lotText+' '+(x.invalidation_condition||x.last_reason||'趋势、硬止损和净期望动态复核'))+'</td></tr>'}).join(''):empty(13,'当前统一模拟账本没有持仓；候选不是持仓，只有模拟成交后才会显示')}
  function renderOrders(){var names={};state.forecasts.forEach(function(x){names[x.stock_code]=x.short_name});el('orderRows').innerHTML=state.orders.length?state.orders.map(function(x){var source=x.ledger_source==='LEGACY_EVENT_SIM'?'事件模拟账本':'V2/V3账本';return '<tr><td>'+security(x.stock_code,x.short_name||names[x.stock_code])+'</td><td>'+esc(x.side==='BUY'?'买入':'卖出')+'</td><td>'+num(x.quantity,0)+'</td><td>'+money(x.limit_price||x.filled_price)+'</td><td>'+esc(status(x.status))+'</td><td>'+esc('['+source+'] '+status(x.waiting_reason))+'</td><td>'+esc((x.earliest_at||x.created_at||'—')+' 至 '+(x.expires_at||x.filled_at||'—'))+'</td></tr>'}).join(''):empty(7,'当前统一模拟账本没有订单')}
  function renderValidation(){
    var v=state.validation||{},ready=state.readiness||{},models=ready.active_oos_models||[],level1=(state.dataEvidence||{}).level1||{};
    var activePass=models.length>0,matchingPass=activePass&&models.every(function(x){return x.validation_status==='PASS'}),level1Days=Number(level1.consecutive_trade_days||0),level1Pass=level1.status==='PASS'&&level1Days>=5;
    [['activeOosGate',activePass,activePass?models.length+' 个':'阻断'],['validationGate',matchingPass,matchingPass?'通过':'阻断'],['level1Gate',level1Pass,level1Days+' / 5 日']].forEach(function(item){var node=el(item[0]);node.textContent=item[2];node.className=item[1]?'safe-text':'danger-text'});
    var passed=[activePass,matchingPass,level1Pass].filter(Boolean).length;
    el('criticalAcceptanceStatus').textContent=passed+' / 3 项通过';el('criticalAcceptanceStatus').className='badge '+(passed===3?'safe':'danger');
    el('criticalAcceptanceFacts').innerHTML=[
      fact('当前有效模型',models.map(function(x){return strategy(x.strategy_key)+' · '+x.model_version}).join('；')||'无'),
      fact('逐模型对应验证',models.map(function(x){return x.model_version+' → '+x.validation_status}).join('；')||'无有效模型，不能判定验证通过'),
      fact('Level1 连续交易日',level1Days+' / 5；状态 '+status(level1.status||'BLOCK')),
      fact('真实下单',ready.real_trading_enabled?'异常：已开启':'固定关闭')
    ].join('');
    el('oosSamples').textContent=num(v.sample_count,0);el('oosExpectancy').textContent=pct(v.net_expectancy_pct);el('oosPf').textContent=ratio(v.profit_factor);el('oosPayoff').textContent=ratio(v.payoff_ratio);el('oosDd').textContent=pct(v.maximum_drawdown_pct);var evidence=v.evidence||{},p=evidence.portfolio||{};el('oosProfit').textContent=money(p.net_profit_cny);el('validationStatus').textContent=v.result_status?status(v.result_status):'暂无结果';el('validationStatus').className='badge '+(v.result_status==='PASS'?'safe':'danger');el('validationEvidence').innerHTML=[fact('模型版本',v.model_version||'—'),fact('样本外区间',(v.period_start||'—')+' 至 '+(v.period_end||'—')),fact('阻断原因',(v.block_reasons||[]).map(status).join('；')||'无'),fact('最终权益',money(p.final_equity_cny)),fact('组合收益',pct(p.total_return_pct)),fact('交易次数',p.trade_count==null?'—':p.trade_count)].join('')
  }
  function renderRecall(){var r=state.recall||{};el('recall20').textContent=r.recall_at_20==null?'—':pct(Number(r.recall_at_20)*100,1);el('recall50').textContent=r.recall_at_50==null?'—':pct(Number(r.recall_at_50)*100,1);el('winnerCount').textContent=r.winner_count==null?'—':r.winner_count;el('missedCount').textContent=r.missed_winner_count==null?'—':r.missed_winner_count;var reasons=r.missed_reason_counts||{};el('missedReasons').innerHTML=Object.keys(reasons).length?Object.keys(reasons).sort(function(a,b){return reasons[b]-reasons[a]}).map(function(k){return fact(status(k),reasons[k]+' 次')}).join(''):fact('当前状态','预测期限尚未成熟，影子复盘正在逐日形成');el('recallStatus').textContent=r.trade_date?'影子复盘更新至 '+r.trade_date:'影子复盘积累中'}
  function renderLearning(){var x=state.learning||{},stages=x.stage_counts||{};el('learningObserved').textContent=x.observed_count==null?'0':x.observed_count;el('learningAccepted').textContent=x.accepted_count==null?'0':x.accepted_count;el('learningWinRate').textContent=x.win_rate==null?'—':pct(Number(x.win_rate)*100,1);el('learningPf').textContent=ratio(x.profit_factor);el('learningStatus').textContent=x.accepted_count?'已形成 '+x.accepted_count+' 笔真实模拟成交闭环':'等待首笔完整模拟成交闭环';el('learningStatus').className='badge '+(Number(x.profit_factor)>=1.3?'safe':'warning');el('learningFacts').innerHTML=[fact('学习证据','仅实际模拟成交与真实手续费'),fact('平均净收益',pct(x.average_net_return_pct)),fact('平均最大不利',pct(x.average_mae_pct)),fact('平均最大有利',pct(x.average_mfe_pct)),fact('影子误触发',String(x.false_positive_count||0)+' 次'),fact('影子漏掉强势',String(x.missed_opportunity_count||0)+' 次'),fact('重新校准门槛',String(x.minimum_samples_before_calibration||80)+' 笔完整成交'),fact('最近结果日期',x.latest_outcome_date||'尚未成熟'),fact('当前结论',x.conclusion||'继续积累真实前向样本')].join('')+(Object.keys(stages).length?'<div><span>阶段分布</span><strong>'+esc(Object.keys(stages).map(function(k){return status(k)+' '+stages[k]}).join('；'))+'</strong></div>':'')}
  function renderEvidence(){var ready=state.readiness||{},run=state.overview.run||{},blocks=ready.blocks||[],warnings=ready.warnings||[],limits=ready.portfolio_limits||{},level1=(state.dataEvidence||{}).level1||{};el('readinessStatus').textContent=ready.paper_ready?'模拟链路就绪':'存在硬阻断';el('readinessStatus').className='badge '+(ready.paper_ready?'safe':'danger');el('blocks').innerHTML=(blocks.length?blocks.map(function(x){return fact('硬阻断',status(x))}).join(''):warnings.length?warnings.map(function(x){return fact('正式策略提示',status(x)+'；左侧实验模拟链路仍可用')}).join(''):fact('模拟链路','数据库、校准和只读接口均已就绪'))+fact('Level1 连续采集',(level1.consecutive_trade_days||0)+' / 5 交易日；'+status(level1.status||'BLOCK'))+fact('生产仓位规则','正式 '+(limits.maximum_positions||'—')+'；试错 '+(limits.maximum_paper_discovery_positions||'—')+'；实时总上限 '+(limits.maximum_live_positions||'—')+'；加仓 '+(limits.maximum_add_count==null?'—':limits.maximum_add_count));el('provenance').innerHTML=[fact('决策批次',run.run_uid||'—'),fact('模型版本',run.model_version||'—'),fact('数据哈希',run.data_snapshot_hash||'—'),fact('结果哈希',run.result_hash||'—'),fact('决策时间',run.decision_at||'—'),fact('真实交易','固定关闭')].join('');var schema=ready.schema||{};el('schema').innerHTML=Object.keys(schema).map(function(k){return '<span class="'+(schema[k]?'':'bad')+'">'+esc(k)+'</span>'}).join('')}
  document.querySelectorAll('.nav').forEach(function(btn){btn.addEventListener('click',function(){activateView(btn.dataset.view);notifyParentResize()})});
  document.addEventListener('click',function(event){var link=event.target.closest&&event.target.closest('.security a[data-stock-code]');if(!link)return;event.preventDefault();requestStockChart(link.dataset.stockCode,link.dataset.stockName)});
  window.addEventListener('message',function(event){if(event.source!==window.parent||!event.data||event.data.type!=='probiga-trading-v3-view')return;var expectedOrigin='*';try{expectedOrigin=new URL(document.referrer).origin}catch(ignore){}if(expectedOrigin!=='*'&&expectedOrigin!=='null'&&event.origin!==expectedOrigin)return;activateView(String(event.data.view||'overview'));notifyParentResize()});
  if(window.ResizeObserver)new ResizeObserver(notifyParentResize).observe(document.documentElement);
  var candidateTimer=null;
  function reloadCandidates(){state.candidatePage=1;var day=el('candidateDate').value,sf=el('strategyFilter').value,st=el('statusFilter').value,q=el('candidateSearch').value.trim(),unifiedDay=el('unifiedCandidateDate').value,unifiedQ=el('unifiedCandidateSearch').value.trim(),unifiedPreset=el('unifiedPresetFilter').value,params=['limit='+(q?1000:200)];if(day)params.push('trade_date='+encodeURIComponent(day));if(sf)params.push('strategy_key='+encodeURIComponent(sf));if(st)params.push('status='+encodeURIComponent(st));if(q)params.push('q='+encodeURIComponent(q));var unified=unifiedDay?fetchJson('/api/screener/history?data_date='+encodeURIComponent(unifiedDay)+'&preset='+encodeURIComponent(unifiedPreset)+'&limit=200'+(unifiedQ?'&q='+encodeURIComponent(unifiedQ):'')):postJson('/api/screener/run',{preset:unifiedPreset,as_of_date:'',universe:'market',top:100,filters:{exclude_st:true}});Promise.all([
    api3('/forecasts/latest?'+params.join('&')).then(function(v){state.forecasts=unwrap(v)||[]}).catch(function(){state.forecasts=[]}),
    unified.then(function(v){state.unifiedRun=v}).catch(function(err){state.unifiedRun={status:'error',error:err.message,data:[],stats:{}}})
  ]).then(function(){renderCandidates();notifyParentResize()})}
  function pollAction(actionKey,taskType,button,remaining){
    setTimeout(function(){
      fetchJson('/api/scheduler/tasks').then(function(payload){
        state.tasks=payload.data||[];var row=task(taskType);
        if(row.last_run_status==='running'&&remaining>0){state.actionMessage=(actionKey==='daily'?'日级选股':'盘中扫描')+'正在执行，请稍候…';renderActions();pollAction(actionKey,taskType,button,remaining-1);return}
        button.disabled=false;
        if(row.last_run_status==='success'){
          var outside=String(row.last_run_output||'').indexOf('OUTSIDE_NEW_ENTRY_SESSION')>=0;
          state.actionMessage=outside?'盘中扫描已执行；当前不在 09:30–14:50 新开仓时段。':(actionKey==='daily'?'日级选股执行成功，正在读取新结果。':'盘中扫描执行成功，正在读取机会。');
        }else{state.actionMessage=(actionKey==='daily'?'日级选股':'盘中扫描')+'执行'+(row.last_run_status==='failed'?'失败':'结束')+'，请查看数据与系统。'}
        load();
      }).catch(function(err){button.disabled=false;state.actionMessage='状态读取失败：'+err.message;renderActions()})
    },3000);
  }
  function runAction(actionKey,button,taskType){
    button.disabled=true;state.actionMessage=(actionKey==='daily'?'日级选股':'盘中扫描')+'已提交，正在启动…';renderActions();
    postJson('/api/v3/actions/'+actionKey).then(function(payload){
      var result=unwrap(payload)||{};
      if(result.status==='disabled'){throw new Error('对应任务未启用')}
      state.actionMessage=result.status==='already_running'?'任务已经在执行，继续等待结果。':'任务已开始执行，请稍候…';
      renderActions();pollAction(actionKey,taskType,button,60);
    }).catch(function(err){button.disabled=false;state.actionMessage='执行失败：'+err.message;renderActions()})
  }
  ['strategyFilter','statusFilter','candidateDate'].forEach(function(id){el(id).addEventListener('change',reloadCandidates)});
  el('candidateSearch').addEventListener('input',function(){clearTimeout(candidateTimer);candidateTimer=setTimeout(reloadCandidates,250)});
  ['unifiedPresetFilter','unifiedCandidateDate'].forEach(function(id){el(id).addEventListener('change',reloadCandidates)});
  el('unifiedCandidateSearch').addEventListener('input',function(){clearTimeout(candidateTimer);candidateTimer=setTimeout(reloadCandidates,250)});
  el('unifiedCurrent').addEventListener('click',function(){el('unifiedCandidateDate').value='';reloadCandidates()});
  el('candidatePrev').addEventListener('click',function(){if(state.candidatePage>1){state.candidatePage-=1;renderCandidates();notifyParentResize()}});
  el('candidateNext').addEventListener('click',function(){state.candidatePage+=1;renderCandidates();notifyParentResize()});
  var hypothesisTimer=null;
  ['hypothesisState','hypothesisDate'].forEach(function(id){el(id).addEventListener('change',reloadHypotheses)});
  el('hypothesisSearch').addEventListener('input',function(){clearTimeout(hypothesisTimer);hypothesisTimer=setTimeout(reloadHypotheses,250)});
  el('hypothesisRows').addEventListener('click',function(event){var button=event.target.closest('.evidence-button');if(button)showHypothesis(button.dataset.hypothesisId)});
  el('runDaily').addEventListener('click',function(){runAction('daily',this,'trading_v3_close_decision')});
  el('runIntraday').addEventListener('click',function(){runAction('intraday',this,'trading_v2_intraday_activation')});
  el('refresh').addEventListener('click',load);activateView(requestedView());load();
})();
