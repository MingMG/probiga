(function(){
  'use strict';
  var state={context:{},marketClock:{},readiness:{},overview:{},stockPool:{},auctionGate:{},watchlistStrategy:{},forecasts:[],oversold:[],targets:[],validation:null,recall:null,learning:{},account:{},paperLedger:{},positions:[],orders:[],runs:[],intraday:{},hypotheses:[],hypothesisDetail:null,tasks:[],dataEvidence:{},lineage:{},researchGovernance:{},batchDiff:{},decisionIntelligence:{},horizonValidation:{},counterfactualResearch:{},shadowPreview:{},researchErrors:{},candidatePage:1,actionMessage:'',errors:{},staleKeys:{},loadedKeys:{},requestedDate:'',activeView:'overview',filters:{},activeJobId:'',loadSeq:0,loading:true};
  var titles={overview:'今日策略',hypotheses:'连续跟踪',candidates:'策略池',intraday:'盘中应急',portfolio:'目标组合',positions:'我的持仓',orders:'模拟订单',validation:'回测验收',missed:'漏抓复盘',evidence:'数据与系统'};
  var subtitles={overview:'先处理持仓，再看新机会',positions:'自选股里的真实持仓，今天怎么操作',candidates:'看清买入范围、卖出范围和突发退出',intraday:'盘中出现意外，先执行退出红线',hypotheses:'买入以后继续跟，不让建议断档'};
  var strategyNames={theme_diffusion:'板块扩散预热',low_base_ignition:'板块点火预判',right_side_trend:'右侧趋势启动',event_drift:'事件后漂移',quality_momentum:'质量与动量',oversold_reversal:'超跌抄底实验',intraday_surprise:'盘中超预期',weak_market_structural_mainline:'弱市结构性主线',ai_application_research:'AI应用纸面研究',robotics_research:'机器人纸面研究',paper_discovery:'模拟试错'};
  var statusNames={VALIDATED_POSITIVE:'扣费后正期望，允许进入组合',RESEARCH_TARGET:'同批次研究目标，不可直接下单',PORTFOLIO_REJECTED:'同批次组合拒绝',RESEARCH_SAMPLE:'研究样本',PAPER_DISCOVERY_CANDIDATE:'触发小仓模拟试错',LEFT_SIDE_PREPARE:'已进入抄底准备区，暂不买',RESEARCH_ONLY_UNCALIBRATED:'没有样本外校准，只记录不交易',RESEARCH_ONLY_MODEL_VERSION_MISMATCH:'旧公式校准已隔离，仅允许模拟盘重新验证',RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED:'高分组反而亏损，排序失真，禁止自动买入',RESEARCH_ONLY_PROFIT_GATE_FAILED:'样本外收益闸门失败，只研究',INSUFFICIENT_DATA:'所需事实不完整，不计算',SETUP_NOT_READY:'板块、趋势和入场位置尚未同时确认',WEAK_MARKET_THEME_WATCH:'弱市结构性机会，进入观察池但暂不自动买入',MARKET_REGIME_BLOCKED:'大盘偏弱且细分板块扩散或个股领导力不足',NO_ACTIVE_OOS_CALIBRATION:'没有策略通过样本外校准',NO_COMPATIBLE_OOS_CALIBRATION:'当前没有与冻结公式匹配且排序可信的正期望模型',V3_SCHEMA_INCOMPLETE:'决策账本尚未完成迁移',PAPER_ACTIVE:'模拟盘已启用',PAPER_TRIAL:'模拟观察',PAPER_DISCOVERY_READY:'模拟盘小仓试错已就绪',READY_WITH_PAPER_DISCOVERY:'正式组合与模拟试错并行',RESEARCH:'研究验证中，不会发出交易指令',QUEUED:'等待模拟撮合',FILLED:'已成交',CANCELLED:'已取消',RISK_APPROVED:'风控通过',HOLDING:'持仓中',holding:'持仓中',SOLD_TODAY:'今日已卖出',V3_PAPER_DISCOVERY:'模拟盘小仓前向验证',V3_VALIDATED_POSITIVE:'正期望组合模拟委托',V3_PROFIT_GATE_MIGRATION:'正期望闸门启用，旧买单已撤销',EXIT_PENDING_T1:'趋势已失效，T+1 后立即卖出'};
  var reasonNames={MARKET_NOT_CONFIRMED:'实时行情覆盖率或市场确认未通过，只提醒不买入',OUTSIDE_DAILY_ENTRY_WINDOW:'已过日级候选的盘中开仓窗口，只观察',DUPLICATE_ENTRY_SAME_DAY_BLOCKED:'当天已处理过同一证券，不重复开仓',DATA_QUALITY_BLOCK:'实时数据质量未通过',RISK_REJECTED:'模拟风控拒绝',ACTIVATE_PROBE:'盘中超预期，小仓试单',ACTIVATE_SUBSTITUTE:'龙头无法成交，选择同题材替补',ACTIVATE_REVERSAL_PROBE:'水下修复并放量，小仓试单',ACTIVATE_VOLUME_PROBE:'突然爆量上攻，小仓试单',WATCH:'观察，不下单'};
  var hypothesisNames={ACTIVE:'已经触发',TRIGGER_READY:'等待价格确认',PREPARE:'重点准备',WATCH:'普通观察',WEAKEN:'正在转弱',INVALIDATED:'已经失效'};
  var probabilityNames={OOS_CALIBRATED:'样本外校准概率',PAPER_FORWARD_PRIOR:'模拟前向先验',STRUCTURED_RESEARCH_PRIOR:'结构化研究先验',INTRADAY_STRUCTURED_PRIOR:'盘中结构化先验',REGIME_MIXTURE:'市场状态混合概率'};
  var actionNames={BUY_OR_HOLD:'模拟买入或继续持有',PAPER_PROBE:'模拟小仓试单',PAPER_PROBE_IF_CONFIRMED:'确认后模拟小仓试单',PAPER_ORDER_CREATED:'模拟订单已创建',ALERT_ONLY:'只提醒，不下单',WAIT_INTRADAY_CONFIRM:'等待盘中确认',WAIT_PRICE_CONFIRM:'等待价格确认',WATCH_CLOSELY:'重点观察',NO_TRADE:'不交易',NO_NEW_BUY:'禁止新买',EXIT_OR_AVOID:'退出或回避',CONTROLLED_RISK_ON:'控制仓位参与',SELECTIVE_PROBES:'只做精选试单',CASH_FIRST:'现金优先'};
  var actionabilityNames={BUY_ZONE:'允许买入区间',PAPER_ONLY:'仅模拟研究',WAIT_TRIGGER:'等待触发',REJECTED:'组合已拒绝',RESEARCH_ONLY:'研究观察（不可执行）'};
  var dailyChangeNames={BASELINE:'首个可验证基线',NEW:'今日新入池',UPGRADED:'较前次上升',DOWNGRADED:'较前次下降',RETAINED:'连续保留'};
  var dynamicRoleNames={LEADER:'当前领先候选',CORE:'核心候选',CONDITIONAL:'条件候选',PRIMARY:'当前优先候选',CORE_ALTERNATIVE:'同场景备选',OBSERVE:'观察候选',INDEPENDENT:'独立候选'};
  var auctionActionNames={BUY_CANDIDATE:'竞价确认候选',WAIT_OPEN_CONFIRM:'等待开盘确认',PAPER_REVIEW:'仅模拟复核',RESEARCH_ONLY:'研究观察',REJECT_CHASE:'竞价拒绝追高',REJECT_WEAK:'竞价走弱剔除',UNBUYABLE:'竞价不可成交',DATA_BLOCKED:'竞价数据阻断',REJECTED:'原组合已拒绝'};
  function el(id){return document.getElementById(id)}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function num(v,d){var n=Number(v);return Number.isFinite(n)?n.toFixed(d==null?2:d):'—'}
  function pct(v,d){var n=Number(v);return Number.isFinite(n)?n.toFixed(d==null?2:d)+'%':'—'}
  function ratio(v){var n=Number(v);return Number.isFinite(n)?n.toFixed(2):'—'}
  function money(v){if(v==null||v==='')return '—';var n=Number(v);return Number.isFinite(n)?'¥'+n.toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:2}):'—'}
  function displayDateTime(v){if(v==null||v==='')return '—';return String(v).replace('T',' ').replace(/\.000000$/,'').replace(/\.000$/,'')}
  function firstNumber(values){for(var i=0;i<values.length;i+=1){if(values[i]!=null&&values[i]!==''&&Number.isFinite(Number(values[i])))return Number(values[i])}return null}
  function pnlClass(value){var n=Number(value);return Number.isFinite(n)?(n>0?'pnl-gain':n<0?'pnl-loss':''):''}
  function unwrap(v){return v&&Object.prototype.hasOwnProperty.call(v,'data')?v.data:v}
  function responseError(r,path){return r.text().then(function(body){var safeMessage='';try{var parsed=JSON.parse(body);safeMessage=parsed.message||parsed.error||''}catch(ignore){}throw new Error('HTTP '+r.status+(safeMessage?' · '+safeMessage:'')+' · '+path)})}
  function fetchJson(path,timeoutMs){
    var controller=typeof AbortController==='function'?new AbortController():null,waitMs=Number(timeoutMs||15000),timer=null,options={headers:{Accept:'application/json'},cache:'no-store'};
    if(controller)options.signal=controller.signal;
    if(controller)timer=setTimeout(function(){controller.abort()},waitMs);
    return fetch(path,options).then(function(r){if(!r.ok)return responseError(r,path);return r.json()}).catch(function(err){if(err&&err.name==='AbortError')throw new Error('请求超时（'+waitMs+'ms） · '+path);throw err}).finally(function(){if(timer)clearTimeout(timer)})
  }
  function postJson(path){return fetch(path,{method:'POST',headers:{Accept:'application/json'}}).then(function(r){if(!r.ok)return responseError(r,path);return r.json()})}
  function api3(path){return fetchJson('/api/v3'+path)}
  function governanceApi(){return api3('/research/governance')}
  function api2(path){return fetchJson('/api/v2'+path)}
  function security(code,name){code=String(code||'').split('.')[0];name=String(name||code||'—');return '<span class="security"><a class="name" href="#" data-stock-code="'+esc(code)+'" data-stock-name="'+esc(name)+'">'+esc(name)+'</a><a class="code" href="#" data-stock-code="'+esc(code)+'" data-stock-name="'+esc(name)+'">'+esc(code)+'</a></span>'}
  function requestStockChart(code,name){
    code=String(code||'').split('.')[0];name=String(name||code||'');
    if(window.parent&&window.parent!==window){parentMessage('probiga-open-kline',{stock_code:code,short_name:name});return}
    var market=code.indexOf('6')===0?'sh':'sz';window.open('https://quote.eastmoney.com/'+market+code+'.html#fullScreenChart','_blank','noopener')
  }
  function empty(cols,text){return '<tr><td class="empty" colspan="'+cols+'">'+esc(text)+'</td></tr>'}
  function fact(label,value){return '<div><span>'+esc(label)+'</span><strong>'+esc(value==null?'—':value)+'</strong></div>'}
  function pnlFact(label,value){return '<div><span>'+esc(label)+'</span><strong class="'+pnlClass(value)+'">'+esc(money(value))+'</strong></div>'}
  function priceRange(range){if(!range)return '—';var low=firstNumber([range.low,range.min,range[0]]),high=firstNumber([range.high,range.max,range[1]]);if(low==null&&high==null)return '—';if(low==null)low=high;if(high==null)high=low;return low===high?money(low):money(low)+' ～ '+money(high)}
  function status(v){return statusNames[String(v||'')]||String(v||'—')}
  function strategy(v){return strategyNames[String(v||'')]||String(v||'—')}
  function isDiscoveryTarget(x){return (x.strategy_keys||[]).indexOf('paper_discovery')>=0||String(x.reason||'').indexOf('PAPER_DISCOVERY')===0}
  function isExecutableTarget(x){return x&&x.new_buy_eligible===true}
  function hasCalibratedExpectation(x){
    var uncalibrated=['RESEARCH_ONLY_UNCALIBRATED','PAPER_DISCOVERY_CANDIDATE','LEFT_SIDE_PREPARE','WEAK_MARKET_THEME_WATCH','SETUP_NOT_READY','MARKET_REGIME_BLOCKED','INSUFFICIENT_DATA'];
    var statuses=x.forecast_statuses||[x.forecast_status];
    return uncalibrated.indexOf(String(x.forecast_status||''))<0&&(Number(x.sample_count||0)>0||statuses.indexOf('VALIDATED_POSITIVE')>=0)&&Number.isFinite(Number(x.expected_return_net_pct));
  }
  function strategyText(x){var keys=(x&&x.strategy_keys)||[];return keys.length?keys.map(strategy).join(' / '):strategy(x&&x.strategy_key)}
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
  function sourceName(v){var k=String(v||'').toUpperCase();if(k==='GJ_BIG_QMT_INNER')return '国金QMT主源';if(k==='PUBLIC_QUOTE_QUORUM_V1')return '公共双源替补（新浪+腾讯）';if(k==='UNATTESTED_MINUTE_SOURCE')return '未补证分钟数据（禁止交易）';return v||'—'}
  function snapshotAge(observed){if(!observed)return null;var t=new Date(String(observed).replace(' ','T')),seconds=(Date.now()-t.getTime())/1000;return Number.isFinite(seconds)?Math.max(0,seconds):null}
  function ageText(seconds){if(seconds==null)return '无时间';if(seconds<60)return Math.round(seconds)+'秒前';if(seconds<3600)return Math.round(seconds/60)+'分钟前';return (seconds/3600).toFixed(1)+'小时前'}
  function isTradingNow(){var d=new Date(),day=d.getDay(),n=d.getHours()*100+d.getMinutes();return day>0&&day<6&&((n>=931&&n<=1130)||(n>=1301&&n<=1500))}
  function task(type){return state.tasks.find(function(x){return x.task_type===type})||{}}
  function localDateKey(){var d=new Date(),month=String(d.getMonth()+1).padStart(2,'0'),day=String(d.getDate()).padStart(2,'0');return d.getFullYear()+'-'+month+'-'+day}
  function isHistoricalRequest(){return (state.context||{}).historical_read_only!==false}
  function dateParam(){return state.requestedDate?'trade_date='+encodeURIComponent(state.requestedDate):''}
  function withDate(path){if(!state.requestedDate)return path;return path+(path.indexOf('?')>=0?'&':'?')+dateParam()}
  function stockPoolIsReadable(pool){
    pool=pool||{};
    var items=pool.items,summary=pool.summary||{},poolStatus=String(pool.pool_status||'').toUpperCase(),runStatus=String(pool.run_status||'').toUpperCase();
    var sessionDate=String(pool.decision_session_date||'').slice(0,10),dataDate=String(pool.trade_date||pool.data_date||'').slice(0,10);
    var datePattern=/^\d{4}-\d{2}-\d{2}$/,stockCount=summary.stock_count,candidateCount=summary.strategy_candidate_count,actualCandidateCount=Array.isArray(items)?items.filter(function(item){return item&&item.is_strategy_candidate===true}).length:-1;
    return !!pool.run_uid&&pool.pool_readable===true&&runStatus==='COMPLETED'&&pool.decision_integrity_verified===true&&(poolStatus==='READY'||poolStatus==='EMPTY')&&datePattern.test(sessionDate)&&datePattern.test(dataDate)&&dataDate<=sessionDate&&Array.isArray(items)&&Number.isInteger(stockCount)&&stockCount===items.length&&Number.isInteger(candidateCount)&&candidateCount===actualCandidateCount&&((poolStatus==='READY'&&candidateCount>0)||(poolStatus==='EMPTY'&&candidateCount===0));
  }
  function stockPoolFormalTruth(pool,requestedDate,latestFormalDate){
    pool=pool||{};var datePattern=/^\d{4}-\d{2}-\d{2}$/,decisionDate=String(pool.decision_session_date||'').slice(0,10),dataDate=String(pool.trade_date||pool.data_date||'').slice(0,10),target=String(requestedDate||pool.requested_trade_date||decisionDate||'').slice(0,10),latestDate=String(latestFormalDate||'').slice(0,10),asOf=pool.is_as_of_fallback===true,reasonCodes=Array.isArray(pool.reason_codes)?pool.reason_codes.filter(Boolean).join('；'):'';
    function blocked(reason,code){return {ready:false,verifiedCompleted:false,requestedDate:target,decisionDate:decisionDate,dataDate:dataDate,reason:reason,reasonCode:code||'FORMAL_POOL_BLOCKED'}}
    if(!datePattern.test(target))return blocked('缺少有效请求日，不能确认这是当前策略池','REQUEST_DATE_INVALID');
    if(!datePattern.test(latestDate))return blocked('无法确认最新正式交易日，策略池保持研究只读','LATEST_FORMAL_DATE_UNKNOWN');
    if(!asOf&&target!==latestDate)return blocked('请求日 '+target+' 不是最新正式交易日 '+latestDate+'，只允许历史研究查看','HISTORICAL_RESEARCH_ONLY');
    if(!stockPoolIsReadable(pool))return blocked('策略池未通过 COMPLETED、完整性、计数或日期校验'+(reasonCodes?'：'+reasonCodes:''),'POOL_NOT_VERIFIED_COMPLETED');
    if(asOf&&(decisionDate!==latestDate||target<decisionDate))return blocked('最新成功批次日 '+(decisionDate||'未知')+' 不能作为请求日 '+target+' 的已完成决策','AS_OF_POOL_DATE_INVALID');
    if(!asOf&&decisionDate!==target)return blocked('策略池决策日 '+(decisionDate||'未知')+' 与请求日 '+target+' 不一致','POOL_DATE_MISMATCH');
    if(dataDate!==decisionDate)return blocked('策略池数据日 '+(dataDate||'未知')+' 与已验证批次日 '+(decisionDate||'未知')+' 不一致','POOL_DATA_DATE_MISMATCH');
    if(pool.is_historical_fallback===true||pool.historical_read_only===true)return blocked('当前展示的是历史只读批次，不是请求日正式票池','HISTORICAL_READ_ONLY');
    if(pool.governance_deferred===true||pool.activation_enabled===false||String(pool.strategy_governance_mode||'').toUpperCase()==='DEFERRED_DB')return blocked('治理数据库处于 DEFERRED_DB，候选只可研究审计','GOVERNANCE_DATABASE_DEFERRED');
    var canonicalGovernance=String(pool.source_system||'').toUpperCase()==='STRATEGY_GOVERNANCE'&&pool.decision_integrity_verified===true&&pool.real_order_authority===false;
    if(!canonicalGovernance&&(String(pool.decision_scope||'').toUpperCase()==='RESEARCH_ONLY'||pool.actionable_output_allowed===false))return blocked('批次权限为 RESEARCH_ONLY，不能升级为当前可执行票池','RESEARCH_ONLY');
    return {ready:true,verifiedCompleted:true,requestedDate:target,decisionDate:decisionDate,dataDate:dataDate,reason:asOf?'请求日使用不晚于该日的最新成功批次':'身份、日期与完整性均已通过',reasonCode:asOf?'VERIFIED_COMPLETED_LATEST_AS_OF_POOL':'VERIFIED_COMPLETED_CURRENT_POOL'}
  }
  function stockPoolWithHistoricalFallback(requestedDate){
    var target=String(requestedDate||'').slice(0,10),exactPath='/stock-pool'+(target?'?trade_date='+encodeURIComponent(target):'');
    return api3(exactPath).then(function(payload){
      var exact=unwrap(payload)||{},exactSession=String(exact.decision_session_date||exact.trade_date||'').slice(0,10),exactAsOf=exact.is_as_of_fallback===true&&!!target&&!!exactSession&&exactSession<=target,exactReadable=stockPoolIsReadable(exact)&&exact.is_historical_fallback!==true&&exact.historical_read_only!==true&&(!target||exactSession===target||exactAsOf);
      if(exactReadable)return Object.assign({},exact,{requested_trade_date:target||exactSession,is_historical_fallback:false});
      function missingExact(){return Object.assign({},exact,{requested_trade_date:target,exact_run_missing:true,exact_run_unreadable:!!exact.run_uid,is_historical_fallback:false})}
      if(!target)return missingExact();
      return api3('/stock-pool?before_session_date='+encodeURIComponent(target)).then(function(latestPayload){
        var latest=unwrap(latestPayload)||{},latestSession=String(latest.decision_session_date||latest.trade_date||'').slice(0,10),boundedTarget=String(latest.before_session_date||latest.requested_trade_date||'').slice(0,10),fallbackSession=String(latest.historical_fallback_session_date||'').slice(0,10);
        var boundedReadable=stockPoolIsReadable(latest)&&latest.is_historical_fallback===true&&latest.historical_read_only===true&&String(latest.historical_fallback_status||'')==='HISTORICAL_READ_ONLY'&&boundedTarget===target&&fallbackSession===latestSession&&!!latestSession&&latestSession<target;
        if(!boundedReadable)return missingExact();
        return Object.assign({},latest,{requested_trade_date:target,exact_run_missing:true,exact_run_unreadable:!!exact.run_uid,is_historical_fallback:true,historical_read_only:true,historical_fallback_session_date:latestSession,historical_fallback_reason:latest.historical_fallback_reason||'请求日没有完整可验证的 V3 决策批次，展示此前最近一次 COMPLETED 历史策略池'});
      }).catch(missingExact)
    })
  }
  function errorText(err){return err&&err.message?err.message:String(err||'未知错误')}
  function requestedView(){
    var view='overview';
    try{view=(window.frameElement&&window.frameElement.dataset.pendingView)||view}catch(ignore){}
    if(location.hash&&titles[location.hash.slice(1)])view=location.hash.slice(1);
    return titles[view]?view:'overview';
  }
  function requestedFrameDate(){
    var value='';
    try{value=String((window.frameElement&&window.frameElement.dataset.requestedDate)||'')}catch(ignore){}
    return /^\d{4}-\d{2}-\d{2}$/.test(value)?value:'';
  }
  function parentMessage(type,payload){
    if(window.parent===window)return false;
    var targetOrigin='*';try{var origin=new URL(document.referrer).origin;if(origin&&origin!=='null')targetOrigin=origin}catch(ignore){}
    try{window.parent.postMessage(Object.assign({type:type},payload||{}),targetOrigin);return true}catch(ignore){return false}
  }
  function activateView(view,options){
    options=options||{};
    view=titles[view]?view:'overview';
    state.activeView=view;
    document.documentElement.dataset.embeddedView=view;
    document.querySelectorAll('.nav').forEach(function(x){x.classList.toggle('active',x.dataset.view===view)});
    document.querySelectorAll('.view').forEach(function(x){x.classList.toggle('active',x.id==='view-'+view)});
    el('pageTitle').textContent=titles[view];
    if(el('pageSubtitle'))el('pageSubtitle').textContent=subtitles[view]||'策略详情与只读证据';
    if(options.userInitiated){
      if(!parentMessage('probiga-trading-v3-navigate',{view:view,requested_date:state.requestedDate,filters:state.filters})){
        history.pushState({view:view},'',location.pathname+location.search+'#'+view);
        load();
      }
    }
  }
  function notifyParentResize(){
    if(window.parent===window)return;
    var height=Math.max(document.body.scrollHeight,document.documentElement.scrollHeight);
    var targetOrigin='*';
    try{var referrerOrigin=new URL(document.referrer).origin;if(referrerOrigin&&referrerOrigin!=='null')targetOrigin=referrerOrigin}catch(ignore){}
    try{window.parent.postMessage({type:'probiga-trading-v3-resize',height:height},targetOrigin)}catch(ignore){}
  }
  function requestResult(key,promise,fallback){
    return promise.then(function(payload){var value=unwrap(payload);return {key:key,ok:true,value:value==null?fallback:value}}).catch(function(err){return {key:key,ok:false,error:errorText(err),fallback:fallback}})
  }
  function holdingStrategyPayload(promise){return promise.then(function(v){if(String(v.status||'ok').toLowerCase()==='error')throw new Error(v.error||'持仓策略不可用');return {rows:Array.isArray(v.data)?v.data:[],summary:v.summary||{},market_context:v.market_context||{},trade_date:v.trade_date,historical_read_only:v.historical_read_only,knowledge_cutoff:v.knowledge_cutoff,execution_authority:v.execution_authority,decision_run_uid:v.decision_run_uid,decision_at:v.decision_at,decision_data_date:v.decision_data_date,decision_session_date:v.decision_session_date,status:v.status}})}
  function verificationFlags(payload,row){
    payload=payload||{};row=row||{};
    var marker=[payload.verification_status,payload.evidence_provenance_status,payload.reason_codes,row.verification_status,row.evidence_provenance_status,row.evidence_source,row.failure_codes,row.reason_codes].filter(Boolean).join(' ').toUpperCase();
    var negativeEvidence=/UNVERIFIED|INVALID|STALE|BLOCKED/.test(marker);
    var verified=(payload.evidence_verified===true||row.evidence_verified===true||payload.verified===true||row.verified===true||/(^|[^A-Z])VERIFIED([^A-Z]|$)|PERSISTED_VERIFIED/.test(marker))&&!negativeEvidence;
    var persisted=payload.persisted===true||row.persisted===true||!!(row.learning_run_id&&row.evidence_hash)||!!((row.gate_id||row.gate_evaluation_id)&&(row.evidence_hash||row.policy_hash));
    var policyMismatch=payload.policy_hash_matches===false||row.policy_hash_matches===false||/POLICY[^ ]*_?MISMATCH|POLICY HASH MISMATCH|GATE_POLICY_STALE|POLICY_STALE/.test(marker);
    var validUntil=row.evidence_valid_until||payload.evidence_valid_until,validDate=validUntil?new Date(validUntil):null;
    var expired=payload.evidence_expired===true||payload.expired===true||row.evidence_expired===true||row.expired===true||/EXPIRED|STALE_EVIDENCE|GATE_EVIDENCE_STALE|EVIDENCE_STALE/.test(marker)||!!(validDate&&Number.isFinite(validDate.getTime())&&validDate.getTime()<Date.now());
    var preview=/UNVERIFIED_PREVIEW|PREVIEW_ONLY/.test(marker)||String(row.status||payload.status||'').toUpperCase()==='UNVERIFIED_PREVIEW';
    return {persisted:persisted&&!preview,verified:verified&&!preview,policyMismatch:policyMismatch,expired:expired,preview:preview,eligible:persisted&&verified&&!policyMismatch&&!expired&&!preview}
  }
  function latestPersisted(result,singular,plural){
    result=result||{};var direct=result[singular]||result['latest_'+singular];if(direct&&typeof direct==='object')return direct;var rows=result[plural]||[];if(Array.isArray(rows)&&rows.length)return rows[0];if(singular==='learning_run'&&result.learning_run_id)return result;return {}
  }
  function loadResearchExtensions(view,runUid,loadSeq){
    state.researchErrors={};var requests=[];
    function add(key,promise,fallback){requests.push(requestResult(key,promise,fallback))}
    if(view==='validation'){
      if(runUid)add('batchDiff',api3('/decision-runs/'+encodeURIComponent(runUid)+'/diff'),{});
      if(runUid)add('horizonValidation',api3('/research/horizons/latest?run_uid='+encodeURIComponent(runUid)+'&limit=300'),{});else state.researchErrors.horizonValidation='统一上下文没有 run_uid，不能读取同批次多周期契约';
      if(runUid)add('decisionIntelligence',api3('/research/decision-intelligence/latest?run_uid='+encodeURIComponent(runUid)),{});else state.researchErrors.decisionIntelligence='统一上下文没有 run_uid，不能读取同批次决策智能快照';
    }
    if(view==='missed'){
      add('counterfactualResearch',api3('/research/learning/latest'),{});
    }
    if(view==='evidence'){
      add('shadowPreview',api3('/research/shadow/status'),{});
    }
    if(!requests.length)return Promise.resolve();
    return Promise.all(requests).then(function(results){if(loadSeq!==state.loadSeq)return;results.forEach(function(result){if(result.ok){state[result.key]=result.value;return}state[result.key]=result.fallback;state.researchErrors[result.key]=result.error})})
  }
  function load(){
    var loadSeq=++state.loadSeq;
    state.loading=true;
    el('updatedAt').textContent='读取中';
    el('truthContext').setAttribute('aria-busy','true');
    var view=state.activeView||requestedView();
    renderAll();activateView(view);notifyParentResize();
    var contextPath='/context'+(state.requestedDate?'?'+dateParam():''),requests=[
      ['context',api3(contextPath),{}],
      ['marketClock',fetchJson('/api/hot-data/market-clock'),{}],
      ['readiness',api3('/readiness'),{}],
      ['overview',api3(withDate('/overview'+(view==='overview'?'':'?compact=true'))),{}],
      ['account',api2('/accounts/paper-main-v2'),{}],
      ['paperLedger',api3('/paper-ledger?account_id=paper-main-v2&limit=200'),{}],
      ['tasks',fetchJson('/api/scheduler/tasks'),[]]
    ];
    function add(key,promise,fallback){requests.push([key,promise,fallback])}
    if(view==='overview'){add('forecasts',api3(withDate('/forecasts/latest?limit=200')),[]);add('oversold',api3(withDate('/forecasts/latest?strategy_key=oversold_reversal&limit=200')),[]);add('targets',api3(withDate('/portfolio/latest')),[]);add('watchlistStrategy',holdingStrategyPayload(fetchJson('/api/portfolio/holding-strategy'+(state.requestedDate?'?'+dateParam():''))),{})}
    if(view==='hypotheses')add('hypotheses',api3(withDate('/hypotheses/latest?limit=300')),[]);
    if(view==='candidates'){
      add('stockPool',stockPoolWithHistoricalFallback(state.requestedDate),{});
      add('auctionGate',api3('/premarket/auction-gate'+(state.requestedDate?'?trade_date='+encodeURIComponent(state.requestedDate):'')),{});
    }
    if(view==='intraday')add('intraday',api2('/accounts/paper-main-v2/intraday?limit=200'),{});
    if(view==='portfolio')add('targets',api3(withDate('/portfolio/latest')),[]);
    if(view==='positions'){add('positions',api2('/accounts/paper-main-v2/positions'),[]);add('targets',api3(withDate('/portfolio/latest')),[]);add('watchlistStrategy',holdingStrategyPayload(fetchJson('/api/portfolio/holding-strategy'+(state.requestedDate?'?'+dateParam():''))),{})}
    if(view==='validation'){add('validation',api3('/validation/latest'),null);add('dataEvidence',api2('/system/data-evidence'),{});add('researchGovernance',governanceApi(),{})}
    if(view==='missed'){add('recall',api3('/opportunity-recall/latest'),null);add('learning',api3('/learning/oversold_reversal'),{});add('researchGovernance',governanceApi(),{})}
    if(view==='evidence'){add('dataEvidence',api2('/system/data-evidence'),{});add('researchGovernance',governanceApi(),{})}
    return Promise.all(requests.map(function(item){return requestResult(item[0],item[1],item[2])})).then(function(results){
      if(loadSeq!==state.loadSeq)return;
      state.errors={};state.staleKeys={};
      results.forEach(function(result){
        if(result.ok){state[result.key]=result.value;state.loadedKeys[result.key]=true;return}
        state.errors[result.key]=result.error;
        if(state.loadedKeys[result.key])state.staleKeys[result.key]=true;else state[result.key]=result.fallback;
      });
      var paperLedger=state.paperLedger||{};
      if(paperLedger.account)state.account=paperLedger.account;
      if(view==='positions')state.positions=(paperLedger.positions||[]).concat(paperLedger.today_closed_positions||[]);
      function finish(){if(loadSeq!==state.loadSeq)return;state.loading=false;var failed=Object.keys(state.errors),stamp=new Date().toLocaleTimeString('zh-CN',{hour12:false});el('updatedAt').textContent=failed.length?'部分读取失败 · '+stamp:'刷新于 '+stamp;renderAll();activateView(view);el('truthContext').setAttribute('aria-busy','false');notifyParentResize()}
      var runUid=String((state.context||{}).run_uid||((state.overview||{}).run||{}).run_uid||'');
      function finishExtensions(){return loadResearchExtensions(view,runUid,loadSeq).then(finish)}
      if(runUid&&['portfolio','positions','orders','evidence'].indexOf(view)>=0){return requestResult('lineage',api3('/decision-runs/'+encodeURIComponent(runUid)+'/lineage'),{}).then(function(result){if(loadSeq!==state.loadSeq)return;if(result.ok){state.lineage=result.value;state.loadedKeys.lineage=true}else{state.errors.lineage=result.error;if(state.loadedKeys.lineage)state.staleKeys.lineage=true;else state.lineage={}}return finishExtensions()})}
      return finishExtensions();
    }).catch(function(err){if(loadSeq!==state.loadSeq)return;state.loading=false;state.errors.load=errorText(err);el('updatedAt').textContent='读取失败';renderTruthContext();el('heroTitle').textContent='接口读取失败';el('heroReason').textContent=errorText(err);el('hero').classList.add('blocked');el('truthContext').setAttribute('aria-busy','false');notifyParentResize()})
  }
  function renderAll(){renderTruthContext();renderChrome();renderActions();renderOverview();renderWatchlistStrategy();renderThemeAudit();renderHypotheses();renderCandidates();renderIntraday();renderPortfolio();renderPositions();renderOrders();renderValidation();renderRecall();renderLearning();renderEvidence();renderLineage();renderBatchDiff();renderAdvisoryResearch();renderHorizonResearch();renderCounterfactualResearch();renderShadowGovernance()}
  function truthState(){
    var ctx=state.context||{},run=(state.overview||{}).run||{};
    if(state.loading)return {code:'LOADING',reason:'正在核对决策批次、证据时点和执行权限；完成前不展示空白结论。'};
    if(state.errors.context)return {code:'UNAVAILABLE',reason:'统一决策上下文读取失败，当前页面不能解释为无机会或空仓。'};
    if(state.staleKeys.context)return {code:'STALE',reason:'统一决策上下文刷新失败，正在展示上次成功快照；禁止据此发起新动作。'};
    if(state.errors.overview)return {code:'UNAVAILABLE',reason:'同批次决策投影读取失败；即使统一上下文仍可读，页面也不能把缺少的目标投影解释为空仓。'};
    if(state.staleKeys.overview)return {code:'STALE',reason:'同批次决策投影刷新失败，页面正在展示旧投影；禁止据此形成当前交易结论。'};
    if(ctx.run_uid&&run.run_uid&&String(ctx.run_uid)!==String(run.run_uid))return {code:'UNAVAILABLE',reason:'统一上下文与页面决策投影的 run_uid 不一致；系统按 fail-closed 处理。'};
    var runStatus=String(ctx.run_status||run.status||'').toUpperCase(),dataStatus=String(ctx.data_status||'').toUpperCase(),decision=String(ctx.decision_status||run.decision_status||'').toUpperCase();
    var loading=/^(PROCESSING|CREATED|RUNNING|QUEUED|LOADING|DECISION_COMMITTED|POSITIONS_SYNCED)$/.test(runStatus)||/^(PROCESSING|CREATED|RUNNING|QUEUED|LOADING|DECISION_COMMITTED|POSITIONS_SYNCED)$/.test(dataStatus)||/^(PROCESSING|CREATED|RUNNING|QUEUED|LOADING|DECISION_COMMITTED|POSITIONS_SYNCED)$/.test(decision);
    if(loading)return {code:'LOADING',reason:'决策批次仍在生成或归集证据；当前内容不是最终结论，禁止发起新动作。'};
    if(ctx.historical_read_only===true)return {code:'STALE',reason:'当前请求的是历史决策会话，只用于复核；模拟账户、持仓和执行权限仍是当前快照。'};
    var validUntil=ctx.valid_until?new Date(ctx.valid_until):null;
    if(ctx.context_date_matches===false||(validUntil&&Number.isFinite(validUntil.getTime())&&validUntil.getTime()<Date.now()))return {code:'STALE',reason:'决策会话日与请求上下文不匹配，或证据已超过有效期；结果仅供历史复核。'};
    if(/FAILED|ERROR|UNAVAILABLE/.test(runStatus)||/FAILED|ERROR|UNAVAILABLE|SCHEMA_MISSING/.test(dataStatus)||/FAILED|ERROR|UNAVAILABLE|SCHEMA_MISSING/.test(decision))return {code:'UNAVAILABLE',reason:'决策批次或依赖不可用，禁止形成交易结论。'};
    if(/BLOCK|REJECT|HALT|PAUSE/.test(dataStatus)||/BLOCK|REJECT|HALT|PAUSE/.test(decision))return {code:'BLOCKED',reason:'数据或决策门禁未通过，不允许新增模拟订单。'};
    if(runStatus!=='COMPLETED'||dataStatus!=='READY'||ctx.decision_integrity_verified!==true||!ctx.run_uid||!ctx.decision_session_date||!ctx.data_date)return {code:'UNAVAILABLE',reason:'决策批次状态、时点或完整性未被服务端明确验证；禁止页面自行补成 READY 或 EMPTY。'};
    var rawTargetCount=ctx.target_count,targetCount=Number(rawTargetCount),targetCountVerified=rawTargetCount!==null&&rawTargetCount!==''&&Number.isInteger(targetCount)&&targetCount>=0;
    if(!targetCountVerified)return {code:'UNAVAILABLE',reason:'决策目标账本计数未验证，不能解释为空仓或有可用目标。'};
    var overviewTargets=((run.portfolio||{}).targets);
    if(!Array.isArray(overviewTargets)||overviewTargets.length!==targetCount)return {code:'UNAVAILABLE',reason:'统一上下文与同批次目标投影的计数不一致；页面禁止根据空数组二次推断 EMPTY。'};
    if(decision==='READY'||decision==='CANDIDATE_AVAILABLE')return targetCount>0?{code:'READY',reason:'决策快照可用；具体动作仍受研究、模拟和真实权限边界约束。'}:{code:'UNAVAILABLE',reason:'服务端声明有候选，但目标账本计数为 0；按 fail-closed 处理。'};
    if(decision==='EMPTY')return targetCount===0?{code:'EMPTY',reason:'批次与完整性已验证，且目标账本为 0；这是可解释的主动空仓。'}:{code:'UNAVAILABLE',reason:'服务端声明 EMPTY，但目标账本非空；按 fail-closed 处理。'};
    return {code:'UNAVAILABLE',reason:'决策状态未知或上下文不完整；系统按 fail-closed 处理，不能形成交易结论。'};
  }
  function setAxis(id,kind,label,detail){var node=el(id);node.className='strategy-status-card '+kind;node.querySelector('strong').textContent=label;node.querySelector('small').textContent=detail}
  function renderTruthContext(){
    var ctx=state.context||{},run=(state.overview||{}).run||{},truth=truthState(),ready=state.readiness||{};
    var requested=ctx.requested_date||state.requestedDate||'—',sessionDate=ctx.decision_session_date||ctx.requested_date||run.decision_session_date||'—',dataDate=ctx.data_date||run.trade_date||'—',expectedDataDate=ctx.expected_data_date||dataDate;
    el('truthContext').dataset.state=truth.code;el('truthState').textContent=truth.code;el('truthStateReason').textContent=truth.reason;
    el('ctxRequestedDate').textContent=requested;el('ctxDecisionSessionDate').textContent=sessionDate;el('ctxDataDate').textContent=dataDate;el('ctxExpectedDataDate').textContent=expectedDataDate;el('ctxRunUid').textContent=ctx.run_uid||run.run_uid||'—';el('ctxDecisionAt').textContent=ctx.decision_at||run.decision_at||'—';el('ctxEvidenceAsOf').textContent=ctx.evidence_as_of||run.evidence_as_of||run.decision_at||'—';el('ctxValidUntil').textContent=ctx.valid_until||run.valid_until||'—';
    var scope=String(ctx.decision_scope||run.decision_scope||'RESEARCH_ONLY').toUpperCase(),paperAuthority=String(ctx.paper_order_authority||'').toUpperCase(),executionAuthority=String(ctx.execution_authority||'').toUpperCase();
    var researchAllowed=truth.code==='READY'||truth.code==='EMPTY',researchLabel=truth.code==='UNAVAILABLE'?'不可用':truth.code==='LOADING'?'等待决策':truth.code==='STALE'?'历史复核':truth.code==='BLOCKED'?'门禁阻断':'研究可读';
    setAxis('researchAuthority',researchAllowed?'allowed':truth.code==='UNAVAILABLE'?'blocked':'limited',researchLabel,truth.code==='LOADING'?'批次完成后才形成研究结论':truth.code==='STALE'?'只读历史证据，不代表当前时点':truth.code==='BLOCKED'?'保留阻断证据，不形成可执行结论':'研究结论只形成证据与排序');
    var paperAllowed=truth.code==='READY'&&scope!=='RESEARCH_ONLY'&&ready.execution_ready===true&&paperAuthority==='V2_GATED'&&executionAuthority==='V2_CANONICAL_LEDGER';
    var paperLabel=paperAllowed?'模拟可入队':truth.code==='LOADING'?'等待批次':truth.code!=='READY'?'不可入队':scope==='RESEARCH_ONLY'?'RESEARCH_ONLY':state.errors.readiness?'执行权限不可用':'执行复验阻断';
    setAxis('paperAuthority',paperAllowed?'allowed':scope==='RESEARCH_ONLY'&&truth.code==='READY'?'limited':'blocked',paperLabel,paperAllowed?'成交前仍会复验账户 / 现金 / T+1 / 行情':'研究排序绝不升级为可执行目标');
    setAxis('realAuthority','blocked','固定关闭','本页面没有真实订单权限');
    var errors=Object.keys(state.errors||{}).map(function(key){return key+'：'+state.errors[key]});el('truthError').hidden=!errors.length;el('truthError').textContent=errors.length?'读取错误｜'+errors.join('；'):'';
    el('lineageStatus').textContent=(ctx.run_uid||run.run_uid)?'批次 '+(ctx.run_uid||run.run_uid):'等待统一批次';el('lineageStatus').className='badge '+(truth.code==='READY'||truth.code==='EMPTY'?'safe':truth.code==='BLOCKED'||truth.code==='STALE'||truth.code==='LOADING'?'warning':'danger');el('lineageEvidence').textContent='evidence_as_of '+(ctx.evidence_as_of||run.evidence_as_of||run.decision_at||'—');el('lineageRun').textContent='run_uid '+(ctx.run_uid||run.run_uid||'—');
  }
  function emptyFor(key,cols,text){return state.errors[key]?'<tr><td class="empty empty-state-error" colspan="'+cols+'">数据不可用：'+esc(state.errors[key])+'；这不代表结果为空</td></tr>':empty(cols,text)}
  function decisionEmptyFor(key,cols,text){
    if(state.errors[key])return emptyFor(key,cols,text);
    var truth=truthState();
    if(truth.code==='LOADING')return empty(cols,'决策批次仍在生成；当前没有记录不是空态结论。');
    if(truth.code==='UNAVAILABLE')return '<tr><td class="empty empty-state-error" colspan="'+cols+'">决策真值不可用；不能据此判断结果为空。</td></tr>';
    if(truth.code==='BLOCKED')return empty(cols,'决策门禁已阻断；缺少记录不得解释为正常空态。');
    if(truth.code==='STALE')return empty(cols,'当前是历史或过期快照；缺少记录不代表现在无机会。');
    return empty(cols,text);
  }
  function dailyActionGate(){
    var ctx=state.context||{},truth=truthState(),daily=task('trading_v3_close_decision');
    if(state.errors.context||state.staleKeys.context)return {allowed:false,reason:'UNAVAILABLE：统一上下文不可用，无法确认当前决策会话；手动按钮按 fail-closed 禁用。'};
    if(ctx.historical_read_only!==false)return {allowed:false,reason:ctx.historical_read_only===true?'历史上下文只读：手动日级重跑只支持服务端确认的当前会话。':'UNAVAILABLE：服务端未明确 historical_read_only=false，不能安全提交当前日任务。'};
    if(state.errors.tasks)return {allowed:false,reason:'UNAVAILABLE：调度任务状态读取失败，无法确认是否已有任务运行；手动按钮按 fail-closed 禁用。'};
    if(!daily.id)return {allowed:false,reason:'日级任务未注册，无法可靠启动或追踪。'};
    if(daily.enabled===false||daily.enabled===0)return {allowed:false,reason:'日级任务已禁用；请先由运维恢复任务配置。'};
    if(state.activeJobId)return {allowed:false,reason:'已有日级任务正在跟踪 · job_id '+state.activeJobId+'；完成前不可重复提交。'};
    if(daily.last_run_status==='running')return {allowed:false,reason:'日级任务正在执行，完成前不可重复提交。'};
    if(truth.code==='STALE'||truth.code==='LOADING')return {allowed:false,reason:truth.code+'：'+truth.reason+' 手动日级任务已禁用。'};
    if(['READY','EMPTY','BLOCKED','UNAVAILABLE'].indexOf(truth.code)<0)return {allowed:false,reason:'当前决策状态无法安全解释，手动日级任务按 fail-closed 禁用。'};
    return {allowed:true,recovery:truth.code==='BLOCKED'||truth.code==='UNAVAILABLE',reason:''}
  }
  function renderActions(){
    var daily=task('trading_v3_close_decision'),gate=dailyActionGate();
    el('runDaily').disabled=!gate.allowed;
    if(state.actionMessage){el('actionStatus').textContent=state.actionMessage;return}
    if(!gate.allowed){el('actionStatus').textContent=gate.reason;return}
    function brief(row,label){if(!row.id)return label+'：任务未注册';var s=row.last_run_status==='success'?'成功':row.last_run_status==='running'?'执行中':row.last_run_status==='failed'?'失败':row.last_run_status||'未执行';return label+'：'+s+' '+(row.last_run_at||'')}
    el('actionStatus').textContent=(gate.recovery?'当前批次不可用或已阻断；允许重跑仅用于恢复，仍不绕过统一执行门禁。 ':brief(daily,'日级')+'；')+'盘中扫描由受控调度运行，本页只读';
  }
  function renderChrome(){
    var run=state.overview.run||{},portfolio=run.portfolio||{},regime=run.regime||{},account=state.account||{},equity=account.latest_equity||{},ledgerSummary=(state.paperLedger||{}).summary||{};
    var ready=state.readiness.paper_ready===true&&state.readiness.paper_authority_ready===true&&state.readiness.execution_ready===true,targets=portfolio.targets||state.targets||[],scope=String((state.context||{}).decision_scope||run.decision_scope||'RESEARCH_ONLY').toUpperCase(),actionableTargets=ready&&scope!=='RESEARCH_ONLY'?targets.filter(isExecutableTarget):[];
    var discoveryTargets=actionableTargets.filter(isDiscoveryTarget);
    var formalTargets=actionableTargets.filter(function(x){return discoveryTargets.indexOf(x)<0}),researchDiscoveryTargets=targets.filter(isDiscoveryTarget),researchFormalTargets=targets.filter(function(x){return researchDiscoveryTargets.indexOf(x)<0});
    var displayEquity=firstNumber([ledgerSummary.display_total_equity,ledgerSummary.canonical_total_equity,equity.total_equity,account.cash_balance]);
    var displayCash=firstNumber([ledgerSummary.display_cash_balance,ledgerSummary.canonical_cash_balance,equity.cash_balance,account.cash_balance]);
    var displayPnl=firstNumber([ledgerSummary.total_unrealized_pnl])||0;
    var accountScope=String(ledgerSummary.display_account_scope||'V2_CANONICAL');
    var scopeText=accountScope==='LEGACY_EVENT_SIM_ACTIVE'?'当前持仓来自事件模拟账本；空仓主模拟账本未重复叠加':accountScope==='MERGED_LEDGER'?'主模拟账本与事件模拟账本均有持仓，按独立账户合计':'当前持仓来自统一主模拟账本';
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
    el('positionCount').textContent=String(positionCount)+' 只'+(positionLots!==positionCount?' / '+String(positionLots)+' 笔':'');
    el('validated').textContent=String(run.validated_count==null?'0':run.validated_count);
    el('sleeves').textContent=(state.readiness.active_calibrated_sleeves||[]).map(strategy).join(' / ')||'暂无策略通过样本外闸门';
    var opportunityAudit=(run.portfolio||{}).opportunity_audit||{},limits=state.readiness.portfolio_limits||{};
    var targetCap=researchFormalTargets.length?Number(limits.maximum_positions||limits.maximum_live_positions||0):Number(limits.maximum_paper_discovery_positions||opportunityAudit.maximum_paper_positions||0);
    if(!targetCap)targetCap=Number(limits.maximum_live_positions||12);
    el('targetCount').textContent=String(targets.length)+' / '+String(targetCap);
    el('targetPolicy').textContent='正式组合最多 '+String(limits.maximum_positions||limits.maximum_live_positions||'—')+' 只；模拟试错最多 '+String(limits.maximum_paper_discovery_positions||'—')+' 只；加仓最多 '+String(limits.maximum_add_count==null?'—':limits.maximum_add_count)+' 次';
    var truth=truthState(),holdingSummary=(state.watchlistStrategy||{}).summary||{},holdingRows=(state.watchlistStrategy||{}).rows||[],urgentHolding=holdingRows.find(function(x){return x.exit_intent==='SELL'||x.exit_intent==='REDUCE'});
    var holdingCount=Number(holdingSummary.holding_count||holdingRows.length||0),sellCount=Number(holdingSummary.sell_count||0),reduceCount=Number(holdingSummary.reduce_count||0),urgentCount=sellCount+reduceCount;
    var redlineCount=holdingRows.filter(function(x){var emergency=x.emergency_exit||{};return emergency.direct===true||emergency.price!=null||x.exit_intent==='SELL'}).length;
    el('actualHoldingCount').textContent=String(holdingCount)+' 只';
    el('urgentActionCount').textContent=String(urgentCount)+' 只';
    el('urgentActionSummary').textContent=sellCount?'退出 '+String(sellCount)+' · 减仓 '+String(reduceCount):reduceCount?'减仓 '+String(reduceCount):holdingCount?'当前无强制卖出':'尚未登记真实持仓';
    el('allowedBuyCount').textContent=String(urgentCount||truth.code!=='READY'?0:actionableTargets.length)+' 只';
    el('intradayRedlineCount').textContent=String(redlineCount)+' 条';
    if(state.errors.watchlistStrategy){el('todayFirstAction').textContent='冻结新增风险，先恢复持仓数据';el('todayFirstActionReason').textContent='持仓策略读取失败，不能把缺失数据解释为继续持有。'}
    else if(sellCount){el('todayFirstAction').textContent='先处理 '+String(sellCount)+' 只退出持仓';el('todayFirstActionReason').textContent=urgentHolding?(urgentHolding.short_name||urgentHolding.stock_code)+'：'+(urgentHolding.action||urgentHolding.reason||'立即卖出'):'退出动作优先于所有新增机会。'}
    else if(reduceCount){el('todayFirstAction').textContent='先处理 '+String(reduceCount)+' 只减仓持仓';el('todayFirstActionReason').textContent=urgentHolding?(urgentHolding.short_name||urgentHolding.stock_code)+'：'+(urgentHolding.action||urgentHolding.reason||'分批减仓'):'先降低已有风险，再看策略池。'}
    else if(holdingCount){el('todayFirstAction').textContent='开盘前检查 '+String(holdingCount)+' 只持仓红线';el('todayFirstActionReason').textContent='没有触发退出前继续持有；触发卖出范围或突发红线就执行。'}
    else{el('todayFirstAction').textContent='先确认自选股持仓是否已登记';el('todayFirstActionReason').textContent='成本价、股数和买入日期完整后，系统才会连续给出后续操作。'}
    if(!state.errors.watchlistStrategy&&Number(holdingSummary.sell_count||0)>0){el('heroTitle').textContent='自选股持仓有 '+String(holdingSummary.sell_count)+' 只需要立即退出';el('heroReason').textContent=urgentHolding?(urgentHolding.short_name||urgentHolding.stock_code)+'：'+(urgentHolding.reason||urgentHolding.action):'持仓退出信号优先于新增买入候选；请先处理风险。';el('hero').classList.add('blocked')}
    else if(!state.errors.watchlistStrategy&&Number(holdingSummary.reduce_count||0)>0){el('heroTitle').textContent='自选股持仓有 '+String(holdingSummary.reduce_count)+' 只需要优先减仓';el('heroReason').textContent=urgentHolding?(urgentHolding.short_name||urgentHolding.stock_code)+'：'+(urgentHolding.reason||urgentHolding.action):'先降低现有持仓风险，再评估新的买入机会。';el('hero').classList.add('blocked')}
    else if(truth.code==='UNAVAILABLE'){el('heroTitle').textContent='数据不可用，不能形成交易结论';el('heroReason').textContent=truth.reason;el('hero').classList.add('blocked')}
    else if(truth.code==='STALE'){el('heroTitle').textContent='正在展示过期或上次成功快照';el('heroReason').textContent=truth.reason;el('hero').classList.add('blocked')}
    else if(truth.code==='LOADING'){el('heroTitle').textContent='决策批次仍在生成';el('heroReason').textContent=truth.reason;el('hero').classList.add('blocked')}
    else if(truth.code==='BLOCKED'){el('heroTitle').textContent='决策门禁已阻断';el('heroReason').textContent=truth.reason;el('hero').classList.add('blocked')}
    else if(scope==='RESEARCH_ONLY'&&targets.length){el('heroTitle').textContent='已形成研究目标，但不拥有订单权限';el('heroReason').textContent='RESEARCH_ONLY 仅用于证据与排序，不会升级为模拟或真实买单。';el('hero').classList.add('blocked')}
    else if(truth.code==='READY'&&ready===false){el('heroTitle').textContent='研究决策有效，模拟执行门禁阻断';el('heroReason').textContent='研究真值保持 READY；执行复验只影响模拟执行轴，不会改写研究结论。';el('hero').classList.add('blocked')}
    else if(!run.run_uid){el('heroTitle').textContent='尚未产生首个统一决策';el('heroReason').textContent='完成样本外验收后才会启用模拟组合。';el('hero').classList.add('blocked')}
    else if(ready&&formalTargets.length){el('heroTitle').textContent='发现扣费后正期望目标，进入模拟组合';el('heroReason').textContent=discoveryTargets.length?'正式目标通过全部闸门；同时有左侧实验小仓收集前向证据。':'目标已经通过样本外、成本、仓位、主题和开放风险约束。';el('hero').classList.remove('blocked')}
    else if(ready&&discoveryTargets.length){el('heroTitle').textContent='左侧实验触发，进入模拟盘小仓试错';el('heroReason').textContent='这不是“已证明能赚钱”的推荐；系统会动态止损，并把成功与失败都写入下一版校准样本。';el('hero').classList.remove('blocked')}
    else if(positionCount){el('heroTitle').textContent='本次没有新增目标，继续管理现有 '+String(positionCount)+' 只持仓';el('heroReason').textContent='现有持仓和盈亏继续按模拟账本管理；本次统一决策没有新增可执行买入目标。';el('hero').classList.add('blocked')}
    else{el('heroTitle').textContent='当前没有值得出手的组合，保持现金';el('heroReason').textContent=ready?'没有股票同时通过净期望和组合约束，空仓不是故障。':'新公式尚未形成排序可信的正期望模型，旧目标已隔离，自动模拟买入暂停。';el('hero').classList.add('blocked')}
  }
  function renderOverview(){
    var run=state.overview.run||{},portfolio=run.portfolio||{},regime=run.regime||{},targets=portfolio.targets||state.targets||[],truth=truthState();
    var discoveryTargets=targets.filter(isDiscoveryTarget),uncalibratedOnly=targets.length>0&&discoveryTargets.length===targets.length;
    el('decisionStatus').textContent=truth.code;el('decisionStatus').title=portfolio.status?'source portfolio.status = '+String(portfolio.status):'';el('decisionStatus').className='badge '+(truth.code==='READY'||truth.code==='EMPTY'?'safe':truth.code==='UNAVAILABLE'?'danger':'warning');
    el('decisionFacts').innerHTML=[
      fact('决策日期',run.trade_date||'—'),fact('市场状态',run.dominant_regime||regime.dominant_state||'—'),fact('目标风险资产',pct(Number(portfolio.target_risk_asset_weight||0)*100,1)),fact('目标现金',money(portfolio.target_cash)),fact('组合预期收益',uncalibratedOnly?'未校准（仅模拟试错）':pct(portfolio.expected_portfolio_return_pct)),fact('最坏开放风险',money(portfolio.worst_case_loss_cny))
    ].join('');
    var counts={};state.forecasts.forEach(function(x){counts[x.strategy_key]=(counts[x.strategy_key]||0)+(x.forecast_status==='VALIDATED_POSITIVE'?1:0)});
    el('sleeveCards').innerHTML=Object.keys(strategyNames).map(function(k){var calibrated=(state.readiness.active_calibrated_sleeves||[]).indexOf(k)>=0;return '<div class="card"><strong>'+esc(strategy(k))+'</strong><span>'+esc(calibrated?'已有样本外校准':k==='oversold_reversal'?'独立模拟试错并积累前向样本':'未校准时只观察，不借用别的策略结论')+'</span><em>'+String(counts[k]||0)+' 个通过</em></div>'}).join('');
    var oversold=(state.oversold||[]).filter(function(x){return x.forecast_status==='LEFT_SIDE_PREPARE'||x.forecast_status==='PAPER_DISCOVERY_CANDIDATE'}).sort(function(a,b){var pa=a.forecast_status==='PAPER_DISCOVERY_CANDIDATE'?0:1,pb=b.forecast_status==='PAPER_DISCOVERY_CANDIDATE'?0:1;return pa-pb||Number(b.raw_score||0)-Number(a.raw_score||0)}).slice(0,20);
    var rejected=((run.portfolio||{}).rejected||[]),targetCodes={};targets.forEach(function(x){targetCodes[String(x.stock_code).slice(0,6)]=true});var rejectionByCode={};rejected.forEach(function(x){rejectionByCode[String(x.stock_code).slice(0,6)]=x});
    el('oversoldRows').innerHTML=oversold.length?oversold.map(function(x){var triggered=x.forecast_status==='PAPER_DISCOVERY_CANDIDATE',code=String(x.stock_code).slice(0,6),reject=rejectionByCode[code],selected=targetCodes[code],action=triggered?(selected?'已进入纸面研究组合':reject?'未入选：'+(reject.reason||reject.reason_code):'等待组合分配'):'准备观察，不买';return '<tr><td>'+security(x.stock_code,x.short_name)+'</td><td>'+esc(themeText(x))+'</td><td>'+esc(status(x.forecast_status))+'</td><td class="'+(selected?'safe-text':'')+'">'+esc(action)+'</td><td>'+num(x.raw_score,3)+'</td><td>'+pct(x.initial_stop_pct)+'</td><td class="reason">'+esc((x.reasons||[]).join('；'))+'</td></tr>'}).join(''):decisionEmptyFor('oversold',7,'当前没有股票进入左侧抄底准备区；系统仍会保留全部未触发记录供后续反事实复盘');
    el('overviewTargets').innerHTML=targets.length?targets.map(function(x){var calibrated=!isDiscoveryTarget(x);return '<tr><td>'+security(x.stock_code,x.stock_name||x.short_name)+'</td><td>'+pct(Number(x.target_weight)*100,1)+'</td><td>'+num(x.target_quantity,0)+'</td><td class="'+(calibrated?'safe-text':'warning-text')+'">'+(calibrated?pct(x.expected_return_net_pct):'未校准')+'</td><td>'+(calibrated?pct(x.conservative_return_pct):'未校准')+'</td><td>'+pct(x.estimated_roundtrip_cost_pct)+'</td><td class="reason">'+esc(x.reason||'—')+'</td></tr>'}).join(''):decisionEmptyFor('targets',7,'已验证批次没有研究目标；研究结果不会直接形成订单');
  }
  function renderWatchlistStrategy(){
    var payload=state.watchlistStrategy||{},rows=Array.isArray(payload.rows)?payload.rows:[],summary=payload.summary||{},error=state.errors.watchlistStrategy;
    function render(statusId,summaryId,rowsId){
      var statusNode=el(statusId),summaryNode=el(summaryId),rowsNode=el(rowsId);if(!statusNode||!summaryNode||!rowsNode)return;
      statusNode.textContent=error?'UNAVAILABLE':summary.sell_count?'立即退出 '+summary.sell_count+' 只':summary.reduce_count?'优先减仓 '+summary.reduce_count+' 只':summary.wait_data_count?'有 '+summary.wait_data_count+' 只证据不完整':'持仓策略已更新';
      statusNode.className='badge '+(error||summary.sell_count?'danger':summary.reduce_count||summary.wait_data_count?'warning':'safe');
      var market=payload.market_context||{},marketCap=market.risk_asset_cap==null?'—':pct(Number(market.risk_asset_cap)*100,0);
      summaryNode.innerHTML=error?fact('读取错误',error+'；不能把策略缺失解释为继续持有'):[fact('自选持仓',String(summary.holding_count||0)+' 只'),fact('立即卖出',String(summary.sell_count||0)+' 只'),fact('分批减仓',String(summary.reduce_count||0)+' 只'),fact('继续持有',String(summary.hold_count||0)+' 只'),fact('证据不完整',String(summary.wait_data_count||0)+' 只'),fact('每日市场状态',String(market.dominant_state||'DATA_BLOCKED')+' · 风险资产 '+marketCap),fact('市场动作',String(market.market_action||'WAIT_DATA')+' · '+String(market.reason||'等待每日市场决策')),fact('统一批次',payload.decision_run_uid?String(payload.decision_run_uid).slice(0,10):'未绑定'),fact('决策日 / 数据日',String(payload.decision_session_date||'—').slice(0,10)+' / '+String(payload.decision_data_date||'—').slice(0,10)),fact('策略权限','ADVISORY_ONLY · 不自动下单')].join('');
      rowsNode.innerHTML=error?empty(8,'持仓策略不可用：'+error):rows.length?rows.map(function(x){var sell=x.sell_plan||{},emergency=x.emergency_exit||{},actionClass=x.exit_intent==='SELL'?'danger-text':x.exit_intent==='REDUCE'||x.exit_intent==='WAIT_DATA'?'warning-text':'safe-text',priceLabel=x.same_session_price?'截止时已知行情 '+String(x.price_trade_date||''):x.late_arriving_quote?'盘后补录行情 '+String(x.price_trade_date||'')+'（仅计盈亏，不倒填策略）':'非当日行情，动作冻结',costLatest=money(x.cost_price)+' / '+money(x.latest_price)+(x.pnl_pct==null?'':' · '+(Number(x.pnl_pct)>0?'+':'')+pct(x.pnl_pct)),buy=(x.buy_plan||{}),cutoff=displayDateTime(x.knowledge_cutoff||x.evaluated_at);return '<tr data-intent="'+esc(x.exit_intent||'WAIT_DATA')+'"><td>'+security(x.stock_code,x.short_name)+'<small class="holding-meta">'+esc((x.position_date||'日期未知')+' · '+num(x.shares,0)+' 股')+'</small></td><td class="'+pnlClass(x.pnl)+'">'+esc(costLatest)+'<small class="holding-plan-label">'+esc(priceLabel+' · '+(x.price_source||'未知来源'))+'</small></td><td><strong class="'+actionClass+'">'+esc(x.action||'—')+'</strong><small class="holding-reason">'+esc(x.reason||'—')+'</small></td><td>'+esc(priceRange(buy.range))+'<small class="holding-plan-label">'+esc(buy.label||'—')+'</small></td><td>'+esc(priceRange(sell.range))+'<small class="holding-plan-label">'+esc(sell.label||'—')+'</small></td><td class="'+(emergency.direct?'danger-text':'')+'">'+esc(emergency.price==null?'—':money(emergency.price))+'<small class="holding-plan-label">'+esc(emergency.label||'—')+'</small></td><td>'+esc(x.next_session_plan||'—')+'</td><td>'+esc(cutoff)+'<small class="holding-plan-label">'+esc(payload.historical_read_only?'历史只读复盘':'每 60 秒低频更新')+'</small></td></tr>'}).join(''):empty(8,'自选股里尚未登记“成本价 + 持仓股数”；登记后会从买入日起持续给出操作建议');
    }
    render('watchlistStrategyStatus','watchlistStrategySummary','watchlistStrategyRows');
    render('watchlistPositionStatus','watchlistPositionSummary','watchlistPositionRows');
  }
  function renderThemeAudit(){
    var run=state.overview.run||{},audit=(run.portfolio||{}).opportunity_audit||{},groups=audit.research_groups||[],themes=audit.dynamic_theme_radar||audit.opportunity_themes||[],warnings=audit.warnings||[];
    var warningNames={UNEXPLAINED_CANDIDATE_OMISSION:'存在无解释落选',CANDIDATE_THEME_MISSING:'存在主题标签缺失',TARGET_THEME_CONCENTRATION:'入选目标共享主题较多'};
    var label=audit.status==='PASS'?'覆盖正常':audit.status==='ATTENTION'?'需要主动复核':'等待审计';
    el('themeAuditStatus').textContent=label;el('themeAuditStatus').className='badge '+(audit.status==='PASS'?'safe':audit.status==='ATTENTION'?'warning':'');
    el('themeAuditFacts').innerHTML=[
      fact('全量覆盖',String(audit.universe_stock_count||0)+' 只 / '+String(audit.forecast_count||0)+' 条策略预测'),
      fact('纸面候选',String(audit.candidate_count||0)+' 只'),
      fact('已入选 / 已解释落选',String(audit.selected_count||0)+' / '+String(audit.rejected_count||0)),
      fact('无原因落选',String(audit.unexplained_unselected_count||0)+' 只'),
      fact('主题标签缺失',String(audit.missing_theme_count||0)+' 只'),
      fact('共同集中主题',(audit.selected_concentration_themes||[]).join(' / ')||'无'),
      fact('主动预警',warnings.map(function(x){if(String(x).indexOf('HIGH_SCORE_RESEARCH_GROUP_UNSELECTED:')===0)return String(x).split(':').slice(1).join(':')+'高分候选全部落选';return warningNames[x]||x}).join('；')||'无')
    ].join('');
    el('researchGroupRows').innerHTML=groups.length?groups.map(function(x){var top=x.top_candidate||{},signal=x.top_signal||{},reason=x.status==='COVERED'?'已有候选入选':x.status==='HIGH_SCORE_UNSELECTED'?'高分候选未入选：'+(top.reason||top.reason_code||'需复核'):x.status==='BELOW_ALERT'?'有候选但未达到预警分':'全量有覆盖，但当前没有纸面候选';return '<tr><td>'+esc(x.group)+'</td><td>'+num(x.universe_stock_count,0)+'</td><td>'+num(x.forecast_count,0)+'</td><td>'+num(x.candidate_count,0)+'</td><td>'+num(x.selected_count,0)+'</td><td>'+(signal.stock_code?security(signal.stock_code,signal.short_name)+'<span>'+esc(strategy(signal.strategy_key))+' · '+num(signal.score,3)+' · '+esc(status(signal.status))+'</span>':'—')+'</td><td class="reason '+(x.status==='HIGH_SCORE_UNSELECTED'?'warning-text':'')+'">'+esc(reason)+'</td></tr>'}).join(''):empty(7,'尚未配置研究主题组');
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
    }).join(''):decisionEmptyFor('hypotheses',10,'已验证批次在该条件下没有交易假设；盘中新发现的机会也会自动进入这里');
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
    syncFilters();api3('/hypotheses/latest?'+params.join('&')).then(function(v){state.errors.hypotheses=null;delete state.errors.hypotheses;state.hypotheses=unwrap(v)||[];renderHypotheses()}).catch(function(err){state.errors.hypotheses=errorText(err);renderHypotheses();renderTruthContext()})
  }
  function renderCandidates(){
    var strategies=Object.keys(strategyNames);if(el('strategyFilter').options.length===1){strategies.forEach(function(k){el('strategyFilter').insertAdjacentHTML('beforeend','<option value="'+k+'">'+esc(strategy(k))+'</option>')})}
    if(!el('candidateDate').value){var defaultDate=(state.stockPool||{}).requested_trade_date||state.requestedDate||(state.context||{}).decision_session_date||(state.stockPool||{}).decision_session_date||(state.stockPool||{}).trade_date||'';if(defaultDate)el('candidateDate').value=String(defaultDate).slice(0,10)}
    var sf=el('strategyFilter').value,st=el('statusFilter').value,q=el('candidateSearch').value.trim().toLowerCase();
    var pool=state.stockPool||{},gate=state.auctionGate||{},clock=state.marketClock||{},latestFormalDate=String(clock.recommendation_trade_date||clock.latest_data_date||'').slice(0,10),historicalFallback=pool.is_historical_fallback===true,governanceDeferred=pool.governance_deferred===true||pool.activation_enabled===false,formalTruth=stockPoolFormalTruth(pool,state.requestedDate||pool.requested_trade_date,latestFormalDate),formalCurrent=formalTruth.ready===true,researchOnlyDisplay=!formalCurrent;
    var gateAligned=!!pool.run_uid&&String(gate.source_run_uid||'')===String(pool.run_uid||'')&&String(gate.session_date||'').slice(0,10)===String(pool.requested_trade_date||state.requestedDate||pool.decision_session_date||'').slice(0,10),auctionByCode={};if(gateAligned)(gate.assessments||[]).forEach(function(row){auctionByCode[String(row.stock_code||'').split('.')[0]]=row});
    function changeText(x){return dailyChangeNames[String(x.daily_change||'')]||String(x.daily_change||'当日变化待生成')}
    function roleText(x){return dynamicRoleNames[String(x.dynamic_role||'')]||String(x.dynamic_role||'动态角色待生成')}
    function auctionRow(x){return auctionByCode[String(x.stock_code||'').split('.')[0]]||null}
    function auctionText(x){var row=auctionRow(x);return row?(auctionActionNames[String(row.advisory_action||'')]||String(row.advisory_action||'竞价已复核')):'尚无同批次竞价结论'}
    var source=(pool.items||[]).map(function(item){var rejection=item.rejection||{},actionability=governanceDeferred?'RESEARCH_ONLY':researchOnlyDisplay?'RESEARCH_ONLY':String(item.actionability||((item.action_plan||{}).actionability)||'RESEARCH_ONLY');return Object.assign({},item,{short_name:item.stock_name,strategy_key:(item.strategy_keys||[])[0]||'',actionability:actionability,forecast_status:actionability,reasons:[rejection.reason||rejection.reason_code].concat(item.reasons||[]).filter(Boolean)})}).filter(function(item){return item.is_strategy_candidate===true||(st==='REJECTED'&&item.actionability==='REJECTED')});
    var scoped=source.filter(function(x){return(!sf||(x.strategy_keys||[]).indexOf(sf)>=0)&&(!q||String(x.stock_code).toLowerCase().indexOf(q)>=0||String(x.short_name).toLowerCase().indexOf(q)>=0||themeText(x).toLowerCase().indexOf(q)>=0)});
    var preferredDefaultRows=scoped.filter(function(x){return x.actionability!=='RESEARCH_ONLY'&&x.actionability!=='REJECTED'}),researchDefaultRows=scoped.filter(function(x){return x.actionability==='RESEARCH_ONLY'}),researchDefaultFallback=!st&&!preferredDefaultRows.length&&researchDefaultRows.length>0;
    var rows=st?scoped.filter(function(x){return x.actionability===st}):(preferredDefaultRows.length?preferredDefaultRows:researchDefaultRows);
    var pageSize=40,pageCount=Math.max(1,Math.ceil(rows.length/pageSize));state.candidatePage=Math.max(1,Math.min(pageCount,Number(state.candidatePage||1)));var start=(state.candidatePage-1)*pageSize,pageRows=rows.slice(start,start+pageSize);
    function themeSummary(x){var full=themeText(x),parts=full.split(' / ').filter(Boolean);return parts.slice(0,3).join(' / ')+(parts.length>3?' / +'+(parts.length-3)+'项':'')}
    function plans(x){var nativePlan=x.action_plan||{},buy=nativePlan.buy_range,sell=nativePlan.sell_range,stop=firstNumber([nativePlan.protective_stop]),actionability=governanceDeferred?'RESEARCH_ONLY':researchOnlyDisplay?'RESEARCH_ONLY':String(nativePlan.actionability||x.actionability||'RESEARCH_ONLY'),buyText=buy?priceRange(buy):actionability==='WAIT_TRIGGER'?'等待触发，不买':actionability==='PAPER_ONLY'?'仅模拟，不买':actionability==='REJECTED'?'已拒绝，不买':'同批次未生成',sellText=sell?priceRange(sell):'同批次未校准，不生成',emergency=stop==null?'同批次暂无可信退出线':'跌破 '+money(stop)+' 直接退出';if(governanceDeferred){buyText='治理数据库延期，不提供当前买入计划';sellText='治理数据库延期，不提供当前卖出计划';emergency='治理数据库延期，不提供当前止损指令'}else if(historicalFallback){buyText='历史记录 · '+buyText;sellText='历史记录 · '+sellText;emergency=stop==null?'历史批次未生成退出线':'历史保护位 '+money(stop)+'（不可作为当前指令）'}else if(researchOnlyDisplay){buyText='正式票池未通过验证，不提供当前买入计划';sellText='正式票池未通过验证，不提供当前卖出计划';emergency='正式票池未通过验证，不提供当前止损指令'}return {buy:buyText,sell:sellText,emergency:emergency,actionability:actionability}}
    function card(x){var calibrated=hasCalibratedExpectation(x),auction=auctionRow(x),reasonList=(x.reasons||[]).slice();if(x.continuity_explanation)reasonList.push(x.continuity_explanation);if(auction)reasonList=reasonList.concat(auction.reasons||[]);var reasons=reasonList.join('；')||actionabilityNames[x.actionability]||x.actionability,fullTheme=themeText(x),plan=plans(x),actionLabel=governanceDeferred?'治理延期 · 只读研究':researchOnlyDisplay?'正式池阻断 · 只读研究':actionabilityNames[plan.actionability]||plan.actionability,badgeClass=historicalFallback||researchOnlyDisplay?'warning':plan.actionability==='BUY_ZONE'?'safe':plan.actionability==='REJECTED'?'danger':'warning';if(historicalFallback&&!governanceDeferred)actionLabel='历史只读 · 原'+actionLabel;return '<article class="candidate-card"><div class="candidate-card-head"><span class="candidate-rank">#'+esc(num(x.rank_no,0))+'</span>'+security(x.stock_code,x.short_name)+'<span class="badge '+badgeClass+'">'+esc(actionLabel)+'</span></div><div class="candidate-card-route"><strong>'+esc(strategyText(x))+'</strong><span title="'+esc(fullTheme)+'">'+esc(themeSummary(x))+'</span></div><div class="candidate-card-plans"><div><span>买入范围</span><strong>'+esc(plan.buy)+'</strong></div><div><span>卖出范围</span><strong>'+esc(plan.sell)+'</strong></div><div><span>突发退出</span><strong>'+esc(plan.emergency)+'</strong></div></div><div class="candidate-card-metrics"><div><span>日变化</span><strong>'+esc(changeText(x))+'</strong></div><div><span>动态角色</span><strong>'+esc(roleText(x))+'</strong></div><div><span>竞价重评</span><strong>'+esc(auctionText(x))+'</strong></div><div><span>竞价后全池排名</span><strong>'+(auction&&auction.decision_rank?'#'+esc(auction.decision_rank):'—')+'</strong></div><div><span>综合分</span><strong>'+num(x.raw_score,3)+'</strong></div><div><span>研究净期望</span><strong class="'+(calibrated&&Number(x.expected_return_net_pct)>0?'safe-text':calibrated?'':'warning-text')+'">'+(calibrated?pct(x.expected_return_net_pct):'未校准')+'</strong></div><div><span>数据来源</span><strong>'+(historicalFallback?'历史 V3 批次':formalCurrent?'当前 VERIFIED COMPLETED':'研究只读批次')+'</strong></div><div><span>批次</span><strong>'+esc(String(pool.run_uid||'—').slice(0,10))+'</strong></div></div><p class="candidate-card-reason"><b>判断依据：</b>'+esc(reasons)+'</p></article>'}
    var candidateTruth=truthState(),candidateEmptyText=candidateTruth.code==='LOADING'?'批次仍在生成，当前无记录不是空态结论':candidateTruth.code==='UNAVAILABLE'?'决策真值不可用，不能据此判断候选为空':candidateTruth.code==='BLOCKED'?'决策门禁已阻断，缺少记录不是正常空态':candidateTruth.code==='STALE'?'当前是历史或过期快照，不代表现在无机会':'已验证批次在该筛选条件下没有记录';
    if(pool.exact_run_missing&&!historicalFallback)candidateEmptyText='请求日没有 V3 决策批次，且没有更早的可读历史策略池可供回看';
    el('candidateCards').innerHTML=pageRows.length?pageRows.map(card).join(''):state.errors.stockPool?'<p class="empty empty-state-error">候选数据不可用：'+esc(state.errors.stockPool)+'；这不代表候选为空</p>':'<p class="empty '+(candidateTruth.code==='UNAVAILABLE'?'empty-state-error':'')+'">'+esc(candidateEmptyText)+'</p>';
    el('candidateRows').innerHTML=pageRows.length?pageRows.map(function(x){var calibrated=hasCalibratedExpectation(x),plan=plans(x),auction=auctionRow(x),actionLabel=actionabilityNames[plan.actionability]||plan.actionability;if(historicalFallback)actionLabel='历史只读 · 原'+actionLabel;return '<tr><td>'+num(x.rank_no,0)+'</td><td>'+security(x.stock_code,x.short_name)+'</td><td>'+esc(strategyText(x))+'</td><td title="'+esc(themeText(x))+'">'+esc(themeSummary(x))+'</td><td>'+esc(changeText(x)+' · '+roleText(x))+'</td><td>'+esc(auctionText(x)+(auction&&auction.decision_rank?' · #'+auction.decision_rank:''))+'</td><td>'+esc(plan.buy)+'</td><td>'+esc(plan.sell)+'</td><td>'+esc(plan.emergency)+'</td><td>'+num(x.raw_score,3)+'</td><td class="'+(calibrated&&Number(x.expected_return_net_pct)>0?'safe-text':calibrated?'':'warning-text')+'">'+(calibrated?pct(x.expected_return_net_pct):'未校准')+'</td><td>'+(calibrated?pct(Number(x.probability_positive)*100,1):'未校准')+'</td><td>'+esc(calibrated?ratio(x.profit_factor):'未校准')+'</td><td>'+esc(calibrated?ratio(x.payoff_ratio):'未校准')+'</td><td>'+esc(calibrated?num(x.sample_count,0):'0')+'</td><td>'+esc(actionLabel)+'</td><td class="reason">'+esc([(x.reasons||[]).join('；'),x.continuity_explanation,(auction&&auction.reasons||[]).join('；')].filter(Boolean).join('；'))+' · V3_IMMUTABLE_RUN'+(historicalFallback?' · HISTORICAL_READ_ONLY':'')+'</td></tr>'}).join(''):decisionEmptyFor('stockPool',17,candidateEmptyText);
    var decisionDate=String(pool.decision_session_date||'—').slice(0,10),dataDate=String(pool.trade_date||'—').slice(0,10),requestedDate=String(pool.requested_trade_date||state.requestedDate||'—').slice(0,10),summary=pool.summary||{};
    var historyNotice=el('candidateHistoryNotice');if(historyNotice){historyNotice.hidden=!historicalFallback;historyNotice.textContent=historicalFallback?'HISTORICAL_READ_ONLY / 历史只读：请求日 '+requestedDate+' 没有完整可验证的 V3 决策批次；当前展示严格更早的最近一次 COMPLETED 策略池（决策日 '+decisionDate+'，数据日 '+dataDate+'）。这些股票仅供历史复核，全部不可执行，也不会创建模拟或真实订单。':''}
    var researchNotice=el('candidateResearchNotice');if(researchNotice){researchNotice.hidden=formalCurrent&&!researchDefaultFallback;researchNotice.textContent=governanceDeferred?'DEFERRED_DB / 治理数据库迁移延期：策略池仅保留不可变审计行，全部为 RESEARCH_ONLY；当前不展示可执行买卖区间或止损指令，也不会创建模拟或真实订单。':!formalCurrent&&!historicalFallback?'RESEARCH_ONLY / 正式票池不可用：'+formalTruth.reason+'。旧日期、未验证或研究只读候选不会进入当前可执行展示。':researchDefaultFallback?'本批次没有“允许买入区间、等待触发或仅模拟研究”候选；当前按策略排名展示研究观察股票。它们没有可执行资格，不会创建模拟或真实订单。':''}
    var daily=pool.daily_change||{},strategyExecution=pool.strategy_execution||{},strategyRows=strategyExecution.strategies||[],dailyNotice=el('candidateDailyNotice');if(dailyNotice)dailyNotice.textContent=String(daily.status||'')==='NO_PREVIOUS_BATCH'?'DAILY CHANGE / 当前是首个可验证比较基线；在产生下一批次前，不把这些股票误称为“今日新入”。':'DAILY CHANGE / 每日重算：'+Number(daily.new_count||0)+' 只新入，'+Number(daily.retained_count||0)+' 只连续保留，'+Number(daily.upgraded_count||0)+' 只上升，'+Number(daily.downgraded_count||0)+' 只下降，'+Number(daily.removed_count||0)+' 只移出。连续保留不是静态不动，每只股票都展示当日排名或分数变化原因。';
    var auctionNotice=el('candidateAuctionNotice');if(auctionNotice){var gateSummary=gate.summary||{},gateReason=gate.reason||'',gateStatus=String(gate.status||'UNAVAILABLE');auctionNotice.textContent=state.errors.auctionGate?'AUCTION UNAVAILABLE / 竞价重评读取失败：'+state.errors.auctionGate+'；这不等于没有可买候选。':!gateAligned?'AUCTION PENDING / 尚无与当前策略批次严格对应的竞价结论；不会用其他日期或其他批次替代。':gateStatus==='COMPLETED'?'AUCTION '+String(gate.stage||'REVIEW')+' / 截止 '+displayDateTime(gate.cutoff_at)+'：全池复核 '+Number(gateSummary.reviewed_count||0)+' 只，竞价确认 '+Number(gateSummary.buy_candidate_count||0)+' 只，等待开盘 '+Number(gateSummary.wait_count||0)+' 只，数据/成交阻断 '+Number(gateSummary.blocked_count||0)+' 只，拒绝追高或弱势 '+Number(gateSummary.rejected_count||0)+' 只。备选关系只用于比较，automatic_substitution=false。':'AUCTION '+gateStatus+' / '+(gateReason||'集合竞价结论尚未形成')+'；不会提前或机械递补。'}
    var strategyRunBox=el('candidateStrategyRuns');if(strategyRunBox)strategyRunBox.innerHTML=strategyRows.length?strategyRows.map(function(row){var label=strategy(row.strategy_key),candidateCount=Number(row.candidate_count||0),runStatus=String(row.status||'UNKNOWN');return '<span title="'+esc(runStatus)+'"><b>'+esc(label)+'</b> · '+candidateCount+' 候选 · '+esc(runStatus)+'</span>'}).join(''):'<span><b>策略执行证据不可用</b> · 不能把缺失解释为全部策略零候选</span>';
    var poolStats=el('candidatePoolStats');if(poolStats)poolStats.innerHTML='<span><b>'+Number(summary.strategy_candidate_count||0)+'</b>研究候选</span><span><b>'+Number(summary.daily_new_count||0)+'</b>今日新入</span><span><b>'+Number(summary.daily_retained_count||0)+'</b>连续保留</span><span><b>'+Number(formalCurrent?summary.wait_trigger_count||0:0)+'</b>等待触发</span><span><b>'+Number(summary.target_count||0)+'</b>研究目标（不可直接下单）</span><span><b>'+Number(summary.rejected_count||0)+'</b>明确拒绝</span>';
    el('candidateSummary').textContent=(formalCurrent?(pool.is_as_of_fallback===true?'VERIFIED COMPLETED · 最新成功批次 · ':'VERIFIED COMPLETED · 当前正式票池 · '):governanceDeferred?'治理延期 · 只读研究 · ':historicalFallback?'历史只读 · ':researchDefaultFallback?'研究观察 · ':'')+'请求日 '+requestedDate+' · 决策日 '+decisionDate+' · 数据日 '+dataDate+' · 当前显示 '+rows.length+' 条'+(formalCurrent?'':' · 阻断 '+formalTruth.reasonCode);el('candidatePageStatus').textContent='第 '+state.candidatePage+' / '+pageCount+' 页';el('candidatePrev').disabled=state.candidatePage<=1;el('candidateNext').disabled=state.candidatePage>=pageCount;el('candidatePager').hidden=rows.length<=pageSize;
  }
  function renderIntraday(){
    var data=state.intraday||{},realtime=data.current_realtime_state||{},history=data.latest_historical_snapshot||{},market=realtime.snapshot||{},isLive=realtime.status==='LIVE',liveStale=realtime.status==='STALE',rows=isLive?(data.decisions||[]):[],evidence=market.evidence||[];
    var age=realtime.snapshot_age_seconds,isFallback=String(market.source_provider||'').toUpperCase()==='PUBLIC_QUOTE_QUORUM_V1';
    var snapshotState=isLive?(isFallback?'替补行情有效，模拟盘降仓':'主源实时快照'):liveStale?'结果已过期，禁止下单':realtime.status==='MARKET_CLOSED'?'已收盘，当前无实时状态':'尚未收到实时快照';
    var label=liveStale?'结果已过期':isLive&&data.status==='actionable'?'允许模拟竞争':isLive&&data.status==='blocked'?'数据门禁阻断':isLive?'观察中':'当前无实时状态';
    el('intradayStatus').textContent=label;el('intradayStatus').className='badge '+(isLive&&data.status==='actionable'?'safe':liveStale||data.status==='blocked'?'danger':'warning');
    el('intradayCount').textContent=String(rows.length)+' 条';
    el('intradayFacts').innerHTML=[
      fact('实时状态',snapshotState),fact('观察时间',isLive?(market.observed_at||'—'):'—'),fact('数据年龄',age==null?'—':ageText(age)),fact('市场状态',isLive?(market.state==='DATA_BLOCKED'?'数据未达标':market.state||'—'):'—'),fact('行情来源',isLive?sourceName(market.source_provider):'—'),fact('行情能力',isLive?(isFallback?'仅模拟盘，降仓50%，无Level-1':'QMT正常时优先Level-1'):'当前不可执行'),fact('有效覆盖率',isLive?pct(Number(market.coverage||0)*100,1):'—'),fact('有效股票',isLive?(market.observed_count||0)+' / '+(market.expected_count||0):'—'),fact('当前结论',realtime.reason||'等待实时数据')
    ].join('');
    var displayed=!isLive?[realtime.reason||'当前没有可执行的实时状态']:evidence;
    el('intradayEvidence').innerHTML=displayed.length?displayed.slice(0,8).map(function(x,i){return fact(i===displayed.length-1?'当前结论':'门禁 '+(i+1),x)}).join(''):[fact('当前结论','尚无盘中扫描结果；盘中任务由受控调度执行，本页只读')].join('');
    el('intradayHistoricalFacts').innerHTML=history&&history.observed_at?[
      fact('历史快照时间',history.observed_at),fact('身份','历史/收盘快照，仅供复盘'),fact('市场状态',history.state||'—'),fact('行情来源',sourceName(history.source_provider)),fact('有效覆盖率',pct(Number(history.coverage||0)*100,1)),fact('执行权限','禁止下单')
    ].join(''):fact('历史快照','尚无已落库历史快照');
    el('intradayRows').innerHTML=rows.length?rows.map(function(x){var p=Number(x.current_price),amount=Number(x.intraday_amount_ratio);return '<tr><td>'+security(x.stock_code,x.short_name)+'</td><td>'+esc(x.theme_name||x.theme_code||'未归属主题')+'</td><td>'+esc(x.role||'观察')+'</td><td>'+esc(reason(x.action))+'</td><td>'+(p>0?'¥'+num(p,2):'—')+'</td><td>'+pct(x.current_return_pct)+'</td><td>'+pct(x.relative_strength_pct)+'</td><td>'+(amount>0?ratio(amount):'—')+'</td><td>'+pct(x.theme_positive_breadth_pct,1)+'</td><td>'+num(x.raw_score,2)+'</td><td>'+ratio(x.risk_reward_ratio)+'</td><td class=\"reason\">'+esc(reason(x.reason_code))+'</td><td>'+esc(x.observed_at||'—')+'</td></tr>'}).join(''):emptyFor('intraday',13,'尚无盘中观察记录；盘中任务由受控调度执行，本页只读');
  }
  function renderPortfolio(){var rows=state.targets||[],truth=truthState();el('portfolioStatus').textContent=truth.code==='READY'&&rows.length?'RESEARCH_ONLY · 入队仍需四项执行复验':truth.code;el('portfolioRows').innerHTML=rows.length?rows.map(function(x){return '<tr><td>'+security(x.stock_code,x.short_name)+'</td><td>'+esc((x.strategy_keys||[]).map(strategy).join(' / '))+'</td><td>'+esc(themeText(x))+'</td><td>'+money(x.target_value)+'</td><td>'+pct(Number(x.target_weight)*100,1)+'</td><td>'+num(x.target_quantity,0)+'</td><td>'+pct(x.expected_mae_pct)+'</td><td class="reason">RESEARCH_ONLY · '+esc(x.reason||'—')+'</td></tr>'}).join(''):decisionEmptyFor('targets',8,'已验证批次没有研究目标；研究分数不作为买入依据')}
  function ledgerSourceName(value){return value==='LEGACY_EVENT_SIM'?'事件模拟账本':value==='V2_CANONICAL'?'统一模拟账本':'合并账本'}
  function closePositionDetail(){var modal=el('positionDetailModal');if(!modal)return;modal.hidden=true;document.body.classList.remove('position-detail-modal-open')}
  function showPositionDetail(index){var x=state.positions[Number(index)];if(!x)return;var modal=el('positionDetailModal'),body=el('positionDetailBody'),lots=x.lot_details||[],sources=(x.ledger_sources||[x.ledger_source]).map(ledgerSourceName).filter(function(v,i,a){return a.indexOf(v)===i}).join(' + '),notes=(x.holding_notes||[]).join('\n')||x.invalidation_condition||x.last_reason||'趋势、硬止损和净期望动态复核';el('positionDetailTitle').textContent=(x.short_name||x.stock_code||'证券')+' · '+status(x.position_state||x.state);body.innerHTML='<div class="position-detail-grid"><div><span>证券代码</span><strong>'+esc(x.stock_code||'—')+'</strong></div><div><span>账本来源</span><strong>'+esc(sources||'—')+'</strong></div><div><span>买入时间</span><strong>'+esc(displayDateTime(x.buy_at))+'</strong></div><div><span>卖出时间</span><strong>'+esc(displayDateTime(x.sell_at))+'</strong></div><div><span>当前数量</span><strong>'+esc(num(x.remaining_quantity||x.quantity,0))+'</strong></div><div><span>今日卖出数量</span><strong>'+esc(num(x.sold_quantity_today,0))+'</strong></div><div><span>加权成本</span><strong>'+esc(money(x.cost_price||x.average_cost))+'</strong></div><div><span>卖出价格</span><strong>'+esc(money(x.sell_price))+'</strong></div><div><span>今日实现盈亏</span><strong class="'+pnlClass(x.realized_pnl)+'">'+esc(money(x.realized_pnl))+'</strong></div></div><section class="position-detail-section"><h4>完整持仓说明</h4><p>'+esc(notes)+'</p></section><section class="position-detail-section"><h4>底层成交批次（'+esc(lots.length)+'）</h4><div class="position-lots">'+(lots.length?lots.map(function(lot,i){return '<article class="position-lot-card"><div><span>批次</span><strong>#'+(i+1)+' · '+esc(ledgerSourceName(lot.ledger_source))+'</strong></div><div><span>数量</span><strong>'+esc(num(lot.quantity,0))+'</strong></div><div><span>成本 / 卖价</span><strong>'+esc(money(lot.cost_price))+' / '+esc(money(lot.sell_price))+'</strong></div><div><span>买入 / 卖出时间</span><strong>'+esc(displayDateTime(lot.buy_at))+'<br>'+esc(displayDateTime(lot.sell_at))+'</strong></div><div class="position-lot-note"><span>批次说明</span><strong>'+esc(lot.note||'—')+'</strong></div></article>'}).join(''):'<p>当前账本没有返回更细的成交批次。</p>')+'</div></section>';modal.hidden=false;document.body.classList.add('position-detail-modal-open')}
  function renderPositions(){var names={},summary=(state.paperLedger||{}).summary||{},stockCount=Number(summary.position_count||0),lotCount=Number(summary.position_lot_count||stockCount),soldCount=Number(summary.today_sold_count||0);state.forecasts.forEach(function(x){names[x.stock_code]=x.short_name});state.targets.forEach(function(x){names[x.stock_code]=x.short_name});el('positionSummary').innerHTML=[fact('当前持仓',String(stockCount)+' 只'+(lotCount>stockCount?'（'+lotCount+' 笔成交批次）':'')),fact('今日已卖',soldCount+' 只（仅当天保留，隔日自动移出）'),fact('合计持仓市值',money(summary.current_market_value)),pnlFact('合计浮动盈亏',summary.total_unrealized_pnl),fact('数据说明','同一股票按代码合并展示；说明只显示两行，点击详情查看全部成交批次')].join('');el('positionRows').innerHTML=state.positions.length?state.positions.map(function(x,index){var isSold=(x.position_state||x.state)==='SOLD_TODAY',sources=x.ledger_sources||[x.ledger_source],source=sources.map(ledgerSourceName).filter(function(v,i,a){return a.indexOf(v)===i}).join(' + '),lots=Number(x.position_lot_count||1),rowPnlClass=pnlClass(x.unrealized_pnl),quote=x.current_price==null?'—':money(x.current_price),quoteAt=x.quote_at?String(x.quote_at)+' · '+sourceName(x.quote_source):'—',lotText=lots>1?'已合并 '+lots+' 笔成交批次。':'单笔成交批次。',note='['+source+'] '+lotText+' '+(x.invalidation_condition||x.last_reason||'趋势、硬止损和净期望动态复核'),quantity=isSold?'0（已卖 '+num(x.sold_quantity_today,0)+'）':num(x.remaining_quantity||x.quantity,0);return '<tr class="'+(isSold?'position-sold-today':'')+'"><td>'+security(x.stock_code,x.short_name||names[x.stock_code])+'</td><td>'+esc(status(x.position_state||x.state))+'</td><td>'+quantity+'</td><td>'+num(x.sellable_quantity,0)+'</td><td>'+money(x.cost_price||x.average_cost)+'</td><td>'+quote+'</td><td>'+money(x.market_value)+'</td><td class="'+rowPnlClass+'">'+money(x.unrealized_pnl)+'</td><td class="'+rowPnlClass+'">'+(x.unrealized_pnl_pct==null?'—':pct(x.unrealized_pnl_pct))+'</td><td>'+esc(displayDateTime(x.buy_at))+'</td><td>'+esc(displayDateTime(x.sell_at))+'</td><td>'+money(x.sell_price)+'</td><td>'+money(x.protective_stop)+'</td><td>'+num(x.add_count,0)+'</td><td>'+esc(quoteAt)+'</td><td class="position-note-cell"><div class="position-note-preview">'+esc(note)+'</div><button type="button" class="position-detail-button" data-position-detail-index="'+index+'">查看详情</button></td></tr>'}).join(''):emptyFor('positions',16,'当前统一模拟账本没有持仓；今日已卖出的证券也只保留到当天')}
  function renderOrders(){var names={};state.forecasts.forEach(function(x){names[x.stock_code]=x.short_name});var rows=Array.isArray((state.lineage||{}).orders)?state.lineage.orders:[];el('orderRows').innerHTML=rows.length?rows.map(function(x){return '<tr><td>'+security(x.stock_code,x.short_name||names[x.stock_code])+'</td><td>'+esc(x.side==='BUY'?'买入':'卖出')+'</td><td>'+num(x.quantity,0)+'</td><td>'+money(x.limit_price)+'</td><td>'+esc(status(x.status))+'</td><td>'+esc(status(x.waiting_reason))+'</td><td>'+esc((x.earliest_at||'—')+' 至 '+(x.expires_at||'—'))+'</td></tr>'}).join(''):decisionEmptyFor('lineage',7,'已验证决策批次没有模拟订单')}
  function renderValidation(){
    var v=state.validation||{},ready=state.readiness||{},models=ready.active_oos_models||[],level1=(state.dataEvidence||{}).level1||{},readinessError=state.errors.readiness,validationError=state.errors.validation,evidenceError=state.errors.dataEvidence;
    var activePass=models.length>0,matchingPass=activePass&&models.every(function(x){return x.validation_status==='PASS'}),level1Days=Number(level1.consecutive_trade_days||0),level1Pass=level1.status==='PASS'&&level1Days>=5;
    [['activeOosGate',activePass,readinessError?'UNAVAILABLE':activePass?models.length+' 个':'BLOCK'],['validationGate',matchingPass,readinessError?'UNAVAILABLE':matchingPass?'PASS':'BLOCK'],['level1Gate',level1Pass,evidenceError?'UNAVAILABLE':level1Days+' / 5 日']].forEach(function(item){var node=el(item[0]);node.textContent=item[2];node.className=item[1]?'safe-text':'danger-text'});
    var passed=[activePass,matchingPass,level1Pass].filter(Boolean).length;
    var acceptanceError=readinessError||evidenceError;el('criticalAcceptanceStatus').textContent=acceptanceError?'UNAVAILABLE':passed+' / 3 PASS';el('criticalAcceptanceStatus').className='badge '+(!acceptanceError&&passed===3?'safe':'danger');
    el('criticalAcceptanceFacts').innerHTML=[
      fact('当前有效模型',readinessError?'UNAVAILABLE · '+readinessError:models.map(function(x){return strategy(x.strategy_key)+' · '+x.model_version}).join('；')||'无'),
      fact('逐模型对应验证',readinessError?'UNAVAILABLE':models.map(function(x){return x.model_version+' → '+x.validation_status}).join('；')||'无有效模型，不能判定验证通过'),
      fact('Level1 连续交易日',evidenceError?'UNAVAILABLE · '+evidenceError:level1Days+' / 5；状态 '+(level1.status||'BLOCK')),
      fact('真实下单',ready.real_trading_enabled?'异常：已开启':'固定关闭')
    ].join('');
    el('oosSamples').textContent=validationError?'—':num(v.sample_count,0);el('oosExpectancy').textContent=validationError?'—':pct(v.net_expectancy_pct);el('oosPf').textContent=validationError?'—':ratio(v.profit_factor);el('oosPayoff').textContent=validationError?'—':ratio(v.payoff_ratio);el('oosDd').textContent=validationError?'—':pct(v.maximum_drawdown_pct);var evidence=v.evidence||{},p=evidence.portfolio||{};el('oosProfit').textContent=validationError?'—':money(p.net_profit_cny);el('validationStatus').textContent=validationError?'UNAVAILABLE':v.result_status||'暂无结果';el('validationStatus').className='badge '+(!validationError&&v.result_status==='PASS'?'safe':'danger');el('validationEvidence').innerHTML=validationError?fact('读取错误',validationError+'；不能解释为验收未通过或零样本'):[fact('模型版本',v.model_version||'—'),fact('样本外区间',(v.period_start||'—')+' 至 '+(v.period_end||'—')),fact('阻断原因',(v.block_reasons||[]).join('；')||'无'),fact('最终权益',money(p.final_equity_cny)),fact('组合收益',pct(p.total_return_pct)),fact('交易次数',p.trade_count==null?'—':p.trade_count)].join('')
  }
  function renderRecall(){var r=state.recall||{},error=state.errors.recall;if(error){el('recall20').textContent='—';el('recall50').textContent='—';el('winnerCount').textContent='—';el('missedCount').textContent='—';el('missedReasons').innerHTML=fact('读取错误','UNAVAILABLE · '+error+'；不能解释为零个赢家或零次漏抓');el('recallStatus').textContent='UNAVAILABLE';el('recallStatus').className='badge danger';return}el('recall20').textContent=r.recall_at_20==null?'—':pct(Number(r.recall_at_20)*100,1);el('recall50').textContent=r.recall_at_50==null?'—':pct(Number(r.recall_at_50)*100,1);el('winnerCount').textContent=r.winner_count==null?'—':r.winner_count;el('missedCount').textContent=r.missed_winner_count==null?'—':r.missed_winner_count;var reasons=r.missed_reason_counts||{};el('missedReasons').innerHTML=Object.keys(reasons).length?Object.keys(reasons).sort(function(a,b){return reasons[b]-reasons[a]}).map(function(k){return fact(status(k),reasons[k]+' 次')}).join(''):fact('当前状态','预测期限尚未成熟，影子复盘正在逐日形成');el('recallStatus').textContent=r.trade_date?'影子复盘更新至 '+r.trade_date:'影子复盘积累中';el('recallStatus').className='badge warning'}
  function renderLearning(){var x=state.learning||{},stages=x.stage_counts||{},error=state.errors.learning;if(error){el('learningObserved').textContent='—';el('learningAccepted').textContent='—';el('learningWinRate').textContent='—';el('learningPf').textContent='—';el('learningStatus').textContent='UNAVAILABLE';el('learningStatus').className='badge danger';el('learningFacts').innerHTML=fact('读取错误',error+'；不能解释为等待首笔闭环或零样本');return}el('learningObserved').textContent=x.observed_count==null?'—':x.observed_count;el('learningAccepted').textContent=x.accepted_count==null?'—':x.accepted_count;el('learningWinRate').textContent=x.win_rate==null?'—':pct(Number(x.win_rate)*100,1);el('learningPf').textContent=ratio(x.profit_factor);el('learningStatus').textContent=x.accepted_count?'已形成 '+x.accepted_count+' 笔真实模拟成交闭环':'等待首笔完整模拟成交闭环';el('learningStatus').className='badge '+(Number(x.profit_factor)>=1.3?'safe':'warning');el('learningFacts').innerHTML=[fact('学习证据','仅实际模拟成交与真实手续费'),fact('平均净收益',pct(x.average_net_return_pct)),fact('平均最大不利',pct(x.average_mae_pct)),fact('平均最大有利',pct(x.average_mfe_pct)),fact('影子误触发',x.false_positive_count==null?'—':String(x.false_positive_count)+' 次'),fact('影子漏掉强势',x.missed_opportunity_count==null?'—':String(x.missed_opportunity_count)+' 次'),fact('重新校准门槛',x.minimum_samples_before_calibration==null?'—':String(x.minimum_samples_before_calibration)+' 笔完整成交'),fact('最近结果日期',x.latest_outcome_date||'尚未成熟'),fact('当前结论',x.conclusion||'继续积累真实前向样本')].join('')+(Object.keys(stages).length?'<div><span>阶段分布</span><strong>'+esc(Object.keys(stages).map(function(k){return status(k)+' '+stages[k]}).join('；'))+'</strong></div>':'')}
  function renderEvidence(){var ready=state.readiness||{},run=state.overview.run||{},blocks=ready.blocks||[],warnings=ready.warnings||[],limits=ready.portfolio_limits||{},level1=(state.dataEvidence||{}).level1||{},executionReady=ready.paper_ready===true&&ready.paper_authority_ready===true&&ready.execution_ready===true,readinessError=state.errors.readiness,evidenceError=state.errors.dataEvidence;el('readinessStatus').textContent=readinessError?'UNAVAILABLE':executionReady?'统一执行复验就绪':'统一执行门禁存在硬阻断';el('readinessStatus').className='badge '+(!readinessError&&executionReady?'safe':'danger');el('blocks').innerHTML=(readinessError?fact('执行门禁','UNAVAILABLE · '+readinessError+'；不能解释为就绪或普通阻断'):evidenceError?fact('证据链','部分证据 UNAVAILABLE；不能宣称只读链路均已就绪'):blocks.length?blocks.map(function(x){return fact('硬阻断',status(x))}).join(''):warnings.length?warnings.map(function(x){return fact('正式策略提示',status(x)+'；左侧实验模拟链路仍可用')}).join(''):fact('模拟链路','数据库、校准和只读接口均已就绪'))+fact('Level1 连续采集',evidenceError?'UNAVAILABLE · '+evidenceError:(level1.consecutive_trade_days||0)+' / 5 交易日；'+(level1.status||'BLOCK'))+fact('生产仓位规则',readinessError?'UNAVAILABLE':'正式 '+(limits.maximum_positions||'—')+'；试错 '+(limits.maximum_paper_discovery_positions||'—')+'；实时总上限 '+(limits.maximum_live_positions||'—')+'；加仓 '+(limits.maximum_add_count==null?'—':limits.maximum_add_count));el('provenance').innerHTML=[fact('决策批次',run.run_uid||'—'),fact('模型版本',run.model_version||'—'),fact('数据哈希',run.data_snapshot_hash||'—'),fact('结果哈希',run.result_hash||'—'),fact('决策时间',run.decision_at||'—'),fact('真实交易','固定关闭')].join('');var schema=ready.schema||{};el('schema').innerHTML=readinessError?'<span class="bad">UNAVAILABLE</span>':Object.keys(schema).map(function(k){return '<span class="'+(schema[k]?'':'bad')+'">'+esc(k)+'</span>'}).join('')}
  function renderBatchDiff(){
    var result=state.batchDiff||{},summary=result.summary||{},error=(state.researchErrors||{}).batchDiff,node=el('batchDiffStatus');
    node.textContent=error?'UNAVAILABLE':result.status||'等待前一批次';node.className='badge '+(error?'danger':result.status==='CHANGED'||result.status==='NO_PREVIOUS_BATCH'?'warning':result.status?'safe':'');
    el('batchDiffFacts').innerHTML=error?fact('读取错误',error+'；不能把对比失败解释为批次无变化'):[fact('前一批次',result.previous_run_uid||'尚无可比批次'),fact('当前批次',result.current_run_uid||(state.context||{}).run_uid||'—'),fact('样本数',String(summary.previous_count||0)+' → '+String(summary.current_count||0)),fact('新增 / 移除',String(summary.added_count||0)+' / '+String(summary.removed_count||0)),fact('字段变化',String(summary.changed_count||0)),fact('权限','RESEARCH_ONLY · order_authority=false')].join('')
  }
  function renderAdvisoryResearch(){
    var snapshot=state.decisionIntelligence||{},replacement=snapshot.replacement_analysis||{},portfolio=snapshot.portfolio_optimization||{},input=snapshot.input_summary||{},run=snapshot.run||{},endpointError=(state.researchErrors||{}).decisionIntelligence,runtimeUnavailable=String(snapshot.status||'').toUpperCase()==='UNAVAILABLE',error=endpointError||(runtimeUnavailable?(snapshot.reason_codes||['DECISION_INTELLIGENCE_UNAVAILABLE']).join('；'):'');
    var verified=!error&&String(snapshot.status||'').toUpperCase()==='READY'&&!!run.run_uid&&!!run.snapshot_manifest_hash,badge=el('advisoryStatus'),displayStatus=error?'UNAVAILABLE':verified?'READY · AUDIT_ONLY':snapshot.status||'COLLECTING';badge.textContent=displayStatus;badge.className='badge '+(error?'danger':'warning');
    if(!verified){el('advisoryFacts').innerHTML=[fact('服务端快照',error?'UNAVAILABLE · '+error:'COLLECTING / UNVERIFIED'),fact('替代分析','UNAVAILABLE；前端不会合成资金、费用或持仓'),fact('组合优化','UNAVAILABLE；前端不会用默认 equity 或费率'),fact('执行边界','RESEARCH_ONLY；统一执行层必须重新校验'),fact('订单权限','order_authority=false')].join('');return}
    el('advisoryFacts').innerHTML=[fact('服务端快照',run.run_uid+' · manifest '+String(run.snapshot_manifest_hash).slice(0,12)),fact('会话日 / 数据日',(run.decision_session_date||'—')+' / '+(run.data_date||'—')),fact('输入摘要',String(input.candidate_count==null?'—':input.candidate_count)+' 候选；'+String(input.holding_count==null?'—':input.holding_count)+' 持仓；权益 '+money(input.equity_cny)),fact('替代分析',String(replacement.status||'UNAVAILABLE')+' · '+String(replacement.eligible_count==null?'—':replacement.eligible_count)+' 个可替代 / '+String((replacement.options||[]).length)+' 个比较'),fact('组合优化',String(portfolio.status||'UNAVAILABLE')+' · '+String((portfolio.targets||[]).length)+' 个只读目标 / '+String((portfolio.rejected||[]).length)+' 个拒绝'),fact('组合风险资产',portfolio.target_risk_asset_weight==null?'—':pct(Number(portfolio.target_risk_asset_weight)*100,1)),fact('服务端警告',(snapshot.warnings||[]).join('；')||'无'),fact('执行边界',snapshot.execution_revalidation_required===true?'RESEARCH_ONLY；统一执行层必须重新校验':'UNVERIFIED'),fact('订单权限','order_authority=false')].join('')
  }
  function renderHorizonResearch(){
    var governance=state.researchGovernance||{},suite=governance.multi_horizon_forecasts||{},validation=state.horizonValidation||{},contracts=Array.isArray(validation.contracts)?validation.contracts:[],outcomes=Array.isArray(validation.outcomes)?validation.outcomes:[],summary=validation.summary||{},registry=validation.artifact_registry||{},selections=validation.runtime_model_selection||{},suiteRuntime=validation.model_suite_runtime||{},runtimeUnavailable=String(validation.status||'').toUpperCase()==='UNAVAILABLE',endpointError=(state.researchErrors||{}).horizonValidation,governanceError=(state.errors||{}).researchGovernance,error=endpointError||(runtimeUnavailable?(validation.reason_codes||['HORIZON_RUNTIME_UNAVAILABLE']).join('；'):'')||(governanceError?'治理配置不可用：'+governanceError:''),registryStatus=String(registry.status||'UNAVAILABLE').toUpperCase(),registryUnavailable=registryStatus!=='AVAILABLE';
    function stateText(value){value=String(value||'COLLECTING').toUpperCase();return value==='REAL_OOS_MODEL'?'真实 OOS 模型':value==='REAL_OOS_MODEL_DEGRADED'?'真实 OOS 模型 · 输入降级':value==='MODEL_INPUT_EVIDENCE_BLOCKED'?'模型输入证据已阻断':value==='CROSS_SUITE_MODEL_EVIDENCE_BLOCKED'?'跨 Suite 证据已阻断':value==='PROXY_FALLBACK'?'代理回退':value==='HISTORICAL_AUDIT_ONLY'?'V1 · HISTORICAL_AUDIT_ONLY':value==='PRE_LEDGER_V2_AUDIT_ONLY'?'V2 · PRE_LEDGER_V2_AUDIT_ONLY':value==='REGISTRY_UNAVAILABLE'?'注册表不可用':value==='MIXED_MODEL_EVIDENCE_BLOCKED'?'混合证据已阻断':value==='COLLECTING'?'等待持久化契约':value}
    function shown(value){return value==null||value===''?'UNAVAILABLE':String(value)}
    function metric(value,digits){var number=Number(value);return value==null||!Number.isFinite(number)?'UNAVAILABLE':number.toFixed(digits==null?4:digits)}
    function runtime(label){return selections[label]||{status:'REGISTRY_UNAVAILABLE',reason_codes:['RUNTIME_MODEL_SELECTION_UNAVAILABLE'],order_authority:null}}
    Array.prototype.forEach.call(el('horizonContracts').children,function(node){var label=node.dataset.horizon,selection=runtime(label),selectionState=String(selection.status||'REGISTRY_UNAVAILABLE').toUpperCase(),kinds=Array.isArray(selection.prediction_kinds)?selection.prediction_kinds:[],kind=kinds.length===1?kinds[0]:'UNAVAILABLE',hashes=Array.isArray(selection.artifact_hashes)?selection.artifact_hashes:[],gate=shown(selection.artifact_gate_status),valid=shown(selection.artifact_valid_until),authority=selection.order_authority===false?'order=false':'order=UNAVAILABLE';node.dataset.kind=kind;node.dataset.runtimeState=selectionState;node.querySelector('strong').textContent=error?'UNAVAILABLE':stateText(selectionState)+' · '+shown(selection.model_key);node.querySelector('small').textContent=error?'LEDGER UNAVAILABLE':kind+' · gate='+gate+' · valid='+valid+' · '+authority+(hashes.length?' · '+hashes[0].slice(0,12)+'…':'')});
    var labels=['T+1','T+5','T+20'],selectionRows=labels.map(runtime),allReal=selectionRows.every(function(x){return x.status==='REAL_OOS_MODEL'&&((x.candidate_ledger||{}).registration_verified===true)})&&suiteRuntime.single_suite===true,anyUnavailable=registryUnavailable||selectionRows.some(function(x){return /UNAVAILABLE|BLOCKED|HISTORICAL|AUDIT_ONLY/.test(String(x.status||''))}),anyDegraded=selectionRows.some(function(x){return /DEGRADED|PROXY_FALLBACK/.test(String(x.status||''))}),collecting=!contracts.length,badge=el('horizonValidationStatus'),serverStatus=String(summary.status||validation.status||(collecting?'COLLECTING':'PERSISTED')).toUpperCase();badge.textContent=error?'UNAVAILABLE':anyUnavailable?'UNAVAILABLE / AUDIT_ONLY / 已阻断':allReal?'当前单 Suite V3 注册账本真实 OOS 模型':anyDegraded?'DEGRADED / 代理回退':serverStatus;badge.className='badge '+(error||anyUnavailable?'danger':allReal?'safe':'warning');
    function stateLine(label){var row=runtime(label),hashes=Array.isArray(row.artifact_hashes)?row.artifact_hashes:[],reasons=Array.isArray(row.reason_codes)?row.reason_codes:[],imputed=Array.isArray(row.imputed_feature_keys)?row.imputed_feature_keys:[],imp=row.imputation||{};return stateText(row.status)+'；suite='+shown(row.suite_release_id)+'；model='+shown(row.model_key)+'@'+shown(row.model_version)+'；artifact='+(hashes.join(',')||'UNAVAILABLE')+'；schema='+shown(row.artifact_schema)+'；evidence='+shown(row.artifact_evidence_status)+'；contract↔artifact='+shown(row.contract_artifact_binding_status)+'；valid='+shown(row.artifact_valid_until)+'；gate='+shown(row.artifact_gate_status)+'；插补证据='+shown(imp.evidence_status)+'；插补键='+(imputed.join(',')||'无')+'；插补率='+metric(imp.imputed_feature_ratio,4)+'；全量插补契约='+shown(imp.fully_imputed_contract_count)+(reasons.length?'；reason='+reasons.join(','):'')+'；order='+(row.order_authority===false?'false':'UNAVAILABLE')}
    function protocolScopeLine(label){var row=runtime(label),p=row.protocols||{},scope=row.candidate_economic_scope||{};return 'artifact='+shown(p.artifact)+'；suite='+shown(p.suite)+'；model='+shown(p.model)+'；selection='+shown(p.selection)+'；calibration='+shown(p.calibration)+'；candidate_scope='+shown(scope.candidate_scope)+'；economic_scope='+shown(scope.economic_evaluation_scope)+'；gate_scope='+shown(scope.gate_scope)+'；candidate='+shown(scope.candidate_sample_count)+'；eligible='+shown(scope.eligible_candidate_count)+'；deployment_gate='+(scope.deployment_gate===false?'false':'UNAVAILABLE')+'；deployment_domain_verified='+(scope.deployment_candidate_domain_verified===false?'false':'UNAVAILABLE')}
    function eligibilityLine(label){var boundary=runtime(label).eligibility_boundary||{},contractEligible=boundary.contract_eligible===true?'true':boundary.contract_eligible===false?'false':'UNAVAILABLE';return 'evidence='+shown(boundary.evidence_status)+'；contract_eligibility_scope='+shown(boundary.contract_eligibility_scope)+'；Shadow contract='+contractEligible+'；paper_eligible='+(boundary.paper_eligible===false?'false':'UNAVAILABLE')+'；production_eligible='+(boundary.production_eligible===false?'false':'UNAVAILABLE')+'；说明：Shadow contract 资格不等于 PAPER 或生产资格'}
    function candidateLedgerLine(label){var ledger=runtime(label).candidate_ledger||{},registered=ledger.registration_verified===true?'true':ledger.registration_verified===false?'false':'UNAVAILABLE';return 'evidence='+shown(ledger.evidence_status)+'；schema='+shown(ledger.schema_version)+'；content_sha256='+shown(ledger.content_sha256)+'；rows/sessions/folds='+shown(ledger.row_count)+'/'+shown(ledger.session_count)+'/'+shown(ledger.fold_count)+'；evaluation rows/sessions='+shown(ledger.evaluation_row_count)+'/'+shown(ledger.evaluation_session_count)+'；registration_verified='+registered+'；registration_verification_hash='+shown(ledger.registration_verification_hash)}
    function selectedLine(label){var row=runtime(label),x=row.selected_economics||{};return 'selected OOS samples='+shown(x.selected_oos_sample_count)+'；sessions='+shown(x.selected_oos_session_count)+'；净期望='+metric(x.net_expectancy_after_cost_pct,4)+'% ；PF='+metric(x.profit_factor,4)+'；cost coverage='+metric(x.cost_coverage_ratio,4)}
    function baselineLine(label){var x=(runtime(label).unconditional_baseline)||{};return 'unconditional 净期望='+metric(x.net_expectancy_after_cost_pct,4)+'% ；PF='+metric(x.profit_factor,4)+'；cost coverage='+metric(x.cost_coverage_ratio,4)}
    function calibrationLine(label){var row=runtime(label),ic=row.session_direction||{},cal=row.calibration_evidence||{},purged=cal.oos_only===true&&cal.prequential===true&&cal.labels_purged_by_maturity===true;return 'Session IC protocol='+shown(ic.protocol)+'；valid/total='+shown(ic.valid_session_count)+'/'+shown(ic.session_count)+'；expected IC='+metric(ic.expected_return_rank_ic,6)+'；probability IC='+metric(ic.probability_rank_ic,6)+'；gate IC='+metric(ic.gate_direction_rank_ic,6)+'；maturity-purged='+(purged?'true':'UNAVAILABLE')+'；evaluation samples/sessions='+shown(cal.evaluation_sample_count)+'/'+shown(cal.evaluation_session_count)}
    var facts=[fact('治理协议',governanceError?'UNAVAILABLE':suite.protocol_version||'UNAVAILABLE'),fact('Artifact 注册表',error?'UNAVAILABLE · '+error:registryStatus+'；current v3='+shown(registry.current_v3_artifact_count)+'；pre-ledger v2='+shown(registry.pre_ledger_v2_artifact_count)+'；historical v1='+shown(registry.historical_v1_artifact_count)+'；unavailable='+shown(registry.unavailable_artifact_count)+'；'+((registry.reason_codes||[]).join('；')||'无异常')),fact('T+1/T+5/T+20 Suite',shown(suiteRuntime.status)+'；protocol='+shown(suiteRuntime.protocol)+'；suite_release_ids='+((suiteRuntime.suite_release_ids||[]).join(',')||'UNAVAILABLE')+'；single_suite='+(suiteRuntime.single_suite===true?'true':'false')+'；order='+(suiteRuntime.order_authority===false?'false':'UNAVAILABLE'))];labels.forEach(function(label){facts.push(fact(label+' 运行态',stateLine(label)),fact(label+' Candidate ledger 注册证据',candidateLedgerLine(label)),fact(label+' Shadow 资格边界',eligibilityLine(label)),fact(label+' 协议与候选范围',protocolScopeLine(label)),fact(label+' 选中 OOS 经济性',selectedLine(label)),fact(label+' 无条件基线（单列）',baselineLine(label)),fact(label+' Session IC / maturity-purged 校准',calibrationLine(label)))});facts.push(fact('持久化契约',error?'UNAVAILABLE · '+error:String(summary.contract_count==null?contracts.length:summary.contract_count)+' 条；'+String(outcomes.length)+' 条 outcome'),fact('已验证 outcome',error?'UNAVAILABLE':String(summary.verified_outcome_count==null?0:summary.verified_outcome_count)),fact('证据来源',validation.evidence_source||'UNAVAILABLE'),fact('研究边界','CALIBRATED_OOS 仅展示服务端 V3 已注册 candidate ledger 证据；V2 固定 PRE_LEDGER_V2_AUDIT_ONLY；V1 固定 HISTORICAL_AUDIT_ONLY；Proxy 不宣称校准或经济性'),fact('入场 / 退出边界',governanceError?'UNAVAILABLE':suite.same_close_entry_allowed===false&&suite.t0_exit_allowed===false?'禁止同收盘入场与 T+0 退出':'UNAVAILABLE'),fact('订单权限',validation.order_authority===false?'RESEARCH_ONLY · order_authority=false':'UNAVAILABLE · 未获得服务端 false 证据'));el('horizonFacts').innerHTML=facts.join('')
  }
  function renderCounterfactualResearch(){
    var result=state.counterfactualResearch||{},run=latestPersisted(result,'learning_run','learning_runs'),metrics=run.metrics_json||result.metrics||{},overall=metrics.overall||metrics||{},counts=overall.quadrant_counts||{},perHorizon=result.per_horizon||run.per_horizon||metrics.per_horizon||{},runtimeUnavailable=/UNAVAILABLE|BLOCKED/.test(String(result.status||'').toUpperCase()),endpointError=(state.researchErrors||{}).counterfactualResearch,governanceError=(state.errors||{}).researchGovernance,error=endpointError||(runtimeUnavailable?(result.reason_codes||['LEARNING_RUNTIME_UNAVAILABLE']).join('；'):'')||(governanceError?'治理配置不可用：'+governanceError:''),flags=verificationFlags(result,run),hasRun=!!(run.learning_run_id||run.evaluated_at||run.evaluation_date),rawStatus=String(run.learning_status||result.learning_status||result.status||'COLLECTING').toUpperCase(),evidenceReady=hasRun&&flags.eligible&&/EVIDENCE_READY|READY|PASS/.test(rawStatus),badge=el('counterfactualStatus');
    var calibrationPolicy=(state.researchGovernance||{}).continuous_calibration||{},evaluated=run.evaluated_at?new Date(run.evaluated_at):null,maxAgeDays=Number(calibrationPolicy.maximum_evidence_age_days),ageExpired=!!(evaluated&&Number.isFinite(evaluated.getTime())&&Number.isFinite(maxAgeDays)&&Date.now()-evaluated.getTime()>maxAgeDays*86400000);if(ageExpired){flags.expired=true;flags.eligible=false;evidenceReady=false}
    counts={SELECTED_WIN:counts.SELECTED_WIN==null?run.selected_win_count:counts.SELECTED_WIN,SELECTED_LOSS:counts.SELECTED_LOSS==null?run.selected_loss_count:counts.SELECTED_LOSS,REJECTED_WIN:counts.REJECTED_WIN==null?run.rejected_win_count:counts.REJECTED_WIN,REJECTED_CORRECT:counts.REJECTED_CORRECT==null?run.rejected_correct_count:counts.REJECTED_CORRECT};
    el('quadrantSelectedWin').textContent=error||!hasRun?'—':String(counts.SELECTED_WIN||0);el('quadrantSelectedLoss').textContent=error||!hasRun?'—':String(counts.SELECTED_LOSS||0);el('quadrantRejectedWin').textContent=error||!hasRun?'—':String(counts.REJECTED_WIN||0);el('quadrantRejectedCorrect').textContent=error||!hasRun?'—':String(counts.REJECTED_CORRECT||0);
    var displayStatus=error?'UNAVAILABLE':!hasRun?'COLLECTING':flags.policyMismatch?'POLICY_MISMATCH':flags.expired?'EVIDENCE_EXPIRED':!flags.verified?'UNVERIFIED':evidenceReady?'EVIDENCE_READY · AUDIT_ONLY':rawStatus;badge.textContent=displayStatus;badge.className='badge '+(error||flags.policyMismatch||flags.expired||flags.preview?'danger':'warning');
    function horizonLine(days){var key='T+'+days,row=perHorizon[key]||perHorizon[String(days)]||{};var samples=row.sample_count;if(samples==null)samples=run['t'+days+'_sample_count'];var ready=row.evidence_ready;if(ready==null)ready=row.ready;if(ready==null)ready=run['t'+days+'_evidence_ready'];return key+' '+String(samples==null?'—':samples)+' 样本 / '+(ready===true?'EVIDENCE_READY':'COLLECTING')}
    el('counterfactualFacts').innerHTML=[fact('学习批次',hasRun?run.learning_run_id||run.evaluated_at||run.evaluation_date:'尚无持久化 learning run'),fact('成熟样本',hasRun?String(run.sample_count==null?overall.sample_count==null?'—':overall.sample_count:run.sample_count):'—'),fact('T+1 / T+5 / T+20',horizonLine(1)+'；'+horizonLine(5)+'；'+horizonLine(20)),fact('选中精度',run.selection_precision==null?overall.selection_precision==null?'—':pct(Number(overall.selection_precision)*100,1):pct(Number(run.selection_precision)*100,1)),fact('赢家召回',run.winner_recall==null?overall.winner_recall==null?'—':pct(Number(overall.winner_recall)*100,1):pct(Number(run.winner_recall)*100,1)),fact('证据校验',error?'UNAVAILABLE · '+error:flags.policyMismatch?'policy_hash 不匹配，禁止使用':flags.expired?'证据已过期，禁止使用':flags.preview?'UNVERIFIED_PREVIEW，禁止使用':flags.verified?'哈希证据已验证；当前 policy_hash 仍为审计展示，不自动激活':'未验证，不能升级为 EVIDENCE_READY'),fact('证据来源',result.evidence_source||'—'),fact('订单权限','RESEARCH_ONLY · order_authority=false')].join('')
  }
  function renderShadowGovernance(){
    var governance=state.researchGovernance||{},releaseConfig=governance.shadow_release||{},calibrationPolicy=governance.continuous_calibration||{},result=state.shadowPreview||{},persistedRelease=latestPersisted(result,'release','releases'),gate=(persistedRelease||{}).latest_gate||latestPersisted(result,'gate','gates'),runtimeUnavailable=String(result.status||'').toUpperCase()==='UNAVAILABLE',endpointError=(state.researchErrors||{}).shadowPreview,governanceError=(state.errors||{}).researchGovernance,error=endpointError||(runtimeUnavailable?(result.reason_codes||['SHADOW_RELEASE_RUNTIME_UNAVAILABLE']).join('；'):'')||(governanceError?'治理配置不可用：'+governanceError:''),configuredStage=releaseConfig.initial_stage||'未配置',hasRelease=!!(persistedRelease.release_id||persistedRelease.audit_id),hasGate=!!(gate.gate_id||gate.gate_evaluation_id),auditStage=String(persistedRelease.audit_stage||persistedRelease.current_stage||persistedRelease.stage||'COLLECTING').toUpperCase(),effectiveStage=String(persistedRelease.effective_stage||(hasRelease?'UNAVAILABLE':'COLLECTING')).toUpperCase(),rawGate=String(gate.status||gate.gate_status||'COLLECTING').toUpperCase();
    var currentChecks=gate.policy_current===true&&gate.config_current===true&&gate.fresh===true&&gate.learning_run_current===true,gatePass=hasGate&&gate.effective_pass===true&&currentChecks,gateStatus;
    if(error)gateStatus='UNAVAILABLE';else if(!hasGate)gateStatus='COLLECTING';else if(gate.policy_current===false)gateStatus='GATE_POLICY_STALE';else if(gate.config_current===false)gateStatus='RELEASE_CONFIG_STALE';else if(gate.fresh===false)gateStatus='GATE_EVIDENCE_STALE';else if(gate.learning_run_current===false)gateStatus='GATE_LEARNING_RUN_STALE';else if(!currentChecks)gateStatus='CURRENT_GATE_UNVERIFIED';else if(gatePass)gateStatus='PASS · PERSISTED';else gateStatus=rawGate==='PASS'?'BLOCKED_BY_CURRENT_GOVERNANCE':rawGate;
    var stageStatus=error?'UNAVAILABLE':hasRelease?effectiveStage:'COLLECTING',stageBlocked=/BLOCKED|UNAVAILABLE/.test(stageStatus),failureCodes=(gate.failure_codes||[]).slice();if(hasGate&&!gatePass&&!failureCodes.length)failureCodes.push(gateStatus);
    el('shadowStage').textContent=stageStatus;el('shadowStage').className='badge '+(error||stageBlocked?'danger':'warning');el('shadowReleaseMode').textContent=governance.release_mode||'SHADOW_RESEARCH_ONLY';el('shadowGateStatus').textContent=gateStatus;el('shadowOrderAuthority').textContent='false';
    var releaseBoundary=auditStage==='PAPER_ELIGIBLE'&&effectiveStage==='BLOCKED'?'历史审计阶段 PAPER_ELIGIBLE 已被当前 Gate 降级为 BLOCKED；RESEARCH_ONLY · order_authority=false · real_order=false':(effectiveStage==='PAPER_ELIGIBLE'?'服务端有效阶段为 PAPER_ELIGIBLE，但仍需外部授权；':'')+'RESEARCH_ONLY · order_authority=false · real_order=false';
    el('shadowFacts').innerHTML=[fact('服务端有效阶段',error?'UNAVAILABLE · '+error:hasRelease?(persistedRelease.release_id||persistedRelease.audit_id)+' · '+effectiveStage:'COLLECTING（没有持久化 release audit）'),fact('历史审计阶段',hasRelease?auditStage:'COLLECTING'),fact('配置初始阶段',configuredStage+'（不是当前发布状态）'),fact('Gate 来源',hasGate?((gate.gate_evaluation_id||gate.gate_id)+' · '+(gate.evidence_provenance_status||gate.verification_status||'未标记')):'COLLECTING（没有持久化 gate）'),fact('服务端 Gate 记录',hasGate?rawGate:'COLLECTING'),fact('当前可采信 Gate',gateStatus),fact('Gate 当前性','policy='+String(gate.policy_current===true)+'；config='+String(gate.config_current===true)+'；fresh='+String(gate.fresh===true)+'；learning_run='+String(gate.learning_run_current===true)+'；effective_pass='+String(gate.effective_pass===true)),fact('Gate 失败项',failureCodes.join('；')||'无'),fact('自动晋级',result.automatic_promotion_allowed===true||releaseConfig.automatic_promotion_allowed===true||persistedRelease.automatic_promotion_allowed===true?'配置异常：开启':'关闭'),fact('外部执行授权',result.external_execution_grant_required===true?'仍然必需':'未获授权'),fact('持续阈值',governanceError?'UNAVAILABLE':calibrationPolicy.protocol_version||'尚未配置'),fact('发布边界',releaseBoundary)].join('')
  }
  function renderLineage(){
    var lineage=state.lineage||{},summary=lineage.summary||{},run=lineage.run||{},error=state.errors.lineage,lotEvidence=lineage.lot_close_evidence||{},lotEvidenceStatus=String(summary.lot_close_evidence_status||lotEvidence.status||'NO_SELL_FILL').toUpperCase(),incomplete=lotEvidenceStatus==='INCOMPLETE';
    var hasLineage=!!run.run_uid;
    el('exitLineageStatus').textContent=error?'血缘不可用':incomplete?'退出 Lot 分配证据不完整':hasLineage?'同批次已核对':'尚无成交血缘';el('exitLineageStatus').className='badge '+(error||incomplete?'danger':hasLineage?'safe':'warning');
    el('exitLineageFacts').innerHTML=error?fact('读取错误',error+'；不能据此判断没有退出链'):[fact('决策批次',run.run_uid||((state.context||{}).run_uid)||'—'),fact('目标 → Intent',String(summary.target_count||0)+' → '+String(summary.intent_count||0)),fact('订单 → 成交',String(summary.order_count||0)+' → '+String(summary.fill_count||0)),fact('开放 Lot',String(summary.open_lot_count||0)),fact('退出 Intent',String(summary.exit_intent_count||0)),fact('Lot 关闭证据',lotEvidenceStatus+(incomplete?'；缺失 fill：'+(lotEvidence.incomplete_sell_fill_ids||[]).join(', '):''))].join('');
    var counts=[summary.fill_count,summary.open_lot_count,summary.exit_intent_count,summary.exit_intent_count,summary.fill_count,summary.open_lot_count];Array.prototype.forEach.call(el('exitWorkflow').children,function(node,index){var value=Number(counts[index]||0);node.dataset.count=String(value);node.classList.toggle('has-record',value>0)});
    var lineageCounts=[summary.target_count,summary.target_count,summary.intent_count,summary.approved_intent_count,summary.fill_count,summary.open_lot_count];Array.prototype.forEach.call(el('lineageWorkflow').children,function(node,index){var value=Number(lineageCounts[index]||0);node.dataset.count=String(value);node.classList.toggle('has-record',value>0)});
  }
  function applyFilters(filters){
    filters=filters||{};state.filters=Object.assign({},state.filters,filters);
    var mapping={candidateDate:'trade_date',strategyFilter:'strategy',statusFilter:'status',candidateSearch:'q',hypothesisDate:'trade_date',hypothesisState:'hypothesis_state',hypothesisSearch:'hypothesis_q'};
    Object.keys(mapping).forEach(function(id){var node=el(id),value=filters[mapping[id]];if(node&&value!=null)node.value=String(value)});
  }
  function syncFilters(){
    var view=state.activeView,filters={trade_date:state.requestedDate||''};
    if(view==='candidates'){filters.trade_date=el('candidateDate').value||filters.trade_date;filters.strategy=el('strategyFilter').value;filters.status=el('statusFilter').value;filters.q=el('candidateSearch').value.trim()}
    if(view==='hypotheses'){filters.trade_date=el('hypothesisDate').value||filters.trade_date;filters.hypothesis_state=el('hypothesisState').value;filters.hypothesis_q=el('hypothesisSearch').value.trim()}
    state.filters=filters;parentMessage('probiga-trading-v3-filter',{view:view,filters:filters});
  }
  document.addEventListener('click',function(event){var detailButton=event.target.closest&&event.target.closest('[data-position-detail-index]');if(detailButton){showPositionDetail(detailButton.dataset.positionDetailIndex);return}if(event.target.closest&&event.target.closest('[data-close-position-detail]')){closePositionDetail();return}var link=event.target.closest&&event.target.closest('.security a[data-stock-code]');if(!link)return;event.preventDefault();requestStockChart(link.dataset.stockCode,link.dataset.stockName)});
  document.addEventListener('keydown',function(event){if(event.key==='Escape')closePositionDetail()});
  document.querySelectorAll('.nav').forEach(function(btn){btn.addEventListener('click',function(){activateView(btn.dataset.view,{userInitiated:true});notifyParentResize()})});
  window.addEventListener('message',function(event){
    if(event.source!==window.parent||!event.data||event.data.type!=='probiga-trading-v3-view')return;
    var expectedOrigin='*';try{expectedOrigin=new URL(document.referrer).origin}catch(ignore){}if(expectedOrigin!=='*'&&expectedOrigin!=='null'&&event.origin!==expectedOrigin)return;
    var previousView=state.activeView,incomingView=String(event.data.view||'overview'),incomingDate=String(event.data.requested_date||''),dateChanged=!!incomingDate&&incomingDate!==state.requestedDate,viewChanged=incomingView!==previousView;
    state.requestedDate=incomingDate||state.requestedDate;applyFilters(event.data.filters||{});activateView(incomingView);
    if(dateChanged||viewChanged)load();else{if(incomingView==='hypotheses')renderHypotheses();if(incomingView==='candidates')renderCandidates();notifyParentResize()}
  });
  if(window.ResizeObserver)new ResizeObserver(notifyParentResize).observe(document.documentElement);
  var candidateTimer=null;
  function reloadCandidates(){state.candidatePage=1;var day=el('candidateDate').value;if(day)state.requestedDate=day;syncFilters();load()}
  function refreshWatchlistStrategy(){
    if(document.hidden||['overview','positions'].indexOf(state.activeView)<0)return;
    if(state.requestedDate&&state.requestedDate!==localDateKey())return;
    holdingStrategyPayload(fetchJson('/api/portfolio/holding-strategy')).then(function(payload){delete state.errors.watchlistStrategy;state.watchlistStrategy=payload||{};renderWatchlistStrategy();renderChrome();notifyParentResize()}).catch(function(err){state.errors.watchlistStrategy=errorText(err);renderWatchlistStrategy();notifyParentResize()})
  }
  function pollActionJob(jobId,button,remaining,delayMs){
    setTimeout(function(){
      api3('/actions/jobs/'+encodeURIComponent(jobId)).then(function(payload){
        var job=unwrap(payload)||{},jobState=String(job.state||job.status||'unknown').toLowerCase(),pending=job.terminal===false||jobState==='running'||jobState==='queued';
        if(pending&&remaining>0){state.actionMessage='日级选股 '+jobState+' · job_id '+jobId.slice(0,12)+'…';renderActions();pollActionJob(jobId,button,remaining-1,delayMs);return}
        if(pending&&remaining<=0){state.actionMessage='前台快速轮询已超时，任务仍在后台运行 · job_id '+jobId+'。按钮保持禁用，已切换低频追踪。';renderActions();pollActionJob(jobId,button,30,10000);return}
        state.activeJobId='';button.disabled=false;
        if(jobState==='succeeded'){state.actionMessage='日级选股执行成功 · job_id '+jobId.slice(0,12)+'…，正在读取新批次。';renderActions();load();return}
        state.actionMessage='日级选股'+(jobState==='cancelled'?'已取消':'执行失败')+' · job_id '+jobId.slice(0,12)+'…'+(job.output?' · '+String(job.output).slice(0,180):'');renderActions();
      }).catch(function(err){state.actionMessage='精确任务状态暂时不可用 · job_id '+jobId+'：'+errorText(err)+'；按钮保持禁用并继续低频追踪。';renderActions();pollActionJob(jobId,button,30,10000)})
    },delayMs||2000);
  }
  function runDailyAction(button){
    var gate=dailyActionGate();
    if(!gate.allowed){state.actionMessage=gate.reason;renderActions();return}
    var requested=(state.context||{}).requested_date||state.requestedDate||localDateKey();
    if(!window.confirm('确认按系统当前时间重跑日级选股？\n\n当前页面请求日期：'+requested+'；调度会自行选择最新可用交易日，不接收历史日期。\n范围：全量研究排序与组合决策；不会开启真实交易。\n运行期间请勿重复提交。'))return;
    button.disabled=true;state.actionMessage='日级选股已确认，正在启动…';renderActions();
    postJson('/api/v3/actions/daily').then(function(payload){
      var result=unwrap(payload)||{};
      if(result.status==='disabled'){throw new Error('对应任务未启用')}
      var jobId=String(result.job_id||result.run_id||result.run_uid||'');
      if(!jobId){button.disabled=false;state.actionMessage=result.status==='already_running'?'任务已经在执行，但本次没有返回可追踪 job_id；请到系统证据核对，勿重复提交。':'任务响应缺少 job_id，无法可靠跟踪；请到系统证据核对。';renderActions();return}
      state.activeJobId=jobId;state.actionMessage='日级选股已提交 · job_id '+jobId.slice(0,12)+'…';renderActions();pollActionJob(jobId,button,90,2000);
    }).catch(function(err){button.disabled=false;state.actionMessage='执行失败：'+err.message;renderActions()})
  }
  ['strategyFilter','statusFilter'].forEach(function(id){el(id).addEventListener('change',function(){state.candidatePage=1;syncFilters();renderCandidates();notifyParentResize()})});
  el('candidateDate').addEventListener('change',reloadCandidates);
  el('candidateSearch').addEventListener('input',function(){clearTimeout(candidateTimer);candidateTimer=setTimeout(function(){state.candidatePage=1;syncFilters();renderCandidates();notifyParentResize()},250)});
  el('candidatePrev').addEventListener('click',function(){if(state.candidatePage>1){state.candidatePage-=1;renderCandidates();notifyParentResize()}});
  el('candidateNext').addEventListener('click',function(){state.candidatePage+=1;renderCandidates();notifyParentResize()});
  var hypothesisTimer=null;
  ['hypothesisState','hypothesisDate'].forEach(function(id){el(id).addEventListener('change',reloadHypotheses)});
  el('hypothesisSearch').addEventListener('input',function(){clearTimeout(hypothesisTimer);hypothesisTimer=setTimeout(reloadHypotheses,250)});
  el('hypothesisRows').addEventListener('click',function(event){var button=event.target.closest('.evidence-button');if(button)showHypothesis(button.dataset.hypothesisId)});
  el('runDaily').addEventListener('click',function(){runDailyAction(this)});
  el('refresh').addEventListener('click',load);setInterval(refreshWatchlistStrategy,60000);state.requestedDate=requestedFrameDate()||state.requestedDate;activateView(requestedView());load();
})();
