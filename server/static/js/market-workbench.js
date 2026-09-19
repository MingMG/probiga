/* Market workbench: presentation of existing observations, never an order producer. */
(function (root) {
    'use strict';
    function number(value) {
        if (value == null || (typeof value !== 'number' && typeof value !== 'string') || (typeof value === 'string' && !value.trim())) return null;
        var n = Number(value);
        return Number.isFinite(n) ? n : null;
    }
    function escape(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) { return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
    function day(value) { var s = String(value || '').slice(0, 10); return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : ''; }
    function list(value) { return Array.isArray(value) ? value : []; }
    function fmt(value, digits, suffix) { var n = number(value); return n === null ? '—' : n.toFixed(digits == null ? 1 : digits) + (suffix || ''); }
    function pct(value) { var n = number(value); return n === null ? '—' : (n > 0 ? '+' : '') + n.toFixed(2) + '%'; }
    function tone(value) { var n = number(value); return n === null || n === 0 ? '' : n > 0 ? 'mw-up' : 'mw-down'; }
    function source(state, name) { var s = state[name] || {}; return s.status === 'ready' ? s.value || {} : {}; }
    function rowsOf(value) { return Array.isArray(value) ? value : list(value && value.data); }
    function empty(title, detail) { return '<div class="mw-empty"><strong>' + escape(title) + '</strong><p>' + escape(detail) + '</p></div>'; }
    function link(tab, label) { return '<button type="button" class="mw-link" data-mw-tab="' + escape(tab) + '">' + escape(label) + '<span aria-hidden="true"> ↗</span></button>'; }
    function panel(title, eyebrow, content, action, classes) { return '<section class="mw-panel ' + (classes || '') + '"><header><div><span class="mw-eyebrow">' + eyebrow + '</span><h3>' + title + '</h3></div>' + (action || '') + '</header>' + content + '</section>'; }
    function statusText(state, key, noData) {
        var s = state[key] || {};
        return s.status === 'error' ? '读取失败，可单独重试' : s.status === 'loading' ? '正在读取' : noData || '没有可用记录';
    }
    function model(state, requestedDate, clock) {
        var monitor = source(state, 'market'), trend = source(state, 'trend');
        if (!day(monitor.trade_date) || day(monitor.trade_date) > requestedDate) monitor = {};
        var context = source(state, 'context'), pool = source(state, 'pool');
        var total = number(monitor.total_count), up = number(monitor.up_count), down = number(monitor.down_count);
        var marketDate = day(monitor.trade_date);
        var validBreadth = !!marketDate && marketDate <= requestedDate && total !== null && total > 0 && up !== null && down !== null && up >= 0 && down >= 0 && up + down <= total;
        var breadth = validBreadth ? up / total * 100 : null;
        var exactMarket = validBreadth && marketDate === requestedDate;
        var contextDate = day(context.decision_session_date || context.requested_date);
        var contextState = String(context.decision_status || '').toUpperCase();
        var truth = typeof state.contextTruth === 'function' ? state.contextTruth(context) : contextState;
        var poolItems = list(pool.items), candidateCount = poolItems.filter(function (r) { return r.is_strategy_candidate === true; }).length;
        var poolReadable = !!pool.run_uid && pool.pool_readable === true && pool.run_status === 'COMPLETED' && pool.decision_integrity_verified === true && pool.is_historical_fallback !== true && ['READY','EMPTY'].indexOf(pool.pool_status) >= 0 && day(pool.decision_session_date) === requestedDate && !!day(pool.trade_date || pool.data_date) && day(pool.trade_date || pool.data_date) <= requestedDate && (pool.summary || {}).stock_count === poolItems.length && (pool.summary || {}).strategy_candidate_count === candidateCount && (pool.pool_status === 'READY' ? candidateCount > 0 : candidateCount === 0);
        var currentDate = day(clock && (clock.active_trade_date || clock.ui_trade_date));
        var historical = context.historical_read_only === true || (!!currentDate && requestedDate < currentDate);
        var historicalReadable = historical && context.historical_read_only === true && context.run_status === 'COMPLETED' && context.data_status === 'READY' && context.decision_integrity_verified === true && ['READY','EMPTY','CANDIDATE_AVAILABLE'].indexOf(contextState) >= 0;
        var featureDatesMatch = !!day(context.data_date) && day(context.data_date) === day(pool.trade_date || pool.data_date);
        var executionDatesMatch = !context.execution_session_date || day(context.execution_session_date) === day(pool.execution_session_date);
        var coherent = poolReadable && contextDate === requestedDate && featureDatesMatch && executionDatesMatch && context.run_uid === pool.run_uid && (['READY','EMPTY'].indexOf(truth) >= 0 || historicalReadable);
        var hypotheses = rowsOf(source(state, 'hypotheses')).filter(function (row) {
            return row.scope_type === 'MARKET' && row.run_uid === context.run_uid && day(row.trade_date || row.decision_session_date) === requestedDate;
        });
        var hypothesis = coherent && hypotheses.length ? hypotheses[0] : null;
        var sectors = list(monitor.top_industries).filter(function (r) { return day(r.trade_date) === marketDate && number(r.change) !== null; });
        var history = monitor.history || {}, historyDates = list(history.trade_dates);
        var points = historyDates.map(function (dateValue, i) {
            return {date: day(dateValue), breadth: number(list(history.heat)[i]), amount: number(list(history.amount)[i])};
        }).filter(function (p) { return p.date && p.date <= requestedDate; }).map(function (p) {
            return {date:p.date, breadth:p.breadth === null ? null : p.breadth / 10, amount:p.amount};
        });
        var headline = breadth === null ? '市场证据暂未齐全' : breadth >= 60 ? '多数个股上涨，观察强势能否延续' : down / total >= 0.6 ? '多数个股承压，先看风险与承接' : (total-up-down)/total >= 0.5 ? '多数个股持平，等待方向与成交确认' : '涨跌分化，聚焦有证据的方向';
        if (validBreadth && !exactMarket) headline = '当前显示历史行情，不能代表所选交易日';
        var indices = list(trend.indices).filter(function (r) { return day(r.data_cutoff) && day(r.data_cutoff) <= requestedDate; });
        return {monitor:monitor, trend:trend, context:context, pool:pool, truth:truth, breadth:breadth, total:validBreadth ? total : null, up:validBreadth ? up : null, down:validBreadth ? down : null, flat:validBreadth ? total-up-down : null, marketDate:marketDate, exactMarket:exactMarket, headline:headline, sectors:sectors, points:points, indices:indices, poolReadable:poolReadable, coherent:coherent, candidateCount:coherent ? candidateCount : null, candidates:coherent ? poolItems.filter(function (r) { return r.is_strategy_candidate === true; }).slice(0, 5) : [], hypothesis:hypothesis, historical:historical, requestedDate:requestedDate};
    }
    function freshness(m) {
        if (!m.marketDate) return '行情待核验';
        if (!m.exactMarket) return '历史回退 · ' + m.marketDate;
        if (m.monitor.is_realtime === true) return '盘中快照 · ' + String(m.monitor.data_time || '').slice(11,19);
        return ({paused:'午间快照',close:'收盘数据',fallback:'历史数据',stale:'行情已过期'}[m.monitor.freshness_status] || '已存行情') + ' · ' + m.marketDate;
    }
    function kpi(label, value, note, cls) { return '<article class="mw-kpi"><span>' + label + '</span><strong class="' + (cls || '') + '">' + value + '</strong><small>' + escape(note) + '</small></article>'; }
    function renderHero(m) {
        var cap = m.coherent && number((m.hypothesis || {}).max_position_weight);
        var action = m.historical ? '历史复盘' : !m.coherent ? '策略判断待核验' : m.candidateCount === 0 ? '本批次无新增候选' : '逐只核对入选依据';
        var detail = m.historical ? '行情与策略按所选日期查看；自选仍为当前快照。' : !m.coherent ? '先看市场事实；策略证据未齐全不能解释为没有机会。' : m.candidateCount === 0 ? '继续跟踪市场与已有持仓，无候选不等于必须清仓。' : '研究入选不等于买点；进入个股详情核对确认条件与风险。';
        return '<section class="mw-hero"><div><div class="mw-eyebrow">MARKET WORKBENCH <span>' + escape(m.requestedDate) + ' / A股</span></div><h2>' + escape(m.headline) + '</h2><p>' + (m.breadth === null ? '广度、趋势、板块各自显示证据状态，不用默认分数替代缺失数据。' : '上涨 ' + m.up + ' 家，下跌 ' + m.down + ' 家；这是市场广度，不是情绪预测或上涨概率。') + '</p><div class="mw-status"><i></i>' + escape(freshness(m)) + '</div></div><aside><span>现在优先做什么</span><strong>' + action + '</strong><p>' + detail + '</p>' + link('trading-v3-candidates', '查看候选依据') + '</aside></section>' +
            '<div class="mw-kpis">' + kpi('上涨家数占比', fmt(m.breadth,1,'%'), m.total === null ? '分母尚未核验' : '有报价样本 ' + m.total + ' 家', tone(m.breadth === null ? null : m.breadth-50)) + kpi(m.monitor.is_realtime ? '盘中累计成交' : '样本成交额', number(m.monitor.total_amount) === null ? '—' : fmt(m.monitor.total_amount / 1e8,0) + '<em> 亿</em>', '仅累计成交；不与昨日全天直接比较') + kpi('涨跌与平盘', m.total === null ? '—' : '<span class="mw-up">' + m.up + '</span><em> / </em><span class="mw-down">' + m.down + '</span><em> / ' + m.flat + '</em>', '红涨绿跌 · 停牌不应混入有效报价样本') + kpi('已验证研究候选', fmt(m.candidateCount,0) + (m.candidateCount === null ? '' : '<em> 只</em>'), cap !== null && cap !== false && cap >= 0 && cap <= 1 ? '研究风险上限 ' + fmt(cap*100,1,'%') : '仅研究观察；执行需另行核验') + '</div>';
    }
    function chart(points, field, maximum, label) {
        var values = points.map(function (p) { return p[field]; }).filter(function (v) { return v !== null; });
        if (values.length < 2) return empty('不足两个有效观察点', '不连补零数据，也不绘制模拟走势。');
        var max = maximum || Math.max.apply(null, values) * 1.1 || 1;
        var x = function (i) { return 42 + i * 638 / Math.max(1, points.length-1); };
        var y = function (v) { return 170 - Math.min(max, Math.max(0,v)) / max * 142; };
        var svg = '<svg viewBox="0 0 704 206" role="img" aria-label="' + escape(label) + '，' + escape(points[0].date) + '至' + escape(points[points.length-1].date) + '">';
        [0,0.5,1].forEach(function (ratio) { var yy=y(ratio*max); svg += '<line x1="42" x2="680" y1="'+yy+'" y2="'+yy+'" class="mw-gridline"/><text x="32" y="'+(yy+4)+'" text-anchor="end">'+fmt(ratio*max,0)+'</text>'; });
        if (field === 'breadth') {
            var segment = '';
            points.forEach(function (p,i) { if (p[field] === null) { if(segment) svg += '<path d="'+segment+'" class="mw-chart-line"/>'; segment=''; } else segment += (segment?' L':'M')+x(i)+' '+y(p[field]); });
            if(segment) svg += '<path d="'+segment+'" class="mw-chart-line"/>';
            points.forEach(function (p,i) { if(p[field] !== null) svg += '<circle cx="'+x(i)+'" cy="'+y(p[field])+'" r="3" class="mw-chart-dot"><title>'+escape(p.date)+' · '+fmt(p[field],1,'%')+'</title></circle>'; });
        } else {
            var width = Math.min(22, 450 / points.length);
            points.forEach(function (p,i) { if(p[field] !== null) svg += '<rect x="'+(x(i)-width/2)+'" y="'+y(p[field])+'" width="'+width+'" height="'+(170-y(p[field]))+'" rx="2" class="mw-chart-bar"><title>'+escape(p.date)+' · '+fmt(p[field],0)+'亿</title></rect>'; });
        }
        [0, Math.floor((points.length-1)/2),points.length-1].forEach(function (i) { svg += '<text x="'+x(i)+'" y="195" text-anchor="middle">'+escape(points[i].date.slice(5))+'</text>'; });
        return svg + '</svg>';
    }
    function renderBreadth(m, state) {
        var current = m.points.length ? m.points[m.points.length-1] : null, previous = m.points.length > 1 ? m.points[m.points.length-2] : null;
        var delta = current && previous && current.breadth !== null && previous.breadth !== null ? current.breadth-previous.breadth : null;
        var body = '<div class="mw-chart-legend"><span><i></i>上涨家数占比 / %</span><span>较上一观察日 '+ (delta === null ? '—' : '<b class="'+tone(delta)+'">'+(delta>0?'+':'')+fmt(delta,1)+' 个百分点</b>')+'</span></div>' + chart(m.points,'breadth',100,'市场广度历史');
        body += '<div class="mw-volume-label">成交额 / 亿元 <span>盘中末柱尚未完成</span></div>' + chart(m.points,'amount',null,'样本成交额历史');
        if (!m.points.length) body = empty(statusText(state,'market','尚无带完整日期的历史观察'), '已有行情不会被补成平直曲线；可继续查看指数与板块。');
        body += '<details class="mw-details"><summary>查看数值与口径</summary><p>广度 = 上涨家数 ÷ 有报价样本数。0% 是有效的全跌状态。缺失观察断开；成交额与广度分别使用独立刻度。</p><div class="mw-table-scroll"><table><thead><tr><th>交易日</th><th>上涨占比</th><th>成交额（亿）</th></tr></thead><tbody>'+m.points.map(function(p){return '<tr><td>'+p.date+'</td><td>'+fmt(p.breadth,1,'%')+'</td><td>'+fmt(p.amount,0)+'</td></tr>';}).join('')+'</tbody></table></div></details>';
        return panel('市场正在扩散，还是收缩', '01 / 市场事实', body, link('monitor','完整监控'),'mw-breadth');
    }
    function direction(period) {
        period = period || {};
        var label = {up:'上行',down:'下行',range:'震荡',unavailable:'不足'}[period.direction] || '不足';
        var pending = period.confirmation_status === 'provisional';
        return '<span class="mw-direction '+(period.direction==='up'?'mw-up':period.direction==='down'?'mw-down':'')+'">'+label+(pending?'<small>未收盘</small>':'')+'</span>';
    }
    function renderTrend(m, state) {
        var body = m.indices.length ? '<div class="mw-table-scroll"><table class="mw-trend-table"><thead><tr><th>指数 / 数据日</th><th>日线</th><th>周线</th><th>月线</th></tr></thead><tbody>'+m.indices.map(function(r){ var p=r.periods||{}; return '<tr><td><b>'+escape(r.index_name)+'</b><small>'+escape(r.data_cutoff)+(r.source_status!=='fresh'?' · 过期':'')+'</small></td><td>'+direction(p.daily)+'</td><td>'+direction(p.weekly)+'</td><td>'+direction(p.monthly)+'</td></tr>'; }).join('')+'</tbody></table></div>' : empty(statusText(state,'trend','指数趋势证据不足'),'需要足够的真实收盘数据，才显示方向。');
        body += '<details class="mw-details"><summary>趋势依据与确认条件</summary><p>均线方向是描述，不是买卖信号。周月线未收盘时保持“未收盘”标记。</p>'+m.indices.map(function(r){return '<div class="mw-method"><b>'+escape(r.index_name)+'</b><p>'+escape(list(((r.periods||{}).daily||{}).evidence).join('；'))+'</p><p>'+escape((r.summary||{}).watch||'')+'</p></div>';}).join('')+'</details>';
        return panel('不同周期是否同向', '02 / 趋势核对',body,link('sentiment','趋势与风格'));
    }
    function renderSectors(m, state) {
        var body = m.sectors.length ? '<ol class="mw-sector-list">'+m.sectors.slice(0,6).map(function(r,i){return '<li><span class="mw-rank">'+String(i+1).padStart(2,'0')+'</span><div><strong>'+escape(r.name)+'</strong><small>'+escape(r.trade_date)+' · '+escape(r.data_source==='qmt'?'市场快照':'板块观察')+'</small></div><span class="mw-sector-bar"><i class="'+tone(r.change)+'" style="width:'+Math.min(100,Math.abs(r.change)*16)+'%"></i></span><b class="'+tone(r.change)+'">'+pct(r.change)+'</b></li>';}).join('')+'</ol>' : empty(statusText(state,'market','没有与行情同日的行业观察'),'前一天的热门板块不冒充今天主线。');
        var metric = m.sectors.length && m.sectors[0].heat_metric === 'turnover' ? '行业样本成交额' : '来源热度';
        body += '<p class="mw-note">按'+metric+'排序，右侧为板块涨跌观察。成交额或热度排名都不能证明资金净流入，也不代表可买入。</p>';
        return panel('成交与关注集中在哪里', '03 / 板块线索',body,link('sector','分析板块'));
    }
    function stockButton(row) { var code=String(row.stock_code||'').split('.')[0]; return /^\d{6}$/.test(code)&&code!=='000000'?'<button type="button" class="mw-stock" data-mw-stock="'+code+'"><b>'+escape(row.stock_name||row.short_name||code)+'</b><small>'+code+'</small></button>':escape(row.stock_name||row.short_name||'未知股票'); }
    function renderCandidates(m, state) {
        var body = m.candidates.length ? '<div class="mw-candidates">'+m.candidates.map(function(r){return '<article>'+stockButton(r)+'<div><span class="mw-tag">研究入选</span><p>'+escape(r.reason||r.selection_reason||r.explanation||list(r.reasons).join('；')||'进入详情核对策略依据与失效条件')+'</p></div></article>';}).join('')+'</div>' : empty(m.coherent?'本批次没有新增研究候选':statusText(state,'pool','候选证据尚未通过一致性核验'),m.coherent?'保留观察，先管理已有持仓。':'日期、批次、数量和决策状态必须匹配；当前不能给出候选数量或买卖结论。');
        if(m.hypothesis) body='<blockquote class="mw-thesis"><strong>本批次市场假设</strong><p>'+escape(m.hypothesis.thesis||'暂无文字假设')+'</p><small>失效条件：'+escape(list(m.hypothesis.invalidations).join('；')||'未记录，不能直接据此行动')+'</small></blockquote>'+body;
        body += '<p class="mw-note">'+(m.historical?'历史候选只用于复盘，进入个股详情查看的是当前快照。':'候选是研究结果，确认条件和模拟执行状态在详情核对。')+'</p>';
        return panel('从线索到个股依据','04 / 候选观察',body,link('trading-v3-candidates','全部候选'));
    }
    function renderWatch(m, state) {
        var payload=source(state,'watch'), rows=rowsOf(payload), held=rows.filter(function(r){return number(r.shares)>0;});
        var body = rows.length ? '<div class="mw-watch-list">'+rows.slice(0,5).map(function(r){
            var quoteDate=day(r.quote_trade_date||r.quote_snapshot_at), stale=payload.snapshot_stale===true||r.snapshot_stale===true||['stale','missing','unavailable'].indexOf(r.quote_status)>=0;
            var verified=!!quoteDate && ['fresh','closed','paused'].indexOf(r.quote_status)>=0 && !stale;
            return '<article>'+stockButton(r)+'<div><strong class="'+(verified?tone(r.change_pct):'')+'">'+pct(r.change_pct)+'</strong><small>'+escape(quoteDate||'报价日期待核验')+(stale?' · 旧报价':verified?'':' · 待核验')+'</small></div><span class="mw-tag">'+(number(r.shares)>0?'持仓记录':'自选观察')+'</span></article>';
        }).join('')+'</div>' : empty(statusText(state,'watch','还没有自选记录'),'在个股详情加入观察，再从这里跟踪。');
        body += '<p class="mw-note">当前自选快照'+(rows.length?' · '+held.length+' 只持仓记录':'')+'。不随历史日期回放；数量不等于可卖数量。</p>';
        return panel('与我有关的变化','05 / 自选与持仓',body,link('portfolio','管理自选'));
    }
    function retainedRows(m) {
        var result=[];
        list(m.trend.retained_history).forEach(function(snapshot){
            var observed=day(snapshot.trade_date);
            if(!observed || observed>=m.requestedDate || !day(snapshot.retained_at) || day(snapshot.retained_at)>observed) return;
            list(snapshot.indices).forEach(function(r){
                var current=m.indices.find(function(i){return i.index_code===r.index_code;});
                if(!current || current.data_cutoff<=observed || number(r.subsequent_change_pct)===null) return;
                result.push({date:observed, end:current.data_cutoff,name:r.index_name||current.index_name, direction:((r.periods||{}).daily||{}).direction,change:r.subsequent_change_pct});
            });
        });
        return result.sort(function(a,b){return b.date.localeCompare(a.date);}).slice(0,4);
    }
    function renderVerdict(m, state) {
        var records=retainedRows(m);
        var body=records.length?'<div class="mw-table-scroll"><table><thead><tr><th>原始观察</th><th>当时日线</th><th>此后指数变化</th></tr></thead><tbody>'+records.map(function(r){return '<tr><td><b>'+escape(r.name)+'</b><small>'+r.date+'</small></td><td>'+direction({direction:r.direction})+'</td><td class="'+tone(r.change)+'">'+pct(r.change)+'<small>截至 '+escape(r.end)+'</small></td></tr>';}).join('')+'</tbody></table></div>':empty(statusText(state,'trend','尚无可核验的原始判断样本'),'只使用当时留存的观察；不从今天的走势倒造历史判断或相似胜率。');
        body+='<p class="mw-note">此后指数变化不是策略收益或预测胜率。交易评判需同时核对样本数、期限、成本、回撤与样本外表现。</p>';
        return panel('当时的判断，后来怎样','06 / 判断验证',body,link('trading-v3-hypotheses','连续跟踪'));
    }
    function renderEvidence(m,state) {
        var titles={market:'市场广度与板块',trend:'指数趋势与历史',context:'策略状态',pool:'候选批次',watch:'当前自选',hypotheses:'市场假设'};
        var body='<div class="mw-evidence-list">'+Object.keys(titles).map(function(key){ var s=state[key]||{}, value=source(state,key), observed=day(value.trade_date||value.data_cutoff||value.decision_session_date||value.data_date); return '<div><span>'+titles[key]+'</span><b class="'+(s.status==='error'?'mw-warning':'')+'">'+(s.status==='ready'?'已读取':s.status==='loading'?'读取中':'读取失败')+'</b><small>'+escape(observed|| (key==='watch'?'当前账户快照':'以详情证据日期为准'))+'</small>'+(s.status==='error'?'<button type="button" data-mw-retry="'+key+'">重试</button>':'')+'</div>';}).join('')+'</div>';
        body+='<p class="mw-note">已读取只表示请求成功，是否可用于判断由日期、完整性与有效状态决定。行情时点 '+escape(m.monitor.data_time||'待核验')+'；策略证据截止 '+escape(m.context.evidence_as_of||'待核验')+'。</p>';
        return '<details class="mw-panel mw-evidence"><summary>证据状态与指标口径<span>数据获取的管理在系统工具中</span></summary>'+body+'</details>';
    }
    function render(state, requestedDate, clock) {
        var m=model(state,requestedDate,clock);
        var errors=Object.keys(state).filter(function(k){return state[k]&&state[k].status==='error';}).length;
        return '<div class="market-workbench">'+renderHero(m)+(errors?'<div class="mw-warning-bar" role="status">'+errors+' 项证据读取失败，已显示为待核验。<button type="button" data-mw-retry="failed">重试失败项</button></div>':'')+'<div class="mw-grid"><div class="mw-column">'+renderBreadth(m,state)+renderSectors(m,state)+renderWatch(m,state)+'</div><div class="mw-column">'+renderTrend(m,state)+renderCandidates(m,state)+renderVerdict(m,state)+'</div></div>'+renderEvidence(m,state)+'<footer class="mw-footer"><span>先看事实，再核对依据，最后验证判断。</span>'+link('review','每日复盘')+link('strategy-center','策略评价')+'</footer></div>';
    }
    var active=null, sequence=0, timer=null;
    function stop() { if(timer) clearTimeout(timer); timer=null; if(active) active.cancelled=true; active=null; }
    function load(requestedDate, container, options) {
        options=options||{}; stop();
        var session={id:++sequence,cancelled:false,container:container,day:requestedDate,state:{contextTruth:options.contextTruth},options:options}; active=session;
        var d=encodeURIComponent(requestedDate);
        var paths={market:'/api/monitor/data?date='+d,trend:'/api/hot-data/market-trend?date='+d,context:'/api/v3/context?trade_date='+d,pool:'/api/v3/stock-pool?trade_date='+d,watch:'/api/portfolio/list',hypotheses:'/api/v3/hypotheses/latest?scope_type=MARKET&limit=8&trade_date='+d};
        Object.keys(paths).forEach(function(k){session.state[k]={status:'loading'};});
        function paint(){
            if(session.cancelled || active!==session) return;
            var opened=[]; container.querySelectorAll('details').forEach(function(e,i){if(e.open) opened.push(i);});
            var focused=container.contains(document.activeElement)?document.activeElement:null;
            var focusKey=focused&&['data-mw-tab','data-mw-stock','data-mw-retry'].map(function(k){return focused.hasAttribute(k)?[k,focused.getAttribute(k)]:null;}).filter(Boolean)[0];
            container.innerHTML=render(session.state,requestedDate,options.clock?options.clock():{});
            container.querySelectorAll('details').forEach(function(e,i){e.open=opened.indexOf(i)>=0;});
            if(focusKey){var target=container.querySelector('['+focusKey[0]+'="'+focusKey[1]+'"]');if(target)target.focus({preventScroll:true});}
        }
        function request(key){
            var previous=session.state[key];
            session.state[key]=previous && previous.status==='ready'?{status:'ready',value:previous.value,refreshing:true}:{status:'loading'};
            return options.request(paths[key],15000).then(function(value){
                if(value && (value.error || value.status==='error')) throw new Error('source unavailable');
                var unwrapped=value && value.data!==undefined && !Array.isArray(value.data) && /^\/api\/v3\//.test(paths[key]) ? value.data:value;
                session.state[key]={status:'ready',value:unwrapped};
            }).catch(function(){session.state[key]={status:'error'};}).finally(paint);
        }
        container.onclick=function(event){
            var button=event.target.closest('button'); if(!button || !container.contains(button)) return;
            if(button.hasAttribute('data-mw-tab')) options.navigate(button.getAttribute('data-mw-tab'));
            else if(button.hasAttribute('data-mw-stock')) options.stock(button.getAttribute('data-mw-stock'));
            else if(button.hasAttribute('data-mw-retry')){
                var key=button.getAttribute('data-mw-retry');
                var keys=key==='failed'?Object.keys(paths).filter(function(k){return session.state[k].status==='error';}):[key];
                keys.filter(function(k){return paths[k]&&session.state[k].status!=='loading';}).forEach(request);paint();
            }
        };
        paint();
        return Promise.all(Object.keys(paths).map(request)).then(function(){
            if(session.cancelled || active!==session)return;
            var failed=Object.keys(paths).filter(function(k){return session.state[k].status==='error';}).length;
            if(options.status)options.status(failed?'工作台已更新，'+failed+'项证据待核验':'工作台已更新',failed>0);
            function schedule(){
                if(session.cancelled || active!==session)return;
                timer=setTimeout(function(){
                if(session.cancelled || active!==session)return;
                if(document.hidden || (options.isActive&&!options.isActive())){schedule();return;}
                var clockRefresh=options.refreshClock?options.refreshClock():Promise.resolve();
                Promise.resolve(clockRefresh).then(function(){
                    if(session.cancelled || active!==session)return;
                    var clock=options.clock?options.clock():{};
                    if(clock.is_intraday!==true || day(clock.active_trade_date)!==requestedDate)return;
                    return Promise.all(Object.keys(paths).map(request));
                }).catch(function(){}).finally(schedule);
            },60000); }
            schedule();
            return failed?{loadError:failed+'项证据待核验'}:{};
        });
    }
    var api={number:number,model:model,render:render,retainedRows:retainedRows,load:load,stop:stop};
    root.MarketWorkbench=api;
    if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
