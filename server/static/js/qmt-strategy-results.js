/* Daily QMT-source simulation receipts; this page only reads published results. */
(function (root) {
    'use strict';
    var active = null;
    function esc(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) { return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
    function list(value) { return Array.isArray(value) ? value : []; }
    function number(value) {
        if (value == null || (typeof value !== 'number' && typeof value !== 'string') || (typeof value === 'string' && !value.trim())) return null;
        var n = Number(value); return Number.isFinite(n) ? n : null;
    }
    function fmt(value, digits) { var n = number(value); return n === null ? '—' : n.toLocaleString('zh-CN', {maximumFractionDigits:digits == null ? 3 : digits}); }
    function pct(value) { var n = number(value); return n === null ? '—' : (n > 0 ? '+' : '') + n.toFixed(2) + '%'; }
    function textValue(value) { if (value == null) return '未取得'; if (typeof value === 'object') return JSON.stringify(value); return String(value); }
    function status(value) {
        var states = {
            COMPLETED:['已完成','ok'], COMPLETED_EMPTY:['已完成 · 无符合股票','empty'], DATA_BLOCKED:['数据不足','blocked'],
            FAILED:['执行失败','error'], ERROR:['执行失败','error'], PARTIAL:['部分完成','blocked'], PARTIAL_DATA_BLOCKED:['部分策略缺数据','blocked'], RUNNING:['执行中','pending'], PENDING:['待执行','pending'], AWAITING_EXECUTION:['已准备输入 · 待执行回传','pending'],
            AVAILABLE:['可读取','ok'], READY:['可读取','ok'], NOT_REGISTERED:['尚未登记每日任务','blocked'],
            REGISTERED:['已登记','ok'], UNKNOWN:['尚未确认','pending'], UNAVAILABLE:['暂不可读取','error'], CONTRACT_MISMATCH:['每日任务配置不一致','error'], QUEUED:['输入准备排队中','pending'], PREPARING:['正在准备输入','pending'], ISSUED:['输入准备完成','ok']
        };
        return states[value] || [value ? '待核验 · ' + value : '待核验','pending'];
    }
    function badge(value) { var s = status(value); return '<span class="qr-status ' + s[1] + '">' + esc(s[0]) + '</span>'; }
    function mode(value) { return value === 'DAILY' ? '每日执行' : value === 'REPLAY' ? '历史重放' : '执行类型待核验'; }
    function origin(value) { return value === 'QMT_ENTRY' ? 'QMT策略入口' : value === 'WINDOWS_DAILY' ? 'Windows每日模拟任务' : value || '执行入口未提供'; }
    function validRow(row) {
        var selected = list(row.selected), count = number(row.selected_count);
        if (row.status === 'COMPLETED_EMPTY') return count === 0 && selected.length === 0;
        return row.status === 'COMPLETED' && count !== null && count > 0 && count === selected.length;
    }
    function model(data, date) {
        data = data || {};
        var run = data.latest, result = run && run.result, readable = !!(run && result && run.trade_date === date && result.trade_date === date && result.simulation_only === true && result.real_order_allowed === false);
        var rows = readable ? list(result.strategy_rows).concat(list(result.combination_rows)) : [];
        var codes = new Set(), completed = 0, empty = 0, blocked = 0, picked = 0;
        rows.forEach(function (r) {
            if (validRow(r)) {
                completed += 1;
                if (r.status === 'COMPLETED_EMPTY') empty += 1;
                picked += r.selected.length;
                r.selected.forEach(function (p) { if (p.stock_code) codes.add(p.stock_code); });
            } else if (r.status === 'DATA_BLOCKED') blocked += 1;
        });
        return {data:data,run:run,result:result,readable:readable,rows:rows,completed:completed,empty:empty,blocked:blocked,picked:picked,unique:codes.size,date:date};
    }
    function scheduleHTML(data) {
        var s = data.schedule || {}, catalog = data.catalog || {};
        var html = '<section class="qr-panel"><div class="qr-panel-head"><h3>每日执行与策略范围</h3>' + badge(s.status) + '</div><div class="qr-schedule">';
        html += '<div><span>计划执行时间</span><strong>' + esc(s.cron_time || '尚未提供') + (s.timezone === 'Asia/Shanghai' ? ' · 北京时间' : '') + '</strong><small>' + esc(s.description || '交易日按计划读取QMT来源数据并保存模拟结果') + '</small></div>';
        html += '<div><span>任务状态</span><strong>' + (s.enabled === true ? '已启用' : s.enabled === false ? '已停用' : '启用状态待确认') + '</strong><small>' + esc(s.owner === 'WINDOWS_QMT' ? 'Windows / QMT' : s.owner || '执行主机尚未提供') + '</small></div>';
        html += '<div><span>执行方式</span><strong>仅模拟</strong><small>选股与后续表现记录 · 不提交真实委托</small></div></div>';
        var recentStatus={success:'COMPLETED',failed:'FAILED',error:'ERROR',running:'RUNNING',blocked:'DATA_BLOCKED',unknown:'UNKNOWN'}[s.last_run_status];
        if (s.last_run_summary || s.last_run_at || recentStatus) {
            html += '<div class="qr-scope-note"><strong>最近调度记录</strong> ' + badge(recentStatus || 'UNKNOWN') + '<p>' + esc(s.last_run_summary || '调度执行状态尚待确认') + '</p>';
            if (s.last_run_at) html += '<span>' + esc(s.last_run_at) + (number(s.last_run_duration) === null ? '' : ' · 用时 ' + fmt(s.last_run_duration,0) + ' 秒') + '</span>';
            html += '</div>';
        }
        var roster = list(catalog.strategies).concat(list(catalog.combinations));
        if (roster.length) html += '<div class="qr-roster" aria-label="每日执行策略名单">' + roster.map(function (r) { return '<span>' + esc(r.name || r.strategy_key) + '</span>'; }).join('') + '</div>';
        var excluded = list(catalog.excluded);
        if (excluded.length) html += '<p class="qr-scope-note">本次未执行：' + excluded.map(function (r) { return esc(r.name || r.strategy_key) + (r.reason ? '（' + esc(r.reason) + '）' : ''); }).join('、') + '</p>';
        return html + '</section>';
    }
    function preparationJobs(data,date) { return list(data.preparation_jobs).filter(function(j){return j && j.trade_date===date;}); }
    function validPreparation(j) {
        return /^[0-9a-f]{32}$/.test(String(j.request_id || '')) && ['QUEUED','PREPARING','ISSUED','FAILED'].indexOf(j.status)>=0 && j.simulation_only===true && j.real_order_allowed===false && j.automatic_real_order_submission===false && j.real_order_authority===false;
    }
    function preparationHTML(data,date) {
        var jobs=preparationJobs(data,date);if(!jobs.length)return '';
        var html='<section class="qr-panel"><div class="qr-panel-head"><h3>策略事实输入准备</h3><span>准备输入不等于策略已经执行</span></div><p class="qr-muted">请求固定在下列交易日；此阶段尚未形成选股结果，也没有后续成交收益。</p>';
        jobs.forEach(function(j,index){
            var verified=validPreparation(j);
            var jobBadge=verified && j.status==='FAILED'?'<span class="qr-status error">输入准备失败</span>':badge(verified?j.status:'UNKNOWN');
            html+='<details class="qr-preparation"'+(index===0?' open':'')+'><summary>'+jobBadge+'<span>'+esc(j.trade_date+' · '+mode(j.run_mode))+'</span><small>'+esc(j.updated_at || j.created_at || '时间未提供')+'</small></summary>';
            html+='<p>'+esc(verified ? j.reason || '等待服务器提供本请求的准备状态说明' : '本准备记录的模拟范围或请求身份尚待核验。')+'</p>';
            if(verified && j.error_code)html+='<p class="qr-preparation-error">失败原因代码：'+esc(j.error_code)+'</p>';
            if(verified && j.status==='PREPARING' && j.lease_stale===true)html+='<p class="qr-preparation-error">准备租约已过期，等待本请求重新取得执行权；尚无新执行结果。</p>';
            html+='<dl class="qr-evidence">';
            [['请求编号',j.request_id],['固定交易日',j.trade_date],['创建时间',j.created_at],['最近状态更新',j.updated_at],['最近准备心跳',j.heartbeat_at],['已尝试次数',number(j.attempt_count)===null?null:fmt(j.attempt_count,0)],['Windows应用版本',j.edge_build_sha]].forEach(function(v){html+='<dt>'+esc(v[0])+'</dt><dd><code>'+esc(v[1]==null || v[1]===''?'未提供':v[1])+'</code></dd>';});
            if(verified && j.status==='ISSUED')html+='<dt>已签发输入编号</dt><dd><code>'+esc(j.snapshot_id || '未提供')+'</code></dd>';
            html+='</dl></details>';
        });
        return html+'</section>';
    }
    function toolbarHTML(m, state) {
        var dates = list(m.data.dates), runs = list(m.data.runs), html = '<div class="qr-toolbar"><label>执行日期<select data-qr="date" aria-label="执行日期">';
        if (!dates.some(function (d) { return d.trade_date === m.date; })) html += '<option value="' + esc(m.date) + '">' + esc(m.date) + ' · 无已保存批次</option>';
        dates.forEach(function (d) { html += '<option value="' + esc(d.trade_date) + '"' + (d.trade_date === m.date ? ' selected' : '') + '>' + esc(d.trade_date) + ' · ' + (number(d.run_count)===0?'输入准备记录':fmt(d.run_count,0)+'个已签发批次') + '</option>'; });
        html += '</select></label>';
        if (runs.length) {
            html += '<label class="qr-run-select">执行批次<select data-qr="run" aria-label="执行批次">';
            runs.forEach(function (r) { html += '<option value="' + esc(r.run_uid) + '"' + (m.run && r.run_uid === m.run.run_uid ? ' selected' : '') + '>' + esc(mode(r.run_mode) + ' · ' + (r.completed_at || r.issued_at || r.run_uid) + ' · ' + status(r.status)[0]) + '</option>'; });
            html += '</select></label>';
        }
        return html + '<button type="button" data-qr="refresh"' + (state.busy ? ' disabled' : '') + '>刷新结果</button></div>';
    }
    function valuesHTML(values) {
        if (!values || typeof values !== 'object' || Array.isArray(values)) return '';
        var labels={provider:'数据提供方',trade_date:'数据日期',flow_date:'资金流日期',feature_time:'特征时间',source:'数据来源',coverage:'覆盖情况',data_snapshot_hash:'数据指纹',calibration_source:'校准数据来源',model_version:'预测模型版本',dataset_hash:'校准数据指纹',sample_count:'校准样本数',confidence:'预测置信度',authority:'校准来源依据',observed_at:'来源观察时间',model_versions:'已冻结模型版本',rejections:'校准未通过原因',
            market_latest_coverage_ratio:'市场最新数据覆盖率',market_tradable_coverage_ratio:'市场可交易数据覆盖率',market_eligible_stock_count:'应覆盖股票数',qmt_attestation_current:'QMT本批次校验证据',optional_formal_factors:'补充因子状态',
            volume_ratio:'量比',turnover_rate:'换手率',change_pct:'涨跌幅',main_net_inflow:'主力净流入',main_net_ratio:'主力净流入占比',
            return_5d:'5日涨跌',return_10d:'10日涨跌',return_20d:'20日涨跌',pct_chg_5d:'5日涨跌',pct_chg_20d:'20日涨跌',roe:'净资产收益率',pe_ttm:'市盈率TTM',pb:'市净率',
            technical_score:'技术评分',capital_score:'资金评分',sentiment_score:'情绪评分',event_score:'事件评分',fundamental_score:'基本面评分',growth_score:'成长评分',valuation_score:'估值评分',risk_score:'风险评分',
            long_term_score:'长期评分',final_trade_score:'综合策略评分',entry_score:'入场评分',data_quality_score:'数据质量评分',main_wave_score:'主升浪评分',trend_hold_score:'趋势持有评分',sector_rotation_score:'板块轮动评分',
            amount:'成交额',main_net_inflow_5d:'5日主力净流入',main_net_inflow_20d:'20日主力净流入',close:'收盘价',price:'参考价',ma5:'5日均价',ma10:'10日均价',ma20:'20日均价',ma60:'60日均价'};
        return '<dl class="qr-values">' + Object.keys(values).map(function (k) {
            var value=values[k], ratio=number(value), rendered=(k==='market_latest_coverage_ratio' || k==='market_tradable_coverage_ratio') && ratio!==null && ratio>=0 && ratio<=1 ? fmt(ratio*100,2)+'%' : esc(textValue(value));
            if(value && typeof value==='object' && !Array.isArray(value))rendered='<details><summary>查看明细</summary>'+valuesHTML(value)+'</details>';
            return '<dt title="' + esc(k) + '">' + esc(labels[k] || k) + '</dt><dd>' + rendered + '</dd>';
        }).join('') + '</dl>';
    }
    function sourceHTML(name, source) {
        source = source || {};
        var html = '<article class="qr-source"><strong>' + esc(name) + '</strong>' + badge(source.status);
        var values = {};
        ['provider','trade_date','flow_date','feature_time','source','coverage','market_latest_coverage_ratio','market_tradable_coverage_ratio','market_eligible_stock_count','qmt_attestation_current','optional_formal_factors','data_snapshot_hash','calibration_source'].forEach(function (k) { if (Object.prototype.hasOwnProperty.call(source,k)) values[k] = source[k]; });
        html += valuesHTML(values);
        if (source.proofs) html += '<details class="qr-evidence-details"><summary>来源时间、覆盖率及校验证据</summary>' + valuesHTML(source.proofs) + '</details>';
        if (list(source.reasons).length) html += '<p>' + esc(source.reasons.join('；')) + '</p>';
        return html + '</article>';
    }
    function evidenceHTML(m) {
        var run = m.run, execution=run.execution || {}, nativeIdentity=execution.bridge_identity || {}, input = run.input || {}, inputStatus = m.result.input_status || {};
        var html = '<section class="qr-panel"><div class="qr-panel-head"><h3>数据与执行依据</h3><span>下列信息来自本批次保存的记录</span></div><div class="qr-source-grid">';
        html += sourceHTML('V2策略输入', Object.assign({},inputStatus.v2,input.v2)) + sourceHTML('V3策略输入', Object.assign({},inputStatus.v3,input.v3)) + '</div>';
        html += '<details class="qr-evidence-details"><summary>查看数据时间、版本与执行指纹</summary><dl class="qr-evidence">';
        var contract=m.result.formula_contract || input.formula_contract || {};
        [
            ['执行入口',origin(run.origin)],['执行类型',mode(run.run_mode)],['决策时间',input.decision_at],['输入准备时间',input.prepared_at],
            ['输入签发时间',run.issued_at],['执行开始',execution.started_at],['执行完成',execution.finished_at || run.completed_at],['策略公式版本',typeof contract==='object' ? 'V2 '+(contract.manifest_version || '未提供')+' / V3 '+(contract.v3_version || '未提供') : contract],
            ['Windows应用版本',run.edge_build_sha],['QMT原生模型版本',nativeIdentity.strategy_build_sha],['执行编号',run.run_uid],['输入指纹',run.input_hash || input.input_hash],['结果指纹',run.result_hash],['原始快照指纹',input.snapshot_sha256]
        ].forEach(function (item) { html += '<dt>' + esc(item[0]) + '</dt><dd><code>' + esc(item[1] || '未提供') + '</code></dd>'; });
        return html + '</dl>' + (typeof contract==='object' && Object.keys(contract).length ? '<details class="qr-evidence-details"><summary>策略公式来源与指纹</summary>'+valuesHTML(contract)+'</details>' : '') + '</details></section>';
    }
    function conditionsHTML(conditions) {
        if (!list(conditions).length) return '';
        return '<div class="qr-condition">' + conditions.map(function (c) {
            return '<div>' + esc(c.label || c.key || '条件') + '：' + esc(textValue(c.value)) + (c.required == null ? '' : ' · 要求 ' + esc(textValue(c.required))) + '<em' + (c.status === 'BLOCK' ? ' class="block"' : '') + '>' + esc({PASS:'满足',BLOCK:'未满足',WATCH:'观察'}[c.status] || c.status || '待核验') + '</em></div>';
        }).join('') + '</div>';
    }
    function stockHTML(p) {
        var code = String(p.stock_code || ''), base = code.split('.')[0];
        if (!/^\d{6}$/.test(base) || base === '000000') return '<strong>' + esc(p.stock_name || code || '证券身份待核验') + '</strong>';
        return '<button type="button" class="qr-stock" data-qr-stock="' + base + '">' + esc(p.stock_name || base) + '<span>' + esc(code) + '</span></button>';
    }
    function performanceFor(performance, strategyKey, stockCode) {
        var entity = list(performance && performance.entities).find(function (e) { return e.strategy_key === strategyKey; });
        return entity && list(entity.picks).find(function (p) { return p.stock_code === stockCode; });
    }
    function performanceHTML(p, performance) {
        if (!p || p.status !== 'AVAILABLE' || number(p.return_pct) === null) return '<span class="qr-muted">' + esc((p && p.reason) || (performance && performance.reason) || '等待后续真实行情') + '</span>';
        var n = number(p.return_pct);
        return '<strong class="' + (n > 0 ? 'qr-up' : n < 0 ? 'qr-down' : '') + '">' + pct(n) + '</strong><small class="qr-muted">' + esc(p.entry_date || '') + ' → ' + esc(p.last_date || '') + '</small><small class="qr-muted">开盘参考 ' + fmt(p.entry_price) + ' / 观察价 ' + fmt(p.last_price) + '</small><small class="qr-muted">价格变化 · 非成交收益</small>';
    }
    function performanceSummaryHTML(performance) {
        var entities=list(performance && performance.entities);
        if(!entities.length)return '';
        var html='<section class="qr-panel"><div class="qr-panel-head"><h3>后续价格观察（非成交收益）</h3><span>按选股后实际取得的行情计算</span></div><p class="qr-muted">'+esc(performance.label || '次一可用开盘价至观察收盘的税费前价格变化')+'；没有成交、持仓或费用证据，不代表实际交易收益。</p><div class="qr-table-wrap"><table class="qr-table"><thead><tr><th scope="col">策略 / 组合</th><th scope="col">已验证股票</th><th scope="col">次一开盘至观察收盘</th><th scope="col">1个交易日</th><th scope="col">5个交易日</th><th scope="col">10个交易日</th><th scope="col">20个交易日</th></tr></thead><tbody>';
        entities.forEach(function(e){
            var average=e.status==='AVAILABLE' && number(e.selected_count)===number(e.verified_count) ? e.avg_return_pct : null;
            html+='<tr><td>'+esc(e.name || e.strategy_key)+'</td><td>'+fmt(e.verified_count,0)+' / '+fmt(e.selected_count,0)+'</td><td>'+pct(average)+'</td>';
            [1,5,10,20].forEach(function(sessions){var h=list(e.horizons).find(function(v){return v.sessions===sessions;});html+='<td>'+(h && h.status==='AVAILABLE' && number(h.verified_count)===number(e.selected_count) ? pct(h.avg_return_pct) : '<span class="qr-muted">等待行情</span>')+'</td>';});
            html+='</tr>';
        });
        return html+'</tbody></table></div></section>';
    }
    function isShadowObservation(p) { return p.selection_kind === 'ORIGINAL_V3_SHADOW_PORTFOLIO_OBSERVATION'; }
    function selectionLabel(p) {
        if (isShadowObservation(p)) return 'V3模拟组合观察入选（非买入指令）';
        if (p.selection_kind === 'FROZEN_MEMBER_UNION') return '成员策略合并观察（非买入指令）';
        if (p.selection_kind === 'FROZEN_V2_CONFIRMED_SIMULATION_SIGNAL') return 'V2冻结策略模拟信号（非买入指令）';
        return '仅模拟入选记录（非买入指令）';
    }
    function observationScoreHTML(p) {
        var html = '<small class="qr-muted">原始分</small>' + fmt(p.score,3) + (p.score_scale == null ? '' : '<small class="qr-muted"> / ' + esc(p.score_scale) + '</small>');
        if (!isShadowObservation(p)) return html;
        var basis = {CALIBRATED_EXPECTED_RETURN_NET_PCT:'校准净期望排序（预测口径）',UNCALIBRATED_RAW_SCORE_RESEARCH_ONLY:'未校准原始评分排序（仅研究）'}[p.ranking_basis];
        return html + '<small class="qr-muted">观察排名 ' + fmt(p.rank_no,0) + '</small><small class="qr-muted">排序分 ' + fmt(p.selection_score,4) + '</small><small class="qr-muted">' + esc(basis || p.ranking_basis || '排序依据未提供') + '</small>';
    }
    function predictionHTML(p) {
        if (!isShadowObservation(p)) return '';
        var expected = number(p.expected_return_net_pct);
        return '<div class="qr-prediction"><strong>模型校准净期望（预测，非已实现收益）</strong><span>' + (expected === null ? '未取得校准预测' : pct(expected)) + '</span><details><summary>预测模型与校准依据</summary>' + valuesHTML({model_version:p.model_version,dataset_hash:p.dataset_hash,sample_count:p.sample_count,confidence:p.confidence}) + '</details></div>';
    }
    function rowHTML(row, performance, combination) {
        var verified = validRow(row), selected = verified ? row.selected : [], shadow = row.family === 'V3' || selected.some(isShadowObservation), html = '<details class="qr-model"' + (selected.length ? ' open' : '') + '><summary><div class="qr-model-title"><strong>' + esc(row.name || row.strategy_key) + '</strong><small>' + esc((combination ? '组合策略' : '独立策略') + ' · ' + (row.version || '版本未提供')) + '</small></div>';
        html += badge((row.status === 'COMPLETED' || row.status === 'COMPLETED_EMPTY') && !verified ? 'UNKNOWN' : row.status) + '<span class="qr-count">' + (verified ? selected.length + '只模拟入选' : '结果未确认') + '</span></summary><div class="qr-model-body">';
        if (list(row.members).length) html += '<div class="qr-members">' + row.members.map(function (member) { var weight = number(member.weight); return '<span>' + esc(member.name || member.strategy_key) + (weight === null ? '' : ' · ' + fmt(weight*100,1) + '%') + '</span>'; }).join('') + '</div>';
        if (shadow) html += '<p class="qr-model-note qr-observation">V3模拟组合观察入选（非买入指令）。遵循原策略观察组合的名额与排序，可能包含未校准或预测条件未满足的股票；执行完成不等于满足买入条件。</p>';
        else if (combination) html += '<p class="qr-model-note">成员策略入选记录的冻结权重合并观察，可能包含V3观察成员；不是新的买入指令。</p>';
        if (!verified) {
            html += '<p class="qr-model-note">' + esc(list(row.blocked_reasons).join('；') || (row.source && list(row.source.reasons).join('；')) || ((row.status === 'COMPLETED' || row.status === 'COMPLETED_EMPTY') ? '选中数量与保存明细不一致，结果尚待核验。' : '尚未取得可核验的执行结果。')) + '</p>';
        } else if (row.status === 'COMPLETED_EMPTY') {
            html += '<p class="qr-model-note">' + (shadow ? '本策略已执行完成，本批次没有进入原模拟观察组合名额的股票。' : combination ? '本组合已执行完成，本批次没有成员策略入选记录。' : '本策略已执行完成，本批次没有满足全部条件的股票。') + '</p>';
        } else {
            html += '<div class="qr-table-wrap"><table class="qr-table"><thead><tr><th class="qr-stock-col" scope="col">模拟入选股票</th><th class="qr-score-col" scope="col">评分 / 排序</th><th scope="col">为什么入选 / 条件</th><th scope="col">本次实测数据</th><th class="qr-performance-col" scope="col">后续价格观察（非成交收益）</th></tr></thead><tbody>';
            selected.forEach(function (p) {
                html += '<tr><td>' + stockHTML(p) + '<small class="qr-selection-kind">' + esc(selectionLabel(p)) + '</small>' + (p.status ? '<small class="qr-muted">' + (isShadowObservation(p) ? '预测状态：' : '原记录状态：') + esc(p.status) + '</small>' : '') + '</td><td>' + observationScoreHTML(p) + '</td><td>';
                html += list(p.reasons).length ? '<ul class="qr-reasons">' + p.reasons.map(function (r) { return '<li>' + esc(r) + '</li>'; }).join('') + '</ul>' : '<span class="qr-muted">未提供选中理由</span>';
                html += conditionsHTML(p.conditions) + predictionHTML(p) + '</td><td>' + (valuesHTML(p.feature_values) || '<span class="qr-muted">未保存特征数值</span>') + '</td><td>' + performanceHTML(performanceFor(performance,row.strategy_key,p.stock_code), performance) + '</td></tr>';
            });
            html += '</tbody></table></div>';
        }
        if (row.source) html += '<details class="qr-rejections"><summary>本策略输入状态与覆盖情况</summary>' + sourceHTML('策略数据来源',row.source) + '</details>';
        if (list(row.rejected_summary).length) html += '<details class="qr-rejections"><summary>未入选与阻断原因</summary>' + row.rejected_summary.map(function (r) { return '<p>' + esc(r.status || '未入选') + ' · ' + fmt(r.count,0) + '只' + (list(r.reasons).length ? ' · ' + esc(r.reasons.join('；')) : '') + '</p>'; }).join('') + '</details>';
        return html + '</div></details>';
    }
    function render(data, date, state) {
        state = state || {}; var m = model(data,date), html = '<section class="qmt-results"><div class="qr-heading"><div><h2>QMT每日模拟</h2><p>查看各策略的模拟入选股票、原始理由与实测数据，并跟踪后续真实行情。V3按原模拟组合排序观察，不等于买入信号。</p></div><span class="qr-simulation">仅模拟 · 真实委托关闭</span></div>';
        html += toolbarHTML(m,state);
        if (state.error) html += '<div class="qr-empty qr-error" role="alert"><strong>结果读取失败</strong><p>' + esc(state.error) + '</p><button class="qr-button" type="button" data-qr="refresh">重新读取</button></div>';
        html += preparationHTML(m.data,m.date);
        if (!m.readable) {
            var invalid = !!m.run, awaiting = invalid && !m.result && m.run.status === 'AWAITING_EXECUTION';
            var latestJob=preparationJobs(m.data,m.date)[0], preparationTitle=latestJob && validPreparation(latestJob) && {QUEUED:'策略输入排队等待准备',PREPARING:'正在准备策略事实输入',FAILED:'策略输入准备失败',ISSUED:'输入准备完成，等待执行结果'}[latestJob.status];
            html += '<div class="qr-empty"><strong>' + (awaiting ? '输入已保存，等待模拟执行结果' : invalid ? '本批次结果尚待核验' : m.data.status === 'UNAVAILABLE' ? '结果暂不可读取' : preparationTitle || '尚无执行结果') + '</strong><p>' + (awaiting ? '已准备本批次策略输入；Windows任务尚未完成结果回传。下方显示最近调度状态。' : invalid ? '执行日期、模拟范围或结果证据未完成核验；请查看批次状态后重试。' : esc(m.data.reason || m.date + ' 尚未保存执行批次。每日任务完成后，这里会显示选股结果和逐条理由。')) + '</p>';
            var latestDate = list(m.data.dates)[0];
            if (latestDate && latestDate.trade_date !== date) html += '<button class="qr-button" type="button" data-qr-date="' + esc(latestDate.trade_date) + '">查看最近一次 · ' + esc(latestDate.trade_date) + '</button>';
            html += '</div>' + scheduleHTML(m.data) + '</section>'; return html;
        }
        html += '<section class="qr-hero"><div class="qr-hero-top"><div><div class="qr-eyebrow">' + esc(mode(m.run.run_mode) + ' · ' + origin(m.run.origin)) + '</div><h3>' + esc(m.date) + ' 的模拟入选结果</h3></div>' + badge(m.run.status) + '</div><p class="qr-run-note">复用系统已登记的策略公式，以本批次QMT来源输入执行。后续表现仅为真实行情价格观察；没有成交、持仓或费用证据，不代表交易收益。</p><div class="qr-metrics">';
        [[m.completed,'策略 / 组合执行完成',m.empty+'个完成但无选股'],[m.blocked,'数据不足', '查看各策略的缺数原因'],[m.completed===0?'未判定':m.unique,'选中股票去重',m.completed===0?'尚无可核验的入选结果':m.picked+'次策略选中记录'],[list(m.result.strategy_rows).length+' + '+list(m.result.combination_rows).length,'独立策略 + 组合','本批次实际执行范围']].forEach(function (v) { html += '<div><span>' + esc(v[1]) + '</span><strong>' + esc(v[0]) + '</strong><small>' + esc(v[2]) + '</small></div>'; });
        html += '</div></section>';
        var returnedPerformance=state.performance || {}, performance = returnedPerformance.run_uid===m.run.run_uid && returnedPerformance.trade_date===m.date && returnedPerformance.is_execution_return!==true ? returnedPerformance : {}, query = String(state.query || '').trim().toLowerCase(), filter = state.filter || '';
        html += '<div class="qr-panel-head"><h3>逐策略查看</h3><span>选中股票可打开详情 · 理由和数值均来自本次保存结果</span></div><div class="qr-toolbar"><label>搜索策略 / 股票<input data-qr="search" value="' + esc(state.query || '') + '" placeholder="策略名、股票名或代码"></label><label>执行状态<select data-qr="filter"><option value="">全部状态</option>';
        [['COMPLETED','有选股结果'],['COMPLETED_EMPTY','已完成无选股'],['DATA_BLOCKED','数据不足']].forEach(function (v) { html += '<option value="' + v[0] + '"' + (filter === v[0] ? ' selected' : '') + '>' + v[1] + '</option>'; });
        html += '</select></label></div><div class="qr-model-list">';
        var shown = 0;
        m.rows.forEach(function (r) {
            var hay = String(r.name || '')+' '+String(r.strategy_key || '')+' '+list(r.selected).map(function (p) { return (p.stock_name || '')+' '+(p.stock_code || ''); }).join(' ');
            if ((filter && r.status !== filter) || (query && hay.toLowerCase().indexOf(query) < 0)) return;
            shown += 1; html += rowHTML(r,performance,list(m.result.combination_rows).indexOf(r)>=0);
        });
        if (!shown) html += '<div class="qr-empty"><strong>没有符合筛选的策略</strong><p>调整策略或股票名称和执行状态后查看。</p></div>';
        html += '</div><p class="qr-scope-note">' + esc(state.performanceError ? '后续表现读取失败：'+state.performanceError : performance.status === 'AVAILABLE' ? '后续表现已按服务器核验的真实行情计算。' : performance.reason || '后续表现等待下一交易日开盘及观察行情；缺少行情时保持未知。') + '</p>';
        return html + performanceSummaryHTML(performance) + evidenceHTML(m) + scheduleHTML(m.data) + '</section>';
    }
    function stop() { if (active) { active.alive=false; active.seq+=1;if(active.pollTimer!==null && active.pollTimer!==undefined)clearTimeout(active.pollTimer);active=null; } }
    function load(date, container, options) {
        stop(); options=options || {};
        var session={alive:true,seq:0,pollTimer:null}, state={data:{},date:date,query:'',filter:'',busy:false}; active=session;
        var request=options.request || function(url) { return fetch(url,{cache:'no-store'}).then(function(r) { if (!r.ok) throw new Error('请求失败（'+r.status+'）'); return r.json(); }); };
        function current(seq) { return session.alive && active === session && seq === session.seq; }
        function draw() {
            if (!session.alive) return;
            var input=container.querySelector('[data-qr="search"]'), focused=input && document.activeElement === input, start=focused ? input.selectionStart : null;
            container.innerHTML=render(state.data,state.date,state);
            if (focused) { input=container.querySelector('[data-qr="search"]'); if(input){input.focus();if(start!=null)input.setSelectionRange(start,start);} }
        }
        function future(run,seq) {
            var endpoint=run && run.performance && run.performance.endpoint;
            if (!run || !run.result || !endpoint || endpoint.indexOf('/api/strategy-center/qmt-results/performance?') !== 0) return Promise.resolve();
            return request(endpoint).then(function(value) { if(current(seq)){state.performance=value;draw();} }).catch(function(e) { if(current(seq)){state.performanceError=e.message || '请求失败';draw();} });
        }
        function schedulePendingRefresh(seq) {
            if(!current(seq) || model(state.data,state.date).readable)return;
            var pending=preparationJobs(state.data,state.date).some(function(j){return validPreparation(j) && (j.status==='QUEUED' || j.status==='PREPARING');});
            var run=state.data.latest;if(!pending && !(run && run.trade_date===state.date && !run.result && run.status==='AWAITING_EXECUTION'))return;
            session.pollTimer=setTimeout(function(){session.pollTimer=null;if(current(seq))return refresh();},15000);
        }
        function refresh(runUid) {
            if(session.pollTimer!==null){clearTimeout(session.pollTimer);session.pollTimer=null;}
            var seq=++session.seq; state.busy=true;state.error='';state.performance=null;state.performanceError='';draw();
            var url=runUid ? '/api/strategy-center/qmt-results/runs/'+encodeURIComponent(runUid) : '/api/strategy-center/qmt-results?trade_date='+encodeURIComponent(state.date);
            return request(url).then(function(value) {
                if (!current(seq)) return;
                if (runUid) state.data=Object.assign({},state.data,{latest:value}); else state.data=value;
                state.busy=false;draw();return future(state.data.latest,seq);
            }).catch(function(e) { if(current(seq)){state.busy=false;state.error=e.message || '请求失败';draw();} return {loadError:e.message || '请求失败'}; }).then(function(value){schedulePendingRefresh(seq);return value;});
        }
        function changeDate(next) {
            if (!/^\d{4}-\d{2}-\d{2}$/.test(next)) return;
            state.date=next;state.data={};state.query='';state.filter='';if(options.date)options.date(next);return refresh();
        }
        container.onclick=function(e) {
            var target=e.target.closest('[data-qr],[data-qr-stock],[data-qr-date]');if(!target)return;
            if(target.dataset.qr==='refresh')refresh();
            if(target.dataset.qrDate)changeDate(target.dataset.qrDate);
            if(target.dataset.qrStock && options.stock)options.stock(target.dataset.qrStock);
        };
        container.onchange=function(e) { if(e.target.dataset.qr==='date')changeDate(e.target.value);if(e.target.dataset.qr==='run')refresh(e.target.value);if(e.target.dataset.qr==='filter'){state.filter=e.target.value;draw();} };
        container.oninput=function(e) { if(e.target.dataset.qr==='search'){state.query=e.target.value;draw();} };
        return refresh();
    }
    var api={load:load,stop:stop,render:render,model:model,number:number,validRow:validRow,performanceHTML:performanceHTML};
    root.QmtStrategyResults=api;
    if (typeof module!=='undefined' && module.exports) module.exports=api;
})(typeof window!=='undefined' ? window : globalThis);
