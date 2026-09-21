/* Daily acquisition monitor. All dates/statuses come from the read-only API. */
(function () {
    'use strict';
    var mounted = null;
    var LABELS = {full: '已完整', partial: '部分／异常', missing: '整日缺失', closed: '非交易日', pending: '未到期', unknown: '待核实'};
    var TASKS = {running: '运行中', success: '执行成功', failed: '执行失败', timeout: '已超时', stopped: '已停止', degraded: '部分完成', unknown: '运行状态待核实'};
    function esc(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) { return {'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c]; }); }
    function today() { return new Intl.DateTimeFormat('en-CA', {timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit'}).format(new Date()); }
    function shift(day, count) { var d = new Date(day + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + count); return d.toISOString().slice(0, 10); }
    function fmt(n) { return n == null ? '—' : Number(n).toLocaleString('zh-CN'); }
    function short(day) { return String(day || '').slice(5); }
    function mark(cell) { return '<span class="dm-dot dm-' + cell.status + (cell.task_state === 'running' ? ' dm-running' : '') + '" aria-hidden="true">' + (cell.status === 'unknown' ? '?' : cell.status === 'missing' ? '−' : '') + '</span>'; }
    function pill(status) { return '<span class="dm-status">' + mark({status: status}) + esc(LABELS[status] || status) + '</span>'; }
    function summarize(rows, dates) {
        var s = {due: 0, complete: 0, gaps: 0, unknown: 0, earliest: '', continuous: ''}, continuous = true;
        dates.forEach(function (d, i) {
            var states = rows.map(function (r) { return r.days[i].status; });
            if (!states.length || d.trade_status === 0 || states.every(function (v) { return v === 'pending'; })) return;
            if (d.trade_status !== 1) { s.unknown++; continuous = false; return; }
            s.due++;
            if (states.every(function (v) { return v === 'full'; })) { s.complete++; if (continuous) s.continuous = d.date; }
            else continuous = false;
            if (states.some(function (v) { return v === 'missing' || v === 'partial'; })) { s.gaps++; s.earliest = s.earliest || d.date; }
            if (states.indexOf('unknown') !== -1) s.unknown++;
        });
        return s;
    }
    function mount(container) {
        if (mounted) mounted.dispose();
        var state = {start: shift(today(), -29), end: today(), range: '30', group: 'all', only: false, selected: null, data: null, detail: null};
        var alive = true, busy = false, detailSeq = 0, loadSeq = 0, timer, controllers = new Set();
        container.innerHTML = '<section class="data-monitor" aria-label="数据获取监控">' +
            '<div class="dm-heading"><div><h2>数据获取监控</h2><p>逐日查看数据完整度与历史补数进展</p></div><div class="dm-freshness" data-dm="freshness">正在读取交易日历…</div></div>' +
            '<div class="dm-toolbar"><div class="dm-ranges" aria-label="日期范围"><button data-range="30" aria-pressed="true">近30天</button><button data-range="90">近90天</button><button data-range="year">今年</button></div>' +
            '<form class="dm-dates"><label>起始<input type="date" name="start" value="' + state.start + '" required></label><span>至</span><label>结束<input type="date" name="end" value="' + state.end + '" required></label><button type="submit">查询</button></form>' +
            '<button data-action="refresh">刷新状态</button></div>' +
            '<div class="dm-errors" data-dm="errors" role="status" aria-live="polite"></div>' +
            '<div class="dm-summary" data-dm="summary"></div>' +
            '<section class="dm-panel"><div class="dm-panel-head"><div><strong>每日完整度</strong><span class="dm-scan" data-dm="scan"></span></div><div class="dm-filters"><label>类型 <select data-dm="group"><option value="all">全部数据</option><option value="market">行情数据</option><option value="flow">资金数据</option><option value="event">事件与热度</option></select></label><label><input type="checkbox" data-dm="only">仅看异常行</label></div></div>' +
            '<div class="dm-grid-scroll" data-dm="grid"><div class="dm-message">正在读取真实数据状态…</div></div>' +
            '<div class="dm-legend">' + ['full','partial','missing','closed','pending','unknown'].map(function (s) { return '<span>' + mark({status:s}) + LABELS[s] + '</span>'; }).join('') + '<span>' + mark({status:'partial',task_state:'running'}) + '补数中</span></div></section>' +
            '<div class="dm-bottom"><section class="dm-panel dm-detail" data-dm="detail" aria-label="所选日期详情" aria-live="polite"><div class="dm-message">点击色点查看当日详情</div></section><section class="dm-panel dm-task-panel" data-dm="tasks" aria-label="采集与补数执行记录"></section></div>' +
            '<div class="dm-footer"><span>非交易日不计入完整率 · 全部时间为北京时间</span><span>状态每30秒刷新；校验结果最长保留30分钟，可逐日复检</span></div>' +
            '<details class="dm-task-settings"><summary>采集任务管理</summary><div data-dm="settings"></div></details></section>';
        var root = container.querySelector('.data-monitor');
        function el(name) { return root.querySelector('[data-dm="' + name + '"]'); }
        function visible() { return alive && container.isConnected && container.classList.contains('active') && !document.hidden; }
        function api(path) {
            var control = new AbortController(); controllers.add(control);
            var timeout = setTimeout(function () { control.abort(); }, 45000);
            return fetch(path, {signal: control.signal, cache: 'no-store'}).then(function (r) {
                if (!r.ok) return r.json().catch(function () { return {}; }).then(function (v) { throw new Error(typeof v.detail === 'string' ? v.detail : '请求失败（' + r.status + '）'); });
                return r.json();
            }).finally(function () { clearTimeout(timeout); controllers.delete(control); });
        }
        function rows() { return state.data.datasets.filter(function (r) { return (state.group === 'all' || r.group === state.group) && (!state.only || r.days.some(function (c) { return ['partial','missing','unknown'].indexOf(c.status) !== -1; })); }); }
        function render() {
            if (!state.data || !alive) return;
            var data = state.data, list = rows(), summary = summarize(list, data.dates);
            var relevantTypes = list.map(function (r) { return r.task_type; });
            var running = data.tasks.filter(function (t) { return t.status === 'running' && (state.group === 'all' || relevantTypes.indexOf(t.task_type) !== -1); });
            el('freshness').textContent = '状态读取：' + data.generated_at + ' · 北京时间';
            el('scan').textContent = data.scan.pending_days ? ' · ' + data.scan.checked_days + ' 天已检查，' + data.scan.pending_days + ' 天排队核验' : ' · 本范围检查已返回';
            el('summary').innerHTML = '<div class="dm-metric"><div>全部完整</div><strong>' + summary.complete + '<small> / ' + summary.due + ' 天</small></strong><span>' + (summary.continuous ? '从范围起点连续完整至 ' + summary.continuous : '范围起点尚未连续完整') + '</span></div>' +
                '<div class="dm-metric"><div>仍有缺口</div><strong>' + summary.gaps + '<small> 天 · ' + summary.unknown + ' 天待核实</small></strong><span>' + (summary.earliest ? '最早缺口 · ' + summary.earliest : '当前范围暂无已确认缺口') + '</span></div>' +
                '<div class="dm-metric"><div>正在采集／补数</div><strong>' + running.length + '<small> 个任务</small></strong><span>' + (data.errors.some(function (e) { return e.indexOf('任务') !== -1; }) ? '任务状态读取失败，数量暂不可确认' : running.length ? esc(running[0].name) : '暂无已确认运行中的任务') + '</span></div>';
            var html = '<table class="dm-matrix"><thead><tr><th scope="col">数据类型</th>';
            data.dates.forEach(function (d, i) { var month = i === 0 || d.date.slice(5,7) !== data.dates[i-1].date.slice(5,7); html += '<th scope="col" class="' + (d.trade_status === 0 ? 'dm-rest ' : '') + (d.date === today() ? 'dm-today ' : '') + (month ? 'dm-month' : '') + '"><span>' + (month ? d.date.slice(5,7) + '月' : '') + '</span>' + d.date.slice(8) + '</th>'; });
            html += '<th scope="col">完整日</th></tr></thead><tbody>';
            list.forEach(function (r) {
                html += '<tr><th scope="row"><strong>' + esc(r.name) + '</strong><small>' + esc(r.scope) + '</small></th>';
                r.days.forEach(function (c) {
                    var selected = state.selected && state.selected.dataset === r.key && state.selected.day === c.trade_date;
                    var title = c.trade_date + ' · ' + r.name + ' · ' + LABELS[c.status] + '\n' + c.reason;
                    if (c.expected_count != null) title += '\n有效 ' + fmt(c.actual_count) + ' / 应有 ' + fmt(c.expected_count);
                    if (c.checked_at) title += '\n校验于 ' + c.checked_at;
                    html += '<td class="' + (c.status === 'closed' ? 'dm-rest' : '') + '"><button class="dm-cell" data-dataset="' + r.key + '" data-day="' + c.trade_date + '" aria-label="' + esc(title) + '" title="' + esc(title) + '" aria-pressed="' + !!selected + '">' + mark(c) + '</button></td>';
                });
                var total = r.days.filter(function (c) { return c.status !== 'closed' && c.status !== 'pending'; }).length;
                html += '<td class="dm-row-total">' + r.days.filter(function (c) { return c.status === 'full'; }).length + ' / ' + total + '</td></tr>';
            });
            if (!list.length) html += '<tr><td colspan="' + (data.dates.length+2) + '" class="dm-message">当前筛选没有匹配的数据行</td></tr>';
            html += '</tbody></table>';
            var scroll = el('grid').scrollLeft;
            el('grid').innerHTML = html; el('grid').scrollLeft = scroll;
            el('tasks').innerHTML = '<div class="dm-panel-title">最近采集与补数任务</div>' + (data.tasks.length ? data.tasks.slice(0,6).map(function (t) {
                return '<div class="dm-task"><div><strong>' + esc(t.name) + '</strong><span class="dm-task-state">' + esc(TASKS[t.status] || t.status) + '</span></div><small>目标日期：' + (t.target_dates.length ? esc(t.target_dates.join('、')) : '未提供可核验日期') + '</small><small>开始 ' + esc(t.started_at) + (t.finished_at ? ' · 结束 ' + esc(t.finished_at) : '') + '</small><small>' + esc(t.progress_note) + '</small></div>';
            }).join('') : '<div class="dm-message">' + (data.errors.length ? '暂无可读取的执行记录' : '此范围没有采集或补数执行记录') + '</div>');
            if (!state.selected && list.length) {
                var target = null;
                list.some(function (r) { for (var i=r.days.length-1;i>=0;i--) { if (r.days[i].status === 'partial' || r.days[i].status === 'missing') { target = {dataset:r.key,day:r.days[i].trade_date}; return true; } } return false; });
                state.selected = target || {dataset:list[0].key,day:data.dates[data.dates.length-1].date};
            }
        }
        function detailHTML(d) {
            var html = '<div class="dm-detail-heading"><strong>' + esc(d.trade_date) + ' · ' + esc(d.definition.name) + '</strong>' + pill(d.status) + '</div><p class="dm-reason">' + esc(d.reason) + '</p>';
            html += '<div class="dm-detail-counts"><div><span>应有 / ' + esc(d.unit) + '</span><strong>' + fmt(d.expected_count) + '</strong></div><div><span>有效入库</span><strong>' + fmt(d.actual_count) + '</strong></div><div><span>仍缺少</span><strong>' + fmt(d.missing_count) + '</strong></div></div>';
            if (d.coverage_ratio != null) html += '<div class="dm-track" role="progressbar" aria-label="数据完整率" aria-valuenow="' + Math.round(d.coverage_ratio*100) + '" aria-valuemin="0" aria-valuemax="100"><div style="width:' + Math.max(0,Math.min(100,d.coverage_ratio*100)) + '%"></div></div><div class="dm-progress-label">完整率 ' + (d.coverage_ratio*100).toFixed(2) + '% · 异常记录 ' + fmt(d.invalid_count) + '</div>';
            html += '<dl class="dm-evidence"><dt>实际记录</dt><dd>' + fmt(d.observed_count) + ' 条</dd><dt>完成时限</dt><dd>' + esc(d.due_at || '—') + '</dd><dt>数据更新</dt><dd>' + esc(d.data_updated_at || '未取得') + '</dd><dt>最近校验</dt><dd>' + esc(d.checked_at || '尚未完成') + (d.stale ? ' · 已过期' : '') + '</dd><dt>数据来源</dt><dd>' + esc(d.source || d.definition.scope) + '</dd><dt>数据表</dt><dd>' + esc(d.definition.table) + '</dd></dl>';
            if (d.check_state === 'checking') html += '<div class="dm-note">正在排队或检查；完成后自动更新。</div>';
            if (d.missing && d.missing.length) {
                html += '<div class="dm-panel-title">缺口与异常 <small>显示 ' + d.missing.length + ' / ' + fmt(d.missing_total) + ' 项</small></div><div class="dm-missing-scroll"><table class="dm-gap-table"><thead><tr><th>股票／指数代码</th><th>缺失数量</th><th>时段／原因</th></tr></thead><tbody>';
                d.missing.forEach(function (m) { html += '<tr><td>' + esc(m.stock_code) + '</td><td>' + fmt(m.missing_count) + '</td><td>' + esc((m.missing_times || []).join('、') || m.reason) + '</td></tr>'; });
                html += '</tbody></table></div>';
            }
            if (d.gaps && d.gaps.length) html += '<div class="dm-note">补数队列：' + d.gaps.map(function (g) { return esc(g.status === 'RETRYING' ? '已排队，等待执行确认' : g.status) + ' · ' + esc(g.reason || '历史缺口'); }).join('；') + '</div>';
            html += '<div class="dm-detail-actions"><button data-action="recheck"' + (d.check_state === 'checking' || d.status === 'closed' || d.status === 'pending' ? ' disabled' : '') + '>重新检查当日</button><button data-action="export"' + (!d.missing_total || d.status === 'unknown' ? ' disabled' : '') + '>导出完整缺口</button></div>';
            return html;
        }
        function loadDetail(force) {
            if (!state.selected) return Promise.resolve();
            var seq = ++detailSeq, pick = state.selected;
            if (!state.detail || state.detail.dataset !== pick.dataset || state.detail.trade_date !== pick.day) el('detail').innerHTML = '<div class="dm-message">正在读取 ' + esc(pick.day) + ' 的检查详情…</div>';
            return api('/api/datasource/monitor/detail?dataset=' + encodeURIComponent(pick.dataset) + '&trade_date=' + pick.day + (force ? '&recheck=true' : '')).then(function (d) {
                if (!alive || seq !== detailSeq) return;
                state.detail = d; el('detail').innerHTML = detailHTML(d);
            }).catch(function (e) { if (alive && seq === detailSeq) el('detail').innerHTML = '<div class="dm-message dm-error">详情读取失败：' + esc(e.message) + '</div>'; });
        }
        function load() {
            if (!alive || busy) return Promise.resolve();
            busy = true;
            var seq = ++loadSeq, requested = state.start + '/' + state.end;
            return api('/api/datasource/monitor?start_date=' + state.start + '&end_date=' + state.end).then(function (d) {
                if (!alive || seq !== loadSeq || requested !== state.start + '/' + state.end) return;
                root.classList.remove('dm-disconnected'); state.data = d; el('errors').textContent = d.errors.join('；'); render(); return loadDetail(false);
            }).catch(function (e) { if (alive && seq === loadSeq) { root.classList.add('dm-disconnected'); el('errors').textContent = '状态读取失败：' + e.message + '。当前画面为上次结果，请刷新。'; el('freshness').textContent = '连接异常 · 尚未取得最新状态'; } }).finally(function () { if (seq === loadSeq) busy = false; });
        }
        function range(start, end, mode) {
            if (!start || !end || end < start || (new Date(end)-new Date(start))/86400000 >= 366) { el('errors').textContent = '请选择 1 至 366 个自然日。'; return; }
            state.start = start; state.end = end; state.range = mode; state.selected = null; state.detail = null;
            root.querySelector('[name="start"]').value = start; root.querySelector('[name="end"]').value = end;
            root.querySelectorAll('[data-range]').forEach(function (b) { b.setAttribute('aria-pressed', String(b.dataset.range === mode)); });
            // Cancel old range requests; stale responses may not replace this range.
            loadSeq++; detailSeq++; controllers.forEach(function (c) { c.abort(); }); busy = false; load();
        }
        root.addEventListener('click', function (event) {
            var cell = event.target.closest('[data-dataset]');
            if (cell) { state.selected = {dataset:cell.dataset.dataset,day:cell.dataset.day}; render(); loadDetail(false); return; }
            var button = event.target.closest('button'); if (!button) return;
            if (button.dataset.range) { var t = today(), mode=button.dataset.range; range(mode === 'year' ? t.slice(0,4)+'-01-01' : shift(t,1-Number(mode)),t,mode); }
            if (button.dataset.action === 'refresh') load();
            if (button.dataset.action === 'recheck') loadDetail(true).then(load);
            if (button.dataset.action === 'export' && state.selected) {
                button.disabled = true;
                var exported = {dataset:state.selected.dataset,day:state.selected.day};
                fetch('/api/datasource/monitor/gaps.csv?dataset='+encodeURIComponent(exported.dataset)+'&trade_date='+exported.day).then(function (r) {
                    if (!r.ok) return r.json().then(function (e) { throw new Error(e.detail || '导出失败'); });
                    return r.blob();
                }).then(function (blob) { var url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download='data-gaps-'+exported.dataset+'-'+exported.day+'.csv';a.click();setTimeout(function(){URL.revokeObjectURL(url);},1000); }).catch(function(e){el('errors').textContent=e.message;}).finally(function(){button.disabled=false;});
            }
        });
        root.querySelector('.dm-dates').addEventListener('submit',function(e){e.preventDefault();range(root.querySelector('[name="start"]').value,root.querySelector('[name="end"]').value,'custom');});
        el('group').addEventListener('change',function(e){state.group=e.target.value;render();});
        el('only').addEventListener('change',function(e){state.only=e.target.checked;render();});
        root.querySelector('details').addEventListener('toggle',function(e){
            if (!e.target.open || el('settings').dataset.loaded) return;
            el('settings').textContent='正在读取采集任务…';
            api('/api/datasource/list').then(function(d){
                el('settings').innerHTML=(d.data||[]).map(function(p){return '<h3>'+esc(p.provider)+'</h3>'+Object.keys(p.types).map(function(k){return p.types[k].map(function(t){return '<div class="dm-setting-row"><span>'+esc(t.task_name)+'</span><span>'+esc(TASKS[t.last_run_status]||t.last_run_status||'待运行')+'</span><button onclick="dsViewLog('+Number(t.id)+')">日志</button><button onclick="dsRunTask('+Number(t.id)+')">执行</button><button onclick="dsToggleTask('+Number(t.id)+')">'+(t.enabled===1?'停用':'启用')+'</button></div>';}).join('');}).join('');}).join('');el('settings').dataset.loaded='true';
            }).catch(function(e){el('settings').textContent='任务读取失败：'+e.message;});
        });
        function resume() { if (visible()) load(); }
        document.addEventListener('visibilitychange',resume);
        timer=setInterval(resume,30000);
        mounted={dispose:function(){alive=false;clearInterval(timer);controllers.forEach(function(c){c.abort();});document.removeEventListener('visibilitychange',resume);}};
        return load();
    }
    window.ProBigADataMonitor={mount:mount};
    if (typeof module !== 'undefined' && module.exports) module.exports={summarize:summarize,esc:esc,shift:shift};
})();
