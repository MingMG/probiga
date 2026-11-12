(function (root) {
    'use strict';

    const states = {
        UPTREND: ['上升趋势', 'positive'],
        TOP_CANDIDATE: ['高位震荡 / 筑顶候选', 'watch'],
        DOWNTREND: ['下跌趋势', 'negative'],
        ACCELERATING_DECLINE: ['加速下跌', 'negative'],
        STABILIZATION_ATTEMPT: ['止跌尝试', 'watch'],
        BOTTOMING_CANDIDATE: ['磨底候选', 'watch'],
        TECHNICAL_REBOUND: ['技术性反弹', 'watch'],
        REVERSAL_CANDIDATE: ['反转候选', 'watch'],
        REVERSAL_CONFIRMED: ['反转确认', 'positive']
    };
    const periods = [['daily', '日线'], ['weekly', '周线'], ['monthly', '月线']];
    const horizons = [5, 10, 20];
    const reversalBreadth = ['advancing_pct', 'above_ma20_pct', 'above_ma60_pct',
        'new_low_20d_pct', 'new_low_60d_pct', 'above_ma20_change_5d_pp'];
    const activeLoads = new WeakMap();

    function escape(value) {
        return String(value ?? '').replace(/[&<>"']/g, char => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        })[char]);
    }

    function stateChip(state) {
        const [name, tone] = Object.hasOwn(states, state) ? states[state] : ['证据不足', ''];
        return `<span class="ms-stage ${tone}">${name}</span>`;
    }

    function format(value, digits, suffix = '') {
        return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) + suffix : '—';
    }

    function yesNo(value) {
        return value == null ? '—' : value ? '是' : '否';
    }

    function list(items) {
        return Array.isArray(items) && items.length
            ? '<ul class="ms-list">' + items.map(item => `<li>${escape(item)}</li>`).join('') + '</ul>'
            : '<p class="ms-note">暂无可展示证据</p>';
    }

    function meta(entries) {
        return '<div class="ms-meta">' + entries.map(([label, value]) => `<span>${label} ${escape(value ?? '—')}</span>`).join('') + '</div>';
    }

    function warning(message) {
        return `<p class="ms-warning" role="status">${escape(message)}</p>`;
    }

    function renderPeriods(snapshot) {
        return `<section class="ms-panel"><h3>多周期状态</h3><div class="ms-grid">${periods.map(([key, name]) => {
            const item = snapshot.periods?.[key] || {};
            const features = item.features || {};
            const metrics = [['收盘点位', features.close], ['趋势均线', features.trend_ma], ['支撑', features.support], ['阻力', features.resistance]];
            const notes = Array.isArray(item.evidence) && item.evidence.length
                ? `<details><summary>判断依据</summary>${list(item.evidence)}<dl>` +
                    metrics.map(([label, value]) => `<dt>${label}</dt><dd>${format(value, 2)}</dd>`).join('') + '</dl></details>' : '';
            return `<article class="ms-card"><h4>${name}</h4><strong>${stateChip(item.state)}</strong>` +
                `${meta([['最近已完成周期', item.end_date], ['样本期数', item.observation_count ?? '—']])}${notes}</article>`;
        }).join('')}</div></section>`;
    }

    function hasReversalBreadth(snapshot) {
        const breadth = snapshot?.breadth;
        return breadth?.quality_status === 'VERIFIED' && typeof breadth.available_at === 'string' &&
            typeof snapshot.trade_date === 'string' && breadth.trade_date === snapshot.trade_date &&
            reversalBreadth.every(key => Number.isFinite(breadth[key]));
    }

    function renderBreadth(snapshot) {
        const breadth = snapshot.breadth;
        const metrics = [
            ['上涨股票占比', breadth?.advancing_pct, '%'],
            ['站上20日均线', breadth?.above_ma20_pct, '%'],
            ['站上60日均线', breadth?.above_ma60_pct, '%'],
            ['20日新低占比', breadth?.new_low_20d_pct, '%'],
            ['60日新低占比', breadth?.new_low_60d_pct, '%'],
            ['跌停股票占比', breadth?.limit_down_pct, '%'],
            ['20日线上占比近5日变化', breadth?.above_ma20_change_5d_pp, ' 个百分点']
        ];
        const confirmation = hasReversalBreadth(snapshot)
            ? '' : warning('所选交易日缺少反转确认所需的完整市场广度证据，反转确认不可核验。');
        return '<section class="ms-panel"><h3>市场广度与风险</h3>' + (breadth
            ? `<div class="ms-grid">${metrics.map(([label, value, suffix]) => `<article class="ms-card"><h4>${label}</h4><strong>${format(value, 1, suffix)}</strong></article>`).join('')}</div>`
            : '') + confirmation + '</section>';
    }

    function renderCurrent(data, requestedDate) {
        const snapshot = data?.snapshot;
        const mode = data?.evidence_mode === 'CURRENT_OBSERVATION' ? '当前观测' :
            data?.evidence_mode === 'VERIFIED_PUBLICATION' ? (snapshot ? '已核验发布时点' : '历史时点核验') : null;
        if (!snapshot) return '<section class="ms-panel">' + warning(data?.reason || '所选交易日暂无可核验的市场阶段数据。') +
            meta([['请求交易日', requestedDate || data?.trade_date], ['数据来源', data?.source], ['证据口径', mode], ['模型版本', data?.model_version]]) + '</section>';
        const sections = [
            ['判断依据', snapshot.evidence], ['进一步确认条件', snapshot.confirmation_conditions],
            ['信号失效条件', snapshot.invalidation_conditions], ['风险情景', snapshot.risk_scenarios],
            ['不确定性', snapshot.uncertainties]
        ];
        return '<section class="ms-panel"><h3>综合判断</h3><strong>' + stateChip(snapshot.state) + '</strong>' +
            meta([['数据交易日', snapshot.trade_date || data.trade_date], ['证据截止', snapshot.as_of],
                ['数据来源', data.source], ['证据口径', mode], ['模型版本', snapshot.model_version || data.model_version]]) +
            (snapshot.candidate_signals ? meta([['技术性反弹信号', yesNo(snapshot.candidate_signals.technical_rebound)],
                ['日线反转候选信号', yesNo(snapshot.candidate_signals.reversal)]]) : '') +
            (requestedDate && snapshot.trade_date && requestedDate !== snapshot.trade_date
                ? warning(`请求日期为 ${requestedDate}，下方展示 ${snapshot.trade_date} 的数据。`) : '') +
            (data.status === 'READY' && snapshot.status === 'READY' ? '' : warning(data.reason || '部分市场证据尚未就绪。')) +
            '</section>' + renderPeriods(snapshot) + renderBreadth(snapshot) +
            `<section class="ms-panel"><h3>证据与边界</h3><div class="ms-grid">${sections.map(([label, items]) =>
                `<article class="ms-card"><h4>${label}</h4>${list(items)}</article>`).join('')}</div></section>`;
    }

    function table(label, columns, rows) {
        return `<div class="ms-table"><table aria-label="${label}"><thead><tr>` +
            columns.map(column => `<th scope="col">${column}</th>`).join('') + '</tr></thead><tbody>' + rows.join('') + '</tbody></table></div>';
    }

    function cells(values) {
        return '<tr>' + values.map(value => `<td>${value}</td>`).join('') + '</tr>';
    }

    function observedCount(count, total) {
        return total > 0 ? `${format(count, 0)} / ${format(total, 0)}` : '—';
    }

    function renderReplay(data, horizon) {
        const replay = data?.replay;
        if (!replay) return warning(data?.reason || '暂无可核验的市场阶段回放。');
        const groups = replay.summary?.[horizon];
        const rows = Array.isArray(replay.rows) ? replay.rows.slice().reverse() : [];
        const breadthMissing = rows.some(({assessment}) => assessment?.status === 'READY' && !hasReversalBreadth(assessment));
        return meta([['判断起日', replay.start_date], ['判断止日', replay.end_date], ['结果观察截止', replay.outcome_as_of],
            ['数据来源', data.source], ['模型版本', data.model_version]]) +
            `<p class="ms-note">历史回放使用当时可见的输入。未来${horizon}个交易日的数据仅用于评价，未走满观察期的结果显示为待观察。</p>` +
            (breadthMissing ? warning('回放包含市场广度证据不全的判断日，后续反转确认只统计证据完整的观察期。') : '') +
            (Array.isArray(groups) && groups.length ? table('按市场阶段汇总', ['市场阶段', '判断样本', '完整观察期', '平均指数收益', '再次创新低', '后续反转确认', '平均局部低点偏移（交易日）'],
                groups.map(group => cells([stateChip(group.state), format(group.assessment_count, 0), format(group.matured_count, 0),
                    format(group.mean_return_pct, 2, '%'), observedCount(group.new_low_count, group.new_low_evaluated_count),
                    observedCount(group.reversal_confirmation_count, group.transition_evaluated_count), format(group.mean_sessions_to_window_low, 1)]))) : '') +
            (rows.length ? table('历史市场状态及后续结果', ['判断交易日', '当时状态与证据', `后续${horizon}日指数收益`, '最大上涨', '最大下跌', '最大回撤', '再次创新低', '突破当时阻力'],
                rows.map(row => {
                    const assessment = row.assessment;
                    const outcome = row.outcomes?.[horizon];
                    const evidence = assessment ? `<details><summary>当时判断依据</summary>${list(assessment.evidence)}` +
                        `<div class="ms-meta">${periods.map(([key, name]) => `<span>${name} ${stateChip(assessment.periods?.[key]?.state)}</span>`).join('')}</div>` +
                        `<p class="ms-note">判断截止 ${escape(row.decision_known_at || assessment.as_of || '—')}</p></details>` : '';
                    const result = outcome ? [format(outcome.return_pct, 2, '%'), format(outcome.max_upside_pct, 2, '%'),
                        format(outcome.max_downside_pct, 2, '%'), format(outcome.max_drawdown_pct, 2, '%'),
                        yesNo(outcome.new_20_session_low), yesNo(outcome.resistance_broken)]
                        : ['待观察或证据不足', '—', '—', '—', '—', '—'];
                    return cells([`<time>${escape(row.trade_date)}</time>`, stateChip(assessment?.state) + evidence, ...result]);
                })) : warning(data.reason || '回放区间暂无有效观察。'));
    }

    function load(tradeDate, container, request, onLatest) {
        const query = tradeDate ? '?trade_date=' + encodeURIComponent(tradeDate) : '';
        const ticket = {};
        activeLoads.set(container, ticket);
        container.innerHTML = '<div class="market-stage">' +
            '<section class="ms-panel ms-heading"><div><h2>大盘走势阶段</h2><p class="ms-note">对照日、周、月线的止跌、反弹与反转证据</p></div>' +
            '<button type="button" data-ms-latest>最近可用收盘</button></section>' +
            '<div class="ms-current" data-ms-current aria-live="polite"><div class="loading">正在读取市场阶段…</div></div>' +
            '<section class="ms-panel ms-replay"><h3>历史市场状态回放</h3><div class="ms-controls">' +
            '<button type="button" data-ms-replay>读取历史回放</button><label>观察期 <select data-ms-horizon>' +
            horizons.map(days => `<option value="${days}" ${days === 20 ? 'selected' : ''}>${days}个交易日</option>`).join('') +
            '</select></label></div><div data-ms-history class="ms-replay-view" aria-live="polite"></div></section></div>';
        const current = container.querySelector('[data-ms-current]');
        const history = container.querySelector('[data-ms-history]');
        const button = container.querySelector('[data-ms-replay]');
        const horizon = container.querySelector('[data-ms-horizon]');
        const isActive = () => activeLoads.get(container) === ticket && container.contains(current);
        let replay;
        container.querySelector('[data-ms-latest]').addEventListener('click', () => {
            onLatest?.();
            return load('', container, request, onLatest);
        });
        horizon.addEventListener('change', () => {
            if (isActive() && replay) history.innerHTML = renderReplay(replay, Number(horizon.value));
        });
        button.addEventListener('click', async () => {
            button.disabled = true;
            history.innerHTML = '<div class="loading">正在计算历史回放…</div>';
            try {
                replay = await request('/api/market-stage/replay' + query);
                if (isActive()) history.innerHTML = renderReplay(replay, Number(horizon.value));
            } catch (error) {
                replay = null;
                if (isActive()) history.innerHTML = warning(error?.isTimeout ? '市场阶段回放请求超时。' : '无法读取市场阶段回放。');
            } finally {
                button.disabled = false;
            }
        });
        return request('/api/market-stage/current' + query).then(data => {
            if (isActive()) current.innerHTML = renderCurrent(data, tradeDate);
        }).catch(error => {
            const message = error?.isTimeout ? '市场阶段请求超时。' : '无法读取市场阶段。';
            if (isActive()) current.innerHTML = warning(message);
            return {loadError: message};
        });
    }

    const api = {load, renderCurrent, renderReplay};
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
    else root.MarketStage = api;
})(typeof window === 'undefined' ? globalThis : window);
