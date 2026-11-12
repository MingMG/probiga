/* Read-only market dashboard. Every observation keeps its source and date. */
(function (root) {
    'use strict';

    const indices = [
        ['000001.SH', '上证指数'], ['399001.SZ', '深证成指'], ['399006.SZ', '创业板指'],
        ['000680.SH', '科创综指'], ['000688.SH', '科创50'], ['000852.SH', '中证1000'], ['000300.SH', '沪深300']
    ];
    const rankings = [
        ['gainers', '涨幅榜', 'change_pct', '%'], ['losers', '跌幅榜', 'change_pct', '%'],
        ['amount', '成交额榜', 'amount', '亿'], ['turnover', '活跃换手', 'turnover_rate', '%']
    ];
    const heatModes = [
        ['change', '行情'], ['net_inflow', '资金净流入'], ['five_day_return', '5日涨幅榜']
    ];
    let active = null;

    function escape(value) {
        return String(value ?? '').replace(/[&<>"']/g, char => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        })[char]);
    }

    function number(value) {
        if (typeof value !== 'number' && (typeof value !== 'string' || !value.trim())) return null;
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    }

    function day(value) {
        const text = typeof value === 'string' ? value.slice(0, 10) : '';
        return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : '';
    }

    function observationLag(market, clock, requested) {
        if (market?.is_realtime !== true || market.trade_date !== requested || market.timezone !== 'Asia/Shanghai') return '';
        function timestamp(value) {
            if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(value)) return NaN;
            const iso = value.replace(' ', 'T');
            return Date.parse(/(?:Z|[+-]\d{2}:\d{2})$/.test(iso) ? iso : iso + '+08:00');
        }
        if (day(market.data_time) !== requested || day(clock?.server_time) !== requested) return '';
        const seconds = Math.floor((timestamp(clock.server_time) - timestamp(market.data_time)) / 1000);
        return Number.isFinite(seconds) && seconds >= 0 ? ` · 延迟 ${seconds < 60 ? seconds + '秒' : Math.floor(seconds / 60) + '分钟'}` : '';
    }

    function rows(value) { return Array.isArray(value) ? value : []; }

    function metric(value, suffix = '', digits = 2) {
        const parsed = number(value);
        return parsed === null ? '—' : parsed.toLocaleString('zh-CN', {minimumFractionDigits: digits, maximumFractionDigits: digits}) + suffix;
    }

    function amount(value) {
        const parsed = number(value);
        return parsed === null || parsed < 0 ? '—' : metric(parsed / 100000000, '亿');
    }

    function tone(value) {
        const n = number(value);
        return n === null || n === 0 ? 'mh-neutral' : n > 0 ? 'mh-up' : 'mh-down';
    }

    function signed(value, unit = '%') {
        const n = number(value);
        return `<span class="${tone(n)}">${n !== null && n > 0 ? '+' : ''}${metric(n, unit)}</span>`;
    }

    function link(tab, text) {
        return `<button type="button" class="mh-link" data-mh-tab="${tab}">${text}</button>`;
    }

    function stock(code, name) {
        return /^\d{6}$/.test(code || '')
            ? `<button type="button" class="mh-link mh-stock" data-mh-stock="${code}">${escape(name || code)}</button>`
            : escape(name || code || '—');
    }

    function panel(title, meta, body, span = 'mh-quarter') {
        return `<section class="mh-panel ${span}"><header class="mh-panel-head"><h3>${title}</h3><span class="mh-meta">${meta}</span></header>${body}</section>`;
    }

    function empty(text) { return `<p class="mh-empty">${escape(text)}</p>`; }

    function tile(name, value, foot, extra = '') {
        return `<article class="mh-tile ${extra}"><span class="mh-tile-name">${name}</span><strong class="mh-tile-value">${value}</strong><small class="mh-meta">${foot}</small></article>`;
    }

    function marketObservation(payload, requested) {
        const date = day(payload?.trade_date);
        if (!date || !requested || date > requested || payload.error || payload.status === 'unavailable') return null;
        const counts = [payload.up_count, payload.flat_count, payload.down_count, payload.total_count].map(number);
        const [up, flat, down, total] = counts;
        const valid = counts.every(count => Number.isInteger(count) && count >= 0) && total > 0 && up + flat + down === total;
        return {...payload, trade_date: date, breadth: valid ? {up, flat, down, total} : null};
    }

    function snapshotObservation(payload, requested) {
        return requested && day(payload?.trade_date) === requested && ['available', 'partial'].includes(payload.status) ? payload : null;
    }

    function freshestIndex(code, requested, market, trend, snapshot) {
        const identity = code.split('.')[0];
        const sources = rows(snapshot?.indices).filter(item => String(item.index_code || '').split('.')[0] === identity);
        if (identity === '000852' && market?.csi1000) sources.push({
            price: market.csi1000.price, change_pct: market.csi1000.change,
            trade_date: market.csi1000.trade_date, data_time: market.csi1000.data_time,
            source: market.csi1000.data_source
        });
        for (const item of rows(trend?.indices)) {
            if (item.index_code !== identity || item.source_status === 'missing') continue;
            sources.push({price: item.periods?.daily?.metrics?.close, trade_date: item.data_cutoff,
                source: trend.source?.table || '日线行情', data_time: item.data_cutoff});
        }
        return sources.filter(item => day(item.trade_date) && day(item.trade_date) <= requested && number(item.price) > 0)
            .sort((a, b) => indexStamp(b).localeCompare(indexStamp(a)))[0];
    }

    function indexStamp(row) { return String(row.as_of || row.data_time || row.trade_date).replace('T', ' '); }

    function renderIndexStrip(requested, market, trend, snapshot) {
        return '<div class="mh-tape">' + indices.map(([code, name]) => {
            const row = freshestIndex(code, requested, market, trend, snapshot);
            const foot = row
                ? `<span>${code}</span><span title="${escape(indexStamp(row) + ' · ' + (row.source || '来源待核验'))}">${escape(day(row.trade_date))}${day(row.trade_date) < requested ? ' · 历史' : ''}</span>`
                : `<span>${code}</span><span>行情待核验</span>`;
            return tile(name, `<span class="mh-tile-change">${signed(row?.change_pct)}</span><span class="${tone(row?.change_pct)}">${metric(row?.price)}</span>`, foot, 'mh-index-tile');
        }).join('') + '</div>';
    }

    function observationMeta(market) {
        return market ? `${escape(market.trade_date)} · ${escape(market.data_source || '来源待核验')}` : '证据待核验';
    }

    function snapshotMeta(snapshot) {
        if (!snapshot) return '证据待核验';
        const kind = {qmt_snapshot: '盘中样本', exchange_close: '收盘样本'}[snapshot.as_of_basis];
        return `${kind ? kind + ' · ' : ''}${escape(snapshot.as_of || snapshot.trade_date)} · ${escape(snapshot.source || '来源待核验')}`;
    }

    function pair(a, b, af = '', bf = '', digits = 0) {
        return `<span class="mh-up">${metric(a, af, digits)}</span><span class="mh-muted"> / </span><span class="mh-down">${metric(b, bf, digits)}</span>`;
    }

    function renderMetrics(market, snapshot) {
        const breadth = market?.breadth;
        const strong = snapshot?.strong || {};
        const limits = snapshot?.limit_ladder;
        const limitMeta = limits ? [limits.scope, limits.source].filter(Boolean).map(escape).join(' · ') : '涨跌停证据待核验';
        const count = breadth ? `<span class="mh-up">${metric(breadth.up, '', 0)}</span><span class="mh-muted"> / ${metric(breadth.flat, '', 0)} / </span><span class="mh-down">${metric(breadth.down, '', 0)}</span>` : '—';
        return '<div class="mh-metrics">' +
            tile('个股涨 / 平 / 跌', count, breadth ? `上涨占比 ${metric(100 * breadth.up / breadth.total, '%', 1)} · ${observationMeta(market)}` : '涨跌家数待核验') +
            tile('强势 / 弱势', pair(strong.up_3pct, strong.down_3pct), number(strong.up_3pct) !== null || number(strong.down_3pct) !== null ? `涨跌幅达到 ±3% · ${snapshotMeta(snapshot)}` : '涨跌幅分布待核验') +
            tile('涨停 / 跌停', pair(limits?.limit_up_count, limits?.limit_down_count), limitMeta) +
            tile('最高连板', metric(limits?.max_streak, '板', 0), limits ? limitMeta : '连板证据待核验') +
            tile('成交额', amount(market?.total_amount), observationMeta(market)) +
            tile('换手 / 量比', '— / —', '缺少统一市场换手与量比口径') + '</div>';
    }

    function renderBreadth(market, snapshot) {
        const b = market?.breadth;
        const observed = rows(snapshot?.distribution).filter(item => Number.isInteger(item.count) && item.count >= 0 && typeof item.label === 'string').map(item => ({
            ...item, direction: item.key === 'flat' ? '' : typeof item.upper === 'number' && item.upper <= 0 ? 'down' : typeof item.lower === 'number' && item.lower >= 0 ? 'up' : ''
        }));
        if (!b && !observed.length) return panel('涨跌分布 / 广度', '样本待核验', empty('暂无可用涨跌分布'));
        const bins = observed.length ? observed : [{label: '下跌', count: b.down, direction: 'down'}, {label: '持平', count: b.flat}, {label: '上涨', count: b.up, direction: 'up'}];
        const max = Math.max(1, ...bins.map(item => item.count));
        const color = {up: 'mh-up', down: 'mh-down'};
        const plot = '<div class="mh-spark" aria-label="涨跌样本分布">' + bins.map(item =>
            `<div><strong>${metric(item.count, '', 0)}</strong><i class="${color[item.direction] || 'mh-neutral'}" style="--mh-height:${100 * item.count / max}%"></i><span>${escape(item.label)}</span></div>`).join('') + '</div>';
        const marketMeta = b ? `${metric(b.total, '只', 0)} · ${observationMeta(market)}` : '';
        const sampleMeta = observed.length ? `分布样本 ${metric(snapshot.sample_size, '只', 0)} · ${snapshotMeta(snapshot)}` : '';
        const bar = b ? '<div class="mh-breadth-bar" aria-hidden="true">' + [[b.down, 'mh-down'], [b.flat, 'mh-neutral'], [b.up, 'mh-up']].map(([count, cls]) =>
            `<span class="mh-breadth-part ${cls}" style="width:${100 * count / b.total}%"></span>`).join('') + '</div>' +
            `<div class="mh-meta">跌 ${metric(b.down, '', 0)} · 平 ${metric(b.flat, '', 0)} · 涨 ${metric(b.up, '', 0)}${sampleMeta ? ' · ' + marketMeta : ''}</div>` : '';
        return panel('涨跌分布 / 广度', sampleMeta || marketMeta, plot + bar);
    }

    function stat(label, value) { return `<div><span class="mh-meta">${label}</span><strong>${value}</strong></div>`; }

    function renderEmotion(market) {
        const b = market?.breadth;
        const percentile = number(market?.heat_percentile);
        if (!b) return panel('情绪观察', '广度证据', empty('市场情绪观察待核验'));
        return panel('情绪观察', observationMeta(market),
            `<div class="mh-section-label">${escape(market.heat_status || '涨跌广度')}</div><div class="mh-stat-grid">` +
            stat('上涨占比', metric(100 * b.up / b.total, '%', 1)) +
            stat('历史广度分位', percentile !== null ? 'P' + metric(percentile, '', 0) : '—') +
            stat('较可比交易日', signed(market.breadth_change_pp, '个百分点')) + '</div>' +
            `<p class="mh-meta">${escape(market.heat_percentile_method || '分位使用已发布的可比历史样本')} · ${metric(market.heat_percentile_sample_count, '日', 0)}</p>`);
    }

    function renderTrend(trend, market, snapshot, requested) {
        const index = rows(trend?.indices).find(item => item.index_code === '000300' && day(item.data_cutoff) && day(item.data_cutoff) <= requested);
        const measures = index?.periods?.daily?.metrics;
        const title = index ? `沪深300 · ${escape(index.data_cutoff)}` : '指数证据待核验';
        return '<div class="mh-stack mh-quarter">' +
            panel('趋势强度', title, measures ? '<div class="mh-stat-grid">' +
                stat('指数点位', metric(measures.close)) + stat('SMA20', metric(measures.sma20)) + stat('SMA60', metric(measures.sma60)) +
                stat('SMA20近5根斜率', signed(measures.sma20_slope_5_pct)) + '</div>' : empty('暂无可核验的指数日线趋势')) +
            panel('实用监控', observationMeta(market), '<div class="mh-stat-grid">' +
                stat('涨停开板样本', metric(snapshot?.limit_ladder?.opened_limit_up_count, '只', 0)) +
                stat('小盘代码样本', metric(market?.board_sample?.count, '只', 0)) +
                stat('小盘样本平均涨跌', signed(market?.board_sample?.change)) + '</div>') + '</div>';
    }

    function renderLadder(snapshot) {
        const notes = snapshot?.limit_ladder || {};
        const exact = day(notes.trade_date) && day(notes.trade_date) === day(snapshot?.trade_date) && notes.source;
        const ladder = exact
            ? rows(notes.rows).filter(item => Number.isInteger(item.streak) && item.streak > 0 && Number.isInteger(item.count) && item.count > 0)
            : [];
        const max = Math.max(1, ...ladder.map(item => item.count));
        const meta = exact ? [notes.trade_date, notes.scope, notes.source].filter(Boolean).map(escape).join(' · ') : snapshotMeta(snapshot);
        const captured = exact && notes.available_at ? `证据可用 ${escape(notes.available_at)} · ` : '';
        return panel('涨停梯队', meta, (ladder.length ? '<div class="mh-list">' + ladder.map(item => {
            const names = rows(item.stocks).filter(name => typeof name === 'string' && name.trim());
            const tier = `<b>${item.streak}板</b><span class="mh-breadth-bar" aria-hidden="true"><span class="mh-breadth-part mh-up" style="width:${100 * item.count / max}%"></span></span><span>${item.count}只</span>`;
            return names.length ? `<details class="mh-ladder-tier"><summary class="mh-row">${tier}<span class="mh-ladder-toggle" aria-hidden="true"></span></summary><ul aria-label="${item.streak}板个股">${names.map(name => `<li>${escape(name)}</li>`).join('')}</ul></details>` :
                `<div class="mh-row">${tier}</div>`;
        }).join('') + '</div>' :
            empty(exact && notes.count === 0 ? '东财收录样本为 0 只' : '暂无可核验的连续涨停梯队')) + `<p class="mh-meta">${captured}封板率 ${metric(ladder.length ? notes.seal_rate_pct : null, '%', 1)} · ${link('intraday-alert', '查看盘中播报')}</p>`);
    }

    function heatValue(value, mode) {
        const sign = value > 0 ? '+' : '';
        if (mode !== 'net_inflow') return sign + metric(value, '%');
        const magnitude = Math.abs(value);
        if (magnitude >= 100000000) return sign + metric(value / 100000000, '亿');
        if (magnitude >= 10000) return sign + metric(value / 10000, '万');
        return sign + metric(value, '元');
    }

    function heatSeries(values, label) {
        const samples = rows(values);
        if (samples.length < 2 || samples.length > 60) return '';
        const series = samples.map(number);
        if (series.some(value => value === null)) return '';
        const min = Math.min(...series);
        const span = Math.max(...series) - min;
        if (!Number.isFinite(span)) return '';
        const points = series.map((value, i) =>
            `${(i * 100 / (series.length - 1)).toFixed(2)},${(span ? 19 - 18 * (value - min) / span : 10).toFixed(2)}`
        ).join(' ');
        const caption = typeof label === 'string' && label.trim() ? `<small>${escape(label)}</small>` : '';
        return `<div class="mh-heat-trend">${caption}<svg class="mh-heat-series" viewBox="0 0 100 20" preserveAspectRatio="none" aria-hidden="true" focusable="false"><polyline points="${points}"></polyline></svg></div>`;
    }

    function renderSector(kind, title, payload, requested, mode) {
        const label = heatModes.find(([key]) => key === mode)[1];
        const exact = requested && payload?.requested_date === requested && payload.trade_date === requested;
        const metricResult = exact ? payload[kind]?.[mode] : null;
        const sample = metricResult?.status === 'available'
            ? rows(metricResult.rows).filter(item => typeof item?.name === 'string' && item.name.trim() && number(item.value) !== null).slice(0, 24)
            : [];
        const max = Math.max(0, ...sample.map(item => Math.abs(number(item.value))));
        const controls = `<div class="mh-heat-modes" role="group" aria-label="${title}指标">` + heatModes.map(([key, text]) =>
            `<button type="button" aria-pressed="${mode === key}" aria-controls="mh-heatmap-${kind}" data-mh-heat-mode="${kind}:${key}">${text}</button>`
        ).join('') + '</div>';
        const plot = sample.length ? `<ul class="mh-heat-grid" aria-label="${title}${label}热力图">` + sample.map(item => {
            const value = number(item.value) || 0;
            const fill = max && value !== 0 ? Math.round(12 + 26 * (Math.abs(value) / max)) : 0;
            const source = escape(item.source || '来源待核验');
            return `<li class="mh-heat-cell ${tone(value)}" style="--mh-heat-fill:${fill}%"><span class="mh-heat-name">${escape(item.name)}</span>` +
                `<strong>${heatValue(value, mode)}</strong>${heatSeries(item.series, item.series_label)}<small class="mh-heat-source">${source}</small></li>`;
        }).join('') + '</ul>' : empty(payload === undefined && requested ? '正在加载板块热力图…' : `所选交易日暂无可核验的${label}数据`);
        const note = metricResult?.note ? `<p class="mh-meta mh-heat-note">${escape(metricResult.note)}</p>` : '';
        return panel(title, escape(exact ? requested : '证据待核验'), controls +
            `<div id="mh-heatmap-${kind}">${plot}${note}</div>` +
            link(kind === 'concept' ? 'concept' : 'sector', '查看板块详情'), 'mh-third');
    }

    function renderEvents(snapshot, requested) {
        const events = rows(snapshot?.events).filter(item => day(item.snapshot_at) === requested && requested);
        const heading = escape(requested || '日期待核验');
        const timeline = day(requested) ? `<a class="mh-link" href="/static/market-event-timeline.html?date=${day(requested)}">查看当日事件时间线</a>` : '';
        if (!events.length) return panel('监控中心', heading, empty('所选交易日的异动事件证据待核验') + timeline + link('market-radar', '查看异动雷达'), 'mh-third');
        const limitLabels = {TOUCH:'触及涨停', SEAL:'涨停封板', RESEAL:'涨停回封', BREAK:'涨停开板'};
        return panel('监控中心', heading, '<div class="mh-list">' + events.slice(0, 8).map(item => {
            const detail = item.detail_json || {};
            const name = item.stock_code ? stock(item.stock_code, detail.short_name) : escape(item.sector_name || '板块异动');
            const limitEvent = item.event_family === 'LIMIT_UP_LIFECYCLE' && Object.hasOwn(limitLabels, item.event_type);
            const label = limitEvent ? limitLabels[item.event_type] : item.direction === 'UP' ? '上涨异动' : item.direction === 'DOWN' ? '下跌异动' : '市场异动';
            const tone = limitEvent ? (item.event_type === 'BREAK' ? 'mh-down' : 'mh-up') : item.direction === 'UP' ? 'mh-up' : item.direction === 'DOWN' ? 'mh-down' : 'mh-neutral';
            const provider = limitEvent ? '国金 QMT 涨停生命周期' : item.data_source || '来源待核验';
            return `<div class="mh-row"><span>${name}<small class="mh-meta">${escape(provider)}</small></span>` +
                `<span class="mh-badge ${tone}">${label}</span>` +
                `<span class="mh-meta">${escape(item.snapshot_at)}${item.available_at ? `<small class="mh-meta">记录 ${escape(item.available_at)}</small>` : ''}</span></div>`;
        }).join('') + '</div>' + timeline, 'mh-third');
    }

    function renderRanking(snapshot, key, title, field, unit) {
        const sourceRows = rows(snapshot?.rankings?.[key]);
        const data = sourceRows.filter(item => day(item.trade_date) === day(snapshot.trade_date) &&
            /^\d{6}$/.test(item.stock_code || '') && number(item[field]) !== null);
        const display = data.slice(0, 8);
        const noMatches = !sourceRows.length && ['gainers', 'losers'].includes(key) && number(snapshot?.sample_size) > 0 &&
            Array.isArray(snapshot?.unavailable_fields) && !snapshot.unavailable_fields.includes('rankings.' + key);
        const coverage = key === 'turnover' ? snapshot?.turnover_coverage : null;
        let scope = '';
        if (Number.isInteger(coverage?.verified_count) && Number.isInteger(coverage?.sample_size)) {
            scope = ` · 已核验 ${metric(coverage.verified_count, '', 0)}/${metric(coverage.sample_size, '只', 0)}`;
        } else if (key === 'amount' && Number.isInteger(snapshot?.price_sample_size)) {
            scope = ` · 价量样本 ${metric(snapshot.price_sample_size, '只', 0)}`;
        }
        const meta = display.length ? `TOP ${display.length}${scope} · ${snapshotMeta(snapshot)}` : noMatches ? snapshotMeta(snapshot) : '排名待核验';
        return panel(title, meta, display.length ?
            '<div class="mh-list">' + display.map((item, i) => {
                const source = key === 'turnover' ? item.turnover_rate_source || item.source : item.source;
                const value = field === 'change_pct' ? signed(item[field]) : unit === '亿' ? amount(item[field]) : metric(item[field], unit);
                return `<div class="mh-row"><span class="mh-muted">${i + 1}</span>` +
                    `<span>${stock(item.stock_code, item.short_name)}<small class="mh-meta">${escape(item.stock_code)}${source ? ' · ' + escape(source) : ''}</small></span>` +
                    `<span>${value}</span></div>`;
            }).join('') + '</div>' :
            empty(noMatches ? (key === 'gainers' ? '当日样本无上涨标的' : '当日样本无下跌标的') : '缺少所选交易日可核验的排名') + link('stock-list', '查看全市场股票'));
    }

    function render(state, requested, clock, modes = {}) {
        const market = marketObservation(state.market, requested);
        const snapshot = snapshotObservation(state.snapshot, requested);
        const labels = {realtime: '盘中快照', close: '收盘观察', paused: '午间快照', stale: '行情已过期'};
        const boardPhase = {qmt_snapshot: '盘中快照', exchange_close: '收盘观察'}[snapshot?.as_of_basis] || '数据待核验';
        const phase = !market ? boardPhase : market.trade_date < requested ? '历史回退' : labels[market.freshness_status] || '已存行情';
        const stamp = market?.data_time || market?.trade_date || snapshot?.as_of;
        const poolUrl = '/?tab=trading-v3-candidates' + (day(requested) ? '&amp;trade_date=' + day(requested) : '');
        const markup = '<section class="market-home" aria-label="市场看板"><header class="mh-head"><div><span class="mh-eyebrow">PROBIGA · A股</span>' +
            '<h2>市场看板</h2><span class="mh-badge">' + phase + '</span>' + link('market-context', '市场环境') +
            `<a class="mh-link" href="${poolUrl}">正式策略票池</a></div>` +
            `<div class="mh-head-actions"><span class="mh-meta">请求 ${escape(requested || '日期待核验')} · 行情 ${escape(stamp || '待核验')}${observationLag(market, clock, requested)}</span>` +
            '<button type="button" class="mh-retry" data-mh-refresh>重载</button></div></header>';
        return markup + renderIndexStrip(requested, market, state.trend, snapshot) + renderMetrics(market, snapshot) +
            '<div class="mh-grid">' + renderBreadth(market, snapshot) + renderEmotion(market) + renderTrend(state.trend, market, snapshot, requested) +
            renderLadder(snapshot) +
            renderSector('concept', '概念热度', state.heatmap, requested, modes.concept || 'change') +
            renderSector('industry', '行业热度', state.heatmap, requested, modes.industry || 'change') +
            renderEvents(snapshot, requested) + rankings.map(([key, title, field, unit]) => renderRanking(snapshot, key, title, field, unit)).join('') +
            '</div></section>';
    }

    function stop() {
        if (!active) return;
        clearTimeout(active.timer);
        active.container.removeEventListener('click', active.click);
        active = null;
    }

    function load(date, container, options = {}) {
        const requested = day(date) || day(options.clock?.()?.ui_trade_date);
        const retained = active?.container === container && active.requested === requested ? active.state : {};
        const modes = active?.container === container ? active.modes : {concept: 'change', industry: 'change'};
        stop();
        const session = {container, requested, state: retained, modes, timer: null, pending: null, click: null};
        active = session;
        const query = encodeURIComponent(requested);
        const paths = {market: '/api/monitor/data?date=' + query,
            trend: '/api/hot-data/market-trend?date=' + query,
            snapshot: '/api/monitor/board?date=' + query,
            heatmap: '/api/monitor/sector-heatmap?date=' + query};
        const current = () => active === session;

        function paint() {
            if (!current()) return;
            const focused = document.activeElement;
            const attributes = ['data-mh-refresh', 'data-mh-stock', 'data-mh-tab', 'data-mh-heat-mode'];
            const action = container.contains(focused) ? attributes.find(name => focused.hasAttribute(name)) : null;
            const value = action && focused.getAttribute(action);
            container.innerHTML = render(session.state, requested, options.clock?.(), session.modes);
            if (action) Array.from(container.querySelectorAll(`[${action}]`))
                .find(button => button.getAttribute(action) === value)?.focus({preventScroll: true});
        }

        function refresh(keys) {
            if (session.pending) return session.pending;
            if (!requested) return Promise.resolve({loadError: '交易日期待核验'});
            session.pending = Promise.all(keys.map(async key => {
                let value = null;
                try {
                    value = await options.request(paths[key], 15000, {cache: 'no-store'});
                } catch {
                    // Other data sources stay available when this request fails.
                }
                if (current()) {
                    session.state[key] = value;
                    paint();
                }
            })).then(() => {
                if (current() && !marketObservation(session.state.market, requested)?.breadth) return {loadError: '市场广度暂不可用'};
                return {};
            }).finally(() => { session.pending = null; });
            return session.pending;
        }

        function schedule() {
            if (!current()) return;
            session.timer = setTimeout(async () => {
                if (!current()) return;
                try {
                    if (document.hidden || options.isActive?.() === false) return;
                    const clock = options.refreshClock ? await options.refreshClock() : options.clock?.();
                    if (current() && clock?.is_intraday === true && day(clock.active_trade_date) === requested) {
                        await refresh(Object.keys(paths).filter(key => key !== 'trend'));
                    }
                } catch {
                    // A failed clock refresh must not relabel the previous observation.
                } finally { schedule(); }
            }, 60000);
        }

        session.click = event => {
            const button = event.target.closest('button');
            if (!button || !container.contains(button)) return;
            if (button.dataset.mhTab) options.navigate?.(button.dataset.mhTab);
            else if (button.dataset.mhStock) options.stock?.(button.dataset.mhStock);
            else if (button.dataset.mhHeatMode) {
                const [kind, mode] = button.dataset.mhHeatMode.split(':');
                if (Object.hasOwn(session.modes, kind) && heatModes.some(([key]) => mode === key)) {
                    session.modes[kind] = mode;
                    paint();
                }
            } else if (button.hasAttribute('data-mh-refresh') && !session.pending) {
                options.status?.('正在重载市场看板…');
                refresh(Object.keys(paths)).then(outcome => {
                    if (current()) options.status?.(outcome.loadError || '市场看板已重载', Boolean(outcome.loadError));
                });
            }
        };
        container.addEventListener('click', session.click);
        paint();
        return refresh(Object.keys(paths)).finally(schedule);
    }

    const api = {load, stop, render};
    root.MarketHome = api;
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window === 'undefined' ? globalThis : window);
