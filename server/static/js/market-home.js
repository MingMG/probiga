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
    const homeSections = [
        ['信息和数据看板', [
            ['review', '盘前盘后', '同一交易日的收盘复盘、下一交易日早报与假设验证。', 'review'],
            ['quant-workbench', '量化工作台', '查看量化能力、运行证据与任务进度。', 'delivery'],
            ['trading-day', '今日看盘', '按交易阶段查看市场状态、观察条件与候选。'],
            ['research-radar', '研报与题材雷达', '汇总研报、新闻催化、题材强度和研究对象。'],
            ['news', '财经信息看板', '按来源查看快讯和重点新闻。'],
            ['notice', 'A股公告', '按日期查询市场公告和个股披露。'],
            ['market-context', '市场环境', '查看全市场广度、指数趋势、轮动与假设。', 'quotes'],
            ['monitor', '市场监控中心', '成交、涨跌分布、板块表现和市场证据。', 'monitor'],
            ['market-stage', '大盘与指数结构', '对照日、周、月阶段及其验证条件。', 'indices'],
            ['macro-release', '宏观经济原表', '核对国家统计局经济运行发布，分别展示当月与累计指标。'],
            ['global-context', '外围与大类资产', '按采集批次核对海外指数、期货、商品、汇率和行业ETF。'],
            ['global-futures', '全球期货监测', '查看当前期货与CFD报价，对照30分钟、1小时及更长窗口的实际分钟价。'],
            ['market-radar', '盘中异动雷达', '跟踪个股与板块异动，进入明细核对。'],
            ['intraday-alert', '实时盯盘与播报', '查看盘中事件、阶段观察与已保存播报。'],
            ['broad-etf-flow', '宽基ETF资金', '对照成交额、公开份额变化和金额估算。'],
            ['sentiment', '涨跌停与市场情绪', '查看市场情绪、连板反馈与风格变化。', 'ladder'],
            ['sector', '行业与板块热度', '对照行业行情、资金及近五日表现。', 'sectors', 'heat'],
            ['industry-breadth', '行业均线宽度', '逐只核验20或30日均线，比较行业内站上均线的比例。'],
            ['concept', '概念与行业', '查看概念、成分和多日热度。', 'concepts'],
            ['commentary', '股评监控', '跟踪公开市场观点与相关标的。']
        ]],
        ['选股专题', [
            ['daily-pool', '每日研究票池', '按交易日查看已保存的研究候选与排除信息。', 'delivery'],
            ['recommended', 'AI研究建议', '查看事件和量价研究、入选依据及风险条件。'],
            ['screen', '条件选股', '按趋势、放量、连板或龙虎榜等条件缩小范围。'],
            ['strong', '强势股研究', '查看强势股票及对应交易日的排名。'],
            ['period-drawdowns', '区间高点回撤', '在完整行情窗口的成交额前100样本中研究高点回撤与TMT行业。'],
            ['fused', '个股热榜', '交叉查看多源个股热度与市场表现。'],
            ['portfolio', '自选股跟踪', '集中查看自选股票、行情、资金和异动。'],
            ['stock-list', '全市场股票', '查询股票并打开个股行情与研究资料。', 'quotes']
        ]],
        ['策略跟踪', [
            ['alist', '龙虎榜与公开席位', '从每日披露清单进入单股公开席位明细。'],
            ['seat-observation', '公开席位交易观察', '追踪榜单披露、后续股价分布和共同上榜，核对成熟样本。'],
            ['trading-v3-candidates', '策略选股结果', '查看正式候选、确认条件和阻断理由。'],
            ['trading-v3-hypotheses', '策略与假设跟踪', '查阅持续跟踪的市场或策略假设。'],
            ['strategy-center', '策略研究与竞技', '对照策略版本、研究表现和正式资格。'],
            ['strategy-backtest', '策略回测', '按已有登记版本查看或运行可复算的回测。'],
            ['qmt-strategy-results', 'QMT每日模拟', '查看每日模拟结果和执行记录。'],
            ['weekly-strategy', '周度策略', '用完整收盘周的证据回顾市场变化。']
        ]],
        ['资料与运行', [
            ['ai-stock', '个股研究问答', '围绕个股行情、公告和研究问题提问。'],
            ['ai-general', '通用研究问答', '梳理市场、资料与研究方法。'],
            ['datasource', '数据获取监控', '核对数据来源、完成记录和真实缺口。'],
            ['engineering-acceptance', '工程验收', '查看发布版本、运行状态及验收证据。']
        ]]
    ];
    const deliveryStates = {
        SUCCESS: '阶段完成', RUNNING: '运行中', FAILED: '失败', BLOCKED: '受阻',
        DEGRADED: '部分可用', TIMEOUT: '超时', STOPPED: '已停止', SUPERSEDED: '已被替代'
    };
    const dailyStages = {
        stock_daily: '股票日线', qmt_stock_daily_canonical: '股票日线', stock_finance: '财务数据',
        target_turnover_snapshot: '换手率快照', qmt_membership_snapshot: '行业与板块成员',
        capital_flow_batch_fast: '股票资金流', notice_eastmoney: '东方财富公告',
        qmt_announcement_pit: '公告历史证据', qmt_announcement_premarket_prepare: '盘前公告准备',
        stock_notice: '公告', stock_minute: '分钟行情', score: '策略评分', stock_score: '策略评分',
        publish: '结果发布', stock_pool: '股票池', analysis_premarket_external: '盘前外围',
        analysis_upper_evidence_prepare: '涨停证据准备', analysis_fast: '盘后分析',
        strategy_governance_daily: '策略验证'
    };
    const stagePages = {
        stock_daily: 'stock-list', qmt_stock_daily_canonical: 'stock-list', stock_finance: 'datasource',
        target_turnover_snapshot: 'monitor', qmt_membership_snapshot: 'concept',
        capital_flow_batch_fast: 'monitor', notice_eastmoney: 'notice', stock_notice: 'notice',
        qmt_announcement_pit: 'datasource', qmt_announcement_premarket_prepare: 'datasource',
        stock_minute: 'datasource', analysis_upper_evidence_prepare: 'sentiment',
        analysis_fast: 'recommended', strategy_governance_daily: 'trading-v3-candidates',
        score: 'recommended', stock_score: 'recommended', publish: 'daily-pool', stock_pool: 'daily-pool'
    };
    const boardFields = {
        limit_ladder: '连板梯队', events: '异动事件', indices: '指数行情', distribution: '涨跌分布',
        'rankings.amount': '成交额榜', 'rankings.turnover': '活跃换手',
        'liquidity.volume_ratio5': '收盘日量比',
        'limit_ladder.limit_down_count': '跌停数据', 'limit_ladder.opened_limit_up_count': '炸板数据'
    };
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

    function link(tab, text, view) {
        return `<button type="button" class="mh-link" data-mh-tab="${tab}"${view ? ` data-mh-view="${view}"` : ''}>${text}</button>`;
    }

    function stock(code, name) {
        return /^\d{6}$/.test(code || '')
            ? `<button type="button" class="mh-link mh-stock" data-mh-stock="${code}">${escape(name || code)}</button>`
            : escape(name || code || '—');
    }

    function panel(title, meta, body, span = 'mh-quarter') {
        return `<section class="mh-panel ${span}${String(meta).includes('mh-observation-expired') ? ' mh-panel-stale' : ''}" data-mh-key="panel:${escape(title)}"><header class="mh-panel-head"><h3>${title}</h3><span class="mh-meta">${meta}</span></header>${body}</section>`;
    }

    function empty(text) { return `<p class="mh-empty">${escape(text)}</p>`; }

    function tile(name, value, foot, extra = '') {
        return `<article class="mh-tile ${extra}${String(foot).includes('mh-observation-expired') ? ' mh-tile-stale' : ''}" data-mh-key="tile:${escape(name)}"><span class="mh-tile-name">${name}</span><strong class="mh-tile-value">${value}</strong><small class="mh-meta">${foot}</small></article>`;
    }

    function marketObservation(payload, requested, fallback) {
        const date = day(payload?.trade_date);
        if (date < requested && day(fallback?.trade_date) === requested) {
            const newer = marketObservation(fallback, requested);
            if (newer?.breadth) return newer;
        }
        if (!date || !requested || date > requested || payload.error || payload.status === 'unavailable') return fallback ? marketObservation(fallback, requested) : null;
        const counts = [payload.up_count, payload.flat_count, payload.down_count, payload.total_count].map(number);
        const [up, flat, down, total] = counts;
        const valid = counts.every(count => Number.isInteger(count) && count >= 0) && total > 0 && up + flat + down === total;
        if (!valid && fallback) return marketObservation(fallback, requested);
        return {...payload, trade_date: date, breadth: valid ? {up, flat, down, total} : null};
    }

    function snapshotObservation(payload, requested) {
        return requested && day(payload?.trade_date) === requested && ['available', 'partial', 'stale'].includes(payload.status) ? payload : null;
    }

    function observationState(meta, clock) {
        if (!meta) return {};
        const source = meta.source_at || meta.received_at;
        const parsed = Date.parse(source || '');
        const current = Date.parse(clock?.server_time || '');
        const age = Number.isFinite(parsed) && Number.isFinite(current)
            ? Math.max(0, Math.floor((current-parsed)/1000)) : meta.age_seconds;
        const local = new Date(current+8*3600000);
        const minute = local.getUTCHours()*60+local.getUTCMinutes();
        const lunch = Number.isFinite(current) && source?.slice(0,10) === local.toISOString().slice(0,10) && minute > 690 && minute < 780;
        const fixed = ['closed', 'reference'].includes(meta.state) || meta.state === 'paused' && lunch;
        const state = fixed ? meta.state : age > meta.expired_seconds ? 'expired' : age > meta.fresh_seconds ? 'delayed' : meta.last_error ? 'retained' : meta.state;
        return {state,age,source,fixed};
    }

    function observationBadge(meta, clock) {
        if (!meta) return '';
        const {state,age,source,fixed} = observationState(meta,clock);
        const names = {fresh:'新近观测',delayed:'数据延迟',expired:'已过期，保留参考',retained:'上次观测',paused:'午间暂停',closed:'收盘观测',reference:'历史参照'};
        const basis = meta.age_basis === 'received' ? '接收时间' : '数据时间';
        const text = (names[state] || '观测状态待核验') + (source ? ` · ${basis} ${source.replace('T',' ').slice(0,19)}` : '') +
            (!fixed && Number.isFinite(age) ? ` · ${age}秒前` : '') + (meta.age_basis === 'received' ? ' · 源端更新时间未提供' : '') +
            (meta.last_error ? ' · 本次采集失败' : '');
        return `<small class="mh-observation mh-observation-${escape(state)}">${escape(text)}</small>`;
    }

    function nodeKey(node) {
        return node.nodeType === 1 ? node.getAttribute('data-mh-key') || node.id || null : null;
    }

    function patchNode(current, next) {
        if (current === next || current.isEqualNode(next)) return;
        if (current.nodeType !== next.nodeType || current.nodeName !== next.nodeName) {
            current.replaceWith(next);
            return;
        }
        if (current.nodeType !== 1 && current.nodeType !== 11) {
            current.nodeValue = next.nodeValue;
            return;
        }
        if (current === document.activeElement && current.nodeName === 'SELECT') return;
        if (current.nodeType === 1) {
            for (const name of current.getAttributeNames()) if (!next.hasAttribute(name)) current.removeAttribute(name);
            for (const name of next.getAttributeNames()) if (current.getAttribute(name) !== next.getAttribute(name)) current.setAttribute(name,next.getAttribute(name));
        }
        const keyed = new Map(Array.from(current.childNodes).filter(nodeKey).map(node => [nodeKey(node),node]));
        let cursor = current.firstChild;
        for (const child of Array.from(next.childNodes)) {
            const key = nodeKey(child);
            let match = key ? keyed.get(key) : cursor && !nodeKey(cursor) && cursor.nodeName === child.nodeName ? cursor : null;
            if (!match) {
                match = child.cloneNode(true);
                current.insertBefore(match,cursor);
            } else if (match !== cursor) current.insertBefore(match,cursor);
            patchNode(match,child);
            cursor = match.nextSibling;
        }
        while (cursor) { const following = cursor.nextSibling; cursor.remove(); cursor = following; }
    }

    function monthShift(month, offset) {
        const [year, index] = month.split('-').map(Number);
        return new Date(Date.UTC(year, index - 1 + offset, 1)).toISOString().slice(0, 7);
    }

    function dashboardFacts(state, requested, market, snapshot) {
        const reviewDates = new Set(rows(state.reviewDates?.dates).map(day).filter(Boolean));
        const delivery = day(state.delivery?.trade_date) === requested ? state.delivery.data : null;
        const receipt = delivery?.receipt;
        const deliveryStatus = state.delivery?.status;
        const receiptMatches = receipt?.schema === 'probiga.daily-delivery.v1' &&
            day(receipt.trade_date) === requested && receipt.status === deliveryStatus;
        const deliveryLabel = receiptMatches && deliveryStatus === 'PASS' ? '交付已验收' :
            receiptMatches && deliveryStatus === 'DEGRADED' ? '交付部分可用' :
            deliveryStatus === 'BLOCKED' ? '交付受阻' :
            deliveryStatus === 'RUNNING' ? '交付运行中' : '有交付记录';
        const trend = rows(state.trend?.indices).map(row => day(row.data_cutoff)).filter(date => date && date <= requested).sort().at(-1);
        const heatmap = day(state.heatmap?.trade_date) === requested ? state.heatmap : null;
        const boardStamp = snapshot?.as_of || market?.data_time || market?.trade_date;
        const ladder = day(snapshot?.limit_ladder?.trade_date) === requested ? snapshot.limit_ladder : null;
        const missing = rows(snapshot?.unavailable_fields).map(field =>
            Object.hasOwn(boardFields, field) ? boardFields[field] : null).filter(Boolean);
        if (snapshot?.turnover_coverage?.status === 'partial' && !missing.includes('活跃换手')) missing.push('活跃换手');
        const facts = {
            review: reviewDates.has(requested) ? ['有复盘记录', requested] : null,
            delivery: delivery?.session ? [deliveryLabel, delivery.session.updated_at] : null,
            quotes: boardStamp ? [
                !snapshot && market?.trade_date !== requested ? '历史行情' :
                    market?.freshness_status === 'stale' ? '行情已过期' : '有行情记录',
                boardStamp
            ] : null,
            monitor: snapshot ? [missing.length ? '待补：' + missing.join('、') :
                snapshot.status === 'partial' ? '部分市场指标待核验' : '有市场数据', boardStamp] : null,
            indices: trend ? ['指数截止', trend] : null,
            ladder: ladder ? ['有连板记录', ladder.trade_date] : null,
            sectors: rows(heatmap?.industry?.change?.rows).length ? ['有行业记录', requested] : null,
            concepts: rows(heatmap?.concept?.change?.rows).length ? ['有概念记录', requested] : null
        };
        return {facts, reviewDates, delivery};
    }

    function renderCalendar(requested, clock, reviewDates, month, reviewDatesState) {
        if (!month || !requested) return empty('观察日期待核验');
        const [year, index] = month.split('-').map(Number);
        const firstWeekday = (new Date(Date.UTC(year, index - 1, 1)).getUTCDay() + 6) % 7;
        const days = new Date(Date.UTC(year, index, 0)).getUTCDate();
        const end = day(clock?.today) || requested;
        const cells = Array.from({length: firstWeekday}, () => '<span aria-hidden="true"></span>');
        for (let n = 1; n <= days; n++) {
            const date = month + '-' + String(n).padStart(2, '0');
            const hasReview = reviewDates.has(date);
            cells.push(`<button type="button" data-mh-date="${date}" aria-label="${date}${hasReview ? '，有复盘记录' : ''}" aria-pressed="${date === requested}"${date > end ? ' disabled' : ''}>${n}${hasReview ? '<i aria-hidden="true"></i>' : ''}</button>`);
        }
        const legend = reviewDatesState === undefined ? '正在读取复盘记录' :
            reviewDatesState === null || reviewDatesState.warnings?.length ? '复盘日期索引未完整返回 · 金点为已返回的复盘记录' :
            '金点为有复盘记录 · 选择日期查看当日看板';
        return `<div class="mh-calendar-head"><h3>研究日历</h3><div>
            <button type="button" class="mh-hub-button" data-mh-month="-1" aria-label="上个月"${month <= '1900-01' ? ' disabled' : ''}>‹</button>
            <time datetime="${month}">${year}年${index}月</time>
            <button type="button" class="mh-hub-button" data-mh-month="1" aria-label="下个月"${month >= end.slice(0, 7) ? ' disabled' : ''}>›</button>
            </div></div><div class="mh-calendar-grid">
            ${['一','二','三','四','五','六','日'].map(name => `<span>${name}</span>`).join('')}${cells.join('')}
            </div><p class="mh-hub-meta">${legend}</p>`;
    }

    function renderTimeline(requested, market, snapshot, reviewDates, delivery, filter) {
        const items = [];
        if (snapshot || market?.trade_date === requested) {
            const count = number(snapshot?.sample_size ?? market?.breadth?.total);
            items.push({kind:'行情', tab:'monitor', stamp:snapshot?.as_of || market?.data_time || requested,
                text: (snapshot?.as_of_basis === 'exchange_close' ? '收盘行情' : '行情观测') + (count === null ? '' : ` · ${metric(count, '只', 0)}样本`)});
        }
        if (reviewDates.has(requested)) items.push({kind:'复盘', tab:'review', stamp:requested, text:'可查看所选日期的复盘记录'});
        for (const attempt of rows(delivery?.attempts)) {
            const label = deliveryStates[attempt.status];
            const stamp = attempt.finished_at || attempt.started_at;
            if (!label || !day(stamp)) continue;
            const stage = Object.hasOwn(dailyStages, attempt.stage_name) ? dailyStages[attempt.stage_name] : '其他交付阶段';
            const tab = Object.hasOwn(stagePages, attempt.stage_name) ? stagePages[attempt.stage_name] : 'scheduler';
            items.push({kind:'交付', tab, stamp, text:`${stage} · ${label}`, reason:attempt.error_code});
        }
        const visible = items.filter(item => filter === '全部' || filter === item.kind)
            .sort((left, right) => String(right.stamp).replace('T', ' ').localeCompare(String(left.stamp).replace('T', ' '))).slice(0, 10);
        return `<div class="mh-calendar-head"><h3>更新时间流</h3>
            <label><span class="sr-only">筛选更新类型</span><select data-mh-feed>
            ${['全部', '行情', '复盘', '交付'].map(kind => `<option${filter === kind ? ' selected' : ''}>${kind}</option>`).join('')}
            </select></label></div>
            ${visible.length ? `<ol class="mh-update-feed">${visible.map(item => `<li><button type="button" data-mh-tab="${item.tab}"${item.reason ? ` title="${escape(item.reason)}"` : ''}>
                <time datetime="${escape(String(item.stamp).replace(' ', 'T'))}">${escape(String(item.stamp).length >= 16 ?
                    item.stamp.slice(day(item.stamp) === requested ? 11 : 5, 16).replace('T', ' ') : day(item.stamp))}</time><span>${item.kind}</span><strong>${escape(item.text)}</strong></button></li>`).join('')}</ol>` :
            empty('所选日期暂无此类可核验记录')}`;
    }

    function renderHub(state, requested, clock, modes, market, snapshot) {
        const {facts, reviewDates, delivery} = dashboardFacts(state, requested, market, snapshot);
        const month = modes.month || requested.slice(0, 7);
        const sections = homeSections.map(([title, cards]) => `<section class="mh-hub-section">
            <h3>${title}</h3><div class="mh-hub-cards">${cards.map(([tab, name, description, key, view]) => {
                const fact = facts[key];
                return `<article class="mh-hub-card">
                    <h4>${name}</h4><p>${description}</p>
                    <footer>${fact ? `<span class="mh-card-state"><strong>${escape(fact[0])}</strong><small>${escape(String(fact[1] || '').replace('T', ' ').slice(0, 16))}</small></span>` : ''}
                    <button type="button" class="mh-hub-button" data-mh-tab="${tab}"${view ? ` data-mh-view="${view}"` : ''} aria-label="打开${name}">打开 <span aria-hidden="true">↗</span></button></footer>
                    </article>`;
            }).join('')}</div></section>`).join('');
        return `<section class="mh-hub" aria-label="A股大看板">
            <header class="mh-hub-head"><div><h2>A股大看板</h2><p>观察日期 <time>${escape(requested || '待核验')}</time></p></div>
            <div><button type="button" class="mh-hub-button" data-mh-observation>行情概览 ↓</button>
            <button type="button" class="mh-hub-button" data-mh-refresh>检查更新</button></div></header>
            <div class="mh-hub-top"><div class="mh-hub-panel">${renderTimeline(requested, market, snapshot, reviewDates, delivery, modes.feed || '全部')}</div>
            <div class="mh-hub-panel">${renderCalendar(requested, clock, reviewDates, month, state.reviewDates)}</div></div>${sections}</section>`;
    }

    function freshestIndex(code, requested, market, trend, snapshot) {
        const identity = code.split('.')[0];
        const sources = rows(snapshot?.indices).filter(item => String(item.index_code || '').split('.')[0] === identity);
        for (const item of rows(trend?.indices)) {
            if (item.index_code !== identity || item.source_status === 'missing') continue;
            sources.push({price: item.periods?.daily?.metrics?.close, trade_date: item.data_cutoff,
                source: trend.source?.table || '日线行情', data_time: item.data_cutoff});
        }
        return sources.filter(item => day(item.trade_date) && day(item.trade_date) <= requested && number(item.price) > 0)
            .sort((a, b) => indexStamp(b).localeCompare(indexStamp(a)))[0];
    }

    function indexStamp(row) { return String(row.as_of || row.data_time || row.trade_date).replace('T', ' '); }

    function renderIndexStrip(requested, market, trend, snapshot, clock) {
        return '<div class="mh-tape">' + indices.map(([code, name]) => {
            const row = freshestIndex(code, requested, market, trend, snapshot);
            const foot = row
                ? `<span>${code}</span><span title="${escape(indexStamp(row) + ' · ' + (row.source || '来源待核验'))}">${escape(day(row.trade_date))}${row.evidence_mode === 'LIVE_OBSERVATION' ? ' ' + escape(indexStamp(row).slice(11, 19)) : ''}${row.retained_observation ? ' · 上次观测' : day(row.trade_date) < requested ? ' · 历史' : ''}</span>`
                : `<span>${code}</span><span>行情待核验</span>`;
            return tile(name, `<span class="mh-tile-change">${signed(row?.change_pct)}</span><span class="${tone(row?.change_pct)}">${metric(row?.price)}</span>`, foot+observationBadge(row?.observation,clock), 'mh-index-tile');
        }).join('') + '</div>';
    }

    function observationMeta(market, clock) {
        return market ? `${escape(market.trade_date)} · ${escape(market.data_source || '来源待核验')}${market.scope_label ? ' · ' + escape(market.scope_label) : ''}${observationBadge(market.observation,clock)}` : '证据待核验';
    }

    function snapshotMeta(snapshot, clock) {
        if (!snapshot) return '证据待核验';
        const kind = {qmt_snapshot: '盘中样本', exchange_close: '收盘样本', public_index_snapshot: '盘中指数', public_stock_ranking: '盘中排名'}[snapshot.as_of_basis];
        return `${kind ? kind + ' · ' : ''}${escape(snapshot.as_of || snapshot.trade_date)} · ${escape(snapshot.source || '来源待核验')}${observationBadge(snapshot.observation,clock)}`;
    }

    function pair(a, b, af = '', bf = '', digits = 0) {
        return `<span class="mh-up">${metric(a, af, digits)}</span><span class="mh-muted"> / </span><span class="mh-down">${metric(b, bf, digits)}</span>`;
    }

    function renderMetrics(market, snapshot, liveLiquidity, requested, clock) {
        const breadth = market?.breadth;
        const strong = snapshot?.strong || {};
        const limits = snapshot?.limit_ladder;
        const hasLiveLiquidity = snapshot?.as_of_basis !== 'exchange_close' && ['available','stale'].includes(liveLiquidity?.status) && liveLiquidity.trade_date === requested;
        const liquidity = hasLiveLiquidity ? liveLiquidity : snapshot?.liquidity;
        const ratio = hasLiveLiquidity ? liquidity.volume_ratio_median : liquidity?.volume_ratio5;
        const limitMeta = limits ? [limits.scope, limits.source].filter(Boolean).map(escape).join(' · ')+observationBadge(limits.observation,clock) : '涨跌停证据待核验';
        const count = breadth ? `<span class="mh-up">${metric(breadth.up, '', 0)}</span><span class="mh-muted"> / ${metric(breadth.flat, '', 0)} / </span><span class="mh-down">${metric(breadth.down, '', 0)}</span>` : '—';
        return '<div class="mh-metrics">' +
            tile('个股涨 / 平 / 跌', count, breadth ? `上涨占比 ${metric(100 * breadth.up / breadth.total, '%', 1)} · ${observationMeta(market,clock)}` : '涨跌家数待核验') +
            tile('强势 / 弱势', pair(strong.up_3pct, strong.down_3pct), number(strong.up_3pct) !== null || number(strong.down_3pct) !== null ? `涨跌幅达到 ±3% · ${snapshotMeta(snapshot,clock)}` : '涨跌幅分布待核验') +
            tile('涨停 / 跌停', pair(limits?.limit_up_count, limits?.limit_down_count), limitMeta) +
            tile('最高连板', metric(limits?.max_streak, '板', 0), limits ? limitMeta : '连板证据待核验') +
            tile('成交额', amount(market?.total_amount), observationMeta(market,clock)) +
            tile(hasLiveLiquidity ? '换手中位 / 盘中量比中位' : '换手中位 / 收盘日量比', metric(liquidity?.turnover_median_pct, '%') + ' / ' + metric(ratio, '倍'),
                liquidity ? `换手 ${metric(liquidity.turnover_sample_count, '只', 0)} · ${hasLiveLiquidity ? '量比' : '日量比同样本'} ${metric(liquidity.volume_sample_count, '只', 0)} · ` +
                    (hasLiveLiquidity ? `${escape(liquidity.definition)} · 数据时间 ${escape(liquidity.as_of)}${observationBadge(liquidity.observation,clock)}` : '较前5个完整交易日均量') : '换手和量比数据待核验') + '</div>';
    }

    function renderBreadth(market, snapshot, clock) {
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
        const marketMeta = b ? `${metric(b.total, '只', 0)} · ${observationMeta(market,clock)}` : '';
        const sampleMeta = observed.length ? `分布样本 ${metric(snapshot.sample_size, '只', 0)} · ${snapshotMeta(snapshot,clock)}` : '';
        const bar = b ? '<div class="mh-breadth-bar" aria-hidden="true">' + [[b.down, 'mh-down'], [b.flat, 'mh-neutral'], [b.up, 'mh-up']].map(([count, cls]) =>
            `<span class="mh-breadth-part ${cls}" style="width:${100 * count / b.total}%"></span>`).join('') + '</div>' +
            `<div class="mh-meta">跌 ${metric(b.down, '', 0)} · 平 ${metric(b.flat, '', 0)} · 涨 ${metric(b.up, '', 0)}${sampleMeta ? ' · ' + marketMeta : ''}</div>` : '';
        return panel('涨跌分布 / 广度', sampleMeta || marketMeta, plot + bar);
    }

    function stat(label, value) { return `<div><span class="mh-meta">${label}</span><strong>${value}</strong></div>`; }

    function renderEmotion(market, clock) {
        const b = market?.breadth;
        const percentile = number(market?.heat_percentile);
        if (!b) return panel('情绪观察', '广度证据', empty('市场情绪观察待核验'));
        return panel('情绪观察', observationMeta(market,clock),
            `<div class="mh-section-label">${escape(market.heat_status || '涨跌广度')}</div><div class="mh-stat-grid">` +
            stat('上涨占比', metric(100 * b.up / b.total, '%', 1)) +
            stat('历史广度分位', percentile !== null ? 'P' + metric(percentile, '', 0) : '—') +
            stat('较可比交易日', signed(market.breadth_change_pp, '个百分点')) + '</div>' +
            `<p class="mh-meta">${escape(market.heat_percentile_method || '分位使用已发布的可比历史样本')} · ${metric(market.heat_percentile_sample_count, '日', 0)}</p>`);
    }

    function renderTrend(trend, market, snapshot, requested, clock) {
        const index = rows(trend?.indices).find(item => item.index_code === '000300' && day(item.data_cutoff) && day(item.data_cutoff) <= requested);
        const measures = index?.periods?.daily?.metrics;
        const title = index ? `沪深300 · ${escape(index.data_cutoff)}` : '指数证据待核验';
        return '<div class="mh-stack mh-quarter">' +
            panel('趋势强度', title, measures ? '<div class="mh-stat-grid">' +
                stat('指数点位', metric(measures.close)) + stat('SMA20', metric(measures.sma20)) + stat('SMA60', metric(measures.sma60)) +
                stat('SMA20近5根斜率', signed(measures.sma20_slope_5_pct)) + '</div>' : empty('暂无可核验的指数日线趋势')) +
            panel('实用监控', observationMeta(market,clock), '<div class="mh-stat-grid">' +
                stat('涨停开板样本', metric(snapshot?.limit_ladder?.opened_limit_up_count, '只', 0)) +
                stat('小盘代码样本', metric(market?.board_sample?.count, '只', 0)) +
                stat('小盘样本平均涨跌', signed(market?.board_sample?.change)) + '</div>') + '</div>';
    }

    function renderLadder(snapshot, clock) {
        const notes = snapshot?.limit_ladder || {};
        const exact = day(notes.trade_date) && day(notes.trade_date) === day(snapshot?.trade_date) && notes.source;
        const ladder = exact
            ? rows(notes.rows).filter(item => Number.isInteger(item.streak) && item.streak > 0 && Number.isInteger(item.count) && item.count > 0)
            : [];
        const max = Math.max(1, ...ladder.map(item => item.count));
        const meta = exact ? [notes.trade_date, notes.scope, notes.source].filter(Boolean).map(escape).join(' · ')+observationBadge(notes.observation,clock) : snapshotMeta(snapshot,clock);
        const captured = exact && notes.available_at ? `证据可用 ${escape(notes.available_at)} · ` : '';
        return panel('涨停梯队', meta, (ladder.length ? '<div class="mh-list">' + ladder.slice(0, 8).map(item =>
            `<div class="mh-row"><b>${item.streak}板</b><span class="mh-breadth-bar" aria-hidden="true"><span class="mh-breadth-part mh-up" style="width:${100 * item.count / max}%"></span></span><span>${item.count}只</span></div>`).join('') + '</div>' :
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

    function renderSector(kind, title, payload, requested, mode, clock) {
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
            return `<li class="mh-heat-cell ${tone(value)}" style="--mh-heat-fill:${fill}%"><button type="button" class="mh-link mh-heat-name" data-mh-board="${kind}" data-mh-board-code="${escape(item.code)}">${escape(item.name)}</button>` +
                `<strong>${heatValue(value, mode)}</strong>${heatSeries(item.series, item.series_label)}<small class="mh-heat-source">${source}</small></li>`;
        }).join('') + '</ul>' : empty(payload === undefined && requested ? '正在加载板块热力图…' : `所选交易日暂无可核验的${label}数据`);
        const stamp = metricResult?.as_of ? ` · 数据时间 ${metricResult.as_of}` : '';
        const noteText = (metricResult?.retained_observation ? '上次观测 · ' : '') + (metricResult?.note || '');
        const note = noteText || stamp ? `<p class="mh-meta mh-heat-note">${escape(noteText + stamp)}</p>` : '';
        return panel(title, escape(exact ? requested : '证据待核验')+observationBadge(metricResult?.observation,clock), controls +
            `<div id="mh-heatmap-${kind}">${plot}${note}</div>` +
            (kind === 'concept' ? link('concept', '查看概念详情', mode === 'five_day_return' ? 'c5' : 'c0') :
                link('sector', '查看行业热度矩阵', 'heat')), 'mh-third');
    }

    function openBoardDetail(container, payload, tradeDate, kind, mode, code) {
        if (payload?.trade_date !== tradeDate || payload?.requested_date !== tradeDate) return;
        const item = rows(payload[kind]?.[mode]?.rows).find(row => String(row.code) === code);
        if (!item) return;
        const sameBoard = heatModes.flatMap(([metric, title]) => {
            const match = rows(payload[kind]?.[metric]?.rows).find(row => String(row.code) === code);
            if (!match || number(match.value) === null) return [];
            const observation = payload[kind][metric];
            return [`<div class="mh-row"><span>${title}<small class="mh-meta">${escape(match.source || '')}</small><small class="mh-meta">${escape(observation.as_of || '')}${observation.retained_observation ? ' · 上次观测' : ''}</small></span>
                <strong class="${tone(match.value)}">${heatValue(match.value, metric)}</strong></div>`];
        });
        const dialog = document.createElement('dialog');
        dialog.className = 'mh-board-detail market-home';
        dialog.innerHTML = `<header class="mh-panel-head"><div><h3 id="mh-board-title">${escape(item.name)}</h3>
            <p class="mh-meta">${kind === 'industry' ? '行业' : '概念'} · <time>${escape(tradeDate)}</time></p></div>
            <form method="dialog"><button class="mh-retry" autofocus>关闭</button></form></header>
            <div class="mh-list">${sameBoard.join('')}</div>${heatSeries(item.series, item.series_label)}`;
        dialog.setAttribute('aria-labelledby', 'mh-board-title');
        dialog.addEventListener('close', () => dialog.remove(), {once:true});
        container.appendChild(dialog);
        dialog.showModal();
        return dialog;
    }

    function eventIdentity(item) {
        return JSON.stringify([item.event_family,item.event_type,item.stock_code,item.sector_name,item.snapshot_at,item.available_at,item.detail_json]);
    }

    function renderEvents(snapshot, requested, clock, waiting) {
        const events = rows(snapshot?.events).filter(item => day(item.snapshot_at) === requested && requested);
        const heading = escape(requested || '日期待核验')+observationBadge(snapshot?.observation,clock);
        if (!events.length && !snapshot?.next_cursor) return panel('监控中心', heading,
            empty(snapshot?.requested_date === requested && Array.isArray(snapshot.events) ? '当日暂无已记录的监控事件' : '所选交易日的异动事件证据待核验') + link('market-radar', '查看异动雷达'), 'mh-third');
        const limitLabels = {TOUCH:'触及涨停', SEAL:'涨停封板', RESEAL:'涨停回封', BREAK:'涨停开板'};
        return panel('监控中心', heading, (waiting ? '<button type="button" class="mh-retry" data-mh-events-latest data-mh-key="events-latest">有新事件 · 回到最新</button>' : '') + '<div class="mh-list mh-event-scroll" data-mh-key="events-list" data-mh-events tabindex="0" role="region" aria-label="监控事件，向下滚动查看更早记录">' + events.map(item => {
            const detail = item.detail_json || {};
            const name = item.stock_code ? stock(item.stock_code, detail.short_name) : escape(item.sector_name || '板块异动');
            const limitEvent = item.event_family === 'LIMIT_UP_LIFECYCLE' && Object.hasOwn(limitLabels, item.event_type);
            const label = limitEvent ? limitLabels[item.event_type] : item.direction === 'UP' ? '上涨异动' : item.direction === 'DOWN' ? '下跌异动' : '市场异动';
            const tone = limitEvent ? (item.event_type === 'BREAK' ? 'mh-down' : 'mh-up') : item.direction === 'UP' ? 'mh-up' : item.direction === 'DOWN' ? 'mh-down' : 'mh-neutral';
            const provider = limitEvent ? '国金 QMT 涨停生命周期' : item.data_source || '来源待核验';
            return `<div class="mh-row" data-mh-key="event:${escape(eventIdentity(item))}"><span>${name}<small class="mh-meta">${escape(provider)}</small></span>` +
                `<span class="mh-badge ${tone}">${label}</span>` +
                `<span class="mh-meta">${escape(item.snapshot_at)}${item.available_at ? `<small class="mh-meta">记录 ${escape(item.available_at)}</small>` : ''}</span></div>`;
        }).join('') + (snapshot?.next_cursor ? '<button type="button" class="mh-retry" data-mh-events-more' +
            (snapshot.loading ? ' disabled' : '') + '>' + (snapshot.loading ? '正在加载…' : snapshot.error ? '加载失败，点击重试' : '加载更早事件') + '</button>' : '<p class="mh-meta">已显示当天全部事件</p>') + '</div>', 'mh-third');
    }

    function renderRanking(snapshot, key, title, field, unit, clock) {
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
        const observation = snapshot?.ranking_observations?.[key];
        const origin = observation && observation.trade_date === snapshot?.trade_date
            ? `${escape(observation.scope)} · ${escape(observation.as_of)} · ${escape(observation.source)}` : `${escape(snapshot?.as_of || snapshot?.trade_date || '')} · ${escape(snapshot?.source || '来源待核验')}`;
        const meta = (display.length ? `TOP ${display.length}${scope} · ${origin}` : noMatches ? origin : '排名待核验') +
            observationBadge(observation?.observation || snapshot?.observation,clock);
        return panel(title, meta, display.length ?
            '<div class="mh-list">' + display.map((item, i) => {
                const source = key === 'turnover' ? item.turnover_rate_source || item.source : item.source;
                const value = field === 'change_pct' ? signed(item[field]) : unit === '亿' ? amount(item[field]) : metric(item[field], unit);
                return `<div class="mh-row" data-mh-key="stock:${escape(item.stock_code)}"><span class="mh-muted">${i + 1}</span>` +
                    `<span>${stock(item.stock_code, item.short_name)}<small class="mh-meta">${escape(item.stock_code)}${source ? ' · ' + escape(source) : ''}</small></span>` +
                    `<span>${value}</span></div>`;
            }).join('') + '</div>' :
            empty(noMatches ? (key === 'gainers' ? '当日样本无上涨标的' : '当日样本无下跌标的') : '缺少所选交易日可核验的排名') + link('stock-list', '查看全市场股票'));
    }

    function renderDrawdowns(snapshot) {
        const data = snapshot?.close_drawdowns;
        const valid = snapshot?.as_of_basis === 'exchange_close' && data && number(data.sample_count) > 0;
        const heading = snapshot?.trade_date || '交易日待核验';
        if (!valid) return panel('大幅回撤', heading, empty('收盘后按已核验的当日最高价与收盘价统计'));
        const samples = `有效最高价 ${metric(data.sample_count, '只', 0)} / 行情样本 ${metric(data.expected_sample_count, '只', 0)}`;
        return panel('大幅回撤', heading, '<div class="mh-stat-grid">' +
            stat('日内高点回落超过7%', metric(data.count, '只', 0)) + '</div>' +
            `<p class="mh-meta">${escape(data.formula)} · ${escape(samples)}</p>` +
            (rows(data.rows).length ? '<div class="mh-list">' + rows(data.rows).map(row =>
                `<div class="mh-row"><span>${stock(row.stock_code, row.short_name)}<small class="mh-meta">最高 ${metric(row.high)} → 收盘 ${metric(row.price)}</small></span>` +
                `<span class="mh-down">-${metric(row.drawdown_pct, '%')}</span></div>`).join('') + '</div>' :
                '<p class="mh-meta">有效样本中没有超过阈值的股票</p>'));
    }

    function render(state, requested, clock, modes = {}) {
        const snapshot = snapshotObservation(state.snapshot, requested);
        const market = marketObservation(state.market, requested, snapshot?.market_overview);
        const failed = Object.keys(state.refreshErrors || {});
        const labels = {realtime: '盘中快照', close: '收盘观察', paused: '午间快照', stale: '行情已过期'};
        const boardPhase = {qmt_snapshot: '盘中快照', exchange_close: '收盘观察', public_index_snapshot: '盘中指数', public_stock_ranking: '盘中排名'}[snapshot?.as_of_basis] || '数据待核验';
        const phase = observationState(snapshot?.observation,clock).state === 'expired' ? '上次观测（已过期）' : !market ? boardPhase : market.trade_date < requested ? '历史回退' : labels[market.freshness_status] || '已存行情';
        const stamp = market?.data_time || market?.trade_date || snapshot?.as_of;
        const sourceNames = {stream:'快照读取', trend:'指数历史趋势', snapshot:'行情与榜单', heatmap:'板块热度', liquidity:'换手与量比', events:'监控事件', reviewDates:'复盘日历', delivery:'交付记录'};
        const transports = {connecting:'正在连接实时推送…',connected:'实时推送已连接 · 后台约每20秒更新，数据时间见各卡片',
            disconnected:'推送已断开，正在自动重连；保留已显示的观测',paused:'页面在后台，推送已暂停；后台采集继续运行',historical:'历史日期 · 显示已保存观测'};
        const refreshText = (transports[state.transport] || '正在同步市场观测…') + (failed.length
            ? ` · 更新失败：${failed.map(key => sourceNames[key] || key).join('、')}，保留原观测时间。` : '');
        const markup = '<section class="market-home" id="mh-market-observation" tabindex="-1" aria-label="行情概览"><header class="mh-head"><div>' +
            '<h2>行情概览</h2><span class="mh-badge">' + phase + '</span>' + link('market-context', '市场环境') + '</div>' +
            `<div class="mh-head-actions"><span class="mh-meta">请求 ${escape(requested || '日期待核验')} · 行情 ${escape(stamp || '待核验')}${observationLag(market, clock, requested)}</span>` +
            '<button type="button" class="mh-retry" data-mh-refresh>重载</button></div></header>' +
            `<p class="mh-meta" role="status" data-mh-refresh-status>${escape(refreshText)}</p>`;
        return renderHub(state, requested, clock, modes, market, snapshot) + markup +
            renderIndexStrip(requested, market, state.trend, snapshot,clock) + renderMetrics(market, snapshot, state.liquidity, requested,clock) +
            '<div class="mh-grid">' + renderBreadth(market, snapshot,clock) + renderEmotion(market,clock) + renderTrend(state.trend, market, snapshot, requested,clock) +
            renderLadder(snapshot,clock) + renderDrawdowns(snapshot) +
            renderSector('concept', '概念热度', state.heatmap, requested, modes.concept || 'change',clock) +
            renderSector('industry', '行业热度', state.heatmap, requested, modes.industry || 'change',clock) +
            renderEvents(state.events, requested,clock,state.pendingEvents) + rankings.map(([key, title, field, unit]) =>
                renderRanking(snapshot,key,title,field,unit,clock)).join('') +
            '</div></section>';
    }

    function stop() {
        if (!active) return;
        active.cleanup?.();
        active.detail?.remove();
        active = null;
    }

    function load(date, container, options = {}) {
        const requested = day(date) || day(options.clock?.()?.ui_trade_date);
        if (!requested) return Promise.resolve({loadError:'交易日期待核验'});
        const retained = active?.container === container && active.requested === requested ? active.state : {};
        const modes = active?.container === container && active.requested === requested ? active.modes :
            {concept:'change',industry:'change',month:requested.slice(0,7),feed:'全部'};
        stop();
        const session = {container,requested,state:retained,modes,socket:null,failures:0,timers:{},pointer:false};
        active = session;
        const current = () => active === session;
        const liveKeys = ['snapshot','heatmap','liquidity','events'];
        const referenceKeys = ['trend','reviewDates','delivery'];
        const activeDay = day(options.clock?.()?.active_trade_date);
        const streaming = !activeDay || requested >= activeDay;
        const query = encodeURIComponent(requested);
        const paths = {snapshot:'/api/monitor/board?date='+query,heatmap:'/api/monitor/sector-heatmap?date='+query,
            liquidity:'/api/monitor/liquidity?date='+query,events:'/api/monitor/events?date='+query,
            trend:'/api/hot-data/market-trend?date='+query,reviewDates:'/api/hot-data/daily-review-dates',
            delivery:'/api/scheduler/daily-delivery?trade_date='+query+'&attempt_limit=12'};
        let firstResolve;
        const firstState = new Promise(resolve => {firstResolve=resolve;});
        function finishInitial(outcome={}) {
            if (!firstResolve) return;
            clearTimeout(session.timers.initial);
            firstResolve(outcome);firstResolve=null;
        }
        function renderClock() {
            const value = {...options.clock?.()};
            if (session.serverTime) value.server_time = new Date(session.serverTime.value+performance.now()-session.serverTime.at).toISOString();
            return value;
        }
        function paint() {
            if (!current()) return;
            if (session.pointer) {session.needsPaint=true;return;}
            const focused = document.activeElement;
            const next = container.cloneNode(false);
            next.innerHTML = render(session.state,requested,renderClock(),session.modes);
            patchNode(container,next);
            if (focused?.isConnected && document.activeElement !== focused) focused.focus?.({preventScroll:true});
            session.needsPaint=false;
        }
        function accept(key,value) {
            const errors = session.state.refreshErrors ||= {};
            if (value === null) return;
            if (['snapshot','heatmap','liquidity','events'].includes(key) &&
                day(value?.requested_date || value?.trade_date) !== requested) {errors[key]=true;return;}
            if (key === 'events') {
                if (!Array.isArray(value.events)) {errors.events=true;return;}
                const list = container.querySelector('[data-mh-events]');
                if (list?.scrollTop > 0 && session.state.events) {
                    const known = new Set(rows(session.state.events.events).map(eventIdentity));
                    if (value.events.some(item => !known.has(eventIdentity(item)))) session.state.pendingEvents=value;
                    delete errors.events;
                    return;
                }
                session.state.pendingEvents=null;
            }
            session.state[key]=value;
            delete errors[key];
        }
        async function requestPart(key) {
            try {
                const value = await options.request(paths[key],key === 'trend' ? 30000 : 15000,{cache:'no-store'});
                if (!current()) return;
                accept(key,value);paint();
                if (liveKeys.includes(key)) finishInitial();
            } catch {
                if (!current()) return;
                (session.state.refreshErrors ||= {})[key]=true;paint();
            }
        }
        function references() {referenceKeys.forEach(key => {void requestPart(key);});}
        function disconnect() {
            clearTimeout(session.timers.connection);
            if (!session.socket) return;
            const socket=session.socket;session.socket=null;
            socket.onopen=socket.onmessage=socket.onclose=socket.onerror=null;
            socket.close(1000,'view changed');
        }
        function reconnectLater() {
            if (!current() || document.hidden) return;
            session.state.transport='disconnected';paint();
            finishInitial({loadError:'推送暂不可用，正在自动重连'});
            const wait=[1000,2000,5000,10000,20000][Math.min(session.failures++,4)];
            clearTimeout(session.timers.reconnect);
            session.timers.reconnect=setTimeout(connect,wait);
        }
        function connect() {
            if (!current() || document.hidden) return;
            disconnect();clearTimeout(session.timers.reconnect);
            session.state.transport='connecting';paint();
            try {
                const address=new URL('/api/monitor/stream?date='+query,root.location.href);
                address.protocol=address.protocol === 'https:' ? 'wss:' : 'ws:';
                const socket=options.streamFactory ? options.streamFactory(address.href) : new root.WebSocket(address.href);
                session.socket=socket;
                const valid=()=>current() && session.socket===socket;
                session.timers.connection=setTimeout(()=>{if(valid()){disconnect();reconnectLater();}},8000);
                socket.onopen=()=>{
                    if(!valid())return;
                    clearTimeout(session.timers.connection);session.failures=0;
                    session.lastMessage=performance.now();session.state.transport='connected';paint();
                };
                socket.onmessage=event=>{
                    if(!valid())return;
                    try {
                        if(typeof event.data!=='string'||event.data.length>2500000)throw new Error('invalid frame');
                        const message=JSON.parse(event.data);
                        if(message.trade_date!==requested)throw new Error('wrong session');
                        const at=Date.parse(message.server_time);
                        if(Number.isFinite(at))session.serverTime={value:at,at:performance.now()};
                        session.lastMessage=performance.now();
                        if(message.type==='closing'){disconnect();reconnectLater();return;}
                        if(message.type==='error'){
                            (session.state.refreshErrors ||= {}).stream=true;
                            finishInitial({loadError:'后台快照暂不可读，保留已显示观测'});
                        } else {
                            delete (session.state.refreshErrors ||= {}).stream;
                            for(const key of liveKeys)if(Object.hasOwn(message.components||{},key))accept(key,message.components[key]);
                            if(message.type==='update')finishInitial();
                        }
                        paint();
                    } catch {disconnect();reconnectLater();}
                };
                socket.onclose=()=>{if(valid()){session.socket=null;reconnectLater();}};
                socket.onerror=()=>{if(valid()){disconnect();reconnectLater();}};
            } catch {reconnectLater();}
        }
        async function olderEvents() {
            const feed=session.state.events;
            if(!current()||!feed?.next_cursor||feed.loading)return;
            feed.loading=true;feed.error=false;paint();
            try {
                const page=await options.request(paths.events+'&before='+encodeURIComponent(feed.next_cursor),15000,{cache:'no-store'});
                if(current()&&session.state.events===feed){
                    if(page?.requested_date!==requested||!Array.isArray(page.events))throw new Error('invalid event page');
                    const known=new Set(feed.events.map(eventIdentity));
                    feed.events.push(...page.events.filter(item=>!known.has(eventIdentity(item))));
                    feed.next_cursor=page.next_cursor;
                }
            } catch {feed.error=true;}
            finally {feed.loading=false;paint();}
        }
        session.click=event=>{
            const button=event.target.closest('button');
            if(!button||!container.contains(button))return;
            if(button.hasAttribute('data-mh-events-latest')){
                if(session.state.pendingEvents)session.state.events=session.state.pendingEvents;
                session.state.pendingEvents=null;
                const list=container.querySelector('[data-mh-events]');if(list)list.scrollTop=0;
                paint();
            } else if(button.hasAttribute('data-mh-events-more'))void olderEvents();
            else if(button.dataset.mhTab)options.navigate?.(button.dataset.mhTab,button.dataset.mhView);
            else if(button.dataset.mhBoard){
                const kind=button.dataset.mhBoard;session.detail?.remove();
                session.detail=openBoardDetail(document.body,session.state.heatmap,requested,kind,session.modes[kind],button.dataset.mhBoardCode);
            } else if(button.dataset.mhStock)options.stock?.(button.dataset.mhStock);
            else if(button.dataset.mhDate)options.selectDate?.(button.dataset.mhDate);
            else if(button.dataset.mhMonth){
                const shift=Number(button.dataset.mhMonth);
                if(shift!==-1&&shift!==1)return;
                const month=monthShift(session.modes.month,shift);
                if(month>='1900-01'&&month<=(day(options.clock?.()?.today)||requested).slice(0,7)){session.modes.month=month;paint();}
            } else if(button.dataset.mhHeatMode){
                const [kind,mode]=button.dataset.mhHeatMode.split(':');
                if(Object.hasOwn(session.modes,kind)&&heatModes.some(([key])=>key===mode)){session.modes[kind]=mode;paint();}
            } else if(button.hasAttribute('data-mh-refresh')){
                if(streaming)connect();else liveKeys.forEach(key=>{void requestPart(key);});
                references();
            } else if(button.hasAttribute('data-mh-observation')){
                const observation=container.querySelector('#mh-market-observation');
                observation?.focus({preventScroll:true});observation?.scrollIntoView({block:'start'});
            }
        };
        session.change=event=>{
            if(event.target.hasAttribute('data-mh-feed')&&['全部','行情','复盘','交付'].includes(event.target.value)){
                session.modes.feed=event.target.value;paint();
            }
        };
        session.scroll=event=>{
            const list=event.target;
            if(list.hasAttribute?.('data-mh-events')&&list.scrollTop+list.clientHeight>=list.scrollHeight-80&&!session.state.events?.error)void olderEvents();
        };
        session.pointerDown=()=>{session.pointer=true;};
        session.pointerUp=()=>{
            clearTimeout(session.timers.pointer);
            session.timers.pointer=setTimeout(()=>{session.pointer=false;if(session.needsPaint)paint();},0);
        };
        session.blur=()=>{clearTimeout(session.timers.blur);session.timers.blur=setTimeout(paint,0);};
        session.visibility=()=>{
            if(!streaming)return;
            if(document.hidden){disconnect();clearTimeout(session.timers.reconnect);session.state.transport='paused';paint();}
            else connect();
        };
        const listeners=[['click',session.click],['change',session.change],['scroll',session.scroll,true],
            ['pointerdown',session.pointerDown,true],['focusout',session.blur]];
        listeners.forEach(args=>container.addEventListener(...args));
        document.addEventListener('pointerup',session.pointerUp,true);
        document.addEventListener('pointercancel',session.pointerUp,true);
        document.addEventListener('visibilitychange',session.visibility);
        session.cleanup=()=>{
            disconnect();Object.values(session.timers).forEach(clearTimeout);
            listeners.forEach(args=>container.removeEventListener(...args));
            document.removeEventListener('pointerup',session.pointerUp,true);
            document.removeEventListener('pointercancel',session.pointerUp,true);
            document.removeEventListener('visibilitychange',session.visibility);
            finishInitial({loadError:'页面已切换'});
        };
        function tick(){
            if(!current())return;
            if(!document.hidden){
                if(streaming&&session.socket&&session.lastMessage&&performance.now()-session.lastMessage>25000){disconnect();reconnectLater();}
                paint();
            }
            session.timers.freshness=setTimeout(tick,5000);
        }
        session.state.transport=streaming?'connecting':'historical';paint();references();
        session.timers.initial=setTimeout(()=>finishInitial({loadError:streaming?'推送暂不可用，正在自动重连':'历史观测暂未返回'}),10000);
        if(streaming)connect();else liveKeys.forEach(key=>{void requestPart(key);});
        tick();
        return firstState;
    }

    const api = {load, stop, render};
    root.MarketHome = api;
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window === 'undefined' ? globalThis : window);
