(() => {
    'use strict';

    const byId = id => document.getElementById(id);
    const date = byId('trade-date');
    const stream = byId('stream');
    const rows = byId('events');
    const status = byId('status');
    const previous = byId('previous');
    const next = byId('next');
    const dayPattern = /^\d{4}-\d{2}-\d{2}$/;
    const labels = {
        TOUCH: '触及涨停', SEAL: '首次封板', RESEAL: '回封', BREAK: '开板',
        QUEUE_SNAPSHOT: '封单快照', stock_anomaly: '个股异动', sector_anomaly: '板块异动',
    };
    const directions = {UP: '上涨', DOWN: '下跌'};
    const reasons = {SESSION_IN_FUTURE: '请求日尚未到达。', EVENT_STORE_UNAVAILABLE: '所选事件来源暂时无法读取。'};

    let request;
    let selection;
    let cursors = [''];
    let page = 0;
    let nextCursor = '';

    function formatTime(value) {
        return value ? String(value).replace('T', ' ').replace(/\+08:00$/, '') : '—';
    }

    function paint(data) {
        for (const item of data.rows) {
            const row = document.createElement('tr');
            const subject = item.stock_code && item.stock_code !== item.subject
                ? item.subject + ' ' + item.stock_code : item.subject;
            const cells = [formatTime(item.event_at), formatTime(item.source_snapshot_at), formatTime(item.available_at),
                labels[item.event_type] || item.event_type, subject, directions[item.direction] || '—'];
            for (const value of cells) {
                const cell = document.createElement('td');
                cell.textContent = value;
                row.append(cell);
            }
            rows.append(row);
        }
        const message = data.status === 'DATA_BLOCKED'
            ? reasons[data.reason_code] || '事件记录暂时无法读取。'
            : data.rows.length ? '本页 ' + data.rows.length + ' 条已核验记录。' : '本页暂无可核验记录。';
        const discarded = data.discarded_count ? '本页有 ' + data.discarded_count + ' 条记录未通过校验。' : '';
        status.textContent = data.trade_date + ' · ' + message + discarded + '全天采集覆盖未核验。' +
            (data.known_through ? ' 本次查询记录截止 ' + formatTime(data.known_through) + '。' : '');
    }

    async function load() {
        if (request) request.abort();
        const controller = new AbortController();
        request = controller;
        const selected = selection;
        const targetPage = page;
        rows.replaceChildren();
        previous.disabled = true;
        next.disabled = true;
        nextCursor = '';
        byId('page-number').textContent = '';
        status.textContent = '正在读取 ' + selected.date + ' 的已记录事件…';
        const params = new URLSearchParams({date: selected.date, stream: selected.stream});
        if (cursors[targetPage]) params.set('cursor', cursors[targetPage]);
        const timer = setTimeout(() => controller.abort(), 8000);
        try {
            const result = await fetch('/api/monitor/event-timeline?' + params, {signal: controller.signal});
            if (!result.ok) throw Error('请求未完成');
            const data = await result.json();
            if (request !== controller) return;
            if (data.trade_date !== selected.date || data.stream !== selected.stream || !Array.isArray(data.rows)) {
                throw Error('日期或事件来源不一致');
            }
            paint(data);
            if (typeof data.page_cursor === 'string') cursors[targetPage] = data.page_cursor;
            nextCursor = typeof data.next_cursor === 'string' ? data.next_cursor : '';
            byId('page-number').textContent = '第 ' + (targetPage + 1) + ' 页';
        } catch (error) {
            if (request === controller) status.textContent = error.name === 'AbortError' ? '读取超时，请重试。' : '读取失败：' + error.message + '。';
        } finally {
            clearTimeout(timer);
            if (request === controller) {
                previous.disabled = targetPage === 0;
                next.disabled = !nextCursor;
            }
        }
    }

    function select() {
        if (!dayPattern.test(date.value)) {
            request?.abort();
            request = null;
            rows.replaceChildren();
            previous.disabled = true;
            next.disabled = true;
            nextCursor = '';
            byId('page-number').textContent = '';
            status.textContent = '请选择交易日。';
            history.replaceState(null, '', location.pathname);
            byId('board').href = '/?tab=workbench';
            return;
        }
        selection = {date: date.value, stream: stream.value};
        page = 0;
        cursors = [''];
        const params = new URLSearchParams(selection);
        history.replaceState(null, '', '?' + params);
        byId('board').href = '/?tab=workbench&trade_date=' + encodeURIComponent(selection.date);
        load();
    }

    byId('filters').addEventListener('submit', event => {
        event.preventDefault();
        select();
    });
    date.addEventListener('change', select);
    stream.addEventListener('change', select);
    next.addEventListener('click', () => {
        if (!nextCursor) return;
        cursors[++page] = nextCursor;
        load();
    });
    previous.addEventListener('click', () => {
        if (page === 0) return;
        page--;
        load();
    });
    const params = new URLSearchParams(location.search);
    if (params.get('stream') === 'radar') stream.value = 'radar';
    if (dayPattern.test(params.get('date') || '')) {
        date.value = params.get('date');
        select();
    }
})();
