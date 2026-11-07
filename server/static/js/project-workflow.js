(function (root) {
    'use strict';

    const api = '/api/project-workflow/tasks';
    const states = new Map(Object.entries({
        NOT_STARTED:'未开始', RUNNING:'进行中', BLOCKED:'受阻', FAILED:'失败',
        COMPLETE:'完成', WAITING:'等待恢复', RECONCILING:'核对外部执行',
        VERIFIED:'已核验', UNKNOWN:'未知', UNVERIFIED:'未验证'
    }));
    const stages = new Map([
        ['merged', 'GitHub 合并'],
        ['tested', '测试'],
        ['released', '实际发布'],
        ['active', '生产生效']
    ]);
    const phases = new Map([['rework', '返工'], ['supervise', '监督诊断'], ['reconcile', '核对回执']]);
    const timeFormat = new Intl.DateTimeFormat('zh-CN', {
        timeZone:'Asia/Shanghai', dateStyle:'short', timeStyle:'medium'
    });
    const mounted = new WeakMap();

    function node(tag, text, className) {
        const element = document.createElement(tag);
        if (text != null) element.textContent = String(text);
        if (className) element.className = className;
        return element;
    }

    function timestamp(value) {
        const date = new Date(Number(value) * 1000);
        return value && Number.isFinite(date.getTime()) ? timeFormat.format(date) : '—';
    }

    function badge(state) {
        const element = node('span', states.get(state) || state || '未记录', 'pw-state');
        element.dataset.state = state || '';
        return element;
    }

    function table(title, headers, rows) {
        const section = node('section');
        section.append(node('h3', title));
        if (!rows.length) {
            section.append(node('p', '暂无记录', 'pw-muted'));
            return section;
        }
        const wrapper = node('div', null, 'pw-table-wrap');
        const grid = node('table', null, 'pw-table');
        const head = node('tr');
        headers.forEach(function (label) {
            const th = node('th', label);
            th.scope = 'col';
            head.append(th);
        });
        const thead = node('thead');
        thead.append(head);
        const tbody = node('tbody');
        rows.forEach(function (row) {
            const tr = node('tr');
            row.forEach(function (value) {
                const td = node('td');
                td.append(value == null || value === '' ? '—' : value);
                tr.append(td);
            });
            tbody.append(tr);
        });
        grid.append(thead, tbody);
        wrapper.append(grid);
        section.append(wrapper);
        return section;
    }

    function facts(values) {
        const list = node('dl', null, 'pw-facts');
        values.forEach(function ([label, value]) {
            const item = node('div');
            item.append(node('dt', label), node('dd', value));
            list.append(item);
        });
        return list;
    }

    function renderTask(task) {
        const fragment = document.createDocumentFragment();
        fragment.append(node('h2', task.title || task.id), badge(task.state));
        const information = [
            ['任务 ID', task.id],
            ['进度', `${task.completed ?? '—'} / ${task.total ?? '—'}`],
            ['当前步骤', task.current || '—'],
            ['最近进展（北京时间）', timestamp(task.updated_at)]
        ];
        if (task.historical) information.push(['记录类型', '历史验收']);
        else information.push(['截止时间（北京时间）', timestamp(task.expires_at)]);
        if (task.paused) information.push(['任务控制', '已暂停']);
        if (task.canceled) information.push(['任务控制', '已取消']);
        if (task.code) information.push(['当前原因', task.code]);
        fragment.append(facts(information));

        const evidenceRows = [];
        for (const [stage, label] of stages) {
            const entries = (task.evidence || []).filter(item => item.stage === stage);
            if (!entries.length) evidenceRows.push([label, badge(), '', '', '', '', '']);
            entries.forEach(function (item) {
                evidenceRows.push([
                    label, badge(item.state), item.candidate,
                    [item.environment, item.endpoint].filter(Boolean).join(' / '),
                    timestamp(item.at), item.code,
                    [item.run_id || item.external_id, item.build, item.instance_id].filter(Boolean).join(' / ')
                ]);
            });
        }
        fragment.append(table('工程验收证据', ['阶段','状态','版本','环境 / 端点','证据时间','来源或结果','运行 / 构建 / 实例'], evidenceRows));

        const steps = (task.steps || []).map(function (step) {
            const units = step.progress || {};
            const progress = units.total != null ? `${units.completed ?? '—'} / ${units.total}` : '';
            return [
                step.id, phases.get(step.phase) || step.kind, step.endpoint, badge(step.state),
                String(step.attempts ?? '—'), [progress, units.unit].filter(Boolean).join(' · '),
                timestamp(step.next_at), step.code
            ];
        });
        fragment.append(table('执行步骤', ['步骤','阶段','端点','状态','尝试次数','单位进度','下次检查','结果'], steps));
        const events = (task.events || []).slice(0, 25).map(function (event) {
            return [timestamp(event.at), event.step, event.code];
        });
        fragment.append(table('最近事件', ['北京时间','步骤','事件'], events));
        return fragment;
    }

    function selectTask(view, taskId) {
        view.selected = taskId;
        view.list.querySelectorAll('button').forEach(button => {
            button.setAttribute('aria-pressed', String(button.dataset.taskId === taskId));
        });
    }

    async function refreshDetail(view) {
        const taskId = view.selected;
        const ticket = ++view.detailTicket;
        try {
            const task = await view.request(api + '/' + encodeURIComponent(taskId), 5000);
            if (view.detailTicket !== ticket) return;
            const signature = JSON.stringify(task);
            if (view.detailSignature !== signature) {
                view.detail.replaceChildren(renderTask(task));
                view.detailSignature = signature;
            }
        } catch (error) {
            if (view.detailTicket !== ticket) return;
            view.detailSignature = '';
            const message = error.httpStatus === 404 ? '未找到该任务' : '任务详情读取失败，等待刷新';
            view.detail.replaceChildren(node('p', message, 'pw-notice'));
        }
    }

    async function refresh(view) {
        try {
            const payload = await view.request(api, 5000);
            if (!Array.isArray(payload.tasks)) throw new Error('Invalid task list');
            const tasks = payload.tasks;
            const signature = JSON.stringify(tasks);
            if (view.listSignature !== signature) {
                const buttons = tasks.map(function (task) {
                    const button = node('button', null, 'pw-task');
                    button.type = 'button';
                    button.dataset.taskId = task.id;
                    button.append(node('strong', task.title || task.id), badge(task.state));
                    button.addEventListener('click', function () {
                        selectTask(view, task.id);
                        view.detail.replaceChildren(node('p', '正在读取任务详情…', 'pw-notice'));
                        view.detailSignature = '';
                        refreshDetail(view);
                    });
                    return button;
                });
                view.list.replaceChildren(...buttons);
                view.listSignature = signature;
            }
            view.notice.textContent = tasks.length ? '' : '尚无已登记任务';
            if (!tasks.length) {
                selectTask(view, null);
                view.detail.replaceChildren(node('p', '登记后可在此查看执行和验收证据', 'pw-notice'));
            } else {
                selectTask(view, tasks.some(task => task.id === view.selected) ? view.selected : tasks[0].id);
                await refreshDetail(view);
            }
        } catch (error) {
            view.notice.textContent = error.httpStatus === 503 ? '编排证据库暂不可用' : '任务列表读取失败，等待刷新';
            view.list.replaceChildren();
            view.detail.replaceChildren();
            view.listSignature = '';
            view.detailSignature = '';
            view.detailTicket++;
            return {loadError:view.notice.textContent};
        }
    }

    function runRefresh(view, options) {
        if (view.pending) return view.pending;
        clearTimeout(view.timer);
        view.pending = refresh(view).finally(function () {
            view.pending = null;
            function poll() {
                if (!options.isActive() || !view.layout.isConnected) return;
                if (document.hidden) view.timer = setTimeout(poll, 10000);
                else runRefresh(view, options);
            }
            if (options.isActive() && view.layout.isConnected) view.timer = setTimeout(poll, 10000);
        });
        return view.pending;
    }

    function load(container, options) {
        let view = mounted.get(container);
        if (!view || !container.contains(view.layout)) {
            if (view) clearTimeout(view.timer);
            const layout = node('div', null, 'pw-layout');
            const sidebar = node('section', null, 'pw-panel');
            const list = node('div', null, 'pw-task-list');
            const notice = node('p', '正在读取任务…', 'pw-notice');
            notice.setAttribute('role', 'status');
            const detail = node('section', null, 'pw-panel');
            sidebar.append(node('h2', '最近工程任务'), notice, list);
            layout.append(sidebar, detail);
            container.replaceChildren(layout);
            view = {
                layout, list, notice, detail, request:options.request,
                selected:view && view.selected, detailTicket:0
            };
            mounted.set(container, view);
        }
        return runRefresh(view, options).then(function (result) {
            if (options.isActive() && view.layout.isConnected) {
                options.status(result && result.loadError || '工程验收已更新', Boolean(result && result.loadError));
            }
            return result;
        });
    }

    root.ProjectWorkflow = {load};
})(window);
