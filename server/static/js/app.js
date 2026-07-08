(function () {
    var API_BASE = '/api/hot-data';
    var ADMIN_TOKEN_STORAGE_KEY = 'probiga_admin_token';

    function adminToken() {
        try { return localStorage.getItem(ADMIN_TOKEN_STORAGE_KEY) || ''; } catch (e) { return ''; }
    }
    function setAdminToken(token) {
        try {
            if (token) localStorage.setItem(ADMIN_TOKEN_STORAGE_KEY, token);
            else localStorage.removeItem(ADMIN_TOKEN_STORAGE_KEY);
        } catch (e) {}
    }
    function sameOriginPath(input) {
        var raw = typeof input === 'string' ? input : (input && input.url) || '';
        if (!raw) return '';
        try {
            var url = new URL(raw, window.location.origin);
            if (url.origin !== window.location.origin) return '';
            return url.pathname || '/';
        } catch (e) {
            return String(raw).charAt(0) === '/' ? String(raw).split('?')[0] : '';
        }
    }
    function withAdminHeader(input, init, retrying) {
        var path = sameOriginPath(input);
        var token = adminToken();
        if (!path || !token) return init;
        var next = {};
        var source = init || {};
        for (var k in source) if (Object.prototype.hasOwnProperty.call(source, k)) next[k] = source[k];
        var headers = new Headers(source.headers || (input && input.headers) || {});
        headers.set('X-ProBigA-Admin-Token', token);
        next.headers = headers;
        if (retrying) next._probigaAdminRetry = true;
        return next;
    }
    if (window.fetch && !window.fetch._probigaAdminWrapped) {
        var nativeFetch = window.fetch.bind(window);
        var wrappedFetch = function (input, init) {
            var firstInit = withAdminHeader(input, init, false);
            return nativeFetch(input, firstInit).then(function (response) {
                if (
                    response.status !== 401 ||
                    !response.headers ||
                    response.headers.get('X-ProBigA-Admin-Auth') !== 'required' ||
                    (firstInit && firstInit._probigaAdminRetry)
                ) {
                    return response;
                }
                var token = window.prompt('Admin token required');
                if (!token) return response;
                setAdminToken(token.trim());
                return nativeFetch(input, withAdminHeader(input, init, true));
            });
        };
        wrappedFetch._probigaAdminWrapped = true;
        window.fetch = wrappedFetch;
        window.probigaSetAdminToken = setAdminToken;
    }

    window.onerror = function (msg, src, line, col, err) {
        console.error('[全局错误]', msg, src, line, col, err);
        var mc = el('mainContent');
        if (mc && mc.innerHTML.indexOf('加载中') !== -1) {
            mc.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 页面加载异常：' + msg + '（第' + line + '行）<br>请刷新页面或检查控制台</div>';
        }
    };

    function el(id) { return document.getElementById(id); }
    function setStatus(msg, err) {
        var s = el('statusText'); if (s) { s.textContent = msg; s.style.color = err ? '#e74c3c' : '#888'; }
    }

    /* ===== 工具函数 ===== */
    function fmt(v, d) { if (v == null || v === '') return '-'; var n = Number(v); return isNaN(n) ? String(v) : (n.toFixed(d != null ? d : 2)); }
    function pct(v) { if (v == null || v === '' || isNaN(v)) return '-'; var n = Number(v); return (n >= 0 ? '+' : '') + n.toFixed(2) + '%'; }
    function clsPct(v) { var n = Number(v); if (isNaN(n)) return ''; return n > 0 ? 'red' : (n < 0 ? 'green' : 'gray'); }
    function fmtMoney(v) { if (v == null || isNaN(v)) return '-'; var n = Math.abs(v); var s = v < 0 ? '-' : ''; if (n >= 1e8) return s + (n / 1e8).toFixed(2) + '亿'; if (n >= 1e4) return s + (n / 1e4).toFixed(1) + '万'; return s + n.toFixed(0); }
    function fmtPrice(v) { if (v == null || isNaN(v)) return '-'; var n = Number(v); var s = String(n); var dot = s.indexOf('.'); if (dot < 0) return s + '.00'; var dec = s.length - dot - 1; if (dec <= 2) return n.toFixed(2); return s; }
    var ACTIVE_TAB = '';
    var MARKET_CLOCK = null;
    function setActiveTab(tabId) {
        ACTIVE_TAB = String(tabId || '');
        window._activeTab = ACTIVE_TAB;
    }
    function activeTabId() {
        if (ACTIVE_TAB) return ACTIVE_TAB;
        var active = document.querySelector('.sidebar-item.active');
        return active ? String(active.getAttribute('data-tab') || '') : '';
    }
    function localTradingTime() {
        var now = new Date();
        var day = now.getDay();
        if (day === 0 || day === 6) return false;
        var hm = now.getHours() * 60 + now.getMinutes();
        return (hm >= 570 && hm <= 690) || (hm >= 780 && hm <= 900);
    }
    function isTradingTime() {
        var localOpen = localTradingTime();
        if (MARKET_CLOCK && typeof MARKET_CLOCK.is_intraday === 'boolean') return MARKET_CLOCK.is_intraday || localOpen;
        return localOpen;
    }
    function rankBadge(r) { if (r == 1) return '<span class="rank-badge rank-1">1</span>'; if (r == 2) return '<span class="rank-badge rank-2">2</span>'; if (r == 3) return '<span class="rank-badge rank-3">3</span>'; return '<span class="rank-badge rank-n">' + r + '</span>'; }
    function sourceTag(f) {
        var m = {
            'all':'4源', 'east_ths_xq':'东财+同花顺+雪球', 'east_ths_sina':'东财+同花顺+新浪',
            'east_xq_sina':'东财+雪球+新浪', 'ths_xq_sina':'同花顺+雪球+新浪',
            'both':'东财+同花顺', 'east_xq':'东财+雪球', 'east_sina':'东财+新浪',
            'ths_xq':'同花顺+雪球', 'ths_sina':'同花顺+新浪', 'xq_sina':'雪球+新浪',
            'east_only':'仅东财', 'ths_only':'仅同花顺', 'xq_only':'仅雪球', 'sina_only':'仅新浪',
        };
        return m[f] || f || '-';
    }
    function nameLink(code, name) {
        var c = (code || '').toString(), n = (name || c);
        return '<a class="clickable-name" href="javascript:void(0)" onclick="openKlineModal(\'' + c + '\',\'' + n.replace(/'/g, "\\'") + '\')">' + n + '</a>';
    }
    function conceptNameLink(code, name, isIndustry) {
        var c = (code || '').toString(), n = (name || c);
        return '<a class="clickable-name" href="javascript:void(0)" onclick="showConceptStocks(\'' + c + '\',\'' + n.replace(/'/g, "\\'") + '\')">' + n + '</a>';
    }

    function apiGet(path) { return fetchJsonWithTimeout(path, 15000); }
    function fetchRawJsonWithTimeout(url, timeoutMs) {
        var controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
        var waitMs = timeoutMs || 5000;
        return new Promise(function (resolve, reject) {
            var settled = false;
            var timer = setTimeout(function () {
                if (settled) return;
                settled = true;
                try {
                    if (controller) controller.abort();
                } catch (e) {}
                reject(new Error('Timed out after ' + waitMs + 'ms'));
            }, waitMs);

            fetch(url, controller ? { signal: controller.signal } : undefined)
                .then(function (r) {
                    return r.text().then(function (text) {
                        var data = null;
                        if (text) {
                            try {
                                data = JSON.parse(text);
                            } catch (e) {
                                var preview = text.replace(/\s+/g, ' ').trim().slice(0, 200);
                                throw new Error(preview || ('HTTP ' + r.status));
                            }
                        }
                        if (!r.ok) {
                            var msg = (data && (data.message || data.error || data.detail)) || ('HTTP ' + r.status);
                            throw new Error(msg);
                        }
                        return data == null ? {} : data;
                    });
                })
                .then(function (data) {
                    if (settled) return;
                    settled = true;
                    clearTimeout(timer);
                    resolve(data);
                })
                .catch(function (err) {
                    if (settled) return;
                    settled = true;
                    clearTimeout(timer);
                    reject(err);
                });
        });
    }
    function fetchJsonWithTimeout(path, timeoutMs) {
        return fetchRawJsonWithTimeout(API_BASE + path, timeoutMs);
    }
    var silentRefreshDepth = 0;
    var innerHtmlDescriptor = Object.getOwnPropertyDescriptor(Element.prototype, 'innerHTML');
    function hasRenderedContent(node) {
        if (!node) return false;
        var text = (node.textContent || '').replace(/\s+/g, '');
        return text.length > 0 && !/^加载中|^正在加载|^Loading/i.test(text);
    }
    function isRefreshPlaceholder(html) {
        var s = String(html == null ? '' : html).trim();
        if (!s) return false;
        if (s.indexOf('class="loading"') >= 0 || s.indexOf("class='loading'") >= 0) {
            return /加载中|正在加载|加载.*数据|加载.*页面|刷新中|加载失败|网络错误|Timed out|Timeout|暂无/.test(s);
        }
        return false;
    }
    function markSilentRefreshTarget(node) {
        if (!node || !node.setAttribute || !hasRenderedContent(node)) return false;
        node.setAttribute('data-refreshing', '1');
        clearTimeout(node._probigaRefreshGuardTimer);
        node._probigaRefreshGuardTimer = setTimeout(function () {
            if (node.removeAttribute) node.removeAttribute('data-refreshing');
        }, 90000);
        return true;
    }
    function runWithSilentRefresh(fn) {
        silentRefreshDepth += 1;
        var result;
        try {
            result = fn();
        } catch (e) {
            silentRefreshDepth = Math.max(0, silentRefreshDepth - 1);
            throw e;
        }
        if (result && typeof result.then === 'function') {
            return result.finally(function () {
                silentRefreshDepth = Math.max(0, silentRefreshDepth - 1);
            });
        }
        silentRefreshDepth = Math.max(0, silentRefreshDepth - 1);
        return result;
    }
    function refreshLoadTab(tabId) {
        return loadTab(tabId, { silent: true });
    }
    try {
        if (!Element.prototype._probigaSilentRefreshGuard && innerHtmlDescriptor && innerHtmlDescriptor.set && innerHtmlDescriptor.get) {
            Object.defineProperty(Element.prototype, 'innerHTML', {
                configurable: true,
                enumerable: innerHtmlDescriptor.enumerable,
                get: function () { return innerHtmlDescriptor.get.call(this); },
                set: function (value) {
                    var guarded = silentRefreshDepth > 0 || (this.getAttribute && this.getAttribute('data-refreshing') === '1');
                    if (guarded && hasRenderedContent(this) && isRefreshPlaceholder(value)) {
                        if (this.setAttribute) {
                            this.setAttribute('data-refreshing', '1');
                            var node = this;
                            clearTimeout(node._probigaRefreshGuardTimer);
                            node._probigaRefreshGuardTimer = setTimeout(function () {
                                if (node.removeAttribute) node.removeAttribute('data-refreshing');
                            }, 90000);
                        }
                        return;
                    }
                    if (this.removeAttribute) {
                        this.removeAttribute('data-refreshing');
                        clearTimeout(this._probigaRefreshGuardTimer);
                    }
                    return innerHtmlDescriptor.set.call(this, value);
                }
            });
            Element.prototype._probigaSilentRefreshGuard = true;
        }
    } catch (e) {}
    function escAttr(v) {
        return String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
    }
    function escHtml(v) {
        return String(v == null ? '' : v)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }
    function safeText(v) { return escAttr(v == null ? '' : v); }
    function pad2(n) { return String(n).padStart(2, '0'); }
    function localDateString(d) {
        d = d || new Date();
        return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate());
    }
    function localDateTimeString(d) {
        d = d || new Date();
        return localDateString(d) + ' ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
    }
    function cleanDateTimeText(v) {
        if (v == null) return '';
        var s = String(v).trim();
        if (!s || s === 'null' || s === 'undefined') return '';
        s = s.replace('T', ' ').replace(/\.\d+/, '').replace(/(Z|[+-]\d{2}:?\d{2})$/, '').trim();
        var m = s.match(/^(\d{4}-\d{2}-\d{2})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?/);
        if (!m) return s;
        var t = m[2] || '';
        if (t && t.length === 5) t += ':00';
        return m[1] + (t ? ' ' + t : '');
    }
    function shortDateTimeText(v) {
        var s = cleanDateTimeText(v);
        var m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/);
        if (!m) return s;
        var datePart = m[1] + '-' + m[2] + '-' + m[3];
        var timePart = m[4] || '';
        if (!timePart) return m[2] + '-' + m[3];
        if (datePart === localDateString(new Date())) return timePart;
        return m[2] + '-' + m[3] + ' ' + timePart.slice(0, 5);
    }
    function currentDateValue() {
        var picker = el('datePicker');
        return (picker && picker.value) ? picker.value : localDateString(new Date());
    }
    function recommendationDateValue() {
        var pickerValue = currentDateValue();
        if (MARKET_CLOCK && MARKET_CLOCK.recommendation_trade_date) {
            if (!MARKET_CLOCK.ui_trade_date || pickerValue === MARKET_CLOCK.ui_trade_date) {
                return MARKET_CLOCK.recommendation_trade_date;
            }
        }
        return pickerValue;
    }
    function applyMarketClock(clock) {
        MARKET_CLOCK = clock || null;
        window._marketClock = MARKET_CLOCK;
        if (!clock) return;
        var picker = el('datePicker');
        if (picker && clock.ui_trade_date) picker.value = clock.ui_trade_date;
        setStatus('数据时钟: ' + (clock.phase_label || '-') + ' / 页面日期 ' + (clock.ui_trade_date || '-') + ' / 最新数据 ' + (clock.latest_data_date || '-'));
    }
    function loadMarketClock() {
        return fetchJsonWithTimeout('/market-clock', 3000)
            .then(function (clock) {
                applyMarketClock(clock);
                return clock;
            })
            .catch(function () {
                return fetchJsonWithTimeout('/latest-trade-date', 3000).then(function (res) {
                    var fallback = { phase_label: '未知', ui_trade_date: res.latest_date || localDateString(new Date()), latest_data_date: res.latest_date || '' };
                    applyMarketClock(fallback);
                    return fallback;
                }).catch(function () {
                    applyMarketClock({ phase_label: '本地', ui_trade_date: localDateString(new Date()), latest_data_date: '' });
                    return null;
                });
            });
    }
    function stripHtmlTags(v) {
        return String(v == null ? '' : v)
            .replace(/<[^>]*>/g, ' ')
            .replace(/[#>*`]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }
    function firstNonEmptyText() {
        for (var i = 0; i < arguments.length; i++) {
            var text = stripHtmlTags(arguments[i]);
            if (text) return text;
        }
        return '';
    }

    function syncDateFromResponse(res) {
        if (res && res.fallback && res.date) {
            el('datePicker').value = res.date;
            setStatus('当前数据日期: ' + res.date + '（最近交易日）');
        }
    }

    function fusedSourceSummary(res) {
        var label = res.source_label || '东财人气榜 / 雪球热股 / 新浪热股 / 同花顺热股';
        if (res.live) {
            return '<span style="background:#e74c3c;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:bold">盘中实时 ' + (res.time || '') + '</span>' +
                '<span style="margin-left:6px;color:#666;font-size:11px">' + label + '</span>';
        }
        return '日终数据 ' + (res.date || '') + '<span style="margin-left:6px;color:#666;font-size:11px">' + label + '</span>';
    }

    function tableHeadHtml(cols, cls) {
        return '<thead' + (cls ? ' class="' + cls + '"' : '') + '><tr>' + cols.map(function (c) { return '<th>' + c + '</th>'; }).join('') + '</tr></thead>';
    }

    function tableHeadFromHtml(headHtml, cls) {
        return cls ? headHtml.replace('<thead', '<thead class="' + cls + '"') : headHtml;
    }

    function stickyTopPx() {
        var h = document.querySelector('.header');
        return Math.ceil(h ? h.getBoundingClientRect().height : 0) + 'px';
    }

    function initPagedSticky(tableId) {
        var wrap = el('tw_' + tableId);
        var headWrap = el('sh_' + tableId);
        if (!wrap || !headWrap || wrap._stickyInited) return;
        wrap._stickyInited = true;
        wrap.addEventListener('scroll', function () { headWrap.scrollLeft = wrap.scrollLeft; });
        syncPagedHeader(tableId);
    }

    function renderPagedTable(container, tableId, toolbarHtml, headHtml, bodyId, pagerId) {
        container.classList.add('paged-table-section');
        var html = '<div class="paged-sticky-panel">';
        html += toolbarHtml;
        html += '<div class="paged-head-wrap" id="sh_' + tableId + '"><table class="paged-head-table">' + headHtml + '</table></div>';
        html += '</div>';
        html += '<div class="table-wrap paged-table-wrap" id="tw_' + tableId + '" data-table-id="' + tableId + '"><table class="paged-body-table">' + tableHeadFromHtml(headHtml, 'paged-original-head') + '<tbody id="' + bodyId + '"></tbody></table></div>';
        html += '<div class="pagination" id="' + pagerId + '"></div>';
        container.innerHTML = html;
        initPagedSticky(tableId);
    }

    function syncPagedHeader(tableId) {
        window.requestAnimationFrame(function () {
            var wrap = el('tw_' + tableId);
            var headWrap = el('sh_' + tableId);
            if (!wrap || !headWrap) return;
            var section = wrap.closest('.paged-table-section');
            if (section) section.style.setProperty('--paged-sticky-top', section.closest('.modal-body') ? '0px' : stickyTopPx());
            var bodyTable = wrap.querySelector('table');
            var headTable = headWrap.querySelector('table');
            if (!bodyTable || !headTable || !headTable.tHead || !headTable.tHead.rows.length) return;
            var bodyRow = bodyTable.tBodies[0] && bodyTable.tBodies[0].rows[0];
            var sourceCells = bodyRow ? bodyRow.children : (bodyTable.tHead && bodyTable.tHead.rows[0] ? bodyTable.tHead.rows[0].children : []);
            var targetCells = headTable.tHead.rows[0].children;
            for (var i = 0; i < targetCells.length; i++) {
                var src = sourceCells[i];
                var w = src ? src.getBoundingClientRect().width : targetCells[i].getBoundingClientRect().width;
                if (w > 0) {
                    targetCells[i].style.width = w + 'px';
                    targetCells[i].style.minWidth = w + 'px';
                    targetCells[i].style.maxWidth = w + 'px';
                }
            }
            headTable.style.width = bodyTable.getBoundingClientRect().width + 'px';
            headWrap.scrollLeft = wrap.scrollLeft;
        });
    }

    window.addEventListener('resize', function () {
        document.querySelectorAll('.paged-table-wrap[data-table-id]').forEach(function (w) {
            syncPagedHeader(w.getAttribute('data-table-id'));
        });
    });

    /* ===== 侧边栏切换 ===== */
    window.switchTab = function (tabId) {
        try {
            setActiveTab(tabId);
            if (tabId !== 'monitor' && typeof window.stopMonitorRefresh === 'function') {
                window.stopMonitorRefresh();
            }
            if (tabId !== 'sector' && typeof window.stopSectorMovementRefresh === 'function') {
                window.stopSectorMovementRefresh();
            }
            if (tabId !== 'intraday-battle' && typeof window.stopIntradayBattleRefresh === 'function') {
                window.stopIntradayBattleRefresh();
            }
            setStatus('加载中...');
            document.querySelectorAll('.sidebar-item').forEach(function (b) { b.classList.remove('active'); });
            var btn = document.querySelector('[data-tab="' + tabId + '"]');
            if (btn) btn.classList.add('active');
            document.querySelectorAll('.tab-content').forEach(function (c) { c.classList.remove('active'); });
            var tc = el('tab-' + tabId);
            if (!tc) tc = el(tabId);
            if (!tc) {
                tc = document.createElement('div');
                tc.id = 'tab-' + tabId;
                tc.className = 'tab-content';
                var area = el('contentArea') || document.body;
                area.appendChild(tc);
            }
            if (tc) tc.classList.add('active');
            el('pageTitle').textContent = (typeof PAGE_TITLES !== 'undefined' && PAGE_TITLES[tabId]) || tabId;
            if (tabId === 'alist-detail' || tabId === 'concept-detail' || tabId === 'minute-chart') return;
            try { localStorage.setItem('probiga_current_tab', tabId); } catch (e) {}
            loadTab(tabId);
        } catch (e) {
            console.error('[switchTab error]', e);
            setStatus('切换失败: ' + e.message, true);
        }
    };

    /* ===== 搜索+分页引擎 ===== */
    window.renderTable = function (container, tableId, cols, rows, renderFn, pageSize, stickyHtml) {
        pageSize = pageSize || 50;
        container.innerHTML = '';
        container.classList.add('paged-table-section');
        var head = tableHeadHtml(cols);
        var hiddenHead = tableHeadHtml(cols, 'paged-original-head');
        var panel = document.createElement('div'); panel.className = 'paged-sticky-panel';
        if (stickyHtml) panel.insertAdjacentHTML('beforeend', stickyHtml);
        var sb = document.createElement('div'); sb.className = 'search-bar paged-table-toolbar';
        sb.innerHTML = '<input type="text" id="s_' + tableId + '" placeholder="🔍 搜索..." oninput="doSearch(\'' + tableId + '\')"><span id="info_' + tableId + '"></span>';
        panel.appendChild(sb);
        var hw = document.createElement('div'); hw.className = 'paged-head-wrap'; hw.id = 'sh_' + tableId;
        hw.innerHTML = '<table class="paged-head-table">' + head + '</table>';
        panel.appendChild(hw);
        container.appendChild(panel);
        var tw = document.createElement('div'); tw.className = 'table-wrap paged-table-wrap';
        tw.id = 'tw_' + tableId;
        tw.setAttribute('data-table-id', tableId);
        tw.innerHTML = '<table class="paged-body-table">' + hiddenHead + '<tbody id="tb_' + tableId + '"></tbody></table>';
        container.appendChild(tw);
        var pg = document.createElement('div'); pg.className = 'pagination'; pg.id = 'pg_' + tableId;
        container.appendChild(pg);
        window['_r_' + tableId] = rows; window['_f_' + tableId] = renderFn;
        window['_p_' + tableId] = 1; window['_ps_' + tableId] = pageSize;
        initPagedSticky(tableId);
        doSearch(tableId);
    };

    window.doSearch = function (tid) {
        var kw = (el('s_' + tid) || { value: '' }).value.toLowerCase();
        var all = window['_r_' + tid] || [];
        var filtered = kw ? all.filter(function (r) { return JSON.stringify(r).toLowerCase().indexOf(kw) > -1; }) : all;
        window['_ft_' + tid] = filtered; window['_p_' + tid] = 1;
        renderPage(tid);
    };

    function renderPage(tid) {
        var p = window['_p_' + tid] || 1, ps = window['_ps_' + tid] || 50;
        var rows = window['_ft_' + tid] || window['_r_' + tid] || [];
        var total = rows.length, tp = Math.ceil(total / ps);
        var s = (p - 1) * ps, e = Math.min(s + ps, total);
        var tbody = el('tb_' + tid);
        if (tbody) tbody.innerHTML = rows.slice(s, e).map(function (r, i) { return window['_f_' + tid](r, s + i); }).join('');
        syncPagedHeader(tid);
        el('info_' + tid).textContent = '共 ' + total + ' 条 | ' + (total > 0 ? (s + 1) + '-' + e + '/' + total : '0');
        var pg = el('pg_' + tid);
        if (!pg || tp <= 1) { if (pg) pg.innerHTML = ''; return; }
        var h = '';
        for (var i = 1; i <= tp; i++) h += '<button class="' + (i === p ? 'active' : '') + '" onclick="window._gp(\'' + tid + '\',' + i + ')">' + i + '</button>';
        pg.innerHTML = h;
    }
    window._gp = function (tid, np) { window['_p_' + tid] = np; renderPage(tid); };

    /* ===== 分时链接 ===== */
    window.minuteBtn = function (code) {
        var c = (code || '').toString();
        return '<a href="javascript:void(0)" onclick="openKlineModal(\'' + c + '\',\'' + c + '\')" class="clickable-name">📈</a>';
    };

    /* ===== K线弹窗 ===== */
    window.openKlineModal = function (code, name) {
        var c = (code || '').toString();
        var url = 'https://quote.eastmoney.com/' + (c.startsWith('6') ? 'sh' : 'sz') + c + '.html#fullScreenChart';
        var d = el('datePicker').value;
        document.getElementById('klineModalTitle').textContent = '📈 ' + code + ' ' + (name || '') + '  |  行情日期: ' + d + '（请在图表中选择对应日期）';
        document.getElementById('klineIframe').src = url;
        document.getElementById('klineModal').classList.add('show');
    };
    window.closeKlineModal = function () {
        document.getElementById('klineModal').classList.remove('show');
        document.getElementById('klineIframe').src = '';
    };
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') { closeKlineModal(); closeAnalyzeModal(); closeStockDetailModal(); window.closeMainforceModal(); } });

    function card(lbl, val, cls) { return '<div class="stat-card"><div class="label">' + lbl + '</div><div class="value ' + (cls || '') + '">' + val + '</div></div>'; }
    function jsonF(v, def) { if (!v) return def; if (typeof v === 'object') return v; try { return JSON.parse(v); } catch(e) { return def; } }
    window.genReviewBtn = function(d) { fetch('/api/hot-data/daily-review/generate?review_date=' + d, {method:'POST'}).then(function(r){return r.json()}).then(function(res){alert('生成' + (res.status === 'success' ? '成功' : '失败') + '!');}); };
    window.exportReview = function(d) { fetch('/api/hot-data/daily-review/print?review_date='+d).then(function(r){return r.blob()}).then(function(b){var a=document.createElement('a'); a.href=URL.createObjectURL(b); a.download='review_'+d+'.html'; a.click(); URL.revokeObjectURL(a.href);}); };
    window.switchReviewTab = function(tab) {
        var basicEl = document.getElementById('review-basic');
        var proEl = document.getElementById('review-pro');
        var tabBasic = document.getElementById('tab-pro-basic');
        var tabPro = document.getElementById('tab-pro-pro');
        if (!basicEl || !proEl) return;
        if (tab === 'pro') {
            basicEl.style.display = 'none';
            proEl.style.display = 'block';
            if (tabBasic) { tabBasic.style.background = '#1e1e1e'; tabBasic.style.color = '#888'; }
            if (tabPro) { tabPro.style.background = '#2a2a3e'; tabPro.style.color = '#1a73e8'; tabPro.style.fontWeight = '600'; }
        } else {
            basicEl.style.display = 'block';
            proEl.style.display = 'none';
            if (tabBasic) { tabBasic.style.background = '#2a2a3e'; tabBasic.style.color = '#1a73e8'; tabBasic.style.fontWeight = '600'; }
            if (tabPro) { tabPro.style.background = '#1e1e1e'; tabPro.style.color = '#888'; tabPro.style.fontWeight = 'normal'; }
        }
    };
    function _renderProReview(text) {
        if (!text) return '';
        function inline(s) {
            return escHtml(s)
                .replace(/\*\*(.+?)\*\*/g, '<strong style="color:#f8fafc">$1</strong>')
                .replace(/&quot;([^&]+?)&quot;/g, '<span style="color:#4fc3f7">"$1"</span>');
        }
        return String(text).replace(/\r\n/g, '\n').split('\n').map(function (line) {
            var s = line.trim();
            var m;
            if (!s) return '<div style="height:10px"></div>';
            m = s.match(/^\u3010(.+?)\u3011$/);
            if (m) {
                return '<div style="font-size:18px;font-weight:800;color:#93c5fd;margin:0 0 18px;text-align:center">' + escHtml(m[1]) + '</div>';
            }
            m = s.match(/^(\d+)\.\s+(.+)$/);
            if (m) {
                return '<div style="font-size:15px;font-weight:800;color:#f8fafc;margin:18px 0 10px;border-bottom:1px solid rgba(148,163,184,.28);padding-bottom:6px">' + escHtml(m[1]) + '. ' + escHtml(m[2]) + '</div>';
            }
            m = s.match(/^[-*]\s+(.+)$/);
            if (m) {
                return '<div style="display:flex;gap:8px;margin:7px 0;color:#cbd5e1;line-height:1.75"><span style="color:#60a5fa;font-weight:900">•</span><span>' + inline(m[1]) + '</span></div>';
            }
            return '<div style="margin:7px 0;color:#cbd5e1;line-height:1.75">' + inline(s) + '</div>';
        }).join('');
        // 简单Markdown渲染：加粗、标题、列表
        return text
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/^【(.+?)】/gm, '<div style="font-size:18px;font-weight:700;color:#1a73e8;margin-bottom:16px;text-align:center">【$1】</div>')
            .replace(/^(\d+)\.\s+(.+)$/gm, '<div style="font-size:15px;font-weight:700;color:#e0e0e0;margin:16px 0 8px;border-bottom:1px solid #333;padding-bottom:4px">$1. $2</div>')
            .replace(/^- (.+)$/gm, '<div style="padding-left:16px;margin:4px 0;color:#bbb">• $1</div>')
            .replace(/\*\*(.+?)\*\*/g, '<strong style="color:#e0e0e0">$1</strong>')
            .replace(/"([^"]+)"/g, '<span style="color:#4fc3f7">"$1"</span>')
            .replace(/\n\n/g, '<br/>')
            .replace(/\n/g, '');
    }
    window.loadPortfolio = function(){ refreshLoadTab('portfolio'); };

    /* ===== 自选股 ===== */
    window.pfAdd = function() {
        var code = el('pfCode').value.trim();
        var price = parseFloat(el('pfPrice').value);
        var shares = parseInt(el('pfShares').value);
        var isTodayBuy = !!(el('pfTodayBuy') && el('pfTodayBuy').checked);
        if (!code) { alert('请输入股票代码'); return; }
        fetch('/api/portfolio/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({stock_code:code, cost_price:price||0, shares:shares||0, is_today_buy:isTodayBuy})})
        .then(function(r){return r.json()}).then(function(res){
            if (res.status === 'ok') { alert('添加成功: '+res.short_name); loadTab('portfolio'); }
            else alert('添加失败: '+res.error);
        });
    };
    window.pfAddWithCode = function(code) {
        if (!code) { alert('股票代码为空'); return; }
        if (!confirm('确认将 ' + code + ' 加入自选股？')) return;
        fetch('/api/portfolio/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({stock_code:code, cost_price:0, shares:0, is_today_buy:false})})
        .then(function(r){return r.json()}).then(function(res){
            if (res.status === 'ok') alert('添加成功: ' + (res.short_name || code));
            else alert('添加失败: ' + (res.error || '未知错误'));
        });
    };
    window.pfRemove = function(code) { if(confirm('确认删除?')) fetch('/api/portfolio/remove/'+code,{method:'DELETE'}).then(function(){loadTab('portfolio');}); };
    window.pfTransact = function(code, name, curCost, curShares) {
        document.getElementById('tradeModalTitle').textContent = '💰 交易 | ' + (name || code);
        document.getElementById('tradeCode').value = code;
        document.getElementById('tradeCost').textContent = Number(curCost||0).toFixed(4);
        document.getElementById('tradeShares').textContent = curShares || 0;
        document.getElementById('tradePrice').value = '';
        document.getElementById('tradeQty').value = '';
        var overlay = document.getElementById('tradeModal');
        if (overlay) overlay.classList.add('show');
        document.getElementById('tradePrice').focus();
    };
    window.pfExecTransact = function(transType) {
        var code = document.getElementById('tradeCode').value;
        var price = parseFloat(document.getElementById('tradePrice').value) || 0;
        var shares = parseInt(document.getElementById('tradeQty').value) || 0;
        if (!price || !shares) { alert('请输入有效价格和股数'); return; }
        fetch('/api/portfolio/transact/'+code, {method:'POST', headers:{'Content-Type':'application/json'},
            body:JSON.stringify({stock_code:code, trans_type:transType, price:price, shares:shares})})
        .then(function(r){return r.json()}).then(function(res){
            if (res.status !== 'ok') { alert('交易失败: '+(res.error||res.status)); return; }
            closeTradeModal();
            var newCost = Number(res.cost_price||0);
            var newShares = Number(res.shares||0);
            // Find the row in current table and update cells
            var row = document.getElementById('pf-tr-'+code);
            if (row) {
                 var curPriceEl = row.querySelector('.pf-cur-price');
                 var costEl = row.querySelector('.pf-cost');
                 var sharesEl = row.querySelector('.pf-shares');
                 var profitEl = row.querySelector('.pf-profit');
                 var pctEl = row.querySelector('.pf-profit-pct');
                 if (curPriceEl && costEl && sharesEl && profitEl && pctEl) {
                     costEl.textContent = fmtPrice(newCost);
                     sharesEl.textContent = newShares;
                     var curPrice = parseFloat(curPriceEl.textContent) || 0;
                     var profit = (curPrice - newCost) * newShares;
                      var hasProfitPct = newCost > 0;
                      var profitPct = hasProfitPct ? (curPrice / newCost - 1) * 100 : 0;
                     var cls = profit >= 0 ? 'c-red' : 'c-green';
                     var isH = newShares > 0;
                     profitEl.textContent = isH ? (profit>=0?'+':'')+profit.toFixed(2) : '-';
                     profitEl.className = 'pf-profit ' + (isH ? cls : 'c-gray');
                      pctEl.textContent = isH && hasProfitPct ? (profitPct>=0?'+':'')+profitPct.toFixed(2)+'%' : '-';
                      pctEl.className = 'pf-profit-pct ' + (isH && hasProfitPct ? cls : 'c-gray');
                     row.setAttribute('data-holding', isH ? '1' : '0');
                     row.style.background = isH ? '#fff4e6' : '';
                     // Update button params
                     var btnCell = row.querySelector('.pf-actions');
                     if (btnCell) {
                         var btns = btnCell.getElementsByTagName('button');
                         if (btns.length > 0) {
                             var nm = row.querySelector('strong');
                             var displayName = nm ? nm.textContent : '';
                             btns[0].setAttribute('onclick', "pfTransact('"+code+"','"+displayName+"',"+newCost+","+newShares+")");
                         }
                     }
                }
            } else {
                loadTab('portfolio');
            }
            fetch('/api/portfolio/list').then(function(r){return r.json()}).then(function(res){
                if (!res.data) return;
                var elTotal = document.getElementById('pfTotalProfit');
                if (elTotal && res.summary) {
                    var s = res.summary;
                    var fmt = function(v) {
                        var n = Number(v || 0);
                        var cls = n >= 0 ? 'c-red' : 'c-green';
                        return '<strong class="' + cls + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '</strong>';
                    };
                    elTotal.innerHTML = fmt(s.total_hold_profit);
                    var elToday = document.getElementById('pfTodayProfit');
                    if (elToday) elToday.innerHTML = fmt(s.today_hold_profit);
                    var elCnt = document.getElementById('pfHoldingCount');
                    if (elCnt) elCnt.textContent = String(s.holding_count != null ? s.holding_count : 0);
                }
                var row = res.data.filter(function(x){ return x.stock_code === code; })[0];
                if (row && window.pfUpdatePortfolioRow) window.pfUpdatePortfolioRow(row);
            });
        }).catch(function(e){ alert('请求失败，请重试'); });
    };
    window.closeTradeModal = function() { document.getElementById('tradeModal').classList.remove('show'); };
    window.refreshPfPrices = function() {
        var status = document.getElementById('pfLiveStatus');
        var btn = document.querySelector('button[onclick="refreshPfPrices()"]');
        var oldText = btn ? btn.textContent : '';
        var shouldForceLive = isTradingTime();
        if (status) status.textContent = shouldForceLive ? '同步实时行情...' : '同步收盘数据...';
        if (btn) {
            btn.disabled = true;
            btn.textContent = '同步中';
        }
        var refreshPromise = shouldForceLive
            ? fetch('/api/portfolio/refresh-prices', {method:'POST'}).then(function(r){return r.json()})
            : Promise.resolve({ status: 'ok', market_mode: 'post_close' });
        refreshPromise.then(function(res){
            if (res.status !== 'ok') throw new Error(res.error || res.message || '未知错误');
            if (window.pfApplyPortfolioLivePayload) {
                return fetch('/api/portfolio/live').then(function(r){return r.json()}).then(function(liveRes){
                    window.pfApplyPortfolioLivePayload(liveRes, shouldForceLive ? '已同步' : '收盘价');
                });
            }
            return refreshLoadTab('portfolio');
        }).catch(function(e){
            if (status) status.textContent = '刷新失败';
            alert('行情刷新失败: ' + (e.message || '网络请求异常'));
        }).finally(function(){
            if (btn) {
                btn.disabled = false;
                btn.textContent = oldText || '📡 同步行情';
            }
        });
    };
    window.savePfOrder = function() {
        var rows = document.querySelectorAll('#pfTable tbody tr[draggable]');
        var codes = [].map.call(rows, function(r){ return r.getAttribute('data-code'); });
        if (!codes.length) return;
        fetch('/api/portfolio/reorder', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({codes:codes})})
        .then(function(r){return r.json()}).then(function(res){
            if (res.status === 'ok') {
                var btn = document.querySelector('button[onclick="savePfOrder()"]');
                if (btn) { var orig = btn.textContent; btn.textContent = '✅ 已保存'; setTimeout(function(){ btn.textContent = orig; }, 1500); }
            } else alert('保存失败');
        });
    };
    function analysisValue(v) {
        return v == null || v === '' || isNaN(Number(v)) ? null : Number(v);
    }
    function firstAnalysisValue() {
        for (var i = 0; i < arguments.length; i++) {
            var n = analysisValue(arguments[i]);
            if (n != null) return n;
        }
        return null;
    }
    function blendedAnalysisRowScore(row) {
        row = row || {};
        var composite = null;
        var quality = analysisValue(row.quality_score);
        var entry = analysisValue(row.entry_score);
        if (quality != null && entry != null) composite = (quality + entry) / 2;
        var dualTrack = null;
        var shortScore = analysisValue(row.short_term_score);
        var longScore = analysisValue(row.long_term_score);
        if (shortScore != null && longScore != null) dualTrack = (shortScore + longScore) / 2;
        return firstAnalysisValue(
            row.final_trade_score,
            row.ai_score,
            composite,
            dualTrack,
            row.short_term_score,
            row.long_term_score,
            row.main_wave_score,
            row.trend_hold_score,
            row.entry_score,
            row.quality_score
        );
    }
    function analysisScoreText(v) {
        var n = analysisValue(v);
        return n == null ? '-' : n.toFixed(1);
    }
    function analysisStatusColor(status) {
        status = String(status || '').toUpperCase();
        if (status === 'ALLOW') return '#1b5e20';
        if (status === 'BLOCK') return '#b71c1c';
        if (status === 'SUSPENDED') return '#8d6e63';
        return '#546e7a';
    }
    function analysisRiskColor(level) {
        level = String(level || '').toUpperCase();
        if (level === 'LOW') return '#2e7d32';
        if (level === 'HIGH') return '#ef6c00';
        if (level === 'CRITICAL') return '#c62828';
        return '#546e7a';
    }
    function analysisActionColor(action) {
        action = String(action || '');
        if (!action) return '#546e7a';
        if (action.indexOf('减') >= 0 || action.indexOf('卖') >= 0 || action.indexOf('回避') >= 0 || action.indexOf('清') >= 0) return '#c62828';
        if (action.indexOf('加') >= 0 || action.indexOf('买') >= 0) return '#2e7d32';
        if (action.indexOf('持有') >= 0 || action.indexOf('关注') >= 0) return '#1565c0';
        return '#f39c12';
    }
    function analysisSummaryMeta(ai, snapshot, fallbackDate, recommendation) {
        ai = ai || {};
        snapshot = snapshot || {};
        recommendation = recommendation || {};
        var scores = ai.scores || {};
        return {
            source: ai.source || '',
            date: ai.analysis_date || snapshot.analysis_date || fallbackDate || '',
            score: analysisValue(ai.score),
            tradeScore: analysisValue(
                recommendation.recommendation_score != null
                    ? recommendation.recommendation_score
                    : recommendation.final_trade_score
            ),
            tradeScoreSource: recommendation.recommendation_score_source || '',
            recommendationDate: recommendation.pick_date || '',
            shortScore: analysisValue(scores.short_term_score != null ? scores.short_term_score : snapshot.short_term_score),
            longScore: analysisValue(scores.long_term_score != null ? scores.long_term_score : snapshot.long_term_score),
            eventScore: analysisValue(scores.event_risk_score != null ? scores.event_risk_score : snapshot.event_risk_score),
            qualityScore: analysisValue(scores.data_quality_score != null ? scores.data_quality_score : snapshot.data_quality_score),
            action: ai.action || '',
            actionReason: ai.action_reason || snapshot.recommend_reason || snapshot.recommendation || snapshot.summary || '',
            status: ai.recommend_status || snapshot.recommend_status || '',
            riskLevel: ai.event_risk_level || snapshot.event_risk_level || ''
        };
    }
    function isIsoDateOlder(a, b) {
        a = String(a || '').slice(0, 10);
        b = String(b || '').slice(0, 10);
        return !!(a && b && a < b);
    }
    function localizeAnalysisSource(source) {
        var text = String(source || '').trim();
        if (!text) return '';
        if (text === 'snapshot_fallback') return '快照回退';
        if (text === 'deepseek') return 'AI 深度分析';
        if (text === 'deepseek+snapshot') return 'AI+快照融合';
        return text;
    }
    function localizeRecommendStatus(status) {
        status = String(status || '').toUpperCase();
        if (status === 'ALLOW') return '可跟踪';
        if (status === 'BLOCK') return '回避';
        if (status === 'SUSPENDED') return '观察';
        return status;
    }
    function localizeRiskLevel(level) {
        level = String(level || '').toUpperCase();
        if (level === 'LOW') return '低';
        if (level === 'MEDIUM') return '中';
        if (level === 'HIGH') return '高';
        if (level === 'CRITICAL') return '极高';
        return level;
    }
    function localizeMachineText(text) {
        if (text == null || text === '') return '';
        var raw = String(text);
        var exact = {
            BUY_READY: '买入就绪',
            SELL_ALERT: '卖出提醒',
            SUSPENDED: '观察',
            CONFIRM: '确认',
            WATCH: '观察',
            ALLOW: '可跟踪',
            BLOCK: '回避',
            BUY: '买入',
            SELL: '卖出',
            PASS: '通过',
            RISK: '风险',
            CRITICAL: '极高',
            MEDIUM: '中',
            HIGH: '高',
            LOW: '低',
            NONE: '-',
            NEW: '新信号',
            ORDERED: '已下单',
            PENDING: '待成交',
            FILLED: '已成交',
            REJECTED: '已拒绝',
            EXPIRED: '已过期',
            RISK_BLOCKED: '风控阻断',
            entry: '入场',
            exit: '出场',
            observe: '观察',
            closed: '休市',
            t: 'T窗口',
            ultra_short: '超短',
            short_term: '短线',
            swing: '波段',
            main_wave: '主升浪',
            missing_hot_rank: '缺少热度榜数据',
            turnover_out_of_range: '换手率不在策略区间',
            roe_below_threshold: 'ROE低于阈值',
            gross_margin_below_threshold: '毛利率低于阈值',
            trend_break: '趋势破位',
            capital_outflow: '资金流出',
            sector_weak: '板块偏弱',
            theme_continuity_low: '题材延续性偏低',
            chasing_risk: '追高风险',
            weekly_overheat: '周线过热',
            event_priced_in: '利好已反映',
            holder_spread: '筹码分散',
            institutional_outflow: '机构流出',
            margin_deleveraging: '两融去杠杆',
            unlock_risk: '解禁风险',
            unlock_watch: '小额解禁观察',
            pledge_risk: '质押风险',
            shareholder_reduction: '股东减持',
            mine_clearance_risk: '财务/监管扫雷风险',
            market_extreme_overheat: '市场极度过热',
            valuation_expensive: '估值偏贵',
            goodwill_risk: '商誉风险',
            style_mismatch: '风格不匹配',
            north_flow_pressure: '北向资金压力',
            macro_policy_pressure: '宏观/政策压力',
            macro_indicator_pressure: '宏观数据压力',
            etf_flow_pressure: 'ETF资金压力',
            north_stock_weak: '北向个股偏弱',
            institutional_profile_weak: '机构画像偏弱',
            investor_interaction_risk: '互动问答风险',
            retail_institution_contrarian_risk: '散户与机构背离风险',
            business_purity_low: '业务纯度偏低',
            industry_prosperity_weak: '行业景气偏弱',
            classic_pattern_risk: '经典形态风险',
            relative_weak: '相对市场偏弱',
            liquidity_risk: '流动性风险',
            float_market_cap_low: '流通市值偏低',
            volume_overheat: '量能过热',
            volume_shrink: '缩量偏弱',
            fundamental_weak: '基本面偏弱',
            event_risk: '事件风险',
            risk_reward_low: '盈亏比偏低',
            review_loss_10d: '10日复盘亏损',
            review_loss_5d: '5日复盘亏损',
            review_loss_3d: '3日复盘亏损'
        };
        var trimmed = raw.trim();
        if (exact[trimmed]) return exact[trimmed];
        return raw
            .replace(/\bBUY_READY\b/g, '买入就绪')
            .replace(/\bSELL_ALERT\b/g, '卖出提醒')
            .replace(/\bSUSPENDED\b/g, '观察')
            .replace(/\bCONFIRM\b/g, '确认')
            .replace(/\bWATCH\b/g, '观察')
            .replace(/\bALLOW\b/g, '可跟踪')
            .replace(/\bBLOCK\b/g, '回避')
            .replace(/\bBUY\b/g, '买入')
            .replace(/\bSELL\b/g, '卖出')
            .replace(/\bPASS\b/g, '通过')
            .replace(/\bCRITICAL\b/g, '极高')
            .replace(/\bMEDIUM\b/g, '中')
            .replace(/\bHIGH\b/g, '高')
            .replace(/\bLOW\b/g, '低')
            .replace(/\bNEW\b/g, '新信号')
            .replace(/\bORDERED\b/g, '已下单')
            .replace(/\bPENDING\b/g, '待成交')
            .replace(/\bFILLED\b/g, '已成交')
            .replace(/\bultra_short\b/g, '超短')
            .replace(/\bshort_term\b/g, '短线')
            .replace(/\bswing\b/g, '波段')
            .replace(/\bmain_wave\b/g, '主升浪')
            .replace(/\bmissing_hot_rank\b/g, '缺少热度榜数据')
            .replace(/\bturnover_out_of_range\b/g, '换手率不在策略区间')
            .replace(/\broe_below_threshold\b/g, 'ROE低于阈值')
            .replace(/\bgross_margin_below_threshold\b/g, '毛利率低于阈值')
            .replace(/\bliquidity_risk\b/g, '流动性风险')
            .replace(/\bfundamental_weak\b/g, '基本面偏弱')
            .replace(/\bcapital_outflow\b/g, '资金流出')
            .replace(/\bsector_weak\b/g, '板块偏弱')
            .replace(/\bchasing_risk\b/g, '追高风险')
            .replace(/\bvaluation_expensive\b/g, '估值偏贵')
            .replace(/\bevent_risk\b/g, '事件风险')
            .replace(/\bAI signal status is ([A-Z_]+); waiting for confirmation\b/g, 'AI信号状态为$1，等待确认')
            .replace(/\bAI signal status is ([^;；]+); waiting for confirmation\b/g, 'AI信号状态为$1，等待确认')
            .replace(/\bAI signal status ([A-Z_]+)\b/g, 'AI信号状态$1')
            .replace(/\bAI signal status ([^;；]+)/g, 'AI信号状态$1')
            .replace(/\bfinal trade score or entry score has not reached confirm threshold\b/g, '最终交易分或买点分尚未达到确认阈值')
            .replace(/\bentry score ([0-9.]+) is weak; good stock but poor buy point\b/g, '买点分$1偏弱，股票可以观察但当前买点不好')
            .replace(/\bexpected upside ([0-9.]+)% is below 5% threshold\b/g, '预期上涨空间$1%低于5%门槛')
            .replace(/\brisk\/reward ([0-9.]+):1 is below ([0-9.]+):1 threshold\b/g, '盈亏比$1:1低于$2:1门槛')
            .replace(/\bsector gate is 回避; board-first rule failed\b/g, '板块门禁回避，板块先行规则未通过')
            .replace(/\bsector gate is BLOCK; board-first rule failed\b/g, '板块门禁回避，板块先行规则未通过')
            .replace(/\bblocked by base recommendation gate\b/g, '基础推荐门禁阻断')
            .replace(/\bbase status is ([A-Z_]+); 主升浪 candidate should wait for tradable pullback\b/g, '基础状态为$1，主升浪候选需等待可交易回踩')
            .replace(/\bbase status is ([^;；]+); 主升浪 candidate should wait for tradable pullback\b/g, '基础状态为$1，主升浪候选需等待可交易回踩')
            .replace(/\bbase status is ([A-Z_]+)\b/g, '基础状态为$1')
            .replace(/\bbase status is ([^;；]+)/g, '基础状态为$1')
            .replace(/\bcooldown active for ([0-9.]+) more days\b/g, '冷却期还剩$1天')
            .replace(/\bnear daily limit-up; wait for tradable pullback\b/g, '接近涨停，等待可交易回踩')
            .replace(/\bliquidity is below intraday trading threshold\b/g, '流动性低于盘中交易门槛')
            .replace(/\bmarket mood is weak\b/g, '市场情绪偏弱')
            .replace(/\bheat overload is high; avoid becoming exit liquidity\b/g, '热度拥挤偏高，避免成为接盘资金')
            .replace(/\brecent recommendation score is unstable\b/g, '近期推荐分不稳定')
            .replace(/\brecent failure samples require caution\b/g, '近期失败样本较多，需要谨慎')
            .replace(/\bmain-wave buy ready: score ([0-9.]+), hold ([0-9.]+);/g, '主升浪买点就绪：评分$1，持有分$2；')
            .replace(/\b([a-z_]+) final ([0-9.]+) confirms candidate \(quality ([0-9.]+), entry ([0-9.]+)\); wait for intraday trigger\b/g, '$1最终分$2确认候选（质量$3，买点$4），等待盘中触发')
            .replace(/\b(超短|短线|波段|主升浪) final ([0-9.]+) confirms candidate \(quality ([0-9.]+), entry ([0-9.]+)\); wait for intraday trigger\b/g, '$1最终分$2确认候选（质量$3，买点$4），等待盘中触发')
            .replace(/\bwait for pullback\/volume confirmation\b/g, '等待回踩或量能确认')
            .replace(/\blive quote is fresh\b/g, '实时行情新鲜')
            .replace(/\bprice stays near VWAP\/MA5 or breaks intraday high\b/g, '价格贴近VWAP/MA5或突破盘中新高')
            .replace(/\bmain capital flow remains positive\b/g, '主力资金保持净流入')
            .replace(/\bdo not chase limit-up or extended gap\b/g, '不追涨停或大幅跳空')
            .replace(/\bmain wave score confirms breakout or trend continuation\b/g, '主升浪评分确认突破或趋势延续')
            .replace(/\bprefer pullback holding MA5\/MA10 instead of chasing a limit-up candle\b/g, '优先等回踩守住MA5/MA10，不追涨停长阳')
            .replace(/\bvolume remains above the 20-day average but is not a blow-off spike\b/g, '成交量高于20日均量但没有爆量失控')
            .replace(/\bsector rotation score stays strong and trend hold score does not deteriorate\b/g, '板块轮动分保持强势，趋势持有分未走坏')
            .replace(/\bprice holds MA20\/MA60 support\b/g, '价格守住MA20/MA60支撑')
            .replace(/\bmedium trend remains upward\b/g, '中期趋势仍向上')
            .replace(/\bevent risk stays 低\b/g, '事件风险保持低位')
            .replace(/\bmarket breadth is not sharply weakening\b/g, '市场宽度没有明显转弱')
            .replace(/\bpullback holds MA5\/MA10 or breaks 20-day platform\b/g, '回踩守住MA5/MA10，或突破20日平台')
            .replace(/\bvolume remains above recent average\b/g, '量能保持在近期均量之上')
            .replace(/\bcapital flow does not reverse\b/g, '资金流没有反转走弱')
            .replace(/\bavoid chasing after sharp daily jump\b/g, '避免日内大涨后追高')
            .replace(/\bstop loss below ([0-9.]+)%\b/g, '跌破$1%止损')
            .replace(/\btake profit levels ([0-9.]+)%\/([0-9.]+)%\b/g, '止盈参考$1%/$2%')
            .replace(/\bmax holding ([0-9]+) trading days\b/g, '最长持有$1个交易日')
            .replace(/\bposition risk ([A-Z_]+), single-stock cap <= ([0-9.]+)%\b/g, '仓位风险$1，单票上限不超过$2%')
            .replace(/\bposition risk ([^,，]+), single-stock cap <= ([0-9.]+)%\b/g, '仓位风险$1，单票上限不超过$2%')
            .replace(/\bdo not exit only because fixed profit target is reached\b/g, '主升浪不因固定止盈位单独卖出')
            .replace(/\breduce when distance from MA20 is excessive and cumulative wave gain is high\b/g, '偏离MA20过大且波段涨幅高时减仓')
            .replace(/\bsell alert when price closes below MA20 after a main-wave advance\b/g, '主升后收盘跌破MA20触发卖出提醒')
            .replace(/\btrend stop reference ([^;]+)/g, '趋势止损参考 $1')
            .replace(/\bstrict gate failed:?/g, '严格门禁未通过：')
            .replace(/\bdata not ready\b/g, '数据未就绪')
            .replace(/\boffline recommendation job is running\b/g, '离线推荐任务运行中')
            .replace(/\boffline recommendation job is queued\b/g, '离线推荐任务排队中')
            .replace(/\bAI recommendation is not available\b/g, 'AI 推荐暂不可用')
            .replace(/\bmarket_closed\b/g, '市场已收盘')
            .replace(/\bmarket_refresh_running\b/g, '行情刷新正在运行')
            .replace(/\bBUY_READY\b/g, '买入就绪')
            .replace(/\bSELL_ALERT\b/g, '卖出提醒')
            .replace(/\bSUSPENDED\b/g, '观察')
            .replace(/\bCONFIRM\b/g, '确认')
            .replace(/\bWATCH\b/g, '观察')
            .replace(/\bALLOW\b/g, '可跟踪')
            .replace(/\bBLOCK\b/g, '回避')
            .replace(/\bCRITICAL\b/g, '极高')
            .replace(/\bMEDIUM\b/g, '中')
            .replace(/\bHIGH\b/g, '高')
            .replace(/\bLOW\b/g, '低');
    }
    function buildStockDetailMeta(detail, analysisMeta) {
        detail = detail || {};
        analysisMeta = analysisMeta || {};
        var recommendationDate = (window._recLastData && window._recLastData.date) || '';
        var referenceDate = recommendationDate || detail.requested_trade_date || window._recLastDate || '';
        var quoteDate = detail.quote_trade_date || detail.date || '';
        var flowDate = detail.flow_trade_date || '';
        var analysisDate = detail.analysis_trade_date || analysisMeta.date || '';
        var chips = [];
        var notes = [];

        if (detail.data_mode_label || detail.mode) chips.push({ label: '行情模式', value: detail.data_mode_label || detail.mode });
        if (referenceDate) chips.push({ label: '推荐数据截至', value: referenceDate });
        if (quoteDate) chips.push({ label: '行情快照', value: quoteDate });
        if (flowDate && flowDate !== quoteDate) chips.push({ label: '资金流日期', value: flowDate });
        if (analysisDate) chips.push({ label: '分析日期', value: analysisDate });

        if (detail.quote_is_stale || isIsoDateOlder(quoteDate, referenceDate)) {
            notes.push('当前详情行情不是推荐页同日快照，请不要按实时信号理解。');
        }
        if (detail.flow_is_stale || isIsoDateOlder(flowDate, referenceDate)) {
            notes.push('资金流日期与推荐页不一致，资金面更适合做辅助判断。');
        }
        if (detail.analysis_is_stale || isIsoDateOlder(analysisDate, referenceDate)) {
            notes.push('综合分析基于更早一期分析快照，不代表盘中实时观点。');
        } else if (analysisDate && quoteDate && isIsoDateOlder(analysisDate, quoteDate)) {
            notes.push('分析结论早于行情快照日期，请结合最新行情自行校验。');
        }
        if (!notes.length && detail.detail_source === 'snapshot_light') {
            notes.push('当前详情使用快照加速模式，优先保证打开速度和稳定性。');
        }

        if (!chips.length && !notes.length) return '';

        var html = '<div style="background:linear-gradient(135deg,#fffaf0,#f8fbff);border:1px solid #e6edf7;border-radius:12px;padding:12px 14px;margin-bottom:14px">';
        html += '<div style="font-size:13px;font-weight:700;color:#334155;margin-bottom:8px">数据说明</div>';
        html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:' + (notes.length ? '10px' : '0') + '">';
        chips.forEach(function (item) {
            html += '<span style="display:inline-flex;align-items:center;gap:6px;background:#fff;border:1px solid #dde6f2;border-radius:999px;padding:5px 10px;font-size:12px;color:#334155">';
            html += '<strong style="color:#0f172a">' + item.label + '</strong><span>' + item.value + '</span></span>';
        });
        html += '</div>';
        if (notes.length) {
            html += '<div style="font-size:12px;line-height:1.7;color:#7c2d12;background:rgba(245,158,11,.08);border-radius:8px;padding:8px 10px">' + notes.join('<br>') + '</div>';
        }
        html += '</div>';
        return html;
    }
    function renderAnalysisSummaryCard(meta, title) {
        meta = meta || {};
        if (!(meta.status || meta.riskLevel || meta.score != null || meta.tradeScore != null || meta.action || meta.date || meta.actionReason)) return '';
        var html = '<div style="background:#fff;border-radius:12px;padding:16px;margin-bottom:14px;box-shadow:0 1px 6px rgba(0,0,0,.05);border:1px solid #eef2f7">';
        html += '<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap">';
        html += '<div style="font-size:15px;font-weight:700;color:#1f2937">' + (title || '综合分析') + '</div>';
        html += '<div style="display:flex;gap:8px;flex-wrap:wrap">';
        if (meta.status) html += '<span style="background:' + analysisStatusColor(meta.status) + ';color:#fff;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700">' + localizeRecommendStatus(meta.status) + '</span>';
        if (meta.riskLevel) html += '<span style="background:' + analysisRiskColor(meta.riskLevel) + ';color:#fff;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700">风险 ' + localizeRiskLevel(meta.riskLevel) + '</span>';
        if (meta.action) html += '<span style="background:' + analysisActionColor(meta.action) + ';color:#fff;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700">' + meta.action + '</span>';
        if (meta.date) html += '<span style="background:#eef2ff;color:#3949ab;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:600">' + meta.date + '</span>';
        if (meta.recommendationDate && meta.recommendationDate !== meta.date) html += '<span style="background:#fff7ed;color:#9a3412;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:600">rec ' + meta.recommendationDate + '</span>';
        if (meta.source) html += '<span style="background:#f5f5f5;color:#455a64;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:600">' + localizeAnalysisSource(meta.source) + '</span>';
        html += '</div></div>';
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(82px,1fr));gap:8px;margin-bottom:' + (meta.actionReason ? '12px' : '0') + '">';
        [
            { label: '交易分', value: meta.tradeScore },
            { label: '综合', value: meta.score },
            { label: '短线', value: meta.shortScore },
            { label: '长线', value: meta.longScore },
            { label: '事件', value: meta.eventScore },
            { label: '质量', value: meta.qualityScore }
        ].forEach(function (item) {
            html += '<div style="background:#f8fafc;border-radius:8px;padding:10px 8px;text-align:center">';
            html += '<div style="font-size:11px;color:#64748b;margin-bottom:4px">' + item.label + '</div>';
            html += '<div style="font-size:16px;font-weight:700;color:#0f172a">' + analysisScoreText(item.value) + '</div>';
            html += '</div>';
        });
        html += '</div>';
        if (meta.actionReason) html += '<div style="font-size:13px;line-height:1.7;color:#374151">' + localizeMachineText(meta.actionReason) + '</div>';
        html += '</div>';
        return html;
    }

    function pfAnalysisInlineHtml(text) {
        var html = escHtml(text || '');
        html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
        html = html.replace(/不建议买入|不宜追高|注意风险|禁止推荐|暂停推荐/g, function(word) {
            return '<span class="pf-analysis-danger">' + word + '</span>';
        });
        html = html.replace(/持有|加仓|减仓|清仓|买入|卖出|观望/g, function(word) {
            return '<span class="pf-analysis-action">' + word + '</span>';
        });
        html = html.replace(/(\d+\.?\d*亿)/g, '<span class="pf-analysis-money">$1</span>');
        html = html.replace(/(第\d+名)/g, '<span class="pf-analysis-rank">$1</span>');
        return html;
    }
    function pfRenderAnalysisText(text) {
        var raw = String(text || '').replace(/\r\n/g, '\n').trim();
        if (!raw) return '<div class="pf-analysis-empty">暂无分析内容</div>';
        var html = '';
        raw.split(/\n+/).forEach(function(line) {
            line = line.trim();
            if (!line) return;
            var title = '';
            var rest = '';
            var m = line.match(/^###\s*(.+)$/);
            if (m) {
                title = m[1].replace(/[：:]\s*$/, '').trim();
            } else {
                m = line.match(/^(趋势判断|资金态度|热度评估|操作建议|风险提示)[：:]\s*(.*)$/);
                if (m) {
                    title = m[1];
                    rest = m[2] || '';
                }
            }
            if (title) {
                var cls = title === '操作建议' ? ' pf-analysis-section-action' : '';
                html += '<div class="pf-analysis-section-title' + cls + '">' + escHtml(title) + '</div>';
                if (rest) html += '<p>' + pfAnalysisInlineHtml(rest) + '</p>';
            } else {
                html += '<p>' + pfAnalysisInlineHtml(line) + '</p>';
            }
        });
        return html;
    }
    function pfAnalysisTimeText(res) {
        if (res && res.analysis_time) return res.analysis_time;
        var d = new Date();
        return pad2(d.getMonth() + 1) + '月' + pad2(d.getDate()) + '日 ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes());
    }
    function pfAnalysisPriceText(res) {
        res = res || {};
        var price = res.cur_price != null ? res.cur_price : (res.holding ? res.holding.cur_price : null);
        var change = res.change_pct;
        if (price == null && (!res.holding || res.holding.cur_price == null)) return res.data_mode_label || '-';
        return '现价 ' + fmtPrice(price) + ' ' + pct(change);
    }
    window.pfAnalyze = function(code, name) {
        var titleEl = document.getElementById('analyzeModalTitle');
        if (titleEl) titleEl.textContent = '🤖 AI 分析 | ' + (name || code);
        var bodyEl = document.getElementById('analyzeModalBody');
        if (bodyEl) bodyEl.innerHTML = '<div style="text-align:center;padding:30px;color:#888"><span class="spinner"></span> 分析中...</div>';
        var overlay = document.getElementById('analyzeModal');
        if (overlay) overlay.classList.add('show');
        fetch('/api/portfolio/analyze/'+code).then(function(r){return r.json()}).then(function(res){
            var b = document.getElementById('analyzeModalBody');
            if (b) {
                var analysis = res.analysis || res.error || '分析失败';
                var isErr = !!res.error;
                var html = '<div class="pf-analysis-legacy">';
                html += '<div class="pf-analysis-topline"><span>🧾 ' + escHtml(pfAnalysisTimeText(res)) + '</span><span>' + escHtml(pfAnalysisPriceText(res)) + '</span></div>';

                // 分析正文
                if (isErr) {
                    html += '<div class="pf-analysis-error">' + escHtml(analysis) + '</div>';
                } else {
                    html += '<div class="pf-analysis-text">' + pfRenderAnalysisText(analysis) + '</div>';
                }

                html += '<div class="pf-analysis-footer">';
                html += '<button onclick="closeAnalyzeModal()">关闭</button>';
                html += '</div>';
                html += '</div>';

                b.innerHTML = html;
            }
        });
    };
    window.closeAnalyzeModal = function() { document.getElementById('analyzeModal').classList.remove('show'); };
    window.pfHistory = function(code, name) {
        document.getElementById('historyModalTitle').textContent = '📋 历史分析 | ' + (name || code);
        document.getElementById('historyModalBody').innerHTML = '<div style="text-align:center;padding:30px;color:#888"><span class="spinner"></span> 加载中...</div>';
        document.getElementById('historyModal').classList.add('show');
        fetch('/api/portfolio/analysis-history/'+code).then(function(r){return r.json()}).then(function(res){
            var b = document.getElementById('historyModalBody');
            if (!res.history || !res.history.length) {
                b.innerHTML = '<div style="text-align:center;padding:40px;color:#888">暂无历史分析记录<br><span style="font-size:12px">点击"分析"按钮生成第一条</span></div>';
                return;
            }
            var html = '';
            res.history.forEach(function(h, i){
                var chgCls = h.change_pct>=0?'c-red':'c-green';
                html += '<div style="padding:12px 0;border-bottom:1px solid #2a2a2a">' +
                    '<div style="display:flex;justify-content:space-between;margin-bottom:6px">' +
                    '<span style="color:#888;font-size:12px">📅 '+h.analysis_time+'</span>' +
                    '<span style="font-size:12px">现价 <strong>'+fmtPrice(h.cur_price)+'</strong> ' +
                    '<span class="'+chgCls+'">'+(Number(h.change_pct||0)>=0?'+':'')+Number(h.change_pct||0).toFixed(2)+'%</span></span>' +
                    '</div>' +
                    '<div style="font-size:13px;line-height:1.7;color:#ccc">' +
                    h.analysis_text.replace(/\*\*([^*]+)\*\*/g,'<strong>$1</strong>').replace(/\n/g,'<br>') +
                    '</div>' +
                    '</div>';
            });
            b.innerHTML = html;
        });
    };
    window.closeHistoryModal = function() { document.getElementById('historyModal').classList.remove('show'); };
    function pfWatchStore() {
        if (!window._pfWatchRows) window._pfWatchRows = {};
        return window._pfWatchRows;
    }
    function pfRegisterWatchRow(r) {
        if (!r || !r.stock_code) return;
        pfWatchStore()[String(r.stock_code)] = r;
    }
    function pfWatchFlowRatioText(a) {
        if (!a || a.funds_ratio == null || a.funds_ratio === '' || isNaN(Number(a.funds_ratio))) return '';
        return ' / 占比 ' + Number(a.funds_ratio).toFixed(1) + '%';
    }
    function pfWatchMomentText(r, a) {
        var parts = [];
        if (a && a.freshness) parts.push(a.freshness);
        var fundsTime = (a && a.funds_latest_time) || r.flow_latest_time || r.flow_trade_date || '';
        var quoteTime = r.quote_snapshot_at || r.snapshot_at || r.quote_trade_date || '';
        if (fundsTime) parts.push('资金 ' + shortDateTimeText(fundsTime));
        if (quoteTime) parts.push('行情 ' + shortDateTimeText(quoteTime));
        return parts.join(' / ') || '-';
    }
    function pfWatchAdviceLines(r) {
        r = r || {};
        var a = r.watch_analysis || {};
        var guard = a.drawdown_guard || {};
        var flow = (a.funds || r.flow_attitude_label || '-') + pfWatchFlowRatioText(a);
        var lines = [
            '操作建议：' + (a.operation_advice || '-'),
            '盘面依据：趋势 ' + (a.trend || '-') + '，资金 ' + flow + '，热度 ' + (a.heat || '-') + (a.heat_score != null ? '（' + a.heat_score + '）' : '') + '。',
            '风险提示：' + (a.risk_tip || '暂无明显') + '。'
        ];
        if (guard.action || guard.reason || guard.stop_loss_line || guard.reduce_line) {
            var guardLine = '回撤守门：' + (guard.action || '-');
            if (guard.reason) guardLine += '，' + guard.reason;
            if (guard.stop_loss_line) guardLine += '；止损线 ' + fmtPrice(guard.stop_loss_line);
            if (guard.reduce_line) guardLine += '；减仓观察线 ' + fmtPrice(guard.reduce_line);
            lines.push(guardLine + '。');
        }
        lines.push('数据时刻：' + pfWatchMomentText(r, a) + '。');
        return lines;
    }
    function pfWatchGuardClass(level) {
        level = String(level || 'LOW').toUpperCase();
        if (level === 'HIGH') return 'pf-watch-level-high';
        if (level === 'MEDIUM') return 'pf-watch-level-medium';
        if (level === 'DATA') return 'pf-watch-level-data';
        return 'pf-watch-level-low';
    }
    function pfWatchDetailCard(label, value, meta, className) {
        return '<div class="pf-watch-detail-card ' + (className || '') + '">' +
            '<div class="pf-watch-detail-label">' + escHtml(label) + '</div>' +
            '<div class="pf-watch-detail-value">' + escHtml(value || '-') + '</div>' +
            (meta ? '<div class="pf-watch-detail-meta">' + escHtml(meta) + '</div>' : '') +
            '</div>';
    }
    function pfWatchToneClass(tone) {
        tone = String(tone || 'neutral').toLowerCase();
        if (tone === 'good' || tone === 'bad' || tone === 'muted') return 'pf-watch-tone-' + tone;
        return 'pf-watch-tone-neutral';
    }
    function pfWatchConfidenceClass(score) {
        score = Number(score);
        if (isNaN(score)) return 'pf-watch-confidence-low';
        if (score >= 75) return 'pf-watch-confidence-high';
        if (score >= 55) return 'pf-watch-confidence-mid';
        return 'pf-watch-confidence-low';
    }
    function pfWatchConfidenceText(a) {
        var c = (a && a.confidence) || {};
        var score = Number(c.score);
        if (isNaN(score)) return { score: '-', label: c.label || '待确认', className: 'pf-watch-confidence-low' };
        return { score: Math.round(score) + '/100', label: c.label || (score >= 75 ? '高' : (score >= 55 ? '中' : '低')), className: pfWatchConfidenceClass(score) };
    }
    function pfWatchTrustItem(label, value, meta, className) {
        return '<div class="pf-watch-trust-item ' + (className || '') + '">' +
            '<span>' + escHtml(label) + '</span>' +
            '<strong>' + escHtml(value || '-') + '</strong>' +
            (meta ? '<em>' + escHtml(meta) + '</em>' : '') +
            '</div>';
    }
    function pfWatchSection(title, html, className) {
        return '<section class="pf-watch-section ' + (className || '') + '">' +
            '<div class="pf-watch-section-title">' + escHtml(title) + '</div>' +
            html +
            '</section>';
    }
    function pfWatchEvidenceHtml(items) {
        items = Array.isArray(items) ? items : [];
        if (!items.length) {
            return '<div class="pf-watch-empty">暂无可展开的证据项，先按摘要和数据时刻复核。</div>';
        }
        var html = '<div class="pf-watch-evidence-grid">';
        items.forEach(function(item) {
            item = item || {};
            html += '<div class="pf-watch-evidence-card ' + pfWatchToneClass(item.tone) + '">' +
                '<div class="pf-watch-evidence-label">' + escHtml(item.label || '-') + '</div>' +
                '<div class="pf-watch-evidence-value">' + escHtml(item.value || '-') + '</div>' +
                '<div class="pf-watch-evidence-explain">' + escHtml(item.explain || '') + '</div>' +
                '</div>';
        });
        html += '</div>';
        return html;
    }
    function pfWatchListHtml(items, ordered) {
        items = Array.isArray(items) ? items.filter(function(item) { return item != null && String(item).trim() !== ''; }) : [];
        if (!items.length) return '<div class="pf-watch-empty">暂无</div>';
        var tag = ordered ? 'ol' : 'ul';
        var html = '<' + tag + ' class="pf-watch-list">';
        items.forEach(function(item) {
            html += '<li>' + escHtml(item) + '</li>';
        });
        html += '</' + tag + '>';
        return html;
    }
    function pfWatchStatusText(status) {
        status = String(status || '').toLowerCase();
        if (status === 'fresh') return '实时/新鲜';
        if (status === 'closed') return '收盘';
        if (status === 'previous_close') return '上一收盘';
        if (status === 'stale') return '滞后';
        if (status === 'missing') return '缺失';
        return status || '-';
    }
    function pfWatchDataQualityHtml(a, row) {
        a = a || {};
        row = row || {};
        var dq = a.data_quality || {};
        var flags = Array.isArray(dq.flags) ? dq.flags : [];
        var html = '<div class="pf-watch-data-quality">';
        html += pfWatchTrustItem('行情状态', pfWatchStatusText(dq.quote_status || row.quote_status), dq.quote_time || row.quote_snapshot_at || row.snapshot_at || row.quote_trade_date || '');
        html += pfWatchTrustItem('资金状态', pfWatchStatusText(dq.flow_status || row.flow_status), dq.flow_time || a.funds_latest_time || row.flow_latest_time || row.flow_trade_date || '');
        html += pfWatchTrustItem('资金口径', a.funds_source_label || '-', a.funds_age_seconds != null ? '延迟约 ' + Math.round(Number(a.funds_age_seconds)) + ' 秒' : '');
        html += '</div>';
        if (flags.length) {
            html += '<div class="pf-watch-flags">';
            flags.forEach(function(flag) {
                html += '<span class="pf-watch-flag">' + escHtml(flag) + '</span>';
            });
            html += '</div>';
        }
        return html;
    }
    function ensurePfWatchModal() {
        var modal = document.getElementById('pfWatchModal');
        if (modal) return modal;
        modal = document.createElement('div');
        modal.className = 'modal-overlay';
        modal.id = 'pfWatchModal';
        modal.onclick = function(e) { if (e.target === modal) window.closePfWatchModal(); };
        modal.innerHTML = '<div class="modal-box pf-watch-modal-box">' +
            '<div class="modal-header">' +
            '<span class="modal-title" id="pfWatchModalTitle">盯盘建议</span>' +
            '<button class="modal-close" onclick="closePfWatchModal()">×</button>' +
            '</div>' +
            '<div class="modal-body pf-watch-modal-body" id="pfWatchModalBody"></div>' +
            '</div>';
        document.body.appendChild(modal);
        return modal;
    }
    window.pfOpenWatchAdvice = function(code) {
        var row = pfWatchStore()[String(code)] || {};
        var a = row.watch_analysis || {};
        var guard = a.drawdown_guard || {};
        var modal = ensurePfWatchModal();
        var title = document.getElementById('pfWatchModalTitle');
        var body = document.getElementById('pfWatchModalBody');
        var guardClass = pfWatchGuardClass(guard.level);
        var flowMeta = (a.funds_source_label || '-') + pfWatchFlowRatioText(a);
        var positionMeta = '持有 ' + (row.shares || 0) + ' 股';
        var confidence = pfWatchConfidenceText(a);
        var dq = a.data_quality || {};
        if (row.cost_price != null && Number(row.cost_price || 0) > 0) positionMeta += ' / 成本 ' + fmtPrice(row.cost_price);
        if (title) title.textContent = '盯盘建议 | ' + (row.display_name || code) + ' ' + code;
        if (body) {
            var html = '<div class="pf-watch-modal-head ' + guardClass + '">' +
                '<div><div class="pf-watch-modal-kicker">操作建议</div>' +
                '<div class="pf-watch-modal-advice">' + escHtml(a.operation_advice || '-') + '</div>' +
                '<div class="pf-watch-modal-sub">用价格、资金、热度、持仓和风险规则共同判断</div></div>' +
                '<div class="pf-watch-confidence ' + confidence.className + '">' +
                '<strong>' + escHtml(confidence.score) + '</strong>' +
                '<span>可信度 ' + escHtml(confidence.label) + '</span>' +
                '</div>' +
                '</div>';
            html += '<div class="pf-watch-trust-grid">' +
                pfWatchTrustItem('数据时刻', pfWatchMomentText(row, a), '行情和资金的最近可用时间') +
                pfWatchTrustItem('数据质量', dq.label || '-', (Array.isArray(dq.flags) ? dq.flags.slice(0, 2).join('；') : '')) +
                pfWatchTrustItem('风险等级', String(guard.level || 'LOW').toUpperCase(), guard.action || '') +
                '</div>';
            html += '<div class="pf-watch-advice-full"><div class="pf-watch-section-title">结论摘要</div>';
            pfWatchAdviceLines(row).forEach(function(line) {
                html += '<p>' + escHtml(line) + '</p>';
            });
            html += '</div>';
            html += pfWatchSection('关键证据', pfWatchEvidenceHtml(a.evidence));
            html += pfWatchSection('触发规则', pfWatchListHtml(a.decision_path, true));
            html += '<div class="pf-watch-detail-grid">' +
                pfWatchDetailCard('价格状态', '现价 ' + fmtPrice(row.cur_price), '涨跌 ' + pct(row.change_pct), '') +
                pfWatchDetailCard('持仓状态', (row.is_holding || Number(row.shares || 0) > 0) ? '持仓中' : '未持仓', positionMeta, '') +
                pfWatchDetailCard('趋势热度', '趋势 ' + (a.trend || '-'), '热度 ' + (a.heat || '-') + (a.heat_score != null ? ' / ' + a.heat_score : ''), '') +
                pfWatchDetailCard('资金态度', a.funds || row.flow_attitude_label || '-', flowMeta, '') +
                pfWatchDetailCard('回撤守门', guard.action || '-', guard.reason || '', guardClass) +
                pfWatchDetailCard('风险提示', a.risk_tip || '暂无明显', '等级 ' + String(guard.level || 'LOW').toUpperCase(), guardClass) +
                '</div>';
            html += pfWatchSection('数据质量', pfWatchDataQualityHtml(a, row));
            html += pfWatchSection('下一步盯盘', pfWatchListHtml(a.next_checks, false));
            body.innerHTML = html;
        }
        modal.classList.add('show');
    };
    window.closePfWatchModal = function() {
        var modal = document.getElementById('pfWatchModal');
        if (modal) modal.classList.remove('show');
    };
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            closeAnalyzeModal();
            closeHistoryModal();
            closeTradeModal();
            if (window.closePfWatchModal) window.closePfWatchModal();
        }
    });

    function renderFusedData(container, res) {
        var data = res.data || [];
        var h = '<div class="stats-bar">' +
            card('上榜', res.total || data.length, 'blue') +
            card('4源', data.filter(function (r) { return r.source_flag === 'all'; }).length, 'orange') +
            card('仅东财', data.filter(function (r) { return r.source_flag === 'east_only'; }).length) +
            card('仅同花顺', data.filter(function (r) { return r.source_flag === 'ths_only'; }).length) +
            card('仅雪球', data.filter(function (r) { return r.source_flag === 'xq_only'; }).length) +
            card('仅新浪', data.filter(function (r) { return r.source_flag === 'sina_only'; }).length) +
            card('数据源', fusedSourceSummary(res), 'red') +
            '</div>';
        window.renderTable(container, 'fused', ['排名', '代码', '名称', '行业', '人气标签', '概念板块', '涨跌幅', '东财排', '同花顺', '雪球', '新浪', '综合分', '来源', '分时'], data, function (r) {
            var pt = r.pop_tag || '-';
            var ct = r.concept_tag || '-';
            if (ct && ct !== '-') {
                var parts = ct.split(';').filter(Boolean);
                ct = parts.slice(0, 4).map(function (p) { return '<span class="badge-tag">' + p + '</span>'; }).join(' ');
            }
            return '<tr><td>' + rankBadge(r.fused_rank) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td class="c-gray">' + (r.industry_name || '-') + '</td><td>' + pt + '</td><td>' + ct + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.east_rank, 0) + '</td><td>' + fmt(r.ths_rank, 0) + '</td><td>' + fmt(r.xq_rank, 0) + '</td><td>' + fmt(r.sina_rank, 0) + '</td><td><strong>' + fmt(r.total_score, 1) + '</strong></td><td>' + sourceTag(r.source_flag) + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
        }, 50, h);
    }

    /* ===== 合并Tab辅助函数 ===== */
    function loadFusedTab(d, c) {
        apiGet('/fused-live?top=100').then(function (res) {
            if (!res.data || !res.data.length) {
                apiGet('/fused?snapshot_date=' + d + '&top=100').then(function (fallback) {
                    syncDateFromResponse(fallback);
                    if (!fallback.data || !fallback.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
                    renderFusedData(c, fallback);
                });
                return;
            }
            renderFusedData(c, res);
        }).catch(function () {
            apiGet('/fused?snapshot_date=' + d + '&top=100').then(function (res) {
                syncDateFromResponse(res);
                if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
                renderFusedData(c, res);
            });
        });
    }
    function loadThsTab(d, c) {
        apiGet('/rank-ths?snapshot_date=' + d + '&top=100').then(function (res) {
            syncDateFromResponse(res);
            if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
            var up = res.data.filter(function (r) { return Number(r.change_pct || 0) >= 0; }).length;
            var down = res.data.filter(function (r) { return Number(r.change_pct || 0) < 0; }).length;
            var h = '<div class="stats-bar">' + card('上榜', res.total, 'blue') + card('上涨', up, 'red') + card('下跌', down, 'green') + '</div>';
            window.renderTable(c, 'ths', ['排名', '代码', '名称', '涨跌幅', '热度值', '人气标签', '概念板块', '分时'], res.data, function (r) {
                return '<tr><td>' + rankBadge(r.rank) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.hot_value, 1) + '</td><td>' + (r.pop_tag || '-') + '</td><td>' + (r.concept_tag || '-') + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
            }, 50, h);
        });
    }
    function loadEastTab(d, c) {
        apiGet('/pop-rank-east?snapshot_date=' + d + '&top=100').then(function (res) {
            syncDateFromResponse(res);
            if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
            var up = res.data.filter(function (r) { return Number(r.change_pct || 0) >= 0; }).length;
            var down = res.data.filter(function (r) { return Number(r.change_pct || 0) < 0; }).length;
            var h = '<div class="stats-bar">' + card('上榜', res.total, 'blue') + card('上涨', up, 'red') + card('下跌', down, 'green') + '</div>';
            window.renderTable(c, 'east', ['排名', '代码', '名称', '热度值', '最新价', '涨跌幅', '人气标签', '概念板块', '分时'], res.data, function (r) {
                var tag = r.pop_tag || '-';
                var ct = r.concept_tag || '-';
                if (ct && ct !== '-') { var parts = ct.split(';').filter(Boolean); ct = parts.slice(0, 4).map(function (p) { return '<span class="badge-tag">' + p + '</span>'; }).join(' '); }
                return '<tr><td>' + rankBadge(r.rank) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td class="c-red">' + fmt(r.hot_value, 1) + '</td><td>' + fmt(r.price, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + tag + '</td><td>' + ct + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
            }, 50, h);
        });
    }
    function loadXqTab(d, c) {
        apiGet('/rank-xq?snapshot_date=' + d + '&top=100').then(function (res) {
            syncDateFromResponse(res);
            if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
            var up = res.data.filter(function (r) { return Number(r.percent || 0) >= 0; }).length;
            var down = res.data.filter(function (r) { return Number(r.percent || 0) < 0; }).length;
            var h = '<div class="stats-bar">' + card('上榜', res.total, 'blue') + card('上涨', up, 'red') + card('下跌', down, 'green') + '</div>';
            window.renderTable(c, 'xq', ['排名', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交额', '市值', '人气标签', '概念板块', '分时'], res.data, function (r) {
                var pt = r.pop_tag || '-';
                var ct = r.concept_tag || '-';
                if (ct && ct !== '-') { var parts = ct.split(';').filter(Boolean); ct = parts.slice(0, 4).map(function (p) { return '<span class="badge-tag">' + p + '</span>'; }).join(' '); }
                return '<tr><td>' + rankBadge(r.rank) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.current, 2) + '</td><td class="' + clsPct(r.percent) + '">' + pct(r.percent) + '</td><td>' + fmt(r.chg, 2) + '</td><td>' + fmtMoney(r.amount) + '</td><td>' + fmtMoney(r.market_capital) + '</td><td>' + pt + '</td><td>' + ct + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
            }, 50, h);
        });
    }
    function loadSinaTab(d, c) {
        apiGet('/rank-sina?top=100').then(function (res) {
            if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
            var up = res.data.filter(function (r) { return Number(r.change_pct || 0) >= 0; }).length;
            var down = res.data.filter(function (r) { return Number(r.change_pct || 0) < 0; }).length;
            var h = '<div class="stats-bar">' + card('上榜', res.total, 'blue') + card('上涨', up, 'red') + card('下跌', down, 'green') + '</div>';
            window.renderTable(c, 'sina', ['排名', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交额', '换手率', '分时'], res.data, function (r) {
                return '<tr><td>' + rankBadge(r.rank) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.price, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.price_change, 2) + '</td><td>' + fmtMoney(r.amount) + '</td><td>' + fmt(r.turnover_ratio, 2) + '%</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
            }, 50, h);
        }).catch(function (e) { c.innerHTML = '<div class="loading">加载失败: ' + e.message + '</div>'; });
    }
    function loadSentimentPage(d, c) {
        if (typeof window.stopMonitorRefresh === 'function') {
            window.stopMonitorRefresh();
        }
        c.innerHTML = '<div class="loading">正在加载情绪详情...</div>';
        fetchJsonWithTimeout('/market-sentiment?days=20&date=' + encodeURIComponent(d) + '&top=8&include_signal=1', 30000).then(function (res) {
            res = res || {};
            var styleSignal = res.style_switch_signal || {};
            if (res.error) { c.innerHTML = '<div class="loading" style="color:#e74c3c">❌ ' + res.error + '</div>'; return; }
            var theme = res.theme_analysis || {};
            var style = res.style_analysis || {};
            var cap = res.capital_analysis || {};
            var h = '';
            if (!styleSignal.error) {
                var sigStatus = styleSignal.status || 'balanced';
                var sigColor = sigStatus === 'risk_off' ? '#16a34a' : (sigStatus === 'switching' ? '#f59e0b' : '#64748b');
                var sigLabel = sigStatus === 'risk_off' ? '避险/防御' : (sigStatus === 'switching' ? '板块切换' : '均衡观察');
                h += '<div style="background:#fff;border-left:4px solid ' + sigColor + ';border-radius:10px;padding:16px 20px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
                h += '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px">';
                h += '<span style="background:' + sigColor + ';color:#fff;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:900">' + sigLabel + '</span>';
                h += '<span style="font-size:15px;font-weight:900;color:#111827">' + (styleSignal.summary || '-') + '</span>';
                h += '<span style="margin-left:auto;font-size:12px;color:#64748b">避险 ' + (styleSignal.risk_off_score || 0) + ' / 切换 ' + (styleSignal.switch_score || 0) + '</span>';
                h += '</div>';
                h += '<div style="font-size:13px;color:#334155;line-height:1.7;margin-bottom:8px">' + (styleSignal.action || '') + '</div>';
                if ((styleSignal.evidence || []).length) {
                    h += '<div style="display:flex;gap:6px;flex-wrap:wrap">';
                    (styleSignal.evidence || []).slice(0, 6).forEach(function(ev) {
                        h += '<span style="font-size:12px;background:#f8fafc;color:#475569;border:1px solid #e2e8f0;border-radius:999px;padding:4px 9px">' + ev + '</span>';
                    });
                    h += '</div>';
                }
                h += '</div>';
            }
            h += '<div style="background:#fff;border-radius:10px;padding:20px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
            h += '<h3 style="margin:0 0 12px;color:#2d3436;font-size:15px">─── 一、主线与轮动分析 ───</h3>';
            var phaseColor = theme.phase && theme.phase.indexOf('主线') >= 0 ? '#4caf50' : theme.phase && theme.phase.indexOf('轮动') >= 0 ? '#ff9800' : '#e53935';
            var rotColor = (theme.rotation_score || 0) < 30 ? '#4caf50' : (theme.rotation_score || 0) < 60 ? '#ff9800' : '#e53935';
            h += '<div class="stats-bar">' + card('市场阶段', theme.phase || '-', phaseColor) + card('轮动强度', (theme.rotation_score || 0) + '/100', rotColor) + card('回顾天数', res.lookback_days || 0, 'blue') + '</div>';
            h += '<p style="margin:12px 0;color:#666;line-height:1.6;font-size:14px">' + (theme.phase_desc || '') + '</p>';
            var themes = theme.main_themes || [];
            if (themes.length) {
                h += '<table style="width:100%;font-size:13px;border-collapse:collapse;margin-top:8px"><thead><tr style="color:#888;text-align:left;border-bottom:1px solid #ddd"><th style="padding:8px">排名</th><th>板块</th><th style="text-align:center">类型</th><th style="text-align:center">出现天数</th><th style="text-align:center">均排名</th><th style="text-align:right">均涨幅</th><th style="text-align:right">得分</th></tr></thead><tbody>';
                themes.forEach(function (t, i) {
                    var chgCls = (t.avg_change_pct || 0) >= 0 ? 'c-red' : 'c-green';
                    h += '<tr style="border-bottom:1px solid #f0f0f0"><td style="padding:6px 8px;color:#999">' + (i+1) + '</td><td style="padding:6px 8px;font-weight:600;color:#2d3436">' + t.name + '</td><td style="padding:6px 8px;text-align:center;color:#999;font-size:11px">' + t.type + '</td><td style="padding:6px 8px;text-align:center;color:#333">' + t.appear_days + '/' + res.lookback_days + '</td><td style="padding:6px 8px;text-align:center;color:#333">' + fmt(t.avg_rank, 1) + '</td><td style="padding:6px 8px;text-align:right;font-weight:600" class="' + chgCls + '">' + pct(t.avg_change_pct) + '</td><td style="padding:6px 8px;text-align:right;color:#e67e22;font-weight:700">' + fmt(t.score, 1) + '</td></tr>';
                });
                h += '</tbody></table>';
            }
            h += '</div>';
            h += '<div style="background:#fff;border-radius:10px;padding:20px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
            h += '<h3 style="margin:0 0 12px;color:#2d3436;font-size:15px">─── 二、大小盘风格分析 ───</h3>';
            h += '<div class="stats-bar">' + card('风格判定', style.bias || '-', style.bias && style.bias.indexOf('小盘') >= 0 ? 'red' : 'green') + card('大小盘差值', (style.large_small_diff != null ? (style.large_small_diff >= 0 ? '+' : '') + style.large_small_diff.toFixed(1) + '%' : '-'), (style.large_small_diff || 0) > 0 ? 'red' : 'green') + '</div>';
            h += '<p style="margin:12px 0;color:#666;line-height:1.6;font-size:14px">' + (style.bias_desc || '') + '</p>';
            h += '</div>';
            h += '<div style="background:#fff;border-radius:10px;padding:20px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
            h += '<h3 style="margin:0 0 12px;color:#2d3436;font-size:15px">─── 三、资金风格分析 ───</h3>';
            h += '<div class="stats-bar">' + card('总体风格', cap.flow_style || '-', cap.flow_style && cap.flow_style.indexOf('流入') >= 0 ? 'red' : 'green') + card('近期趋势', cap.recent_trend || '-', cap.recent_trend && cap.recent_trend.indexOf('流入') >= 0 ? 'red' : 'green') + '</div>';
            h += '</div>';
            h += '<div style="color:#999;font-size:11px;text-align:center;padding:8px">📅 分析日期: ' + (res.analysis_date || d) + '</div>';
            c.innerHTML = h;
        }).catch(function (e) {
            c.innerHTML = '<div class="loading" style="color:#e74c3c">❌ 加载失败: ' + (e.message || '网络错误') + '</div>';
        });
    }
    function loadCommandPage(d, c) {
        c.innerHTML = '<div class="loading">正在组装智能决策首页...</div>';
        function commandMonitorData(day) {
            function usable(res) {
                return res && !res.error && res.trade_date && res.market_heat != null;
            }
            return fetchRawJsonWithTimeout('/api/monitor/data?date=' + encodeURIComponent(day), 8000)
                .then(function (res) {
                    if (usable(res)) return res;
                    throw new Error((res && res.error) || 'monitor data incomplete');
                })
                .catch(function () {
                    return fetchJsonWithTimeout('/command-monitor?date=' + encodeURIComponent(day), 8000)
                        .then(function (res) { return usable(res) ? res : {}; })
                        .catch(function () { return {}; });
                });
        }
        commandMonitorData(d)
            .then(function (monitorFirst) {
                var activeTradeDate = (monitorFirst && monitorFirst.trade_date) ||
                    ((window._marketClock || {}).ui_trade_date) ||
                    d;
                return Promise.all([
                    fetchJsonWithTimeout('/recommended-stocks?trade_date=' + encodeURIComponent(activeTradeDate), 4500).catch(function () { return {}; }),
                    fetchJsonWithTimeout('/news-important?pages=2', 2500).catch(function () { return {}; }),
                    fetchJsonWithTimeout('/daily-review/export?review_date=' + encodeURIComponent(activeTradeDate), 2500).catch(function () { return {}; }),
                    fetchRawJsonWithTimeout('/api/portfolio/list', 3500).catch(function () { return {}; }),
                    fetchJsonWithTimeout('/sector-rotation?trade_date=' + encodeURIComponent(activeTradeDate) + '&days=10', 4000).catch(function () { return {}; }),
                    fetchJsonWithTimeout('/research-radar?trade_date=' + encodeURIComponent(activeTradeDate), 4000).catch(function () { return {}; }),
                    fetchJsonWithTimeout('/decision-radar?date=' + encodeURIComponent(activeTradeDate) + '&days=2', 4000).catch(function () { return {}; }),
                    fetchRawJsonWithTimeout('/api/datasource/required-health', 3500).catch(function () { return {}; }),
                    fetchRawJsonWithTimeout('/api/scheduler/tasks', 3500).catch(function () { return {}; }),
                    fetchRawJsonWithTimeout('/api/health/qmt-bridge', 3500).catch(function () { return {}; })
                ]).then(function (rest) {
                    if (!monitorFirst || monitorFirst.market_heat == null || monitorFirst.trade_date !== activeTradeDate) {
                        return commandMonitorData(activeTradeDate).then(function (monitorSecond) {
                            return [monitorSecond || monitorFirst || {}, rest[0] || {}, rest[1] || {}, rest[2] || {}, rest[3] || {}, rest[4] || {}, rest[5] || {}, rest[6] || {}, rest[7] || {}, rest[8] || {}, rest[9] || {}, activeTradeDate];
                        });
                    }
                    return [monitorFirst || {}, rest[0] || {}, rest[1] || {}, rest[2] || {}, rest[3] || {}, rest[4] || {}, rest[5] || {}, rest[6] || {}, rest[7] || {}, rest[8] || {}, rest[9] || {}, activeTradeDate];
                });
            }).then(function (results) {
            var monitorRes = results[0] || {};
            var recRes = results[1] || {};
            var newsRes = results[2] || {};
            var reviewRes = results[3] || {};
            var portfolioRes = results[4] || {};
            var rotationRes = results[5] || {};
            var radarRes = results[6] || {};
            var decisionRadar = results[7] || {};
            var datasourceHealthRes = results[8] || {};
            var schedulerRes = results[9] || {};
            var qmtBridgeRes = results[10] || {};
            var activeTradeDate = results[11] || monitorRes.trade_date || d;

            var picks = (recRes.data || []).slice().sort(function (a, b) {
                var aScore = blendedAnalysisRowScore(a) || 0;
                var bScore = blendedAnalysisRowScore(b) || 0;
                return bScore - aScore;
            });
            var topPicks = picks.slice(0, 6);
            var buyReadyCount = picks.filter(function (item) {
                var status = item.signal_status || item.recommend_status || '';
                return status === 'BUY_READY' || status === 'CONFIRM';
            }).length;
            var reviewText = normalizeReviewText(reviewRes.text);
            var reviewPreview = reviewText ? (reviewText.length > 220 ? reviewText.slice(0, 220) + '...' : reviewText) : '当前日期暂无复盘摘要，可以切到复盘页生成或查看最近数据。';
            var portfolioSummary = portfolioRes.summary || {};
            if (!(decisionRadar && (decisionRadar.status || decisionRadar.triggered || decisionRadar.risk || decisionRadar.opportunity))) {
                decisionRadar = portfolioSummary.tech_risk_signal || {};
            }
            var totalHoldProfit = Number(portfolioSummary.total_hold_profit || 0);
            var todayHoldProfit = Number(portfolioSummary.today_hold_profit || 0);
            var newsItems = (newsRes.data || []).slice(0, 6);
            var topIndustries = (monitorRes.top_industries || []).slice(0, 5);
            var topConcepts = (monitorRes.concept_rows || []).slice(0, 6);
            var signalText = monitorRes.analysis ? firstNonEmptyText(monitorRes.analysis.signal) : '';
            var tempText = monitorRes.analysis ? firstNonEmptyText(monitorRes.analysis.market_temp) : '';
            var focusText = monitorRes.analysis ? firstNonEmptyText(monitorRes.analysis.industry_focus) : '';
            var capitalText = monitorRes.analysis ? firstNonEmptyText(monitorRes.analysis.capital_flow) : '';
            var rotationSignal = rotationRes.rotation_signal || {};
            var radarThemes = (radarRes.themes || []).slice(0, 4);
            var coreSection = extractReviewSection(reviewText, '1. 今日核心结论');
            var dataSection = extractReviewSection(reviewText, '2. 关键数据复核');
            var boardSection = extractReviewSection(reviewText, '4. 主线与板块判断');
            var planSection = extractReviewSection(reviewText, '5. 明日执行计划');
            var coreBullets = extractReviewBullets(coreSection, 3);
            var dataBullets = extractReviewBullets(dataSection, 3);
            var boardBullets = extractReviewBullets(boardSection, 3);
            var planBullets = extractReviewBullets(planSection, 3);
            var mainBoardRows = parseBoardRanking(reviewText);
            var mainBoardSource = mainBoardRows.length ? '复盘涨幅Top5' : '行业成交额/热度';
            if (!mainBoardRows.length) {
                mainBoardRows = normalizeIndustryRows(topIndustries);
            }
            var conceptBoardRows = normalizeConceptRows(rotationRes, topConcepts);

            function normalizeReviewText(value) {
                return String(value == null ? '' : value)
                    .replace(/<br\s*\/?>/gi, '\n')
                    .replace(/<\/p>/gi, '\n')
                    .replace(/<[^>]*>/g, ' ')
                    .replace(/[#>*`]/g, ' ')
                    .replace(/\r\n/g, '\n')
                    .replace(/[ \t]+/g, ' ')
                    .replace(/\n[ \t]+/g, '\n')
                    .replace(/[ \t]+\n/g, '\n')
                    .replace(/\n{3,}/g, '\n\n')
                    .trim();
            }
            function cleanReviewLine(line) {
                return firstNonEmptyText(line)
                    .replace(/^\s*[-•]\s*/, '')
                    .replace(/^\s*\d+\.\s*/, '')
                    .trim();
            }
            function extractReviewSection(text, heading) {
                text = normalizeReviewText(text);
                if (!text) return '';
                var idx = text.indexOf(heading);
                if (idx < 0) {
                    idx = text.indexOf(heading.replace(/^\d+\.\s*/, ''));
                }
                if (idx < 0) return '';
                var rest = text.slice(idx);
                var next = rest.slice(1).match(/\n\s*\d+\.\s+/);
                return (next ? rest.slice(0, next.index + 1) : rest).trim();
            }
            function extractReviewBullets(section, limit) {
                section = normalizeReviewText(section);
                if (!section) return [];
                var lines = section.split(/\r?\n/);
                var bullets = [];
                lines.forEach(function (line) {
                    var raw = String(line || '').trim();
                    if (!raw || /^\d+\.\s*/.test(raw)) return;
                    var item = cleanReviewLine(raw);
                    if (item && item.length > 4) bullets.push(item);
                });
                if (!bullets.length) {
                    bullets = section.replace(/^\d+\.\s*[^\n]+/, '')
                        .split(/[。；;]/)
                        .map(cleanReviewLine)
                        .filter(function (line) { return line.length > 4; });
                }
                return bullets.slice(0, limit || 3);
            }
            function findBullet(bullets, keyword) {
                for (var i = 0; i < (bullets || []).length; i += 1) {
                    if (String(bullets[i]).indexOf(keyword) >= 0) return bullets[i];
                }
                return '';
            }
            function signedPctText(v) {
                var n = Number(v);
                if (isNaN(n)) return '-';
                return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
            }
            function parseBoardRanking(text) {
                text = normalizeReviewText(text);
                if (!text) return [];
                var rows = [];
                var match = text.match(/板块涨幅排名Top5[：:]\s*([^\n；;]+)/);
                if (match && match[1]) {
                    match[1].split(/[、,，]/).forEach(function (part) {
                        var item = firstNonEmptyText(part);
                        if (!item) return;
                        var m = item.match(/^(.+?)[（(]\s*(?:涨幅)?\s*([+-]?\d+(?:\.\d+)?)%\s*[）)]/);
                        if (m) {
                            var chg = Number(m[2]);
                            rows.push({
                                name: m[1].trim(),
                                metricLabel: '涨幅',
                                metric: signedPctText(chg),
                                change: chg,
                                note: '复盘涨幅Top5'
                            });
                        }
                    });
                }
                if (rows.length) return rows.slice(0, 6);
                var focus = text.match(/选股范围优先放在([^。\n]+)/);
                if (focus && focus[1]) {
                    focus[1].replace(/等.*$/, '').split(/[、,，]/).forEach(function (name) {
                        name = cleanReviewLine(name);
                        if (name) {
                            rows.push({
                                name: name,
                                metricLabel: '主线',
                                metric: '观察',
                                change: null,
                                note: '复盘提及方向'
                            });
                        }
                    });
                }
                return rows.slice(0, 6);
            }
            function normalizeIndustryRows(items) {
                return (items || []).slice(0, 6).map(function (item) {
                    var chg = Number(item.change || item.change_pct || 0);
                    return {
                        name: item.name || '-',
                        metricLabel: '成交额',
                        metric: fmtMoney(Number(item.amount || item.heat || 0)),
                        change: isNaN(chg) ? null : chg,
                        note: '行业成交额/热度'
                    };
                });
            }
            function normalizeConceptRows(rotation, fallbackConcepts) {
                var concepts = (rotation.rising_concepts || []).slice(0, 6);
                if (concepts.length) {
                    return concepts.map(function (item) {
                        var avgChange = Number(item.avg_change_pct);
                        var momentum = Number(item.momentum || item.momentum_score || 0);
                        var noteParts = [];
                        if (item.appear_days != null) noteParts.push('出现' + item.appear_days + '日');
                        if (item.avg_rank != null) noteParts.push('均排' + fmt(Number(item.avg_rank), 1));
                        return {
                            name: item.name || '-',
                            metricLabel: '动量',
                            metric: fmt(momentum, 1),
                            change: isNaN(avgChange) ? null : avgChange,
                            note: noteParts.join(' / ') || '概念动量'
                        };
                    });
                }
                return (fallbackConcepts || []).slice(0, 6).map(function (item) {
                    var chg = Number(item.change || item.change_pct || 0);
                    return {
                        name: item.name || '-',
                        metricLabel: '热度',
                        metric: fmtMoney(Number(item.heat || 0)),
                        change: isNaN(chg) ? null : chg,
                        note: '概念热度'
                    };
                });
            }
            function joinNames(items, limit, emptyText) {
                var arr = (items || []).slice(0, limit || 3).map(function (item) {
                    return firstNonEmptyText(item.name || item);
                }).filter(Boolean);
                return arr.length ? arr.join('、') : (emptyText || '-');
            }
            function renderDecisionCard(label, title, body, tone) {
                return '<div class="command-decision-card ' + (tone || '') + '">' +
                    '<div class="command-decision-label">' + escHtml(label) + '</div>' +
                    '<div class="command-decision-title">' + escHtml(title || '-') + '</div>' +
                    '<div class="command-decision-text">' + escHtml(body || '-') + '</div>' +
                '</div>';
            }
            function renderReviewBlock(title, bullets) {
                if (!bullets || !bullets.length) return '';
                return '<div class="command-review-block">' +
                    '<div class="command-review-heading">' + escHtml(title) + '</div>' +
                    '<ul class="command-review-bullets">' +
                    bullets.map(function (line) { return '<li>' + escHtml(line) + '</li>'; }).join('') +
                    '</ul>' +
                '</div>';
            }
            function renderReviewSummary() {
                var html = renderReviewBlock('核心结论', coreBullets) +
                    renderReviewBlock('关键数据', dataBullets) +
                    renderReviewBlock('主线判断', boardBullets) +
                    renderReviewBlock('明日计划', planBullets);
                if (!html) {
                    return '<div class="command-review-preview">' + escHtml(reviewPreview) + '</div>';
                }
                return '<div class="command-review-sections">' + html + '</div>';
            }
            function renderBoardRows(items, emptyText) {
                if (!items || !items.length) {
                    return '<div class="command-empty">' + escHtml(emptyText || '暂无数据') + '</div>';
                }
                return items.map(function (item) {
                    var chg = Number(item.change);
                    var hasChange = !isNaN(chg);
                    var cls = hasChange ? (chg >= 0 ? 'up' : 'down') : '';
                    return '<div class="command-board-row">' +
                        '<div class="command-board-main">' +
                        '<div class="command-board-name">' + escHtml(item.name || '-') + '</div>' +
                        '<div class="command-board-note">' + escHtml(item.note || '') + '</div>' +
                        '</div>' +
                        '<div class="command-board-metric">' +
                        '<span>' + escHtml(item.metricLabel || '指标') + '</span>' +
                        '<strong>' + escHtml(item.metric || '-') + '</strong>' +
                        '</div>' +
                        '<div class="command-board-change ' + cls + '">' + (hasChange ? signedPctText(chg) : '-') + '</div>' +
                    '</div>';
                }).join('');
            }
            function renderRotationDetail() {
                var toRows = (rotationSignal.to_sectors && rotationSignal.to_sectors.length ? rotationSignal.to_sectors : rotationRes.rising_sectors || []).slice(0, 4);
                var fromRows = (rotationSignal.from_sectors && rotationSignal.from_sectors.length ? rotationSignal.from_sectors : rotationRes.falling_sectors || []).slice(0, 4);
                var conceptRows = (rotationRes.rising_concepts || []).slice(0, 4);
                var fundIn = (rotationSignal.fund_in_sectors || []).slice(0, 3);
                var fundOut = (rotationSignal.fund_out_sectors || []).slice(0, 3);
                var flowText = fundIn.length || fundOut.length
                    ? '流入：' + joinNames(fundIn, 3, '暂无') + '；流出：' + joinNames(fundOut, 3, '暂无')
                    : '资金流向快照暂无净流入/净流出排名，先以动量和跌幅退潮方向观察。';
                return '<div class="command-rotation-grid">' +
                    '<div class="command-rotation-box"><div class="command-rotation-label">结论</div><div class="command-rotation-text">' + escHtml(rotationSignal.summary || '暂无明确板块切换信号') + '</div></div>' +
                    '<div class="command-rotation-box"><div class="command-rotation-label">动作</div><div class="command-rotation-text">' + escHtml(rotationSignal.action || '维持观察，等主线确认后再加仓') + '</div></div>' +
                    '<div class="command-rotation-box"><div class="command-rotation-label">进攻观察</div><div class="command-rotation-text">' + escHtml(joinNames(toRows, 4, joinNames(conceptRows, 4, '暂无明显进攻方向'))) + '</div></div>' +
                    '<div class="command-rotation-box"><div class="command-rotation-label">退潮/风险</div><div class="command-rotation-text">' + escHtml(joinNames(fromRows, 4, '暂无明显退潮方向')) + '</div></div>' +
                    '<div class="command-rotation-box command-rotation-box-wide"><div class="command-rotation-label">资金快照</div><div class="command-rotation-text">' + escHtml(flowText) + '</div></div>' +
                    '<div class="command-rotation-box command-rotation-box-wide"><div class="command-rotation-label">验证条件</div><div class="command-rotation-text">次日重点看前排承接、成交额是否放大、后排是否扩散；若放量下跌或跌停扩张，先降仓防守。</div></div>' +
                '</div>';
            }

            function scoreForPick(item) {
                return Math.round(blendedAnalysisRowScore(item) || 0);
            }
            function pickStatus(item) {
                var status = item.signal_status || item.recommend_status || 'WATCH';
                var map = {
                    BUY_READY: { label: '买点就绪', cls: 'buy' },
                    CONFIRM: { label: '确认', cls: 'buy' },
                    WATCH: { label: '观察', cls: 'watch' },
                    SELL_ALERT: { label: '卖出提醒', cls: 'risk' },
                    BLOCK: { label: '不推荐', cls: 'risk' }
                };
                return map[status] || { label: status, cls: 'watch' };
            }
            function renderNewsList(items) {
                if (!items.length) {
                    return '<div class="command-empty">暂无重要快讯</div>';
                }
                return items.map(function (item) {
                    var sources = item.sources || (item.source ? [item.source] : []);
                    return '<div class="command-news-item">' +
                        '<div class="command-news-meta">' +
                        '<span>' + (item.time || item.publish_time || '-') + '</span>' +
                        '<span>' + sources.join(' / ') + '</span>' +
                        '</div>' +
                        '<div class="command-news-title">' + (item.title || '重要资讯') + '</div>' +
                    '</div>';
                }).join('');
            }
            function renderSimpleTags(items) {
                if (!items.length) {
                    return '<div class="command-empty">暂无主线数据</div>';
                }
                return items.map(function (item) {
                    var chg = Number(item.change || 0);
                    var cls = chg >= 0 ? 'up' : 'down';
                    var changeText = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%';
                    return '<div class="command-tag-row">' +
                        '<span class="command-tag-name">' + item.name + '</span>' +
                        '<span class="command-tag-value">' + Math.round(Number(item.heat || 0)) + '</span>' +
                        '<span class="command-tag-change ' + cls + '">' + changeText + '</span>' +
                    '</div>';
                }).join('');
            }
            function renderRadarMini(themes) {
                if (!themes.length) {
                    return '<div class="command-empty">暂无研报雷达数据</div>';
                }
                return themes.map(function (theme) {
                    var stocks = (theme.stocks || []).slice(0, 4).map(function (s) { return s.name; }).join('、') || '-';
                    return '<div class="command-radar-row">' +
                        '<div class="command-radar-top"><strong>' + escHtml(theme.name) + '</strong><span>' + (theme.score || '-') + '/100</span></div>' +
                        '<div class="command-radar-note">' + escHtml(theme.trend || '-') + ' · ' + escHtml(theme.evidence_level || '-') + '</div>' +
                        '<div class="command-radar-stocks">' + escHtml(stocks) + '</div>' +
                    '</div>';
                }).join('');
            }
            function radarNameList(items, limit, emptyText) {
                var names = (items || []).slice(0, limit || 5).map(function (item) {
                    return firstNonEmptyText((item || {}).name || item);
                }).filter(Boolean);
                return names.length ? names.join('、') : (emptyText || '-');
            }
            function radarStockLabel(item) {
                item = item || {};
                var code = firstNonEmptyText(item.stock_code || item.code || '');
                var name = firstNonEmptyText(item.short_name || item.name || item.stock_name || '');
                return name && code ? name + '(' + code + ')' : (name || code || '');
            }
            function radarStockList(items, limit, emptyText) {
                var names = (items || []).slice(0, limit || 6).map(radarStockLabel).filter(Boolean);
                return names.length ? names.join('、') : (emptyText || '-');
            }
            function renderDecisionRadarAlert(signal) {
                signal = signal || {};
                var risk = signal.risk || signal || {};
                var opportunity = signal.opportunity || {};
                var html = '';
                if (risk.triggered) {
                    html += '<div class="command-radar-alert risk">' +
                        '<div class="command-radar-alert-kicker">黑天鹅/板块风险</div>' +
                        '<div class="command-radar-alert-title">' + escHtml(risk.headline || '风险触发，先防守') + '</div>' +
                        '<div class="command-radar-alert-text">' + escHtml(risk.action || '命中方向先控仓，弱反弹不加仓。') + '</div>' +
                        '<div class="command-radar-chip-row">' +
                        '<span class="command-radar-chip">板块：' + escHtml(radarNameList(risk.affected_sectors || [], 6, '相关板块')) + '</span>' +
                        '<span class="command-radar-chip strong">持仓：' + escHtml(radarStockList(risk.exposed_holdings || [], 8, '暂无实际持仓命中')) + '</span>' +
                        '</div>' +
                    '</div>';
                }
                if (opportunity.status && opportunity.status !== 'clear') {
                    html += '<div class="command-radar-alert opportunity">' +
                        '<div class="command-radar-alert-kicker">机会观察</div>' +
                        '<div class="command-radar-alert-title">' + escHtml(opportunity.headline || '机会方向出现观察信号') + '</div>' +
                        '<div class="command-radar-alert-text">' + escHtml(opportunity.action || '等资金和指数确认后再动手。') + '</div>' +
                        '<div class="command-radar-chip-row">' +
                        '<span class="command-radar-chip">板块：' + escHtml(radarNameList(opportunity.opportunity_sectors || [], 6, '强势方向')) + '</span>' +
                        '<span class="command-radar-chip strong">个股：' + escHtml(radarStockList(opportunity.candidate_stocks || [], 8, '暂无候选个股')) + '</span>' +
                        '</div>' +
                    '</div>';
                }
                return html ? '<div class="command-radar-alert-wrap">' + html + '</div>' : '';
            }
            function systemTone(status) {
                status = String(status || '').toLowerCase();
                if (status === 'ok' || status === 'fresh' || status === 'success' || status === 'running') return 'good';
                if (status === 'warn' || status === 'fallback' || status === 'stale' || status === 'disabled' || status === 'pending') return 'warn';
                if (status === 'error' || status === 'failed' || status === 'fail') return 'bad';
                return 'muted';
            }
            function systemScoreForTone(tone) {
                return tone === 'good' ? 25 : tone === 'warn' ? 14 : tone === 'bad' ? 4 : 9;
            }
            function systemHealthItem(label, value, note, tone, action) {
                tone = tone || 'muted';
                var actionHtml = action ? '<div class="command-system-action">' + escHtml(action) + '</div>' : '';
                return '<div class="command-system-item ' + tone + '">' +
                    '<div class="command-system-top"><span>' + escHtml(label) + '</span><strong>' + escHtml(value || '-') + '</strong></div>' +
                    '<div class="command-system-note">' + escHtml(note || '-') + '</div>' +
                    actionHtml +
                '</div>';
            }
            function summarizeDatasourceHealth() {
                var rows = datasourceHealthRes.data || datasourceHealthRes.required_health || [];
                var badRows = rows.filter(function (item) {
                    var status = String((item || {}).status || '').toLowerCase();
                    return status && status !== 'ok' && status !== 'running';
                });
                if (!rows.length) {
                    return { label: '关键数据任务', value: '未返回', note: '数据源健康接口暂无结果', tone: 'muted', action: '打开数据源管理确认任务表' };
                }
                if (badRows.length === 0) {
                    return { label: '关键数据任务', value: '全部可用', note: rows.length + ' 个关键任务通过或运行中', tone: 'good', action: '' };
                }
                var names = badRows.slice(0, 3).map(function (item) {
                    return item.label || item.name || item.task_name || item.biz_type || item.task_type || item.id || '任务';
                }).join('、');
                return { label: '关键数据任务', value: badRows.length + ' 项异常', note: names || '存在异常任务', tone: 'bad', action: '先补关键数据，再看推荐排序' };
            }
            function summarizeSchedulerHealth() {
                var runtime = schedulerRes.runtime || {};
                var tasks = schedulerRes.data || [];
                var online = Boolean(runtime.standalone_scheduler_online || runtime.embedded_scheduler_running);
                var failed = tasks.filter(function (item) { return String((item || {}).last_run_status || '').toLowerCase() === 'failed'; });
                var running = tasks.filter(function (item) { return String((item || {}).last_run_status || '').toLowerCase() === 'running'; });
                if (!schedulerRes || (!tasks.length && !Object.keys(runtime).length)) {
                    return { label: '调度运行', value: '未返回', note: '调度接口暂无结果', tone: 'muted', action: '打开调度管理查看任务' };
                }
                if (failed.length) {
                    return { label: '调度运行', value: failed.length + ' 个失败', note: failed.slice(0, 3).map(function (t) { return t.name || t.task_name || t.task_type || t.id; }).join('、'), tone: 'bad', action: '优先重跑失败任务' };
                }
                if (online) {
                    return { label: '调度运行', value: runtime.standalone_scheduler_online ? '独立在线' : '内嵌在线', note: '运行中 ' + running.length + ' 个 / 启用 ' + tasks.filter(function (t) { return Number(t.enabled) === 1; }).length + ' 个', tone: 'good', action: '' };
                }
                return { label: '调度运行', value: '未在线', note: '未检测到独立或内嵌调度心跳', tone: 'warn', action: '需要自动刷新时先启动调度' };
            }
            function summarizeQmtHealth() {
                var status = String(qmtBridgeRes.status || '').toLowerCase();
                if (!status) {
                    return { label: '实时行情', value: '未返回', note: 'QMT/实时行情健康接口暂无结果', tone: 'muted', action: '盘中看价前确认实时源' };
                }
                var stock = qmtBridgeRes.stock_current || {};
                var index = qmtBridgeRes.index_current || {};
                var age = stock.age_seconds != null ? stock.age_seconds + '秒' : '-';
                var note = qmtBridgeRes.status_reason || ('个股快照 ' + (stock.latest_snapshot_at || '-') + ' / 指数快照 ' + (index.latest_snapshot_at || '-') + ' / 延迟 ' + age);
                var value = status === 'ok' ? '可用' : status === 'warn' ? '偏旧' : status === 'disabled' ? '未启用' : '异常';
                return { label: '实时行情', value: value, note: note, tone: systemTone(status), action: status === 'ok' ? '' : '盘中交易前先修复实时源' };
            }
            function summarizeRecommendationFreshness() {
                var freshness = recRes.freshness || {};
                var status = freshness.status || (recRes.total ? 'fresh' : 'pending');
                var stale = freshness.stale_sources || [];
                var dateText = freshness.result_date || recRes.date || activeTradeDate || '-';
                var note = freshness.status_label || '推荐数据状态';
                if (stale.length) note += '：' + stale.join('、');
                if (freshness.quote_mode === 'live') note += ' / 实时行情覆盖 ' + (freshness.live_quote_count || 0) + ' 只';
                return {
                    label: '推荐数据',
                    value: freshness.status_label || (recRes.total ? '有结果' : '待生成'),
                    note: '结果日 ' + dateText + ' / ' + note,
                    tone: systemTone(status),
                    action: status === 'fresh' ? '' : '必要时重新生成 AI 推荐'
                };
            }
            function renderSystemHealthPanel() {
                var items = [
                    summarizeDatasourceHealth(),
                    summarizeSchedulerHealth(),
                    summarizeQmtHealth(),
                    summarizeRecommendationFreshness()
                ];
                var score = items.reduce(function (sum, item) { return sum + systemScoreForTone(item.tone); }, 0);
                var tone = score >= 82 ? 'good' : score >= 58 ? 'warn' : 'bad';
                var verdict = score >= 82 ? '可信度高' : score >= 58 ? '可用但要复核' : '先补数据再决策';
                var action = score >= 82
                    ? '可以进入主线、个股和仓位判断'
                    : score >= 58
                        ? '只用作观察，关键买卖先复核数据源'
                        : '暂停追单，先修复异常任务和实时行情';
                return '<section class="command-system-panel ' + tone + '">' +
                    '<div class="command-system-head">' +
                    '<div><div class="command-system-kicker">系统可信度</div><h3>' + verdict + '</h3><p>' + action + '</p></div>' +
                    '<div class="command-system-score"><strong>' + score + '</strong><span>/100</span></div>' +
                    '</div>' +
                    '<div class="command-system-grid">' +
                    items.map(function (item) { return systemHealthItem(item.label, item.value, item.note, item.tone, item.action); }).join('') +
                    '</div>' +
                    '<div class="command-mini-meta">参考开源投研系统的做法：先看数据质量与运行状态，再解释信号和执行动作。</div>' +
                '</section>';
            }

            var upCount = Number(monitorRes.up_count || 0);
            var downCount = Number(monitorRes.down_count || 0);
            var totalBreadth = upCount + downCount;
            var redRatio = totalBreadth > 0 ? (upCount / totalBreadth * 100).toFixed(1) + '%' : '-';
            var environmentLine = '上涨' + (upCount || '-') + '家 / 下跌' + (downCount || '-') + '家，红盘率' + redRatio + '。' + (tempText || '');
            var opportunityLine = findBullet(coreBullets, '机会方向') || findBullet(boardBullets, '选股范围') || focusText || '暂无主线确认，先观察前排强度。';
            var actionLine = findBullet(coreBullets, '执行结论') || findBullet(planBullets, '操作节奏') || signalText || '等待数据确认，避免在弱势中追高。';
            var riskNames = joinNames((rotationSignal.from_sectors && rotationSignal.from_sectors.length ? rotationSignal.from_sectors : rotationRes.falling_sectors || []), 4, '');
            var riskLine = riskNames ? ('退潮方向：' + riskNames + '；若放量下跌或跌停扩张，先降仓。') : '暂无明确退潮方向，但市场温度偏低时仍以仓位控制优先。';
            var verifyLine = '看三件事：前排是否继续承接、成交额是否放大、后排是否扩散。TMT成交占比' + (monitorRes.tmt_ratio != null ? Number(monitorRes.tmt_ratio).toFixed(2) + '%' : '-') + '，中证1000热度' + ((monitorRes.csi1000 || {}).heat != null ? (monitorRes.csi1000 || {}).heat : '-') + '。';
            var radarRisk = decisionRadar.risk || decisionRadar || {};
            var radarOpportunity = decisionRadar.opportunity || {};
            if (radarRisk.triggered) {
                actionLine = radarRisk.action || actionLine;
                riskLine = radarRisk.action || riskLine;
            }
            if (radarOpportunity.status && radarOpportunity.status !== 'clear') {
                opportunityLine = radarOpportunity.action || opportunityLine;
            }

            var h = '';
            h += '<div class="command-shell">';
            h += '<div class="command-hero">';
            h += '<div>';
            h += '<div class="command-eyebrow">ProBigA / A股智能决策工作台</div>';
            h += '<h2 class="command-title">先看情绪，再看主线，再看个股</h2>';
            h += '<p class="command-subtitle">把市场温度、资金风格、重要新闻、复盘摘要和推荐股票压到一页里，打开就知道今天先做什么。</p>';
            h += '</div>';
            h += '<div class="command-actions">';
            h += '<button class="command-action-btn" onclick="switchTab(\'monitor\')">市场监控</button>';
            h += '<button class="command-action-btn" onclick="switchTab(\'recommended\')">AI推荐</button>';
            h += '<button class="command-action-btn" onclick="switchTab(\'review\')">每日复盘</button>';
            h += '<button class="command-action-btn" onclick="switchTab(\'research-radar\')">研报雷达</button>';
            h += '<button class="command-action-btn" onclick="switchTab(\'news\')">政策资讯</button>';
            h += '</div>';
            h += '</div>';

            h += '<div class="command-kpi-grid">';
            h += '<div class="command-kpi"><div class="command-kpi-label">市场热度</div><div class="command-kpi-value">' + (monitorRes.market_heat != null ? monitorRes.market_heat : '-') + '</div><div class="command-kpi-note">' + (monitorRes.heat_status || '等待数据') + '</div></div>';
            h += '<div class="command-kpi"><div class="command-kpi-label">小波动占比</div><div class="command-kpi-value">' + (monitorRes.sideline_ratio != null ? Number(monitorRes.sideline_ratio).toFixed(2) + '%' : '-') + '</div><div class="command-kpi-note">涨跌幅±1%内</div></div>';
            h += '<div class="command-kpi"><div class="command-kpi-label">TMT成交占比</div><div class="command-kpi-value">' + (monitorRes.tmt_ratio != null ? Number(monitorRes.tmt_ratio).toFixed(2) + '%' : '-') + '</div><div class="command-kpi-note">电子/通信/计算机/传媒</div></div>';
            h += '<div class="command-kpi"><div class="command-kpi-label">中证1000热度</div><div class="command-kpi-value">' + ((monitorRes.csi1000 || {}).heat != null ? (monitorRes.csi1000 || {}).heat : '-') + '</div><div class="command-kpi-note">' + ((monitorRes.csi1000 || {}).change != null ? pct((monitorRes.csi1000 || {}).change) : '等待数据') + '</div></div>';
            h += '<div class="command-kpi"><div class="command-kpi-label">推荐股票</div><div class="command-kpi-value">' + picks.length + '</div><div class="command-kpi-note">买点就绪 ' + buyReadyCount + ' 只</div></div>';
            h += '<div class="command-kpi"><div class="command-kpi-label">持仓盈亏</div><div class="command-kpi-value ' + (totalHoldProfit >= 0 ? 'up' : 'down') + '">' + (totalHoldProfit >= 0 ? '+' : '') + totalHoldProfit.toFixed(2) + '</div><div class="command-kpi-note">当日 ' + (todayHoldProfit >= 0 ? '+' : '') + todayHoldProfit.toFixed(2) + '</div></div>';
            h += '</div>';

            h += renderDecisionRadarAlert(decisionRadar);
            h += renderSystemHealthPanel();

            h += '<div class="command-grid">';
            h += '<section class="command-panel command-panel-wide">';
            h += '<div class="command-panel-title">市场结论</div>';
            h += '<div class="command-decision-grid">';
            h += renderDecisionCard('环境', monitorRes.heat_status || '市场温度', environmentLine, 'neutral');
            h += renderDecisionCard('主线', '优先方向', opportunityLine, 'focus');
            h += renderDecisionCard('动作', '今日执行', actionLine, 'action');
            h += renderDecisionCard('风险', radarRisk.headline || '先防守再进攻', riskLine, 'risk');
            h += renderDecisionCard('验证', '盘中确认点', verifyLine, 'verify');
            h += '</div>';
            h += '<div class="command-mini-meta">页面日期 ' + (monitorRes.requested_date || d) + ' / 统一交易日 ' + (monitorRes.trade_date || activeTradeDate) + (monitorRes.is_realtime ? ' / 盘中实时' : ' / 日线汇总') + '</div>';
            h += '</section>';

            h += '<section class="command-panel">';
            h += '<div class="command-panel-title">AI推荐 Top 6</div>';
            if (topPicks.length) {
                h += topPicks.map(function (item) {
                    var status = pickStatus(item);
                    return '<div class="command-pick-item">' +
                        '<div class="command-pick-main">' +
                        '<div class="command-pick-code">' + (item.stock_code || '-') + '</div>' +
                        '<div class="command-pick-name">' + (item.short_name || '-') + '</div>' +
                        '</div>' +
                        '<div class="command-pick-side">' +
                        '<span class="command-pick-score">' + scoreForPick(item) + '</span>' +
                        '<span class="command-pill ' + status.cls + '">' + status.label + '</span>' +
                        '</div>' +
                    '</div>';
                }).join('');
            } else {
                h += '<div class="command-empty">当前日期暂无推荐结果，可以直接去 AI 推荐页重新触发筛选。</div>';
            }
            h += '</section>';

            h += '<section class="command-panel">';
            h += '<div class="command-panel-title">重要资讯</div>';
            h += renderNewsList(newsItems);
            h += '</section>';

            h += '<section class="command-panel command-panel-wide">';
            h += '<div class="command-panel-title">复盘摘要</div>';
            h += renderReviewSummary();
            h += '<div class="command-mini-meta">复盘日期 ' + (reviewRes.date || activeTradeDate) + '</div>';
            h += '</section>';

            h += '<section class="command-panel">';
            h += '<div class="command-panel-title">主线板块</div>';
            h += '<div class="command-mini-meta command-mini-meta-top">数字含义：' + mainBoardSource + '；右侧为涨跌幅。</div>';
            h += renderBoardRows(mainBoardRows, '暂无主线板块数据');
            h += '</section>';

            h += '<section class="command-panel">';
            h += '<div class="command-panel-title">热门概念</div>';
            h += '<div class="command-mini-meta command-mini-meta-top">优先使用板块轮动里的概念动量；没有时回退概念热度。</div>';
            h += renderBoardRows(conceptBoardRows, '暂无概念动量数据');
            h += '</section>';

            h += '<section class="command-panel command-panel-wide">';
            h += '<div class="command-panel-title">板块切换</div>';
            h += renderRotationDetail();
            h += '<div class="command-mini-meta">资金快照 ' + (rotationSignal.flow_snapshot_at || rotationRes.flow_snapshot_at || '暂无') + '</div>';
            h += '</section>';

            h += '<section class="command-panel command-panel-wide">';
            h += '<div class="command-panel-title">研报趋势雷达</div>';
            h += renderRadarMini(radarThemes);
            h += '<div class="command-mini-meta">博主/公众号用于发现热度，研报和财报用于验证。<a href="javascript:void(0)" onclick="switchTab(\'research-radar\')" style="color:#1a73e8;text-decoration:none;margin-left:8px">查看完整股票池</a></div>';
            h += '</section>';

            h += '<section class="command-panel command-panel-wide">';
            h += '<div class="command-panel-title">持仓与节奏</div>';
            h += '<div class="command-summary-list">';
            h += '<div><strong>持仓只数：</strong>' + (portfolioSummary.holding_count != null ? portfolioSummary.holding_count : 0) + '</div>';
            h += '<div><strong>今日新开：</strong>' + (portfolioSummary.today_open_count != null ? portfolioSummary.today_open_count : 0) + '</div>';
            h += '<div><strong>今日清仓：</strong>' + (portfolioSummary.today_cleared_count != null ? portfolioSummary.today_cleared_count : 0) + '</div>';
            h += '<div><strong>事件命中：</strong>' + (portfolioSummary.tech_risk_alerts != null ? portfolioSummary.tech_risk_alerts : (radarRisk.exposed_holding_count || 0)) + ' 只</div>';
            h += '<div><strong>成交额：</strong>' + (monitorRes.amount_display || '-') + '</div>';
            h += '</div>';
            h += '</section>';
            h += '</div>';
            h += '</div>';
            c.innerHTML = h;
        }).catch(function (e) {
            c.innerHTML = '<div class="loading" style="color:#e74c3c">❌ 智能决策页加载失败: ' + (e.message || '网络错误') + '</div>';
        });
    }

    var intradayBattleTimer = null;
    var intradayBattleRequestSeq = 0;
    window.stopIntradayBattleRefresh = function () {
        if (intradayBattleTimer) {
            clearInterval(intradayBattleTimer);
            intradayBattleTimer = null;
        }
        intradayBattleRequestSeq += 1;
    };

    function loadIntradayBattlePage(d, c) {
        if (intradayBattleTimer) {
            clearInterval(intradayBattleTimer);
            intradayBattleTimer = null;
        }
        intradayBattleRequestSeq += 1;
        var requestSeq = intradayBattleRequestSeq;
        var lastHtml = '';
        var portfolioLivePending = false;
        c.innerHTML = '<div class="loading">正在加载盘中作战台...</div>';

        function jsonList(value) {
            if (!value) return [];
            if (Array.isArray(value)) return value;
            try {
                var parsed = JSON.parse(value);
                return Array.isArray(parsed) ? parsed : [];
            } catch (e) {
                return [];
            }
        }
        function numberValue(v, fallback) {
            var n = Number(v);
            return isNaN(n) ? (fallback || 0) : n;
        }
        function shortText(v, n) {
            var text = firstNonEmptyText(v);
            if (!text) return '-';
            return text.length > (n || 88) ? text.slice(0, n || 88) + '...' : text;
        }
        function stockInline(code, name) {
            code = String(code || '').trim();
            if (/^\d+$/.test(code)) code = code.padStart(6, '0');
            return '<a class="clickable-name" href="javascript:void(0)" onclick="openKlineModal(\'' + escAttr(code) + '\',\'' + escAttr(name || code) + '\')">' + escHtml(name || code) + '</a>';
        }
        function actionPill(text, tone) {
            return '<span class="battle-pill ' + (tone || '') + '">' + escHtml(localizeMachineText(text || '-')) + '</span>';
        }
        function metric(label, value, note, tone) {
            return '<div class="battle-metric ' + (tone || '') + '">' +
                '<div class="battle-metric-label">' + escHtml(label) + '</div>' +
                '<div class="battle-metric-value">' + escHtml(value) + '</div>' +
                '<div class="battle-metric-note">' + escHtml(note || '') + '</div>' +
            '</div>';
        }
        function extractProbability(row) {
            var chain = jsonList(row.evidence_chain_json);
            for (var i = 0; i < chain.length; i += 1) {
                if (chain[i] && chain[i].module === 'probability' && chain[i].value) {
                    return chain[i].value;
                }
            }
            return {};
        }
        function rowScore(row) {
            return Number(blendedAnalysisRowScore(row) || row.final_trade_score || row.ai_score || 0);
        }
        function sectorLabel(status) {
            status = String(status || '').toUpperCase();
            if (status === 'PASS') return actionPill('板块通过', 'buy');
            if (status === 'BLOCK' || status === 'RISK') return actionPill('板块风险', 'risk');
            return actionPill('板块观察', 'watch');
        }
        function quoteStatusLabel(status) {
            status = String(status || '').toLowerCase();
            if (status === 'fresh') return actionPill('实时', 'buy');
            if (status === 'stale') return actionPill('过期', 'watch');
            return actionPill('缺行情', 'risk');
        }
        function renderSectorRows(rows, emptyText) {
            rows = (rows || []).slice(0, 6);
            if (!rows.length) return '<div class="battle-empty">' + escHtml(emptyText || '暂无板块异动') + '</div>';
            return rows.map(function (s) {
                var leader = s.leader || {};
                var chg = numberValue(s.avg_change, 0);
                return '<div class="battle-sector-row">' +
                    '<div><div class="battle-row-title">' + escHtml(s.name || '-') + '</div>' +
                    '<div class="battle-row-sub">龙头 ' + escHtml(leader.name || leader.code || '-') + ' · 成分 ' + Number(s.stock_count || 0) + ' · 上涨 ' + Number(s.up_count || 0) + '</div></div>' +
                    '<div class="' + (chg >= 0 ? 'battle-up' : 'battle-down') + '">' + pct(chg) + '</div>' +
                '</div>';
            }).join('');
        }
        function renderRotationRows(rows, moneyField, emptyText) {
            rows = (rows || []).slice(0, 5);
            if (!rows.length) return '<div class="battle-empty">' + escHtml(emptyText || '暂无轮动数据') + '</div>';
            return rows.map(function (s) {
                var value = s[moneyField || 'main_net_inflow'];
                var chg = numberValue(s.change_pct || s.avg_change_pct || s.avg_change || 0, 0);
                return '<div class="battle-sector-row">' +
                    '<div><div class="battle-row-title">' + escHtml(s.name || '-') + '</div>' +
                    '<div class="battle-row-sub">资金 ' + fmtMoney(Number(value || 0)) + '</div></div>' +
                    '<div class="' + (chg >= 0 ? 'battle-up' : 'battle-down') + '">' + pct(chg) + '</div>' +
                '</div>';
            }).join('');
        }
        function renderPickRows(rows, emptyText) {
            rows = (rows || []).slice(0, 8);
            if (!rows.length) return '<div class="battle-empty">' + escHtml(emptyText || '暂无可确认候选') + '</div>';
            return rows.map(function (r) {
                var probability = extractProbability(r);
                var upside = probability.upside_probability_pct;
                var downside = probability.downside_probability_pct;
                var entry = numberValue(r.entry_price_low, 0) > 0 && numberValue(r.entry_price_high, 0) > 0
                    ? fmtPrice(r.entry_price_low) + '-' + fmtPrice(r.entry_price_high)
                    : '-';
                var action = String(r.signal_status || r.recommend_status || '').toUpperCase();
                var tone = (action === 'CONFIRM' || action === 'BUY_READY' || action === 'ALLOW') ? 'buy' : (action === 'SUSPENDED' || action === 'BLOCK' || action === 'SELL_ALERT' ? 'risk' : 'watch');
                return '<div class="battle-stock-row">' +
                    '<div class="battle-stock-main"><div class="battle-row-title">' + stockInline(r.stock_code, r.short_name) + ' <span class="battle-code">' + escHtml(r.stock_code || '') + '</span></div>' +
                    '<div class="battle-row-sub">入场 ' + escHtml(entry) + ' · 止损 ' + escHtml(fmtPrice(r.stop_loss_price)) + ' · 盈亏比 ' + fmt(numberValue(r.risk_reward_ratio, 0), 2) + '</div>' +
                    '<div class="battle-row-sub">' + escHtml(shortText(localizeMachineText(r.reason || r.recommend_reason || r.summary || r.sector_gate_reason), 92)) + '</div></div>' +
                    '<div class="battle-stock-side"><div class="battle-score">' + fmt(rowScore(r), 0) + '</div>' +
                    '<div>' + actionPill(action || 'WATCH', tone) + '</div>' +
                    '<div class="battle-row-sub">上' + (upside != null ? fmt(upside, 1) + '%' : '-') + ' / 下' + (downside != null ? fmt(downside, 1) + '%' : '-') + '</div>' +
                    '<div>' + sectorLabel(r.sector_gate_status) + '</div></div>' +
                '</div>';
            }).join('');
        }
        function renderCandidateRows(rows, emptyText) {
            rows = (rows || []).slice(0, 8);
            if (!rows.length) return '<div class="battle-empty">' + escHtml(emptyText || '暂无模拟动作') + '</div>';
            return rows.map(function (r) {
                var action = String(r.action || '').toUpperCase();
                var tone = action === 'BUY_READY' ? 'buy' : (action === 'SELL_ALERT' ? 'risk' : 'watch');
                var entry = numberValue(r.entry_price_low, 0) > 0 && numberValue(r.entry_price_high, 0) > 0
                    ? fmtPrice(r.entry_price_low) + '-' + fmtPrice(r.entry_price_high)
                    : '-';
                return '<div class="battle-stock-row">' +
                    '<div class="battle-stock-main"><div class="battle-row-title">' + stockInline(r.stock_code, r.short_name) + ' <span class="battle-code">' + escHtml(r.stock_code || '') + '</span></div>' +
                    '<div class="battle-row-sub">入场 ' + escHtml(entry) + ' · 止损 ' + escHtml(fmtPrice(r.stop_loss_price || r.trend_stop_price)) + ' · ' + escHtml(r.preferred_strategy_name || localizeMachineText(r.primary_strategy) || '-') + '</div>' +
                    '<div class="battle-row-sub">' + escHtml(shortText(localizeMachineText(r.action_reason), 100)) + '</div></div>' +
                    '<div class="battle-stock-side"><div class="battle-score">' + fmt(rowScore(r), 0) + '</div>' +
                    '<div>' + actionPill(r.action_label || action, tone) + '</div></div>' +
                '</div>';
            }).join('');
        }
        function renderHoldingRows(rows, emptyText) {
            rows = (rows || []).slice(0, 8);
            if (!rows.length) return '<div class="battle-empty">' + escHtml(emptyText || '暂无持仓风险') + '</div>';
            return rows.map(function (r) {
                var watch = r.watch_analysis || {};
                var guard = (watch.drawdown_guard || {}).level || '';
                var profit = numberValue(r.profit_pct, 0);
                var today = numberValue(r.today_profit, 0);
                var tone = r.macro_risk_triggered || guard === 'HIGH' || profit <= -5 ? 'risk' : (guard === 'MEDIUM' || profit <= -2 ? 'watch' : 'buy');
                return '<div class="battle-stock-row">' +
                    '<div class="battle-stock-main"><div class="battle-row-title">' + stockInline(r.stock_code, r.display_name || r.short_name) + ' <span class="battle-code">' + escHtml(r.stock_code || '') + '</span></div>' +
                    '<div class="battle-row-sub">现价 ' + escHtml(fmtPrice(r.cur_price)) + ' · 成本 ' + escHtml(fmtPrice(r.cost_price)) + ' · 持仓 ' + Number(r.shares || 0) + '</div>' +
                    '<div class="battle-row-sub">' + escHtml(shortText((watch.operation_advice || '') + ' ' + (watch.primary_reason || '') + ' ' + (r.macro_risk_reason || ''), 100)) + '</div></div>' +
                    '<div class="battle-stock-side"><div class="' + (profit >= 0 ? 'battle-up' : 'battle-down') + '">' + pct(profit) + '</div>' +
                    '<div class="' + (today >= 0 ? 'battle-up' : 'battle-down') + '">' + (today >= 0 ? '+' : '') + fmt(today, 0) + '</div>' +
                    '<div>' + quoteStatusLabel(r.quote_status) + '</div>' +
                    '<div>' + actionPill(guard || (tone === 'risk' ? '风险' : '正常'), tone) + '</div></div>' +
                '</div>';
            }).join('');
        }
        function renderBattle(data) {
            var clock = data.clock || {};
            var monitor = data.monitor || {};
            var rotation = data.rotation || {};
            var movement = data.movement || {};
            var rec = data.rec || {};
            var portfolio = data.portfolio || {};
            var candidates = data.candidates || {};
            var sim = data.sim || {};
            var qmt = data.qmt || {};
            var recRows = (rec.data || []).slice().sort(function (a, b) { return rowScore(b) - rowScore(a); });
            var candidateRows = candidates.data || [];
            var buyCandidates = candidateRows.filter(function (r) { return r.action === 'BUY_READY'; });
            var waitCandidates = candidateRows.filter(function (r) { return r.action === 'WAIT'; });
            var sellCandidates = candidateRows.filter(function (r) { return r.action === 'SELL_ALERT'; });
            var holdingRows = (portfolio.data || []).filter(function (r) { return Number(r.shares || 0) > 0; });
            var holdingRisks = holdingRows.filter(function (r) {
                var watch = r.watch_analysis || {};
                var guard = String((watch.drawdown_guard || {}).level || '').toUpperCase();
                var advice = String(watch.operation_advice || '');
                return r.macro_risk_triggered || guard === 'HIGH' || guard === 'MEDIUM' || Number(r.profit_pct || 0) <= -2 || /卖|减|风险|止损/.test(advice);
            }).sort(function (a, b) { return Number(a.profit_pct || 0) - Number(b.profit_pct || 0); });
            var quoteCounts = (portfolio.summary || {}).quote_status_counts || {};
            var freshQuotes = Number(quoteCounts.fresh || 0);
            var closedQuotes = Number(quoteCounts.closed || 0);
            var previousCloseQuotes = Number(quoteCounts.previous_close || 0);
            var staleQuotes = Number(quoteCounts.stale || 0);
            var missingQuotes = Number(quoteCounts.missing || 0);
            var quoteUsable = freshQuotes + closedQuotes + previousCloseQuotes;
            var quoteTotal = quoteUsable + staleQuotes + missingQuotes;
            var upCount = Number(monitor.up_count || 0);
            var downCount = Number(monitor.down_count || 0);
            var redRatio = upCount + downCount > 0 ? upCount / (upCount + downCount) * 100 : 0;
            var marketHeat = Number(monitor.market_heat || 0);
            var isLive = Boolean(clock.is_intraday) || Boolean(monitor.is_realtime);
            var qmtStatus = String(qmt.status || '').toLowerCase();
            var qmtOk = !qmt.status || qmtStatus === 'ok' || qmtStatus === 'warn';
            var risingSectors = (movement.industry_sectors || movement.sectors || []).filter(function (s) { return Number(s.avg_change || 0) >= 0.5; });
            var fallingSectors = (movement.industry_sectors || movement.sectors || []).filter(function (s) { return Number(s.avg_change || 0) <= -0.5; });
            var conceptSurge = (movement.concept_sectors || []).filter(function (s) { return Number(s.avg_change || 0) >= 0.5; });
            var rotationSignal = rotation.rotation_signal || {};
            var fundIn = rotationSignal.fund_in_sectors || [];
            var fundOut = rotationSignal.fund_out_sectors || [];
            var actionTitle = '只观察';
            var actionTone = 'watch';
            var actionText = '等待市场、板块和个股三项同时确认。';
            if (!clock.is_intraday) {
                actionTitle = '非盘中，只做预案';
                actionText = '现在不是连续竞价时段，推荐和复盘只能作为下一交易日计划。';
            } else if (!qmtOk || (quoteTotal > 0 && quoteUsable === 0)) {
                actionTitle = '实时源不稳';
                actionTone = 'risk';
                actionText = '先修复实时行情或等待快照恢复，避免用过期价格做盘中动作。';
            } else if (marketHeat < 350 || redRatio < 35 || fallingSectors.length > risingSectors.length + 2) {
                actionTitle = '先防守';
                actionTone = 'risk';
                actionText = '市场温度或红盘率偏弱，优先处理持仓风险，不追后排。';
            } else if (buyCandidates.length > 0 && (risingSectors.length || fundIn.length || conceptSurge.length)) {
                actionTitle = '可小仓试错';
                actionTone = 'buy';
                actionText = '只看买点就绪且板块仍在的前排，按入场区间和止损执行。';
            } else {
                actionText = '市场不算差，但个股买点或板块承接还不完整，继续等确认。';
            }
            var topRecConfirm = recRows.filter(function (r) {
                var s = String(r.signal_status || r.recommend_status || '').toUpperCase();
                return s === 'CONFIRM' || s === 'BUY_READY' || s === 'ALLOW';
            }).slice(0, 8);
            if (!topRecConfirm.length) topRecConfirm = recRows.slice(0, 8);

            var html = '';
            html += '<div class="battle-shell">';
            html += '<section class="battle-hero ' + actionTone + '">';
            html += '<div><div class="battle-eyebrow">ProBigA Intraday</div><h2>盘中作战台</h2>';
            html += '<p>当前动作：<strong>' + escHtml(actionTitle) + '</strong>。' + escHtml(actionText) + '</p></div>';
            html += '<div class="battle-hero-meta"><span>' + escHtml(clock.phase_label || '-') + '</span><span>' + escHtml(clock.server_time || localDateTimeString(new Date())) + '</span><span>推荐日 ' + escHtml(clock.recommendation_trade_date || candidates.date || '-') + '</span></div>';
            html += '</section>';

            html += '<section class="battle-metric-grid">';
            html += metric('市场热度', monitor.market_heat != null ? fmt(monitor.market_heat, 0) : '-', monitor.heat_status || '-', marketHeat >= 650 ? 'buy' : (marketHeat < 350 ? 'risk' : 'watch'));
            html += metric('红盘率', (upCount + downCount > 0 ? fmt(redRatio, 1) + '%' : '-'), '涨 ' + (upCount || 0) + ' / 跌 ' + (downCount || 0), redRatio >= 55 ? 'buy' : (redRatio < 35 ? 'risk' : 'watch'));
            html += metric('行情口径', quoteTotal ? quoteUsable + '/' + quoteTotal : (monitor.is_realtime ? '市场实时' : '-'), '收盘 ' + closedQuotes + ' / 滞后 ' + staleQuotes + ' / 缺失 ' + missingQuotes, quoteUsable > 0 || monitor.is_realtime ? 'buy' : 'watch');
            html += metric('买点就绪', String(buyCandidates.length), '等待 ' + waitCandidates.length + ' / 卖点 ' + sellCandidates.length, buyCandidates.length ? 'buy' : 'watch');
            html += metric('持仓风险', String(holdingRisks.length), '持仓 ' + holdingRows.length + ' 只', holdingRisks.length ? 'risk' : 'buy');
            html += metric('国金QMT状态', qmt.status || 'unknown', ((qmt.stock_current || {}).latest_snapshot_at || '') + '', qmtOk ? 'buy' : 'risk');
            html += '</section>';

            html += '<section class="battle-grid">';
            html += '<div class="battle-panel battle-panel-wide"><div class="battle-panel-title">盘中执行检查</div>';
            html += '<div class="battle-check-grid">';
            html += '<div><strong>市场：</strong>热度 ' + escHtml(fmt(marketHeat, 0)) + '，红盘率 ' + escHtml(fmt(redRatio, 1)) + '%，' + escHtml(monitor.is_realtime ? '使用实时快照' : '使用日线/缓存') + '</div>';
            html += '<div><strong>板块：</strong>拉升行业 ' + risingSectors.length + ' 个，跳水行业 ' + fallingSectors.length + ' 个，概念拉升 ' + conceptSurge.length + ' 个</div>';
            html += '<div><strong>个股：</strong>模拟买点 ' + buyCandidates.length + ' 只，AI确认候选 ' + topRecConfirm.length + ' 只</div>';
            html += '<div><strong>持仓：</strong>风险/减仓提醒 ' + holdingRisks.length + ' 只，今日盈亏 ' + escHtml(fmtMoney(Number((portfolio.summary || {}).today_hold_profit || 0))) + '</div>';
            html += '</div><div class="battle-mini-meta">页面自动刷新；离开本页会停止刷新。市场数据日 ' + escHtml(monitor.trade_date || clock.ui_trade_date || '-') + ' / 快照 ' + escHtml(movement.snapshot_time || '-') + '</div></div>';

            html += '<div class="battle-panel"><div class="battle-panel-title">进攻方向</div>';
            html += renderSectorRows(risingSectors, '暂无明显拉升行业');
            html += '</div>';
            html += '<div class="battle-panel"><div class="battle-panel-title">退潮方向</div>';
            html += renderSectorRows(fallingSectors, '暂无明显跳水行业');
            html += '</div>';
            html += '<div class="battle-panel"><div class="battle-panel-title">资金流入板块</div>';
            html += renderRotationRows(fundIn, 'main_net_inflow', '暂无资金流入方向');
            html += '</div>';
            html += '<div class="battle-panel"><div class="battle-panel-title">资金流出板块</div>';
            html += renderRotationRows(fundOut, 'main_net_inflow', '暂无资金流出方向');
            html += '</div>';
            html += '<div class="battle-panel battle-panel-wide"><div class="battle-panel-title">推荐股盘中确认</div>';
            html += renderPickRows(topRecConfirm, '暂无推荐股确认数据');
            html += '</div>';
            html += '<div class="battle-panel"><div class="battle-panel-title">模拟交易动作</div>';
            html += renderCandidateRows(buyCandidates.concat(sellCandidates).concat(waitCandidates).slice(0, 8), '暂无模拟动作');
            html += '<div class="battle-mini-meta"><a href="javascript:void(0)" onclick="switchTab(\'sim-trade\')">查看模拟交易全页</a></div>';
            html += '</div>';
            html += '<div class="battle-panel"><div class="battle-panel-title">持仓风险</div>';
            html += renderHoldingRows(holdingRisks, '暂无持仓风险提醒');
            html += '<div class="battle-mini-meta"><a href="javascript:void(0)" onclick="switchTab(\'portfolio\')">查看自选股全页</a></div>';
            html += '</div>';
            html += '</section></div>';
            return html;
        }
        function fetchBattle(silent) {
            if (!silent && !lastHtml) c.innerHTML = '<div class="loading">正在加载盘中作战台...</div>';
            return fetchJsonWithTimeout('/market-clock', 3000)
                .then(function (clock) {
                    applyMarketClock(clock);
                    var activeDate = (clock && clock.ui_trade_date) || d;
                    var recDate = (clock && clock.recommendation_trade_date) || activeDate;
                    return Promise.all([
                        fetchRawJsonWithTimeout('/api/monitor/data?date=' + encodeURIComponent(activeDate), 7000).catch(function () { return {}; }),
                        fetchJsonWithTimeout('/sector-rotation?trade_date=' + encodeURIComponent(activeDate) + '&days=10', 7000).catch(function () { return {}; }),
                        fetchRawJsonWithTimeout('/api/sector/movement?group_by=all', 7000).catch(function () { return {}; }),
                        fetchJsonWithTimeout('/recommended-stocks?trade_date=' + encodeURIComponent(recDate), 7000).catch(function () { return {}; }),
                        fetchRawJsonWithTimeout('/api/portfolio/list', 5000).catch(function () { return {}; }),
                        fetchRawJsonWithTimeout('/api/sim-trade/candidates?trade_mode=live&limit=80', 7000).catch(function () { return {}; }),
                        fetchRawJsonWithTimeout('/api/sim-trade/dashboard?trade_mode=live', 7000).catch(function () { return {}; }),
                        fetchRawJsonWithTimeout('/api/health/qmt-bridge', 5000).catch(function () { return {}; })
                    ]).then(function (results) {
                        return {
                            clock: clock || {},
                            monitor: results[0] || {},
                            rotation: results[1] || {},
                            movement: results[2] || {},
                            rec: results[3] || {},
                            portfolio: results[4] || {},
                            candidates: results[5] || {},
                            sim: results[6] || {},
                            qmt: results[7] || {},
                        };
                    });
                })
                .then(function (data) {
                    if (requestSeq !== intradayBattleRequestSeq) return;
                    lastHtml = renderBattle(data);
                    c.innerHTML = lastHtml;
                    setStatus('盘中作战台已刷新: ' + localDateTimeString(new Date()));
                    if (!portfolioLivePending) {
                        portfolioLivePending = true;
                        fetchRawJsonWithTimeout('/api/portfolio/live', 25000)
                            .then(function (livePortfolio) {
                                portfolioLivePending = false;
                                if (requestSeq !== intradayBattleRequestSeq || !livePortfolio || !livePortfolio.data) return;
                                data.portfolio = livePortfolio;
                                lastHtml = renderBattle(data);
                                c.innerHTML = lastHtml;
                                setStatus('盘中作战台持仓实时行情已补齐: ' + localDateTimeString(new Date()));
                            })
                            .catch(function () {
                                portfolioLivePending = false;
                            });
                    }
                })
                .catch(function (e) {
                    if (requestSeq !== intradayBattleRequestSeq) return;
                    if (!lastHtml) {
                        c.innerHTML = '<div class="loading" style="color:#e74c3c">盘中作战台加载失败: ' + escHtml(e.message || '网络错误') + '</div>';
                    }
                    setStatus('盘中作战台刷新失败: ' + (e.message || '网络错误'), true);
                });
        }
        fetchBattle(false);
        intradayBattleTimer = setInterval(function () {
            if (requestSeq !== intradayBattleRequestSeq) return;
            fetchBattle(true);
        }, isTradingTime() ? 30000 : 120000);
    }

    /* ===== 子视图切换 ===== */
    function subViewBar(containerId, views, activeId) {
        var h = '<div class="sub-view-bar" id="svb_' + containerId + '">';
        views.forEach(function (v) {
            var cls = v.id === activeId ? 'sub-view-btn active' : 'sub-view-btn';
            h += '<button class="' + cls + '" data-sv="' + v.id + '" onclick="switchSubView(\'' + containerId + '\',\'' + v.id + '\')">' + v.label + '</button>';
        });
        h += '</div>';
        return h;
    }
    window.switchSubView = function (containerId, viewId) {
        var bar = el('svb_' + containerId);
        if (bar) {
            bar.querySelectorAll('.sub-view-btn').forEach(function (b) {
                b.classList.toggle('active', b.getAttribute('data-sv') === viewId);
            });
        }
        if (containerId === 'sector' && viewId !== 'movement' && typeof window.stopSectorMovementRefresh === 'function') {
            window.stopSectorMovementRefresh();
        }
        var state = window._subViewState || (window._subViewState = {});
        state[containerId] = viewId;
        var handler = (state['_handler_' + containerId]);
        if (handler) handler(viewId);
    };
    function prepareSubViewContainer(container, containerId, views, defaultId, bodyId) {
        var state = window._subViewState || (window._subViewState = {});
        var activeId = state[containerId] || defaultId;
        if (!views.some(function (v) { return v.id === activeId; })) activeId = defaultId;
        var body = el(bodyId);
        var reuseBody = container && container.getAttribute && container.getAttribute('data-refreshing') === '1' &&
            body && container.contains(body) && hasRenderedContent(body);
        if (!reuseBody) {
            container.innerHTML = subViewBar(containerId, views, activeId) + '<div id="' + bodyId + '"></div>';
            body = el(bodyId);
        } else {
            markSilentRefreshTarget(body);
            var bar = el('svb_' + containerId);
            if (bar) {
                bar.querySelectorAll('.sub-view-btn').forEach(function (b) {
                    b.classList.toggle('active', b.getAttribute('data-sv') === activeId);
                });
            }
        }
        state[containerId] = activeId;
        return { state: state, body: body, activeId: activeId, reused: reuseBody };
    }

    function commentaryCollectForm() {
        return {
            id: Number((el('commentaryProfileId') || {}).value || 0) || null,
            profile_name: (el('commentaryProfileName') || {}).value || '',
            text: (el('commentaryText') || {}).value || '',
            reference_date: (el('commentaryReferenceDate') || {}).value || null,
            phase: (el('commentaryPhase') || {}).value || 'premarket',
            cron_time: (el('commentaryCronTime') || {}).value || '08:55',
            webhook_kind: (el('commentaryWebhookKind') || {}).value || 'briefing',
            enabled: !!((el('commentaryEnabled') || {}).checked),
            push_enabled: !!((el('commentaryPushEnabled') || {}).checked)
        };
    }

    function commentaryFillForm(profile) {
        profile = profile || {};
        if (el('commentaryProfileId')) el('commentaryProfileId').value = profile.id || '';
        if (el('commentaryProfileName')) el('commentaryProfileName').value = profile.profile_name || '';
        if (el('commentaryText')) el('commentaryText').value = profile.text || '';
        if (el('commentaryReferenceDate')) el('commentaryReferenceDate').value = profile.reference_date || '';
        if (el('commentaryPhase')) el('commentaryPhase').value = profile.phase || 'premarket';
        if (el('commentaryCronTime')) el('commentaryCronTime').value = profile.cron_time || '08:55';
        if (el('commentaryWebhookKind')) el('commentaryWebhookKind').value = profile.webhook_kind || 'briefing';
        if (el('commentaryEnabled')) el('commentaryEnabled').checked = profile.enabled !== false;
        if (el('commentaryPushEnabled')) el('commentaryPushEnabled').checked = profile.push_enabled !== false;
        try { localStorage.setItem('probiga_commentary_selected', String(profile.id || '')); } catch (e) {}
    }

    function commentaryStatusClass(status) {
        if (status === 'TRACK') return 'track';
        if (status === 'WATCH') return 'watch';
        if (status === 'RISK') return 'risk';
        return 'neutral';
    }

    function commentaryRenderResult(res) {
        var box = el('commentaryResult');
        if (!box) return;
        if (!res) {
            box.innerHTML = '<div class="commentary-empty">还没有评估结果，先粘贴股评文本再点“立即评估”。</div>';
            return;
        }
        var payload = res.result || res;
        var feasibility = payload.project_feasibility || {};
        var items = payload.items || [];
        var h = '';
        h += '<div class="commentary-result-head">';
        h += '<div><strong>参考日:</strong> ' + (payload.reference_date || '-') + ' <strong>交易日:</strong> ' + (payload.trade_date || '-') + ' <strong>模式:</strong> ' + (payload.phase || '-') + '</div>';
        h += '<div><strong>可行性:</strong> 盘前 ' + (feasibility.premarket || '-') + ' / 盘中 ' + (feasibility.intraday || '-') + '</div>';
        h += '</div>';
        if (!items.length) {
            h += '<div class="commentary-empty">没有解析出可评估的股票条目。</div>';
            box.innerHTML = h;
            return;
        }
        items.forEach(function (item) {
            if (item.error) {
                h += '<div class="commentary-item commentary-item-error">';
                h += '<div class="commentary-item-top"><div class="commentary-item-title">' + (item.stock_name || '-') + ' (' + (item.stock_code || '-') + ')</div></div>';
                h += '<div class="commentary-item-desc">评估失败: ' + item.error + '</div>';
                h += '</div>';
                return;
            }
            var verdict = item.verdict || {};
            var current = item.current || {};
            var anchor = item.anchor || {};
            var checks = item.checks || [];
            h += '<div class="commentary-item">';
            h += '<div class="commentary-item-top">';
            h += '<div><div class="commentary-item-title">' + (item.index || '-') + '. ' + item.stock_name + ' (' + item.stock_code + ')</div><div class="commentary-item-meta">' + (item.sector || '未分组') + '</div></div>';
            h += '<div class="commentary-badge ' + commentaryStatusClass(verdict.status) + '">' + (verdict.status || '-') + '</div>';
            h += '</div>';
            h += '<div class="commentary-item-grid">';
            h += '<div><span>现价</span><strong>' + fmt(current.price, 2) + '</strong></div>';
            h += '<div><span>涨跌幅</span><strong class="' + clsPct(current.change_pct) + '">' + pct(current.change_pct) + '</strong></div>';
            h += '<div><span>量比</span><strong>' + fmt(current.volume_ratio, 2) + '</strong></div>';
            h += '<div><span>5日资金</span><strong>' + fmtMoney(current.flow_5d_wan ? current.flow_5d_wan * 10000 : null) + '</strong></div>';
            h += '<div><span>启动日</span><strong>' + (anchor.trade_date || '-') + '</strong></div>';
            h += '<div><span>启动低点</span><strong>' + fmt(anchor.low, 2) + '</strong></div>';
            h += '</div>';
            h += '<div class="commentary-item-desc">' + (verdict.summary || '-') + '</div>';
            if (item.description) h += '<div class="commentary-item-source">' + item.description + '</div>';
            h += '<div class="commentary-check-row">';
            checks.forEach(function (check) {
                if (check.status === 'info') return;
                h += '<span class="commentary-check-pill pill-' + check.status + '" title="' + escAttr(check.detail || '') + '">' + check.label + '</span>';
            });
            h += '</div>';
            if (item.news && item.news.items && item.news.items.length) {
                h += '<div class="commentary-news-list">';
                item.news.items.slice(0, 3).forEach(function (news) {
                    var title = firstNonEmptyText(news.title, news.content).slice(0, 60);
                    h += '<div class="commentary-news-item">[' + (news.source || '-') + '] ' + title + '</div>';
                });
                h += '</div>';
            }
            h += '</div>';
        });
        box.innerHTML = h;
    }

    function commentaryRenderPage(container, profiles) {
        profiles = profiles || [];
        var h = '';
        h += '<div class="commentary-page">';
        h += '<div class="commentary-editor">';
        h += '<div class="commentary-card">';
        h += '<div class="commentary-card-title">股评输入与监控配置</div>';
        h += '<input type="hidden" id="commentaryProfileId">';
        h += '<div class="commentary-form-grid">';
        h += '<div><label>配置名称</label><input id="commentaryProfileName" type="text" placeholder="例如：6月13日科技方向股评"></div>';
        h += '<div><label>股评发布日期</label><input id="commentaryReferenceDate" type="date"></div>';
        h += '<div><label>评估模式</label><select id="commentaryPhase"><option value="premarket">盘前</option><option value="intraday">盘中</option></select></div>';
        h += '<div><label>自动推送时间</label><input id="commentaryCronTime" type="time" value="08:55"></div>';
        h += '<div><label>Webhook 通道</label><select id="commentaryWebhookKind"><option value="briefing">briefing</option><option value="default">default</option><option value="news">news</option></select></div>';
        h += '<div class="commentary-checkboxes"><label><input id="commentaryEnabled" type="checkbox" checked> 启用配置</label><label><input id="commentaryPushEnabled" type="checkbox" checked> 允许推送</label></div>';
        h += '</div>';
        h += '<label class="commentary-text-label">股评原文</label>';
        h += '<textarea id="commentaryText" placeholder="把微信群、文章或笔记里的股评原文直接粘过来。系统会自动抽取代码、名称、锚点日期和规则检查项。"></textarea>';
        h += '<div class="commentary-actions">';
        h += '<button onclick="commentaryRunDraft()">立即评估</button>';
        h += '<button class="secondary" onclick="commentarySaveProfile()">保存配置</button>';
        h += '<button class="secondary" onclick="commentarySaveAndPush()">保存并推送</button>';
        h += '<button class="ghost" onclick="commentaryNewProfile()">新建</button>';
        h += '</div>';
        h += '</div>';
        h += '<div class="commentary-card">';
        h += '<div class="commentary-card-title">评估结果</div>';
        h += '<div id="commentaryResult" class="commentary-result"></div>';
        h += '</div>';
        h += '</div>';
        h += '<div class="commentary-sidebar">';
        h += '<div class="commentary-card">';
        h += '<div class="commentary-card-title">已保存配置</div>';
        if (!profiles.length) {
            h += '<div class="commentary-empty">还没有保存的股评监控配置。</div>';
        } else {
            profiles.forEach(function (profile) {
                var task = profile.task || {};
                h += '<div class="commentary-profile-card">';
                h += '<div class="commentary-profile-head"><strong>' + profile.profile_name + '</strong><span class="commentary-mini-badge ' + (profile.enabled ? 'enabled' : 'disabled') + '">' + (profile.enabled ? '启用' : '停用') + '</span></div>';
                h += '<div class="commentary-profile-meta">' + (profile.phase === 'intraday' ? '盘中' : '盘前') + ' / ' + (profile.reference_date || '-') + ' / ' + (profile.cron_time || '-') + '</div>';
                h += '<div class="commentary-profile-meta">推送 ' + (profile.push_enabled ? '开启' : '关闭') + ' / webhook ' + (profile.webhook_kind || '-') + '</div>';
                if (task && task.id) h += '<div class="commentary-profile-meta">任务 #' + task.id + ' / ' + (task.enabled ? '已启用' : '已停用') + '</div>';
                h += '<div class="commentary-profile-actions">';
                h += '<button onclick="commentaryLoadProfile(' + profile.id + ')">载入</button>';
                h += '<button class="secondary" onclick="commentaryRunProfile(' + profile.id + ', false)">评估</button>';
                h += '<button class="secondary" onclick="commentaryRunProfile(' + profile.id + ', true)">推送</button>';
                h += '<button class="ghost" onclick="commentaryEnsureTask(' + profile.id + ')">同步任务</button>';
                h += '<button class="ghost" onclick="commentaryToggleProfile(' + profile.id + ')">' + (profile.enabled ? '停用' : '启用') + '</button>';
                h += '</div>';
                h += '</div>';
            });
        }
        h += '</div>';
        h += '</div>';
        h += '</div>';
        container.innerHTML = h;
        window._commentaryProfiles = profiles;
        var selected = '';
        try { selected = localStorage.getItem('probiga_commentary_selected') || ''; } catch (e) {}
        var active = null;
        profiles.forEach(function (profile) {
            if (String(profile.id) === String(selected)) active = profile;
        });
        if (!active && profiles.length) active = profiles[0];
        if (active) commentaryFillForm(active);
        commentaryRenderResult(window._commentaryLastResult || null);
    }

    window.commentaryNewProfile = function () {
        commentaryFillForm({ profile_name: '', text: '', phase: 'premarket', cron_time: '08:55', webhook_kind: 'briefing', enabled: true, push_enabled: true });
        commentaryRenderResult(window._commentaryLastResult || null);
    };

    window.commentaryLoadProfile = function (profileId) {
        var profiles = window._commentaryProfiles || [];
        profiles.forEach(function (profile) {
            if (Number(profile.id) === Number(profileId)) commentaryFillForm(profile);
        });
    };

    window.commentaryRunDraft = function () {
        var payload = commentaryCollectForm();
        fetch('/api/commentary/assess', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: payload.text, reference_date: payload.reference_date, phase: payload.phase })
        }).then(function (r) { return r.json(); }).then(function (res) {
            window._commentaryLastResult = res;
            commentaryRenderResult(res);
        }).catch(function (err) { alert('评估失败: ' + err.message); });
    };

    window.commentarySaveProfile = function () {
        var payload = commentaryCollectForm();
        fetch('/api/commentary/profiles', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        }).then(function (r) { return r.json(); }).then(function (res) {
            if (!res.success) { alert('保存失败'); return; }
            if (res.profile) commentaryFillForm(res.profile);
            loadTab('commentary');
        }).catch(function (err) { alert('保存失败: ' + err.message); });
    };

    window.commentarySaveAndPush = function () {
        var payload = commentaryCollectForm();
        fetch('/api/commentary/profiles', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        }).then(function (r) { return r.json(); }).then(function (res) {
            if (!res.success || !res.profile) { alert('保存失败'); return null; }
            commentaryFillForm(res.profile);
            return fetch('/api/commentary/profiles/' + res.profile.id + '/run?push=true', { method: 'POST' });
        }).then(function (r) {
            if (!r) return null;
            return r.json();
        }).then(function (res) {
            if (!res) return;
            window._commentaryLastResult = res.result;
            commentaryRenderResult(res.result);
            alert(res.push && res.push.success ? '已推送到企业微信' : '评估完成，但推送失败');
            loadTab('commentary');
        }).catch(function (err) { alert('操作失败: ' + err.message); });
    };

    window.commentaryRunProfile = function (profileId, push) {
        fetch('/api/commentary/profiles/' + profileId + '/run?push=' + (push ? 'true' : 'false'), { method: 'POST' })
            .then(function (r) { return r.json(); })
            .then(function (res) {
                window._commentaryLastResult = res.result;
                commentaryRenderResult(res.result);
                if (res.profile) commentaryFillForm(res.profile);
                if (push) alert(res.push && res.push.success ? '已推送到企业微信' : ('推送失败: ' + ((res.push || {}).error || '未知错误')));
                loadTab('commentary');
            }).catch(function (err) { alert('执行失败: ' + err.message); });
    };

    window.commentaryEnsureTask = function (profileId) {
        fetch('/api/commentary/profiles/' + profileId + '/task/ensure', { method: 'POST' })
            .then(function (r) { return r.json(); })
            .then(function (res) {
                alert('调度任务已同步，任务ID: ' + res.id);
                loadTab('commentary');
            }).catch(function (err) { alert('同步任务失败: ' + err.message); });
    };

    window.commentaryToggleProfile = function (profileId) {
        fetch('/api/commentary/profiles/' + profileId + '/toggle', { method: 'POST' })
            .then(function (r) { return r.json(); })
            .then(function () { loadTab('commentary'); })
            .catch(function (err) { alert('切换失败: ' + err.message); });
    };

    function loadResearchRadarPage(d, c) {
        c.innerHTML = '<div class="loading">加载研报趋势雷达...</div>';
        apiGet('/research-radar?trade_date=' + encodeURIComponent(d)).then(function (res) {
            if (res.error) {
                c.innerHTML = '<div class="loading" style="color:#e74c3c">加载失败: ' + escHtml(res.error) + '</div>';
                return;
            }
            var themes = res.themes || [];
            var sourceSignals = res.source_signals || [];
            var reportSources = res.report_sources || [];
            var stockPool = res.stock_pool || [];
            var h = '';
            h += '<div class="radar-shell">';
            h += '<div class="radar-header">';
            h += '<div><div class="radar-eyebrow">ProBigA Research Radar</div><h2>研报趋势雷达</h2><p>' + escHtml(res.method || '') + '</p></div>';
            h += '<div class="radar-meta"><strong>' + escHtml(res.trade_date || d) + '</strong><span>更新 ' + escHtml(res.generated_at || '-') + '</span></div>';
            h += '</div>';

            h += '<div class="radar-theme-grid">';
            themes.forEach(function (theme) {
                var hits = (theme.market_hits || []).slice(0, 3).map(function (hit) {
                    var chg = Number(hit.change_pct || 0);
                    return '<span class="' + (chg >= 0 ? 'radar-hit up' : 'radar-hit down') + '">' + escHtml(hit.name) + ' ' + pct(chg) + '</span>';
                }).join('');
                var stocks = (theme.stocks || []).slice(0, 5).map(function (s) {
                    return '<span class="radar-stock-chip">' + escHtml(s.name) + '<em>' + escHtml(s.role) + '</em></span>';
                }).join('');
                h += '<section class="radar-theme-card">';
                h += '<div class="radar-theme-top"><div><h3>' + escHtml(theme.name) + '</h3><span>' + escHtml(theme.trend || '-') + ' · ' + escHtml(theme.evidence_level || '-') + '</span></div><strong>' + (theme.score || '-') + '</strong></div>';
                h += '<p>' + escHtml(theme.logic || '') + '</p>';
                h += '<div class="radar-chip-row">' + stocks + '</div>';
                h += '<div class="radar-mini-title">盘面映射</div>';
                h += '<div class="radar-hit-row">' + (hits || '<span class="radar-empty-inline">等待盘面验证</span>') + '</div>';
                h += '<div class="radar-note"><b>验证：</b>' + escHtml(theme.verification || '-') + '</div>';
                h += '<div class="radar-note risk"><b>风险：</b>' + escHtml(theme.risk || '-') + '</div>';
                h += '</section>';
            });
            h += '</div>';

            h += '<div class="radar-grid">';
            h += '<section class="radar-panel"><h3>信息源权重</h3>';
            sourceSignals.forEach(function (src) {
                h += '<div class="radar-source-row"><div><strong>' + escHtml(src.name) + '</strong><span>' + escHtml(src.type) + ' · ' + escHtml(src.focus) + '</span></div><em>' + escHtml(src.weight) + '</em></div>';
            });
            h += '</section>';

            h += '<section class="radar-panel"><h3>研报与财报验证</h3>';
            reportSources.forEach(function (src) {
                h += '<a class="radar-link-row" href="' + escAttr(src.url) + '" target="_blank" rel="noreferrer"><span>' + escHtml(src.title) + '</span><em>' + escHtml(src.tag || '') + '</em></a>';
            });
            h += '</section>';
            h += '</div>';

            h += '<section class="radar-panel radar-stock-panel"><h3>股票池映射</h3>';
            h += '<div class="table-wrap"><table><thead><tr><th>代码</th><th>名称</th><th>主线</th><th>环节</th><th>验证层级</th></tr></thead><tbody>';
            stockPool.forEach(function (s) {
                h += '<tr><td>' + escHtml(s.code) + '</td><td>' + nameLink(s.code, s.name) + '</td><td>' + escHtml(s.theme) + '</td><td>' + escHtml(s.role) + '</td><td>' + escHtml(s.tier) + '</td></tr>';
            });
            h += '</tbody></table></div>';
            h += '<p class="radar-disclaimer">' + escHtml(res.disclaimer || '仅用于研究跟踪，不构成投资建议。') + '</p>';
            h += '</section>';
            h += '</div>';
            c.innerHTML = h;
        }).catch(function (e) {
            c.innerHTML = '<div class="loading" style="color:#e74c3c">加载失败: ' + escHtml(e.message || e) + '</div>';
        });
    }

    function loadHunterPage(d, c) {
        c.innerHTML = '<div class="loading">加载狩猎场候选池...</div>';
        var recDate = recommendationDateValue ? recommendationDateValue() : d;
        Promise.all([
            fetchJsonWithTimeout('/recommended-stocks?trade_date=' + encodeURIComponent(recDate), 9000).catch(function () { return {}; }),
            fetchRawJsonWithTimeout('/api/portfolio/list', 4500).catch(function () { return {}; }),
            fetchJsonWithTimeout('/sector-rotation?trade_date=' + encodeURIComponent(recDate) + '&days=10', 7000).catch(function () { return {}; })
        ]).then(function (results) {
            var rec = results[0] || {};
            var portfolio = results[1] || {};
            var rotation = results[2] || {};
            var rows = (rec.data || []).slice().sort(function (a, b) {
                return (blendedAnalysisRowScore(b) || 0) - (blendedAnalysisRowScore(a) || 0);
            });
            var holdingSet = {};
            (portfolio.data || []).forEach(function (p) {
                var code = String(p.stock_code || '').trim().padStart(6, '0');
                if (Number(p.shares || 0) > 0) holdingSet[code] = true;
            });
            var buyReady = rows.filter(function (r) { return /BUY_READY|CONFIRM/.test(String(r.signal_status || r.recommend_status || '')); }).length;
            var watch = rows.filter(function (r) { return String(r.signal_status || r.recommend_status || '').indexOf('WATCH') >= 0; }).length;
            var riskBlocked = rows.filter(function (r) { return /BLOCK|SELL|HIGH|CRITICAL/.test(String(r.signal_status || '') + ' ' + String(r.recommend_status || '') + ' ' + String(r.event_risk_level || '')); }).length;
            var avgScore = rows.length ? rows.reduce(function (sum, r) { return sum + Number(blendedAnalysisRowScore(r) || 0); }, 0) / rows.length : 0;
            var strongSectors = (rotation.rising_sectors || rotation.rising_concepts || []).slice(0, 4).map(function (s) { return s.name; }).filter(Boolean);

            function num(v, fallback) {
                var n = Number(v);
                return isNaN(n) ? (fallback == null ? null : fallback) : n;
            }
            function score(v) {
                var n = num(v, 0);
                return Math.max(0, Math.min(100, n));
            }
            function scoreColor(v) {
                var n = score(v);
                if (n >= 80) return '#dc2626';
                if (n >= 70) return '#d97706';
                if (n >= 60) return '#1d4ed8';
                return '#64748b';
            }
            function strategyLabel(v) {
                var map = {
                    ultra_short: '超短',
                    short_term: '短线',
                    swing: '波段',
                    main_wave: '主升'
                };
                return map[String(v || '')] || String(v || '综合');
            }
            function statusMeta(r) {
                var s = String(r.signal_status || r.recommend_status || 'WATCH').toUpperCase();
                if (s === 'BUY_READY' || s === 'CONFIRM') return { text: s === 'CONFIRM' ? '确认' : '买点', cls: 'buy' };
                if (s.indexOf('SELL') >= 0 || s === 'BLOCK') return { text: '风险', cls: 'risk' };
                return { text: '观察', cls: 'watch' };
            }
            function parseList(v) {
                if (!v) return [];
                if (Array.isArray(v)) return v;
                try {
                    var parsed = JSON.parse(v);
                    return Array.isArray(parsed) ? parsed : [];
                } catch (e) {
                    return String(v).split(/[;；,，]/).map(function (x) { return x.trim(); }).filter(Boolean);
                }
            }
            function dimensionScore(r, key) {
                if (key === 'technical') return firstAnalysisValue(r.technical, r.technical_score, r.entry_score, 0) || 0;
                if (key === 'capital') return firstAnalysisValue(r.capital_score, r.chip_capital_score, 0) || 0;
                if (key === 'sector') return firstAnalysisValue(r.sector_rotation_score, r.sector_score, 0) || 0;
                if (key === 'chip') return firstAnalysisValue(r.chip_capital_score, r.confidence_score, 0) || 0;
                if (key === 'risk') {
                    var risk = String(r.event_risk_level || '').toUpperCase();
                    if (risk === 'LOW') return 85;
                    if (risk === 'MEDIUM') return 62;
                    if (risk === 'HIGH') return 38;
                    if (risk === 'CRITICAL') return 15;
                    return firstAnalysisValue(r.event_score, 60) || 60;
                }
                return 0;
            }
            function scoreBar(label, value) {
                var n = score(value);
                return '<div class="hunter-score-row"><span>' + escHtml(label) + '</span><div class="hunter-score-track"><i style="width:' + n + '%;background:' + scoreColor(n) + '"></i></div><strong>' + Math.round(n) + '</strong></div>';
            }
            function pricePlan(r) {
                var low = num(r.entry_price_low);
                var high = num(r.entry_price_high);
                var stop = num(r.stop_loss_price || r.trend_stop_price);
                var take = num(r.take_profit_1 || r.resistance_price);
                var bits = [];
                if (low != null && high != null) bits.push('买区 ' + fmtPrice(low) + '-' + fmtPrice(high));
                if (stop != null) bits.push('止损 ' + fmtPrice(stop));
                if (take != null) bits.push('目标 ' + fmtPrice(take));
                if (r.risk_reward_ratio != null) bits.push('盈亏比 ' + fmt(Number(r.risk_reward_ratio), 2));
                return bits.length ? bits.join(' / ') : '等待交易计划';
            }
            function emptyState() {
                var resultDate = rec.date || recDate || '-';
                return '<div class="hunter-empty">' +
                    '<div class="hunter-empty-kicker">候选池为空</div>' +
                    '<h3>暂无可展示候选</h3>' +
                    '<p>结果日 ' + escHtml(resultDate) + ' 没有进入狩猎场的候选。可以先生成 AI 推荐，或切换到已有推荐结果日查看。</p>' +
                    '<div class="hunter-empty-actions">' +
                    '<button onclick="switchTab(\'recommended\')">前往 AI 推荐</button>' +
                    '<button onclick="refreshAll()">刷新狩猎场</button>' +
                    '</div>' +
                    '</div>';
            }
            function card(r, idx) {
                var s = statusMeta(r);
                var code = String(r.stock_code || '');
                var name = r.short_name || code;
                var finalScore = blendedAnalysisRowScore(r) || 0;
                var strategy = r.primary_strategy || r.strategy_profile || 'all';
                var entryConditions = parseList(r.entry_conditions_json).slice(0, 2).map(localizeMachineText);
                var sellRules = parseList(r.sell_rules_json).slice(0, 2).map(localizeMachineText);
                var failureTags = parseList(r.failure_tags_json).slice(0, 4).map(localizeMachineText);
                var tags = failureTags.map(function (t) { return '<span class="hunter-tag risk">' + escHtml(t) + '</span>'; }).join('');
                if (holdingSet[code]) tags = '<span class="hunter-tag hold">已持仓</span>' + tags;
                if (r.main_wave_signal) tags += '<span class="hunter-tag">' + escHtml(localizeMachineText(r.main_wave_signal)) + '</span>';
                var chg = num(r.change_pct);
                var chgCls = chg != null && chg >= 0 ? 'up' : 'down';
                return '<article class="hunter-card" data-score="' + Math.round(finalScore) + '" data-status="' + escAttr(s.cls) + '" data-strategy="' + escAttr(strategy) + '">' +
                    '<div class="hunter-card-top">' +
                    '<div><div class="hunter-rank">#' + (idx + 1) + ' · ' + escHtml(strategyLabel(strategy)) + '</div><h3>' + nameLink(code, name) + '<em>' + escHtml(code) + '</em></h3></div>' +
                    '<div class="hunter-score" style="color:' + scoreColor(finalScore) + '">' + Math.round(finalScore) + '</div>' +
                    '</div>' +
                    '<div class="hunter-pill-row"><span class="hunter-pill ' + s.cls + '">' + escHtml(s.text) + '</span><span class="hunter-pill neutral">仓位 ' + escHtml(r.position_weight != null ? fmt(Number(r.position_weight), 1) + '%' : '-') + '</span><span class="hunter-pill neutral">持有 ' + escHtml(r.max_holding_days || '-') + '天</span></div>' +
                    '<div class="hunter-market-line"><span>现价 <b>' + escHtml(fmtPrice(r.price)) + '</b></span><span class="' + chgCls + '">' + escHtml(chg == null ? '-' : pct(chg)) + '</span><span>成交额 ' + escHtml(fmtMoney(Number(r.amount || 0))) + '</span></div>' +
                    '<div class="hunter-plan">' + escHtml(pricePlan(r)) + '</div>' +
                    '<div class="hunter-score-grid">' +
                    scoreBar('技术', dimensionScore(r, 'technical')) +
                    scoreBar('资金', dimensionScore(r, 'capital')) +
                    scoreBar('板块', dimensionScore(r, 'sector')) +
                    scoreBar('筹码', dimensionScore(r, 'chip')) +
                    scoreBar('风控', dimensionScore(r, 'risk')) +
                    '</div>' +
                    '<div class="hunter-rule-box"><strong>入场</strong>' + (entryConditions.length ? entryConditions.map(function (x) { return '<span>' + escHtml(x) + '</span>'; }).join('') : '<span>等待买点确认</span>') + '</div>' +
                    '<div class="hunter-rule-box sell"><strong>退出</strong>' + (sellRules.length ? sellRules.map(function (x) { return '<span>' + escHtml(x) + '</span>'; }).join('') : '<span>按止损/趋势规则退出</span>') + '</div>' +
                    '<div class="hunter-tag-row">' + (tags || '<span class="hunter-tag">暂无风险标签</span>') + '</div>' +
                    '<div class="hunter-actions">' +
                    '<button onclick="openKlineModal(\'' + escAttr(code) + '\',\'' + escAttr(name) + '\')">K线</button>' +
                    '<button onclick="pfAddWithCode(\'' + escAttr(code) + '\')">加自选</button>' +
                    '<button onclick="switchTab(\'recommended\')">AI详情</button>' +
                    '</div>' +
                    '</article>';
            }

            var h = '';
            h += '<div class="hunter-shell">';
            h += '<section class="hunter-hero">';
            h += '<div><div class="hunter-eyebrow">ProBigA Hunter</div><h2>狩猎场</h2><p>主线观察：' + escHtml(strongSectors.join('、') || '等待板块确认') + '</p></div>';
            h += '<div class="hunter-hero-meta"><strong>' + Math.round(avgScore) + '</strong><span>平均交易分</span><em>结果日 ' + escHtml(rec.date || recDate) + '</em></div>';
            h += '</section>';
            h += '<section class="hunter-metric-grid">';
            h += '<div><span>候选</span><strong>' + rows.length + '</strong><em>' + escHtml((rec.freshness || {}).status_label || '推荐池') + '</em></div>';
            h += '<div><span>买点</span><strong>' + buyReady + '</strong><em>买入就绪 / 确认</em></div>';
            h += '<div><span>观察</span><strong>' + watch + '</strong><em>等待确认</em></div>';
            h += '<div><span>风险</span><strong>' + riskBlocked + '</strong><em>阻断/卖出/高风险</em></div>';
            h += '<div><span>轮动</span><strong>' + escHtml(strongSectors[0] || '-').slice(0, 6) + '</strong><em>' + escHtml(strongSectors.slice(1).join('、') || '等待板块数据') + '</em></div>';
            h += '</section>';
            h += '<section class="hunter-toolbar">';
            h += '<label>策略 <select id="hunterStrategy" onchange="hunterFilter()"><option value="">全部</option><option value="ultra_short">超短</option><option value="short_term">短线</option><option value="swing">波段</option><option value="main_wave">主升</option></select></label>';
            h += '<label>状态 <select id="hunterStatus" onchange="hunterFilter()"><option value="">全部</option><option value="buy">买点</option><option value="watch">观察</option><option value="risk">风险</option></select></label>';
            h += '<label>最低分 <select id="hunterMinScore" onchange="hunterFilter()"><option value="0">不限</option><option value="60">60+</option><option value="70">70+</option><option value="80">80+</option></select></label>';
            h += '<button onclick="refreshAll()">刷新狩猎场</button>';
            h += '<span id="hunterFilterInfo"></span>';
            h += '</section>';
            h += '<section class="hunter-grid" id="hunterGrid">' + (rows.length ? rows.slice(0, 60).map(card).join('') : emptyState()) + '</section>';
            h += '</div>';
            c.innerHTML = h;
            window.hunterFilter();
        }).catch(function (e) {
            c.innerHTML = '<div class="loading" style="color:#e74c3c">狩猎场加载失败: ' + escHtml(e.message || e) + '</div>';
        });
    }

    window.hunterFilter = function () {
        var strategy = (el('hunterStrategy') || {}).value || '';
        var status = (el('hunterStatus') || {}).value || '';
        var minScore = Number((el('hunterMinScore') || {}).value || 0);
        var cards = document.querySelectorAll('#hunterGrid .hunter-card');
        var shown = 0;
        [].forEach.call(cards, function (card) {
            var ok = true;
            if (strategy && card.getAttribute('data-strategy') !== strategy) ok = false;
            if (status && card.getAttribute('data-status') !== status) ok = false;
            if (Number(card.getAttribute('data-score') || 0) < minScore) ok = false;
            card.style.display = ok ? '' : 'none';
            if (ok) shown += 1;
        });
        var info = el('hunterFilterInfo');
        if (info) info.textContent = '显示 ' + shown + ' / ' + cards.length;
    };

    /* ===== Tabs ===== */
    var LOADERS = {
        /* ── 市场概览 ── */
        command: function (d, c) {
            loadCommandPage(d, c);
        },
        monitor: function (d, c) {
            var views = [{id:'dashboard', label:'📊 仪表盘'}, {id:'sentiment', label:'🧠 情绪详情'}];
            var prepared = prepareSubViewContainer(c, 'monitor', views, 'dashboard', 'monitorBody');
            var body = prepared.body;
            var state = prepared.state;
            state['_handler_monitor'] = function (vid) {
                if (vid === 'dashboard') { loadMonitorPage(body, d); }
                else {
                    if (typeof window.stopMonitorRefresh === 'function') {
                        window.stopMonitorRefresh();
                    }
                    loadSentimentPage(d, body);
                }
            };
            state['_handler_monitor'](prepared.activeId);
        },
        /* ── 热股排行（融合 + 四大来源） ── */
        fused: function (d, c) {
            var views = [{id:'fused', label:'🔥 融合榜'}, {id:'east', label:'✨ 东财'}, {id:'ths', label:'🏆 同花顺'}, {id:'xq', label:'❄️ 雪球'}, {id:'sina', label:'🌐 新浪'}];
            var prepared = prepareSubViewContainer(c, 'fused', views, 'fused', 'fusedBody');
            var body = prepared.body;
            var state = prepared.state;
            state['_handler_fused'] = function (vid) {
                if (vid === 'fused') loadFusedTab(d, body);
                else if (vid === 'east') loadEastTab(d, body);
                else if (vid === 'ths') loadThsTab(d, body);
                else if (vid === 'xq') loadXqTab(d, body);
                else if (vid === 'sina') loadSinaTab(d, body);
            };
            state['_handler_fused'](prepared.activeId);
        },
        /* ── 板块分析（异动 + 轮动 + 热度） ── */
        sector: function (d, c) {
            var views = [{id:'movement', label:'🌊 异动监控'}, {id:'rotation', label:'🔄 轮动分析'}, {id:'heat', label:'🌡 热度矩阵'}];
            var prepared = prepareSubViewContainer(c, 'sector', views, 'movement', 'sectorBody');
            var body = prepared.body;
            var state = prepared.state;
            state['_handler_sector'] = function (vid) {
                if (vid === 'movement') loadSectorMovementPage(body);
                else if (vid === 'rotation') loadSectorRotationPage(d, body);
                else if (vid === 'heat') loadSectorHeatPage(d, body);
            };
            state['_handler_sector'](prepared.activeId);
        },
        /* ── 强势股（3天/5天切换） ── */
        strong: function (d, c) {
            var views = [{id:'3', label:'🔥 近3天'}, {id:'5', label:'🔥 近5天'}];
            var prepared = prepareSubViewContainer(c, 'strong', views, '3', 'strongBody');
            var body = prepared.body;
            var state = prepared.state;
            state['_handler_strong'] = function (vid) { loadMulti(d, parseInt(vid), body); };
            state['_handler_strong'](prepared.activeId);
        },
        /* ── 概念/行业（类型 + 天数切换） ── */
        concept: function (d, c) {
            var views = [{id:'c0', label:'🏷️ 当日概念'}, {id:'c3', label:'📋 3天概念'}, {id:'c5', label:'📋 5天概念'}, {id:'i3', label:'🏭 3天行业'}, {id:'i5', label:'🏭 5天行业'}];
            var prepared = prepareSubViewContainer(c, 'concept', views, 'c0', 'conceptBody');
            var body = prepared.body;
            var state = prepared.state;
            state['_handler_concept'] = function (vid) {
                if (vid === 'c0') {
                    apiGet('/concept-ths-live').then(function (live) {
                        if (live.data && live.data.length) { renderConceptData(body, live.data, true, live.time); }
                        else {
                            apiGet('/concept-ths?snapshot_date=' + d).then(function (res) {
                                syncDateFromResponse(res);
                                if (!res.data || !res.data.length) { body.innerHTML = '<div class="loading">暂无数据</div>'; return; }
                                renderConceptData(body, res.data, false, res.date || d);
                            });
                        }
                    }).catch(function () {
                        apiGet('/concept-ths?snapshot_date=' + d).then(function (res) {
                            syncDateFromResponse(res);
                            if (!res.data || !res.data.length) { body.innerHTML = '<div class="loading">暂无数据</div>'; return; }
                            renderConceptData(body, res.data, false, res.date || d);
                        });
                    });
                } else if (vid === 'c3') loadConceptMulti(d, 3, 1, body);
                else if (vid === 'c5') loadConceptMulti(d, 5, 1, body);
                else if (vid === 'i3') loadConceptMulti(d, 3, 2, body);
                else if (vid === 'i5') loadConceptMulti(d, 5, 2, body);
            };
            state['_handler_concept'](prepared.activeId);
        },
        /* ── 龙虎榜 ── */
        alist: function (d, c) {
            apiGet('/a-list-daily?trade_date=' + d).then(function (res) {
                syncDateFromResponse(res);
                if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无龙虎榜数据</div>'; return; }
                var h = '<div class="stats-bar">' + card('龙虎榜股票', res.total, 'blue') + '</div>';
                window.renderTable(c, 'alist', ['代码', '名称', '收盘价', '涨跌幅', '换手率', '净买入额', '买入额', '卖出额', '总成交额', '上榜原因', '席位'], res.data, function (r) {
                    return '<tr><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.close, 2) + '</td><td class="' + clsPct(r.change_cpt) + '">' + pct(r.change_cpt) + '</td><td>' + fmt(r.turnover_ratio, 2) + '%</td><td class="' + clsPct(r.a_net_amount) + '">' + fmtMoney(r.a_net_amount) + '</td><td>' + fmtMoney(r.a_buy_amount) + '</td><td>' + fmtMoney(r.a_sell_amount) + '</td><td>' + fmtMoney(r.amount) + '</td><td style="max-width:200px;white-space:normal;font-size:11px">' + (r.reason || '-') + '</td><td><button onclick="showAListDetail(\'' + d + '\',\'' + r.stock_code + '\',\'' + (r.short_name || '') + '\')" style="padding:2px 8px;font-size:11px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer">查看</button></td></tr>';
                }, 50, h);
            });
        },
        /* ── 资金流向（个股资金 + 实时查询） ── */
        capital: function (d, c) {
            var views = [{id:'daily', label:'💰 每日资金'}, {id:'realtime', label:'⏱ 实时查询'}];
            var prepared = prepareSubViewContainer(c, 'capital', views, 'daily', 'capitalBody');
            var body = prepared.body;
            var state = prepared.state;
            state['_handler_capital'] = function (vid) {
                if (vid === 'daily') {
                    if (!prepared.reused || !el('capResult2')) body.innerHTML = '<div id="capResult2"></div>';
                    loadCap2(prepared.reused);
                    if (window._capAutoRefresh) clearInterval(window._capAutoRefresh);
                    window._capAutoRefreshDone = false;
                    window._capAutoRefresh = setInterval(function () {
                        var active = document.querySelector('.sidebar-item.active');
                        if (active && active.getAttribute('data-tab') === 'capital') {
                            if (isTradingTime()) { window._capAutoRefreshDone = false; loadCap2(true); }
                            else if (!window._capAutoRefreshDone) { window._capAutoRefreshDone = true; loadCap2(true); }
                        }
                    }, 5000);
                } else if (!prepared.reused || !el('rtCode')) {
                    body.innerHTML = '<div class="search-bar"><input type="text" id="rtCode" placeholder="输入股票代码" style="width:130px"><button onclick="loadRT()">查询</button><span id="rtInfo"></span></div><div id="rtResult"></div>';
                }
            };
            state['_handler_capital'](prepared.activeId);
        },
        /* ── 主力行为 ── */
        mainforce: function (d, c) {
            loadMainforcePage(d, c);
        },
        /* ── 选股工具 ── */
        screen: function (d, c) {
            var modes = [
                {id:'startup',label:'🚀 趋势启动',desc:'盘整突破+放量+均线上翘+非垃圾板块'},
                {id:'macd',label:'📉 MACD金叉',desc:'EMA12>EMA26多头 + KDJ低位+代码0/60'},
                {id:'flow',label:'💰 资金流入',desc:'主力净流入 ≥ 500万'},
                {id:'k_day',label:'📊 K线筛选',desc:'涨幅+换手率区间'},
                {id:'trend',label:'📈 多头趋势',desc:'MA5>MA10>MA20 多头排列'},
                {id:'trend_strong',label:'🔥 强势趋势票',desc:'四线多头+连续站MA5≥5天+距新高10%+温和量比'},
                {id:'low_start',label:'🚀 低位放量',desc:'距近20日低点≤5% + 放量1.5倍'},
                {id:'ladder',label:'🔗 连板股',desc:'2~5连板'},
                {id:'lhb',label:'🏦 龙虎榜',desc:'上榜个股，游资/机构动向'},
            ];
            var h = '<div class="stats-bar">' + card('🎯 选股策略', modes.length, 'blue') + '</div>';
            h += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:12px">';
            modes.forEach(function(m) {
                h += '<div class="stat-card screen-card" id="scard_' + m.id + '" style="cursor:pointer;flex:1;min-width:140px;border:2px solid transparent;transition:all 0.2s" onclick="runScreen(\'' + m.id + '\')"><div class="label">' + m.label + '</div><div class="value" style="font-size:13px;color:#666">' + m.desc + '</div></div>';
            });
            h += '</div><div id="screenResult" style="margin-top:8px"><div class="loading">加载中...</div></div>';
            c.innerHTML = h;
            setTimeout(function() { runScreen('macd'); }, 100);
        },
        'jq-picks': function (d, c) {
            loadJqPicksPage(c);
        },
        'recommended': function (d, c) {
            loadRecommendedPage(currentDateValue() || d, c);
        },
        hunter: function (d, c) {
            loadHunterPage(recommendationDateValue() || d, c);
        },
        /* ── 持仓管理 ── */
        news: function (d, c) {
            c.innerHTML = '<div class="loading">加载中...</div>';
            Promise.all([
                apiGet('/news-flash?rn=200&pages=3'),
                apiGet('/news-important?pages=3'),
                apiGet('/news-history?limit=200')
            ]).then(function (results) {
                var flashRes = results[0], impRes = results[1], histRes = results[2];
                var allItems = flashRes.data || [];
                var impItems = impRes.data || [];
                var histItems = histRes.data || [];
                if (!allItems.length && !histItems.length) { c.innerHTML = '<div class="loading">暂无快讯数据</div>'; return; }

                var srcMap = {cls:'财联社', eastmoney:'东方财富', sina:'新浪财经'};
                var srcColor = {cls:'#e74c3c', eastmoney:'#1a73e8', sina:'#f5a623'};
                var srcCount = {};
                allItems.forEach(function(n){(n.sources||[n.source]).forEach(function(s){srcCount[s]=(srcCount[s]||0)+1;});});

                var h = '<div class="stats-bar">';
                h += card('全部快讯', allItems.length, 'blue');
                h += card('重要快讯', impItems.length, 'red');
                h += card('历史存储', histItems.length, 'green');
                Object.keys(srcCount).forEach(function(s){h += card(srcMap[s]||s, srcCount[s], srcColor[s]==='#e74c3c'?'red':srcColor[s]==='#1a73e8'?'blue':'orange');});
                h += '</div>';
                h += '<div class="news-tabs">';
                h += '<button class="news-tab active" onclick="switchNewsTab(this,\'all\')">📰 全部聚合</button>';
                h += '<button class="news-tab" onclick="switchNewsTab(this,\'important\')">🔥 重要快讯</button>';
                h += '<button class="news-tab" onclick="switchNewsTab(this,\'history\')">📚 历史数据</button>';
                h += '</div>';
                h += '<div class="search-bar"><input type="text" id="s_news" placeholder="🔍 搜索标题/内容/个股..." oninput="newsFilter()"><span id="info_news"></span></div>';
                h += '<div id="news_list"></div>';
                c.innerHTML = h;

                window._news_mode = 'all';
                window.switchNewsTab = function(btn, mode) {
                    document.querySelectorAll('.news-tab').forEach(function(t){t.classList.remove('active');});
                    btn.classList.add('active');
                    window._news_mode = mode;
                    var src = mode === 'important' ? impItems : mode === 'history' ? histItems : allItems;
                    newsRender(src);
                    el('info_news').textContent = '共 ' + src.length + ' 条';
                };
                window.newsFilter = function () {
                    var kw = (el('s_news') || {}).value.toLowerCase();
                    var src = window._news_mode === 'important' ? impItems : window._news_mode === 'history' ? histItems : allItems;
                    var filtered = kw ? src.filter(function (n) {
                        var stockText = (n.stocks || []).map(function(s){return (s.name||s)+' '+(s.code||'');}).join(' ');
                        var subjText = (n.subjects || []).map(function(s){return s.name||s;}).join(' ');
                        return (n.title + n.content + stockText + subjText).toLowerCase().indexOf(kw) > -1;
                    }) : src;
                    newsRender(filtered);
                    el('info_news').textContent = kw ? '匹配 ' + filtered.length + ' 条' : '共 ' + filtered.length + ' 条';
                };
                window.newsRender = function (list) {
                    var html = '';
                    list.forEach(function (n) {
                        var isHigh = (n.importance_score >= 4) || n.level === 'A';
                        var isMid = (n.importance_score >= 2) || n.level === 'B';
                        var cls = isHigh ? 'news-item news-important' : (isMid ? 'news-item news-notable' : 'news-item');
                        html += '<div class="' + cls + '">';

                        html += '<div class="news-header">';
                        html += '<span class="news-time">' + (n.time || n.publish_time || '-') + '</span>';

                        var sources = n.sources || [n.source];
                        sources.forEach(function(s) {
                            var label = srcMap[s] || s;
                            var color = srcColor[s] || '#999';
                            html += ' <span class="news-badge" style="background:' + color + ';color:#fff">' + label + '</span>';
                        });

                        if (n.level && n.level !== 'C') html += ' <span class="news-badge badge-level">Level ' + n.level + '</span>';
                        if (n.jpush) html += ' <span class="news-badge badge-jpush">推送</span>';
                        if (n.is_top) html += ' <span class="news-badge badge-top">置顶</span>';
                        if (n.bold) html += ' <span class="news-badge badge-bold">加粗</span>';
                        if (n.importance_score) html += ' <span class="news-badge badge-score">评分' + n.importance_score + '</span>';
                        if (n.reading_num >= 10000) html += ' <span class="news-badge badge-hot">' + (n.reading_num >= 100000 ? '🔥' : '📈') + (n.reading_num/10000).toFixed(1) + '万阅</span>';
                        if (n.author) html += ' <span class="news-badge badge-author">' + n.author + '</span>';
                        html += '</div>';

                        html += '<div class="news-title">' + (n.title || '快讯') + '</div>';
                        html += '<div class="news-content">' + (n.content || '') + '</div>';

                        var tags = '';
                        if (n.stocks && n.stocks.length) {
                            tags += n.stocks.map(function(s) {
                                var name = s.name || s;
                                return '<span class="stock-tag" title="' + (s.code||'') + '">' + name + '</span>';
                            }).join('');
                        }
                        if (n.subjects && n.subjects.length) {
                            tags += n.subjects.map(function(s) {
                                return '<span class="subj-tag">' + (s.name || s) + '</span>';
                            }).join('');
                        }
                        if (tags) html += '<div class="news-tags">' + tags + '</div>';
                        html += '</div>';
                    });
                    el('news_list').innerHTML = html;
                    el('info_news').textContent = '共 ' + list.length + ' 条';
                };
                newsRender(allItems);
                el('info_news').textContent = '共 ' + allItems.length + ' 条';
            }).catch(function (e) { c.innerHTML = '<div class="loading">加载失败: ' + e + '</div>'; });
        },
        'research-radar': function (d, c) {
            loadResearchRadarPage(d, c);
        },
        notice: function (d, c) {
            c.innerHTML = '<div id="noticeResult"><div class="loading">请输入股票代码或直接回车查看最新公告</div></div>';
            window.loadNotices = function () {
                var codeEl = el('noticeCode');
                var code = codeEl ? codeEl.value.trim() : '';
                var r = el('noticeResult');
                r.innerHTML = '<div class="loading">加载中...</div>';
                var qs = code ? '?stock_code=' + code + '&limit=100' : '?limit=100';
                var toolbar = '<div class="search-bar paged-table-toolbar"><input type="text" id="noticeCode" value="' + escAttr(code) + '" placeholder="输入股票代码查询" style="width:150px"><button onclick="loadNotices()">查询</button><span id="noticeInfo" style="font-size:12px;color:#888"></span></div>';
                apiGet('/stock-notices' + qs).then(function (res) {
                    if (!res.data || !res.data.length) {
                        r.classList.add('paged-table-section');
                        r.innerHTML = '<div class="paged-sticky-panel">' + toolbar + '</div><div class="loading">暂无公告数据<br><span style="font-size:12px;color:#999;margin-top:8px;display:inline-block">公告数据需定期同步到数据库（si_notice_eastmoney），当前暂无已同步的公告记录。</span></div>';
                        return;
                    }
                    toolbar = toolbar.replace('<span id="noticeInfo" style="font-size:12px;color:#888"></span>', '<span id="noticeInfo" style="font-size:12px;color:#888">共 ' + res.total + ' 条</span>');
                    var cols = ['股票代码', '公告日期', '标题', '分类', '详情'];
                    window.renderTable(r, 'notice_t', cols, res.data, function (n) {
                        var titleHtml = n.detail_url ? '<a href="' + n.detail_url + '" target="_blank" style="color:#1a73e8;text-decoration:none">' + (n.title || '-') + '</a>' : (n.title || '-');
                        return '<tr><td>' + (n.stock_code || '-') + '</td><td>' + (n.notice_date || '-') + '</td><td style="max-width:400px;white-space:normal;font-size:12px">' + titleHtml + '</td><td style="font-size:12px;color:#888">' + (n.column_name || '-') + '</td><td>' + (n.detail_url ? '<a href="' + n.detail_url + '" target="_blank" style="color:#1a73e8;font-size:12px">查看</a>' : '-') + '</td></tr>';
                    }, 30, toolbar);
                }).catch(function () { r.innerHTML = '<div class="loading">加载失败</div>'; });
            };
            loadNotices();
        },
        commentary: function (d, c) {
            fetch('/api/commentary/profiles').then(function (r) { return r.json(); }).then(function (res) {
                commentaryRenderPage(c, res.data || []);
            }).catch(function (err) {
                c.innerHTML = '<div class="loading" style="color:#e74c3c">加载失败: ' + err.message + '</div>';
            });
        },
        review: function (d, c) {
            // 先加载基础复盘，同时加载专业复盘
            Promise.all([
                apiGet('/daily-review?review_date=' + d),
                apiGet('/daily-review/pro?review_date=' + d)
            ]).then(function(results) {
                var res = results[0];
                var proRes = results[1];
                syncDateFromResponse(res);
                if (!res.data || !res.data.length) {
                    c.innerHTML = '<div class="loading" style="padding:20px"><p>当前日期暂无复盘数据</p><button onclick="genReviewBtn(\'' + d + '\')" style="margin-top:10px;padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:14px">🔄 生成复盘数据</button></div>';
                    return;
                }
                var r = res.data[0];
                var hasPro = proRes && proRes.data && proRes.data.length && proRes.data[0].pro_review;
                var proData = hasPro ? proRes.data[0] : null;

                var html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
                html += '<span style="font-size:16px;font-weight:700;color:#e0e0e0">📋 复盘数据 | ' + r.review_date + '</span>';
                html += '<div><button onclick="exportReview(\'' + r.review_date + '\')" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:12px;margin-right:8px">📥 导出</button>';
                if (hasPro) html += '<button onclick="exportReview(\'' + r.review_date + '\')" style="padding:6px 14px;border:none;border-radius:6px;background:#4caf50;color:#fff;cursor:pointer;font-size:12px">📥 导出专业版</button>';
                html += '</div></div>';

                // Tab 切换
                if (hasPro) {
                    html += '<div style="display:flex;gap:4px;margin-bottom:16px">';
                    html += '<span id="tab-pro-basic" onclick="switchReviewTab(\'basic\')" style="padding:6px 16px;border-radius:6px 6px 0 0;cursor:pointer;font-size:13px;background:#2a2a3e;color:#1a73e8;font-weight:600">📊 数据总览</span>';
                    html += '<span id="tab-pro-pro" onclick="switchReviewTab(\'pro\')" style="padding:6px 16px;border-radius:6px 6px 0 0;cursor:pointer;font-size:13px;background:#1e1e1e;color:#888">📋 专业复盘</span>';
                    html += '</div>';
                }

                // 基础复盘内容
                html += '<div id="review-basic">';
                var hot = jsonF(r.hot_sectors, []);
                var cold = jsonF(r.cold_sectors, []);
                var volUp = jsonF(r.volume_up_sectors, []);
                var volDown = jsonF(r.volume_down_sectors, []);
                var idxA = jsonF(r.index_analysis, []);
                var amt = (Number(r.total_amount || 0) / 1e8).toFixed(0);
                var idxChg = Number(r.index_change_pct || 0);

                html += '<div class="stats-bar">' + card('市场热度', (r.market_heat || '-') + '%', 'blue') + card('情绪周期', r.sentiment_cycle || '-', 'orange') + card('涨停/跌停', Number(r.limit_up_count || 0) + '/' + Number(r.limit_down_count || 0), 'red') + card('炸板率', (r.broken_rate != null ? Number(r.broken_rate).toFixed(1) + '%' : '-'), 'green') + card('最高连板', (r.max_boards != null ? r.max_boards + '板' : '-'), 'blue') + card('成交额', amt + '亿', 'orange') + card(r.index_name || '指数', (r.index_price || '-'), 'red') + card('涨跌幅', (idxChg >= 0 ? '+' : '') + idxChg.toFixed(2) + '%', idxChg >= 0 ? 'red' : 'green') + card('量能', r.total_amount_change || '-', 'blue') + card('观望', (r.sideline_ratio || '-') + '%', 'comment') + '</div>';
                html += '<p style="margin:12px 0;color:#aaa;line-height:1.6">' + (r.market_heat_note || '') + '</p>';

                if ((hot && hot.length) || (cold && cold.length)) {
                    html += '<div style="display:flex;gap:20px;flex-wrap:wrap">';
                    if (hot && hot.length) {
                        html += '<div style="flex:1;min-width:280px"><h4 style="color:#f44336;margin:0 0 8px">🔥 热度上升板块</h4><table style="width:100%;font-size:13px;border-collapse:collapse"><thead><tr style="color:#888"><td>名称</td><td style="text-align:right">涨跌幅</td></tr></thead><tbody>';
                        hot.forEach(function(s) { var chg=Number(s.change_pct||0); html += '<tr><td style="padding:4px 8px">'+s.name+'</td><td style="padding:4px 8px;text-align:right" class="'+(chg>=0?'c-red':'c-green')+'">'+(chg>=0?'+':'')+chg.toFixed(2)+'%</td></tr>'; });
                        html += '</tbody></table></div>';
                    }
                    if (cold && cold.length) {
                        html += '<div style="flex:1;min-width:280px"><h4 style="color:#4caf50;margin:0 0 8px">❄ 热度下降板块</h4><table style="width:100%;font-size:13px;border-collapse:collapse"><thead><tr style="color:#888"><td>名称</td><td style="text-align:right">涨跌幅</td></tr></thead><tbody>';
                        cold.forEach(function(s) { var chg=Number(s.change_pct||0); html += '<tr><td style="padding:4px 8px">'+s.name+'</td><td style="padding:4px 8px;text-align:right" class="'+(chg>=0?'c-red':'c-green')+'">'+(chg>=0?'+':'')+chg.toFixed(2)+'%</td></tr>'; });
                        html += '</tbody></table></div>';
                    }
                    html += '</div>';
                }

                if ((volUp && volUp.length) || (volDown && volDown.length)) {
                    html += '<div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:16px">';
                    if (volUp && volUp.length) { html += '<div style="flex:1;min-width:200px"><h4 style="color:#ff9800;margin:0 0 8px">📈 放量板块</h4>'; volUp.forEach(function(s) { html += '<span style="display:inline-block;background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;margin:2px;font-size:12px">'+s.name+'</span>'; }); html += '</div>'; }
                    if (volDown && volDown.length) { html += '<div style="flex:1;min-width:200px"><h4 style="color:#2196f3;margin:0 0 8px">📉 缩量板块</h4>'; volDown.forEach(function(s) { html += '<span style="display:inline-block;background:#e3f2fd;color:#1565c0;padding:2px 8px;border-radius:4px;margin:2px;font-size:12px">'+s.name+'</span>'; }); html += '</div>'; }
                    html += '</div>';
                }

                if (idxA && idxA.length) {
                    html += '<div style="margin-top:16px"><h4 style="color:#e0e0e0;margin:0 0 8px">📊 指数技术分析</h4>';
                    idxA.forEach(function(ia) { html += '<div style="background:#1e1e1e;padding:10px 16px;border-radius:6px;margin-bottom:8px;font-size:13px;color:#ccc"><b>'+(ia.name||'')+'</b> ('+(ia.price||'')+') - '+(ia.note||'')+' | 均线:'+(ia.ma20||'-')+'</div>'; });
                    html += '</div>';
                }

                if (r.summary) html += '<div style="margin-top:16px;background:#1a1a2e;border-left:3px solid #1a73e8;padding:12px 16px;border-radius:4px"><h4 style="color:#e0e0e0;margin:0 0 6px">📝 综合结论</h4><p style="color:#aaa;line-height:1.8;font-size:13px;white-space:pre-wrap;margin:0">'+(r.summary||'')+'</p></div>';
                html += '<p style="margin-top:10px;color:#666;font-size:11px">'+(r.disclaimer||'')+'</p>';
                html += '</div>';

                // 专业复盘内容
                if (hasPro) {
                    html += '<div id="review-pro" style="display:none">';
                    html += '<div style="background:#111827;border-radius:8px;padding:22px 24px;font-size:14px;line-height:1.75;color:#cbd5e1;font-family:system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">';
                    html += _renderProReview(proData.pro_review);
                    html += '</div></div>';
                }

                c.innerHTML = html;
            });
        },
        portfolio: function (d, c) {
            function pfFmtProfit(v) {
                var n = Number(v || 0);
                var cls = n >= 0 ? 'c-red' : 'c-green';
                return '<strong class="' + cls + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '</strong>';
            }
            function pfFmtFlow(v) {
                if (v == null || v === '') return '-';
                var n = Number(v || 0);
                var cls = n >= 0 ? 'c-red' : 'c-green';
                return '<span class="' + cls + '">' + (n >= 0 ? '+' : '') + fmtMoney(n) + '</span>';
            }
            function pfFlowCell(r) {
                function flowCls(level) {
                    return (level === 'strong_in' || level === 'in') ? 'c-red' : ((level === 'strong_out' || level === 'out') ? 'c-green' : 'c-gray');
                }
                if (r.flow_status === 'fresh' && (r.flow_5m != null || r.flow_attitude_label)) {
                    var label = r.flow_attitude_label || '分钟';
                    var level = r.flow_attitude || 'neutral';
                    var cls = flowCls(level);
                    var ratio = r.flow_attitude_ratio != null ? ' / ' + Number(r.flow_attitude_ratio).toFixed(1) + '%' : '';
                    return '<div class="' + cls + '" title="5分钟实时资金；1m ' + fmtMoney(Number(r.flow_1m || 0)) + ' / 15m ' + fmtMoney(Number(r.flow_15m || 0)) + '">' + label + ratio + '</div>' +
                        '<div style="font-size:10px;color:#888">5m ' + pfFmtFlow(r.flow_5m) + '</div>';
                }
                var basis = r.flow_attitude_basis || (r.flow_status === 'stale' && r.flow_5m != null ? 'minute_5m_stale' : 'daily_flow');
                var staleMinute = basis === 'minute_5m_stale';
                var title = staleMinute ? '分时资金稍旧，展示最近5分钟快照' : '最近日资金';
                var label2 = r.flow_attitude_label || '中性';
                var ratio2 = r.flow_attitude_ratio != null && staleMinute ? ' / ' + Number(r.flow_attitude_ratio).toFixed(1) + '%' : '';
                var sub = staleMinute ? ('5m ' + pfFmtFlow(r.flow_5m)) : pfFmtFlow(r.main_net_inflow);
                var time = staleMinute ? shortDateTimeText(r.flow_latest_time || r.flow_trade_date || '') : (r.flow_trade_date || '-');
                return '<div class="' + flowCls(r.flow_attitude || 'neutral') + '" title="' + escAttr(title) + '">' + escHtml(label2) + ratio2 + '</div>' +
                    '<div style="font-size:10px;color:#888">' + sub + ' · ' + escHtml(time || '-') + '</div>';
            }
            function pfWatchCell(r) {
                var a = r.watch_analysis || {};
                var guard = a.drawdown_guard || {};
                var fundsLevel = a.funds_level || r.flow_attitude || 'neutral';
                var cls = (fundsLevel === 'strong_in' || fundsLevel === 'in') ? 'c-red' : ((fundsLevel === 'strong_out' || fundsLevel === 'out') ? 'c-green' : 'c-gray');
                var advice = a.operation_advice || '-';
                var fundsText = (a.funds || r.flow_attitude_label || '-') + '(' + (a.funds_source_label || '-') + ')';
                var meta = '趋势 ' + (a.trend || '-') + ' / 资金 ' + fundsText + ' / 热度 ' + (a.heat || '-');
                var risk = a.risk_tip || '暂无明显';
                var guardLevel = String(guard.level || 'LOW').toUpperCase();
                var guardColor = guardLevel === 'HIGH' ? '#dc2626' : (guardLevel === 'MEDIUM' ? '#d97706' : (guardLevel === 'DATA' ? '#64748b' : '#16a34a'));
                var guardText = guard.action ? ('控回撤 ' + guard.action) : '控回撤 正常';
                var guardLine = guard.stop_loss_line ? ('止损线 ' + fmtPrice(guard.stop_loss_line)) : (guard.reason || '');
                return '<button type="button" class="pf-watch-box pf-watch-button" onclick="event.stopPropagation();pfOpenWatchAdvice(\'' + escAttr(r.stock_code) + '\')" title="' + escAttr(meta + '；风险 ' + risk + '；数据 ' + (a.freshness || '-')) + '">' +
                    '<div class="pf-watch-topline"><span class="' + cls + ' pf-watch-primary">' + escHtml(advice) + '</span><span class="pf-watch-open">详情</span></div>' +
                    '<div class="pf-watch-meta">' + escHtml(meta) + '</div>' +
                    '<div class="pf-watch-guard" style="color:' + guardColor + '" title="' + escAttr(guard.reason || '') + '">' + escHtml(guardText + (guardLine ? ' / ' + guardLine : '')) + '</div>' +
                    '<div class="pf-watch-risk">风险 ' + escHtml(risk) + '</div>' +
                    '</button>';
            }
            function pfLiveIntervalMs() {
                var saved = 0;
                try { saved = Number(localStorage.getItem('probiga_pf_live_ms') || 5000); } catch (e) { saved = 5000; }
                return saved === 3000 ? 3000 : 5000;
            }
            function pfLiveUrl(force) {
                return '/api/portfolio/live' + (force ? '?force=true' : '');
            }
            function pfIsActiveTab() {
                return activeTabId() === 'portfolio';
            }
            function pfQuoteStatusText(r) {
                var status = r.quote_status || 'missing';
                var age = r.quote_age_seconds;
                var ageText = age != null ? (' ' + Math.round(Number(age)) + 's') : '';
                if (status === 'fresh') return '实时' + ageText;
                if (status === 'closed') return '收盘';
                if (status === 'previous_close') return '上一收盘';
                if (status === 'stale') return '滞后' + ageText;
                return '缺失';
            }
            function pfQuoteSourceText(source) {
                var s = String(source || 'gj_qmt');
                if (s === 'gj_qmt') return '国金QMT';
                if (s === 'daily_kline') return '日线收盘';
                if (s === 'qmt_close_table') return '国金QMT收盘';
                if (s === 'qmt_close_archive') return '国金QMT收盘档案';
                if (s === 'qmt_live_table') return '国金QMT表快照';
                if (s === 'qmt_live_table_stale') return '国金QMT表快照(非实时)';
                if (s.indexOf('qmt') >= 0 || s.indexOf('QMT') >= 0) return s.replace(/qmt/ig, '国金QMT');
                return s;
            }
            function pfDataMoment(r) {
                r = r || {};
                var quoteRaw = r.quote_snapshot_at || r.snapshot_at || r.quote_trade_date || '';
                var flowRaw = r.flow_latest_time || r.flow_trade_date || '';
                var quote = quoteRaw ? shortDateTimeText(quoteRaw) : '';
                var flow = flowRaw ? shortDateTimeText(flowRaw) : '';
                var titleParts = [];
                if (quoteRaw) titleParts.push('行情 ' + cleanDateTimeText(quoteRaw));
                if (flowRaw) titleParts.push('资金 ' + cleanDateTimeText(flowRaw));
                var html = '';
                if (quote) html += '<span class="pf-time-primary">行情 ' + escHtml(quote) + '</span>';
                if (flow) html += '<span class="' + (quote ? 'pf-time-secondary' : 'pf-time-primary') + '">资金 ' + escHtml(flow) + '</span>';
                return { html: html || '-', title: titleParts.join(' / ') };
            }
            function pfSetDataMomentCell(cell, r) {
                if (!cell) return;
                var moment = pfDataMoment(r);
                cell.innerHTML = moment.html;
                if (moment.title) cell.setAttribute('title', moment.title);
                else cell.removeAttribute('title');
            }
            function pfLiveStatusText(res, prefix) {
                var s = (res && res.summary) || {};
                var qc = s.quote_status_counts || {};
                var t = shortDateTimeText(s.quote_generated_at || localDateTimeString(new Date()));
                var marketText = '';
                if (qc.fresh) marketText = '实时 ' + qc.fresh;
                else if (qc.closed) marketText = '收盘 ' + qc.closed;
                else if (qc.previous_close) marketText = '上一收盘 ' + qc.previous_close;
                else marketText = '行情 0';
                return (prefix || '国金QMT') + ' ' + (pfLiveIntervalMs() / 1000) + 's' +
                    (t ? ' · ' + t : '') +
                    ' · ' + marketText +
                    (qc.stale ? ' · 滞后 ' + qc.stale : '') +
                    (qc.missing ? ' · 缺失 ' + qc.missing : '');
            }
            function pfUpdateLiveStatus(res, prefix) {
                var status = document.getElementById('pfLiveStatus');
                if (status) status.textContent = pfLiveStatusText(res, prefix);
            }
            function pfSetLiveStatusText(text, isError) {
                var status = document.getElementById('pfLiveStatus');
                if (!status) return;
                status.textContent = text;
                status.style.color = isError ? '#dc2626' : '#9ca3af';
            }
            function pfApplyLivePayload(res, prefix) {
                if (!res || !res.data) return;
                if (res.summary) pfUpdateSummary(res);
                res.data.forEach(pfUpdateRow);
                pfUpdateLiveStatus(res, prefix);
            }
            function pfFetchAndApplyLive(prefix) {
                if (window._pfLiveInFlight) return Promise.resolve(null);
                window._pfLiveInFlight = true;
                window._pfLastAutoRefreshAt = Date.now();
                return fetchRawJsonWithTimeout(pfLiveUrl(false), 9000).then(function(res) {
                    pfApplyLivePayload(res, prefix);
                    return res;
                }).catch(function(e) {
                    pfSetLiveStatusText('自动刷新失败，等待下一轮 · ' + shortDateTimeText(localDateTimeString(new Date())), true);
                    return null;
                }).finally(function() {
                    window._pfLiveInFlight = false;
                });
            }
            window.pfApplyPortfolioLivePayload = pfApplyLivePayload;
            function pfUpdateSummary(res) {
                var s = res.summary || {};
                var elTotal = document.getElementById('pfTotalProfit');
                var elToday = document.getElementById('pfTodayProfit');
                var elCnt = document.getElementById('pfHoldingCount');
                var elOpen = document.getElementById('pfTodayOpenCount');
                var elCleared = document.getElementById('pfTodayClearedCount');
                var elDrawdown = document.getElementById('pfDrawdownAlerts');
                if (elTotal) elTotal.innerHTML = pfFmtProfit(s.total_hold_profit);
                if (elToday) elToday.innerHTML = pfFmtProfit(s.today_hold_profit);
                if (elCnt) elCnt.textContent = String(s.holding_count != null ? s.holding_count : 0);
                if (elOpen) elOpen.textContent = String(s.today_open_count != null ? s.today_open_count : 0);
                if (elCleared) elCleared.textContent = String(s.today_cleared_count != null ? s.today_cleared_count : 0);
                if (elDrawdown) elDrawdown.textContent = String(s.drawdown_guard_alerts != null ? s.drawdown_guard_alerts : 0);
            }
            function pfFmtTodayCell(r) {
                if (r.today_profit == null) return { text: '-', cls: 'c-gray pf-today-profit' };
                var td = Number(r.today_profit || 0);
                var cls = (td >= 0 ? 'c-red' : 'c-green') + ' pf-today-profit';
                return { text: (td >= 0 ? '+' : '') + td.toFixed(2), cls: cls };
            }
            function pfBadge(r) {
                if (r.is_today_cleared) return '<span class="pf-badge pf-badge-cleared">今日清仓</span>';
                if (r.is_today_reopened) return '<span class="pf-badge pf-badge-open">今日重开</span>';
                if (r.is_today_open) return '<span class="pf-badge pf-badge-open">今日开仓</span>';
                if (r.has_today_trade) return '<span class="pf-badge pf-badge-trade">今日交易</span>';
                if (r.is_holding || (r.shares || 0) > 0) return '<span class="pf-badge pf-badge-hold">持仓</span>';
                return '';
            }
            function pfUpdateRow(r) {
                var row = document.getElementById('pf-tr-' + r.stock_code);
                if (!row) return;
                pfRegisterWatchRow(r);
                var pr = Number(r.cur_price || 0);
                var cp = Number(r.cost_price || 0);
                var chg = Number(r.change_pct || 0);
                var profit = Number(r.profit || 0);
                var hasProfitPct = r.profit_pct != null;
                var profitPct = hasProfitPct ? Number(r.profit_pct || 0) : 0;
                var cls = profit >= 0 ? 'c-red' : 'c-green';
                var chgCls = chg >= 0 ? 'c-red' : 'c-green';
                var isHolding = !!(r.is_holding || (r.shares || 0) > 0);
                var tdCell = pfFmtTodayCell(r);
                row.setAttribute('data-holding', isHolding ? '1' : '0');
                row.setAttribute('data-today-status', r.today_position_status || '');
                row.style.background = isHolding ? '#fff4e6' : (r.is_today_cleared ? '#f1f5f9' : '');
                var curEl = row.querySelector('.pf-cur-price');
                var chgEl = row.querySelector('.pf-chg-pct');
                var costEl = row.querySelector('.pf-cost');
                var sharesEl = row.querySelector('.pf-shares');
                var todayEl = row.querySelector('.pf-today-profit');
                var profitEl = row.querySelector('.pf-profit');
                var pctEl = row.querySelector('.pf-profit-pct');
                var flowEl = row.querySelector('.pf-main-flow');
                var snapEl = row.querySelector('.pf-snapshot-at');
                var srcEl = row.querySelector('.pf-quote-source');
                var watchEl = row.querySelector('.pf-watch-advice');
                var badgeEl = row.querySelector('.pf-row-badge');
                if (curEl) curEl.textContent = pr.toFixed(2);
                if (chgEl) { chgEl.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%'; chgEl.className = chgCls + ' pf-chg-pct'; }
                if (costEl) costEl.textContent = fmtPrice(cp);
                if (sharesEl) sharesEl.textContent = r.shares || 0;
                if (todayEl) { todayEl.textContent = tdCell.text; todayEl.className = tdCell.cls; }
                if (badgeEl) badgeEl.innerHTML = pfBadge(r);
                if (profitEl) {
                    profitEl.textContent = (r.shares || 0) > 0 ? (profit >= 0 ? '+' : '') + profit.toFixed(2) : '-';
                    profitEl.className = 'pf-profit ' + ((r.shares || 0) > 0 ? cls : 'c-gray');
                }
                if (pctEl) {
                    pctEl.textContent = (r.shares || 0) > 0 && hasProfitPct ? (profitPct >= 0 ? '+' : '') + profitPct.toFixed(2) + '%' : '-';
                    pctEl.className = 'pf-profit-pct ' + ((r.shares || 0) > 0 && hasProfitPct ? cls : 'c-gray');
                }
                if (flowEl) flowEl.innerHTML = pfFlowCell(r);
                pfSetDataMomentCell(snapEl, r);
                if (srcEl) srcEl.textContent = pfQuoteSourceText(r.quote_source) + ' / ' + pfQuoteStatusText(r);
                if (watchEl) watchEl.innerHTML = pfWatchCell(r);
            }
            window.pfUpdatePortfolioRow = pfUpdateRow;
            window.pfUpdatePortfolioSummary = pfUpdateSummary;
            window.pfSetLiveInterval = function(ms) {
                var next = Number(ms) === 3000 ? 3000 : 5000;
                try { localStorage.setItem('probiga_pf_live_ms', String(next)); } catch (e) {}
                var status = document.getElementById('pfLiveStatus');
                if (status) status.textContent = '国金QMT ' + (next / 1000) + 's';
                if (window._pfAutoRefresh) clearInterval(window._pfAutoRefresh);
                window._pfAutoRefresh = null;
                if (typeof window._pfStartAutoRefresh === 'function') window._pfStartAutoRefresh();
            };
            function renderPortfolio(res) {
                var addForm = '<div style="padding:14px 16px;background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);border-radius:10px;margin-bottom:14px;border:1px solid #2a2a4a">' +
                    '<h4 style="margin:0 0 10px;color:#e0e0e0;font-size:13px">➕ 添加自选股</h4>' +
                    '<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end">' +
                    '<div><label style="font-size:11px;color:#888;display:block">股票代码</label><input id="pfCode" placeholder="000001" style="width:100px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<div><label style="font-size:11px;color:#888;display:block">成本价(元)</label><input id="pfPrice" type="number" step="0.001" placeholder="0" style="width:100px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<div><label style="font-size:11px;color:#888;display:block">股数</label><input id="pfShares" type="number" placeholder="0" style="width:80px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<label title="勾选后会写入今日买入流水，当日盈亏按 现价-买入价 计算；不勾选则按历史持仓录入" style="display:flex;gap:4px;align-items:center;color:#aaa;font-size:12px;margin-bottom:7px"><input id="pfTodayBuy" type="checkbox">今日买入</label>' +
                    '<button onclick="pfAdd()" style="padding:6px 16px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:13px">添加</button>' +
                    '</div></div>' +
                    '<div id="pfMarketBar" style="display:flex;gap:16px;align-items:center;padding:8px 16px;background:#111827;border-radius:8px;margin-bottom:14px;font-size:12px;color:#9ca3af;flex-wrap:wrap">' +
                    '<span>📊 市场概览加载中...</span></div>';

                var sum = res.summary || {};
                var toolbar = '<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:10px">' +
                    '<span id="pfSummaryBar" style="display:flex;gap:14px;align-items:center;flex-wrap:wrap;font-size:13px;color:#ccc">' +
                    '<span>持仓 <span id="pfHoldingCount" style="color:#ff9800;font-weight:700">'+(sum.holding_count||0)+'</span> 只</span>' +
                    '<span>今开 <span id="pfTodayOpenCount" style="color:#1a73e8;font-weight:700">'+(sum.today_open_count||0)+'</span> 只</span>' +
                    '<span>今清 <span id="pfTodayClearedCount" style="color:#64748b;font-weight:700">'+(sum.today_cleared_count||0)+'</span> 只</span>' +
                    '<span title="回撤守门：高/中风险持仓数量">控回撤 <span id="pfDrawdownAlerts" style="color:#dc2626;font-weight:700">'+(sum.drawdown_guard_alerts||0)+'</span> 只</span>' +
                    '<span title="(现价-成本)×股数">持仓盈亏 <span id="pfTotalProfit">'+pfFmtProfit(sum.total_hold_profit)+'</span></span>' +
                    '<span title="昨日持仓×涨跌额 + 今日买入/卖出盈亏">当日盈亏 <span id="pfTodayProfit">'+pfFmtProfit(sum.today_hold_profit)+'</span></span>' +
                    '</span>' +
                    '<span style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">' +
                    '<span style="font-size:14px;font-weight:700;color:#e0e0e0">📈 我的自选股 ('+res.total+'只)</span>' +
                    '<button onclick="savePfOrder()" style="padding:4px 10px;border:none;border-radius:4px;background:#f0c040;color:#1a1a1a;cursor:pointer;font-size:11px;font-weight:600">💾 保存顺序</button>' +
                    '<button onclick="refreshPfPrices()" style="padding:4px 10px;border:none;border-radius:4px;background:#388e3c;color:#fff;cursor:pointer;font-size:11px">📡 同步行情</button>' +
                    '<button onclick="loadPortfolio()" style="padding:4px 12px;border:none;border-radius:4px;background:#333;color:#aaa;cursor:pointer;font-size:12px">🔄 刷新</button>' +
                    '</span></div>';

                if (!res.data || !res.data.length) {
                    c.innerHTML = addForm + toolbar + '<div class="loading">暂无自选股</div>';
                    return;
                }

                toolbar = toolbar.replace(
                    '</span></div>',
                    '<button onclick="pfSetLiveInterval(5000)" style="padding:4px 8px;border:none;border-radius:4px;background:#1f2937;color:#d1d5db;cursor:pointer;font-size:11px">5s</button>' +
                    '<button onclick="pfSetLiveInterval(3000)" style="padding:4px 8px;border:none;border-radius:4px;background:#7c2d12;color:#fff;cursor:pointer;font-size:11px">3s</button>' +
                    '<span id="pfLiveStatus" style="font-size:11px;color:#9ca3af">国金QMT ' + (pfLiveIntervalMs()/1000) + 's</span></span></div>'
                );
                var html = toolbar;

                // Build table with drag handles
                html += '<div class="table-wrap pf-table-wrap"><table id="pfTable" class="pf-table"><thead><tr>' +
                    '<th style="width:28px"></th>' +
                    '<th>代码</th><th>名称</th><th>现价</th><th title="个股行情涨跌，非您的持仓盈亏">涨跌%</th><th>成本</th><th>持有</th>' +
                    '<th title="昨日持仓×涨跌额 + 今日买入/卖出盈亏">当日盈亏</th><th title="(现价-成本)×股数">持仓盈亏</th><th title="相对成本">收益率</th>' +
                    '<th title="分钟资金优先；没有分钟资金时展示最近日级主力净流入">资金态度/净流</th><th class="pf-watch-advice-col">盯盘建议</th><th>数据时刻</th><th>来源</th><th class="pf-sticky pf-sticky-action">操作</th><th class="pf-sticky pf-sticky-analysis">分析</th><th class="pf-sticky pf-sticky-history">历史</th>' +
                    '</tr></thead><tbody>';
                res.data.forEach(function(r, idx){
                    pfRegisterWatchRow(r);
                    var pr = Number(r.cur_price||0);
                    var cp = Number(r.cost_price||0);
                    var chg = Number(r.change_pct||0);
                    var profit = Number(r.profit||0);
                    var hasProfitPct = r.profit_pct != null;
                    var profitPct = hasProfitPct ? Number(r.profit_pct||0) : 0;
                    var chgCls = chg>=0?'c-red':'c-green';
                    var pfCls = profit>=0?'c-red':'c-green';
                    var isHolding = !!(r.is_holding || (r.shares||0) > 0);
                    var rowBg = isHolding ? 'background:#fff4e6;' : (r.is_today_cleared ? 'background:#f1f5f9;' : '');
                    var tdCell = pfFmtTodayCell(r);
                    var profitTxt = isHolding ? (profit>=0?'+':'')+profit.toFixed(2) : '-';
                    var pctTxt = isHolding && hasProfitPct ? (profitPct>=0?'+':'')+profitPct.toFixed(2)+'%' : '-';
                    var pfClsRow = isHolding ? pfCls : 'c-gray';
                    var dataMoment = pfDataMoment(r);
                    html += '<tr id=\"pf-tr-'+r.stock_code+'\" draggable=\"true\" data-code=\"'+r.stock_code+'\" data-holding=\"'+(isHolding?'1':'0')+'\" data-today-status=\"'+(r.today_position_status||'')+'\" style=\"cursor:grab;'+rowBg+'\">' +
                        '<td style=\"text-align:center;color:#555;font-size:14px;cursor:grab\" class=\"pf-drag-handle\">⠿</td>' +
                        '<td>'+nameLink(r.stock_code, r.stock_code)+'</td>' +
                        '<td><strong>'+nameLink(r.stock_code, r.display_name)+'</strong><span class=\"pf-row-badge\">'+pfBadge(r)+'</span></td>' +
                        '<td class=\"pf-cur-price\">'+fmtPrice(pr)+'</td>' +
                        '<td class=\"'+chgCls+' pf-chg-pct\">'+(chg>=0?'+':'')+chg.toFixed(2)+'%</td>' +
                        '<td class=\"pf-cost\">'+fmtPrice(cp)+'</td>' +
                        '<td class=\"pf-shares\">'+(r.shares||0)+'</td>' +
                        '<td class=\"'+tdCell.cls+'\">'+tdCell.text+'</td>' +
                        '<td class=\"pf-profit '+pfClsRow+'\">'+profitTxt+'</td>' +
                        '<td class=\"pf-profit-pct '+pfClsRow+'\">'+pctTxt+'</td>' +
                        '<td class=\"pf-main-flow\">'+pfFlowCell(r)+'</td>' +
                        '<td class=\"pf-watch-advice\">'+pfWatchCell(r)+'</td>' +
                        '<td class=\"pf-snapshot-at\" title=\"'+escAttr(dataMoment.title)+'\">'+dataMoment.html+'</td>' +
                        '<td class=\"pf-quote-source\" style=\"font-size:11px;color:#666\">'+pfQuoteSourceText(r.quote_source)+' / '+pfQuoteStatusText(r)+'</td>' +
                        '<td class=\"pf-actions pf-sticky pf-sticky-action\"><button onclick=\"event.stopPropagation();pfTransact(\''+r.stock_code+'\',\''+r.display_name+'\','+cp+','+(r.shares||0)+')\" style=\"padding:2px 8px;border:none;border-radius:4px;background:#388e3c;color:#fff;cursor:pointer;font-size:11px\">💰</button>' +
                        '<button onclick=\"event.stopPropagation();pfRemove(\''+r.stock_code+'\')\" style=\"padding:2px 8px;border:none;border-radius:4px;background:#c62828;color:#fff;cursor:pointer;font-size:11px;margin-left:2px\">✕</button></td>' +
                        '<td class=\"pf-sticky pf-sticky-analysis\"><button onclick=\"event.stopPropagation();pfAnalyze(\''+r.stock_code+'\',\''+r.display_name+'\')\" style=\"padding:4px 12px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer;font-size:11px\">🤖 分析</button></td>' +
                        '<td class=\"pf-sticky pf-sticky-history\"><button onclick=\"event.stopPropagation();pfHistory(\''+r.stock_code+'\',\''+r.display_name+'\')\" style=\"padding:4px 10px;border:none;border-radius:4px;background:#555;color:#ccc;cursor:pointer;font-size:11px\">📋</button></td>' +
                        '</tr>';
                });
                html += '</tbody></table></div>';
                c.innerHTML = addForm + html;
                pfUpdateLiveStatus(res, '已加载');

                // Drag and drop
                var tb = document.getElementById('pfTable');
                if (tb) {
                    var dragRow = null;
                    tb.addEventListener('dragstart', function(e) {
                        dragRow = e.target.closest('tr[draggable]');
                        if (!dragRow) return;
                        dragRow.style.opacity = '0.4';
                        e.dataTransfer.effectAllowed = 'move';
                    });
                    tb.addEventListener('dragend', function(e) {
                        if (dragRow) dragRow.style.opacity = '1';
                        dragRow = null;
                        [].forEach.call(tb.querySelectorAll('tr'), function(tr){ tr.style.borderTop = ''; });
                    });
                    tb.addEventListener('dragover', function(e) {
                        e.preventDefault();
                        e.dataTransfer.dropEffect = 'move';
                        var tr = e.target.closest('tr[draggable]');
                        if (tr && tr !== dragRow) {
                            var rect = tr.getBoundingClientRect();
                            if (e.clientY - rect.top < rect.height/2) {
                                tr.style.borderTop = '2px solid #f0c040';
                                tr.style.borderBottom = '';
                            } else {
                                tr.style.borderBottom = '2px solid #f0c040';
                                tr.style.borderTop = '';
                            }
                        }
                    });
                    tb.addEventListener('dragleave', function(e) {
                        var tr = e.target.closest('tr[draggable]');
                        if (tr) { tr.style.borderTop = ''; tr.style.borderBottom = ''; }
                    });
                    tb.addEventListener('drop', function(e) {
                        e.preventDefault();
                        var tr = e.target.closest('tr[draggable]');
                        if (!tr || !dragRow || tr === dragRow) return;
                        tr.style.borderTop = ''; tr.style.borderBottom = '';
                        var rect = tr.getBoundingClientRect();
                        var tbody = tr.parentNode;
                        if (e.clientY - rect.top < rect.height/2) {
                            tbody.insertBefore(dragRow, tr);
                        } else {
                            tbody.insertBefore(dragRow, tr.nextSibling);
                        }
                    });
                }
                // 加载市场概览条
                window._pfRefreshMarketBar = function() {
                    fetch('/api/monitor/data').then(function(r){return r.json()}).then(function(md) {
                        var bar = document.getElementById('pfMarketBar');
                        if (!bar || md.error) return;
                        var upc = md.up_count || 0, dnc = md.down_count || 0, flt = md.sideline_count || 0;
                        var total = upc + dnc + flt || 1;
                        var upPct = (upc / total * 100).toFixed(0);
                        var amt = md.total_amount ? (md.total_amount / 1e8).toFixed(0) + '亿' : '-';
                        var heat = md.market_heat || 0;
                        var heatColor = heat > 600 ? '#ef4444' : heat < 400 ? '#22c55e' : '#f59e0b';
                        var chg = md.heat_change || 0;
                        var chgIcon = chg >= 0 ? '▲' : '▼';
                        var topInd = (md.top_industries || []).slice(0, 3).map(function(t){ return t.name; }).join(' · ') || '-';
                        bar.innerHTML =
                            '<span style="font-weight:600;color:#e5e7eb">📊 市场</span>' +
                            '<span>上涨 <b style="color:#ef4444">' + upc + '</b></span>' +
                            '<span>持平 <b style="color:#9ca3af">' + flt + '</b></span>' +
                            '<span>下跌 <b style="color:#22c55e">' + dnc + '</b></span>' +
                            '<span>涨比 <b style="color:#f59e0b">' + upPct + '%</b></span>' +
                            '<span>成交 <b style="color:#60a5fa">' + amt + '</b></span>' +
                            '<span>热度 <b style="color:' + heatColor + '">' + heat.toFixed(0) + '</b> <span style="font-size:11px;color:' + (chg>=0?'#ef4444':'#22c55e') + '">' + chgIcon + Math.abs(chg).toFixed(1) + '%</span></span>' +
                            '<span>热门 ' + topInd + '</span>';
                    }).catch(function(){});
                };
                window._pfRefreshMarketBar();
            }
            fetchRawJsonWithTimeout(pfLiveUrl(false), 12000).then(renderPortfolio).catch(function(e) {
                c.innerHTML = '<div class="loading" style="color:#e74c3c">自选股加载失败: ' + escHtml(e.message || '网络异常') + '</div>';
            });

            // Auto-refresh every 3s by default, or 1s in fast mode, during trading hours.
            if (window._pfAutoRefresh) clearInterval(window._pfAutoRefresh);
            window._pfAutoRefreshDone = false;
            window._pfStartAutoRefresh = function() {
            window._pfAutoRefresh = setInterval(function() {
                if (pfIsActiveTab()) {
                    window._pfAutoRefreshDone = false;
                    pfFetchAndApplyLive(isTradingTime() ? '' : '已同步');
                    if (window._pfRefreshMarketBar) window._pfRefreshMarketBar();
                }
            }, pfLiveIntervalMs());
            };
            window._pfStartAutoRefresh();
        },
        datasource: function (d, c) {
            Promise.all([
                fetch('/api/datasource/stats').then(function(r) { return r.json(); }),
                fetch('/api/datasource/list').then(function(r) { return r.json(); })
            ]).then(function(results) {
                var stats = results[0];
                var listRes = results[1];
                var providers = listRes.data || [];

                var html = '';

                // 顶部统计条
                html += '<div style="display:flex;gap:24px;margin-bottom:16px;padding:14px 20px;background:#fff;border-radius:10px;box-shadow:0 1px 6px rgba(0,0,0,.05);align-items:center;flex-wrap:wrap">';
                html += '<div style="font-size:15px;font-weight:700;color:#333">📊 数据源总览</div>';
                html += '<div style="display:flex;gap:16px;margin-left:auto;font-size:13px">';
                html += '<span style="color:#666">共 <b style="color:#333">' + stats.total + '</b> 个</span>';
                html += '<span style="color:#27ae60">✅ ' + stats.success + '</span>';
                html += '<span style="color:#e74c3c">❌ ' + stats.failed + '</span>';
                html += '<span style="color:#2980b9">⏳ ' + stats.running + '</span>';
                html += '<span style="color:#999">⏸ ' + stats.pending + '</span>';
                html += '</div>';
                html += '</div>';

                var requiredHealth = stats.required_health || [];
                if (requiredHealth.length) {
                    var badCount = requiredHealth.filter(function(item) { return item.status !== 'ok' && item.status !== 'running'; }).length;
                    var dsHealthBadge = badCount ? '<span style="color:#e74c3c;font-weight:700">异常 ' + badCount + '</span>' : '<span style="color:#27ae60;font-weight:700">正常</span>';
                    html += '<div style="background:#fff;border-radius:10px;box-shadow:0 1px 6px rgba(0,0,0,.05);margin-bottom:16px;overflow:hidden">';
                    html += '<div style="display:flex;align-items:center;gap:12px;padding:12px 16px;border-bottom:1px solid #eee">';
                    html += '<div style="font-size:14px;font-weight:700;color:#333">关键数据任务健康</div>';
                    html += '<div style="font-size:12px;color:#888">新浪热股 / 个股资金流向 / 概念资金流向</div>';
                    html += '<div style="margin-left:auto;font-size:12px">' + dsHealthBadge + '</div>';
                    html += '</div>';
                    html += '<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:12px"><thead><tr style="background:#f8f9fa">';
                    html += '<th style="padding:9px 12px;text-align:left">任务</th><th style="padding:9px 12px;text-align:center">状态</th><th style="padding:9px 12px;text-align:center">最近执行</th><th style="padding:9px 12px;text-align:center">最新数据</th><th style="padding:9px 12px;text-align:center">最新条数</th><th style="padding:9px 12px;text-align:center">操作</th>';
                    html += '</tr></thead><tbody>';
                    requiredHealth.forEach(function(item, idx) {
                        var color = item.status === 'ok' ? '#27ae60' : item.status === 'running' ? '#2980b9' : '#e74c3c';
                        var bg = idx % 2 === 0 ? '#fff' : '#fafafa';
                        var label = item.label || item.task_type || '-';
                        var lastRun = item.last_run_at ? String(item.last_run_at).replace('T', ' ').slice(0, 16) : '-';
                        var maxData = item.max_data_time ? String(item.max_data_time).replace('T', ' ').slice(0, 19) : '-';
                        var action = item.task_id ? '<button onclick="dsRunTask(' + item.task_id + ')" style="padding:3px 8px;font-size:11px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer;margin-right:4px">执行</button><button onclick="dsViewLog(' + item.task_id + ')" style="padding:3px 8px;font-size:11px;border:none;border-radius:4px;background:#666;color:#fff;cursor:pointer">日志</button>' : '<span style="color:#aaa">未配置</span>';
                        html += '<tr style="background:' + bg + ';border-bottom:1px solid #f0f0f0">';
                        html += '<td style="padding:9px 12px"><strong>' + label + '</strong><div style="color:#999;font-size:11px">' + (item.table || '-') + '</div></td>';
                        html += '<td style="padding:9px 12px;text-align:center;color:' + color + ';font-weight:700">' + (item.message || item.status || '-') + '</td>';
                        html += '<td style="padding:9px 12px;text-align:center;color:#666">' + lastRun + '</td>';
                        html += '<td style="padding:9px 12px;text-align:center;color:#666">' + maxData + '</td>';
                        html += '<td style="padding:9px 12px;text-align:center;color:#666">' + (item.row_count_latest == null ? '-' : item.row_count_latest) + '</td>';
                        html += '<td style="padding:9px 12px;text-align:center;white-space:nowrap">' + action + '</td>';
                        html += '</tr>';
                    });
                    html += '</tbody></table></div></div>';
                }

                // Tab 栏
                html += '<div class="ds-tabs" style="display:flex;gap:4px;margin-bottom:16px;flex-wrap:wrap;border-bottom:2px solid #e8e8e8;padding-bottom:0">';
                providers.forEach(function(provider, idx) {
                    var active = idx === 0 ? 'border-bottom:2px solid #1a73e8;color:#1a73e8;background:#fff;font-weight:600;' : 'border-bottom:2px solid transparent;color:#666;background:#f5f5f5;';
                    var total = 0;
                    Object.keys(provider.types).forEach(function(t) { total += provider.types[t].length; });
                    html += '<div class="ds-tab" data-provider="' + idx + '" onclick="dsSwitchTab(' + idx + ')" style="padding:10px 18px;cursor:pointer;border-radius:8px 8px 0 0;font-size:13px;transition:all .2s;' + active + '">';
                    html += provider.icon + ' ' + provider.provider + ' <span style="font-size:11px;opacity:0.7">(' + total + ')</span>';
                    html += '</div>';
                });
                html += '</div>';

                // Tab 内容区
                html += '<div id="dsTabContent">';
                providers.forEach(function(provider, pIdx) {
                    var display = pIdx === 0 ? 'block' : 'none';
                    html += '<div class="ds-tab-panel" data-provider="' + pIdx + '" style="display:' + display + '">';

                    // 任务表格
                    html += '<div style="background:#fff;border-radius:0 0 10px 10px;box-shadow:0 1px 6px rgba(0,0,0,.05);overflow:hidden">';
                    html += '<table style="width:100%;border-collapse:collapse;font-size:13px">';
                    html += '<thead><tr style="background:#f8f9fa">';
                    html += '<th style="padding:11px 16px;text-align:left;font-weight:600;color:#555;border-bottom:1px solid #eee">任务名称</th>';
                    html += '<th style="padding:11px 12px;text-align:left;font-weight:600;color:#555;border-bottom:1px solid #eee">业务类型</th>';
                    html += '<th style="padding:11px 12px;text-align:center;font-weight:600;color:#555;border-bottom:1px solid #eee">状态</th>';
                    html += '<th style="padding:11px 12px;text-align:center;font-weight:600;color:#555;border-bottom:1px solid #eee">上次执行</th>';
                    html += '<th style="padding:11px 12px;text-align:center;font-weight:600;color:#555;border-bottom:1px solid #eee">耗时</th>';
                    html += '<th style="padding:11px 16px;text-align:center;font-weight:600;color:#555;border-bottom:1px solid #eee">操作</th>';
                    html += '</tr></thead><tbody>';

                    Object.keys(provider.types).forEach(function(bizType) {
                        var tasks = provider.types[bizType];
                        if (!tasks || !tasks.length) return;

                        tasks.forEach(function(task, idx) {
                            var statusHtml = '';
                            if (task.last_run_status === 'success') statusHtml = '<span style="color:#27ae60;font-weight:600">✅ 成功</span>';
                            else if (task.last_run_status === 'failed') statusHtml = '<span style="color:#e74c3c;font-weight:600">❌ 失败</span>';
                            else if (task.last_run_status === 'running') statusHtml = '<span style="color:#2980b9;font-weight:600">⏳ 运行中</span>';
                            else statusHtml = '<span style="color:#999">⏸ 待运行</span>';

                            var lastRun = task.last_run_at ? task.last_run_at.replace('T', ' ').slice(0, 16) : '-';
                            var duration = task.last_run_duration ? task.last_run_duration + 's' : '-';
                            var rowStyle = task.enabled === 1 ? '' : 'opacity:0.5;';
                            var rowBg = idx % 2 === 0 ? '#fff' : '#fafafa';

                            html += '<tr style="background:' + rowBg + ';' + rowStyle + 'border-bottom:1px solid #f0f0f0">';
                            html += '<td style="padding:10px 16px"><span style="font-weight:500;color:#333">' + task.task_name + '</span></td>';
                            html += '<td style="padding:10px 12px;color:#888">' + bizType + '</td>';
                            html += '<td style="padding:10px 12px;text-align:center">' + statusHtml + '</td>';
                            html += '<td style="padding:10px 12px;text-align:center;color:#666;font-size:12px">' + lastRun + '</td>';
                            html += '<td style="padding:10px 12px;text-align:center;color:#666">' + duration + '</td>';
                            html += '<td style="padding:10px 16px;text-align:center;white-space:nowrap">';
                            html += '<button onclick="dsRunTask(' + task.id + ')" style="padding:4px 10px;font-size:11px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer;margin-right:4px">执行</button>';
                            html += '<button onclick="dsToggleTask(' + task.id + ')" style="padding:4px 10px;font-size:11px;border:none;border-radius:4px;background:' + (task.enabled === 1 ? '#f39c12' : '#27ae60') + ';color:#fff;cursor:pointer;margin-right:4px">' + (task.enabled === 1 ? '停用' : '启用') + '</button>';
                            html += '<button onclick="dsViewLog(' + task.id + ')" style="padding:4px 10px;font-size:11px;border:none;border-radius:4px;background:#666;color:#fff;cursor:pointer">日志</button>';
                            html += '</td></tr>';
                        });
                    });

                    html += '</tbody></table>';
                    html += '</div>';
                    html += '</div>';
                });
                html += '</div>';

                c.innerHTML = html;
            }).catch(function(err) {
                c.innerHTML = '<div class="loading" style="color:#e74c3c">加载失败: ' + err.message + '</div>';
            });
        },
        scheduler: function (d, c) {
            fetch('/api/scheduler/tasks').then(function (r) { return r.json(); }).then(function (res) {
                function runtimePanel(runtime) {
                    runtime = runtime || {};
                    var heartbeat = runtime.heartbeat || {};
                    var safe = runtime.api_restart_safe;
                    var online = runtime.standalone_scheduler_online;
                    var embedded = runtime.embedded_scheduler_enabled;
                    var color = safe ? '#16a34a' : (embedded ? '#dc2626' : '#f59e0b');
                    var bg = safe ? '#ecfdf5' : (embedded ? '#fef2f2' : '#fffbeb');
                    var mode = safe ? '独立调度在线' : (embedded ? '内嵌调度' : '独立调度未检测到心跳');
                    var heartbeatText = heartbeat.heartbeat_at ? heartbeat.heartbeat_at + '（' + (heartbeat.heartbeat_age_seconds || 0) + '秒前）' : '-';
                    var hostText = heartbeat.host_name ? heartbeat.host_name + ' / PID ' + (heartbeat.pid || '-') : '-';
                    var restartText = safe ? '可以重启API，不影响定时任务' : (embedded ? '重启API会中断定时任务' : '先启动独立调度进程');
                    var h = '<div style="background:' + bg + ';border:1px solid ' + color + ';border-radius:12px;padding:14px 16px;margin-bottom:16px;">';
                    h += '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px;">';
                    h += '<span style="background:' + color + ';color:#fff;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:900;">' + mode + '</span>';
                    h += '<span style="font-size:14px;font-weight:900;color:#111827;">' + restartText + '</span>';
                    h += '<span style="font-size:12px;color:#64748b;">' + (runtime.status_text || '') + '</span>';
                    h += '</div>';
                    h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;font-size:12px;color:#475569;">';
                    h += '<div><strong style="color:#111827">独立心跳：</strong>' + heartbeatText + '</div>';
                    h += '<div><strong style="color:#111827">实例：</strong>' + hostText + '</div>';
                    h += '<div><strong style="color:#111827">轮询：</strong>' + (runtime.scheduler_poll_seconds || '-') + '秒 / 并发 ' + (runtime.scheduler_max_concurrent_tasks || '-') + '</div>';
                    h += '<div><strong style="color:#111827">启动命令：</strong><code>' + (runtime.scheduler_daemon_command || 'python tools/run_scheduler_daemon.py') + '</code></div>';
                    h += '</div></div>';
                    return h;
                }
                var runtimeHtml = runtimePanel(res.runtime || {});
                if (!res.data || !res.data.length) { c.innerHTML = runtimeHtml + '<div class="loading">暂无任务</div>'; return; }
                var GROUP_ORDER = ['复盘数据', '概念行业', '资金流向', '龙虎榜', '系统管理', '其他'];
                var GROUP_ICONS = {'复盘数据':'📊','概念行业':'🏷️','资金流向':'💰','龙虎榜':'🐲','系统管理':'⚙️','其他':'📌'};
                var GROUP_IDS = {'复盘数据':'review','概念行业':'concept','资金流向':'capital','龙虎榜':'lhb','系统管理':'sys','其他':'other'};
                var groups = {};
                res.data.forEach(function (t) {
                    var g = t.group_name || '其他';
                    if (!groups[g]) groups[g] = [];
                    groups[g].push(t);
                });
                c.innerHTML = runtimeHtml;
                var cols = ['任务名称', '脚本', '执行时间', '日期参数', '状态', '上次执行', '下次执行', '耗时', '操作'];
                var gIdx = 0;
                GROUP_ORDER.forEach(function (gName) {
                    var tasks = groups[gName];
                    if (!tasks || !tasks.length) return;
                    var section = document.createElement('div');
                    section.style.cssText = 'margin-bottom:24px';
                    c.appendChild(section);
                    var tableId = 'sch_' + (GROUP_IDS[gName] || ('g' + gIdx));
                    var titleHtml = '<div class="section-title">' + (GROUP_ICONS[gName] || '') + ' ' + gName + '（' + tasks.length + '）</div>';
                    gIdx++;
                    window.renderTable(section, tableId, cols, tasks, function (t) {
                        var sl = ''; if (t.last_run_status === 'success') sl = '<span style="color:#27ae60;font-weight:600">✅ 成功</span>'; else if (t.last_run_status === 'failed') sl = '<span style="color:#e74c3c;font-weight:600">❌ 失败</span>'; else if (t.last_run_status === 'running') sl = '<span style="color:#2980b9;">⏳ 运行中</span>'; else sl = '<span style="color:#999">⏸ 待运行</span>';
                        var on = t.enabled === 1 ? '🟢' : '🔴', tx = t.enabled === 1 ? '停用' : '启用';
                        var la = t.last_run_at ? t.last_run_at.replace('T', ' ').slice(0, 16) : '-';
                        var next = t.next_run_at ? t.next_run_at.slice(0, 16) : '-';
                        var nc = t.enabled === 1 ? '#f0c040' : '#666';
                        return '<tr><td><strong>' + t.task_name + '</strong></td><td style="font-size:11px;color:#888">' + (t.script_path || '-') + '</td><td><input type="time" value="' + t.cron_time + '" style="width:90px;padding:3px 6px;border:1px solid #ddd;border-radius:4px;font-size:12px" onchange="updCron(' + t.id + ',this.value)"></td><td><input type="text" value="' + (t.date_param || '') + '" placeholder="空=当天" style="width:170px;padding:3px 6px;border:1px solid #ddd;border-radius:4px;font-size:11px" onchange="updDp(' + t.id + ',this.value)"></td><td>' + sl + '</td><td style="font-size:11px">' + la + '</td><td style="font-size:11px;color:' + nc + ';font-weight:600">' + next + '</td><td>' + (t.last_run_duration ? t.last_run_duration + 's' : '-') + '</td><td style="white-space:nowrap"><button onclick="runT(' + t.id + ')" style="padding:2px 8px;font-size:11px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer">▶</button> <button onclick="togT(' + t.id + ')" style="padding:2px 8px;font-size:11px;border:none;border-radius:4px;background:#f39c12;color:#fff;cursor:pointer">' + on + ' ' + tx + '</button> <button onclick="logT(' + t.id + ')" style="padding:2px 8px;font-size:11px;border:none;border-radius:4px;background:#666;color:#fff;cursor:pointer">📋</button></td></tr>';
                    }, 30, titleHtml);
                });
            });
        },
        'stock-list': function (d, c) {
            loadStockListPage(d, c);
        },

        /* ── 老版 tab（供布局切换使用） ── */
        'intraday-battle': function (d, c) { loadIntradayBattlePage(d, c); },
        sentiment: function (d, c) { loadSentimentPage(d, c); },
        multi3: function (d, c) { loadMulti(d, 3, c); },
        multi5: function (d, c) { loadMulti(d, 5, c); },
        ths: function (d, c) { loadThsTab(d, c); },
        east: function (d, c) { loadEastTab(d, c); },
        xq: function (d, c) { loadXqTab(d, c); },
        sina: function (d, c) { loadSinaTab(d, c); },
        concept3: function (d, c) { loadConceptMulti(d, 3, 1, c); },
        concept5: function (d, c) { loadConceptMulti(d, 5, 1, c); },
        industry3: function (d, c) { loadConceptMulti(d, 3, 2, c); },
        industry5: function (d, c) { loadConceptMulti(d, 5, 2, c); },
        'capital-rt': function (d, c) {
            c.innerHTML = '<div class="search-bar"><input type="text" id="rtCode" placeholder="输入股票代码" style="width:130px"><button onclick="loadRT()">查询</button><span id="rtInfo"></span></div><div id="rtResult"></div>';
        },
        'sector-heat': function (d, c) {
            loadSectorHeatPage(d, c);
        },
        'sector-movement': function (d, c) { loadSectorMovementPage(c); },
        'sector-rotation': function (d, c) { loadSectorRotationPage(d, c); },
        'review-print': function (d, c) {
            fetch('/api/hot-data/daily-review/print?review_date=' + d).then(function (r) { return r.text(); }).then(function (html) {
                if (!html || html.length < 50) { c.innerHTML = '<div class="loading">暂无复盘数据，点击右上角生成</div>'; return; }
                c.innerHTML = '<div style="background:#fff;border-radius:10px;padding:20px;box-shadow:0 1px 6px rgba(0,0,0,.05)">' + html + '</div>';
            }).catch(function () { c.innerHTML = '<div class="loading">加载失败</div>'; });
        },
        /* ── 模拟交易 ── */
        'sim-trade': function (d, c) {
            loadSimTradePage(c, 'live');
        },
        'strategy-backtest': function (d, c) {
            loadStrategyBacktestPage(c);
        }
    };

    /* ===== 布局切换 ===== */
    var LAYOUT_OLD = [
        {group:'市场分析', items:[
            {id:'command',icon:'🧭',label:'智能决策'},
            {id:'intraday-battle',icon:'⚡',label:'盘中作战'},
            {id:'monitor',icon:'📺',label:'市场监控中心'},
            {id:'sector-movement',icon:'🌊',label:'板块异动'},
            {id:'fused',icon:'📊',label:'融合榜单 TOP100'},
            {id:'sentiment',icon:'🧠',label:'市场情绪与风格'},
            {id:'sector-rotation',icon:'🔄',label:'板块轮动分析'},
            {id:'stock-list',icon:'📋',label:'全市场股票'}
        ]},
        {group:'复盘数据', items:[
            {id:'multi3',icon:'🔥',label:'近3天强势股'},
            {id:'multi5',icon:'🔥',label:'近5天强势股'},
            {id:'ths',icon:'🏆',label:'同花顺热股'},
            {id:'east',icon:'✨',label:'东财人气榜'},
            {id:'xq',icon:'❄️',label:'雪球热股'},
            {id:'sina',icon:'🌐',label:'新浪热股'},
            {id:'screen',icon:'🎯',label:'选股策略'},
            {id:'jq-picks',icon:'🤖',label:'聚宽策略选股'},
            {id:'review',icon:'📋',label:'复盘数据'},
            {id:'sector-heat',icon:'🌡',label:'板块热度'},
            {id:'hunter',icon:'🏹',label:'狩猎场'},
            {id:'portfolio',icon:'📈',label:'自选股'},
            {id:'recommended',icon:'💎',label:'AI推荐买入'},
            {id:'sim-trade',icon:'🤖',label:'模拟交易'}
        ]},
        {group:'概念 / 行业', items:[
            {id:'concept',icon:'🏷️',label:'热门概念 (当日)'},
            {id:'concept3',icon:'📋',label:'近3天热门概念'},
            {id:'concept5',icon:'📋',label:'近5天热门概念'},
            {id:'industry3',icon:'🏭',label:'近3天热门行业'},
            {id:'industry5',icon:'🏭',label:'近5天热门行业'}
        ]},
        {group:'资金流向', items:[
            {id:'capital',icon:'💰',label:'个股资金净流入'},
            {id:'capital-rt',icon:'⏱',label:'实时资金'},
            {id:'mainforce',icon:'🔍',label:'主力行为分析'}
        ]},
        {group:'新闻公告', items:[
            {id:'news',icon:'📰',label:'财联社快讯'},
            {id:'research-radar',icon:'🧭',label:'研报雷达'},
            {id:'notice',icon:'📜',label:'个股公告'}
        ]},
        {group:'龙虎榜', items:[
            {id:'alist',icon:'🐲',label:'龙虎榜列表'}
        ]},
        {group:'系统管理', items:[
            {id:'datasource',icon:'🔌',label:'数据源管理'},
            {id:'scheduler',icon:'⚙️',label:'调度管理'},
            {id:'commentary',icon:'🧠',label:'股评监控'}
        ]}
    ];
    var LAYOUT_NEW = [
        {group:'市场概览', items:[
            {id:'command',icon:'🧭',label:'智能决策'},
            {id:'intraday-battle',icon:'⚡',label:'盘中作战'},
            {id:'monitor',icon:'📺',label:'市场监控'},
            {id:'review',icon:'📋',label:'每日复盘'},
            {id:'fused',icon:'📊',label:'热股排行'},
            {id:'sector',icon:'🌊',label:'板块分析'}
        ]},
        {group:'个股热度', items:[
            {id:'strong',icon:'🔥',label:'强势股'},
            {id:'concept',icon:'🏷️',label:'概念 / 行业'},
            {id:'alist',icon:'🐲',label:'龙虎榜'}
        ]},
        {group:'资金流向', items:[
            {id:'capital',icon:'💰',label:'个股资金'},
            {id:'mainforce',icon:'🔍',label:'主力行为'}
        ]},
        {group:'选股工具', items:[
            {id:'screen',icon:'🎯',label:'选股策略'},
            {id:'jq-picks',icon:'🤖',label:'聚宽策略'},
            {id:'recommended',icon:'💎',label:'AI推荐'},
            {id:'hunter',icon:'🏹',label:'狩猎场'}
        ]},
        {group:'持仓管理', items:[
            {id:'portfolio',icon:'📈',label:'自选股'},
            {id:'sim-trade',icon:'🤖',label:'模拟交易'}
        ]},
        {group:'资讯公告', items:[
            {id:'news',icon:'📰',label:'快讯'},
            {id:'research-radar',icon:'🧭',label:'研报雷达'},
            {id:'notice',icon:'📜',label:'个股公告'}
        ]},
        {group:'系统', items:[
            {id:'datasource',icon:'🔌',label:'数据源管理'},
            {id:'scheduler',icon:'⚙️',label:'调度管理'},
            {id:'commentary',icon:'🧠',label:'股评监控'},
            {id:'stock-list',icon:'📋',label:'全市场股票'}
        ]}
    ];
    function ensureLayoutItem(layout, groupIndex, item) {
        if (!layout[groupIndex] || !layout[groupIndex].items) return;
        var exists = layout[groupIndex].items.some(function(it){ return it.id === item.id; });
        if (!exists) layout[groupIndex].items.push(item);
    }
    if (typeof PAGE_TITLES !== 'undefined') {
        PAGE_TITLES['intraday-battle'] = '⚡ 盘中作战';
        PAGE_TITLES['strategy-backtest'] = '📊 策略回测';
        PAGE_TITLES['research-radar'] = '🧭 研报趋势雷达';
        PAGE_TITLES['hunter'] = '🏹 狩猎场';
    }
    ensureLayoutItem(LAYOUT_OLD, 1, {id:'strategy-backtest', icon:'📊', label:'策略回测'});
    ensureLayoutItem(LAYOUT_NEW, 4, {id:'strategy-backtest', icon:'📊', label:'策略回测'});
    var ALL_OLD_IDS = []; LAYOUT_OLD.forEach(function(g){ g.items.forEach(function(it){ ALL_OLD_IDS.push(it.id); }); });
    var ALL_NEW_IDS = []; LAYOUT_NEW.forEach(function(g){ g.items.forEach(function(it){ ALL_NEW_IDS.push(it.id); }); });

    function renderSidebar(layout, activeId) {
        var sb = el('sidebar');
        var h = '<div class="sidebar-logo">Pro<span>Big</span>A</div>';
        layout.forEach(function (g) {
            h += '<div class="sidebar-group"><div class="sidebar-group-title">' + g.group + '</div>';
            g.items.forEach(function (it) {
                var cls = it.id === activeId ? 'sidebar-item active' : 'sidebar-item';
                h += '<button class="' + cls + '" data-tab="' + it.id + '" onclick="switchTab(\'' + it.id + '\')">' + it.icon + ' ' + it.label + '</button>';
            });
            h += '</div>';
        });
        sb.innerHTML = h;
    }

    window.toggleLayout = function () {
        var current = localStorage.getItem('probiga_layout') || 'new';
        var next = current === 'new' ? 'old' : 'new';
        localStorage.setItem('probiga_layout', next);
        applyLayout(next);
    };

    function applyLayout(mode) {
        var btn = el('btnLayoutToggle');
        if (btn) btn.textContent = mode === 'new' ? '🔀 新版' : '🔀 老版';
        var layout = mode === 'new' ? LAYOUT_NEW : LAYOUT_OLD;
        var firstId = layout[0].items[0].id;
        renderSidebar(layout, firstId);
        // 隐藏所有 tab-content
        document.querySelectorAll('.tab-content').forEach(function (tc) { tc.classList.remove('active'); });
        // 显示第一个
        var first = el('tab-' + firstId);
        if (first) first.classList.add('active');
        el('pageTitle').textContent = (PAGE_TITLES[firstId] || firstId);
        loadTab(firstId);
    }

    /* ===== 特殊加载 ===== */
    function loadMulti(d, days, c) {
        apiGet('/multi-day?stat_date=' + d + '&days=' + days + '&top=100').then(function (res) {
            syncDateFromResponse(res);
            if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
            var h = '<div class="stats-bar">' + card('区间', '近' + days + '天', 'blue') + card('上榜', res.total) + '</div>';
            window.renderTable(c, 'multi' + days, ['排名', '代码', '名称', '行业', '人气标签', '概念板块', '出现天数', '频率', '均涨跌幅', '均东财', '均同花', '均雪球', '均新浪', '最新东', '最新同', '最新雪', '最新新浪', '分时'], res.data, function (r) {
                var pt = r.pop_tag || '-';
                var ct = r.concept_tag || '-';
                if (ct && ct !== '-') {
                    var parts = ct.split(';').filter(Boolean);
                    ct = parts.slice(0, 4).map(function (p) { return '<span class="badge-tag">' + p + '</span>'; }).join(' ');
                }
                return '<tr><td>' + rankBadge(r.fused_rank) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td class="c-gray">' + (r.industry_name || '-') + '</td><td>' + pt + '</td><td>' + ct + '</td><td>' + r.appear_days + '/' + days + '</td><td>' + fmt(r.continuity_rate, 0) + '%</td><td class="' + clsPct(r.avg_change_pct) + '">' + pct(r.avg_change_pct) + '</td><td>' + fmt(r.avg_east_rank, 1) + '</td><td>' + fmt(r.avg_ths_rank, 1) + '</td><td>' + fmt(r.avg_xq_rank, 1) + '</td><td>' + fmt(r.avg_sina_rank, 1) + '</td><td>' + fmt(r.last_east_rank, 0) + '</td><td>' + fmt(r.last_ths_rank, 0) + '</td><td>' + fmt(r.last_xq_rank, 0) + '</td><td>' + fmt(r.last_sina_rank, 0) + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
            }, 50, h);
        });
    }

    function loadConceptMulti(d, days, pt, c) {
        var label = pt === 1 ? '概念' : '行业';
        apiGet('/concept-multi-day?stat_date=' + d + '&days=' + days + '&plate_type=' + pt).then(function (res) {
            syncDateFromResponse(res);
            if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无近' + days + '天' + label + '数据</div>'; return; }
            var isIndustry = (pt === 2);
            var h = '<div class="stats-bar">' + card('区间', '近' + days + '天', 'blue') + card(label + '板块', res.total, 'orange') + '</div>';
            var cols = ['排名', '代码', '名称', '出现天数', '频率', '平均排名', '最佳排名', '最新排名', '均涨跌幅', '最新涨跌幅', '均热度', '成分股'];
            window.renderTable(c, 'cm' + days + '_' + pt, cols, res.data, function (r, i) {
                var hh = '<tr><td>' + rankBadge(i + 1) + '</td><td>' + r.concept_code + '</td><td>' + conceptNameLink(r.concept_code, r.concept_name, isIndustry) + '</td><td>' + r.appear_days + '/' + days + '</td><td>' + (r.appear_pct || 0) + '%</td><td>' + fmt(r.avg_rank, 1) + '</td><td>' + fmt(r.best_rank, 0) + '</td><td>' + fmt(r.last_rank, 0) + '</td><td class="' + clsPct(r.avg_change_pct) + '">' + pct(r.avg_change_pct) + '</td><td class="' + clsPct(r.last_change_pct) + '">' + pct(r.last_change_pct) + '</td><td>' + fmt(r.avg_hot_value, 0) + '</td>';
                hh += '<td><button onclick="showConceptStocks(\'' + r.concept_code + '\',\'' + (r.concept_name || '') + '\')" style="padding:2px 8px;font-size:11px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer">查看</button></td></tr>';
                return hh;
            }, 20, h);
        }).catch(function () { c.innerHTML = '<div class="loading">加载失败</div>'; });
    }

    /* ===== 全市场股票 ===== */
    var _stockListState = { page: 1, keyword: '', price: '', sort: 'change_pct', order: 'desc' };

    function loadStockListPage(d, c) {
        var h = '<div style="background:#fff;border-radius:10px;padding:16px 20px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
        h += '<div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center">';
        h += '<input id="slKeyword" type="text" placeholder="输入股票代码或名称" value="' + _stockListState.keyword + '" style="width:200px;padding:8px 12px;border:1px solid #ddd;border-radius:6px;font-size:14px">';
        h += '<input id="slPrice" type="number" step="0.01" placeholder="输入金额搜索" value="' + _stockListState.price + '" style="width:160px;padding:8px 12px;border:1px solid #ddd;border-radius:6px;font-size:14px">';
        h += '<button onclick="doStockListSearch()" style="padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:14px;cursor:pointer;font-weight:600">🔍 搜索</button>';
        h += '<button onclick="resetStockListSearch()" style="padding:8px 16px;border:1px solid #ddd;border-radius:6px;background:#fff;font-size:13px;cursor:pointer;color:#666">重置</button>';
        h += '<span id="slModeLabel" style="font-size:12px;color:#888;margin-left:8px"></span>';
        h += '</div></div>';
        h += '<div id="stockListContent"><div class="loading">加载中...</div></div>';
        c.innerHTML = h;
        // 绑定回车搜索
        var kwInput = document.getElementById('slKeyword');
        var prInput = document.getElementById('slPrice');
        if (kwInput) kwInput.addEventListener('keydown', function (e) { if (e.key === 'Enter') doStockListSearch(); });
        if (prInput) prInput.addEventListener('keydown', function (e) { if (e.key === 'Enter') doStockListSearch(); });
        fetchStockList();
    }

    window.doStockListSearch = function () {
        _stockListState.keyword = (document.getElementById('slKeyword') || {}).value || '';
        _stockListState.price = (document.getElementById('slPrice') || {}).value || '';
        _stockListState.page = 1;
        fetchStockList();
    };

    window.resetStockListSearch = function () {
        _stockListState.keyword = '';
        _stockListState.price = '';
        _stockListState.page = 1;
        _stockListState.sort = 'change_pct';
        _stockListState.order = 'desc';
        var kw = document.getElementById('slKeyword'); if (kw) kw.value = '';
        var pr = document.getElementById('slPrice'); if (pr) pr.value = '';
        fetchStockList();
    };

    function fetchStockList() {
        var s = _stockListState;
        var url = '/stock-list?page=' + s.page + '&page_size=50&sort=' + s.sort + '&order=' + s.order;
        if (s.keyword) url += '&keyword=' + encodeURIComponent(s.keyword);
        if (s.price) url += '&price=' + s.price;
        var box = document.getElementById('stockListContent');
        if (box) box.innerHTML = '<div class="loading">加载中...</div>';
        apiGet(url).then(function (res) {
            var label = document.getElementById('slModeLabel');
            if (label) label.textContent = res.mode_label ? '📡 ' + res.mode_label + ' | ' + res.date : '';
            renderStockListTable(box || document.getElementById('stockListContent'), res);
        }).catch(function () {
            if (box) box.innerHTML = '<div class="loading">加载失败</div>';
        });
    }

    function renderStockListTable(box, res) {
        if (!box) return;
        if (!res.data || !res.data.length) {
            box.innerHTML = '<div class="loading">暂无数据' + (res.error ? '<br><span style="font-size:11px;color:#e74c3c">' + res.error + '</span>' : '') + '</div>';
            return;
        }
        var st = _stockListState;
        var sortArrow = function (col) {
            if (st.sort !== col) return '';
            return st.order === 'desc' ? ' ▼' : ' ▲';
        };
        var sortClick = function (col) {
            if (st.sort === col) { st.order = st.order === 'desc' ? 'asc' : 'desc'; }
            else { st.sort = col; st.order = 'desc'; }
            st.page = 1;
            fetchStockList();
        };
        var cols = [
            { label: '序号', key: null },
            { label: '代码' + sortArrow('stock_code'), key: 'stock_code' },
            { label: '名称' + sortArrow('short_name'), key: 'short_name' },
            { label: '最新价' + sortArrow('close'), key: 'close' },
            { label: '涨跌幅' + sortArrow('change_pct'), key: 'change_pct' },
            { label: '近3日' + sortArrow('change_3d'), key: 'change_3d' },
            { label: '近5日' + sortArrow('change_5d'), key: 'change_5d' },
            { label: '近10日' + sortArrow('change_10d'), key: 'change_10d' },
            { label: '成交额' + sortArrow('amount'), key: 'amount' },
            { label: '换手率' + sortArrow('turnover_ratio'), key: 'turnover_ratio' },
            { label: '主力净流入' + sortArrow('main_net_inflow'), key: 'main_net_inflow' },
            { label: '总市值' + sortArrow('market_cap'), key: 'market_cap' },
            { label: '行业', key: null }
        ];

        var h = '<div class="table-wrap"><table><thead><tr>';
        cols.forEach(function (c, i) {
            if (c.key) {
                h += '<th style="cursor:pointer;user-select:none" onclick="slSort(\'' + c.key + '\')">' + c.label + '</th>';
            } else {
                h += '<th>' + c.label + '</th>';
            }
        });
        h += '</tr></thead><tbody>';
        var offset = ((res.page || 1) - 1) * (res.page_size || 50);
        res.data.forEach(function (r, i) {
            var code = r.stock_code || '';
            var name = r.short_name || '';
            var price = r.price != null ? Number(r.price).toFixed(2) : '-';
            var chgClass = clsPct(r.change_pct);
            var chg = pct(r.change_pct);
            var chg3 = pct(r.change_3d);
            var chg3Cls = clsPct(r.change_3d);
            var chg5 = pct(r.change_5d);
            var chg5Cls = clsPct(r.change_5d);
            var chg10 = pct(r.change_10d);
            var chg10Cls = clsPct(r.change_10d);
            var amt = r.amount != null ? fmtMoney(r.amount) : '-';
            var to = r.turnover_ratio != null ? Number(r.turnover_ratio).toFixed(2) + '%' : '-';
            var flow = r.main_net_inflow != null ? fmtFlow(r.main_net_inflow) : '-';
            var flowCls = r.main_net_inflow > 0 ? 'style="color:#e74c3c"' : r.main_net_inflow < 0 ? 'style="color:#27ae60"' : '';
            var mcap = r.market_cap != null ? fmtMoney(r.market_cap) : '-';
            var ind = r.industry || '-';
            h += '<tr style="cursor:pointer" onclick="openStockDetail(\'' + code + '\')">';
            h += '<td>' + (offset + i + 1) + '</td>';
            h += '<td>' + code + '</td>';
            h += '<td><strong>' + name + '</strong>';
            if (r.is_holding) h += ' <span style="background:#e74c3c;color:#fff;padding:1px 5px;border-radius:3px;font-size:10px;margin-left:4px">持仓</span>';
            else if (r.sort_order != null) h += ' <span style="background:#1a73e8;color:#fff;padding:1px 5px;border-radius:3px;font-size:10px;margin-left:4px">自选</span>';
            h += '</td>';
            h += '<td class="' + chgClass + '">' + price + '</td>';
            h += '<td class="' + chgClass + '">' + chg + '</td>';
            h += '<td class="' + chg3Cls + '">' + chg3 + '</td>';
            h += '<td class="' + chg5Cls + '">' + chg5 + '</td>';
            h += '<td class="' + chg10Cls + '">' + chg10 + '</td>';
            h += '<td>' + amt + '</td>';
            h += '<td>' + to + '</td>';
            h += '<td ' + flowCls + '>' + flow + '</td>';
            h += '<td>' + mcap + '</td>';
            h += '<td class="c-gray">' + ind + '</td>';
            h += '</tr>';
        });
        h += '</tbody></table></div>';

        // 分页
        var total = res.total || 0;
        var pageSize = res.page_size || 50;
        var totalPages = Math.ceil(total / pageSize);
        var curPage = res.page || 1;
        h += '<div style="display:flex;justify-content:space-between;align-items:center;padding:12px 0">';
        h += '<span style="color:#888;font-size:13px">共 ' + total + ' 只股票，第 ' + curPage + '/' + totalPages + ' 页</span>';
        h += '<div style="display:flex;gap:6px">';
        if (curPage > 1) h += '<button onclick="slPage(' + (curPage - 1) + ')" style="padding:6px 14px;border:1px solid #ddd;border-radius:4px;background:#fff;cursor:pointer;font-size:13px">上一页</button>';
        if (curPage < totalPages) h += '<button onclick="slPage(' + (curPage + 1) + ')" style="padding:6px 14px;border:1px solid #ddd;border-radius:4px;background:#fff;cursor:pointer;font-size:13px">下一页</button>';
        h += '</div></div>';
        box.innerHTML = h;
    }

    window.slSort = function (col) {
        var st = _stockListState;
        if (st.sort === col) { st.order = st.order === 'desc' ? 'asc' : 'desc'; }
        else { st.sort = col; st.order = 'desc'; }
        st.page = 1;
        fetchStockList();
    };

    window.slPage = function (p) {
        _stockListState.page = p;
        fetchStockList();
    };

    function fmtMoney(v) {
        if (v == null || v === 0) return '-';
        v = Number(v);
        if (Math.abs(v) >= 1e12) return (v / 1e12).toFixed(2) + '万亿';
        if (Math.abs(v) >= 1e8) return (v / 1e8).toFixed(2) + '亿';
        if (Math.abs(v) >= 1e4) return (v / 1e4).toFixed(0) + '万';
        return v.toFixed(0);
    }

    function fmtFlow(v) {
        if (v == null || v === 0) return '-';
        v = Number(v);
        var sign = v > 0 ? '+' : '';
        if (Math.abs(v) >= 1e8) return sign + (v / 1e8).toFixed(2) + '亿';
        if (Math.abs(v) >= 1e4) return sign + (v / 1e4).toFixed(0) + '万';
        return sign + v.toFixed(0);
    }

    /* ===== 股票详情弹窗 ===== */
    function clampNumber(v, min, max) {
        var n = Number(v);
        if (isNaN(n)) return min;
        return Math.max(min, Math.min(max, n));
    }

    function mainforceBehaviorFromScore(score) {
        var n = Number(score);
        if (isNaN(n)) return '\u4e2d\u6027';
        if (n >= 62) return '\u5efa\u4ed3';
        if (n <= 38) return '\u51fa\u8d27';
        return '\u6d17\u76d8';
    }

    function mainforceScoreColor(score) {
        var n = Number(score);
        if (isNaN(n)) return '#64748b';
        return n >= 60 ? '#e74c3c' : (n <= 40 ? '#27ae60' : '#f5a623');
    }

    function mainforceConfidenceColor(confidence) {
        var n = Number(confidence);
        if (isNaN(n)) return '#64748b';
        return n > 70 ? '#e74c3c' : (n > 50 ? '#f5a623' : '#27ae60');
    }

    function mainforcePriceDirection(changePct) {
        var n = Number(changePct);
        if (isNaN(n) || n === 0) return '\u4e2d\u6027';
        if (n > 0) return '\u5efa\u4ed3';
        return n >= -2 ? '\u6d17\u76d8' : '\u51fa\u8d27';
    }

    function buildMainforceFallback(detail, degraded) {
        detail = detail || {};
        var market = detail.market || {};
        var cap = detail.capital || {};
        var today = cap.today || {};
        var holder = detail.holder || {};
        var ai = detail.ai_analysis || {};
        var snap = detail.analysis_snapshot || {};
        var tags = [];
        var score = 50;

        if (today.main_net_inflow != null && !isNaN(today.main_net_inflow)) {
            var mainFlow = Number(today.main_net_inflow);
            score += mainFlow > 0 ? 10 : (mainFlow < 0 ? -10 : 0);
            tags.push({
                label: 'Day Flow',
                value: fmtFlow(mainFlow),
                direction: mainFlow > 0 ? '\u5efa\u4ed3' : (mainFlow < 0 ? '\u51fa\u8d27' : '\u4e2d\u6027')
            });
        }

        if (cap.flow_5d != null && !isNaN(cap.flow_5d)) {
            var flow5d = Number(cap.flow_5d);
            score += flow5d > 0 ? 8 : (flow5d < 0 ? -8 : 0);
            tags.push({
                label: '5D Flow',
                value: fmtFlow(flow5d),
                direction: flow5d > 0 ? '\u5efa\u4ed3' : (flow5d < 0 ? '\u51fa\u8d27' : '\u4e2d\u6027')
            });
        }

        if (market.change_pct != null && !isNaN(market.change_pct)) {
            var priceChange = Number(market.change_pct);
            score += priceChange > 0 ? 4 : (priceChange < 0 ? -4 : 0);
            tags.push({
                label: 'Price',
                value: pct(priceChange),
                direction: mainforcePriceDirection(priceChange)
            });
        }

        var holderChange = holder.holder_num_change;
        if ((holderChange == null || isNaN(holderChange)) && holder.holder_num_ratio != null && !isNaN(holder.holder_num_ratio)) {
            holderChange = Number(holder.holder_num_ratio);
        }
        if (holderChange != null && !isNaN(holderChange)) {
            holderChange = Number(holderChange);
            score += holderChange < 0 ? 4 : (holderChange > 0 ? -4 : 0);
            tags.push({
                label: 'Holders',
                value: holderChange > 0 ? '+' + holderChange.toFixed(1) : holderChange.toFixed(1),
                direction: holderChange < 0 ? '\u5efa\u4ed3' : (holderChange > 0 ? '\u51fa\u8d27' : '\u4e2d\u6027')
            });
        }

        if (ai.score != null && !isNaN(ai.score)) {
            score = score * 0.7 + Number(ai.score) * 0.3;
        } else if (snap.total_score != null && !isNaN(snap.total_score)) {
            score = score * 0.7 + Number(snap.total_score) * 0.3;
        }

        score = Math.round(clampNumber(score, 20, 80) * 10) / 10;

        var noteParts = [];
        if (degraded) {
            noteParts.push('实时主力分析暂不可用，先保留快速判断结果。');
        } else {
            noteParts.push('基于资金流和价格行为生成快速判断，若实时信号可用会自动升级。');
        }

        var reasonText = firstNonEmptyText(ai.action_reason, snap.recommend_reason, snap.summary);
        if (reasonText) noteParts.push(reasonText);

        return {
            behavior: mainforceBehaviorFromScore(score),
            confidence: Math.round(clampNumber(34 + tags.length * 7 + (reasonText ? 5 : 0), 34, 68)),
            score: score,
            tags: tags,
            note: noteParts.join(' '),
            sourceNote: degraded ? 'fallback' : 'quick view'
        };
    }

    function renderStockDetailMainforceSummary(data, detail) {
        data = data || {};
        detail = detail || {};
        var behavior = data.behavior || '\u4e2d\u6027';
        var score = Number(data.score != null ? data.score : 50);
        var confidence = Number(data.confidence != null ? data.confidence : 0);
        var sourceNote = data.sourceNote || '';
        var tags = Array.isArray(data.tags) ? data.tags.slice() : [];
        if (!tags.length && data.signals) {
            var sigLabels = {
                volume_price: '量价',
                capital_flow: '资金',
                kline_pattern: 'K线',
                chip_concentration: '筹码',
                institutional: '机构'
            };
            Object.keys(sigLabels).forEach(function (key) {
                var signal = data.signals[key] || {};
                tags.push({
                    label: sigLabels[key],
                    value: signal.score != null ? signal.score : '-',
                    direction: signal.direction || '\u4e2d\u6027'
                });
            });
        }

        var safeCode = String(detail.stock_code || '').replace(/'/g, "\\'");
        var safeName = String(detail.short_name || detail.stock_code || '').replace(/'/g, "\\'");
        var h = '';
        h += '<div style="display:flex;align-items:center;gap:16px;margin-bottom:10px;flex-wrap:wrap">';
        h += '<span class="mf-big-badge mf-big-' + behavior + '" style="font-size:16px;padding:4px 16px">' + behavior + '</span>';
        h += '<div style="text-align:center"><div style="font-size:24px;font-weight:800;color:' + mainforceScoreColor(score) + '">' + fmt(score, 1) + '</div><div style="font-size:10px;color:#888">评分</div></div>';
        h += '<div style="flex:1;min-width:180px"><div style="font-size:11px;color:#888;margin-bottom:3px">置信度 ' + Math.round(confidence) + '%</div>';
        h += '<div class="mf-confidence-bar"><div class="mf-confidence-fill" style="width:' + clampNumber(confidence, 0, 100) + '%;background:' + mainforceConfidenceColor(confidence) + '"></div></div>';
        if (sourceNote) h += '<div style="font-size:10px;color:#888;margin-top:6px">' + (sourceNote === 'live' ? '实时分析' : sourceNote === 'fallback' ? '回退视图' : sourceNote === 'quick view' ? '快速视图' : sourceNote) + '</div>';
        h += '</div>';
        h += '<button onclick="openMainforceDetailSafe(\'' + safeCode + '\',\'' + safeName + '\')" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:12px;cursor:pointer;white-space:nowrap">查看详解</button>';
        h += '</div>';
        if (tags.length) {
            h += '<div style="display:flex;gap:6px;flex-wrap:wrap">';
            tags.forEach(function (tag) {
                h += '<span class="mf-badge mf-' + (tag.direction || '\u4e2d\u6027') + '" style="font-size:11px">' + (tag.label || '-') + ' ' + (tag.value != null ? tag.value : '-') + '</span>';
            });
            h += '</div>';
        }
        if (data.note) {
            h += '<div style="margin-top:8px;font-size:11px;color:#666;line-height:1.5">' + data.note + '</div>';
        }
        return h;
    }

    function renderMainforceFallbackBody(detail, message) {
        var fallback = buildMainforceFallback(detail, true);
        if (message) fallback.note = fallback.note + ' ' + message;
        var h = '<div style="background:#fff;border-radius:10px;padding:16px 20px">';
        h += '<div style="font-size:14px;font-weight:700;margin-bottom:10px;color:#333">主力行为快速视图</div>';
        h += renderStockDetailMainforceSummary(fallback, detail);
        h += '</div>';
        return h;
    }

    window.openMainforceDetailSafe = function (code, name) {
        var titleEl = document.getElementById('mainforceModalTitle');
        if (titleEl) titleEl.textContent = '\u{1F50D} \u4E3B\u529B\u884C\u4E3A\u5206\u6790 | ' + (name || code);
        var bodyEl = document.getElementById('mainforceModalBody');
        var fallbackDetail = window._mainforceFallbackContext && window._mainforceFallbackContext[code];
        if (bodyEl) {
            bodyEl.innerHTML = fallbackDetail
                ? renderMainforceFallbackBody(fallbackDetail, '正在后台补充实时主力明细。')
                : '<div style="text-align:center;padding:30px;color:#888"><span class="spinner"></span> \u5206\u6790\u4E2D...</div>';
        }
        var overlay = document.getElementById('mainforceModal');
        if (overlay) overlay.classList.add('show');

        var requestId = (window._safeMainforceModalReqId || 0) + 1;
        window._safeMainforceModalReqId = requestId;

        fetch(API_BASE + '/mainforce-analysis?stock_code=' + encodeURIComponent(code))
            .then(function (r) { return r.json(); })
            .then(function (data) {
                if (window._safeMainforceModalReqId !== requestId) return;
                var b = document.getElementById('mainforceModalBody');
                if (!b) return;
                if (data && !data.error) {
                    b.innerHTML = renderMainforceDetail(data);
                    setTimeout(function () { initMainforceCharts(data); }, 100);
                    return;
                }
                if (fallbackDetail) {
                    b.innerHTML = renderMainforceFallbackBody(fallbackDetail, 'Live mainforce detail is unavailable right now, so the quick version is kept on screen.');
                    return;
                }
                b.innerHTML = '<div style="color:#e74c3c;padding:20px">\u26A0\uFE0F ' + ((data && data.error) || 'Unavailable') + '</div>';
            })
            .catch(function (e) {
                if (window._safeMainforceModalReqId !== requestId) return;
                var b = document.getElementById('mainforceModalBody');
                if (!b) return;
                if (fallbackDetail) {
                    b.innerHTML = renderMainforceFallbackBody(fallbackDetail, 'Live mainforce detail request failed, so the quick version is kept on screen.');
                    return;
                }
                b.innerHTML = '<div style="color:#e74c3c;padding:20px">\u26A0\uFE0F ' + e.message + '</div>';
            });
    };

    function loadStockDetailMainforce(detail) {
        var mfContent = document.getElementById('mfDetailContent');
        if (!mfContent) return;

        var requestId = (window._stockDetailMainforceReqId || 0) + 1;
        window._stockDetailMainforceReqId = requestId;
        window._mainforceFallbackContext = window._mainforceFallbackContext || {};
        window._mainforceFallbackContext[detail.stock_code] = detail;

        var quickView = buildMainforceFallback(detail, false);
        mfContent.innerHTML = renderStockDetailMainforceSummary(quickView, detail);

        fetchJsonWithTimeout('/mainforce-analysis?stock_code=' + encodeURIComponent(detail.stock_code), 4500)
            .then(function (mf) {
                if (window._stockDetailMainforceReqId !== requestId) return;
                if (!mfContent || !document.body.contains(mfContent)) return;
                if (!mf || mf.error) {
                    quickView.sourceNote = 'fallback';
                    quickView.note = '实时主力分析未返回有效结果，先保留快速视图。';
                    mfContent.innerHTML = renderStockDetailMainforceSummary(quickView, detail);
                    return;
                }
                mf.sourceNote = 'live';
                mfContent.innerHTML = renderStockDetailMainforceSummary(mf, detail);
            })
            .catch(function () {
                if (window._stockDetailMainforceReqId !== requestId) return;
                if (!mfContent || !document.body.contains(mfContent)) return;
                mfContent.innerHTML = renderStockDetailMainforceSummary(buildMainforceFallback(detail, true), detail);
            });
    }

    window.openStockDetail = function (code) {
        var body = document.getElementById('stockDetailBody');
        var title = document.getElementById('stockDetailTitle');
        title.textContent = '📋 股票详情 — ' + code;
        body.innerHTML = '<div class="loading">加载中...</div>';
        document.getElementById('stockDetailModal').classList.add('show');
        apiGet('/stock-detail?stock_code=' + code).then(function (res) {
            if (res.error) { body.innerHTML = '<div class="loading" style="color:#e74c3c">❌ ' + res.error + '</div>'; return; }
            renderStockDetail(body, res);
        }).catch(function () { body.innerHTML = '<div class="loading">加载失败</div>'; });
    };

    function renderStockDetail(body, d) {
        var m = d.market || {};
        var cap = d.capital || {};
        var fin = d.finance || {};
        var val = d.valuation || {};
        var tech = d.technical || {};
        var news = d.news || {};
        var ai = d.ai_analysis || {};
        var snap = d.analysis_snapshot || {};
        var snapStrengths = Array.isArray(snap.strengths) ? snap.strengths : [];
        var snapRisks = Array.isArray(snap.risks) ? snap.risks : [];
        var qualityFlags = Array.isArray(snap.data_quality_flags) ? snap.data_quality_flags : [];
        var analysisMeta = analysisSummaryMeta(ai, snap, d.date, d.recommendation_snapshot || {});
        var hasAnalysisMeta = !!(analysisMeta.status || analysisMeta.riskLevel || analysisMeta.score != null || analysisMeta.tradeScore != null || analysisMeta.action || analysisMeta.date || snap.summary || snap.recommendation);
        var chgClass = clsPct(m.change_pct);
        var h = '';

        // ── 标题行 ──
        h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">';
        h += '<div><span style="font-size:20px;font-weight:700">' + (d.short_name || '') + '</span> <span style="color:#888;font-size:14px">' + d.stock_code + '</span>';
        if (d.industry) h += ' <span style="background:#e8f5e9;color:#2e7d32;padding:2px 8px;border-radius:4px;font-size:12px">' + d.industry + '</span>';
        h += '</div>';
        h += '<div style="text-align:right"><span class="' + chgClass + '" style="font-size:24px;font-weight:700">' + (m.price != null ? Number(m.price).toFixed(2) : '-') + '</span>';
        h += '<br><span class="' + chgClass + '" style="font-size:14px">' + pct(m.change_pct) + '</span></div>';
        h += '</div>';

        // ── 持仓信息（如有） ──
        h += buildStockDetailMeta(d, analysisMeta);
        if (d.holding && d.holding.shares > 0) {
            var hd = d.holding;
            var costP = Number(hd.cost_price) || 0;
            var curP = Number(m.price) || 0;
            var pnl = curP > 0 && costP > 0 ? ((curP / costP - 1) * 100).toFixed(2) : '0';
            var pnlAmt = curP > 0 && costP > 0 ? Math.round((curP - costP) * hd.shares) : 0;
            var pnlCls = pnlAmt >= 0 ? '#e74c3c' : '#27ae60';
            h += '<div style="background:linear-gradient(135deg,#2d1b69,#1a1a2e);border-radius:10px;padding:12px 16px;margin-bottom:16px;color:#fff;display:flex;align-items:center;gap:16px">';
            h += '<div style="text-align:center"><div style="font-size:10px;color:#aaa">持仓</div><div style="font-size:16px;font-weight:700">' + hd.shares + '股</div></div>';
            h += '<div style="text-align:center"><div style="font-size:10px;color:#aaa">成本价</div><div style="font-size:16px;font-weight:700">' + costP.toFixed(2) + '</div></div>';
            h += '<div style="text-align:center"><div style="font-size:10px;color:#aaa">持仓盈亏</div><div style="font-size:16px;font-weight:700;color:' + pnlCls + '">' + (pnlAmt >= 0 ? '+' : '') + pnlAmt + '元</div></div>';
            h += '<div style="text-align:center"><div style="font-size:10px;color:#aaa">盈亏比例</div><div style="font-size:16px;font-weight:700;color:' + pnlCls + '">' + (pnl >= 0 ? '+' : '') + pnl + '%</div></div>';
            h += '</div>';
        }

        // ── 七、AI投资分析（置顶）──
        // 原格式：显示 DeepSeek 生成的详细分析
        if (hasAnalysisMeta) {
            h += renderAnalysisSummaryCard(analysisMeta, '综合分析');
            if (snap.summary) h += '<div style="font-size:13px;line-height:1.7;color:#374151;margin:-6px 0 12px"><strong>分析摘要：</strong>' + localizeMachineText(snap.summary) + '</div>';
            if (snap.recommendation) h += '<div style="font-size:13px;line-height:1.7;color:#374151;margin-bottom:12px"><strong>操作建议：</strong>' + localizeMachineText(snap.recommendation) + '</div>';
            if (snapStrengths.length > 0) {
                h += '<div style="margin-bottom:8px"><div style="font-size:12px;font-weight:700;color:#2e7d32;margin-bottom:6px">优势亮点</div><div style="display:flex;gap:6px;flex-wrap:wrap">';
                snapStrengths.slice(0, 6).forEach(function (item) {
                    h += '<span style="background:#e8f5e9;color:#1b5e20;padding:4px 10px;border-radius:999px;font-size:12px">' + item + '</span>';
                });
                h += '</div></div>';
            }
            if (snapRisks.length > 0 || qualityFlags.length > 0) {
                h += '<div><div style="font-size:12px;font-weight:700;color:#b71c1c;margin-bottom:6px">风险提示</div><div style="display:flex;gap:6px;flex-wrap:wrap">';
                snapRisks.slice(0, 6).forEach(function (item) {
                    h += '<span style="background:#ffebee;color:#b71c1c;padding:4px 10px;border-radius:999px;font-size:12px">' + item + '</span>';
                });
                qualityFlags.slice(0, 4).forEach(function (item) {
                    h += '<span style="background:#fff3e0;color:#e65100;padding:4px 10px;border-radius:999px;font-size:12px">数据标记：' + item + '</span>';
                });
                h += '</div></div>';
            }
            var eventDetail = snap.event_risk_detail || {};
            if (Array.isArray(eventDetail)) eventDetail = eventDetail[0] || {};
            var eventImpact = eventDetail.event_impact || {};
            var beneficiaries = Array.isArray(eventImpact.beneficiaries) ? eventImpact.beneficiaries : [];
            var damaged = Array.isArray(eventImpact.damaged) ? eventImpact.damaged : [];
            if (beneficiaries.length > 0 || damaged.length > 0) {
                h += '<div style="margin-top:10px"><div style="font-size:12px;font-weight:700;color:#374151;margin-bottom:6px">事件影响</div><div style="display:flex;gap:6px;flex-wrap:wrap">';
                beneficiaries.slice(0, 3).forEach(function (item) {
                    h += '<span title="' + escAttr(item.reason || '') + '" style="background:#e8f5e9;color:#166534;padding:4px 10px;border-radius:999px;font-size:12px">受益：' + escHtml(item.target || '-') + '</span>';
                });
                damaged.slice(0, 3).forEach(function (item) {
                    h += '<span title="' + escAttr(item.reason || '') + '" style="background:#ffebee;color:#991b1b;padding:4px 10px;border-radius:999px;font-size:12px">受损：' + escHtml(item.target || '-') + '</span>';
                });
                h += '</div></div>';
            }
            h += '</div>';
        }

        var hasConclusion = ai.conclusion && ai.conclusion.length > 10;

        if (hasConclusion) {
            h += '<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;margin-bottom:16px;color:#fff">';
            h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
            h += '<span style="font-size:12px;color:#aaa">分析日期 ' + (analysisMeta.date || d.analysis_trade_date || d.date || '') + '</span>';
            h += '<span style="font-size:14px;color:#ddd">行情日期 ' + (d.quote_trade_date || d.date || '-') + ' | 现价 ' + (m.price || '-') + ' ' + pct(m.change_pct) + '</span>';
            h += '</div>';
            h += '<div style="font-size:14px;color:#ddd;line-height:1.8;white-space:pre-wrap">' + localizeMachineText(ai.conclusion) + '</div>';
            if (analysisMeta.action) {
                var actionColor = analysisActionColor(analysisMeta.action);
                h += '<div style="margin-top:12px;display:flex;align-items:center;gap:10px">';
                h += '<span style="background:' + actionColor + ';color:#fff;padding:4px 12px;border-radius:6px;font-size:14px;font-weight:700">操作建议：' + analysisMeta.action + '</span>';
                if (analysisMeta.actionReason) h += '<span style="font-size:12px;color:#ccc">' + localizeMachineText(analysisMeta.actionReason) + '</span>';
                h += '</div>';
            }
            h += '</div>';
        } else if (analysisMeta.score != null || analysisMeta.longScore != null) {
            h += '<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;margin-bottom:16px;color:#fff">';
            var scoreValue = analysisMeta.score != null ? analysisMeta.score : analysisMeta.longScore;
            var sColor = scoreValue >= 70 ? '#e74c3c' : scoreValue >= 50 ? '#f39c12' : '#27ae60';
            h += '<div style="display:flex;align-items:center;gap:16px;margin-bottom:12px">';
            h += '<div style="text-align:center"><div style="font-size:36px;font-weight:800;color:' + sColor + '">' + (scoreValue != null ? scoreValue : '-') + '</div><div style="font-size:11px;color:#aaa">????</div></div>';
            h += '</div>';
            if (ai.conclusion) h += '<div style="font-size:13px;color:#ddd;line-height:1.6">' + ai.conclusion + '</div>';
            h += '</div>';
        }

        h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
        h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">📊 行情面</div>';
        h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(108px,1fr));gap:10px">';
        var qFields = [
            { label: '现价', value: m.price != null ? fmtPrice(m.price) : '-' },
            { label: '涨跌幅', value: m.change_pct != null ? pct(m.change_pct) : '-', cls: chgClass },
            { label: 'PE(TTM)', value: m.pe_ttm != null ? Number(m.pe_ttm).toFixed(1) : '-' },
            { label: 'PB', value: m.pb != null ? Number(m.pb).toFixed(2) : '-' },
            { label: '量比', value: m.volume_ratio != null ? Number(m.volume_ratio).toFixed(2) : '-' },
            { label: '换手率', value: m.turnover_ratio != null ? Number(m.turnover_ratio).toFixed(2) + '%' : '-' },
            { label: '振幅', value: m.amplitude != null ? m.amplitude.toFixed(2) + '%' : '-' },
            { label: '成交额', value: m.amount ? fmtMoney(m.amount) : '-' },
            { label: '今开', value: m.open != null ? Number(m.open).toFixed(2) : '-' },
            { label: '最高', value: m.high != null ? Number(m.high).toFixed(2) : '-' },
            { label: '最低', value: m.low != null ? Number(m.low).toFixed(2) : '-' },
            { label: '昨收', value: m.pre_close != null ? Number(m.pre_close).toFixed(2) : '-' },
            { label: '总市值', value: m.market_cap ? fmtMoney(m.market_cap) : '-' },
            { label: '流通市值', value: m.float_market_cap ? fmtMoney(m.float_market_cap) : '-' },
            { label: '总股本', value: m.total_shares ? fmtMoney(m.total_shares) : '-' },
            { label: '成交量', value: m.volume ? fmtMoney(m.volume) : '-' }
        ];
        qFields.forEach(function (item) {
            h += '<div style="background:#fff;padding:10px 8px;border-radius:8px;text-align:center;border:1px solid #eef2f7">';
            h += '<div style="font-size:11px;color:#8a94a6;margin-bottom:4px">' + item.label + '</div>';
            h += '<div class="' + (item.cls || '') + '" style="font-size:14px;font-weight:700;color:' + (item.cls ? '' : '#333') + '">' + item.value + '</div>';
            h += '</div>';
        });
        h += '</div>';
        h += '</div>';

        // ── 股东人数 ──
        var hd = d.holder || {};
        if (hd.holder_num != null) {
            h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
            h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">👥 股东人数 <span style="font-size:11px;color:#888">报告期：' + (hd.report_date || '') + '</span></div>';
            h += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">';
            // 股东人数
            var hNum = hd.holder_num != null ? (hd.holder_num >= 10000 ? (hd.holder_num / 10000).toFixed(1) + '万' : hd.holder_num.toLocaleString()) : '-';
            h += '<div style="text-align:center"><div style="font-size:10px;color:#888">股东人数</div><div style="font-size:14px;font-weight:700;color:#333">' + hNum + '</div></div>';
            // 变化
            if (hd.holder_num_change != null) {
                var chgSign = hd.holder_num_change > 0 ? '+' : '';
                var chgColor = hd.holder_num_change > 0 ? '#e74c3c' : hd.holder_num_change < 0 ? '#27ae60' : '#666';
                var chgText = hd.holder_num_change >= 10000 || hd.holder_num_change <= -10000
                    ? chgSign + (hd.holder_num_change / 10000).toFixed(1) + '万'
                    : chgSign + hd.holder_num_change.toLocaleString();
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">较上期变化</div><div style="font-size:14px;font-weight:700;color:' + chgColor + '">' + chgText + '</div></div>';
            } else {
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">较上期变化</div><div style="font-size:14px;font-weight:700;color:#666">-</div></div>';
            }
            // 变化比例
            if (hd.holder_num_ratio != null) {
                var ratioSign = hd.holder_num_ratio > 0 ? '+' : '';
                var ratioColor = hd.holder_num_ratio > 0 ? '#e74c3c' : hd.holder_num_ratio < 0 ? '#27ae60' : '#666';
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">变化比例</div><div style="font-size:14px;font-weight:700;color:' + ratioColor + '">' + ratioSign + Number(hd.holder_num_ratio).toFixed(2) + '%</div></div>';
            } else {
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">变化比例</div><div style="font-size:14px;font-weight:700;color:#666">-</div></div>';
            }
            // 人均持股
            if (hd.avg_free_shares != null) {
                var avgText = hd.avg_free_shares >= 10000 ? (hd.avg_free_shares / 10000).toFixed(2) + '万股' : Number(hd.avg_free_shares).toFixed(0) + '股';
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">人均持股</div><div style="font-size:14px;font-weight:700;color:#333">' + avgText + '</div></div>';
            } else {
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">人均持股</div><div style="font-size:14px;font-weight:700;color:#666">-</div></div>';
            }
            h += '</div>';
            // 提示文字
            if (hd.holder_num_change != null) {
                var tip = hd.holder_num_change < 0 ? '📉 股东人数减少，筹码趋于集中（利好）' : hd.holder_num_change > 0 ? '📈 股东人数增加，筹码趋于分散（利空）' : '';
                if (tip) h += '<div style="font-size:11px;color:#888;margin-top:8px">' + tip + '</div>';
            }
            h += '</div>';
        }

        // ── 二、资金面 ──
        var ft = cap.today || {};
        if (ft.main_net_inflow != null || cap.flow_5d != null) {
            h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
            var dsLabel = ft.data_source === 'ths' ? '同花顺 · 仅净额' : '东财';
            var dsColor = ft.data_source === 'ths' ? '#e67e22' : '#27ae60';
            h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">💰 资金面 <span style="font-size:11px;color:' + dsColor + ';font-weight:400">(' + dsLabel + ')</span></div>';
            h += '<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:10px">';
            var hasDetail = ft.max_net_inflow != 0 || ft.lg_net_inflow != 0;
            var flows = [
                { label: '今日主力', val: ft.main_net_inflow },
                { label: '超大单', val: hasDetail ? ft.max_net_inflow : null },
                { label: '3日累计', val: cap.flow_3d },
                { label: '5日累计', val: cap.flow_5d },
                { label: '20日累计', val: cap.flow_20d }
            ];
            flows.forEach(function (item) {
                var color = item.val > 0 ? '#e74c3c' : item.val < 0 ? '#27ae60' : '#666';
                var display = item.val != null ? fmtFlow(item.val) : '<span style="color:#ccc">-</span>';
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">' + item.label + '</div>';
                h += '<div style="font-size:12px;font-weight:600;color:' + color + '">' + display + '</div></div>';
            });
            h += '</div>';
            // 龙虎榜
            var lhb = cap.dragon_tiger || {};
            if (lhb.count_20d > 0) {
                h += '<div style="border-top:1px solid #e0e0e0;padding-top:8px;font-size:12px">';
                h += '<span style="color:#e74c3c;font-weight:600">🔥 近20日龙虎榜 ' + lhb.count_20d + ' 次</span>';
                if (lhb.inst_net_buy) h += '，机构净买入 <strong>' + fmtFlow(lhb.inst_net_buy) + '</strong>';
                h += '</div>';
            }
            h += '</div>';
        }

        // ── 主力行为分析（异步加载） ──
        h += '<div id="mfDetailSection" style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
        h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">🔍 主力行为分析 <span style="font-size:11px;color:#888">建仓/洗盘/出货判断</span></div>';
        h += '<div id="mfDetailContent" style="text-align:center;color:#888;font-size:12px">加载中...</div>';
        h += '</div>';

        // ── 三、财务面 ──
        var fl = fin.latest || {};
        if (fl.total_rev) {
            h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
            h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">📈 财务面 <span style="font-size:11px;color:#888">报告期：' + (fl.report_date || '') + '</span></div>';
            h += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px">';
            var finFields = [
                { label: '营收', value: fmtMoney(fl.total_rev) },
                { label: '营收同比', value: fl.total_rev_yoy_gr != null ? (fl.total_rev_yoy_gr > 0 ? '+' : '') + fl.total_rev_yoy_gr.toFixed(1) + '%' : '-', color: fl.total_rev_yoy_gr > 0 ? '#e74c3c' : '#27ae60' },
                { label: '净利润', value: fmtMoney(fl.net_profit_attr_sh) },
                { label: '净利润同比', value: fl.net_profit_yoy_gr != null ? (fl.net_profit_yoy_gr > 0 ? '+' : '') + fl.net_profit_yoy_gr.toFixed(1) + '%' : '-', color: fl.net_profit_yoy_gr > 0 ? '#e74c3c' : '#27ae60' },
                { label: 'ROE', value: fl.roe_wtd != null ? fl.roe_wtd.toFixed(1) + '%' : '-' },
                { label: 'ROA', value: fl.roa_wtd != null ? Number(fl.roa_wtd).toFixed(2) + '%' : '-' },
                { label: '毛利率', value: fl.gross_margin != null ? fl.gross_margin.toFixed(1) + '%' : '-' },
                { label: '净利率', value: fl.net_margin != null ? fl.net_margin.toFixed(1) + '%' : '-' }
            ];
            finFields.forEach(function (item) {
                h += '<div style="text-align:center"><div style="font-size:10px;color:#888">' + item.label + '</div>';
                h += '<div style="font-size:13px;font-weight:600;color:' + (item.color || '#333') + '">' + item.value + '</div></div>';
            });
            h += '</div>';
            // 季度趋势
            var quarters = fin.quarters || [];
            if (quarters.length > 1) {
                h += '<div style="border-top:1px solid #e0e0e0;padding-top:8px">';
                h += '<div style="font-size:11px;color:#888;margin-bottom:6px">季度趋势</div>';
                h += '<div style="display:flex;gap:6px;flex-wrap:wrap">';
                quarters.slice(0, 6).forEach(function (q) {
                    var rc = q.report_date ? q.report_date.substring(2, 7) : '';
                    var qc = q.net_profit_yoy_gr > 0 ? '#e74c3c' : '#27ae60';
                    h += '<div style="background:#fff;padding:4px 8px;border-radius:4px;text-align:center;min-width:60px">';
                    h += '<div style="font-size:9px;color:#888">' + rc + '</div>';
                    h += '<div style="font-size:11px;font-weight:600;color:' + qc + '">' + (q.net_profit_yoy_gr != null ? (q.net_profit_yoy_gr > 0 ? '+' : '') + q.net_profit_yoy_gr.toFixed(0) + '%' : '-') + '</div>';
                    h += '</div>';
                });
                h += '</div></div>';
            }
            h += '</div>';
        }

        // ── 四、估值面 ──
        if (val.pe_ttm != null || val.pb != null) {
            h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
            h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">💎 估值面</div>';
            h += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px">';
            // PE
            h += '<div style="text-align:center"><div style="font-size:10px;color:#888">PE(TTM)</div>';
            h += '<div style="font-size:16px;font-weight:700;color:#333">' + (val.pe_ttm != null ? Number(val.pe_ttm).toFixed(1) : '-') + '</div>';
            if (val.pe_percentile != null) {
                var pc = val.pe_percentile > 70 ? '#e74c3c' : val.pe_percentile < 30 ? '#27ae60' : '#f39c12';
                h += '<div style="font-size:10px;color:' + pc + '">历史' + val.pe_percentile + '%分位</div>';
            }
            h += '</div>';
            // PB
            h += '<div style="text-align:center"><div style="font-size:10px;color:#888">PB</div>';
            h += '<div style="font-size:16px;font-weight:700;color:#333">' + (val.pb != null ? Number(val.pb).toFixed(2) : '-') + '</div>';
            if (val.pb_percentile != null) {
                var bc = val.pb_percentile > 70 ? '#e74c3c' : val.pb_percentile < 30 ? '#27ae60' : '#f39c12';
                h += '<div style="font-size:10px;color:' + bc + '">历史' + val.pb_percentile + '%分位</div>';
            }
            h += '</div>';
            // 判定
            h += '<div style="text-align:center;display:flex;flex-direction:column;justify-content:center">';
            if (val.verdict) {
                var vc = val.verdict === '偏高' ? '#e74c3c' : val.verdict === '偏低' ? '#27ae60' : '#f39c12';
                h += '<div style="font-size:20px;font-weight:800;color:' + vc + '">' + val.verdict + '</div>';
            }
            h += '<div style="font-size:10px;color:#888">估值判定</div></div>';
            h += '</div></div>';
        }

        // ── 五、技术面 ──
        var ma = tech.ma || {};
        var macd = tech.macd || {};
        var kdj = tech.kdj || {};
        var rsi = tech.rsi || {};
        var boll = tech.boll || {};
        var trend = tech.trend || {};
        if (ma.ma5 || macd.dif != null) {
            h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
            h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">📐 技术面</div>';
            // 趋势
            h += '<div style="display:flex;gap:10px;margin-bottom:10px">';
            [{l:'短期',v:trend.short},{l:'中期',v:trend.mid},{l:'长期',v:trend.long}].forEach(function(t){
                var tc = t.v === '上涨' ? '#e74c3c' : t.v === '下跌' ? '#27ae60' : '#f39c12';
                h += '<div style="flex:1;background:#fff;padding:6px;border-radius:6px;text-align:center">';
                h += '<div style="font-size:10px;color:#888">' + t.l + '</div>';
                h += '<div style="font-size:14px;font-weight:700;color:' + tc + '">' + (t.v || '-') + '</div></div>';
            });
            h += '</div>';
            // 指标
            h += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">';
            var techFields = [
                { label: 'MA5', v: ma.ma5 },
                { label: 'MA20', v: ma.ma20 },
                { label: 'MA60', v: ma.ma60 },
                { label: 'MA250', v: ma.ma250 },
                { label: 'MACD DIF', v: macd.dif },
                { label: 'MACD DEA', v: macd.dea },
                { label: 'KDJ-K', v: kdj.k },
                { label: 'KDJ-J', v: kdj.j },
                { label: 'RSI6', v: rsi.rsi6 },
                { label: 'RSI12', v: rsi.rsi12 },
                { label: 'BOLL上轨', v: boll.upper },
                { label: 'BOLL下轨', v: boll.lower }
            ];
            techFields.forEach(function(item){
                h += '<div style="text-align:center"><div style="font-size:9px;color:#888">' + item.label + '</div>';
                h += '<div style="font-size:12px;font-weight:600;color:#333">' + (item.v != null ? Number(item.v).toFixed(2) : '-') + '</div></div>';
            });
            h += '</div>';
            // 支撑压力
            if (tech.support || tech.resistance) {
                h += '<div style="border-top:1px solid #e0e0e0;padding-top:8px;margin-top:8px">';
                h += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">';
                h += '<div style="text-align:center"><div style="font-size:9px;color:#888">短期支撑</div><div style="font-size:14px;font-weight:700;color:#27ae60">' + (tech.support != null ? Number(tech.support).toFixed(2) : '-') + '</div></div>';
                h += '<div style="text-align:center"><div style="font-size:9px;color:#888">短期压力</div><div style="font-size:14px;font-weight:700;color:#e74c3c">' + (tech.resistance != null ? Number(tech.resistance).toFixed(2) : '-') + '</div></div>';
                h += '<div style="text-align:center"><div style="font-size:9px;color:#888">中期支撑</div><div style="font-size:14px;font-weight:700;color:#27ae60">' + (tech.support_mid != null ? Number(tech.support_mid).toFixed(2) : '-') + '</div></div>';
                h += '<div style="text-align:center"><div style="font-size:9px;color:#888">中期压力</div><div style="font-size:14px;font-weight:700;color:#e74c3c">' + (tech.resistance_mid != null ? Number(tech.resistance_mid).toFixed(2) : '-') + '</div></div>';
                h += '</div></div>';
            }
            if (macd.golden_cross) h += '<div style="margin-top:8px;text-align:center"><span style="background:#e74c3c;color:#fff;padding:2px 10px;border-radius:4px;font-size:11px">🔥 MACD金叉</span></div>';
            h += '</div>';
        }

        // ── 六、消息面 ──
        var notices = news.notices || [];
        var newsList = news.news || [];
        if (notices.length || newsList.length) {
            h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px;margin-bottom:16px">';
            h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">📰 消息面</div>';
            if (notices.length) {
                h += '<div style="margin-bottom:8px"><div style="font-size:11px;color:#888;margin-bottom:4px">最近公告</div>';
                notices.slice(0, 5).forEach(function(n){
                    h += '<div style="font-size:12px;padding:3px 0;border-bottom:1px solid #eee"><span style="color:#888">' + (n.notice_date || '') + '</span> ';
                    if (n.detail_url) h += '<a href="' + n.detail_url + '" target="_blank" style="color:#1a73e8;text-decoration:none">' + (n.title || '') + '</a>';
                    else h += (n.title || '');
                    h += '</div>';
                });
                h += '</div>';
            }
            if (newsList.length) {
                h += '<div><div style="font-size:11px;color:#888;margin-bottom:4px">相关快讯</div>';
                newsList.slice(0, 5).forEach(function(n){
                    if (n.title) h += '<div style="font-size:12px;padding:3px 0;border-bottom:1px solid #eee"><span style="color:#888">' + (n.publish_time || '') + '</span> ' + n.title + '</div>';
                });
                h += '</div>';
            }
            h += '</div>';
        }

        // ── 概念板块 ──
        var concepts = d.concepts || [];
        if (concepts.length) {
            h += '<div style="margin-bottom:16px"><span style="font-weight:600;font-size:14px">🏷️ 概念板块：</span>';
            h += '<span style="font-size:13px;color:#555">' + concepts.join('、') + '</span></div>';
        }

        // ── 底部操作 ──
        h += '<div style="margin-top:16px;text-align:center">';
        h += '<button onclick="openKlineModal(\'' + d.stock_code + '\',\'' + (d.short_name || '') + '\')" style="padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:13px;cursor:pointer;margin:0 4px">📈 K线走势</button>';
        h += '<button onclick="pfAddWithCode(\'' + d.stock_code + '\')" style="padding:8px 20px;border:none;border-radius:6px;background:#388e3c;color:#fff;font-size:13px;cursor:pointer;margin:0 4px">⭐ 加自选</button>';
        h += '</div>';

        body.innerHTML = h;
        loadStockDetailMainforce(d);
    }

    window.closeStockDetailModal = function () {
        document.getElementById('stockDetailModal').classList.remove('show');
    };

    function renderConceptData(container, rows, isLive, label) {
        var concept = rows.filter(function (r) { return r.plate_type === 1; });
        var industry = rows.filter(function (r) { return r.plate_type === 2; });
        var tag = isLive ? '<span style="background:#e74c3c;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:bold">🟢 盘中实时 ' + label + '</span>' : '📅 日终数据 ' + label;
        var h = '<div class="stats-bar">' + card('概念板块', concept.length, 'blue') + card('行业板块', industry.length, 'orange') + card('数据源', tag, 'red') + '</div>';
        container.innerHTML = '';
        var cd = document.createElement('div'), id = document.createElement('div');
        container.appendChild(cd); container.appendChild(id);
        window.renderTable(cd, 'concept_t_live', ['排名', '代码', '名称', '涨跌幅', '热度值', '热度标签', '成分股'], concept, function (r) {
            return '<tr><td>' + rankBadge(r.rank) + '</td><td>' + r.concept_code + '</td><td>' + conceptNameLink(r.concept_code, r.concept_name) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.hot_value, 1) + '</td><td>' + (r.hot_tag || '-') + '</td><td><button onclick="showConceptStocks(\'' + r.concept_code + '\',\'' + (r.concept_name || '') + '\')" style="padding:2px 8px;font-size:11px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer">查看</button></td></tr>';
        }, 30, h);
        window.renderTable(id, 'industry_t_live', ['排名', '代码', '名称', '涨跌幅', '热度值', '热度标签', '成分股'], industry, function (r) {
            return '<tr><td>' + rankBadge(r.rank) + '</td><td>' + r.concept_code + '</td><td>' + conceptNameLink(r.concept_code, r.concept_name, true) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.hot_value, 1) + '</td><td>' + (r.hot_tag || '-') + '</td><td><button onclick="showConceptStocks(\'' + r.concept_code + '\',\'' + (r.concept_name || '') + '\')" style="padding:2px 8px;font-size:11px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer">查看</button></td></tr>';
        }, 30);
    }

    /* ===== 概念成分股 ===== */
    window._csReqId = 0;
    window.showConceptStocks = function (code, name) {
        var d = el('datePicker').value;
        var myId = ++window._csReqId;
        var body = document.getElementById('conceptModalBody');
        document.getElementById('conceptModalTitle').textContent = '📋 ' + name + ' 成分股 | 日期: ' + d;
        body.innerHTML = '<div class="loading">加载中...</div>';
        document.getElementById('conceptModal').classList.add('show');

        apiGet('/concept-stocks?concept_code=' + code + '&trade_date=' + d + '&_=' + Date.now()).then(function (res) {
            if (myId !== window._csReqId) return;
            if (!res.data || !res.data.length) { body.innerHTML = '<div class="loading">暂无成分股数据</div>'; return; }
            renderConceptTable(body, res.data);
        }).catch(function () { if (myId === window._csReqId) body.innerHTML = '<div class="loading">加载失败</div>'; });
    };

    function renderConceptTable(container, rows) {
        var id = window._csReqId;
        var toolbar = '<div class="stats-bar"><div class="stat-card"><div class="label">成分股数量</div><div class="value blue">' + rows.length + '</div></div></div>';
        toolbar += '<div class="search-bar paged-table-toolbar"><input type="text" id="cs_s_' + id + '" placeholder="🔍 搜索..." oninput="csSearch(' + id + ')"><span id="cs_i_' + id + '"></span></div>';
        renderPagedTable(container, 'cs_' + id, toolbar, tableHeadHtml(['代码', '名称', '最新价', '涨跌幅', 'K线']), 'cs_t_' + id, 'cs_p_' + id);
        window['_cs_' + window._csReqId] = { rows: rows, page: 1, ps: 30 };
        csPage(window._csReqId);
    }

    window.csSearch = function (id) {
        var kw = (document.getElementById('cs_s_' + id) || {}).value || '';
        var data = window['_cs_' + id];
        if (!data) return;
        data.filtered = kw ? data.rows.filter(function (r) { return JSON.stringify(r).toLowerCase().indexOf(kw.toLowerCase()) > -1; }) : null;
        data.page = 1;
        csPage(id);
    };

    window.csPage = function (id) {
        var data = window['_cs_' + id];
        if (!data) return;
        var all = data.filtered || data.rows;
        var p = data.page, ps = data.ps;
        var total = all.length, tp = Math.ceil(total / ps);
        var s = (p - 1) * ps, e = Math.min(s + ps, total);
        var tbody = document.getElementById('cs_t_' + id);
        if (tbody) tbody.innerHTML = all.slice(s, e).map(function (r) {
            return '<tr><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.price, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
        }).join('');
        syncPagedHeader('cs_' + id);
        document.getElementById('cs_i_' + id).textContent = '共 ' + total + ' 条 | ' + (total > 0 ? (s + 1) + '-' + e + '/' + total : '0');
        var pg = document.getElementById('cs_p_' + id);
        if (!pg || tp <= 1) { if (pg) pg.innerHTML = ''; return; }
        var ph = '';
        for (var i = 1; i <= tp; i++) ph += '<button class="' + (i === p ? 'active' : '') + '" onclick="window._cs_' + id + '.page=' + i + ';csPage(' + id + ')">' + i + '</button>';
        pg.innerHTML = ph;
    };

    window.runScreen = function (mode) {
        // Highlight active card
        document.querySelectorAll('.screen-card').forEach(function(c) { c.style.background = ''; c.style.borderColor = 'transparent'; c.style.boxShadow = ''; });
        var activeCard = document.getElementById('scard_' + mode);
        if (activeCard) { activeCard.style.background = '#3d2e0a'; activeCard.style.borderColor = '#f0c040'; activeCard.style.boxShadow = '0 0 14px rgba(240,192,64,0.35)'; }

        var d = el('datePicker').value;
        var r = document.getElementById('screenResult');
        r.innerHTML = '<div class="loading">筛选中...</div>';
        var params = 'mode=' + mode + '&trade_date=' + d + '&top=50';
        if (mode === 'k_day' || mode === 'low_start') params += '&min_chg=3&max_chg=20&min_tor=0';
        if (mode === 'low_start') params += '&vboost=1.5&max_dist=0.05&lookback=20';
        if (mode === 'trend') params += '&min_trend=0';
        if (mode === 'trend_strong') params += '&t_days=5&slope=0.2&vr_min=0.5&vr_max=3.0&max_gain=200&nh_pct=0.90';
        if (mode === 'ladder') params += '&min_b=2&max_b=5&limit=9.5';
        if (mode === 'flow') params += '&min_flow=5000000';
        apiGet('/screen-stocks?' + params).then(function (res) {
            if (!res.data || !res.data.length) { r.innerHTML = '<div class="loading">暂无结果，可调整日期或参数</div>'; return; }
            var modeLabels = {startup:'🚀 趋势启动',macd:'📉 MACD金叉',flow:'💰 资金流入',k_day:'📊 K线筛选',trend:'📈 多头趋势',trend_strong:'🔥 强势趋势票',low_start:'🚀 低位放量',ladder:'🔗 连板股',lhb:'🏦 龙虎榜'};
            var html = '<div class="stats-bar">' + card(modeLabels[mode] || mode, res.total, 'blue') + card('日期', res.date || d, 'blue') + '</div>';
            r.innerHTML = html;
            var tableId = 's_' + mode;
            var cols, renderFn;
            if (mode === 'k_day' || mode === 'low_start' || mode === 'trend' || mode === 'trend_strong') {
                cols = ['排名','代码','名称','收盘价','涨跌幅','换手率','分时'];
                if (mode === 'low_start') cols = ['排名','代码','名称','收盘价','涨跌幅','换手率','距低点%','放量比','分时'];
                if (mode === 'trend') cols = ['排名','代码','名称','收盘价','涨跌幅','5日前价','10日前价','20日前价','偏离%','分时'];
                if (mode === 'trend_strong') cols = ['排名','代码','名称','收盘价','涨跌幅','MA5上天数','60日涨幅','距新高%','量比','MA20斜率','分时'];
                renderFn = function (r) {
                    var row = '<td>' + rankBadge(r.rank || (window['_s_' + tableId] || []).indexOf(r) + 1) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.close, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td>';
                    if (mode === 'trend_strong') {
                        var days = r.above_ma5_days || 0;
                        var daysColor = days >= 20 ? '#e74c3c' : days >= 10 ? '#f39c12' : '#666';
                        row += '<td style="color:' + daysColor + ';font-weight:700">' + days + '天</td>';
                        row += '<td class="' + clsPct(r.gain_60d) + '">' + fmt(r.gain_60d, 1) + '%</td>';
                        row += '<td>' + fmt(r.near_high_pct, 1) + '%</td>';
                        row += '<td>' + fmt(r.vol_ratio, 2) + '</td>';
                        row += '<td>' + fmt(r.ma20_slope_pct, 2) + '%</td>';
                    } else {
                        row += '<td>' + fmt(r.turnover_ratio, 1) + '%</td>';
                        if (mode === 'low_start') row += '<td>' + fmt(r.dist_from_low_pct, 1) + '%</td><td>' + fmt(r.vol_ratio, 1) + 'x</td>';
                        if (mode === 'trend') row += '<td>' + fmt(r.ma5_c, 2) + '</td><td>' + fmt(r.ma10_c, 2) + '</td><td>' + fmt(r.ma20_c, 2) + '</td><td>' + fmt(r.ma_spread_pct, 1) + '%</td>';
                    }
                    row += '<td>' + minuteBtn(r.stock_code) + '</td>';
                    return '<tr>' + row + '</tr>';
                };
            } else if (mode === 'lhb') {
                cols = ['排名','代码','名称','涨跌幅','换手率','净买入额','上榜原因','分时'];
                renderFn = function (r) {
                    return '<tr><td>' + rankBadge(r.rank || (window['_s_' + tableId] || []).indexOf(r) + 1) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.turnover_ratio, 1) + '%</td><td class="' + clsPct(r.a_net_amount) + '">' + fmtMoney(r.a_net_amount) + '</td><td style="max-width:250px;white-space:normal;font-size:11px">' + (r.reason || '-') + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
                };
            } else if (mode === 'flow') {
                cols = ['排名','代码','名称','主力净流入','最大单笔','分时'];
                renderFn = function (r) {
                    return '<tr><td>' + rankBadge(r.rank || (window['_s_' + tableId] || []).indexOf(r) + 1) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td class="' + clsPct(r.main_net_inflow) + '">' + fmtMoney(r.main_net_inflow) + '</td><td>' + fmtMoney(r.max_net_inflow) + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
                };
            } else if (mode === 'ladder') {
                cols = ['排名','代码','名称','收盘价','涨跌幅','连板数','分时'];
                renderFn = function (r) {
                    return '<tr><td>' + rankBadge(r.rank || (window['_s_' + tableId] || []).indexOf(r) + 1) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.close, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td><span style="color:#e74c3c;font-weight:bold">' + r.boards + '连板</span></td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
                };
            } else if (mode === 'macd') {
                cols = ['排名','代码','名称','收盘价','涨跌幅','DIF','DEA','MACD柱','K','D','J','分时'];
                renderFn = function (r) {
                    var histCls = (r.hist || 0) >= 0 ? 'c-red' : 'c-green';
                    var jCls = (r.j || 50) >= 80 ? 'c-red' : ((r.j || 50) <= 20 ? 'c-green' : '');
                    return '<tr><td>' + rankBadge(r.rank || (window['_s_' + tableId] || []).indexOf(r) + 1) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.close, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.dif, 2) + '</td><td>' + fmt(r.dea, 2) + '</td><td class="' + histCls + '">' + fmt(r.hist, 2) + '</td><td>' + fmt(r.k, 1) + '</td><td>' + fmt(r.d, 1) + '</td><td class="' + jCls + '" style="font-weight:700">' + fmt(r.j, 1) + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
                };
            } else if (mode === 'startup') {
                cols = ['排名','代码','名称','收盘价','涨跌幅','MA20','突破%','量比','资金','J','消息','MACD','分时'];
                renderFn = function (r) {
                    var flowStr = (r.main_net_inflow != null) ? (r.main_net_inflow >= 0 ? '+' : '') + fmtMoney(r.main_net_inflow) : '-';
                    var flowCls = (r.main_net_inflow != null && r.main_net_inflow > 0) ? 'c-red' : 'c-green';
                    var jCls = (r.j || 50) >= 80 ? 'c-red' : ((r.j || 50) <= 20 ? 'c-green' : '');
                    var newsStr = (r.news_count || 0) > 0 ? '📰' + r.news_count : '-';
                    var macdStr = r.macd_golden ? '✅金叉' : '-';
                    return '<tr><td>' + rankBadge(r.rank || (window['_s_' + tableId] || []).indexOf(r) + 1) + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.close, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td>' + fmt(r.ma20, 2) + '</td><td class="c-green">' + fmt(r.breakout_pct, 1) + '%</td><td class="c-orange">' + fmt(r.vol_ratio, 1) + 'x</td><td class="' + flowCls + '">' + flowStr + '</td><td class="' + jCls + '" style="font-weight:700">' + fmt(r.j, 1) + '</td><td style="font-size:11px">' + newsStr + '</td><td style="font-size:11px;color:#f0c040">' + macdStr + '</td><td>' + minuteBtn(r.stock_code) + '</td></tr>';
                };
            }
            window['_s_' + tableId] = res.data;
            window.renderTable(r, tableId, cols, res.data, renderFn, 30, html);
        });
    };

    window.closeConceptModal = function () {
        document.getElementById('conceptModal').classList.remove('show');
        document.getElementById('conceptModalBody').innerHTML = '';
    };
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            var klineModal = document.getElementById('klineModal');
            var alistModal = document.getElementById('alistModal');
            var conceptModal = document.getElementById('conceptModal');
            var aiModal = document.getElementById('aiModal');
            if (klineModal && klineModal.classList.contains('show')) closeKlineModal();
            else if (alistModal && alistModal.classList.contains('show')) closeAlistModal();
            else if (conceptModal && conceptModal.classList.contains('show')) closeConceptModal();
            else if (aiModal && aiModal.classList.contains('show') && window.closeAIModal) closeAIModal();
        }
    });

    /* ===== 龙虎榜详情 ===== */
    window.showAListDetail = function (date, code, name) {
        var modal = document.getElementById('alistModal');
        var body = document.getElementById('alistModalBody');
        document.getElementById('alistModalTitle').textContent = '🐲 ' + code + ' ' + (name || '') + ' 龙虎榜席位 | ' + date;
        body.innerHTML = '<div class="loading">加载中...</div>';
        modal.classList.add('show');

        apiGet('/a-list-info?trade_date=' + date + '&stock_code=' + code).then(function (res) {
            if (!res.data || !res.data.length) { body.innerHTML = '<div class="loading">暂无席位数据</div>'; return; }
            var rows = res.data;
            var summary = res.summary || {};
            var buyRows = rows.filter(function (r) { return Number(r.a_buy_amount || 0) >= Number(r.a_sell_amount || 0); })
                .sort(function (a, b) { return Number(b.a_buy_amount || 0) - Number(a.a_buy_amount || 0); });
            var sellRows = rows.filter(function (r) { return Number(r.a_sell_amount || 0) > Number(r.a_buy_amount || 0); })
                .sort(function (a, b) { return Number(b.a_sell_amount || 0) - Number(a.a_sell_amount || 0); });
            function branchTag(name, side) {
                name = String(name || '');
                var tag = '';
                if (name.indexOf('深股通') >= 0 || name.indexOf('沪股通') >= 0) tag = name.indexOf('深股通') >= 0 ? '深股通专用' : '沪股通专用';
                else if (name.indexOf('机构') >= 0) tag = '机构专用';
                if (!tag) return '';
                var color = side === 'sell' ? '#ef4444' : '#3b82f6';
                return '<div style="margin-top:8px"><span style="display:inline-block;border:1px solid ' + color + ';color:' + color + ';border-radius:5px;padding:2px 7px;font-size:12px;background:#fff">' + tag + '</span></div>';
            }
            function branchRows(list, side) {
                var top = list.slice(0, 10);
                if (!top.length) return '<div style="padding:28px;color:#999;text-align:center">暂无数据</div>';
                return top.map(function (r) {
                    var amount = side === 'sell' ? Number(r.a_sell_amount || 0) : Number(r.a_buy_amount || 0);
                    var rate = side === 'sell' ? r.a_sell_amount_rate : r.a_buy_amount_rate;
                    var amountText = fmtMoney(amount);
                    var rateText = rate != null && rate !== '' ? Number(rate).toFixed(1) + '%' : '-';
                    var nameText = r.operate_name || '-';
                    return '<div style="display:grid;grid-template-columns:minmax(0,1fr) 150px 90px;gap:16px;align-items:center;padding:16px 18px;border-bottom:1px solid #eee">' +
                        '<div style="min-width:0"><div style="font-size:16px;color:#222;white-space:normal;line-height:1.35">' + nameText + '</div>' + branchTag(nameText, side) + '</div>' +
                        '<div style="font-size:16px;font-weight:700;color:#222;text-align:right">' + amountText + '</div>' +
                        '<div style="font-size:15px;color:#666;text-align:center">' + rateText + '</div>' +
                    '</div>';
                }).join('');
            }
            var h = '';
            h += '<div style="display:grid;grid-template-columns:repeat(4,minmax(160px,1fr));gap:18px;margin-bottom:22px">';
            h += '<div style="border:1px solid #ddd;border-radius:8px;padding:18px;text-align:center"><div style="color:#888;font-size:15px;margin-bottom:10px">总买入</div><div style="color:#e53935;font-size:24px;font-weight:800">' + fmtMoney(summary.a_buy_amount) + '</div></div>';
            h += '<div style="border:1px solid #ddd;border-radius:8px;padding:18px;text-align:center"><div style="color:#888;font-size:15px;margin-bottom:10px">总卖出</div><div style="color:#43a047;font-size:24px;font-weight:800">' + fmtMoney(summary.a_sell_amount) + '</div></div>';
            h += '<div style="border:1px solid #ddd;border-radius:8px;padding:18px;text-align:center"><div style="color:#888;font-size:15px;margin-bottom:10px">净买入</div><div class="' + clsPct(summary.a_net_amount) + '" style="font-size:24px;font-weight:800">' + fmtMoney(summary.a_net_amount) + '</div></div>';
            h += '<div style="border:1px solid #ddd;border-radius:8px;padding:18px;text-align:center"><div style="color:#888;font-size:15px;margin-bottom:10px">涨跌幅</div><div class="' + clsPct(summary.change_cpt) + '" style="font-size:24px;font-weight:800">' + pct(summary.change_cpt) + '</div></div>';
            h += '</div>';
            if (summary.reason) {
                h += '<div style="margin-bottom:18px;padding:12px 16px;background:#fff7ed;border-left:4px solid #f59e0b;border-radius:6px;font-size:13px;color:#7c2d12;line-height:1.6">上榜原因：' + summary.reason + '</div>';
            }
            h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:22px;margin-bottom:20px">';
            h += '<section style="border:1px solid #eee;border-radius:8px;overflow:hidden;background:#fff"><div style="padding:16px 18px;border-bottom:1px solid #eee;font-size:18px;font-weight:800">买入营业部</div><div style="display:grid;grid-template-columns:minmax(0,1fr) 150px 90px;gap:16px;background:#fafafa;padding:14px 18px;font-size:16px;font-weight:800"><span>营业部</span><span>金额</span><span>占比</span></div>' + branchRows(buyRows, 'buy') + '</section>';
            h += '<section style="border:1px solid #eee;border-radius:8px;overflow:hidden;background:#fff"><div style="padding:16px 18px;border-bottom:1px solid #eee;font-size:18px;font-weight:800">卖出营业部</div><div style="display:grid;grid-template-columns:minmax(0,1fr) 150px 90px;gap:16px;background:#fafafa;padding:14px 18px;font-size:16px;font-weight:800"><span>营业部</span><span>金额</span><span>占比</span></div>' + branchRows(sellRows, 'sell') + '</section>';
            h += '</div>';
            h += '<div class="search-bar paged-table-toolbar"><input type="text" id="als_search" placeholder="🔍 搜索明细..." oninput="alsFilter()"><span id="als_info"></span></div>';
            h += '<div id="als_table_mount"></div>';
            body.innerHTML = h;
            renderPagedTable(document.getElementById('als_table_mount'), 'als', '', tableHeadHtml(['营业部', '类型', '净买入额', '买入额', '卖出额', '原因']), 'als_tbody', 'als_pager');
            window._als_rows = rows;
            window._als_page = 1;
            window._als_ps = 20;
            window.alsFilter = function () {
                var kw = (document.getElementById('als_search') || {}).value || '';
                window._als_filtered = kw ? rows.filter(function (r) { return JSON.stringify(r).toLowerCase().indexOf(kw.toLowerCase()) > -1; }) : rows;
                window._als_page = 1;
                alsRender();
            };
            window.alsRender = function () {
                var all = window._als_filtered || rows;
                var p = window._als_page, ps = window._als_ps;
                var total = all.length, tp = Math.ceil(total / ps);
                var s = (p - 1) * ps, e = Math.min(s + ps, total);
                var tbody = document.getElementById('als_tbody');
                if (tbody) tbody.innerHTML = all.slice(s, e).map(function (r) {
                    return '<tr><td>' + (r.operate_name || '-') + '</td><td>' + (r.operate_type || '-') + '</td><td class="' + clsPct(r.a_net_amount) + '">' + fmtMoney(r.a_net_amount) + '</td><td>' + fmtMoney(r.a_buy_amount) + '</td><td>' + fmtMoney(r.a_sell_amount) + '</td><td style="font-size:11px;max-width:300px;white-space:normal">' + (r.reason || '-') + '</td></tr>';
                }).join('');
                syncPagedHeader('als');
                document.getElementById('als_info').textContent = '共 ' + total + ' 条 | ' + (total > 0 ? (s + 1) + '-' + e + '/' + total : '0');
                var pg = document.getElementById('als_pager');
                if (!pg || tp <= 1) { if (pg) pg.innerHTML = ''; return; }
                var ph = '';
                for (var i = 1; i <= tp; i++) ph += '<button class="' + (i === p ? 'active' : '') + '" onclick="window._als_page=' + i + ';alsRender()">' + i + '</button>';
                pg.innerHTML = ph;
            };
            alsRender();
            window._als_filtered = rows;
        }).catch(function () { body.innerHTML = '<div class="loading">加载失败</div>'; });
    };
    window.closeAlistModal = function () {
        document.getElementById('alistModal').classList.remove('show');
        document.getElementById('alistModalBody').innerHTML = '';
    };
    window.hideDetail = window.closeAlistModal;

    /* ===== 资金净流入 ===== */
    window.loadCap2 = function (silent) {
        var d = el('datePicker').value;
        var s = (el('capSort2') || { value: 'desc' }).value;
        var t = (el('capTop2') || { value: '100' }).value;
        var cd = (el('capCode2') || { value: '' }).value;
        var c = el('capResult2');
        if (!c) return;
        var keepCurrent = !!silent && hasRenderedContent(c);
        if (keepCurrent) markSilentRefreshTarget(c);
        if (!keepCurrent) c.innerHTML = '<div class="loading">加载中...</div>';
        apiGet('/capital-flow?trade_date=' + d + '&sort=' + s + '&top=' + t + '&stock_code=' + encodeURIComponent(cd)).then(function (res) {
            syncDateFromResponse(res);
            if (!res.data || !res.data.length) {
                if (keepCurrent) {
                    setStatus('资金流刷新暂无新数据，已保留现有结果');
                    return;
                }
                c.innerHTML = '<div class="loading">暂无数据</div>';
                return;
            }
            var src = res.mode_label || '';
            if (res.live_error) src += ' · 实时源回落';
            var dataTime = res.data_time || res.snapshot_at || res.date || '-';
            var info = '数据时间：' + dataTime + (src ? ' · 数据模式：' + src : '') + ' · 共 ' + res.total + ' 条';
            var toolbar = '<div class="search-bar paged-table-toolbar">';
            toolbar += '<select id="capSort2"><option value="desc"' + (s === 'desc' ? ' selected' : '') + '>净流入↓</option><option value="asc"' + (s === 'asc' ? ' selected' : '') + '>净流入↑</option></select>';
            toolbar += '<select id="capTop2"><option value="100"' + (t === '100' ? ' selected' : '') + '>前100</option><option value="500"' + (t === '500' ? ' selected' : '') + '>前500</option><option value="0"' + (t === '0' ? ' selected' : '') + '>全部</option></select>';
            toolbar += '<input type="text" id="capCode2" value="' + escAttr(cd) + '" placeholder="股票代码" style="width:110px"><button onclick="loadCap2(true)">查询</button><button onclick="loadCap2(true)">刷新</button><span id="capInfo2" style="font-size:12px;color:#888">' + info + '</span></div>';
            window.renderTable(c, 'cap', ['排名', '代码', '名称', '现价', '涨跌幅', '主力净流入', '超大单', '大单', '中单', '小单'], res.data, function (r, i) {
                var rk = t > 0 ? rankBadge(i + 1) : '-';
                var hasDetail = r.max_net_inflow != 0 || r.lg_net_inflow != 0;
                var maxFmt = hasDetail ? fmtMoney(r.max_net_inflow) : '<span style="color:#ccc">-</span>';
                var lgFmt = hasDetail ? fmtMoney(r.lg_net_inflow) : '<span style="color:#ccc">-</span>';
                var midFmt = hasDetail ? fmtMoney(r.mid_net_inflow) : '<span style="color:#ccc">-</span>';
                var smFmt = hasDetail ? fmtMoney(r.sm_net_inflow) : '<span style="color:#ccc">-</span>';
                return '<tr><td>' + rk + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.price, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td class="' + clsPct(r.main_net_inflow) + '"><strong>' + fmtMoney(r.main_net_inflow) + '</strong></td><td class="' + clsPct(r.max_net_inflow) + '">' + maxFmt + '</td><td class="' + clsPct(r.lg_net_inflow) + '">' + lgFmt + '</td><td class="' + clsPct(r.mid_net_inflow) + '">' + midFmt + '</td><td class="' + clsPct(r.sm_net_inflow) + '">' + smFmt + '</td></tr>';
            }, 50, toolbar);
        }).catch(function (e) {
            if (keepCurrent) {
                setStatus('资金流刷新失败，已保留现有结果', true);
                return;
            }
            c.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 加载失败: ' + e.message + '</div>';
        });
    };

    /* ===== 实时资金 ===== */
    window.loadRT = function () {
        var code = (el('rtCode') || {}).value, r = el('rtResult');
        if (!code) { el('rtInfo').textContent = '请输入股票代码'; return; }
        r.innerHTML = '<div class="loading">获取中...</div>'; el('rtInfo').textContent = '加载中...';
        apiGet('/capital-flow-realtime?stock_code=' + code).then(function (res) {
            if (!res.latest) { r.innerHTML = '<div class="loading">无数据</div>'; return; }
            var l = res.latest;
            var flowStatusColor = res.flow_status === 'fresh' ? '#16a34a' : '#d97706';
            var h = '<div class="stats-bar">' + card('时间', l.trade_time.slice(11, 16), 'blue') + card('主力净流入', fmtMoney(l.main_net_inflow), clsPct(l.main_net_inflow)) + card('超大单', fmtMoney(l.max_net_inflow), clsPct(l.max_net_inflow)) + card('大单', fmtMoney(l.lg_net_inflow)) + card('中单', fmtMoney(l.mid_net_inflow)) + card('小单', fmtMoney(l.sm_net_inflow)) + '</div>';
            h += '<div class="section-title">分时资金流向（' + res.total + ' 条）</div><div class="table-wrap" style="max-height:500px;overflow-y:auto"><table><thead><tr><th>时间</th><th>主力净流入</th><th>超大单</th><th>大单</th><th>中单</th><th>小单</th></tr></thead><tbody>';
            h += '<tr><td colspan="6" style="font-size:12px;color:#64748b;background:#f8fafc;">来源 <strong>' + (res.source || '-') + '</strong> · 状态 <strong style="color:' + flowStatusColor + '">' + (res.flow_status || '-') + '</strong> · 延迟 ' + (res.flow_age_seconds == null ? '-' : (res.flow_age_seconds + 's')) + '</td></tr>';
            res.data.forEach(function (rr) { h += '<tr><td>' + rr.trade_time.slice(11, 16) + '</td><td class="' + clsPct(rr.main_net_inflow) + '"><strong>' + fmtMoney(rr.main_net_inflow) + '</strong></td><td class="' + clsPct(rr.max_net_inflow) + '">' + fmtMoney(rr.max_net_inflow) + '</td><td class="' + clsPct(rr.lg_net_inflow) + '">' + fmtMoney(rr.lg_net_inflow) + '</td><td class="' + clsPct(rr.mid_net_inflow) + '">' + fmtMoney(rr.mid_net_inflow) + '</td><td class="' + clsPct(rr.sm_net_inflow) + '">' + fmtMoney(rr.sm_net_inflow) + '</td></tr>'; });
            h += '</tbody></table></div>'; r.innerHTML = h; el('rtInfo').textContent = '共 ' + res.total + ' 条';
        });
    };

    /* ===== 调度管理操作 ===== */
    window.updCron = function (id, v) { fetch('/api/scheduler/tasks/' + id + '/cron?cron_time=' + v).then(function () { refreshLoadTab('scheduler'); }); };
    window.updDp = function (id, v) { fetch('/api/scheduler/tasks/' + id + '/date-param?date_param=' + encodeURIComponent(v)).then(function () { refreshLoadTab('scheduler'); }); };
    window.togT = function (id) { fetch('/api/scheduler/tasks/' + id + '/toggle', { method: 'POST' }).then(function () { refreshLoadTab('scheduler'); }); };
    window.runT = function (id) {
        if (!confirm('确认立即执行？')) return;
        fetch('/api/scheduler/tasks/' + id + '/run', { method: 'POST' }).then(function (r) { return r.json(); }).then(function (res) {
            alert('执行完成！状态: ' + res.status + '，耗时: ' + (res.duration || 0) + '秒');
            refreshLoadTab('scheduler');
        });
    };
    window.logT = function (id) {
        fetch('/api/scheduler/tasks').then(function (r) { return r.json(); }).then(function (res) {
            var t = res.data.find(function (x) { return x.id === id; });
            if (!t || !t.last_run_output) { alert('暂无日志'); return; }
            var w = window.open('', '_blank', 'width=800,height=600');
            w.document.write('<html><head><title>' + t.task_name + ' 执行日志</title><style>body{font-family:monospace;font-size:13px;background:#1e1e1e;color:#d4d4d4;padding:20px;white-space:pre-wrap;word-break:break-all}</style></head><body><div style="font-size:18px;color:#569cd6;font-weight:700;margin-bottom:16px">' + t.task_name + '</div><div>时间: ' + (t.last_run_at || '-') + '</div><div>状态: ' + t.last_run_status + '</div><div>耗时: ' + (t.last_run_duration || 0) + 's</div><hr style="border-color:#333"><pre>' + (t.last_run_output || '').replace(/</g, '&lt;') + '</pre></body></html>');
            w.document.close();
        });
    };

    /* ===== 数据源管理操作 ===== */
    window.dsSwitchTab = function (idx) {
        // 切换 tab 样式
        document.querySelectorAll('.ds-tab').forEach(function(tab) {
            var isActive = tab.getAttribute('data-provider') === String(idx);
            tab.style.borderBottom = isActive ? '2px solid #1a73e8' : '2px solid transparent';
            tab.style.color = isActive ? '#1a73e8' : '#666';
            tab.style.background = isActive ? '#fff' : '#f5f5f5';
            tab.style.fontWeight = isActive ? '600' : '400';
        });
        // 切换内容
        document.querySelectorAll('.ds-tab-panel').forEach(function(panel) {
            panel.style.display = panel.getAttribute('data-provider') === String(idx) ? 'block' : 'none';
        });
    };
    window.dsRunTask = function (id) {
        if (!confirm('确认立即执行该数据源？')) return;
        fetch('/api/datasource/' + id + '/run', { method: 'POST' }).then(function (r) { return r.json(); }).then(function (res) {
            alert('执行完成！状态: ' + res.status + '，耗时: ' + (res.duration || 0) + '秒');
            loadTab('datasource');
        });
    };
    window.dsToggleTask = function (id) {
        fetch('/api/datasource/' + id + '/toggle', { method: 'POST' }).then(function (r) { return r.json(); }).then(function (res) {
            loadTab('datasource');
        });
    };
    window.dsViewLog = function (id) {
        fetch('/api/datasource/' + id + '/log').then(function (r) { return r.json(); }).then(function (res) {
            if (!res.data || !res.data.output) { alert('暂无日志'); return; }
            var d = res.data;
            var w = window.open('', '_blank', 'width=800,height=600');
            w.document.write('<html><head><title>数据源执行日志</title><style>body{font-family:monospace;font-size:13px;background:#1e1e1e;color:#d4d4d4;padding:20px;white-space:pre-wrap;word-break:break-all}</style></head><body><div style="font-size:18px;color:#569cd6;font-weight:700;margin-bottom:16px">数据源执行日志</div><div>时间: ' + (d.run_at || '-') + '</div><div>状态: ' + (d.status || '-') + '</div><hr style="border-color:#333"><pre>' + (d.output || '').replace(/</g, '&lt;') + '</pre></body></html>');
            w.document.close();
        });
    };

    /* ===== 主力行为分析 ===== */
    function loadMainforcePage(d, c) {
        var h = '<div style="background:#fff;border-radius:10px;padding:16px 20px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
        h += '<div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center">';
        h += '<span style="font-size:15px;font-weight:700;color:#333">🔍 主力行为分析</span>';
        h += '<span style="font-size:12px;color:#888">综合 K线形态 · 量价关系 · 资金流向 · 筹码变化 · 龙虎榜</span>';
        h += '<input id="mfCodeInput" type="text" placeholder="输入股票代码分析单只股票" style="width:200px;padding:8px 12px;border:1px solid #ddd;border-radius:6px;font-size:14px;margin-left:auto">';
        h += '<button onclick="doMainforceAnalyze()" style="padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:14px;cursor:pointer;font-weight:600">🔍 分析</button>';
        h += '<button onclick="doMainforceScan()" style="padding:8px 16px;border:1px solid #ddd;border-radius:6px;background:#fff;font-size:13px;cursor:pointer;color:#666">📊 全市场扫描</button>';
        h += '</div></div>';
        h += '<div id="mainforceContent"><div class="loading">输入股票代码点击分析，或点击全市场扫描</div></div>';
        c.innerHTML = h;
        var codeInput = document.getElementById('mfCodeInput');
        if (codeInput) codeInput.addEventListener('keydown', function (e) { if (e.key === 'Enter') doMainforceAnalyze(); });
    }

    window.doMainforceAnalyze = function () {
        var code = (document.getElementById('mfCodeInput') || {}).value || '';
        code = code.trim();
        if (!code) { alert('请输入股票代码'); return; }
        var container = document.getElementById('mainforceContent');
        if (!container) return;
        container.innerHTML = '<div class="loading"><span class="spinner"></span> 分析中...</div>';
        fetchJsonWithTimeout('/mainforce-analysis?stock_code=' + encodeURIComponent(code), 6000).then(function (data) {
            if (data.error) { container.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ ' + data.error + '</div>'; return; }
            container.innerHTML = renderMainforceDetail(data);
            setTimeout(function () { initMainforceCharts(data); }, 100);
        }).catch(function (e) {
            container.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 请求失败: ' + e.message + '</div>';
        });
    };

    window.doMainforceScan = function () {
        var container = document.getElementById('mainforceContent');
        if (!container) return;
        container.innerHTML = '<div class="loading"><span class="spinner"></span> 全市场扫描中，请稍候...</div>';
        var d = el('datePicker').value;
        fetch(API_BASE + '/mainforce-scan?trade_date=' + d + '&top=50').then(function (r) { return r.json(); }).then(function (res) {
            if (!res.results || !res.results.length) {
                container.innerHTML = '<div class="loading">暂无扫描结果</div>';
                return;
            }
            var summary = res.summary || {};
            var h = '<div class="stats-bar">';
            h += card('建仓', summary['建仓'] || 0, 'red');
            h += card('洗盘', summary['洗盘'] || 0, 'orange');
            h += card('出货', summary['出货'] || 0, 'green');
            h += card('扫描日期', res.trade_date, 'blue');
            h += '</div>';
            window.renderTable(container, 'mfScan',
                ['股票代码', '名称', '现价', '涨跌幅', '主力行为', '置信度', '综合得分', '量价信号', '资金信号', 'K线信号', '筹码信号', '机构信号', '操作'],
                res.results, function (r) {
                    var sigs = r.signals || {};
                    var sigHtml = function (key) {
                        var s = sigs[key];
                        if (!s) return '<span class="c-gray">-</span>';
                        return '<span class="mf-badge mf-' + s.direction + '">' + s.score + ' ' + s.direction + '</span>';
                    };
                    return '<tr>' +
                        '<td>' + r.stock_code + '</td>' +
                        '<td>' + nameLink(r.stock_code, r.short_name) + '</td>' +
                        '<td>' + (r.price || '-') + '</td>' +
                        '<td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td>' +
                        '<td><span class="mf-badge mf-' + r.behavior + '">' + r.behavior + '</span></td>' +
                        '<td><div class="mf-confidence-bar"><div class="mf-confidence-fill" style="width:' + r.confidence + '%;background:' + (r.confidence > 70 ? '#e74c3c' : r.confidence > 50 ? '#f5a623' : '#27ae60') + '"></div></div><span style="font-size:11px;color:#888">' + r.confidence + '%</span></td>' +
                        '<td style="font-weight:700">' + r.score + '</td>' +
                        '<td>' + sigHtml('volume_price') + '</td>' +
                        '<td>' + sigHtml('capital_flow') + '</td>' +
                        '<td>' + sigHtml('kline_pattern') + '</td>' +
                        '<td>' + sigHtml('chip_concentration') + '</td>' +
                        '<td>' + sigHtml('institutional') + '</td>' +
                        '<td><button onclick="openMainforceModal(\'' + r.stock_code + '\',\'' + (r.short_name || '') + '\')" style="padding:3px 10px;border:none;border-radius:4px;background:#1a73e8;color:#fff;font-size:11px;cursor:pointer">详情</button></td>' +
                        '</tr>';
                }, 30, h);
        }).catch(function (e) {
            container.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 扫描失败: ' + e.message + '</div>';
        });
    };

    window.openMainforceModal = function (code, name) {
        return window.openMainforceDetailSafe(code, name);
    };

    window.closeMainforceModal = function () {
        var el = document.getElementById('mainforceModal');
        if (el) el.classList.remove('show');
    };

    function renderMainforceDetail(data) {
        var behavior = data.behavior || '中性';
        var confidence = data.confidence || 0;
        var score = data.score || 50;
        var signals = data.signals || {};
        var history = data.history || [];
        var evidence = data.evidence || [];

        var h = '';

        // 顶部：行为判断 + 综合得分
        h += '<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px 20px;margin-bottom:16px;color:#fff;display:flex;align-items:center;gap:20px;flex-wrap:wrap">';
        h += '<div style="text-align:center"><span class="mf-big-badge mf-big-' + behavior + '">' + behavior + '</span>';
        h += '<div style="font-size:11px;color:#aaa;margin-top:6px">主力行为判断</div></div>';
        h += '<div style="text-align:center"><div style="font-size:42px;font-weight:800;color:' + (score >= 60 ? '#e74c3c' : score <= 40 ? '#27ae60' : '#f5a623') + '">' + score + '</div><div style="font-size:11px;color:#aaa">综合得分</div></div>';
        h += '<div style="flex:1;min-width:200px">';
        h += '<div style="font-size:12px;color:#aaa;margin-bottom:4px">置信度: ' + confidence + '%</div>';
        h += '<div class="mf-confidence-bar" style="background:rgba(255,255,255,0.15)"><div class="mf-confidence-fill" style="width:' + confidence + '%;background:' + (confidence > 70 ? '#e74c3c' : confidence > 50 ? '#f5a623' : '#27ae60') + '"></div></div>';
        h += '<div style="font-size:11px;color:#666;margin-top:8px">' + data.stock_code + ' ' + (data.short_name || '') + '</div>';
        h += '</div>';
        h += '</div>';

        if (evidence.length) {
            h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">关键证据</div>';
            h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px;margin-bottom:16px">';
            evidence.slice(0, 8).forEach(function(ev) {
                var dir = ev.direction || '中性';
                var color = dir === '建仓' ? '#e74c3c' : (dir === '出货' ? '#27ae60' : '#f5a623');
                h += '<div style="background:#fff;border:1px solid #eef2f7;border-left:4px solid ' + color + ';border-radius:8px;padding:10px 12px;box-shadow:0 1px 4px rgba(15,23,42,.04)">';
                h += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:5px">';
                h += '<span style="font-size:12px;color:#475569;font-weight:800">' + (ev.kind || '-') + '</span>';
                h += '<span class="mf-badge mf-' + dir + '">' + dir + ' ' + (ev.strength || 0) + '</span>';
                h += '</div>';
                h += '<div style="font-size:12px;color:#334155;line-height:1.55">' + (ev.text || '-') + '</div>';
                h += '</div>';
            });
            h += '</div>';
        }

        // 信号维度详情
        var signalNames = {
            volume_price: '📊 量价背离',
            capital_flow: '💰 资金流向',
            kline_pattern: '📈 K线形态',
            chip_concentration: '🎯 筹码集中',
            institutional: '🏛 机构动向'
        };

        h += '<div style="font-weight:600;margin-bottom:10px;font-size:14px">五维信号分析</div>';
        h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px">';
        for (var key in signalNames) {
            var sig = signals[key] || {};
            var dir = sig.direction || '中性';
            var sc = sig.score || 50;
            h += '<div class="mf-signal-card mf-sig-' + dir + '">';
            h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">';
            h += '<span style="font-size:13px;font-weight:600">' + signalNames[key] + '</span>';
            h += '<span class="mf-badge mf-' + dir + '">' + sc + '分 ' + dir + '</span>';
            h += '</div>';
            h += '<div style="font-size:11px;color:#666;line-height:1.5">' + (sig.detail || '-') + '</div>';
            h += '</div>';
        }
        h += '</div>';

        // 图表区域
        h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">';
        h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px"><div style="font-weight:600;margin-bottom:8px;font-size:13px">信号雷达图</div><div class="mf-radar-chart-container"><canvas id="mfRadarChart"></canvas></div></div>';
        h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px"><div style="font-weight:600;margin-bottom:8px;font-size:13px">近20日得分趋势</div><div class="mf-trend-chart-container"><canvas id="mfTrendChart"></canvas></div></div>';
        h += '</div>';

        // 信号详细说明（可折叠）
        h += '<div style="background:#f8f9fa;border-radius:8px;padding:14px">';
        h += '<div style="font-weight:600;margin-bottom:10px;font-size:13px">📋 判断依据说明</div>';
        h += '<div style="font-size:12px;color:#555;line-height:1.8">';
        h += '<div><strong>建仓特征：</strong>低位横盘、小阴小阳、温和放量、主力持续净流入、筹码集中</div>';
        h += '<div><strong>洗盘特征：</strong>上涨中回调、缩量下跌、长下影线、主力小幅流出或流入</div>';
        h += '<div><strong>出货特征：</strong>高位放量滞涨、大阴线/长上影、主力持续净流出、散户净流入</div>';
        h += '</div></div>';

        return h;
    }

    function initMainforceCharts(data) {
        var signals = data.signals || {};
        var history = data.history || [];

        // 雷达图
        var radarCanvas = document.getElementById('mfRadarChart');
        if (radarCanvas && typeof Chart !== 'undefined') {
            var labels = ['量价背离', '资金流向', 'K线形态', '筹码集中', '机构动向'];
            var scores = [
                (signals.volume_price || {}).score || 50,
                (signals.capital_flow || {}).score || 50,
                (signals.kline_pattern || {}).score || 50,
                (signals.chip_concentration || {}).score || 50,
                (signals.institutional || {}).score || 50
            ];
            new Chart(radarCanvas.getContext('2d'), {
                type: 'radar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: '信号得分',
                        data: scores,
                        backgroundColor: 'rgba(26,115,232,0.15)',
                        borderColor: '#1a73e8',
                        borderWidth: 2,
                        pointBackgroundColor: '#1a73e8',
                        pointRadius: 4
                    }, {
                        label: '中性线(50)',
                        data: [50, 50, 50, 50, 50],
                        backgroundColor: 'transparent',
                        borderColor: 'rgba(0,0,0,0.1)',
                        borderWidth: 1,
                        borderDash: [4, 4],
                        pointRadius: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        r: {
                            beginAtZero: true,
                            max: 100,
                            ticks: { stepSize: 20, font: { size: 9 }, color: '#999', backdropColor: 'transparent' },
                            grid: { color: 'rgba(0,0,0,0.06)' },
                            pointLabels: { font: { size: 11 }, color: '#555' }
                        }
                    },
                    plugins: {
                        legend: { display: false }
                    }
                }
            });
        }

        // 趋势图
        var trendCanvas = document.getElementById('mfTrendChart');
        if (trendCanvas && typeof Chart !== 'undefined' && history.length > 0) {
            var dates = history.map(function (h) { return h.date; });
            var trendScores = history.map(function (h) { return h.score; });
            var bgColors = trendScores.map(function (s) {
                return s >= 60 ? 'rgba(231,76,60,0.7)' : s <= 40 ? 'rgba(39,174,96,0.7)' : 'rgba(245,166,35,0.7)';
            });
            new Chart(trendCanvas.getContext('2d'), {
                type: 'bar',
                data: {
                    labels: dates,
                    datasets: [{
                        label: '综合得分',
                        data: trendScores,
                        backgroundColor: bgColors,
                        borderRadius: 3,
                        barThickness: 12
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { ticks: { color: '#888', font: { size: 9 }, maxRotation: 45 }, grid: { display: false } },
                        y: { min: 0, max: 100, ticks: { color: '#888', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } }
                    },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                afterLabel: function (ctx) {
                                    var idx = ctx.dataIndex;
                                    return history[idx] ? '判断: ' + history[idx].behavior : '';
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    /* ===== 路由 ===== */
    function loadTab(tabId, options) {
        options = options || {};
        if (!options.silent) setActiveTab(tabId);
        var d = el('datePicker').value, c = el('tab-' + tabId);
        if (!c) { c = el(tabId); } if (!c) return;
        var keepCurrent = options.silent && hasRenderedContent(c);
        if (keepCurrent) markSilentRefreshTarget(c);
        if (!keepCurrent) {
            c.innerHTML = '<div class="loading">加载中...</div>';
        }
        var loader = LOADERS[tabId];
        try {
            if (loader) {
                if (keepCurrent) {
                    runWithSilentRefresh(function () { loader(d, c); });
                } else {
                    loader(d, c);
                }
            } else {
                c.innerHTML = '<div class="loading">未知页面: ' + tabId + '</div>';
            }
        } catch (e) {
            console.error('[loadTab error]', tabId, e);
            if (!options.silent || !hasRenderedContent(c)) {
                c.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 加载失败: ' + e.message + '</div>';
            }
            setStatus('加载失败', true);
        }
    }

    function refreshAll() {
        setStatus('后台刷新中...');
        var a = document.querySelector('.sidebar-item.active');
        if (a) { var id = a.getAttribute('data-tab'); if (id && LOADERS[id]) refreshLoadTab(id); }
        setStatus('已触发后台刷新');
    }
    window.refreshAll = refreshAll;
    el('datePicker').addEventListener('change', refreshAll);

    /* ===== 板块轮动分析 ===== */
    function loadSectorRotationPage(d, c) {
        c.innerHTML = '<div class="loading"><span class="spinner"></span> 分析板块轮动中...</div>';
        fetchJsonWithTimeout('/sector-rotation?trade_date=' + encodeURIComponent(d) + '&days=10', 30000).then(function (data) {
            if (data.error) { c.innerHTML = '<div class="loading">' + data.error + '</div>'; return; }
            renderSectorRotation(c, data);
        }).catch(function (e) {
            c.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 加载失败: ' + e.message + '</div>';
        });
    }

    function renderSectorRotation(container, data) {
        var h = '';
        var advice = data.advice || [];
        var risingS = data.rising_sectors || [];
        var fallingS = data.falling_sectors || [];
        var risingC = data.rising_concepts || [];
        var flowIn = data.flow_in_top || [];
        var flowOut = data.flow_out_top || [];
        var groups = data.group_momentum || [];
        var rotationSignal = data.rotation_signal || {};

        // ── 顶部统计 ──
        h += '<div class="stats-bar">';
        h += card('回看天数', data.lookback_days + '天', 'blue');
        h += card('崛起行业', risingS.length, 'red');
        h += card('退潮行业', fallingS.length, 'green');
        h += card('崛起概念', risingC.length, 'orange');
        if (data.data_source) h += card('数据源', data.data_source === 'east' ? '东财成交额' : 'THS搜索热度', data.data_source === 'east' ? '#4caf50' : '#ff9800');
        h += '</div>';

        if (rotationSignal.summary) {
            var sigColor = rotationSignal.status === 'switching' ? '#f59e0b' : rotationSignal.status === 'inflow' ? '#e74c3c' : rotationSignal.status === 'outflow' ? '#27ae60' : '#64748b';
            var sigLabel = rotationSignal.status === 'switching' ? '板块切换' : rotationSignal.status === 'inflow' ? '资金进攻' : rotationSignal.status === 'outflow' ? '资金撤退' : '轮动均衡';
            var toText = (rotationSignal.to_sectors || []).slice(0, 3).map(function(s){ return s.name; }).join('、') || '-';
            var fromText = (rotationSignal.from_sectors || []).slice(0, 3).map(function(s){ return s.name; }).join('、') || '-';
            var inText = (rotationSignal.fund_in_sectors || []).slice(0, 3).map(function(s){ return s.name + ' ' + fmtMoney(s.main_net_inflow); }).join(' / ') || '-';
            var outText = (rotationSignal.fund_out_sectors || []).slice(0, 3).map(function(s){ return s.name + ' ' + fmtMoney(s.main_net_inflow); }).join(' / ') || '-';
            h += '<div style="background:#fff;border-radius:10px;padding:16px 18px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05);border-left:4px solid ' + sigColor + '">';
            h += '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px">';
            h += '<span style="background:' + sigColor + ';color:#fff;padding:3px 10px;border-radius:5px;font-size:12px;font-weight:800">' + sigLabel + '</span>';
            h += '<span style="font-size:15px;font-weight:800;color:#111827">' + rotationSignal.summary + '</span>';
            h += '<span style="margin-left:auto;font-size:11px;color:#888">资金快照 ' + (rotationSignal.flow_snapshot_at || data.flow_snapshot_at || '-') + '</span>';
            h += '</div>';
            h += '<div style="display:grid;grid-template-columns:repeat(4,minmax(160px,1fr));gap:10px;font-size:12px;color:#475569">';
            h += '<div><strong style="color:#111827">可关注：</strong>' + toText + '</div>';
            h += '<div><strong style="color:#111827">需降仓：</strong>' + fromText + '</div>';
            h += '<div><strong style="color:#e74c3c">资金进：</strong>' + inText + '</div>';
            h += '<div><strong style="color:#27ae60">资金出：</strong>' + outText + '</div>';
            h += '</div>';
            h += '<div style="margin-top:10px;font-size:13px;color:#334155;line-height:1.5">' + (rotationSignal.action || '') + '</div>';
            h += '</div>';
        }

        // ── 调仓建议（核心区域） ──
        if (advice.length > 0) {
            h += '<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px 20px;margin-bottom:16px;color:#fff">';
            h += '<div style="font-size:15px;font-weight:700;margin-bottom:12px">💡 调仓换股建议</div>';
            advice.forEach(function (a) {
                var icon = a.type === 'add' ? '🔴' : a.type === 'reduce' ? '🟢' : '⚪';
                var color = a.type === 'add' ? '#e74c3c' : a.type === 'reduce' ? '#27ae60' : '#f5a623';
                var tag = a.type === 'add' ? '可加仓' : a.type === 'reduce' ? '建议减仓' : '观望';
                h += '<div style="display:flex;align-items:flex-start;gap:10px;margin-bottom:10px;padding:10px 14px;background:rgba(255,255,255,0.06);border-radius:8px;border-left:3px solid ' + color + '">';
                h += '<span style="font-size:18px">' + icon + '</span>';
                h += '<div style="flex:1">';
                h += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">';
                h += '<span style="font-weight:700;font-size:14px">' + a.group + '</span>';
                h += '<span style="background:' + color + ';color:#fff;padding:1px 8px;border-radius:4px;font-size:11px;font-weight:600">' + tag + '</span>';
                h += '<span style="font-size:11px;color:#888">动量 ' + a.momentum + '</span>';
                h += '</div>';
                h += '<div style="font-size:13px;color:#ccc;line-height:1.5">' + a.text + '</div>';
                h += '</div></div>';
            });
            h += '</div>';
        }

        // ── 宏观板块动量对比 ──
        if (groups.length > 0) {
            h += '<div style="background:#fff;border-radius:10px;padding:16px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
            h += '<div style="font-weight:700;font-size:14px;margin-bottom:12px;color:#333">📊 宏观板块动量对比</div>';
            h += '<div style="display:grid;grid-template-columns:repeat(' + Math.min(groups.length, 5) + ',1fr);gap:10px">';
            groups.forEach(function (g) {
                var barColor = g.avg_momentum > 5 ? '#e74c3c' : g.avg_momentum < -5 ? '#27ae60' : '#f5a623';
                var arrow = g.avg_momentum > 5 ? ' ↑' : g.avg_momentum < -5 ? ' ↓' : ' →';
                h += '<div style="background:#f8f9fa;border-radius:8px;padding:12px;text-align:center;border-top:3px solid ' + barColor + '">';
                h += '<div style="font-size:13px;font-weight:700;color:#333">' + g.group + '</div>';
                h += '<div style="font-size:24px;font-weight:800;color:' + barColor + ';margin:6px 0">' + (g.avg_momentum > 0 ? '+' : '') + g.avg_momentum + arrow + '</div>';
                if (g.rising_sectors.length > 0) h += '<div style="font-size:10px;color:#e74c3c">↑ ' + g.rising_sectors.slice(0, 2).join(' ') + '</div>';
                if (g.falling_sectors.length > 0) h += '<div style="font-size:10px;color:#27ae60">↓ ' + g.falling_sectors.slice(0, 2).join(' ') + '</div>';
                h += '</div>';
            });
            h += '</div></div>';
        }

        // ── 双列布局：崛起 vs 退潮 ──
        h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">';

        // 崛起行业
        h += '<div style="background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
        h += '<div style="font-weight:700;font-size:14px;margin-bottom:10px;color:#e74c3c">🔴 崛起行业 TOP</div>';
        if (risingS.length === 0) h += '<div style="color:#888;font-size:12px">暂无明显崛起行业</div>';
        risingS.forEach(function (s, i) {
            var flowInfo = flowIn.find(function (f) { return f.name === s.name; });
            h += '<div style="display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid #f0f0f0">';
            h += '<span style="min-width:20px;font-size:12px;font-weight:700;color:#e74c3c">' + (i + 1) + '</span>';
            h += '<span style="flex:1;font-size:13px;font-weight:600;color:#333">' + s.name + '</span>';
            h += '<span style="font-size:12px;color:#e74c3c;font-weight:700">动量 +' + s.momentum + '</span>';
            h += '<span style="font-size:11px;color:#888">排名 ' + s.avg_rank + '</span>';
            if (flowInfo) h += '<span style="font-size:11px;color:#e74c3c">资金+' + fmtMoney(flowInfo.main_net_inflow) + '</span>';
            h += '</div>';
        });
        h += '</div>';

        // 退潮行业
        h += '<div style="background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
        h += '<div style="font-weight:700;font-size:14px;margin-bottom:10px;color:#27ae60">🟢 退潮行业 TOP</div>';
        if (fallingS.length === 0) h += '<div style="color:#888;font-size:12px">暂无明显退潮行业</div>';
        fallingS.forEach(function (s, i) {
            var flowInfo = flowOut.find(function (f) { return f.name === s.name; });
            h += '<div style="display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid #f0f0f0">';
            h += '<span style="min-width:20px;font-size:12px;font-weight:700;color:#27ae60">' + (i + 1) + '</span>';
            h += '<span style="flex:1;font-size:13px;font-weight:600;color:#333">' + s.name + '</span>';
            h += '<span style="font-size:12px;color:#27ae60;font-weight:700">动量 ' + s.momentum + '</span>';
            h += '<span style="font-size:11px;color:#888">排名 ' + s.avg_rank + '</span>';
            if (flowInfo) h += '<span style="font-size:11px;color:#27ae60">资金' + fmtMoney(flowInfo.main_net_inflow) + '</span>';
            h += '</div>';
        });
        h += '</div>';
        h += '</div>';

        // ── 崛起概念板块 ──
        if (risingC.length > 0) {
            h += '<div style="background:#fff;border-radius:10px;padding:16px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
            h += '<div style="font-weight:700;font-size:14px;margin-bottom:10px;color:#e67e22">🔥 崛起概念板块 TOP</div>';
            h += '<div style="display:flex;gap:8px;flex-wrap:wrap">';
            risingC.forEach(function (c) {
                h += '<div style="background:#fff3e0;border:1px solid #ffe0b2;border-radius:6px;padding:6px 12px">';
                h += '<span style="font-size:13px;font-weight:600;color:#e65100">' + c.name + '</span>';
                h += '<span style="font-size:11px;color:#e67e22;margin-left:6px">动量 +' + c.momentum + '</span>';
                h += '</div>';
            });
            h += '</div></div>';
        }

        // ── 资金流入/流出 TOP ──
        if (flowIn.length > 0 || flowOut.length > 0) {
            h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">';

            // 资金流入
            h += '<div style="background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
            h += '<div style="font-weight:700;font-size:14px;margin-bottom:10px;color:#e74c3c">💰 板块资金净流入 TOP</div>';
            flowIn.slice(0, 8).forEach(function (f, i) {
                h += '<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #f0f0f0">';
                h += '<span style="min-width:20px;font-size:12px;font-weight:700;color:#e74c3c">' + (i + 1) + '</span>';
                h += '<span style="flex:1;font-size:13px;color:#333">' + f.name + '</span>';
                h += '<span style="font-size:12px;font-weight:700;color:#e74c3c">+' + fmtMoney(f.main_net_inflow) + '</span>';
                if (f.leader_stock) h += '<span style="font-size:10px;color:#888">龙头:' + f.leader_stock + '</span>';
                h += '</div>';
            });
            h += '</div>';

            // 资金流出
            h += '<div style="background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
            h += '<div style="font-weight:700;font-size:14px;margin-bottom:10px;color:#27ae60">💸 板块资金净流出 TOP</div>';
            flowOut.slice(0, 8).forEach(function (f, i) {
                h += '<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #f0f0f0">';
                h += '<span style="min-width:20px;font-size:12px;font-weight:700;color:#27ae60">' + (i + 1) + '</span>';
                h += '<span style="flex:1;font-size:13px;color:#333">' + f.name + '</span>';
                h += '<span style="font-size:12px;font-weight:700;color:#27ae60">' + fmtMoney(f.main_net_inflow) + '</span>';
                if (f.leader_stock) h += '<span style="font-size:10px;color:#888">龙头:' + f.leader_stock + '</span>';
                h += '</div>';
            });
            h += '</div>';
            h += '</div>';
        }

        // ── 行业动量排行图表 ──
        h += '<div style="background:#fff;border-radius:10px;padding:16px;margin-bottom:16px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
        h += '<div style="font-weight:700;font-size:14px;margin-bottom:12px;color:#333">📈 行业动量排行</div>';
        h += '<div class="mf-trend-chart-container" style="height:' + Math.max(200, (data.industry_trends || []).length * 28) + 'px"><canvas id="sectorRotationChart"></canvas></div>';
        h += '</div>';

        container.innerHTML = h;

        // 渲染图表
        setTimeout(function () {
            var trends = data.industry_trends || [];
            if (trends.length > 0 && typeof Chart !== 'undefined') {
                var canvas = document.getElementById('sectorRotationChart');
                if (!canvas) return;
                var labels = trends.map(function (t) { return t.name; });
                var values = trends.map(function (t) { return t.momentum; });
                var colors = values.map(function (v) { return v > 0 ? 'rgba(231,76,60,0.7)' : 'rgba(39,174,96,0.7)'; });
                new Chart(canvas.getContext('2d'), {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: '动量',
                            data: values,
                            backgroundColor: colors,
                            borderRadius: 3,
                            barThickness: 16
                        }]
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: { ticks: { color: '#888', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } },
                            y: { ticks: { color: '#555', font: { size: 11 } }, grid: { display: false } }
                        },
                        plugins: { legend: { display: false } }
                    }
                });
            }
        }, 100);
    }

    /* ===== 板块热度矩阵 ===== */
    function loadSectorHeatPage(d, container) {
        container.innerHTML = '<div class="loading"><span class="spinner"></span> 加载板块热度矩阵...</div>';
        fetchJsonWithTimeout('/sector-heat-matrix?end_date=' + encodeURIComponent(d) + '&days=26', 45000).then(function (res) {
            if (!res.data || !res.data.length) {
                container.innerHTML = '<div class="loading" style="padding:20px"><p>当前日期暂无板块热度数据</p><p style="font-size:12px;color:#888;margin-top:8px">可从东财同步最新行业/概念热度；若是非交易日，系统会自动回退最近有数据日期。</p><button onclick="syncSectorHeatBtn(\'' + d + '\')" style="margin-top:10px;padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:14px">🔄 同步东财板块热度</button></div>';
                return;
            }
            renderSectorHeatMatrix(container, res);
        }).catch(function (err) {
            container.innerHTML = '<div class="loading" style="color:#e74c3c">加载失败: ' + err.message + '</div>';
        });
    }

    function renderSectorHeatMatrix(container, res) {
        var groups = res.groups || {};
        var rows = res.data || [];
        window._SH_DAILY_TOTALS = res.daily_totals || {};
        window._SH_EAST_TREE = res.east_tree || {};
        window._SH_RAW_DATA = res.raw_data || {};
        var allDates = [];
        rows.forEach(function (r) { if (r.date) allDates.push(r.date); });
        var dateRange = allDates.length > 0 ? allDates[0] + ' ~ ' + allDates[allDates.length - 1] : '';

        var tabGroups = ['东财一级行业', '同花顺行业TOP20', '同花顺概念板块TOP100'];
        var savedTab = el('sh-tab-active') ? el('sh-tab-active').value : '';
        var activeTab = groups[savedTab] ? savedTab : (groups['东财一级行业'] ? '东财一级行业' : (groups['同花顺行业TOP20'] ? '同花顺行业TOP20' : '同花顺概念板块TOP100'));

        var html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
        html += '<span style="font-size:16px;font-weight:700;color:#e0e0e0">🌡 东财板块热度矩阵 | ' + dateRange + '</span>';
        html += '<div style="display:flex;gap:8px;align-items:center">';
        html += '<span style="font-size:11px;color:#888">冷</span>';
        html += '<div style="width:80px;height:12px;border-radius:6px;background:linear-gradient(90deg,rgba(60,180,55,0.7),rgba(200,220,40,0.75),rgba(250,130,30,0.8),rgba(240,25,25,0.9))"></div>';
        html += '<span style="font-size:11px;color:#888">热</span>';
        html += '</div></div>';

        html += '<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center">';
        html += '<button onclick="syncSectorHeatBtn(\'' + (res.date || '') + '\')" style="padding:6px 14px;border:none;border-radius:6px;background:#27ae60;color:#fff;cursor:pointer;font-size:12px;font-weight:600">🔄 刷新东财板块</button>';
        if (res.fallback) {
            html += '<span style="font-size:12px;color:#f59e0b;font-weight:700">请求 ' + (res.requested_date || '-') + ' 无数据，已回退 ' + (res.date || '-') + '</span>';
        }
        (res.warnings || []).forEach(function(w) {
            html += '<span style="font-size:12px;color:#ef4444;font-weight:700">' + w + '</span>';
        });
        tabGroups.forEach(function (tg) {
            var cnt = groups[tg] ? groups[tg].length : 0;
            var active = (tg === activeTab) ? 'background:#1a73e8;color:#fff' : 'background:#2a2a2e;color:#aaa';
            html += '<button class="sh-tab-btn" data-sh-tab="' + tg + '" style="padding:6px 14px;border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;' + active + '" onclick="switchShTab(\'' + tg + '\',\'' + (res.date || '') + '\')">' + tg + ' (' + cnt + ')</button>';
        });
        html += '<input type="hidden" id="sh-tab-active" value="' + activeTab + '">';
        html += '</div>';

        html += '<div id="sh-tab-content">';
        html += '</div>';

        container.innerHTML = html;
        renderShTab(activeTab, rows, groups);
    }

    function renderShTab(tabName, rows, groups) {
        var content = document.getElementById('sh-tab-content');
        if (!content) return;
        var tabRows = rows;
        var groupNames = groups[tabName] || [];
        if (!groupNames.length) {
            content.innerHTML = '<div class="loading" style="padding:20px">暂无数据</div>';
            return;
        }

        var eastTree = window._SH_EAST_TREE || {};
        var dailyTotals = window._SH_DAILY_TOTALS || {};
        var rawData = window._SH_RAW_DATA || {};

        var dataKey = tabName === '同花顺行业TOP20' ? 'ths_industry' : (tabName === '东财一级行业' ? 'east_industry' : 'ths_concept');
        var subKey = tabName === '东财一级行业' ? 'east_industry_sub' : null;

        var html = '<div class="sector-heat-wrap"><table class="sector-heat-table"><thead>';

        if (tabName === '东财一级行业' && Object.keys(eastTree).length > 0) {
            html += '<tr class="sh-cat-row"><th class="sh-row-header sh-date-header">日期</th><th class="sh-cat-header"></th>';
            groupNames.forEach(function (l1) {
                var children = eastTree[l1] || [];
                var colspan = 1 + children.length;
                html += '<th class="sh-cat-header" colspan="' + colspan + '">' + l1 + '</th>';
            });
            html += '</tr>';

            html += '<tr class="sh-name-row"><th class="sh-row-header sh-date-header"></th><th class="sh-col-name" style="color:#f0c040">均值</th>';
            groupNames.forEach(function (l1) {
                html += '<th class="sh-col-name sh-l1-name" style="font-weight:700">' + l1 + '</th>';
                (eastTree[l1] || []).forEach(function (l2) {
                    html += '<th class="sh-col-name sh-l2-name" style="font-size:10px;color:#999">' + l2 + '</th>';
                });
            });
            html += '</tr>';
        } else {
            html += '<tr class="sh-name-row"><th class="sh-row-header sh-date-header">日期</th><th class="sh-col-name" style="color:#f0c040">均值</th>';
            groupNames.forEach(function (name) {
                html += '<th class="sh-col-name">' + name + '</th>';
            });
            html += '</tr>';
        }

        html += '</thead><tbody>';

        tabRows.forEach(function (row, ri) {
            var d = row.date || '';
            var parts = d.split('-');
            var mmdd = (parts[1] || '') + '/' + (parts[2] || '');
            var cls = (ri === 0) ? ' sh-row-today' : '';

            var totals = dailyTotals[d] || {};
            var t = totals[dataKey];
            var st = subKey ? totals[subKey] : 0;
            var grand = Math.round((t || 0) + (st || 0));

            html += '<tr class="sh-data-row' + cls + '">';
            html += '<td class="sh-row-header sh-date-cell">' + mmdd + '</td>';
            html += '<td class="sh-cell sh-total-cell" style="font-weight:700;color:#e6a800" title="' + d + ' 平均热度: ' + grand + '">' + grand + '</td>';

            var groupData = row[dataKey] || {};
            var rawGroupData = rawData[d] && rawData[d][dataKey] ? rawData[d][dataKey] : {};

            if (tabName === '东财一级行业' && Object.keys(eastTree).length > 0) {
                groupNames.forEach(function (l1) {
                    var children = eastTree[l1] || [];
                    var hasV = Object.prototype.hasOwnProperty.call(groupData, l1);
                    var v = hasV ? Number(groupData[l1]) : null;
                    var rawV = rawGroupData[l1];
                    var bg = getHeatColor(v, 0, 100);
                    var tc = getTextColor(v, 0, 100);
                    var title = hasV ? (d + ' | ' + l1 + ' 热度: ' + v + ' | 成交额: ' + fmtMoney(rawV)) : (d + ' | ' + l1 + ': 无东财值');
                    html += '<td class="sh-cell" style="background:' + bg + ';color:' + tc + ';font-weight:700" title="' + title + '">' + (hasV ? Math.round(v) : '-') + '</td>';
                    children.forEach(function (l2) {
                        var subData = row['east_industry_sub'] || {};
                        var rawSubData = rawData[d] && rawData[d]['east_industry_sub'] ? rawData[d]['east_industry_sub'] : {};
                        var hasV2 = Object.prototype.hasOwnProperty.call(subData, l2);
                        var v2 = hasV2 ? Number(subData[l2]) : null;
                        var rawV2 = rawSubData[l2];
                        var bg2 = getHeatColor(v2, 0, 100);
                        var tc2 = getTextColor(v2, 0, 100);
                        var title2 = hasV2 ? (d + ' | ' + l2 + ' 热度: ' + v2 + ' | 成交额: ' + fmtMoney(rawV2)) : (d + ' | ' + l2 + ': 无东财值');
                        html += '<td class="sh-cell" style="background:' + bg2 + ';color:' + tc2 + ';font-size:11px" title="' + title2 + '">' + (hasV2 ? Math.round(v2) : '-') + '</td>';
                    });
                });
            } else {
                groupNames.forEach(function (name) {
                    var hasVal = Object.prototype.hasOwnProperty.call(groupData, name);
                    var v = hasVal ? Number(groupData[name]) : null;
                    var bg = getHeatColor(v, 0, 100);
                    var tc = getTextColor(v, 0, 100);
                    var rawVal = rawGroupData[name];
                    var tip = hasVal ? (d + ' | ' + name + ' 热度: ' + v + (tabName.indexOf('东财') === 0 ? ' | 成交额: ' + fmtMoney(rawVal) : '')) : (d + ' | ' + name + ': 无值');
                    html += '<td class="sh-cell" style="background:' + bg + ';color:' + tc + '" title="' + tip + '">' + (hasVal ? Math.round(v) : '-') + '</td>';
                });
            }

            html += '</tr>';
        });

        html += '</tbody></table></div>';

        html += '<div style="margin-top:12px;color:#888;font-size:11px;display:flex;gap:20px;flex-wrap:wrap">';
        html += '<span>📊 数值越大热度越高，颜色越红</span>';
        html += '<span>📈 各组独立归一化(log压缩+全局min-max)，组内跨日可比</span>';
        if (tabName === '东财一级行业') html += '<span>📂 加粗为一级行业，子行业在右侧</span>';
        html += '</div>';

        content.innerHTML = html;
    }

    window.switchShTab = function (tabName, dateStr) {
        document.querySelectorAll('.sh-tab-btn').forEach(function (b) {
            var isActive = b.getAttribute('data-sh-tab') === tabName;
            b.style.background = isActive ? '#1a73e8' : '#2a2a2e';
            b.style.color = isActive ? '#fff' : '#aaa';
        });
        var hid = document.getElementById('sh-tab-active');
        if (hid) hid.value = tabName;
        fetchJsonWithTimeout('/sector-heat-matrix?end_date=' + encodeURIComponent(dateStr) + '&days=26', 45000).then(function (res) {
            window._SH_DAILY_TOTALS = res.daily_totals || {};
            window._SH_EAST_TREE = res.east_tree || {};
            window._SH_RAW_DATA = res.raw_data || {};
            renderShTab(tabName, res.data || [], res.groups || {});
        });
    };

    function renderHeatRowLabel(name, level) {
        return level > 0 ? '<span style="padding-left:' + (level * 12) + 'px;font-size:11px;color:#999">└ ' + name + '</span>' : '<strong>' + name + '</strong>';
    }

    window.syncSectorHeatBtn = function (d) {
        var btn = event ? event.target : null;
        if (btn) { btn.disabled = true; btn.textContent = '⏳ 同步中...'; }
        fetch(API_BASE + '/sector-heat-matrix/sync-today?date=' + d, { method: 'POST' })
            .then(function (r) { return r.json(); })
            .then(function (res) {
                var countText = res.synced != null ? res.synced + ' 条数据已入库' : '已完成';
                alert(res.status === 'success' ? '✅ 东财同步成功！' + countText : '❌ 东财同步失败: ' + (res.error || res.status));
                switchSubView('sector', 'heat');
            })
            .catch(function () {
                alert('❌ 同步请求失败，请检查服务器');
            });
    };

    function getHeatColor(val, minVal, maxVal) {
        if (val == null || val === 0) return 'transparent';
        var t = (val - minVal) / (maxVal - minVal || 1);
        t = Math.max(0, Math.min(1, t));
        var r, g, b;
        if (t < 0.5) {
            var s = t * 2;
            r = Math.round(46 + s * 200);
            g = Math.round(180 + s * 25);
            b = Math.round(70 - s * 30);
        } else {
            var s = (t - 0.5) * 2;
            r = Math.round(246 - s * 10);
            g = Math.round(205 - s * 175);
            b = Math.round(40 - s * 15);
        }
        var a = 0.55 + t * 0.4;
        return 'rgba(' + r + ',' + g + ',' + b + ',' + a.toFixed(2) + ')';
    }

    function getTextColor(val, minVal, maxVal) {
        if (val == null || val === 0) return '#999';
        var ratio = (val - minVal) / (maxVal - minVal || 1);
        ratio = Math.max(0, Math.min(1, ratio));
        if (ratio > 0.55) return '#fff';
        return '#222';
    }

    /* ===== 板块异动 ===== */
    var sectorMoveTimer = null;
    var sectorMoveData = null;
    var sectorMoveFilter = 'all';
    var sectorMoveGroupBy = 'industry';
    var sectorMoveRequestSeq = 0;

    function stopSectorMovementRefresh() {
        if (sectorMoveTimer) {
            clearInterval(sectorMoveTimer);
            sectorMoveTimer = null;
        }
        sectorMoveRequestSeq += 1;
    }
    window.stopSectorMovementRefresh = stopSectorMovementRefresh;

    function loadSectorMovementPage(container) {
        stopSectorMovementRefresh();
        var requestSeq = sectorMoveRequestSeq;
        var url = '/api/sector/movement?group_by=all';
        container.innerHTML = '<div class="loading">加载板块异动数据...</div>';
        fetchRawJsonWithTimeout(url, 20000)
            .then(function(data) {
                if (requestSeq !== sectorMoveRequestSeq) return;
                if (data.error) {
                    container.innerHTML = '<div class="loading" style="color:#e74c3c">❌ ' + data.error + '</div>';
                    return;
                }
                sectorMoveData = data;
                sectorMoveFilter = 'all';
                sectorMoveGroupBy = 'industry';
                renderSectorMovementPage(container, data);
                if (sectorMoveTimer) clearInterval(sectorMoveTimer);
                sectorMoveTimer = setInterval(function() {
                    fetchRawJsonWithTimeout(url, 20000).then(function(d) {
                        if (requestSeq !== sectorMoveRequestSeq) return;
                        if (!d.error) { sectorMoveData = d; renderSectorMovementPage(container, d); }
                    }).catch(function () {
                        var rsEl = document.getElementById('smRefreshStatus');
                        if (rsEl) rsEl.textContent = '自动刷新失败: ' + new Date().toLocaleTimeString();
                    });
                }, 30000);
            })
            .catch(function(err) {
                if (requestSeq !== sectorMoveRequestSeq) return;
                container.innerHTML = '<div class="loading" style="color:#e74c3c">❌ 加载失败: ' + err.message + '</div>';
            });
    }

    function renderSectorMovementPage(container, data) {
        // 根据当前分组模式选择数据
        var sectors;
        if (sectorMoveGroupBy === 'industry' && data.industry_sectors) {
            sectors = data.industry_sectors || [];
        } else if (sectorMoveGroupBy === 'concept' && data.concept_sectors) {
            sectors = data.concept_sectors || [];
        } else {
            sectors = data.sectors || [];
        }
        var surgeSectors = sectors.filter(function(s) { return s.avg_change > 0.5; });
        var plungeSectors = sectors.filter(function(s) { return s.avg_change < -0.5; });
        var filtered = sectors;
        if (sectorMoveFilter === 'surge') filtered = surgeSectors;
        else if (sectorMoveFilter === 'plunge') filtered = plungeSectors;

        var h = '';
        h += '<div style="background:#f5f5f5;border:1px solid #e8e8e8;border-radius:16px;padding:20px 30px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center">';
        h += '<div>';
        h += '<div style="font-size:20px;font-weight:700;color:#222">🌊 板块异动监控</div>';
        h += '<div style="font-size:13px;color:#888;margin-top:4px">数据时间: ' + (data.snapshot_time || '-') + (data.has_momentum ? ' · 含2分钟动量' : ' · 仅当前快照') + '</div>';
        h += '</div>';
        h += '<div style="display:flex;gap:16px;align-items:center">';
        h += '<span style="font-size:12px;color:#aaa" id="smRefreshStatus">30秒自动刷新</span>';
        h += '<span style="font-size:13px;color:#e74c3c;font-weight:600">拉升 <b>' + surgeSectors.length + '</b></span>';
        h += '<span style="font-size:13px;color:#27ae60;font-weight:600">跳水 <b>' + plungeSectors.length + '</b></span>';
        h += '</div>';
        h += '</div>';

        // 行业/概念分组切换
        h += '<div style="display:flex;gap:10px;margin-bottom:12px;align-items:center">';
        var grpBtnBase = 'padding:6px 16px;border:1px solid #ddd;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;transition:all 0.2s';
        h += '<span style="font-size:12px;color:#888;margin-right:4px">分组:</span>';
        h += '<button onclick="window._smGroupBy(\'industry\')" style="' + grpBtnBase + ';color:' + (sectorMoveGroupBy === 'industry' ? '#fff' : '#666') + ';background:' + (sectorMoveGroupBy === 'industry' ? '#1a73e8' : '#f5f5f5') + '">行业板块</button>';
        h += '<button onclick="window._smGroupBy(\'concept\')" style="' + grpBtnBase + ';color:' + (sectorMoveGroupBy === 'concept' ? '#fff' : '#666') + ';background:' + (sectorMoveGroupBy === 'concept' ? '#1a73e8' : '#f5f5f5') + '">概念板块</button>';
        h += '</div>';

        h += '<div style="display:flex;gap:10px;margin-bottom:20px">';
        var btnBase = 'padding:8px 20px;border:1px solid #ddd;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;transition:all 0.2s;background:#fff';
        h += '<button onclick="window._smFilter(\'all\')" style="' + btnBase + ';color:' + (sectorMoveFilter === 'all' ? '#222' : '#999') + (sectorMoveFilter === 'all' ? ';background:#f0f0f0' : '') + '">全部 (' + sectors.length + ')</button>';
        h += '<button onclick="window._smFilter(\'surge\')" style="' + btnBase + ';color:' + (sectorMoveFilter === 'surge' ? '#222' : '#999') + (sectorMoveFilter === 'surge' ? ';background:#fde8e8' : '') + '">拉升 (' + surgeSectors.length + ')</button>';
        h += '<button onclick="window._smFilter(\'plunge\')" style="' + btnBase + ';color:' + (sectorMoveFilter === 'plunge' ? '#222' : '#999') + (sectorMoveFilter === 'plunge' ? ';background:#e8f5e9' : '') + '">跳水 (' + plungeSectors.length + ')</button>';
        h += '</div>';

        if (filtered.length === 0) {
            h += '<div style="text-align:center;padding:60px;color:#999;font-size:16px">暂无异动板块数据</div>';
        } else {
            h += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(380px,1fr));gap:16px">';
            filtered.forEach(function(sec, idx) {
                var chg = sec.avg_change || 0;
                var arrow = chg >= 0 ? '▲' : '▼';
                var barColor = chg >= 0 ? '#e74c3c' : '#27ae60';

                h += '<div style="background:#fff;border-radius:10px;padding:16px 18px;cursor:pointer;transition:all 0.2s;box-shadow:0 1px 4px rgba(0,0,0,0.08);border-left:4px solid ' + barColor + '" onclick="window._smToggle(' + idx + ')" id="smCard' + idx + '">';

                h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
                h += '<span style="font-size:16px;font-weight:700;color:#333">' + sec.name + '</span>';
                h += '<span style="font-size:20px;font-weight:800;color:' + barColor + '">' + arrow + ' ' + (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%</span>';
                h += '</div>';

                h += '<div style="display:flex;gap:16px;font-size:12px;color:#999;margin-bottom:10px">';
                h += '<span>成分股 <b style="color:#666">' + sec.stock_count + '</b></span>';
                h += '<span style="color:#e74c3c">↑ ' + sec.up_count + '</span>';
                h += '<span style="color:#27ae60">↓ ' + sec.down_count + '</span>';
                h += '</div>';

                if (sec.leader) {
                    var ld = sec.leader;
                    h += '<div style="background:#f8f8f8;border-radius:8px;padding:8px 12px;display:flex;align-items:center;gap:10px">';
                    h += '<span style="font-size:14px">🐲</span>';
                    h += '<span style="font-size:13px;font-weight:600;color:#b8860b">龙头</span>';
                    h += '<span style="font-size:13px;font-weight:600;color:#333">' + ld.name + '</span>';
                    h += '<span style="font-size:11px;color:#aaa">' + ld.code + '</span>';
                    h += '<span style="font-size:12px;color:#666;font-weight:700">动量' + (ld.momentum >= 0 ? '+' : '') + ld.momentum.toFixed(2) + '%</span>';
                    h += '<span style="font-size:12px;color:#999">日涨幅 ' + (ld.change_pct >= 0 ? '+' : '') + ld.change_pct.toFixed(2) + '%</span>';
                    h += '</div>';
                }

                h += '<div style="display:none;margin-top:12px" id="smMovers' + idx + '">';
                if (sec.top_movers && sec.top_movers.length > 0) {
                    h += '<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:8px">';
                    h += '<thead><tr style="color:#999;text-align:left;border-bottom:1px solid #ddd">';
                    h += '<th style="padding:6px 4px">#</th><th>股票</th><th style="text-align:right">2分钟动量</th><th style="text-align:right">日涨幅</th><th style="text-align:right">成交额</th>';
                    h += '</tr></thead><tbody>';
                    sec.top_movers.forEach(function(s, si) {
                        h += '<tr style="border-bottom:1px solid #eee">';
                        h += '<td style="padding:5px 4px;color:' + (si < 3 ? '#b8860b' : '#999') + ';font-weight:' + (si < 3 ? '700' : '400') + '">' + (si + 1) + '</td>';
                        h += '<td style="padding:5px 4px"><span style="color:#333;font-weight:600">' + s.name + '</span> <span style="color:#bbb;font-size:11px">' + s.code + '</span></td>';
                        h += '<td style="text-align:right;padding:5px 4px;color:' + (s.momentum >= 0 ? '#c0392b' : '#1e8449') + ';font-weight:600">' + (s.momentum >= 0 ? '+' : '') + s.momentum.toFixed(2) + '%</td>';
                        h += '<td style="text-align:right;padding:5px 4px;color:#555">' + (s.change_pct >= 0 ? '+' : '') + s.change_pct.toFixed(2) + '%</td>';
                        h += '<td style="text-align:right;padding:5px 4px;color:#999">' + fmtMoney(s.amount) + '</td>';
                        h += '</tr>';
                    });
                    h += '</tbody></table>';
                }
                h += '</div>';

                h += '</div>';
            });
            h += '</div>';
        }
        container.innerHTML = h;
        var rsEl = document.getElementById('smRefreshStatus');
        if (rsEl) rsEl.textContent = '上次刷新: ' + new Date().toLocaleTimeString();
    }

    window._smFilter = function(f) {
        sectorMoveFilter = f;
        var c = document.getElementById('sectorBody');
        if (c && sectorMoveData) renderSectorMovementPage(c, sectorMoveData);
    };

    window._smGroupBy = function(g) {
        sectorMoveGroupBy = g;
        var c = document.getElementById('sectorBody');
        if (c && sectorMoveData) renderSectorMovementPage(c, sectorMoveData);
    };

    window._smToggle = function(idx) {
        var panel = document.getElementById('smMovers' + idx);
        if (panel) panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
    };

    function renderJqMinutePanel(status) {
        status = status || {};
        var jqStatus = status.jq_status || {};
        var latestDay = status.latest_day || {};
        var task = status.scheduler_task || {};
        var available = !!jqStatus.available;
        var tableOk = !!status.table_exists;
        var taskOn = Number(task.enabled || 0) === 1;
        var color = available && tableOk ? '#16a34a' : '#f59e0b';
        var title = available ? '聚宽账号可用' : '聚宽未就绪';
        var latestText = latestDay.latest_trade_time ? (String(latestDay.trade_date || '').slice(0, 10) + ' ' + String(latestDay.latest_trade_time).slice(11, 19)) : '-';
        var quota = status.jq_query_count ? ('额度 ' + JSON.stringify(status.jq_query_count)) : '';
        var h = '<div style="background:#fff;border-left:4px solid ' + color + ';border-radius:10px;padding:14px 16px;margin-bottom:14px;box-shadow:0 1px 6px rgba(0,0,0,.05)">';
        h += '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px">';
        h += '<span style="background:' + color + ';color:#fff;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:900">' + title + '</span>';
        h += '<span style="font-size:14px;font-weight:900;color:#111827">分钟GML接入</span>';
        h += '<span style="font-size:12px;color:#64748b">表 ' + (tableOk ? '已建' : '未建') + ' / 自动任务 ' + (taskOn ? '已启用' : '未启用') + ' / 最新分钟 ' + latestText + '</span>';
        if (quota) h += '<span style="font-size:11px;color:#64748b">' + quota + '</span>';
        h += '</div>';
        if (!available) {
            h += '<div style="font-size:12px;color:#b45309;line-height:1.6;margin-bottom:10px">未检测到聚宽 SDK 或账号配置：' + (jqStatus.reason || '请配置 JQ_PHONE / JQ_PASSWORD 并安装 jqdatasdk') + '</div>';
        }
        h += '<div style="display:flex;gap:8px;flex-wrap:wrap">';
        h += '<button onclick="jqEnsureMinuteTable()" style="padding:6px 12px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:12px;font-weight:700">建表</button>';
        h += '<button onclick="jqSyncMinuteOnce()" style="padding:6px 12px;border:none;border-radius:6px;background:#16a34a;color:#fff;cursor:pointer;font-size:12px;font-weight:700">手动同步</button>';
        h += '<button onclick="jqEnableMinuteAuto()" style="padding:6px 12px;border:none;border-radius:6px;background:#f59e0b;color:#111827;cursor:pointer;font-size:12px;font-weight:800">启用1分钟自动同步</button>';
        h += '<button onclick="jqDisableMinuteAuto()" style="padding:6px 12px;border:none;border-radius:6px;background:#64748b;color:#fff;cursor:pointer;font-size:12px;font-weight:700">停用自动同步</button>';
        h += '<span id="jqMinuteStatusMsg" style="font-size:12px;color:#64748b;align-self:center"></span>';
        h += '</div></div>';
        return h;
    }

    function jqMinuteAction(url, options, successText) {
        var msg = document.getElementById('jqMinuteStatusMsg');
        if (msg) msg.textContent = '执行中...';
        return fetch(url, options || { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(res) {
                if (res.error || res.detail || res.success === false || res.status === 'error') {
                    throw new Error(res.error || res.detail || res.message || '执行失败');
                }
                if (msg) msg.textContent = successText || '已完成';
                setTimeout(function() { loadJqPicksPage(document.getElementById('tab-jq-picks')); }, 800);
                return res;
            })
            .catch(function(e) {
                if (msg) msg.textContent = '失败: ' + e.message;
                alert('聚宽操作失败: ' + e.message);
            });
    }

    window.jqEnsureMinuteTable = function() {
        jqMinuteAction('/api/jq/minute/table/ensure', { method: 'POST' }, '表已确认');
    };
    window.jqSyncMinuteOnce = function() {
        jqMinuteAction('/api/jq/minute/sync?universe=latest-kline&limit=80&count=3&batch_size=200&min_coverage=0', { method: 'POST' }, '同步已完成');
    };
    window.jqEnableMinuteAuto = function() {
        jqMinuteAction('/api/jq/minute/automation/enable?universe=latest-kline&limit=0&count=3&batch_size=200&interval_minutes=1&cron_time=09%3A30&min_coverage=0', { method: 'POST' }, '自动同步已启用');
    };
    window.jqDisableMinuteAuto = function() {
        jqMinuteAction('/api/jq/minute/automation/disable', { method: 'POST' }, '自动同步已停用');
    };

    function loadJqPicksPage(container) {
        container.innerHTML = '<div class="loading">加载聚宽接入状态...</div>';
        Promise.all([
            fetch('/api/jq/minute/status?include_quota=true').then(function(r) { return r.json(); }).catch(function(e) { return { jq_status: { available: false, reason: e.message } }; }),
            fetch('/api/strategy/picks/list').then(function(r) { return r.json(); }).catch(function() { return { strategies: [] }; })
        ])
            .then(function(results) {
                var jqStatus = results[0] || {};
                var meta = results[1] || {};
                var strategies = meta.strategies || [];
                if (!strategies.length) {
                    container.innerHTML = renderJqMinutePanel(jqStatus) + '<div style="text-align:center;padding:60px 20px;color:#888;background:#fff;border-radius:10px;box-shadow:0 1px 6px rgba(0,0,0,.05)">' +
                        '<div style="font-size:48px;margin-bottom:16px;">🤖</div>' +
                        '<div style="font-size:16px;font-weight:600;color:#555;margin-bottom:8px;">暂无策略数据</div>' +
                        '<div style="font-size:13px;color:#999;">请在聚宽平台运行策略后同步到此系统</div>' +
                        '</div>';
                    return;
                }
                var firstStrategy = strategies[0].strategy_name;
                fetchJqPicksData(container, firstStrategy, null, strategies, jqStatus);
            })
            .catch(function() {
                container.innerHTML = '<div class="loading">加载失败</div>';
            });
    }

    function fetchJqPicksData(container, strategyName, date, strategies, jqStatus) {
        var url = '/api/strategy/picks/data?strategy_name=' + encodeURIComponent(strategyName);
        if (date) url += '&date=' + date;
        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(data) {
                renderJqPicksPage(container, data, strategies, jqStatus);
            })
            .catch(function() {
                container.innerHTML = '<div class="loading">加载失败</div>';
            });
    }

    function renderJqPicksPage(container, data, strategies, jqStatus) {
        var picks = data.picks || [];
        var strategies = strategies || [];
        var h = renderJqMinutePanel(jqStatus || {});

        h += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px;">';
        h += '<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">';
        if (strategies.length > 1) {
            h += '<select id="jqStrategySelect" onchange="window._jqChangeStrategy()" style="padding:6px 12px;border:1px solid #ddd;border-radius:6px;font-size:13px;background:#fff;">';
            strategies.forEach(function(s) {
                var sel = s.strategy_name === data.strategy_name ? ' selected' : '';
                h += '<option value="' + s.strategy_name + '"' + sel + '>' + s.strategy_name + '</option>';
            });
            h += '</select>';
        } else {
            h += '<span style="font-size:14px;font-weight:600;color:#333;">📋 ' + (data.strategy_name || '策略') + '</span>';
        }
        h += '<span style="font-size:12px;color:#888;">选股日期: ' + (data.date || '-') + '</span>';
        h += '<span style="font-size:12px;color:#888;">共 ' + picks.length + ' 只</span>';
        h += '</div>';
        h += '</div>';

        if (!picks.length) {
            h += '<div style="text-align:center;padding:40px;color:#999;">暂无选股结果</div>';
            container.innerHTML = h;
            return;
        }

        h += '<div style="overflow-x:auto;">';
        h += '<table class="data-table" style="width:100%;">';
        h += '<thead><tr>';
        h += '<th style="width:50px;">序号</th>';
        h += '<th>股票代码</th>';
        h += '<th>股票名称</th>';
        h += '<th>分数</th>';
        h += '<th>入选原因</th>';
        h += '<th>现价</th>';
        h += '<th>涨跌幅</th>';
        h += '<th>涨跌额</th>';
        h += '<th>成交额</th>';
        h += '</tr></thead><tbody>';

        picks.forEach(function(p, i) {
            var pct = p.change_pct != null ? p.change_pct : null;
            var color = pct == null ? '#333' : pct >= 0 ? '#e74c3c' : '#27ae60';
            var arrow = pct == null ? '' : pct >= 0 ? '▲' : '▼';

            h += '<tr>';
            h += '<td style="text-align:center;color:#888;">' + (i + 1) + '</td>';
            h += '<td style="font-family:monospace;font-weight:600;">' + p.stock_code + '</td>';
            h += '<td>' + (p.short_name || '-') + '</td>';
            h += '<td style="text-align:center;">' + (p.score != null ? p.score.toFixed(2) : '-') + '</td>';
            h += '<td style="font-size:12px;color:#666;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + (p.reason || '') + '">' + (p.reason || '-') + '</td>';
            h += '<td style="text-align:right;font-weight:600;">' + (p.price != null ? p.price.toFixed(2) : '-') + '</td>';
            h += '<td style="text-align:right;color:' + color + ';font-weight:600;">' + (pct != null ? arrow + ' ' + pct.toFixed(2) + '%' : '-') + '</td>';
            h += '<td style="text-align:right;color:' + color + ';">' + (p.change_amt != null ? p.change_amt.toFixed(2) : '-') + '</td>';
            h += '<td style="text-align:right;">' + (p.amount != null ? formatAmount(p.amount) : '-') + '</td>';
            h += '</tr>';
        });

        h += '</tbody></table></div>';
        container.innerHTML = h;
    }

    window._jqChangeStrategy = function() {
        var sel = document.getElementById('jqStrategySelect');
        if (!sel) return;
        var c = document.getElementById('tab-jq-picks');
        var strategies = [];
        Array.prototype.slice.call(sel.options || []).forEach(function(opt) {
            strategies.push({ strategy_name: opt.value });
        });
        fetch('/api/jq/minute/status?include_quota=true')
            .then(function(r) { return r.json(); })
            .catch(function(e) { return { jq_status: { available: false, reason: e.message } }; })
            .then(function(jqStatus) {
                fetchJqPicksData(c, sel.value, null, strategies, jqStatus);
            });
    };

    /* ===== AI推荐买入页面 ===== */
    function recQueryUrl(d) {
        var qs = [];
        var s = document.getElementById('recStartDate');
        var e = document.getElementById('recEndDate');
        if (s && s.value) qs.push('start_date=' + encodeURIComponent(s.value));
        if (e && e.value) qs.push('end_date=' + encodeURIComponent(e.value));
        if (!qs.length) qs.push('trade_date=' + encodeURIComponent(d || currentDateValue()));
        return '/recommended-stocks?' + qs.join('&');
    }

    function recGateUrl(d) {
        var targetDate = d || currentDateValue();
        var executionDate = currentDateValue();
        var qs = [
            'check_readiness=false',
            'target_trade_date=' + encodeURIComponent(targetDate),
            'execution_time=' + encodeURIComponent(executionDate + ' 08:30:00')
        ];
        return '/recommended-stocks/gate?' + qs.join('&');
    }

    function recRuntimeParamsUrl(d) {
        var targetDate = d || currentDateValue();
        return '/strategy-runtime-params?as_of_date=' + encodeURIComponent(targetDate);
    }

    function loadRecommendedPage(d, container) {
        var reqId = (window._recLoadReqId || 0) + 1;
        window._recLoadReqId = reqId;
        var keepCurrent = silentRefreshDepth > 0 && hasRenderedContent(container);
        setStatus(keepCurrent ? 'AI 推荐后台刷新中...' : '正在加载 AI 推荐...');
        if (!keepCurrent) {
            renderRecommendedLoadingState(container, d, {
                message: '正在加载推荐信号...',
                statusText: '正在准备推荐视图...'
            });
        }
        Promise.all([
            fetchJsonWithTimeout(recQueryUrl(d), 8000),
            fetchJsonWithTimeout(recGateUrl(d), 8000).catch(function(e) {
                return {
                    status: 'degraded',
                    ready: false,
                    strict_ok: false,
                    expected_trade_date: d || currentDateValue(),
                    target_source: 'client_fallback',
                    error: '门禁状态检查超时，不影响已生成推荐列表展示；开始筛选时仍会执行严格门禁。' + (e && e.message ? ' ' + e.message : '')
                };
            }),
            fetchJsonWithTimeout(recRuntimeParamsUrl(d), 8000).catch(function(e) {
                return { params: [], params_map: {}, error: e && e.message ? e.message : String(e || '') };
            })
        ]).then(function(results) {
            if (window._recLoadReqId !== reqId) return;
            var res = results[0] || {};
            res.gate = results[1] || {};
            res.runtime_params = results[2] || {};
            setStatus('AI 推荐结果已就绪。');
            renderRecommendedPage(container, res, d);
        }).catch(function(err) {
            if (window._recLoadReqId !== reqId) return;
            fetch('/api/hot-data/recommended-stocks/progress')
                .then(function(r) { return r.json(); })
                .then(function(progress) {
                    if (window._recLoadReqId !== reqId) return;
                    setStatus(keepCurrent ? 'AI 推荐刷新较慢，继续显示现有结果。' : 'AI 推荐响应较慢，先显示进度。');
                    if (!keepCurrent) {
                        renderRecommendedLoadingState(container, d, {
                            message: '推荐数据加载时间比平时更长。',
                            statusText: progress && progress.is_running
                                ? '推荐筛选任务正在运行，进度如下。'
                                : '页面已就绪，可以启动或重试筛选。',
                            errorText: err && err.message ? err.message : '',
                            progress: progress
                        });
                    }
                    if (progress && progress.is_running) {
                        _pollRecProgress();
                    } else {
                        _resumeRecommendedProgress();
                    }
                })
                .catch(function() {
                    if (window._recLoadReqId !== reqId) return;
                    setStatus(keepCurrent ? 'AI 推荐刷新失败，已保留现有结果。' : 'AI 推荐响应较慢，请稍后重试。');
                    if (!keepCurrent) {
                        renderRecommendedLoadingState(container, d, {
                            message: '推荐数据加载时间比平时更长。',
                            statusText: '页面已就绪，可以重新加载或启动筛选。',
                            errorText: err && err.message ? err.message : ''
                        });
                    }
                });
        });
    }

    function renderRecommendedLoadingState(container, dateStr, options) {
        options = options || {};
        var progress = options.progress || null;
        var h = '';
        h += '<div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;">';
        h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
        h += '<div style="font-size:28px;font-weight:800;color:#9ca3af;">--</div><div style="font-size:11px;color:#aaa;">推荐数量</div></div>';
        h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
        h += '<div style="font-size:28px;font-weight:800;color:#60a5fa;">--</div><div style="font-size:11px;color:#aaa;">平均评分</div></div>';
        h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
        h += '<div style="font-size:28px;font-weight:800;color:#34d399;">--</div><div style="font-size:11px;color:#aaa;">最高评分</div></div>';
        h += '</div>';
        h += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap;">';
        h += '<button onclick="window._runRecommendedScreen()" style="padding:8px 20px;border:none;border-radius:8px;background:linear-gradient(135deg,#e74c3c,#c0392b);color:#fff;font-weight:700;font-size:14px;cursor:pointer;box-shadow:0 2px 8px rgba(231,76,60,0.3);">开始筛选</button>';
        h += '<button onclick="window._retryRecommendedLoad()" style="padding:8px 16px;border:1px solid #d0d7de;border-radius:8px;background:#fff;color:#333;font-weight:600;font-size:13px;cursor:pointer;">重新加载</button>';
        h += '<span style="font-size:12px;color:#888;">请求日期：' + (dateStr || '-') + '</span>';
        h += '<span id="recStatus" style="font-size:12px;color:#f39c12;"></span>';
        h += '</div>';
        h += '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:18px 20px;margin-bottom:14px;">';
        h += '<div style="font-size:16px;font-weight:700;color:#333;margin-bottom:8px;">' + (options.message || '正在加载推荐数据...') + '</div>';
        h += '<div style="font-size:13px;color:#666;line-height:1.6;">' + (options.statusText || '请稍候，系统正在准备推荐结果。') + '</div>';
        if (options.errorText) {
            h += '<div style="margin-top:10px;font-size:12px;color:#c0392b;">最近错误：' + escAttr(options.errorText) + '</div>';
        }
        h += '</div>';
        h += '<div id="recProgressBar" style="display:none;margin-bottom:14px;padding:10px 14px;background:#1a1a2e;border-radius:8px;border:1px solid #2a2a4a;">';
        h += '<div style="display:flex;align-items:center;gap:10px;">';
        h += '<div style="flex:1;height:20px;background:#111827;border-radius:10px;overflow:hidden;position:relative;">';
        h += '<div id="recProgressFill" style="height:100%;width:0%;border-radius:10px;transition:width 0.5s ease;background:linear-gradient(90deg,#3b82f6,#8b5cf6);"></div>';
        h += '<span id="recProgressText" style="position:absolute;top:0;left:0;right:0;text-align:center;line-height:20px;font-size:11px;color:#fff;font-weight:600;text-shadow:0 1px 2px rgba(0,0,0,0.5);">准备中...</span>';
        h += '</div></div></div>';
        container.innerHTML = h;
        if (progress) {
            _updateRecommendedProgressUI(progress);
        }
    }

    window._retryRecommendedLoad = function() {
        var c = document.getElementById('tab-recommended');
        if (c) runWithSilentRefresh(function () { loadRecommendedPage(currentDateValue(), c); });
    };

    window._applyRecDateRange = function() {
        var c = document.getElementById('tab-recommended');
        if (c) runWithSilentRefresh(function () { loadRecommendedPage(currentDateValue(), c); });
    };

    window._clearRecDateRange = function() {
        var s = document.getElementById('recStartDate');
        var e = document.getElementById('recEndDate');
        if (s) s.value = '';
        if (e) e.value = '';
        var c = document.getElementById('tab-recommended');
        if (c) runWithSilentRefresh(function () { loadRecommendedPage(currentDateValue(), c); });
    };

    window._startRecommendedAutoRefresh = function() {
        if (window._recAutoRefresh) clearInterval(window._recAutoRefresh);
        window._recAutoRefresh = setInterval(function() {
            var active = document.querySelector('.sidebar-item.active');
            if (!active || active.getAttribute('data-tab') !== 'recommended') return;
            if (!isTradingTime()) return;
            var c = document.getElementById('tab-recommended');
            if (!c) return;
            var now = Date.now();
            if (window._recLastAutoRefreshAt && now - window._recLastAutoRefreshAt < 60000) return;
            fetch('/api/hot-data/recommended-stocks/progress')
                .then(function(r) { return r.json(); })
                .then(function(progress) {
                    if (progress && progress.is_running) return;
                    window._recLastAutoRefreshAt = Date.now();
                    runWithSilentRefresh(function () { loadRecommendedPage(currentDateValue(), c); });
                })
                .catch(function() {});
        }, 15000);
    };

    function renderRecommendedPage(container, data, dateStr) {
        window._recLastData = data;
        window._recLastDate = dateStr;
        var rawItems = data.data || [];
        var currentStrategy = window._recStrategy || 'all';
        function recStrategyLabel(v) {
            var m = {all: '全部', ultra_short: '超短', short_term: '短线', swing: '波段', main_wave: '主升浪'};
            return m[v] || v || '-';
        }
        function recStatusLabel(v) {
            var m = {WATCH: '观察', CONFIRM: '确认', BUY_READY: '买入就绪', SELL_ALERT: '卖出提醒', BLOCK: '屏蔽', ALLOW: '允许', SUSPENDED: '暂停'};
            return m[v] || v || '-';
        }
        function recStatusColor(v) {
            var m = {WATCH: '#f39c12', CONFIRM: '#1a73e8', BUY_READY: '#27ae60', SELL_ALERT: '#e67e22', BLOCK: '#e74c3c', SUSPENDED: '#f39c12', ALLOW: '#27ae60'};
            return m[v] || '#666';
        }
        function recRatingColor(v) {
            var m = {'买入': '#dc2626', '增持': '#ea580c', '中性': '#64748b', '减持': '#d97706', '卖出': '#16a34a'};
            return m[v] || '#64748b';
        }
        function recParseStrategies(v) {
            if (!v) return [];
            if (Array.isArray(v)) return v;
            try { var a = JSON.parse(v); return Array.isArray(a) ? a : []; } catch(e) { return String(v).split(','); }
        }
        function recHasStrategy(r, strategy) {
            if (!strategy || strategy === 'all') return true;
            if ((r.primary_strategy || r.strategy_profile) === strategy) return true;
            return recParseStrategies(r.suitable_strategies).indexOf(strategy) >= 0;
        }
        function recScoreFor(r, strategy) {
            if (strategy === 'ultra_short') return firstAnalysisValue(r.ultra_short_score, r.short_term_score, blendedAnalysisRowScore(r), 0) || 0;
            if (strategy === 'swing') return firstAnalysisValue(r.swing_score, r.long_term_score, blendedAnalysisRowScore(r), 0) || 0;
            if (strategy === 'main_wave') return firstAnalysisValue(r.main_wave_score, r.final_trade_score, blendedAnalysisRowScore(r), 0) || 0;
            if (strategy === 'short_term') return firstAnalysisValue(r.short_term_score, blendedAnalysisRowScore(r), 0) || 0;
            return firstAnalysisValue(r.final_trade_score, blendedAnalysisRowScore(r), r.short_term_score, 0) || 0;
        }
        function recMainWaveLabel(v) {
            var m = {BUY_READY: '买点', WATCH: '观察', REDUCE: '减仓', SELL_ALERT: '卖出', NONE: '-'};
            return m[v] || v || '-';
        }
        function recMainWaveColor(v) {
            var m = {BUY_READY: '#27ae60', WATCH: '#f39c12', REDUCE: '#e67e22', SELL_ALERT: '#e74c3c', NONE: '#888'};
            return m[v] || '#666';
        }
        function recPrice(v) { return v == null || v === '' || isNaN(v) ? '-' : Number(v).toFixed(2); }
        function recRange(a, b) { return (a != null && b != null) ? recPrice(a) + '-' + recPrice(b) : '-'; }
        function recJsonField(v, fallback) {
            if (!v) return fallback;
            if (typeof v !== 'string') return v;
            try { return JSON.parse(v); } catch(e) { return fallback; }
        }
        function recEvidenceText(r) {
            var chain = recJsonField(r.evidence_chain_json, []);
            if (!Array.isArray(chain) || !chain.length) return localizeMachineText(r.signal_reason || r.recommend_reason || '-');
            return chain.slice(0, 40).map(function(x) {
                return localizeMachineText(x.module || '-') + ': ' + localizeMachineText(x.text || x.status || '-');
            }).join('\n');
        }
        function recReviewText(r) {
            var parts = [];
            [['1日', r.review_1d_pct], ['3日', r.review_3d_pct], ['5日', r.review_5d_pct], ['10日', r.review_10d_pct]].forEach(function(x) {
                if (x[1] != null && x[1] !== '') parts.push(x[0] + ' ' + Number(x[1]).toFixed(1) + '%');
            });
            return parts.length ? parts.join(' / ') : '-';
        }
        function recSectorLabel(v) {
            var m = {PASS: '通过', WATCH: '观察', BLOCK: '不合格'};
            return m[v] || v || '-';
        }
        function recSectorColor(v) {
            var m = {PASS: '#16a34a', WATCH: '#d97706', BLOCK: '#dc2626'};
            return m[v] || '#64748b';
        }
        var items = rawItems.filter(function(r) { return recHasStrategy(r, currentStrategy); }).sort(function(a, b) { return recScoreFor(b, currentStrategy) - recScoreFor(a, currentStrategy); });
        var h = '';
        var gate = data.gate || {};
        var gateReady = !!(gate.ready || gate.strict_ok);
        var gateRec = gate.recommendation || {};
        var gateReadiness = gate.readiness || {};
        var gateNews = gate.news || {};
        var gateDegraded = gate.status === 'error' || gate.status === 'degraded';
        var hasDisplayedRecommendation = rawItems.length > 0;
        var fresh = data.freshness || {};
        var showingFallbackRecommendation = !!(fresh.is_fallback_date || (data.date && dateStr && data.date !== dateStr));
        gateReady = !!(gateReady || gate.has_recommendation || gateRec.count || hasDisplayedRecommendation);
        var gateColor = gateReady ? '#16a34a' : (gateDegraded ? '#d97706' : '#dc2626');
        var gateBg = gateReady ? '#ecfdf5' : (gateDegraded ? '#fff7ed' : '#fff1f2');
        var gateStatusText = gateDegraded ? '门禁状态待确认' : (gateReady ? (showingFallbackRecommendation ? '显示最近推荐日' : '数据可用') : '目标日推荐未生成');
        var klineText = gateReadiness.skipped ? '-/-' : ((gateReadiness.kline_count || 0) + '/' + (gateReadiness.expected_count || '-'));
        h += '<div style="background:' + gateBg + ';border:1px solid ' + gateColor + '33;border-radius:14px;padding:12px 14px;margin-bottom:14px;">';
        h += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;">';
        h += '<div><div style="font-size:15px;font-weight:900;color:#0f172a;">08:30 AI推荐严格门禁</div>';
        h += '<div style="font-size:12px;color:#64748b;margin-top:4px;line-height:1.6;">严格门禁检查目标日是否可生成；下方列表可显示最近已生成推荐日，避免空白页误导。</div></div>';
        h += '<span style="padding:5px 10px;border-radius:999px;background:#fff;color:' + gateColor + ';font-size:12px;font-weight:900;">' + gateStatusText + '</span>';
        h += '</div>';
        h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin-top:10px;font-size:12px;">';
        h += '<div style="background:#fff;border-radius:10px;padding:9px;"><div style="color:#64748b;">目标交易日</div><div style="font-size:16px;font-weight:900;color:#0f172a;">' + (gate.expected_trade_date || '-') + '</div></div>';
        h += '<div style="background:#fff;border-radius:10px;padding:9px;"><div style="color:#64748b;">K线覆盖</div><div style="font-size:16px;font-weight:900;color:' + gateColor + ';">' + klineText + '</div></div>';
        h += '<div style="background:#fff;border-radius:10px;padding:9px;"><div style="color:#64748b;">已有推荐</div><div style="font-size:16px;font-weight:900;color:#2563eb;">' + (gateRec.count || 0) + '</div></div>';
        h += '<div style="background:#fff;border-radius:10px;padding:9px;"><div style="color:#64748b;">截止新闻</div><div style="font-size:16px;font-weight:900;color:#ea580c;">' + (gateNews.count || 0) + '</div></div>';
        h += '<div style="background:#fff;border-radius:10px;padding:9px;"><div style="color:#64748b;">执行时间</div><div style="font-size:13px;font-weight:800;color:#0f172a;">' + (gate.execution_time || '-') + '</div></div>';
        h += '</div>';
        if (gate.error) h += '<div style="margin-top:8px;font-size:12px;color:' + gateColor + ';">' + escAttr(gate.error) + '</div>';
        h += '</div>';
        var freshSources = (fresh.sources || []).map(function(s) { return (s.label || s.key || '-') + ':' + (s.latest_date || '-'); }).join(' / ');
        var freshColor = fresh.status === 'fresh' ? '#16a34a' : (fresh.status === 'fallback' ? '#d97706' : '#dc2626');
        var freshBg = fresh.status === 'fresh' ? '#ecfdf5' : (fresh.status === 'fallback' ? '#fff7ed' : '#fff1f2');
        h += '<div style="background:' + freshBg + ';border:1px solid ' + freshColor + '33;border-radius:12px;padding:10px 12px;margin-bottom:14px;display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap;">';
        h += '<div><div style="font-size:13px;font-weight:900;color:' + freshColor + ';">推荐列表数据新鲜度：' + safeText(fresh.status_label || '-') + '</div>';
        h += '<div style="font-size:12px;color:#64748b;margin-top:4px;line-height:1.6;">请求 ' + safeText(fresh.requested_date || dateStr || '-') + ' / 实际推荐 ' + safeText(fresh.result_date || data.date || '-') + ' / 行情 ' + safeText(fresh.quote_mode || '-') + '</div></div>';
        h += '<div style="font-size:12px;color:#64748b;max-width:560px;line-height:1.6;">' + safeText(freshSources || '-') + '</div>';
        h += '</div>';
        var runtime = data.runtime_params || {};
        var runtimeParams = Array.isArray(runtime.params) ? runtime.params : [];
        var runtimeLabels = {
            min_risk_reward: '最低盈亏比',
            min_sector_flow_amount_3d: '板块资金阈值',
            min_sector_rotation_score: '板块轮动分',
            price_crosscheck_tolerance_pct: '价格校验偏差'
        };
        if (runtimeParams.length) {
            h += '<div style="background:#f8fafc;border:1px solid #dbe4ee;border-radius:12px;padding:10px 12px;margin-bottom:14px;">';
            h += '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;">';
            h += '<div style="font-size:13px;font-weight:900;color:#0f172a;">运行阈值</div>';
            h += '<div style="font-size:12px;color:#64748b;">生效日 ' + safeText(runtime.as_of_date || '-') + (runtime.error ? ' / ' + safeText(runtime.error) : '') + '</div>';
            h += '</div>';
            h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-top:8px;">';
            runtimeParams.forEach(function(p) {
                var key = p.param_key || '';
                var value = Number(p.param_value || 0);
                var displayValue = key === 'min_sector_flow_amount_3d' ? fmtMoney(value) : (key === 'price_crosscheck_tolerance_pct' ? value.toFixed(2) + '%' : value.toFixed(2));
                h += '<div style="background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:8px;">';
                h += '<div style="font-size:11px;color:#64748b;">' + safeText(runtimeLabels[key] || key || '-') + '</div>';
                h += '<div style="font-size:16px;font-weight:900;color:#0f172a;">' + safeText(displayValue) + '</div>';
                h += '<div style="font-size:10px;color:#94a3b8;">' + safeText(p.source || 'default') + '</div>';
                h += '</div>';
            });
            h += '</div></div>';
        }

        // 检查是否是新格式
        var hasNewFormat = items.length > 0 && (items[0].long_term_score != null || items[0].short_term_score != null);

        // 顶部统计卡片
        var count = items.length;
        var avgScore = 0, maxScore = 0;
        if (count > 0) {
            var sum = 0;
            items.forEach(function(r) {
                var score = hasNewFormat ? recScoreFor(r, currentStrategy) : (blendedAnalysisRowScore(r) || 0);
                sum += score;
                if (score > maxScore) maxScore = score;
            });
            avgScore = Math.round(sum / count);
        }

        h += '<div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;">';
        h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
        h += '<div style="font-size:32px;font-weight:800;color:#e74c3c;">' + count + '</div><div style="font-size:11px;color:#aaa;">推荐数量</div></div>';
        h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
        h += '<div style="font-size:32px;font-weight:800;color:#f39c12;">' + avgScore + '</div><div style="font-size:11px;color:#aaa;">平均评分</div></div>';
        h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
        h += '<div style="font-size:32px;font-weight:800;color:#27ae60;">' + Math.round(maxScore) + '</div><div style="font-size:11px;color:#aaa;">最高评分</div></div>';

        // 新格式：统计风险等级
        if (hasNewFormat) {
            var riskCounts = {LOW: 0, MEDIUM: 0, HIGH: 0, CRITICAL: 0};
            items.forEach(function(r) {
                var level = r.event_risk_level || 'LOW';
                riskCounts[level] = (riskCounts[level] || 0) + 1;
            });
            h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
            h += '<div style="font-size:14px;font-weight:700;color:#27ae60;">' + riskCounts.LOW + '</div><div style="font-size:10px;color:#aaa;">低风险</div></div>';
            if (riskCounts.MEDIUM > 0 || riskCounts.HIGH > 0) {
                h += '<div style="flex:1;min-width:120px;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;text-align:center;color:#fff;">';
                h += '<div style="font-size:14px;font-weight:700;color:#f39c12;">' + (riskCounts.MEDIUM + riskCounts.HIGH) + '</div><div style="font-size:10px;color:#aaa;">中高风险</div></div>';
            }
        }
        h += '</div>';

        // 操作栏 - 使用今天的日期显示
        h += '<div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;align-items:center;">';
        h += '<span style="font-size:12px;color:#888;">' + (data.model_version || '') + '</span>';
        h += '</div>';

        var todayStr = new Date().toISOString().split('T')[0];
        var displayDate = data.date || dateStr;
        var dateInfo = todayStr;
        if (displayDate && displayDate !== todayStr) {
            dateInfo = todayStr + '（数据截至 ' + displayDate + '）';
        }

        // 统计推荐买入数量
        var buyCount = 0, watchCount = 0, blockCount = 0;
        items.forEach(function(r) {
            var status = r.signal_status || r.recommend_status || 'WATCH';
            if (status === 'BLOCK') blockCount++;
            else if (status === 'CONFIRM' || status === 'BUY_READY') buyCount++;
            else watchCount++;
        });

        // 推荐买入汇总卡片
        h += '<div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;">';
        h += '<div style="flex:1;min-width:100px;background:linear-gradient(135deg,#0d4a0d,#1a7a1a);border-radius:10px;padding:12px;text-align:center;color:#fff;border:1px solid #27ae60;">';
        h += '<div style="font-size:28px;font-weight:800;color:#2ecc71;">' + buyCount + '</div><div style="font-size:11px;color:#a8e6cf;">✅ 推荐买入</div></div>';
        h += '<div style="flex:1;min-width:100px;background:linear-gradient(135deg,#4a3a0d,#7a6a1a);border-radius:10px;padding:12px;text-align:center;color:#fff;border:1px solid #f39c12;">';
        h += '<div style="font-size:28px;font-weight:800;color:#f1c40f;">' + watchCount + '</div><div style="font-size:11px;color:#f9e79f;">⚡ 谨慎观望</div></div>';
        h += '<div style="flex:1;min-width:100px;background:linear-gradient(135deg,#4a0d0d,#7a1a1a);border-radius:10px;padding:12px;text-align:center;color:#fff;border:1px solid #e74c3c;">';
        h += '<div style="font-size:28px;font-weight:800;color:#e74c3c;">' + blockCount + '</div><div style="font-size:11px;color:#f5b7b1;">❌ 不推荐</div></div>';
        h += '</div>';

        h += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap;">';
        h += '<button onclick="window._runRecommendedScreen()" style="padding:8px 20px;border:none;border-radius:8px;background:linear-gradient(135deg,#e74c3c,#c0392b);color:#fff;font-weight:700;font-size:14px;cursor:pointer;box-shadow:0 2px 8px rgba(231,76,60,0.3);">🔍 开始筛选</button>';
        h += '<span style="font-size:12px;color:#888;">筛选日期: ' + dateInfo + ' | 共 <span id="recFilteredCount">' + count + '</span> 只</span>';
        h += '<span id="recStatus" style="font-size:12px;color:#f39c12;"></span>';
        h += '</div>';
        // 进度条
        h += '<div id="recProgressBar" style="display:none;margin-bottom:14px;padding:10px 14px;background:#1a1a2e;border-radius:8px;border:1px solid #2a2a4a;">';
        h += '<div style="display:flex;align-items:center;gap:10px;">';
        h += '<div style="flex:1;height:20px;background:#111827;border-radius:10px;overflow:hidden;position:relative;">';
        h += '<div id="recProgressFill" style="height:100%;width:0%;border-radius:10px;transition:width 0.5s ease;background:linear-gradient(90deg,#3b82f6,#8b5cf6);"></div>';
        h += '<span id="recProgressText" style="position:absolute;top:0;left:0;right:0;text-align:center;line-height:20px;font-size:11px;color:#fff;font-weight:600;text-shadow:0 1px 2px rgba(0,0,0,0.5);">启动中...</span>';
        h += '</div></div></div>';
        h += '<div id="recRunHistory" style="margin-bottom:14px;"></div>';
        // 筛选条件栏
        h += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap;">';
        h += '<label style="font-size:13px;color:#888;">策略:</label>';
        h += '<input id="recStartDate" type="date" value="' + escAttr(data.start_date || '') + '" style="padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:13px;">';
        h += '<span style="font-size:12px;color:#888;">-</span>';
        h += '<input id="recEndDate" type="date" value="' + escAttr(data.end_date || '') + '" style="padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:13px;">';
        h += '<button onclick="window._applyRecDateRange()" style="padding:6px 12px;border:1px solid #1a73e8;border-radius:6px;background:#fff;color:#1a73e8;font-size:13px;cursor:pointer;">查询</button>';
        h += '<button onclick="window._clearRecDateRange()" style="padding:6px 12px;border:1px solid #ddd;border-radius:6px;background:#fff;color:#666;font-size:13px;cursor:pointer;">清空</button>';
        h += '<select id="recFilterStrategy" style="padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:13px;" onchange="window._setRecStrategy(this.value)">';
        h += '<option value="all"' + (currentStrategy === 'all' ? ' selected' : '') + '>全部</option>';
        h += '<option value="ultra_short"' + (currentStrategy === 'ultra_short' ? ' selected' : '') + '>超短</option>';
        h += '<option value="short_term"' + (currentStrategy === 'short_term' ? ' selected' : '') + '>短线</option>';
        h += '<option value="swing"' + (currentStrategy === 'swing' ? ' selected' : '') + '>波段</option>';
        h += '<option value="main_wave"' + (currentStrategy === 'main_wave' ? ' selected' : '') + '>主升浪</option>';
        h += '</select>';
        h += '<label style="font-size:13px;color:#888;">最低分数:</label>';
        h += '<input id="recMinScoreInput" type="number" min="0" max="100" step="5" value="0" style="width:80px;padding:6px 10px;border:1px solid #ddd;border-radius:6px;font-size:13px;" oninput="window._filterRecommendedAll()">';
        h += '<label style="font-size:13px;color:#888;">状态:</label>';
        h += '<select id="recFilterStatus" style="padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:13px;" onchange="window._filterRecommendedAll()">';
        h += '<option value="">全部</option>';
        h += '<option value="WATCH">观察</option>';
        h += '<option value="BUY_READY">买入就绪</option>';
        h += '<option value="SELL_ALERT">卖出提醒</option>';
        h += '</select>';
        h += '<label style="font-size:13px;color:#888;">主升信号:</label>';
        h += '<select id="recFilterMainWave" style="padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:13px;" onchange="window._filterRecommendedAll()">';
        h += '<option value="">全部</option>';
        h += '<option value="BUY_READY">买点</option>';
        h += '<option value="WATCH">观察</option>';
        h += '<option value="REDUCE">减仓</option>';
        h += '<option value="SELL_ALERT">卖出</option>';
        h += '</select>';
        h += '<label style="font-size:13px;color:#888;">买入建议:</label>';
        h += '<select id="recFilterAdvice" style="padding:6px 8px;border:1px solid #ddd;border-radius:6px;font-size:13px;" onchange="window._filterRecommendedAll()">';
        h += '<option value="">全部</option>';
        h += '<option value="buy_ready">买点就绪</option>';
        h += '<option value="recommend">推荐买入</option>';
        h += '<option value="caution">谨慎买入</option>';
        h += '<option value="watch">观望</option>';
        h += '<option value="reduce">减仓</option>';
        h += '<option value="sell">卖出提醒</option>';
        h += '<option value="block">不推荐</option>';
        h += '<option value="suspended">暂停</option>';
        h += '</select>';
        h += '<button onclick="window._filterRecommendedAll()" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:12px;cursor:pointer;">筛选</button>';
        h += '</div>';

        if (!items.length) {
            h += '<div style="text-align:center;padding:60px 20px;color:#888;">';
            h += '<div style="font-size:48px;margin-bottom:16px;">💎</div>';
            h += '<div style="font-size:16px;font-weight:600;color:#555;margin-bottom:8px;">当前无买入信号</div>';
            h += '<div style="font-size:13px;color:#999;margin-bottom:8px;">系统已完成筛选，但没有股票满足买点/观察候选条件。</div>';
            h += '<div style="font-size:12px;color:#aaa;">筛选日期: ' + dateInfo + '；卖出提醒和风险票不会再占用 AI推荐买入榜。</div>';
            h += '</div>';
            // 进度条（空数据时也需要显示）
            h += '<div id="recProgressBar" style="display:none;margin-bottom:14px;padding:10px 14px;background:#1a1a2e;border-radius:8px;border:1px solid #2a2a4a;">';
            h += '<div style="display:flex;align-items:center;gap:10px;">';
            h += '<div style="flex:1;height:20px;background:#111827;border-radius:10px;overflow:hidden;position:relative;">';
            h += '<div id="recProgressFill" style="height:100%;width:0%;border-radius:10px;transition:width 0.5s ease;background:linear-gradient(90deg,#3b82f6,#8b5cf6);"></div>';
            h += '<span id="recProgressText" style="position:absolute;top:0;left:0;right:0;text-align:center;line-height:20px;font-size:11px;color:#fff;font-weight:600;text-shadow:0 1px 2px rgba(0,0,0,0.5);">启动中...</span>';
            h += '</div></div></div>';
            container.innerHTML = h;
            _resumeRecommendedProgress();
            return;
        }

        // 表格
        h += '<div style="overflow-x:auto;">';
        h += '<table class="data-table" style="width:100%;">';
        h += '<thead><tr>';
        h += '<th style="width:40px;">序号</th>';
        h += '<th>代码</th><th>名称</th>';
        if (hasNewFormat) {
            h += '<th>策略</th><th>状态</th><th>评级</th><th>交易分</th><th>质量</th><th>买点</th><th>空间</th><th>盈亏比</th><th>板块</th><th>证据</th><th>复盘</th><th>拥挤</th><th>可信</th><th>筹码</th><th>买入区间</th><th>止损</th><th>止盈</th><th>仓位</th><th>超短</th><th>波段</th><th>主升</th><th>持有</th><th>主升信号</th><th>趋势止损</th>';
        }

        if (hasNewFormat) {
            h += '<th style="width:90px;">买入建议</th><th>短线评分</th><th>长线评分</th>';
            h += '<th>资金面</th><th>技术面</th><th>情绪面</th>';
            h += '<th>市场情绪</th><th>消息面</th><th>风险等级</th>';
        } else {
            h += '<th>综合评分</th><th>基本面</th><th>资金面</th><th>估值</th><th>技术面</th>';
        }

        h += '<th>推荐理由</th><th>来源策略</th>';
        h += '<th>操作</th>';
        h += '</tr></thead><tbody>';

        items.forEach(function(r, i) {
            var scoreColor = function(s) { return s >= 70 ? '#e74c3c' : s >= 50 ? '#f39c12' : '#27ae60'; };
            var statusColors = {ALLOW: '#27ae60', SUSPENDED: '#f39c12', BLOCK: '#e74c3c'};
            var statusLabels = {ALLOW: '可跟踪', SUSPENDED: '观察', BLOCK: '回避'};
            var riskColors = {LOW: '#27ae60', MEDIUM: '#f39c12', HIGH: '#e74c3c', CRITICAL: '#c0392b'};
            var activeScore = recScoreFor(r, currentStrategy);
            var rowScore = Math.round(activeScore);

            var primaryStrategy = r.primary_strategy || r.strategy_profile || 'short_term';
            var signalStatus = r.signal_status || r.recommend_status || 'WATCH';
            var rowStatus = r.recommend_status || 'ALLOW';
            var stScore = firstAnalysisValue(r.short_term_score, blendedAnalysisRowScore(r), 0) || 0;
            var rowMainWave = r.main_wave_signal || 'NONE';
            var rowAdvice = 'watch';
            if (signalStatus === 'SELL_ALERT' || r.main_wave_signal === 'SELL_ALERT') { rowAdvice = 'sell'; }
            else if (r.main_wave_signal === 'REDUCE') { rowAdvice = 'reduce'; }
            else if (signalStatus === 'BUY_READY' || r.main_wave_signal === 'BUY_READY') { rowAdvice = 'buy_ready'; }
            else if (rowStatus === 'BLOCK') { rowAdvice = 'block'; }
            else if (rowStatus === 'SUSPENDED') { rowAdvice = 'suspended'; }
            else if (stScore >= 70) { rowAdvice = 'recommend'; }
            else if (stScore >= 60) { rowAdvice = 'caution'; }
            h += '<tr data-score="' + rowScore + '" data-status="' + signalStatus + '" data-mainwave="' + rowMainWave + '" data-advice="' + rowAdvice + '" class="rec-row">';
            h += '<td style="text-align:center;color:#888;">' + (i + 1) + '</td>';
            h += '<td style="font-family:monospace;font-weight:600;">' + r.stock_code + '</td>';
            h += '<td>' + (r.short_name || '-') + '</td>';

            if (hasNewFormat) {
                var signalColor = recStatusColor(signalStatus);
                h += '<td style="text-align:center;"><span style="display:inline-block;padding:3px 8px;border-radius:6px;background:#eef4ff;color:#1a73e8;font-size:12px;font-weight:700;white-space:nowrap;">' + recStrategyLabel(primaryStrategy) + '</span></td>';
                h += '<td style="text-align:center;"><span style="display:inline-block;padding:3px 8px;border-radius:6px;background:' + signalColor + ';color:#fff;font-size:12px;font-weight:700;white-space:nowrap;" title="' + escAttr(localizeMachineText(r.signal_reason || '')) + '">' + recStatusLabel(signalStatus) + '</span></td>';
                var rating = r.investment_rating || '中性';
                h += '<td style="text-align:center;"><span style="display:inline-block;padding:3px 8px;border-radius:6px;background:#fff;color:' + recRatingColor(rating) + ';border:1px solid ' + recRatingColor(rating) + '55;font-size:12px;font-weight:900;white-space:nowrap;" title="' + escAttr(localizeMachineText(r.rating_reason || '')) + '">' + safeText(rating) + '</span></td>';
                h += '<td style="text-align:center;font-weight:800;color:' + scoreColor(r.final_trade_score) + ';" title="final_trade_score / ai_score">' + (r.final_trade_score != null ? Math.round(r.final_trade_score) : '-') + '<div style="font-size:10px;color:#64748b;font-weight:600">AI ' + (r.ai_score != null ? Math.round(r.ai_score) : '-') + '</div></td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.quality_score) + ';">' + (r.quality_score != null ? Math.round(r.quality_score) : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.entry_score) + ';">' + (r.entry_score != null ? Math.round(r.entry_score) : '-') + '</td>';
                h += '<td style="text-align:center;font-family:monospace;white-space:nowrap;" title="压力位 ' + recPrice(r.resistance_price) + '">' + (r.expected_return_pct != null ? Number(r.expected_return_pct).toFixed(1) + '%' : '-') + '</td>';
                var rr = Number(r.risk_reward_ratio || 0);
                var rrColor = rr >= 3 ? '#16a34a' : (rr > 0 ? '#dc2626' : '#64748b');
                h += '<td style="text-align:center;font-family:monospace;font-weight:800;color:' + rrColor + ';">' + (rr > 0 ? rr.toFixed(2) + ':1' : '-') + '</td>';
                var sectorStatus = r.sector_gate_status || 'WATCH';
                h += '<td style="text-align:center;"><span style="display:inline-block;padding:3px 8px;border-radius:6px;background:#fff;color:' + recSectorColor(sectorStatus) + ';border:1px solid ' + recSectorColor(sectorStatus) + '55;font-size:12px;font-weight:800;white-space:nowrap;" title="' + escAttr(localizeMachineText(r.sector_gate_reason || '')) + '">' + recSectorLabel(sectorStatus) + '</span></td>';
                h += '<td style="text-align:center;"><span style="display:inline-block;padding:3px 8px;border-radius:6px;background:#f8fafc;color:#475569;border:1px solid #cbd5e1;font-size:12px;font-weight:700;white-space:nowrap;" title="' + escAttr(recEvidenceText(r)) + '">查看</span></td>';
                var reviewText = recReviewText(r);
                var reviewColor = reviewText.indexOf('-') === 0 ? '#64748b' : (Number(r.review_5d_pct || r.review_3d_pct || r.review_1d_pct || 0) >= 0 ? '#16a34a' : '#dc2626');
                h += '<td style="text-align:center;font-family:monospace;font-size:12px;color:' + reviewColor + ';white-space:nowrap;" title="' + escAttr(reviewText) + '">' + reviewText.split(' / ').slice(0, 2).join('<br>') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.heat_overload_score) + ';">' + (r.heat_overload_score != null ? Math.round(r.heat_overload_score) : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.confidence_score) + ';">' + (r.confidence_score != null ? Math.round(r.confidence_score) : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.chip_capital_score) + ';">' + (r.chip_capital_score != null ? Math.round(r.chip_capital_score) : '-') + '</td>';
                h += '<td style="text-align:center;font-family:monospace;font-size:12px;white-space:nowrap;">' + recRange(r.entry_price_low, r.entry_price_high) + '</td>';
                h += '<td style="text-align:center;font-family:monospace;color:#27ae60;">' + recPrice(r.stop_loss_price) + '</td>';
                h += '<td style="text-align:center;font-family:monospace;color:#e74c3c;white-space:nowrap;">' + recPrice(r.take_profit_1) + '/' + recPrice(r.take_profit_2) + '</td>';
                h += '<td style="text-align:center;font-weight:700;">' + (r.position_weight != null ? Number(r.position_weight).toFixed(1) + '%' : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.ultra_short_score) + ';">' + (r.ultra_short_score != null ? Math.round(r.ultra_short_score) : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.swing_score) + ';">' + (r.swing_score != null ? Math.round(r.swing_score) : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.main_wave_score) + ';">' + (r.main_wave_score != null ? Math.round(r.main_wave_score) : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.trend_hold_score) + ';">' + (r.trend_hold_score != null ? Math.round(r.trend_hold_score) : '-') + '</td>';
                h += '<td style="text-align:center;"><span style="display:inline-block;padding:3px 8px;border-radius:6px;background:' + recMainWaveColor(r.main_wave_signal) + ';color:#fff;font-size:12px;font-weight:700;white-space:nowrap;" title="' + escAttr(localizeMachineText(r.main_wave_reason || '')) + '">' + recMainWaveLabel(r.main_wave_signal) + '</span></td>';
                h += '<td style="text-align:center;font-family:monospace;color:#27ae60;">' + recPrice(r.trend_stop_price) + '</td>';
                // 推荐状态 - 醒目的买入建议标签
                var badgeText, badgeBg, badgeColor;
                if (signalStatus === 'SELL_ALERT' || r.main_wave_signal === 'SELL_ALERT') {
                    badgeText = '🔻 卖出提醒'; badgeBg = 'linear-gradient(135deg,#922b21,#e74c3c)'; badgeColor = '#fff';
                } else if (r.main_wave_signal === 'REDUCE') {
                    badgeText = '⚠️ 减仓'; badgeBg = 'linear-gradient(135deg,#ba4a00,#e67e22)'; badgeColor = '#fff';
                } else if (signalStatus === 'BUY_READY' || r.main_wave_signal === 'BUY_READY') {
                    badgeText = '✅ 买点就绪'; badgeBg = 'linear-gradient(135deg,#1e8449,#27ae60)'; badgeColor = '#fff';
                } else if (rowStatus === 'BLOCK') {
                    badgeText = '❌ 不推荐'; badgeBg = 'linear-gradient(135deg,#c0392b,#e74c3c)'; badgeColor = '#fff';
                } else if (rowStatus === 'SUSPENDED') {
                    badgeText = '⚠️ 暂停'; badgeBg = 'linear-gradient(135deg,#d4a017,#f39c12)'; badgeColor = '#fff';
                } else if (stScore >= 70) {
                    badgeText = '✅ 推荐买入'; badgeBg = 'linear-gradient(135deg,#1e8449,#27ae60)'; badgeColor = '#fff';
                } else if (stScore >= 60) {
                    badgeText = '⚡ 谨慎买入'; badgeBg = 'linear-gradient(135deg,#b7950b,#f1c40f)'; badgeColor = '#fff';
                } else {
                    badgeText = '⏸ 观望'; badgeBg = 'linear-gradient(135deg,#555,#888)'; badgeColor = '#fff';
                }
                h += '<td style="text-align:center;"><span style="background:' + badgeBg + ';color:' + badgeColor + ';padding:4px 10px;border-radius:6px;font-size:12px;font-weight:700;white-space:nowrap;display:inline-block;">' + badgeText + '</span></td>';

                // 短线评分
                h += '<td style="text-align:center;font-weight:700;font-size:16px;color:' + scoreColor(r.short_term_score) + ';">' + (r.short_term_score != null ? Math.round(r.short_term_score) : '-') + '</td>';

                // 长线评分
                h += '<td style="text-align:center;font-weight:600;color:' + scoreColor(r.long_term_score) + ';">' + (r.long_term_score != null ? Math.round(r.long_term_score) : '-') + '</td>';

                // 资金面
                h += '<td style="text-align:center;color:' + scoreColor(r.capital_score) + ';">' + (r.capital_score != null ? Math.round(r.capital_score) : '-') + '</td>';

                // 技术面
                h += '<td style="text-align:center;color:' + scoreColor(r.technical) + ';">' + (r.technical != null ? Math.round(r.technical) : '-') + '</td>';

                // 情绪面
                h += '<td style="text-align:center;color:' + scoreColor(r.sentiment_score) + ';">' + (r.sentiment_score != null ? Math.round(r.sentiment_score) : '-') + '</td>';

                // 市场情绪
                h += '<td style="text-align:center;color:' + scoreColor(r.market_mood_score) + ';">' + (r.market_mood_score != null ? Math.round(r.market_mood_score) : '-') + '</td>';

                // 消息面
                h += '<td style="text-align:center;color:' + scoreColor(r.event_score) + ';">' + (r.event_score != null ? Math.round(r.event_score) : '-') + '</td>';

                // 风险等级
                var riskLevel = r.event_risk_level || 'LOW';
                var riskColor = riskColors[riskLevel] || '#666';
                h += '<td style="text-align:center;"><span style="color:' + riskColor + ';font-weight:600;">' + localizeRiskLevel(riskLevel) + '</span></td>';
            } else {
                // 旧格式
                var legacyScore = blendedAnalysisRowScore(r);
                h += '<td style="text-align:center;font-weight:700;font-size:16px;color:' + scoreColor(legacyScore || 0) + ';">' + (legacyScore != null ? legacyScore.toFixed(1) : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.fundamental) + ';">' + (r.fundamental != null ? r.fundamental : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.capital_score) + ';">' + (r.capital_score != null ? r.capital_score : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.valuation) + ';">' + (r.valuation != null ? r.valuation : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.technical) + ';">' + (r.technical != null ? r.technical : '-') + '</td>';
            }

            h += '<td style="font-size:12px;color:#666;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + escAttr(localizeMachineText(r.reason || '')) + '">' + localizeMachineText(r.reason || '-') + '</td>';
            h += '<td style="font-size:11px;color:#888;">' + (r.sources || '-') + '</td>';
            h += '<td style="text-align:center;"><button onclick="window.openStockDetail(\'' + r.stock_code + '\')" style="padding:3px 10px;border:1px solid #1a73e8;border-radius:4px;background:transparent;color:#1a73e8;font-size:11px;cursor:pointer;">详情</button></td>';
            h += '</tr>';
        });

        h += '</tbody></table></div>';
        container.innerHTML = h;
        _filterRecommendedAll();
        _resumeRecommendedProgress();
        _loadRecommendedRunHistory();
        window._startRecommendedAutoRefresh();
    }

    function _loadRecommendedRunHistory() {
        var mount = document.getElementById('recRunHistory');
        if (!mount) return;
        fetch('/api/hot-data/recommended-stocks/run-history?limit=8')
            .then(function(r) { return r.json(); })
            .then(function(res) {
                var rows = res.data || [];
                if (!rows.length) {
                    mount.innerHTML = '<div style="padding:10px 12px;background:#fff;border:1px solid #eee;border-radius:8px;color:#888;font-size:12px;">暂无筛选执行记录</div>';
                    return;
                }
                function statusText(s) {
                    if (s === 'done') return '<span style="color:#16a34a;font-weight:700">完成</span>';
                    if (s === 'error') return '<span style="color:#dc2626;font-weight:700">失败</span>';
                    if (s === 'queued') return '<span style="color:#a16207;font-weight:700">排队中</span>';
                    return '<span style="color:#2563eb;font-weight:700">运行中</span>';
                }
                var h = '<div style="background:#fff;border:1px solid #eee;border-radius:8px;overflow:hidden;">';
                h += '<div style="padding:10px 12px;font-size:13px;font-weight:800;border-bottom:1px solid #eee;">最近执行结果</div>';
                h += '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;"><thead><tr style="background:#f8fafc;">';
                h += '<th style="padding:8px;text-align:left;">开始时间</th><th style="padding:8px;text-align:center;">状态</th><th style="padding:8px;text-align:center;">日期</th><th style="padding:8px;text-align:center;">通过/总数</th><th style="padding:8px;text-align:center;">耗时</th><th style="padding:8px;text-align:left;">结果</th>';
                h += '</tr></thead><tbody>';
                rows.forEach(function(row) {
                    var started = row.started_at ? String(row.started_at).replace('T', ' ').slice(0, 19) : '-';
                    var countText = (row.passed != null ? row.passed : '-') + ' / ' + (row.total != null ? row.total : '-');
                    var duration = row.duration_seconds != null ? row.duration_seconds + 's' : '-';
                    var result = localizeMachineText(row.error || row.message || '');
                    if (!result && (row.flow_date || row.hot_date)) result = '资金 ' + (row.flow_date || '-') + ' / 热度 ' + (row.hot_date || '-');
                    h += '<tr style="border-bottom:1px solid #f1f5f9;">';
                    h += '<td style="padding:8px;color:#475569;">' + started + '</td>';
                    h += '<td style="padding:8px;text-align:center;">' + statusText(row.status) + '</td>';
                    h += '<td style="padding:8px;text-align:center;color:#475569;">' + (row.trade_date || '-') + '</td>';
                    h += '<td style="padding:8px;text-align:center;color:#475569;">' + countText + '</td>';
                    h += '<td style="padding:8px;text-align:center;color:#475569;">' + duration + '</td>';
                    h += '<td style="padding:8px;color:#64748b;max-width:360px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="' + escAttr(result) + '">' + (result || '-') + '</td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div></div>';
                mount.innerHTML = h;
            })
            .catch(function() {});
    }

    function _updateRecommendedProgressUI(progress) {
        var barEl = document.getElementById('recProgressBar');
        var barFillEl = document.getElementById('recProgressFill');
        var barTextEl = document.getElementById('recProgressText');
        var statusEl = document.getElementById('recStatus');
        if (!barEl || !barFillEl || !barTextEl) return;
        if (!progress || progress.status === 'idle') {
            if (statusEl) statusEl.innerHTML = '';
            return;
        }
        var pct = progress.percent || 0;
        barEl.style.display = 'block';
        barFillEl.style.width = pct + '%';
        barFillEl.style.background = progress.status === 'error' ? '#ef4444' : (pct >= 100 ? '#22c55e' : 'linear-gradient(90deg,#3b82f6,#8b5cf6)');
        var stepText = localizeMachineText(progress.step || progress.error || '处理中...');
        barTextEl.textContent = stepText + '  ' + pct + '%';
        if (statusEl) {
            var extra = progress.trade_date ? (' | 日期 ' + progress.trade_date) : '';
            statusEl.innerHTML = (progress.status === 'done' ? '✅ ' : progress.status === 'error' ? '❌ ' : '⏳ ') + stepText + extra;
        }
    }

    function _resumeRecommendedProgress() {
        fetch('/api/hot-data/recommended-stocks/progress')
            .then(function(r) { return r.json(); })
            .then(function(progress) {
                _updateRecommendedProgressUI(progress);
                if (progress && (progress.is_running || progress.status === 'queued')) {
                    _pollRecProgress();
                }
            })
            .catch(function() {});
    }

    window._setRecStrategy = function(strategy) {
        window._recStrategy = strategy || 'all';
        // 同步下拉框
        var sel = document.getElementById('recFilterStrategy');
        if (sel) sel.value = window._recStrategy;
        var c = document.getElementById('tab-recommended');
        if (c && window._recLastData) {
            renderRecommendedPage(c, window._recLastData, window._recLastDate || '');
        }
    };

    window._runRecommendedScreen = function() {
        var statusEl = document.getElementById('recStatus');
        var barEl = document.getElementById('recProgressBar');
        var barFillEl = document.getElementById('recProgressFill');
        var barTextEl = document.getElementById('recProgressText');
        if (statusEl) statusEl.innerHTML = '⏳ 正在启动筛选...';
        if (barEl) barEl.style.display = 'block';
        if (barFillEl) barFillEl.style.width = '0%';
        if (barTextEl) barTextEl.textContent = '启动中...';
        // 读取输入框的最低分数，默认50
        var scoreInput = document.getElementById('recMinScoreInput');
        var minScore = (scoreInput && scoreInput.value) ? parseInt(scoreInput.value) : 50;
        var tradeDate = currentDateValue();
        var executionTime = localDateTimeString(new Date());
        fetch('/api/hot-data/recommended-stocks/run?trade_date=' + encodeURIComponent(tradeDate) + '&min_score=' + minScore + '&top_n=80&strict_prev_trade_day=false&execution_time=' + encodeURIComponent(executionTime) + '&min_kline_coverage=0.80&auto_repair_missing_kline=true&refresh_realtime=true', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(res) {
                if (res.status === 'protected' || res.status === 'error') {
                    var protectedProgress = res.progress || { status: res.status, step: res.note || res.error || 'AI 推荐暂不可用。', percent: 0, trade_date: tradeDate };
                    if (statusEl) statusEl.textContent = (res.status === 'protected' ? '已保护：' : '启动失败：') + localizeMachineText(protectedProgress.step || res.note || res.error || '');
                    if (barEl) barEl.style.display = 'block';
                    if (barFillEl) {
                        barFillEl.style.width = '100%';
                        barFillEl.style.background = '#ef4444';
                    }
                    if (barTextEl) barTextEl.textContent = localizeMachineText(protectedProgress.step || res.note || res.error || '');
                    _updateRecommendedProgressUI(protectedProgress);
                    _loadRecommendedRunHistory();
                    return;
                }
                if (res.status === 'queued') {
                    if (statusEl) statusEl.textContent = '已入队：' + ((res.progress || {}).step || res.note || 'AI 推荐任务已进入队列');
                    _updateRecommendedProgressUI(res.progress || { status: 'queued', step: res.note || 'AI 推荐任务已进入队列', percent: 5, trade_date: tradeDate });
                    _loadRecommendedRunHistory();
                    _pollRecProgress();
                    return;
                }
                if (statusEl) statusEl.innerHTML = '⏳ 筛选进行中...';
                _updateRecommendedProgressUI(res.progress || { status: res.status === 'running' ? 'running' : 'started', step: res.note || '筛选已启动', percent: 0, trade_date: tradeDate });
                _loadRecommendedRunHistory();
                // 启动进度轮询
                _pollRecProgress();
            })
            .catch(function() {
                if (statusEl) statusEl.innerHTML = '❌ 启动失败';
                if (barEl) barEl.style.display = 'none';
            });
    };

    function _pollRecProgress() {
        if (window._recProgressTimer) clearInterval(window._recProgressTimer);
        var pollCount = 0;
        window._recProgressTimer = setInterval(function() {
            pollCount++;
            fetch('/api/hot-data/recommended-stocks/progress').then(function(r) { return r.json(); }).then(function(p) {
                var barEl = document.getElementById('recProgressBar');
                var barFillEl = document.getElementById('recProgressFill');
                var barTextEl = document.getElementById('recProgressText');
                var statusEl = document.getElementById('recStatus');
                if (!barFillEl || !barTextEl) return;
                var pct = p.percent || 0;
                barFillEl.style.width = pct + '%';
                barFillEl.style.background = pct >= 100 ? '#22c55e' : 'linear-gradient(90deg,#3b82f6,#8b5cf6)';
                barTextEl.textContent = (p.step || '') + '  ' + pct + '%';
                _updateRecommendedProgressUI(p);
                if (p.status === 'done') {
                    clearInterval(window._recProgressTimer);
                    _loadRecommendedRunHistory();
                    if (statusEl) statusEl.innerHTML = '✅ ' + (p.step || '筛选完成');
                    // 加载结果
                    setTimeout(function() {
                        var c = document.getElementById('tab-recommended');
                        if (c) loadRecommendedPage(currentDateValue(), c);
                        // 3秒后隐藏进度条
                        setTimeout(function() { if (barEl) barEl.style.display = 'none'; }, 3000);
                    }, 500);
                } else if (p.status === 'error' || p.status === 'protected') {
                    clearInterval(window._recProgressTimer);
                    _loadRecommendedRunHistory();
                    if (statusEl) statusEl.innerHTML = '❌ ' + (p.step || '筛选失败');
                    barFillEl.style.background = '#ef4444';
                }
            }).catch(function() {});
            // 全市场筛选常见耗时 15-30 分钟，保留更长轮询窗口。
            if (pollCount > 2700) clearInterval(window._recProgressTimer);
        }, 2000);
    }

    // 综合筛选推荐结果（分数 + 状态 + 主升信号 + 买入建议）
    window._filterRecommendedAll = function() {
        var minScore = parseInt((document.getElementById('recMinScoreInput') || {}).value) || 0;
        var filterStatus = (document.getElementById('recFilterStatus') || {}).value || '';
        var filterMainWave = (document.getElementById('recFilterMainWave') || {}).value || '';
        var filterAdvice = (document.getElementById('recFilterAdvice') || {}).value || '';
        var rows = document.querySelectorAll('.rec-row');
        var visibleCount = 0;
        rows.forEach(function(row) {
            var score = parseInt(row.getAttribute('data-score')) || 0;
            var status = row.getAttribute('data-status') || '';
            var mainWave = row.getAttribute('data-mainwave') || '';
            var advice = row.getAttribute('data-advice') || '';
            var show = true;
            if (score < minScore) show = false;
            if (filterStatus && status !== filterStatus) show = false;
            if (filterMainWave && mainWave !== filterMainWave) show = false;
            if (filterAdvice && advice !== filterAdvice) show = false;
            row.style.display = show ? '' : 'none';
            if (show) visibleCount++;
        });
        var countEl = document.getElementById('recFilteredCount');
        if (countEl) countEl.textContent = visibleCount;
    };
    // 兼容旧调用
    window._filterRecommendedByScore = window._filterRecommendedAll;

    /* ===== 模拟交易页面 ===== */
    function simTradeModeNav(activeMode) {
        function btn(mode, label, target, color) {
            var active = activeMode === mode;
            return '<button class="sim-mode-btn ' + (active ? 'active' : '') + '" style="--sim-accent:' + color + ';" onclick="' + target + '">' + label + '</button>';
        }
        var h = '<div class="sim-mode-nav">';
        h += btn('live', '今日模拟', "simTradeSetMode('live')", '#1a73e8');
        h += btn('forward', 'T+1验证', "simTradeSetMode('forward')", '#34495e');
        h += btn('backtest', '策略回测', "simTradeSetMode('backtest')", '#e67e22');
        h += btn('recommended', 'AI推荐', "switchTab('recommended')", '#8e44ad');
        h += '<span class="sim-mode-note">AI推荐 → 模拟交易 → 策略回测</span>';
        h += '</div>';
        return h;
    }

    function loadSimTradePage(container, mode) {
        container.innerHTML = '<div class="loading"><span class="spinner"></span> 加载模拟交易数据...</div>';
        var SIM_API = '/api/sim-trade';
        mode = mode || window._simTradeMode || 'live';
        window._simTradeMode = mode;
        var modeQuery = 'trade_mode=' + encodeURIComponent(mode);
        var strategyOrder = ['ultra_short', 'short_term', 'swing', 'main_wave'];
        var strategyNames = {ultra_short: '超短', short_term: '短线', swing: '波段', main_wave: '主升浪'};
        var strategyColors = {ultra_short: '#e74c3c', short_term: '#f39c12', swing: '#3498db', main_wave: '#8e44ad'};
        var strategyCaps = {ultra_short: 3, short_term: 3, swing: 2, main_wave: 2};
        var strategyLabels = {
            ultra_short: '超短快进快出',
            short_term: '短线趋势跟随',
            swing: '波段持有',
            main_wave: '主升趋势持有'
        };
        var strategyHints = {
            ultra_short: '持有 1 到 3 天，强调强势与节奏。',
            short_term: '持有 3 到 10 天，跟随短线趋势延续。',
            swing: '持有 10 到 30 天，兼看波段机会与基本面。',
            main_wave: '只做主升段，重视趋势是否仍在。'
        };
        var modeMeta = {
            live: {title: '今日 AI 模拟交易', desc: '先看买点、仓位和风险，再决定是否扫描或平仓。当前只做模拟交易，不会真实下单。'},
            forward: {title: 'T+1 验证回放', desc: '用推荐日之后的数据验证次日买入后的表现，主要校验推荐节奏是否可靠。'},
            backtest: {title: '历史策略回测', desc: '批量回放历史推荐，统计胜率、收益、回撤和资金效率。'}
        };

        function num(v) { var n = Number(v || 0); return isNaN(n) ? 0 : n; }
        function fmtPnl(v) { var n = num(v); return '<span class="' + (n >= 0 ? 'c-red' : 'c-green') + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '</span>'; }
        function fmtRate(v) { var n = num(v); return '<span class="' + (n >= 0 ? 'c-red' : 'c-green') + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '%</span>'; }
        function fmtPlainRate(v) { var n = num(v); return (n >= 0 ? '+' : '') + n.toFixed(2) + '%'; }
        function fmtRatio(v) { var n = num(v); return n > 0 ? n.toFixed(2) : '-'; }
        function fmtMoney(v) { var n = num(v); if (Math.abs(n) >= 1e4) return (n / 1e4).toFixed(2) + '万'; return n.toFixed(0); }
        function fmtCellPrice(v) { var n = num(v); return n > 0 ? n.toFixed(2) : '-'; }
        function safeText(v) { return escHtml(v == null ? '' : v); }
        function safeAttr(v) { return escAttr(v == null ? '' : v); }
        function jsString(v) { return String(v == null ? '' : v).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); }
        function stockLink(code, name) {
            var display = name || code || '-';
            return '<a href="javascript:void(0)" onclick="openKlineModal(\'' + jsString(code) + '\',\'' + jsString(display) + '\')" class="clickable-name">' + safeText(display) + '</a>';
        }
        function parseReason(v) {
            if (!v) return '-';
            if (typeof v === 'string' && v.charAt(0) === '{') {
                try {
                    var o = JSON.parse(v);
                    return localizeMachineText(o.reason || o.sell_reason || v);
                } catch(e) {
                    return localizeMachineText(v);
                }
            }
            return localizeMachineText(v);
        }
        var sellReasonMap = {take_profit:'止盈', stop_loss:'止损', time_limit:'时间止损', trailing_stop:'动态止盈', manual_close:'手动平仓'};
        function fmtSellReason(v) { if (!v) return '-'; return sellReasonMap[v] || v; }
        function strategyTitle(st, fallback) { return strategyLabels[st] || strategyNames[st] || fallback || st; }
        function toneClass(tone) { return tone ? ' sim-tone-' + tone : ''; }
        function metricCard(label, value, note, tone) {
            return '<div class="sim-metric' + toneClass(tone) + '"><div class="sim-metric-label">' + safeText(label) + '</div><div class="sim-metric-value">' + value + '</div><div class="sim-metric-note">' + safeText(note || '') + '</div></div>';
        }
        function sectionHead(title, meta) {
            return '<div class="sim-section-head"><h3>' + safeText(title) + '</h3>' + (meta ? '<span>' + safeText(meta) + '</span>' : '') + '</div>';
        }
        function emptyState(text) {
            return '<div class="sim-empty">' + safeText(text) + '</div>';
        }
        function simActionTag(action, label) {
            var tone = action === 'BUY_READY' ? 'good' : (action === 'SELL_ALERT' ? 'bad' : 'warn');
            return '<span class="sim-tag sim-tag-' + tone + '">' + safeText(label || action || '-') + '</span>';
        }
        function statusCard(label, value, note, tone) {
            return '<div class="sim-status-card' + toneClass(tone) + '"><div class="sim-status-label">' + safeText(label) + '</div><div class="sim-status-value">' + safeText(value || '-') + '</div><div class="sim-status-note">' + safeText(note || '') + '</div></div>';
        }
        function candidateGroupCard(title, rows, tone, emptyText) {
            var out = '<div class="sim-candidate-card sim-tone-' + tone + '">';
            out += '<div class="sim-candidate-card-head"><strong>' + safeText(title) + '</strong><span>' + rows.length + ' 只</span></div>';
            var showRows = rows.slice(0, 4);
            if (!showRows.length) {
                out += emptyState(emptyText);
            } else {
                showRows.forEach(function(r) {
                    var st = r.preferred_strategy || r.primary_strategy || '';
                    var score = blendedAnalysisRowScore(r);
                    var entryRange = (num(r.entry_price_low) > 0 && num(r.entry_price_high) > 0)
                        ? fmtCellPrice(r.entry_price_low) + ' ~ ' + fmtCellPrice(r.entry_price_high)
                        : '-';
                    out += '<div class="sim-candidate-row">';
                    out += '<div><div class="sim-row-title">' + stockLink(r.stock_code, r.short_name) + '</div><div class="sim-row-meta">' + safeText(r.stock_code || '-') + ' · ' + safeText(strategyNames[st] || r.preferred_strategy_name || '-') + '</div></div>';
                    out += '<div class="sim-row-score">' + fmtCellPrice(score) + '<span>综合分</span></div>';
                    out += '<div class="sim-row-note">买入区间 ' + safeText(entryRange) + ' · 止损 ' + safeText(fmtCellPrice(r.stop_loss_price || r.trend_stop_price)) + '</div>';
                    out += '</div>';
                });
                if (rows.length > showRows.length) out += '<div class="sim-more">还有 ' + (rows.length - showRows.length) + ' 只，展开明细表查看</div>';
            }
            out += '</div>';
            return out;
        }
        function holdingRiskTag(ph) {
            var pnl = num(ph.pnl_rate);
            var days = num(ph.holding_days);
            var score = num(blendedAnalysisRowScore(ph));
            if (pnl <= -5 || score < 55) return {text:'高风险', tone:'bad'};
            if (pnl <= -2 || days >= 10) return {text:'需复核', tone:'warn'};
            return {text:'正常', tone:'good'};
        }
        function holdingShares(ph) {
            return num(ph.buy_shares != null ? ph.buy_shares : ph.shares);
        }
        function buildHoldingGroups(rows) {
            var map = {};
            rows.forEach(function(ph) {
                var st = ph.strategy_type || '';
                var code = String(ph.stock_code || '').padStart(6, '0');
                var key = st + '|' + code;
                var shares = holdingShares(ph);
                var buyPrice = num(ph.buy_price);
                var curPrice = num(ph.cur_price);
                var cost = buyPrice * shares;
                var value = curPrice * shares;
                var pnl = ph.pnl != null ? num(ph.pnl) : (value - cost);
                var score = num(blendedAnalysisRowScore(ph));
                if (!map[key]) {
                    map[key] = {
                        stock_code: code,
                        short_name: ph.short_name || code,
                        strategy_type: st,
                        shares: 0,
                        cost: 0,
                        market_value: 0,
                        pnl: 0,
                        holding_days: 0,
                        score_weight: 0,
                        score_base: 0,
                        children: []
                    };
                }
                map[key].shares += shares;
                map[key].cost += cost;
                map[key].market_value += value;
                map[key].pnl += pnl;
                map[key].holding_days = Math.max(map[key].holding_days, num(ph.holding_days));
                map[key].score_weight += score * (shares || 1);
                map[key].score_base += shares || 1;
                map[key].children.push(ph);
            });
            return Object.keys(map).map(function(key) {
                var g = map[key];
                g.batch_count = g.children.length;
                g.buy_price = g.shares > 0 ? g.cost / g.shares : 0;
                g.cur_price = g.shares > 0 ? g.market_value / g.shares : 0;
                g.pnl_rate = g.cost > 0 ? g.pnl / g.cost * 100 : 0;
                g.ai_score = g.score_base > 0 ? g.score_weight / g.score_base : 0;
                var tones = g.children.map(function(ph) { return holdingRiskTag(ph).tone; });
                g.risk = tones.indexOf('bad') >= 0
                    ? {text:'高风险', tone:'bad'}
                    : (tones.indexOf('warn') >= 0 ? {text:'需复核', tone:'warn'} : {text:'正常', tone:'good'});
                return g;
            }).sort(function(a, b) {
                if (a.risk.tone !== b.risk.tone) {
                    var order = {bad: 0, warn: 1, good: 2};
                    return (order[a.risk.tone] || 9) - (order[b.risk.tone] || 9);
                }
                return Math.abs(num(b.pnl_rate)) - Math.abs(num(a.pnl_rate));
            });
        }
        function renderEventStrip(rows) {
            if (!rows.length) return '';
            var out = '<div class="sim-event-strip">';
            rows.slice(0, 4).forEach(function(r) {
                var title = r.short_name || r.stock_code || r.event_type || '事件';
                var meta = (r.event_date || '') + (r.event_time ? ' ' + r.event_time : '');
                out += '<div class="sim-event-item"><strong>' + safeText(title) + '</strong><span>' + safeText(meta || r.event_type || '-') + '</span></div>';
            });
            out += '</div>';
            return out;
        }

        Promise.all([
            fetch(SIM_API + '/dashboard?' + modeQuery).then(function(r){return r.json()}),
            fetch(SIM_API + '/history?' + modeQuery + '&limit=100').then(function(r){return r.json()}),
            fetch(SIM_API + '/flow?' + modeQuery + '&limit=200').then(function(r){return r.json()}),
            fetch(SIM_API + '/candidates?' + modeQuery + '&limit=80').then(function(r){return r.json()}),
            fetch(SIM_API + '/recommendation-summary?trade_mode=backtest&days=20').then(function(r){return r.json()}),
            fetch(SIM_API + '/orders?' + modeQuery + '&limit=80').then(function(r){return r.json()}),
            fetch(SIM_API + '/risk-budget?' + modeQuery).then(function(r){return r.json()}),
            fetch(SIM_API + '/events?' + modeQuery + '&limit=20').then(function(r){return r.json()}),
            fetch(SIM_API + '/automation-status').then(function(r){return r.json()}).catch(function(e){ return {sim_auto_ready:false, sim_auto_label:'模拟自动交易状态异常', real_trading_label:'真实自动买卖未启用', error:e.message}; })
        ]).then(function(results) {
            var dash = results[0] || {}, hist = results[1] || {}, flow = results[2] || {}, candidates = results[3] || {}, recSummary = results[4] || {}, ordersRes = results[5] || {}, riskRes = results[6] || {}, eventsRes = results[7] || {}, automation = results[8] || {};
            if (dash.error) { container.innerHTML = '<div class="loading" style="color:#e74c3c">' + safeText(dash.error) + '</div>'; return; }

            var sum = dash.summary || {};
            var currentModeMeta = modeMeta[mode] || modeMeta.live;
            var recLatest = recSummary.latest || {};
            var recRecent = (recSummary.recent || []).slice(0, 8);
            var candidateRows = candidates.data || [];
            var riskBudgets = riskRes.budgets || [];
            var portfolioState = riskRes.portfolio_state || dash.portfolio_state || {};
            var eventRows = eventsRes.data || [];
            var buyReadyRows = candidateRows.filter(function(r){ return r.action === 'BUY_READY'; });
            var waitRows = candidateRows.filter(function(r){ return r.action === 'WAIT'; });
            var sellAlertRows = candidateRows.filter(function(r){ return r.action === 'SELL_ALERT'; });
            var allHoldings = [];
            strategyOrder.forEach(function(st) {
                (((dash.strategies || {})[st] || {}).holdings || []).forEach(function(ph) {
                    ph.strategy_type = st;
                    allHoldings.push(ph);
                });
            });
            var holdingGroups = buildHoldingGroups(allHoldings);
            var riskHoldingGroups = holdingGroups.filter(function(g) { return g.risk.tone !== 'good'; });
            var positionLimit = strategyOrder.reduce(function(total, st){ return total + (strategyCaps[st] || 0); }, 0);
            var slotsLeft = Math.max(0, positionLimit - allHoldings.length);
            var marketOpen = isTradingTime();
            var refreshTime = new Date().toLocaleTimeString('zh-CN', {hour12:false});
            var histData = hist.data || [];
            var flowData = flow.data || [];
            var orderRows = ordersRes.data || [];

            var h = simTradeModeNav(mode);
            h += '<div class="sim-page">';
            h += '<section class="sim-hero" data-sim-section="decision-desk">';
            h += '<div class="sim-hero-top"><div><div class="sim-eyebrow">Simulation Desk</div><h2>' + safeText(currentModeMeta.title) + '</h2><p>' + safeText(currentModeMeta.desc) + '</p></div>';
            h += '<div class="sim-actions">';
            if (mode === 'live') {
                h += '<button onclick="simTradeScan()" class="sim-btn sim-btn-primary">扫描推荐</button>';
                h += '<button onclick="simTradeForwardStart()" class="sim-btn">T+1验证</button>';
            } else if (mode === 'forward') {
                h += '<button onclick="simTradeForwardScan()" class="sim-btn sim-btn-primary">扫描卖点</button>';
                h += '<button onclick="simTradeForwardStart()" class="sim-btn">重新启动</button>';
            } else {
                h += '<button onclick="simTradeBacktest()" class="sim-btn sim-btn-primary">运行回测</button>';
            }
            h += '<span class="sim-live-pill ' + (marketOpen ? 'is-open' : '') + '">' + (marketOpen ? '盘中' : '非交易时段') + '</span>';
            h += '<span class="sim-live-pill">刷新 ' + safeText(refreshTime) + '</span>';
            h += '<span class="sim-live-pill">数据日 ' + safeText(candidates.date || recLatest.signal_date || '-') + '</span>';
            h += '</div></div>';
            h += '<div class="sim-metric-grid sim-hero-metrics">';
            h += metricCard('可买信号', buyReadyRows.length, '规则通过', buyReadyRows.length ? 'good' : 'neutral');
            h += metricCard('等待确认', waitRows.length, '继续观察', waitRows.length ? 'warn' : 'neutral');
            h += metricCard('卖点提醒', sellAlertRows.length, '优先复核', sellAlertRows.length ? 'bad' : 'neutral');
            h += metricCard('持仓股票', holdingGroups.length, '买入批次 ' + allHoldings.length + '/' + positionLimit, holdingGroups.length ? 'good' : 'neutral');
            h += metricCard('可用现金', fmtMoney(sum.cash_available || 0), '仓位 ' + fmtPlainRate(sum.position_usage_rate || 0), 'accent');
            h += metricCard('持仓风险', riskHoldingGroups.length, riskHoldingGroups.length ? '需要复核' : '暂无明显风险', riskHoldingGroups.length ? 'bad' : 'good');
            h += '</div></section>';

            if (mode === 'live') {
                var autoTasks = automation.tasks || {};
                var tickTask = autoTasks.intraday_tick || {};
                var prepareTask = autoTasks.signal_prepare || {};
                var scheduler = automation.scheduler || {};
                var intradayWindow = automation.intraday_window || {};
                h += '<section class="sim-panel" data-sim-section="stage-status">';
                h += sectionHead('自动交易状态', '最近事件 ' + eventRows.length + ' 条');
                h += '<div class="sim-status-grid">';
                h += statusCard('模拟自动买卖', automation.sim_auto_label || '-', '盘中Tick ' + (tickTask.status_label || '-') + ' / 盘前信号池 ' + (prepareTask.status_label || '-'), automation.sim_auto_ready ? 'good' : 'bad');
                h += statusCard('真实自动买卖', automation.real_trading_label || '真实自动买卖未启用', automation.real_trading_reason || '当前只做模拟交易和风控提示', 'neutral');
                h += statusCard('盘中窗口', intradayWindow.label || '-', '入场 ' + ((intradayWindow.entry_windows || []).join(' / ') || '-') + ' / 出场 ' + ((intradayWindow.exit_windows || []).join(' / ') || '-'), intradayWindow.is_entry_window ? 'good' : (intradayWindow.is_exit_window ? 'bad' : 'neutral'));
                h += statusCard('调度状态', scheduler.standalone_online ? '独立调度在线' : (scheduler.embedded_running ? '内嵌调度运行' : '未检测到调度'), 'API重启安全: ' + (scheduler.api_restart_safe ? '是' : '否'), scheduler.standalone_online || scheduler.embedded_running ? 'good' : 'warn');
                h += '</div>' + renderEventStrip(eventRows) + '</section>';

                var candData = candidateRows.slice(0, 30);
                h += '<section class="sim-panel" data-sim-section="candidate-queue">';
                h += sectionHead('今日决策队列' + (candidates.date ? ' (' + candidates.date + ')' : ''), '候选 ' + candidateRows.length + ' 只 · 可买 ' + buyReadyRows.length + ' · 等待 ' + waitRows.length + ' · 卖点 ' + sellAlertRows.length);
                h += '<div class="sim-empty" style="text-align:left;margin-bottom:10px;">主力行为“建仓”只作为证据，不会直接触发买入；模拟盘还要同时通过 AI 信号、买点分、盈亏比、板块门禁、实时价、买入区间、入场窗口和组合风控。</div>';
                if (!candidateRows.length) {
                    h += emptyState('暂无可展示的 AI 推荐候选');
                } else {
                    h += '<div class="sim-candidate-grid">';
                    h += candidateGroupCard('可买信号', buyReadyRows, 'good', '暂无可买信号');
                    h += candidateGroupCard('等待确认', waitRows, 'warn', '暂无等待候选');
                    h += candidateGroupCard('卖点提醒', sellAlertRows, 'bad', '暂无卖点提醒');
                    h += '</div>';
                    h += '<details class="sim-details"><summary>展开候选明细表（最多显示前 ' + candData.length + ' 只）</summary>';
                    h += '<div class="sim-table-scroll"><table class="sim-table sim-candidate-table"><thead><tr>';
                    h += '<th>动作</th><th>代码</th><th>名称</th><th>策略</th><th>综合分</th><th>最终分</th><th>入场分</th><th>主升分</th><th>持有分</th><th>买入区间</th><th>止损/止盈</th><th>原因</th>';
                    h += '</tr></thead><tbody>';
                    candData.forEach(function(r) {
                        var st = r.preferred_strategy || r.primary_strategy || '';
                        var entryRange = (num(r.entry_price_low) > 0 && num(r.entry_price_high) > 0)
                            ? fmtCellPrice(r.entry_price_low) + ' ~ ' + fmtCellPrice(r.entry_price_high)
                            : '-';
                        var stopTake = fmtCellPrice(r.stop_loss_price || r.trend_stop_price) + ' / ' + fmtCellPrice(r.take_profit_1 || r.trend_reduce_price);
                        h += '<tr><td>' + simActionTag(r.action, r.action_label) + '</td>';
                        h += '<td>' + safeText(r.stock_code || '-') + '</td><td>' + stockLink(r.stock_code, r.short_name) + '</td>';
                        h += '<td><span style="color:' + (strategyColors[st] || '#64748b') + ';font-weight:800;">' + safeText(strategyNames[st] || r.preferred_strategy_name || '-') + '</span></td>';
                        h += '<td>' + fmtCellPrice(blendedAnalysisRowScore(r)) + '</td><td>' + fmtCellPrice(r.final_trade_score) + '</td><td>' + fmtCellPrice(r.entry_score) + '</td><td>' + fmtCellPrice(r.main_wave_score) + '</td><td>' + fmtCellPrice(r.trend_hold_score) + '</td>';
                        h += '<td>' + safeText(entryRange) + '</td><td>' + safeText(stopTake) + '</td>';
                        h += '<td class="sim-text-cell" title="' + safeAttr(parseReason(r.action_reason)) + '">' + safeText(parseReason(r.action_reason)) + '</td></tr>';
                    });
                    h += '</tbody></table></div></details>';
                }
                h += '</section>';
            }

            h += '<section class="sim-panel" data-sim-section="holdings">';
            h += sectionHead('当前持仓', '股票 ' + holdingGroups.length + ' · 买入批次 ' + allHoldings.length + '/' + positionLimit + ' · 剩余仓位 ' + slotsLeft + ' · 需复核 ' + riskHoldingGroups.length);
            if (!holdingGroups.length) {
                h += emptyState('暂无持仓');
            } else {
                h += '<div class="sim-mini-summary">';
                h += metricCard('持仓股票', holdingGroups.length, '按股票合并展示', 'neutral');
                h += metricCard('买入批次', allHoldings.length, '展开下方可看每笔', 'accent');
                h += metricCard('风险复核', riskHoldingGroups.length, riskHoldingGroups.length ? '优先查看' : '状态正常', riskHoldingGroups.length ? 'bad' : 'good');
                h += '</div><div class="sim-table-scroll"><table class="sim-table"><thead><tr>';
                h += '<th>策略</th><th>代码</th><th>名称</th><th>批次</th><th>总股数</th><th>均价</th><th>现价</th><th>市值</th><th>盈亏</th><th>收益率</th><th>最长持有</th><th>评分</th><th>风险</th>';
                h += '</tr></thead><tbody>';
                holdingGroups.forEach(function(g) {
                    h += '<tr><td><span style="color:' + strategyColors[g.strategy_type] + ';font-weight:800;">' + safeText(strategyNames[g.strategy_type] || '-') + '</span></td>';
                    h += '<td>' + safeText(g.stock_code || '-') + '</td><td>' + stockLink(g.stock_code, g.short_name) + '</td>';
                    h += '<td><span class="sim-tag sim-tag-neutral">' + g.batch_count + '笔</span></td><td>' + num(g.shares) + '</td>';
                    h += '<td>' + fmtCellPrice(g.buy_price) + '</td><td>' + fmtCellPrice(g.cur_price) + '</td><td>' + fmtMoney(g.market_value) + '</td>';
                    h += '<td>' + fmtPnl(g.pnl) + '</td><td>' + fmtRate(g.pnl_rate) + '</td><td>' + num(g.holding_days) + '天</td>';
                    h += '<td>' + analysisScoreText(g.ai_score) + '</td><td><span class="sim-tag sim-tag-' + g.risk.tone + '">' + g.risk.text + '</span></td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div>';
                h += '<details class="sim-details"><summary>展开买入批次明细（' + allHoldings.length + ' 笔）</summary>';
                h += '<div class="sim-table-scroll"><table class="sim-table"><thead><tr>';
                h += '<th>策略</th><th>代码</th><th>名称</th><th>买入时间</th><th>股数</th><th>买入价</th><th>现价</th><th>盈亏</th><th>收益率</th><th>风险</th><th>操作</th>';
                h += '</tr></thead><tbody>';
                allHoldings.forEach(function(ph) {
                    var risk = holdingRiskTag(ph);
                    h += '<tr><td><span style="color:' + strategyColors[ph.strategy_type] + ';font-weight:800;">' + safeText(strategyNames[ph.strategy_type] || '-') + '</span></td>';
                    h += '<td>' + safeText(ph.stock_code || '-') + '</td><td>' + stockLink(ph.stock_code, ph.short_name) + '</td>';
                    h += '<td class="sim-date-cell">' + safeText(ph.buy_date || '') + '</td><td>' + holdingShares(ph) + '</td>';
                    h += '<td>' + fmtCellPrice(ph.buy_price) + '</td><td>' + fmtCellPrice(ph.cur_price) + '</td><td>' + fmtPnl(ph.pnl) + '</td><td>' + fmtRate(ph.pnl_rate) + '</td>';
                    h += '<td><span class="sim-tag sim-tag-' + risk.tone + '">' + risk.text + '</span></td>';
                    if (mode === 'live') h += '<td><button onclick="simTradeClosePos(' + (ph.id || 0) + ')" class="sim-link-btn danger">平仓</button></td>';
                    else h += '<td><span class="sim-muted">' + (mode === 'forward' ? '验证中' : '回测未平') + '</span></td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div></details>';
            }
            h += '</section>';

            h += '<section class="sim-panel" data-sim-section="capital">';
            h += sectionHead('资金与绩效', '近 3 月模拟表现');
            h += '<div class="sim-metric-grid">';
            h += metricCard('总资产', fmtMoney(sum.total_assets || 0), '含现金与持仓', 'neutral');
            h += metricCard('可用现金', fmtMoney(sum.cash_available || 0), '可用于新仓', 'accent');
            h += metricCard('仓位使用', fmtPlainRate(sum.position_usage_rate || 0), '资金占用', 'neutral');
            h += metricCard('近3月交易', sum.trades_3m || 0, '已平与持仓统计', 'neutral');
            h += metricCard('平均收益率', fmtPlainRate(sum.avg_return_3m || 0), '单笔平均', num(sum.avg_return_3m) >= 0 ? 'good' : 'bad');
            h += metricCard('最大回撤', num(sum.max_drawdown_3m).toFixed(2) + '%', '风险约束', 'warn');
            h += metricCard('盈亏比', fmtRatio(sum.profit_loss_ratio_3m), '收益/亏损', 'accent');
            h += metricCard('Profit Factor', fmtRatio(sum.profit_factor_3m), '盈利因子', 'accent');
            h += '</div></section>';

            h += '<div class="sim-chip-row" data-sim-section="strategy-caption">';
            h += '<span class="sim-chip red">超短快进快出: 1 到 3 天</span><span class="sim-chip orange">短线趋势跟随: 3 到 10 天</span><span class="sim-chip blue">波段持有: 10 到 30 天</span><span class="sim-chip purple">主升趋势持有: 只做主升段</span>';
            h += '</div>';
            h += '<section class="sim-strategy-grid" data-sim-section="strategies">';
            strategyOrder.forEach(function(st) {
                var s = (dash.strategies || {})[st] || {};
                var color = strategyColors[st];
                var strategyLots = (s.holdings || []).map(function(ph) {
                    ph.strategy_type = ph.strategy_type || st;
                    return ph;
                });
                var strategyGroups = buildHoldingGroups(strategyLots);
                h += '<article class="sim-strategy-card" style="--strategy-color:' + color + ';">';
                h += '<div class="sim-strategy-head"><strong>' + safeText(s.name || strategyNames[st]) + '策略</strong><span>股票 ' + strategyGroups.length + ' · 批次 ' + num(s.holding_count) + '/' + strategyCaps[st] + '</span></div>';
                h += '<p>' + safeText(strategyTitle(st, s.name || st) + ': ' + (strategyHints[st] || '')) + '</p>';
                h += '<div class="sim-strategy-stats"><div><strong class="' + (num(s.avg_return_3m) >= 0 ? 'c-red' : 'c-green') + '">' + fmtPlainRate(s.avg_return_3m || 0) + '</strong><span>近3月均收</span></div><div><strong>' + num(s.max_drawdown_3m).toFixed(2) + '%</strong><span>最大回撤</span></div><div><strong>' + fmtRatio(s.profit_factor_3m) + '</strong><span>盈利因子</span></div></div>';
                if (strategyGroups.length) {
                    h += '<div class="sim-strategy-holdings">';
                    strategyGroups.forEach(function(g) {
                        h += '<div><span>' + stockLink(g.stock_code, g.short_name) + ' <em>' + g.batch_count + '笔</em></span><span>' + fmtRate(g.pnl_rate) + '</span></div>';
                    });
                    h += '</div>';
                }
                h += '</article>';
            });
            h += '</section>';

            h += '<section class="sim-panel" data-sim-section="win-history">';
            h += sectionHead('AI 推荐买入判断与历史胜率', '按推荐信号做买入判断后统计结果');
            h += '<div class="sim-metric-grid">';
            h += metricCard('推荐日', recLatest.signal_date || '-', '最新样本', 'neutral');
            h += metricCard('推荐总数', num(recLatest.total_recommendations), 'AI推荐池', 'neutral');
            h += metricCard('判断可买', num(recLatest.buy_ready_count), '通过买入规则', 'good');
            h += metricCard('平均交易分', num(recLatest.avg_final_trade_score).toFixed(1), '最终分', 'accent');
            h += metricCard('近20日信号', recRecent.length, '历史样本', 'neutral');
            h += '</div>';
            if (recRecent.length) {
                h += '<div class="sim-table-scroll compact"><table class="sim-table"><thead><tr><th>信号日</th><th>推荐</th><th>可买</th><th>平均分</th><th>回测买入</th><th>回测卖出</th></tr></thead><tbody>';
                recRecent.forEach(function(r) {
                    h += '<tr><td>' + safeText(r.signal_date || '-') + '</td><td>' + num(r.total_recommendations) + '</td><td>' + num(r.buy_ready_count) + '</td><td>' + num(r.avg_final_trade_score).toFixed(1) + '</td><td>' + num(r.backtest_bought_count) + '</td><td>' + num(r.backtest_sold_count) + '</td></tr>';
                });
                h += '</tbody></table></div>';
            }
            h += '</section>';

            h += '<section class="sim-panel" data-sim-section="trade-history">';
            h += '<div class="sim-section-head"><h3>历史交易 (' + histData.length + ')</h3><select id="simHistFilter" onchange="filterSimHistory()" class="sim-select"><option value="">全部策略</option><option value="ultra_short">超短</option><option value="short_term">短线</option><option value="swing">波段</option><option value="main_wave">主升浪</option></select></div>';
            h += '<details class="sim-details"><summary>展开历史交易明细</summary>';
            if (!histData.length) {
                h += emptyState('暂无交易记录');
            } else {
                h += '<div class="sim-table-scroll"><table class="sim-table"><thead><tr><th>状态</th><th>策略</th><th>代码</th><th>名称</th><th>买入价</th><th>卖出价</th><th>收益率</th><th>盈亏</th><th>持有天数</th><th>买入原因</th><th>卖出原因</th><th>评分</th><th>日期</th></tr></thead><tbody id="simHistBody">';
                histData.forEach(function(r) {
                    var st = r.strategy_type || '';
                    var isSold = r.status === 'sold';
                    h += '<tr class="sim-hist-row" data-st="' + safeAttr(st) + '"><td><span class="sim-tag ' + (isSold ? 'sim-tag-good' : 'sim-tag-warn') + '">' + (isSold ? '已平仓' : '持仓中') + '</span></td>';
                    h += '<td><span style="color:' + (strategyColors[st] || '#64748b') + ';font-weight:800;">' + safeText(strategyNames[st] || st || '-') + '</span></td><td>' + safeText(r.stock_code || '-') + '</td><td>' + stockLink(r.stock_code, r.short_name) + '</td>';
                    h += '<td>' + fmtCellPrice(r.buy_price) + '</td><td>' + (isSold ? fmtCellPrice(r.sell_price) : '-') + '</td><td>' + (isSold ? fmtRate(r.profit_rate) : '-') + '</td><td>' + (isSold ? fmtPnl(r.profit) : '-') + '</td><td>' + (isSold ? num(r.holding_days) + '天' : '-') + '</td>';
                    h += '<td class="sim-text-cell" title="' + safeAttr(parseReason(r.buy_reason)) + '">' + safeText(parseReason(r.buy_reason)) + '</td><td class="sim-text-cell" title="' + safeAttr(fmtSellReason(r.sell_reason)) + '">' + safeText(isSold ? fmtSellReason(r.sell_reason) : '-') + '</td><td>' + analysisScoreText(blendedAnalysisRowScore(r)) + '</td>';
                    h += '<td class="sim-date-cell">' + safeText(r.buy_date || '') + ' ' + safeText(r.buy_time || '') + (isSold ? '<br>' + safeText(r.sell_date || '') + ' ' + safeText(r.sell_time || '') : '') + '</td></tr>';
                });
                h += '</tbody></table></div>';
            }
            h += '</details></section>';

            h += '<section class="sim-panel" data-sim-section="flow-history">';
            h += '<div class="sim-section-head"><h3>操作流水 (' + flowData.length + ')</h3><div class="sim-filter-row"><select id="simFlowSource" onchange="filterSimFlow()" class="sim-select"><option value="">全部来源</option><option value="simulation">模拟交易</option><option value="watchlist">自选股</option></select><select id="simFlowType" onchange="filterSimFlow()" class="sim-select"><option value="">全部类型</option><option value="sim_buy">模拟买入</option><option value="sim_sell">模拟卖出</option><option value="watch_buy">自选买入</option><option value="watch_sell">自选卖出</option></select></div></div>';
            h += '<details class="sim-details"><summary>展开操作流水明细</summary>';
            if (!flowData.length) {
                h += emptyState('暂无流水记录');
            } else {
                h += '<div class="sim-table-scroll"><table class="sim-table"><thead><tr><th>日期</th><th>时间</th><th>代码</th><th>名称</th><th>来源</th><th>方向</th><th>策略</th><th>价格</th><th>股数</th><th>金额</th><th>手续费</th><th>原因</th><th>评分</th></tr></thead><tbody id="simFlowBody">';
                flowData.forEach(function(r) {
                    var isBuy = r.trans_type === 'buy';
                    var srcLabel = r.source === 'simulation' ? '模拟' : '自选';
                    h += '<tr class="sim-flow-row" data-source="' + safeAttr(r.source || '') + '" data-flowtype="' + safeAttr(r.flow_type || '') + '"><td class="sim-date-cell">' + safeText(r.trans_date || '') + '</td><td class="sim-date-cell">' + safeText(r.trans_time || '') + '</td>';
                    h += '<td>' + safeText(r.stock_code || '-') + '</td><td>' + stockLink(r.stock_code, r.short_name) + '</td><td><span class="sim-tag sim-tag-neutral">' + safeText(srcLabel) + '</span></td><td><span class="sim-tag ' + (isBuy ? 'sim-tag-bad' : 'sim-tag-good') + '">' + (isBuy ? '买入' : '卖出') + '</span></td>';
                    h += '<td><span style="color:' + (strategyColors[r.strategy_type] || '#64748b') + ';font-weight:800;">' + safeText(strategyNames[r.strategy_type] || '-') + '</span></td><td>' + fmtCellPrice(r.price) + '</td><td>' + num(r.shares) + '</td><td>' + fmtMoney(r.amount || 0) + '</td><td>' + num(r.fee).toFixed(2) + '</td>';
                    h += '<td class="sim-text-cell" title="' + safeAttr(parseReason(r.reason)) + '">' + safeText(parseReason(r.reason)) + '</td><td>' + analysisScoreText(blendedAnalysisRowScore(r)) + '</td></tr>';
                });
                h += '</tbody></table></div>';
            }
            h += '</details></section>';

            h += '<div id="simTradeStatus" class="sim-inline-status">' + safeText(window._simTradeLastStatus || '') + '</div>';
            h += '</div>';

            container.innerHTML = h;
            window._simTradeLastStatus = '';
            setStatus(mode === 'backtest' ? '策略回测已加载' : (mode === 'forward' ? 'T+1验证已加载' : '模拟交易已加载'));
        }).catch(function(e) {
            container.innerHTML = '<div class="loading" style="color:#e74c3c">加载失败: ' + safeText(e.message) + '</div>';
        });
    }

    function loadSimTradePageLegacy(container, mode) {
        container.innerHTML = '<div class="loading"><span class="spinner"></span> 加载模拟交易数据...</div>';
        var SIM_API = '/api/sim-trade';
        mode = mode || window._simTradeMode || 'live';
        window._simTradeMode = mode;
        var modeQuery = 'trade_mode=' + encodeURIComponent(mode);
        var strategyOrder = ['ultra_short', 'short_term', 'swing', 'main_wave'];
        var strategyNames = {ultra_short: '超短', short_term: '短线', swing: '波段', main_wave: '主升浪'};
        var strategyColors = {ultra_short: '#e74c3c', short_term: '#f39c12', swing: '#3498db', main_wave: '#8e44ad'};
        var strategyCaps = {ultra_short: 3, short_term: 3, swing: 2, main_wave: 2};
        var strategyLabels = {
            ultra_short: '超短快进快出',
            short_term: '短线趋势跟随',
            swing: '波段持有',
            main_wave: '主升趋势持有'
        };
        var strategyHints = {
            ultra_short: '看强势和节奏，通常持有 1 到 3 天。',
            short_term: '看短线趋势延续，通常持有 3 到 10 天。',
            swing: '看波段机会和基本面，通常持有 10 到 30 天。',
            main_wave: '只做主升段，愿意拿更久，强调趋势是否还在。'
        };
        var modeMeta = {
            live: {
                title: '今日 AI 模拟交易',
                desc: '这页会把今天的 AI 推荐，按买入规则过一遍，告诉你哪些票现在可以模拟买、哪些还要等、哪些偏向卖出提醒。'
            },
            forward: {
                title: 'T+1 验证回放',
                desc: '这是把推荐日后的分钟线拿来做验证，模拟“次日买入后，后面会怎么走”，主要用来校验推荐是否经得起真实节奏。'
            },
            backtest: {
                title: '历史回测',
                desc: '这是把历史推荐批量回放，统计胜率、收益、回撤，不代表今天现在就该下单。'
            }
        };
        function strategyTitle(st, fallback) {
            return strategyLabels[st] || strategyNames[st] || fallback || st;
        }

        function fmtPnl(v) { var n = Number(v || 0); return '<span class="' + (n >= 0 ? 'c-red' : 'c-green') + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '</span>'; }
        function fmtRate(v) { var n = Number(v || 0); return '<span class="' + (n >= 0 ? 'c-red' : 'c-green') + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '%</span>'; }
        function fmtPlainRate(v) { var n = Number(v || 0); return (n >= 0 ? '+' : '') + n.toFixed(2) + '%'; }
        function fmtRatio(v) { var n = Number(v || 0); return n > 0 ? n.toFixed(2) : '-'; }
        function fmtMoney(v) { var n = Number(v || 0); if (Math.abs(n) >= 1e4) return (n / 1e4).toFixed(2) + '万'; return n.toFixed(0); }
        function fmtCellPrice(v) { var n = Number(v || 0); return n > 0 ? n.toFixed(2) : '-'; }
        function safeText(v) { return escAttr(v == null ? '' : v); }
        function stockLink(code, name) { return '<a href="javascript:void(0)" onclick="openKlineModal(\'' + code + '\',\'' + (name || '') + '\')" class="clickable-name">' + (name || code) + '</a>'; }
        function parseReason(v) { if (!v) return '-'; if (typeof v === 'string' && v.charAt(0) === '{') { try { var o = JSON.parse(v); return localizeMachineText(o.reason || o.sell_reason || v); } catch(e) { return localizeMachineText(v); } } return localizeMachineText(v); }
        var sellReasonMap = {take_profit:'止盈', stop_loss:'止损', time_limit:'时间止损', trailing_stop:'动态止盈', manual_close:'手动平仓'};
        function fmtSellReason(v) { if (!v) return '-'; return sellReasonMap[v] || v; }
        function simActionTag(action, label) {
            var color = action === 'BUY_READY' ? '#27ae60' : (action === 'SELL_ALERT' ? '#e74c3c' : '#7f8c8d');
            return '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:' + color + '12;color:' + color + ';font-weight:700;">' + (label || action || '-') + '</span>';
        }
        function signalMetric(label, value, color, note) {
            return '<div style="flex:1;min-width:130px;background:#fff;border:1px solid #e8edf3;border-radius:12px;padding:12px 14px;box-shadow:0 1px 8px rgba(15,23,42,.04);">'
                + '<div style="font-size:12px;color:#64748b;margin-bottom:4px;">' + label + '</div>'
                + '<div style="font-size:22px;line-height:1.1;font-weight:800;color:' + color + ';">' + value + '</div>'
                + '<div style="font-size:11px;color:#94a3b8;margin-top:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + (note || '') + '</div>'
                + '</div>';
        }
        function candidateGroupCard(title, rows, color, emptyText) {
            var out = '<div style="flex:1;min-width:210px;border:1px solid #e8edf3;border-radius:12px;background:#fff;overflow:hidden;">';
            out += '<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 10px;background:' + color + '10;border-bottom:1px solid #eef2f7;">';
            out += '<span style="font-size:13px;font-weight:800;color:' + color + ';">' + title + '</span>';
            out += '<span style="font-size:12px;color:#64748b;">' + rows.length + ' 只</span>';
            out += '</div>';
            var showRows = rows.slice(0, 3);
            if (showRows.length === 0) {
                out += '<div style="padding:14px 10px;color:#94a3b8;font-size:12px;text-align:center;">' + emptyText + '</div>';
            } else {
                showRows.forEach(function(r) {
                    var st = r.preferred_strategy || r.primary_strategy || '';
                    var score = blendedAnalysisRowScore(r);
                    var entryRange = (Number(r.entry_price_low || 0) > 0 && Number(r.entry_price_high || 0) > 0)
                        ? fmtCellPrice(r.entry_price_low) + ' ~ ' + fmtCellPrice(r.entry_price_high)
                        : '-';
                    out += '<div style="padding:8px 10px;border-bottom:1px solid #f1f5f9;">';
                    out += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center;">';
                    out += '<div style="min-width:0;"><div style="font-size:12px;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + stockLink(r.stock_code, r.short_name) + '</div>';
                    out += '<div style="font-size:11px;color:#94a3b8;margin-top:3px;">' + (r.stock_code || '-') + ' · ' + (strategyNames[st] || r.preferred_strategy_name || '-') + '</div></div>';
                    out += '<div style="text-align:right;white-space:nowrap;"><div style="font-size:15px;font-weight:800;color:' + color + ';">' + fmtCellPrice(score) + '</div><div style="font-size:10px;color:#94a3b8;">综合分</div></div>';
                    out += '</div>';
                    out += '<div style="display:flex;justify-content:space-between;gap:8px;margin-top:5px;font-size:11px;color:#64748b;">';
                    out += '<span>买入区间 ' + entryRange + '</span><span>止损 ' + fmtCellPrice(r.stop_loss_price || r.trend_stop_price) + '</span>';
                    out += '</div></div>';
                });
                if (rows.length > showRows.length) {
                    out += '<div style="padding:6px 10px;color:#94a3b8;font-size:11px;text-align:center;">还有 ' + (rows.length - showRows.length) + ' 只，展开下方明细查看</div>';
                }
            }
            out += '</div>';
            return out;
        }
        function holdingRiskTag(ph) {
            var pnl = Number(ph.pnl_rate || 0);
            var days = Number(ph.holding_days || 0);
            var score = Number(blendedAnalysisRowScore(ph) || 0);
            if (pnl <= -5 || score < 55) return {text:'高风险', color:'#dc2626', bg:'#fff1f2'};
            if (pnl <= -2 || days >= 10) return {text:'需复核', color:'#d97706', bg:'#fff7ed'};
            return {text:'正常', color:'#16a34a', bg:'#ecfdf5'};
        }

        Promise.all([
            fetch(SIM_API + '/dashboard?' + modeQuery).then(function(r){return r.json()}),
            fetch(SIM_API + '/history?' + modeQuery + '&limit=100').then(function(r){return r.json()}),
            fetch(SIM_API + '/flow?' + modeQuery + '&limit=200').then(function(r){return r.json()}),
            fetch(SIM_API + '/candidates?' + modeQuery + '&limit=80').then(function(r){return r.json()}),
            fetch(SIM_API + '/recommendation-summary?trade_mode=backtest&days=20').then(function(r){return r.json()}),
            fetch(SIM_API + '/orders?' + modeQuery + '&limit=80').then(function(r){return r.json()}),
            fetch(SIM_API + '/risk-budget?' + modeQuery).then(function(r){return r.json()}),
            fetch(SIM_API + '/events?' + modeQuery + '&limit=20').then(function(r){return r.json()}),
            fetch(SIM_API + '/automation-status').then(function(r){return r.json()}).catch(function(e){ return {sim_auto_ready:false, sim_auto_label:'模拟自动交易状态异常', real_trading_label:'真实自动买卖未启用', error:e.message}; })
        ]).then(function(results) {
            var dash = results[0], hist = results[1], flow = results[2], candidates = results[3] || {}, recSummary = results[4] || {}, ordersRes = results[5] || {}, riskRes = results[6] || {}, eventsRes = results[7] || {}, automation = results[8] || {};
            if (dash.error) { container.innerHTML = '<div class="loading" style="color:#e74c3c">' + dash.error + '</div>'; return; }

            var h = simTradeModeNav(mode);
            var sum = dash.summary || {};
            var currentModeMeta = modeMeta[mode] || modeMeta.live;
            var recLatest = recSummary.latest || {};
            var recRecent = (recSummary.recent || []).slice(0, 10);
            var candidateRows = candidates.data || [];
            var orderRows = ordersRes.data || [];
            var riskBudgets = riskRes.budgets || [];
            var portfolioState = riskRes.portfolio_state || dash.portfolio_state || {};
            var eventRows = eventsRes.data || [];
            var buyReadyRows = candidateRows.filter(function(r){ return r.action === 'BUY_READY'; });
            var waitRows = candidateRows.filter(function(r){ return r.action === 'WAIT'; });
            var sellAlertRows = candidateRows.filter(function(r){ return r.action === 'SELL_ALERT'; });
            var allHoldings = [];
            strategyOrder.forEach(function(st) {
                (((dash.strategies||{})[st]||{}).holdings||[]).forEach(function(ph) {
                    ph.strategy_type = st;
                    allHoldings.push(ph);
                });
            });
            var riskHoldings = allHoldings.filter(function(ph) {
                var risk = holdingRiskTag(ph);
                return risk.text !== '正常';
            });
            var positionLimit = strategyOrder.reduce(function(total, st){ return total + (strategyCaps[st] || 0); }, 0);
            var slotsLeft = Math.max(0, positionLimit - allHoldings.length);
            var marketOpen = isTradingTime();
            var refreshTime = new Date().toLocaleTimeString('zh-CN', {hour12:false});

            if (mode === 'live') {
                h += '<div data-sim-section="decision-desk" style="background:linear-gradient(135deg,#f8fbff,#fff);border:1px solid #dbeafe;border-radius:16px;padding:16px;margin-bottom:16px;box-shadow:0 6px 18px rgba(30,64,175,.06);">';
                h += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;margin-bottom:12px;">';
                h += '<div><div style="font-size:18px;font-weight:900;color:#0f172a;">盘中模拟决策台</div>';
                h += '<div style="font-size:12px;color:#64748b;margin-top:5px;">先看信号、仓位和风险，再决定是否手动扫描或平仓。当前只做模拟交易，不会真实下单。</div></div>';
                h += '<div style="display:flex;gap:8px;align-items:center;justify-content:flex-end;flex-wrap:wrap;font-size:12px;color:#64748b;">';
                h += '<button onclick="simTradeScan()" class="btn-refresh" style="padding:7px 12px;font-size:12px;">扫描推荐</button>';
                h += '<button onclick="simTradeForwardStart()" style="padding:7px 12px;border:none;border-radius:8px;background:#34495e;color:#fff;cursor:pointer;font-size:12px;font-weight:700;">T+1验证</button>';
                h += '<span style="padding:6px 10px;border-radius:999px;background:' + (marketOpen ? '#ecfdf5' : '#f8fafc') + ';color:' + (marketOpen ? '#047857' : '#64748b') + ';font-weight:800;">' + (marketOpen ? '盘中' : '非交易时段') + '</span>';
                h += '<span style="padding:6px 10px;border-radius:999px;background:#f8fafc;">刷新 ' + refreshTime + '</span>';
                h += '<span style="padding:6px 10px;border-radius:999px;background:#f8fafc;">数据日 ' + (candidates.date || '-') + '</span>';
                h += '</div></div>';
                h += '<div style="display:flex;gap:10px;flex-wrap:wrap;">';
                h += signalMetric('可买信号', buyReadyRows.length, '#16a34a', '规则通过');
                h += signalMetric('等待确认', waitRows.length, '#d97706', '继续观察');
                h += signalMetric('卖点提醒', sellAlertRows.length, '#dc2626', '优先复核');
                h += signalMetric('持仓/上限', allHoldings.length + '/' + positionLimit, '#2563eb', '剩余 ' + slotsLeft + ' 个仓位');
                h += signalMetric('可用现金', fmtMoney(sum.cash_available || 0), '#ea580c', '仓位 ' + fmtPlainRate(sum.position_usage_rate || 0));
                h += signalMetric('持仓风险', riskHoldings.length, riskHoldings.length ? '#dc2626' : '#16a34a', riskHoldings.length ? '需优先复核' : '暂无明显风险');
                h += '</div>';
                h += '</div>';
            }

            // ── 汇总卡片(使用系统 stat-card 样式) ──
            if (mode === 'live') {
                var orderCounts = (sum.order_counts || {});
                var signalCounts = (sum.signal_counts || {});
                var autoTasks = automation.tasks || {};
                var tickTask = autoTasks.intraday_tick || {};
                var prepareTask = autoTasks.signal_prepare || {};
                var scheduler = automation.scheduler || {};
                var intradayWindow = automation.intraday_window || {};
                var autoColor = automation.sim_auto_ready ? '#16a34a' : '#dc2626';
                var autoBg = automation.sim_auto_ready ? '#ecfdf5' : '#fff1f2';
                h += '<div data-sim-section="stage-status" style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;margin-bottom:16px;">';
                h += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:10px;">';
                h += '<div><div style="font-size:15px;font-weight:900;color:#0f172a;">三阶段自动交易状态</div><div style="font-size:12px;color:#64748b;margin-top:3px;">信号池 → 模拟订单 → 撮合成交 → 组合风险预算</div></div>';
                h += '<div style="font-size:12px;color:#94a3b8;">最近事件 ' + eventRows.length + ' 条</div></div>';
                h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px;margin-bottom:12px;">';
                h += '<div style="background:' + autoBg + ';border:1px solid ' + autoColor + '22;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#64748b;">模拟自动买卖</div><div style="font-size:16px;font-weight:900;color:' + autoColor + ';">' + safeText(automation.sim_auto_label || '-') + '</div><div style="font-size:11px;color:#64748b;margin-top:4px;">盘中Tick ' + safeText(tickTask.status_label || '-') + ' / 盘前信号池 ' + safeText(prepareTask.status_label || '-') + '</div></div>';
                h += '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#64748b;">真实自动买卖</div><div style="font-size:16px;font-weight:900;color:#475569;">' + safeText(automation.real_trading_label || '真实自动买卖未启用') + '</div><div style="font-size:11px;color:#64748b;margin-top:4px;">' + safeText(automation.real_trading_reason || '当前只做模拟交易和风控提示') + '</div></div>';
                h += '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#64748b;">盘中窗口</div><div style="font-size:16px;font-weight:900;color:' + (intradayWindow.is_entry_window ? '#16a34a' : (intradayWindow.is_exit_window ? '#dc2626' : '#475569')) + ';">' + safeText(intradayWindow.label || '-') + '</div><div style="font-size:11px;color:#64748b;margin-top:4px;">入场 ' + safeText((intradayWindow.entry_windows || []).join(' / ') || '-') + ' / 出场 ' + safeText((intradayWindow.exit_windows || []).join(' / ') || '-') + '</div></div>';
                h += '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#64748b;">调度状态</div><div style="font-size:16px;font-weight:900;color:#2563eb;">' + (scheduler.standalone_online ? '独立调度在线' : (scheduler.embedded_running ? '内嵌调度运行' : '未检测到调度')) + '</div><div style="font-size:11px;color:#64748b;margin-top:4px;">API重启安全: ' + (scheduler.api_restart_safe ? '是' : '否') + '</div></div>';
                h += '</div>';
                h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:12px;">';
                h += '<div style="background:#f8fafc;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#64748b;">信号池</div><div style="font-size:20px;font-weight:900;color:#2563eb;">' + Number(signalCounts.total || 0) + '</div><div style="font-size:11px;color:#94a3b8;">新信号 ' + Number(signalCounts.NEW || 0) + ' / 已下单 ' + Number(signalCounts.ORDERED || 0) + '</div></div>';
                h += '<div style="background:#f8fafc;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#64748b;">订单撮合</div><div style="font-size:20px;font-weight:900;color:#16a34a;">' + Number(orderCounts.total || 0) + '</div><div style="font-size:11px;color:#94a3b8;">待成交 ' + Number(orderCounts.PENDING || 0) + ' / 已成交 ' + Number(orderCounts.FILLED || 0) + '</div></div>';
                h += '<div style="background:#fff7ed;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#c2410c;">风险后现金</div><div style="font-size:20px;font-weight:900;color:#ea580c;">' + fmtMoney((portfolioState || {}).cash_available_after_buffer || 0) + '</div><div style="font-size:11px;color:#b45309;">扣除现金缓冲后</div></div>';
                h += '<div style="background:#eff6ff;border-radius:10px;padding:10px;"><div style="font-size:12px;color:#1d4ed8;">总仓位上限</div><div style="font-size:20px;font-weight:900;color:#1d4ed8;">' + fmtMoney((portfolioState || {}).max_total_position_amount || 0) + '</div><div style="font-size:11px;color:#64748b;">当前 ' + fmtMoney((portfolioState || {}).holding_value || 0) + '</div></div>';
                h += '</div>';
                h += '<div style="display:grid;grid-template-columns:minmax(260px,1.3fr) minmax(240px,.9fr);gap:12px;align-items:start;">';
                h += '<div style="border:1px solid #eef2f7;border-radius:12px;overflow:hidden;"><div style="padding:8px 10px;background:#f8fafc;font-size:13px;font-weight:800;color:#0f172a;">最近模拟订单</div>';
                if (!orderRows.length) {
                    h += '<div style="padding:14px;color:#94a3b8;font-size:12px;text-align:center;">暂无订单，盘前信号池准备后由盘中 Tick 自动生成</div>';
                } else {
                    h += '<div style="max-height:180px;overflow:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;"><thead><tr style="color:#64748b;text-align:left;border-bottom:1px solid #e8edf3;"><th style="padding:6px;">时间</th><th>股票</th><th>方向</th><th>状态</th><th>委托/成交</th><th>说明</th></tr></thead><tbody>';
                    orderRows.slice(0, 6).forEach(function(o) {
                        var sideColor = o.side === 'SELL' ? '#dc2626' : '#16a34a';
                        var note = o.last_match_reason || o.risk_budget_note || o.reason || '-';
                        h += '<tr style="border-bottom:1px solid #f1f5f9;">';
                        h += '<td style="padding:6px;white-space:nowrap;color:#64748b;">' + (o.order_time || '-') + '</td>';
                        h += '<td>' + stockLink(o.stock_code, o.short_name) + '</td>';
                        h += '<td style="font-weight:800;color:' + sideColor + ';">' + localizeMachineText(o.side || '-') + '</td>';
                        h += '<td>' + localizeMachineText(o.status || '-') + '</td>';
                        h += '<td>' + Number(o.requested_shares || 0) + '/' + Number(o.filled_shares || 0) + '</td>';
                        h += '<td style="max-width:220px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#64748b;" title="' + safeText(note) + '">' + safeText(note) + '</td>';
                        h += '</tr>';
                    });
                    h += '</tbody></table></div>';
                }
                h += '</div>';
                h += '<div style="border:1px solid #eef2f7;border-radius:12px;overflow:hidden;"><div style="padding:8px 10px;background:#f8fafc;font-size:13px;font-weight:800;color:#0f172a;">策略风险预算</div>';
                if (!riskBudgets.length) {
                    h += '<div style="padding:14px;color:#94a3b8;font-size:12px;text-align:center;">暂无预算快照</div>';
                } else {
                    riskBudgets.forEach(function(b) {
                        h += '<div style="padding:8px 10px;border-bottom:1px solid #f1f5f9;font-size:12px;">';
                        h += '<div style="display:flex;justify-content:space-between;gap:8px;"><span style="font-weight:800;color:#0f172a;">' + (strategyNames[b.strategy_type] || b.strategy_type) + '</span><span style="color:#16a34a;font-weight:800;">' + fmtMoney(b.available_strategy_amount || 0) + '</span></div>';
                        h += '<div style="color:#94a3b8;margin-top:3px;">已用 ' + fmtMoney(b.used_strategy_amount || 0) + ' / 待成交 ' + fmtMoney(b.pending_strategy_amount || 0) + '</div>';
                        h += '</div>';
                    });
                }
                h += '</div></div></div>';
            }

            if (mode !== 'live') {
                h += '<div style="background:linear-gradient(135deg,#fffaf0,#f7fbff);border:1px solid #e6edf7;border-radius:12px;padding:14px 16px;margin-bottom:16px;">';
                h += '<div style="font-size:15px;font-weight:700;color:#1f2937;margin-bottom:6px;">' + currentModeMeta.title + '</div>';
                h += '<div style="font-size:13px;line-height:1.7;color:#475569;margin-bottom:8px;">' + currentModeMeta.desc + '</div>';
                h += '<div style="font-size:12px;line-height:1.7;color:#64748b;">下面按四种交易风格展示当前持仓与近三个月效果。</div>';
                h += '</div>';
            }
            if (mode === 'forward') {
                h += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;">';
                h += '<button onclick="simTradeForwardScan()" style="padding:8px 16px;border:none;border-radius:8px;background:#34495e;color:#fff;cursor:pointer;font-size:13px;font-weight:600;">扫描验证仓位卖点</button>';
                h += '<button onclick="simTradeForwardStart()" style="padding:8px 16px;border:1px solid #34495e;border-radius:8px;background:#fff;color:#34495e;cursor:pointer;font-size:13px;font-weight:600;">重新启动验证</button>';
                h += '</div>';
            } else if (mode === 'backtest') {
                h += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;">';
                h += '<button onclick="simTradeBacktest()" style="padding:8px 18px;border:none;border-radius:8px;background:#e67e22;color:#fff;cursor:pointer;font-size:13px;font-weight:700;">运行历史回测</button>';
                h += '</div>';
            }
            h += '<div class="table-wrap" data-sim-section="win-history" style="padding:16px;margin-bottom:16px;">';
            h += '<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px;">';
            h += '<h3 style="margin:0;color:#2d3436;font-size:15px;">AI推荐买入判断与历史胜率</h3>';
            h += '<span style="color:#64748b;font-size:12px;">口径：按每日 AI 推荐先做是否买入判断，再用历史回测统计买后结果</span>';
            h += '</div>';
            h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:12px;">';
            h += '<div style="background:#f8fafc;border-radius:10px;padding:12px;"><div style="font-size:12px;color:#64748b;">推荐日</div><div style="font-size:18px;font-weight:700;color:#0f172a;">' + (recLatest.signal_date || '-') + '</div></div>';
            h += '<div style="background:#f8fafc;border-radius:10px;padding:12px;"><div style="font-size:12px;color:#64748b;">推荐总数</div><div style="font-size:18px;font-weight:700;color:#0f172a;">' + Number(recLatest.total_recommendations || 0) + '</div></div>';
            h += '<div style="background:#ecfdf5;border-radius:10px;padding:12px;"><div style="font-size:12px;color:#047857;">判断可买</div><div style="font-size:18px;font-weight:700;color:#047857;">' + Number(recLatest.buy_ready_count || 0) + '</div></div>';
            h += '<div style="background:#eff6ff;border-radius:10px;padding:12px;"><div style="font-size:12px;color:#1d4ed8;">实际买入(回测)</div><div style="font-size:18px;font-weight:700;color:#1d4ed8;">' + Number(recLatest.bought_count || 0) + '</div></div>';
            h += '<div style="background:#fff7ed;border-radius:10px;padding:12px;"><div style="font-size:12px;color:#c2410c;">已平仓</div><div style="font-size:18px;font-weight:700;color:#c2410c;">' + Number(recLatest.closed_count || 0) + '</div></div>';
            h += '<div style="background:#f5f3ff;border-radius:10px;padding:12px;"><div style="font-size:12px;color:#6d28d9;">胜率</div><div style="font-size:18px;font-weight:700;color:#6d28d9;">' + fmtPlainRate(recLatest.win_rate || 0) + '</div></div>';
            h += '<div style="background:#fffaf0;border-radius:10px;padding:12px;"><div style="font-size:12px;color:#b45309;">平均收益</div><div style="font-size:18px;font-weight:700;color:#b45309;">' + fmtPlainRate(recLatest.avg_profit_rate || 0) + '</div></div>';
            h += '</div>';
            if (recRecent.length > 0) {
                h += '<div style="font-size:12px;line-height:1.7;color:#64748b;margin-bottom:8px;">最近推荐日里，最重要的是两列：`判断可买` 和 `胜率`。前者告诉你 AI 推荐里有多少只通过了买入规则，后者告诉你历史上真按规则买进去以后表现如何。</div>';
                h += '<div style="max-height:260px;overflow:auto;">';
                h += '<table style="width:100%;font-size:12px;border-collapse:collapse;">';
                h += '<thead><tr style="color:#999;text-align:left;border-bottom:1px solid #e8e8e8;position:sticky;top:0;background:#fff;">';
                h += '<th style="padding:8px;">推荐日</th><th>推荐数</th><th>判断可买</th><th>实际买入</th><th>已平仓</th><th>胜率</th><th>平均收益</th></tr></thead><tbody>';
                recRecent.forEach(function(r) {
                    h += '<tr style="border-bottom:1px solid #f0f0f0;">';
                    h += '<td style="padding:8px;white-space:nowrap;">' + (r.signal_date || '-') + '</td>';
                    h += '<td>' + Number(r.total_recommendations || 0) + '</td>';
                    h += '<td style="color:#047857;font-weight:700;">' + Number(r.buy_ready_count || 0) + '</td>';
                    h += '<td>' + Number(r.bought_count || 0) + '</td>';
                    h += '<td>' + Number(r.closed_count || 0) + '</td>';
                    h += '<td>' + fmtPlainRate(r.win_rate || 0) + '</td>';
                    h += '<td>' + fmtPlainRate(r.avg_profit_rate || 0) + '</td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div>';
            }
            h += '</div>';
            h += '<div class="stats-bar" data-sim-section="capital">';
            h += card('本金', fmtMoney(sum.initial_capital || 1000000), 'blue');
            h += card('总权益', fmtMoney(sum.total_equity || 1000000), (sum.total_equity || 1000000) >= 1000000 ? 'red' : 'green');
            h += card('可用现金', fmtMoney(sum.cash_available || 0), 'orange');
            h += card('仓位使用', fmtPlainRate(sum.position_usage_rate || 0), 'blue');
            h += card('近3月交易', sum.trades_3m || 0, 'blue');
            h += card('平均收益率', fmtPlainRate(sum.avg_return_3m || 0), (sum.avg_return_3m || 0) >= 0 ? 'red' : 'green');
            h += card('最大回撤', (sum.max_drawdown_3m || 0).toFixed ? (sum.max_drawdown_3m || 0).toFixed(2) + '%' : (sum.max_drawdown_3m || 0) + '%', 'green');
            h += card('盈亏比', fmtRatio(sum.profit_loss_ratio_3m), 'orange');
            h += card('Sharpe', fmtRatio(sum.sharpe_ratio_3m), 'blue');
            h += card('Profit Factor', fmtRatio(sum.profit_factor_3m), 'blue');
            h += '</div>';

            // ── 策略卡片 ──
            h += '<div data-sim-section="strategy-caption" style="display:flex;gap:8px;flex-wrap:wrap;margin:-4px 0 12px;">';
            h += '<span style="padding:6px 10px;border-radius:999px;background:#fff1f2;color:#be123c;font-size:12px;">超短快进快出: 持有 1 到 3 天</span>';
            h += '<span style="padding:6px 10px;border-radius:999px;background:#fff7ed;color:#c2410c;font-size:12px;">短线趋势跟随: 持有 3 到 10 天</span>';
            h += '<span style="padding:6px 10px;border-radius:999px;background:#eff6ff;color:#1d4ed8;font-size:12px;">波段持有: 持有 10 到 30 天</span>';
            h += '<span style="padding:6px 10px;border-radius:999px;background:#faf5ff;color:#7e22ce;font-size:12px;">主升趋势持有: 只做主升段</span>';
            h += '</div>';
            h += '<div data-sim-section="strategies" style="display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap;">';
            strategyOrder.forEach(function(st) {
                var s = (dash.strategies || {})[st] || {};
                var color = strategyColors[st];
                h += '<div style="flex:1;min-width:220px;background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 6px rgba(0,0,0,.05);">';
                h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">';
                h += '<span style="font-size:15px;font-weight:700;color:' + color + ';">' + (s.name || st) + '策略</span>';
                h += '<span style="font-size:11px;color:#999;">持仓 ' + (s.holding_count || 0) + '/' + strategyCaps[st] + '</span>';
                h += '</div>';
                h += '<div style="font-size:12px;line-height:1.6;color:#64748b;margin-bottom:8px;">' + strategyTitle(st, s.name || st) + '：' + (strategyHints[st] || '') + '</div>';
                h += '<div style="display:flex;gap:8px;margin-bottom:8px;">';
                h += '<div style="flex:1;text-align:center;"><div style="font-size:18px;font-weight:700;' + ((s.avg_return_3m||0)>=0?'color:#e74c3c':'color:#27ae60') + ';">' + fmtPlainRate(s.avg_return_3m || 0) + '</div><div style="font-size:10px;color:#999;">近3月均收</div></div>';
                h += '<div style="flex:1;text-align:center;"><div style="font-size:18px;font-weight:700;color:#27ae60;">' + Number(s.max_drawdown_3m || 0).toFixed(2) + '%</div><div style="font-size:10px;color:#999;">最大回撤</div></div>';
                h += '<div style="flex:1;text-align:center;"><div style="font-size:18px;font-weight:700;color:#333;">' + fmtRatio(s.profit_factor_3m) + '</div><div style="font-size:10px;color:#999;">盈利因子</div></div>';
                h += '</div>';
                if (s.holdings && s.holdings.length > 0) {
                    h += '<div style="border-top:1px solid #f0f0f0;padding-top:8px;margin-top:4px;">';
                    s.holdings.forEach(function(ph) {
                        h += '<div style="display:flex;justify-content:space-between;font-size:11px;padding:2px 0;">';
                        h += '<span>' + stockLink(ph.stock_code, ph.short_name) + '</span>';
                        h += '<span>' + fmtRate(ph.pnl_rate) + '</span>';
                        h += '</div>';
                    });
                    h += '</div>';
                }
                h += '</div>';
            });
            h += '</div>';

            // ── AI推荐模拟池 ──
            if (mode === 'live') {
                var candData = candidateRows.slice(0, 30);
                h += '<div class="table-wrap" data-sim-section="candidate-queue" style="padding:12px;margin-bottom:14px;">';
                h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;gap:10px;flex-wrap:wrap;">';
                h += '<h3 style="margin:0;color:#2d3436;font-size:15px;">今日决策队列 ' + (candidates.date ? '(' + candidates.date + ')' : '') + '</h3>';
                h += '<span style="color:#999;font-size:12px;">候选 ' + candidateRows.length + ' 只 · 可买 ' + buyReadyRows.length + ' · 等待 ' + waitRows.length + ' · 卖点 ' + sellAlertRows.length + '</span>';
                h += '</div>';
                h += '<div style="font-size:12px;line-height:1.6;color:#64748b;margin-bottom:8px;">主力行为“建仓”只作为证据，不会直接触发买入；模拟盘还要同时通过 AI 信号、买点分、盈亏比、板块门禁、实时价、买入区间、入场窗口和组合风控。绿色看仓位，黄色继续观察，红色优先复核；完整明细在下方展开。</div>';
                if (candidateRows.length === 0) {
                    h += '<div style="color:#999;font-size:13px;padding:20px;text-align:center;">暂无可展示的AI推荐候选</div>';
                } else {
                    h += '<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-start;margin-bottom:8px;">';
                    h += candidateGroupCard('可买信号', buyReadyRows, '#16a34a', '暂无可买信号');
                    h += candidateGroupCard('等待确认', waitRows, '#d97706', '暂无等待候选');
                    h += candidateGroupCard('卖点提醒', sellAlertRows, '#dc2626', '暂无卖点提醒');
                    h += '</div>';
                    h += '<details style="border:1px solid #eef2f7;border-radius:12px;background:#fbfdff;">';
                    h += '<summary style="cursor:pointer;padding:8px 10px;color:#334155;font-size:12px;font-weight:800;">展开候选明细表（最多显示前 ' + candData.length + ' 只）</summary>';
                    h += '<div style="max-height:360px;overflow:auto;">';
                    h += '<table style="width:100%;font-size:12px;border-collapse:collapse;">';
                    h += '<thead><tr style="color:#999;text-align:left;border-bottom:1px solid #e8e8e8;position:sticky;top:0;background:#fff;">';
                    h += '<th style="padding:8px;">动作</th><th>代码</th><th>名称</th><th>策略</th><th>综合分</th><th>最终分</th><th>入场分</th><th>主升分</th><th>持有分</th><th>买入区间</th><th>止损/止盈</th><th>原因</th>';
                    h += '</tr></thead><tbody>';
                    candData.forEach(function(r) {
                        var st = r.preferred_strategy || r.primary_strategy || '';
                        var entryRange = (Number(r.entry_price_low || 0) > 0 && Number(r.entry_price_high || 0) > 0)
                            ? fmtCellPrice(r.entry_price_low) + ' ~ ' + fmtCellPrice(r.entry_price_high)
                            : '-';
                        var stopTake = fmtCellPrice(r.stop_loss_price || r.trend_stop_price) + ' / ' + fmtCellPrice(r.take_profit_1 || r.trend_reduce_price);
                        h += '<tr style="border-bottom:1px solid #f0f0f0;">';
                        h += '<td style="padding:8px;">' + simActionTag(r.action, r.action_label) + '</td>';
                        h += '<td>' + r.stock_code + '</td>';
                        h += '<td>' + stockLink(r.stock_code, r.short_name) + '</td>';
                        h += '<td style="color:' + (strategyColors[st] || '#999') + ';font-weight:700;">' + (strategyNames[st] || r.preferred_strategy_name || '-') + '</td>';
                        h += '<td>' + fmtCellPrice(blendedAnalysisRowScore(r)) + '</td>';
                        h += '<td>' + fmtCellPrice(r.final_trade_score) + '</td>';
                        h += '<td>' + fmtCellPrice(r.entry_score) + '</td>';
                        h += '<td>' + fmtCellPrice(r.main_wave_score) + '</td>';
                        h += '<td>' + fmtCellPrice(r.trend_hold_score) + '</td>';
                        h += '<td style="white-space:nowrap;">' + entryRange + '</td>';
                        h += '<td style="white-space:nowrap;">' + stopTake + '</td>';
                        h += '<td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#666;" title="' + safeText(parseReason(r.action_reason)) + '">' + safeText(parseReason(r.action_reason)) + '</td>';
                        h += '</tr>';
                    });
                    h += '</tbody></table></div>';
                    h += '</details>';
                }
                h += '</div>';
            }

            // ── 当前持仓表 ──
            h += '<div class="table-wrap" data-sim-section="holdings" style="padding:16px;margin-bottom:20px;">';
            h += '<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px;">';
            h += '<h3 style="margin:0;color:#2d3436;font-size:15px;">当前持仓</h3>';
            h += '<span style="color:#64748b;font-size:12px;">持仓 ' + allHoldings.length + '/' + positionLimit + ' · 剩余仓位 ' + slotsLeft + ' · 需复核 ' + riskHoldings.length + '</span>';
            h += '</div>';
            if (allHoldings.length === 0) {
                h += '<div style="color:#999;font-size:13px;padding:20px;text-align:center;">暂无持仓</div>';
            } else {
                h += '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;">';
                h += '<div style="flex:1;min-width:180px;background:#f8fafc;border-radius:10px;padding:10px 12px;"><div style="font-size:12px;color:#64748b;">持仓股票</div><div style="font-size:20px;font-weight:800;color:#0f172a;">' + allHoldings.length + '</div></div>';
                h += '<div style="flex:1;min-width:180px;background:#ecfdf5;border-radius:10px;padding:10px 12px;"><div style="font-size:12px;color:#047857;">剩余仓位</div><div style="font-size:20px;font-weight:800;color:#047857;">' + slotsLeft + '</div></div>';
                h += '<div style="flex:1;min-width:180px;background:' + (riskHoldings.length ? '#fff1f2' : '#ecfdf5') + ';border-radius:10px;padding:10px 12px;"><div style="font-size:12px;color:' + (riskHoldings.length ? '#be123c' : '#047857') + ';">风险复核</div><div style="font-size:20px;font-weight:800;color:' + (riskHoldings.length ? '#be123c' : '#047857') + ';">' + riskHoldings.length + '</div></div>';
                h += '</div>';
                h += '<table style="width:100%;font-size:12px;border-collapse:collapse;">';
                h += '<thead><tr style="color:#999;text-align:left;border-bottom:1px solid #e8e8e8;">';
                h += '<th style="padding:8px;">策略</th><th>代码</th><th>名称</th><th>买入价</th><th>现价</th><th>盈亏</th><th>收益率</th><th>持有天数</th><th>评分</th><th>风险</th><th>操作</th>';
                h += '</tr></thead><tbody>';
                allHoldings.forEach(function(ph) {
                    var risk = holdingRiskTag(ph);
                    h += '<tr style="border-bottom:1px solid #f0f0f0;">';
                    h += '<td style="padding:8px;color:' + strategyColors[ph.strategy_type] + ';font-weight:600;">' + strategyNames[ph.strategy_type] + '</td>';
                    h += '<td>' + ph.stock_code + '</td>';
                    h += '<td>' + stockLink(ph.stock_code, ph.short_name) + '</td>';
                    h += '<td>' + (ph.buy_price||0).toFixed(2) + '</td>';
                    h += '<td>' + (ph.cur_price||0).toFixed(2) + '</td>';
                    h += '<td>' + fmtPnl(ph.pnl) + '</td>';
                    h += '<td>' + fmtRate(ph.pnl_rate) + '</td>';
                    h += '<td>' + (ph.holding_days||0) + '天</td>';
                    h += '<td>' + analysisScoreText(blendedAnalysisRowScore(ph)) + '</td>';
                    h += '<td><span style="display:inline-block;padding:3px 8px;border-radius:999px;background:' + risk.bg + ';color:' + risk.color + ';font-weight:800;">' + risk.text + '</span></td>';
                    if (mode === 'live') {
                        h += '<td><button onclick="simTradeClosePos(' + (ph.id||0) + ')" style="padding:2px 8px;border:1px solid #e74c3c;border-radius:4px;background:transparent;color:#e74c3c;font-size:11px;cursor:pointer;">平仓</button></td>';
                    } else if (mode === 'forward') {
                        h += '<td style="color:#999;font-size:11px;">验证中</td>';
                    } else {
                        h += '<td style="color:#999;font-size:11px;">回测未平</td>';
                    }
                    h += '</tr>';
                });
                h += '</tbody></table>';
            }
            h += '</div>';

            // ── 历史交易记录 ──
            var histData = hist.data || [];
            h += '<div class="table-wrap" data-sim-section="trade-history" style="padding:16px;margin-bottom:20px;">';
            h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">';
            h += '<h3 style="margin:0;color:#2d3436;font-size:14px;">历史交易 (' + histData.length + ')</h3>';
            h += '<select id="simHistFilter" onchange="filterSimHistory()" style="padding:4px 8px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#333;font-size:12px;">';
            h += '<option value="">全部策略</option><option value="ultra_short">超短</option><option value="short_term">短线</option><option value="swing">波段</option><option value="main_wave">主升浪</option>';
            h += '</select></div>';
            h += '<details style="border:1px solid #eef2f7;border-radius:12px;background:#fbfdff;">';
            h += '<summary style="cursor:pointer;padding:10px 12px;color:#334155;font-size:13px;font-weight:800;">展开历史交易明细</summary>';
            if (histData.length === 0) {
                h += '<div style="color:#999;font-size:13px;padding:20px;text-align:center;">暂无交易记录</div>';
            } else {
                h += '<div style="max-height:400px;overflow-y:auto;">';
                h += '<table style="width:100%;font-size:12px;border-collapse:collapse;">';
                h += '<thead><tr style="color:#999;text-align:left;border-bottom:1px solid #e8e8e8;position:sticky;top:0;background:#fff;">';
                h += '<th style="padding:8px;">状态</th><th>策略</th><th>代码</th><th>名称</th><th>买入价</th><th>卖出价</th><th>收益率</th><th>盈亏</th><th>持有天数</th><th>买入原因</th><th>卖出原因</th><th>评分</th><th>日期</th>';
                h += '</tr></thead><tbody id="simHistBody">';
                histData.forEach(function(r) {
                    var st = r.strategy_type || '';
                    var isSold = r.status === 'sold';
                    var statusHtml = isSold
                        ? '<span style="color:#27ae60;font-weight:600;">已平仓</span>'
                        : '<span style="color:#e74c3c;font-weight:600;">持仓中</span>';
                    h += '<tr class="sim-hist-row" data-st="' + st + '" style="border-bottom:1px solid #f0f0f0;">';
                    h += '<td style="padding:8px;">' + statusHtml + '</td>';
                    h += '<td style="color:' + (strategyColors[st]||'#999') + ';font-weight:600;">' + (strategyNames[st]||st) + '</td>';
                    h += '<td>' + r.stock_code + '</td>';
                    h += '<td>' + stockLink(r.stock_code, r.short_name) + '</td>';
                    h += '<td>' + (r.buy_price||0).toFixed(2) + '</td>';
                    h += '<td>' + (isSold ? (r.sell_price||0).toFixed(2) : '-') + '</td>';
                    h += '<td>' + (isSold ? fmtRate(r.profit_rate) : '-') + '</td>';
                    h += '<td>' + (isSold ? fmtPnl(r.profit) : '-') + '</td>';
                    h += '<td>' + (isSold ? (r.holding_days||0) + '天' : '-') + '</td>';
                    h += '<td style="font-size:11px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#666;" title="' + escAttr(parseReason(r.buy_reason)) + '">' + parseReason(r.buy_reason) + '</td>';
                    h += '<td style="font-size:11px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#666;" title="' + escAttr(fmtSellReason(r.sell_reason)) + '">' + (isSold ? fmtSellReason(r.sell_reason) : '-') + '</td>';
                    h += '<td>' + analysisScoreText(blendedAnalysisRowScore(r)) + '</td>';
                    h += '<td style="font-size:11px;color:#999;white-space:nowrap;">' + (r.buy_date||'') + ' ' + (r.buy_time||'') + (isSold ? '<br>→ ' + (r.sell_date||'') + ' ' + (r.sell_time||'') : '') + '</td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div>';
            }
            h += '</details>';
            h += '</div>';

            // ── 操作流水表 ──
            var flowData = flow.data || [];
            h += '<div class="table-wrap" data-sim-section="flow-history" style="padding:16px;margin-bottom:20px;">';
            h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">';
            h += '<h3 style="margin:0;color:#2d3436;font-size:14px;">操作流水 (' + flowData.length + ')</h3>';
            h += '<div style="display:flex;gap:8px;">';
            h += '<select id="simFlowSource" onchange="filterSimFlow()" style="padding:4px 8px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#333;font-size:12px;">';
            h += '<option value="">全部来源</option><option value="simulation">模拟交易</option><option value="watchlist">自选股</option>';
            h += '</select>';
            h += '<select id="simFlowType" onchange="filterSimFlow()" style="padding:4px 8px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#333;font-size:12px;">';
            h += '<option value="">全部类型</option><option value="sim_buy">模拟买入</option><option value="sim_sell">模拟卖出</option><option value="watch_buy">自选买入</option><option value="watch_sell">自选卖出</option>';
            h += '</select>';
            h += '</div></div>';
            h += '<details style="border:1px solid #eef2f7;border-radius:12px;background:#fbfdff;">';
            h += '<summary style="cursor:pointer;padding:10px 12px;color:#334155;font-size:13px;font-weight:800;">展开操作流水明细</summary>';
            if (flowData.length === 0) {
                h += '<div style="color:#999;font-size:13px;padding:20px;text-align:center;">暂无流水记录</div>';
            } else {
                h += '<div style="max-height:400px;overflow-y:auto;">';
                h += '<table style="width:100%;font-size:12px;border-collapse:collapse;">';
                h += '<thead><tr style="color:#999;text-align:left;border-bottom:1px solid #e8e8e8;position:sticky;top:0;background:#fff;">';
                h += '<th style="padding:8px;">日期</th><th>时间</th><th>代码</th><th>名称</th><th>来源</th><th>方向</th><th>策略</th><th>价格</th><th>股数</th><th>金额</th><th>手续费</th><th>原因</th><th>评分</th>';
                h += '</tr></thead><tbody id="simFlowBody">';
                flowData.forEach(function(r) {
                    var isBuy = r.trans_type === 'buy';
                    var srcLabel = r.source === 'simulation' ? '模拟' : '自选';
                    var srcColor = r.source === 'simulation' ? '#1a73e8' : '#e67e22';
                    h += '<tr class="sim-flow-row" data-source="' + (r.source||'') + '" data-flowtype="' + (r.flow_type||'') + '" style="border-bottom:1px solid #f0f0f0;">';
                    h += '<td style="padding:8px;font-size:11px;color:#999;">' + (r.trans_date||'') + '</td>';
                    h += '<td style="font-size:11px;color:#999;">' + (r.trans_time||'') + '</td>';
                    h += '<td>' + r.stock_code + '</td>';
                    h += '<td>' + stockLink(r.stock_code, r.short_name) + '</td>';
                    h += '<td><span style="color:' + srcColor + ';font-weight:600;">' + srcLabel + '</span></td>';
                    h += '<td><span style="color:' + (isBuy?'#e74c3c':'#27ae60') + ';font-weight:600;">' + (isBuy?'买入':'卖出') + '</span></td>';
                    h += '<td style="color:' + (strategyColors[r.strategy_type]||'#999') + ';">' + (strategyNames[r.strategy_type]||'-') + '</td>';
                    h += '<td>' + (r.price||0).toFixed(2) + '</td>';
                    h += '<td>' + (r.shares||0) + '</td>';
                    h += '<td>' + fmtMoney(r.amount||0) + '</td>';
                    h += '<td>' + (r.fee||0).toFixed(2) + '</td>';
                    h += '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + escAttr(parseReason(r.reason)) + '">' + parseReason(r.reason) + '</td>';
                    h += '<td>' + analysisScoreText(blendedAnalysisRowScore(r)) + '</td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div>';
            }
            h += '</details>';
            h += '</div>';

            h += '<div id="simTradeStatus" style="color:#999;font-size:12px;text-align:center;padding:8px;">' + (window._simTradeLastStatus || '') + '</div>';

            container.innerHTML = h;
            if (mode === 'live') {
                var simStatus = document.getElementById('simTradeStatus');
                var simOrder = [
                    'decision-desk',
                    'stage-status',
                    'candidate-queue',
                    'holdings',
                    'capital',
                    'strategy-caption',
                    'strategies',
                    'win-history',
                    'trade-history',
                    'flow-history'
                ];
                var simFrag = document.createDocumentFragment();
                simOrder.forEach(function(section) {
                    var node = container.querySelector('[data-sim-section="' + section + '"]');
                    if (node) simFrag.appendChild(node);
                });
                if (simStatus) container.insertBefore(simFrag, simStatus);
                else container.appendChild(simFrag);
            }
            window._simTradeLastStatus = '';
            setStatus(mode === 'backtest' ? '策略回测已加载' : (mode === 'forward' ? 'T+1验证已加载' : '模拟交易已加载'));
        }).catch(function(e) {
            container.innerHTML = '<div class="loading" style="color:#e74c3c">❌ 加载失败: ' + e.message + '</div>';
        });
    }

    function loadStrategyBacktestPage(container) {
        container.innerHTML = '<div class="loading"><span class="spinner"></span> 加载策略回测数据...</div>';
        window._strategyBacktestContainer = container;

        var strategyOrder = ['ultra_short', 'short_term', 'swing', 'main_wave'];
        var strategyNames = {
            ultra_short: '超短',
            short_term: '短线',
            swing: '波段',
            main_wave: '主升浪'
        };
        var strategyFullNames = {
            ultra_short: '超短快进快出',
            short_term: '短线趋势跟随',
            swing: '波段持有',
            main_wave: '主升趋势持有'
        };
        var strategyColors = {
            ultra_short: '#e74c3c',
            short_term: '#f39c12',
            swing: '#3498db',
            main_wave: '#8e44ad'
        };
        var defaultEnd = new Date().toISOString().slice(0, 10);
        var defaultStart = defaultEnd.slice(0, 4) + '-01-01';
        var selectedStrategies = window._strategyBacktestStrategies || strategyOrder.slice();
        var initialCapital = Number(window._strategyBacktestCapital || 1000000);

        function fmtMoney(v) {
            var n = Number(v || 0);
            var sign = n < 0 ? '-' : '';
            n = Math.abs(n);
            if (n >= 1e8) return sign + (n / 1e8).toFixed(2) + '亿';
            if (n >= 1e4) return sign + (n / 1e4).toFixed(2) + '万';
            return sign + n.toFixed(0);
        }
        function fmtRate(v) {
            var n = Number(v || 0);
            return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
        }
        function fmtRatio(v) {
            var n = Number(v || 0);
            return n > 0 ? n.toFixed(2) : '-';
        }
        function pnlColor(v) {
            return Number(v || 0) >= 0 ? '#dc2626' : '#16a34a';
        }
        function metricCard(label, value, note, color) {
            return '<div style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;box-shadow:0 1px 8px rgba(15,23,42,.04);">'
                + '<div style="font-size:12px;color:#64748b;margin-bottom:5px;">' + label + '</div>'
                + '<div style="font-size:22px;font-weight:900;color:' + (color || '#0f172a') + ';">' + value + '</div>'
                + '<div style="font-size:11px;color:#94a3b8;margin-top:6px;">' + (note || '') + '</div>'
                + '</div>';
        }
        function stockLink(code, name) {
            code = String(code || '').padStart(6, '0');
            var label = name || code;
            return '<a href="javascript:void(0)" onclick="openKlineModal(\'' + code + '\',\'' + escAttr(label) + '\')" class="clickable-name">' + label + '</a>';
        }
        function miniLine(points, key, color, emptyText) {
            points = points || [];
            if (!points.length) {
                return '<div style="height:110px;display:flex;align-items:center;justify-content:center;color:#94a3b8;font-size:12px;background:#f8fafc;border-radius:12px;">' + (emptyText || '暂无曲线数据') + '</div>';
            }
            var w = 520, h = 110, pad = 10;
            var values = points.map(function(p){ return Number(p[key] || 0); });
            var min = Math.min.apply(null, values);
            var max = Math.max.apply(null, values);
            if (max === min) { max += 1; min -= 1; }
            var path = points.map(function(p, i) {
                var x = pad + (w - pad * 2) * (points.length === 1 ? 0 : i / (points.length - 1));
                var y = h - pad - (Number(p[key] || 0) - min) / (max - min) * (h - pad * 2);
                return (i === 0 ? 'M' : 'L') + x.toFixed(1) + ' ' + y.toFixed(1);
            }).join(' ');
            var first = points[0] || {};
            var last = points[points.length - 1] || {};
            return '<div style="background:#f8fafc;border-radius:12px;padding:10px;">'
                + '<svg viewBox="0 0 ' + w + ' ' + h + '" style="width:100%;height:110px;display:block;">'
                + '<path d="' + path + '" fill="none" stroke="' + color + '" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>'
                + '</svg>'
                + '<div style="display:flex;justify-content:space-between;color:#94a3b8;font-size:11px;"><span>' + (first.date || '-') + '</span><span>' + (last.date || '-') + '</span></div>'
                + '</div>';
        }
        function strategyCheckboxes() {
            return strategyOrder.map(function(st) {
                var checked = selectedStrategies.indexOf(st) >= 0 ? 'checked' : '';
                return '<label style="display:inline-flex;align-items:center;gap:6px;padding:7px 10px;border:1px solid #e2e8f0;border-radius:999px;background:#fff;font-size:12px;color:#334155;cursor:pointer;">'
                    + '<input type="checkbox" class="btStrategy" value="' + st + '" ' + checked + '> '
                    + '<span style="font-weight:800;color:' + strategyColors[st] + ';">' + strategyNames[st] + '</span>'
                    + '</label>';
            }).join('');
        }
        function selectedStrategyValues() {
            var nodes = document.querySelectorAll('.btStrategy:checked');
            var arr = [];
            nodes.forEach(function(n){ arr.push(n.value); });
            return arr.length ? arr : strategyOrder.slice();
        }

        var reportUrl = '/api/sim-trade/backtest/report?strategy_types=' + encodeURIComponent(selectedStrategies.join(','))
            + '&initial_capital=' + encodeURIComponent(initialCapital);

        fetch(reportUrl)
            .then(function(r){ return r.json(); })
            .then(function(res) {
                if (res.status && res.status !== 'ok') {
                    container.innerHTML = '<div class="loading" style="color:#e74c3c">策略回测加载失败: ' + (res.error || '未知错误') + '</div>';
                    return;
                }
                var sum = res.summary || {};
                var byStrategy = res.by_strategy || {};
                var recent = res.recent_trades || [];
                var openPositions = res.open_positions || [];
                var distribution = res.profit_distribution || [];
                var dailyPnl = res.daily_pnl || [];
                var equityCurve = res.equity_curve || [];
                var drawdownCurve = res.drawdown_curve || [];
                var lastStatus = window._strategyBacktestLastStatus || '';

                var h = simTradeModeNav('backtest');
                h += '<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px;">';
                h += '<span style="padding:8px 12px;border-radius:10px;background:#1a73e8;color:#fff;font-weight:800;">策略回测</span>';
                h += '<span style="font-size:12px;color:#64748b;">独立回测台：按 AI 推荐信号 T 日、T+1 开盘模拟买入，按策略规则自动卖出。</span>';
                h += '</div>';

                h += '<div style="background:linear-gradient(135deg,#f8fbff,#fffaf0);border:1px solid #dbeafe;border-radius:16px;padding:16px;margin-bottom:16px;box-shadow:0 6px 18px rgba(30,64,175,.06);">';
                h += '<div style="display:flex;justify-content:space-between;gap:14px;align-items:flex-start;flex-wrap:wrap;margin-bottom:12px;">';
                h += '<div><div style="font-size:18px;font-weight:900;color:#0f172a;">多策略历史回测工作台</div>';
                h += '<div style="font-size:12px;color:#64748b;margin-top:5px;line-height:1.7;">用于验证不同策略的胜率、盈亏比、回撤和资金曲线。这里是模拟回测，不会触发真实下单。</div></div>';
                h += '<button onclick="runStrategyBacktest()" style="padding:9px 16px;border:none;border-radius:10px;background:#e67e22;color:#fff;cursor:pointer;font-size:13px;font-weight:800;">运行回测</button>';
                h += '</div>';
                h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;align-items:end;">';
                h += '<label style="font-size:12px;color:#64748b;">开始日期<input id="btStartDate" type="date" value="' + (window._strategyBacktestStart || defaultStart) + '" style="margin-top:5px;width:100%;padding:8px;border:1px solid #dbe3ee;border-radius:8px;"></label>';
                h += '<label style="font-size:12px;color:#64748b;">结束日期<input id="btEndDate" type="date" value="' + (window._strategyBacktestEnd || defaultEnd) + '" style="margin-top:5px;width:100%;padding:8px;border:1px solid #dbe3ee;border-radius:8px;"></label>';
                h += '<label style="font-size:12px;color:#64748b;">本金<input id="btInitialCapital" type="number" value="' + initialCapital + '" step="10000" min="10000" style="margin-top:5px;width:100%;padding:8px;border:1px solid #dbe3ee;border-radius:8px;"></label>';
                h += '<div style="font-size:12px;color:#64748b;">策略选择<div style="display:flex;gap:7px;flex-wrap:wrap;margin-top:5px;">' + strategyCheckboxes() + '</div></div>';
                h += '</div>';
                h += '<div id="strategyBacktestStatus" style="margin-top:10px;font-size:12px;color:' + (lastStatus.indexOf('失败') >= 0 ? '#dc2626' : '#64748b') + ';">' + (lastStatus || '打开页面只读取最近一次回测结果；点击“运行回测”会清空旧 backtest 模式结果后重跑。') + '</div>';
                h += '</div>';

                h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:16px;">';
                h += metricCard('总收益', fmtMoney(sum.total_profit || 0), '已平仓实现收益', pnlColor(sum.total_profit || 0));
                h += metricCard('收益率', fmtRate(sum.total_return_rate || 0), '相对本金', pnlColor(sum.total_return_rate || 0));
                h += metricCard('胜率', Number(sum.win_rate || 0).toFixed(2) + '%', '盈利笔数 / 已平仓', '#2563eb');
                h += metricCard('最大回撤', Number(sum.max_drawdown || 0).toFixed(2) + '%', '按每日实现盈亏曲线', '#16a34a');
                h += metricCard('Profit Factor', fmtRatio(sum.profit_factor), '总盈利 / 总亏损', '#7c3aed');
                h += metricCard('盈亏比', fmtRatio(sum.profit_loss_ratio), '平均盈利率 / 平均亏损率', '#ea580c');
                h += metricCard('已平/未平', Number(sum.closed_count || 0) + ' / ' + Number(sum.holding_count || 0), '未平成本 ' + fmtMoney(sum.holding_amount || 0), '#0f172a');
                h += metricCard('期末权益', fmtMoney(sum.ending_equity || initialCapital), '本金 + 已实现收益', pnlColor((sum.ending_equity || initialCapital) - initialCapital));
                h += '</div>';

                h += '<div style="display:grid;grid-template-columns:minmax(300px,1.2fr) minmax(260px,.8fr);gap:14px;margin-bottom:16px;">';
                h += '<div style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;">';
                h += '<div style="font-size:15px;font-weight:900;color:#0f172a;margin-bottom:8px;">资金曲线</div>';
                h += miniLine(equityCurve, 'equity', '#2563eb', '暂无资金曲线，先运行一次回测');
                h += '</div>';
                h += '<div style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;">';
                h += '<div style="font-size:15px;font-weight:900;color:#0f172a;margin-bottom:8px;">回撤曲线</div>';
                h += miniLine(drawdownCurve, 'drawdown', '#16a34a', '暂无回撤曲线');
                h += '</div>';
                h += '</div>';

                h += '<div style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;margin-bottom:16px;">';
                h += '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px;">';
                h += '<div style="font-size:15px;font-weight:900;color:#0f172a;">策略对比</div>';
                h += '<div style="font-size:12px;color:#94a3b8;">同一推荐池下，对比不同交易风格的结果</div>';
                h += '</div>';
                h += '<div style="overflow:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;min-width:820px;">';
                h += '<thead><tr style="text-align:left;color:#64748b;border-bottom:1px solid #e8edf3;"><th style="padding:8px;">策略</th><th>已平</th><th>未平</th><th>胜率</th><th>总收益</th><th>平均收益率</th><th>最大盈利</th><th>最大亏损</th><th>PF</th><th>盈亏比</th></tr></thead><tbody>';
                strategyOrder.forEach(function(st) {
                    if (selectedStrategies.indexOf(st) < 0) return;
                    var s = byStrategy[st] || {};
                    h += '<tr style="border-bottom:1px solid #f1f5f9;">';
                    h += '<td style="padding:8px;"><span style="font-weight:900;color:' + strategyColors[st] + ';">' + (s.name || strategyFullNames[st] || st) + '</span><div style="font-size:11px;color:#94a3b8;">' + st + '</div></td>';
                    h += '<td>' + Number(s.closed_count || 0) + '</td>';
                    h += '<td>' + Number(s.holding_count || 0) + '</td>';
                    h += '<td>' + Number(s.win_rate || 0).toFixed(2) + '%</td>';
                    h += '<td style="font-weight:800;color:' + pnlColor(s.total_profit || 0) + ';">' + fmtMoney(s.total_profit || 0) + '</td>';
                    h += '<td style="color:' + pnlColor(s.avg_profit_rate || 0) + ';">' + fmtRate(s.avg_profit_rate || 0) + '</td>';
                    h += '<td style="color:#dc2626;">' + fmtRate(s.max_profit_rate || 0) + '</td>';
                    h += '<td style="color:#16a34a;">' + fmtRate(s.max_loss_rate || 0) + '</td>';
                    h += '<td>' + fmtRatio(s.profit_factor) + '</td>';
                    h += '<td>' + fmtRatio(s.profit_loss_ratio) + '</td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div></div>';

                h += '<div style="display:grid;grid-template-columns:minmax(260px,.75fr) minmax(320px,1.25fr);gap:14px;margin-bottom:16px;">';
                h += '<div style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;">';
                h += '<div style="font-size:15px;font-weight:900;color:#0f172a;margin-bottom:10px;">收益分布</div>';
                var maxBucket = Math.max.apply(null, distribution.map(function(x){ return Number(x.count || 0); }).concat([1]));
                distribution.forEach(function(b) {
                    var pct = Math.max(2, Number(b.count || 0) / maxBucket * 100);
                    h += '<div style="display:flex;align-items:center;gap:8px;margin:8px 0;font-size:12px;">';
                    h += '<span style="width:58px;color:#64748b;">' + b.range + '%</span>';
                    h += '<div style="flex:1;height:9px;background:#f1f5f9;border-radius:999px;overflow:hidden;"><div style="width:' + pct + '%;height:100%;background:#2563eb;border-radius:999px;"></div></div>';
                    h += '<span style="width:36px;text-align:right;color:#0f172a;font-weight:800;">' + Number(b.count || 0) + '</span>';
                    h += '</div>';
                });
                h += '</div>';
                h += '<div style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;">';
                h += '<div style="font-size:15px;font-weight:900;color:#0f172a;margin-bottom:10px;">每日实现盈亏</div>';
                if (!dailyPnl.length) {
                    h += '<div style="height:150px;display:flex;align-items:center;justify-content:center;color:#94a3b8;font-size:12px;background:#f8fafc;border-radius:12px;">暂无每日盈亏数据</div>';
                } else {
                    h += '<div style="max-height:190px;overflow:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;">';
                    h += '<thead><tr style="text-align:left;color:#64748b;border-bottom:1px solid #e8edf3;"><th style="padding:7px;">日期</th><th>盈亏</th><th>平仓数</th></tr></thead><tbody>';
                    dailyPnl.slice().reverse().slice(0, 30).forEach(function(d) {
                        h += '<tr style="border-bottom:1px solid #f1f5f9;"><td style="padding:7px;">' + (d.date || '-') + '</td><td style="font-weight:800;color:' + pnlColor(d.pnl || 0) + ';">' + fmtMoney(d.pnl || 0) + '</td><td>' + Number(d.count || 0) + '</td></tr>';
                    });
                    h += '</tbody></table></div>';
                }
                h += '</div></div>';

                h += '<div style="background:#fff;border:1px solid #e8edf3;border-radius:14px;padding:14px;margin-bottom:16px;">';
                h += '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px;">';
                h += '<div style="font-size:15px;font-weight:900;color:#0f172a;">最近回测成交</div>';
                h += '<div style="font-size:12px;color:#94a3b8;">展示最近 80 笔已平仓交易</div>';
                h += '</div>';
                if (!recent.length) {
                    h += '<div style="padding:20px;text-align:center;color:#94a3b8;background:#f8fafc;border-radius:12px;">暂无成交。选择日期和策略后运行一次回测。</div>';
                } else {
                    h += '<div style="overflow:auto;max-height:360px;"><table style="width:100%;border-collapse:collapse;font-size:12px;min-width:920px;">';
                    h += '<thead><tr style="text-align:left;color:#64748b;border-bottom:1px solid #e8edf3;position:sticky;top:0;background:#fff;"><th style="padding:8px;">股票</th><th>策略</th><th>信号日</th><th>买入日</th><th>卖出日</th><th>买入价</th><th>卖出价</th><th>收益</th><th>收益率</th><th>持仓天数</th><th>卖出原因</th></tr></thead><tbody>';
                    recent.forEach(function(t) {
                        h += '<tr style="border-bottom:1px solid #f1f5f9;">';
                        h += '<td style="padding:8px;">' + stockLink(t.stock_code, t.short_name) + '<div style="font-size:11px;color:#94a3b8;">' + (t.stock_code || '-') + '</div></td>';
                        h += '<td><span style="font-weight:800;color:' + (strategyColors[t.strategy_type] || '#64748b') + ';">' + (t.strategy_name || t.strategy_type || '-') + '</span></td>';
                        h += '<td>' + (t.signal_date || '-') + '</td>';
                        h += '<td>' + (t.buy_date || '-') + '</td>';
                        h += '<td>' + (t.sell_date || '-') + '</td>';
                        h += '<td>' + Number(t.buy_price || 0).toFixed(2) + '</td>';
                        h += '<td>' + Number(t.sell_price || 0).toFixed(2) + '</td>';
                        h += '<td style="font-weight:800;color:' + pnlColor(t.profit || 0) + ';">' + fmtMoney(t.profit || 0) + '</td>';
                        h += '<td style="font-weight:800;color:' + pnlColor(t.profit_rate || 0) + ';">' + fmtRate(t.profit_rate || 0) + '</td>';
                        h += '<td>' + Number(t.holding_days || 0) + '</td>';
                        h += '<td style="max-width:220px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="' + escAttr(t.sell_reason || '') + '">' + (t.sell_reason || '-') + '</td>';
                        h += '</tr>';
                    });
                    h += '</tbody></table></div>';
                }
                h += '</div>';

                if (openPositions.length) {
                    h += '<div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:14px;padding:14px;margin-bottom:16px;">';
                    h += '<div style="font-size:15px;font-weight:900;color:#9a3412;margin-bottom:10px;">未平仓回测持仓</div>';
                    h += '<div style="font-size:12px;color:#b45309;margin-bottom:8px;">如果这里长期不为空，说明回测结束日期太近，策略还没自然触发卖出，可把结束日期往后放。</div>';
                    h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;">';
                    openPositions.slice(0, 12).forEach(function(p) {
                        h += '<div style="background:#fff;border-radius:10px;padding:10px;border:1px solid #fed7aa;">';
                        h += '<div style="font-weight:800;">' + stockLink(p.stock_code, p.short_name) + '</div>';
                        h += '<div style="font-size:11px;color:#94a3b8;margin-top:3px;">' + (p.strategy_name || p.strategy_type) + ' · 买入 ' + (p.buy_date || '-') + '</div>';
                        h += '<div style="font-size:12px;color:#64748b;margin-top:6px;">成本 ' + fmtMoney(p.buy_amount || 0) + ' / ' + Number(p.buy_price || 0).toFixed(2) + '</div>';
                        h += '</div>';
                    });
                    h += '</div></div>';
                }

                container.innerHTML = h;
                window._strategyBacktestLastStatus = '';
                if (typeof setStatus === 'function') setStatus('策略回测已加载');
            })
            .catch(function(e) {
                container.innerHTML = '<div class="loading" style="color:#e74c3c">策略回测加载失败: ' + e.message + '</div>';
            });

        window._getStrategyBacktestSelected = selectedStrategyValues;
    }

    window.simTradeSetMode = function(mode) {
        window._simTradeMode = mode || 'live';
        if (window._simTradeMode === 'backtest') {
            switchTab('strategy-backtest');
            return;
        }
        if (window._simTradeMode === 'live') {
            switchTab('sim-trade');
            return;
        }
        loadSimTradePage(el('tab-sim-trade'), window._simTradeMode);
    };

    // 筛选历史记录
    window.filterSimHistory = function() {
        var filter = document.getElementById('simHistFilter').value;
        document.querySelectorAll('.sim-hist-row').forEach(function(row) {
            if (!filter || row.getAttribute('data-st') === filter) row.style.display = '';
            else row.style.display = 'none';
        });
    };

    // 筛选流水
    window.filterSimFlow = function() {
        var src = document.getElementById('simFlowSource').value;
        var ft = document.getElementById('simFlowType').value;
        document.querySelectorAll('.sim-flow-row').forEach(function(row) {
            var matchSrc = !src || row.getAttribute('data-source') === src;
            var matchFt = !ft || row.getAttribute('data-flowtype') === ft;
            row.style.display = (matchSrc && matchFt) ? '' : 'none';
        });
    };

    // 手动扫描信号
    window.simTradeScan = function() {
        var statusEl = document.getElementById('simTradeStatus');
        if (statusEl) statusEl.innerHTML = '⏳ 正在扫描交易信号...';
        fetch('/api/sim-trade/scan', {method: 'POST'})
            .then(function(r){return r.json()})
            .then(function(res) {
                if (res.status === 'ok') {
                    var r = res.results || {};
                    var sold = (r.sell_signals||[]).length;
                    var forwardSold = (r.forward_sell_signals||[]).length;
                    var bought = 0;
                    Object.values(r.buy_signals||{}).forEach(function(arr){ bought += arr.length; });
                    if (statusEl) statusEl.innerHTML = '✅ 扫描完成: 实时卖出 ' + sold + ' 笔, 验证卖出 ' + forwardSold + ' 笔, 买入 ' + bought + ' 笔';
                    setTimeout(function(){ loadSimTradePage(el('tab-sim-trade'), window._simTradeMode || 'live'); }, 1500);
                } else {
                    if (statusEl) statusEl.innerHTML = '❌ ' + (res.error||'扫描失败');
                }
            })
            .catch(function(e) {
                if (statusEl) statusEl.innerHTML = '❌ 网络错误: ' + e.message;
            });
    };

    // 盘中验证：信号日T，验证日T+1开盘分时买入
    window.simTradeForwardStart = function() {
        var today = new Date().toISOString().slice(0, 10);
        var signalDate = prompt('AI推荐信号日 (YYYY-MM-DD)，留空使用验证日前一个交易日');
        if (signalDate === null) return;
        var tradeDate = prompt('验证买入日 (YYYY-MM-DD)，留空使用今天', today);
        if (tradeDate === null) return;
        var endDate = prompt('验证结束日 (YYYY-MM-DD)，留空只创建/更新验证仓位；填日期则用分钟线回放卖点');
        if (endDate === null) return;
        var doReset = confirm('是否清空旧的盘中验证结果后重新开始？\n确定=清空重跑，取消=增量/避免重复买入');
        var statusEl = document.getElementById('simTradeStatus');
        if (statusEl) statusEl.innerHTML = '⏳ 正在启动盘中验证...';
        var params = [];
        if (signalDate) params.push('signal_date=' + encodeURIComponent(signalDate));
        if (tradeDate) params.push('trade_date=' + encodeURIComponent(tradeDate));
        if (endDate) params.push('end_date=' + encodeURIComponent(endDate));
        if (doReset) params.push('reset=true');
        fetch('/api/sim-trade/forward/start?' + params.join('&'), {method:'POST'})
            .then(function(r){return r.json()})
            .then(function(res) {
                if (res.status === 'ok') {
                    var summary = ((res.stats || {}).summary || {});
                    var skip = res.skipped || {};
                    var skipMsg = Object.keys(skip).length ? ', 跳过 ' + Object.keys(skip).map(function(k){ return k + ':' + skip[k]; }).join(' ') : '';
                    var note = res.data_note ? '；' + res.data_note : '';
                    var msg = '✅ 盘中验证: 信号日' + res.signal_date + ', 验证日' + res.trade_date + ', 买入' + res.total_bought + '笔, 卖出' + res.total_sold + '笔, 近3月均收' + fmtPlainRate(summary.avg_return_3m || 0) + ', 回撤' + Number(summary.max_drawdown_3m || 0).toFixed(2) + '%, PF ' + fmtRatio(summary.profit_factor_3m) + skipMsg + note;
                    if (statusEl) statusEl.innerHTML = msg;
                    window._simTradeMode = 'forward';
                    window._simTradeLastStatus = msg;
                    setTimeout(function(){ loadSimTradePage(el('tab-sim-trade'), 'forward'); }, 1200);
                } else {
                    if (statusEl) statusEl.innerHTML = '❌ ' + (res.error || '盘中验证失败');
                }
            })
            .catch(function(e) {
                if (statusEl) statusEl.innerHTML = '❌ 网络错误: ' + e.message;
            });
    };

    // 盘中验证：实时扫描验证仓位卖点
    window.simTradeForwardScan = function() {
        var statusEl = document.getElementById('simTradeStatus');
        if (statusEl) statusEl.innerHTML = '⏳ 正在扫描盘中验证卖点...';
        fetch('/api/sim-trade/forward/scan', {method:'POST'})
            .then(function(r){return r.json()})
            .then(function(res) {
                if (res.status === 'ok') {
                    var summary = ((res.stats || {}).summary || {});
                    var msg = '✅ 验证卖点扫描完成: 卖出 ' + res.sell_count + ' 笔, 近3月均收' + fmtPlainRate(summary.avg_return_3m || 0) + ', PF ' + fmtRatio(summary.profit_factor_3m) + ', 未平' + (summary.total_holding || 0) + '笔';
                    if (!res.is_trading_time) msg += '；当前非交易时间，只展示已有验证结果';
                    if (statusEl) statusEl.innerHTML = msg;
                    window._simTradeMode = 'forward';
                    window._simTradeLastStatus = msg;
                    setTimeout(function(){ loadSimTradePage(el('tab-sim-trade'), 'forward'); }, 1200);
                } else {
                    if (statusEl) statusEl.innerHTML = '❌ ' + (res.error || '扫描失败');
                }
            })
            .catch(function(e) {
                if (statusEl) statusEl.innerHTML = '❌ 网络错误: ' + e.message;
            });
    };

    // 回测
    window.runStrategyBacktest = function() {
        var startEl = document.getElementById('btStartDate');
        var endEl = document.getElementById('btEndDate');
        var capitalEl = document.getElementById('btInitialCapital');
        var statusEl = document.getElementById('strategyBacktestStatus') || document.getElementById('simTradeStatus');
        var start = startEl ? startEl.value : '';
        var end = endEl ? endEl.value : '';
        var capital = Number((capitalEl || {}).value || 1000000);
        var strategies = [];
        document.querySelectorAll('.btStrategy:checked').forEach(function(n){ strategies.push(n.value); });
        if (!strategies.length) {
            alert('至少选择一个策略');
            return;
        }
        window._strategyBacktestStart = start;
        window._strategyBacktestEnd = end;
        window._strategyBacktestCapital = capital;
        window._strategyBacktestStrategies = strategies;

        function fmtMoney(v) {
            var n = Number(v || 0);
            var sign = n < 0 ? '-' : '';
            n = Math.abs(n);
            if (n >= 1e8) return sign + (n / 1e8).toFixed(2) + '亿';
            if (n >= 1e4) return sign + (n / 1e4).toFixed(2) + '万';
            return sign + n.toFixed(0);
        }
        function fmtRate(v) {
            var n = Number(v || 0);
            return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
        }

        if (statusEl) statusEl.innerHTML = '⏳ 正在运行策略回测，旧 backtest 结果会被清空后重算...';
        var params = [];
        if (start) params.push('start_date=' + encodeURIComponent(start));
        if (end) params.push('end_date=' + encodeURIComponent(end));
        params.push('strategy_types=' + encodeURIComponent(strategies.join(',')));
        params.push('initial_capital=' + encodeURIComponent(capital));
        fetch('/api/sim-trade/backtest?' + params.join('&'), {method: 'POST'})
            .then(function(r){ return r.json(); })
            .then(function(res) {
                if (res.status === 'ok') {
                    var report = res.report || {};
                    var sum = report.summary || ((res.stats || {}).summary || {});
                    var msg = '✅ 回测完成：信号日 ' + (res.signal_start_date || '-') + ' ~ ' + (res.signal_end_date || '-')
                        + '，买入 ' + Number(res.total_bought || 0) + ' 笔，卖出 ' + Number(res.total_sold || 0) + ' 笔'
                        + '，总收益 ' + fmtMoney(sum.total_profit || 0)
                        + '，收益率 ' + fmtRate(sum.total_return_rate || 0)
                        + '，最大回撤 ' + Number(sum.max_drawdown || 0).toFixed(2) + '%';
                    window._strategyBacktestLastStatus = msg;
                    window._simTradeLastStatus = msg;
                    var target = document.getElementById('tab-strategy-backtest') || window._strategyBacktestContainer || document.getElementById('tab-sim-trade');
                    if (target) {
                        setTimeout(function(){ loadStrategyBacktestPage(target); }, 500);
                    } else {
                        switchTab('strategy-backtest');
                    }
                } else {
                    var err = '❌ 回测失败：' + (res.error || '未知错误');
                    window._strategyBacktestLastStatus = err;
                    if (statusEl) statusEl.innerHTML = err;
                }
            })
            .catch(function(e) {
                var err = '❌ 网络错误：' + e.message;
                window._strategyBacktestLastStatus = err;
                if (statusEl) statusEl.innerHTML = err;
            });
    };

    window.simTradeBacktest = function() {
        if (document.getElementById('btStartDate')) {
            window.runStrategyBacktest();
            return;
        }
        var start = prompt('回测开始日期 (YYYY-MM-DD)，留空使用最早推荐数据');
        if (start === null) return;
        var end = prompt('回测结束日期 (YYYY-MM-DD)，留空使用今天');
        if (end === null) return;
        var statusEl = document.getElementById('simTradeStatus');
        if (statusEl) statusEl.innerHTML = '⏳ 正在回测...';
        var url = '/api/sim-trade/backtest?';
        if (start) url += 'start_date=' + start + '&';
        if (end) url += 'end_date=' + end;
        fetch(url, {method: 'POST'})
            .then(function(r){return r.json()})
            .then(function(res) {
                if (res.status === 'ok') {
                    var summary = ((res.stats || {}).summary || {});
                    var msg = '✅ 回测完成: 信号' + res.trade_days + '个交易日, 买入' + res.total_bought + '笔, 卖出' + res.total_sold + '笔, 近3月均收' + fmtPlainRate(summary.avg_return_3m || 0) + ', 回撤' + Number(summary.max_drawdown_3m || 0).toFixed(2) + '%, PF ' + fmtRatio(summary.profit_factor_3m) + ', 未平' + (summary.total_holding || 0) + '笔';
                    if (statusEl) statusEl.innerHTML = msg;
                    window._simTradeMode = 'backtest';
                    window._simTradeLastStatus = msg;
                    setTimeout(function(){ switchTab('strategy-backtest'); }, 1200);
                } else {
                    if (statusEl) statusEl.innerHTML = '❌ ' + (res.error||'回测失败');
                }
            })
            .catch(function(e) {
                if (statusEl) statusEl.innerHTML = '❌ 网络错误: ' + e.message;
            });
    };

    // 手动平仓(通过持仓ID)
    window.simTradeClosePos = function(posId) {
        if (!posId || !confirm('确认平仓？')) return;
        fetch('/api/sim-trade/close/' + posId, {method:'POST'})
            .then(function(r){return r.json()})
            .then(function(res) {
                if (res.status === 'ok') {
                    alert('平仓成功: 盈亏 ' + (res.profit||0).toFixed(2) + '元');
                    loadSimTradePage(el('tab-sim-trade'));
                } else {
                    alert('平仓失败: ' + (res.error||'未知错误'));
                }
            })
            .catch(function(e) { alert('网络错误: ' + e.message); });
    };

    function formatAmount(val) {
        if (val >= 1e12) return (val / 1e12).toFixed(2) + '万亿';
        if (val >= 1e8) return (val / 1e8).toFixed(2) + '亿';
        if (val >= 1e4) return (val / 1e4).toFixed(0) + '万';
        return val.toFixed(0);
    }

    var monitorRefreshTimer = null;
    var _monitorCharts = {};  // Chart.js 实例注册表，避免重复创建

    function loadMonitorPage(container, requestedDate) {
        // 清理旧图表实例
        Object.keys(_monitorCharts).forEach(function(k) {
            if (_monitorCharts[k]) { _monitorCharts[k].destroy(); delete _monitorCharts[k]; }
        });

        function fetchMonitorData() {
            var activeDate = requestedDate || currentDateValue();
            fetch('/api/monitor/data?date=' + encodeURIComponent(activeDate))
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    if (data.error) {
                        container.innerHTML = '<div class="loading" style="color:#e74c3c">❌ 数据加载失败: ' + data.error + '</div>';
                        return;
                    }
                    if (!document.getElementById('monitorGauge')) {
                        // 首次渲染：构建完整 DOM
                        renderMonitorPage(container, data);
                    } else {
                        // 刷新：仅更新数据和图表
                        updateMonitorPage(data);
                    }
                    var statusEl = document.getElementById('monitorRefreshStatus');
                    if (statusEl) {
                        statusEl.textContent = '上次刷新: ' + new Date().toLocaleTimeString();
                    }
                })
                .catch(function(error) {
                    var statusEl = document.getElementById('monitorRefreshStatus');
                    if (statusEl) {
                        statusEl.textContent = '刷新失败: ' + new Date().toLocaleTimeString();
                    }
                });
        }

        fetchMonitorData();

        if (monitorRefreshTimer) {
            clearInterval(monitorRefreshTimer);
        }
        monitorRefreshTimer = setInterval(fetchMonitorData, 60000);
    }

    window.stopMonitorRefresh = function() {
        if (monitorRefreshTimer) {
            clearInterval(monitorRefreshTimer);
            monitorRefreshTimer = null;
        }
        Object.keys(_monitorCharts).forEach(function(k) {
            if (_monitorCharts[k]) { _monitorCharts[k].destroy(); delete _monitorCharts[k]; }
        });
    };

    /* 更新仪表盘数据（不重建 DOM，只更新数值和图表数据） */
    function updateMonitorPage(data) {
        var heatVal = data.market_heat || 0;
        var heatColor = heatVal <= 200 ? '#66bb6a' : heatVal <= 400 ? '#bdbdbd' : heatVal <= 600 ? '#ef5350' : '#c62828';

        // 更新仪表盘数值
        var gv = document.querySelector('.gauge-value');
        if (gv) { gv.style.color = heatColor; gv.textContent = heatVal; }
        var gl = document.querySelector('.gauge-label');
        if (gl) { gl.style.color = heatColor; gl.textContent = data.heat_status || '-'; }
        var gc = document.querySelector('.gauge-change');
        if (gc) {
            gc.className = 'gauge-change ' + (data.heat_change >= 0 ? 'positive' : 'negative');
            gc.textContent = '较昨日 ' + (data.heat_change >= 0 ? '↑ +' : '↓ ') + Math.abs(data.heat_change || 0).toFixed(2) + '%';
        }
        drawGauge(heatVal);

        // 更新更新时间
        var rtBadge = data.is_realtime ? ' <span style="color:#e74c3c">●实时</span>' : '';
        var subtitle = document.querySelector('.monitor-subtitle');
        if (subtitle) {
            subtitle.innerHTML = '请求 ' + (data.requested_date || '-') + ' / 交易日 ' + (data.trade_date || '-') + ' / 更新 ' + (data.update_time || '-') + rtBadge + ' | <span id="monitorRefreshStatus">自动刷新中...</span>';
        }

        // 更新分析文本
        var aLabels = ['market_temp', 'industry_focus', 'style_judge', 'capital_flow', 'signal'];
        var aItems = document.querySelectorAll('.analysis-item .analysis-content');
        aItems.forEach(function(el, i) {
            if (aLabels[i] && data.analysis && data.analysis[aLabels[i]]) {
                el.textContent = data.analysis[aLabels[i]];
            }
        });

        // 更新统计数值
        var tmtEl = document.getElementById('tmtRatio');
        if (tmtEl) tmtEl.textContent = (data.tmt_ratio || 0).toFixed(2) + '%';
        var csiHeatEl = document.getElementById('csi1000Heat');
        if (csiHeatEl) csiHeatEl.textContent = data.csi1000.heat || '-';
        var csiChgEl = document.getElementById('csi1000Chg');
        if (csiChgEl) csiChgEl.textContent = (data.csi1000.change >= 0 ? '+' : '') + (data.csi1000.change || 0).toFixed(2) + '%';
        var sideEl = document.getElementById('sidelineRatio');
        if (sideEl) sideEl.textContent = (data.sideline_ratio || 0).toFixed(2) + '%';

        // 更新概念表格
        var tbody = document.getElementById('conceptTableBody');
        if (tbody && data.concept_rows) {
            var th = '';
            data.concept_rows.forEach(function(r) {
                var tagCls = r.change >= 0 ? 'tag-up' : 'tag-down';
                var tagText = r.change >= 0 ? '↑ 上涨' : '↓ 下跌';
                th += '<tr><td>' + r.name + '</td><td class="value">' + r.heat + '</td>';
                th += '<td><span class="tag ' + tagCls + '">' + tagText + ' ' + Math.abs(r.change).toFixed(2) + '%</span></td></tr>';
            });
            tbody.innerHTML = th;
        }

        // 更新摘要数值
        var indRows = document.querySelectorAll('.indicator-row .indicator-value');
        if (indRows.length >= 4) {
            indRows[0].textContent = data.up_count || 0;
            indRows[1].textContent = data.down_count || 0;
            indRows[2].textContent = (data.sideline_ratio || 0).toFixed(2) + '%';
            indRows[3].textContent = data.total_amount ? (data.total_amount / 1e8).toFixed(0) + '亿' : '-';
        }

        // 更新图表数据（复用已有 Chart 实例，不重建）
        setTimeout(function() {
            updateChart('heatChart', data.history.dates, [
                { data: data.history.heat },
                { data: data.history.amount }
            ]);
            if (data.top_industries && data.top_industries.length > 0) {
                updateChart('industryChart',
                    data.top_industries.map(function(r) { return r.name; }),
                    [{ data: data.top_industries.map(function(r) { return r.heat; }),
                       backgroundColor: data.top_industries.map(function(r) { return r.change >= 0 ? '#d32f2f' : '#2e7d32'; }) }]
                );
            }
            if (data.history && data.history.tmt_ratio) {
                updateChart('tmtChart', data.history.dates, [{ data: data.history.tmt_ratio }]);
            }
            if (data.csi1000 && data.history && data.history.csi1000_heat) {
                updateChart('csi1000Chart', data.history.dates, [{ data: data.history.csi1000_heat }]);
            }
            if (data.concept_rows && data.concept_rows.length > 0) {
                updateChart('conceptChart',
                    data.concept_rows.map(function(r) { return r.name; }),
                    [{ data: data.concept_rows.map(function(r) { return r.heat; }) }]
                );
            }
            if (data.history && data.history.sideline) {
                updateChart('sidelineChart', data.history.dates, [{ data: data.history.sideline }]);
            }
        }, 50);
    }

    /* 更新已有的 Chart.js 实例数据（不销毁重建） */
    function updateChart(canvasId, labels, datasets) {
        var chart = _monitorCharts[canvasId];
        if (!chart) return;
        chart.data.labels = labels;
        datasets.forEach(function(ds, i) {
            if (chart.data.datasets[i]) {
                chart.data.datasets[i].data = ds.data;
                if (ds.backgroundColor !== undefined) chart.data.datasets[i].backgroundColor = ds.backgroundColor;
            }
        });
        chart.update('none');  // 'none' 跳过动画，直接刷新
    }

    function renderMonitorPage(container, data) {
        var h = '';
        var heatVal = data.market_heat || 0;
        var heatColor = heatVal <= 200 ? '#66bb6a' : heatVal <= 400 ? '#bdbdbd' : heatVal <= 600 ? '#ef5350' : '#c62828';

        h += '<div class="monitor-header">';
        h += '<div class="monitor-title">A股市场监控中心</div>';
        h += '<div class="monitor-subtitle">';
        const rtBadge = data.is_realtime ? ' <span style="color:#e74c3c">●实时</span>' : '';
        h += '请求 ' + (data.requested_date || '-') + ' / 交易日 ' + (data.trade_date || '-') + ' / 更新 ' + (data.update_time || '-') + rtBadge + ' | ';
        h += '<span id="monitorRefreshStatus">自动刷新中...</span>';
        h += '</div>';
        h += '</div>';

        h += '<div class="monitor-grid monitor-grid-row1" style="margin-bottom:16px">';
        h += '<div class="monitor-card gauge-card">';
        h += '<div class="card-title"><span class="card-tag">FIG1 · 核心</span> 恐慌贪婪指数</div>';
        h += '<div class="gauge-container"><canvas id="monitorGauge" width="260" height="150"></canvas></div>';
        h += '<div class="gauge-value" style="color:' + heatColor + '">' + heatVal + '</div>';
        h += '<div class="gauge-label" style="color:' + heatColor + '">' + (data.heat_status || '-') + '</div>';
        h += '<div class="gauge-change ' + (data.heat_change >= 0 ? 'positive' : 'negative') + '">';
        h += '较昨日 ' + (data.heat_change >= 0 ? '↑ +' : '↓ ') + Math.abs(data.heat_change || 0).toFixed(2) + '%';
        h += '</div>';
        h += '<div class="gauge-legend">';
        h += '<span><i class="dot" style="background:#2e7d32"></i>极度恐慌</span>';
        h += '<span><i class="dot" style="background:#66bb6a"></i>恐慌</span>';
        h += '<span><i class="dot" style="background:#bdbdbd"></i>中性</span>';
        h += '<span><i class="dot" style="background:#ef5350"></i>贪婪</span>';
        h += '<span><i class="dot" style="background:#c62828"></i>极度贪婪</span>';
        h += '</div>';
        h += '</div>';

        h += '<div class="monitor-card analysis-card">';
        h += '<div class="card-title"><span class="card-tag">FIG2 · 判断</span> 今日市场综合判断</div>';
        h += '<div class="analysis-section">';
        h += '<div class="analysis-item"><span class="analysis-label">【市场温度】</span><span class="analysis-content">' + (data.analysis.market_temp || '-') + '</span></div>';
        h += '<div class="analysis-item"><span class="analysis-label">【行业焦点】</span><span class="analysis-content">' + (data.analysis.industry_focus || '-') + '</span></div>';
        h += '<div class="analysis-item"><span class="analysis-label">【风格判断】</span><span class="analysis-content">' + (data.analysis.style_judge || '-') + '</span></div>';
        h += '<div class="analysis-item"><span class="analysis-label">【资金流向】</span><span class="analysis-content">' + (data.analysis.capital_flow || '-') + '</span></div>';
        h += '<div class="analysis-item"><span class="analysis-label">【后市信号】</span><span class="analysis-content signal">' + (data.analysis.signal || '-') + '</span></div>';
        h += '</div>';
        h += '</div>';
        h += '</div>';

        h += '<div class="monitor-grid monitor-grid-row2" style="margin-bottom:16px">';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">FIG3 · 趋势</span> 全A热度 + 成交额</div>';
        h += '<div class="chart-container"><canvas id="heatChart"></canvas></div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">FIG4 · 行业</span> 行业热度 Top8</div>';
        h += '<div class="chart-container"><canvas id="industryChart"></canvas></div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">FIG5 · TMT</span> TMT合计监控</div>';
        h += '<div class="chart-container"><canvas id="tmtChart"></canvas></div>';
        h += '<div class="monitor-stats">';
        h += '<div class="stat-item"><span class="stat-value" id="tmtRatio">' + (data.tmt_ratio || 0).toFixed(2) + '%</span><span class="stat-label">TMT合计占比</span></div>';
        h += '</div>';
        h += '</div>';
        h += '</div>';

        h += '<div class="monitor-grid monitor-grid-row3" style="margin-bottom:16px">';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">FIG6 · 小盘</span> CSI1000小盘热度</div>';
        h += '<div class="chart-container"><canvas id="csi1000Chart"></canvas></div>';
        h += '<div class="monitor-stats">';
        h += '<div class="stat-item"><span class="stat-value" id="csi1000Heat">' + (data.csi1000.heat || '-') + '</span><span class="stat-label">小盘热度</span></div>';
        h += '<div class="stat-item"><span class="stat-value" id="csi1000Chg">' + (data.csi1000.change >= 0 ? '+' : '') + (data.csi1000.change || 0).toFixed(2) + '%</span><span class="stat-label">平均涨跌</span></div>';
        h += '</div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">FIG7 · 概念</span> 热门概念 Top8</div>';
        h += '<div class="chart-container"><canvas id="conceptChart"></canvas></div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">FIG8 · 资金</span> 观望资金趋势</div>';
        h += '<div class="chart-container"><canvas id="sidelineChart"></canvas></div>';
        h += '<div class="monitor-stats">';
        h += '<div class="stat-item"><span class="stat-value" id="sidelineRatio">' + (data.sideline_ratio || 0).toFixed(2) + '%</span><span class="stat-label">当前占比</span></div>';
        h += '</div>';
        h += '</div>';
        h += '</div>';

        h += '<div class="monitor-grid monitor-grid-row4">';
        h += '<div class="monitor-card table-card">';
        h += '<div class="card-title"><span class="card-tag">FIG9 · 明细</span> 热门概念详情</div>';
        h += '<table class="detail-table"><thead><tr><th>概念名称</th><th>热度值</th><th>涨跌幅</th></tr></thead>';
        h += '<tbody id="conceptTableBody">';
        if (data.concept_rows) {
            data.concept_rows.forEach(function(r) {
                var tagCls = r.change >= 0 ? 'tag-up' : 'tag-down';
                var tagText = r.change >= 0 ? '↑ 上涨' : '↓ 下跌';
                h += '<tr><td>' + r.name + '</td><td class="value">' + r.heat + '</td>';
                h += '<td><span class="tag ' + tagCls + '">' + tagText + ' ' + Math.abs(r.change).toFixed(2) + '%</span></td></tr>';
            });
        }
        h += '</tbody></table>';
        h += '</div>';
        h += '<div class="monitor-card">';
        h += '<div class="card-title"><span class="card-tag">FIG10 · 摘要</span> 市场数据摘要</div>';
        h += '<div class="indicator-card">';
        h += '<div class="indicator-row"><span class="indicator-label">上涨家数</span><span class="indicator-value positive">' + (data.up_count || 0) + '</span></div>';
        h += '<div class="indicator-row"><span class="indicator-label">下跌家数</span><span class="indicator-value negative">' + (data.down_count || 0) + '</span></div>';
        h += '<div class="indicator-row"><span class="indicator-label">观望资金占比</span><span class="indicator-value">' + (data.sideline_ratio || 0).toFixed(2) + '%</span></div>';
        h += '<div class="indicator-row"><span class="indicator-label">总成交额</span><span class="indicator-value">' + (data.total_amount ? (data.total_amount / 1e8).toFixed(0) + '亿' : '-') + '</span></div>';
        h += '</div>';
        h += '</div>';
        h += '</div>';

        container.innerHTML = h;

        setTimeout(function() {
            drawGauge(heatVal);
            var chartOpts = {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                plugins: {
                    legend: { labels: { color: '#666', font: { size: 11 }, boxWidth: 12, padding: 12 } }
                }
            };
            var xAxisOpts = { ticks: { color: '#888', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } };

            _monitorCharts['heatChart'] = createChart('heatChart', 'line', {
                labels: data.history.dates,
                datasets: [{
                    label: '热度',
                    data: data.history.heat,
                    borderColor: '#c62828',
                    backgroundColor: 'rgba(198,40,40,0.12)',
                    fill: true,
                    borderWidth: 2,
                    yAxisID: 'y',
                    tension: 0.4,
                    pointRadius: 0
                }, {
                    label: '成交额(亿)',
                    data: data.history.amount,
                    borderColor: '#1976d2',
                    backgroundColor: 'rgba(25,118,210,0.12)',
                    fill: true,
                    borderWidth: 2,
                    yAxisID: 'y1',
                    tension: 0.4,
                    pointRadius: 0
                }]
            }, Object.assign({}, chartOpts, {
                scales: {
                    y: { type: 'linear', position: 'left', ticks: { color: '#888', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } },
                    y1: { type: 'linear', position: 'right', ticks: { color: '#888', font: { size: 10 } }, grid: { drawOnChartArea: false } },
                    x: xAxisOpts
                }
            }));

            if (data.top_industries && data.top_industries.length > 0) {
                _monitorCharts['industryChart'] = createChart('industryChart', 'bar', {
                    labels: data.top_industries.map(function(r) { return r.name; }),
                    datasets: [{
                        label: '热度',
                        data: data.top_industries.map(function(r) { return r.heat; }),
                        backgroundColor: data.top_industries.map(function(r) { return r.change >= 0 ? '#d32f2f' : '#2e7d32'; }),
                        borderRadius: 4,
                        barThickness: 14
                    }]
                }, Object.assign({}, chartOpts, {
                    indexAxis: 'y',
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { ticks: { color: '#888', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } },
                        y: { ticks: { color: '#555', font: { size: 11 } }, grid: { display: false } }
                    }
                }));
            }

            if (data.history && data.history.tmt_ratio) {
                _monitorCharts['tmtChart'] = createChart('tmtChart', 'line', {
                    labels: data.history.dates,
                    datasets: [{
                        label: 'TMT合计占比',
                        data: data.history.tmt_ratio,
                        borderColor: '#d32f2f',
                        backgroundColor: 'rgba(211,47,47,0.12)',
                        fill: true,
                        borderWidth: 2,
                        tension: 0.4,
                        pointRadius: 0
                    }]
                }, Object.assign({}, chartOpts, {
                    scales: {
                        x: xAxisOpts,
                        y: { ticks: { color: '#888', font: { size: 10 }, callback: function(v) { return v + '%'; } }, grid: { color: 'rgba(0,0,0,0.04)' } }
                    }
                }));
            }

            if (data.csi1000 && data.history && data.history.csi1000_heat) {
                _monitorCharts['csi1000Chart'] = createChart('csi1000Chart', 'line', {
                    labels: data.history.dates,
                    datasets: [{
                        label: 'CSI1000热度',
                        data: data.history.csi1000_heat,
                        borderColor: '#1976d2',
                        backgroundColor: 'rgba(25,118,210,0.12)',
                        fill: true,
                        borderWidth: 2,
                        tension: 0.4,
                        pointRadius: 0
                    }]
                }, Object.assign({}, chartOpts, {
                    scales: {
                        x: xAxisOpts,
                        y: { ticks: { color: '#888', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } }
                    }
                }));
            }

            if (data.concept_rows && data.concept_rows.length > 0) {
                _monitorCharts['conceptChart'] = createChart('conceptChart', 'bar', {
                    labels: data.concept_rows.map(function(r) { return r.name; }),
                    datasets: [{
                        label: '热度',
                        data: data.concept_rows.map(function(r) { return r.heat; }),
                        backgroundColor: '#1e88e5',
                        borderRadius: 4,
                        barThickness: 14
                    }]
                }, Object.assign({}, chartOpts, {
                    indexAxis: 'y',
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { ticks: { color: '#888', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.04)' } },
                        y: { ticks: { color: '#555', font: { size: 11 } }, grid: { display: false } }
                    }
                }));
            }

            if (data.history && data.history.sideline) {
                _monitorCharts['sidelineChart'] = createChart('sidelineChart', 'line', {
                    labels: data.history.dates,
                    datasets: [{
                        label: '观望资金占比',
                        data: data.history.sideline,
                        borderColor: '#d4a017',
                        backgroundColor: 'rgba(212,160,23,0.12)',
                        fill: true,
                        borderWidth: 2,
                        tension: 0.4,
                        pointRadius: 0
                    }]
                }, Object.assign({}, chartOpts, {
                    scales: {
                        x: xAxisOpts,
                        y: { ticks: { color: '#888', font: { size: 10 }, callback: function(v) { return v + '%'; } }, grid: { color: 'rgba(0,0,0,0.04)' } }
                    }
                }));
            }
        }, 100);
    }

    function drawGauge(value) {
        var canvas = document.getElementById('monitorGauge');
        if (!canvas) return;
        var ctx = canvas.getContext('2d');
        var W = 260, H = 150;
        var cx = W / 2, cy = H - 20;
        var outerR = 100, innerR = 70;
        ctx.clearRect(0, 0, W, H);

        var segments = [
            { start: Math.PI, end: Math.PI + Math.PI * 0.2, color: '#2e7d32' },
            { start: Math.PI + Math.PI * 0.2, end: Math.PI + Math.PI * 0.4, color: '#66bb6a' },
            { start: Math.PI + Math.PI * 0.4, end: Math.PI + Math.PI * 0.6, color: '#bdbdbd' },
            { start: Math.PI + Math.PI * 0.6, end: Math.PI + Math.PI * 0.8, color: '#ef5350' },
            { start: Math.PI + Math.PI * 0.8, end: Math.PI + Math.PI * 1.0, color: '#c62828' }
        ];

        segments.forEach(function(seg) {
            ctx.beginPath();
            ctx.arc(cx, cy, outerR, seg.start, seg.end);
            ctx.arc(cx, cy, innerR, seg.end, seg.start, true);
            ctx.closePath();
            ctx.fillStyle = seg.color;
            ctx.fill();
        });

        var clampedVal = Math.max(0, Math.min(1000, value));
        var angle = Math.PI + (clampedVal / 1000) * Math.PI;
        var needleLen = outerR - 8;
        var nx = cx + Math.cos(angle) * needleLen;
        var ny = cy + Math.sin(angle) * needleLen;

        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(nx, ny);
        ctx.strokeStyle = '#222';
        ctx.lineWidth = 3;
        ctx.lineCap = 'round';
        ctx.stroke();

        ctx.beginPath();
        ctx.arc(cx, cy, 6, 0, Math.PI * 2);
        ctx.fillStyle = '#222';
        ctx.fill();

        var labels = [
            { text: '0', angle: Math.PI },
            { text: '200', angle: Math.PI + Math.PI * 0.2 },
            { text: '400', angle: Math.PI + Math.PI * 0.4 },
            { text: '600', angle: Math.PI + Math.PI * 0.6 },
            { text: '800', angle: Math.PI + Math.PI * 0.8 },
            { text: '1000', angle: Math.PI + Math.PI }
        ];
        ctx.fillStyle = '#999';
        ctx.font = '10px sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        labels.forEach(function(lb) {
            var lr = outerR + 14;
            ctx.fillText(lb.text, cx + Math.cos(lb.angle) * lr, cy + Math.sin(lb.angle) * lr);
        });
    }

    function createChart(canvasId, type, data, options) {
        var canvas = document.getElementById(canvasId);
        if (!canvas) return null;
        var ctx = canvas.getContext('2d');
        return new Chart(ctx, {
            type: type,
            data: data,
            options: options || {}
        });
    }

    function _restoreTab(savedTab) {
        // 直接操作 DOM 恢复 tab，不走 switchTab（避免 init 阶段副作用）
        if (savedTab && document.querySelector('[data-tab="' + savedTab + '"]')) {
            setActiveTab(savedTab);
            document.querySelectorAll('.sidebar-item').forEach(function (b) { b.classList.remove('active'); });
            var btn = document.querySelector('[data-tab="' + savedTab + '"]');
            if (btn) btn.classList.add('active');
            document.querySelectorAll('.tab-content').forEach(function (c) { c.classList.remove('active'); });
            var tc = el('tab-' + savedTab) || el(savedTab);
            if (!tc) {
                tc = document.createElement('div');
                tc.id = 'tab-' + savedTab;
                tc.className = 'tab-content';
                var area = el('contentArea') || document.body;
                area.appendChild(tc);
            }
            if (tc) tc.classList.add('active');
            el('pageTitle').textContent = (typeof PAGE_TITLES !== 'undefined' && PAGE_TITLES[savedTab]) || savedTab;
            loadTab(savedTab);
            return true;
        }
        return false;
    }

    function routeTabFromLocation() {
        var path = String(window.location.pathname || '').replace(/^\/+|\/+$/g, '');
        var routeMap = {
            'intraday-battle': 'intraday-battle',
            'battle': 'intraday-battle'
        };
        if (routeMap[path]) return routeMap[path];
        try {
            var tab = new URLSearchParams(window.location.search || '').get('tab') || '';
            if (tab && document.querySelector('[data-tab="' + tab + '"]')) return tab;
        } catch (e) {}
        return '';
    }

    function init() {
        var today = localDateString(new Date());
        el('datePicker').value = today;
        // 应用保存的布局
        var savedLayout = 'new';
        try { savedLayout = localStorage.getItem('probiga_layout') || 'new'; } catch (e) {}
        applyLayout(savedLayout);
        // 恢复保存的页面
        var savedTab = '';
        try { savedTab = localStorage.getItem('probiga_current_tab') || ''; } catch (e) {}
        var routeTab = routeTabFromLocation();
        loadMarketClock().then(function () {
            if (routeTab) {
                _restoreTab(routeTab);
            } else if (!_restoreTab(savedTab)) {
                // 没有保存的页面或页面不存在，加载默认页面
                loadTab('command');
            }
        });
    }
    try { init(); } catch (e) {
        console.error('[init error]', e);
        var mc = el('mainContent');
        if (mc) mc.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 初始化失败: ' + e.message + '</div>';
    }
})();
