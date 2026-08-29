(function () {
    var API_BASE = '/api/hot-data';

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
    var MARKET_CLOCK_REFRESHED_AT = 0;
    function setActiveTab(tabId) {
        var previousTab = ACTIVE_TAB;
        ACTIVE_TAB = String(tabId || '');
        window._activeTab = ACTIVE_TAB;
        if (previousTab === 'broad-etf-flow' && ACTIVE_TAB !== 'broad-etf-flow' && typeof window.stopBroadEtfFlow === 'function') {
            window.stopBroadEtfFlow();
        }
        if (document.body) document.body.classList.toggle('ai-workspace-active', ACTIVE_TAB === 'ai-stock' || ACTIVE_TAB === 'ai-general');
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
        // A fresh server trading clock knows holidays and special closures.
        // Local wall-clock time is also a safety stop when a page opened during
        // trading has stayed open across the midday or closing boundary.
        if (MARKET_CLOCK && typeof MARKET_CLOCK.is_intraday === 'boolean') {
            return MARKET_CLOCK.is_intraday && localOpen;
        }
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
    function fetchRawJsonWithTimeout(url, timeoutMs, requestOptions) {
        var controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
        var waitMs = timeoutMs || 5000;
        var fetchOptions = {};
        Object.keys(requestOptions || {}).forEach(function(key) { fetchOptions[key] = requestOptions[key]; });
        if (controller) fetchOptions.signal = controller.signal;
        return new Promise(function (resolve, reject) {
            var settled = false;
            var timer = setTimeout(function () {
                if (settled) return;
                settled = true;
                try {
                    if (controller) controller.abort();
                } catch (e) {}
                var timeoutError = new Error('Timed out after ' + waitMs + 'ms');
                timeoutError.isTimeout = true;
                timeoutError.httpStatus = 0;
                reject(timeoutError);
            }, waitMs);

            fetch(url, fetchOptions)
                .then(function (r) {
                    return r.text().then(function (text) {
                        var data = null;
                        if (text) {
                            try {
                                data = JSON.parse(text);
                            } catch (e) {
                                var preview = text.replace(/\s+/g, ' ').trim().slice(0, 200);
                                var parseError = new Error(preview || ('HTTP ' + r.status));
                                parseError.httpStatus = r.status;
                                parseError.invalidJson = true;
                                throw parseError;
                            }
                        }
                        if (!r.ok) {
                            var msg = (data && (data.message || data.error || data.detail)) || ('HTTP ' + r.status);
                            var httpError = new Error(msg);
                            httpError.httpStatus = r.status;
                            throw httpError;
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
            // Only hide transient loading placeholders during a silent refresh.
            // Final empty/error/timeout states must replace old data so users
            // never mistake a stale table for a successful refresh.
            return /加载中|正在加载|加载.*数据|加载.*页面|刷新中/.test(s);
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
    function refreshLoadTab(tabId, options) {
        options = options || {};
        if (tabId === 'portfolio' && options.force == null) options.force = false;
        options.silent = true;
        return loadTab(tabId, options);
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
            // The trading calendar can be newer than the local data pipeline.
            // Do not query an unavailable recommendation date and turn a
            // valid previous snapshot into a misleading empty page.
            var recommendationDate = MARKET_CLOCK.recommendation_trade_date;
            var latestDataDate = MARKET_CLOCK.latest_data_date || '';
            if (latestDataDate && recommendationDate > latestDataDate) recommendationDate = latestDataDate;
            if (!MARKET_CLOCK.ui_trade_date || pickerValue === MARKET_CLOCK.ui_trade_date) {
                return recommendationDate;
            }
        }
        return pickerValue;
    }
    function latestFormalStrategyDateValue() {
        var clock = MARKET_CLOCK || {};
        var recommendationDate = String(clock.recommendation_trade_date || '').slice(0, 10);
        var latestDataDate = String(clock.latest_data_date || '').slice(0, 10);
        if (recommendationDate && latestDataDate && recommendationDate > latestDataDate) recommendationDate = latestDataDate;
        return recommendationDate || latestDataDate || '';
    }
    function applyMarketClock(clock) {
        MARKET_CLOCK = clock || null;
        MARKET_CLOCK_REFRESHED_AT = Date.now();
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
    function refreshMarketClockSilently(minAgeMs) {
        var maxAge = Number(minAgeMs || 60000);
        if (window._marketClockRefreshInFlight) return window._marketClockRefreshInFlight;
        if (MARKET_CLOCK_REFRESHED_AT && Date.now() - MARKET_CLOCK_REFRESHED_AT < maxAge) {
            return Promise.resolve(MARKET_CLOCK);
        }
        var wasTrading = isTradingTime();
        window._marketClockRefreshInFlight = fetchJsonWithTimeout('/market-clock', 3000)
            .then(function(clock) {
                MARKET_CLOCK = clock || null;
                MARKET_CLOCK_REFRESHED_AT = Date.now();
                window._marketClock = MARKET_CLOCK;
                if (wasTrading !== isTradingTime() && typeof window._pfStartAutoRefresh === 'function') {
                    window._pfStartAutoRefresh();
                }
                return MARKET_CLOCK;
            })
            .catch(function() { return MARKET_CLOCK; })
            .finally(function() { window._marketClockRefreshInFlight = null; });
        return window._marketClockRefreshInFlight;
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
    function isTradingDecisionTab(tabId) {
        return tabId === 'trading' || String(tabId || '').indexOf('trading-v3-') === 0 || String(tabId || '').indexOf('trading-shared-') === 0;
    }
    function tradingRouteFilters() {
        var result = {};
        try {
            var params = new URLSearchParams(window.location.search || '');
            ['trade_date','strategy','status','q','kind','hypothesis_state','hypothesis_q'].forEach(function(key) { var value = params.get(key); if (value != null && value !== '') result[key] = value; });
        } catch (e) {}
        return result;
    }
    function syncTabRoute(tabId, mode) {
        if (!window.history || !window.URL) return;
        try {
            var url = new URL(window.location.href);
            url.searchParams.set('tab', tabId);
            if (isTradingDecisionTab(tabId)) {
                var picker = el('datePicker');
                if (picker && picker.value) url.searchParams.set('trade_date', picker.value);
            }
            var next = url.pathname + url.search + url.hash;
            var current = window.location.pathname + window.location.search + window.location.hash;
            if (next === current) return;
            window.history[mode === 'replace' ? 'replaceState' : 'pushState']({ tab:tabId }, '', next);
        } catch (e) { console.warn('[route sync]', e); }
    }
    window.updateTradingRouteFilters = function(filters) {
        try {
            var url = new URL(window.location.href), values = filters || {};
            ['trade_date','strategy','status','q','kind','hypothesis_state','hypothesis_q'].forEach(function(key) {
                if (!Object.prototype.hasOwnProperty.call(values, key)) return;
                var value = values[key];
                if (value == null || value === '') url.searchParams.delete(key); else url.searchParams.set(key, String(value));
            });
            window.history.replaceState({ tab:activeTabId() }, '', url.pathname + url.search + url.hash);
        } catch (e) { console.warn('[trading filter route]', e); }
    };
    window.onDecisionDateChange = function() {
        if (isTradingDecisionTab(activeTabId())) window.updateTradingRouteFilters({ trade_date:currentDateValue() });
        return refreshAll();
    };
    window.switchTab = function (tabId, options) {
        options = options || {};
        try {
            setActiveTab(tabId);
            if (!options.fromHistory && !options.skipHistory) syncTabRoute(tabId, options.replaceHistory ? 'replace' : 'push');
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
            expandSidebarGroupForItem(tabId);
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
        // Auto-refresh replaces the table markup. Keep the user's local view
        // state so a background refresh does not erase an active search.
        var previousWrap = el('tw_' + tableId);
        var previousSearchInput = el('s_' + tableId);
        var previousSearch = previousSearchInput ? previousSearchInput.value : '';
        var previousPage = previousWrap ? Number(window['_p_' + tableId] || 1) : 1;
        var previousScrollTop = previousWrap ? previousWrap.scrollTop : 0;
        var previousScrollLeft = previousWrap ? previousWrap.scrollLeft : 0;
        var restoreSearchFocus = previousSearchInput && document.activeElement === previousSearchInput;
        var previousSelectionStart = restoreSearchFocus && previousSearchInput.selectionStart != null
            ? previousSearchInput.selectionStart : null;
        var previousSelectionEnd = restoreSearchFocus && previousSearchInput.selectionEnd != null
            ? previousSearchInput.selectionEnd : null;
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
        var nextSearchInput = el('s_' + tableId);
        if (nextSearchInput) nextSearchInput.value = previousSearch;
        doSearch(tableId);

        // doSearch starts at page 1; restore the previous page when it still
        // exists after the refreshed dataset has been filtered.
        var filteredRows = window['_ft_' + tableId] || [];
        var totalPages = Math.ceil(filteredRows.length / pageSize);
        if (totalPages > 0) {
            window['_p_' + tableId] = Math.min(Math.max(previousPage, 1), totalPages);
            if (window['_p_' + tableId] !== 1) renderPage(tableId);
        }

        var nextWrap = el('tw_' + tableId);
        if (nextWrap) {
            nextWrap.scrollTop = previousScrollTop;
            nextWrap.scrollLeft = previousScrollLeft;
        }
        if (restoreSearchFocus && nextSearchInput) {
            nextSearchInput.focus();
            if (previousSelectionStart != null && nextSearchInput.setSelectionRange) {
                var maxSelection = nextSearchInput.value.length;
                nextSearchInput.setSelectionRange(
                    Math.min(previousSelectionStart, maxSelection),
                    Math.min(previousSelectionEnd == null ? previousSelectionStart : previousSelectionEnd, maxSelection)
                );
            }
        }
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
    window.genReviewBtn = function(d) {
        fetch('/api/hot-data/daily-review/generate?review_date=' + encodeURIComponent(d), {method:'POST'})
            .then(function(r){return r.json();})
            .then(function(res){
                if (!res.accepted) {
                    alert('生成失败：' + (res.output || res.status || '未知错误'));
                    return;
                }
                alert('复盘生成任务已提交后台执行' + (res.job_id ? '（任务号：' + res.job_id + '）' : ''));
            })
            .catch(function(err){ alert('生成失败：' + (err.message || err)); });
    };
    window.exportReview = function(d) {
        fetch('/api/hot-data/daily-review/export?review_date=' + encodeURIComponent(d))
            .then(function(r) { return r.json(); })
            .then(function(res) {
                if (res.error || !res.text) {
                    var quality = jsonF(res.quality, {});
                    var errors = Array.isArray(quality.errors) ? quality.errors : [];
                    var detail = errors.length ? '\n- ' + errors.join('\n- ') : '';
                    alert((res.error || '暂无可导出的复盘') + detail);
                    return;
                }
                var blob = new Blob(['\ufeff' + String(res.text)], {type:'text/markdown;charset=utf-8'});
                var url = URL.createObjectURL(blob);
                var a = document.createElement('a');
                a.href = url;
                a.download = 'review_' + (res.date || d) + '.md';
                document.body.appendChild(a);
                a.click();
                a.remove();
                setTimeout(function() { URL.revokeObjectURL(url); }, 0);
            })
            .catch(function(err) { alert('导出失败：' + (err.message || err)); });
    };
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
    window.loadPortfolio = function(){
        // "刷新" reconciles the complete watchlist.  Remote quote collection
        // remains the separate "同步行情" action.
        return refreshLoadTab('portfolio', { force: true });
    };

    /* ===== 自选股 ===== */
    function pfReloadAfterMutation() {
        // Every mutation invalidates the server snapshot cache, so forcing a
        // second uncached rebuild only adds avoidable database load.
        return refreshLoadTab('portfolio', { force: false });
    }
    window.pfAdd = function() {
        var code = el('pfCode').value.trim();
        var price = parseFloat(el('pfPrice').value);
        var shares = parseInt(el('pfShares').value);
        var isTodayBuy = !!(el('pfTodayBuy') && el('pfTodayBuy').checked);
        var positionDate = el('pfPositionDate') ? el('pfPositionDate').value : '';
        if (!code) { alert('请输入股票代码'); return; }
        return fetchRawJsonWithTimeout('/api/portfolio/add', 10000, {
            method:'POST',
            cache:'no-store',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({
                stock_code:code,
                cost_price:price||0,
                shares:shares||0,
                is_today_buy:isTodayBuy,
                position_date:positionDate||null,
                watchlist_only:false
            })
        }).then(function(res){
            if (res.status !== 'ok') throw new Error(res.error || '未知错误');
            alert('添加成功: ' + (res.short_name || code));
            return pfReloadAfterMutation();
        }).catch(function(e){ alert('添加失败: ' + (e.message || '网络请求异常')); });
    };
    window.pfAddWithCode = function(code) {
        if (!code) { alert('股票代码为空'); return; }
        if (!confirm('确认将 ' + code + ' 加入自选股？')) return;
        return fetchRawJsonWithTimeout('/api/portfolio/add', 10000, {
            method:'POST',
            cache:'no-store',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({stock_code:code, cost_price:0, shares:0, is_today_buy:false, watchlist_only:true})
        }).then(function(res){
            if (res.status !== 'ok') throw new Error(res.error || '未知错误');
            alert('添加成功: ' + (res.short_name || code));
            return pfReloadAfterMutation();
        }).catch(function(e){ alert('添加失败: ' + (e.message || '网络请求异常')); });
    };
    window.pfRemove = function(code) {
        if (!confirm('确认删除?')) return;
        return fetchRawJsonWithTimeout('/api/portfolio/remove/' + encodeURIComponent(code), 10000, {
            method:'DELETE',
            cache:'no-store'
        }).then(function(res){
            if (res.status !== 'ok') throw new Error(res.error || '未知错误');
            return pfReloadAfterMutation();
        }).catch(function(e){ alert('删除失败: ' + (e.message || '网络请求异常')); });
    };
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
        return fetchRawJsonWithTimeout('/api/portfolio/transact/' + encodeURIComponent(code), 10000, {
            method:'POST',
            cache:'no-store',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({stock_code:code, trans_type:transType, price:price, shares:shares})
        }).then(function(res){
            if (res.status !== 'ok') throw new Error(res.error || res.status || '未知错误');
            closeTradeModal();
            return pfReloadAfterMutation();
        }).catch(function(e){ alert('交易失败: ' + (e.message || '网络请求异常')); });
    };
    window.closeTradeModal = function() { document.getElementById('tradeModal').classList.remove('show'); };
    window.refreshPfPrices = function() {
        window._pfManualRefreshToken = (Number(window._pfManualRefreshToken) || 0) + 1;
        var status = document.getElementById('pfLiveStatus');
        var btn = document.querySelector('button[onclick="refreshPfPrices()"]');
        var oldText = btn ? btn.textContent : '';
        var shouldForceLive = isTradingTime();
        if (status) status.textContent = shouldForceLive ? '同步实时行情...' : '同步收盘数据...';
        if (btn) {
            btn.disabled = true;
            btn.textContent = '同步中';
        }
        var refreshPromise = fetchRawJsonWithTimeout(
            '/api/portfolio/refresh-prices',
            10000,
            {method:'POST', cache:'no-store'}
        );
        return refreshPromise.then(function(res){
            if (!res.accepted) throw new Error(res.error || res.message || res.status || '任务提交失败');
            var jobId = res.job_id || '';
            if (status) status.textContent = '行情刷新任务已提交后台执行' + (jobId ? '（' + jobId + '）' : '');
            var fetchLive = typeof window.pfFetchPortfolioWithRetry === 'function'
                ? window.pfFetchPortfolioWithRetry(true)
                : fetchRawJsonWithTimeout(
                    '/api/portfolio/live?force=true&refresh_id=' +
                        encodeURIComponent('pf-submit-' + Date.now().toString(36)) +
                        '&_=' + Date.now(),
                    12000,
                    {cache:'no-store'}
                );
            return fetchLive.then(function(liveRes){
                if (liveRes && liveRes.error) throw new Error(liveRes.error);
                var prefix = '刷新任务已提交';
                if (window.pfRenderPortfolio) {
                    window.pfRenderPortfolio(liveRes, prefix);
                    return;
                }
                return refreshLoadTab('portfolio', { force: false });
            });
        }).catch(function(e){
            if (e && e.cancelled) return;
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
        return fetchRawJsonWithTimeout('/api/portfolio/reorder', 10000, {
            method:'POST',
            cache:'no-store',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({codes:codes})
        }).then(function(res){
            if (res.status === 'ok') {
                var btn = document.querySelector('button[onclick="savePfOrder()"]');
                if (btn) { var orig = btn.textContent; btn.textContent = '✅ 已保存'; setTimeout(function(){ btn.textContent = orig; }, 1500); }
            } else throw new Error(res.error || '未知错误');
        }).catch(function(e){ alert('保存失败: ' + (e.message || '网络请求异常')); });
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
        // 自选股和 AI 推荐详情统一使用同一套数据、分析快照和详情视图。
        // 这样自选股不会再退回到只显示一段旧格式文本的轻量弹窗。
        if (typeof window.openStockDetail === 'function') {
            window.openStockDetail(code);
            return;
        }

        // 兼容极早期页面脚本尚未挂载详情函数的情况。
        var titleEl = document.getElementById('analyzeModalTitle');
        if (titleEl) titleEl.textContent = '🤖 AI 分析 | ' + (name || code);
        var bodyEl = document.getElementById('analyzeModalBody');
        if (bodyEl) bodyEl.innerHTML = '<div style="text-align:center;padding:30px;color:#888"><span class="spinner"></span> 分析中...</div>';
        var overlay = document.getElementById('analyzeModal');
        if (overlay) overlay.classList.add('show');
        fetch('/api/portfolio/analyze/'+code).then(function(r){return r.json()}).then(function(res){
            var b = document.getElementById('analyzeModalBody');
            if (b) b.innerHTML = '<div class="pf-analysis-legacy">' +
                '<div class="pf-analysis-error">' + escHtml(res.analysis || res.error || '分析失败') + '</div>' +
                '<div class="pf-analysis-footer"><button onclick="closeAnalyzeModal()">关闭</button></div></div>';
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
    function loadFusedTab(d, c, liveRefresh) {
        var freshQuery = liveRefresh ? '&fresh=1&_ts=' + Date.now() : '';
        return apiGet('/fused-live?top=100' + freshQuery).then(function (res) {
            if (!res.data || !res.data.length) {
                return apiGet('/fused?snapshot_date=' + d + '&top=100').then(function (fallback) {
                    syncDateFromResponse(fallback);
                    if (!fallback.data || !fallback.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
                    renderFusedData(c, fallback);
                });
            }
            renderFusedData(c, res);
        }).catch(function () {
            return apiGet('/fused?snapshot_date=' + d + '&top=100').then(function (res) {
                syncDateFromResponse(res);
                if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
                renderFusedData(c, res);
            });
        });
    }
    function loadThsTab(d, c) {
        return apiGet('/rank-ths?snapshot_date=' + d + '&top=100').then(function (res) {
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
        return apiGet('/pop-rank-east?snapshot_date=' + d + '&top=100').then(function (res) {
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
        return apiGet('/rank-xq?snapshot_date=' + d + '&top=100').then(function (res) {
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
        return apiGet('/rank-sina?top=100').then(function (res) {
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
                return hasExplicitNewBuyGate(item);
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
                if (!hasExplicitNewBuyGate(item) && (status === 'BUY_READY' || status === 'CONFIRM')) {
                    return { label: '执行门未齐', cls: 'risk' };
                }
                var map = {
                    BUY_READY: { label: '四门就绪', cls: 'buy' },
                    CONFIRM: { label: '四门确认', cls: 'buy' },
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
            function systemItemName(item) {
                return item.label || item.name || item.task_name || item.biz_type || item.task_type || item.id || '任务';
            }
            function systemNameSummary(rows, limit) {
                var seen = {};
                var names = (rows || []).map(systemItemName).filter(function (name) {
                    name = String(name || '').trim();
                    if (!name || seen[name]) return false;
                    seen[name] = true;
                    return true;
                });
                limit = Math.max(1, Number(limit || 8));
                var visible = names.slice(0, limit);
                return {
                    note: visible.join('、') + (names.length > visible.length ? '，另 ' + (names.length - visible.length) + ' 项' : ''),
                    full: names.join('、')
                };
            }
            function systemHealthItem(label, value, note, tone, action, fullNote) {
                tone = tone || 'muted';
                var noteText = note || '-';
                var noteTitle = fullNote && fullNote !== noteText ? ' title="' + escAttr(fullNote) + '"' : '';
                var actionHtml = action ? '<div class="command-system-action">' + escHtml(action) + '</div>' : '';
                return '<div class="command-system-item ' + tone + '">' +
                    '<div class="command-system-top"><span>' + escHtml(label) + '</span><strong>' + escHtml(value || '-') + '</strong></div>' +
                    '<div class="command-system-note"' + noteTitle + '>' + escHtml(noteText) + '</div>' +
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
                var names = systemNameSummary(badRows, 8);
                return { label: '关键数据任务', value: badRows.length + ' 项异常', note: names.note || '存在异常任务', fullNote: names.full, tone: 'bad', action: '先补关键数据，再看推荐排序' };
            }
            function summarizeSchedulerHealth() {
                var runtime = schedulerRes.runtime || {};
                var tasks = schedulerRes.data || [];
                var online = Boolean(runtime.standalone_scheduler_online || runtime.embedded_scheduler_running);
                var enabledTasks = tasks.filter(function (item) { return Number((item || {}).enabled) === 1; });
                var failed = enabledTasks.filter(function (item) { return String((item || {}).last_run_status || '').toLowerCase() === 'failed'; });
                var running = tasks.filter(function (item) { return String((item || {}).last_run_status || '').toLowerCase() === 'running'; });
                if (!schedulerRes || (!tasks.length && !Object.keys(runtime).length)) {
                    return { label: '调度运行', value: '未返回', note: '调度接口暂无结果', tone: 'muted', action: '打开调度管理查看任务' };
                }
                if (failed.length) {
                    var failedNames = systemNameSummary(failed, 8);
                    return { label: '调度运行', value: failed.length + ' 个失败', note: failedNames.note || '存在失败任务', fullNote: failedNames.full, tone: 'bad', action: '优先重跑失败任务' };
                }
                if (online) {
                    return { label: '调度运行', value: runtime.standalone_scheduler_online ? '独立在线' : '内嵌在线', note: '运行中 ' + running.length + ' 个 / 启用 ' + enabledTasks.length + ' 个', tone: 'good', action: '' };
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
                    items.map(function (item) { return systemHealthItem(item.label, item.value, item.note, item.tone, item.action, item.fullNote); }).join('') +
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
            h += '<div class="command-kpi"><div class="command-kpi-label">研究候选</div><div class="command-kpi-value">' + picks.length + '</div><div class="command-kpi-note">四门确认 ' + buyReadyCount + ' 只</div></div>';
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
                var formalPoolCurrent = r.formal_pool_current === true;
                var probability = extractProbability(r);
                var upside = probability.upside_probability_pct;
                var downside = probability.downside_probability_pct;
                var entry = formalPoolCurrent && numberValue(r.entry_price_low, 0) > 0 && numberValue(r.entry_price_high, 0) > 0
                    ? fmtPrice(r.entry_price_low) + '-' + fmtPrice(r.entry_price_high)
                    : '研究只读';
                var sourceAction = String(r.signal_status || r.recommend_status || '').toUpperCase();
                var action = formalPoolCurrent && hasExplicitNewBuyGate(r) ? sourceAction : 'RESEARCH_ONLY';
                var tone = formalPoolCurrent && hasExplicitNewBuyGate(r) ? 'buy' : 'watch';
                return '<div class="battle-stock-row">' +
                    '<div class="battle-stock-main"><div class="battle-row-title">' + stockInline(r.stock_code, r.short_name) + ' <span class="battle-code">' + escHtml(r.stock_code || '') + '</span></div>' +
                    '<div class="battle-row-sub">入场 ' + escHtml(entry) + ' · 止损 ' + escHtml(formalPoolCurrent ? fmtPrice(r.stop_loss_price) : '研究只读') + ' · 盈亏比 ' + (formalPoolCurrent ? fmt(numberValue(r.risk_reward_ratio, 0), 2) : '—') + '</div>' +
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
                var formalPoolCurrent = r.formal_pool_current === true;
                var paperExecutionReady = r.paper_execution_ready === true && String(r.execution_authority || '').toUpperCase() === 'V2_GATED';
                var action = paperExecutionReady ? String(r.action || '').toUpperCase() : 'WAIT';
                var tone = action === 'BUY_READY' ? 'buy' : (action === 'SELL_ALERT' ? 'risk' : 'watch');
                var entry = paperExecutionReady && numberValue(r.entry_price_low, 0) > 0 && numberValue(r.entry_price_high, 0) > 0
                    ? fmtPrice(r.entry_price_low) + '-' + fmtPrice(r.entry_price_high)
                    : '咨询只读';
                return '<div class="battle-stock-row">' +
                    '<div class="battle-stock-main"><div class="battle-row-title">' + stockInline(r.stock_code, r.short_name) + ' <span class="battle-code">' + escHtml(r.stock_code || '') + '</span></div>' +
                    '<div class="battle-row-sub">入场 ' + escHtml(entry) + ' · 止损 ' + escHtml(paperExecutionReady ? fmtPrice(r.stop_loss_price || r.trend_stop_price) : '咨询只读') + ' · ' + escHtml(r.preferred_strategy_name || localizeMachineText(r.primary_strategy) || '-') + '</div>' +
                    '<div class="battle-row-sub">' + escHtml(shortText(localizeMachineText(r.action_reason), 100)) + '</div></div>' +
                    '<div class="battle-stock-side"><div class="battle-score">' + fmt(rowScore(r), 0) + '</div>' +
                    '<div>' + actionPill(paperExecutionReady ? (r.action_label || action) : (formalPoolCurrent ? 'ADVISORY_ONLY' : 'RESEARCH_ONLY'), tone) + '</div></div>' +
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
            var formalPoolCurrent = candidates.formal_current === true;
            var qmt = data.qmt || {};
            var recRows = (rec.data || []).slice().sort(function (a, b) { return rowScore(b) - rowScore(a); });
            var candidateRows = candidates.data || [];
            var buyCandidates = formalPoolCurrent ? candidateRows.filter(function (r) { return r.paper_execution_ready === true && r.action === 'BUY_READY'; }) : [];
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
            if (!formalPoolCurrent) {
                actionTitle = '策略池研究只读';
                actionTone = 'risk';
                actionText = candidates.blocking_reason || '请求日没有通过身份、日期与完整性校验的 VERIFIED COMPLETED 策略池，禁止把旧候选升级为当前动作。';
            } else if (!buyCandidates.length) {
                actionTitle = '当前票池已更新';
                actionText = '已读取最新选股结果；V3 只有咨询权限，未绑定同批次 V2 模拟执行账本前不生成买入动作。';
            } else if (!clock.is_intraday) {
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
            var topRecConfirm = recRows.slice(0, 8).map(function (row) {
                return Object.assign({}, row, { formal_pool_current:false });
            });

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
            html += metric('当前选股', String(formalPoolCurrent ? candidateRows.length : 0), formalPoolCurrent ? '执行就绪 ' + buyCandidates.length + ' / 咨询只读 ' + (candidateRows.length - buyCandidates.length) : '历史研究 ' + candidateRows.length + ' / 正式池 0', buyCandidates.length ? 'buy' : formalPoolCurrent ? 'watch' : 'risk');
            html += metric('持仓风险', String(holdingRisks.length), '持仓 ' + holdingRows.length + ' 只', holdingRisks.length ? 'risk' : 'buy');
            html += metric('国金QMT状态', qmt.status || 'unknown', ((qmt.stock_current || {}).latest_snapshot_at || '') + '', qmtOk ? 'buy' : 'risk');
            html += '</section>';

            html += '<div class="sc-freshness ' + (formalPoolCurrent ? 'ok' : 'error') + '"><strong>' + (formalPoolCurrent ? 'VERIFIED COMPLETED / 当前策略选股结果（咨询只读）' : 'RESEARCH_ONLY / 正式策略池不可执行') + '</strong><span>请求日 ' + escHtml(candidates.requested_date || '-') + ' · 决策日 ' + escHtml(candidates.decision_date || '-') + ' · 数据日 ' + escHtml(candidates.data_date || '-') + ' · run_uid ' + escHtml(String(candidates.run_uid || '-').slice(0, 12)) + (formalPoolCurrent ? '；选股身份、日期与完整性已通过，V3 仍为 ADVISORY_ONLY，模拟和真实下单权限都不由此接口授予。' : '；' + escHtml(candidates.blocking_reason || '旧日期、未验证或研究只读候选已隔离。')) + '</span></div>';

            html += '<section class="battle-grid">';
            html += '<div class="battle-panel battle-panel-wide"><div class="battle-panel-title">盘中执行检查</div>';
            html += '<div class="battle-check-grid">';
            html += '<div><strong>市场：</strong>热度 ' + escHtml(fmt(marketHeat, 0)) + '，红盘率 ' + escHtml(fmt(redRatio, 1)) + '%，' + escHtml(monitor.is_realtime ? '使用实时快照' : '使用日线/缓存') + '</div>';
            html += '<div><strong>板块：</strong>拉升行业 ' + risingSectors.length + ' 个，跳水行业 ' + fallingSectors.length + ' 个，概念拉升 ' + conceptSurge.length + ' 个</div>';
            html += '<div><strong>个股：</strong>当前策略选股 ' + (formalPoolCurrent ? candidateRows.length : 0) + ' 只，执行账本就绪 ' + buyCandidates.length + ' 只，旧推荐研究观察 ' + topRecConfirm.length + ' 只</div>';
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
            html += '<div class="battle-panel battle-panel-wide"><div class="battle-panel-title">历史/旧推荐研究数据（不可执行）</div>';
            html += renderPickRows(topRecConfirm, '暂无旧推荐研究记录');
            html += '</div>';
            html += '<div class="battle-panel"><div class="battle-panel-title">当前策略选股结果（咨询只读）</div>';
            html += renderCandidateRows(buyCandidates.concat(sellCandidates).concat(waitCandidates).slice(0, 8), '暂无模拟动作');
            html += '<div class="battle-mini-meta"><a href="/?tab=trading">查看策略入选、证据与独立执行账本</a></div>';
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
                        fetchRawJsonWithTimeout('/api/v3/stock-pool?trade_date=' + encodeURIComponent(recDate), 12000).catch(function () { return {}; }),
                        fetchRawJsonWithTimeout('/api/health/qmt-bridge', 5000).catch(function () { return {}; })
                    ]).then(function (results) {
                        var strategyEnvelope = results[5] || {};
                        var strategyPool = strategyEnvelope.data || strategyEnvelope || {};
                        var poolTruth = candidateCenterStockPoolTruth(strategyPool, recDate, recDate);
                        var v3Candidates = (Array.isArray(strategyPool.items) ? strategyPool.items : []).filter(function (row) {
                            return row && row.is_strategy_candidate === true;
                        }).map(function (row) {
                            var plan = row.action_plan || {};
                            var actionability = String(row.actionability || plan.actionability || 'RESEARCH_ONLY').toUpperCase();
                            var executionAuthority = String(plan.execution_authority || 'ADVISORY_ONLY').toUpperCase();
                            var action = 'WAIT';
                            var blocking = row.reasons || [];
                            if (!poolTruth.ready) blocking = [poolTruth.reason].concat(blocking).filter(Boolean);
                            return {
                                stock_code: row.stock_code,
                                short_name: row.stock_name || row.short_name,
                                action: action,
                                action_label: poolTruth.ready ? 'ADVISORY_ONLY' : 'RESEARCH_ONLY',
                                action_reason: blocking.length ? blocking.join('；') : (plan.label || actionability),
                                entry_price_low: null,
                                entry_price_high: null,
                                stop_loss_price: null,
                                primary_strategy: (row.strategy_keys || [])[0] || '',
                                preferred_strategy_name: (row.strategy_keys || []).join(' / '),
                                ai_score: row.raw_score,
                                final_trade_score: row.raw_score,
                                formal_pool_current: poolTruth.ready,
                                execution_authority: executionAuthority,
                                paper_execution_ready: false
                            };
                        });
                        return {
                            clock: clock || {},
                            monitor: results[0] || {},
                            rotation: results[1] || {},
                            movement: results[2] || {},
                            rec: results[3] || {},
                            portfolio: results[4] || {},
                            candidates: {
                                data: v3Candidates,
                                formal_current: poolTruth.ready,
                                blocking_reason: poolTruth.reason,
                                requested_date: poolTruth.requestedDate || recDate,
                                decision_date: poolTruth.decisionDate,
                                data_date: poolTruth.dataDate,
                                run_uid: strategyPool.run_uid || '',
                                date: strategyPool.trade_date || recDate
                            },
                            qmt: results[6] || {},
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
                var catalysts = (theme.trigger_labels || []).map(function (label) {
                    return '<span class="radar-hit">' + escHtml(label) + '</span>';
                }).join('');
                var newsHits = (theme.news_hits || []).slice(0, 2).map(function (item) {
                    return '<div class="radar-note"><b>' + (Number(item.direction || 0) < 0 ? '风险：' : '催化：') + '</b>' + escHtml(item.title || '-') + '</div>';
                }).join('');
                h += '<section class="radar-theme-card">';
                h += '<div class="radar-theme-top"><div><h3>#' + escHtml(theme.rank || '-') + ' ' + escHtml(theme.name) + '</h3><span>' + escHtml(theme.rank_tier || '观察') + ' · ' + escHtml(theme.status || '-') + ' · ' + escHtml(theme.trend || '-') + '</span></div><strong>' + (theme.score || '-') + '</strong></div>';
                h += '<p>' + escHtml(theme.logic || '') + '</p>';
                h += '<div class="radar-chip-row">' + stocks + '</div>';
                if (catalysts) h += '<div class="radar-mini-title">当前催化</div><div class="radar-hit-row">' + catalysts + '</div>';
                h += newsHits;
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

    function hasExplicitNewBuyGate(row) {
        row = row || {};
        var recommend = String(row.recommend_status || 'DATA_BLOCKED').toUpperCase();
        var signal = String(row.signal_status || 'WATCH').toUpperCase();
        var chase = String(row.chase_risk_status || 'DATA_BLOCKED').toUpperCase();
        var ordinary = row.ordinary_buy_eligible === true || row.ordinary_buy_eligible === 1;
        return recommend === 'ALLOW' &&
            (signal === 'CONFIRM' || signal === 'BUY_READY') &&
            chase === 'ALLOW' && ordinary;
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
            var buyReady = rows.filter(hasExplicitNewBuyGate).length;
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
                if (hasExplicitNewBuyGate(r)) return { text: s === 'CONFIRM' ? '四门确认' : '四门就绪', cls: 'buy' };
                if (s.indexOf('SELL') >= 0 || s === 'BLOCK') return { text: '风险', cls: 'risk' };
                if (String(r.chase_risk_status || '').toUpperCase().indexOf('BLOCK') >= 0) return { text: '追高阻断', cls: 'risk' };
                return { text: '观察/数据待确认', cls: 'watch' };
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
            h += '<div><span>四门确认</span><strong>' + buyReady + '</strong><em>推荐 / 信号 / 追高 / 可成交均通过</em></div>';
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

    /* ===== 策略中心 ===== */
    function strategyCenterLabel(value) {
        var map = {
            trend_bullish: '趋势偏多',
            high_range: '高位震荡',
            risk_declining: '风险下降',
            extreme_event: '极端事件',
            unknown: '数据不足'
        };
        return map[value] || value || '数据不足';
    }

    function strategyCenterStatus(value) {
        var map = {
            READY: '确认前候选', WATCH: '观察', CONFLICT: '信号冲突', BLOCKED: '已阻断',
            SELL_ALERT: '风险提醒', INSUFFICIENT_DATA: '数据不足', BUY: '偏多', SELL: '偏空', HOLD: '中性'
        };
        return map[value] || value || '-';
    }

    function strategyCenterTone(value) {
        value = String(value || '').toUpperCase();
        if (value === 'READY' || value === 'BUY') return 'buy';
        if (value === 'BLOCKED' || value === 'SELL_ALERT' || value === 'SELL') return 'risk';
        return 'watch';
    }

    function strategyCenterMoney(value) {
        return value == null || value === '' ? '暂无样本' : pct(value);
    }

    function strategyCenterJson(value) {
        if (!value || (Array.isArray(value) && !value.length)) return '-';
        try { return JSON.stringify(value, null, 2); } catch (e) { return String(value); }
    }

    function strategyGovernanceMetric(value, suffix, digits) {
        if (value == null || value === '' || isNaN(Number(value))) return '样本不足';
        return Number(value).toFixed(digits == null ? 2 : digits) + (suffix || '');
    }

    function strategyCompetitionScoreSummary(row) {
        row = row || {};
        var executionReady = row.execution_evidence_comparable === true;
        var signalReady = row.signal_validation_comparable === true;
        var execution = executionReady
            ? '成交实证 #' + Number(row.execution_evidence_rank || 0) + ' · ' + strategyGovernanceMetric(row.execution_evidence_score, '', 1)
            : '成交实证 · 待满足可比样本';
        var signal = signalReady
            ? '独立信号 #' + Number(row.signal_validation_rank || 0) + ' · ' + strategyGovernanceMetric(row.signal_validation_score, '', 1)
            : '独立信号 · 待复核样本外证据';
        return '<strong>' + escHtml(execution) + '</strong><small>' + escHtml(signal) + '</small><small>信号榜仅研究，不授予模拟资金</small>';
    }

    function strategyGovernanceWindowSummary(row) {
        row = row || {};
        var metrics = row.metrics || {};
        var gates = row.multi_window_gate || {};
        return [20, 60, 120].map(function (windowDays) {
            var metric = metrics[String(windowDays)] || {};
            var gate = metric.profit_gate || gates[String(windowDays)] || {};
            var coverage = metric.portfolio_coverage_days;
            if (coverage == null) coverage = metric.coverage_days;
            var passed = gate.passed === true;
            return '<div class="sc-window-row" title="' + escAttr(gate.reason || '窗口证据尚未通过') + '"><b>' + windowDays + '日</b><span>' + strategyGovernanceMetric(metric.completed_trades, '', 0) + '笔 / ' + strategyGovernanceMetric(coverage, '', 0) + '日</span><span>净 ' + strategyGovernanceMetric(metric.net_expectancy_pct, '%', 3) + ' · PF ' + strategyGovernanceMetric(metric.profit_factor, '', 2) + '</span><em class="' + (passed ? 'pass' : 'fail') + '">' + (passed ? '通过' : '未通过') + '</em>' + strategyStatisticalProofSummary(row, metric, windowDays) + '</div>';
        }).join('');
    }

    function strategyStatisticalProofSummary(row, metric, windowDays) {
        row = row || {};
        metric = metric || {};
        var hac = metric.statistical_guard || {};
        var forward = metric.internal_forward_stability || {};
        var ess = hac.ess != null ? hac.ess : hac.effective_sample_size;
        var netLcb = hac.net_lcb_pct != null ? hac.net_lcb_pct : hac.net_expectancy_one_sided_95_lcb_pct;
        var pfLcb = hac.pf_lcb != null ? hac.pf_lcb : hac.profit_factor_one_sided_95_lcb;
        var payoffLcb = hac.payoff_lcb != null ? hac.payoff_lcb : hac.payoff_ratio_one_sided_95_lcb;
        var officialHealth = metric.health_score;
        var pointHealth = metric.point_health_score;
        var segments = forward.segments != null ? forward.segments : forward.segment_count;
        var positiveSegments = forward.positive_segments != null ? forward.positive_segments : forward.positive_segment_count;
        var proofReady = (hac.passed === true || hac.threshold_passed === true) && (forward.passed === true);
        var html = '<small class="sc-stat-proof">单侧95%下界：净 ' + strategyGovernanceMetric(netLcb, '%', 3) + ' · PF ' + strategyGovernanceMetric(pfLcb, '', 2) + ' · 盈亏比 ' + strategyGovernanceMetric(payoffLcb, '', 2) + ' · 有效样本 ' + strategyGovernanceMetric(ess, '', 1) + '</small>';
        html += '<small class="sc-stat-proof">内部非重叠时序 ' + strategyGovernanceMetric(positiveSegments, '', 0) + '/' + strategyGovernanceMetric(segments, '', 0) + ' · 正式健康分 ' + strategyGovernanceMetric(officialHealth, '', 1) + '（点估计 ' + strategyGovernanceMetric(pointHealth, '', 1) + '） · ' + (proofReady ? '不可变日历回执已绑定' : '统计证据未通过') + '</small>';
        if (Number(windowDays) === 60) {
            var by = row.statistical_family_decision || {};
            var confirmation = row.confirmation_guard || {};
            var pValue = by.p_value != null ? by.p_value : by.candidate_p_value;
            var critical = by.critical != null ? by.critical : by.critical_value;
            var trials = by.trials != null ? by.trials : by.total_hypotheses;
            var gap = confirmation.gap_sessions != null ? confirmation.gap_sessions : confirmation.minimum_new_sessions;
            var confirmations = confirmation.confirmations != null ? confirmation.confirmations : confirmation.total_confirmation_count;
            html += '<small class="sc-stat-proof">服务端BY多重检验：p ' + strategyGovernanceMetric(pValue, '', 5) + ' / 临界值 ' + strategyGovernanceMetric(critical, '', 5) + ' · 全族 ' + strategyGovernanceMetric(trials, '', 0) + ' 项 · ' + (by.passed === true ? '通过' : '未通过') + '</small>';
            html += '<small class="sc-stat-proof">精确间隔确认：' + strategyGovernanceMetric(confirmations, '', 0) + '/3 次 · 每次至少新增 ' + strategyGovernanceMetric(gap, '', 0) + ' 个权威会话 · ' + (confirmation.passed === true ? '通过' : '等待确认') + '</small>';
        }
        return html;
    }

    function strategyCombinationConstraintSummary(row) {
        row = row || {};
        var evaluation = row.constraint_evaluation || {};
        var correlations = Array.isArray(evaluation.pairwise_correlations) ? evaluation.pairwise_correlations : [];
        var overlaps = Array.isArray(evaluation.pairwise_stock_overlaps) ? evaluation.pairwise_stock_overlaps : [];
        var industries = evaluation.industry_weights_pct || {};
        function maximumNumber(rows, key) {
            var maximum = null;
            rows.forEach(function (item) {
                var raw = item == null ? null : item[key];
                if (raw == null || raw === '') return;
                var value = Number(raw);
                if (!isFinite(value)) return;
                if (maximum == null || value > maximum) maximum = value;
            });
            return maximum;
        }
        var maximumCorrelation = maximumNumber(correlations, 'correlation');
        var maximumOverlap = maximumNumber(overlaps, 'overlap_pct');
        var industryRows = Object.keys(industries).map(function (name) {
            return {name:name, weight:Number(industries[name])};
        }).filter(function (item) { return isFinite(item.weight); }).sort(function (left, right) {
            return right.weight - left.weight || left.name.localeCompare(right.name);
        }).slice(0, 3);
        var relationship = '最高相关 ' + strategyGovernanceMetric(maximumCorrelation, '', 2) + ' · 个股重叠 ' + strategyGovernanceMetric(maximumOverlap, '%', 2);
        var industryFocus = industryRows.length ? industryRows.map(function (item) {
            return escHtml(item.name) + ' ' + strategyGovernanceMetric(item.weight, '%', 1);
        }).join('、') : '待验证';
        return '<strong>' + escHtml(row.correlation_status || '约束证据待验证') + '</strong><small>' + relationship + '</small><small>行业侧重：' + industryFocus + '</small>';
    }

    function strategyGovernanceMetricValue(row, metrics, key) {
        metrics = metrics || {};
        if (metrics[key] != null && metrics[key] !== '') return metrics[key];
        return (row || {})[key];
    }

    function strategyExecutionAdapterSummary(row) {
        row = row || {};
        var adapter = row.execution_adapter || {};
        var executable = adapter.executable === true || row.execution_adapter_executable === true;
        var fundingReady = adapter.funding_pipeline_ready === true || row.funding_pipeline_ready === true;
        var structureReady = adapter.paper_chain_structure_ready === true;
        var evidenceState = String(adapter.funding_evidence_state || '');
        var ledgerInvalid = evidenceState === 'INVALID' || String(adapter.funding_status || '').indexOf('INVALID') >= 0;
        var label = adapter.status_label || (executable ? (fundingReady ? '执行适配器与模拟链成熟证据已就绪' : (structureReady ? '模拟链结构已就绪，证据积累中' : '模拟链校验失败')) : '执行适配器未部署');
        var reason = adapter.reason || row.execution_adapter_reason || '未绑定可验证的执行适配器';
        var identity = [adapter.adapter_key, adapter.adapter_version].filter(Boolean).join(' / ');
        var bindingHash = adapter.execution_binding_hash || row.execution_binding_hash || '';
        var tone = executable && fundingReady ? 'pass' : (executable && structureReady && !ledgerInvalid ? 'pending' : 'fail');
        var evidenceLabels = {
            MATURED:'成熟证据已通过复算',
            EMPTY_ACCUMULATING:'模拟链为空，正在积累首批证据',
            PENDING_MATURITY:'影子试验已产生，等待成熟闭环',
            INVALID:'模拟链证据无效，已阻断'
        };
        var chainText = evidenceLabels[evidenceState] || (fundingReady ? evidenceLabels.MATURED : (structureReady && !ledgerInvalid ? '结构已就绪，证据积累中' : evidenceLabels.INVALID));
        return '<span class="sc-gate-result ' + tone + '">' + escHtml(label) + '</span>' +
            '<small>' + escHtml(reason) + '</small>' +
            '<small>内部模拟链：' + chainText + '</small>' +
            (identity ? '<small>' + escHtml(identity) + '</small>' : '') +
            (bindingHash ? '<small>绑定 ' + escHtml(String(bindingHash).slice(0, 12)) + '</small>' : '');
    }

    function strategyMemberSleevesSummary(row) {
        var sleeves = Array.isArray((row || {}).member_sleeves) ? row.member_sleeves : [];
        if (!sleeves.length) return '';
        return '<div class="sc-sleeve-list">' + sleeves.map(function (item) {
            var key = item.strategy_name || item.strategy_key || '未知成员';
            var version = item.strategy_version || '-';
            var status = strategyLifecycleLabel(item.member_lifecycle_status || item.lifecycle_status);
            var configured = item.configured_weight_pct;
            var base = item.base_weight_pct;
            var effective = item.effective_weight_pct;
            var discount = item.discount_to_cash_pct;
            if (base == null && item.base_basis_points != null) base = Number(item.base_basis_points) / 100;
            if (effective == null && item.effective_basis_points != null) effective = Number(item.effective_basis_points) / 100;
            if (discount == null && item.discount_to_cash_basis_points != null) discount = Number(item.discount_to_cash_basis_points) / 100;
            return '<small><strong>' + escHtml(key) + '</strong> ' + escHtml(version) + ' · ' + escHtml(status) +
                ' · 原始 ' + strategyGovernanceMetric(configured, '%', 2) +
                ' · 基础 ' + strategyGovernanceMetric(base, '%', 2) +
                ' · 生效 ' + strategyGovernanceMetric(effective, '%', 2) +
                ' · 留现金 ' + strategyGovernanceMetric(discount, '%', 2) +
                (item.sleeve_row_hash ? ' · ' + escHtml(String(item.sleeve_row_hash).slice(0, 12)) : '') + '</small>';
        }).join('') + '</div>';
    }

    function strategyCombinationRecipeSummary(row) {
        var recipe = (row || {}).combination_recipe_ref || {};
        var members = Array.isArray(recipe.members) ? recipe.members : [];
        if (!recipe.recipe_hash) {
            return '<small>组合事实配方：尚未生成；不可授予模拟资金</small>';
        }
        var memberText = members.map(function (item) {
            return String(item.strategy_key || '-') + '@' + String(item.strategy_version || '-') +
                ' ' + strategyGovernanceMetric(Number(item.weight || 0) * 100, '%', 2) +
                ' · CP ' + String(item.checkpoint_id || '-').slice(0, 10) +
                ' · facts ' + String(item.history_fact_set_hash || '-').slice(0, 10);
        }).join('；');
        var ready = recipe.member_fact_sets_ready === true;
        return '<div class="sc-sleeve-list"><small><strong>成员事实链复算配方</strong> · ' +
            (ready ? '成员事实集合已冻结' : '成员事实集合未就绪') +
            ' · recipe ' + escHtml(String(recipe.recipe_hash).slice(0, 12)) + '</small>' +
            '<small>' + escHtml(memberText || '没有可复算成员') + '</small>' +
            '<small>不生成独立组合现金事实；' + escHtml(recipe.reason || '未完成复算前不可授资') + '</small></div>';
    }

    function strategyIndustryFocusSummary(row) {
        row = row || {};
        var focus = Array.isArray(row.industry_focus) ? row.industry_focus.slice(0, 3) : [];
        var text = focus.map(function (item) {
            return String(item.industry_name || '未分类') + ' ' + strategyGovernanceMetric(item.candidate_share_pct, '%', 1);
        }).join('、');
        return '<strong>' + escHtml(row.primary_industry || '待形成行业侧重') + '</strong><small>' +
            escHtml(text || '尚无有效候选样本') + '</small><small>' +
            strategyGovernanceMetric(row.industry_candidate_count, '', 0) + ' 个行业候选</small>';
    }

    function strategyLifecycleLabel(status) {
        var labels = {ACTIVE:'正常运行',REDUCE:'降权运行',SHADOW:'影子观察',SUSPENDED:'暂停使用',RETIRED:'已淘汰'};
        return labels[String(status || '').toUpperCase()] || '未知状态';
    }

    function strategyFundingProvenanceLabel(provenance) {
        var value = String(provenance || '').toUpperCase();
        if (value === 'INTERNAL_PORTFOLIO_CHECKPOINT_FACT_LEDGER_V3') {
            return '检查点与日频事实链资金证据';
        }
        if (value === 'INTERNAL_PORTFOLIO_CHECKPOINT_LEDGER_V2' || value === 'INTERNAL_PORTFOLIO_LEDGER_V1') {
            return '旧版内部账本，仅供历史展示';
        }
        return '外部研究证据，不授予资金';
    }

    function strategyTradingGateLabel(status) {
        var labels = {
            ALLOW_NEW_BUY:'正常新增风险',
            REDUCE_NEW_BUY:'降权新增风险',
            BLOCK_NEW_BUY:'暂停新增风险',
            DATA_NOT_READY:'数据未就绪',
            REVIEW_REQUIRED:'等待人工复核'
        };
        return labels[String(status || '').toUpperCase()] || '市场门禁待确认';
    }

    function strategyLifecycleTone(status) {
        status = String(status || '').toUpperCase();
        if (status === 'ACTIVE') return 'active';
        if (status === 'REDUCE') return 'reduce';
        if (status === 'SUSPENDED') return 'suspended';
        if (status === 'RETIRED') return 'retired';
        return 'shadow';
    }

    function strategyGovernanceActions(row, entityType) {
        var authRole = String((((window._strategyCenterAuth || {}).user || {}).role) || '').toUpperCase();
        if (authRole !== 'ADMIN') return '<span class="sc-muted">仅管理员可治理</span>';
        var status = String(row.current_status || 'SHADOW').toUpperCase();
        var key = entityType === 'COMBINATION' ? row.combination_key : row.strategy_key;
        var buttons = [];
        function add(label, next) {
            buttons.push('<button class="sc-mini-btn" onclick="window._strategyGovernanceTransition(\'' + escAttr(key) + '\',\'' + next + '\',\'' + entityType + '\')">' + label + '</button>');
        }
        if (status === 'ACTIVE' || status === 'REDUCE') add('转为影子', 'SHADOW');
        if (status !== 'SUSPENDED' && status !== 'RETIRED') add('暂停', 'SUSPENDED');
        if (status !== 'RETIRED') add('淘汰版本', 'RETIRED');
        if (status === 'SUSPENDED') buttons.unshift('<span class="sc-muted">等待暂停后新证据自动恢复</span>');
        return buttons.join('') || '<span class="sc-muted">终态，仅可注册新版本</span>';
    }

    function strategyGovernancePaginationHtml(page, entityType) {
        page = page || {};
        var normalizedType = String(entityType || '').toUpperCase();
        var inputId = normalizedType === 'COMBINATION' ? 'scCombinationRankSearch' : 'scStrategyRankSearch';
        var total = Number(page.total_count || 0);
        var limit = Math.max(1, Number(page.limit || 50));
        var offset = Math.max(0, Number(page.offset || 0));
        var pageNo = total ? Math.floor(offset / limit) + 1 : 1;
        var pageCount = Math.max(1, Math.ceil(total / limit));
        var previous = page.previous_cursor == null ? '' : String(page.previous_cursor);
        var next = page.next_cursor == null ? '' : String(page.next_cursor);
        return '<div class="sc-governance-toolbar sc-ranking-pagination">' +
            '<label>搜索<input id="' + inputId + '" maxlength="80" value="' + escAttr(page.query || '') + '" onkeydown="if(event.key===\'Enter\'){window._strategyGovernanceRankingSearch(\'' + normalizedType + '\',\'' + inputId + '\')}"></label>' +
            '<button class="sc-btn" ' + (previous ? '' : 'disabled ') + 'onclick="window._strategyGovernanceRankingPage(\'' + normalizedType + '\',\'' + escAttr(previous) + '\',\'' + escAttr(page.query || '') + '\')">上一页</button>' +
            '<span>第 ' + pageNo + ' / ' + pageCount + ' 页 · 共 ' + total + ' 条 · 每页最多 ' + limit + ' 条</span>' +
            '<button class="sc-btn" ' + (next ? '' : 'disabled ') + 'onclick="window._strategyGovernanceRankingPage(\'' + normalizedType + '\',\'' + escAttr(next) + '\',\'' + escAttr(page.query || '') + '\')">下一页</button>' +
            '<button class="sc-btn" onclick="window._strategyGovernanceRankingSearch(\'' + normalizedType + '\',\'' + inputId + '\')">搜索</button></div>';
    }

    function strategyGovernanceHistoryPaginationHtml(page, section) {
        page = page || {};
        var normalizedSection = String(section || '').toLowerCase();
        var inputIds = {
            lifecycle:'scLifecycleHistorySearch',
            audit:'scAuditHistorySearch',
            metric_evidence:'scMetricEvidenceHistorySearch',
            adapter_run_receipts:'scAdapterReceiptHistorySearch'
        };
        var inputId = inputIds[normalizedSection] || '';
        var state = ((window._strategyGovernanceHistoryState || {})[normalizedSection]) || {};
        var total = Number(page.total_count || 0);
        var rowCount = Number(page.row_count || ((page.rows || []).length) || 0);
        var pageNo = Math.max(1, Number(state.pageNo || 1));
        var filters = page.filters || {};
        var hasPrevious = Array.isArray(state.stack) && state.stack.length > 0;
        var hasOlder = typeof page.next_cursor === 'string' && page.next_cursor.length > 0;
        var filterHtml = inputId ? '<label>对象代码筛选<input id="' + inputId + '" maxlength="80" value="' + escAttr(filters.entity_key || '') + '" onkeydown="if(event.key===\'Enter\'){window._strategyGovernanceHistoryPage(\'' + normalizedSection + '\',\'search\')}"></label>' +
            '<button class="sc-btn" onclick="window._strategyGovernanceHistoryPage(\'' + normalizedSection + '\',\'search\')">筛选</button>' : '';
        return '<div class="sc-governance-toolbar sc-history-pagination">' + filterHtml +
            '<button class="sc-btn" ' + (hasPrevious ? '' : 'disabled ') + 'onclick="window._strategyGovernanceHistoryPage(\'' + normalizedSection + '\',\'previous\')">较新一页</button>' +
            '<span>第 ' + pageNo + ' 页 · 本页 ' + rowCount + ' 条 · 共 ' + total + ' 条</span>' +
            '<button class="sc-btn" ' + (hasOlder ? '' : 'disabled ') + 'onclick="window._strategyGovernanceHistoryPage(\'' + normalizedSection + '\',\'next\')">更早一页</button>' +
            '<small>修订 ' + escHtml(String(page.history_revision_hash || '-').slice(0, 12)) + '</small></div>';
    }

    function strategyFundingDetailButton(row, entityType) {
        row = row || {};
        var normalizedType = String(entityType || '').toUpperCase();
        var key = normalizedType === 'COMBINATION' ? row.combination_key : row.strategy_key;
        var available = normalizedType === 'COMBINATION'
            ? (row.paper_allocation_eligible === true && row.funding_recipe_ready === true && !!row.combination_recipe_ref)
            : !!row.funding_checkpoint_ref;
        if (!available) return '<small>当前无可展开的V3资金事实</small>';
        return '<button class="sc-mini-btn" onclick="window._strategyFundingDetail(\'' + normalizedType + '\',\'' + escAttr(key || '') + '\',60,\'daily_records\',\'\')">查看资金明细</button>';
    }

    function strategyGovernancePoolTruth(governance, requestedDate, latestFormalDate) {
        governance = governance || {};
        var datePattern = /^\d{4}-\d{2}-\d{2}$/;
        var target = String(requestedDate || governance.requested_trade_date || governance.trade_date || '').slice(0, 10);
        var resultDate = String(governance.trade_date || '').slice(0, 10);
        var resultMode = String(governance.result_mode || '').toUpperCase();
        var runUid = String(governance.run_uid || '');
        var resultHash = String(governance.canonical_result_hash || '');
        var latestDate = String(latestFormalDate || '').slice(0, 10);
        function blocked(reason, code) {
            return { ready:false, verifiedCompleted:false, requestedDate:target, resultDate:resultDate, runUid:runUid, reason:reason, reasonCode:code || 'FORMAL_GOVERNANCE_POOL_BLOCKED' };
        }
        if (!datePattern.test(target)) return blocked('缺少有效请求日，不能确认当前规范票池', 'REQUEST_DATE_INVALID');
        if (!datePattern.test(latestDate)) return blocked('无法确认最新正式交易日，规范票池保持研究只读', 'LATEST_FORMAL_DATE_UNKNOWN');
        if (target !== latestDate) return blocked('请求日 ' + target + ' 不是最新正式交易日 ' + latestDate + '，只允许历史研究查看', 'HISTORICAL_RESEARCH_ONLY');
        if (String(governance.strategy_governance_mode || '').toUpperCase() === 'DEFERRED_DB' || governance.governance_deferred === true || governance.activation_enabled === false) return blocked(governance.input_reason || '治理数据库处于 DEFERRED_DB，候选只可研究审计', 'GOVERNANCE_DATABASE_DEFERRED');
        if (governance.is_canonical !== true || resultMode !== 'CANONICAL_PERSISTED' || String(governance.status || '').toLowerCase() !== 'ok' || governance.input_ready !== true) return blocked(governance.input_reason || '治理结果不是已完成且通过复算的 canonical 快照', 'CANONICAL_NOT_VERIFIED_COMPLETED');
        if (resultDate !== target) return blocked('canonical 结果日 ' + (resultDate || '未知') + ' 与最新正式交易日 ' + target + ' 不一致', 'CANONICAL_DATE_MISMATCH');
        if (!/^[0-9a-f]{32}$/i.test(runUid) || !/^[0-9a-f]{64}$/i.test(resultHash)) return blocked('canonical 运行编号或结果哈希无效', 'CANONICAL_IDENTITY_INVALID');
        if (governance.automatic_real_order_submission !== false || governance.real_order_authority !== false) return blocked('规范结果没有显式关闭真实下单权限', 'ORDER_AUTHORITY_INVALID');
        return { ready:true, verifiedCompleted:true, requestedDate:target, resultDate:resultDate, runUid:runUid, reason:'canonical 身份、日期、哈希与 COMPLETED 持久结果均已验证', reasonCode:'VERIFIED_COMPLETED_CURRENT_POOL' };
    }

    function strategyPaperExecutionPlanHtml(governance) {
        governance = governance || {};
        var title = '<div class="sc-section-title"><span>个股级模拟执行计划</span><small>资金分配继续落实到单票；单票、权威一级行业、组合相关性、预期损失和新增买入换手超限的额度自动留在现金，退出不受新增买入上限阻挡</small></div>';
        var mode = String(governance.strategy_governance_mode || '').toUpperCase();
        var resultMode = String(governance.result_mode || '').toUpperCase();
        var deferred = mode === 'DEFERRED_DB' || governance.activation_enabled === false;
        if (deferred) {
            return title + '<div class="sc-warning" data-execution-plan-state="blocked"><strong>规范执行计划不可用</strong>：' + escHtml(governance.input_reason || '治理数据库迁移尚未完成') + '。当前保持 100% 现金并禁止新增买入；这不是一个已经验证的“0只空仓”结论。<small>阻断阶段 ' + escHtml(governance.blocking_stage || 'DATABASE_MIGRATION') + ' · 原因 ' + escHtml(governance.reason_code || 'GOVERNANCE_DATABASE_DEFERRED') + '</small></div>';
        }

        var plan = governance.paper_execution_plan;
        var targets = plan && Array.isArray(plan.targets) ? plan.targets : null;
        var exits = plan && Array.isArray(plan.exit_targets) ? plan.exit_targets : null;
        var planHash = plan && String(plan.plan_hash || '');
        var topHash = String(governance.paper_execution_plan_hash || '');
        var canonical = governance.is_canonical === true && resultMode === 'CANONICAL_PERSISTED';
        var planRowsSafe = targets && targets.every(function (row) {
            return row && typeof row === 'object' && row.allocation_backed === true && row.new_buy_allowed === true && row.exit_always_allowed === true && row.real_order_authority === false;
        });
        var exitRowsSafe = exits && exits.every(function (row) {
            return row && typeof row === 'object' && row.new_buy_allowed === false && row.exit_always_allowed === true && row.real_order_authority === false;
        });
        var weightsValid = plan && Number(plan.invested_bp) >= 0 && Number(plan.cash_bp) >= 0 && Number(plan.invested_bp) + Number(plan.cash_bp) === 10000;
        var valid = canonical && plan && typeof plan === 'object' && plan.schema === 'probiga.governance-paper-execution-plan.v1' && targets && exits && planHash && planHash === topHash && plan.automatic_real_order_submission === false && plan.real_order_authority === false && governance.automatic_real_order_submission === false && governance.real_order_authority === false && Number(plan.target_count) === targets.length && planRowsSafe && exitRowsSafe && weightsValid;
        if (!valid) {
            return title + '<div class="sc-warning" data-execution-plan-state="unavailable"><strong>规范执行计划不可用</strong>：' + escHtml(governance.input_reason || '当前结果未通过规范身份、计划哈希或权限边界校验') + '。页面不会把研究候选升级为执行目标，也不会把缺失计划显示成有效空仓。<small>结果 ' + escHtml(resultMode || 'UNKNOWN') + ' · 阶段 ' + escHtml(governance.blocking_stage || 'CANONICAL_READ') + ' · 原因 ' + escHtml(governance.reason_code || 'PAPER_PLAN_UNAVAILABLE') + '</small></div>';
        }

        var risk = plan.portfolio_risk || {};
        var h = title + '<div class="sc-governance-notice" data-execution-plan-state="canonical"><strong>计划摘要</strong><span>' + targets.length + ' 只 · 模拟风险资产 ' + strategyGovernanceMetric(Number(plan.invested_bp) / 100, '%', 1) + ' · 现金 ' + strategyGovernanceMetric(Number(plan.cash_bp) / 100, '%', 1) + ' · 年化波动 ' + strategyGovernanceMetric(risk.annualized_volatility_pct, '%', 2) + ' · 日度ES(95%) ' + strategyGovernanceMetric(risk.expected_shortfall_95_pct, '%', 2) + ' · 真实下单权限关闭</span><small>交易日 ' + escHtml(plan.trade_date || governance.trade_date || '-') + ' · 计划哈希 ' + escHtml(planHash.slice(0, 16)) + '</small></div>';
        if (!targets.length && !exits.length) {
            return h + '<div class="sc-governance-notice"><strong>规范空计划已验证</strong><span>本轮没有新增、调仓或退出目标；现金比例和权限边界已经通过规范批次校验。</span></div>';
        }
        if (targets.length) {
            h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>证券/行业</th><th>策略/版本</th><th>资金归属</th><th>目标/原权重</th><th>新增买入</th><th>参考价/整手数</th><th>机会/执行分</th><th>盈亏比/止损</th><th>两档止盈</th><th>权限</th></tr></thead><tbody>';
            targets.forEach(function (row) {
                var allocation = String(row.allocation_target_type || '-') + ' · ' + String(row.allocation_target_key || '-') + (row.allocation_target_version ? ' · ' + String(row.allocation_target_version) : '');
                h += '<tr><td><strong>' + escHtml(row.stock_name || row.stock_code || '-') + '</strong><small>' + escHtml(row.stock_code || '-') + ' · ' + escHtml(row.industry_name || '行业待确认') + '</small></td><td>' + escHtml(row.strategy_key || '-') + '<small>' + escHtml(row.strategy_version || '-') + '</small></td><td class="sc-wrap">' + escHtml(allocation) + '</td><td>' + strategyGovernanceMetric(Number(row.target_bp || 0) / 100, '%', 2) + '<small>原 ' + strategyGovernanceMetric(Number(row.previous_target_bp || 0) / 100, '%', 2) + '</small></td><td>' + strategyGovernanceMetric(Number(row.new_buy_delta_bp || 0) / 100, '%', 2) + '</td><td>' + strategyGovernanceMetric(row.reference_price, '', 4) + '<small>' + escHtml(row.reference_board_lot_quantity == null ? '-' : String(row.reference_board_lot_quantity) + ' 股') + ' · 仅参考，OMS重算</small></td><td>' + strategyGovernanceMetric(row.opportunity_score, '', 1) + ' / ' + strategyGovernanceMetric(row.execution_score, '', 1) + '</td><td>' + strategyGovernanceMetric(row.planned_risk_reward_ratio, '', 2) + '<small>止损 ' + strategyGovernanceMetric(row.stop_loss_price, '', 4) + '</small></td><td>' + strategyGovernanceMetric(row.take_profit_1, '', 4) + '<small>' + strategyGovernanceMetric(row.take_profit_2, '', 4) + '</small></td><td><span class="sc-gate-result pass">仅模拟</span><small>新增买入允许 · 退出始终允许 · 真实下单关闭</small></td></tr>';
            });
            h += '</tbody></table></div>';
        } else {
            h += '<div class="sc-governance-notice"><strong>本轮无新增或持有目标</strong><span>规范批次有效；请继续查看下方退出计划。</span></div>';
        }
        if (exits.length) {
            h += '<div class="sc-section-title"><span>退出目标</span><small>退出不受新增买入换手上限阻挡；所有行仍为模拟建议，不授予真实下单权限</small></div><div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>证券</th><th>原权重 → 目标</th><th>动作</th><th>退出权限</th><th>新增买入</th><th>真实下单</th></tr></thead><tbody>';
            exits.forEach(function (row) {
                h += '<tr><td><strong>' + escHtml(row.stock_code || '-') + '</strong></td><td>' + strategyGovernanceMetric(Number(row.previous_target_bp || 0) / 100, '%', 2) + ' → 0.00%</td><td>' + escHtml(row.action === 'EXIT_OR_REDUCE_ONLY' ? '仅退出或减仓' : row.action || '-') + '</td><td><span class="sc-gate-result pass">始终允许退出</span></td><td>禁止</td><td>关闭</td></tr>';
            });
            h += '</tbody></table></div>';
        }
        return h;
    }

    function strategyGovernanceHtml(governance, history) {
        governance = governance || {};
        history = history || {};
        var poolTruth = strategyGovernancePoolTruth(governance, window._strategyCenterDate || governance.trade_date, latestFormalStrategyDateValue());
        var summary = Object.assign({}, governance.summary || {});
        var strategies = Array.isArray(governance.strategies) ? governance.strategies : [];
        var combinations = Array.isArray(governance.combinations) ? governance.combinations : [];
        var rankingPages = governance.ranking_pages || {};
        var adapterCapabilities = Array.isArray(governance.adapter_capabilities) ? governance.adapter_capabilities : [];
        var adapterCapabilitySummary = governance.adapter_capability_summary || {};
        var rawPools = governance.pools || {};
        var pools = poolTruth.ready ? rawPools : {observation:[], confirmation:[], tradable:[]};
        var rawResearchRows = [];
        if (!poolTruth.ready) ['observation','confirmation','tradable'].forEach(function (level) {
            (Array.isArray(rawPools[level]) ? rawPools[level] : []).forEach(function (row) {
                rawResearchRows.push({level:level, row:row || {}});
            });
        });
        var allocations = poolTruth.ready && Array.isArray(governance.allocations) ? governance.allocations : [{target_type:'CASH',target_key:'cash',name:'现金',simulated_weight_pct:100,reason:poolTruth.reason,real_order_authority:false}];
        if (!poolTruth.ready) {
            summary.observation_count = 0;
            summary.confirmation_count = 0;
            summary.tradable_count = 0;
            summary.allocation_count = 0;
            summary.cash_weight_pct = 100;
        }
        var lifecycle = Array.isArray(history.lifecycle_events) ? history.lifecycle_events : [];
        var audits = Array.isArray(history.audit_events) ? history.audit_events : [];
        var historyPages = history.history_pages || {};
        var runs = Array.isArray(history.runs) ? history.runs : [];
        var metricEvidence = Array.isArray(history.metric_evidence) ? history.metric_evidence : [];
        var challengers = Array.isArray(history.challengers) ? history.challengers : [];
        var tradingGate = governance.trading_gate || {};
        var tradingGateLabel = summary.trading_gate_status_label || tradingGate.status_label || strategyTradingGateLabel(summary.trading_gate_status || tradingGate.status);
        var marketRiskCap = summary.market_risk_cap_pct;
        if (marketRiskCap == null) marketRiskCap = tradingGate.market_risk_cap_pct;
        var authRole = String(((((window._strategyCenterAuth || {}).user) || {}).role) || '').toUpperCase();
        var governanceDeferred = String(governance.strategy_governance_mode || '').toUpperCase() === 'DEFERRED_DB' || governance.activation_enabled === false;
        var canAdmin = authRole === 'ADMIN' && !governanceDeferred;
        var canReview = authRole === 'EVIDENCE_REVIEWER';
        var roleLabel = authRole === 'ADMIN' ? (governanceDeferred ? '管理员（治理暂锁）' : '管理员') : (canReview ? '证据复核员' : '只读账号');
        var resultMode = String(governance.result_mode || '').toUpperCase();
        var canonicalLabel = governance.is_canonical === true || resultMode.indexOf('CANONICAL_PERSISTED') === 0 ? '当前规范结果' : (resultMode.indexOf('PREVIEW') === 0 ? '实时预览（不生效）' : (resultMode === 'CANONICAL_UNAVAILABLE' ? '尚无规范结果' : '结果状态待确认'));
        window._strategyCenterAdapterCapabilities = adapterCapabilities;
        var adapterCapabilityOptions = '<option value="">仅登记研究元数据，不绑定执行代码</option>' + adapterCapabilities.map(function (item, index) {
            var evaluatorTypes = Array.isArray(item.evaluator_types) ? item.evaluator_types.join(' / ') : '';
            var label = (item.adapter_key || '未命名适配器') + ' · ' + (item.adapter_version || '-') + (evaluatorTypes ? ' · ' + evaluatorTypes : '');
            return '<option value="' + index + '">' + escHtml(label) + '</option>';
        }).join('');
        var h = '<section class="sc-governance">';
        h += '<div class="sc-section-title"><span>动态策略治理总览</span><small>策略数量不设上限 · 每日重新评估 · 无合格策略保持现金</small></div>';
        if (governanceDeferred) {
            var baseSchemaMessage = governance.base_schema_ready === true ? '基础表、字段、索引、初始化数据和版本标记已上线' : '基础数据库结构尚未完成验证';
            h += '<div class="sc-warning"><strong>数据库防篡改门禁待完成</strong>：' + baseSchemaMessage + '；排行、健康度、治理票池和状态变更暂不生效，模拟资金保持 100% 现金，真实下单与新买入均关闭。</div>';
        }
        h += '<div class="sc-governance-notice" data-formal-pool-state="' + (poolTruth.ready ? 'VERIFIED_COMPLETED' : 'RESEARCH_ONLY') + '"><strong>' + (poolTruth.ready ? 'VERIFIED COMPLETED / 当前正式票池' : 'RESEARCH_ONLY / 正式票池不可用') + '</strong><span>请求日 ' + escHtml(poolTruth.requestedDate || '-') + ' · 结果日 ' + escHtml(poolTruth.resultDate || '-') + ' · run_uid ' + escHtml(String(poolTruth.runUid || '-').slice(0, 12)) + '；' + escHtml(poolTruth.reason) + '。</span><small>' + (poolTruth.ready ? '仅当前请求日、哈希有效的 canonical COMPLETED 批次进入分层票池；真实下单固定关闭。' : '旧日期、未验证、DEFERRED 或 research-only 候选只能在研究只读区查看，不进入正式票池。') + '</small></div>';
        h += '<div class="sc-governance-toolbar">' + (canAdmin ? '<button id="scGovernanceRunBtn" class="sc-btn primary" onclick="window._strategyGovernanceRun()">执行最新数据日治理</button><button class="sc-btn" onclick="window._strategyRegistrationToggle()">新增策略 / 新版本</button><button class="sc-btn" onclick="window._strategyCombinationToggle()">新增组合 / 新版本</button><button class="sc-btn" onclick="window._strategyReviewerToggle()">创建独立复核账号</button>' : '') + '<span>当前职责：' + escHtml(roleLabel) + '</span><span>结果口径：' + escHtml(canonicalLabel) + '</span><span>真实下单权限：关闭</span><span>市场门禁：' + escHtml(tradingGateLabel) + '</span><span>风险上限：' + strategyGovernanceMetric(marketRiskCap, '%', 1) + '</span><span>模拟现金：' + strategyGovernanceMetric(summary.cash_weight_pct, '%', 1) + '</span><span>构建：' + escHtml(String(governance.build_commit_sha || '-').slice(0, 12)) + '</span><span>路由：' + escHtml(String(governance.router_snapshot_hash || '-').slice(0, 12)) + '</span><span>决策哈希：' + escHtml(String(governance.decision_hash || '-').slice(0, 12)) + '</span></div>';
        h += '<div class="sc-governance-notice"><strong>收益与双榜口径</strong><span>客户端声明信号榜只比较经双人复核、结构可重算的外部提交，但提交来源未与权威行情逐行认证；成交实证榜只比较内部模拟成交、实际费用和逐日净值。共享账户里同票未成交不会被伪造成 fill。声明信号榜不授予模拟资金，缺少它也不会否定已由内部账本完整证明的资金资格。正期望、盈亏比和利润因子不代表未来一定盈利；任何内部证据、行情或风险门槛失败时资金回到现金。</span></div>';
        h += '<div class="sc-governance-notice"><strong>行业数据口径</strong><span>精确日期行业/概念成分归属、来源特定板块热度、按成分股聚合的强弱、QMT原生 .BKZS 板块指数是四类不同证据；不得互相替代，未认证的 .BKZS 不用合成曲线补齐。</span></div>';
        h += '<div id="scFundingDetailPanel" class="sc-registration" style="display:none"></div>';
        h += '<div id="scRegistrationForm" class="sc-registration" style="display:none">';
        h += '<div><label>策略代码<input id="scRegKey" placeholder="例如 earnings_surprise"></label><label>中文名称<input id="scRegName" placeholder="业绩超预期漂移"></label><label>版本<input id="scRegVersion" placeholder="v1.0.0"></label><label>分类<input id="scRegCategory" placeholder="事件/趋势/轮动"></label><label>最大持有日<input id="scRegMaxHold" type="number" min="1" max="250" step="1" placeholder="1~250"></label></div>';
        h += '<div><label>趋势偏多系数<input id="scRegRouteTrend" type="number" min="0" max="1.5" step="0.05" placeholder="0~1.5"></label><label>高位震荡系数<input id="scRegRouteRange" type="number" min="0" max="1.5" step="0.05" placeholder="0~1.5"></label><label>风险下降系数<input id="scRegRouteRisk" type="number" min="0" max="1.5" step="0.05" placeholder="0~1.5"></label><label>极端事件系数<input value="0" disabled></label></div>';
        h += '<div><label>已部署执行适配器（可选）<select id="scRegAdapterCapability" onchange="window._strategyAdapterCapabilityApply()">' + adapterCapabilityOptions + '</select></label><label>评估器类型<select id="scRegEvaluatorType"><option value="external_evidence">外部研究证据（仅影子）</option></select></label><label>执行适配器代码<input id="scRegAdapterKey" readonly placeholder="由已部署能力自动填写"></label><label>适配器版本<input id="scRegAdapterVersion" readonly placeholder="由已部署能力自动填写"></label><label>制品 SHA-256<input id="scRegArtifactSha" readonly maxlength="64" placeholder="由服务器可信清单提供"></label><label>成本模型代码<input id="scRegCostModelKey" placeholder="例如 cn_a_share_v1"></label><label>币种<input value="CNY" disabled></label></div>';
        h += '<div><label>单边佣金(%)<input id="scRegCommission" type="number" min="0" max="10" step="0.000001" placeholder="必须明确填写"></label><label>卖出印花税(%)<input id="scRegStampTax" type="number" min="0" max="10" step="0.000001" placeholder="必须明确填写"></label><label>单边滑点(%)<input id="scRegSlippage" type="number" min="0" max="10" step="0.000001" placeholder="必须明确填写"></label><label>单边过户费(%)<input id="scRegTransferFee" type="number" min="0" max="10" step="0.000001" placeholder="必须明确填写"></label></div>';
        h += '<label>说明<textarea id="scRegDescription" placeholder="策略适用市场、入场逻辑、失效条件"></textarea></label><button class="sc-btn primary" onclick="window._strategyRegister(true)">登记为挑战者（已有策略新版本）</button><button class="sc-btn" onclick="window._strategyRegister(false)">注册全新策略代码</button><small>注册入口只允许从未存在过的新策略代码；已有策略的任何新版本必须走挑战者流程。挑战者提交的逐笔交易、权益曲线与 Purged Walk-Forward 产物由服务器重算结构和哈希，但数据来源仍是客户端声明，复核通过最多只允许晋级为无资金的影子版本。只有后续执行适配器在内部模拟账户产生可重算成交、费用和逐日净值，并通过全部资金门槛，才会进入模拟可交易池。</small></div>';
        h += '<div id="scCombinationForm" class="sc-registration" style="display:none"><div><label>组合代码<input id="scComboKey" placeholder="例如 earnings_trend_mix"></label><label>中文名称<input id="scComboName" placeholder="业绩趋势组合"></label><label>版本<input id="scComboVersion" placeholder="v1.0.0"></label></div><label>成员与原始权重<textarea id="scComboMembers" placeholder="right_side_trend=45, theme_diffusion=35, low_base_ignition=20"></textarea></label><div><label>单成员上限<input id="scComboMaxMember" type="number" min="0.05" max="1" step="0.05" value="0.60"></label><label>相关系数上限<input id="scComboMaxCorr" type="number" min="-1" max="1" step="0.05" value="0.80"></label><label>同步观测下限<input id="scComboMinCorrObs" type="number" min="20" max="5000" step="1" value="60"></label><label>个股重叠上限(%)<input id="scComboMaxOverlap" type="number" min="0" max="100" step="1" value="40"></label><label>单行业上限(%)<input id="scComboMaxIndustry" type="number" min="1" max="100" step="1" value="45"></label></div><label>说明<textarea id="scComboDescription" placeholder="组合适用市场、成员分工、相关性和失效条件"></textarea></label><button class="sc-btn primary" onclick="window._strategyCombinationRegister()">冻结当前成员版本并进入影子观察</button><small>至少两个已注册成员；系统会冻结精确成员版本、权重和相关性/个股/行业约束。组合净值按成员已验证事实链复算，不生成或伪造组合现金事实；未完成复算和独立复核前不能获得模拟资金。</small></div>';
        if (canAdmin) h += '<div id="scReviewerForm" class="sc-registration" style="display:none"><div><label>独立复核账号<input id="scReviewerName" autocomplete="off" placeholder="例如 strategy_reviewer"></label><label>初始密码<input id="scReviewerPassword" type="password" autocomplete="new-password" placeholder="至少10位，仅当次提交"></label><label>角色<input value="证据复核员" disabled></label></div><button class="sc-btn primary" onclick="window._strategyReviewerCreate()">由管理员创建复核账号</button><small>管理员负责提交证据，证据复核员只负责确认或驳回；二者必须使用不同实名账号。密码不会显示、回传或写入治理日志，旧管理令牌没有治理写权限。</small></div>';
        h += '<div class="sc-stats governance">';
        [['strategy_count','动态策略'],['formal_count','正式/降权'],['shadow_count','影子观察'],['suspended_count','暂停'],['retired_count','已淘汰'],['combination_count','策略组合'],['observation_count','观察池'],['confirmation_count','确认池'],['tradable_count','模拟可交易池'],['allocation_count','获模拟资金']].forEach(function (item) { h += '<div><strong>' + (summary[item[0]] == null ? 0 : summary[item[0]]) + '</strong><span>' + item[1] + '</span></div>'; });
        h += '</div>';
        h += '<div class="sc-lifecycle-legend"><span class="active">正常运行</span><span class="reduce">降权运行</span><span class="shadow">影子观察</span><span class="suspended">暂停使用</span><span class="retired">已淘汰</span><em>页面只显示中文状态；内部代码仅用于审计和接口兼容。</em></div>';
        if (governance.input_ready === false) h += '<div class="sc-warning">治理输入未就绪：' + escHtml(governance.input_reason || '数据新鲜度或日期校验未通过') + '。当前不会更新生命周期，也不会生成模拟资金候选。</div>';
        h += '<div class="sc-governance-notice"><strong>今日市场路由</strong><span>' + escHtml(tradingGateLabel) + '；' + escHtml(summary.trading_gate_reason || tradingGate.reason || '等待市场门禁证据') + '。组合新增风险总上限 ' + strategyGovernanceMetric(marketRiskCap, '%', 1) + '，额度之外保持现金。</span></div>';
        if (governance.error) h += '<div class="sc-warning">治理数据降级：' + escHtml(governance.error) + '</div>';
        if (Number(adapterCapabilitySummary.adapter_count) === 0) h += '<div class="sc-governance-notice"><strong>动态执行能力</strong><span>基础设施就绪、动态执行未启用；真实下单权限保持关闭。</span></div>';

        h += '<div class="sc-section-title"><span>单策略竞技场</span><small>资金指标来自资本加权日频净值；逐笔胜率/盈亏比仅属于交易诊断或独立信号研究口径。20日近期稳定、60日盈利门槛、120日长期稳定必须共同通过</small></div>';
        h += strategyGovernancePaginationHtml(rankingPages.strategy, 'STRATEGY');
        h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>排名/赛道</th><th>策略与版本</th><th>执行适配器</th><th>行业侧重</th><th>中文状态</th><th>双榜成绩</th><th>20/60/120窗口证据</th><th>样本/覆盖</th><th>盈利日占比（资金口径）</th><th>日均净收益（资金口径）</th><th>日频盈亏比</th><th>日频PF</th><th>最大回撤</th><th>准入结果与理由</th><th>恢复条件</th><th>治理操作</th></tr></thead><tbody>';
        strategies.forEach(function (row) {
            var m = (row.metrics || {})['60'] || row.primary_metrics || {};
            var evidenceBlocks = Array.isArray(row.evidence_block_reasons) ? row.evidence_block_reasons.join('；') : '';
            var adapterExecutable = ((row.execution_adapter || {}).executable === true) || row.execution_adapter_executable === true;
            var fundingPipelineReady = ((row.execution_adapter || {}).funding_pipeline_ready === true) || row.funding_pipeline_ready === true;
            var paperChainStructureReady = ((row.execution_adapter || {}).paper_chain_structure_ready === true);
            var strategyAdmissionReady = row.profit_gate_passed && row.market_route_eligible && adapterExecutable && fundingPipelineReady;
            var officialStrategyRank = row.execution_evidence_comparable === true
                ? row.execution_evidence_rank
                : (row.signal_validation_comparable === true ? row.signal_validation_rank : null);
            var officialStrategyRankBasis = row.execution_evidence_comparable === true
                ? '成交实证'
                : (row.signal_validation_comparable === true ? '客户端声明（研究）' : '');
            var strategyRankHtml = officialStrategyRank == null ? '<strong>未入榜</strong>' : '<strong>' + escHtml(officialStrategyRankBasis) + ' #' + Number(officialStrategyRank) + '</strong>';
            h += '<tr><td>' + strategyRankHtml + '<small>' + escHtml(row.lane || '-') + '</small></td><td><strong>' + escHtml(row.strategy_name || row.strategy_key) + '</strong><small>' + escHtml(row.strategy_key || '-') + ' · ' + escHtml(row.current_version || '-') + '</small></td><td>' + strategyExecutionAdapterSummary(row) + '</td><td>' + strategyIndustryFocusSummary(row) + '</td><td><span class="sc-life ' + strategyLifecycleTone(row.current_status) + '">' + escHtml(strategyLifecycleLabel(row.current_status)) + '</span><small>' + escHtml(row.status_reason || '-') + '</small></td><td>' + strategyCompetitionScoreSummary(row) + '</td><td class="sc-window-cell">' + strategyGovernanceWindowSummary(row) + strategyFundingDetailButton(row, 'STRATEGY') + '</td><td>' + strategyGovernanceMetric(m.completed_trades, '', 0) + '笔<small>' + strategyGovernanceMetric(m.coverage_days, '', 0) + '日</small></td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'win_rate_pct'), '%', 1) + '</td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'net_expectancy_pct'), '%', 3) + '</td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'payoff_ratio'), '', 2) + '</td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'profit_factor'), '', 2) + '</td><td>' + strategyGovernanceMetric(m.max_drawdown_pct, '%', 2) + '<small>' + (m.drawdown_basis === 'internal_version_bound_portfolio_equity' ? '内部日频组合权益' : '交易序列诊断') + '</small></td><td><span class="sc-gate-result ' + (strategyAdmissionReady ? 'pass' : (paperChainStructureReady && adapterExecutable ? 'pending' : 'fail')) + '">' + (strategyAdmissionReady ? '盈利、执行、成熟资金证据与行情均通过' : (!fundingPipelineReady && adapterExecutable && paperChainStructureReady ? '模拟链结构已就绪，证据积累中' : (!fundingPipelineReady && adapterExecutable ? '模拟链校验失败' : '继续验证/当前不路由'))) + '</span><small>' + escHtml(row.profit_gate_reason || row.recommendation_reason || '-') + '</small>' + (evidenceBlocks ? '<small>证据账本阻断：' + escHtml(evidenceBlocks) + '</small>' : '') + '<small>行情路由：' + escHtml(row.market_route_reason || '-') + '</small></td><td class="sc-wrap">' + escHtml((row.recovery_conditions || []).join('；') || '-') + '</td><td>' + strategyGovernanceActions(row, 'STRATEGY') + '</td></tr>';
        });
        if (!strategies.length) h += '<tr><td colspan="16" class="sc-empty-cell">' + (governanceDeferred ? '治理数据库门禁未完成，已登记策略暂不进入规范展示' : '尚无已注册策略') + '</td></tr>';
        h += '</tbody></table></div>';

        h += '<div class="sc-section-title"><span>冠军 / 挑战者策略工厂</span><small>策略公式不在原版本上热改；客户端产物先冻结，服务器重算结构与哈希，复核通过最多晋级为无资金影子版本，不能证明权威行情来源</small></div><div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>提交时间</th><th>策略</th><th>冠军 → 挑战者</th><th>状态</th><th>客户端声明样本外门槛</th><th>操作</th></tr></thead><tbody>';
        challengers.forEach(function (row) {
            var validation = row.latest_validation || {};
            var submission = row.evidence_submission || {};
            var metrics = ((validation.gate_validation || {}).metrics) || submission.metrics || {};
            var actions = '-';
            if (canAdmin && row.status === 'VALIDATING') actions = '<button class="sc-btn" onclick="window._strategyChallengerEvidenceSubmit(\'' + escAttr(row.challenger_id) + '\')">提交可重算产物</button>';
            if (canReview && row.status === 'REVIEW_PENDING') actions = '<button class="sc-btn" onclick="window._strategyChallengerReview(\'' + escAttr(row.challenger_id) + '\')">独立复核</button>';
            if (canAdmin && row.status === 'READY') actions = '<button class="sc-btn primary" onclick="window._strategyChallengerPromote(\'' + escAttr(row.challenger_id) + '\')">晋级为新影子版本</button>';
            h += '<tr><td>' + escHtml(row.submitted_at || '-') + '<small>' + escHtml(String(row.challenger_id || '').slice(0, 12)) + '</small></td><td>' + escHtml(row.strategy_key || '-') + '</td><td>' + escHtml(row.parent_version || '-') + ' → ' + escHtml(row.proposed_version || '-') + '</td><td><span class="sc-gate-result ' + (row.status === 'READY' || row.status === 'PROMOTED' ? 'pass' : (row.status === 'REJECTED' ? 'fail' : 'pending')) + '">' + escHtml(row.status_label || row.status || '-') + '</span></td><td>PF ' + strategyGovernanceMetric(metrics.profit_factor, '', 2) + ' · 盈亏比 ' + strategyGovernanceMetric(metrics.payoff_ratio, '', 2) + '<small>净期望 ' + strategyGovernanceMetric(metrics.net_expectancy_pct, '%', 3) + ' · 成本压力 ' + strategyGovernanceMetric(metrics.cost_stress_expectancy_pct, '%', 3) + ' · WF ' + strategyGovernanceMetric(metrics.positive_segments, '', 0) + '/' + strategyGovernanceMetric(metrics.walk_forward_segments, '', 0) + '</small><small>产物 ' + escHtml(String(submission.artifact_hash || '-').slice(0, 12)) + ' · 提交内容 ' + escHtml(String(submission.source_dataset_hash || '-').slice(0, 12)) + '（来源未认证）</small></td><td>' + actions + '</td></tr>';
        });
        if (!challengers.length) h += '<tr><td colspan="6" class="sc-empty-cell">尚无挑战者。已有策略的改进版本应先登记为挑战者，不直接覆盖冠军。</td></tr>';
        h += '</tbody></table></div>';

        h += '<div class="sc-section-title"><span>组合策略竞技场</span><small>组合指标按冻结成员权重和已验证成员事实链重建；不生成独立组合现金事实，逐笔指标不参与正式资金排名</small></div>';
        h += strategyGovernancePaginationHtml(rankingPages.combination, 'COMBINATION');
        h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>排名/赛道</th><th>组合</th><th>中文状态</th><th>组合分</th><th>20/60/120窗口证据</th><th>成员与权重</th><th>盈利日占比（资金口径）</th><th>日均净收益（资金口径）</th><th>日频盈亏比</th><th>日频PF</th><th>回撤</th><th>相关性/个股重叠/行业</th><th>准入</th><th>治理操作</th></tr></thead><tbody>';
        var combinationAllocations = {};
        allocations.forEach(function (item) { if (item.target_type === 'COMBINATION') combinationAllocations[item.target_key] = item; });
        combinations.forEach(function (row) {
            var m = (row.metrics || {})['60'] || row.primary_metrics || {};
            var members = (row.member_details || []).map(function (item) { return item.strategy_name + ' ' + Number(item.weight * 100).toFixed(0) + '%'; }).join('、');
            var sleeveDetails = strategyMemberSleevesSummary(combinationAllocations[row.combination_key] || row);
            var officialCombinationScore = row.has_independent_evidence === true ? row.ranking_score_display : null;
            var officialCombinationRank = row.has_independent_evidence === true ? row.independent_evidence_rank : null;
            var combinationRankHtml = officialCombinationRank == null ? '<strong>未入榜</strong>' : '<strong>#' + officialCombinationRank + '</strong>';
            var combinationScoreHtml = officialCombinationScore == null ? '<strong>未入榜</strong>' : '<strong>' + strategyGovernanceMetric(officialCombinationScore, '', 1) + '</strong>';
            if (officialCombinationScore == null) combinationScoreHtml += '<small>成员参考 ' + strategyGovernanceMetric(row.provisional_member_reference_score, '', 1) + '，不参与正式排名</small>';
            h += '<tr><td>' + combinationRankHtml + '<small>' + escHtml(row.lane || '-') + '</small></td><td><strong>' + escHtml(row.combination_name || row.combination_key) + '</strong><small>' + escHtml(row.current_version || '-') + '</small></td><td><span class="sc-life ' + strategyLifecycleTone(row.current_status) + '">' + escHtml(strategyLifecycleLabel(row.current_status)) + '</span><small>' + escHtml(row.status_reason || '-') + '</small></td><td>' + combinationScoreHtml + '</td><td class="sc-window-cell">' + strategyGovernanceWindowSummary(row) + strategyFundingDetailButton(row, 'COMBINATION') + '</td><td class="sc-wrap">' + escHtml(members || '-') + sleeveDetails + strategyCombinationRecipeSummary(row) + '</td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'win_rate_pct'), '%', 1) + '</td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'net_expectancy_pct'), '%', 3) + '</td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'payoff_ratio'), '', 2) + '</td><td>' + strategyGovernanceMetric(strategyGovernanceMetricValue(row, m, 'profit_factor'), '', 2) + '</td><td>' + strategyGovernanceMetric(m.max_drawdown_pct, '%', 2) + '<small>' + (m.drawdown_basis === 'internal_version_bound_portfolio_equity' ? '成员事实链复算净值' : '交易序列诊断') + '</small></td><td class="sc-wrap">' + strategyCombinationConstraintSummary(row) + '</td><td><span class="sc-gate-result ' + (row.paper_allocation_eligible ? 'pass' : 'fail') + '">' + (row.paper_allocation_eligible ? '可获模拟资金' : '继续验证/当前不路由') + '</span><small>' + escHtml(row.gate_reason || '-') + '</small><small>行情路由：' + escHtml(row.market_route_reason || '-') + '</small><small>恢复条件：' + escHtml((row.recovery_conditions || []).join('；') || '-') + '</small></td><td>' + strategyGovernanceActions(row, 'COMBINATION') + '</td></tr>';
        });
        if (!combinations.length) h += '<tr><td colspan="14" class="sc-empty-cell">' + (governanceDeferred ? '治理数据库门禁未完成，已登记组合暂不进入规范展示' : '尚无组合策略') + '</td></tr>';
        h += '</tbody></table></div>';

        h += '<div class="sc-section-title"><span>今日分层票池</span><small>观察池用于展示可审计研究候选且允许为空；确认池等待价格与数据；模拟可交易池允许为空且不代表真实下单 · ' + escHtml(tradingGateLabel) + ' · 风险上限 ' + strategyGovernanceMetric(marketRiskCap, '%', 1) + '</small></div>';
        h += '<div class="sc-pool-tabs"><button class="active" onclick="window._strategyPoolShow(\'observation\',this)">观察池 ' + ((pools.observation || []).length) + '</button><button onclick="window._strategyPoolShow(\'confirmation\',this)">等待确认池 ' + ((pools.confirmation || []).length) + '</button><button onclick="window._strategyPoolShow(\'tradable\',this)">模拟可交易池 ' + ((pools.tradable || []).length) + '</button></div><div id="scGovernancePool"></div>';
        if (!poolTruth.ready && rawResearchRows.length) {
            h += '<details class="sc-research-diagnostics" data-research-only-pool><summary><strong>隔离的研究只读候选 ' + rawResearchRows.length + ' 条</strong><span>来源批次未进入当前正式票池；这里只展示股票、来源层级、证据日期与阻断原因</span></summary><div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>来源层级</th><th>股票</th><th>策略</th><th>证据日期</th><th>阻断原因</th><th>权限</th></tr></thead><tbody>';
            rawResearchRows.forEach(function (entry) {
                var row = entry.row || {}, evidence = row.evidence || {};
                var levelLabel = entry.level === 'tradable' ? '原模拟池记录' : entry.level === 'confirmation' ? '原确认池记录' : '观察池记录';
                h += '<tr><td>' + escHtml(levelLabel) + '</td><td><strong>' + escHtml(row.stock_code || '-') + '</strong><small>' + escHtml(row.stock_name || '-') + '</small></td><td>' + escHtml(row.dominant_strategy_name || row.dominant_strategy || '-') + '</td><td>' + escHtml(evidence.data_date || poolTruth.resultDate || '-') + '</td><td class="sc-wrap">' + escHtml(poolTruth.reason + (row.reason ? '；' + row.reason : '')) + '</td><td><span class="sc-gate-result pending">RESEARCH_ONLY</span><small>不可执行、不可分配资金</small></td></tr>';
            });
            h += '</tbody></table></div></details>';
        }

        h += '<div class="sc-section-title"><span>模拟资金分配</span><small>只分配给通过盈利硬门槛且适配当前市场的策略或组合；组合逐成员展示基础、生效及留现金袖套</small></div><div class="sc-allocation-grid">';
        allocations.forEach(function (row) { h += '<div class="' + (row.target_type === 'CASH' ? 'cash' : 'risk') + '"><strong>' + escHtml(row.name || row.target_key) + '</strong><b>' + strategyGovernanceMetric(row.simulated_weight_pct, '%', 1) + '</b><span>' + escHtml(row.reason || '-') + '</span>' + strategyMemberSleevesSummary(row) + '</div>'; });
        h += '</div>';
        h += strategyPaperExecutionPlanHtml(poolTruth.ready ? governance : Object.assign({}, governance, {is_canonical:false,result_mode:'CANONICAL_UNAVAILABLE',input_reason:poolTruth.reason}));

        var adapterRunReceipts = Array.isArray(history.adapter_run_receipts) ? history.adapter_run_receipts : [];
        h += '<div class="sc-section-title"><span>动态适配器运行回执</span><small>零候选也单独留档；运行号和完成时间只用于审计，不改变同日权威决策哈希</small></div>';
        h += strategyGovernanceHistoryPaginationHtml(historyPages.adapter_run_receipts, 'adapter_run_receipts');
        h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>交易日/完成时间</th><th>策略/版本</th><th>适配器</th><th>结果</th><th>候选数</th><th>输入/输出哈希</th><th>回执哈希</th></tr></thead><tbody>';
        adapterRunReceipts.forEach(function (row) {
            var ok = String(row.status || '').toUpperCase() === 'COMPLETED' && row.hash_valid !== false;
            h += '<tr><td>' + escHtml(row.trade_date || '-') + '<small>' + escHtml(row.completed_at || '-') + '</small></td><td>' + escHtml(row.strategy_key || '-') + '<small>' + escHtml(row.strategy_version || '-') + '</small></td><td>' + escHtml(row.adapter_key || '-') + '<small>' + escHtml(row.adapter_version || '-') + '</small></td><td><span class="sc-gate-result ' + (ok ? 'pass' : 'fail') + '">' + (ok ? '运行完成' : '运行无效/失败') + '</span><small>' + escHtml(row.reason || '-') + '</small></td><td>' + Number(row.candidate_count || 0) + '<small>' + (Number(row.candidate_count || 0) === 0 ? '零候选已留痕' : '候选身份已绑定') + '</small></td><td>' + escHtml(String(row.input_hash || '-').slice(0, 12)) + '<small>' + escHtml(String(row.output_hash || '-').slice(0, 12)) + '</small></td><td>' + escHtml(String(row.receipt_hash || '-').slice(0, 16)) + '<small>' + escHtml(String(row.run_uid || '-').slice(0, 12)) + '</small></td></tr>';
        });
        if (!adapterRunReceipts.length) h += '<tr><td colspan="7" class="sc-empty-cell">尚无已持久化的动态适配器运行回执；动态策略保持影子观察</td></tr>';
        h += '</tbody></table></div>';

        h += '<div class="sc-section-title"><span>外部研究声明复核台账</span><small>记录客户端提交、版本绑定、提交内容哈希和双人结构复核；该哈希不是权威行情源认证，不授予资金</small></div>';
        h += strategyGovernanceHistoryPaginationHtml(historyPages.metric_evidence, 'metric_evidence');
        h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>提交时间</th><th>对象/版本</th><th>窗口/截止日</th><th>复核状态</th><th>样本/覆盖</th><th>净期望/盈亏比</th><th>利润因子/回撤</th><th>协议与高水位</th><th>提交/复核人</th><th>产物/提交内容哈希</th><th>复核操作</th></tr></thead><tbody>';
        metricEvidence.forEach(function (row) {
            var m = row.metrics || {};
            var entityLabel = row.entity_type === 'COMBINATION' ? '策略组合' : '单策略';
            var reviewTone = row.verification_status === 'CONFIRMED' ? 'pass' : (row.verification_status === 'REJECTED' ? 'fail' : 'pending');
            var detailLink = '<a class="sc-mini-btn" target="_blank" rel="noopener" href="/api/strategy-center/metrics/' + encodeURIComponent(row.evidence_id || '') + '">查看产物</a>';
            var reviewActions = row.verification_status === 'PENDING' ? (canReview ? detailLink + '<button class="sc-mini-btn" onclick="window._strategyMetricReview(\'' + escAttr(row.evidence_id || '') + '\',\'CONFIRM\')">确认</button><button class="sc-mini-btn" onclick="window._strategyMetricReview(\'' + escAttr(row.evidence_id || '') + '\',\'REJECT\')">驳回</button>' : detailLink + '<span class="sc-muted">请由证据复核员登录处理</span>') : detailLink + '<span class="sc-muted">复核已完成</span>';
            h += '<tr><td>' + escHtml(row.created_at || '-') + '<small>' + escHtml(String(row.evidence_id || '').slice(0, 12)) + '</small></td><td><strong>' + escHtml(row.strategy_key || '-') + '</strong><small>' + entityLabel + ' · ' + escHtml(row.strategy_version || '-') + '</small></td><td>' + Number(row.window_days || 0) + '日<small>' + escHtml(row.as_of_date || '-') + '</small></td><td><span class="sc-gate-result ' + reviewTone + '">' + escHtml(row.verification_status_label || '未知复核状态') + '</span><small>' + (row.independent_review ? '提交与复核已分离' : '尚未完成独立复核') + '</small><small>客户端声明来源未认证 · 不授予资金</small></td><td>' + strategyGovernanceMetric(m.completed_trades, '', 0) + '笔<small>' + strategyGovernanceMetric(m.coverage_days, '', 0) + '日</small></td><td>' + strategyGovernanceMetric(m.net_expectancy_pct, '%', 3) + '<small>盈亏比 ' + strategyGovernanceMetric(m.payoff_ratio, '', 2) + '</small></td><td>' + strategyGovernanceMetric(m.profit_factor, '', 2) + '<small>回撤 ' + strategyGovernanceMetric(m.max_drawdown_pct, '%', 2) + '</small></td><td>' + escHtml(row.evidence_protocol || '-') + '<small>' + escHtml(row.evidence_revision_at || '-') + '</small></td><td>' + escHtml(row.submitted_by || '-') + '<small>' + escHtml(row.reviewed_by || '等待复核') + '</small></td><td>' + escHtml(String(row.artifact_hash || '-').slice(0, 12)) + '<small>' + escHtml(String(row.source_dataset_hash || '-').slice(0, 12)) + '（非源认证）</small></td><td>' + reviewActions + '</td></tr>';
        });
        if (!metricEvidence.length) h += '<tr><td colspan="11" class="sc-empty-cell">尚无外部研究声明；它仅用于研究版本选择，不是内部账本资金资格的必要条件</td></tr>';
        h += '</tbody></table></div>';

        h += '<div class="sc-section-title"><span>生命周期与理由记录</span><small>新增、升级、降权、暂停、恢复和淘汰全部可追溯</small></div>';
        h += strategyGovernanceHistoryPaginationHtml(historyPages.lifecycle, 'lifecycle');
        h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>时间</th><th>对象</th><th>原状态</th><th>新状态</th><th>理由</th><th>触发方式</th><th>操作人</th><th>哈希校验</th></tr></thead><tbody>';
        lifecycle.forEach(function (row) { var triggers = {AUTOMATIC_GATE:'盈利门槛自动判断',MANUAL_GOVERNANCE:'人工治理',VERSION_REGISTRATION:'版本注册'}; h += '<tr><td>' + escHtml(row.occurred_at || '-') + '</td><td>' + escHtml(row.entity_key || '-') + '<small>' + escHtml(row.entity_version || '-') + '</small></td><td>' + escHtml(strategyLifecycleLabel(row.previous_status)) + '</td><td>' + escHtml(strategyLifecycleLabel(row.next_status)) + '</td><td class="sc-wrap">' + escHtml(row.reason || '-') + '</td><td>' + escHtml(triggers[row.trigger_type] || '系统治理') + '</td><td>' + escHtml(row.operator_name || '-') + '</td><td><span class="sc-gate-result ' + (row.hash_valid === true ? 'pass' : 'fail') + '">' + (row.hash_valid === true ? '哈希有效' : '待核验') + '</span></td></tr>'; });
        if (!lifecycle.length) h += '<tr><td colspan="8" class="sc-empty-cell">尚无状态变化；初始注册会保持影子观察</td></tr>';
        h += '</tbody></table></div>';

        h += '<div class="sc-section-title"><span>每日治理运行记录</span><small>同一数据日允许可追溯修订；页面明确区分当前生效与已被替代</small></div>';
        h += strategyGovernanceHistoryPaginationHtml(historyPages.runs, 'runs');
        h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>数据日/修订</th><th>生效状态</th><th>来源/输入</th><th>构建</th><th>决策哈希</th><th>策略/组合</th><th>票池</th><th>完成时间</th></tr></thead><tbody>';
        runs.forEach(function (row) { var canonical = Number(row.is_canonical || 0) === 1; h += '<tr><td>' + escHtml(row.trade_date || '-') + '<small>修订号 ' + Number(row.run_revision || 1) + ' · ' + escHtml(String(row.run_uid || '-').slice(0, 12)) + '</small></td><td><span class="sc-gate-result ' + (canonical ? 'pass' : 'pending') + '">' + (canonical ? '当前生效' : '已被替代') + '</span><small>' + (row.supersedes_run_uid ? '替代 ' + escHtml(String(row.supersedes_run_uid).slice(0, 12)) : '首个修订') + '</small></td><td>' + escHtml(row.source_status || '-') + '<small>' + (row.input_ready ? '输入已校验' : '输入未就绪') + ' · ' + escHtml(String(row.input_hash || '-').slice(0, 12)) + '</small></td><td>' + escHtml(String(row.build_commit_sha || '-').slice(0, 12)) + '</td><td>' + escHtml(String(row.decision_hash || '-').slice(0, 16)) + '</td><td>' + Number(row.strategy_count || 0) + ' / ' + Number(row.combination_count || 0) + '</td><td>观察 ' + Number(row.observation_count || 0) + '，可交易 ' + Number(row.tradable_count || 0) + '</td><td>' + escHtml(row.finished_at || row.created_at || '-') + '</td></tr>'; });
        if (!runs.length) h += '<tr><td colspan="8" class="sc-empty-cell">尚无已完成的每日治理记录</td></tr>';
        h += '</tbody></table></div>';

        h += '<div class="sc-section-title"><span>治理操作审计</span><small>版本注册、证据新增、状态变化和启停操作均保留理由</small></div>';
        h += strategyGovernanceHistoryPaginationHtml(historyPages.audit, 'audit');
        h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>时间</th><th>对象</th><th>动作</th><th>理由</th><th>操作人</th><th>哈希校验</th></tr></thead><tbody>';
        audits.forEach(function (row) { var actions = {REGISTER_VERSION:'注册版本',ADD_METRIC_EVIDENCE:'新增证据',CONFIRM_METRIC_EVIDENCE:'确认独立证据',REJECT_METRIC_EVIDENCE:'驳回独立证据',LIFECYCLE_TRANSITION:'生命周期变更',RUN_GOVERNANCE:'完成每日治理'}; var entityTypes = {STRATEGY:'单策略',COMBINATION:'策略组合',SYSTEM:'系统闭环'}; h += '<tr><td>' + escHtml(row.created_at || '-') + '</td><td>' + escHtml(row.entity_key || '-') + '<small>' + escHtml(entityTypes[row.entity_type] || '治理对象') + '</small></td><td>' + escHtml(actions[row.action] || '系统治理动作') + '</td><td class="sc-wrap">' + escHtml(row.reason || '-') + '</td><td>' + escHtml(row.operator_name || '-') + '</td><td><span class="sc-gate-result ' + (row.hash_valid === true ? 'pass' : 'fail') + '">' + (row.hash_valid === true ? '哈希有效' : '待核验') + '</span></td></tr>'; });
        if (!audits.length) h += '<tr><td colspan="6" class="sc-empty-cell">尚无治理操作审计</td></tr>';
        h += '</tbody></table></div></section>';
        return h;
    }

    function loadStrategyCenterPage(d, container) {
        var target = d || currentDateValue();
        if (target === currentDateValue()) target = recommendationDateValue();
        setStatus('正在加载策略中心...');
        Promise.all([
            fetchRawJsonWithTimeout('/api/strategy-center/overview?trade_date=' + encodeURIComponent(target) + '&limit=250', 45000).catch(function (err) { return {status:'degraded', trade_date:target, strategies:[], candidates:[], conflicts:[], summary:{}, error:'研究输入诊断接口暂时不可用：' + (err && err.message ? err.message : '请求失败')}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance?trade_date=' + encodeURIComponent(target), 60000).catch(function (err) { return {status:'degraded', result_mode:'CANONICAL_UNAVAILABLE', is_canonical:false, input_ready:false, input_reason:'治理接口暂时不可用', strategies:[], combinations:[], pools:{observation:[],confirmation:[],tradable:[]}, allocations:[{target_type:'CASH',target_key:'cash',name:'现金',simulated_weight_pct:100,reason:'治理接口不可用，保持现金'}], error:err && err.message ? err.message : '请求失败'}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance/history?limit=50', 30000).catch(function () { return {metric_evidence:[], lifecycle_events:[], audit_events:[], runs:[]}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance/history/lifecycle?limit=50', 30000).catch(function () { return {status:'degraded'}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance/history/audit?limit=50', 30000).catch(function () { return {status:'degraded'}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance/history/metric-evidence?limit=50', 30000).catch(function () { return {status:'degraded'}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance/history/adapter-run-receipts?limit=50', 30000).catch(function () { return {status:'degraded'}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance/history/runs?limit=50', 30000).catch(function () { return {status:'degraded'}; }),
            fetchRawJsonWithTimeout('/api/auth/status', 15000).catch(function () { return {authenticated:false}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/governance/adapter-capabilities', 30000).catch(function () { return {adapters:[]}; }),
            fetchRawJsonWithTimeout('/api/strategy-center/challengers', 30000).catch(function () { return {challengers:[]}; })
        ])
            .then(function (payloads) {
                var data = payloads[0] || {};
                var governance = payloads[1] || {};
                var history = payloads[2] || {};
                var lifecyclePagePayload = payloads[3] || {};
                var auditPagePayload = payloads[4] || {};
                var metricPagePayload = payloads[5] || {};
                var adapterPagePayload = payloads[6] || {};
                var runsPagePayload = payloads[7] || {};
                window._strategyCenterAuth = payloads[8] || {};
                var capabilityPayload = payloads[9] || {};
                var challengerPayload = payloads[10] || {};
                history.history_pages = history.history_pages || {};
                if (lifecyclePagePayload.status === 'ok' && lifecyclePagePayload.page && Array.isArray(lifecyclePagePayload.page.rows)) {
                    history.lifecycle_events = lifecyclePagePayload.page.rows;
                    history.history_pages.lifecycle = lifecyclePagePayload.page;
                }
                if (auditPagePayload.status === 'ok' && auditPagePayload.page && Array.isArray(auditPagePayload.page.rows)) {
                    history.audit_events = auditPagePayload.page.rows;
                    history.history_pages.audit = auditPagePayload.page;
                }
                if (metricPagePayload.status === 'ok' && metricPagePayload.page && Array.isArray(metricPagePayload.page.rows)) {
                    history.metric_evidence = metricPagePayload.page.rows;
                    history.history_pages.metric_evidence = metricPagePayload.page;
                }
                if (adapterPagePayload.status === 'ok' && adapterPagePayload.page && Array.isArray(adapterPagePayload.page.rows)) {
                    history.adapter_run_receipts = adapterPagePayload.page.rows;
                    history.history_pages.adapter_run_receipts = adapterPagePayload.page;
                }
                if (runsPagePayload.status === 'ok' && runsPagePayload.page && Array.isArray(runsPagePayload.page.rows)) {
                    history.runs = runsPagePayload.page.rows;
                    history.history_pages.runs = runsPagePayload.page;
                }
                window._strategyGovernanceHistoryState = {
                    lifecycle:{currentCursor:'', stack:[], pageNo:1},
                    audit:{currentCursor:'', stack:[], pageNo:1},
                    metric_evidence:{currentCursor:'', stack:[], pageNo:1},
                    adapter_run_receipts:{currentCursor:'', stack:[], pageNo:1},
                    runs:{currentCursor:'', stack:[], pageNo:1}
                };
                history.challengers = Array.isArray(challengerPayload.challengers) ? challengerPayload.challengers : [];
                if (Array.isArray(capabilityPayload.adapters)) governance.adapter_capabilities = capabilityPayload.adapters;
                else if (!Array.isArray(governance.adapter_capabilities)) governance.adapter_capabilities = [];
                governance.adapter_capability_summary = capabilityPayload;
                window._strategyCenterData = data || {};
                window._strategyCenterGovernance = governance;
                window._strategyCenterGovernanceHistory = history;
                window._strategyCenterDate = target;
                renderStrategyCenter(container, data || {}, governance, history);
                var governanceDegraded = governance.status === 'degraded' || governance.input_ready === false;
                var diagnosticsDegraded = data.status === 'degraded' || data.is_stale === true;
                setStatus(governanceDegraded ? '规范策略治理数据降级' : (diagnosticsDegraded ? '规范策略治理已就绪；研究输入诊断降级' : '动态策略治理已就绪'), governanceDegraded);
            })
            .catch(function (err) {
                container.innerHTML = '<div class="sc-empty"><strong>策略中心暂时无法加载</strong><p>' + escHtml(err && err.message ? err.message : '请求失败') + '</p><button class="sc-btn" onclick="window._strategyCenterReload()">重新加载</button></div>';
                setStatus('策略中心加载失败', true);
            });
    }

    function renderStrategyCenter(container, data, governance, history) {
        window._strategyCenterContainer = container;
        data = data || {};
        governance = governance || {};
        history = history || {};
        var state = data.market_state || {};
        var stateKey = state.key || 'unknown';
        var stateColor = state.color || '#64748b';
        var summary = data.summary || {};
        var strategies = Array.isArray(data.strategies) ? data.strategies : [];
        var candidates = Array.isArray(data.candidates) ? data.candidates : [];
        var conflicts = Array.isArray(data.conflicts) ? data.conflicts : [];
        var canAdmin = String(((((window._strategyCenterAuth || {}).user) || {}).role) || '').toUpperCase() === 'ADMIN' && governance.activation_enabled !== false;
        var strategyMap = {};
        strategies.forEach(function (item) { strategyMap[item.key] = item; });
        var categories = [];
        strategies.forEach(function (item) { if (item.category && categories.indexOf(item.category) < 0) categories.push(item.category); });
        var h = '<div class="sc-page">';
        h += strategyGovernanceHtml(governance, history);
        h += '<details id="scResearchInputDiagnostics" class="sc-research-diagnostics"><summary><strong>研究输入诊断（非规范结果，默认收起）</strong><span>仅用于排查底层适配器输入；不参与默认排名、规范票池或资金分配展示</span></summary><div class="sc-research-diagnostics-body">';
        h += '<div class="sc-hero" style="border-left-color:' + escAttr(stateColor) + '">';
        h += '<div><div class="sc-kicker">研究输入诊断 · 非规范结果</div><h2>输入市场环境 · ' + escHtml(state.name || strategyCenterLabel(stateKey)) + '</h2>';
        h += '<p>' + escHtml(state.description || '当前状态仅用于策略适配和研究提示') + '</p></div>';
        h += '<div class="sc-hero-meta"><span class="sc-pill" style="background:' + escAttr(stateColor) + ';color:#fff">置信度 ' + (state.confidence != null ? fmt(state.confidence, 0) + '%' : '暂无') + '</span>';
        h += '<span>数据日 ' + escHtml(data.data_date || data.trade_date || '-') + '</span><span>' + (data.is_stale ? '数据需复核' : '数据新鲜') + '</span></div></div>';
        var gate = data.global_gate || {};
        var gateClass = String(gate.status || '').indexOf('BLOCK') >= 0 || gate.status === 'DATA_NOT_READY' ? 'risk' : (gate.status === 'ALLOW_NEW_BUY' ? 'buy' : 'watch');
        h += '<div class="sc-gate ' + gateClass + '"><strong>输入层门禁诊断（不生效）：' + escHtml(strategyTradingGateLabel(gate.status)) + '</strong><span>' + escHtml(gate.reason || '暂无门禁说明') + '</span><em>仅用于研究输入排查，不代表规范治理结论</em></div>';
        var reference = data.reference_pool || {};
        if (reference.enabled) {
            var limits = reference.position_limits || {};
            var referenceGate = reference.global_gate || {};
            h += '<div class="sc-reference-note"><strong>日期化研究池 · 已接生产库交叉验证</strong><span>来源：' + escHtml(reference.source || 'dated_reference_pool') + '；行情日：' + escHtml(reference.reference_as_of_date || '-') + '；盘前复核：' + escHtml(reference.recheck_after || '08:45') + ' 后</span><span>仓位上限：单只 ' + escHtml(limits.single_pct == null ? '—' : limits.single_pct + '%') + '，合计 ' + escHtml(limits.aggregate_pct == null ? '—' : limits.aggregate_pct + '%') + '</span><em>' + escHtml(referenceGate.invalidation_condition || '指数与板块条件未满足前不生成确定性动作') + '</em></div>';
        }
        if (data.error) h += '<div class="sc-warning">' + escHtml(data.error) + '</div>';
        h += '<div class="sc-stats">';
        h += '<div><strong>' + (summary.strategy_count != null ? summary.strategy_count : strategies.length) + '</strong><span>输入适配器数</span></div>';
        h += '<div><strong>' + (summary.enabled_count != null ? summary.enabled_count : '-') + '</strong><span>配置启用</span></div>';
        h += '<div><strong>' + (summary.candidate_count != null ? summary.candidate_count : candidates.length) + '</strong><span>原始研究输入</span></div>';
        h += '<div><strong>' + (summary.conflict_count != null ? summary.conflict_count : conflicts.length) + '</strong><span>输入冲突</span></div>';
        h += '<div><strong>' + (summary.buy_count != null ? summary.buy_count : '-') + '</strong><span>偏多观察</span></div></div>';
        h += '<div class="sc-section-title"><span>研究输入适配器诊断</span><small>底层公式和原始裁决仅作为治理输入，不是规范排名</small></div>';
        h += '<div class="sc-section-title"><span>输入适配器配置（不代表治理状态）</span><small>启停只影响后续研究输入；正式生命周期以 canonical 治理结果为准</small></div>';
        h += '<div class="sc-strategy-grid">';
        strategies.forEach(function (item) {
            var tone = item.effective_weight != null && Number(item.effective_weight) > 0 ? 'active' : 'muted';
            h += '<article class="sc-strategy-card ' + tone + '">';
            h += '<div class="sc-card-head"><div><h3>' + escHtml(item.name || item.key) + '</h3><span>' + escHtml(item.category || '-') + ' · ' + escHtml(item.description || '') + '</span></div>';
            h += (canAdmin ? '<label class="sc-switch"><input type="checkbox" ' + (item.enabled ? 'checked' : '') + ' onchange="window._strategyCenterToggle(\'' + escAttr(item.key) + '\', this.checked)"><i></i></label>' : '<span class="sc-muted">' + (item.enabled ? '已启用' : '已停用') + '</span>') + '</div>';
            h += '<div class="sc-weight"><strong>' + (item.effective_weight != null ? Number(item.effective_weight).toFixed(2) : '—') + '</strong><span>有效权重</span><em>' + escHtml(item.weight_reason || '暂无权重说明') + '</em></div>';
            h += '<div class="sc-metric-grid"><div><strong>' + (item.today_signal_count != null ? item.today_signal_count : '-') + '</strong><span>今日信号</span></div><div><strong>' + strategyCenterMoney(item.return_pct) + '</strong><span>收益</span></div><div><strong>' + strategyCenterMoney(item.max_drawdown_pct) + '</strong><span>回撤</span></div><div><strong>' + (item.win_rate_pct == null ? '暂无样本' : fmt(item.win_rate_pct, 1) + '%') + '</strong><span>胜率</span></div><div><strong>' + (item.profit_factor == null ? '暂无样本' : fmt(item.profit_factor, 2)) + '</strong><span>利润因子</span></div><div><strong>' + (item.sample_count || 0) + '</strong><span>样本数</span></div></div>';
            var modelStatus = item.model_status === 'baseline_adapter' ? '透明基线（非独立训练）' : (item.model_status === 'historical_review' ? '历史复盘基线' : '生产适配');
            h += '<div class="sc-card-foot" title="' + escAttr(item.metric_note || '') + '"><span>' + escHtml(item.metric_source || '暂无复盘样本') + ' · ' + escHtml(modelStatus) + '</span><span>' + escHtml(item.model_version || ('v' + (item.version || 1))) + '</span></div></article>';
        });
        if (!strategies.length) h += '<div class="sc-empty">暂无策略配置</div>';
        h += '</div>';
        h += '<div class="sc-section-title"><span>未治理研究输入明细（非规范）</span><small>仅用于诊断策略分类、横向比较和原始冲突</small></div>';
        h += '<div class="sc-filter-row"><select id="scStrategyFilter" onchange="window._strategyCenterFilter()"><option value="">全部策略</option>';
        strategies.forEach(function (item) { h += '<option value="' + escAttr(item.key) + '">' + escHtml(item.name || item.key) + '</option>'; });
        h += '</select><select id="scCategoryFilter" onchange="window._strategyCenterFilter()"><option value="">全部分类</option>';
        categories.forEach(function (item) { h += '<option value="' + escAttr(item) + '">' + escHtml(item) + '</option>'; });
        h += '</select><select id="scStatusFilter" onchange="window._strategyCenterFilter()"><option value="">全部状态</option><option value="READY">确认前候选</option><option value="WATCH">观察</option><option value="CONFLICT">信号冲突</option><option value="BLOCKED">已阻断</option><option value="SELL_ALERT">风险提醒</option></select>';
        h += '<select id="scDirectionFilter" onchange="window._strategyCenterFilter()"><option value="">全部方向</option><option value="BUY">偏多</option><option value="HOLD">中性</option><option value="SELL">偏空</option></select><button class="sc-btn" onclick="window._strategyCenterReload()">刷新数据</button></div>';
        h += '<div id="scCandidateTable"></div>';
        h += '<div class="sc-section-title"><span>研究输入冲突诊断</span><small>仅解释底层输入如何产生冲突，不代表 canonical 治理裁决</small></div><div id="scConflictList">';
        if (conflicts.length) {
            conflicts.forEach(function (item) {
                var decision = item.decision || {};
                h += '<details class="sc-conflict"><summary>' + escHtml(item.stock_code + ' ' + (item.stock_name || '')) + ' · ' + escHtml(strategyCenterStatus(decision.final_status)) + '</summary><div><p>' + escHtml(decision.conflict_summary || '-') + '</p><p>偏多 ' + fmt(decision.buy_score, 1) + ' / 偏空 ' + fmt(decision.sell_score, 1) + ' / 中性 ' + fmt(decision.hold_score, 1) + '</p><pre>' + escHtml(strategyCenterJson(item.signals || [])) + '</pre></div></details>';
            });
        } else h += '<div class="sc-empty compact">当前没有需要展开的冲突记录</div>';
        h += '</div><div class="sc-disclaimer">' + escHtml(data.disclaimer || '仅用于研究候选和风险提示；未经明确确认不会执行任何交易。') + '</div></div></details></div>';
        container.innerHTML = h;
        window._renderStrategyCenterCandidates(candidates, strategyMap);
        window._strategyPoolShow('observation');
    }

    window._renderStrategyCenterCandidates = function (candidates, strategyMap) {
        var target = el('scCandidateTable');
        if (!target) return;
        var strategyFilter = (el('scStrategyFilter') || {}).value || '';
        var categoryFilter = (el('scCategoryFilter') || {}).value || '';
        var statusFilter = (el('scStatusFilter') || {}).value || '';
        var directionFilter = (el('scDirectionFilter') || {}).value || '';
        var rows = (candidates || []).filter(function (row) {
            var keys = row.strategies || [];
            if (strategyFilter && keys.indexOf(strategyFilter) < 0) return false;
            if (categoryFilter && !keys.some(function (key) { return strategyMap[key] && strategyMap[key].category === categoryFilter; })) return false;
            if (statusFilter && row.final_status !== statusFilter) return false;
            if (directionFilter && row.final_direction !== directionFilter) return false;
            return true;
        });
        var html = '<div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>优先级</th><th>股票</th><th>入选策略</th><th>最终信号</th><th>置信度</th><th>观察区间</th><th>完整触发条件</th><th>止损</th><th>止盈</th><th>禁止追高</th><th>风险</th><th>裁决</th></tr></thead><tbody>';
        rows.forEach(function (row) {
            var tone = strategyCenterTone(row.final_status);
            var strategyNames = (row.strategies || []).map(function (key) { return strategyMap[key] ? strategyMap[key].name : key; }).join('、');
            var trigger = strategyCenterJson(row.trigger_conditions);
            html += '<tr><td><span class="sc-priority ' + tone + '">' + escHtml(row.priority || '-') + '</span></td><td><strong>' + escHtml(row.stock_code || '-') + '</strong><br><small>' + escHtml(row.stock_name || '-') + '</small></td><td class="sc-text-cell">' + escHtml(strategyNames || '-') + '</td><td><span class="sc-status ' + tone + '">' + escHtml(strategyCenterStatus(row.final_status)) + '</span><br><small>' + escHtml(row.final_direction || '-') + '</small></td><td>' + (row.model_confidence == null ? '暂无' : fmt(row.model_confidence, 0) + '%') + '</td><td>' + (row.entry_low == null && row.entry_high == null ? '-' : fmtPrice(row.entry_low) + '–' + fmtPrice(row.entry_high)) + '</td><td class="sc-condition" title="' + escAttr(trigger) + '">' + escHtml(row.today_signal || trigger || '-') + '</td><td>' + fmtPrice(row.stop_loss) + '</td><td>' + fmtPrice(row.take_profit_1) + (row.take_profit_2 != null ? '–' + fmtPrice(row.take_profit_2) : '') + '</td><td>' + fmtPrice(row.no_chase_price) + '</td><td><span class="sc-risk-' + String(row.risk_level || 'LOW').toLowerCase() + '">' + escHtml(row.risk_level || '-') + '</span></td><td class="sc-text-cell">' + escHtml(row.conflict_summary || (row.blocking_reasons || []).join('；') || '-') + '</td></tr>';
        });
        if (!rows.length) html += '<tr><td colspan="12" class="sc-empty-cell">当前筛选条件下暂无候选，或数据尚未完成。</td></tr>';
        html += '</tbody></table></div><div class="sc-table-note">显示 ' + rows.length + ' / ' + (candidates || []).length + ' 条；价格、公告、集合竞价和实时资金缺失时不生成确定性动作。</div>';
        target.innerHTML = html;
    };

    window._strategyCenterFilter = function () {
        var data = window._strategyCenterData || {};
        var map = {};
        (data.strategies || []).forEach(function (item) { map[item.key] = item; });
        window._renderStrategyCenterCandidates(data.candidates || [], map);
    };

    window._strategyCenterReload = function () {
        var c = el('tab-strategy-center');
        if (c) loadStrategyCenterPage(window._strategyCenterDate || currentDateValue(), c);
    };

    window._strategyCenterToggle = function (key, enabled) {
        fetch('/api/strategy-center/strategies/' + encodeURIComponent(key) + '/toggle', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({enabled: !!enabled, reason: '策略中心页面切换'})
        }).then(function (response) { return response.json(); }).then(function (result) {
            if (result.status !== 'ok') throw new Error(result.message || result.error || '策略配置更新失败');
            window._strategyCenterReload();
        }).catch(function (err) {
            setStatus('策略启停更新失败', true);
            window.alert(err.message || '策略配置更新失败');
            window._strategyCenterReload();
        });
    };

    window._strategyPoolShow = function (level, button) {
        var governance = window._strategyCenterGovernance || {};
        var selectedPool = (governance.pools || {})[level];
        var rows = Array.isArray(selectedPool) ? selectedPool : [];
        var poolTruth = strategyGovernancePoolTruth(governance, window._strategyCenterDate || governance.trade_date, latestFormalStrategyDateValue());
        var rejectedMalformedCount = 0;
        if (!poolTruth.ready) rows = [];
        if (poolTruth.ready && level === 'tradable') {
            var verifiedRows = rows.filter(function (row) {
                var evidence = row && row.evidence || {};
                var blocking = row && Array.isArray(row.blocking_reasons) ? row.blocking_reasons : [];
                return row && row.paper_allocation_eligible === true && row.real_order_authority === false && blocking.length === 0 && String(evidence.data_date || '').slice(0, 10) === poolTruth.resultDate;
            });
            rejectedMalformedCount = rows.length - verifiedRows.length;
            rows = verifiedRows;
        }
        var target = el('scGovernancePool');
        if (!target) return;
        if (button && button.parentNode) {
            [].forEach.call(button.parentNode.querySelectorAll('button'), function (item) { item.classList.remove('active'); });
            button.classList.add('active');
        }
        var html = '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>排名</th><th>股票</th><th>行业</th><th>主策略/组合</th><th>机会分</th><th>执行分</th><th>计划风险收益比</th><th>信号状态</th><th>入选/阻断理由</th><th>证据时点</th><th>权限</th></tr></thead><tbody>';
        rows.forEach(function (row) {
            var ownerLabel = row.dominant_strategy_name || row.dominant_strategy || '-';
            if (row.allocation_target_type === 'COMBINATION') ownerLabel += ' / 组合 ' + String(row.allocation_target_key || '-');
            var allocationNote = row.allocation_backed === true ? ' · 目标 ' + strategyGovernanceMetric(row.target_weight_pct, '%', 2) : '';
            var permission = level === 'tradable' ? 'VERIFIED_PAPER / 仅模拟' : level === 'confirmation' ? 'WAIT_CONFIRM / 不可执行' : 'RESEARCH_ONLY / 研究观察';
            html += '<tr><td>#' + (row.rank || '-') + '</td><td><strong>' + escHtml(row.stock_code || '-') + '</strong><small>' + escHtml(row.stock_name || '-') + allocationNote + '</small></td><td>' + escHtml(row.industry_name || '未分类') + '</td><td>' + escHtml(ownerLabel) + '</td><td>' + strategyGovernanceMetric(row.opportunity_score, '', 1) + '</td><td>' + strategyGovernanceMetric(row.execution_score, '', 1) + '</td><td>' + strategyGovernanceMetric(row.risk_reward_ratio, '', 2) + '</td><td>' + escHtml(strategyCenterStatus(row.final_status)) + '</td><td class="sc-wrap">' + escHtml(row.reason || (row.blocking_reasons || []).join('；') || '-') + '</td><td>' + escHtml((row.evidence || {}).data_date || '-') + '</td><td><span class="sc-gate-result ' + (level === 'tradable' ? 'pass' : 'pending') + '">' + escHtml(permission) + '</span><small>真实下单固定关闭</small></td></tr>';
        });
        if (!rows.length) html += '<tr><td colspan="11" class="sc-empty-cell">' + escHtml(poolTruth.ready ? '本层票池为空。这是当前 VERIFIED COMPLETED 批次的有效结果，不会为了凑数量强制生成买入候选。' : '正式票池不可用：' + poolTruth.reason + '。旧候选已隔离到研究只读区，不能解释为当前0只。') + '</td></tr>';
        target.innerHTML = html + '</tbody></table></div><div class="sc-table-note">' + (poolTruth.ready ? '规范票池完整展示 ' + rows.length + ' 条；请求日与 canonical 日期一致。' + (rejectedMalformedCount ? '另有 ' + rejectedMalformedCount + ' 条未通过模拟池逐行校验，已拒绝展示。' : '') : '正式票池 0 条；阻断代码 ' + escHtml(poolTruth.reasonCode) + '，研究只读记录不会获得资金或执行权限。') + '</div>';
    };

    window._strategyRegistrationToggle = function () {
        var node = el('scRegistrationForm');
        if (node) node.style.display = node.style.display === 'none' ? 'block' : 'none';
    };

    window._strategyAdapterCapabilityApply = function () {
        var select = el('scRegAdapterCapability');
        var capabilities = Array.isArray(window._strategyCenterAdapterCapabilities) ? window._strategyCenterAdapterCapabilities : [];
        var rawIndex = select ? String(select.value || '') : '';
        var capability = rawIndex === '' ? null : capabilities[Number(rawIndex)];
        var evaluator = el('scRegEvaluatorType');
        var evaluatorTypes = capability && Array.isArray(capability.evaluator_types) ? capability.evaluator_types : ['external_evidence'];
        if (evaluator) evaluator.innerHTML = evaluatorTypes.map(function (value) { return '<option value="' + escAttr(value) + '">' + escHtml(value) + '</option>'; }).join('');
        var values = {
            scRegAdapterKey: capability ? capability.adapter_key : '',
            scRegAdapterVersion: capability ? capability.adapter_version : '',
            scRegArtifactSha: capability ? capability.artifact_sha256 : ''
        };
        Object.keys(values).forEach(function (id) { var node = el(id); if (node) node.value = values[id] || ''; });
    };

    window._strategyCombinationToggle = function () {
        var node = el('scCombinationForm');
        if (node) node.style.display = node.style.display === 'none' ? 'block' : 'none';
    };

    window._strategyReviewerToggle = function () {
        var node = el('scReviewerForm');
        if (node) node.style.display = node.style.display === 'none' ? 'block' : 'none';
    };

    window._strategyReviewerCreate = function () {
        var passwordNode = el('scReviewerPassword');
        var payload = {
            username: (el('scReviewerName') || {}).value || '',
            password: passwordNode ? passwordNode.value : '',
            role: 'EVIDENCE_REVIEWER'
        };
        fetch('/api/auth/users', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)})
            .then(function (response) { return response.json().then(function (body) { return {ok:response.ok, body:body}; }); })
            .then(function (result) {
                if (!result.ok || result.body.status !== 'ok') throw new Error(result.body.message || result.body.error || '复核账号创建失败');
                if (passwordNode) passwordNode.value = '';
                setStatus('独立复核账号已创建');
                window.alert('独立复核账号已创建。请让复核人单独登录后完成证据复核。');
            })
            .catch(function (err) { setStatus('复核账号创建失败', true); window.alert(err.message || '复核账号创建失败'); });
    };

    window._strategyRegister = function (asChallenger) {
        var evaluatorType = String((el('scRegEvaluatorType') || {}).value || '').trim();
        if (!evaluatorType) { window.alert('请选择明确的评估器类型'); return; }
        var routeValues = ['scRegRouteTrend','scRegRouteRange','scRegRouteRisk'].map(function (id) { return String((el(id) || {}).value || '').trim(); });
        if (routeValues.some(function (value) { return value === '' || !isFinite(Number(value)) || Number(value) < 0 || Number(value) > 1.5; })) { window.alert('三种市场路由系数必须明确填写，且均在0至1.5之间'); return; }
        var maxHoldingRaw = String((el('scRegMaxHold') || {}).value || '').trim();
        if (maxHoldingRaw === '' || !/^\d+$/.test(maxHoldingRaw) || Number(maxHoldingRaw) < 1 || Number(maxHoldingRaw) > 250) { window.alert('最大持有日必须是1至250之间的整数'); return; }
        var payload = {
            strategy_key: (el('scRegKey') || {}).value || '',
            strategy_name: (el('scRegName') || {}).value || '',
            version: (el('scRegVersion') || {}).value || '',
            category: (el('scRegCategory') || {}).value || '未分类',
            description: (el('scRegDescription') || {}).value || '',
            evaluator_type: evaluatorType,
            evaluator_config: {market_regime_multipliers: {
                trend_bullish: Number(routeValues[0]),
                high_range: Number(routeValues[1]),
                risk_declining: Number(routeValues[2]),
                extreme_event: 0
            }},
            parameters: {max_holding_days: Number(maxHoldingRaw)},
            reason: asChallenger ? '策略治理页面登记挑战者版本' : '策略治理页面注册全新策略并进入影子'
        };
        var adapterKey = String((el('scRegAdapterKey') || {}).value || '').trim().toLowerCase();
        var adapterVersion = String((el('scRegAdapterVersion') || {}).value || '').trim();
        var artifactSha = String((el('scRegArtifactSha') || {}).value || '').trim();
        var costModelKey = String((el('scRegCostModelKey') || {}).value || '').trim().toLowerCase();
        var costInputs = {
            commission_pct: String((el('scRegCommission') || {}).value || '').trim(),
            stamp_tax_pct: String((el('scRegStampTax') || {}).value || '').trim(),
            slippage_pct: String((el('scRegSlippage') || {}).value || '').trim(),
            transfer_fee_pct: String((el('scRegTransferFee') || {}).value || '').trim()
        };
        var bindingRequested = !!(adapterKey || adapterVersion || artifactSha || costModelKey || Object.keys(costInputs).some(function (key) { return costInputs[key] !== ''; }));
        if (bindingRequested) {
            var capabilities = Array.isArray(window._strategyCenterAdapterCapabilities) ? window._strategyCenterAdapterCapabilities : [];
            var matchedCapability = capabilities.some(function (item) {
                return String(item.adapter_key || '') === adapterKey && String(item.adapter_version || '') === adapterVersion && String(item.artifact_sha256 || '') === artifactSha && Array.isArray(item.evaluator_types) && item.evaluator_types.indexOf(evaluatorType) >= 0;
            });
            if (!matchedCapability) { window.alert('执行适配器、版本、制品或评估器类型不在服务器可信发布清单中'); return; }
            if (!/^[a-z][a-z0-9_.-]{2,79}$/.test(adapterKey)) { window.alert('执行适配器代码格式无效'); return; }
            if (!/^[0-9A-Za-z][0-9A-Za-z_.:-]{0,159}$/.test(adapterVersion)) { window.alert('执行适配器版本格式无效'); return; }
            if (!/^[0-9a-f]{64}$/.test(artifactSha)) { window.alert('制品 SHA-256 必须是64位小写十六进制'); return; }
            if (!/^[a-z][a-z0-9_.-]{2,79}$/.test(costModelKey)) { window.alert('成本模型代码格式无效'); return; }
            var invalidCost = Object.keys(costInputs).some(function (key) {
                var value = Number(costInputs[key]);
                return costInputs[key] === '' || !isFinite(value) || value < 0 || value > 10;
            });
            if (invalidCost) { window.alert('佣金、印花税、滑点和过户费必须全部明确填写，且均在0至10之间'); return; }
            payload.execution_binding = {
                adapter_key: adapterKey,
                adapter_version: adapterVersion,
                strategy_version: payload.version,
                artifact_sha256: artifactSha,
                cost_model: {
                    model_key: costModelKey,
                    currency: 'CNY',
                    commission_pct: Number(costInputs.commission_pct),
                    stamp_tax_pct: Number(costInputs.stamp_tax_pct),
                    slippage_pct: Number(costInputs.slippage_pct),
                    transfer_fee_pct: Number(costInputs.transfer_fee_pct)
                }
            };
        }
        var registrationUrl = asChallenger ? '/api/strategy-center/challengers' : '/api/strategy-center/registry';
        fetch(registrationUrl, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.error || '登记失败'); window._strategyCenterReload(); })
            .catch(function (err) { setStatus(asChallenger ? '挑战者登记失败' : '策略注册失败', true); window.alert(err.message || '登记失败'); });
    };

    window._strategyChallengerEvidenceSubmit = function (challengerId) {
        var raw = window.prompt('粘贴完整可重算证据 JSON：必须包含 as_of_date、window_days、evidence_protocol、evidence_revision_at、metrics、artifact_hash 和 artifact_manifest。页面不提供可冒充真实结果的门槛模板。', '');
        if (!raw) return;
        var evidence;
        try { evidence = JSON.parse(raw); } catch (err) { window.alert('可重算证据 JSON 无效'); return; }
        var reason = window.prompt('请输入产物提交理由：', '提交完整逐笔交易、权益曲线和 Purged Walk-Forward 原始产物');
        if (!reason) return;
        evidence.reason = reason;
        fetch('/api/strategy-center/challengers/' + encodeURIComponent(challengerId) + '/evidence', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(evidence)})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.error || '挑战者产物提交失败'); window._strategyCenterReload(); })
            .catch(function (err) { setStatus('挑战者产物提交失败', true); window.alert(err.message || '挑战者产物提交失败'); });
    };

    window._strategyChallengerReview = function (challengerId) {
        var decision = window.confirm('确认服务器应再次重放冻结产物并在全部可重算门槛通过时允许晋级吗？选择“取消”将以 REJECT 驳回。') ? 'CONFIRM' : 'REJECT';
        var reason = window.prompt('请输入独立复核理由：', decision === 'CONFIRM' ? '已核对冻结产物身份与样本隔离，要求服务器再次重放' : '独立复核驳回');
        if (!reason) return;
        fetch('/api/strategy-center/challengers/' + encodeURIComponent(challengerId) + '/review', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({decision:decision, reason:reason})})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.error || '挑战者复核失败'); window._strategyCenterReload(); })
            .catch(function (err) { setStatus('挑战者复核失败', true); window.alert(err.message || '挑战者复核失败'); });
    };

    window._strategyChallengerPromote = function (challengerId) {
        if (!window.confirm('晋级后会创建不可变新版本并从“影子观察”重新积累真实前向证据，不会直接获得模拟资金。继续吗？')) return;
        fetch('/api/strategy-center/challengers/' + encodeURIComponent(challengerId) + '/promote', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({reason:'冻结产物经独立复核再次重放通过，晋级为新影子版本'})})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.error || '挑战者晋级失败'); window._strategyCenterReload(); })
            .catch(function (err) { setStatus('挑战者晋级失败', true); window.alert(err.message || '挑战者晋级失败'); });
    };

    window._strategyCombinationRegister = function () {
        var rawMembers = String((el('scComboMembers') || {}).value || '');
        var members = [];
        try {
            rawMembers.split(/[,，\n]+/).forEach(function (raw) {
                var item = raw.trim();
                if (!item) return;
                var parts = item.split(/[=:：]/);
                var weight = Number(parts[1]);
                if (parts.length !== 2 || !parts[0].trim() || !isFinite(weight) || weight <= 0) throw new Error('组合成员格式必须是 strategy_key=正数权重');
                members.push({strategy_key:parts[0].trim(), weight:weight});
            });
        } catch (err) {
            window.alert(err.message || '组合成员格式无效');
            return;
        }
        if (members.length < 2) { window.alert('请至少填写两个组合成员及权重'); return; }
        var payload = {
            combination_key: (el('scComboKey') || {}).value || '',
            combination_name: (el('scComboName') || {}).value || '',
            version: (el('scComboVersion') || {}).value || '',
            description: (el('scComboDescription') || {}).value || '',
            members: members,
            constraints: {
                formal_requires_all_members_eligible:true,
                maximum_member_weight:Number((el('scComboMaxMember') || {}).value),
                maximum_pairwise_correlation:Number((el('scComboMaxCorr') || {}).value),
                minimum_pairwise_observations:Number((el('scComboMinCorrObs') || {}).value),
                maximum_stock_overlap_pct:Number((el('scComboMaxOverlap') || {}).value),
                maximum_industry_weight_pct:Number((el('scComboMaxIndustry') || {}).value),
                real_order_authority:false
            },
            reason: '策略治理页面注册新组合或新版本'
        };
        fetch('/api/strategy-center/combinations', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.error || '组合注册失败'); window._strategyCenterReload(); })
            .catch(function (err) { setStatus('组合注册失败', true); window.alert(err.message || '组合注册失败'); });
    };

    window._strategyGovernanceRankingPage = function (entityType, cursor, query) {
        var governance = window._strategyCenterGovernance || {};
        var normalizedType = String(entityType || '').toUpperCase();
        if (normalizedType !== 'STRATEGY' && normalizedType !== 'COMBINATION') return;
        var params = new URLSearchParams({
            trade_date:String(governance.trade_date || ''),
            run_uid:String(governance.run_uid || ''),
            canonical_result_hash:String(governance.canonical_result_hash || ''),
            cursor:String(cursor || ''),
            limit:'50',
            query:String(query || '')
        });
        setStatus('正在读取规范排名分页...');
        fetch('/api/strategy-center/governance/rankings/' + normalizedType + '?' + params.toString())
            .then(function (response) { return response.json(); })
            .then(function (result) {
                if (result.status !== 'ok' || !result.page) throw new Error(result.message || result.error || '排名分页读取失败');
                if (normalizedType === 'STRATEGY') governance.strategies = result.page.rows || [];
                else governance.combinations = result.page.rows || [];
                governance.ranking_pages = governance.ranking_pages || {};
                governance.ranking_pages[normalizedType === 'STRATEGY' ? 'strategy' : 'combination'] = result.page;
                renderStrategyCenter(
                    window._strategyCenterContainer,
                    window._strategyCenterData || {},
                    governance,
                    window._strategyCenterGovernanceHistory || {}
                );
                setStatus('规范排名分页已更新');
            })
            .catch(function (err) {
                setStatus('规范排名分页读取失败', true);
                window.alert(err.message || '排名分页读取失败，请刷新总览');
            });
    };

    window._strategyGovernanceRankingSearch = function (entityType, inputId) {
        var input = el(inputId);
        window._strategyGovernanceRankingPage(
            entityType, '', String((input || {}).value || '').trim()
        );
    };

    window._strategyGovernanceHistoryPage = function (section, direction) {
        var normalizedSection = String(section || '').toLowerCase();
        var sectionEndpoints = {
            lifecycle:'lifecycle',
            audit:'audit',
            metric_evidence:'metric-evidence',
            adapter_run_receipts:'adapter-run-receipts',
            runs:'runs'
        };
        var sectionArrays = {
            lifecycle:'lifecycle_events',
            audit:'audit_events',
            metric_evidence:'metric_evidence',
            adapter_run_receipts:'adapter_run_receipts',
            runs:'runs'
        };
        var sectionInputs = {
            lifecycle:'scLifecycleHistorySearch',
            audit:'scAuditHistorySearch',
            metric_evidence:'scMetricEvidenceHistorySearch',
            adapter_run_receipts:'scAdapterReceiptHistorySearch'
        };
        if (!sectionEndpoints[normalizedSection]) return;
        var history = window._strategyCenterGovernanceHistory || {};
        history.history_pages = history.history_pages || {};
        var currentPage = history.history_pages[normalizedSection] || {};
        window._strategyGovernanceHistoryState = window._strategyGovernanceHistoryState || {};
        var currentState = window._strategyGovernanceHistoryState[normalizedSection] || {currentCursor:'', stack:[], pageNo:1};
        var nextState = {
            currentCursor:String(currentState.currentCursor || ''),
            stack:Array.isArray(currentState.stack) ? currentState.stack.slice() : [],
            pageNo:Math.max(1, Number(currentState.pageNo || 1))
        };
        var requestCursor = '';
        var filters = currentPage.filters || {};
        var inputId = sectionInputs[normalizedSection] || '';
        if (direction === 'next') {
            if (!currentPage.next_cursor) return;
            requestCursor = String(currentPage.next_cursor);
            nextState.stack.push(nextState.currentCursor);
            nextState.currentCursor = requestCursor;
            nextState.pageNo += 1;
        } else if (direction === 'previous') {
            if (!nextState.stack.length) return;
            requestCursor = String(nextState.stack.pop() || '');
            nextState.currentCursor = requestCursor;
            nextState.pageNo = Math.max(1, nextState.pageNo - 1);
        } else if (direction === 'search') {
            if (!inputId) return;
            filters = {entity_key:String(((el(inputId) || {}).value) || '').trim()};
            nextState = {currentCursor:'', stack:[], pageNo:1};
        } else {
            return;
        }
        var params = new URLSearchParams({limit:'50', cursor:requestCursor});
        ['entity_type','entity_key','action','date_from','date_to'].forEach(function (key) {
            if (filters[key]) params.set(key, String(filters[key]));
        });
        setStatus('正在读取治理理由历史...');
        fetch('/api/strategy-center/governance/history/' + sectionEndpoints[normalizedSection] + '?' + params.toString())
            .then(function (response) { return response.json(); })
            .then(function (result) {
                var page = result.page || {};
                if (
                    result.status !== 'ok' ||
                    result.automatic_real_order_submission !== false ||
                    result.real_order_authority !== false ||
                    page.automatic_real_order_submission !== false ||
                    page.real_order_authority !== false ||
                    page.raw_payload_inline !== false ||
                    !Array.isArray(page.rows)
                ) throw new Error(result.message || result.error || '治理历史分页合同无效');
                history.history_pages[normalizedSection] = page;
                history[sectionArrays[normalizedSection]] = page.rows;
                window._strategyGovernanceHistoryState[normalizedSection] = nextState;
                window._strategyCenterGovernanceHistory = history;
                renderStrategyCenter(
                    window._strategyCenterContainer,
                    window._strategyCenterData || {},
                    window._strategyCenterGovernance || {},
                    history
                );
                setStatus('治理理由历史已更新');
            })
            .catch(function (err) {
                setStatus('治理理由历史读取失败', true);
                window.alert(err.message || '历史分页读取失败，请刷新总览');
            });
    };

    window._strategyFundingDetail = function (entityType, entityKey, windowDays, series, cursor) {
        var governance = window._strategyCenterGovernance || {};
        var normalizedType = String(entityType || '').toUpperCase();
        var normalizedSeries = String(series || 'daily_records');
        var windowValue = Number(windowDays || 60);
        var panel = el('scFundingDetailPanel');
        if (!panel || (normalizedType !== 'STRATEGY' && normalizedType !== 'COMBINATION')) return;
        var endpointType = normalizedType === 'COMBINATION' ? 'combinations' : 'strategies';
        var params = new URLSearchParams({
            trade_date:String(governance.trade_date || ''),
            run_uid:String(governance.run_uid || ''),
            canonical_result_hash:String(governance.canonical_result_hash || ''),
            window_days:String(windowValue),
            series:normalizedSeries,
            limit:'50',
            cursor:String(cursor || '')
        });
        panel.style.display = '';
        panel.innerHTML = '<strong>正在读取已验证V3资金事实...</strong><small>只读取当前canonical修订；真实下单权限关闭</small>';
        setStatus('正在读取资金事实明细...');
        fetch('/api/strategy-center/governance/funding/' + endpointType + '/' + encodeURIComponent(entityKey) + '?' + params.toString())
            .then(function (response) { return response.json(); })
            .then(function (result) {
                var page = result.page || {};
                if (
                    result.status !== 'ok' ||
                    result.automatic_real_order_submission !== false ||
                    result.real_order_authority !== false ||
                    page.automatic_real_order_submission !== false ||
                    page.real_order_authority !== false ||
                    page.response_byte_boxed !== true ||
                    !Array.isArray(page.items)
                ) throw new Error(result.message || result.error || '资金明细合同无效');
                var title = normalizedType === 'COMBINATION' ? '组合成员事实链复算明细' : '单策略V3资金检查点明细';
                var seriesButtons = [
                    ['daily_records','日收益'],
                    ['equity_curve','净值']
                ];
                if (normalizedType === 'COMBINATION') seriesButtons.push(['members','成员哈希']);
                var h = '<div class="sc-section-title"><span>' + title + '</span><small>' + escHtml(entityKey) + ' · ' + windowValue + '日 · 当前canonical修订</small></div>';
                h += '<div class="sc-governance-toolbar">';
                [20,60,120].forEach(function (days) {
                    h += '<button class="sc-btn" onclick="window._strategyFundingDetail(\'' + normalizedType + '\',\'' + escAttr(entityKey) + '\',' + days + ',\'' + normalizedSeries + '\',\'\')">' + days + '日</button>';
                });
                seriesButtons.forEach(function (item) {
                    h += '<button class="sc-btn" onclick="window._strategyFundingDetail(\'' + normalizedType + '\',\'' + escAttr(entityKey) + '\',' + windowValue + ',\'' + item[0] + '\',\'\')">' + item[1] + '</button>';
                });
                h += '<button class="sc-btn" onclick="window._strategyFundingDetail(\'' + normalizedType + '\',\'' + escAttr(entityKey) + '\',' + windowValue + ',\'' + normalizedSeries + '\',\'\')">第一页</button>';
                if (page.next_cursor) h += '<button class="sc-btn" onclick="window._strategyFundingDetail(\'' + normalizedType + '\',\'' + escAttr(entityKey) + '\',' + windowValue + ',\'' + normalizedSeries + '\',\'' + escAttr(page.next_cursor) + '\')">下一页</button>';
                h += '<button class="sc-btn" onclick="document.getElementById(\'scFundingDetailPanel\').style.display=\'none\'">关闭</button>';
                h += '<span>本页 ' + Number(page.row_count || page.items.length) + ' 条 · 共 ' + Number(page.total_count || 0) + ' 条 · 响应硬上限4MiB</span></div>';
                if (normalizedType === 'COMBINATION') {
                    h += '<div class="sc-governance-notice"><strong>组合真值口径</strong><span>按冻结成员权重和各成员已验证fact-set重建窗口初始袖套，随后权重自然漂移；不生成独立组合现金事实，本详情本身不授予资金。</span></div>';
                    h += '<small>配方 ' + escHtml(String(page.recipe_hash || '-').slice(0, 16)) + ' · 复算 ' + escHtml(String(page.reconstruction_hash || '-').slice(0, 16)) + ' · 成员 ' + Number((page.member_fact_sets || []).length) + '</small>';
                } else {
                    h += '<small>检查点 ' + escHtml(String(page.checkpoint_id || '-').slice(0, 16)) + ' · fact-set ' + escHtml(String(page.history_fact_set_hash || '-').slice(0, 16)) + ' · 来源仅限V3规范化事实链</small>';
                }
                h += '<div class="sc-table-wrap"><table class="sc-table governance-table"><thead><tr><th>日期/成员</th><th>净收益/权重</th><th>实际成本/版本</th><th>净值/检查点</th><th>事实集合哈希</th></tr></thead><tbody>';
                page.items.forEach(function (item) {
                    h += '<tr><td>' + escHtml(item.trade_date || item.strategy_key || '-') + '</td><td>' + (item.return_pct == null ? strategyGovernanceMetric(Number(item.weight || 0) * 100, '%', 2) : strategyGovernanceMetric(item.return_pct, '%', 4)) + '</td><td>' + (item.actual_cost_pct == null ? escHtml(item.strategy_version || '-') : strategyGovernanceMetric(item.actual_cost_pct, '%', 4)) + '</td><td>' + (item.equity == null ? escHtml(String(item.checkpoint_id || '-').slice(0, 16)) : strategyGovernanceMetric(item.equity, '', 6)) + '</td><td>' + escHtml(String(item.history_fact_set_hash || item.fact_hash || '-').slice(0, 16)) + '</td></tr>';
                });
                if (!page.items.length) h += '<tr><td colspan="5" class="sc-empty-cell">该序列当前为空</td></tr>';
                h += '</tbody></table></div>';
                panel.innerHTML = h;
                panel.scrollIntoView({behavior:'smooth', block:'start'});
                setStatus('已加载当前canonical资金事实明细');
            })
            .catch(function (err) {
                panel.innerHTML = '<div class="sc-warning">资金明细读取失败：' + escHtml(err.message || '请刷新规范结果后重试') + '</div><button class="sc-btn" onclick="document.getElementById(\'scFundingDetailPanel\').style.display=\'none\'">关闭</button>';
                setStatus('资金明细读取失败', true);
            });
    };

    window._strategyGovernanceTransition = function (key, nextStatus, entityType) {
        var labels = {SHADOW:'影子观察', SUSPENDED:'暂停使用', RETIRED:'已淘汰'};
        var reason = window.prompt('请输入转为“' + (labels[nextStatus] || nextStatus) + '”的具体理由。该理由会永久记录：', '人工复核后的治理决定');
        if (!reason) return;
        var prefix = entityType === 'COMBINATION' ? '/api/strategy-center/combinations/' : '/api/strategy-center/strategies/';
        fetch(prefix + encodeURIComponent(key) + '/lifecycle', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({next_status:nextStatus, reason:reason, evidence:{source:'manual_page_review'}})})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.error || '状态更新失败'); window._strategyCenterReload(); })
            .catch(function (err) { setStatus('生命周期更新失败', true); window.alert(err.message || '生命周期更新失败'); });
    };

    window._strategyMetricReview = function (evidenceId, decision) {
        var actionLabel = decision === 'CONFIRM' ? '确认' : '驳回';
        var reason = window.prompt('请输入' + actionLabel + '该独立证据的具体复核理由。提交人与复核人必须不同：', actionLabel === '确认' ? '已核对逐笔样本、权益曲线、版本、窗口与哈希' : '证据内容或验证边界不符合要求');
        if (!reason) return;
        fetch('/api/strategy-center/metrics/' + encodeURIComponent(evidenceId) + '/review', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({decision:decision, reason:reason})})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.error || '证据复核失败'); window._strategyCenterReload(); })
            .catch(function (err) { setStatus('证据复核失败', true); window.alert(err.message || '证据复核失败'); });
    };

    window._strategyGovernanceRun = function () {
        if (window._strategyGovernanceRunning) return;
        if (!window.confirm('系统只会使用最新且已通过新鲜度校验的数据日执行治理；历史日期仅供回看。将写入健康快照、票池、排名、模拟权重和必要的状态变化记录。继续吗？')) return;
        window._strategyGovernanceRunning = true;
        var button = el('scGovernanceRunBtn');
        if (button) { button.disabled = true; button.textContent = '治理执行中…'; }
        setStatus('正在执行最新数据日策略治理更新...');
        fetch('/api/strategy-center/governance/run', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({trade_date:'', limit:500})})
            .then(function (response) { return response.json(); })
            .then(function (result) { if (result.status !== 'ok') throw new Error(result.message || result.reason || result.error || '治理更新失败'); window._strategyCenterDate = result.trade_date || currentDateValue(); window._strategyCenterReload(); })
            .catch(function (err) { setStatus('策略治理更新失败', true); window.alert(err.message || '策略治理更新失败'); })
            .finally(function () { window._strategyGovernanceRunning = false; var currentButton = el('scGovernanceRunBtn'); if (currentButton) { currentButton.disabled = false; currentButton.textContent = '执行最新数据日治理'; } });
    };

    function loadMarketRadarPage(d, c) {
        if (window._marketRadarTimer) {
            clearInterval(window._marketRadarTimer);
            window._marketRadarTimer = null;
        }
        var loading = false;
        c.innerHTML = '<style>' +
            '.market-radar-panel{padding:4px 0}.market-radar-head{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px}' +
            '.market-radar-pill{background:#f0f3f7;border-radius:14px;padding:6px 10px;color:#667085;font-size:12px}' +
            '.market-radar-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}' +
            '.market-radar-card{background:#fff;border-radius:10px;padding:14px;box-shadow:0 1px 5px rgba(16,24,40,.08);overflow:auto}' +
            '.market-radar-card.wide{grid-column:1/-1}.market-radar-card h3{margin:0 0 10px;font-size:15px}' +
            '.market-radar-card h3.up{color:#d92d20}.market-radar-card h3.down{color:#079455}' +
            '.market-radar-table{width:100%;border-collapse:collapse;font-size:12px}.market-radar-table th,.market-radar-table td{padding:7px 6px;border-bottom:1px solid #edf0f3;text-align:left;white-space:nowrap}' +
            '.market-radar-table th{color:#667085;font-weight:600}.market-radar-up{color:#d92d20}.market-radar-down{color:#079455}' +
            '.market-radar-muted{color:#98a2b3}.market-radar-event{padding:8px 10px;border-left:3px solid #1769e0;background:#f8fafc;margin-bottom:7px;font-size:12px}' +
            '@media(max-width:900px){.market-radar-grid{grid-template-columns:1fr}}' +
            '</style><div class="market-radar-panel"><div class="market-radar-head">' +
            '<button onclick="window.marketRadarScan()">立即扫描</button><span id="marketRadarStatus" class="market-radar-pill">读取中...</span>' +
            '<span id="marketRadarMethod" class="market-radar-pill"></span></div>' +
            '<div class="market-radar-grid"><section class="market-radar-card wide"><h3>最近事件</h3><div id="marketRadarEvents" class="market-radar-muted">暂无</div></section>' +
            '<section class="market-radar-card"><h3 class="up">上涨板块 / 带头股</h3><div id="marketRadarUpSectors" class="market-radar-muted">暂无</div></section>' +
            '<section class="market-radar-card"><h3 class="down">下跌板块 / 带头股</h3><div id="marketRadarDownSectors" class="market-radar-muted">暂无</div></section>' +
            '<section class="market-radar-card"><h3 class="up">上涨个股</h3><div id="marketRadarUpStocks" class="market-radar-muted">暂无</div></section>' +
            '<section class="market-radar-card"><h3 class="down">下跌个股</h3><div id="marketRadarDownStocks" class="market-radar-muted">暂无</div></section></div></div>';

        function radarGet(path) {
            var joiner = path.indexOf('?') >= 0 ? '&' : '?';
            return fetchRawJsonWithTimeout(path + joiner + '_=' + Date.now(), 15000);
        }
        function radarPct(value) {
            var n = Number(value || 0);
            return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
        }
        function radarScore(value) {
            var n = Number(value);
            return isNaN(n) ? '-' : n.toFixed(1);
        }
        function radarRows(rows, kind) {
            if (!rows || !rows.length) return '<div class="market-radar-muted">暂无数据</div>';
            if (kind === 'stock') {
                var stockHtml = '<table class="market-radar-table"><thead><tr><th>代码</th><th>名称</th><th>涨跌</th><th>评分</th><th>成交额增量</th><th>五档压力</th><th>标签</th></tr></thead><tbody>';
                rows.forEach(function (r) {
                    var cls = r.direction === 'DOWN' ? 'market-radar-down' : 'market-radar-up';
                    stockHtml += '<tr><td>' + escHtml(r.stock_code || '-') + '</td><td>' + escHtml(r.short_name || '-') + '</td><td class="' + cls + '">' + radarPct(r.change_pct) + '</td><td>' + radarScore(r.score) + '</td><td>' + fmtMoney(r.amount_delta) + '</td><td>' + radarPct(r.five_pressure) + '</td><td class="market-radar-muted">' + escHtml((r.signal_tags || []).join(' / ')) + '</td></tr>';
                });
                return stockHtml + '</tbody></table>';
            }
            var sectorHtml = '<table class="market-radar-table"><thead><tr><th>板块</th><th>评分</th><th>宽度</th><th>涨跌均值</th><th>龙一</th><th>龙二</th><th>龙三</th><th>中军</th></tr></thead><tbody>';
            rows.forEach(function (r) {
                var dragons = Array.isArray(r.dragon_json) ? r.dragon_json : [];
                var core = r.core_json || {};
                sectorHtml += '<tr><td>' + escHtml(r.sector_name || '-') + '<span class="market-radar-muted"> ' + escHtml(r.sector_type || '') + '</span></td><td>' + radarScore(r.score) + '</td><td>' + radarPct(r.breadth_pct) + '</td><td>' + radarPct(r.avg_change_pct) + '</td><td>' + escHtml((dragons[0] && (dragons[0].short_name || dragons[0].stock_code)) || '-') + '</td><td>' + escHtml((dragons[1] && (dragons[1].short_name || dragons[1].stock_code)) || '-') + '</td><td>' + escHtml((dragons[2] && (dragons[2].short_name || dragons[2].stock_code)) || '-') + '</td><td>' + escHtml(core.short_name || core.stock_code || '-') + '</td></tr>';
            });
            return sectorHtml + '</tbody></table>';
        }
        function render(data) {
            var status = data[0] || {};
            var latest = status.latest || {};
            el('marketRadarStatus').textContent = (latest.latest_stock_at || '未扫描') + '｜个股 ' + (latest.stock_rows || 0) + '｜板块 ' + (latest.sector_rows || 0) + '｜事件 ' + (latest.event_rows || 0);
            el('marketRadarMethod').textContent = status.flow_note || 'QMT 五档压力代理';
            el('marketRadarEvents').innerHTML = (data[5].rows || []).map(function (item) {
                var cls = item.direction === 'DOWN' ? 'market-radar-down' : 'market-radar-up';
                return '<div class="market-radar-event"><b class="' + cls + '">' + escHtml(item.direction || '-') + '</b> ' + escHtml(item.sector_name || item.stock_code || '市场') + '｜评分 ' + radarScore(item.score) + '｜' + escHtml(item.snapshot_at || '') + '</div>';
            }).join('') || '暂无事件';
            el('marketRadarUpSectors').innerHTML = radarRows(data[1].rows, 'sector');
            el('marketRadarDownSectors').innerHTML = radarRows(data[2].rows, 'sector');
            el('marketRadarUpStocks').innerHTML = radarRows(data[3].rows, 'stock');
            el('marketRadarDownStocks').innerHTML = radarRows(data[4].rows, 'stock');
        }
        function refresh() {
            if (loading || activeTabId() !== 'market-radar') return;
            loading = true;
            Promise.all([
                radarGet('/api/market-radar/status'),
                radarGet('/api/market-radar/sectors?direction=UP&limit=20'),
                radarGet('/api/market-radar/sectors?direction=DOWN&limit=20'),
                radarGet('/api/market-radar/stocks?direction=UP&limit=30'),
                radarGet('/api/market-radar/stocks?direction=DOWN&limit=30'),
                radarGet('/api/market-radar/events?limit=15')
            ]).then(render).catch(function (e) {
                el('marketRadarStatus').textContent = '读取失败：' + e.message;
            }).finally(function () { loading = false; });
        }
        window.marketRadarRefresh = refresh;
        window.marketRadarScan = function () {
            el('marketRadarStatus').textContent = '扫描中...';
            fetch('/api/market-radar/scan', {method:'POST'}).then(function (r) {
                if (!r.ok) throw new Error('HTTP ' + r.status);
                return r.json();
            }).then(refresh).catch(function (e) { el('marketRadarStatus').textContent = '扫描失败：' + e.message; });
        };
        refresh();
        window._marketRadarTimer = setInterval(function () {
            if (activeTabId() !== 'market-radar') {
                clearInterval(window._marketRadarTimer);
                window._marketRadarTimer = null;
                return;
            }
            refresh();
        }, 5000);
    }

    /* ===== 策略与模拟：把选股、委托、成交和持仓放在同一条链路里 ===== */
    var TRADING_STRATEGY_NAMES = {
        theme_diffusion: '板块扩散预热',
        low_base_ignition: '板块点火预判',
        right_side_trend: '右侧趋势启动',
        event_drift: '事件后漂移',
        quality_momentum: '质量与动量',
        oversold_reversal: '超跌反转试验',
        intraday_surprise: '盘中超预期',
        ai_application_research: 'AI 应用研究',
        robotics_research: '机器人研究',
        paper_discovery: '模拟试错'
    };
    var TRADING_STATUS_NAMES = {
        PLANNED: '已入选',
        QUEUED: '等待模拟撮合',
        SUBMITTED: '已提交模拟委托',
        PENDING: '等待模拟成交',
        RISK_APPROVED: '模拟风控通过',
        PARTIALLY_FILLED: '部分模拟成交',
        FILLED: '已模拟成交',
        CANCELLED: '已取消',
        REJECTED: '已拒绝',
        EXPIRED: '已过期',
        VALIDATED_POSITIVE: '通过策略闸门',
        PAPER_DISCOVERY_CANDIDATE: '小仓模拟试错',
        LEFT_SIDE_PREPARE: '准备观察，暂不买',
        RESEARCH_ONLY_UNCALIBRATED: '仅研究，未校准',
        RESEARCH_ONLY_MODEL_VERSION_MISMATCH: '模型版本不匹配',
        RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED: '排序校准失败',
        RESEARCH_ONLY_PROFIT_GATE_FAILED: '收益闸门失败',
        INSUFFICIENT_DATA: '数据不足',
        SETUP_NOT_READY: '入场条件未齐',
        WEAK_MARKET_THEME_WATCH: '弱市观察',
        MARKET_REGIME_BLOCKED: '市场状态阻断'
    };
    var TRADING_REGIME_NAMES = {
        trend_bullish: '趋势偏强',
        risk_declining: '风险收缩',
        high_range: '高波动',
        extreme_event: '极端风险',
        unknown: '状态未知'
    };
    function tradingDeskData(payload) {
        return payload && Object.prototype.hasOwnProperty.call(payload, 'data') ? payload.data : payload;
    }
    function tradingDeskRequest(path) {
        return fetchRawJsonWithTimeout(path, 20000).then(function (payload) {
            return { data: tradingDeskData(payload), generatedAt: payload && payload.generated_at, error: '' };
        }).catch(function (err) {
            return { data: null, generatedAt: '', error: err && err.message ? err.message : '接口读取失败' };
        });
    }
    function tradingDeskCode(value) {
        var code = String(value || '').split('.')[0].trim();
        return code ? code.padStart(6, '0') : '';
    }
    function tradingDeskStrategy(value) {
        return TRADING_STRATEGY_NAMES[String(value || '')] || String(value || '未标注');
    }
    function tradingDeskStrategies(row) {
        var values = (row && row.strategy_keys) || [];
        if (!Array.isArray(values)) values = [values];
        return values.length ? values.map(tradingDeskStrategy).join('、') : tradingDeskStrategy(row && (row.strategy_key || row.strategy_version));
    }
    function tradingDeskStatus(value) {
        return TRADING_STATUS_NAMES[String(value || '').toUpperCase()] || localizeMachineText(String(value || '—'));
    }
    function tradingDeskMoney(value) {
        if (value == null || value === '') return '—';
        var number = Number(value);
        return Number.isFinite(number) ? '¥' + number.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '—';
    }
    function tradingDeskPercent(value, scale) {
        if (value == null || value === '') return '—';
        var number = Number(value);
        return Number.isFinite(number) ? (number * (scale || 1)).toFixed(2) + '%' : '—';
    }
    function tradingDeskPrice(value) {
        if (value == null || value === '') return '—';
        var number = Number(value);
        return Number.isFinite(number) ? number.toFixed(2) : '—';
    }
    function tradingDeskTime(value) {
        return value ? String(value).replace('T', ' ').slice(0, 16) : '—';
    }
    function tradingDecisionTruth(context) {
        context = context || {};
        var decisionStatus = String(context.decision_status || '').toUpperCase();
        var dataStatus = String(context.data_status || '').toUpperCase();
        var runStatus = String(context.run_status || '').toUpperCase();
        var loading = /^(LOADING|PROCESSING|CREATED|RUNNING|QUEUED|DECISION_COMMITTED|POSITIONS_SYNCED)$/.test(decisionStatus) || /^(LOADING|PROCESSING|CREATED|RUNNING|QUEUED|DECISION_COMMITTED|POSITIONS_SYNCED)$/.test(dataStatus) || /^(LOADING|PROCESSING|CREATED|RUNNING|QUEUED|DECISION_COMMITTED|POSITIONS_SYNCED)$/.test(runStatus);
        if (loading) return 'LOADING';
        if (context.historical_read_only === true) return 'STALE';
        var validUntil = context.valid_until ? new Date(context.valid_until) : null;
        if (context.context_date_matches === false || (validUntil && Number.isFinite(validUntil.getTime()) && validUntil.getTime() < Date.now())) return 'STALE';
        if (/FAILED|ERROR|UNAVAILABLE/.test(runStatus) || /FAILED|ERROR|UNAVAILABLE|SCHEMA_MISSING/.test(dataStatus) || /FAILED|ERROR|UNAVAILABLE|SCHEMA_MISSING/.test(decisionStatus)) return 'UNAVAILABLE';
        if (/BLOCK|REJECT|HALT|PAUSE/.test(dataStatus) || /BLOCK|REJECT|HALT|PAUSE/.test(decisionStatus)) return 'BLOCKED';
        if (runStatus !== 'COMPLETED' || dataStatus !== 'READY' || context.decision_integrity_verified !== true || !context.run_uid || !context.decision_session_date || !context.data_date) return 'UNAVAILABLE';
        var rawTargetCount = context.target_count;
        var targetCount = Number(rawTargetCount);
        var targetCountVerified = rawTargetCount !== null && rawTargetCount !== '' && Number.isInteger(targetCount) && targetCount >= 0;
        if (!targetCountVerified) return 'UNAVAILABLE';
        if (decisionStatus === 'EMPTY') return targetCount === 0 ? 'EMPTY' : 'UNAVAILABLE';
        if (decisionStatus === 'READY' || decisionStatus === 'CANDIDATE_AVAILABLE') return targetCount > 0 ? 'READY' : 'UNAVAILABLE';
        return 'UNAVAILABLE';
    }
    function tradingDeskSameDate(value, tradeDate) {
        return !!value && !!tradeDate && String(value).slice(0, 10) === String(tradeDate).slice(0, 10);
    }
    function tradingDeskName(row, fallback) {
        row = row || {};
        var code = tradingDeskCode(row.stock_code || row.code);
        var rawCode = String(row.stock_code || row.code || '').trim();
        var candidates = [row.short_name, row.stock_name, row.security_name, row.name, fallback];
        for (var i = 0; i < candidates.length; i++) {
            var value = String(candidates[i] || '').trim();
            if (value && value !== code && value !== rawCode) return value;
        }
        return code || String(fallback || '').trim();
    }
    function tradingDeskSecurity(code, name) {
        code = tradingDeskCode(code);
        return '<strong class="trade-stock-name">' + escHtml(name || code) + '</strong><small>' + escHtml(code) + '</small>';
    }
    function tradingDeskEmpty(columns, title, detail) {
        return '<tr><td colspan="' + columns + '"><div class="trade-empty"><strong>' + escHtml(title) + '</strong><span>' + escHtml(detail || '') + '</span></div></td></tr>';
    }
    function tradingDeskLatestByCode(rows, dateField) {
        var result = {};
        (rows || []).forEach(function (row) {
            var code = tradingDeskCode(row.stock_code);
            var stamp = String(row[dateField] || row.created_at || '');
            var current = result[code];
            var currentStamp = current ? String(current[dateField] || current.created_at || '') : '';
            if (!current || stamp > currentStamp) result[code] = row;
        });
        return result;
    }
    window.resizeTradingModuleFrame = function(frame, hintedHeight) {
        if (!frame) return;
        var height = Number(hintedHeight || 0);
        if (!height) {
            try {
                var doc = frame.contentDocument;
                height = Math.max(doc.body.scrollHeight, doc.documentElement.scrollHeight);
            } catch (e) { height = 0; }
        }
        if (height) frame.style.height = Math.min(1100, Math.max(720, height + 12)) + 'px';
    };
    window.resizeTradingV2Frame = window.resizeTradingModuleFrame;
    window.loadTradingModuleFrame = function(frame) {
        if (!frame || frame.dataset.loading === '1' || frame.dataset.loaded === '1') return;
        frame.dataset.loading = '1';
        var modulePage = frame.dataset.modulePage === 'v2' ? 'v2' : 'v3';
        fetch('/static/trading-' + modulePage + '.html?embedded=1', { credentials: 'same-origin', cache: 'no-store' })
            .then(function(response) {
                if (response.redirected && /\/login(?:\?|$)/.test(response.url || '')) {
                    throw new Error('登录状态已失效，请刷新页面后重新登录');
                }
                if (!response.ok) throw new Error('工作台加载失败（HTTP ' + response.status + '）');
                return response.text();
            })
            .then(function(html) {
                frame.dataset.loaded = '1';
                frame.srcdoc = html;
            })
            .catch(function(error) {
                frame.srcdoc = '<!doctype html><html lang="zh-CN"><body style="margin:0;padding:28px;background:#0e171c;color:#e9eef2;font-family:Microsoft YaHei,sans-serif"><main role="alert" aria-live="assertive"><h1 style="margin:0 0 10px;font-size:20px">完整交易工作台暂时无法加载</h1><p style="margin:0;color:#c7d0d6;font-size:14px;line-height:1.6">' + escHtml(error.message || error) + '</p></main></body></html>';
                setStatus('交易模块加载失败: ' + (error.message || error), true);
            })
            .finally(function() { frame.dataset.loading = '0'; });
    };
    window.loadTradingV2Frame = window.loadTradingModuleFrame;
    window.tradingDeskFrameLoaded = function(frame) {
        if (!frame || frame.dataset.loaded !== '1') return;
        window.resizeTradingModuleFrame(frame);
        var view = frame && frame.dataset ? (frame.dataset.pendingView || '') : '';
        var modulePage = frame && frame.dataset && frame.dataset.modulePage === 'v2' ? 'v2' : 'v3';
        if (view && frame.contentWindow) {
            frame.contentWindow.postMessage({ type: 'probiga-trading-' + modulePage + '-view', view: view, requested_date:frame.dataset.requestedDate || currentDateValue(), filters:tradingRouteFilters() }, window.location.origin);
        }
        if (frame && frame.contentDocument && window.ResizeObserver && !frame._contentResizeObserver) {
            frame._contentResizeObserver = new ResizeObserver(function() {
                window.resizeTradingModuleFrame(frame);
            });
            frame._contentResizeObserver.observe(frame.contentDocument.documentElement);
            if (frame.contentDocument.body) frame._contentResizeObserver.observe(frame.contentDocument.body);
        }
        [100, 400, 1200, 3000].forEach(function(delay) {
            window.setTimeout(function() { window.resizeTradingModuleFrame(frame); }, delay);
        });
        setStatus((frame.title || '交易模块') + '已加载');
    };
    window.addEventListener('message', function(event) {
        if (event.data && event.data.type === 'probiga-trading-v3-navigate') {
            if (event.origin !== window.location.origin && event.origin !== 'null') return;
            var viewMap = {overview:'trading-v3-overview',positions:'trading-v3-positions',candidates:'trading-v3-candidates',intraday:'trading-v3-intraday',hypotheses:'trading-v3-hypotheses',portfolio:'trading-v3-positions',orders:'trading-v3-positions',validation:'trading-v3-hypotheses',missed:'trading-v3-hypotheses',evidence:'trading-v3-overview'};
            if (event.data.requested_date && el('datePicker')) el('datePicker').value = event.data.requested_date;
            window.updateTradingRouteFilters(event.data.filters || {});
            if (viewMap[event.data.view]) window.switchTab(viewMap[event.data.view]);
            return;
        }
        if (event.data && event.data.type === 'probiga-trading-v3-filter') {
            if (event.origin !== window.location.origin && event.origin !== 'null') return;
            window.updateTradingRouteFilters(event.data.filters || {});
            return;
        }
        if (event.data && event.data.type === 'probiga-open-kline') {
            if (event.origin !== window.location.origin && event.origin !== 'null') return;
            var sourceFrame = null;
            document.querySelectorAll('.trade-full-frame').forEach(function(frame) {
                if (event.source === frame.contentWindow) sourceFrame = frame;
            });
            if (!sourceFrame) return;
            window.openKlineModal(String(event.data.stock_code || ''), String(event.data.short_name || ''));
            return;
        }
        if (!event.data || ['probiga-trading-v2-resize', 'probiga-trading-v3-resize'].indexOf(event.data.type) < 0) return;
        var matchedFrame = null;
        document.querySelectorAll('.trade-full-frame').forEach(function(frame) {
            if (event.source === frame.contentWindow) matchedFrame = frame;
        });
        if (!matchedFrame || (event.origin !== window.location.origin && event.origin !== 'null')) return;
        window.resizeTradingModuleFrame(matchedFrame, event.data.height);
    });
    window.openTradingModule = function(tabId) {
        window.switchTab(String(tabId || 'trading-v3-overview'));
    };
    window.openTradingV2Module = function(view) {
        var compatibility = {
            trust:'trading-v3-overview', tomorrow:'trading-v3-overview',
            opportunity:'trading-v3-candidates', plan:'trading-v3-positions',
            positions:'trading-v3-positions', orders:'trading-v3-positions',
            etf:'trading-v3-overview', evidence:'trading-v3-overview',
            operations:'trading-v3-overview', review:'trading-v3-hypotheses',
            lab:'trading-v3-candidates'
        };
        window.openTradingModule(compatibility[String(view || '')] || 'trading-v3-overview');
    };
    function loadTradingModulePage(container, item, requestedDate) {
        var view = item.tradingView;
        var modulePage = item.modulePage === 'v2' ? 'v2' : 'v3';
        var moduleRequestedDate = requestedDate || currentDateValue();
        if (moduleRequestedDate === currentDateValue()) moduleRequestedDate = recommendationDateValue();
        var frameId = 'tradeModuleFrame-' + item.id.replace(/[^a-z0-9_-]/gi, '-');
        var moduleLabel = modulePage === 'v3' ? '统一决策、研究验证与模拟账本' : '共享运行证据';
        container.innerHTML = '<div class="trade-module-page">' +
            '<section class="trade-module-head"><span>交易策略 / ' + moduleLabel + '</span><h2>' + escHtml(item.label) + '</h2>' +
            '<p>本页使用当前生产决策、模拟账本与验收证据，不再读取旧版选股结论。</p></section>' +
            '<iframe id="' + frameId + '" class="trade-full-frame" title="' + escHtml(item.label) + '" data-module-page="' + modulePage + '" data-pending-view="' + view + '" data-requested-date="' + escAttr(moduleRequestedDate) + '" onload="tradingDeskFrameLoaded(this)" scrolling="auto"></iframe>' +
            '</div>';
        var frame = document.getElementById(frameId);
        if (frame) window.loadTradingModuleFrame(frame);
    }
    function renderTradingDesk(container, result) {
        var readiness = result.readiness.data || {};
        var executionReadiness = result.executionReadiness.data || {};
        var context = result.context.data || {};
        var lineage = result.lineage.data || {};
        var overview = result.overview.data || {};
        var run = overview.run || {};
        var portfolio = run.portfolio || {};
        var targets = Array.isArray(lineage.targets) ? lineage.targets : [];
        var account = result.account.data || {};
        var currentPositions = Array.isArray(result.positions.data) ? result.positions.data : [];
        var orders = Array.isArray(lineage.orders) ? lineage.orders : [];
        var fills = Array.isArray(lineage.fills) ? lineage.fills : [];
        var lots = Array.isArray(lineage.lots) ? lineage.lots : [];
        var forecasts = Array.isArray(portfolio.rejected) ? portfolio.rejected : [];
        var tradeDate = String(run.trade_date || (targets[0] || {}).trade_date || '');
        var buyOrders = orders.filter(function (row) { return String(row.side || '').toUpperCase() === 'BUY'; });
        var buyFills = fills.filter(function (row) { return String(row.side || '').toUpperCase() === 'BUY'; });
        var openStatuses = { QUEUED: 1, SUBMITTED: 1, PENDING: 1, RISK_APPROVED: 1, PARTIALLY_FILLED: 1 };
        var visibleOrders = buyOrders;
        var visibleFills = buyFills;
        var targetByCode = {};
        targets.forEach(function (row) { targetByCode[tradingDeskCode(row.stock_code)] = row; });
        var orderByCode = tradingDeskLatestByCode(visibleOrders, 'created_at');
        var fillByCode = tradingDeskLatestByCode(visibleFills, 'filled_at');
        var positionsByCode = {};
        lots.forEach(function (row) {
            var code = tradingDeskCode(row.stock_code);
            if (!positionsByCode[code]) positionsByCode[code] = { rows: [], quantity: 0, costValue: 0, name: tradingDeskName(row, '') };
            var quantity = Number(row.remaining_quantity != null ? row.remaining_quantity : row.quantity) || 0;
            var costPrice = Number(row.cost_price != null ? row.cost_price : (row.average_cost != null ? row.average_cost : (row.open_price != null ? row.open_price : row.price))) || 0;
            positionsByCode[code].rows.push(row);
            positionsByCode[code].quantity += quantity;
            positionsByCode[code].costValue += quantity * costPrice;
        });
        var pipelineCodes = [];
        function addPipelineCode(code) { code = tradingDeskCode(code); if (code && pipelineCodes.indexOf(code) < 0) pipelineCodes.push(code); }
        targets.forEach(function (row) { addPipelineCode(row.stock_code); });
        visibleOrders.forEach(function (row) { addPipelineCode(row.stock_code); });
        visibleFills.forEach(function (row) { addPipelineCode(row.stock_code); });
        lots.forEach(function (row) { addPipelineCode(row.stock_code); });

        var openBuyCount = visibleOrders.filter(function (row) { return openStatuses[String(row.status || '').toUpperCase()]; }).length;
        var currentPositionCodes = {};
        currentPositions.forEach(function(row) { if (Number(row.remaining_quantity != null ? row.remaining_quantity : row.quantity) > 0) currentPositionCodes[tradingDeskCode(row.stock_code)] = true; });
        var currentPositionCount = Object.keys(currentPositionCodes).length;
        var latestEquity = account.latest_equity || {};
        var cash = account.cash_balance;
        var equity = latestEquity.total_equity != null ? latestEquity.total_equity : cash;
        var contextStatus = String(context.decision_status || '').toUpperCase();
        var dataStatus = String(context.data_status || '').toUpperCase();
        var contextRunStatus = String(context.run_status || run.status || '').toUpperCase();
        var truthCode = tradingDecisionTruth(context);
        var contextLoading = truthCode === 'LOADING';
        var failures = [], criticalFailures = [], lineageFailures = [];
        Object.keys(result).forEach(function (key) { if (result[key] && result[key].error) { var failure=key + '：' + result[key].error;failures.push(failure);if (key === 'context' || key === 'overview') criticalFailures.push(failure);if (key === 'lineage') lineageFailures.push(failure); } });
        if (!contextLoading && context.run_uid && run.run_uid && context.run_uid !== run.run_uid) { failures.push('批次不一致：context 与 overview 的 run_uid 不同');criticalFailures.push('批次不一致'); }
        var lotCloseEvidence = lineage.lot_close_evidence || {}, lotCloseStatus = String((lineage.summary || {}).lot_close_evidence_status || lotCloseEvidence.status || 'NO_SELL_FILL').toUpperCase();
        if (lotCloseStatus === 'INCOMPLETE') { failures.push('Lot 关闭证据：INCOMPLETE；缺失 fill ' + String((lotCloseEvidence.incomplete_sell_fill_ids || []).join(', ') || '未标识'));lineageFailures.push('Lot 关闭证据不完整'); }
        var decisionScope = String(context.decision_scope || 'RESEARCH_ONLY').toUpperCase();
        var contextStale = truthCode === 'STALE';
        var contextUnavailable = criticalFailures.length > 0 || truthCode === 'UNAVAILABLE';
        var lineageUnavailable = lineageFailures.length > 0;
        var contextBlocked = truthCode === 'BLOCKED';
        var projectedTargetCount = Number(context.target_count);
        var lineageTargetCount = Number((lineage.summary || {}).target_count);
        if (!lineageUnavailable && (truthCode === 'READY' || truthCode === 'EMPTY') && (targets.length !== projectedTargetCount || (Number.isFinite(lineageTargetCount) && lineageTargetCount !== projectedTargetCount))) {
            failures.push('决策整体性：context 与 lineage 的目标数不一致');
            lineageFailures.push('目标账本计数不一致');
            lineageUnavailable = true;
        }
        var paperAuthority = String(context.paper_order_authority || '').toUpperCase();
        var executionAuthority = String(context.execution_authority || '').toUpperCase();
        var paperExecutable = !contextUnavailable && !contextStale && !contextLoading && !contextBlocked && !lineageUnavailable && truthCode === 'READY' && decisionScope !== 'RESEARCH_ONLY' && readiness.execution_ready === true && executionReadiness.ready_for_new_positions === true && paperAuthority === 'V2_GATED' && executionAuthority === 'V2_CANONICAL_LEDGER';
        var conclusion = '';
        if (contextLoading) conclusion = '决策批次仍在生成，当前内容不是最终交易结论';
        else if (contextUnavailable) conclusion = '关键决策数据不可用，不能把当前页面解释为“没有机会”或“保持现金”';
        else if (contextStale) conclusion = '决策会话日与请求上下文不匹配，或证据已经过期，当前快照只供历史复核';
        else if (contextBlocked) conclusion = '决策门禁已阻断，当前不会创建新的模拟买单';
        else if (lineageUnavailable) conclusion = '研究决策可读，但同批次执行血缘不可用，不能判断是否已委托、成交或持仓';
        else if (decisionScope === 'RESEARCH_ONLY' && targets.length) conclusion = '本轮形成 ' + targets.length + ' 只研究目标，但 RESEARCH_ONLY 不拥有订单权限';
        else if (readiness.execution_ready === false || executionReadiness.ready_for_new_positions === false) conclusion = '研究决策可读，但统一模拟执行门禁已阻断，当前不会创建新的模拟买单';
        else if (!run.run_uid) conclusion = '还没有最新策略决策，当前不会创建新的模拟买单';
        else if (truthCode === 'EMPTY' && currentPositionCount) conclusion = '本轮明确无新增研究目标；当前模拟账户快照仍持有 ' + currentPositionCount + ' 只仓位';
        else if (truthCode === 'EMPTY') conclusion = '已验证完整批次且无研究目标，当前保持现金';
        else if (!targets.length) conclusion = '目标账本与服务端决策真值不一致，禁止解释为空仓';
        else if (!paperExecutable) conclusion = '策略产出 ' + targets.length + ' 只目标，但仍需通过统一模拟执行门禁';
        else if (!visibleOrders.length && !visibleFills.length) conclusion = '策略选出 ' + targets.length + ' 只股票，尚未创建模拟买单';
        else if (!visibleFills.length) conclusion = '策略选出 ' + targets.length + ' 只，已有 ' + visibleOrders.length + ' 笔模拟买单等待处理';
        else conclusion = '本轮选出 ' + targets.length + ' 只，已模拟成交 ' + visibleFills.length + ' 笔，当前持有 ' + currentPositionCount + ' 只';

        var h = '<div class="trade-desk">';
        if (criticalFailures.length) truthCode = 'UNAVAILABLE';
        h += '<section class="trade-context-light" data-state="' + truthCode + '"><div><b>' + truthCode + '</b><span>统一决策上下文与权限边界</span></div><dl><div><dt>页面请求日</dt><dd>' + escHtml(context.requested_date || '-') + '</dd></div><div><dt>决策会话日</dt><dd>' + escHtml(context.decision_session_date || context.requested_date || '-') + '</dd></div><div><dt>特征数据日</dt><dd>' + escHtml(context.data_date || tradeDate || '-') + '</dd></div><div><dt>预期数据日</dt><dd>' + escHtml(context.expected_data_date || context.data_date || tradeDate || '-') + '</dd></div><div><dt>run_uid</dt><dd>' + escHtml(context.run_uid || run.run_uid || '-') + '</dd></div><div><dt>decision_at</dt><dd>' + escHtml(context.decision_at || run.decision_at || '-') + '</dd></div><div><dt>evidence_as_of</dt><dd>' + escHtml(context.evidence_as_of || '-') + '</dd></div><div><dt>valid_until</dt><dd>' + escHtml(context.valid_until || '-') + '</dd></div></dl><div class="trade-authority-light"><span class="research">研究：' + (contextLoading ? '等待决策完成' : contextUnavailable ? '不可用' : contextStale ? '历史复核' : contextBlocked ? '门禁阻断' : '可读') + '</span><span class="paper">模拟：' + (paperExecutable ? '可入队，成交前复验' : contextLoading ? '等待批次完成' : contextUnavailable || contextStale || contextBlocked || truthCode === 'EMPTY' ? '不可入队' : decisionScope === 'RESEARCH_ONLY' ? 'RESEARCH_ONLY' : '执行门禁阻断') + '</span><span class="real">真实：固定关闭</span></div></section>';
        h += '<section class="trade-hero ' + (targets.length ? 'has-target' : 'cash') + '">';
        h += '<div><span class="trade-eyebrow">LATEST DECISION · ' + escHtml(tradeDate || '等待首个决策') + '</span><h2>' + escHtml(conclusion) + '</h2>';
        h += '<p>这里只展示实际链路：策略入选不等于已经买入；出现模拟成交或当前持仓，才代表模拟盘真正买过。</p></div>';
        h += '<div class="trade-mode"><i></i><strong>仅模拟交易</strong><span>真实下单固定关闭</span></div></section>';
        if (failures.length) h += '<div class="trade-warning"><strong>部分数据暂时不可用</strong><span>' + escHtml(failures.join('；')) + '</span></div>';
        h += '<section class="trade-kpis">';
        h += '<button type="button" onclick="openTradingV2Module(\'tomorrow\')" aria-label="打开明日动作页面"><span>研究目标</span><strong>' + (lineageUnavailable ? '—' : targets.length + '<small>只</small>') + '</strong><p>' + (lineageUnavailable ? '同批次血缘不可用' : paperExecutable ? '可进入执行复验' : '不可直接下单') + '</p></button>';
        h += '<button type="button" onclick="openTradingV2Module(\'orders\')" aria-label="打开订单与成交页面"><span>待处理买单</span><strong>' + (lineageUnavailable ? '—' : openBuyCount + '<small>笔</small>') + '</strong><p>' + (lineageUnavailable ? 'UNAVAILABLE' : '尚未完成模拟成交') + '</p></button>';
        h += '<button type="button" onclick="openTradingV2Module(\'orders\')" aria-label="打开订单与成交页面"><span>本轮模拟成交</span><strong>' + (lineageUnavailable ? '—' : visibleFills.length + '<small>笔</small>') + '</strong><p>' + (lineageUnavailable ? 'UNAVAILABLE' : escHtml(tradeDate || '最新决策日')) + '</p></button>';
        h += '<button type="button" onclick="openTradingV2Module(\'positions\')" aria-label="打开当前持仓页面"><span>当前模拟持仓</span><strong>' + currentPositionCount + '<small>只</small></strong><p>当前账户态，不随历史决策日回放</p></button>';
        h += '<button type="button" onclick="openTradingV2Module(\'trust\')" aria-label="打开系统可信度页面"><span>模拟账户可用现金</span><strong class="money">' + tradingDeskMoney(cash) + '</strong><p>账户权益 ' + tradingDeskMoney(equity) + '</p></button></section>';

        h += '<section class="trade-panel trade-primary trade-scroll-target" id="tradePipeline"><div class="trade-panel-head"><div><span>从策略到模拟盘</span><h3>这只股票走到哪一步了</h3></div><p>入选 → 委托 → 成交 → 持仓</p></div><div class="trade-table-wrap"><table class="trade-table"><thead><tr><th>股票</th><th>策略为什么选</th><th>① 策略入选</th><th>② 模拟委托</th><th>③ 模拟成交</th><th>④ 当前持仓</th><th>当前结论</th></tr></thead><tbody>';
        if (!pipelineCodes.length) {
            h += contextLoading
                ? tradingDeskEmpty(7, '决策血缘仍在生成', '批次完成前不能据此判断没有入选、委托、成交或持仓。')
                : contextUnavailable || lineageUnavailable
                    ? tradingDeskEmpty(7, '同批次决策血缘不可用', '请求失败或批次不完整，不能据此判断没有入选、委托、成交或持仓。')
                : contextBlocked
                    ? tradingDeskEmpty(7, '当前批次被门禁阻断', '没有进入模拟执行链路；这不是可执行的空仓结论。')
                    : contextStale
                        ? tradingDeskEmpty(7, '历史快照没有链路记录', '结果只对应实际数据日期，不代表当前执行状态。')
                        : tradingDeskEmpty(7, '当前链路里没有股票', '同批次确认没有入选、模拟买单、模拟成交或持仓。');
        } else {
            pipelineCodes.forEach(function (code) {
                var target = targetByCode[code] || {};
                var order = orderByCode[code] || {};
                var fill = fillByCode[code] || {};
                var position = positionsByCode[code] || {};
                var name = tradingDeskName(target, tradingDeskName(order, tradingDeskName(fill, position.name || code)));
                var selected = !!target.stock_code;
                var ordered = !!order.stock_code;
                var filled = !!fill.stock_code;
                var held = Number(position.quantity || 0) > 0;
                var orderStatus = String(order.status || '').toUpperCase();
                var conclusionText = held ? '已模拟买入并持有' : filled ? '本轮已有模拟成交' : ordered && openStatuses[orderStatus] ? '已下模拟买单，等待成交' : ordered ? '模拟委托未形成持仓' : selected ? '只入选，尚未下单' : '历史模拟持仓';
                h += '<tr><td>' + tradingDeskSecurity(code, name) + '</td>';
                h += '<td><b>' + escHtml(selected ? tradingDeskStrategies(target) : tradingDeskStrategy((position.rows || [])[0] && (position.rows || [])[0].strategy_version)) + '</b><span class="trade-cell-note">' + escHtml(target.reason || '当前持仓来自历史模拟成交') + '</span></td>';
                h += '<td><span class="trade-step ' + (selected ? 'done' : '') + '">' + (selected ? '已入选' : '—') + '</span><small>' + (selected ? ('目标 ' + tradingDeskPercent(target.target_weight, 100)) : '') + '</small></td>';
                h += '<td><span class="trade-step ' + (ordered ? (openStatuses[orderStatus] ? 'current' : 'done') : '') + '">' + (ordered ? escHtml(tradingDeskStatus(order.status)) : '—') + '</span><small>' + (ordered ? ((order.quantity || 0) + ' 股 · ¥' + tradingDeskPrice(order.limit_price)) : '') + '</small></td>';
                h += '<td><span class="trade-step ' + (filled ? 'done' : '') + '">' + (filled ? '已成交' : '—') + '</span><small>' + (filled ? ((fill.quantity || 0) + ' 股 · ¥' + tradingDeskPrice(fill.price)) : '') + '</small></td>';
                h += '<td><span class="trade-step ' + (held ? 'done' : '') + '">' + (held ? (position.quantity + ' 股') : '—') + '</span><small>' + (held ? ('成本 ¥' + tradingDeskPrice(position.costValue / position.quantity)) : '') + '</small></td>';
                h += '<td><strong class="trade-result ' + (held || filled ? 'bought' : ordered ? 'waiting' : '') + '">' + escHtml(conclusionText) + '</strong></td></tr>';
            });
        }
        h += '</tbody></table></div></section>';

        h += '<div class="trade-columns"><section class="trade-panel trade-scroll-target" id="tradeSelections"><div class="trade-panel-head"><div><span>策略选了什么</span><h3>最新组合入选清单</h3></div><p>' + targets.length + ' 只</p></div><div class="trade-table-wrap"><table class="trade-table"><thead><tr><th>#</th><th>股票</th><th>入选策略</th><th>目标仓位 / 股数</th><th>扣费后预期</th><th>入选依据</th></tr></thead><tbody>';
        if (!targets.length) {
            h += contextLoading
                ? tradingDeskEmpty(6, '研究目标仍在生成', '批次完成前，目标为 0 不是空仓结论。')
                : contextUnavailable || lineageUnavailable
                    ? tradingDeskEmpty(6, '目标血缘不可用', '请求失败或批次不完整，不代表本轮没有研究目标。')
                : contextBlocked
                    ? tradingDeskEmpty(6, '当前批次被门禁阻断', '目标为 0 不能解释为可执行的空仓结论。')
                    : contextStale
                        ? tradingDeskEmpty(6, '历史快照没有研究目标', '结果只对应实际数据日期，不代表当前决策。')
                        : tradingDeskEmpty(6, '本轮策略没有选出股票', '同批次数据完整，这是明确的空仓结论。');
        } else {
            targets.forEach(function (row, index) {
                h += '<tr><td>' + (row.rank_no || index + 1) + '</td><td>' + tradingDeskSecurity(row.stock_code, tradingDeskName(row, '')) + '</td><td><b>' + escHtml(tradingDeskStrategies(row)) + '</b></td><td><strong>' + tradingDeskPercent(row.target_weight, 100) + '</strong><small>' + (row.target_quantity || 0) + ' 股</small></td><td><strong class="trade-positive">' + tradingDeskPercent(row.expected_return_net_pct, 1) + '</strong></td><td><span class="trade-cell-note wide">' + escHtml(row.reason || '通过策略与组合约束') + '</span></td></tr>';
            });
        }
        h += '</tbody></table></div></section>';

        var recentTradeRows = [];
        buyOrders.slice(0, 20).forEach(function (row) { recentTradeRows.push({ type: 'order', at: row.created_at || '', row: row }); });
        buyFills.slice(0, 20).forEach(function (row) { recentTradeRows.push({ type: 'fill', at: row.filled_at || '', row: row }); });
        recentTradeRows.sort(function (a, b) { return String(b.at).localeCompare(String(a.at)); });
        recentTradeRows = recentTradeRows.slice(0, 20);
        h += '<section class="trade-panel trade-scroll-target" id="tradeBuys"><div class="trade-panel-head"><div><span>模拟买了什么</span><h3>最近买入委托与成交</h3></div><p>最近 20 条</p></div><div class="trade-table-wrap"><table class="trade-table"><thead><tr><th>时间</th><th>股票</th><th>动作</th><th>数量</th><th>价格</th><th>结果</th></tr></thead><tbody>';
        if (!recentTradeRows.length) {
            h += contextLoading
                ? tradingDeskEmpty(6, '订单与成交血缘仍在生成', '批次完成前，缺少记录不代表模拟盘为空。')
                : contextUnavailable || lineageUnavailable
                    ? tradingDeskEmpty(6, '同批次订单与成交不可用', '请求失败或批次不完整，不能据此判断模拟盘没有买入记录。')
                : contextBlocked
                    ? tradingDeskEmpty(6, '当前批次未进入模拟买入', '门禁已阻断；这里不把缺少记录解释为正常空态。')
                    : contextStale
                        ? tradingDeskEmpty(6, '历史快照没有买入记录', '结果只对应实际数据日期，不代表当前模拟盘。')
                        : tradingDeskEmpty(6, '模拟盘还没有买入记录', '策略入选后也可能因为风控、价格或时段条件而不下单。');
        } else {
            recentTradeRows.forEach(function (item) {
                var row = item.row;
                var target = targetByCode[tradingDeskCode(row.stock_code)] || {};
                h += '<tr><td>' + tradingDeskTime(item.at) + '</td><td>' + tradingDeskSecurity(row.stock_code, tradingDeskName(row, tradingDeskName(target, ''))) + '</td><td><span class="trade-kind ' + item.type + '">' + (item.type === 'fill' ? '模拟成交' : '模拟委托') + '</span></td><td>' + (row.quantity || 0) + ' 股</td><td>¥' + tradingDeskPrice(item.type === 'fill' ? row.price : row.limit_price) + '</td><td><strong>' + escHtml(item.type === 'fill' ? '已买入' : tradingDeskStatus(row.status)) + '</strong><small>' + escHtml(row.waiting_reason ? tradingDeskStatus(row.waiting_reason) : '') + '</small></td></tr>';
            });
        }
        h += '</tbody></table></div></section></div>';

        h += '<section class="trade-panel trade-account trade-scroll-target" id="tradeAccount"><div class="trade-panel-head"><div><span>当前模拟账户快照</span><h3>账户资金与当前持仓摘要</h3></div><p>不随历史 trade_date 回放 · ' + escHtml(account.account_name || account.account_id || '统一模拟账户') + '</p></div>';
        h += '<div class="trade-account-grid"><div><span>可用现金</span><strong>' + tradingDeskMoney(cash) + '</strong></div><div><span>账户总权益</span><strong>' + tradingDeskMoney(equity) + '</strong></div><div><span>当前持仓</span><strong>' + currentPositionCount + ' 只</strong></div></div></section>';

        var targetCodes = Object.keys(targetByCode);
        var rejected = forecasts.filter(function (row) { return targetCodes.indexOf(tradingDeskCode(row.stock_code)) < 0; }).slice(0, 12);
        h += '<details class="trade-panel trade-rejected"><summary><span><b>为什么其他候选没买</b><small>展示前 12 条未进入组合的策略判断</small></span><em>' + rejected.length + ' 条</em></summary><div class="trade-table-wrap"><table class="trade-table"><thead><tr><th>股票</th><th>策略</th><th>结论</th><th>原因</th></tr></thead><tbody>';
        if (!rejected.length) h += contextLoading
            ? tradingDeskEmpty(4, '拒绝证据仍在生成', '批次完成前不能据此判断没有拒绝记录。')
            : contextUnavailable || lineageUnavailable
                ? tradingDeskEmpty(4, '拒绝证据不可用', '请求失败或批次不完整，不代表没有拒绝记录。')
            : contextStale
                ? tradingDeskEmpty(4, '历史快照暂无未入选明细', '结果只对应实际数据日期。')
                : tradingDeskEmpty(4, '暂无未入选明细', '同批次数据完整，当前决策没有可展示的拒绝记录。');
        else rejected.forEach(function (row) { h += '<tr><td>' + tradingDeskSecurity(row.stock_code, tradingDeskName(row, '')) + '</td><td>' + escHtml(tradingDeskStrategy(row.strategy_key || row.primary_strategy_key)) + '</td><td>' + escHtml(tradingDeskStatus(row.forecast_status || row.status || 'REJECTED')) + '</td><td><span class="trade-cell-note wide">' + escHtml((row.reasons || []).join('；') || row.reason || tradingDeskStatus(row.reason_code) || '未通过组合约束') + '</span></td></tr>'; });
        h += '</tbody></table></div></details>';
        h += '<footer class="trade-foot"><span>决策批次 ' + escHtml(run.run_uid || '—') + '</span><span>市场状态 ' + escHtml(TRADING_REGIME_NAMES[run.dominant_regime] || localizeMachineText(run.dominant_regime || '—')) + '</span><span>风险资产上限 ' + tradingDeskPercent(run.risk_asset_cap, 100) + '</span><span>' + (paperExecutable ? '模拟可入队，成交前仍会复验' : '模拟链路存在阻断或仅研究') + '</span></footer></div>';
        container.innerHTML = h;
        setStatus('交易策略已更新');
    }
    function loadTradingDesk(container, requestedDate) {
        var paths = {
            context: '/api/v3/context?trade_date=' + encodeURIComponent(requestedDate || ''),
            readiness: '/api/v3/readiness',
            executionReadiness: '/api/v2/system/readiness',
            overview: '/api/v3/overview?compact=true&trade_date=' + encodeURIComponent(requestedDate || ''),
            account: '/api/v2/accounts/paper-main-v2',
            positions: '/api/v2/accounts/paper-main-v2/positions'
        };
        var keys = Object.keys(paths);
        return Promise.all(keys.map(function (key) { return tradingDeskRequest(paths[key]); })).then(function (values) {
            var result = {};
            keys.forEach(function (key, index) { result[key] = values[index]; });
            var context = result.context.data || {}, runUid = String(context.run_uid || '');
            if (result.context.error || !runUid) {
                result.lineage = { data:null, error:result.context.error ? '统一决策上下文不可用，无法读取同批次订单血缘' : '当前没有有效 run_uid，无法读取同批次订单血缘' };
                renderTradingDesk(container, result);
                return;
            }
            return tradingDeskRequest('/api/v3/decision-runs/' + encodeURIComponent(runUid) + '/lineage').then(function(lineage) {
                result.lineage = lineage;
                renderTradingDesk(container, result);
            });
        });
    }

    /* ===== Tabs ===== */
    var LOADERS = {
        trading: function (d, c) {
            loadTradingDesk(c, d);
        },
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
            state['_handler_fused'] = function (vid, liveRefresh) {
                if (vid === 'fused') return loadFusedTab(d, body, liveRefresh);
                if (vid === 'east') return loadEastTab(d, body);
                if (vid === 'ths') return loadThsTab(d, body);
                if (vid === 'xq') return loadXqTab(d, body);
                if (vid === 'sina') return loadSinaTab(d, body);
                return Promise.resolve(null);
            };
            if (window._hotRankAutoRefresh) clearInterval(window._hotRankAutoRefresh);
            window._hotRankLiveInFlight = false;
            window._hotRankLiveInFlight = true;
            Promise.resolve(state['_handler_fused'](prepared.activeId, false))
                .catch(function () {})
                .finally(function () { window._hotRankLiveInFlight = false; });
            window._hotRankAutoRefresh = setInterval(function () {
                if (activeTabId() !== 'fused' || !isTradingTime() || window._hotRankLiveInFlight) return;
                var handler = state['_handler_fused'];
                if (!handler) return;
                window._hotRankLiveInFlight = true;
                Promise.resolve(handler(state.fused || prepared.activeId, true))
                    .catch(function () {})
                    .finally(function () { window._hotRankLiveInFlight = false; });
            }, 1000);
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
        'market-radar': function (d, c) {
            loadMarketRadarPage(d, c);
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
        /* ── 宽基 ETF 资金监测 ── */
        'broad-etf-flow': function (d, c) {
            return loadBroadEtfFlowPage(d, c);
        },
        /* ── 主力行为 ── */
        mainforce: function (d, c) {
            loadMainforcePage(d, c);
        },
        /* ── 选股工具 ── */
        screen: function (d, c) {
            loadScreenerWorkbench(d, c);
        },
        'jq-picks': function (d, c) {
            loadJqPicksPage(c);
        },
        'strategy-center': function (d, c) {
            loadStrategyCenterPage(d, c);
        },
        'recommended': function (d, c) {
            // Ask for the selected/current day.  The API may return the latest
            // available recommendation with explicit fallback metadata, which
            // keeps a missing current-day run visible instead of hiding the
            // date mismatch by changing the request date client-side.
            loadRecommendedPage(d || currentDateValue(), c);
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
            // 量化三段式为默认视图；目标日完全无量化记录时保留旧版历史视图。
            Promise.all([
                apiGet('/daily-review?review_date=' + d),
                apiGet('/daily-review/pro?review_date=' + d),
                apiGet('/daily-review/quant?review_date=' + d)
            ]).then(function(results) {
                var res = results[0];
                var proRes = results[1];
                var quantRes = results[2] || {};
                var quantData = quantRes.data && quantRes.data.length ? quantRes.data[0] : null;

                if (quantRes.error && !quantRes.unavailable) {
                    c.innerHTML = '<div style="padding:18px 20px;background:#2b1115;border:1px solid #7f1d1d;border-radius:8px;color:#fecaca"><div style="font-weight:700;margin-bottom:8px">量化复盘读取失败</div><div style="font-size:13px">' + escHtml(quantRes.error) + '</div></div>';
                    return;
                }

                if (quantData) {
                    syncDateFromResponse(quantRes);
                    var reviewDate = quantRes.date || quantData.review_date || d;
                    var publishStatus = String(quantData.publish_status || '').toLowerCase();
                    var quality = jsonF(quantData.quality_json, {});
                    var coverage = quality.coverage || {};
                    var gates = Array.isArray(quality.gates) ? quality.gates : [];
                    var errors = Array.isArray(quality.errors) ? quality.errors.slice() : [];
                    if (!errors.length && publishStatus !== 'ready') {
                        gates.forEach(function(gate) {
                            if (String(gate.status || '').toLowerCase() === 'blocked') {
                                errors.push(gate.message || (gate.name + ' 未通过'));
                            }
                        });
                    }
                    var targetCoverage = Number(coverage.target);
                    var coverageText = Number.isFinite(targetCoverage) ? (targetCoverage * 100).toFixed(1) + '%' : '-';
                    var qualityStatus = String(quality.status || (publishStatus === 'ready' ? 'pass' : 'blocked')).toLowerCase();
                    var gateLabel = publishStatus === 'ready'
                        ? (qualityStatus === 'warn' ? '通过（有警告）' : '通过')
                        : '未通过';
                    var gateColor = publishStatus === 'ready' ? (qualityStatus === 'warn' ? '#f59e0b' : '#22c55e') : '#ef4444';
                    var cutoff = quantData.data_cutoff_at ? String(quantData.data_cutoff_at).replace('T', ' ').replace(/\.\d+$/, '') : '-';
                    var html = '<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px">';
                    html += '<span style="font-size:16px;font-weight:700;color:#e0e0e0">盘后量化复盘 | ' + escHtml(reviewDate) + '</span>';
                    html += '<div><button onclick="exportReview(\'' + escAttr(reviewDate) + '\')" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:12px">导出 Markdown</button></div></div>';
                    if (quantRes.fallback) {
                        html += '<div style="margin-bottom:12px;padding:9px 12px;border:1px solid #854d0e;background:#422006;color:#fde68a;border-radius:6px;font-size:12px">请求日 ' + escHtml(quantRes.requested_date || d) + ' 暂无量化复盘，当前显示最近可发布日 ' + escHtml(reviewDate) + '。</div>';
                    }
                    html += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px;font-size:12px">';
                    html += '<span style="padding:5px 9px;border-radius:12px;background:#172554;color:#bfdbfe">数据截止：' + escHtml(cutoff) + '</span>';
                    html += '<span style="padding:5px 9px;border-radius:12px;background:#172554;color:#bfdbfe">目标覆盖：' + escHtml(coverageText) + '</span>';
                    html += '<span style="padding:5px 9px;border-radius:12px;background:#111827;color:' + gateColor + ';border:1px solid ' + gateColor + '">质量门禁：' + escHtml(gateLabel) + '</span>';
                    html += '</div>';

                    if (publishStatus === 'ready') {
                        html += '<div style="background:#111827;border-radius:8px;padding:22px 24px;font-size:14px;line-height:1.75;color:#cbd5e1;font-family:system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">';
                        html += _renderProReview(quantData.compact_review || '');
                        html += '</div>';
                    } else {
                        html += '<div style="background:#2b1115;border:1px solid #7f1d1d;border-radius:8px;padding:18px 20px;color:#fecaca">';
                        html += '<div style="font-size:15px;font-weight:700;margin-bottom:10px">本日复盘未发布：质量门禁未通过</div>';
                        if (errors.length) {
                            html += '<ul style="margin:0;padding-left:20px;line-height:1.8">';
                            errors.forEach(function(message) { html += '<li>' + escHtml(message) + '</li>'; });
                            html += '</ul>';
                        } else {
                            html += '<div>未记录具体门禁原因，请重新生成并检查数据完整性。</div>';
                        }
                        html += '<button onclick="genReviewBtn(\'' + escAttr(reviewDate) + '\')" style="margin-top:14px;padding:7px 16px;border:none;border-radius:6px;background:#b91c1c;color:#fff;cursor:pointer">重新生成</button></div>';
                    }
                    c.innerHTML = html;
                    return;
                }

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
        portfolio: function (d, c, options) {
            options = options || {};
            window._pfLoadToken = (Number(window._pfLoadToken) || 0) + 1;
            var portfolioLoadToken = window._pfLoadToken;
            if (options.force) {
                window._pfManualRefreshToken = (Number(window._pfManualRefreshToken) || 0) + 1;
            }
            function pfNumberOrNull(v) {
                if (v == null || v === '') return null;
                var n = Number(v);
                return Number.isFinite(n) ? n : null;
            }
            function pfFmtProfit(v) {
                var n = pfNumberOrNull(v);
                if (n == null) return '<span class="c-gray">-</span>';
                var cls = n >= 0 ? 'c-red' : 'c-green';
                return '<strong class="' + cls + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '</strong>';
            }
            function pfFmtFlow(v) {
                var n = pfNumberOrNull(v);
                if (n == null) return '-';
                var cls = n >= 0 ? 'c-red' : 'c-green';
                return '<span class="' + cls + '">' + (n >= 0 ? '+' : '') + fmtMoney(n) + '</span>';
            }
            function pfFlowCell(r) {
                function flowCls(level) {
                    return (level === 'strong_in' || level === 'in') ? 'c-red' : ((level === 'strong_out' || level === 'out') ? 'c-green' : 'c-gray');
                }
                if (r.flow_status === 'fresh' && r.main_net_inflow != null) {
                    if (r.flow_5m != null || r.flow_attitude_label) {
                        var label = r.flow_attitude_label || '分钟';
                        var level = r.flow_attitude || 'neutral';
                        var cls = flowCls(level);
                        var ratio = r.flow_attitude_ratio != null ? ' / ' + Number(r.flow_attitude_ratio).toFixed(1) + '%' : '';
                        return '<div class="' + cls + '" title="近5分钟资金增量；1m ' + fmtMoney(Number(r.flow_1m || 0)) + ' / 15m ' + fmtMoney(Number(r.flow_15m || 0)) + '">' + label + ratio + '</div>' +
                            '<div style="font-size:10px;color:#888">今流 ' + pfFmtFlow(r.main_net_inflow) + ' · 5m ' + pfFmtFlow(r.flow_5m) + '</div>';
                    }
                    return '<div class="c-gray" title="今日资金快照已到达；正在积累5分钟变化基线">基线建立中</div>' +
                        '<div style="font-size:10px;color:#888">今流 ' + pfFmtFlow(r.main_net_inflow) + ' · 5m -</div>';
                }
                if (r.flow_status === 'closed' && r.main_net_inflow != null) {
                    var closeLabel = r.flow_attitude_label || '中性';
                    var closeRatio = r.flow_attitude_ratio != null ? ' / ' + Number(r.flow_attitude_ratio).toFixed(1) + '%' : '';
                    var closeTime = r.flow_latest_time || r.flow_trade_date || '-';
                    return '<div class="' + flowCls(r.flow_attitude || 'neutral') + '" title="目标交易日收盘资金">' + escHtml(closeLabel) + closeRatio + '</div>' +
                        '<div style="font-size:10px;color:#888">净流 ' + pfFmtFlow(r.main_net_inflow) + ' · ' + escHtml(shortDateTimeText(closeTime)) + '</div>';
                }
                if (r.flow_status === 'stale') {
                    var staleTime = r.flow_latest_time || r.flow_trade_date || '-';
                    return '<div class="c-gray" title="该资金快照未达到当前交易日/收盘完整性要求，不参与资金态度和盯盘建议">已过期</div>' +
                        '<div style="font-size:10px;color:#b45309">' + escHtml(shortDateTimeText(staleTime)) + ' · 目标 ' + escHtml(r.expected_flow_date || '-') + '</div>';
                }
                return '<div class="c-gray" title="当前目标交易日没有可用资金数据">暂无今日资金</div>' +
                    '<div style="font-size:10px;color:#888">目标 ' + escHtml(r.expected_flow_date || '-') + '</div>';
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
                try { saved = Number(localStorage.getItem('probiga_pf_live_ms') || 3000); } catch (e) { saved = 3000; }
                return saved === 3000 ? 3000 : 1000;
            }
            function pfLiveUrl(force, refreshId) {
                if (!force) return '/api/portfolio/live';
                var url = '/api/portfolio/live?force=true';
                if (refreshId) url += '&refresh_id=' + encodeURIComponent(refreshId);
                return url + '&_=' + Date.now();
            }
            function pfIsActiveTab() {
                return activeTabId() === 'portfolio';
            }
            function pfPageIsHidden() {
                return document.visibilityState === 'hidden' || document.hidden === true;
            }
            function pfAutoRefreshDelayMs() {
                if (pfPageIsHidden() || !pfIsActiveTab() || !isTradingTime()) return 60000;
                return pfLiveIntervalMs();
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
                var s = String(source || '').trim();
                if (!s) return '来源未标注';
                if (s === 'gj_qmt') return '国金QMT';
                if (s === 'daily_kline') return '日线收盘';
                if (s === 'qmt_close_table') return '国金QMT收盘';
                if (s === 'qmt_close_archive') return '国金QMT收盘档案';
                if (s === 'current_close_table') return '收盘行情快照';
                if (s === 'current_close_archive') return '收盘行情档案';
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
                return (prefix || '行情轮询') + ' ' + (pfLiveIntervalMs() / 1000) + 's' +
                    (t ? ' · ' + t : '') +
                    ' · ' + marketText +
                    (res && res.snapshot_stale ? ' · 正在刷新，暂用上次数据' : '') +
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
            function pfIsTransientRequestError(error) {
                if (error && error.cancelled) return false;
                var status = Number(error && error.httpStatus);
                if (error && error.invalidJson && (!status || status === 200)) return true;
                return !status || status === 408 || status === 425 || status === 429 || status >= 500;
            }
            function pfCancelledLoadError() {
                var error = new Error('Portfolio load superseded');
                error.cancelled = true;
                return error;
            }
            function pfWaitForRetry(waitMs) {
                return new Promise(function(resolve, reject) {
                    if (waitMs <= 0) {
                        resolve();
                        return;
                    }
                    var settled = false;
                    var timer = setTimeout(function() {
                        if (settled) return;
                        settled = true;
                        clearTimeout(cancelPoll);
                        resolve();
                    }, waitMs);
                    var cancelPoll = null;
                    function pollForCancellation() {
                        if (portfolioLoadToken === window._pfLoadToken && activeTabId() === 'portfolio') {
                            cancelPoll = setTimeout(pollForCancellation, 250);
                            return;
                        }
                        if (settled) return;
                        settled = true;
                        clearTimeout(timer);
                        clearTimeout(cancelPoll);
                        reject(pfCancelledLoadError());
                    }
                    cancelPoll = setTimeout(pollForCancellation, 250);
                });
            }
            function pfFetchPortfolioWithRetry(force) {
                // Covers a normal deployment/tunnel reconnection window while
                // keeping authentication and other 4xx failures fail-fast.
                var retryDelays = [0, 1000, 2000, 4000, 8000, 15000, 15000, 15000, 10000];
                var retryDeadline = Date.now() + 75000;
                var forceRefreshId = '';
                if (force) {
                    window._pfForceRefreshSequence = (Number(window._pfForceRefreshSequence) || 0) + 1;
                    forceRefreshId = 'pf-' + Date.now().toString(36) + '-' + window._pfForceRefreshSequence.toString(36);
                }
                function attempt(index, lastError) {
                    if (portfolioLoadToken !== window._pfLoadToken || activeTabId() !== 'portfolio') {
                        return Promise.reject(pfCancelledLoadError());
                    }
                    var waitMs = retryDelays[index];
                    if (Date.now() + waitMs >= retryDeadline) {
                        return Promise.reject(lastError || new Error('自选股重连超时'));
                    }
                    if (index > 0) {
                        var reconnectText = '服务短暂切换，正在自动重连（' + (index + 1) + '/' + retryDelays.length + '）';
                        var status = document.getElementById('pfLiveStatus');
                        if (status) pfSetLiveStatusText(reconnectText, false);
                        else if (!document.getElementById('pfTable')) {
                            c.innerHTML = '<div class="loading">' + escHtml(reconnectText) + '</div>';
                        }
                    }
                    return pfWaitForRetry(waitMs).then(function() {
                        if (portfolioLoadToken !== window._pfLoadToken || activeTabId() !== 'portfolio') {
                            throw pfCancelledLoadError();
                        }
                        var remainingMs = retryDeadline - Date.now();
                        if (remainingMs <= 0) throw (lastError || new Error('自选股重连超时'));
                        return fetchRawJsonWithTimeout(
                            pfLiveUrl(!!force, forceRefreshId),
                            Math.min(12000, remainingMs),
                            {cache:'no-store'}
                        );
                    }).then(function(payload) {
                        if (!payload || payload.error || !Array.isArray(payload.data)) {
                            var payloadError = new Error(
                                (payload && payload.error) || '自选股响应无效'
                            );
                            payloadError.httpStatus = payload && payload.retryable === false ? 400 : 503;
                            throw payloadError;
                        }
                        return payload;
                    }).catch(function(error) {
                        if (
                            index + 1 >= retryDelays.length ||
                            Date.now() >= retryDeadline ||
                            !pfIsTransientRequestError(error)
                        ) throw error;
                        return attempt(index + 1, error);
                    });
                }
                return attempt(0, null);
            }
            window.pfFetchPortfolioWithRetry = pfFetchPortfolioWithRetry;
            function pfApplyLivePayload(res, prefix) {
                if (!res || res.error || !Array.isArray(res.data)) {
                    throw new Error((res && res.error) || '自选股行情响应无效');
                }
                var incomingCodes = res.data.map(function(item) { return String(item.stock_code || ''); }).sort();
                var renderedCodes = [].map.call(
                    document.querySelectorAll('#pfTable tbody tr[data-code]'),
                    function(row) { return String(row.getAttribute('data-code') || ''); }
                ).sort();
                if (
                    incomingCodes.join(',') !== renderedCodes.join(',') &&
                    typeof window.pfRenderPortfolio === 'function'
                ) {
                    window.pfRenderPortfolio(res, prefix);
                    return;
                }
                if (res.summary) pfUpdateSummary(res);
                res.data.forEach(pfUpdateRow);
                pfUpdateLiveStatus(res, prefix);
            }
            function pfFetchAndApplyLive(prefix) {
                if (window._pfLiveInFlight) return Promise.resolve(null);
                var requestToken = Number(window._pfManualRefreshToken) || 0;
                window._pfLiveInFlight = true;
                window._pfLastAutoRefreshAt = Date.now();
                return fetchRawJsonWithTimeout(pfLiveUrl(false), 9000, {cache:'no-store'}).then(function(res) {
                    if (requestToken === (Number(window._pfManualRefreshToken) || 0)) pfApplyLivePayload(res, prefix);
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
                var td = pfNumberOrNull(r.today_profit);
                if (td == null) return { text: '-', cls: 'c-gray pf-today-profit' };
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
                var pr = pfNumberOrNull(r.cur_price);
                var cp = pfNumberOrNull(r.cost_price);
                var chg = pfNumberOrNull(r.change_pct);
                var profit = pfNumberOrNull(r.profit);
                var profitPct = pfNumberOrNull(r.profit_pct);
                var shares = pfNumberOrNull(r.shares);
                var hasProfitPct = profitPct != null;
                var cls = profit == null ? 'c-gray' : (profit >= 0 ? 'c-red' : 'c-green');
                var chgCls = chg == null ? 'c-gray' : (chg >= 0 ? 'c-red' : 'c-green');
                var pctCls = profitPct == null ? 'c-gray' : (profitPct >= 0 ? 'c-red' : 'c-green');
                var isHolding = !!(r.is_holding || (shares != null && shares > 0));
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
                if (curEl) curEl.textContent = fmtPrice(pr);
                if (chgEl) { chgEl.textContent = chg == null ? '-' : (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%'; chgEl.className = chgCls + ' pf-chg-pct'; }
                if (costEl) costEl.textContent = fmtPrice(cp);
                if (sharesEl) sharesEl.textContent = shares == null ? '-' : String(shares);
                if (todayEl) { todayEl.textContent = tdCell.text; todayEl.className = tdCell.cls; }
                if (badgeEl) badgeEl.innerHTML = pfBadge(r);
                if (profitEl) {
                    profitEl.textContent = isHolding && profit != null ? (profit >= 0 ? '+' : '') + profit.toFixed(2) : '-';
                    profitEl.className = 'pf-profit ' + (isHolding && profit != null ? cls : 'c-gray');
                }
                if (pctEl) {
                    pctEl.textContent = isHolding && hasProfitPct ? (profitPct >= 0 ? '+' : '') + profitPct.toFixed(2) + '%' : '-';
                    pctEl.className = 'pf-profit-pct ' + (isHolding && hasProfitPct ? pctCls : 'c-gray');
                }
                if (flowEl) flowEl.innerHTML = pfFlowCell(r);
                pfSetDataMomentCell(snapEl, r);
                if (srcEl) srcEl.textContent = pfQuoteSourceText(r.quote_source) + ' / ' + pfQuoteStatusText(r);
                if (watchEl) watchEl.innerHTML = pfWatchCell(r);
            }
            window.pfUpdatePortfolioRow = pfUpdateRow;
            window.pfUpdatePortfolioSummary = pfUpdateSummary;
            window.pfSetLiveInterval = function(ms) {
                var next = Number(ms) === 3000 ? 3000 : 1000;
                try { localStorage.setItem('probiga_pf_live_ms', String(next)); } catch (e) {}
                var status = document.getElementById('pfLiveStatus');
                if (status) status.textContent = '行情轮询 ' + (next / 1000) + 's';
                if (window._pfAutoRefresh) clearTimeout(window._pfAutoRefresh);
                window._pfAutoRefresh = null;
                if (typeof window._pfStartAutoRefresh === 'function') window._pfStartAutoRefresh();
            };
            function renderPortfolio(res, statusPrefix) {
                if (!res || res.error) {
                    throw new Error((res && res.error) || '自选股响应为空');
                }
                if (!Array.isArray(res.data)) {
                    throw new Error('自选股响应缺少 data 数组');
                }
                res.total = Number.isFinite(Number(res.total)) ? Number(res.total) : res.data.length;
                var addForm = '<div style="padding:14px 16px;background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);border-radius:10px;margin-bottom:14px;border:1px solid #2a2a4a">' +
                    '<h4 style="margin:0 0 10px;color:#e0e0e0;font-size:13px">➕ 添加自选股</h4>' +
                    '<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end">' +
                    '<div><label style="font-size:11px;color:#888;display:block">股票代码</label><input id="pfCode" placeholder="000001" style="width:100px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<div><label style="font-size:11px;color:#888;display:block">成本价(元)</label><input id="pfPrice" type="number" step="0.001" placeholder="0" style="width:100px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<div><label style="font-size:11px;color:#888;display:block">股数</label><input id="pfShares" type="number" placeholder="0" style="width:80px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<div><label style="font-size:11px;color:#888;display:block">买入日期</label><input id="pfPositionDate" type="date" style="width:132px;padding:5px 8px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
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
                    '<button onclick="pfSetLiveInterval(1000)" style="padding:4px 8px;border:none;border-radius:4px;background:#7c2d12;color:#fff;cursor:pointer;font-size:11px">1s</button>' +
                    '<button onclick="pfSetLiveInterval(3000)" style="padding:4px 8px;border:none;border-radius:4px;background:#7c2d12;color:#fff;cursor:pointer;font-size:11px">3s</button>' +
                    '<span id="pfLiveStatus" style="font-size:11px;color:#9ca3af">行情轮询 ' + (pfLiveIntervalMs()/1000) + 's</span></span></div>'
                );
                var html = toolbar;

                // Build table with drag handles
                html += '<div class="table-wrap pf-table-wrap"><table id="pfTable" class="pf-table"><thead><tr>' +
                    '<th style="width:28px"></th>' +
                    '<th>代码</th><th>名称</th><th>现价</th><th title="个股行情涨跌，非您的持仓盈亏">涨跌%</th><th>成本</th><th>持有</th>' +
                    '<th title="昨日持仓×涨跌额 + 今日买入/卖出盈亏">当日盈亏</th><th title="(现价-成本)×股数">持仓盈亏</th><th title="相对成本">收益率</th>' +
                    '<th title="净流为当日累计；盘中态度按近5分钟增量，盘后按当日收盘资金；过期数据不参与判断">资金态度/净流</th><th class="pf-watch-advice-col">盯盘建议</th><th>数据时刻</th><th>来源</th><th class="pf-sticky pf-sticky-action">操作</th><th class="pf-sticky pf-sticky-analysis">分析</th><th class="pf-sticky pf-sticky-history">历史</th>' +
                    '</tr></thead><tbody>';
                res.data.forEach(function(r, idx){
                    pfRegisterWatchRow(r);
                    var pr = pfNumberOrNull(r.cur_price);
                    var cp = pfNumberOrNull(r.cost_price);
                    var chg = pfNumberOrNull(r.change_pct);
                    var profit = pfNumberOrNull(r.profit);
                    var profitPct = pfNumberOrNull(r.profit_pct);
                    var shares = pfNumberOrNull(r.shares);
                    var hasProfitPct = profitPct != null;
                    var chgCls = chg == null ? 'c-gray' : (chg>=0?'c-red':'c-green');
                    var pfCls = profit == null ? 'c-gray' : (profit>=0?'c-red':'c-green');
                    var pctCls = profitPct == null ? 'c-gray' : (profitPct>=0?'c-red':'c-green');
                    var isHolding = !!(r.is_holding || (shares != null && shares > 0));
                    var rowBg = isHolding ? 'background:#fff4e6;' : (r.is_today_cleared ? 'background:#f1f5f9;' : '');
                    var tdCell = pfFmtTodayCell(r);
                    var chgTxt = chg == null ? '-' : (chg>=0?'+':'')+chg.toFixed(2)+'%';
                    var profitTxt = isHolding && profit != null ? (profit>=0?'+':'')+profit.toFixed(2) : '-';
                    var pctTxt = isHolding && hasProfitPct ? (profitPct>=0?'+':'')+profitPct.toFixed(2)+'%' : '-';
                    var pfClsRow = isHolding ? pfCls : 'c-gray';
                    var pctClsRow = isHolding && hasProfitPct ? pctCls : 'c-gray';
                    var dataMoment = pfDataMoment(r);
                    html += '<tr id=\"pf-tr-'+r.stock_code+'\" draggable=\"true\" data-code=\"'+r.stock_code+'\" data-holding=\"'+(isHolding?'1':'0')+'\" data-today-status=\"'+(r.today_position_status||'')+'\" style=\"cursor:grab;'+rowBg+'\">' +
                        '<td style=\"text-align:center;color:#555;font-size:14px;cursor:grab\" class=\"pf-drag-handle\">⠿</td>' +
                        '<td>'+nameLink(r.stock_code, r.stock_code)+'</td>' +
                        '<td><strong>'+nameLink(r.stock_code, r.display_name)+'</strong><span class=\"pf-row-badge\">'+pfBadge(r)+'</span></td>' +
                        '<td class=\"pf-cur-price\">'+fmtPrice(pr)+'</td>' +
                        '<td class=\"'+chgCls+' pf-chg-pct\">'+chgTxt+'</td>' +
                        '<td class=\"pf-cost\">'+fmtPrice(cp)+'</td>' +
                        '<td class=\"pf-shares\">'+(shares == null ? '-' : shares)+'</td>' +
                        '<td class=\"'+tdCell.cls+'\">'+tdCell.text+'</td>' +
                        '<td class=\"pf-profit '+pfClsRow+'\">'+profitTxt+'</td>' +
                        '<td class=\"pf-profit-pct '+pctClsRow+'\">'+pctTxt+'</td>' +
                        '<td class=\"pf-main-flow\">'+pfFlowCell(r)+'</td>' +
                        '<td class=\"pf-watch-advice\">'+pfWatchCell(r)+'</td>' +
                        '<td class=\"pf-snapshot-at\" title=\"'+escAttr(dataMoment.title)+'\">'+dataMoment.html+'</td>' +
                        '<td class=\"pf-quote-source\" style=\"font-size:11px;color:#666\">'+pfQuoteSourceText(r.quote_source)+' / '+pfQuoteStatusText(r)+'</td>' +
                        '<td class=\"pf-actions pf-sticky pf-sticky-action\"><button onclick=\"event.stopPropagation();pfTransact(\''+r.stock_code+'\',\''+r.display_name+'\','+(cp == null ? 0 : cp)+','+(shares == null ? 0 : shares)+')\" style=\"padding:2px 8px;border:none;border-radius:4px;background:#388e3c;color:#fff;cursor:pointer;font-size:11px\">💰</button>' +
                        '<button onclick=\"event.stopPropagation();pfRemove(\''+r.stock_code+'\')\" style=\"padding:2px 8px;border:none;border-radius:4px;background:#c62828;color:#fff;cursor:pointer;font-size:11px;margin-left:2px\">✕</button></td>' +
                        '<td class=\"pf-sticky pf-sticky-analysis\"><button onclick=\"event.stopPropagation();pfAnalyze(\''+r.stock_code+'\',\''+r.display_name+'\')\" style=\"padding:4px 12px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer;font-size:11px\">🤖 分析</button></td>' +
                        '<td class=\"pf-sticky pf-sticky-history\"><button onclick=\"event.stopPropagation();pfHistory(\''+r.stock_code+'\',\''+r.display_name+'\')\" style=\"padding:4px 10px;border:none;border-radius:4px;background:#555;color:#ccc;cursor:pointer;font-size:11px\">📋</button></td>' +
                        '</tr>';
                });
                html += '</tbody></table></div>';
                c.innerHTML = addForm + html;
                pfUpdateLiveStatus(res, statusPrefix || '已加载');

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
                    var now = Date.now();
                    if (window._pfMarketBarInFlight || (window._pfMarketBarLastAt && now - window._pfMarketBarLastAt < 5000)) return;
                    window._pfMarketBarInFlight = true;
                    window._pfMarketBarLastAt = now;
                    fetchRawJsonWithTimeout('/api/monitor/data?_=' + now, 8000, {cache:'no-store'}).then(function(md) {
                        var bar = document.getElementById('pfMarketBar');
                        if (!bar || md.error) return;
                        var upc = Number(md.up_count || 0), dnc = Number(md.down_count || 0);
                        var flat = Number(md.flat_count || 0), narrow = Number(md.sideline_count || 0);
                        var total = Number(md.total_count || 0) || Math.max(1, upc + dnc + flat);
                        var upPct = (upc / total * 100).toFixed(0);
                        var amt = md.total_amount ? (md.total_amount / 1e8).toFixed(0) + '亿' : '-';
                        var heat = md.market_heat || 0;
                        var heatColor = heat > 600 ? '#ef4444' : heat < 400 ? '#22c55e' : '#f59e0b';
                        var chg = md.heat_change || 0;
                        var chgIcon = chg >= 0 ? '▲' : '▼';
                        var topInd = (md.top_industries || []).slice(0, 3).map(function(t){ return t.name; }).join(' · ') || '-';
                        var dataTime = String(md.data_time || md.trade_date || '');
                        var dataClock = dataTime.length > 10 ? dataTime.slice(11, 19) : dataTime;
                        var statusText = md.is_realtime ? ('● 实时 ' + dataClock) : (md.freshness_status === 'paused' ? ('午间 ' + dataClock) : ((md.freshness_status === 'fallback' ? '⚠ 回退 ' : '收盘 ') + (md.trade_date || dataClock || '-')));
                        var statusColor = md.is_realtime ? '#22c55e' : (md.freshness_status === 'fallback' ? '#f59e0b' : (md.freshness_status === 'paused' ? '#60a5fa' : '#94a3b8'));
                        var topDate = (md.top_industries && md.top_industries[0] && md.top_industries[0].trade_date) || md.trade_date || '';
                        var topLabel = topDate && topDate !== md.trade_date ? ('热门(' + topDate.slice(5) + ')') : '热门';
                        bar.innerHTML =
                            '<span style="font-weight:600;color:#e5e7eb">📊 市场 <b style="font-size:10px;color:' + statusColor + '" title="数据源 ' + escAttr(md.data_source || '-') + '">' + escHtml(statusText) + '</b></span>' +
                            '<span>上涨 <b style="color:#ef4444">' + upc + '</b></span>' +
                            '<span>平盘 <b style="color:#9ca3af">' + flat + '</b></span>' +
                            '<span>下跌 <b style="color:#22c55e">' + dnc + '</b></span>' +
                            '<span title="涨幅绝对值小于1%，与上涨/下跌有重叠">±1%内 <b style="color:#9ca3af">' + narrow + '</b></span>' +
                            '<span>红盘 <b style="color:#f59e0b">' + upPct + '%</b></span>' +
                            '<span>成交 <b style="color:#60a5fa">' + amt + '</b></span>' +
                            '<span>热度 <b style="color:' + heatColor + '">' + heat.toFixed(0) + '</b> <span style="font-size:11px;color:' + (chg>=0?'#ef4444':'#22c55e') + '">' + chgIcon + Math.abs(chg).toFixed(1) + '%</span></span>' +
                            '<span>' + escHtml(topLabel) + ' ' + escHtml(topInd) + '</span>';
                    }).catch(function(){}).finally(function(){ window._pfMarketBarInFlight = false; });
                };
                window._pfRefreshMarketBar();
            }
            window.pfRenderPortfolio = renderPortfolio;
            var portfolioLoadPromise = pfFetchPortfolioWithRetry(!!options.force)
                .then(function(res) {
                    if (portfolioLoadToken !== window._pfLoadToken || activeTabId() !== 'portfolio') {
                        return {cancelled: true};
                    }
                    return renderPortfolio(res);
                })
                .catch(function(e) {
                    if (e && e.cancelled) return {cancelled: true};
                    var message = e.message || '网络异常';
                    if (document.getElementById('pfTable')) {
                        pfSetLiveStatusText('服务暂不可用，已保留上次数据', true);
                        return {loadError: message, retained: true};
                    }
                    c.innerHTML = '<div class="loading" style="color:#e74c3c">自选股加载失败: ' + escHtml(message) + ' <button onclick="loadPortfolio()" style="margin-left:8px">重试</button></div>';
                    return {loadError: message};
                });

            // Poll quickly only while this visible tab is in a live trading session.
            // Hidden, inactive and closed-market pages keep a low-cost wake-up timer
            // solely so the server market clock can be renewed for the next session.
            if (window._pfAutoRefresh) clearTimeout(window._pfAutoRefresh);
            window._pfStartAutoRefresh = function() {
                if (window._pfAutoRefresh) clearTimeout(window._pfAutoRefresh);
                function tick() {
                    var hidden = pfPageIsHidden();
                    if (!hidden) refreshMarketClockSilently(60000);
                    if (!hidden && pfIsActiveTab() && isTradingTime()) {
                        pfFetchAndApplyLive('');
                        if (window._pfRefreshMarketBar) window._pfRefreshMarketBar();
                    }
                    window._pfAutoRefresh = setTimeout(tick, pfAutoRefreshDelayMs());
                }
                window._pfAutoRefresh = setTimeout(tick, pfAutoRefreshDelayMs());
            };
            if (!window._pfVisibilityRefreshBound) {
                document.addEventListener('visibilitychange', function() {
                    if (typeof window._pfStartAutoRefresh === 'function') window._pfStartAutoRefresh();
                });
                window._pfVisibilityRefreshBound = true;
            }
            window._pfStartAutoRefresh();
            return portfolioLoadPromise;
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
        {group:'交易决策', items:[
            {id:'trading',icon:'◎',label:'交易策略'}
        ]},
        {group:'自选管理', items:[
            {id:'portfolio',icon:'📈',label:'自选股'}
        ]},
        {group:'市场分析', items:[
            {id:'command',icon:'🧭',label:'智能决策'},
            {id:'intraday-battle',icon:'⚡',label:'盘中作战'},
            {id:'monitor',icon:'📺',label:'市场监控中心'},
            {id:'sector-movement',icon:'🌊',label:'板块异动'},
            {id:'market-radar',icon:'📡',label:'异动雷达'},
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
            {id:'screen',icon:'🎯',label:'选股工作台'},
            {id:'strategy-center',icon:'🏆',label:'动态策略竞技场'},
            {id:'review',icon:'📋',label:'复盘数据'},
            {id:'sector-heat',icon:'🌡',label:'板块热度'},
            {id:'sim-trade',icon:'🤖',label:'旧模拟交易（归档）'}
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
            {id:'broad-etf-flow',icon:'🏛',label:'宽基资金监测'},
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
        ]},
        {group:'AI 问答', items:[
            {id:'ai-stock',icon:'📈',label:'股票问答'},
            {id:'ai-general',icon:'💬',label:'通用问答'}
        ]}
    ];
    var LAYOUT_NEW = [
        {group:'交易决策', items:[
            {id:'trading',icon:'◎',label:'交易策略'},
            {id:'strategy-backtest',icon:'📊',label:'策略回测'}
        ]},
        {group:'自选管理', items:[
            {id:'portfolio',icon:'📈',label:'自选股'}
        ]},
        {group:'市场概览', items:[
            {id:'command',icon:'🧭',label:'智能决策'},
            {id:'intraday-battle',icon:'⚡',label:'盘中作战'},
            {id:'monitor',icon:'📺',label:'市场监控'},
            {id:'review',icon:'📋',label:'每日复盘'},
            {id:'fused',icon:'📊',label:'热股排行'},
            {id:'sector',icon:'🌊',label:'板块分析'},
            {id:'market-radar',icon:'📡',label:'异动雷达'}
        ]},
        {group:'个股热度', items:[
            {id:'strong',icon:'🔥',label:'强势股'},
            {id:'concept',icon:'🏷️',label:'概念 / 行业'},
            {id:'alist',icon:'🐲',label:'龙虎榜'}
        ]},
        {group:'资金流向', items:[
            {id:'capital',icon:'💰',label:'个股资金'},
            {id:'broad-etf-flow',icon:'🏛',label:'宽基资金'},
            {id:'mainforce',icon:'🔍',label:'主力行为'}
        ]},
        {group:'研究工具', items:[
            {id:'screen',icon:'🎯',label:'旧选股工作台（研究）'},
            {id:'strategy-center',icon:'🏆',label:'动态策略竞技场'}
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
        ]},
        {group:'AI 问答', items:[
            {id:'ai-stock',icon:'📈',label:'股票问答'},
            {id:'ai-general',icon:'💬',label:'通用问答'}
        ]}
    ];
    var TRADING_MODULE_NAV_ITEMS = [
        {id:'trading-v3-overview', modulePage:'v3', tradingView:'overview', icon:'01', label:'今日策略'},
        {id:'trading-v3-positions', modulePage:'v3', tradingView:'positions', icon:'02', label:'我的持仓'},
        {id:'trading-v3-candidates', modulePage:'v3', tradingView:'candidates', icon:'03', label:'策略池'},
        {id:'trading-v3-intraday', modulePage:'v3', tradingView:'intraday', icon:'04', label:'盘中应急'},
        {id:'trading-v3-hypotheses', modulePage:'v3', tradingView:'hypotheses', icon:'05', label:'连续跟踪'}
    ];
    TRADING_MODULE_NAV_ITEMS.forEach(function(item) {
        LOADERS[item.id] = function(d, container) {
            if (item.decisionCockpit) {
                loadDecisionCockpitPage(d, container);
                return;
            }
            if (item.candidateDecision) {
                loadCandidateDecisionPage(d, container);
                return;
            }
            if (item.candidateCenter) {
                loadCandidateCenterPage(d, container);
                return;
            }
            loadTradingModulePage(container, item, d);
        };
    });
    function installTradingModuleNavigation(layout) {
        if (!layout[0] || !layout[0].items) return;
        var items = layout[0].items;
        var tradingIndex = items.findIndex(function(item) { return item.id === 'trading'; });
        if (tradingIndex < 0 || items.some(function(item) { return !!item.tradingView; })) return;
        var subItems = TRADING_MODULE_NAV_ITEMS.map(function(item) {
            return { id:item.id, modulePage:item.modulePage, tradingView:item.tradingView, decisionCockpit:item.decisionCockpit, candidateDecision:item.candidateDecision, candidateCenter:item.candidateCenter, tradingSection:item.tradingSection, icon:item.icon, label:item.label };
        });
        items.splice.apply(items, [tradingIndex + 1, 0].concat(subItems));
    }
    installTradingModuleNavigation(LAYOUT_OLD);
    installTradingModuleNavigation(LAYOUT_NEW);

    function ensureLayoutItem(layout, groupIndex, item) {
        if (!layout[groupIndex] || !layout[groupIndex].items) return;
        var exists = layout[groupIndex].items.some(function(it){ return it.id === item.id; });
        if (!exists) layout[groupIndex].items.push(item);
    }
    if (typeof PAGE_TITLES !== 'undefined') {
        PAGE_TITLES['trading'] = '◎ 交易策略';
        PAGE_TITLES['intraday-battle'] = '⚡ 盘中作战';
        PAGE_TITLES['strategy-backtest'] = '📊 策略回测';
        PAGE_TITLES['research-radar'] = '🧭 研报趋势雷达';
        PAGE_TITLES['market-radar'] = '📡 异动雷达';
        PAGE_TITLES['broad-etf-flow'] = '🏛 宽基资金监测';
        PAGE_TITLES['hunter'] = '🏹 狩猎场';
        PAGE_TITLES['ai-stock'] = '📈 股票问答';
        PAGE_TITLES['ai-general'] = '💬 通用问答';
        TRADING_MODULE_NAV_ITEMS.forEach(function(item) {
            PAGE_TITLES[item.id] = item.icon + ' ' + item.label;
        });
    }
    ensureLayoutItem(LAYOUT_OLD, 3, {id:'strategy-backtest', icon:'📊', label:'策略回测'});
    var ALL_OLD_IDS = []; LAYOUT_OLD.forEach(function(g){ g.items.forEach(function(it){ ALL_OLD_IDS.push(it.id); }); });
    var ALL_NEW_IDS = []; LAYOUT_NEW.forEach(function(g){ g.items.forEach(function(it){ ALL_NEW_IDS.push(it.id); }); });

    var PRIMARY_NAV_ORDER = ['ai-stock', 'portfolio', 'trading', 'command'];
    function arrangePrimaryNavigation(layout) {
        var remaining = layout.slice();
        var ordered = [];
        PRIMARY_NAV_ORDER.forEach(function (firstItemId) {
            var index = remaining.findIndex(function (group) {
                return group.items && group.items.some(function (item) { return item.id === firstItemId; });
            });
            if (index >= 0) ordered.push(remaining.splice(index, 1)[0]);
        });
        layout.splice.apply(layout, [0, layout.length].concat(ordered, remaining));
    }
    arrangePrimaryNavigation(LAYOUT_OLD);
    arrangePrimaryNavigation(LAYOUT_NEW);

    function loadEmbeddedAiPage(container, channel) {
        var isStock = channel === 'stock';
        var route = isStock ? '/ai-stock?embedded=1' : '/ai-general?embedded=1';
        var title = isStock ? '股票问答' : '通用问答';
        container.innerHTML = '<iframe class="ai-embedded-frame" src="' + route + '" title="' + title + '" loading="eager"></iframe>';
        setStatus(title + '已打开');
    }
    LOADERS['ai-stock'] = function (d, container) { loadEmbeddedAiPage(container, 'stock'); };
    LOADERS['ai-general'] = function (d, container) { loadEmbeddedAiPage(container, 'general'); };

    var SIDEBAR_GROUP_STATE_KEY = 'probiga_sidebar_group_state_v1';
    function sidebarGroupKey(group) {
        return group && group.items && group.items[0] ? String(group.items[0].id || '') : '';
    }
    function readSidebarGroupState() {
        try {
            var parsed = JSON.parse(localStorage.getItem(SIDEBAR_GROUP_STATE_KEY) || '{}');
            return parsed && typeof parsed === 'object' ? parsed : {};
        } catch (e) {
            return {};
        }
    }
    function writeSidebarGroupState(state) {
        try { localStorage.setItem(SIDEBAR_GROUP_STATE_KEY, JSON.stringify(state || {})); } catch (e) {}
    }
    window.toggleSidebarGroup = function (button) {
        if (!button) return;
        var group = button.closest('.sidebar-group');
        if (!group) return;
        var collapsed = group.classList.toggle('collapsed');
        button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        var state = readSidebarGroupState();
        state[String(button.getAttribute('data-group-key') || '')] = collapsed;
        writeSidebarGroupState(state);
    };
    function expandSidebarGroupForItem(tabId) {
        var item = document.querySelector('[data-tab="' + tabId + '"]');
        var group = item && item.closest ? item.closest('.sidebar-group') : null;
        if (!group || !group.classList.contains('collapsed')) return;
        group.classList.remove('collapsed');
        var toggle = group.querySelector('.sidebar-group-toggle');
        if (toggle) {
            toggle.setAttribute('aria-expanded', 'true');
            var state = readSidebarGroupState();
            state[String(toggle.getAttribute('data-group-key') || '')] = false;
            writeSidebarGroupState(state);
        }
    }

    function renderSidebar(layout, activeId) {
        var sb = el('sidebar');
        var h = '<div class="sidebar-logo">Pro<span>Big</span>A</div>';
        var collapsedState = readSidebarGroupState();
        layout.forEach(function (g) {
            var groupKey = sidebarGroupKey(g);
            var containsActive = g.items.some(function (it) { return it.id === activeId; });
            var collapsed = collapsedState[groupKey] === true && !containsActive;
            var itemsId = 'sidebar-group-items-' + groupKey;
            h += '<div class="sidebar-group' + (collapsed ? ' collapsed' : '') + '" data-group-key="' + escAttr(groupKey) + '">';
            h += '<button type="button" class="sidebar-group-title sidebar-group-toggle" data-group-key="' + escAttr(groupKey) + '" aria-expanded="' + (collapsed ? 'false' : 'true') + '" aria-controls="' + escAttr(itemsId) + '" onclick="toggleSidebarGroup(this)"><span>' + escHtml(g.group) + '</span><span class="sidebar-group-chevron" aria-hidden="true">⌄</span></button>';
            h += '<div class="sidebar-group-items" id="' + escAttr(itemsId) + '">';
            var lastTradingSection = '';
            g.items.forEach(function (it) {
                if (it.tradingSection && it.tradingSection !== lastTradingSection) {
                    h += '<div class="sidebar-trading-section">' + escHtml(it.tradingSection) + '</div>';
                    lastTradingSection = it.tradingSection;
                }
                var cls = it.id === activeId ? 'sidebar-item active' : 'sidebar-item';
                var isTradingItem = !!it.tradingSection;
                if (isTradingItem) cls += ' sidebar-trading-item';
                if (it.tradingView) {
                    h += '<button class="' + cls + '" data-tab="' + it.id + '" data-trading-view="' + it.tradingView + '" data-module-page="' + (it.modulePage || 'v3') + '" onclick="openTradingModule(\'' + it.id + '\')"><span>' + it.icon + '</span>' + it.label + '</button>';
                } else if (it.href) {
                    h += '<a class="' + cls + '" href="' + it.href + '">' + it.icon + ' ' + it.label + '</a>';
                } else if (isTradingItem) {
                    h += '<button class="' + cls + '" data-tab="' + it.id + '" onclick="switchTab(\'' + it.id + '\')"><span>' + it.icon + '</span>' + it.label + '</button>';
                } else {
                    h += '<button class="' + cls + '" data-tab="' + it.id + '" onclick="switchTab(\'' + it.id + '\')">' + it.icon + ' ' + it.label + '</button>';
                }
            });
            h += '</div></div>';
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
        var firstId = 'trading';
        var hasTrading = layout.some(function (group) {
            return group.items.some(function (item) { return item.id === firstId; });
        });
        if (!hasTrading) {
            layout.some(function (group) {
                return group.items.some(function (item) {
                    if (item.href) return false;
                    firstId = item.id;
                    return true;
                });
            });
        }
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

    function stockDetailStrategyRange(plan) {
        plan = plan || {};
        var range = plan.range || {};
        if (range.low == null && range.high == null) return '-';
        if (range.low != null && range.high != null && Number(range.low) !== Number(range.high)) {
            return fmtPrice(range.low) + ' - ' + fmtPrice(range.high);
        }
        return fmtPrice(range.low != null ? range.low : range.high);
    }

    function stockDetailStrategyReason(reason) {
        var text = localizeMachineText(reason || '');
        return text
            .replace(/latest price ([0-9.]+) invalidated MA20 trend stop ([0-9.]+)/i, '最新价 $1 跌破 MA20 趋势保护位 $2')
            .replace(/latest price ([0-9.]+) breached stop loss ([0-9.]+)/i, '最新价 $1 跌破止损位 $2')
            .replace(/latest price ([0-9.]+) breached trend stop ([0-9.]+)/i, '最新价 $1 跌破趋势止损位 $2')
            .replace(/latest price ([0-9.]+) breached reduction line ([0-9.]+)/i, '最新价 $1 跌破减仓观察位 $2')
            .replace(/persisted strategy signal is 卖出提醒/i, '已持久化策略信号触发卖出提醒')
            .replace(/persisted strategy signal is REDUCE/i, '已持久化策略信号触发减仓')
            .replace(/no cutoff-eligible recommendation row/i, '截止时点没有可用的推荐证据')
            .replace(/no cutoff-eligible analysis row/i, '截止时点没有可用的分析证据')
            .replace(/no cutoff-eligible holding price/i, '截止时点没有可用的持仓价格');
    }

    function renderStockDetailExecutionStrategy(d) {
        d = d || {};
        var strategy = d.holding_strategy;
        var watch = d.watch_analysis || {};
        var holding = d.holding || {};
        var context = d.holding_strategy_context || {};
        var contextReason = String(context.reason_code || '');
        var quoteUnavailable = contextReason === 'PORTFOLIO_QUOTE_UNVERIFIED';
        var contextUnavailable = contextReason.indexOf('PORTFOLIO_') === 0 || d.portfolio_context_status === 'unavailable';
        if (d.watchlist_member !== true && !contextUnavailable) return '';
        var h = '<section id="stockDetailExecutionStrategy" style="border:1px solid #dbe4f0;background:#f8fbff;border-radius:10px;padding:14px 16px;margin-bottom:16px">';
        h += '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:12px"><div><div style="font-size:14px;font-weight:800;color:#172033">当前执行策略</div><div style="font-size:11px;color:#64748b;margin-top:3px">自选持仓连续策略 · 仅供决策参考，不会自动下单</div></div>';
        if (quoteUnavailable) {
            h += '<span style="background:#b45309;color:#fff;padding:5px 11px;border-radius:999px;font-size:12px;font-weight:700">动作冻结</span></div>';
            h += '<div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:11px 12px;color:#9a3412;font-size:13px;line-height:1.7"><strong>持仓信息已读取，但行情时效或完整性未通过校验。</strong> 页面不会基于未验证价格生成或复用买卖动作。' + (d.portfolio_snapshot_stale ? '<br>当前持仓来自最近一次缓存快照，请刷新后再复核。' : '') + '<br>状态：' + escHtml(contextReason) + '</div>';
        } else if (contextUnavailable) {
            h += '<span style="background:#b45309;color:#fff;padding:5px 11px;border-radius:999px;font-size:12px;font-weight:700">校验失败</span></div>';
            h += '<div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:11px 12px;color:#9a3412;font-size:13px;line-height:1.7"><strong>自选与持仓状态暂时无法校验，执行策略不可用。</strong> 页面已清除缓存中的旧持仓和旧动作，不会把历史策略当成当前策略。<br>状态：' + escHtml(context.reason_code || 'PORTFOLIO_CONTEXT_UNAVAILABLE') + '</div>';
        } else if (strategy) {
            var intent = String(strategy.exit_intent || 'WAIT_DATA').toUpperCase();
            var actionColor = intent === 'SELL' ? '#b91c1c' : (intent === 'REDUCE' || intent === 'WAIT_DATA' ? '#b45309' : '#166534');
            var sellPlan = strategy.sell_plan || {};
            var emergency = strategy.emergency_exit || {};
            h += '<span style="background:' + actionColor + ';color:#fff;padding:5px 11px;border-radius:999px;font-size:12px;font-weight:700">' + escHtml(strategy.action || '等待数据') + '</span></div>';
            h += '<div style="font-size:13px;line-height:1.7;color:#334155;margin-bottom:12px"><strong>判断依据：</strong>' + escHtml(stockDetailStrategyReason(strategy.reason || '证据不足，暂不形成动作')) + '</div>';
            h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:8px;margin-bottom:10px">';
            h += '<div style="background:#fff;border:1px solid #e5eaf1;border-radius:8px;padding:10px"><small style="color:#64748b">持仓 / 可卖</small><strong style="display:block;margin-top:4px">' + escHtml(String(strategy.shares == null ? holding.shares || 0 : strategy.shares)) + ' / ' + escHtml(String(strategy.sellable_shares == null ? '-' : strategy.sellable_shares)) + ' 股</strong></div>';
            h += '<div style="background:#fff;border:1px solid #e5eaf1;border-radius:8px;padding:10px"><small style="color:#64748b">卖出计划</small><strong style="display:block;margin-top:4px">' + escHtml(stockDetailStrategyRange(sellPlan)) + '</strong><span style="display:block;color:#64748b;font-size:11px;margin-top:3px">' + escHtml(sellPlan.label || '-') + '</span></div>';
            h += '<div style="background:#fff;border:1px solid #e5eaf1;border-radius:8px;padding:10px"><small style="color:#64748b">紧急退出位</small><strong style="display:block;margin-top:4px;color:' + (emergency.direct ? '#b91c1c' : '#172033') + '">' + escHtml(emergency.price == null ? '-' : fmtPrice(emergency.price)) + '</strong><span style="display:block;color:#64748b;font-size:11px;margin-top:3px">' + escHtml(emergency.label || '-') + '</span></div>';
            h += '<div style="background:#fff;border:1px solid #e5eaf1;border-radius:8px;padding:10px"><small style="color:#64748b">浮动盈亏</small><strong style="display:block;margin-top:4px">' + escHtml(strategy.pnl_pct == null ? '-' : pct(strategy.pnl_pct)) + '</strong><span style="display:block;color:#64748b;font-size:11px;margin-top:3px">成本 ' + escHtml(strategy.cost_price == null ? '-' : fmtPrice(strategy.cost_price)) + ' / 现价 ' + escHtml(strategy.latest_price == null ? '-' : fmtPrice(strategy.latest_price)) + '</span></div>';
            h += '</div>';
            h += '<div style="background:#eef4ff;border-radius:8px;padding:10px 12px;font-size:12px;line-height:1.7;color:#334155"><strong>下一交易日：</strong>' + escHtml(strategy.next_session_plan || '收盘后重新评估') + '<br><strong>买入权限：</strong>' + escHtml(((strategy.buy_plan || {}).label) || '持仓策略默认不加仓') + '</div>';
            if (strategy.market_context_status && String(strategy.market_context_status).toUpperCase() !== 'READY') {
                h += '<div style="margin-top:8px;color:#b45309;font-size:12px"><strong>市场数据门禁：</strong>' + escHtml(strategy.market_context_reason || strategy.market_context_status) + '</div>';
            }
            h += '<div style="margin-top:9px;color:#64748b;font-size:11px">策略时点 ' + escHtml(strategy.knowledge_cutoff || strategy.evaluated_at || '-') + ' · 行情日 ' + escHtml(strategy.price_trade_date || d.quote_trade_date || '-') + ' · 权限 ' + escHtml(strategy.execution_authority || context.execution_authority || 'ADVISORY_ONLY') + '</div>';
        } else if (Number(holding.shares || 0) > 0) {
            h += '<span style="background:#b45309;color:#fff;padding:5px 11px;border-radius:999px;font-size:12px;font-weight:700">等待数据</span></div>';
            h += '<div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:11px 12px;color:#9a3412;font-size:13px;line-height:1.7"><strong>持仓执行策略暂不可用。</strong> 当前不会沿用旧 AI 建议形成持有或加仓动作；已知硬止损仍应优先复核。' + (context.reason_code ? '<br>状态：' + escHtml(context.reason_code) : '') + '</div>';
        } else if (watch.operation_advice) {
            h += '<span style="background:#475569;color:#fff;padding:5px 11px;border-radius:999px;font-size:12px;font-weight:700">' + escHtml(watch.operation_advice) + '</span></div>';
            h += '<div style="font-size:13px;line-height:1.7;color:#334155"><strong>未持仓盯盘建议：</strong>' + escHtml(watch.operation_advice) + '；趋势 ' + escHtml(watch.trend || '-') + '，资金 ' + escHtml(watch.funds || '-') + '，热度 ' + escHtml(watch.heat || '-') + '。<br><strong>风险提示：</strong>' + escHtml(watch.risk_tip || '暂无明显风险') + '。该建议不构成买入授权。</div>';
            h += '<div style="margin-top:9px;color:#64748b;font-size:11px">数据时刻 ' + escHtml(((watch.data_quality || {}).quote_time) || d.quote_snapshot_at || d.quote_trade_date || '-') + '</div>';
        } else {
            h += '<span style="background:#b45309;color:#fff;padding:5px 11px;border-radius:999px;font-size:12px;font-weight:700">不可用</span></div>';
            h += '<div style="color:#9a3412;font-size:13px">当前自选股策略数据不可用，页面不会用旧分析冒充今日执行策略。</div>';
        }
        return h + '</section>';
    }

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

        h += renderStockDetailExecutionStrategy(d);

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

    /* ===== unified screener workbench ===== */
    var _screenerState = window._screenerState || { catalog: { presets: [] }, status: {}, saved: [], data: [], definition: null, center: null };
    window._screenerState = _screenerState;

    function screenerJson(url, options) {
        return fetch(url, options || {}).then(function (r) {
            return r.json().then(function (data) {
                if (!r.ok || (data && data.status === 'error')) throw new Error((data && (data.detail || data.error || data.message)) || ('HTTP ' + r.status));
                return data;
            });
        });
    }

    function screenerNumber(id, fallback) {
        var node = document.getElementById(id);
        var value = node ? node.value : '';
        return value === '' ? fallback : Number(value);
    }

    function screenerDefinitionFromForm() {
        return {
            preset: (document.getElementById('screenerPreset') || {}).value || 'trend_breakout',
            as_of_date: (document.getElementById('datePicker') || {}).value || '',
            universe: (document.getElementById('screenerUniverse') || {}).value || 'market',
            concept_code: (document.getElementById('screenerConceptCode') || {}).value || '',
            filters: {
                min_change: screenerNumber('screenerMinChange', null),
                max_change: screenerNumber('screenerMaxChange', null),
                min_turnover: screenerNumber('screenerMinTurnover', null),
                min_amount: screenerNumber('screenerMinAmount', null),
                min_flow: screenerNumber('screenerMinFlow', null),
                min_score: screenerNumber('screenerMinScore', null),
                keyword: (document.getElementById('screenerKeyword') || {}).value || '',
                exclude_st: !!((document.getElementById('screenerExcludeSt') || {}).checked),
                trend_days: screenerNumber('screenerTrendDays', null),
                vol_boost: screenerNumber('screenerVolBoost', null)
            },
            top: screenerNumber('screenerTop', 50)
        };
    }

    function screenerPresetButtons(catalog) {
        return (catalog.presets || []).map(function (p) {
            return '<button class="sc-preset" data-preset="' + escAttr(p.key) + '" onclick="window.selectScreenerPreset(\'' + escAttr(p.key) + '\')"><strong>' + escHtml(p.name) + '</strong><span>' + escHtml(p.description || '') + '</span></button>';
        }).join('');
    }

    function screenerVersionCards(catalog) {
        return (catalog.versions || []).map(function (version) {
            var blocked = version.decision === 'BLOCK';
            var stateClass = blocked ? ' blocked' : (version.production_selector ? ' active' : ' observe');
            return '<div class="sc-version-card' + stateClass + '"><div><strong>' + escHtml(version.version) + '</strong><span>' + escHtml(version.decision || '-') + '</span></div><b>' + escHtml(version.role || '-') + '</b><small>' + escHtml(version.reason || '-') + '</small></div>';
        }).join('');
    }

    function screenerVersionScores(row) {
        var versions = (row || {}).selector_versions || {};
        return ['V3', 'V4', 'V5', 'V6'].map(function (key) {
            var item = versions[key] || {};
            var value = item.score == null ? '回退' : fmt(item.score, 1);
            if (item.status === 'HARD_REJECT') value = '硬拒绝';
            return key + ' ' + value;
        }).join(' · ');
    }

    function screenerHorizonScores(row) {
        var scores = (((row || {}).multi_horizon || {}).scores || {});
        return ['T+1', 'T+5', 'T+20'].map(function (key) {
            return key + ' ' + (scores[key] == null ? '-' : fmt(scores[key], 1));
        }).join(' · ');
    }

    function screenerExecutionText(row) {
        var execution = (row || {}).execution_diagnostics || {};
        var cost = execution.estimated_round_trip_cost_bps;
        return (execution.status || 'DATA_BLOCKED') + (cost == null ? '' : ' · ' + fmt(cost, 1) + 'bps');
    }

    function screenerGateBanner(status) {
        status = status || {};
        var ready = status.selection_ready === true;
        var recommendationReady = status.recommendation_ready === true;
        var stateClass = ready ? (recommendationReady ? ' ok' : ' warn') : ' blocked';
        var title = ready ? (recommendationReady ? '数据闸门通过' : '规则选股可用，推荐证据未齐') : '基础选股数据已阻断';
        var dates = status.data_dates || {};
        return '<section class="sc-version-gate' + stateClass + '"><div><strong>' + title + '</strong><span>' + escHtml(status.gate || '-') + '</span></div><p>' + escHtml(status.message || '正在读取数据状态') + '</p><small>要求 ' + escHtml(status.expected_completed_session || '-') + ' · 日K ' + escHtml(dates.daily_kline || '-') + ' · 资金 ' + escHtml(dates.capital_flow || '-') + ' · 分析 ' + escHtml(dates.analysis || '-') + ' · 推荐 ' + escHtml(dates.recommendation || '-') + ' · 新闻 ' + escHtml(dates.news || '-') + ' · 公告 ' + escHtml(dates.notice || '-') + '</small></section>';
    }

    function renderScreenerWorkbench(container) {
        var catalog = _screenerState.catalog || { presets: [] };
        var h = '<div class="screen-page screener-workbench">';
        h += '<section class="screen-intro"><div><div class="screen-eyebrow">SCREENER WORKBENCH / V3-V6 生产融合选股</div><h2 class="screen-title">选股工作台</h2><p class="screen-subtitle">V4 硬门禁、V5 全局市场状态、V6 PIT 财务证据参与生产排序；同时输出 T+1/T+5/T+20、多维成本容量和组合约束。缺失或未来证据会回退或阻断，排序结果不自动下单。</p></div><div class="screen-intro-count"><strong>' + (catalog.presets || []).length + '</strong><span>套预设</span></div></section>';
        h += screenerGateBanner(_screenerState.status);
        h += '<section class="sc-panel"><div class="sc-panel-title"><strong>生产融合边界</strong><span>V4 硬拒绝不可被覆盖；V5/V6 只获得受限排名权重；真实自动下单权限保持关闭</span></div><div class="sc-version-grid">' + screenerVersionCards(catalog) + '</div></section>';
        h += '<section class="sc-toolbar"><div class="sc-toolbar-row"><label>保存方案 <select id="screenerSaved" onchange="window.loadSavedScreener(this.value)"><option value="">当前方案</option>' + (_screenerState.saved || []).map(function (x) { return '<option value="' + x.id + '">' + escHtml(x.name) + '</option>'; }).join('') + '</select></label><input id="screenerSaveName" placeholder="方案名称，例如：趋势回踩观察" maxlength="120"><button class="sc-primary" onclick="window.saveCurrentScreener()">保存方案</button><button class="sc-secondary" onclick="window.exportScreenerResults()">导出结果</button><span id="screenerStatus" class="sc-status-text"></span></div></section>';
        h += '<section class="sc-panel"><div class="sc-panel-title"><strong>1. 选择预设</strong><span>预设只决定初筛逻辑，下面的条件仍可继续叠加</span></div><div class="sc-preset-grid">' + screenerPresetButtons(catalog) + '</div></section>';
        h += '<section class="sc-panel"><div class="sc-panel-title"><strong>2. 组合条件</strong><span>日期按 as-of 口径执行；若没有数据只允许回退到更早日期，并明确标注</span></div>';
        h += '<div class="sc-filter-grid"><label>预设<select id="screenerPreset"><option value="trend_breakout">趋势突破</option>' + (catalog.presets || []).filter(function (p) { return p.key !== 'trend_breakout'; }).map(function (p) { return '<option value="' + escAttr(p.key) + '">' + escHtml(p.name) + '</option>'; }).join('') + '</select></label>';
        h += '<label>股票范围<select id="screenerUniverse"><option value="market">全市场</option><option value="portfolio">我的自选</option><option value="concept">概念成分</option></select></label><label>概念代码<input id="screenerConceptCode" placeholder="BKxxxx"></label>';
        h += '<label>最低涨幅<input id="screenerMinChange" type="number" step="0.1" placeholder="不限"></label><label>最高涨幅<input id="screenerMaxChange" type="number" step="0.1" placeholder="不限"></label><label>最低换手<input id="screenerMinTurnover" type="number" step="0.1" placeholder="不限"></label>';
        h += '<label>最低成交额<input id="screenerMinAmount" type="number" step="1000000" placeholder="元"></label><label>最低主力净流入<input id="screenerMinFlow" type="number" step="1000000" placeholder="元"></label><label>最低综合分<input id="screenerMinScore" type="number" step="1" placeholder="不限"></label>';
        h += '<label>搜索<input id="screenerKeyword" placeholder="代码 / 名称"></label><label>返回数量<input id="screenerTop" type="number" min="1" max="200" value="50"></label><label class="sc-check"><input id="screenerExcludeSt" type="checkbox" checked> 排除 ST / *ST</label></div>';
        h += '<div class="sc-advanced"><label>趋势持续天数<input id="screenerTrendDays" type="number" min="1" max="60" placeholder="预设默认"></label><label>放量倍数<input id="screenerVolBoost" type="number" step="0.1" placeholder="预设默认"></label><button class="sc-primary" onclick="window.runUnifiedScreener()">开始筛选</button><span class="sc-hint">资金、板块、公告和风险证据会在候选详情中继续确认</span></div></section>';
        h += '<section id="screenerResults" class="sc-panel"><div class="loading">正在准备选股工作台...</div></section></div>';
        container.innerHTML = h;
        var definition = _screenerState.definition || {};
        if (definition.preset && document.getElementById('screenerPreset')) document.getElementById('screenerPreset').value = definition.preset;
        if (definition.universe && document.getElementById('screenerUniverse')) document.getElementById('screenerUniverse').value = definition.universe;
        if (definition.concept_code && document.getElementById('screenerConceptCode')) document.getElementById('screenerConceptCode').value = definition.concept_code;
        var f = definition.filters || {};
        [['screenerMinChange','min_change'],['screenerMaxChange','max_change'],['screenerMinTurnover','min_turnover'],['screenerMinAmount','min_amount'],['screenerMinFlow','min_flow'],['screenerMinScore','min_score'],['screenerKeyword','keyword'],['screenerTrendDays','trend_days'],['screenerVolBoost','vol_boost']].forEach(function (pair) { if (f[pair[1]] != null && document.getElementById(pair[0])) document.getElementById(pair[0]).value = f[pair[1]]; });
        if (f.exclude_st === false && document.getElementById('screenerExcludeSt')) document.getElementById('screenerExcludeSt').checked = false;
        if (definition.top && document.getElementById('screenerTop')) document.getElementById('screenerTop').value = definition.top;
        window.markScreenerPreset(definition.preset || 'trend_breakout');
        window.runUnifiedScreener();
    }

    function loadScreenerWorkbench(d, container) {
        container.innerHTML = '<div class="loading">加载选股工作台配置...</div>';
        Promise.all([
            screenerJson('/api/screener/catalog').catch(function () { return { presets: [] }; }),
            screenerJson('/api/screener/status').catch(function (e) { return { status: 'blocked', gate: 'STATUS_UNAVAILABLE', message: e.message || String(e), selection_ready: false, recommendation_ready: false, data_dates: {} }; }),
            screenerJson('/api/screener/saved').catch(function () { return { data: [] }; })
        ]).then(function (result) {
            _screenerState.catalog = result[0] || { presets: [] };
            _screenerState.status = result[1] || {};
            _screenerState.saved = result[2] && result[2].data || [];
            renderScreenerWorkbench(container);
        }).catch(function (e) {
            container.innerHTML = '<div class="loading" style="color:#e74c3c">选股工作台加载失败：' + escHtml(e.message || e) + '</div>';
        });
    }

    window.markScreenerPreset = function (key) {
        document.querySelectorAll('.sc-preset').forEach(function (node) { node.classList.toggle('active', node.getAttribute('data-preset') === key); });
    };
    window.selectScreenerPreset = function (key) {
        var node = document.getElementById('screenerPreset');
        if (node) node.value = key;
        window.markScreenerPreset(key);
        window.runUnifiedScreener();
    };

    window.runUnifiedScreener = function () {
        var result = document.getElementById('screenerResults');
        if (!result) return;
        var definition = screenerDefinitionFromForm();
        _screenerState.definition = definition;
        window.markScreenerPreset(definition.preset);
        result.innerHTML = '<div class="loading">正在按 ' + escHtml(definition.preset) + ' 筛选...</div>';
        var status = document.getElementById('screenerStatus');
        if (status) status.textContent = '执行中...';
        screenerJson('/api/screener/run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(definition) }).then(function (data) {
            _screenerState.data = data.data || [];
            var stale = data.freshness && data.freshness !== 'exact';
            var selectorSummary = ((data.stats || {}).selector_summary || {}), gradeCounts = selectorSummary.grades || {};
            var fingerprint = selectorSummary.model_fingerprint || (((data || {}).selector || {}).model_fingerprint || '');
            var banner = '<div class="sc-result-head"><div><div class="screen-result-kicker">统一筛选结果</div><h3>' + escHtml((data.preset || {}).name || definition.preset) + '</h3><p>请求日期 ' + escHtml(data.requested_date || definition.as_of_date || '-') + ' · 实际数据 ' + escHtml(data.data_date || '-') + ' · 来源 ' + escHtml(data.source || '-') + ' · 模型 ' + escHtml(String(fingerprint).slice(0, 12) || '-') + '</p></div><div class="sc-result-metrics"><strong>' + (data.total || 0) + '</strong><span>候选</span></div></div>';
            if (data.data_gate && data.data_gate.selection_ready !== true) banner += '<div class="sc-freshness error">基础数据未到最近完整交易日：本次只能回看研究，不得作为交易输入。</div>';
            if (stale) banner += '<div class="sc-freshness warn">数据按可用日期回退，请按实际数据日期理解结果。</div>';
            if (data.error) banner += '<div class="sc-freshness error">' + escHtml(data.error) + '</div>';
            var h = banner + '<div class="sc-table-wrap"><table class="sc-candidate-table"><thead><tr><th>#</th><th>股票</th><th>综合分</th><th>价格/涨幅</th><th>关键指标</th><th>命中条件</th><th>状态</th><th>动作</th></tr></thead><tbody>';
            if (!_screenerState.data.length) h += '<tr><td colspan="8" class="sc-empty-cell">当前条件没有候选，可放宽参数或切换日期。</td></tr>';
            _screenerState.data.forEach(function (row) {
                var code = String(row.stock_code || '').padStart(6, '0');
                var keyMetrics = [];
                if (row.turnover_ratio != null) keyMetrics.push('换手 ' + fmt(row.turnover_ratio, 1) + '%');
                if (row.main_net_inflow != null) keyMetrics.push('资金 ' + fmtMoney(row.main_net_inflow));
                if (row.vol_ratio != null) keyMetrics.push('量能 ' + fmt(row.vol_ratio, 1) + 'x');
                if (row.boards != null) keyMetrics.push(row.boards + ' 连板');
                if (row.gain_60d != null) keyMetrics.push('60日 ' + fmt(row.gain_60d, 1) + '%');
                h += '<tr><td>' + escHtml(row.rank || '-') + '</td><td><strong>' + nameLink(code, row.stock_name || row.short_name || code) + '</strong><small>' + escHtml(code) + '</small></td><td><b class="sc-score">' + fmt(row.score, 1) + ' · ' + escHtml(row.candidate_grade || 'C') + '</b><small title="各版本缺失证据会回退，V4 硬拒绝除外">' + escHtml(screenerVersionScores(row)) + '</small><small>' + escHtml(screenerHorizonScores(row)) + '</small></td><td>' + escHtml(fmtPrice(row.price || row.close)) + '<br><span class="' + clsPct(row.change_pct) + '">' + escHtml(pct(row.change_pct)) + '</span></td><td>' + escHtml(keyMetrics.join(' · ') || '-') + '<br><small>' + escHtml(screenerExecutionText(row)) + '</small></td><td>' + escHtml((row.matched_conditions || []).join('、')) + '<br><small>' + escHtml(row.explanation || '') + '</small></td><td><span class="sc-status-pill">' + escHtml(row.action || 'WATCH') + '</span><small>' + (row.portfolio_eligible ? '组合可用' : '组合受限') + '</small></td><td><button class="sc-mini-btn" onclick="window.saveScreenerCandidate(\'' + escAttr(code) + '\')">候选池</button><button class="sc-mini-btn" onclick="pfAddWithCode(\'' + escAttr(code) + '\')">自选</button></td></tr>';
            });
            h += '</tbody></table></div><div class="sc-table-note">筛选完成：全市场扫描 ' + ((data.stats || {}).scan_limit || (data.stats || {}).input_count || 0) + '，输入 ' + ((data.stats || {}).input_count || 0) + '，结果 ' + ((data.stats || {}).result_count || 0) + '；A/B/C/拒绝 ' + (gradeCounts.A || 0) + '/' + (gradeCounts.B || 0) + '/' + (gradeCounts.C || 0) + '/' + (gradeCounts.REJECT || 0) + '。分数只用于排序，不构成交易指令。</div>';
            result.innerHTML = h;
            if (status) status.textContent = '已完成 ' + (data.data_date || '');
        }).catch(function (e) {
            result.innerHTML = '<div class="loading" style="color:#e74c3c">筛选失败：' + escHtml(e.message || e) + '</div>';
            if (status) status.textContent = '执行失败';
        });
    };

    window.saveCurrentScreener = function () {
        var name = ((document.getElementById('screenerSaveName') || {}).value || '').trim();
        if (!name) { alert('请输入方案名称'); return; }
        var definition = screenerDefinitionFromForm();
        screenerJson('/api/screener/saved', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, definition: definition }) }).then(function () {
            var status = document.getElementById('screenerStatus');
            if (status) status.textContent = '方案已保存';
        }).catch(function (e) { alert('保存方案失败：' + (e.message || e)); });
    };

    window.loadSavedScreener = function (id) {
        var saved = (_screenerState.saved || []).find(function (item) { return String(item.id) === String(id); });
        if (!saved) return;
        _screenerState.definition = saved.definition || {};
        var c = document.getElementById('tab-screen');
        if (c) renderScreenerWorkbench(c);
    };

    window.exportScreenerResults = function () {
        var rows = _screenerState.data || [];
        if (!rows.length) { alert('当前没有可导出的结果'); return; }
        var header = ['排名','股票代码','股票名称','综合分','候选等级','T+1','T+5','T+20','预计往返成本bps','组合可用','组合限制原因','收盘价','涨跌幅','换手率','主力净流入','命中条件','状态','模型指纹'];
        var csv = [header].concat(rows.map(function (r) { var hs = ((r.multi_horizon || {}).scores || {}), ex = r.execution_diagnostics || {}; return [r.rank, r.stock_code, r.stock_name || r.short_name, r.score, r.candidate_grade, hs['T+1'], hs['T+5'], hs['T+20'], ex.estimated_round_trip_cost_bps, r.portfolio_eligible, (r.portfolio_reject_reasons || []).join('|'), r.price || r.close, r.change_pct, r.turnover_ratio, r.main_net_inflow, (r.matched_conditions || []).join('、'), r.action || r.signal_status || r.risk_level, r.model_fingerprint]; })).map(function (line) { return line.map(function (v) { return '"' + String(v == null ? '' : v).replace(/"/g, '""') + '"'; }).join(','); }).join('\n');
        var blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' });
        var url = URL.createObjectURL(blob), a = document.createElement('a');
        a.href = url; a.download = 'probiga_screener_' + ((document.getElementById('datePicker') || {}).value || 'results') + '.csv'; a.click(); URL.revokeObjectURL(url);
    };

    window.saveScreenerCandidate = function (code) {
        var row = (_screenerState.data || []).find(function (item) { return String(item.stock_code).padStart(6, '0') === String(code).padStart(6, '0'); });
        if (!row) return;
        screenerJson('/api/screener/candidates', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ stock_code: code, stock_name: row.stock_name || row.short_name || code, source: 'screener', screen_name: ((_screenerState.definition || {}).preset || ''), score: row.score, as_of_date: row.data_date || (document.getElementById('datePicker') || {}).value || '', reason: row.explanation || '', payload: row }) }).then(function () { alert(code + ' 已加入候选池'); }).catch(function (e) { alert('加入候选池失败：' + (e.message || e)); });
    };

    function decisionRecommendationStatus(row) {
        var readiness = row && row.decision_readiness || {};
        return String(readiness.recommend_status || row.recommend_status || row.signal_status || '').toUpperCase();
    }

    function decisionPayloadIsFresh(payload, expectedDay) {
        var freshness = String(payload && payload.freshness || '').toLowerCase();
        var requested = String(payload && payload.requested_date || '').slice(0, 10);
        var dataDate = String(payload && payload.data_date || '').slice(0, 10);
        var expected = String(expectedDay || '').slice(0, 10);
        return !payload._load_error && (freshness === 'exact' || freshness === 'live') && !!requested && !!dataDate && requested === dataDate && (!expected || requested === expected);
    }

    function qualifiedDecisionRows(payload, expectedDay) {
        if (!decisionPayloadIsFresh(payload, expectedDay)) return [];
        return (payload && payload.data || []).filter(function (row) {
            var grade = String(row.candidate_grade || '').toUpperCase();
            var rejects = row.risk_gate && row.risk_gate.reject_reasons || [];
            return (grade === 'A' || grade === 'B') && row.portfolio_eligible === true && decisionRecommendationStatus(row) === 'ALLOW' && !rejects.length;
        }).sort(function (left, right) {
            return Number(left.rank || 999999) - Number(right.rank || 999999) || String(left.stock_code || '').localeCompare(String(right.stock_code || ''));
        });
    }

    function decisionHistoryUrl(day, preset) {
        return '/api/screener/history?data_date=' + encodeURIComponent(day) + '&preset=' + encodeURIComponent(preset) + '&limit=300';
    }

    function decisionRead(url, timeoutMs) {
        return fetchRawJsonWithTimeout(url, timeoutMs).catch(function(error) {
            return { data:[], _load_error:String(error && error.message || error || '接口读取失败') };
        });
    }

    function decisionRowCard(row, index) {
        var code = String(row.stock_code || '').padStart(6, '0');
        var score = row.ensemble_score == null ? row.score : row.ensemble_score;
        var theme = row.primary_concept || row.theme_name || row.concept_name || '主题待确认';
        var reason = (row.matched_conditions || []).join('、') || row.explanation || '评级、组合约束与推荐门禁均已通过';
        return '<article class="decision-stock-card"><span class="decision-stock-rank">' + escHtml(row.rank || index + 1) + '</span><div><strong>' + nameLink(code, row.stock_name || row.short_name || code) + '</strong><small>' + escHtml(code) + ' · ' + escHtml(theme) + '</small><p>' + escHtml(reason) + '</p></div><div class="decision-stock-score"><b>' + escHtml(fmt(score, 2)) + '</b><span>' + escHtml(row.candidate_grade || '-') + '级 · ALLOW</span></div></article>';
    }

    function decisionColumn(title, kicker, payload, expectedDay) {
        payload = payload || {};
        var rows = qualifiedDecisionRows(payload, expectedDay);
        var allRows = payload.data || [];
        var isFresh = decisionPayloadIsFresh(payload, expectedDay);
        var state = payload._load_error ? '数据不可用' : !payload.run ? '暂无批次' : !isFresh ? '数据回退，禁止推荐' : rows.length ? '有合格候选' : '无合格候选';
        var tone = rows.length ? 'ready' : isFresh ? 'empty' : 'blocked';
        var meta = '决策日 ' + escHtml(payload.requested_date || '-') + ' · 数据日 ' + escHtml(payload.data_date || '-') + ' · 审计 ' + allRows.length + ' 只';
        var body = rows.length ? rows.slice(0, 12).map(decisionRowCard).join('') : '<div class="decision-empty"><strong>' + escHtml(state) + '</strong><span>' + (payload._load_error ? '接口读取失败：' + escHtml(payload._load_error) + '；不能解释为没有候选。' : isFresh ? '没有股票同时通过 A/B 评级、组合约束与推荐门禁；保持现金与观察。' : '当前结果只可用于历史审计，不能作为今天的个股推荐。') + '</span></div>';
        return '<section class="decision-column" data-tone="' + tone + '"><header><div><span>' + escHtml(kicker) + '</span><h3>' + escHtml(title) + '</h3><p>' + meta + '</p></div><b>' + escHtml(state) + '</b></header><div class="decision-stock-list">' + body + '</div><footer>批次 ' + escHtml(String((payload.run || {}).run_uid || '-').slice(0, 12)) + ' · ' + escHtml(payload.observed_at || payload.generated_at || '-') + '</footer></section>';
    }

    function loadCandidateDecisionPage(d, container) {
        var day = String(d || currentDateValue() || '').slice(0, 10);
        container.innerHTML = '<div class="loading">正在加载盘前与盘中决策...</div>';
        Promise.all([
            decisionRead(decisionHistoryUrl(day, 'capital_support'), 15000),
            decisionRead(decisionHistoryUrl(day, 'intraday_sector'), 15000)
        ]).then(function (result) {
            var premarket = result[0] || {}, intraday = result[1] || {};
            var preQualified = qualifiedDecisionRows(premarket, day).length;
            var intradayQualified = qualifiedDecisionRows(intraday, day).length;
            var anyUnavailable = !!premarket._load_error || !!intraday._load_error;
            container.innerHTML = '<div class="decision-page candidate-decision-page"><section class="decision-page-head"><div><span>03 / CANDIDATES & DECISIONS</span><h2>候选与决策</h2><p>盘前看计划，盘中看确认；两列都只展示日期新鲜、评级与门禁全部通过的标的。拒绝项仍保留在“04 候选账本”审计，不再冒充推荐。</p></div><div class="decision-page-count"><strong>' + (anyUnavailable ? '—' : preQualified + intradayQualified) + '</strong><span>' + (anyUnavailable ? '部分数据不可用' : '当前合格记录') + '</span></div></section><div class="decision-dual-grid">' + decisionColumn('盘前计划', '09:08 / PREMARKET', premarket, day) + decisionColumn('盘中确认', '09:32+ / INTRADAY', intraday, day) + '</div><section class="decision-page-note"><strong>阅读顺序</strong><span>先看数据日期是否一致，再看是否有合格候选，最后才看分数；没有合格候选本身就是明确决策。</span><button type="button" onclick="switchTab(\'trading-v3-ledger\')">查看全部候选与拒绝账本</button></section></div>';
            setStatus(anyUnavailable ? '盘前与盘中决策已加载，部分数据不可用' : '盘前与盘中决策已按同一页面日期加载', anyUnavailable);
        }).catch(function (error) {
            container.innerHTML = '<div class="decision-load-error"><strong>候选与决策加载失败</strong><span>' + escHtml(error.message || error) + '</span></div>';
            setStatus('候选与决策加载失败: ' + (error.message || error), true);
        });
    }

    function decisionActionCopy(action) {
        return {
            CONTROLLED_RISK_ON: {label:'控制风险参与', detail:'只做已确认主线，分批进场，不追高；总风险仓位不超过系统上限。', tone:'positive'},
            SELECTIVE_PROBES: {label:'小仓选择性试错', detail:'只在盘中确认后用小仓试单，失败立即停止新增风险。', tone:'cautious'},
            CASH_FIRST: {label:'现金优先', detail:'不开无依据的新仓，等待市场宽度、成交与核心板块重新确认。', tone:'defensive'}
        }[String(action || '').toUpperCase()] || {label:'等待有效决策', detail:'数据或决策批次尚未就绪，不根据旧快照行动。', tone:'blocked'};
    }

    function decisionTruthLabel(state) {
        return {READY:'可执行研究决策', EMPTY:'无目标，现金优先', BLOCKED:'门禁阻断', STALE:'数据已过期', LOADING:'决策生成中', UNAVAILABLE:'决策不可用'}[state] || state || '决策不可用';
    }

    function premarketForecastIsFresh(forecast, day) {
        if (!forecast || forecast._load_error || forecast.fallback === true) return false;
        var sessionDate = String(forecast.session_date || forecast.requested_date || '').slice(0, 10);
        return !!sessionDate && sessionDate === String(day || '').slice(0, 10);
    }

    function loadDecisionCockpitPage(d, container) {
        var day = String(d || currentDateValue() || '').slice(0, 10);
        container.innerHTML = '<div class="loading">正在整理今日大盘预期与操作策略...</div>';
        Promise.all([
            decisionRead('/api/v3/context?trade_date=' + encodeURIComponent(day), 12000),
            decisionRead('/api/v3/hypotheses/latest?limit=50&scope_type=MARKET&trade_date=' + encodeURIComponent(day), 12000),
            decisionRead('/api/hot-data/premarket-theme-forecast?session_date=' + encodeURIComponent(day), 15000),
            decisionRead(decisionHistoryUrl(day, 'capital_support'), 15000),
            decisionRead(decisionHistoryUrl(day, 'intraday_sector'), 15000)
        ]).then(function (result) {
            var contextEnvelope = result[0] || {}, hypothesisEnvelope = result[1] || {};
            var context = contextEnvelope._load_error ? {} : contextEnvelope.data || contextEnvelope;
            var hypotheses = hypothesisEnvelope._load_error ? [] : hypothesisEnvelope.data || hypothesisEnvelope || [];
            var market = Array.isArray(hypotheses) ? hypotheses.filter(function (row) { return row.scope_type === 'MARKET'; })[0] || {} : {};
            var forecast = result[2] || {}, premarket = result[3] || {}, intraday = result[4] || {};
            var contextState = tradingDecisionTruth(context);
            var marketUsable = !hypothesisEnvelope._load_error && contextState === 'READY' && market.scope_type === 'MARKET';
            var forecastFresh = premarketForecastIsFresh(forecast, day);
            var action = marketUsable ? decisionActionCopy(market.proposed_action) : decisionActionCopy(contextState === 'EMPTY' || contextState === 'BLOCKED' ? 'CASH_FIRST' : '');
            var riskCap = marketUsable && market.max_position_weight != null ? fmt(Number(market.max_position_weight) * 100, 1) + '%' : '—';
            var freshThemes = forecastFresh ? (forecast.themes || []) : [];
            var thesis = marketUsable && market.thesis
                ? market.thesis
                : forecastFresh && forecast.summary
                ? forecast.summary
                : freshThemes.length
                ? '盘前主线优先观察：' + freshThemes.slice(0, 3).map(function (theme) { return theme.theme_name; }).join('、')
                : '今天尚未形成可验证的大盘假设，暂不根据旧数据行动。';
            var invalidations = marketUsable && (market.invalidations || []).length ? market.invalidations.join('；') : '盘中宽度、成交、核心板块或风险事件出现反向变化时立即降级。';
            var preCount = qualifiedDecisionRows(premarket, day).length, intraCount = qualifiedDecisionRows(intraday, day).length;
            var loadErrors = result.map(function(item) { return item && item._load_error; }).filter(Boolean);
            var warning = loadErrors.length ? '<div class="decision-load-error"><strong>部分决策证据不可用</strong><span>' + escHtml(loadErrors.join('；')) + '；缺失项不会被解释为零或空态。</span></div>' : '';
            var preCountText = premarket._load_error ? '—' : String(preCount), intraCountText = intraday._load_error ? '—' : String(intraCount);
            var themeCards = freshThemes.slice(0, 6).map(function (theme) {
                return '<article><span>#' + escHtml(theme.rank || '-') + '</span><strong>' + escHtml(theme.theme_name || '-') + '</strong><b>' + escHtml(fmt(theme.score, 1)) + '</b><small>' + escHtml((theme.evidence || []).slice(0, 2).join('；') || theme.status || '等待盘中确认') + '</small></article>';
            }).join('') || '<div class="decision-empty"><strong>' + (forecast._load_error ? '盘前主题数据不可用' : '没有可用的盘前主题预判') + '</strong><span>' + (forecast._load_error ? escHtml(forecast._load_error) + '；不能解释为没有主题。' : '历史回退主题不参与今天的市场预期。') + '</span></div>';
            container.innerHTML = '<div class="decision-page decision-cockpit"><section class="decision-hero" data-tone="' + action.tone + '"><div><span>01 / TODAY\'S DECISION</span><h2>今日大盘预期</h2><p>' + escHtml(thesis) + '</p></div><div class="decision-hero-action"><small>整体操作</small><strong>' + escHtml(action.label) + '</strong><p>' + escHtml(action.detail) + '</p></div></section>' + warning + '<div class="decision-kpis"><article><span>决策状态</span><strong>' + escHtml(decisionTruthLabel(contextState)) + '</strong><small>' + escHtml(context.data_date || '-') + ' 数据</small></article><article><span>风险仓位上限</span><strong>' + escHtml(riskCap) + '</strong><small>不是目标仓位，是不可突破的上限</small></article><article><span>盘前合格候选</span><strong>' + preCountText + '</strong><small>' + escHtml(premarket._load_error ? '数据不可用' : premarket.freshness || '暂无批次') + '</small></article><article><span>盘中合格候选</span><strong>' + intraCountText + '</strong><small>' + escHtml(intraday._load_error ? '数据不可用' : intraday.freshness || '暂无批次') + '</small></article></div><div class="decision-main-grid"><section class="decision-panel"><header><div><span>MARKET PLAYBOOK</span><h3>今天怎么操作</h3></div></header><ol class="decision-playbook"><li><b>1</b><div><strong>开盘前：先确认数据</strong><span>请求日、数据日和证据时点必须匹配；回退数据只复盘，不荐股。</span></div></li><li><b>2</b><div><strong>盘中：只做确认后的候选</strong><span>盘前计划必须经过实时价格、板块宽度和风险门禁确认；无合格候选就不做。</span></div></li><li><b>3</b><div><strong>仓位：服从 ' + escHtml(riskCap) + ' 上限</strong><span>' + escHtml(action.detail) + '</span></div></li><li class="danger"><b>!</b><div><strong>失效条件</strong><span>' + escHtml(invalidations) + '</span></div></li></ol><button class="decision-primary-button" type="button" onclick="switchTab(\'trading-v3-candidates\')">查看盘前 / 盘中候选决策</button></section><section class="decision-panel"><header><div><span>PREMARKET THEMES</span><h3>盘前主线预期</h3></div><b>' + escHtml(forecast._load_error ? '数据不可用' : forecastFresh ? forecast.data_quality || '当日冻结' : forecast.fallback ? '历史回退（不采用）' : '等待当日数据') + '</b></header><div class="decision-theme-grid">' + themeCards + '</div><footer>冻结时点 ' + escHtml(forecast.cutoff_at || '-') + ' · A股源数据 ' + escHtml(forecast.source_trade_date || '-') + '</footer></section></div></div>';
            setStatus(loadErrors.length ? '今日决策已加载，部分证据不可用' : '今日大盘预期与操作策略已加载', loadErrors.length > 0);
        }).catch(function (error) {
            container.innerHTML = '<div class="decision-load-error"><strong>交易决策总览加载失败</strong><span>' + escHtml(error.message || error) + '</span></div>';
            setStatus('交易决策总览加载失败: ' + (error.message || error), true);
        });
    }

    function candidateCenterStockPoolIsReadable(pool) {
        pool = pool || {};
        var items = pool.items, summary = pool.summary || {};
        var poolStatus = String(pool.pool_status || '').toUpperCase();
        var runStatus = String(pool.run_status || '').toUpperCase();
        var sessionDate = String(pool.decision_session_date || '').slice(0, 10);
        var dataDate = String(pool.trade_date || pool.data_date || '').slice(0, 10);
        var datePattern = /^\d{4}-\d{2}-\d{2}$/;
        var stockCount = summary.stock_count;
        var candidateCount = summary.strategy_candidate_count;
        var actualCandidateCount = Array.isArray(items) ? items.filter(function(item) { return item && item.is_strategy_candidate === true; }).length : -1;
        return !!pool.run_uid && pool.pool_readable === true && runStatus === 'COMPLETED' && pool.decision_integrity_verified === true && (poolStatus === 'READY' || poolStatus === 'EMPTY') && datePattern.test(sessionDate) && datePattern.test(dataDate) && dataDate <= sessionDate && Array.isArray(items) && Number.isInteger(stockCount) && stockCount === items.length && Number.isInteger(candidateCount) && candidateCount === actualCandidateCount && ((poolStatus === 'READY' && candidateCount > 0) || (poolStatus === 'EMPTY' && candidateCount === 0));
    }

    function candidateCenterStockPoolTruth(pool, requestedDate, latestFormalDate) {
        pool = pool || {};
        var datePattern = /^\d{4}-\d{2}-\d{2}$/;
        var decisionDate = String(pool.decision_session_date || '').slice(0, 10);
        var dataDate = String(pool.trade_date || pool.data_date || '').slice(0, 10);
        var target = String(requestedDate || pool.requested_trade_date || decisionDate || '').slice(0, 10);
        var latestDate = String(latestFormalDate || '').slice(0, 10);
        var reasonCodes = Array.isArray(pool.reason_codes) ? pool.reason_codes.filter(Boolean).join('；') : '';
        function blocked(reason, code) {
            return { ready:false, verifiedCompleted:false, requestedDate:target, decisionDate:decisionDate, dataDate:dataDate, reason:reason, reasonCode:code || 'FORMAL_POOL_BLOCKED' };
        }
        if (!datePattern.test(target)) return blocked('缺少有效请求日，不能确认这是当前策略池', 'REQUEST_DATE_INVALID');
        if (!datePattern.test(latestDate)) return blocked('无法确认最新正式交易日，策略池保持研究只读', 'LATEST_FORMAL_DATE_UNKNOWN');
        if (target !== latestDate) return blocked('请求日 ' + target + ' 不是最新正式交易日 ' + latestDate + '，只允许历史研究查看', 'HISTORICAL_RESEARCH_ONLY');
        if (!candidateCenterStockPoolIsReadable(pool)) return blocked('策略池未通过 COMPLETED、完整性、计数或日期校验' + (reasonCodes ? '：' + reasonCodes : ''), 'POOL_NOT_VERIFIED_COMPLETED');
        if (decisionDate !== target) return blocked('策略池决策日 ' + (decisionDate || '未知') + ' 与请求日 ' + target + ' 不一致', 'POOL_DATE_MISMATCH');
        if (dataDate !== target) return blocked('策略池数据日 ' + (dataDate || '未知') + ' 与最新正式交易日 ' + target + ' 不一致', 'POOL_DATA_DATE_MISMATCH');
        if (pool.is_historical_fallback === true || pool.historical_read_only === true) return blocked('当前展示的是历史只读批次，不是请求日正式票池', 'HISTORICAL_READ_ONLY');
        if (pool.governance_deferred === true || pool.activation_enabled === false || String(pool.strategy_governance_mode || '').toUpperCase() === 'DEFERRED_DB') return blocked('治理数据库处于 DEFERRED_DB，候选只可研究审计', 'GOVERNANCE_DATABASE_DEFERRED');
        if (String(pool.decision_scope || '').toUpperCase() === 'RESEARCH_ONLY' || pool.actionable_output_allowed === false) return blocked('批次权限为 RESEARCH_ONLY，不能升级为当前可执行票池', 'RESEARCH_ONLY');
        return { ready:true, verifiedCompleted:true, requestedDate:target, decisionDate:decisionDate, dataDate:dataDate, reason:'身份、日期与完整性均已通过', reasonCode:'VERIFIED_COMPLETED_CURRENT_POOL' };
    }

    function candidateCenterStockPoolWithHistoricalFallback(requestedDate) {
        var target = String(requestedDate || '').slice(0, 10);
        var exactPath = '/api/v3/stock-pool' + (target ? '?trade_date=' + encodeURIComponent(target) : '');
        return fetchRawJsonWithTimeout(exactPath, 15000).then(function(exactEnvelope) {
            var exact = (exactEnvelope || {}).data || exactEnvelope || {};
            var exactSession = String(exact.decision_session_date || exact.trade_date || '').slice(0, 10);
            var exactReadable = candidateCenterStockPoolIsReadable(exact) && exact.is_historical_fallback !== true && exact.historical_read_only !== true && (!target || exactSession === target);
            if (exactReadable) {
                return Object.assign({}, exact, {
                    requested_trade_date: target || exactSession,
                    is_historical_fallback: false
                });
            }
            function missingExact() {
                return Object.assign({}, exact, {
                    requested_trade_date: target,
                    exact_run_missing: true,
                    exact_run_unreadable: !!exact.run_uid,
                    is_historical_fallback: false
                });
            }
            if (!target) return missingExact();
            return fetchRawJsonWithTimeout('/api/v3/stock-pool?before_session_date=' + encodeURIComponent(target), 15000).then(function(latestEnvelope) {
                var latest = (latestEnvelope || {}).data || latestEnvelope || {};
                var latestSession = String(latest.decision_session_date || latest.trade_date || '').slice(0, 10);
                var boundedTarget = String(latest.before_session_date || latest.requested_trade_date || '').slice(0, 10);
                var fallbackSession = String(latest.historical_fallback_session_date || '').slice(0, 10);
                var readable = candidateCenterStockPoolIsReadable(latest) && latest.is_historical_fallback === true && latest.historical_read_only === true && String(latest.historical_fallback_status || '') === 'HISTORICAL_READ_ONLY' && boundedTarget === target && fallbackSession === latestSession && !!latestSession && latestSession < target;
                if (!readable) return missingExact();
                return Object.assign({}, latest, {
                    requested_trade_date: target,
                    exact_run_missing: true,
                    exact_run_unreadable: !!exact.run_uid,
                    is_historical_fallback: true,
                    historical_read_only: true,
                    historical_fallback_session_date: latestSession,
                    historical_fallback_reason: latest.historical_fallback_reason || '请求日没有完整可验证的 V3 决策批次，展示此前最近一次 COMPLETED 历史策略池'
                });
            }).catch(missingExact);
        });
    }

    function loadCandidateCenterPage(d, container) {
        container.innerHTML = '<div class="loading">正在按统一决策批次加载候选与拒绝...</div>';
        var requestedDate = String(d || '').slice(0, 10);
        if (requestedDate === currentDateValue()) requestedDate = recommendationDateValue();
        Promise.all([
            candidateCenterStockPoolWithHistoricalFallback(requestedDate),
            fetchRawJsonWithTimeout('/api/v3/context?trade_date=' + encodeURIComponent(requestedDate), 10000).catch(function(error) {
                return { data: {}, _load_error: String(error && error.message || error || '统一决策上下文读取失败') };
            })
        ]).then(function(unifiedResult) {
            var poolEnvelope = unifiedResult[0] || {}, contextEnvelope = unifiedResult[1] || {};
            var pool = poolEnvelope.data || poolEnvelope;
            var context = contextEnvelope.data || contextEnvelope;
            var contextLoadError = String(contextEnvelope._load_error || '');
            var rows = Array.isArray(pool.items) ? pool.items.slice() : [];
            rows.sort(function (left, right) {
                var leftRank = Number(left.rank_no == null ? 999999 : left.rank_no);
                var rightRank = Number(right.rank_no == null ? 999999 : right.rank_no);
                return leftRank - rightRank || String(left.stock_code || '').localeCompare(String(right.stock_code || ''));
            });
            var summary = pool.summary || {};
            var historicalFallback = pool.is_historical_fallback === true;
            var formalPoolTruth = candidateCenterStockPoolTruth(pool, requestedDate, latestFormalStrategyDateValue());
            var decisionSessionDate = String(historicalFallback ? (pool.decision_session_date || pool.trade_date || '') : (context.decision_session_date || pool.decision_session_date || '')).slice(0, 10);
            var resolvedDate = String(historicalFallback ? (pool.trade_date || pool.data_date || '') : (context.data_date || pool.trade_date || '')).slice(0, 10);
            var batchMismatch = !!(!historicalFallback && context.run_uid && pool.run_uid && context.run_uid !== pool.run_uid);
            var contextState = historicalFallback ? 'STALE' : tradingDecisionTruth(context);
            var projectedTargetCount = Number(context.target_count);
            var poolTargetCount = Number(summary.target_count);
            if (batchMismatch || (!historicalFallback && (contextState === 'READY' || contextState === 'EMPTY') && (!Number.isFinite(poolTargetCount) || poolTargetCount !== projectedTargetCount))) contextState = 'UNAVAILABLE';
            if (!pool.run_uid && contextState !== 'BLOCKED') contextState = 'UNAVAILABLE';
            var filter = _screenerState.centerFilter || {};
            var filterStock = String(filter.stock || '').trim();
            var filterKind = String(filter.kind || '');
            _screenerState.center = { candidates: rows, data_date: resolvedDate, run_uid: pool.run_uid, historical_read_only: historicalFallback, formal_pool_ready:formalPoolTruth.ready, requested_trade_date: requestedDate };

            function kindOf(row) {
                if (row.target) return 'TARGET';
                if (row.rejection) return 'REJECTED';
                if (row.is_strategy_candidate) return 'CANDIDATE';
                return 'RESEARCH';
            }
            function kindText(kind) {
                return { TARGET:'研究目标', REJECTED:'组合拒绝', CANDIDATE:'研究候选', RESEARCH:'研究样本' }[kind] || '研究样本';
            }
            function reasonText(row) {
                var rejection = row.rejection || {};
                return rejection.reason || rejection.reason_code || (row.reasons || []).join('；') || '暂无补充证据';
            }

            var h = '<div class="screen-page candidate-center unified-stock-pool">';
            var introCount = pool.run_uid ? rows.length : '—';
            var introCountLabel = historicalFallback ? '历史只读股票' : pool.run_uid ? '同批次股票' : '当前不可用';
            h += '<section class="screen-intro"><div><div class="screen-eyebrow">04 / IMMUTABLE STOCK POOL / SAME RUN_UID</div><h2 class="screen-title">候选账本</h2><p class="screen-subtitle">保留原有完整账本视图，用于查看候选、目标和拒绝原因；所有行严格按第一列 rank_no 升序展示，研究排序不拥有模拟或真实订单权限。</p></div><div class="screen-intro-count"><strong id="candidateCenterVisibleCount">' + introCount + '</strong><span>' + introCountLabel + '</span></div></section>';
            h += '<div class="sc-freshness ' + (formalPoolTruth.ready ? 'ok' : 'error') + '"><strong>' + (formalPoolTruth.ready ? 'VERIFIED COMPLETED / 当前正式策略池' : 'RESEARCH_ONLY / 正式策略池不可用') + '</strong><span>请求日 ' + escHtml(formalPoolTruth.requestedDate || '-') + ' · 决策日 ' + escHtml(formalPoolTruth.decisionDate || '-') + ' · 数据日 ' + escHtml(formalPoolTruth.dataDate || '-') + '；' + escHtml(formalPoolTruth.reason) + (formalPoolTruth.ready ? '。候选账本仍为只读审计，模拟执行须另经统一复验。' : '。旧日期、未验证或只读候选不会进入正式展示。') + '</span></div>';
            if (historicalFallback) h += '<div class="sc-freshness warn"><strong>HISTORICAL_READ_ONLY / 历史只读</strong><span>原请求日 ' + escHtml(requestedDate || '-') + ' 没有 V3 决策批次；当前只回看严格更早的最近可读批次（决策日 ' + escHtml(decisionSessionDate || '-') + '，数据日 ' + escHtml(resolvedDate || '-') + '）。全部股票不可执行，不会创建模拟或真实订单，也不代表请求日的 READY 结论。</span></div>';
            if (pool.exact_run_missing && !historicalFallback) h += '<div class="sc-freshness error"><strong>' + escHtml(contextState === 'BLOCKED' ? 'BLOCKED' : 'UNAVAILABLE') + '</strong><span>原请求日 ' + escHtml(requestedDate || '-') + ' 没有 V3 决策批次，且没有严格更早的可读历史策略池。这不是正常空态，不得解释为没有候选。</span></div>';
            if (contextLoadError) h += '<div class="sc-freshness error"><strong>请求日上下文 UNAVAILABLE</strong><span>' + escHtml(contextLoadError) + '；' + (historicalFallback ? '历史批次仍只作独立只读回看，不与请求日合并。' : '当前策略池不得升级为 READY 或正常空态。') + '</span></div>';
            var displayedRequestedDate = pool.requested_trade_date || requestedDate || context.requested_date || '-';
            var displayedExpectedDataDate = historicalFallback ? '-' : (context.expected_data_date || resolvedDate || '-');
            var displayedRunUid = historicalFallback ? pool.run_uid : (context.run_uid || pool.run_uid);
            var displayedDecisionAt = historicalFallback ? (pool.decision_at || pool.generated_at) : (context.decision_at || pool.decision_at || pool.generated_at);
            var displayedEvidenceAsOf = historicalFallback ? '-' : (context.evidence_as_of || '-');
            var displayedValidUntil = historicalFallback ? '-' : (context.valid_until || '-');
            h += '<section class="trade-context-light" data-state="' + escAttr(contextState) + '"><div><b>' + escHtml(contextState) + '</b><span>' + (contextState === 'UNAVAILABLE' ? '决策真值或同批次账本不可验证，不能解释为没有机会' : contextState === 'STALE' ? '历史策略池仅供只读复核，不是原请求日的同批次 READY 结论' : contextState === 'LOADING' ? '批次仍在生成，当前内容不是最终结论' : contextState === 'BLOCKED' ? '数据或决策门禁阻断，不允许新增订单' : contextState === 'EMPTY' ? '批次完整性已验证，且没有研究目标' : '统一批次真值可读') + '</span></div><dl><div><dt>原请求日</dt><dd>' + escHtml(displayedRequestedDate) + '</dd></div><div><dt>决策会话日</dt><dd>' + escHtml(decisionSessionDate || '-') + '</dd></div><div><dt>特征数据日</dt><dd>' + escHtml(resolvedDate || '-') + '</dd></div><div><dt>预期数据日</dt><dd>' + escHtml(displayedExpectedDataDate) + '</dd></div><div><dt>run_uid</dt><dd>' + escHtml(displayedRunUid || '-') + '</dd></div><div><dt>decision_at</dt><dd>' + escHtml(displayedDecisionAt || '-') + '</dd></div><div><dt>evidence_as_of</dt><dd>' + escHtml(displayedEvidenceAsOf) + '</dd></div><div><dt>valid_until</dt><dd>' + escHtml(displayedValidUntil) + '</dd></div></dl><div class="trade-authority-light"><span class="research">研究：' + (historicalFallback ? '历史只读' : contextState === 'LOADING' ? '等待决策完成' : contextState === 'UNAVAILABLE' ? '不可用' : contextState === 'BLOCKED' ? '门禁阻断' : '可读') + '</span><span class="paper">模拟：' + escHtml(!formalPoolTruth.ready ? 'RESEARCH_ONLY / 不可入队' : contextState !== 'READY' ? '不可入队' : '须经统一执行复验') + '</span><span class="real">真实：固定关闭</span></div></section>';
            var metricsReadable = historicalFallback || contextState === 'READY' || contextState === 'EMPTY';
            h += '<div class="stats-bar">' + card('同批次股票', metricsReadable ? Number(summary.stock_count || 0) : '—', 'blue') + card('研究候选', metricsReadable ? Number(summary.strategy_candidate_count || 0) : '—', 'orange') + card('研究目标', metricsReadable ? Number(summary.target_count || 0) : '—', 'red') + card('明确拒绝', metricsReadable ? Number(summary.rejected_count || 0) : '—', 'green') + '</div>';
            h += '<section class="sc-panel"><div class="candidate-center-filterbar" aria-label="候选与拒绝筛选"><label><span>决策日期</span><input id="candidateCenterDateFilter" type="date" value="' + escAttr(requestedDate || resolvedDate) + '"></label><label><span>账本状态</span><select id="candidateCenterKindFilter"><option value="">全部状态</option><option value="TARGET">研究目标</option><option value="CANDIDATE">研究候选</option><option value="REJECTED">组合拒绝</option><option value="RESEARCH">研究样本</option></select></label><label class="candidate-center-stock-filter"><span>股票</span><input id="candidateCenterStockFilter" type="search" value="' + escAttr(filterStock) + '" placeholder="代码或名称" autocomplete="off"></label><button id="candidateCenterQueryButton" class="sc-primary" type="button">查询批次</button><span id="candidateCenterFilterCount" class="candidate-center-filter-count"></span></div><div class="sc-table-wrap"><table class="sc-candidate-table"><thead><tr><th>#</th><th>股票</th><th>账本状态</th><th>独立策略</th><th>主题</th><th>研究分 / 净期望</th><th>证据或拒绝原因</th><th>权限</th></tr></thead><tbody id="candidateCenterUnifiedRows"></tbody></table></div><div class="sc-table-note">显示上限 300 条；每一行都属于 run_uid ' + escHtml(pool.run_uid || '-') + '。正式身份：' + escHtml(formalPoolTruth.ready ? 'VERIFIED COMPLETED / 当前请求日' : 'RESEARCH_ONLY / ' + formalPoolTruth.reasonCode) + '。V4 硬拒绝会计入证据覆盖，但只保留在研究审计层；RESEARCH_ONLY 永远不会显示为“可执行”。</div></section></div>';
            container.innerHTML = h;
            var dateInput = container.querySelector('#candidateCenterDateFilter'), stockInput = container.querySelector('#candidateCenterStockFilter'), kindInput = container.querySelector('#candidateCenterKindFilter'), tbody = container.querySelector('#candidateCenterUnifiedRows'), count = container.querySelector('#candidateCenterFilterCount');
            if (kindInput) kindInput.value = filterKind;
            function applyStockFilter() {
                var keyword = String((stockInput || {}).value || '').trim().toLowerCase(), selectedKind = String((kindInput || {}).value || '');
                var filtered = rows.filter(function(row) { var haystack=(String(row.stock_code||'')+' '+String(row.stock_name||'')).toLowerCase();return (!keyword||haystack.indexOf(keyword)>=0)&&(!selectedKind||kindOf(row)===selectedKind); });
                var visible = filtered.slice(0, 300);
                var emptyCopy = historicalFallback ? '历史只读批次没有可展示记录；该回看不代表原请求日没有策略候选。' : contextState === 'LOADING' ? '批次仍在生成，当前没有记录不是空态结论。' : contextState === 'UNAVAILABLE' ? '批次真值或账本不可验证，不能据此判断没有记录。' : contextState === 'BLOCKED' ? '批次已被门禁阻断，不得解释为正常空态。' : contextState === 'STALE' ? '当前是历史或过期证据，不代表现在没有机会。' : contextState === 'EMPTY' ? '完整批次已验证为无研究目标，当前筛选也没有记录。' : '统一批次有效，但当前筛选条件没有记录。';
                tbody.innerHTML = visible.length ? visible.map(function(row, index) { var code=String(row.stock_code||'').padStart(6,'0'),kind=kindOf(row),target=row.target||{};return '<tr><td>' + escHtml(row.rank_no || target.rank_no || index+1) + '</td><td><strong>' + nameLink(code,row.stock_name||code) + '</strong><small>' + escHtml(code) + '</small></td><td><span class="sc-status-pill">' + escHtml(kindText(kind)) + '</span></td><td>' + escHtml((row.strategy_keys||[]).join(' / ')||'-') + '</td><td>' + escHtml((row.theme_codes||[]).join(' / ')||'-') + '</td><td><b class="sc-score">' + escHtml(fmt(row.raw_score,3)) + '</b><small>' + (row.expected_return_net_pct==null?'未校准':escHtml(fmt(row.expected_return_net_pct,2)+'%')) + '</small></td><td title="' + escAttr(reasonText(row)) + '">' + escHtml(localizeMachineText(reasonText(row)).slice(0,180)) + '</td><td><span class="sc-status-pill">' + (historicalFallback ? 'HISTORICAL_READ_ONLY' : 'RESEARCH_ONLY') + '</span><small>' + (historicalFallback ? '历史回看，全部不可执行' : '不可直接下单') + '</small></td></tr>'; }).join('') : '<tr><td colspan="8" class="sc-empty-cell">' + escHtml(emptyCopy) + '</td></tr>';
                count.textContent = '显示 ' + visible.length + ' / ' + filtered.length + ' 条';
                _screenerState.centerFilter = { tradeDate:String((dateInput||{}).value||requestedDate),stock:keyword,kind:selectedKind };
                if (typeof window.updateTradingRouteFilters === 'function') window.updateTradingRouteFilters({trade_date:String((dateInput||{}).value||requestedDate),q:keyword,kind:selectedKind});
            }
            function queryCandidateCenter(){var next=String((dateInput||{}).value||'').trim();if(next){var picker=document.getElementById('datePicker');if(picker)picker.value=next;loadCandidateCenterPage(next,container)}}
            if (stockInput) stockInput.addEventListener('input', applyStockFilter);
            if (kindInput) kindInput.addEventListener('change', applyStockFilter);
            if (dateInput) dateInput.addEventListener('change', queryCandidateCenter);
            var queryButton = container.querySelector('#candidateCenterQueryButton');
            if (queryButton) queryButton.addEventListener('click', queryCandidateCenter);
            applyStockFilter();
        }).catch(function(error) {
            container.innerHTML = '<div class="screen-page"><section class="trade-context-light" data-state="UNAVAILABLE"><div><b>UNAVAILABLE</b><span>候选与拒绝账本读取失败；这不代表候选为空。</span></div><p class="sc-freshness error">' + escHtml(error.message || error) + '</p></section></div>';
        });
    }

    window.runScreen = function (mode) {
        // Highlight active card
        document.querySelectorAll('.screen-card').forEach(function(c) { c.classList.remove('active'); });
        var activeCard = document.getElementById('scard_' + mode);
        if (activeCard) activeCard.classList.add('active');

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
            var html = '<div class="screen-result-summary"><div><div class="screen-result-kicker">筛选结果</div><h3>' + (modeLabels[mode] || mode) + '</h3><p>已按当前交易日完成筛选，可通过搜索快速定位股票。</p></div><div class="screen-result-metrics"><div><strong>' + (res.total || res.data.length) + '</strong><span>候选</span></div><div><strong>' + (res.date || d) + '</strong><span>交易日</span></div></div></div>';
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

    /* ===== 宽基 ETF 资金监测 ===== */
    var _broadEtfFlowChart = null;
    var _broadEtfFlowRequestId = 0;

    function destroyBroadEtfFlowChart() {
        if (_broadEtfFlowChart) {
            try { _broadEtfFlowChart.destroy(); } catch (e) {}
            _broadEtfFlowChart = null;
        }
        if (typeof Chart !== 'undefined' && typeof Chart.getChart === 'function') {
            var canvas = el('broadEtfFlowChart');
            var existing = canvas ? Chart.getChart(canvas) : null;
            if (existing) {
                try { existing.destroy(); } catch (e) {}
            }
        }
    }

    window.stopBroadEtfFlow = function () {
        _broadEtfFlowRequestId += 1;
        destroyBroadEtfFlowChart();
    };

    function broadEtfFinite(value) {
        if (value == null || value === '') return null;
        var number = Number(value);
        return Number.isFinite(number) ? number : null;
    }

    function broadEtfSignedMoney(value) {
        var number = broadEtfFinite(value);
        if (number == null) return '-';
        return (number > 0 ? '+' : '') + fmtMoney(number);
    }

    function broadEtfSignedNumber(value, suffix, digits) {
        var number = broadEtfFinite(value);
        if (number == null) return '-';
        return (number > 0 ? '+' : '') + number.toFixed(digits == null ? 2 : digits) + (suffix || '');
    }

    function broadEtfShareNumber(value) {
        var number = broadEtfFinite(value);
        if (number == null) return '-';
        var sign = number > 0 ? '+' : (number < 0 ? '-' : '');
        var absolute = Math.abs(number);
        if (absolute >= 100000000) return sign + (absolute / 100000000).toFixed(2) + ' 亿份';
        if (absolute >= 10000) return sign + (absolute / 10000).toFixed(1) + ' 万份';
        return sign + absolute.toFixed(0) + ' 份';
    }

    function broadEtfTone(value, fallbackText) {
        var tone = String(value || '').toLowerCase();
        var text = String(fallbackText || '');
        if (['positive', 'inflow', 'entry', 'bullish', 'red'].indexOf(tone) >= 0 || /入场|流入|增持|积极/.test(text)) return 'positive';
        if (['negative', 'outflow', 'exit', 'bearish', 'green'].indexOf(tone) >= 0 || /减持|流出|赎回|谨慎|出货/.test(text)) return 'negative';
        if (['unknown', 'insufficient', 'missing'].indexOf(tone) >= 0 || /不足|缺失|未知/.test(text)) return 'unknown';
        return 'neutral';
    }

    function broadEtfAmountTone(value) {
        var number = broadEtfFinite(value);
        return number == null ? 'unknown' : (number > 0 ? 'positive' : (number < 0 ? 'negative' : 'neutral'));
    }

    function broadEtfAxisMoney(value) {
        var number = Number(value || 0);
        var abs = Math.abs(number);
        if (abs >= 1e8) return (number / 1e8).toFixed(abs >= 1e10 ? 0 : 1) + '亿';
        if (abs >= 1e4) return (number / 1e4).toFixed(0) + '万';
        return String(Math.round(number));
    }

    function broadEtfKpi(label, value, hint) {
        var tone = broadEtfAmountTone(value);
        return '<article class="bef-kpi bef-tone-' + tone + '">' +
            '<span>' + escHtml(label) + '</span>' +
            '<strong>' + escHtml(broadEtfSignedMoney(value)) + '</strong>' +
            '<small>' + escHtml(hint || '宽基 ETF 净申购估算') + '</small>' +
            '</article>';
    }

    function broadEtfEmptyRow(colspan, text) {
        return '<tr><td class="bef-empty-cell" colspan="' + colspan + '">' + escHtml(text || '暂无数据') + '</td></tr>';
    }

    function broadEtfEvidenceTone(item) {
        var text = String((item || {}).kind || '') + ' ' + String((item || {}).title || '') + ' ' + String((item || {}).detail || '');
        if (/反证|风险|减持|流出|赎回|不足|缺失/.test(text)) return 'risk';
        if (/支持|入场|流入|申购|增加/.test(text)) return 'support';
        return 'neutral';
    }

    function broadEtfQualityTone(status) {
        var value = String(status || '').toLowerCase();
        if (/pass|ok|good|complete|valid/.test(value)) return 'good';
        if (/warn|partial|degraded|stale/.test(value)) return 'warn';
        if (/fail|error|missing|invalid/.test(value)) return 'bad';
        return 'neutral';
    }

    function broadEtfSafeUrl(value) {
        try {
            var url = new URL(String(value || ''), window.location.origin);
            return url.protocol === 'http:' || url.protocol === 'https:' ? url.href : '';
        } catch (e) {
            return '';
        }
    }

    function renderBroadEtfFlowPage(container, data) {
        data = data || {};
        var summary = data.summary || {};
        var history = Array.isArray(data.history) ? data.history.slice() : [];
        var benchmarks = Array.isArray(data.benchmarks) ? data.benchmarks : [];
        var etfs = Array.isArray(data.etfs) ? data.etfs : [];
        var evidence = Array.isArray(data.evidence) ? data.evidence : [];
        var caveats = Array.isArray(data.caveats) ? data.caveats : [];
        var sources = Array.isArray(data.sources) ? data.sources : [];
        var coverage = data.coverage || {};
        var signal = summary.signal || '数据不足';
        var signalTone = broadEtfTone(summary.signal_tone, signal);
        var coveragePct = broadEtfFinite(summary.coverage_pct);
        var positiveCount = broadEtfFinite(summary.positive_count);
        var totalCount = broadEtfFinite(summary.total_count);
        var confidence = broadEtfFinite(summary.confidence);
        var confidenceText = confidence == null ? '-' : confidence.toFixed(0) + '%';
        if (summary.confidence_label) confidenceText += ' · ' + summary.confidence_label;
        history.sort(function (a, b) { return String(a.trade_date || '').localeCompare(String(b.trade_date || '')); });

        var h = '<div class="bef-page">';
        h += '<section class="bef-hero">';
        h += '<div class="bef-hero-copy"><span class="bef-eyebrow">BROAD-BASED ETF FLOW PROXY</span><h2>宽基 ETF 资金监测</h2>';
        h += '<p>用公开份额变化估算宽基 ETF 净申购，辅助观察大资金方向；这是代理信号，不能识别最终持有人。</p></div>';
        h += '<div class="bef-signal bef-tone-' + signalTone + '"><small>国家队动向代理</small><strong>' + escHtml(signal) + '</strong><span>置信度 ' + escHtml(confidenceText) + '</span></div>';
        h += '</section>';

        h += '<div class="bef-meta" aria-label="数据口径状态">';
        h += '<span>请求日 <strong>' + escHtml(data.requested_date || '-') + '</strong></span>';
        h += '<span>交易日 <strong>' + escHtml(data.trade_date || '-') + '</strong></span>';
        h += '<span>数据截至 <strong>' + escHtml(data.data_as_of || '-') + '</strong></span>';
        h += '<span>覆盖 <strong>' + (coveragePct == null ? '-' : coveragePct.toFixed(0) + '%') + '</strong></span>';
        h += '</div>';

        h += '<section class="bef-kpis" aria-label="宽基资金关键指标">';
        h += broadEtfKpi('今日净申购估算', summary.net_1d, '份额变化 × 前一交易日收盘价');
        h += broadEtfKpi('近 3 日累计', summary.net_3d, '连续性观察');
        h += broadEtfKpi('近 5 日累计', summary.net_5d, '短周期方向');
        h += broadEtfKpi('近 20 日累计', summary.net_20d, '中周期方向');
        h += '<article class="bef-kpi bef-coverage"><span>净申购 ETF 数</span><strong>' + (positiveCount == null ? '-' : positiveCount.toFixed(0)) + '<em> / ' + (totalCount == null ? '-' : totalCount.toFixed(0)) + '</em></strong><small>当日估算净申购为正</small></article>';
        h += '</section>';

        h += '<section class="bef-overview-grid">';
        h += '<article class="bef-panel bef-chart-panel"><div class="bef-panel-head"><div><span>日度趋势</span><h3>净申购估算与区间累计</h3></div><p>最近 ' + escHtml(data.window_days || 20) + ' 个交易日 · 金额单位随刻度显示</p></div>';
        h += history.length ? '<div class="bef-chart-wrap"><canvas id="broadEtfFlowChart" role="img" aria-label="宽基 ETF 日度净申购柱状图与累计金额折线图"></canvas></div>' : '<div class="bef-empty">暂无趋势数据</div>';
        h += '</article>';
        h += '<aside class="bef-panel bef-evidence-panel"><div class="bef-panel-head"><div><span>判断依据</span><h3>证据与反证</h3></div></div><div class="bef-evidence-list">';
        if (!evidence.length) {
            h += '<div class="bef-empty">暂无可用证据</div>';
        } else {
            evidence.forEach(function (item) {
                var tone = broadEtfEvidenceTone(item);
                h += '<article class="bef-evidence bef-evidence-' + tone + '"><span>' + escHtml(item.kind || '观察') + '</span><strong>' + escHtml(item.title || '-') + '</strong><p>' + escHtml(item.detail || '-') + '</p></article>';
            });
        }
        h += '</div></aside></section>';

        h += '<section class="bef-panel"><div class="bef-panel-head"><div><span>基准拆解</span><h3>各宽基贡献汇总</h3></div><p>正数为净申购，负数为净赎回</p></div><div class="bef-table-wrap"><table class="bef-table bef-benchmark-table"><thead><tr><th>跟踪基准</th><th>今日估算</th><th>近3日</th><th>近5日</th><th>近20日</th><th>份额变化</th><th>净申购数</th></tr></thead><tbody>';
        if (!benchmarks.length) h += broadEtfEmptyRow(7, '暂无基准汇总数据');
        benchmarks.forEach(function (row) {
            h += '<tr><td><strong>' + escHtml(row.benchmark || '-') + '</strong></td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.net_1d) + '">' + escHtml(broadEtfSignedMoney(row.net_1d)) + '</td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.net_3d) + '">' + escHtml(broadEtfSignedMoney(row.net_3d)) + '</td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.net_5d) + '">' + escHtml(broadEtfSignedMoney(row.net_5d)) + '</td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.net_20d) + '">' + escHtml(broadEtfSignedMoney(row.net_20d)) + '</td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.share_change) + '">' + escHtml(broadEtfShareNumber(row.share_change)) + '</td>' +
                '<td>' + escHtml(row.positive_count == null ? '-' : row.positive_count) + ' / ' + escHtml(row.total_count == null ? '-' : row.total_count) + '</td></tr>';
        });
        h += '</tbody></table></div></section>';

        h += '<section class="bef-panel"><div class="bef-panel-head"><div><span>ETF 明细</span><h3>当日份额与资金变化</h3></div><p>共 ' + etfs.length + ' 只</p></div><div class="bef-table-wrap"><table class="bef-table bef-detail-table"><thead><tr><th>日期</th><th>代码 / 名称</th><th>跟踪基准</th><th>净申购估算</th><th>份额变化</th><th>份额变化率</th><th>总份额</th><th>价格 / 涨跌</th><th>成交额</th><th>来源</th><th>质量</th></tr></thead><tbody>';
        if (!etfs.length) h += broadEtfEmptyRow(11, '暂无 ETF 明细数据');
        etfs.forEach(function (row) {
            var quality = row.quality_status || '-';
            h += '<tr><td>' + escHtml(row.trade_date || data.trade_date || '-') + '</td>' +
                '<td><strong class="bef-code">' + escHtml(row.etf_code || '-') + '</strong><small>' + escHtml(row.short_name || '-') + (row.exchange ? ' · ' + escHtml(row.exchange) : '') + '</small></td>' +
                '<td>' + escHtml(row.benchmark || '-') + '</td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.net_amount) + '"><strong>' + escHtml(broadEtfSignedMoney(row.net_amount)) + '</strong></td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.share_change) + '">' + escHtml(broadEtfShareNumber(row.share_change)) + '</td>' +
                '<td class="bef-number bef-tone-' + broadEtfAmountTone(row.share_change_pct) + '">' + escHtml(broadEtfSignedNumber(row.share_change_pct, '%', 2)) + '</td>' +
                '<td class="bef-number">' + escHtml(broadEtfShareNumber(row.fund_share).replace(/^\+/, '')) + '</td>' +
                '<td class="bef-number">' + escHtml(fmt(row.price, 3)) + '<small class="bef-tone-' + broadEtfAmountTone(row.change_pct) + '">' + escHtml(pct(row.change_pct)) + '</small></td>' +
                '<td class="bef-number">' + escHtml(fmtMoney(row.amount)) + '</td>' +
                '<td>' + escHtml(row.source || '-') + '</td>' +
                '<td><span class="bef-quality bef-quality-' + broadEtfQualityTone(quality) + '">' + escHtml(quality) + '</span></td></tr>';
        });
        h += '</tbody></table></div></section>';

        h += '<details class="bef-method"><summary>口径说明、覆盖范围与数据源</summary><div class="bef-method-body">';
        h += '<section><h3>估算口径</h3><p><strong>日度净申购估算 = 基金份额日变化 × 前一交易日收盘价。</strong>该指标反映 ETF 一级市场份额变化对应的近似资金量，不等于二级市场成交净流入，也不能确认申购或赎回方身份。</p></section>';
        h += '<section><h3>覆盖情况</h3><p>预期 ' + escHtml(coverage.expected == null ? '-' : coverage.expected) + ' 只，可用 ' + escHtml(coverage.available == null ? '-' : coverage.available) + ' 只。';
        if (Array.isArray(coverage.missing) && coverage.missing.length) h += ' 缺失：' + coverage.missing.map(function (item) { return escHtml(typeof item === 'object' ? ((item.etf_code || '') + (item.short_name ? ' ' + item.short_name : '')) : item); }).join('、') + '。';
        h += '</p></section>';
        h += '<section><h3>注意事项</h3><ul><li>“国家队动向”仅为宽基 ETF 资金的代理观察，不构成账户身份识别或投资建议。</li>';
        caveats.forEach(function (item) { h += '<li>' + escHtml(item) + '</li>'; });
        h += '</ul></section>';
        if (sources.length) {
            h += '<section><h3>数据源</h3><ul>';
            sources.forEach(function (source) {
                var url = broadEtfSafeUrl(source.url);
                var name = escHtml(source.name || source.url || '数据源');
                h += '<li>' + (url ? '<a href="' + escAttr(url) + '" target="_blank" rel="noopener noreferrer">' + name + '</a>' : name) + (source.note ? '：' + escHtml(source.note) : '') + '</li>';
            });
            h += '</ul></section>';
        }
        h += '</div></details></div>';
        container.innerHTML = h;
        initBroadEtfFlowChart(history);
    }

    function initBroadEtfFlowChart(history) {
        destroyBroadEtfFlowChart();
        var canvas = el('broadEtfFlowChart');
        if (!canvas || typeof Chart === 'undefined' || !history.length) return;
        var netValues = history.map(function (row) { return broadEtfFinite(row.net_amount); });
        var cumulativeValues = history.map(function (row) { return broadEtfFinite(row.cumulative_amount); });
        _broadEtfFlowChart = new Chart(canvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: history.map(function (row) { return String(row.trade_date || '').slice(5); }),
                datasets: [{
                    type: 'bar',
                    label: '日度净申购估算',
                    data: netValues,
                    backgroundColor: netValues.map(function (value) { return value >= 0 ? 'rgba(220, 38, 38, .72)' : 'rgba(22, 163, 74, .72)'; }),
                    borderColor: netValues.map(function (value) { return value >= 0 ? '#dc2626' : '#16a34a'; }),
                    borderWidth: 1,
                    borderRadius: 3,
                    maxBarThickness: 26,
                    yAxisID: 'y'
                }, {
                    type: 'line',
                    label: '区间累计',
                    data: cumulativeValues,
                    borderColor: '#2563eb',
                    backgroundColor: '#2563eb',
                    borderWidth: 2,
                    pointRadius: history.length > 30 ? 0 : 2,
                    pointHoverRadius: 4,
                    tension: .28,
                    spanGaps: true,
                    yAxisID: 'y1'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top', align: 'end', labels: { color: '#64748b', boxWidth: 11, usePointStyle: true, font: { size: 11 } } },
                    tooltip: { callbacks: { label: function (context) { return context.dataset.label + '：' + broadEtfSignedMoney(context.raw); } } }
                },
                scales: {
                    x: { grid: { display: false }, ticks: { color: '#8290a3', maxRotation: 0, autoSkip: true, maxTicksLimit: 10, font: { size: 10 } } },
                    y: { position: 'left', grid: { color: 'rgba(148, 163, 184, .16)' }, ticks: { color: '#8290a3', callback: broadEtfAxisMoney, font: { size: 10 } } },
                    y1: { position: 'right', grid: { drawOnChartArea: false }, ticks: { color: '#2563eb', callback: broadEtfAxisMoney, font: { size: 10 } } }
                }
            }
        });
    }

    function loadBroadEtfFlowPage(requestedDate, container) {
        var keepCurrent = silentRefreshDepth > 0 && hasRenderedContent(container);
        var requestId = ++_broadEtfFlowRequestId;
        if (!keepCurrent) {
            destroyBroadEtfFlowChart();
            container.innerHTML = '<div class="loading"><span class="spinner"></span> 正在读取宽基 ETF 资金数据...</div>';
        }
        return fetchJsonWithTimeout('/broad-etf-flow?trade_date=' + encodeURIComponent(requestedDate || currentDateValue()) + '&days=20', 45000)
            .then(function (data) {
                if (requestId !== _broadEtfFlowRequestId || activeTabId() !== 'broad-etf-flow') return { cancelled: true };
                renderBroadEtfFlowPage(container, data);
                var degraded = ['degraded', 'insufficient', 'error'].indexOf(String(data.status || '').toLowerCase()) >= 0;
                setStatus(degraded ? '宽基资金数据覆盖不足' : '宽基资金已更新');
                return data;
            })
            .catch(function (error) {
                if (requestId !== _broadEtfFlowRequestId) return { cancelled: true };
                if (!keepCurrent) {
                    container.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 宽基资金加载失败：' + escHtml(error.message || '网络异常') + '</div>';
                }
                setStatus(keepCurrent ? '宽基资金刷新失败，已保留现有结果' : '宽基资金加载失败', true);
                return { loadError: error.message || '网络异常' };
            });
    }

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
            var accepted = res && res.accepted === true;
            alert((accepted ? '任务已受审计地进入后台执行。' : '任务未启动。') + ' 状态: ' + ((res && res.status) || 'unknown'));
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

    function mainforceScanSummary(rows) {
        var summary = {'建仓': 0, '洗盘': 0, '出货': 0};
        (rows || []).forEach(function (row) {
            if (summary[row.behavior] != null) summary[row.behavior] += 1;
        });
        return summary;
    }

    function mainforceAverageConfidence(rows) {
        var values = (rows || []).map(function (row) { return Number(row.confidence); }).filter(Number.isFinite);
        if (!values.length) return '--';
        return Math.round(values.reduce(function (sum, value) { return sum + value; }, 0) / values.length) + '%';
    }

    function mainforceFilterCard(label, value, color, behavior, active) {
        return '<div class="stat-card mf-filter-card' + (active ? ' active' : '') + '" role="button" tabindex="0" data-mainforce-filter="' + (behavior || '') + '" title="' + (behavior ? '筛选' + behavior : '清除筛选') + '">' +
            '<div class="label">' + label + '</div><div class="value ' + color + '">' + value + '</div></div>';
    }

    function bindMainforceFilterCards() {
        document.querySelectorAll('#mainforceContent .mf-filter-card').forEach(function (cardEl) {
            var behavior = cardEl.getAttribute('data-mainforce-filter') || '';
            var run = function () { window.filterMainforceScan(behavior); };
            cardEl.addEventListener('click', run);
            cardEl.addEventListener('keydown', function (event) {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    run();
                }
            });
        });
    }

    function renderMainforceScanResults(container, res, behaviorFilter) {
        var allRows = res.results || [];
        var rows = behaviorFilter ? allRows.filter(function (row) { return row.behavior === behaviorFilter; }) : allRows;
        var summary = mainforceScanSummary(allRows);
        var h = '<div class="stats-bar">';
        h += mainforceFilterCard('建仓信号', summary['建仓'] || 0, 'red', '建仓', behaviorFilter === '建仓');
        h += mainforceFilterCard('洗盘信号', summary['洗盘'] || 0, 'orange', '洗盘', behaviorFilter === '洗盘');
        h += mainforceFilterCard('出货信号', summary['出货'] || 0, 'green', '出货', behaviorFilter === '出货');
        h += mainforceFilterCard('平均置信度', mainforceAverageConfidence(allRows), 'blue', '', false);
        h += '</div>';
        h += '<div class="mf-filter-note">' + (behaviorFilter ? '当前筛选：' + behaviorFilter + ' · ' + rows.length + ' / ' + allRows.length + ' 条' : '全部结果：' + allRows.length + ' 条') + '</div>';
        window.renderTable(container, 'mfScan',
            ['股票代码', '名称', '现价', '涨跌幅', '主力行为', '置信度', '综合得分', '量价信号', '资金信号', 'K线信号', '筹码信号', '机构信号', '操作'],
            rows, function (r) {
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
        bindMainforceFilterCards();
    }

    window.filterMainforceScan = function (behavior) {
        var state = window._mainforceScanState;
        var container = document.getElementById('mainforceContent');
        if (!state || !state.response || !container) return;
        state.filter = state.filter === behavior ? '' : behavior;
        renderMainforceScanResults(container, state.response, state.filter);
    };

    window.doMainforceScan = function () {
        var container = document.getElementById('mainforceContent');
        if (!container) return;
        container.innerHTML = '<div class="loading"><span class="spinner"></span> 全市场扫描中，请稍候...</div>';
        var d = el('datePicker').value;
        fetch(API_BASE + '/mainforce-scan?trade_date=' + d + '&top=50').then(function (r) { return r.json(); }).then(function (res) {
            if (!res.results || !res.results.length) {
                container.innerHTML = '<div class="loading">暂无扫描结果</div>';
            }
            window._mainforceScanState = {response: res, filter: ''};
            renderMainforceScanResults(container, res, '');
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
        if (!c) { c = el(tabId); } if (!c) return null;
        if ((tabId === 'ai-stock' || tabId === 'ai-general') && c.querySelector('.ai-embedded-frame')) {
            setStatus((tabId === 'ai-stock' ? '股票问答' : '通用问答') + '已打开');
            return null;
        }
        var keepCurrent = options.silent && hasRenderedContent(c);
        if (keepCurrent) markSilentRefreshTarget(c);
        if (!keepCurrent) {
            c.innerHTML = '<div class="loading">加载中...</div>';
        }
        var loader = LOADERS[tabId];
        var loadResult = null;
        try {
            if (loader) {
                if (keepCurrent && tabId !== 'portfolio') {
                    loadResult = runWithSilentRefresh(function () { return loader(d, c, options); });
                } else {
                    // Portfolio may retry across a brief deploy/tunnel outage.
                    // Its own target guard preserves the current table; keeping
                    // the global silent-refresh depth open for that whole retry
                    // window would suppress loading placeholders on other tabs.
                    loadResult = loader(d, c, options);
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
        return loadResult;
    }

    function refreshAll() {
        setStatus('后台刷新中...');
        var a = document.querySelector('.sidebar-item.active');
        var result = null;
        if (a) {
            var id = a.getAttribute('data-tab');
            if (id && LOADERS[id]) result = refreshLoadTab(id);
        }
        if (result && typeof result.then === 'function') {
            return result.then(function(outcome) {
                if (outcome && outcome.loadError) {
                    setStatus('刷新失败: ' + outcome.loadError, true);
                    return outcome;
                }
                setStatus('刷新完成');
                return outcome;
            }).catch(function(e) {
                setStatus('刷新失败: ' + (e.message || '网络异常'), true);
                return null;
            });
        }
        setStatus('刷新请求已提交');
        return result;
    }
    window.refreshAll = refreshAll;

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
                if (!res.accepted) {
                    alert('❌ 东财同步任务提交失败: ' + (res.error || res.status || '未知错误'));
                    return;
                }
                alert('东财同步任务已提交后台执行' + (res.job_id ? '（任务号：' + res.job_id + '）' : ''));
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
        if (!qs.length) {
            qs.push('trade_date=' + encodeURIComponent(d || recommendationDateValue() || currentDateValue()));
            qs.push('prefer_latest=true');
        }
        return '/recommended-stocks?' + qs.join('&');
    }

    function recGateUrl(d) {
        var targetDate = d || recommendationDateValue() || currentDateValue();
        var executionDate = currentDateValue();
        var qs = [
            'check_readiness=false',
            'target_trade_date=' + encodeURIComponent(targetDate),
            'execution_time=' + encodeURIComponent(executionDate + ' 08:30:00')
        ];
        return '/recommended-stocks/gate?' + qs.join('&');
    }

    function recRuntimeParamsUrl(d) {
        var targetDate = d || recommendationDateValue() || currentDateValue();
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

    function renderRecommendationThemeOverview(data) {
        var themes = Array.isArray(data.theme_overview) ? data.theme_overview : [];
        if (!themes.length) return '';
        var coverage = data.theme_coverage || {};
        var dimensions = Array.isArray(data.theme_scan_dimensions) ? data.theme_scan_dimensions : [];
        var unclassified = Array.isArray(data.unclassified_catalysts) ? data.unclassified_catalysts : [];
        var statusColors = {
            '主线候选': '#dc2626',
            '轮动候选': '#ea580c',
            '事件观察': '#2563eb',
            '逻辑转弱': '#7c3aed',
            '常规观察': '#64748b'
        };
        var tierColors = {S: '#b91c1c', A: '#dc2626', B: '#d97706', '观察': '#64748b', '风险观察': '#7c3aed'};
        var html = '';
        html += '<section style="background:linear-gradient(135deg,#fff,#f8fafc);border:1px solid #dbe4ee;border-radius:14px;padding:14px;margin-bottom:16px;">';
        html += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;">';
        html += '<div><div style="font-size:16px;font-weight:900;color:#0f172a;">全市场催化候选池</div>';
        html += '<div style="font-size:12px;color:#64748b;margin-top:4px;line-height:1.6;">先展示全部市场逻辑，再筛选股票。主题即使没有个股通过门禁，也不会从推荐整体中消失。</div></div>';
        html += '<div style="font-size:11px;color:#64748b;text-align:right;">雷达 ' + safeText(data.theme_radar_version || '-') + '<br>截止 ' + safeText(coverage.radar_cutoff_at || '-') + '</div>';
        html += '</div>';
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:8px;margin-top:12px;">';
        [
            ['扫描维度', coverage.scanned_dimension_count || dimensions.length || 0, '#2563eb'],
            ['扫描主题', coverage.scanned_theme_count || themes.length || 0, '#0f172a'],
            ['当前有证据', coverage.active_theme_count || 0, '#dc2626'],
            ['已映射推荐', coverage.represented_theme_count || 0, '#16a34a'],
            ['有主题无合格股', coverage.unrepresented_active_theme_count || 0, '#d97706'],
            ['逻辑转弱', coverage.weakening_theme_count || 0, '#7c3aed']
        ].forEach(function(metric) {
            html += '<div style="background:#fff;border:1px solid #e2e8f0;border-radius:9px;padding:9px;text-align:center;">';
            html += '<div style="font-size:20px;font-weight:900;color:' + metric[2] + ';">' + safeText(metric[1]) + '</div>';
            html += '<div style="font-size:10px;color:#64748b;">' + metric[0] + '</div></div>';
        });
        html += '</div>';
        if (dimensions.length) {
            html += '<div style="margin-top:10px;font-size:11px;color:#64748b;line-height:1.7;"><strong style="color:#334155;">已检查：</strong>' + dimensions.map(safeText).join(' · ') + '</div>';
        }
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px;margin-top:12px;">';
        themes.forEach(function(theme) {
            var color = statusColors[theme.status] || '#64748b';
            var tierColor = tierColors[theme.rank_tier] || color;
            var triggers = (theme.trigger_labels || []).map(function(label) {
                return '<span style="display:inline-block;padding:2px 6px;margin:2px 4px 2px 0;border-radius:999px;background:#f1f5f9;color:#475569;font-size:10px;">' + safeText(label) + '</span>';
            }).join('');
            var catalysts = (theme.catalysts || []).slice(0, 2).map(function(item) {
                var prefix = Number(item.direction || 0) < 0 ? '风险：' : '催化：';
                return '<div style="font-size:11px;color:#475569;line-height:1.55;margin-top:3px;">' + prefix + safeText(item.title || '-') + '</div>';
            }).join('');
            var candidates = (theme.candidate_names || []).slice(0, 6).join('、');
            html += '<article style="background:#fff;border:1px solid #e2e8f0;border-left:4px solid ' + color + ';border-radius:10px;padding:10px 11px;min-width:0;">';
            html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start;">';
            html += '<div style="min-width:0;"><div style="font-size:10px;color:#94a3b8;">#' + safeText(theme.rank || '-') + ' · ' + safeText(theme.category || '-') + '</div>';
            html += '<div style="font-size:13px;font-weight:900;color:#0f172a;line-height:1.45;">' + safeText(theme.name || '-') + '</div></div>';
            html += '<div style="white-space:nowrap;text-align:right;"><span style="font-size:11px;font-weight:900;color:' + tierColor + ';">' + safeText(theme.rank_tier || '观察') + '</span><br><strong style="font-size:18px;color:' + color + ';">' + safeText(theme.score == null ? '-' : theme.score) + '</strong></div>';
            html += '</div>';
            html += '<div style="margin-top:5px;font-size:11px;font-weight:800;color:' + color + ';">' + safeText(theme.status || '-') + ' · ' + safeText(theme.coverage_status || '-') + '</div>';
            if (triggers) html += '<div style="margin-top:5px;">' + triggers + '</div>';
            html += catalysts;
            html += '<div style="font-size:11px;color:' + (theme.candidate_count ? '#166534' : '#9a3412') + ';line-height:1.55;margin-top:5px;"><strong>推荐映射：</strong>' + safeText(candidates || '暂无合格标的，主题继续保留观察') + '</div>';
            html += '<details style="margin-top:6px;"><summary style="cursor:pointer;font-size:10px;color:#2563eb;">展开逻辑、验证与风险</summary>';
            html += '<div style="font-size:11px;color:#475569;line-height:1.6;margin-top:5px;"><strong>逻辑：</strong>' + safeText(theme.logic || '-') + '<br><strong>验证：</strong>' + safeText(theme.verification || '-') + '<br><strong>风险：</strong>' + safeText(theme.risk || '-') + '</div>';
            html += '</details></article>';
        });
        html += '</div>';
        if (unclassified.length) {
            html += '<div style="margin-top:12px;padding:10px 12px;background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;">';
            html += '<div style="font-size:12px;font-weight:900;color:#9a3412;">未归类高优先级催化（保留，不静默丢弃）</div>';
            html += '<div style="font-size:11px;color:#7c2d12;line-height:1.7;margin-top:4px;">' + unclassified.slice(0, 10).map(function(item) { return safeText(item.title || '-'); }).join('；') + '</div></div>';
        }
        html += '</section>';
        return html;
    }

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
            var m = {WATCH: '观察', CONFIRM: '确认', BUY_READY: '买入就绪', SELL_ALERT: '卖出提醒', BLOCK: '屏蔽', ALLOW: '允许', SUSPENDED: '暂停', DATA_BLOCKED: '数据阻断', EXECUTION_BLOCKED: '执行阻断', REDUCE: '减仓'};
            return m[v] || v || '-';
        }
        function recStatusColor(v) {
            var m = {WATCH: '#f39c12', CONFIRM: '#1a73e8', BUY_READY: '#27ae60', SELL_ALERT: '#e67e22', BLOCK: '#e74c3c', SUSPENDED: '#f39c12', ALLOW: '#27ae60', DATA_BLOCKED: '#e74c3c', EXECUTION_BLOCKED: '#c0392b', REDUCE: '#e67e22'};
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
        var readiness = gate.data_readiness || {};
        var readinessSources = Array.isArray(readiness.sources) ? readiness.sources : [];
        if (readinessSources.length) {
            var readinessMissing = readinessSources.filter(function(s) { return s.required && !s.ready; }).map(function(s) { return s.key; }).join('、');
            h += '<div style="background:#f8fafc;border:1px solid #dbe4ee;border-radius:12px;padding:10px 12px;margin-bottom:14px;">';
            h += '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;">';
            h += '<div style="font-size:13px;font-weight:900;color:#0f172a;">推荐数据完整度 · ' + safeText(readiness.trade_date || '-') + '</div>';
            h += '<div style="font-size:12px;color:' + (readinessMissing ? '#dc2626' : '#16a34a') + ';font-weight:800;">' + (readinessMissing ? '待补齐：' + safeText(readinessMissing) : '核心数据已满足生成门槛') + '</div>';
            h += '</div><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-top:8px;">';
            readinessSources.forEach(function(s) {
                var pct = Math.round(Number(s.coverage || 0) * 100);
                var color = s.ready ? '#16a34a' : (s.required ? '#dc2626' : '#d97706');
                h += '<div style="background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:8px;"><div style="font-size:11px;color:#64748b;">' + safeText(s.key || '-') + '</div><div style="font-size:16px;font-weight:900;color:' + color + ';">' + pct + '%</div><div style="font-size:10px;color:#94a3b8;">' + (s.count || 0) + '/' + (s.expected || '-') + '</div></div>';
            });
            h += '</div></div>';
        }
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

        h += renderRecommendationThemeOverview(data);

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
            if (hasExplicitNewBuyGate(r)) buyCount++;
            else if (status === 'BLOCK' || String(r.chase_risk_status || '').toUpperCase() !== 'ALLOW' || !(r.ordinary_buy_eligible === true || r.ordinary_buy_eligible === 1)) blockCount++;
            else watchCount++;
        });

        // 推荐买入汇总卡片
        h += '<div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;">';
        h += '<div style="flex:1;min-width:100px;background:linear-gradient(135deg,#0d4a0d,#1a7a1a);border-radius:10px;padding:12px;text-align:center;color:#fff;border:1px solid #27ae60;">';
        h += '<div style="font-size:28px;font-weight:800;color:#2ecc71;">' + buyCount + '</div><div style="font-size:11px;color:#a8e6cf;">✅ 四门确认候选</div></div>';
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
        h += '<div style="background:#0f172a;color:#e2e8f0;border-radius:12px;padding:12px 14px;margin-bottom:14px;">';
        h += '<div style="font-size:13px;font-weight:900;color:#fff;margin-bottom:8px;">AI 策略自动化流程 · 北京时间</div>';
        h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;font-size:12px;line-height:1.5;">';
        h += '<div style="background:#1e293b;border-radius:8px;padding:9px;"><strong style="color:#93c5fd;">前一交易日 20:00</strong><br>生成次日候选池、观察区间、止损止盈和失效条件</div>';
        h += '<div style="background:#1e293b;border-radius:8px;padding:9px;"><strong style="color:#86efac;">交易日 09:00</strong><br>盘前复核数据完整度、公告风险和优先级</div>';
        h += '<div style="background:#1e293b;border-radius:8px;padding:9px;"><strong style="color:#fde68a;">10:00 / 11:00 / 13:00 / 14:00</strong><br>按策略检查买卖条件、实时价格和风险变化</div>';
        h += '<div style="background:#1e293b;border-radius:8px;padding:9px;"><strong style="color:#fca5a5;">交易日 15:30</strong><br>输出胜率、收益、回撤、盈亏比和策略复盘</div>';
        h += '</div><div style="margin-top:9px;font-size:11px;color:#cbd5e1;">安全边界：仅生成研究候选、提醒和买卖建议；不会登录券商、下单、撤单或自动执行任何交易。任何模拟盘/实盘操作都必须先展示确认单并取得你的明确确认。</div></div>';
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
        h += '<option value="recommend">条件确认</option>';
        h += '<option value="caution">执行门未齐</option>';
        h += '<option value="watch">观望</option>';
        h += '<option value="reduce">减仓</option>';
        h += '<option value="sell">卖出提醒</option>';
        h += '<option value="block">不推荐</option>';
        h += '<option value="suspended">暂停</option>';
        h += '</select>';
        h += '<button onclick="window._filterRecommendedAll()" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:12px;cursor:pointer;">筛选</button>';
        h += '</div>';

        if (!items.length) {
            var emptyTitle = rawItems.length ? '当前筛选条件下暂无候选' : (fresh.status === 'missing' ? '目标交易日尚未生成推荐' : '当前没有可展示的推荐候选');
            var emptyReason = rawItems.length ? '可以放宽策略、状态或最低分筛选。' : (readinessSources.some(function(s) { return s.required && !s.ready; }) ? '基础数据覆盖不足，系统会先补齐 K 线和资金流，再重新运行推荐。' : '请先运行 AI 推荐筛选，或切换到最近一个有数据的推荐日。');
            h += '<div style="margin:0 0 14px;padding:12px 14px;border-radius:10px;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;"><div style="font-weight:900;">' + emptyTitle + '</div><div style="font-size:12px;margin-top:4px;line-height:1.6;">' + emptyReason + ' 不会自动下单，盘中建议仍需人工确认。</div></div>';
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
            var rowStatus = r.recommend_status || 'DATA_BLOCKED';
            var stScore = firstAnalysisValue(r.short_term_score, blendedAnalysisRowScore(r), 0) || 0;
            var rowMainWave = r.main_wave_signal || 'NONE';
            var rowAdvice = 'watch';
            if (signalStatus === 'SELL_ALERT' || r.main_wave_signal === 'SELL_ALERT') { rowAdvice = 'sell'; }
            else if (r.main_wave_signal === 'REDUCE') { rowAdvice = 'reduce'; }
            else if (hasExplicitNewBuyGate(r)) { rowAdvice = 'buy_ready'; }
            else if (rowStatus === 'BLOCK') { rowAdvice = 'block'; }
            else if (rowStatus === 'SUSPENDED') { rowAdvice = 'suspended'; }
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
                } else if (hasExplicitNewBuyGate(r)) {
                    badgeText = '✅ 四门确认（成交前复验）'; badgeBg = 'linear-gradient(135deg,#1e8449,#27ae60)'; badgeColor = '#fff';
                } else if (rowStatus === 'BLOCK') {
                    badgeText = '❌ 不推荐'; badgeBg = 'linear-gradient(135deg,#c0392b,#e74c3c)'; badgeColor = '#fff';
                } else if (rowStatus === 'SUSPENDED') {
                    badgeText = '⚠️ 暂停'; badgeBg = 'linear-gradient(135deg,#d4a017,#f39c12)'; badgeColor = '#fff';
                } else if (stScore >= 60) {
                    badgeText = '⚡ 仅观察，执行门未齐'; badgeBg = 'linear-gradient(135deg,#b7950b,#f1c40f)'; badgeColor = '#fff';
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
                if (res.status === 'DATA_UNAVAILABLE' || res.error_code) {
                    mount.innerHTML = '<div style="padding:10px 12px;background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;color:#9a3412;font-size:12px;">' + escHtml(res.message || '筛选执行记录暂不可用，请稍后重试。') + '</div>';
                    return;
                }
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
        var tradeDate = recommendationDateValue() || currentDateValue();
        var executionTime = localDateTimeString(new Date());
        fetch('/api/hot-data/recommended-stocks/run?min_score=' + minScore + '&top_n=80&strict_prev_trade_day=true&execution_time=' + encodeURIComponent(executionTime) + '&min_kline_coverage=0.80&auto_repair_missing_kline=false&refresh_realtime=false&date_policy=previous_complete', { method: 'POST' })
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
                if (!res.accepted) {
                    throw new Error(res.error || res.status || '生产调度器拒绝任务');
                }
                var jobId = res.job_id || res.scheduler_job_id || '';
                if (statusEl) statusEl.textContent = '已提交后台执行' + (jobId ? '（' + jobId + '）' : '');
                _updateRecommendedProgressUI({
                    status: 'running',
                    step: res.note || '已提交生产调度器',
                    percent: 1,
                    trade_date: res.date || tradeDate,
                    run_uid: res.run_uid || '',
                    scheduler_job_id: jobId
                });
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
                        if (c) loadRecommendedPage(recommendationDateValue() || currentDateValue(), c);
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
        var holdingLimitLabel = '数量不设上限';
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
                        exit_reasons: [],
                        exit_action: 'HOLD',
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
                if (ph.exit_reason_detail) {
                    var exitText = (ph.buy_date ? String(ph.buy_date).slice(0, 10) + '批次：' : '') + ph.exit_reason_detail;
                    if (map[key].exit_reasons.indexOf(exitText) < 0) map[key].exit_reasons.push(exitText);
                }
                if (ph.exit_action === 'SELL') map[key].exit_action = 'SELL';
                else if (ph.exit_action === 'WAIT' && map[key].exit_action !== 'SELL') map[key].exit_action = 'WAIT';
                map[key].children.push(ph);
            });
            return Object.keys(map).map(function(key) {
                var g = map[key];
                g.batch_count = g.children.length;
                g.buy_price = g.shares > 0 ? g.cost / g.shares : 0;
                g.cur_price = g.shares > 0 ? g.market_value / g.shares : 0;
                g.pnl_rate = g.cost > 0 ? g.pnl / g.cost * 100 : 0;
                g.ai_score = g.score_base > 0 ? g.score_weight / g.score_base : 0;
                g.exit_reason_detail = g.exit_reasons.join('；') || '等待卖出规则检查';
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
            h += metricCard('持仓股票', holdingGroups.length, '买入批次 ' + allHoldings.length + ' · ' + holdingLimitLabel, holdingGroups.length ? 'good' : 'neutral');
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
                h += statusCard('盘中执行', intradayWindow.is_trading_time ? '实时判断中' : (intradayWindow.label || '-'), '买入：交易时段持续看盘 / 卖出：每分钟风控', intradayWindow.is_trading_time ? 'good' : 'neutral');
                h += statusCard('调度状态', scheduler.standalone_online ? '独立调度在线' : (scheduler.embedded_running ? '内嵌调度运行' : '未检测到调度'), 'API重启安全: ' + (scheduler.api_restart_safe ? '是' : '否'), scheduler.standalone_online || scheduler.embedded_running ? 'good' : 'warn');
                h += '</div>' + renderEventStrip(eventRows) + '</section>';

                var candData = candidateRows.slice(0, 30);
                h += '<section class="sim-panel" data-sim-section="candidate-queue">';
                h += sectionHead('今日决策队列' + (candidates.date ? ' (' + candidates.date + ')' : ''), '候选 ' + candidateRows.length + ' 只 · 可买 ' + buyReadyRows.length + ' · 等待 ' + waitRows.length + ' · 卖点 ' + sellAlertRows.length);
                h += '<div class="sim-empty" style="text-align:left;margin-bottom:10px;">主力行为“建仓”只作为证据，不会直接触发买入；交易时段内每分钟结合 AI 信号、买点分、盈亏比、板块门禁、实时价、买入区间和组合风控判断是否买入。</div>';
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
            h += sectionHead('当前持仓', '股票 ' + holdingGroups.length + ' · 买入批次 ' + allHoldings.length + ' · ' + holdingLimitLabel + ' · 需复核 ' + riskHoldingGroups.length);
            if (!holdingGroups.length) {
                h += emptyState('暂无持仓');
            } else {
                h += '<div class="sim-mini-summary">';
                h += metricCard('持仓股票', holdingGroups.length, '按股票合并展示', 'neutral');
                h += metricCard('买入批次', allHoldings.length, '展开下方可看每笔', 'accent');
                h += metricCard('风险复核', riskHoldingGroups.length, riskHoldingGroups.length ? '优先查看' : '状态正常', riskHoldingGroups.length ? 'bad' : 'good');
                h += '</div><div class="sim-table-scroll"><table class="sim-table"><thead><tr>';
                h += '<th>策略</th><th>代码</th><th>名称</th><th>批次</th><th>总股数</th><th>均价</th><th>现价</th><th>市值</th><th>盈亏</th><th>收益率</th><th>最长持有</th><th>评分</th><th>风险</th><th>持有/卖出理由</th>';
                h += '</tr></thead><tbody>';
                holdingGroups.forEach(function(g) {
                    h += '<tr><td><span style="color:' + strategyColors[g.strategy_type] + ';font-weight:800;">' + safeText(strategyNames[g.strategy_type] || '-') + '</span></td>';
                    h += '<td>' + safeText(g.stock_code || '-') + '</td><td>' + stockLink(g.stock_code, g.short_name) + '</td>';
                    h += '<td><span class="sim-tag sim-tag-neutral">' + g.batch_count + '笔</span></td><td>' + num(g.shares) + '</td>';
                    h += '<td>' + fmtCellPrice(g.buy_price) + '</td><td>' + fmtCellPrice(g.cur_price) + '</td><td>' + fmtMoney(g.market_value) + '</td>';
                    h += '<td>' + fmtPnl(g.pnl) + '</td><td>' + fmtRate(g.pnl_rate) + '</td><td>' + num(g.holding_days) + '天</td>';
                    h += '<td>' + analysisScoreText(g.ai_score) + '</td><td><span class="sim-tag sim-tag-' + g.risk.tone + '">' + g.risk.text + '</span></td>';
                    h += '<td class="sim-text-cell" title="' + safeAttr(g.exit_reason_detail) + '">' + safeText(g.exit_reason_detail) + '</td>';
                    h += '</tr>';
                });
                h += '</tbody></table></div>';
                h += '<details class="sim-details"><summary>展开买入批次明细（' + allHoldings.length + ' 笔）</summary>';
                h += '<div class="sim-table-scroll"><table class="sim-table"><thead><tr>';
                h += '<th>策略</th><th>代码</th><th>名称</th><th>买入时间</th><th>股数</th><th>买入价</th><th>现价</th><th>盈亏</th><th>收益率</th><th>风险</th><th>持有/卖出理由</th><th>操作</th>';
                h += '</tr></thead><tbody>';
                allHoldings.forEach(function(ph) {
                    var risk = holdingRiskTag(ph);
                    h += '<tr><td><span style="color:' + strategyColors[ph.strategy_type] + ';font-weight:800;">' + safeText(strategyNames[ph.strategy_type] || '-') + '</span></td>';
                    h += '<td>' + safeText(ph.stock_code || '-') + '</td><td>' + stockLink(ph.stock_code, ph.short_name) + '</td>';
                    h += '<td class="sim-date-cell">' + safeText(ph.buy_date || '') + '</td><td>' + holdingShares(ph) + '</td>';
                    h += '<td>' + fmtCellPrice(ph.buy_price) + '</td><td>' + fmtCellPrice(ph.cur_price) + '</td><td>' + fmtPnl(ph.pnl) + '</td><td>' + fmtRate(ph.pnl_rate) + '</td>';
                    h += '<td><span class="sim-tag sim-tag-' + risk.tone + '">' + risk.text + '</span></td>';
                    h += '<td class="sim-text-cell" title="' + safeAttr(ph.exit_reason_detail || '-') + '">' + safeText(ph.exit_reason_detail || '-') + '</td>';
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
                h += '<div class="sim-strategy-head"><strong>' + safeText(s.name || strategyNames[st]) + '策略</strong><span>股票 ' + strategyGroups.length + ' · 批次 ' + num(s.holding_count) + ' · ' + holdingLimitLabel + '</span></div>';
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
        h += '<div class="card-title"><span class="card-tag">核心指标</span> 恐慌贪婪指数</div>';
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
        h += '<div class="card-title"><span class="card-tag">市场判断</span> 今日市场综合判断</div>';
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
        h += '<div class="card-title"><span class="card-tag">趋势</span> 全A热度 + 成交额</div>';
        h += '<div class="chart-container"><canvas id="heatChart"></canvas></div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">行业</span> 行业热度 Top8</div>';
        h += '<div class="chart-container"><canvas id="industryChart"></canvas></div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">TMT</span> TMT合计监控</div>';
        h += '<div class="chart-container"><canvas id="tmtChart"></canvas></div>';
        h += '<div class="monitor-stats">';
        h += '<div class="stat-item"><span class="stat-value" id="tmtRatio">' + (data.tmt_ratio || 0).toFixed(2) + '%</span><span class="stat-label">TMT合计占比</span></div>';
        h += '</div>';
        h += '</div>';
        h += '</div>';

        h += '<div class="monitor-grid monitor-grid-row3" style="margin-bottom:16px">';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">小盘</span> CSI1000小盘热度</div>';
        h += '<div class="chart-container"><canvas id="csi1000Chart"></canvas></div>';
        h += '<div class="monitor-stats">';
        h += '<div class="stat-item"><span class="stat-value" id="csi1000Heat">' + (data.csi1000.heat || '-') + '</span><span class="stat-label">小盘热度</span></div>';
        h += '<div class="stat-item"><span class="stat-value" id="csi1000Chg">' + (data.csi1000.change >= 0 ? '+' : '') + (data.csi1000.change || 0).toFixed(2) + '%</span><span class="stat-label">平均涨跌</span></div>';
        h += '</div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">概念</span> 热门概念 Top8</div>';
        h += '<div class="chart-container"><canvas id="conceptChart"></canvas></div>';
        h += '</div>';
        h += '<div class="monitor-card chart-card">';
        h += '<div class="card-title"><span class="card-tag">资金</span> 观望资金趋势</div>';
        h += '<div class="chart-container"><canvas id="sidelineChart"></canvas></div>';
        h += '<div class="monitor-stats">';
        h += '<div class="stat-item"><span class="stat-value" id="sidelineRatio">' + (data.sideline_ratio || 0).toFixed(2) + '%</span><span class="stat-label">当前占比</span></div>';
        h += '</div>';
        h += '</div>';
        h += '</div>';

        h += '<div class="monitor-grid monitor-grid-row4">';
        h += '<div class="monitor-card table-card">';
        h += '<div class="card-title"><span class="card-tag">明细</span> 热门概念详情</div>';
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
        h += '<div class="card-title"><span class="card-tag">摘要</span> 市场数据摘要</div>';
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

    function routeDecisionDateFromLocation() {
        try {
            var value = new URLSearchParams(window.location.search || '').get('trade_date') || '';
            return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : '';
        } catch (e) { return ''; }
    }

    window.addEventListener('popstate', function() {
        var routeDate = routeDecisionDateFromLocation();
        if (routeDate && el('datePicker')) el('datePicker').value = routeDate;
        var tab = routeTabFromLocation();
        if (tab) _restoreTab(tab);
    });

    function linkedStockCodeFromLocation() {
        try {
            var code = new URLSearchParams(window.location.search || '').get('stock_code') || '';
            code = String(code).trim().split('.', 1)[0];
            return /^\d{1,6}$/.test(code) ? code.padStart(6, '0') : '';
        } catch (e) {
            return '';
        }
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
            var routeDate = routeDecisionDateFromLocation();
            if (routeDate && el('datePicker')) el('datePicker').value = routeDate;
            if (routeTab) {
                _restoreTab(routeTab);
            } else if (!_restoreTab(savedTab)) {
                // 没有保存的页面或页面不存在，加载默认页面
                loadTab('trading');
            }
            var linkedStockCode = linkedStockCodeFromLocation();
            if (linkedStockCode && typeof window.openStockDetail === 'function') {
                setTimeout(function () {
                    window.openStockDetail(linkedStockCode);
                }, 120);
            }
        });
    }
    try { init(); } catch (e) {
        console.error('[init error]', e);
        var mc = el('mainContent');
        if (mc) mc.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 初始化失败: ' + e.message + '</div>';
    }
})();
