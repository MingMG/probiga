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
    function isTradingTime() {
        var now = new Date();
        var day = now.getDay();
        if (day === 0 || day === 6) return false;
        var hm = now.getHours() * 60 + now.getMinutes();
        return (hm >= 570 && hm <= 690) || (hm >= 780 && hm <= 900);
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

    function apiGet(path) { return fetch(API_BASE + path).then(function (r) { return r.json(); }); }
    function escAttr(v) {
        return String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
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
            if (tabId !== 'monitor' && typeof window.stopMonitorRefresh === 'function') {
                window.stopMonitorRefresh();
            }
            if (tabId !== 'sector' && sectorMoveTimer) {
                clearInterval(sectorMoveTimer);
                sectorMoveTimer = null;
            }
            setStatus('加载中...');
            document.querySelectorAll('.sidebar-item').forEach(function (b) { b.classList.remove('active'); });
            var btn = document.querySelector('[data-tab="' + tabId + '"]');
            if (btn) btn.classList.add('active');
            document.querySelectorAll('.tab-content').forEach(function (c) { c.classList.remove('active'); });
            var tc = el('tab-' + tabId);
            if (!tc) tc = el(tabId);
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
    window.loadPortfolio = function(){ loadTab('portfolio'); };

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
                     costEl.textContent = newCost.toFixed(4);
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
        fetch('/api/portfolio/refresh-prices', {method:'POST'}).then(function(r){return r.json()}).then(function(res){
            if (res.status === 'ok') { loadTab('portfolio'); }
            else alert('行情刷新失败');
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

                // 头部信息
                var html = '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid rgba(255,255,255,0.08)">';
                html += '<div>';
                var modeTag = res.data_mode === 'intraday' ? '🟢 盘中实时' : '📅 盘后收盘';
                if (res.quote_trade_date) modeTag += ' · ' + res.quote_trade_date;
                html += '<span style="color:#888;font-size:11px">' + modeTag + '</span>';
                html += '</div>';
                html += '<span style="color:#888;font-size:10px;background:rgba(255,255,255,0.05);padding:3px 8px;border-radius:4px">仅供参考</span>';
                html += '</div>';

                // 持仓卡片（如有）
                if (res.holding) {
                    var hd = res.holding;
                    var pColor = hd.profit_pct >= 0 ? '#e74c3c' : '#27ae60';
                    var pSign = hd.profit_pct >= 0 ? '+' : '';
                    html += '<div style="background:linear-gradient(135deg,rgba(26,115,232,0.12),rgba(26,115,232,0.04));border:1px solid rgba(26,115,232,0.2);border-radius:10px;padding:12px 16px;margin-bottom:14px;display:flex;align-items:center;gap:20px;flex-wrap:wrap">';
                    html += '<div><div style="font-size:10px;color:#888;margin-bottom:2px">现价</div><div style="font-size:20px;font-weight:800;color:#222">' + Number(hd.cur_price).toFixed(2) + '</div></div>';
                    html += '<div style="width:1px;height:32px;background:rgba(0,0,0,0.1)"></div>';
                    html += '<div><div style="font-size:10px;color:#888;margin-bottom:2px">持仓</div><div style="font-size:14px;font-weight:600;color:#333">' + hd.shares + '股</div></div>';
                    html += '<div><div style="font-size:10px;color:#888;margin-bottom:2px">成本</div><div style="font-size:14px;font-weight:600;color:#333">' + Number(hd.cost_price).toFixed(2) + '</div></div>';
                    html += '<div style="width:1px;height:32px;background:rgba(0,0,0,0.1)"></div>';
                    html += '<div style="width:1px;height:32px;background:rgba(255,255,255,0.1)"></div>';
                    html += '<div><div style="font-size:10px;color:#888;margin-bottom:2px">盈亏</div><div style="font-size:18px;font-weight:800;color:' + pColor + '">' + pSign + hd.profit_pct + '%</div></div>';
                    if (hd.profit_amount) {
                        html += '<div><div style="font-size:10px;color:#888;margin-bottom:2px">金额</div><div style="font-size:14px;font-weight:600;color:' + pColor + '">' + (hd.profit_amount >= 0 ? '+' : '') + Number(hd.profit_amount).toFixed(0) + '元</div></div>';
                    }
                    html += '</div>';
                }

                // 分析正文
                if (isErr) {
                    html += '<div style="color:#e74c3c;padding:16px;background:rgba(231,76,60,0.1);border-radius:8px;font-size:13px">' + analysis + '</div>';
                } else {
                    // 格式化分析文本
                    analysis = analysis
                        .replace(/\*\*([^*]+)\*\*/g, '<strong style="color:#111">$1</strong>')
                        .replace(/###\s*(.+)/g, '<div style="font-size:14px;font-weight:700;color:#1a73e8;margin:18px 0 8px;padding-left:10px;border-left:3px solid #1a73e8">$1</div>')
                        .replace(/(趋势判断|资金态度|热度评估|风险提示)：/g, '<div style="font-size:14px;font-weight:700;color:#1a73e8;margin:18px 0 8px;padding-left:10px;border-left:3px solid #1a73e8">$1</div>')
                        .replace(/操作建议：/g, '<div style="font-size:14px;font-weight:700;color:#ff9800;margin:18px 0 8px;padding-left:10px;border-left:3px solid #ff9800">操作建议</div>')
                        .replace(/持有|加仓|减仓|清仓|买入|卖出|观望/g, '<span style="color:#ff9800;font-weight:700">$&</span>')
                        .replace(/不建议买入|不宜追高|注意风险|禁止推荐|暂停推荐/g, '<span style="color:#e53935;font-weight:700">$&</span>')
                        .replace(/(\d+\.?\d*亿)/g, '<span style="color:#e74c3c;font-weight:600">$1</span>')
                        .replace(/(第\d+名)/g, '<span style="color:#f39c12;font-weight:600">$1</span>')
                        .replace(/\n{2,}/g, '</p><p style="margin:6px 0;line-height:1.8">')
                        .replace(/\n/g, '<br>');
                    html += '<div style="font-size:13px;color:#222;line-height:1.8">' + analysis + '</div>';
                }

                html += '<div style="margin-top:18px;padding-top:12px;border-top:1px solid rgba(255,255,255,0.08);text-align:right">';
                html += '<button onclick="closeAnalyzeModal()" style="padding:8px 24px;border:none;border-radius:8px;background:linear-gradient(135deg,#1a73e8,#1565c0);color:#fff;cursor:pointer;font-size:13px;font-weight:500;box-shadow:0 2px 8px rgba(26,115,232,0.3)">关闭</button>';
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
                    '<span style="font-size:12px">现价 <strong>'+Number(h.cur_price||0).toFixed(2)+'</strong> ' +
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
    document.addEventListener('keydown', function(e) { if (e.key === 'Escape') { closeAnalyzeModal(); closeHistoryModal(); closeTradeModal(); } });

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
        apiGet('/market-sentiment?days=20&date=' + d + '&top=8').then(function (res) {
            if (res.error) { c.innerHTML = '<div class="loading" style="color:#e74c3c">❌ ' + res.error + '</div>'; return; }
            var theme = res.theme_analysis || {};
            var style = res.style_analysis || {};
            var cap = res.capital_analysis || {};
            var h = '';
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
        var state = window._subViewState || (window._subViewState = {});
        state[containerId] = viewId;
        var handler = (state['_handler_' + containerId]);
        if (handler) handler(viewId);
    };

    /* ===== Tabs ===== */
    var LOADERS = {
        /* ── 市场概览 ── */
        monitor: function (d, c) {
            var views = [{id:'dashboard', label:'📊 仪表盘'}, {id:'sentiment', label:'🧠 情绪详情'}];
            c.innerHTML = subViewBar('monitor', views, 'dashboard') + '<div id="monitorBody"></div>';
            var body = el('monitorBody');
            var state = window._subViewState || (window._subViewState = {});
            state['_handler_monitor'] = function (vid) {
                if (vid === 'dashboard') { loadMonitorPage(body); }
                else { loadSentimentPage(d, body); }
            };
            state['monitor'] = 'dashboard';
            loadMonitorPage(body);
        },
        /* ── 热股排行（融合 + 四大来源） ── */
        fused: function (d, c) {
            var views = [{id:'fused', label:'🔥 融合榜'}, {id:'east', label:'✨ 东财'}, {id:'ths', label:'🏆 同花顺'}, {id:'xq', label:'❄️ 雪球'}, {id:'sina', label:'🌐 新浪'}];
            c.innerHTML = subViewBar('fused', views, 'fused') + '<div id="fusedBody"></div>';
            var body = el('fusedBody');
            var state = window._subViewState || (window._subViewState = {});
            state['_handler_fused'] = function (vid) {
                if (vid === 'fused') loadFusedTab(d, body);
                else if (vid === 'east') loadEastTab(d, body);
                else if (vid === 'ths') loadThsTab(d, body);
                else if (vid === 'xq') loadXqTab(d, body);
                else if (vid === 'sina') loadSinaTab(d, body);
            };
            state['fused'] = 'fused';
            loadFusedTab(d, body);
        },
        /* ── 板块分析（异动 + 轮动 + 热度） ── */
        sector: function (d, c) {
            var views = [{id:'movement', label:'🌊 异动监控'}, {id:'rotation', label:'🔄 轮动分析'}, {id:'heat', label:'🌡 热度矩阵'}];
            c.innerHTML = subViewBar('sector', views, 'movement') + '<div id="sectorBody"></div>';
            var body = el('sectorBody');
            var state = window._subViewState || (window._subViewState = {});
            state['_handler_sector'] = function (vid) {
                if (vid === 'movement') loadSectorMovementPage(body);
                else if (vid === 'rotation') loadSectorRotationPage(d, body);
                else if (vid === 'heat') {
                    apiGet('/sector-heat-matrix?end_date=' + d + '&days=26').then(function (res) {
                        if (!res.data || !res.data.length) {
                            body.innerHTML = '<div class="loading" style="padding:20px"><p>当前日期暂无板块热度数据</p><p style="font-size:12px;color:#888;margin-top:8px">点击下方按钮从东财同步最新数据</p><button onclick="syncSectorHeatBtn(\'' + d + '\')" style="margin-top:10px;padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:14px">🔄 同步东财板块热度</button></div>';
                            return;
                        }
                        renderSectorHeatMatrix(body, res);
                    }).catch(function () { body.innerHTML = '<div class="loading">加载失败</div>'; });
                }
            };
            state['sector'] = 'movement';
            loadSectorMovementPage(body);
        },
        /* ── 强势股（3天/5天切换） ── */
        strong: function (d, c) {
            var views = [{id:'3', label:'🔥 近3天'}, {id:'5', label:'🔥 近5天'}];
            c.innerHTML = subViewBar('strong', views, '3') + '<div id="strongBody"></div>';
            var body = el('strongBody');
            var state = window._subViewState || (window._subViewState = {});
            state['_handler_strong'] = function (vid) { loadMulti(d, parseInt(vid), body); };
            state['strong'] = '3';
            loadMulti(d, 3, body);
        },
        /* ── 概念/行业（类型 + 天数切换） ── */
        concept: function (d, c) {
            var views = [{id:'c0', label:'🏷️ 当日概念'}, {id:'c3', label:'📋 3天概念'}, {id:'c5', label:'📋 5天概念'}, {id:'i3', label:'🏭 3天行业'}, {id:'i5', label:'🏭 5天行业'}];
            c.innerHTML = subViewBar('concept', views, 'c0') + '<div id="conceptBody"></div>';
            var body = el('conceptBody');
            var state = window._subViewState || (window._subViewState = {});
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
            state['concept'] = 'c0';
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
            c.innerHTML = subViewBar('capital', views, 'daily') + '<div id="capitalBody"></div>';
            var body = el('capitalBody');
            var state = window._subViewState || (window._subViewState = {});
            state['_handler_capital'] = function (vid) {
                if (vid === 'daily') {
                    body.innerHTML = '<div id="capResult2"></div>';
                    loadCap2();
                    if (window._capAutoRefresh) clearInterval(window._capAutoRefresh);
                    window._capAutoRefreshDone = false;
                    window._capAutoRefresh = setInterval(function () {
                        var active = document.querySelector('.sidebar-item.active');
                        if (active && active.getAttribute('data-tab') === 'capital') {
                            if (isTradingTime()) { window._capAutoRefreshDone = false; loadCap2(true); }
                            else if (!window._capAutoRefreshDone) { window._capAutoRefreshDone = true; loadCap2(true); }
                        }
                    }, 5000);
                } else {
                    body.innerHTML = '<div class="search-bar"><input type="text" id="rtCode" placeholder="输入股票代码" style="width:130px"><button onclick="loadRT()">查询</button><span id="rtInfo"></span></div><div id="rtResult"></div>';
                }
            };
            state['capital'] = 'daily';
            body.innerHTML = '<div id="capResult2"></div>';
            loadCap2();
            if (window._capAutoRefresh) clearInterval(window._capAutoRefresh);
            window._capAutoRefreshDone = false;
            window._capAutoRefresh = setInterval(function () {
                var active = document.querySelector('.sidebar-item.active');
                if (active && active.getAttribute('data-tab') === 'capital') {
                    if (isTradingTime()) { window._capAutoRefreshDone = false; loadCap2(true); }
                    else if (!window._capAutoRefreshDone) { window._capAutoRefreshDone = true; loadCap2(true); }
                }
            }, 5000);
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
                {id:'trend_strong',label:'🔥 强势趋势票',desc:'四线多头+连续站MA5+创新高+温和量比'},
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
            // AI推荐默认加载最新数据，不使用日期选择器的值
            loadRecommendedPage('', c);
        },
        /* ── 持仓管理 ── */
        portfolio: function (d, c) {
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
        review: function (d, c) {
            apiGet('/daily-review?review_date=' + d).then(function (res) {
                syncDateFromResponse(res);
                if (!res.data || !res.data.length) {
                    c.innerHTML = '<div class="loading" style="padding:20px"><p>当前日期暂无复盘数据</p><button onclick="genReviewBtn(\'' + d + '\')" style="margin-top:10px;padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:14px">🔄 生成复盘数据</button></div>';
                    return;
                }
                var r = res.data[0];
                var hot = jsonF(r.hot_sectors, []);
                var cold = jsonF(r.cold_sectors, []);
                var volUp = jsonF(r.volume_up_sectors, []);
                var volDown = jsonF(r.volume_down_sectors, []);
                var idxA = jsonF(r.index_analysis, []);

                var amt = (Number(r.total_amount || 0) / 1e8).toFixed(0);
                var idxChg = Number(r.index_change_pct || 0);

                var html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
                html += '<span style="font-size:16px;font-weight:700;color:#e0e0e0">📋 复盘数据 | ' + r.review_date + '</span>';
                html += '<button onclick="exportReview(\'' + r.review_date + '\')" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:12px">📥 导出</button>';
                html += '</div>';

                html += '<div class="stats-bar">' + card('市场热度', (r.market_heat || '-') + '%', 'blue') + card('成交额', amt + '亿', 'orange') + card(r.index_name || '指数', (r.index_price || '-'), 'red') + card('涨跌幅', (idxChg >= 0 ? '+' : '') + idxChg.toFixed(2) + '%', idxChg >= 0 ? 'red' : 'green') + card('量能', r.total_amount_change || '-', 'blue') + card('观望', (r.sideline_ratio || '-') + '%', 'comment') + '</div>';

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
                c.innerHTML = html;
            });
        },
        portfolio: function (d, c) {
            function pfFmtProfit(v) {
                var n = Number(v || 0);
                var cls = n >= 0 ? 'c-red' : 'c-green';
                return '<strong class="' + cls + '">' + (n >= 0 ? '+' : '') + n.toFixed(2) + '</strong>';
            }
            function pfUpdateSummary(res) {
                var s = res.summary || {};
                var elTotal = document.getElementById('pfTotalProfit');
                var elToday = document.getElementById('pfTodayProfit');
                var elCnt = document.getElementById('pfHoldingCount');
                var elOpen = document.getElementById('pfTodayOpenCount');
                var elCleared = document.getElementById('pfTodayClearedCount');
                if (elTotal) elTotal.innerHTML = pfFmtProfit(s.total_hold_profit);
                if (elToday) elToday.innerHTML = pfFmtProfit(s.today_hold_profit);
                if (elCnt) elCnt.textContent = String(s.holding_count != null ? s.holding_count : 0);
                if (elOpen) elOpen.textContent = String(s.today_open_count != null ? s.today_open_count : 0);
                if (elCleared) elCleared.textContent = String(s.today_cleared_count != null ? s.today_cleared_count : 0);
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
                var badgeEl = row.querySelector('.pf-row-badge');
                if (curEl) curEl.textContent = pr.toFixed(2);
                if (chgEl) { chgEl.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%'; chgEl.className = chgCls + ' pf-chg-pct'; }
                if (costEl) costEl.textContent = cp.toFixed(4);
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
            }
            window.pfUpdatePortfolioRow = pfUpdateRow;
            window.pfUpdatePortfolioSummary = pfUpdateSummary;
            function renderPortfolio(res) {
                var addForm = '<div style="padding:14px 16px;background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);border-radius:10px;margin-bottom:14px;border:1px solid #2a2a4a">' +
                    '<h4 style="margin:0 0 10px;color:#e0e0e0;font-size:13px">➕ 添加自选股</h4>' +
                    '<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end">' +
                    '<div><label style="font-size:11px;color:#888;display:block">股票代码</label><input id="pfCode" placeholder="000001" style="width:100px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<div><label style="font-size:11px;color:#888;display:block">成本价(元)</label><input id="pfPrice" type="number" step="0.001" placeholder="0" style="width:100px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<div><label style="font-size:11px;color:#888;display:block">股数</label><input id="pfShares" type="number" placeholder="0" style="width:80px;padding:6px 10px;border-radius:4px;border:1px solid #444;background:#2a2a2e;color:#e0e0e0"></div>' +
                    '<label title="勾选后会写入今日买入流水，当日盈亏按 现价-买入价 计算；不勾选则按历史持仓录入" style="display:flex;gap:4px;align-items:center;color:#aaa;font-size:12px;margin-bottom:7px"><input id="pfTodayBuy" type="checkbox">今日买入</label>' +
                    '<button onclick="pfAdd()" style="padding:6px 16px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:13px">添加</button>' +
                    '</div></div>';

                var sum = res.summary || {};
                var toolbar = '<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:10px">' +
                    '<span id="pfSummaryBar" style="display:flex;gap:14px;align-items:center;flex-wrap:wrap;font-size:13px;color:#ccc">' +
                    '<span>持仓 <span id="pfHoldingCount" style="color:#ff9800;font-weight:700">'+(sum.holding_count||0)+'</span> 只</span>' +
                    '<span>今开 <span id="pfTodayOpenCount" style="color:#1a73e8;font-weight:700">'+(sum.today_open_count||0)+'</span> 只</span>' +
                    '<span>今清 <span id="pfTodayClearedCount" style="color:#64748b;font-weight:700">'+(sum.today_cleared_count||0)+'</span> 只</span>' +
                    '<span title="(现价-成本)×股数">持仓盈亏 <span id="pfTotalProfit">'+pfFmtProfit(sum.total_hold_profit)+'</span></span>' +
                    '<span title="昨日持仓×涨跌额 + 今日买入/卖出盈亏">当日盈亏 <span id="pfTodayProfit">'+pfFmtProfit(sum.today_hold_profit)+'</span></span>' +
                    '</span>' +
                    '<span style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">' +
                    '<span style="font-size:14px;font-weight:700;color:#e0e0e0">📈 我的自选股 ('+res.total+'只)</span>' +
                    '<button onclick="savePfOrder()" style="padding:4px 10px;border:none;border-radius:4px;background:#f0c040;color:#1a1a1a;cursor:pointer;font-size:11px;font-weight:600">💾 保存顺序</button>' +
                    '<button onclick="refreshPfPrices()" style="padding:4px 10px;border:none;border-radius:4px;background:#388e3c;color:#fff;cursor:pointer;font-size:11px">📡 实时</button>' +
                    '<button onclick="loadPortfolio()" style="padding:4px 12px;border:none;border-radius:4px;background:#333;color:#aaa;cursor:pointer;font-size:12px">🔄 刷新</button>' +
                    '</span></div>';

                if (!res.data || !res.data.length) {
                    c.innerHTML = addForm + toolbar + '<div class="loading">暂无自选股</div>';
                    return;
                }

                var html = toolbar;

                // Build table with drag handles
                html += '<div class="table-wrap"><table id="pfTable"><thead><tr>' +
                    '<th style="width:28px"></th>' +
                    '<th>代码</th><th>名称</th><th>现价</th><th title="个股行情涨跌，非您的持仓盈亏">涨跌%</th><th>成本</th><th>持有</th>' +
                    '<th title="昨日持仓×涨跌额 + 今日买入/卖出盈亏">当日盈亏</th><th title="(现价-成本)×股数">持仓盈亏</th><th title="相对成本">收益率</th><th>操作</th><th>分析</th><th>历史</th>' +
                    '</tr></thead><tbody>';

                res.data.forEach(function(r, idx){
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
                    html += '<tr id=\"pf-tr-'+r.stock_code+'\" draggable=\"true\" data-code=\"'+r.stock_code+'\" data-holding=\"'+(isHolding?'1':'0')+'\" data-today-status=\"'+(r.today_position_status||'')+'\" style=\"cursor:grab;'+rowBg+'\">' +
                        '<td style=\"text-align:center;color:#555;font-size:14px;cursor:grab\" class=\"pf-drag-handle\">⠿</td>' +
                        '<td>'+nameLink(r.stock_code, r.stock_code)+'</td>' +
                        '<td><strong>'+nameLink(r.stock_code, r.display_name)+'</strong><span class=\"pf-row-badge\">'+pfBadge(r)+'</span></td>' +
                        '<td class=\"pf-cur-price\">'+pr.toFixed(2)+'</td>' +
                        '<td class=\"'+chgCls+' pf-chg-pct\">'+(chg>=0?'+':'')+chg.toFixed(2)+'%</td>' +
                        '<td class=\"pf-cost\">'+cp.toFixed(4)+'</td>' +
                        '<td class=\"pf-shares\">'+(r.shares||0)+'</td>' +
                        '<td class=\"'+tdCell.cls+'\">'+tdCell.text+'</td>' +
                        '<td class=\"pf-profit '+pfClsRow+'\">'+profitTxt+'</td>' +
                        '<td class=\"pf-profit-pct '+pfClsRow+'\">'+pctTxt+'</td>' +
                        '<td class=\"pf-actions\"><button onclick=\"event.stopPropagation();pfTransact(\''+r.stock_code+'\',\''+r.display_name+'\','+cp+','+(r.shares||0)+')\" style=\"padding:2px 8px;border:none;border-radius:4px;background:#388e3c;color:#fff;cursor:pointer;font-size:11px\">💰</button>' +
                        '<button onclick=\"event.stopPropagation();pfRemove(\''+r.stock_code+'\')\" style=\"padding:2px 8px;border:none;border-radius:4px;background:#c62828;color:#fff;cursor:pointer;font-size:11px;margin-left:2px\">✕</button></td>' +
                        '<td><button onclick=\"event.stopPropagation();pfAnalyze(\''+r.stock_code+'\',\''+r.display_name+'\')\" style=\"padding:4px 12px;border:none;border-radius:4px;background:#1a73e8;color:#fff;cursor:pointer;font-size:11px\">🤖 分析</button></td>' +
                        '<td><button onclick=\"event.stopPropagation();pfHistory(\''+r.stock_code+'\',\''+r.display_name+'\')\" style=\"padding:4px 10px;border:none;border-radius:4px;background:#555;color:#ccc;cursor:pointer;font-size:11px\">📋</button></td>' +
                        '</tr>';
                });
                html += '</tbody></table></div>';
                c.innerHTML = addForm + html;

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
            }
            fetch('/api/portfolio/list').then(function(r){return r.json()}).then(renderPortfolio);

            // Auto-refresh every 5s during trading hours
            if (window._pfAutoRefresh) clearInterval(window._pfAutoRefresh);
            window._pfAutoRefreshDone = false;
            window._pfAutoRefresh = setInterval(function() {
                var active = document.querySelector('.sidebar-item.active');
                if (active && active.getAttribute('onclick') && active.getAttribute('onclick').indexOf('portfolio') > -1) {
                    if (isTradingTime()) {
                        window._pfAutoRefreshDone = false;
                        fetch('/api/portfolio/live').then(function(r){return r.json()}).then(function(res){
                             if (!res.data) return;
                             if (res.summary) pfUpdateSummary(res);
                             res.data.forEach(pfUpdateRow);
                        });
                    } else if (!window._pfAutoRefreshDone) {
                        window._pfAutoRefreshDone = true;
                        fetch('/api/portfolio/live').then(function(r){return r.json()}).then(function(res){
                             if (!res.data) return;
                             if (res.summary) pfUpdateSummary(res);
                             res.data.forEach(pfUpdateRow);
                        });
                    }
                }
            }, 5000);
        },
        scheduler: function (d, c) {
            fetch('/api/scheduler/tasks').then(function (r) { return r.json(); }).then(function (res) {
                if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无任务</div>'; return; }
                var GROUP_ORDER = ['复盘数据', '概念行业', '资金流向', '龙虎榜', '系统管理', '其他'];
                var GROUP_ICONS = {'复盘数据':'📊','概念行业':'🏷️','资金流向':'💰','龙虎榜':'🐲','系统管理':'⚙️','其他':'📌'};
                var GROUP_IDS = {'复盘数据':'review','概念行业':'concept','资金流向':'capital','龙虎榜':'lhb','系统管理':'sys','其他':'other'};
                var groups = {};
                res.data.forEach(function (t) {
                    var g = t.group_name || '其他';
                    if (!groups[g]) groups[g] = [];
                    groups[g].push(t);
                });
                c.innerHTML = '';
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
            apiGet('/sector-heat-matrix?end_date=' + d + '&days=26').then(function (res) {
                if (!res.data || !res.data.length) {
                    c.innerHTML = '<div class="loading" style="padding:20px"><p>当前日期暂无板块热度数据</p><button onclick="syncSectorHeatBtn(\'' + d + '\')" style="margin-top:10px;padding:8px 20px;border:none;border-radius:6px;background:#1a73e8;color:#fff;cursor:pointer;font-size:14px">🔄 同步东财板块热度</button></div>';
                    return;
                }
                renderSectorHeatMatrix(c, res);
            }).catch(function () { c.innerHTML = '<div class="loading">加载失败</div>'; });
        },
        'sector-movement': function (d, c) { loadSectorMovementPage(c); },
        'sector-rotation': function (d, c) { loadSectorRotationPage(d, c); },
        review: function (d, c) {
            fetch('/api/hot-data/daily-review/print?review_date=' + d).then(function (r) { return r.text(); }).then(function (html) {
                if (!html || html.length < 50) { c.innerHTML = '<div class="loading">暂无复盘数据，点击右上角生成</div>'; return; }
                c.innerHTML = '<div style="background:#fff;border-radius:10px;padding:20px;box-shadow:0 1px 6px rgba(0,0,0,.05)">' + html + '</div>';
            }).catch(function () { c.innerHTML = '<div class="loading">加载失败</div>'; });
        }
    };

    /* ===== 布局切换 ===== */
    var LAYOUT_OLD = [
        {group:'市场分析', items:[
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
            {id:'portfolio',icon:'📈',label:'自选股'},
            {id:'recommended',icon:'💎',label:'AI推荐买入'}
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
            {id:'notice',icon:'📜',label:'个股公告'}
        ]},
        {group:'龙虎榜', items:[
            {id:'alist',icon:'🐲',label:'龙虎榜列表'}
        ]},
        {group:'系统管理', items:[
            {id:'scheduler',icon:'⚙️',label:'调度管理'}
        ]}
    ];
    var LAYOUT_NEW = [
        {group:'市场概览', items:[
            {id:'monitor',icon:'📺',label:'市场监控'},
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
            {id:'recommended',icon:'💎',label:'AI推荐'}
        ]},
        {group:'持仓管理', items:[
            {id:'portfolio',icon:'📈',label:'自选股'}
        ]},
        {group:'资讯公告', items:[
            {id:'news',icon:'📰',label:'快讯'},
            {id:'notice',icon:'📜',label:'个股公告'}
        ]},
        {group:'系统', items:[
            {id:'scheduler',icon:'⚙️',label:'调度管理'},
            {id:'stock-list',icon:'📋',label:'全市场股票'}
        ]}
    ];
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
        var hasConclusion = ai.conclusion && ai.conclusion.length > 10;

        if (hasConclusion) {
            h += '<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;margin-bottom:16px;color:#fff">';
            // 日期和价格
            h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
            h += '<span style="font-size:12px;color:#aaa">📅 ' + (ai.analysis_date || d.date || '') + '</span>';
            h += '<span style="font-size:14px;color:#ddd">现价 ' + (m.price || '-') + ' ' + pct(m.change_pct) + '</span>';
            h += '</div>';
            // 分析内容
            h += '<div style="font-size:14px;color:#ddd;line-height:1.8;white-space:pre-wrap">' + ai.conclusion + '</div>';
            // 操作建议
            if (ai.action) {
                var actionColor = ai.action === '加仓' ? '#27ae60' : ai.action === '减仓' ? '#e74c3c' : '#f39c12';
                h += '<div style="margin-top:12px;display:flex;align-items:center;gap:10px">';
                h += '<span style="background:' + actionColor + ';color:#fff;padding:4px 12px;border-radius:6px;font-size:14px;font-weight:700">操作建议：' + ai.action + '</span>';
                if (ai.action_reason) h += '<span style="font-size:12px;color:#ccc">' + ai.action_reason + '</span>';
                h += '</div>';
            }
            h += '</div>';
        } else if (ai.score != null || ai.long_term_score != null) {
            // 评分格式（无详细分析时显示）
            h += '<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;padding:16px;margin-bottom:16px;color:#fff">';
            var sColor = ai.score >= 70 ? '#e74c3c' : ai.score >= 50 ? '#f39c12' : '#27ae60';
            h += '<div style="display:flex;align-items:center;gap:16px;margin-bottom:12px">';
            h += '<div style="text-align:center"><div style="font-size:36px;font-weight:800;color:' + sColor + '">' + (ai.score || '-') + '</div><div style="font-size:11px;color:#aaa">综合评分</div></div>';
            h += '</div>';
            if (ai.conclusion) h += '<div style="font-size:13px;color:#ddd;line-height:1.6">' + ai.conclusion + '</div>';
            h += '</div>';
        }

        // ── 一、行情数据 ──
        h += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px">';
        var qFields = [
            { label: '现价', value: m.price != null ? Number(m.price).toFixed(2) : '-' },
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
            h += '<div style="background:#f8f9fa;padding:8px 6px;border-radius:6px;text-align:center">';
            h += '<div style="font-size:10px;color:#888;margin-bottom:2px">' + item.label + '</div>';
            h += '<div style="font-size:13px;font-weight:600;color:#333' + (item.cls ? '' : '') + '">' + item.value + '</div>';
            h += '</div>';
        });
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

        // 异步加载主力行为分析
        fetch(API_BASE + '/mainforce-analysis?stock_code=' + encodeURIComponent(d.stock_code)).then(function (r) { return r.json(); }).then(function (mf) {
            var mfContent = document.getElementById('mfDetailContent');
            if (!mfContent || mf.error) {
                if (mfContent) mfContent.innerHTML = '<span style="color:#999">暂无数据</span>';
                return;
            }
            var behavior = mf.behavior || '中性';
            var score = mf.score || 50;
            var confidence = mf.confidence || 0;
            var signals = mf.signals || {};
            var mh = '';
            mh += '<div style="display:flex;align-items:center;gap:16px;margin-bottom:10px">';
            mh += '<span class="mf-big-badge mf-big-' + behavior + '" style="font-size:16px;padding:4px 16px">' + behavior + '</span>';
            mh += '<div style="text-align:center"><div style="font-size:24px;font-weight:800;color:' + (score >= 60 ? '#e74c3c' : score <= 40 ? '#27ae60' : '#f5a623') + '">' + score + '</div><div style="font-size:10px;color:#888">综合得分</div></div>';
            mh += '<div style="flex:1"><div style="font-size:11px;color:#888;margin-bottom:3px">置信度 ' + confidence + '%</div>';
            mh += '<div class="mf-confidence-bar"><div class="mf-confidence-fill" style="width:' + confidence + '%;background:' + (confidence > 70 ? '#e74c3c' : confidence > 50 ? '#f5a623' : '#27ae60') + '"></div></div></div>';
            mh += '<button onclick="openMainforceModal(\'' + d.stock_code + '\',\'' + (d.short_name || '') + '\')" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:12px;cursor:pointer;white-space:nowrap">📊 详细分析</button>';
            mh += '</div>';
            // 信号条
            mh += '<div style="display:flex;gap:6px;flex-wrap:wrap">';
            var sigLabels = {volume_price:'量价',capital_flow:'资金',kline_pattern:'K线',chip_concentration:'筹码',institutional:'机构'};
            for (var k in sigLabels) {
                var s = signals[k] || {};
                mh += '<span class="mf-badge mf-' + (s.direction || '中性') + '" style="font-size:11px">' + sigLabels[k] + ' ' + (s.score || '-') + '</span>';
            }
            mh += '</div>';
            mfContent.innerHTML = mh;
        }).catch(function () {
            var mfContent = document.getElementById('mfDetailContent');
            if (mfContent) mfContent.innerHTML = '<span style="color:#999">加载失败</span>';
        });
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
        if (mode === 'trend_strong') params += '&t_days=10&slope=0.5&vr_min=0.8&vr_max=2.5&max_gain=150&nh_pct=0.95';
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
            if (document.getElementById('klineModal').classList.contains('show')) closeKlineModal();
            else if (document.getElementById('alistModal').classList.contains('show')) closeAlistModal();
            else if (document.getElementById('conceptModal').classList.contains('show')) closeConceptModal();
            else if (document.getElementById('aiModal').classList.contains('show')) closeAIModal();
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
            var toolbar = '<div class="stats-bar"><div class="stat-card"><div class="label">席位数量</div><div class="value blue">' + rows.length + '</div></div></div>';
            toolbar += '<div class="search-bar paged-table-toolbar"><input type="text" id="als_search" placeholder="🔍 搜索..." oninput="alsFilter()"><span id="als_info"></span></div>';
            renderPagedTable(body, 'als', toolbar, tableHeadHtml(['营业部', '类型', '净买入额', '买入额', '卖出额', '原因']), 'als_tbody', 'als_pager');
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
        if (!silent) c.innerHTML = '<div class="loading">加载中...</div>';
        apiGet('/capital-flow?trade_date=' + d + '&sort=' + s + '&top=' + t + '&stock_code=' + encodeURIComponent(cd)).then(function (res) {
            syncDateFromResponse(res);
            if (!res.data || !res.data.length) { c.innerHTML = '<div class="loading">暂无数据</div>'; return; }
            var src = res.mode_label || '';
            if (res.live_error) src += ' · 实时源回落';
            var dataTime = res.data_time || res.snapshot_at || res.date || '-';
            var info = '数据时间：' + dataTime + (src ? ' · 数据模式：' + src : '') + ' · 共 ' + res.total + ' 条';
            var toolbar = '<div class="search-bar paged-table-toolbar">';
            toolbar += '<select id="capSort2"><option value="desc"' + (s === 'desc' ? ' selected' : '') + '>净流入↓</option><option value="asc"' + (s === 'asc' ? ' selected' : '') + '>净流入↑</option></select>';
            toolbar += '<select id="capTop2"><option value="100"' + (t === '100' ? ' selected' : '') + '>前100</option><option value="500"' + (t === '500' ? ' selected' : '') + '>前500</option><option value="0"' + (t === '0' ? ' selected' : '') + '>全部</option></select>';
            toolbar += '<input type="text" id="capCode2" value="' + escAttr(cd) + '" placeholder="股票代码" style="width:110px"><button onclick="loadCap2()">查询</button><button onclick="loadCap2()">刷新</button><span id="capInfo2" style="font-size:12px;color:#888">' + info + '</span></div>';
            window.renderTable(c, 'cap', ['排名', '代码', '名称', '现价', '涨跌幅', '主力净流入', '超大单', '大单', '中单', '小单'], res.data, function (r, i) {
                var rk = t > 0 ? rankBadge(i + 1) : '-';
                var hasDetail = r.max_net_inflow != 0 || r.lg_net_inflow != 0;
                var maxFmt = hasDetail ? fmtMoney(r.max_net_inflow) : '<span style="color:#ccc">-</span>';
                var lgFmt = hasDetail ? fmtMoney(r.lg_net_inflow) : '<span style="color:#ccc">-</span>';
                var midFmt = hasDetail ? fmtMoney(r.mid_net_inflow) : '<span style="color:#ccc">-</span>';
                var smFmt = hasDetail ? fmtMoney(r.sm_net_inflow) : '<span style="color:#ccc">-</span>';
                return '<tr><td>' + rk + '</td><td>' + r.stock_code + '</td><td>' + nameLink(r.stock_code, r.short_name) + '</td><td>' + fmt(r.price, 2) + '</td><td class="' + clsPct(r.change_pct) + '">' + pct(r.change_pct) + '</td><td class="' + clsPct(r.main_net_inflow) + '"><strong>' + fmtMoney(r.main_net_inflow) + '</strong></td><td class="' + clsPct(r.max_net_inflow) + '">' + maxFmt + '</td><td class="' + clsPct(r.lg_net_inflow) + '">' + lgFmt + '</td><td class="' + clsPct(r.mid_net_inflow) + '">' + midFmt + '</td><td class="' + clsPct(r.sm_net_inflow) + '">' + smFmt + '</td></tr>';
            }, 50, toolbar);
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
            var h = '<div class="stats-bar">' + card('时间', l.trade_time.slice(11, 16), 'blue') + card('主力净流入', fmtMoney(l.main_net_inflow), clsPct(l.main_net_inflow)) + card('超大单', fmtMoney(l.max_net_inflow), clsPct(l.max_net_inflow)) + card('大单', fmtMoney(l.lg_net_inflow)) + card('中单', fmtMoney(l.mid_net_inflow)) + card('小单', fmtMoney(l.sm_net_inflow)) + '</div>';
            h += '<div class="section-title">分时资金流向（' + res.total + ' 条）</div><div class="table-wrap" style="max-height:500px;overflow-y:auto"><table><thead><tr><th>时间</th><th>主力净流入</th><th>超大单</th><th>大单</th><th>中单</th><th>小单</th></tr></thead><tbody>';
            res.data.forEach(function (rr) { h += '<tr><td>' + rr.trade_time.slice(11, 16) + '</td><td class="' + clsPct(rr.main_net_inflow) + '"><strong>' + fmtMoney(rr.main_net_inflow) + '</strong></td><td class="' + clsPct(rr.max_net_inflow) + '">' + fmtMoney(rr.max_net_inflow) + '</td><td class="' + clsPct(rr.lg_net_inflow) + '">' + fmtMoney(rr.lg_net_inflow) + '</td><td class="' + clsPct(rr.mid_net_inflow) + '">' + fmtMoney(rr.mid_net_inflow) + '</td><td class="' + clsPct(rr.sm_net_inflow) + '">' + fmtMoney(rr.sm_net_inflow) + '</td></tr>'; });
            h += '</tbody></table></div>'; r.innerHTML = h; el('rtInfo').textContent = '共 ' + res.total + ' 条';
        });
    };

    /* ===== 调度管理操作 ===== */
    window.updCron = function (id, v) { fetch('/api/scheduler/tasks/' + id + '/cron?cron_time=' + v).then(function () { loadTab('scheduler'); }); };
    window.updDp = function (id, v) { fetch('/api/scheduler/tasks/' + id + '/date-param?date_param=' + encodeURIComponent(v)).then(function () { loadTab('scheduler'); }); };
    window.togT = function (id) { fetch('/api/scheduler/tasks/' + id + '/toggle', { method: 'POST' }).then(function () { loadTab('scheduler'); }); };
    window.runT = function (id) {
        if (!confirm('确认立即执行？')) return;
        fetch('/api/scheduler/tasks/' + id + '/run', { method: 'POST' }).then(function (r) { return r.json(); }).then(function (res) {
            alert('执行完成！状态: ' + res.status + '，耗时: ' + (res.duration || 0) + '秒');
            loadTab('scheduler');
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
        fetch(API_BASE + '/mainforce-analysis?stock_code=' + encodeURIComponent(code)).then(function (r) { return r.json(); }).then(function (data) {
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
        var titleEl = document.getElementById('mainforceModalTitle');
        if (titleEl) titleEl.textContent = '🔍 主力行为分析 | ' + (name || code);
        var bodyEl = document.getElementById('mainforceModalBody');
        if (bodyEl) bodyEl.innerHTML = '<div style="text-align:center;padding:30px;color:#888"><span class="spinner"></span> 分析中...</div>';
        var overlay = document.getElementById('mainforceModal');
        if (overlay) overlay.classList.add('show');
        fetch(API_BASE + '/mainforce-analysis?stock_code=' + encodeURIComponent(code)).then(function (r) { return r.json(); }).then(function (data) {
            var b = document.getElementById('mainforceModalBody');
            if (!b) return;
            if (data.error) { b.innerHTML = '<div style="color:#e74c3c;padding:20px">⚠️ ' + data.error + '</div>'; return; }
            b.innerHTML = renderMainforceDetail(data);
            setTimeout(function () { initMainforceCharts(data); }, 100);
        }).catch(function (e) {
            var b = document.getElementById('mainforceModalBody');
            if (b) b.innerHTML = '<div style="color:#e74c3c;padding:20px">⚠️ 请求失败: ' + e.message + '</div>';
        });
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
    function loadTab(tabId) {
        var d = el('datePicker').value, c = el('tab-' + tabId);
        if (!c) { c = el(tabId); } if (!c) return;
        c.innerHTML = '<div class="loading">加载中...</div>';
        var loader = LOADERS[tabId];
        try {
            if (loader) { loader(d, c); } else { c.innerHTML = '<div class="loading">未知页面: ' + tabId + '</div>'; }
        } catch (e) {
            console.error('[loadTab error]', tabId, e);
            c.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 加载失败: ' + e.message + '</div>';
            setStatus('加载失败', true);
        }
    }

    function refreshAll() {
        setStatus('加载中...');
        var a = document.querySelector('.sidebar-item.active');
        if (a) { var id = a.getAttribute('data-tab'); if (id && LOADERS[id]) loadTab(id); }
        setStatus('已刷新');
    }
    window.refreshAll = refreshAll;
    el('datePicker').addEventListener('change', refreshAll);

    /* ===== 板块轮动分析 ===== */
    function loadSectorRotationPage(d, c) {
        c.innerHTML = '<div class="loading"><span class="spinner"></span> 分析板块轮动中...</div>';
        fetch(API_BASE + '/sector-rotation?trade_date=' + d + '&days=10').then(function (r) { return r.json(); }).then(function (data) {
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

        // ── 顶部统计 ──
        h += '<div class="stats-bar">';
        h += card('回看天数', data.lookback_days + '天', 'blue');
        h += card('崛起行业', risingS.length, 'red');
        h += card('退潮行业', fallingS.length, 'green');
        h += card('崛起概念', risingC.length, 'orange');
        if (data.data_source) h += card('数据源', data.data_source === 'east' ? '东财成交额' : 'THS搜索热度', data.data_source === 'east' ? '#4caf50' : '#ff9800');
        h += '</div>';

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
        apiGet('/sector-heat-matrix?end_date=' + dateStr + '&days=26').then(function (res) {
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

    function loadSectorMovementPage(container) {
        container.innerHTML = '<div class="loading">加载板块异动数据...</div>';
        fetch('/api/sector/movement?group_by=all')
            .then(function(r) { return r.json(); })
            .then(function(data) {
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
                    fetch('/api/sector/movement?group_by=all').then(function(r) { return r.json(); }).then(function(d) {
                        if (!d.error) { sectorMoveData = d; renderSectorMovementPage(container, d); }
                    });
                }, 30000);
            })
            .catch(function(err) {
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

    function loadJqPicksPage(container) {
        container.innerHTML = '<div class="loading">加载策略选股数据中...</div>';
        fetch('/api/strategy/picks/list')
            .then(function(r) { return r.json(); })
            .then(function(meta) {
                var strategies = meta.strategies || [];
                if (!strategies.length) {
                    container.innerHTML = '<div style="text-align:center;padding:60px 20px;color:#888;">' +
                        '<div style="font-size:48px;margin-bottom:16px;">🤖</div>' +
                        '<div style="font-size:16px;font-weight:600;color:#555;margin-bottom:8px;">暂无策略数据</div>' +
                        '<div style="font-size:13px;color:#999;">请在聚宽平台运行策略后同步到此系统</div>' +
                        '</div>';
                    return;
                }
                var firstStrategy = strategies[0].strategy_name;
                fetchJqPicksData(container, firstStrategy, null, strategies);
            })
            .catch(function() {
                container.innerHTML = '<div class="loading">加载失败</div>';
            });
    }

    function fetchJqPicksData(container, strategyName, date, strategies) {
        var url = '/api/strategy/picks/data?strategy_name=' + encodeURIComponent(strategyName);
        if (date) url += '&date=' + date;
        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(data) {
                renderJqPicksPage(container, data, strategies);
            })
            .catch(function() {
                container.innerHTML = '<div class="loading">加载失败</div>';
            });
    }

    function renderJqPicksPage(container, data, strategies) {
        var picks = data.picks || [];
        var strategies = strategies || [];
        var h = '';

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
        loadJqPicksPage(c);
    };

    /* ===== AI推荐买入页面 ===== */
    function loadRecommendedPage(d, container) {
        container.innerHTML = '<div class="loading">加载推荐数据中...</div>';
        apiGet('/recommended-stocks?trade_date=' + d).then(function(res) {
            renderRecommendedPage(container, res, d);
        }).catch(function() {
            container.innerHTML = '<div class="loading">加载失败</div>';
        });
    }

    function renderRecommendedPage(container, data, dateStr) {
        var items = data.data || [];
        var h = '';

        // 检查是否是新格式
        var hasNewFormat = items.length > 0 && (items[0].long_term_score != null || items[0].short_term_score != null);

        // 顶部统计卡片
        var count = items.length;
        var avgScore = 0, maxScore = 0;
        if (count > 0) {
            var sum = 0;
            items.forEach(function(r) {
                var score = hasNewFormat ? (r.short_term_score || r.ai_score || 0) : (r.ai_score || 0);
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
        var todayStr = new Date().toISOString().split('T')[0];
        var displayDate = data.date || dateStr;
        var dateInfo = todayStr;
        if (displayDate && displayDate !== todayStr) {
            dateInfo = todayStr + '（数据截至 ' + displayDate + '）';
        }

        // 统计推荐买入数量
        var buyCount = 0, watchCount = 0, blockCount = 0;
        items.forEach(function(r) {
            var status = r.recommend_status || 'ALLOW';
            var score = r.short_term_score || r.ai_score || 0;
            if (status === 'BLOCK') blockCount++;
            else if (status === 'SUSPENDED') watchCount++;
            else if (score >= 60) buyCount++;
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
        // 分数筛选输入框
        h += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap;">';
        h += '<label style="font-size:13px;color:#888;">最低分数筛选:</label>';
        h += '<input id="recMinScoreInput" type="number" min="0" max="100" step="5" value="0" style="width:80px;padding:6px 10px;border:1px solid #ddd;border-radius:6px;font-size:13px;" oninput="window._filterRecommendedByScore()">';
        h += '<button onclick="window._filterRecommendedByScore()" style="padding:6px 14px;border:none;border-radius:6px;background:#1a73e8;color:#fff;font-size:12px;cursor:pointer;">筛选</button>';
        h += '</div>';

        if (!items.length) {
            h += '<div style="text-align:center;padding:60px 20px;color:#888;">';
            h += '<div style="font-size:48px;margin-bottom:16px;">💎</div>';
            h += '<div style="font-size:16px;font-weight:600;color:#555;margin-bottom:8px;">暂无推荐数据</div>';
            h += '<div style="font-size:13px;color:#999;margin-bottom:8px;">点击「开始筛选」按钮，系统将自动量化筛选 + AI评分</div>';
            h += '<div style="font-size:12px;color:#aaa;">筛选日期: ' + dateInfo + '（如已点击筛选但无结果，说明当前无股票满足全部条件）</div>';
            h += '</div>';
            container.innerHTML = h;
            return;
        }

        // 表格
        h += '<div style="overflow-x:auto;">';
        h += '<table class="data-table" style="width:100%;">';
        h += '<thead><tr>';
        h += '<th style="width:40px;">序号</th>';
        h += '<th>代码</th><th>名称</th>';

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
            var statusLabels = {ALLOW: '允许', SUSPENDED: '暂停', BLOCK: '禁止'};
            var riskColors = {LOW: '#27ae60', MEDIUM: '#f39c12', HIGH: '#e74c3c', CRITICAL: '#c0392b'};
            var rowScore = Math.round(r.short_term_score || r.ai_score || 0);

            h += '<tr data-score="' + rowScore + '" class="rec-row">';
            h += '<td style="text-align:center;color:#888;">' + (i + 1) + '</td>';
            h += '<td style="font-family:monospace;font-weight:600;">' + r.stock_code + '</td>';
            h += '<td>' + (r.short_name || '-') + '</td>';

            if (hasNewFormat) {
                // 推荐状态 - 醒目的买入建议标签
                var status = r.recommend_status || 'ALLOW';
                var stScore = r.short_term_score || r.ai_score || 0;
                var badgeText, badgeBg, badgeColor;
                if (status === 'BLOCK') {
                    badgeText = '❌ 不推荐'; badgeBg = 'linear-gradient(135deg,#c0392b,#e74c3c)'; badgeColor = '#fff';
                } else if (status === 'SUSPENDED') {
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
                h += '<td style="text-align:center;"><span style="color:' + riskColor + ';font-weight:600;">' + riskLevel + '</span></td>';
            } else {
                // 旧格式
                h += '<td style="text-align:center;font-weight:700;font-size:16px;color:' + scoreColor(r.ai_score) + ';">' + (r.ai_score != null ? r.ai_score : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.fundamental) + ';">' + (r.fundamental != null ? r.fundamental : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.capital_score) + ';">' + (r.capital_score != null ? r.capital_score : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.valuation) + ';">' + (r.valuation != null ? r.valuation : '-') + '</td>';
                h += '<td style="text-align:center;color:' + scoreColor(r.technical) + ';">' + (r.technical != null ? r.technical : '-') + '</td>';
            }

            h += '<td style="font-size:12px;color:#666;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + (r.reason || '') + '">' + (r.reason || '-') + '</td>';
            h += '<td style="font-size:11px;color:#888;">' + (r.sources || '-') + '</td>';
            h += '<td style="text-align:center;"><button onclick="window.openStockDetail(\'' + r.stock_code + '\')" style="padding:3px 10px;border:1px solid #1a73e8;border-radius:4px;background:transparent;color:#1a73e8;font-size:11px;cursor:pointer;">详情</button></td>';
            h += '</tr>';
        });

        h += '</tbody></table></div>';
        container.innerHTML = h;
    }

    window._runRecommendedScreen = function() {
        var statusEl = document.getElementById('recStatus');
        if (statusEl) statusEl.innerHTML = '⏳ 筛选中，请稍候（约1-2分钟）...';
        // 读取输入框的最低分数，默认50
        var scoreInput = document.getElementById('recMinScoreInput');
        var minScore = (scoreInput && scoreInput.value) ? parseInt(scoreInput.value) : 50;
        // 不传日期，由后端自动判断盘中/盘后
        fetch('/api/hot-data/recommended-stocks/run?min_score=' + minScore, { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(res) {
                if (statusEl) statusEl.innerHTML = '✅ ' + (res.note || '筛选已启动，正在分析中...');
                // 多次轮询刷新（筛选可能需要1-2分钟）
                var pollTimes = [30000, 60000, 90000, 120000];
                pollTimes.forEach(function(delay) {
                    setTimeout(function() {
                        var c = document.getElementById('tab-recommended');
                        if (c) loadRecommendedPage('', c);
                    }, delay);
                });
            })
            .catch(function() {
                if (statusEl) statusEl.innerHTML = '❌ 启动失败';
            });
    };

    // 按分数筛选推荐结果
    window._filterRecommendedByScore = function() {
        var input = document.getElementById('recMinScoreInput');
        var minScore = parseInt(input.value) || 0;
        var rows = document.querySelectorAll('.rec-row');
        var visibleCount = 0;
        rows.forEach(function(row) {
            var score = parseInt(row.getAttribute('data-score')) || 0;
            if (score >= minScore) {
                row.style.display = '';
                visibleCount++;
            } else {
                row.style.display = 'none';
            }
        });
        var countEl = document.getElementById('recFilteredCount');
        if (countEl) countEl.textContent = visibleCount;
    };

    function formatAmount(val) {
        if (val >= 1e12) return (val / 1e12).toFixed(2) + '万亿';
        if (val >= 1e8) return (val / 1e8).toFixed(2) + '亿';
        if (val >= 1e4) return (val / 1e4).toFixed(0) + '万';
        return val.toFixed(0);
    }

    var monitorRefreshTimer = null;
    var _monitorCharts = {};  // Chart.js 实例注册表，避免重复创建

    function loadMonitorPage(container) {
        // 清理旧图表实例
        Object.keys(_monitorCharts).forEach(function(k) {
            if (_monitorCharts[k]) { _monitorCharts[k].destroy(); delete _monitorCharts[k]; }
        });

        function fetchMonitorData() {
            fetch('/api/monitor/data')
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
            subtitle.innerHTML = '数据更新: ' + (data.update_time || '-') + rtBadge + ' | <span id="monitorRefreshStatus">自动刷新中...</span>';
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
        h += '数据更新: ' + (data.update_time || '-') + rtBadge + ' | ';
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
            document.querySelectorAll('.sidebar-item').forEach(function (b) { b.classList.remove('active'); });
            var btn = document.querySelector('[data-tab="' + savedTab + '"]');
            if (btn) btn.classList.add('active');
            document.querySelectorAll('.tab-content').forEach(function (c) { c.classList.remove('active'); });
            var tc = el('tab-' + savedTab) || el(savedTab);
            if (tc) tc.classList.add('active');
            el('pageTitle').textContent = (typeof PAGE_TITLES !== 'undefined' && PAGE_TITLES[savedTab]) || savedTab;
            loadTab(savedTab);
            return true;
        }
        return false;
    }

    function init() {
        var today = new Date().toISOString().split('T')[0];
        el('datePicker').value = today;
        // 应用保存的布局
        var savedLayout = 'new';
        try { savedLayout = localStorage.getItem('probiga_layout') || 'new'; } catch (e) {}
        applyLayout(savedLayout);
        // 恢复保存的页面
        var savedTab = '';
        try { savedTab = localStorage.getItem('probiga_current_tab') || ''; } catch (e) {}
        if (!_restoreTab(savedTab)) {
            // 没有保存的页面或页面不存在，加载默认页面
            loadTab('fused');
        }
        // 再更新日期
        fetch(API_BASE + '/latest-trade-date').then(function (r) { return r.json(); }).then(function (res) {
            if (res.latest_date && res.latest_date <= today) {
                el('datePicker').value = res.latest_date;
            }
        });
    }
    try { init(); } catch (e) {
        console.error('[init error]', e);
        var mc = el('mainContent');
        if (mc) mc.innerHTML = '<div class="loading" style="color:#e74c3c">⚠️ 初始化失败: ' + e.message + '</div>';
    }
})();
