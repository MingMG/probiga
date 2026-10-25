(function () {
    'use strict';

    function esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function number(value, digits) {
        var parsed = Number(value);
        if (!Number.isFinite(parsed)) return '—';
        return parsed.toFixed(digits == null ? 2 : digits);
    }

    function money(value) {
        var parsed = Number(value);
        if (!Number.isFinite(parsed)) return '—';
        return (parsed / 100000000).toFixed(2) + ' 亿';
    }

    function metric(label, value, color) {
        return '<div style="background:#fff;border:1px solid #e2e8f0;border-radius:11px;padding:12px;min-width:0;">' +
            '<div style="font-size:11px;color:#64748b;font-weight:700;">' + esc(label) + '</div>' +
            '<div style="font-size:22px;color:' + (color || '#0f172a') + ';font-weight:900;margin-top:3px;">' + esc(value) + '</div></div>';
    }

    function renderScenarios(data) {
        var colors = {CONTINUATION: '#15803d', DIVERGENCE: '#c2410c', RETREAT: '#b91c1c', ROTATION: '#1d4ed8', REPAIR: '#7c3aed', STABILIZE: '#475569'};
        return '<section style="margin-top:16px;"><div class="section-title">🗺 明天三种剧本</div><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(235px,1fr));gap:12px;">' +
            (data.scenarios || []).map(function (scenario) {
                var color = colors[scenario.key] || '#334155';
                return '<article style="background:#fff;border:1px solid #e2e8f0;border-top:5px solid ' + color + ';border-radius:12px;padding:15px;">' +
                    '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start;">' +
                    '<strong style="font-size:16px;color:#0f172a;">' + esc(scenario.label) + '</strong>' +
                    '<span style="font-size:22px;font-weight:900;color:' + color + ';">' + esc(scenario.probability_pct) + '%</span></div>' +
                    '<p style="font-size:13px;color:#475569;line-height:1.65;margin:10px 0;">' + esc(scenario.description) + '</p>' +
                    '<div style="font-size:12px;color:' + color + ';background:#f8fafc;border-radius:8px;padding:9px;line-height:1.55;"><strong>动作：</strong>' + esc(scenario.action) + '</div></article>';
            }).join('') + '</div></section>';
    }

    function renderStocks(data) {
        if (!data.stocks || !data.stocks.length) {
            return '<section style="margin-top:16px;background:#fff7ed;border:1px solid #fed7aa;border-radius:12px;padding:16px;color:#9a3412;">当前票池没有带盘中封板证据的战术票，明天以市场确认和新题材首板为主。</section>';
        }
        return '<section style="margin-top:16px;"><div class="section-title">🎯 明天核心票计划</div><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:12px;">' +
            data.stocks.map(function (stock) {
                var leader = String(stock.role || '').indexOf('龙头') >= 0;
                var color = leader ? '#b91c1c' : '#c2410c';
                return '<article style="background:#fff;border:1px solid #e2e8f0;border-left:5px solid ' + color + ';border-radius:12px;padding:15px;">' +
                    '<div style="display:flex;justify-content:space-between;gap:12px;"><div><div style="font-size:11px;color:#64748b;">' + esc(stock.role || '战术候选') + '</div>' +
                    '<div style="font-size:19px;font-weight:900;color:#0f172a;margin-top:2px;">' + esc(stock.short_name) + ' <span style="color:#64748b;font-size:13px;">' + esc(stock.stock_code) + '</span></div></div>' +
                    '<div style="font-size:12px;color:' + color + ';font-weight:900;text-align:right;">短线 ' + number(stock.short_term_score, 1) + '<br>资金 ' + number(stock.capital_score, 1) + '</div></div>' +
                    '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-top:12px;">' +
                    metric('昨收', number(stock.close, 2)) + metric('明日涨停参考', number(stock.next_limit_up_price, 2), color) + metric('前日VWAP', number(stock.previous_vwap, 2)) + '</div>' +
                    '<div style="font-size:12px;color:#475569;line-height:1.65;margin-top:11px;"><strong>周五证据：</strong>最低 ' + number(stock.low_return_pct, 2) + '%，低点回拉 ' + number(stock.rebound_from_low_pct, 2) + '%，首次封板 ' + esc(stock.first_limit_time || '—') + '，开板 ' + number(stock.open_board_count, 0) + ' 次</div>' +
                    '<div style="font-size:12px;color:#475569;line-height:1.65;margin-top:7px;"><strong>触发：</strong>' + esc(stock.trigger || '等待次日盘中确认') + '</div>' +
                    '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:11px;">' +
                    metric('入场下沿', number(stock.entry_price_low, 2)) + metric('入场上沿', number(stock.entry_price_high, 2)) + metric('失效线', number(stock.stop_loss_price, 2), '#b91c1c') + metric('第一目标', number(stock.take_profit_1, 2), '#15803d') + '</div>' +
                    '<div style="font-size:11px;color:#9a3412;margin-top:9px;line-height:1.55;"><strong>失效：</strong>' + esc(stock.invalidation || '龙头炸板或跌破计划止损') + '</div></article>';
            }).join('') + '</div></section>';
    }

    function renderTimeline(data) {
        return '<section style="margin-top:16px;"><div class="section-title">⏱ 明天盯盘顺序</div><div style="display:grid;gap:9px;">' +
            (data.session_timeline || []).map(function (item) {
                return '<div style="display:grid;grid-template-columns:72px 150px 1fr;gap:12px;background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:11px 13px;align-items:center;">' +
                    '<strong style="font-size:16px;color:#0f172a;">' + esc(item.time) + '</strong>' +
                    '<span style="font-size:13px;font-weight:800;color:#1d4ed8;">' + esc(item.title) + '</span>' +
                    '<span style="font-size:12px;color:#475569;line-height:1.55;">' + esc(item.condition) + '</span></div>';
            }).join('') + '</div></section>';
    }

    function render(data) {
        if (!data || data.status !== 'READY') {
            return '<div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:12px;padding:18px;color:#9a3412;">' +
                '<strong>明日推演暂不可用</strong><div style="margin-top:6px;font-size:13px;">' + esc((data && data.reason) || '数据不足') + '</div></div>';
        }
        var current = data.current_market || {};
        var previous = data.previous_market || {};
        var pool = data.pool || {};
        var h = '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;">' +
            '<div><div style="font-size:12px;color:#64748b;font-weight:800;">NEXT SESSION OUTLOOK</div>' +
            '<h2 style="margin:4px 0 3px;color:#0f172a;">' + esc(data.next_trade_date || '下一交易日') + ' 盘面推演</h2>' +
            '<div style="font-size:12px;color:#64748b;">基于 ' + esc(data.trade_date) + ' 收盘数据 · 生成于 ' + esc(data.generated_at) + '</div></div>' +
            '<div style="display:flex;gap:8px;"><button type="button" onclick="switchTab(\'recommended\')" style="padding:8px 13px;border:1px solid #1d4ed8;border-radius:8px;background:#fff;color:#1d4ed8;cursor:pointer;">查看完整票池</button>' +
            '<button type="button" id="marketOutlookRefresh" style="padding:8px 13px;border:0;border-radius:8px;background:#0f172a;color:#fff;cursor:pointer;">刷新</button></div></div>' +
            '<section style="margin-top:16px;padding:16px 18px;border-radius:14px;background:linear-gradient(135deg,#0f172a,#1e3a5f);color:#fff;">' +
            '<div style="display:flex;justify-content:space-between;gap:14px;align-items:center;flex-wrap:wrap;"><div><div style="font-size:11px;color:#93c5fd;font-weight:800;">当前市场状态</div>' +
            '<div style="font-size:25px;font-weight:900;margin-top:3px;">' + esc(data.market_mode_label) + ' · ' + esc(data.tactical_style) + '</div></div>' +
            '<div style="font-size:12px;color:#dbeafe;line-height:1.7;max-width:680px;">' + (data.evidence || []).map(esc).join(' · ') + '</div></div></section>' +
            '<section style="margin-top:12px;display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:9px;">' +
            metric('上涨宽度', number(current.breadth_pct, 1) + '%', '#15803d') +
            metric('等权涨幅', number(current.average_change_pct, 2) + '%', '#15803d') +
            metric('涨停代理', number(current.limit_up_count, 0) + ' 家', '#b91c1c') +
            metric('跌停代理', number(current.limit_down_count, 0) + ' 家', '#b91c1c') +
            metric('成交额', money(current.amount_cny)) +
            metric('前日宽度', number(previous.breadth_pct, 1) + '%', '#64748b') + '</section>' +
            renderScenarios(data) + renderStocks(data) + renderTimeline(data) +
            '<div style="margin-top:15px;padding:12px 14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;font-size:11px;color:#64748b;line-height:1.65;">' +
            esc(data.notice || '') + '<br>票池：' + esc(pool.recommendation_count || 0) + ' 只，执行授权 ' + esc(pool.executable_count || 0) + ' 只 · run_uid ' + esc(pool.run_uid || '—') + '</div>';
        setTimeout(function () {
            var button = document.getElementById('marketOutlookRefresh');
            if (button) button.addEventListener('click', function () { load(data.trade_date, document.getElementById('tab-market-outlook')); });
        }, 0);
        return h;
    }

    function load(tradeDate, container) {
        if (!container) return;
        container.innerHTML = '<div class="loading">正在生成下一交易日推演…</div>';
        var query = tradeDate ? '?trade_date=' + encodeURIComponent(String(tradeDate).slice(0, 10)) : '';
        var controller = new AbortController();
        var timer = setTimeout(function () { controller.abort(); }, 20000);
        fetch('/api/market-outlook' + query, {signal: controller.signal})
            .then(function (response) { return response.json(); })
            .then(function (data) { container.innerHTML = render(data); })
            .catch(function (error) {
                container.innerHTML = render({status: 'DATA_BLOCKED', reason: error && error.name === 'AbortError' ? '请求超过20秒' : String(error && error.message || error)});
            })
            .finally(function () { clearTimeout(timer); });
    }

    window.MarketOutlook = {load: load, render: render};
}());
