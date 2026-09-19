(function () {
  'use strict';
  var charts = {}, pending = false, hasData = false, activeRequest = null;
  var embedded = new URLSearchParams(window.location.search).get('embedded') === '1';
  var chartText = embedded ? '#888' : '#a8b2d1';
  var chartGrid = embedded ? 'rgba(0,0,0,.04)' : 'rgba(255,255,255,.05)';
  var parentVisible = !embedded;
  function parentMessage(type, detail) {
    if (!embedded || window.parent === window) return;
    window.parent.postMessage(Object.assign({type: type}, detail || {}), window.location.origin);
  }
  function reportHeight() { parentMessage('probiga-monitor-height', {height: el('app').scrollHeight + 16}); }
  function el(id) { return document.getElementById(id); }
  function numeric(value) { return value != null && value !== '' && typeof value !== 'boolean' && Number.isFinite(Number(value)); }
  function shown(value, digits) { return numeric(value) ? Number(value).toFixed(digits == null ? 0 : digits) : '—'; }
  function change(value, suffix) { return numeric(value) ? (Number(value) > 0 ? '+' : '') + Number(value).toFixed(2) + (suffix || '%') : '—'; }
  function color(value) { return !numeric(value) || Number(value) === 0 ? '' : Number(value) > 0 ? 'positive' : 'negative'; }
  function text(id, value) { el(id).textContent = value; }
  function notice(message) { text('monitorNotice', message); el('monitorNotice').hidden = !message; reportHeight(); }
  function esc(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (character) { return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[character]; }); }

  function gauge(value) {
    var canvas = el('gaugeChart'), ctx = canvas.getContext('2d');
    canvas.width = 300; canvas.height = 180;
    ctx.beginPath(); ctx.arc(150, 160, 120, Math.PI, 2 * Math.PI);
    ctx.lineWidth = 30; ctx.strokeStyle = embedded ? '#e8edf3' : 'rgba(255,255,255,0.15)'; ctx.stroke();
    if (!numeric(value)) return;
    var angle = Math.PI + Math.max(0, Math.min(100, Number(value))) / 100 * Math.PI;
    ctx.beginPath(); ctx.arc(150, 160, 120, Math.PI, angle);
    ctx.strokeStyle = '#3498db'; ctx.stroke();
    ctx.beginPath(); ctx.moveTo(150, 160); ctx.lineTo(150 + 80 * Math.cos(angle), 160 + 80 * Math.sin(angle));
    ctx.lineWidth = 3; ctx.strokeStyle = embedded ? '#333' : '#fff'; ctx.stroke();
  }

  function chart(id, labels, datasets, horizontal) {
    if (charts[id]) { charts[id].destroy(); delete charts[id]; }
    if (typeof window.Chart !== 'function') return;
    charts[id] = new window.Chart(el(id), {
      type: horizontal ? 'bar' : 'line',
      data: {labels: labels, datasets: datasets},
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        indexAxis: horizontal ? 'y' : 'x',
        interaction: {mode: 'index', intersect: false},
        plugins: {legend: {display: !horizontal, labels: {color: chartText}}},
        scales: {
          x: {ticks: {color: chartText, maxTicksLimit: 7}, grid: {color: chartGrid}},
          y: {ticks: {color: chartText}, grid: {color: chartGrid}},
          y1: {display: id === 'heatChart', position: 'right', grid: {drawOnChartArea: false}, ticks: {color: '#3498db'}}
        }
      }
    });
  }
  function series(label, values, stroke, axis) {
    return {label: label, data: values || [], borderColor: stroke, backgroundColor: stroke,
      tension: 0, pointRadius: 2, fill: false, spanGaps: false, yAxisID: axis || 'y'};
  }
  function plateTable(id, rows) {
    el(id).innerHTML = rows.length ? rows.map(function (row) {
      return '<tr><td>' + esc(row.name) + '</td><td class="value">' + shown(row.heat, 2) + ' ' + esc(row.heat_unit || '') +
        '</td><td class="' + color(row.change) + '" title="' + esc(row.change_method || '') + '">' + change(row.change) + '</td></tr>';
    }).join('') : '<tr><td colspan="3">所选交易日暂无可用数据</td></tr>';
  }
  function plates(chartId, basisId, rows, tableId) {
    var unit = rows.length ? rows[0].heat_unit : '', method = rows.length ? rows[0].change_method : '';
    text(basisId, rows.length ? rows[0].trade_date + ' · ' + (unit || '口径未提供') + ' · ' + method + ' · ' + (rows[0].membership_basis || '') : '所选交易日暂无可用数据');
    chart(chartId, rows.map(function (row) { return row.name; }), [{
      label: unit || '观测值', data: rows.map(function (row) { return row.heat; }), backgroundColor: '#3498db'
    }], true);
    plateTable(tableId, rows);
  }
  function render(data) {
    hasData = true; el('loading').style.display = 'none';
    ['headerEl', 'emotionSection', 'chartsSection', 'auxSection', 'detailSection'].forEach(function (id) {
      el(id).style.display = id === 'headerEl' ? 'flex' : 'grid';
    });
    var statuses = {realtime: '盘中快照', paused: '午休快照', close: '收盘数据', fallback: '历史数据', unavailable: '数据缺失', stale: '行情已过期'};
    text('updateTime', data.data_time || '时间未知'); text('tradeDate', data.trade_date || '—');
    text('snapshotStatus', (statuses[data.freshness_status] || '状态未知') + ' · 北京时间 · ' + (data.total_count || 0) + '个样本');
    var notes = [];
    if (data.requested_date !== data.trade_date) notes.push('请求 ' + data.requested_date + '，当前展示 ' + data.trade_date + ' 的历史数据。');
    if (data.freshness_status === 'stale') notes.push('当前行情已过期，仅可作为历史快照观察。');
    if (typeof window.Chart !== 'function') notes.push('图表组件未加载，数值与明细仍可阅读。');
    notice(notes.join(' '));

    var percentile = data.heat_percentile;
    text('gaugeValue', shown(percentile)); gauge(percentile);
    text('percentileSample', '此前 ' + (data.heat_percentile_sample_count || 0) + ' 个有数据交易日，不含当日');
    text('gaugeStatus', !numeric(percentile) ? '缺少可比样本' : percentile < 30 ? '广度分位偏低' : percentile > 70 ? '广度分位偏高' : '广度分位居中');
    el('gaugeStatus').className = 'gauge-status neutral';
    text('heatValue', shown(data.market_heat)); text('heatChange', change(data.breadth_change_pp, ' 个百分点'));
    el('heatChange').className = color(data.breadth_change_pp);
    text('heatCurrentValue', shown(data.market_heat)); text('heatCurrentChange', change(data.breadth_change_pp, ' 个百分点'));
    el('heatCurrentChange').className = 'metric-change ' + color(data.breadth_change_pp);
    text('heatPercentile', numeric(percentile) ? 'P' + shown(percentile) : '—');
    var analysis = data.analysis || {};
    [['analysisTemp','market_temp'],['analysisIndustry','industry_focus'],['analysisStyle','style_judge'],['analysisCapital','capital_flow'],['analysisSignal','signal']].forEach(function (item) {
      text(item[0], analysis[item[1]] || '暂无可用观察');
    });

    var history = data.history || {}, dates = history.trade_dates || history.dates || [];
    chart('heatChart', dates, [series('上涨占比 × 1000', history.heat, '#e74c3c'), series('成交额（亿元）', history.amount, '#3498db', 'y1')]);
    chart('tmtChart', dates, [series('TMT行业成交样本占比（%）', history.tmt_ratio, '#e74c3c')]);
    chart('sidelineChart', dates, [series('小波动个股占比（%）', history.sideline, '#f39c12')]);
    chart('csi1000Chart', dates, [series('中证1000（点）', history.csi1000_price, '#3498db')]);
    text('tmtRatio', numeric(data.tmt_ratio) ? shown(data.tmt_ratio, 2) + '%' : '—');
    text('sidelineRatio', numeric(data.sideline_ratio) ? shown(data.sideline_ratio, 2) + '%' : '—');
    var index = data.csi1000 || {};
    text('csi1000Heat', shown(index.price, 2)); text('csi1000Chg', change(index.change));
    el('csi1000Chg').className = 'metric-value ' + color(index.change);
    text('indexBasis', '000852 · ' + (statuses[index.status] || '状态未知') + ' · ' + (index.data_time || '无行情时间'));

    var industries = data.top_industries || [], concepts = data.concept_rows || [];
    plates('industryChart', 'industryBasis', industries, 'industryTableBody');
    plates('conceptChart', 'conceptBasis', concepts, 'conceptTableBody');
    var values = industries.map(function (row) { return row.heat; }).filter(numeric);
    text('industryAvgHeat', values.length ? shown(values.reduce(function (a, b) { return a + Number(b); }, 0) / values.length, 2) + ' ' + industries[0].heat_unit : '—');
    reportHeight();
  }

  async function refresh() {
    if (pending || document.hidden || !parentVisible) return;
    pending = true;
    var controller = new AbortController(), timer = setTimeout(function () { controller.abort(); }, 12000);
    activeRequest = controller;
    try {
      var requested = new URLSearchParams(window.location.search).get('date');
      var response = await fetch('/api/monitor/data' + (requested ? '?date=' + encodeURIComponent(requested) : ''), {cache: 'no-store', signal: controller.signal});
      var data = await response.json();
      if (!response.ok || data.error || data.status === 'unavailable') throw new Error(data.message || '市场观察暂时不可用');
      if (parentVisible && !document.hidden) render(data);
    } catch (error) {
      if (!parentVisible || document.hidden) return;
      var message = error.name === 'AbortError' ? '请求超时，请稍后重试。' : '市场观察读取失败，请稍后重试。';
      notice((hasData ? '刷新失败，以下保留上次成功读取的数据；请检查各项数据时间。' : '') + message);
      if (!hasData) text('loading', message);
      else text('snapshotStatus', '刷新失败 · 上次成功数据，仅供历史查看');
    } finally { clearTimeout(timer); pending = false; activeRequest = null; }
  }
  document.addEventListener('DOMContentLoaded', function () {
    if (embedded) document.body.classList.add('embedded');
    parentMessage('probiga-monitor-ready'); reportHeight(); refresh(); setInterval(refresh, 15000);
  });
  document.addEventListener('visibilitychange', function () { if (!document.hidden) refresh(); });
  window.addEventListener('message', function (event) {
    if (!embedded || event.origin !== window.location.origin || event.source !== window.parent || !event.data || event.data.type !== 'probiga-monitor-visibility') return;
    parentVisible = event.data.visible === true;
    if (!parentVisible && activeRequest) activeRequest.abort();
    if (parentVisible) refresh();
  });
})();
