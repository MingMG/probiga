(function () {
  'use strict';

  var API = '/api/ai-bridge';
  var channel = document.body.dataset.channel === 'stock' ? 'stock' : 'general';
  var form = document.getElementById('questionForm');
  var input = document.getElementById('questionInput');
  var sendButton = document.getElementById('sendButton');
  var conversation = document.getElementById('conversation');
  var emptyState = document.getElementById('emptyState');
  var sourceLabel = document.getElementById('sourceLabel');
  var bridgeState = document.getElementById('bridgeState');
  var bridgeDiagnostics = document.getElementById('bridgeDiagnostics');
  var jobs = new Map();
  var pollTimer = null;

  function requestJson(url, options) {
    var init = Object.assign({
      headers: { 'Accept': 'application/json' },
      cache: 'no-store'
    }, options || {});
    if (init.body) {
      init.headers = Object.assign({}, init.headers, { 'Content-Type': 'application/json' });
    }
    return fetch(url, init).then(function (response) {
      return response.json().catch(function () { return {}; }).then(function (data) {
        if (!response.ok) {
          var detail = data.detail || data.message || ('请求失败（' + response.status + '）');
          throw new Error(typeof detail === 'string' ? detail : '请求失败');
        }
        return data;
      });
    });
  }

  function formatTime(value) {
    if (!value) return '';
    var date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleString('zh-CN', { hour12: false });
  }

  function statusInfo(job) {
    if (job.status === 'completed') {
      return {
        code: job.source || 'waiting',
        label: job.source_label || ({deepseek_web:'DeepSeek 网页',codex_gpt:'GPT（Codex）'}[job.source]) || '来源未确认',
        state: '回答已完成，请结合来源和证据日期核对'
      };
    }
    if (job.status === 'failed') {
      return { code: 'failed', label: '未产生回答', state: '本次未取得答案，可重新提交；详细原因见诊断' };
    }
    if (job.provider_attempt === 'deepseek_web') {
      return { code: 'deepseek_web', label: '正在尝试：DeepSeek 网页', state: '正在等待回答' };
    }
    if (job.provider_attempt === 'codex_gpt') {
      return { code: 'codex_gpt', label: '正在尝试：GPT（Codex）', state: '正在分析问题' };
    }
    if (job.status === 'processing') {
      return { code: 'waiting', label: '正在处理', state: '正在确认可用的回答来源' };
    }
    return { code: 'waiting', label: '等待回答', state: '问题已提交，正在排队处理' };
  }

  function diagnosticText(job) {
    return ['请求编号：'+(job.request_id||'尚未提交'),'状态：'+(job.status||'未知'),
      '实际来源：'+(job.source_label||job.source||'尚未返回'),'当前来源尝试：'+(job.provider_attempt||'尚未确认'),
      '创建时间：'+(job.created_at||'—'),'完成时间：'+(job.completed_at||'—'),
      '诊断：'+(job.error_message||'无错误记录')].join('\n');
  }

  function setHeadline(job) {
    var info = statusInfo(job);
    sourceLabel.className = 'source-value ' + info.code;
    sourceLabel.textContent = info.label;
    bridgeState.textContent = info.state;
    if (bridgeDiagnostics) bridgeDiagnostics.textContent = diagnosticText(job);
  }

  function pendingDots() {
    var dots = document.createElement('span');
    dots.className = 'dot-pulse';
    dots.setAttribute('aria-hidden', 'true');
    for (var i = 0; i < 3; i += 1) dots.appendChild(document.createElement('i'));
    return dots;
  }

  function createExchange(job) {
    var exchange = document.createElement('article');
    exchange.className = 'exchange';
    exchange.dataset.requestId = job.request_id;

    var questionRow = document.createElement('div');
    questionRow.className = 'question-row';
    var question = document.createElement('div');
    question.className = 'question-bubble';
    question.textContent = job.question;
    questionRow.appendChild(question);

    var answerCard = document.createElement('div');
    answerCard.className = 'answer-card';
    var meta = document.createElement('div');
    meta.className = 'answer-meta';
    var badge = document.createElement('span');
    badge.className = 'source-badge waiting';
    var time = document.createElement('span');
    time.className = 'answer-time';
    meta.appendChild(badge);
    meta.appendChild(time);
    var answer = document.createElement('div');
    answer.className = 'answer-text pending';
    answerCard.appendChild(meta);
    answerCard.appendChild(answer);
    var analysisNote = document.createElement('p');
    analysisNote.className = 'analysis-note';
    analysisNote.textContent = channel === 'stock'
      ? '模型研究观点。回答中的价格、公告与持仓事实需按原始来源和截止日期核对；不替代系统行情或买卖条件。'
      : '模型回答。重要事实请结合回答引用的原始来源核对。';
    answerCard.appendChild(analysisNote);
    var diagnostics = document.createElement('details');
    diagnostics.className = 'answer-diagnostics';
    var summary = document.createElement('summary');
    summary.textContent = '查看回答来源与诊断';
    var diagnosticBody = document.createElement('pre');
    diagnostics.appendChild(summary);
    diagnostics.appendChild(diagnosticBody);
    answerCard.appendChild(diagnostics);

    exchange.appendChild(questionRow);
    exchange.appendChild(answerCard);
    conversation.appendChild(exchange);
    updateExchange(job, exchange);
    return exchange;
  }

  function updateExchange(job, exchange) {
    var info = statusInfo(job);
    var badge = exchange.querySelector('.source-badge');
    var answer = exchange.querySelector('.answer-text');
    var time = exchange.querySelector('.answer-time');
    badge.className = 'source-badge ' + info.code;
    badge.textContent = info.label;
    time.textContent = formatTime(job.completed_at || job.created_at);
    exchange.querySelector('.answer-diagnostics pre').textContent = diagnosticText(job);
    exchange.querySelector('.analysis-note').hidden = job.status !== 'completed';
    answer.replaceChildren();

    if (job.status === 'completed') {
      answer.className = 'answer-text';
      // textContent preserves the provider's returned text and never executes it as HTML.
      answer.textContent = job.answer == null ? '' : String(job.answer);
    } else if (job.status === 'failed') {
      answer.className = 'answer-text error';
      answer.textContent = '本次没有取得答案，可重新提交问题。具体原因保留在下方诊断中。';
    } else {
      answer.className = 'answer-text pending';
      answer.appendChild(document.createTextNode(info.state));
      answer.appendChild(pendingDots());
    }
  }

  function upsertJob(job, prepend) {
    var existing = jobs.get(job.request_id);
    jobs.set(job.request_id, job);
    var exchange = conversation.querySelector('[data-request-id="' + CSS.escape(job.request_id) + '"]');
    if (!exchange) {
      if (emptyState) emptyState.remove();
      exchange = createExchange(job);
      if (prepend) conversation.prepend(exchange);
    } else {
      updateExchange(job, exchange);
    }
    if (!existing || existing.status !== job.status || existing.provider_attempt !== job.provider_attempt) {
      setHeadline(job);
    }
    return exchange;
  }

  function hasPendingJobs() {
    var pending = false;
    jobs.forEach(function (job) {
      if (job.status === 'queued' || job.status === 'processing') pending = true;
    });
    return pending;
  }

  function schedulePoll() {
    if (pollTimer) window.clearTimeout(pollTimer);
    if (!hasPendingJobs()) return;
    pollTimer = window.setTimeout(pollPending, 2500);
  }

  function pollPending() {
    var pendingIds = [];
    jobs.forEach(function (job, id) {
      if (job.status === 'queued' || job.status === 'processing') pendingIds.push(id);
    });
    Promise.all(pendingIds.map(function (id) {
      return requestJson(API + '/questions/' + encodeURIComponent(id)).then(function (data) {
        upsertJob(data.job, false);
      }).catch(function (error) {
        bridgeState.textContent = '状态暂时无法刷新，稍后自动重试';
        if (bridgeDiagnostics) bridgeDiagnostics.textContent = '状态刷新失败：'+error.message;
      });
    })).finally(schedulePoll);
  }

  function loadHistory() {
    requestJson(API + '/questions?channel=' + encodeURIComponent(channel) + '&limit=20')
      .then(function (data) {
        var history = Array.isArray(data.jobs) ? data.jobs.slice().reverse() : [];
        history.forEach(function (job) { upsertJob(job, false); });
        if (history.length) {
          setHeadline(history[history.length - 1]);
          conversation.scrollTop = conversation.scrollHeight;
        }
        schedulePoll();
      })
      .catch(function (error) {
        bridgeState.textContent = '历史记录暂时不可用，仍可输入新问题';
        if (bridgeDiagnostics) bridgeDiagnostics.textContent = '历史记录加载失败：'+error.message;
      });
  }

  function resizeInput() {
    input.style.height = 'auto';
    input.style.height = Math.min(input.scrollHeight, 220) + 'px';
  }

  function prefillFromSearch(search, targetChannel) {
    var params = new URLSearchParams(search || '');
    var question = params.get('question') || '';
    var rawCode = String(params.get('stock_code') || '').trim().split('.')[0];
    var code = targetChannel === 'stock' && /^\d{6}$/.test(rawCode) ? rawCode : '';
    if (code && !question.trim()) question = '请分析 '+code+'。请分别列出：可核实的事实及来源与截止日期、研究观点及反方证据、仍待验证的条件。缺失证据请明确标注。';
    else if (code && question.indexOf(code) < 0) question = '研究对象：'+code+'\n'+question;
    return {stockCode:code, question:question.slice(0,8000)};
  }

  function applyPrefill() {
    var prefill = prefillFromSearch(window.location.search, channel);
    if (prefill.question) input.value = prefill.question;
    var context = document.getElementById('stockContext');
    var contextLink = document.getElementById('stockContextLink');
    if (context && contextLink && prefill.stockCode) {
      context.hidden = false;
      contextLink.textContent = prefill.stockCode+' · 查看系统个股事实';
      contextLink.href = '/?tab=workbench&stock_code='+encodeURIComponent(prefill.stockCode);
    }
    if (prefill.question) document.getElementById('inputHint').textContent = '问题已预填，请核对后发送';
  }

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    var question = input.value;
    if (!question.trim() || sendButton.disabled) return;
    sendButton.disabled = true;
    bridgeState.textContent = '正在提交问题……';
    requestJson(API + '/questions', {
      method: 'POST',
      body: JSON.stringify({ channel: channel, question: question })
    }).then(function (data) {
      var exchange = upsertJob(data.job, false);
      input.value = '';
      resizeInput();
      exchange.scrollIntoView({ behavior: 'smooth', block: 'end' });
      schedulePoll();
    }).catch(function (error) {
      bridgeState.textContent = '提交失败，问题仍保留，可重试';
      if (bridgeDiagnostics) bridgeDiagnostics.textContent = '提交失败：'+error.message;
      sourceLabel.className = 'source-value failed';
      sourceLabel.textContent = '提交失败';
    }).finally(function () {
      sendButton.disabled = false;
      input.focus();
    });
  });

  input.addEventListener('input', resizeInput);
  input.addEventListener('keydown', function (event) {
    if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      form.requestSubmit();
    }
  });

  applyPrefill();
  resizeInput();
  loadHistory();
})();
