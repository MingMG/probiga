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
        label: job.source_label || (job.source === 'deepseek_web' ? 'DeepSeek 网页' : 'GPT（Codex）'),
        state: '回答已完成，页面展示的是实际返回来源'
      };
    }
    if (job.status === 'failed') {
      return { code: 'failed', label: '未产生回答', state: job.error_message || 'GPT 与 DeepSeek 网页均未能返回答案' };
    }
    if (job.provider_attempt === 'deepseek_web') {
      return { code: 'deepseek_web', label: '正在尝试：DeepSeek 网页', state: 'GPT（Codex）不可用，已切换到 DeepSeek 网页兜底' };
    }
    if (job.provider_attempt === 'codex_gpt') {
      return { code: 'codex_gpt', label: '正在尝试：GPT（Codex）', state: '问题已转发到固定 Codex 任务，正在等待原始回答' };
    }
    if (job.status === 'processing') {
      return { code: 'waiting', label: '工作器已接单', state: '正在确认可用的回答来源' };
    }
    return { code: 'waiting', label: '等待确认', state: '问题已进入生产队列，等待本机桥接工作器' };
  }

  function setHeadline(job) {
    var info = statusInfo(job);
    sourceLabel.className = 'source-value ' + info.code;
    sourceLabel.textContent = info.label;
    bridgeState.textContent = info.state;
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
    answer.replaceChildren();

    if (job.status === 'completed') {
      answer.className = 'answer-text';
      // textContent preserves the provider's returned text and never executes it as HTML.
      answer.textContent = job.answer == null ? '' : String(job.answer);
    } else if (job.status === 'failed') {
      answer.className = 'answer-text error';
      answer.textContent = job.error_message || '本次没有取得答案，请确认本机工作器和 DeepSeek 登录状态后重试。';
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
        bridgeState.textContent = '状态刷新失败：' + error.message + '，稍后自动重试';
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
        bridgeState.textContent = '历史记录加载失败：' + error.message;
      });
  }

  function resizeInput() {
    input.style.height = 'auto';
    input.style.height = Math.min(input.scrollHeight, 220) + 'px';
  }

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    var question = input.value;
    if (!question.trim() || sendButton.disabled) return;
    sendButton.disabled = true;
    bridgeState.textContent = '正在提交到生产问答队列……';
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
      bridgeState.textContent = '提交失败：' + error.message;
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

  resizeInput();
  loadHistory();
})();
