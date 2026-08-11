(function () {
  'use strict';

  var form = document.getElementById('authForm');
  var username = document.getElementById('username');
  var password = document.getElementById('password');
  var confirmWrap = document.getElementById('confirmWrap');
  var confirmPassword = document.getElementById('confirmPassword');
  var title = document.getElementById('formTitle');
  var description = document.getElementById('formDescription');
  var submitButton = document.getElementById('submitButton');
  var errorBox = document.getElementById('formError');
  var modeBadge = document.getElementById('modeBadge');
  var firstAccountNote = document.getElementById('firstAccountNote');
  var loadingPanel = document.getElementById('loadingPanel');
  var formPanel = document.getElementById('formPanel');
  var mode = 'login';

  function safeNext() {
    var raw = new URLSearchParams(window.location.search).get('next') || '/';
    try {
      var value = decodeURIComponent(raw);
      if (!value.startsWith('/') || value.startsWith('//') || value.startsWith('/login')) return '/';
      return value;
    } catch (e) {
      return '/';
    }
  }

  function setError(message) {
    errorBox.textContent = message || '';
    errorBox.hidden = !message;
  }

  function setMode(nextMode) {
    mode = nextMode;
    var registering = mode === 'register';
    var closed = mode === 'closed';
    modeBadge.textContent = closed ? '注册窗口关闭' : (registering ? '首次初始化' : '安全登录');
    title.textContent = closed ? '等待重新开放注册' : (registering ? '创建管理员账号' : '欢迎回来');
    description.textContent = closed
      ? '系统尚未创建管理员，但本次安全注册窗口已经到期。'
      : (registering
        ? '这是唯一一次公开注册。创建成功后，注册入口会自动关闭。'
        : '输入账号和密码进入 ProBigA。');
    submitButton.textContent = registering ? '创建账号并进入系统' : '登录';
    confirmWrap.hidden = !registering;
    confirmPassword.required = registering;
    password.autocomplete = registering ? 'new-password' : 'current-password';
    form.hidden = closed;
    firstAccountNote.hidden = !(registering || closed);
    firstAccountNote.textContent = closed
      ? '请联系系统维护者重新开启一个短时注册窗口。入口不会永久暴露在公网。'
      : '第一个账号会成为系统管理员。创建成功后，其他公网访问者不能再自行注册。';
    setError('');
  }

  function setBusy(busy) {
    submitButton.disabled = busy;
    username.disabled = busy;
    password.disabled = busy;
    confirmPassword.disabled = busy;
    submitButton.textContent = busy
      ? (mode === 'register' ? '正在创建…' : '正在登录…')
      : (mode === 'register' ? '创建账号并进入系统' : '登录');
  }

  function parseResponse(response) {
    return response.json().catch(function () { return {}; }).then(function (data) {
      if (!response.ok) {
        var error = new Error(data.message || '请求失败，请稍后重试。');
        error.code = data.error || '';
        throw error;
      }
      return data;
    });
  }

  function loadStatus() {
    fetch('/api/auth/status', { cache: 'no-store', headers: { 'Accept': 'application/json' } })
      .then(parseResponse)
      .then(function (data) {
        if (data.authenticated) {
          window.location.replace(safeNext());
          return;
        }
        setMode(data.registration_open ? 'register' : (data.user_initialized ? 'login' : 'closed'));
        loadingPanel.hidden = true;
        formPanel.hidden = false;
        window.setTimeout(function () { username.focus(); }, 30);
      })
      .catch(function (error) {
        loadingPanel.hidden = true;
        formPanel.hidden = false;
        setMode('login');
        setError(error.message || '登录服务暂时不可用。');
      });
  }

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    setError('');
    var userValue = username.value.trim();
    var passwordValue = password.value;
    if (mode === 'register' && passwordValue !== confirmPassword.value) {
      setError('两次输入的密码不一致。');
      confirmPassword.focus();
      return;
    }
    setBusy(true);
    fetch('/api/auth/' + mode, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ username: userValue, password: passwordValue })
    }).then(parseResponse)
      .then(function () {
        window.location.replace(safeNext());
      })
      .catch(function (error) {
        setBusy(false);
        if (error.code === 'registration_closed') {
          setMode('login');
          setError('管理员账号刚刚已经创建，请直接登录。');
          return;
        }
        setError(error.message || '操作失败，请重试。');
      });
  });

  Array.prototype.forEach.call(document.querySelectorAll('[data-toggle-password]'), function (button) {
    button.addEventListener('click', function () {
      var target = document.getElementById(button.getAttribute('data-toggle-password'));
      if (!target) return;
      var showing = target.type === 'text';
      target.type = showing ? 'password' : 'text';
      button.textContent = showing ? '显示' : '隐藏';
      target.focus();
    });
  });

  if (window.location.protocol !== 'https:') {
    document.getElementById('transportWarning').hidden = false;
  }
  loadStatus();
})();
