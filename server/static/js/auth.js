(function () {
  'use strict';

  var AUTH_STATUS_URL = '/api/auth/status';
  var AUTH_REFRESH_URL = '/api/auth/refresh';
  var AUTH_LOGOUT_URL = '/api/auth/logout';
  var redirecting = false;
  var refreshTimer = null;
  var nativeFetch = window.fetch ? window.fetch.bind(window) : null;

  function safeNextPath() {
    var path = window.location.pathname + window.location.search + window.location.hash;
    return path && path !== '/login' ? path : '/';
  }

  function redirectToLogin() {
    if (redirecting || window.location.pathname === '/login') return;
    redirecting = true;
    window.location.replace('/login?next=' + encodeURIComponent(safeNextPath()));
  }

  function isAuthRequest(input) {
    var raw = typeof input === 'string' ? input : (input && input.url) || '';
    try {
      var url = new URL(raw, window.location.origin);
      return url.origin === window.location.origin && url.pathname.indexOf('/api/auth/') === 0;
    } catch (e) {
      return false;
    }
  }

  function installFetchGuard() {
    if (!nativeFetch || window.fetch._probigaAccountGuard) return;
    var guardedFetch = function (input, init) {
      return nativeFetch(input, init).then(function (response) {
        if (
          response.status === 401 &&
          !isAuthRequest(input) &&
          response.headers &&
          response.headers.get('X-ProBigA-Admin-Auth') === 'required'
        ) {
          redirectToLogin();
        }
        return response;
      });
    };
    guardedFetch._probigaAccountGuard = true;
    guardedFetch._probigaNativeFetch = nativeFetch;
    window.fetch = guardedFetch;
  }

  function parseUtc(value) {
    var parsed = Date.parse(value || '');
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function scheduleRefresh(session) {
    if (refreshTimer) window.clearTimeout(refreshTimer);
    var refreshAt = parseUtc(session && session.refresh_after);
    var expiresAt = parseUtc(session && session.expires_at);
    if (!expiresAt) return;
    var target = refreshAt || (expiresAt - 60 * 60 * 1000);
    var wait = Math.max(15 * 1000, Math.min(target - Date.now(), 2147480000));
    refreshTimer = window.setTimeout(refreshSession, wait);
  }

  function refreshSession() {
    if (!nativeFetch) return Promise.resolve(null);
    return nativeFetch(AUTH_REFRESH_URL, {
      method: 'POST',
      headers: { 'Accept': 'application/json' }
    }).then(function (response) {
      if (!response.ok) {
        redirectToLogin();
        return null;
      }
      return response.json();
    }).then(function (data) {
      if (data && data.authenticated) scheduleRefresh(data);
      return data;
    }).catch(function () {
      // Keep the current session during a transient network failure and retry.
      refreshTimer = window.setTimeout(refreshSession, 5 * 60 * 1000);
      return null;
    });
  }

  function injectAccountControl(user) {
    if (!user || document.getElementById('probigaAccountControl')) return;
    var link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = '/static/css/auth.css?v=1';
    document.head.appendChild(link);

    var control = document.createElement('div');
    control.id = 'probigaAccountControl';
    control.className = 'probiga-account-control';

    var avatar = document.createElement('span');
    avatar.className = 'probiga-account-avatar';
    avatar.textContent = String(user.username || 'A').slice(0, 1).toUpperCase();

    var name = document.createElement('span');
    name.className = 'probiga-account-name';
    name.textContent = user.username || '管理员';

    var logout = document.createElement('button');
    logout.type = 'button';
    logout.className = 'probiga-account-logout';
    logout.textContent = '退出';
    logout.addEventListener('click', function () {
      logout.disabled = true;
      nativeFetch(AUTH_LOGOUT_URL, { method: 'POST', headers: { 'Accept': 'application/json' } })
        .catch(function () { return null; })
        .finally(function () {
          window.location.replace('/login');
        });
    });

    control.appendChild(avatar);
    control.appendChild(name);
    control.appendChild(logout);
    document.body.appendChild(control);
  }

  function bootstrap() {
    if (!nativeFetch || window.location.pathname === '/login') return;
    nativeFetch(AUTH_STATUS_URL, {
      headers: { 'Accept': 'application/json' },
      cache: 'no-store'
    }).then(function (response) {
      return response.json().catch(function () { return {}; }).then(function (data) {
        data = data || {};
        data._responseOk = response.ok;
        return data;
      });
    }).then(function (data) {
      // Development can explicitly disable the account boundary. Missing or
      // malformed status responses still fail closed in the request catch.
      if (data.required !== true) return;
      if (!data._responseOk || !data.authenticated) {
        redirectToLogin();
        return;
      }
      injectAccountControl(data.user);
      scheduleRefresh(data);
    }).catch(function () {
      redirectToLogin();
    });
  }

  installFetchGuard();
  window.probigaAuth = {
    refresh: refreshSession,
    logout: function () {
      return nativeFetch(AUTH_LOGOUT_URL, { method: 'POST' }).finally(redirectToLogin);
    }
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootstrap);
  } else {
    bootstrap();
  }
})();
