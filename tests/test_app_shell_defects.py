"""Two shell defects that made the pages look broken everywhere.

1. `showFatalError` wrote into `#mainContent`, which exists zero times in
   index.html.  `el('mainContent')` was therefore null and the error handler
   itself threw, so a page that hit any fatal path showed nothing at all.
2. The 策略选股 page read its table from the POST /api/screener/run receipt,
   but that endpoint only queues a delivery task and its receipt carries no
   rows - every view of the page came out as "没有候选".  The table has to come
   from the published batch at /api/screener/history.

Both are checked by running the real client code, following the pattern in
tests/test_monitor_accuracy.py: app.js is a browser script, so it is evaluated
in a Node vm with a stubbed DOM.
"""
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "server/static/js/app.js"


def _run_node(tmp_path, name, body, *argv):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable")
    runner = tmp_path / name
    runner.write_text(body, encoding="utf-8")
    result = subprocess.run(
        [node, str(runner), *argv], capture_output=True, text=True, timeout=60,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_fatal_error_banner_lands_beside_the_shell(tmp_path):
    source = APP_JS.read_text(encoding="utf-8")
    start = source.index("    function showFatalError(message) {")
    fragment = source[start:source.index("    function setStatus(msg, err) {")]
    assert "showFatalError" in fragment

    _run_node(tmp_path, "fatal-banner.cjs", r"""
const assert = require('assert'), vm = require('vm');
const fragment = FRAGMENT;
// The DOM the bug happened in: the shell has #contentArea and no #mainContent.
const contentArea = {
  id: 'contentArea', children: [], firstChild: null,
  insertBefore(child) { this.children.unshift(child); this.firstChild = this.children[0]; child.parentNode = this; },
};
const nodes = { contentArea };
const context = {
  document: {
    body: { id: 'body' },
    getElementById: id => nodes[id] || null,
    createElement: () => {
      const el = { style: {}, dataset: {}, attributes: {},
                   setAttribute: (k, v) => { el.attributes[k] = v; } };
      Object.defineProperty(el, 'id', { get() { return el.attributes.id; },
                                        set(value) { el.attributes.id = value; nodes[value] = el; } });
      return el;
    },
  },
};
vm.runInNewContext('function el(id){ return document.getElementById(id); }\n' + fragment, context);
context.showFatalError('行情接口不可用');
assert.equal(contentArea.children.length, 1, 'the banner was not placed in the shell');
const banner = contentArea.children[0];
assert.equal(banner.textContent, '行情接口不可用');
assert.equal(banner.attributes.role, 'alert', 'a fatal banner must announce itself');
assert.equal(banner.attributes.id, 'globalErrorBanner');
assert.ok(!('mainContent' in nodes));
assert.ok(!fragment.includes('mainContent'), 'the fatal path still targets the absent #mainContent');
""".replace("FRAGMENT", json.dumps(fragment)))


def test_screener_table_comes_from_the_published_batch(tmp_path):
    _run_node(tmp_path, "screener-results.cjs", r"""
const assert = require('assert'), vm = require('vm');
process.on('unhandledRejection', () => {});
const noop = () => {};
const nodes = {};
function stub(id) {
  const el = {
    innerHTML: '', textContent: '', value: '', checked: false, hidden: false, src: '',
    style: {}, dataset: {}, children: [],
    classList: { add: noop, remove: noop, toggle: noop, contains: () => false },
    setAttribute: noop, getAttribute: () => null, removeAttribute: noop,
    addEventListener: noop, removeEventListener: noop, dispatchEvent: noop,
    appendChild(c) { this.children.push(c); c.parentNode = this; return c; },
    insertBefore(c) { this.children.unshift(c); c.parentNode = this; return c; },
    removeChild(c) { this.children = this.children.filter(x => x !== c); return c; },
    querySelector: () => null, querySelectorAll: () => [], focus: noop, click: noop,
    getContext: () => null, closest: () => null, contains: () => false,
  };
  Object.defineProperty(el, 'id', {
    get() { return id; },
    set(value) { nodes[value] = el; },
  });
  return el;
}
const document = {
  hidden: false, readyState: 'complete', cookie: '',
  getElementById: id => nodes[id] || (nodes[id] = stub(id)),
  querySelector: () => null, querySelectorAll: () => [],
  createElement: () => stub(undefined), createTextNode: () => stub(undefined),
  addEventListener: noop, removeEventListener: noop,
  body: stub('body'), head: stub('head'), documentElement: stub('documentElement'),
};
const context = {
  document, console, setTimeout, clearTimeout, setInterval, clearInterval,
  JSON, Math, Date, Promise, Object, Array, String, Number, Boolean, RegExp, Error,
  parseInt, parseFloat, isNaN, isFinite, encodeURIComponent, decodeURIComponent,
  URL, reject: noop, AbortController: function () { this.abort = noop; this.signal = {}; },
  localStorage: { getItem: () => null, setItem: noop, removeItem: noop },
  location: { href: 'http://localhost/', search: '', hash: '', pathname: '/' },
  navigator: { userAgent: 'node' },
  requestAnimationFrame: noop, cancelAnimationFrame: noop,
  Element: function () {}, HTMLElement: function () {}, Node: function () {},
  Event: function () {}, CustomEvent: function () {},
  MutationObserver: function () { this.observe = noop; this.disconnect = noop; },
  matchMedia: () => ({ matches: false, addListener: noop, removeListener: noop,
                       addEventListener: noop, removeEventListener: noop }),
  getComputedStyle: () => ({ getPropertyValue: () => '' }),
  alert: noop, confirm: () => false, open: () => null,
  addEventListener: noop, removeEventListener: noop, dispatchEvent: noop,
};
context.window = context;
context.self = context;
context.globalThis = context;
context.fetch = () => Promise.resolve({ ok: true, status: 200, json: async () => ({}), text: async () => '' });

const source = require('fs').readFileSync(process.argv[2], 'utf8');
try { vm.runInNewContext(source, context, { filename: 'app.js' }); }
catch (error) { /* init() touches DOM ids the stub set does not model */ }
assert.equal(typeof context.window.loadScreenerResults, 'function');

let payload = {};
const requests = [];
context.fetch = (url, options) => {
  requests.push({ url: String(url), method: ((options || {}).method) || 'GET' });
  return Promise.resolve({ ok: true, status: 200, json: async () => payload });
};

const definition = { as_of_date: '2026-10-16', preset: 'trend_breakout' };
const published = {
  status: 'ok', requested_date: '2026-10-16', data_date: '2026-10-16',
  source: 'published', freshness: 'exact', preset: { name: '趋势突破' },
  stats: { input_count: 5000, result_count: 1, scan_limit: 5000, selector_summary: { grades: { A: 1 } } },
  data: [{ stock_code: '600001', stock_name: '示例股', score: 88, candidate_grade: 'A',
           change_pct: 1.5, action: 'WATCH', rank: 1 }],
};

(async () => {
  payload = published;
  await context.window.loadScreenerResults(definition);
  assert.equal(requests.length, 1, JSON.stringify(requests));
  assert.equal(requests[0].method, 'GET');
  assert.equal(requests[0].url,
    '/api/screener/history?data_date=2026-10-16&preset=trend_breakout&limit=200',
    'the table must be read from the published batch, not from the queue receipt');
  assert.ok(nodes.screenerResults.innerHTML.includes('600001'));
  assert.ok(nodes.screenerResults.innerHTML.includes('趋势突破'));

  // The launch receipt carries no rows: it must not be painted as results.
  requests.length = 0;
  payload = { queued: true, task_type: 'unified_screener', status: 'queued' };
  await context.window.loadScreenerResults(definition);
  assert.equal(requests[0].url,
    '/api/screener/history?data_date=2026-10-16&preset=trend_breakout&limit=200');
  const rendered = nodes.screenerResults.innerHTML;
  assert.ok(!rendered.includes('600001'), 'a queue receipt was rendered as a result row');
  assert.ok(rendered.includes('sc-empty-cell'), rendered.slice(0, 400));
})().catch(error => { console.error(error); process.exit(1); });
""", str(APP_JS))


def test_the_run_button_delegates_to_the_published_batch_reader():
    source = APP_JS.read_text(encoding="utf-8")
    assert re.search(
        r"window\.loadScreenerResults\(screenerDefinitionFromForm\(\)\)", source
    ), "提交筛选后没有回到已发布批次的读取路径"
