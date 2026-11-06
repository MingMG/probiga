"""Run the real app-shell date and announcement rendering code in Node."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


APP = Path(__file__).resolve().parents[1] / "server/static/js/app.js"


def run_client(tmp_path, fragment, assertions):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable")
    runner = tmp_path / "app-shell-regression.cjs"
    runner.write_text(
        "const assert = require('node:assert/strict');\n"
        "const vm = require('node:vm');\n"
        + "const fragment = " + json.dumps(fragment) + ";\n"
        + assertions,
        encoding="utf-8",
    )
    result = subprocess.run(
        [node, str(runner)], capture_output=True, text=True, encoding="utf-8", timeout=20
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_market_stage_date_is_shareable_and_fallback_preserves_request_date(tmp_path):
    source = APP.read_text(encoding="utf-8")
    fallback = source[source.index("    function syncDateFromResponse("):source.index("    function fusedSourceSummary(")]
    routing = source[source.index("    function isTradingDecisionTab("):source.index("    window.switchTab =")]
    run_client(tmp_path, fallback + routing, r"""
const picker = {value:'2026-11-04'};
const location = new URL('https://preview.test/?tab=market-stage&trade_date=2026-11-05');
const calls = [];
const context = {
    URL, URLSearchParams, console,
    window: {
        location,
        history:{replaceState(_, __, next){location.href = new URL(next, location.href).href;}},
    },
    el:id=>id==='datePicker'?picker:null,
    activeTabId:()=>'market-stage',
    currentDateValue:()=>picker.value,
    refreshAll:()=>calls.push(['refresh', picker.value]),
    setStatus:message=>calls.push(['status', message]),
};
vm.createContext(context);
vm.runInContext(fragment, context);
context.window.onDecisionDateChange();
assert.equal(location.searchParams.get('trade_date'), '2026-11-04');
assert.deepEqual(calls[0], ['refresh','2026-11-04']);
context.syncDateFromResponse({fallback:true,date:'2026-10-30'});
assert.equal(picker.value, '2026-11-04');
assert.equal(location.searchParams.get('trade_date'), '2026-11-04');
assert.match(calls[1][1], /2026-11-04.*2026-10-30/);
""")


def test_announcement_links_reject_script_urls_and_escape_titles(tmp_path):
    source = APP.read_text(encoding="utf-8")
    start = source.index("    function escAttr(v)")
    end = source.index("    function safeText(v)")
    run_client(tmp_path, source[start:end], r"""
const context = {URL};
vm.createContext(context);
vm.runInContext(fragment, context);
const title = '<img src=x onerror="alert(1)">';
const invalid = context.noticeLink('javascript:alert(1)', title);
assert.equal(invalid, '&lt;img src=x onerror=&quot;alert(1)&quot;&gt;');
assert.equal(context.noticeLink('//attacker.test/item', '公告'), '公告');
const valid = context.noticeLink('https://data.eastmoney.com/notices/detail/600000/a.html?q="x"&kind=notice', title);
assert.match(valid, /^<a href="https:\/\/data\.eastmoney\.com\/notices\/detail\/600000\/a\.html\?q=%22x%22&amp;kind=notice"/);
assert.match(valid, /rel="noopener noreferrer"/);
assert.doesNotMatch(valid, /<img/);
""")
