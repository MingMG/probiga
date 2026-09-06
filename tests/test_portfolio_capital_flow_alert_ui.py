import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _script() -> str:
    return (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")


def test_watchlist_flow_alert_is_connected_to_initial_and_live_rendering():
    script = _script()
    portfolio = script.split("portfolio: function", 1)[1].split(
        "datasource: function", 1
    )[0]

    assert 'id="pfCapitalFlowAlert"' in portfolio
    assert "pfUpdateCapitalFlowAlert(res.data, capitalFlowSnapshot.observed_at_ms);" in portfolio
    assert "portfolioCapitalInflowAlerts(res.data, initialCapitalFlowSnapshot.observed_at_ms)" in portfolio
    assert "pfRefreshCapitalFlowAlertFreshness();" in portfolio
    assert "pfInvalidateCapitalFlowAlert();" in portfolio
    assert "pfFocusCapitalFlowAlert" in script
    assert "scrollIntoView({behavior:'smooth', block:'center', inline:'nearest'})" in script
    assert "至少需要 5 只有效自选股" in portfolio
    assert "近 5 分钟净流入占当日累计成交额的比例，在自选股中异常偏高" in portfolio
    assert "window._pfCapitalInflowAlertSeen" in portfolio
    assert "target.contains(document.activeElement)" in portfolio


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_watchlist_flow_alert_accepts_only_backend_verified_relative_anomaly():
    script = _script()
    start = script.index("    function portfolioCapitalInflowAlerts(rows")
    end = script.index("    window.pfFocusCapitalFlowAlert", start)
    helper = script[start:end]
    harness = f"""
const assert = require('assert');
{helper}
const rows = [
  {{stock_code:'000001',display_name:'甲',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_5m:300,flow_trade_date:'2026-09-06',expected_flow_date:'2026-09-06',flow_latest_time:'2026-09-06 10:05:00',flow_anomaly:{{status:'alert',direction:'inflow',normalized_flow_pct:0.03,robust_z:2.4,threshold:2,sample_size:8,method:'watchlist robust-z'}}}},
  {{stock_code:'000002',display_name:'乙',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_5m:600,flow_trade_date:'2026-09-06',expected_flow_date:'2026-09-06',flow_anomaly:{{status:'alert',direction:'inflow',normalized_flow_pct:0.04,robust_z:3.2,threshold:2,sample_size:8,method:'watchlist robust-z'}}}},
  {{stock_code:'000003',flow_status:'stale',flow_attitude_basis:'minute_5m_fresh',flow_5m:1000,flow_anomaly:{{status:'alert',direction:'inflow',robust_z:5,threshold:2}}}},
  {{stock_code:'000004',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_attitude:'strong_in',flow_5m:1000,flow_anomaly:{{status:'normal',direction:'inflow',robust_z:4,threshold:2}}}},
  {{stock_code:'000005',flow_status:'fresh',flow_attitude_basis:'minute_current_fresh',flow_5m:null,flow_anomaly:{{status:'alert',direction:'inflow',robust_z:5,threshold:2}}}},
  {{stock_code:'000006',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_5m:1000,flow_trade_date:'2026-09-05',expected_flow_date:'2026-09-06',flow_anomaly:{{status:'alert',direction:'inflow',robust_z:5,threshold:2}}}},
  {{stock_code:'000007',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_5m:1000,flow_trade_date:'2026-09-06',expected_flow_date:'2026-09-06',flow_anomaly:{{status:'alert',direction:'outflow',robust_z:5,threshold:2}}}},
  {{stock_code:'000008',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_attitude:'strong_in',flow_5m:2000,flow_trade_date:'2026-09-06',expected_flow_date:'2026-09-06'}},
  {{stock_code:'000009',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_5m:2000,flow_trade_date:'',expected_flow_date:'2026-09-06',flow_anomaly:{{status:'alert',direction:'inflow',robust_z:5,threshold:2}}}},
  {{stock_code:'000010',flow_status:'fresh',flow_attitude_basis:'minute_5m_fresh',flow_5m:2000,flow_trade_date:'2026-09-06',expected_flow_date:'',flow_anomaly:{{status:'alert',direction:'inflow',robust_z:5,threshold:2}}}}
].map(row => ({{quote_status:'fresh',quote_age_seconds:30,...row}}));
const alerts = portfolioCapitalInflowAlerts(rows);
assert.deepStrictEqual(alerts.map(item => item.stock_code), ['000002','000001']);
assert.strictEqual(alerts[1].anomaly_score, 2.4);
assert.strictEqual(alerts[1].sample_size, 8);
assert.strictEqual(alerts[1].basis, 'watchlist robust-z');
const eligible = rows[0];
for (const age of [0, 30, 90]) {{
  assert.strictEqual(portfolioCapitalInflowAlerts([{{...eligible,quote_age_seconds:age}}]).length, 1, 'fresh quote at or below 90 seconds is eligible');
}}
for (const age of [-1, 90.001, 91, null, undefined, NaN, Infinity, -Infinity, '', '30', false]) {{
  assert.deepStrictEqual(portfolioCapitalInflowAlerts([{{...eligible,quote_age_seconds:age}}]), [], 'missing, nonnumeric, nonfinite or stale quote age must be excluded: ' + String(age));
}}
for (const status of ['stale', 'closed', 'missing', '', null, undefined]) {{
  assert.deepStrictEqual(portfolioCapitalInflowAlerts([{{...eligible,quote_status:status}}]), [], 'only fresh quote status may alert');
}}
const observedAt = Date.now();
const realDateNow = Date.now;
Date.now = function() {{ return observedAt + 60001; }};
assert.deepStrictEqual(
  portfolioCapitalInflowAlerts([{{...eligible,quote_age_seconds:30}}], observedAt),
  [],
  'a rendered alert must expire as local elapsed time pushes its quote past 90 seconds'
);
Date.now = realDateNow;
let remembered = portfolioCapitalInflowRemember(alerts, {{}});
assert.strictEqual(remembered.has_new, true);
remembered = portfolioCapitalInflowRemember(alerts.slice().reverse(), remembered.seen);
assert.strictEqual(remembered.has_new, false, 'rank changes are not new alerts');
remembered = portfolioCapitalInflowRemember(alerts.slice(0, 1), remembered.seen);
assert.strictEqual(remembered.has_new, false, 'removed members are not new alerts');
remembered = portfolioCapitalInflowRemember([], remembered.seen);
assert.strictEqual(remembered.has_new, false);
remembered = portfolioCapitalInflowRemember(alerts.slice(1), remembered.seen);
assert.strictEqual(remembered.has_new, false, 'same-day reappearance remains remembered');
remembered = portfolioCapitalInflowRemember([{{...alerts[0],stock_code:'000011'}}], remembered.seen);
assert.strictEqual(remembered.has_new, true, 'a newly anomalous stock is announced');
remembered = portfolioCapitalInflowRemember([{{...alerts[0],trade_date:'2026-09-07'}}], remembered.seen);
assert.strictEqual(remembered.has_new, true, 'the same stock can alert again on a new trade date');
process.stdout.write(JSON.stringify({{status:'PASS'}}));
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_watchlist_flow_alert_timer_expires_and_failure_invalidates_existing_alert():
    script = _script()
    helper_start = script.index("    function portfolioCapitalInflowAlerts(rows")
    helper_end = script.index("    window.pfFocusCapitalFlowAlert", helper_start)
    helpers = script[helper_start:helper_end]
    ui_start = script.index("            function pfCapitalFlowAlertContent(rows")
    ui_end = script.index("            function pfWatchCell(r)", ui_start)
    alert_ui = script[ui_start:ui_end]
    harness = f"""
const assert = require('assert');
let now = 1000000;
const realDateNow = Date.now;
Date.now = function() {{ return now; }};
global.window = {{_pfCapitalInflowAlertSeen:{{}}}};
const target = {{
  innerHTML:'', className:'', attributes:{{}},
  setAttribute:function(key,value){{this.attributes[key]=value;}},
  contains:function(){{return false;}},
  querySelectorAll:function(){{return [];}},
  classList:{{remove:function(){{}}}}
}};
global.document = {{activeElement:null,getElementById:function(id){{return id==='pfCapitalFlowAlert'?target:null;}}}};
global.setTimeout = function(){{}};
function escHtml(value) {{ return String(value == null ? '' : value); }}
function escAttr(value) {{ return escHtml(value); }}
function shortDateTimeText(value) {{ return String(value || ''); }}
function fmtMoney(value) {{ return String(value); }}
{helpers}
{alert_ui}
const eligible = {{
  stock_code:'000001', display_name:'甲', quote_status:'fresh', quote_age_seconds:30,
  flow_status:'fresh', flow_attitude_basis:'minute_5m_fresh', flow_5m:300,
  flow_trade_date:'2026-09-06', expected_flow_date:'2026-09-06',
  flow_latest_time:'2026-09-06 10:05:00',
  flow_anomaly:{{status:'alert',direction:'inflow',normalized_flow_pct:0.03,robust_z:2.4,threshold:2,sample_size:8,method:'watchlist robust-z'}}
}};
let snapshot = pfRememberCapitalFlowAlertSnapshot([eligible]);
pfUpdateCapitalFlowAlert(snapshot.rows, snapshot.observed_at_ms);
assert.match(target.className, /has-alerts/);
assert.match(target.innerHTML, /🔥/);

now += 60001;
pfRefreshCapitalFlowAlertFreshness();
assert.doesNotMatch(target.className, /has-alerts/);
assert.doesNotMatch(target.innerHTML, /🔥/);
assert.match(target.innerHTML, /过期数据不提示/);

now += 1;
snapshot = pfRememberCapitalFlowAlertSnapshot([eligible]);
pfUpdateCapitalFlowAlert(snapshot.rows, snapshot.observed_at_ms);
assert.match(target.className, /has-alerts/);
pfInvalidateCapitalFlowAlert();
assert.doesNotMatch(target.className, /has-alerts/);
assert.doesNotMatch(target.innerHTML, /🔥/);
assert.strictEqual(window._pfCapitalFlowAlertSnapshot, null);
Date.now = realDateNow;
process.stdout.write(JSON.stringify({{status:'PASS'}}));
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_old_auto_refresh_failure_cannot_clear_a_newer_manual_alert():
    script = _script()
    start = script.index("            function pfFetchAndApplyLive(prefix)")
    end = script.index("            window.pfApplyPortfolioLivePayload", start)
    fetcher = script[start:end]
    harness = f"""
const assert = require('assert');
global.window = {{_pfManualRefreshToken:0, _pfLoadToken:1, _pfLiveInFlight:false}};
const portfolioLoadToken = 1;
const requests = [];
let invalidations = 0;
let statusWrites = 0;
function deferred() {{
  let resolve, reject;
  const promise = new Promise((yes, no) => {{ resolve = yes; reject = no; }});
  return {{promise, resolve, reject}};
}}
function fetchRawJsonWithTimeout() {{ const request = deferred(); requests.push(request); return request.promise; }}
function pfLiveUrl() {{ return '/api/portfolio/live'; }}
function pfIsActiveTab() {{ return true; }}
function pfApplyLivePayload() {{}}
function pfInvalidateCapitalFlowAlert() {{ invalidations += 1; }}
function pfSetLiveStatusText() {{ statusWrites += 1; }}
function shortDateTimeText(value) {{ return value; }}
function localDateTimeString() {{ return '2026-09-06 10:00:00'; }}
{fetcher}
(async function() {{
  const oldAuto = pfFetchAndApplyLive('');
  window._pfManualRefreshToken = 1;
  let newerManualAlertVisible = true;
  requests[0].reject(new Error('old auto request failed'));
  await oldAuto;
  assert.strictEqual(newerManualAlertVisible, true);
  assert.strictEqual(invalidations, 0, 'an old auto failure must not clear the newer manual alert');
  assert.strictEqual(statusWrites, 0, 'an old auto failure must not replace the newer manual status');

  const currentAuto = pfFetchAndApplyLive('');
  requests[1].reject(new Error('current auto request failed'));
  await currentAuto;
  assert.strictEqual(invalidations, 1, 'the current failed refresh must fail closed');
  assert.strictEqual(statusWrites, 1);
  process.stdout.write(JSON.stringify({{status:'PASS'}}));
}})().catch(function(error) {{ console.error(error); process.exit(1); }});
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}
