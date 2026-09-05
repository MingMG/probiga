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
    assert "pfUpdateCapitalFlowAlert(res.data);" in portfolio
    assert "portfolioCapitalInflowAlerts(res.data)" in portfolio
    assert "pfFocusCapitalFlowAlert" in script
    assert "scrollIntoView({behavior:'smooth', block:'center', inline:'nearest'})" in script
    assert "至少需要 5 只有效自选股" in portfolio
    assert "近 5 分钟净流入占当日累计成交额的比例，在自选股中异常偏高" in portfolio
    assert "window._pfCapitalInflowAlertSeen" in portfolio
    assert "target.contains(document.activeElement)" in portfolio


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_watchlist_flow_alert_accepts_only_backend_verified_relative_anomaly():
    script = _script()
    start = script.index("    function portfolioCapitalInflowAlerts(rows)")
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
];
const alerts = portfolioCapitalInflowAlerts(rows);
assert.deepStrictEqual(alerts.map(item => item.stock_code), ['000002','000001']);
assert.strictEqual(alerts[1].anomaly_score, 2.4);
assert.strictEqual(alerts[1].sample_size, 8);
assert.strictEqual(alerts[1].basis, 'watchlist robust-z');
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
