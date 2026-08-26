import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _script() -> str:
    return (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")


def test_main_strategy_pool_requests_exact_run_before_strictly_older_fallback():
    script = _script()

    assert "function candidateCenterStockPoolWithHistoricalFallback(requestedDate)" in script
    assert "candidateCenterStockPoolWithHistoricalFallback(requestedDate)" in script
    assert "var exactPath = '/api/v3/stock-pool'" in script
    assert "fetchRawJsonWithTimeout('/api/v3/stock-pool?before_session_date='" in script
    assert "latestSession < target" in script
    assert "function candidateCenterStockPoolIsReadable(pool)" in script
    assert "candidateCenterStockPoolIsReadable(latest)" in script
    assert "latest.historical_read_only === true" in script
    assert "historical_fallback_status || '') === 'HISTORICAL_READ_ONLY'" in script
    assert "is_historical_fallback: true" in script
    assert "exact_run_missing: true" in script


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_main_strategy_pool_executes_only_bounded_verified_fallback():
    script = _script()
    start = script.index("    function candidateCenterStockPoolIsReadable(pool)")
    end = script.index("    function loadCandidateCenterPage", start)
    functions = script[start:end]
    harness = f"""
const assert = require('assert');
let responses = Object.create(null), calls = [];
function fetchRawJsonWithTimeout(path) {{
  calls.push(path);
  if (!(path in responses)) return Promise.reject(new Error('unexpected '+path));
  return Promise.resolve(responses[path]);
}}
{functions}
function readable(uid, session, target, status, count) {{
  const items = Array.from({{length:count}}, () => ({{is_strategy_candidate:true}}));
  return {{data:{{run_uid:uid,decision_session_date:session,trade_date:session,
    pool_readable:true,run_status:'COMPLETED',decision_integrity_verified:true,
    pool_status:status,items:items,
    summary:{{stock_count:items.length,strategy_candidate_count:count}},
    before_session_date:target,requested_trade_date:target,
    is_historical_fallback:true,historical_read_only:true,
    historical_fallback_status:'HISTORICAL_READ_ONLY',
    historical_fallback_session_date:session}}}};
}}
(async () => {{
  const exact='/api/v3/stock-pool?trade_date=2026-08-26';
  const bounded='/api/v3/stock-pool?before_session_date=2026-08-26';
  responses[exact]={{data:{{run_uid:null,pool_status:'UNAVAILABLE',
    pool_readable:false,items:[],summary:{{stock_count:0,strategy_candidate_count:0}}}}}};
  responses[bounded]=readable('older','2026-08-25','2026-08-26','READY',1);
  const historical=await candidateCenterStockPoolWithHistoricalFallback('2026-08-26');
  assert.deepStrictEqual(calls,[exact,bounded]);
  assert.strictEqual(historical.run_uid,'older');
  assert.strictEqual(historical.is_historical_fallback,true);
  assert.strictEqual(historical.historical_read_only,true);

  responses[bounded]=readable('newer','2026-08-27','2026-08-26','READY',1);
  calls=[];
  const unavailable=await candidateCenterStockPoolWithHistoricalFallback('2026-08-26');
  assert.strictEqual(unavailable.run_uid,null);
  assert.strictEqual(unavailable.is_historical_fallback,false);
  process.stdout.write(JSON.stringify({{status:'PASS'}}));
}})().catch(error => {{ console.error(error); process.exit(1); }});
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


def test_main_strategy_pool_never_merges_historical_pool_into_today_ready_context():
    script = _script()

    assert "return { data: {}, _load_error:" in script
    assert "var contextState = historicalFallback ? 'STALE' : tradingDecisionTruth(context);" in script
    assert "!!(!historicalFallback && context.run_uid && pool.run_uid" in script
    assert "var displayedRunUid = historicalFallback ? pool.run_uid" in script
    assert "var displayedExpectedDataDate = historicalFallback ? '-'" in script
    assert "HISTORICAL_READ_ONLY / 历史只读" in script
    assert "原请求日 " in script
    assert "决策日 " in script
    assert "数据日 " in script
    assert "也不代表请求日的 READY 结论" in script
    assert "历史批次仍只作独立只读回看，不与请求日合并" in script


def test_main_strategy_pool_fails_closed_when_no_readable_batch_exists():
    script = _script()

    assert "if (!pool.run_uid && contextState !== 'BLOCKED') contextState = 'UNAVAILABLE';" in script
    assert "这不是正常空态，不得解释为没有候选" in script
    assert "historicalFallback ? 'HISTORICAL_READ_ONLY' : 'RESEARCH_ONLY'" in script
    assert "历史回看，全部不可执行" in script
    assert "历史批次不可入队" in script
