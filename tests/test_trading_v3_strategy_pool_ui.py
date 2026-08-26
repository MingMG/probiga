import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_strategy_pool_defaults_to_strategy_selected_stocks_without_hiding_research_rows():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")
    page = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")

    assert "STRATEGY-SELECTED STOCK POOL" in page
    assert "策略选股池" in page
    assert 'value="RESEARCH_ONLY">研究观察（不可执行）' in page
    assert "item.is_strategy_candidate===true" in script
    assert "st==='REJECTED'&&item.actionability==='REJECTED'" in script
    assert "x.actionability!=='RESEARCH_ONLY'&&x.actionability!=='REJECTED'" in script
    assert "researchDefaultFallback=!st&&!preferredDefaultRows.length" in script
    assert "preferredDefaultRows.length?preferredDefaultRows:researchDefaultRows" in script
    assert ".filter(function(item){return item.actionability!=='RESEARCH_ONLY'})" not in script
    assert "研究观察（不可执行）" in script


def test_strategy_pool_uses_only_an_older_readable_batch_as_historical_fallback():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")

    assert "function stockPoolWithHistoricalFallback(requestedDate)" in script
    assert "function stockPoolIsReadable(pool)" in script
    assert "return api3('/stock-pool?before_session_date='" in script
    assert "latestSession<target" in script
    assert "latest.historical_read_only===true" in script
    assert "latest.historical_fallback_status||'')==='HISTORICAL_READ_ONLY'" in script
    assert "runStatus==='COMPLETED'" in script
    assert "pool.decision_integrity_verified===true" in script
    assert "pool.pool_readable===true" in script
    assert "Number.isInteger(stockCount)" in script
    assert "datePattern.test(sessionDate)" in script
    assert "candidateCount===actualCandidateCount" in script
    assert "exactSession===target" in script
    assert "is_historical_fallback:true" in script
    assert "exact_run_missing:true" in script
    assert "历史只读 · 原" in script
    assert "HISTORICAL_READ_ONLY" in script
    assert "全部不可执行，也不会创建模拟或真实订单" in script
    assert "历史保护位 " in script
    assert "不可作为当前指令" in script


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_strategy_pool_fallback_executes_fail_closed_for_partial_batches():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(
        encoding="utf-8"
    )
    start = script.index("  function stockPoolIsReadable(pool)")
    end = script.index("  function errorText(err)", start)
    functions = script[start:end]
    harness = f"""
const assert = require('assert');
let responses = Object.create(null), calls = [];
function unwrap(value) {{ return value && value.data !== undefined ? value.data : value; }}
function api3(path) {{
  calls.push(path);
  if (!(path in responses)) return Promise.reject(new Error('unexpected '+path));
  return Promise.resolve(responses[path]);
}}
{functions}
function readable(runUid, session, status, candidateCount) {{
  const items = Array.from({{length:candidateCount}}, () => ({{is_strategy_candidate:true}}));
  return {{run_uid:runUid, decision_session_date:session, trade_date:session,
    pool_readable:true, run_status:'COMPLETED', decision_integrity_verified:true,
    pool_status:status, items:items,
    summary:{{stock_count:items.length,strategy_candidate_count:candidateCount}}}};
}}
function boundedReadable(runUid, session, target, status, candidateCount) {{
  return Object.assign(readable(runUid,session,status,candidateCount), {{
    before_session_date:target,requested_trade_date:target,
    is_historical_fallback:true,historical_read_only:true,
    historical_fallback_status:'HISTORICAL_READ_ONLY',
    historical_fallback_session_date:session
  }});
}}
(async () => {{
  responses = {{
    '/stock-pool?trade_date=2026-08-26': {{run_uid:'partial',
      decision_session_date:'2026-08-26',trade_date:'2026-08-25',
      pool_readable:false,run_status:'PROCESSING',
      decision_integrity_verified:false,pool_status:'UNAVAILABLE',items:[],
      summary:{{stock_count:0,strategy_candidate_count:0}}}},
    '/stock-pool?before_session_date=2026-08-26': boundedReadable('older-complete','2026-08-25','2026-08-26','READY',1)
  }};
  calls = [];
  const historical = await stockPoolWithHistoricalFallback('2026-08-26');
  assert.deepStrictEqual(calls, ['/stock-pool?trade_date=2026-08-26','/stock-pool?before_session_date=2026-08-26']);
  assert.strictEqual(historical.run_uid, 'older-complete');
  assert.strictEqual(historical.is_historical_fallback, true);
  assert.strictEqual(historical.historical_read_only, true);
  assert.strictEqual(historical.exact_run_unreadable, true);

  responses = {{
    '/stock-pool?trade_date=2026-08-26': readable('verified-empty','2026-08-26','EMPTY',0)
  }};
  calls = [];
  const empty = await stockPoolWithHistoricalFallback('2026-08-26');
  assert.deepStrictEqual(calls, ['/stock-pool?trade_date=2026-08-26']);
  assert.strictEqual(empty.pool_status, 'EMPTY');
  assert.strictEqual(empty.is_historical_fallback, false);
  const stringCount = readable('forged-string-count','2026-08-26','READY',1);
  stringCount.summary.stock_count = '1';
  assert.strictEqual(stockPoolIsReadable(stringCount), false);
  const futureData = readable('forged-future-data','2026-08-26','READY',1);
  futureData.trade_date = '2026-08-27';
  assert.strictEqual(stockPoolIsReadable(futureData), false);

  responses = {{
    '/stock-pool?trade_date=2026-08-26': {{run_uid:null,pool_status:'UNAVAILABLE',
      pool_readable:false,items:[],summary:{{stock_count:0,strategy_candidate_count:0}}}},
    '/stock-pool?before_session_date=2026-08-26': Object.assign(boundedReadable('forged','2026-08-25','2026-08-26','READY',1),
      {{summary:{{stock_count:0,strategy_candidate_count:1}}}})
  }};
  calls = [];
  const unavailable = await stockPoolWithHistoricalFallback('2026-08-26');
  assert.strictEqual(unavailable.run_uid, null);
  assert.strictEqual(unavailable.exact_run_missing, true);
  assert.strictEqual(unavailable.is_historical_fallback, false);
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


def test_strategy_pool_summary_exposes_each_decision_layer():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")
    page = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")

    assert 'id="candidateHistoryNotice"' in page
    assert 'id="candidateResearchNotice"' in page
    assert 'id="candidatePoolStats"' in page
    assert "summary.strategy_candidate_count" in script
    assert "summary.wait_trigger_count" in script
    assert "summary.target_count" in script
    assert "summary.rejected_count" in script
    assert "研究目标（不可直接下单）" in page


def test_strategy_pool_governance_deferred_mode_never_renders_action_ranges():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(
        encoding="utf-8"
    )

    assert (
        "governanceDeferred=pool.governance_deferred===true"
        "||pool.activation_enabled===false"
    ) in script
    assert (
        "actionability=governanceDeferred?'RESEARCH_ONLY'"
    ) in script
    assert (
        "if(governanceDeferred){buyText='治理数据库延期，不提供当前买入计划'"
    ) in script
    assert "sellText='治理数据库延期，不提供当前卖出计划'" in script
    assert "emergency='治理数据库延期，不提供当前止损指令'" in script
    assert "governanceDeferred?'治理延期 · 只读研究'" in script
    assert (
        "全部为 RESEARCH_ONLY；当前不展示可执行买卖区间或止损指令"
    ) in script
