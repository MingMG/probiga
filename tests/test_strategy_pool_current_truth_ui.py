import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _node(source: str) -> dict:
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=source,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_trading_v3_formal_pool_truth_fails_closed_for_old_or_research_batches():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")
    start = script.index("  function stockPoolIsReadable(pool)")
    end = script.index("  function stockPoolWithHistoricalFallback(requestedDate)", start)
    functions = script[start:end]
    harness = f"""
const assert = require('assert');
{functions}
function readable() {{
  return {{run_uid:'verified',decision_session_date:'2026-08-28',trade_date:'2026-08-28',
    pool_readable:true,run_status:'COMPLETED',decision_integrity_verified:true,
    pool_status:'READY',items:[{{is_strategy_candidate:true}}],
    summary:{{stock_count:1,strategy_candidate_count:1}}}};
}}
const current = readable();
assert.strictEqual(stockPoolFormalTruth(current, '2026-08-28', '2026-08-28').ready, true);
const old = readable(); old.decision_session_date = '2026-08-27'; old.trade_date = '2026-08-27';
assert.strictEqual(stockPoolFormalTruth(old, '2026-08-28', '2026-08-28').reasonCode, 'POOL_DATE_MISMATCH');
assert.strictEqual(stockPoolFormalTruth(old, '2026-08-27', '2026-08-28').reasonCode, 'HISTORICAL_RESEARCH_ONLY');
const staleData = readable(); staleData.trade_date = '2026-08-27';
assert.strictEqual(stockPoolFormalTruth(staleData, '2026-08-28', '2026-08-28').reasonCode, 'POOL_DATA_DATE_MISMATCH');
const deferred = readable(); deferred.governance_deferred = true;
assert.strictEqual(stockPoolFormalTruth(deferred, '2026-08-28', '2026-08-28').reasonCode, 'GOVERNANCE_DATABASE_DEFERRED');
const research = readable(); research.decision_scope = 'RESEARCH_ONLY';
assert.strictEqual(stockPoolFormalTruth(research, '2026-08-28', '2026-08-28').reasonCode, 'RESEARCH_ONLY');
const unverified = readable(); unverified.decision_integrity_verified = false;
assert.strictEqual(stockPoolFormalTruth(unverified, '2026-08-28', '2026-08-28').reasonCode, 'POOL_NOT_VERIFIED_COMPLETED');
const historical = readable(); historical.historical_read_only = true;
assert.strictEqual(stockPoolFormalTruth(historical, '2026-08-28', '2026-08-28').reasonCode, 'HISTORICAL_READ_ONLY');
const asOf = readable(); asOf.is_as_of_fallback = true; asOf.requested_trade_date = '2026-08-31';
const asOfTruth = stockPoolFormalTruth(asOf, '2026-08-31', '2026-08-28');
assert.strictEqual(asOfTruth.ready, true);
assert.strictEqual(asOfTruth.reasonCode, 'VERIFIED_COMPLETED_LATEST_AS_OF_POOL');
const wrongAsOf = readable(); wrongAsOf.is_as_of_fallback = true; wrongAsOf.decision_session_date = '2026-08-27'; wrongAsOf.trade_date = '2026-08-27';
assert.strictEqual(stockPoolFormalTruth(wrongAsOf, '2026-08-31', '2026-08-28').reasonCode, 'AS_OF_POOL_DATE_INVALID');
const futureAsOf = readable(); futureAsOf.is_as_of_fallback = true;
assert.strictEqual(stockPoolFormalTruth(futureAsOf, '2026-08-27', '2026-08-28').reasonCode, 'AS_OF_POOL_DATE_INVALID');
process.stdout.write(JSON.stringify({{status:'PASS'}}));
"""
    assert _node(harness) == {"status": "PASS"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_main_page_uses_same_verified_current_pool_contract():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    start = script.index("    function candidateCenterStockPoolIsReadable(pool)")
    end = script.index("    function candidateCenterStockPoolWithHistoricalFallback(requestedDate)", start)
    functions = script[start:end]
    harness = f"""
const assert = require('assert');
{functions}
function readable() {{
  return {{run_uid:'verified',decision_session_date:'2026-08-28',trade_date:'2026-08-28',
    pool_readable:true,run_status:'COMPLETED',decision_integrity_verified:true,
    pool_status:'READY',items:[{{is_strategy_candidate:true}}],
    summary:{{stock_count:1,strategy_candidate_count:1}}}};
}}
assert.strictEqual(candidateCenterStockPoolTruth(readable(), '2026-08-28', '2026-08-28').ready, true);
const stale = readable(); stale.decision_session_date='2026-08-27'; stale.trade_date='2026-08-27';
assert.strictEqual(candidateCenterStockPoolTruth(stale, '2026-08-28', '2026-08-28').ready, false);
assert.strictEqual(candidateCenterStockPoolTruth(stale, '2026-08-27', '2026-08-28').reasonCode, 'HISTORICAL_RESEARCH_ONLY');
const staleData = readable(); staleData.trade_date='2026-08-27';
assert.strictEqual(candidateCenterStockPoolTruth(staleData, '2026-08-28', '2026-08-28').reasonCode, 'POOL_DATA_DATE_MISMATCH');
const deferred = readable(); deferred.activation_enabled=false;
assert.strictEqual(candidateCenterStockPoolTruth(deferred, '2026-08-28', '2026-08-28').reasonCode, 'GOVERNANCE_DATABASE_DEFERRED');
const research = readable(); research.actionable_output_allowed=false;
assert.strictEqual(candidateCenterStockPoolTruth(research, '2026-08-28', '2026-08-28').reasonCode, 'RESEARCH_ONLY');
const asOf = readable(); asOf.is_as_of_fallback=true; asOf.requested_trade_date='2026-08-31';
const asOfTruth = candidateCenterStockPoolTruth(asOf, '2026-08-31', '2026-08-28');
assert.strictEqual(asOfTruth.ready, true);
assert.strictEqual(asOfTruth.reasonCode, 'VERIFIED_COMPLETED_LATEST_AS_OF_POOL');
const wrongAsOf = readable(); wrongAsOf.is_as_of_fallback=true; wrongAsOf.decision_session_date='2026-08-27'; wrongAsOf.trade_date='2026-08-27';
assert.strictEqual(candidateCenterStockPoolTruth(wrongAsOf, '2026-08-31', '2026-08-28').reasonCode, 'AS_OF_POOL_DATE_INVALID');
process.stdout.write(JSON.stringify({{status:'PASS'}}));
"""
    assert _node(harness) == {"status": "PASS"}

    assert "/api/v3/stock-pool?trade_date=" in script
    assert "/api/strategy-center/candidates?trade_date=" not in script
    assert "formal_pool_current: poolTruth.ready" in script
    assert "paper_execution_ready: false" in script
    assert "execution_authority: executionAuthority" in script
    assert "actionability === 'BUY_ZONE' ? 'BUY_READY'" not in script
    assert "V3 仍为 ADVISORY_ONLY" in script
    assert "当前策略选股结果（咨询只读）" in script
    assert "策略池研究只读" in script
    assert "历史/旧推荐研究数据（不可执行）" in script
    assert "旧推荐研究观察 ' + topRecConfirm.length" in script
    assert "AI确认候选" not in script
    assert "RESEARCH_ONLY / 正式策略池不可执行" in script


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_recommendation_session_is_not_replaced_by_older_data_date():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    start = script.index("    function recommendationDateValue()")
    end = script.index("    function applyMarketClock(clock)", start)
    functions = script[start:end]
    harness = f"""
const assert = require('assert');
let MARKET_CLOCK = {{
  recommendation_trade_date: '2026-08-28',
  latest_data_date: '2026-08-27',
  ui_trade_date: '2026-08-29'
}};
function currentDateValue() {{ return '2026-08-29'; }}
{functions}
assert.strictEqual(recommendationDateValue(), '2026-08-28');
assert.strictEqual(latestFormalStrategyDateValue(), '2026-08-28');
process.stdout.write(JSON.stringify({{status:'PASS'}}));
"""
    assert _node(harness) == {"status": "PASS"}

    trading = (ROOT / "server/static/js/trading-v3.js").read_text(
        encoding="utf-8"
    )
    assert "clock.recommendation_trade_date||clock.latest_data_date" in trading
    assert "latestFormalDate>String(clock.latest_data_date)" not in trading


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_strategy_center_only_promotes_exact_verified_canonical_pool():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    start = script.index("    function strategyGovernancePoolTruth(governance, requestedDate, latestFormalDate)")
    end = script.index("    function strategyPaperExecutionPlanHtml(governance)", start)
    function = script[start:end]
    harness = f"""
const assert = require('assert');
{function}
function canonical() {{
  return {{status:'ok',result_mode:'CANONICAL_PERSISTED',is_canonical:true,input_ready:true,
    trade_date:'2026-08-28',run_uid:'a'.repeat(32),canonical_result_hash:'b'.repeat(64),
    automatic_real_order_submission:false,real_order_authority:false}};
}}
assert.strictEqual(strategyGovernancePoolTruth(canonical(), '2026-08-28', '2026-08-28').ready, true);
const stale = canonical(); stale.trade_date='2026-08-27';
assert.strictEqual(strategyGovernancePoolTruth(stale, '2026-08-28', '2026-08-28').reasonCode, 'CANONICAL_DATE_MISMATCH');
assert.strictEqual(strategyGovernancePoolTruth(stale, '2026-08-27', '2026-08-28').reasonCode, 'HISTORICAL_RESEARCH_ONLY');
const preview = canonical(); preview.is_canonical=false; preview.result_mode='PREVIEW_REALTIME';
assert.strictEqual(strategyGovernancePoolTruth(preview, '2026-08-28', '2026-08-28').reasonCode, 'CANONICAL_NOT_VERIFIED_COMPLETED');
const deferred = canonical(); deferred.strategy_governance_mode='DEFERRED_DB';
assert.strictEqual(strategyGovernancePoolTruth(deferred, '2026-08-28', '2026-08-28').reasonCode, 'GOVERNANCE_DATABASE_DEFERRED');
const forged = canonical(); forged.canonical_result_hash='short';
assert.strictEqual(strategyGovernancePoolTruth(forged, '2026-08-28', '2026-08-28').reasonCode, 'CANONICAL_IDENTITY_INVALID');
process.stdout.write(JSON.stringify({{status:'PASS'}}));
"""
    assert _node(harness) == {"status": "PASS"}

    assert 'data-formal-pool-state="' in script
    assert "data-research-only-pool" in script
    assert "if (!poolTruth.ready) rows = [];" in script
    assert "row.paper_allocation_eligible === true" in script
    assert "row.real_order_authority === false" in script
    assert "旧候选已隔离到研究只读区" in script
    assert "VERIFIED_PAPER / 仅模拟" in script


def test_strategy_pool_javascript_cache_versions_are_advanced():
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    trading = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")
    assert "style.css?v=45" in index
    assert "app.js?v=119" in index
    assert "trading-v3.js?v=36" in trading
    assert "旧日期、未验证、DEFERRED 或 RESEARCH_ONLY" in trading
