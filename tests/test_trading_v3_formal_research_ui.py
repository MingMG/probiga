"""Behavior checks for formal research evidence presentation and request ordering."""

from pathlib import Path
import shutil
import subprocess
from unittest import SkipTest


ROOT = Path(__file__).resolve().parents[1]


def _function(name, next_name):
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")
    start = script.index("  function " + name + "(")
    end = script.index("  function " + next_name + "(", start)
    return script[start:end]


def _run_node(script):
    node = shutil.which("node")
    if not node:
        raise SkipTest("Node.js is required for frontend behavior checks")
    result = subprocess.run(
        [node, "-"], input=script, capture_output=True, text=True, encoding="utf-8"
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_formal_classification_does_not_hide_failed_gates_behind_holdout():
    _run_node(
        "const assert = require('node:assert/strict');\n"
        + _function("formalRunClassification", "formalBacktestMetrics")
        + r"""
function classify(blockers, status = 'COMPLETED', gate = 'BLOCK') {
  return formalRunClassification({status, gate_status: gate, result: {
    promotion_protocol: {blockers}
  }}).code;
}
for (const failed of ['STATISTICAL_GATE_BLOCKED', 'BASELINE_COMPARISON_BLOCKED']) {
  assert.equal(classify(['FROZEN_HOLDOUT_NOT_PRISTINE', failed]), 'NOT_QUALIFIED');
  assert.equal(classify([failed, 'FROZEN_HOLDOUT_NOT_PRISTINE']), 'NOT_QUALIFIED');
}
assert.equal(classify(['FROZEN_HOLDOUT_NOT_PRISTINE']), 'INSUFFICIENT');
assert.equal(classify(['TRADE_LEVEL_LEDGER_REQUIRED']), 'INSUFFICIENT');
assert.equal(classify([], 'COMPLETED', 'PASS'), 'PASS');
assert.equal(classify(['STATISTICAL_GATE_BLOCKED'], 'FAILED'), 'FAILED');
assert.equal(classify(['BASELINE_COMPARISON_BLOCKED'], 'RUNNING'), 'RUNNING');
assert.equal(formalRunClassification({status: 'COMPLETED', result: {
  promotion_protocol: {reason: ['BASELINE_COMPARISON_BLOCKED', 'FROZEN_HOLDOUT_NOT_PRISTINE']}
}}).code, 'NOT_QUALIFIED');
"""
    )


def test_formal_paper_evidence_uses_actual_summary_scope_without_inventing_totals():
    _run_node(
        r"""
const assert = require('node:assert/strict');
const nodes = {};
const state = {errors: {}, paperLedger: {summary: {
  position_count: 0, position_lot_count: 0, today_closed_position_count: 2,
  total_unrealized_pnl: 0, legacy_realized_pnl: 9876
}}};
function el(id) { return nodes[id] || (nodes[id] = {}); }
function fact(label, value) { return label + ':' + value + '\n'; }
function pnlFact(label, value) { return fact(label, 'PNL ' + value); }
"""
        + _function("renderFormalPaperEvidence", "renderFormalResearch")
        + r"""
renderFormalPaperEvidence();
let facts = nodes.formalPaperFacts.innerHTML;
assert.match(nodes.researchPaperState.textContent, /不可归因到当前策略版本/);
assert.match(facts, /持仓股票:0 只/);
assert.match(facts, /未平仓批次:0/);
assert.match(facts, /今日已平仓股票:2 只/);
assert.match(facts, /当前浮动盈亏:PNL 0/);
assert.match(facts, /累计成交统计:不可用（接口未提供）/);
assert.match(facts, /累计已实现盈亏:不可用（接口未提供统一累计统计）/);
assert.match(facts, /暂无当前策略版本的模拟证据/);
assert.match(facts, /不参与策略竞技或前向验证/);
assert.doesNotMatch(facts, /9876|undefined/);
assert.doesNotMatch(nodes.researchPaperState.textContent, /尚无完整模拟成交/);
state.paperLedger.summary.position_lot_count = 3;
renderFormalPaperEvidence();
assert.match(nodes.formalPaperFacts.innerHTML, /未平仓批次:3/);
assert.doesNotMatch(nodes.researchPaperState.textContent, /3 笔模拟证据/);
assert.equal(nodes.formalPaperStatus.className, 'badge warning');
state.errors.paperLedger = '连接失败';
renderFormalPaperEvidence();
assert.equal(nodes.formalPaperStatus.className, 'badge danger');
assert.match(nodes.formalPaperFacts.innerHTML, /连接失败；不能解释为模拟账户表现为零/);
assert.doesNotMatch(nodes.formalPaperFacts.innerHTML, /未平仓批次/);
state.errors = {};
state.paperLedger = {};
renderFormalPaperEvidence();
assert.equal(nodes.researchPaperState.textContent, '模拟账户账本不可用');
state.paperLedger = {summary: {}};
renderFormalPaperEvidence();
assert.match(nodes.formalPaperFacts.innerHTML, /持仓股票:不可用/);
assert.match(nodes.formalPaperFacts.innerHTML, /当前浮动盈亏:不可用/);
"""
    )


def test_formal_backtest_detail_ignores_stale_success_and_error_responses():
    _run_node(
        r"""
const assert = require('node:assert/strict');
const requests = [];
const state = {activeBacktest: {backtest_uid: 'previous'}};
let renderCount = 0;
function api2(path) {
  return new Promise((resolve, reject) => requests.push({path, resolve, reject}));
}
function unwrap(payload) { return payload.data; }
function renderFormalResearch() { renderCount++; }
function notifyParentResize() {}
function errorText(err) { return err.message; }
"""
        + _function("fetchFormalBacktest", "pollFormalBacktestJob")
        + r"""
(async function () {
  let a = fetchFormalBacktest('run-A');
  assert.deepEqual(state.activeBacktest, {});
  let b = fetchFormalBacktest('run-B');
  assert.equal(requests[1].path, '/research/backtests/run-B');
  requests[1].resolve({data: {backtest_uid: 'run-B', result: 'new'}});
  await b;
  const message = state.formalBacktestMessage, rendered = renderCount;
  requests[0].resolve({data: {backtest_uid: 'run-A', result: 'old'}});
  await a;
  assert.equal(state.activeBacktest.backtest_uid, 'run-B');
  assert.equal(state.formalBacktestMessage, message);
  assert.equal(renderCount, rendered);

  a = fetchFormalBacktest('run-C');
  b = fetchFormalBacktest('run-D');
  requests[3].resolve({data: {backtest_uid: 'run-D'}});
  await b;
  const successfulMessage = state.formalBacktestMessage;
  requests[2].reject(new Error('old request failed'));
  await a;
  assert.equal(state.activeBacktest.backtest_uid, 'run-D');
  assert.equal(state.formalBacktestMessage, successfulMessage);

  a = fetchFormalBacktest('run-E');
  assert.deepEqual(state.activeBacktest, {});
  requests[4].reject(new Error('latest request failed'));
  await a;
  assert.deepEqual(state.activeBacktest, {});
  assert.match(state.formalBacktestMessage, /latest request failed/);

  a = fetchFormalBacktest('same-UID');
  b = fetchFormalBacktest('same-UID');
  requests[6].resolve({data: {backtest_uid: 'same-UID', result: 'latest'}});
  await b;
  requests[5].resolve({data: {backtest_uid: 'same-UID', result: 'stale'}});
  await a;
  assert.equal(state.activeBacktest.result, 'latest');

  a = fetchFormalBacktest('requested-UID');
  requests[7].resolve({data: {backtest_uid: 'different-UID'}});
  await a;
  assert.deepEqual(state.activeBacktest, {});
  assert.match(state.formalBacktestMessage, /运行标识与请求不一致/);
})().catch(err => { console.error(err); process.exitCode = 1; });
"""
    )


def test_formal_polling_keeps_submission_locked_until_an_explicit_terminal_state():
    _run_node(
        r"""
const assert = require('node:assert/strict');
const runButton = {disabled: false};
const state = {activeBacktestJobId: 'job-1', formalBacktestMessage: ''};
const scheduled = [];
let response;
function el(id) { assert.equal(id, 'formalBacktestRun'); return runButton; }
function setTimeout(callback, delay) { scheduled.push({callback, delay}); }
function api2() { return response; }
function unwrap(payload) { return payload; }
function renderFormalResearch() {}
function errorText(error) { return error.message; }
function loadFormalResearch() {}
function fetchFormalBacktest() {}
"""
        + _function("pollFormalBacktestJob", "runFormalBacktest")
        + r"""
async function runScheduled() {
  const item = scheduled.shift();
  item.callback();
  await Promise.resolve();
  await Promise.resolve();
  return item;
}
(async function () {
  response = Promise.resolve({status: 'RUNNING'});
  pollFormalBacktestJob('job-1', 0, 25);
  assert.equal((await runScheduled()).delay, 25);
  assert.equal(state.activeBacktestJobId, 'job-1');
  assert.equal(runButton.disabled, true);
  assert.match(state.formalBacktestMessage, /按钮保持禁用/);
  assert.equal(scheduled[0].delay, 10000);

  scheduled.length = 0;
  runButton.disabled = false;
  response = Promise.reject(new Error('temporary timeout'));
  pollFormalBacktestJob('job-1', 10, 30);
  await runScheduled();
  assert.equal(state.activeBacktestJobId, 'job-1');
  assert.equal(runButton.disabled, true);
  assert.match(state.formalBacktestMessage, /继续低频追踪/);
  assert.equal(scheduled[0].delay, 10000);

  scheduled.length = 0;
  response = Promise.resolve({status: 'FAILED', error_message: 'test failure'});
  pollFormalBacktestJob('job-1', 10, 40);
  assert.equal((await runScheduled()).delay, 40);
  assert.equal(state.activeBacktestJobId, '');
  assert.equal(runButton.disabled, false);
  assert.match(state.formalBacktestMessage, /test failure/);
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    )


def test_formal_strategy_defaults_use_latest_validated_session():
    _run_node(
        r"""
const assert = require('node:assert/strict');
const nodes = {
  formalBacktestStrategy: {value: '0'},
  formalBacktestBinding: {},
  formalBacktestStart: {value: '2020-01-01', min: ''},
  formalBacktestEnd: {value: '2099-01-01', max: ''},
  formalBacktestCapital: {value: ''}
};
const state = {strategyVersions: [{
  strategy_id: 'etf_trend_risk', version: 'etf_trend_risk_v2.0.0',
  instrument_scope: 'EXCHANGE_TRADED_FUND',
  config_hash: 'a'.repeat(64), code_commit_sha: 'b'.repeat(64),
  backtest_adapter_minimum_start_date: '2021-01-04',
  latest_validated_session: '2026-09-04',
  latest_runnable_session: '2026-09-04',
  formal_data_contract_status: 'COMPLETE',
  formal_fee_coverage_usable: true,
  earliest_fee_covered_start: '2026-07-27',
  latest_fee_covered_end: null,
  backtest_adapter_reason: '已绑定 ETF 成交级回放'
}]};
function el(id) { return nodes[id]; }
function formalStrategyLabel() { return 'ETF趋势风险'; }
"""
        + _function("formalFeeCoverageLabel", "formalRunClassification")
        + _function("selectedFormalStrategy", "renderFormalBacktestBinding")
        + _function("renderFormalBacktestBinding", "renderValidation")
        + _function("applyFormalStrategyDefaults", "fetchFormalBacktest")
        + r"""
applyFormalStrategyDefaults();
assert.equal(nodes.formalBacktestEnd.value, '2026-09-04');
assert.equal(nodes.formalBacktestEnd.max, '2026-09-04');
assert.equal(nodes.formalBacktestStart.min, '2026-07-27');
assert.equal(nodes.formalBacktestStart.value, '2026-07-27');
assert.equal(nodes.formalBacktestCapital.value, '200000');
assert.match(nodes.formalBacktestBinding.textContent, /完整数据截止 2026-09-04/);
assert.match(nodes.formalBacktestBinding.textContent, /可运行截止 2026-09-04/);
assert.match(nodes.formalBacktestBinding.textContent, /运行时须绑定已确认费率/);
"""
    )


def test_formal_backtest_requires_complete_data_and_confirmed_fee_coverage():
    _run_node(
        "const assert = require('node:assert/strict');\n"
        + _function("formalAdapterSupported", "formalRunClassification")
        + r"""
const ready = {
  backtest_adapter_supported: true,
  latest_validated_session: '2026-09-04',
  latest_runnable_session: '2026-09-04',
  formal_data_contract_status: 'COMPLETE',
  formal_fee_coverage_usable: true,
  earliest_fee_covered_start: '2026-07-27',
  latest_fee_covered_end: null
};
assert.equal(formalBacktestRunnable(ready), true);
assert.equal(formalBacktestRunnable({...ready, formal_fee_coverage_usable: false}), false);
assert.equal(formalBacktestRunnable({...ready, formal_data_contract_status: 'BLOCKED'}), false);
assert.equal(formalBacktestRunnable({...ready, latest_runnable_session: null}), false);
assert.equal(formalBacktestRunnable({
  ...ready, latest_runnable_session: '2026-07-26'
}), false);
assert.equal(formalFeeCoverageLabel(ready), '2026-07-27 至 长期有效');
"""
    )


def test_formal_backtest_initialization_does_not_use_local_today_as_end():
    source = (ROOT / "server/static/js/trading-v3.js").read_text(
        encoding="utf-8"
    )
    html = (ROOT / "server/static/trading-v3.html").read_text(
        encoding="utf-8"
    )

    assert "var formalEnd=localDateKey()" not in source
    assert "latest_runnable_session" in source
    assert "formalBacktestCost" not in source
    assert "formalBacktestCost" not in html
    assert "round_trip_cost" not in source
    assert "运行时须绑定已确认费率；系统另做 2 倍压力情景" in html


def test_formal_backtest_identity_never_falls_back_to_raw_result_binding():
    _run_node(
        "const assert = require('node:assert/strict');\n"
        + _function("formalBacktestIdentity", "formalComparisonConditions")
        + _function("formalComparisonConditions", "renderFormalBacktestComparison")
        + r"""
const rawConflict = {
  strategy_identity_status: 'UNAVAILABLE',
  strategy_id: null,
  strategy_version: 'shared-v1',
  start_date: '2025-01-01', end_date: '2025-12-31', random_seed: 7,
  protocol_version: 'p1', code_commit_sha: 'a'.repeat(64),
  data_snapshot_hash: 'b'.repeat(64),
  result: {
    strategy_binding: {strategy_id: 'forged', strategy_version: 'shared-v1'},
    adapter: 'etf_trade_level_replay_v2',
    execution_assumptions: {
      initial_capital_cny: 200000, cost_scenario_multiplier: 1,
      fee_profile: {usable: true}
    }
  }
};
assert.deepEqual(formalBacktestIdentity(rawConflict), {
  verified: false, label: '策略身份不可确认'
});
assert.ok(formalComparisonConditions(rawConflict).missing.includes('strategy_identity'));

function verified(strategyId, version) {
  return {
    ...rawConflict,
    strategy_identity_status: 'VERIFIED',
    strategy_id: strategyId,
    strategy_version: version,
    result: {...rawConflict.result, strategy_binding: {
      strategy_id: 'untrusted-other', strategy_version: 'untrusted-version'
    }}
  };
}
const alpha = verified('alpha', 'alpha-v1');
const beta = verified('beta', 'beta-v2');
assert.equal(formalBacktestIdentity(alpha).label, 'alpha @ alpha-v1');
assert.equal(formalBacktestIdentity(beta).label, 'beta @ beta-v2');
assert.equal(formalComparisonConditions(alpha).key, formalComparisonConditions(beta).key);
assert.ok(!formalComparisonConditions(alpha).missing.includes('strategy_identity'));
"""
    )


def test_formal_backtest_renderers_use_only_verified_row_identity():
    source = (ROOT / "server/static/js/trading-v3.js").read_text(
        encoding="utf-8"
    )

    assert "binding.strategy_id||row.strategy_id" not in source
    assert "leftBinding.strategy_id||left.strategy_id" not in source
    assert "rightBinding.strategy_id||right.strategy_id" not in source
    assert "formalBacktestIdentity(row).verified" in source
    assert "identity.verified&&(classification.code" in source
    assert "<td>'+esc(identity.label)+'</td>" in source


def test_formal_backtest_detail_prefers_persisted_config_and_code_evidence():
    _run_node(
        r"""
const assert = require('node:assert/strict');
const nodes = {formalBacktestDetail: {innerHTML: ''}};
function el(id) { return nodes[id]; }
function esc(value) { return String(value); }
function fact(label, value) { return label + ':' + value + '\n'; }
function money(value) { return String(value ?? '—'); }
function pct(value) { return String(value ?? '—'); }
function formalRunClassification() { return {kind: 'safe', label: '完成'}; }
function formalBacktestMetrics() { return {}; }
const state = {activeBacktest: {}};
"""
        + _function("formalBacktestIdentity", "formalComparisonConditions")
        + _function("renderFormalBacktestDetail", "renderFormalPaperEvidence")
        + r"""
const row = {
  backtest_uid: 'run-1',
  strategy_identity_status: 'VERIFIED',
  strategy_id: 'verified-strategy',
  strategy_version: 'verified-v1',
  config_hash: 'persisted-config-hash',
  code_commit_sha: 'persisted-code-sha',
  result: {
    strategy_binding: {
      strategy_id: 'raw-strategy',
      config_hash: 'raw-config-hash',
      code_commit_sha: 'raw-code-sha'
    },
    execution_assumptions: {},
    promotion_protocol: {}
  }
};
renderFormalBacktestDetail(row);
let detail = nodes.formalBacktestDetail.innerHTML;
assert.match(detail, /配置 \/ 代码:persisted-config /);
assert.match(detail, /persisted-code-s/);
assert.doesNotMatch(detail, /raw-config-hash|raw-code-sha|raw-strategy/);
assert.match(detail, /费用口径:费率证据缺失，结果阻断/);
row.result.execution_assumptions = {
  cost_scenario_multiplier: 1,
  fee_profile: {usable: true}
};
renderFormalBacktestDetail(row);
detail = nodes.formalBacktestDetail.innerHTML;
assert.match(detail, /费用口径:已确认费率基准 \+ 系统 2 倍压力情景/);
"""
    )
