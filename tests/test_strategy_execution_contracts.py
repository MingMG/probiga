from __future__ import annotations

import copy
import hashlib
from contextlib import nullcontext

import pytest

from server.engine import strategy_center as center
from server.engine import strategy_governance as governance
from server.engine.strategy_execution_adapters import (
    StrategyExecutionAdapter,
    compute_strategy_execution_adapter_artifact_sha256,
    create_candidate_batch,
    execute_dynamic_adapter_candidate_batch,
    normalize_execution_binding,
    persist_strategy_adapter_run_receipt,
    register_strategy_execution_adapter,
    strategy_execution_adapter_status,
    unregister_strategy_execution_adapter,
    validate_strategy_adapter_run_receipt,
    verify_dynamic_shadow_ledger_chain,
    verify_persisted_strategy_adapter_run_receipt,
)


def trusted_dynamic_candidate_builder(strategy, context):
    deployed = strategy["evaluator_config"]["execution_adapter"]
    return create_candidate_batch(strategy, context, [{
        "stock_code": "600036",
        "strategy_key": strategy["strategy_key"],
        "strategy_version": strategy["current_version"],
        "strategy_version_hash": strategy["version_hash"],
        "execution_binding_hash": deployed["execution_binding_hash"],
        "adapter_artifact_sha256": deployed["artifact_sha256"],
        "cost_model_hash": deployed["cost_model_hash"],
        "signal_direction": "BUY", "signal_status": "READY",
        "gate_status": "PASS", "effective_weight": 1.0,
        "model_confidence": 88.0, "effective_score": 88.0,
        "risk_level": "LOW", "industry_name": "银行",
    }], run_uid="1" * 32, completed_at="2026-08-21T15:00:00+08:00")


def trusted_none_candidate_builder(_strategy, _context):
    return None


def trusted_random_receipt_candidate_builder(strategy, context):
    deployed = strategy["evaluator_config"]["execution_adapter"]
    return create_candidate_batch(strategy, context, [{
        "stock_code": "600036", "strategy_key": strategy["strategy_key"],
        "strategy_version": strategy["current_version"],
        "strategy_version_hash": strategy["version_hash"],
        "execution_binding_hash": deployed["execution_binding_hash"],
        "adapter_artifact_sha256": deployed["artifact_sha256"],
        "cost_model_hash": deployed["cost_model_hash"],
    }])


def trusted_zero_candidate_builder(strategy, context):
    return create_candidate_batch(strategy, context, [])


def trusted_audit_field_candidate_builder(strategy, context):
    deployed = strategy["evaluator_config"]["execution_adapter"]
    return create_candidate_batch(strategy, context, [{
        "stock_code": "600036", "strategy_key": strategy["strategy_key"],
        "strategy_version": strategy["current_version"],
        "strategy_version_hash": strategy["version_hash"],
        "execution_binding_hash": deployed["execution_binding_hash"],
        "adapter_artifact_sha256": deployed["artifact_sha256"],
        "cost_model_hash": deployed["cost_model_hash"],
        "evidence": {"run_uid": "1" * 32},
    }])


def trusted_wrong_identity_candidate_builder(strategy, context):
    deployed = strategy["evaluator_config"]["execution_adapter"]
    return create_candidate_batch(strategy, context, [{
        "stock_code": "600036", "strategy_key": "wrong_strategy",
        "strategy_version": strategy["current_version"],
        "strategy_version_hash": strategy["version_hash"],
        "execution_binding_hash": deployed["execution_binding_hash"],
        "adapter_artifact_sha256": deployed["artifact_sha256"],
        "cost_model_hash": deployed["cost_model_hash"],
    }])


_MUTABLE_ADAPTER_GLOBAL = []


def adapter_with_mutable_global(_strategy, _context):
    return len(_MUTABLE_ADAPTER_GLOBAL)


def adapter_with_nested_mutable_global(_strategy, _context):
    return [len(_MUTABLE_ADAPTER_GLOBAL) for _item in range(1)]


def _adapter_helper_one(_strategy, _context):
    return 1


def _adapter_helper_two(_strategy, _context):
    return 2


_REBINDABLE_ADAPTER_HELPER = _adapter_helper_one

_TRANSITIVE_MUTABLE_HELPER_STATE = []


def _adapter_helper_with_mutable_global(_strategy, _context):
    return len(_TRANSITIVE_MUTABLE_HELPER_STATE)


def adapter_with_transitive_mutable_helper(strategy, context):
    return _adapter_helper_with_mutable_global(strategy, context)


def adapter_with_rebindable_helper(strategy, context):
    return _REBINDABLE_ADAPTER_HELPER(strategy, context)


def mutating_strategy_validator(strategy):
    strategy["source_kind"] = "immutable_manifest"
    return True, "伪造内置来源"


def _candidate_source(trade_date: str, candidate_count: int = 0) -> dict:
    payload = {
        "schema": "probiga.strategy-candidate-source.v1",
        "source": "test_source",
        "status": "COMPLETED",
        "query_completed": True,
        "trade_date": trade_date,
        "data_date": trade_date,
        "source_row_count": candidate_count,
        "loaded_row_count": candidate_count,
        "loaded_rows_hash": governance._digest([]),
        "candidate_count": candidate_count,
        "candidate_identity": [],
        "reason": "测试候选源已完成",
    }
    return {**payload, "source_hash": governance._digest(payload)}


def _snapshot(state: str, status: str, *, candidates=None) -> dict:
    rows = list(candidates or [])
    source = _candidate_source("2026-08-21", len(rows))
    source["candidate_identity"] = sorted(
        str(row.get("stock_code") or "") for row in rows
    )
    payload = {key: value for key, value in source.items() if key != "source_hash"}
    source["source_hash"] = governance._digest(payload)
    return {
        "source_status": "fresh",
        "is_stale": False,
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "candidate_source": source,
        "market_state": {"key": state},
        "global_gate": {"status": status, "reason": "测试门禁"},
        "candidates": rows,
    }


def _allocation_contract() -> list[dict]:
    return [{
        "target_type": "STRATEGY",
        "target_key": "dynamic_alpha",
        "target_version": "v1",
        "target_name": "动态Alpha",
        "funding_gate_hash": "f" * 64,
        "ranking_score": 90.0,
        "enabled": True,
        "profit_gate_passed": True,
        "paper_allocation_eligible": True,
        "market_state": "",
        "market_match_score": 100.0,
        "market_route_eligible": True,
        "router_decision_hash": "r" * 64,
        "exposure_keys": ["dynamic_alpha"],
        "lifecycle_status": "ACTIVE",
        "lifecycle_status_label": "正常运行",
        "lifecycle_risk_multiplier": 1.0,
    }]


def test_reduce_new_buy_keeps_nonzero_allocation_inside_50_and_20_caps():
    for state, expected_cap in (("high_range", 50.0), ("risk_declining", 20.0)):
        snapshot = _snapshot(state, "REDUCE_NEW_BUY")
        gate = governance._snapshot_trading_gate(snapshot)
        assert gate["trading_allowed"] is True
        assert gate["market_risk_cap_pct"] == expected_cap
        allocations = governance._allocation(
            [], [], state,
            trading_allowed=True,
            candidate_contract=_allocation_contract(),
            trading_gate=gate,
        )
        funded = sum(
            row["simulated_weight_pct"]
            for row in allocations if row["target_type"] != "CASH"
        )
        assert 0 < funded <= expected_cap
        assert all(row["market_gate_status"] == "REDUCE_NEW_BUY" for row in allocations)
        assert all(row["market_risk_cap_pct"] == expected_cap for row in allocations)


@pytest.mark.parametrize(
    "status", ["BLOCK_NEW_BUY", "DATA_NOT_READY", "REVIEW_REQUIRED", ""],
)
def test_non_funding_market_gates_remain_fail_closed(status):
    gate = governance._snapshot_trading_gate(
        _snapshot("trend_bullish", status)
    )
    assert gate["trading_allowed"] is False


def test_candidate_source_completion_proof_is_mandatory_and_hash_bound():
    snapshot = _snapshot("trend_bullish", "ALLOW_NEW_BUY")
    assert governance.governance_input_ready(snapshot)[0] is True

    missing = dict(snapshot)
    missing.pop("candidate_source")
    ready, reason = governance.governance_input_ready(missing)
    assert ready is False
    assert "完成证明" in reason

    tampered = dict(snapshot)
    tampered["candidate_source"] = {
        **snapshot["candidate_source"], "candidate_count": 99,
    }
    ready, reason = governance.governance_input_ready(tampered)
    assert ready is False
    assert "哈希无效" in reason


@pytest.mark.parametrize("lifecycle_status", ["ACTIVE", "REDUCE"])
def test_enabled_research_ready_runtime_requires_receipt_for_funded_lifecycle(
    lifecycle_status,
):
    snapshot = _snapshot("trend_bullish", "ALLOW_NEW_BUY")
    snapshot["dynamic_adapter_statuses"] = [{
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "adapter_capability_status": "RESEARCH_READY",
        "enabled": True,
        "lifecycle_status": lifecycle_status,
    }]
    source = {
        **snapshot["candidate_source"],
        "schema": "probiga.strategy-candidate-source.v2",
        "dynamic_adapter_receipts": [],
        "dynamic_adapter_results": [],
        "dynamic_adapter_results_hash": governance._digest([]),
    }
    source_payload = {
        key: value for key, value in source.items()
        if key not in {"source_hash", "dynamic_adapter_receipts"}
    }
    source["source_hash"] = governance._digest(source_payload)
    snapshot["candidate_source"] = source

    ready, reason = governance.governance_input_ready(snapshot)

    assert ready is False
    assert "运行回执" in reason


def test_dynamic_strategy_without_exact_deployed_adapter_stays_shadow():
    strategy = {
        "strategy_key": "dynamic_alpha",
        "strategy_name": "动态Alpha",
        "current_version": "v1",
        "current_status": "SHADOW",
        "enabled": True,
        "source_kind": "runtime_registry",
        "evaluator_type": "dynamic_score",
        "evaluator_config": {},
        "version_integrity_valid": True,
    }
    status = strategy_execution_adapter_status(strategy)
    assert status["executable"] is False
    assert "执行适配器未部署/无效" in status["reason"]
    strategy.update({
        "execution_adapter_executable": False,
        "execution_adapter_reason": status["reason"],
        "market_route": {"eligible": True, "router_decision_hash": "a" * 64},
    })
    windows = {
        window: {
            "health_score": 99.0,
            "profit_gate": {"passed": True, "reason": "通过", "failed_checks": []},
            "evidence_hash": str(window) * 32,
            "evidence_revision_at": "2026-08-21T15:00:00",
        }
        for window in governance.WINDOWS
    }
    ranked = governance._strategy_rankings(
        [strategy], {"dynamic_alpha": windows},
    )[0]
    assert ranked["profit_gate_passed"] is False
    assert ranked["paper_allocation_eligible"] is False
    recommended, reason = governance._recommend_strategy_row(ranked)
    assert recommended == "SHADOW"
    assert "执行适配器未部署/无效" in reason


def test_dynamic_adapter_registry_is_version_artifact_and_cost_bound():
    artifact_sha256 = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.alpha",
        adapter_version="1.0.0",
        evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_dynamic_candidate_builder,
    )
    binding = normalize_execution_binding({
        "adapter_key": "dynamic.alpha",
        "adapter_version": "1.0.0",
        "strategy_version": "v1",
        "artifact_sha256": artifact_sha256,
        "cost_model": {
            "model_key": "cn.paper.v1",
            "currency": "CNY",
            "commission_pct": 0.03,
            "stamp_tax_pct": 0.05,
            "slippage_pct": 0.10,
            "transfer_fee_pct": 0.001,
        },
    }, strategy_version="v1")
    adapter = StrategyExecutionAdapter(
        adapter_key="dynamic.alpha",
        adapter_version="1.0.0",
        artifact_sha256=artifact_sha256,
        evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_dynamic_candidate_builder,
    )
    register_strategy_execution_adapter(adapter)
    try:
        strategy = {
            "strategy_key": "dynamic_alpha",
            "strategy_name": "动态Alpha",
            "current_version": "v1",
            "version_hash": "b" * 64,
            "source_kind": "runtime_registry",
            "evaluator_type": "dynamic_score",
            "evaluator_config": {"execution_adapter": binding},
            "version_integrity_valid": True,
            "enabled": True,
            "current_status": "SHADOW",
        }
        status = strategy_execution_adapter_status(strategy)
        assert status["executable"] is True
        assert status["status"] == "RESEARCH_READY"
        assert status["funding_pipeline_ready"] is False
        assert len(status["execution_binding_hash"]) == 64
        execution = execute_dynamic_adapter_candidate_batch(
            strategy, {"trade_date": "2026-08-21"}
        )
        signals = execution["signals"]
        assert signals[0]["strategy_key"] == "dynamic_alpha"
        assert signals[0]["adapter_artifact_sha256"] == artifact_sha256
        assert signals[0]["cost_model_hash"] == binding["cost_model_hash"]
        assert len(signals[0]["candidate_receipt_hash"]) == 64
        common = {
            "strategy_key": "dynamic_alpha",
            "strategy_version": "v1",
            "strategy_version_hash": "b" * 64,
            "execution_binding_hash": binding["execution_binding_hash"],
            "adapter_artifact_sha256": artifact_sha256,
            "cost_model_hash": binding["cost_model_hash"],
            "candidate_receipt_hash": execution["receipt"]["receipt_hash"],
            "real_order_authority": False,
        }

        def record(layer, schema, parent_field, parent_hash, **extra):
            payload = {
                "schema": schema, "layer": layer, **common,
                parent_field: parent_hash, **extra,
            }
            return {**payload, "record_hash": governance._digest(payload)}

        intent = record(
            "PAPER_INTENT", "probiga.dynamic-shadow-paper-intent.v1",
            "candidate_receipt_hash", execution["receipt"]["receipt_hash"],
            action="BUY",
        )
        order = record(
            "PAPER_ORDER", "probiga.dynamic-shadow-paper-order.v1",
            "intent_hash", intent["record_hash"], side="BUY",
        )
        fill = record(
            "PAPER_FILL", "probiga.dynamic-shadow-paper-fill.v1",
            "order_hash", order["record_hash"], side="BUY",
        )
        evidence = record(
            "FORWARD_EVIDENCE",
            "probiga.dynamic-shadow-forward-evidence.v1",
            "fill_hash", fill["record_hash"],
            evidence_kind="EXECUTED_PAPER",
        )
        with pytest.raises(RuntimeError, match="资金链尚未部署"):
            verify_dynamic_shadow_ledger_chain(
                strategy,
                candidate_receipt=execution["receipt"],
                intent=intent, order=order, fill=fill,
                forward_evidence=evidence,
            )
    finally:
        unregister_strategy_execution_adapter(
            "dynamic.alpha", "1.0.0", explicit_test_mode=True,
        )


def test_adapter_artifact_cannot_be_self_reported_and_none_batch_fails():
    with pytest.raises(ValueError, match="可复算指纹"):
        StrategyExecutionAdapter(
            adapter_key="dynamic.forged",
            adapter_version="1.0.0",
            artifact_sha256="a" * 64,
            evaluator_types=frozenset({"dynamic_score"}),
            candidate_builder=trusted_none_candidate_builder,
        )


@pytest.mark.parametrize(
    ("mode", "error_text"),
    (("none", "必须返回CandidateBatch"), ("identity", "strategy_key")),
)
def test_candidate_batch_none_and_identity_mismatch_fail_closed(mode, error_text):
    candidate_builder = (
        trusted_none_candidate_builder
        if mode == "none" else trusted_wrong_identity_candidate_builder
    )

    adapter_key = f"dynamic.{mode}"
    artifact = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key=adapter_key,
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=candidate_builder,
    )
    binding = normalize_execution_binding({
        "adapter_key": adapter_key,
        "adapter_version": "1.0.0",
        "strategy_version": "v1",
        "artifact_sha256": artifact,
        "cost_model": {
            "model_key": "cn.paper.v1", "currency": "CNY",
            "commission_pct": 0.03, "stamp_tax_pct": 0.05,
            "slippage_pct": 0.1, "transfer_fee_pct": 0.001,
        },
    }, strategy_version="v1")
    adapter = StrategyExecutionAdapter(
        adapter_key=adapter_key,
        adapter_version="1.0.0",
        artifact_sha256=artifact,
        evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=candidate_builder,
    )
    register_strategy_execution_adapter(adapter)
    strategy = {
        "strategy_key": "dynamic_alpha", "strategy_name": "动态Alpha",
        "current_version": "v1", "version_hash": "b" * 64,
        "source_kind": "runtime_registry",
        "evaluator_type": "dynamic_score",
        "evaluator_config": {"execution_adapter": binding},
        "version_integrity_valid": True,
        "enabled": True, "current_status": "SHADOW",
    }
    try:
        with pytest.raises(ValueError, match=error_text):
            execute_dynamic_adapter_candidate_batch(
                strategy, {"trade_date": "2026-08-21"}
            )
    finally:
        unregister_strategy_execution_adapter(
            adapter_key, "1.0.0", explicit_test_mode=True,
        )


def test_unregister_requires_explicit_test_mode():
    with pytest.raises(RuntimeError, match="显式测试模式"):
        unregister_strategy_execution_adapter("dynamic.missing", "1.0.0")


def test_candidate_stable_result_ignores_random_audit_identity():
    artifact = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.stable", adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=trusted_random_receipt_candidate_builder,
    )
    binding = normalize_execution_binding({
        "adapter_key": "dynamic.stable", "adapter_version": "1.0.0",
        "strategy_version": "v1", "artifact_sha256": artifact,
        "cost_model": {
            "model_key": "cn.paper.v1", "currency": "CNY",
            "commission_pct": 0.03, "stamp_tax_pct": 0.05,
            "slippage_pct": 0.1, "transfer_fee_pct": 0.001,
        },
    }, strategy_version="v1")
    adapter = StrategyExecutionAdapter(
        adapter_key="dynamic.stable", adapter_version="1.0.0",
        artifact_sha256=artifact, evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_random_receipt_candidate_builder,
    )
    register_strategy_execution_adapter(adapter)
    strategy = {
        "strategy_key": "dynamic_alpha", "strategy_name": "动态Alpha",
        "current_version": "v1", "version_hash": "b" * 64,
        "source_kind": "runtime_registry", "evaluator_type": "dynamic_score",
        "evaluator_config": {"execution_adapter": binding},
        "version_integrity_valid": True, "enabled": True,
        "current_status": "SHADOW",
    }
    try:
        first = execute_dynamic_adapter_candidate_batch(
            strategy, {
                "trade_date": "2026-08-21",
                "market": {
                    "risk_score": 20,
                    "generated_at": "2026-08-21 15:00:00",
                    "cache_status": "fresh_compute",
                    "nested": {"run_uid": "1" * 32, "fact": "same"},
                },
            },
        )["receipt"]
        second = execute_dynamic_adapter_candidate_batch(
            strategy, {
                "trade_date": "2026-08-21",
                "market": {
                    "risk_score": 20,
                    "generated_at": "2026-08-21 15:01:00",
                    "cache_status": "hit",
                    "nested": {
                        "run_uid": "2" * 32,
                        "completed_at": "2026-08-21T15:01:00+08:00",
                        "receipt_hash": "a" * 64,
                        "fact": "same",
                    },
                },
            },
        )["receipt"]
        assert first["run_uid"] != second["run_uid"]
        assert first["receipt_hash"] != second["receipt_hash"]
        assert first["input_hash"] == second["input_hash"]
        assert first["output_hash"] == second["output_hash"]
        assert first["stable_result_hash"] == second["stable_result_hash"]
        changed_fact = execute_dynamic_adapter_candidate_batch(
            strategy, {
                "trade_date": "2026-08-21",
                "market": {"risk_score": 21, "nested": {"fact": "same"}},
            },
        )["receipt"]
        assert changed_fact["input_hash"] != first["input_hash"]
        assert changed_fact["stable_result_hash"] != first["stable_result_hash"]

        class Result:
            rowcount = 1

        class Connection:
            def __init__(self):
                self.params = None

            def execute(self, statement, params):
                assert "INSERT INTO st_strategy_adapter_run_receipt" in str(statement)
                self.params = params
                return Result()

        connection = Connection()
        persisted = persist_strategy_adapter_run_receipt(connection, first)
        assert persisted == validate_strategy_adapter_run_receipt(first)
        assert connection.params["candidate_count"] == 1
        assert verify_persisted_strategy_adapter_run_receipt(
            first, connection.params,
        )["receipt_hash"] == first["receipt_hash"]
        with pytest.raises(ValueError, match="adapter_version漂移"):
            verify_persisted_strategy_adapter_run_receipt(
                first, {**connection.params, "adapter_version": "2.0.0"},
            )

        future = {**first, "completed_at": "2999-01-01T00:00:00+00:00"}
        future_payload = {
            key: value for key, value in future.items() if key != "receipt_hash"
        }
        future["receipt_hash"] = governance._digest(future_payload)
        with pytest.raises(ValueError, match="时间越界"):
            validate_strategy_adapter_run_receipt(future)
    finally:
        unregister_strategy_execution_adapter(
            "dynamic.stable", "1.0.0", explicit_test_mode=True,
        )


def test_zero_candidate_completed_run_still_has_valid_receipt():
    artifact = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.zero", adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=trusted_zero_candidate_builder,
    )
    binding = normalize_execution_binding({
        "adapter_key": "dynamic.zero", "adapter_version": "1.0.0",
        "strategy_version": "v1", "artifact_sha256": artifact,
        "cost_model": {
            "model_key": "cn.paper.v1", "currency": "CNY",
            "commission_pct": 0.03, "stamp_tax_pct": 0.05,
            "slippage_pct": 0.1, "transfer_fee_pct": 0.001,
        },
    }, strategy_version="v1")
    adapter = StrategyExecutionAdapter(
        adapter_key="dynamic.zero", adapter_version="1.0.0",
        artifact_sha256=artifact, evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_zero_candidate_builder,
    )
    register_strategy_execution_adapter(adapter)
    try:
        execution = execute_dynamic_adapter_candidate_batch({
            "strategy_key": "dynamic_zero", "strategy_name": "零候选",
            "current_version": "v1", "version_hash": "b" * 64,
            "source_kind": "runtime_registry",
            "evaluator_type": "dynamic_score",
            "evaluator_config": {"execution_adapter": binding},
            "version_integrity_valid": True, "enabled": True,
            "current_status": "SHADOW",
        }, {"trade_date": "2026-08-21"})
        assert execution["signals"] == []
        assert execution["receipt"]["candidate_count"] == 0
        assert execution["receipt"]["candidate_identity"] == []
        validate_strategy_adapter_run_receipt(execution["receipt"])
    finally:
        unregister_strategy_execution_adapter(
            "dynamic.zero", "1.0.0", explicit_test_mode=True,
        )


def test_candidate_output_rejects_nested_attempt_audit_fields():
    artifact = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.audit_field", adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=trusted_audit_field_candidate_builder,
    )
    binding = normalize_execution_binding({
        "adapter_key": "dynamic.audit_field", "adapter_version": "1.0.0",
        "strategy_version": "v1", "artifact_sha256": artifact,
        "cost_model": {
            "model_key": "cn.paper.v1", "currency": "CNY",
            "commission_pct": 0.03, "stamp_tax_pct": 0.05,
            "slippage_pct": 0.1, "transfer_fee_pct": 0.001,
        },
    }, strategy_version="v1")
    adapter = StrategyExecutionAdapter(
        adapter_key="dynamic.audit_field", adapter_version="1.0.0",
        artifact_sha256=artifact, evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_audit_field_candidate_builder,
    )
    register_strategy_execution_adapter(adapter)
    try:
        with pytest.raises(ValueError, match="候选稳定输出包含审计字段"):
            execute_dynamic_adapter_candidate_batch({
                "strategy_key": "dynamic_audit", "strategy_name": "审计字段",
                "current_version": "v1", "version_hash": "b" * 64,
                "source_kind": "runtime_registry", "evaluator_type": "dynamic_score",
                "evaluator_config": {"execution_adapter": binding},
                "version_integrity_valid": True, "enabled": True,
                "current_status": "SHADOW",
            }, {"trade_date": "2026-08-21"})
    finally:
        unregister_strategy_execution_adapter(
            "dynamic.audit_field", "1.0.0", explicit_test_mode=True,
        )


def test_governance_stable_input_excludes_dynamic_attempt_audit_fields():
    base = {
        "trade_date": "2026-08-21", "data_date": "2026-08-21",
        "source_status": "fresh", "is_stale": False,
        "data_sources": [], "configuration": {}, "market_state": {},
        "global_gate": {}, "strategies": [], "conflicts": [],
        "candidate_source": {
            "source_hash": "a" * 64,
            "dynamic_adapter_results": [{"stable_result_hash": "b" * 64}],
            "dynamic_adapter_receipts": [{"run_uid": "1" * 32}],
        },
        "dynamic_adapter_statuses": [{
            "strategy_key": "alpha", "candidate_run_uid": "1" * 32,
            "candidate_receipt_hash": "c" * 64,
            "candidate_completed_at": "2026-08-21T15:00:00+08:00",
            "candidate_stable_result_hash": "b" * 64,
        }],
        "candidates": [{
            "stock_code": "600036", "candidate_run_uid": "1" * 32,
            "candidate_receipt_hash": "c" * 64,
            "candidate_stable_result_hash": "b" * 64,
        }],
    }
    replay = copy.deepcopy(base)
    replay["candidate_source"]["dynamic_adapter_receipts"] = [{
        "run_uid": "2" * 32,
    }]
    replay["dynamic_adapter_statuses"][0].update({
        "candidate_run_uid": "2" * 32,
        "candidate_receipt_hash": "d" * 64,
        "candidate_completed_at": "2026-08-21T15:01:00+08:00",
    })
    replay["candidates"][0].update({
        "candidate_run_uid": "2" * 32,
        "candidate_receipt_hash": "d" * 64,
    })
    assert governance._governance_source_input_hash(base) == (
        governance._governance_source_input_hash(replay)
    )


def test_disabled_retired_and_suspended_dynamic_adapters_do_not_block_quorum():
    blocked = [
        {
            "strategy_key": f"blocked_{index}",
            "strategy_version": "v1",
            "enabled": enabled,
            "lifecycle_status": lifecycle,
            "adapter_capability_status": "RESEARCH_READY",
            "run_receipt_valid": False,
        }
        for index, (enabled, lifecycle) in enumerate((
            (False, "ACTIVE"), (True, "RETIRED"), (True, "SUSPENDED"),
        ))
    ]
    completed = center._candidate_source_contract(
        "2026-08-21", [], [],
        reference_pool={"_path": "frozen/pool.json"},
        dynamic_adapter_statuses=blocked,
    )
    assert completed["status"] == "COMPLETED"
    assert completed["query_completed"] is True

    eligible_missing_receipt = {
        **blocked[0],
        "strategy_key": "eligible",
        "enabled": True,
        "lifecycle_status": "SHADOW",
    }
    incomplete = center._candidate_source_contract(
        "2026-08-21", [], [],
        reference_pool={"_path": "frozen/pool.json"},
        dynamic_adapter_statuses=[eligible_missing_receipt],
    )
    assert incomplete["status"] == "INCOMPLETE"
    assert incomplete["query_completed"] is False


def test_candidate_source_rejects_partial_read_even_when_rows_are_nonempty(
    monkeypatch,
):
    monkeypatch.setattr(center, "_table_exists", lambda _name: True)
    monkeypatch.setattr(
        center, "_table_columns", lambda _name: {"stock_code", "pick_date"},
    )
    monkeypatch.setattr(
        center, "_db_read", lambda *_args, **_kwargs: [{"cnt": 1000}],
    )
    rows = [
        {"stock_code": str(index).zfill(6), "pick_date": "2026-08-21"}
        for index in range(500)
    ]
    incomplete = center._candidate_source_contract(
        "2026-08-21", rows, [],
    )
    assert incomplete["status"] == "INCOMPLETE"
    assert incomplete["query_completed"] is False
    assert incomplete["source_row_count"] == 1000
    assert incomplete["loaded_row_count"] == 500
    assert len(incomplete["loaded_rows_hash"]) == 64


def test_governance_rejects_forged_completed_partial_candidate_source():
    snapshot = _snapshot("strong_trend", "ALLOW_NEW_BUY")
    source = dict(snapshot["candidate_source"])
    source.update({"source_row_count": 1000, "loaded_row_count": 500})
    payload = {key: value for key, value in source.items() if key != "source_hash"}
    source["source_hash"] = governance._digest(payload)
    snapshot["candidate_source"] = source
    ready, reason = governance.governance_input_ready(snapshot)
    assert ready is False
    assert "行数" in reason


def test_canonical_governance_loader_returns_only_hash_verified_full_result(
    monkeypatch,
):
    trade_date = "2026-08-21"
    market_state = "extreme_event"
    pools = {"observation": [], "confirmation": [], "tradable": []}
    pool_snapshot, pool_hash, _row_hashes = governance._pool_snapshot_contract(
        trade_date, pools,
    )
    candidates = []
    candidate_hash = governance._digest({
        "schema": "probiga.strategy-allocation-candidate-set.v1",
        "allocation_policy_version": governance.ALLOCATION_POLICY_VERSION,
        "trade_date": trade_date,
        "market_state": market_state,
        "candidates": candidates,
    })
    allocations = [{
        "target_type": "CASH", "target_key": "cash", "target_version": "",
        "funding_gate_hash": "", "market_state": market_state,
        "market_match_score": 0.0, "router_decision_hash": "",
        "name": "现金", "simulated_weight_pct": 100.0,
        "market_gate_status": "BLOCK_NEW_BUY", "market_risk_cap_pct": 0.0,
        "reason": "测试保持现金", "real_order_authority": False,
    }]
    allocation_rows = governance._allocation_snapshot_contract(allocations)
    allocation_hash = governance._digest({
        "schema": "probiga.strategy-allocation-snapshot.v1",
        "allocation_policy_version": governance.ALLOCATION_POLICY_VERSION,
        "trade_date": trade_date, "market_state": market_state,
        "market_risk_cap_pct": 0.0, "trading_gate_passed": False,
        "candidate_set_hash": candidate_hash, "allocations": allocation_rows,
    })
    router_snapshot = {
        "schema": "probiga.strategy-market-router-snapshot.v1",
        "policy_version": governance.MARKET_ROUTER_POLICY_VERSION,
        "trade_date": trade_date, "market_state": market_state,
        "market_state_config_hash": "", "strategy_routes": {},
        "combination_routes": {},
    }
    router_hash = governance._digest(router_snapshot)
    transition_plan = {
        "schema": governance.AUTOMATIC_TRANSITION_PLAN_SCHEMA,
        "trade_date": trade_date, "transition_count": 0, "transitions": [],
    }
    transition_hash = governance._digest(transition_plan)
    input_hash = "a" * 64
    build_commit_sha = "test-build"
    decision_hash = governance._digest({
        "schema": "strategy-governance-decision.v6",
        "trade_date": trade_date, "build_commit_sha": build_commit_sha,
        "input_hash": input_hash, "router_snapshot_hash": router_hash,
        "allocation_policy_version": governance.ALLOCATION_POLICY_VERSION,
        "trading_gate_passed": False, "market_risk_cap_pct": 0.0,
        "allocation_candidate_count": 0, "eligible_candidate_count": 0,
        "candidate_set_hash": candidate_hash,
        "allocation_snapshot_hash": allocation_hash,
        "pool_snapshot_hash": pool_hash, "strategies": [], "combinations": [],
    })
    result = {
        "status": "ok", "run_uid": "1" * 32,
        "trade_date": trade_date, "input_hash": input_hash,
        "decision_hash": decision_hash, "result_mode": "CANONICAL_PERSISTED",
        "is_canonical": True, "strategies": [], "combinations": [],
        "input_ready": True, "status_labels": governance.LIFECYCLE_LABELS,
        "allocation_policy_version": governance.ALLOCATION_POLICY_VERSION,
        "router_policy_version": governance.MARKET_ROUTER_POLICY_VERSION,
        "market_state": {"key": market_state}, "build_commit_sha": build_commit_sha,
        "trading_gate_passed": False,
        "allocation_candidate_set": candidates, "candidate_set_hash": candidate_hash,
        "allocation_snapshot_hash": allocation_hash,
        "router_snapshot": router_snapshot, "router_snapshot_hash": router_hash,
        "automatic_transition_plan": transition_plan,
        "automatic_transition_plan_hash": transition_hash,
        "pool_snapshot": pool_snapshot, "pool_snapshot_hash": pool_hash,
        "summary": {
            "candidate_set_hash": candidate_hash,
            "allocation_snapshot_hash": allocation_hash,
            "pool_snapshot_hash": pool_hash,
            "automatic_transition_plan_hash": transition_hash,
            "allocation_candidate_count": 0, "eligible_candidate_count": 0,
            "market_risk_cap_pct": 0.0,
        },
        "pools": pools, "allocations": allocations,
        "automatic_real_order_submission": False,
    }
    raw = governance._json_text(result)
    row = {
        "run_uid": result["run_uid"], "trade_date": result["trade_date"],
        "input_hash": result["input_hash"],
        "decision_hash": result["decision_hash"], "result_json": raw,
        "result_hash": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    }
    monkeypatch.setattr(governance, "_table_exists", lambda _name: True)
    monkeypatch.setattr(governance, "_db_read", lambda *_args, **_kwargs: [row])
    loaded = governance.load_canonical_governance_snapshot("2026-08-21")
    assert loaded["result_mode"] == "CANONICAL_PERSISTED"
    assert loaded["is_canonical"] is True
    assert loaded["canonical_result_hash"] == row["result_hash"]

    monkeypatch.setattr(
        governance, "_db_read",
        lambda *_args, **_kwargs: [{**row, "result_hash": "f" * 64}],
    )
    with pytest.raises(RuntimeError, match="完整结果哈希无效"):
        governance.load_canonical_governance_snapshot("2026-08-21")

    duplicate = {**row, "run_uid": "2" * 32}
    monkeypatch.setattr(
        governance, "_db_read", lambda *_args, **_kwargs: [row, duplicate],
    )
    with pytest.raises(RuntimeError, match="同一交易日存在多条canonical"):
        governance.load_canonical_governance_snapshot()

    forged = copy.deepcopy(result)
    forged["allocations"][0]["real_order_authority"] = True
    forged_raw = governance._json_text(forged)
    forged_row = {
        **row,
        "result_json": forged_raw,
        "result_hash": hashlib.sha256(forged_raw.encode("utf-8")).hexdigest(),
    }
    monkeypatch.setattr(governance, "_db_read", lambda *_args, **_kwargs: [forged_row])
    with pytest.raises(RuntimeError, match="真实下单权限"):
        governance.load_canonical_governance_snapshot(trade_date)


def test_adapter_rejects_nested_lambda_bound_default_and_mutable_global():
    def nested(_strategy, _context):
        return None

    class CallableObject:
        def __call__(self, _strategy, _context):
            return None

    def with_default(_strategy, _context=None):
        return None

    for candidate in (
        nested,
        lambda _strategy, _context: None,
        CallableObject(),
        with_default,
        adapter_with_mutable_global,
        adapter_with_nested_mutable_global,
    ):
        with pytest.raises(ValueError, match="模块级|lambda|默认参数|可变"):
            compute_strategy_execution_adapter_artifact_sha256(
                adapter_key="dynamic.rejected",
                adapter_version="1.0.0",
                evaluator_types={"dynamic_score"},
                candidate_builder=candidate,
            )


def test_adapter_artifact_binds_exact_runtime_helper_symbol(monkeypatch):
    first = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.helper",
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=adapter_with_rebindable_helper,
    )
    monkeypatch.setitem(
        adapter_with_rebindable_helper.__globals__,
        "_REBINDABLE_ADAPTER_HELPER",
        _adapter_helper_two,
    )
    second = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.helper",
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=adapter_with_rebindable_helper,
    )
    assert first != second


def test_adapter_artifact_rejects_transitive_helper_mutable_global():
    with pytest.raises(ValueError, match="helper拒绝可变或不透明全局"):
        compute_strategy_execution_adapter_artifact_sha256(
            adapter_key="dynamic.transitive_mutable",
            adapter_version="1.0.0",
            evaluator_types={"dynamic_score"},
            candidate_builder=adapter_with_transitive_mutable_helper,
        )


def test_validator_cannot_mutate_strategy_identity_or_run_after_hard_failure(
    monkeypatch,
):
    artifact = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.validator",
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=trusted_zero_candidate_builder,
        validator=mutating_strategy_validator,
    )
    binding = normalize_execution_binding({
        "adapter_key": "dynamic.validator",
        "adapter_version": "1.0.0",
        "strategy_version": "v1",
        "artifact_sha256": artifact,
        "cost_model": {
            "model_key": "cn.paper.v1", "currency": "CNY",
            "commission_pct": 0.03, "stamp_tax_pct": 0.05,
            "slippage_pct": 0.10, "transfer_fee_pct": 0.001,
        },
    }, strategy_version="v1")
    adapter = StrategyExecutionAdapter(
        adapter_key="dynamic.validator",
        adapter_version="1.0.0",
        artifact_sha256=artifact,
        evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_zero_candidate_builder,
        validator=mutating_strategy_validator,
    )
    register_strategy_execution_adapter(adapter)
    try:
        strategy = {
            "strategy_key": "dynamic_validator",
            "current_version": "v1",
            "version_hash": "b" * 64,
            "source_kind": "runtime_registry",
            "evaluator_type": "dynamic_score",
            "evaluator_config": {"execution_adapter": binding},
            "version_integrity_valid": True,
            "enabled": True,
            "current_status": "SHADOW",
        }
        status = strategy_execution_adapter_status(strategy)
        assert status["executable"] is False
        assert "修改了只读策略输入" in status["reason"]
        assert strategy["source_kind"] == "runtime_registry"

        called = False

        def should_not_run(_strategy):
            nonlocal called
            called = True
            return True, ""

        object.__setattr__(adapter, "validator", should_not_run)
        monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
        hard_failed = strategy_execution_adapter_status(strategy)
        assert hard_failed["executable"] is False
        assert called is False
    finally:
        monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
        object.__setattr__(adapter, "validator", mutating_strategy_validator)
        unregister_strategy_execution_adapter(
            "dynamic.validator", "1.0.0", explicit_test_mode=True,
        )


def test_combination_cannot_wrap_reduce_member_to_bypass_discount():
    combination = {
        "combination_key": "combo",
        "current_version": "v1",
        "combination_name": "组合",
        "enabled": True,
        "funding_gate_hash": "f" * 64,
        "ranking_score": 90.0,
        "ranking_basis": governance.DAILY_NAV_RANKING_BASIS,
        "profit_gate_passed": True,
        "paper_allocation_eligible": True,
        "market_route": {
            "market_state": "trend_bullish", "eligible": True,
            "market_match_score": 100.0,
            "router_decision_hash": "r" * 64,
        },
        "current_status": "ACTIVE",
        "member_sleeve_risk_multiplier": 0.75,
        "member_details": [
            {"strategy_key": "active_member", "strategy_version": "v1", "current_strategy_version": "v1", "version_match": True, "weight": 0.5, "lifecycle_status": "ACTIVE", "lifecycle_risk_multiplier": 1.0},
            {"strategy_key": "reduce_member", "strategy_version": "v1", "current_strategy_version": "v1", "version_match": True, "weight": 0.5, "lifecycle_status": "REDUCE", "lifecycle_risk_multiplier": 0.5},
        ],
        "constraint_evaluation": {"passed": True},
    }
    contract = governance._allocation_candidate_contract([], [combination])
    assert contract[0]["lifecycle_risk_multiplier"] == 0.75
    allocations = governance._allocation(
        [], [], "trend_bullish", trading_allowed=True,
        candidate_contract=contract,
    )
    assert allocations[0]["base_competitive_weight_pct"] == 85.0
    assert allocations[0]["simulated_weight_pct"] == 63.75
    assert sum(item["base_bp"] for item in allocations[0]["member_sleeves"]) == 8500
    assert sum(item["effective_bp"] for item in allocations[0]["member_sleeves"]) == 6375
    assert allocations[0]["cash_discount_bp"] == 2125
    assert allocations[-1]["simulated_weight_pct"] == 36.25


def test_strategy_and_combination_use_fixed_type_lanes_not_cross_raw_score():
    base = _allocation_contract()[0]
    strategy = {**base, "ranking_score": 1.0, "exposure_keys": ["solo"]}
    combo = {
        **base,
        "target_type": "COMBINATION",
        "target_key": "combo",
        "target_name": "组合",
        "ranking_score": 99.0,
        "exposure_keys": ["member"],
        "member_sleeves_source": [{
            "strategy_key": "member", "strategy_version": "v1",
            "current_strategy_version": "v1", "version_match": True,
            "original_weight": 1.0, "member_lifecycle_status": "ACTIVE",
            "member_lifecycle_multiplier": 1.0,
        }],
        "constraint_passed": True,
    }
    allocations = governance._allocation(
        [], [], "trend_bullish", trading_allowed=True,
        candidate_contract=[strategy, combo],
    )
    funded = {
        row["target_type"]: row["base_competitive_weight_pct"]
        for row in allocations if row["target_type"] != "CASH"
    }
    assert funded == {"COMBINATION": 42.5, "STRATEGY": 42.5}


def test_pool_filters_fake_dynamic_strategy_and_preserves_theme_industry():
    candidate = {
        "stock_code": "600036",
        "stock_name": "招商银行",
        "strategies": ["fake_dynamic", "valid_strategy"],
        "dominant_strategy": "fake_dynamic",
        "theme_code": "银行",
        "model_confidence": 88,
        "risk_reward_ratio": 2.0,
        "final_status": "READY",
        "blocking_reasons": [],
        "data_date": "2026-08-21",
    }
    strategies = [
        {
            "strategy_key": "fake_dynamic",
            "strategy_name": "伪动态",
            "ranking_score": 99,
            "execution_adapter_executable": False,
            "paper_allocation_eligible": True,
            "enabled": True,
            "current_status": "ACTIVE",
        },
        {
            "strategy_key": "valid_strategy",
            "strategy_name": "有效策略",
            "ranking_score": 80,
            "execution_adapter_executable": True,
            "paper_allocation_eligible": True,
            "enabled": True,
            "current_status": "ACTIVE",
        },
    ]
    pools = governance._build_pools(
        _snapshot("high_range", "REDUCE_NEW_BUY", candidates=[candidate]),
        strategies,
    )
    assert pools["observation"] == []
    assert pools["confirmation"] == []
    assert pools["tradable"] == []

    valid_candidate = {
        **candidate,
        "strategies": ["valid_strategy"],
        "dominant_strategy": "valid_strategy",
    }
    pools = governance._build_pools(
        _snapshot("high_range", "REDUCE_NEW_BUY", candidates=[valid_candidate]),
        strategies,
    )
    assert pools["tradable"][0]["strategies"] == ["valid_strategy"]
    assert pools["tradable"][0]["industry_name"] == "银行"
    governance._attach_pool_industry_focus(strategies, pools)
    assert strategies[1]["primary_industry"] == "银行"
    assert strategies[0]["industry_candidate_count"] == 0


def test_pool_industry_focus_preserves_each_signal_industry():
    candidate = {
        "stock_code": "600000",
        "stock_name": "测试",
        "strategies": ["bank_signal", "tech_signal"],
        "dominant_strategy": "bank_signal",
        "industry_name": "银行",
        "industry_names": ["电子", "银行"],
        "industry_by_strategy": {
            "bank_signal": "银行", "tech_signal": "电子",
        },
        "model_confidence": 88,
        "risk_reward_ratio": 2.0,
        "final_status": "WATCH",
        "blocking_reasons": [],
        "data_date": "2026-08-21",
    }
    strategies = [
        {
            "strategy_key": key, "strategy_name": key,
            "ranking_score": 80,
            "execution_adapter_executable": True,
            "paper_allocation_eligible": False,
            "enabled": True,
            "current_status": "SHADOW",
        }
        for key in ("bank_signal", "tech_signal")
    ]
    pools = governance._build_pools(
        _snapshot("high_range", "REDUCE_NEW_BUY", candidates=[candidate]),
        strategies,
    )
    assert pools["observation"][0]["industry_names"] == ["电子", "银行"]
    governance._attach_pool_industry_focus(strategies, pools)
    assert {
        row["strategy_key"]: row["primary_industry"] for row in strategies
    } == {"bank_signal": "银行", "tech_signal": "电子"}


@pytest.mark.parametrize(
    ("enabled", "lifecycle"),
    ((False, "ACTIVE"), (True, "RETIRED"), (True, "SUSPENDED")),
)
def test_disabled_retired_and_suspended_never_enter_any_pool(enabled, lifecycle):
    strategy = {
        "strategy_key": "blocked", "strategy_name": "阻断策略",
        "ranking_score": 99, "execution_adapter_executable": True,
        "paper_allocation_eligible": True, "enabled": enabled,
        "current_status": lifecycle,
    }
    candidate = {
        "stock_code": "600000", "strategies": ["blocked"],
        "dominant_strategy": "blocked", "model_confidence": 99,
        "risk_reward_ratio": 3.0, "final_status": "READY",
        "blocking_reasons": [], "data_date": "2026-08-21",
    }
    pools = governance._build_pools(
        _snapshot("trend_bullish", "ALLOW_NEW_BUY", candidates=[candidate]),
        [strategy],
    )
    assert pools["observation"] == []
    assert pools["confirmation"] == []
    assert pools["tradable"] == []


def test_missing_or_tampered_trade_day_industry_snapshot_blocks_combination():
    metrics = {
        "internal_daily_records": [
            {"trade_date": f"2026-01-{day:02d}", "return_pct": float(day % 2)}
            for day in range(1, 21)
        ],
        "internal_stock_exposure": {"000001": "1"},
    }
    members = [
        {
            "weight": 0.5,
            "strategy": {
                "strategy_key": key,
                "metrics": {"60": metrics},
            },
        }
        for key in ("left", "right")
    ]
    combination = {
        "combination_key": "combo", "current_version": "v1",
        "constraints": {
            **governance.DEFAULT_COMBINATION_CONSTRAINTS,
            "minimum_pairwise_observations": 20,
        },
    }
    missing = governance._combination_constraint_evaluation(
        combination, members, trade_date="2026-01-21",
        industry_snapshot=None,
    )
    assert missing["passed"] is False
    assert missing["checks"][-1]["industry_snapshot_valid"] is False

    payload = {
        "schema": "probiga.governance-industry-snapshot.v1",
        "trade_date": "2026-01-21", "as_of_exclusive": "2026-01-22",
        "status": "COMPLETED", "requested_stock_codes": ["000001"],
        "rows": [{
            "stock_code": "000001", "industry_name": "银行",
            "industry_type": "L1",
            "source_etl_sync_at": "2026-01-20T12:00:00",
            "source_id": "1",
        }],
        "reason": "行业快照已按治理交易日冻结",
    }
    tampered = {**payload, "snapshot_hash": "f" * 64}
    rejected = governance._combination_constraint_evaluation(
        combination, members, trade_date="2026-01-21",
        industry_snapshot=tampered,
    )
    assert rejected["passed"] is False
    assert rejected["checks"][-1]["industry_snapshot_valid"] is False


def test_toggle_uses_post_transition_status_in_compare_and_swap(monkeypatch):
    captured = {}

    class Result:
        rowcount = 1

        def scalar(self):
            return 0

    class Connection:
        def execute(self, statement, params=None):
            sql = str(statement)
            if "UPDATE st_strategy_registry" in sql:
                captured.update(params or {})
            return Result()

    class Engine:
        def begin(self):
            return nullcontext(Connection())

    monkeypatch.setattr(governance, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(governance, "get_engine", lambda: Engine())
    monkeypatch.setattr(governance, "_db_read", lambda *_a, **_k: [{
        "current_version": "v1", "current_status": "ACTIVE", "enabled": 1,
    }])
    monkeypatch.setattr(governance, "transition_lifecycle", lambda *_a, **_k: {
        "entity_key": "dynamic_alpha",
        "previous_status": "ACTIVE",
        "next_status": "SUSPENDED",
        "changed": True,
    })
    result = governance.toggle_strategy_enabled(
        "dynamic_alpha", False, reason="人工关闭", operator="tester",
    )
    assert result["enabled"] is False
    assert captured["current_status"] == "SUSPENDED"
