from __future__ import annotations

import copy
import hashlib
from contextlib import nullcontext
from datetime import date, timedelta

import pytest

from server.engine import strategy_center as center
from server.engine import strategy_governance as governance
from server.engine import strategy_execution_adapters as adapter_module
from server.trading_v3 import paper_execution
from server.trading_v3.decision_truth import canonical_hash
from server.engine.strategy_execution_adapters import (
    StrategyExecutionAdapter,
    compute_strategy_execution_adapter_artifact_sha256,
    create_candidate_batch,
    execute_dynamic_adapter_candidate_batch,
    normalize_execution_binding,
    persist_strategy_adapter_run_receipt,
    register_strategy_execution_adapter,
    strategy_execution_adapter_capabilities,
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


def _adapter_helper_with_module(_strategy, _context):
    return adapter_module.os.getcwd()


def adapter_with_module_helper(strategy, context):
    return _adapter_helper_with_module(strategy, context)


def _adapter_helper_with_module_default(
    _strategy, _context, module=adapter_module.os,
):
    return module.getcwd()


def adapter_with_module_default_helper(strategy, context):
    return _adapter_helper_with_module_default(strategy, context)


def _make_closure_helper():
    state = []

    def helper(_strategy, _context):
        state.append("side-effect")
        return len(state)

    return helper


_ADAPTER_CLOSURE_HELPER = _make_closure_helper()


def adapter_with_closure_helper(strategy, context):
    return _ADAPTER_CLOSURE_HELPER(strategy, context)


def adapter_with_direct_framework_globals(_strategy, _context):
    return create_candidate_batch.__globals__["os"].getcwd()


def _adapter_helper_with_indirect_framework_globals(_strategy, _context):
    return create_candidate_batch.__globals__["os"].getcwd()


def adapter_with_indirect_framework_globals(strategy, context):
    return _adapter_helper_with_indirect_framework_globals(strategy, context)


def adapter_with_builtin_getattr_reflection(_strategy, _context):
    namespace = getattr(create_candidate_batch, "__globals__")
    return namespace["os"].getcwd()


def adapter_with_dunder_type_reflection(_strategy, _context):
    return ().__class__.__base__.__subclasses__()


_ADAPTER_OS_MODULE_ALIAS = adapter_module.os


def adapter_with_module_alias(_strategy, _context):
    return _ADAPTER_OS_MODULE_ALIAS.getcwd()


def _make_reflection_closure_helper():
    target = create_candidate_batch

    def helper(_strategy, _context):
        return target.__globals__["os"].getcwd()

    return helper


_REFLECTION_CLOSURE_HELPER = _make_reflection_closure_helper()


def adapter_with_reflection_closure_helper(strategy, context):
    return _REFLECTION_CLOSURE_HELPER(strategy, context)


class _AdapterObjectCapability:
    def escape(self):
        return create_candidate_batch.__globals__["os"].getcwd()


_ADAPTER_OBJECT_CAPABILITY = _AdapterObjectCapability()


def adapter_with_object_capability(_strategy, _context):
    return _ADAPTER_OBJECT_CAPABILITY.escape()


class _AdapterBoundMethodCapability:
    @classmethod
    def escape(cls):
        return create_candidate_batch.__globals__["os"].getcwd()


_ADAPTER_BOUND_METHOD_CAPABILITY = _AdapterBoundMethodCapability.escape


def adapter_with_bound_method_capability(_strategy, _context):
    return _ADAPTER_BOUND_METHOD_CAPABILITY()


class _AdapterBuiltinReflector:
    REFLECT = staticmethod(getattr)


def adapter_with_class_builtin_getattr(_strategy, _context):
    namespace = _AdapterBuiltinReflector.REFLECT(
        create_candidate_batch, "__globals__",
    )
    return namespace["os"].getcwd()


class _AdapterEscapingMetaclass(type):
    def __call__(cls):
        return create_candidate_batch.__globals__["os"].getcwd()


class _AdapterMetaclassCarrier(metaclass=_AdapterEscapingMetaclass):
    pass


def adapter_with_metaclass_escape(_strategy, _context):
    return _AdapterMetaclassCarrier()


class _PureAdapterClassHelper:
    SCALE = 2

    @staticmethod
    def score(value):
        return abs(value) * _PureAdapterClassHelper.SCALE


def adapter_with_pure_class_helper(_strategy, _context):
    return _PureAdapterClassHelper.score(-2)


class _ModuleBackedAdapterClassHelper:
    @staticmethod
    def run():
        return adapter_module.os.getcwd()


def adapter_with_module_backed_class(_strategy, _context):
    return _ModuleBackedAdapterClassHelper.run()


class _MutableStateAdapterClassHelper:
    STATE = []

    @staticmethod
    def run():
        _MutableStateAdapterClassHelper.STATE.append("side-effect")
        return 1


def adapter_with_mutable_class(_strategy, _context):
    return _MutableStateAdapterClassHelper.run()


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
        "pit_reconstruction": {
            "schema": "probiga.strategy-candidate-reconstruction.v1",
            "mode": "NONE",
            "trade_date": trade_date,
        },
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
        "generated_at": "2026-08-21T15:00:00",
        "candidate_source": source,
        "market_state": {"key": state},
        "global_gate": {"status": status, "reason": "测试门禁"},
        "candidates": rows,
    }


def _exact_industry_snapshot(
    stock_to_industry: dict[str, str],
    *, trade_date: str = "2026-08-21",
) -> dict:
    from server.engine.strategy_industry_history import build_history_rows

    source_hash = governance._digest({
        "schema": "test.qmt-industry-source.v1",
        "trade_date": trade_date,
        "stock_to_industry": stock_to_industry,
    })
    _snapshot_id, rows = build_history_rows(
        [
            {
                "industry_code": f"TEST-{index:03d}",
                "industry_name": industry,
                "industry_type": "L1",
                "stock_code": stock_code,
            }
            for index, (stock_code, industry) in enumerate(
                sorted(stock_to_industry.items())
            )
        ],
        trade_date=trade_date,
        source="QMT_TEST",
        industry_hash=source_hash,
        captured_at=f"{trade_date}T15:05:00",
    )
    payload = {
        "schema": governance.INDUSTRY_SNAPSHOT_SCHEMA,
        "snapshot_id": rows[0]["snapshot_id"],
        "trade_date": trade_date,
        "as_of_exclusive": "2026-08-22T00:00:00",
        "status": "COMPLETED",
        "requested_stock_codes": sorted(stock_to_industry),
        "rows": rows,
        "reason": "测试目标日QMT一级行业冻结事实",
    }
    return {**payload, "snapshot_hash": governance._digest(payload)}


def _portfolio_risk_evidence(
    stock_code: str = "600000", *, phase: int = 0,
) -> dict:
    patterns = (
        (0.10, -0.10),
        (0.10, 0.10, -0.10, -0.10),
    )
    pattern = patterns[phase % len(patterns)]
    start = date(2026, 1, 1)
    daily_exposures = [
        {
            "trade_date": (start + timedelta(days=index)).isoformat(),
            "gross_exposure_value": "10000.00000000",
            "normalized_stock_weights": [{
                "stock_code": stock_code,
                "normalized_weight": "1.000000000000",
            }],
        }
        for index in range(60)
    ]
    payload = {
        "schema": governance.PORTFOLIO_RISK_EVIDENCE_SCHEMA,
        "status": "READY",
        "window_days": 60,
        "daily_returns": [
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "return_pct": f"{pattern[index % len(pattern)]:.8f}",
            }
            for index in range(60)
        ],
        "daily_stock_exposures": daily_exposures,
        "current_stock_exposure": [{
            "stock_code": stock_code,
            "normalized_weight": "1.000000000000",
        }],
        "peak_gross_exposure_value": "10000.00000000",
        "peak_gross_exposure_trade_date": daily_exposures[0]["trade_date"],
        "exposure_path_hash": governance._digest({
            "schema": "probiga.strategy-daily-stock-exposure-path.v1",
            "rows": daily_exposures,
        }),
    }
    return {**payload, "evidence_hash": governance._digest(payload)}


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
        "portfolio_risk_evidence": _portfolio_risk_evidence(),
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


def test_governance_requires_publication_after_historical_reconstruction():
    snapshot = _snapshot("trend_bullish", "ALLOW_NEW_BUY")
    trade_date = snapshot["trade_date"]
    core = {
        "schema": "probiga.qmt-announcement-historical-reconstruction.v2",
        "mode": "HISTORICAL_RECONSTRUCTION",
        "target_trade_date": trade_date,
        "source_query_cutoff_at": f"{trade_date}T23:59:59.999999",
        "reconstructed_at": "2026-08-22T13:00:20.000000",
        "known_at": "2026-08-22T13:00:20.000000",
        "provider": "cninfo.announcement",
        "source": "cninfo.announcement",
        "scheduler_run_uid": "1" * 32,
        "build_sha": "2" * 40,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    provenance = {
        **core,
        "reconstruction_sha256": governance._digest(core),
    }
    source = dict(snapshot["candidate_source"])
    source["pit_reconstruction"] = {
        "schema": "probiga.strategy-candidate-reconstruction.v1",
        "mode": "HISTORICAL_RECONSTRUCTION",
        "trade_date": trade_date,
        "reconstruction_sha256": provenance["reconstruction_sha256"],
        "reconstructed_at": provenance["reconstructed_at"],
        "known_at": provenance["known_at"],
        "provenance": provenance,
    }
    source["publication_proof"] = {
        "status": "COMPLETED",
        "published_at": "2026-08-22T13:00:20.000000",
        "finished_at": "2026-08-22T13:00:20.000000",
    }
    source["source_hash"] = governance._digest({
        key: value for key, value in source.items() if key != "source_hash"
    })
    snapshot["candidate_source"] = source
    snapshot["generated_at"] = "2026-08-22T13:00:19.999999"
    assert governance.governance_input_ready(snapshot)[0] is False
    snapshot["generated_at"] = "2026-08-22T13:00:20.000000"
    assert governance.governance_input_ready(snapshot)[0] is True


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
    assert "执行适配器未部署" in status["reason"]
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
    assert "执行适配器未部署" in reason


@pytest.mark.parametrize(
    ("ledger_status", "producer_ready", "funding_ready", "expected_label"),
    (
        ("VERIFIED_EMPTY", True, False, "模拟链结构已就绪，证据积累中"),
        ("VERIFIED_PENDING", True, False, "模拟链结构已就绪，证据积累中"),
        ("VERIFIED", True, True, "执行适配器与模拟链成熟证据已就绪"),
        ("INVALID", False, False, "模拟链校验失败"),
    ),
)
def test_dynamic_ledger_display_states_do_not_confuse_accumulation_with_deploy(
    ledger_status, producer_ready, funding_ready, expected_label,
):
    base = {
        "executable": True,
        "status": "RESEARCH_READY",
        "adapter_key": "dynamic.alpha",
        "adapter_version": "1.0.0",
    }
    readiness = {
        "status": ledger_status,
        "schema_readable": True,
        "shadow_trial_producer_ready": producer_ready,
        "funding_pipeline_ready": funding_ready,
        "verified_forward_evidence_ready": funding_ready,
        "verified_chain_count": 1 if funding_ready else 0,
        "pending_plan_count": 1 if ledger_status == "VERIFIED_PENDING" else 0,
        "invalid_chain_count": 1 if ledger_status == "INVALID" else 0,
        "invalid_chains": ([{"reason": "hash mismatch"}]
                           if ledger_status == "INVALID" else []),
        "ledger_hash": "a" * 64,
    }
    status = adapter_module._with_dynamic_ledger_status(base, readiness)
    assert status["status_label"] == expected_label
    assert "资金链尚未部署" not in status["reason"]
    assert status["funding_pipeline_ready"] is funding_ready
    assert status["real_order_submission_enabled"] is False
    assert status["automatic_real_order_submission"] is False


def test_builtin_adapter_status_exposes_compatible_funding_fields():
    status = strategy_execution_adapter_status({
        "strategy_key": "right_side_trend",
        "current_version": "manifest-v1",
        "version_hash": "a" * 64,
        "source_kind": "immutable_manifest",
        "evaluator_type": "manifest_score_adapter",
        "version_integrity_valid": True,
        "enabled": True,
        "current_status": "ACTIVE",
    })
    assert status["executable"] is True
    assert status["funding_pipeline_ready"] is True
    assert status["funding_status"] == "NOT_APPLICABLE_BUILTIN"
    assert status["funding_evidence_state"] == "BUILTIN_VERSION_BOUND_PATH"
    assert status["real_order_submission_enabled"] is False
    assert status["automatic_real_order_submission"] is False


def test_adapter_capabilities_do_not_confuse_registry_integrity_with_execution(
    monkeypatch,
):
    artifact_sha256 = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.capability",
        adapter_version="1.0.0",
        evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_zero_candidate_builder,
    )
    adapter = StrategyExecutionAdapter(
        adapter_key="dynamic.capability",
        adapter_version="1.0.0",
        artifact_sha256=artifact_sha256,
        evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_zero_candidate_builder,
    )
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setattr(adapter_module, "_REGISTRY", {})
    monkeypatch.setattr(adapter_module, "_REGISTRY_SEALED", False)
    monkeypatch.setattr(adapter_module, "_REGISTRY_SEAL_HASH", "")

    unsealed = strategy_execution_adapter_capabilities()
    assert unsealed["registry_integrity_ready"] is False
    assert unsealed["adapter_configured"] is False
    assert unsealed["candidate_execution_ready"] is False
    assert unsealed["funding_pipeline_ready"] is False
    assert unsealed["governance_paper_execution_ready"] is False
    assert unsealed["production_execution_ready"] is False
    assert unsealed["real_order_submission_enabled"] is False
    assert unsealed["automatic_real_order_submission"] is False
    assert unsealed["real_order_authority"] is False

    monkeypatch.setattr(adapter_module, "_REGISTRY_SEALED", True)
    monkeypatch.setattr(adapter_module, "_REGISTRY_SEAL_HASH", "a" * 64)
    sealed_empty = strategy_execution_adapter_capabilities()
    assert sealed_empty["registry_integrity_ready"] is True
    assert sealed_empty["adapter_count"] == 0
    assert sealed_empty["adapter_configured"] is False
    assert sealed_empty["candidate_execution_ready"] is False
    assert sealed_empty["production_execution_ready"] is False

    monkeypatch.setattr(
        adapter_module,
        "_REGISTRY",
        {(adapter.adapter_key, adapter.adapter_version): adapter},
    )
    configured = strategy_execution_adapter_capabilities()
    assert configured["registry_integrity_ready"] is True
    assert configured["adapter_configured"] is True
    assert configured["candidate_execution_ready"] is True
    assert configured["funding_pipeline_ready"] is False
    assert configured["governance_paper_execution_ready"] is False
    assert configured["production_execution_ready"] is False
    assert configured["real_order_submission_enabled"] is False
    assert configured["automatic_real_order_submission"] is False
    assert configured["real_order_authority"] is False
    assert all(
        row["real_order_submission_enabled"] is False
        and row["automatic_real_order_submission"] is False
        and row["real_order_authority"] is False
        for row in configured["adapters"]
    )

    dynamic = strategy_execution_adapter_capabilities(registry_rows=[{
        "strategy_key": "dynamic_capability_strategy",
        "current_version": "v1",
        "version_hash": "b" * 64,
        "source_kind": "runtime_registry",
        "execution_adapter": {
            "executable": True,
            "execution_binding_hash": "c" * 64,
            "funding_status": "ACCUMULATING",
            "funding_evidence_state": "PENDING_MATURITY",
            "paper_chain_structure_ready": True,
            "funding_pipeline_ready": False,
            "funding_ledger_hash": "d" * 64,
            "verified_forward_chain_count": 0,
            "pending_shadow_plan_count": 1,
        },
    }])
    assert dynamic["dynamic_version_count"] == 1
    assert dynamic["dynamic_version_readiness"][0][
        "real_order_submission_enabled"
    ] is False
    assert dynamic["dynamic_version_readiness"][0][
        "automatic_real_order_submission"
    ] is False
    assert dynamic["dynamic_version_readiness"][0][
        "real_order_authority"
    ] is False


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
        with pytest.raises(RuntimeError, match="不是可信事实"):
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


def test_candidate_execution_reuses_prebatched_status_without_ledger_query(
    monkeypatch,
):
    from server.engine import dynamic_shadow_ledger

    artifact = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.batch_ready",
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=trusted_zero_candidate_builder,
    )
    binding = normalize_execution_binding({
        "adapter_key": "dynamic.batch_ready",
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
        adapter_key="dynamic.batch_ready",
        adapter_version="1.0.0",
        artifact_sha256=artifact,
        evaluator_types=frozenset({"dynamic_score"}),
        candidate_builder=trusted_zero_candidate_builder,
    )
    strategy = {
        "strategy_key": "dynamic_batch_ready",
        "strategy_name": "批量就绪复用",
        "current_version": "v1",
        "version_hash": "b" * 64,
        "source_kind": "runtime_registry",
        "evaluator_type": "dynamic_score",
        "evaluator_config": {"execution_adapter": binding},
        "version_integrity_valid": True,
        "enabled": True,
        "current_status": "SHADOW",
    }
    register_strategy_execution_adapter(adapter)
    try:
        batch_status = strategy_execution_adapter_status(
            strategy, ledger_readiness=None,
        )
        assert batch_status["executable"] is True
        monkeypatch.setattr(
            dynamic_shadow_ledger,
            "dynamic_shadow_ledger_readiness",
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("per-strategy ledger query")
            ),
        )
        execution = execute_dynamic_adapter_candidate_batch(
            strategy,
            {"trade_date": "2026-08-24"},
            adapter_status=batch_status,
        )
        assert execution["receipt"]["candidate_count"] == 0
    finally:
        unregister_strategy_execution_adapter(
            "dynamic.batch_ready", "1.0.0", explicit_test_mode=True,
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


def test_candidate_source_zero_rows_requires_same_day_publication_receipt(
    monkeypatch,
):
    monkeypatch.setattr(center, "_table_exists", lambda _name: True)
    monkeypatch.setattr(
        center,
        "_table_columns",
        lambda name: (
            {"stock_code", "pick_date"}
            if name == "st_recommended_stocks"
            else {
                "run_uid", "trade_date", "status", "finished_at",
                "published_at", "publisher_task_type",
                "canonical_pool_sha256", "passed", "total", "build_sha",
                "membership_snapshot_date", "membership_snapshot_source",
                "membership_proof_sha256",
            }
        ),
    )

    def read(sql, _params):
        if "COUNT(*)" in sql:
            return [{"cnt": 0}]
        if "st_recommended_run_history" in sql:
            return []
        raise AssertionError(sql)

    monkeypatch.setattr(center, "_db_read", read)

    source = center._candidate_source_contract("2026-08-31", [], [])

    assert source["status"] == "NOT_PUBLISHED"
    assert source["query_completed"] is False
    assert source["source_row_count"] == 0
    assert source["publication_proof"]["status"] == "NOT_PUBLISHED"


def test_candidate_source_accepts_verified_same_day_zero_publication(
    monkeypatch,
):
    monkeypatch.setattr(center, "_table_exists", lambda _name: True)
    monkeypatch.setattr(
        center,
        "_table_columns",
        lambda name: (
            {"stock_code", "pick_date"}
            if name == "st_recommended_stocks"
            else {
                "run_uid", "trade_date", "status", "finished_at",
                "published_at", "publisher_task_type",
                "canonical_pool_sha256", "passed", "total", "build_sha",
                "membership_snapshot_date", "membership_snapshot_source",
                "membership_proof_sha256",
            }
        ),
    )

    def read(sql, _params):
        if "COUNT(*)" in sql:
            return [{"cnt": 0}]
        if "st_recommended_run_history" in sql:
            return [{
                "run_uid": "1" * 32,
                "trade_date": "2026-08-31",
                "status": "done",
                "finished_at": "2026-08-31 22:20:00",
                "published_at": "2026-08-31 22:19:59",
                "publisher_task_type": "analysis_fast",
                "canonical_pool_sha256": "2" * 64,
                "passed": 0,
                "total": 5000,
                "build_sha": "3" * 40,
                "membership_snapshot_date": "2026-08-31",
                "membership_snapshot_source": "QMT_CANONICAL",
                "membership_proof_sha256": "4" * 64,
            }]
        raise AssertionError(sql)

    monkeypatch.setattr(center, "_db_read", read)

    source = center._candidate_source_contract("2026-08-31", [], [])

    assert source["status"] == "COMPLETED"
    assert source["query_completed"] is True
    assert source["publication_proof"]["published_row_count"] == 0


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
        "automatic_real_order_submission": False,
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
    funding_manifest, _funding_candidates = (
        governance._build_funding_checkpoint_manifest(
            run_uid="1" * 32,
            trade_date=trade_date,
            strategies=[],
            combinations=[],
        )
    )
    funding_coverage = funding_manifest["coverage"]
    industry_snapshot_payload = {
        "schema": governance.INDUSTRY_SNAPSHOT_SCHEMA,
        "snapshot_id": "",
        "trade_date": trade_date,
        "as_of_exclusive": "2026-08-22T00:00:00",
        "status": "INCOMPLETE",
        "requested_stock_codes": [],
        "rows": [],
        "reason": "没有候选证券需要行业冻结",
    }
    industry_snapshot = {
        **industry_snapshot_payload,
        "snapshot_hash": governance._digest(industry_snapshot_payload),
    }
    paper_plan_payload = {
        "schema": "probiga.governance-paper-execution-plan.v1",
        "trade_date": trade_date,
        "industry_snapshot_id": "",
        "industry_snapshot_hash": industry_snapshot["snapshot_hash"],
        "industry_snapshot_status": "INCOMPLETE",
        "policy": governance.GLOBAL_PORTFOLIO_POLICY,
        "funded_sleeves": [],
        "portfolio_risk": {
            "valid": False,
            "observations": 0,
            "annualized_volatility_pct": None,
            "expected_shortfall_95_pct": None,
            "risk_multiplier": 0.0,
            "reason": "没有合格策略，保持现金",
        },
        "requested_new_buy_turnover_bp": 0,
        "new_buy_turnover_multiplier": 1.0,
        "actual_new_buy_turnover_bp": 0,
        "targets": [],
        "exit_targets": [],
        "target_count": 0,
        "invested_bp": 0,
        "cash_bp": 10_000,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    paper_plan = {
        **paper_plan_payload,
        "plan_hash": governance._digest(paper_plan_payload),
    }
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
        "paper_execution_plan_hash": paper_plan["plan_hash"],
        "pool_snapshot_hash": pool_hash, "strategies": [], "combinations": [],
        "candidate_industry_snapshot_hash": industry_snapshot[
            "snapshot_hash"
        ],
        "funding_checkpoint_manifest_hash": funding_manifest[
            "manifest_hash"
        ],
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
        "paper_execution_plan": paper_plan,
        "paper_execution_plan_hash": paper_plan["plan_hash"],
        "candidate_industry_snapshot": industry_snapshot,
        "candidate_industry_snapshot_hash": industry_snapshot[
            "snapshot_hash"
        ],
        "router_snapshot": router_snapshot, "router_snapshot_hash": router_hash,
        "automatic_transition_plan": transition_plan,
        "automatic_transition_plan_hash": transition_hash,
        "funding_checkpoint_manifest": funding_manifest,
        "pool_snapshot": pool_snapshot, "pool_snapshot_hash": pool_hash,
        "summary": {
            "candidate_set_hash": candidate_hash,
                "allocation_snapshot_hash": allocation_hash,
                "paper_execution_plan_hash": paper_plan["plan_hash"],
                "candidate_industry_snapshot_id": "",
                "candidate_industry_snapshot_hash": industry_snapshot[
                    "snapshot_hash"
                ],
                "candidate_industry_snapshot_status": "INCOMPLETE",
            "paper_target_count": 0,
            "paper_invested_weight_pct": 0.0,
            "pool_snapshot_hash": pool_hash,
            "automatic_transition_plan_hash": transition_hash,
            "allocation_candidate_count": 0, "eligible_candidate_count": 0,
            "market_risk_cap_pct": 0.0,
            "funding_checkpoint_manifest_hash": funding_manifest[
                "manifest_hash"
            ],
            "funding_checkpoint_eligible_count": funding_coverage[
                "eligible_count"
            ],
            "funding_checkpointed_count": funding_coverage[
                "checkpointed_count"
            ],
            "funding_strategy_checkpoint_count": funding_coverage[
                "strategy_checkpoint_count"
            ],
            "funding_combination_recipe_count": funding_coverage[
                "combination_recipe_count"
            ],
            "funding_ready_count": funding_coverage["funding_ready_count"],
            "funding_checkpoint_ineligible_count": funding_coverage[
                "ineligible_count"
            ],
        },
        "pools": pools, "allocations": allocations,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
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


def test_paper_buy_receipt_requires_full_canonical_governance_replay(
    monkeypatch,
):
    target_payload = {
        "stock_code": "600036",
        "strategy_key": "right_side_trend",
        "strategy_version": "v7",
        "target_bp": 500,
        "allocation_backed": True,
        "new_buy_allowed": True,
        "exit_always_allowed": True,
        "real_order_authority": False,
    }
    target = {
        **target_payload,
        "target_hash": canonical_hash({
            "schema": "probiga.governance-paper-target.v1",
            **target_payload,
        }),
    }
    plan_payload = {
        "targets": [target],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    plan = {**plan_payload, "plan_hash": canonical_hash(plan_payload)}
    canonical_result = {
        "input_ready": True,
        "decision_contract_version": (
            governance.STATISTICAL_DECISION_CONTRACT
        ),
        "statistical_funding_eligible": True,
        "automatic_real_order_submission": False,
        "paper_execution_plan": plan,
        "paper_execution_plan_hash": plan["plan_hash"],
    }
    ledger = {
        "run_uid": "1" * 32,
        "trade_date": date(2026, 8, 21),
        "input_ready": 1,
        "build_commit_sha": "test-build",
        "input_hash": "a" * 64,
        "decision_hash": "b" * 64,
        "result_json": "{}",
        "result_hash": hashlib.sha256(b"{}").hexdigest(),
    }

    class Result:
        def __init__(self, *, rows=None, first=None):
            self._rows = rows or []
            self._first = first

        def mappings(self):
            return self

        def all(self):
            return self._rows

        def first(self):
            return self._first

    class Connection:
        def execute(self, statement, _params=None):
            sql = str(statement)
            if "FROM st_strategy_governance_run" in sql:
                assert "input_hash" in sql
                return Result(rows=[ledger])
            if "FROM st_strategy_registry" in sql:
                return Result(first={
                    "current_version": "v7",
                    "current_status": "ACTIVE",
                    "enabled": 1,
                })
            raise AssertionError(sql)

    replayed = []

    def replay(row):
        replayed.append(dict(row))
        return canonical_result

    monkeypatch.setattr(
        governance, "_canonical_governance_result_from_row", replay,
    )
    receipt, reason = paper_execution._canonical_governance_buy_receipt(
        Connection(),
        trade_date=date(2026, 8, 21),
        stock_code="600036",
        strategy_keys=["right_side_trend"],
    )
    assert reason == ""
    assert receipt is not None
    assert receipt["strategy_version"] == "v7"
    assert replayed == [ledger]

    monkeypatch.setattr(
        governance,
        "_canonical_governance_result_from_row",
        lambda _row: (_ for _ in ()).throw(
            RuntimeError("self-consistent forged canonical payload")
        ),
    )
    receipt, reason = paper_execution._canonical_governance_buy_receipt(
        Connection(),
        trade_date=date(2026, 8, 21),
        stock_code="600036",
        strategy_keys=["right_side_trend"],
    )
    assert receipt is None
    assert reason == "GOVERNANCE_CANONICAL_REPLAY_INVALID"


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


@pytest.mark.parametrize(
    ("candidate", "reason"),
    (
        (adapter_with_module_helper, "模块依赖"),
        (adapter_with_module_default_helper, "默认值"),
        (adapter_with_closure_helper, "闭包状态"),
    ),
)
def test_adapter_helper_dependencies_reject_module_default_and_closure(
    candidate, reason,
):
    with pytest.raises(ValueError, match=reason):
        compute_strategy_execution_adapter_artifact_sha256(
            adapter_key="dynamic.malicious_helper",
            adapter_version="1.0.0",
            evaluator_types={"dynamic_score"},
            candidate_builder=candidate,
        )


def test_adapter_class_dependencies_are_recursive_and_fail_closed():
    pure = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.pure_class",
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=adapter_with_pure_class_helper,
    )
    assert len(pure) == 64

    for candidate in (
        adapter_with_module_backed_class,
        adapter_with_mutable_class,
    ):
        with pytest.raises(ValueError, match="模块依赖|可变或不透明类属性"):
            compute_strategy_execution_adapter_artifact_sha256(
                adapter_key="dynamic.malicious_class",
                adapter_version="1.0.0",
                evaluator_types={"dynamic_score"},
                candidate_builder=candidate,
            )


@pytest.mark.parametrize(
    "candidate",
    (
        adapter_with_direct_framework_globals,
        adapter_with_indirect_framework_globals,
        adapter_with_builtin_getattr_reflection,
        adapter_with_dunder_type_reflection,
        adapter_with_module_alias,
        adapter_with_reflection_closure_helper,
        adapter_with_object_capability,
        adapter_with_bound_method_capability,
        adapter_with_class_builtin_getattr,
        adapter_with_metaclass_escape,
    ),
)
def test_adapter_reflection_and_object_capability_escapes_fail_closed(candidate):
    with pytest.raises(ValueError, match=(
        "反射|LOAD_GLOBAL|模块|闭包|可变|不透明|绑定方法|描述符"
    )):
        compute_strategy_execution_adapter_artifact_sha256(
            adapter_key="dynamic.reflection_escape",
            adapter_version="1.0.0",
            evaluator_types={"dynamic_score"},
            candidate_builder=candidate,
        )


def test_adapter_reflection_hardening_preserves_pure_helpers():
    function_helper = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.pure_function_after_reflection_hardening",
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=adapter_with_rebindable_helper,
    )
    class_helper = compute_strategy_execution_adapter_artifact_sha256(
        adapter_key="dynamic.pure_class_after_reflection_hardening",
        adapter_version="1.0.0",
        evaluator_types={"dynamic_score"},
        candidate_builder=adapter_with_pure_class_helper,
    )
    assert len(function_helper) == 64
    assert len(class_helper) == 64
    assert function_helper != class_helper


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
    contract[0]["portfolio_risk_evidence"] = _portfolio_risk_evidence()
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


def test_strategy_and_combination_share_one_quality_weighted_lane():
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
        "portfolio_risk_evidence": _portfolio_risk_evidence(
            "600001", phase=1,
        ),
    }
    allocations = governance._allocation(
        [], [], "trend_bullish", trading_allowed=True,
        candidate_contract=[strategy, combo],
    )
    funded = {
        row["target_type"]: row["base_competitive_weight_pct"]
        for row in allocations if row["target_type"] != "CASH"
    }
    assert funded == {"COMBINATION": 84.15, "STRATEGY": 0.85}
    assert all(
        row["allocation_type_lane_policy"]
        == governance.ALLOCATION_TYPE_LANE_POLICY
        for row in allocations
        if row["target_type"] != "CASH"
    )


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
        industry_snapshot=_exact_industry_snapshot({"600036": "银行"}),
    )
    assert pools["tradable"][0]["strategies"] == ["valid_strategy"]
    assert pools["tradable"][0]["industry_name"] == "银行"
    governance._attach_pool_industry_focus(strategies, pools)
    assert strategies[1]["primary_industry"] == "银行"
    assert strategies[0]["industry_candidate_count"] == 0


def test_pool_industry_focus_uses_one_authoritative_qmt_l1_industry():
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
        industry_snapshot=_exact_industry_snapshot({"600000": "银行"}),
    )
    assert pools["observation"][0]["industry_names"] == ["银行"]
    governance._attach_pool_industry_focus(strategies, pools)
    assert {
        row["strategy_key"]: row["primary_industry"] for row in strategies
    } == {"bank_signal": "银行", "tech_signal": "银行"}


@pytest.mark.parametrize(
    "drift_kind", ("tampered", "missing", "wrong_date", "non_l1")
)
def test_pool_bad_or_missing_exact_industry_is_observation_only(drift_kind):
    candidate = {
        "stock_code": "600036", "stock_name": "招商银行",
        "strategies": ["valid_strategy"],
        "dominant_strategy": "valid_strategy",
        "model_confidence": 88, "risk_reward_ratio": 2.0,
        "final_status": "READY", "blocking_reasons": [],
        "data_date": "2026-08-21",
    }
    strategy = {
        "strategy_key": "valid_strategy", "strategy_name": "有效策略",
        "ranking_score": 80, "execution_adapter_executable": True,
        "paper_allocation_eligible": True, "enabled": True,
        "current_status": "ACTIVE",
    }
    industry = _exact_industry_snapshot({"600036": "银行"})
    if drift_kind == "tampered":
        industry["rows"][0]["industry_name"] = "电子"
    elif drift_kind == "missing":
        industry["rows"] = []
        industry["snapshot_id"] = ""
        industry["status"] = "INCOMPLETE"
        industry["snapshot_hash"] = governance._digest({
            key: value for key, value in industry.items()
            if key != "snapshot_hash"
        })
    elif drift_kind == "wrong_date":
        industry["trade_date"] = "2026-08-20"
        industry["snapshot_hash"] = governance._digest({
            key: value for key, value in industry.items()
            if key != "snapshot_hash"
        })
    else:
        row = industry["rows"][0]
        row["industry_type"] = "L2"
        row["row_hash"] = governance._digest({
            key: value for key, value in row.items() if key != "row_hash"
        })
        industry["snapshot_hash"] = governance._digest({
            key: value for key, value in industry.items()
            if key != "snapshot_hash"
        })

    pools = governance._build_pools(
        _snapshot("trend_bullish", "ALLOW_NEW_BUY", candidates=[candidate]),
        [strategy],
        industry_snapshot=industry,
    )

    assert len(pools["observation"]) == 1
    assert pools["confirmation"] == []
    assert pools["tradable"] == []
    assert pools["observation"][0]["industry_name"] == ""
    assert "目标日QMT一级行业冻结事实缺失或无效" in (
        pools["observation"][0]["blocking_reasons"]
    )


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
