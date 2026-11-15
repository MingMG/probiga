from __future__ import annotations

from copy import deepcopy

import pytest
from sqlalchemy import create_engine, text

from server.common.sql_reader import bind_sql_connection
from server.engine import strategy_center as center
from server.engine import strategy_governance as governance
from server.engine.strategy_execution_adapters import strategy_execution_adapter_status
from server.trading_v3.config import load_v3_config
from test_dynamic_shadow_ledger import _schema
from test_native_shadow_trials import _strategy


TRADE_DATE = "2026-11-13"
CUTOFF = "2026-11-15T21:14:30"
RUN_UID = "a" * 32
MISSING = "SHADOW_V3_COMPLETED_SOURCE_UNAVAILABLE_AS_OF"


def _publication_proof():
    return {
        "schema": "probiga.strategy-candidate-publication-proof.v1",
        "status": "COMPLETED",
        "trade_date": TRADE_DATE,
        "publisher_task_type": "analysis_fast",
        "publisher_run_uid": RUN_UID,
        "build_sha": "b" * 40,
        "canonical_pool_sha256": "c" * 64,
        "analysis_score_snapshot_sha256": "d" * 64,
        "membership_snapshot_source": center.PROVIDER_ID,
        "membership_proof_sha256": "e" * 64,
        "decision_known_at": CUTOFF,
    }


def _build_candidate_source(statuses, proof):
    return center._candidate_source_contract(
        TRADE_DATE, [], [], dynamic_adapter_statuses=statuses, fixed_publication_proof=proof,
    )


@pytest.fixture
def fixed_native_sources(monkeypatch):
    shadow_keys = sorted(load_v3_config()["sleeves"])
    formal_keys = ["main_wave", "short_term", "swing", "ultra_short"]
    registry = [_strategy(key) for key in [*shadow_keys, *formal_keys]]
    for strategy in registry:
        strategy["execution_adapter"] = strategy_execution_adapter_status(strategy, ledger_readiness=None)
        assert strategy["execution_adapter"]["executable"] is True
    strategy_versions = {item["strategy_key"]: item["current_version"] for item in registry}
    monkeypatch.setattr(governance, "load_registry", lambda: registry)

    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection, bind_sql_connection(connection):
        _schema(connection)
        connection.exec_driver_sql("""
            CREATE TABLE st_decision_run_v3 (
                run_uid TEXT, trade_date TEXT, model_version TEXT, status TEXT, mode TEXT,
                decision_at TEXT, created_at TEXT, completed_at TEXT
            )
        """)
        connection.exec_driver_sql("""
            CREATE TABLE st_strategy_governance_run (
                trade_date TEXT, is_canonical INTEGER, status TEXT, run_revision INTEGER,
                result_json TEXT, result_hash TEXT
            )
        """)
        connection.execute(text("INSERT INTO si_trade_calendar VALUES (:trade_date, 1)"), {"trade_date": TRADE_DATE})
        connection.execute(text("INSERT INTO st_equity_daily_v2 VALUES ('paper-main-v2', :trade_date, 1000000)"),
                           {"trade_date": TRADE_DATE})
        connection.execute(text("""
            INSERT INTO st_decision_run_v3 VALUES (
                'too-late', :trade_date, :model, 'COMPLETED', 'close',
                '2026-11-13 16:00:00', '2026-11-15 21:00:00', '2026-11-15 21:14:31'
            )
        """), {"trade_date": TRADE_DATE, "model": registry[0]["evaluator_config"]["model_version"]})
        signals, statuses = center._dynamic_execution_signals(
            trade_date=TRADE_DATE, recommendation_rows=[], market={"market_state": "extreme_event"},
            configs={}, metrics={}, persist_receipts=True, analysis_run_uid=RUN_UID,
            decision_known_at=CUTOFF,
            native_analysis_source=(
                {"run_uid": RUN_UID, "trade_date": TRADE_DATE},
                {"run_uid": RUN_UID, "decision_at": CUTOFF, "scored_rows": []},
            ),
        )
        assert signals == []
        assert connection.execute(text("""
            SELECT strategy_key,candidate_count FROM st_strategy_adapter_run_receipt ORDER BY strategy_key
        """)).all() == [(key, 0) for key in formal_keys]
        for table in ("st_strategy_adapter_candidate_fact", "st_dynamic_shadow_trial_plan", "st_trade_intent_v2", "st_order_v2"):
            assert connection.exec_driver_sql(f"SELECT COUNT(*) FROM {table}").scalar_one() == 0
    engine.dispose()

    assert len(shadow_keys) == 8
    assert all(
        item["reason"] == MISSING and item["status"] == "ADAPTER_SOURCE_BLOCKED"
        and item["run_receipt_valid"] is False
        for item in statuses if item["strategy_key"] in shadow_keys
    )
    return statuses, shadow_keys, formal_keys, strategy_versions


def _strategy_snapshot(statuses, source):
    return {
        "source_status": "fresh", "is_stale": False, "trade_date": TRADE_DATE, "data_date": TRADE_DATE,
        "candidates": [], "dynamic_adapter_statuses": statuses, "candidate_source": source,
        "market_state": {"key": "extreme_event"}, "global_gate": {"status": "BLOCK_NEW_BUY"},
    }


def test_dated_formal_source_can_complete_with_eight_exact_v3_shadow_deferrals(fixed_native_sources):
    statuses, shadow_keys, formal_keys, strategy_versions = fixed_native_sources
    source = _build_candidate_source(statuses, _publication_proof())
    assert source["status"] == "COMPLETED"
    deferred = center.validated_deferred_shadow_sources(source)
    assert deferred == [
        {
            "strategy_key": key, "strategy_version": strategy_versions[key], "source_kind": "immutable_v3_sleeve",
            "reason_code": MISSING, "decision_known_at": CUTOFF,
        }
        for key in shadow_keys
    ]
    assert [item["strategy_key"] for item in source["dynamic_adapter_results"]] == formal_keys
    assert governance.governance_input_ready(_strategy_snapshot(statuses, source))[0] is True
    assert governance._snapshot_trading_gate(_strategy_snapshot(statuses, source))["trading_allowed"] is False

    tampered = deepcopy(source)
    tampered["dynamic_adapter_receipts"][0]["candidate_count"] += 1
    assert governance.governance_input_ready(_strategy_snapshot(statuses, tampered))[0] is False
    tampered = deepcopy(source)
    tampered["deferred_shadow_sources"][0]["strategy_version"] = "other"
    with pytest.raises(ValueError, match="proof is invalid"):
        center.validated_deferred_shadow_sources(tampered)
    assert governance.governance_input_ready(_strategy_snapshot(statuses, tampered))[0] is False


@pytest.mark.parametrize("unexpected", [
    {"lifecycle_status": "ACTIVE"},
    {"source_kind": "immutable_manifest"},
    {"status": "ADAPTER_RUNTIME_INVALID", "reason": "SHADOW_V3_CORRUPT"},
    {"reason": "SHADOW_V3_COMPLETED_SOURCE_MISSING"},
    {"candidate_count": 1},
    {"candidate_run_uid": "f" * 32},
    {"shadow_trial_plan_count": 1},
])
def test_dated_governance_cannot_exempt_other_broken_adapter_state(fixed_native_sources, unexpected):
    statuses, shadow_keys, _, _ = fixed_native_sources
    first = next(item for item in statuses if item["strategy_key"] == shadow_keys[0])
    valid_source = _build_candidate_source(statuses, _publication_proof())
    first.update(unexpected)
    assert governance.governance_input_ready(_strategy_snapshot(statuses, valid_source))[0] is False
    source = _build_candidate_source(statuses, _publication_proof())
    assert source["status"] == "INCOMPLETE"
    assert "deferred_shadow_sources" not in source


def test_v3_shadow_deferral_requires_original_dated_cutoff_and_cannot_mask_other_providers(fixed_native_sources):
    statuses, _, _, _ = fixed_native_sources
    for changes in (
        {"decision_known_at": None}, {"decision_known_at": "2026-11-13T15:00:00+08:00"},
        {"decision_known_at": "2026-11-12T21:14:30"}, {"publisher_task_type": "analysis_research"},
        {"analysis_score_snapshot_sha256": "0" * 64},
    ):
        proof = {**_publication_proof(), **changes}
        assert _build_candidate_source(statuses, proof)["status"] == "INCOMPLETE"
    source = center._candidate_source_contract(
        TRADE_DATE, [], [], dynamic_adapter_statuses=statuses, fixed_publication_proof=_publication_proof(),
        reference_pool={"_path": "read-only-research-reference"},
    )
    assert source["status"] == "INCOMPLETE"
    assert "deferred_shadow_sources" not in source


def test_deferred_shadow_funding_partition_and_readback_reject_new_execution(fixed_native_sources):
    statuses, _, _, _ = fixed_native_sources
    source = _build_candidate_source(statuses, _publication_proof())
    strategies = []
    for proof in center.validated_deferred_shadow_sources(source):
        row = {
            "strategy_key": proof["strategy_key"], "current_version": proof["strategy_version"],
            "source_kind": proof["source_kind"], "current_status": "SHADOW", "enabled": True,
            "funding_checkpoint_ready": True, "funding_checkpoint_storage_bytes": 150,
            "funding_checkpoint_candidate": {"pre-existing": True}, "funding_checkpoint_ref": {"pre-existing": True},
        }
        status, reason = governance._hold_deferred_shadow_strategy(row, proof)
        status, _, confirmation = governance._confirmed_funding_status(
            entity_type="STRATEGY", entity_key=row["strategy_key"], entity_version=row["current_version"],
            current_status="SHADOW", recommended_status=status, reason=reason, trade_date=TRADE_DATE,
            funding_gate_hash="", funding_evidence_revision_at="", minimum_new_sessions=2,
            authoritative_sessions_desc=[],
        )
        assert row["funding_checkpoint_storage_bytes"] == 0
        row.update(recommended_status=status, projected_status=status, confirmation_guard=confirmation, paper_allocation_eligible=False)
        strategies.append(row)
    manifest, candidates = governance._build_funding_checkpoint_manifest(
        run_uid="f" * 32, trade_date=TRADE_DATE, strategies=strategies, combinations=[],
    )
    assert candidates == []
    assert all(row["funding_manifest_ineligible"]["reason_code"] == MISSING for row in strategies)
    result = {
        "trade_date": TRADE_DATE, "candidate_source": source,
        "strategies": strategies, "pools": {"watch": [], "tradable": []},
        "allocations": [{"target_type": "CASH", "target_key": "cash", "simulated_weight_pct": 100}],
        "paper_execution_plan": {"targets": [], "funded_sleeves": []},
        "automatic_transition_plan": {"transitions": []},
        "shadow_capacity_queue": statuses[0]["shadow_capacity_queue"],
        "funding_checkpoint_manifest": manifest,
    }
    governance._validate_deferred_shadow_outcome(result)
    key = strategies[0]["strategy_key"]
    expected_rejections = {
        "allocations": [{"target_type": "STRATEGY", "target_key": key}],
        "paper_execution_plan": {"funded_sleeves": [{"strategy_key": key}]},
        "automatic_transition_plan": {"transitions": [{"entity_type": "STRATEGY", "entity_key": key}]},
        "shadow_capacity_queue": {"selected": {"strategy_key": key}, "authorized_plan_ids": ["plan"]},
        "pools": {"watch": [{"dominant_strategy": key}]},
    }
    for field, invalid in expected_rejections.items():
        with pytest.raises(RuntimeError, match="模拟执行授权"):
            governance._validate_deferred_shadow_outcome({**result, field: invalid})
    with pytest.raises(RuntimeError, match="交易日不一致"):
        governance._validate_deferred_shadow_outcome({**result, "trade_date": "2026-11-16"})
    strategies[0]["projected_status"] = "ACTIVE"
    with pytest.raises(RuntimeError, match="资金资格"):
        governance._validate_deferred_shadow_outcome(result)
