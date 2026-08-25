from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from server.engine import strategy_governance_orchestrator as orchestrator_module
from server.engine import strategy_governance as governance_module
from server.engine.strategy_governance import GovernanceEvidenceNotReady
from server.engine.strategy_governance_orchestrator import (
    INTEGRITY_ERROR,
    NOT_DUE,
    NOT_READY,
    PROGRAM_ERROR,
    orchestrate_strategy_governance,
)
from server.engine.strategy_industry_history import (
    IndustrySnapshotIntegrityError,
    IndustrySnapshotNotReady,
)


def test_engine_unavailable_closes_both_real_flags_and_never_logs_secret(
    monkeypatch, caplog,
):
    secret = "mysql://user:" + "password@private-db.internal/PROBIGA"
    monkeypatch.setattr(
        orchestrator_module,
        "get_engine",
        lambda: (_ for _ in ()).throw(RuntimeError(secret)),
    )

    result = orchestrate_strategy_governance(
        requested_trade_date="2026-08-21",
    )

    assert result["reason_code"] == "DATABASE_ENGINE_UNAVAILABLE"
    assert result["automatic_real_order_submission"] is False
    assert result["real_order_authority"] is False
    assert secret not in caplog.text
    assert "RuntimeError" in caplog.text
    assert "stage=DATABASE_ENGINE" in caplog.text


def test_blocked_attempt_hash_explicitly_binds_both_real_flags_off():
    result = {
        "orchestration_status": "NOT_READY",
        "error_class": "NOT_READY",
        "retryable": True,
        "reason_code": "FIXTURE",
        "blocking_stage": "INPUT",
        "reason": "fixture",
    }

    _audit_id, _audit_hash, payload, _before, _after = (
        orchestrator_module._attempt_payload(result, operator="pytest")
    )

    assert payload["evidence"]["automatic_real_order_submission"] is False
    assert payload["evidence"]["real_order_authority"] is False
    assert len(payload["evidence"]["attempt_hash"]) == 64


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE si_trade_calendar (
                trade_date TEXT PRIMARY KEY,
                trade_status INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_strategy_governance_run (
                run_uid TEXT PRIMARY KEY,
                trade_date TEXT NOT NULL,
                run_revision INTEGER NOT NULL,
                input_hash TEXT NOT NULL DEFAULT '',
                decision_hash TEXT NOT NULL,
                build_commit_sha TEXT NOT NULL,
                status TEXT NOT NULL,
                is_canonical INTEGER NOT NULL,
                result_json TEXT NOT NULL DEFAULT '',
                result_hash TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_strategy_governance_audit (
                audit_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL,
                operator_name TEXT NOT NULL,
                before_json TEXT NOT NULL,
                after_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                audit_hash TEXT NOT NULL UNIQUE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            INSERT INTO si_trade_calendar VALUES
            ('2026-08-20', 1), ('2026-08-21', 1),
            ('2026-08-22', 0), ('2026-08-24', 1)
        """))
    return engine


def _industry(_engine, *, trade_date: str):
    return {
        "status": "COMPLETED",
        "trade_date": trade_date,
        "snapshot_id": "a" * 64,
        "source_snapshot_hash": "b" * 64,
    }


def _governance(**kwargs):
    run_uid = "a" * 32
    manifest, _candidates = (
        governance_module._build_funding_checkpoint_manifest(
            run_uid=run_uid,
            trade_date=kwargs["trade_date"],
            strategies=[],
            combinations=[],
        )
    )
    coverage = manifest["coverage"]
    return {
        "status": "ok",
        "result_mode": "CANONICAL_PERSISTED",
        "run_uid": run_uid,
        "is_canonical": True,
        "trade_date": kwargs["trade_date"],
        "build_commit_sha": "WORKTREE_UNVERSIONED",
        "canonical_result_hash": "c" * 64,
        "decision_contract_version": "strategy-governance-decision.v7",
        "statistical_funding_eligible": True,
        "input_hash": "1" * 64,
        "decision_hash": "2" * 64,
        "strategies": [],
        "combinations": [],
        "funding_checkpoint_manifest": manifest,
        "summary": {
            "funding_checkpoint_manifest_hash": manifest["manifest_hash"],
            "funding_checkpoint_eligible_count": coverage["eligible_count"],
            "funding_checkpointed_count": coverage["checkpointed_count"],
            "funding_strategy_checkpoint_count": coverage[
                "strategy_checkpoint_count"
            ],
            "funding_combination_recipe_count": coverage[
                "combination_recipe_count"
            ],
            "funding_ready_count": coverage["funding_ready_count"],
            "funding_checkpoint_ineligible_count": coverage[
                "ineligible_count"
            ],
        },
        "allocations": [{
            "target_type": "CASH",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _replace_funding_contract(
    payload, *, strategies=None, combinations=None,
):
    strategies = list(strategies or [])
    combinations = list(combinations or [])
    manifest, _candidates = (
        governance_module._build_funding_checkpoint_manifest(
            run_uid=payload["run_uid"],
            trade_date=payload["trade_date"],
            strategies=strategies,
            combinations=combinations,
        )
    )
    coverage = manifest["coverage"]
    payload["strategies"] = strategies
    payload["combinations"] = combinations
    payload["funding_checkpoint_manifest"] = manifest
    payload["summary"].update({
        "funding_checkpoint_manifest_hash": manifest["manifest_hash"],
        "funding_checkpoint_eligible_count": coverage["eligible_count"],
        "funding_checkpointed_count": coverage["checkpointed_count"],
        "funding_strategy_checkpoint_count": coverage[
            "strategy_checkpoint_count"
        ],
        "funding_combination_recipe_count": coverage[
            "combination_recipe_count"
        ],
        "funding_ready_count": coverage["funding_ready_count"],
        "funding_checkpoint_ineligible_count": coverage["ineligible_count"],
    })
    return payload


def _eligible_combination(trade_date):
    members = [{
        "strategy_key": "member_alpha",
        "strategy_version": "v1",
        "weight": 1.0,
        "checkpoint_id": "3" * 64,
        "account_id": "paper-main-v2",
        "checkpoint_hash": "4" * 64,
        "chain_hash": "5" * 64,
        "history_fact_set_hash": "6" * 64,
        "checkpoint_trade_date": trade_date,
    }]
    risk_payload = {
        "schema": "probiga.combination-drift-risk-binding.v2",
        "window_days": 60,
        "risk_path_hash": "7" * 64,
        "constraint_evaluation_hash": "8" * 64,
        "constraint_passed": True,
        "peak_member_weight": 1.0,
        "current_member_weight": 1.0,
        "peak_pairwise_stock_overlap_pct": 0.0,
        "current_pairwise_stock_overlap_pct": 0.0,
        "peak_industry_weight_pct": 100.0,
        "current_industry_weight_pct": 100.0,
        "industry_snapshot_path_hash": "9" * 64,
        "industry_trade_dates_hash": "a" * 64,
        "industry_stock_code_sets_hash": "b" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    risk_binding = {
        **risk_payload,
        "binding_hash": governance_module._digest(risk_payload),
    }
    constraint_evaluation = {
        "passed": True,
        "evaluation_hash": risk_payload["constraint_evaluation_hash"],
        "drift_risk_path": {
            "risk_path_hash": risk_payload["risk_path_hash"],
        },
        "industry_snapshot_path": {
            "status": "COMPLETED",
            "window_days": 60,
            "path_hash": risk_payload["industry_snapshot_path_hash"],
            "trade_dates_hash": risk_payload["industry_trade_dates_hash"],
            "stock_code_sets_hash": risk_payload[
                "industry_stock_code_sets_hash"
            ],
        },
        "risk_binding": risk_binding,
    }
    recipe_payload = {
        "schema": "probiga.combination-member-fact-recipe.v1",
        "combination_key": "combo_ready",
        "combination_version": "v1",
        "trade_date": trade_date,
        "members": members,
        "risk_constraint_binding": risk_binding,
        "cash_fact_materialized": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    recipe_hash = governance_module._digest(recipe_payload)
    pre_recipe_hash = "c" * 64
    recipe_gate_hash = governance_module._digest({
        "schema": "probiga.combination-recipe-funding-gate.v1",
        "combination_key": "combo_ready",
        "combination_version": "v1",
        "trade_date": trade_date,
        "pre_recipe_funding_gate_hash": pre_recipe_hash,
        "recipe_hash": recipe_hash,
        "recipe_ready": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    })
    combination = {
        "combination_key": "combo_ready",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "projected_status": "ACTIVE",
        "paper_allocation_eligible": True,
        "profit_gate_passed": True,
        "market_route_eligible": True,
        "constraint_evaluation": constraint_evaluation,
        "pre_confirmation_funding_gate_hash": recipe_gate_hash,
        "statistical_family_decision": {
            "valid": True,
            "passed": True,
            "decision_hash": "d" * 64,
        },
        "confirmation_guard": {
            "valid": True,
            "passed": True,
            "compact_hash": "e" * 64,
        },
        "combination_recipe_ref": {
            **recipe_payload,
            "member_fact_sets_ready": True,
            "pre_recipe_funding_gate_hash": pre_recipe_hash,
            "recipe_hash": recipe_hash,
            "recipe_gate_hash": recipe_gate_hash,
        },
    }
    combination["funding_gate_hash"] = (
        governance_module._finalize_funding_gate_hash(
            combination, entity_type="COMBINATION",
        )
    )
    return combination


def test_monday_preclose_recovers_missed_friday_from_exact_snapshot():
    engine = _engine()
    calls = []

    def capture(_engine, *, trade_date):
        calls.append(("industry", trade_date))
        return _industry(_engine, trade_date=trade_date)

    def govern(**kwargs):
        calls.append(("governance", kwargs["trade_date"]))
        return _governance(**kwargs)

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=capture,
        governance_runner=govern,
        operator="scheduled_daily_governance",
    )

    assert result["status"] == "ok"
    assert result["orchestration_status"] == "COMPLETED"
    assert result["target_trade_date"] == "2026-08-21"
    assert calls == [
        ("industry", "2026-08-21"),
        ("governance", "2026-08-21"),
    ]


def test_completed_friday_is_not_due_on_weekend_or_monday():
    engine = _engine()
    current_sha = "1" * 40
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run
                (run_uid, trade_date, run_revision, decision_hash,
                 build_commit_sha, status, is_canonical, finished_at)
            VALUES
                ('run-friday', '2026-08-21', 1, 'decision', :build_sha,
                 'COMPLETED', 1, '2026-08-21 22:35:00')
        """), {"build_sha": current_sha})

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("NOT_DUE must not capture inputs")
        ),
        governance_runner=_governance,
        ensure_build_commit_sha=current_sha,
    )

    assert result["orchestration_status"] == NOT_DUE
    assert result["reason_code"] == "CANONICAL_ALREADY_COMPLETED"
    assert result["current_run"]["run_uid"] == "run-friday"
    assert result["current_run"]["build_commit_sha"] == current_sha


def test_release_build_revises_old_sha_canonical_for_authoritative_day():
    engine = _engine()
    old_sha = "1" * 40
    new_sha = "2" * 40
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run
                (run_uid, trade_date, run_revision, decision_hash,
                 build_commit_sha, status, is_canonical, finished_at)
            VALUES
                ('run-old-build', '2026-08-21', 1, 'old-decision', :old_sha,
                 'COMPLETED', 1, '2026-08-21 22:35:00')
        """), {"old_sha": old_sha})
    calls = []

    def capture(_engine, *, trade_date):
        calls.append(("industry", trade_date))
        return _industry(_engine, trade_date=trade_date)

    def govern(**kwargs):
        calls.append(("governance", kwargs["trade_date"]))
        return {
            **_governance(**kwargs),
            "build_commit_sha": new_sha,
        }

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=capture,
        governance_runner=govern,
        ensure_build_commit_sha=new_sha,
    )

    assert result["status"] == "ok"
    assert result["orchestration_status"] == "COMPLETED"
    assert result["build_commit_sha"] == new_sha
    assert calls == [
        ("industry", "2026-08-21"),
        ("governance", "2026-08-21"),
    ]


def test_release_build_revision_fails_closed_on_result_sha_mismatch():
    engine = _engine()
    old_sha = "1" * 40
    new_sha = "2" * 40
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run
                (run_uid, trade_date, run_revision, decision_hash,
                 build_commit_sha, status, is_canonical, finished_at)
            VALUES
                ('run-old-build', '2026-08-21', 1, 'old-decision', :old_sha,
                 'COMPLETED', 1, '2026-08-21 22:35:00')
        """), {"old_sha": old_sha})

    def wrong_build(**kwargs):
        return {
            **_governance(**kwargs),
            "build_commit_sha": old_sha,
        }

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=wrong_build,
        ensure_build_commit_sha=new_sha,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == "GOVERNANCE_BUILD_SHA_MISMATCH"
    assert result["automatic_real_order_submission"] is False


def test_governance_runner_cannot_hide_unsafe_order_authority_in_completion():
    engine = _engine()

    def unsafe(**kwargs):
        result = _governance(**kwargs)
        result["automatic_real_order_submission"] = "false"
        return result

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=unsafe,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == "GOVERNANCE_ORDER_AUTHORITY_INVALID"
    assert result["automatic_real_order_submission"] is False
    assert result["allocations"] == [{
        "target_type": "CASH",
        "target_key": "cash",
        "name": "现金",
        "simulated_weight_pct": 100.0,
        "reason": "治理完成结果的模拟资金或真实下单权限合同无效",
        "real_order_authority": False,
    }]


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        (lambda payload: payload.update(run_uid="not-a-run"),
         "GOVERNANCE_COMPLETION_IDENTITY_INVALID"),
        (lambda payload: payload.update(trade_date="2026-08-20"),
         "GOVERNANCE_COMPLETION_IDENTITY_INVALID"),
        (lambda payload: payload.pop("summary"),
         "GOVERNANCE_COMPLETION_IDENTITY_INVALID"),
        (lambda payload: payload["funding_checkpoint_manifest"].update(
            manifest_hash="d" * 64,
        ), "GOVERNANCE_COMPLETION_IDENTITY_INVALID"),
    ],
)
def test_completed_result_requires_exact_canonical_identity(
    mutation, expected_reason,
):
    engine = _engine()

    def incomplete(**kwargs):
        payload = _governance(**kwargs)
        mutation(payload)
        return payload

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=incomplete,
    )

    assert result["status"] == "blocked"
    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == expected_reason
    assert result["automatic_real_order_submission"] is False


def test_manifest_ineligible_combination_cannot_receive_simulated_funding():
    engine = _engine()

    def ineligible_funded(**kwargs):
        payload = _governance(**kwargs)
        combination = {
            "combination_key": "combo_deferred",
            "current_version": "v1",
            "paper_allocation_eligible": False,
        }
        manifest, _candidates = (
            governance_module._build_funding_checkpoint_manifest(
                run_uid=payload["run_uid"],
                trade_date=kwargs["trade_date"],
                strategies=[],
                combinations=[combination],
            )
        )
        coverage = manifest["coverage"]
        payload["combinations"] = [combination]
        payload["funding_checkpoint_manifest"] = manifest
        payload["summary"].update({
            "funding_checkpoint_manifest_hash": manifest["manifest_hash"],
            "funding_checkpoint_eligible_count": coverage["eligible_count"],
            "funding_checkpointed_count": coverage["checkpointed_count"],
            "funding_strategy_checkpoint_count": coverage[
                "strategy_checkpoint_count"
            ],
            "funding_combination_recipe_count": coverage[
                "combination_recipe_count"
            ],
            "funding_ready_count": coverage["funding_ready_count"],
            "funding_checkpoint_ineligible_count": coverage[
                "ineligible_count"
            ],
        })
        payload["allocations"] = [{
            "target_type": "COMBINATION",
            "target_key": "combo_deferred",
            "target_version": "v1",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }]
        return payload

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=ineligible_funded,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == "GOVERNANCE_COMPLETION_IDENTITY_INVALID"
    assert result["allocations"][0]["target_type"] == "CASH"


def test_compact_manifest_accepts_funding_ready_combination():
    engine = _engine()

    def eligible(**kwargs):
        payload = _replace_funding_contract(
            _governance(**kwargs),
            combinations=[_eligible_combination(kwargs["trade_date"])],
        )
        payload["allocations"] = [{
            "target_type": "COMBINATION",
            "target_key": "combo_ready",
            "target_version": "v1",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }]
        return payload

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=eligible,
    )

    assert result["orchestration_status"] == "COMPLETED"
    assert result["funding_checkpoint_manifest"]["coverage"][
        "combination_recipe_count"
    ] == 1


@pytest.mark.parametrize(
    "mutation",
    [
        lambda manifest: manifest["combination_recipe_root"].update(
            root_hash="0" * 64,
        ),
        lambda manifest: manifest["combination_recipe_root"].update(count=2),
        lambda manifest: manifest["coverage"].update(
            current_entity_count=2,
        ),
        lambda manifest: manifest["coverage"].update(
            funding_ready_set_hash="0" * 64,
        ),
        lambda manifest: manifest.update(target_total_bytes=1),
        lambda manifest: manifest.update(
            checkpoint_storage_bytes=1,
            total_storage_bytes=1,
        ),
    ],
)
def test_compact_manifest_rejects_root_count_and_partition_drift(mutation):
    engine = _engine()

    def drifted(**kwargs):
        payload = _replace_funding_contract(
            _governance(**kwargs),
            combinations=[_eligible_combination(kwargs["trade_date"])],
        )
        payload["allocations"] = [{
            "target_type": "COMBINATION",
            "target_key": "combo_ready",
            "target_version": "v1",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }]
        manifest = payload["funding_checkpoint_manifest"]
        mutation(manifest)
        manifest_payload = {
            key: value for key, value in manifest.items()
            if key != "manifest_hash"
        }
        manifest["manifest_hash"] = (
            governance_module._checkpoint_canonical_hash(manifest_payload)
        )
        payload["summary"]["funding_checkpoint_manifest_hash"] = (
            manifest["manifest_hash"]
        )
        return payload

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=drifted,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == "GOVERNANCE_COMPLETION_IDENTITY_INVALID"


@pytest.mark.parametrize("field", ["strategies", "combinations", "allocations"])
def test_completion_rejects_non_object_compact_manifest_members(field):
    engine = _engine()

    def malformed(**kwargs):
        payload = _governance(**kwargs)
        payload[field] = [None]
        return payload

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=malformed,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == "GOVERNANCE_COMPLETION_IDENTITY_INVALID"


@pytest.mark.parametrize(
    "strategies",
    [
        [{"strategy_key": "", "current_version": "v1"}],
        [
            {"strategy_key": "duplicate", "current_version": "v1"},
            {"strategy_key": "duplicate", "current_version": "v1"},
        ],
    ],
)
def test_completion_rejects_empty_or_duplicate_current_identity(strategies):
    engine = _engine()

    def malformed(**kwargs):
        payload = _governance(**kwargs)
        payload["strategies"] = strategies
        return payload

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=malformed,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == "GOVERNANCE_COMPLETION_IDENTITY_INVALID"


@pytest.mark.parametrize(
    ("row_mutation", "expected_fragment"),
    [
        (lambda row: row.update(result_hash="f" * 64), "身份"),
        (lambda row: row.update(is_canonical=0), "身份"),
        (lambda row: row.update(status="FAILED"), "身份"),
        (lambda row: row.update(build_commit_sha="f" * 40), "身份"),
    ],
)
def test_exact_readback_rejects_hash_noncanonical_status_and_build(
    monkeypatch, row_mutation, expected_fragment,
):
    engine = _engine()
    runner = _governance(trade_date="2026-08-21")
    result_json = governance_module._json_text(runner)
    row = {
        "run_uid": runner["run_uid"],
        "trade_date": runner["trade_date"],
        "input_hash": runner["input_hash"],
        "decision_hash": runner["decision_hash"],
        "build_commit_sha": runner["build_commit_sha"],
        "status": "COMPLETED",
        "is_canonical": 1,
        "result_json": result_json,
        "result_hash": runner["canonical_result_hash"],
    }
    row_mutation(row)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run
                (run_uid, trade_date, run_revision, input_hash,
                 decision_hash, build_commit_sha, status, is_canonical,
                 result_json, result_hash, finished_at)
            VALUES
                (:run_uid, :trade_date, 1, :input_hash, :decision_hash,
                 :build_commit_sha, :status, :is_canonical, :result_json,
                 :result_hash, '2026-08-21 22:35:00')
        """), row)
    called = []
    monkeypatch.setattr(
        governance_module,
        "_canonical_governance_result_from_row",
        lambda value: called.append(value) or runner,
    )

    with pytest.raises(RuntimeError, match=expected_fragment):
        orchestrator_module._exact_canonical_governance_readback(
            engine,
            runner_result=runner,
            target_trade_date="2026-08-21",
        )
    assert called == []


def test_exact_readback_returns_full_validator_output(monkeypatch):
    engine = _engine()
    runner = _governance(trade_date="2026-08-21")
    validated = {**runner, "validated_database_readback": True}
    result_json = governance_module._json_text(runner)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run
                (run_uid, trade_date, run_revision, input_hash,
                 decision_hash, build_commit_sha, status, is_canonical,
                 result_json, result_hash, finished_at)
            VALUES
                (:run_uid, :trade_date, 1, :input_hash, :decision_hash,
                 :build_commit_sha, 'COMPLETED', 1, :result_json,
                 :result_hash, '2026-08-21 22:35:00')
        """), {
            **runner,
            "result_json": result_json,
            "result_hash": runner["canonical_result_hash"],
        })
    captured = []
    monkeypatch.setattr(
        governance_module,
        "_canonical_governance_result_from_row",
        lambda row: captured.append(row) or validated,
    )

    readback = orchestrator_module._exact_canonical_governance_readback(
        engine,
        runner_result=runner,
        target_trade_date="2026-08-21",
    )

    assert readback is validated
    assert readback["validated_database_readback"] is True
    assert len(captured) == 1


def test_default_persistent_runner_returns_validated_database_readback(
    monkeypatch,
):
    engine = _engine()

    def persistent_runner(**kwargs):
        return _governance(**kwargs)

    validated = {
        **_governance(trade_date="2026-08-21"),
        "validated_database_readback": True,
    }
    captured = []
    monkeypatch.setattr(
        governance_module, "governance_snapshot", persistent_runner,
    )
    monkeypatch.setattr(
        orchestrator_module,
        "_exact_canonical_governance_readback",
        lambda _engine, **kwargs: captured.append(kwargs) or validated,
    )

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
    )

    assert result["orchestration_status"] == "COMPLETED"
    assert result["validated_database_readback"] is True
    assert captured[0]["runner_result"]["run_uid"] == "a" * 32


def test_default_persistent_runner_runtime_readback_failure_is_invalid(
    monkeypatch,
):
    engine = _engine()

    def persistent_runner(**kwargs):
        return _governance(**kwargs)

    monkeypatch.setattr(
        governance_module, "governance_snapshot", persistent_runner,
    )
    monkeypatch.setattr(
        orchestrator_module,
        "_exact_canonical_governance_readback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("canonical replay mismatch")
        ),
    )

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["reason_code"] == "GOVERNANCE_COMPLETION_IDENTITY_INVALID"
    assert result["allocations"][0]["target_type"] == "CASH"


def test_current_open_session_before_shanghai_close_is_not_due():
    engine = _engine()

    result = orchestrate_strategy_governance(
        engine=engine,
        requested_trade_date="2026-08-21",
        now=datetime(2026, 8, 21, 17, 59, 59),
        allow_revision=True,
        industry_capture=_industry,
        governance_runner=_governance,
    )

    assert result["orchestration_status"] == NOT_DUE
    assert result["reason_code"] == "SESSION_NOT_CLOSED"


def test_non_session_requested_date_is_not_due():
    engine = _engine()

    result = orchestrate_strategy_governance(
        engine=engine,
        requested_trade_date="2026-08-22",
        now=datetime(2026, 8, 22, 20, 0, 0),
        allow_revision=True,
        industry_capture=_industry,
        governance_runner=_governance,
    )

    assert result["orchestration_status"] == NOT_DUE
    assert result["reason_code"] == "NON_SESSION_DATE"


def test_missing_current_calendar_record_is_retryable_not_ready():
    engine = _engine()

    result = orchestrate_strategy_governance(
        engine=engine,
        requested_trade_date="2026-08-23",
        now=datetime(2026, 8, 23, 20, 0, 0),
        allow_revision=True,
        industry_capture=_industry,
        governance_runner=_governance,
    )

    assert result["orchestration_status"] == NOT_READY
    assert result["reason_code"] == "CALENDAR_SESSION_NOT_READY"
    assert result["retryable"] is True


def test_aware_utc_clock_is_converted_to_asia_shanghai_close():
    engine = _engine()

    result = orchestrate_strategy_governance(
        engine=engine,
        requested_trade_date="2026-08-21",
        # 10:00 UTC is 18:00 Asia/Shanghai.
        now=datetime(2026, 8, 21, 10, 0, 0, tzinfo=timezone.utc),
        allow_revision=True,
        industry_capture=_industry,
        governance_runner=_governance,
    )

    assert result["status"] == "ok"
    assert result["target_trade_date"] == "2026-08-21"


def test_missing_qmt_snapshot_is_retryable_and_block_audit_is_idempotent():
    engine = _engine()

    def unavailable(_engine, *, trade_date):
        raise IndustrySnapshotNotReady(
            f"{trade_date}的QMT精确日期行业快照尚未发布"
        )

    first = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=unavailable,
        governance_runner=_governance,
        operator="scheduled_daily_governance",
    )
    second = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=unavailable,
        governance_runner=_governance,
        operator="scheduled_daily_governance",
    )

    assert first["orchestration_status"] == NOT_READY
    assert first["retryable"] is True
    assert first["attempt_audit_persisted"] is True
    assert first["attempt_audit"]["idempotent_replay"] is False
    assert second["attempt_audit"]["idempotent_replay"] is True
    assert first["attempt_audit"]["audit_hash"] == second["attempt_audit"]["audit_hash"]
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT action, payload_json, audit_hash
            FROM st_strategy_governance_audit
        """)).mappings().all()
    assert len(rows) == 1
    assert rows[0]["action"] == "RUN_BLOCKED"
    payload = json.loads(rows[0]["payload_json"])
    assert set(payload) == {
        "entity_type", "entity_key", "action", "reason", "operator",
        "before", "after", "evidence", "nonce",
    }
    assert payload["evidence"]["reason_code"] == "QMT_INDUSTRY_SNAPSHOT_NOT_READY"
    assert len(payload["nonce"]) == 32


def test_qmt_integrity_failure_is_non_retryable_and_audited():
    engine = _engine()

    def corrupt(_engine, *, trade_date):
        raise IndustrySnapshotIntegrityError(
            f"{trade_date} canonical hash校验失败"
        )

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=corrupt,
        governance_runner=_governance,
    )

    assert result["orchestration_status"] == INTEGRITY_ERROR
    assert result["error_class"] == "INTEGRITY"
    assert result["retryable"] is False
    assert result["attempt_audit_persisted"] is True


def test_unexpected_capture_failure_is_program_error_not_not_ready():
    engine = _engine()

    def broken(_engine, *, trade_date):
        raise RuntimeError(f"bug for {trade_date}")

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=broken,
        governance_runner=_governance,
    )

    assert result["orchestration_status"] == PROGRAM_ERROR
    assert result["error_class"] == "PROGRAM"
    assert result["retryable"] is False
    assert result["reason"] == "行业快照程序执行失败（RuntimeError）"


def test_process_preflight_program_error_is_classified_and_audited():
    engine = _engine()

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        process_preflight=lambda: (_ for _ in ()).throw(
            RuntimeError("adapter seal drift")
        ),
        industry_capture=_industry,
        governance_runner=_governance,
    )

    assert result["orchestration_status"] == PROGRAM_ERROR
    assert result["reason_code"] == "ADAPTER_BOOTSTRAP_FAILED"
    assert result["attempt_audit_persisted"] is True


def test_governance_evidence_gap_is_retryable_and_binds_industry_snapshot():
    engine = _engine()

    def evidence_gap(**_kwargs):
        raise GovernanceEvidenceNotReady(
            "公司行动权威账本未建立",
            blocking_record={
                "status": "INPUT_NOT_READY",
                "reason": "公司行动权威账本未建立",
            },
        )

    result = orchestrate_strategy_governance(
        engine=engine,
        now=datetime(2026, 8, 24, 10, 0, 0),
        industry_capture=_industry,
        governance_runner=evidence_gap,
    )

    assert result["orchestration_status"] == NOT_READY
    assert result["blocking_stage"] == "GOVERNANCE_INPUT"
    assert result["industry_snapshot"]["snapshot_id"] == "a" * 64
    assert result["allocations"][0]["simulated_weight_pct"] == 100.0
