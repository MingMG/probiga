from __future__ import annotations

import pytest

from server.common import production_runtime_schema_bundle as bundle


EXPECTED_BUNDLE_CONTRACT_HASH = (
    "61f9ddfb3179f30c9976a090fce00adb8613d4e38d698c6cfc954f957084845f"
)


def test_bundle_names_are_unique_and_cover_every_seed_dependency():
    migration_names = [name for name, _ in bundle._MIGRATIONS]
    seed_names = [name for name, _ in bundle._SEEDS]
    validator_names = [name for name, _ in bundle._VALIDATORS]
    recovery_planner_names = [name for name, _ in bundle._RECOVERY_PLANNERS]
    assert len(migration_names) == len(set(migration_names))
    assert len(seed_names) == len(set(seed_names))
    assert len(validator_names) == len(set(validator_names))
    assert recovery_planner_names == [
        "ai_bridge",
        "analysis_output",
        "recommended_run_history",
        "sim_trade",
        "qmt_catalog",
        "qmt_audit",
    ]
    assert set(seed_names) <= set(migration_names)
    assert {
        "scheduler_tasks",
        "scheduler_task_history",
        "release_manifest",
        "daily_delivery",
        "auth",
        "ai_bridge",
        "versioned_strategy",
        "strategy_center",
        "sim_trade",
        "portfolio",
        "commentary_profile",
        "screener",
        "jq_minute",
        "hot_rank",
        "auxiliary_runtime",
        "qmt_stock_catalog_truth",
        "qmt_trade_calendar",
        "market_field_capture",
        "qmt_catalog",
        "qmt_audit",
    } <= set(migration_names)
    metadata = bundle._contract_metadata()
    assert metadata["schema"] == bundle.BUNDLE_CONTRACT_SCHEMA
    assert metadata["migration_count"] == len(migration_names)
    assert metadata["seed_count"] == len(seed_names)
    assert metadata["validator_count"] == len(validator_names)
    assert metadata["recovery_planner_names"] == recovery_planner_names
    assert metadata["recovery_planner_count"] == 6
    assert metadata["trigger_installation_policy"] == (
        "FROZEN_RELEASE_BROKER_ONLY"
    )
    assert metadata["broker_owned_trigger_migration_names"] == [
        "qmt_stock_catalog_truth",
        "qmt_trade_calendar",
        "market_field_capture",
        "auxiliary_runtime",
    ]
    assert metadata["contract_hash"] == EXPECTED_BUNDLE_CONTRACT_HASH


def test_broker_owned_bundle_migrations_explicitly_disable_trigger_ddl(
    monkeypatch,
):
    calls: list[tuple[str, bool]] = []
    for function_name, wrapper, label in (
        (
            "privileged_migrate_stock_catalog_schema",
            bundle._migrate_stock_catalog_tables,
            "stock_catalog",
        ),
        (
            "privileged_migrate_trade_calendar_schema",
            bundle._migrate_trade_calendar_tables,
            "trade_calendar",
        ),
        (
            "privileged_migrate_market_field_capture_schema",
            bundle._migrate_market_field_capture_tables,
            "market_field_capture",
        ),
        (
            "privileged_migrate_auxiliary_runtime_schema",
            bundle._migrate_auxiliary_runtime_tables,
            "auxiliary_runtime",
        ),
    ):
        monkeypatch.setattr(
            bundle,
            function_name,
            lambda _engine, *, install_triggers, label=label: calls.append(
                (label, install_triggers)
            ),
        )
        wrapper(object())

    assert calls == [
        ("stock_catalog", False),
        ("trade_calendar", False),
        ("market_field_capture", False),
        ("auxiliary_runtime", False),
    ]


def test_recovery_planner_registry_names_and_count_are_hash_bound(monkeypatch):
    original = bundle._contract_metadata()
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        bundle._RECOVERY_PLANNERS[:-1],
    )

    changed = bundle._contract_metadata()

    assert changed["recovery_planner_count"] == 5
    assert changed["recovery_planner_names"] == [
        "ai_bridge",
        "analysis_output",
        "recommended_run_history",
        "sim_trade",
        "qmt_catalog",
    ]
    assert changed["contract_hash"] != original["contract_hash"]


def test_privileged_bundle_runs_migrations_then_seeds_then_read_only_validation(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(
        bundle,
        "_MIGRATIONS",
        (("migrate", lambda _engine: events.append("migrate")),),
    )
    monkeypatch.setattr(
        bundle,
        "_SEEDS",
        (("seed", lambda _engine: events.append("seed")),),
    )
    monkeypatch.setattr(
        bundle,
        "validate_runtime_schema_bundle",
        lambda _engine: events.append("validate") or {"read_only": True},
    )
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        ((
            "receipt",
            lambda _engine: events.append("plan") or {
                "plan_sha256": "a" * 64,
                "ready_for_privileged_apply": True,
            },
        ),),
    )
    result = bundle.privileged_migrate_runtime_schema_bundle(object())
    assert events == ["migrate", "seed", "validate", "plan"]
    assert result["runtime_validation"]["read_only"] is True
    assert result["recovery_plan_count"] == 1
    assert result["recovery_ready_for_privileged_apply"] is True
    assert result["privileged_migration"] is True


def test_privileged_bundle_can_defer_trigger_validation_until_broker(
    monkeypatch,
):
    events: list[str] = []
    monkeypatch.setattr(
        bundle,
        "_MIGRATIONS",
        (("migrate", lambda _engine: events.append("migrate")),),
    )
    monkeypatch.setattr(bundle, "_SEEDS", ())
    monkeypatch.setattr(bundle, "_RECOVERY_PLANNERS", ())
    monkeypatch.setattr(
        bundle,
        "validate_runtime_schema_bundle",
        lambda _engine: events.append("validate"),
    )

    result = bundle.privileged_migrate_runtime_schema_bundle(
        object(),
        defer_trigger_validation=True,
    )

    assert events == ["migrate"]
    assert result["trigger_validation_deferred"] is True
    assert result["runtime_validation"]["trigger_validation_deferred"] is True
    assert result["runtime_validation"]["required_surface_verified"] is False


def test_runtime_validation_executes_only_validator_registry(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (
            ("first", lambda _engine: calls.append("first")),
            ("second", lambda _engine: calls.append("second") or {"ok": True}),
        ),
    )
    result = bundle.validate_runtime_schema_bundle(object())
    assert calls == ["first", "second"]
    assert result["contract_count"] == 2
    assert result["read_only"] is True


def test_preflight_reports_migration_without_leaking_exception_message(monkeypatch):
    def fail(_engine):
        raise RuntimeError("secret database detail")

    monkeypatch.setattr(bundle, "_VALIDATORS", (("broken", fail),))
    result = bundle.preflight_runtime_schema_bundle(object())
    assert result["migration_required"] is True
    assert result["contracts"]["broken"] == {
        "status": "MIGRATION_REQUIRED",
        "error_type": "RuntimeError",
        "read_only": True,
    }


def test_preflight_marks_successful_validator_contract_as_read_only(monkeypatch):
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"physical_contract_verified": True}),),
    )
    monkeypatch.setattr(bundle, "_RECOVERY_PLANNERS", ())

    result = bundle.preflight_runtime_schema_bundle(object())

    assert result["contracts"]["ready"] == {
        "physical_contract_verified": True,
        "status": "READY",
        "read_only": True,
    }


def test_preflight_includes_read_only_recovery_plans(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        ((
            "legacy",
            lambda _engine: calls.append("legacy") or {
                "plan_sha256": "a" * 64,
                "ready_for_privileged_apply": True,
                "read_only": True,
            },
        ),),
    )

    result = bundle.preflight_runtime_schema_bundle(object())

    assert calls == ["legacy"]
    assert result["recovery_plan_count"] == 1
    assert result["recovery_plans"]["legacy"] == {
        "plan_sha256": "a" * 64,
        "ready_for_privileged_apply": True,
        "read_only": True,
        "status": "PLANNED",
    }
    assert result["recovery_ready_for_privileged_apply"] is True
    assert result["migration_required"] is False


def test_preflight_uses_recovery_bundle_hash_as_canonical_plan_hash(monkeypatch):
    atomic_hash = "a" * 64
    bundle_hash = "b" * 64
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        ((
            "dual_hash",
            lambda _engine: {
                "plan_sha256": atomic_hash,
                "recovery_bundle_sha256": bundle_hash,
                "ready_for_privileged_apply": True,
                "read_only": True,
            },
        ),),
    )

    result = bundle.preflight_runtime_schema_bundle(object())

    plan = result["recovery_plans"]["dual_hash"]
    assert plan["status"] == "PLANNED"
    assert plan["plan_sha256"] == bundle_hash
    assert plan["recovery_bundle_sha256"] == bundle_hash
    assert plan["atomic_plan_sha256"] == atomic_hash
    assert result["recovery_ready_for_privileged_apply"] is True
    assert result["migration_required"] is False


def test_preflight_rejects_each_invalid_declared_recovery_hash(monkeypatch):
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    invalid_plans = (
        {
            "plan_sha256": "not-a-sha256",
            "recovery_bundle_sha256": "b" * 64,
            "ready_for_privileged_apply": True,
            "read_only": True,
        },
        {
            "plan_sha256": "a" * 64,
            "recovery_bundle_sha256": "not-a-sha256",
            "ready_for_privileged_apply": True,
            "read_only": True,
        },
    )

    for invalid_plan in invalid_plans:
        monkeypatch.setattr(
            bundle,
            "_RECOVERY_PLANNERS",
            (("dual_hash", lambda _engine, plan=invalid_plan: plan),),
        )

        result = bundle.preflight_runtime_schema_bundle(object())

        assert result["recovery_plans"]["dual_hash"] == {
            "status": "PLAN_UNAVAILABLE",
            "error_type": "RuntimeError",
            "plan_sha256": None,
            "ready_for_privileged_apply": False,
            "read_only": True,
        }
        assert result["recovery_ready_for_privileged_apply"] is False
        assert result["migration_required"] is True


@pytest.mark.parametrize(
    "invalid_plan",
    (
        {
            "plan_sha256": "a" * 64,
            "atomic_plan_sha256": "not-a-sha256",
        },
        {
            "plan_sha256": "a" * 64,
            "recovery_bundle_sha256": "b" * 64,
            "atomic_plan_sha256": "c" * 64,
        },
        {
            "plan_sha256": "a" * 64,
            "atomic_plan_sha256": "b" * 64,
        },
        {
            "recovery_bundle_sha256": "b" * 64,
            "atomic_plan_sha256": "a" * 64,
        },
    ),
)
def test_preflight_rejects_invalid_or_inconsistent_declared_atomic_hash(
    monkeypatch,
    invalid_plan,
):
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        ((
            "dual_hash",
            lambda _engine: {
                **invalid_plan,
                "ready_for_privileged_apply": True,
                "read_only": True,
            },
        ),),
    )

    result = bundle.preflight_runtime_schema_bundle(object())

    assert result["recovery_plans"]["dual_hash"] == {
        "status": "PLAN_UNAVAILABLE",
        "error_type": "RuntimeError",
        "plan_sha256": None,
        "ready_for_privileged_apply": False,
        "read_only": True,
    }
    assert result["recovery_ready_for_privileged_apply"] is False
    assert result["migration_required"] is True


def test_preflight_accepts_declared_atomic_hash_matching_source_plan(monkeypatch):
    atomic_hash = "a" * 64
    bundle_hash = "b" * 64
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        ((
            "dual_hash",
            lambda _engine: {
                "plan_sha256": atomic_hash,
                "recovery_bundle_sha256": bundle_hash,
                "atomic_plan_sha256": atomic_hash,
                "ready_for_privileged_apply": True,
                "read_only": True,
            },
        ),),
    )

    result = bundle.preflight_runtime_schema_bundle(object())

    plan = result["recovery_plans"]["dual_hash"]
    assert plan["status"] == "PLANNED"
    assert plan["plan_sha256"] == bundle_hash
    assert plan["atomic_plan_sha256"] == atomic_hash
    assert result["recovery_ready_for_privileged_apply"] is True


def test_preflight_plan_failure_is_not_publishable_and_does_not_leak_message(
    monkeypatch,
):
    def fail(_engine):
        raise RuntimeError("secret recovery detail")

    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    monkeypatch.setattr(bundle, "_RECOVERY_PLANNERS", (("broken", fail),))

    result = bundle.preflight_runtime_schema_bundle(object())

    assert result["migration_required"] is True
    assert result["recovery_ready_for_privileged_apply"] is False
    assert result["recovery_plans"]["broken"] == {
        "status": "PLAN_UNAVAILABLE",
        "error_type": "RuntimeError",
        "plan_sha256": None,
        "ready_for_privileged_apply": False,
        "read_only": True,
    }


def test_preflight_rejects_invalid_or_unsafe_recovery_plan(monkeypatch):
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        ((
            "unsafe",
            lambda _engine: {
                "plan_sha256": "not-a-sha256",
                "ready_for_privileged_apply": False,
                "read_only": True,
            },
        ),),
    )

    result = bundle.preflight_runtime_schema_bundle(object())

    assert result["migration_required"] is True
    assert result["recovery_ready_for_privileged_apply"] is False
    assert result["recovery_plans"]["unsafe"]["status"] == "PLAN_UNAVAILABLE"


def test_preflight_keeps_safe_hash_but_blocks_plan_not_ready_for_apply(monkeypatch):
    monkeypatch.setattr(
        bundle,
        "_VALIDATORS",
        (("ready", lambda _engine: {"read_only": True}),),
    )
    monkeypatch.setattr(
        bundle,
        "_RECOVERY_PLANNERS",
        ((
            "manual",
            lambda _engine: {
                "plan_sha256": "b" * 64,
                "ready_for_privileged_apply": False,
                "read_only": True,
            },
        ),),
    )

    result = bundle.preflight_runtime_schema_bundle(object())

    assert result["recovery_plans"]["manual"]["status"] == "PLANNED"
    assert result["recovery_plans"]["manual"]["plan_sha256"] == "b" * 64
    assert result["recovery_ready_for_privileged_apply"] is False
    assert result["migration_required"] is True
