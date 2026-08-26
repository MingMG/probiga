from __future__ import annotations

from server.common import production_runtime_schema_bundle as bundle


EXPECTED_BUNDLE_CONTRACT_HASH = (
    "7b4e2b261e0a8b2ad0c07c6c536574b4abc64022f251f2c104190009d0c36c3e"
)


def test_bundle_names_are_unique_and_cover_every_seed_dependency():
    migration_names = [name for name, _ in bundle._MIGRATIONS]
    seed_names = [name for name, _ in bundle._SEEDS]
    validator_names = [name for name, _ in bundle._VALIDATORS]
    assert len(migration_names) == len(set(migration_names))
    assert len(seed_names) == len(set(seed_names))
    assert len(validator_names) == len(set(validator_names))
    assert set(seed_names) <= set(migration_names)
    assert {
        "scheduler_tasks",
        "scheduler_task_history",
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
        "qmt_catalog",
        "qmt_audit",
    } <= set(migration_names)
    metadata = bundle._contract_metadata()
    assert metadata["schema"] == bundle.BUNDLE_CONTRACT_SCHEMA
    assert metadata["migration_count"] == len(migration_names)
    assert metadata["seed_count"] == len(seed_names)
    assert metadata["validator_count"] == len(validator_names)
    assert metadata["contract_hash"] == EXPECTED_BUNDLE_CONTRACT_HASH


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
    result = bundle.privileged_migrate_runtime_schema_bundle(object())
    assert events == ["migrate", "seed", "validate"]
    assert result["runtime_validation"]["read_only"] is True
    assert result["privileged_migration"] is True


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
