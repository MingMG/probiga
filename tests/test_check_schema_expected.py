from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools" / "check_schema_expected.py"
SPEC = importlib.util.spec_from_file_location("release_schema_guard", TOOL)
schema_guard = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = schema_guard
SPEC.loader.exec_module(schema_guard)


def test_ready_schema_guard_uses_code_owned_validators_without_writes():
    calls: list[object] = []

    def validator(engine):
        calls.append(engine)
        return {"physical_schema_verified": True}

    result = schema_guard.collect_schema_guard(
        object(),
        validators=(("first", validator), ("second", validator)),
    )

    assert result["schema"] == "probiga.release-schema-guard.v1"
    assert result["status"] == "READY"
    assert result["read_only"] is True
    assert result["check_count"] == 2
    assert result["mismatch_count"] == 0
    assert len(calls) == 2
    assert all(item == {"status": "READY", "read_only": True}
               for item in result["checks"].values())


def test_mismatch_identifies_the_contract_without_leaking_exception_text():
    def missing_column(engine):
        raise RuntimeError("ALTER TABLE private_table ADD secret_column")

    result = schema_guard.collect_schema_guard(
        object(),
        validators=(("scheduler_tasks", missing_column),),
    )

    assert result["status"] == "MIGRATION_REQUIRED"
    assert result["mismatch_count"] == 1
    assert result["checks"]["scheduler_tasks"] == {
        "status": "MIGRATION_REQUIRED",
        "error_type": "RuntimeError",
        "read_only": True,
    }
    encoded = json.dumps(result)
    assert "private_table" not in encoded
    assert "secret_column" not in encoded


def test_empty_or_duplicate_required_schema_inventory_fails_closed():
    try:
        schema_guard.collect_schema_guard(object(), validators=())
    except schema_guard.SchemaGuardError as exc:
        assert str(exc) == "required_schema_inventory_empty"
    else:
        raise AssertionError("empty schema inventory was accepted")

    validator = lambda engine: None
    try:
        schema_guard.collect_schema_guard(
            object(),
            validators=(("duplicate", validator), ("duplicate", validator)),
        )
    except schema_guard.SchemaGuardError as exc:
        assert str(exc) == "required_schema_inventory_duplicate"
    else:
        raise AssertionError("duplicate schema inventory was accepted")


def test_fast_dispatch_runs_schema_guard_before_linux_activation():
    script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    guard_function = script.index("check_fast_release_schema() {")
    dispatch = script.index(
        'if [ "$RELEASE_EXECUTION_SCOPE" = LINUX ]; then',
        script.index("CUTOVER_STEP=prepare_release"),
    )
    guard = script.index("check_fast_release_schema", dispatch)
    activation = script.index("activate_linux_release", guard)
    assert guard < activation
    assert "retry with --full" in script[guard_function:dispatch]


def test_schema_guard_boundary_includes_trigger_metadata_authority():
    source = TOOL.read_text(encoding="utf-8")
    function = source[
        source.index("def _open_read_only_boundary()"):
        source.index("def _parser()")
    ]
    assert "include_migrator=True" in function
    assert "boundary.migrator_engine" in source
