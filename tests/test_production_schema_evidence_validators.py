from __future__ import annotations

import copy
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from server.common.production_runtime_schema_bundle import _contract_metadata
from tools.prepare_strategy_governance_schema import (
    EXPECTED_GOVERNANCE_TRIGGER_NAMES,
    _non_v3_trigger_contracts,
)
from tools.sync_guojin_qmt_reference_data import (
    REFERENCE_TABLE_NAMES,
    REFERENCE_TRIGGER_NAMES,
)


ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT = ROOT / "deploy" / "production_deploy.sh"

FUNDING_CONTRACT_HASH = (
    "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
)
GOVERNANCE_SOURCE_HASH = (
    "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
)
SUPPORTING_SOURCE_HASH = (
    "4a59e6364edc9191dc08131e1806fe58ddf5231b41a4bd7627606d024a6c5175"
)
PIT_CONTRACT_HASH = (
    "c374e0ba62eb2e5b9bef802ce2bdd89fae0c63391d918e922ff21781707863ae"
)
QMT_REFERENCE_CONTRACT_HASH = (
    "64982c16c517f7e5c0e6ee9b88b1bf33df98f9aebf66440eedc916eae76f3dd5"
)
APPEND_PHYSICAL_HASH = (
    "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8"
)
METRIC_PHYSICAL_HASH = (
    "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28"
)
CORE_APPEND_HASH = (
    "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
)
CORE_METRIC_HASH = (
    "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
)

SUPPORTING_OWNER_COUNTS = {
    "pit_facts": 6,
    "qmt_attestation": 6,
    "qmt_history_coverage": 4,
    "qmt_reference": 10,
    "scheduler_task_history": 2,
    "strategy_governance": 40,
}
GOVERNANCE_TRIGGER_NAMES = sorted(EXPECTED_GOVERNANCE_TRIGGER_NAMES)
SUPPORTING_TRIGGER_NAMES = sorted(_non_v3_trigger_contracts())
QMT_TABLE_NAMES = list(REFERENCE_TABLE_NAMES)
QMT_TRIGGER_NAMES = list(REFERENCE_TRIGGER_NAMES)
RUNTIME_BUNDLE_METADATA = _contract_metadata()


def _runtime_bundle_validation() -> dict[str, Any]:
    contracts = {
        name: {"read_only": True}
        for name in RUNTIME_BUNDLE_METADATA["validator_names"]
    }
    return {
        **RUNTIME_BUNDLE_METADATA,
        "contracts": contracts,
        "contract_count": len(contracts),
        "required_surface_verified": True,
        "read_only": True,
    }


def _runtime_bundle_migration() -> dict[str, Any]:
    return {
        **RUNTIME_BUNDLE_METADATA,
        "migrations": {
            name: {"privileged_migration": True}
            for name in RUNTIME_BUNDLE_METADATA["migration_names"]
        },
        "seeds": {
            name: {"privileged_seed": True}
            for name in RUNTIME_BUNDLE_METADATA["seed_names"]
        },
        "runtime_validation": _runtime_bundle_validation(),
        "runtime_ddl_required": False,
        "privileged_migration": True,
    }


def _runtime_bundle_preflight() -> dict[str, Any]:
    contracts = {
        name: {"status": "READY", "read_only": True}
        for name in RUNTIME_BUNDLE_METADATA["validator_names"]
    }
    return {
        **RUNTIME_BUNDLE_METADATA,
        "contracts": contracts,
        "contract_count": len(contracts),
        "migration_required": False,
        "read_only": True,
    }


def _extract_python_validator(function_name: str) -> str:
    """Extract the program that the shell function passes to ``python -I -c``."""

    source = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    function_anchor = f"{function_name}() {{\n"
    function_start = source.index(function_anchor)
    invocation = '"$python_bin" -I -c \\\n    \'\n'
    program_start = source.index(invocation, function_start) + len(invocation)
    program_end = source.index("\n'\n}", program_start)
    program = source[program_start:program_end]
    assert "json.load(sys.stdin)" in program
    assert "raise SystemExit(0 if ok else 2)" in program
    return program


@pytest.fixture(scope="module")
def validator_programs() -> dict[str, str]:
    return {
        "resume": _extract_python_validator(
            "controlled_guard_validate_resume_json"
        ),
        "preflight": _extract_python_validator(
            "controlled_guard_validate_preflight_json"
        ),
    }


def _run_validator(program: str, payload: dict[str, Any]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-c", program],
        input=json.dumps(payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )


def _runtime_grant_summary(*, legacy: bool = False) -> dict[str, Any]:
    persistent_ddl = (
        ["ALTER", "CREATE", "DROP", "INDEX", "REFERENCES"]
        if legacy
        else []
    )
    probiga_privileges = [
        "CREATE TEMPORARY TABLES",
        "DELETE",
        "INSERT",
        "SELECT",
        "UPDATE",
    ]
    if legacy:
        probiga_privileges = [
            "ALTER",
            "CREATE",
            "CREATE TEMPORARY TABLES",
            "DELETE",
            "DROP",
            "INDEX",
            "INSERT",
            "REFERENCES",
            "SELECT",
            "UPDATE",
        ]
    return {
        "observed_contract": (
            "LEGACY_DDL_COMPATIBILITY"
            if legacy
            else "TARGET_LEAST_PRIVILEGE"
        ),
        "persistent_ddl_privileges": persistent_ddl,
        "global_privileges": ["USAGE"],
        "schema_privileges": {
            "BIGA.*": ["SELECT"],
            "PROBIGA.*": probiga_privileges,
            "PROBIGA_QMT_HISTORY.*": ["SELECT"],
        },
        "funding_append_only_tables": [
            "st_strategy_funding_checkpoint",
            "st_strategy_funding_daily_fact",
        ],
        "funding_append_only_verified": True,
        "funding_row_mutation_denied_by_triggers": ["DELETE", "UPDATE"],
        "funding_structural_bypass_privileges": persistent_ddl,
        "truncate_denied_by_absent_drop_privilege": not legacy,
        "trigger_drop_denied_by_absent_trigger_privilege": True,
        "require_ssl": True,
        "roles": [],
        "grant_option": False,
    }


def _common_payload(phase: str) -> dict[str, Any]:
    return {
        "status": "ok",
        "phase": phase,
        "runtime_privilege_boundary_verified": True,
        "runtime_least_privilege_verified": True,
        "runtime_legacy_ddl_compatibility": False,
        "runtime_grant_summary": _runtime_grant_summary(),
        "runtime_definer_routine_count": 0,
        "runtime_definer_routine_inventory_verified": True,
        "legacy_binding_plan": {
            "legacy_run_count": 0,
            "legacy_binding_plan_hash": "no-legacy-runs",
            "legacy_binding_pending": False,
            "legacy_binding_marker_present": False,
        },
        "pit_fact_schema": {
            "schema": "probiga.pit-fact-schema-health.v1",
            "status": "HEALTHY",
            "valid": True,
            "table_count": 3,
            "trigger_count": 6,
            "missing_tables": [],
            "missing_columns": {},
            "missing_triggers": [],
            "contract_hash": PIT_CONTRACT_HASH,
        },
        "automatic_real_order_submission": False,
    }


def _governance_source(*, resume: bool) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "source_contract_hash": GOVERNANCE_SOURCE_HASH,
        "append_only_physical_contract_hash": APPEND_PHYSICAL_HASH,
        "metric_review_physical_contract_hash": METRIC_PHYSICAL_HASH,
        "core_append_only_contract_hash": CORE_APPEND_HASH,
        "core_metric_review_contract_hash": CORE_METRIC_HASH,
        "funding_schema_contract_hash": FUNDING_CONTRACT_HASH,
    }
    if resume:
        detail.update(
            {
                "observed_count": 40,
                "required_count": 40,
                "created_count": 0,
                "created_names": [],
                "expected_names": GOVERNANCE_TRIGGER_NAMES,
            }
        )
    else:
        detail.update(
            {
                "trigger_count": 40,
                "trigger_names": GOVERNANCE_TRIGGER_NAMES,
            }
        )
    return detail


def _supporting_source(*, resume: bool) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "source_contract_hash": SUPPORTING_SOURCE_HASH,
        "owner_counts": SUPPORTING_OWNER_COUNTS,
    }
    if resume:
        detail.update(
            {
                "required_count": 68,
                "optional_count": 0,
                "observed_count": 68,
                "definer": "probiga_migrator@127.0.0.1",
                "metadata_frozen": True,
                "legacy_rehome_names": [],
                "created_count": 0,
                "created_names": [],
                "expected_names": SUPPORTING_TRIGGER_NAMES,
            }
        )
    else:
        detail.update(
            {
                "trigger_count": 68,
                "trigger_names": SUPPORTING_TRIGGER_NAMES,
            }
        )
    return detail


def _resume_payload() -> dict[str, Any]:
    payload = _common_payload("resume")
    payload.update(
        {
            "v3_migrations": [{"status": "applied"}],
            "trigger_contract": {
                "metadata_frozen": True,
                "legacy_rehome_names": [],
                "definer": "probiga_migrator@127.0.0.1",
                "required_count": 88,
                "optional_count": 0,
                "observed_count": 88,
            },
            "legacy_trigger_repair": {
                "candidate_names": [],
                "repaired_names": [],
                "post_validation_verified": True,
            },
            "trigger_trust_window_names": [],
            "trigger_trust_window_count": 0,
            "global_trust_changed": False,
            "trust_restoration_verified": True,
            "restore_primary_verified": True,
            "restore_secondary_verified": True,
            "runtime_trust_off_verified": True,
            "funding_checkpoint_schema": {
                "table_count": 2,
                "tables": {
                    "st_strategy_funding_daily_fact": {
                        "column_count": 29,
                        "index_count": 9,
                        "foreign_key_count": 3,
                        "check_count": 7,
                    },
                    "st_strategy_funding_checkpoint": {
                        "column_count": 46,
                        "index_count": 12,
                        "foreign_key_count": 7,
                        "check_count": 13,
                    },
                },
                "trigger_count": 4,
                "contract_hash": FUNDING_CONTRACT_HASH,
                "trigger_contract_hash": "a" * 64,
                "daily_path_base_authoritative_sessions": 120,
                "daily_path_max_incremental_replay_sessions": 370,
                "maximum_holding_buffer_sessions": 250,
                "bootstrap_mode": (
                    "EXPLICIT_FULL_HISTORY_ONCE_PER_VERSION_ACCOUNT"
                ),
                "bootstrap_is_bounded": False,
                "rolling_history_storage": (
                    "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN"
                ),
                "checkpoint_target_average_bytes": 8192,
                "checkpoint_total_target_bytes": 8388608,
                "checkpoint_total_hard_bytes": 16777216,
                "batch_max_rows": 100,
                "batch_max_bytes": 4194304,
                "manifest_max_bytes": 1048576,
                "audit_max_bytes": 131072,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            },
            "funding_checkpoint_contract_hash": FUNDING_CONTRACT_HASH,
            "funding_checkpoint_table_count": 2,
            "funding_checkpoint_trigger_count": 4,
            "governance_append_only_trigger_count": 38,
            "governance_metric_review_trigger_count": 2,
            "governance_trigger_count": 40,
            "governance_trigger_source_contract_hash": GOVERNANCE_SOURCE_HASH,
            "governance_append_only_physical_contract_hash": (
                APPEND_PHYSICAL_HASH
            ),
            "governance_metric_review_physical_contract_hash": (
                METRIC_PHYSICAL_HASH
            ),
            "governance_append_only_core_contract_hash": CORE_APPEND_HASH,
            "governance_metric_review_core_contract_hash": CORE_METRIC_HASH,
            "governance_trigger_source_contract": _governance_source(
                resume=True
            ),
            "supporting_trigger_source_contract": _supporting_source(
                resume=True
            ),
            "qmt_reference_schema": {
                "contract_key": "qmt_reference_truth_v2",
                "contract_hash": QMT_REFERENCE_CONTRACT_HASH,
                "table_names": QMT_TABLE_NAMES,
                "trigger_names": QMT_TRIGGER_NAMES,
                "table_ddl_count": 5,
                "migration_ddl_count": 14,
                "runtime_ddl_required": False,
                "table_contract_hash": "b" * 64,
                "trigger_contract_hash": "c" * 64,
            },
            "qmt_history_coverage_schema": {
                "database": "probiga",
                "table_count": 2,
                "foreign_key_count": 3,
                "trigger_count": 4,
                "runtime_ddl_required": False,
                "physical_schema_verified": True,
                "physical_seal_verified": True,
            },
            "qmt_history_coverage_runtime_schema": {
                "database": "probiga",
                "table_count": 2,
                "trigger_count": 4,
                "physical_schema_verified": True,
                "physical_seal_verified": True,
            },
            "scheduler_task_history_schema": {
                "table": "st_scheduled_task_history",
                "status": "ok",
                "required_index_count": 3,
                "physical_contract_verified": True,
                "runtime_ddl_required": False,
                "runtime_validation": {
                    "physical_contract_verified": True,
                },
            },
            "scheduler_task_history_runtime_schema": {
                "table": "st_scheduled_task_history",
                "required_index_count": 3,
                "physical_contract_verified": True,
                "runtime_ddl_required": False,
                "read_only": True,
            },
            "runtime_schema_bundle": _runtime_bundle_migration(),
            "runtime_schema_bundle_validation": (
                _runtime_bundle_validation()
            ),
            "seeded_strategy_count": 1,
        }
    )
    return payload


def _preflight_payload() -> dict[str, Any]:
    payload = _common_payload("preflight")
    payload.update(
        {
            "v3_migrations": [{"status": "exists"}],
            "pending_v3_versions": [],
            "trigger_contract": {
                "metadata_frozen": True,
                "legacy_rehome_names": [],
                "definer": "probiga_migrator@127.0.0.1",
                "required_count": 20,
                "optional_count": 68,
                "observed_count": 88,
            },
            "governance_trigger_source_contract": _governance_source(
                resume=False
            ),
            "supporting_trigger_source_contract": _supporting_source(
                resume=False
            ),
            "qmt_reference_schema": {
                "status": "READY_FOR_PHYSICAL_ATTESTATION",
                "read_only": True,
                "contract_key": "qmt_reference_truth_v2",
                "contract_hash": QMT_REFERENCE_CONTRACT_HASH,
                "table_names": QMT_TABLE_NAMES,
                "trigger_names": QMT_TRIGGER_NAMES,
                "present_tables": QMT_TABLE_NAMES,
                "absent_tables": [],
                "missing_columns": {},
                "table_ddl_count": 5,
                "migration_ddl_count": 14,
                "trigger_ddl_count": 10,
            },
            "qmt_history_coverage_schema": {
                "status": "READY_FOR_TRIGGER_CUTOVER",
                "database": "probiga",
                "table_names": [
                    "qmt_history_coverage_manifest",
                    "qmt_history_coverage_entity",
                ],
                "trigger_names": [
                    "trg_qmt_history_coverage_no_update",
                    "trg_qmt_history_coverage_no_delete",
                    "trg_qmt_history_coverage_entity_no_update",
                    "trg_qmt_history_coverage_entity_no_delete",
                ],
                "runtime_ddl_required": False,
                "read_only": True,
                "table_count": 2,
                "foreign_key_count": 3,
                "physical_schema_verified": True,
            },
            "scheduler_task_history_schema": {
                "table": "st_scheduled_task_history",
                "status": "READY",
                "runtime_ddl_required": False,
                "read_only": True,
                "required_index_count": 3,
                "physical_contract_verified": True,
            },
            "runtime_schema_bundle": _runtime_bundle_preflight(),
            "global_trust_changed": False,
            "trust_restoration_verified": True,
            "qmt_table_count": 4,
            "governance_table_count": 15,
        }
    )
    return payload


PayloadFactory = Callable[[], dict[str, Any]]
Mutator = Callable[[dict[str, Any]], None]


def _set_path(*path_and_value: Any) -> Mutator:
    *path, value = path_and_value

    def mutate(payload: dict[str, Any]) -> None:
        cursor: dict[str, Any] = payload
        for key in path[:-1]:
            cursor = cursor[key]
        cursor[path[-1]] = value

    return mutate


def _drop_last(path: tuple[str, ...]) -> Mutator:
    def mutate(payload: dict[str, Any]) -> None:
        cursor: Any = payload
        for key in path:
            cursor = cursor[key]
        cursor.pop()

    return mutate


VALIDATORS: tuple[tuple[str, PayloadFactory], ...] = (
    ("resume", _resume_payload),
    ("preflight", _preflight_payload),
)


@pytest.mark.parametrize(("phase", "payload_factory"), VALIDATORS)
def test_schema_evidence_validator_accepts_complete_payload(
    validator_programs: dict[str, str],
    phase: str,
    payload_factory: PayloadFactory,
) -> None:
    result = _run_validator(validator_programs[phase], payload_factory())

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(("phase", "payload_factory"), VALIDATORS)
def test_schema_evidence_validator_accepts_only_frozen_legacy_compatibility(
    validator_programs: dict[str, str],
    phase: str,
    payload_factory: PayloadFactory,
) -> None:
    payload = payload_factory()
    payload["runtime_grant_summary"] = _runtime_grant_summary(legacy=True)
    payload["runtime_least_privilege_verified"] = False
    payload["runtime_legacy_ddl_compatibility"] = True

    result = _run_validator(validator_programs[phase], payload)

    assert result.returncode == 0, result.stderr


COMMON_MUTATIONS: tuple[tuple[str, Mutator], ...] = (
    (
        "runtime_contract_label",
        _set_path(
            "runtime_grant_summary",
            "observed_contract",
            "LEGACY_DDL_COMPATIBILITY",
        ),
    ),
    (
        "runtime_persistent_ddl_evidence",
        _set_path(
            "runtime_grant_summary",
            "persistent_ddl_privileges",
            ["CREATE"],
        ),
    ),
    (
        "runtime_partial_legacy_schema",
        _set_path(
            "runtime_grant_summary",
            "schema_privileges",
            {
                "BIGA.*": ["SELECT"],
                "PROBIGA.*": [
                    "CREATE",
                    "CREATE TEMPORARY TABLES",
                    "DELETE",
                    "INSERT",
                    "SELECT",
                    "UPDATE",
                ],
                "PROBIGA_QMT_HISTORY.*": ["SELECT"],
            },
        ),
    ),
    (
        "runtime_bundle_contract_hash",
        _set_path("runtime_schema_bundle", "contract_hash", "0" * 64),
    ),
    (
        "supporting_owner_count",
        _set_path(
            "supporting_trigger_source_contract",
            "owner_counts",
            {**SUPPORTING_OWNER_COUNTS, "qmt_reference": 9},
        ),
    ),
    (
        "supporting_source_hash",
        _set_path(
            "supporting_trigger_source_contract",
            "source_contract_hash",
            "0" * 64,
        ),
    ),
    (
        "aggregate_observed_count",
        _set_path("trigger_contract", "observed_count", 81),
    ),
    (
        "pit_contract_hash",
        _set_path("pit_fact_schema", "contract_hash", "0" * 64),
    ),
    (
        "pit_trigger_count",
        _set_path("pit_fact_schema", "trigger_count", 5),
    ),
    (
        "qmt_contract_hash",
        _set_path("qmt_reference_schema", "contract_hash", "0" * 64),
    ),
    (
        "qmt_table_list",
        _drop_last(("qmt_reference_schema", "table_names")),
    ),
)


@pytest.mark.parametrize(("phase", "payload_factory"), VALIDATORS)
@pytest.mark.parametrize(("case_name", "mutate"), COMMON_MUTATIONS)
def test_schema_evidence_validator_rejects_common_tampering(
    validator_programs: dict[str, str],
    phase: str,
    payload_factory: PayloadFactory,
    case_name: str,
    mutate: Mutator,
) -> None:
    payload = copy.deepcopy(payload_factory())
    mutate(payload)

    result = _run_validator(validator_programs[phase], payload)

    assert result.returncode == 2, (
        phase,
        case_name,
        result.returncode,
        result.stdout,
        result.stderr,
    )


PHASE_MUTATIONS: tuple[tuple[str, PayloadFactory, str, Mutator], ...] = (
    (
        "resume",
        _resume_payload,
        "supporting_68_trigger_inventory",
        _drop_last(("supporting_trigger_source_contract", "expected_names")),
    ),
    (
        "resume",
        _resume_payload,
        "aggregate_required_count",
        _set_path("trigger_contract", "required_count", 87),
    ),
    (
        "resume",
        _resume_payload,
        "qmt_physical_hash",
        _set_path("qmt_reference_schema", "table_contract_hash", "bad"),
    ),
    (
        "preflight",
        _preflight_payload,
        "supporting_68_trigger_inventory",
        _drop_last(("supporting_trigger_source_contract", "trigger_names")),
    ),
    (
        "preflight",
        _preflight_payload,
        "supporting_trigger_count",
        _set_path("supporting_trigger_source_contract", "trigger_count", 67),
    ),
    (
        "preflight",
        _preflight_payload,
        "aggregate_optional_count",
        _set_path("trigger_contract", "optional_count", 67),
    ),
    (
        "preflight",
        _preflight_payload,
        "qmt_status",
        _set_path("qmt_reference_schema", "status", "HEALTHY"),
    ),
    (
        "preflight",
        _preflight_payload,
        "qmt_trigger_list",
        _drop_last(("qmt_reference_schema", "trigger_names")),
    ),
    (
        "preflight",
        _preflight_payload,
        "qmt_attestation_table_count",
        _set_path("qmt_table_count", 3),
    ),
    (
        "preflight",
        _preflight_payload,
        "governance_table_count",
        _set_path("governance_table_count", 14),
    ),
)


@pytest.mark.parametrize(
    ("phase", "payload_factory", "case_name", "mutate"),
    PHASE_MUTATIONS,
)
def test_schema_evidence_validator_rejects_phase_specific_tampering(
    validator_programs: dict[str, str],
    phase: str,
    payload_factory: PayloadFactory,
    case_name: str,
    mutate: Mutator,
) -> None:
    payload = copy.deepcopy(payload_factory())
    mutate(payload)

    result = _run_validator(validator_programs[phase], payload)

    assert result.returncode == 2, (
        phase,
        case_name,
        result.returncode,
        result.stdout,
        result.stderr,
    )
