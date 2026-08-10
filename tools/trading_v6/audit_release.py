#!/usr/bin/env python3
"""Audit independent Trading V6 research and frozen BLOCK evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sys
import tempfile
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.trading_v6.evidence import V6EvidenceError, audit_v6_evidence_bytes  # noqa: E402
from tools.trading_v6.release_integrity import (  # noqa: E402
    V6ReleaseIntegrityError,
    validate_v6_release,
)


RELEASE_ID = "trading_v6.0.0-research"
MANIFEST_PATH = ROOT / "versions/trading_v6/releases" / RELEASE_ID / "manifest.json"
RUNTIME_PATH = f"strategies/trading_v6/releases/{RELEASE_ID}/runtime.json"
ARTIFACT_NAMESPACE = (
    ROOT / "artifacts" / "trading_v6" / "releases" / RELEASE_ID
).resolve()
RUNTIME_KEYS = {
    "schema_version", "system", "system_version", "release_id", "created_on",
    "lifecycle_status", "release_decision", "purpose", "historical_evidence",
    "historical_limitations", "research_primitives", "prospective_gate",
    "environment_path", "environment_sha256", "environment_fully_reproducible",
    "database", "execution_boundary", "entrypoint", "artifact_namespace",
    "api_prefix", "task_namespace", "activation_eligible", "paper_eligible",
    "production_eligible",
}


class V6ReleaseAuditError(ValueError):
    """Raised when a V6 release contract or evidence binding drifts."""


def build_release_audit() -> dict[str, Any]:
    integrity = validate_v6_release()
    manifest = integrity.document
    runtime = _read_json_once(
        RUNTIME_PATH, manifest.get("config_sha256"), "runtime"
    )
    _exact_keys(runtime, RUNTIME_KEYS, "runtime")
    _validate_runtime_identity(runtime)

    config_files = _mapping(manifest.get("config_files"), "config_files")
    environment_path = runtime["environment_path"]
    environment_hash = _digest(runtime["environment_sha256"], "environment hash")
    if config_files.get(environment_path) != environment_hash:
        raise V6ReleaseAuditError("environment hash differs from manifest")
    environment = _read_json_once(
        environment_path, environment_hash, "environment"
    )
    _validate_environment(environment)

    historical = _mapping(runtime.get("historical_evidence"), "historical_evidence")
    expected_historical = _expected_historical_evidence()
    _exact_mapping(historical, expected_historical, "historical_evidence")
    campaign_path = historical["campaign_path"]
    campaign_hash = historical["campaign_sha256"]
    if config_files.get(campaign_path) != campaign_hash:
        raise V6ReleaseAuditError("legacy campaign copy is absent from config_files")
    campaign_bytes = _read_bytes_once(campaign_path, campaign_hash, "legacy campaign")
    artifact_bytes = _read_bytes_once(
        historical["artifact_path"], historical["artifact_sha256"], "legacy artifact"
    )
    runner_bytes = _read_bytes_once(
        historical["historical_runner_path"],
        historical["historical_runner_sha256"],
        "legacy runner",
    )
    log_bytes = {
        "failed_stdout": _read_bytes_once(
            historical["failed_stdout_path"], historical["failed_stdout_sha256"],
            "failed stdout",
        ),
        "failed_stderr": _read_bytes_once(
            historical["failed_stderr_path"], historical["failed_stderr_sha256"],
            "failed stderr",
        ),
        "completed_stdout": _read_bytes_once(
            historical["completed_stdout_path"], historical["completed_stdout_sha256"],
            "completed stdout",
        ),
        "completed_stderr": _read_bytes_once(
            historical["completed_stderr_path"], historical["completed_stderr_sha256"],
            "completed stderr",
        ),
    }
    log_hashes = {
        name: historical[f"{name}_sha256"] for name in log_bytes
    }
    evidence = audit_v6_evidence_bytes(
        campaign_bytes,
        artifact_bytes,
        runner_bytes,
        log_bytes,
        expected_campaign_sha256=campaign_hash,
        expected_artifact_sha256=historical["artifact_sha256"],
        expected_runner_sha256=historical["historical_runner_sha256"],
        expected_log_sha256=log_hashes,
    ).as_dict()
    if evidence["status"] != "BLOCK":
        raise V6ReleaseAuditError("V6 historical evidence escaped BLOCK")

    report = {
        "schema_version": "probiga.trading-v6-release-audit.v1",
        "system": "trading_v6",
        "system_version": "6.0.0-research",
        "release_id": RELEASE_ID,
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "manifest_sha256": integrity.manifest_sha256,
        "manifest_integrity_status": integrity.status,
        "source_tree_sha256": integrity.source_tree_sha256,
        "historical_evidence_audit": evidence,
        "new_research_primitives": {
            "pit_finance_protocol": "v6:pit-finance-after-close:v4",
            "hurdle_protocol": "v6:pit-hurdle-logistic-ridge:v5",
            "historical_result_reproduced": False,
            "registered_model_count": 0,
            "activation_eligible": False,
        },
        "database": {
            "status": "DEFERRED_MIGRATION_IN_PROGRESS",
            "tests_run": False,
            "counts_as_pass": False,
        },
        "registered_model_count": 0,
        "forecasts": [],
        "actions": [],
        "execution_intents": [],
        "actionable_output_allowed": False,
        "activation_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
        "paper_orders_allowed": False,
        "real_orders_allowed": False,
    }
    json.dumps(report, allow_nan=False, sort_keys=True)
    return report


def _validate_runtime_identity(runtime: Mapping[str, Any]) -> None:
    expected_top = {
        "schema_version": "probiga.trading-v6-release-runtime.v1",
        "system": "trading_v6",
        "system_version": "6.0.0-research",
        "release_id": RELEASE_ID,
        "created_on": "2026-08-08",
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "purpose": (
            "Independent exact-time PIT finance and true logistic hurdle research "
            "primitives plus byte-frozen legacy V6 evidence auditing; this is not "
            "an active stock selector."
        ),
        "environment_path": (
            f"strategies/trading_v6/releases/{RELEASE_ID}/environment.json"
        ),
        "environment_fully_reproducible": False,
        "entrypoint": "tools/trading_v6/audit_release.py",
        "artifact_namespace": f"artifacts/trading_v6/releases/{RELEASE_ID}",
        "api_prefix": None,
        "task_namespace": None,
        "activation_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
    }
    for name, expected in expected_top.items():
        _exact_value(runtime, name, expected, "runtime")
    _digest(runtime.get("environment_sha256"), "environment_sha256")
    _exact_mapping(
        _mapping(runtime.get("historical_limitations"), "historical_limitations"),
        {
            "legacy_hurdle_probability_status": "CLIPPED_LINEAR_SCORE_NOT_TRUE_PROBABILITY",
            "legacy_pit_finance_time_precision": "DATE_LEVEL_NOT_INTRADAY_CERTIFIED",
            "declared_bootstrap_iterations": 2000,
            "performed_bootstrap_iterations_inferred_from_bound_early_exit": 0,
            "performed_iteration_counter_recorded": False,
            "stress_expected_scenarios": 108,
            "stress_recorded_scenarios": 0,
            "stress_matrix_complete": False,
            "historical_success_cannot_activate": True,
        },
        "historical_limitations",
    )
    _exact_mapping(
        _mapping(runtime.get("research_primitives"), "research_primitives"),
        {
            "pit_finance_protocol": "v6:pit-finance-after-close:v4",
            "hurdle_protocol": "v6:pit-hurdle-logistic-ridge:v5",
            "win_probability_model": "L2_LOGISTIC",
            "positive_payoff_model": "L2_RIDGE",
            "nonpositive_loss_model": "L2_RIDGE_INCLUDING_ZERO_RETURN_ROWS",
            "exact_published_and_knowledge_timestamps_required": True,
            "after_close_signal_required": True,
            "caller_supplied_regime_labels_forbidden": True,
            "caller_supplied_regime_probabilities_forbidden": True,
            "label_maturity_required": True,
            "prediction_strictly_after_training_cutoff": True,
            "prediction_requires_process_local_fit_attestation": True,
            "finance_model_inputs_require_process_local_builder_attestation": True,
            "finance_model_rows_bind_feature_snapshot_sha256": True,
            "finance_source_manifest_prefix_invariant": True,
            "finance_conflicting_effective_statements_fail_closed": True,
            "finance_numeric_semantics": (
                "CANONICAL_12_SIGNIFICANT_DIGITS_BEFORE_HASH_AND_COMPUTE"
            ),
            "finance_peer_count_matches_ranked_cohort": True,
            "finance_signal_cohort_manifest_consistency_required": True,
            "pit_feature_exact_type_required": True,
            "prediction_score_attestation_required": True,
            "score_batch_validation_required_before_ranking": True,
            "score_batch_membership_attestation": (
                "PROCESS_LOCAL_SINGLE_COPY_NOT_SERIALIZABLE"
            ),
            "model_fit_configuration_bound": True,
            "ready_status": "PIT_RESEARCH_FEATURE_READY",
            "explicit_input_source_certified": False,
            "trading_calendar_snapshot_bound": False,
            "freshness_sla_enforced": False,
            "minimum_peer_coverage_enforced": False,
            "serialized_model_reload_supported": False,
            "model_integrity_scope": (
                "SELF_CONSISTENCY_PLUS_PROCESS_LOCAL_FIT_ATTESTATION_"
                "NOT_EXTERNAL_TRUST"
            ),
            "historical_result_reproduced_by_new_primitives": False,
            "registered_model_count": 0,
        },
        "research_primitives",
    )
    _exact_mapping(
        _mapping(runtime.get("prospective_gate"), "prospective_gate"),
        {
            "start_date": "2026-08-03",
            "minimum_trading_days": 120,
            "minimum_mature_portfolio_trades": 80,
            "recorded_evidence_trading_days": 0,
            "recorded_evidence_mature_portfolio_trades": 0,
            "status": "NOT_EVALUATED",
            "counts_as_pass": False,
        },
        "prospective_gate",
    )
    _exact_mapping(
        _mapping(runtime.get("database"), "database"),
        {
            "status": "DEFERRED_MIGRATION_IN_PROGRESS",
            "tests_run": False,
            "counts_as_pass": False,
        },
        "database",
    )
    _exact_mapping(
        _mapping(runtime.get("execution_boundary"), "execution_boundary"),
        {
            "actionable_output_allowed": False,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
            "order_intents_allowed": False,
            "v2_v3_v4_v5_runtime_imports_allowed": False,
        },
        "execution_boundary",
    )


def _expected_historical_evidence() -> dict[str, Any]:
    return {
        "campaign_path": f"strategies/trading_v6/releases/{RELEASE_ID}/legacy_campaign.json",
        "campaign_sha256": "b89c5bc54f2e12eb61f5d3b12f06bc62b6c49820fe01417be59aa647ba891caa",
        "research_contract_sha256": "d5993b9da941596b4777aa5a93d1e452c74232127759a50e4431b03fcb2e747e",
        "artifact_path": "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.json",
        "artifact_sha256": "4097c64da94417520771e129b6a0028c3387e52707f3c1dc48c04d2b65fa0137",
        "historical_runner_path": "tools/research_trading_v4_ml_campaign.py",
        "historical_runner_sha256": "cfae4b9dd18f5e612a03bfa197385818cd24cb71645c9a4341afb9b3827c424f",
        "failed_stdout_path": "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.stdout.log",
        "failed_stdout_sha256": "45c16b525603605ea1cc4f1c85c18263f94ed4040e3fdc18e54bfb6732af5a8d",
        "failed_stderr_path": "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.stderr.log",
        "failed_stderr_sha256": "40a709e6745a006e7271aba17f4750edbffc568b59184f8b2efd2cc2c61a3740",
        "completed_stdout_path": "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.py313.stdout.log",
        "completed_stdout_sha256": "3cea8ce9d1ffd833e65771956dcf07a5f1b3d96d56f9430f0657564a2bb2b1a1",
        "completed_stderr_path": "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.py313.stderr.log",
        "completed_stderr_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "candidate_count": 3,
        "candidate_pass_count": 0,
        "final_trade_count": 0,
        "raw_closed_trade_count": 842,
        "current_source_reproducible": False,
        "raw_data_snapshot_bound": False,
        "runtime_environment_bound": False,
        "counts_as_activation_evidence": False,
    }


def _validate_environment(environment: Mapping[str, Any]) -> None:
    _exact_mapping(
        environment,
        {
            "schema_version": "probiga.trading-v6-research-environment.v1",
            "release_id": RELEASE_ID,
            "observed_environment": {"python": "3.14.3"},
            "scope": "STANDARD_LIBRARY_RESEARCH_AND_EVIDENCE_TEST_ENVIRONMENT_ONLY",
            "observed_environment_evidence_status": "SELF_REPORTED_NOT_INDEPENDENT_EVIDENCE",
            "wheel_or_lockfile_hashes_present": False,
            "environment_fully_reproducible": False,
            "counts_as_model_evidence": False,
            "database_dependency": False,
            "note": (
                "The independent V6 package uses the Python standard library, but "
                "the interpreter build is not bound by a distributable binary hash; "
                "this is not a cross-runtime reproducibility claim."
            ),
        },
        "environment",
    )


def _read_json_once(path_text: str, expected_hash: Any, label: str) -> Mapping[str, Any]:
    return _strict_json(_read_bytes_once(path_text, expected_hash, label), label)


def _read_bytes_once(path_text: str, expected_hash: Any, label: str) -> bytes:
    path = _owned_path(path_text)
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != _digest(expected_hash, f"{label} hash"):
        raise V6ReleaseAuditError(f"{label} byte hash differs")
    return payload


def _strict_json(payload: bytes, label: str) -> Mapping[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise V6ReleaseAuditError(f"{label} duplicate key: {key}")
            result[key] = value
        return result

    def constant(value: str) -> None:
        raise V6ReleaseAuditError(f"{label} contains non-finite {value}")

    try:
        value = json.loads(
            payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V6ReleaseAuditError(f"{label} is not strict UTF-8 JSON") from exc
    return _mapping(value, label)


def _owned_path(path_text: Any) -> Path:
    if not isinstance(path_text, str) or not path_text or "\\" in path_text:
        raise V6ReleaseAuditError("release path is not canonical")
    pure = PurePosixPath(path_text)
    if pure.is_absolute() or ".." in pure.parts or pure.as_posix() != path_text:
        raise V6ReleaseAuditError("release path escapes the repository")
    path = (ROOT / Path(*pure.parts)).resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise V6ReleaseAuditError("release path escapes the repository") from exc
    if not path.is_file():
        raise V6ReleaseAuditError(f"release file is missing: {path_text}")
    return path


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise V6ReleaseAuditError(f"{label} must be an object")
    return value


def _digest(value: Any, label: str) -> str:
    if (
        not isinstance(value, str) or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise V6ReleaseAuditError(f"{label} must be a lowercase SHA-256")
    return value


def _exact_keys(document: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(document) != expected:
        raise V6ReleaseAuditError(
            f"{label} keys differ: missing={sorted(expected - set(document))} "
            f"extra={sorted(set(document) - expected)}"
        )


def _exact_mapping(
    document: Mapping[str, Any], expected: Mapping[str, Any], label: str
) -> None:
    _exact_keys(document, set(expected), label)
    for name, expected_value in expected.items():
        actual = document.get(name)
        if actual != expected_value or type(actual) is not type(expected_value):
            raise V6ReleaseAuditError(f"{label}.{name} differs")


def _exact_value(
    document: Mapping[str, Any], name: str, expected: Any, label: str
) -> None:
    actual = document.get(name)
    if actual != expected or type(actual) is not type(expected):
        raise V6ReleaseAuditError(f"{label}.{name} differs")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit frozen Trading V6 evidence (always research-only)."
    )
    parser.add_argument(
        "--output", type=Path,
        help=f"Optional new JSON below artifacts/trading_v6/releases/{RELEASE_ID}; never overwrite.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = build_release_audit()
        encoded = json.dumps(
            report, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2
        ) + "\n"
        if args.output is None:
            sys.stdout.write(encoded)
            return 0
        output = args.output.resolve()
        try:
            output.relative_to(ARTIFACT_NAMESPACE)
        except ValueError as exc:
            raise V6ReleaseAuditError("output escapes the V6 release namespace") from exc
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", newline="\n", prefix=".v6-audit-",
                suffix=".tmp", dir=output.parent, delete=False,
            ) as handle:
                temporary = Path(handle.name)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, output)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        print(f"audit_written={output} release_decision=BLOCK")
        return 0
    except (
        OSError, V6EvidenceError, V6ReleaseAuditError, V6ReleaseIntegrityError
    ) as exc:
        print(f"release_audit=FAILED error={exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
