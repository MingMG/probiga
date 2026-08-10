#!/usr/bin/env python3
"""Audit the frozen Trading V5 legacy evidence without DB or order access."""

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

from server.trading_v5.evidence import (  # noqa: E402
    EvidenceContractError,
    audit_campaign_bytes,
)
from tools.trading_v5.release_integrity import (  # noqa: E402
    V5ReleaseIntegrityError,
    validate_v5_release,
)


RELEASE_ID = "trading_v5.0.0-research"
MANIFEST_PATH = (
    ROOT
    / "versions"
    / "trading_v5"
    / "releases"
    / RELEASE_ID
    / "manifest.json"
)
EXPECTED_RUNTIME_PATH = (
    "strategies/trading_v5/releases/"
    f"{RELEASE_ID}/runtime.json"
)
EXPECTED_ARTIFACT_NAMESPACE = (
    ROOT / "artifacts" / "trading_v5" / "releases" / RELEASE_ID
).resolve()
RUNTIME_KEYS = {
    "schema_version", "system", "system_version", "release_id", "created_on",
    "lifecycle_status", "release_decision", "purpose", "owned_campaigns",
    "historical_evidence", "prospective_gate", "model_research", "database",
    "execution_boundary", "entrypoint", "artifact_namespace", "api_prefix",
    "task_namespace", "activation_eligible", "paper_eligible",
    "production_eligible",
}
CAMPAIGN_KEYS = {
    "name", "campaign_path", "campaign_sha256", "research_contract_sha256",
    "historical_artifact_path", "historical_artifact_sha256",
    "historical_gate_status", "governance_status", "json_status",
    "current_source_reproducible",
}


class ReleaseAuditError(ValueError):
    """Raised when V5 release identity or ownership drifts."""


def build_release_audit() -> dict[str, Any]:
    """Return a strict finite BLOCK report for the frozen V5 release."""

    integrity = validate_v5_release()
    manifest = integrity.document
    if manifest.get("release_id") != RELEASE_ID:
        raise ReleaseAuditError("release manifest identity differs")
    if manifest.get("system") != "trading_v5":
        raise ReleaseAuditError("release manifest system differs")
    if manifest.get("lifecycle_status") != "RESEARCH_ONLY":
        raise ReleaseAuditError("V5 release must remain RESEARCH_ONLY")
    for field in ("activation_eligible", "paper_eligible", "production_eligible"):
        if manifest.get(field) is not False:
            raise ReleaseAuditError(f"manifest {field} must be false")
    source_files = _mapping(manifest.get("source_files"), "source_files")
    for path_text, expected_hash in source_files.items():
        if not isinstance(path_text, str) or not path_text.startswith(
            ("server/trading_v5/", "tools/trading_v5/")
        ):
            raise ReleaseAuditError("manifest source ownership crossed a version")
        _verify_hash(path_text, expected_hash)
    if manifest.get("config_path") != EXPECTED_RUNTIME_PATH:
        raise ReleaseAuditError("manifest runtime path differs")
    runtime = _read_frozen_json(
        EXPECTED_RUNTIME_PATH,
        manifest.get("config_sha256"),
        "runtime",
    )
    _exact_keys(runtime, RUNTIME_KEYS, "runtime")
    if runtime.get("release_id") != RELEASE_ID:
        raise ReleaseAuditError("runtime release identity differs")
    expected_runtime = {
        "schema_version": "probiga.trading-v5-release-runtime.v1",
        "system": "trading_v5",
        "system_version": "5.0.0-research",
        "release_id": RELEASE_ID,
        "created_on": "2026-08-08",
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "purpose": (
            "Independent model-research primitives plus byte-frozen legacy "
            "evidence auditing; this is not an active stock selector."
        ),
        "entrypoint": "tools/trading_v5/audit_release.py",
        "artifact_namespace": (
            "artifacts/trading_v5/releases/trading_v5.0.0-research"
        ),
        "activation_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
        "api_prefix": None,
        "task_namespace": None,
    }
    for field_name, expected_value in expected_runtime.items():
        if (
            runtime.get(field_name) != expected_value
            or type(runtime.get(field_name)) is not type(expected_value)
        ):
            raise ReleaseAuditError(f"runtime {field_name} differs")
    if runtime.get("release_decision") != "BLOCK":
        raise ReleaseAuditError("runtime release decision must remain BLOCK")

    historical = _mapping(runtime.get("historical_evidence"), "historical_evidence")
    _exact_mapping(
        historical,
        {
            "classification": "HISTORICAL_EXPLORATORY_BLOCK",
            "all_history_through_2026_07_31_contaminated_by_prior_inspection": True,
            "historical_pass_cannot_activate_model": True,
            "source_snapshot_bound": False,
            "raw_data_snapshot_bound": False,
            "complete_runtime_config_bound": False,
            "full_stress_matrix_executed": False,
            "fold_equity_curves_preserved": False,
            "bootstrap_seed_preregistered": False,
            "counts_as_activation_evidence": False,
        },
        "historical_evidence",
    )
    prospective = _mapping(runtime.get("prospective_gate"), "prospective_gate")
    _exact_mapping(
        prospective,
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
    boundary = _mapping(runtime.get("execution_boundary"), "execution_boundary")
    expected_boundary_keys = {
        "actionable_output_allowed",
        "paper_orders_allowed",
        "real_orders_allowed",
        "order_intents_allowed",
        "v2_v3_v4_runtime_imports_allowed",
    }
    if set(boundary) != expected_boundary_keys:
        raise ReleaseAuditError("runtime execution boundary keys differ")
    if any(value is not False for value in boundary.values()):
        raise ReleaseAuditError("runtime execution boundary must be all false")
    database = _mapping(runtime.get("database"), "database")
    _exact_mapping(
        database,
        {
            "status": "DEFERRED_MIGRATION_IN_PROGRESS",
            "tests_run": False,
            "counts_as_pass": False,
        },
        "database",
    )
    model_research = _mapping(runtime.get("model_research"), "model_research")
    environment_path = (
        "strategies/trading_v5/releases/trading_v5.0.0-research/"
        "environment.json"
    )
    config_files = _mapping(manifest.get("config_files"), "manifest config_files")
    environment_hash = _digest(
        model_research.get("environment_sha256"),
        "environment_sha256",
    )
    _exact_mapping(
        model_research,
        {
            "ridge_protocol": "v5:point-in-time-ridge:v2",
            "regime_expert_protocol": "v5:point-in-time-regime-expert-ridge:v2",
            "regime_router_protocol": "v5:market-regime-softmax:v1",
            "training_input": "EXPLICIT_TIMEZONE_AWARE_DATAFRAME_ONLY",
            "target_and_result_features_forbidden": True,
            "caller_supplied_regime_labels_forbidden": True,
            "caller_supplied_regime_probabilities_forbidden": True,
            "prediction_samples_must_be_strictly_after_training_cutoff": True,
            "model_activation_claim_forbidden": True,
            "prediction_requires_process_local_fit_attestation": True,
            "serialized_model_reload_supported": False,
            "model_integrity_scope": (
                "SELF_CONSISTENCY_PLUS_PROCESS_LOCAL_FIT_ATTESTATION_"
                "NOT_EXTERNAL_TRUST"
            ),
            "registered_model_count": 0,
            "environment_path": environment_path,
            "environment_sha256": environment_hash,
            "environment_fully_reproducible": False,
        },
        "model_research",
    )
    if config_files.get(environment_path) != environment_hash:
        raise ReleaseAuditError("environment hash differs from release manifest")
    environment = _read_frozen_json(
        environment_path,
        environment_hash,
        "model environment",
    )
    expected_environment = {
        "schema_version": "probiga.trading-v5-research-environment.v1",
        "release_id": RELEASE_ID,
        "observed_environment": {
            "python": "3.14.3",
            "numpy": "2.4.5",
            "pandas": "3.0.3",
        },
        "scope": "MODEL_RESEARCH_TEST_ENVIRONMENT_ONLY",
        "observed_environment_evidence_status": (
            "SELF_REPORTED_NOT_INDEPENDENT_EVIDENCE"
        ),
        "wheel_or_lockfile_hashes_present": False,
        "environment_fully_reproducible": False,
        "counts_as_model_evidence": False,
        "database_dependency": False,
        "note": (
            "Versions record the environment used for local verification; no "
            "wheel hashes or complete lockfile are available, so this is not "
            "a reproducible environment claim."
        ),
    }
    _exact_mapping(environment, expected_environment, "model environment")

    audits = []
    campaigns = runtime.get("owned_campaigns")
    if not isinstance(campaigns, list) or len(campaigns) != 2:
        raise ReleaseAuditError("runtime must own exactly two historical campaigns")
    expected_campaigns = {
        "regime_expert": {
            "name": "regime_expert",
            "campaign_path": (
                "strategies/trading_v5/releases/trading_v5.0.0-research/"
                "campaigns/regime_expert/campaign.json"
            ),
            "campaign_sha256": "9bafc682995480b41b85ab0ef45e4b45e7415e4cf45743e1a703eef276eb8198",
            "research_contract_sha256": "b7ed63588e427d337e9a660c06138b78f69fda3cf8195c38e27fdf35bc9037e8",
            "historical_artifact_path": (
                "artifacts/trading_v5/regime_expert_capacity_oos_20260802.json"
            ),
            "historical_artifact_sha256": "1a4f8c5fa229352ad20324f8cf4cc9ad85891d5d6aed78d1eb76795a64ba6259",
            "historical_gate_status": "BLOCK",
            "governance_status": "LEGACY_UNGOVERNED",
            "json_status": "LEGACY_NON_STRICT_JSON",
            "current_source_reproducible": False,
            "evidence_name": "regime_expert_capacity",
        },
        "unified_router": {
            "name": "unified_router",
            "campaign_path": (
                "strategies/trading_v5/releases/trading_v5.0.0-research/"
                "campaigns/unified_router/campaign.json"
            ),
            "campaign_sha256": "4cbaea2ea7fc4639605c8c6facd3eb3cf2a6a03ba5085406e5197622b0d9cafd",
            "research_contract_sha256": "3a6180d6dc43d0d12406f60dc8a7cdfde0a445b8c8af10747dd14dd5f5c6d095",
            "historical_artifact_path": (
                "artifacts/trading_v5/unified_router_capacity_oos_20260802.json"
            ),
            "historical_artifact_sha256": "2702acf26801020b0f6ffae1081d9641bd50b1b7f5461afc7731f25d6ea58477",
            "historical_gate_status": "BLOCK",
            "governance_status": "RECORDED_GOVERNED_EXPLORATORY_BYTE_FROZEN_NOT_RECOMPUTED",
            "json_status": "STRICT_JSON",
            "current_source_reproducible": False,
            "evidence_name": "unified_router_capacity",
        },
    }
    campaign_names = [
        item.get("name") if isinstance(item, Mapping) else None
        for item in campaigns
    ]
    if campaign_names != list(expected_campaigns):
        raise ReleaseAuditError("runtime historical campaign identities differ")
    evidence_files = _mapping(
        manifest.get("historical_evidence_files"),
        "historical_evidence_files",
    )
    for raw_campaign in campaigns:
        campaign = _mapping(raw_campaign, "owned campaign")
        identity = expected_campaigns[campaign["name"]]
        expected_runtime_campaign = {
            key: value for key, value in identity.items() if key != "evidence_name"
        }
        _exact_keys(campaign, CAMPAIGN_KEYS, "owned campaign")
        _exact_mapping(campaign, expected_runtime_campaign, "owned campaign")
        evidence_item = _mapping(
            evidence_files.get(identity["evidence_name"]),
            "manifest evidence item",
        )
        _exact_keys(
            evidence_item,
            {"path", "sha256", "decision", "json_status", "current_source_reproducible"},
            "manifest evidence item",
        )
        _exact_mapping(
            evidence_item,
            {
                "path": identity["historical_artifact_path"],
                "sha256": identity["historical_artifact_sha256"],
                "decision": "BLOCK",
                "json_status": identity["json_status"],
                "current_source_reproducible": False,
            },
            "manifest evidence item",
        )
        campaign_path = _owned_path(
            campaign.get("campaign_path"),
            f"strategies/trading_v5/releases/{RELEASE_ID}/campaigns/",
        )
        artifact_path = _owned_path(
            campaign.get("historical_artifact_path"),
            "artifacts/trading_v5/",
        )
        audit = audit_campaign_bytes(
            campaign_path.read_bytes(),
            artifact_path.read_bytes(),
            expected_campaign_sha256=_digest(
                campaign.get("campaign_sha256"),
                "campaign_sha256",
            ),
            expected_artifact_sha256=_digest(
                campaign.get("historical_artifact_sha256"),
                "historical_artifact_sha256",
            ),
        )
        if audit.research_contract_sha256 != campaign.get(
            "research_contract_sha256"
        ):
            raise ReleaseAuditError("runtime research contract hash differs")
        if audit.status != "BLOCK":
            raise ReleaseAuditError("historical campaign unexpectedly escaped BLOCK")
        audits.append(audit.as_dict())

    report = {
        "schema_version": "probiga.trading-v5-release-audit.v1",
        "system": "trading_v5",
        "system_version": "5.0.0-research",
        "release_id": RELEASE_ID,
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "manifest_sha256": integrity.manifest_sha256,
        "manifest_integrity_status": integrity.status,
        "source_tree_sha256": integrity.source_tree_sha256,
        "historical_campaigns": audits,
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


def _load_json(payload: bytes, label: str) -> Mapping[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ReleaseAuditError(f"{label} has duplicate key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ReleaseAuditError(f"{label} contains non-finite {value}")

    try:
        document = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseAuditError(f"{label} is not strict UTF-8 JSON") from exc
    return _mapping(document, label)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReleaseAuditError(f"{label} must be an object")
    return value


def _read_frozen_json(
    path_text: str,
    expected_hash: Any,
    label: str,
) -> Mapping[str, Any]:
    path = _owned_path(path_text, "")
    payload = path.read_bytes()
    digest = _digest(expected_hash, f"hash for {path_text}")
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ReleaseAuditError(f"release file hash drifted: {path_text}")
    return _load_json(payload, label)


def _exact_keys(
    document: Mapping[str, Any],
    expected: set[str],
    label: str,
) -> None:
    if set(document) != expected:
        raise ReleaseAuditError(
            f"{label} keys differ: missing={sorted(expected - set(document))} "
            f"extra={sorted(set(document) - expected)}"
        )


def _exact_mapping(
    document: Mapping[str, Any],
    expected: Mapping[str, Any],
    label: str,
) -> None:
    _exact_keys(document, set(expected), label)
    for name, expected_value in expected.items():
        actual = document.get(name)
        if actual != expected_value or type(actual) is not type(expected_value):
            raise ReleaseAuditError(
                f"{label}.{name} differs: expected={expected_value!r} actual={actual!r}"
            )


def _digest(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ReleaseAuditError(f"{label} must be a lowercase SHA-256")
    return value


def _verify_hash(path_text: str, expected_hash: Any) -> None:
    digest = _digest(expected_hash, f"hash for {path_text}")
    path = _owned_path(path_text, "")
    if not path.is_file():
        raise ReleaseAuditError(f"release file is missing: {path_text}")
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != digest:
        raise ReleaseAuditError(f"release file hash drifted: {path_text}")


def _owned_path(value: Any, required_prefix: str) -> Path:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or "\\" in value
        or not value.startswith(required_prefix)
    ):
        raise ReleaseAuditError("release path is not canonical or version-owned")
    pure = PurePosixPath(value)
    if pure.is_absolute() or ".." in pure.parts:
        raise ReleaseAuditError("release path escapes the repository")
    path = (ROOT / Path(*pure.parts)).resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise ReleaseAuditError("release path escapes the repository") from exc
    return path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit frozen Trading V5 evidence (always research-only)."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Optional new JSON file below artifacts/trading_v5/releases/"
            f"{RELEASE_ID}; existing files are never overwritten."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = build_release_audit()
        encoded = json.dumps(
            report,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        ) + "\n"
        if args.output is None:
            sys.stdout.write(encoded)
            return 0
        output = args.output.resolve()
        try:
            output.relative_to(EXPECTED_ARTIFACT_NAMESPACE)
        except ValueError as exc:
            raise ReleaseAuditError("output escapes the V5 release namespace") from exc
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                prefix=".v5-audit-",
                suffix=".tmp",
                dir=output.parent,
                delete=False,
            ) as handle:
                temporary_path = Path(handle.name)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary_path, output)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        print(f"audit_written={output} release_decision=BLOCK")
        return 0
    except (
        OSError,
        EvidenceContractError,
        ReleaseAuditError,
        V5ReleaseIntegrityError,
    ) as exc:
        print(f"release_audit=FAILED error={exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
