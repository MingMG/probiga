"""Read-only, fail-closed production checks for Trading V3 layer four.

The long-history model/OOS evidence and the forward Shadow outcome ledger are
deliberately reported as separate authorities.  A valid research artifact does
not attest a forward market outcome, and a scheduler exit code does not attest
either one.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from server.common.config import get_admin_auth_config
from server.common.scheduler_args import build_scheduler_task_args
from server.common.scheduler_authority import (
    LAYER4_WRITER_TASK_TYPES,
    PRODUCTION_SCHEDULER_MODE,
    scheduler_authority_contract,
)
from server.trading_v3.horizon_candidate_ledger_schema import (
    CANDIDATE_EVALUATION_LEDGER_SCHEMA,
    CURRENT_HORIZON_ARTIFACT_SCHEMA,
    CURRENT_HORIZON_MODEL_PROTOCOL,
    CURRENT_HORIZON_SELECTION_POLICY_HASH,
    CURRENT_HORIZON_SELECTION_PROTOCOL,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
    HORIZON_CANDIDATE_LEDGER_DDL,
    HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
    validate_horizon_candidate_ledger_schema,
)
from server.trading_v3.horizon_protocol_v2_schema import (
    HORIZON_PROTOCOL_V2_DDL,
    HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
    validate_horizon_protocol_v2_schema,
)
from server.trading_v3.shadow_intelligence_schema import (
    SHADOW_INTELLIGENCE_DDL,
    SHADOW_INTELLIGENCE_MIGRATION_VERSION,
    validate_shadow_intelligence_schema,
)
from server.trading_v3.versioning import code_version


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_HORIZONS = (1, 5, 20)
PAGE_GET_PATHS = {
    "readiness": "/api/v3/readiness",
    "governance": "/api/v3/research/governance",
    "horizons": "/api/v3/research/horizons/latest?limit=1000",
    "learning": "/api/v3/research/learning/latest",
    "shadow": "/api/v3/research/shadow/status",
}

LAYER4_SCHEDULER_TASK_TYPES = LAYER4_WRITER_TASK_TYPES


def _expected_layer4_scheduler_tasks() -> dict[str, dict[str, Any]]:
    """Read the deployment definitions used by the task upsert itself."""

    from tools.add_trading_v3_tasks import TASKS

    return {
        str(item["task_type"]): dict(item)
        for item in TASKS
        if item.get("task_type") in LAYER4_SCHEDULER_TASK_TYPES
    }


def _expected_migration() -> dict[str, Any]:
    statements = tuple(SHADOW_INTELLIGENCE_DDL)
    checksum = hashlib.sha256(
        "\n".join(item.strip() for item in statements).encode("utf-8")
    ).hexdigest()
    return {
        "version": SHADOW_INTELLIGENCE_MIGRATION_VERSION,
        "checksum": checksum,
        "statement_count": len(statements),
    }


def _expected_protocol_migration() -> dict[str, Any]:
    statements = tuple(HORIZON_PROTOCOL_V2_DDL)
    checksum = hashlib.sha256(
        "\n".join(item.strip() for item in statements).encode("utf-8")
    ).hexdigest()
    return {
        "version": HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
        "checksum": checksum,
        "statement_count": len(statements),
    }


def _expected_candidate_ledger_migration() -> dict[str, Any]:
    statements = tuple(HORIZON_CANDIDATE_LEDGER_DDL)
    checksum = hashlib.sha256(
        "\n".join(item.strip() for item in statements).encode("utf-8")
    ).hexdigest()
    return {
        "version": HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
        "checksum": checksum,
        "statement_count": len(statements),
    }


def evaluate_migration_state(
    ledger: Mapping[str, Any] | None,
    progress: Mapping[str, Any] | None,
    *,
    schema_verified: bool,
    schema_error: str = "",
    expected: Mapping[str, Any] | None = None,
    reason_namespace: str = "SHADOW",
) -> dict[str, Any]:
    frozen_expected = dict(expected or _expected_migration())
    prefix = str(reason_namespace or "SHADOW").strip().upper()
    row = dict(ledger or {})
    progress_row = dict(progress or {})
    ledger_current = bool(
        row
        and str(row.get("version") or "") == frozen_expected["version"]
        and str(row.get("checksum") or "") == frozen_expected["checksum"]
        and int(row.get("statement_count") or 0)
        == frozen_expected["statement_count"]
    )
    completed = int(progress_row.get("completed_statement_count") or 0)
    # Current forward migrations always create and fully advance a durable
    # progress row before writing the ledger.  Treating a copied ledger plus
    # no progress row as complete would discard the runner's recovery proof.
    progress_consistent = bool(
        (not row and not progress_row)
        or (
            bool(progress_row)
            and
            str(progress_row.get("version") or "")
            == frozen_expected["version"]
            and str(progress_row.get("checksum") or "")
            == frozen_expected["checksum"]
            and int(progress_row.get("statement_count") or 0)
            == frozen_expected["statement_count"]
            and completed == frozen_expected["statement_count"]
        )
    )
    reasons: list[str] = []
    if not row:
        reasons.append(f"{prefix}_MIGRATION_LEDGER_MISSING")
    elif not ledger_current:
        reasons.append(f"{prefix}_MIGRATION_LEDGER_DRIFT")
    if row and not progress_row:
        reasons.append(f"{prefix}_MIGRATION_PROGRESS_MISSING")
    if progress_row and not progress_consistent:
        reasons.append(f"{prefix}_MIGRATION_PARTIALLY_APPLIED")
    if not schema_verified:
        reasons.append(f"{prefix}_SCHEMA_UNVERIFIED")
    return {
        "expected": frozen_expected,
        "ledger": row,
        "progress": progress_row,
        "ledger_current": ledger_current,
        "progress_consistent": progress_consistent,
        "schema_verified": bool(schema_verified),
        "schema_error": str(schema_error or "")[:300],
        "reason_codes": reasons,
        "ready": bool(
            ledger_current and progress_consistent and schema_verified
        ),
    }


def collect_migration_readiness(engine: Engine) -> dict[str, Any]:
    expected_migrations = (
        ("shadow", _expected_migration(), validate_shadow_intelligence_schema),
        (
            "horizon_protocol_v2",
            _expected_protocol_migration(),
            validate_horizon_protocol_v2_schema,
        ),
        (
            "horizon_candidate_ledger_v3",
            _expected_candidate_ledger_migration(),
            validate_horizon_candidate_ledger_schema,
        ),
    )
    results: dict[str, dict[str, Any]] = {}
    try:
        with engine.connect() as connection:
            for name, expected, validator in expected_migrations:
                row = connection.execute(
                    text(
                        "SELECT version, checksum, statement_count, applied_at "
                        "FROM schema_migration_v3 WHERE version=:version"
                    ),
                    {"version": expected["version"]},
                ).mappings().first()
                ledger = dict(row) if row else {}
                progress_row = connection.execute(
                    text(
                        "SELECT version, checksum, statement_count, "
                        "completed_statement_count, updated_at "
                        "FROM schema_migration_v3_progress "
                        "WHERE version=:version"
                    ),
                    {"version": expected["version"]},
                ).mappings().first()
                progress = dict(progress_row) if progress_row else {}
                schema_verified = False
                schema_error = ""
                try:
                    if ledger:
                        validator(connection)
                        schema_verified = True
                except Exception as exc:  # drift is evidence, not a crash
                    schema_error = f"{type(exc).__name__}: {exc}"
                results[name] = evaluate_migration_state(
                    ledger,
                    progress,
                    schema_verified=schema_verified,
                    schema_error=schema_error,
                    expected=expected,
                    reason_namespace=(
                        "SHADOW"
                        if name == "shadow"
                        else (
                            "HORIZON_PROTOCOL_V2"
                            if name == "horizon_protocol_v2"
                            else "HORIZON_CANDIDATE_LEDGER_V3"
                        )
                    ),
                )
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        for name, expected, _validator in expected_migrations:
            results.setdefault(
                name,
                evaluate_migration_state(
                    {},
                    {},
                    schema_verified=False,
                    schema_error=detail,
                    expected=expected,
                    reason_namespace=(
                        "SHADOW"
                        if name == "shadow"
                        else (
                            "HORIZON_PROTOCOL_V2"
                            if name == "horizon_protocol_v2"
                            else "HORIZON_CANDIDATE_LEDGER_V3"
                        )
                    ),
                ),
            )
    shadow = results["shadow"]
    reasons = [
        reason
        for result in results.values()
        for reason in result["reason_codes"]
    ]
    return {
        **shadow,
        "migrations": results,
        "expected_migrations": [
            dict(item[1]) for item in expected_migrations
        ],
        "reason_codes": list(dict.fromkeys(reasons)),
        "ready": all(result["ready"] for result in results.values()),
    }


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        result = as_dict()
        if isinstance(result, Mapping):
            return dict(result)
    return dict(getattr(value, "__dict__", {}) or {})


def _artifact_path(
    model: Mapping[str, Any],
    *,
    release_id: str,
    horizon: int,
) -> Path | None:
    raw = str(model.get("artifact_path") or "").strip()
    if raw:
        path = Path(raw)
        path = path if path.is_absolute() else ROOT / path
    elif release_id:
        path = (
            ROOT
            / "artifacts"
            / "trading_v3"
            / "horizon_models"
            / release_id
            / f"T{horizon}.json"
        )
    else:
        return None
    resolved = path.resolve()
    artifact_root = (ROOT / "artifacts" / "trading_v3").resolve()
    if artifact_root not in resolved.parents:
        return None
    return resolved


def _session_count(artifact: Mapping[str, Any]) -> int:
    walk_forward = dict(artifact.get("walk_forward") or {})
    oos_evidence = dict(artifact.get("oos_evidence") or {})
    candidates = (
        oos_evidence.get("distinct_oos_sessions"),
        walk_forward.get("distinct_oos_session_count"),
        walk_forward.get("oos_session_count"),
        walk_forward.get("total_oos_sessions"),
        dict(walk_forward.get("summary") or {}).get("oos_session_count"),
    )
    for value in candidates:
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    folds = walk_forward.get("folds") or []
    sessions: set[str] = set()
    for fold in folds if isinstance(folds, list) else []:
        if not isinstance(fold, Mapping):
            continue
        for key in ("oos_sessions", "validation_sessions"):
            values = fold.get(key) or []
            if isinstance(values, list):
                sessions.update(str(item) for item in values if str(item))
    return len(sessions)


def collect_artifact_readiness(
    config: Mapping[str, Any],
    *,
    current_config_hash: str,
) -> dict[str, Any]:
    policy = dict(config.get("multi_horizon_forecasts") or {})
    models = dict(policy.get("trainable_models") or {})
    release_id = str(policy.get("artifact_release_id") or "").strip()
    continuous_policy = dict(config.get("continuous_calibration") or {})
    training_policy = dict(policy.get("training_policy") or {})
    minimum_sessions = dict(
        training_policy.get("minimum_oos_sessions")
        or continuous_policy.get("minimum_oos_sessions")
        or {}
    )
    minimum_selected_samples = dict(
        continuous_policy.get("minimum_oos_samples") or {}
    )
    results: dict[str, dict[str, Any]] = {}
    reasons: list[str] = []
    try:
        from server.trading_v3.horizon_models import load_horizon_artifact
    except Exception as exc:
        return {
            "release_id": release_id,
            "artifacts": {},
            "artifact_hashes": {},
            "feature_protocol_hashes": {},
            "reason_codes": ["HORIZON_ARTIFACT_VERIFIER_UNAVAILABLE"],
            "detail": f"{type(exc).__name__}: {exc}"[:300],
            "long_history_oos_ready": False,
            "ready": False,
        }
    if not release_id and not all(
        str(dict(models.get(f"T+{h}") or {}).get("artifact_path") or "")
        for h in REQUIRED_HORIZONS
    ):
        reasons.append("HORIZON_ARTIFACT_RELEASE_NOT_PINNED")
    for horizon in REQUIRED_HORIZONS:
        key = f"T+{horizon}"
        model = dict(models.get(key) or {})
        path = _artifact_path(
            model,
            release_id=release_id,
            horizon=horizon,
        )
        item: dict[str, Any] = {
            "path": str(path) if path else "",
            "horizon_days": horizon,
            "ready": False,
        }
        if path is None or not path.is_file():
            reasons.append(f"HORIZON_ARTIFACT_MISSING_T{horizon}")
            results[key] = item
            continue
        try:
            raw_artifact = json.loads(path.read_text("utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            raw_artifact = None
        if (
            isinstance(raw_artifact, Mapping)
            and raw_artifact.get("schema_version") in {
                HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
                HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
            }
        ):
            historical_schema = str(raw_artifact["schema_version"])
            is_v1 = (
                historical_schema == HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1
            )
            item.update({
                "artifact_schema_version": historical_schema,
                "protocol_status": (
                    "HISTORICAL_AUDIT_ONLY"
                    if is_v1
                    else "PRE_LEDGER_V2_AUDIT_ONLY"
                ),
                "runtime_eligible": False,
                "order_authority": False,
                "blockers": [
                    "HISTORICAL_PROTOCOL_AUDIT_ONLY"
                    if is_v1
                    else "PRE_LEDGER_V2_AUDIT_ONLY"
                ],
            })
            reasons.append(
                f"HORIZON_ARTIFACT_{'V1' if is_v1 else 'V2'}_"
                f"AUDIT_ONLY_T{horizon}"
            )
            results[key] = item
            continue
        try:
            artifact = _as_mapping(load_horizon_artifact(path))
        except Exception as exc:
            item["error"] = f"{type(exc).__name__}: {exc}"[:300]
            reasons.append(f"HORIZON_ARTIFACT_INVALID_T{horizon}")
            results[key] = item
            continue
        gate = dict(artifact.get("gate") or {})
        feature = dict(artifact.get("feature_protocol") or {})
        dataset = dict(artifact.get("dataset_manifest") or {})
        oos_evidence = dict(artifact.get("oos_evidence") or {})
        selection_policy = dict(artifact.get("selection_policy") or {})
        selection_evidence = dict(
            oos_evidence.get("selection_evidence") or {}
        )
        sessions = _session_count(artifact)
        required_sessions = int(minimum_sessions.get(str(horizon)) or 1)
        blockers: list[str] = []
        expected_key = str(model.get("model_key") or "")
        expected_version = str(model.get("model_version") or "")
        if int(artifact.get("horizon_days") or 0) != horizon:
            blockers.append("HORIZON_MISMATCH")
        if str(artifact.get("schema_version") or "") != (
            CURRENT_HORIZON_ARTIFACT_SCHEMA
        ):
            blockers.append("ARTIFACT_SCHEMA_NOT_CURRENT_V3")
        if str(artifact.get("model_protocol") or "") != (
            CURRENT_HORIZON_MODEL_PROTOCOL
        ) or str(oos_evidence.get("model_protocol") or "") != (
            CURRENT_HORIZON_MODEL_PROTOCOL
        ):
            blockers.append("MODEL_PROTOCOL_NOT_CURRENT_V2")
        candidate_ledger = dict(
            artifact.get("candidate_evaluation_ledger") or {}
        )
        if (
            candidate_ledger.get("schema_version")
            != CANDIDATE_EVALUATION_LEDGER_SCHEMA
            or len(str(candidate_ledger.get("content_sha256") or "")) != 64
            or int(candidate_ledger.get("row_count") or 0) <= 0
            or candidate_ledger.get("registration_verification_required")
            is not True
        ):
            blockers.append("CANDIDATE_LEDGER_NOT_STREAM_VERIFIABLE")
        if str(selection_policy.get("selection_policy_hash") or "") != (
            CURRENT_HORIZON_SELECTION_POLICY_HASH
        ) or str(selection_evidence.get("selection_policy_hash") or "") != (
            CURRENT_HORIZON_SELECTION_POLICY_HASH
        ):
            blockers.append("SELECTION_POLICY_NOT_CURRENT")
        if str(selection_evidence.get("protocol") or "") != (
            CURRENT_HORIZON_SELECTION_PROTOCOL
        ):
            blockers.append("SELECTION_PROTOCOL_NOT_CURRENT")
        if (
            selection_evidence.get("order_authority") is not False
            or selection_evidence.get(
                "deployment_candidate_domain_verified"
            )
            is not False
            or oos_evidence.get(
                "economic_metrics_use_frozen_selection_ledger"
            )
            is not True
        ):
            blockers.append("SELECTION_RESEARCH_BOUNDARY_INVALID")
        if expected_key and str(artifact.get("model_key") or "") != expected_key:
            blockers.append("MODEL_KEY_MISMATCH")
        if expected_version and str(artifact.get("model_version") or "") != expected_version:
            blockers.append("MODEL_VERSION_MISMATCH")
        if str(artifact.get("prediction_kind") or "") != "CALIBRATED_OOS":
            blockers.append("NOT_CALIBRATED_OOS")
        if str(artifact.get("config_hash") or "") != current_config_hash:
            blockers.append("CONFIG_HASH_STALE")
        if bool(artifact.get("order_authority")):
            blockers.append("ORDER_AUTHORITY_MUST_BE_FALSE")
        if str(gate.get("status") or "").upper() != "PASS":
            blockers.append("ARTIFACT_GATE_NOT_PASS")
        if gate.get("contract_eligible") is not True:
            blockers.append("ARTIFACT_NOT_CONTRACT_ELIGIBLE")
        if sessions < required_sessions:
            blockers.append("OOS_SESSIONS_INSUFFICIENT")
            reasons.append(f"HORIZON_OOS_SESSIONS_INSUFFICIENT_T{horizon}")
        selected_samples = int(
            selection_evidence.get("selected_oos_sample_count") or 0
        )
        selected_sessions = int(
            selection_evidence.get("selected_oos_session_count") or 0
        )
        required_selected_samples = int(
            minimum_selected_samples.get(str(horizon)) or 1
        )
        if selected_samples < required_selected_samples:
            blockers.append("SELECTED_OOS_SAMPLES_INSUFFICIENT")
        if selected_sessions < required_sessions:
            blockers.append("SELECTED_OOS_SESSIONS_INSUFFICIENT")
        evidence_scope = str(
            oos_evidence.get("execution_evidence_scope")
            or dataset.get("execution_evidence_scope")
            or ""
        )
        label_attestation_required = (
            oos_evidence.get("label_attestation_required_for_execution")
            if "label_attestation_required_for_execution" in oos_evidence
            else dataset.get("label_attestation_required_for_execution")
        )
        executable_verified = (
            oos_evidence.get("executable_verified")
            if "executable_verified" in oos_evidence
            else dataset.get("executable_verified")
        )
        attested_label_count = int(
            oos_evidence.get("qmt_attested_label_count")
            or dataset.get("qmt_attested_label_count")
            or 0
        )
        if evidence_scope != "LONG_HISTORY_OOS_RESEARCH_ONLY":
            blockers.append("OOS_EXECUTION_EVIDENCE_SCOPE_INVALID")
        if label_attestation_required is not True:
            blockers.append("OOS_LABEL_ATTESTATION_POLICY_MISSING")
        if executable_verified is not False:
            blockers.append("OOS_FALSE_EXECUTABLE_VERIFICATION_CLAIM")
        if attested_label_count <= 0:
            blockers.append("OOS_QMT_ATTESTATION_COVERAGE_MISSING")
        item.update({
            "release_id": artifact.get("release_id"),
            "model_key": artifact.get("model_key"),
            "model_version": artifact.get("model_version"),
            "artifact_schema_version": artifact.get("schema_version"),
            "model_protocol": artifact.get("model_protocol"),
            "selection_protocol": selection_evidence.get("protocol"),
            "selection_policy_hash": selection_evidence.get(
                "selection_policy_hash"
            ),
            "economic_evaluation_scope": selection_evidence.get(
                "economic_evaluation_scope"
            ),
            "deployment_candidate_domain_verified": (
                selection_evidence.get(
                    "deployment_candidate_domain_verified"
                )
            ),
            "selected_oos_sample_count": selected_samples,
            "selected_oos_session_count": selected_sessions,
            "minimum_selected_oos_sample_count": required_selected_samples,
            "prediction_kind": artifact.get("prediction_kind"),
            "artifact_hash": artifact.get("artifact_hash"),
            "feature_protocol_hash": (
                artifact.get("feature_protocol_hash")
                or feature.get("feature_protocol_hash")
                or feature.get("hash")
            ),
            "dataset_manifest_hash": (
                artifact.get("dataset_manifest_hash")
                or dict(artifact.get("dataset_manifest") or {}).get("hash")
            ),
            "created_at": artifact.get("created_at"),
            "distinct_oos_session_count": sessions,
            "minimum_oos_session_count": required_sessions,
            "execution_evidence_scope": evidence_scope,
            "label_attestation_required": label_attestation_required,
            "executable_verified": executable_verified,
            "qmt_attested_label_count": attested_label_count,
            "gate_status": gate.get("status"),
            "contract_eligible": gate.get("contract_eligible"),
            "blockers": blockers,
            "ready": not blockers,
        })
        if blockers and f"HORIZON_ARTIFACT_INVALID_T{horizon}" not in reasons:
            reasons.append(f"HORIZON_ARTIFACT_INVALID_T{horizon}")
        results[key] = item
    hashes = {
        key: str(item.get("artifact_hash") or "")
        for key, item in results.items()
    }
    feature_hashes = {
        key: str(item.get("feature_protocol_hash") or "")
        for key, item in results.items()
    }
    independent = bool(
        len(results) == 3
        and all(len(value) == 64 for value in hashes.values())
        and len(set(hashes.values())) == 3
        and all(len(value) == 64 for value in feature_hashes.values())
        and len(set(feature_hashes.values())) == 3
    )
    if not independent:
        reasons.append("HORIZON_ARTIFACTS_NOT_INDEPENDENT")
    ready = bool(
        independent
        and len(results) == 3
        and all(item.get("ready") for item in results.values())
        and not reasons
    )
    paper_deployment_ready = bool(
        ready
        and all(
            item.get("deployment_candidate_domain_verified") is True
            for item in results.values()
        )
    )
    if ready and not paper_deployment_ready:
        reasons.append("SELECTION_DOMAIN_NOT_DEPLOYMENT_VERIFIED")
    return {
        "release_id": release_id,
        "artifacts": results,
        "artifact_hashes": hashes,
        "feature_protocol_hashes": feature_hashes,
        "independent": independent,
        "reason_codes": list(dict.fromkeys(reasons)),
        "historical_v1_runtime_eligible": False,
        "shadow_research_ready": ready,
        "long_history_oos_ready": ready,
        "paper_deployment_ready": paper_deployment_ready,
        "ready": paper_deployment_ready,
    }


def evaluate_scheduler_state(
    tasks: list[Mapping[str, Any]],
    heartbeat: Mapping[str, Any] | None,
    *,
    heartbeats: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = [dict(item) for item in tasks]
    expected_tasks = _expected_layer4_scheduler_tasks()
    reasons: list[str] = []
    task_evaluations: dict[str, Any] = {}
    fenced_task_types: list[str] = []
    if set(expected_tasks) != set(LAYER4_SCHEDULER_TASK_TYPES):
        reasons.append("SHADOW_SCHEDULER_EXPECTED_DEFINITION_INVALID")
    if len(rows) != len(LAYER4_SCHEDULER_TASK_TYPES):
        reasons.append("SHADOW_SCHEDULER_TASK_CARDINALITY_INVALID")

    for task_type in LAYER4_SCHEDULER_TASK_TYPES:
        matches = [
            item for item in rows
            if str(item.get("task_type") or "") == task_type
        ]
        task = matches[0] if len(matches) == 1 else {}
        expected = expected_tasks.get(task_type) or {}
        built_args = (
            build_scheduler_task_args(
                task,
                str(task.get("script_path") or ""),
                "2099-12-31",
            )
            if task else []
        )
        expected_args = (
            build_scheduler_task_args(
                expected,
                str(expected.get("script_path") or ""),
                "2099-12-31",
            )
            if expected else []
        )
        exact_definition = bool(
            task
            and expected
            and int(task.get("enabled") or 0) == 1
            and str(task.get("script_path") or "")
            == str(expected.get("script_path") or "")
            and built_args == expected_args
            and str(task.get("date_param") or "").strip() == ""
            and str(task.get("cron_time") or "")
            == str(expected.get("cron_time") or "")
            and int(task.get("interval_minutes") or 0)
            == int(expected.get("interval_minutes") or 0)
        )
        last_run_success = str(task.get("last_run_status") or "").lower() in {
            "success", "succeeded", "completed"
        }
        if len(matches) != 1:
            reasons.append("SHADOW_SCHEDULER_TASK_CARDINALITY_INVALID")
        if task and int(task.get("enabled") or 0) != 1:
            reasons.append("SHADOW_SCHEDULER_TASK_DISABLED")
            fenced_task_types.append(task_type)
        if task and expected and str(task.get("script_path") or "") != str(
            expected.get("script_path") or ""
        ):
            reasons.append("SHADOW_SCHEDULER_SCRIPT_INVALID")
        if task and built_args != expected_args:
            reasons.append("SHADOW_SCHEDULER_BOUNDS_MISSING")
        if task and "2099-12-31" in built_args:
            reasons.append("SHADOW_SCHEDULER_INJECTS_POSITIONAL_DATE")
        if task and not exact_definition:
            reasons.append("SHADOW_SCHEDULER_TASK_DEFINITION_DRIFT")
        if not last_run_success:
            reasons.append("SHADOW_SCHEDULER_LAST_RUN_NOT_SUCCESS")
        task_evaluations[task_type] = {
            "task": task,
            "expected": expected,
            "built_args_probe": built_args,
            "expected_args": expected_args,
            "exact_definition": exact_definition,
            "last_run_success": last_run_success,
            "ready": bool(exact_definition and last_run_success),
        }

    heartbeat_rows = [
        dict(item) for item in (
            heartbeats
            if heartbeats is not None
            else ([heartbeat] if heartbeat else [])
        )
    ]
    fresh_rows: list[dict[str, Any]] = []
    clock_skew = False
    invalid_heartbeat_contract = False
    for item in heartbeat_rows:
        try:
            age = int(item.get("heartbeat_age_seconds"))
        except (TypeError, ValueError):
            invalid_heartbeat_contract = True
            continue
        try:
            poll = int(item.get("poll_seconds"))
        except (TypeError, ValueError):
            invalid_heartbeat_contract = True
            poll = 60
        if poll <= 0:
            invalid_heartbeat_contract = True
            poll = 60
        if age < 0:
            clock_skew = True
            continue
        if age <= poll * 2:
            fresh_rows.append(item)
    if invalid_heartbeat_contract:
        reasons.append("SCHEDULER_HEARTBEAT_CONTRACT_INVALID")
    heartbeat_row = fresh_rows[0] if len(fresh_rows) == 1 else dict(
        heartbeat or {}
    )
    if clock_skew:
        reasons.append("SCHEDULER_HEARTBEAT_CLOCK_SKEW")
    if not fresh_rows:
        reasons.append("SCHEDULER_HEARTBEAT_STALE")
    if len(fresh_rows) > 1:
        reasons.append("SCHEDULER_MULTIPLE_LIVE_WRITERS")
    if any(
        str(item.get("mode") or "").lower() != PRODUCTION_SCHEDULER_MODE
        for item in fresh_rows
    ):
        reasons.append("SCHEDULER_NOT_STANDALONE")
    writer_fence_active = set(fenced_task_types) == set(
        LAYER4_SCHEDULER_TASK_TYPES
    )
    if writer_fence_active:
        reasons.append("LAYER4_WRITER_FENCE_ACTIVE")
    return {
        "tasks": rows,
        "heartbeat": heartbeat_row,
        "heartbeats": heartbeat_rows,
        "live_heartbeats": fresh_rows,
        "task_evaluations": task_evaluations,
        "authority_contract": scheduler_authority_contract(),
        "writer_fence_active": writer_fence_active,
        "fenced_task_types": fenced_task_types,
        "reason_codes": list(dict.fromkeys(reasons)),
        "ready": not reasons,
    }


def collect_scheduler_readiness(engine: Engine) -> dict[str, Any]:
    try:
        with engine.connect() as connection:
            tasks = [dict(row) for row in connection.execute(text(
                "SELECT id, task_name, task_type, enabled, script_path, "
                "script_args, date_param, cron_time, interval_minutes, "
                "last_run_status, last_run_at, last_run_output "
                "FROM st_scheduled_tasks "
                "WHERE task_type IN ("
                "'trading_v3_counterfactual_audit', "
                "'trading_v3_continuous_calibration') "
                "ORDER BY id"
            )).mappings().all()]
            heartbeat_rows = [dict(item) for item in connection.execute(text(
                "SELECT instance_id, mode, host_name, pid, started_at, "
                "heartbeat_at, TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) "
                "AS heartbeat_age_seconds, poll_seconds, "
                "max_concurrent_tasks FROM st_scheduler_runtime "
                "ORDER BY heartbeat_at DESC"
            )).mappings().all()]
            heartbeat = heartbeat_rows[0] if heartbeat_rows else {}
    except Exception as exc:
        return {
            "tasks": [],
            "heartbeat": {},
            "reason_codes": ["SHADOW_SCHEDULER_QUERY_FAILED"],
            "detail": f"{type(exc).__name__}: {exc}"[:300],
            "ready": False,
        }
    return evaluate_scheduler_state(
        tasks,
        heartbeat,
        heartbeats=heartbeat_rows,
    )


def collect_shadow_runtime(
    engine: Engine,
    *,
    current_config_hash: str,
    current_code_version: str | None = None,
) -> dict[str, Any]:
    resolved_code_version = current_code_version or code_version()[0]
    protocol_params = {
        "artifact_schema_version": CURRENT_HORIZON_ARTIFACT_SCHEMA,
        "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
        "selection_policy_hash": CURRENT_HORIZON_SELECTION_POLICY_HASH,
        "candidate_ledger_schema_version": (
            CANDIDATE_EVALUATION_LEDGER_SCHEMA
        ),
        "config_hash": current_config_hash,
        "code_version": resolved_code_version,
    }
    try:
        with engine.connect() as connection:
            contracts = [dict(row) for row in connection.execute(text(
                "SELECT a.suite_release_id, a.release_id, a.artifact_id, "
                "a.horizon_days, c.prediction_kind, c.model_key, "
                "c.model_version, c.model_artifact_hash, "
                "COUNT(*) AS row_count, "
                "SUM(c.order_authority <> 0) AS authority_violation_count "
                "FROM st_horizon_forecast_contract_v3 c "
                "JOIN st_horizon_model_artifact_v3 a "
                "ON a.artifact_id=c.model_artifact_hash "
                "AND a.model_key=c.model_key "
                "AND a.model_version=c.model_version "
                "AND a.horizon_days=c.horizon_days "
                "WHERE c.prediction_kind='CALIBRATED_OOS' "
                "AND a.artifact_schema_version=:artifact_schema_version "
                "AND a.model_protocol=:model_protocol "
                "AND a.selection_policy_hash=:selection_policy_hash "
                "AND a.candidate_ledger_schema_version="
                ":candidate_ledger_schema_version "
                "AND a.candidate_ledger_content_sha256 IS NOT NULL "
                "AND a.candidate_ledger_row_count > 0 "
                "AND a.ledger_registration_evidence_hash IS NOT NULL "
                "AND a.registration_verification_hash IS NOT NULL "
                "AND a.registration_evidence_hash IS NOT NULL "
                "AND a.artifact_status='OOS_VERIFIED' "
                "AND a.training_receipt_status='PROCESS_VERIFIED' "
                "AND a.config_hash=:config_hash "
                "AND a.code_version=:code_version "
                "AND a.created_at <= UTC_TIMESTAMP(6) "
                "AND a.evidence_valid_until >= UTC_TIMESTAMP(6) "
                "AND a.order_authority=0 "
                "GROUP BY a.suite_release_id, a.release_id, a.artifact_id, "
                "a.horizon_days, c.prediction_kind, c.model_key, "
                "c.model_version, c.model_artifact_hash "
                "ORDER BY a.suite_release_id, a.horizon_days"
            ), protocol_params).mappings().all()]
            artifacts = [dict(row) for row in connection.execute(text(
                "SELECT artifact_id, release_id, suite_release_id, "
                "model_key, model_version, horizon_days, "
                "artifact_schema_version, model_protocol, "
                "selection_policy_hash, candidate_ledger_schema_version, "
                "candidate_ledger_content_sha256, candidate_ledger_row_count, "
                "ledger_registration_evidence_hash, "
                "registration_verification_hash, artifact_status, "
                "training_receipt_status, config_hash, code_version, "
                "evidence_valid_until, order_authority, created_at "
                "FROM st_horizon_model_artifact_v3 "
                "WHERE artifact_schema_version=:artifact_schema_version "
                "AND model_protocol=:model_protocol "
                "AND selection_policy_hash=:selection_policy_hash "
                "AND candidate_ledger_schema_version="
                ":candidate_ledger_schema_version "
                "AND candidate_ledger_content_sha256 IS NOT NULL "
                "AND candidate_ledger_row_count > 0 "
                "AND ledger_registration_evidence_hash IS NOT NULL "
                "AND registration_verification_hash IS NOT NULL "
                "AND registration_evidence_hash IS NOT NULL "
                "AND artifact_status='OOS_VERIFIED' "
                "AND training_receipt_status='PROCESS_VERIFIED' "
                "AND config_hash=:config_hash AND code_version=:code_version "
                "AND created_at <= UTC_TIMESTAMP(6) "
                "AND evidence_valid_until >= UTC_TIMESTAMP(6) "
                "AND order_authority=0 "
                "ORDER BY created_at DESC, artifact_id DESC"
            ), protocol_params).mappings().all()]
            historical_v1_count = int(connection.execute(text(
                "SELECT COUNT(*) FROM st_horizon_model_artifact_v3 "
                "WHERE artifact_schema_version=:historical_schema"
            ), {
                "historical_schema": HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
            }).scalar() or 0)
            historical_v2_count = int(connection.execute(text(
                "SELECT COUNT(*) FROM st_horizon_model_artifact_v3 "
                "WHERE artifact_schema_version=:historical_schema"
            ), {
                "historical_schema": HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
            }).scalar() or 0)
            latest_outcome_row = connection.execute(text(
                "SELECT o.*, c.model_artifact_hash, a.suite_release_id "
                "FROM st_horizon_outcome_v3 o JOIN "
                "st_horizon_forecast_contract_v3 c "
                "ON c.contract_id=o.contract_id "
                "JOIN st_horizon_model_artifact_v3 a "
                "ON a.artifact_id=c.model_artifact_hash "
                "WHERE a.artifact_schema_version=:artifact_schema_version "
                "AND a.model_protocol=:model_protocol "
                "AND a.selection_policy_hash=:selection_policy_hash "
                "AND a.candidate_ledger_schema_version="
                ":candidate_ledger_schema_version "
                "AND a.candidate_ledger_content_sha256 IS NOT NULL "
                "AND a.candidate_ledger_row_count > 0 "
                "AND a.ledger_registration_evidence_hash IS NOT NULL "
                "AND a.registration_verification_hash IS NOT NULL "
                "AND a.registration_evidence_hash IS NOT NULL "
                "AND a.artifact_status='OOS_VERIFIED' "
                "AND a.training_receipt_status='PROCESS_VERIFIED' "
                "AND a.config_hash=:config_hash "
                "AND a.code_version=:code_version "
                "AND a.order_authority=0 "
                "ORDER BY o.observed_at DESC, o.outcome_id DESC LIMIT 1"
            ), protocol_params).mappings().first()
            latest_learning_row = connection.execute(text(
                "SELECT learning_run_id, learning_status, sample_count, "
                "t1_sample_count, t5_sample_count, t20_sample_count, "
                "t1_evidence_ready, t5_evidence_ready, t20_evidence_ready, "
                "evidence_source, config_hash, code_version, "
                "model_artifact_hashes_json, can_activate_model, "
                "order_authority, evaluated_at "
                "FROM st_counterfactual_learning_run_v3 "
                "WHERE config_hash=:config_hash AND code_version=:code_version "
                "ORDER BY evaluated_at DESC, learning_run_id DESC LIMIT 1"
            ), protocol_params).mappings().first()
            releases = [dict(row) for row in connection.execute(text(
                "SELECT s.release_id, s.model_key, s.model_version, "
                "s.horizon_days, s.current_stage, s.config_hash, "
                "s.order_authority, s.occurred_at FROM st_shadow_release_v3 s "
                "JOIN (SELECT release_id, MAX(transition_sequence) seq "
                "FROM st_shadow_release_v3 GROUP BY release_id) latest "
                "ON latest.release_id=s.release_id "
                "AND latest.seq=s.transition_sequence ORDER BY s.horizon_days"
            )).mappings().all()]
    except Exception as exc:
        return {
            "contracts": [], "latest_outcome": {}, "latest_learning": {},
            "releases": [], "current_artifacts": [],
            "historical_v1_artifact_count": 0,
            "reason_codes": ["SHADOW_RUNTIME_QUERY_FAILED"],
            "detail": f"{type(exc).__name__}: {exc}"[:300], "ready": False,
        }
    outcome = dict(latest_outcome_row) if latest_outcome_row else {}
    learning = dict(latest_learning_row) if latest_learning_row else {}
    reasons: list[str] = []
    suites: dict[str, dict[int, dict[str, Any]]] = {}
    corrupt_suites: set[str] = set()
    for artifact in artifacts:
        suite_id = str(artifact.get("suite_release_id") or "")
        horizon = int(artifact.get("horizon_days") or 0)
        if not suite_id or horizon not in REQUIRED_HORIZONS:
            continue
        members = suites.setdefault(suite_id, {})
        if horizon in members:
            corrupt_suites.add(suite_id)
        else:
            members[horizon] = artifact
    complete_suites = [
        (suite_id, members)
        for suite_id, members in suites.items()
        if suite_id not in corrupt_suites
        and set(members) == set(REQUIRED_HORIZONS)
    ]
    selected_suite_id = ""
    selected_artifacts: dict[int, dict[str, Any]] = {}
    if complete_suites:
        selected_suite_id, selected_artifacts = max(
            complete_suites,
            key=lambda item: (
                min(str(member.get("created_at") or "")
                    for member in item[1].values()),
                item[0],
            ),
        )
    else:
        reasons.append("CURRENT_V2_ARTIFACT_SUITE_INCOMPLETE")
    selected_contracts = [
        item for item in contracts
        if str(item.get("suite_release_id") or "") == selected_suite_id
    ]
    observed_horizons = {
        int(item.get("horizon_days") or 0) for item in selected_contracts
    }
    if observed_horizons != set(REQUIRED_HORIZONS):
        reasons.append("FORWARD_SHADOW_HORIZONS_INCOMPLETE")
    if any(
        int(item.get("authority_violation_count") or 0)
        for item in selected_contracts
    ):
        reasons.append("FORWARD_SHADOW_ORDER_AUTHORITY_VIOLATION")
    if not outcome:
        reasons.append("LATEST_SHADOW_OUTCOME_MISSING")
    elif str(outcome.get("suite_release_id") or "") != selected_suite_id:
        reasons.append("LATEST_SHADOW_OUTCOME_SUITE_MISMATCH")
    elif str(outcome.get("execution_feasibility") or "") != "UNVERIFIED_RESEARCH":
        reasons.append("SHADOW_OUTCOME_FALSE_EXECUTION_CLAIM")
    if not learning:
        reasons.append("CONTINUOUS_CALIBRATION_RUN_MISSING")
    expected_release_ids = {
        str(item.get("release_id") or "")
        for item in selected_artifacts.values()
    }
    selected_releases = [
        item for item in releases
        if str(item.get("release_id") or "") in expected_release_ids
    ]
    if len(selected_releases) != 3:
        reasons.append("SHADOW_RELEASE_SUITE_INCOMPLETE")
    if any(int(item.get("order_authority") or 0) for item in selected_releases):
        reasons.append("SHADOW_RELEASE_ORDER_AUTHORITY_VIOLATION")
    if any(
        str(item.get("current_stage") or "")
        not in {"SHADOW", "CALIBRATION_REVIEW"}
        for item in selected_releases
    ):
        reasons.append("SHADOW_RELEASE_STAGE_INVALID_FOR_DIAGNOSTIC_V2")
    if learning:
        raw_hashes = learning.get("model_artifact_hashes_json") or "{}"
        try:
            learning_hashes = (
                dict(raw_hashes)
                if isinstance(raw_hashes, Mapping)
                else dict(json.loads(str(raw_hashes)))
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            learning_hashes = {}
        expected_hashes = {
            str(item["release_id"]): str(item["artifact_id"])
            for item in selected_artifacts.values()
        }
        if learning_hashes != expected_hashes:
            reasons.append("CONTINUOUS_CALIBRATION_SUITE_MISMATCH")
        if bool(learning.get("can_activate_model")) or bool(
            learning.get("order_authority")
        ):
            reasons.append("CONTINUOUS_CALIBRATION_AUTHORITY_VIOLATION")
    return {
        "current_protocol": {
            "artifact_schema_version": CURRENT_HORIZON_ARTIFACT_SCHEMA,
            "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
            "selection_policy_hash": CURRENT_HORIZON_SELECTION_POLICY_HASH,
            "candidate_ledger_schema_version": (
                CANDIDATE_EVALUATION_LEDGER_SCHEMA
            ),
            "config_hash": current_config_hash,
            "code_version": resolved_code_version,
        },
        "selected_suite_release_id": selected_suite_id,
        "current_artifacts": list(selected_artifacts.values()),
        "historical_v1_artifact_count": historical_v1_count,
        "historical_v1_runtime_eligible": False,
        "historical_v2_artifact_count": historical_v2_count,
        "historical_v2_runtime_eligible": False,
        "contracts": selected_contracts,
        "latest_outcome": outcome,
        "latest_learning": learning,
        "releases": selected_releases,
        "reason_codes": reasons,
        "ready": not reasons,
    }


def collect_kline_evidence(
    engine: Engine,
    *,
    latest_outcome: Mapping[str, Any] | None,
) -> dict[str, Any]:
    outcome = dict(latest_outcome or {})
    reasons: list[str] = []
    try:
        with engine.connect() as connection:
            identity = connection.execute(text(
                "SELECT DATABASE() AS database_name, CURRENT_USER() AS runtime_user"
            )).mappings().first()
            first = connection.execute(text(
                "SELECT trade_date FROM sm_stock_kline FORCE INDEX "
                "(idx_date_ktype) WHERE k_type=1 AND adjust_type=0 "
                "ORDER BY trade_date ASC LIMIT 1"
            )).scalar()
            latest = connection.execute(text(
                "SELECT trade_date FROM sm_stock_kline FORCE INDEX "
                "(idx_date_ktype) WHERE k_type=1 AND adjust_type=0 "
                "ORDER BY trade_date DESC LIMIT 1"
            )).scalar()
            attestations = [dict(row) for row in connection.execute(text(
                "SELECT run_id, provider, start_date, end_date, status, "
                "target_rows, qmt_rows, matched_rows, missing_qmt_rows, "
                "mismatched_rows, finished_at FROM qmt_kline_attestation_run "
                "WHERE status='COMPLETED' ORDER BY end_date DESC, finished_at DESC"
            )).mappings().all()]
            outcome_rows: list[dict[str, Any]] = []
            evidence = outcome.get("market_evidence_json") or {}
            if isinstance(evidence, str):
                try:
                    evidence = json.loads(evidence)
                except json.JSONDecodeError:
                    evidence = {}
            dates = [
                str(item.get("trade_date") or "")[:10]
                for item in dict(evidence or {}).get("bars") or []
                if isinstance(item, Mapping) and item.get("trade_date")
            ]
            stock_code = str(outcome.get("stock_code") or "")
            if stock_code and dates:
                statement = text(
                    "SELECT trade_date, data_source, quality_status "
                    "FROM sm_stock_kline WHERE stock_code=:stock_code "
                    "AND k_type=1 AND adjust_type=0 AND trade_date IN :dates"
                ).bindparams(bindparam("dates", expanding=True))
                outcome_rows = [dict(row) for row in connection.execute(
                    statement, {"stock_code": stock_code, "dates": dates}
                ).mappings().all()]
    except Exception as exc:
        return {
            "permission_verified": False,
            "reason_codes": ["KLINE_PERMISSION_OR_QUERY_FAILED"],
            "detail": f"{type(exc).__name__}: {exc}"[:300],
            "latest_outcome_qmt_attested": False,
            "ready": False,
        }
    clean_runs = [
        item for item in attestations
        if int(item.get("target_rows") or 0) > 0
        and int(item.get("matched_rows") or 0)
        == int(item.get("target_rows") or 0)
        and int(item.get("missing_qmt_rows") or 0) == 0
        and int(item.get("mismatched_rows") or 0) == 0
    ]
    evidence = outcome.get("market_evidence_json") or {}
    if isinstance(evidence, str):
        try:
            evidence = json.loads(evidence)
        except json.JSONDecodeError:
            evidence = {}
    expected_bar_count = len(dict(evidence or {}).get("bars") or [])
    latest_outcome_attested = evaluate_latest_outcome_attestation(
        outcome,
        outcome_rows,
        expected_bar_count=expected_bar_count,
    )
    if outcome and not latest_outcome_attested:
        reasons.append("LATEST_SHADOW_OUTCOME_QMT_ATTESTATION_MISSING")
    if not clean_runs:
        reasons.append("QMT_ATTESTED_HISTORY_MISSING")
    return {
        "permission_verified": True,
        "database_identity": dict(identity or {}),
        "raw_history_start": first,
        "raw_history_end": latest,
        "clean_attestation_runs": clean_runs,
        "latest_outcome_bar_quality": outcome_rows,
        "latest_outcome_qmt_attested": latest_outcome_attested,
        "execution_feasibility": outcome.get("execution_feasibility"),
        "reason_codes": reasons,
        "ready": not reasons,
    }


def evaluate_latest_outcome_attestation(
    outcome: Mapping[str, Any] | None,
    rows: list[Mapping[str, Any]],
    *,
    expected_bar_count: int,
) -> bool:
    """Return true only when every frozen outcome bar is row-attested by QMT.

    This does not promote ``execution_feasibility``.  Capacity, suspension,
    limit-state and actual fills remain separate execution evidence.
    """

    return bool(
        outcome
        and expected_bar_count > 0
        and len(rows) == expected_bar_count
        and all(
            str(item.get("quality_status") or "") == "QMT_ATTESTED"
            and bool(str(item.get("data_source") or "").strip())
            for item in rows
        )
    )


def _url_origin(value: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(value)
    return (
        parsed.scheme.casefold(),
        str(parsed.hostname or "").casefold(),
        parsed.port,
    )


def _local_api_base_url_error(value: str) -> str:
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        return "invalid URL or port"
    if parsed.scheme.casefold() not in {"http", "https"}:
        return "scheme must be http or https"
    if str(parsed.hostname or "").casefold() not in {"127.0.0.1", "::1"}:
        return "host must be an IP loopback literal"
    if parsed.username is not None or parsed.password is not None:
        return "userinfo is not allowed"
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        return "base URL must not contain a path, query, or fragment"
    return ""


def _fetch_one(base_url: str, path: str, token: str) -> dict[str, Any]:
    requested_url = base_url.rstrip("/") + path
    request = Request(
        requested_url,
        headers={"X-ProBigA-Admin-Token": token} if token else {},
        method="GET",
    )
    try:
        with urlopen(request, timeout=30) as response:
            final_url = str(response.geturl() or requested_url)
            if _url_origin(final_url) != _url_origin(requested_url):
                return {"error": "redirect escaped the local API origin"}
            return {
                "http_status": int(response.status),
                "payload": json.loads(response.read().decode("utf-8")),
            }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"[:300]}


def evaluate_page_get_truth(
    responses: Mapping[str, Any],
    *,
    config_version: str,
    config_hash: str,
) -> dict[str, Any]:
    reasons: list[str] = []
    summary: dict[str, Any] = {}
    for name in PAGE_GET_PATHS:
        response = dict(responses.get(name) or {})
        payload = response.get("payload")
        if not isinstance(payload, Mapping) or response.get("http_status") != 200:
            reasons.append(f"PAGE_GET_{name.upper()}_UNAVAILABLE")
            summary[name] = {"available": False, "error": response.get("error")}
            continue
        envelope = dict(payload)
        data = envelope.get("data")
        data = dict(data) if isinstance(data, Mapping) else {}
        order_authority = data.get("order_authority")
        real_order_allowed = data.get("real_order_allowed")
        safe = bool(
            str(envelope.get("config_version") or "") == config_version
            and str(envelope.get("config_hash") or "") == config_hash
            and envelope.get("real_trading_enabled") is False
            and (
                "order_authority" not in data
                or order_authority is False
            )
            and (
                "real_order_allowed" not in data
                or real_order_allowed is False
            )
            and str(envelope.get("status") or "").lower()
            not in {"error", "unavailable"}
            and str(data.get("status") or "").upper() != "UNAVAILABLE"
        )
        if not safe:
            reasons.append(f"PAGE_GET_{name.upper()}_TRUTH_INVALID")
        summary[name] = {
            "available": True,
            "safe": safe,
            "envelope_status": envelope.get("status"),
            "data_status": data.get("status"),
            "order_authority": data.get("order_authority"),
            "real_order_allowed": data.get("real_order_allowed"),
            "blocks": data.get("blocks") if name == "readiness" else None,
        }
    readiness_payload = dict(
        dict(responses.get("readiness") or {}).get("payload") or {}
    )
    readiness_data = dict(readiness_payload.get("data") or {})
    readiness_ready = bool(
        readiness_payload.get("status") == "ok"
        and readiness_data.get("paper_ready") is True
    )
    if not readiness_ready:
        reasons.append("PAGE_GET_READINESS_NOT_READY")
    return {
        "endpoints": summary,
        "readiness_ready": readiness_ready,
        "reason_codes": list(dict.fromkeys(reasons)),
        "ready": not reasons,
    }


def collect_page_get_truth(
    *,
    config_version: str,
    config_hash: str,
    base_url: str | None = None,
) -> dict[str, Any]:
    resolved_base = str(
        base_url
        or os.environ.get("PROBIGA_LOCAL_API_BASE_URL")
        or "http://127.0.0.1:8000"
    ).rstrip("/")
    base_error = _local_api_base_url_error(resolved_base)
    if base_error:
        return {
            "base_url": resolved_base,
            "endpoints": {},
            "readiness_ready": False,
            "reason_codes": ["PAGE_GET_BASE_URL_NOT_LOOPBACK"],
            "detail": base_error,
            "ready": False,
        }
    token = str(get_admin_auth_config().get("token") or "")
    responses: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=len(PAGE_GET_PATHS)) as executor:
        futures = {
            executor.submit(_fetch_one, resolved_base, path, token): name
            for name, path in PAGE_GET_PATHS.items()
        }
        for future in as_completed(futures):
            responses[futures[future]] = future.result()
    result = evaluate_page_get_truth(
        responses,
        config_version=config_version,
        config_hash=config_hash,
    )
    result["base_url"] = resolved_base
    return result


def collect_fourth_layer_readiness(
    primary: Engine,
    kline: Engine,
    *,
    config: Mapping[str, Any],
    current_config_hash: str,
    api_base_url: str | None = None,
) -> dict[str, Any]:
    migration = collect_migration_readiness(primary)
    artifacts = collect_artifact_readiness(
        config,
        current_config_hash=current_config_hash,
    )
    scheduler = collect_scheduler_readiness(primary)
    runtime = collect_shadow_runtime(
        primary,
        current_config_hash=current_config_hash,
    )
    kline_evidence = collect_kline_evidence(
        kline,
        latest_outcome=runtime.get("latest_outcome") or {},
    )
    page_get = collect_page_get_truth(
        config_version=str(config.get("strategy_version") or ""),
        config_hash=current_config_hash,
        base_url=api_base_url,
    )
    checks = {
        "shadow_migration_ready": bool(migration.get("ready")),
        "independent_horizon_artifacts_ready": bool(artifacts.get("ready")),
        "long_history_oos_ready": bool(
            artifacts.get("long_history_oos_ready")
        ),
        "shadow_scheduler_ready": bool(scheduler.get("ready")),
        "forward_shadow_runtime_ready": bool(runtime.get("ready")),
        "kline_permission_verified": bool(
            kline_evidence.get("permission_verified")
        ),
        "latest_shadow_outcome_qmt_attested": bool(
            kline_evidence.get("latest_outcome_qmt_attested")
        ),
        "page_get_truth_ready": bool(page_get.get("ready")),
    }
    return {
        "migration": migration,
        "artifacts": artifacts,
        "scheduler": scheduler,
        "forward_shadow": runtime,
        "kline_evidence": kline_evidence,
        "page_get_truth": page_get,
        "checklist": checks,
        "activation_status": "PASS" if all(checks.values()) else "BLOCKED",
    }


__all__ = [
    "collect_artifact_readiness",
    "collect_fourth_layer_readiness",
    "collect_kline_evidence",
    "collect_migration_readiness",
    "collect_page_get_truth",
    "collect_scheduler_readiness",
    "collect_shadow_runtime",
    "evaluate_migration_state",
    "evaluate_latest_outcome_attestation",
    "evaluate_page_get_truth",
    "evaluate_scheduler_state",
]
