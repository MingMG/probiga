from __future__ import annotations

import hashlib
import json
import math
import sys
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .config import PROJECT_ROOT
from .config import config_hash as current_config_hash
from .config import load_v3_config
from .horizon_contracts import HorizonForecastContract, HorizonOutcomeEvidence
from .horizon_candidate_ledger_schema import (
    CANDIDATE_EVALUATION_LEDGER_SCHEMA,
    CANDIDATE_LEDGER_REGISTRATION_PROTOCOL,
    CURRENT_HORIZON_ARTIFACT_SCHEMA,
    CURRENT_HORIZON_MODEL_PROTOCOL,
    CURRENT_HORIZON_SELECTION_POLICY_HASH,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
    PROCESS_VERIFIED_LEDGER_REGISTRATION_PROTOCOL,
)
from .release_governance import (
    CalibrationGateDecision,
    ContinuousCalibrationEvidence,
    ReleaseTransition,
    evaluate_continuous_calibration,
)
from .versioning import code_version


MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
PROCESS_TRAINING_RECEIPT_SCHEMA = (
    "probiga.trading-v3.process-training-receipt.v1"
)
TRAINING_RECEIPT_UNVERIFIED = "TRAINING_RECEIPT_UNVERIFIED"
FORWARD_SHADOW_BINDING_PROTOCOL = (
    "probiga.trading-v3.forward-shadow-artifact-binding.v1"
)
QMT_OUTCOME_ATTESTATION_PROTOCOL = (
    "probiga.trading-v3.qmt-attested-outcome-bars.v1"
)

_LEDGER_VERIFICATION_KEYS = frozenset({
    "protocol",
    "artifact_hash",
    "ledger_schema",
    "ledger_content_sha256",
    "canonical_records_sha256",
    "row_count",
    "session_count",
    "evaluation_row_count",
    "evaluation_session_count",
    "fold_count",
    "selection_policy_hash",
    "selection_evidence_hash",
    "selected_ledger_hash",
    "selected_oos_sample_count",
    "selected_oos_session_count",
    "net_expectancy_after_cost_pct",
    "profit_factor",
    "cost_coverage_ratio",
    "fold_oos_prediction_hashes",
    "registration_evidence_hash",
})


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _digest_text(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if (
        len(result) != 64
        or any(character not in "0123456789abcdef" for character in result)
    ):
        raise ValueError(f"{field} must be a 64-character SHA-256 digest")
    return result


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("persisted Shadow timestamps must include a timezone")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _row_dict(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _decimal_projection_matches(actual: Any, expected: Any) -> bool:
    """Compare a JSON metric with its DECIMAL(18,8) SQL projection."""

    if expected is None:
        return actual is None
    try:
        actual_number = float(actual)
        expected_number = float(expected)
    except (TypeError, ValueError):
        return False
    return (
        math.isfinite(actual_number)
        and math.isfinite(expected_number)
        and math.isclose(
            actual_number,
            expected_number,
            rel_tol=0.0,
            abs_tol=5.1e-9,
        )
    )


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _datetime_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        raw = str(value or "").strip()
        result = datetime.fromisoformat(
            raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        )
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _market_aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value))
    if result.tzinfo is None:
        result = result.replace(tzinfo=MARKET_TIMEZONE)
    return result


def _unverified_training_receipt(
    document: Mapping[str, Any],
) -> dict[str, Any]:
    body = {
        "schema_version": PROCESS_TRAINING_RECEIPT_SCHEMA,
        "status": "UNVERIFIED",
        "reason": TRAINING_RECEIPT_UNVERIFIED,
        "suite_release_id": str(document.get("suite_release_id") or ""),
        "artifact_hash": _digest_text(
            document.get("artifact_hash"), "artifact_hash"
        ),
    }
    return {**body, "receipt_hash": _hash(body)}


def _training_receipt_projection(
    document: Mapping[str, Any],
    training_receipt: Mapping[str, Any] | None,
    *,
    verify_current_trainer: bool,
) -> dict[str, Any]:
    """Validate process provenance independently from artifact self-hashes."""

    if training_receipt is None:
        return _unverified_training_receipt(document)
    receipt = dict(training_receipt)
    status = str(receipt.get("status") or "").strip().upper()
    if status == "UNVERIFIED":
        expected = _unverified_training_receipt(document)
        if _json(receipt) != _json(expected):
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_UNVERIFIED_INVALID")
        return expected
    if status != "PROCESS_VERIFIED":
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STATUS_INVALID")
    required = {
        "schema_version",
        "status",
        "suite_release_id",
        "artifact_hashes",
        "config_hash",
        "code_version",
        "artifact_code_hash",
        "training_cutoff",
        "trainer_script",
        "trainer_script_hash",
        "argv",
        "exit_code",
        "stdout_sha256",
        "stdout_text",
        "completed_at",
        "process_nonce",
        "receipt_hash",
    }
    if set(receipt) != required:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_SHAPE_INVALID")
    if receipt.get("schema_version") != PROCESS_TRAINING_RECEIPT_SCHEMA:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_SCHEMA_INVALID")
    claimed_hash = _digest_text(
        receipt.get("receipt_hash"), "training_receipt.receipt_hash"
    )
    body = {
        key: value for key, value in receipt.items() if key != "receipt_hash"
    }
    if _hash(body) != claimed_hash:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_HASH_INVALID")

    suite_release_id = str(document.get("suite_release_id") or "")
    training_cutoff = str(document.get("training_cutoff") or "")
    artifact_hash = _digest_text(document.get("artifact_hash"), "artifact_hash")
    if str(receipt.get("suite_release_id") or "") != suite_release_id:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_SUITE_MISMATCH")
    process_nonce = str(receipt.get("process_nonce") or "").strip().lower()
    if len(process_nonce) != 64 or any(
        character not in "0123456789abcdef" for character in process_nonce
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_NONCE_INVALID")
    if str(receipt.get("training_cutoff") or "") != training_cutoff:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_CUTOFF_MISMATCH")
    if _digest_text(receipt.get("config_hash"), "receipt.config_hash") != str(
        document.get("config_hash") or ""
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_CONFIG_MISMATCH")
    if str(receipt.get("code_version") or "") != str(
        document.get("code_version") or ""
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_CODE_VERSION_MISMATCH")
    if _digest_text(
        receipt.get("artifact_code_hash"), "receipt.artifact_code_hash"
    ) != str(document.get("code_hash") or ""):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_CODE_HASH_MISMATCH")
    artifact_hashes_raw = receipt.get("artifact_hashes")
    if not isinstance(artifact_hashes_raw, Mapping):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_ARTIFACTS_INVALID")
    artifact_hashes = {
        str(key): _digest_text(value, f"receipt.artifact_hashes.{key}")
        for key, value in artifact_hashes_raw.items()
    }
    if set(artifact_hashes) != {"1", "5", "20"}:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_SUITE_INCOMPLETE")
    horizon_key = str(int(document.get("horizon_days") or 0))
    if artifact_hashes.get(horizon_key) != artifact_hash:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_ARTIFACT_MISMATCH")
    if int(receipt.get("exit_code") if receipt.get("exit_code") is not None else -1) != 0:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_EXIT_INVALID")

    argv_raw = receipt.get("argv")
    if not isinstance(argv_raw, list) or not all(
        isinstance(item, str) and item for item in argv_raw
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_ARGV_INVALID")
    argv = list(argv_raw)
    if len(argv) != 14 or len(argv[2:]) % 2:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_ARGV_INVALID")
    trainer_script = Path(str(receipt.get("trainer_script") or "")).resolve()
    if Path(argv[1]).resolve() != trainer_script:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_ARGV_SCRIPT_MISMATCH")
    flags: dict[str, str] = {}
    for index in range(2, len(argv), 2):
        flag, value = argv[index], argv[index + 1]
        if flag in flags:
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_ARGV_DUPLICATE")
        flags[flag] = value
    training_config = dict(
        dict(load_v3_config().get("multi_horizon_forecasts") or {}).get(
            "training_policy"
        )
        or {}
    )
    configured_history_start = _date_value(
        training_config.get("history_start")
    ).isoformat()
    artifact_window = dict(document.get("training_window") or {})
    if (
        artifact_window.get("protocol")
        != training_config.get("training_window_protocol")
        or artifact_window.get("configured_history_start")
        != configured_history_start
        or artifact_window.get("signal_start")
        != configured_history_start
        or artifact_window.get("status")
        != "FROZEN_DEFAULT_TRAINING_WINDOW"
        or artifact_window.get("is_current_config_default") is not True
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_WINDOW_MISMATCH")
    expected_flags = {
        "--start": configured_history_start,
        "--end": training_cutoff,
        "--training-cutoff": training_cutoff,
        "--release-id": suite_release_id,
        "--output-root": flags.get("--output-root", ""),
        "--max-stocks": "0",
    }
    if flags != expected_flags:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_ARGV_POLICY_MISMATCH")
    expected_script = (
        PROJECT_ROOT / "tools" / "train_trading_v3_horizon_models.py"
    ).resolve()
    trainer_script_hash = _digest_text(
        receipt.get("trainer_script_hash"), "receipt.trainer_script_hash"
    )
    if verify_current_trainer:
        if trainer_script != expected_script:
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_TRAINER_MISMATCH")
        if Path(argv[0]).resolve() != Path(sys.executable).resolve():
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_PYTHON_MISMATCH")
        if (
            not expected_script.is_file()
            or hashlib.sha256(expected_script.read_bytes()).hexdigest()
            != trainer_script_hash
        ):
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_TRAINER_HASH_MISMATCH")

    stdout_text = receipt.get("stdout_text")
    if not isinstance(stdout_text, str):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_INVALID")
    if hashlib.sha256(stdout_text.encode("utf-8")).hexdigest() != _digest_text(
        receipt.get("stdout_sha256"), "receipt.stdout_sha256"
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_HASH_MISMATCH")
    try:
        stdout = json.loads(stdout_text)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_INVALID") from exc
    if not isinstance(stdout, Mapping):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_INVALID")
    models_raw = stdout.get("models")
    if not isinstance(models_raw, list) or len(models_raw) != 3:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_SUITE_INVALID")
    models: dict[str, Mapping[str, Any]] = {}
    for item in models_raw:
        if not isinstance(item, Mapping):
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_SUITE_INVALID")
        key = str(int(item.get("horizon_days") or 0))
        if key in models:
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_SUITE_INVALID")
        models[key] = item
    if set(models) != {"1", "5", "20"}:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_SUITE_INVALID")
    if (
        str(stdout.get("status") or "") not in {"PASS", "BLOCK"}
        or str(stdout.get("release_id") or "") != suite_release_id
        or stdout.get("reused_immutable_release") is not False
        or str(stdout.get("universe_scope") or "")
        != "FULL_A_SHARE_POINT_IN_TIME"
        or stdout.get("automatic_promotion_allowed") is not False
        or stdout.get("order_authority") is not False
        or str(stdout.get("training_window_protocol") or "")
        != str(training_config.get("training_window_protocol") or "")
        or str(stdout.get("configured_history_start") or "")
        != configured_history_start
        or str(stdout.get("signal_start") or "")
        != configured_history_start
        or str(stdout.get("training_window_status") or "")
        != "FROZEN_DEFAULT_TRAINING_WINDOW"
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_POLICY_MISMATCH")
    output_root = Path(flags["--output-root"]).resolve()
    approved_output_root = (
        PROJECT_ROOT / "artifacts" / "trading_v3" / "horizon_models"
    ).resolve()
    if output_root != approved_output_root:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_OUTPUT_ROOT_UNAPPROVED")
    if Path(str(stdout.get("release_root") or "")).resolve() != (
        output_root / suite_release_id
    ).resolve():
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_OUTPUT_ROOT_MISMATCH")
    for key, item in models.items():
        if (
            str(item.get("schema_version") or "")
            != CURRENT_HORIZON_ARTIFACT_SCHEMA
            or str(item.get("model_protocol") or "")
            != CURRENT_HORIZON_MODEL_PROTOCOL
            or _digest_text(item.get("artifact_hash"), "stdout.artifact_hash")
            != artifact_hashes[key]
            or str(item.get("suite_release_id") or "") != suite_release_id
            or _digest_text(item.get("config_hash"), "stdout.config_hash")
            != str(document.get("config_hash") or "")
            or str(item.get("code_version") or "")
            != str(document.get("code_version") or "")
            or _digest_text(item.get("code_hash"), "stdout.code_hash")
            != str(document.get("code_hash") or "")
            or str(item.get("training_cutoff") or "") != training_cutoff
            or _json(dict(item.get("training_window") or {}))
            != _json(artifact_window)
        ):
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_BINDING_MISMATCH")
        model_created_at = _datetime_utc(item.get("created_at"))
        if model_created_at > _datetime_utc(receipt.get("completed_at")):
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_CLOCK_INVALID")
    current_model = models[horizon_key]
    current_ledger_reference = current_model.get(
        "candidate_evaluation_ledger"
    )
    artifact_ledger_reference = document.get(
        "candidate_evaluation_ledger"
    )
    if (
        not isinstance(current_ledger_reference, Mapping)
        or not isinstance(artifact_ledger_reference, Mapping)
        or _json(dict(current_ledger_reference))
        != _json(dict(artifact_ledger_reference))
    ):
        raise RuntimeError(
            "HORIZON_TRAINING_RECEIPT_LEDGER_BINDING_MISMATCH"
        )
    if (
        str(current_model.get("release_id") or "")
        != str(document.get("release_id") or "")
        or str(current_model.get("model_key") or "")
        != str(document.get("model_key") or "")
        or str(current_model.get("model_version") or "")
        != str(document.get("model_version") or "")
        or str(document.get("schema_version") or "")
        != CURRENT_HORIZON_ARTIFACT_SCHEMA
        or str(document.get("model_protocol") or "")
        != CURRENT_HORIZON_MODEL_PROTOCOL
        or str(
            dict(document.get("selection_policy") or {}).get(
                "selection_policy_hash"
            )
            or ""
        )
        != CURRENT_HORIZON_SELECTION_POLICY_HASH
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_MODEL_IDENTITY_MISMATCH")
    expected_status = (
        "PASS"
        if all(
            str(item.get("gate_status") or "") == "PASS"
            for item in models.values()
        )
        else "BLOCK"
    )
    if stdout.get("status") != expected_status:
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_STDOUT_GATE_MISMATCH")
    completed_at = _datetime_utc(receipt.get("completed_at"))
    artifact_created_at = _datetime_utc(document.get("created_at"))
    if (
        completed_at < artifact_created_at
        or _date_value(document.get("training_cutoff"))
        > artifact_created_at.astimezone(MARKET_TIMEZONE).date()
    ):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_CLOCK_INVALID")
    if verify_current_trainer:
        now = datetime.now(timezone.utc)
        if completed_at > now.replace(microsecond=999999) or (
            now - completed_at
        ).total_seconds() > 300:
            raise RuntimeError("HORIZON_TRAINING_RECEIPT_COMPLETION_NOT_FRESH")
    return receipt


def _candidate_ledger_registration_projection(
    document: Mapping[str, Any],
    *,
    receipt_status: str,
    training_receipt_hash: str,
    artifact_root: str | Path | None,
    persisted: Mapping[str, Any] | None,
    require_current_code: bool,
    require_current_config: bool,
) -> dict[str, Any]:
    reference = document.get("candidate_evaluation_ledger")
    if not isinstance(reference, Mapping):
        raise RuntimeError("HORIZON_CANDIDATE_LEDGER_REFERENCE_MISSING")
    schema_version = str(reference.get("schema_version") or "")
    if schema_version != CANDIDATE_EVALUATION_LEDGER_SCHEMA:
        raise RuntimeError("HORIZON_CANDIDATE_LEDGER_SCHEMA_INVALID")
    content_sha256 = _digest_text(
        reference.get("content_sha256"),
        "candidate_evaluation_ledger.content_sha256",
    )
    try:
        row_count = int(reference.get("row_count"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("HORIZON_CANDIDATE_LEDGER_ROW_COUNT_INVALID") from exc
    if row_count < 0:
        raise RuntimeError("HORIZON_CANDIDATE_LEDGER_ROW_COUNT_INVALID")

    projection: dict[str, Any] = {
        "candidate_ledger_schema_version": schema_version,
        "candidate_ledger_content_sha256": content_sha256,
        "candidate_ledger_row_count": row_count,
        "ledger_registration_evidence_hash": None,
        "registration_verification_hash": None,
    }
    if receipt_status != "PROCESS_VERIFIED":
        if persisted is not None and any(
            persisted.get(field) is not None
            for field in (
                "ledger_registration_evidence_hash",
                "registration_verification_hash",
            )
        ):
            raise RuntimeError(
                "HORIZON_UNVERIFIED_LEDGER_PROJECTION_CLAIMS_VERIFICATION"
            )
        return projection

    if row_count <= 0:
        raise RuntimeError("HORIZON_CANDIDATE_LEDGER_ROW_COUNT_INVALID")

    ledger_registration_hash: str
    if artifact_root is not None:
        from .horizon_models import verify_candidate_evaluation_ledger

        verification = verify_candidate_evaluation_ledger(
            document,
            artifact_root,
            require_current_code=require_current_code,
            require_current_config=require_current_config,
        )
        if set(verification) != _LEDGER_VERIFICATION_KEYS:
            raise RuntimeError(
                "HORIZON_CANDIDATE_LEDGER_VERIFICATION_SHAPE_INVALID"
            )
        if (
            verification.get("protocol")
            != CANDIDATE_LEDGER_REGISTRATION_PROTOCOL
            or _digest_text(
                verification.get("artifact_hash"),
                "candidate_ledger_verification.artifact_hash",
            )
            != _digest_text(document.get("artifact_hash"), "artifact_hash")
            or verification.get("ledger_schema") != schema_version
            or _digest_text(
                verification.get("ledger_content_sha256"),
                "candidate_ledger_verification.ledger_content_sha256",
            )
            != content_sha256
            or int(verification.get("row_count") or 0) != row_count
            or _digest_text(
                verification.get("selection_policy_hash"),
                "candidate_ledger_verification.selection_policy_hash",
            )
            != _digest_text(
                dict(document.get("selection_policy") or {}).get(
                    "selection_policy_hash"
                ),
                "selection_policy_hash",
            )
        ):
            raise RuntimeError(
                "HORIZON_CANDIDATE_LEDGER_VERIFICATION_PROJECTION_MISMATCH"
            )
        ledger_registration_hash = _digest_text(
            verification.get("registration_evidence_hash"),
            "candidate_ledger_verification.registration_evidence_hash",
        )
    elif persisted is not None:
        if (
            str(persisted.get("candidate_ledger_schema_version") or "")
            != schema_version
            or _digest_text(
                persisted.get("candidate_ledger_content_sha256"),
                "candidate_ledger_content_sha256",
            )
            != content_sha256
            or int(persisted.get("candidate_ledger_row_count") or 0)
            != row_count
        ):
            raise RuntimeError(
                "HORIZON_CANDIDATE_LEDGER_DURABLE_PROJECTION_MISMATCH"
            )
        ledger_registration_hash = _digest_text(
            persisted.get("ledger_registration_evidence_hash"),
            "ledger_registration_evidence_hash",
        )
    else:
        raise RuntimeError("HORIZON_CANDIDATE_LEDGER_ROOT_REQUIRED")

    artifact_hash = _digest_text(document.get("artifact_hash"), "artifact_hash")
    final_hash = _hash({
        "protocol": PROCESS_VERIFIED_LEDGER_REGISTRATION_PROTOCOL,
        "artifact_hash": artifact_hash,
        "ledger_registration_evidence_hash": ledger_registration_hash,
        "training_receipt_hash": _digest_text(
            training_receipt_hash, "training_receipt_hash"
        ),
    })
    if persisted is not None and _digest_text(
        persisted.get("registration_verification_hash"),
        "registration_verification_hash",
    ) != final_hash:
        raise RuntimeError(
            "HORIZON_CANDIDATE_LEDGER_REGISTRATION_VERIFICATION_MISMATCH"
        )
    projection["ledger_registration_evidence_hash"] = (
        ledger_registration_hash
    )
    projection["registration_verification_hash"] = final_hash
    return projection


def _artifact_registration_projection(
    artifact: Mapping[str, Any],
    *,
    registration_evidence_hash: str | None = None,
    training_receipt: Any | None = None,
    artifact_root: str | Path | None = None,
    persisted_ledger_verification: Mapping[str, Any] | None = None,
    verify_current_trainer: bool = True,
    require_current_code: bool = True,
    require_current_config: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Recompute every registry projection from a self-verifying artifact."""

    from .horizon_models import verify_horizon_artifact

    document = verify_horizon_artifact(
        artifact,
        require_current_code=require_current_code,
        require_current_config=require_current_config,
    )
    evidence = dict(document.get("oos_evidence") or {})
    gate = dict(document.get("gate") or {})
    selection_policy = dict(document.get("selection_policy") or {})
    if (
        document.get("schema_version") != CURRENT_HORIZON_ARTIFACT_SCHEMA
        or document.get("model_protocol") != CURRENT_HORIZON_MODEL_PROTOCOL
        or selection_policy.get("selection_policy_hash")
        != CURRENT_HORIZON_SELECTION_POLICY_HASH
    ):
        raise RuntimeError("HORIZON_ARTIFACT_PROTOCOL_NOT_CURRENT")
    folds = list(dict(document.get("walk_forward") or {}).get("folds") or ())
    status = str(gate.get("status") or "")
    if status not in {"PASS", "BLOCK"}:
        raise RuntimeError("HORIZON_ARTIFACT_GATE_STATUS_INVALID")
    raw_block_reasons = list(gate.get("block_reasons") or ())
    if (status == "PASS") != (not raw_block_reasons):
        raise RuntimeError("HORIZON_ARTIFACT_GATE_REASON_PROJECTION_INVALID")
    receipt_candidate: Mapping[str, Any] | None
    if verify_current_trainer and training_receipt is not None:
        try:
            from .continuous_calibration import (
                _process_bound_training_receipt_payload,
            )

            receipt_candidate = _process_bound_training_receipt_payload(
                training_receipt,
                horizon_days=int(document["horizon_days"]),
                artifact_hash=str(document["artifact_hash"]),
                consume=False,
            )
        except Exception:
            # A hand-built/self-asserted mapping is evidence of no controlled
            # process capability.  Persist it as UNVERIFIED/BLOCKED rather
            # than allowing its self-hash to upgrade authority.
            receipt_candidate = None
    elif isinstance(training_receipt, Mapping):
        receipt_candidate = training_receipt
    else:
        receipt_candidate = None
    receipt = _training_receipt_projection(
        document,
        receipt_candidate,
        verify_current_trainer=verify_current_trainer,
    )
    receipt_status = str(receipt["status"])
    if receipt_status == "PROCESS_VERIFIED" and verify_current_trainer:
        document = verify_horizon_artifact(
            document,
            require_current_code=False,
            require_current_config=True,
        )
    artifact_status = (
        "OOS_VERIFIED"
        if status == "PASS" and receipt_status == "PROCESS_VERIFIED"
        else "BLOCKED"
    )
    block_reasons = list(raw_block_reasons)
    if receipt_status != "PROCESS_VERIFIED":
        block_reasons.append(TRAINING_RECEIPT_UNVERIFIED)
    training_sessions = int(evidence.get("distinct_train_sessions") or 0)
    if training_sessions <= 0:
        raise RuntimeError("HORIZON_ARTIFACT_HAS_NO_TRAINING_SESSION_TRUTH")
    fold_count = int(evidence.get("walk_forward_fold_count") or 0)
    if fold_count != len(folds):
        raise RuntimeError("HORIZON_ARTIFACT_FOLD_COUNT_PROJECTION_INVALID")
    if folds:
        first_fold = dict(folds[0])
        last_fold = dict(folds[-1])
        final_model = document.get("final_model")
        if not isinstance(final_model, Mapping):
            raise RuntimeError(
                "HORIZON_ARTIFACT_FINAL_MODEL_CLOCK_UNAVAILABLE"
            )
        training_start = _date_value(final_model["training_start"])
        training_end = _date_value(final_model["training_end"])
        validation_start = _date_value(first_fold["validation_start"])
        validation_end = _date_value(last_fold["validation_end"])
    else:
        training_start = None
        training_end = None
        validation_start = None
        validation_end = None
    metric_names = (
        "direction_rank_correlation",
        "calibration_mae",
        "brier_score",
        "population_stability_index",
        "net_expectancy_after_cost_pct",
        "profit_factor",
        "cost_coverage_ratio",
    )
    metrics: dict[str, float] = {}
    for name in metric_names:
        try:
            metric = float(evidence[name])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"HORIZON_ARTIFACT_METRIC_INVALID:{name}"
            ) from exc
        if not math.isfinite(metric) or abs(metric) >= 10_000_000_000:
            raise RuntimeError(f"HORIZON_ARTIFACT_METRIC_INVALID:{name}")
        metrics[name] = metric
    valid_on = _date_value(document["valid_until"])
    valid_until = datetime.combine(
        valid_on,
        time(23, 59, 59, 999999),
        tzinfo=timezone.utc,
    )
    artifact_created_at = _datetime_utc(document["created_at"])
    if artifact_status == "OOS_VERIFIED" and valid_until <= artifact_created_at:
        raise RuntimeError(
            "HORIZON_ARTIFACT_OOS_EVIDENCE_EXPIRED_AT_CREATION"
        )
    registration_hash = (
        _digest_text(registration_evidence_hash, "registration_evidence_hash")
        if registration_evidence_hash is not None
        else None
    )
    if receipt_status == "PROCESS_VERIFIED" and registration_hash is None:
        raise RuntimeError(
            "HORIZON_PROCESS_REGISTRATION_EVIDENCE_REQUIRED"
        )
    artifact_hash = _digest_text(document.get("artifact_hash"), "artifact_hash")
    receipt_hash = _digest_text(
        receipt.get("receipt_hash"), "training_receipt_hash"
    )
    ledger_projection = _candidate_ledger_registration_projection(
        document,
        receipt_status=receipt_status,
        training_receipt_hash=receipt_hash,
        artifact_root=artifact_root,
        persisted=persisted_ledger_verification,
        require_current_code=require_current_code,
        require_current_config=(
            require_current_config
            or (receipt_status == "PROCESS_VERIFIED" and verify_current_trainer)
        ),
    )
    projection = {
        "artifact_id": artifact_hash,
        "release_id": str(document["release_id"]),
        "suite_release_id": str(document["suite_release_id"]),
        "model_key": str(document["model_key"]),
        "model_version": str(document["model_version"]),
        "horizon_days": int(document["horizon_days"]),
        "prediction_kind": str(document["prediction_kind"]),
        "artifact_status": artifact_status,
        "artifact_schema_version": str(document["schema_version"]),
        "model_protocol": str(document["model_protocol"]),
        "selection_policy_hash": _digest_text(
            selection_policy.get("selection_policy_hash"),
            "selection_policy_hash",
        ),
        **ledger_projection,
        "training_start": training_start,
        "training_end": training_end,
        "validation_start": validation_start,
        "validation_end": validation_end,
        "training_session_count": training_sessions,
        "oos_session_count": int(evidence.get("distinct_oos_sessions") or 0),
        "matured_sample_count": int(evidence.get("matured_sample_count") or 0),
        "oos_sample_count": int(evidence.get("oos_sample_count") or 0),
        "walk_forward_fold_count": fold_count,
        **metrics,
        "dataset_hash": _digest_text(document.get("dataset_hash"), "dataset_hash"),
        "feature_protocol_hash": _digest_text(
            document.get("feature_protocol_hash"), "feature_protocol_hash"
        ),
        "model_artifact_hash": artifact_hash,
        "calibration_evidence_hash": _digest_text(
            document.get("oos_evidence_hash"), "oos_evidence_hash"
        ),
        "registration_evidence_hash": registration_hash,
        "training_receipt_status": receipt_status,
        "training_receipt_hash": receipt_hash,
        "training_receipt_json": _json(receipt),
        "config_hash": _digest_text(document.get("config_hash"), "config_hash"),
        "code_version": str(document.get("code_version") or "").strip(),
        "artifact_json": _json(document),
        "metrics_json": _json(evidence),
        "block_reasons_json": _json(block_reasons),
        "evidence_valid_until": _utc_naive(valid_until),
        "order_authority": 0,
        "created_at": _utc_naive(artifact_created_at),
    }
    if not projection["code_version"]:
        raise RuntimeError("HORIZON_ARTIFACT_CODE_VERSION_INVALID")
    return document, projection


def _historical_artifact_audit_document(
    row: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Reverify a V1/V2 envelope without granting runtime authority."""

    document = json.loads(_json(dict(artifact)))
    schema_version = str(document.get("schema_version") or "")
    if schema_version not in {
        HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
        HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
    }:
        raise RuntimeError("HORIZON_HISTORICAL_ARTIFACT_SCHEMA_INVALID")
    artifact_hash = _digest_text(document.get("artifact_hash"), "artifact_hash")
    core = dict(document)
    for field in ("artifact_hash", "created_at", "creation_envelope_hash"):
        core.pop(field, None)
    if _hash(core) != artifact_hash:
        raise RuntimeError("HORIZON_HISTORICAL_ARTIFACT_HASH_INVALID")
    suite_release_id = str(document.get("suite_release_id") or "").strip()
    model_key = str(document.get("model_key") or "").strip()
    model_version = str(document.get("model_version") or "").strip()
    try:
        horizon = int(document.get("horizon_days") or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("HORIZON_HISTORICAL_ARTIFACT_IDENTITY_INVALID") from exc
    expected_release_id = (
        f"{suite_release_id}:{model_key}:{model_version}:T+{horizon}"
    )
    gate = document.get("gate")
    projected_schema = str(row.get("artifact_schema_version") or schema_version)
    is_v1 = schema_version == HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1
    projected_model_protocol = row.get("model_protocol")
    projected_selection_hash = row.get("selection_policy_hash")
    document_selection_hash = str(
        dict(document.get("selection_policy") or {}).get(
            "selection_policy_hash"
        )
        or ""
    )
    if (
        not suite_release_id
        or not model_key
        or not model_version
        or horizon not in {1, 5, 20}
        or str(document.get("release_id") or "") != expected_release_id
        or document.get("order_authority") is not False
        or not isinstance(gate, Mapping)
        or gate.get("order_authority") is not False
        or projected_schema != schema_version
        or (
            is_v1
            and (
                projected_model_protocol is not None
                or projected_selection_hash is not None
            )
        )
        or (
            not is_v1
            and (
                str(document.get("model_protocol") or "")
                != CURRENT_HORIZON_MODEL_PROTOCOL
                or str(projected_model_protocol or "")
                != CURRENT_HORIZON_MODEL_PROTOCOL
                or document_selection_hash
                != CURRENT_HORIZON_SELECTION_POLICY_HASH
                or str(projected_selection_hash or "")
                != CURRENT_HORIZON_SELECTION_POLICY_HASH
            )
        )
        or any(
            row.get(field) is not None
            for field in (
                "candidate_ledger_schema_version",
                "candidate_ledger_content_sha256",
                "candidate_ledger_row_count",
                "ledger_registration_evidence_hash",
                "registration_verification_hash",
            )
        )
        or str(row.get("artifact_id") or "") != artifact_hash
        or str(row.get("model_artifact_hash") or "") != artifact_hash
        or str(row.get("release_id") or "") != expected_release_id
        or str(row.get("suite_release_id") or "") != suite_release_id
        or str(row.get("model_key") or "") != model_key
        or str(row.get("model_version") or "") != model_version
        or int(row.get("horizon_days") or 0) != horizon
        or bool(row.get("order_authority"))
    ):
        raise RuntimeError("HORIZON_HISTORICAL_ARTIFACT_PROJECTION_INVALID")
    creation_hash = document.get("creation_envelope_hash")
    if creation_hash is not None and _hash({
        "artifact_hash": artifact_hash,
        "created_at": document.get("created_at"),
    }) != _digest_text(creation_hash, "creation_envelope_hash"):
        raise RuntimeError("HORIZON_HISTORICAL_ARTIFACT_ENVELOPE_INVALID")
    return document


def _artifact_row_document(
    row: Mapping[str, Any],
    *,
    require_current_code: bool,
    require_current_config: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_artifact = row.get("artifact_json")
    try:
        artifact = (
            dict(raw_artifact)
            if isinstance(raw_artifact, Mapping)
            else json.loads(str(raw_artifact or ""))
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("HORIZON_ARTIFACT_REGISTRY_JSON_INVALID") from exc
    if not isinstance(artifact, Mapping):
        raise RuntimeError("HORIZON_ARTIFACT_REGISTRY_JSON_INVALID")
    if artifact.get("schema_version") in {
        HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
        HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
    }:
        if require_current_code or require_current_config:
            raise RuntimeError("HORIZON_HISTORICAL_ARTIFACT_AUDIT_ONLY")
        return _historical_artifact_audit_document(row, artifact), {}
    raw_receipt = row.get("training_receipt_json")
    try:
        training_receipt = (
            dict(raw_receipt)
            if isinstance(raw_receipt, Mapping)
            else json.loads(str(raw_receipt or ""))
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "HORIZON_TRAINING_RECEIPT_REGISTRY_JSON_INVALID"
        ) from exc
    if not isinstance(training_receipt, Mapping):
        raise RuntimeError("HORIZON_TRAINING_RECEIPT_REGISTRY_JSON_INVALID")
    document, expected = _artifact_registration_projection(
        artifact,
        registration_evidence_hash=(
            str(row["registration_evidence_hash"])
            if row.get("registration_evidence_hash") is not None
            else None
        ),
        training_receipt=training_receipt,
        persisted_ledger_verification={
            field: row.get(field)
            for field in (
                "candidate_ledger_schema_version",
                "candidate_ledger_content_sha256",
                "candidate_ledger_row_count",
                "ledger_registration_evidence_hash",
                "registration_verification_hash",
            )
        },
        verify_current_trainer=False,
        require_current_code=require_current_code,
        require_current_config=require_current_config,
    )
    date_fields = {
        "training_start", "training_end", "validation_start", "validation_end"
    }
    datetime_fields = {"evidence_valid_until", "created_at"}
    json_fields = {
        "artifact_json",
        "metrics_json",
        "block_reasons_json",
        "training_receipt_json",
    }
    decimal_fields = {
        "direction_rank_correlation", "calibration_mae", "brier_score",
        "population_stability_index", "net_expectancy_after_cost_pct",
        "profit_factor", "cost_coverage_ratio",
    }
    for field, expected_value in expected.items():
        actual = row.get(field)
        if field in date_fields:
            matches = (
                actual is None and expected_value is None
            ) or (
                actual is not None
                and expected_value is not None
                and _date_value(actual) == expected_value
            )
        elif field in datetime_fields:
            matches = (
                actual is not None
                and _datetime_utc(actual).replace(tzinfo=None)
                == expected_value
            )
        elif field in json_fields:
            try:
                parsed = (
                    actual
                    if isinstance(actual, (Mapping, list))
                    else json.loads(str(actual))
                )
                matches = _json(parsed) == expected_value
            except (TypeError, ValueError, json.JSONDecodeError):
                matches = False
        elif field in decimal_fields:
            matches = _decimal_projection_matches(expected_value, actual)
        elif expected_value is None:
            matches = actual is None
        elif isinstance(expected_value, int):
            try:
                matches = int(actual) == expected_value
            except (TypeError, ValueError):
                matches = False
        else:
            matches = str(actual) == str(expected_value)
        if not matches:
            raise RuntimeError(
                f"HORIZON_ARTIFACT_REGISTRY_PROJECTION_MISMATCH:{field}"
            )
    return document, expected


def _runtime_provenance() -> dict[str, Any]:
    config = load_v3_config()
    models = dict(
        dict(config.get("multi_horizon_forecasts") or {}).get("models") or {}
    )
    artifacts: dict[str, str] = {}
    scorer_source_hash = hashlib.sha256(
        Path(__file__).with_name(
            "shadow_intelligence_worker.py"
        ).read_bytes()
    ).hexdigest()
    for label, raw in models.items():
        payload = dict(raw or {})
        horizon = int(str(label).replace("T+", ""))
        protocol_hash = _hash(dict(payload.get("feature_protocol") or {}))
        artifact = {
            "model_key": payload.get("model_key"),
            "model_version": payload.get("model_version"),
            "horizon_days": horizon,
            "prediction_kind": payload.get("prediction_kind"),
            "source_strategy_keys": list(
                payload.get("source_strategy_keys") or ()
            ),
            "feature_protocol_hash": protocol_hash,
            "cost_assumption_pct": float(payload.get("cost_assumption_pct")),
            "cost_model_version": payload.get("cost_model_version"),
            "scorer_algorithm_version": dict(
                config.get("multi_horizon_forecasts") or {}
            ).get("scorer_algorithm_version"),
            "scorer_source_hash": scorer_source_hash,
        }
        release_id = ShadowIntelligenceRepository.release_id(
            model_key=str(payload.get("model_key") or ""),
            model_version=str(payload.get("model_version") or ""),
            horizon_days=horizon,
        )
        artifacts[release_id] = _hash(artifact)
    if set(
        int(key.rsplit("T+", 1)[1]) for key in artifacts
    ) != {1, 5, 20}:
        raise RuntimeError("CURRENT_HORIZON_ARTIFACTS_INCOMPLETE")
    version, version_kind = code_version()
    return {
        "config_hash": current_config_hash(),
        "continuous_policy": dict(config.get("continuous_calibration") or {}),
        "model_artifact_hashes": dict(sorted(artifacts.items())),
        "code_version": version,
        "code_version_kind": version_kind,
    }


def _validate_outcome_evidence(
    outcome: HorizonOutcomeEvidence,
    evidence: Mapping[str, Any],
) -> None:
    if (
        str(evidence.get("contract_id") or "") != outcome.contract_id
        or str(evidence.get("contract_hash") or "") != outcome.contract_hash
        or str(evidence.get("stock_code") or "") != outcome.stock_code
        or int(evidence.get("horizon_days") or 0) != outcome.horizon_days
        or str(evidence.get("entry_trade_date") or "")
        != outcome.entry_trade_date.isoformat()
        or str(evidence.get("exit_trade_date") or "")
        != outcome.exit_trade_date.isoformat()
        or str(evidence.get("market_data_source") or "")
        != outcome.market_data_source
        or str(evidence.get("cost_model_version") or "")
        != outcome.cost_model_version
        or str(evidence.get("execution_feasibility") or "")
        != outcome.execution_feasibility
        or abs(
            float(evidence.get("realized_cost_pct"))
            - outcome.realized_cost_pct
        ) > 1e-9
    ):
        raise RuntimeError("SHADOW_HORIZON_MARKET_EVIDENCE_IDENTITY_MISMATCH")
    if outcome.execution_feasibility != "UNVERIFIED_RESEARCH":
        raise RuntimeError(
            "EXECUTABLE_OUTCOME_REQUIRES_SEPARATE_ATTESTED_FILL_PIPELINE"
        )
    qmt_attestation = evidence.get("qmt_attestation")
    if not isinstance(qmt_attestation, Mapping):
        raise RuntimeError("SHADOW_HORIZON_QMT_ATTESTATION_MISSING")
    if (
        str(qmt_attestation.get("protocol") or "")
        != QMT_OUTCOME_ATTESTATION_PROTOCOL
        or str(qmt_attestation.get("status") or "") != "QMT_ATTESTED"
        or str(qmt_attestation.get("provider") or "")
        != "gj_big_qmt_inner"
        or int(qmt_attestation.get("attested_bar_count") or 0)
        != outcome.horizon_days + 1
        or not str(qmt_attestation.get("attestation_hash") or "")
    ):
        raise RuntimeError("SHADOW_HORIZON_QMT_ATTESTATION_INVALID")
    bars = evidence.get("bars")
    if not isinstance(bars, list) or len(bars) != outcome.horizon_days + 1:
        raise RuntimeError("SHADOW_HORIZON_MARKET_EVIDENCE_BAR_COUNT_INVALID")
    normalized: list[dict[str, Any]] = []
    try:
        for raw in bars:
            if not isinstance(raw, Mapping):
                raise TypeError("bar is not a mapping")
            bar = {
                "trade_date": date.fromisoformat(str(raw["trade_date"])),
                "open": float(raw["open"]),
                "high": float(raw["high"]),
                "low": float(raw["low"]),
                "close": float(raw["close"]),
                "pre_close": (
                    float(raw["pre_close"])
                    if raw.get("pre_close") is not None
                    else None
                ),
                "amount": (
                    float(raw["amount"])
                    if raw.get("amount") is not None
                    else None
                ),
                "etl_sync_at": _datetime_utc(raw["etl_sync_at"]),
                "data_source": str(raw.get("data_source") or ""),
                "quality_status": str(raw.get("quality_status") or ""),
                "source_time": str(raw.get("source_time") or ""),
                "received_at": str(raw.get("received_at") or ""),
                "batch_id": str(raw.get("batch_id") or ""),
                "data_version": str(raw.get("data_version") or ""),
            }
            numeric = [
                bar["open"], bar["high"], bar["low"], bar["close"]
            ] + [
                value for value in (bar["pre_close"], bar["amount"])
                if value is not None
            ]
            if (
                not all(math.isfinite(value) for value in numeric)
                or min(bar["open"], bar["high"], bar["low"], bar["close"])
                <= 0
                or bar["high"] < max(bar["open"], bar["close"])
                or bar["low"] > min(bar["open"], bar["close"])
                or bar["data_source"] != "gj_big_qmt_inner"
                or bar["quality_status"] != "QMT_ATTESTED"
                or not bar["source_time"]
                or not bar["received_at"]
                or not bar["batch_id"]
                or not bar["data_version"]
            ):
                raise ValueError("invalid OHLC")
            normalized.append(bar)
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(
            "SHADOW_HORIZON_MARKET_EVIDENCE_BAR_INVALID"
        ) from exc
    dates = [item["trade_date"] for item in normalized]
    if (
        dates != sorted(set(dates))
        or dates[0] != outcome.entry_trade_date
        or dates[-1] != outcome.exit_trade_date
    ):
        raise RuntimeError("SHADOW_HORIZON_MARKET_EVIDENCE_CLOCK_INVALID")
    observed = outcome.observed_at.astimezone(timezone.utc)
    if any(item["etl_sync_at"] > observed for item in normalized):
        raise RuntimeError("SHADOW_HORIZON_MARKET_EVIDENCE_NOT_YET_KNOWN")
    qmt_projection = [
        {
            "trade_date": item["trade_date"].isoformat(),
            "data_source": item["data_source"],
            "quality_status": item["quality_status"],
            "source_time": item["source_time"],
            "received_at": item["received_at"],
            "batch_id": item["batch_id"],
            "data_version": item["data_version"],
        }
        for item in normalized
    ]
    if _hash(qmt_projection) != str(
        qmt_attestation.get("attestation_hash") or ""
    ):
        raise RuntimeError("SHADOW_HORIZON_QMT_ATTESTATION_HASH_MISMATCH")
    try:
        if _datetime_utc(evidence["knowledge_cutoff"]) != observed:
            raise RuntimeError(
                "SHADOW_HORIZON_MARKET_EVIDENCE_CUTOFF_MISMATCH"
            )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "SHADOW_HORIZON_MARKET_EVIDENCE_CUTOFF_INVALID"
        ) from exc
    entry_price = float(normalized[0]["open"])
    exit_price = float(normalized[-1]["close"])
    gross = (exit_price / entry_price - 1.0) * 100.0
    mae = min(
        (float(item["low"]) / entry_price - 1.0) * 100.0
        for item in normalized
    )
    mfe = max(
        (float(item["high"]) / entry_price - 1.0) * 100.0
        for item in normalized
    )
    expected_values = (
        (entry_price, outcome.entry_price),
        (exit_price, outcome.exit_price),
        (gross, outcome.gross_return_pct),
        (gross - outcome.realized_cost_pct, outcome.realized_net_return_pct),
        (mae, outcome.realized_mae_pct),
        (mfe, outcome.realized_mfe_pct),
    )
    if any(abs(left - right) > 1e-6 for left, right in expected_values):
        raise RuntimeError("SHADOW_HORIZON_OUTCOME_NOT_DERIVED_FROM_BARS")
    corporate_action = any(
        item["pre_close"] is None
        or item["pre_close"] <= 0
        or abs(
            float(item["pre_close"])
            / float(normalized[index - 1]["close"])
            - 1.0
        ) * 100.0 > 0.05
        for index, item in enumerate(normalized[1:], start=1)
    )
    guard = dict(evidence.get("corporate_action_guard") or {})
    if (
        guard.get("method") != "PRE_CLOSE_VS_PRIOR_UNADJUSTED_CLOSE"
        or float(guard.get("tolerance_pct") or -1) != 0.05
        or bool(guard.get("detected")) != corporate_action
        or outcome.outcome_status
        != ("QUARANTINED" if corporate_action else "MATURED_VERIFIED")
    ):
        raise RuntimeError("SHADOW_HORIZON_CORPORATE_ACTION_GUARD_MISMATCH")


class ShadowIntelligenceRepository:
    """Append-only persistence for research-only decision intelligence."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @property
    def _insert_ignore(self) -> str:
        if str(self.engine.dialect.name).casefold() == "sqlite":
            return "INSERT OR IGNORE"
        return "INSERT IGNORE"

    def save_horizon_model_artifact(
        self,
        artifact: Mapping[str, Any],
        *,
        registration_evidence_hash: str | None = None,
        training_receipt: Any | None = None,
        artifact_root: str | Path | None = None,
    ) -> dict[str, Any]:
        """Append an artifact only after recomputing all integrity and gate claims."""

        # An immutable registry row is the durable process-provenance truth.
        # Replays after a scheduler restart must therefore verify and reuse an
        # existing row before asking for the one-shot in-process capability
        # that is available only during the original trainer subprocess.
        from .horizon_models import verify_horizon_artifact

        incoming_document = verify_horizon_artifact(
            artifact,
            require_current_code=False,
            require_current_config=False,
        )
        incoming_artifact_id = _digest_text(
            incoming_document.get("artifact_hash"), "artifact_hash"
        )
        incoming_valid_until = datetime.combine(
            _date_value(incoming_document["valid_until"]),
            time(23, 59, 59, 999999),
            tzinfo=timezone.utc,
        )
        if (
            str(dict(incoming_document.get("gate") or {}).get("status") or "")
            == "PASS"
            and incoming_valid_until
            <= _datetime_utc(incoming_document["created_at"])
        ):
            raise RuntimeError(
                "HORIZON_ARTIFACT_OOS_EVIDENCE_EXPIRED_AT_CREATION"
            )
        expected_registration_hash = (
            _digest_text(
                registration_evidence_hash,
                "registration_evidence_hash",
            )
            if registration_evidence_hash is not None
            else None
        )

        def _existing_result(
            raw: Mapping[str, Any],
        ) -> dict[str, Any]:
            observed_row = dict(raw)
            stored_document, _ = _artifact_row_document(
                observed_row,
                require_current_code=False,
                require_current_config=False,
            )
            if (
                _json(stored_document) != _json(incoming_document)
                or observed_row.get("registration_evidence_hash")
                != expected_registration_hash
            ):
                raise RuntimeError(
                    "HORIZON_ARTIFACT_REGISTRY_IDEMPOTENCY_CONFLICT"
                )
            return {
                "status": "ok",
                "artifact_id": incoming_artifact_id,
                "release_id": str(observed_row["release_id"]),
                "suite_release_id": str(observed_row["suite_release_id"]),
                "artifact_status": str(observed_row["artifact_status"]),
                "training_receipt_status": str(
                    observed_row["training_receipt_status"]
                ),
                "training_receipt_hash": str(
                    observed_row["training_receipt_hash"]
                ),
                "candidate_ledger_schema_version": observed_row.get(
                    "candidate_ledger_schema_version"
                ),
                "candidate_ledger_content_sha256": observed_row.get(
                    "candidate_ledger_content_sha256"
                ),
                "candidate_ledger_row_count": observed_row.get(
                    "candidate_ledger_row_count"
                ),
                "ledger_registration_evidence_hash": observed_row.get(
                    "ledger_registration_evidence_hash"
                ),
                "registration_verification_hash": observed_row.get(
                    "registration_verification_hash"
                ),
                "inserted": False,
                "existing": True,
                "order_authority": False,
            }

        with self.engine.connect() as connection:
            existing = connection.execute(
                text(
                    """
                    SELECT * FROM st_horizon_model_artifact_v3
                    WHERE artifact_id = :artifact_id
                    """
                ),
                {"artifact_id": incoming_artifact_id},
            ).mappings().first()
        if existing is not None:
            return _existing_result(existing)

        # No durable row exists: only this path may turn a same-process trainer
        # capability into PROCESS_VERIFIED.  A plain/self-asserted mapping is
        # still persisted as UNVERIFIED/BLOCKED for audit.
        document, values = _artifact_registration_projection(
            incoming_document,
            registration_evidence_hash=registration_evidence_hash,
            training_receipt=training_receipt,
            artifact_root=artifact_root,
            verify_current_trainer=True,
            require_current_code=False,
            require_current_config=False,
        )
        inserted = False
        observed: dict[str, Any]
        with self.engine.begin() as connection:
            result = connection.execute(
                text(
                    f"""
                    {self._insert_ignore} INTO st_horizon_model_artifact_v3 (
                        artifact_id, release_id, suite_release_id,
                        model_key, model_version, horizon_days,
                        prediction_kind, artifact_status,
                        artifact_schema_version, model_protocol,
                        selection_policy_hash,
                        candidate_ledger_schema_version,
                        candidate_ledger_content_sha256,
                        candidate_ledger_row_count,
                        ledger_registration_evidence_hash,
                        registration_verification_hash,
                        training_start, training_end,
                        validation_start, validation_end,
                        training_session_count, oos_session_count,
                        matured_sample_count, oos_sample_count,
                        walk_forward_fold_count,
                        direction_rank_correlation, calibration_mae,
                        brier_score, population_stability_index,
                        net_expectancy_after_cost_pct, profit_factor,
                        cost_coverage_ratio, dataset_hash,
                        feature_protocol_hash, model_artifact_hash,
                        calibration_evidence_hash,
                        registration_evidence_hash,
                        training_receipt_status, training_receipt_hash,
                        training_receipt_json, config_hash,
                        code_version, artifact_json, metrics_json,
                        block_reasons_json, evidence_valid_until,
                        order_authority, created_at
                    ) VALUES (
                        :artifact_id, :release_id, :suite_release_id,
                        :model_key, :model_version, :horizon_days,
                        :prediction_kind, :artifact_status,
                        :artifact_schema_version, :model_protocol,
                        :selection_policy_hash,
                        :candidate_ledger_schema_version,
                        :candidate_ledger_content_sha256,
                        :candidate_ledger_row_count,
                        :ledger_registration_evidence_hash,
                        :registration_verification_hash,
                        :training_start, :training_end,
                        :validation_start, :validation_end,
                        :training_session_count, :oos_session_count,
                        :matured_sample_count, :oos_sample_count,
                        :walk_forward_fold_count,
                        :direction_rank_correlation, :calibration_mae,
                        :brier_score, :population_stability_index,
                        :net_expectancy_after_cost_pct, :profit_factor,
                        :cost_coverage_ratio, :dataset_hash,
                        :feature_protocol_hash, :model_artifact_hash,
                        :calibration_evidence_hash,
                        :registration_evidence_hash,
                        :training_receipt_status, :training_receipt_hash,
                        :training_receipt_json, :config_hash,
                        :code_version, :artifact_json, :metrics_json,
                        :block_reasons_json, :evidence_valid_until,
                        0, :created_at
                    )
                    """
                ),
                values,
            )
            observed = connection.execute(
                text(
                    """
                    SELECT * FROM st_horizon_model_artifact_v3
                    WHERE artifact_id = :artifact_id
                    """
                ),
                {"artifact_id": values["artifact_id"]},
            ).mappings().first()
            if observed is None:
                raise RuntimeError("HORIZON_ARTIFACT_REGISTRY_INSERT_MISSING")
            observed = dict(observed)
            inserted = int(getattr(result, "rowcount", 0) or 0) == 1
            if not inserted:
                # A concurrent writer won the INSERT IGNORE race.  Its
                # immutable, deeply reverified row is the only authority.
                return _existing_result(observed)
            stored_document, _ = _artifact_row_document(
                observed,
                require_current_code=False,
                require_current_config=False,
            )
            if (
                observed.get("registration_evidence_hash")
                != values["registration_evidence_hash"]
                or observed.get("registration_verification_hash")
                != values["registration_verification_hash"]
                or _json(stored_document) != _json(document)
                or str(observed.get("training_receipt_hash") or "")
                != values["training_receipt_hash"]
            ):
                raise RuntimeError(
                    "HORIZON_ARTIFACT_REGISTRY_IDEMPOTENCY_CONFLICT"
                )

        # Consume only after the database transaction has committed.  A failed
        # commit leaves the capability reusable for a truthful retry; a crash
        # after commit is harmless because the immutable DB row is authoritative.
        if inserted and values["training_receipt_status"] == "PROCESS_VERIFIED":
            from .continuous_calibration import (
                _process_bound_training_receipt_payload,
            )

            _process_bound_training_receipt_payload(
                training_receipt,
                horizon_days=int(document["horizon_days"]),
                artifact_hash=str(document["artifact_hash"]),
                consume=True,
            )
        return {
            "status": "ok",
            "artifact_id": values["artifact_id"],
            "release_id": values["release_id"],
            "suite_release_id": values["suite_release_id"],
            "artifact_status": str(observed["artifact_status"]),
            "training_receipt_status": str(
                observed["training_receipt_status"]
            ),
            "training_receipt_hash": str(observed["training_receipt_hash"]),
            "candidate_ledger_schema_version": observed.get(
                "candidate_ledger_schema_version"
            ),
            "candidate_ledger_content_sha256": observed.get(
                "candidate_ledger_content_sha256"
            ),
            "candidate_ledger_row_count": observed.get(
                "candidate_ledger_row_count"
            ),
            "ledger_registration_evidence_hash": observed.get(
                "ledger_registration_evidence_hash"
            ),
            "registration_verification_hash": observed.get(
                "registration_verification_hash"
            ),
            "inserted": inserted,
            "existing": not inserted,
            "order_authority": False,
        }

    def horizon_model_artifacts(
        self,
        *,
        model_key: str | None = None,
        horizon_days: int | None = None,
        artifact_status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: dict[str, Any] = {
            "limit": max(1, min(int(limit), 10_000))
        }
        if model_key is not None:
            clauses.append("model_key = :model_key")
            parameters["model_key"] = str(model_key)
        if horizon_days is not None:
            horizon = int(horizon_days)
            if horizon not in {1, 5, 20}:
                raise ValueError("horizon_days must be one of 1, 5 or 20")
            clauses.append("horizon_days = :horizon_days")
            parameters["horizon_days"] = horizon
        if artifact_status is not None:
            status = str(artifact_status).strip().upper()
            if status not in {"BLOCKED", "OOS_VERIFIED"}:
                raise ValueError(
                    "artifact_status must be BLOCKED or OOS_VERIFIED"
                )
            clauses.append("artifact_status = :artifact_status")
            parameters["artifact_status"] = status
        where = " AND ".join(clauses) if clauses else "1 = 1"
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT * FROM st_horizon_model_artifact_v3
                    WHERE {where}
                    ORDER BY created_at DESC, artifact_id DESC
                    LIMIT :limit
                    """
                ),
                parameters,
            ).mappings().all()
        result: list[dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            document, _ = _artifact_row_document(
                row,
                require_current_code=False,
                require_current_config=False,
            )
            raw_block_reasons = row["block_reasons_json"]
            block_reasons = (
                list(raw_block_reasons)
                if isinstance(raw_block_reasons, list)
                else list(json.loads(str(raw_block_reasons)))
            )
            raw_receipt = row["training_receipt_json"]
            receipt = (
                dict(raw_receipt)
                if isinstance(raw_receipt, Mapping)
                else dict(json.loads(str(raw_receipt)))
            )
            schema_version = str(
                row.get("artifact_schema_version")
                or document.get("schema_version")
                or ""
            )
            is_current_protocol = (
                schema_version == CURRENT_HORIZON_ARTIFACT_SCHEMA
                and str(row.get("model_protocol") or "")
                == CURRENT_HORIZON_MODEL_PROTOCOL
                and str(row.get("selection_policy_hash") or "")
                == CURRENT_HORIZON_SELECTION_POLICY_HASH
            )
            ledger_verified = (
                str(row.get("candidate_ledger_schema_version") or "")
                == CANDIDATE_EVALUATION_LEDGER_SCHEMA
                and len(str(row.get("candidate_ledger_content_sha256") or ""))
                == 64
                and int(row.get("candidate_ledger_row_count") or 0) > 0
                and len(
                    str(row.get("ledger_registration_evidence_hash") or "")
                )
                == 64
                and len(str(row.get("registration_verification_hash") or ""))
                == 64
            )
            runtime_eligible = (
                is_current_protocol
                and ledger_verified
                and str(row.get("artifact_status") or "")
                == "OOS_VERIFIED"
                and str(row.get("training_receipt_status") or "")
                == "PROCESS_VERIFIED"
                and not bool(row.get("order_authority"))
            )
            result.append({
                **row,
                "artifact": document,
                "metrics": dict(document["oos_evidence"]),
                "block_reasons": block_reasons,
                "training_receipt": receipt,
                "protocol_status": (
                    "HISTORICAL_AUDIT_ONLY"
                    if schema_version == HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1
                    else (
                        "PRE_LEDGER_V2_AUDIT_ONLY"
                        if schema_version
                        == HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2
                        else (
                            "CURRENT_V3_LEDGER_VERIFIED"
                            if is_current_protocol and ledger_verified
                            else "CURRENT_V3_LEDGER_UNVERIFIED"
                        )
                    )
                ),
                "runtime_eligible": runtime_eligible,
                "order_authority": False,
            })
        return result

    def latest_verified_horizon_artifact(
        self,
        *,
        model_key: str,
        horizon_days: int,
        model_version: str | None = None,
        decision_as_of: datetime | None = None,
    ) -> dict[str, Any] | None:
        horizon = int(horizon_days)
        if horizon not in {1, 5, 20}:
            raise ValueError("horizon_days must be one of 1, 5 or 20")
        if decision_as_of is not None and (
            decision_as_of.tzinfo is None
            or decision_as_of.utcoffset() is None
        ):
            raise ValueError("decision_as_of must include a timezone")
        evaluated_at = _datetime_utc(
            decision_as_of or datetime.now(timezone.utc)
        )
        parameters: dict[str, Any] = {
            "model_key": str(model_key),
            "horizon_days": horizon,
            "decision_as_of": _utc_naive(evaluated_at),
            "config_hash": current_config_hash(),
            "code_version": code_version()[0],
            "artifact_schema_version": CURRENT_HORIZON_ARTIFACT_SCHEMA,
            "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
            "selection_policy_hash": CURRENT_HORIZON_SELECTION_POLICY_HASH,
            "candidate_ledger_schema_version": (
                CANDIDATE_EVALUATION_LEDGER_SCHEMA
            ),
        }
        version_clause = ""
        if model_version is not None:
            version_clause = "AND model_version = :model_version"
            parameters["model_version"] = str(model_version)
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT * FROM st_horizon_model_artifact_v3
                    WHERE model_key = :model_key
                      AND horizon_days = :horizon_days
                      AND artifact_status = 'OOS_VERIFIED'
                      AND artifact_schema_version = :artifact_schema_version
                      AND model_protocol = :model_protocol
                      AND selection_policy_hash = :selection_policy_hash
                      AND candidate_ledger_schema_version
                          = :candidate_ledger_schema_version
                      AND candidate_ledger_content_sha256 IS NOT NULL
                      AND candidate_ledger_row_count > 0
                      AND ledger_registration_evidence_hash IS NOT NULL
                      AND registration_verification_hash IS NOT NULL
                      AND registration_evidence_hash IS NOT NULL
                      AND training_receipt_status = 'PROCESS_VERIFIED'
                      AND config_hash = :config_hash
                      AND code_version = :code_version
                      AND created_at <= :decision_as_of
                      AND evidence_valid_until >= :decision_as_of
                      {version_clause}
                    ORDER BY created_at DESC, artifact_id DESC
                    LIMIT 100
                    """
                ),
                parameters,
            ).mappings().all()
        for row in rows:
            result = dict(row)
            try:
                document, _ = _artifact_row_document(
                    result,
                    require_current_code=True,
                    require_current_config=True,
                )
            except (KeyError, TypeError, ValueError, RuntimeError):
                continue
            return {
                **result,
                "artifact": document,
                "metrics": dict(document["oos_evidence"]),
                "block_reasons": [],
                "order_authority": False,
            }
        return None

    def latest_verified_horizon_suite(
        self,
        *,
        model_specs: Mapping[int, Mapping[str, Any]],
        decision_as_of: datetime,
    ) -> dict[str, Any] | None:
        """Return one complete current T1/T5/T20 suite or no real model.

        Selection is intentionally suite-atomic.  A partially registered new
        release may remain in the immutable audit ledger, but it cannot mix
        with older horizons (or proxy fallbacks) in one decision run.
        """

        if decision_as_of.tzinfo is None or decision_as_of.utcoffset() is None:
            raise ValueError("decision_as_of must include a timezone")
        normalized_specs = {
            int(horizon): dict(spec)
            for horizon, spec in dict(model_specs).items()
        }
        if set(normalized_specs) != {1, 5, 20}:
            raise ValueError("model_specs must contain exactly T1/T5/T20")
        evaluated_at = _datetime_utc(decision_as_of)
        parameters = {
            "decision_as_of": _utc_naive(evaluated_at),
            "config_hash": current_config_hash(),
            "code_version": code_version()[0],
            "artifact_schema_version": CURRENT_HORIZON_ARTIFACT_SCHEMA,
            "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
            "selection_policy_hash": CURRENT_HORIZON_SELECTION_POLICY_HASH,
            "candidate_ledger_schema_version": (
                CANDIDATE_EVALUATION_LEDGER_SCHEMA
            ),
        }
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT * FROM st_horizon_model_artifact_v3
                    WHERE artifact_status = 'OOS_VERIFIED'
                      AND artifact_schema_version = :artifact_schema_version
                      AND model_protocol = :model_protocol
                      AND selection_policy_hash = :selection_policy_hash
                      AND candidate_ledger_schema_version
                          = :candidate_ledger_schema_version
                      AND candidate_ledger_content_sha256 IS NOT NULL
                      AND candidate_ledger_row_count > 0
                      AND ledger_registration_evidence_hash IS NOT NULL
                      AND registration_verification_hash IS NOT NULL
                      AND registration_evidence_hash IS NOT NULL
                      AND training_receipt_status = 'PROCESS_VERIFIED'
                      AND config_hash = :config_hash
                      AND code_version = :code_version
                      AND created_at <= :decision_as_of
                      AND evidence_valid_until >= :decision_as_of
                    ORDER BY created_at DESC, artifact_id DESC
                    LIMIT 1000
                    """
                ),
                parameters,
            ).mappings().all()

        suites: dict[str, dict[int, dict[str, Any]]] = {}
        for raw in rows:
            row = dict(raw)
            try:
                document, _ = _artifact_row_document(
                    row,
                    require_current_code=True,
                    require_current_config=True,
                )
                horizon = int(document["horizon_days"])
                spec = normalized_specs[horizon]
                if (
                    str(document["model_key"])
                    != str(spec.get("model_key") or "")
                    or str(document["model_version"])
                    != str(spec.get("model_version") or "")
                ):
                    continue
                suite_release_id = str(
                    document.get("suite_release_id") or ""
                ).strip()
                if not suite_release_id:
                    continue
            except (KeyError, TypeError, ValueError, RuntimeError):
                continue
            candidates = suites.setdefault(suite_release_id, {})
            if horizon in candidates:
                # A suite/horizon identity is immutable and unique.  Treat an
                # unexpected duplicate as a corrupt suite, never as a tiebreak.
                suites[suite_release_id] = {}
                continue
            candidates[horizon] = {
                **row,
                "artifact": document,
                "metrics": dict(document["oos_evidence"]),
                "block_reasons": [],
                "order_authority": False,
            }

        complete = [
            (suite_id, members)
            for suite_id, members in suites.items()
            if set(members) == {1, 5, 20}
            and {
                str(item["artifact"]["suite_release_id"])
                for item in members.values()
            } == {suite_id}
        ]
        if not complete:
            return None
        suite_id, selected = max(
            complete,
            key=lambda item: (
                min(
                    _datetime_utc(member["created_at"])
                    for member in item[1].values()
                ),
                item[0],
            ),
        )
        release_ids = {
            str(member["release_id"]) for member in selected.values()
        }
        statement = text(
            """
            SELECT * FROM st_shadow_release_v3
            WHERE release_id IN :release_ids
            ORDER BY release_id, transition_sequence DESC
            """
        ).bindparams(bindparam("release_ids", expanding=True))
        with self.engine.connect() as connection:
            release_rows = connection.execute(
                statement,
                {"release_ids": tuple(sorted(release_ids))},
            ).mappings().all()
        latest_release_by_id: dict[str, dict[str, Any]] = {}
        for raw in release_rows:
            row = dict(raw)
            latest_release_by_id.setdefault(str(row["release_id"]), row)
        allowed_runtime_stages = {
            "SHADOW", "CALIBRATION_REVIEW", "PAPER_ELIGIBLE",
        }
        release_states_by_horizon: dict[int, dict[str, Any]] = {}
        for horizon, artifact_row in selected.items():
            release_id = str(artifact_row["release_id"])
            release = latest_release_by_id.get(release_id)
            document = dict(artifact_row["artifact"])
            if (
                release is None
                or str(release.get("current_stage") or "")
                not in allowed_runtime_stages
                or str(release.get("model_key") or "")
                != str(document["model_key"])
                or str(release.get("model_version") or "")
                != str(document["model_version"])
                or int(release.get("horizon_days") or 0) != horizon
                or str(release.get("config_hash") or "")
                != parameters["config_hash"]
                or bool(release.get("order_authority"))
            ):
                return None
            release_states_by_horizon[horizon] = release
        return {
            "suite_release_id": suite_id,
            "artifacts_by_horizon": {
                horizon: selected[horizon] for horizon in (1, 5, 20)
            },
            "release_states_by_horizon": release_states_by_horizon,
            "order_authority": False,
        }

    def latest_forward_shadow_research_suite(
        self,
        *,
        model_specs: Mapping[int, Mapping[str, Any]],
        decision_as_of: datetime,
    ) -> dict[str, Any] | None:
        """Return one deeply verified suite for research-only forward scoring.

        Unlike ``latest_verified_horizon_suite``, this lookup deliberately
        permits a process-verified artifact whose frozen OOS gate is BLOCK.
        It does not consult or create release state and never grants promotion
        authority.  This lets a failed historical candidate accumulate a new,
        artifact-bound forward cohort before any subsequent retraining request.
        """

        if decision_as_of.tzinfo is None or decision_as_of.utcoffset() is None:
            raise ValueError("decision_as_of must include a timezone")
        normalized_specs = {
            int(horizon): dict(spec)
            for horizon, spec in dict(model_specs).items()
        }
        if set(normalized_specs) != {1, 5, 20}:
            raise ValueError("model_specs must contain exactly T1/T5/T20")
        parameters = {
            "decision_as_of": _utc_naive(_datetime_utc(decision_as_of)),
            "config_hash": current_config_hash(),
            "code_version": code_version()[0],
            "artifact_schema_version": CURRENT_HORIZON_ARTIFACT_SCHEMA,
            "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
            "selection_policy_hash": CURRENT_HORIZON_SELECTION_POLICY_HASH,
            "candidate_ledger_schema_version": (
                CANDIDATE_EVALUATION_LEDGER_SCHEMA
            ),
        }
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT * FROM st_horizon_model_artifact_v3
                    WHERE artifact_status IN ('BLOCKED', 'OOS_VERIFIED')
                      AND artifact_schema_version = :artifact_schema_version
                      AND model_protocol = :model_protocol
                      AND selection_policy_hash = :selection_policy_hash
                      AND candidate_ledger_schema_version
                          = :candidate_ledger_schema_version
                      AND candidate_ledger_content_sha256 IS NOT NULL
                      AND candidate_ledger_row_count > 0
                      AND ledger_registration_evidence_hash IS NOT NULL
                      AND registration_verification_hash IS NOT NULL
                      AND registration_evidence_hash IS NOT NULL
                      AND training_receipt_status = 'PROCESS_VERIFIED'
                      AND config_hash = :config_hash
                      AND code_version = :code_version
                      AND created_at <= :decision_as_of
                      AND evidence_valid_until >= :decision_as_of
                    ORDER BY created_at DESC, artifact_id DESC
                    LIMIT 1000
                    """
                ),
                parameters,
            ).mappings().all()

        suites: dict[str, dict[int, dict[str, Any]]] = {}
        corrupt_suite_ids: set[str] = set()
        for raw in rows:
            row = dict(raw)
            try:
                document, _ = _artifact_row_document(
                    row,
                    require_current_code=True,
                    require_current_config=True,
                )
                horizon = int(document["horizon_days"])
                spec = normalized_specs[horizon]
                gate_status = str(
                    dict(document.get("gate") or {}).get("status") or ""
                )
                expected_registry_status = (
                    "OOS_VERIFIED" if gate_status == "PASS" else "BLOCKED"
                )
                suite_release_id = str(
                    document.get("suite_release_id") or ""
                ).strip()
                if (
                    gate_status not in {"PASS", "BLOCK"}
                    or str(row.get("artifact_status") or "")
                    != expected_registry_status
                    or str(document["model_key"])
                    != str(spec.get("model_key") or "")
                    or str(document["model_version"])
                    != str(spec.get("model_version") or "")
                    or not suite_release_id
                    or bool(row.get("order_authority"))
                ):
                    continue
            except (KeyError, TypeError, ValueError, RuntimeError):
                continue
            candidates = suites.setdefault(suite_release_id, {})
            if horizon in candidates:
                corrupt_suite_ids.add(suite_release_id)
                continue
            candidates[horizon] = {
                **row,
                "artifact": document,
                "metrics": dict(document["oos_evidence"]),
                "block_reasons": list(
                    dict(document.get("gate") or {}).get("block_reasons")
                    or ()
                ),
                "research_runtime_eligible": True,
                "runtime_eligible": False,
                "order_authority": False,
            }

        complete = [
            (suite_id, members)
            for suite_id, members in suites.items()
            if suite_id not in corrupt_suite_ids
            and set(members) == {1, 5, 20}
            and {
                str(item["artifact"]["suite_release_id"])
                for item in members.values()
            }
            == {suite_id}
        ]
        if not complete:
            return None
        suite_id, selected = max(
            complete,
            key=lambda item: (
                min(
                    _datetime_utc(member["created_at"])
                    for member in item[1].values()
                ),
                item[0],
            ),
        )
        return {
            "binding_protocol": FORWARD_SHADOW_BINDING_PROTOCOL,
            "suite_release_id": suite_id,
            "artifacts_by_horizon": {
                horizon: selected[horizon] for horizon in (1, 5, 20)
            },
            "gate_statuses_by_horizon": {
                horizon: str(
                    dict(selected[horizon]["artifact"].get("gate") or {}).get(
                        "status"
                    )
                    or ""
                )
                for horizon in (1, 5, 20)
            },
            "decision_scope": "FORWARD_SHADOW_RESEARCH_ONLY",
            "promotion_eligible": False,
            "order_authority": False,
        }

    def effective_runtime_provenance(
        self,
        *,
        decision_as_of: datetime,
    ) -> dict[str, Any]:
        """Resolve the one authoritative model cohort for a point in time.

        The frozen proxy suite remains the fail-closed baseline.  It is
        replaced only when the registry and release ledger jointly expose a
        complete, current, same-suite T1/T5/T20 Shadow release.
        """

        if decision_as_of.tzinfo is None or decision_as_of.utcoffset() is None:
            raise ValueError("decision_as_of must include a timezone")
        runtime = dict(_runtime_provenance())
        runtime.update({
            "runtime_model_source": "FROZEN_PROXY_SUITE",
            "runtime_prediction_kind": "PROXY_SCORE",
            "suite_release_id": None,
            "artifact_registry_status": "AVAILABLE",
            "order_authority": False,
        })
        from .shadow_intelligence_worker import _trainable_model_specs

        specs = _trainable_model_specs(load_v3_config())
        try:
            suite = self.latest_verified_horizon_suite(
                model_specs=specs,
                decision_as_of=decision_as_of,
            )
        except SQLAlchemyError:
            runtime["artifact_registry_status"] = "UNAVAILABLE"
            return runtime
        if suite is None:
            return runtime
        members = {
            int(key): dict(value)
            for key, value in dict(
                suite.get("artifacts_by_horizon") or {}
            ).items()
        }
        if set(members) != {1, 5, 20} or bool(
            suite.get("order_authority")
        ):
            raise RuntimeError("CURRENT_HORIZON_ARTIFACT_SUITE_INVALID")
        artifact_hashes: dict[str, str] = {}
        for horizon in (1, 5, 20):
            row = members[horizon]
            release_id = str(row.get("release_id") or "").strip()
            artifact_hash = _digest_text(
                row.get("artifact_id"),
                f"runtime_artifact_hash.T+{horizon}",
            )
            if not release_id or release_id in artifact_hashes:
                raise RuntimeError("CURRENT_HORIZON_RELEASE_SUITE_INVALID")
            artifact_hashes[release_id] = artifact_hash
        if len(set(artifact_hashes.values())) != 3:
            raise RuntimeError("CURRENT_HORIZON_ARTIFACTS_NOT_INDEPENDENT")
        runtime.update({
            "model_artifact_hashes": dict(sorted(artifact_hashes.items())),
            "runtime_model_source": "PERSISTED_SHADOW_OOS_SUITE",
            "runtime_prediction_kind": "CALIBRATED_OOS",
            "suite_release_id": str(suite["suite_release_id"]),
            "artifact_registry_status": "AVAILABLE",
            "order_authority": False,
        })
        return runtime

    def save_horizon_contracts(
        self,
        rows: Iterable[tuple[HorizonForecastContract, str]],
        *,
        created_at: datetime,
    ) -> dict[str, Any]:
        from .horizon_models import predict_horizon_artifact
        from .shadow_intelligence_worker import _model_specs, score_proxy_model

        created = _utc_naive(created_at)
        runtime = _runtime_provenance()
        specs = {
            int(item["horizon_days"]): item
            for item in _model_specs(load_v3_config())
        }
        inserted = 0
        existing = 0
        contract_ids: list[str] = []
        with self.engine.begin() as connection:
            for contract, raw_source_forecast_id in rows:
                source_forecast_id = str(raw_source_forecast_id or "").strip()
                if not source_forecast_id:
                    raise ValueError("source_forecast_id must not be empty")
                source = connection.execute(
                    text(
                        """
                        SELECT f.forecast_id, f.run_uid, f.stock_code,
                               f.strategy_key, f.raw_score, f.feature_time,
                               f.valid_until, f.forecast_status,
                               f.model_version AS source_model_version,
                               f.dataset_hash, f.features_json, f.reasons_json,
                               r.status AS run_status, r.decision_at,
                               r.result_hash AS decision_result_hash,
                               r.data_snapshot_hash, r.config_hash,
                               t.target_id, t.strategy_keys_json,
                               t.attribution_snapshot_hash,
                               t.status AS target_status
                        FROM st_alpha_forecast_v3 f
                        JOIN st_decision_run_v3 r ON r.run_uid = f.run_uid
                        LEFT JOIN st_target_portfolio_v3 t
                          ON t.run_uid = f.run_uid
                         AND t.stock_code = f.stock_code
                        WHERE f.forecast_id = :forecast_id
                        """
                    ),
                    {"forecast_id": source_forecast_id},
                ).mappings().first()
                if source is None or str(source["run_status"]) != "COMPLETED":
                    raise RuntimeError("SHADOW_SOURCE_FORECAST_NOT_COMPLETED")
                source = dict(source)
                try:
                    features = json.loads(str(source["features_json"] or "{}"))
                    strategy_keys = tuple(sorted({
                        str(item)
                        for item in json.loads(
                            str(source["strategy_keys_json"] or "[]")
                        )
                        if str(item)
                    }))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError(
                        "SHADOW_SOURCE_EVIDENCE_JSON_INVALID"
                    ) from exc
                selected = bool(
                    source.get("target_id") is not None
                    and str(source["strategy_key"]) in strategy_keys
                )
                selection_status = "SELECTED" if selected else "REJECTED"
                selection_reason = (
                    "TARGET_PORTFOLIO_SELECTED"
                    if selected
                    else "NOT_SELECTED_IN_FROZEN_TARGET"
                )
                source_snapshot = {
                    "forecast_id": source_forecast_id,
                    "run_uid": str(source["run_uid"]),
                    "stock_code": str(source["stock_code"]),
                    "strategy_key": str(source["strategy_key"]),
                    "source_model_version": str(
                        source.get("source_model_version") or ""
                    ),
                    "dataset_hash": str(source.get("dataset_hash") or ""),
                    "forecast_status": str(
                        source.get("forecast_status") or ""
                    ),
                    "raw_score": float(source["raw_score"]),
                    "feature_time": _market_aware(
                        source["feature_time"]
                    ).isoformat(),
                    "valid_until": _market_aware(
                        source["valid_until"]
                    ).isoformat(),
                    "features": features,
                    "reasons_json": str(source.get("reasons_json") or ""),
                    "decision_result_hash": str(
                        source.get("decision_result_hash") or ""
                    ),
                    "data_snapshot_hash": str(
                        source.get("data_snapshot_hash") or ""
                    ),
                    "decision_config_hash": str(
                        source.get("config_hash") or ""
                    ),
                }
                selection_snapshot = {
                    "target_id": source.get("target_id"),
                    "target_status": source.get("target_status"),
                    "target_strategy_keys": list(strategy_keys),
                    "attribution_snapshot_hash": source.get(
                        "attribution_snapshot_hash"
                    ),
                    "selection_status": selection_status,
                    "selection_reason_code": selection_reason,
                }
                selection_evidence = {
                    "source_forecast_id": source_forecast_id,
                    "source_forecast_hash": _hash(source_snapshot),
                    "run_uid": str(source["run_uid"]),
                    "source_strategy_key": str(source["strategy_key"]),
                    "decision_result_hash": str(
                        source["decision_result_hash"]
                    ),
                    "selection_snapshot": selection_snapshot,
                }
                spec = specs.get(contract.horizon_days)
                release_id = self.release_id(
                    model_key=contract.model_key,
                    model_version=contract.model_version,
                    horizon_days=contract.horizon_days,
                )
                prediction_kind = contract.prediction_kind.value
                calibration_hash: str | None = None
                scoring_mismatch = False
                if prediction_kind == "PROXY_SCORE":
                    frozen_proxy_hash = runtime[
                        "model_artifact_hashes"
                    ].get(release_id)
                    if contract.model_artifact_hash == frozen_proxy_hash:
                        rescored = score_proxy_model(features, spec=spec or {})
                        scoring_mismatch = (
                            contract.feature_protocol_hash
                            != str(rescored["feature_protocol_hash"])
                            or dict(contract.model_inputs)
                            != dict(rescored["model_inputs"])
                            or abs(contract.score - float(rescored["score"]))
                            > 1e-9
                            or contract.expected_return_net_pct is not None
                            or contract.probability_positive is not None
                            or contract.calibration_evidence is not None
                            or bool(contract.imputed_feature_keys)
                        )
                    else:
                        # A gate-BLOCK artifact may accumulate strictly
                        # research-only forward evidence, but it must retain
                        # PROXY_SCORE semantics so neither the Python contract
                        # nor the database trigger can mistake the row for a
                        # calibrated/releasable forecast.  The repository still
                        # reloads and re-scores the exact immutable artifact.
                        registered = connection.execute(
                            text(
                                """
                                SELECT * FROM st_horizon_model_artifact_v3
                                WHERE model_key = :model_key
                                  AND model_version = :model_version
                                  AND horizon_days = :horizon_days
                                  AND model_artifact_hash = :model_artifact_hash
                                  AND artifact_status = 'BLOCKED'
                                  AND training_receipt_status = 'PROCESS_VERIFIED'
                                  AND artifact_schema_version = :artifact_schema_version
                                  AND model_protocol = :model_protocol
                                  AND selection_policy_hash = :selection_policy_hash
                                  AND candidate_ledger_schema_version
                                      = :candidate_ledger_schema_version
                                  AND candidate_ledger_content_sha256 IS NOT NULL
                                  AND candidate_ledger_row_count > 0
                                  AND ledger_registration_evidence_hash IS NOT NULL
                                  AND registration_verification_hash IS NOT NULL
                                  AND registration_evidence_hash IS NOT NULL
                                  AND config_hash = :config_hash
                                  AND code_version = :code_version
                                  AND created_at <= :decision_as_of
                                  AND evidence_valid_until >= :decision_as_of
                                LIMIT 1
                                """
                            ),
                            {
                                "model_key": contract.model_key,
                                "model_version": contract.model_version,
                                "horizon_days": contract.horizon_days,
                                "model_artifact_hash": (
                                    contract.model_artifact_hash
                                ),
                                "artifact_schema_version": (
                                    CURRENT_HORIZON_ARTIFACT_SCHEMA
                                ),
                                "model_protocol": (
                                    CURRENT_HORIZON_MODEL_PROTOCOL
                                ),
                                "selection_policy_hash": (
                                    CURRENT_HORIZON_SELECTION_POLICY_HASH
                                ),
                                "candidate_ledger_schema_version": (
                                    CANDIDATE_EVALUATION_LEDGER_SCHEMA
                                ),
                                "config_hash": str(
                                    source.get("config_hash") or ""
                                ),
                                "code_version": code_version()[0],
                                "decision_as_of": _utc_naive(
                                    contract.decision_as_of
                                ),
                            },
                        ).mappings().first()
                        if registered is None:
                            raise RuntimeError(
                                "BLOCKED_RESEARCH_CONTRACT_REQUIRES_CURRENT_PROCESS_VERIFIED_ARTIFACT"
                            )
                        artifact_document, _ = _artifact_row_document(
                            dict(registered),
                            require_current_code=True,
                            require_current_config=True,
                        )
                        artifact_gate = dict(
                            artifact_document.get("gate") or {}
                        )
                        if (
                            str(artifact_gate.get("status") or "")
                            != "BLOCK"
                            or artifact_document.get("contract_eligible")
                            is not False
                            or artifact_gate.get("contract_eligible") is not False
                            or bool(artifact_document.get("order_authority"))
                            or bool(artifact_gate.get("order_authority"))
                        ):
                            raise RuntimeError(
                                "BLOCKED_RESEARCH_CONTRACT_ARTIFACT_GATE_BINDING_INVALID"
                            )
                        model = dict(
                            artifact_document.get("final_model") or {}
                        )
                        names = tuple(
                            str(item) for item in model.get("features") or ()
                        )
                        if not names:
                            raise RuntimeError(
                                "VERIFIED_HORIZON_ARTIFACT_MODEL_INPUTS_INVALID"
                            )
                        expected_inputs: dict[str, float] = {}
                        for name in names:
                            try:
                                observed_value = float(features[name])
                            except (KeyError, TypeError, ValueError) as exc:
                                raise RuntimeError(
                                    "BLOCKED_RESEARCH_CONTRACT_FEATURE_IMPUTATION_FORBIDDEN"
                                ) from exc
                            if not math.isfinite(observed_value):
                                raise RuntimeError(
                                    "BLOCKED_RESEARCH_CONTRACT_FEATURE_IMPUTATION_FORBIDDEN"
                                )
                            expected_inputs[name] = observed_value
                        prediction = predict_horizon_artifact(
                            artifact_document,
                            expected_inputs,
                        )
                        execution = dict(
                            artifact_document.get("execution_feasibility")
                            or {}
                        )
                        scoring_mismatch = (
                            str(prediction.model_artifact_hash)
                            != contract.model_artifact_hash
                            or str(prediction.feature_protocol_hash)
                            != contract.feature_protocol_hash
                            or dict(contract.model_inputs) != expected_inputs
                            or abs(contract.score - float(prediction.score))
                            > 1e-9
                            or contract.expected_return_net_pct is not None
                            or contract.probability_positive is not None
                            or contract.calibration_evidence is not None
                            or bool(contract.imputed_feature_keys)
                            or bool(prediction.contract_eligible)
                            or contract.cost_model_version
                            != str(execution.get("cost_model_version") or "")
                            or abs(
                                contract.cost_assumption_pct
                                - float(
                                    execution.get("cost_assumption_pct")
                                    or 0.0
                                )
                            )
                            > 1e-9
                        )
                elif prediction_kind == "CALIBRATED_OOS":
                    registered = connection.execute(
                        text(
                            """
                            SELECT * FROM st_horizon_model_artifact_v3
                            WHERE model_key = :model_key
                              AND model_version = :model_version
                              AND horizon_days = :horizon_days
                              AND model_artifact_hash = :model_artifact_hash
                              AND artifact_status = 'OOS_VERIFIED'
                              AND training_receipt_status = 'PROCESS_VERIFIED'
                              AND artifact_schema_version = :artifact_schema_version
                              AND model_protocol = :model_protocol
                              AND selection_policy_hash = :selection_policy_hash
                              AND candidate_ledger_schema_version
                                  = :candidate_ledger_schema_version
                              AND candidate_ledger_content_sha256 IS NOT NULL
                              AND candidate_ledger_row_count > 0
                              AND ledger_registration_evidence_hash IS NOT NULL
                              AND registration_verification_hash IS NOT NULL
                              AND registration_evidence_hash IS NOT NULL
                              AND config_hash = :config_hash
                              AND code_version = :code_version
                              AND created_at <= :decision_as_of
                              AND evidence_valid_until >= :decision_as_of
                            LIMIT 1
                            """
                        ),
                        {
                            "model_key": contract.model_key,
                            "model_version": contract.model_version,
                            "horizon_days": contract.horizon_days,
                            "model_artifact_hash": contract.model_artifact_hash,
                            "artifact_schema_version": (
                                CURRENT_HORIZON_ARTIFACT_SCHEMA
                            ),
                            "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
                            "selection_policy_hash": (
                                CURRENT_HORIZON_SELECTION_POLICY_HASH
                            ),
                            "candidate_ledger_schema_version": (
                                CANDIDATE_EVALUATION_LEDGER_SCHEMA
                            ),
                            "config_hash": str(source.get("config_hash") or ""),
                            "code_version": code_version()[0],
                            "decision_as_of": _utc_naive(
                                contract.decision_as_of
                            ),
                        },
                    ).mappings().first()
                    if registered is None:
                        raise RuntimeError(
                            "CALIBRATED_CONTRACT_REQUIRES_CURRENT_PROCESS_VERIFIED_ARTIFACT"
                        )
                    artifact_document, _ = _artifact_row_document(
                        dict(registered),
                        require_current_code=True,
                        require_current_config=True,
                    )
                    artifact_gate_status = str(
                        dict(artifact_document.get("gate") or {}).get(
                            "status"
                        )
                        or ""
                    )
                    if (
                        artifact_gate_status != "PASS"
                        or str(registered.get("artifact_status") or "")
                        != "OOS_VERIFIED"
                    ):
                        raise RuntimeError(
                            "CALIBRATED_CONTRACT_ARTIFACT_GATE_BINDING_INVALID"
                        )
                    model = dict(artifact_document.get("final_model") or {})
                    names = tuple(str(item) for item in model.get("features") or ())
                    medians = tuple(float(item) for item in model.get("medians") or ())
                    if not names or len(names) != len(medians):
                        raise RuntimeError(
                            "VERIFIED_HORIZON_ARTIFACT_MODEL_INPUTS_INVALID"
                        )
                    expected_inputs: dict[str, float] = {}
                    expected_imputed_feature_keys: list[str] = []
                    for name, median in zip(names, medians, strict=True):
                        try:
                            observed_value = float(features.get(name))
                        except (TypeError, ValueError):
                            observed_value = median
                            expected_imputed_feature_keys.append(name)
                        if not math.isfinite(observed_value):
                            observed_value = median
                            if name not in expected_imputed_feature_keys:
                                expected_imputed_feature_keys.append(name)
                        expected_inputs[name] = observed_value
                    prediction = predict_horizon_artifact(
                        artifact_document,
                        expected_inputs,
                    )
                    artifact_evidence = dict(
                        artifact_document["oos_evidence"]
                    )
                    execution = dict(
                        artifact_document["execution_feasibility"]
                    )
                    evidence_valid_until = datetime.combine(
                        _date_value(artifact_document["valid_until"]),
                        time(23, 59, 59, 999999),
                        tzinfo=timezone.utc,
                    )
                    expected_calibration = {
                        "evidence_id": str(
                            artifact_document["oos_evidence_hash"]
                        ),
                        "model_key": str(artifact_document["model_key"]),
                        "model_version": str(
                            artifact_document["model_version"]
                        ),
                        "horizon_days": int(
                            artifact_document["horizon_days"]
                        ),
                        "dataset_hash": str(artifact_document["dataset_hash"]),
                        "feature_protocol_hash": str(
                            artifact_document["feature_protocol_hash"]
                        ),
                        "cost_model_version": str(
                            execution["cost_model_version"]
                        ),
                        "cost_assumption_pct": float(
                            execution["cost_assumption_pct"]
                        ),
                        "matured_sample_count": int(
                            artifact_evidence["matured_sample_count"]
                        ),
                        "oos_sample_count": int(
                            artifact_evidence["oos_sample_count"]
                        ),
                        "walk_forward_fold_count": int(
                            artifact_evidence["walk_forward_fold_count"]
                        ),
                        "outcomes_include_costs": bool(
                            artifact_evidence["outcomes_include_costs"]
                        ),
                        "score_direction_valid": True,
                        "calibration_mae": float(
                            artifact_evidence["calibration_mae"]
                        ),
                        "brier_score": float(
                            artifact_evidence["brier_score"]
                        ),
                        "generated_at": _datetime_utc(
                            artifact_document["created_at"]
                        ).isoformat(),
                        "valid_until": evidence_valid_until.isoformat(),
                    }
                    actual_calibration = (
                        contract.calibration_evidence.as_dict()
                        if contract.calibration_evidence is not None
                        else None
                    )
                    calibration_hash = expected_calibration["evidence_id"]
                    scoring_mismatch = (
                        contract.model_artifact_hash
                        != str(prediction.model_artifact_hash)
                        or contract.feature_protocol_hash
                        != str(prediction.feature_protocol_hash)
                        or dict(contract.model_inputs) != expected_inputs
                        or tuple(contract.imputed_feature_keys)
                        != tuple(sorted(expected_imputed_feature_keys))
                        or abs(contract.score - float(prediction.score)) > 1e-9
                        or contract.expected_return_net_pct is None
                        or abs(
                            float(contract.expected_return_net_pct or 0.0)
                            - float(prediction.expected_return_net_pct)
                        ) > 1e-9
                        or contract.probability_positive is None
                        or abs(
                            float(contract.probability_positive or 0.0)
                            - float(prediction.probability_positive)
                        ) > 1e-9
                        or contract.cost_model_version
                        != expected_calibration["cost_model_version"]
                        or abs(
                            contract.cost_assumption_pct
                            - float(expected_calibration["cost_assumption_pct"])
                        ) > 1e-9
                        or _json(actual_calibration)
                        != _json(expected_calibration)
                        or str(prediction.calibration_evidence_hash)
                        != calibration_hash
                        or bool(prediction.contract_eligible)
                        != (artifact_gate_status == "PASS")
                    )
                else:
                    scoring_mismatch = True
                if (
                    contract.run_uid != str(source["run_uid"])
                    or contract.stock_code != str(source["stock_code"])
                    or contract.source_strategy_key
                    != str(source["strategy_key"])
                    or contract.source_forecast_hash != _hash(source_snapshot)
                    or dict(contract.source_evidence) != source_snapshot
                    or contract.decision_result_hash
                    != str(source["decision_result_hash"])
                    or contract.selection_status != selection_status
                    or contract.selection_reason_code != selection_reason
                    or contract.selection_evidence_hash
                    != _hash(selection_evidence)
                    or dict(contract.selection_evidence)
                    != selection_evidence
                    or scoring_mismatch
                    or contract.decision_as_of
                    != _market_aware(source["decision_at"]).astimezone(timezone.utc)
                    or contract.feature_as_of
                    != _market_aware(source["feature_time"]).astimezone(timezone.utc)
                ):
                    raise RuntimeError("SHADOW_SOURCE_CONTRACT_EVIDENCE_MISMATCH")
                payload = contract.as_dict()
                contract_hash = _hash(payload)
                contract_id = hashlib.sha256(
                    (
                        f"{source_forecast_id}|{contract.forecast_id}|"
                        f"{contract.model_key}|{contract.horizon_days}"
                    ).encode("utf-8")
                ).hexdigest()
                result = connection.execute(
                    text(
                        f"""
                        {self._insert_ignore} INTO
                        st_horizon_forecast_contract_v3 (
                            contract_id, source_forecast_id, run_uid,
                            stock_code, model_key, model_version,
                            source_strategy_key, source_forecast_hash,
                            source_evidence_json,
                            decision_result_hash, feature_protocol_hash,
                            model_artifact_hash, model_inputs_json,
                            selection_status, selection_reason_code,
                            selection_evidence_hash, selection_evidence_json,
                            horizon_days, prediction_kind,
                            decision_as_of, feature_as_of,
                            decision_session_date,
                            entry_trade_date, earliest_exit_trade_date,
                            outcome_matures_on,
                            entry_session_sequence,
                            earliest_exit_session_sequence,
                            outcome_maturity_session_sequence, score,
                            expected_return_net_pct, probability_positive,
                            cost_assumption_pct, cost_model_version,
                            calibration_evidence_hash, contract_hash,
                            contract_json, decision_scope,
                            order_authority, created_at
                        ) VALUES (
                            :contract_id, :source_forecast_id, :run_uid,
                            :stock_code, :model_key, :model_version,
                            :source_strategy_key, :source_forecast_hash,
                            :source_evidence_json,
                            :decision_result_hash, :feature_protocol_hash,
                            :model_artifact_hash, :model_inputs_json,
                            :selection_status, :selection_reason_code,
                            :selection_evidence_hash, :selection_evidence_json,
                            :horizon_days, :prediction_kind,
                            :decision_as_of, :feature_as_of,
                            :decision_session_date,
                            :entry_trade_date, :earliest_exit_trade_date,
                            :outcome_matures_on,
                            :entry_session_sequence,
                            :earliest_exit_session_sequence,
                            :outcome_maturity_session_sequence, :score,
                            :expected_return_net_pct, :probability_positive,
                            :cost_assumption_pct, :cost_model_version,
                            :calibration_evidence_hash, :contract_hash,
                            :contract_json, 'RESEARCH_ONLY', 0,
                            :created_at
                        )
                        """
                    ),
                    {
                        "contract_id": contract_id,
                        "source_forecast_id": source_forecast_id,
                        "run_uid": contract.run_uid,
                        "stock_code": contract.stock_code,
                        "model_key": contract.model_key,
                        "model_version": contract.model_version,
                        "source_strategy_key": contract.source_strategy_key,
                        "source_forecast_hash": contract.source_forecast_hash,
                        "source_evidence_json": _json(
                            dict(contract.source_evidence)
                        ),
                        "decision_result_hash": contract.decision_result_hash,
                        "feature_protocol_hash": contract.feature_protocol_hash,
                        "model_artifact_hash": contract.model_artifact_hash,
                        "model_inputs_json": _json(dict(contract.model_inputs)),
                        "selection_status": contract.selection_status,
                        "selection_reason_code": (
                            contract.selection_reason_code
                        ),
                        "selection_evidence_hash": (
                            contract.selection_evidence_hash
                        ),
                        "selection_evidence_json": _json(
                            dict(contract.selection_evidence)
                        ),
                        "horizon_days": contract.horizon_days,
                        "prediction_kind": contract.prediction_kind.value,
                        "decision_as_of": _utc_naive(contract.decision_as_of),
                        "feature_as_of": _utc_naive(contract.feature_as_of),
                        "decision_session_date": contract.decision_session_date,
                        "entry_trade_date": contract.entry_trade_date,
                        "earliest_exit_trade_date": (
                            contract.earliest_exit_trade_date
                        ),
                        "outcome_matures_on": contract.outcome_matures_on,
                        "entry_session_sequence": contract.entry_session_sequence,
                        "earliest_exit_session_sequence": (
                            contract.earliest_exit_session_sequence
                        ),
                        "outcome_maturity_session_sequence": (
                            contract.outcome_maturity_session_sequence
                        ),
                        "score": contract.score,
                        "expected_return_net_pct": (
                            contract.expected_return_net_pct
                        ),
                        "probability_positive": contract.probability_positive,
                        "cost_assumption_pct": contract.cost_assumption_pct,
                        "cost_model_version": contract.cost_model_version,
                        "calibration_evidence_hash": calibration_hash,
                        "contract_hash": contract_hash,
                        "contract_json": _json(payload),
                        "created_at": created,
                    },
                )
                observed = connection.execute(
                    text(
                        """
                        SELECT contract_id, contract_hash
                        FROM st_horizon_forecast_contract_v3
                        WHERE source_forecast_id = :source_forecast_id
                          AND model_key = :model_key
                          AND model_version = :model_version
                          AND horizon_days = :horizon_days
                          AND model_artifact_hash = :model_artifact_hash
                        """
                    ),
                    {
                        "source_forecast_id": source_forecast_id,
                        "model_key": contract.model_key,
                        "model_version": contract.model_version,
                        "horizon_days": contract.horizon_days,
                        "model_artifact_hash": contract.model_artifact_hash,
                    },
                ).mappings().first()
                if (
                    observed is None
                    or str(observed["contract_id"]) != contract_id
                    or str(observed["contract_hash"]) != contract_hash
                ):
                    raise RuntimeError(
                        "SHADOW_HORIZON_CONTRACT_IDEMPOTENCY_CONFLICT"
                    )
                contract_ids.append(contract_id)
                if int(getattr(result, "rowcount", 0) or 0) == 1:
                    inserted += 1
                else:
                    existing += 1
        return {
            "status": "ok",
            "inserted_count": inserted,
            "existing_count": existing,
            "contract_ids": contract_ids,
            "order_authority": False,
        }

    def horizon_contracts(
        self,
        *,
        run_uid: str | None = None,
        evaluation_date: date | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        evaluated_on = evaluation_date or datetime.now(MARKET_TIMEZONE).date()
        if isinstance(evaluated_on, datetime) or not isinstance(
            evaluated_on, date
        ):
            raise ValueError("evaluation_date must be a date")
        parameters: dict[str, Any] = {
            "limit": max(1, min(int(limit), 10000)),
            "evaluation_date": evaluated_on,
        }
        clause = ""
        if run_uid:
            clause = "WHERE h.run_uid = :run_uid"
            parameters["run_uid"] = str(run_uid)
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT h.*,
                           CASE
                               WHEN o.outcome_status = 'MATURED_VERIFIED'
                               THEN 'MATURED_VERIFIED'
                               WHEN o.outcome_status = 'QUARANTINED'
                               THEN 'QUARANTINED'
                               WHEN h.outcome_matures_on <= :evaluation_date
                               THEN 'AWAITING_OUTCOME_EVIDENCE'
                               ELSE 'OPEN'
                           END AS derived_contract_status
                    FROM st_horizon_forecast_contract_v3 h
                    LEFT JOIN st_horizon_outcome_v3 o
                      ON o.contract_id = h.contract_id
                    {clause}
                    ORDER BY h.decision_as_of DESC, h.contract_id
                    LIMIT :limit
                    """
                ),
                parameters,
            ).mappings().all()
        return [
            {
                **dict(row),
                "status_evaluated_on": evaluated_on,
                "evaluation_timezone": "Asia/Shanghai",
            }
            for row in rows
        ]

    def mature_horizon_contracts(
        self,
        *,
        evaluation_date: date,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        if isinstance(evaluation_date, datetime) or not isinstance(
            evaluation_date, date
        ):
            raise ValueError("evaluation_date must be a date")
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT h.*
                    FROM st_horizon_forecast_contract_v3 h
                    LEFT JOIN st_horizon_outcome_v3 o
                      ON o.contract_id = h.contract_id
                    WHERE h.outcome_matures_on <= :evaluation_date
                      AND o.outcome_id IS NULL
                    ORDER BY h.outcome_matures_on, h.contract_id
                    LIMIT :limit
                    """
                ),
                {
                    "evaluation_date": evaluation_date,
                    "limit": max(1, min(int(limit), 100000)),
                },
            ).mappings().all()
        return [dict(row) for row in rows]

    def save_horizon_outcomes(
        self,
        rows: Iterable[
            tuple[HorizonOutcomeEvidence, Mapping[str, Any]]
        ],
        *,
        created_at: datetime,
    ) -> dict[str, Any]:
        created = _utc_naive(created_at)
        inserted = 0
        existing = 0
        outcome_ids: list[str] = []
        with self.engine.begin() as connection:
            for outcome, raw_market_evidence in rows:
                if outcome.observed_at.astimezone(MARKET_TIMEZONE).date() <= (
                    outcome.exit_trade_date
                ):
                    raise RuntimeError(
                        "SHADOW_HORIZON_OUTCOME_REQUIRES_CLOSED_PRIOR_SESSION"
                    )
                market_evidence = dict(raw_market_evidence)
                if _hash(market_evidence) != outcome.market_evidence_hash:
                    raise ValueError("market evidence hash does not match payload")
                _validate_outcome_evidence(outcome, market_evidence)
                contract = connection.execute(
                    text(
                        """
                        SELECT contract_id, contract_hash, stock_code,
                               horizon_days, entry_trade_date,
                               outcome_matures_on, cost_assumption_pct,
                               cost_model_version
                        FROM st_horizon_forecast_contract_v3
                        WHERE contract_id = :contract_id
                        """
                    ),
                    {"contract_id": outcome.contract_id},
                ).mappings().first()
                if contract is None:
                    raise RuntimeError("SHADOW_HORIZON_OUTCOME_CONTRACT_MISSING")
                if (
                    str(contract["contract_hash"]) != outcome.contract_hash
                    or str(contract["stock_code"]) != outcome.stock_code
                    or int(contract["horizon_days"]) != outcome.horizon_days
                    or _date_value(contract["entry_trade_date"])
                    != outcome.entry_trade_date
                    or _date_value(contract["outcome_matures_on"])
                    != outcome.exit_trade_date
                    or abs(
                        float(contract["cost_assumption_pct"])
                        - outcome.realized_cost_pct
                    ) > 1e-8
                    or str(contract["cost_model_version"])
                    != outcome.cost_model_version
                ):
                    raise RuntimeError("SHADOW_HORIZON_OUTCOME_CONTRACT_MISMATCH")
                payload = outcome.as_dict()
                outcome_hash = _hash(payload)
                outcome_id = hashlib.sha256(
                    f"{outcome.contract_id}|{outcome_hash}".encode("utf-8")
                ).hexdigest()
                result = connection.execute(
                    text(
                        f"""
                        {self._insert_ignore} INTO st_horizon_outcome_v3 (
                            outcome_id, contract_id, contract_hash,
                            stock_code, horizon_days, entry_trade_date,
                            exit_trade_date, entry_price, exit_price,
                            gross_return_pct, realized_cost_pct,
                            realized_net_return_pct, realized_mae_pct,
                            realized_mfe_pct, bar_count,
                            cost_model_version, market_data_source,
                            market_evidence_hash, market_evidence_json,
                            execution_feasibility,
                            outcome_hash, outcome_status, order_authority,
                            observed_at, created_at
                        ) VALUES (
                            :outcome_id, :contract_id, :contract_hash,
                            :stock_code, :horizon_days, :entry_trade_date,
                            :exit_trade_date, :entry_price, :exit_price,
                            :gross_return_pct, :realized_cost_pct,
                            :realized_net_return_pct, :realized_mae_pct,
                            :realized_mfe_pct, :bar_count,
                            :cost_model_version, :market_data_source,
                            :market_evidence_hash, :market_evidence_json,
                            :execution_feasibility,
                            :outcome_hash, :outcome_status, 0,
                            :observed_at, :created_at
                        )
                        """
                    ),
                    {
                        **payload,
                        "outcome_id": outcome_id,
                        "market_evidence_json": _json(market_evidence),
                        "outcome_hash": outcome_hash,
                        "observed_at": _utc_naive(outcome.observed_at),
                        "created_at": created,
                    },
                )
                observed = connection.execute(
                    text(
                        """
                        SELECT outcome_id, outcome_hash, market_evidence_hash
                        FROM st_horizon_outcome_v3
                        WHERE contract_id = :contract_id
                        """
                    ),
                    {"contract_id": outcome.contract_id},
                ).mappings().first()
                if (
                    observed is None
                    or str(observed["outcome_id"]) != outcome_id
                    or str(observed["outcome_hash"]) != outcome_hash
                    or str(observed["market_evidence_hash"])
                    != outcome.market_evidence_hash
                ):
                    raise RuntimeError("SHADOW_HORIZON_OUTCOME_IDEMPOTENCY_CONFLICT")
                outcome_ids.append(outcome_id)
                if int(getattr(result, "rowcount", 0) or 0) == 1:
                    inserted += 1
                else:
                    existing += 1
        return {
            "status": "ok",
            "inserted_count": inserted,
            "existing_count": existing,
            "outcome_ids": outcome_ids,
            "order_authority": False,
        }

    def horizon_outcomes(
        self,
        *,
        run_uid: str | None = None,
        contract_ids: Iterable[str] | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        ids = tuple(sorted({
            str(item) for item in (contract_ids or ()) if str(item)
        }))
        if contract_ids is not None and not ids:
            return []
        clauses = []
        parameters: dict[str, Any] = {
            "limit": max(1, min(int(limit), 100000))
        }
        if run_uid:
            clauses.append("h.run_uid = :run_uid")
            parameters["run_uid"] = str(run_uid)
        if ids:
            clauses.append("o.contract_id IN :contract_ids")
            parameters["contract_ids"] = ids
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        statement = text(
            f"""
            SELECT o.* FROM st_horizon_outcome_v3 o
            JOIN st_horizon_forecast_contract_v3 h
              ON h.contract_id = o.contract_id
            {where}
            ORDER BY o.exit_trade_date DESC, o.outcome_id
            LIMIT :limit
            """
        )
        if ids:
            statement = statement.bindparams(bindparam("contract_ids", expanding=True))
        with self.engine.connect() as connection:
            rows = connection.execute(
                statement,
                parameters,
            ).mappings().all()
        return [dict(row) for row in rows]

    @staticmethod
    def release_id(
        *, model_key: str, model_version: str, horizon_days: int
    ) -> str:
        return f"{model_key}:{model_version}:T+{int(horizon_days)}"

    def latest_release(self, release_id: str) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_shadow_release_v3
                    WHERE release_id = :release_id
                    ORDER BY transition_sequence DESC
                    LIMIT 1
                    """
                ),
                {"release_id": str(release_id)},
            ).mappings().first()
        return _row_dict(row)

    def ensure_release(
        self,
        *,
        model_key: str,
        model_version: str,
        horizon_days: int,
        config_hash: str,
        occurred_at: datetime,
        release_id: str | None = None,
        suite_release_id: str | None = None,
    ) -> dict[str, Any]:
        explicit_release_id = release_id is not None
        if explicit_release_id:
            from .horizon_models import horizon_governance_release_id

            if not str(suite_release_id or "").strip():
                raise RuntimeError(
                    "SHADOW_ARTIFACT_RELEASE_REQUIRES_SUITE_ID"
                )
            expected_release_id = horizon_governance_release_id(
                suite_release_id=str(suite_release_id),
                model_key=model_key,
                model_version=model_version,
                horizon_days=horizon_days,
            )
            if str(release_id) != expected_release_id:
                raise RuntimeError("SHADOW_ARTIFACT_RELEASE_IDENTITY_INVALID")
            resolved_release_id = expected_release_id
        else:
            if suite_release_id is not None:
                raise RuntimeError(
                    "SHADOW_SUITE_ID_REQUIRES_EXPLICIT_ARTIFACT_RELEASE"
                )
            resolved_release_id = self.release_id(
                model_key=model_key,
                model_version=model_version,
                horizon_days=horizon_days,
            )
        existing = self.latest_release(resolved_release_id)
        if existing is not None:
            if (
                str(existing["model_key"]) != str(model_key)
                or str(existing["model_version"]) != str(model_version)
                or int(existing["horizon_days"]) != int(horizon_days)
                or (
                    explicit_release_id
                    and str(existing.get("config_hash") or "")
                    != str(config_hash)
                )
            ):
                raise RuntimeError("SHADOW_RELEASE_IDENTITY_CONFLICT")
            return existing
        observed = _utc_naive(occurred_at)
        evidence_hash = _hash({
            "release_id": resolved_release_id,
            "stage": "DRAFT",
        })
        transition_hash = _hash({
            "release_id": resolved_release_id,
            "sequence": 0,
            "event": "INITIALIZE",
            "stage": "DRAFT",
            "evidence_hash": evidence_hash,
        })
        state_id = hashlib.sha256(
            f"{resolved_release_id}|0|{transition_hash}".encode("utf-8")
        ).hexdigest()
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    {self._insert_ignore} INTO st_shadow_release_v3 (
                        release_state_id, release_id,
                        transition_sequence, model_key, model_version,
                        horizon_days, previous_stage, current_stage,
                        release_event, transition_accepted, reason_code,
                        gate_evaluation_id, evidence_hash, config_hash,
                        transition_hash, order_authority,
                        occurred_at, created_at
                    ) VALUES (
                        :release_state_id, :release_id, 0,
                        :model_key, :model_version, :horizon_days,
                        'DRAFT', 'DRAFT', 'INITIALIZE', 1,
                        'SHADOW_RELEASE_INITIALIZED', NULL,
                        :evidence_hash, :config_hash, :transition_hash,
                        0, :occurred_at, :created_at
                    )
                    """
                ),
                {
                    "release_state_id": state_id,
                    "release_id": resolved_release_id,
                    "model_key": model_key,
                    "model_version": model_version,
                    "horizon_days": int(horizon_days),
                    "evidence_hash": evidence_hash,
                    "config_hash": str(config_hash),
                    "transition_hash": transition_hash,
                    "occurred_at": observed,
                    "created_at": observed,
                },
            )
        latest = self.latest_release(resolved_release_id)
        if latest is None or str(latest["current_stage"]) != "DRAFT":
            raise RuntimeError("SHADOW_RELEASE_INITIALIZATION_FAILED")
        return latest

    def publish_horizon_suite_shadow(
        self,
        *,
        suite_release_id: str,
        members: Iterable[Mapping[str, Any]],
        config_hash: str,
        occurred_at: datetime,
    ) -> dict[str, Any]:
        """Initialize/START_SHADOW for T1/T5/T20 in one DB transaction."""

        from .horizon_models import horizon_governance_release_id
        from .release_governance import ReleaseEvent, ReleaseStage
        from .release_governance import transition_shadow_release

        suite_id = str(suite_release_id or "").strip()
        rows = [dict(item) for item in members]
        by_horizon = {
            int(item.get("horizon_days") or 0): item for item in rows
        }
        if (
            not suite_id
            or len(rows) != 3
            or set(by_horizon) != {1, 5, 20}
        ):
            raise RuntimeError("SHADOW_SUITE_PUBLICATION_IDENTITY_INVALID")
        for horizon, item in by_horizon.items():
            if str(item.get("suite_release_id") or "") != suite_id:
                raise RuntimeError("SHADOW_SUITE_PUBLICATION_IDENTITY_INVALID")
            expected_release_id = horizon_governance_release_id(
                suite_release_id=suite_id,
                model_key=str(item.get("model_key") or ""),
                model_version=str(item.get("model_version") or ""),
                horizon_days=horizon,
            )
            if str(item.get("release_id") or "") != expected_release_id:
                raise RuntimeError("SHADOW_SUITE_PUBLICATION_IDENTITY_INVALID")
            item["evidence_hash"] = _digest_text(
                item.get("evidence_hash"),
                f"suite_member_{horizon}_evidence_hash",
            )
        observed = _utc_naive(occurred_at)
        allowed_runtime_stages = {
            "SHADOW", "CALIBRATION_REVIEW", "PAPER_ELIGIBLE",
        }

        def _latest(connection: Any, release_id: str) -> dict[str, Any] | None:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_shadow_release_v3
                    WHERE release_id = :release_id
                    ORDER BY transition_sequence DESC
                    LIMIT 1
                    """
                ),
                {"release_id": release_id},
            ).mappings().first()
            return dict(row) if row is not None else None

        with self.engine.begin() as connection:
            latest_by_horizon: dict[int, dict[str, Any]] = {}
            for horizon, item in sorted(by_horizon.items()):
                release_id = str(item["release_id"])
                latest = _latest(connection, release_id)
                if latest is None:
                    draft_evidence_hash = _hash({
                        "release_id": release_id,
                        "stage": "DRAFT",
                    })
                    draft_transition_hash = _hash({
                        "release_id": release_id,
                        "sequence": 0,
                        "event": "INITIALIZE",
                        "stage": "DRAFT",
                        "evidence_hash": draft_evidence_hash,
                    })
                    state_id = hashlib.sha256(
                        f"{release_id}|0|{draft_transition_hash}".encode(
                            "utf-8"
                        )
                    ).hexdigest()
                    connection.execute(
                        text(
                            f"""
                            {self._insert_ignore} INTO st_shadow_release_v3 (
                                release_state_id, release_id,
                                transition_sequence, model_key, model_version,
                                horizon_days, previous_stage, current_stage,
                                release_event, transition_accepted, reason_code,
                                gate_evaluation_id, evidence_hash, config_hash,
                                transition_hash, order_authority,
                                occurred_at, created_at
                            ) VALUES (
                                :release_state_id, :release_id, 0,
                                :model_key, :model_version, :horizon_days,
                                'DRAFT', 'DRAFT', 'INITIALIZE', 1,
                                'SHADOW_RELEASE_INITIALIZED', NULL,
                                :evidence_hash, :config_hash, :transition_hash,
                                0, :occurred_at, :created_at
                            )
                            """
                        ),
                        {
                            "release_state_id": state_id,
                            "release_id": release_id,
                            "model_key": str(item["model_key"]),
                            "model_version": str(item["model_version"]),
                            "horizon_days": horizon,
                            "evidence_hash": draft_evidence_hash,
                            "config_hash": str(config_hash),
                            "transition_hash": draft_transition_hash,
                            "occurred_at": observed,
                            "created_at": observed,
                        },
                    )
                    latest = _latest(connection, release_id)
                if (
                    latest is None
                    or str(latest.get("model_key") or "")
                    != str(item["model_key"])
                    or str(latest.get("model_version") or "")
                    != str(item["model_version"])
                    or int(latest.get("horizon_days") or 0) != horizon
                    or str(latest.get("config_hash") or "")
                    != str(config_hash)
                    or bool(latest.get("order_authority"))
                ):
                    raise RuntimeError("SHADOW_SUITE_RELEASE_IDENTITY_CONFLICT")
                if str(latest["current_stage"]) not in {
                    "DRAFT", *allowed_runtime_stages,
                }:
                    raise RuntimeError("SHADOW_SUITE_RELEASE_NOT_PUBLISHABLE")
                latest_by_horizon[horizon] = latest

            # All three identities/stages are preflighted before the first
            # START_SHADOW insert.  Any trigger, constraint or commit failure
            # rolls the whole suite back.
            for horizon, item in sorted(by_horizon.items()):
                latest = latest_by_horizon[horizon]
                if str(latest["current_stage"]) != "DRAFT":
                    continue
                transition = transition_shadow_release(
                    ReleaseStage.DRAFT,
                    ReleaseEvent.START_SHADOW,
                )
                transition_hash = _hash({
                    "release_id": item["release_id"],
                    "previous_stage": transition.previous_stage,
                    "event": transition.event,
                    "next_stage": transition.next_stage,
                    "accepted": transition.accepted,
                    "reason_code": transition.reason_code,
                    "gate_evaluation_id": "",
                    "evidence_hash": item["evidence_hash"],
                })
                sequence = int(latest["transition_sequence"]) + 1
                state_id = hashlib.sha256(
                    f"{item['release_id']}|{sequence}|{transition_hash}".encode(
                        "utf-8"
                    )
                ).hexdigest()
                connection.execute(
                    text(
                        """
                        INSERT INTO st_shadow_release_v3 (
                            release_state_id, release_id,
                            transition_sequence, model_key, model_version,
                            horizon_days, previous_stage, current_stage,
                            release_event, transition_accepted, reason_code,
                            gate_evaluation_id, evidence_hash, config_hash,
                            transition_hash, order_authority,
                            occurred_at, created_at
                        ) VALUES (
                            :release_state_id, :release_id,
                            :transition_sequence, :model_key, :model_version,
                            :horizon_days, :previous_stage, :current_stage,
                            :release_event, :transition_accepted, :reason_code,
                            NULL, :evidence_hash, :config_hash,
                            :transition_hash, 0, :occurred_at, :created_at
                        )
                        """
                    ),
                    {
                        "release_state_id": state_id,
                        "release_id": item["release_id"],
                        "transition_sequence": sequence,
                        "model_key": latest["model_key"],
                        "model_version": latest["model_version"],
                        "horizon_days": horizon,
                        "previous_stage": transition.previous_stage,
                        "current_stage": transition.next_stage,
                        "release_event": transition.event,
                        "transition_accepted": int(transition.accepted),
                        "reason_code": transition.reason_code,
                        "evidence_hash": item["evidence_hash"],
                        "config_hash": str(config_hash),
                        "transition_hash": transition_hash,
                        "occurred_at": observed,
                        "created_at": observed,
                    },
                )

            final_by_horizon = {
                horizon: _latest(connection, str(item["release_id"]))
                for horizon, item in by_horizon.items()
            }
            if any(
                row is None
                or str(row.get("current_stage") or "")
                not in allowed_runtime_stages
                for row in final_by_horizon.values()
            ):
                raise RuntimeError("SHADOW_SUITE_PUBLICATION_INCOMPLETE")

        return {
            "suite_release_id": suite_id,
            "releases_by_horizon": final_by_horizon,
            "order_authority": False,
        }

    def append_release_transition(
        self,
        *,
        release_id: str,
        transition: ReleaseTransition,
        evidence_hash: str,
        config_hash: str,
        occurred_at: datetime,
        gate_evaluation_id: str | None = None,
    ) -> dict[str, Any]:
        latest = self.latest_release(release_id)
        if latest is None:
            raise RuntimeError("SHADOW_RELEASE_NOT_INITIALIZED")
        if str(latest["current_stage"]) != transition.previous_stage:
            raise RuntimeError("SHADOW_RELEASE_STALE_TRANSITION")
        if transition.next_stage == "PAPER_ELIGIBLE":
            if not gate_evaluation_id:
                raise RuntimeError("PAPER_ELIGIBLE_REQUIRES_PERSISTED_PASS_GATE")
            gate = self.latest_calibration_gate(release_id)
            if (
                gate is None
                or str(gate["gate_evaluation_id"]) != gate_evaluation_id
                or str(gate["gate_status"]) != "PASS"
                or str(gate["evidence_provenance_status"])
                != "PERSISTED_VERIFIED"
                or not str(gate.get("learning_run_id") or "")
                or str(gate["evidence_hash"]) != str(evidence_hash)
                or self.learning_run(str(gate["learning_run_id"])) is None
            ):
                raise RuntimeError("PAPER_ELIGIBLE_REQUIRES_LATEST_VERIFIED_GATE")
        transition_hash = _hash({
            "release_id": release_id,
            "previous_stage": transition.previous_stage,
            "event": transition.event,
            "next_stage": transition.next_stage,
            "accepted": transition.accepted,
            "reason_code": transition.reason_code,
            "gate_evaluation_id": gate_evaluation_id or "",
            "evidence_hash": evidence_hash,
        })
        with self.engine.connect() as connection:
            duplicate = connection.execute(
                text(
                    """
                    SELECT * FROM st_shadow_release_v3
                    WHERE release_id = :release_id
                      AND transition_hash = :transition_hash
                    """
                ),
                {
                    "release_id": release_id,
                    "transition_hash": transition_hash,
                },
            ).mappings().first()
        if duplicate is not None:
            return dict(duplicate)
        sequence = int(latest["transition_sequence"]) + 1
        state_id = hashlib.sha256(
            f"{release_id}|{sequence}|{transition_hash}".encode("utf-8")
        ).hexdigest()
        observed = _utc_naive(occurred_at)
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    {self._insert_ignore} INTO st_shadow_release_v3 (
                        release_state_id, release_id,
                        transition_sequence, model_key, model_version,
                        horizon_days, previous_stage, current_stage,
                        release_event, transition_accepted, reason_code,
                        gate_evaluation_id, evidence_hash, config_hash,
                        transition_hash, order_authority,
                        occurred_at, created_at
                    ) VALUES (
                        :release_state_id, :release_id,
                        :transition_sequence, :model_key, :model_version,
                        :horizon_days, :previous_stage, :current_stage,
                        :release_event, :transition_accepted, :reason_code,
                        :gate_evaluation_id, :evidence_hash, :config_hash,
                        :transition_hash, 0, :occurred_at, :created_at
                    )
                    """
                ),
                {
                    "release_state_id": state_id,
                    "release_id": release_id,
                    "transition_sequence": sequence,
                    "model_key": latest["model_key"],
                    "model_version": latest["model_version"],
                    "horizon_days": latest["horizon_days"],
                    "previous_stage": transition.previous_stage,
                    "current_stage": transition.next_stage,
                    "release_event": transition.event,
                    "transition_accepted": int(transition.accepted),
                    "reason_code": transition.reason_code,
                    "gate_evaluation_id": gate_evaluation_id,
                    "evidence_hash": evidence_hash,
                    "config_hash": str(config_hash),
                    "transition_hash": transition_hash,
                    "occurred_at": observed,
                    "created_at": observed,
                },
            )
        persisted = self.latest_release(release_id)
        if (
            persisted is None
            or str(persisted["transition_hash"]) != transition_hash
            or int(persisted["order_authority"]) != 0
        ):
            raise RuntimeError("SHADOW_RELEASE_TRANSITION_CONFLICT")
        return persisted

    def release_transition_for_gate(
        self,
        *,
        release_id: str,
        gate_evaluation_id: str,
    ) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_shadow_release_v3
                    WHERE release_id = :release_id
                      AND gate_evaluation_id = :gate_evaluation_id
                    ORDER BY transition_sequence DESC
                    LIMIT 1
                    """
                ),
                {
                    "release_id": str(release_id),
                    "gate_evaluation_id": str(gate_evaluation_id),
                },
            ).mappings().first()
        return _row_dict(row)

    def save_calibration_gate(
        self,
        *,
        release_id: str,
        model_key: str,
        model_version: str,
        horizon_days: int,
        prediction_kind: str,
        decision: CalibrationGateDecision,
        evidence: ContinuousCalibrationEvidence | None,
        raw_evidence: Mapping[str, Any],
        policy: Mapping[str, Any],
        evaluated_at: datetime,
    ) -> dict[str, Any]:
        runtime = self.effective_runtime_provenance(
            decision_as_of=_datetime_utc(evaluated_at),
        )
        if runtime.get("artifact_registry_status") != "AVAILABLE":
            raise RuntimeError("CALIBRATION_ARTIFACT_REGISTRY_UNAVAILABLE")
        if _hash(dict(policy)) != _hash(runtime["continuous_policy"]):
            raise RuntimeError("CALIBRATION_POLICY_NOT_CURRENT_FROZEN_CONFIG")
        model_artifact_hash = runtime["model_artifact_hashes"].get(release_id)
        if not model_artifact_hash:
            raise RuntimeError("CALIBRATION_RELEASE_ARTIFACT_NOT_CURRENT")
        if (
            decision.release_id != release_id
            or int(decision.horizon_days) != int(horizon_days)
            or decision.order_authority
        ):
            raise RuntimeError("CALIBRATION_DECISION_IDENTITY_MISMATCH")
        evidence_payload = dict(raw_evidence)
        provenance_status = str(
            decision.evidence_provenance_status or ""
        ).strip()
        learning_run_id = str(
            evidence_payload.get("learning_run_id") or ""
        ).strip() or None
        if decision.status == "PASS" and (
            provenance_status != "PERSISTED_VERIFIED"
            or learning_run_id is None
        ):
            raise RuntimeError(
                "CALIBRATION_PASS_REQUIRES_PERSISTED_LEARNING_RUN"
            )
        if provenance_status == "PERSISTED_VERIFIED":
            learning_row = self.verified_learning_run(learning_run_id or "")
            if (
                learning_row is None
                or str(evidence_payload.get("learning_result_hash") or "")
                != str(learning_row["learning_result_hash"])
                or str(evidence_payload.get("learning_evidence_hash") or "")
                != str(learning_row["evidence_hash"])
                or str(evidence_payload.get("learning_policy_hash") or "")
                != str(learning_row["policy_hash"])
            ):
                raise RuntimeError(
                    "CALIBRATION_EVIDENCE_PROVENANCE_NOT_VERIFIED"
                )
        elif provenance_status != "UNVERIFIED_PREVIEW":
            raise RuntimeError("CALIBRATION_EVIDENCE_PROVENANCE_INVALID")
        if evidence is not None:
            identity_matches = (
                evidence.release_id == release_id
                and evidence.model_key == model_key
                and evidence.model_version == model_version
                and evidence.horizon_days == int(horizon_days)
                and evidence.prediction_kind.value == str(prediction_kind)
            )
            numeric_fields = (
                "matured_sample_count",
                "oos_sample_count",
                "walk_forward_fold_count",
                "direction_rank_correlation",
                "calibration_mae",
                "brier_score",
                "population_stability_index",
                "net_expectancy_after_cost_pct",
                "profit_factor",
                "cost_coverage_ratio",
            )
            raw_matches = all(
                field in evidence_payload
                and abs(
                    float(evidence_payload[field])
                    - float(getattr(evidence, field))
                ) < 1e-9
                for field in numeric_fields
            )
            raw_matches = raw_matches and all(
                str(evidence_payload.get(field) or "")
                == str(expected)
                for field, expected in (
                    ("release_id", release_id),
                    ("model_key", model_key),
                    ("model_version", model_version),
                    ("horizon_days", int(horizon_days)),
                )
            )
            try:
                raw_matches = raw_matches and (
                    _datetime_utc(evidence_payload["observed_at"])
                    == evidence.observed_at
                    and _datetime_utc(evidence_payload["valid_until"])
                    == evidence.valid_until
                )
            except (KeyError, TypeError, ValueError):
                raw_matches = False
            if not identity_matches or not raw_matches:
                raise RuntimeError("CALIBRATION_EVIDENCE_PAYLOAD_MISMATCH")
            expected_decision = evaluate_continuous_calibration(
                evidence,
                policy=runtime["continuous_policy"],
                evaluated_at=evaluated_at,
            )
            expected_decision = replace(
                expected_decision,
                evidence_provenance_status=provenance_status,
            )
            if _hash(expected_decision.as_dict()) != _hash(decision.as_dict()):
                raise RuntimeError("CALIBRATION_DECISION_NOT_RECOMPUTED")
        elif (
            decision.status != "BLOCK"
            or decision.recommended_stage != "BLOCKED"
            or provenance_status != "UNVERIFIED_PREVIEW"
        ):
            raise RuntimeError("UNVERIFIED_CALIBRATION_MUST_BLOCK")
        evidence_hash = _hash(evidence_payload)
        policy_hash = _hash(dict(policy))
        result_payload = decision.as_dict()
        result_hash = _hash(result_payload)
        evaluated = _utc_naive(evaluated_at)
        gate_id = hashlib.sha256(
            (
                f"{release_id}|{evidence_hash}|{policy_hash}|"
                f"{evaluated.isoformat()}"
            ).encode("utf-8")
        ).hexdigest()
        values = {
            "gate_evaluation_id": gate_id,
            "release_id": release_id,
            "model_key": model_key,
            "model_version": model_version,
            "horizon_days": int(horizon_days),
            "prediction_kind": prediction_kind,
            "gate_status": decision.status,
            "recommended_stage": decision.recommended_stage,
            "learning_run_id": learning_run_id,
            "evidence_provenance_status": provenance_status,
            "matured_sample_count": (
                evidence.matured_sample_count if evidence else None
            ),
            "oos_sample_count": evidence.oos_sample_count if evidence else None,
            "walk_forward_fold_count": (
                evidence.walk_forward_fold_count if evidence else None
            ),
            "direction_rank_correlation": (
                evidence.direction_rank_correlation if evidence else None
            ),
            "calibration_mae": evidence.calibration_mae if evidence else None,
            "brier_score": evidence.brier_score if evidence else None,
            "population_stability_index": (
                evidence.population_stability_index if evidence else None
            ),
            "net_expectancy_after_cost_pct": (
                evidence.net_expectancy_after_cost_pct if evidence else None
            ),
            "profit_factor": evidence.profit_factor if evidence else None,
            "cost_coverage_ratio": (
                evidence.cost_coverage_ratio if evidence else None
            ),
            "evidence_observed_at": (
                _utc_naive(evidence.observed_at) if evidence else None
            ),
            "evidence_valid_until": (
                _utc_naive(evidence.valid_until) if evidence else None
            ),
            "failure_codes_json": _json(list(decision.failure_codes)),
            "evidence_json": _json(evidence_payload),
            "evidence_hash": evidence_hash,
            "policy_hash": policy_hash,
            "config_hash": runtime["config_hash"],
            "code_version": runtime["code_version"],
            "model_artifact_hash": model_artifact_hash,
            "gate_result_hash": result_hash,
            "evaluated_at": evaluated,
            "created_at": evaluated,
        }
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    {self._insert_ignore} INTO st_calibration_gate_v3 (
                        gate_evaluation_id, release_id, model_key,
                        model_version, horizon_days, prediction_kind,
                        gate_status, recommended_stage,
                        learning_run_id, evidence_provenance_status,
                        matured_sample_count, oos_sample_count,
                        walk_forward_fold_count,
                        direction_rank_correlation, calibration_mae,
                        brier_score, population_stability_index,
                        net_expectancy_after_cost_pct, profit_factor,
                        cost_coverage_ratio, evidence_observed_at,
                        evidence_valid_until, failure_codes_json,
                        evidence_json, evidence_hash, policy_hash,
                        config_hash, code_version, model_artifact_hash,
                        gate_result_hash, order_authority,
                        evaluated_at, created_at
                    ) VALUES (
                        :gate_evaluation_id, :release_id, :model_key,
                        :model_version, :horizon_days, :prediction_kind,
                        :gate_status, :recommended_stage,
                        :learning_run_id, :evidence_provenance_status,
                        :matured_sample_count, :oos_sample_count,
                        :walk_forward_fold_count,
                        :direction_rank_correlation, :calibration_mae,
                        :brier_score, :population_stability_index,
                        :net_expectancy_after_cost_pct, :profit_factor,
                        :cost_coverage_ratio, :evidence_observed_at,
                        :evidence_valid_until, :failure_codes_json,
                        :evidence_json, :evidence_hash, :policy_hash,
                        :config_hash, :code_version, :model_artifact_hash,
                        :gate_result_hash, 0, :evaluated_at, :created_at
                    )
                    """
                ),
                values,
            )
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_calibration_gate_v3
                    WHERE gate_evaluation_id = :gate_evaluation_id
                    """
                ),
                {"gate_evaluation_id": gate_id},
            ).mappings().first()
        if row is None or str(row["gate_result_hash"]) != result_hash:
            raise RuntimeError("CALIBRATION_GATE_IDEMPOTENCY_CONFLICT")
        return dict(row)

    def latest_calibration_gate(
        self, release_id: str
    ) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_calibration_gate_v3
                    WHERE release_id = :release_id
                    ORDER BY evaluated_at DESC, gate_evaluation_id DESC
                    LIMIT 1
                    """
                ),
                {"release_id": str(release_id)},
            ).mappings().first()
        return _row_dict(row)

    def save_learning_run(
        self,
        *,
        metrics: Mapping[str, Any] | None = None,
        samples: Iterable[Mapping[str, Any]] | None = None,
        policy: Mapping[str, Any],
        evaluation_date: date,
        evaluated_at: datetime,
    ) -> dict[str, Any]:
        from .learning_intelligence import counterfactual_learning_metrics

        evaluated_aware = evaluated_at
        if evaluated_aware.tzinfo is None or evaluated_aware.utcoffset() is None:
            raise ValueError("evaluated_at must include a timezone")
        if evaluation_date != evaluated_aware.astimezone(MARKET_TIMEZONE).date():
            raise RuntimeError("LEARNING_EVALUATION_DATE_SESSION_MISMATCH")
        if evaluation_date > datetime.now(MARKET_TIMEZONE).date():
            raise RuntimeError("LEARNING_EVALUATION_DATE_IN_FUTURE")
        runtime = self.effective_runtime_provenance(
            decision_as_of=_datetime_utc(evaluated_at),
        )
        if runtime.get("artifact_registry_status") != "AVAILABLE":
            raise RuntimeError("LEARNING_ARTIFACT_REGISTRY_UNAVAILABLE")
        if _hash(dict(policy)) != _hash(runtime["continuous_policy"]):
            raise RuntimeError("LEARNING_POLICY_NOT_CURRENT_FROZEN_CONFIG")
        minimum_by_horizon = dict(
            dict(policy).get("minimum_mature_samples") or {}
        )
        if set(map(str, minimum_by_horizon)) != {"1", "5", "20"}:
            raise RuntimeError(
                "LEARNING_POLICY_REQUIRES_T1_T5_T20_THRESHOLDS"
            )
        minimum_values = {
            str(key): int(value)
            for key, value in minimum_by_horizon.items()
        }
        if min(minimum_values.values()) <= 0:
            raise RuntimeError("LEARNING_POLICY_THRESHOLDS_MUST_BE_POSITIVE")
        authoritative_samples = self.counterfactual_learning_samples(
            evaluation_date=evaluation_date
        )
        authoritative_metrics = counterfactual_learning_metrics(
            authoritative_samples,
            minimum_mature_samples=min(minimum_values.values()),
            minimum_mature_samples_by_horizon=minimum_values,
        )
        if samples is not None and _hash([
            dict(item) for item in samples
        ]) != _hash(authoritative_samples):
            raise RuntimeError("LEARNING_CALLER_SAMPLES_NOT_AUTHORITATIVE")
        if metrics is not None and _hash(dict(metrics)) != _hash(
            authoritative_metrics
        ):
            raise RuntimeError("LEARNING_CALLER_METRICS_NOT_AUTHORITATIVE")
        sample_rows = sorted(
            (dict(item) for item in authoritative_samples),
            key=lambda item: str(item.get("forecast_id") or ""),
        )
        metrics = authoritative_metrics
        with self.engine.connect() as connection:
            for sample in sample_rows:
                if str(sample.get("evidence_source") or "") != (
                    "HORIZON_CONTRACT_OUTCOME_LEDGER"
                ):
                    raise RuntimeError("LEARNING_SAMPLE_SOURCE_NOT_VERIFIED")
                outcome_id = str(sample.get("outcome_id") or "")
                ledger = connection.execute(
                    text(
                        """
                        SELECT contract_id, market_evidence_hash, outcome_hash
                        FROM st_horizon_outcome_v3
                        WHERE outcome_id = :outcome_id
                          AND outcome_status = 'MATURED_VERIFIED'
                        """
                    ),
                    {"outcome_id": outcome_id},
                ).mappings().first()
                if (
                    ledger is None
                    or str(ledger["contract_id"])
                    != str(sample.get("contract_id") or "")
                    or str(ledger["market_evidence_hash"])
                    != str(sample.get("market_evidence_hash") or "")
                    or str(ledger["outcome_hash"])
                    != str(sample.get("outcome_hash") or "")
                ):
                    raise RuntimeError("LEARNING_SAMPLE_LEDGER_MISMATCH")
        evidence_hash = _hash(sample_rows)
        policy_hash = _hash(dict(policy))
        learning_identity = {
            "evaluation_date": evaluation_date.isoformat(),
            "evidence_hash": evidence_hash,
            "policy_hash": policy_hash,
            "config_hash": runtime["config_hash"],
            "code_version": runtime["code_version"],
            "model_artifact_hashes": runtime["model_artifact_hashes"],
        }
        learning_id = _hash(learning_identity)
        metrics_payload = dict(metrics)
        metrics_payload["provenance"] = {
            "learning_run_id": learning_id,
            "learning_evidence_hash": evidence_hash,
            "learning_policy_hash": policy_hash,
            "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
            "config_hash": runtime["config_hash"],
            "code_version": runtime["code_version"],
            "code_version_kind": runtime["code_version_kind"],
            "model_artifact_hashes": runtime["model_artifact_hashes"],
        }
        provenance_hash = _hash(metrics_payload["provenance"])
        result_hash = _hash(metrics_payload)
        overall = dict(metrics_payload.get("overall") or {})
        quadrants = dict(overall.get("quadrant_counts") or {})
        readiness = dict(metrics_payload.get("horizon_readiness") or {})
        horizon_counts = {
            horizon: sum(
                1 for sample in sample_rows
                if int(sample.get("horizon_days") or 0) == horizon
            )
            for horizon in (1, 5, 20)
        }
        if int(overall.get("sample_count") or 0) != len(sample_rows):
            raise RuntimeError("LEARNING_METRICS_SAMPLE_COUNT_MISMATCH")
        horizon_ready = {
            horizon: bool(dict(readiness.get(f"T+{horizon}") or {}).get("ready"))
            for horizon in (1, 5, 20)
        }
        learning_status = str(metrics_payload.get("status") or "COLLECTING")
        if learning_status == "EVIDENCE_READY" and not all(
            horizon_ready.values()
        ):
            raise RuntimeError("LEARNING_EVIDENCE_READY_REQUIRES_ALL_HORIZONS")
        evaluated = _utc_naive(evaluated_at)
        values = {
            "learning_run_id": learning_id,
            "evaluation_date": evaluation_date,
            "learning_status": learning_status,
            "sample_count": int(overall.get("sample_count") or 0),
            "selected_win_count": int(quadrants.get("SELECTED_WIN") or 0),
            "selected_loss_count": int(quadrants.get("SELECTED_LOSS") or 0),
            "rejected_win_count": int(quadrants.get("REJECTED_WIN") or 0),
            "rejected_correct_count": int(
                quadrants.get("REJECTED_CORRECT") or 0
            ),
            "selection_precision": overall.get("selection_precision"),
            "winner_recall": overall.get("winner_recall"),
            "mean_absolute_forecast_error_pct": overall.get(
                "mean_absolute_forecast_error_pct"
            ),
            "mean_brier_score": overall.get("mean_brier_score"),
            "total_opportunity_cost_pct": float(
                overall.get("total_opportunity_cost_pct") or 0.0
            ),
            "t1_sample_count": horizon_counts[1],
            "t5_sample_count": horizon_counts[5],
            "t20_sample_count": horizon_counts[20],
            "t1_evidence_ready": int(horizon_ready[1]),
            "t5_evidence_ready": int(horizon_ready[5]),
            "t20_evidence_ready": int(horizon_ready[20]),
            "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
            "config_hash": runtime["config_hash"],
            "code_version": runtime["code_version"],
            "code_version_kind": runtime["code_version_kind"],
            "model_artifact_hashes_json": _json(
                runtime["model_artifact_hashes"]
            ),
            "provenance_hash": provenance_hash,
            "metrics_json": _json(metrics_payload),
            "evidence_hash": evidence_hash,
            "policy_hash": policy_hash,
            "learning_result_hash": result_hash,
            "evaluated_at": evaluated,
            "created_at": evaluated,
        }
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    {self._insert_ignore} INTO
                    st_counterfactual_learning_run_v3 (
                        learning_run_id, evaluation_date, learning_status,
                        sample_count, selected_win_count,
                        selected_loss_count, rejected_win_count,
                        rejected_correct_count, selection_precision,
                        winner_recall,
                        mean_absolute_forecast_error_pct,
                        mean_brier_score, total_opportunity_cost_pct,
                        t1_sample_count, t5_sample_count,
                        t20_sample_count, t1_evidence_ready,
                        t5_evidence_ready, t20_evidence_ready,
                        evidence_source,
                        config_hash, code_version, code_version_kind,
                        model_artifact_hashes_json, provenance_hash,
                        metrics_json, evidence_hash, policy_hash,
                        learning_result_hash, can_activate_model,
                        order_authority, evaluated_at, created_at
                    ) VALUES (
                        :learning_run_id, :evaluation_date, :learning_status,
                        :sample_count, :selected_win_count,
                        :selected_loss_count, :rejected_win_count,
                        :rejected_correct_count, :selection_precision,
                        :winner_recall,
                        :mean_absolute_forecast_error_pct,
                        :mean_brier_score, :total_opportunity_cost_pct,
                        :t1_sample_count, :t5_sample_count,
                        :t20_sample_count, :t1_evidence_ready,
                        :t5_evidence_ready, :t20_evidence_ready,
                        :evidence_source,
                        :config_hash, :code_version, :code_version_kind,
                        :model_artifact_hashes_json, :provenance_hash,
                        :metrics_json, :evidence_hash, :policy_hash,
                        :learning_result_hash, 0, 0,
                        :evaluated_at, :created_at
                    )
                    """
                ),
                values,
            )
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_counterfactual_learning_run_v3
                    WHERE learning_run_id = :learning_run_id
                    """
                ),
                {"learning_run_id": learning_id},
            ).mappings().first()
        if row is None or str(row["learning_result_hash"]) != result_hash:
            raise RuntimeError("LEARNING_RUN_IDEMPOTENCY_CONFLICT")
        return dict(row)

    def latest_learning_run(self) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_counterfactual_learning_run_v3
                    ORDER BY evaluation_date DESC, evaluated_at DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()
        return _row_dict(row)

    def learning_run(self, learning_run_id: str) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_counterfactual_learning_run_v3
                    WHERE learning_run_id = :learning_run_id
                    """
                ),
                {"learning_run_id": str(learning_run_id)},
            ).mappings().first()
        return _row_dict(row)

    def verified_learning_run(
        self, learning_run_id: str
    ) -> dict[str, Any] | None:
        row = self.learning_run(learning_run_id)
        if row is None:
            return None
        try:
            evaluation_date = _date_value(row["evaluation_date"])
            provenance_as_of = datetime.combine(
                evaluation_date,
                time(23, 59, 59, 999999),
                tzinfo=MARKET_TIMEZONE,
            ).astimezone(timezone.utc)
            runtime = self.effective_runtime_provenance(
                decision_as_of=provenance_as_of,
            )
            if runtime.get("artifact_registry_status") != "AVAILABLE":
                return None
        except (KeyError, TypeError, ValueError, RuntimeError):
            return None
        if (
            str(row.get("evidence_source") or "")
            != "HORIZON_CONTRACT_OUTCOME_LEDGER"
            or int(row.get("order_authority") or 0) != 0
            or int(row.get("can_activate_model") or 0) != 0
            or str(row.get("config_hash") or "") != runtime["config_hash"]
            or str(row.get("code_version") or "") != runtime["code_version"]
            or str(row.get("policy_hash") or "")
            != _hash(runtime["continuous_policy"])
        ):
            return None
        try:
            metrics = json.loads(str(row.get("metrics_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if _hash(metrics) != str(row.get("learning_result_hash") or ""):
            return None
        try:
            minimum_by_horizon = {
                str(key): int(value)
                for key, value in dict(
                    runtime["continuous_policy"].get(
                        "minimum_mature_samples"
                    ) or {}
                ).items()
            }
            from .learning_intelligence import counterfactual_learning_metrics

            authoritative_samples = sorted(
                self.counterfactual_learning_samples(
                    evaluation_date=evaluation_date
                ),
                key=lambda item: str(item.get("forecast_id") or ""),
            )
            authoritative_metrics = counterfactual_learning_metrics(
                authoritative_samples,
                minimum_mature_samples=min(minimum_by_horizon.values()),
                minimum_mature_samples_by_horizon=minimum_by_horizon,
            )
        except (KeyError, TypeError, ValueError, RuntimeError):
            return None
        authoritative_evidence_hash = _hash(authoritative_samples)
        authoritative_policy_hash = _hash(runtime["continuous_policy"])
        learning_identity = {
            "evaluation_date": evaluation_date.isoformat(),
            "evidence_hash": authoritative_evidence_hash,
            "policy_hash": authoritative_policy_hash,
            "config_hash": runtime["config_hash"],
            "code_version": runtime["code_version"],
            "model_artifact_hashes": runtime["model_artifact_hashes"],
        }
        expected_learning_id = _hash(learning_identity)
        expected_metrics = dict(authoritative_metrics)
        expected_metrics["provenance"] = {
            "learning_run_id": expected_learning_id,
            "learning_evidence_hash": authoritative_evidence_hash,
            "learning_policy_hash": authoritative_policy_hash,
            "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
            "config_hash": runtime["config_hash"],
            "code_version": runtime["code_version"],
            "code_version_kind": runtime["code_version_kind"],
            "model_artifact_hashes": runtime["model_artifact_hashes"],
        }
        if (
            str(row["learning_run_id"]) != expected_learning_id
            or str(row["evidence_hash"]) != authoritative_evidence_hash
            or str(row["learning_result_hash"]) != _hash(expected_metrics)
            or metrics != expected_metrics
        ):
            return None
        overall = dict(authoritative_metrics.get("overall") or {})
        quadrants = dict(overall.get("quadrant_counts") or {})
        readiness = dict(
            authoritative_metrics.get("horizon_readiness") or {}
        )
        horizon_counts = {
            horizon: sum(
                1
                for sample in authoritative_samples
                if int(sample.get("horizon_days") or 0) == horizon
            )
            for horizon in (1, 5, 20)
        }
        expected_integer_projection = {
            "sample_count": len(authoritative_samples),
            "selected_win_count": int(
                quadrants.get("SELECTED_WIN") or 0
            ),
            "selected_loss_count": int(
                quadrants.get("SELECTED_LOSS") or 0
            ),
            "rejected_win_count": int(
                quadrants.get("REJECTED_WIN") or 0
            ),
            "rejected_correct_count": int(
                quadrants.get("REJECTED_CORRECT") or 0
            ),
            "t1_sample_count": horizon_counts[1],
            "t5_sample_count": horizon_counts[5],
            "t20_sample_count": horizon_counts[20],
            "t1_evidence_ready": int(bool(
                dict(readiness.get("T+1") or {}).get("ready")
            )),
            "t5_evidence_ready": int(bool(
                dict(readiness.get("T+5") or {}).get("ready")
            )),
            "t20_evidence_ready": int(bool(
                dict(readiness.get("T+20") or {}).get("ready")
            )),
        }
        try:
            if any(
                int(row.get(field)) != expected
                for field, expected in expected_integer_projection.items()
            ):
                return None
        except (TypeError, ValueError):
            return None
        if str(row.get("learning_status") or "") != str(
            authoritative_metrics.get("status") or "COLLECTING"
        ):
            return None
        expected_decimal_projection = {
            "selection_precision": overall.get("selection_precision"),
            "winner_recall": overall.get("winner_recall"),
            "mean_absolute_forecast_error_pct": overall.get(
                "mean_absolute_forecast_error_pct"
            ),
            "mean_brier_score": overall.get("mean_brier_score"),
            "total_opportunity_cost_pct": float(
                overall.get("total_opportunity_cost_pct") or 0.0
            ),
        }
        if any(
            not _decimal_projection_matches(row.get(field), expected)
            for field, expected in expected_decimal_projection.items()
        ):
            return None
        provenance = dict(metrics.get("provenance") or {})
        try:
            artifacts = json.loads(
                str(row.get("model_artifact_hashes_json") or "{}")
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if (
            str(provenance.get("learning_run_id") or "")
            != str(row["learning_run_id"])
            or str(provenance.get("learning_evidence_hash") or "")
            != str(row["evidence_hash"])
            or str(provenance.get("learning_policy_hash") or "")
            != str(row["policy_hash"])
            or str(provenance.get("evidence_source") or "")
            != "HORIZON_CONTRACT_OUTCOME_LEDGER"
            or artifacts != runtime["model_artifact_hashes"]
            or dict(provenance.get("model_artifact_hashes") or {})
            != runtime["model_artifact_hashes"]
            or _hash(provenance) != str(row.get("provenance_hash") or "")
        ):
            return None
        evaluated_at = row.get("evaluated_at")
        if isinstance(evaluated_at, str):
            evaluated_at = datetime.fromisoformat(evaluated_at)
        if not isinstance(evaluated_at, datetime):
            return None
        if evaluated_at.tzinfo is None:
            evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        maximum_age = float(
            runtime["continuous_policy"].get("maximum_evidence_age_days", 0)
        )
        age_days = (now - evaluated_at.astimezone(timezone.utc)).total_seconds() / 86400
        if age_days < 0 or age_days > maximum_age:
            return None
        return {**row, "metrics": metrics}

    def verified_calibration_evidence(
        self,
        *,
        release_id: str,
        model_key: str,
        model_version: str,
        horizon_days: int,
    ) -> dict[str, Any] | None:
        row = self.latest_learning_run()
        if row is None:
            return None
        verified = self.verified_learning_run(str(row["learning_run_id"]))
        if verified is None or str(verified["learning_status"]) != "EVIDENCE_READY":
            return None
        if not bool(verified.get(f"t{int(horizon_days)}_evidence_ready")):
            return None
        metrics = dict(verified["metrics"])
        collection = metrics.get("continuous_calibration_evidence")
        if not isinstance(collection, Mapping):
            return None
        raw_payload = collection.get(release_id)
        if not isinstance(raw_payload, Mapping):
            return None
        payload = dict(raw_payload)
        if (
            str(payload.get("release_id") or "") != str(release_id)
            or str(payload.get("model_key") or "") != str(model_key)
            or str(payload.get("model_version") or "") != str(model_version)
            or int(payload.get("horizon_days") or 0) != int(horizon_days)
        ):
            return None
        return {
            **payload,
            "learning_run_id": str(verified["learning_run_id"]),
            "learning_result_hash": str(verified["learning_result_hash"]),
            "learning_evidence_hash": str(verified["evidence_hash"]),
            "learning_policy_hash": str(verified["policy_hash"]),
            "evidence_provenance_status": "PERSISTED_VERIFIED",
        }

    def _requested_as_of_sql(self, alias: str) -> str:
        if str(self.engine.dialect.name).casefold() == "sqlite":
            return (
                f"COALESCE({alias}.requested_as_of, "
                f"json_extract({alias}.portfolio_json, "
                "'$.decision_snapshot.requested_as_of'), "
                f"date({alias}.decision_at))"
            )
        return (
            f"COALESCE({alias}.requested_as_of, "
            "STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT("
            f"{alias}.portfolio_json, "
            "'$.decision_snapshot.requested_as_of')), '%Y-%m-%d'), "
            f"DATE({alias}.decision_at))"
        )

    def latest_forecast_rows(self, *, limit: int = 50000) -> list[dict[str, Any]]:
        requested = self._requested_as_of_sql("r")
        requested_subquery = self._requested_as_of_sql("r2")
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT f.forecast_id, f.run_uid, f.stock_code,
                           f.short_name, f.strategy_key, f.horizon_days,
                           f.raw_score, f.feature_time, f.valid_until,
                           f.forecast_status, f.model_version AS source_model_version,
                           f.dataset_hash, f.features_json, f.reasons_json,
                           r.trade_date,
                           {requested} AS requested_as_of,
                           r.decision_at, r.result_hash AS decision_result_hash,
                           r.data_snapshot_hash, r.config_hash,
                           t.target_id, t.strategy_keys_json,
                           t.attribution_snapshot_hash,
                           t.status AS target_status
                    FROM st_alpha_forecast_v3 f
                    JOIN st_decision_run_v3 r ON r.run_uid = f.run_uid
                    LEFT JOIN st_target_portfolio_v3 t
                      ON t.run_uid = f.run_uid
                     AND t.stock_code = f.stock_code
                    WHERE r.status = 'COMPLETED'
                      AND f.raw_score IS NOT NULL
                      AND f.forecast_status NOT IN (
                          'DATA_BLOCKED', 'FEATURE_QUALITY_BLOCKED'
                      )
                      AND r.run_uid = (
                          SELECT r2.run_uid
                          FROM st_decision_run_v3 r2
                          WHERE r2.status = 'COMPLETED'
                          ORDER BY {requested_subquery} DESC,
                                   r2.decision_at DESC, r2.run_uid DESC
                          LIMIT 1
                      )
                    ORDER BY f.stock_code, f.raw_score DESC, f.strategy_key
                    LIMIT :limit
                    """
                ),
                {"limit": max(1, min(int(limit), 100000))},
            ).mappings().all()
        result = []
        for raw in rows:
            row = dict(raw)
            try:
                features = json.loads(str(row.pop("features_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                features = None
            try:
                strategy_keys = tuple(sorted({
                    str(item)
                    for item in json.loads(
                        str(row.pop("strategy_keys_json", None) or "[]")
                    )
                    if str(item)
                }))
            except (TypeError, ValueError, json.JSONDecodeError):
                strategy_keys = ()
            selected = bool(
                row.get("target_id") is not None
                and str(row["strategy_key"]) in strategy_keys
            )
            row["features"] = features
            row["selection_status"] = (
                "SELECTED" if selected else "REJECTED"
            )
            row["selection_reason_code"] = (
                "TARGET_PORTFOLIO_SELECTED"
                if selected
                else "NOT_SELECTED_IN_FROZEN_TARGET"
            )
            row["selection_snapshot"] = {
                "target_id": row.pop("target_id", None),
                "target_status": row.pop("target_status", None),
                "target_strategy_keys": list(strategy_keys),
                "attribution_snapshot_hash": row.pop(
                    "attribution_snapshot_hash", None
                ),
                "selection_status": row["selection_status"],
                "selection_reason_code": row["selection_reason_code"],
            }
            result.append(row)
        return result

    def counterfactual_learning_samples(
        self,
        *,
        evaluation_date: date | None = None,
    ) -> list[dict[str, Any]]:
        if evaluation_date is not None and (
            isinstance(evaluation_date, datetime)
            or not isinstance(evaluation_date, date)
        ):
            raise ValueError("evaluation_date must be a date")
        provenance_as_of = (
            datetime.combine(
                evaluation_date,
                time(23, 59, 59, 999999),
                tzinfo=MARKET_TIMEZONE,
            ).astimezone(timezone.utc)
            if evaluation_date is not None
            else datetime.now(timezone.utc)
        )
        runtime = self.effective_runtime_provenance(
            decision_as_of=provenance_as_of,
        )
        artifact_release_ids = {
            str(artifact_hash): str(release_id)
            for release_id, artifact_hash in dict(
                runtime["model_artifact_hashes"]
            ).items()
        }
        cutoff_clause = (
            "AND o.exit_trade_date <= :evaluation_date"
            if evaluation_date is not None
            else ""
        )
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT h.contract_id AS forecast_id,
                           h.contract_id, o.outcome_id, o.outcome_hash,
                           h.run_uid, h.stock_code, h.model_key,
                           h.model_version, h.horizon_days,
                           h.model_artifact_hash,
                           h.prediction_kind,
                           h.source_strategy_key,
                           h.selection_status,
                           h.selection_reason_code AS reason_code,
                           h.source_forecast_hash,
                           h.selection_evidence_hash,
                           o.realized_net_return_pct,
                           o.realized_mae_pct, o.realized_mfe_pct,
                           o.realized_cost_pct, o.market_evidence_hash,
                           o.execution_feasibility,
                           h.expected_return_net_pct,
                           h.probability_positive
                    FROM st_horizon_forecast_contract_v3 h
                    JOIN st_horizon_outcome_v3 o
                      ON o.contract_id = h.contract_id
                     AND o.outcome_status = 'MATURED_VERIFIED'
                    WHERE 1 = 1
                      {cutoff_clause}
                    ORDER BY h.contract_id
                    """
                ),
                (
                    {"evaluation_date": evaluation_date}
                    if evaluation_date is not None
                    else {}
                ),
            ).mappings().all()
        samples = []
        for row in rows:
            item = dict(row)
            artifact_hash = str(item.get("model_artifact_hash") or "")
            release_id = artifact_release_ids.get(artifact_hash)
            if not release_id:
                # A new scorer/model artifact starts a new OOS evidence cohort;
                # historical versions remain queryable in the ledgers but may
                # never satisfy the current continuous-calibration gate.
                continue
            selected = str(item["selection_status"]) == "SELECTED"
            realized = float(item["realized_net_return_pct"] or 0.0)
            won = realized > 0
            if selected and won:
                quadrant = "SELECTED_WIN"
            elif selected:
                quadrant = "SELECTED_LOSS"
            elif won:
                quadrant = "REJECTED_WIN"
            else:
                quadrant = "REJECTED_CORRECT"
            probability = item.get("probability_positive")
            expected = item.get("expected_return_net_pct")
            samples.append({
                **item,
                "release_id": release_id,
                "selected": selected,
                "accepted": selected,
                "quadrant": quadrant,
                "opportunity_cost_pct": (
                    max(0.0, realized) if not selected else max(0.0, -realized)
                ),
                "absolute_forecast_error_pct": (
                    abs(realized - float(expected))
                    if expected is not None
                    else None
                ),
                "brier_score": (
                    (float(probability) - (1.0 if won else 0.0)) ** 2
                    if probability is not None
                    else None
                ),
                "calibration_eligible": (
                    str(item["prediction_kind"]) == "CALIBRATED_OOS"
                    and str(item["execution_feasibility"])
                    == "EXECUTABLE_VERIFIED"
                ),
                "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
                "can_activate_model": False,
                "order_authority": False,
            })
        return samples

    def calibration_outcome_checkpoint(
        self,
        *,
        evaluation_date: date,
    ) -> dict[str, Any]:
        """Build a stable incremental cursor from immutable outcome rows.

        The checkpoint deliberately counts distinct decision sessions in
        addition to outcome rows.  A large cross-section from one or two days
        must not masquerade as longitudinal forward evidence for retraining.
        """

        if isinstance(evaluation_date, datetime) or not isinstance(
            evaluation_date, date
        ):
            raise ValueError("evaluation_date must be a date")
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT o.outcome_id, o.outcome_hash, o.contract_id,
                           o.market_evidence_hash, o.market_evidence_json,
                           o.exit_trade_date,
                           o.execution_feasibility, o.observed_at,
                           h.contract_hash, h.run_uid, h.model_key,
                           h.model_version, h.model_artifact_hash,
                           h.horizon_days, h.decision_session_date,
                           h.prediction_kind
                    FROM st_horizon_outcome_v3 o
                    JOIN st_horizon_forecast_contract_v3 h
                      ON h.contract_id = o.contract_id
                    WHERE o.outcome_status = 'MATURED_VERIFIED'
                      AND o.exit_trade_date <= :evaluation_date
                    ORDER BY h.horizon_days, h.decision_session_date,
                             o.outcome_id
                    """
                ),
                {"evaluation_date": evaluation_date},
            ).mappings().all()
        from .shadow_intelligence_worker import _trainable_model_specs

        trainable_specs = _trainable_model_specs(load_v3_config())
        trainable_identities = {
            (
                str(spec["model_key"]),
                str(spec["model_version"]),
                int(horizon),
            )
            for horizon, spec in trainable_specs.items()
        }
        registered_artifact_hashes = sorted({
            str(row.get("model_artifact_hash") or "")
            for row in rows
            if (
                str(row.get("model_key") or ""),
                str(row.get("model_version") or ""),
                int(row.get("horizon_days") or 0),
            )
            in trainable_identities
            and str(row.get("model_artifact_hash") or "")
        })
        artifact_bindings: dict[str, dict[str, Any]] = {}
        if registered_artifact_hashes:
            artifact_statement = text(
                """
                SELECT * FROM st_horizon_model_artifact_v3
                WHERE artifact_id IN :artifact_ids
                """
            ).bindparams(bindparam("artifact_ids", expanding=True))
            with self.engine.connect() as connection:
                artifact_rows = connection.execute(
                    artifact_statement,
                    {"artifact_ids": tuple(registered_artifact_hashes)},
                ).mappings().all()
            for raw_artifact in artifact_rows:
                artifact_row = dict(raw_artifact)
                artifact_id = str(artifact_row.get("artifact_id") or "")
                try:
                    artifact, _ = _artifact_row_document(
                        artifact_row,
                        require_current_code=False,
                        require_current_config=False,
                    )
                    training_window = dict(
                        artifact.get("training_window") or {}
                    )
                    gate_status = str(
                        dict(artifact.get("gate") or {}).get("status") or ""
                    )
                    expected_registry_status = (
                        "OOS_VERIFIED"
                        if gate_status == "PASS"
                        else "BLOCKED"
                    )
                    binding_verified = (
                        artifact_id
                        == str(artifact.get("artifact_hash") or "")
                        and str(artifact_row.get("artifact_status") or "")
                        == expected_registry_status
                        and str(
                            artifact_row.get("training_receipt_status") or ""
                        )
                        == "PROCESS_VERIFIED"
                        and str(
                            artifact_row.get("artifact_schema_version") or ""
                        )
                        == CURRENT_HORIZON_ARTIFACT_SCHEMA
                        and str(artifact_row.get("model_protocol") or "")
                        == CURRENT_HORIZON_MODEL_PROTOCOL
                        and str(
                            artifact_row.get("candidate_ledger_schema_version")
                            or ""
                        )
                        == CANDIDATE_EVALUATION_LEDGER_SCHEMA
                        and bool(
                            artifact_row.get(
                                "ledger_registration_evidence_hash"
                            )
                        )
                        and bool(
                            artifact_row.get("registration_verification_hash")
                        )
                        and str(artifact.get("config_hash") or "")
                        == current_config_hash()
                        and str(artifact.get("code_version") or "")
                        == code_version()[0]
                        and str(training_window.get("status") or "")
                        == "FROZEN_DEFAULT_TRAINING_WINDOW"
                        and training_window.get("is_current_config_default")
                        is True
                    )
                    artifact_bindings[artifact_id] = {
                        "protocol": FORWARD_SHADOW_BINDING_PROTOCOL,
                        "binding_verified": binding_verified,
                        "suite_release_id": str(
                            artifact.get("suite_release_id") or ""
                        ),
                        "release_id": str(artifact.get("release_id") or ""),
                        "model_key": str(artifact.get("model_key") or ""),
                        "model_version": str(
                            artifact.get("model_version") or ""
                        ),
                        "horizon_days": int(
                            artifact.get("horizon_days") or 0
                        ),
                        "artifact_status": str(
                            artifact_row.get("artifact_status") or ""
                        ),
                        "artifact_schema_version": str(
                            artifact.get("schema_version") or ""
                        ),
                        "model_protocol": str(
                            artifact.get("model_protocol") or ""
                        ),
                        "training_window_protocol": str(
                            training_window.get("protocol") or ""
                        ),
                        "training_window_hash": str(
                            training_window.get("training_window_hash") or ""
                        ),
                        "training_window_status": str(
                            training_window.get("status") or ""
                        ),
                        "configured_history_start": str(
                            training_window.get("configured_history_start")
                            or ""
                        ),
                        "signal_start": str(
                            training_window.get("signal_start") or ""
                        ),
                        "gate_status": gate_status,
                        "promotion_eligible": False,
                        "order_authority": False,
                    }
                except (KeyError, TypeError, ValueError, RuntimeError):
                    artifact_bindings[artifact_id] = {
                        "protocol": FORWARD_SHADOW_BINDING_PROTOCOL,
                        "binding_verified": False,
                        "promotion_eligible": False,
                        "order_authority": False,
                    }

        projected = []
        for raw in rows:
            row = dict(raw)
            raw_market_evidence = row.get("market_evidence_json")
            try:
                market_evidence = (
                    dict(raw_market_evidence)
                    if isinstance(raw_market_evidence, Mapping)
                    else dict(json.loads(str(raw_market_evidence or "{}")))
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                market_evidence = {}
            qmt_attestation = dict(
                market_evidence.get("qmt_attestation") or {}
            )
            qmt_attested = (
                bool(market_evidence)
                and _hash(market_evidence)
                == str(row.get("market_evidence_hash") or "")
                and str(qmt_attestation.get("protocol") or "")
                == QMT_OUTCOME_ATTESTATION_PROTOCOL
                and str(qmt_attestation.get("status") or "")
                == "QMT_ATTESTED"
                and str(qmt_attestation.get("provider") or "")
                == "gj_big_qmt_inner"
                and int(qmt_attestation.get("attested_bar_count") or 0)
                == int(row["horizon_days"]) + 1
            )
            artifact_hash = str(row["model_artifact_hash"])
            binding = dict(artifact_bindings.get(artifact_hash) or {})
            prediction_kind = str(row["prediction_kind"])
            binding_gate_status = str(binding.get("gate_status") or "")
            binding_registry_status = str(
                binding.get("artifact_status") or ""
            )
            prediction_semantics_verified = (
                prediction_kind == "CALIBRATED_OOS"
                and binding_gate_status == "PASS"
                and binding_registry_status == "OOS_VERIFIED"
            ) or (
                prediction_kind == "PROXY_SCORE"
                and binding_gate_status == "BLOCK"
                and binding_registry_status == "BLOCKED"
            )
            binding_verified = (
                binding.get("binding_verified") is True
                and prediction_semantics_verified
                and str(binding.get("model_key") or "")
                == str(row["model_key"])
                and str(binding.get("model_version") or "")
                == str(row["model_version"])
                and int(binding.get("horizon_days") or 0)
                == int(row["horizon_days"])
            )
            projected.append({
                "outcome_id": str(row["outcome_id"]),
                "outcome_hash": str(row["outcome_hash"]),
                "contract_id": str(row["contract_id"]),
                "contract_hash": str(row["contract_hash"]),
                "market_evidence_hash": str(row["market_evidence_hash"]),
                "run_uid": str(row["run_uid"]),
                "model_key": str(row["model_key"]),
                "model_version": str(row["model_version"]),
                "model_artifact_hash": str(row["model_artifact_hash"]),
                "horizon_days": int(row["horizon_days"]),
                "decision_session_date": _date_value(
                    row["decision_session_date"]
                ).isoformat(),
                "exit_trade_date": _date_value(
                    row["exit_trade_date"]
                ).isoformat(),
                "prediction_kind": prediction_kind,
                "forward_shadow_binding": binding,
                "artifact_binding_verified": binding_verified,
                "qmt_attestation_status": (
                    "QMT_ATTESTED" if qmt_attested else "UNVERIFIED"
                ),
                "qmt_attestation_hash": str(
                    qmt_attestation.get("attestation_hash") or ""
                ),
                "forward_evidence_eligible": bool(
                    qmt_attested and binding_verified
                ),
                "execution_feasibility": str(
                    row["execution_feasibility"]
                ),
                "observed_at": _datetime_utc(
                    row["observed_at"]
                ).isoformat(),
            })
        by_horizon: dict[str, Any] = {}
        for horizon in (1, 5, 20):
            cohort = [
                item for item in projected
                if int(item["horizon_days"]) == horizon
            ]
            eligible_cohort = [
                item for item in cohort
                if item["forward_evidence_eligible"] is True
            ]
            sessions = sorted({
                str(item["decision_session_date"]) for item in cohort
            })
            eligible_sessions = sorted({
                str(item["decision_session_date"])
                for item in eligible_cohort
            })
            executable = sum(
                1 for item in cohort
                if str(item["execution_feasibility"])
                == "EXECUTABLE_VERIFIED"
            )
            by_horizon[str(horizon)] = {
                "horizon_days": horizon,
                "sample_count": len(cohort),
                "distinct_decision_session_count": len(sessions),
                "forward_eligible_sample_count": len(eligible_cohort),
                "forward_eligible_decision_session_count": len(
                    eligible_sessions
                ),
                "qmt_attested_count": sum(
                    1 for item in cohort
                    if item["qmt_attestation_status"] == "QMT_ATTESTED"
                ),
                "artifact_binding_verified_count": sum(
                    1 for item in cohort
                    if item["artifact_binding_verified"] is True
                ),
                "first_decision_session_date": sessions[0] if sessions else None,
                "latest_decision_session_date": sessions[-1] if sessions else None,
                "latest_exit_trade_date": max(
                    (str(item["exit_trade_date"]) for item in cohort),
                    default=None,
                ),
                "executable_verified_count": executable,
                "unverified_research_count": len(cohort) - executable,
                "evidence_hash": _hash(cohort),
                "forward_evidence_hash": _hash(eligible_cohort),
            }
        by_artifact_hash: dict[str, Any] = {}
        artifact_hashes = sorted({
            str(item["model_artifact_hash"])
            for item in projected
            if str(item.get("model_artifact_hash") or "")
        })
        for artifact_hash in artifact_hashes:
            cohort = [
                item for item in projected
                if str(item["model_artifact_hash"]) == artifact_hash
            ]
            eligible_cohort = [
                item for item in cohort
                if item["forward_evidence_eligible"] is True
            ]
            horizons = {int(item["horizon_days"]) for item in cohort}
            if len(horizons) != 1:
                raise RuntimeError(
                    "CALIBRATION_ARTIFACT_HORIZON_IDENTITY_CONFLICT"
                )
            horizon = next(iter(horizons))
            sessions = sorted({
                str(item["decision_session_date"]) for item in cohort
            })
            eligible_sessions = sorted({
                str(item["decision_session_date"])
                for item in eligible_cohort
            })
            executable = sum(
                1 for item in cohort
                if str(item["execution_feasibility"])
                == "EXECUTABLE_VERIFIED"
            )
            by_artifact_hash[artifact_hash] = {
                "model_artifact_hash": artifact_hash,
                "horizon_days": horizon,
                "sample_count": len(cohort),
                "distinct_decision_session_count": len(sessions),
                "forward_eligible_sample_count": len(eligible_cohort),
                "forward_eligible_decision_session_count": len(
                    eligible_sessions
                ),
                "qmt_attested_count": sum(
                    1 for item in cohort
                    if item["qmt_attestation_status"] == "QMT_ATTESTED"
                ),
                "artifact_binding_verified_count": sum(
                    1 for item in cohort
                    if item["artifact_binding_verified"] is True
                ),
                "forward_shadow_binding": dict(
                    artifact_bindings.get(artifact_hash) or {}
                ),
                "first_decision_session_date": (
                    sessions[0] if sessions else None
                ),
                "latest_decision_session_date": (
                    sessions[-1] if sessions else None
                ),
                "latest_exit_trade_date": max(
                    (str(item["exit_trade_date"]) for item in cohort),
                    default=None,
                ),
                "executable_verified_count": executable,
                "unverified_research_count": len(cohort) - executable,
                "evidence_hash": _hash(cohort),
                "forward_evidence_hash": _hash(eligible_cohort),
            }
        return {
            "schema_version": (
                "probiga.trading-v3.calibration-outcome-checkpoint.v1"
            ),
            "evaluation_date": evaluation_date.isoformat(),
            "sample_count": len(projected),
            "distinct_decision_session_count": len({
                str(item["decision_session_date"]) for item in projected
            }),
            "forward_eligible_sample_count": sum(
                1 for item in projected
                if item["forward_evidence_eligible"] is True
            ),
            "qmt_attested_sample_count": sum(
                1 for item in projected
                if item["qmt_attestation_status"] == "QMT_ATTESTED"
            ),
            "by_horizon": by_horizon,
            # A new artifact starts a new temporal evidence cohort.  The
            # aggregate horizon counters remain useful for monitoring, but may
            # never authorize refresh or promotion of a different artifact.
            "by_artifact_hash": by_artifact_hash,
            "evidence_hash": _hash(projected),
            "execution_feasibility": (
                "EXECUTABLE_VERIFIED"
                if projected and all(
                    str(item["execution_feasibility"])
                    == "EXECUTABLE_VERIFIED"
                    for item in projected
                )
                else "UNVERIFIED_RESEARCH"
            ),
            "order_authority": False,
        }

    def calibration_evidence_payload(
        self,
        *,
        model_key: str,
        model_version: str,
    ) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT metrics_json, created_at, activated_at
                    FROM st_model_registry_v3
                    WHERE strategy_key = :model_key
                      AND model_version = :model_version
                    ORDER BY activated_at DESC, created_at DESC
                    LIMIT 1
                    """
                ),
                {"model_key": model_key, "model_version": model_version},
            ).mappings().first()
        if row is None:
            return None
        try:
            metrics = json.loads(str(row["metrics_json"] or "{}"))
        except (TypeError, ValueError):
            return None
        payload = dict(metrics.get("continuous_calibration") or {})
        payload.setdefault("observed_at", row["activated_at"] or row["created_at"])
        payload["evidence_provenance_status"] = "UNVERIFIED_PREVIEW"
        return payload

    def release_audit(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        runtime = self.effective_runtime_provenance(
            decision_as_of=now,
        )
        if runtime.get("artifact_registry_status") != "AVAILABLE":
            raise RuntimeError("SHADOW_ARTIFACT_REGISTRY_UNAVAILABLE")
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT r.*
                    FROM st_shadow_release_v3 r
                    WHERE NOT EXISTS (
                        SELECT 1 FROM st_shadow_release_v3 newer
                        WHERE newer.release_id = r.release_id
                          AND newer.transition_sequence > r.transition_sequence
                    )
                    ORDER BY r.horizon_days, r.model_key
                    """
                )
            ).mappings().all()
        releases = []
        for raw in rows:
            release = dict(raw)
            blockers: list[str] = []
            gate = self.latest_calibration_gate(str(release["release_id"]))
            if str(release.get("config_hash") or "") != runtime["config_hash"]:
                blockers.append("RELEASE_CONFIG_STALE")
            if gate is None:
                blockers.append("CALIBRATION_GATE_MISSING")
            else:
                if str(gate.get("config_hash") or "") != runtime["config_hash"]:
                    blockers.append("GATE_CONFIG_STALE")
                if str(gate.get("code_version") or "") != runtime["code_version"]:
                    blockers.append("GATE_CODE_VERSION_STALE")
                if str(gate.get("policy_hash") or "") != _hash(
                    runtime["continuous_policy"]
                ):
                    blockers.append("GATE_POLICY_STALE")
                if str(gate.get("model_artifact_hash") or "") != (
                    runtime["model_artifact_hashes"].get(
                        str(release["release_id"])
                    )
                ):
                    blockers.append("GATE_MODEL_ARTIFACT_STALE")
                valid_until = gate.get("evidence_valid_until")
                if valid_until is None or _datetime_utc(valid_until) < now:
                    blockers.append("GATE_EVIDENCE_EXPIRED")
                learning_id = str(gate.get("learning_run_id") or "")
                if (
                    str(gate.get("gate_status") or "") != "PASS"
                    or str(gate.get("evidence_provenance_status") or "")
                    != "PERSISTED_VERIFIED"
                    or not learning_id
                    or self.verified_learning_run(learning_id) is None
                ):
                    blockers.append("LATEST_PERSISTED_PASS_GATE_REQUIRED")
            audit_stage = str(release["current_stage"])
            effective_stage = audit_stage
            if audit_stage == "PAPER_ELIGIBLE" and blockers:
                effective_stage = "BLOCKED"
            release["audit_stage"] = audit_stage
            release["effective_stage"] = effective_stage
            release["effective_blockers"] = list(dict.fromkeys(blockers))
            release["latest_gate"] = gate
            release["order_authority"] = False
            releases.append(release)
        paper_eligible_count = sum(
            1 for item in releases
            if item["effective_stage"] == "PAPER_ELIGIBLE"
        )
        return {
            "schema_version": "probiga.trading-v3.shadow-release-audit.v1",
            "status": (
                "READY"
                if releases and not any(
                    item["effective_blockers"] for item in releases
                )
                else ("STALE_OR_BLOCKED" if releases else "COLLECTING")
            ),
            "releases": releases,
            "paper_eligible_count": paper_eligible_count,
            "config_hash": runtime["config_hash"],
            "code_version": runtime["code_version"],
            "order_authority": False,
            "automatic_promotion_allowed": False,
        }


__all__ = ["ShadowIntelligenceRepository"]
