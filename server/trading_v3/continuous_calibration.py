from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import re
import secrets
import subprocess
import sys
import threading
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from zoneinfo import ZoneInfo

from sqlalchemy.engine import Engine

from server.common.mysql_lock import mysql_named_lock

from .config import PROJECT_ROOT, config_hash, load_v3_config
from .horizon_candidate_ledger_schema import (
    CANDIDATE_EVALUATION_LEDGER_SCHEMA,
    CURRENT_HORIZON_ARTIFACT_SCHEMA,
    CURRENT_HORIZON_MODEL_PROTOCOL,
    CURRENT_HORIZON_SELECTION_POLICY_HASH,
    CURRENT_HORIZON_SELECTION_PROTOCOL,
    CURRENT_HORIZON_SUITE_SCHEMA,
    HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
    HISTORICAL_HORIZON_SUITE_SCHEMA_V2,
)
from .horizon_models import (
    TRAINING_CONFIG_PROTOCOL,
    TRAINING_WINDOW_PROTOCOL,
)
from .release_governance import (
    ReleaseStage,
    ReleaseTransition,
)
from .versioning import code_version


MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
CONTINUOUS_CYCLE_LOCK = "probiga:trading_v3:continuous_calibration"
CONTINUOUS_ORCHESTRATION_SCHEMA = (
    "probiga.trading-v3.continuous-calibration-orchestration.v1"
)
IMMUTABLE_EVIDENCE_SCHEMA = (
    "probiga.trading-v3.immutable-calibration-evidence.v1"
)
MODEL_ARTIFACT_SCHEMA = CURRENT_HORIZON_ARTIFACT_SCHEMA
PROCESS_TRAINING_RECEIPT_SCHEMA = (
    "probiga.trading-v3.process-training-receipt.v1"
)
RETRAINING_REQUEST_IDENTITY_PROTOCOL = (
    "probiga.trading-v3.retraining-request-identity.v1"
)

# This policy controls *when* an external trainer is asked to refresh a model.
# It does not weaken the release gate in strategies/trading_v3.json.  A refresh
# is requested after 10% of the gate's mature-sample threshold is newly
# available; a full retrain is requested after one full threshold of new rows.
FROZEN_ORCHESTRATION_POLICY: Mapping[str, Any] = {
    "schema_version": CONTINUOUS_ORCHESTRATION_SCHEMA,
    "incremental_refresh_fraction": 0.10,
    "full_retrain_fraction": 1.00,
    "minimum_distinct_decision_sessions": {"1": 120, "5": 160, "20": 240},
    "minimum_new_decision_sessions": {"1": 5, "5": 5, "20": 8},
    "automatic_training_start_source": (
        "CONFIG_MULTI_HORIZON_TRAINING_POLICY_HISTORY_START"
    ),
    "automatic_training_timeout_seconds": 21600,
    "artifact_root": "artifacts/trading_v3/horizon_models",
    "evidence_root": "artifacts/trading_v3/continuous_calibration",
    "artifact_maximum_age_uses_gate_policy": True,
    "external_signed_attestation_required": True,
    "persisted_pass_gate_required": True,
    "automatic_real_order_authority": False,
}

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_KIND = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_LOCAL_CYCLE_LOCK = threading.Lock()
_PROCESS_RECEIPT_LOCK = threading.Lock()
_PROCESS_RECEIPT_SECRET = secrets.token_bytes(32)
_PROCESS_RECEIPT_CAPABILITIES: dict[str, dict[str, Any]] = {}


def _configured_training_window() -> dict[str, str]:
    suite = dict(load_v3_config().get("multi_horizon_forecasts") or {})
    training = dict(suite.get("training_policy") or {})
    training_protocol = str(training.get("protocol_version") or "").strip()
    protocol = str(training.get("training_window_protocol") or "").strip()
    history_start = _date(training.get("history_start"), "history_start")
    if (
        training_protocol != TRAINING_CONFIG_PROTOCOL
        or protocol != TRAINING_WINDOW_PROTOCOL
    ):
        raise ContinuousCalibrationError(
            "CURRENT_TRAINING_WINDOW_PROTOCOL_INVALID"
        )
    return {
        "protocol": protocol,
        "history_start": history_start.isoformat(),
    }


class ContinuousCalibrationError(RuntimeError):
    """Raised when lifecycle evidence cannot be handled fail-closed."""


class ContinuousCalibrationAlreadyRunning(ContinuousCalibrationError):
    """Raised when another process owns the lifecycle advisory lock."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class _ProcessBoundTrainingReceipt:
    receipt: Mapping[str, Any]
    process_nonce: str
    capability_mac: str


def _issue_process_bound_training_receipt(
    receipt_body: Mapping[str, Any],
) -> _ProcessBoundTrainingReceipt:
    """Issue an opaque, same-process capability after the trainer exits."""

    nonce = secrets.token_hex(32)
    body = {**dict(receipt_body), "process_nonce": nonce}
    receipt = {**body, "receipt_hash": _hash(body)}
    mac_payload = {
        "process_nonce": nonce,
        "receipt_hash": receipt["receipt_hash"],
    }
    capability_mac = hmac.new(
        _PROCESS_RECEIPT_SECRET,
        _canonical_bytes(mac_payload),
        hashlib.sha256,
    ).hexdigest()
    hashes = dict(receipt.get("artifact_hashes") or {})
    with _PROCESS_RECEIPT_LOCK:
        _PROCESS_RECEIPT_CAPABILITIES[nonce] = {
            "receipt_hash": receipt["receipt_hash"],
            "artifact_hashes": hashes,
            "capability_mac": capability_mac,
            "consumed_horizons": set(),
        }
    return _ProcessBoundTrainingReceipt(
        receipt=receipt,
        process_nonce=nonce,
        capability_mac=capability_mac,
    )


def _process_bound_training_receipt_payload(
    capability: Any,
    *,
    horizon_days: int,
    artifact_hash: str,
    consume: bool,
) -> dict[str, Any]:
    """Authenticate and optionally consume one horizon of an issued suite."""

    if type(capability) is not _ProcessBoundTrainingReceipt:
        raise ContinuousCalibrationError(
            "TRAINING_RECEIPT_PROCESS_CAPABILITY_REQUIRED"
        )
    assert isinstance(capability, _ProcessBoundTrainingReceipt)
    receipt = dict(capability.receipt)
    nonce = str(receipt.get("process_nonce") or "")
    mac_payload = {
        "process_nonce": nonce,
        "receipt_hash": str(receipt.get("receipt_hash") or ""),
    }
    expected_mac = hmac.new(
        _PROCESS_RECEIPT_SECRET,
        _canonical_bytes(mac_payload),
        hashlib.sha256,
    ).hexdigest()
    if (
        nonce != capability.process_nonce
        or not hmac.compare_digest(expected_mac, capability.capability_mac)
    ):
        raise ContinuousCalibrationError(
            "TRAINING_RECEIPT_PROCESS_CAPABILITY_INVALID"
        )
    horizon_key = str(int(horizon_days))
    with _PROCESS_RECEIPT_LOCK:
        state = _PROCESS_RECEIPT_CAPABILITIES.get(nonce)
        if (
            state is None
            or not hmac.compare_digest(
                str(state.get("capability_mac") or ""),
                capability.capability_mac,
            )
            or str(state.get("receipt_hash") or "")
            != str(receipt.get("receipt_hash") or "")
            or str(dict(state.get("artifact_hashes") or {}).get(horizon_key) or "")
            != str(artifact_hash)
        ):
            raise ContinuousCalibrationError(
                "TRAINING_RECEIPT_PROCESS_CAPABILITY_INVALID"
            )
        consumed = state["consumed_horizons"]
        if horizon_key in consumed:
            raise ContinuousCalibrationError(
                "TRAINING_RECEIPT_PROCESS_CAPABILITY_REUSED"
            )
        if consume:
            consumed.add(horizon_key)
            if consumed == {"1", "5", "20"}:
                _PROCESS_RECEIPT_CAPABILITIES.pop(nonce, None)
    return json.loads(json.dumps(receipt, ensure_ascii=False))


def _digest(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if not _HEX64.fullmatch(result):
        raise ContinuousCalibrationError(f"{field} must be a sha256 digest")
    return result


def _aware(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        raw = str(value or "").strip()
        try:
            result = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError as exc:
            raise ContinuousCalibrationError(
                f"{field} must be an ISO-8601 datetime"
            ) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ContinuousCalibrationError(f"{field} must include a timezone")
    return result.astimezone(timezone.utc)


def _date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        raise ContinuousCalibrationError(f"{field} must be a date")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or ""))
    except ValueError as exc:
        raise ContinuousCalibrationError(
            f"{field} must be an ISO-8601 date"
        ) from exc


def _release_id(
    suite_release_id: str,
    model_key: str,
    model_version: str,
    horizon_days: int,
) -> str:
    from .horizon_models import horizon_governance_release_id

    try:
        return horizon_governance_release_id(
            suite_release_id=suite_release_id,
            model_key=model_key,
            model_version=model_version,
            horizon_days=horizon_days,
        )
    except ValueError as exc:
        raise ContinuousCalibrationError(
            "MODEL_ARTIFACT_RELEASE_ID_INVALID"
        ) from exc


def _artifact_valid_until(value: Any) -> datetime:
    """Interpret the artifact's date-only validity as inclusive UTC day end."""

    valid_on = _date(value, "valid_until")
    return datetime.combine(
        valid_on,
        time(23, 59, 59, 999999),
        tzinfo=timezone.utc,
    )


def default_artifact_root() -> Path:
    configured = str(
        os.getenv("PROBIGA_V3_HORIZON_ARTIFACT_ROOT") or ""
    ).strip()
    return Path(configured).resolve() if configured else (
        PROJECT_ROOT / str(FROZEN_ORCHESTRATION_POLICY["artifact_root"])
    ).resolve()


def default_evidence_root() -> Path:
    configured = str(
        os.getenv("PROBIGA_V3_CALIBRATION_EVIDENCE_ROOT") or ""
    ).strip()
    return Path(configured).resolve() if configured else (
        PROJECT_ROOT / str(FROZEN_ORCHESTRATION_POLICY["evidence_root"])
    ).resolve()


@dataclass(frozen=True, slots=True)
class StoredEvidence:
    kind: str
    evidence_hash: str
    path: str
    created: bool


class ImmutableEvidenceStore:
    """Content-addressed, exclusive-create evidence storage.

    Files are never updated.  A pre-existing digest path must contain the
    exact same bytes; a partial write or manual rewrite therefore stops the
    cycle instead of silently changing model evidence.
    """

    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root or default_evidence_root()).resolve()

    def _directory(self, kind: str) -> Path:
        normalized = str(kind or "").strip().lower()
        if not _KIND.fullmatch(normalized):
            raise ContinuousCalibrationError("invalid evidence kind")
        directory = (self.root / normalized).resolve()
        if self.root != directory and self.root not in directory.parents:
            raise ContinuousCalibrationError("evidence path escaped root")
        return directory

    def put(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        created_at: datetime,
    ) -> StoredEvidence:
        observed = _aware(created_at, "created_at")
        normalized = str(kind).strip().lower()
        body = {
            "schema_version": IMMUTABLE_EVIDENCE_SCHEMA,
            "kind": normalized,
            "created_at": observed.isoformat(),
            "payload": dict(payload),
        }
        evidence_hash = _hash(body)
        body["evidence_hash"] = evidence_hash
        raw = _canonical_bytes(body) + b"\n"
        directory = self._directory(normalized)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{evidence_hash}.json"
        created = False
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o444,
            )
        except FileExistsError:
            existing = path.read_bytes()
            if existing != raw:
                raise ContinuousCalibrationError(
                    "IMMUTABLE_EVIDENCE_CONTENT_MISMATCH"
                )
        else:
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(raw)
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    path.chmod(0o444)
                except OSError:
                    # Content addressing and byte verification remain the
                    # authority on filesystems without POSIX permission bits.
                    pass
                created = True
            except BaseException:
                # Leave a partial file visible.  The next run detects the byte
                # mismatch and fails closed rather than overwriting evidence.
                raise
        return StoredEvidence(
            kind=normalized,
            evidence_hash=evidence_hash,
            path=str(path),
            created=created,
        )

    def records(self, kind: str) -> tuple[dict[str, Any], ...]:
        directory = self._directory(kind)
        if not directory.exists():
            return ()
        rows: list[dict[str, Any]] = []
        for path in sorted(directory.glob("*.json")):
            try:
                raw = path.read_bytes()
                record = json.loads(raw)
                claimed = str(record.pop("evidence_hash", ""))
                expected = _hash(record)
                record["evidence_hash"] = claimed
            except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ContinuousCalibrationError(
                    f"IMMUTABLE_EVIDENCE_INVALID:{path.name}"
                ) from exc
            if claimed != expected or path.stem != claimed:
                raise ContinuousCalibrationError(
                    f"IMMUTABLE_EVIDENCE_HASH_MISMATCH:{path.name}"
                )
            rows.append(record)
        return tuple(rows)


@dataclass(frozen=True, slots=True)
class VerifiedHorizonArtifact:
    path: str
    artifact_schema_version: str
    model_protocol: str
    selection_protocol: str
    selection_policy_hash: str
    deployment_candidate_domain_verified: bool
    selected_oos_sample_count: int
    selected_oos_session_count: int
    candidate_evaluation_ledger: Mapping[str, Any]
    training_window: Mapping[str, Any]
    release_id: str
    suite_release_id: str
    model_key: str
    model_version: str
    horizon_days: int
    prediction_kind: str
    artifact_hash: str
    config_hash: str
    code_version: str
    code_hash: str
    feature_protocol_hash: str
    calibration_protocol_hash: str
    dataset_hash: str
    oos_evidence_hash: str
    training_cutoff: date
    created_at: datetime
    valid_until: datetime
    manifest: Mapping[str, Any]

    @classmethod
    def from_manifest(
        cls,
        manifest: Mapping[str, Any],
        *,
        path: Path,
    ) -> "VerifiedHorizonArtifact":
        payload = dict(manifest)
        if str(payload.get("schema_version") or "") != MODEL_ARTIFACT_SCHEMA:
            raise ContinuousCalibrationError("MODEL_ARTIFACT_SCHEMA_INVALID")
        if str(payload.get("model_protocol") or "") != (
            CURRENT_HORIZON_MODEL_PROTOCOL
        ):
            raise ContinuousCalibrationError("MODEL_ARTIFACT_PROTOCOL_INVALID")
        model_key = str(payload.get("model_key") or "").strip()
        model_version = str(payload.get("model_version") or "").strip()
        if not model_key or not model_version:
            raise ContinuousCalibrationError("MODEL_ARTIFACT_IDENTITY_EMPTY")
        try:
            horizon = int(payload.get("horizon_days"))
        except (TypeError, ValueError) as exc:
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_HORIZON_INVALID"
            ) from exc
        if horizon not in {1, 5, 20}:
            raise ContinuousCalibrationError("MODEL_ARTIFACT_HORIZON_INVALID")
        manifest_release_id = str(payload.get("release_id") or "").strip()
        suite_release_id = str(payload.get("suite_release_id") or "").strip()
        if not suite_release_id:
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_SUITE_RELEASE_ID_EMPTY"
            )
        release_id = _release_id(
            suite_release_id,
            model_key,
            model_version,
            horizon,
        )
        if manifest_release_id != release_id:
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_RELEASE_ID_MISMATCH"
            )
        if bool(payload.get("order_authority")):
            raise ContinuousCalibrationError("MODEL_ARTIFACT_ORDER_AUTHORITY_FORBIDDEN")
        oos = payload.get("oos_evidence")
        if not isinstance(oos, Mapping):
            raise ContinuousCalibrationError("MODEL_ARTIFACT_OOS_EVIDENCE_MISSING")
        selection_policy = payload.get("selection_policy")
        selection_evidence = oos.get("selection_evidence")
        if not isinstance(selection_policy, Mapping) or not isinstance(
            selection_evidence, Mapping
        ):
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_SELECTION_EVIDENCE_MISSING"
            )
        selection_policy_hash = _digest(
            selection_policy.get("selection_policy_hash"),
            "selection_policy_hash",
        )
        if (
            selection_policy_hash != CURRENT_HORIZON_SELECTION_POLICY_HASH
            or str(selection_evidence.get("selection_policy_hash") or "")
            != selection_policy_hash
            or str(selection_evidence.get("protocol") or "")
            != CURRENT_HORIZON_SELECTION_PROTOCOL
            or selection_evidence.get("order_authority") is not False
            or selection_evidence.get(
                "deployment_candidate_domain_verified"
            )
            is not False
            or oos.get("economic_metrics_use_frozen_selection_ledger")
            is not True
        ):
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_SELECTION_PROTOCOL_INVALID"
            )
        try:
            selected_samples = int(
                selection_evidence.get("selected_oos_sample_count")
            )
            selected_sessions = int(
                selection_evidence.get("selected_oos_session_count")
            )
        except (TypeError, ValueError) as exc:
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_SELECTION_COUNTS_INVALID"
            ) from exc
        if selected_samples < 0 or selected_sessions < 0:
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_SELECTION_COUNTS_INVALID"
            )
        candidate_ledger = payload.get("candidate_evaluation_ledger")
        if not isinstance(candidate_ledger, Mapping):
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_CANDIDATE_LEDGER_MISSING"
            )
        try:
            candidate_row_count = int(candidate_ledger.get("row_count"))
        except (TypeError, ValueError) as exc:
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_CANDIDATE_LEDGER_INVALID"
            ) from exc
        if (
            candidate_ledger.get("schema_version")
            != CANDIDATE_EVALUATION_LEDGER_SCHEMA
            or not _HEX64.fullmatch(
                str(candidate_ledger.get("content_sha256") or "")
            )
            or candidate_row_count < 0
            or candidate_ledger.get("registration_verification_required")
            is not True
        ):
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_CANDIDATE_LEDGER_INVALID"
            )
        training_window = payload.get("training_window")
        configured_window = _configured_training_window()
        if not isinstance(training_window, Mapping):
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_TRAINING_WINDOW_MISSING"
            )
        signal_end_value = training_window.get("signal_end")
        signal_end = (
            _date(signal_end_value, "training_window.signal_end").isoformat()
            if signal_end_value is not None
            else None
        )
        expected_window_body = {
            "protocol": configured_window["protocol"],
            "configured_history_start": configured_window["history_start"],
            "signal_start": configured_window["history_start"],
            "signal_start_inclusive": True,
            "signal_end": signal_end,
            "signal_end_inclusive": True,
            "status": "FROZEN_DEFAULT_TRAINING_WINDOW",
            "is_current_config_default": True,
        }
        expected_window = {
            **expected_window_body,
            "training_window_hash": _hash(expected_window_body),
        }
        if (
            dict(training_window) != expected_window
            or oos.get("training_window") != training_window
            or oos.get("training_window_status")
            != "FROZEN_DEFAULT_TRAINING_WINDOW"
            or oos.get("training_window_is_current_config_default") is not True
            or (
                signal_end is not None
                and _date(signal_end, "training_window.signal_end")
                > _date(payload.get("training_cutoff"), "training_cutoff")
            )
        ):
            raise ContinuousCalibrationError(
                "MODEL_ARTIFACT_TRAINING_WINDOW_NOT_CURRENT"
            )
        return cls(
            path=str(path.resolve()),
            artifact_schema_version=CURRENT_HORIZON_ARTIFACT_SCHEMA,
            model_protocol=CURRENT_HORIZON_MODEL_PROTOCOL,
            selection_protocol=CURRENT_HORIZON_SELECTION_PROTOCOL,
            selection_policy_hash=selection_policy_hash,
            deployment_candidate_domain_verified=False,
            selected_oos_sample_count=selected_samples,
            selected_oos_session_count=selected_sessions,
            candidate_evaluation_ledger=dict(candidate_ledger),
            training_window=dict(training_window),
            release_id=release_id,
            suite_release_id=suite_release_id,
            model_key=model_key,
            model_version=model_version,
            horizon_days=horizon,
            prediction_kind=str(payload.get("prediction_kind") or ""),
            artifact_hash=_digest(payload.get("artifact_hash"), "artifact_hash"),
            config_hash=_digest(payload.get("config_hash"), "config_hash"),
            code_version=str(payload.get("code_version") or "").strip(),
            code_hash=_digest(payload.get("code_hash"), "code_hash"),
            feature_protocol_hash=_digest(
                payload.get("feature_protocol_hash"), "feature_protocol_hash"
            ),
            calibration_protocol_hash=_digest(
                payload.get("calibration_protocol_hash"),
                "calibration_protocol_hash",
            ),
            dataset_hash=_digest(payload.get("dataset_hash"), "dataset_hash"),
            oos_evidence_hash=_digest(
                payload.get("oos_evidence_hash") or oos.get("evidence_hash"),
                "oos_evidence_hash",
            ),
            training_cutoff=_date(payload.get("training_cutoff"), "training_cutoff"),
            created_at=_aware(payload.get("created_at"), "created_at"),
            valid_until=_artifact_valid_until(payload.get("valid_until")),
            manifest=payload,
        )


@dataclass(frozen=True, slots=True)
class ArtifactDiscovery:
    artifacts: tuple[VerifiedHorizonArtifact, ...]
    rejected: tuple[Mapping[str, Any], ...]
    loader_available: bool


@dataclass(frozen=True, slots=True)
class RetrainingRequest:
    request_id: str
    request_kind: str
    horizon_days: int
    release_id: str
    prior_artifact_hash: str | None
    outcome_checkpoint_hash: str
    outcome_sample_count: int
    new_outcome_count: int
    distinct_decision_session_count: int
    new_decision_session_count: int
    policy_hash: str
    config_hash: str
    code_version: str
    requested_at: str
    training_window_protocol: str = ""
    configured_history_start: str = ""
    order_authority: bool = False


@dataclass(frozen=True, slots=True)
class RetrainingSubmission:
    request_id: str
    status: str
    external_job_id: str | None = None
    detail: str = ""
    order_authority: bool = False


@runtime_checkable
class HorizonModelLifecycleAdapter(Protocol):
    """Adapter boundary; orchestration never embeds a model trainer."""

    def discover(self, *, evaluated_at: datetime) -> ArtifactDiscovery:
        ...

    def submit_retraining(
        self,
        request: RetrainingRequest,
        *,
        primary_engine: Engine,
        market_engine: Engine,
        evaluated_at: datetime,
    ) -> RetrainingSubmission:
        ...

    def training_receipt(
        self, *, suite_release_id: str
    ) -> Any | None:
        ...


class FilesystemHorizonModelAdapter:
    """Discover verified JSON artifacts and persist trainer handoff requests.

    The artifact module owns training and full manifest validation.  If that
    module is unavailable, files are reported as rejected and can never enter
    release governance.  The default submit implementation records a durable
    request; an actual trainer can be injected through the protocol.
    """

    def __init__(
        self,
        artifact_root: Path | str | None = None,
        *,
        trainer_script: Path | str | None = None,
        training_timeout_seconds: int | None = None,
    ) -> None:
        self.artifact_root = Path(
            artifact_root or default_artifact_root()
        ).resolve()
        self.trainer_script = Path(
            trainer_script
            or PROJECT_ROOT / "tools" / "train_trading_v3_horizon_models.py"
        ).resolve()
        self.training_timeout_seconds = max(
            1,
            int(
                training_timeout_seconds
                or FROZEN_ORCHESTRATION_POLICY[
                    "automatic_training_timeout_seconds"
                ]
            ),
        )
        self._cycle_training_result: Mapping[str, Any] | None = None

    @staticmethod
    def _loader() -> Any | None:
        try:
            module = importlib.import_module("server.trading_v3.horizon_models")
        except (ImportError, ModuleNotFoundError):
            return None
        return getattr(module, "load_horizon_artifact", None)

    @staticmethod
    def _suite_loader() -> Any | None:
        try:
            module = importlib.import_module("server.trading_v3.horizon_models")
        except (ImportError, ModuleNotFoundError):
            return None
        return getattr(module, "load_horizon_suite", None)

    def discover(self, *, evaluated_at: datetime) -> ArtifactDiscovery:
        _aware(evaluated_at, "evaluated_at")
        loader = self._loader()
        suite_loader = self._suite_loader()
        if not self.artifact_root.exists():
            return ArtifactDiscovery(
                (), (), loader is not None and suite_loader is not None
            )
        suites: list[tuple[str, tuple[VerifiedHorizonArtifact, ...]]] = []
        rejected: list[Mapping[str, Any]] = []
        suite_paths = sorted(self.artifact_root.rglob("suite.json"))
        suite_parents = {item.parent.resolve() for item in suite_paths}
        for path in sorted(self.artifact_root.rglob("T*.json")):
            if (
                path.name in {"T1.json", "T5.json", "T20.json"}
                and path.parent.resolve() not in suite_parents
            ):
                rejected.append({
                    "path": str(path.resolve()),
                    "reason_code": "MODEL_ARTIFACT_ORPHANED_FROM_SUITE",
                    "reason": "MODEL_ARTIFACT_ORPHANED_FROM_SUITE",
                })
        for suite_path in suite_paths:
            try:
                if loader is None or suite_loader is None:
                    raise ContinuousCalibrationError(
                        "MODEL_ARTIFACT_VERIFIED_LOADER_UNAVAILABLE"
                    )
                try:
                    raw_suite = json.loads(suite_path.read_text("utf-8"))
                except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ContinuousCalibrationError(
                        "MODEL_ARTIFACT_SUITE_JSON_INVALID"
                    ) from exc
                if not isinstance(raw_suite, Mapping):
                    raise ContinuousCalibrationError(
                        "MODEL_ARTIFACT_SUITE_JSON_INVALID"
                    )
                raw_suite_schema = str(
                    raw_suite.get("schema_version") or ""
                )
                if raw_suite_schema in {
                    HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
                    HISTORICAL_HORIZON_SUITE_SCHEMA_V2,
                }:
                    rejected.append({
                        "path": str(suite_path.resolve()),
                        "reason_code": (
                            "HISTORICAL_PROTOCOL_AUDIT_ONLY"
                            if raw_suite_schema
                            == HISTORICAL_HORIZON_SUITE_SCHEMA_V1
                            else "PRE_LEDGER_V2_AUDIT_ONLY"
                        ),
                        "reason": (
                            "HISTORICAL_PROTOCOL_AUDIT_ONLY"
                            if raw_suite_schema
                            == HISTORICAL_HORIZON_SUITE_SCHEMA_V1
                            else "PRE_LEDGER_V2_AUDIT_ONLY"
                        ),
                        "schema_version": raw_suite_schema,
                        "runtime_eligible": False,
                        "order_authority": False,
                    })
                    continue
                if (
                    raw_suite_schema != CURRENT_HORIZON_SUITE_SCHEMA
                    or str(raw_suite.get("model_protocol") or "")
                    != CURRENT_HORIZON_MODEL_PROTOCOL
                ):
                    raise ContinuousCalibrationError(
                        "MODEL_ARTIFACT_SUITE_PROTOCOL_INVALID"
                    )
                suite = suite_loader(
                    suite_path,
                    require_current_code=False,
                    require_current_config=False,
                )
                if not isinstance(suite, Mapping):
                    raise ContinuousCalibrationError(
                        "MODEL_ARTIFACT_SUITE_LOADER_RETURN_INVALID"
                    )
                suite_id = str(suite.get("suite_release_id") or "").strip()
                if not suite_id:
                    raise ContinuousCalibrationError(
                        "MODEL_ARTIFACT_SUITE_RELEASE_ID_EMPTY"
                    )
                members: list[VerifiedHorizonArtifact] = []
                for horizon in (1, 5, 20):
                    artifact_path = suite_path.parent / f"T{horizon}.json"
                    loaded = loader(
                        artifact_path,
                        require_current_code=False,
                        require_current_config=False,
                    )
                    if not isinstance(loaded, Mapping):
                        raise ContinuousCalibrationError(
                            "MODEL_ARTIFACT_LOADER_RETURN_INVALID"
                        )
                    member = VerifiedHorizonArtifact.from_manifest(
                        loaded,
                        path=artifact_path,
                    )
                    if (
                        member.horizon_days != horizon
                        or member.suite_release_id != suite_id
                    ):
                        raise ContinuousCalibrationError(
                            "MODEL_ARTIFACT_SUITE_MEMBER_IDENTITY_MISMATCH"
                        )
                    members.append(member)
                if {item.horizon_days for item in members} != {1, 5, 20}:
                    raise ContinuousCalibrationError(
                        "MODEL_ARTIFACT_SUITE_INCOMPLETE"
                    )
                for field in (
                    "config_hash",
                    "code_version",
                    "code_hash",
                    "training_cutoff",
                ):
                    if len({getattr(item, field) for item in members}) != 1:
                        raise ContinuousCalibrationError(
                            f"MODEL_ARTIFACT_SUITE_{field.upper()}_MIXED"
                        )
                suites.append((
                    suite_id,
                    tuple(sorted(members, key=lambda item: item.horizon_days)),
                ))
            except Exception as exc:
                rejected.append({
                    "path": str(suite_path.resolve()),
                    "reason_code": type(exc).__name__,
                    "reason": str(exc),
                })
        selected: tuple[VerifiedHorizonArtifact, ...] = ()
        if suites:
            _suite_id, selected = max(
                suites,
                key=lambda item: (
                    max(member.training_cutoff for member in item[1]),
                    max(member.created_at for member in item[1]),
                    item[0],
                ),
            )
        return ArtifactDiscovery(
            selected,
            tuple(rejected),
            loader is not None and suite_loader is not None,
        )

    def training_receipt(
        self, *, suite_release_id: str
    ) -> Any | None:
        result = self._cycle_training_result
        if (
            result is None
            or str(result.get("suite_release_id") or "")
            != str(suite_release_id or "")
        ):
            return None
        return result.get("training_receipt_capability")

    def submit_retraining(
        self,
        request: RetrainingRequest,
        *,
        primary_engine: Engine,
        market_engine: Engine,
        evaluated_at: datetime,
    ) -> RetrainingSubmission:
        del primary_engine, market_engine
        now = _aware(evaluated_at, "evaluated_at")
        if not self.trainer_script.is_file():
            raise ContinuousCalibrationError(
                "HORIZON_MODEL_TRAINING_CLI_UNAVAILABLE"
            )
        if PROJECT_ROOT not in self.trainer_script.parents:
            raise ContinuousCalibrationError(
                "HORIZON_MODEL_TRAINING_CLI_OUTSIDE_PROJECT"
            )
        approved_artifact_root = (
            PROJECT_ROOT / str(FROZEN_ORCHESTRATION_POLICY["artifact_root"])
        ).resolve()
        if self.artifact_root != approved_artifact_root:
            raise ContinuousCalibrationError(
                "HORIZON_MODEL_TRAINING_OUTPUT_ROOT_UNAPPROVED"
            )
        if self._cycle_training_result is None:
            cutoff = now.astimezone(MARKET_TIMEZONE).date()
            start = _date(
                _configured_training_window()["history_start"],
                "automatic_training_start",
            )
            current_code, _kind = code_version()
            suite_release_id = (
                f"shadow-auto-{cutoff:%Y%m%d}-"
                f"{config_hash()[:12]}-{current_code[:12]}-"
                f"{secrets.token_hex(4)}"
            )
            command = [
                sys.executable,
                str(self.trainer_script),
                "--start",
                start.isoformat(),
                "--end",
                cutoff.isoformat(),
                "--training-cutoff",
                cutoff.isoformat(),
                "--release-id",
                suite_release_id,
                "--output-root",
                str(self.artifact_root),
                "--max-stocks",
                "0",
            ]
            completed = subprocess.run(
                command,
                cwd=str(PROJECT_ROOT),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.training_timeout_seconds,
                check=False,
            )
            raw_stdout = str(completed.stdout or "")
            stdout_sha256 = hashlib.sha256(
                raw_stdout.encode("utf-8")
            ).hexdigest()
            if completed.returncode != 0:
                stderr = " ".join(
                    str(completed.stderr or "").strip().split()
                )[-1000:]
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_CLI_FAILED:"
                    f"exit={completed.returncode}:"
                    f"stdout_sha256={stdout_sha256}:stderr={stderr}"
                )
            try:
                output = json.loads(raw_stdout)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_CLI_OUTPUT_INVALID"
                ) from exc
            if not isinstance(output, Mapping):
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_CLI_OUTPUT_INVALID"
                )
            rediscovered = self.discover(evaluated_at=now)
            if set(item.horizon_days for item in rediscovered.artifacts) != {
                1, 5, 20
            }:
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_ARTIFACT_SUITE_INCOMPLETE"
                )
            if {
                item.suite_release_id for item in rediscovered.artifacts
            } != {suite_release_id}:
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_ARTIFACT_SUITE_MISMATCH"
                )
            models_raw = output.get("models")
            if not isinstance(models_raw, list) or len(models_raw) != 3:
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_CLI_SUITE_BINDING_INVALID"
                )
            models_by_horizon: dict[int, Mapping[str, Any]] = {}
            for model in models_raw:
                if not isinstance(model, Mapping):
                    raise ContinuousCalibrationError(
                        "HORIZON_MODEL_TRAINING_CLI_SUITE_BINDING_INVALID"
                    )
                try:
                    horizon = int(model.get("horizon_days") or 0)
                except (TypeError, ValueError) as exc:
                    raise ContinuousCalibrationError(
                        "HORIZON_MODEL_TRAINING_CLI_SUITE_BINDING_INVALID"
                    ) from exc
                if horizon in models_by_horizon:
                    raise ContinuousCalibrationError(
                        "HORIZON_MODEL_TRAINING_CLI_SUITE_BINDING_INVALID"
                    )
                models_by_horizon[horizon] = model
            if set(models_by_horizon) != {1, 5, 20}:
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_CLI_SUITE_BINDING_INVALID"
                )
            discovered_by_horizon = {
                item.horizon_days: item for item in rediscovered.artifacts
            }
            if (
                str(output.get("status") or "") not in {"PASS", "BLOCK"}
                or str(output.get("release_id") or "") != suite_release_id
                or output.get("reused_immutable_release") is not False
                or str(output.get("universe_scope") or "")
                != "FULL_A_SHARE_POINT_IN_TIME"
                or output.get("automatic_promotion_allowed") is not False
                or output.get("order_authority") is not False
                or str(output.get("training_window_protocol") or "")
                != _configured_training_window()["protocol"]
                or str(output.get("configured_history_start") or "")
                != _configured_training_window()["history_start"]
                or str(output.get("signal_start") or "")
                != _configured_training_window()["history_start"]
                or str(output.get("training_window_status") or "")
                != "FROZEN_DEFAULT_TRAINING_WINDOW"
                or Path(str(output.get("release_root") or "")).resolve()
                != (self.artifact_root / suite_release_id).resolve()
            ):
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_CLI_POLICY_BINDING_INVALID"
                )
            for horizon, member in discovered_by_horizon.items():
                stdout_model = models_by_horizon[horizon]
                if (
                    str(stdout_model.get("schema_version") or "")
                    != member.artifact_schema_version
                    or str(stdout_model.get("model_protocol") or "")
                    != member.model_protocol
                    or str(stdout_model.get("artifact_hash") or "")
                    != member.artifact_hash
                    or str(stdout_model.get("release_id") or "")
                    != member.release_id
                    or str(stdout_model.get("suite_release_id") or "")
                    != suite_release_id
                    or str(stdout_model.get("model_key") or "")
                    != member.model_key
                    or str(stdout_model.get("model_version") or "")
                    != member.model_version
                    or str(stdout_model.get("config_hash") or "")
                    != member.config_hash
                    or str(stdout_model.get("code_version") or "")
                    != member.code_version
                    or str(stdout_model.get("code_hash") or "")
                    != member.code_hash
                    or str(stdout_model.get("training_cutoff") or "")
                    != member.training_cutoff.isoformat()
                    or not isinstance(
                        stdout_model.get("candidate_evaluation_ledger"),
                        Mapping,
                    )
                    or _canonical_bytes(dict(
                        stdout_model["candidate_evaluation_ledger"]
                    ))
                    != _canonical_bytes(dict(
                        member.candidate_evaluation_ledger
                    ))
                    or not isinstance(
                        stdout_model.get("training_window"),
                        Mapping,
                    )
                    or _canonical_bytes(dict(
                        stdout_model["training_window"]
                    ))
                    != _canonical_bytes(dict(member.training_window))
                ):
                    raise ContinuousCalibrationError(
                        "HORIZON_MODEL_TRAINING_CLI_ARTIFACT_BINDING_INVALID"
                    )
            expected_stdout_status = (
                "PASS"
                if all(
                    str(model.get("gate_status") or "") == "PASS"
                    for model in models_by_horizon.values()
                )
                else "BLOCK"
            )
            if output.get("status") != expected_stdout_status:
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_CLI_GATE_BINDING_INVALID"
                )
            shared_config_hash = {
                item.config_hash for item in rediscovered.artifacts
            }
            shared_code_version = {
                item.code_version for item in rediscovered.artifacts
            }
            shared_code_hash = {
                item.code_hash for item in rediscovered.artifacts
            }
            shared_cutoff = {
                item.training_cutoff for item in rediscovered.artifacts
            }
            if not all(
                len(values) == 1
                for values in (
                    shared_config_hash,
                    shared_code_version,
                    shared_code_hash,
                    shared_cutoff,
                )
            ):
                raise ContinuousCalibrationError(
                    "HORIZON_MODEL_TRAINING_ARTIFACT_SUITE_MIXED"
                )
            completed_at = datetime.now(timezone.utc)
            receipt_body = {
                "schema_version": PROCESS_TRAINING_RECEIPT_SCHEMA,
                "status": "PROCESS_VERIFIED",
                "suite_release_id": suite_release_id,
                "artifact_hashes": {
                    str(item.horizon_days): item.artifact_hash
                    for item in rediscovered.artifacts
                },
                "config_hash": next(iter(shared_config_hash)),
                "code_version": next(iter(shared_code_version)),
                "artifact_code_hash": next(iter(shared_code_hash)),
                "training_cutoff": next(iter(shared_cutoff)).isoformat(),
                "trainer_script": str(self.trainer_script),
                "trainer_script_hash": hashlib.sha256(
                    self.trainer_script.read_bytes()
                ).hexdigest(),
                "argv": command,
                "exit_code": int(completed.returncode),
                "stdout_sha256": stdout_sha256,
                "stdout_text": raw_stdout,
                "completed_at": completed_at.isoformat(),
            }
            training_receipt_capability = (
                _issue_process_bound_training_receipt(receipt_body)
            )
            training_receipt = dict(training_receipt_capability.receipt)
            self._cycle_training_result = {
                "suite_release_id": suite_release_id,
                "trainer_argv": command,
                "trainer_exit_code": int(completed.returncode),
                "trainer_stdout_sha256": stdout_sha256,
                "stdout": dict(output),
                "artifact_hashes": {
                    str(item.horizon_days): item.artifact_hash
                    for item in rediscovered.artifacts
                },
                "training_receipt": training_receipt,
                "training_receipt_capability": training_receipt_capability,
            }
        assert self._cycle_training_result is not None
        job_id = _hash(self._cycle_training_result)
        submission_detail = {
            "suite_release_id": self._cycle_training_result[
                "suite_release_id"
            ],
            "trainer_argv": self._cycle_training_result["trainer_argv"],
            "trainer_exit_code": self._cycle_training_result[
                "trainer_exit_code"
            ],
            "trainer_stdout_sha256": self._cycle_training_result[
                "trainer_stdout_sha256"
            ],
            "trainer_result_hash": job_id,
            "artifact_hashes": self._cycle_training_result[
                "artifact_hashes"
            ],
            "order_authority": False,
        }
        return RetrainingSubmission(
            request_id=request.request_id,
            status="TRAINING_CLI_SUCCEEDED",
            external_job_id=job_id,
            detail=_canonical_bytes(submission_detail).decode("utf-8"),
        )


@contextmanager
def continuous_cycle_lock(
    engine: Engine,
    *,
    timeout_seconds: int = 0,
) -> Iterable[None]:
    """Serialize the whole contract/outcome/learning/release lifecycle."""

    dialect = str(getattr(getattr(engine, "dialect", None), "name", ""))
    if dialect.casefold() == "mysql":
        acquired = False
        try:
            with mysql_named_lock(
                engine,
                CONTINUOUS_CYCLE_LOCK,
                timeout_seconds=max(0, int(timeout_seconds)),
            ):
                acquired = True
                yield
        except TimeoutError as exc:
            if acquired:
                raise
            raise ContinuousCalibrationAlreadyRunning(
                "CONTINUOUS_CALIBRATION_ALREADY_RUNNING"
            ) from exc
        return
    acquired = _LOCAL_CYCLE_LOCK.acquire(
        timeout=max(0, int(timeout_seconds))
    ) if timeout_seconds else _LOCAL_CYCLE_LOCK.acquire(blocking=False)
    if not acquired:
        raise ContinuousCalibrationAlreadyRunning(
            "CONTINUOUS_CALIBRATION_ALREADY_RUNNING"
        )
    try:
        yield
    finally:
        _LOCAL_CYCLE_LOCK.release()


def _latest_request_counts(
    store: ImmutableEvidenceStore,
) -> Mapping[tuple[int, str], tuple[int, int]]:
    accepted_request_ids = {
        str(payload.get("request_id") or "")
        for record in store.records("retrain_submission")
        for payload in [dict(record.get("payload") or {})]
        if str(payload.get("status") or "") in {
            "TRAINING_CLI_SUCCEEDED",
            "TRAINING_JOB_QUEUED",
        }
    }
    latest: dict[tuple[int, str], tuple[datetime, int, int]] = {}
    for record in store.records("retrain_request"):
        payload = dict(record.get("payload") or {})
        if str(payload.get("request_id") or "") not in accepted_request_ids:
            continue
        try:
            horizon = int(payload["horizon_days"])
            requested = _aware(payload["requested_at"], "requested_at")
            count = int(payload["outcome_sample_count"])
            sessions = int(payload["distinct_decision_session_count"])
        except (KeyError, TypeError, ValueError, ContinuousCalibrationError):
            raise ContinuousCalibrationError("RETRAIN_REQUEST_EVIDENCE_INVALID")
        cohort_key = (
            horizon,
            str(payload.get("prior_artifact_hash") or ""),
        )
        prior = latest.get(cohort_key)
        if prior is None or requested > prior[0]:
            latest[cohort_key] = (requested, count, sessions)
    return {key: (value[1], value[2]) for key, value in latest.items()}


def _retraining_request_identity(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "identity_protocol": RETRAINING_REQUEST_IDENTITY_PROTOCOL,
        "schema_version": CONTINUOUS_ORCHESTRATION_SCHEMA,
        **{
            key: payload.get(key)
            for key in (
                "request_kind",
                "horizon_days",
                "release_id",
                "prior_artifact_hash",
                "outcome_checkpoint_hash",
                "policy_hash",
                "config_hash",
                "code_version",
                "training_window_protocol",
                "configured_history_start",
            )
        },
    }


def _new_request(
    *,
    request_kind: str,
    horizon: int,
    release_id: str,
    artifact_hash: str | None,
    checkpoint_hash: str,
    sample_count: int,
    new_count: int,
    distinct_session_count: int,
    new_session_count: int,
    policy_hash: str,
    current_config_hash: str,
    current_code_version: str,
    evaluated_at: datetime,
) -> RetrainingRequest:
    training_window = _configured_training_window()
    payload = {
        "request_kind": request_kind,
        "horizon_days": horizon,
        "release_id": release_id,
        "prior_artifact_hash": artifact_hash,
        "outcome_checkpoint_hash": checkpoint_hash,
        "outcome_sample_count": sample_count,
        "new_outcome_count": new_count,
        "distinct_decision_session_count": distinct_session_count,
        "new_decision_session_count": new_session_count,
        "policy_hash": policy_hash,
        "config_hash": current_config_hash,
        "code_version": current_code_version,
        "training_window_protocol": training_window["protocol"],
        "configured_history_start": training_window["history_start"],
    }
    # Delta counts are audit fields relative to the last accepted request.
    # They must not change the idempotency identity of the same immutable
    # checkpoint/config/code/window request.
    identity = _retraining_request_identity(payload)
    return RetrainingRequest(
        request_id=_hash(identity),
        requested_at=evaluated_at.astimezone(timezone.utc).isoformat(),
        **payload,
    )


def _artifact_blockers(
    artifact: VerifiedHorizonArtifact,
    *,
    current_config_hash: str,
    current_code_version: str,
    maximum_age_days: float,
    evaluated_at: datetime,
) -> list[str]:
    blockers: list[str] = []
    manifest = dict(artifact.manifest)
    if artifact.artifact_schema_version != CURRENT_HORIZON_ARTIFACT_SCHEMA:
        blockers.append("ARTIFACT_PROTOCOL_NOT_CURRENT")
    if artifact.model_protocol != CURRENT_HORIZON_MODEL_PROTOCOL:
        blockers.append("ARTIFACT_MODEL_PROTOCOL_NOT_CURRENT")
    if artifact.selection_protocol != CURRENT_HORIZON_SELECTION_PROTOCOL:
        blockers.append("ARTIFACT_SELECTION_PROTOCOL_NOT_CURRENT")
    if artifact.selection_policy_hash != CURRENT_HORIZON_SELECTION_POLICY_HASH:
        blockers.append("ARTIFACT_SELECTION_POLICY_NOT_CURRENT")
    if (
        artifact.candidate_evaluation_ledger.get("schema_version")
        != CANDIDATE_EVALUATION_LEDGER_SCHEMA
        or int(artifact.candidate_evaluation_ledger.get("row_count") or 0)
        <= 0
    ):
        blockers.append("ARTIFACT_CANDIDATE_LEDGER_NOT_REGISTERABLE")
    if artifact.deployment_candidate_domain_verified is not False:
        blockers.append("ARTIFACT_SELECTION_DOMAIN_CLAIM_INVALID")
    if artifact.prediction_kind != "CALIBRATED_OOS":
        blockers.append("PREDICTION_IS_NOT_CALIBRATED_OOS")
    if artifact.config_hash != current_config_hash:
        blockers.append("ARTIFACT_CONFIG_STALE")
    if artifact.code_version != current_code_version:
        blockers.append("ARTIFACT_CODE_VERSION_STALE")
    current_model_code_hash = hashlib.sha256(
        (PROJECT_ROOT / "server" / "trading_v3" / "horizon_models.py")
        .resolve()
        .read_bytes()
    ).hexdigest()
    if artifact.code_hash != current_model_code_hash:
        blockers.append("ARTIFACT_CODE_HASH_STALE")
    age_days = (
        evaluated_at.astimezone(timezone.utc) - artifact.created_at
    ).total_seconds() / 86400.0
    if age_days < 0:
        blockers.append("ARTIFACT_NOT_YET_AVAILABLE")
    elif age_days > maximum_age_days:
        blockers.append("ARTIFACT_TOO_OLD")
    if evaluated_at.astimezone(timezone.utc) > artifact.valid_until:
        blockers.append("ARTIFACT_EXPIRED")
    gate = manifest.get("gate")
    gate_status = (
        str(gate.get("status") or "")
        if isinstance(gate, Mapping)
        else str(manifest.get("gate_status") or "")
    )
    if gate_status != "PASS":
        blockers.append("ARTIFACT_OOS_GATE_NOT_PASSED")
    if manifest.get("contract_eligible") is not True:
        blockers.append("ARTIFACT_CONTRACT_NOT_ELIGIBLE")
    # Shadow publication consumes the artifact's OOS research gate.  The deep
    # artifact loader has already verified any embedded research execution
    # protocol and its hash, but an externally persisted executable attestation
    # is deliberately *not* a Shadow prerequisite.  That stronger evidence is
    # enforced below when a PAPER_ELIGIBLE request is assembled.
    if bool(manifest.get("order_authority")):
        blockers.append("ARTIFACT_ORDER_AUTHORITY_FORBIDDEN")
    return list(dict.fromkeys(blockers))


def _persist_auto_demotion(
    repository: Any,
    *,
    evidence_hash: str,
    current_config_hash: str,
    evaluated_at: datetime,
) -> list[dict[str, Any]]:
    audit = repository.release_audit()
    demoted: list[dict[str, Any]] = []
    for release in audit.get("releases") or ():
        if (
            str(release.get("audit_stage") or release.get("current_stage"))
            != ReleaseStage.PAPER_ELIGIBLE.value
            or str(release.get("effective_stage") or "")
            == ReleaseStage.PAPER_ELIGIBLE.value
        ):
            continue
        release_id = str(release.get("release_id") or "")
        latest = repository.latest_release(release_id)
        if latest is None or str(latest.get("current_stage")) != (
            ReleaseStage.PAPER_ELIGIBLE.value
        ):
            continue
        transition = ReleaseTransition(
            previous_stage=ReleaseStage.PAPER_ELIGIBLE.value,
            event="AUTOMATIC_EVIDENCE_DEMOTION",
            next_stage=ReleaseStage.BLOCKED.value,
            accepted=True,
            reason_code="CURRENT_RELEASE_EVIDENCE_INVALID",
            order_authority=False,
        )
        persisted = repository.append_release_transition(
            release_id=release_id,
            transition=transition,
            evidence_hash=evidence_hash,
            config_hash=current_config_hash,
            occurred_at=evaluated_at,
        )
        demoted.append({
            "release_id": release_id,
            "release_state_id": persisted.get("release_state_id"),
            "stage": persisted.get("current_stage"),
        })
    return demoted


def run_continuous_calibration_orchestration(
    repository: Any,
    primary_engine: Engine,
    market_engine: Engine,
    *,
    config: Mapping[str, Any],
    evaluated_at: datetime,
    learning_run: Mapping[str, Any],
    lifecycle_adapter: HorizonModelLifecycleAdapter | None = None,
    evidence_store: ImmutableEvidenceStore | None = None,
    _post_training_rediscovery: bool = True,
) -> dict[str, Any]:
    """Discover artifacts, trigger refreshes, and enforce release truth.

    This function deliberately cannot fabricate a PASS gate.  It can publish
    immutable retraining/promotion requests and automatically demote stale
    PAPER_ELIGIBLE releases.  Promotion remains the responsibility of an
    external signed-attestation adapter; after that adapter writes DB state,
    ``release_audit`` is still the final server-side authority.
    """

    now = _aware(evaluated_at, "evaluated_at")
    policy = dict(config.get("continuous_calibration") or {})
    minimum = {
        int(key): int(value)
        for key, value in dict(policy.get("minimum_mature_samples") or {}).items()
    }
    if set(minimum) != {1, 5, 20} or min(minimum.values()) <= 0:
        raise ContinuousCalibrationError(
            "CONTINUOUS_CALIBRATION_HORIZONS_INCOMPLETE"
        )
    current_config_hash = config_hash()
    current_code_version, current_code_kind = code_version()
    orchestration_policy = {
        **dict(FROZEN_ORCHESTRATION_POLICY),
        "release_gate_policy_hash": _hash(policy),
        "minimum_mature_samples": minimum,
    }
    orchestration_policy_hash = _hash(orchestration_policy)
    store = evidence_store or ImmutableEvidenceStore()
    adapter = lifecycle_adapter or FilesystemHorizonModelAdapter()
    checkpoint = repository.calibration_outcome_checkpoint(
        evaluation_date=now.astimezone(MARKET_TIMEZONE).date()
    )
    observed_checkpoint_count = int(checkpoint.get("sample_count") or 0)
    forward_eligible_checkpoint_count = int(
        checkpoint.get("forward_eligible_sample_count") or 0
    )
    if observed_checkpoint_count <= 0:
        forward_evidence_progress = "EMPTY"
    elif forward_eligible_checkpoint_count <= 0:
        forward_evidence_progress = "UNVERIFIED_OR_PROXY_ONLY"
    else:
        forward_evidence_progress = "VERIFIED_PROGRESS"
    checkpoint_record = store.put(
        "outcome_checkpoint",
        {
            **dict(checkpoint),
            "policy_hash": orchestration_policy_hash,
            "config_hash": current_config_hash,
            "code_version": current_code_version,
        },
        created_at=now,
    )
    discovery = adapter.discover(evaluated_at=now)
    artifacts_by_horizon = {
        item.horizon_days: item for item in discovery.artifacts
    }
    artifact_rows: list[dict[str, Any]] = []
    registrations: list[dict[str, Any]] = []
    artifact_evidence: dict[int, StoredEvidence] = {}
    publication_payloads: dict[int, dict[str, Any]] = {}
    discovered_horizons = {
        item.horizon_days for item in discovery.artifacts
    }
    discovered_suite_ids = {
        item.suite_release_id for item in discovery.artifacts
    }
    suite_identity_blockers: list[str] = []
    if discovered_horizons != {1, 5, 20}:
        suite_identity_blockers.append("HORIZON_SUITE_INCOMPLETE")
    if len(discovered_suite_ids) != 1:
        suite_identity_blockers.append("HORIZON_SUITE_IDENTITY_MIXED")
    maximum_age = float(policy.get("maximum_evidence_age_days") or 0)
    if maximum_age <= 0:
        raise ContinuousCalibrationError(
            "CONTINUOUS_CALIBRATION_MAXIMUM_AGE_INVALID"
        )
    for artifact in discovery.artifacts:
        stored = store.put(
            "model_artifact",
            {
                "artifact": dict(artifact.manifest),
                "artifact_hash": artifact.artifact_hash,
                "artifact_config_hash": artifact.config_hash,
                "artifact_code_version": artifact.code_version,
                "artifact_code_hash": artifact.code_hash,
            },
            # Artifact evidence is an immutable content identity, not an
            # observation of this scheduler run.  Keeping its envelope tied to
            # the artifact creation time makes registration idempotent across
            # later cycles (including cycles where current config/code changed).
            created_at=artifact.created_at,
        )
        artifact_evidence[artifact.horizon_days] = stored
        blockers = _artifact_blockers(
            artifact,
            current_config_hash=current_config_hash,
            current_code_version=current_code_version,
            maximum_age_days=maximum_age,
            evaluated_at=now,
        )
        artifact_rows.append({
            "release_id": artifact.release_id,
            "suite_release_id": artifact.suite_release_id,
            "horizon_days": artifact.horizon_days,
            "artifact_hash": artifact.artifact_hash,
            "immutable_evidence_hash": stored.evidence_hash,
            "status": "VERIFIED" if not blockers else "BLOCKED",
            "blockers": blockers,
            "order_authority": False,
        })
        # Every artifact-level blocker prevents Shadow publication.  External
        # EXECUTABLE_VERIFIED/fill attestation is intentionally evaluated
        # later at PAPER_ELIGIBLE, so research execution status="PASS" is not
        # an artifact blocker in ``_artifact_blockers``.
        registration_blockers = list(blockers)
        registration_blockers.extend(suite_identity_blockers)
        receipt_loader = getattr(adapter, "training_receipt", None)
        training_receipt = (
            receipt_loader(suite_release_id=artifact.suite_release_id)
            if callable(receipt_loader)
            else None
        )
        registry_registration_payload = {
            "release_id": artifact.release_id,
            "suite_release_id": artifact.suite_release_id,
            "model_key": artifact.model_key,
            "model_version": artifact.model_version,
            "horizon_days": artifact.horizon_days,
            "artifact_hash": artifact.artifact_hash,
            "artifact_evidence_hash": stored.evidence_hash,
            "artifact_config_hash": artifact.config_hash,
            "artifact_code_version": artifact.code_version,
            "artifact_code_hash": artifact.code_hash,
            "artifact_created_at": artifact.created_at.isoformat(),
            "candidate_evaluation_ledger": dict(
                artifact.candidate_evaluation_ledger
            ),
            "requested_action": "REGISTER_OOS_ARTIFACT",
            # The immutable registration identity must not depend on an
            # ephemeral, one-process capability.  On retries the registry row
            # below supplies the durable receipt truth.
            "receipt_authority": "IMMUTABLE_DB_ROW_OR_FIRST_INSERT_CAPABILITY",
            "order_authority": False,
        }
        registration_record = store.put(
            "artifact_registration_request",
            registry_registration_payload,
            created_at=artifact.created_at,
        )
        publication_payload = {
            **registry_registration_payload,
            "artifact_path": artifact.path,
            "config_hash": current_config_hash,
            "code_version": current_code_version,
            "requested_stage": "SHADOW",
            "blockers": registration_blockers,
            "order_authority": False,
        }
        publication_payloads[artifact.horizon_days] = publication_payload
        save_kwargs: dict[str, Any] = {
            "registration_evidence_hash": registration_record.evidence_hash,
            "artifact_root": str(Path(artifact.path).resolve().parent),
        }
        if training_receipt is not None:
            save_kwargs["training_receipt"] = training_receipt
        registry_row = repository.save_horizon_model_artifact(
            artifact.manifest,
            **save_kwargs,
        )
        if (
            str(registry_row.get("release_id") or "") != artifact.release_id
            or str(registry_row.get("artifact_id") or "")
            != artifact.artifact_hash
            or bool(registry_row.get("order_authority"))
        ):
            raise ContinuousCalibrationError(
                "HORIZON_ARTIFACT_REGISTRATION_IDENTITY_MISMATCH"
            )
        registry_status = str(registry_row.get("artifact_status") or "")
        registry_receipt_status = str(
            registry_row.get("training_receipt_status") or "UNVERIFIED"
        )
        publication_blockers = list(registration_blockers)
        if registry_receipt_status != "PROCESS_VERIFIED":
            publication_blockers.append("TRAINING_RECEIPT_UNVERIFIED")
        if registry_status != "OOS_VERIFIED":
            publication_blockers.append("ARTIFACT_REGISTRY_NOT_OOS_VERIFIED")
        artifact_row = artifact_rows[-1]
        artifact_row["training_receipt_status"] = registry_receipt_status
        artifact_row["training_receipt_hash"] = registry_row.get(
            "training_receipt_hash"
        )
        artifact_row["candidate_ledger_schema_version"] = registry_row.get(
            "candidate_ledger_schema_version"
        )
        artifact_row["candidate_ledger_content_sha256"] = registry_row.get(
            "candidate_ledger_content_sha256"
        )
        artifact_row["candidate_ledger_row_count"] = registry_row.get(
            "candidate_ledger_row_count"
        )
        artifact_row["ledger_registration_evidence_hash"] = registry_row.get(
            "ledger_registration_evidence_hash"
        )
        artifact_row["registration_verification_hash"] = registry_row.get(
            "registration_verification_hash"
        )
        artifact_row["blockers"] = list(dict.fromkeys(publication_blockers))
        artifact_row["status"] = (
            "VERIFIED"
            if not artifact_row["blockers"]
            and registry_status == "OOS_VERIFIED"
            else "BLOCKED"
        )
        registrations.append({
            **publication_payload,
            "status": (
                "PENDING_ATOMIC_SUITE"
                if not publication_blockers
                else "BLOCKED"
            ),
            "publication_blockers": publication_blockers,
            "registry_status": registry_status,
            "training_receipt_status": registry_receipt_status,
            "training_receipt_hash": registry_row.get("training_receipt_hash"),
            "candidate_ledger_schema_version": registry_row.get(
                "candidate_ledger_schema_version"
            ),
            "candidate_ledger_content_sha256": registry_row.get(
                "candidate_ledger_content_sha256"
            ),
            "candidate_ledger_row_count": registry_row.get(
                "candidate_ledger_row_count"
            ),
            "ledger_registration_evidence_hash": registry_row.get(
                "ledger_registration_evidence_hash"
            ),
            "registration_verification_hash": registry_row.get(
                "registration_verification_hash"
            ),
            "artifact_id": registry_row.get("artifact_id"),
            "inserted": bool(registry_row.get("inserted")),
            "registration_request_hash": registration_record.evidence_hash,
            "publication_request_hash": None,
            "release_state_id": None,
        })

    # START_SHADOW is a suite-level decision.  No horizon is initialized or
    # advanced until the selected release has one verified, receipt-backed
    # member for each of T1/T5/T20.  This prevents partial real suites from
    # mixing with older models or proxies after a mid-registration failure.
    atomic_suite_ready = (
        discovered_horizons == {1, 5, 20}
        and len(discovered_suite_ids) == 1
        and len(registrations) == 3
        and all(
            not item["publication_blockers"]
            and item["registry_status"] == "OOS_VERIFIED"
            and item["training_receipt_status"] == "PROCESS_VERIFIED"
            for item in registrations
        )
    )
    if not atomic_suite_ready:
        suite_blocker = "HORIZON_SUITE_NOT_ATOMICALLY_VERIFIED"
        for item in artifact_rows:
            item["blockers"] = list(dict.fromkeys([
                *item["blockers"], suite_blocker,
            ]))
            item["status"] = "BLOCKED"
        for item in registrations:
            item["publication_blockers"] = list(dict.fromkeys([
                *item["publication_blockers"], suite_blocker,
            ]))
            item["status"] = "BLOCKED"
    else:
        registrations_by_horizon = {
            int(item["horizon_days"]): item for item in registrations
        }
        suite_id = next(iter(discovered_suite_ids))
        publish_suite = getattr(
            repository, "publish_horizon_suite_shadow", None
        )
        if not callable(publish_suite):
            raise ContinuousCalibrationError(
                "HORIZON_SUITE_ATOMIC_PUBLISHER_UNAVAILABLE"
            )
        published = publish_suite(
            suite_release_id=suite_id,
            members=[
                {
                    "suite_release_id": artifact.suite_release_id,
                    "release_id": artifact.release_id,
                    "model_key": artifact.model_key,
                    "model_version": artifact.model_version,
                    "horizon_days": artifact.horizon_days,
                    "evidence_hash": registrations_by_horizon[
                        artifact.horizon_days
                    ]["registration_request_hash"],
                }
                for artifact in sorted(
                    discovery.artifacts, key=lambda item: item.horizon_days
                )
            ],
            config_hash=current_config_hash,
            occurred_at=now,
        )
        release_rows = dict(published.get("releases_by_horizon") or {})
        if set(map(int, release_rows)) != {1, 5, 20}:
            raise ContinuousCalibrationError(
                "HORIZON_SUITE_ATOMIC_PUBLICATION_INCOMPLETE"
            )
        for horizon, item in registrations_by_horizon.items():
            latest_release = dict(
                release_rows.get(horizon)
                or release_rows.get(str(horizon))
                or {}
            )
            item["release_state_id"] = latest_release.get("release_state_id")
            item["status"] = (
                "PUBLISHED_SHADOW"
                if str(latest_release.get("current_stage") or "")
                in {
                    ReleaseStage.SHADOW.value,
                    "CALIBRATION_REVIEW",
                    ReleaseStage.PAPER_ELIGIBLE.value,
                }
                else "EXISTING_RELEASE_STATE"
            )

    # Persist the final suite-aware publication request only after the atomic
    # decision above, so evidence cannot claim an individual publish that the
    # suite gate rejected.
    for item in registrations:
        horizon = int(item["horizon_days"])
        publication_payload = {
            **publication_payloads[horizon],
            "blockers": list(item["publication_blockers"]),
        }
        publication_record = store.put(
            "shadow_publication_request",
            publication_payload,
            created_at=now,
        )
        item["blockers"] = list(item["publication_blockers"])
        item["publication_request_hash"] = publication_record.evidence_hash

    previous_counts = _latest_request_counts(store)
    existing_requests_by_id: dict[str, dict[str, Any]] = {}
    for record in store.records("retrain_request"):
        payload = dict(record.get("payload") or {})
        request_id = str(payload.get("request_id") or "")
        # Pre-identity-protocol records are audit history only.  Current
        # requests always bind the configured frozen training window.
        if not request_id or not str(
            payload.get("training_window_protocol") or ""
        ):
            continue
        if _hash(_retraining_request_identity(payload)) != request_id:
            raise ContinuousCalibrationError(
                "RETRAIN_REQUEST_IDENTITY_INVALID"
            )
        prior = existing_requests_by_id.get(request_id)
        if prior is None or str(record.get("created_at") or "") < str(
            prior.get("created_at") or ""
        ):
            existing_requests_by_id[request_id] = dict(record)
    existing_submissions_by_id: dict[str, dict[str, Any]] = {}
    for record in store.records("retrain_submission"):
        payload = dict(record.get("payload") or {})
        request_id = str(payload.get("request_id") or "")
        if request_id:
            existing_submissions_by_id[request_id] = payload
    by_artifact_hash = dict(checkpoint.get("by_artifact_hash") or {})
    retraining: list[dict[str, Any]] = []
    for horizon in (1, 5, 20):
        artifact = artifacts_by_horizon.get(horizon)
        horizon_checkpoint = dict(
            by_artifact_hash.get(artifact.artifact_hash) or {}
            if artifact is not None
            else {}
        )
        observed_sample_count = int(
            horizon_checkpoint.get("sample_count") or 0
        )
        sample_count = int(
            horizon_checkpoint.get("forward_eligible_sample_count") or 0
        )
        prior_count, prior_sessions = previous_counts.get(
            (horizon, artifact.artifact_hash if artifact else ""),
            (0, 0),
        )
        prior_count = int(prior_count)
        prior_sessions = int(prior_sessions)
        new_count = max(0, sample_count - prior_count)
        observed_distinct_sessions = int(
            horizon_checkpoint.get("distinct_decision_session_count") or 0
        )
        distinct_sessions = int(
            horizon_checkpoint.get(
                "forward_eligible_decision_session_count"
            )
            or 0
        )
        new_sessions = max(0, distinct_sessions - prior_sessions)
        minimum_sessions = int(
            dict(FROZEN_ORCHESTRATION_POLICY[
                "minimum_distinct_decision_sessions"
            ])[str(horizon)]
        )
        minimum_new_sessions = int(
            dict(FROZEN_ORCHESTRATION_POLICY[
                "minimum_new_decision_sessions"
            ])[str(horizon)]
        )
        refresh_threshold = max(
            1,
            int(
                minimum[horizon]
                * float(FROZEN_ORCHESTRATION_POLICY[
                    "incremental_refresh_fraction"
                ])
            ),
        )
        full_threshold = max(
            refresh_threshold,
            int(
                minimum[horizon]
                * float(FROZEN_ORCHESTRATION_POLICY["full_retrain_fraction"])
            ),
        )
        artifact_blockers = (
            next(
                item["blockers"]
                for item in artifact_rows
                if item["horizon_days"] == horizon
            )
            if artifact is not None
            else []
        )
        # A historically blocked Gate is the beginning of a forward research
        # cohort, not permission to tune the same frozen history every day.
        # Immediate replacement is reserved for a missing or unverifiable
        # artifact.  A valid process/ledger-backed BLOCK suite retrains only
        # after new QMT-attested, artifact-bound outcomes and distinct decision
        # sessions reach the frozen thresholds below.
        force_full_retrain = artifact is None or any(
            code in {
                "ARTIFACT_TOO_OLD",
                "ARTIFACT_EXPIRED",
                "ARTIFACT_CONFIG_STALE",
                "ARTIFACT_CODE_VERSION_STALE",
                "ARTIFACT_CODE_HASH_STALE",
                "ARTIFACT_PROTOCOL_NOT_CURRENT",
                "ARTIFACT_MODEL_PROTOCOL_NOT_CURRENT",
                "ARTIFACT_SELECTION_PROTOCOL_NOT_CURRENT",
                "ARTIFACT_SELECTION_POLICY_NOT_CURRENT",
                "ARTIFACT_CANDIDATE_LEDGER_NOT_REGISTERABLE",
                "ARTIFACT_SELECTION_DOMAIN_CLAIM_INVALID",
                "PREDICTION_IS_NOT_CALIBRATED_OOS",
                "TRAINING_RECEIPT_UNVERIFIED",
                "HORIZON_SUITE_INCOMPLETE",
                "HORIZON_SUITE_IDENTITY_MIXED",
            }
            for code in artifact_blockers
        )
        if not force_full_retrain and (
            sample_count < refresh_threshold
            or distinct_sessions < minimum_sessions
            or new_count < refresh_threshold
            or new_sessions < minimum_new_sessions
        ):
            retraining.append({
                "horizon_days": horizon,
                "status": "NOT_DUE",
                "sample_count": sample_count,
                "observed_sample_count": observed_sample_count,
                "new_outcome_count": new_count,
                "distinct_decision_session_count": distinct_sessions,
                "observed_distinct_decision_session_count": (
                    observed_distinct_sessions
                ),
                "new_decision_session_count": new_sessions,
                "minimum_distinct_decision_sessions": minimum_sessions,
                "minimum_new_decision_sessions": minimum_new_sessions,
                "refresh_threshold": refresh_threshold,
                "full_retrain_threshold": full_threshold,
                "order_authority": False,
            })
            continue
        request_kind = (
            "FULL_RETRAIN"
            if force_full_retrain
            else (
                "FULL_RETRAIN_ON_OUTCOME_THRESHOLD"
                if new_count >= full_threshold
                else "FULL_RETRAIN_FOR_CALIBRATION_REFRESH"
            )
        )
        release_id = (
            artifact.release_id
            if artifact is not None
            else f"unassigned:T+{horizon}"
        )
        request = _new_request(
            request_kind=request_kind,
            horizon=horizon,
            release_id=release_id,
            artifact_hash=artifact.artifact_hash if artifact else None,
            checkpoint_hash=str(
                horizon_checkpoint.get("forward_evidence_hash")
                or horizon_checkpoint.get("evidence_hash")
                or checkpoint_record.evidence_hash
            ),
            sample_count=sample_count,
            new_count=new_count,
            distinct_session_count=distinct_sessions,
            new_session_count=new_sessions,
            policy_hash=orchestration_policy_hash,
            current_config_hash=current_config_hash,
            current_code_version=current_code_version,
            evaluated_at=now,
        )
        existing_request = existing_requests_by_id.get(request.request_id)
        if existing_request is not None:
            prior_submission = dict(
                existing_submissions_by_id.get(request.request_id) or {}
            )
            retraining.append({
                **asdict(request),
                "status": "NOT_DUE",
                "reason_code": "SAME_CHECKPOINT_REQUEST_REUSED",
                "reused_request_evidence_hash": str(
                    existing_request.get("evidence_hash") or ""
                ),
                "reused_submission_status": (
                    str(prior_submission.get("status") or "") or None
                ),
                "order_authority": False,
            })
            continue
        stored_request = store.put(
            "retrain_request",
            asdict(request),
            created_at=now,
        )
        submission = adapter.submit_retraining(
            request,
            primary_engine=primary_engine,
            market_engine=market_engine,
            evaluated_at=now,
        )
        if submission.request_id != request.request_id:
            raise ContinuousCalibrationError(
                "RETRAINING_SUBMISSION_IDENTITY_MISMATCH"
            )
        stored_submission = store.put(
            "retrain_submission",
            asdict(submission),
            created_at=now,
        )
        retraining.append({
            **asdict(request),
            "status": submission.status,
            "request_evidence_hash": stored_request.evidence_hash,
            "submission_evidence_hash": stored_submission.evidence_hash,
        })

    if (
        _post_training_rediscovery
        and any(
            item.get("status") == "TRAINING_CLI_SUCCEEDED"
            for item in retraining
        )
    ):
        refreshed = run_continuous_calibration_orchestration(
            repository,
            primary_engine,
            market_engine,
            config=config,
            evaluated_at=now,
            learning_run=learning_run,
            lifecycle_adapter=adapter,
            evidence_store=store,
            _post_training_rediscovery=False,
        )
        refreshed["retraining_triggered"] = retraining
        refreshed["artifact_rediscovered_after_training"] = True
        return refreshed

    verified_learning = repository.verified_learning_run(
        str(learning_run.get("learning_run_id") or "")
    )
    promotion: list[dict[str, Any]] = []
    for artifact in discovery.artifacts:
        blockers = list(next(
            item["blockers"]
            for item in artifact_rows
            if item["release_id"] == artifact.release_id
        ))
        horizon_checkpoint = dict(
            by_artifact_hash.get(artifact.artifact_hash)
            or {}
        )
        forward_count = int(
            horizon_checkpoint.get("forward_eligible_sample_count") or 0
        )
        executable_count = int(
            horizon_checkpoint.get("executable_verified_count") or 0
        )
        if forward_count <= 0 or executable_count != forward_count:
            blockers.append("FORWARD_OUTCOMES_NOT_EXECUTABLE_VERIFIED")
        feasibility = artifact.manifest.get("execution_feasibility")
        if not isinstance(feasibility, Mapping) or str(
            feasibility.get("status") or ""
        ) != "EXECUTABLE_VERIFIED":
            blockers.append("EXECUTION_FEASIBILITY_UNVERIFIED")
        if not isinstance(feasibility, Mapping) or str(
            feasibility.get("provenance_status") or ""
        ) != "PERSISTED_VERIFIED":
            blockers.append("EXECUTION_ATTESTATION_NOT_PERSISTED_VERIFIED")
        if verified_learning is None:
            blockers.append("PERSISTED_CURRENT_LEARNING_RUN_REQUIRED")
        else:
            try:
                hashes = json.loads(
                    str(verified_learning.get("model_artifact_hashes_json") or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                hashes = {}
            if artifact.artifact_hash not in set(map(str, hashes.values())):
                blockers.append("LEARNING_RUN_ARTIFACT_NOT_CURRENT")
        latest_gate = repository.latest_calibration_gate(artifact.release_id)
        if (
            latest_gate is None
            or str(latest_gate.get("gate_status") or "") != "PASS"
            or str(latest_gate.get("evidence_provenance_status") or "")
            != "PERSISTED_VERIFIED"
        ):
            blockers.append("LATEST_PERSISTED_PASS_GATE_REQUIRED")
        # The external pipeline is required even when all quantitative gates
        # pass.  This repository's current DB trigger intentionally rejects a
        # handmade PASS, so the request remains non-authoritative.
        blockers.append("EXTERNAL_SIGNED_ATTESTATION_REQUIRED")
        blockers = list(dict.fromkeys(blockers))
        request_payload = {
            "release_id": artifact.release_id,
            "artifact_hash": artifact.artifact_hash,
            "artifact_evidence_hash": artifact_evidence[
                artifact.horizon_days
            ].evidence_hash,
            "learning_run_id": learning_run.get("learning_run_id"),
            "outcome_checkpoint_hash": checkpoint_record.evidence_hash,
            "artifact_outcome_cohort_hash": horizon_checkpoint.get(
                "evidence_hash"
            ),
            "policy_hash": orchestration_policy_hash,
            "config_hash": current_config_hash,
            "code_version": current_code_version,
            "blockers": blockers,
            "requested_stage": "PAPER_ELIGIBLE",
            "status": "BLOCKED",
            "order_authority": False,
            "real_order_allowed": False,
        }
        stored = store.put(
            "promotion_request",
            request_payload,
            created_at=now,
        )
        promotion.append({
            **request_payload,
            "promotion_request_hash": stored.evidence_hash,
        })

    cycle_payload = {
        "schema_version": CONTINUOUS_ORCHESTRATION_SCHEMA,
        "evaluated_at": now.isoformat(),
        "outcome_checkpoint_hash": checkpoint_record.evidence_hash,
        "artifact_evidence_hashes": {
            str(key): value.evidence_hash
            for key, value in sorted(artifact_evidence.items())
        },
        "retraining_request_ids": [
            item.get("request_id") for item in retraining if item.get("request_id")
        ],
        "promotion_request_hashes": [
            item["promotion_request_hash"] for item in promotion
        ],
        "policy_hash": orchestration_policy_hash,
        "config_hash": current_config_hash,
        "code_version": current_code_version,
        "code_version_kind": current_code_kind,
        "forward_evidence_progress": forward_evidence_progress,
        "observed_outcome_count": observed_checkpoint_count,
        "forward_eligible_outcome_count": (
            forward_eligible_checkpoint_count
        ),
        "order_authority": False,
    }
    cycle_record = store.put(
        "cycle_result",
        cycle_payload,
        created_at=now,
    )
    demoted = _persist_auto_demotion(
        repository,
        evidence_hash=cycle_record.evidence_hash,
        current_config_hash=current_config_hash,
        evaluated_at=now,
    )
    if discovery.rejected:
        lifecycle_status = "BLOCKED"
    elif (
        {item["horizon_days"] for item in artifact_rows} != {1, 5, 20}
        or any(item["status"] == "BLOCKED" for item in artifact_rows)
    ):
        lifecycle_status = "BLOCKED"
    elif any(item["blockers"] for item in promotion):
        # Only forward-outcome / external PAPER evidence accumulation is a
        # healthy COLLECTING state.  Artifact integrity, gate and receipt
        # failures are terminal for this cycle and are handled above.
        lifecycle_status = "COLLECTING"
    else:
        lifecycle_status = "READY"
    return {
        "schema_version": CONTINUOUS_ORCHESTRATION_SCHEMA,
        "status": lifecycle_status,
        "evaluated_at": now.isoformat(),
        "policy_hash": orchestration_policy_hash,
        "outcome_checkpoint": checkpoint,
        "forward_evidence_progress": forward_evidence_progress,
        "observed_outcome_count": observed_checkpoint_count,
        "forward_eligible_outcome_count": (
            forward_eligible_checkpoint_count
        ),
        "artifacts": artifact_rows,
        "artifact_registrations": registrations,
        "artifact_rejections": list(discovery.rejected),
        "retraining": retraining,
        "promotion": promotion,
        "automatic_demotions": demoted,
        "cycle_evidence_hash": cycle_record.evidence_hash,
        "external_signed_attestation_required": True,
        "paper_eligible_granted": False,
        "order_authority": False,
        "real_order_allowed": False,
    }


__all__ = [
    "ArtifactDiscovery",
    "ContinuousCalibrationAlreadyRunning",
    "ContinuousCalibrationError",
    "FilesystemHorizonModelAdapter",
    "FROZEN_ORCHESTRATION_POLICY",
    "HorizonModelLifecycleAdapter",
    "ImmutableEvidenceStore",
    "RetrainingRequest",
    "RetrainingSubmission",
    "StoredEvidence",
    "VerifiedHorizonArtifact",
    "continuous_cycle_lock",
    "run_continuous_calibration_orchestration",
]
