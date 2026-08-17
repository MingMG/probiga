from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.trading_v3.config import PROJECT_ROOT, config_hash, load_v3_config
from server.trading_v3.continuous_calibration import (
    ArtifactDiscovery,
    ContinuousCalibrationAlreadyRunning,
    ContinuousCalibrationError,
    FilesystemHorizonModelAdapter,
    ImmutableEvidenceStore,
    RetrainingRequest,
    RetrainingSubmission,
    VerifiedHorizonArtifact,
    _ProcessBoundTrainingReceipt,
    continuous_cycle_lock,
    run_continuous_calibration_orchestration,
)
from server.trading_v3.horizon_candidate_ledger_schema import (
    CANDIDATE_EVALUATION_LEDGER_SCHEMA,
    CURRENT_HORIZON_ARTIFACT_SCHEMA,
    CURRENT_HORIZON_MODEL_PROTOCOL,
    CURRENT_HORIZON_SELECTION_POLICY_HASH,
    CURRENT_HORIZON_SELECTION_PROTOCOL,
    CURRENT_HORIZON_SUITE_SCHEMA,
    HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
)
from server.trading_v3.horizon_models import canonical_hash
from server.trading_v3.shadow_intelligence_repository import (
    ShadowIntelligenceRepository,
)
from server.trading_v3.versioning import code_version


UTC = timezone.utc


def _digest(label: str) -> str:
    import hashlib

    return hashlib.sha256(label.encode()).hexdigest()


def test_immutable_evidence_is_idempotent_and_detects_rewrite(tmp_path):
    store = ImmutableEvidenceStore(tmp_path)
    observed = datetime(2026, 8, 16, 8, tzinfo=UTC)
    first = store.put("cycle_result", {"status": "ok"}, created_at=observed)
    second = store.put("cycle_result", {"status": "ok"}, created_at=observed)
    assert first.evidence_hash == second.evidence_hash
    assert first.created is True
    assert second.created is False

    path = tmp_path / "cycle_result" / f"{first.evidence_hash}.json"
    path.chmod(0o644)
    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ContinuousCalibrationError, match="HASH_MISMATCH"):
        store.records("cycle_result")


def _artifact_manifest(*, horizon: int = 1) -> dict:
    version, _kind = code_version()
    model_key = f"independent-t{horizon}"
    model_version = "2026.08.16-oos"
    suite_release_id = "suite-2026-08-16"
    release_id = (
        f"{suite_release_id}:{model_key}:{model_version}:T+{horizon}"
    )
    training_config = load_v3_config()["multi_horizon_forecasts"][
        "training_policy"
    ]
    training_window = {
        "protocol": training_config["training_window_protocol"],
        "configured_history_start": training_config["history_start"],
        "signal_start": training_config["history_start"],
        "signal_start_inclusive": True,
        "signal_end": "2026-08-15",
        "signal_end_inclusive": True,
        "status": "FROZEN_DEFAULT_TRAINING_WINDOW",
        "is_current_config_default": True,
    }
    training_window["training_window_hash"] = canonical_hash(training_window)
    return {
        "schema_version": CURRENT_HORIZON_ARTIFACT_SCHEMA,
        "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
        "release_id": release_id,
        "suite_release_id": suite_release_id,
        "model_key": model_key,
        "model_version": model_version,
        "horizon_days": horizon,
        "prediction_kind": "CALIBRATED_OOS",
        "artifact_hash": _digest(f"artifact-{horizon}"),
        "config_hash": config_hash(),
        "code_version": version,
        "code_hash": hashlib.sha256(
            (
                PROJECT_ROOT
                / "server"
                / "trading_v3"
                / "horizon_models.py"
            ).read_bytes()
        ).hexdigest(),
        "feature_protocol_hash": _digest(f"feature-{horizon}"),
        "calibration_protocol_hash": _digest(f"calibration-{horizon}"),
        "dataset_hash": _digest(f"dataset-{horizon}"),
        "training_window": training_window,
        "training_cutoff": "2026-08-15",
        "created_at": "2026-08-16T07:00:00+00:00",
        "valid_until": "2026-09-15",
        "candidate_evaluation_ledger": {
            "schema_version": CANDIDATE_EVALUATION_LEDGER_SCHEMA,
            "binding_protocol": (
                "FULL_PREQUENTIAL_OOS_CANDIDATE_LEDGER_CONTENT_ADDRESS_V1"
            ),
            "encoding": "DETERMINISTIC_GZIP_CANONICAL_JSONL_V1",
            "hash_algorithm": "SHA256_COMPRESSED_BYTES",
            "content_sha256": _digest(f"candidate-ledger-{horizon}"),
            "canonical_records_sha256": _digest(
                f"candidate-records-{horizon}"
            ),
            "relative_path": (
                "candidate-ledgers/sha256/"
                f"{_digest(f'candidate-ledger-{horizon}')[:2]}/"
                f"{_digest(f'candidate-ledger-{horizon}')}.jsonl.gz"
            ),
            "compressed_size_bytes": 4096,
            "row_count": 100,
            "session_count": 80,
            "evaluation_row_count": 100,
            "evaluation_session_count": 80,
            "fold_count": 3,
            "header_hash": _digest(f"candidate-header-{horizon}"),
            "reference_hash": _digest(f"candidate-reference-{horizon}"),
            "registration_verification_required": True,
        },
        "selection_policy": {
            "selection_policy_hash": CURRENT_HORIZON_SELECTION_POLICY_HASH,
        },
        "oos_evidence": {
            "evidence_hash": _digest(f"oos-{horizon}"),
            "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
            "economic_metrics_use_frozen_selection_ledger": True,
            "training_window": training_window,
            "training_window_status": "FROZEN_DEFAULT_TRAINING_WINDOW",
            "training_window_is_current_config_default": True,
            "selection_evidence": {
                "protocol": CURRENT_HORIZON_SELECTION_PROTOCOL,
                "selection_policy_hash": (
                    CURRENT_HORIZON_SELECTION_POLICY_HASH
                ),
                "selected_oos_sample_count": 100,
                "selected_oos_session_count": 80,
                "deployment_candidate_domain_verified": False,
                "order_authority": False,
            },
        },
        "execution_feasibility": {
            "status": "UNVERIFIED_RESEARCH",
            "provenance_status": "UNVERIFIED_PREVIEW",
            "attestation_hash": _digest(f"execution-{horizon}"),
        },
        "gate_status": "BLOCK",
        "contract_eligible": False,
        "order_authority": False,
    }


def test_continuous_manifest_rejects_nondefault_training_window(tmp_path):
    manifest = _artifact_manifest()
    window = dict(manifest["training_window"])
    window.update({
        "signal_start": "2024-01-02",
        "status": "NON_DEFAULT_TRAINING_WINDOW",
        "is_current_config_default": False,
    })
    manifest["training_window"] = window
    manifest["oos_evidence"]["training_window"] = copy.deepcopy(window)
    manifest["oos_evidence"]["training_window_status"] = (
        "NON_DEFAULT_TRAINING_WINDOW"
    )
    manifest["oos_evidence"][
        "training_window_is_current_config_default"
    ] = False
    with pytest.raises(
        ContinuousCalibrationError,
        match="MODEL_ARTIFACT_TRAINING_WINDOW_NOT_CURRENT",
    ):
        VerifiedHorizonArtifact.from_manifest(
            manifest,
            path=tmp_path / "T1.json",
        )


def _suite_manifest(suite_release_id: str) -> dict:
    return {
        "schema_version": CURRENT_HORIZON_SUITE_SCHEMA,
        "model_protocol": CURRENT_HORIZON_MODEL_PROTOCOL,
        "suite_release_id": suite_release_id,
    }


def test_filesystem_discovery_requires_verified_artifact_loader(
    tmp_path, monkeypatch
):
    release_root = tmp_path / "release"
    release_root.mkdir(parents=True)
    manifests = {
        horizon: _artifact_manifest(horizon=horizon)
        for horizon in (1, 5, 20)
    }
    for horizon, manifest in manifests.items():
        (release_root / f"T{horizon}.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
    (release_root / "suite.json").write_text(
        json.dumps(_suite_manifest("suite-2026-08-16")),
        encoding="utf-8",
    )
    adapter = FilesystemHorizonModelAdapter(tmp_path)
    monkeypatch.setattr(
        adapter,
        "_loader",
        lambda: lambda path, **_kwargs: manifests[
            int(path.stem.removeprefix("T"))
        ],
    )
    monkeypatch.setattr(
        adapter,
        "_suite_loader",
        lambda: lambda _path, **_kwargs: {
            "suite_release_id": "suite-2026-08-16"
        },
    )

    result = adapter.discover(
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC)
    )
    assert result.loader_available is True
    assert len(result.artifacts) == 3
    assert result.artifacts[0].artifact_hash == manifests[1]["artifact_hash"]
    assert all(
        item.valid_until
        == datetime(2026, 9, 15, 23, 59, 59, 999999, tzinfo=UTC)
        for item in result.artifacts
    )
    assert result.rejected == ()

    monkeypatch.setattr(adapter, "_loader", lambda: None)
    rejected = adapter.discover(
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC)
    )
    assert not rejected.artifacts
    assert rejected.rejected[0]["reason"].endswith("LOADER_UNAVAILABLE")


def test_filesystem_discovery_keeps_v1_suite_audit_only(tmp_path, monkeypatch):
    release_root = tmp_path / "historical-v1"
    release_root.mkdir()
    (release_root / "suite.json").write_text(json.dumps({
        "schema_version": HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
        "suite_release_id": "historical-v1",
        "order_authority": False,
    }), encoding="utf-8")
    adapter = FilesystemHorizonModelAdapter(tmp_path)
    monkeypatch.setattr(adapter, "_loader", lambda: lambda *_a, **_k: None)
    monkeypatch.setattr(
        adapter, "_suite_loader", lambda: lambda *_a, **_k: None
    )
    result = adapter.discover(
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC)
    )
    assert result.artifacts == ()
    assert result.rejected == ({
        "path": str((release_root / "suite.json").resolve()),
        "reason_code": "HISTORICAL_PROTOCOL_AUDIT_ONLY",
        "reason": "HISTORICAL_PROTOCOL_AUDIT_ONLY",
        "schema_version": HISTORICAL_HORIZON_SUITE_SCHEMA_V1,
        "runtime_eligible": False,
        "order_authority": False,
    },)


def test_filesystem_discovery_selects_one_complete_latest_suite(
    tmp_path, monkeypatch
):
    manifests = {}
    for suite_id, cutoff, created in (
        ("suite-a", "2026-08-14", "2026-08-15T07:00:00+00:00"),
        ("suite-b", "2026-08-15", "2026-08-16T07:00:00+00:00"),
    ):
        root = tmp_path / suite_id
        root.mkdir()
        (root / "suite.json").write_text(
            json.dumps(_suite_manifest(suite_id)), encoding="utf-8"
        )
        for horizon in (1, 5, 20):
            manifest = _artifact_manifest(horizon=horizon)
            manifest["suite_release_id"] = suite_id
            manifest["release_id"] = (
                f"{suite_id}:{manifest['model_key']}:"
                f"{manifest['model_version']}:T+{horizon}"
            )
            manifest["training_cutoff"] = cutoff
            training_window = dict(manifest["training_window"])
            training_window["signal_end"] = cutoff
            training_window["training_window_hash"] = canonical_hash({
                key: value
                for key, value in training_window.items()
                if key != "training_window_hash"
            })
            manifest["training_window"] = training_window
            manifest["oos_evidence"]["training_window"] = copy.deepcopy(
                training_window
            )
            manifest["created_at"] = created
            manifest["artifact_hash"] = _digest(
                f"{suite_id}-artifact-{horizon}"
            )
            manifests[(suite_id, horizon)] = manifest
            (root / f"T{horizon}.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )

    adapter = FilesystemHorizonModelAdapter(tmp_path)
    monkeypatch.setattr(
        adapter,
        "_loader",
        lambda: lambda path, **_kwargs: manifests[
            (path.parent.name, int(path.stem.removeprefix("T")))
        ],
    )
    monkeypatch.setattr(
        adapter,
        "_suite_loader",
        lambda: lambda path, **_kwargs: {
            "suite_release_id": path.parent.name
        },
    )

    result = adapter.discover(
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC)
    )
    assert {item.horizon_days for item in result.artifacts} == {1, 5, 20}
    assert {item.suite_release_id for item in result.artifacts} == {"suite-b"}
    assert result.rejected == ()


class _LifecycleRepository:
    def __init__(self, checkpoint):
        self.checkpoint = checkpoint
        self.release_calls = 0
        self.registration_hashes = {}

    def calibration_outcome_checkpoint(self, *, evaluation_date):
        assert evaluation_date == date(2026, 8, 16)
        return self.checkpoint

    def verified_learning_run(self, _learning_id):
        return None

    def latest_calibration_gate(self, _release_id):
        return None

    def release_audit(self):
        return {"releases": []}

    def save_horizon_model_artifact(
        self,
        artifact,
        *,
        registration_evidence_hash,
        training_receipt=None,
        artifact_root=None,
    ):
        assert artifact_root is not None
        artifact_hash = artifact["artifact_hash"]
        prior_hash = self.registration_hashes.get(artifact_hash)
        if (
            prior_hash is not None
            and prior_hash != registration_evidence_hash
        ):
            raise RuntimeError("HORIZON_ARTIFACT_REGISTRY_IDEMPOTENCY_CONFLICT")
        self.registration_hashes[artifact_hash] = registration_evidence_hash
        receipt_status = (
            "PROCESS_VERIFIED"
            if type(training_receipt) is _ProcessBoundTrainingReceipt
            else "UNVERIFIED"
        )
        status = (
            "OOS_VERIFIED"
            if (
                dict(artifact.get("gate") or {}).get("status") == "PASS"
                or artifact.get("gate_status") == "PASS"
            )
            and receipt_status == "PROCESS_VERIFIED"
            else "BLOCKED"
        )
        return {
            "artifact_id": artifact_hash,
            "release_id": artifact["release_id"],
            "artifact_status": status,
            "training_receipt_status": receipt_status,
            "training_receipt_hash": (
                dict(training_receipt.receipt).get("receipt_hash")
                if type(training_receipt) is _ProcessBoundTrainingReceipt
                else _digest(f"unverified-{artifact_hash}")
            ),
            "inserted": prior_hash is None,
            "candidate_ledger_schema_version": (
                CANDIDATE_EVALUATION_LEDGER_SCHEMA
            ),
            "candidate_ledger_content_sha256": dict(
                artifact["candidate_evaluation_ledger"]
            )["content_sha256"],
            "candidate_ledger_row_count": dict(
                artifact["candidate_evaluation_ledger"]
            )["row_count"],
            "ledger_registration_evidence_hash": (
                _digest(f"ledger-registration-{artifact_hash}")
                if receipt_status == "PROCESS_VERIFIED"
                else None
            ),
            "registration_verification_hash": (
                _digest(f"registration-verification-{artifact_hash}")
                if receipt_status == "PROCESS_VERIFIED"
                else None
            ),
            "order_authority": False,
        }

    def ensure_release(self, **kwargs):
        self.release_calls += 1
        return {
            **kwargs,
            "release_state_id": "release-state-1",
            "current_stage": "DRAFT",
        }

    def append_release_transition(self, *, release_id, transition, **kwargs):
        del kwargs
        return {
            "release_id": release_id,
            "release_state_id": "release-state-2",
            "current_stage": transition.next_stage,
        }

    def publish_horizon_suite_shadow(
        self, *, suite_release_id, members, config_hash, occurred_at
    ):
        del config_hash, occurred_at
        rows = list(members)
        assert {int(item["horizon_days"]) for item in rows} == {1, 5, 20}
        assert {str(item["suite_release_id"]) for item in rows} == {
            suite_release_id
        }
        self.release_calls += 3
        return {
            "suite_release_id": suite_release_id,
            "releases_by_horizon": {
                int(item["horizon_days"]): {
                    **item,
                    "release_state_id": f"release-{item['horizon_days']}",
                    "current_stage": "SHADOW",
                    "order_authority": False,
                }
                for item in rows
            },
            "order_authority": False,
        }


class _CapturingAdapter:
    def __init__(self, artifacts=()):
        self.artifacts = tuple(artifacts)
        self.requests = []

    def discover(self, *, evaluated_at):
        assert evaluated_at.tzinfo is not None
        return ArtifactDiscovery(self.artifacts, (), True)

    def submit_retraining(
        self, request, *, primary_engine, market_engine, evaluated_at
    ):
        del primary_engine, market_engine, evaluated_at
        self.requests.append(request)
        return RetrainingSubmission(
            request_id=request.request_id,
            status="TRAINING_CLI_SUCCEEDED_ARTIFACT_PENDING_REDISCOVERY",
            external_job_id="job-1",
        )


class _ReceiptAdapter(_CapturingAdapter):
    def __init__(self, artifacts=()):
        super().__init__(artifacts)
        self.capability = _ProcessBoundTrainingReceipt(
            receipt={
                "status": "PROCESS_VERIFIED",
                "receipt_hash": _digest("fake-process-receipt"),
            },
            process_nonce="test-process-nonce",
            capability_mac="test-process-mac",
        )

    def training_receipt(self, *, suite_release_id):
        assert suite_release_id == "suite-2026-08-16"
        return self.capability


def _verified_manifest(*, horizon: int, gate_status: str = "PASS") -> dict:
    manifest = _artifact_manifest(horizon=horizon)
    manifest.pop("gate_status")
    reasons = [] if gate_status == "PASS" else ["OOS_GATE_FAILED"]
    manifest["gate"] = {
        "status": gate_status,
        "block_reasons": reasons,
        "contract_eligible": gate_status == "PASS",
        "automatic_promotion_allowed": False,
        "external_signed_attestation_required": True,
        "order_authority": False,
    }
    manifest["contract_eligible"] = gate_status == "PASS"
    manifest["execution_feasibility"] = {
        "status": "RESEARCH_LABEL_PROTOCOL_VERIFIED",
        "provenance": "SELF_VERIFIED_RESEARCH_ARTIFACT",
        "execution_evidence_scope": "LONG_HISTORY_OOS_RESEARCH_ONLY",
        "executable_verified": False,
        "attestation_hash": _digest(f"research-execution-{horizon}"),
    }
    return manifest


def _checkpoint(*, samples: int, sessions: int) -> dict:
    by_horizon = {
        str(horizon): {
            "horizon_days": horizon,
            "sample_count": samples,
            "distinct_decision_session_count": sessions,
            "forward_eligible_sample_count": samples,
            "forward_eligible_decision_session_count": sessions,
            "executable_verified_count": 0,
            "unverified_research_count": samples,
            "evidence_hash": _digest(
                f"checkpoint-{horizon}-{samples}-{sessions}"
            ),
            "forward_evidence_hash": _digest(
                f"forward-checkpoint-{horizon}-{samples}-{sessions}"
            ),
        }
        for horizon in (1, 5, 20)
    }
    return {
        "schema_version": "checkpoint.v1",
        "evaluation_date": "2026-08-16",
        "sample_count": samples * 3,
        "forward_eligible_sample_count": samples * 3,
        "distinct_decision_session_count": sessions,
        "evidence_hash": _digest(f"checkpoint-{samples}-{sessions}"),
        "by_horizon": by_horizon,
        "by_artifact_hash": {
            _artifact_manifest(horizon=horizon)["artifact_hash"]: {
                **by_horizon[str(horizon)],
                "model_artifact_hash": _artifact_manifest(
                    horizon=horizon
                )["artifact_hash"],
            }
            for horizon in (1, 5, 20)
        },
        "execution_feasibility": "UNVERIFIED_RESEARCH",
        "order_authority": False,
    }


def test_unverified_artifact_forces_controlled_retraining_despite_sample_count(
    tmp_path,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifacts = tuple(
        VerifiedHorizonArtifact.from_manifest(
            _artifact_manifest(horizon=horizon),
            path=tmp_path / f"T{horizon}.json",
        )
        for horizon in (1, 5, 20)
    )
    adapter = _CapturingAdapter(artifacts)
    repository = _LifecycleRepository(
        _checkpoint(samples=10000, sessions=6)
    )
    store = ImmutableEvidenceStore(tmp_path)
    result = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=store,
    )
    repeated = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 9, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=store,
    )
    assert {item.horizon_days for item in adapter.requests} == {1, 5, 20}
    assert {item.request_kind for item in adapter.requests} == {"FULL_RETRAIN"}
    assert result["status"] == "BLOCKED"
    assert all(
        item["status"]
        == "TRAINING_CLI_SUCCEEDED_ARTIFACT_PENDING_REDISCOVERY"
        for item in result["retraining"]
    )
    assert all(
        item["distinct_decision_session_count"] == 6
        for item in result["retraining"]
    )
    assert result["artifact_registrations"][0]["registry_status"] == "BLOCKED"
    assert all(
        item["status"] == "BLOCKED"
        for item in result["artifact_registrations"]
    )
    assert result["artifact_registrations"][0]["publication_blockers"]
    assert repository.release_calls == 0
    assert len(adapter.requests) == 3
    assert all(
        item["status"] == "NOT_DUE"
        and item["reason_code"] == "SAME_CHECKPOINT_REQUEST_REUSED"
        for item in repeated["retraining"]
    )
    assert len(store.records("retrain_request")) == 3
    assert len(store.records("retrain_submission")) == 3
    engine.dispose()


def test_manual_oos_pass_artifact_is_blocked_without_process_receipt(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    manifest = _artifact_manifest()
    manifest.pop("gate_status")
    manifest["gate"] = {
        "status": "PASS",
        "block_reasons": [],
        "contract_eligible": True,
        "automatic_promotion_allowed": False,
        "external_signed_attestation_required": True,
        "order_authority": False,
    }
    manifest["contract_eligible"] = True
    manifest["execution_feasibility"] = {
        "status": "RESEARCH_LABEL_PROTOCOL_VERIFIED",
        "provenance": "SELF_VERIFIED_RESEARCH_ARTIFACT",
        "execution_evidence_scope": "LONG_HISTORY_OOS_RESEARCH_ONLY",
        "executable_verified": False,
        "attestation_hash": _digest("research-execution"),
    }
    artifact = VerifiedHorizonArtifact.from_manifest(
        manifest, path=tmp_path / "T1.json"
    )
    repository = _LifecycleRepository(_checkpoint(samples=10, sessions=10))
    result = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=_CapturingAdapter((artifact,)),
        evidence_store=ImmutableEvidenceStore(tmp_path / "evidence"),
    )

    registration = result["artifact_registrations"][0]
    assert registration["registry_status"] == "BLOCKED"
    assert registration["status"] == "BLOCKED"
    assert registration["training_receipt_status"] == "UNVERIFIED"
    assert "TRAINING_RECEIPT_UNVERIFIED" in registration["publication_blockers"]
    assert repository.release_calls == 0

    promotion = result["promotion"][0]
    assert promotion["status"] == "BLOCKED"
    assert "FORWARD_OUTCOMES_NOT_EXECUTABLE_VERIFIED" in promotion["blockers"]
    assert "EXECUTION_FEASIBILITY_UNVERIFIED" in promotion["blockers"]
    assert "EXECUTION_ATTESTATION_NOT_PERSISTED_VERIFIED" in promotion["blockers"]
    assert "LATEST_PERSISTED_PASS_GATE_REQUIRED" in promotion["blockers"]
    assert "EXTERNAL_SIGNED_ATTESTATION_REQUIRED" in promotion["blockers"]
    assert result["paper_eligible_granted"] is False
    assert result["order_authority"] is False
    engine.dispose()


def test_shadow_publication_is_atomic_across_all_three_horizons(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifacts = tuple(
        VerifiedHorizonArtifact.from_manifest(
            _verified_manifest(
                horizon=horizon,
                gate_status="BLOCK" if horizon == 20 else "PASS",
            ),
            path=tmp_path / f"T{horizon}.json",
        )
        for horizon in (1, 5, 20)
    )
    repository = _LifecycleRepository(_checkpoint(samples=0, sessions=0))
    adapter = _ReceiptAdapter(artifacts)
    result = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=ImmutableEvidenceStore(tmp_path / "evidence"),
    )

    assert repository.release_calls == 0
    assert all(
        item["status"] == "BLOCKED"
        and "HORIZON_SUITE_NOT_ATOMICALLY_VERIFIED"
        in item["publication_blockers"]
        for item in result["artifact_registrations"]
    )
    assert adapter.requests == []
    assert all(
        item["status"] == "NOT_DUE" for item in result["retraining"]
    )
    engine.dispose()


def test_blocked_suite_retrains_once_only_after_new_forward_checkpoint(
    tmp_path,
):
    class _QueuedAdapter(_ReceiptAdapter):
        def submit_retraining(
            self, request, *, primary_engine, market_engine, evaluated_at
        ):
            del primary_engine, market_engine, evaluated_at
            self.requests.append(request)
            return RetrainingSubmission(
                request_id=request.request_id,
                status="TRAINING_JOB_QUEUED",
                external_job_id=f"queued-{request.horizon_days}",
            )

    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifacts = tuple(
        VerifiedHorizonArtifact.from_manifest(
            _verified_manifest(horizon=horizon, gate_status="BLOCK"),
            path=tmp_path / f"T{horizon}.json",
        )
        for horizon in (1, 5, 20)
    )
    repository = _LifecycleRepository(
        _checkpoint(samples=200, sessions=250)
    )
    adapter = _QueuedAdapter(artifacts)
    store = ImmutableEvidenceStore(tmp_path / "evidence")

    first = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=store,
    )
    second = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 9, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=store,
    )

    assert len(adapter.requests) == 3
    assert all(
        item["status"] == "TRAINING_JOB_QUEUED"
        for item in first["retraining"]
    )
    assert all(
        item["status"] == "NOT_DUE"
        and item["new_outcome_count"] == 0
        and item["new_decision_session_count"] == 0
        for item in second["retraining"]
    )
    assert len(store.records("retrain_request")) == 3
    assert len(store.records("retrain_submission")) == 3
    assert first["order_authority"] is False
    assert second["paper_eligible_granted"] is False
    engine.dispose()


def test_complete_receipt_backed_suite_publishes_in_one_repository_call(
    tmp_path,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifacts = tuple(
        VerifiedHorizonArtifact.from_manifest(
            _verified_manifest(horizon=horizon),
            path=tmp_path / f"T{horizon}.json",
        )
        for horizon in (1, 5, 20)
    )
    repository = _LifecycleRepository(_checkpoint(samples=0, sessions=0))
    result = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=_ReceiptAdapter(artifacts),
        evidence_store=ImmutableEvidenceStore(tmp_path / "evidence"),
    )

    assert repository.release_calls == 3
    assert all(
        item["status"] == "PUBLISHED_SHADOW"
        and not item["publication_blockers"]
        for item in result["artifact_registrations"]
    )
    engine.dispose()


def test_artifact_registration_evidence_is_stable_across_cycles(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifacts = tuple(
        VerifiedHorizonArtifact.from_manifest(
            _artifact_manifest(horizon=horizon),
            path=tmp_path / f"T{horizon}.json",
        )
        for horizon in (1, 5, 20)
    )
    repository = _LifecycleRepository(_checkpoint(samples=0, sessions=0))
    adapter = _CapturingAdapter(artifacts)
    store = ImmutableEvidenceStore(tmp_path / "evidence")

    first = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=store,
    )
    second = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 9, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=store,
    )

    assert [
        item["registration_request_hash"]
        for item in first["artifact_registrations"]
    ] == [
        item["registration_request_hash"]
        for item in second["artifact_registrations"]
    ]
    assert all(
        item["inserted"] is False
        for item in second["artifact_registrations"]
    )
    assert len(repository.registration_hashes) == 3
    engine.dispose()


def test_registration_evidence_does_not_drift_when_process_receipt_disappears(
    tmp_path,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifacts = tuple(
        VerifiedHorizonArtifact.from_manifest(
            _verified_manifest(horizon=horizon),
            path=tmp_path / f"T{horizon}.json",
        )
        for horizon in (1, 5, 20)
    )
    repository = _LifecycleRepository(_checkpoint(samples=0, sessions=0))
    store = ImmutableEvidenceStore(tmp_path / "evidence")
    first = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=_ReceiptAdapter(artifacts),
        evidence_store=store,
    )
    restarted = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 9, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=_CapturingAdapter(artifacts),
        evidence_store=store,
    )

    assert [
        item["registration_request_hash"]
        for item in first["artifact_registrations"]
    ] == [
        item["registration_request_hash"]
        for item in restarted["artifact_registrations"]
    ]
    engine.dispose()


def test_artifact_registry_failure_aborts_cycle_fail_closed(tmp_path):
    class _FailingRegistryRepository(_LifecycleRepository):
        def save_horizon_model_artifact(self, *args, **kwargs):
            del args, kwargs
            raise RuntimeError("HORIZON_ARTIFACT_REGISTRY_WRITE_FAILED")

    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifact = VerifiedHorizonArtifact.from_manifest(
        _artifact_manifest(), path=tmp_path / "T1.json"
    )
    with pytest.raises(
        RuntimeError, match="HORIZON_ARTIFACT_REGISTRY_WRITE_FAILED"
    ):
        run_continuous_calibration_orchestration(
            _FailingRegistryRepository(_checkpoint(samples=0, sessions=0)),
            engine,
            engine,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
            learning_run={"learning_run_id": "learning-1"},
            lifecycle_adapter=_CapturingAdapter((artifact,)),
            evidence_store=ImmutableEvidenceStore(tmp_path / "evidence"),
        )
    engine.dispose()


def test_missing_artifacts_trigger_full_retrain_only_after_time_evidence(
    tmp_path,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    adapter = _CapturingAdapter()
    result = run_continuous_calibration_orchestration(
        _LifecycleRepository(_checkpoint(samples=0, sessions=0)),
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=adapter,
        evidence_store=ImmutableEvidenceStore(tmp_path),
    )
    assert {item.horizon_days for item in adapter.requests} == {1, 5, 20}
    assert {item.request_kind for item in adapter.requests} == {"FULL_RETRAIN"}
    assert all(item.order_authority is False for item in adapter.requests)
    assert result["paper_eligible_granted"] is False
    assert result["status"] == "BLOCKED"
    engine.dispose()


def test_verified_artifact_still_blocks_without_execution_and_persisted_gate(
    tmp_path,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    artifact = VerifiedHorizonArtifact.from_manifest(
        _artifact_manifest(), path=tmp_path / "T1.json"
    )
    result = run_continuous_calibration_orchestration(
        _LifecycleRepository(_checkpoint(samples=0, sessions=0)),
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=_CapturingAdapter((artifact,)),
        evidence_store=ImmutableEvidenceStore(tmp_path / "evidence"),
    )
    promotion = result["promotion"][0]
    assert "EXECUTION_FEASIBILITY_UNVERIFIED" in promotion["blockers"]
    assert "LATEST_PERSISTED_PASS_GATE_REQUIRED" in promotion["blockers"]
    assert "EXTERNAL_SIGNED_ATTESTATION_REQUIRED" in promotion["blockers"]
    assert promotion["status"] == "BLOCKED"
    assert promotion["order_authority"] is False
    engine.dispose()


def test_non_mysql_cycle_lock_reports_already_running():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with continuous_cycle_lock(engine):
        with pytest.raises(
            ContinuousCalibrationAlreadyRunning,
            match="ALREADY_RUNNING",
        ):
            with continuous_cycle_lock(engine):
                pass
    engine.dispose()


def test_invalid_effective_paper_release_is_persistently_demoted(tmp_path):
    class _DemotionRepository(_LifecycleRepository):
        def __init__(self):
            super().__init__(_checkpoint(samples=0, sessions=0))
            self.transitions = []

        def release_audit(self):
            return {"releases": [{
                "release_id": "model:v:T+1",
                "audit_stage": "PAPER_ELIGIBLE",
                "effective_stage": "BLOCKED",
            }]}

        def latest_release(self, release_id):
            return {
                "release_id": release_id,
                "current_stage": "PAPER_ELIGIBLE",
            }

        def append_release_transition(self, **kwargs):
            self.transitions.append(kwargs["transition"])
            return {
                "release_state_id": "demoted-1",
                "current_stage": kwargs["transition"].next_stage,
            }

    engine = create_engine("sqlite+pysqlite:///:memory:")
    repository = _DemotionRepository()
    result = run_continuous_calibration_orchestration(
        repository,
        engine,
        engine,
        config=load_v3_config(),
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
        learning_run={"learning_run_id": "learning-1"},
        lifecycle_adapter=_CapturingAdapter(),
        evidence_store=ImmutableEvidenceStore(tmp_path),
    )
    assert result["automatic_demotions"][0]["stage"] == "BLOCKED"
    assert repository.transitions[0].event == "AUTOMATIC_EVIDENCE_DEMOTION"
    assert repository.transitions[0].order_authority is False
    engine.dispose()


def test_counterfactual_cli_reports_lock_conflict_as_nonzero(
    monkeypatch, capsys
):
    import sys
    import tools.run_trading_v3_counterfactual as runner

    class _Engine:
        def dispose(self):
            pass

    monkeypatch.setattr(runner, "load_project_env", lambda: None)
    monkeypatch.setattr(runner, "create_tool_engine", _Engine)
    monkeypatch.setattr(runner, "get_kline_engine", _Engine)
    monkeypatch.setattr(
        runner,
        "drain_counterfactual_backlog",
        lambda *args, **kwargs: {"status": "ok"},
    )

    def _busy(*args, **kwargs):
        raise ContinuousCalibrationAlreadyRunning(
            "CONTINUOUS_CALIBRATION_ALREADY_RUNNING"
        )

    monkeypatch.setattr(runner, "run_shadow_intelligence_cycle", _busy)
    monkeypatch.setattr(sys, "argv", ["run_trading_v3_counterfactual.py"])
    assert runner.main() == 75
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ALREADY_RUNNING"
    assert payload["order_authority"] is False


def test_shadow_cycle_persists_failure_status_and_reraises(tmp_path, monkeypatch):
    import server.trading_v3.shadow_intelligence_worker as worker

    engine = create_engine("sqlite+pysqlite:///:memory:")
    store = ImmutableEvidenceStore(tmp_path)

    def _fail(*args, **kwargs):
        raise RuntimeError("TRAINING_CLI_FAILED")

    monkeypatch.setattr(worker, "_run_shadow_intelligence_cycle_unlocked", _fail)
    with pytest.raises(RuntimeError, match="TRAINING_CLI_FAILED"):
        worker.run_shadow_intelligence_cycle(
            engine,
            engine,
            evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
            evidence_store=store,
        )
    failure = store.records("cycle_failure")[0]["payload"]
    assert failure["error_type"] == "RuntimeError"
    assert failure["error_code"] == "TRAINING_CLI_FAILED"
    assert failure["order_authority"] is False
    engine.dispose()


def test_default_adapter_calls_full_universe_training_cli_without_shell(
    tmp_path, monkeypatch
):
    approved_root = (
        PROJECT_ROOT / "artifacts" / "trading_v3" / "horizon_models"
    ).resolve()
    adapter = FilesystemHorizonModelAdapter(approved_root)
    current_version = code_version()[0]
    monkeypatch.setattr(
        "server.trading_v3.continuous_calibration.secrets.token_hex",
        lambda _size: "deadbeef",
    )
    suite_release_id = (
        f"shadow-auto-20260816-{config_hash()[:12]}-"
        f"{current_version[:12]}-deadbeef"
    )
    manifests = []
    for horizon in (1, 5, 20):
        manifest = _artifact_manifest(horizon=horizon)
        manifest["suite_release_id"] = suite_release_id
        manifest["release_id"] = (
            f"{suite_release_id}:{manifest['model_key']}:"
            f"{manifest['model_version']}:T+{horizon}"
        )
        manifest["training_cutoff"] = "2026-08-16"
        manifests.append(manifest)
    artifacts = tuple(
        VerifiedHorizonArtifact.from_manifest(
            manifest,
            path=tmp_path / "release" / f"T{horizon}.json",
        )
        for horizon, manifest in zip((1, 5, 20), manifests, strict=True)
    )
    monkeypatch.setattr(
        adapter,
        "discover",
        lambda **_kwargs: ArtifactDiscovery(artifacts, (), True),
    )
    calls = []
    stdout_payload = {
        "status": "BLOCK",
        "release_id": suite_release_id,
        "release_root": str((approved_root / suite_release_id).resolve()),
        "reused_immutable_release": False,
        "universe_scope": "FULL_A_SHARE_POINT_IN_TIME",
        "training_window_protocol": manifests[0]["training_window"][
            "protocol"
        ],
        "configured_history_start": manifests[0]["training_window"][
            "configured_history_start"
        ],
        "signal_start": manifests[0]["training_window"]["signal_start"],
        "training_window_status": "FROZEN_DEFAULT_TRAINING_WINDOW",
        "models": manifests,
        "automatic_promotion_allowed": False,
        "order_authority": False,
    }
    stdout_text = json.dumps(stdout_payload)

    def _run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout=stdout_text,
            stderr="",
        )

    monkeypatch.setattr(
        "server.trading_v3.continuous_calibration.subprocess.run",
        _run,
    )
    request = RetrainingRequest(
        request_id=_digest("request"),
        request_kind="FULL_RETRAIN",
        horizon_days=1,
        release_id="unassigned:T+1",
        prior_artifact_hash=None,
        outcome_checkpoint_hash=_digest("checkpoint"),
        outcome_sample_count=0,
        new_outcome_count=0,
        distinct_decision_session_count=0,
        new_decision_session_count=0,
        policy_hash=_digest("policy"),
        config_hash=config_hash(),
        code_version=code_version()[0],
        requested_at="2026-08-16T08:00:00+00:00",
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    submission = adapter.submit_retraining(
        request,
        primary_engine=engine,
        market_engine=engine,
        evaluated_at=datetime(2026, 8, 16, 8, tzinfo=UTC),
    )
    command, kwargs = calls[0]
    assert command[command.index("--start") + 1] == "2023-01-01"
    assert command[command.index("--max-stocks") + 1] == "0"
    assert "shell" not in kwargs
    assert kwargs["check"] is False
    assert submission.status == "TRAINING_CLI_SUCCEEDED"
    assert submission.order_authority is False
    detail = json.loads(submission.detail)
    assert detail["trainer_argv"] == command
    assert detail["trainer_exit_code"] == 0
    assert detail["trainer_stdout_sha256"] == _digest(
        stdout_text
    )
    assert detail["trainer_result_hash"] == submission.external_job_id
    assert detail["order_authority"] is False
    capability = adapter.training_receipt(suite_release_id=suite_release_id)
    assert type(capability) is _ProcessBoundTrainingReceipt
    engine.dispose()


def test_continuous_calibration_has_independent_bounded_scheduler_task():
    from server.api import scheduler_runtime
    from server.common.scheduler_args import build_scheduler_task_args
    from tools.add_trading_v3_tasks import TASKS

    task = next(
        item for item in TASKS
        if item["task_type"] == "trading_v3_continuous_calibration"
    )
    assert task["cron_time"] == "17:10"
    assert task["script_path"] == (
        "tools/run_trading_v3_continuous_calibration.py"
    )
    assert "--training-timeout-seconds 19800" in task["script_args"]
    assert task["enabled"] == 1
    assert build_scheduler_task_args(
        task, task["script_path"], "2026-08-16"
    ) == [
        "--lock-timeout-seconds",
        "0",
        "--training-timeout-seconds",
        "19800",
    ]
    assert scheduler_runtime._task_timeout_minutes(task) == (
        scheduler_runtime.LONG_TASK_TIMEOUT_MINUTES
    )
    assert scheduler_runtime._should_skip_non_trading_day(
        task, object()
    ) is False


@pytest.mark.parametrize(
    ("result_status", "progress", "expected_exit"),
    [
        ("READY", "EMPTY", 0),
        ("COLLECTING", "VERIFIED_PROGRESS", 0),
        ("COLLECTING", "EMPTY", 3),
        ("BLOCKED", "VERIFIED_PROGRESS", 2),
    ],
)
def test_continuous_calibration_cli_exit_semantics(
    result_status, progress, expected_exit, monkeypatch, capsys
):
    import sys
    import tools.run_trading_v3_continuous_calibration as runner

    class _Engine:
        def dispose(self):
            pass

    monkeypatch.setattr(runner, "load_project_env", lambda: None)
    monkeypatch.setattr(runner, "create_tool_engine", _Engine)
    monkeypatch.setattr(runner, "get_kline_engine", _Engine)
    monkeypatch.setattr(
        runner,
        "run_continuous_model_lifecycle_cycle",
        lambda *args, **kwargs: {
            "status": result_status,
            "forward_evidence_progress": progress,
            "order_authority": False,
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_trading_v3_continuous_calibration.py"],
    )
    assert runner.main() == expected_exit
    assert json.loads(capsys.readouterr().out)["status"] == result_status


def test_continuous_calibration_cli_error_and_lock_are_nonzero(
    monkeypatch, capsys
):
    import sys
    import tools.run_trading_v3_continuous_calibration as runner

    class _Engine:
        def dispose(self):
            pass

    monkeypatch.setattr(runner, "load_project_env", lambda: None)
    monkeypatch.setattr(runner, "create_tool_engine", _Engine)
    monkeypatch.setattr(runner, "get_kline_engine", _Engine)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_trading_v3_continuous_calibration.py"],
    )

    def _error(*args, **kwargs):
        raise RuntimeError("TRAINING_ERROR")

    monkeypatch.setattr(
        runner, "run_continuous_model_lifecycle_cycle", _error
    )
    assert runner.main() == 1
    error = json.loads(capsys.readouterr().err)
    assert error["status"] == "ERROR"

    def _busy(*args, **kwargs):
        raise ContinuousCalibrationAlreadyRunning(
            "CONTINUOUS_CALIBRATION_ALREADY_RUNNING"
        )

    monkeypatch.setattr(
        runner, "run_continuous_model_lifecycle_cycle", _busy
    )
    assert runner.main() == 75
    busy = json.loads(capsys.readouterr().out)
    assert busy["status"] == "ALREADY_RUNNING"


def test_v3_task_upsert_includes_continuous_calibration(monkeypatch, capsys):
    import tools.add_trading_v3_tasks as registration

    class _Engine:
        def dispose(self):
            pass

    observed = []
    monkeypatch.setattr(registration, "load_project_env", lambda: None)
    monkeypatch.setattr(registration, "create_tool_engine", _Engine)
    monkeypatch.setattr(
        registration,
        "upsert_scheduler_task",
        lambda _engine, task, **_kwargs: observed.append(task["task_type"])
        or {"action": "inserted"},
    )
    monkeypatch.setattr(
        registration,
        "enforce_layer4_writer_fence_atomically",
        lambda _engine: 0,
    )
    assert registration.main([]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert "trading_v3_continuous_calibration" in observed
    assert len(payload["tasks"]) == len(registration.TASKS)


def test_repository_checkpoint_counts_sessions_not_only_cross_section():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_horizon_forecast_contract_v3 ("
            "contract_id TEXT, contract_hash TEXT, run_uid TEXT, "
            "model_key TEXT, model_version TEXT, model_artifact_hash TEXT, "
            "horizon_days INT, decision_session_date DATE, prediction_kind TEXT)"
        ))
        connection.execute(text(
            "CREATE TABLE st_horizon_outcome_v3 ("
            "outcome_id TEXT, outcome_hash TEXT, contract_id TEXT, "
            "market_evidence_hash TEXT, exit_trade_date DATE, "
            "market_evidence_json TEXT, execution_feasibility TEXT, "
            "outcome_status TEXT, observed_at TEXT)"
        ))
        for index in range(100):
            contract_id = f"contract-{index}"
            connection.execute(text(
                "INSERT INTO st_horizon_forecast_contract_v3 VALUES "
                "(:id, :hash, 'run-1', 'm', 'v', :artifact, 1, "
                "'2026-08-10', 'PROXY_SCORE')"
            ), {
                "id": contract_id,
                "hash": _digest(f"contract-{index}"),
                "artifact": _digest("artifact"),
            })
            connection.execute(text(
                "INSERT INTO st_horizon_outcome_v3 VALUES "
                "(:id, :hash, :contract, :market, '2026-08-11', '{}', "
                "'UNVERIFIED_RESEARCH', 'MATURED_VERIFIED', :observed)"
            ), {
                "id": f"outcome-{index}",
                "hash": _digest(f"outcome-{index}"),
                "contract": contract_id,
                "market": canonical_hash({}),
                "observed": "2026-08-12T00:00:00+00:00",
            })
    checkpoint = ShadowIntelligenceRepository(
        engine
    ).calibration_outcome_checkpoint(evaluation_date=date(2026, 8, 16))
    assert checkpoint["by_horizon"]["1"]["sample_count"] == 100
    assert checkpoint["by_horizon"]["1"][
        "distinct_decision_session_count"
    ] == 1
    artifact_cohort = checkpoint["by_artifact_hash"][_digest("artifact")]
    assert artifact_cohort["sample_count"] == 100
    assert artifact_cohort["distinct_decision_session_count"] == 1
    assert artifact_cohort["horizon_days"] == 1
    assert checkpoint["execution_feasibility"] == "UNVERIFIED_RESEARCH"
    engine.dispose()
