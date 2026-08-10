"""Executable namespace isolation for V4 model and calibration artifacts."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from server.trading_v4.domain import (
    V4_ARTIFACT_NAMESPACE,
    CalibrationArtifactRef,
    DataManifest,
    DecisionClock,
    DecisionContext,
    ModelArtifactRef,
)


AS_OF = datetime(2026, 8, 3, 6, 30, tzinfo=timezone.utc)
AVAILABLE_AT = AS_OF - timedelta(days=1)
TRAINING_CUTOFF = AS_OF - timedelta(days=30)


def _context(
    *,
    model_version: str = "v4:model-1",
    calibration_version: str = "v4:calibration-1",
) -> DecisionContext:
    return DecisionContext(
        decision_time=AS_OF,
        decision_clock=DecisionClock.INTRADAY,
        knowledge_cutoff=AS_OF,
        trade_date=date(2026, 8, 3),
        universe_version="pit-universe-v1",
        data_manifest=DataManifest(record_hashes={"record-1": "a" * 64}),
        portfolio_policy_version="portfolio-v1",
        execution_contract_version="execution-v1",
        fee_schedule_version="fees-v1",
        account_snapshot_id="account-snapshot-1",
        code_commit_sha="b" * 40,
        config_hash="c" * 64,
        random_seed=1,
        model_versions={"stock": model_version},
        model_artifact_hashes={"stock": "d" * 64},
        model_training_cutoffs={"stock": TRAINING_CUTOFF},
        model_available_at={"stock": AVAILABLE_AT},
        calibration_versions={"stock": calibration_version},
        calibration_artifact_hashes={"stock": "e" * 64},
        calibration_training_cutoffs={"stock": TRAINING_CUTOFF},
        calibration_available_at={"stock": AVAILABLE_AT},
    )


def test_v4_artifact_namespace_is_explicit_and_context_enforced():
    assert V4_ARTIFACT_NAMESPACE == "v4:"
    assert _context().model_versions["stock"].startswith(
        V4_ARTIFACT_NAMESPACE
    )
    with pytest.raises(ValueError, match="namespace"):
        _context(model_version="v3:model-1")
    with pytest.raises(ValueError, match="namespace"):
        _context(calibration_version="legacy-calibration-1")


def test_v4_artifact_refs_cannot_alias_legacy_versions():
    common = {
        "artifact_hash": "f" * 64,
        "training_cutoff": TRAINING_CUTOFF,
        "forecast_contract_id": "next-session-v1",
        "promoted_at": AVAILABLE_AT,
        "status": "ACTIVE",
    }
    with pytest.raises(ValueError, match="namespace"):
        ModelArtifactRef(
            model_id="stock",
            model_version="v3:model-1",
            feature_spec_version="feature-v1",
            calibration_artifact_hash="1" * 64,
            **common,
        )
    with pytest.raises(ValueError, match="namespace"):
        CalibrationArtifactRef(
            calibration_id="stock-calibration",
            calibration_version="v3:calibration-1",
            model_id="stock",
            model_version="v4:model-1",
            **common,
        )

