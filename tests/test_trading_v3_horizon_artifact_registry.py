from __future__ import annotations

import copy
import hashlib
import json
import sys
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from server.trading_v3.horizon_contracts import (
    CalibrationEvidence,
    HorizonForecastContract,
)
from server.trading_v3.config import PROJECT_ROOT, config_hash
from server.trading_v3.continuous_calibration import (
    PROCESS_TRAINING_RECEIPT_SCHEMA,
    _issue_process_bound_training_receipt,
)
from server.trading_v3.horizon_models import (
    HORIZON_MODEL_SPECS,
    HorizonModelError,
    HorizonTrainingPolicy,
    _artifact_core_payload,
    build_horizon_dataset,
    canonical_hash,
    horizon_governance_release_id,
    predict_horizon_artifact,
    train_independent_horizon_model,
    write_horizon_artifact,
)
from server.trading_v3.horizon_candidate_ledger_schema import (
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
    HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
)
from server.trading_v3.shadow_intelligence_repository import (
    ShadowIntelligenceRepository,
    _hash,
)
from server.trading_v3.shadow_intelligence_schema import SHADOW_INTELLIGENCE_DDL


UTC = timezone.utc
SHANGHAI = ZoneInfo("Asia/Shanghai")
_LEDGER_ROOTS: dict[str, Path] = {}


def _artifact_root(artifact: dict) -> Path:
    digest = str(artifact["candidate_evaluation_ledger"]["content_sha256"])
    return _LEDGER_ROOTS[digest]


def _bars(
    *,
    sessions: int,
    stocks: int,
    start: str = "2024-01-02",
) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=sessions)
    rows = []
    for stock in range(stocks):
        code = f"{stock + 1:06d}"
        previous = 10.0 + stock * 0.15
        for index, day in enumerate(dates):
            # Keep the registry's positive-control artifact economically
            # selectable after the V2 gate switched from the unconditional
            # market baseline to the frozen expected-net/probability ledger.
            common = 0.0020 * np.sin(index / 9.0) + 0.0035
            stock_wave = 0.0015 * np.cos(index / 5.0 + stock / 4.0)
            open_price = previous * (
                1.0 + 0.0005 * np.sin(index / 4.0 + stock)
            )
            close_price = open_price * (1.0 + common + stock_wave)
            rows.append({
                "stock_code": code,
                "trade_date": day,
                "open": open_price,
                "high": max(open_price, close_price) * 1.006,
                "low": min(open_price, close_price) * 0.994,
                "close": close_price,
                "pre_close": previous,
                "amount": 50_000_000.0 * (
                    1.0 + 0.25 * np.sin(index / 6.0 + stock)
                ),
                "change_pct": (close_price / previous - 1.0) * 100.0,
                "data_source": "gj_big_qmt_inner",
                "quality_status": "QMT_ATTESTED" if index >= 70 else "RAW",
            })
            previous = close_price
    return pd.DataFrame(rows)


def _relaxed_policy() -> HorizonTrainingPolicy:
    return HorizonTrainingPolicy(
        minimum_mature_samples={1: 20, 5: 20, 20: 20},
        minimum_oos_samples={1: 20, 5: 20, 20: 20},
        minimum_train_sessions={1: 50, 5: 60, 20: 80},
        minimum_oos_sessions={1: 15, 5: 15, 20: 20},
        walk_forward_fold_count=3,
        minimum_direction_rank_correlation=-1.0,
        maximum_calibration_mae=1.0,
        maximum_brier_score=1.0,
        maximum_population_stability_index=1.0,
        minimum_net_expectancy_after_cost_pct=-999.0,
        minimum_profit_factor=0.0,
        minimum_cost_coverage_ratio=0.0,
        minimum_maturity_coverage=0.5,
        calibration_bucket_count=5,
    )


def _release_id(horizon: int, suite_release_id: str) -> str:
    spec = HORIZON_MODEL_SPECS[horizon]
    return horizon_governance_release_id(
        suite_release_id=suite_release_id,
        model_key=spec.model_key,
        model_version=spec.model_version,
        horizon_days=horizon,
    )


def _process_receipt(artifact: dict):
    suite = artifact["suite_release_id"]
    current_horizon = int(artifact["horizon_days"])
    hashes = {
        str(horizon): (
            artifact["artifact_hash"]
            if horizon == current_horizon
            else hashlib.sha256(
                f"{suite}:sibling:{horizon}".encode()
            ).hexdigest()
        )
        for horizon in (1, 5, 20)
    }
    models = []
    for horizon in (1, 5, 20):
        is_current = horizon == current_horizon
        model_key = artifact["model_key"] if is_current else f"sibling-t{horizon}"
        model_version = artifact["model_version"]
        models.append({
            "schema_version": artifact["schema_version"],
            "model_protocol": artifact["model_protocol"],
            "horizon_days": horizon,
            "artifact_hash": hashes[str(horizon)],
            "release_id": (
                artifact["release_id"]
                if is_current
                else f"{suite}:{model_key}:{model_version}:T+{horizon}"
            ),
            "suite_release_id": suite,
            "model_key": model_key,
            "model_version": model_version,
            "config_hash": artifact["config_hash"],
            "code_version": artifact["code_version"],
            "code_hash": artifact["code_hash"],
            "training_cutoff": artifact["training_cutoff"],
            "created_at": artifact["created_at"],
            "gate_status": "PASS",
            "candidate_evaluation_ledger": copy.deepcopy(
                artifact["candidate_evaluation_ledger"]
            ),
            "training_window": copy.deepcopy(artifact["training_window"]),
        })
    approved_root = (
        PROJECT_ROOT / "artifacts" / "trading_v3" / "horizon_models"
    ).resolve()
    stdout = {
        "status": "PASS",
        "release_id": suite,
        "release_root": str((approved_root / suite).resolve()),
        "reused_immutable_release": False,
        "universe_scope": "FULL_A_SHARE_POINT_IN_TIME",
        "training_window_protocol": artifact["training_window"]["protocol"],
        "configured_history_start": artifact["training_window"][
            "configured_history_start"
        ],
        "signal_start": artifact["training_window"]["signal_start"],
        "training_window_status": artifact["training_window"]["status"],
        "models": models,
        "automatic_promotion_allowed": False,
        "order_authority": False,
    }
    stdout_text = json.dumps(stdout, ensure_ascii=False)
    trainer_script = (
        PROJECT_ROOT / "tools" / "train_trading_v3_horizon_models.py"
    ).resolve()
    body = {
        "schema_version": PROCESS_TRAINING_RECEIPT_SCHEMA,
        "status": "PROCESS_VERIFIED",
        "suite_release_id": suite,
        "artifact_hashes": hashes,
        "config_hash": artifact["config_hash"],
        "code_version": artifact["code_version"],
        "artifact_code_hash": artifact["code_hash"],
        "training_cutoff": artifact["training_cutoff"],
        "trainer_script": str(trainer_script),
        "trainer_script_hash": hashlib.sha256(
            trainer_script.read_bytes()
        ).hexdigest(),
        "argv": [
            sys.executable,
            str(trainer_script),
            "--start", "2023-01-01",
            "--end", artifact["training_cutoff"],
            "--training-cutoff", artifact["training_cutoff"],
            "--release-id", suite,
            "--output-root", str(approved_root),
            "--max-stocks", "0",
        ],
        "exit_code": 0,
        "stdout_sha256": hashlib.sha256(stdout_text.encode()).hexdigest(),
        "stdout_text": stdout_text,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    return _issue_process_bound_training_receipt(body)


@pytest.fixture(scope="module")
def verified_artifact(tmp_path_factory):
    bars = _bars(sessions=240, stocks=24, start="2022-10-24")
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=pd.Timestamp(calendar[-4]).date(),
    )
    cutoff = pd.Timestamp(calendar[-1]).date()
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_release_id(1, "registry-verified-suite"),
        suite_release_id="registry-verified-suite",
        training_cutoff=cutoff,
        policy=_relaxed_policy(),
        created_at=f"{cutoff.isoformat()}T00:00:00+00:00",
    )
    assert artifact["gate"]["status"] == "PASS"
    artifact_root = tmp_path_factory.mktemp("registry-ledger")
    write_horizon_artifact(artifact, artifact_root / "T1.json")
    _LEDGER_ROOTS[
        artifact["candidate_evaluation_ledger"]["content_sha256"]
    ] = artifact_root
    return artifact, dataset, cutoff


@pytest.fixture(scope="module")
def blocked_zero_fold_artifact():
    bars = _bars(sessions=100, stocks=40)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start=pd.Timestamp(calendar[-8]).date(),
        signal_end=pd.Timestamp(calendar[-3]).date(),
    )
    cutoff = pd.Timestamp(calendar[-1]).date()
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_release_id(1, "registry-blocked-suite"),
        suite_release_id="registry-blocked-suite",
        training_cutoff=cutoff,
        created_at=f"{cutoff.isoformat()}T00:00:00+00:00",
    )
    assert artifact["gate"]["status"] == "BLOCK"
    assert artifact["training_window"]["status"] == (
        "NON_DEFAULT_TRAINING_WINDOW"
    )
    assert artifact["oos_evidence"]["walk_forward_fold_count"] == 0
    assert artifact["oos_evidence"]["oos_sample_count"] == 0
    return artifact


def _registry_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_horizon_model_artifact_v3 (
                artifact_id TEXT PRIMARY KEY, release_id TEXT NOT NULL,
                suite_release_id TEXT NOT NULL, model_key TEXT NOT NULL,
                model_version TEXT NOT NULL, horizon_days INTEGER NOT NULL,
                prediction_kind TEXT NOT NULL, artifact_status TEXT NOT NULL,
                artifact_schema_version TEXT NOT NULL,
                model_protocol TEXT,
                selection_policy_hash TEXT,
                candidate_ledger_schema_version TEXT,
                candidate_ledger_content_sha256 TEXT,
                candidate_ledger_row_count INTEGER,
                ledger_registration_evidence_hash TEXT,
                registration_verification_hash TEXT,
                training_start DATE, training_end DATE,
                validation_start DATE, validation_end DATE,
                training_session_count INTEGER NOT NULL,
                oos_session_count INTEGER NOT NULL,
                matured_sample_count INTEGER NOT NULL,
                oos_sample_count INTEGER NOT NULL,
                walk_forward_fold_count INTEGER NOT NULL,
                direction_rank_correlation NUMERIC,
                calibration_mae NUMERIC, brier_score NUMERIC,
                population_stability_index NUMERIC,
                net_expectancy_after_cost_pct NUMERIC,
                profit_factor NUMERIC, cost_coverage_ratio NUMERIC,
                dataset_hash TEXT NOT NULL,
                feature_protocol_hash TEXT NOT NULL,
                model_artifact_hash TEXT NOT NULL UNIQUE,
                calibration_evidence_hash TEXT,
                registration_evidence_hash TEXT,
                training_receipt_status TEXT NOT NULL,
                training_receipt_hash TEXT NOT NULL,
                training_receipt_json TEXT NOT NULL,
                config_hash TEXT NOT NULL, code_version TEXT NOT NULL,
                artifact_json TEXT NOT NULL, metrics_json TEXT NOT NULL,
                block_reasons_json TEXT NOT NULL,
                evidence_valid_until DATETIME,
                order_authority INTEGER NOT NULL,
                created_at DATETIME NOT NULL
            )
        """))
    return engine


def _create_release_table(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_shadow_release_v3 (
                release_state_id TEXT PRIMARY KEY,
                release_id TEXT NOT NULL,
                transition_sequence INTEGER NOT NULL,
                model_key TEXT NOT NULL,
                model_version TEXT NOT NULL,
                horizon_days INTEGER NOT NULL,
                previous_stage TEXT NOT NULL,
                current_stage TEXT NOT NULL,
                release_event TEXT NOT NULL,
                transition_accepted INTEGER NOT NULL,
                reason_code TEXT NOT NULL,
                gate_evaluation_id TEXT,
                evidence_hash TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                transition_hash TEXT NOT NULL,
                order_authority INTEGER NOT NULL,
                occurred_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE (release_id, transition_sequence),
                UNIQUE (release_id, transition_hash)
            )
        """))


def _suite_publication_members(suite_release_id: str) -> list[dict]:
    return [
        {
            "suite_release_id": suite_release_id,
            "release_id": _release_id(horizon, suite_release_id),
            "model_key": HORIZON_MODEL_SPECS[horizon].model_key,
            "model_version": HORIZON_MODEL_SPECS[horizon].model_version,
            "horizon_days": horizon,
            "evidence_hash": hashlib.sha256(
                f"publish-{horizon}".encode()
            ).hexdigest(),
        }
        for horizon in (1, 5, 20)
    ]


def test_shadow_suite_publication_rolls_back_all_horizons_on_second_failure():
    engine = _registry_engine()
    _create_release_table(engine)
    repository = ShadowIntelligenceRepository(engine)
    members = _suite_publication_members("atomic-publication-suite")
    start_inserts = 0

    def _fail_second_start(
        _connection, _cursor, statement, _parameters, _context, _many
    ):
        nonlocal start_inserts
        if "INSERT INTO st_shadow_release_v3" not in statement:
            return
        start_inserts += 1
        if start_inserts == 2:
            raise RuntimeError("SIMULATED_SECOND_SHADOW_INSERT_FAILURE")

    event.listen(engine, "before_cursor_execute", _fail_second_start)
    with pytest.raises(
        RuntimeError, match="SIMULATED_SECOND_SHADOW_INSERT_FAILURE"
    ):
        repository.publish_horizon_suite_shadow(
            suite_release_id="atomic-publication-suite",
            members=members,
            config_hash=config_hash(),
            occurred_at=datetime.now(UTC),
        )
    event.remove(engine, "before_cursor_execute", _fail_second_start)
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_shadow_release_v3"
        )).scalar_one() == 0
    assert all(
        repository.latest_release(str(item["release_id"])) is None
        for item in members
    )

    published = repository.publish_horizon_suite_shadow(
        suite_release_id="atomic-publication-suite",
        members=members,
        config_hash=config_hash(),
        occurred_at=datetime.now(UTC),
    )
    assert {
        str(row["current_stage"])
        for row in published["releases_by_horizon"].values()
    } == {"SHADOW"}
    repository.publish_horizon_suite_shadow(
        suite_release_id="atomic-publication-suite",
        members=members,
        config_hash=config_hash(),
        occurred_at=datetime.now(UTC),
    )
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_shadow_release_v3"
        )).scalar_one() == 6
    engine.dispose()


def test_registry_persists_zero_fold_block_as_null_clock_truth(
    blocked_zero_fold_artifact,
):
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    saved = repository.save_horizon_model_artifact(
        blocked_zero_fold_artifact,
        registration_evidence_hash="d" * 64,
    )
    assert saved["artifact_status"] == "BLOCKED"
    assert saved["training_receipt_status"] == "UNVERIFIED"
    rows = repository.horizon_model_artifacts(artifact_status="BLOCKED")
    assert len(rows) == 1
    assert rows[0]["oos_sample_count"] == 0
    assert rows[0]["walk_forward_fold_count"] == 0
    assert rows[0]["training_start"] is None
    assert rows[0]["validation_start"] is None
    assert rows[0]["block_reasons"]
    assert repository.latest_verified_horizon_artifact(
        model_key=blocked_zero_fold_artifact["model_key"],
        horizon_days=1,
        decision_as_of=datetime.now(UTC),
    ) is None
    engine.dispose()


def test_registry_reverifies_json_projection_and_registration_idempotency(
    verified_artifact,
):
    artifact, _, cutoff = verified_artifact
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    first = repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="e" * 64,
        training_receipt=(capability := _process_receipt(artifact)),
        artifact_root=_artifact_root(artifact),
    )
    assert first["artifact_status"] == "OOS_VERIFIED"
    assert first["inserted"] is True
    same_process_retry = repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="e" * 64,
        training_receipt=capability,
        artifact_root=_artifact_root(artifact),
    )
    assert same_process_retry["existing"] is True
    assert same_process_retry["training_receipt_status"] == "PROCESS_VERIFIED"
    second = repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="e" * 64,
    )
    assert second["existing"] is True
    with pytest.raises(RuntimeError, match="IDEMPOTENCY_CONFLICT"):
        repository.save_horizon_model_artifact(
            artifact,
            registration_evidence_hash="f" * 64,
        )
    latest = repository.latest_verified_horizon_artifact(
        model_key=artifact["model_key"],
        horizon_days=1,
        decision_as_of=datetime.combine(cutoff, time(1), tzinfo=UTC),
    )
    assert latest is not None
    assert latest["artifact"]["artifact_hash"] == artifact["artifact_hash"]
    assert date.fromisoformat(str(latest["training_start"])) == date.fromisoformat(
        artifact["final_model"]["training_start"]
    )
    assert date.fromisoformat(str(latest["training_end"])) == date.fromisoformat(
        artifact["final_model"]["training_end"]
    )

    tampered = copy.deepcopy(artifact)
    tampered["oos_evidence"]["profit_factor"] += 1.0
    with pytest.raises(HorizonModelError):
        repository.save_horizon_model_artifact(tampered)

    stale = copy.deepcopy(artifact)
    stale["created_at"] = (
        date.fromisoformat(stale["valid_until"]) + timedelta(days=1)
    ).isoformat() + "T00:00:00+00:00"
    stale["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": stale["artifact_hash"],
        "created_at": stale["created_at"],
    })
    with pytest.raises(RuntimeError, match="EXPIRED_AT_CREATION"):
        repository.save_horizon_model_artifact(
            stale, training_receipt=_process_receipt(stale)
        )

    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_horizon_model_artifact_v3 "
            "SET training_session_count = training_session_count + 1"
        ))
    with pytest.raises(RuntimeError, match="PROJECTION_MISMATCH"):
        repository.horizon_model_artifacts()
    engine.dispose()


def test_training_receipt_is_not_consumed_when_registry_commit_fails(
    verified_artifact,
):
    artifact, _, _ = verified_artifact
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    capability = _process_receipt(artifact)

    def _fail_commit(_connection):
        raise RuntimeError("SIMULATED_REGISTRY_COMMIT_FAILURE")

    event.listen(engine, "commit", _fail_commit)
    with pytest.raises(RuntimeError, match="SIMULATED_REGISTRY_COMMIT_FAILURE"):
        repository.save_horizon_model_artifact(
            artifact,
            registration_evidence_hash="c" * 64,
            training_receipt=capability,
            artifact_root=_artifact_root(artifact),
        )
    event.remove(engine, "commit", _fail_commit)
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_horizon_model_artifact_v3"
        )).scalar_one() == 0

    retried = repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="c" * 64,
        training_receipt=capability,
        artifact_root=_artifact_root(artifact),
    )
    assert retried["inserted"] is True
    assert retried["training_receipt_status"] == "PROCESS_VERIFIED"
    engine.dispose()


def test_process_registration_requires_ledger_root_and_preserves_capability(
    verified_artifact,
):
    artifact, _, _ = verified_artifact
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    capability = _process_receipt(artifact)
    with pytest.raises(RuntimeError, match="LEDGER_ROOT_REQUIRED"):
        repository.save_horizon_model_artifact(
            artifact,
            registration_evidence_hash="5" * 64,
            training_receipt=capability,
        )
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_horizon_model_artifact_v3"
        )).scalar_one() == 0
    retried = repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="5" * 64,
        training_receipt=capability,
        artifact_root=_artifact_root(artifact),
    )
    assert retried["training_receipt_status"] == "PROCESS_VERIFIED"
    assert retried["ledger_registration_evidence_hash"]
    assert retried["registration_verification_hash"]
    engine.dispose()


def test_corrupt_ledger_rejects_registration_without_consuming_capability(
    verified_artifact,
    tmp_path,
):
    artifact, _, _ = verified_artifact
    reference = dict(artifact["candidate_evaluation_ledger"])
    relative = Path(str(reference["relative_path"]))
    source = _artifact_root(artifact) / relative
    destination = tmp_path / relative
    destination.parent.mkdir(parents=True)
    original = source.read_bytes()
    destination.write_bytes(original[:-1] + bytes([original[-1] ^ 0x01]))
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    capability = _process_receipt(artifact)
    with pytest.raises(HorizonModelError, match="hash differs"):
        repository.save_horizon_model_artifact(
            artifact,
            registration_evidence_hash="6" * 64,
            training_receipt=capability,
            artifact_root=tmp_path,
        )
    destination.write_bytes(original)
    retried = repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="6" * 64,
        training_receipt=capability,
        artifact_root=tmp_path,
    )
    assert retried["artifact_status"] == "OOS_VERIFIED"
    assert retried["training_receipt_status"] == "PROCESS_VERIFIED"
    engine.dispose()


def test_self_asserted_pass_and_plain_mapping_receipt_cannot_enter_shadow(
    verified_artifact,
):
    artifact, _, cutoff = verified_artifact
    forged = copy.deepcopy(artifact)
    forged["oos_evidence"]["profit_factor"] = 99.0
    evidence = forged["oos_evidence"]
    evidence["evidence_hash"] = canonical_hash({
        key: value for key, value in evidence.items() if key != "evidence_hash"
    })
    forged["oos_evidence_hash"] = evidence["evidence_hash"]
    forged["artifact_hash"] = canonical_hash(_artifact_core_payload(forged))
    forged["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": forged["artifact_hash"],
        "created_at": forged["created_at"],
    })
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    # V2 rejects a self-rehashed economic aggregate before it reaches the
    # registry boundary because it no longer matches the selected ledger.
    with pytest.raises(HorizonModelError, match="OOS selected metric differs"):
        repository.save_horizon_model_artifact(forged)

    # A byte-perfect plain receipt mapping is still not the opaque, one-use
    # process capability issued after subprocess exit.
    fake_receipt = dict(_process_receipt(artifact).receipt)
    saved = repository.save_horizon_model_artifact(
        artifact,
        training_receipt=fake_receipt,
    )
    assert saved["artifact_status"] == "BLOCKED"
    assert saved["training_receipt_status"] == "UNVERIFIED"
    rows = repository.horizon_model_artifacts(artifact_status="BLOCKED")
    assert rows[0]["block_reasons"] == ["TRAINING_RECEIPT_UNVERIFIED"]
    assert repository.latest_verified_horizon_artifact(
        model_key=artifact["model_key"],
        horizon_days=1,
        decision_as_of=datetime.combine(cutoff, time(1), tzinfo=UTC),
    ) is None
    engine.dispose()


def test_registry_audits_old_config_but_never_returns_it_as_current_authority(
    verified_artifact,
):
    artifact, _, cutoff = verified_artifact
    historical = copy.deepcopy(artifact)
    historical["config_hash"] = "9" * 64
    historical["artifact_hash"] = canonical_hash(
        _artifact_core_payload(historical)
    )
    historical["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": historical["artifact_hash"],
        "created_at": historical["created_at"],
    })
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    saved = repository.save_horizon_model_artifact(historical)
    assert saved["artifact_status"] == "BLOCKED"
    assert len(repository.horizon_model_artifacts()) == 1
    assert repository.latest_verified_horizon_artifact(
        model_key=historical["model_key"],
        horizon_days=1,
        decision_as_of=datetime.combine(cutoff, time(1), tzinfo=UTC),
    ) is None
    engine.dispose()


def test_registry_reads_v1_as_audit_only_and_excludes_it_from_runtime(
    blocked_zero_fold_artifact,
):
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    saved = repository.save_horizon_model_artifact(
        blocked_zero_fold_artifact
    )
    historical = copy.deepcopy(blocked_zero_fold_artifact)
    historical["schema_version"] = HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1
    historical.pop("model_protocol", None)
    core = {
        key: value
        for key, value in historical.items()
        if key not in {"artifact_hash", "created_at", "creation_envelope_hash"}
    }
    historical["artifact_hash"] = _hash(core)
    historical["creation_envelope_hash"] = _hash({
        "artifact_hash": historical["artifact_hash"],
        "created_at": historical["created_at"],
    })
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_horizon_model_artifact_v3 "
            "SET artifact_id=:historical_hash, "
            "model_artifact_hash=:historical_hash, "
            "artifact_schema_version=:schema_version, "
            "model_protocol=NULL, selection_policy_hash=NULL, "
            "candidate_ledger_schema_version=NULL, "
            "candidate_ledger_content_sha256=NULL, "
            "candidate_ledger_row_count=NULL, "
            "ledger_registration_evidence_hash=NULL, "
            "registration_verification_hash=NULL, "
            "artifact_json=:artifact_json "
            "WHERE artifact_id=:current_hash"
        ), {
            "historical_hash": historical["artifact_hash"],
            "schema_version": HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1,
            "artifact_json": json.dumps(historical),
            "current_hash": saved["artifact_id"],
        })
    rows = repository.horizon_model_artifacts()
    assert len(rows) == 1
    assert rows[0]["protocol_status"] == "HISTORICAL_AUDIT_ONLY"
    assert rows[0]["runtime_eligible"] is False
    assert rows[0]["order_authority"] is False
    assert repository.latest_verified_horizon_artifact(
        model_key=historical["model_key"],
        horizon_days=1,
        decision_as_of=datetime.now(UTC),
    ) is None
    engine.dispose()


def test_registry_reads_pre_ledger_v2_process_row_as_audit_only(
    verified_artifact,
):
    artifact, _, _ = verified_artifact
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    saved = repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="7" * 64,
        training_receipt=_process_receipt(artifact),
        artifact_root=_artifact_root(artifact),
    )
    historical = copy.deepcopy(artifact)
    historical["schema_version"] = HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2
    historical.pop("candidate_evaluation_ledger", None)
    core = {
        key: value
        for key, value in historical.items()
        if key not in {"artifact_hash", "created_at", "creation_envelope_hash"}
    }
    historical["artifact_hash"] = _hash(core)
    historical["creation_envelope_hash"] = _hash({
        "artifact_hash": historical["artifact_hash"],
        "created_at": historical["created_at"],
    })
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_horizon_model_artifact_v3 "
            "SET artifact_id=:historical_hash, "
            "model_artifact_hash=:historical_hash, "
            "artifact_schema_version=:schema_version, "
            "candidate_ledger_schema_version=NULL, "
            "candidate_ledger_content_sha256=NULL, "
            "candidate_ledger_row_count=NULL, "
            "ledger_registration_evidence_hash=NULL, "
            "registration_verification_hash=NULL, "
            "artifact_json=:artifact_json "
            "WHERE artifact_id=:current_hash"
        ), {
            "historical_hash": historical["artifact_hash"],
            "schema_version": HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2,
            "artifact_json": json.dumps(historical),
            "current_hash": saved["artifact_id"],
        })
    rows = repository.horizon_model_artifacts()
    assert len(rows) == 1
    assert rows[0]["artifact_status"] == "OOS_VERIFIED"
    assert rows[0]["training_receipt_status"] == "PROCESS_VERIFIED"
    assert rows[0]["protocol_status"] == "PRE_LEDGER_V2_AUDIT_ONLY"
    assert rows[0]["runtime_eligible"] is False
    assert repository.latest_verified_horizon_artifact(
        model_key=historical["model_key"],
        horizon_days=1,
        decision_as_of=datetime.now(UTC),
    ) is None
    engine.dispose()


def test_latest_verified_skips_newer_stale_code_artifact(
    verified_artifact,
):
    artifact, _, cutoff = verified_artifact
    engine = _registry_engine()
    repository = ShadowIntelligenceRepository(engine)
    repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="1" * 64,
        training_receipt=_process_receipt(artifact),
        artifact_root=_artifact_root(artifact),
    )

    stale = copy.deepcopy(artifact)
    stale["code_version"] = "stale-code-version"
    stale["created_at"] = (
        datetime.fromisoformat(artifact["created_at"]) + timedelta(minutes=1)
    ).isoformat()
    stale["artifact_hash"] = canonical_hash(_artifact_core_payload(stale))
    stale["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": stale["artifact_hash"],
        "created_at": stale["created_at"],
    })
    repository.save_horizon_model_artifact(
        stale,
        registration_evidence_hash="2" * 64,
        training_receipt=_process_receipt(stale),
        artifact_root=_artifact_root(stale),
    )

    wrong_code_hash = copy.deepcopy(artifact)
    wrong_code_hash["code_hash"] = "f" * 64
    wrong_code_hash["created_at"] = (
        datetime.fromisoformat(artifact["created_at"])
        + timedelta(seconds=30)
    ).isoformat()
    wrong_code_hash["artifact_hash"] = canonical_hash(
        _artifact_core_payload(wrong_code_hash)
    )
    wrong_code_hash["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": wrong_code_hash["artifact_hash"],
        "created_at": wrong_code_hash["created_at"],
    })
    repository.save_horizon_model_artifact(
        wrong_code_hash,
        registration_evidence_hash="3" * 64,
        training_receipt=_process_receipt(wrong_code_hash),
        artifact_root=_artifact_root(wrong_code_hash),
    )

    selected = repository.latest_verified_horizon_artifact(
        model_key=artifact["model_key"],
        model_version=artifact["model_version"],
        horizon_days=1,
        decision_as_of=datetime.combine(cutoff, time(2), tzinfo=UTC),
    )
    assert selected is not None
    assert selected["artifact"]["artifact_hash"] == artifact["artifact_hash"]
    engine.dispose()


def test_explicit_shadow_release_must_match_artifact_suite_identity():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    repository = ShadowIntelligenceRepository(engine)
    spec = HORIZON_MODEL_SPECS[1]
    with pytest.raises(RuntimeError, match="ARTIFACT_RELEASE_IDENTITY_INVALID"):
        repository.ensure_release(
            model_key=spec.model_key,
            model_version=spec.model_version,
            horizon_days=1,
            release_id=_release_id(1, "suite-a"),
            suite_release_id="suite-b",
            config_hash="a" * 64,
            occurred_at=datetime(2026, 8, 16, tzinfo=UTC),
        )
    engine.dispose()


def _create_contract_tables(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_decision_run_v3 (
                run_uid TEXT PRIMARY KEY, status TEXT, decision_at DATETIME,
                result_hash TEXT, data_snapshot_hash TEXT, config_hash TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_alpha_forecast_v3 (
                forecast_id TEXT PRIMARY KEY, run_uid TEXT, stock_code TEXT,
                strategy_key TEXT, raw_score NUMERIC, feature_time DATETIME,
                valid_until DATETIME, forecast_status TEXT,
                model_version TEXT, dataset_hash TEXT,
                features_json TEXT, reasons_json TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_target_portfolio_v3 (
                target_id TEXT, run_uid TEXT, stock_code TEXT,
                strategy_keys_json TEXT, attribution_snapshot_hash TEXT,
                status TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_horizon_forecast_contract_v3 (
                contract_id TEXT PRIMARY KEY, source_forecast_id TEXT,
                run_uid TEXT, stock_code TEXT, model_key TEXT,
                model_version TEXT, source_strategy_key TEXT,
                source_forecast_hash TEXT, source_evidence_json TEXT,
                decision_result_hash TEXT, feature_protocol_hash TEXT,
                model_artifact_hash TEXT, model_inputs_json TEXT,
                selection_status TEXT, selection_reason_code TEXT,
                selection_evidence_hash TEXT, selection_evidence_json TEXT,
                horizon_days INTEGER, prediction_kind TEXT,
                decision_as_of DATETIME, feature_as_of DATETIME,
                decision_session_date DATE, entry_trade_date DATE,
                earliest_exit_trade_date DATE, outcome_matures_on DATE,
                entry_session_sequence INTEGER,
                earliest_exit_session_sequence INTEGER,
                outcome_maturity_session_sequence INTEGER, score NUMERIC,
                expected_return_net_pct NUMERIC,
                probability_positive NUMERIC, cost_assumption_pct NUMERIC,
                cost_model_version TEXT, calibration_evidence_hash TEXT,
                contract_hash TEXT, contract_json TEXT,
                decision_scope TEXT, order_authority INTEGER,
                created_at DATETIME,
                UNIQUE (
                    source_forecast_id, model_key, model_version,
                    horizon_days, model_artifact_hash
                )
            )
        """))


def test_calibrated_contract_is_rescored_from_registered_artifact(
    verified_artifact,
):
    artifact, dataset, cutoff = verified_artifact
    engine = _registry_engine()
    _create_contract_tables(engine)
    repository = ShadowIntelligenceRepository(engine)
    repository.save_horizon_model_artifact(
        artifact,
        registration_evidence_hash="4" * 64,
        training_receipt=_process_receipt(artifact),
        artifact_root=_artifact_root(artifact),
    )

    decision_at = datetime.combine(cutoff, time(15, 5), tzinfo=SHANGHAI)
    feature_at = datetime.combine(cutoff, time(15), tzinfo=SHANGHAI)
    source_forecast_id = "source-forecast-registry-1"
    run_uid = "run-registry-1"
    stock_code = "000001"
    strategy_key = "intraday_surprise"
    decision_result_hash = "a" * 64
    data_snapshot_hash = "b" * 64
    source_dataset_hash = "c" * 64
    sample = dataset.frame.iloc[-1]
    model = artifact["final_model"]
    model_inputs = {
        name: float(sample[name]) for name in model["features"]
    }
    features = dict(model_inputs)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_decision_run_v3 VALUES (
                :run_uid, 'COMPLETED', :decision_at, :result_hash,
                :data_snapshot_hash, :config_hash
            )
        """), {
            "run_uid": run_uid,
            "decision_at": decision_at.isoformat(),
            "result_hash": decision_result_hash,
            "data_snapshot_hash": data_snapshot_hash,
            "config_hash": artifact["config_hash"],
        })
        connection.execute(text("""
            INSERT INTO st_alpha_forecast_v3 VALUES (
                :forecast_id, :run_uid, :stock_code, :strategy_key, 0.5,
                :feature_time, :valid_until, 'ACTIVE', 'source-v1',
                :dataset_hash, :features_json, '[]'
            )
        """), {
            "forecast_id": source_forecast_id,
            "run_uid": run_uid,
            "stock_code": stock_code,
            "strategy_key": strategy_key,
            "feature_time": feature_at.isoformat(),
            "valid_until": (
                decision_at + timedelta(hours=1)
            ).isoformat(),
            "dataset_hash": source_dataset_hash,
            "features_json": json.dumps(features, sort_keys=True),
        })

    source_snapshot = {
        "forecast_id": source_forecast_id,
        "run_uid": run_uid,
        "stock_code": stock_code,
        "strategy_key": strategy_key,
        "source_model_version": "source-v1",
        "dataset_hash": source_dataset_hash,
        "forecast_status": "ACTIVE",
        "raw_score": 0.5,
        "feature_time": feature_at.isoformat(),
        "valid_until": (decision_at + timedelta(hours=1)).isoformat(),
        "features": features,
        "reasons_json": "[]",
        "decision_result_hash": decision_result_hash,
        "data_snapshot_hash": data_snapshot_hash,
        "decision_config_hash": artifact["config_hash"],
    }
    selection_snapshot = {
        "target_id": None,
        "target_status": None,
        "target_strategy_keys": [],
        "attribution_snapshot_hash": None,
        "selection_status": "REJECTED",
        "selection_reason_code": "NOT_SELECTED_IN_FROZEN_TARGET",
    }
    selection_evidence = {
        "source_forecast_id": source_forecast_id,
        "source_forecast_hash": _hash(source_snapshot),
        "run_uid": run_uid,
        "source_strategy_key": strategy_key,
        "decision_result_hash": decision_result_hash,
        "selection_snapshot": selection_snapshot,
    }
    prediction = predict_horizon_artifact(artifact, model_inputs)
    oos = artifact["oos_evidence"]
    execution = artifact["execution_feasibility"]
    calibration = CalibrationEvidence(
        evidence_id=artifact["oos_evidence_hash"],
        model_key=artifact["model_key"],
        model_version=artifact["model_version"],
        horizon_days=1,
        dataset_hash=artifact["dataset_hash"],
        feature_protocol_hash=artifact["feature_protocol_hash"],
        cost_model_version=execution["cost_model_version"],
        cost_assumption_pct=execution["cost_assumption_pct"],
        matured_sample_count=oos["matured_sample_count"],
        oos_sample_count=oos["oos_sample_count"],
        walk_forward_fold_count=oos["walk_forward_fold_count"],
        outcomes_include_costs=True,
        score_direction_valid=True,
        calibration_mae=oos["calibration_mae"],
        brier_score=oos["brier_score"],
        generated_at=datetime.fromisoformat(artifact["created_at"]),
        valid_until=datetime.combine(
            date.fromisoformat(artifact["valid_until"]),
            time(23, 59, 59, 999999),
            tzinfo=UTC,
        ),
    )
    entry_date = cutoff + timedelta(days=1)
    exit_date = cutoff + timedelta(days=2)
    contract = HorizonForecastContract(
        forecast_id=hashlib.sha256(b"calibrated-contract").hexdigest(),
        run_uid=run_uid,
        stock_code=stock_code,
        model_key=artifact["model_key"],
        model_version=artifact["model_version"],
        source_strategy_key=strategy_key,
        source_forecast_hash=_hash(source_snapshot),
        source_evidence=source_snapshot,
        decision_result_hash=decision_result_hash,
        feature_protocol_hash=artifact["feature_protocol_hash"],
        model_artifact_hash=artifact["artifact_hash"],
        model_inputs=model_inputs,
        selection_status="REJECTED",
        selection_reason_code="NOT_SELECTED_IN_FROZEN_TARGET",
        selection_evidence_hash=_hash(selection_evidence),
        selection_evidence=selection_evidence,
        horizon_days=1,
        prediction_kind="CALIBRATED_OOS",
        decision_as_of=decision_at,
        feature_as_of=feature_at,
        decision_session_date=cutoff,
        entry_trade_date=entry_date,
        earliest_exit_trade_date=exit_date,
        outcome_matures_on=exit_date,
        entry_session_sequence=1,
        earliest_exit_session_sequence=2,
        outcome_maturity_session_sequence=2,
        score=prediction.score,
        expected_return_net_pct=prediction.expected_return_net_pct,
        probability_positive=prediction.probability_positive,
        cost_assumption_pct=execution["cost_assumption_pct"],
        cost_model_version=execution["cost_model_version"],
        calibration_evidence=calibration,
    )
    saved = repository.save_horizon_contracts(
        [(contract, source_forecast_id)],
        created_at=decision_at + timedelta(minutes=1),
    )
    assert saved["inserted_count"] == 1
    with pytest.raises(RuntimeError, match="EVIDENCE_MISMATCH"):
        repository.save_horizon_contracts(
            [(replace(contract, score=contract.score + 0.01), source_forecast_id)],
            created_at=decision_at + timedelta(minutes=2),
        )
    engine.dispose()


def test_mysql_guard_contains_json_projection_and_immutable_boundaries():
    ddl = "\n".join(SHADOW_INTELLIGENCE_DDL)
    assert "suite_release_id" in ddl
    assert "$.artifact_hash" in ddl
    assert "$.oos_evidence.distinct_train_sessions" in ddl
    assert "$.gate.block_reasons" in ddl
    assert "<=> NEW.model_artifact_hash" in ddl
    assert "COALESCE(JSON_CONTAINS" in ddl
    assert "blocked model without folds must not invent validation dates" in ddl
    assert "horizon model artifact is immutable" in ddl
    assert "horizon model artifact cannot be deleted" in ddl
    assert "NEW.suite_release_id, CHAR(58), NEW.model_key" in ddl
    assert "training_receipt_status = 'PROCESS_VERIFIED'" in ddl
    assert "TRAINING_RECEIPT_UNVERIFIED" in ddl
    assert "$.final_model.training_end" in ddl
    assert "h.prediction_kind = 'CALIBRATED_OOS'" in ddl
    assert "calibrated outcome requires its current verified OOS artifact" in ddl
