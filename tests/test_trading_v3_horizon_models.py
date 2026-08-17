from __future__ import annotations

import copy
import gzip
import hashlib
import io
import json
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath

import numpy as np
import pandas as pd
import pytest

from server.trading_v3.horizon_models import (
    ARTIFACT_SCHEMA,
    CALIBRATION_PROTOCOL,
    CANDIDATE_EVALUATION_LEDGER_SCHEMA,
    CANDIDATE_LEDGER_BINDING_PROTOCOL,
    CONTRACT_ELIGIBILITY_SCOPE,
    DEFAULT_SELECTION_POLICY,
    DEFAULT_SELECTION_POLICY_HASH,
    HORIZON_MODEL_SPECS,
    HISTORICAL_ARTIFACT_SCHEMA_V1,
    HISTORICAL_ARTIFACT_SCHEMA_V2,
    HISTORICAL_SUITE_SCHEMA_V1,
    HISTORICAL_SUITE_SCHEMA_V2,
    MODEL_CODE_VERSION,
    MODEL_PROTOCOL,
    PSI_PROTOCOL,
    SCORE_NORMALIZATION_PROTOCOL,
    SELECTION_PROTOCOL,
    SUITE_SCHEMA,
    TRAINING_CONFIG_PROTOCOL,
    TRAINING_WINDOW_PROTOCOL,
    HorizonModelError,
    HorizonSelectionPolicy,
    HorizonTrainingPolicy,
    _artifact_core_payload,
    _build_selection_evidence,
    _fit_model,
    _fit_calibration,
    _normalized_model_score,
    _apply_calibration,
    _session_direction_evidence,
    _solve_weighted_ridge,
    _spearman,
    _verify_selection_evidence,
    _verify_calibration,
    artifact_manifest,
    build_horizon_dataset,
    canonical_hash,
    canonical_json,
    horizon_governance_release_id,
    load_horizon_artifact,
    load_horizon_suite,
    predict_horizon_artifact,
    train_horizon_suite,
    train_independent_horizon_model,
    verify_horizon_artifact,
    verify_candidate_evaluation_ledger,
    verify_horizon_suite,
    write_horizon_artifact,
    write_horizon_suite,
)
from server.trading_v3.config import config_hash, load_v3_config
from tools.train_trading_v3_horizon_models import (
    _existing_suite,
    _require_closed_training_cutoff,
    _resolve_training_start,
    _verify_existing_suite_request,
)


def test_same_session_training_waits_for_finalized_daily_bar():
    cutoff = pd.Timestamp("2026-08-17").date()
    with pytest.raises(RuntimeError, match="post-close daily bars"):
        _require_closed_training_cutoff(
            cutoff,
            observed_at=datetime.fromisoformat(
                "2026-08-17T14:59:59+08:00"
            ),
        )

    _require_closed_training_cutoff(
        cutoff,
        observed_at=datetime.fromisoformat("2026-08-17T15:30:00+08:00"),
    )

    with pytest.raises(ValueError, match="future"):
        _require_closed_training_cutoff(
            pd.Timestamp("2026-08-18").date(),
            observed_at=datetime.fromisoformat(
                "2026-08-17T16:00:00+08:00"
            ),
        )


def test_cli_start_defaults_to_config_and_full_universe_rejects_override(
    tmp_path,
):
    configured = date(2023, 1, 1)
    assert _resolve_training_start(
        "",
        maximum_stocks=0,
        configured_start=configured,
    ) == (configured, False)
    override = date(2024, 3, 1)
    assert _resolve_training_start(
        override.isoformat(),
        maximum_stocks=8,
        configured_start=configured,
    ) == (override, True)
    with pytest.raises(ValueError, match="must equal frozen config"):
        _resolve_training_start(
            override.isoformat(),
            maximum_stocks=0,
            configured_start=configured,
        )
    cutoff = date(2026, 8, 15)
    artifacts = {
        horizon: {
            "suite_release_id": "immutable-suite",
            "training_cutoff": cutoff.isoformat(),
            "horizon_days": horizon,
            "training_window": {
                "protocol": TRAINING_WINDOW_PROTOCOL,
                "configured_history_start": configured.isoformat(),
                "signal_start": configured.isoformat(),
                "signal_end": cutoff.isoformat(),
            },
            "config_hash": config_hash(),
            "dataset_manifest": {
                "universe_scope": "FULL_A_SHARE_POINT_IN_TIME",
            },
        }
        for horizon in (1, 5, 20)
    }
    _verify_existing_suite_request(
        artifacts,
        suite_release_id="immutable-suite",
        signal_start=configured,
        training_cutoff=cutoff,
        universe_scope="FULL_A_SHARE_POINT_IN_TIME",
        configured_history_start=configured,
        training_window_protocol=TRAINING_WINDOW_PROTOCOL,
        current_config_hash=config_hash(),
    )
    artifacts[5]["training_window"]["signal_start"] = "2020-01-02"
    with pytest.raises(RuntimeError, match="differs from requested"):
        _verify_existing_suite_request(
            artifacts,
            suite_release_id="immutable-suite",
            signal_start=configured,
            training_cutoff=cutoff,
            universe_scope="FULL_A_SHARE_POINT_IN_TIME",
            configured_history_start=configured,
            training_window_protocol=TRAINING_WINDOW_PROTOCOL,
            current_config_hash=config_hash(),
        )

    partial_release = tmp_path / "partial-release"
    partial_release.mkdir()
    for horizon in (1, 5, 20):
        (partial_release / f"T{horizon}.json").write_text(
            "{}\n",
            encoding="utf-8",
        )
    with pytest.raises(RuntimeError, match="release is partial"):
        _existing_suite(
            partial_release,
            require_current_config=False,
        )

def _bars(*, sessions: int = 340, stocks: int = 14) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=sessions)
    rows = []
    for stock in range(stocks):
        code = f"{stock + 1:06d}"
        previous = 10.0 + stock * 0.15
        for index, day in enumerate(dates):
            common = 0.0020 * np.sin(index / 9.0) + 0.0007
            stock_wave = 0.0015 * np.cos(index / 5.0 + stock / 4.0)
            gap = 0.0005 * np.sin(index / 4.0 + stock)
            open_price = previous * (1.0 + gap)
            close_price = open_price * (1.0 + common + stock_wave)
            high = max(open_price, close_price) * 1.006
            low = min(open_price, close_price) * 0.994
            rows.append({
                "stock_code": code,
                "trade_date": day,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close_price,
                "pre_close": previous,
                "amount": 50_000_000.0 * (1.0 + 0.25 * np.sin(index / 6.0 + stock)),
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


def _governance_release(horizon: int, suite_release_id: str) -> str:
    spec = HORIZON_MODEL_SPECS[horizon]
    return horizon_governance_release_id(
        suite_release_id=suite_release_id,
        model_key=spec.model_key,
        model_version=spec.model_version,
        horizon_days=horizon,
    )


def _rehash_artifact(artifact: dict) -> None:
    artifact["artifact_hash"] = canonical_hash(
        _artifact_core_payload(artifact)
    )
    artifact["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": artifact["artifact_hash"],
        "created_at": artifact["created_at"],
    })


def _candidate_ledger_path(root: Path, artifact: dict) -> Path:
    relative = PurePosixPath(
        artifact["candidate_evaluation_ledger"]["relative_path"]
    )
    return root.joinpath(*relative.parts)


def _place_candidate_ledger(
    root: Path,
    artifact: dict,
    payload: bytes,
) -> Path:
    path = _candidate_ledger_path(root, artifact)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _rebind_candidate_ledger(
    artifact: dict,
    records: list[dict],
    *,
    preserve_header_hash: bool = True,
) -> tuple[dict, bytes]:
    forged = copy.deepcopy(artifact)
    canonical_payload = b"".join(
        (canonical_json(item) + "\n").encode("utf-8")
        for item in records
    )
    compressed_buffer = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        fileobj=compressed_buffer,
        compresslevel=6,
        mtime=0,
    ) as stream:
        stream.write(canonical_payload)
    compressed = compressed_buffer.getvalue()
    content_hash = hashlib.sha256(compressed).hexdigest()
    reference = forged["candidate_evaluation_ledger"]
    reference["content_sha256"] = content_hash
    reference["canonical_records_sha256"] = hashlib.sha256(
        canonical_payload
    ).hexdigest()
    reference["relative_path"] = (
        f"candidate-ledgers/sha256/{content_hash[:2]}/"
        f"{content_hash}.jsonl.gz"
    )
    reference["compressed_size_bytes"] = len(compressed)
    if not preserve_header_hash:
        reference["header_hash"] = canonical_hash(records[0])
    reference["reference_hash"] = canonical_hash({
        key: value for key, value in reference.items()
        if key != "reference_hash"
    })
    selection = forged["oos_evidence"]["selection_evidence"]
    selection["candidate_ledger_content_sha256"] = content_hash
    selection["candidate_ledger_canonical_records_sha256"] = reference[
        "canonical_records_sha256"
    ]
    selection["candidate_ledger_reference_hash"] = reference[
        "reference_hash"
    ]
    selection["selection_evidence_hash"] = canonical_hash({
        key: value for key, value in selection.items()
        if key != "selection_evidence_hash"
    })
    evidence = forged["oos_evidence"]
    evidence["candidate_evaluation_ledger_reference_hash"] = reference[
        "reference_hash"
    ]
    evidence["evidence_hash"] = canonical_hash({
        key: value for key, value in evidence.items()
        if key != "evidence_hash"
    })
    forged["oos_evidence_hash"] = evidence["evidence_hash"]
    _rehash_artifact(forged)
    return forged, compressed


def test_governance_release_identity_is_bound_to_training_suite():
    first = _governance_release(1, "suite-a")
    second = _governance_release(1, "suite-b")
    assert first != second
    assert first.startswith("suite-a:")
    assert second.startswith("suite-b:")
    assert len(first) <= 160


def test_dataset_uses_exact_sessions_costs_and_attestation():
    bars = _bars(sessions=150, stocks=4)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        5,
        trade_calendar=calendar,
        signal_start=pd.Timestamp(calendar[80]).date(),
        signal_end=pd.Timestamp(calendar[-7]).date(),
    )
    row = dataset.frame.iloc[0]
    signal_index = calendar.index(np.datetime64(row["decision_session_date"]))
    assert pd.Timestamp(row["entry_trade_date"]) == pd.Timestamp(calendar[signal_index + 1])
    assert pd.Timestamp(row["outcome_matures_on"]) == pd.Timestamp(calendar[signal_index + 6])
    assert row["net_return_pct"] == pytest.approx(
        row["gross_return_pct"] - 0.20
    )
    assert row["feature_available_at"].endswith("+08:00")
    assert row["label_mature_at"].endswith("+08:00")
    assert dataset.manifest["outcomes_include_costs"] is True
    assert dataset.manifest["executable_verified"] is False
    assert dataset.manifest["qmt_attested_label_count"] > 0


def test_future_bars_do_not_change_frozen_signal_features():
    bars = _bars(sessions=150, stocks=4)
    calendar = sorted(bars["trade_date"].unique())
    signal_day = pd.Timestamp(calendar[100]).date()
    before = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start=signal_day,
        signal_end=signal_day,
    )
    changed = bars.copy()
    changed.loc[changed["trade_date"] > pd.Timestamp(signal_day), "amount"] *= 100.0
    after = build_horizon_dataset(
        changed,
        1,
        trade_calendar=calendar,
        signal_start=signal_day,
        signal_end=signal_day,
    )
    features = list(HORIZON_MODEL_SPECS[1].features)
    pd.testing.assert_frame_equal(
        before.frame[["sample_id", *features]],
        after.frame[["sample_id", *features]],
    )


def test_many_rows_across_six_sessions_cannot_pass_temporal_gate():
    bars = _bars(sessions=100, stocks=40)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start=pd.Timestamp(calendar[-8]).date(),
        signal_end=pd.Timestamp(calendar[-3]).date(),
    )
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_governance_release(1, "six-session-adversarial"),
        suite_release_id="six-session-adversarial",
        training_cutoff=pd.Timestamp(calendar[-1]).date(),
    )
    assert artifact["oos_evidence"]["matured_sample_count"] >= 160
    assert artifact["oos_evidence"]["distinct_train_sessions"] == 6
    assert artifact["gate"]["status"] == "BLOCK"
    assert "INSUFFICIENT_TEMPORAL_COVERAGE" in artifact["gate"]["block_reasons"]


def test_non_default_training_window_is_research_block_and_not_current():
    bars = _bars(sessions=220, stocks=8)
    calendar = sorted(bars["trade_date"].unique())
    signal_start = pd.Timestamp(calendar[50]).date()
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start=signal_start,
        signal_end=pd.Timestamp(calendar[-4]).date(),
        universe_scope="BOUNDED_SMOKE_RESEARCH_ONLY",
    )
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_governance_release(1, "window-override-smoke"),
        suite_release_id="window-override-smoke",
        training_cutoff=pd.Timestamp(calendar[-1]).date(),
        policy=_relaxed_policy(),
        selection_policy=HorizonSelectionPolicy(minimum_cross_section_size=8),
        created_at="2026-08-17T08:00:00+00:00",
    )
    assert artifact["training_window"]["signal_start"] == (
        signal_start.isoformat()
    )
    assert artifact["training_window"]["status"] == (
        "NON_DEFAULT_TRAINING_WINDOW"
    )
    assert artifact["gate"]["status"] == "BLOCK"
    assert "NON_DEFAULT_TRAINING_WINDOW" in artifact["gate"]["block_reasons"]
    verify_horizon_artifact(artifact, require_current_config=False)
    with pytest.raises(HorizonModelError, match="not current config"):
        verify_horizon_artifact(artifact)
    manifest = artifact_manifest(artifact, require_current_config=False)
    assert manifest["training_window_is_current_config_default"] is False


def test_suite_trains_independent_models_and_purged_walk_forward(tmp_path):
    bars = _bars()
    calendar = sorted(bars["trade_date"].unique())
    datasets = {
        horizon: build_horizon_dataset(
            bars,
            horizon,
            trade_calendar=calendar,
            signal_start="2023-01-01",
            signal_end=pd.Timestamp(calendar[-22]).date(),
        )
        for horizon in (1, 5, 20)
    }
    artifacts = train_horizon_suite(
        datasets,
        release_id="suite-2025-01-01-v1",
        training_cutoff=pd.Timestamp(calendar[-1]).date(),
        policy=_relaxed_policy(),
        created_at="2026-08-16T12:00:00+00:00",
    )
    assert len({item["model_key"] for item in artifacts.values()}) == 3
    assert len({item["feature_protocol_hash"] for item in artifacts.values()}) == 3
    assert len({item["dataset_hash"] for item in artifacts.values()}) == 3
    assert len({item["final_model"]["model_hash"] for item in artifacts.values()}) == 3
    for horizon, artifact in artifacts.items():
        assert artifact["release_id"] == _governance_release(
            horizon, "suite-2025-01-01-v1"
        )
        assert artifact["suite_release_id"] == "suite-2025-01-01-v1"
        assert artifact["oos_evidence"]["walk_forward_fold_count"] == 3
        assert artifact["oos_evidence"]["distinct_oos_sessions"] >= 15
        assert artifact["oos_evidence"]["population_stability_index"] >= 0
        psi = artifact["oos_evidence"]["population_stability_evidence"]
        assert psi["uses_final_model_predictions"] is False
        assert psi["uses_labels"] is False
        assert psi["protocol"] == PSI_PROTOCOL
        assert psi["score_source"] == "FROZEN_PRE_OOS_ANCHOR_MODEL"
        assert psi["anchor_model_hash"] == artifact["walk_forward"][
            "folds"
        ][0]["model_hash"]
        for fold in artifact["walk_forward"]["folds"]:
            assert fold["latest_training_label_maturity"] < fold["validation_start"]
            assert fold["score_normalization_protocol"] == (
                SCORE_NORMALIZATION_PROTOCOL
            )
            assert fold["training_score_std"] > 0
            if fold["calibration_training_sample_count"]:
                assert (
                    fold["latest_calibration_label_maturity"]
                    < fold["validation_start"]
                )
    suite = write_horizon_suite(artifacts, tmp_path / "suite-2025-01-01-v1")
    assert suite["suite_release_id"] == "suite-2025-01-01-v1"
    assert suite["release_id"] == suite["suite_release_id"]
    assert suite["status"] in {"PASS", "BLOCK"}
    discovered = load_horizon_suite(
        tmp_path / "suite-2025-01-01-v1" / "suite.json"
    )
    assert discovered["suite_hash"] == suite["suite_hash"]
    assert [item["horizon_days"] for item in discovered["models"]] == [1, 5, 20]

    forged_suite = copy.deepcopy(suite)
    for model_manifest in forged_suite["models"]:
        window = model_manifest["training_window"]
        window["signal_start_inclusive"] = False
        window["training_window_hash"] = canonical_hash({
            key: value
            for key, value in window.items()
            if key != "training_window_hash"
        })
    forged_suite["suite_hash"] = canonical_hash({
        key: value
        for key, value in forged_suite.items()
        if key != "suite_hash"
    })
    with pytest.raises(
        HorizonModelError,
        match="suite model training window fields differ",
    ):
        verify_horizon_suite(forged_suite, artifact_root=None)

    mixed_artifacts = copy.deepcopy(artifacts)
    mixed_artifacts[5]["config_hash"] = "9" * 64
    _rehash_artifact(mixed_artifacts[5])
    mixed_root = tmp_path / "mixed-suite-must-not-materialize"
    with pytest.raises(HorizonModelError, match="suite independence"):
        write_horizon_suite(
            mixed_artifacts,
            mixed_root,
            require_current_config=False,
        )
    assert not any(mixed_root.glob("T*.json"))


def test_artifact_core_hash_is_reproducible_and_prediction_is_research_only(tmp_path):
    bars = _bars(sessions=240, stocks=8)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=pd.Timestamp(calendar[-4]).date(),
    )
    kwargs = {
        "release_id": _governance_release(1, "reproducible-suite"),
        "suite_release_id": "reproducible-suite",
        "training_cutoff": pd.Timestamp(calendar[-1]).date(),
        "policy": _relaxed_policy(),
    }
    first = train_independent_horizon_model(
        dataset,
        created_at="2026-08-16T01:00:00+00:00",
        **kwargs,
    )
    second = train_independent_horizon_model(
        dataset,
        created_at="2026-08-16T02:00:00+00:00",
        **kwargs,
    )
    assert first["artifact_hash"] == second["artifact_hash"]
    assert first["creation_envelope_hash"] != second["creation_envelope_hash"]
    path = write_horizon_artifact(first, tmp_path / "T1.json")
    loaded = load_horizon_artifact(path)
    sample = dataset.frame.iloc[-1]
    prediction = predict_horizon_artifact(
        loaded,
        {name: sample[name] for name in HORIZON_MODEL_SPECS[1].features},
    )
    assert 0 <= prediction.probability_positive <= 1
    assert prediction.order_authority is False
    assert prediction.model_artifact_hash == loaded["artifact_hash"]
    manifest = artifact_manifest(loaded)
    assert manifest["execution_evidence_scope"] == "LONG_HISTORY_OOS_RESEARCH_ONLY"
    assert manifest["executable_verified"] is False


def test_artifact_loader_recomputes_gate_and_rejects_tampering():
    bars = _bars(sessions=180, stocks=6)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=pd.Timestamp(calendar[-4]).date(),
    )
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_governance_release(1, "tamper-suite"),
        suite_release_id="tamper-suite",
        training_cutoff=pd.Timestamp(calendar[-1]).date(),
        policy=_relaxed_policy(),
        created_at=datetime(2026, 8, 16, tzinfo=timezone.utc),
    )
    beyond_cutoff_dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=(pd.Timestamp(calendar[-1]) + pd.Timedelta(days=30)).date(),
    )
    with pytest.raises(
        HorizonModelError,
        match="training_window.signal_end exceeds training_cutoff",
    ):
        train_independent_horizon_model(
            beyond_cutoff_dataset,
            release_id=_governance_release(1, "future-window-suite"),
            suite_release_id="future-window-suite",
            training_cutoff=pd.Timestamp(calendar[-1]).date(),
            policy=_relaxed_policy(),
            created_at=datetime(2026, 8, 16, tzinfo=timezone.utc),
        )
    window_tamper = copy.deepcopy(artifact)
    window_tamper["training_window"]["signal_start"] = "2020-01-02"
    _rehash_artifact(window_tamper)
    with pytest.raises(HorizonModelError, match="training_window_hash differs"):
        verify_horizon_artifact(window_tamper)

    pre_window_sample = copy.deepcopy(artifact)
    manifest = pre_window_sample["dataset_manifest"]
    manifest["first_decision_session"] = "2022-12-30"
    manifest["manifest_hash"] = canonical_hash({
        key: value
        for key, value in manifest.items()
        if key != "manifest_hash"
    })
    _rehash_artifact(pre_window_sample)
    with pytest.raises(
        HorizonModelError,
        match="dataset clock exceeds frozen training window",
    ):
        verify_horizon_artifact(pre_window_sample)

    truncated_manifest = copy.deepcopy(artifact)
    manifest = truncated_manifest["dataset_manifest"]
    manifest["last_decision_session"] = manifest[
        "first_decision_session"
    ]
    manifest["manifest_hash"] = canonical_hash({
        key: value
        for key, value in manifest.items()
        if key != "manifest_hash"
    })
    _rehash_artifact(truncated_manifest)
    with pytest.raises(
        HorizonModelError,
        match="clock exceeds dataset manifest",
    ):
        verify_horizon_artifact(truncated_manifest)

    post_window_fold = copy.deepcopy(artifact)
    last_fold = post_window_fold["walk_forward"]["folds"][-1]
    last_fold["validation_end"] = (
        pd.Timestamp(post_window_fold["training_window"]["signal_end"])
        + pd.Timedelta(days=1)
    ).date().isoformat()
    last_fold["fold_hash"] = canonical_hash({
        key: value
        for key, value in last_fold.items()
        if key != "fold_hash"
    })
    _rehash_artifact(post_window_fold)
    with pytest.raises(
        HorizonModelError,
        match="walk-forward clock exceeds frozen training window",
    ):
        verify_horizon_artifact(post_window_fold)
    forged = copy.deepcopy(artifact)
    forged["gate"]["status"] = "PASS" if artifact["gate"]["status"] == "BLOCK" else "BLOCK"
    forged["gate"]["block_reasons"] = []
    forged["gate"]["contract_eligible"] = forged["gate"]["status"] == "PASS"
    forged["contract_eligible"] = forged["gate"]["contract_eligible"]
    forged["artifact_hash"] = canonical_hash(_artifact_core_payload(forged))
    forged["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": forged["artifact_hash"],
        "created_at": forged["created_at"],
    })
    with pytest.raises(HorizonModelError, match="persisted gate differs"):
        verify_horizon_artifact(forged)

    paper_scope_tamper = copy.deepcopy(artifact)
    paper_scope_tamper["paper_eligible"] = True
    _rehash_artifact(paper_scope_tamper)
    with pytest.raises(
        HorizonModelError,
        match="contract eligibility exceeded Shadow scope",
    ):
        verify_horizon_artifact(paper_scope_tamper)

    coefficient_tamper = copy.deepcopy(artifact)
    coefficient_tamper["final_model"]["coefficients"][0] += 1.0
    coefficient_tamper["artifact_hash"] = canonical_hash(
        _artifact_core_payload(coefficient_tamper)
    )
    coefficient_tamper["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": coefficient_tamper["artifact_hash"],
        "created_at": coefficient_tamper["created_at"],
    })
    with pytest.raises(HorizonModelError, match="model hash differs"):
        verify_horizon_artifact(coefficient_tamper)

    calibration_metric_tamper = copy.deepcopy(artifact)
    original_brier = float(
        calibration_metric_tamper["oos_evidence"]["brier_score"]
    )
    calibration_metric_tamper["oos_evidence"]["brier_score"] = (
        0.0 if original_brier != 0.0 else 1.0
    )
    evidence = calibration_metric_tamper["oos_evidence"]
    evidence["evidence_hash"] = canonical_hash({
        key: value for key, value in evidence.items()
        if key != "evidence_hash"
    })
    calibration_metric_tamper["oos_evidence_hash"] = evidence[
        "evidence_hash"
    ]
    _rehash_artifact(calibration_metric_tamper)
    with pytest.raises(HorizonModelError, match="OOS calibration metric differs"):
        verify_horizon_artifact(calibration_metric_tamper)

    version_tamper = copy.deepcopy(artifact)
    version_tamper["code_version"] = "0" * 64
    version_tamper["artifact_hash"] = canonical_hash(
        _artifact_core_payload(version_tamper)
    )
    version_tamper["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": version_tamper["artifact_hash"],
        "created_at": version_tamper["created_at"],
    })
    with pytest.raises(HorizonModelError, match="code_version is not current"):
        verify_horizon_artifact(version_tamper)

    future_cutoff = copy.deepcopy(artifact)
    future_cutoff["training_cutoff"] = "2027-01-01"
    future_cutoff["valid_until"] = "2027-01-31"
    future_evidence = future_cutoff["oos_evidence"]
    future_evidence["training_cutoff"] = "2027-01-01"
    future_evidence["valid_until"] = "2027-01-31"
    future_evidence["evidence_hash"] = canonical_hash({
        key: value
        for key, value in future_evidence.items()
        if key != "evidence_hash"
    })
    future_cutoff["oos_evidence_hash"] = future_evidence["evidence_hash"]
    future_cutoff["artifact_hash"] = canonical_hash(
        _artifact_core_payload(future_cutoff)
    )
    future_cutoff["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": future_cutoff["artifact_hash"],
        "created_at": future_cutoff["created_at"],
    })
    with pytest.raises(
        HorizonModelError, match="training_cutoff follows artifact creation"
    ):
        verify_horizon_artifact(future_cutoff)

    psi_tamper = copy.deepcopy(artifact)
    psi_tamper["oos_evidence"]["population_stability_evidence"][
        "uses_final_model_predictions"
    ] = True
    psi_body = psi_tamper["oos_evidence"]["population_stability_evidence"]
    psi_body["psi_evidence_hash"] = canonical_hash({
        key: value for key, value in psi_body.items()
        if key != "psi_evidence_hash"
    })
    evidence = psi_tamper["oos_evidence"]
    evidence["evidence_hash"] = canonical_hash({
        key: value for key, value in evidence.items() if key != "evidence_hash"
    })
    psi_tamper["oos_evidence_hash"] = evidence["evidence_hash"]
    psi_tamper["artifact_hash"] = canonical_hash(_artifact_core_payload(psi_tamper))
    psi_tamper["creation_envelope_hash"] = canonical_hash({
        "artifact_hash": psi_tamper["artifact_hash"],
        "created_at": psi_tamper["created_at"],
    })
    with pytest.raises(HorizonModelError, match="PSI evidence uses a leaky source"):
        verify_horizon_artifact(psi_tamper)


def test_weighted_solver_centers_intercept_and_is_weight_scale_invariant():
    design = np.asarray([[0.0], [1.0], [2.0], [3.0]])
    target = np.asarray([5.0, 7.0, 9.0, 11.0])
    weights = np.asarray([1.0, 7.0, 2.0, 11.0])

    first_intercept, first_coefficients = _solve_weighted_ridge(
        design,
        target,
        1e-12,
        weights,
    )
    scaled_intercept, scaled_coefficients = _solve_weighted_ridge(
        design,
        target,
        1e-12,
        weights * 1_000_000.0,
    )

    assert first_intercept == pytest.approx(5.0, abs=1e-10)
    assert first_coefficients.tolist() == pytest.approx([2.0], abs=1e-10)
    assert scaled_intercept == pytest.approx(first_intercept, abs=1e-12)
    assert scaled_coefficients == pytest.approx(
        first_coefficients, abs=1e-12
    )


def test_session_balanced_fit_is_unchanged_by_copying_one_session():
    bars = _bars(sessions=140, stocks=5)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        5,
        trade_calendar=calendar,
        signal_start=pd.Timestamp(calendar[80]).date(),
        signal_end=pd.Timestamp(calendar[115]).date(),
    )
    frame = dataset.frame[dataset.frame["label_available"]].copy()
    original = _fit_model(frame, HORIZON_MODEL_SPECS[5])

    copied_session = frame["decision_session_date"].iloc[5]
    duplicated = frame[
        frame["decision_session_date"].eq(copied_session)
    ].copy()
    duplicated["sample_id"] = duplicated["sample_id"].astype(str) + "-copy"
    copied = _fit_model(
        pd.concat([frame, duplicated], ignore_index=True),
        HORIZON_MODEL_SPECS[5],
    )

    for field in (
        "intercept",
        "medians",
        "means",
        "scales",
        "coefficients",
        "training_score_mean",
        "training_score_std",
    ):
        assert np.asarray(copied[field]) == pytest.approx(
            np.asarray(original[field]), abs=1e-12
        )


@pytest.mark.parametrize("zero_volume_offset", [1, 2])
def test_entry_or_exit_zero_volume_quarantines_label(zero_volume_offset: int):
    bars = _bars(sessions=150, stocks=4)
    calendar = sorted(bars["trade_date"].unique())
    signal_index = 100
    stock_code = "000001"
    bars.loc[
        bars["stock_code"].eq(stock_code)
        & bars["trade_date"].eq(calendar[signal_index + zero_volume_offset]),
        "amount",
    ] = 0.0

    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start=pd.Timestamp(calendar[signal_index]).date(),
        signal_end=pd.Timestamp(calendar[signal_index]).date(),
    )
    row = dataset.frame[dataset.frame["stock_code"].eq(stock_code)].iloc[0]

    assert bool(row["label_available"]) is False
    assert row["label_quarantine_reason"] == "EXACT_SESSION_ZERO_VOLUME"
    assert pd.isna(row["gross_return_pct"])
    assert pd.isna(row["net_return_pct"])
    assert dataset.manifest["quarantined_sample_count"] >= 1


def test_session_fold_ic_blocks_global_simpson_reversal():
    frame = pd.DataFrame([
        {
            "fold_number": 1,
            "decision_session_date": "2026-01-05",
            "expected_return_net_pct": 0.0,
            "probability_positive": 0.10,
            "net_return_pct": 100.0,
        },
        {
            "fold_number": 1,
            "decision_session_date": "2026-01-05",
            "expected_return_net_pct": 1.0,
            "probability_positive": 0.20,
            "net_return_pct": 101.0,
        },
        {
            "fold_number": 2,
            "decision_session_date": "2026-02-05",
            "expected_return_net_pct": 10.0,
            "probability_positive": 0.80,
            "net_return_pct": 0.0,
        },
        {
            "fold_number": 2,
            "decision_session_date": "2026-02-05",
            "expected_return_net_pct": 11.0,
            "probability_positive": 0.90,
            "net_return_pct": 1.0,
        },
    ])

    assert _spearman(
        frame["expected_return_net_pct"], frame["net_return_pct"]
    ) < 0
    evidence = _session_direction_evidence(
        frame,
        minimum_cross_section_size=2,
    )
    assert evidence["expected_return_rank_ic"] == pytest.approx(1.0)
    assert evidence["probability_rank_ic"] == pytest.approx(1.0)
    assert evidence["gate_direction_rank_ic"] == pytest.approx(1.0)


def test_calibration_never_splits_equal_scores_across_overlapping_buckets():
    frame = pd.DataFrame({
        "sample_id": ["a", "b", "c", "d"],
        "decision_session_date": [
            "2026-01-05", "2026-01-05", "2026-01-06", "2026-01-06"
        ],
        "normalized_score": [-1.0, -1.0, 1.0, 1.0],
        "net_return_pct": [-2.0, -1.0, 1.0, 2.0],
    })
    calibration = _fit_calibration(frame, bucket_count=4)

    assert len(calibration["buckets"]) == 2
    assert calibration["buckets"][0]["upper_score"] < (
        calibration["buckets"][1]["lower_score"]
    )
    _verify_calibration(calibration)

    forged = copy.deepcopy(calibration)
    forged["buckets"][1]["lower_score"] = forged["buckets"][0][
        "upper_score"
    ]
    forged["calibration_hash"] = canonical_hash({
        key: value for key, value in forged.items()
        if key != "calibration_hash"
    })
    with pytest.raises(HorizonModelError, match="score buckets overlap"):
        _verify_calibration(forged)


def test_selection_gate_uses_ranked_ledger_not_negative_unconditional_market():
    rows = []
    for fold_number, day in ((1, "2026-03-02"), (2, "2026-04-02")):
        for index in range(20):
            net_return = float(index - 18)
            rows.append({
                "fold_number": fold_number,
                "decision_session_date": day,
                "sample_id": f"{fold_number}-{index:02d}",
                "stock_code": f"{index + 1:06d}",
                "normalized_score": float(index),
                "expected_return_net_pct": net_return,
                "probability_positive": 0.05 + index * 0.025,
                "gross_return_pct": net_return + 0.20,
                "net_return_pct": net_return,
            })
    evaluation = pd.DataFrame(rows)

    assert evaluation["net_return_pct"].mean() < 0
    assert _session_direction_evidence(
        evaluation,
        minimum_cross_section_size=2,
    )["gate_direction_rank_ic"] == pytest.approx(1.0)
    selection = _build_selection_evidence(
        evaluation,
        policy=DEFAULT_SELECTION_POLICY,
        cost_assumption_pct=0.20,
    )
    metrics = _verify_selection_evidence(
        selection,
        policy=DEFAULT_SELECTION_POLICY,
        cost_assumption_pct=0.20,
    )

    assert selection["protocol"] == SELECTION_PROTOCOL
    assert selection["deployment_candidate_domain_verified"] is False
    assert selection["order_authority"] is False
    assert metrics["selected_oos_sample_count"] == 2
    assert metrics["selected_oos_session_count"] == 2
    assert metrics["net_expectancy_after_cost_pct"] == pytest.approx(1.0)
    assert metrics["profit_factor"] > 1.0

    forged = copy.deepcopy(selection)
    forged["selected_ledger"][0]["net_return_pct"] = -5.0
    forged["selected_ledger_hash"] = canonical_hash(
        forged["selected_ledger"]
    )
    forged["selection_evidence_hash"] = canonical_hash({
        key: value
        for key, value in forged.items()
        if key != "selection_evidence_hash"
    })
    with pytest.raises(
        HorizonModelError,
        match="selected ledger differs from frozen selection frontier",
    ):
        _verify_selection_evidence(
            forged,
            policy=DEFAULT_SELECTION_POLICY,
            cost_assumption_pct=0.20,
        )


def test_calibration_clock_is_maturity_purged_and_anchor_is_frozen():
    bars = _bars(sessions=260, stocks=8)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=pd.Timestamp(calendar[-4]).date(),
    )
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_governance_release(1, "purged-calibration-suite"),
        suite_release_id="purged-calibration-suite",
        training_cutoff=pd.Timestamp(calendar[-1]).date(),
        policy=_relaxed_policy(),
        selection_policy=HorizonSelectionPolicy(
            minimum_cross_section_size=8
        ),
        created_at="2025-01-01T00:00:00+00:00",
    )
    assert artifact["gate"]["status"] == "BLOCK"
    assert "NON_DEFAULT_SELECTION_POLICY_RESEARCH_ONLY" in artifact["gate"][
        "block_reasons"
    ]

    assert artifact["schema_version"] == ARTIFACT_SCHEMA
    assert artifact["model_protocol"] == MODEL_PROTOCOL
    assert artifact["gate"]["gate_scope"] == (
        "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC"
    )
    assert artifact["gate"]["deployment_gate"] is False
    assert artifact["contract_eligibility_scope"] == (
        CONTRACT_ELIGIBILITY_SCOPE
    )
    assert artifact["gate"]["contract_eligibility_scope"] == (
        CONTRACT_ELIGIBILITY_SCOPE
    )
    assert artifact["paper_eligible"] is False
    assert artifact["production_eligible"] is False
    assert artifact["gate"]["paper_eligible"] is False
    assert artifact["gate"]["production_eligible"] is False
    assert artifact["model_code_version"] == MODEL_CODE_VERSION
    assert artifact["calibration"]["protocol"] == CALIBRATION_PROTOCOL
    assert artifact["final_model"][
        "score_normalization_protocol"
    ] == SCORE_NORMALIZATION_PROTOCOL
    folds_with_calibration = [
        fold
        for fold in artifact["walk_forward"]["folds"]
        if fold["calibration_training_sample_count"] > 0
    ]
    assert folds_with_calibration
    for fold in folds_with_calibration:
        assert (
            fold["latest_calibration_label_maturity"]
            < fold["validation_start"]
        )

    psi = artifact["oos_evidence"]["population_stability_evidence"]
    assert psi["anchor_model_hash"] == artifact["walk_forward"]["folds"][0][
        "model_hash"
    ]

    leaked = copy.deepcopy(artifact)
    leaked_fold = next(
        fold
        for fold in leaked["walk_forward"]["folds"]
        if fold["calibration_training_sample_count"] > 0
    )
    leaked_fold["latest_calibration_label_maturity"] = leaked_fold[
        "validation_start"
    ]
    leaked_fold["fold_hash"] = canonical_hash({
        key: value
        for key, value in leaked_fold.items()
        if key != "fold_hash"
    })
    _rehash_artifact(leaked)
    with pytest.raises(
        HorizonModelError,
        match="walk-forward calibration leaks an immature label",
    ):
        verify_horizon_artifact(leaked)

    wrong_anchor = copy.deepcopy(artifact)
    psi = wrong_anchor["oos_evidence"]["population_stability_evidence"]
    psi["anchor_model_hash"] = "0" * 64
    psi["psi_evidence_hash"] = canonical_hash({
        key: value for key, value in psi.items()
        if key != "psi_evidence_hash"
    })
    evidence = wrong_anchor["oos_evidence"]
    evidence["evidence_hash"] = canonical_hash({
        key: value for key, value in evidence.items()
        if key != "evidence_hash"
    })
    wrong_anchor["oos_evidence_hash"] = evidence["evidence_hash"]
    _rehash_artifact(wrong_anchor)
    with pytest.raises(
        HorizonModelError,
        match="PSI anchor is not the frozen first-fold model",
    ):
        verify_horizon_artifact(wrong_anchor)

    for historical_schema in (
        HISTORICAL_ARTIFACT_SCHEMA_V1,
        HISTORICAL_ARTIFACT_SCHEMA_V2,
    ):
        legacy = copy.deepcopy(artifact)
        legacy["schema_version"] = historical_schema
        _rehash_artifact(legacy)
        with pytest.raises(HorizonModelError, match="schema is unsupported"):
            verify_horizon_artifact(legacy)


def test_candidate_sidecar_stream_verifies_full_oos_binding(tmp_path):
    bars = _bars(sessions=220, stocks=8)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=pd.Timestamp(calendar[-4]).date(),
    )
    suite_id = "candidate-ledger-suite"
    root = tmp_path / suite_id
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_governance_release(1, suite_id),
        suite_release_id=suite_id,
        training_cutoff=pd.Timestamp(calendar[-1]).date(),
        policy=_relaxed_policy(),
        selection_policy=HorizonSelectionPolicy(
            minimum_cross_section_size=8
        ),
        created_at="2025-01-01T00:00:00+00:00",
        candidate_ledger_root=root,
    )
    artifact_path = write_horizon_artifact(artifact, root / "T1.json")
    loaded = load_horizon_artifact(artifact_path)
    registration = verify_candidate_evaluation_ledger(loaded, root)
    assert verify_candidate_evaluation_ledger(loaded, root) == registration
    reference = artifact["candidate_evaluation_ledger"]

    assert verify_horizon_artifact(artifact)["mapping_verification_scope"] == (
        "SELF_CONSISTENT_ONLY_NOT_REGISTRATION_EVIDENCE"
    )
    assert artifact["candidate_ledger_registration_required"] is True
    assert reference["schema_version"] == CANDIDATE_EVALUATION_LEDGER_SCHEMA
    assert reference["binding_protocol"] == CANDIDATE_LEDGER_BINDING_PROTOCOL
    assert reference["row_count"] == artifact["oos_evidence"][
        "oos_sample_count"
    ]
    assert reference["evaluation_row_count"] == artifact["oos_evidence"][
        "calibration_evaluation_sample_count"
    ]
    assert registration["ledger_content_sha256"] == reference[
        "content_sha256"
    ]
    assert registration["selection_evidence_hash"] == artifact[
        "oos_evidence"
    ]["selection_evidence"]["selection_evidence_hash"]
    assert len(registration["registration_evidence_hash"]) == 64
    assert artifact_manifest(artifact)["candidate_evaluation_ledger"] == (
        reference
    )
    compressed = _candidate_ledger_path(root, artifact).read_bytes()
    assert len(gzip.decompress(compressed)) > len(compressed)


def test_candidate_sidecar_rejects_missing_tamper_omission_duplicate_and_cross_suite(
    tmp_path,
):
    bars = _bars(sessions=220, stocks=8)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=pd.Timestamp(calendar[-4]).date(),
    )

    def train(suite_id: str, root: Path) -> dict:
        return train_independent_horizon_model(
            dataset,
            release_id=_governance_release(1, suite_id),
            suite_release_id=suite_id,
            training_cutoff=pd.Timestamp(calendar[-1]).date(),
            policy=_relaxed_policy(),
            selection_policy=HorizonSelectionPolicy(
                minimum_cross_section_size=8
            ),
            created_at="2025-01-01T00:00:00+00:00",
            candidate_ledger_root=root,
        )

    source_root = tmp_path / "source"
    artifact = train("candidate-ledger-attacks", source_root)
    source_path = _candidate_ledger_path(source_root, artifact)
    source_bytes = source_path.read_bytes()
    records = [
        json.loads(line)
        for line in gzip.decompress(source_bytes).decode("utf-8").splitlines()
    ]

    with pytest.raises(HorizonModelError, match="ledger is missing"):
        verify_candidate_evaluation_ledger(artifact, tmp_path / "missing")

    tamper_root = tmp_path / "byte-tamper"
    tampered = bytearray(source_bytes)
    tampered[-1] ^= 1
    _place_candidate_ledger(tamper_root, artifact, bytes(tampered))
    with pytest.raises(HorizonModelError, match="compressed content hash differs"):
        verify_candidate_evaluation_ledger(artifact, tamper_root)

    selected_sample = artifact["oos_evidence"]["selection_evidence"][
        "selected_ledger"
    ][0]["sample_id"]
    omitted_records = [
        item for item in records
        if item.get("sample_id") != selected_sample
    ]
    omitted_artifact, omitted_bytes = _rebind_candidate_ledger(
        artifact,
        omitted_records,
    )
    omitted_root = tmp_path / "omitted-high-score"
    _place_candidate_ledger(omitted_root, omitted_artifact, omitted_bytes)
    with pytest.raises(
        HorizonModelError,
        match="fold prediction hash differs|row_count differs",
    ):
        verify_candidate_evaluation_ledger(omitted_artifact, omitted_root)

    duplicate_records = list(records)
    duplicate_records.insert(2, copy.deepcopy(duplicate_records[1]))
    duplicate_artifact, duplicate_bytes = _rebind_candidate_ledger(
        artifact,
        duplicate_records,
    )
    duplicate_root = tmp_path / "duplicate"
    _place_candidate_ledger(duplicate_root, duplicate_artifact, duplicate_bytes)
    with pytest.raises(
        HorizonModelError,
        match="duplicated or out of order",
    ):
        verify_candidate_evaluation_ledger(duplicate_artifact, duplicate_root)

    immature_records = copy.deepcopy(records)
    immature_records[1]["outcome_matures_on"] = (
        pd.Timestamp(artifact["training_cutoff"]) + pd.Timedelta(days=1)
    ).date().isoformat()
    immature_artifact, immature_bytes = _rebind_candidate_ledger(
        artifact,
        immature_records,
    )
    immature_root = tmp_path / "immature-label"
    _place_candidate_ledger(
        immature_root,
        immature_artifact,
        immature_bytes,
    )
    with pytest.raises(HorizonModelError, match="maturity clock differs"):
        verify_candidate_evaluation_ledger(
            immature_artifact,
            immature_root,
        )

    other_original_root = tmp_path / "other-original"
    other = train("candidate-ledger-other-suite", other_original_root)
    cross_suite, cross_suite_bytes = _rebind_candidate_ledger(
        other,
        records,
        preserve_header_hash=True,
    )
    cross_suite_root = tmp_path / "cross-suite"
    _place_candidate_ledger(
        cross_suite_root,
        cross_suite,
        cross_suite_bytes,
    )
    with pytest.raises(HorizonModelError, match="ledger header differs"):
        verify_candidate_evaluation_ledger(cross_suite, cross_suite_root)


def test_runtime_and_config_are_frozen_to_v3_ledger_bound_contract():
    root_config = load_v3_config()
    config = root_config["multi_horizon_forecasts"]
    assert root_config["strategy_version"] == "trading_v3.11.0-paper"
    assert root_config["frozen_at"].startswith("2026-08-17T")
    assert DEFAULT_SELECTION_POLICY.minimum_cross_section_size == 20
    assert DEFAULT_SELECTION_POLICY_HASH == (
        "824721cb771a3d73b4dcad9f7ff69acd300f74f291f2e87c81ad793a74b2d941"
    )
    assert config["protocol_version"] == (
        "INDEPENDENT_T1_T5_T20_LEDGER_BOUND_CONTRACT_V3"
    )
    assert config["model_protocol"] == MODEL_PROTOCOL
    with pytest.raises(HorizonModelError, match="threshold must remain frozen"):
        HorizonSelectionPolicy(minimum_expected_return_net_pct=0.01)
    with pytest.raises(HorizonModelError, match="threshold must remain frozen"):
        HorizonSelectionPolicy(minimum_probability_positive=0.51)
    assert config["training_policy"]["protocol_version"] == (
        TRAINING_CONFIG_PROTOCOL
    )
    assert config["training_policy"]["training_window_protocol"] == (
        TRAINING_WINDOW_PROTOCOL
    )
    assert config["training_policy"]["history_start"] == "2023-01-01"
    runtime = config["runtime_model_selection"]
    current = runtime["current_protocol"]
    assert current == {
        "artifact_schema": ARTIFACT_SCHEMA,
        "suite_schema": SUITE_SCHEMA,
        "model_protocol": MODEL_PROTOCOL,
        "selection_protocol": SELECTION_PROTOCOL,
        "selection_policy_hash": DEFAULT_SELECTION_POLICY_HASH,
        "candidate_ledger_schema": CANDIDATE_EVALUATION_LEDGER_SCHEMA,
        "candidate_ledger_binding_protocol": CANDIDATE_LEDGER_BINDING_PROTOCOL,
        "candidate_ledger_registration_required": True,
        "training_window_protocol": TRAINING_WINDOW_PROTOCOL,
        "history_start": "2023-01-01",
        "artifact_status_required": "OOS_VERIFIED",
        "contract_eligibility_scope": CONTRACT_ELIGIBILITY_SCOPE,
        "deployment_gate": False,
        "paper_eligible": False,
        "order_allowed": False,
    }
    assert runtime["historical_v1"] == {
        "artifact_schema": HISTORICAL_ARTIFACT_SCHEMA_V1,
        "suite_schema": HISTORICAL_SUITE_SCHEMA_V1,
        "mode": "AUDIT_ONLY",
        "runtime_selectable": False,
        "contract_eligible": False,
        "deployment_gate": False,
        "paper_eligible": False,
        "order_allowed": False,
    }
    assert runtime["historical_v2"] == {
        "artifact_schema": HISTORICAL_ARTIFACT_SCHEMA_V2,
        "suite_schema": HISTORICAL_SUITE_SCHEMA_V2,
        "mode": "AUDIT_ONLY",
        "missing_candidate_ledger": True,
        "runtime_selectable": False,
        "contract_eligible": False,
        "deployment_gate": False,
        "paper_eligible": False,
        "order_allowed": False,
    }
    assert runtime["fallback_prediction_kind"] == "PROXY_SCORE"
    assert runtime["fallback_proxy_models_enabled"] is True
    assert runtime["fallback_can_activate_model"] is False
    assert runtime["order_allowed"] is False
    for horizon, spec in HORIZON_MODEL_SPECS.items():
        configured = config["trainable_models"][f"T+{horizon}"]
        assert configured["model_key"] == spec.model_key
        assert configured["model_version"] == spec.model_version
        assert configured["algorithm"] == spec.algorithm
        assert tuple(configured["features"]) == spec.features

    bars = _bars(sessions=220, stocks=8)
    calendar = sorted(bars["trade_date"].unique())
    dataset = build_horizon_dataset(
        bars,
        1,
        trade_calendar=calendar,
        signal_start="2023-01-01",
        signal_end=pd.Timestamp(calendar[-4]).date(),
    )
    artifact = train_independent_horizon_model(
        dataset,
        release_id=_governance_release(1, "runtime-normalization-suite"),
        suite_release_id="runtime-normalization-suite",
        training_cutoff=pd.Timestamp(calendar[-1]).date(),
        policy=_relaxed_policy(),
        created_at="2025-01-01T00:00:00+00:00",
    )
    sample = dataset.frame.iloc[-1]
    runtime_frame = pd.DataFrame([{
        name: sample[name] for name in HORIZON_MODEL_SPECS[1].features
    }])
    raw, normalized = _normalized_model_score(
        artifact["final_model"], runtime_frame
    )
    expected, probability = _apply_calibration(
        artifact["calibration"], normalized
    )
    prediction = predict_horizon_artifact(
        artifact, runtime_frame.iloc[0].to_dict()
    )

    assert prediction.raw_expected_return_net_pct == pytest.approx(raw[0])
    assert prediction.expected_return_net_pct == pytest.approx(expected[0])
    assert prediction.probability_positive == pytest.approx(probability[0])
