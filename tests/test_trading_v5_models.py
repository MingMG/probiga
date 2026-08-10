from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from server.trading_v5 import models as model_module
from server.trading_v5.models import (
    ResearchTrainingError,
    fit_regime_expert_model,
    fit_ridge_return_model,
    predict_regime_expert_return,
    predict_ridge_return,
)
from server.trading_v5.regime import assess_regime


def _training_frame(rows: int = 12) -> pd.DataFrame:
    signal = pd.date_range("2026-01-01T15:00:00Z", periods=rows, freq="D")
    return pd.DataFrame(
        {
            "sample_id": [f"sample-{index:03d}" for index in range(rows)],
            "signal_at": signal,
            "feature_available_at": signal - pd.Timedelta(hours=1),
            "label_mature_at": signal + pd.Timedelta(days=2),
            "net_return_pct": [float(index % 7 - 3) for index in range(rows)],
            "return_2d_pct": [float(index) / 3.0 for index in range(rows)],
            "quality_percentile": [float(index) / rows for index in range(rows)],
            "market_return_20d_pct": [2.0] * rows,
            "market_breadth_pct": [55.0] * rows,
            "breadth_change_5d_pct": [3.0] * rows,
            "realized_volatility_20d_pct": [18.0] * rows,
            "limit_down_ratio_pct": [1.0] * rows,
            "market_input_manifest_sha256": ["a" * 64] * rows,
            "market_constituent_sample_count": [300] * rows,
        }
    )


def _cutoff() -> str:
    return "2026-01-20T15:00:00Z"


def _prediction_frame(rows: int = 4) -> pd.DataFrame:
    frame = _training_frame(rows)
    signal = pd.date_range("2026-01-21T15:00:00Z", periods=rows, freq="D")
    frame["sample_id"] = [f"prediction-{index:03d}" for index in range(rows)]
    frame["signal_at"] = signal
    frame["feature_available_at"] = signal - pd.Timedelta(hours=1)
    frame["label_mature_at"] = signal + pd.Timedelta(days=2)
    return frame


def test_ridge_fit_is_deterministic_after_sample_id_sort() -> None:
    frame = _training_frame()
    kwargs = {
        "features": ["return_2d_pct", "quality_percentile"],
        "ridge_lambda": 30.0,
        "target_clip": (-12.0, 20.0),
        "training_cutoff": _cutoff(),
        "minimum_feature_coverage": 0.8,
    }
    forward = fit_ridge_return_model(frame, **kwargs)
    reverse = fit_ridge_return_model(frame.iloc[::-1], **kwargs)
    assert forward.as_dict() == reverse.as_dict()
    assert forward.lifecycle_status == "RESEARCH_ONLY"
    assert forward.activation_eligible is False
    assert np.isfinite(predict_ridge_return(forward, _prediction_frame())).all()


def test_target_and_unordered_feature_inputs_are_rejected() -> None:
    frame = _training_frame()
    with pytest.raises(ResearchTrainingError, match="allowlist"):
        fit_ridge_return_model(
            frame,
            features=["net_return_pct"],
            ridge_lambda=1.0,
            target_clip=(-12.0, 20.0),
            training_cutoff=_cutoff(),
        )
    with pytest.raises(ResearchTrainingError, match="ordered"):
        fit_ridge_return_model(
            frame,
            features={"return_2d_pct", "quality_percentile"},
            ridge_lambda=1.0,
            target_clip=(-12.0, 20.0),
            training_cutoff=_cutoff(),
        )


def test_unmatured_label_is_rejected() -> None:
    frame = _training_frame()
    frame.loc[0, "label_mature_at"] = pd.Timestamp("2027-01-01T00:00:00Z")
    with pytest.raises(ResearchTrainingError, match="not fully mature"):
        fit_ridge_return_model(
            frame,
            features=["return_2d_pct"],
            ridge_lambda=1.0,
            target_clip=(-12.0, 20.0),
            training_cutoff=_cutoff(),
        )


def test_label_cannot_mature_at_signal_time() -> None:
    frame = _training_frame()
    frame["label_mature_at"] = frame["signal_at"]
    with pytest.raises(ResearchTrainingError, match="must be after"):
        fit_ridge_return_model(
            frame,
            features=["return_2d_pct"],
            ridge_lambda=1.0,
            target_clip=(-12.0, 20.0),
            training_cutoff=_cutoff(),
        )


def test_extreme_fit_and_prediction_values_fail_closed() -> None:
    frame = _training_frame()
    frame.loc[0, "return_2d_pct"] = 1.7e308
    with pytest.raises(ResearchTrainingError, match="frozen bounds"):
        fit_ridge_return_model(
            frame,
            features=["return_2d_pct"],
            ridge_lambda=1.0,
            target_clip=(-12.0, 20.0),
            training_cutoff=_cutoff(),
        )
    clean = _training_frame()
    model = fit_ridge_return_model(
        clean,
        features=["return_2d_pct"],
        ridge_lambda=1.0,
        target_clip=(-12.0, 20.0),
        training_cutoff=_cutoff(),
    )
    clean = _prediction_frame()
    clean.loc[0, "return_2d_pct"] = np.inf
    with pytest.raises(ResearchTrainingError, match="infinite"):
        predict_ridge_return(model, clean)


def test_model_lifecycle_fields_cannot_be_replaced() -> None:
    model = fit_ridge_return_model(
        _training_frame(),
        features=["return_2d_pct"],
        ridge_lambda=1.0,
        target_clip=(-12.0, 20.0),
        training_cutoff=_cutoff(),
    )
    with pytest.raises((TypeError, ValueError)):
        replace(model, activation_eligible=True)
    with pytest.raises((TypeError, ValueError)):
        replace(model, lifecycle_status="PRODUCTION")
    with pytest.raises(ResearchTrainingError, match="coefficients"):
        replace(model, coefficients=(float("inf"),))


def test_regime_expert_derives_state_and_ignores_caller_label() -> None:
    frame = _training_frame()
    frame["research_regime"] = ["WIN" if value > 0 else "LOSS" for value in frame["net_return_pct"]]
    model = fit_regime_expert_model(
        frame,
        features=["return_2d_pct"],
        ridge_lambda=30.0,
        target_clip=(-12.0, 20.0),
        training_cutoff=_cutoff(),
        minimum_regime_samples=2,
    )
    prediction = _prediction_frame()
    prediction["research_regime"] = "FAKE_FUTURE_LABEL"
    first = predict_regime_expert_return(model, prediction)
    prediction["research_regime"] = "FUTURE_LABEL_CHANGED"
    second = predict_regime_expert_return(model, prediction)
    assert np.array_equal(first, second)
    assert model.lifecycle_status == "RESEARCH_ONLY"
    assert model.activation_eligible is False
    with pytest.raises((TypeError, ValueError)):
        replace(model, activation_eligible=True)


def test_external_regime_probability_features_are_forbidden() -> None:
    frame = _training_frame()
    frame["regime_probability_trend_up"] = (
        frame["net_return_pct"] > 0
    ).astype(float)
    with pytest.raises(ResearchTrainingError, match="allowlist"):
        fit_ridge_return_model(
            frame,
            features=["regime_probability_trend_up"],
            ridge_lambda=1.0,
            target_clip=(-12.0, 20.0),
            training_cutoff=_cutoff(),
        )


def test_prediction_requires_strictly_post_training_samples() -> None:
    frame = _training_frame()
    model = fit_ridge_return_model(
        frame,
        features=["return_2d_pct"],
        ridge_lambda=1.0,
        target_clip=(-12.0, 20.0),
        training_cutoff=_cutoff(),
    )
    with pytest.raises(ResearchTrainingError, match="not after"):
        predict_ridge_return(model, frame)


def test_prediction_rechecks_model_integrity_after_cutoff_tamper() -> None:
    frame = _training_frame()
    model = fit_ridge_return_model(
        frame,
        features=["return_2d_pct"],
        ridge_lambda=1.0,
        target_clip=(-12.0, 20.0),
        training_cutoff=_cutoff(),
    )
    with pytest.raises(ResearchTrainingError, match="integrity hash"):
        replace(model, training_cutoff="1900-01-01T00:00:00Z")
    object.__setattr__(model, "training_cutoff", "1900-01-01T00:00:00Z")
    with pytest.raises(ResearchTrainingError, match="integrity hash"):
        predict_ridge_return(model, frame)


def test_coordinated_digest_rewrite_lacks_process_local_fit_attestation() -> None:
    frame = _training_frame()
    model = fit_ridge_return_model(
        frame,
        features=["return_2d_pct"],
        ridge_lambda=1.0,
        target_clip=(-12.0, 20.0),
        training_cutoff=_cutoff(),
    )
    payload = model_module._ridge_integrity_payload(model)
    payload["training_cutoff"] = "1900-01-01T00:00:00Z"
    rewritten_hash = model_module._sha256_json(payload)
    forged = replace(
        model,
        training_cutoff="1900-01-01T00:00:00Z",
        model_integrity_sha256=rewritten_hash,
    )
    with pytest.raises(ResearchTrainingError, match="process-local fit attestation"):
        predict_ridge_return(forged, frame)

    object.__setattr__(model, "training_cutoff", "1900-01-01T00:00:00Z")
    object.__setattr__(model, "model_integrity_sha256", rewritten_hash)
    with pytest.raises(ResearchTrainingError, match="fit attestation differs"):
        predict_ridge_return(model, frame)


def test_regime_input_contract_is_time_bound_and_bounded() -> None:
    kwargs = {
        "signal_at": "2026-01-01T15:00:00Z",
        "feature_available_at": "2026-01-01T14:00:00Z",
        "source_manifest_sha256": "b" * 64,
        "constituent_sample_count": 300,
        "market_return_20d_pct": 2.0,
        "market_breadth_pct": 55.0,
        "breadth_change_5d_pct": 3.0,
        "realized_volatility_20d_pct": 18.0,
        "limit_down_ratio_pct": 1.0,
    }
    assessment = assess_regime(**kwargs)
    assert assessment.research_only is True
    assert assessment.activation_eligible is False
    assert assessment.as_dict()["activation_eligible"] is False
    with pytest.raises((TypeError, ValueError)):
        replace(assessment, research_only=False)
    with pytest.raises(ValueError, match="within"):
        assess_regime(**{**kwargs, "market_breadth_pct": -999.0})
    with pytest.raises(ValueError, match="exceeds"):
        assess_regime(
            **{
                **kwargs,
                "feature_available_at": "2026-01-01T16:00:00Z",
            }
        )
