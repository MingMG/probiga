from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from server.trading_v6 import models as model_module
from server.trading_v6.models import (
    HurdleResearchScore,
    V6ResearchModelError,
    fit_hurdle_research_model,
    predict_hurdle_research_scores,
    validate_hurdle_research_score_batch,
)
from server.trading_v6.pit_finance import (
    PitFinanceFeature,
    build_pit_finance_features,
)


TZ8 = timezone(timedelta(hours=8))
CUTOFF = "2026-03-31T15:30:00+08:00"


def _training_rows(count=60):
    start = datetime(2026, 1, 1, 15, 30, tzinfo=TZ8)
    rows = []
    for index in range(count):
        signal = start + timedelta(days=index)
        feature = float(index % 10 - 5)
        target = 1.5 + feature * 0.15 if index % 2 else -(1.0 - feature * 0.05)
        rows.append(
            {
                "sample_id": f"train-{index:03d}",
                "signal_at": signal.isoformat(),
                "feature_available_at": (signal - timedelta(minutes=5)).isoformat(),
                "label_mature_at": (signal + timedelta(days=1)).isoformat(),
                "net_return_pct": target,
                "return_2d_pct": feature,
                "quality_percentile": (index + 1) / count,
            }
        )
    return rows


def _prediction_rows():
    return [
        {
            "sample_id": "predict-001",
            "signal_at": "2026-04-01T15:30:00+08:00",
            "feature_available_at": "2026-04-01T15:25:00+08:00",
            "return_2d_pct": 2.0,
            "quality_percentile": 0.8,
        },
        {
            "sample_id": "predict-002",
            "signal_at": "2026-04-02T15:30:00+08:00",
            "feature_available_at": "2026-04-02T15:25:00+08:00",
            "return_2d_pct": -2.0,
            "quality_percentile": 0.2,
        },
    ]


def _fit(rows=None):
    return fit_hurdle_research_model(
        rows or _training_rows(),
        features=["return_2d_pct"],
        training_cutoff_at=CUTOFF,
        l2_penalty=30.0,
        minimum_component_samples=20,
    )


def _pit_features_for_rows(rows):
    market_rows = [
        {
            "sample_id": row["sample_id"],
            "instrument_id": "000001",
            "signal_at": row["signal_at"],
            "feature_available_at": row["feature_available_at"],
            "raw_close": 10.0,
            "eligible_liquid": True,
        }
        for row in rows
    ]
    finance_rows = [
        {
            "statement_id": "statement-001",
            "instrument_id": "000001",
            "report_date": "2025-09-30",
            "notice_at": "2025-10-30T18:00:00+08:00",
            "knowledge_at": "2025-10-30T18:01:00+08:00",
            "net_asset_ps": 5.0,
            "oper_cf_ps": 1.0,
            "net_profit_yoy_gr": 10.0,
            "roe_wtd": 10.0,
            "gross_margin": 20.0,
            "net_margin": 5.0,
            "cash_flow_ratio": 2.0,
            "asset_liab_ratio": 40.0,
        }
    ]
    return build_pit_finance_features(market_rows, finance_rows)


def test_true_logistic_hurdle_is_deterministic_and_research_only() -> None:
    forward = _fit()
    reverse = _fit(list(reversed(_training_rows())))
    assert forward.as_dict() == reverse.as_dict()
    scores = predict_hurdle_research_scores(forward, _prediction_rows())
    assert len(scores) == 2
    assert all(0.0 < item.win_probability < 1.0 for item in scores)
    assert all(0.0 < item.research_score < 1.0 for item in scores)
    assert all(item.activation_eligible is False for item in scores)
    assert validate_hurdle_research_score_batch(scores) == scores
    assert scores[0].as_dict()["actionable_output_allowed"] is False
    assert forward.as_dict()["target_clip_pct"] == [-12.0, 20.0]
    assert forward.as_dict()["minimum_component_samples"] == 20


def test_label_and_prediction_time_boundaries_fail_closed() -> None:
    rows = _training_rows()
    rows[0]["label_mature_at"] = rows[0]["signal_at"]
    with pytest.raises(V6ResearchModelError, match="must be after"):
        _fit(rows)

    model = _fit()
    prediction = _prediction_rows()
    prediction[0]["signal_at"] = CUTOFF
    prediction[0]["feature_available_at"] = "2026-03-31T15:25:00+08:00"
    with pytest.raises(V6ResearchModelError, match="not after cutoff"):
        predict_hurdle_research_scores(model, prediction)


def test_future_and_external_regime_inputs_are_rejected() -> None:
    rows = _training_rows()
    rows[0]["future_net_return_hint"] = 99.0
    with pytest.raises(V6ResearchModelError, match="result-like"):
        _fit(rows)

    rows = _training_rows()
    rows[0]["research_regime"] = "TREND_UP"
    with pytest.raises(V6ResearchModelError, match="regime"):
        _fit(rows)

    rows = _training_rows()
    rows[0]["regime_probability_trend_up"] = 1.0
    with pytest.raises(V6ResearchModelError, match="regime"):
        _fit(rows)


def test_nonfinite_duplicate_and_future_feature_inputs_are_rejected() -> None:
    rows = _training_rows()
    rows[0]["return_2d_pct"] = float("inf")
    with pytest.raises(V6ResearchModelError, match="finite"):
        _fit(rows)
    rows = _training_rows()
    rows[1]["sample_id"] = rows[0]["sample_id"]
    with pytest.raises(V6ResearchModelError, match="unique"):
        _fit(rows)
    prediction = _prediction_rows()
    prediction[0]["feature_available_at"] = "2026-04-01T16:00:00+08:00"
    with pytest.raises(V6ResearchModelError, match="exceeds"):
        predict_hurdle_research_scores(_fit(), prediction)


def test_coordinated_model_rewrite_lacks_fit_attestation() -> None:
    model = _fit()
    payload = model_module._model_payload(model)
    payload["training_cutoff_at"] = "1900-01-01T15:30:00+08:00"
    payload["prediction_not_before_at"] = "1900-01-01T15:30:00+08:00"
    rewritten = model_module._canonical_sha256(payload)
    forged = replace(
        model,
        training_cutoff_at="1900-01-01T15:30:00+08:00",
        prediction_not_before_at="1900-01-01T15:30:00+08:00",
        model_integrity_sha256=rewritten,
    )
    with pytest.raises(V6ResearchModelError, match="fit attestation"):
        predict_hurdle_research_scores(forged, _prediction_rows())

    object.__setattr__(model, "training_cutoff_at", "1900-01-01T15:30:00+08:00")
    object.__setattr__(
        model, "prediction_not_before_at", "1900-01-01T15:30:00+08:00"
    )
    object.__setattr__(model, "model_integrity_sha256", rewritten)
    with pytest.raises(V6ResearchModelError, match="attestation differs"):
        predict_hurdle_research_scores(model, _prediction_rows())


def test_nested_component_mutation_is_rejected_before_prediction() -> None:
    model = _fit()
    original_hash = model.model_integrity_sha256
    object.__setattr__(
        model.win_probability_model,
        "coefficients",
        tuple(value + 1000.0 for value in model.win_probability_model.coefficients),
    )
    assert model.model_integrity_sha256 == original_hash
    with pytest.raises(V6ResearchModelError, match="component hash differs"):
        predict_hurdle_research_scores(model, _prediction_rows())


def test_finance_features_require_bound_builder_provenance() -> None:
    training = _training_rows()
    for row, feature in zip(training, _pit_features_for_rows(training), strict=True):
        row["pit_finance_feature"] = feature
        row.pop("quality_percentile")
    model = fit_hurdle_research_model(
        training,
        features=["quality_percentile"],
        training_cutoff_at=CUTOFF,
        l2_penalty=30.0,
        minimum_component_samples=20,
    )

    prediction = _prediction_rows()
    prediction[0]["quality_percentile"] = 999999.0
    with pytest.raises(V6ResearchModelError, match="process-local V6 PIT feature"):
        predict_hurdle_research_scores(model, prediction)

    for row, feature in zip(
        prediction, _pit_features_for_rows(prediction), strict=True
    ):
        row.pop("quality_percentile", None)
        row["pit_finance_feature"] = feature
    scores = predict_hurdle_research_scores(model, prediction)
    assert len(scores) == 2
    assert all(0.0 < item.win_probability < 1.0 for item in scores)
    assert all(item.instrument_id == "000001" for item in scores)
    assert all(item.finance_peer_count == 1 for item in scores)


def test_pit_feature_subclasses_are_rejected() -> None:
    class ForgedPitFeature(PitFinanceFeature):
        def __post_init__(self) -> None:
            return None

        def assert_integrity(self) -> None:
            return None

    forged = object.__new__(ForgedPitFeature)
    training = _training_rows()
    training[0]["pit_finance_feature"] = forged

    with pytest.raises(V6ResearchModelError, match="process-local V6 PIT feature"):
        fit_hurdle_research_model(
            training,
            features=["quality_percentile"],
            training_cutoff_at=CUTOFF,
            minimum_component_samples=20,
        )


def test_score_requires_valid_fields_and_prediction_attestation() -> None:
    score = predict_hurdle_research_scores(_fit(), _prediction_rows()[:1])[0]
    with pytest.raises(V6ResearchModelError, match="prediction attestation"):
        replace(score).as_dict()

    object.__setattr__(score, "win_probability", 999.0)
    with pytest.raises(V6ResearchModelError, match="win_probability"):
        score.as_dict()


def test_score_subclasses_cannot_override_integrity() -> None:
    class ForgedScore(HurdleResearchScore):
        def __post_init__(self) -> None:
            return None

        def assert_integrity(self) -> None:
            return None

    forged = object.__new__(ForgedScore)
    with pytest.raises(V6ResearchModelError, match="exactly HurdleResearchScore"):
        forged.as_dict()


def test_one_signal_cannot_mix_different_finance_peer_universes() -> None:
    training = _training_rows()
    for row, feature in zip(training, _pit_features_for_rows(training), strict=True):
        row["pit_finance_feature"] = feature
        row.pop("quality_percentile")
    model = fit_hurdle_research_model(
        training,
        features=["quality_percentile"],
        training_cutoff_at=CUTOFF,
        minimum_component_samples=20,
    )

    rows = _prediction_rows()
    rows[1]["signal_at"] = rows[0]["signal_at"]
    rows[1]["feature_available_at"] = rows[0]["feature_available_at"]
    for index, row in enumerate(rows, start=1):
        instrument = f"00000{index}"
        market = [{
            "sample_id": row["sample_id"],
            "instrument_id": instrument,
            "signal_at": row["signal_at"],
            "feature_available_at": row["feature_available_at"],
            "raw_close": 10.0 + index,
            "eligible_liquid": True,
        }]
        finance = [{
            "statement_id": f"statement-{index}",
            "instrument_id": instrument,
            "report_date": "2025-09-30",
            "notice_at": "2025-10-30T18:00:00+08:00",
            "knowledge_at": "2025-10-30T18:01:00+08:00",
            "net_asset_ps": 5.0,
            "oper_cf_ps": 1.0,
            "net_profit_yoy_gr": 10.0,
            "roe_wtd": 10.0,
            "gross_margin": 20.0,
            "net_margin": 5.0,
            "cash_flow_ratio": 2.0,
            "asset_liab_ratio": 40.0,
        }]
        row.pop("quality_percentile")
        row["pit_finance_feature"] = build_pit_finance_features(
            market, finance
        )[0]

    with pytest.raises(V6ResearchModelError, match="peer universe"):
        predict_hurdle_research_scores(model, rows)

    first = predict_hurdle_research_scores(model, rows[:1])
    second = predict_hurdle_research_scores(model, rows[1:])
    with pytest.raises(V6ResearchModelError, match="mixes prediction calls"):
        validate_hurdle_research_score_batch((*first, *second))


def test_batch_validation_rejects_missing_and_duplicate_members() -> None:
    scores = predict_hurdle_research_scores(_fit(), _prediction_rows())
    assert len(scores) == 2
    with pytest.raises(V6ResearchModelError, match="incomplete"):
        validate_hurdle_research_score_batch(scores[:1])
    with pytest.raises(V6ResearchModelError, match="incomplete|duplicate"):
        validate_hurdle_research_score_batch((scores[0], scores[0]))


def test_ridge_intercept_satisfies_the_unpenalized_normal_equation() -> None:
    matrix = [[2.0], [3.0], [7.0], [9.0]]
    targets = [1.0, 2.0, 4.0, 8.0]
    fitted = model_module._fit_ridge(matrix, targets, penalty=30.0)
    residual_sum = sum(
        target
        - (
            fitted["intercept"]
            + fitted["coefficients"][0] * row[0]
        )
        for row, target in zip(matrix, targets, strict=True)
    )
    assert residual_sum == pytest.approx(0.0, abs=1e-9)


def test_prediction_manifest_preserves_microsecond_timestamps() -> None:
    model = _fit()
    first = deepcopy(_prediction_rows()[0])
    second = deepcopy(_prediction_rows()[0])
    first["signal_at"] = "2026-04-01T15:30:00.000001+08:00"
    first["feature_available_at"] = "2026-04-01T15:25:00.000001+08:00"
    second["signal_at"] = "2026-04-01T15:30:00.000002+08:00"
    second["feature_available_at"] = "2026-04-01T15:25:00.000002+08:00"
    first_score = predict_hurdle_research_scores(model, [first])[0]
    second_score = predict_hurdle_research_scores(model, [second])[0]
    assert first_score.signal_at != second_score.signal_at
    assert first_score.prediction_input_sha256 != second_score.prediction_input_sha256
