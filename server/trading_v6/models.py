"""Independent, time-bound hurdle research model for Trading V6.

The historical V6 code called a clipped ridge output ``P(win)``.  This module
uses a real L2 logistic component for win probability, a ridge model for
positive payoff, and a ridge model for non-positive loss magnitude.  Outputs
are research scores only and can never create an action or order intent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import json
import math
from typing import Any, Mapping, Sequence
import weakref

from .pit_finance import PitFinanceContractError, PitFinanceFeature


HURDLE_PROTOCOL = "v6:pit-hurdle-logistic-ridge:v5"
LOGISTIC_PROTOCOL = "v6:l2-logistic-win-probability:v1"
RIDGE_PROTOCOL = "v6:l2-ridge-conditional-magnitude:v2"
SHANGHAI_OFFSET = timedelta(hours=8)
MAX_ABS_FEATURE_VALUE = 1_000_000.0
MAX_ABS_TARGET_VALUE = 10_000.0
V6_ALLOWED_FEATURES = frozenset(
    {
        "return_2d_pct", "return_5d_pct", "return_20d_pct", "return_60d_pct",
        "ma20_slope_5d_pct", "breakout_20d_proximity", "amount_ratio_5_20",
        "amount_ratio_1_20", "relative_strength_20d_pct", "distance_ma20_pct",
        "distance_ma5_pct", "drawdown_20d_pct", "rebound_from_low_pct",
        "previous_change_pct", "atr_14d_pct", "close_location_value",
        "market_return_20d_pct", "market_return_60d_pct", "market_breadth_pct",
        "breadth_change_5d_pct", "realized_volatility_20d_pct",
        "limit_down_ratio_pct", "market_health", "exit_sleeve_reversal",
        "quality_percentile", "cashflow_percentile", "valuation_percentile",
        "asset_liab_ratio_pit", "net_profit_yoy_gr_pit",
    }
)
_PIT_FINANCE_FEATURES = frozenset(
    {
        "quality_percentile",
        "cashflow_percentile",
        "valuation_percentile",
        "asset_liab_ratio_pit",
        "net_profit_yoy_gr_pit",
    }
)
_PREDICTION_FORBIDDEN_EXACT = frozenset(
    {
        "net_return_pct", "label_mature_at", "entry_at", "exit_at",
        "entry_price", "exit_price", "pnl", "profit", "mae", "mfe",
        "research_regime",
    }
)
_FORBIDDEN_PREFIXES = (
    "future_", "forward_", "target_", "label_", "regime_probability_",
)


class V6ResearchModelError(ValueError):
    """Raised when V6 research model or time contracts fail closed."""


class _FitToken:
    __slots__ = ("__weakref__",)


_FIT_ATTESTATIONS: weakref.WeakKeyDictionary[_FitToken, str] = (
    weakref.WeakKeyDictionary()
)


class _PredictionToken:
    __slots__ = ("__weakref__",)


_PREDICTION_ATTESTATIONS: weakref.WeakKeyDictionary[_PredictionToken, str] = (
    weakref.WeakKeyDictionary()
)


class _PredictionBatchToken:
    """Identity-only weak-reference token compatible with Python 3.10+."""

    __slots__ = ("__weakref__",)


_PREDICTION_BATCH_ATTESTATIONS: weakref.WeakKeyDictionary[
    _PredictionBatchToken,
    tuple[str, tuple[str, ...]],
] = weakref.WeakKeyDictionary()


@dataclass(frozen=True, slots=True)
class LogisticComponent:
    features: tuple[str, ...]
    medians: tuple[float, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    coefficients: tuple[float, ...]
    intercept: float
    l2_penalty: float
    training_sample_count: int
    component_sha256: str

    def __post_init__(self) -> None:
        self.assert_integrity()

    def assert_integrity(self) -> None:
        _validate_component_vectors(self)
        if _finite(self.l2_penalty, "l2_penalty") <= 0:
            raise V6ResearchModelError("l2_penalty must be positive")
        _sha256(self.component_sha256, "component_sha256")
        if self.component_sha256 != _canonical_sha256(_component_payload(self)):
            raise V6ResearchModelError("logistic component hash differs")

    def as_dict(self) -> dict[str, Any]:
        self.assert_integrity()
        return {**_component_payload(self), "component_sha256": self.component_sha256}


@dataclass(frozen=True, slots=True)
class RidgeComponent:
    purpose: str
    features: tuple[str, ...]
    medians: tuple[float, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    coefficients: tuple[float, ...]
    intercept: float
    l2_penalty: float
    training_sample_count: int
    component_sha256: str

    def __post_init__(self) -> None:
        self.assert_integrity()

    def assert_integrity(self) -> None:
        if self.purpose not in {"POSITIVE_PAYOFF", "NONPOSITIVE_LOSS_MAGNITUDE"}:
            raise V6ResearchModelError("ridge component purpose differs")
        _validate_component_vectors(self)
        if _finite(self.l2_penalty, "l2_penalty") <= 0:
            raise V6ResearchModelError("l2_penalty must be positive")
        _sha256(self.component_sha256, "component_sha256")
        if self.component_sha256 != _canonical_sha256(_component_payload(self)):
            raise V6ResearchModelError("ridge component hash differs")

    def as_dict(self) -> dict[str, Any]:
        self.assert_integrity()
        return {**_component_payload(self), "component_sha256": self.component_sha256}


@dataclass(frozen=True, slots=True)
class HurdleResearchModel:
    features: tuple[str, ...]
    win_probability_model: LogisticComponent
    positive_payoff_model: RidgeComponent
    nonpositive_loss_model: RidgeComponent
    training_cutoff_at: str
    prediction_not_before_at: str
    training_sample_count: int
    positive_sample_count: int
    nonpositive_sample_count: int
    minimum_feature_coverage: float
    target_clip_pct: tuple[float, float]
    minimum_component_samples: int
    embargo_calendar_days: int
    training_manifest_sha256: str
    feature_protocol_sha256: str
    model_integrity_sha256: str
    _fit_token: _FitToken = field(
        default_factory=_FitToken, init=False, repr=False, compare=False
    )

    @property
    def lifecycle_status(self) -> str:
        return "RESEARCH_ONLY"

    @property
    def activation_eligible(self) -> bool:
        return False

    @property
    def registration_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False

    def __post_init__(self) -> None:
        features = _normalize_features(self.features)
        for component in (
            self.win_probability_model,
            self.positive_payoff_model,
            self.nonpositive_loss_model,
        ):
            if component.features != features:
                raise V6ResearchModelError("hurdle component feature order differs")
        cutoff = _after_close_timestamp(
            self.training_cutoff_at, "training_cutoff_at"
        )
        not_before = _after_close_timestamp(
            self.prediction_not_before_at, "prediction_not_before_at"
        )
        if not_before < cutoff:
            raise V6ResearchModelError("prediction_not_before_at precedes cutoff")
        if (
            type(self.training_sample_count) is not int
            or type(self.positive_sample_count) is not int
            or type(self.nonpositive_sample_count) is not int
            or self.training_sample_count < 1
            or self.positive_sample_count < 1
            or self.nonpositive_sample_count < 1
            or self.positive_sample_count + self.nonpositive_sample_count
            != self.training_sample_count
        ):
            raise V6ResearchModelError("hurdle sample counts differ")
        coverage = _finite(
            self.minimum_feature_coverage, "minimum_feature_coverage"
        )
        if not 0.0 <= coverage <= 1.0:
            raise V6ResearchModelError("minimum_feature_coverage must be 0..1")
        if (
            not isinstance(self.target_clip_pct, tuple)
            or len(self.target_clip_pct) != 2
        ):
            raise V6ResearchModelError("target_clip_pct must be a two-value tuple")
        lower = _finite(self.target_clip_pct[0], "target_clip_pct lower")
        upper = _finite(self.target_clip_pct[1], "target_clip_pct upper")
        if lower >= 0 or upper <= 0 or lower >= upper:
            raise V6ResearchModelError("target_clip_pct must span zero")
        if (
            type(self.minimum_component_samples) is not int
            or self.minimum_component_samples < 2
            or self.positive_sample_count < self.minimum_component_samples
            or self.nonpositive_sample_count < self.minimum_component_samples
        ):
            raise V6ResearchModelError("minimum_component_samples differs")
        if type(self.embargo_calendar_days) is not int or self.embargo_calendar_days < 0:
            raise V6ResearchModelError("embargo_calendar_days must be non-negative")
        if not_before != cutoff + timedelta(days=self.embargo_calendar_days):
            raise V6ResearchModelError("prediction_not_before_at differs from embargo")
        _sha256(self.training_manifest_sha256, "training_manifest_sha256")
        _sha256(self.feature_protocol_sha256, "feature_protocol_sha256")
        self._assert_self_consistency()

    def _assert_self_consistency(self) -> None:
        for component in (
            self.win_probability_model,
            self.positive_payoff_model,
            self.nonpositive_loss_model,
        ):
            component.assert_integrity()
        _sha256(self.model_integrity_sha256, "model_integrity_sha256")
        if self.model_integrity_sha256 != _canonical_sha256(_model_payload(self)):
            raise V6ResearchModelError("hurdle model integrity hash differs")

    def assert_integrity(self) -> None:
        self._assert_self_consistency()
        expected = _FIT_ATTESTATIONS.get(self._fit_token)
        if expected is None:
            raise V6ResearchModelError(
                "model lacks a process-local fit attestation; reload is unsupported"
            )
        if expected != self.model_integrity_sha256:
            raise V6ResearchModelError("process-local fit attestation differs")

    def as_dict(self) -> dict[str, Any]:
        self.assert_integrity()
        return {
            **_model_payload(self),
            "model_integrity_sha256": self.model_integrity_sha256,
            "lifecycle_status": "RESEARCH_ONLY",
            "activation_eligible": False,
            "registration_eligible": False,
            "execution_eligible": False,
            "real_order_submission": False,
            "integrity_scope": (
                "SELF_CONSISTENCY_PLUS_PROCESS_LOCAL_FIT_ATTESTATION_"
                "NOT_EXTERNAL_TRUST"
            ),
            "serialized_model_reload_supported": False,
        }


@dataclass(frozen=True, slots=True)
class HurdleResearchScore:
    sample_id: str
    instrument_id: str | None
    signal_at: str
    win_probability: float
    expected_positive_payoff_pct: float
    expected_nonpositive_loss_pct: float
    expected_net_return_pct: float
    research_score: float
    prediction_input_sha256: str
    prediction_batch_sha256: str
    prediction_batch_size: int
    model_integrity_sha256: str
    pit_finance_snapshot_sha256: str | None
    finance_source_manifest_sha256: str | None
    finance_peer_manifest_sha256: str | None
    finance_peer_count: int | None
    score_integrity_sha256: str
    _prediction_batch_token: _PredictionBatchToken = field(
        repr=False,
        compare=False,
    )
    _prediction_token: _PredictionToken = field(
        default_factory=_PredictionToken, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        _text(self.sample_id, "sample_id")
        signal = _after_close_timestamp(self.signal_at, "signal_at")
        if _timestamp_text(signal) != self.signal_at:
            raise V6ResearchModelError("signal_at must use canonical ISO text")
        provenance = (
            self.instrument_id,
            self.pit_finance_snapshot_sha256,
            self.finance_source_manifest_sha256,
            self.finance_peer_manifest_sha256,
            self.finance_peer_count,
        )
        if any(value is not None for value in provenance):
            if any(value is None for value in provenance):
                raise V6ResearchModelError("PIT score provenance must be complete")
            _text(self.instrument_id, "instrument_id")
            _sha256(
                self.pit_finance_snapshot_sha256,
                "pit_finance_snapshot_sha256",
            )
            _sha256(
                self.finance_source_manifest_sha256,
                "finance_source_manifest_sha256",
            )
            _sha256(
                self.finance_peer_manifest_sha256,
                "finance_peer_manifest_sha256",
            )
            if type(self.finance_peer_count) is not int or self.finance_peer_count < 1:
                raise V6ResearchModelError("finance_peer_count must be positive")
        probability = _finite(self.win_probability, "win_probability")
        win = _finite(
            self.expected_positive_payoff_pct,
            "expected_positive_payoff_pct",
        )
        loss = _finite(
            self.expected_nonpositive_loss_pct,
            "expected_nonpositive_loss_pct",
        )
        expected = _finite(self.expected_net_return_pct, "expected_net_return_pct")
        score = _finite(self.research_score, "research_score")
        if not 0.0 <= probability <= 1.0:
            raise V6ResearchModelError("win_probability must be within [0, 1]")
        if win < 0 or loss < 0:
            raise V6ResearchModelError("conditional payoff magnitudes must be non-negative")
        if not 0.0 <= score <= 1.0:
            raise V6ResearchModelError("research_score must be within [0, 1]")
        recomputed_expected = probability * win - (1.0 - probability) * loss
        if not math.isclose(expected, recomputed_expected, rel_tol=1e-10, abs_tol=1e-10):
            raise V6ResearchModelError("expected_net_return_pct identity differs")
        recomputed_score = _sigmoid(expected / 3.0)
        if not math.isclose(score, recomputed_score, rel_tol=1e-10, abs_tol=1e-10):
            raise V6ResearchModelError("research_score identity differs")
        _sha256(self.prediction_input_sha256, "prediction_input_sha256")
        _sha256(self.prediction_batch_sha256, "prediction_batch_sha256")
        if type(self.prediction_batch_size) is not int or self.prediction_batch_size < 1:
            raise V6ResearchModelError("prediction_batch_size must be positive")
        _sha256(self.model_integrity_sha256, "model_integrity_sha256")
        _sha256(self.score_integrity_sha256, "score_integrity_sha256")
        if self.score_integrity_sha256 != _canonical_sha256(_score_payload(self)):
            raise V6ResearchModelError("research score integrity hash differs")

    def assert_integrity(self) -> None:
        if type(self) is not HurdleResearchScore:
            raise V6ResearchModelError("score must be exactly HurdleResearchScore")
        HurdleResearchScore.__post_init__(self)
        expected = _PREDICTION_ATTESTATIONS.get(self._prediction_token)
        if expected is None:
            raise V6ResearchModelError(
                "score lacks a process-local prediction attestation"
            )
        if expected != self.score_integrity_sha256:
            raise V6ResearchModelError("prediction attestation differs")
        batch_attestation = _PREDICTION_BATCH_ATTESTATIONS.get(
            self._prediction_batch_token
        )
        if batch_attestation is None:
            raise V6ResearchModelError("score lacks a process-local batch attestation")
        batch_hash, members = batch_attestation
        if (
            batch_hash != self.prediction_batch_sha256
            or len(members) != self.prediction_batch_size
        ):
            raise V6ResearchModelError("prediction batch attestation differs")

    @property
    def lifecycle_status(self) -> str:
        return "RESEARCH_ONLY"

    @property
    def activation_eligible(self) -> bool:
        return False

    def as_dict(self) -> dict[str, Any]:
        if type(self) is not HurdleResearchScore:
            raise V6ResearchModelError("score must be exactly HurdleResearchScore")
        HurdleResearchScore.assert_integrity(self)
        return {
            **_score_payload(self),
            "score_integrity_sha256": self.score_integrity_sha256,
            "lifecycle_status": "RESEARCH_ONLY",
            "activation_eligible": False,
            "registration_eligible": False,
            "execution_eligible": False,
            "actionable_output_allowed": False,
        }


def fit_hurdle_research_model(
    rows: Sequence[Mapping[str, Any]],
    *,
    features: Sequence[str],
    training_cutoff_at: Any,
    l2_penalty: float = 30.0,
    target_clip_pct: tuple[float, float] = (-12.0, 20.0),
    minimum_feature_coverage: float = 0.8,
    minimum_component_samples: int = 20,
    embargo_calendar_days: int = 0,
) -> HurdleResearchModel:
    requested = _normalize_features(features)
    cutoff = _after_close_timestamp(training_cutoff_at, "training_cutoff_at")
    if type(minimum_component_samples) is not int or minimum_component_samples < 2:
        raise V6ResearchModelError("minimum_component_samples must be at least 2")
    if type(embargo_calendar_days) is not int or embargo_calendar_days < 0:
        raise V6ResearchModelError("embargo_calendar_days must be non-negative")
    penalty = _finite(l2_penalty, "l2_penalty")
    if penalty <= 0:
        raise V6ResearchModelError("l2_penalty must be positive")
    lower, upper = (_finite(value, "target_clip_pct") for value in target_clip_pct)
    if lower >= 0 or upper <= 0:
        raise V6ResearchModelError("target_clip_pct must span zero")
    coverage = _finite(minimum_feature_coverage, "minimum_feature_coverage")
    if not 0.0 <= coverage <= 1.0:
        raise V6ResearchModelError("minimum_feature_coverage must be 0..1")
    training = _normalize_training_rows(rows, requested, cutoff)
    matrix, medians, means, scales = _design_training_matrix(
        training, requested, coverage
    )
    targets = [max(lower, min(upper, row["net_return_pct"])) for row in training]
    positive_indices = [index for index, value in enumerate(targets) if value > 0]
    nonpositive_indices = [index for index, value in enumerate(targets) if value <= 0]
    if (
        len(positive_indices) < minimum_component_samples
        or len(nonpositive_indices) < minimum_component_samples
    ):
        raise V6ResearchModelError(
            "hurdle model needs enough positive and non-positive mature rows"
        )
    binary = [1.0 if value > 0 else 0.0 for value in targets]
    logistic_values = _fit_logistic(matrix, binary, penalty)
    win_values = _fit_ridge(
        [matrix[index] for index in positive_indices],
        [targets[index] for index in positive_indices],
        penalty,
    )
    loss_values = _fit_ridge(
        [matrix[index] for index in nonpositive_indices],
        [-targets[index] for index in nonpositive_indices],
        penalty,
    )
    common = {
        "features": requested,
        "medians": medians,
        "means": means,
        "scales": scales,
        "l2_penalty": penalty,
    }
    probability_payload = {
        "protocol": LOGISTIC_PROTOCOL,
        **common,
        **logistic_values,
        "training_sample_count": len(training),
    }
    probability = LogisticComponent(
        **{key: value for key, value in probability_payload.items() if key != "protocol"},
        component_sha256=_canonical_sha256(probability_payload),
    )
    positive_payload = {
        "protocol": RIDGE_PROTOCOL,
        "purpose": "POSITIVE_PAYOFF",
        **common,
        **win_values,
        "training_sample_count": len(positive_indices),
    }
    positive = RidgeComponent(
        **{key: value for key, value in positive_payload.items() if key != "protocol"},
        component_sha256=_canonical_sha256(positive_payload),
    )
    loss_payload = {
        "protocol": RIDGE_PROTOCOL,
        "purpose": "NONPOSITIVE_LOSS_MAGNITUDE",
        **common,
        **loss_values,
        "training_sample_count": len(nonpositive_indices),
    }
    loss = RidgeComponent(
        **{key: value for key, value in loss_payload.items() if key != "protocol"},
        component_sha256=_canonical_sha256(loss_payload),
    )
    model_values = {
        "features": requested,
        "win_probability_model": probability,
        "positive_payoff_model": positive,
        "nonpositive_loss_model": loss,
        "training_cutoff_at": _timestamp_text(cutoff),
        "prediction_not_before_at": _timestamp_text(
            cutoff + timedelta(days=embargo_calendar_days)
        ),
        "training_sample_count": len(training),
        "positive_sample_count": len(positive_indices),
        "nonpositive_sample_count": len(nonpositive_indices),
        "minimum_feature_coverage": coverage,
        "target_clip_pct": (_stable_float(lower), _stable_float(upper)),
        "minimum_component_samples": minimum_component_samples,
        "embargo_calendar_days": embargo_calendar_days,
        "training_manifest_sha256": _canonical_sha256(
            [_training_manifest_row(row, requested) for row in training]
        ),
        "feature_protocol_sha256": _canonical_sha256(
            {
                "features": list(requested),
                "forbidden_prefixes": list(_FORBIDDEN_PREFIXES),
                "external_regime_inputs_forbidden": True,
                "pit_finance_features": sorted(
                    _PIT_FINANCE_FEATURES.intersection(requested)
                ),
                "pit_finance_builder_attestation_required": bool(
                    _PIT_FINANCE_FEATURES.intersection(requested)
                ),
                "pit_finance_snapshot_bound_in_manifests": bool(
                    _PIT_FINANCE_FEATURES.intersection(requested)
                ),
                "time_contract": (
                    "feature_available_at<=signal_at<label_mature_at"
                    "<=training_cutoff_at"
                ),
                "minimum_feature_coverage": coverage,
                "target_clip_pct": [_stable_float(lower), _stable_float(upper)],
                "minimum_component_samples": minimum_component_samples,
                "embargo_calendar_days": embargo_calendar_days,
            }
        ),
    }
    model = HurdleResearchModel(
        **model_values,
        model_integrity_sha256=_canonical_sha256(_model_payload(model_values)),
    )
    _FIT_ATTESTATIONS[model._fit_token] = model.model_integrity_sha256
    return model


def predict_hurdle_research_scores(
    model: HurdleResearchModel,
    rows: Sequence[Mapping[str, Any]],
) -> tuple[HurdleResearchScore, ...]:
    if type(model) is not HurdleResearchModel:
        raise TypeError("model must be exactly HurdleResearchModel")
    model.assert_integrity()
    prediction = _normalize_prediction_rows(
        rows,
        model.features,
        _after_close_timestamp(model.training_cutoff_at, "training_cutoff_at"),
        _after_close_timestamp(
            model.prediction_not_before_at, "prediction_not_before_at"
        ),
    )
    prediction_manifests = tuple(
        _prediction_manifest_row(row, model.features) for row in prediction
    )
    prediction_input_sha256s = tuple(
        _canonical_sha256(manifest) for manifest in prediction_manifests
    )
    prediction_member_sha256s = tuple(sorted(set(prediction_input_sha256s)))
    if len(prediction_member_sha256s) != len(prediction_input_sha256s):
        raise V6ResearchModelError("prediction batch contains duplicate inputs")
    prediction_batch_sha256 = _canonical_sha256(
        {
            "protocol": HURDLE_PROTOCOL,
            "model_integrity_sha256": model.model_integrity_sha256,
            "prediction_member_sha256s": prediction_member_sha256s,
        }
    )
    prediction_batch_token = _PredictionBatchToken()
    output: list[HurdleResearchScore] = []
    for row, input_sha256 in zip(
        prediction,
        prediction_input_sha256s,
        strict=True,
    ):
        vector = _prediction_vector(
            row, model.features, model.win_probability_model
        )
        probability = _sigmoid(
            model.win_probability_model.intercept
            + _dot(vector, model.win_probability_model.coefficients)
        )
        win = max(
            0.0,
            model.positive_payoff_model.intercept
            + _dot(vector, model.positive_payoff_model.coefficients),
        )
        loss = max(
            0.0,
            model.nonpositive_loss_model.intercept
            + _dot(vector, model.nonpositive_loss_model.coefficients),
        )
        expected = probability * win - (1.0 - probability) * loss
        score = _sigmoid(expected / 3.0)
        score_values = {
            "sample_id": row["sample_id"],
            "instrument_id": row.get("instrument_id"),
            "signal_at": _timestamp_text(row["signal_at"]),
            "win_probability": _stable_float(probability),
            "expected_positive_payoff_pct": _stable_float(win),
            "expected_nonpositive_loss_pct": _stable_float(loss),
            "expected_net_return_pct": _stable_float(expected),
            "research_score": _stable_float(score),
            "prediction_input_sha256": input_sha256,
            "prediction_batch_sha256": prediction_batch_sha256,
            "prediction_batch_size": len(prediction_input_sha256s),
            "model_integrity_sha256": model.model_integrity_sha256,
            "pit_finance_snapshot_sha256": row.get(
                "pit_finance_snapshot_sha256"
            ),
            "finance_source_manifest_sha256": row.get(
                "finance_source_manifest_sha256"
            ),
            "finance_peer_manifest_sha256": row.get(
                "finance_peer_manifest_sha256"
            ),
            "finance_peer_count": row.get("finance_peer_count"),
        }
        score = HurdleResearchScore(
            **score_values,
            score_integrity_sha256=_canonical_sha256(_score_payload(score_values)),
            _prediction_batch_token=prediction_batch_token,
        )
        _PREDICTION_ATTESTATIONS[score._prediction_token] = score.score_integrity_sha256
        output.append(score)
    _PREDICTION_BATCH_ATTESTATIONS[prediction_batch_token] = (
        prediction_batch_sha256,
        prediction_member_sha256s,
    )
    return tuple(output)


def validate_hurdle_research_score_batch(
    scores: Sequence[HurdleResearchScore],
) -> tuple[HurdleResearchScore, ...]:
    """Validate the only supported boundary before comparing/ranking scores."""

    if not isinstance(scores, Sequence) or isinstance(scores, (str, bytes)) or not scores:
        raise V6ResearchModelError("score batch must be a non-empty sequence")
    normalized = tuple(scores)
    for score in normalized:
        if type(score) is not HurdleResearchScore:
            raise V6ResearchModelError("score batch requires exact score types")
        HurdleResearchScore.assert_integrity(score)
    if len({score.model_integrity_sha256 for score in normalized}) != 1:
        raise V6ResearchModelError("score batch mixes model identities")
    if len({score.prediction_batch_sha256 for score in normalized}) != 1:
        raise V6ResearchModelError("score batch mixes prediction calls")
    declared_sizes = {score.prediction_batch_size for score in normalized}
    batch_token = normalized[0]._prediction_batch_token
    if (
        any(score._prediction_batch_token is not batch_token for score in normalized)
        or len(declared_sizes) != 1
    ):
        raise V6ResearchModelError("score batch commitments differ")
    attestation = _PREDICTION_BATCH_ATTESTATIONS.get(batch_token)
    if attestation is None:
        raise V6ResearchModelError("score batch lacks process-local attestation")
    attested_hash, expected_members = attestation
    if attested_hash != normalized[0].prediction_batch_sha256:
        raise V6ResearchModelError("score batch hash differs from attestation")
    actual_members = tuple(sorted(score.prediction_input_sha256 for score in normalized))
    expected_size = next(iter(declared_sizes))
    if len(normalized) != expected_size or actual_members != expected_members:
        raise V6ResearchModelError("score batch is incomplete or duplicated")
    sample_ids = [score.sample_id for score in normalized]
    if len(set(sample_ids)) != len(sample_ids):
        raise V6ResearchModelError("score batch repeats sample_id")
    economic_keys = [
        (score.instrument_id, score.signal_at)
        for score in normalized
        if score.instrument_id is not None
    ]
    if len(set(economic_keys)) != len(economic_keys):
        raise V6ResearchModelError("score batch repeats instrument signal key")
    cohorts: dict[str, tuple[str, str, int] | None] = {}
    for score in normalized:
        cohort = (
            (
                score.finance_source_manifest_sha256,
                score.finance_peer_manifest_sha256,
                score.finance_peer_count,
            )
            if score.finance_peer_manifest_sha256 is not None
            else None
        )
        if score.signal_at in cohorts and cohorts[score.signal_at] != cohort:
            raise V6ResearchModelError(
                "one score signal must share one PIT finance peer universe"
            )
        cohorts[score.signal_at] = cohort
    return normalized


def fit_and_score_hurdle_research(
    training_rows: Sequence[Mapping[str, Any]],
    prediction_rows: Sequence[Mapping[str, Any]],
    **fit_options: Any,
) -> tuple[dict[str, Any], tuple[HurdleResearchScore, ...]]:
    """One-shot supported API; it returns no persistable model object."""

    model = fit_hurdle_research_model(training_rows, **fit_options)
    scores = predict_hurdle_research_scores(model, prediction_rows)
    return model.as_dict(), validate_hurdle_research_score_batch(scores)


def _normalize_training_rows(
    rows: Sequence[Mapping[str, Any]],
    features: tuple[str, ...],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    normalized = _base_rows(rows, features, prediction=False)
    for row in normalized:
        label_at = _aware_shanghai_timestamp(
            row["label_mature_at"], "label_mature_at"
        )
        if label_at <= row["signal_at"]:
            raise V6ResearchModelError("label_mature_at must be after signal_at")
        if row["signal_at"] > cutoff or label_at > cutoff:
            raise V6ResearchModelError("training row is not mature by cutoff")
        target = _finite(row["net_return_pct"], "net_return_pct")
        if abs(target) > MAX_ABS_TARGET_VALUE:
            raise V6ResearchModelError("net_return_pct is outside the safe range")
        row["label_mature_at"] = label_at
        row["net_return_pct"] = target
    return normalized


def _normalize_prediction_rows(
    rows: Sequence[Mapping[str, Any]],
    features: tuple[str, ...],
    cutoff: datetime,
    not_before: datetime,
) -> list[dict[str, Any]]:
    normalized = _base_rows(rows, features, prediction=True)
    for row in normalized:
        if row["signal_at"] <= cutoff:
            raise V6ResearchModelError("prediction signal_at is not after cutoff")
        if row["signal_at"] < not_before:
            raise V6ResearchModelError("prediction signal_at precedes embargo")
    return normalized


def _base_rows(
    rows: Sequence[Mapping[str, Any]],
    features: tuple[str, ...],
    *,
    prediction: bool,
) -> list[dict[str, Any]]:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not rows:
        raise V6ResearchModelError("rows must be a non-empty sequence")
    output: list[dict[str, Any]] = []
    sample_ids: set[str] = set()
    finance_economic_keys: set[tuple[str, datetime]] = set()
    finance_cohorts: dict[datetime, tuple[str, str, int]] = {}
    requires_pit_finance = bool(_PIT_FINANCE_FEATURES.intersection(features))
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise V6ResearchModelError(f"row {index} must be an object")
        _reject_forbidden_columns(raw, prediction=prediction)
        sample_id = _text(raw.get("sample_id"), "sample_id")
        if sample_id in sample_ids:
            raise V6ResearchModelError("sample_id values must be unique")
        sample_ids.add(sample_id)
        signal = _after_close_timestamp(raw.get("signal_at"), "signal_at")
        available = _aware_shanghai_timestamp(
            raw.get("feature_available_at"), "feature_available_at"
        )
        if available > signal:
            raise V6ResearchModelError("feature_available_at exceeds signal_at")
        pit_finance: PitFinanceFeature | None = None
        if requires_pit_finance:
            candidate = raw.get("pit_finance_feature")
            if type(candidate) is not PitFinanceFeature:
                raise V6ResearchModelError(
                    "finance features require a process-local V6 PIT feature"
                )
            try:
                candidate.assert_integrity()
            except PitFinanceContractError as exc:
                raise V6ResearchModelError(
                    f"PIT finance feature integrity failed: {exc}"
                ) from exc
            if candidate.status != "PIT_RESEARCH_FEATURE_READY":
                raise V6ResearchModelError("PIT finance feature is DATA_BLOCKED")
            if candidate.sample_id != sample_id:
                raise V6ResearchModelError("PIT finance sample_id differs")
            pit_signal = _after_close_timestamp(
                candidate.signal_at, "pit_finance_feature.signal_at"
            )
            if pit_signal != signal:
                raise V6ResearchModelError("PIT finance signal_at differs")
            economic_key = (candidate.instrument_id, signal)
            if economic_key in finance_economic_keys:
                raise V6ResearchModelError(
                    "PIT finance instrument and signal keys must be unique"
                )
            finance_economic_keys.add(economic_key)
            cohort = (
                candidate.finance_source_manifest_sha256,
                candidate.finance_peer_manifest_sha256,
                candidate.finance_peer_count,
            )
            if signal in finance_cohorts and finance_cohorts[signal] != cohort:
                raise V6ResearchModelError(
                    "one signal cohort must share one PIT finance peer universe"
                )
            finance_cohorts[signal] = cohort
            knowledge = _aware_shanghai_timestamp(
                candidate.knowledge_at, "pit_finance_feature.knowledge_at"
            )
            market_available = _aware_shanghai_timestamp(
                candidate.market_feature_available_at,
                "pit_finance_feature.market_feature_available_at",
            )
            if available < max(knowledge, market_available):
                raise V6ResearchModelError(
                    "feature_available_at precedes bound PIT finance availability"
                )
            pit_finance = candidate
        elif "pit_finance_feature" in raw:
            raise V6ResearchModelError(
                "unused PIT finance feature is forbidden from the model manifest"
            )
        row: dict[str, Any] = {
            "sample_id": sample_id,
            "signal_at": signal,
            "feature_available_at": available,
        }
        if pit_finance is not None:
            row["instrument_id"] = pit_finance.instrument_id
            row["pit_finance_snapshot_sha256"] = (
                pit_finance.feature_snapshot_sha256
            )
            row["finance_source_manifest_sha256"] = (
                pit_finance.finance_source_manifest_sha256
            )
            row["finance_peer_manifest_sha256"] = (
                pit_finance.finance_peer_manifest_sha256
            )
            row["finance_peer_count"] = pit_finance.finance_peer_count
        if not prediction:
            if "label_mature_at" not in raw or "net_return_pct" not in raw:
                raise V6ResearchModelError("training row lacks target maturity fields")
            row["label_mature_at"] = raw["label_mature_at"]
            row["net_return_pct"] = raw["net_return_pct"]
        for feature in features:
            if feature in _PIT_FINANCE_FEATURES:
                assert pit_finance is not None
                value = getattr(pit_finance, feature)
                if feature in raw and raw[feature] != value:
                    raise V6ResearchModelError(
                        f"row finance value differs from PIT feature: {feature}"
                    )
            elif feature not in raw:
                raise V6ResearchModelError(f"row is missing feature: {feature}")
            else:
                value = raw[feature]
            if value is None:
                row[feature] = None
                continue
            numeric = _finite(value, feature)
            if abs(numeric) > MAX_ABS_FEATURE_VALUE:
                raise V6ResearchModelError(f"feature is outside the safe range: {feature}")
            row[feature] = numeric
        output.append(row)
    return sorted(output, key=lambda item: item["sample_id"])


def _design_training_matrix(
    rows: Sequence[Mapping[str, Any]],
    features: tuple[str, ...],
    minimum_coverage: float,
) -> tuple[list[list[float]], tuple[float, ...], tuple[float, ...], tuple[float, ...]]:
    medians: list[float] = []
    means: list[float] = []
    scales: list[float] = []
    columns: list[list[float]] = []
    for feature in features:
        observed = sorted(
            float(row[feature]) for row in rows if row[feature] is not None
        )
        coverage = len(observed) / len(rows)
        if not observed or coverage < minimum_coverage:
            raise V6ResearchModelError(
                f"feature coverage is below the frozen gate: {feature}"
            )
        median = _median(observed)
        values = [
            float(row[feature]) if row[feature] is not None else median
            for row in rows
        ]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        scale = math.sqrt(variance) if variance > 1e-24 else 1.0
        medians.append(median)
        means.append(mean)
        scales.append(scale)
        columns.append([(value - mean) / scale for value in values])
    matrix = [
        [_stable_float(columns[column][row]) for column in range(len(features))]
        for row in range(len(rows))
    ]
    return (
        matrix,
        _stable_floats(medians),
        _stable_floats(means),
        _stable_floats(scales),
    )


def _fit_logistic(
    matrix: list[list[float]], targets: list[float], penalty: float
) -> dict[str, Any]:
    columns = len(matrix[0])
    prevalence = (sum(targets) + 0.5) / (len(targets) + 1.0)
    parameters = [math.log(prevalence / (1.0 - prevalence)), *([0.0] * columns)]
    for _ in range(100):
        gradient = [0.0] * (columns + 1)
        hessian = [[0.0] * (columns + 1) for _ in range(columns + 1)]
        for row, target in zip(matrix, targets):
            design = [1.0, *row]
            probability = _sigmoid(_dot(design, parameters))
            residual = probability - target
            weight = max(probability * (1.0 - probability), 1e-12)
            for left in range(columns + 1):
                gradient[left] += residual * design[left]
                for right in range(columns + 1):
                    hessian[left][right] += weight * design[left] * design[right]
        hessian[0][0] += 1e-12
        for index in range(1, columns + 1):
            gradient[index] += penalty * parameters[index]
            hessian[index][index] += penalty
        delta = _solve_linear_system(hessian, gradient)
        parameters = [value - change for value, change in zip(parameters, delta)]
        if max(abs(change) for change in delta) < 1e-10:
            break
    else:
        raise V6ResearchModelError("L2 logistic fit did not converge")
    return {
        "intercept": _stable_float(parameters[0]),
        "coefficients": _stable_floats(parameters[1:]),
    }


def _fit_ridge(
    matrix: list[list[float]], targets: list[float], penalty: float
) -> dict[str, Any]:
    columns = len(matrix[0])
    normal = [[0.0] * (columns + 1) for _ in range(columns + 1)]
    right = [0.0] * (columns + 1)
    for row, target in zip(matrix, targets):
        design = [1.0, *row]
        for left in range(columns + 1):
            right[left] += design[left] * target
            for column in range(columns + 1):
                normal[left][column] += design[left] * design[column]
    for index in range(1, columns + 1):
        normal[index][index] += penalty
    parameters = _solve_linear_system(normal, right)
    return {
        "intercept": _stable_float(parameters[0]),
        "coefficients": _stable_floats(parameters[1:]),
    }


def _solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [list(row) + [vector[index]] for index, row in enumerate(matrix)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-15:
            raise V6ResearchModelError("model linear system is singular")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            if factor == 0:
                continue
            augmented[row] = [
                value - factor * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[column])
            ]
    result = [augmented[index][-1] for index in range(size)]
    if not all(math.isfinite(value) for value in result):
        raise V6ResearchModelError("model fit produced non-finite parameters")
    return result


def _prediction_vector(
    row: Mapping[str, Any],
    features: tuple[str, ...],
    component: LogisticComponent,
) -> list[float]:
    return [
        (
            (float(row[feature]) if row[feature] is not None else component.medians[index])
            - component.means[index]
        )
        / component.scales[index]
        for index, feature in enumerate(features)
    ]


def _normalize_features(features: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(features, Sequence) or isinstance(features, (str, bytes)):
        raise V6ResearchModelError("features must be an ordered sequence")
    result = tuple(features)
    if not result or len(result) != len(set(result)):
        raise V6ResearchModelError("features must be non-empty and unique")
    for feature in result:
        if not isinstance(feature, str) or feature not in V6_ALLOWED_FEATURES:
            raise V6ResearchModelError(f"feature is outside V6 allowlist: {feature}")
        if feature.startswith("regime_probability_") or feature == "research_regime":
            raise V6ResearchModelError("caller-supplied regime inputs are forbidden")
    return result


def _reject_forbidden_columns(row: Mapping[str, Any], *, prediction: bool) -> None:
    for raw_name in row:
        if not isinstance(raw_name, str):
            raise V6ResearchModelError("row column names must be text")
        name = raw_name.lower()
        if name == "research_regime" or name.startswith("regime_probability_"):
            raise V6ResearchModelError("caller-supplied regime inputs are forbidden")
        if any(name.startswith(prefix) for prefix in _FORBIDDEN_PREFIXES):
            if not (not prediction and name == "label_mature_at"):
                raise V6ResearchModelError(f"result-like column is forbidden: {raw_name}")
        if prediction and name in _PREDICTION_FORBIDDEN_EXACT:
            raise V6ResearchModelError(f"prediction result column is forbidden: {raw_name}")


def _validate_component_vectors(component: Any) -> None:
    features = _normalize_features(component.features)
    for label in ("medians", "means", "scales", "coefficients"):
        values = tuple(getattr(component, label))
        if len(values) != len(features):
            raise V6ResearchModelError(f"{label} length differs from features")
        if not all(math.isfinite(float(value)) for value in values):
            raise V6ResearchModelError(f"{label} must be finite")
    if any(float(value) <= 0 for value in component.scales):
        raise V6ResearchModelError("scales must be positive")
    _finite(component.intercept, "intercept")
    if type(component.training_sample_count) is not int or component.training_sample_count < 1:
        raise V6ResearchModelError("component training_sample_count must be positive")


def _component_payload(component: Any) -> dict[str, Any]:
    protocol = LOGISTIC_PROTOCOL if isinstance(component, LogisticComponent) else RIDGE_PROTOCOL
    payload = {
        "protocol": protocol,
        "features": list(component.features),
        "medians": list(component.medians),
        "means": list(component.means),
        "scales": list(component.scales),
        "coefficients": list(component.coefficients),
        "intercept": component.intercept,
        "l2_penalty": component.l2_penalty,
        "training_sample_count": component.training_sample_count,
    }
    if isinstance(component, RidgeComponent):
        payload["purpose"] = component.purpose
    return payload


def _model_payload(model: HurdleResearchModel | Mapping[str, Any]) -> dict[str, Any]:
    getter = (
        (lambda name: model[name])
        if isinstance(model, Mapping)
        else (lambda name: getattr(model, name))
    )
    return {
        "protocol": HURDLE_PROTOCOL,
        "features": list(getter("features")),
        "win_probability_component_sha256": getter("win_probability_model").component_sha256,
        "positive_payoff_component_sha256": getter("positive_payoff_model").component_sha256,
        "nonpositive_loss_component_sha256": getter("nonpositive_loss_model").component_sha256,
        "training_cutoff_at": getter("training_cutoff_at"),
        "prediction_not_before_at": getter("prediction_not_before_at"),
        "training_sample_count": getter("training_sample_count"),
        "positive_sample_count": getter("positive_sample_count"),
        "nonpositive_sample_count": getter("nonpositive_sample_count"),
        "minimum_feature_coverage": getter("minimum_feature_coverage"),
        "target_clip_pct": list(getter("target_clip_pct")),
        "minimum_component_samples": getter("minimum_component_samples"),
        "embargo_calendar_days": getter("embargo_calendar_days"),
        "training_manifest_sha256": getter("training_manifest_sha256"),
        "feature_protocol_sha256": getter("feature_protocol_sha256"),
    }


def _score_payload(score: HurdleResearchScore | Mapping[str, Any]) -> dict[str, Any]:
    getter = (
        (lambda name: score[name])
        if isinstance(score, Mapping)
        else (lambda name: getattr(score, name))
    )
    return {
        "protocol": HURDLE_PROTOCOL,
        "sample_id": getter("sample_id"),
        "instrument_id": getter("instrument_id"),
        "signal_at": getter("signal_at"),
        "win_probability": getter("win_probability"),
        "expected_positive_payoff_pct": getter("expected_positive_payoff_pct"),
        "expected_nonpositive_loss_pct": getter("expected_nonpositive_loss_pct"),
        "expected_net_return_pct": getter("expected_net_return_pct"),
        "research_score": getter("research_score"),
        "prediction_input_sha256": getter("prediction_input_sha256"),
        "prediction_batch_sha256": getter("prediction_batch_sha256"),
        "prediction_batch_size": getter("prediction_batch_size"),
        "model_integrity_sha256": getter("model_integrity_sha256"),
        "pit_finance_snapshot_sha256": getter("pit_finance_snapshot_sha256"),
        "finance_source_manifest_sha256": getter(
            "finance_source_manifest_sha256"
        ),
        "finance_peer_manifest_sha256": getter("finance_peer_manifest_sha256"),
        "finance_peer_count": getter("finance_peer_count"),
    }


def _training_manifest_row(
    row: Mapping[str, Any], features: tuple[str, ...]
) -> dict[str, Any]:
    result = {
        "sample_id": row["sample_id"],
        "signal_at": _timestamp_text(row["signal_at"]),
        "feature_available_at": _timestamp_text(row["feature_available_at"]),
        "label_mature_at": _timestamp_text(row["label_mature_at"]),
        "net_return_pct": _stable_float(row["net_return_pct"]),
        "features": {feature: row[feature] for feature in features},
    }
    if "pit_finance_snapshot_sha256" in row:
        result["instrument_id"] = row["instrument_id"]
        result["pit_finance_snapshot_sha256"] = row[
            "pit_finance_snapshot_sha256"
        ]
        result["finance_source_manifest_sha256"] = row[
            "finance_source_manifest_sha256"
        ]
        result["finance_peer_manifest_sha256"] = row[
            "finance_peer_manifest_sha256"
        ]
        result["finance_peer_count"] = row["finance_peer_count"]
    return result


def _prediction_manifest_row(
    row: Mapping[str, Any], features: tuple[str, ...]
) -> dict[str, Any]:
    result = {
        "sample_id": row["sample_id"],
        "signal_at": _timestamp_text(row["signal_at"]),
        "feature_available_at": _timestamp_text(row["feature_available_at"]),
        "features": {feature: row[feature] for feature in features},
    }
    if "pit_finance_snapshot_sha256" in row:
        result["instrument_id"] = row["instrument_id"]
        result["pit_finance_snapshot_sha256"] = row[
            "pit_finance_snapshot_sha256"
        ]
        result["finance_source_manifest_sha256"] = row[
            "finance_source_manifest_sha256"
        ]
        result["finance_peer_manifest_sha256"] = row[
            "finance_peer_manifest_sha256"
        ]
        result["finance_peer_count"] = row["finance_peer_count"]
    return result


def _aware_shanghai_timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise V6ResearchModelError(f"{label} must be an ISO timestamp") from exc
    else:
        raise V6ResearchModelError(f"{label} must be an ISO timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise V6ResearchModelError(f"{label} must be timezone-aware")
    if result.utcoffset() != SHANGHAI_OFFSET:
        raise V6ResearchModelError(f"{label} must use +08:00")
    return result


def _after_close_timestamp(value: Any, label: str) -> datetime:
    result = _aware_shanghai_timestamp(value, label)
    if (result.hour, result.minute, result.second) < (15, 0, 0):
        raise V6ResearchModelError(f"{label} must use AFTER_CLOSE time")
    return result


def _timestamp_text(value: datetime) -> str:
    return value.isoformat(timespec="auto")


def _median(values: Sequence[float]) -> float:
    midpoint = len(values) // 2
    if len(values) % 2:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / 2.0


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _sigmoid(value: float) -> float:
    if value >= 0:
        return 1.0 / (1.0 + math.exp(-min(value, 700.0)))
    exp_value = math.exp(max(value, -700.0))
    return exp_value / (1.0 + exp_value)


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise V6ResearchModelError(f"{label} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise V6ResearchModelError(f"{label} must be numeric") from exc
    if not math.isfinite(result):
        raise V6ResearchModelError(f"{label} must be finite")
    return result


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise V6ResearchModelError(f"{label} must be canonical non-empty text")
    return value


def _stable_float(value: Any) -> float:
    return float(f"{_finite(value, 'model value'):.15g}")


def _stable_floats(values: Sequence[Any]) -> tuple[float, ...]:
    return tuple(_stable_float(value) for value in values)


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _sha256(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise V6ResearchModelError(f"{label} must be a lowercase SHA-256")
    return value


__all__ = [
    "HURDLE_PROTOCOL",
    "HurdleResearchModel",
    "HurdleResearchScore",
    "V6ResearchModelError",
    "V6_ALLOWED_FEATURES",
    "fit_and_score_hurdle_research",
    "fit_hurdle_research_model",
    "predict_hurdle_research_scores",
    "validate_hurdle_research_score_batch",
]
