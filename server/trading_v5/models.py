"""Point-in-time model-research primitives owned by Trading V5.

These objects are permanently research-only.  They cannot carry an activation
claim, accept arbitrary label-like features, trust caller-provided regime
labels, or fit rows whose labels have not matured by an explicit cutoff.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
from typing import Any, Iterable, Mapping
import weakref

import numpy as np
import pandas as pd

from .regime import (
    REGIME_CONTEXT_COLUMNS,
    REGIME_ROUTER_INPUTS,
    REGIME_ROUTER_VERSION,
    REGIME_STATES,
    assess_regime,
)


RIDGE_PROTOCOL = "v5:point-in-time-ridge:v2"
REGIME_EXPERT_PROTOCOL = "v5:point-in-time-regime-expert-ridge:v2"
TIME_COLUMNS = ("signal_at", "feature_available_at", "label_mature_at")
SAMPLE_ID_COLUMN = "sample_id"
MAX_ABS_FEATURE_VALUE = 1_000_000.0
MAX_ABS_TARGET_VALUE = 10_000.0
V5_ALLOWED_FEATURES = frozenset(
    {
        "amount_ratio_1_20",
        "amount_ratio_5_20",
        "atr_14d_pct",
        "breadth_change_5d_pct",
        "breadth_ma20_change_5d_pct",
        "breakout_20d_proximity",
        "cashflow_percentile",
        "close_location_value",
        "distance_ma20_pct",
        "distance_ma5_pct",
        "drawdown_20d_pct",
        "exit_sleeve_reversal",
        "limit_down_breadth_pct",
        "limit_down_ratio_pct",
        "ma20_slope_5d_pct",
        "market_aligned_breadth_pct",
        "market_breadth_ma20_pct",
        "market_breadth_pct",
        "market_health",
        "market_return_20d_change_10d_pct",
        "market_return_20d_pct",
        "market_return_60d_pct",
        "previous_change_pct",
        "quality_percentile",
        "realized_volatility_20d_pct",
        "rebound_from_low_pct",
        "relative_strength_20d_pct",
        "return_20d_pct",
        "return_2d_pct",
        "return_5d_pct",
        "return_60d_pct",
        "valuation_percentile",
    }
)


class ResearchTrainingError(ValueError):
    """Raised when a V5 training or prediction boundary fails closed."""


class _FitToken:
    """Identity token for a model fitted in this Python process."""

    __slots__ = ("__weakref__",)


_FIT_ATTESTATIONS: weakref.WeakKeyDictionary[_FitToken, str] = (
    weakref.WeakKeyDictionary()
)


@dataclass(frozen=True, slots=True)
class RidgeReturnModel:
    features: tuple[str, ...]
    medians: tuple[float, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    coefficients: tuple[float, ...]
    intercept: float
    ridge_lambda: float
    target_clip: tuple[float, float]
    requested_features: tuple[str, ...]
    dropped_features: tuple[str, ...]
    feature_coverage: tuple[tuple[str, float], ...]
    training_cutoff: str
    training_sample_count: int
    feature_protocol_sha256: str
    training_manifest_sha256: str
    model_integrity_sha256: str
    _fit_token: _FitToken = field(
        default_factory=_FitToken,
        init=False,
        repr=False,
        compare=False,
    )

    @property
    def protocol(self) -> str:
        return RIDGE_PROTOCOL

    @property
    def lifecycle_status(self) -> str:
        return "RESEARCH_ONLY"

    @property
    def activation_eligible(self) -> bool:
        return False

    def __post_init__(self) -> None:
        requested = _validate_feature_tuple(self.requested_features, "requested")
        accepted = _validate_feature_tuple(self.features, "accepted")
        if not set(accepted).issubset(requested):
            raise ResearchTrainingError("accepted features are not requested")
        if len(self.medians) != len(accepted):
            raise ResearchTrainingError("median vector length differs from features")
        if len(self.means) != len(accepted):
            raise ResearchTrainingError("mean vector length differs from features")
        if len(self.scales) != len(accepted):
            raise ResearchTrainingError("scale vector length differs from features")
        if len(self.coefficients) != len(accepted):
            raise ResearchTrainingError("coefficient length differs from features")
        _finite_vector(self.medians, "medians")
        _finite_vector(self.means, "means")
        _finite_vector(self.coefficients, "coefficients")
        scales = _finite_vector(self.scales, "scales")
        if any(value <= 0 for value in scales):
            raise ResearchTrainingError("scales must be positive")
        _finite(self.intercept, "intercept")
        if _finite(self.ridge_lambda, "ridge_lambda") <= 0:
            raise ResearchTrainingError("ridge_lambda must be positive")
        if len(self.target_clip) != 2:
            raise ResearchTrainingError("target_clip must have two values")
        lower, upper = _finite_vector(self.target_clip, "target_clip")
        if lower >= upper:
            raise ResearchTrainingError("target_clip must be increasing")
        dropped = tuple(self.dropped_features)
        if len(dropped) != len(set(dropped)) or not set(dropped).issubset(requested):
            raise ResearchTrainingError("dropped features are invalid")
        if set(dropped).intersection(accepted):
            raise ResearchTrainingError("feature cannot be accepted and dropped")
        if set(dropped).union(accepted) != set(requested):
            raise ResearchTrainingError("requested feature disposition is incomplete")
        coverage = dict(self.feature_coverage)
        if len(coverage) != len(self.feature_coverage) or set(coverage) != set(requested):
            raise ResearchTrainingError("feature coverage does not match requests")
        if any(
            not math.isfinite(float(value)) or not 0.0 <= float(value) <= 1.0
            for value in coverage.values()
        ):
            raise ResearchTrainingError("feature coverage must be within 0..1")
        _aware_timestamp(self.training_cutoff, "training_cutoff")
        if type(self.training_sample_count) is not int or self.training_sample_count < 1:
            raise ResearchTrainingError("training_sample_count must be positive")
        _sha256(self.feature_protocol_sha256, "feature_protocol_sha256")
        _sha256(self.training_manifest_sha256, "training_manifest_sha256")
        self._assert_self_consistency()

    def _assert_self_consistency(self) -> None:
        _sha256(self.model_integrity_sha256, "model_integrity_sha256")
        expected = _sha256_json(_ridge_integrity_payload(self))
        if self.model_integrity_sha256 != expected:
            raise ResearchTrainingError("ridge model integrity hash differs")

    def assert_integrity(self) -> None:
        self._assert_self_consistency()
        _assert_process_local_fit(self._fit_token, self.model_integrity_sha256)

    def as_dict(self) -> dict[str, Any]:
        self.assert_integrity()
        return {
            "protocol": RIDGE_PROTOCOL,
            "features": list(self.features),
            "medians": list(self.medians),
            "means": list(self.means),
            "scales": list(self.scales),
            "coefficients": list(self.coefficients),
            "intercept": self.intercept,
            "ridge_lambda": self.ridge_lambda,
            "target_clip": list(self.target_clip),
            "requested_features": list(self.requested_features),
            "dropped_features": list(self.dropped_features),
            "feature_coverage": dict(self.feature_coverage),
            "training_cutoff": self.training_cutoff,
            "training_sample_count": self.training_sample_count,
            "feature_protocol_sha256": self.feature_protocol_sha256,
            "training_manifest_sha256": self.training_manifest_sha256,
            "model_integrity_sha256": self.model_integrity_sha256,
            "integrity_scope": (
                "SELF_CONSISTENCY_PLUS_PROCESS_LOCAL_FIT_ATTESTATION_"
                "NOT_EXTERNAL_TRUST"
            ),
            "serialized_model_reload_supported": False,
            "lifecycle_status": "RESEARCH_ONLY",
            "activation_eligible": False,
        }


@dataclass(frozen=True, slots=True)
class RegimeExpertModel:
    experts: tuple[tuple[str, RidgeReturnModel], ...]
    fallback: RidgeReturnModel
    minimum_regime_samples: int
    training_cutoff: str
    routing_manifest_sha256: str
    model_integrity_sha256: str
    _fit_token: _FitToken = field(
        default_factory=_FitToken,
        init=False,
        repr=False,
        compare=False,
    )

    @property
    def regime_column(self) -> str:
        return "research_regime"

    @property
    def router_version(self) -> str:
        return REGIME_ROUTER_VERSION

    @property
    def protocol(self) -> str:
        return REGIME_EXPERT_PROTOCOL

    @property
    def lifecycle_status(self) -> str:
        return "RESEARCH_ONLY"

    @property
    def activation_eligible(self) -> bool:
        return False

    def __post_init__(self) -> None:
        if type(self.fallback) is not RidgeReturnModel:
            raise ResearchTrainingError("fallback must be a V5 RidgeReturnModel")
        states = tuple(state for state, _ in self.experts)
        if len(states) != len(set(states)) or any(
            state not in REGIME_STATES for state in states
        ):
            raise ResearchTrainingError("regime expert states are invalid")
        if tuple(sorted(states)) != states:
            raise ResearchTrainingError("regime experts must use stable state order")
        for state, model in self.experts:
            if type(model) is not RidgeReturnModel:
                raise ResearchTrainingError(f"expert {state} has an invalid model")
            if model.training_cutoff != self.training_cutoff:
                raise ResearchTrainingError("expert training cutoff differs")
        if self.fallback.training_cutoff != self.training_cutoff:
            raise ResearchTrainingError("fallback training cutoff differs")
        if type(self.minimum_regime_samples) is not int or self.minimum_regime_samples < 1:
            raise ResearchTrainingError("minimum_regime_samples must be positive")
        _aware_timestamp(self.training_cutoff, "training_cutoff")
        _sha256(self.routing_manifest_sha256, "routing_manifest_sha256")
        self._assert_self_consistency()

    def _assert_self_consistency(self) -> None:
        self.fallback._assert_self_consistency()
        for _, expert in self.experts:
            expert._assert_self_consistency()
        _sha256(self.model_integrity_sha256, "model_integrity_sha256")
        expected = _sha256_json(_regime_integrity_payload(self))
        if self.model_integrity_sha256 != expected:
            raise ResearchTrainingError("regime model integrity hash differs")

    def assert_integrity(self) -> None:
        self.fallback.assert_integrity()
        for _, expert in self.experts:
            expert.assert_integrity()
        self._assert_self_consistency()
        _assert_process_local_fit(self._fit_token, self.model_integrity_sha256)

    def as_dict(self) -> dict[str, Any]:
        self.assert_integrity()
        return {
            "protocol": REGIME_EXPERT_PROTOCOL,
            "regime_column": "research_regime",
            "router_version": REGIME_ROUTER_VERSION,
            "minimum_regime_samples": self.minimum_regime_samples,
            "experts": {state: model.as_dict() for state, model in self.experts},
            "fallback": self.fallback.as_dict(),
            "training_cutoff": self.training_cutoff,
            "routing_manifest_sha256": self.routing_manifest_sha256,
            "model_integrity_sha256": self.model_integrity_sha256,
            "integrity_scope": (
                "SELF_CONSISTENCY_PLUS_PROCESS_LOCAL_FIT_ATTESTATION_"
                "NOT_EXTERNAL_TRUST"
            ),
            "serialized_model_reload_supported": False,
            "lifecycle_status": "RESEARCH_ONLY",
            "activation_eligible": False,
        }


def feature_availability_report(
    frame: pd.DataFrame,
    features: Iterable[str],
    *,
    minimum_coverage: float = 0.0,
) -> dict[str, Any]:
    requested = _normalize_features(features)
    threshold = _finite(minimum_coverage, "minimum_coverage")
    if not 0.0 <= threshold <= 1.0:
        raise ResearchTrainingError("minimum_coverage must be within 0..1")
    accepted: list[str] = []
    dropped: dict[str, str] = {}
    coverage: dict[str, float] = {}
    for column in requested:
        if column not in frame.columns:
            coverage[column] = 0.0
            dropped[column] = "MISSING_COLUMN"
            continue
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(
            dtype=float,
            copy=False,
        )
        finite = np.isfinite(values) & (np.abs(values) <= MAX_ABS_FEATURE_VALUE)
        ratio = float(finite.mean()) if len(values) else 0.0
        coverage[column] = ratio
        if not finite.any():
            dropped[column] = "NO_FINITE_BOUNDED_TRAINING_VALUE"
        elif ratio < threshold:
            dropped[column] = "TRAINING_COVERAGE_TOO_LOW"
        else:
            accepted.append(column)
    return {
        "protocol": "v5:train-only-feature-availability:v2",
        "minimum_coverage": threshold,
        "requested": list(requested),
        "accepted": accepted,
        "dropped": dropped,
        "coverage": coverage,
        "status": "PASS" if accepted else "BLOCK",
    }


def fit_ridge_return_model(
    frame: pd.DataFrame,
    *,
    features: Iterable[str],
    ridge_lambda: float,
    target_clip: tuple[float, float],
    training_cutoff: Any,
    minimum_feature_coverage: float = 0.0,
) -> RidgeReturnModel:
    requested = _normalize_features(features)
    cutoff, ordered = _validate_training_frame(
        frame,
        features=requested,
        training_cutoff=training_cutoff,
    )
    return _fit_validated_ridge(
        ordered,
        requested=requested,
        ridge_lambda=ridge_lambda,
        target_clip=target_clip,
        training_cutoff=cutoff,
        minimum_feature_coverage=minimum_feature_coverage,
    )


def fit_regime_expert_model(
    frame: pd.DataFrame,
    *,
    features: Iterable[str],
    training_cutoff: Any,
    ridge_lambda: float,
    target_clip: tuple[float, float],
    minimum_regime_samples: int = 160,
    minimum_feature_coverage: float = 0.80,
) -> RegimeExpertModel:
    requested = _normalize_features(features)
    if type(minimum_regime_samples) is not int or minimum_regime_samples < 1:
        raise ResearchTrainingError("minimum_regime_samples must be positive")
    routing_columns = (*REGIME_ROUTER_INPUTS, *REGIME_CONTEXT_COLUMNS)
    cutoff, ordered = _validate_training_frame(
        frame,
        features=tuple(dict.fromkeys((*requested, *routing_columns))),
        training_cutoff=training_cutoff,
        numeric_features=tuple(
            dict.fromkeys((*requested, *REGIME_ROUTER_INPUTS))
        ),
        allow_context_columns=True,
    )
    routed = ordered.copy()
    routed["research_regime"] = _derive_regime_states(routed)
    fallback = _fit_validated_ridge(
        routed,
        requested=requested,
        ridge_lambda=ridge_lambda,
        target_clip=target_clip,
        training_cutoff=cutoff,
        minimum_feature_coverage=minimum_feature_coverage,
    )
    experts: list[tuple[str, RidgeReturnModel]] = []
    for state, group in routed.groupby("research_regime", sort=True, observed=True):
        if len(group) < minimum_regime_samples:
            continue
        experts.append(
            (
                str(state),
                _fit_validated_ridge(
                    group,
                    requested=requested,
                    ridge_lambda=ridge_lambda,
                    target_clip=target_clip,
                    training_cutoff=cutoff,
                    minimum_feature_coverage=minimum_feature_coverage,
                ),
            )
        )
    regime_values = {
        "experts": tuple(experts),
        "fallback": fallback,
        "minimum_regime_samples": minimum_regime_samples,
        "training_cutoff": _timestamp_text(cutoff),
        "routing_manifest_sha256": _training_manifest_sha256(
            routed,
            tuple(dict.fromkeys((*requested, *routing_columns, "research_regime"))),
        ),
    }
    model = RegimeExpertModel(
        **regime_values,
        model_integrity_sha256=_sha256_json(
            _regime_integrity_payload(regime_values)
        ),
    )
    _register_process_local_fit(model._fit_token, model.model_integrity_sha256)
    return model


def predict_ridge_return(
    model: RidgeReturnModel,
    frame: pd.DataFrame,
) -> np.ndarray:
    if type(model) is not RidgeReturnModel:
        raise TypeError("model must be exactly RidgeReturnModel")
    model.assert_integrity()
    _validate_prediction_frame(frame, training_cutoff=model.training_cutoff)
    design = _design_matrix(
        frame,
        features=model.features,
        medians=np.asarray(model.medians),
        means=np.asarray(model.means),
        scales=np.asarray(model.scales),
    )
    with np.errstate(over="raise", invalid="raise", divide="raise"):
        try:
            result = model.intercept + design @ np.asarray(model.coefficients)
        except FloatingPointError as exc:
            raise ResearchTrainingError("prediction arithmetic overflowed") from exc
    if not np.isfinite(result).all():
        raise ResearchTrainingError("research predictions must be finite")
    return result


def predict_regime_expert_return(
    model: RegimeExpertModel,
    frame: pd.DataFrame,
) -> np.ndarray:
    if type(model) is not RegimeExpertModel:
        raise TypeError("model must be exactly RegimeExpertModel")
    model.assert_integrity()
    _require_regime_columns(frame)
    result = predict_ridge_return(model.fallback, frame)
    states = np.asarray(_derive_regime_states(frame), dtype=str)
    for state, expert in model.experts:
        mask = states == state
        if mask.any():
            result[mask] = predict_ridge_return(expert, frame.loc[mask])
    if not np.isfinite(result).all():
        raise ResearchTrainingError("regime predictions must be finite")
    return result


def prediction_to_score(prediction: np.ndarray) -> np.ndarray:
    """Convert an unvalidated research prediction into an ordering score."""

    values = np.asarray(prediction, dtype=float)
    if not np.isfinite(values).all():
        raise ResearchTrainingError("research predictions must be finite")
    clipped = np.clip(values / 3.0, -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _fit_validated_ridge(
    frame: pd.DataFrame,
    *,
    requested: tuple[str, ...],
    ridge_lambda: float,
    target_clip: tuple[float, float],
    training_cutoff: pd.Timestamp,
    minimum_feature_coverage: float,
) -> RidgeReturnModel:
    penalty_value = _finite(ridge_lambda, "ridge_lambda")
    if penalty_value <= 0:
        raise ResearchTrainingError("ridge_lambda must be positive")
    if not isinstance(target_clip, tuple) or len(target_clip) != 2:
        raise ResearchTrainingError("target_clip must be a two-item tuple")
    lower, upper = (
        _finite(target_clip[0], "target_clip lower"),
        _finite(target_clip[1], "target_clip upper"),
    )
    if lower >= upper:
        raise ResearchTrainingError("target_clip must be increasing")
    quality = feature_availability_report(
        frame,
        requested,
        minimum_coverage=minimum_feature_coverage,
    )
    columns = tuple(quality["accepted"])
    if not columns:
        raise ResearchTrainingError("no feature passes the availability gate")
    if len(frame) <= len(columns):
        raise ResearchTrainingError("not enough rows to fit ridge model")
    raw = frame.loc[:, list(columns)].apply(
        pd.to_numeric,
        errors="coerce",
    ).to_numpy(dtype=float, copy=True)
    raw[~np.isfinite(raw) | (np.abs(raw) > MAX_ABS_FEATURE_VALUE)] = np.nan
    with np.errstate(over="raise", invalid="raise", divide="raise"):
        try:
            medians = np.nanmedian(raw, axis=0)
            medians[~np.isfinite(medians)] = 0.0
            missing = np.isnan(raw)
            if missing.any():
                raw[missing] = np.take(medians, np.nonzero(missing)[1])
            means = raw.mean(axis=0)
            scales = raw.std(axis=0)
            scales[~np.isfinite(scales) | (scales < 1e-8)] = 1.0
            design = (raw - means) / scales
        except FloatingPointError as exc:
            raise ResearchTrainingError("feature normalization overflowed") from exc
    if not all(
        np.isfinite(item).all() for item in (raw, medians, means, scales, design)
    ):
        raise ResearchTrainingError("feature normalization produced non-finite values")
    target = pd.to_numeric(
        frame["net_return_pct"],
        errors="coerce",
    ).to_numpy(dtype=float, copy=True)
    target = np.clip(target, lower, upper)
    with np.errstate(over="raise", invalid="raise", divide="raise"):
        try:
            intercept = float(target.mean())
            penalty = np.eye(len(columns), dtype=float) * penalty_value
            gram = design.T @ design + penalty
            right = design.T @ (target - intercept)
            coefficients = np.linalg.solve(gram, right)
        except (FloatingPointError, np.linalg.LinAlgError) as exc:
            raise ResearchTrainingError("ridge fitting failed safely") from exc
    if not all(
        np.isfinite(item).all()
        for item in (target, gram, right, coefficients)
    ) or not math.isfinite(intercept):
        raise ResearchTrainingError("ridge fitting produced non-finite parameters")
    feature_protocol = {
        "requested_features": list(requested),
        "accepted_features": list(columns),
        "minimum_feature_coverage": float(minimum_feature_coverage),
        "time_columns": list(TIME_COLUMNS),
        "sample_id_column": SAMPLE_ID_COLUMN,
        "maximum_absolute_feature_value": MAX_ABS_FEATURE_VALUE,
    }
    model_values = {
        "features": columns,
        "medians": _stable_floats(medians),
        "means": _stable_floats(means),
        "scales": _stable_floats(scales),
        "coefficients": _stable_floats(coefficients),
        "intercept": _stable_float(intercept),
        "ridge_lambda": penalty_value,
        "target_clip": (lower, upper),
        "requested_features": requested,
        "dropped_features": tuple(quality["dropped"]),
        "feature_coverage": tuple(
            (column, float(quality["coverage"][column]))
            for column in requested
        ),
        "training_cutoff": _timestamp_text(training_cutoff),
        "training_sample_count": len(frame),
        "feature_protocol_sha256": _sha256_json(feature_protocol),
        "training_manifest_sha256": _training_manifest_sha256(frame, requested),
    }
    model = RidgeReturnModel(
        **model_values,
        model_integrity_sha256=_sha256_json(
            _ridge_integrity_payload(model_values)
        ),
    )
    _register_process_local_fit(model._fit_token, model.model_integrity_sha256)
    return model


def _validate_training_frame(
    frame: pd.DataFrame,
    *,
    features: tuple[str, ...],
    training_cutoff: Any,
    numeric_features: tuple[str, ...] | None = None,
    allow_context_columns: bool = False,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ResearchTrainingError("training frame must be a non-empty DataFrame")
    cutoff = _aware_timestamp(training_cutoff, "training_cutoff")
    allowed_context = set(REGIME_CONTEXT_COLUMNS) if allow_context_columns else set()
    for feature in features:
        if feature not in V5_ALLOWED_FEATURES and feature not in allowed_context:
            raise ResearchTrainingError(f"feature is outside the frozen allowlist: {feature}")
    required = {"net_return_pct", SAMPLE_ID_COLUMN, *TIME_COLUMNS, *features}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ResearchTrainingError("training frame missing columns: " + ", ".join(missing))
    sample_ids = frame[SAMPLE_ID_COLUMN].tolist()
    if any(not isinstance(item, str) or not item.strip() for item in sample_ids):
        raise ResearchTrainingError("sample_id values must be non-empty strings")
    if len(sample_ids) != len(set(sample_ids)):
        raise ResearchTrainingError("sample_id values must be unique")
    ordered = frame.sort_values(SAMPLE_ID_COLUMN, kind="mergesort").reset_index(drop=True)
    timestamps = {
        name: tuple(_aware_timestamp(value, name) for value in ordered[name].tolist())
        for name in TIME_COLUMNS
    }
    for index, (signal_at, feature_at, label_at) in enumerate(
        zip(
            timestamps["signal_at"],
            timestamps["feature_available_at"],
            timestamps["label_mature_at"],
        )
    ):
        if feature_at > signal_at:
            raise ResearchTrainingError(
                f"row {index} feature_available_at exceeds signal_at"
            )
        if label_at <= signal_at:
            raise ResearchTrainingError(
                f"row {index} label_mature_at must be after signal_at"
            )
        if signal_at > cutoff or label_at > cutoff:
            raise ResearchTrainingError(
                f"row {index} is not fully mature by training_cutoff"
            )
    target = pd.to_numeric(ordered["net_return_pct"], errors="coerce").to_numpy(
        dtype=float,
        copy=False,
    )
    if not np.isfinite(target).all() or (np.abs(target) > MAX_ABS_TARGET_VALUE).any():
        raise ResearchTrainingError("net_return_pct must be finite and bounded")
    for feature in numeric_features or features:
        if feature in allowed_context:
            continue
        values = pd.to_numeric(ordered[feature], errors="coerce").to_numpy(
            dtype=float,
            copy=False,
        )
        if np.isinf(values).any():
            raise ResearchTrainingError(f"feature is infinite: {feature}")
        finite = values[np.isfinite(values)]
        if finite.size and (np.abs(finite) > MAX_ABS_FEATURE_VALUE).any():
            raise ResearchTrainingError(f"feature exceeds frozen bounds: {feature}")
    return cutoff, ordered


def _derive_regime_states(frame: pd.DataFrame) -> list[str]:
    _require_regime_columns(frame)
    states: list[str] = []
    for row in frame.to_dict(orient="records"):
        states.append(
            assess_regime(
                signal_at=row["signal_at"],
                feature_available_at=row["feature_available_at"],
                source_manifest_sha256=row["market_input_manifest_sha256"],
                constituent_sample_count=row["market_constituent_sample_count"],
                **{name: row[name] for name in REGIME_ROUTER_INPUTS},
            ).dominant_state
        )
    return states


def _require_regime_columns(frame: pd.DataFrame) -> None:
    required = {
        "signal_at",
        "feature_available_at",
        *REGIME_ROUTER_INPUTS,
        *REGIME_CONTEXT_COLUMNS,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ResearchTrainingError("regime input missing columns: " + ", ".join(missing))


def _validate_prediction_frame(
    frame: pd.DataFrame,
    *,
    training_cutoff: str,
) -> None:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ResearchTrainingError("prediction frame must be a non-empty DataFrame")
    required = {SAMPLE_ID_COLUMN, "signal_at", "feature_available_at"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ResearchTrainingError(
            "prediction frame missing boundary columns: " + ", ".join(missing)
        )
    sample_ids = frame[SAMPLE_ID_COLUMN].tolist()
    if any(not isinstance(item, str) or not item.strip() for item in sample_ids):
        raise ResearchTrainingError("prediction sample_id values must be non-empty")
    if len(sample_ids) != len(set(sample_ids)):
        raise ResearchTrainingError("prediction sample_id values must be unique")
    cutoff = _aware_timestamp(training_cutoff, "model training_cutoff")
    for index, (signal_value, available_value) in enumerate(
        zip(frame["signal_at"].tolist(), frame["feature_available_at"].tolist())
    ):
        signal = _aware_timestamp(signal_value, "prediction signal_at")
        available = _aware_timestamp(
            available_value,
            "prediction feature_available_at",
        )
        if available > signal:
            raise ResearchTrainingError(
                f"prediction row {index} feature_available_at exceeds signal_at"
            )
        if signal <= cutoff:
            raise ResearchTrainingError(
                f"prediction row {index} is not after the model training cutoff"
            )


def _design_matrix(
    frame: pd.DataFrame,
    *,
    features: tuple[str, ...],
    medians: np.ndarray,
    means: np.ndarray,
    scales: np.ndarray,
) -> np.ndarray:
    missing = sorted(set(features) - set(frame.columns))
    if missing:
        raise ResearchTrainingError("prediction frame missing features: " + ", ".join(missing))
    values = frame.loc[:, list(features)].apply(
        pd.to_numeric,
        errors="coerce",
    ).to_numpy(dtype=float, copy=True)
    if np.isinf(values).any():
        raise ResearchTrainingError("prediction feature is infinite")
    too_large = np.isfinite(values) & (np.abs(values) > MAX_ABS_FEATURE_VALUE)
    if too_large.any():
        raise ResearchTrainingError("prediction feature exceeds frozen bounds")
    values[~np.isfinite(values)] = np.nan
    absent = np.isnan(values)
    if absent.any():
        values[absent] = np.take(medians, np.nonzero(absent)[1])
    with np.errstate(over="raise", invalid="raise", divide="raise"):
        try:
            design = (values - means) / scales
        except FloatingPointError as exc:
            raise ResearchTrainingError("prediction normalization overflowed") from exc
    if not np.isfinite(design).all():
        raise ResearchTrainingError("prediction design matrix is non-finite")
    return design


def _training_manifest_sha256(
    frame: pd.DataFrame,
    features: tuple[str, ...],
) -> str:
    rows: list[dict[str, Any]] = []
    for row in frame.sort_values(SAMPLE_ID_COLUMN, kind="mergesort").to_dict(
        orient="records"
    ):
        values: dict[str, Any] = {
            "sample_id": row[SAMPLE_ID_COLUMN],
            "signal_at": _timestamp_text(_aware_timestamp(row["signal_at"], "signal_at")),
            "feature_available_at": _timestamp_text(
                _aware_timestamp(row["feature_available_at"], "feature_available_at")
            ),
            "label_mature_at": _timestamp_text(
                _aware_timestamp(row["label_mature_at"], "label_mature_at")
            ),
            "net_return_pct": _stable_float(
                _finite(row["net_return_pct"], "net_return_pct")
            ),
        }
        for feature in features:
            raw = row[feature]
            if raw is None or pd.isna(raw):
                values[feature] = None
            elif feature == "market_input_manifest_sha256":
                values[feature] = str(raw)
            elif feature == "research_regime":
                values[feature] = str(raw)
            elif feature == "market_constituent_sample_count":
                values[feature] = int(raw)
            else:
                values[feature] = _stable_float(_finite(raw, feature))
        rows.append(values)
    return _sha256_json({"protocol": "v5:training-manifest:v1", "rows": rows})


def _normalize_features(features: Iterable[str]) -> tuple[str, ...]:
    if type(features) not in {list, tuple}:
        raise ResearchTrainingError("features must be an ordered list or tuple")
    result = tuple(features)
    if not result or any(not isinstance(item, str) or not item for item in result):
        raise ResearchTrainingError("features must contain non-empty strings")
    if len(result) != len(set(result)):
        raise ResearchTrainingError("features must be unique")
    forbidden = sorted(set(result) - V5_ALLOWED_FEATURES)
    if forbidden:
        raise ResearchTrainingError(
            "features are outside the frozen V5 allowlist: " + ", ".join(forbidden)
        )
    return result


def _validate_feature_tuple(value: tuple[str, ...], label: str) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise ResearchTrainingError(f"{label} features must be a tuple")
    return _normalize_features(list(value))


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise ResearchTrainingError(f"{label} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ResearchTrainingError(f"{label} must be numeric") from exc
    if not math.isfinite(result):
        raise ResearchTrainingError(f"{label} must be finite")
    return result


def _finite_vector(values: Iterable[Any], label: str) -> tuple[float, ...]:
    return tuple(_finite(value, label) for value in values)


def _aware_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ResearchTrainingError(f"{name} must be a timestamp") from exc
    if timestamp.tzinfo is None:
        raise ResearchTrainingError(f"{name} must be timezone-aware")
    return timestamp.tz_convert("UTC")


def _timestamp_text(value: pd.Timestamp) -> str:
    return value.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def _sha256(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ResearchTrainingError(f"{label} must be a lowercase SHA-256")
    return value


def _stable_float(value: Any) -> float:
    result = _finite(value, "model value")
    return float(format(result, ".15g"))


def _stable_floats(values: Iterable[Any]) -> tuple[float, ...]:
    return tuple(_stable_float(value) for value in values)


def _sha256_json(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _ridge_integrity_payload(
    value: RidgeReturnModel | Mapping[str, Any],
) -> dict[str, Any]:
    names = (
        "features",
        "medians",
        "means",
        "scales",
        "coefficients",
        "intercept",
        "ridge_lambda",
        "target_clip",
        "requested_features",
        "dropped_features",
        "feature_coverage",
        "training_cutoff",
        "training_sample_count",
        "feature_protocol_sha256",
        "training_manifest_sha256",
    )
    return {
        "protocol": RIDGE_PROTOCOL,
        **{
            name: value[name] if isinstance(value, Mapping) else getattr(value, name)
            for name in names
        },
    }


def _regime_integrity_payload(
    value: RegimeExpertModel | Mapping[str, Any],
) -> dict[str, Any]:
    getter = (
        (lambda name: value[name])
        if isinstance(value, Mapping)
        else (lambda name: getattr(value, name))
    )
    experts = getter("experts")
    fallback = getter("fallback")
    return {
        "protocol": REGIME_EXPERT_PROTOCOL,
        "router_version": REGIME_ROUTER_VERSION,
        "experts": [
            [state, model.model_integrity_sha256]
            for state, model in experts
        ],
        "fallback_model_integrity_sha256": fallback.model_integrity_sha256,
        "minimum_regime_samples": getter("minimum_regime_samples"),
        "training_cutoff": getter("training_cutoff"),
        "routing_manifest_sha256": getter("routing_manifest_sha256"),
    }


def _register_process_local_fit(token: _FitToken, digest: str) -> None:
    """Bind one fitted instance without claiming a persistent trust anchor."""

    _FIT_ATTESTATIONS[token] = _sha256(digest, "model_integrity_sha256")


def _assert_process_local_fit(token: _FitToken, digest: str) -> None:
    expected = _FIT_ATTESTATIONS.get(token)
    if expected is None:
        raise ResearchTrainingError(
            "model lacks a process-local fit attestation; serialized reload is unsupported"
        )
    if expected != digest:
        raise ResearchTrainingError("process-local fit attestation differs")


__all__ = [
    "MAX_ABS_FEATURE_VALUE",
    "REGIME_EXPERT_PROTOCOL",
    "RIDGE_PROTOCOL",
    "RegimeExpertModel",
    "ResearchTrainingError",
    "RidgeReturnModel",
    "V5_ALLOWED_FEATURES",
    "feature_availability_report",
    "fit_regime_expert_model",
    "fit_ridge_return_model",
    "predict_regime_expert_return",
    "predict_ridge_return",
    "prediction_to_score",
]
