"""Independent, point-in-time T+1/T+5/T+20 research models.

This module owns the *model body* behind the Trading V3 multi-horizon shadow
loop.  It deliberately has no database or order-writing dependency.  A model
is trained from daily bars, validated by expanding walk-forward folds split on
exchange sessions, calibrated only from out-of-sample predictions, and stored
as an executable-free JSON artifact plus a content-addressed, deterministic
gzip JSONL sidecar containing the complete prequential OOS candidate ledger.

The artifact is research evidence, not a production signature.  Even a PASS
artifact has ``order_authority=False`` and still needs the external release
governance/attestation boundary before it may be registered by Shadow.
"""

from __future__ import annotations

import copy
import gzip
import hashlib
import io
import json
import math
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .config import config_hash as current_config_hash, load_v3_config
from .versioning import code_version


ARTIFACT_SCHEMA = "probiga.trading-v3.independent-horizon-model-artifact.v3"
SUITE_SCHEMA = "probiga.trading-v3.independent-horizon-model-suite.v3"
HISTORICAL_ARTIFACT_SCHEMA_V1 = (
    "probiga.trading-v3.independent-horizon-model-artifact.v1"
)
HISTORICAL_SUITE_SCHEMA_V1 = (
    "probiga.trading-v3.independent-horizon-model-suite.v1"
)
HISTORICAL_ARTIFACT_SCHEMA_V2 = (
    "probiga.trading-v3.independent-horizon-model-artifact.v2"
)
HISTORICAL_SUITE_SCHEMA_V2 = (
    "probiga.trading-v3.independent-horizon-model-suite.v2"
)
DATASET_SCHEMA = "probiga.trading-v3.horizon-training-dataset.v3"
FEATURE_PROTOCOL_SCHEMA = "probiga.trading-v3.horizon-feature-protocol.v2"
MODEL_PROTOCOL = "POINT_IN_TIME_INDEPENDENT_NUMPY_REGRESSION_V2"
CALIBRATION_PROTOCOL = "MATURITY_PURGED_PREQUENTIAL_ISOTONIC_BINS_V2"
SCORE_NORMALIZATION_PROTOCOL = "FROZEN_TRAINING_SCORE_ZSCORE_V2"
SELECTION_PROTOCOL = "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC_V2"
PSI_PROTOCOL = "FROZEN_PRE_OOS_ANCHOR_SCORE_PSI_V2"
MULTI_HORIZON_CONFIG_PROTOCOL = (
    "INDEPENDENT_T1_T5_T20_LEDGER_BOUND_CONTRACT_V3"
)
TRAINING_CONFIG_PROTOCOL = "POINT_IN_TIME_MATURITY_PURGED_PREQUENTIAL_OOS_V3"
TRAINING_WINDOW_PROTOCOL = "FROZEN_CONFIG_SIGNAL_START_INCLUSIVE_V1"
DEFAULT_HISTORY_START = date(2023, 1, 1)
CONTRACT_ELIGIBILITY_SCOPE = "SHADOW_CONTRACT_ONLY"
CANDIDATE_EVALUATION_LEDGER_SCHEMA = (
    "probiga.trading-v3.prequential-candidate-evaluation-ledger.v1"
)
CANDIDATE_LEDGER_BINDING_PROTOCOL = (
    "FULL_PREQUENTIAL_OOS_CANDIDATE_LEDGER_CONTENT_ADDRESS_V1"
)
CANDIDATE_LEDGER_ENCODING = "DETERMINISTIC_GZIP_CANONICAL_JSONL_V1"
CANDIDATE_LEDGER_REGISTRATION_PROTOCOL = (
    "STREAM_VERIFIED_CANDIDATE_LEDGER_REGISTRATION_V1"
)
LABEL_PROTOCOL = (
    "SIGNAL_CLOSE_NEXT_SESSION_OPEN_TO_HORIZON_CLOSE_"
    "ROUNDTRIP_COST_NET_ZERO_VOLUME_QUARANTINE_V2"
)
MODEL_CODE_VERSION = "trading-v3-independent-horizons.3.2.0"
EXCHANGE_TIMEZONE = ZoneInfo("Asia/Shanghai")
SUPPORTED_HORIZONS = (1, 5, 20)
_EPHEMERAL_CANDIDATE_LEDGER_BYTES: dict[str, bytes] = {}
_MAX_EPHEMERAL_CANDIDATE_LEDGER_BYTES = 64 * 1024 * 1024
_MAX_CANDIDATE_LEDGER_RECORD_BYTES = 16 * 1024
HASH_FIELDS = frozenset(
    {
        "artifact_hash",
        "attestation_hash",
        "calibration_hash",
        "evidence_hash",
        "fold_hash",
        "model_hash",
        "policy_hash",
        "suite_hash",
    }
)


class HorizonModelError(ValueError):
    """Raised when point-in-time training or artifact integrity fails."""


def horizon_governance_release_id(
    *,
    suite_release_id: str,
    model_key: str,
    model_version: str,
    horizon_days: int,
) -> str:
    """Return the immutable per-suite governance identity for one horizon.

    Model key/version values remain reusable across retraining suites.  The
    suite binding prevents a new calibration batch from sharing release state
    with an older artifact without introducing an artifact-hash cycle.
    """

    suite_id = str(suite_release_id or "").strip()
    key = str(model_key or "").strip()
    version = str(model_version or "").strip()
    try:
        horizon = int(horizon_days)
    except (TypeError, ValueError) as exc:
        raise HorizonModelError("release horizon must be an integer") from exc
    if not suite_id or not key or not version:
        raise HorizonModelError("release suite/model identity must not be empty")
    if horizon not in SUPPORTED_HORIZONS:
        raise HorizonModelError("release horizon is unsupported")
    release_id = f"{suite_id}:{key}:{version}:T+{horizon}"
    if len(release_id) > 160:
        raise HorizonModelError("governance release_id exceeds 160 characters")
    return release_id


def canonical_json(value: Any) -> str:
    """Return the one canonical representation used by every artifact hash."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise HorizonModelError("value is not canonical JSON") from exc


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _without_hash(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    result = copy.deepcopy(dict(value))
    result.pop(field, None)
    return result


def _artifact_core_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    """Exclude the non-deterministic creation envelope from the core hash."""

    result = copy.deepcopy(dict(value))
    for field in ("artifact_hash", "created_at", "creation_envelope_hash"):
        result.pop(field, None)
    return result


def _digest(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if len(result) != 64 or any(item not in "0123456789abcdef" for item in result):
        raise HorizonModelError(f"{field} must be a SHA-256 digest")
    return result


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise HorizonModelError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise HorizonModelError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise HorizonModelError(f"{field} must be finite")
    return result


def _aware_timestamp(value: Any, field: str) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise HorizonModelError(f"{field} must be a timestamp") from exc
    if result.tzinfo is None:
        raise HorizonModelError(f"{field} must include a timezone")
    return result.tz_convert("UTC")


def _date_value(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        raise HorizonModelError(f"{field} must be a date, not datetime")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise HorizonModelError(f"{field} must be an ISO date") from exc


def _stable_float(value: Any) -> float:
    result = _finite(value, "numeric value")
    rounded = round(result, 12)
    return 0.0 if rounded == 0 else rounded


def _shanghai_timestamp(day: date, clock: time) -> str:
    return datetime.combine(day, clock, EXCHANGE_TIMEZONE).isoformat()


FEATURE_FORMULAS: Mapping[str, str] = MappingProxyType(
    {
        "return_1d_pct": "adjusted_close.pct_change(1)*100",
        "return_5d_pct": "adjusted_close.pct_change(5)*100",
        "return_20d_pct": "adjusted_close.pct_change(20)*100",
        "return_60d_pct": "adjusted_close.pct_change(60)*100",
        "overnight_gap_pct": "raw_open/pre_close-1, known by signal close",
        "intraday_return_pct": "raw_close/raw_open-1, known by signal close",
        "amount_ratio_1_20": "amount/rolling_mean(amount,20)",
        "amount_ratio_5_20": "rolling_mean(amount,5)/rolling_mean(amount,20)",
        "amount_ratio_20_60": "rolling_mean(amount,20)/rolling_mean(amount,60)",
        "range_1d_pct": "(raw_high-raw_low)/pre_close*100",
        "close_location_value": "(raw_close-raw_low)/(raw_high-raw_low)",
        "volatility_5d_pct": "rolling_std(adjusted_daily_return,5)*100",
        "volatility_20d_pct": "rolling_std(adjusted_daily_return,20)*100",
        "distance_ma5_pct": "adjusted_close/rolling_mean(adjusted_close,5)-1",
        "distance_ma20_pct": "adjusted_close/rolling_mean(adjusted_close,20)-1",
        "drawdown_20d_pct": "adjusted_close/rolling_max(adjusted_close,20)-1",
        "drawdown_60d_pct": "adjusted_close/rolling_max(adjusted_close,60)-1",
        "ma20_slope_5d_pct": "ma20/ma20.shift(5)-1",
        "ma60_slope_10d_pct": "ma60/ma60.shift(10)-1",
        "relative_return_1d_pct": "return_1d_pct-cross_section_median(return_1d_pct)",
        "relative_return_20d_pct": "return_20d_pct-cross_section_median(return_20d_pct)",
    }
)


@dataclass(frozen=True, slots=True)
class HorizonModelSpec:
    horizon_days: int
    model_key: str
    model_version: str
    algorithm: str
    features: tuple[str, ...]
    ridge_lambda: float
    target_clip_pct: tuple[float, float]
    maximum_history_sessions: int
    cost_assumption_pct: float = 0.20
    cost_model_version: str = "ROUNDTRIP_COST_ASSUMPTION_V1"
    huber_delta: float | None = None
    recency_half_life_sessions: int | None = None

    def __post_init__(self) -> None:
        if self.horizon_days not in SUPPORTED_HORIZONS:
            raise HorizonModelError("unsupported model horizon")
        if not self.model_key or not self.model_version:
            raise HorizonModelError("model key/version must not be empty")
        if not self.features or len(self.features) != len(set(self.features)):
            raise HorizonModelError("features must be unique and non-empty")
        unknown = sorted(set(self.features) - set(FEATURE_FORMULAS))
        if unknown:
            raise HorizonModelError("unknown feature formulas: " + ", ".join(unknown))
        if _finite(self.ridge_lambda, "ridge_lambda") <= 0:
            raise HorizonModelError("ridge_lambda must be positive")
        lower, upper = map(float, self.target_clip_pct)
        if not lower < upper:
            raise HorizonModelError("target clip must be increasing")
        if self.maximum_history_sessions < 1:
            raise HorizonModelError("maximum history must be positive")
        if _finite(self.cost_assumption_pct, "cost_assumption_pct") < 0:
            raise HorizonModelError("cost must not be negative")
        if self.algorithm not in {
            "RIDGE_RETURN_V2",
            "HUBER_RIDGE_RETURN_V2",
            "RECENCY_WEIGHTED_RIDGE_RETURN_V2",
        }:
            raise HorizonModelError("unsupported regression algorithm")
        if self.algorithm == "HUBER_RIDGE_RETURN_V2" and not (
            self.huber_delta is not None and self.huber_delta > 0
        ):
            raise HorizonModelError("Huber model requires a positive delta")
        if self.algorithm == "RECENCY_WEIGHTED_RIDGE_RETURN_V2" and not (
            self.recency_half_life_sessions is not None
            and self.recency_half_life_sessions > 0
        ):
            raise HorizonModelError("recency model requires a positive half-life")

    def feature_protocol(self) -> dict[str, Any]:
        return {
            "schema_version": FEATURE_PROTOCOL_SCHEMA,
            "model_key": self.model_key,
            "horizon_days": self.horizon_days,
            "feature_available_at": "SIGNAL_SESSION_CLOSE_15:00_ASIA_SHANGHAI",
            "decision_at": "SIGNAL_SESSION_15:05_ASIA_SHANGHAI",
            "cross_section_clock": "SAME_SIGNAL_SESSION_CLOSE_ONLY",
            "missing_policy": "TRAIN_MEDIAN_FROZEN_IN_MODEL",
            "history_sessions_required": self.maximum_history_sessions,
            "features": [
                {"name": item, "formula": FEATURE_FORMULAS[item]}
                for item in self.features
            ],
            "label_protocol": LABEL_PROTOCOL,
            "entry_clock": "NEXT_EXCHANGE_SESSION_OPEN",
            "exit_clock": "ENTRY_SESSION_PLUS_HORIZON_EXCHANGE_SESSION_CLOSE",
            "same_close_entry_allowed": False,
            "t0_exit_allowed": False,
            "cost_model_version": self.cost_model_version,
            "cost_assumption_pct": self.cost_assumption_pct,
            "corporate_action_policy": "QUARANTINE_RAW_PRICE_DISCONTINUITY",
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "horizon_days": self.horizon_days,
            "model_key": self.model_key,
            "model_version": self.model_version,
            "algorithm": self.algorithm,
            "features": list(self.features),
            "ridge_lambda": self.ridge_lambda,
            "target_clip_pct": list(self.target_clip_pct),
            "maximum_history_sessions": self.maximum_history_sessions,
            "cost_assumption_pct": self.cost_assumption_pct,
            "cost_model_version": self.cost_model_version,
            "huber_delta": self.huber_delta,
            "recency_half_life_sessions": self.recency_half_life_sessions,
        }


HORIZON_MODEL_SPECS: Mapping[int, HorizonModelSpec] = MappingProxyType(
    {
        1: HorizonModelSpec(
            horizon_days=1,
            model_key="trading_v3_t1_micro_reversion_ridge_v2",
            model_version="2.0.0-oos-shadow",
            algorithm="RIDGE_RETURN_V2",
            features=(
                "return_1d_pct",
                "overnight_gap_pct",
                "intraday_return_pct",
                "amount_ratio_1_20",
                "range_1d_pct",
                "close_location_value",
                "volatility_5d_pct",
                "relative_return_1d_pct",
            ),
            ridge_lambda=8.0,
            target_clip_pct=(-12.0, 12.0),
            maximum_history_sessions=20,
        ),
        5: HorizonModelSpec(
            horizon_days=5,
            model_key="trading_v3_t5_swing_huber_ridge_v2",
            model_version="2.0.0-oos-shadow",
            algorithm="HUBER_RIDGE_RETURN_V2",
            features=(
                "return_5d_pct",
                "return_20d_pct",
                "distance_ma5_pct",
                "distance_ma20_pct",
                "amount_ratio_5_20",
                "volatility_20d_pct",
                "drawdown_20d_pct",
                "relative_return_20d_pct",
            ),
            ridge_lambda=16.0,
            target_clip_pct=(-25.0, 30.0),
            maximum_history_sessions=20,
            huber_delta=1.5,
        ),
        20: HorizonModelSpec(
            horizon_days=20,
            model_key="trading_v3_t20_trend_recency_ridge_v2",
            model_version="2.0.0-oos-shadow",
            algorithm="RECENCY_WEIGHTED_RIDGE_RETURN_V2",
            features=(
                "return_20d_pct",
                "return_60d_pct",
                "ma20_slope_5d_pct",
                "ma60_slope_10d_pct",
                "volatility_20d_pct",
                "drawdown_60d_pct",
                "amount_ratio_20_60",
                "relative_return_20d_pct",
            ),
            ridge_lambda=32.0,
            target_clip_pct=(-40.0, 60.0),
            maximum_history_sessions=70,
            recency_half_life_sessions=252,
        ),
    }
)


@dataclass(frozen=True, slots=True)
class HorizonTrainingPolicy:
    training_window_protocol: str = TRAINING_WINDOW_PROTOCOL
    history_start: date | str = DEFAULT_HISTORY_START
    minimum_mature_samples: Mapping[int, int] | None = None
    minimum_oos_samples: Mapping[int, int] | None = None
    minimum_train_sessions: Mapping[int, int] | None = None
    minimum_oos_sessions: Mapping[int, int] | None = None
    walk_forward_fold_count: int = 3
    minimum_direction_rank_correlation: float = 0.05
    maximum_calibration_mae: float = 0.15
    maximum_brier_score: float = 0.24
    maximum_population_stability_index: float = 0.20
    minimum_net_expectancy_after_cost_pct: float = 0.0
    minimum_profit_factor: float = 1.30
    minimum_cost_coverage_ratio: float = 1.0
    minimum_maturity_coverage: float = 0.98
    calibration_bucket_count: int = 10

    def __post_init__(self) -> None:
        if self.training_window_protocol != TRAINING_WINDOW_PROTOCOL:
            raise HorizonModelError("training window protocol differs")
        object.__setattr__(
            self,
            "history_start",
            _date_value(self.history_start, "history_start"),
        )
        defaults = {
            "minimum_mature_samples": {1: 160, 5: 120, 20: 80},
            "minimum_oos_samples": {1: 100, 5: 100, 20: 80},
            "minimum_train_sessions": {1: 120, 5: 160, 20: 240},
            "minimum_oos_sessions": {1: 40, 5: 50, 20: 80},
        }
        for name, fallback in defaults.items():
            raw = getattr(self, name)
            normalized = dict(fallback if raw is None else raw)
            if set(normalized) != set(SUPPORTED_HORIZONS):
                raise HorizonModelError(f"{name} must define T+1/T+5/T+20")
            if any(type(value) is not int or value < 1 for value in normalized.values()):
                raise HorizonModelError(f"{name} values must be positive integers")
            object.__setattr__(self, name, MappingProxyType(normalized))
        if self.walk_forward_fold_count < 3:
            raise HorizonModelError("at least three walk-forward folds are required")
        if self.calibration_bucket_count < 2:
            raise HorizonModelError("at least two calibration buckets are required")
        for field in (
            "maximum_calibration_mae",
            "maximum_brier_score",
            "maximum_population_stability_index",
            "minimum_maturity_coverage",
        ):
            value = _finite(getattr(self, field), field)
            if not 0 <= value <= 1:
                raise HorizonModelError(f"{field} must be between zero and one")
        _finite(
            self.minimum_direction_rank_correlation,
            "minimum_direction_rank_correlation",
        )
        _finite(
            self.minimum_net_expectancy_after_cost_pct,
            "minimum_net_expectancy_after_cost_pct",
        )
        if self.minimum_profit_factor < 0 or self.minimum_cost_coverage_ratio < 0:
            raise HorizonModelError("profit/cost gates must not be negative")

    def as_dict(self) -> dict[str, Any]:
        return {
            "training_window_protocol": self.training_window_protocol,
            "history_start": self.history_start.isoformat(),
            "minimum_mature_samples": {
                str(key): value for key, value in self.minimum_mature_samples.items()
            },
            "minimum_oos_samples": {
                str(key): value for key, value in self.minimum_oos_samples.items()
            },
            "minimum_train_sessions": {
                str(key): value for key, value in self.minimum_train_sessions.items()
            },
            "minimum_oos_sessions": {
                str(key): value for key, value in self.minimum_oos_sessions.items()
            },
            "walk_forward_fold_count": self.walk_forward_fold_count,
            "minimum_direction_rank_correlation": (
                self.minimum_direction_rank_correlation
            ),
            "maximum_calibration_mae": self.maximum_calibration_mae,
            "maximum_brier_score": self.maximum_brier_score,
            "maximum_population_stability_index": (
                self.maximum_population_stability_index
            ),
            "minimum_net_expectancy_after_cost_pct": (
                self.minimum_net_expectancy_after_cost_pct
            ),
            "minimum_profit_factor": self.minimum_profit_factor,
            "minimum_cost_coverage_ratio": self.minimum_cost_coverage_ratio,
            "minimum_maturity_coverage": self.minimum_maturity_coverage,
            "calibration_bucket_count": self.calibration_bucket_count,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "HorizonTrainingPolicy":
        def horizon_map(name: str) -> dict[int, int]:
            raw = value.get(name) or {}
            return {int(key): int(item) for key, item in dict(raw).items()}

        return cls(
            training_window_protocol=str(value["training_window_protocol"]),
            history_start=_date_value(value["history_start"], "history_start"),
            minimum_mature_samples=horizon_map("minimum_mature_samples"),
            minimum_oos_samples=horizon_map("minimum_oos_samples"),
            minimum_train_sessions=horizon_map("minimum_train_sessions"),
            minimum_oos_sessions=horizon_map("minimum_oos_sessions"),
            walk_forward_fold_count=int(value["walk_forward_fold_count"]),
            minimum_direction_rank_correlation=float(
                value["minimum_direction_rank_correlation"]
            ),
            maximum_calibration_mae=float(value["maximum_calibration_mae"]),
            maximum_brier_score=float(value["maximum_brier_score"]),
            maximum_population_stability_index=float(
                value["maximum_population_stability_index"]
            ),
            minimum_net_expectancy_after_cost_pct=float(
                value["minimum_net_expectancy_after_cost_pct"]
            ),
            minimum_profit_factor=float(value["minimum_profit_factor"]),
            minimum_cost_coverage_ratio=float(value["minimum_cost_coverage_ratio"]),
            minimum_maturity_coverage=float(value["minimum_maturity_coverage"]),
            calibration_bucket_count=int(value["calibration_bucket_count"]),
        )


DEFAULT_TRAINING_POLICY = HorizonTrainingPolicy()


def current_training_window_contract() -> dict[str, str]:
    """Return the frozen signal-start contract from the current V3 config."""

    suite = dict(load_v3_config().get("multi_horizon_forecasts") or {})
    training = dict(suite.get("training_policy") or {})
    if training.get("protocol_version") != TRAINING_CONFIG_PROTOCOL:
        raise HorizonModelError("current training config protocol differs")
    if training.get("training_window_protocol") != TRAINING_WINDOW_PROTOCOL:
        raise HorizonModelError("current training window protocol differs")
    history_start = _date_value(
        training.get("history_start"),
        "current training history_start",
    )
    return {
        "training_config_protocol": TRAINING_CONFIG_PROTOCOL,
        "training_window_protocol": TRAINING_WINDOW_PROTOCOL,
        "history_start": history_start.isoformat(),
    }


@dataclass(frozen=True, slots=True)
class HorizonSelectionPolicy:
    """Frozen, label-blind economic evaluation policy.

    This V2 policy is deliberately a full-universe research diagnostic.  It is
    not represented as the source-strategy candidate population used by the
    runtime worker and therefore can never grant order authority by itself.
    """

    protocol: str = SELECTION_PROTOCOL
    candidate_domain: str = "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC"
    top_k_per_session: int = 12
    minimum_expected_return_net_pct: float = 0.0
    minimum_probability_positive: float = 0.5
    minimum_cross_section_size: int = 20
    rank_fields: tuple[str, ...] = (
        "expected_return_net_pct:DESC",
        "probability_positive:DESC",
        "normalized_score:DESC",
        "stock_code:ASC",
        "sample_id:ASC",
    )

    def __post_init__(self) -> None:
        if self.protocol != SELECTION_PROTOCOL:
            raise HorizonModelError("selection protocol is unsupported")
        if self.candidate_domain != "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC":
            raise HorizonModelError("selection candidate domain is unsupported")
        if self.top_k_per_session != 12:
            raise HorizonModelError("V2 selection top_k must remain frozen at 12")
        if self.minimum_cross_section_size < 2:
            raise HorizonModelError(
                "selection cross-section must contain at least two rows"
            )
        expected_threshold = _finite(
            self.minimum_expected_return_net_pct,
            "minimum_expected_return_net_pct",
        )
        probability = _finite(
            self.minimum_probability_positive,
            "minimum_probability_positive",
        )
        if expected_threshold != 0.0:
            raise HorizonModelError(
                "V2 selection expected-net threshold must remain frozen at zero"
            )
        if probability != 0.5:
            raise HorizonModelError(
                "V2 selection probability threshold must remain frozen at 0.5"
            )
        if self.rank_fields != (
            "expected_return_net_pct:DESC",
            "probability_positive:DESC",
            "normalized_score:DESC",
            "stock_code:ASC",
            "sample_id:ASC",
        ):
            raise HorizonModelError("selection tie-break policy differs")

    def as_dict(self) -> dict[str, Any]:
        return {
            "protocol": self.protocol,
            "candidate_domain": self.candidate_domain,
            "top_k_per_session": self.top_k_per_session,
            "minimum_expected_return_net_pct": (
                self.minimum_expected_return_net_pct
            ),
            "minimum_probability_positive": self.minimum_probability_positive,
            "minimum_cross_section_size": self.minimum_cross_section_size,
            "rank_fields": list(self.rank_fields),
            "label_blind_selection": True,
            "order_authority": False,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "HorizonSelectionPolicy":
        if value.get("label_blind_selection") is not True:
            raise HorizonModelError("selection must be label blind")
        if value.get("order_authority") is not False:
            raise HorizonModelError("selection escaped research-only boundary")
        return cls(
            protocol=str(value.get("protocol") or ""),
            candidate_domain=str(value.get("candidate_domain") or ""),
            top_k_per_session=int(value.get("top_k_per_session") or 0),
            minimum_expected_return_net_pct=float(
                value.get("minimum_expected_return_net_pct")
            ),
            minimum_probability_positive=float(
                value.get("minimum_probability_positive")
            ),
            minimum_cross_section_size=int(
                value.get("minimum_cross_section_size") or 0
            ),
            rank_fields=tuple(str(item) for item in value.get("rank_fields") or ()),
        )


DEFAULT_SELECTION_POLICY = HorizonSelectionPolicy()
DEFAULT_SELECTION_POLICY_HASH = canonical_hash(
    DEFAULT_SELECTION_POLICY.as_dict()
)


@dataclass(slots=True)
class HorizonDataset:
    horizon_days: int
    frame: pd.DataFrame
    manifest: dict[str, Any]

    @property
    def dataset_hash(self) -> str:
        return str(self.manifest["dataset_hash"])


@dataclass(frozen=True, slots=True)
class HorizonPrediction:
    model_key: str
    model_version: str
    horizon_days: int
    prediction_kind: str
    raw_expected_return_net_pct: float
    expected_return_net_pct: float
    probability_positive: float
    score: float
    model_artifact_hash: str
    feature_protocol_hash: str
    calibration_evidence_hash: str
    contract_eligible: bool
    lifecycle: str = "SHADOW_RESEARCH_ONLY"
    order_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_key": self.model_key,
            "model_version": self.model_version,
            "horizon_days": self.horizon_days,
            "prediction_kind": self.prediction_kind,
            "raw_expected_return_net_pct": self.raw_expected_return_net_pct,
            "expected_return_net_pct": self.expected_return_net_pct,
            "probability_positive": self.probability_positive,
            "score": self.score,
            "model_artifact_hash": self.model_artifact_hash,
            "feature_protocol_hash": self.feature_protocol_hash,
            "calibration_evidence_hash": self.calibration_evidence_hash,
            "contract_eligible": self.contract_eligible,
            "lifecycle": self.lifecycle,
            "order_authority": self.order_authority,
        }


def _normalize_trade_calendar(
    bars: pd.DataFrame,
    trade_calendar: Sequence[Any] | None,
) -> tuple[pd.Timestamp, ...]:
    raw = (
        list(trade_calendar)
        if trade_calendar is not None
        else bars["trade_date"].dropna().tolist()
    )
    dates = sorted({pd.Timestamp(item).normalize() for item in raw})
    if not dates:
        raise HorizonModelError("trade calendar must not be empty")
    return tuple(dates)


def _normalize_bars(
    bars: pd.DataFrame,
    trade_calendar: Sequence[Any] | None,
) -> tuple[pd.DataFrame, tuple[pd.Timestamp, ...]]:
    if not isinstance(bars, pd.DataFrame) or bars.empty:
        raise HorizonModelError("bars must be a non-empty DataFrame")
    required = {"stock_code", "trade_date", "open", "high", "low", "close", "amount"}
    missing = sorted(required - set(bars.columns))
    if missing:
        raise HorizonModelError("bars missing columns: " + ", ".join(missing))
    frame = bars.copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str[:6]
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
    for column in ("open", "high", "low", "close", "amount", "pre_close", "change_pct"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["stock_code", "trade_date", "open", "high", "low", "close"])
    if frame.empty:
        raise HorizonModelError("bars contain no valid rows")
    if frame.duplicated(["stock_code", "trade_date"]).any():
        raise HorizonModelError("bars must be unique by stock_code/trade_date")
    valid_price = frame[["open", "high", "low", "close"]].gt(0).all(axis=1)
    valid_ohlc = (
        frame["high"].ge(frame[["open", "close", "low"]].max(axis=1))
        & frame["low"].le(frame[["open", "close", "high"]].min(axis=1))
    )
    frame = frame.loc[valid_price & valid_ohlc].copy()
    frame["amount"] = frame["amount"].fillna(0.0).clip(lower=0.0)
    quality = frame.get(
        "quality_status",
        pd.Series("", index=frame.index, dtype="object"),
    ).fillna("").astype(str).str.upper()
    source = frame.get(
        "data_source",
        pd.Series("", index=frame.index, dtype="object"),
    ).fillna("").astype(str).str.strip()
    frame["_qmt_attested"] = quality.eq("QMT_ATTESTED") & source.ne("")
    frame = frame.sort_values(["stock_code", "trade_date"], kind="mergesort").reset_index(drop=True)
    calendar = _normalize_trade_calendar(frame, trade_calendar)
    calendar_index = {item: index for index, item in enumerate(calendar)}
    frame["_session_index"] = frame["trade_date"].map(calendar_index)
    if frame["_session_index"].isna().any():
        raise HorizonModelError("bar trade_date is outside the supplied calendar")
    frame["_session_index"] = frame["_session_index"].astype(int)
    groups = frame.groupby("stock_code", sort=False, observed=True)
    previous_raw_close = groups["close"].shift(1)
    if "pre_close" not in frame:
        frame["pre_close"] = previous_raw_close
    frame["pre_close"] = frame["pre_close"].where(frame["pre_close"] > 0, previous_raw_close)
    fallback_change = (frame["close"] / frame["pre_close"] - 1.0) * 100.0
    if "change_pct" not in frame:
        frame["change_pct"] = fallback_change
    else:
        frame["change_pct"] = frame["change_pct"].where(frame["change_pct"].notna(), fallback_change)
    raw_mismatch = (fallback_change - frame["change_pct"]).abs()
    previous_discontinuity = (frame["pre_close"] / previous_raw_close - 1.0).abs()
    frame["_price_discontinuity"] = (
        raw_mismatch.gt(2.0)
        | previous_discontinuity.gt(0.25)
        | frame["change_pct"].abs().gt(25.0)
    ).fillna(False)
    growth = (1.0 + frame["change_pct"].fillna(0.0) / 100.0).clip(lower=0.01)
    adjusted = growth.groupby(frame["stock_code"], observed=True).cumprod()
    first_close = groups["close"].transform("first")
    frame["_adjusted_close"] = adjusted * first_close
    frame["_raw_open"] = frame["open"]
    frame["_raw_high"] = frame["high"]
    frame["_raw_low"] = frame["low"]
    frame["_raw_close"] = frame["close"]
    return frame, calendar


def _add_point_in_time_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    groups = result.groupby("stock_code", sort=False, observed=True)
    adjusted = groups["_adjusted_close"]
    for horizon in (1, 5, 20, 60):
        result[f"return_{horizon}d_pct"] = adjusted.pct_change(
            horizon,
            fill_method=None,
        ) * 100.0
    result["overnight_gap_pct"] = (result["_raw_open"] / result["pre_close"] - 1.0) * 100.0
    result["intraday_return_pct"] = (result["_raw_close"] / result["_raw_open"] - 1.0) * 100.0
    result["range_1d_pct"] = (result["_raw_high"] - result["_raw_low"]) / result["pre_close"] * 100.0
    bar_range = result["_raw_high"] - result["_raw_low"]
    result["close_location_value"] = (
        (result["_raw_close"] - result["_raw_low"]) / bar_range.where(bar_range > 0)
    ).fillna(0.5)
    daily_return = result["return_1d_pct"]
    for window in (5, 20):
        result[f"volatility_{window}d_pct"] = daily_return.groupby(
            result["stock_code"], observed=True
        ).transform(lambda values, size=window: values.rolling(size).std(ddof=0))
    for window in (5, 20, 60):
        result[f"_ma{window}"] = groups["_adjusted_close"].transform(
            lambda values, size=window: values.rolling(size).mean()
        )
        result[f"_amount{window}"] = groups["amount"].transform(
            lambda values, size=window: values.rolling(size).mean()
        )
    result["amount_ratio_1_20"] = result["amount"] / result["_amount20"]
    result["amount_ratio_5_20"] = result["_amount5"] / result["_amount20"]
    result["amount_ratio_20_60"] = result["_amount20"] / result["_amount60"]
    result["distance_ma5_pct"] = (result["_adjusted_close"] / result["_ma5"] - 1.0) * 100.0
    result["distance_ma20_pct"] = (result["_adjusted_close"] / result["_ma20"] - 1.0) * 100.0
    for window in (20, 60):
        high = groups["_adjusted_close"].transform(
            lambda values, size=window: values.rolling(size).max()
        )
        result[f"drawdown_{window}d_pct"] = (result["_adjusted_close"] / high - 1.0) * 100.0
    result["ma20_slope_5d_pct"] = (
        result["_ma20"] / result.groupby("stock_code", observed=True)["_ma20"].shift(5) - 1.0
    ) * 100.0
    result["ma60_slope_10d_pct"] = (
        result["_ma60"] / result.groupby("stock_code", observed=True)["_ma60"].shift(10) - 1.0
    ) * 100.0
    market_1 = result.groupby("trade_date", observed=True)["return_1d_pct"].median()
    market_20 = result.groupby("trade_date", observed=True)["return_20d_pct"].median()
    result["relative_return_1d_pct"] = result["return_1d_pct"] - result["trade_date"].map(market_1)
    result["relative_return_20d_pct"] = result["return_20d_pct"] - result["trade_date"].map(market_20)
    return result


def _dataset_frame_hash(frame: pd.DataFrame, features: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    columns = (
        "sample_id",
        "stock_code",
        "decision_session_date",
        "entry_trade_date",
        "outcome_matures_on",
        "label_available",
        "label_qmt_attested",
        "label_quarantine_reason",
        "entry_amount",
        "maturity_amount",
        "gross_return_pct",
        "cost_assumption_pct",
        "net_return_pct",
        *features,
    )
    ordered = frame.sort_values("sample_id", kind="mergesort")
    for row in ordered.loc[:, list(columns)].itertuples(index=False, name=None):
        values: list[Any] = []
        for item in row:
            if item is None or pd.isna(item):
                values.append(None)
            elif isinstance(item, (np.bool_, bool)):
                values.append(bool(item))
            elif isinstance(item, (float, np.floating, int, np.integer)):
                values.append(_stable_float(item))
            elif isinstance(item, (date, datetime, pd.Timestamp)):
                values.append(pd.Timestamp(item).date().isoformat())
            else:
                values.append(str(item))
        digest.update(canonical_json(values).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _training_window_document(
    *,
    configured_history_start: date | str,
    signal_start: date | str,
    signal_end: date | str | None,
) -> dict[str, Any]:
    configured = _date_value(
        configured_history_start,
        "configured_history_start",
    )
    requested_start = _date_value(signal_start, "signal_start")
    requested_end = (
        _date_value(signal_end, "signal_end")
        if signal_end is not None else None
    )
    if requested_end is not None and requested_end < requested_start:
        raise HorizonModelError("signal_end must not precede signal_start")
    is_default = requested_start == configured
    body = {
        "protocol": TRAINING_WINDOW_PROTOCOL,
        "configured_history_start": configured.isoformat(),
        "signal_start": requested_start.isoformat(),
        "signal_start_inclusive": True,
        "signal_end": requested_end.isoformat() if requested_end else None,
        "signal_end_inclusive": True,
        "status": (
            "FROZEN_DEFAULT_TRAINING_WINDOW"
            if is_default else "NON_DEFAULT_TRAINING_WINDOW"
        ),
        "is_current_config_default": is_default,
    }
    return {**body, "training_window_hash": canonical_hash(body)}


def _dataset_identity_hash(
    *,
    frame_hash: str,
    spec: HorizonModelSpec,
    training_window: Mapping[str, Any],
) -> str:
    return canonical_hash({
        "schema_version": DATASET_SCHEMA,
        "model_key": spec.model_key,
        "horizon_days": spec.horizon_days,
        "label_protocol": LABEL_PROTOCOL,
        "frame_hash": _digest(frame_hash, "dataset frame_hash"),
        "training_window_hash": _digest(
            training_window.get("training_window_hash"),
            "training_window_hash",
        ),
    })


def build_horizon_dataset(
    bars: pd.DataFrame,
    horizon_days: int,
    *,
    trade_calendar: Sequence[Any] | None = None,
    signal_start: date | str | None = None,
    signal_end: date | str | None = None,
    minimum_universe_per_session: int = 1,
    universe_scope: str = "FULL_A_SHARE_POINT_IN_TIME",
) -> HorizonDataset:
    """Build a cost-inclusive label set using exact exchange-session offsets."""

    if horizon_days not in HORIZON_MODEL_SPECS:
        raise HorizonModelError("horizon_days must be one of 1, 5 or 20")
    if minimum_universe_per_session < 1:
        raise HorizonModelError("minimum_universe_per_session must be positive")
    if universe_scope not in {
        "FULL_A_SHARE_POINT_IN_TIME",
        "BOUNDED_SMOKE_RESEARCH_ONLY",
        "CALLER_SUPPLIED_RESEARCH_ONLY",
    }:
        raise HorizonModelError("universe_scope is unsupported")
    current_window = current_training_window_contract()
    requested_start = _date_value(
        signal_start if signal_start is not None else current_window["history_start"],
        "signal_start",
    )
    requested_end = (
        _date_value(signal_end, "signal_end")
        if signal_end is not None else None
    )
    training_window = _training_window_document(
        configured_history_start=current_window["history_start"],
        signal_start=requested_start,
        signal_end=requested_end,
    )
    spec = HORIZON_MODEL_SPECS[horizon_days]
    normalized, calendar = _normalize_bars(bars, trade_calendar)
    featured = _add_point_in_time_features(normalized)
    session_counts = featured.groupby("trade_date", observed=True)["stock_code"].nunique()
    valid_sessions = set(session_counts[session_counts >= minimum_universe_per_session].index)
    featured = featured[featured["trade_date"].isin(valid_sessions)].copy()
    groups = featured.groupby("stock_code", sort=False, observed=True)
    prior_index = groups["_session_index"].shift(spec.maximum_history_sessions)
    complete_history = (
        featured["_session_index"] - prior_index
    ).eq(spec.maximum_history_sessions)
    feature_values = featured.loc[:, list(spec.features)].replace([np.inf, -np.inf], np.nan)
    eligible = complete_history & feature_values.notna().all(axis=1)
    candidates = featured.loc[eligible].copy()
    candidates = candidates[
        candidates["trade_date"] >= pd.Timestamp(requested_start)
    ]
    if requested_end is not None:
        candidates = candidates[
            candidates["trade_date"] <= pd.Timestamp(requested_end)
        ]
    index_to_date = {index: item for index, item in enumerate(calendar)}
    candidates["_entry_index"] = candidates["_session_index"] + 1
    candidates["_maturity_index"] = candidates["_entry_index"] + horizon_days
    candidates["entry_trade_date"] = candidates["_entry_index"].map(index_to_date)
    candidates["outcome_matures_on"] = candidates["_maturity_index"].map(index_to_date)
    right_censored = int(candidates["outcome_matures_on"].isna().sum())
    candidates = candidates.dropna(subset=["entry_trade_date", "outcome_matures_on"]).copy()
    if candidates.empty:
        raise HorizonModelError("no feature-complete sample has a scheduled maturity")
    lookup = normalized.copy()
    anomaly_groups = lookup.groupby("stock_code", sort=False, observed=True)
    lookup["_anomaly_cumulative"] = anomaly_groups["_price_discontinuity"].cumsum()
    entry_lookup = lookup.loc[:, [
        "stock_code", "trade_date", "_raw_open", "amount", "_anomaly_cumulative",
        "_qmt_attested", "_price_discontinuity",
    ]].rename(columns={
        "trade_date": "entry_trade_date",
        "_raw_open": "entry_open",
        "amount": "entry_amount",
        "_anomaly_cumulative": "entry_anomaly_cumulative",
        "_qmt_attested": "entry_qmt_attested",
        "_price_discontinuity": "entry_price_discontinuity",
    })
    exit_lookup = lookup.loc[:, [
        "stock_code", "trade_date", "_raw_close", "amount", "_anomaly_cumulative",
        "_qmt_attested",
    ]].rename(columns={
        "trade_date": "outcome_matures_on",
        "_raw_close": "maturity_close",
        "amount": "maturity_amount",
        "_anomaly_cumulative": "maturity_anomaly_cumulative",
        "_qmt_attested": "maturity_qmt_attested",
    })
    candidates = candidates.merge(
        entry_lookup,
        on=["stock_code", "entry_trade_date"],
        how="left",
        validate="many_to_one",
    ).merge(
        exit_lookup,
        on=["stock_code", "outcome_matures_on"],
        how="left",
        validate="many_to_one",
    )
    prices_available = candidates["entry_open"].gt(0) & candidates["maturity_close"].gt(0)
    volume_available = (
        candidates["entry_amount"].gt(0)
        & candidates["maturity_amount"].gt(0)
    )
    anomaly_free = (
        candidates["maturity_anomaly_cumulative"]
        - candidates["entry_anomaly_cumulative"].fillna(0)
    ).fillna(1).eq(0) & ~candidates["entry_price_discontinuity"].fillna(True)
    candidates["label_available"] = (
        prices_available & volume_available & anomaly_free
    )
    candidates["label_qmt_attested"] = (
        candidates["label_available"]
        & candidates["entry_qmt_attested"].fillna(False).astype(bool)
        & candidates["maturity_qmt_attested"].fillna(False).astype(bool)
    )
    candidates["label_quarantine_reason"] = np.select(
        [
            ~prices_available,
            prices_available & ~volume_available,
            prices_available & volume_available & ~anomaly_free,
        ],
        [
            "EXACT_SESSION_BAR_UNAVAILABLE",
            "EXACT_SESSION_ZERO_VOLUME",
            "CORPORATE_ACTION_OR_PRICE_DISCONTINUITY",
        ],
        default="",
    )
    gross = (candidates["maturity_close"] / candidates["entry_open"] - 1.0) * 100.0
    candidates["gross_return_pct"] = gross.where(candidates["label_available"])
    candidates["cost_assumption_pct"] = spec.cost_assumption_pct
    candidates["net_return_pct"] = (
        candidates["gross_return_pct"] - spec.cost_assumption_pct
    ).where(candidates["label_available"])
    candidates["decision_session_date"] = candidates["trade_date"]
    candidates["signal_at"] = candidates["decision_session_date"].map(
        lambda item: _shanghai_timestamp(pd.Timestamp(item).date(), time(15, 5))
    )
    candidates["feature_available_at"] = candidates["decision_session_date"].map(
        lambda item: _shanghai_timestamp(pd.Timestamp(item).date(), time(15, 0))
    )
    candidates["label_mature_at"] = candidates["outcome_matures_on"].map(
        lambda item: _shanghai_timestamp(pd.Timestamp(item).date(), time(15, 1))
    )
    candidates["sample_id"] = [
        hashlib.sha256(
            f"{code}|{pd.Timestamp(day).date().isoformat()}|T+{horizon_days}|{LABEL_PROTOCOL}".encode("utf-8")
        ).hexdigest()
        for code, day in zip(candidates["stock_code"], candidates["decision_session_date"])
    ]
    keep = [
        "sample_id",
        "stock_code",
        "decision_session_date",
        "signal_at",
        "feature_available_at",
        "entry_trade_date",
        "outcome_matures_on",
        "label_mature_at",
        "label_available",
        "label_qmt_attested",
        "label_quarantine_reason",
        "entry_open",
        "maturity_close",
        "entry_amount",
        "maturity_amount",
        "gross_return_pct",
        "cost_assumption_pct",
        "net_return_pct",
        *spec.features,
    ]
    dataset_frame = candidates.loc[:, keep].sort_values(
        ["decision_session_date", "stock_code"], kind="mergesort"
    ).reset_index(drop=True)
    dataset_frame_hash = _dataset_frame_hash(dataset_frame, spec.features)
    dataset_hash = _dataset_identity_hash(
        frame_hash=dataset_frame_hash,
        spec=spec,
        training_window=training_window,
    )
    feature_protocol = spec.feature_protocol()
    attested_rows = normalized[normalized["_qmt_attested"]]
    attested_label_count = int(dataset_frame["label_qmt_attested"].sum())
    labeled_count = int(dataset_frame["label_available"].sum())
    manifest_body = {
        "schema_version": DATASET_SCHEMA,
        "model_key": spec.model_key,
        "horizon_days": horizon_days,
        "label_protocol": LABEL_PROTOCOL,
        "cost_model_version": spec.cost_model_version,
        "cost_assumption_pct": spec.cost_assumption_pct,
        "feature_protocol_hash": canonical_hash(feature_protocol),
        "dataset_frame_hash": dataset_frame_hash,
        "dataset_hash": dataset_hash,
        "training_window": training_window,
        "candidate_count": len(dataset_frame),
        "labeled_sample_count": labeled_count,
        "quarantined_sample_count": int((~dataset_frame["label_available"]).sum()),
        "right_censored_signal_count": right_censored,
        "distinct_decision_sessions": int(dataset_frame["decision_session_date"].nunique()),
        "first_decision_session": pd.Timestamp(dataset_frame["decision_session_date"].min()).date().isoformat(),
        "last_decision_session": pd.Timestamp(dataset_frame["decision_session_date"].max()).date().isoformat(),
        "last_maturity_session": pd.Timestamp(dataset_frame["outcome_matures_on"].max()).date().isoformat(),
        "trade_calendar_hash": canonical_hash(
            [item.date().isoformat() for item in calendar]
        ),
        "trade_calendar_session_count": len(calendar),
        "exact_exchange_session_offsets": True,
        "same_close_entry_allowed": False,
        "t0_exit_allowed": False,
        "outcomes_include_costs": True,
        "universe_scope": universe_scope,
        "execution_evidence_scope": "LONG_HISTORY_OOS_RESEARCH_ONLY",
        "label_attestation_required_for_execution": True,
        "executable_verified": False,
        "qmt_attested_start": (
            pd.Timestamp(attested_rows["trade_date"].min()).date().isoformat()
            if not attested_rows.empty else None
        ),
        "qmt_attested_end": (
            pd.Timestamp(attested_rows["trade_date"].max()).date().isoformat()
            if not attested_rows.empty else None
        ),
        "qmt_attested_row_count": len(attested_rows),
        "qmt_attested_label_count": attested_label_count,
        "qmt_attested_label_coverage": _stable_float(
            attested_label_count / labeled_count if labeled_count else 0.0
        ),
    }
    manifest = {
        **manifest_body,
        "manifest_hash": canonical_hash(manifest_body),
    }
    return HorizonDataset(horizon_days, dataset_frame, manifest)


def _weighted_median(
    values: np.ndarray,
    weights: np.ndarray,
) -> float:
    numeric = np.asarray(values, dtype=float)
    weight_values = np.asarray(weights, dtype=float)
    finite = np.isfinite(numeric) & np.isfinite(weight_values) & (weight_values > 0)
    if not finite.any():
        return 0.0
    numeric = numeric[finite]
    weight_values = weight_values[finite]
    order = np.argsort(numeric, kind="mergesort")
    numeric = numeric[order]
    weight_values = weight_values[order]
    cutoff = float(weight_values.sum()) / 2.0
    index = int(
        np.searchsorted(np.cumsum(weight_values), cutoff, side="left")
    )
    return float(numeric[min(index, len(numeric) - 1)])


def _training_matrix(
    frame: pd.DataFrame,
    features: tuple[str, ...],
    *,
    sample_weights: np.ndarray | None = None,
    medians: np.ndarray | None = None,
    means: np.ndarray | None = None,
    scales: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    values = frame.loc[:, list(features)].apply(
        pd.to_numeric, errors="coerce"
    ).to_numpy(dtype=float, copy=True)
    values[~np.isfinite(values)] = np.nan
    normalized_weights: np.ndarray | None = None
    if sample_weights is not None:
        normalized_weights = np.asarray(sample_weights, dtype=float)
        if (
            normalized_weights.shape != (len(frame),)
            or not np.isfinite(normalized_weights).all()
            or (normalized_weights <= 0).any()
        ):
            raise HorizonModelError(
                "feature-normalization weights must be positive and finite"
            )
        normalized_weights = normalized_weights / float(
            normalized_weights.sum()
        )
    if medians is None:
        if normalized_weights is None:
            medians = np.nanmedian(values, axis=0)
        else:
            weighted_medians: list[float] = []
            for column in range(values.shape[1]):
                weighted_medians.append(
                    _weighted_median(
                        values[:, column], normalized_weights
                    )
                )
            medians = np.asarray(weighted_medians, dtype=float)
        medians[~np.isfinite(medians)] = 0.0
    missing = np.isnan(values)
    if missing.any():
        values[missing] = np.take(medians, np.nonzero(missing)[1])
    if means is None:
        means = (
            values.mean(axis=0)
            if normalized_weights is None
            else np.sum(values * normalized_weights[:, None], axis=0)
        )
    if scales is None:
        scales = (
            values.std(axis=0)
            if normalized_weights is None
            else np.sqrt(
                np.sum(
                    np.square(values - means) * normalized_weights[:, None],
                    axis=0,
                )
            )
        )
        scales[~np.isfinite(scales) | (scales < 1e-8)] = 1.0
    design = (values - means) / scales
    if not np.isfinite(design).all():
        raise HorizonModelError("feature normalization produced non-finite values")
    return design, medians, means, scales


def _solve_weighted_ridge(
    design: np.ndarray,
    target: np.ndarray,
    ridge_lambda: float,
    weights: np.ndarray,
    *,
    effective_sample_size: float | None = None,
) -> tuple[float, np.ndarray]:
    if len(target) <= design.shape[1]:
        raise HorizonModelError("not enough samples to fit independent model")
    weights = np.asarray(weights, dtype=float)
    if not np.isfinite(weights).all() or (weights <= 0).any():
        raise HorizonModelError("training weights must be positive and finite")
    # Normalize the total weight so a uniform multiplication of every weight
    # cannot silently change the effective ridge penalty.
    normalization_mass = (
        float(len(weights))
        if effective_sample_size is None
        else _finite(effective_sample_size, "effective_sample_size")
    )
    if normalization_mass <= 0:
        raise HorizonModelError("effective_sample_size must be positive")
    weights = weights * (normalization_mass / float(weights.sum()))
    total = float(weights.sum())
    design_mean = np.sum(design * weights[:, None], axis=0) / total
    target_mean = float(np.sum(weights * target) / total)
    centered_design = design - design_mean
    centered_target = target - target_mean
    root = np.sqrt(weights)
    weighted_design = centered_design * root[:, None]
    weighted_target = centered_target * root
    gram = weighted_design.T @ weighted_design + np.eye(design.shape[1]) * ridge_lambda
    try:
        coefficients = np.linalg.solve(gram, weighted_design.T @ weighted_target)
    except np.linalg.LinAlgError as exc:
        raise HorizonModelError("ridge fit failed safely") from exc
    intercept = float(target_mean - design_mean @ coefficients)
    if not math.isfinite(intercept) or not np.isfinite(coefficients).all():
        raise HorizonModelError("model fit produced non-finite parameters")
    return intercept, coefficients


def _model_integrity_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    return _without_hash(value, "model_hash")


def _fit_model(frame: pd.DataFrame, spec: HorizonModelSpec) -> dict[str, Any]:
    ordered = frame.sort_values("sample_id", kind="mergesort").reset_index(drop=True)
    # Every decision session receives equal base mass.  Repeated copies of one
    # stock within a session also share that stock's mass, so neither a growing
    # cross-section nor accidental row duplication can dominate temporal OOS
    # evidence.
    session_stock = ordered.loc[:, [
        "decision_session_date", "stock_code"
    ]].astype(str)
    stock_multiplicity = session_stock.groupby(
        ["decision_session_date", "stock_code"],
        sort=False,
    )["stock_code"].transform("size").to_numpy(dtype=float)
    unique_stocks = session_stock.drop_duplicates().groupby(
        "decision_session_date", sort=False
    )["stock_code"].nunique()
    session_unique_count = session_stock["decision_session_date"].map(
        unique_stocks
    ).to_numpy(dtype=float)
    base_weights = 1.0 / (session_unique_count * stock_multiplicity)
    weights = base_weights.copy()
    if spec.algorithm == "RECENCY_WEIGHTED_RIDGE_RETURN_V2":
        sessions = pd.to_datetime(ordered["decision_session_date"]).rank(method="dense").to_numpy(dtype=float)
        age = sessions.max() - sessions
        weights *= np.power(
            0.5, age / float(spec.recency_half_life_sessions)
        )
    design, medians, means, scales = _training_matrix(
        ordered,
        spec.features,
        sample_weights=weights,
    )
    target = np.clip(
        pd.to_numeric(ordered["net_return_pct"], errors="coerce").to_numpy(dtype=float),
        spec.target_clip_pct[0],
        spec.target_clip_pct[1],
    )
    if not np.isfinite(target).all():
        raise HorizonModelError("training target must be finite")
    intercept, coefficients = _solve_weighted_ridge(
        design,
        target,
        spec.ridge_lambda,
        weights,
        effective_sample_size=float(
            ordered["decision_session_date"].nunique()
        ),
    )
    if spec.algorithm == "HUBER_RIDGE_RETURN_V2":
        for _ in range(8):
            residual = target - (intercept + design @ coefficients)
            residual_center = _weighted_median(residual, base_weights)
            scale = float(
                _weighted_median(
                    np.abs(residual - residual_center), base_weights
                )
                * 1.4826
            )
            scale = max(scale, 1e-8)
            threshold = float(spec.huber_delta) * scale
            absolute = np.abs(residual)
            robust_weights = np.ones_like(absolute)
            mask = absolute > threshold
            robust_weights[mask] = threshold / absolute[mask]
            weights = base_weights * robust_weights
            intercept, coefficients = _solve_weighted_ridge(
                design,
                target,
                spec.ridge_lambda,
                weights,
                effective_sample_size=float(
                    ordered["decision_session_date"].nunique()
                ),
            )
    training_manifest = canonical_hash(
        [
            {
                "sample_id": str(row.sample_id),
                "label_mature_at": str(row.label_mature_at),
                "net_return_pct": _stable_float(row.net_return_pct),
            }
            for row in ordered.loc[:, ["sample_id", "label_mature_at", "net_return_pct"]].itertuples(index=False)
        ]
    )
    fitted_training_scores = intercept + design @ coefficients
    score_weights = weights / float(weights.sum())
    training_score_mean = float(
        np.sum(fitted_training_scores * score_weights)
    )
    training_score_std = float(
        np.sqrt(
            np.sum(
                np.square(fitted_training_scores - training_score_mean)
                * score_weights
            )
        )
    )
    if not math.isfinite(training_score_std) or training_score_std < 1e-8:
        training_score_std = 1.0
    body = {
        "protocol": MODEL_PROTOCOL,
        "model_key": spec.model_key,
        "model_version": spec.model_version,
        "horizon_days": spec.horizon_days,
        "algorithm": spec.algorithm,
        "features": list(spec.features),
        "medians": [_stable_float(item) for item in medians],
        "means": [_stable_float(item) for item in means],
        "scales": [_stable_float(item) for item in scales],
        "coefficients": [_stable_float(item) for item in coefficients],
        "intercept": _stable_float(intercept),
        "score_normalization_protocol": SCORE_NORMALIZATION_PROTOCOL,
        "training_score_mean": _stable_float(training_score_mean),
        "training_score_std": _stable_float(training_score_std),
        "ridge_lambda": spec.ridge_lambda,
        "target_clip_pct": list(spec.target_clip_pct),
        "training_sample_count": len(ordered),
        "distinct_training_sessions": int(ordered["decision_session_date"].nunique()),
        "training_start": pd.Timestamp(ordered["decision_session_date"].min()).date().isoformat(),
        "training_end": pd.Timestamp(ordered["decision_session_date"].max()).date().isoformat(),
        "latest_label_maturity": pd.Timestamp(ordered["outcome_matures_on"].max()).date().isoformat(),
        "training_manifest_hash": training_manifest,
    }
    return {**body, "model_hash": canonical_hash(body)}


def _verify_model(model: Mapping[str, Any], spec: HorizonModelSpec) -> None:
    if canonical_hash(_model_integrity_payload(model)) != _digest(model.get("model_hash"), "model_hash"):
        raise HorizonModelError("model hash differs")
    if model.get("protocol") != MODEL_PROTOCOL:
        raise HorizonModelError("model protocol differs from frozen V2 protocol")
    if (
        model.get("model_key") != spec.model_key
        or model.get("model_version") != spec.model_version
        or int(model.get("horizon_days", 0)) != spec.horizon_days
        or model.get("algorithm") != spec.algorithm
    ):
        raise HorizonModelError("model identity differs from artifact")
    if tuple(model.get("features") or ()) != spec.features:
        raise HorizonModelError("model features differ from frozen protocol")
    size = len(spec.features)
    for field in ("medians", "means", "scales", "coefficients"):
        values = model.get(field) or []
        if len(values) != size:
            raise HorizonModelError(f"model {field} length differs")
        normalized = [_finite(item, f"model.{field}") for item in values]
        if field == "scales" and any(item <= 0 for item in normalized):
            raise HorizonModelError("model scales must be positive")
    _finite(model.get("intercept"), "model.intercept")
    if _finite(model.get("ridge_lambda"), "model.ridge_lambda") != float(
        spec.ridge_lambda
    ):
        raise HorizonModelError("model ridge penalty differs from frozen spec")
    if tuple(float(item) for item in model.get("target_clip_pct") or ()) != tuple(
        float(item) for item in spec.target_clip_pct
    ):
        raise HorizonModelError("model target clip differs from frozen spec")
    if model.get("score_normalization_protocol") != SCORE_NORMALIZATION_PROTOCOL:
        raise HorizonModelError("model score normalization protocol differs")
    _finite(model.get("training_score_mean"), "model.training_score_mean")
    if _finite(model.get("training_score_std"), "model.training_score_std") <= 0:
        raise HorizonModelError("model training_score_std must be positive")


def _predict_model(model: Mapping[str, Any], frame: pd.DataFrame) -> np.ndarray:
    features = tuple(str(item) for item in model["features"])
    design, _, _, _ = _training_matrix(
        frame,
        features,
        medians=np.asarray(model["medians"], dtype=float),
        means=np.asarray(model["means"], dtype=float),
        scales=np.asarray(model["scales"], dtype=float),
    )
    result = float(model["intercept"]) + design @ np.asarray(model["coefficients"], dtype=float)
    if not np.isfinite(result).all():
        raise HorizonModelError("prediction produced non-finite values")
    return result


def _normalized_model_score(
    model: Mapping[str, Any],
    frame: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    raw = _predict_model(model, frame)
    mean = float(model["training_score_mean"])
    scale = float(model["training_score_std"])
    if not math.isfinite(mean) or not math.isfinite(scale) or scale <= 0:
        raise HorizonModelError("model score normalization is invalid")
    normalized = (raw - mean) / scale
    if not np.isfinite(normalized).all():
        raise HorizonModelError("normalized model score is non-finite")
    return raw, normalized


def _pava(values: Sequence[float], weights: Sequence[int]) -> list[float]:
    blocks: list[dict[str, Any]] = []
    for index, (value, weight) in enumerate(zip(values, weights)):
        blocks.append({"start": index, "end": index, "value": float(value), "weight": float(weight)})
        while len(blocks) >= 2 and blocks[-2]["value"] > blocks[-1]["value"]:
            right = blocks.pop()
            left = blocks.pop()
            total = left["weight"] + right["weight"]
            blocks.append({
                "start": left["start"],
                "end": right["end"],
                "value": (left["value"] * left["weight"] + right["value"] * right["weight"]) / total,
                "weight": total,
            })
    result = [0.0] * len(values)
    for block in blocks:
        for index in range(block["start"], block["end"] + 1):
            result[index] = float(block["value"])
    return result


def _fit_calibration(
    predictions: pd.DataFrame,
    *,
    bucket_count: int,
) -> dict[str, Any]:
    ordered = predictions.sort_values(
        ["normalized_score", "sample_id"], kind="mergesort"
    ).reset_index(drop=True)
    if ordered.empty:
        raise HorizonModelError("calibration requires OOS predictions")
    if ordered["sample_id"].astype(str).duplicated().any():
        raise HorizonModelError("calibration sample identities must be unique")
    for field in ("normalized_score", "net_return_pct"):
        numeric = pd.to_numeric(ordered[field], errors="coerce").to_numpy(
            dtype=float
        )
        if not np.isfinite(numeric).all():
            raise HorizonModelError(
                f"calibration {field} must be finite"
            )
    # Never split identical scores across buckets.  Otherwise two adjacent
    # inclusive intervals can overlap and runtime would silently choose the
    # first bucket for a score that trained in both.
    unique_scores = ordered["normalized_score"].drop_duplicates().to_numpy(
        dtype=float
    )
    count = min(bucket_count, len(unique_scores))
    score_groups = [
        item
        for item in np.array_split(unique_scores, count)
        if len(item)
    ]
    raw_buckets: list[dict[str, Any]] = []
    for score_group in score_groups:
        group = ordered[
            ordered["normalized_score"].isin(score_group)
        ]
        raw_buckets.append({
            "lower_score": float(group["normalized_score"].min()),
            "upper_score": float(group["normalized_score"].max()),
            "sample_count": len(group),
            "expected_return_net_pct": float(group["net_return_pct"].mean()),
            "probability_positive": float((group["net_return_pct"] > 0).mean()),
        })
    weights = [item["sample_count"] for item in raw_buckets]
    expected = _pava([item["expected_return_net_pct"] for item in raw_buckets], weights)
    probability = _pava([item["probability_positive"] for item in raw_buckets], weights)
    buckets: list[dict[str, Any]] = []
    for raw, fitted_return, fitted_probability in zip(raw_buckets, expected, probability):
        buckets.append({
            "lower_score": _stable_float(raw["lower_score"]),
            "upper_score": _stable_float(raw["upper_score"]),
            "sample_count": raw["sample_count"],
            "expected_return_net_pct": _stable_float(fitted_return),
            "probability_positive": _stable_float(min(1.0, max(0.0, fitted_probability))),
        })
    body = {
        "protocol": CALIBRATION_PROTOCOL,
        "fitted_on": "MATURITY_PURGED_WALK_FORWARD_OOS_PREDICTIONS_ONLY",
        "extrapolation_policy": "CLIP_TO_EDGE_RESEARCH_ONLY",
        "sample_count": len(ordered),
        "distinct_oos_sessions": int(ordered["decision_session_date"].nunique()),
        "score_support": [buckets[0]["lower_score"], buckets[-1]["upper_score"]],
        "buckets": buckets,
    }
    return {**body, "calibration_hash": canonical_hash(body)}


def _apply_calibration(calibration: Mapping[str, Any], scores: Sequence[float]) -> tuple[np.ndarray, np.ndarray]:
    buckets = list(calibration.get("buckets") or ())
    if not buckets:
        raise HorizonModelError("calibration buckets are empty")
    expected: list[float] = []
    probability: list[float] = []
    for score in np.asarray(scores, dtype=float):
        selected = None
        for bucket in buckets:
            if float(bucket["lower_score"]) <= score <= float(bucket["upper_score"]):
                selected = bucket
                break
        if selected is None:
            selected = min(
                buckets,
                key=lambda item: min(
                    abs(score - float(item["lower_score"])),
                    abs(score - float(item["upper_score"])),
                ),
            )
        expected.append(float(selected["expected_return_net_pct"]))
        probability.append(float(selected["probability_positive"]))
    return np.asarray(expected), np.asarray(probability)


def _verify_calibration(calibration: Mapping[str, Any]) -> None:
    _verify_hashed_mapping(calibration, "calibration_hash")
    if (
        calibration.get("protocol") != CALIBRATION_PROTOCOL
        or calibration.get("fitted_on")
        != "MATURITY_PURGED_WALK_FORWARD_OOS_PREDICTIONS_ONLY"
        or calibration.get("extrapolation_policy")
        != "CLIP_TO_EDGE_RESEARCH_ONLY"
    ):
        raise HorizonModelError("calibration provenance differs")
    buckets = list(calibration.get("buckets") or ())
    if not buckets:
        raise HorizonModelError("calibration buckets are empty")
    sample_count = 0
    previous_upper: float | None = None
    previous_expected: float | None = None
    previous_probability: float | None = None
    for bucket in buckets:
        lower = _finite(bucket.get("lower_score"), "calibration.lower_score")
        upper = _finite(bucket.get("upper_score"), "calibration.upper_score")
        expected = _finite(
            bucket.get("expected_return_net_pct"),
            "calibration.expected_return_net_pct",
        )
        probability = _finite(
            bucket.get("probability_positive"),
            "calibration.probability_positive",
        )
        count = int(bucket.get("sample_count") or 0)
        if count < 1 or lower > upper:
            raise HorizonModelError("calibration bucket boundary differs")
        if previous_upper is not None and lower <= previous_upper:
            raise HorizonModelError("calibration score buckets overlap")
        if previous_expected is not None and expected < previous_expected:
            raise HorizonModelError("calibration expected return is not monotonic")
        if (
            not 0.0 <= probability <= 1.0
            or previous_probability is not None
            and probability < previous_probability
        ):
            raise HorizonModelError("calibration probability is invalid")
        sample_count += count
        previous_upper = upper
        previous_expected = expected
        previous_probability = probability
    if sample_count != int(calibration.get("sample_count") or 0):
        raise HorizonModelError("calibration sample count differs")
    if int(calibration.get("distinct_oos_sessions") or 0) < 1:
        raise HorizonModelError("calibration session count differs")
    support = list(calibration.get("score_support") or ())
    if len(support) != 2 or (
        _finite(support[0], "calibration.score_support")
        != _finite(buckets[0].get("lower_score"), "calibration.lower_score")
        or _finite(support[1], "calibration.score_support")
        != _finite(buckets[-1].get("upper_score"), "calibration.upper_score")
    ):
        raise HorizonModelError("calibration score support differs")


def _spearman(left: pd.Series, right: pd.Series) -> float:
    if len(left) < 2:
        return 0.0
    value = left.rank(method="average").corr(right.rank(method="average"))
    return 0.0 if value is None or not math.isfinite(float(value)) else float(value)


def _mean_session_spearman(
    frame: pd.DataFrame,
    score_field: str,
    *,
    minimum_size: int = 2,
) -> float:
    values: list[float] = []
    if frame.empty:
        return 0.0
    for _, group in frame.groupby(
        "decision_session_date", sort=True, observed=True
    ):
        if (
            len(group) >= minimum_size
            and group[score_field].nunique(dropna=True) >= 2
            and group["net_return_pct"].nunique(dropna=True) >= 2
        ):
            values.append(
                _spearman(group[score_field], group["net_return_pct"])
            )
    return float(np.mean(values)) if values else 0.0


def _profit_factor(values: Iterable[float]) -> float:
    rows = [float(item) for item in values]
    gains = sum(item for item in rows if item > 0)
    losses = -sum(item for item in rows if item < 0)
    if losses <= 0:
        return 999999.0 if gains > 0 else 0.0
    return gains / losses


def _calibration_metric_evidence(frame: pd.DataFrame) -> dict[str, Any]:
    sessions: list[dict[str, Any]] = []
    bins: list[dict[str, Any]] = []
    if frame.empty:
        body = {
            "protocol": "PREQUENTIAL_RUNTIME_CALIBRATION_METRICS_V2",
            "sample_count": 0,
            "distinct_session_count": 0,
            "brier_score": 1.0,
            "calibration_mae": 1.0,
            "sessions": sessions,
            "calibration_bins": bins,
        }
        return {
            **body,
            "calibration_metric_evidence_hash": canonical_hash(body),
        }
    for (fold_number, session), group in frame.groupby(
        ["fold_number", "decision_session_date"],
        sort=True,
        observed=True,
    ):
        probabilities = pd.to_numeric(
            group["probability_positive"], errors="coerce"
        ).to_numpy(dtype=float)
        outcomes = (
            pd.to_numeric(group["net_return_pct"], errors="coerce")
            .gt(0)
            .to_numpy(dtype=float)
        )
        if (
            not np.isfinite(probabilities).all()
            or (probabilities < 0).any()
            or (probabilities > 1).any()
        ):
            raise HorizonModelError(
                "prequential calibration probabilities are invalid"
            )
        sessions.append({
            "fold_number": int(fold_number),
            "decision_session_date": pd.Timestamp(session).date().isoformat(),
            "sample_count": len(group),
            "squared_error_sum": _stable_float(
                np.square(probabilities - outcomes).sum()
            ),
        })
    ordered = frame.sort_values(
        ["probability_positive", "sample_id"], kind="mergesort"
    ).reset_index(drop=True)
    bucket_count = min(10, len(ordered))
    for indexes in np.array_split(
        ordered.index.to_numpy(), bucket_count
    ):
        if not len(indexes):
            continue
        group = ordered.loc[indexes]
        bins.append({
            "sample_count": len(group),
            "mean_probability_positive": _stable_float(
                group["probability_positive"].mean()
            ),
            "realized_positive_rate": _stable_float(
                group["net_return_pct"].gt(0).mean()
            ),
        })
    total = len(ordered)
    brier = sum(
        float(item["squared_error_sum"]) for item in sessions
    ) / total
    calibration_mae = sum(
        int(item["sample_count"])
        / total
        * abs(
            float(item["mean_probability_positive"])
            - float(item["realized_positive_rate"])
        )
        for item in bins
    )
    body = {
        "protocol": "PREQUENTIAL_RUNTIME_CALIBRATION_METRICS_V2",
        "sample_count": total,
        "distinct_session_count": len(sessions),
        "brier_score": _stable_float(brier),
        "calibration_mae": _stable_float(calibration_mae),
        "sessions": sessions,
        "calibration_bins": bins,
    }
    return {
        **body,
        "calibration_metric_evidence_hash": canonical_hash(body),
    }


def _verify_calibration_metric_evidence(
    value: Mapping[str, Any],
) -> tuple[float, float]:
    _verify_hashed_mapping(value, "calibration_metric_evidence_hash")
    if value.get("protocol") != "PREQUENTIAL_RUNTIME_CALIBRATION_METRICS_V2":
        raise HorizonModelError("calibration metric protocol differs")
    sample_count = int(value.get("sample_count") or 0)
    sessions = list(value.get("sessions") or ())
    bins = list(value.get("calibration_bins") or ())
    if sample_count == 0:
        if sessions or bins:
            raise HorizonModelError("empty calibration metric evidence differs")
        brier = 1.0
        calibration_mae = 1.0
    else:
        seen: set[tuple[int, str]] = set()
        session_samples = 0
        squared_error_sum = 0.0
        for item in sessions:
            key = (
                int(item.get("fold_number") or 0),
                str(item.get("decision_session_date") or ""),
            )
            _date_value(key[1], "calibration_metric.decision_session_date")
            count = int(item.get("sample_count") or 0)
            squared = _finite(
                item.get("squared_error_sum"),
                "calibration_metric.squared_error_sum",
            )
            if key[0] < 1 or key in seen or count < 1 or not 0 <= squared <= count:
                raise HorizonModelError("calibration session metric differs")
            seen.add(key)
            session_samples += count
            squared_error_sum += squared
        if session_samples != sample_count or int(
            value.get("distinct_session_count") or 0
        ) != len(sessions):
            raise HorizonModelError("calibration session aggregate differs")
        bin_samples = 0
        calibration_mae = 0.0
        for item in bins:
            count = int(item.get("sample_count") or 0)
            probability = _finite(
                item.get("mean_probability_positive"),
                "calibration_metric.mean_probability_positive",
            )
            realized = _finite(
                item.get("realized_positive_rate"),
                "calibration_metric.realized_positive_rate",
            )
            if count < 1 or not 0 <= probability <= 1 or not 0 <= realized <= 1:
                raise HorizonModelError("calibration bin metric differs")
            bin_samples += count
            calibration_mae += count / sample_count * abs(
                probability - realized
            )
        if bin_samples != sample_count:
            raise HorizonModelError("calibration bin aggregate differs")
        brier = squared_error_sum / sample_count
    persisted_brier = _finite(value.get("brier_score"), "brier_score")
    persisted_mae = _finite(value.get("calibration_mae"), "calibration_mae")
    if (
        abs(brier - persisted_brier) > 1e-9
        or abs(calibration_mae - persisted_mae) > 1e-9
    ):
        raise HorizonModelError("calibration metric aggregate differs")
    return brier, calibration_mae


def _population_stability_index(
    reference: Sequence[float],
    observed: Sequence[float],
    *,
    bucket_count: int = 10,
) -> float:
    """Compute PSI from a training reference and chronological OOS scores."""

    base = np.asarray(reference, dtype=float)
    sample = np.asarray(observed, dtype=float)
    base = base[np.isfinite(base)]
    sample = sample[np.isfinite(sample)]
    if len(base) < 2 or len(sample) < 2:
        return 999.0
    quantiles = np.linspace(0.0, 1.0, min(bucket_count, len(base)) + 1)
    edges = np.unique(np.quantile(base, quantiles))
    if len(edges) < 2:
        return 999.0
    edges[0] = -np.inf
    edges[-1] = np.inf
    base_counts, _ = np.histogram(base, bins=edges)
    sample_counts, _ = np.histogram(sample, bins=edges)
    epsilon = 1e-6
    base_share = np.maximum(base_counts / len(base), epsilon)
    sample_share = np.maximum(sample_counts / len(sample), epsilon)
    value = float(np.sum((sample_share - base_share) * np.log(sample_share / base_share)))
    return value if math.isfinite(value) else 999.0


def _population_stability_evidence(
    reference: Sequence[float],
    observed: Sequence[float],
    *,
    reference_fold: int,
    anchor_model_hash: str | None,
    bucket_count: int = 10,
) -> dict[str, Any]:
    base = np.asarray(reference, dtype=float)
    sample = np.asarray(observed, dtype=float)
    base = base[np.isfinite(base)]
    sample = sample[np.isfinite(sample)]
    body: dict[str, Any] = {
        "protocol": PSI_PROTOCOL,
        "reference_fold": reference_fold,
        "score_source": "FROZEN_PRE_OOS_ANCHOR_MODEL",
        "anchor_model_hash": anchor_model_hash,
        "reference_sample_count": len(base),
        "observed_sample_count": len(sample),
        "uses_final_model_predictions": False,
        "uses_labels": False,
        "uses_one_frozen_model_for_all_folds": True,
    }
    if len(base) < 2 or len(sample) < 2:
        body.update({
            "status": "INSUFFICIENT_OOS_DISTRIBUTION",
            "interior_edges": [],
            "reference_counts": [],
            "observed_counts": [],
            "population_stability_index": 999.0,
        })
        return {**body, "psi_evidence_hash": canonical_hash(body)}
    quantiles = np.linspace(0.0, 1.0, min(bucket_count, len(base)) + 1)
    raw_edges = np.unique(np.quantile(base, quantiles))
    if len(raw_edges) < 2:
        body.update({
            "status": "DEGENERATE_REFERENCE_DISTRIBUTION",
            "interior_edges": [],
            "reference_counts": [len(base)],
            "observed_counts": [len(sample)],
            "population_stability_index": 999.0,
        })
        return {**body, "psi_evidence_hash": canonical_hash(body)}
    interior = raw_edges[1:-1]
    bins = np.concatenate(([-np.inf], interior, [np.inf]))
    reference_counts, _ = np.histogram(base, bins=bins)
    observed_counts, _ = np.histogram(sample, bins=bins)
    value = _population_stability_index(base, sample, bucket_count=bucket_count)
    body.update({
        "status": "CALCULATED",
        "interior_edges": [_stable_float(item) for item in interior],
        "reference_counts": [int(item) for item in reference_counts],
        "observed_counts": [int(item) for item in observed_counts],
        "population_stability_index": _stable_float(value),
    })
    return {**body, "psi_evidence_hash": canonical_hash(body)}


def _verify_population_stability_evidence(value: Mapping[str, Any]) -> float:
    _verify_hashed_mapping(value, "psi_evidence_hash")
    if (
        value.get("uses_final_model_predictions") is not False
        or value.get("uses_labels") is not False
        or value.get("uses_one_frozen_model_for_all_folds") is not True
        or value.get("score_source") != "FROZEN_PRE_OOS_ANCHOR_MODEL"
    ):
        raise HorizonModelError("PSI evidence uses a leaky source")
    if value.get("protocol") != PSI_PROTOCOL:
        raise HorizonModelError("PSI protocol differs")
    if value.get("status") == "CALCULATED":
        _digest(value.get("anchor_model_hash"), "PSI anchor_model_hash")
    if value.get("status") != "CALCULATED":
        return 999.0
    reference_counts = np.asarray(value.get("reference_counts") or (), dtype=float)
    observed_counts = np.asarray(value.get("observed_counts") or (), dtype=float)
    if (
        reference_counts.size == 0
        or reference_counts.shape != observed_counts.shape
        or (reference_counts < 0).any()
        or (observed_counts < 0).any()
        or int(reference_counts.sum()) != int(value.get("reference_sample_count") or 0)
        or int(observed_counts.sum()) != int(value.get("observed_sample_count") or 0)
    ):
        raise HorizonModelError("PSI distribution counts differ")
    epsilon = 1e-6
    reference_share = np.maximum(reference_counts / reference_counts.sum(), epsilon)
    observed_share = np.maximum(observed_counts / observed_counts.sum(), epsilon)
    result = float(np.sum(
        (observed_share - reference_share)
        * np.log(observed_share / reference_share)
    ))
    persisted = _finite(
        value.get("population_stability_index"),
        "population_stability_index",
    )
    if abs(result - persisted) > 1e-9:
        raise HorizonModelError("PSI value differs from distribution counts")
    return result


def _fold_hash_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    return _without_hash(value, "fold_hash")


def _fold_equal_mean(items: Sequence[Mapping[str, Any]], field: str) -> float:
    by_fold: dict[int, list[float]] = {}
    for item in items:
        by_fold.setdefault(int(item["fold_number"]), []).append(
            float(item[field])
        )
    fold_values = [float(np.mean(values)) for values in by_fold.values() if values]
    return float(np.mean(fold_values)) if fold_values else 0.0


def _session_direction_evidence(
    frame: pd.DataFrame,
    *,
    minimum_cross_section_size: int,
) -> dict[str, Any]:
    sessions: list[dict[str, Any]] = []
    if not frame.empty:
        for (fold_number, session), group in frame.groupby(
            ["fold_number", "decision_session_date"],
            sort=True,
            observed=True,
        ):
            valid = (
                len(group) >= minimum_cross_section_size
                and group["net_return_pct"].nunique(dropna=True) >= 2
                and group["expected_return_net_pct"].nunique(dropna=True) >= 2
                and group["probability_positive"].nunique(dropna=True) >= 2
            )
            sessions.append({
                "fold_number": int(fold_number),
                "decision_session_date": pd.Timestamp(session).date().isoformat(),
                "sample_count": len(group),
                "valid": bool(valid),
                "expected_return_rank_ic": _stable_float(
                    _spearman(
                        group["expected_return_net_pct"],
                        group["net_return_pct"],
                    ) if valid else 0.0
                ),
                "probability_rank_ic": _stable_float(
                    _spearman(
                        group["probability_positive"],
                        group["net_return_pct"],
                    ) if valid else 0.0
                ),
            })
    expected_ic = _fold_equal_mean(sessions, "expected_return_rank_ic")
    probability_ic = _fold_equal_mean(sessions, "probability_rank_ic")
    body = {
        "protocol": "PREQUENTIAL_RUNTIME_SESSION_FOLD_EQUAL_IC_V2",
        "minimum_cross_section_size": minimum_cross_section_size,
        "session_count": len(sessions),
        "valid_session_count": sum(bool(item["valid"]) for item in sessions),
        "expected_return_rank_ic": _stable_float(expected_ic),
        "probability_rank_ic": _stable_float(probability_ic),
        "gate_direction_rank_ic": _stable_float(min(expected_ic, probability_ic)),
        "sessions": sessions,
    }
    return {**body, "direction_evidence_hash": canonical_hash(body)}


def _verify_direction_evidence(value: Mapping[str, Any]) -> tuple[float, float, float]:
    _verify_hashed_mapping(value, "direction_evidence_hash")
    if value.get("protocol") != "PREQUENTIAL_RUNTIME_SESSION_FOLD_EQUAL_IC_V2":
        raise HorizonModelError("direction evidence protocol differs")
    sessions = list(value.get("sessions") or ())
    if int(value.get("session_count") or 0) != len(sessions):
        raise HorizonModelError("direction evidence session count differs")
    if int(value.get("valid_session_count") or 0) != sum(
        bool(item.get("valid")) for item in sessions
    ):
        raise HorizonModelError("direction evidence valid-session count differs")
    seen_sessions: set[tuple[int, str]] = set()
    minimum_size = int(value.get("minimum_cross_section_size") or 0)
    if minimum_size < 2:
        raise HorizonModelError("direction evidence cross-section differs")
    for item in sessions:
        fold_number = int(item.get("fold_number") or 0)
        session = str(item.get("decision_session_date") or "")
        _date_value(session, "direction.decision_session_date")
        key = (fold_number, session)
        if fold_number < 1 or key in seen_sessions:
            raise HorizonModelError("direction evidence session identity differs")
        seen_sessions.add(key)
        sample_count = int(item.get("sample_count") or 0)
        expected_ic = _finite(
            item.get("expected_return_rank_ic"),
            "direction.expected_return_rank_ic",
        )
        probability_ic = _finite(
            item.get("probability_rank_ic"),
            "direction.probability_rank_ic",
        )
        if not -1.0 <= expected_ic <= 1.0 or not -1.0 <= probability_ic <= 1.0:
            raise HorizonModelError("direction session IC is outside rank bounds")
        if bool(item.get("valid")):
            if sample_count < minimum_size:
                raise HorizonModelError("direction valid session is undersized")
        elif expected_ic != 0.0 or probability_ic != 0.0:
            raise HorizonModelError("direction invalid session contributes IC")
    expected = _fold_equal_mean(sessions, "expected_return_rank_ic")
    probability = _fold_equal_mean(sessions, "probability_rank_ic")
    gate = min(expected, probability)
    for actual, field in (
        (expected, "expected_return_rank_ic"),
        (probability, "probability_rank_ic"),
        (gate, "gate_direction_rank_ic"),
    ):
        if abs(actual - _finite(value.get(field), field)) > 1e-9:
            raise HorizonModelError("direction evidence aggregate differs")
    return gate, expected, probability


def _selection_policy_document(
    policy: HorizonSelectionPolicy,
) -> dict[str, Any]:
    body = policy.as_dict()
    return {**body, "selection_policy_hash": canonical_hash(body)}


def _selected_ledger_metrics(
    ledger: Sequence[Mapping[str, Any]],
    *,
    cost_assumption_pct: float,
) -> dict[str, Any]:
    net = [float(item["net_return_pct"]) for item in ledger]
    gross = [float(item["gross_return_pct"]) for item in ledger]
    gross_expectancy = float(np.mean(gross)) if gross else -999.0
    return {
        "selected_oos_sample_count": len(ledger),
        "selected_oos_session_count": len({
            str(item["decision_session_date"]) for item in ledger
        }),
        "net_expectancy_after_cost_pct": _stable_float(
            float(np.mean(net)) if net else -999.0
        ),
        "profit_factor": _stable_float(_profit_factor(net)),
        "cost_coverage_ratio": _stable_float(
            max(0.0, gross_expectancy) / cost_assumption_pct
            if cost_assumption_pct > 0 else 999999.0
        ),
    }


def _build_selection_evidence(
    evaluation: pd.DataFrame,
    *,
    policy: HorizonSelectionPolicy,
    cost_assumption_pct: float,
    candidate_ledger_reference: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    selected_rows: list[dict[str, Any]] = []
    selection_frontier: list[dict[str, Any]] = []
    eligible_count = 0
    candidate_count = len(evaluation)
    if not evaluation.empty:
        for (fold_number, session), group in evaluation.groupby(
            ["fold_number", "decision_session_date"],
            sort=True,
            observed=True,
        ):
            if len(group) < policy.minimum_cross_section_size:
                continue
            eligible = group[
                group["expected_return_net_pct"].gt(
                    policy.minimum_expected_return_net_pct
                )
                & group["probability_positive"].gt(
                    policy.minimum_probability_positive
                )
            ].copy()
            eligible_count += len(eligible)
            eligible = eligible.sort_values(
                [
                    "expected_return_net_pct",
                    "probability_positive",
                    "normalized_score",
                    "stock_code",
                    "sample_id",
                ],
                ascending=[False, False, False, True, True],
                kind="mergesort",
            ).head(policy.top_k_per_session)
            for rank, row in enumerate(eligible.itertuples(index=False), 1):
                frontier_row = {
                    "fold_number": int(fold_number),
                    "decision_session_date": pd.Timestamp(session).date().isoformat(),
                    "sample_id": str(row.sample_id),
                    "stock_code": str(row.stock_code),
                    "normalized_score": _stable_float(row.normalized_score),
                    "expected_return_net_pct": _stable_float(
                        row.expected_return_net_pct
                    ),
                    "probability_positive": _stable_float(
                        row.probability_positive
                    ),
                    "gross_return_pct": _stable_float(row.gross_return_pct),
                    "net_return_pct": _stable_float(row.net_return_pct),
                }
                selection_frontier.append(frontier_row)
                selected_rows.append({
                    **frontier_row,
                    "rank": rank,
                })
    return _selection_evidence_from_components(
        selected_rows=selected_rows,
        selection_frontier=selection_frontier,
        candidate_count=candidate_count,
        eligible_count=eligible_count,
        policy=policy,
        cost_assumption_pct=cost_assumption_pct,
        candidate_ledger_reference=candidate_ledger_reference,
    )


def _selection_evidence_from_components(
    *,
    selected_rows: Sequence[Mapping[str, Any]],
    selection_frontier: Sequence[Mapping[str, Any]],
    candidate_count: int,
    eligible_count: int,
    policy: HorizonSelectionPolicy,
    cost_assumption_pct: float,
    candidate_ledger_reference: Mapping[str, Any] | None,
) -> dict[str, Any]:
    selected = [dict(item) for item in selected_rows]
    frontier = [dict(item) for item in selection_frontier]
    policy_document = _selection_policy_document(policy)
    metrics = _selected_ledger_metrics(
        selected,
        cost_assumption_pct=cost_assumption_pct,
    )
    body = {
        "protocol": SELECTION_PROTOCOL,
        "economic_evaluation_scope": policy.candidate_domain,
        "deployment_candidate_domain_verified": False,
        "selection_policy_hash": policy_document["selection_policy_hash"],
        "candidate_sample_count": candidate_count,
        "eligible_candidate_count": eligible_count,
        **metrics,
        "selection_frontier_hash": canonical_hash(frontier),
        "selection_frontier": frontier,
        "selected_ledger_hash": canonical_hash(selected),
        "selected_ledger": selected,
        "order_authority": False,
    }
    if candidate_ledger_reference is not None:
        body.update({
            "candidate_ledger_schema": candidate_ledger_reference[
                "schema_version"
            ],
            "candidate_ledger_content_sha256": candidate_ledger_reference[
                "content_sha256"
            ],
            "candidate_ledger_canonical_records_sha256": (
                candidate_ledger_reference["canonical_records_sha256"]
            ),
            "candidate_ledger_reference_hash": candidate_ledger_reference[
                "reference_hash"
            ],
        })
    return {**body, "selection_evidence_hash": canonical_hash(body)}


def _verify_selection_evidence(
    value: Mapping[str, Any],
    *,
    policy: HorizonSelectionPolicy,
    cost_assumption_pct: float,
    candidate_ledger_reference: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _verify_hashed_mapping(value, "selection_evidence_hash")
    if (
        value.get("protocol") != SELECTION_PROTOCOL
        or value.get("economic_evaluation_scope") != policy.candidate_domain
        or value.get("deployment_candidate_domain_verified") is not False
        or value.get("order_authority") is not False
    ):
        raise HorizonModelError("selection evidence scope differs")
    policy_document = _selection_policy_document(policy)
    if value.get("selection_policy_hash") != policy_document["selection_policy_hash"]:
        raise HorizonModelError("selection evidence policy differs")
    if candidate_ledger_reference is not None and (
        value.get("candidate_ledger_schema")
        != candidate_ledger_reference.get("schema_version")
        or value.get("candidate_ledger_content_sha256")
        != candidate_ledger_reference.get("content_sha256")
        or value.get("candidate_ledger_canonical_records_sha256")
        != candidate_ledger_reference.get("canonical_records_sha256")
        or value.get("candidate_ledger_reference_hash")
        != candidate_ledger_reference.get("reference_hash")
    ):
        raise HorizonModelError("selection evidence candidate ledger differs")
    ledger = list(value.get("selected_ledger") or ())
    if canonical_hash(ledger) != _digest(
        value.get("selected_ledger_hash"), "selected_ledger_hash"
    ):
        raise HorizonModelError("selected ledger hash differs")
    frontier = list(value.get("selection_frontier") or ())
    if canonical_hash(frontier) != _digest(
        value.get("selection_frontier_hash"), "selection_frontier_hash"
    ):
        raise HorizonModelError("selection frontier hash differs")
    rebuilt_ledger: list[dict[str, Any]] = []
    frontier_by_session: dict[
        tuple[int, str], list[Mapping[str, Any]]
    ] = {}
    frontier_seen: set[str] = set()
    for item in frontier:
        sample_id = str(item.get("sample_id") or "")
        if not sample_id or sample_id in frontier_seen:
            raise HorizonModelError("selection frontier sample identity differs")
        frontier_seen.add(sample_id)
        if (
            _finite(
                item.get("expected_return_net_pct"),
                "frontier.expected_return_net_pct",
            ) <= policy.minimum_expected_return_net_pct
            or _finite(
                item.get("probability_positive"),
                "frontier.probability_positive",
            ) <= policy.minimum_probability_positive
        ):
            raise HorizonModelError("selection frontier violates frozen thresholds")
        probability = _finite(
            item.get("probability_positive"),
            "frontier.probability_positive",
        )
        gross = _finite(
            item.get("gross_return_pct"),
            "frontier.gross_return_pct",
        )
        net = _finite(
            item.get("net_return_pct"),
            "frontier.net_return_pct",
        )
        _finite(item.get("normalized_score"), "frontier.normalized_score")
        if not 0.0 <= probability <= 1.0:
            raise HorizonModelError("selection frontier probability is invalid")
        if abs((gross - net) - cost_assumption_pct) > 1e-9:
            raise HorizonModelError("selection frontier cost arithmetic differs")
        key = (int(item["fold_number"]), str(item["decision_session_date"]))
        frontier_by_session.setdefault(key, []).append(item)
    for key in sorted(frontier_by_session):
        rows = sorted(
            frontier_by_session[key],
            key=lambda item: (
                -float(item["expected_return_net_pct"]),
                -float(item["probability_positive"]),
                -float(item["normalized_score"]),
                str(item["stock_code"]),
                str(item["sample_id"]),
            ),
        )
        if len(rows) > policy.top_k_per_session:
            raise HorizonModelError("selection frontier exceeds frozen top_k")
        for rank, item in enumerate(rows, 1):
            rebuilt_ledger.append({**dict(item), "rank": rank})
    if canonical_hash(rebuilt_ledger) != canonical_hash(ledger):
        raise HorizonModelError(
            "selected ledger differs from frozen selection frontier"
        )
    if int(value.get("candidate_sample_count") or 0) < int(
        value.get("eligible_candidate_count") or 0
    ) or int(value.get("eligible_candidate_count") or 0) < len(frontier):
        raise HorizonModelError("selection candidate counts differ")
    seen: set[str] = set()
    by_session: dict[tuple[int, str], list[Mapping[str, Any]]] = {}
    for item in ledger:
        sample_id = str(item.get("sample_id") or "")
        if not sample_id or sample_id in seen:
            raise HorizonModelError("selected ledger sample identity differs")
        seen.add(sample_id)
        if (
            _finite(
                item.get("expected_return_net_pct"),
                "selected.expected_return_net_pct",
            ) <= policy.minimum_expected_return_net_pct
            or _finite(
                item.get("probability_positive"),
                "selected.probability_positive",
            ) <= policy.minimum_probability_positive
        ):
            raise HorizonModelError("selected ledger violates frozen thresholds")
        key = (int(item["fold_number"]), str(item["decision_session_date"]))
        by_session.setdefault(key, []).append(item)
    for rows in by_session.values():
        if len(rows) > policy.top_k_per_session:
            raise HorizonModelError("selected ledger exceeds frozen top_k")
        ordered = sorted(
            rows,
            key=lambda item: (
                -float(item["expected_return_net_pct"]),
                -float(item["probability_positive"]),
                -float(item["normalized_score"]),
                str(item["stock_code"]),
                str(item["sample_id"]),
            ),
        )
        if [int(item["rank"]) for item in ordered] != list(
            range(1, len(ordered) + 1)
        ):
            raise HorizonModelError("selected ledger rank differs")
    metrics = _selected_ledger_metrics(
        ledger,
        cost_assumption_pct=cost_assumption_pct,
    )
    for field, actual in metrics.items():
        persisted = value.get(field)
        if isinstance(actual, int):
            if int(persisted or 0) != actual:
                raise HorizonModelError("selected ledger count aggregate differs")
        elif abs(float(persisted) - float(actual)) > 1e-9:
            raise HorizonModelError("selected ledger metric aggregate differs")
    return metrics


_CANDIDATE_LEDGER_ROW_KEYS = frozenset({
    "record_type",
    "fold_number",
    "sample_id",
    "stock_code",
    "decision_session_date",
    "outcome_matures_on",
    "gross_return_pct",
    "net_return_pct",
    "raw_expected_return_net_pct",
    "normalized_score",
    "anchor_score",
    "calibration_available",
    "expected_return_net_pct",
    "probability_positive",
})


def _candidate_ledger_relative_path(content_sha256: str) -> str:
    digest = _digest(content_sha256, "candidate ledger content_sha256")
    return (
        f"candidate-ledgers/sha256/{digest[:2]}/{digest}.jsonl.gz"
    )


def _candidate_ledger_header(
    *,
    suite_release_id: str,
    release_id: str,
    spec: HorizonModelSpec,
    dataset_hash: str,
    training_window: Mapping[str, Any],
    selection_policy_hash: str,
    calibration_protocol_hash: str,
) -> dict[str, Any]:
    return {
        "record_type": "HEADER",
        "schema_version": CANDIDATE_EVALUATION_LEDGER_SCHEMA,
        "binding_protocol": CANDIDATE_LEDGER_BINDING_PROTOCOL,
        "suite_release_id": suite_release_id,
        "release_id": release_id,
        "model_key": spec.model_key,
        "model_version": spec.model_version,
        "model_protocol": MODEL_PROTOCOL,
        "horizon_days": spec.horizon_days,
        "dataset_hash": dataset_hash,
        "training_window_hash": _digest(
            training_window.get("training_window_hash"),
            "training_window_hash",
        ),
        "signal_start": str(training_window.get("signal_start") or ""),
        "selection_policy_hash": selection_policy_hash,
        "calibration_protocol_hash": calibration_protocol_hash,
        "label_protocol": LABEL_PROTOCOL,
        "candidate_domain": DEFAULT_SELECTION_POLICY.candidate_domain,
        "cost_assumption_pct": spec.cost_assumption_pct,
    }


def _candidate_ledger_row(row: Any) -> dict[str, Any]:
    calibration_available = bool(row.calibration_available)
    return {
        "record_type": "CANDIDATE",
        "fold_number": int(row.fold_number),
        "sample_id": str(row.sample_id),
        "stock_code": str(row.stock_code),
        "decision_session_date": pd.Timestamp(
            row.decision_session_date
        ).date().isoformat(),
        "outcome_matures_on": pd.Timestamp(
            row.outcome_matures_on
        ).date().isoformat(),
        "gross_return_pct": _stable_float(row.gross_return_pct),
        "net_return_pct": _stable_float(row.net_return_pct),
        "raw_expected_return_net_pct": _stable_float(
            row.raw_expected_return_net_pct
        ),
        "normalized_score": _stable_float(row.normalized_score),
        "anchor_score": _stable_float(row.anchor_score),
        "calibration_available": calibration_available,
        "expected_return_net_pct": (
            _stable_float(row.expected_return_net_pct)
            if calibration_available else None
        ),
        "probability_positive": (
            _stable_float(row.probability_positive)
            if calibration_available else None
        ),
    }


def _stream_candidate_ledger_bytes(
    output: Any,
    *,
    header: Mapping[str, Any],
    ordered_oos: pd.DataFrame,
) -> str:
    canonical_digest = hashlib.sha256()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        fileobj=output,
        compresslevel=6,
        mtime=0,
    ) as compressed:
        for record in (dict(header),):
            line = (canonical_json(record) + "\n").encode("utf-8")
            canonical_digest.update(line)
            compressed.write(line)
        for row in ordered_oos.itertuples(index=False):
            record = _candidate_ledger_row(row)
            line = (canonical_json(record) + "\n").encode("utf-8")
            canonical_digest.update(line)
            compressed.write(line)
    return canonical_digest.hexdigest()


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                size += len(block)
                digest.update(block)
    except OSError as exc:
        raise HorizonModelError(
            f"cannot read candidate evaluation ledger: {path}"
        ) from exc
    return digest.hexdigest(), size


class _HashingWriter:
    def __init__(self) -> None:
        self.digest = hashlib.sha256()
        self.size = 0

    def write(self, value: bytes) -> int:
        self.digest.update(value)
        self.size += len(value)
        return len(value)

    def flush(self) -> None:
        return None


def _build_candidate_ledger_reference(
    oos: pd.DataFrame,
    *,
    header: Mapping[str, Any],
    ledger_root: str | Path | None,
) -> dict[str, Any]:
    if oos.empty:
        ordered = pd.DataFrame(columns=[
            "fold_number",
            "decision_session_date",
            "sample_id",
            "stock_code",
            "outcome_matures_on",
            "gross_return_pct",
            "net_return_pct",
            "raw_expected_return_net_pct",
            "normalized_score",
            "anchor_score",
            "calibration_available",
            "expected_return_net_pct",
            "probability_positive",
        ])
    else:
        oos.sort_values(
            ["fold_number", "decision_session_date", "sample_id"],
            kind="mergesort",
            inplace=True,
            ignore_index=True,
        )
        ordered = oos
    row_count = len(ordered)
    session_count = int(
        ordered[["fold_number", "decision_session_date"]]
        .drop_duplicates()
        .shape[0]
    ) if row_count else 0
    evaluation = ordered[
        ordered.get(
            "calibration_available",
            pd.Series(False, index=ordered.index),
        ).astype(bool)
    ]
    evaluation_row_count = len(evaluation)
    evaluation_session_count = int(
        evaluation[["fold_number", "decision_session_date"]]
        .drop_duplicates()
        .shape[0]
    ) if evaluation_row_count else 0
    fold_count = int(ordered["fold_number"].nunique()) if row_count else 0

    canonical_records_sha256: str
    content_sha256: str
    compressed_size_bytes: int
    if ledger_root is None:
        buffer = io.BytesIO()
        canonical_records_sha256 = _stream_candidate_ledger_bytes(
            buffer,
            header=header,
            ordered_oos=ordered,
        )
        payload = buffer.getvalue()
        if len(payload) > _MAX_EPHEMERAL_CANDIDATE_LEDGER_BYTES:
            raise HorizonModelError(
                "candidate_ledger_root is required for a large training ledger"
            )
        content_sha256 = hashlib.sha256(payload).hexdigest()
        compressed_size_bytes = len(payload)
        _EPHEMERAL_CANDIDATE_LEDGER_BYTES[content_sha256] = payload
    else:
        root = Path(ledger_root)
        pending_root = root / "candidate-ledgers" / ".pending"
        pending_root.mkdir(parents=True, exist_ok=True)
        pending_path = pending_root / f"{uuid.uuid4().hex}.jsonl.gz"
        try:
            with pending_path.open("xb") as output:
                canonical_records_sha256 = _stream_candidate_ledger_bytes(
                    output,
                    header=header,
                    ordered_oos=ordered,
                )
                output.flush()
                os.fsync(output.fileno())
            content_sha256, compressed_size_bytes = _sha256_file(
                pending_path
            )
            relative_path = _candidate_ledger_relative_path(content_sha256)
            destination = root.joinpath(*PurePosixPath(relative_path).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                existing_hash, existing_size = _sha256_file(destination)
                if (
                    existing_hash != content_sha256
                    or existing_size != compressed_size_bytes
                ):
                    raise HorizonModelError(
                        "content-addressed candidate ledger collision"
                    )
                pending_path.unlink()
            else:
                os.replace(pending_path, destination)
        finally:
            if pending_path.exists():
                pending_path.unlink()

    body = {
        "schema_version": CANDIDATE_EVALUATION_LEDGER_SCHEMA,
        "binding_protocol": CANDIDATE_LEDGER_BINDING_PROTOCOL,
        "encoding": CANDIDATE_LEDGER_ENCODING,
        "hash_algorithm": "SHA256_COMPRESSED_BYTES",
        "content_sha256": content_sha256,
        "canonical_records_sha256": canonical_records_sha256,
        "relative_path": _candidate_ledger_relative_path(content_sha256),
        "compressed_size_bytes": compressed_size_bytes,
        "row_count": row_count,
        "session_count": session_count,
        "evaluation_row_count": evaluation_row_count,
        "evaluation_session_count": evaluation_session_count,
        "fold_count": fold_count,
        "header_hash": canonical_hash(dict(header)),
        "registration_verification_required": True,
    }
    return {**body, "reference_hash": canonical_hash(body)}


def _verify_candidate_ledger_reference(
    value: Mapping[str, Any],
    *,
    document: Mapping[str, Any],
    spec: HorizonModelSpec,
    selection_policy: HorizonSelectionPolicy,
) -> dict[str, Any]:
    _verify_hashed_mapping(value, "reference_hash")
    content_sha256 = _digest(
        value.get("content_sha256"),
        "candidate ledger content_sha256",
    )
    _digest(
        value.get("canonical_records_sha256"),
        "candidate ledger canonical_records_sha256",
    )
    if (
        value.get("schema_version") != CANDIDATE_EVALUATION_LEDGER_SCHEMA
        or value.get("binding_protocol") != CANDIDATE_LEDGER_BINDING_PROTOCOL
        or value.get("encoding") != CANDIDATE_LEDGER_ENCODING
        or value.get("hash_algorithm") != "SHA256_COMPRESSED_BYTES"
        or value.get("relative_path")
        != _candidate_ledger_relative_path(content_sha256)
        or value.get("registration_verification_required") is not True
    ):
        raise HorizonModelError("candidate ledger reference protocol differs")
    for field in (
        "compressed_size_bytes",
        "row_count",
        "session_count",
        "evaluation_row_count",
        "evaluation_session_count",
        "fold_count",
    ):
        raw = value.get(field)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise HorizonModelError(f"candidate ledger {field} differs")
    selection_policy_hash = _selection_policy_document(selection_policy)[
        "selection_policy_hash"
    ]
    expected_header = _candidate_ledger_header(
        suite_release_id=str(document.get("suite_release_id") or ""),
        release_id=str(document.get("release_id") or ""),
        spec=spec,
        dataset_hash=str(document.get("dataset_hash") or ""),
        training_window=dict(document.get("training_window") or {}),
        selection_policy_hash=selection_policy_hash,
        calibration_protocol_hash=str(
            document.get("calibration_protocol_hash") or ""
        ),
    )
    if value.get("header_hash") != canonical_hash(expected_header):
        raise HorizonModelError("candidate ledger header binding differs")
    return expected_header


def _resolve_candidate_ledger_path(
    artifact_root: str | Path,
    reference: Mapping[str, Any],
) -> Path:
    root = Path(artifact_root).resolve()
    relative = str(reference.get("relative_path") or "")
    pure = PurePosixPath(relative)
    expected = _candidate_ledger_relative_path(
        str(reference.get("content_sha256") or "")
    )
    if (
        relative != expected
        or pure.is_absolute()
        or ".." in pure.parts
        or "\\" in relative
    ):
        raise HorizonModelError("candidate ledger relative path differs")
    candidate = root.joinpath(*pure.parts)
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise HorizonModelError("candidate evaluation ledger is missing") from exc
    if not resolved.is_file() or not resolved.is_relative_to(root):
        raise HorizonModelError("candidate ledger escaped approved artifact root")
    return resolved


def _canonical_ledger_record(line: bytes, line_number: int) -> dict[str, Any]:
    if not line.endswith(b"\n"):
        raise HorizonModelError("candidate ledger record lacks canonical LF")
    try:
        decoded = line.decode("utf-8")
        value = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HorizonModelError(
            f"candidate ledger record {line_number} is invalid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise HorizonModelError("candidate ledger record must be an object")
    if (canonical_json(value) + "\n").encode("utf-8") != line:
        raise HorizonModelError("candidate ledger record is not canonical JSONL")
    return value


def _fold_oos_prediction_record(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "sample_id": str(row["sample_id"]),
        "decision_session_date": pd.Timestamp(
            row["decision_session_date"]
        ).date().isoformat(),
        "outcome_matures_on": pd.Timestamp(
            row["outcome_matures_on"]
        ).date().isoformat(),
        "raw_expected_return_net_pct": _stable_float(
            row["raw_expected_return_net_pct"]
        ),
        "normalized_score": _stable_float(row["normalized_score"]),
        "anchor_score": _stable_float(row["anchor_score"]),
        "net_return_pct": _stable_float(row["net_return_pct"]),
    }


def _finalize_stream_session(
    rows: Sequence[Mapping[str, Any]],
    *,
    fold: Mapping[str, Any],
    policy: HorizonSelectionPolicy,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, bool]:
    if not rows:
        return [], [], 0, False
    calibration = fold.get("prequential_calibration")
    calibration_available = bool(rows[0]["calibration_available"])
    if any(
        bool(item["calibration_available"]) != calibration_available
        for item in rows
    ):
        raise HorizonModelError("candidate ledger session calibration clock differs")
    if calibration_available != (calibration is not None):
        raise HorizonModelError("candidate ledger fold calibration clock differs")
    if not calibration_available:
        return [], [], 0, False
    scores = np.asarray(
        [float(item["normalized_score"]) for item in rows],
        dtype=float,
    )
    expected, probability = _apply_calibration(calibration, scores)
    for index, item in enumerate(rows):
        if (
            abs(float(item["expected_return_net_pct"]) - expected[index])
            > 1e-9
            or abs(float(item["probability_positive"]) - probability[index])
            > 1e-9
        ):
            raise HorizonModelError(
                "candidate ledger prequential calibration prediction differs"
            )
    if len(rows) < policy.minimum_cross_section_size:
        return [], [], 0, True
    eligible = [
        item for item in rows
        if float(item["expected_return_net_pct"])
        > policy.minimum_expected_return_net_pct
        and float(item["probability_positive"])
        > policy.minimum_probability_positive
    ]
    eligible_count = len(eligible)
    selected = sorted(
        eligible,
        key=lambda item: (
            -float(item["expected_return_net_pct"]),
            -float(item["probability_positive"]),
            -float(item["normalized_score"]),
            str(item["stock_code"]),
            str(item["sample_id"]),
        ),
    )[:policy.top_k_per_session]
    frontier = [
        {
            "fold_number": int(item["fold_number"]),
            "decision_session_date": str(item["decision_session_date"]),
            "sample_id": str(item["sample_id"]),
            "stock_code": str(item["stock_code"]),
            "normalized_score": _stable_float(item["normalized_score"]),
            "expected_return_net_pct": _stable_float(
                item["expected_return_net_pct"]
            ),
            "probability_positive": _stable_float(
                item["probability_positive"]
            ),
            "gross_return_pct": _stable_float(item["gross_return_pct"]),
            "net_return_pct": _stable_float(item["net_return_pct"]),
        }
        for item in selected
    ]
    ledger = [
        {**item, "rank": rank}
        for rank, item in enumerate(frontier, 1)
    ]
    return frontier, ledger, eligible_count, True


def _stream_verify_candidate_ledger(
    document: Mapping[str, Any],
    path: Path,
    *,
    expected_header: Mapping[str, Any],
    policy: HorizonSelectionPolicy,
    spec: HorizonModelSpec,
) -> dict[str, Any]:
    reference = dict(document["candidate_evaluation_ledger"])
    dataset_manifest = dict(document.get("dataset_manifest") or {})
    manifest_first_decision = _date_value(
        dataset_manifest.get("first_decision_session"),
        "candidate ledger dataset first_decision_session",
    )
    manifest_last_decision = _date_value(
        dataset_manifest.get("last_decision_session"),
        "candidate ledger dataset last_decision_session",
    )
    manifest_last_maturity = _date_value(
        dataset_manifest.get("last_maturity_session"),
        "candidate ledger dataset last_maturity_session",
    )
    training_cutoff = _date_value(
        document.get("training_cutoff"),
        "candidate ledger training_cutoff",
    )
    actual_content_hash, actual_size = _sha256_file(path)
    if (
        actual_content_hash != reference["content_sha256"]
        or actual_size != int(reference["compressed_size_bytes"])
    ):
        raise HorizonModelError("candidate ledger compressed content hash differs")

    folds = {
        int(item["fold_number"]): item
        for item in document["walk_forward"]["folds"]
    }
    canonical_digest = hashlib.sha256()
    selected_rows: list[dict[str, Any]] = []
    selection_frontier: list[dict[str, Any]] = []
    eligible_count = 0
    row_count = 0
    session_count = 0
    evaluation_row_count = 0
    evaluation_session_count = 0
    evaluation_session_counts: dict[tuple[int, str], int] = {}
    fold_counts: dict[int, int] = {item: 0 for item in folds}
    fold_session_counts: dict[int, int] = {item: 0 for item in folds}
    fold_hashers: dict[int, Any] = {}
    fold_hash_first: dict[int, bool] = {}
    previous_key: tuple[int, str, str] | None = None
    previous_session: tuple[int, str] | None = None
    previous_session_date: str | None = None
    session_rows: list[dict[str, Any]] = []

    def finalize_session() -> None:
        nonlocal eligible_count, session_count
        nonlocal evaluation_session_count
        if not session_rows:
            return
        fold_number = int(session_rows[0]["fold_number"])
        frontier, ledger, eligible, evaluated = _finalize_stream_session(
            session_rows,
            fold=folds[fold_number],
            policy=policy,
        )
        selection_frontier.extend(frontier)
        selected_rows.extend(ledger)
        eligible_count += eligible
        session_count += 1
        fold_session_counts[fold_number] += 1
        if evaluated:
            evaluation_session_count += 1
            evaluation_session_counts[
                (
                    int(session_rows[0]["fold_number"]),
                    str(session_rows[0]["decision_session_date"]),
                )
            ] = len(session_rows)

    deterministic_gzip = _HashingWriter()
    try:
        with gzip.open(path, "rb") as stream, gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=deterministic_gzip,
            compresslevel=6,
            mtime=0,
        ) as recompressed:
            header_line = stream.readline(
                _MAX_CANDIDATE_LEDGER_RECORD_BYTES + 1
            )
            if not header_line:
                raise HorizonModelError("candidate ledger is empty")
            if len(header_line) > _MAX_CANDIDATE_LEDGER_RECORD_BYTES:
                raise HorizonModelError("candidate ledger header is oversized")
            canonical_digest.update(header_line)
            recompressed.write(header_line)
            header = _canonical_ledger_record(header_line, 1)
            if header != dict(expected_header):
                raise HorizonModelError("candidate ledger header differs")
            line_number = 1
            while True:
                line = stream.readline(_MAX_CANDIDATE_LEDGER_RECORD_BYTES + 1)
                if not line:
                    break
                line_number += 1
                if len(line) > _MAX_CANDIDATE_LEDGER_RECORD_BYTES:
                    raise HorizonModelError("candidate ledger row is oversized")
                if line_number > int(reference["row_count"]) + 1:
                    raise HorizonModelError("candidate ledger contains extra rows")
                canonical_digest.update(line)
                recompressed.write(line)
                item = _canonical_ledger_record(line, line_number)
                if set(item) != _CANDIDATE_LEDGER_ROW_KEYS:
                    raise HorizonModelError("candidate ledger row schema differs")
                if item.get("record_type") != "CANDIDATE":
                    raise HorizonModelError("candidate ledger row type differs")
                fold_number_raw = item.get("fold_number")
                if (
                    isinstance(fold_number_raw, bool)
                    or not isinstance(fold_number_raw, int)
                    or fold_number_raw not in folds
                ):
                    raise HorizonModelError("candidate ledger fold differs")
                fold_number = int(fold_number_raw)
                decision = _date_value(
                    item.get("decision_session_date"),
                    "candidate ledger decision_session_date",
                )
                maturity = _date_value(
                    item.get("outcome_matures_on"),
                    "candidate ledger outcome_matures_on",
                )
                if (
                    maturity <= decision
                    or maturity > manifest_last_maturity
                    or maturity > training_cutoff
                ):
                    raise HorizonModelError("candidate ledger maturity clock differs")
                if not (
                    manifest_first_decision
                    <= decision
                    <= manifest_last_decision
                ):
                    raise HorizonModelError(
                        "candidate ledger dataset clock differs"
                    )
                fold = folds[fold_number]
                if not (
                    _date_value(fold["validation_start"], "validation_start")
                    <= decision
                    <= _date_value(fold["validation_end"], "validation_end")
                ):
                    raise HorizonModelError("candidate ledger validation clock differs")
                stock_code = str(item.get("stock_code") or "").strip()
                sample_id = str(item.get("sample_id") or "")
                expected_sample_id = hashlib.sha256(
                    (
                        f"{stock_code}|{decision.isoformat()}|T+"
                        f"{spec.horizon_days}|{LABEL_PROTOCOL}"
                    ).encode("utf-8")
                ).hexdigest()
                if not stock_code or sample_id != expected_sample_id:
                    raise HorizonModelError("candidate ledger sample identity differs")
                key = (fold_number, decision.isoformat(), sample_id)
                if previous_key is not None and key <= previous_key:
                    raise HorizonModelError(
                        "candidate ledger rows are duplicated or out of order"
                    )
                current_session = (fold_number, decision.isoformat())
                if previous_session is not None and current_session != previous_session:
                    finalize_session()
                    session_rows.clear()
                    if (
                        previous_session_date is not None
                        and decision.isoformat() <= previous_session_date
                    ):
                        raise HorizonModelError(
                            "candidate ledger session chronology differs"
                        )
                for field in (
                    "gross_return_pct",
                    "net_return_pct",
                    "raw_expected_return_net_pct",
                    "normalized_score",
                    "anchor_score",
                ):
                    item[field] = _finite(item.get(field), f"candidate.{field}")
                if abs(
                    (float(item["gross_return_pct"]) - float(item["net_return_pct"]))
                    - spec.cost_assumption_pct
                ) > 1e-9:
                    raise HorizonModelError("candidate ledger cost arithmetic differs")
                calibrated = item.get("calibration_available")
                if not isinstance(calibrated, bool):
                    raise HorizonModelError("candidate ledger calibration flag differs")
                if calibrated:
                    item["expected_return_net_pct"] = _finite(
                        item.get("expected_return_net_pct"),
                        "candidate.expected_return_net_pct",
                    )
                    item["probability_positive"] = _finite(
                        item.get("probability_positive"),
                        "candidate.probability_positive",
                    )
                    if not 0.0 <= float(item["probability_positive"]) <= 1.0:
                        raise HorizonModelError(
                            "candidate ledger probability differs"
                        )
                    evaluation_row_count += 1
                elif (
                    item.get("expected_return_net_pct") is not None
                    or item.get("probability_positive") is not None
                ):
                    raise HorizonModelError(
                        "candidate ledger unavailable calibration has predictions"
                    )

                if fold_number not in fold_hashers:
                    fold_hashers[fold_number] = hashlib.sha256()
                    fold_hashers[fold_number].update(b"[")
                    fold_hash_first[fold_number] = True
                if not fold_hash_first[fold_number]:
                    fold_hashers[fold_number].update(b",")
                fold_hashers[fold_number].update(
                    canonical_json(_fold_oos_prediction_record(item)).encode(
                        "utf-8"
                    )
                )
                fold_hash_first[fold_number] = False
                fold_counts[fold_number] += 1
                row_count += 1
                session_rows.append(item)
                previous_key = key
                previous_session = current_session
                previous_session_date = decision.isoformat()
            finalize_session()
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise HorizonModelError("cannot decode candidate evaluation ledger") from exc

    if canonical_digest.hexdigest() != reference["canonical_records_sha256"]:
        raise HorizonModelError("candidate ledger canonical record hash differs")
    if (
        deterministic_gzip.digest.hexdigest() != actual_content_hash
        or deterministic_gzip.size != actual_size
    ):
        raise HorizonModelError("candidate ledger gzip encoding is not deterministic")
    fold_oos_prediction_hashes: dict[str, str] = {}
    for fold_number, fold in sorted(folds.items()):
        hasher = fold_hashers.get(fold_number)
        if hasher is None:
            raise HorizonModelError("candidate ledger omitted a walk-forward fold")
        hasher.update(b"]")
        fold_hash = hasher.hexdigest()
        fold_oos_prediction_hashes[str(fold_number)] = fold_hash
        if fold_hash != fold.get("oos_prediction_hash"):
            raise HorizonModelError("candidate ledger fold prediction hash differs")
        if (
            fold_counts[fold_number] != int(fold.get("validation_sample_count") or 0)
            or fold_session_counts[fold_number]
            != int(fold.get("distinct_validation_sessions") or 0)
        ):
            raise HorizonModelError("candidate ledger fold counts differ")

    actual_counts = {
        "row_count": row_count,
        "session_count": session_count,
        "evaluation_row_count": evaluation_row_count,
        "evaluation_session_count": evaluation_session_count,
        "fold_count": len(fold_hashers),
    }
    for field, actual in actual_counts.items():
        if actual != int(reference.get(field) or 0):
            raise HorizonModelError(f"candidate ledger {field} differs")
    evidence = document["oos_evidence"]
    persisted_session_counts = {
        (
            int(item["fold_number"]),
            str(item["decision_session_date"]),
        ): int(item["sample_count"])
        for item in evidence["direction_evidence"]["sessions"]
    }
    if evaluation_session_counts != persisted_session_counts:
        raise HorizonModelError(
            "candidate ledger per-session candidate counts differ"
        )
    if (
        row_count != int(document["walk_forward"]["oos_sample_count"])
        or row_count != int(evidence["oos_sample_count"])
        or session_count != int(evidence["distinct_oos_sessions"])
        or evaluation_row_count
        != int(evidence["calibration_evaluation_sample_count"])
        or evaluation_session_count
        != int(evidence["distinct_calibration_evaluation_sessions"])
    ):
        raise HorizonModelError("candidate ledger artifact counts differ")

    rebuilt_selection = _selection_evidence_from_components(
        selected_rows=selected_rows,
        selection_frontier=selection_frontier,
        candidate_count=evaluation_row_count,
        eligible_count=eligible_count,
        policy=policy,
        cost_assumption_pct=spec.cost_assumption_pct,
        candidate_ledger_reference=reference,
    )
    persisted_selection = evidence["selection_evidence"]
    if canonical_hash(rebuilt_selection) != canonical_hash(persisted_selection):
        raise HorizonModelError(
            "candidate ledger selection reconstruction differs from artifact"
        )
    metrics = _selected_ledger_metrics(
        selected_rows,
        cost_assumption_pct=spec.cost_assumption_pct,
    )
    body = {
        "protocol": CANDIDATE_LEDGER_REGISTRATION_PROTOCOL,
        "artifact_hash": document["artifact_hash"],
        "ledger_schema": CANDIDATE_EVALUATION_LEDGER_SCHEMA,
        "ledger_content_sha256": reference["content_sha256"],
        "canonical_records_sha256": reference["canonical_records_sha256"],
        **actual_counts,
        "selection_policy_hash": document["selection_policy"][
            "selection_policy_hash"
        ],
        "selection_evidence_hash": rebuilt_selection[
            "selection_evidence_hash"
        ],
        "selected_ledger_hash": rebuilt_selection["selected_ledger_hash"],
        **metrics,
        "fold_oos_prediction_hashes": fold_oos_prediction_hashes,
    }
    return {**body, "registration_evidence_hash": canonical_hash(body)}


def _split_walk_forward_sessions(
    sessions: list[date],
    horizon: int,
    policy: HorizonTrainingPolicy,
) -> list[list[date]]:
    minimum_train = policy.minimum_train_sessions[horizon]
    available_oos = max(0, len(sessions) - minimum_train)
    if available_oos <= 0:
        return []
    desired_oos = max(
        policy.minimum_oos_sessions[horizon],
        int(math.ceil(len(sessions) * 0.25)),
        policy.walk_forward_fold_count * 2,
    )
    oos_count = min(available_oos, desired_oos)
    oos_sessions = sessions[-oos_count:]
    return [
        list(item)
        for item in np.array_split(np.asarray(oos_sessions, dtype=object), policy.walk_forward_fold_count)
        if len(item)
    ]


def _evaluate_gate(
    evidence: Mapping[str, Any],
    policy: HorizonTrainingPolicy,
    horizon: int,
    *,
    final_model_available: bool,
    selection_policy_is_production_default: bool,
) -> tuple[str, list[str]]:
    def metric(name: str, missing: float) -> float:
        value = evidence.get(name)
        return missing if value is None else float(value)

    reasons: list[str] = []
    if int(evidence.get("distinct_train_sessions") or 0) < policy.minimum_train_sessions[horizon]:
        reasons.append("INSUFFICIENT_TEMPORAL_COVERAGE")
    if int(evidence.get("distinct_oos_sessions") or 0) < policy.minimum_oos_sessions[horizon]:
        reasons.append("INSUFFICIENT_TEMPORAL_COVERAGE")
    if int(evidence.get("matured_sample_count") or 0) < policy.minimum_mature_samples[horizon]:
        reasons.append("INSUFFICIENT_MATURE_SAMPLES")
    if int(evidence.get("oos_sample_count") or 0) < policy.minimum_oos_samples[horizon]:
        reasons.append("INSUFFICIENT_OOS_SAMPLES")
    if int(evidence.get("selected_oos_sample_count") or 0) < policy.minimum_oos_samples[horizon]:
        reasons.append("INSUFFICIENT_SELECTED_OOS_SAMPLES")
    if int(evidence.get("selected_oos_session_count") or 0) < policy.minimum_oos_sessions[horizon]:
        reasons.append("INSUFFICIENT_SELECTED_OOS_SESSIONS")
    if int(evidence.get("walk_forward_fold_count") or 0) < policy.walk_forward_fold_count:
        reasons.append("INSUFFICIENT_WALK_FORWARD_FOLDS")
    if metric("maturity_coverage", 0.0) < policy.minimum_maturity_coverage:
        reasons.append("MATURITY_COVERAGE_INCOMPLETE")
    if metric("direction_rank_correlation", -1.0) < policy.minimum_direction_rank_correlation:
        reasons.append("SCORE_DIRECTION_INVALID")
    if metric("calibration_mae", 1.0) > policy.maximum_calibration_mae:
        reasons.append("CALIBRATION_MAE_FAILED")
    if metric("brier_score", 1.0) > policy.maximum_brier_score:
        reasons.append("BRIER_SCORE_FAILED")
    if metric("population_stability_index", 999.0) > policy.maximum_population_stability_index:
        reasons.append("POPULATION_STABILITY_FAILED")
    if metric("net_expectancy_after_cost_pct", -999.0) <= policy.minimum_net_expectancy_after_cost_pct:
        reasons.append("NET_EXPECTANCY_AFTER_COST_FAILED")
    if metric("profit_factor", 0.0) < policy.minimum_profit_factor:
        reasons.append("PROFIT_FACTOR_FAILED")
    if metric("cost_coverage_ratio", 0.0) < policy.minimum_cost_coverage_ratio:
        reasons.append("COST_COVERAGE_FAILED")
    if not bool(evidence.get("outcomes_include_costs")):
        reasons.append("COST_INCLUSIVE_LABEL_UNVERIFIED")
    if not bool(evidence.get("calibration_is_oos_only")):
        reasons.append("OOS_CALIBRATION_UNVERIFIED")
    if not bool(evidence.get("calibration_evaluation_is_prequential")):
        reasons.append("PREQUENTIAL_CALIBRATION_UNVERIFIED")
    if not bool(evidence.get("calibration_labels_purged_by_maturity")):
        reasons.append("CALIBRATION_MATURITY_PURGE_UNVERIFIED")
    if not bool(evidence.get("economic_metrics_use_frozen_selection_ledger")):
        reasons.append("FROZEN_SELECTION_LEDGER_UNVERIFIED")
    if not selection_policy_is_production_default:
        reasons.append("NON_DEFAULT_SELECTION_POLICY_RESEARCH_ONLY")
    if (
        evidence.get("training_window_status")
        != "FROZEN_DEFAULT_TRAINING_WINDOW"
        or evidence.get("training_window_is_current_config_default") is not True
    ):
        reasons.append("NON_DEFAULT_TRAINING_WINDOW")
    if evidence.get("universe_scope") != "FULL_A_SHARE_POINT_IN_TIME":
        reasons.append("UNIVERSE_SCOPE_NOT_PRODUCTION")
    if not final_model_available:
        reasons.append("TRAINING_FIT_UNAVAILABLE")
    reasons = list(dict.fromkeys(reasons))
    return ("PASS" if not reasons else "BLOCK"), reasons


def train_independent_horizon_model(
    dataset: HorizonDataset,
    *,
    release_id: str,
    suite_release_id: str | None = None,
    training_cutoff: date | datetime | str,
    policy: HorizonTrainingPolicy = DEFAULT_TRAINING_POLICY,
    selection_policy: HorizonSelectionPolicy = DEFAULT_SELECTION_POLICY,
    created_at: datetime | str | None = None,
    config_sha256: str | None = None,
    candidate_ledger_root: str | Path | None = None,
) -> dict[str, Any]:
    """Train one horizon and return a self-verifying research artifact."""

    if not release_id or not str(release_id).strip():
        raise HorizonModelError("release_id must not be empty")
    horizon = int(dataset.horizon_days)
    if horizon not in HORIZON_MODEL_SPECS:
        raise HorizonModelError("unsupported dataset horizon")
    spec = HORIZON_MODEL_SPECS[horizon]
    suite_id = str(suite_release_id or "").strip()
    if not suite_id:
        raise HorizonModelError("suite_release_id must not be empty")
    expected_release_id = horizon_governance_release_id(
        suite_release_id=suite_id,
        model_key=spec.model_key,
        model_version=spec.model_version,
        horizon_days=horizon,
    )
    if str(release_id).strip() != expected_release_id:
        raise HorizonModelError(
            "release_id must bind suite/model/version/horizon"
        )
    dataset_window = dict(dataset.manifest.get("training_window") or {})
    _verify_hashed_mapping(dataset_window, "training_window_hash")
    if (
        dataset_window.get("protocol") != TRAINING_WINDOW_PROTOCOL
        or dataset_window.get("configured_history_start")
        != policy.history_start.isoformat()
    ):
        raise HorizonModelError("dataset/training policy window differs")
    dataset_frame_hash = _dataset_frame_hash(dataset.frame, spec.features)
    if (
        dataset.manifest.get("dataset_frame_hash") != dataset_frame_hash
        or dataset.manifest.get("dataset_hash")
        != _dataset_identity_hash(
            frame_hash=dataset_frame_hash,
            spec=spec,
            training_window=dataset_window,
        )
    ):
        raise HorizonModelError("dataset frame/window differs from dataset_hash")
    cutoff_date = (
        training_cutoff.date()
        if isinstance(training_cutoff, datetime)
        else _date_value(training_cutoff, "training_cutoff")
    )
    cutoff = pd.Timestamp(datetime.combine(cutoff_date, time(23, 59), EXCHANGE_TIMEZONE)).tz_convert("UTC")
    generated = (
        _aware_timestamp(created_at, "created_at").to_pydatetime()
        if created_at is not None
        else datetime.now(timezone.utc).replace(microsecond=0)
    )
    frame = dataset.frame.copy()
    maturity = pd.to_datetime(frame["label_mature_at"], utc=True)
    eligible_by_cutoff = pd.to_datetime(frame["outcome_matures_on"]).dt.date <= cutoff_date
    mature = frame["label_available"].astype(bool) & maturity.le(cutoff)
    usable = frame.loc[mature].copy()
    expected_mature = int(eligible_by_cutoff.sum())
    maturity_coverage = len(usable) / expected_mature if expected_mature else 0.0
    final_model: dict[str, Any] | None = None
    if len(usable) > len(spec.features):
        final_model = _fit_model(usable, spec)
    sessions = sorted(pd.to_datetime(usable["decision_session_date"]).dt.date.unique().tolist())
    folds: list[dict[str, Any]] = []
    oos_parts: list[pd.DataFrame] = []
    calibration_evaluation_parts: list[pd.DataFrame] = []
    prior_oos_parts: list[pd.DataFrame] = []
    anchor_model: dict[str, Any] | None = None
    for fold_number, validation_sessions in enumerate(
        _split_walk_forward_sessions(sessions, horizon, policy), start=1
    ):
        validation_start = min(validation_sessions)
        training = usable[
            (pd.to_datetime(usable["decision_session_date"]).dt.date < validation_start)
            & (pd.to_datetime(usable["outcome_matures_on"]).dt.date < validation_start)
        ].copy()
        validation = usable[
            pd.to_datetime(usable["decision_session_date"]).dt.date.isin(validation_sessions)
        ].copy()
        if len(training) <= len(spec.features) or validation.empty:
            continue
        model = _fit_model(training, spec)
        raw, normalized_score = _normalized_model_score(model, validation)
        if anchor_model is None:
            anchor_model = model
        _, anchor_score = _normalized_model_score(anchor_model, validation)
        fold_oos = validation.loc[:, [
            "sample_id", "stock_code", "decision_session_date",
            "outcome_matures_on", "label_mature_at",
            "gross_return_pct", "net_return_pct",
        ]].copy()
        fold_oos["raw_expected_return_net_pct"] = raw
        fold_oos["normalized_score"] = normalized_score
        fold_oos["anchor_score"] = anchor_score
        fold_oos["fold_number"] = fold_number
        fold_oos["calibration_available"] = False
        fold_oos["expected_return_net_pct"] = np.nan
        fold_oos["probability_positive"] = np.nan
        calibration_sample_count = 0
        calibration_latest_maturity: str | None = None
        calibration_training_hash = canonical_hash([])
        fold_calibration: dict[str, Any] | None = None
        if prior_oos_parts:
            prior = pd.concat(prior_oos_parts, ignore_index=True)
            prior = prior[
                pd.to_datetime(prior["outcome_matures_on"]).dt.date
                < validation_start
            ].copy()
            if not prior.empty:
                calibration_sample_count = len(prior)
                calibration_latest_maturity = pd.Timestamp(
                    prior["outcome_matures_on"].max()
                ).date().isoformat()
                calibration_training_rows = [
                    {
                        "sample_id": str(row.sample_id),
                        "outcome_matures_on": pd.Timestamp(
                            row.outcome_matures_on
                        ).date().isoformat(),
                        "normalized_score": _stable_float(
                            row.normalized_score
                        ),
                        "net_return_pct": _stable_float(row.net_return_pct),
                    }
                    for row in prior.sort_values(
                        "sample_id", kind="mergesort"
                    ).itertuples(index=False)
                ]
                calibration_training_hash = canonical_hash(
                    calibration_training_rows
                )
                fold_calibration = _fit_calibration(
                    prior,
                    bucket_count=policy.calibration_bucket_count,
                )
                expected, probability = _apply_calibration(
                    fold_calibration, normalized_score
                )
                evaluated = fold_oos.copy()
                evaluated["expected_return_net_pct"] = expected
                evaluated["probability_positive"] = probability
                evaluated["calibration_available"] = True
                fold_oos["expected_return_net_pct"] = expected
                fold_oos["probability_positive"] = probability
                fold_oos["calibration_available"] = True
                calibration_evaluation_parts.append(evaluated)
        prior_oos_parts.append(fold_oos)
        oos_parts.append(fold_oos)
        fold_body = {
            "fold_number": fold_number,
            "training_start": pd.Timestamp(training["decision_session_date"].min()).date().isoformat(),
            "training_end": pd.Timestamp(training["decision_session_date"].max()).date().isoformat(),
            "latest_training_label_maturity": pd.Timestamp(training["outcome_matures_on"].max()).date().isoformat(),
            "validation_start": validation_start.isoformat(),
            "validation_end": max(validation_sessions).isoformat(),
            "training_sample_count": len(training),
            "validation_sample_count": len(validation),
            "distinct_training_sessions": int(training["decision_session_date"].nunique()),
            "distinct_validation_sessions": int(validation["decision_session_date"].nunique()),
            "validation_net_expectancy_after_cost_pct": _stable_float(
                fold_oos["net_return_pct"].mean()
            ),
            "validation_profit_factor": _stable_float(
                _profit_factor(fold_oos["net_return_pct"].astype(float).tolist())
            ),
            "validation_direction_rank_correlation": _stable_float(
                _mean_session_spearman(
                    fold_oos,
                    "normalized_score",
                    minimum_size=selection_policy.minimum_cross_section_size,
                )
            ),
            "validation_metrics_are_unconditional_diagnostics_only": True,
            "score_normalization_protocol": SCORE_NORMALIZATION_PROTOCOL,
            "training_score_mean": model["training_score_mean"],
            "training_score_std": model["training_score_std"],
            "calibration_training_sample_count": calibration_sample_count,
            "latest_calibration_label_maturity": calibration_latest_maturity,
            "calibration_training_hash": calibration_training_hash,
            "prequential_calibration": fold_calibration,
            "model_hash": model["model_hash"],
            "oos_prediction_hash": canonical_hash([
                _fold_oos_prediction_record(row._asdict())
                for row in fold_oos.sort_values(
                    ["decision_session_date", "sample_id"],
                    kind="mergesort",
                ).itertuples(index=False)
            ]),
        }
        folds.append({**fold_body, "fold_hash": canonical_hash(fold_body)})
    oos = pd.concat(oos_parts, ignore_index=True) if oos_parts else pd.DataFrame()
    evaluation = (
        pd.concat(calibration_evaluation_parts, ignore_index=True)
        if calibration_evaluation_parts
        else pd.DataFrame()
    )
    calibration = (
        _fit_calibration(oos, bucket_count=policy.calibration_bucket_count)
        if not oos.empty
        else None
    )
    calibration_protocol_hash = canonical_hash({
        "protocol": CALIBRATION_PROTOCOL,
        "bucket_count": policy.calibration_bucket_count,
        "evaluation": "MATURITY_PURGED_PREQUENTIAL_PRIOR_FOLDS_ONLY",
        "score_normalization": SCORE_NORMALIZATION_PROTOCOL,
    })
    selection_policy_document = _selection_policy_document(selection_policy)
    candidate_ledger_header = _candidate_ledger_header(
        suite_release_id=suite_id,
        release_id=str(release_id).strip(),
        spec=spec,
        dataset_hash=dataset.dataset_hash,
        training_window=dataset_window,
        selection_policy_hash=selection_policy_document[
            "selection_policy_hash"
        ],
        calibration_protocol_hash=calibration_protocol_hash,
    )
    candidate_ledger_reference = _build_candidate_ledger_reference(
        oos,
        header=candidate_ledger_header,
        ledger_root=candidate_ledger_root,
    )
    calibration_metric_evidence = _calibration_metric_evidence(evaluation)
    brier, calibration_mae = _verify_calibration_metric_evidence(
        calibration_metric_evidence
    )
    direction_evidence = _session_direction_evidence(
        evaluation,
        minimum_cross_section_size=selection_policy.minimum_cross_section_size,
    )
    selection_evidence = _build_selection_evidence(
        evaluation,
        policy=selection_policy,
        cost_assumption_pct=spec.cost_assumption_pct,
        candidate_ledger_reference=candidate_ledger_reference,
    )
    selection_metrics = _verify_selection_evidence(
        selection_evidence,
        policy=selection_policy,
        cost_assumption_pct=spec.cost_assumption_pct,
        candidate_ledger_reference=candidate_ledger_reference,
    )
    unconditional_values = (
        oos["net_return_pct"].astype(float).tolist() if not oos.empty else []
    )
    unconditional_gross = (
        float(oos["gross_return_pct"].mean()) if not oos.empty else -999.0
    )
    unconditional_cost_coverage = (
        max(0.0, unconditional_gross) / spec.cost_assumption_pct
        if spec.cost_assumption_pct > 0 else 999999.0
    )
    population_stability = 999.0
    psi_evidence = _population_stability_evidence(
        (), (), reference_fold=0, anchor_model_hash=None,
        bucket_count=policy.calibration_bucket_count
    )
    if not oos.empty and oos["fold_number"].nunique() >= 2:
        first_fold = int(oos["fold_number"].min())
        reference_scores = oos.loc[
            oos["fold_number"].eq(first_fold), "anchor_score"
        ].to_numpy(dtype=float)
        subsequent_scores = oos.loc[
            oos["fold_number"].gt(first_fold), "anchor_score"
        ].to_numpy(dtype=float)
        psi_evidence = _population_stability_evidence(
            reference_scores,
            subsequent_scores,
            reference_fold=first_fold,
            anchor_model_hash=(
                str(anchor_model["model_hash"])
                if anchor_model is not None else None
            ),
            bucket_count=policy.calibration_bucket_count,
        )
        population_stability = float(
            psi_evidence["population_stability_index"]
        )
    evidence_body = {
        "evidence_schema": "probiga.trading-v3.horizon-oos-evidence.v3",
        "model_key": spec.model_key,
        "model_version": spec.model_version,
        "model_protocol": MODEL_PROTOCOL,
        "horizon_days": horizon,
        "dataset_hash": dataset.dataset_hash,
        "training_window": copy.deepcopy(dataset_window),
        "training_window_status": dataset_window["status"],
        "training_window_is_current_config_default": dataset_window[
            "is_current_config_default"
        ],
        "feature_protocol_hash": canonical_hash(spec.feature_protocol()),
        "calibration_protocol_hash": calibration_protocol_hash,
        "training_cutoff": cutoff_date.isoformat(),
        "valid_until": (cutoff_date + timedelta(days=30)).isoformat(),
        "matured_sample_count": len(usable),
        "expected_mature_candidate_count": expected_mature,
        "maturity_coverage": _stable_float(maturity_coverage),
        "distinct_train_sessions": len(sessions),
        "oos_sample_count": len(oos),
        "distinct_oos_sessions": (
            int(oos["decision_session_date"].nunique()) if not oos.empty else 0
        ),
        "calibration_evaluation_sample_count": len(evaluation),
        "distinct_calibration_evaluation_sessions": (
            int(evaluation["decision_session_date"].nunique()) if not evaluation.empty else 0
        ),
        "walk_forward_fold_count": len(folds),
        "direction_rank_correlation": direction_evidence[
            "gate_direction_rank_ic"
        ],
        "expected_return_direction_rank_correlation": direction_evidence[
            "expected_return_rank_ic"
        ],
        "probability_direction_rank_correlation": direction_evidence[
            "probability_rank_ic"
        ],
        "direction_evidence": direction_evidence,
        "calibration_mae": _stable_float(calibration_mae),
        "brier_score": _stable_float(brier),
        "calibration_metric_evidence": calibration_metric_evidence,
        "population_stability_index": _stable_float(population_stability),
        "population_stability_evidence": psi_evidence,
        **selection_metrics,
        "selection_evidence": selection_evidence,
        "unconditional_baseline_net_expectancy_after_cost_pct": _stable_float(
            float(np.mean(unconditional_values))
            if unconditional_values else -999.0
        ),
        "unconditional_baseline_profit_factor": _stable_float(
            _profit_factor(unconditional_values)
        ),
        "unconditional_baseline_cost_coverage_ratio": _stable_float(
            unconditional_cost_coverage
        ),
        "outcomes_include_costs": True,
        "calibration_is_oos_only": True,
        "calibration_evaluation_is_prequential": True,
        "calibration_labels_purged_by_maturity": True,
        "economic_metrics_use_frozen_selection_ledger": True,
        "candidate_economic_metrics_bound_to_stream_verifiable_ledger": True,
        "candidate_evaluation_ledger_reference_hash": (
            candidate_ledger_reference["reference_hash"]
        ),
        "selection_policy_is_production_default": (
            _selection_policy_document(selection_policy)[
                "selection_policy_hash"
            ]
            == DEFAULT_SELECTION_POLICY_HASH
        ),
        "horizontal_sample_count_cannot_replace_sessions": True,
        "universe_scope": dataset.manifest.get("universe_scope"),
        "execution_evidence_scope": "LONG_HISTORY_OOS_RESEARCH_ONLY",
        "qmt_attested_label_count": int(
            usable.get("label_qmt_attested", pd.Series(False, index=usable.index)).sum()
        ),
        "qmt_attested_label_coverage": _stable_float(
            usable.get("label_qmt_attested", pd.Series(False, index=usable.index)).mean()
            if len(usable) else 0.0
        ),
        "label_attestation_required_for_execution": True,
        "executable_verified": False,
    }
    evidence = {**evidence_body, "evidence_hash": canonical_hash(evidence_body)}
    gate_status, block_reasons = _evaluate_gate(
        evidence,
        policy,
        horizon,
        final_model_available=final_model is not None and calibration is not None,
        selection_policy_is_production_default=bool(
            evidence["selection_policy_is_production_default"]
        ),
    )
    policy_body = policy.as_dict()
    frozen_policy = {**policy_body, "policy_hash": canonical_hash(policy_body)}
    frozen_selection_policy = _selection_policy_document(selection_policy)
    execution_body = {
        "entry_price": "NEXT_EXCHANGE_SESSION_RAW_OPEN",
        "exit_price": "ENTRY_PLUS_HORIZON_EXCHANGE_SESSION_RAW_CLOSE",
        "exact_exchange_session_offsets": True,
        "same_close_entry_allowed": False,
        "t0_exit_allowed": False,
        "outcomes_include_roundtrip_costs": True,
        "zero_volume_entry_exit_quarantined": True,
        "cost_model_version": spec.cost_model_version,
        "cost_assumption_pct": spec.cost_assumption_pct,
        "corporate_actions_quarantined": True,
        "label_protocol": LABEL_PROTOCOL,
        "status": "RESEARCH_LABEL_PROTOCOL_VERIFIED",
        "provenance": "SELF_VERIFIED_RESEARCH_ARTIFACT",
        "execution_evidence_scope": "LONG_HISTORY_OOS_RESEARCH_ONLY",
        "label_attestation_required_for_execution": True,
        "executable_verified": False,
    }
    execution = {**execution_body, "attestation_hash": canonical_hash(execution_body)}
    feature_protocol = spec.feature_protocol()
    config_digest = _digest(config_sha256 or current_config_hash(), "config_hash")
    current_code_version, current_code_kind = code_version()
    source_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    artifact_body: dict[str, Any] = {
        "schema_version": ARTIFACT_SCHEMA,
        "release_id": str(release_id).strip(),
        "suite_release_id": suite_id,
        "model_key": spec.model_key,
        "model_version": spec.model_version,
        "model_protocol": MODEL_PROTOCOL,
        "horizon_days": horizon,
        "prediction_kind": "CALIBRATED_OOS",
        "lifecycle": "SHADOW_RESEARCH_ONLY",
        "order_authority": False,
        "contract_eligible": gate_status == "PASS",
        "contract_eligibility_scope": CONTRACT_ELIGIBILITY_SCOPE,
        "paper_eligible": False,
        "production_eligible": False,
        "created_at": generated.astimezone(timezone.utc).isoformat(),
        "training_cutoff": cutoff_date.isoformat(),
        "valid_until": evidence["valid_until"],
        "config_hash": config_digest,
        "code_version": current_code_version,
        "code_version_kind": current_code_kind,
        "model_code_version": MODEL_CODE_VERSION,
        "code_hash": source_hash,
        "integrity_scope": "SELF_CONSISTENT_LOCAL_RESEARCH_ARTIFACT_NOT_EXTERNAL_ATTESTATION",
        "mapping_verification_scope": (
            "SELF_CONSISTENT_ONLY_NOT_REGISTRATION_EVIDENCE"
        ),
        "candidate_ledger_registration_required": True,
        "feature_protocol": feature_protocol,
        "feature_protocol_hash": canonical_hash(feature_protocol),
        "dataset_manifest": copy.deepcopy(dataset.manifest),
        "dataset_hash": dataset.dataset_hash,
        "training_window": copy.deepcopy(dataset_window),
        "candidate_evaluation_ledger": candidate_ledger_reference,
        "model_spec": spec.as_dict(),
        "final_model": final_model,
        "walk_forward": {
            "protocol": "EXPANDING_SESSION_SPLIT_PURGED_BY_LABEL_MATURITY_V2",
            "folds": folds,
            "oos_sample_count": len(oos),
            "distinct_oos_sessions": evidence["distinct_oos_sessions"],
        },
        "calibration": calibration,
        "calibration_protocol_hash": evidence["calibration_protocol_hash"],
        "oos_evidence": evidence,
        "oos_evidence_hash": evidence["evidence_hash"],
        "execution_feasibility": execution,
        "training_policy": frozen_policy,
        "selection_policy": frozen_selection_policy,
        "gate": {
            "status": gate_status,
            "gate_scope": selection_policy.candidate_domain,
            "deployment_gate": False,
            "training_window_status": dataset_window["status"],
            "training_window_is_current_config_default": dataset_window[
                "is_current_config_default"
            ],
            "block_reasons": block_reasons,
            "contract_eligible": gate_status == "PASS",
            "contract_eligibility_requires_stream_verified_ledger": True,
            "contract_eligibility_scope": CONTRACT_ELIGIBILITY_SCOPE,
            "paper_eligible": False,
            "production_eligible": False,
            "automatic_promotion_allowed": False,
            "external_signed_attestation_required": True,
            "order_authority": False,
        },
    }
    artifact_hash = canonical_hash(_artifact_core_payload(artifact_body))
    artifact = {
        **artifact_body,
        "artifact_hash": artifact_hash,
        "creation_envelope_hash": canonical_hash({
            "artifact_hash": artifact_hash,
            "created_at": artifact_body["created_at"],
        }),
    }
    verify_horizon_artifact(artifact, require_current_config=False)
    if dataset_window["is_current_config_default"] is True:
        verify_horizon_artifact(artifact)
    return artifact


def _verify_hashed_mapping(value: Mapping[str, Any], field: str) -> None:
    expected = _digest(value.get(field), field)
    if canonical_hash(_without_hash(value, field)) != expected:
        raise HorizonModelError(f"{field} differs")


def _verify_current_config_model_contract(
    spec: HorizonModelSpec,
    *,
    training_policy: HorizonTrainingPolicy,
    training_window: Mapping[str, Any],
) -> None:
    suite = dict(load_v3_config().get("multi_horizon_forecasts") or {})
    if (
        suite.get("protocol_version") != MULTI_HORIZON_CONFIG_PROTOCOL
        or suite.get("model_protocol") != MODEL_PROTOCOL
    ):
        raise HorizonModelError("current multi-horizon config protocol differs")
    training_config = dict(suite.get("training_policy") or {})
    if (
        training_config.get("protocol_version") != TRAINING_CONFIG_PROTOCOL
        or training_config.get("training_window_protocol")
        != TRAINING_WINDOW_PROTOCOL
    ):
        raise HorizonModelError("current training config protocol differs")
    current_history_start = _date_value(
        training_config.get("history_start"),
        "current training history_start",
    ).isoformat()
    if (
        training_window.get("status")
        != "FROZEN_DEFAULT_TRAINING_WINDOW"
        or training_window.get("is_current_config_default") is not True
        or training_window.get("protocol") != TRAINING_WINDOW_PROTOCOL
        or training_window.get("configured_history_start")
        != current_history_start
        or training_window.get("signal_start") != current_history_start
        or training_policy.training_window_protocol
        != TRAINING_WINDOW_PROTOCOL
        or training_policy.history_start.isoformat() != current_history_start
    ):
        raise HorizonModelError("artifact training window is not current config")
    runtime_selection = dict(suite.get("runtime_model_selection") or {})
    current_runtime = dict(runtime_selection.get("current_protocol") or {})
    if (
        current_runtime.get("artifact_schema") != ARTIFACT_SCHEMA
        or current_runtime.get("suite_schema") != SUITE_SCHEMA
        or current_runtime.get("model_protocol") != MODEL_PROTOCOL
        or current_runtime.get("selection_protocol") != SELECTION_PROTOCOL
        or current_runtime.get("selection_policy_hash")
        != DEFAULT_SELECTION_POLICY_HASH
        or current_runtime.get("candidate_ledger_schema")
        != CANDIDATE_EVALUATION_LEDGER_SCHEMA
        or current_runtime.get("candidate_ledger_binding_protocol")
        != CANDIDATE_LEDGER_BINDING_PROTOCOL
        or current_runtime.get("candidate_ledger_registration_required")
        is not True
        or current_runtime.get("training_window_protocol")
        != TRAINING_WINDOW_PROTOCOL
        or current_runtime.get("history_start") != current_history_start
        or current_runtime.get("artifact_status_required") != "OOS_VERIFIED"
        or current_runtime.get("contract_eligibility_scope")
        != CONTRACT_ELIGIBILITY_SCOPE
        or current_runtime.get("deployment_gate") is not False
        or current_runtime.get("paper_eligible") is not False
        or current_runtime.get("order_allowed") is not False
    ):
        raise HorizonModelError("current runtime model protocol differs")
    historical_runtime = dict(runtime_selection.get("historical_v1") or {})
    if (
        historical_runtime.get("artifact_schema")
        != HISTORICAL_ARTIFACT_SCHEMA_V1
        or historical_runtime.get("suite_schema") != HISTORICAL_SUITE_SCHEMA_V1
        or historical_runtime.get("mode") != "AUDIT_ONLY"
        or historical_runtime.get("runtime_selectable") is not False
        or historical_runtime.get("contract_eligible") is not False
        or historical_runtime.get("deployment_gate") is not False
        or historical_runtime.get("paper_eligible") is not False
        or historical_runtime.get("order_allowed") is not False
    ):
        raise HorizonModelError("historical V1 runtime audit boundary differs")
    historical_runtime_v2 = dict(
        runtime_selection.get("historical_v2") or {}
    )
    if (
        historical_runtime_v2.get("artifact_schema")
        != HISTORICAL_ARTIFACT_SCHEMA_V2
        or historical_runtime_v2.get("suite_schema")
        != HISTORICAL_SUITE_SCHEMA_V2
        or historical_runtime_v2.get("mode") != "AUDIT_ONLY"
        or historical_runtime_v2.get("missing_candidate_ledger") is not True
        or historical_runtime_v2.get("runtime_selectable") is not False
        or historical_runtime_v2.get("contract_eligible") is not False
        or historical_runtime_v2.get("deployment_gate") is not False
        or historical_runtime_v2.get("paper_eligible") is not False
        or historical_runtime_v2.get("order_allowed") is not False
    ):
        raise HorizonModelError("historical V2 runtime audit boundary differs")
    if (
        runtime_selection.get("policy")
        != "CURRENT_DB_OOS_VERIFIED_ELSE_FROZEN_PROXY"
        or runtime_selection.get("fallback_prediction_kind") != "PROXY_SCORE"
        or runtime_selection.get("fallback_proxy_models_enabled") is not True
        or runtime_selection.get("fallback_can_activate_model") is not False
        or runtime_selection.get("order_allowed") is not False
    ):
        raise HorizonModelError("runtime model selection boundary differs")
    configured = dict(
        (suite.get("trainable_models") or {}).get(
            f"T+{spec.horizon_days}"
        )
        or {}
    )
    if (
        configured.get("model_key") != spec.model_key
        or configured.get("model_version") != spec.model_version
        or configured.get("algorithm") != spec.algorithm
        or tuple(configured.get("features") or ()) != spec.features
        or configured.get("order_allowed") is not False
    ):
        raise HorizonModelError("current trainable model config differs from V2")


def verify_horizon_artifact(
    artifact: Mapping[str, Any],
    *,
    require_current_code: bool = True,
    require_current_config: bool = True,
) -> dict[str, Any]:
    """Deeply verify an artifact and recompute its gate from frozen evidence."""

    document = copy.deepcopy(dict(artifact))
    if document.get("schema_version") != ARTIFACT_SCHEMA:
        raise HorizonModelError("artifact schema is unsupported")
    artifact_hash = _digest(document.get("artifact_hash"), "artifact_hash")
    if canonical_hash(_artifact_core_payload(document)) != artifact_hash:
        raise HorizonModelError("artifact hash differs")
    if canonical_hash({
        "artifact_hash": artifact_hash,
        "created_at": document.get("created_at"),
    }) != _digest(document.get("creation_envelope_hash"), "creation_envelope_hash"):
        raise HorizonModelError("artifact creation envelope differs")
    horizon = int(document.get("horizon_days") or 0)
    if horizon not in HORIZON_MODEL_SPECS:
        raise HorizonModelError("artifact horizon is unsupported")
    spec = HORIZON_MODEL_SPECS[horizon]
    if document.get("model_key") != spec.model_key or document.get("model_version") != spec.model_version:
        raise HorizonModelError("artifact model identity is not frozen")
    if document.get("model_protocol") != MODEL_PROTOCOL:
        raise HorizonModelError("artifact model protocol differs from V2")
    if document.get("model_spec") != spec.as_dict():
        raise HorizonModelError("artifact model spec differs from frozen V2 spec")
    if document.get("model_code_version") != MODEL_CODE_VERSION:
        raise HorizonModelError("artifact model code protocol is not current V2")
    expected_release_id = horizon_governance_release_id(
        suite_release_id=str(document.get("suite_release_id") or ""),
        model_key=spec.model_key,
        model_version=spec.model_version,
        horizon_days=horizon,
    )
    if document.get("release_id") != expected_release_id:
        raise HorizonModelError("artifact governance release_id differs")
    if not str(document.get("suite_release_id") or "").strip():
        raise HorizonModelError("artifact suite_release_id is missing")
    if document.get("prediction_kind") != "CALIBRATED_OOS":
        raise HorizonModelError("artifact prediction kind differs")
    if document.get("lifecycle") != "SHADOW_RESEARCH_ONLY" or document.get("order_authority") is not False:
        raise HorizonModelError("artifact escaped the research-only boundary")
    feature_protocol = document.get("feature_protocol") or {}
    if feature_protocol != spec.feature_protocol():
        raise HorizonModelError("feature protocol differs from current frozen spec")
    if canonical_hash(feature_protocol) != _digest(document.get("feature_protocol_hash"), "feature_protocol_hash"):
        raise HorizonModelError("feature protocol hash differs")
    manifest = document.get("dataset_manifest") or {}
    if (
        manifest.get("schema_version") != DATASET_SCHEMA
        or manifest.get("model_key") != spec.model_key
        or int(manifest.get("horizon_days") or 0) != horizon
        or manifest.get("label_protocol") != LABEL_PROTOCOL
        or manifest.get("cost_model_version") != spec.cost_model_version
        or _finite(
            manifest.get("cost_assumption_pct"),
            "dataset_manifest.cost_assumption_pct",
        )
        != float(spec.cost_assumption_pct)
        or manifest.get("outcomes_include_costs") is not True
    ):
        raise HorizonModelError("dataset protocol differs from frozen V2 contract")
    if manifest.get("dataset_hash") != document.get("dataset_hash"):
        raise HorizonModelError("dataset hashes disagree")
    dataset_window = dict(manifest.get("training_window") or {})
    artifact_window = dict(document.get("training_window") or {})
    _verify_hashed_mapping(dataset_window, "training_window_hash")
    _verify_hashed_mapping(artifact_window, "training_window_hash")
    expected_window = _training_window_document(
        configured_history_start=artifact_window.get(
            "configured_history_start"
        ),
        signal_start=artifact_window.get("signal_start"),
        signal_end=artifact_window.get("signal_end"),
    )
    if (
        dataset_window != artifact_window
        or artifact_window != expected_window
    ):
        raise HorizonModelError("artifact training window differs")
    frame_hash = _digest(
        manifest.get("dataset_frame_hash"),
        "dataset_manifest.dataset_frame_hash",
    )
    if document.get("dataset_hash") != _dataset_identity_hash(
        frame_hash=frame_hash,
        spec=spec,
        training_window=artifact_window,
    ):
        raise HorizonModelError("dataset training window identity differs")
    manifest_hash = _digest(manifest.get("manifest_hash"), "dataset manifest_hash")
    if canonical_hash(_without_hash(manifest, "manifest_hash")) != manifest_hash:
        raise HorizonModelError("dataset manifest hash differs")
    _digest(document.get("dataset_hash"), "dataset_hash")
    model = document.get("final_model")
    if model is not None:
        _verify_model(model, spec)
    walk_forward = document.get("walk_forward") or {}
    if walk_forward.get("protocol") != "EXPANDING_SESSION_SPLIT_PURGED_BY_LABEL_MATURITY_V2":
        raise HorizonModelError("walk-forward protocol differs")
    folds = list(walk_forward.get("folds") or ())
    if int(walk_forward.get("oos_sample_count") or 0) != sum(
        int(fold.get("validation_sample_count") or 0) for fold in folds
    ):
        raise HorizonModelError("walk-forward OOS sample count differs")
    if int(walk_forward.get("distinct_oos_sessions") or 0) != sum(
        int(fold.get("distinct_validation_sessions") or 0) for fold in folds
    ):
        raise HorizonModelError("walk-forward OOS session count differs")
    previous_validation_end: date | None = None
    previous_calibration_count = 0
    previous_calibration_latest: date | None = None
    cumulative_prior_validation_samples = 0
    for expected_fold_number, fold in enumerate(folds, 1):
        _verify_hashed_mapping(fold, "fold_hash")
        if int(fold.get("fold_number") or 0) != expected_fold_number:
            raise HorizonModelError("walk-forward fold sequence differs")
        latest_label = _date_value(fold["latest_training_label_maturity"], "latest_training_label_maturity")
        validation_start = _date_value(fold["validation_start"], "validation_start")
        validation_end = _date_value(fold["validation_end"], "validation_end")
        if latest_label >= validation_start:
            raise HorizonModelError("walk-forward fold leaks an immature label")
        calibration_count = int(
            fold.get("calibration_training_sample_count") or 0
        )
        calibration_latest = fold.get("latest_calibration_label_maturity")
        _digest(
            fold.get("calibration_training_hash"),
            "fold.calibration_training_hash",
        )
        if (
            calibration_count < previous_calibration_count
            or calibration_count > cumulative_prior_validation_samples
        ):
            raise HorizonModelError("fold calibration sample clock differs")
        if calibration_count == 0:
            if calibration_latest is not None:
                raise HorizonModelError("empty fold calibration has a maturity clock")
            if fold.get("calibration_training_hash") != canonical_hash([]):
                raise HorizonModelError("empty fold calibration hash differs")
            if fold.get("prequential_calibration") is not None:
                raise HorizonModelError("empty fold has a calibration model")
        else:
            calibration_latest_date = (
                _date_value(
                    calibration_latest,
                    "latest_calibration_label_maturity",
                )
                if calibration_latest is not None
                else None
            )
            if calibration_latest_date is None or calibration_latest_date >= validation_start:
                raise HorizonModelError(
                    "walk-forward calibration leaks an immature label"
                )
            if (
                previous_calibration_latest is not None
                and calibration_latest_date < previous_calibration_latest
            ):
                raise HorizonModelError("fold calibration maturity clock regresses")
            previous_calibration_latest = calibration_latest_date
            fold_calibration = fold.get("prequential_calibration")
            if not isinstance(fold_calibration, Mapping):
                raise HorizonModelError("fold calibration model is missing")
            _verify_calibration(fold_calibration)
            if int(fold_calibration.get("sample_count") or 0) != calibration_count:
                raise HorizonModelError("fold calibration sample count differs")
        if fold.get("score_normalization_protocol") != SCORE_NORMALIZATION_PROTOCOL:
            raise HorizonModelError("fold score normalization differs")
        if (
            fold.get(
                "validation_metrics_are_unconditional_diagnostics_only"
            )
            is not True
        ):
            raise HorizonModelError("fold diagnostics were promoted to gate evidence")
        _finite(fold.get("training_score_mean"), "fold.training_score_mean")
        if _finite(
            fold.get("training_score_std"), "fold.training_score_std"
        ) <= 0:
            raise HorizonModelError("fold training score scale is invalid")
        if previous_validation_end is not None and validation_start <= previous_validation_end:
            raise HorizonModelError("walk-forward validation folds overlap")
        if validation_end < validation_start:
            raise HorizonModelError("walk-forward validation interval is inverted")
        previous_validation_end = validation_end
        previous_calibration_count = calibration_count
        cumulative_prior_validation_samples += int(
            fold.get("validation_sample_count") or 0
        )
    calibration = document.get("calibration")
    if calibration is not None:
        _verify_calibration(calibration)
    evidence = document.get("oos_evidence") or {}
    _verify_hashed_mapping(evidence, "evidence_hash")
    if evidence.get("evidence_hash") != document.get("oos_evidence_hash"):
        raise HorizonModelError("OOS evidence hashes disagree")
    if (
        evidence.get("evidence_schema")
        != "probiga.trading-v3.horizon-oos-evidence.v3"
        or evidence.get("model_protocol") != MODEL_PROTOCOL
        or evidence.get("calibration_labels_purged_by_maturity") is not True
        or evidence.get("economic_metrics_use_frozen_selection_ledger") is not True
        or evidence.get(
            "candidate_economic_metrics_bound_to_stream_verifiable_ledger"
        ) is not True
    ):
        raise HorizonModelError("OOS evidence protocol differs")
    if (
        evidence.get("training_window") != artifact_window
        or evidence.get("training_window_status")
        != artifact_window.get("status")
        or evidence.get("training_window_is_current_config_default")
        is not artifact_window.get("is_current_config_default")
    ):
        raise HorizonModelError("OOS training window evidence differs")
    manifest_candidate_count = int(manifest.get("candidate_count") or 0)
    manifest_labeled_count = int(manifest.get("labeled_sample_count") or 0)
    matured_sample_count = int(evidence.get("matured_sample_count") or 0)
    expected_mature_count = int(
        evidence.get("expected_mature_candidate_count") or 0
    )
    if (
        expected_mature_count != manifest_candidate_count
        or matured_sample_count != manifest_labeled_count
        or manifest_labeled_count > manifest_candidate_count
    ):
        raise HorizonModelError("maturity evidence differs from dataset manifest")
    expected_coverage = (
        matured_sample_count / expected_mature_count
        if expected_mature_count else 0.0
    )
    if abs(
        expected_coverage
        - _finite(evidence.get("maturity_coverage"), "maturity_coverage")
    ) > 1e-9:
        raise HorizonModelError("maturity coverage aggregate differs")
    qmt_count = int(evidence.get("qmt_attested_label_count") or 0)
    if qmt_count != int(manifest.get("qmt_attested_label_count") or 0):
        raise HorizonModelError("QMT label evidence differs from dataset manifest")
    expected_qmt_coverage = (
        qmt_count / matured_sample_count if matured_sample_count else 0.0
    )
    if abs(
        expected_qmt_coverage
        - _finite(
            evidence.get("qmt_attested_label_coverage"),
            "qmt_attested_label_coverage",
        )
    ) > 1e-9:
        raise HorizonModelError("QMT label coverage aggregate differs")
    if int(evidence.get("calibration_evaluation_sample_count") or 0) > int(
        evidence.get("oos_sample_count") or 0
    ) or int(
        evidence.get("distinct_calibration_evaluation_sessions") or 0
    ) > int(evidence.get("distinct_oos_sessions") or 0):
        raise HorizonModelError("prequential calibration evidence exceeds OOS")
    oos_sample_count = int(evidence.get("oos_sample_count") or 0)
    if (calibration is None) != (oos_sample_count == 0):
        raise HorizonModelError("final calibration availability differs from OOS")
    if calibration is not None:
        if model is None:
            raise HorizonModelError("calibration exists without a final model")
        if int(calibration.get("sample_count") or 0) != oos_sample_count:
            raise HorizonModelError("calibration/OOS sample count differs")
        if int(calibration.get("distinct_oos_sessions") or 0) != int(
            evidence.get("distinct_oos_sessions") or 0
        ):
            raise HorizonModelError("calibration/OOS session count differs")
    verified_brier, verified_calibration_mae = (
        _verify_calibration_metric_evidence(
            evidence.get("calibration_metric_evidence") or {}
        )
    )
    if (
        abs(
            verified_brier
            - _finite(evidence.get("brier_score"), "brier_score")
        )
        > 1e-9
        or abs(
            verified_calibration_mae
            - _finite(evidence.get("calibration_mae"), "calibration_mae")
        )
        > 1e-9
    ):
        raise HorizonModelError("OOS calibration metric differs")
    calibration_metric_document = evidence.get(
        "calibration_metric_evidence"
    ) or {}
    if int(calibration_metric_document.get("sample_count") or 0) != int(
        evidence.get("calibration_evaluation_sample_count") or 0
    ) or int(
        calibration_metric_document.get("distinct_session_count") or 0
    ) != int(
        evidence.get("distinct_calibration_evaluation_sessions") or 0
    ):
        raise HorizonModelError("calibration metric/prequential count differs")
    direction_gate, direction_expected, direction_probability = (
        _verify_direction_evidence(evidence.get("direction_evidence") or {})
    )
    for actual, field in (
        (direction_gate, "direction_rank_correlation"),
        (direction_expected, "expected_return_direction_rank_correlation"),
        (direction_probability, "probability_direction_rank_correlation"),
    ):
        if abs(actual - _finite(evidence.get(field), field)) > 1e-9:
            raise HorizonModelError("OOS direction metric differs")
    verified_psi = _verify_population_stability_evidence(
        evidence.get("population_stability_evidence") or {}
    )
    if abs(
        verified_psi
        - _finite(
            evidence.get("population_stability_index"),
            "population_stability_index",
        )
    ) > 1e-9:
        raise HorizonModelError("PSI evidence/index differs")
    psi_document = evidence.get("population_stability_evidence") or {}
    if len(folds) >= 2:
        if (
            int(psi_document.get("reference_fold") or 0)
            != int(folds[0].get("fold_number") or 0)
            or int(psi_document.get("reference_sample_count") or 0)
            + int(psi_document.get("observed_sample_count") or 0)
            != int(evidence.get("oos_sample_count") or 0)
        ):
            raise HorizonModelError("PSI fold population differs from OOS")
    execution = document.get("execution_feasibility") or {}
    _verify_hashed_mapping(execution, "attestation_hash")
    if not all(
        execution.get(field) is True
        for field in (
            "exact_exchange_session_offsets",
            "outcomes_include_roundtrip_costs",
            "corporate_actions_quarantined",
            "zero_volume_entry_exit_quarantined",
        )
    ) or execution.get("same_close_entry_allowed") is not False or execution.get("t0_exit_allowed") is not False:
        raise HorizonModelError("execution feasibility boundary differs")
    if (
        execution.get("status") != "RESEARCH_LABEL_PROTOCOL_VERIFIED"
        or execution.get("provenance") != "SELF_VERIFIED_RESEARCH_ARTIFACT"
        or execution.get("execution_evidence_scope")
        != "LONG_HISTORY_OOS_RESEARCH_ONLY"
        or execution.get("executable_verified") is not False
        or execution.get("label_attestation_required_for_execution") is not True
    ):
        raise HorizonModelError("research evidence was misrepresented as executable")
    frozen_policy = document.get("training_policy") or {}
    _verify_hashed_mapping(frozen_policy, "policy_hash")
    policy = HorizonTrainingPolicy.from_dict(_without_hash(frozen_policy, "policy_hash"))
    if (
        policy.training_window_protocol != artifact_window.get("protocol")
        or policy.history_start.isoformat()
        != artifact_window.get("configured_history_start")
    ):
        raise HorizonModelError("training policy/window differs")
    frozen_selection = document.get("selection_policy") or {}
    _verify_hashed_mapping(frozen_selection, "selection_policy_hash")
    selection_policy = HorizonSelectionPolicy.from_dict(
        _without_hash(frozen_selection, "selection_policy_hash")
    )
    selection_policy_is_default = (
        frozen_selection.get("selection_policy_hash")
        == DEFAULT_SELECTION_POLICY_HASH
    )
    if evidence.get("selection_policy_is_production_default") is not (
        selection_policy_is_default
    ):
        raise HorizonModelError("selection policy production status differs")
    direction_document = evidence.get("direction_evidence") or {}
    if int(direction_document.get("minimum_cross_section_size") or 0) != (
        selection_policy.minimum_cross_section_size
    ):
        raise HorizonModelError("direction/selection cross-section differs")
    fold_intervals = {
        int(fold["fold_number"]): (
            _date_value(fold["validation_start"], "validation_start"),
            _date_value(fold["validation_end"], "validation_end"),
        )
        for fold in folds
    }
    direction_session_keys: set[tuple[int, str]] = set()
    for item in direction_document.get("sessions") or ():
        fold_number = int(item["fold_number"])
        session_text = str(item["decision_session_date"])
        session_date = _date_value(
            session_text, "direction.decision_session_date"
        )
        interval = fold_intervals.get(fold_number)
        if interval is None or not interval[0] <= session_date <= interval[1]:
            raise HorizonModelError("direction session lies outside its fold")
        direction_session_keys.add((fold_number, session_text))
    calibration_metric_session_counts = {
        (
            int(item["fold_number"]),
            str(item["decision_session_date"]),
        ): int(item["sample_count"])
        for item in calibration_metric_document.get("sessions") or ()
    }
    direction_session_counts = {
        (
            int(item["fold_number"]),
            str(item["decision_session_date"]),
        ): int(item["sample_count"])
        for item in direction_document.get("sessions") or ()
    }
    if calibration_metric_session_counts != direction_session_counts:
        raise HorizonModelError("calibration/direction session evidence differs")
    expected_calibration_protocol_hash = canonical_hash({
        "protocol": CALIBRATION_PROTOCOL,
        "bucket_count": policy.calibration_bucket_count,
        "evaluation": "MATURITY_PURGED_PREQUENTIAL_PRIOR_FOLDS_ONLY",
        "score_normalization": SCORE_NORMALIZATION_PROTOCOL,
    })
    if (
        evidence.get("calibration_protocol_hash")
        != expected_calibration_protocol_hash
        or document.get("calibration_protocol_hash")
        != expected_calibration_protocol_hash
    ):
        raise HorizonModelError("calibration protocol hash differs")
    candidate_ledger_reference = dict(
        document.get("candidate_evaluation_ledger") or {}
    )
    _verify_candidate_ledger_reference(
        candidate_ledger_reference,
        document=document,
        spec=spec,
        selection_policy=selection_policy,
    )
    if (
        evidence.get("candidate_evaluation_ledger_reference_hash")
        != candidate_ledger_reference.get("reference_hash")
        or int(candidate_ledger_reference.get("row_count") or 0)
        != int(evidence.get("oos_sample_count") or 0)
        or int(candidate_ledger_reference.get("session_count") or 0)
        != int(evidence.get("distinct_oos_sessions") or 0)
        or int(candidate_ledger_reference.get("evaluation_row_count") or 0)
        != int(evidence.get("calibration_evaluation_sample_count") or 0)
        or int(
            candidate_ledger_reference.get("evaluation_session_count") or 0
        )
        != int(evidence.get("distinct_calibration_evaluation_sessions") or 0)
        or int(candidate_ledger_reference.get("fold_count") or 0)
        != len(folds)
    ):
        raise HorizonModelError("candidate ledger reference counts differ")
    if int(evidence.get("walk_forward_fold_count") or 0) != len(folds):
        raise HorizonModelError("OOS evidence fold count differs")
    if int(evidence.get("oos_sample_count") or 0) != int(
        walk_forward.get("oos_sample_count") or 0
    ):
        raise HorizonModelError("OOS evidence sample count differs")
    if int(evidence.get("distinct_oos_sessions") or 0) != int(
        walk_forward.get("distinct_oos_sessions") or 0
    ):
        raise HorizonModelError("OOS evidence session count differs")
    verified_selection = _verify_selection_evidence(
        evidence.get("selection_evidence") or {},
        policy=selection_policy,
        cost_assumption_pct=float(spec.cost_assumption_pct),
        candidate_ledger_reference=candidate_ledger_reference,
    )
    for field, actual in verified_selection.items():
        persisted = evidence.get(field)
        if isinstance(actual, int):
            if int(persisted or 0) != actual:
                raise HorizonModelError("OOS selected count differs")
        elif abs(float(persisted) - float(actual)) > 1e-9:
            raise HorizonModelError("OOS selected metric differs")
    for item in (
        (evidence.get("selection_evidence") or {}).get(
            "selection_frontier"
        )
        or ()
    ):
        key = (int(item["fold_number"]), str(item["decision_session_date"]))
        if key not in direction_session_keys:
            raise HorizonModelError("selection session lacks direction evidence")
    if int(
        (evidence.get("selection_evidence") or {}).get(
            "candidate_sample_count"
        )
        or 0
    ) != int(evidence.get("calibration_evaluation_sample_count") or 0):
        raise HorizonModelError("selection/prequential candidate count differs")
    if int(
        (evidence.get("direction_evidence") or {}).get("session_count") or 0
    ) != int(
        evidence.get("distinct_calibration_evaluation_sessions") or 0
    ):
        raise HorizonModelError("direction/prequential session count differs")
    psi = evidence.get("population_stability_evidence") or {}
    if psi.get("status") == "CALCULATED":
        if not folds or psi.get("anchor_model_hash") != folds[0].get("model_hash"):
            raise HorizonModelError("PSI anchor is not the frozen first-fold model")
    status, reasons = _evaluate_gate(
        evidence,
        policy,
        horizon,
        final_model_available=model is not None and calibration is not None,
        selection_policy_is_production_default=selection_policy_is_default,
    )
    gate = document.get("gate") or {}
    if (
        gate.get("gate_scope") != selection_policy.candidate_domain
        or gate.get("deployment_gate") is not False
        or gate.get("training_window_status")
        != artifact_window.get("status")
        or gate.get("training_window_is_current_config_default")
        is not artifact_window.get("is_current_config_default")
        or gate.get("contract_eligibility_requires_stream_verified_ledger")
        is not True
    ):
        raise HorizonModelError("artifact gate scope was misrepresented")
    if gate.get("status") != status or list(gate.get("block_reasons") or ()) != reasons:
        raise HorizonModelError("persisted gate differs from recomputed evidence")
    eligible = status == "PASS"
    if bool(document.get("contract_eligible")) != eligible or bool(gate.get("contract_eligible")) != eligible:
        raise HorizonModelError("contract eligibility differs from recomputed gate")
    if (
        document.get("lifecycle") != "SHADOW_RESEARCH_ONLY"
        or document.get("mapping_verification_scope")
        != "SELF_CONSISTENT_ONLY_NOT_REGISTRATION_EVIDENCE"
        or document.get("candidate_ledger_registration_required") is not True
        or document.get("contract_eligibility_scope")
        != CONTRACT_ELIGIBILITY_SCOPE
        or document.get("paper_eligible") is not False
        or document.get("production_eligible") is not False
        or gate.get("contract_eligibility_scope")
        != CONTRACT_ELIGIBILITY_SCOPE
        or gate.get("paper_eligible") is not False
        or gate.get("production_eligible") is not False
    ):
        raise HorizonModelError("contract eligibility exceeded Shadow scope")
    if gate.get("automatic_promotion_allowed") is not False or gate.get("external_signed_attestation_required") is not True or gate.get("order_authority") is not False:
        raise HorizonModelError("artifact promotion boundary differs")
    if require_current_config:
        _verify_current_config_model_contract(
            spec,
            training_policy=policy,
            training_window=artifact_window,
        )
        if document.get("config_hash") != current_config_hash():
            raise HorizonModelError("artifact config_hash is not current")
    _digest(document.get("config_hash"), "config_hash")
    source_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if require_current_code:
        if document.get("code_hash") != source_hash:
            raise HorizonModelError("artifact code_hash is not current")
        runtime_version, runtime_kind = code_version()
        if (
            document.get("code_version") != runtime_version
            or document.get("code_version_kind") != runtime_kind
        ):
            raise HorizonModelError("artifact code_version is not current")
    _digest(document.get("code_hash"), "code_hash")
    created_at = _aware_timestamp(document.get("created_at"), "created_at")
    training_cutoff = _date_value(document.get("training_cutoff"), "training_cutoff")
    valid_until = _date_value(document.get("valid_until"), "valid_until")
    if valid_until != training_cutoff + timedelta(days=30):
        raise HorizonModelError("artifact evidence validity window differs")
    if evidence.get("valid_until") != valid_until.isoformat():
        raise HorizonModelError("artifact/evidence valid_until differs")
    if evidence.get("training_cutoff") != training_cutoff.isoformat():
        raise HorizonModelError("artifact/evidence training_cutoff differs")
    created_market_date = created_at.tz_convert(EXCHANGE_TIMEZONE).date()
    if training_cutoff > created_market_date:
        raise HorizonModelError("training_cutoff follows artifact creation")
    manifest_last_maturity = _date_value(
        manifest.get("last_maturity_session"),
        "dataset_manifest.last_maturity_session",
    )
    manifest_last_decision = _date_value(
        manifest.get("last_decision_session"),
        "dataset_manifest.last_decision_session",
    )
    manifest_first_decision = _date_value(
        manifest.get("first_decision_session"),
        "dataset_manifest.first_decision_session",
    )
    signal_start = _date_value(
        artifact_window.get("signal_start"),
        "training_window.signal_start",
    )
    signal_end = (
        _date_value(
            artifact_window.get("signal_end"),
            "training_window.signal_end",
        )
        if artifact_window.get("signal_end") is not None
        else None
    )
    if signal_end is not None and signal_end > training_cutoff:
        raise HorizonModelError(
            "training_window.signal_end exceeds training_cutoff"
        )
    effective_signal_end = signal_end or training_cutoff
    if (
        manifest_last_decision > training_cutoff
        or manifest_last_maturity > training_cutoff
        or manifest_first_decision < signal_start
        or manifest_first_decision > manifest_last_decision
        or manifest_last_decision > effective_signal_end
    ):
        raise HorizonModelError("dataset clock exceeds frozen training window")
    if model is not None:
        model_training_start = _date_value(
            model.get("training_start"), "model.training_start"
        )
        model_training_end = _date_value(
            model.get("training_end"), "model.training_end"
        )
        latest_label_maturity = _date_value(
            model.get("latest_label_maturity"),
            "model.latest_label_maturity",
        )
        if not (
            signal_start
            <= model_training_start
            <= model_training_end
            <= effective_signal_end
            and latest_label_maturity <= training_cutoff
        ):
            raise HorizonModelError(
                "final model clock exceeds frozen training window"
            )
        if not (
            manifest_first_decision
            <= model_training_start
            <= model_training_end
            <= manifest_last_decision
            and latest_label_maturity <= manifest_last_maturity
        ):
            raise HorizonModelError(
                "final model clock exceeds dataset manifest"
            )
    for fold in folds:
        fold_training_start = _date_value(
            fold.get("training_start"), "fold.training_start"
        )
        fold_training_end = _date_value(
            fold.get("training_end"), "fold.training_end"
        )
        fold_validation_start = _date_value(
            fold.get("validation_start"), "fold.validation_start"
        )
        fold_validation_end = _date_value(
            fold.get("validation_end"), "fold.validation_end"
        )
        fold_latest_maturity = _date_value(
            fold.get("latest_training_label_maturity"),
            "fold.latest_training_label_maturity",
        )
        if (
            fold_training_start < signal_start
            or fold_validation_start < signal_start
            or fold_training_end > effective_signal_end
            or fold_latest_maturity > training_cutoff
            or fold_validation_end > effective_signal_end
        ):
            raise HorizonModelError(
                "walk-forward clock exceeds frozen training window"
            )
        if (
            fold_training_start < manifest_first_decision
            or fold_validation_start < manifest_first_decision
            or fold_training_end > manifest_last_decision
            or fold_validation_end > manifest_last_decision
            or fold_latest_maturity > manifest_last_maturity
            or (
                fold.get("latest_calibration_label_maturity") is not None
                and _date_value(
                    fold.get("latest_calibration_label_maturity"),
                    "fold.latest_calibration_label_maturity",
                )
                > manifest_last_maturity
            )
        ):
            raise HorizonModelError(
                "walk-forward clock exceeds dataset manifest"
            )
    return document


def load_horizon_artifact(
    path: str | Path,
    *,
    require_current_code: bool = True,
    require_current_config: bool = True,
) -> dict[str, Any]:
    artifact_path = Path(path)
    try:
        document = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HorizonModelError(f"cannot load horizon artifact: {artifact_path}") from exc
    if not isinstance(document, dict):
        raise HorizonModelError("horizon artifact root must be an object")
    verified = verify_horizon_artifact(
        document,
        require_current_code=require_current_code,
        require_current_config=require_current_config,
    )
    verify_candidate_evaluation_ledger(
        verified,
        artifact_path.parent,
        require_current_code=require_current_code,
        require_current_config=require_current_config,
    )
    return verified


def verify_candidate_evaluation_ledger(
    artifact: Mapping[str, Any],
    artifact_root: str | Path,
    *,
    require_current_code: bool = True,
    require_current_config: bool = True,
) -> dict[str, Any]:
    """Stream-verify the content-addressed full prequential OOS sidecar.

    The returned registration evidence is the only model-layer proof suitable
    for a PROCESS_VERIFIED registry request.  Mapping-only artifact validation
    remains self-consistency checking and does not grant registration status.
    """

    document = verify_horizon_artifact(
        artifact,
        require_current_code=require_current_code,
        require_current_config=require_current_config,
    )
    horizon = int(document["horizon_days"])
    spec = HORIZON_MODEL_SPECS[horizon]
    frozen_selection = document["selection_policy"]
    policy = HorizonSelectionPolicy.from_dict(
        _without_hash(frozen_selection, "selection_policy_hash")
    )
    reference = document["candidate_evaluation_ledger"]
    expected_header = _verify_candidate_ledger_reference(
        reference,
        document=document,
        spec=spec,
        selection_policy=policy,
    )
    path = _resolve_candidate_ledger_path(artifact_root, reference)
    return _stream_verify_candidate_ledger(
        document,
        path,
        expected_header=expected_header,
        policy=policy,
        spec=spec,
    )


def _materialize_ephemeral_candidate_ledger(
    document: Mapping[str, Any],
    artifact_root: Path,
) -> Path:
    reference = document["candidate_evaluation_ledger"]
    relative = PurePosixPath(str(reference["relative_path"]))
    destination = artifact_root.joinpath(*relative.parts)
    if destination.exists():
        return destination
    payload = _EPHEMERAL_CANDIDATE_LEDGER_BYTES.get(
        str(reference["content_sha256"])
    )
    if payload is None:
        raise HorizonModelError(
            "candidate evaluation ledger is missing from approved artifact root"
        )
    if hashlib.sha256(payload).hexdigest() != reference["content_sha256"]:
        raise HorizonModelError("ephemeral candidate ledger hash differs")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("xb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
    except FileExistsError:
        pass
    return destination


def write_horizon_artifact(
    artifact: Mapping[str, Any],
    path: str | Path,
    *,
    require_current_config: bool = True,
) -> Path:
    document = verify_horizon_artifact(
        artifact,
        require_current_config=require_current_config,
    )
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    _materialize_ephemeral_candidate_ledger(document, output.parent)
    verify_candidate_evaluation_ledger(
        document,
        output.parent,
        require_current_config=require_current_config,
    )
    content = json.dumps(document, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2) + "\n"
    if output.exists():
        if output.read_text(encoding="utf-8") != content:
            raise FileExistsError(f"immutable horizon artifact already exists: {output}")
        return output
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(content)
    return output


def predict_horizon_artifact(
    artifact: Mapping[str, Any],
    features: Mapping[str, Any] | pd.DataFrame,
) -> HorizonPrediction | list[HorizonPrediction]:
    document = verify_horizon_artifact(artifact)
    model = document.get("final_model")
    calibration = document.get("calibration")
    if model is None or calibration is None:
        raise HorizonModelError("artifact has no fitted model/calibration")
    frame = (
        features.copy()
        if isinstance(features, pd.DataFrame)
        else pd.DataFrame([dict(features)])
    )
    if frame.empty:
        raise HorizonModelError("prediction features must not be empty")
    raw, normalized_score = _normalized_model_score(model, frame)
    expected, probability = _apply_calibration(calibration, normalized_score)
    predictions = [
        HorizonPrediction(
            model_key=str(document["model_key"]),
            model_version=str(document["model_version"]),
            horizon_days=int(document["horizon_days"]),
            prediction_kind=str(document["prediction_kind"]),
            raw_expected_return_net_pct=_stable_float(raw[index]),
            expected_return_net_pct=_stable_float(expected[index]),
            probability_positive=_stable_float(probability[index]),
            score=_stable_float(probability[index]),
            model_artifact_hash=str(document["artifact_hash"]),
            feature_protocol_hash=str(document["feature_protocol_hash"]),
            calibration_evidence_hash=str(document["oos_evidence_hash"]),
            contract_eligible=bool(document["contract_eligible"]),
        )
        for index in range(len(frame))
    ]
    return predictions if isinstance(features, pd.DataFrame) else predictions[0]


def artifact_manifest(
    artifact: Mapping[str, Any],
    *,
    require_current_config: bool = True,
) -> dict[str, Any]:
    document = verify_horizon_artifact(
        artifact,
        require_current_config=require_current_config,
    )
    evidence = document["oos_evidence"]
    execution = document["execution_feasibility"]
    return {
        "schema_version": document["schema_version"],
        "release_id": document["release_id"],
        "suite_release_id": document["suite_release_id"],
        "model_key": document["model_key"],
        "model_version": document["model_version"],
        "model_protocol": document["model_protocol"],
        "horizon_days": document["horizon_days"],
        "prediction_kind": document["prediction_kind"],
        "artifact_hash": document["artifact_hash"],
        "config_hash": document["config_hash"],
        "code_version": document["code_version"],
        "code_version_kind": document["code_version_kind"],
        "code_hash": document["code_hash"],
        "feature_protocol_hash": document["feature_protocol_hash"],
        "calibration_protocol_hash": document["calibration_protocol_hash"],
        "candidate_evaluation_ledger": copy.deepcopy(
            document["candidate_evaluation_ledger"]
        ),
        "mapping_verification_scope": document[
            "mapping_verification_scope"
        ],
        "candidate_ledger_registration_required": True,
        "selection_policy_hash": document["selection_policy"][
            "selection_policy_hash"
        ],
        "selection_policy_is_production_default": evidence[
            "selection_policy_is_production_default"
        ],
        "economic_evaluation_scope": evidence["selection_evidence"][
            "economic_evaluation_scope"
        ],
        "dataset_hash": document["dataset_hash"],
        "training_window": copy.deepcopy(document["training_window"]),
        "training_window_status": document["training_window"]["status"],
        "training_window_is_current_config_default": document[
            "training_window"
        ]["is_current_config_default"],
        "training_cutoff": document["training_cutoff"],
        "valid_until": document["valid_until"],
        "created_at": document["created_at"],
        "oos_evidence_hash": document["oos_evidence_hash"],
        "execution_feasibility_attestation_hash": execution["attestation_hash"],
        "distinct_train_sessions": evidence["distinct_train_sessions"],
        "distinct_oos_sessions": evidence["distinct_oos_sessions"],
        "maturity_coverage": evidence["maturity_coverage"],
        "population_stability_index": evidence["population_stability_index"],
        "execution_evidence_scope": execution["execution_evidence_scope"],
        "executable_verified": False,
        "gate_status": document["gate"]["status"],
        "gate_scope": document["gate"]["gate_scope"],
        "deployment_gate": False,
        "block_reasons": list(document["gate"]["block_reasons"]),
        "contract_eligible": document["contract_eligible"],
        "contract_eligibility_requires_stream_verified_ledger": True,
        "contract_eligibility_scope": document[
            "contract_eligibility_scope"
        ],
        "paper_eligible": False,
        "production_eligible": False,
        "order_authority": False,
        "external_signed_attestation_required": True,
    }


def train_horizon_suite(
    datasets: Mapping[int, HorizonDataset],
    *,
    release_id: str,
    training_cutoff: date | datetime | str,
    policy: HorizonTrainingPolicy = DEFAULT_TRAINING_POLICY,
    selection_policy: HorizonSelectionPolicy = DEFAULT_SELECTION_POLICY,
    created_at: datetime | str | None = None,
    config_sha256: str | None = None,
    candidate_ledger_root: str | Path | None = None,
) -> dict[int, dict[str, Any]]:
    if set(datasets) != set(SUPPORTED_HORIZONS):
        raise HorizonModelError("suite requires exactly T+1/T+5/T+20 datasets")
    artifacts = {
        horizon: train_independent_horizon_model(
            datasets[horizon],
            release_id=horizon_governance_release_id(
                suite_release_id=release_id,
                model_key=HORIZON_MODEL_SPECS[horizon].model_key,
                model_version=HORIZON_MODEL_SPECS[horizon].model_version,
                horizon_days=horizon,
            ),
            suite_release_id=release_id,
            training_cutoff=training_cutoff,
            policy=policy,
            selection_policy=selection_policy,
            created_at=created_at,
            config_sha256=config_sha256,
            candidate_ledger_root=candidate_ledger_root,
        )
        for horizon in SUPPORTED_HORIZONS
    }
    for field in ("model_key", "feature_protocol_hash", "dataset_hash"):
        values = [artifacts[horizon][field] for horizon in SUPPORTED_HORIZONS]
        if len(values) != len(set(values)):
            raise HorizonModelError(f"horizon suite is not independent by {field}")
    if len({
        artifacts[horizon]["selection_policy"]["selection_policy_hash"]
        for horizon in SUPPORTED_HORIZONS
    }) != 1:
        raise HorizonModelError("horizon suite selection policies differ")
    if len({
        artifacts[horizon]["training_window"]["training_window_hash"]
        for horizon in SUPPORTED_HORIZONS
    }) != 1:
        raise HorizonModelError("horizon suite training windows differ")
    model_hashes = [
        artifacts[horizon]["final_model"]["model_hash"]
        if artifacts[horizon]["final_model"] is not None else f"BLOCKED_T{horizon}"
        for horizon in SUPPORTED_HORIZONS
    ]
    if len(model_hashes) != len(set(model_hashes)):
        raise HorizonModelError("horizon suite does not contain independent fitted models")
    return artifacts


def write_horizon_suite(
    artifacts: Mapping[int, Mapping[str, Any]],
    root: str | Path,
    *,
    require_current_config: bool = True,
) -> dict[str, Any]:
    if set(artifacts) != set(SUPPORTED_HORIZONS):
        raise HorizonModelError("suite requires exactly T+1/T+5/T+20 artifacts")
    output_root = Path(root)
    manifests: list[dict[str, Any]] = []
    for horizon in SUPPORTED_HORIZONS:
        manifest = artifact_manifest(
            artifacts[horizon],
            require_current_config=require_current_config,
        )
        manifest["relative_path"] = f"T{horizon}.json"
        manifests.append(manifest)
    if len({item["suite_release_id"] for item in manifests}) != 1:
        raise HorizonModelError("model artifacts disagree on suite_release_id")
    all_pass = all(item["gate_status"] == "PASS" for item in manifests)
    body = {
        "schema_version": SUITE_SCHEMA,
        "model_protocol": MODEL_PROTOCOL,
        "candidate_ledger_schema": CANDIDATE_EVALUATION_LEDGER_SCHEMA,
        "candidate_ledger_binding_protocol": CANDIDATE_LEDGER_BINDING_PROTOCOL,
        "candidate_ledger_registration_required": True,
        "training_window_protocol": TRAINING_WINDOW_PROTOCOL,
        "training_window_is_current_config_default": all(
            item["training_window_is_current_config_default"] is True
            for item in manifests
        ),
        "release_id": manifests[0]["suite_release_id"],
        "suite_release_id": manifests[0]["suite_release_id"],
        "status": "PASS" if all_pass else "BLOCK",
        "created_at": manifests[0]["created_at"],
        "required_horizons": list(SUPPORTED_HORIZONS),
        "model_count": len(manifests),
        "independence_checks": {
            "distinct_model_keys": len({item["model_key"] for item in manifests}) == 3,
            "distinct_feature_protocol_hashes": len({item["feature_protocol_hash"] for item in manifests}) == 3,
            "distinct_dataset_hashes": len({item["dataset_hash"] for item in manifests}) == 3,
            "uniform_selection_policy_hash": len({
                item["selection_policy_hash"] for item in manifests
            }) == 1,
            "uniform_training_window_hash": len({
                item["training_window"]["training_window_hash"]
                for item in manifests
            }) == 1,
            "uniform_config_hash": len({
                item["config_hash"] for item in manifests
            }) == 1,
            "uniform_code_version": len({
                item["code_version"] for item in manifests
            }) == 1,
            "uniform_code_hash": len({
                item["code_hash"] for item in manifests
            }) == 1,
            "uniform_training_cutoff": len({
                item["training_cutoff"] for item in manifests
            }) == 1,
        },
        "models": manifests,
        "block_reasons_by_horizon": {
            str(item["horizon_days"]): list(item["block_reasons"])
            for item in manifests if item["block_reasons"]
        },
        "all_model_gates_pass": all_pass,
        "automatic_promotion_allowed": False,
        "order_authority": False,
    }
    suite = {**body, "suite_hash": canonical_hash(body)}
    content = json.dumps(suite, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2) + "\n"
    suite_path = output_root / "suite.json"
    if suite_path.exists() and suite_path.read_text(encoding="utf-8") != content:
        raise FileExistsError(f"immutable horizon suite already exists: {suite_path}")
    # Validate cross-model identity before materializing any immutable T*.json
    # file.  A mixed config/code/cutoff suite must fail without leaving a
    # permanently partial release directory behind.
    verify_horizon_suite(
        suite,
        artifact_root=None,
        require_current_config=require_current_config,
    )
    for horizon in SUPPORTED_HORIZONS:
        write_horizon_artifact(
            artifacts[horizon],
            output_root / f"T{horizon}.json",
            require_current_config=require_current_config,
        )
    if not suite_path.exists():
        with suite_path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
    verify_horizon_suite(
        suite,
        artifact_root=output_root,
        require_current_config=require_current_config,
    )
    return suite


def verify_horizon_suite(
    suite: Mapping[str, Any],
    *,
    artifact_root: str | Path | None = None,
    require_current_code: bool = True,
    require_current_config: bool = True,
) -> dict[str, Any]:
    document = copy.deepcopy(dict(suite))
    if document.get("schema_version") != SUITE_SCHEMA:
        raise HorizonModelError("horizon suite schema is unsupported")
    if document.get("model_protocol") != MODEL_PROTOCOL:
        raise HorizonModelError("horizon suite model protocol differs")
    if (
        document.get("candidate_ledger_schema")
        != CANDIDATE_EVALUATION_LEDGER_SCHEMA
        or document.get("candidate_ledger_binding_protocol")
        != CANDIDATE_LEDGER_BINDING_PROTOCOL
        or document.get("candidate_ledger_registration_required") is not True
    ):
        raise HorizonModelError("horizon suite candidate ledger protocol differs")
    if (
        document.get("training_window_protocol")
        != TRAINING_WINDOW_PROTOCOL
        or not isinstance(
            document.get("training_window_is_current_config_default"),
            bool,
        )
    ):
        raise HorizonModelError("horizon suite training window protocol differs")
    suite_hash = _digest(document.get("suite_hash"), "suite_hash")
    if canonical_hash(_without_hash(document, "suite_hash")) != suite_hash:
        raise HorizonModelError("horizon suite hash differs")
    suite_release_id = str(document.get("suite_release_id") or "").strip()
    if not suite_release_id or document.get("release_id") != suite_release_id:
        raise HorizonModelError("suite release identifiers differ")
    if document.get("required_horizons") != list(SUPPORTED_HORIZONS):
        raise HorizonModelError("suite required horizons differ")
    models = list(document.get("models") or ())
    if len(models) != 3 or int(document.get("model_count") or 0) != 3:
        raise HorizonModelError("suite must contain exactly three models")
    by_horizon = {int(item.get("horizon_days") or 0): item for item in models}
    if set(by_horizon) != set(SUPPORTED_HORIZONS):
        raise HorizonModelError("suite model horizons differ")
    for horizon, manifest in by_horizon.items():
        spec = HORIZON_MODEL_SPECS[horizon]
        expected_release = horizon_governance_release_id(
            suite_release_id=suite_release_id,
            model_key=spec.model_key,
            model_version=spec.model_version,
            horizon_days=horizon,
        )
        manifest_window = dict(manifest.get("training_window") or {})
        _verify_hashed_mapping(manifest_window, "training_window_hash")
        expected_manifest_window = _training_window_document(
            configured_history_start=manifest_window.get(
                "configured_history_start"
            ),
            signal_start=manifest_window.get("signal_start"),
            signal_end=manifest_window.get("signal_end"),
        )
        if manifest_window != expected_manifest_window:
            raise HorizonModelError(
                "suite model training window fields differ"
            )
        manifest_cutoff = _date_value(
            manifest.get("training_cutoff"),
            "suite model training_cutoff",
        )
        manifest_signal_end = manifest_window.get("signal_end")
        if (
            manifest_signal_end is not None
            and _date_value(
                manifest_signal_end,
                "suite model training_window.signal_end",
            )
            > manifest_cutoff
        ):
            raise HorizonModelError(
                "suite model training window exceeds training_cutoff"
            )
        manifest_window_default = (
            manifest_window.get("status")
            == "FROZEN_DEFAULT_TRAINING_WINDOW"
            and manifest_window.get("is_current_config_default") is True
        )
        if (
            manifest.get("release_id") != expected_release
            or manifest.get("suite_release_id") != suite_release_id
            or manifest.get("model_key") != spec.model_key
            or manifest.get("model_version") != spec.model_version
            or manifest.get("relative_path") != f"T{horizon}.json"
            or manifest.get("model_protocol") != MODEL_PROTOCOL
            or not isinstance(
                manifest.get("selection_policy_is_production_default"), bool
            )
            or (
                manifest.get("selection_policy_is_production_default") is True
                and manifest.get("selection_policy_hash")
                != DEFAULT_SELECTION_POLICY_HASH
            )
            or manifest.get("economic_evaluation_scope")
            != DEFAULT_SELECTION_POLICY.candidate_domain
            or manifest.get("gate_scope")
            != DEFAULT_SELECTION_POLICY.candidate_domain
            or manifest.get("deployment_gate") is not False
            or manifest.get("contract_eligibility_scope")
            != CONTRACT_ELIGIBILITY_SCOPE
            or manifest.get("paper_eligible") is not False
            or manifest.get("production_eligible") is not False
            or manifest.get("mapping_verification_scope")
            != "SELF_CONSISTENT_ONLY_NOT_REGISTRATION_EVIDENCE"
            or manifest.get("candidate_ledger_registration_required") is not True
            or manifest.get("training_window_status")
            != manifest_window.get("status")
            or manifest.get("training_window_is_current_config_default")
            is not manifest_window.get("is_current_config_default")
            or manifest.get(
                "contract_eligibility_requires_stream_verified_ledger"
            ) is not True
        ):
            raise HorizonModelError("suite model governance identity differs")
        if require_current_config:
            current_window = current_training_window_contract()
            if (
                not manifest_window_default
                or manifest_window.get("protocol")
                != current_window["training_window_protocol"]
                or manifest_window.get("configured_history_start")
                != current_window["history_start"]
                or manifest_window.get("signal_start")
                != current_window["history_start"]
            ):
                raise HorizonModelError(
                    "suite model training window is not current config"
                )
        _digest(manifest.get("artifact_hash"), "model artifact_hash")
        _digest(
            manifest.get("selection_policy_hash"),
            "model selection_policy_hash",
        )
        candidate_ledger_reference = dict(
            manifest.get("candidate_evaluation_ledger") or {}
        )
        _verify_hashed_mapping(
            candidate_ledger_reference,
            "reference_hash",
        )
        if (
            candidate_ledger_reference.get("schema_version")
            != CANDIDATE_EVALUATION_LEDGER_SCHEMA
        ):
            raise HorizonModelError("suite candidate ledger protocol differs")
        if (
            manifest.get("gate_status") == "PASS"
            and manifest.get("selection_policy_is_production_default")
            is not True
        ):
            raise HorizonModelError("suite PASS uses a non-production selection policy")
    checks = document.get("independence_checks") or {}
    recomputed_checks = {
        "distinct_model_keys": len({item["model_key"] for item in models}) == 3,
        "distinct_feature_protocol_hashes": len({item["feature_protocol_hash"] for item in models}) == 3,
        "distinct_dataset_hashes": len({item["dataset_hash"] for item in models}) == 3,
        "uniform_selection_policy_hash": len({
            item["selection_policy_hash"] for item in models
        }) == 1,
        "uniform_training_window_hash": len({
            item["training_window"]["training_window_hash"]
            for item in models
        }) == 1,
        "uniform_config_hash": len({
            item["config_hash"] for item in models
        }) == 1,
        "uniform_code_version": len({
            item["code_version"] for item in models
        }) == 1,
        "uniform_code_hash": len({
            item["code_hash"] for item in models
        }) == 1,
        "uniform_training_cutoff": len({
            item["training_cutoff"] for item in models
        }) == 1,
    }
    if checks != recomputed_checks or not all(recomputed_checks.values()):
        raise HorizonModelError("suite independence checks differ")
    all_pass = all(item.get("gate_status") == "PASS" for item in models)
    all_default_windows = all(
        item.get("training_window_is_current_config_default") is True
        for item in models
    )
    expected_status = "PASS" if all_pass else "BLOCK"
    if (
        document.get("status") != expected_status
        or bool(document.get("all_model_gates_pass")) != all_pass
        or document.get("training_window_is_current_config_default")
        is not all_default_windows
    ):
        raise HorizonModelError("suite status differs from model gates")
    blockers = {
        str(item["horizon_days"]): list(item.get("block_reasons") or ())
        for item in models if item.get("block_reasons")
    }
    if document.get("block_reasons_by_horizon") != blockers:
        raise HorizonModelError("suite blockers differ from model manifests")
    if (
        document.get("automatic_promotion_allowed") is not False
        or document.get("order_authority") is not False
    ):
        raise HorizonModelError("suite escaped the research-only boundary")
    if artifact_root is not None:
        root = Path(artifact_root)
        for horizon, manifest in by_horizon.items():
            artifact = load_horizon_artifact(
                root / f"T{horizon}.json",
                require_current_code=require_current_code,
                require_current_config=require_current_config,
            )
            if (
                artifact["artifact_hash"] != manifest["artifact_hash"]
                or artifact["suite_release_id"] != suite_release_id
                or artifact["candidate_evaluation_ledger"]
                != manifest["candidate_evaluation_ledger"]
                or artifact["training_window"]
                != manifest["training_window"]
            ):
                raise HorizonModelError("suite manifest differs from model artifact")
    return document


def load_horizon_suite(
    path: str | Path,
    *,
    require_current_code: bool = True,
    require_current_config: bool = True,
) -> dict[str, Any]:
    suite_path = Path(path)
    try:
        document = json.loads(suite_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HorizonModelError(f"cannot load horizon suite: {suite_path}") from exc
    if not isinstance(document, dict):
        raise HorizonModelError("horizon suite root must be an object")
    return verify_horizon_suite(
        document,
        artifact_root=suite_path.parent,
        require_current_code=require_current_code,
        require_current_config=require_current_config,
    )


__all__ = [
    "ARTIFACT_SCHEMA",
    "CALIBRATION_PROTOCOL",
    "CANDIDATE_EVALUATION_LEDGER_SCHEMA",
    "CANDIDATE_LEDGER_BINDING_PROTOCOL",
    "CANDIDATE_LEDGER_ENCODING",
    "CANDIDATE_LEDGER_REGISTRATION_PROTOCOL",
    "CONTRACT_ELIGIBILITY_SCOPE",
    "DEFAULT_SELECTION_POLICY",
    "DEFAULT_SELECTION_POLICY_HASH",
    "DEFAULT_TRAINING_POLICY",
    "HORIZON_MODEL_SPECS",
    "HISTORICAL_ARTIFACT_SCHEMA_V1",
    "HISTORICAL_ARTIFACT_SCHEMA_V2",
    "HISTORICAL_SUITE_SCHEMA_V1",
    "HISTORICAL_SUITE_SCHEMA_V2",
    "HorizonDataset",
    "HorizonModelError",
    "HorizonModelSpec",
    "HorizonPrediction",
    "HorizonSelectionPolicy",
    "HorizonTrainingPolicy",
    "MODEL_PROTOCOL",
    "PSI_PROTOCOL",
    "SCORE_NORMALIZATION_PROTOCOL",
    "SELECTION_PROTOCOL",
    "TRAINING_CONFIG_PROTOCOL",
    "TRAINING_WINDOW_PROTOCOL",
    "artifact_manifest",
    "build_horizon_dataset",
    "canonical_hash",
    "canonical_json",
    "current_training_window_contract",
    "horizon_governance_release_id",
    "load_horizon_artifact",
    "load_horizon_suite",
    "predict_horizon_artifact",
    "train_horizon_suite",
    "train_independent_horizon_model",
    "verify_horizon_artifact",
    "verify_candidate_evaluation_ledger",
    "verify_horizon_suite",
    "write_horizon_artifact",
    "write_horizon_suite",
]
