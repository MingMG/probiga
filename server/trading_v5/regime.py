"""Frozen, fail-closed market regime router owned by Trading V5."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

import pandas as pd


REGIME_ROUTER_VERSION = "v5:market-regime-softmax:v1"
REGIME_STATES = (
    "TREND_UP",
    "THEME_ROTATION",
    "RANGE",
    "PANIC_RECOVERY",
    "RISK_OFF",
)
REGIME_ROUTER_INPUTS = (
    "market_return_20d_pct",
    "market_breadth_pct",
    "breadth_change_5d_pct",
    "realized_volatility_20d_pct",
    "limit_down_ratio_pct",
)
REGIME_CONTEXT_COLUMNS = (
    "market_input_manifest_sha256",
    "market_constituent_sample_count",
)


@dataclass(frozen=True, slots=True)
class RegimeAssessment:
    probabilities: tuple[tuple[str, float], ...]
    dominant_state: str
    confidence: float
    signal_at: str
    feature_available_at: str
    source_manifest_sha256: str
    constituent_sample_count: int

    @property
    def router_version(self) -> str:
        return REGIME_ROUTER_VERSION

    @property
    def research_only(self) -> bool:
        return True

    @property
    def activation_eligible(self) -> bool:
        return False

    def __post_init__(self) -> None:
        if tuple(state for state, _ in self.probabilities) != REGIME_STATES:
            raise ValueError("regime probabilities must use the frozen state order")
        values = tuple(value for _, value in self.probabilities)
        if any(not math.isfinite(value) or value < 0.0 for value in values):
            raise ValueError("regime probabilities must be finite and non-negative")
        if not math.isclose(sum(values), 1.0, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("regime probabilities must sum to one")
        if self.dominant_state not in REGIME_STATES:
            raise ValueError("dominant_state is not a frozen V5 regime")
        if self.dominant_state != max(
            REGIME_STATES,
            key=lambda state: dict(self.probabilities)[state],
        ):
            raise ValueError("dominant_state differs from the probabilities")
        if not math.isfinite(self.confidence) or not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be finite and within 0..1")
        signal = _aware_timestamp(self.signal_at, "signal_at")
        available = _aware_timestamp(
            self.feature_available_at,
            "feature_available_at",
        )
        if available > signal:
            raise ValueError("regime features were unavailable at signal time")
        _sha256(self.source_manifest_sha256, "source_manifest_sha256")
        if (
            type(self.constituent_sample_count) is not int
            or self.constituent_sample_count < 100
        ):
            raise ValueError("constituent_sample_count must be at least 100")

    def as_dict(self) -> dict[str, Any]:
        return {
            "router_version": REGIME_ROUTER_VERSION,
            "probabilities": dict(self.probabilities),
            "dominant_state": self.dominant_state,
            "confidence": self.confidence,
            "signal_at": self.signal_at,
            "feature_available_at": self.feature_available_at,
            "source_manifest_sha256": self.source_manifest_sha256,
            "constituent_sample_count": self.constituent_sample_count,
            "research_only": True,
            "activation_eligible": False,
        }


def assess_regime(
    *,
    signal_at: Any,
    feature_available_at: Any,
    source_manifest_sha256: str,
    constituent_sample_count: int,
    market_return_20d_pct: float,
    market_breadth_pct: float,
    breadth_change_5d_pct: float,
    realized_volatility_20d_pct: float,
    limit_down_ratio_pct: float,
) -> RegimeAssessment:
    """Evaluate the frozen V5 router from explicit point-in-time inputs."""

    signal = _aware_timestamp(signal_at, "signal_at")
    available = _aware_timestamp(feature_available_at, "feature_available_at")
    if available > signal:
        raise ValueError("feature_available_at exceeds signal_at")
    manifest = _sha256(source_manifest_sha256, "source_manifest_sha256")
    if type(constituent_sample_count) is not int or constituent_sample_count < 100:
        raise ValueError("constituent_sample_count must be at least 100")
    ret20 = _bounded_float(
        market_return_20d_pct,
        "market_return_20d_pct",
        -80.0,
        200.0,
    )
    breadth = _bounded_float(
        market_breadth_pct,
        "market_breadth_pct",
        0.0,
        100.0,
    )
    breadth_delta = _bounded_float(
        breadth_change_5d_pct,
        "breadth_change_5d_pct",
        -100.0,
        100.0,
    )
    volatility = _bounded_float(
        realized_volatility_20d_pct,
        "realized_volatility_20d_pct",
        0.0,
        300.0,
    )
    limit_down = _bounded_float(
        limit_down_ratio_pct,
        "limit_down_ratio_pct",
        0.0,
        100.0,
    )
    logits = {
        "TREND_UP": (
            0.11 * ret20
            + 0.035 * (breadth - 50)
            + 0.025 * breadth_delta
            - 0.08 * volatility
            - 0.15 * limit_down
        ),
        "THEME_ROTATION": (
            0.055 * ret20
            + 0.015 * (breadth - 45)
            + 0.02 * breadth_delta
            - 0.05 * volatility
        ),
        "RANGE": (
            1.2
            - 0.10 * abs(ret20)
            - 0.025 * abs(breadth - 50)
            - 0.025 * volatility
        ),
        "PANIC_RECOVERY": (
            -0.04 * ret20
            + 0.055 * breadth_delta
            + 0.02 * (breadth - 40)
            + 0.02 * volatility
        ),
        "RISK_OFF": (
            -0.12 * ret20
            - 0.04 * (breadth - 50)
            + 0.10 * volatility
            + 0.22 * limit_down
        ),
    }
    probabilities = _softmax(logits)
    ordered = sorted(probabilities.values(), reverse=True)
    dominant = max(REGIME_STATES, key=lambda state: probabilities[state])
    return RegimeAssessment(
        probabilities=tuple(
            (state, round(probabilities[state], 12))
            for state in REGIME_STATES
        ),
        dominant_state=dominant,
        confidence=round(ordered[0] - ordered[1], 12),
        signal_at=_timestamp_text(signal),
        feature_available_at=_timestamp_text(available),
        source_manifest_sha256=manifest,
        constituent_sample_count=constituent_sample_count,
    )


def _softmax(values: dict[str, float]) -> dict[str, float]:
    top = max(values.values())
    exponentials = {
        key: math.exp(value - top) for key, value in values.items()
    }
    total = sum(exponentials.values())
    return {key: value / total for key, value in exponentials.items()}


def _bounded_float(value: Any, name: str, lower: float, upper: float) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    try:
        converted = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(converted) or not lower <= converted <= upper:
        raise ValueError(f"{name} must be finite and within {lower}..{upper}")
    return converted


def _aware_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a timestamp") from exc
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return timestamp.tz_convert("UTC")


def _timestamp_text(value: pd.Timestamp) -> str:
    return value.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def _sha256(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return value


__all__ = [
    "REGIME_CONTEXT_COLUMNS",
    "REGIME_ROUTER_INPUTS",
    "REGIME_ROUTER_VERSION",
    "REGIME_STATES",
    "RegimeAssessment",
    "assess_regime",
]
