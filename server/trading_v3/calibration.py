from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from statistics import mean, median
from typing import Any, Iterable


CALIBRATION_PROTOCOL = "QUANTILE_MONOTONIC_PAVA_TOLERANCE_0P25_V1"


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * q
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if not values or losses <= 0:
        return None if gains <= 0 else math.inf
    return gains / losses


def _payoff_ratio(values: list[float]) -> float | None:
    wins = [value for value in values if value > 0]
    losses = [-value for value in values if value < 0]
    if not wins or not losses:
        return None if not wins else math.inf
    return mean(wins) / mean(losses)


def _expected_return(rows: list[dict[str, float]]) -> float:
    return mean(item["net_return_pct"] for item in rows)


def _monotonic_score_groups(
    groups: list[list[dict[str, float]]],
    *,
    tolerance_pct: float = 0.25,
) -> list[list[dict[str, float]]]:
    """Pool adjacent score bins whose realized return ranks backwards.

    This is the pool-adjacent-violators algorithm applied to ordered score
    bins.  It does not change any return or profit threshold: it only replaces
    noisy, non-monotonic quantile boundaries with wider evidence buckets.  A
    fully inverted score collapses to one bucket and is still rejected by the
    production minimum-bucket/direction gates.
    """

    pooled = [list(group) for group in groups if group]
    index = 0
    while index < len(pooled) - 1:
        left = _expected_return(pooled[index])
        right = _expected_return(pooled[index + 1])
        if right + float(tolerance_pct) >= left:
            index += 1
            continue
        pooled[index : index + 2] = [
            pooled[index] + pooled[index + 1]
        ]
        index = max(0, index - 1)
    return pooled


@dataclass(frozen=True)
class CalibrationBucket:
    lower_score: float
    upper_score: float
    sample_count: int
    expected_return_net_pct: float
    q10_pct: float
    q50_pct: float
    q90_pct: float
    probability_positive: float
    expected_mae_pct: float
    expected_mfe_pct: float
    profit_factor: float | None
    payoff_ratio: float | None


@dataclass(frozen=True)
class CalibrationTable:
    strategy_key: str
    model_version: str
    dataset_hash: str
    buckets: tuple[CalibrationBucket, ...]

    @property
    def score_range(self) -> tuple[float, float] | None:
        if not self.buckets:
            return None
        return (
            min(item.lower_score for item in self.buckets),
            max(item.upper_score for item in self.buckets),
        )

    def contains_score(self, score: float) -> bool:
        score_range = self.score_range
        return bool(
            score_range is not None
            and score_range[0] <= float(score) <= score_range[1]
        )

    def bucket_for(self, score: float) -> CalibrationBucket | None:
        if not self.buckets or not self.contains_score(score):
            return None
        for bucket in self.buckets:
            if bucket.lower_score <= score <= bucket.upper_score:
                return bucket
        # Quantile buckets can have small gaps between adjacent observed
        # scores.  Interpolation inside the fitted global support is valid;
        # extrapolation beyond that support remains forbidden above.
        return min(
            self.buckets,
            key=lambda item: min(
                abs(score - item.lower_score),
                abs(score - item.upper_score),
            ),
        )

    def has_valid_score_direction(self) -> bool:
        """Reject a calibration whose score ranks risk backwards.

        A strategy score is defined as "higher is better".  Letting a lower
        score bucket trade while the highest score bucket loses money turns
        calibration noise into a large amount of churn.  A single bucket
        cannot prove ranking direction, but remains usable when it passes the
        normal profit gates; with multiple buckets the expected return must be
        non-decreasing within a small sampling tolerance and the top bucket
        must not be negative.
        """

        if len(self.buckets) <= 1:
            return False
        ordered = sorted(self.buckets, key=lambda item: item.lower_score)
        if ordered[-1].expected_return_net_pct <= 0:
            return False
        tolerance_pct = 0.25
        return all(
            right.expected_return_net_pct + tolerance_pct
            >= left.expected_return_net_pct
            for left, right in zip(ordered, ordered[1:])
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_key": self.strategy_key,
            "model_version": self.model_version,
            "dataset_hash": self.dataset_hash,
            "buckets": [asdict(item) for item in self.buckets],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "CalibrationTable":
        return cls(
            strategy_key=str(value["strategy_key"]),
            model_version=str(value["model_version"]),
            dataset_hash=str(value["dataset_hash"]),
            buckets=tuple(
                CalibrationBucket(**item)
                for item in value.get("buckets", [])
            ),
        )


def fit_calibration(
    strategy_key: str,
    samples: Iterable[dict[str, float]],
    *,
    model_version: str,
    bucket_count: int = 10,
) -> CalibrationTable:
    rows = sorted(
        (
            {
                "score": float(item["score"]),
                "net_return_pct": float(item["net_return_pct"]),
                "mae_pct": float(item.get("mae_pct", 0.0)),
                "mfe_pct": float(item.get("mfe_pct", 0.0)),
            }
            for item in samples
            if all(
                math.isfinite(float(item[key]))
                for key in ("score", "net_return_pct")
            )
        ),
        key=lambda item: item["score"],
    )
    payload = json.dumps(
        rows,
        sort_keys=True,
        separators=(",", ":"),
    )
    dataset_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if not rows:
        return CalibrationTable(
            strategy_key=strategy_key,
            model_version=model_version,
            dataset_hash=dataset_hash,
            buckets=(),
        )
    size = max(1, math.ceil(len(rows) / max(1, bucket_count)))
    groups = _monotonic_score_groups([
        rows[start : start + size]
        for start in range(0, len(rows), size)
    ])
    buckets: list[CalibrationBucket] = []
    for group in groups:
        outcomes = [item["net_return_pct"] for item in group]
        maes = [item["mae_pct"] for item in group]
        mfes = [item["mfe_pct"] for item in group]
        buckets.append(
            CalibrationBucket(
                lower_score=group[0]["score"],
                upper_score=group[-1]["score"],
                sample_count=len(group),
                expected_return_net_pct=mean(outcomes),
                q10_pct=float(_quantile(outcomes, 0.10) or 0.0),
                q50_pct=float(median(outcomes)),
                q90_pct=float(_quantile(outcomes, 0.90) or 0.0),
                probability_positive=(
                    sum(value > 0 for value in outcomes) / len(outcomes)
                ),
                expected_mae_pct=mean(maes),
                expected_mfe_pct=mean(mfes),
                profit_factor=_profit_factor(outcomes),
                payoff_ratio=_payoff_ratio(outcomes),
            )
        )
    return CalibrationTable(
        strategy_key=strategy_key,
        model_version=model_version,
        dataset_hash=dataset_hash,
        buckets=tuple(buckets),
    )
