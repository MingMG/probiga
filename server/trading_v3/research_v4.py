from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from .backtest import (
    _band_series,
    _dynamic_signal_outcome,
    _execution_fee,
    _scaled,
    _validated_bucket,
)
from .calibration import CalibrationTable, fit_calibration
from .config import load_v3_config
from .metrics import maximum_drawdown, trade_metrics
from .portfolio_constraints import PortfolioConstraintState
from .regime import core_regime_probabilities
from .right_side_policy import (
    AMOUNT_RATIO_5_20_RANGE,
    DISTANCE_MA20_RANGE,
    MA20_SLOPE_5D_RANGE,
    MAXIMUM_LATEST_CHANGE_PCT,
    MINIMUM_MARKET_RETURN_20D_PCT,
    RETURN_20D_RANGE,
    RETURN_60D_RANGE,
)
from .validation import model_gate_failures


TREND_CANDIDATES = frozenset({
    "health_pullback_blended_v1",
    "rs_hpb_v1",
    "rs_hpb_no_health_v1",
    "production_trend_health_gate_v1",
})
REVERSAL_CANDIDATES = frozenset({
    "nvcr_price_reversal_v1",
    "qfbr_quality_reversal_v1",
})


@dataclass(frozen=True)
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

    def as_dict(self) -> dict[str, Any]:
        return {
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
        }


@dataclass(frozen=True)
class RegimeExpertModel:
    regime_column: str
    experts: tuple[tuple[str, RidgeReturnModel], ...]
    fallback: RidgeReturnModel
    minimum_regime_samples: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "protocol": "POINT_IN_TIME_REGIME_EXPERT_RIDGE_V1",
            "regime_column": self.regime_column,
            "minimum_regime_samples": self.minimum_regime_samples,
            "experts": {
                state: model.as_dict()
                for state, model in self.experts
            },
            "fallback": self.fallback.as_dict(),
        }


@dataclass(frozen=True)
class HurdleReturnModel:
    """Separate win probability from conditional win/loss magnitude."""

    probability_model: RidgeReturnModel
    positive_return_model: RidgeReturnModel
    loss_magnitude_model: RidgeReturnModel
    positive_sample_count: int
    negative_sample_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "protocol": "POINT_IN_TIME_HURDLE_RIDGE_V1",
            "probability_model": self.probability_model.as_dict(),
            "positive_return_model": self.positive_return_model.as_dict(),
            "loss_magnitude_model": self.loss_magnitude_model.as_dict(),
            "positive_sample_count": self.positive_sample_count,
            "negative_sample_count": self.negative_sample_count,
        }


def feature_availability_report(
    frame: pd.DataFrame,
    features: Iterable[str],
    *,
    minimum_coverage: float = 0.0,
) -> dict[str, Any]:
    """Freeze train-only feature availability and fail closed on sparse data."""

    requested = tuple(dict.fromkeys(str(item) for item in features))
    accepted: list[str] = []
    dropped: dict[str, str] = {}
    coverage: dict[str, float] = {}
    threshold = max(0.0, min(1.0, float(minimum_coverage)))
    for column in requested:
        if column not in frame.columns:
            coverage[column] = 0.0
            dropped[column] = "MISSING_COLUMN"
            continue
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(
            dtype=float,
            copy=False,
        )
        finite = np.isfinite(values)
        ratio = float(finite.mean()) if len(values) else 0.0
        coverage[column] = ratio
        if not finite.any():
            dropped[column] = "NO_FINITE_TRAINING_VALUE"
        elif ratio < threshold:
            dropped[column] = "TRAINING_COVERAGE_TOO_LOW"
        else:
            accepted.append(column)
    return {
        "protocol": "TRAIN_ONLY_FEATURE_AVAILABILITY_V1",
        "minimum_coverage": threshold,
        "requested": list(requested),
        "accepted": accepted,
        "dropped": dropped,
        "coverage": coverage,
        "status": "PASS" if accepted else "BLOCK",
    }


def _design_matrix(
    frame: pd.DataFrame,
    *,
    features: tuple[str, ...],
    medians: np.ndarray,
    means: np.ndarray,
    scales: np.ndarray,
) -> np.ndarray:
    values = frame.loc[:, list(features)].to_numpy(dtype=float, copy=True)
    values[~np.isfinite(values)] = np.nan
    missing = np.isnan(values)
    if missing.any():
        values[missing] = np.take(medians, np.nonzero(missing)[1])
    return (values - means) / scales


def fit_ridge_return_model(
    frame: pd.DataFrame,
    *,
    features: Iterable[str],
    ridge_lambda: float,
    target_clip: tuple[float, float],
    minimum_feature_coverage: float = 0.0,
) -> RidgeReturnModel:
    requested = tuple(features)
    if not requested:
        raise ValueError("features must not be empty")
    quality = feature_availability_report(
        frame,
        requested,
        minimum_coverage=minimum_feature_coverage,
    )
    columns = tuple(quality["accepted"])
    if not columns:
        raise ValueError("no feature passes the train-only availability gate")
    if len(frame) <= len(columns):
        raise ValueError("not enough rows to fit ridge model")
    raw = frame.loc[:, list(columns)].to_numpy(dtype=float, copy=True)
    raw[~np.isfinite(raw)] = np.nan
    medians = np.nanmedian(raw, axis=0)
    medians[~np.isfinite(medians)] = 0.0
    missing = np.isnan(raw)
    if missing.any():
        raw[missing] = np.take(medians, np.nonzero(missing)[1])
    means = raw.mean(axis=0)
    scales = raw.std(axis=0)
    scales[~np.isfinite(scales) | (scales < 1e-8)] = 1.0
    design = (raw - means) / scales
    target = frame["net_return_pct"].to_numpy(dtype=float, copy=True)
    target = np.clip(target, target_clip[0], target_clip[1])
    intercept = float(target.mean())
    centered_target = target - intercept
    penalty = np.eye(len(columns), dtype=float) * float(ridge_lambda)
    coefficients = np.linalg.solve(
        design.T @ design + penalty,
        design.T @ centered_target,
    )
    return RidgeReturnModel(
        features=columns,
        medians=tuple(float(value) for value in medians),
        means=tuple(float(value) for value in means),
        scales=tuple(float(value) for value in scales),
        coefficients=tuple(float(value) for value in coefficients),
        intercept=intercept,
        ridge_lambda=float(ridge_lambda),
        target_clip=(float(target_clip[0]), float(target_clip[1])),
        requested_features=requested,
        dropped_features=tuple(quality["dropped"]),
        feature_coverage=tuple(
            (column, float(quality["coverage"][column]))
            for column in requested
        ),
    )


def fit_regime_expert_model(
    frame: pd.DataFrame,
    *,
    features: Iterable[str],
    regime_column: str = "research_regime",
    ridge_lambda: float,
    target_clip: tuple[float, float],
    minimum_regime_samples: int = 160,
    minimum_feature_coverage: float = 0.80,
) -> RegimeExpertModel:
    if regime_column not in frame.columns:
        raise ValueError(f"missing point-in-time regime column: {regime_column}")
    fallback = fit_ridge_return_model(
        frame,
        features=features,
        ridge_lambda=ridge_lambda,
        target_clip=target_clip,
        minimum_feature_coverage=minimum_feature_coverage,
    )
    experts: list[tuple[str, RidgeReturnModel]] = []
    for state, group in frame.groupby(regime_column, sort=True, observed=True):
        if len(group) < int(minimum_regime_samples):
            continue
        experts.append((
            str(state),
            fit_ridge_return_model(
                group,
                features=features,
                ridge_lambda=ridge_lambda,
                target_clip=target_clip,
                minimum_feature_coverage=minimum_feature_coverage,
            ),
        ))
    return RegimeExpertModel(
        regime_column=regime_column,
        experts=tuple(experts),
        fallback=fallback,
        minimum_regime_samples=int(minimum_regime_samples),
    )


def predict_ridge_return(
    model: RidgeReturnModel,
    frame: pd.DataFrame,
) -> np.ndarray:
    design = _design_matrix(
        frame,
        features=model.features,
        medians=np.asarray(model.medians),
        means=np.asarray(model.means),
        scales=np.asarray(model.scales),
    )
    return model.intercept + design @ np.asarray(model.coefficients)


def fit_hurdle_return_model(
    frame: pd.DataFrame,
    *,
    features: Iterable[str],
    ridge_lambda: float,
    target_clip: tuple[float, float],
    minimum_feature_coverage: float = 0.0,
) -> HurdleReturnModel:
    """Fit P(win), E(win|win), and E(loss|loss) on train rows only."""

    finite = frame[
        pd.to_numeric(frame["net_return_pct"], errors="coerce").notna()
    ].copy()
    positive = finite[finite["net_return_pct"] > 0].copy()
    negative = finite[finite["net_return_pct"] < 0].copy()
    requested = tuple(features)
    minimum_component_rows = max(20, len(requested) + 2)
    if (
        len(positive) < minimum_component_rows
        or len(negative) < minimum_component_rows
    ):
        raise ValueError(
            "hurdle model requires enough positive and negative training rows"
        )
    probability = finite.copy()
    probability["net_return_pct"] = (
        probability["net_return_pct"] > 0
    ).astype(float)
    positive_target = positive.copy()
    positive_target["net_return_pct"] = positive_target[
        "net_return_pct"
    ].clip(lower=0.0, upper=max(0.01, float(target_clip[1])))
    loss_target = negative.copy()
    loss_target["net_return_pct"] = (-loss_target["net_return_pct"]).clip(
        lower=0.0,
        upper=max(0.01, abs(float(target_clip[0]))),
    )
    common = {
        "features": requested,
        "ridge_lambda": ridge_lambda,
        "minimum_feature_coverage": minimum_feature_coverage,
    }
    return HurdleReturnModel(
        probability_model=fit_ridge_return_model(
            probability,
            target_clip=(0.0, 1.0),
            **common,
        ),
        positive_return_model=fit_ridge_return_model(
            positive_target,
            target_clip=(0.0, max(0.01, float(target_clip[1]))),
            **common,
        ),
        loss_magnitude_model=fit_ridge_return_model(
            loss_target,
            target_clip=(0.0, max(0.01, abs(float(target_clip[0])))),
            **common,
        ),
        positive_sample_count=len(positive),
        negative_sample_count=len(negative),
    )


def predict_hurdle_return(
    model: HurdleReturnModel,
    frame: pd.DataFrame,
) -> np.ndarray:
    probability = np.clip(
        predict_ridge_return(model.probability_model, frame),
        0.01,
        0.99,
    )
    positive = np.maximum(
        0.0,
        predict_ridge_return(model.positive_return_model, frame),
    )
    loss = np.maximum(
        0.0,
        predict_ridge_return(model.loss_magnitude_model, frame),
    )
    return probability * positive - (1.0 - probability) * loss


def predict_regime_expert_return(
    model: RegimeExpertModel,
    frame: pd.DataFrame,
) -> np.ndarray:
    if model.regime_column not in frame.columns:
        raise ValueError(
            f"missing point-in-time regime column: {model.regime_column}"
        )
    result = predict_ridge_return(model.fallback, frame)
    states = frame[model.regime_column].astype(str).to_numpy()
    for state, expert in model.experts:
        mask = states == state
        if mask.any():
            result[mask] = predict_ridge_return(expert, frame.loc[mask])
    return result


def prediction_to_score(prediction: np.ndarray) -> np.ndarray:
    clipped = np.clip(prediction / 3.0, -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _rolling_percentile(series: pd.Series) -> pd.Series:
    values = series.to_numpy(dtype=float, copy=False)
    result = np.full(len(values), np.nan, dtype=float)
    for index in range(252, len(values)):
        history = values[index - 252:index]
        if not math.isfinite(values[index]) or np.count_nonzero(np.isfinite(history)) < 252:
            continue
        target = values[index]
        less = int(np.count_nonzero(history < target))
        equal = int(np.count_nonzero(history == target))
        result[index] = (less + equal / 2.0) / len(history)
    return pd.Series(result, index=series.index)


def enrich_research_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach only point-in-time market and reversal features."""

    result = frame.copy()
    daily = result.groupby("trade_date", sort=True, observed=True).agg(
        market_return_60d_pct=("return_60d_pct", "median"),
        market_breadth_ma20_pct=(
            "close_above_ma20",
            lambda values: float(values.mean()) * 100.0,
        ),
        market_aligned_breadth_pct=(
            "ma20_above_ma60",
            lambda values: float(values.mean()) * 100.0,
        ),
        market_daily_return_pct=("change_pct", "median"),
        market_amount_ratio_20d=("amount_ratio_5_20", "median"),
        limit_down_breadth_pct=(
            "change_pct",
            lambda values: float((values <= -9.5).mean()) * 100.0,
        ),
    )
    daily["market_return_20d_pct"] = result.groupby(
        "trade_date",
        observed=True,
    )["market_return_20d_pct"].first()
    daily["market_return_20d_change_10d_pct"] = (
        daily["market_return_20d_pct"]
        - daily["market_return_20d_pct"].shift(10)
    )
    daily["breadth_ma20_change_5d_pct"] = (
        daily["market_breadth_ma20_pct"]
        - daily["market_breadth_ma20_pct"].shift(5)
    )
    daily["market_volatility_20d_pct"] = daily[
        "market_daily_return_pct"
    ].rolling(20, min_periods=15).std()
    positive_breadth = result.groupby(
        "trade_date",
        sort=True,
        observed=True,
    )["change_pct"].agg(lambda values: float((values > 0).mean()) * 100.0)
    daily["market_breadth_pct"] = positive_breadth
    daily["breadth_change_5d_pct"] = positive_breadth - positive_breadth.shift(5)
    daily["realized_volatility_20d_pct"] = result.groupby(
        "trade_date",
        sort=True,
        observed=True,
    )["change_pct"].median().rolling(20, min_periods=15).std(ddof=0)
    daily["limit_down_ratio_pct"] = daily["limit_down_breadth_pct"]
    daily["market_log_amount_ratio_20d"] = np.log(
        daily["market_amount_ratio_20d"].clip(lower=1e-6)
    )
    percentile_sources = {
        "market_return_60d_pct": 0.25,
        "market_breadth_ma20_pct": 0.25,
        "market_aligned_breadth_pct": 0.20,
        "market_return_20d_change_10d_pct": 0.15,
        "market_log_amount_ratio_20d": 0.05,
    }
    health = pd.Series(0.0, index=daily.index)
    for column, weight in percentile_sources.items():
        health = health + weight * _rolling_percentile(daily[column])
    health = health + 0.10 * (
        1.0 - _rolling_percentile(daily["market_volatility_20d_pct"])
    )
    daily["market_health"] = health
    daily["prior_market_health"] = health.shift(1)
    daily["research_regime"] = classify_research_regime(daily)
    for state in (
        "TREND_UP",
        "THEME_ROTATION",
        "RANGE",
        "PANIC_RECOVERY",
        "RISK_OFF",
    ):
        daily[f"regime_probability_{state.lower()}"] = [
            core_regime_probabilities(
                market_return_20d_pct=row.market_return_20d_pct,
                market_breadth_pct=row.market_breadth_pct,
                breadth_change_5d_pct=row.breadth_change_5d_pct,
                realized_volatility_20d_pct=row.realized_volatility_20d_pct,
                limit_down_ratio_pct=row.limit_down_ratio_pct,
            )[state]
            if all(math.isfinite(float(value)) for value in (
                row.market_return_20d_pct,
                row.market_breadth_pct,
                row.breadth_change_5d_pct,
                row.realized_volatility_20d_pct,
                row.limit_down_ratio_pct,
            ))
            else math.nan
            for row in daily.itertuples()
        ]
    for column in daily.columns:
        if column == "research_regime":
            continue
        result[column] = result["trade_date"].map(daily[column]).astype(
            "float32"
        )
    result["research_regime"] = result["trade_date"].map(
        daily["research_regime"]
    ).astype("category")

    groups = result.groupby("stock_code", sort=False, observed=True)
    result["prior_10d_low"] = groups["raw_low"].transform(
        lambda values: values.shift(1).rolling(10, min_periods=10).min()
    ).astype("float32")
    result["ma10"] = groups["close"].transform(
        lambda values: values.rolling(10, min_periods=10).mean()
    ).astype("float32")
    spread = (result["raw_high"] - result["raw_low"]).replace(0, np.nan)
    result["close_location_value"] = (
        (result["raw_close"] - result["raw_low"]) / spread
    ).clip(lower=0.0, upper=1.0).fillna(0.5).astype("float32")
    result["reversal_confirmation_score"] = (
        0.40 * _band_series(
            result["close_location_value"], 0.65, 1.0, 0.20
        )
        + 0.30 * _scaled(result["rebound_from_low_pct"], 2, 8)
        + 0.20 * _band_series(result["change_pct"], 0.5, 5, 2)
        + 0.10 * _band_series(result["amount_ratio_1_20"], 1, 2, 1)
    ).clip(lower=0.0, upper=1.0).astype("float32")
    result["market_repair_score"] = (
        0.60 * _scaled(result["breadth_ma20_change_5d_pct"], 3, 15)
        + 0.40 * _scaled(result["market_daily_return_pct"], -2.5, 2.5)
    ).clip(lower=0.0, upper=1.0).astype("float32")
    return result


def classify_research_regime(frame: pd.DataFrame) -> pd.Series:
    """Use the exact market-only state router shared with production."""

    required = (
        "market_return_20d_pct",
        "market_breadth_pct",
        "breadth_change_5d_pct",
        "realized_volatility_20d_pct",
        "limit_down_ratio_pct",
    )
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError("regime inputs missing: " + ", ".join(missing))
    states: list[str] = []
    for row in frame.loc[:, required].itertuples(index=False, name=None):
        if not all(math.isfinite(float(value)) for value in row):
            states.append("DATA_BLOCKED")
            continue
        probabilities = core_regime_probabilities(
            market_return_20d_pct=row[0],
            market_breadth_pct=row[1],
            breadth_change_5d_pct=row[2],
            realized_volatility_20d_pct=row[3],
            limit_down_ratio_pct=row[4],
        )
        states.append(max(probabilities, key=probabilities.get))
    return pd.Series(states, index=frame.index, dtype="category")


def _research_theme_codes(row: Mapping[str, Any]) -> tuple[str, ...]:
    values: set[str] = set()
    for key in (
        "theme_codes",
        "theme_cluster_keys",
        "all_theme_cluster_keys",
    ):
        raw = row.get(key)
        if isinstance(raw, (list, tuple, set)):
            values.update(str(item) for item in raw if str(item))
    if row.get("theme_code"):
        values.add(str(row["theme_code"]))
    return tuple(sorted(values))


def research_risk_asset_cap(
    row: Mapping[str, Any],
    *,
    config: Mapping[str, Any] | None = None,
) -> float:
    """Reproduce the production probabilistic risk cap from PIT fields only."""

    runtime = dict(config or load_v3_config())
    required = (
        "market_return_20d_pct",
        "market_breadth_pct",
        "breadth_change_5d_pct",
        "realized_volatility_20d_pct",
        "limit_down_ratio_pct",
    )
    try:
        values = [float(row[key]) for key in required]
    except (KeyError, TypeError, ValueError):
        state = str(row.get("research_regime") or "")
        return max(
            0.0,
            min(
                1.0,
                float(
                    runtime.get("regime", {})
                    .get("risk_asset_caps", {})
                    .get(state, 0.0)
                ),
            ),
        )
    if not all(math.isfinite(value) for value in values):
        return 0.0
    probabilities = core_regime_probabilities(
        market_return_20d_pct=values[0],
        market_breadth_pct=values[1],
        breadth_change_5d_pct=values[2],
        realized_volatility_20d_pct=values[3],
        limit_down_ratio_pct=values[4],
    )
    caps = runtime["regime"]["risk_asset_caps"]
    base_cap = sum(
        float(probability) * float(caps[state])
        for state, probability in probabilities.items()
    )
    concentration = float(row.get("sector_concentration_pct") or 0.0)
    policy_support = float(row.get("policy_support_score") or 0.0)
    news_risk = float(row.get("news_risk_score") or 0.0)
    overseas_risk = float(row.get("overseas_risk_score") or 0.0)
    concentration_penalty = 0.20 * max(
        0.0,
        min(1.0, (concentration - 45.0) / 55.0),
    )
    context_multiplier = max(
        0.45,
        min(
            1.05,
            1.0
            + 0.10 * policy_support
            - 0.35 * news_risk
            - 0.28 * overseas_risk
            - concentration_penalty,
        ),
    )
    return max(0.0, min(1.0, base_cap * context_multiplier))


def portfolio_capacity_training_rows(
    labels: pd.DataFrame,
    *,
    config: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Keep only labels feasible under the shared production constraints.

    Outcomes are already after costs.  This train-only filter now uses the
    same state machine as live portfolio construction: probabilistic regime
    cap, turnover, theme/correlation caps, open risk, risk sizing, board lots,
    minimum economic order and available cash.
    """

    if labels.empty:
        return labels.copy()
    required = {"stock_code", "entry_date", "entry_open", "score"}
    missing = required.difference(labels.columns)
    if missing:
        raise ValueError(
            "portfolio-aligned labels missing: " + ", ".join(sorted(missing))
        )
    runtime = dict(config or load_v3_config())
    account = dict(runtime["account"])
    policy = dict(runtime["portfolio"])
    initial_cash = float(account["initial_cash_cny"])
    ordered = labels.copy()
    ordered["entry_date"] = pd.to_datetime(ordered["entry_date"])
    if "exit_date" in ordered:
        ordered["exit_date"] = pd.to_datetime(ordered["exit_date"])
    ordered = ordered.sort_values(
        ["entry_date", "score", "stock_code"],
        ascending=[True, False, True],
        kind="stable",
    )
    active: dict[str, dict[str, Any]] = {}
    cash = initial_cash
    selected_indices: list[Any] = []
    rejection_counts: dict[str, int] = defaultdict(int)
    for entry_day, group in ordered.groupby("entry_date", sort=True):
        for code, position in list(active.items()):
            exit_day = position["exit_date"]
            if pd.isna(exit_day) or exit_day > entry_day:
                continue
            net_return = float(position.get("net_return_pct") or 0.0)
            cash += float(position["entry_value"]) * (
                1.0 + net_return / 100.0
            ) + float(position.get("buy_fee") or 0.0)
            del active[code]
        marked_positions = sum(
            float(position["entry_value"]) for position in active.values()
        )
        equity = max(1.0, cash + marked_positions)
        position_weights = {
            code: float(position["entry_value"]) / equity
            for code, position in active.items()
        }
        position_quantities = {
            code: int(position["quantity"])
            for code, position in active.items()
        }
        position_themes = {
            code: tuple(position["themes"])
            for code, position in active.items()
        }
        theme_weights: dict[str, float] = defaultdict(float)
        open_risk_cny = 0.0
        for code, position in active.items():
            weight = position_weights[code]
            for theme in position["themes"]:
                theme_weights[str(theme)] += weight
            open_risk_cny += float(position["open_risk_cny"])
        first = group.iloc[0].to_dict()
        constraints = PortfolioConstraintState(
            policy=policy,
            equity=equity,
            risk_asset_cap=research_risk_asset_cap(
                first,
                config=runtime,
            ),
            current_theme_weights=theme_weights,
            current_position_weights=position_weights,
            current_position_quantities=position_quantities,
            current_position_themes=position_themes,
            current_open_risk_weight=open_risk_cny / equity,
        )
        for index, row in group.iterrows():
            code = str(row["stock_code"])
            price = float(row["entry_open"] or 0.0)
            admission = constraints.admit(
                stock_code=code,
                price=price,
                initial_stop_pct=float(row.get("initial_stop_pct") or -8.0),
                candidate_themes=_research_theme_codes(row.to_dict()),
                available_cash_cny=cash,
            )
            if not admission.accepted:
                rejection_counts[admission.reason_code] += 1
                continue
            selected_indices.append(index)
            exit_day = row.get("exit_date")
            buy_fee = _execution_fee(
                admission.order_value,
                account=account,
                sell=False,
            )
            cash -= admission.order_value + buy_fee
            active[code] = {
                "exit_date": (
                    pd.Timestamp(exit_day)
                    if exit_day is not None and not pd.isna(exit_day)
                    else pd.NaT
                ),
                "entry_value": admission.order_value,
                "buy_fee": buy_fee,
                "quantity": admission.delta_quantity,
                "themes": _research_theme_codes(row.to_dict()),
                "open_risk_cny": admission.open_risk_cny,
                "net_return_pct": row.get("net_return_pct"),
            }
    result = ordered.loc[selected_indices].copy()
    result["training_objective_protocol"] = (
        "AFTER_COST_PRODUCTION_CONSTRAINT_PARITY_V2"
    )
    result.attrs["capacity_report"] = {
        "protocol": "PRODUCTION_PORTFOLIO_CONSTRAINT_STATE_V1",
        "input_count": len(ordered),
        "selected_count": len(result),
        "rejection_counts": dict(sorted(rejection_counts.items())),
    }
    return result


def _common_liquid_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        (frame["amount20"] >= 50_000_000)
        & (frame["amount"] > 0)
        & (frame["raw_close"] >= 2)
        & (~frame["name_excluded"].fillna(0).astype(bool))
        & (frame["change_pct"] < MAXIMUM_LATEST_CHANGE_PCT)
    )


def _health_gate(frame: pd.DataFrame) -> pd.Series:
    return (
        (frame["market_health"] >= 0.60)
        & (frame["prior_market_health"] >= 0.60)
        & (frame["limit_down_breadth_pct"] < 1.0)
        & (frame["market_daily_return_pct"] > -2.5)
    )


def _pullback_components(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    quality = (
        0.30 * _band_series(frame["relative_strength_20d_pct"], 3, 18, 12)
        + 0.20 * _band_series(frame["distance_ma20_pct"], 0, 4, 4)
        + 0.15 * _band_series(frame["return_5d_pct"], -2, 6, 6)
        + 0.15 * _band_series(frame["amount_ratio_5_20"], 0.8, 1.4, 0.7)
        + 0.10 * (1.0 - _scaled(frame["atr_14d_pct"], 1, 5))
        + 0.10 * _band_series(frame["change_pct"], -1, 3, 4)
    ).clip(lower=0.0, upper=1.0)
    eligibility = (
        (frame["close_above_ma20"] == 1)
        & (frame["ma20_above_ma60"] == 1)
        & frame["return_60d_pct"].between(8, 45)
        & frame["return_20d_pct"].between(0, 18)
        & frame["distance_ma20_pct"].between(-1, 6)
        & frame["amount_ratio_5_20"].between(0.7, 1.5)
        & frame["change_pct"].between(-2, 4)
        & frame["atr_14d_pct"].between(1, 5)
    )
    return eligibility, quality


def _strict_pullback_eligibility(
    frame: pd.DataFrame,
    quality: pd.Series,
) -> pd.Series:
    base, _ = _pullback_components(frame)
    return (
        base
        & frame["return_5d_pct"].between(-3, 10)
        & frame["relative_strength_20d_pct"].between(-2, 25)
        & (quality >= 0.60)
    )


def _production_eligibility(frame: pd.DataFrame) -> pd.Series:
    return (
        (frame["close_above_ma20"] == 1)
        & (frame["ma20_above_ma60"] == 1)
        & (frame["market_return_20d_pct"] >= MINIMUM_MARKET_RETURN_20D_PCT)
        & frame["return_60d_pct"].between(*RETURN_60D_RANGE)
        & frame["return_20d_pct"].between(*RETURN_20D_RANGE)
        & frame["ma20_slope_5d_pct"].between(*MA20_SLOPE_5D_RANGE)
        & frame["distance_ma20_pct"].between(*DISTANCE_MA20_RANGE)
        & frame["amount_ratio_5_20"].between(*AMOUNT_RATIO_5_20_RANGE)
    )


def _reversal_eligibility(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["return_20d_pct"].between(-35, -10)
        & frame["drawdown_20d_pct"].between(-45, -12)
        & frame["distance_ma20_pct"].between(-22, -4)
        & frame["atr_14d_pct"].between(2, 8)
        & (frame["market_return_20d_pct"] >= -12)
        & (frame["breadth_ma20_change_5d_pct"] >= 3)
        & (frame["limit_down_breadth_pct"] <= 1.5)
        & (frame["raw_low"] <= frame["prior_10d_low"] * 1.01)
        & (frame["close_location_value"] >= 0.65)
        & frame["change_pct"].between(0.5, 7)
        & (frame["latest_relative_to_market_pct"] >= 1)
        & (frame["rebound_from_low_pct"] >= 2)
        & frame["amount_ratio_1_20"].between(1, 3)
    )


def _nvcr_score(frame: pd.DataFrame) -> pd.Series:
    return (
        0.28 * _scaled(-frame["drawdown_20d_pct"], 12, 45)
        + 0.27 * _band_series(frame["close_location_value"], 0.65, 1.0, 0.20)
        + 0.15 * _scaled(frame["rebound_from_low_pct"], 2, 8)
        + 0.15 * _scaled(frame["breadth_ma20_change_5d_pct"], 3, 15)
        + 0.10 * _band_series(frame["amount_ratio_1_20"], 1, 2, 1)
        + 0.05 * (1.0 - _scaled(frame["atr_14d_pct"], 2, 8))
    ).clip(lower=0.0, upper=1.0)


def candidate_universes(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return eligible, unranked rows for every frozen candidate."""

    common = _common_liquid_mask(frame)
    health = _health_gate(frame)
    pullback, quality = _pullback_components(frame)
    reversal = _reversal_eligibility(frame)
    nvcr_score = _nvcr_score(frame)
    candidates: dict[str, pd.DataFrame] = {}

    def add(name: str, mask: pd.Series, score: pd.Series, sleeve: str) -> None:
        selected = frame.loc[common & mask].copy()
        selected["score"] = score.loc[selected.index].astype("float32")
        selected["candidate_id"] = name
        selected["exit_sleeve"] = sleeve
        if "short_name" not in selected:
            selected["short_name"] = selected["stock_code"].astype(str)
        candidates[name] = selected

    add(
        "health_pullback_blended_v1",
        pullback & health,
        0.75 * quality + 0.25 * frame["market_health"],
        "trend",
    )
    strict_pullback = _strict_pullback_eligibility(frame, quality)
    add(
        "rs_hpb_v1",
        strict_pullback & health,
        quality,
        "trend",
    )
    add(
        "rs_hpb_no_health_v1",
        strict_pullback,
        quality,
        "trend",
    )
    add(
        "production_trend_health_gate_v1",
        _production_eligibility(frame) & health,
        frame["score"],
        "trend",
    )
    add(
        "nvcr_price_reversal_v1",
        reversal,
        nvcr_score,
        "reversal",
    )
    finance_columns = {
        "quality_percentile",
        "cashflow_percentile",
        "valuation_percentile",
        "asset_liab_ratio_pit",
        "net_profit_yoy_gr_pit",
    }
    if finance_columns.issubset(frame.columns):
        finance_mask = (
            reversal
            & (frame["quality_percentile"] >= 0.65)
            & (frame["cashflow_percentile"] >= 0.60)
            & (frame["asset_liab_ratio_pit"] <= 70)
            & (frame["net_profit_yoy_gr_pit"] > -30)
        )
        qfbr_score = (
            0.22 * frame["quality_percentile"]
            + 0.16 * frame["cashflow_percentile"]
            + 0.10 * frame["valuation_percentile"]
            + 0.18 * _scaled(-frame["drawdown_20d_pct"], 12, 45)
            + 0.22 * frame["reversal_confirmation_score"]
            + 0.12 * frame["market_repair_score"]
        ).clip(lower=0.0, upper=1.0)
        add(
            "qfbr_quality_reversal_v1",
            finance_mask,
            qfbr_score,
            "reversal",
        )
    else:
        candidates["qfbr_quality_reversal_v1"] = frame.iloc[0:0].copy()
    return candidates


def attach_point_in_time_finance(
    reversal_rows: pd.DataFrame,
    *,
    market_frame: pd.DataFrame,
    finance_rows: pd.DataFrame,
) -> pd.DataFrame:
    """Rank finance using only statements visible on each signal date."""

    if reversal_rows.empty:
        return reversal_rows.copy()
    required = {
        "stock_code",
        "report_date",
        "notice_date",
        "id",
        "net_asset_ps",
        "oper_cf_ps",
        "net_profit_yoy_gr",
        "roe_wtd",
        "gross_margin",
        "net_margin",
        "cash_flow_ratio",
        "asset_liab_ratio",
    }
    missing = required.difference(finance_rows.columns)
    if missing:
        raise ValueError("finance rows missing: " + ", ".join(sorted(missing)))
    events = finance_rows.copy()
    events["stock_code"] = events["stock_code"].astype(str).str[:6]
    events["report_date"] = pd.to_datetime(events["report_date"])
    events["notice_date"] = pd.to_datetime(events["notice_date"])
    events = events[
        events["notice_date"].notna()
        & events["report_date"].notna()
        & (events["notice_date"] >= events["report_date"])
    ].sort_values(["notice_date", "id"], kind="stable")
    event_records = events.to_dict("records")
    event_index = 0
    latest: dict[str, dict[str, Any]] = {}
    market_groups = market_frame.groupby("trade_date", sort=False, observed=True)
    output: list[pd.DataFrame] = []
    for signal_day, candidate_day in reversal_rows.groupby(
        "trade_date", sort=True, observed=True
    ):
        signal_day = pd.Timestamp(signal_day)
        while (
            event_index < len(event_records)
            and pd.Timestamp(event_records[event_index]["notice_date"]) <= signal_day
        ):
            event = event_records[event_index]
            code = str(event["stock_code"])
            previous = latest.get(code)
            if (
                previous is None
                or pd.Timestamp(event["report_date"])
                > pd.Timestamp(previous["report_date"])
                or (
                    pd.Timestamp(event["report_date"])
                    == pd.Timestamp(previous["report_date"])
                    and int(event["id"]) > int(previous["id"])
                )
            ):
                latest[code] = event
            event_index += 1
        try:
            market_day = market_groups.get_group(signal_day)
        except KeyError:
            continue
        liquid_day = market_day.loc[_common_liquid_mask(market_day)]
        quality_raw: dict[str, float] = {}
        cashflow_raw: dict[str, float] = {}
        valuation_raw: dict[str, float] = {}
        # Iterating a very wide cached DataFrame through ``itertuples`` can
        # fail inside pandas' dynamically generated namedtuple code on some
        # Python/pandas combinations.  Only these two columns are needed;
        # iterate their arrays directly so feature-cache width cannot change
        # the point-in-time finance result.
        for raw_code, raw_close in zip(
            liquid_day["stock_code"].array,
            liquid_day["raw_close"].array,
        ):
            code = str(raw_code)
            values = latest.get(code)
            if not values:
                continue
            quality_values = tuple(
                values.get(key)
                for key in (
                    "roe_wtd",
                    "gross_margin",
                    "net_margin",
                    "asset_liab_ratio",
                )
            )
            if all(value is not None and math.isfinite(float(value)) for value in quality_values):
                quality_raw[code] = (
                    float(quality_values[0])
                    + float(quality_values[1]) * 0.25
                    + float(quality_values[2]) * 0.25
                    - float(quality_values[3]) * 0.15
                )
            cash_values = (
                values.get("oper_cf_ps"),
                values.get("cash_flow_ratio"),
            )
            if all(value is not None and math.isfinite(float(value)) for value in cash_values):
                cashflow_raw[code] = float(cash_values[0]) + float(cash_values[1]) * 0.1
            net_asset = values.get("net_asset_ps")
            if net_asset is not None and math.isfinite(float(net_asset)) and float(net_asset) > 0:
                valuation_raw[code] = float(raw_close) / float(net_asset)
        quality_rank = pd.Series(quality_raw, dtype=float).rank(pct=True).to_dict()
        cashflow_rank = pd.Series(cashflow_raw, dtype=float).rank(pct=True).to_dict()
        valuation_rank = pd.Series(valuation_raw, dtype=float).rank(
            pct=True,
            ascending=False,
        ).to_dict()
        enriched = candidate_day.copy()
        codes = enriched["stock_code"].astype(str)
        enriched["quality_percentile"] = codes.map(quality_rank)
        enriched["cashflow_percentile"] = codes.map(cashflow_rank)
        enriched["valuation_percentile"] = codes.map(valuation_rank)
        enriched["asset_liab_ratio_pit"] = codes.map(
            lambda code: (latest.get(code) or {}).get("asset_liab_ratio")
        )
        enriched["net_profit_yoy_gr_pit"] = codes.map(
            lambda code: (latest.get(code) or {}).get("net_profit_yoy_gr")
        )
        output.append(enriched)
    if not output:
        return reversal_rows.iloc[0:0].copy()
    return pd.concat(output, ignore_index=True)


def quality_reversal_universe(finance_reversal_rows: pd.DataFrame) -> pd.DataFrame:
    if finance_reversal_rows.empty:
        return finance_reversal_rows.copy()
    mask = (
        (finance_reversal_rows["quality_percentile"] >= 0.65)
        & (finance_reversal_rows["cashflow_percentile"] >= 0.60)
        & (finance_reversal_rows["asset_liab_ratio_pit"] <= 70)
        & (finance_reversal_rows["net_profit_yoy_gr_pit"] > -30)
    )
    result = finance_reversal_rows.loc[mask].copy()
    result["score"] = (
        0.22 * result["quality_percentile"]
        + 0.16 * result["cashflow_percentile"]
        + 0.10 * result["valuation_percentile"]
        + 0.18 * _scaled(-result["drawdown_20d_pct"], 12, 45)
        + 0.22 * result["reversal_confirmation_score"]
        + 0.12 * result["market_repair_score"]
    ).clip(lower=0.0, upper=1.0).astype("float32")
    result["candidate_id"] = "qfbr_quality_reversal_v1"
    result["exit_sleeve"] = "reversal"
    return result


def select_top_per_day(
    candidates: pd.DataFrame,
    *,
    top_per_day: int = 10,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()
    return (
        candidates.sort_values(
            ["trade_date", "score", "stock_code"],
            ascending=[True, False, True],
            kind="stable",
        )
        .groupby("trade_date", sort=False, observed=True)
        .head(top_per_day)
        .copy()
    )


def combine_dual_regime(
    health_rows: pd.DataFrame,
    reversal_rows: pd.DataFrame,
    *,
    top_per_day: int = 10,
) -> pd.DataFrame:
    trend = health_rows[health_rows["market_health"] >= 0.60]
    reversal = reversal_rows[reversal_rows["market_health"] < 0.55]
    combined = pd.concat([trend, reversal], ignore_index=True)
    if combined.empty:
        return combined
    combined["candidate_id"] = "dual_regime_health_reversal_v1"
    return select_top_per_day(combined, top_per_day=top_per_day)


def _bar_value(row: Mapping[str, Any], key: str) -> float:
    return float(row[key])


def reversal_signal_outcome(
    group: pd.DataFrame,
    *,
    signal_index: int,
    config: Mapping[str, Any],
    include_censored: bool = True,
) -> dict[str, Any] | None:
    """Replay the frozen five-session reversal exit with next-open fills."""

    signal = group.iloc[signal_index]
    entry_index: int | None = None
    for index in range(signal_index + 1, len(group)):
        row = group.iloc[index]
        if float(row["amount"] or 0.0) <= 0:
            continue
        previous_close = float(group.iloc[index - 1]["raw_close"])
        one_price_limit_up = (
            float(row["raw_open"]) >= previous_close * 1.095
            and abs(float(row["raw_high"]) - float(row["raw_low"])) < 1e-8
        )
        if one_price_limit_up:
            continue
        entry_index = index
        break
    if entry_index is None:
        return None
    entry = group.iloc[entry_index]
    entry_price = float(entry["raw_open"])
    if entry_price > float(signal["raw_close"]) * 1.005:
        return None
    account = config["account"]
    policy = config["portfolio"]
    desired = float(account["initial_cash_cny"]) * float(
        policy.get("initial_probe_position_weight", policy["normal_position_weight"])
    )
    quantity = math.floor(desired / entry_price / 100) * 100
    entry_value = quantity * entry_price
    if quantity <= 0 or entry_value < float(policy["minimum_economic_order_cny"]):
        return None
    atr_value = float(signal["atr_14d_pct"]) / 100.0 * float(signal["raw_close"])
    stop = max(entry_price * 0.94, float(signal["raw_low"]) - 0.25 * atr_value)
    risk = max(entry_price - stop, entry_price * 0.005)
    buy_fee = _execution_fee(entry_value, account=dict(account), sell=False)
    minimum_low = entry_price
    maximum_high = entry_price
    pending_reason: str | None = None
    holding_sessions = 0
    for index in range(entry_index, len(group)):
        row = group.iloc[index]
        if float(row["amount"] or 0.0) <= 0:
            continue
        if pending_reason is not None:
            previous_close = float(group.iloc[index - 1]["raw_close"])
            one_price_limit_down = (
                float(row["raw_open"]) <= previous_close * 0.905
                and abs(float(row["raw_high"]) - float(row["raw_low"])) < 1e-8
            )
            if one_price_limit_down:
                continue
            exit_price = float(row["raw_open"])
            exit_value = quantity * exit_price
            sell_fee = _execution_fee(exit_value, account=dict(account), sell=True)
            net_pnl = (exit_price - entry_price) * quantity - buy_fee - sell_fee
            return {
                "entry_open": entry_price,
                "exit_close": exit_price,
                "entry_date": pd.Timestamp(entry["trade_date"]),
                "exit_date": pd.Timestamp(row["trade_date"]),
                "exit_reason": pending_reason,
                "holding_days": holding_sessions,
                "label_order_value_cny": entry_value,
                "net_return_pct": net_pnl / entry_value * 100.0,
                "mae_pct": (minimum_low / entry_price - 1.0) * 100.0,
                "mfe_pct": (maximum_high / entry_price - 1.0) * 100.0,
                "label_mature": True,
            }
        holding_sessions += 1
        minimum_low = min(minimum_low, float(row["raw_low"]))
        maximum_high = max(maximum_high, float(row["raw_high"]))
        close = float(row["raw_close"])
        adjusted_close = float(row["close"])
        if float(row["raw_low"]) <= stop:
            pending_reason = "HARD_STOP"
        elif close < float(signal["raw_low"]):
            pending_reason = "SIGNAL_LOW_BROKEN"
        elif close >= entry_price + 2.0 * risk:
            pending_reason = "TWO_R_TARGET"
        elif adjusted_close > float(row["ma10"]) and close > entry_price:
            pending_reason = "MA10_RECLAIM_PROFIT"
        elif holding_sessions >= 5:
            pending_reason = "MAX_HOLD_5"
    if not include_censored:
        return None
    return {
        "entry_open": entry_price,
        "exit_close": math.nan,
        "entry_date": pd.Timestamp(entry["trade_date"]),
        "exit_date": pd.NaT,
        "exit_reason": "RIGHT_CENSORED",
        "holding_days": holding_sessions,
        "label_order_value_cny": entry_value,
        "net_return_pct": math.nan,
        "mae_pct": math.nan,
        "mfe_pct": math.nan,
        "label_mature": False,
    }


def bounded_signal_outcome(
    group: pd.DataFrame,
    *,
    signal_index: int,
    config: Mapping[str, Any],
    stop_pct: float,
    take_profit_pct: float,
    maximum_holding_sessions: int,
    maximum_entry_gap_pct: float,
    include_censored: bool = True,
) -> dict[str, Any] | None:
    """Replay a close-decided bounded-horizon policy at next tradable opens."""

    signal = group.iloc[signal_index]
    entry_index: int | None = None
    for index in range(signal_index + 1, len(group)):
        row = group.iloc[index]
        if float(row["amount"] or 0.0) <= 0:
            continue
        previous_close = float(group.iloc[index - 1]["raw_close"])
        one_price_limit_up = (
            float(row["raw_open"]) >= previous_close * 1.095
            and abs(float(row["raw_high"]) - float(row["raw_low"]))
            <= max(1e-8, previous_close * 1e-6)
        )
        if one_price_limit_up:
            continue
        entry_index = index
        break
    if entry_index is None:
        return None
    entry = group.iloc[entry_index]
    entry_price = float(entry["raw_open"])
    if entry_price > float(signal["raw_close"]) * (1.0 + maximum_entry_gap_pct / 100.0):
        return None
    account = dict(config["account"])
    policy = dict(config["portfolio"])
    desired = float(account["initial_cash_cny"]) * float(
        policy.get("initial_probe_position_weight", policy["normal_position_weight"])
    )
    quantity = math.floor(desired / entry_price / 100) * 100
    entry_value = quantity * entry_price
    if quantity <= 0 or entry_value < float(policy["minimum_economic_order_cny"]):
        return None
    buy_fee = _execution_fee(entry_value, account=account, sell=False)
    stop_price = entry_price * (1.0 - stop_pct / 100.0)
    target_price = entry_price * (1.0 + take_profit_pct / 100.0)
    minimum_low = entry_price
    maximum_high = entry_price
    holding_sessions = 0
    pending_reason: str | None = None
    for index in range(entry_index, len(group)):
        row = group.iloc[index]
        if float(row["amount"] or 0.0) <= 0:
            continue
        if pending_reason is not None:
            previous_close = float(group.iloc[index - 1]["raw_close"])
            one_price_limit_down = (
                float(row["raw_open"]) <= previous_close * 0.905
                and abs(float(row["raw_high"]) - float(row["raw_low"]))
                <= max(1e-8, previous_close * 1e-6)
            )
            if one_price_limit_down:
                continue
            exit_price = float(row["raw_open"])
            exit_value = quantity * exit_price
            sell_fee = _execution_fee(exit_value, account=account, sell=True)
            net_pnl = (exit_price - entry_price) * quantity - buy_fee - sell_fee
            return {
                "entry_open": entry_price,
                "exit_close": exit_price,
                "entry_date": pd.Timestamp(entry["trade_date"]),
                "exit_date": pd.Timestamp(row["trade_date"]),
                "exit_reason": pending_reason,
                "holding_days": holding_sessions,
                "label_order_value_cny": entry_value,
                "net_return_pct": net_pnl / entry_value * 100.0,
                "mae_pct": (minimum_low / entry_price - 1.0) * 100.0,
                "mfe_pct": (maximum_high / entry_price - 1.0) * 100.0,
                "label_mature": True,
            }
        holding_sessions += 1
        minimum_low = min(minimum_low, float(row["raw_low"]))
        maximum_high = max(maximum_high, float(row["raw_high"]))
        if float(row["raw_low"]) <= stop_price:
            pending_reason = "BOUNDED_STOP_TOUCH"
        elif float(row["raw_high"]) >= target_price:
            pending_reason = "BOUNDED_TARGET_TOUCH"
        elif holding_sessions >= maximum_holding_sessions:
            pending_reason = "BOUNDED_TIME_EXIT"
    if not include_censored:
        return None
    return {
        "entry_open": entry_price,
        "exit_close": math.nan,
        "entry_date": pd.Timestamp(entry["trade_date"]),
        "exit_date": pd.NaT,
        "exit_reason": "RIGHT_CENSORED",
        "holding_days": holding_sessions,
        "label_order_value_cny": entry_value,
        "net_return_pct": math.nan,
        "mae_pct": math.nan,
        "mfe_pct": math.nan,
        "label_mature": False,
    }


def label_bounded_candidate(
    features: pd.DataFrame,
    selected: pd.DataFrame,
    *,
    stop_pct: float,
    take_profit_pct: float,
    maximum_holding_sessions: int,
    maximum_entry_gap_pct: float,
    config: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    if selected.empty:
        return selected.assign(
            net_return_pct=pd.Series(dtype="float64"),
            mae_pct=pd.Series(dtype="float64"),
            mfe_pct=pd.Series(dtype="float64"),
            exit_date=pd.Series(dtype="datetime64[ns]"),
            label_mature=pd.Series(dtype="bool"),
        )
    runtime = dict(config or load_v3_config())
    selected_by_code = {
        str(code): group.sort_values("trade_date").to_dict("records")
        for code, group in selected.groupby("stock_code", observed=True)
    }
    records: list[dict[str, Any]] = []
    for code_value, code_group in features.groupby(
        "stock_code", sort=False, observed=True
    ):
        candidates = selected_by_code.get(str(code_value))
        if not candidates:
            continue
        group = code_group.reset_index(drop=True)
        locations = {
            pd.Timestamp(value): index
            for index, value in enumerate(group["trade_date"])
        }
        blocked_through = -1
        for item in candidates:
            signal_index = locations.get(pd.Timestamp(item["trade_date"]))
            if signal_index is None or signal_index <= blocked_through:
                continue
            outcome = bounded_signal_outcome(
                group,
                signal_index=signal_index,
                config=runtime,
                stop_pct=stop_pct,
                take_profit_pct=take_profit_pct,
                maximum_holding_sessions=maximum_holding_sessions,
                maximum_entry_gap_pct=maximum_entry_gap_pct,
                include_censored=True,
            )
            if outcome is None:
                continue
            records.append({**item, **outcome})
            if bool(outcome["label_mature"]):
                exit_index = locations.get(pd.Timestamp(outcome["exit_date"]), signal_index)
                blocked_through = min(len(group) - 1, exit_index + 5)
            else:
                blocked_through = len(group)
    if records:
        return pd.DataFrame(records)
    return selected.iloc[0:0].copy().assign(
        net_return_pct=pd.Series(dtype="float64"),
        mae_pct=pd.Series(dtype="float64"),
        mfe_pct=pd.Series(dtype="float64"),
        entry_date=pd.Series(dtype="datetime64[ns]"),
        exit_date=pd.Series(dtype="datetime64[ns]"),
        label_mature=pd.Series(dtype="bool"),
    )


def label_candidate_signals(
    features: pd.DataFrame,
    selected: pd.DataFrame,
    *,
    config: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Label actual non-overlapping stock episodes, retaining censoring."""

    if selected.empty:
        return selected.assign(
            net_return_pct=pd.Series(dtype="float64"),
            mae_pct=pd.Series(dtype="float64"),
            mfe_pct=pd.Series(dtype="float64"),
            exit_date=pd.Series(dtype="datetime64[ns]"),
            label_mature=pd.Series(dtype="bool"),
        )
    runtime = dict(config or load_v3_config())
    selected_by_code = {
        str(code): group.sort_values("trade_date").to_dict("records")
        for code, group in selected.groupby("stock_code", observed=True)
    }
    records: list[dict[str, Any]] = []
    for code_value, code_group in features.groupby(
        "stock_code", sort=False, observed=True
    ):
        candidates = selected_by_code.get(str(code_value))
        if not candidates:
            continue
        group = code_group.reset_index(drop=True)
        locations = {
            pd.Timestamp(value): index
            for index, value in enumerate(group["trade_date"])
        }
        blocked_through = -1
        for item in candidates:
            signal_index = locations.get(pd.Timestamp(item["trade_date"]))
            if signal_index is None or signal_index <= blocked_through:
                continue
            sleeve = str(item.get("exit_sleeve") or "trend")
            if sleeve == "reversal":
                outcome = reversal_signal_outcome(
                    group,
                    signal_index=signal_index,
                    config=runtime,
                    include_censored=True,
                )
            else:
                outcome = _dynamic_signal_outcome(
                    group,
                    signal_index=signal_index,
                    config=runtime,
                    initial_stop_pct=float(item["initial_stop_pct"]),
                    include_censored=True,
                )
            if outcome is None:
                continue
            records.append({**item, **outcome})
            if bool(outcome["label_mature"]):
                exit_index = locations.get(pd.Timestamp(outcome["exit_date"]), signal_index)
                cooldown = 5
                blocked_through = min(len(group) - 1, exit_index + cooldown)
            else:
                blocked_through = len(group)
    if records:
        return pd.DataFrame(records)
    return selected.iloc[0:0].copy().assign(
        net_return_pct=pd.Series(dtype="float64"),
        mae_pct=pd.Series(dtype="float64"),
        mfe_pct=pd.Series(dtype="float64"),
        entry_date=pd.Series(dtype="datetime64[ns]"),
        exit_date=pd.Series(dtype="datetime64[ns]"),
        label_mature=pd.Series(dtype="bool"),
    )


def _metric(values: Iterable[float]) -> dict[str, Any]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    result = trade_metrics(finite)
    result["sample_count"] = len(finite)
    result["total_net_return_pct"] = sum(finite)
    return result


def trade_attribution_report(
    trades: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Aggregate after-cost portfolio trades by stable diagnostic axes."""

    rows = [dict(item) for item in trades]
    if not rows:
        return {
            "protocol": "AFTER_COST_PORTFOLIO_ATTRIBUTION_V1",
            "by_year": {},
            "by_regime": {},
            "by_sleeve": {},
            "by_theme": {},
            "by_exit_reason": {},
            "tail_risk": {
                "large_loss_threshold_pct": -8.0,
                "large_loss_count": 0,
                "hard_stop_count": 0,
                "hard_stop_gross_loss_share": 0.0,
                "hard_stop_net_deficit_share": None,
            },
        }

    def grouped(key: str, fallback: str = "UNKNOWN") -> dict[str, Any]:
        values: dict[str, list[float]] = defaultdict(list)
        for item in rows:
            label = str(item.get(key) or fallback)
            values[label].append(float(item["net_return_pct"]))
        return {
            label: _metric(group)
            for label, group in sorted(values.items())
        }

    years: dict[str, list[float]] = defaultdict(list)
    for item in rows:
        years[str(item.get("entry_date") or "UNKNOWN")[:4]].append(
            float(item["net_return_pct"])
        )
    returns = [float(item["net_return_pct"]) for item in rows]
    hard_stops = [
        float(item["net_return_pct"])
        for item in rows
        if str(item.get("exit_reason") or "") == "HARD_STOP"
    ]
    gross_loss = -sum(value for value in returns if value < 0)
    hard_stop_loss = -sum(value for value in hard_stops if value < 0)
    net_total = sum(returns)
    non_hard_stop = [
        float(item["net_return_pct"])
        for item in rows
        if str(item.get("exit_reason") or "") != "HARD_STOP"
    ]
    return {
        "protocol": "AFTER_COST_PORTFOLIO_ATTRIBUTION_V1",
        "by_year": {
            label: _metric(group)
            for label, group in sorted(years.items())
        },
        "by_regime": grouped("research_regime"),
        "by_sleeve": grouped("exit_sleeve"),
        "by_theme": grouped("theme_code", "NO_THEME"),
        "by_exit_reason": grouped("exit_reason"),
        "tail_risk": {
            "large_loss_threshold_pct": -8.0,
            "large_loss_count": sum(value <= -8.0 for value in returns),
            "hard_stop_count": len(hard_stops),
            "hard_stop_total_return_pct": sum(hard_stops),
            "hard_stop_gross_loss_share": (
                hard_stop_loss / gross_loss if gross_loss > 0 else 0.0
            ),
            "hard_stop_net_deficit_share": (
                hard_stop_loss / -net_total if net_total < 0 else None
            ),
            "non_hard_stop_metrics": _metric(non_hard_stop),
            "diagnostic_only": True,
        },
    }


def _portfolio_replay(
    labels: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
    close_by_day: Mapping[pd.Timestamp, pd.Series],
    config: Mapping[str, Any],
    calibration: CalibrationTable | None = None,
    enforce_production_edge: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    account = dict(config["account"])
    policy = dict(config["portfolio"])
    initial_cash = float(account["initial_cash_cny"])
    cash = initial_cash
    positions: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    curve: list[dict[str, Any]] = []
    total_cost = 0.0
    rejection_counts: dict[str, int] = defaultdict(int)
    rejection_rows: list[dict[str, Any]] = []
    last_mark_prices: dict[str, float] = {}
    entries: dict[pd.Timestamp, list[dict[str, Any]]] = defaultdict(list)
    for row in labels.to_dict("records"):
        entry_date = pd.Timestamp(row["entry_date"])
        if start_date <= entry_date.date() <= end_date:
            entries[entry_date].append(row)
    days = [
        day for day in sorted(close_by_day)
        if start_date <= day.date() <= end_date
    ]
    for day in days:
        for code, position in list(positions.items()):
            exit_date = position.get("exit_date")
            if exit_date is None or pd.isna(exit_date) or pd.Timestamp(exit_date) != day:
                continue
            price = float(position["exit_close"])
            value = price * position["quantity"]
            fee = _execution_fee(value, account=account, sell=True)
            total_cost += fee
            cash += value - fee
            net = (
                (price - position["entry_price"]) * position["quantity"]
                - position["buy_fee"]
                - fee
            )
            trades.append({
                "stock_code": code,
                "entry_date": pd.Timestamp(position["entry_date"]).date().isoformat(),
                "exit_date": day.date().isoformat(),
                "net_return_pct": net / position["entry_value"] * 100.0,
                "net_pnl_cny": net,
                "exit_reason": position["exit_reason"],
                "candidate_id": position.get("candidate_id"),
                "research_regime": position.get("research_regime"),
                "exit_sleeve": position.get("exit_sleeve"),
                "theme_code": position.get("theme_code"),
            })
            del positions[code]
        marked_before_entry = cash
        for code, position in positions.items():
            marked_before_entry += (
                last_mark_prices.get(code, float(position["entry_price"]))
                * int(position["quantity"])
            )
        equity_before_entry = max(1.0, marked_before_entry)
        position_weights = {
            code: (
                last_mark_prices.get(code, float(position["entry_price"]))
                * int(position["quantity"])
                / equity_before_entry
            )
            for code, position in positions.items()
        }
        position_quantities = {
            code: int(position["quantity"])
            for code, position in positions.items()
        }
        position_themes = {
            code: _research_theme_codes(position)
            for code, position in positions.items()
        }
        theme_weights: dict[str, float] = defaultdict(float)
        open_risk_cny = 0.0
        for code, position in positions.items():
            for theme in position_themes[code]:
                theme_weights[theme] += position_weights[code]
            mark = last_mark_prices.get(code, float(position["entry_price"]))
            protective_stop = float(position.get("protective_stop") or 0.0)
            risk_per_share = (
                max(0.0, mark - protective_stop)
                if protective_stop > 0
                else mark * 0.08
            )
            open_risk_cny += int(position["quantity"]) * risk_per_share
        candidates = sorted(
            entries.get(day, []),
            key=lambda item: (-float(item["score"]), str(item["stock_code"])),
        )
        constraints = None
        if candidates:
            constraints = PortfolioConstraintState(
                policy=policy,
                equity=equity_before_entry,
                risk_asset_cap=research_risk_asset_cap(
                    candidates[0],
                    config=config,
                ),
                current_theme_weights=theme_weights,
                current_position_weights=position_weights,
                current_position_quantities=position_quantities,
                current_position_themes=position_themes,
                current_open_risk_weight=open_risk_cny / equity_before_entry,
            )
        for item in candidates:
            code = str(item["stock_code"])
            decision_price = float(
                item.get("raw_close")
                or item.get("entry_open")
                or 0.0
            )
            bucket = (
                calibration.bucket_for(float(item["score"]))
                if calibration is not None
                else None
            )
            conservative_return = None
            if bucket is not None:
                uncertainty = max(
                    0.0,
                    float(bucket.expected_return_net_pct)
                    - float(bucket.q10_pct),
                )
                conservative_return = (
                    float(bucket.expected_return_net_pct)
                    - 0.20 * uncertainty
                )
            admission = constraints.admit(
                stock_code=code,
                price=decision_price,
                initial_stop_pct=float(item.get("initial_stop_pct") or -8.0),
                candidate_themes=_research_theme_codes(item),
                conservative_return_pct=conservative_return,
                fees=account if enforce_production_edge else None,
                minimum_edge_to_cost_multiple=(
                    float(
                        config["profit_gate"][
                            "minimum_edge_to_roundtrip_cost_multiple"
                        ]
                    )
                    if enforce_production_edge
                    else None
                ),
                available_cash_cny=cash,
            )
            if not admission.accepted:
                rejection_counts[admission.reason_code] += 1
                if len(rejection_rows) < 500:
                    rejection_rows.append({
                        "trade_date": day.date().isoformat(),
                        "stock_code": code,
                        "reason_code": admission.reason_code,
                    })
                continue
            quantity = int(admission.delta_quantity)
            price = float(item["entry_open"])
            value = quantity * price
            fee = _execution_fee(value, account=account, sell=False)
            if value + fee > cash:
                rejection_counts["CASH_NOT_AVAILABLE_AT_FILL"] += 1
                continue
            cash -= value + fee
            total_cost += fee
            positions[code] = {
                **item,
                "quantity": quantity,
                "entry_price": price,
                "entry_value": value,
                "buy_fee": fee,
                "protective_stop": price
                * (
                    1.0
                    + float(item.get("initial_stop_pct") or -8.0) / 100.0
                ),
            }
        marked = cash
        prices = close_by_day[day]
        for code, position in positions.items():
            price = prices.get(code)
            if price is not None and math.isfinite(float(price)):
                marked += float(price) * position["quantity"]
                last_mark_prices[code] = float(price)
            else:
                marked += position["entry_price"] * position["quantity"]
        curve.append({
            "trade_date": day.date().isoformat(),
            "equity": marked,
            "position_count": len(positions),
        })
    returns = [float(item["net_return_pct"]) for item in trades]
    metrics = _metric(returns)
    final_equity = curve[-1]["equity"] if curve else initial_cash
    drawdown = maximum_drawdown(item["equity"] for item in curve)
    metrics.update({
        "trade_count": len(trades),
        "initial_cash_cny": initial_cash,
        "final_equity_cny": final_equity,
        "net_profit_cny": final_equity - initial_cash,
        "total_return_pct": (final_equity / initial_cash - 1.0) * 100.0,
        "maximum_drawdown_pct": abs(float(drawdown or 0.0)),
        "total_cost_cny": total_cost,
        "portfolio_constraint_protocol": (
            "PRODUCTION_PORTFOLIO_CONSTRAINT_STATE_V1"
        ),
        "constraint_rejection_counts": dict(sorted(rejection_counts.items())),
        "constraint_rejection_rows": rejection_rows,
    })
    return metrics, trades, curve


def evaluate_fold(
    labels: pd.DataFrame,
    *,
    fold: Mapping[str, str],
    candidate_id: str,
    close_by_day: Mapping[pd.Timestamp, pd.Series],
    config: Mapping[str, Any] | None = None,
    bucket_count: int | None = None,
) -> dict[str, Any]:
    runtime = dict(config or load_v3_config())
    training_start = pd.Timestamp(fold["training_start"])
    training_end = pd.Timestamp(fold["training_end"])
    validation_start = pd.Timestamp(fold["validation_start"])
    validation_end = pd.Timestamp(fold["validation_end"])
    mature = labels[
        labels["label_mature"].fillna(False)
        & labels["net_return_pct"].notna()
    ]
    train = mature[
        (mature["trade_date"] >= training_start)
        & (mature["trade_date"] <= training_end)
        & (mature["exit_date"] <= training_end)
    ].copy()
    calibration = fit_calibration(
        candidate_id,
        train[["score", "net_return_pct", "mae_pct", "mfe_pct"]].to_dict("records"),
        model_version=f"{candidate_id}-{fold['name']}",
        bucket_count=int(
            bucket_count
            if bucket_count is not None
            else runtime.get("calibration", {}).get("bucket_count", 5)
        ),
    )
    validation_candidates = labels[
        (labels["trade_date"] >= validation_start)
        & (labels["trade_date"] <= validation_end)
    ].copy()
    raw_outcomes = validation_candidates[
        validation_candidates["label_mature"].fillna(False)
        & validation_candidates["net_return_pct"].notna()
    ]
    raw_portfolio, raw_trades, _ = _portfolio_replay(
        validation_candidates,
        start_date=validation_start.date(),
        end_date=validation_end.date(),
        close_by_day=close_by_day,
        config=runtime,
        calibration=calibration,
        enforce_production_edge=False,
    )
    direction_valid = calibration.has_valid_score_direction()
    if direction_valid and not validation_candidates.empty:
        accepted = validation_candidates[
            validation_candidates["score"].map(
                lambda score: _validated_bucket(calibration, float(score))
            ).astype(bool)
        ].copy()
    else:
        accepted = validation_candidates.iloc[0:0].copy()
    accepted_outcomes = accepted[
        accepted["label_mature"].fillna(False)
        & accepted["net_return_pct"].notna()
    ]
    portfolio, trades, curve = _portfolio_replay(
        accepted,
        start_date=validation_start.date(),
        end_date=validation_end.date(),
        close_by_day=close_by_day,
        config=runtime,
        calibration=calibration,
        enforce_production_edge=True,
    )
    return {
        "name": fold["name"],
        "training_sample_count": len(train),
        "calibration": calibration.as_dict(),
        "calibration_direction_valid": direction_valid,
        "raw_validation": _metric(raw_outcomes["net_return_pct"].tolist()),
        "raw_portfolio": raw_portfolio,
        "raw_trades": raw_trades,
        "accepted_validation": _metric(accepted_outcomes["net_return_pct"].tolist()),
        "portfolio": portfolio,
        "trades": trades,
        "equity_curve": curve,
        "attribution": trade_attribution_report(trades),
    }


def aggregate_candidate(
    folds: Iterable[Mapping[str, Any]],
    *,
    config: Mapping[str, Any] | None = None,
    required_positive_folds: int = 4,
    minimum_fold_profit_factor: float = 0.9,
) -> dict[str, Any]:
    runtime = dict(config or load_v3_config())
    fold_list = list(folds)
    validation_returns = [
        float(trade["net_return_pct"])
        for fold in fold_list
        for trade in fold["trades"]
    ]
    validation = _metric(validation_returns)
    net_profit = sum(float(fold["portfolio"]["net_profit_cny"]) for fold in fold_list)
    portfolio = dict(validation)
    portfolio.update({
        "trade_count": len(validation_returns),
        "net_profit_cny": net_profit,
        "maximum_drawdown_pct": max(
            (float(fold["portfolio"]["maximum_drawdown_pct"]) for fold in fold_list),
            default=0.0,
        ),
    })
    positive_folds = sum(
        float(fold["portfolio"].get("net_profit_cny") or 0.0) > 0
        and float(fold["portfolio"].get("profit_factor") or 0.0)
        >= minimum_fold_profit_factor
        for fold in fold_list
    )
    blocks = list(model_gate_failures(
        validation=validation,
        portfolio=portfolio,
        config=runtime,
    ))
    if positive_folds < required_positive_folds:
        blocks.append("POSITIVE_OUTER_FOLD_COUNT_TOO_LOW")
    if not all(bool(fold["calibration_direction_valid"]) for fold in fold_list):
        blocks.append("CALIBRATION_DIRECTION_FAILED_IN_OUTER_FOLD")
    return {
        "validation": validation,
        "portfolio": portfolio,
        "positive_outer_folds": positive_folds,
        "outer_fold_count": len(fold_list),
        "gate_status": "PASS" if not blocks else "BLOCK",
        "block_reasons": list(dict.fromkeys(blocks)),
        "attribution": trade_attribution_report(
            trade
            for fold in fold_list
            for trade in fold["trades"]
        ),
    }
