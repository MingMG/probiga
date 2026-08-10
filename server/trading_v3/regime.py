from __future__ import annotations

import math
from typing import Any

from .config import load_v3_config
from .domain import RegimeProbabilities


REQUIRED = (
    "market_return_20d_pct",
    "market_breadth_pct",
    "breadth_change_5d_pct",
    "realized_volatility_20d_pct",
    "limit_down_ratio_pct",
)


def _softmax(values: dict[str, float]) -> dict[str, float]:
    top = max(values.values())
    exps = {key: math.exp(value - top) for key, value in values.items()}
    total = sum(exps.values())
    return {key: value / total for key, value in exps.items()}


def core_regime_probabilities(
    *,
    market_return_20d_pct: float,
    market_breadth_pct: float,
    breadth_change_5d_pct: float,
    realized_volatility_20d_pct: float,
    limit_down_ratio_pct: float,
) -> dict[str, float]:
    """Shared market-only state model used by research and production."""

    ret20 = float(market_return_20d_pct)
    breadth = float(market_breadth_pct)
    breadth_delta = float(breadth_change_5d_pct)
    volatility = float(realized_volatility_20d_pct)
    limit_down = float(limit_down_ratio_pct)
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
    return _softmax(logits)


def classify_regime_probabilities(
    features: dict[str, Any],
) -> RegimeProbabilities:
    missing = [key for key in REQUIRED if features.get(key) is None]
    if missing:
        return RegimeProbabilities(
            probabilities={},
            risk_asset_cap=0.0,
            confidence=1.0,
            quality_status="BLOCK",
            evidence=("缺少市场特征：" + "、".join(missing),),
        )
    quality_policy = load_v3_config().get("data_quality", {})
    minimum_stock_count = int(
        quality_policy.get("minimum_market_stock_count", 2000)
    )
    minimum_coverage = float(
        quality_policy.get("minimum_market_coverage_ratio", 0.85)
    )
    maximum_concept_age = int(
        quality_policy.get("maximum_concept_snapshot_age_days", 5)
    )
    require_current_qmt_attestation = bool(
        quality_policy.get("require_current_qmt_attestation", True)
    )
    eligible_count = features.get("market_eligible_stock_count")
    latest_coverage = features.get("market_latest_coverage_ratio")
    tradable_coverage = features.get("market_tradable_coverage_ratio")
    concept_snapshot_age = features.get("concept_snapshot_age_days")
    coverage_failures = []
    if (
        require_current_qmt_attestation
        and "qmt_attestation_current" in features
        and not bool(features.get("qmt_attestation_current"))
    ):
        status = str(
            features.get("qmt_attestation_status") or "MISSING"
        )
        coverage_failures.append(
            f"QMT_DAILY_KLINE_ATTESTATION_{status}"
        )
    if eligible_count is not None and float(eligible_count) < minimum_stock_count:
        coverage_failures.append(
            f"MARKET_STOCK_COUNT_{int(float(eligible_count))}_LT_{minimum_stock_count}"
        )
    if latest_coverage is not None and float(latest_coverage) < minimum_coverage:
        coverage_failures.append(
            "MARKET_LATEST_COVERAGE_"
            f"{float(latest_coverage):.1%}_LT_{minimum_coverage:.1%}"
        )
    if tradable_coverage is not None and float(tradable_coverage) < minimum_coverage:
        coverage_failures.append(
            "MARKET_TRADABLE_COVERAGE_"
            f"{float(tradable_coverage):.1%}_LT_{minimum_coverage:.1%}"
        )
    if concept_snapshot_age is None and "concept_snapshot_age_days" in features:
        coverage_failures.append("CONCEPT_SNAPSHOT_MISSING")
    elif (
        concept_snapshot_age is not None
        and float(concept_snapshot_age) > maximum_concept_age
    ):
        coverage_failures.append(
            "CONCEPT_SNAPSHOT_AGE_"
            f"{int(float(concept_snapshot_age))}D_GT_{maximum_concept_age}D"
        )
    if coverage_failures:
        return RegimeProbabilities(
            probabilities={},
            risk_asset_cap=0.0,
            confidence=1.0,
            quality_status="BLOCK",
            evidence=tuple(coverage_failures),
        )
    ret20 = float(features["market_return_20d_pct"])
    breadth = float(features["market_breadth_pct"])
    breadth_delta = float(features["breadth_change_5d_pct"])
    volatility = float(features["realized_volatility_20d_pct"])
    limit_down = float(features["limit_down_ratio_pct"])
    concentration = float(features.get("sector_concentration_pct") or 0.0)
    policy_support = float(features.get("policy_support_score") or 0.0)
    news_risk = float(features.get("news_risk_score") or 0.0)
    overseas_risk = float(
        features.get("overseas_risk_score") or 0.0
    )
    probabilities = core_regime_probabilities(
        market_return_20d_pct=ret20,
        market_breadth_pct=breadth,
        breadth_change_5d_pct=breadth_delta,
        realized_volatility_20d_pct=volatility,
        limit_down_ratio_pct=limit_down,
    )
    caps = load_v3_config()["regime"]["risk_asset_caps"]
    base_cap = sum(
        probabilities[state] * float(caps[state])
        for state in probabilities
    )
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
    cap = base_cap * context_multiplier
    ordered = sorted(probabilities.values(), reverse=True)
    confidence = ordered[0] - ordered[1] if len(ordered) > 1 else 1.0
    dominant = max(probabilities, key=probabilities.get)
    context_text = (
        f"外围风险{overseas_risk:.1%}"
        if features.get("overseas_risk_score") is not None
        else "外围结构化行情缺失，仅使用已落库快讯且不补猜"
    )
    return RegimeProbabilities(
        probabilities={
            key: round(value, 8)
            for key, value in probabilities.items()
        },
        risk_asset_cap=round(cap, 6),
        confidence=round(confidence, 6),
        quality_status="PASS",
        evidence=(
            f"市场状态采用概率混合，当前最高为{dominant}"
            f"（{probabilities[dominant]:.1%}）",
            f"风险资产上限按状态概率加权为{cap:.1%}",
            (
                f"消息上下文：政策支持{policy_support:.1%}，"
                f"新闻风险{news_risk:.1%}，{context_text}"
            ),
        ),
    )
