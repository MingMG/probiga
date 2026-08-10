from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class RawSignal:
    stock_code: str
    stock_name: str
    strategy_key: str
    horizon_days: int
    score: float
    feature_time: datetime
    valid_until: datetime
    initial_stop_pct: float
    theme_code: str = ""
    status: str = "SCORED"
    reasons: tuple[str, ...] = ()
    features: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AlphaForecast:
    stock_code: str
    stock_name: str
    strategy_key: str
    horizon_days: int
    expected_return_net_pct: float | None
    return_q10_pct: float | None
    return_q50_pct: float | None
    return_q90_pct: float | None
    probability_positive: float | None
    expected_mae_pct: float | None
    expected_mfe_pct: float | None
    profit_factor: float | None
    payoff_ratio: float | None
    sample_count: int
    confidence: float
    status: str
    feature_time: datetime
    valid_until: datetime
    initial_stop_pct: float
    theme_code: str = ""
    raw_score: float | None = None
    reasons: tuple[str, ...] = ()
    model_version: str = ""
    dataset_hash: str = ""
    features: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TradeHypothesis:
    hypothesis_key: str
    run_uid: str
    trade_date: str
    scope_type: str
    scope_code: str
    scope_name: str
    direction: str
    state: str
    probability: float
    prior_probability: float
    probability_kind: str
    confidence: float
    score: float
    horizon_minutes: int
    alpha_half_life_minutes: int
    proposed_action: str
    max_position_weight: float
    theme_code: str
    role: str
    thesis: str
    counter_thesis: str
    supporting_evidence: tuple[str, ...]
    opposing_evidence: tuple[str, ...]
    triggers: tuple[str, ...]
    invalidations: tuple[str, ...]
    strategy_keys: tuple[str, ...]
    feature_time: datetime
    valid_until: datetime
    source_forecast_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HypothesisEvidence:
    hypothesis_key: str
    observed_at: datetime
    evidence_type: str
    polarity: str
    strength: float
    source: str
    summary: str
    probability_before: float
    probability_after: float
    state_before: str
    state_after: str
    payload: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RegimeProbabilities:
    probabilities: dict[str, float]
    risk_asset_cap: float
    confidence: float
    quality_status: str
    evidence: tuple[str, ...]

    @property
    def dominant_state(self) -> str:
        if not self.probabilities:
            return "DATA_BLOCKED"
        return max(self.probabilities, key=self.probabilities.get)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["dominant_state"] = self.dominant_state
        return payload


@dataclass(frozen=True)
class ConsensusForecast:
    stock_code: str
    stock_name: str
    expected_return_net_pct: float
    conservative_return_pct: float
    probability_positive: float
    expected_mae_pct: float
    profit_factor: float
    payoff_ratio: float
    confidence: float
    selection_score: float
    strategy_keys: tuple[str, ...]
    primary_strategy_key: str
    theme_code: str
    initial_stop_pct: float
    evidence: tuple[str, ...]
    theme_codes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioTarget:
    stock_code: str
    stock_name: str
    target_weight: float
    target_value: float
    target_quantity: int
    estimated_roundtrip_cost_pct: float
    expected_return_net_pct: float
    conservative_return_pct: float
    expected_mae_pct: float
    theme_code: str
    strategy_keys: tuple[str, ...]
    reason: str
    theme_codes: tuple[str, ...] = ()
    primary_strategy_key: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioDecision:
    targets: tuple[PortfolioTarget, ...]
    rejected: tuple[dict[str, Any], ...]
    target_cash: float
    target_risk_asset_weight: float
    expected_portfolio_return_pct: float
    worst_case_loss_cny: float
    status: str
    estimated_one_way_turnover_weight: float = 0.0
    current_risk_asset_weight: float = 0.0
    opportunity_audit: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "targets": [item.as_dict() for item in self.targets],
            "rejected": list(self.rejected),
            "target_cash": self.target_cash,
            "target_risk_asset_weight": self.target_risk_asset_weight,
            "expected_portfolio_return_pct": (
                self.expected_portfolio_return_pct
            ),
            "worst_case_loss_cny": self.worst_case_loss_cny,
            "status": self.status,
            "estimated_one_way_turnover_weight": (
                self.estimated_one_way_turnover_weight
            ),
            "current_risk_asset_weight": (
                self.current_risk_asset_weight
            ),
            "opportunity_audit": self.opportunity_audit,
        }
