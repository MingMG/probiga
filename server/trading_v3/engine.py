from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable

from .calibration import CalibrationTable
from .config import load_v3_config
from .domain import AlphaForecast, PortfolioDecision, RawSignal
from .hypotheses import strategy_weights_for_regime
from .portfolio import (
    _paper_opportunity_audit,
    add_paper_discovery_targets,
    build_consensus,
    optimize_retail_portfolio,
)
from .regime import classify_regime_probabilities
from .sleeves import SLEEVE_BUILDERS


class TradingV3Engine:
    """Deterministic V3 engine.

    Raw strategy scores are never tradable on their own. A strategy becomes
    actionable only when an out-of-sample calibration bucket satisfies the
    frozen positive-expectancy gates.
    """

    def __init__(
        self,
        calibrations: dict[str, CalibrationTable] | None = None,
    ) -> None:
        self.config = load_v3_config()
        self.calibrations = calibrations or {}

    def forecast(self, signal: RawSignal) -> AlphaForecast:
        if signal.status != "SCORED":
            return AlphaForecast(
                stock_code=signal.stock_code,
                stock_name=signal.stock_name,
                strategy_key=signal.strategy_key,
                horizon_days=signal.horizon_days,
                expected_return_net_pct=None,
                return_q10_pct=None,
                return_q50_pct=None,
                return_q90_pct=None,
                probability_positive=None,
                expected_mae_pct=None,
                expected_mfe_pct=None,
                profit_factor=None,
                payoff_ratio=None,
                sample_count=0,
                confidence=0.0,
                status=signal.status,
                feature_time=signal.feature_time,
                valid_until=signal.valid_until,
                initial_stop_pct=signal.initial_stop_pct,
                theme_code=signal.theme_code,
                raw_score=signal.score,
                reasons=signal.reasons,
                features=signal.features,
            )
        table = self.calibrations.get(signal.strategy_key)
        required_versions = dict(
            self.config.get("calibration_version_tokens") or {}
        )
        required_version = str(
            required_versions.get(signal.strategy_key)
            or self.config.get("calibration_version_token")
            or ""
        )
        if (
            table is not None
            and required_version
            and required_version not in table.model_version
        ):
            table = None
            version_mismatch = True
        else:
            version_mismatch = False
        direction_failed = bool(
            table is not None
            and not table.has_valid_score_direction()
        )
        minimum_bucket_count = int(
            self.config.get("calibration", {}).get(
                "minimum_bucket_count",
                2,
            )
        )
        resolution_failed = bool(
            table is not None
            and len(table.buckets) < minimum_bucket_count
        )
        score_out_of_range = bool(
            table is not None
            and not direction_failed
            and not resolution_failed
            and not table.contains_score(signal.score)
        )
        bucket = (
            table.bucket_for(signal.score)
            if (
                table is not None
                and not direction_failed
                and not resolution_failed
            )
            else None
        )
        if (
            not table
            or not bucket
            or direction_failed
            or resolution_failed
        ):
            discovery = dict(
                self.config.get("paper_discovery") or {}
            )
            experimental = bool(
                discovery.get("enabled")
                and signal.strategy_key
                in set(
                    discovery.get(
                        "single_sleeve_strategy_keys",
                        (),
                    )
                )
            )
            if version_mismatch:
                status = "RESEARCH_ONLY_MODEL_VERSION_MISMATCH"
                reason = (
                    "激活校准属于旧公式版本，已隔离；"
                    "仅允许模拟盘重新收集证据"
                )
            elif direction_failed:
                status = "RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED"
                reason = (
                    "历史校准出现高分组反而亏损，分数排序失真；"
                    "禁止自动买入并要求重新校准"
                )
            elif resolution_failed:
                status = "RESEARCH_ONLY_CALIBRATION_TOO_COARSE"
                reason = (
                    f"校准仅有{len(table.buckets)}个分数桶，"
                    f"低于最少{minimum_bucket_count}桶；"
                    "无法证明分数排序有效"
                )
            elif score_out_of_range:
                status = "RESEARCH_ONLY_SCORE_OUT_OF_RANGE"
                score_range = table.score_range
                reason = (
                    f"实时分数{signal.score:.4f}超出校准范围"
                    f"{score_range[0]:.4f}—{score_range[1]:.4f}；"
                    "禁止使用最近桶外推"
                )
            elif experimental:
                status = "PAPER_DISCOVERY_CANDIDATE"
                reason = (
                    "尚无足够前向样本证明正期望；满足冻结公式时，"
                    "只允许ProBigA模拟盘小仓试错并进入反事实复盘"
                )
            else:
                status = "RESEARCH_ONLY_UNCALIBRATED"
                reason = "没有样本外校准，不允许进入正式组合"
            return AlphaForecast(
                stock_code=signal.stock_code,
                stock_name=signal.stock_name,
                strategy_key=signal.strategy_key,
                horizon_days=signal.horizon_days,
                expected_return_net_pct=None,
                return_q10_pct=None,
                return_q50_pct=None,
                return_q90_pct=None,
                probability_positive=None,
                expected_mae_pct=None,
                expected_mfe_pct=None,
                profit_factor=None,
                payoff_ratio=None,
                sample_count=0,
                confidence=0.0,
                status=status,
                feature_time=signal.feature_time,
                valid_until=signal.valid_until,
                initial_stop_pct=signal.initial_stop_pct,
                theme_code=signal.theme_code,
                raw_score=signal.score,
                reasons=signal.reasons + (reason,),
                model_version=(
                    table.model_version if table is not None else ""
                ),
                dataset_hash=(
                    table.dataset_hash if table is not None else ""
                ),
                features=signal.features,
            )
        gates = self.config["profit_gate"]
        pf = (
            float(bucket.profit_factor)
            if bucket.profit_factor is not None
            and math.isfinite(float(bucket.profit_factor))
            else None
        )
        payoff = (
            float(bucket.payoff_ratio)
            if bucket.payoff_ratio is not None
            and math.isfinite(float(bucket.payoff_ratio))
            else None
        )
        positive = (
            bucket.sample_count >= int(gates["minimum_oos_samples"])
            and bucket.expected_return_net_pct
            > float(gates["minimum_expected_return_net_pct"])
            and pf is not None
            and pf >= float(gates["minimum_profit_factor"])
            and payoff is not None
            and payoff >= float(gates["minimum_payoff_ratio"])
        )
        confidence = min(
            1.0,
            bucket.sample_count
            / max(1, int(gates["minimum_oos_samples"]) * 3),
        )
        return AlphaForecast(
            stock_code=signal.stock_code,
            stock_name=signal.stock_name,
            strategy_key=signal.strategy_key,
            horizon_days=signal.horizon_days,
            expected_return_net_pct=round(
                bucket.expected_return_net_pct,
                6,
            ),
            return_q10_pct=round(bucket.q10_pct, 6),
            return_q50_pct=round(bucket.q50_pct, 6),
            return_q90_pct=round(bucket.q90_pct, 6),
            probability_positive=round(
                bucket.probability_positive,
                6,
            ),
            expected_mae_pct=round(bucket.expected_mae_pct, 6),
            expected_mfe_pct=round(bucket.expected_mfe_pct, 6),
            profit_factor=pf,
            payoff_ratio=payoff,
            sample_count=bucket.sample_count,
            confidence=round(confidence, 6),
            status=(
                "VALIDATED_POSITIVE"
                if positive
                else "RESEARCH_ONLY_PROFIT_GATE_FAILED"
            ),
            feature_time=signal.feature_time,
            valid_until=signal.valid_until,
            initial_stop_pct=signal.initial_stop_pct,
            theme_code=signal.theme_code,
            raw_score=signal.score,
            reasons=signal.reasons,
            model_version=table.model_version,
            dataset_hash=table.dataset_hash,
            features=signal.features,
        )

    def evaluate_stock(
        self,
        stock_code: str,
        stock_name: str,
        features: dict[str, Any],
        feature_time: datetime,
        valid_until: datetime,
    ) -> tuple[AlphaForecast, ...]:
        forecasts, _theme_signals = self.evaluate_stock_with_theme_signals(
            stock_code,
            stock_name,
            features,
            feature_time,
            valid_until,
        )
        return forecasts

    def evaluate_stock_with_theme_signals(
        self,
        stock_code: str,
        stock_name: str,
        features: dict[str, Any],
        feature_time: datetime,
        valid_until: datetime,
    ) -> tuple[tuple[AlphaForecast, ...], tuple[dict[str, Any], ...]]:
        """Evaluate theme-led sleeves once per independent theme candidate."""

        policy = dict(self.config.get("theme_signals") or {})
        theme_strategies = set(
            policy.get("strategy_keys")
            or ("theme_diffusion", "weak_market_structural_mainline")
        )
        theme_strategies &= set(SLEEVE_BUILDERS)
        raw_candidates = features.get("theme_signal_candidates") or ()
        candidates = [
            dict(item)
            for item in raw_candidates
            if isinstance(item, dict)
            and str(item.get("theme_feature_key") or "")
        ]
        common_features = dict(features)
        common_features.pop("theme_signal_candidates", None)
        selected: dict[str, AlphaForecast] = {}
        for strategy_key, builder in SLEEVE_BUILDERS.items():
            if strategy_key in theme_strategies and candidates:
                continue
            selected[strategy_key] = self.forecast(
                builder(
                    stock_code,
                    stock_name,
                    common_features,
                    feature_time,
                    valid_until,
                )
            )

        evaluated: list[tuple[dict[str, Any], str, AlphaForecast]] = []
        for candidate in candidates:
            candidate_features = {**common_features, **candidate}
            for strategy_key in sorted(theme_strategies):
                signal = SLEEVE_BUILDERS[strategy_key](
                    stock_code,
                    stock_name,
                    candidate_features,
                    feature_time,
                    valid_until,
                )
                evaluated.append((
                    candidate,
                    signal.status,
                    self.forecast(signal),
                ))

        for strategy_key in sorted(theme_strategies):
            strategy_rows = [
                row for row in evaluated if row[2].strategy_key == strategy_key
            ]
            if not strategy_rows:
                continue
            best = min(
                strategy_rows,
                key=lambda row: (
                    0 if row[1] == "SCORED" else 1,
                    -float(row[2].raw_score or 0.0),
                    str(row[0].get("theme_feature_key") or ""),
                ),
            )
            selected[strategy_key] = best[2]

        theme_signal_rows = []
        theme_evidence_stock_keys = {
            "market_return_20d_pct",
            "return_5d_pct",
            "return_20d_pct",
            "relative_strength_20d_pct",
            "amount_ratio_5_20",
            "latest_change_pct",
            "distance_ma20_pct",
            "atr_14d_pct",
            "entry_eligible",
            "latest_tradable",
            "data_quality_status",
            "market_data_quality_status",
            "theme_feature_quality_status",
            "qmt_attestation_current",
        }
        selected_feature_keys = {
            strategy_key: str(
                forecast.features.get("theme_feature_key") or ""
            )
            for strategy_key, forecast in selected.items()
        }
        for candidate, signal_status, forecast in evaluated:
            feature_key = str(candidate["theme_feature_key"])
            evidence_keys = set(candidate) | theme_evidence_stock_keys
            evidence_features = {
                key: value
                for key, value in forecast.features.items()
                if key in evidence_keys
            }
            theme_signal_rows.append({
                "stock_code": stock_code,
                "short_name": stock_name,
                "strategy_key": forecast.strategy_key,
                "theme_feature_key": feature_key,
                "theme_code": str(candidate.get("theme_code") or ""),
                "theme_name": str(candidate.get("theme_name") or ""),
                "theme_source": str(candidate.get("theme_source") or ""),
                "theme_cluster_keys": list(
                    candidate.get("theme_cluster_keys") or ()
                ),
                "horizon_days": forecast.horizon_days,
                "raw_score": forecast.raw_score,
                "signal_status": signal_status,
                "forecast_status": forecast.status,
                "expected_return_net_pct": (
                    forecast.expected_return_net_pct
                ),
                "selected_as_primary": int(
                    selected_feature_keys.get(forecast.strategy_key)
                    == feature_key
                ),
                "feature_time": forecast.feature_time,
                "valid_until": forecast.valid_until,
                "features": evidence_features,
            })
        ordered = tuple(
            selected[key]
            for key in SLEEVE_BUILDERS
            if key in selected
        )
        return ordered, tuple(theme_signal_rows)

    def decide(
        self,
        forecasts: Iterable[AlphaForecast],
        *,
        market_features: dict[str, Any],
        prices: dict[str, float],
        equity: float,
        current_theme_weights: dict[str, float] | None = None,
        current_position_weights: dict[str, float] | None = None,
        current_position_quantities: dict[str, int] | None = None,
        current_position_themes: dict[str, tuple[str, ...]] | None = None,
        current_paper_discovery_codes: set[str] | None = None,
        current_open_risk_weight: float = 0.0,
        strategy_weights: dict[str, float] | None = None,
        allow_paper_discovery: bool = False,
        paper_discovery_learning: dict[str, Any] | None = None,
        opportunity_audit_forecasts: Iterable[AlphaForecast] | None = None,
        decision_at: datetime | None = None,
    ) -> dict[str, Any]:
        regime = classify_regime_probabilities(market_features)
        if regime.quality_status != "PASS":
            audit_forecasts = tuple(
                opportunity_audit_forecasts
                if opportunity_audit_forecasts is not None
                else forecasts
            )
            opportunity_audit = _paper_opportunity_audit(
                [],
                forecasts=audit_forecasts,
                targets=[],
                rejected=[],
                config=self.config,
            )
            opportunity_audit["selection_blocked_by_data_quality"] = list(
                regime.evidence
            )
            warnings = list(opportunity_audit.get("warnings") or [])
            if "DATA_QUALITY_BLOCKED" not in warnings:
                warnings.insert(0, "DATA_QUALITY_BLOCKED")
            opportunity_audit["warnings"] = warnings
            opportunity_audit["status"] = "ATTENTION"
            empty = PortfolioDecision(
                targets=(),
                rejected=(),
                target_cash=equity,
                target_risk_asset_weight=0.0,
                expected_portfolio_return_pct=0.0,
                worst_case_loss_cny=0.0,
                status="DATA_BLOCKED",
                opportunity_audit=opportunity_audit,
            )
            return {
                "regime": regime.as_dict(),
                "consensus": [],
                "portfolio": empty.as_dict(),
            }
        forecasts = tuple(forecasts)
        evaluation_time = decision_at or max(
            (item.feature_time for item in forecasts),
            default=datetime.min,
        )
        fresh_forecasts = tuple(
            item
            for item in forecasts
            if item.feature_time <= evaluation_time <= item.valid_until
        )
        resolved_strategy_weights = (
            strategy_weights
            if strategy_weights is not None
            else strategy_weights_for_regime(regime)
        )
        consensus = build_consensus(
            fresh_forecasts,
            strategy_weights=resolved_strategy_weights,
        )
        portfolio = optimize_retail_portfolio(
            consensus,
            prices=prices,
            equity=equity,
            current_theme_weights=current_theme_weights,
            current_position_weights=current_position_weights,
            current_position_quantities=current_position_quantities,
            current_position_themes=current_position_themes,
            current_open_risk_weight=current_open_risk_weight,
            regime=regime,
        )
        if allow_paper_discovery:
            portfolio = add_paper_discovery_targets(
                portfolio,
                fresh_forecasts,
                prices=prices,
                equity=equity,
                current_theme_weights=current_theme_weights,
                current_position_weights=current_position_weights,
                current_position_quantities=current_position_quantities,
                current_position_themes=current_position_themes,
                current_paper_discovery_codes=(
                    current_paper_discovery_codes
                ),
                regime=regime,
                learning_context=paper_discovery_learning,
                opportunity_audit_forecasts=(
                    opportunity_audit_forecasts
                ),
            )
        return {
            "regime": regime.as_dict(),
            "strategy_weights": resolved_strategy_weights,
            "expired_forecast_count": len(forecasts) - len(fresh_forecasts),
            "consensus": [item.as_dict() for item in consensus],
            "portfolio": portfolio.as_dict(),
        }
