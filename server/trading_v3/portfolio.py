from __future__ import annotations

import math
from collections import defaultdict
from statistics import mean
from typing import Any, Iterable

from .config import load_v3_config
from .domain import (
    AlphaForecast,
    ConsensusForecast,
    PortfolioDecision,
    PortfolioTarget,
    RegimeProbabilities,
)
from .portfolio_constraints import (
    PortfolioConstraintState,
    estimate_roundtrip_cost_pct as _shared_roundtrip_cost_pct,
)
from .theme_features import cluster_themes_by_component_overlap


def _finite(value: float | None, default: float = 0.0) -> float:
    if value is None or not math.isfinite(float(value)):
        return default
    return float(value)


def _forecast_theme_codes(
    forecast: AlphaForecast,
) -> tuple[str, ...]:
    raw = (
        forecast.features.get("all_theme_cluster_keys")
        or forecast.features.get("theme_cluster_keys")
        or forecast.features.get("theme_codes")
    )
    codes = {
        str(item)
        for item in (
            raw if isinstance(raw, (list, tuple, set)) else ()
        )
        if str(item)
    }
    if forecast.theme_code:
        codes.add(str(forecast.theme_code))
    return tuple(sorted(codes))


def _paper_probe_quantity(
    *,
    equity: float,
    price: float,
    desired_weight: float,
    maximum_weight: float,
    preferred_minimum_order_cny: float,
    absolute_minimum_lot_order_cny: float,
) -> tuple[int, str]:
    """Choose a board lot without silently losing small paper probes.

    The preferred minimum keeps commission drag economical.  A paper-only
    order may remain below it when the floor lot is still above the absolute
    research minimum.  Rounding up is allowed only inside the supplied hard
    portfolio, theme, turnover and per-position weight cap.
    """

    if equity <= 0 or price <= 0 or desired_weight <= 0 or maximum_weight <= 0:
        return 0, "PAPER_DISCOVERY_NO_ORDER_CAPACITY"
    desired_value = equity * desired_weight
    floor_quantity = math.floor(desired_value / price / 100) * 100
    floor_value = floor_quantity * price
    if floor_quantity > 0 and floor_value >= preferred_minimum_order_cny:
        return floor_quantity, ""
    preferred_quantity = (
        math.ceil(preferred_minimum_order_cny / price / 100) * 100
    )
    preferred_weight = preferred_quantity * price / equity
    if preferred_quantity > 0 and preferred_weight <= maximum_weight + 1e-12:
        return preferred_quantity, "PAPER_DISCOVERY_LOT_ROUNDED_UP"
    if (
        floor_quantity > 0
        and floor_value >= absolute_minimum_lot_order_cny
    ):
        return floor_quantity, "PAPER_DISCOVERY_SUBMINIMUM_RESEARCH_LOT"
    return 0, "PAPER_DISCOVERY_LOT_NOT_ECONOMIC"


def _diversified_dynamic_theme_rows(
    rows: Iterable[dict[str, Any]],
    *,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    """Prefer different leading stocks before repeating one stock's tags.

    A strong stock can belong to dozens of provider concepts.  Ranking only
    by its maximum signal would therefore turn a dynamic research table into
    twenty aliases for the same security.  The first pass keeps the best
    ranked theme for each distinct leading stock; a second pass fills any
    remaining capacity so small universes are still fully represented.
    """

    maximum = max(0, int(limit))
    if maximum == 0:
        return [], 0
    ordered = list(rows)
    selected: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    leader_codes: set[str] = set()
    for item in ordered:
        leader = str((item.get("top_signal") or {}).get("stock_code") or "")
        if leader and leader in leader_codes:
            deferred.append(item)
            continue
        selected.append(item)
        if leader:
            leader_codes.add(leader)
        if len(selected) >= maximum:
            return selected, len(deferred)
    omitted_duplicates = len(deferred)
    for item in deferred:
        if len(selected) >= maximum:
            break
        selected.append(item)
    return selected, omitted_duplicates


def _paper_opportunity_audit(
    candidates: list[tuple[float, str, AlphaForecast, list[AlphaForecast]]],
    *,
    forecasts: Iterable[AlphaForecast],
    targets: list[PortfolioTarget],
    rejected: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Explain full-universe thematic coverage before users find omissions."""

    research = dict(config.get("theme_research") or {})
    alert_score = float(research.get("minimum_alert_score", 0.82))
    max_theme_rows = int(research.get("maximum_audit_theme_rows", 20))
    max_unselected_rows = int(
        research.get("maximum_audit_unselected_rows", 20)
    )
    target_codes = {str(item.stock_code) for item in targets}
    rejection_by_code = {
        str(item.get("stock_code")): item for item in rejected
    }
    theme_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    candidate_rows: list[dict[str, Any]] = []
    selected_theme_sets: list[set[str]] = []

    forecast_rows = []
    for forecast in forecasts:
        row = {
            "stock_code": str(forecast.stock_code),
            "short_name": forecast.stock_name,
            "strategy_key": forecast.strategy_key,
            "score": (
                round(float(forecast.raw_score), 8)
                if forecast.raw_score is not None
                else None
            ),
            "status": forecast.status,
            "theme_code": forecast.theme_code,
            "theme_names": sorted({
                str(value)
                for value in (
                    forecast.features.get("all_theme_cluster_labels")
                    or forecast.features.get("theme_cluster_labels")
                    or forecast.features.get("theme_names")
                    or forecast.features.get("theme_codes")
                    or ()
                )
                if str(value)
            } | ({str(forecast.theme_code)} if forecast.theme_code else set())),
            "research_groups": sorted({
                str(item)
                for item in (
                    forecast.features.get("paper_research_groups") or ()
                )
                if str(item)
            }),
        }
        forecast_rows.append(row)

    for candidate_order, (score, code, primary, _support) in enumerate(
        candidates,
        1,
    ):
        code = str(code)
        selected = code in target_codes
        rejection = rejection_by_code.get(code) or {}
        themes = {
            str(item)
            for item in (
                primary.features.get("all_theme_cluster_labels")
                or primary.features.get("theme_cluster_labels")
                or primary.features.get("theme_names")
                or primary.features.get("theme_codes")
                or ()
            )
            if str(item)
        }
        if primary.theme_code:
            themes.add(str(primary.theme_code))
        row = {
            "candidate_order": candidate_order,
            "stock_code": code,
            "short_name": primary.stock_name,
            "strategy_key": primary.strategy_key,
            "score": round(float(score), 8),
            "theme_code": primary.theme_code,
            "theme_names": sorted(themes),
            "research_groups": sorted({
                str(item)
                for item in (
                    primary.features.get("paper_research_groups") or ()
                )
                if str(item)
            }),
            "selected": selected,
            "reason_code": rejection.get("reason_code"),
            "reason": rejection.get("reason"),
        }
        candidate_rows.append(row)
        if selected:
            selected_theme_sets.append(themes)
        for theme in themes:
            theme_rows[theme].append(row)

    warnings: list[str] = []
    unexplained = [
        row
        for row in candidate_rows
        if not row["selected"] and not row["reason_code"]
    ]
    if unexplained:
        warnings.append("UNEXPLAINED_CANDIDATE_OMISSION")
    missing_theme_count = sum(
        1 for row in candidate_rows if not row["theme_names"]
    )
    if missing_theme_count:
        warnings.append("CANDIDATE_THEME_MISSING")

    common_selected_themes: set[str] = set()
    if len(selected_theme_sets) >= 2:
        common_selected_themes = set.intersection(*selected_theme_sets)
    if common_selected_themes:
        warnings.append("TARGET_THEME_CONCENTRATION")
    selected_theme_weight_exposure: dict[str, float] = defaultdict(float)
    total_selected_weight = sum(float(item.target_weight) for item in targets)
    for target in targets:
        target_themes = set(target.theme_codes)
        if target.theme_code:
            target_themes.add(target.theme_code)
        for theme in target_themes:
            selected_theme_weight_exposure[str(theme)] += float(
                target.target_weight
            )
    dominant_theme_weight_share = (
        max(selected_theme_weight_exposure.values(), default=0.0)
        / total_selected_weight
        if total_selected_weight > 0
        else 0.0
    )
    selected_theme_hhi = (
        sum(
            (weight / total_selected_weight) ** 2
            for weight in selected_theme_weight_exposure.values()
        )
        if total_selected_weight > 0
        else 0.0
    )
    if len(targets) >= 2 and dominant_theme_weight_share >= 0.75:
        if "TARGET_THEME_CONCENTRATION" not in warnings:
            warnings.append("TARGET_THEME_CONCENTRATION")

    full_market_theme_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in forecast_rows:
        for theme in row["theme_names"]:
            full_market_theme_rows[str(theme)].append(row)
    dynamic_theme_radar = []
    component_clusters = cluster_themes_by_component_overlap(
        {
            theme: {
                str(row["stock_code"])
                for row in rows
            }
            for theme, rows in full_market_theme_rows.items()
        }
    )
    for cluster in component_clusters:
        theme_aliases = tuple(str(item) for item in cluster["theme_ids"])
        rows_by_forecast = {
            (str(row["stock_code"]), str(row["strategy_key"])): row
            for theme in theme_aliases
            for row in full_market_theme_rows.get(theme, [])
        }
        rows = list(rows_by_forecast.values())
        evaluated = [
            row
            for row in rows
            if row["score"] is not None
            and row["status"] != "INSUFFICIENT_DATA"
        ]
        if not evaluated:
            continue
        ordered = sorted(
            evaluated,
            key=lambda item: (
                -float(item["score"]),
                item["stock_code"],
                item["strategy_key"],
            ),
        )
        stock_codes = {str(item["stock_code"]) for item in rows}
        status_counts: dict[str, int] = defaultdict(int)
        sleeve_counts: dict[str, int] = defaultdict(int)
        for item in rows:
            status_counts[str(item["status"])] += 1
            sleeve_counts[str(item["strategy_key"])] += 1
        candidate_rows_by_code = {
            str(row["stock_code"]): row
            for theme in theme_aliases
            for row in theme_rows.get(theme, [])
        }
        candidate_codes = set(candidate_rows_by_code)
        ordered_candidates = sorted(
            candidate_rows_by_code.values(),
            key=lambda item: (
                -float(item["score"]),
                item["stock_code"],
            ),
        )
        dynamic_theme_radar.append({
            "theme": str(cluster["canonical_label"]),
            "theme_aliases": list(theme_aliases),
            "component_cluster_key": cluster["component_cluster_key"],
            "semantic_cluster_keys": list(
                cluster["semantic_cluster_keys"]
            ),
            "minimum_pairwise_component_overlap": cluster[
                "minimum_pairwise_overlap"
            ],
            "universe_stock_count": len(stock_codes),
            "candidate_count": len(candidate_codes),
            "candidate_definition": (
                "UNIQUE_STOCKS_PASSING_PAPER_DISCOVERY_SIGNAL_GATES_"
                "BEFORE_PORTFOLIO_CONSTRAINTS"
            ),
            "forecast_count": len(rows),
            "high_score_forecast_count": sum(
                float(item["score"]) >= alert_score for item in evaluated
            ),
            "high_score_stock_count": len({
                str(item["stock_code"])
                for item in evaluated
                if float(item["score"]) >= alert_score
            }),
            "selected_stock_count": len(stock_codes & target_codes),
            "selected_count": len(stock_codes & target_codes),
            "top_signal": ordered[0],
            "top_candidate": (
                ordered_candidates[0] if ordered_candidates else None
            ),
            "status_counts": dict(sorted(status_counts.items())),
            "sleeve_counts": dict(sorted(sleeve_counts.items())),
        })
    dynamic_theme_radar.sort(
        key=lambda item: (
            -float(item["top_signal"]["score"]),
            -int(item["universe_stock_count"]),
            item["theme"],
        )
    )
    dynamic_research_rows, duplicate_leader_theme_count = (
        _diversified_dynamic_theme_rows(
            dynamic_theme_radar,
            limit=max_theme_rows,
        )
    )
    research_group_audit = []
    for item in dynamic_research_rows:
        selected_count = int(item["selected_count"])
        candidate_count = int(item["candidate_count"])
        high_score_unselected = bool(
            selected_count == 0
            and float(item["top_signal"]["score"]) >= alert_score
        )
        research_group_audit.append({
            "group": str(item["theme"]),
            "source": "DYNAMIC_ALL_MARKET_THEME",
            "theme_aliases": list(item["theme_aliases"]),
            "component_cluster_key": item["component_cluster_key"],
            "universe_stock_count": int(item["universe_stock_count"]),
            "forecast_count": int(item["forecast_count"]),
            "expected_forecast_count": None,
            "missing_forecast_count": 0,
            "candidate_count": candidate_count,
            "selected_count": selected_count,
            "top_signal": item["top_signal"],
            "top_candidate": item["top_candidate"],
            "status_counts": dict(item["status_counts"]),
            "sleeve_counts": dict(item["sleeve_counts"]),
            "status": (
                "COVERED"
                if selected_count
                else "HIGH_SCORE_UNSELECTED"
                if high_score_unselected
                else "BELOW_ALERT"
                if candidate_count
                else "NO_CANDIDATE"
            ),
        })
    high_score_dynamic_theme_unselected_details = []
    for item in dynamic_theme_radar:
        if (
            float(item["top_signal"]["score"]) >= alert_score
            and int(item["selected_stock_count"]) == 0
        ):
            high_score_dynamic_theme_unselected_details.append({
                "theme": str(item["theme"]),
                "theme_aliases": list(item["theme_aliases"]),
                "component_cluster_key": item["component_cluster_key"],
                "top_score": float(item["top_signal"]["score"]),
                "top_stock_code": str(
                    item["top_signal"]["stock_code"]
                ),
                "top_strategy_key": str(
                    item["top_signal"]["strategy_key"]
                ),
                "universe_stock_count": int(
                    item["universe_stock_count"]
                ),
                "forecast_count": int(item["forecast_count"]),
                "candidate_count": int(item["candidate_count"]),
                "selected_stock_count": int(
                    item["selected_stock_count"]
                ),
            })
    dynamic_warning_limit = max(0, max_theme_rows)
    emitted_dynamic_warning_details = (
        high_score_dynamic_theme_unselected_details[:dynamic_warning_limit]
    )
    for item in emitted_dynamic_warning_details:
        warnings.append(
            "HIGH_SCORE_DYNAMIC_THEME_UNSELECTED:"
            + str(item["theme"])
        )
    dynamic_warning_truncated_count = max(
        0,
        len(high_score_dynamic_theme_unselected_details)
        - len(emitted_dynamic_warning_details),
    )
    if high_score_dynamic_theme_unselected_details:
        warnings.append(
            "HIGH_SCORE_DYNAMIC_THEME_UNSELECTED_COUNT:"
            + str(len(high_score_dynamic_theme_unselected_details))
        )
    if dynamic_warning_truncated_count:
        warnings.append(
            "HIGH_SCORE_DYNAMIC_THEME_WARNING_ROWS_TRUNCATED:"
            + str(dynamic_warning_truncated_count)
        )

    opportunity_themes = []
    for theme, rows in theme_rows.items():
        ordered = sorted(
            rows,
            key=lambda item: (-float(item["score"]), item["stock_code"]),
        )
        opportunity_themes.append({
            "theme": theme,
            "candidate_count": len(rows),
            "selected_count": sum(bool(item["selected"]) for item in rows),
            "top_candidate": ordered[0],
        })
    opportunity_themes.sort(
        key=lambda item: (
            -int(item["candidate_count"]),
            -float(item["top_candidate"]["score"]),
            item["theme"],
        )
    )
    top_unselected = [
        row for row in candidate_rows if not row["selected"]
    ][:max(0, max_unselected_rows)]
    reason_counts: dict[str, int] = defaultdict(int)
    for row in candidate_rows:
        if row["reason_code"]:
            reason_counts[str(row["reason_code"])] += 1

    return {
        "scope": "ALL_DECISION_FORECASTS",
        "research_group_kind": "DYNAMIC_ALL_MARKET_THEME_RADAR",
        "research_group_mode": "DYNAMIC_EACH_DECISION",
        "research_group_selection_rule": (
            "TOP_SIGNAL_DESC_THEN_DISTINCT_LEADER_STOCK"
        ),
        "duplicate_leader_theme_count": duplicate_leader_theme_count,
        "status": "ATTENTION" if warnings else "PASS",
        "universe_stock_count": len({
            str(row["stock_code"]) for row in forecast_rows
        }),
        "forecast_count": len(forecast_rows),
        "candidate_count": len(candidate_rows),
        "candidate_definition": (
            "UNIQUE_STOCKS_PASSING_PAPER_DISCOVERY_SIGNAL_GATES_"
            "BEFORE_PORTFOLIO_CONSTRAINTS"
        ),
        "selected_count": sum(
            bool(row["selected"]) for row in candidate_rows
        ),
        "rejected_count": sum(
            bool(row["reason_code"]) for row in candidate_rows
        ),
        "unexplained_unselected_count": len(unexplained),
        "missing_theme_count": missing_theme_count,
        "minimum_alert_score": alert_score,
        "maximum_paper_positions": int(
            config.get("paper_discovery", {}).get(
                "maximum_positions",
                0,
            )
        ),
        "warnings": warnings,
        "warning_details": {
            "high_score_dynamic_theme_unselected": {
                "count": len(
                    high_score_dynamic_theme_unselected_details
                ),
                "emitted_warning_count": len(
                    emitted_dynamic_warning_details
                ),
                "truncated_warning_count": dynamic_warning_truncated_count,
                "items": high_score_dynamic_theme_unselected_details,
            },
        },
        "selected_concentration_themes": sorted(common_selected_themes),
        "selected_theme_weight_exposure": {
            key: round(value, 8)
            for key, value in sorted(
                selected_theme_weight_exposure.items()
            )
        },
        "dominant_theme_weight_share": round(
            dominant_theme_weight_share,
            8,
        ),
        "selected_theme_hhi": round(selected_theme_hhi, 8),
        "research_groups": research_group_audit,
        "dynamic_theme_radar": dynamic_theme_radar[
            :max(0, max_theme_rows)
        ],
        "opportunity_themes": opportunity_themes[:max(0, max_theme_rows)],
        "top_unselected": top_unselected,
        "rejection_reason_counts": dict(sorted(reason_counts.items())),
    }


def build_consensus(
    forecasts: Iterable[AlphaForecast],
    *,
    strategy_weights: dict[str, float] | None = None,
) -> tuple[ConsensusForecast, ...]:
    config = load_v3_config()
    gates = config["profit_gate"]
    default_weights = {
        key: float(value["default_risk_weight"])
        for key, value in config["sleeves"].items()
    }
    weights = {**default_weights, **(strategy_weights or {})}
    grouped: dict[str, list[AlphaForecast]] = defaultdict(list)
    for forecast in forecasts:
        if forecast.status != "VALIDATED_POSITIVE":
            continue
        if forecast.sample_count < int(gates["minimum_oos_samples"]):
            continue
        if (
            _finite(forecast.expected_return_net_pct)
            <= float(gates["minimum_expected_return_net_pct"])
        ):
            continue
        if (
            _finite(forecast.profit_factor)
            < float(gates["minimum_profit_factor"])
        ):
            continue
        if (
            _finite(forecast.payoff_ratio)
            < float(gates["minimum_payoff_ratio"])
        ):
            continue
        grouped[forecast.stock_code].append(forecast)
    consensus: list[ConsensusForecast] = []
    for code, items in grouped.items():
        valid_weights = [
            max(0.0, weights.get(item.strategy_key, 0.0))
            * max(0.05, item.confidence)
            for item in items
        ]
        total = sum(valid_weights)
        if total <= 0:
            continue
        expected = sum(
            _finite(item.expected_return_net_pct) * weight
            for item, weight in zip(items, valid_weights)
        ) / total
        q10 = sum(
            _finite(item.return_q10_pct) * weight
            for item, weight in zip(items, valid_weights)
        ) / total
        probability = sum(
            _finite(item.probability_positive) * weight
            for item, weight in zip(items, valid_weights)
        ) / total
        mae = sum(
            abs(_finite(item.expected_mae_pct)) * weight
            for item, weight in zip(items, valid_weights)
        ) / total
        selection_score = sum(
            _finite(item.raw_score) * weight
            for item, weight in zip(items, valid_weights)
        ) / total
        uncertainty = max(0.0, expected - q10)
        conservative = expected - 0.20 * uncertainty
        evidence = tuple(
            f"{item.strategy_key}：净期望"
            f"{_finite(item.expected_return_net_pct):.2f}%，"
            f"样本{item.sample_count}"
            for item in items
        )
        primary_strategy_key = max(
            zip(items, valid_weights),
            key=lambda pair: (
                pair[1],
                float(pair[0].raw_score or 0.0),
                pair[0].strategy_key,
            ),
        )[0].strategy_key
        consensus.append(
            ConsensusForecast(
                stock_code=code,
                stock_name=items[0].stock_name,
                expected_return_net_pct=round(expected, 6),
                conservative_return_pct=round(conservative, 6),
                probability_positive=round(probability, 6),
                expected_mae_pct=round(mae, 6),
                profit_factor=min(
                    _finite(item.profit_factor, math.inf)
                    for item in items
                ),
                payoff_ratio=min(
                    _finite(item.payoff_ratio, math.inf)
                    for item in items
                ),
                confidence=round(mean(item.confidence for item in items), 6),
                selection_score=round(selection_score, 8),
                strategy_keys=tuple(
                    sorted({item.strategy_key for item in items})
                ),
                primary_strategy_key=primary_strategy_key,
                theme_code=next(
                    (item.theme_code for item in items if item.theme_code),
                    "",
                ),
                initial_stop_pct=min(
                    item.initial_stop_pct for item in items
                ),
                evidence=evidence,
                theme_codes=tuple(sorted({
                    theme_code
                    for item in items
                    for theme_code in _forecast_theme_codes(item)
                })),
            )
        )
    return tuple(
        sorted(
            consensus,
            key=lambda item: (
                -item.conservative_return_pct,
                -item.probability_positive,
                -item.selection_score,
                item.stock_code,
            ),
        )
    )


def estimate_roundtrip_cost_pct(
    order_value: float,
    *,
    commission_rate: float,
    minimum_commission: float,
    transfer_fee_rate: float,
    sell_stamp_duty_rate: float,
    slippage_rate: float,
) -> float:
    return _shared_roundtrip_cost_pct(
        order_value,
        commission_rate=commission_rate,
        minimum_commission=minimum_commission,
        transfer_fee_rate=transfer_fee_rate,
        sell_stamp_duty_rate=sell_stamp_duty_rate,
        slippage_rate=slippage_rate,
    )


def optimize_retail_portfolio(
    consensus: Iterable[ConsensusForecast],
    *,
    prices: dict[str, float],
    equity: float,
    current_theme_weights: dict[str, float] | None,
    regime: RegimeProbabilities,
    current_position_weights: dict[str, float] | None = None,
    current_position_quantities: dict[str, int] | None = None,
    current_position_themes: dict[str, tuple[str, ...]] | None = None,
    current_open_risk_weight: float = 0.0,
) -> PortfolioDecision:
    config = load_v3_config()
    policy = config["portfolio"]
    fees = config["account"]
    gate = config["profit_gate"]
    rejected: list[dict[str, Any]] = []
    targets: list[PortfolioTarget] = []
    position_weights = {
        str(code): max(0.0, float(weight))
        for code, weight in (current_position_weights or {}).items()
        if float(weight) > 0
    }
    constraints = PortfolioConstraintState(
        policy=policy,
        equity=equity,
        risk_asset_cap=regime.risk_asset_cap,
        current_theme_weights=current_theme_weights,
        current_position_weights=position_weights,
        current_position_quantities=current_position_quantities,
        current_position_themes=current_position_themes,
        current_open_risk_weight=current_open_risk_weight,
    )
    for item in consensus:
        price = float(prices.get(item.stock_code) or 0.0)
        candidate_themes = set(item.theme_codes)
        if item.theme_code:
            candidate_themes.add(item.theme_code)
        admission = constraints.admit(
            stock_code=item.stock_code,
            price=price,
            initial_stop_pct=item.initial_stop_pct,
            candidate_themes=candidate_themes,
            conservative_return_pct=item.conservative_return_pct,
            fees=fees,
            minimum_edge_to_cost_multiple=float(
                gate["minimum_edge_to_roundtrip_cost_multiple"]
            ),
        )
        if not admission.accepted:
            reason = admission.reason
            if admission.reason_code == "ORDER_NOT_ECONOMIC":
                reason = (
                    "计划订单低于小账户最小经济订单"
                    f"{float(policy['minimum_economic_order_cny']):.2f}元"
                )
            elif admission.reason_code == "NET_EDGE_BELOW_COST_BUFFER":
                reason = (
                    f"保守净收益{item.conservative_return_pct:.2f}%"
                    f"不足往返成本缓冲"
                )
            rejected.append({
                "stock_code": item.stock_code,
                "reason_code": admission.reason_code,
                "reason": reason,
            })
            continue
        targets.append(
            PortfolioTarget(
                stock_code=item.stock_code,
                stock_name=item.stock_name,
                target_weight=round(admission.target_weight, 8),
                target_value=round(admission.target_value, 2),
                target_quantity=admission.target_quantity,
                estimated_roundtrip_cost_pct=round(
                    admission.estimated_roundtrip_cost_pct,
                    6,
                ),
                expected_return_net_pct=item.expected_return_net_pct,
                conservative_return_pct=item.conservative_return_pct,
                expected_mae_pct=item.expected_mae_pct,
                theme_code=item.theme_code,
                strategy_keys=item.strategy_keys,
                reason="；".join(item.evidence),
                theme_codes=item.theme_codes,
                primary_strategy_key=item.primary_strategy_key,
            )
        )
    invested = constraints.invested_weight
    turnover = constraints.estimated_one_way_turnover_weight
    expected_return = sum(
        max(
            0.0,
            item.target_weight
            - position_weights.get(item.stock_code, 0.0),
        )
        * item.expected_return_net_pct
        for item in targets
    )
    minimum_positions = int(policy["minimum_positions"])
    status = (
        "READY"
        if len(constraints.planned_weights) >= minimum_positions and targets
        else "CASH_OR_ETF_PREFERRED"
    )
    return PortfolioDecision(
        targets=tuple(targets),
        rejected=tuple(rejected),
        target_cash=round(equity * (1.0 - invested), 2),
        target_risk_asset_weight=round(invested, 8),
        expected_portfolio_return_pct=round(expected_return, 6),
        worst_case_loss_cny=round(constraints.total_open_risk_cny, 2),
        status=status,
        estimated_one_way_turnover_weight=round(turnover, 8),
        current_risk_asset_weight=round(constraints.current_invested, 8),
    )


def add_paper_discovery_targets(
    portfolio: PortfolioDecision,
    forecasts: Iterable[AlphaForecast],
    *,
    prices: dict[str, float],
    equity: float,
    current_theme_weights: dict[str, float] | None,
    regime: RegimeProbabilities,
    current_position_weights: dict[str, float] | None = None,
    current_position_quantities: dict[str, int] | None = None,
    current_position_themes: dict[str, tuple[str, ...]] | None = None,
    current_paper_discovery_codes: set[str] | None = None,
    learning_context: dict[str, Any] | None = None,
    opportunity_audit_forecasts: Iterable[AlphaForecast] | None = None,
) -> PortfolioDecision:
    """Add tightly capped research probes to the internal paper account.

    Discovery targets never claim positive expectancy and are never eligible
    for real execution.  Their purpose is to collect forward evidence for a
    strong, multi-sleeve or exceptional single-sleeve signal that would
    otherwise be hidden by a coarse strategy-wide calibration gate.
    """

    config = load_v3_config()
    forecasts = tuple(forecasts)
    audit_forecasts = tuple(
        opportunity_audit_forecasts
        if opportunity_audit_forecasts is not None
        else forecasts
    )
    policy = dict(config.get("paper_discovery") or {})
    if not policy.get("enabled"):
        return portfolio
    allowed_strategies = set(
        policy.get("allowed_strategy_keys") or ()
    )
    single_sleeve_strategies = set(
        policy.get("single_sleeve_strategy_keys") or ()
    )
    current_weights = {
        str(code): max(0.0, float(weight))
        for code, weight in (current_position_weights or {}).items()
        if float(weight) > 0
    }
    current_quantities = {
        str(code): max(0, int(quantity))
        for code, quantity in (
            current_position_quantities or {}
        ).items()
    }
    current_themes = {
        str(code): {
            str(theme)
            for theme in themes
            if str(theme)
        }
        for code, themes in (
            current_position_themes or {}
        ).items()
    }
    paper_positions = {
        str(code)
        for code in (current_paper_discovery_codes or set())
        if str(code) in current_weights
    }
    existing_targets = {
        item.stock_code for item in portfolio.targets
    }
    # Existing validated positions stay excluded. Existing discovery probes
    # must be reconsidered so an unchanged signal can emit an explicit hold
    # target instead of being mistaken for an ended signal.
    existing = existing_targets | (
        set(current_weights) - paper_positions
    )
    grouped: dict[str, list[AlphaForecast]] = defaultdict(list)
    for item in forecasts:
        if item.stock_code in existing:
            continue
        if (
            allowed_strategies
            and item.strategy_key not in allowed_strategies
        ):
            continue
        if item.status not in {
            "PAPER_DISCOVERY_CANDIDATE",
            "RESEARCH_ONLY_UNCALIBRATED",
            "RESEARCH_ONLY_PROFIT_GATE_FAILED",
            "RESEARCH_ONLY_MODEL_VERSION_MISMATCH",
            "RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED",
            "RESEARCH_ONLY_SCORE_OUT_OF_RANGE",
        }:
            continue
        if item.raw_score is None:
            continue
        grouped[item.stock_code].append(item)
    candidates = []
    primary_threshold = float(
        policy.get("minimum_primary_score", 0.82)
    )
    theme_threshold = float(
        policy.get("minimum_theme_score", 0.72)
    )
    trend_threshold = float(
        policy.get("minimum_trend_score", 0.72)
    )
    single_threshold = float(
        policy.get("minimum_single_sleeve_score", 0.72)
    )
    learning = dict(learning_context or {})
    learned_samples = int(learning.get("accepted_count") or 0)
    learned_pf = learning.get("profit_factor")
    learned_average = learning.get("average_net_return_pct")
    threshold_penalty = 0.0
    position_multiplier = 1.0
    if learned_samples >= 10:
        numeric_pf = (
            float(learned_pf)
            if learned_pf is not None
            else 0.0
        )
        numeric_average = float(learned_average or 0.0)
        if numeric_pf < 0.8 or numeric_average < -1.0:
            threshold_penalty = 0.10
            position_multiplier = 0.50
        elif numeric_pf < 1.0 or numeric_average < 0.0:
            threshold_penalty = 0.06
            position_multiplier = 0.70
        elif numeric_pf < 1.3:
            threshold_penalty = 0.03
            position_multiplier = 0.85
    single_threshold = min(
        0.92,
        single_threshold + threshold_penalty,
    )
    for code, items in grouped.items():
        ordered = sorted(
            items,
            key=lambda item: (
                -float(item.raw_score or 0.0),
                item.strategy_key,
            ),
        )
        primary = ordered[0]
        primary_score = float(primary.raw_score or 0.0)
        score_by_sleeve = {
            item.strategy_key: float(item.raw_score or 0.0)
            for item in ordered
        }
        support = [
            item
            for item in ordered
            if item.strategy_key
            in {"theme_diffusion", "right_side_trend"}
        ]
        if primary.strategy_key in single_sleeve_strategies:
            if (
                primary.status != "PAPER_DISCOVERY_CANDIDATE"
                or primary_score < single_threshold
            ):
                continue
            candidates.append(
                (
                    primary_score,
                    code,
                    primary,
                    [primary],
                )
            )
            continue
        if primary.strategy_key not in {
            "theme_diffusion",
            "right_side_trend",
        }:
            continue
        if primary_score < primary_threshold:
            continue
        if (
            score_by_sleeve.get("theme_diffusion", 0.0)
            < theme_threshold
            or score_by_sleeve.get("right_side_trend", 0.0)
            < trend_threshold
        ):
            continue
        candidates.append(
            (
                primary_score + min(0.06, len(support) * 0.015),
                code,
                primary,
                support,
            )
        )
    candidates.sort(key=lambda item: (-item[0], item[1]))

    targets = list(portfolio.targets)
    rejected = list(portfolio.rejected)

    def reject_discovery(
        code: str,
        primary: AlphaForecast,
        reason_code: str,
        reason: str,
    ) -> None:
        rejected.append({
            "stock_code": code,
            "strategy_key": primary.strategy_key,
            "raw_score": primary.raw_score,
            "theme_code": primary.theme_code,
            "theme_codes": list(_forecast_theme_codes(primary)),
            "paper_research_groups": list(
                primary.features.get("paper_research_groups") or []
            ),
            "reason_code": reason_code,
            "reason": reason,
        })

    theme_weights = defaultdict(float, current_theme_weights or {})
    planned_weights = dict(current_weights)
    planned_themes = dict(current_themes)
    for item in targets:
        previous_weight = current_weights.get(item.stock_code, 0.0)
        incremental_weight = max(
            0.0,
            item.target_weight - previous_weight,
        )
        planned_weights[item.stock_code] = item.target_weight
        item_themes = set(item.theme_codes)
        if item.theme_code:
            item_themes.add(item.theme_code)
        planned_themes[item.stock_code] = item_themes
        for theme in item_themes:
            theme_weights[theme] += incremental_weight
    invested = float(portfolio.target_risk_asset_weight)
    current_discovery_weight = sum(
        current_weights.get(code, 0.0)
        for code in paper_positions
    )
    new_discovery_weight = 0.0
    worst_case_loss = float(portfolio.worst_case_loss_cny)
    max_positions = int(policy.get("maximum_positions", 3))
    position_weight = (
        float(policy.get("position_weight", 0.03))
        * position_multiplier
    )
    maximum_new_discovery_weight = min(
        max(
            0.0,
            float(policy.get("maximum_total_weight", 0.09))
            - current_discovery_weight,
        ),
        max(0.0, float(regime.risk_asset_cap) - invested),
    )
    remaining_turnover = max(
        0.0,
        float(config["portfolio"]["maximum_daily_turnover"])
        - float(portfolio.estimated_one_way_turnover_weight),
    )
    theme_weight_cap = float(
        policy.get("maximum_theme_weight", 0.06)
    )
    fees = config["account"]
    added = 0
    retained = 0
    for _, code, primary, support in candidates:
        if code in paper_positions:
            if code in existing_targets:
                continue
            price = float(prices.get(code) or 0.0)
            current_weight = current_weights.get(code, 0.0)
            quantity = current_quantities.get(code, 0)
            if quantity <= 0:
                quantity = math.floor(
                    (
                        equity
                        * current_weight
                        / max(price, 1e-9)
                        + 1e-8
                    )
                    / 100
                ) * 100
            if price <= 0 or current_weight <= 0 or quantity <= 0:
                reject_discovery(
                    code,
                    primary,
                    "PAPER_DISCOVERY_HOLD_TARGET_INVALID",
                    "已有纸面研究仓位缺少有效价格、权重或整手数量",
                )
                continue
            candidate_themes = set(_forecast_theme_codes(primary))
            stop_pct = max(
                -float(policy.get("maximum_stop_pct", 6.0)),
                min(item.initial_stop_pct for item in support),
            )
            strategy_keys = tuple(
                sorted({item.strategy_key for item in support})
                + ["paper_discovery"]
            )
            targets.append(
                PortfolioTarget(
                    stock_code=code,
                    stock_name=primary.stock_name,
                    target_weight=round(current_weight, 8),
                    target_value=round(quantity * price, 2),
                    target_quantity=quantity,
                    estimated_roundtrip_cost_pct=0.0,
                    expected_return_net_pct=0.0,
                    conservative_return_pct=0.0,
                    expected_mae_pct=abs(float(stop_pct)),
                    theme_code=primary.theme_code,
                    strategy_keys=strategy_keys,
                    reason=(
                        "PAPER_DISCOVERY｜模拟试错信号仍有效；"
                        "维持原小仓，不加仓、不转实盘"
                    ),
                    theme_codes=tuple(sorted(candidate_themes)),
                    primary_strategy_key=primary.strategy_key,
                )
            )
            retained += 1
            continue
        if len(paper_positions) + added >= max_positions:
            reject_discovery(
                code,
                primary,
                "PAPER_DISCOVERY_POSITION_CAP",
                f"纸面研究新开仓数量已达到{max_positions}只上限",
            )
            continue
        if len(planned_weights) >= int(
            config["portfolio"]["maximum_positions"]
        ):
            reject_discovery(
                code,
                primary,
                "PORTFOLIO_POSITION_CAP",
                "组合可用持仓数量已满",
            )
            continue
        if new_discovery_weight >= maximum_new_discovery_weight:
            reject_discovery(
                code,
                primary,
                "PAPER_DISCOVERY_TOTAL_WEIGHT_CAP",
                "纸面研究总仓位额度已用完",
            )
            continue
        price = float(prices.get(code) or 0.0)
        if price <= 0:
            reject_discovery(
                code,
                primary,
                "PRICE_MISSING",
                "缺少可执行价格",
            )
            continue
        candidate_themes = set(_forecast_theme_codes(primary))
        maximum_same_theme_positions = int(
            policy.get("maximum_same_theme_positions", 1)
        )
        same_theme_positions = sum(
            1
            for planned_code in planned_weights
            if candidate_themes
            & planned_themes.get(planned_code, set())
        )
        if (
            candidate_themes
            and same_theme_positions >= maximum_same_theme_positions
        ):
            reject_discovery(
                code,
                primary,
                "PAPER_DISCOVERY_THEME_POSITION_CAP",
                "纸面研究组合已有同主题持仓，优先保留跨主题验证名额",
            )
            continue
        correlated_weight = sum(
            weight
            for planned_code, weight in planned_weights.items()
            if candidate_themes
            & planned_themes.get(planned_code, set())
        )
        theme_room = min(
            (
                theme_weight_cap - theme_weights[theme]
                for theme in candidate_themes
            ),
            default=theme_weight_cap,
        )
        correlated_room = max(
            0.0,
            float(
                config["portfolio"][
                    "maximum_correlated_theme_weight"
                ]
            )
            - correlated_weight,
        )
        desired_weight = min(
            position_weight,
            maximum_new_discovery_weight - new_discovery_weight,
            remaining_turnover,
            max(0.0, theme_room),
            correlated_room,
        )
        maximum_weight = min(
            position_weight
            + float(policy.get("maximum_lot_rounding_overweight", 0.0)),
            maximum_new_discovery_weight - new_discovery_weight,
            remaining_turnover,
            max(0.0, theme_room),
            correlated_room,
        )
        if desired_weight <= 0 or maximum_weight <= 0:
            reject_discovery(
                code,
                primary,
                "PAPER_DISCOVERY_THEME_OR_RISK_BUDGET_FULL",
                "主题、相关主题、换手或纸面研究总仓位额度不足",
            )
            continue
        preferred_minimum = float(
            policy.get("minimum_order_cny", 5_000.0)
        )
        absolute_minimum = float(
            policy.get(
                "absolute_minimum_lot_order_cny",
                preferred_minimum,
            )
        )
        quantity, lot_reason = _paper_probe_quantity(
            equity=equity,
            price=price,
            desired_weight=desired_weight,
            maximum_weight=maximum_weight,
            preferred_minimum_order_cny=preferred_minimum,
            absolute_minimum_lot_order_cny=absolute_minimum,
        )
        order_value = quantity * price
        if quantity <= 0:
            floor_quantity = (
                math.floor(equity * desired_weight / price / 100) * 100
            )
            floor_value = floor_quantity * price
            reject_discovery(
                code,
                primary,
                lot_reason,
                (
                    f"目标仓位{desired_weight:.3%}按整手取整后"
                    f"为{floor_value:.2f}元；低于纸面研究硬下限"
                    f"{absolute_minimum:.2f}元，且向上取整会突破仓位约束"
                ),
            )
            continue
        actual_weight = order_value / equity
        cost_pct = estimate_roundtrip_cost_pct(
            order_value,
            commission_rate=float(fees["commission_rate"]),
            minimum_commission=float(fees["minimum_commission_cny"]),
            transfer_fee_rate=float(fees["transfer_fee_rate"]),
            sell_stamp_duty_rate=float(fees["sell_stamp_duty_rate"]),
            slippage_rate=float(fees["default_slippage_rate"]),
        )
        stop_pct = max(
            -float(policy.get("maximum_stop_pct", 6.0)),
            min(item.initial_stop_pct for item in support),
        )
        open_risk = order_value * abs(stop_pct) / 100.0
        if (
            worst_case_loss + open_risk
            > equity
            * float(config["portfolio"]["maximum_open_risk"])
        ):
            reject_discovery(
                code,
                primary,
                "PORTFOLIO_OPEN_RISK_CAP",
                "新增纸面研究仓位会突破组合开放风险上限",
            )
            continue
        strategy_keys = tuple(
            sorted({item.strategy_key for item in support})
            + ["paper_discovery"]
        )
        lot_note = {
            "PAPER_DISCOVERY_LOT_ROUNDED_UP": (
                "；为满足最低经济订单，在全部仓位约束内向上取整一手"
            ),
            "PAPER_DISCOVERY_SUBMINIMUM_RESEARCH_LOT": (
                "；受总仓位约束采用低于首选金额的整手纸面研究单，成本照实计入"
            ),
        }.get(lot_reason, "")
        targets.append(
            PortfolioTarget(
                stock_code=code,
                stock_name=primary.stock_name,
                target_weight=round(actual_weight, 8),
                target_value=round(order_value, 2),
                target_quantity=quantity,
                estimated_roundtrip_cost_pct=round(cost_pct, 6),
                expected_return_net_pct=0.0,
                conservative_return_pct=0.0,
                expected_mae_pct=abs(float(stop_pct)),
                theme_code=primary.theme_code,
                strategy_keys=strategy_keys,
                reason=(
                    "PAPER_DISCOVERY｜模拟试错：尚未通过正期望校准；"
                    f"主信号{primary.strategy_key}="
                    f"{float(primary.raw_score or 0):.3f}；"
                    "仅限ProBigA模拟盘小仓，动态失效即退出，"
                    "结果自动进入前向与反事实复盘"
                    + (
                        f"；前向已成熟{learned_samples}笔，"
                        f"本轮信号阈值{single_threshold:.2f}，"
                        f"仓位系数{position_multiplier:.0%}"
                        if learned_samples
                        else ""
                    )
                    + lot_note
                ),
                theme_codes=tuple(sorted(candidate_themes)),
                primary_strategy_key=primary.strategy_key,
            )
        )
        new_discovery_weight += actual_weight
        remaining_turnover -= actual_weight
        invested += actual_weight
        worst_case_loss += open_risk
        planned_weights[code] = actual_weight
        planned_themes[code] = candidate_themes
        for theme in candidate_themes:
            theme_weights[theme] += actual_weight
        added += 1
    opportunity_audit = _paper_opportunity_audit(
        candidates,
        forecasts=audit_forecasts,
        targets=targets,
        rejected=rejected,
        config=config,
    )
    if not added and not retained:
        return PortfolioDecision(
            targets=portfolio.targets,
            rejected=tuple(rejected),
            target_cash=portfolio.target_cash,
            target_risk_asset_weight=portfolio.target_risk_asset_weight,
            expected_portfolio_return_pct=(
                portfolio.expected_portfolio_return_pct
            ),
            worst_case_loss_cny=portfolio.worst_case_loss_cny,
            status=portfolio.status,
            estimated_one_way_turnover_weight=(
                portfolio.estimated_one_way_turnover_weight
            ),
            current_risk_asset_weight=portfolio.current_risk_asset_weight,
            opportunity_audit=opportunity_audit,
        )
    return PortfolioDecision(
        targets=tuple(targets),
        rejected=tuple(rejected),
        target_cash=round(equity * max(0.0, 1.0 - invested), 2),
        target_risk_asset_weight=round(invested, 8),
        expected_portfolio_return_pct=(
            portfolio.expected_portfolio_return_pct
        ),
        worst_case_loss_cny=round(worst_case_loss, 2),
        status=(
            "PAPER_DISCOVERY_READY"
            if not portfolio.targets
            else "READY_WITH_PAPER_DISCOVERY"
        ),
        estimated_one_way_turnover_weight=round(
            portfolio.estimated_one_way_turnover_weight
            + new_discovery_weight,
            8,
        ),
        current_risk_asset_weight=(
            portfolio.current_risk_asset_weight
        ),
        opportunity_audit=opportunity_audit,
    )
