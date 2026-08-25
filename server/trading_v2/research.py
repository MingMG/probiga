"""Exact V2 research metrics and promotion-gate evaluation."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

from server.engine.strategy_statistical_guards import newey_west_nav_statistics

from .config import canonical_json_hash
from .domain import decimal_value


@dataclass(frozen=True)
class CompletedTrade:
    trade_id: str
    stock_code: str
    trade_net_pnl: Decimal
    initial_risk_amount: Decimal


def _ratio_or_none(
    numerator: Decimal,
    denominator: Decimal,
    *,
    positive_without_negative: bool = False,
) -> Decimal | None | str:
    if denominator > 0:
        return numerator / denominator
    if positive_without_negative and numerator > 0:
        return "INF"
    return None


def trade_metrics(
    trades: Iterable[CompletedTrade],
    *,
    max_drawdown: Decimal,
) -> dict[str, Any]:
    rows = list(trades)
    if not rows:
        return {
            "completed_trade_count": 0,
            "expectancy_cny": None,
            "expectancy_r": None,
            "profit_factor": None,
            "payoff_ratio": None,
            "max_drawdown": str(max_drawdown),
            "cumulative_net_pnl": "0",
            "win_rate": None,
            "maximum_single_security_profit_contribution": None,
        }
    pnl = [decimal_value(item.trade_net_pnl) for item in rows]
    positive = [value for value in pnl if value > 0]
    negative = [value for value in pnl if value < 0]
    total = sum(pnl, Decimal("0"))
    valid_r = [
        decimal_value(item.trade_net_pnl)
        / decimal_value(item.initial_risk_amount)
        for item in rows
        if decimal_value(item.initial_risk_amount) > 0
    ]
    security_profit: dict[str, Decimal] = {}
    for item in rows:
        if item.trade_net_pnl > 0:
            security_profit[item.stock_code] = (
                security_profit.get(item.stock_code, Decimal("0"))
                + item.trade_net_pnl
            )
    positive_total = sum(positive, Decimal("0"))
    maximum_contribution = (
        max(security_profit.values(), default=Decimal("0"))
        / positive_total
        if positive_total > 0
        else None
    )
    average_positive = (
        positive_total / len(positive) if positive else Decimal("0")
    )
    negative_total = abs(sum(negative, Decimal("0")))
    average_negative = (
        negative_total / len(negative) if negative else Decimal("0")
    )
    return {
        "completed_trade_count": len(rows),
        "expectancy_cny": str(total / len(rows)),
        "expectancy_r": (
            str(sum(valid_r, Decimal("0")) / len(rows))
            if len(valid_r) == len(rows)
            else None
        ),
        "profit_factor": _ratio_text(
            _ratio_or_none(
                positive_total,
                negative_total,
                positive_without_negative=True,
            )
        ),
        "payoff_ratio": _ratio_text(
            _ratio_or_none(
                average_positive,
                average_negative,
                positive_without_negative=True,
            )
        ),
        "max_drawdown": str(max_drawdown),
        "cumulative_net_pnl": str(total),
        "win_rate": str(Decimal(len(positive)) / Decimal(len(rows))),
        "maximum_single_security_profit_contribution": (
            str(maximum_contribution)
            if maximum_contribution is not None
            else None
        ),
    }


def _ratio_text(value: Decimal | None | str) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return str(value)


def _metric_decimal(value: Any) -> Decimal | None:
    if value in {None, "INF"}:
        return None
    return decimal_value(value)


def nav_records_from_equity(equity: Any) -> list[dict[str, Any]]:
    """Convert an exact dated equity curve to capital-weighted daily returns."""

    items = list(equity.items()) if hasattr(equity, "items") else []
    normalized: list[tuple[str, Decimal]] = []
    for raw_day, raw_value in items:
        day = getattr(raw_day, "date", lambda: raw_day)()
        day_text = str(day)[:10]
        value = decimal_value(raw_value)
        if not day_text or value <= 0:
            return []
        normalized.append((day_text, value))
    if (
        len(normalized) < 2
        or [day for day, _value in normalized]
        != sorted(set(day for day, _value in normalized))
    ):
        return []
    return [
        {
            "trade_date": day,
            "return_pct": str(
                (value / previous - Decimal("1")) * Decimal("100")
            ),
        }
        for (day, value), (_prior_day, previous) in zip(
            normalized[1:], normalized[:-1], strict=True,
        )
    ]


def v2_nav_statistical_guard(
    nav_records: Iterable[Mapping[str, Any]] | None,
    *,
    minimum_observations: int,
    minimum_effective_sample_size: int,
    minimum_profit_factor_lcb: Decimal,
    minimum_payoff_ratio_lcb: Decimal = Decimal("1"),
) -> dict[str, Any]:
    """Build the sole V2 GREEN/research statistical authority from daily NAV."""

    records = [dict(item) for item in (nav_records or [])]
    full = newey_west_nav_statistics(
        records,
        confidence_level=0.95,
        bandwidth=None,
        minimum_positive_days=2,
        minimum_negative_days=2,
    )
    profit_detail = full.get("profit_factor")
    payoff_detail = full.get("payoff_ratio")
    pf_lcb = (
        _metric_decimal(profit_detail.get("one_sided_95_lcb"))
        if isinstance(profit_detail, dict) else None
    )
    payoff_lcb = (
        _metric_decimal(payoff_detail.get("one_sided_95_lcb"))
        if isinstance(payoff_detail, dict) else None
    )
    net_lcb = _metric_decimal(
        full.get("net_expectancy_one_sided_95_lcb_pct")
    )
    ess = _metric_decimal(full.get("effective_sample_size"))
    observation_count = int(full.get("observation_count") or 0)
    positive_count = int(full.get("positive_day_count") or 0)
    negative_count = int(full.get("negative_day_count") or 0)
    passed = bool(
        full.get("valid") is True
        and observation_count >= int(minimum_observations)
        and ess is not None
        and ess >= Decimal(int(minimum_effective_sample_size))
        and positive_count >= 2
        and negative_count >= 2
        and net_lcb is not None and net_lcb > 0
        and pf_lcb is not None and pf_lcb >= minimum_profit_factor_lcb
        and payoff_lcb is not None
        and payoff_lcb >= minimum_payoff_ratio_lcb
    )
    payload = {
        "schema": "probiga.trading-v2-nav-hac-admission.v1",
        "valid": full.get("valid") is True,
        "passed": passed,
        "reason": (
            "NAV_HAC_LCB_PASSED" if passed
            else "NAV_HAC_LCB_OR_SUPPORT_FAILED"
        ),
        "observation_count": observation_count,
        "minimum_observations": int(minimum_observations),
        "effective_sample_size": str(ess) if ess is not None else None,
        "minimum_effective_sample_size": int(minimum_effective_sample_size),
        "positive_day_count": positive_count,
        "negative_day_count": negative_count,
        "net_expectancy_one_sided_95_lcb_pct": (
            str(net_lcb) if net_lcb is not None else None
        ),
        "profit_factor_one_sided_95_lcb": (
            str(pf_lcb) if pf_lcb is not None else None
        ),
        "minimum_profit_factor_lcb": str(minimum_profit_factor_lcb),
        "payoff_ratio_one_sided_95_lcb": (
            str(payoff_lcb) if payoff_lcb is not None else None
        ),
        "minimum_payoff_ratio_lcb": str(minimum_payoff_ratio_lcb),
        "full_input_hash": str(full.get("input_hash") or ""),
        "full_parameter_hash": str(full.get("parameter_hash") or ""),
        "full_result_hash": str(full.get("result_hash") or ""),
    }
    return {**payload, "guard_hash": canonical_json_hash(payload)}


def evaluate_oos_gate(
    *,
    security_scope: str,
    trading_days: int,
    oos_windows: int,
    metrics: dict[str, Any],
    doubled_cost_metrics: dict[str, Any],
    remove_best_three_net_pnl: Decimal | None,
    robustness: dict[str, Any],
    future_data_violations: int,
    impossible_fill_profit: Decimal,
    nav_records: Iterable[Mapping[str, Any]] | None = None,
    doubled_cost_nav_records: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    stock_scope = security_scope == "A_SHARE"
    minimum_oos = 120 if stock_scope else 24
    checks: list[dict[str, Any]] = []

    def add(code: str, passed: bool, actual: Any, required: Any) -> None:
        checks.append(
            {
                "code": code,
                "passed": bool(passed),
                "actual": actual,
                "required": required,
            }
        )

    count = int(metrics.get("completed_trade_count") or 0)
    expectancy = _metric_decimal(metrics.get("expectancy_cny"))
    expectancy_r = _metric_decimal(metrics.get("expectancy_r"))
    profit_factor = _metric_decimal(metrics.get("profit_factor"))
    payoff = _metric_decimal(metrics.get("payoff_ratio"))
    drawdown = abs(decimal_value(metrics.get("max_drawdown") or 0))
    doubled_expectancy = _metric_decimal(
        doubled_cost_metrics.get("expectancy_cny")
    )
    doubled_pf = _metric_decimal(doubled_cost_metrics.get("profit_factor"))
    contribution = _metric_decimal(
        metrics.get("maximum_single_security_profit_contribution")
    )
    minimum_hac_observations = max(20, minimum_oos)
    minimum_hac_ess = max(20, minimum_oos // 2)
    nav_guard = v2_nav_statistical_guard(
        nav_records,
        minimum_observations=minimum_hac_observations,
        minimum_effective_sample_size=minimum_hac_ess,
        minimum_profit_factor_lcb=Decimal("1.30"),
        minimum_payoff_ratio_lcb=Decimal("1.50"),
    )
    doubled_nav_guard = v2_nav_statistical_guard(
        doubled_cost_nav_records,
        minimum_observations=minimum_hac_observations,
        minimum_effective_sample_size=minimum_hac_ess,
        minimum_profit_factor_lcb=Decimal("1.10"),
        minimum_payoff_ratio_lcb=Decimal("1.00"),
    )
    add("HISTORY_500_DAYS", trading_days >= 500, trading_days, 500)
    add(
        "OOS_SAMPLE",
        oos_windows >= minimum_oos,
        oos_windows,
        minimum_oos,
    )
    add("COMPLETED_TRADES_PRESENT", count > 0, count, ">0")
    add("EXPECTANCY_CNY_POSITIVE", expectancy is not None and expectancy > 0, metrics.get("expectancy_cny"), ">0")
    add("EXPECTANCY_R_POSITIVE", expectancy_r is not None and expectancy_r > 0, metrics.get("expectancy_r"), ">0")
    add("PROFIT_FACTOR_1_30", profit_factor is not None and profit_factor >= Decimal("1.30"), metrics.get("profit_factor"), ">=1.30且不得为INF")
    add("PAYOFF_RATIO_1_50", payoff is not None and payoff >= Decimal("1.50"), metrics.get("payoff_ratio"), ">=1.50且不得为INF")
    add("NAV_HAC_ESS_LCB", nav_guard["passed"] is True, nav_guard, "完整逐日NAV、HAC有效样本及单侧95%下界全部通过")
    add("MAX_DRAWDOWN_12", drawdown <= Decimal("0.12"), str(drawdown), "<=0.12")
    add("DOUBLED_COST_EXPECTANCY_POSITIVE", doubled_expectancy is not None and doubled_expectancy > 0, doubled_cost_metrics.get("expectancy_cny"), ">0")
    add("DOUBLED_COST_PF_1_10", doubled_pf is not None and doubled_pf >= Decimal("1.10"), doubled_cost_metrics.get("profit_factor"), ">=1.10且不得为INF")
    add("DOUBLED_COST_NAV_HAC_ESS_LCB", doubled_nav_guard["passed"] is True, doubled_nav_guard, "双倍成本逐日NAV的HAC/ESS与单侧95%下界通过")
    add("REMOVE_BEST_THREE_POSITIVE", remove_best_three_net_pnl is not None and remove_best_three_net_pnl > 0, str(remove_best_three_net_pnl) if remove_best_three_net_pnl is not None else None, ">0")
    add("SECURITY_CONTRIBUTION_35", contribution is not None and contribution <= Decimal("0.35"), metrics.get("maximum_single_security_profit_contribution"), "<=0.35")
    add("IMPOSSIBLE_FILL_PROFIT_ZERO", impossible_fill_profit == 0, str(impossible_fill_profit), "0")
    add("FUTURE_DATA_VIOLATIONS_ZERO", int(future_data_violations) == 0, int(future_data_violations), 0)
    add(
        "ROBUSTNESS_PROTOCOL_COMPLETE",
        robustness.get("complete") is True
        and int(robustness.get("block_bootstrap_paths") or 0) >= 2000
        and decimal_value(
            robustness.get("positive_parameter_neighborhood_ratio") or 0
        )
        > Decimal("0.60"),
        robustness,
        {
            "complete": True,
            "block_bootstrap_paths": 2000,
            "positive_parameter_neighborhood_ratio": ">0.60",
        },
    )
    return {
        "status": "PASS" if all(item["passed"] for item in checks) else "BLOCK",
        "checks": checks,
        "nav_statistical_guard": nav_guard,
        "doubled_cost_nav_statistical_guard": doubled_nav_guard,
    }


def completed_trade_dicts(
    trades: Iterable[CompletedTrade],
) -> list[dict[str, Any]]:
    return [asdict(item) for item in trades]
