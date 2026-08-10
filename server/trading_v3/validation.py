from __future__ import annotations

import math
from typing import Any


def _number(mapping: dict[str, Any], key: str) -> float | None:
    value = mapping.get(key)
    if not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) or math.isinf(number) else None


def model_gate_failures(
    *,
    validation: dict[str, Any],
    portfolio: dict[str, Any],
    config: dict[str, Any],
) -> tuple[str, ...]:
    """Return every signal- and portfolio-level production gate failure."""

    gate = dict(config.get("profit_gate") or {})
    failures: list[str] = []

    sample_count = _number(validation, "sample_count")
    if sample_count is None:
        failures.append("OOS_SAMPLE_COUNT_MISSING")
    elif sample_count < float(gate.get("minimum_oos_samples", 80)):
        failures.append("OOS_SAMPLE_COUNT_TOO_LOW")

    expectancy = _number(validation, "net_expectancy_pct")
    if expectancy is None:
        failures.append("OOS_NET_EXPECTANCY_MISSING")
    elif expectancy <= float(
        gate.get("minimum_expected_return_net_pct", 0.0)
    ):
        failures.append("OOS_NET_EXPECTANCY_NOT_POSITIVE")

    profit_factor = _number(validation, "profit_factor")
    if profit_factor is None:
        failures.append("OOS_PROFIT_FACTOR_MISSING")
    elif profit_factor < float(gate.get("minimum_profit_factor", 1.3)):
        failures.append("OOS_PROFIT_FACTOR_TOO_LOW")

    payoff = _number(validation, "payoff_ratio")
    if payoff is None:
        failures.append("OOS_PAYOFF_RATIO_MISSING")
    elif payoff < float(gate.get("minimum_payoff_ratio", 1.0)):
        failures.append("OOS_PAYOFF_RATIO_TOO_LOW")

    drawdown = _number(portfolio, "maximum_drawdown_pct")
    if drawdown is None:
        failures.append("PORTFOLIO_MAX_DRAWDOWN_MISSING")
    elif drawdown > float(gate.get("maximum_drawdown_pct", 12.0)):
        failures.append("PORTFOLIO_MAX_DRAWDOWN_TOO_HIGH")

    portfolio_trades = _number(portfolio, "trade_count")
    if portfolio_trades is None:
        portfolio_trades = _number(portfolio, "sample_count")
    if portfolio_trades is None:
        failures.append("PORTFOLIO_TRADE_COUNT_MISSING")
    elif portfolio_trades < float(
        gate.get("minimum_portfolio_trades", 80)
    ):
        failures.append("PORTFOLIO_TRADE_COUNT_TOO_LOW")

    portfolio_expectancy = _number(
        portfolio,
        "net_expectancy_pct",
    )
    if portfolio_expectancy is None:
        failures.append("PORTFOLIO_NET_EXPECTANCY_MISSING")
    elif portfolio_expectancy <= float(
        gate.get("minimum_portfolio_net_expectancy_pct", 0.0)
    ):
        failures.append("PORTFOLIO_NET_EXPECTANCY_NOT_POSITIVE")

    portfolio_pf = _number(portfolio, "profit_factor")
    if portfolio_pf is None:
        failures.append("PORTFOLIO_PROFIT_FACTOR_MISSING")
    elif portfolio_pf < float(
        gate.get(
            "minimum_portfolio_profit_factor",
            gate.get("minimum_profit_factor", 1.3),
        )
    ):
        failures.append("PORTFOLIO_PROFIT_FACTOR_TOO_LOW")

    portfolio_payoff = _number(portfolio, "payoff_ratio")
    if portfolio_payoff is None:
        failures.append("PORTFOLIO_PAYOFF_RATIO_MISSING")
    elif portfolio_payoff < float(
        gate.get(
            "minimum_portfolio_payoff_ratio",
            gate.get("minimum_payoff_ratio", 1.0),
        )
    ):
        failures.append("PORTFOLIO_PAYOFF_RATIO_TOO_LOW")

    net_profit = _number(portfolio, "net_profit_cny")
    if net_profit is None:
        failures.append("PORTFOLIO_NET_PROFIT_MISSING")
    elif net_profit <= 0:
        failures.append("PORTFOLIO_NET_PROFIT_NOT_POSITIVE")

    return tuple(dict.fromkeys(failures))
