from __future__ import annotations

import math
from statistics import mean
from typing import Iterable


def trade_metrics(returns_pct: Iterable[float]) -> dict[str, float | int | None]:
    values = [float(value) for value in returns_pct if math.isfinite(float(value))]
    wins = [value for value in values if value > 0]
    losses = [-value for value in values if value < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    return {
        "trade_count": len(values),
        "win_rate": (len(wins) / len(values)) if values else None,
        "net_expectancy_pct": mean(values) if values else None,
        "average_win_pct": mean(wins) if wins else None,
        "average_loss_pct": mean(losses) if losses else None,
        "payoff_ratio": (
            mean(wins) / mean(losses)
            if wins and losses
            else (math.inf if wins and not losses else None)
        ),
        "profit_factor": (
            gross_profit / gross_loss
            if gross_loss > 0
            else (math.inf if gross_profit > 0 else None)
        ),
        "gross_profit_pct": gross_profit,
        "gross_loss_pct": gross_loss,
    }


def maximum_drawdown(equity_curve: Iterable[float]) -> float | None:
    peak = -math.inf
    worst = 0.0
    found = False
    for value in equity_curve:
        current = float(value)
        if not math.isfinite(current) or current <= 0:
            continue
        found = True
        peak = max(peak, current)
        worst = min(worst, current / peak - 1.0)
    return worst * 100.0 if found else None
