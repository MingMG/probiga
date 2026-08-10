"""ProBigA V2 deterministic paper-trading core.

This package is deliberately independent from the legacy ``st_sim_*`` engine.
It contains no broker adapter and cannot submit a real order.
"""

from .domain import (
    AccountSnapshot,
    InstrumentRule,
    IntentAction,
    OrderSide,
    PositionState,
    Quote,
    TradeIntent,
)
from .policy import PortfolioPolicy, RiskAdjudicator, load_portfolio_policy

__all__ = [
    "AccountSnapshot",
    "InstrumentRule",
    "IntentAction",
    "OrderSide",
    "PortfolioPolicy",
    "PositionState",
    "Quote",
    "RiskAdjudicator",
    "TradeIntent",
    "load_portfolio_policy",
]
