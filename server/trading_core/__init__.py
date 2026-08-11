"""Strategy-neutral trading primitives shared by future trading facades.

This package deliberately contains no stock-selection, scoring, portfolio, or
strategy-version imports.  Its contracts and pure functions describe only
execution mechanics that must mean the same thing for every caller.
"""

from .contracts import (
    ExecutionIntent,
    ExecutionResult,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    execution_intent_idempotency_key,
    execution_result_idempotency_key,
)

__all__ = [
    "ExecutionIntent",
    "ExecutionResult",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "TimeInForce",
    "execution_intent_idempotency_key",
    "execution_result_idempotency_key",
]
