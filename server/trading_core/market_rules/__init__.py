"""Pure market-mechanics functions with versioned caller-supplied rules."""

from .fees import (
    FeeBreakdown,
    FeeSchedule,
    calculate_order_fees,
    cash_effect,
    incremental_order_fee_delta,
)
from .instruments import (
    InstrumentRule,
    PriceBand,
    RuleCheck,
    RuleViolation,
    calculate_price_band,
    floor_buy_quantity,
    is_tick_aligned,
    validate_intent_against_rule,
)
from .settlement import (
    earliest_sell_date,
    is_lot_sellable,
    locked_quantity,
    sellable_quantity,
)

__all__ = [
    "FeeBreakdown",
    "FeeSchedule",
    "InstrumentRule",
    "PriceBand",
    "RuleCheck",
    "RuleViolation",
    "calculate_order_fees",
    "calculate_price_band",
    "cash_effect",
    "earliest_sell_date",
    "floor_buy_quantity",
    "is_lot_sellable",
    "is_tick_aligned",
    "incremental_order_fee_delta",
    "locked_quantity",
    "sellable_quantity",
    "validate_intent_against_rule",
]
