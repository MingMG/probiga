"""Dynamic position state machine: trend breaks exit early; strength may extend."""
from __future__ import annotations

from decimal import Decimal

from .domain import (
    IntentAction,
    PositionDecision,
    PositionFacts,
    PositionState,
)


ALLOWED_TRANSITIONS: dict[PositionState, frozenset[PositionState]] = {
    PositionState.OPENING: frozenset({
        PositionState.VALID_STRONG, PositionState.VALID, PositionState.WEAKENED,
        PositionState.BROKEN, PositionState.RISK_EXIT,
        PositionState.EXIT_PENDING_T1,
        PositionState.EXIT_PENDING_LIQUIDITY,
    }),
    PositionState.VALID_STRONG: frozenset({
        PositionState.VALID, PositionState.WEAKENED, PositionState.BROKEN,
        PositionState.RISK_EXIT,
        PositionState.EXIT_PENDING_T1,
        PositionState.EXIT_PENDING_LIQUIDITY,
    }),
    PositionState.VALID: frozenset({
        PositionState.VALID_STRONG, PositionState.WEAKENED,
        PositionState.BROKEN, PositionState.RISK_EXIT,
        PositionState.EXIT_PENDING_T1,
        PositionState.EXIT_PENDING_LIQUIDITY,
    }),
    PositionState.WEAKENED: frozenset({
        PositionState.VALID_STRONG, PositionState.VALID,
        PositionState.BROKEN, PositionState.RISK_EXIT,
        PositionState.EXIT_PENDING_T1,
        PositionState.EXIT_PENDING_LIQUIDITY,
    }),
    PositionState.BROKEN: frozenset({
        PositionState.EXIT_PENDING_T1,
        PositionState.EXIT_PENDING_LIQUIDITY,
        PositionState.CLOSED,
    }),
    PositionState.RISK_EXIT: frozenset({
        PositionState.EXIT_PENDING_T1,
        PositionState.EXIT_PENDING_LIQUIDITY,
        PositionState.CLOSED,
    }),
    PositionState.EXIT_PENDING_T1: frozenset({
        PositionState.EXIT_PENDING_LIQUIDITY, PositionState.CLOSED,
    }),
    PositionState.EXIT_PENDING_LIQUIDITY: frozenset({PositionState.CLOSED}),
    PositionState.CLOSED: frozenset(),
}


def assert_transition(previous: PositionState, next_state: PositionState) -> None:
    if previous == next_state:
        return
    if next_state not in ALLOWED_TRANSITIONS[previous]:
        raise ValueError(f"illegal position transition: {previous} -> {next_state}")


def monotonic_protective_stop(current: Decimal, proposed: Decimal) -> Decimal:
    if current <= 0:
        return proposed
    return max(current, proposed)


def evaluate_position(
    facts: PositionFacts,
    *,
    maximum_add_count: int = 1,
) -> PositionDecision:
    if facts.current_state == PositionState.CLOSED:
        raise ValueError("closed position cannot be evaluated")
    protective_stop = monotonic_protective_stop(
        facts.current_protective_stop,
        facts.proposed_protective_stop,
    )

    # Once an exit has entered a waiting state it is irreversible.  Continue
    # requesting the exit until the ledger closes the lot; do not let a later
    # price bounce resurrect the position.
    if facts.current_state == PositionState.EXIT_PENDING_T1:
        next_state = (
            PositionState.EXIT_PENDING_LIQUIDITY
            if facts.can_sell_today and not facts.liquidity_available
            else PositionState.EXIT_PENDING_T1
        )
        return PositionDecision(
            previous_state=facts.current_state,
            next_state=next_state,
            action=IntentAction.EXIT,
            target_quantity=0,
            protective_stop=protective_stop,
            reason_code=(
                "EXIT_BLOCKED_LIQUIDITY"
                if next_state == PositionState.EXIT_PENDING_LIQUIDITY
                else "EXIT_PENDING_T1_RETRY"
            ),
        )
    if facts.current_state == PositionState.EXIT_PENDING_LIQUIDITY:
        return PositionDecision(
            previous_state=facts.current_state,
            next_state=PositionState.EXIT_PENDING_LIQUIDITY,
            action=IntentAction.EXIT,
            target_quantity=0,
            protective_stop=protective_stop,
            reason_code="EXIT_PENDING_LIQUIDITY_RETRY",
        )
    if facts.current_state in {
        PositionState.BROKEN,
        PositionState.RISK_EXIT,
    }:
        if not facts.can_sell_today:
            next_state = PositionState.EXIT_PENDING_T1
            reason_code = "EXIT_BLOCKED_T1"
        elif not facts.liquidity_available:
            next_state = PositionState.EXIT_PENDING_LIQUIDITY
            reason_code = "EXIT_BLOCKED_LIQUIDITY"
        else:
            next_state = facts.current_state
            reason_code = "EXIT_COMMITMENT_RETRY"
        assert_transition(facts.current_state, next_state)
        return PositionDecision(
            previous_state=facts.current_state,
            next_state=next_state,
            action=IntentAction.EXIT,
            target_quantity=0,
            protective_stop=protective_stop,
            reason_code=reason_code,
        )

    # Fixed priority: account/market risk, hard stop, invalidation, protection,
    # then trend quality. Holding days are intentionally absent.
    if facts.risk_event:
        desired = PositionState.RISK_EXIT
        reason = "RISK_EVENT"
    elif facts.hard_stop_breached or (
        protective_stop > 0 and facts.last_price <= protective_stop
    ):
        desired = PositionState.RISK_EXIT
        reason = "PROTECTIVE_STOP_BREACHED"
    elif facts.invalidated or not facts.trend_valid:
        desired = PositionState.BROKEN
        reason = "STRATEGY_INVALIDATED"
    elif facts.theme_faded:
        desired = PositionState.WEAKENED
        reason = "THEME_FADED"
    elif facts.trend_strong:
        desired = PositionState.VALID_STRONG
        reason = "LOGIC_STRENGTHENED"
    else:
        desired = PositionState.VALID
        reason = "LOGIC_VALID"

    if desired in {PositionState.BROKEN, PositionState.RISK_EXIT}:
        if not facts.can_sell_today:
            next_state = PositionState.EXIT_PENDING_T1
            reason = "EXIT_BLOCKED_T1"
        elif not facts.liquidity_available:
            next_state = PositionState.EXIT_PENDING_LIQUIDITY
            reason = "EXIT_BLOCKED_LIQUIDITY"
        else:
            next_state = desired
        target = 0
        action = IntentAction.EXIT
    elif desired == PositionState.WEAKENED:
        next_state = desired
        target = facts.approved_target_quantity // 2
        action = IntentAction.REDUCE if facts.current_quantity > target else IntentAction.HOLD
    elif (
        desired == PositionState.VALID_STRONG
        and facts.add_count < maximum_add_count
        and facts.last_price > facts.average_cost
        and facts.current_quantity < facts.approved_target_quantity
    ):
        next_state = desired
        target = facts.approved_target_quantity
        action = IntentAction.ADD
    else:
        next_state = desired
        target = facts.current_quantity
        action = IntentAction.HOLD

    assert_transition(facts.current_state, next_state)
    return PositionDecision(
        previous_state=facts.current_state,
        next_state=next_state,
        action=action,
        target_quantity=max(0, target),
        protective_stop=protective_stop,
        reason_code=reason,
    )
