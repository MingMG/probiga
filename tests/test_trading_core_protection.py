from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.trading_core.contracts import (
    ExecutionIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)
from server.trading_core.execution import (
    ProtectionQuote,
    ProtectionRuleAttestation,
    ProtectionState,
    ProtectiveInstruction,
    bind_protection_rule_check,
    evaluate_protection,
)
from server.trading_core.market_rules.instruments import (
    RuleCheck,
    RuleViolation,
)


NOW = datetime(2026, 8, 3, 2, 0, tzinfo=timezone.utc)
ACCOUNT_STATE_HASH = "a" * 64


def _intent() -> ExecutionIntent:
    return ExecutionIntent(
        intent_id="protective-sell-1",
        account_id="paper-v2",
        decision_id="decision-1",
        instrument_id="600001.SH",
        side=OrderSide.SELL,
        quantity=100,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        created_at=NOW - timedelta(minutes=5),
        earliest_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
        idempotency_key="protective-sell-key-1",
        rule_version="rules-v1",
        fee_profile_version="fees-v1",
        execution_policy_version="execution-v1",
        limit_price=Decimal("9.80"),
    )


def _instruction() -> ProtectiveInstruction:
    return ProtectiveInstruction(
        protection_id="protection-1",
        intent=_intent(),
        trigger_price=Decimal("10.00"),
        trigger_version="explicit-stop-v1",
        account_state_hash=ACCOUNT_STATE_HASH,
    )


def _quote(price: str, **changes) -> ProtectionQuote:
    base = ProtectionQuote(
        event_id="quote-1",
        instrument_id="600001.SH",
        last_price=Decimal(price),
        quote_at=NOW - timedelta(seconds=1),
    )
    return replace(base, **changes)


def _attestation(
    instruction: ProtectiveInstruction | None = None,
    *,
    rule_check: RuleCheck | None = None,
    checked_at: datetime = NOW - timedelta(seconds=1),
    valid_until: datetime = NOW + timedelta(seconds=5),
) -> ProtectionRuleAttestation:
    instruction = instruction or _instruction()
    return bind_protection_rule_check(
        instruction.intent,
        account_state_hash=instruction.account_state_hash,
        rule_check=rule_check or RuleCheck(allowed=True, violations=()),
        checked_at=checked_at,
        valid_until=valid_until,
    )


def test_supervisor_only_releases_the_precommitted_sell_intent():
    instruction = _instruction()
    decision = evaluate_protection(
        instruction,
        quote=_quote("9.99"),
        evaluated_at=NOW,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(instruction),
    )
    retry = evaluate_protection(
        instruction,
        quote=_quote("9.99"),
        evaluated_at=NOW,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(instruction),
    )

    assert decision.state == ProtectionState.RELEASE
    assert decision.released_intent is instruction.intent
    assert decision == retry


def test_supervisor_does_not_release_above_explicit_trigger():
    decision = evaluate_protection(
        _instruction(),
        quote=_quote("10.01"),
        evaluated_at=NOW,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(),
    )

    assert decision.state == ProtectionState.ARMED
    assert decision.reason_code == "TRIGGER_NOT_REACHED"
    assert decision.released_intent is None


@pytest.mark.parametrize(
    ("quote", "state", "reason"),
    (
        (None, ProtectionState.WAIT_QUOTE, "QUOTE_UNAVAILABLE"),
        (
            _quote("9.90", quote_at=NOW + timedelta(seconds=1)),
            ProtectionState.WAIT_QUOTE,
            "QUOTE_NOT_FRESH",
        ),
        (
            _quote("9.90", quote_at=NOW - timedelta(seconds=4)),
            ProtectionState.WAIT_QUOTE,
            "QUOTE_NOT_FRESH",
        ),
        (
            _quote("9.90", suspended=True),
            ProtectionState.BLOCKED,
            "INSTRUMENT_SUSPENDED",
        ),
    ),
)
def test_supervisor_fails_closed_on_bad_quote(quote, state, reason):
    decision = evaluate_protection(
        _instruction(),
        quote=quote,
        evaluated_at=NOW,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(),
    )
    assert decision.state == state
    assert decision.reason_code == reason


def test_standard_rule_and_t1_failure_blocks_release():
    decision = evaluate_protection(
        _instruction(),
        quote=_quote("9.90"),
        evaluated_at=NOW,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(
            rule_check=RuleCheck(
                allowed=False,
                violations=(RuleViolation.T1_QUANTITY_LOCKED,),
            ),
        ),
    )

    assert decision.state == ProtectionState.BLOCKED
    assert decision.reason_code == "RULE_BLOCKED:T1_QUANTITY_LOCKED"
    assert decision.released_intent is None


def test_expiry_and_not_yet_active_take_precedence_without_quote():
    instruction = _instruction()
    early = evaluate_protection(
        instruction,
        quote=None,
        evaluated_at=instruction.intent.earliest_at - timedelta(seconds=1),
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(instruction),
    )
    expired = evaluate_protection(
        instruction,
        quote=None,
        evaluated_at=instruction.intent.expires_at,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(instruction),
    )

    assert early.state == ProtectionState.ARMED
    assert early.reason_code == "NOT_ACTIVE_YET"
    assert expired.state == ProtectionState.EXPIRED


def test_protective_instruction_rejects_buy_or_forged_intent():
    with pytest.raises(ValueError, match="SELL"):
        replace(_instruction(), intent=replace(_intent(), side=OrderSide.BUY))

    class ForgedIntent(ExecutionIntent):
        pass

    forged = object.__new__(ForgedIntent)
    with pytest.raises(TypeError, match="exactly ExecutionIntent"):
        ProtectiveInstruction(
            protection_id="protection-1",
            intent=forged,
            trigger_price=Decimal("10"),
            trigger_version="stop-v1",
            account_state_hash=ACCOUNT_STATE_HASH,
        )


@pytest.mark.parametrize(
    "rule_check",
    (
        RuleCheck(allowed=True, violations=(RuleViolation.T1_QUANTITY_LOCKED,)),
        RuleCheck(allowed=False, violations=()),
        RuleCheck(allowed=1, violations=()),
        RuleCheck(allowed=False, violations=[RuleViolation.T1_QUANTITY_LOCKED]),
    ),
)
def test_supervisor_rejects_inconsistent_or_malformed_rule_checks(rule_check):
    with pytest.raises((TypeError, ValueError)):
        bind_protection_rule_check(
            _intent(),
            account_state_hash=ACCOUNT_STATE_HASH,
            rule_check=rule_check,
            checked_at=NOW,
            valid_until=NOW + timedelta(seconds=1),
        )


def test_rule_attestation_must_bind_intent_account_state_rule_and_time():
    instruction = _instruction()
    wrong_state = bind_protection_rule_check(
        instruction.intent,
        account_state_hash="b" * 64,
        rule_check=RuleCheck(allowed=True, violations=()),
        checked_at=NOW,
        valid_until=NOW + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="not bound"):
        evaluate_protection(
            instruction,
            quote=_quote("9.90"),
            evaluated_at=NOW,
            quote_max_age=timedelta(seconds=3),
            rule_attestation=wrong_state,
        )

    stale = _attestation(
        instruction,
        checked_at=NOW - timedelta(seconds=10),
        valid_until=NOW - timedelta(seconds=1),
    )
    decision = evaluate_protection(
        instruction,
        quote=_quote("9.90"),
        evaluated_at=NOW,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=stale,
    )
    assert decision.state == ProtectionState.BLOCKED
    assert decision.reason_code == "RULE_ATTESTATION_NOT_CURRENT"


def test_release_id_is_stable_across_retry_time_decimal_and_timezone_forms():
    instruction = _instruction()
    equivalent_instruction = replace(
        instruction,
        trigger_price=Decimal("10.0"),
    )
    quote = _quote("9.900")
    equivalent_quote = ProtectionQuote(
        event_id=quote.event_id,
        instrument_id=quote.instrument_id,
        last_price=Decimal("9.90"),
        quote_at=quote.quote_at.astimezone(timezone(timedelta(hours=8))),
    )
    first = evaluate_protection(
        instruction,
        quote=quote,
        evaluated_at=NOW,
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(instruction),
    )
    retry = evaluate_protection(
        equivalent_instruction,
        quote=equivalent_quote,
        evaluated_at=NOW + timedelta(seconds=1),
        quote_max_age=timedelta(seconds=3),
        rule_attestation=_attestation(
            equivalent_instruction,
            valid_until=NOW + timedelta(seconds=5),
        ),
    )

    assert first.state == retry.state == ProtectionState.RELEASE
    assert first.decision_id == retry.decision_id
    assert first.quote_hash == retry.quote_hash
