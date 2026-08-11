from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import inspect

import pytest

from server.trading_core.contracts import OrderSide
from server.trading_core.execution import risk_envelope as risk_module
from server.trading_core.execution.risk_envelope import (
    ExecutionRiskAccountSnapshot,
    ExecutionRiskOrder,
    ExecutionRiskPosition,
    NeutralExecutionRiskDecision,
    NeutralExecutionRiskEnvelope,
    RiskAccountStatus,
    RiskEnvelopeDecisionStatus,
    RiskEnvelopeReason,
    RiskNotionalRounding,
    RiskReconciliationStatus,
    evaluate_neutral_execution_risk,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 3, 2, 0, tzinfo=UTC)


def _envelope(**overrides: object) -> NeutralExecutionRiskEnvelope:
    values: dict[str, object] = {
        "envelope_version": "neutral-hard-risk-v1",
        "snapshot_max_age": timedelta(seconds=30),
        "max_order_quantity": 1_000,
        "max_order_notional": Decimal("20000"),
        "max_instrument_position_quantity": 2_000,
        "max_total_position_notional": Decimal("50000"),
        "max_total_pending_order_notional": Decimal("30000"),
        "max_absolute_concentration": Decimal("0.20"),
    }
    values.update(overrides)
    return NeutralExecutionRiskEnvelope(**values)  # type: ignore[arg-type]


def _positions() -> tuple[ExecutionRiskPosition, ...]:
    return (
        ExecutionRiskPosition(
            instrument_id="000001.SZ",
            quantity=50,
            notional=Decimal("500"),
        ),
        ExecutionRiskPosition(
            instrument_id="600001.SH",
            quantity=100,
            notional=Decimal("1000"),
        ),
    )


def _account(**overrides: object) -> ExecutionRiskAccountSnapshot:
    values: dict[str, object] = {
        "snapshot_id": "account-snapshot-1",
        "account_id": "paper-account-1",
        "account_status": RiskAccountStatus.ACTIVE,
        "reconciliation_status": RiskReconciliationStatus.PASS,
        "observed_at": NOW - timedelta(seconds=5),
        "account_equity": Decimal("100000"),
        "available_cash": Decimal("50000"),
        "total_pending_order_notional": Decimal("1000"),
        "positions": _positions(),
    }
    values.update(overrides)
    return ExecutionRiskAccountSnapshot(**values)  # type: ignore[arg-type]


def _order(**overrides: object) -> ExecutionRiskOrder:
    values: dict[str, object] = {
        "order_id": "order-1",
        "account_id": "paper-account-1",
        "instrument_id": "600001.SH",
        "side": OrderSide.BUY,
        "quantity": 100,
        "limit_price": Decimal("10"),
        "fee_reserve": Decimal("5"),
    }
    values.update(overrides)
    return ExecutionRiskOrder(**values)  # type: ignore[arg-type]


def test_approved_buy_has_deterministic_mechanical_projection_and_hash() -> None:
    envelope = _envelope()
    account = _account()
    order = _order()

    first = evaluate_neutral_execution_risk(
        envelope,
        account=account,
        order=order,
        evaluated_at=NOW,
    )
    second = evaluate_neutral_execution_risk(
        envelope,
        account=account,
        order=order,
        evaluated_at=NOW,
    )

    assert first == second
    assert first.status == RiskEnvelopeDecisionStatus.APPROVED
    assert first.reason_codes == ()
    assert first.order_notional == Decimal("1000.00")
    assert first.cash_required == Decimal("1005.00")
    assert first.projected_instrument_quantity == 200
    assert first.projected_instrument_notional == Decimal("2000.00")
    assert first.projected_total_position_notional == Decimal("2500.00")
    assert first.projected_total_pending_order_notional == Decimal("2000.00")
    assert first.projected_max_instrument_notional == Decimal("2000.00")
    assert len(first.decision_hash) == 64
    with pytest.raises(FrozenInstanceError):
        first.status = RiskEnvelopeDecisionStatus.BLOCKED  # type: ignore[misc]


def test_notional_rounding_is_versioned_and_independent_of_global_context() -> None:
    order = _order(quantity=7, limit_price=Decimal("0.985"))
    down = _envelope()
    half_up = _envelope(
        envelope_version="neutral-hard-risk-half-up-v1",
        notional_rounding=RiskNotionalRounding.HALF_UP,
    )

    assert down.order_notional(order) == Decimal("6.89")
    assert half_up.order_notional(order) == Decimal("6.90")
    assert down.envelope_hash != half_up.envelope_hash


def test_account_snapshot_is_canonical_and_content_addressed() -> None:
    first = _account()
    second = _account(positions=tuple(reversed(_positions())))

    assert first.positions == second.positions == _positions()
    assert first.total_position_notional == Decimal("1500")
    assert first.snapshot_hash == second.snapshot_hash
    with pytest.raises(ValueError, match="duplicate instruments"):
        _account(positions=(_positions()[0], _positions()[0]))


def test_account_state_reconciliation_and_future_snapshot_fail_closed() -> None:
    account = _account(
        account_id="other-account",
        account_status=RiskAccountStatus.SUSPENDED,
        reconciliation_status=RiskReconciliationStatus.BLOCKED,
        observed_at=NOW + timedelta(microseconds=1),
    )

    decision = evaluate_neutral_execution_risk(
        _envelope(),
        account=account,
        order=_order(),
        evaluated_at=NOW,
    )

    assert decision.status == RiskEnvelopeDecisionStatus.BLOCKED
    assert decision.reason_codes == (
        RiskEnvelopeReason.ACCOUNT_ID_MISMATCH,
        RiskEnvelopeReason.ACCOUNT_NOT_ACTIVE,
        RiskEnvelopeReason.RECONCILIATION_NOT_PASS,
        RiskEnvelopeReason.SNAPSHOT_FROM_FUTURE,
    )


def test_snapshot_freshness_boundary_is_inclusive_and_stale_afterward() -> None:
    envelope = _envelope(snapshot_max_age=timedelta(seconds=30))
    boundary = _account(observed_at=NOW - timedelta(seconds=30))
    stale = _account(observed_at=NOW - timedelta(seconds=30, microseconds=1))

    assert evaluate_neutral_execution_risk(
        envelope,
        account=boundary,
        order=_order(),
        evaluated_at=NOW,
    ).status == RiskEnvelopeDecisionStatus.APPROVED
    assert evaluate_neutral_execution_risk(
        envelope,
        account=stale,
        order=_order(),
        evaluated_at=NOW,
    ).reason_codes == (RiskEnvelopeReason.SNAPSHOT_STALE,)


def test_every_absolute_order_and_account_limit_has_a_stable_reason_code() -> None:
    envelope = _envelope(
        max_order_quantity=50,
        max_order_notional=Decimal("500"),
        max_instrument_position_quantity=150,
        max_total_position_notional=Decimal("2000"),
        max_total_pending_order_notional=Decimal("1500"),
        max_absolute_concentration=Decimal("0.015"),
    )
    account = _account(available_cash=Decimal("500"))

    decision = evaluate_neutral_execution_risk(
        envelope,
        account=account,
        order=_order(),
        evaluated_at=NOW,
    )

    assert decision.reason_codes == (
        RiskEnvelopeReason.ORDER_QUANTITY_LIMIT_EXCEEDED,
        RiskEnvelopeReason.ORDER_NOTIONAL_LIMIT_EXCEEDED,
        RiskEnvelopeReason.INSTRUMENT_POSITION_LIMIT_EXCEEDED,
        RiskEnvelopeReason.TOTAL_POSITION_NOTIONAL_LIMIT_EXCEEDED,
        RiskEnvelopeReason.TOTAL_PENDING_ORDER_NOTIONAL_LIMIT_EXCEEDED,
        RiskEnvelopeReason.INSUFFICIENT_AVAILABLE_CASH,
        RiskEnvelopeReason.ABSOLUTE_CONCENTRATION_LIMIT_EXCEEDED,
    )
    assert decision.decision_hash == evaluate_neutral_execution_risk(
        envelope,
        account=account,
        order=_order(),
        evaluated_at=NOW,
    ).decision_hash


def test_sell_has_no_cash_requirement_and_mechanically_reduces_exposure() -> None:
    account = _account(
        available_cash=Decimal("0"),
        positions=(
            ExecutionRiskPosition(
                instrument_id="600001.SH",
                quantity=100,
                notional=Decimal("1000"),
            ),
        ),
    )
    order = _order(
        side=OrderSide.SELL,
        quantity=100,
        fee_reserve=Decimal("0"),
    )

    decision = evaluate_neutral_execution_risk(
        _envelope(),
        account=account,
        order=order,
        evaluated_at=NOW,
    )

    assert decision.status == RiskEnvelopeDecisionStatus.APPROVED
    assert decision.cash_required == 0
    assert decision.projected_instrument_quantity == 0
    assert decision.projected_instrument_notional == 0
    assert decision.projected_total_position_notional == 0


def test_sell_quantity_cannot_exceed_snapshot_position() -> None:
    decision = evaluate_neutral_execution_risk(
        _envelope(),
        account=_account(
            positions=(
                ExecutionRiskPosition(
                    instrument_id="600001.SH",
                    quantity=100,
                    notional=Decimal("1000"),
                ),
            ),
        ),
        order=_order(
            side=OrderSide.SELL,
            quantity=101,
            fee_reserve=Decimal("0"),
        ),
        evaluated_at=NOW,
    )

    assert decision.status == RiskEnvelopeDecisionStatus.BLOCKED
    assert RiskEnvelopeReason.SELL_QUANTITY_EXCEEDS_POSITION in (
        decision.reason_codes
    )


def test_partial_sell_uses_snapshot_position_value_not_order_limit_price() -> None:
    account = _account(
        positions=(
            ExecutionRiskPosition(
                instrument_id="600001.SH",
                quantity=100,
                notional=Decimal("1000"),
            ),
        ),
    )
    order = _order(
        side=OrderSide.SELL,
        quantity=1,
        limit_price=Decimal("1000"),
        fee_reserve=Decimal("0"),
    )

    decision = evaluate_neutral_execution_risk(
        _envelope(max_order_notional=Decimal("2000")),
        account=account,
        order=order,
        evaluated_at=NOW,
    )

    assert decision.status == RiskEnvelopeDecisionStatus.APPROVED
    assert decision.order_notional == Decimal("1000.00")
    assert decision.projected_instrument_quantity == 99
    assert decision.projected_instrument_notional == Decimal("990.00")
    assert decision.projected_total_position_notional == Decimal("990.00")


class _DecimalSubclass(Decimal):
    pass


class _DatetimeSubclass(datetime):
    pass


class _IntSubclass(int):
    pass


class _EnvelopeSubclass(NeutralExecutionRiskEnvelope):
    pass


@pytest.mark.parametrize(
    "factory",
    (
        lambda: _order(limit_price=_DecimalSubclass("10")),
        lambda: _order(quantity=_IntSubclass(100)),
        lambda: _account(observed_at=_DatetimeSubclass(2026, 8, 3, tzinfo=UTC)),
        lambda: _account(account_status="ACTIVE"),
        lambda: _account(positions=list(_positions())),
        lambda: _envelope(max_order_notional=_DecimalSubclass("1000")),
    ),
)
def test_owned_inputs_reject_subclasses_and_coercive_types(factory) -> None:
    with pytest.raises(TypeError):
        factory()


def test_evaluator_rejects_owned_aggregate_subclasses() -> None:
    subclass = _EnvelopeSubclass(
        envelope_version="subclass",
        snapshot_max_age=timedelta(seconds=30),
        max_order_quantity=1,
        max_order_notional=Decimal("1"),
        max_instrument_position_quantity=1,
        max_total_position_notional=Decimal("1"),
        max_total_pending_order_notional=Decimal("1"),
        max_absolute_concentration=Decimal("1"),
    )
    with pytest.raises(TypeError, match="exactly NeutralExecutionRiskEnvelope"):
        evaluate_neutral_execution_risk(
            subclass,
            account=_account(),
            order=_order(),
            evaluated_at=NOW,
        )


def test_malformed_limits_and_positions_fail_during_construction() -> None:
    with pytest.raises(ValueError, match="cannot exceed one"):
        _envelope(max_absolute_concentration=Decimal("1.01"))
    with pytest.raises(ValueError, match="both be zero or positive"):
        ExecutionRiskPosition(
            instrument_id="600001.SH",
            quantity=1,
            notional=Decimal("0"),
        )
    with pytest.raises(ValueError, match="positive"):
        _account(account_equity=Decimal("0"))


def test_evaluator_revalidates_low_level_frozen_object_mutations() -> None:
    forged_envelope = replace(_envelope())
    object.__setattr__(
        forged_envelope,
        "max_order_quantity",
        10_000_000,
    )
    with pytest.raises(ValueError, match="canonical reconstructed"):
        evaluate_neutral_execution_risk(
            forged_envelope,
            account=_account(),
            order=_order(),
            evaluated_at=NOW,
        )

    forged_account = replace(_account())
    object.__setattr__(forged_account, "available_cash", Decimal("999999"))
    with pytest.raises(ValueError, match="canonical reconstructed"):
        evaluate_neutral_execution_risk(
            _envelope(),
            account=forged_account,
            order=_order(),
            evaluated_at=NOW,
        )

    forged_order = replace(_order())
    object.__setattr__(forged_order, "quantity", 1)
    with pytest.raises(ValueError, match="canonical reconstructed"):
        evaluate_neutral_execution_risk(
            _envelope(),
            account=_account(),
            order=forged_order,
            evaluated_at=NOW,
        )


def test_contract_surface_contains_no_strategy_opinion_fields() -> None:
    contract_types = (
        ExecutionRiskPosition,
        ExecutionRiskAccountSnapshot,
        ExecutionRiskOrder,
        NeutralExecutionRiskEnvelope,
        NeutralExecutionRiskDecision,
    )
    forbidden_tokens = {
        "candidate",
        "industry",
        "market_state",
        "rank",
        "regime",
        "return",
        "score",
        "sector",
        "strategy",
        "theme",
        "trend",
    }
    actual_names = {
        item.name.casefold()
        for contract_type in contract_types
        for item in fields(contract_type)
    }
    assert all(
        token not in field_name
        for token in forbidden_tokens
        for field_name in actual_names
    )
    signature = inspect.signature(risk_module.evaluate_neutral_execution_risk)
    assert tuple(signature.parameters) == (
        "envelope",
        "account",
        "order",
        "evaluated_at",
    )
