from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pytest

from server.integrations.v2_accounting_evidence_writer import (
    AccountingEvidenceAppendConflictError,
    AccountingEvidenceAppendStatus,
    AccountingEvidenceCanonicalRowError,
    AccountingEvidenceTransactionError,
    append_fill_accounting_outcome,
)
from server.integrations.v2_accounting_evidence_writer import writer as writer_impl
from server.trading_v2.accounting_evidence import (
    AccountingEvidenceInvariantError,
    AccountingOutcomeFinalizationStatus,
    FillAccountingOutcome,
    LotAccountingEffect,
    LotEffectKind,
    LotSnapshot,
    finalize_fill_accounting_outcome,
    validate_fill_accounting_finalization,
    validate_fill_accounting_outcome,
)
from server.trading_v2.domain import OrderStatus, PositionState
from server.trading_v2.execution_evidence import (
    CanonicalJson,
    CashEventBinding,
    FillExecutionEvidence,
    HistoryOrigin,
    OrderTransitionEvidence,
    OrderTransitionKind,
)
from tools.trading_v2_evidence_behavioral_scenario import build_behavioral_scenario


ZONE = ZoneInfo("Asia/Shanghai")


def _at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second, tzinfo=ZONE)


def _buy_fill() -> FillExecutionEvidence:
    return build_behavioral_scenario().cases[2].evidence  # type: ignore[return-value]


def _sell_fill() -> FillExecutionEvidence:
    base = _buy_fill()
    fill_payload = base.fill_payload.value()
    fill_payload.update(
        {
            "side": "SELL",
            "fee_amount": "0.80",
            "net_cash_amount": "999.20",
        }
    )
    order_payload = base.order_payload.value()
    order_payload["side"] = "SELL"
    canonical_order = CanonicalJson.from_value(order_payload)
    matcher_request = base.matcher_request.value()
    matcher_request["order_payload_hash"] = canonical_order.payload_hash
    canonical_matcher_request = CanonicalJson.from_value(matcher_request)
    matcher_response = base.matcher_response.value()
    matcher_response.update(
        {
            "side": "SELL",
            "matcher_request_hash": canonical_matcher_request.payload_hash,
        }
    )
    canonical_matcher_response = CanonicalJson.from_value(matcher_response)
    accounting_request = base.accounting_request.value()
    accounting_request.update(
        {
            "side": "SELL",
            "fee_amount": "0.80",
            "net_cash_amount": "999.20",
            "matcher_output_hash": canonical_matcher_response.payload_hash,
        }
    )
    return replace(
        base,
        fill_payload=CanonicalJson.from_value(fill_payload),
        order_payload=canonical_order,
        matcher_request=canonical_matcher_request,
        matcher_response=canonical_matcher_response,
        accounting_request=CanonicalJson.from_value(accounting_request),
    )


def _cash_binding(fill: FillExecutionEvidence) -> CashEventBinding:
    payload = fill.fill_payload.value()
    side = str(payload["side"])
    before = Decimal("100000.00")
    amount = Decimal(str(payload["net_cash_amount"]))
    after = before + amount
    event_id = f"cash-{side.lower()}-1"
    return CashEventBinding(
        cash_event_id=event_id,
        account_id=fill.account_id,
        account_sequence=1,
        cash_event_type=f"{side}_FILL",
        cash_event_payload=CanonicalJson.from_value(
            {
                "account_id": fill.account_id,
                "amount": format(amount, ".2f"),
                "balance_after": format(after, ".2f"),
                "business_event_key": f"FILL:{payload['idempotency_key']}",
                "cash_event_id": event_id,
                "created_at": _at(10, 0, 3),
                "event_type": f"{side}_FILL",
                "occurred_at": fill.executed_at,
                "related_fill_id": fill.fill_id,
                "related_order_id": fill.order_id,
                "reversal_of": None,
            }
        ),
        occurred_at=fill.executed_at,
        bound_at=_at(10, 0, 5),
        provenance=fill.provenance,
        related_order_id=fill.order_id,
        related_fill_id=fill.fill_id,
        fill_execution_evidence=fill,
    )


def _fill_transition(fill: FillExecutionEvidence) -> OrderTransitionEvidence:
    return OrderTransitionEvidence(
        order_id=fill.order_id,
        account_id=fill.account_id,
        order_payload=fill.order_payload,
        transition_sequence=4,
        from_status=OrderStatus.QUEUED,
        to_status=OrderStatus.FILLED,
        previous_filled_quantity=0,
        next_filled_quantity=int(fill.fill_payload.value()["quantity"]),
        transition_kind=OrderTransitionKind.FILL_APPLIED,
        source_event_type="FILL_APPLIED",
        source_event_id=fill.fill_id,
        source_event_hash="9" * 64,
        occurred_at=fill.executed_at,
        recorded_at=_at(10, 0, 5),
        provenance=fill.provenance,
        related_fill_id=fill.fill_id,
        fill_execution_evidence=fill,
    )


def _lot(
    *,
    fill: FillExecutionEvidence,
    lot_id: str,
    opened: date,
    settlement: date,
    original: int,
    remaining: int,
    version: int,
    state: PositionState = PositionState.VALID,
    opened_fill_id: str = "older-fill",
    closed_at: datetime | None = None,
    created_at: datetime = _at(9),
) -> LotSnapshot:
    return LotSnapshot(
        lot_id=lot_id,
        account_id=fill.account_id,
        stock_code=fill.stock_code,
        theme_code="",
        strategy_version="strategy-v1",
        opened_fill_id=opened_fill_id,
        opened_trade_date=opened,
        settlement_date=settlement,
        original_quantity=original,
        remaining_quantity=remaining,
        cost_price=Decimal("10.000000"),
        allocated_buy_fee=Decimal("0.30"),
        position_state=state,
        approved_target_quantity=original,
        add_count=0,
        initial_stop=Decimal("9.000000"),
        protective_stop=Decimal("9.000000"),
        invalidation_condition="close below 9",
        version=version,
        created_at=created_at,
        closed_at=closed_at,
    )


def _buy_outcome() -> FillAccountingOutcome:
    fill = _buy_fill()
    after = _lot(
        fill=fill,
        lot_id=f"LOT:{fill.fill_id}",
        opened=fill.quote_evidence.trade_date,
        settlement=date(2026, 8, 4),
        original=100,
        remaining=100,
        version=1,
        state=PositionState.OPENING,
        opened_fill_id=fill.fill_id,
        created_at=_at(10, 0, 3),
    )
    effect = LotAccountingEffect(
        fill_execution_evidence=fill,
        effect_sequence=0,
        lot_transition_sequence=0,
        effect_kind=LotEffectKind.BUY_CREATE,
        before_lot=None,
        after_lot=after,
        consumed_quantity=0,
        occurred_at=fill.executed_at,
        bound_at=_at(10, 0, 6),
        provenance=fill.provenance,
    )
    return FillAccountingOutcome(
        fill_execution_evidence=fill,
        cash_binding=_cash_binding(fill),
        order_transition=_fill_transition(fill),
        account_cash_before=Decimal("100000.00"),
        account_cash_after=Decimal("98999.70"),
        lot_effects=(effect,),
        recorded_at=_at(10, 0, 7),
        provenance=fill.provenance,
    )


def _sell_outcome() -> FillAccountingOutcome:
    fill = _sell_fill()
    first_before = _lot(
        fill=fill,
        lot_id="lot-a",
        opened=date(2026, 7, 30),
        settlement=date(2026, 7, 31),
        original=60,
        remaining=60,
        version=3,
        created_at=_at(8, 30),
    )
    first_after = replace(
        first_before,
        remaining_quantity=0,
        version=4,
        position_state=PositionState.CLOSED,
        closed_at=fill.executed_at,
    )
    first = LotAccountingEffect(
        fill_execution_evidence=fill,
        effect_sequence=0,
        lot_transition_sequence=0,
        effect_kind=LotEffectKind.SELL_FIFO_CONSUME,
        before_lot=first_before,
        after_lot=first_after,
        consumed_quantity=60,
        occurred_at=fill.executed_at,
        bound_at=_at(10, 0, 6),
        provenance=fill.provenance,
    )
    second_before = _lot(
        fill=fill,
        lot_id="lot-b",
        opened=date(2026, 8, 1),
        settlement=date(2026, 8, 2),
        original=80,
        remaining=80,
        version=1,
        created_at=_at(8, 45),
    )
    second_after = replace(second_before, remaining_quantity=40, version=2)
    second = LotAccountingEffect(
        fill_execution_evidence=fill,
        effect_sequence=1,
        lot_transition_sequence=0,
        effect_kind=LotEffectKind.SELL_FIFO_CONSUME,
        before_lot=second_before,
        after_lot=second_after,
        consumed_quantity=40,
        occurred_at=fill.executed_at,
        bound_at=_at(10, 0, 6),
        provenance=fill.provenance,
        previous_effect_id=first.lot_transition_evidence_id,
        previous_effect_hash=first.effect_hash,
    )
    return FillAccountingOutcome(
        fill_execution_evidence=fill,
        cash_binding=_cash_binding(fill),
        order_transition=_fill_transition(fill),
        account_cash_before=Decimal("100000.00"),
        account_cash_after=Decimal("100999.20"),
        lot_effects=(first, second),
        recorded_at=_at(10, 0, 7),
        provenance=fill.provenance,
    )


def test_buy_outcome_fully_binds_fill_cash_order_and_created_lot() -> None:
    first = _buy_outcome()
    second = _buy_outcome()
    validate_fill_accounting_outcome(first)
    assert first == second
    assert first.accounting_outcome_id == first.outcome_hash
    assert first.lot_effects[0].after_lot.opened_fill_id == first.fill_execution_evidence.fill_id
    assert first.lot_effects[0].after_lot.snapshot_hash == second.lot_effects[0].after_lot.snapshot_hash


def test_accounting_domain_rejects_subsecond_time_before_hashing() -> None:
    outcome = _buy_outcome()
    with pytest.raises(ValueError, match="whole-second precision"):
        replace(
            outcome,
            recorded_at=outcome.recorded_at.replace(microsecond=1),
        )


def test_finalization_is_deterministic_ordered_and_only_allows_final() -> None:
    outcome = _sell_outcome()
    first = finalize_fill_accounting_outcome(outcome)
    second = finalize_fill_accounting_outcome(outcome)
    assert first == second
    assert first.finalization_id == first.finalization_hash
    assert first.finalization_status is AccountingOutcomeFinalizationStatus.FINAL
    assert first.effect_hashes_json == CanonicalJson.from_value(
        [item.effect_hash for item in outcome.lot_effects]
    ).json_text
    assert outcome.lot_effect_count == 2
    assert outcome.total_effect_quantity == 100
    validate_fill_accounting_finalization(first)
    with pytest.raises(ValueError, match="cannot finalize before"):
        replace(first, finalized_at=_at(10, 0, 6))
    object.__setattr__(first, "effect_hashes_json", "[]")
    with pytest.raises(AccountingEvidenceInvariantError, match="canonical reconstruction"):
        validate_fill_accounting_finalization(first)


def test_outcome_rejects_cash_and_non_fill_transition_mismatch() -> None:
    outcome = _buy_outcome()
    with pytest.raises(ValueError, match="cash before/after"):
        replace(outcome, account_cash_before=Decimal("99999.99"))
    transition = outcome.order_transition
    with pytest.raises(ValueError, match="only FILL_APPLIED"):
        replace(
            transition,
            transition_kind=OrderTransitionKind.STATUS_CHANGE,
            related_fill_id=None,
            fill_execution_evidence=None,
        )


def test_buy_effect_rejects_incomplete_or_fabricated_after_row() -> None:
    outcome = _buy_outcome()
    effect = outcome.lot_effects[0]
    with pytest.raises(ValueError, match="canonical fill outcome"):
        replace(effect, after_lot=replace(effect.after_lot, version=2))
    with pytest.raises(ValueError, match="zero consumption"):
        replace(effect, consumed_quantity=1)


def test_sell_binds_strict_fifo_versions_settlement_chain_and_total() -> None:
    outcome = _sell_outcome()
    validate_fill_accounting_outcome(outcome)
    first, second = outcome.lot_effects
    assert first.after_lot.version == first.before_lot.version + 1  # type: ignore[union-attr]
    assert second.after_lot.remaining_quantity == 40
    assert second.previous_effect_id == first.lot_transition_evidence_id
    reordered_first = replace(
        second,
        effect_sequence=0,
        previous_effect_id=None,
        previous_effect_hash=None,
    )
    reordered_second = replace(
        first,
        effect_sequence=1,
        previous_effect_id=reordered_first.lot_transition_evidence_id,
        previous_effect_hash=reordered_first.effect_hash,
    )
    with pytest.raises(ValueError, match="deterministic FIFO"):
        replace(outcome, lot_effects=(reordered_first, reordered_second))
    with pytest.raises(ValueError, match="version"):
        replace(first, after_lot=replace(first.after_lot, version=5))
    with pytest.raises(ValueError, match="unsettled"):
        replace(
            first,
            before_lot=replace(first.before_lot, settlement_date=date(2026, 8, 4)),  # type: ignore[arg-type]
            after_lot=replace(first.after_lot, settlement_date=date(2026, 8, 4)),
        )
    with pytest.raises(ValueError, match="cover the full"):
        replace(outcome, lot_effects=(first,))


def test_sell_chain_discontinuity_and_complete_history_backclaim_are_rejected() -> None:
    outcome = _sell_outcome()
    first, second = outcome.lot_effects
    with pytest.raises(ValueError, match="discontinuous"):
        replace(
            outcome,
            lot_effects=(
                first,
                replace(
                    second,
                    previous_effect_id="a" * 64,
                    previous_effect_hash="a" * 64,
                ),
            ),
        )
    complete = replace(
        outcome.provenance,
        history_origin=HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN,
    )
    complete_fill = replace(outcome.fill_execution_evidence, provenance=complete)
    with pytest.raises(ValueError, match="complete SELL lot history"):
        replace(
            first,
            fill_execution_evidence=complete_fill,
            provenance=complete,
        )
    assert first.provenance.history_origin is HistoryOrigin.START_AFTER_UNKNOWN
    assert first.lot_transition_sequence == 0


def _naive(value: datetime) -> datetime:
    return value.astimezone(ZONE).replace(tzinfo=None)


def _lot_row(snapshot: LotSnapshot) -> dict[str, Any]:
    return {
        **snapshot.canonical_payload(),
        "cost_price": snapshot.cost_price,
        "allocated_buy_fee": snapshot.allocated_buy_fee,
        "position_state": snapshot.position_state.value,
        "initial_stop": snapshot.initial_stop,
        "protective_stop": snapshot.protective_stop,
        "created_at": _naive(snapshot.created_at),
        "closed_at": None if snapshot.closed_at is None else _naive(snapshot.closed_at),
    }


def _canonical_rows(outcome: FillAccountingOutcome) -> dict[str, Any]:
    fill = outcome.fill_execution_evidence
    payload = fill.fill_payload.value()
    order_seed = next(
        dict(item.values)
        for item in build_behavioral_scenario().seed_rows
        if item.table == "st_order_v2" and item.values["order_id"] == fill.order_id
    )
    order_seed["side"] = payload["side"]
    account = {
        "account_id": fill.account_id,
        "cash_balance": outcome.account_cash_after,
    }
    fill_row = {
        "fill_id": fill.fill_id,
        "order_id": fill.order_id,
        "account_id": fill.account_id,
        "stock_code": fill.stock_code,
        "side": payload["side"],
        "quantity": payload["quantity"],
        "price": Decimal(str(payload["price"])),
        "gross_amount": Decimal(str(payload["gross_amount"])),
        "fee_amount": Decimal(str(payload["fee_amount"])),
        "net_cash_amount": Decimal(str(payload["net_cash_amount"])),
        "quote_event_id": payload["quote_event_id"],
        "match_event_id": payload["match_event_id"],
        "idempotency_key": payload["idempotency_key"],
        "filled_at": _naive(fill.executed_at),
        "created_at": _naive(datetime.fromisoformat(str(payload["created_at"]))),
    }
    cash_payload = outcome.cash_binding.cash_event_payload.value()
    cash_row = {
        "cash_event_id": cash_payload["cash_event_id"],
        "account_id": cash_payload["account_id"],
        "business_event_key": cash_payload["business_event_key"],
        "event_type": cash_payload["event_type"],
        "amount": Decimal(str(cash_payload["amount"])),
        "balance_after": Decimal(str(cash_payload["balance_after"])),
        "related_order_id": cash_payload["related_order_id"],
        "related_fill_id": cash_payload["related_fill_id"],
        "reversal_of": cash_payload["reversal_of"],
        "occurred_at": _naive(datetime.fromisoformat(str(cash_payload["occurred_at"]))),
        "created_at": _naive(datetime.fromisoformat(str(cash_payload["created_at"]))),
    }
    transition = outcome.order_transition
    return {
        "lock_order": {key: order_seed[key] for key in writer_impl.ORDER_COLUMNS},
        "lock_account": account,
        "lock_fill": fill_row,
        "lock_cash": cash_row,
        "lock_lots": [_lot_row(item.after_lot) for item in outcome.lot_effects],
        "lock_fill_evidence": {
            "fill_execution_evidence_id": fill.fill_execution_evidence_id,
            "fill_id": fill.fill_id,
            "order_id": fill.order_id,
            "account_id": fill.account_id,
            "stock_code": fill.stock_code,
            "evidence_hash": fill.evidence_hash,
        },
        "lock_cash_binding": {
            "cash_binding_id": outcome.cash_binding.cash_binding_id,
            "cash_event_id": outcome.cash_binding.cash_event_id,
            "account_id": fill.account_id,
            "related_order_id": fill.order_id,
            "related_fill_id": fill.fill_id,
            "fill_execution_evidence_id": fill.fill_execution_evidence_id,
            "fill_execution_evidence_hash": fill.evidence_hash,
            "binding_hash": outcome.cash_binding.binding_hash,
        },
        "lock_order_transition": {
            "transition_id": transition.transition_id,
            "order_id": fill.order_id,
            "account_id": fill.account_id,
            "to_status": transition.to_status.value,
            "next_filled_quantity": transition.next_filled_quantity,
            "transition_kind": transition.transition_kind.value,
            "related_fill_id": fill.fill_id,
            "fill_execution_evidence_id": fill.fill_execution_evidence_id,
            "fill_execution_evidence_hash": fill.evidence_hash,
            "transition_hash": transition.transition_hash,
        },
    }


class _Mappings:
    def __init__(self, rows: list[Mapping[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[Mapping[str, Any]]:
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class _Result:
    def __init__(self, rows: list[Mapping[str, Any]] | None = None) -> None:
        self._rows = rows or []

    def mappings(self) -> _Mappings:
        return _Mappings(self._rows)


class ScriptedConnection:
    def __init__(
        self,
        outcome: FillAccountingOutcome,
        *,
        active: bool = True,
        fail_insert: bool = False,
        fail_tag: str | None = None,
        fence_state: str = "INACTIVE",
    ) -> None:
        self.rows = _canonical_rows(outcome)
        self.active = active
        self.fail_insert = fail_insert
        self.fail_tag = fail_tag
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.sql_calls: list[tuple[str, str]] = []
        self.outcomes: dict[str, dict[str, Any]] = {}
        self.effects: dict[str, dict[str, Any]] = {}
        self.finalizations: dict[str, dict[str, Any]] = {}
        self.lifecycle_calls: list[str] = []
        self.fence_state = fence_state

    def in_transaction(self) -> bool:
        return self.active

    def execute(self, statement: Any, params: Mapping[str, Any]) -> _Result:
        sql = str(statement)
        if "schema_migration_v2_maintenance_fence" in sql:
            self.calls.append(("maintenance_fence", dict(params)))
            self.sql_calls.append(("maintenance_fence", sql))
            return _Result(
                [
                    {
                        "fence_name": "execution_evidence_011_015",
                        "state": self.fence_state,
                    }
                ]
            )
        match = re.search(r"/\* v2ao:([a-z0-9_]+) \*/", sql)
        assert match is not None
        tag = match.group(1)
        values = dict(params)
        self.calls.append((tag, values))
        self.sql_calls.append((tag, sql))
        if tag == "select_outcome_by_id" or tag == "read_back_outcome":
            row = self.outcomes.get(str(values["accounting_outcome_id"]))
            return _Result([] if row is None else [dict(row)])
        if tag == "select_outcome_by_fill":
            rows = [
                dict(row)
                for row in self.outcomes.values()
                if row["fill_id"] == values["fill_id"]
            ]
            return _Result(rows)
        if tag == "select_existing_effects":
            rows = sorted(
                (
                    dict(row)
                    for row in self.effects.values()
                    if row["accounting_outcome_id"]
                    == values["accounting_outcome_id"]
                ),
                key=lambda row: row["effect_sequence"],
            )
            return _Result(rows)
        if tag == "select_finalization_by_id" or tag == "read_back_finalization":
            row = self.finalizations.get(str(values["finalization_id"]))
            return _Result([] if row is None else [dict(row)])
        if tag == "select_finalization_by_outcome":
            rows = [
                dict(row)
                for row in self.finalizations.values()
                if row["accounting_outcome_id"]
                == values["accounting_outcome_id"]
            ]
            return _Result(rows)
        if tag == "lock_lot_head":
            rows = sorted(
                (
                    row
                    for row in self.effects.values()
                    if row["lot_id"] == values["lot_id"]
                ),
                key=lambda row: row["lot_transition_sequence"],
                reverse=True,
            )
            if not rows:
                return _Result()
            head = rows[0]
            return _Result(
                [{key: head[key] for key in writer_impl.LOT_HEAD_COLUMNS}]
            )
        if tag == "insert_outcome":
            if self.fail_insert or self.fail_tag == tag:
                raise RuntimeError("simulated insert failure")
            self.outcomes[str(values["accounting_outcome_id"])] = values
            return _Result()
        if tag == "insert_lot_effect":
            if self.fail_tag == tag:
                raise RuntimeError("simulated insert failure")
            self.effects[str(values["lot_transition_evidence_id"])] = values
            return _Result()
        if tag == "insert_finalization":
            if self.fail_tag == tag:
                raise RuntimeError("simulated insert failure")
            self.finalizations[str(values["finalization_id"])] = values
            return _Result()
        scripted = self.rows.get(tag)
        if scripted is None:
            return _Result()
        if type(scripted) is list:
            return _Result([dict(item) for item in scripted])
        return _Result([dict(scripted)])


def test_writer_locks_canonical_rows_in_order_inserts_and_reads_back() -> None:
    outcome = _buy_outcome()
    connection = ScriptedConnection(outcome)
    result = append_fill_accounting_outcome(connection, outcome)
    assert result.status is AccountingEvidenceAppendStatus.INSERTED
    assert result.finalization_status is AccountingOutcomeFinalizationStatus.FINAL
    assert result.finalization_id == result.finalization_hash
    tags = [tag for tag, _ in connection.calls]
    assert tags[:6] == [
        "maintenance_fence",
        "lock_order",
        "lock_account",
        "lock_fill",
        "lock_cash",
        "lock_lots",
    ]
    assert tags.index("lock_fill_evidence") < tags.index("select_outcome_by_id")
    assert tags.index("lock_lot_head") < tags.index("insert_outcome")
    assert tags.index("insert_outcome") < tags.index("insert_lot_effect")
    assert tags.index("insert_lot_effect") < tags.index("insert_finalization")
    assert tags.index("insert_finalization") < tags.index("read_back_outcome")
    assert tags.index("insert_finalization") < tags.index("read_back_finalization")
    assert len(connection.finalizations) == 1
    assert connection.lifecycle_calls == []
    for tag, sql in connection.sql_calls:
        if tag.startswith("lock_") or tag.startswith("select_") or tag.startswith("read_back"):
            assert sql.rstrip().endswith("FOR UPDATE")


def test_writer_exact_replay_is_idempotent_and_different_content_conflicts() -> None:
    outcome = _buy_outcome()
    connection = ScriptedConnection(outcome)
    assert append_fill_accounting_outcome(connection, outcome).status is AccountingEvidenceAppendStatus.INSERTED
    connection.rows["lock_account"]["cash_balance"] = Decimal("90000.00")
    connection.rows["lock_lots"][0]["remaining_quantity"] = 50
    connection.rows["lock_lots"][0]["version"] = 2
    assert append_fill_accounting_outcome(connection, outcome).status is AccountingEvidenceAppendStatus.IDEMPOTENT
    effect = outcome.lot_effects[0]
    changed_effect = replace(
        effect,
        after_lot=replace(effect.after_lot, strategy_version="strategy-v2"),
    )
    conflicting = replace(outcome, lot_effects=(changed_effect,))
    with pytest.raises(AccountingEvidenceAppendConflictError, match="natural fill"):
        append_fill_accounting_outcome(connection, conflicting)


def test_unfinalized_parent_is_not_effective_and_full_pending_rows_can_finalize() -> None:
    outcome = _buy_outcome()
    parent_only = ScriptedConnection(outcome)
    parent_only.outcomes[outcome.accounting_outcome_id] = (
        writer_impl._outcome_storage(outcome)
    )
    with pytest.raises(AccountingEvidenceAppendConflictError, match="incomplete"):
        append_fill_accounting_outcome(parent_only, outcome)
    assert parent_only.finalizations == {}
    assert not any(tag == "insert_finalization" for tag, _ in parent_only.calls)

    pending = ScriptedConnection(outcome)
    pending.outcomes[outcome.accounting_outcome_id] = (
        writer_impl._outcome_storage(outcome)
    )
    for effect in outcome.lot_effects:
        pending.effects[effect.lot_transition_evidence_id] = (
            writer_impl._effect_storage(outcome, effect)
        )
    result = append_fill_accounting_outcome(pending, outcome)
    assert result.status is AccountingEvidenceAppendStatus.FINALIZED
    assert result.finalization_status is AccountingOutcomeFinalizationStatus.FINAL
    tags = [tag for tag, _ in pending.calls]
    assert "insert_outcome" not in tags
    assert "insert_lot_effect" not in tags
    assert tags.index("select_existing_effects") < tags.index("insert_finalization")
    assert len(pending.finalizations) == 1


def test_different_finalization_content_conflicts() -> None:
    outcome = _buy_outcome()
    connection = ScriptedConnection(outcome)
    append_fill_accounting_outcome(connection, outcome)
    marker = next(iter(connection.finalizations.values()))
    marker["finalization_hash"] = "a" * 64
    with pytest.raises(
        AccountingEvidenceAppendConflictError,
        match="finalization has different content",
    ):
        append_fill_accounting_outcome(connection, outcome)


def test_writer_rejects_current_cash_or_fifo_skip() -> None:
    buy = _buy_outcome()
    bad_cash = ScriptedConnection(buy)
    bad_cash.rows["lock_account"]["cash_balance"] = Decimal("1.00")
    with pytest.raises(AccountingEvidenceCanonicalRowError, match="account cash"):
        append_fill_accounting_outcome(bad_cash, buy)
    bad_buy_lot = ScriptedConnection(buy)
    bad_buy_lot.rows["lock_lots"][0]["version"] = 2
    with pytest.raises(AccountingEvidenceCanonicalRowError, match="differs from after"):
        append_fill_accounting_outcome(bad_buy_lot, buy)

    sell = _sell_outcome()
    skipped = ScriptedConnection(sell)
    older = _lot(
        fill=sell.fill_execution_evidence,
        lot_id="lot-0-skipped",
        opened=date(2026, 7, 1),
        settlement=date(2026, 7, 2),
        original=10,
        remaining=10,
        version=1,
        created_at=_at(8),
    )
    skipped.rows["lock_lots"].insert(0, _lot_row(older))
    with pytest.raises(AccountingEvidenceCanonicalRowError, match="skip or reorder"):
        append_fill_accounting_outcome(skipped, sell)

    omitted_closed = ScriptedConnection(sell)
    closed_before = _lot(
        fill=sell.fill_execution_evidence,
        lot_id="lot-0-omitted-closed",
        opened=date(2026, 7, 1),
        settlement=date(2026, 7, 2),
        original=10,
        remaining=0,
        version=2,
        state=PositionState.CLOSED,
        closed_at=sell.fill_execution_evidence.executed_at,
        created_at=_at(8),
    )
    omitted_closed.rows["lock_lots"].insert(0, _lot_row(closed_before))
    with pytest.raises(AccountingEvidenceCanonicalRowError, match="omit a lot closed"):
        append_fill_accounting_outcome(omitted_closed, sell)


def test_writer_rejects_a_lot_chain_branch_at_the_locked_head() -> None:
    outcome = _sell_outcome()
    connection = ScriptedConnection(outcome)
    effect = outcome.lot_effects[0]
    fake_head = writer_impl._effect_storage(outcome, effect)
    fake_head.update(
        {
            "lot_transition_evidence_id": "a" * 64,
            "effect_hash": "a" * 64,
            "lot_transition_sequence": 0,
            "accounting_outcome_id": "b" * 64,
            "fill_id": "older-sell",
        }
    )
    connection.effects["a" * 64] = fake_head
    with pytest.raises(AccountingEvidenceAppendConflictError, match="chain head"):
        append_fill_accounting_outcome(connection, outcome)


def test_writer_requires_active_transaction_and_propagates_insert_failure() -> None:
    outcome = _buy_outcome()
    with pytest.raises(AccountingEvidenceTransactionError):
        append_fill_accounting_outcome(
            ScriptedConnection(outcome, active=False),
            outcome,
        )
    failed = ScriptedConnection(outcome, fail_insert=True)
    with pytest.raises(RuntimeError, match="simulated insert failure"):
        append_fill_accounting_outcome(failed, outcome)
    tags = [tag for tag, _ in failed.calls]
    assert tags[-1] == "insert_outcome"
    assert "read_back_outcome" not in tags

    marker_failed = ScriptedConnection(outcome, fail_tag="insert_finalization")
    with pytest.raises(RuntimeError, match="simulated insert failure"):
        append_fill_accounting_outcome(marker_failed, outcome)
    marker_tags = [tag for tag, _ in marker_failed.calls]
    assert marker_tags[-1] == "insert_finalization"
    assert "read_back_finalization" not in marker_tags
    assert marker_failed.finalizations == {}


def test_active_maintenance_fence_blocks_accounting_before_fact_locks() -> None:
    outcome = _buy_outcome()
    connection = ScriptedConnection(outcome, fence_state="ACTIVE")

    with pytest.raises(
        AccountingEvidenceTransactionError,
        match="maintenance fence",
    ):
        append_fill_accounting_outcome(connection, outcome)

    assert [tag for tag, _params in connection.calls] == ["maintenance_fence"]


def test_writer_source_has_no_transaction_lifecycle_or_production_wiring() -> None:
    source = Path(writer_impl.__file__).read_text(encoding="utf-8").lower()
    for forbidden in (".begin(", ".commit(", ".rollback(", "create_engine"):
        assert forbidden not in source
    assert "from server.trading_v2.execution import" not in source
    assert "from server.trading_v3" not in source
