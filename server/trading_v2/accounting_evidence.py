"""Immutable accounting-outcome evidence layered on the canonical V2 ledger.

These values are evidence contracts, not a second account, cash, order, fill,
or lot ledger.  They bind one already-materialized V2 fill to its actual cash,
order-state, account-cash, and FIFO lot outcomes.  This module performs no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
from typing import Any

from .domain import PositionState
from .execution_evidence import (
    AuthorityStatus,
    CashEventBinding,
    EvidenceProvenance,
    FillExecutionEvidence,
    HistoryOrigin,
    OrderTransitionEvidence,
    OrderTransitionKind,
    validate_cash_event_binding,
    validate_fill_execution_evidence,
    validate_order_transition_evidence,
)


class AccountingEvidenceInvariantError(ValueError):
    """Accounting evidence cannot be reconstructed from canonical inputs."""


class LotEffectKind(str, Enum):
    BUY_CREATE = "BUY_CREATE"
    SELL_FIFO_CONSUME = "SELL_FIFO_CONSUME"


class AccountingOutcomeFinalizationStatus(str, Enum):
    FINAL = "FINAL"


def _text(
    value: object,
    name: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be exactly str")
    normalized = value.strip()
    if not normalized and not allow_empty:
        raise ValueError(f"{name} is required")
    if len(normalized) > maximum:
        raise ValueError(f"{name} exceeds {maximum} characters")
    return normalized


def _optional_text(value: object, name: str, *, maximum: int) -> str | None:
    if value is None:
        return None
    return _text(value, name, maximum=maximum)


def _sha256(value: object, name: str) -> str:
    normalized = _text(value, name, maximum=64).lower()
    if len(normalized) != 64 or any(item not in "0123456789abcdef" for item in normalized):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return normalized


def _optional_sha256(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, name)


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be exactly int >= {minimum}")
    return value


def _aware(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if value.microsecond != 0:
        raise ValueError(
            f"{name} exceeds V2 DATETIME whole-second precision"
        )
    return value


def _date(value: object, name: str) -> date:
    if type(value) is not date:
        raise TypeError(f"{name} must be exactly date")
    return value


def _decimal(value: object, name: str, *, scale: int) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise TypeError(f"{name} must be a finite Decimal")
    quantum = Decimal(1).scaleb(-scale)
    try:
        normalized = value.quantize(quantum)
    except InvalidOperation as exc:
        raise ValueError(f"{name} cannot be represented at scale {scale}") from exc
    if normalized != value:
        raise ValueError(f"{name} exceeds scale {scale}")
    return normalized


def _decimal_from_payload(value: object, name: str, *, scale: int) -> Decimal:
    if type(value) is not str:
        raise TypeError(f"{name} must be canonical decimal text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} is not decimal text") from exc
    return _decimal(parsed, name, scale=scale)


def _canonical(value: Any) -> Any:
    if type(value) is datetime:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is date:
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if type(value) in {tuple, list}:
        return [_canonical(item) for item in value]
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise TypeError("canonical accounting keys must be exactly str")
        return {
            key: _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: pair[0])
        }
    if value is None or type(value) in {str, int, bool}:
        return value
    raise TypeError(f"unsupported accounting hash value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _payload(value: object, name: str) -> dict[str, Any]:
    payload_method = getattr(value, "value", None)
    if not callable(payload_method):
        raise TypeError(f"{name} must expose canonical JSON value()")
    result = payload_method()
    if type(result) is not dict:
        raise ValueError(f"{name} must contain a JSON object")
    return result


def _reconstruct(value: Any, expected_type: type, name: str) -> Any:
    if type(value) is not expected_type:
        raise TypeError(f"{name} must be exactly {expected_type.__name__}")
    try:
        rebuilt = replace(value)
    except (TypeError, ValueError, AttributeError) as exc:
        raise AccountingEvidenceInvariantError(
            f"{name} cannot be reconstructed"
        ) from exc
    if rebuilt != value:
        raise AccountingEvidenceInvariantError(
            f"{name} differs from canonical reconstruction"
        )
    return value


def _require_provenance(value: EvidenceProvenance, *, occurred_at: datetime) -> None:
    value = _reconstruct(value, EvidenceProvenance, "provenance")
    if value.authority_status is not AuthorityStatus.CONTENT_HASH_ONLY:
        raise ValueError("accounting evidence authority must be CONTENT_HASH_ONLY")
    if value.history_origin not in {
        HistoryOrigin.START_AFTER_UNKNOWN,
        HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN,
    }:
        raise ValueError(
            "accounting history must start after unknown history or a declared origin"
        )
    if value.history_origin_at is None or value.history_origin_at > occurred_at:
        raise ValueError("accounting history origin cannot follow the fill")


def _same_provenance(left: EvidenceProvenance, right: EvidenceProvenance) -> bool:
    return left.provenance_hash == right.provenance_hash


@dataclass(frozen=True, slots=True)
class LotSnapshot:
    lot_id: str
    account_id: str
    stock_code: str
    theme_code: str
    strategy_version: str
    opened_fill_id: str
    opened_trade_date: date
    settlement_date: date
    original_quantity: int
    remaining_quantity: int
    cost_price: Decimal
    allocated_buy_fee: Decimal
    position_state: PositionState
    approved_target_quantity: int
    add_count: int
    initial_stop: Decimal
    protective_stop: Decimal
    invalidation_condition: str
    version: int
    created_at: datetime
    closed_at: datetime | None = None
    snapshot_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, maximum, allow_empty in (
            ("lot_id", 64, False),
            ("account_id", 64, False),
            ("stock_code", 16, False),
            ("theme_code", 80, True),
            ("strategy_version", 80, False),
            ("opened_fill_id", 64, False),
            ("invalidation_condition", 1000, False),
        ):
            object.__setattr__(
                self,
                name,
                _text(
                    getattr(self, name),
                    name,
                    maximum=maximum,
                    allow_empty=allow_empty,
                ),
            )
        opened = _date(self.opened_trade_date, "opened_trade_date")
        settlement = _date(self.settlement_date, "settlement_date")
        if settlement < opened:
            raise ValueError("lot settlement_date cannot precede opened_trade_date")
        original = _integer(self.original_quantity, "original_quantity", minimum=1)
        remaining = _integer(self.remaining_quantity, "remaining_quantity")
        if remaining > original:
            raise ValueError("lot remaining quantity exceeds original quantity")
        object.__setattr__(self, "cost_price", _decimal(self.cost_price, "cost_price", scale=6))
        object.__setattr__(
            self,
            "allocated_buy_fee",
            _decimal(self.allocated_buy_fee, "allocated_buy_fee", scale=2),
        )
        object.__setattr__(self, "initial_stop", _decimal(self.initial_stop, "initial_stop", scale=6))
        object.__setattr__(
            self,
            "protective_stop",
            _decimal(self.protective_stop, "protective_stop", scale=6),
        )
        _integer(self.approved_target_quantity, "approved_target_quantity", minimum=1)
        _integer(self.add_count, "add_count")
        _integer(self.version, "version", minimum=1)
        if type(self.position_state) is not PositionState:
            raise TypeError("position_state must be exactly PositionState")
        created = _aware(self.created_at, "created_at")
        closed = None if self.closed_at is None else _aware(self.closed_at, "closed_at")
        if remaining == 0:
            if self.position_state is not PositionState.CLOSED or closed is None:
                raise ValueError("zero-quantity lot must be CLOSED with closed_at")
            if closed < created:
                raise ValueError("lot cannot close before creation")
        elif self.position_state is PositionState.CLOSED or closed is not None:
            raise ValueError("open-quantity lot cannot be CLOSED or carry closed_at")
        object.__setattr__(self, "closed_at", closed)
        object.__setattr__(
            self,
            "snapshot_hash",
            _digest("trading-v2.lot-snapshot.v1", self.canonical_payload()),
        )

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "lot_id": self.lot_id,
            "account_id": self.account_id,
            "stock_code": self.stock_code,
            "theme_code": self.theme_code,
            "strategy_version": self.strategy_version,
            "opened_fill_id": self.opened_fill_id,
            "opened_trade_date": self.opened_trade_date,
            "settlement_date": self.settlement_date,
            "original_quantity": self.original_quantity,
            "remaining_quantity": self.remaining_quantity,
            "cost_price": format(self.cost_price, ".6f"),
            "allocated_buy_fee": format(self.allocated_buy_fee, ".2f"),
            "position_state": self.position_state,
            "approved_target_quantity": self.approved_target_quantity,
            "add_count": self.add_count,
            "initial_stop": format(self.initial_stop, ".6f"),
            "protective_stop": format(self.protective_stop, ".6f"),
            "invalidation_condition": self.invalidation_condition,
            "version": self.version,
            "created_at": self.created_at,
            "closed_at": self.closed_at,
        }


def lot_effect_root_hash(fill: FillExecutionEvidence) -> str:
    validate_fill_execution_evidence(fill)
    fill_payload = _payload(fill.fill_payload, "fill_payload")
    return _digest(
        "trading-v2.lot-effect-root.v1",
        {
            "fill_execution_evidence_id": fill.fill_execution_evidence_id,
            "fill_execution_evidence_hash": fill.evidence_hash,
            "fill_id": fill.fill_id,
            "order_id": fill.order_id,
            "account_id": fill.account_id,
            "stock_code": fill.stock_code,
            "side": fill_payload["side"],
        },
    )


@dataclass(frozen=True, slots=True)
class LotAccountingEffect:
    fill_execution_evidence: FillExecutionEvidence
    effect_sequence: int
    lot_transition_sequence: int
    effect_kind: LotEffectKind
    before_lot: LotSnapshot | None
    after_lot: LotSnapshot
    consumed_quantity: int
    occurred_at: datetime
    bound_at: datetime
    provenance: EvidenceProvenance
    previous_effect_id: str | None = None
    previous_effect_hash: str | None = None
    previous_lot_transition_id: str | None = None
    previous_lot_transition_hash: str | None = None
    lot_effect_root_hash: str = field(init=False)
    effect_hash: str = field(init=False)
    lot_transition_evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        fill = _reconstruct(
            self.fill_execution_evidence,
            FillExecutionEvidence,
            "fill_execution_evidence",
        )
        validate_fill_execution_evidence(fill)
        sequence = _integer(self.effect_sequence, "effect_sequence")
        lot_sequence = _integer(
            self.lot_transition_sequence,
            "lot_transition_sequence",
        )
        if type(self.effect_kind) is not LotEffectKind:
            raise TypeError("effect_kind must be exactly LotEffectKind")
        after = _reconstruct(self.after_lot, LotSnapshot, "after_lot")
        before = (
            None
            if self.before_lot is None
            else _reconstruct(self.before_lot, LotSnapshot, "before_lot")
        )
        consumed = _integer(self.consumed_quantity, "consumed_quantity")
        occurred = _aware(self.occurred_at, "occurred_at")
        bound = _aware(self.bound_at, "bound_at")
        if occurred != fill.executed_at or bound < max(occurred, fill.bound_at):
            raise ValueError("lot effect time must bind and follow fill execution evidence")
        _require_provenance(self.provenance, occurred_at=occurred)
        if not _same_provenance(self.provenance, fill.provenance):
            raise ValueError("lot effect and fill history origins differ")
        previous_effect_id = _optional_sha256(self.previous_effect_id, "previous_effect_id")
        previous_effect_hash = _optional_sha256(
            self.previous_effect_hash,
            "previous_effect_hash",
        )
        if (previous_effect_id is None) != (previous_effect_hash is None):
            raise ValueError("previous effect id and hash must be provided together")
        if sequence == 0 and previous_effect_id is not None:
            raise ValueError("first fill effect cannot reference a previous effect")
        if sequence > 0 and previous_effect_id is None:
            raise ValueError("later fill effect must reference its predecessor")
        previous_lot_id = _optional_sha256(
            self.previous_lot_transition_id,
            "previous_lot_transition_id",
        )
        previous_lot_hash = _optional_sha256(
            self.previous_lot_transition_hash,
            "previous_lot_transition_hash",
        )
        if (previous_lot_id is None) != (previous_lot_hash is None):
            raise ValueError("previous lot transition id and hash must be provided together")
        if lot_sequence == 0 and previous_lot_id is not None:
            raise ValueError("lot transition genesis cannot reference a predecessor")
        if lot_sequence > 0 and previous_lot_id is None:
            raise ValueError("later lot transition must reference its predecessor")
        fill_payload = _payload(fill.fill_payload, "fill_payload")
        side = fill_payload["side"]
        if after.account_id != fill.account_id or after.stock_code != fill.stock_code:
            raise ValueError("lot effect account or stock differs from fill")
        if self.effect_kind is LotEffectKind.BUY_CREATE:
            if side != "BUY" or before is not None or consumed != 0:
                raise ValueError("BUY_CREATE requires a BUY fill, no before row, and zero consumption")
            if lot_sequence != 0 or previous_lot_id is not None:
                raise ValueError("BUY_CREATE must be the lot transition genesis")
            settlement = _payload(fill.settlement_evidence, "settlement_evidence")
            trade_date = fill.quote_evidence.trade_date
            if (
                after.lot_id != f"LOT:{fill.fill_id}"
                or after.opened_fill_id != fill.fill_id
                or after.opened_trade_date != trade_date
                or after.settlement_date
                != date.fromisoformat(str(settlement["settlement_date"]))
                or after.original_quantity != fill_payload["quantity"]
                or after.remaining_quantity != fill_payload["quantity"]
                or after.cost_price
                != _decimal_from_payload(fill_payload["price"], "fill price", scale=6)
                or after.allocated_buy_fee
                != _decimal_from_payload(fill_payload["fee_amount"], "fill fee", scale=2)
                or after.position_state is not PositionState.OPENING
                or after.version != 1
                or after.closed_at is not None
                or not occurred <= after.created_at <= bound
            ):
                raise ValueError("BUY_CREATE after row differs from the canonical fill outcome")
        elif self.effect_kind is LotEffectKind.SELL_FIFO_CONSUME:
            if side != "SELL" or before is None or consumed < 1:
                raise ValueError("SELL_FIFO_CONSUME requires SELL, before row, and consumption")
            if self.provenance.history_is_complete and previous_lot_id is None:
                raise ValueError(
                    "complete SELL lot history must reference a recorded predecessor"
                )
            if before.lot_id != after.lot_id:
                raise ValueError("SELL lot identity cannot change")
            static_fields = (
                "account_id",
                "stock_code",
                "theme_code",
                "strategy_version",
                "opened_fill_id",
                "opened_trade_date",
                "settlement_date",
                "original_quantity",
                "cost_price",
                "allocated_buy_fee",
                "approved_target_quantity",
                "add_count",
                "initial_stop",
                "protective_stop",
                "invalidation_condition",
                "created_at",
            )
            if any(getattr(before, name) != getattr(after, name) for name in static_fields):
                raise ValueError("SELL lot immutable fields changed")
            if before.remaining_quantity < consumed:
                raise ValueError("SELL lot consumption exceeds before quantity")
            if after.remaining_quantity != before.remaining_quantity - consumed:
                raise ValueError("SELL lot remaining quantity does not match consumption")
            if after.version != before.version + 1:
                raise ValueError("SELL lot version must increase by exactly one")
            if after.remaining_quantity == 0:
                if after.position_state is not PositionState.CLOSED or after.closed_at != occurred:
                    raise ValueError("fully consumed SELL lot must close at execution time")
            elif (
                after.position_state is not before.position_state
                or after.closed_at != before.closed_at
            ):
                raise ValueError("partially consumed SELL lot cannot change state")
            if before.settlement_date > fill.quote_evidence.trade_date:
                raise ValueError("SELL cannot consume an unsettled lot")
        else:  # pragma: no cover - exact Enum check above
            raise TypeError("unsupported lot effect kind")
        root_hash = lot_effect_root_hash(fill)
        object.__setattr__(self, "previous_effect_id", previous_effect_id)
        object.__setattr__(self, "previous_effect_hash", previous_effect_hash)
        object.__setattr__(self, "previous_lot_transition_id", previous_lot_id)
        object.__setattr__(self, "previous_lot_transition_hash", previous_lot_hash)
        object.__setattr__(self, "lot_effect_root_hash", root_hash)
        effect_hash = _digest(
            "trading-v2.lot-accounting-effect.v1",
            {
                "fill_execution_evidence_id": fill.fill_execution_evidence_id,
                "fill_execution_evidence_hash": fill.evidence_hash,
                "effect_sequence": sequence,
                "lot_transition_sequence": lot_sequence,
                "effect_kind": self.effect_kind,
                "lot_effect_root_hash": root_hash,
                "previous_effect_id": previous_effect_id,
                "previous_effect_hash": previous_effect_hash,
                "previous_lot_transition_id": previous_lot_id,
                "previous_lot_transition_hash": previous_lot_hash,
                "lot_id": after.lot_id,
                "before_lot_hash": None if before is None else before.snapshot_hash,
                "after_lot_hash": after.snapshot_hash,
                "consumed_quantity": consumed,
                "occurred_at": occurred,
                "bound_at": bound,
                "provenance_hash": self.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "effect_hash", effect_hash)
        object.__setattr__(self, "lot_transition_evidence_id", effect_hash)


@dataclass(frozen=True, slots=True)
class FillAccountingOutcome:
    fill_execution_evidence: FillExecutionEvidence
    cash_binding: CashEventBinding
    order_transition: OrderTransitionEvidence
    account_cash_before: Decimal
    account_cash_after: Decimal
    lot_effects: tuple[LotAccountingEffect, ...]
    recorded_at: datetime
    provenance: EvidenceProvenance
    lot_effect_root_hash: str = field(init=False)
    lot_effects_hash: str = field(init=False)
    lot_effect_count: int = field(init=False)
    total_effect_quantity: int = field(init=False)
    outcome_hash: str = field(init=False)
    accounting_outcome_id: str = field(init=False)

    def __post_init__(self) -> None:
        fill = _reconstruct(
            self.fill_execution_evidence,
            FillExecutionEvidence,
            "fill_execution_evidence",
        )
        cash = _reconstruct(self.cash_binding, CashEventBinding, "cash_binding")
        transition = _reconstruct(
            self.order_transition,
            OrderTransitionEvidence,
            "order_transition",
        )
        validate_fill_execution_evidence(fill)
        validate_cash_event_binding(cash)
        validate_order_transition_evidence(transition)
        fill_payload = _payload(fill.fill_payload, "fill_payload")
        side = fill_payload["side"]
        if (
            cash.cash_event_type != f"{side}_FILL"
            or cash.related_fill_id != fill.fill_id
            or cash.related_order_id != fill.order_id
            or cash.fill_execution_evidence is None
            or cash.fill_execution_evidence.evidence_hash != fill.evidence_hash
        ):
            raise ValueError("accounting outcome cash binding differs from fill")
        if (
            transition.transition_kind is not OrderTransitionKind.FILL_APPLIED
            or transition.related_fill_id != fill.fill_id
            or transition.fill_execution_evidence is None
            or transition.fill_execution_evidence.evidence_hash != fill.evidence_hash
            or transition.next_filled_quantity - transition.previous_filled_quantity
            != fill_payload["quantity"]
        ):
            raise ValueError("accounting outcome order transition differs from fill")
        _require_provenance(self.provenance, occurred_at=fill.executed_at)
        for nested in (fill.provenance, cash.provenance, transition.provenance):
            if not _same_provenance(self.provenance, nested):
                raise ValueError("accounting outcome nested history origins differ")
        before = _decimal(self.account_cash_before, "account_cash_before", scale=2)
        after = _decimal(self.account_cash_after, "account_cash_after", scale=2)
        if before < 0 or after < 0:
            raise ValueError("account cash before/after cannot be negative")
        cash_payload = _payload(cash.cash_event_payload, "cash_event_payload")
        amount = _decimal_from_payload(cash_payload["amount"], "cash amount", scale=2)
        bound_after = _decimal_from_payload(
            cash_payload["balance_after"],
            "cash balance_after",
            scale=2,
        )
        net_cash = _decimal_from_payload(
            fill_payload["net_cash_amount"],
            "fill net_cash_amount",
            scale=2,
        )
        if amount != net_cash or after != bound_after or before + amount != after:
            raise ValueError("account cash before/after does not reconcile to fill cash")
        effects = self.lot_effects
        if type(effects) is not tuple or not effects:
            raise ValueError("accounting outcome requires a non-empty tuple of lot effects")
        reconstructed = tuple(
            _reconstruct(item, LotAccountingEffect, "lot effect") for item in effects
        )
        expected_root = lot_effect_root_hash(fill)
        remaining = int(fill_payload["quantity"])
        previous: LotAccountingEffect | None = None
        lot_ids: set[str] = set()
        for sequence, effect in enumerate(reconstructed):
            if effect.effect_sequence != sequence:
                raise ValueError("lot effect sequence must be contiguous from zero")
            if effect.fill_execution_evidence.evidence_hash != fill.evidence_hash:
                raise ValueError("lot effect binds a different fill")
            if effect.lot_effect_root_hash != expected_root:
                raise ValueError("lot effect root differs from fill")
            if not _same_provenance(self.provenance, effect.provenance):
                raise ValueError("lot effect history origin differs from outcome")
            if effect.after_lot.lot_id in lot_ids:
                raise ValueError("one fill cannot affect the same lot twice")
            lot_ids.add(effect.after_lot.lot_id)
            if previous is None:
                if effect.previous_effect_id is not None:
                    raise ValueError("first lot effect has an unexpected predecessor")
            elif (
                effect.previous_effect_id != previous.lot_transition_evidence_id
                or effect.previous_effect_hash != previous.effect_hash
            ):
                raise ValueError("lot effect chain is discontinuous")
            previous = effect
        if side == "BUY":
            if len(effects) != 1 or effects[0].effect_kind is not LotEffectKind.BUY_CREATE:
                raise ValueError("BUY accounting outcome requires exactly one BUY_CREATE")
            total_effect_quantity = effects[0].after_lot.original_quantity
        else:
            fifo_key = tuple(
                (item.before_lot.opened_trade_date, item.before_lot.lot_id)
                for item in effects
                if item.before_lot is not None
            )
            if fifo_key != tuple(sorted(fifo_key)):
                raise ValueError("SELL lot effects must be in deterministic FIFO order")
            for effect in effects:
                if effect.effect_kind is not LotEffectKind.SELL_FIFO_CONSUME:
                    raise ValueError("SELL accounting outcome contains a non-SELL effect")
                assert effect.before_lot is not None
                expected_consumed = min(remaining, effect.before_lot.remaining_quantity)
                if effect.consumed_quantity != expected_consumed:
                    raise ValueError("SELL lot effect does not consume strict FIFO quantity")
                remaining -= effect.consumed_quantity
            if remaining != 0:
                raise ValueError("SELL lot effects do not cover the full fill quantity")
            total_effect_quantity = sum(item.consumed_quantity for item in effects)
        recorded = _aware(self.recorded_at, "recorded_at")
        if recorded < max(
            cash.bound_at,
            transition.recorded_at,
            *(item.bound_at for item in effects),
        ):
            raise ValueError("accounting outcome recorded_at precedes nested evidence")
        effects_hash = _digest(
            "trading-v2.lot-accounting-effect-list.v1",
            {
                "root_hash": expected_root,
                "effect_hashes": tuple(item.effect_hash for item in effects),
            },
        )
        object.__setattr__(self, "account_cash_before", before)
        object.__setattr__(self, "account_cash_after", after)
        object.__setattr__(self, "lot_effect_root_hash", expected_root)
        object.__setattr__(self, "lot_effects_hash", effects_hash)
        object.__setattr__(self, "lot_effect_count", len(effects))
        object.__setattr__(self, "total_effect_quantity", total_effect_quantity)
        outcome_hash = _digest(
            "trading-v2.fill-accounting-outcome.v1",
            {
                "fill_execution_evidence_id": fill.fill_execution_evidence_id,
                "fill_execution_evidence_hash": fill.evidence_hash,
                "cash_binding_id": cash.cash_binding_id,
                "cash_binding_hash": cash.binding_hash,
                "order_transition_id": transition.transition_id,
                "order_transition_hash": transition.transition_hash,
                "account_id": fill.account_id,
                "stock_code": fill.stock_code,
                "side": side,
                "account_cash_before": format(before, ".2f"),
                "account_cash_after": format(after, ".2f"),
                "lot_effect_root_hash": expected_root,
                "lot_effects_hash": effects_hash,
                "lot_effect_count": len(effects),
                "total_effect_quantity": total_effect_quantity,
                "recorded_at": recorded,
                "provenance_hash": self.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "outcome_hash", outcome_hash)
        object.__setattr__(self, "accounting_outcome_id", outcome_hash)


@dataclass(frozen=True, slots=True)
class FillAccountingFinalization:
    """Append-only commit marker that makes one complete outcome effective.

    A :class:`FillAccountingOutcome` row without this marker is only a pending
    parent.  Readers must join the finalization table and require ``FINAL``;
    the marker is created only after every lot effect has been appended.
    """

    accounting_outcome: FillAccountingOutcome
    finalized_at: datetime
    finalization_status: AccountingOutcomeFinalizationStatus = (
        AccountingOutcomeFinalizationStatus.FINAL
    )
    effect_hashes_json: str = field(init=False)
    finalization_hash: str = field(init=False)
    finalization_id: str = field(init=False)

    def __post_init__(self) -> None:
        outcome = _reconstruct(
            self.accounting_outcome,
            FillAccountingOutcome,
            "accounting_outcome",
        )
        if type(self.finalization_status) is not AccountingOutcomeFinalizationStatus:
            raise TypeError(
                "finalization_status must be exactly "
                "AccountingOutcomeFinalizationStatus"
            )
        if self.finalization_status is not AccountingOutcomeFinalizationStatus.FINAL:
            raise ValueError("accounting outcome finalization must be FINAL")
        finalized = _aware(self.finalized_at, "finalized_at")
        if finalized < outcome.recorded_at:
            raise ValueError("accounting outcome cannot finalize before it is recorded")
        effect_hashes = tuple(item.effect_hash for item in outcome.lot_effects)
        effect_hashes_json = json.dumps(
            _canonical(effect_hashes),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if _digest(
            "trading-v2.lot-accounting-effect-list.v1",
            {
                "root_hash": outcome.lot_effect_root_hash,
                "effect_hashes": effect_hashes,
            },
        ) != outcome.lot_effects_hash:
            raise AccountingEvidenceInvariantError(
                "finalization effect manifest differs from accounting outcome"
            )
        finalization_hash = _digest(
            "trading-v2.fill-accounting-finalization.v1",
            {
                "accounting_outcome_id": outcome.accounting_outcome_id,
                "outcome_hash": outcome.outcome_hash,
                "fill_id": outcome.fill_execution_evidence.fill_id,
                "fill_execution_evidence_id": (
                    outcome.fill_execution_evidence.fill_execution_evidence_id
                ),
                "fill_execution_evidence_hash": (
                    outcome.fill_execution_evidence.evidence_hash
                ),
                "lot_effect_root_hash": outcome.lot_effect_root_hash,
                "lot_effects_hash": outcome.lot_effects_hash,
                "effect_hashes": effect_hashes,
                "lot_effect_count": outcome.lot_effect_count,
                "total_effect_quantity": outcome.total_effect_quantity,
                "finalization_status": self.finalization_status,
                "finalized_at": finalized,
                "provenance_hash": outcome.provenance.provenance_hash,
            },
        )
        object.__setattr__(self, "finalized_at", finalized)
        object.__setattr__(self, "effect_hashes_json", effect_hashes_json)
        object.__setattr__(self, "finalization_hash", finalization_hash)
        object.__setattr__(self, "finalization_id", finalization_hash)


def finalize_fill_accounting_outcome(
    outcome: FillAccountingOutcome,
    *,
    finalized_at: datetime | None = None,
) -> FillAccountingFinalization:
    """Build the deterministic FINAL marker for a complete outcome."""

    outcome = _reconstruct(outcome, FillAccountingOutcome, "accounting_outcome")
    return FillAccountingFinalization(
        accounting_outcome=outcome,
        finalized_at=outcome.recorded_at if finalized_at is None else finalized_at,
    )


def validate_lot_snapshot(value: LotSnapshot) -> None:
    _reconstruct(value, LotSnapshot, "lot snapshot")


def validate_lot_accounting_effect(value: LotAccountingEffect) -> None:
    _reconstruct(value, LotAccountingEffect, "lot accounting effect")


def validate_fill_accounting_outcome(value: FillAccountingOutcome) -> None:
    _reconstruct(value, FillAccountingOutcome, "fill accounting outcome")


def validate_fill_accounting_finalization(
    value: FillAccountingFinalization,
) -> None:
    _reconstruct(value, FillAccountingFinalization, "fill accounting finalization")


__all__ = [
    "AccountingOutcomeFinalizationStatus",
    "AccountingEvidenceInvariantError",
    "FillAccountingFinalization",
    "FillAccountingOutcome",
    "LotAccountingEffect",
    "LotEffectKind",
    "LotSnapshot",
    "finalize_fill_accounting_outcome",
    "lot_effect_root_hash",
    "validate_fill_accounting_finalization",
    "validate_fill_accounting_outcome",
    "validate_lot_accounting_effect",
    "validate_lot_snapshot",
]
