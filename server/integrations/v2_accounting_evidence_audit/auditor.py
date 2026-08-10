"""Independent, fail-closed audit of persisted V2 accounting evidence.

The accounting writer is deliberately outside this module's trust boundary.
The auditor reads all migration-015 rows in deterministic order under shared
row locks, rebuilds the domain contracts from their raw columns, verifies the
canonical V2 parents, and asks MySQL ``SHA2`` to recompute every hash preimage
that can be expressed from persisted columns.

The caller owns both the connection and the active transaction.  This module
opens no engine and never commits or rolls back.  Empty accounting tables are
valid; non-empty tables are valid only when every outcome has its complete,
ordered effect set and exactly one ``FINAL`` marker.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.integrations.v2_execution_evidence_audit import (
    auditor as execution_auditor,
)
from server.trading_v2.accounting_evidence import (
    AccountingOutcomeFinalizationStatus,
    FillAccountingFinalization,
    FillAccountingOutcome,
    LotAccountingEffect,
    LotEffectKind,
    LotSnapshot,
)
from server.trading_v2.domain import PositionState
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    CashEventBinding,
    EvidenceProvenance,
    FillExecutionEvidence,
    HistoryOrigin,
    OrderTransitionEvidence,
)


MARKET_ZONE = ZoneInfo("Asia/Shanghai")


class V2AccountingEvidenceAuditError(ValueError):
    """Persisted accounting evidence cannot be independently reproduced."""


OUTCOME_TABLE = "st_fill_accounting_outcome_v2"
LOT_EFFECT_TABLE = "st_lot_transition_evidence_v2"
FINALIZATION_TABLE = "st_fill_accounting_outcome_finalization_v2"
ACCOUNTING_AUDIT_TABLES = (
    OUTCOME_TABLE,
    LOT_EFFECT_TABLE,
    FINALIZATION_TABLE,
)


PROVENANCE_COLUMNS = (
    "history_origin",
    "history_origin_id",
    "history_origin_at",
    "authority_status",
    "authority_receipt_hash",
    "provenance_hash",
)
OUTCOME_COLUMNS = (
    "accounting_outcome_id",
    "fill_id",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "cash_binding_id",
    "cash_binding_hash",
    "cash_event_id",
    "order_transition_id",
    "order_transition_hash",
    "order_id",
    "account_id",
    "stock_code",
    "side",
    "account_cash_before",
    "account_cash_after",
    "lot_effect_root_hash",
    "lot_effects_hash",
    "lot_effect_count",
    "total_effect_quantity",
    *PROVENANCE_COLUMNS,
    "recorded_at",
    "outcome_hash",
    "created_at",
)
LOT_EFFECT_COLUMNS = (
    "lot_transition_evidence_id",
    "accounting_outcome_id",
    "fill_id",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "effect_sequence",
    "lot_transition_sequence",
    "effect_kind",
    "lot_effect_root_hash",
    "previous_effect_id",
    "previous_effect_hash",
    "previous_lot_transition_id",
    "previous_lot_transition_hash",
    "lot_id",
    "consumed_quantity",
    "before_lot_json",
    "before_lot_hash",
    "after_lot_json",
    "after_lot_hash",
    "occurred_at",
    "bound_at",
    *PROVENANCE_COLUMNS,
    "effect_hash",
    "created_at",
)
FINALIZATION_COLUMNS = (
    "finalization_id",
    "accounting_outcome_id",
    "fill_id",
    "outcome_hash",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "lot_effect_root_hash",
    "lot_effects_hash",
    "effect_hashes_json",
    "lot_effect_count",
    "total_effect_quantity",
    "finalization_status",
    *PROVENANCE_COLUMNS,
    "finalized_at",
    "finalization_hash",
    "created_at",
)


OUTCOME_DB_HASH_ALIASES = (
    "__dbhash_provenance_hash",
    "__dbhash_lot_effect_root_hash",
    "__dbhash_outcome_hash",
)
LOT_EFFECT_DB_HASH_ALIASES = (
    "__dbhash_provenance_hash",
    "__dbhash_lot_effect_root_hash",
    "__dbhash_before_lot_hash",
    "__dbhash_after_lot_hash",
    "__dbhash_effect_hash",
)
FINALIZATION_DB_HASH_ALIASES = (
    "__dbhash_provenance_hash",
    "__dbhash_lot_effects_hash",
    "__dbhash_finalization_hash",
)


# Logical independently reconstructed hashes.  The effect-list digest belongs
# to its outcome even though MySQL recomputes it from the FINAL marker's exact
# ordered manifest.  ``before_lot_hash`` is nullable for BUY_CREATE, but proving
# that both the stored value and the MySQL recomputation are exactly NULL is
# still one verification of that declared hash field.
ACCOUNTING_AUDIT_HASH_FIELDS: Mapping[str, tuple[str, ...]] = {
    OUTCOME_TABLE: (
        "provenance_hash",
        "lot_effect_root_hash",
        "lot_effects_hash",
        "outcome_hash",
    ),
    LOT_EFFECT_TABLE: (
        "provenance_hash",
        "lot_effect_root_hash",
        "before_lot_hash",
        "after_lot_hash",
        "effect_hash",
    ),
    FINALIZATION_TABLE: (
        "provenance_hash",
        "finalization_hash",
    ),
}

ACCOUNTING_AUDIT_PARENT_KINDS = (
    "account",
    "cash",
    "fill",
    "lot",
    "order",
)


ACCOUNT_COLUMNS = ("account_id", "cash_balance")
ORDER_COLUMNS = (
    "order_id",
    "account_id",
    "intent_id",
    "stock_code",
    "side",
    "order_type",
    "limit_price",
    "quantity",
    "filled_quantity",
    "status",
    "waiting_reason",
    "earliest_at",
    "expires_at",
    "idempotency_key",
    "created_at",
    "updated_at",
)
FILL_COLUMNS = (
    "fill_id",
    "order_id",
    "account_id",
    "stock_code",
    "side",
    "quantity",
    "price",
    "gross_amount",
    "fee_amount",
    "net_cash_amount",
    "quote_event_id",
    "match_event_id",
    "idempotency_key",
    "filled_at",
    "created_at",
)
CASH_COLUMNS = (
    "cash_event_id",
    "account_id",
    "business_event_key",
    "event_type",
    "amount",
    "balance_after",
    "related_order_id",
    "related_fill_id",
    "reversal_of",
    "occurred_at",
    "created_at",
)
LOT_COLUMNS = (
    "lot_id",
    "account_id",
    "stock_code",
    "theme_code",
    "strategy_version",
    "opened_fill_id",
    "opened_trade_date",
    "settlement_date",
    "original_quantity",
    "remaining_quantity",
    "cost_price",
    "allocated_buy_fee",
    "position_state",
    "approved_target_quantity",
    "add_count",
    "initial_stop",
    "protective_stop",
    "invalidation_condition",
    "version",
    "created_at",
    "closed_at",
)


def _exact_report_counts(
    value: object,
    *,
    expected_names: tuple[str, ...],
) -> dict[str, int] | None:
    """Decode one frozen ordered count vector without dict-collapse gaps."""

    if type(value) is not tuple or len(value) != len(expected_names):
        return None
    result: dict[str, int] = {}
    for expected_name, item in zip(expected_names, value, strict=True):
        if (
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or item[0] != expected_name
            or item[0] in result
            or type(item[1]) is not int
            or item[1] < 0
        ):
            return None
        result[item[0]] = item[1]
    return result


def _exact_report_text_ids(
    value: object,
    *,
    require_sha256: bool = False,
) -> tuple[str, ...] | None:
    if type(value) is not tuple:
        return None
    items: list[str] = []
    for item in value:
        if type(item) is not str or not item or item != item.strip():
            return None
        if require_sha256 and (
            len(item) != 64
            or item != item.lower()
            or any(character not in "0123456789abcdef" for character in item)
        ):
            return None
        items.append(item)
    if len(set(items)) != len(items) or tuple(items) != tuple(sorted(items)):
        return None
    return tuple(items)


def _exact_parent_row_checks(
    value: object,
) -> tuple[tuple[str, str], ...] | None:
    if type(value) is not tuple:
        return None
    checks: list[tuple[str, str]] = []
    for item in value:
        if (
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or item[0] not in ACCOUNTING_AUDIT_PARENT_KINDS
            or type(item[1]) is not str
            or not item[1]
            or item[1] != item[1].strip()
        ):
            return None
        checks.append(item)
    if len(set(checks)) != len(checks) or tuple(checks) != tuple(sorted(checks)):
        return None
    return tuple(checks)


@dataclass(frozen=True, slots=True)
class V2AccountingEvidenceAuditParents:
    """Already reconstructed evidence plus canonical V2 fact rows."""

    fills: Mapping[str, FillExecutionEvidence]
    cash_bindings: Mapping[str, CashEventBinding]
    order_transitions: Mapping[str, OrderTransitionEvidence]
    accounts: Mapping[str, Mapping[str, Any]]
    orders: Mapping[str, Mapping[str, Any]]
    fill_facts: Mapping[str, Mapping[str, Any]]
    cash_facts: Mapping[str, Mapping[str, Any]]
    lots: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class V2AccountingEvidenceAuditReport:
    table_counts: tuple[tuple[str, int], ...]
    hash_verifications: tuple[tuple[str, int], ...]
    hashes_verified: int
    rows_reconstructed: int
    finalized_outcomes: int
    finalized_outcome_ids: tuple[str, ...]
    lot_chains_checked: int
    lot_chain_ids: tuple[str, ...]
    parent_rows_checked: int
    parent_row_checks: tuple[tuple[str, str], ...]
    database_sha2_used: bool
    shared_row_locks_used: bool

    @property
    def audit_passed(self) -> bool:
        counts = _exact_report_counts(
            self.table_counts,
            expected_names=ACCOUNTING_AUDIT_TABLES,
        )
        hashes = _exact_report_counts(
            self.hash_verifications,
            expected_names=ACCOUNTING_AUDIT_TABLES,
        )
        if counts is None or hashes is None:
            return False
        expected_hashes = {
            table: counts[table] * len(ACCOUNTING_AUDIT_HASH_FIELDS[table])
            for table in ACCOUNTING_AUDIT_TABLES
        }
        finalized_ids = _exact_report_text_ids(
            self.finalized_outcome_ids,
            require_sha256=True,
        )
        lot_ids = _exact_report_text_ids(self.lot_chain_ids)
        parent_checks = _exact_parent_row_checks(self.parent_row_checks)
        metrics = (
            self.hashes_verified,
            self.rows_reconstructed,
            self.finalized_outcomes,
            self.lot_chains_checked,
            self.parent_rows_checked,
        )
        if (
            finalized_ids is None
            or lot_ids is None
            or parent_checks is None
            or any(type(value) is not int or value < 0 for value in metrics)
        ):
            return False
        parent_lot_ids = tuple(
            identity for kind, identity in parent_checks if kind == "lot"
        )
        parent_kinds = frozenset(kind for kind, _ in parent_checks)
        expected_parent_kinds = (
            frozenset(ACCOUNTING_AUDIT_PARENT_KINDS)
            if counts[OUTCOME_TABLE]
            else frozenset()
        )
        return (
            self.database_sha2_used is True
            and self.shared_row_locks_used is True
            and hashes == expected_hashes
            and self.rows_reconstructed == sum(counts.values())
            and self.hashes_verified == sum(hashes.values())
            and self.finalized_outcomes == counts[OUTCOME_TABLE]
            and counts[FINALIZATION_TABLE] == counts[OUTCOME_TABLE]
            and self.finalized_outcomes == len(finalized_ids)
            and self.lot_chains_checked == len(lot_ids)
            and self.lot_chains_checked <= counts[LOT_EFFECT_TABLE]
            and bool(self.lot_chains_checked) == bool(counts[LOT_EFFECT_TABLE])
            and self.parent_rows_checked == len(parent_checks)
            and parent_kinds == expected_parent_kinds
            and parent_lot_ids == lot_ids
        )

    @property
    def production_activation_allowed(self) -> bool:
        # Stored-row correctness is necessary but never an activation grant.
        return False

    @property
    def actionable_output_allowed(self) -> bool:
        return False


def _fail(message: str) -> V2AccountingEvidenceAuditError:
    return V2AccountingEvidenceAuditError(message)


def _text_value(
    value: object,
    name: str,
    *,
    allow_empty: bool = False,
) -> str:
    if type(value) is not str or value != value.strip():
        raise _fail(f"{name} must be exact text")
    if not value and not allow_empty:
        raise _fail(f"{name} must be non-blank")
    return value


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _text_value(value, name)


def _int_value(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise _fail(f"{name} must be int >= {minimum}")
    return value


def _date_value(value: object, name: str) -> date:
    if type(value) is not date:
        raise _fail(f"{name} must be exactly date")
    return value


def _datetime_value(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise _fail(f"{name} must be exactly datetime")
    if value.microsecond != 0:
        raise _fail(f"{name} exceeds V2 DATETIME whole-second precision")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=MARKET_ZONE)
    return value.astimezone(MARKET_ZONE)


def _decimal_value(value: object, name: str, *, scale: int) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise _fail(f"{name} must be a finite Decimal")
    quantum = Decimal(1).scaleb(-scale)
    try:
        normalized = value.quantize(quantum)
    except InvalidOperation as exc:
        raise _fail(f"{name} cannot be represented at scale {scale}") from exc
    if normalized != value:
        raise _fail(f"{name} exceeds scale {scale}")
    return normalized


def _decimal_text(value: object, name: str, *, scale: int) -> str:
    return format(_decimal_value(value, name, scale=scale), f".{scale}f")


def _hash_value(value: object, name: str) -> str:
    result = _text_value(value, name)
    if result != result.lower() or len(result) != 64 or any(
        item not in "0123456789abcdef" for item in result
    ):
        raise _fail(f"{name} must be lowercase SHA-256")
    return result


def _optional_hash(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _hash_value(value, name)


def _expect_hash(value: object, expected: str, name: str) -> None:
    if _hash_value(value, name) != expected:
        raise _fail(f"{name} differs from independently reconstructed hash")


def _expect_optional_hash(
    value: object,
    expected: str | None,
    name: str,
) -> None:
    if _optional_hash(value, name) != expected:
        raise _fail(f"{name} differs from independently reconstructed hash")


def _expect_time(
    row: Mapping[str, Any],
    column: str,
    expected: datetime,
    name: str,
) -> None:
    if _datetime_value(row[column], f"{name}.{column}") != expected:
        raise _fail(f"{name}.{column} differs from canonical time")


def _provenance(row: Mapping[str, Any], name: str) -> EvidenceProvenance:
    try:
        value = EvidenceProvenance(
            history_origin=HistoryOrigin(
                _text_value(row["history_origin"], f"{name}.history_origin")
            ),
            history_origin_id=_optional_text(
                row["history_origin_id"], f"{name}.history_origin_id"
            ),
            history_origin_at=(
                None
                if row["history_origin_at"] is None
                else _datetime_value(
                    row["history_origin_at"], f"{name}.history_origin_at"
                )
            ),
            authority_status=AuthorityStatus(
                _text_value(row["authority_status"], f"{name}.authority_status")
            ),
            authority_receipt_hash=_optional_hash(
                row["authority_receipt_hash"],
                f"{name}.authority_receipt_hash",
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AccountingEvidenceAuditError):
            raise
        raise _fail(f"{name} provenance cannot be reconstructed") from exc
    if value.authority_status is not AuthorityStatus.CONTENT_HASH_ONLY:
        raise _fail(f"{name} accounting authority must be CONTENT_HASH_ONLY")
    if value.history_origin not in {
        HistoryOrigin.START_AFTER_UNKNOWN,
        HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN,
    }:
        raise _fail(f"{name} accounting history origin is not admissible")
    _expect_hash(row["provenance_hash"], value.provenance_hash, f"{name}.provenance_hash")
    _expect_hash(
        row["__dbhash_provenance_hash"],
        value.provenance_hash,
        f"{name}.__dbhash_provenance_hash",
    )
    return value


LOT_JSON_FIELDS = frozenset(
    {
        "lot_id",
        "account_id",
        "stock_code",
        "theme_code",
        "strategy_version",
        "opened_fill_id",
        "opened_trade_date",
        "settlement_date",
        "original_quantity",
        "remaining_quantity",
        "cost_price",
        "allocated_buy_fee",
        "position_state",
        "approved_target_quantity",
        "add_count",
        "initial_stop",
        "protective_stop",
        "invalidation_condition",
        "version",
        "created_at",
        "closed_at",
    }
)


def _iso_date(value: object, name: str) -> date:
    text_value = _text_value(value, name)
    try:
        parsed = date.fromisoformat(text_value)
    except ValueError as exc:
        raise _fail(f"{name} is not canonical ISO date text") from exc
    if parsed.isoformat() != text_value:
        raise _fail(f"{name} is not canonical ISO date text")
    return parsed


def _iso_datetime(value: object, name: str) -> datetime:
    text_value = _text_value(value, name)
    try:
        parsed = datetime.fromisoformat(text_value)
    except ValueError as exc:
        raise _fail(f"{name} is not canonical ISO datetime text") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(f"{name} must carry an offset")
    if parsed.isoformat(timespec="microseconds") != text_value:
        raise _fail(f"{name} is not canonical ISO datetime text")
    return parsed


def _payload_decimal(value: object, name: str, *, scale: int) -> Decimal:
    text_value = _text_value(value, name)
    try:
        parsed = Decimal(text_value)
    except InvalidOperation as exc:
        raise _fail(f"{name} is not decimal text") from exc
    result = _decimal_value(parsed, name, scale=scale)
    if format(result, f".{scale}f") != text_value:
        raise _fail(f"{name} is not canonical decimal text")
    return result


def _lot_from_json(raw: object, name: str) -> LotSnapshot:
    if type(raw) is not str:
        raise _fail(f"{name} must be exact canonical JSON text")
    try:
        canonical = CanonicalJson(raw)
        payload = canonical.value()
    except (TypeError, ValueError) as exc:
        raise _fail(f"{name} is not strict canonical JSON") from exc
    if type(payload) is not dict or frozenset(payload) != LOT_JSON_FIELDS:
        raise _fail(f"{name} lot snapshot fields are not exact")
    try:
        value = LotSnapshot(
            lot_id=_text_value(payload["lot_id"], f"{name}.lot_id"),
            account_id=_text_value(payload["account_id"], f"{name}.account_id"),
            stock_code=_text_value(payload["stock_code"], f"{name}.stock_code"),
            theme_code=_text_value(
                payload["theme_code"], f"{name}.theme_code", allow_empty=True
            ),
            strategy_version=_text_value(
                payload["strategy_version"], f"{name}.strategy_version"
            ),
            opened_fill_id=_text_value(
                payload["opened_fill_id"], f"{name}.opened_fill_id"
            ),
            opened_trade_date=_iso_date(
                payload["opened_trade_date"], f"{name}.opened_trade_date"
            ),
            settlement_date=_iso_date(
                payload["settlement_date"], f"{name}.settlement_date"
            ),
            original_quantity=_int_value(
                payload["original_quantity"],
                f"{name}.original_quantity",
                minimum=1,
            ),
            remaining_quantity=_int_value(
                payload["remaining_quantity"], f"{name}.remaining_quantity"
            ),
            cost_price=_payload_decimal(
                payload["cost_price"], f"{name}.cost_price", scale=6
            ),
            allocated_buy_fee=_payload_decimal(
                payload["allocated_buy_fee"],
                f"{name}.allocated_buy_fee",
                scale=2,
            ),
            position_state=PositionState(
                _text_value(
                    payload["position_state"], f"{name}.position_state"
                )
            ),
            approved_target_quantity=_int_value(
                payload["approved_target_quantity"],
                f"{name}.approved_target_quantity",
                minimum=1,
            ),
            add_count=_int_value(payload["add_count"], f"{name}.add_count"),
            initial_stop=_payload_decimal(
                payload["initial_stop"], f"{name}.initial_stop", scale=6
            ),
            protective_stop=_payload_decimal(
                payload["protective_stop"],
                f"{name}.protective_stop",
                scale=6,
            ),
            invalidation_condition=_text_value(
                payload["invalidation_condition"],
                f"{name}.invalidation_condition",
            ),
            version=_int_value(payload["version"], f"{name}.version", minimum=1),
            created_at=_iso_datetime(payload["created_at"], f"{name}.created_at"),
            closed_at=(
                None
                if payload["closed_at"] is None
                else _iso_datetime(payload["closed_at"], f"{name}.closed_at")
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AccountingEvidenceAuditError):
            raise
        raise _fail(f"{name} lot snapshot cannot be reconstructed") from exc
    rebuilt = CanonicalJson.from_value(value.canonical_payload())
    if rebuilt.json_text != raw:
        raise _fail(f"{name} differs from reconstructed canonical JSON")
    return value


def _exact_row_keys(
    row: Mapping[str, Any],
    columns: tuple[str, ...],
    aliases: tuple[str, ...],
    name: str,
) -> None:
    if not isinstance(row, Mapping):
        raise _fail(f"{name} must be a mapping row")
    if frozenset(row) != frozenset((*columns, *aliases)):
        raise _fail(f"{name} columns are not exact")


def _effect(
    row: Mapping[str, Any],
    number: int,
    parents: V2AccountingEvidenceAuditParents,
) -> LotAccountingEffect:
    name = f"lot_effect[{number}]"
    _exact_row_keys(row, LOT_EFFECT_COLUMNS, LOT_EFFECT_DB_HASH_ALIASES, name)
    evidence_id = _hash_value(
        row["fill_execution_evidence_id"], f"{name}.fill_execution_evidence_id"
    )
    try:
        fill = parents.fills[evidence_id]
    except KeyError as exc:
        raise _fail(f"{name} references absent fill evidence") from exc
    _expect_hash(
        row["fill_execution_evidence_hash"],
        fill.evidence_hash,
        f"{name}.fill_execution_evidence_hash",
    )
    if _text_value(row["fill_id"], f"{name}.fill_id") != fill.fill_id:
        raise _fail(f"{name}.fill_id differs from fill evidence")
    before_raw = row["before_lot_json"]
    before = None if before_raw is None else _lot_from_json(before_raw, f"{name}.before")
    after = _lot_from_json(row["after_lot_json"], f"{name}.after")
    if _text_value(row["lot_id"], f"{name}.lot_id") != after.lot_id:
        raise _fail(f"{name}.lot_id differs from after snapshot")
    before_hash = None if before is None else before.snapshot_hash
    _expect_optional_hash(row["before_lot_hash"], before_hash, f"{name}.before_lot_hash")
    _expect_optional_hash(
        row["__dbhash_before_lot_hash"],
        before_hash,
        f"{name}.__dbhash_before_lot_hash",
    )
    _expect_hash(row["after_lot_hash"], after.snapshot_hash, f"{name}.after_lot_hash")
    _expect_hash(
        row["__dbhash_after_lot_hash"],
        after.snapshot_hash,
        f"{name}.__dbhash_after_lot_hash",
    )
    provenance = _provenance(row, name)
    try:
        value = LotAccountingEffect(
            fill_execution_evidence=fill,
            effect_sequence=_int_value(
                row["effect_sequence"], f"{name}.effect_sequence"
            ),
            lot_transition_sequence=_int_value(
                row["lot_transition_sequence"],
                f"{name}.lot_transition_sequence",
            ),
            effect_kind=LotEffectKind(
                _text_value(row["effect_kind"], f"{name}.effect_kind")
            ),
            before_lot=before,
            after_lot=after,
            consumed_quantity=_int_value(
                row["consumed_quantity"], f"{name}.consumed_quantity"
            ),
            occurred_at=_datetime_value(row["occurred_at"], f"{name}.occurred_at"),
            bound_at=_datetime_value(row["bound_at"], f"{name}.bound_at"),
            provenance=provenance,
            previous_effect_id=_optional_hash(
                row["previous_effect_id"], f"{name}.previous_effect_id"
            ),
            previous_effect_hash=_optional_hash(
                row["previous_effect_hash"], f"{name}.previous_effect_hash"
            ),
            previous_lot_transition_id=_optional_hash(
                row["previous_lot_transition_id"],
                f"{name}.previous_lot_transition_id",
            ),
            previous_lot_transition_hash=_optional_hash(
                row["previous_lot_transition_hash"],
                f"{name}.previous_lot_transition_hash",
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AccountingEvidenceAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    outcome_id = _hash_value(row["accounting_outcome_id"], f"{name}.outcome_id")
    if not outcome_id:
        raise _fail(f"{name} outcome identity is empty")
    _expect_hash(
        row["lot_effect_root_hash"],
        value.lot_effect_root_hash,
        f"{name}.lot_effect_root_hash",
    )
    _expect_hash(
        row["__dbhash_lot_effect_root_hash"],
        value.lot_effect_root_hash,
        f"{name}.__dbhash_lot_effect_root_hash",
    )
    _expect_hash(row["effect_hash"], value.effect_hash, f"{name}.effect_hash")
    _expect_hash(
        row["lot_transition_evidence_id"],
        value.lot_transition_evidence_id,
        f"{name}.lot_transition_evidence_id",
    )
    _expect_hash(
        row["__dbhash_effect_hash"],
        value.effect_hash,
        f"{name}.__dbhash_effect_hash",
    )
    _expect_time(row, "created_at", value.bound_at, name)
    return value


def _outcome(
    row: Mapping[str, Any],
    number: int,
    effects: tuple[LotAccountingEffect, ...],
    parents: V2AccountingEvidenceAuditParents,
) -> FillAccountingOutcome:
    name = f"outcome[{number}]"
    _exact_row_keys(row, OUTCOME_COLUMNS, OUTCOME_DB_HASH_ALIASES, name)
    fill_evidence_id = _hash_value(
        row["fill_execution_evidence_id"], f"{name}.fill_execution_evidence_id"
    )
    cash_binding_id = _hash_value(row["cash_binding_id"], f"{name}.cash_binding_id")
    transition_id = _hash_value(
        row["order_transition_id"], f"{name}.order_transition_id"
    )
    try:
        fill = parents.fills[fill_evidence_id]
        cash = parents.cash_bindings[cash_binding_id]
        transition = parents.order_transitions[transition_id]
    except KeyError as exc:
        raise _fail(f"{name} references absent execution evidence") from exc
    _expect_hash(
        row["fill_execution_evidence_hash"],
        fill.evidence_hash,
        f"{name}.fill_execution_evidence_hash",
    )
    _expect_hash(row["cash_binding_hash"], cash.binding_hash, f"{name}.cash_hash")
    _expect_hash(
        row["order_transition_hash"],
        transition.transition_hash,
        f"{name}.transition_hash",
    )
    direct_values = {
        "fill_id": fill.fill_id,
        "cash_event_id": cash.cash_event_id,
        "order_id": fill.order_id,
        "account_id": fill.account_id,
        "stock_code": fill.stock_code,
        "side": str(fill.fill_payload.value()["side"]),
    }
    for column, expected in direct_values.items():
        if _text_value(row[column], f"{name}.{column}") != expected:
            raise _fail(f"{name}.{column} differs from nested evidence")
    provenance = _provenance(row, name)
    try:
        value = FillAccountingOutcome(
            fill_execution_evidence=fill,
            cash_binding=cash,
            order_transition=transition,
            account_cash_before=_decimal_value(
                row["account_cash_before"], f"{name}.account_cash_before", scale=2
            ),
            account_cash_after=_decimal_value(
                row["account_cash_after"], f"{name}.account_cash_after", scale=2
            ),
            lot_effects=effects,
            recorded_at=_datetime_value(row["recorded_at"], f"{name}.recorded_at"),
            provenance=provenance,
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AccountingEvidenceAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    integer_values = {
        "lot_effect_count": value.lot_effect_count,
        "total_effect_quantity": value.total_effect_quantity,
    }
    for column, expected in integer_values.items():
        if _int_value(row[column], f"{name}.{column}", minimum=1) != expected:
            raise _fail(f"{name}.{column} differs from reconstructed outcome")
    for column, expected in (
        ("lot_effect_root_hash", value.lot_effect_root_hash),
        ("lot_effects_hash", value.lot_effects_hash),
        ("outcome_hash", value.outcome_hash),
        ("accounting_outcome_id", value.accounting_outcome_id),
    ):
        _expect_hash(row[column], expected, f"{name}.{column}")
    _expect_hash(
        row["__dbhash_lot_effect_root_hash"],
        value.lot_effect_root_hash,
        f"{name}.__dbhash_lot_effect_root_hash",
    )
    _expect_hash(
        row["__dbhash_outcome_hash"],
        value.outcome_hash,
        f"{name}.__dbhash_outcome_hash",
    )
    _expect_time(row, "created_at", value.recorded_at, name)
    return value


def _finalization(
    row: Mapping[str, Any],
    number: int,
    outcome: FillAccountingOutcome,
) -> FillAccountingFinalization:
    name = f"finalization[{number}]"
    _exact_row_keys(row, FINALIZATION_COLUMNS, FINALIZATION_DB_HASH_ALIASES, name)
    try:
        manifest = CanonicalJson(
            _text_value(row["effect_hashes_json"], f"{name}.effect_hashes_json")
        )
        hashes = manifest.value()
    except (TypeError, ValueError) as exc:
        if isinstance(exc, V2AccountingEvidenceAuditError):
            raise
        raise _fail(f"{name}.effect_hashes_json is not strict canonical JSON") from exc
    expected_hashes = [item.effect_hash for item in outcome.lot_effects]
    if type(hashes) is not list or hashes != expected_hashes:
        raise _fail(f"{name} effect hash manifest differs from ordered effects")
    for index, item in enumerate(hashes):
        _hash_value(item, f"{name}.effect_hashes[{index}]")
    try:
        value = FillAccountingFinalization(
            accounting_outcome=outcome,
            finalized_at=_datetime_value(row["finalized_at"], f"{name}.finalized_at"),
            finalization_status=AccountingOutcomeFinalizationStatus(
                _text_value(
                    row["finalization_status"], f"{name}.finalization_status"
                )
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AccountingEvidenceAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    fill = outcome.fill_execution_evidence
    hash_values = {
        "accounting_outcome_id": outcome.accounting_outcome_id,
        "outcome_hash": outcome.outcome_hash,
        "fill_execution_evidence_id": fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": fill.evidence_hash,
        "lot_effect_root_hash": outcome.lot_effect_root_hash,
        "lot_effects_hash": outcome.lot_effects_hash,
        "provenance_hash": outcome.provenance.provenance_hash,
        "finalization_hash": value.finalization_hash,
        "finalization_id": value.finalization_id,
    }
    for column, expected in hash_values.items():
        _expect_hash(row[column], expected, f"{name}.{column}")
    if _text_value(row["fill_id"], f"{name}.fill_id") != fill.fill_id:
        raise _fail(f"{name}.fill_id differs from outcome")
    for column, expected in (
        ("lot_effect_count", outcome.lot_effect_count),
        ("total_effect_quantity", outcome.total_effect_quantity),
    ):
        if _int_value(row[column], f"{name}.{column}", minimum=1) != expected:
            raise _fail(f"{name}.{column} differs from outcome")
    provenance = _provenance(row, name)
    if provenance != outcome.provenance:
        raise _fail(f"{name} provenance differs from outcome")
    if value.effect_hashes_json != row["effect_hashes_json"]:
        raise _fail(f"{name}.effect_hashes_json is not canonical")
    _expect_hash(
        row["__dbhash_lot_effects_hash"],
        outcome.lot_effects_hash,
        f"{name}.__dbhash_lot_effects_hash",
    )
    _expect_hash(
        row["__dbhash_finalization_hash"],
        value.finalization_hash,
        f"{name}.__dbhash_finalization_hash",
    )
    _expect_time(row, "created_at", value.finalized_at, name)
    return value


def _unique_objects(values: tuple[Any, ...], attribute: str, name: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for value in values:
        key = getattr(value, attribute)
        if key in result:
            raise _fail(f"duplicate reconstructed {name}: {key}")
        result[key] = value
    return result


def _parent_row_map(
    value: Mapping[str, Mapping[str, Any]],
    *,
    key_column: str,
    columns: tuple[str, ...],
    name: str,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, Mapping):
        raise _fail(f"{name} parents must be a mapping")
    result: dict[str, Mapping[str, Any]] = {}
    for raw_key, row in value.items():
        key = _text_value(raw_key, f"{name} parent key")
        if not isinstance(row, Mapping) or frozenset(row) != frozenset(columns):
            raise _fail(f"{name} parent row columns are not exact")
        if _text_value(row[key_column], f"{name}.{key_column}") != key:
            raise _fail(f"{name} parent map key differs from row")
        if key in result:
            raise _fail(f"duplicate {name} parent: {key}")
        result[key] = row
    return result


def _canonical_payload_matches(
    supplied: CanonicalJson,
    projection: Mapping[str, Any],
    name: str,
) -> None:
    expected = CanonicalJson.from_value(dict(projection))
    if supplied.json_text != expected.json_text or supplied.payload_hash != expected.payload_hash:
        raise _fail(f"{name} differs from canonical V2 parent projection")


def _order_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _text_value(row["account_id"], "order.account_id"),
        "created_at": _datetime_value(row["created_at"], "order.created_at"),
        "earliest_at": _datetime_value(row["earliest_at"], "order.earliest_at"),
        "expires_at": _datetime_value(row["expires_at"], "order.expires_at"),
        "idempotency_key": _text_value(
            row["idempotency_key"], "order.idempotency_key"
        ),
        "intent_id": _text_value(row["intent_id"], "order.intent_id"),
        "limit_price": _decimal_text(row["limit_price"], "order.limit_price", scale=6),
        "order_id": _text_value(row["order_id"], "order.order_id"),
        "order_type": _text_value(row["order_type"], "order.order_type"),
        "quantity": _int_value(row["quantity"], "order.quantity", minimum=1),
        "side": _text_value(row["side"], "order.side"),
        "stock_code": _text_value(row["stock_code"], "order.stock_code"),
    }


def _fill_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _text_value(row["account_id"], "fill.account_id"),
        "created_at": _datetime_value(row["created_at"], "fill.created_at"),
        "fee_amount": _decimal_text(row["fee_amount"], "fill.fee_amount", scale=2),
        "fill_id": _text_value(row["fill_id"], "fill.fill_id"),
        "filled_at": _datetime_value(row["filled_at"], "fill.filled_at"),
        "gross_amount": _decimal_text(
            row["gross_amount"], "fill.gross_amount", scale=2
        ),
        "idempotency_key": _text_value(
            row["idempotency_key"], "fill.idempotency_key"
        ),
        "match_event_id": _text_value(row["match_event_id"], "fill.match_event_id"),
        "net_cash_amount": _decimal_text(
            row["net_cash_amount"], "fill.net_cash_amount", scale=2
        ),
        "order_id": _text_value(row["order_id"], "fill.order_id"),
        "price": _decimal_text(row["price"], "fill.price", scale=6),
        "quantity": _int_value(row["quantity"], "fill.quantity", minimum=1),
        "quote_event_id": _text_value(row["quote_event_id"], "fill.quote_event_id"),
        "side": _text_value(row["side"], "fill.side"),
        "stock_code": _text_value(row["stock_code"], "fill.stock_code"),
    }


def _cash_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _text_value(row["account_id"], "cash.account_id"),
        "amount": _decimal_text(row["amount"], "cash.amount", scale=2),
        "balance_after": _decimal_text(
            row["balance_after"], "cash.balance_after", scale=2
        ),
        "business_event_key": _text_value(
            row["business_event_key"], "cash.business_event_key"
        ),
        "cash_event_id": _text_value(row["cash_event_id"], "cash.cash_event_id"),
        "created_at": _datetime_value(row["created_at"], "cash.created_at"),
        "event_type": _text_value(row["event_type"], "cash.event_type"),
        "occurred_at": _datetime_value(row["occurred_at"], "cash.occurred_at"),
        "related_fill_id": _optional_text(row["related_fill_id"], "cash.related_fill_id"),
        "related_order_id": _optional_text(
            row["related_order_id"], "cash.related_order_id"
        ),
        "reversal_of": _optional_text(row["reversal_of"], "cash.reversal_of"),
    }


def _lot_from_parent_row(row: Mapping[str, Any], name: str) -> LotSnapshot:
    try:
        return LotSnapshot(
            lot_id=_text_value(row["lot_id"], f"{name}.lot_id"),
            account_id=_text_value(row["account_id"], f"{name}.account_id"),
            stock_code=_text_value(row["stock_code"], f"{name}.stock_code"),
            theme_code=_text_value(
                row["theme_code"], f"{name}.theme_code", allow_empty=True
            ),
            strategy_version=_text_value(
                row["strategy_version"], f"{name}.strategy_version"
            ),
            opened_fill_id=_text_value(
                row["opened_fill_id"], f"{name}.opened_fill_id"
            ),
            opened_trade_date=_date_value(
                row["opened_trade_date"], f"{name}.opened_trade_date"
            ),
            settlement_date=_date_value(
                row["settlement_date"], f"{name}.settlement_date"
            ),
            original_quantity=_int_value(
                row["original_quantity"], f"{name}.original_quantity", minimum=1
            ),
            remaining_quantity=_int_value(
                row["remaining_quantity"], f"{name}.remaining_quantity"
            ),
            cost_price=_decimal_value(
                row["cost_price"], f"{name}.cost_price", scale=6
            ),
            allocated_buy_fee=_decimal_value(
                row["allocated_buy_fee"], f"{name}.allocated_buy_fee", scale=2
            ),
            position_state=PositionState(
                _text_value(row["position_state"], f"{name}.position_state")
            ),
            approved_target_quantity=_int_value(
                row["approved_target_quantity"],
                f"{name}.approved_target_quantity",
                minimum=1,
            ),
            add_count=_int_value(row["add_count"], f"{name}.add_count"),
            initial_stop=_decimal_value(
                row["initial_stop"], f"{name}.initial_stop", scale=6
            ),
            protective_stop=_decimal_value(
                row["protective_stop"], f"{name}.protective_stop", scale=6
            ),
            invalidation_condition=_text_value(
                row["invalidation_condition"], f"{name}.invalidation_condition"
            ),
            version=_int_value(row["version"], f"{name}.version", minimum=1),
            created_at=_datetime_value(row["created_at"], f"{name}.created_at"),
            closed_at=(
                None
                if row["closed_at"] is None
                else _datetime_value(row["closed_at"], f"{name}.closed_at")
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AccountingEvidenceAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc


def _validate_parent_facts(
    outcomes: tuple[FillAccountingOutcome, ...],
    parents: V2AccountingEvidenceAuditParents,
) -> tuple[dict[str, LotSnapshot], tuple[tuple[str, str], ...]]:
    accounts = _parent_row_map(
        parents.accounts,
        key_column="account_id",
        columns=ACCOUNT_COLUMNS,
        name="account",
    )
    orders = _parent_row_map(
        parents.orders,
        key_column="order_id",
        columns=ORDER_COLUMNS,
        name="order",
    )
    fill_facts = _parent_row_map(
        parents.fill_facts,
        key_column="fill_id",
        columns=FILL_COLUMNS,
        name="fill",
    )
    cash_facts = _parent_row_map(
        parents.cash_facts,
        key_column="cash_event_id",
        columns=CASH_COLUMNS,
        name="cash",
    )
    lot_rows = _parent_row_map(
        parents.lots,
        key_column="lot_id",
        columns=LOT_COLUMNS,
        name="lot",
    )
    lots = {
        lot_id: _lot_from_parent_row(row, f"canonical_lot[{lot_id}]")
        for lot_id, row in lot_rows.items()
    }
    checked: set[tuple[str, str]] = set()
    for number, outcome in enumerate(outcomes):
        name = f"outcome[{number}]"
        fill = outcome.fill_execution_evidence
        cash = outcome.cash_binding
        try:
            account_row = accounts[fill.account_id]
            order_row = orders[fill.order_id]
            fill_row = fill_facts[fill.fill_id]
            cash_row = cash_facts[cash.cash_event_id]
        except KeyError as exc:
            raise _fail(f"{name} references an absent canonical V2 fact") from exc
        if _decimal_value(
            account_row["cash_balance"], "account.cash_balance", scale=2
        ) < 0:
            raise _fail(f"{name} canonical account cash is negative")
        _canonical_payload_matches(fill.order_payload, _order_projection(order_row), "order payload")
        _canonical_payload_matches(fill.fill_payload, _fill_projection(fill_row), "fill payload")
        _canonical_payload_matches(
            cash.cash_event_payload, _cash_projection(cash_row), "cash payload"
        )
        checked.update(
            {
                ("account", fill.account_id),
                ("order", fill.order_id),
                ("fill", fill.fill_id),
                ("cash", cash.cash_event_id),
            }
        )
    for lot_id, lot in lots.items():
        try:
            opened_fill = fill_facts[lot.opened_fill_id]
        except KeyError as exc:
            raise _fail(f"canonical lot {lot_id} has no opened-fill parent") from exc
        if (
            _text_value(opened_fill["account_id"], f"lot[{lot_id}].opened_account")
            != lot.account_id
            or _text_value(opened_fill["stock_code"], f"lot[{lot_id}].opened_stock")
            != lot.stock_code
        ):
            raise _fail(f"canonical lot {lot_id} differs from its opened fill")
        checked.add(("lot", lot_id))
        checked.add(("fill", lot.opened_fill_id))
    return lots, tuple(sorted(checked))


def _validate_lot_chains(
    effects: tuple[LotAccountingEffect, ...],
    canonical_lots: Mapping[str, LotSnapshot],
) -> tuple[str, ...]:
    by_lot: dict[str, list[LotAccountingEffect]] = {}
    by_id = _unique_objects(effects, "lot_transition_evidence_id", "lot effect")
    for effect in effects:
        by_lot.setdefault(effect.after_lot.lot_id, []).append(effect)
    for lot_id, items in by_lot.items():
        ordered = sorted(items, key=lambda item: item.lot_transition_sequence)
        for sequence, effect in enumerate(ordered):
            if effect.lot_transition_sequence != sequence:
                raise _fail(f"lot chain {lot_id} is not contiguous from zero")
            if sequence == 0:
                if effect.previous_lot_transition_id is not None:
                    raise _fail(f"lot chain {lot_id} genesis has a predecessor")
            else:
                previous = ordered[sequence - 1]
                if (
                    effect.previous_lot_transition_id
                    != previous.lot_transition_evidence_id
                    or effect.previous_lot_transition_hash != previous.effect_hash
                    or effect.before_lot != previous.after_lot
                ):
                    raise _fail(f"lot chain {lot_id} predecessor is discontinuous")
                if effect.previous_lot_transition_id not in by_id:
                    raise _fail(f"lot chain {lot_id} predecessor is absent")
        current = canonical_lots.get(lot_id)
        if current is None:
            raise _fail(f"lot chain {lot_id} has no canonical lot parent")
        if current != ordered[-1].after_lot:
            raise _fail(f"canonical lot {lot_id} differs from latest accounting effect")
    return tuple(sorted(by_lot))


def expected_accounting_hash_verifications(
    rows_by_table: Mapping[str, tuple[Mapping[str, Any], ...]],
) -> tuple[tuple[str, int], ...]:
    """Return the exact logical hash count for the supplied accounting rows."""

    if set(rows_by_table) != set(ACCOUNTING_AUDIT_TABLES):
        raise _fail("accounting audit requires exactly the three migration-015 tables")
    counts: list[tuple[str, int]] = []
    for table in ACCOUNTING_AUDIT_TABLES:
        rows = rows_by_table[table]
        if type(rows) is not tuple:
            raise _fail(f"{table} rows must be exactly tuple")
        count = len(rows) * len(ACCOUNTING_AUDIT_HASH_FIELDS[table])
        counts.append((table, count))
    return tuple(counts)


def audit_v2_accounting_evidence_rows(
    rows_by_table: Mapping[str, tuple[Mapping[str, Any], ...]],
    *,
    parents: V2AccountingEvidenceAuditParents,
    database_sha2_used: bool = True,
    shared_row_locks_used: bool = False,
) -> V2AccountingEvidenceAuditReport:
    """Reconstruct all migration-015 rows already carrying DB SHA2 aliases."""

    if set(rows_by_table) != set(ACCOUNTING_AUDIT_TABLES):
        raise _fail("accounting audit requires exactly the three migration-015 tables")
    if type(parents) is not V2AccountingEvidenceAuditParents:
        raise _fail("accounting audit parents have an invalid type")
    if type(database_sha2_used) is not bool or not database_sha2_used:
        raise _fail("database SHA2 recomputation is mandatory")
    if type(shared_row_locks_used) is not bool:
        raise _fail("shared_row_locks_used must be exactly bool")
    for table in ACCOUNTING_AUDIT_TABLES:
        if type(rows_by_table[table]) is not tuple:
            raise _fail(f"{table} rows must be exactly tuple")

    effect_rows = rows_by_table[LOT_EFFECT_TABLE]
    effects = tuple(_effect(row, number, parents) for number, row in enumerate(effect_rows))
    effect_ids = _unique_objects(effects, "lot_transition_evidence_id", "lot effect")
    effect_hashes = _unique_objects(effects, "effect_hash", "lot effect hash")
    if len(effect_ids) != len(effect_hashes):
        raise _fail("lot effect identities and hashes are not one-to-one")
    raw_effect_groups: dict[str, list[tuple[Mapping[str, Any], LotAccountingEffect]]] = {}
    seen_outcome_sequences: set[tuple[str, int]] = set()
    seen_fill_lots: set[tuple[str, str]] = set()
    for row, effect in zip(effect_rows, effects, strict=True):
        outcome_id = _hash_value(row["accounting_outcome_id"], "effect.outcome_id")
        sequence_key = (outcome_id, effect.effect_sequence)
        fill_lot_key = (effect.fill_execution_evidence.fill_id, effect.after_lot.lot_id)
        if sequence_key in seen_outcome_sequences:
            raise _fail(f"duplicate outcome/effect sequence: {sequence_key}")
        if fill_lot_key in seen_fill_lots:
            raise _fail(f"duplicate fill/lot accounting effect: {fill_lot_key}")
        seen_outcome_sequences.add(sequence_key)
        seen_fill_lots.add(fill_lot_key)
        raw_effect_groups.setdefault(outcome_id, []).append((row, effect))

    outcome_rows = rows_by_table[OUTCOME_TABLE]
    outcomes_list: list[FillAccountingOutcome] = []
    outcome_ids_seen: set[str] = set()
    outcome_fill_ids: set[str] = set()
    for number, row in enumerate(outcome_rows):
        outcome_id = _hash_value(row["accounting_outcome_id"], f"outcome[{number}].id")
        if outcome_id in outcome_ids_seen:
            raise _fail(f"duplicate accounting outcome: {outcome_id}")
        fill_id = _text_value(row["fill_id"], f"outcome[{number}].fill_id")
        if fill_id in outcome_fill_ids:
            raise _fail(f"duplicate accounting outcome fill: {fill_id}")
        grouped = raw_effect_groups.pop(outcome_id, None)
        if not grouped:
            raise _fail(f"accounting outcome {outcome_id} has no lot effects")
        grouped.sort(key=lambda pair: pair[1].effect_sequence)
        ordered_effects = tuple(item[1] for item in grouped)
        for expected_sequence, effect in enumerate(ordered_effects):
            if effect.effect_sequence != expected_sequence:
                raise _fail(f"accounting outcome {outcome_id} effect sequence is not contiguous")
        value = _outcome(row, number, ordered_effects, parents)
        outcomes_list.append(value)
        outcome_ids_seen.add(outcome_id)
        outcome_fill_ids.add(fill_id)
    if raw_effect_groups:
        raise _fail("one or more lot effects reference an absent accounting outcome")
    outcomes = tuple(outcomes_list)
    outcomes_by_id = _unique_objects(outcomes, "accounting_outcome_id", "outcome")

    canonical_lots, parent_row_checks = _validate_parent_facts(outcomes, parents)
    lot_chain_ids = _validate_lot_chains(effects, canonical_lots)

    final_rows = rows_by_table[FINALIZATION_TABLE]
    finalizations: list[FillAccountingFinalization] = []
    finalized_ids: set[str] = set()
    finalized_fills: set[str] = set()
    for number, row in enumerate(final_rows):
        outcome_id = _hash_value(
            row["accounting_outcome_id"], f"finalization[{number}].outcome_id"
        )
        try:
            outcome = outcomes_by_id[outcome_id]
        except KeyError as exc:
            raise _fail(f"finalization[{number}] references absent outcome") from exc
        if outcome_id in finalized_ids:
            raise _fail(f"duplicate finalization for outcome: {outcome_id}")
        if outcome.fill_execution_evidence.fill_id in finalized_fills:
            raise _fail("duplicate finalization for fill")
        finalizations.append(_finalization(row, number, outcome))
        finalized_ids.add(outcome_id)
        finalized_fills.add(outcome.fill_execution_evidence.fill_id)
    missing_final = set(outcomes_by_id) - finalized_ids
    if missing_final:
        raise _fail(
            "accounting outcomes without FINAL marker: " + ", ".join(sorted(missing_final))
        )
    _unique_objects(tuple(finalizations), "finalization_id", "finalization")

    table_counts = tuple(
        (table, len(rows_by_table[table])) for table in ACCOUNTING_AUDIT_TABLES
    )
    hash_counts = expected_accounting_hash_verifications(rows_by_table)
    rows_reconstructed = len(outcomes) + len(effects) + len(finalizations)
    return V2AccountingEvidenceAuditReport(
        table_counts=table_counts,
        hash_verifications=hash_counts,
        hashes_verified=sum(value for _, value in hash_counts),
        rows_reconstructed=rows_reconstructed,
        finalized_outcomes=len(finalizations),
        finalized_outcome_ids=tuple(sorted(finalized_ids)),
        lot_chains_checked=len(lot_chain_ids),
        lot_chain_ids=lot_chain_ids,
        parent_rows_checked=len(parent_row_checks),
        parent_row_checks=parent_row_checks,
        database_sha2_used=True,
        shared_row_locks_used=shared_row_locks_used,
    )


def _sql_json_text(column: str) -> str:
    return f"JSON_QUOTE(CONVERT({column} USING utf8mb4))"


def _sql_nullable_json_text(column: str) -> str:
    return f"IF({column} IS NULL, 'null', {_sql_json_text(column)})"


def _sql_datetime(column: str) -> str:
    # Asia/Shanghai has no DST.  V2 persists wall-clock DATETIME at +08:00,
    # while the canonical contracts hash UTC ISO text with six microseconds.
    return (
        "JSON_QUOTE(CONCAT(DATE_FORMAT(DATE_SUB("
        f"{column}, INTERVAL 8 HOUR), '%Y-%m-%dT%H:%i:%s.000000'), '+00:00'))"
    )


def _sql_sha2(concat_expression: str, alias: str) -> str:
    return (
        f"LOWER(SHA2(CAST({concat_expression} AS BINARY), 256)) AS {alias}"
    )


def _db_provenance_expression(prefix: str = "") -> str:
    column = lambda name: f"{prefix}{name}"  # noqa: E731
    value = (
        "CONCAT('{\"namespace\":\"trading-v2.execution-evidence-provenance.v1\","
        "\"payload\":{\"authority_receipt_hash\":',"
        f"{_sql_nullable_json_text(column('authority_receipt_hash'))},"
        "',\"authority_status\":',"
        f"{_sql_json_text(column('authority_status'))},"
        "',\"history_origin\":',"
        f"{_sql_json_text(column('history_origin'))},"
        "',\"history_origin_at\":',"
        f"{_sql_datetime(column('history_origin_at'))},"
        "',\"history_origin_id\":',"
        f"{_sql_nullable_json_text(column('history_origin_id'))}, '}}}}')"
    )
    return _sql_sha2(value, "__dbhash_provenance_hash")


def _db_lot_snapshot_expression(column: str, alias: str) -> str:
    value = (
        "IF("
        f"{column} IS NULL, NULL, CONCAT("
        "'{\"namespace\":\"trading-v2.lot-snapshot.v1\",\"payload\":',"
        f"CONVERT({column} USING utf8mb4), '}}'))"
    )
    return _sql_sha2(value, alias)


def _db_lot_root_expression(prefix: str = "", outcome_prefix: str | None = None) -> str:
    source = prefix if outcome_prefix is None else outcome_prefix
    col = lambda name: f"{source}{name}"  # noqa: E731
    effect_col = lambda name: f"{prefix}{name}"  # noqa: E731
    value = (
        "CONCAT('{\"namespace\":\"trading-v2.lot-effect-root.v1\","
        "\"payload\":{\"account_id\":',"
        f"{_sql_json_text(col('account_id'))},"
        "',\"fill_execution_evidence_hash\":',"
        f"{_sql_json_text(effect_col('fill_execution_evidence_hash'))},"
        "',\"fill_execution_evidence_id\":',"
        f"{_sql_json_text(effect_col('fill_execution_evidence_id'))},"
        "',\"fill_id\":',"
        f"{_sql_json_text(effect_col('fill_id'))},"
        "',\"order_id\":',"
        f"{_sql_json_text(col('order_id'))},"
        "',\"side\":',"
        f"{_sql_json_text(col('side'))},"
        "',\"stock_code\":',"
        f"{_sql_json_text(col('stock_code'))}, '}}}}')"
    )
    return _sql_sha2(value, "__dbhash_lot_effect_root_hash")


def _db_outcome_expression(prefix: str = "") -> str:
    col = lambda name: f"{prefix}{name}"  # noqa: E731
    value = (
        "CONCAT('{\"namespace\":\"trading-v2.fill-accounting-outcome.v1\","
        "\"payload\":{\"account_cash_after\":',"
        f"JSON_QUOTE(CAST({col('account_cash_after')} AS CHAR)),"
        "',\"account_cash_before\":',"
        f"JSON_QUOTE(CAST({col('account_cash_before')} AS CHAR)),"
        "',\"account_id\":',"
        f"{_sql_json_text(col('account_id'))},"
        "',\"cash_binding_hash\":',"
        f"{_sql_json_text(col('cash_binding_hash'))},"
        "',\"cash_binding_id\":',"
        f"{_sql_json_text(col('cash_binding_id'))},"
        "',\"fill_execution_evidence_hash\":',"
        f"{_sql_json_text(col('fill_execution_evidence_hash'))},"
        "',\"fill_execution_evidence_id\":',"
        f"{_sql_json_text(col('fill_execution_evidence_id'))},"
        "',\"lot_effect_count\":',"
        f"CAST({col('lot_effect_count')} AS CHAR),"
        "',\"lot_effect_root_hash\":',"
        f"{_sql_json_text(col('lot_effect_root_hash'))},"
        "',\"lot_effects_hash\":',"
        f"{_sql_json_text(col('lot_effects_hash'))},"
        "',\"order_transition_hash\":',"
        f"{_sql_json_text(col('order_transition_hash'))},"
        "',\"order_transition_id\":',"
        f"{_sql_json_text(col('order_transition_id'))},"
        "',\"provenance_hash\":',"
        f"{_sql_json_text(col('provenance_hash'))},"
        "',\"recorded_at\":',"
        f"{_sql_datetime(col('recorded_at'))},"
        "',\"side\":',"
        f"{_sql_json_text(col('side'))},"
        "',\"stock_code\":',"
        f"{_sql_json_text(col('stock_code'))},"
        "',\"total_effect_quantity\":',"
        f"CAST({col('total_effect_quantity')} AS CHAR), '}}}}')"
    )
    return _sql_sha2(value, "__dbhash_outcome_hash")


def _db_effect_expression(prefix: str = "") -> str:
    col = lambda name: f"{prefix}{name}"  # noqa: E731
    value = (
        "CONCAT('{\"namespace\":\"trading-v2.lot-accounting-effect.v1\","
        "\"payload\":{\"after_lot_hash\":',"
        f"{_sql_json_text(col('after_lot_hash'))},"
        "',\"before_lot_hash\":',"
        f"{_sql_nullable_json_text(col('before_lot_hash'))},"
        "',\"bound_at\":',"
        f"{_sql_datetime(col('bound_at'))},"
        "',\"consumed_quantity\":',"
        f"CAST({col('consumed_quantity')} AS CHAR),"
        "',\"effect_kind\":',"
        f"{_sql_json_text(col('effect_kind'))},"
        "',\"effect_sequence\":',"
        f"CAST({col('effect_sequence')} AS CHAR),"
        "',\"fill_execution_evidence_hash\":',"
        f"{_sql_json_text(col('fill_execution_evidence_hash'))},"
        "',\"fill_execution_evidence_id\":',"
        f"{_sql_json_text(col('fill_execution_evidence_id'))},"
        "',\"lot_effect_root_hash\":',"
        f"{_sql_json_text(col('lot_effect_root_hash'))},"
        "',\"lot_id\":',"
        f"{_sql_json_text(col('lot_id'))},"
        "',\"lot_transition_sequence\":',"
        f"CAST({col('lot_transition_sequence')} AS CHAR),"
        "',\"occurred_at\":',"
        f"{_sql_datetime(col('occurred_at'))},"
        "',\"previous_effect_hash\":',"
        f"{_sql_nullable_json_text(col('previous_effect_hash'))},"
        "',\"previous_effect_id\":',"
        f"{_sql_nullable_json_text(col('previous_effect_id'))},"
        "',\"previous_lot_transition_hash\":',"
        f"{_sql_nullable_json_text(col('previous_lot_transition_hash'))},"
        "',\"previous_lot_transition_id\":',"
        f"{_sql_nullable_json_text(col('previous_lot_transition_id'))},"
        "',\"provenance_hash\":',"
        f"{_sql_json_text(col('provenance_hash'))}, '}}}}')"
    )
    return _sql_sha2(value, "__dbhash_effect_hash")


def _db_effect_list_expression(prefix: str = "") -> str:
    col = lambda name: f"{prefix}{name}"  # noqa: E731
    value = (
        "CONCAT('{\"namespace\":\"trading-v2.lot-accounting-effect-list.v1\","
        "\"payload\":{\"effect_hashes\":',"
        f"CONVERT({col('effect_hashes_json')} USING utf8mb4),"
        "',\"root_hash\":',"
        f"{_sql_json_text(col('lot_effect_root_hash'))}, '}}}}')"
    )
    return _sql_sha2(value, "__dbhash_lot_effects_hash")


def _db_finalization_expression(prefix: str = "") -> str:
    col = lambda name: f"{prefix}{name}"  # noqa: E731
    value = (
        "CONCAT('{\"namespace\":\"trading-v2.fill-accounting-finalization.v1\","
        "\"payload\":{\"accounting_outcome_id\":',"
        f"{_sql_json_text(col('accounting_outcome_id'))},"
        "',\"effect_hashes\":',"
        f"CONVERT({col('effect_hashes_json')} USING utf8mb4),"
        "',\"fill_execution_evidence_hash\":',"
        f"{_sql_json_text(col('fill_execution_evidence_hash'))},"
        "',\"fill_execution_evidence_id\":',"
        f"{_sql_json_text(col('fill_execution_evidence_id'))},"
        "',\"fill_id\":',"
        f"{_sql_json_text(col('fill_id'))},"
        "',\"finalization_status\":',"
        f"{_sql_json_text(col('finalization_status'))},"
        "',\"finalized_at\":',"
        f"{_sql_datetime(col('finalized_at'))},"
        "',\"lot_effect_count\":',"
        f"CAST({col('lot_effect_count')} AS CHAR),"
        "',\"lot_effect_root_hash\":',"
        f"{_sql_json_text(col('lot_effect_root_hash'))},"
        "',\"lot_effects_hash\":',"
        f"{_sql_json_text(col('lot_effects_hash'))},"
        "',\"outcome_hash\":',"
        f"{_sql_json_text(col('outcome_hash'))},"
        "',\"provenance_hash\":',"
        f"{_sql_json_text(col('provenance_hash'))},"
        "',\"total_effect_quantity\":',"
        f"CAST({col('total_effect_quantity')} AS CHAR), '}}}}')"
    )
    return _sql_sha2(value, "__dbhash_finalization_hash")


def _all_mappings(result: Any, table: str) -> tuple[Mapping[str, Any], ...]:
    try:
        values = result.mappings().all()
    except Exception as exc:
        raise _fail(f"{table} did not return mapping rows") from exc
    rows: list[Mapping[str, Any]] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise _fail(f"{table} returned a non-mapping row")
        rows.append(dict(value))
    return tuple(rows)


def _load_execution_evidence_parents(
    connection: Any,
) -> tuple[
    dict[str, FillExecutionEvidence],
    dict[str, CashEventBinding],
    dict[str, OrderTransitionEvidence],
]:
    rows_by_table: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for table, columns, order_by in execution_auditor._TABLES:
        expressions = tuple(
            execution_auditor._db_hash_expression(json_column, hash_column)
            for json_column, hash_column in execution_auditor.EVIDENCE_JSON_HASH_COLUMNS[
                table
            ]
        )
        result = connection.execute(
            text(
                f"/* v2aoa:parent_{table} */\n"
                f"SELECT {', '.join((*columns, *expressions))} FROM {table} "
                f"ORDER BY {order_by} LOCK IN SHARE MODE"
            )
        )
        rows_by_table[table] = _all_mappings(result, table)
    core_report = execution_auditor.audit_v2_execution_evidence_rows(
        rows_by_table,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    core_counts = dict(core_report.table_counts)
    expected_payloads = sum(
        core_counts.get(table, 0) * len(columns)
        for table, columns in execution_auditor.EVIDENCE_JSON_HASH_COLUMNS.items()
    )
    if (
        frozenset(core_counts)
        != frozenset(execution_auditor.EVIDENCE_JSON_HASH_COLUMNS)
        or core_report.rows_reconstructed != sum(core_counts.values())
        or core_report.payload_hashes_verified != expected_payloads
        or not core_report.database_sha2_used
        or not core_report.shared_row_locks_used
    ):
        raise _fail("parent execution-evidence audit was incomplete")

    payloads: dict[str, tuple[dict[str, CanonicalJson], ...]] = {}
    for table, _, _ in execution_auditor._TABLES:
        payloads[table] = tuple(
            execution_auditor._canonical_payloads(table, row, index)
            for index, row in enumerate(rows_by_table[table])
        )
    calendar_rows = rows_by_table["st_market_calendar_evidence_v2"]
    calendars_tuple = tuple(
        execution_auditor._calendar(
            row, payloads["st_market_calendar_evidence_v2"][index], index
        )
        for index, row in enumerate(calendar_rows)
    )
    calendars = execution_auditor._unique_map(
        calendars_tuple, "calendar_evidence_id", "calendar evidence"
    )
    quote_rows = rows_by_table["st_quote_receipt_evidence_v2"]
    quotes_tuple = tuple(
        execution_auditor._quote(
            row, payloads["st_quote_receipt_evidence_v2"][index], index
        )
        for index, row in enumerate(quote_rows)
    )
    quotes = execution_auditor._unique_map(
        quotes_tuple, "quote_evidence_id", "quote evidence"
    )
    fill_rows = rows_by_table["st_fill_execution_evidence_v2"]
    fills_tuple = tuple(
        execution_auditor._fill(
            row,
            payloads["st_fill_execution_evidence_v2"][index],
            index,
            calendars,
            quotes,
        )
        for index, row in enumerate(fill_rows)
    )
    fills = execution_auditor._unique_map(
        fills_tuple, "fill_execution_evidence_id", "fill evidence"
    )
    cash_rows = rows_by_table["st_cash_event_binding_v2"]
    cash_tuple = tuple(
        execution_auditor._cash(
            row,
            payloads["st_cash_event_binding_v2"][index],
            index,
            fills,
        )
        for index, row in enumerate(cash_rows)
    )
    cash = execution_auditor._unique_map(cash_tuple, "cash_binding_id", "cash binding")
    order_rows = rows_by_table["st_order_transition_v2"]
    orders_tuple = tuple(
        execution_auditor._order(
            row,
            payloads["st_order_transition_v2"][index],
            index,
            fills,
        )
        for index, row in enumerate(order_rows)
    )
    orders = execution_auditor._unique_map(
        orders_tuple, "transition_id", "order transition"
    )
    return dict(fills), dict(cash), dict(orders)


def _select_parent_rows(
    connection: Any,
    *,
    table: str,
    columns: tuple[str, ...],
    key_column: str,
    keys: set[str],
) -> dict[str, Mapping[str, Any]]:
    if not keys:
        return {}
    ordered = sorted(keys)
    params = {f"key_{index}": value for index, value in enumerate(ordered)}
    placeholders = ", ".join(f":key_{index}" for index in range(len(ordered)))
    result = connection.execute(
        text(
            f"/* v2aoa:parent_{table} */\n"
            f"SELECT {', '.join(columns)} FROM {table} "
            f"WHERE {key_column} IN ({placeholders}) "
            f"ORDER BY {key_column} LOCK IN SHARE MODE"
        ),
        params,
    )
    rows = _all_mappings(result, table)
    mapped: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if frozenset(row) != frozenset(columns):
            raise _fail(f"{table} parent columns are not exact")
        key = _text_value(row[key_column], f"{table}.{key_column}")
        if key in mapped:
            raise _fail(f"duplicate {table} parent: {key}")
        mapped[key] = row
    missing = keys - set(mapped)
    if missing:
        raise _fail(f"{table} parent rows are missing: {', '.join(sorted(missing))}")
    return mapped


def _database_parents(
    connection: Any,
    rows_by_table: Mapping[str, tuple[Mapping[str, Any], ...]],
) -> V2AccountingEvidenceAuditParents:
    fills, cash_bindings, order_transitions = _load_execution_evidence_parents(
        connection
    )
    outcome_rows = rows_by_table[OUTCOME_TABLE]
    effect_rows = rows_by_table[LOT_EFFECT_TABLE]
    account_ids = {
        _text_value(row["account_id"], "outcome.account_id") for row in outcome_rows
    }
    order_ids = {
        _text_value(row["order_id"], "outcome.order_id") for row in outcome_rows
    }
    cash_ids = {
        _text_value(row["cash_event_id"], "outcome.cash_event_id")
        for row in outcome_rows
    }
    lot_ids = {
        _text_value(row["lot_id"], "effect.lot_id") for row in effect_rows
    }
    lots = _select_parent_rows(
        connection,
        table="st_position_lot_v2",
        columns=LOT_COLUMNS,
        key_column="lot_id",
        keys=lot_ids,
    )
    fill_ids = {
        _text_value(row["fill_id"], "outcome.fill_id") for row in outcome_rows
    }
    fill_ids.update(
        _text_value(row["opened_fill_id"], "lot.opened_fill_id")
        for row in lots.values()
    )
    return V2AccountingEvidenceAuditParents(
        fills=fills,
        cash_bindings=cash_bindings,
        order_transitions=order_transitions,
        accounts=_select_parent_rows(
            connection,
            table="st_trade_account_v2",
            columns=ACCOUNT_COLUMNS,
            key_column="account_id",
            keys=account_ids,
        ),
        orders=_select_parent_rows(
            connection,
            table="st_order_v2",
            columns=ORDER_COLUMNS,
            key_column="order_id",
            keys=order_ids,
        ),
        fill_facts=_select_parent_rows(
            connection,
            table="st_fill_v2",
            columns=FILL_COLUMNS,
            key_column="fill_id",
            keys=fill_ids,
        ),
        cash_facts=_select_parent_rows(
            connection,
            table="st_cash_ledger_v2",
            columns=CASH_COLUMNS,
            key_column="cash_event_id",
            keys=cash_ids,
        ),
        lots=lots,
    )


def audit_v2_accounting_evidence_database(
    connection: Any,
) -> V2AccountingEvidenceAuditReport:
    """Audit one caller-owned, already-open MySQL transaction."""

    if connection is None or not callable(getattr(connection, "execute", None)):
        raise _fail("a SQLAlchemy-like connection is required")
    in_transaction = getattr(connection, "in_transaction", None)
    if not callable(in_transaction) or in_transaction() is not True:
        raise _fail("connection must already be in a transaction")

    outcome_expressions = (
        _db_provenance_expression(),
        _db_lot_root_expression(),
        _db_outcome_expression(),
    )
    outcome_result = connection.execute(
        text(
            "/* v2aoa:audit_st_fill_accounting_outcome_v2 */\n"
            f"SELECT {', '.join((*OUTCOME_COLUMNS, *outcome_expressions))} "
            f"FROM {OUTCOME_TABLE} ORDER BY accounting_outcome_id "
            "LOCK IN SHARE MODE"
        )
    )
    outcome_rows = _all_mappings(outcome_result, OUTCOME_TABLE)

    effect_columns = tuple(f"effect.{column} AS {column}" for column in LOT_EFFECT_COLUMNS)
    effect_expressions = (
        _db_provenance_expression("effect."),
        _db_lot_root_expression("effect.", "outcome."),
        _db_lot_snapshot_expression(
            "effect.before_lot_json", "__dbhash_before_lot_hash"
        ),
        _db_lot_snapshot_expression(
            "effect.after_lot_json", "__dbhash_after_lot_hash"
        ),
        _db_effect_expression("effect."),
    )
    effect_result = connection.execute(
        text(
            "/* v2aoa:audit_st_lot_transition_evidence_v2 */\n"
            f"SELECT {', '.join((*effect_columns, *effect_expressions))} "
            f"FROM {LOT_EFFECT_TABLE} effect LEFT JOIN {OUTCOME_TABLE} outcome "
            "ON outcome.accounting_outcome_id = effect.accounting_outcome_id "
            "ORDER BY effect.accounting_outcome_id, effect.effect_sequence, "
            "effect.lot_transition_evidence_id LOCK IN SHARE MODE"
        )
    )
    effect_rows = _all_mappings(effect_result, LOT_EFFECT_TABLE)

    final_expressions = (
        _db_provenance_expression(),
        _db_effect_list_expression(),
        _db_finalization_expression(),
    )
    final_result = connection.execute(
        text(
            "/* v2aoa:audit_st_fill_accounting_outcome_finalization_v2 */\n"
            f"SELECT {', '.join((*FINALIZATION_COLUMNS, *final_expressions))} "
            f"FROM {FINALIZATION_TABLE} ORDER BY accounting_outcome_id "
            "LOCK IN SHARE MODE"
        )
    )
    final_rows = _all_mappings(final_result, FINALIZATION_TABLE)
    rows_by_table = {
        OUTCOME_TABLE: outcome_rows,
        LOT_EFFECT_TABLE: effect_rows,
        FINALIZATION_TABLE: final_rows,
    }
    parents = _database_parents(connection, rows_by_table)
    return audit_v2_accounting_evidence_rows(
        rows_by_table,
        parents=parents,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )


__all__ = [
    "ACCOUNTING_AUDIT_HASH_FIELDS",
    "ACCOUNTING_AUDIT_PARENT_KINDS",
    "ACCOUNTING_AUDIT_TABLES",
    "FINALIZATION_TABLE",
    "LOT_EFFECT_TABLE",
    "OUTCOME_TABLE",
    "V2AccountingEvidenceAuditError",
    "V2AccountingEvidenceAuditParents",
    "V2AccountingEvidenceAuditReport",
    "audit_v2_accounting_evidence_database",
    "audit_v2_accounting_evidence_rows",
    "expected_accounting_hash_verifications",
]
